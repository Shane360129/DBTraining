# train__dora_benchmark_v0322.py
# ============================================================
# 標準 NL-to-SQL Benchmark 訓練腳本
# 支援 Spider 1.0 / BIRD / WP_M09 三種 Benchmark
#
# 論文驗證流程：
#   Phase 1: Spider 1.0 訓練 → 官方 dev 評估 (證明方法有效)
#   Phase 2: BIRD 訓練 → 官方 dev 評估 (證明複雜場景有效)
#   Phase 3: WP_M09 訓練 → val_v2 評估 (證明企業場景可行)
#
# 統一方法: DoRA (r=16, alpha=32) on Llama-3.1-8B-Instruct
# 統一格式: Llama-3.1 Chat Template + Per-Database Schema
#
# 用法:
#   python train__dora_benchmark_v0322.py --mode spider
#   python train__dora_benchmark_v0322.py --mode bird
#   python train__dora_benchmark_v0322.py --mode wp_m09
#   python train__dora_benchmark_v0322.py --mode spider+wp_m09
#
# 輸出:
#   outputs/models/benchmark_{mode}_0322/final_model/
# ============================================================

import json
import os
import re
import sys
import argparse
import torch
import statistics
from datetime import datetime
from collections import Counter
from datasets import Dataset
from transformers import (
    AutoTokenizer,
    AutoModelForCausalLM,
    BitsAndBytesConfig,
)
from peft import LoraConfig, get_peft_model, TaskType
from trl import SFTTrainer, SFTConfig


# ============================================================
# Global Settings
# ============================================================
MODEL_PATH = "meta-llama/Llama-3.1-8B-Instruct"
DATE_STR   = "0322"

# ---- DoRA / LoRA ----
LORA_R        = 16
LORA_ALPHA    = 32
LORA_DROPOUT  = 0.05
USE_DORA      = True

# ---- Default training hyperparams (per-mode overrides below) ----
DEFAULT_HPARAMS = {
    "spider": {
        "epochs":       3,
        "lr":           2e-5,
        "batch_size":   4,
        "grad_accum":   4,    # effective batch = 16
        "max_seq_len":  1024,
        "warmup_ratio": 0.05,
        "weight_decay": 0.01,
    },
    "bird": {
        "epochs":       3,
        "lr":           2e-5,
        "batch_size":   4,
        "grad_accum":   4,
        "max_seq_len":  1536,  # BIRD schemas can be larger
        "warmup_ratio": 0.05,
        "weight_decay": 0.01,
    },
    "wp_m09": {
        "epochs":       6,
        "lr":           5e-5,
        "batch_size":   4,
        "grad_accum":   4,
        "max_seq_len":  640,
        "warmup_ratio": 0.06,
        "weight_decay": 0.01,
    },
    "spider+wp_m09": {
        "epochs":       4,
        "lr":           3e-5,
        "batch_size":   4,
        "grad_accum":   4,
        "max_seq_len":  1024,
        "warmup_ratio": 0.05,
        "weight_decay": 0.01,
    },
}

# ============================================================
# Data paths
# ============================================================
SPIDER_DIR       = r"data\spider"
SPIDER_TRAIN     = [
    os.path.join(SPIDER_DIR, "train_spider.json"),   # 7,000
    os.path.join(SPIDER_DIR, "train_others.json"),    # 1,659
]
SPIDER_DEV       = os.path.join(SPIDER_DIR, "dev.json")
SPIDER_TABLES    = os.path.join(SPIDER_DIR, "tables.json")
SPIDER_DB_DIR    = os.path.join(SPIDER_DIR, "database")

BIRD_DIR         = r"data\bird"
BIRD_TRAIN       = os.path.join(BIRD_DIR, "train", "train.json")
BIRD_DEV         = os.path.join(BIRD_DIR, "dev", "dev.json")
BIRD_TABLES      = os.path.join(BIRD_DIR, "dev", "dev_tables.json")
BIRD_DB_DIR      = os.path.join(BIRD_DIR, "dev", "dev_databases")

WPM09_TRAIN      = [
    r"data\wp_m09\train_spider_WP_M09.json",
    r"data\wp_m09\train_claude_en_2000.json",
]
WPM09_DEV        = r"data\wp_m09\val_claude_en_spider_v2.json"
WPM09_TABLES     = r"data\wp_m09\tables.json"


# ============================================================
# Schema Builder — 從 tables.json 建構 CREATE TABLE
# ============================================================
def load_schemas_from_tables_json(tables_json_path: str) -> dict:
    """
    讀取 Spider/BIRD 格式的 tables.json，
    為每個 db_id 建構 CREATE TABLE 語句。

    Returns: {db_id: "CREATE TABLE t1 (...); CREATE TABLE t2 (...);" }
    """
    with open(tables_json_path, "r", encoding="utf-8") as f:
        all_dbs = json.load(f)

    schemas = {}
    for db in all_dbs:
        db_id = db["db_id"]
        table_names = db["table_names_original"]
        columns     = db["column_names_original"]  # [[table_idx, col_name], ...]
        col_types   = db["column_types"]            # ["text", "number", ...]
        pks         = set(db.get("primary_keys", []))
        fks         = db.get("foreign_keys", [])

        # 按表分組欄位
        table_cols = {i: [] for i in range(len(table_names))}
        for col_idx, (table_idx, col_name) in enumerate(columns):
            if table_idx == -1:  # skip wildcard *
                continue
            ctype = col_types[col_idx] if col_idx < len(col_types) else "text"
            type_str = "TEXT" if ctype == "text" else "INTEGER"
            pk_str = " PRIMARY KEY" if col_idx in pks else ""
            table_cols[table_idx].append(f"  {col_name} {type_str}{pk_str}")

        # 建構 FK 語句
        fk_by_table = {}
        for src_idx, tgt_idx in fks:
            src_tbl_idx, src_col = columns[src_idx]
            tgt_tbl_idx, tgt_col = columns[tgt_idx]
            if src_tbl_idx not in fk_by_table:
                fk_by_table[src_tbl_idx] = []
            fk_by_table[src_tbl_idx].append(
                f"  FOREIGN KEY ({src_col}) REFERENCES {table_names[tgt_tbl_idx]}({tgt_col})"
            )

        # 組合 CREATE TABLE
        stmts = []
        for i, tname in enumerate(table_names):
            cols = table_cols.get(i, [])
            fk_lines = fk_by_table.get(i, [])
            all_lines = cols + fk_lines
            if all_lines:
                stmt = f"CREATE TABLE {tname} (\n" + ",\n".join(all_lines) + "\n);"
            else:
                stmt = f"CREATE TABLE {tname} ();"
            stmts.append(stmt)

        schemas[db_id] = "\n\n".join(stmts)

    return schemas


# ============================================================
# WP_M09 專用 Schema（從 v0322 訓練腳本匯入）
# ============================================================
WPM09_VIEW_SCHEMAS = {
    "WP_vAcctIn": """CREATE TABLE WP_vAcctIn (
  -- Accounts Receivable. isDel='N' header, dtlIsDel='N' detail. Date: LEFT(acctInId,8)='YYYYMMDD'.
  -- SUM/AVG on amount: use (SELECT DISTINCT acctInId, amount FROM ... WHERE isDel='N') sub
  sn INT, acctInId NVARCHAR, acctInDate DATETIME, amount DECIMAL, memo NVARCHAR,
  empId NVARCHAR, isDel CHAR, dtlSn INT, OutStkId NVARCHAR, outStkAmtTotal DECIMAL,
  dtlIsDel CHAR, memSn INT, memId NVARCHAR, memName NVARCHAR,
  pNo INT, pBarcode NVARCHAR, pName NVARCHAR, pNameS NVARCHAR,
  oStkDtlAmt DECIMAL, oStkDtlQty DECIMAL, oStkDtlAmtTotal DECIMAL,
  dtlDiscnt DECIMAL, dtlDiscntShare DECIMAL, discount DECIMAL, discountShare DECIMAL
);""",
    "WP_vAcctOut": """CREATE TABLE WP_vAcctOut (
  -- Accounts Payable. isDel='N' header, dtlIsDel='N' detail. Date: LEFT(acctOutId,8)='YYYYMMDD'.
  -- SUM/AVG on amount: use (SELECT DISTINCT acctOutId, amount FROM ... WHERE isDel='N') sub
  sn INT, acctOutId NVARCHAR, acctOutDate DATETIME, amount DECIMAL, transAmt DECIMAL,
  memo NVARCHAR, empId NVARCHAR, empName NVARCHAR, isDel CHAR,
  dtlSn INT, InStkId NVARCHAR, dtlAmt DECIMAL, qty DECIMAL, amtTotal DECIMAL,
  dtlIsDel CHAR, pNo INT, pName NVARCHAR, pNameS NVARCHAR, pBarcode NVARCHAR,
  pvId NVARCHAR, pvName NVARCHAR, pvNameS NVARCHAR, pvSn INT, pvDiscount DECIMAL,
  inStkAmt DECIMAL, inStkAmtTotal DECIMAL, payType NVARCHAR
);""",
    "WP_vOutStock": """CREATE TABLE WP_vOutStock (
  -- Sales/Outbound Stock. isDel='N' header, dtlIsDel='N' detail. Date: LEFT(OutStkId,8)='YYYYMMDD'.
  -- SUM/AVG on amount: use (SELECT DISTINCT OutStkId, amount FROM ... WHERE isDel='N') sub
  sn INT, OutStkId NVARCHAR, OutStkDate DATETIME, amount DECIMAL, tax DECIMAL,
  amtNoneTax DECIMAL, isDel CHAR, empId NVARCHAR, empName NVARCHAR, memo NVARCHAR,
  memSn INT, memId NVARCHAR, memName NVARCHAR, outType NVARCHAR,
  dtlSn INT, pNo INT, qty DECIMAL, dtlAmt DECIMAL, amtTotal DECIMAL,
  dtlIsDel CHAR, dtlCostAvg DECIMAL, dtlCostStd DECIMAL,
  dtlDiscnt DECIMAL, dtlDiscntPer DECIMAL, dtlDiscntShare DECIMAL,
  pName NVARCHAR, pBarcode NVARCHAR, pUName NVARCHAR, costStd DECIMAL,
  discount DECIMAL, discountShare DECIMAL,
  memTel NVARCHAR, memCityName NVARCHAR, memZoneName NVARCHAR
);""",
    "WP_vTransfer": """CREATE TABLE WP_vTransfer (
  -- Transfer Orders. isDel='N' AND dtlIsDel='N' (almost always both needed). No header amount field.
  -- Date: LEFT(TransferId,8)='YYYYMMDD'. Use SUM(qty) or SUM(costAvg*qty) for value.
  sn INT, TransferId NVARCHAR, empId NVARCHAR, dtlSn INT,
  FromWhSn INT, fWhId NVARCHAR, fWhName NVARCHAR,
  ToWhSn INT, tfWhId NVARCHAR, tfWhName NVARCHAR,
  TransferDate DATETIME, pNo INT, qty DECIMAL,
  pName NVARCHAR, pNameS NVARCHAR, pBarcode NVARCHAR, pCode NVARCHAR,
  isDel CHAR, dtlIsDel CHAR, costAvg DECIMAL
);""",
    "WP_vInventory": """CREATE TABLE WP_vInventory (
  -- Inventory. NO isDel/dtlIsDel columns. NO date filtering. pNo is sequential number, NOT a date.
  whSn INT, WarehouseId NVARCHAR, WarehouseName NVARCHAR,
  pNo INT, pName NVARCHAR, pNameS NVARCHAR, pBarcode NVARCHAR,
  pUnit NVARCHAR, pUName NVARCHAR, priceStd DECIMAL, priceLow DECIMAL,
  priceMem DECIMAL, priceBat DECIMAL, costStd DECIMAL, costAvg DECIMAL,
  isSale CHAR, pvName NVARCHAR, pvNameS NVARCHAR, qtyNow DECIMAL,
  pvSn INT, qtySafe DECIMAL, qty DECIMAL
);""",
    "WP_vProduct": """CREATE TABLE WP_vProduct (
  -- Product. NO isDel/dtlIsDel columns. NO date filtering. pNo is sequential number, NOT a date.
  pNo INT, pName NVARCHAR, pNameS NVARCHAR, pBarcode NVARCHAR,
  pCode NVARCHAR, pUnit NVARCHAR, pUName NVARCHAR,
  priceStd DECIMAL, priceLow DECIMAL, priceMem DECIMAL, priceBat DECIMAL,
  isPvDiscount CHAR, isSale CHAR, costStd DECIMAL, costAvg DECIMAL,
  pvSn INT, pvId NVARCHAR, pvName NVARCHAR, pvNameS NVARCHAR,
  qtyNow DECIMAL, qtySafe DECIMAL, pvDiscount DECIMAL
);""",
    "WP_vProvider": """CREATE TABLE WP_vProvider (
  -- Supplier/Provider. NO isDel/dtlIsDel. Use isStop='N'/'Y'. NO date filtering.
  sn INT, pvId NVARCHAR, pvName NVARCHAR, pvNameS NVARCHAR,
  pvKId NVARCHAR, pvBoss NVARCHAR, pvTel NVARCHAR,
  pvCityId NVARCHAR, pvZoneId NVARCHAR, pvCity NVARCHAR, pvZone NVARCHAR,
  pvAddr NVARCHAR, ctactName NVARCHAR, ctactTel NVARCHAR,
  fax NVARCHAR, email NVARCHAR, taxId NVARCHAR, isStop CHAR,
  invoTitle NVARCHAR, bankId NVARCHAR, bankName NVARCHAR,
  bankAccount NVARCHAR, bankAcctName NVARCHAR, memo NVARCHAR,
  pvKName NVARCHAR, pvDiscount DECIMAL
);""",
}

WPM09_VIEW_LIST = "Available views: WP_vAcctIn (receivable), WP_vAcctOut (payable), WP_vOutStock (sales/outbound), WP_vTransfer (transfer), WP_vInventory (inventory), WP_vProduct (product), WP_vProvider (supplier)"

WPM09_VIEW_RULES = {
    "WP_vAcctIn":    "Rules: isDel='N' for active header, add dtlIsDel='N' for detail columns. Date: LEFT(acctInId,8)='YYYYMMDD'. Chinese: N prefix. Prefix: WP_M09.dbo.WP_vAcctIn. SUM/AVG on amount: use (SELECT DISTINCT acctInId, amount FROM ... WHERE isDel='N') sub.",
    "WP_vAcctOut":   "Rules: isDel='N' for active header, add dtlIsDel='N' for detail columns. Date: LEFT(acctOutId,8)='YYYYMMDD'. Chinese: N prefix. Prefix: WP_M09.dbo.WP_vAcctOut. SUM/AVG on amount: use (SELECT DISTINCT acctOutId, amount FROM ... WHERE isDel='N') sub.",
    "WP_vOutStock":  "Rules: isDel='N' for active header, add dtlIsDel='N' for detail columns. Date: LEFT(OutStkId,8)='YYYYMMDD'. Chinese: N prefix. Prefix: WP_M09.dbo.WP_vOutStock. SUM/AVG on amount: use (SELECT DISTINCT OutStkId, amount FROM ... WHERE isDel='N') sub.",
    "WP_vTransfer":  "Rules: isDel='N' AND dtlIsDel='N' almost always both needed. No header amount, use SUM(qty) or SUM(costAvg*qty). Date: LEFT(TransferId,8)='YYYYMMDD'. Chinese: N prefix. Prefix: WP_M09.dbo.WP_vTransfer.",
    "WP_vInventory": "Rules: NO isDel/dtlIsDel — never add them. pNo is sequential, not a date. Chinese: N prefix. Prefix: WP_M09.dbo.WP_vInventory.",
    "WP_vProduct":   "Rules: NO isDel/dtlIsDel — never add them. pNo is sequential, not a date. Chinese: N prefix. Prefix: WP_M09.dbo.WP_vProduct.",
    "WP_vProvider":  "Rules: NO isDel/dtlIsDel — use isStop='N' (active) / isStop='Y' (stopped). Chinese: N prefix. Prefix: WP_M09.dbo.WP_vProvider.",
}


# ============================================================
# System Prompt Builders
# ============================================================
def build_system_prompt_spider(db_id: str, schema_text: str) -> str:
    """Spider 1.0 系統 prompt：包含完整 DB schema。"""
    return (
        f"You are an expert SQL assistant. Given the database schema below, "
        f"generate ONLY the SQL query (SQLite dialect). Do not explain.\n\n"
        f"Database: {db_id}\n\n"
        f"{schema_text}"
    )


def build_system_prompt_bird(db_id: str, schema_text: str, evidence: str = "") -> str:
    """BIRD 系統 prompt：包含 DB schema + evidence（領域知識提示）。"""
    parts = [
        f"You are an expert SQL assistant. Given the database schema and evidence below, "
        f"generate ONLY the SQL query (SQLite dialect). Do not explain.",
        f"Database: {db_id}",
        schema_text,
    ]
    if evidence and evidence.strip():
        parts.append(f"Evidence: {evidence}")
    return "\n\n".join(parts)


def build_system_prompt_wpm09(table: str) -> str:
    """WP_M09 系統 prompt：單表 schema + 規則（同 v0322）。"""
    schema = WPM09_VIEW_SCHEMAS.get(table, "")
    rules = WPM09_VIEW_RULES.get(table, "")
    return "\n\n".join([
        "You are an expert T-SQL assistant for WP_M09 (SQL Server). Generate ONLY the SQL query.",
        WPM09_VIEW_LIST,
        schema,
        rules,
    ])


# ============================================================
# Chat Template Builder
# ============================================================
def build_chat_text(system_prompt: str, question: str, sql: str, tokenizer) -> str:
    """統一的 Chat Template 建構函式。"""
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": question},
        {"role": "assistant", "content": sql},
    ]
    return tokenizer.apply_chat_template(
        messages, tokenize=False, add_generation_prompt=False,
    )


# ============================================================
# Data Loading — Spider 1.0
# ============================================================
def load_spider_data(tokenizer, db_schemas: dict, max_seq_len: int):
    """載入 Spider 1.0 訓練資料。"""
    all_samples = []
    for path in SPIDER_TRAIN:
        if not os.path.exists(path):
            print(f"  [WARN] {path} not found, skipping")
            continue
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"  Loaded {path}: {len(data)} samples")
        all_samples.extend(data)

    print(f"  Total Spider samples: {len(all_samples)}")

    texts = []
    skipped = 0
    db_counter = Counter()
    for s in all_samples:
        db_id = s.get("db_id", "")
        question = s.get("question", "")
        sql = s.get("query", "").strip().rstrip(';').strip()

        schema_text = db_schemas.get(db_id, "")
        if not schema_text:
            skipped += 1
            continue

        system_prompt = build_system_prompt_spider(db_id, schema_text)
        text = build_chat_text(system_prompt, question, sql, tokenizer)

        # 檢查 token 長度
        tok_len = len(tokenizer(text, truncation=False)["input_ids"])
        if tok_len > max_seq_len:
            skipped += 1
            continue

        texts.append({"text": text, "db_id": db_id, "tok_len": tok_len})
        db_counter[db_id] += 1

    print(f"  Included: {len(texts)}, Skipped (no schema or too long): {skipped}")
    print(f"  Unique databases: {len(db_counter)}")
    return texts


# ============================================================
# Data Loading — BIRD
# ============================================================
def load_bird_data(tokenizer, max_seq_len: int):
    """載入 BIRD 訓練資料。"""
    if not os.path.exists(BIRD_TRAIN):
        print(f"\n  [INFO] BIRD data not found at {BIRD_TRAIN}")
        print(f"  Download BIRD from: https://bird-bench.github.io/")
        print(f"  Expected structure:")
        print(f"    data/bird/train/train.json")
        print(f"    data/bird/train/train_tables.json")
        print(f"    data/bird/dev/dev.json")
        print(f"    data/bird/dev/dev_tables.json")
        print(f"    data/bird/dev/dev_databases/<db_id>/<db_id>.sqlite")
        return []

    # Load BIRD tables
    bird_tables_path = os.path.join(BIRD_DIR, "train", "train_tables.json")
    if not os.path.exists(bird_tables_path):
        bird_tables_path = BIRD_TABLES
    if not os.path.exists(bird_tables_path):
        print(f"  [WARN] BIRD tables.json not found")
        return []

    db_schemas = load_schemas_from_tables_json(bird_tables_path)

    with open(BIRD_TRAIN, "r", encoding="utf-8") as f:
        all_samples = json.load(f)
    print(f"  Loaded {BIRD_TRAIN}: {len(all_samples)} samples")

    texts = []
    skipped = 0
    for s in all_samples:
        db_id = s.get("db_id", "")
        question = s.get("question", "")
        sql = s.get("SQL", s.get("query", "")).strip().rstrip(';').strip()
        evidence = s.get("evidence", "")

        schema_text = db_schemas.get(db_id, "")
        if not schema_text:
            skipped += 1
            continue

        system_prompt = build_system_prompt_bird(db_id, schema_text, evidence)
        text = build_chat_text(system_prompt, question, sql, tokenizer)

        tok_len = len(tokenizer(text, truncation=False)["input_ids"])
        if tok_len > max_seq_len:
            skipped += 1
            continue

        texts.append({"text": text, "db_id": db_id, "tok_len": tok_len})

    print(f"  Included: {len(texts)}, Skipped: {skipped}")
    return texts


# ============================================================
# Data Loading — WP_M09
# ============================================================
def extract_table_from_sql(sql: str) -> str:
    m = re.search(r'WP_M09\.dbo\.(\w+)', sql)
    if m:
        return m.group(1)
    m = re.search(r'\[WP_M09\]\.\[dbo\]\.\[(\w+)\]', sql)
    return m.group(1) if m else ""


def load_wpm09_data(tokenizer, max_seq_len: int):
    """載入 WP_M09 訓練資料。"""
    all_samples = []
    for path in WPM09_TRAIN:
        if not os.path.exists(path):
            print(f"  [WARN] {path} not found")
            continue
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"  Loaded {path}: {len(data)} samples")
        all_samples.extend(data)

    # 去重
    seen = set()
    deduped = []
    for s in all_samples:
        norm = re.sub(r'\s+', ' ', s.get("query", "").strip().rstrip(';').strip()).upper()
        if norm not in seen:
            seen.add(norm)
            deduped.append(s)
    print(f"  After dedup: {len(deduped)} (removed {len(all_samples)-len(deduped)})")

    texts = []
    skipped = 0
    for s in deduped:
        table = extract_table_from_sql(s.get("query", ""))
        if not table:
            skipped += 1
            continue

        question = s.get("question", "")
        sql = s.get("query", "").strip().rstrip(';').strip()

        system_prompt = build_system_prompt_wpm09(table)
        text = build_chat_text(system_prompt, question, sql, tokenizer)

        tok_len = len(tokenizer(text, truncation=False)["input_ids"])
        if tok_len > max_seq_len:
            skipped += 1
            continue

        texts.append({"text": text, "db_id": "WP_M09", "tok_len": tok_len})

    print(f"  Included: {len(texts)}, Skipped: {skipped}")
    return texts


# ============================================================
# Unified data pipeline
# ============================================================
def load_data_for_mode(mode: str, tokenizer, max_seq_len: int):
    """根據 mode 載入訓練資料。"""
    all_texts = []

    modes = mode.split("+")

    if "spider" in modes:
        print(f"\n{'='*50}")
        print("Loading Spider 1.0 training data...")
        print(f"{'='*50}")
        db_schemas = load_schemas_from_tables_json(SPIDER_TABLES)
        print(f"  Loaded schemas for {len(db_schemas)} databases")
        spider_texts = load_spider_data(tokenizer, db_schemas, max_seq_len)
        all_texts.extend(spider_texts)

    if "bird" in modes:
        print(f"\n{'='*50}")
        print("Loading BIRD training data...")
        print(f"{'='*50}")
        bird_texts = load_bird_data(tokenizer, max_seq_len)
        all_texts.extend(bird_texts)

    if "wp_m09" in modes:
        print(f"\n{'='*50}")
        print("Loading WP_M09 training data...")
        print(f"{'='*50}")
        wpm09_texts = load_wpm09_data(tokenizer, max_seq_len)
        all_texts.extend(wpm09_texts)

    if not all_texts:
        print("\n[ERROR] No training data loaded!")
        sys.exit(1)

    # Token length statistics
    lengths = [t["tok_len"] for t in all_texts]
    print(f"\n{'='*50}")
    print(f"Combined Dataset Statistics")
    print(f"{'='*50}")
    print(f"  Total samples: {len(all_texts)}")
    print(f"  Token length: Min={min(lengths)}, Max={max(lengths)}, "
          f"Mean={statistics.mean(lengths):.0f}, Median={statistics.median(lengths):.0f}")

    over_max = sum(1 for l in lengths if l > max_seq_len)
    print(f"  Over {max_seq_len}: {over_max} ({over_max/len(lengths)*100:.1f}%)")

    db_dist = Counter(t["db_id"] for t in all_texts)
    print(f"  Unique db_ids: {len(db_dist)}")
    if len(db_dist) <= 20:
        for db, cnt in db_dist.most_common():
            print(f"    {db}: {cnt}")
    else:
        for db, cnt in db_dist.most_common(10):
            print(f"    {db}: {cnt}")
        print(f"    ... and {len(db_dist)-10} more")

    # Show sample prompt
    print(f"\n  Sample prompt (first 800 chars):")
    print("-" * 60)
    print(all_texts[0]["text"][:800])
    print("-" * 60)

    dataset = Dataset.from_list([{"text": t["text"]} for t in all_texts])
    return dataset, len(all_texts)


# ============================================================
# Model loading
# ============================================================
def load_model_and_tokenizer(max_seq_len: int):
    print(f"\nLoading base model: {MODEL_PATH} ...")

    bnb_cfg = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16,
        bnb_4bit_use_double_quant=True,
    )

    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, use_fast=True)
    tokenizer.pad_token        = tokenizer.eos_token
    tokenizer.padding_side     = "right"
    tokenizer.model_max_length = max_seq_len

    model = AutoModelForCausalLM.from_pretrained(
        MODEL_PATH,
        quantization_config=bnb_cfg,
        device_map="auto",
        dtype=torch.bfloat16,
        attn_implementation="eager",
    )
    model.config.use_cache = False
    print("Base model loaded")
    return tokenizer, model


# ============================================================
# DoRA setup
# ============================================================
def apply_dora(model):
    lora_cfg = LoraConfig(
        r=LORA_R,
        lora_alpha=LORA_ALPHA,
        target_modules=[
            "q_proj", "k_proj", "v_proj", "o_proj",
            "gate_proj", "up_proj", "down_proj",
        ],
        lora_dropout=LORA_DROPOUT,
        bias="none",
        task_type=TaskType.CAUSAL_LM,
        use_dora=USE_DORA,
    )
    model = get_peft_model(model, lora_cfg)
    model.print_trainable_parameters()
    mode = "DoRA" if USE_DORA else "LoRA"
    print(f"Fine-tune method: {mode}  (r={LORA_R}, alpha={LORA_ALPHA})")
    return model


# ============================================================
# Training
# ============================================================
def train(model, tokenizer, dataset, hparams: dict, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)

    sft_cfg = SFTConfig(
        output_dir=output_dir,
        num_train_epochs=hparams["epochs"],
        per_device_train_batch_size=hparams["batch_size"],
        gradient_accumulation_steps=hparams["grad_accum"],
        learning_rate=hparams["lr"],
        lr_scheduler_type="cosine",
        warmup_ratio=hparams["warmup_ratio"],
        weight_decay=hparams["weight_decay"],
        optim="paged_adamw_8bit",
        bf16=True,
        logging_steps=10,
        save_strategy="epoch",
        save_total_limit=3,
        report_to="none",
        dataloader_num_workers=0,
        dataset_text_field="text",
        packing=False,
        max_seq_length=hparams["max_seq_len"],
    )

    trainer = SFTTrainer(
        model=model,
        processing_class=tokenizer,
        train_dataset=dataset,
        args=sft_cfg,
    )

    effective_batch = hparams["batch_size"] * hparams["grad_accum"]
    steps_per_epoch = len(dataset) // effective_batch
    total_steps = steps_per_epoch * hparams["epochs"]

    print(f"\nStarting training...")
    print(f"  Epochs:           {hparams['epochs']}")
    print(f"  Batch size:       {hparams['batch_size']} x {hparams['grad_accum']} = {effective_batch}")
    print(f"  Steps/epoch:      ~{steps_per_epoch}")
    print(f"  Total steps:      ~{total_steps}")
    print(f"  Learning rate:    {hparams['lr']}  (cosine decay)")
    print(f"  Max seq len:      {hparams['max_seq_len']}")
    print(f"  Output dir:       {output_dir}\n")

    # Resume from checkpoint
    last_ckpt = None
    if os.path.isdir(output_dir):
        ckpts = [d for d in os.listdir(output_dir) if d.startswith("checkpoint-")]
        if ckpts:
            last_ckpt = os.path.join(output_dir, sorted(ckpts, key=lambda x: int(x.split("-")[1]))[-1])
            print(f"  Resuming from: {last_ckpt}")

    trainer.train(resume_from_checkpoint=last_ckpt)
    return trainer


# ============================================================
# Save model
# ============================================================
def save_model(trainer, tokenizer, mode: str, n_samples: int, hparams: dict, output_dir: str):
    final_dir = os.path.join(output_dir, "final_model")
    os.makedirs(final_dir, exist_ok=True)

    trainer.model.save_pretrained(final_dir)
    tokenizer.save_pretrained(final_dir)

    info = {
        "base_model":       MODEL_PATH,
        "benchmark_mode":   mode,
        "train_format":     "Chat Template + Per-Database Schema",
        "train_script":     f"train__dora_benchmark_v{DATE_STR}.py",
        "method":           "DoRA" if USE_DORA else "LoRA",
        "lora_r":           LORA_R,
        "lora_alpha":       LORA_ALPHA,
        "train_samples":    n_samples,
        "epochs":           hparams["epochs"],
        "effective_batch":  hparams["batch_size"] * hparams["grad_accum"],
        "learning_rate":    hparams["lr"],
        "max_seq_len":      hparams["max_seq_len"],
        "weight_decay":     hparams["weight_decay"],
        "warmup_ratio":     hparams["warmup_ratio"],
        "date":             DATE_STR,
        "final_loss":       round(trainer.state.log_history[-1].get("loss", 0), 4),
    }
    with open(os.path.join(final_dir, "training_info.json"), "w") as f:
        json.dump(info, f, indent=2)

    print(f"\nModel saved: {final_dir}")
    for k, v in info.items():
        print(f"  {k}: {v}")
    return final_dir


# ============================================================
# Main
# ============================================================
def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark NL-to-SQL Training (Spider/BIRD/WP_M09)")
    parser.add_argument("--mode", type=str, default="spider",
                        choices=["spider", "bird", "wp_m09", "spider+wp_m09",
                                 "spider+bird", "spider+bird+wp_m09"],
                        help="Training mode / benchmark selection")
    parser.add_argument("--epochs", type=int, default=None, help="Override epochs")
    parser.add_argument("--lr", type=float, default=None, help="Override learning rate")
    parser.add_argument("--max-seq-len", type=int, default=None, help="Override max sequence length")
    parser.add_argument("--batch-size", type=int, default=None, help="Override batch size")
    return parser.parse_args()


def main():
    args = parse_args()
    mode = args.mode

    # 取得超參數（mode 預設值 + CLI override）
    base_mode = mode.split("+")[0]
    hparams = DEFAULT_HPARAMS.get(base_mode, DEFAULT_HPARAMS["spider"]).copy()

    # 聯合訓練模式的超參數
    if "+" in mode:
        if mode in DEFAULT_HPARAMS:
            hparams = DEFAULT_HPARAMS[mode].copy()
        else:
            # spider+bird 等組合，使用 spider 的參數但加長 seq_len
            hparams = DEFAULT_HPARAMS["spider"].copy()
            if "bird" in mode:
                hparams["max_seq_len"] = 1536
            if "wp_m09" in mode:
                hparams["epochs"] = max(hparams["epochs"], 4)

    # CLI overrides
    if args.epochs is not None:
        hparams["epochs"] = args.epochs
    if args.lr is not None:
        hparams["lr"] = args.lr
    if args.max_seq_len is not None:
        hparams["max_seq_len"] = args.max_seq_len
    if args.batch_size is not None:
        hparams["batch_size"] = args.batch_size

    output_dir = f"outputs/models/benchmark_{mode.replace('+','_')}_{DATE_STR}"

    print("=" * 70)
    print(f"NL-to-SQL Benchmark Training v{DATE_STR}")
    print(f"  Mode:       {mode}")
    print(f"  Model:      {MODEL_PATH}")
    print(f"  Method:     {'DoRA' if USE_DORA else 'LoRA'} (r={LORA_R}, alpha={LORA_ALPHA})")
    print(f"  Output:     {output_dir}")
    print(f"  Hparams:    {hparams}")
    print("=" * 70)

    # 1. Load model
    tokenizer, model = load_model_and_tokenizer(hparams["max_seq_len"])

    # 2. Load data
    dataset, n_samples = load_data_for_mode(mode, tokenizer, hparams["max_seq_len"])

    # 3. Apply DoRA
    model = apply_dora(model)

    # 4. Train
    trainer = train(model, tokenizer, dataset, hparams, output_dir)

    # 5. Save
    final_dir = save_model(trainer, tokenizer, mode, n_samples, hparams, output_dir)

    # 6. Print eval commands
    print("\n" + "=" * 70)
    print("Training complete! Evaluate with:")
    print("=" * 70)

    if "spider" in mode:
        print(f"\n# Spider 1.0 Official Evaluation:")
        print(f"python eval__benchmark_official.py \\")
        print(f"    --mode spider \\")
        print(f"    --model {final_dir} \\")
        print(f"    --gold {SPIDER_DEV} \\")
        print(f"    --tables {SPIDER_TABLES} \\")
        print(f"    --db-dir {SPIDER_DB_DIR} \\")
        print(f"    --output outputs/eval_spider_{DATE_STR}.json")

    if "bird" in mode:
        print(f"\n# BIRD Official Evaluation:")
        print(f"python eval__benchmark_official.py \\")
        print(f"    --mode bird \\")
        print(f"    --model {final_dir} \\")
        print(f"    --gold {BIRD_DEV} \\")
        print(f"    --tables {BIRD_TABLES} \\")
        print(f"    --db-dir {BIRD_DB_DIR} \\")
        print(f"    --output outputs/eval_bird_{DATE_STR}.json")

    if "wp_m09" in mode:
        print(f"\n# WP_M09 Evaluation:")
        print(f"python eval__benchmark_official.py \\")
        print(f"    --mode wp_m09 \\")
        print(f"    --model {final_dir} \\")
        print(f"    --gold {WPM09_DEV} \\")
        print(f"    --output outputs/eval_wpm09_{DATE_STR}.json \\")
        print(f"    --db-host \"SHANE\\SQLEXPRESS\" --db-trusted")


if __name__ == "__main__":
    main()
