# eval__benchmark_official.py
# ============================================================
# 統一 Benchmark 評估腳本
# 支援 Spider 1.0 / BIRD / WP_M09 三種 Benchmark 的官方評估
#
# 指標：
#   - Spider 1.0: Exact Match (EM) + Execution Accuracy (EX)
#   - BIRD:       Execution Accuracy (EX) + Valid Efficiency Score (VES)
#   - WP_M09:     EM + EX (SQL Server)
#
# 用法：
#   python eval__benchmark_official.py --mode spider --model <path> --gold <path> --tables <path> --db-dir <path>
#   python eval__benchmark_official.py --mode bird   --model <path> --gold <path> --tables <path> --db-dir <path>
#   python eval__benchmark_official.py --mode wp_m09 --model <path> --gold <path> --db-host "SHANE\SQLEXPRESS" --db-trusted
#
# 輸出：
#   - JSON 報告（含每筆預測 + 總結統計）
#   - 終端機印出結果表格
# ============================================================

import json
import os
import re
import sys
import time
import argparse
import sqlite3
import torch
from collections import Counter, defaultdict
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from peft import PeftModel

# 匯入 Spider 官方評估模組（如果可用）
SPIDER_EVAL_DIR = os.path.join("data", "spider")
sys.path.insert(0, SPIDER_EVAL_DIR)
try:
    from process_sql import get_schema as spider_get_schema, Schema as SpiderSchema, get_sql
    from evaluation import Evaluator as SpiderEvaluator, build_foreign_key_map_from_json
    HAS_SPIDER_EVAL = True
except ImportError:
    HAS_SPIDER_EVAL = False
    print("[WARN] Spider official evaluation modules not found. Using string-based EM only.")


# ============================================================
# Schema Builder（從 train 腳本共用）
# ============================================================
def load_schemas_from_tables_json(tables_json_path: str) -> dict:
    """讀取 tables.json，為每個 db_id 建構 CREATE TABLE 語句。"""
    with open(tables_json_path, "r", encoding="utf-8") as f:
        all_dbs = json.load(f)

    schemas = {}
    for db in all_dbs:
        db_id = db["db_id"]
        table_names = db["table_names_original"]
        columns     = db["column_names_original"]
        col_types   = db["column_types"]
        pks         = set(db.get("primary_keys", []))
        fks         = db.get("foreign_keys", [])

        table_cols = {i: [] for i in range(len(table_names))}
        for col_idx, (table_idx, col_name) in enumerate(columns):
            if table_idx == -1:
                continue
            ctype = col_types[col_idx] if col_idx < len(col_types) else "text"
            type_str = "TEXT" if ctype == "text" else "INTEGER"
            pk_str = " PRIMARY KEY" if col_idx in pks else ""
            table_cols[table_idx].append(f"  {col_name} {type_str}{pk_str}")

        fk_by_table = {}
        for src_idx, tgt_idx in fks:
            src_tbl_idx, src_col = columns[src_idx]
            tgt_tbl_idx, tgt_col = columns[tgt_idx]
            if src_tbl_idx not in fk_by_table:
                fk_by_table[src_tbl_idx] = []
            fk_by_table[src_tbl_idx].append(
                f"  FOREIGN KEY ({src_col}) REFERENCES {table_names[tgt_tbl_idx]}({tgt_col})"
            )

        stmts = []
        for i, tname in enumerate(table_names):
            cols = table_cols.get(i, [])
            fk_lines = fk_by_table.get(i, [])
            all_lines = cols + fk_lines
            stmt = f"CREATE TABLE {tname} (\n" + ",\n".join(all_lines) + "\n);" if all_lines else f"CREATE TABLE {tname} ();"
            stmts.append(stmt)

        schemas[db_id] = "\n\n".join(stmts)
    return schemas


# ============================================================
# WP_M09 Schema（同 train 腳本）
# ============================================================
WPM09_VIEW_SCHEMAS = None
WPM09_VIEW_LIST = None
WPM09_VIEW_RULES = None

def _load_wpm09_schemas():
    """Lazy import from training script."""
    global WPM09_VIEW_SCHEMAS, WPM09_VIEW_LIST, WPM09_VIEW_RULES
    try:
        from train__dora_benchmark_v0322 import (
            WPM09_VIEW_SCHEMAS as VS,
            WPM09_VIEW_LIST as VL,
            WPM09_VIEW_RULES as VR,
        )
        WPM09_VIEW_SCHEMAS = VS
        WPM09_VIEW_LIST = VL
        WPM09_VIEW_RULES = VR
    except ImportError:
        # Fallback: import from v0322 training script
        try:
            from train__dora_spider_v0322 import (
                VIEW_SCHEMAS as VS,
                VIEW_LIST as VL,
                VIEW_RULES as VR,
            )
            WPM09_VIEW_SCHEMAS = VS
            WPM09_VIEW_LIST = VL
            WPM09_VIEW_RULES = VR
        except ImportError:
            print("[ERROR] Cannot import WP_M09 schemas from training script.")
            sys.exit(1)


# ============================================================
# Prompt Builders（同 train 腳本，但加 add_generation_prompt=True）
# ============================================================
def build_inference_prompt_spider(db_id: str, schema_text: str, question: str, tokenizer) -> str:
    system = (
        f"You are an expert SQL assistant. Given the database schema below, "
        f"generate ONLY the SQL query (SQLite dialect). Do not explain.\n\n"
        f"Database: {db_id}\n\n"
        f"{schema_text}"
    )
    messages = [
        {"role": "system", "content": system},
        {"role": "user",   "content": question},
    ]
    return tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)


def build_inference_prompt_bird(db_id: str, schema_text: str, question: str, evidence: str, tokenizer) -> str:
    parts = [
        f"You are an expert SQL assistant. Given the database schema and evidence below, "
        f"generate ONLY the SQL query (SQLite dialect). Do not explain.",
        f"Database: {db_id}",
        schema_text,
    ]
    if evidence and evidence.strip():
        parts.append(f"Evidence: {evidence}")
    system = "\n\n".join(parts)
    messages = [
        {"role": "system", "content": system},
        {"role": "user",   "content": question},
    ]
    return tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)


def build_inference_prompt_wpm09(table: str, question: str, tokenizer) -> str:
    if WPM09_VIEW_SCHEMAS is None:
        _load_wpm09_schemas()

    schema = WPM09_VIEW_SCHEMAS.get(table, "")
    rules = WPM09_VIEW_RULES.get(table, "")
    system = "\n\n".join([
        "You are an expert T-SQL assistant for WP_M09 (SQL Server). Generate ONLY the SQL query.",
        WPM09_VIEW_LIST,
        schema,
        rules,
    ])
    messages = [
        {"role": "system", "content": system},
        {"role": "user",   "content": question},
    ]
    return tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)


# ============================================================
# Table inference for WP_M09 (keyword-based)
# ============================================================
TABLE_KEYWORD_MAP = {
    "WP_vAcctIn":    {"receivable": 3, "acctIn": 5, "應收": 3, "acctInId": 5, "收款": 3},
    "WP_vAcctOut":   {"payable": 3, "acctOut": 5, "應付": 3, "acctOutId": 5, "付款": 3, "purchase": 2, "進貨": 3, "inStkId": 5, "supplier payment": 3},
    "WP_vOutStock":  {"outstock": 5, "outstkid": 5, "outbound": 3, "出庫": 3, "銷貨": 3, "sale": 2, "sold": 2, "出貨": 3, "sell": 2, "sales order": 3},
    "WP_vTransfer":  {"transfer": 4, "調撥": 5, "transferid": 5, "warehouse to warehouse": 4, "from warehouse": 3, "to warehouse": 3},
    "WP_vInventory": {"inventory": 4, "庫存": 4, "stock level": 3, "qtyNow": 5, "warehouse": 2, "current stock": 3, "in stock": 3},
    "WP_vProduct":   {"product": 2, "商品": 3, "pName": 3, "priceStd": 4, "barcode": 3, "product info": 4, "item": 1},
    "WP_vProvider":  {"provider": 3, "supplier": 3, "供應商": 4, "pvName": 5, "isStop": 5, "vendor": 3, "pvDiscount": 4},
}

def infer_table_from_question(question: str) -> str:
    q_lower = question.lower()
    scores = {t: 0 for t in TABLE_KEYWORD_MAP}
    for table, kw_map in TABLE_KEYWORD_MAP.items():
        for kw, weight in kw_map.items():
            if kw.lower() in q_lower:
                scores[table] += weight
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "WP_vOutStock"


# ============================================================
# SQL Normalization
# ============================================================
def normalize_sql(sql: str) -> str:
    """正規化 SQL 用於 EM 比較。"""
    s = sql.strip().rstrip(';').strip()
    # 移除 WP_M09 前綴
    s = re.sub(r'\[?WP_M09\]?\.\[?dbo\]?\.', '', s)
    # 移除方括號
    s = re.sub(r'\[(\w+)\]', r'\1', s)
    # 壓縮空格
    s = re.sub(r'\s+', ' ', s)
    # 統一大小寫（關鍵字）
    keywords = ['SELECT','FROM','WHERE','AND','OR','GROUP','BY','ORDER','HAVING',
                'JOIN','LEFT','RIGHT','INNER','ON','AS','IN','NOT','NULL','IS',
                'BETWEEN','LIKE','EXISTS','DISTINCT','TOP','COUNT','SUM','AVG',
                'MAX','MIN','LIMIT','ASC','DESC','UNION','INTERSECT','EXCEPT',
                'INSERT','UPDATE','DELETE','CREATE','DROP','ALTER','INTO','VALUES',
                'SET','CASE','WHEN','THEN','ELSE','END','CAST','CONVERT']
    for kw in keywords:
        s = re.sub(r'\b' + kw + r'\b', kw, s, flags=re.IGNORECASE)
    return s.strip()


# ============================================================
# Model loading
# ============================================================
def load_model(model_path: str, max_seq_len: int = 1024):
    print(f"\nLoading model from: {model_path}")

    bnb_cfg = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16,
        bnb_4bit_use_double_quant=True,
    )

    # 讀取 training info
    info_path = os.path.join(model_path, "training_info.json")
    base_model = MODEL_PATH
    if os.path.exists(info_path):
        with open(info_path, "r") as f:
            info = json.load(f)
        base_model = info.get("base_model", MODEL_PATH)
        print(f"  Base model: {base_model}")
        print(f"  Training mode: {info.get('benchmark_mode', 'unknown')}")
        print(f"  Train samples: {info.get('train_samples', 'unknown')}")

    tokenizer = AutoTokenizer.from_pretrained(model_path, use_fast=True)
    tokenizer.pad_token = tokenizer.eos_token
    tokenizer.padding_side = "left"

    model = AutoModelForCausalLM.from_pretrained(
        base_model,
        quantization_config=bnb_cfg,
        device_map="auto",
        torch_dtype=torch.bfloat16,
    )
    model = PeftModel.from_pretrained(model, model_path)
    model.eval()

    print("Model loaded")
    return tokenizer, model


# ============================================================
# Inference
# ============================================================
def generate_sql(model, tokenizer, prompt: str, max_new_tokens: int = 256) -> str:
    inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=2048)
    inputs = {k: v.to(model.device) for k, v in inputs.items()}

    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=max_new_tokens,
            do_sample=False,
            temperature=1.0,
            pad_token_id=tokenizer.eos_token_id,
        )

    # 解碼只取新生成的部分
    new_tokens = outputs[0][inputs["input_ids"].shape[1]:]
    raw = tokenizer.decode(new_tokens, skip_special_tokens=True).strip()

    # 擷取 SQL（第一行或直到 <|eot_id|>）
    lines = raw.split("\n")
    sql_lines = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith("```") or line.startswith("--") or line.lower().startswith("note:"):
            break
        sql_lines.append(line)
        # 如果看到分號結尾，停止
        if line.endswith(";"):
            break

    result = " ".join(sql_lines).strip().rstrip(";").strip()
    return result


# ============================================================
# Execution — Spider (SQLite)
# ============================================================
def execute_sqlite(db_path: str, sql: str, timeout: int = 30):
    """在 SQLite 資料庫上執行 SQL，回傳排序後的結果。"""
    try:
        conn = sqlite3.connect(db_path)
        conn.execute(f"PRAGMA busy_timeout = {timeout * 1000}")
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        conn.close()
        return sorted([tuple(str(x) for x in row) for row in results])
    except Exception as e:
        return f"ERROR: {e}"


# ============================================================
# Execution — WP_M09 (SQL Server)
# ============================================================
def execute_sqlserver(sql: str, db_host: str, db_trusted: bool = True):
    """在 SQL Server 上執行 SQL。"""
    try:
        import pyodbc
        if db_trusted:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_host};DATABASE=WP_M09;Trusted_Connection=yes;"
        else:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_host};DATABASE=WP_M09;"
        conn = pyodbc.connect(conn_str, timeout=30)
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        conn.close()
        return sorted([tuple(str(x) for x in row) for row in results])
    except Exception as e:
        return f"ERROR: {e}"


# ============================================================
# Spider 1.0 Official Evaluation
# ============================================================
def eval_spider(model, tokenizer, gold_path: str, tables_path: str, db_dir: str, output_path: str):
    """Spider 1.0 官方評估流程。"""
    print(f"\n{'='*70}")
    print(f"Spider 1.0 Evaluation")
    print(f"{'='*70}")

    # Load data
    with open(gold_path, "r", encoding="utf-8") as f:
        gold_data = json.load(f)
    print(f"  Dev set: {len(gold_data)} samples")

    db_schemas = load_schemas_from_tables_json(tables_path)
    print(f"  Database schemas: {len(db_schemas)}")

    # 載入 tables.json 原始資料（用於官方 eval）
    with open(tables_path, "r", encoding="utf-8") as f:
        tables_raw = json.load(f)
    tables_by_id = {t["db_id"]: t for t in tables_raw}

    # Inference
    predictions = []
    start_time = time.time()
    for idx, sample in enumerate(gold_data):
        db_id = sample["db_id"]
        question = sample["question"]
        gold_sql = sample["query"]

        schema_text = db_schemas.get(db_id, "")
        prompt = build_inference_prompt_spider(db_id, schema_text, question, tokenizer)
        pred_sql = generate_sql(model, tokenizer, prompt)

        predictions.append({
            "idx": idx,
            "db_id": db_id,
            "question": question,
            "gold_sql": gold_sql,
            "pred_sql": pred_sql,
        })

        if (idx + 1) % 50 == 0:
            elapsed = time.time() - start_time
            rate = (idx + 1) / elapsed
            remaining = (len(gold_data) - idx - 1) / rate
            print(f"  [{idx+1}/{len(gold_data)}] {rate:.1f} samples/sec, ETA: {remaining/60:.1f} min")

    elapsed = time.time() - start_time
    print(f"\n  Inference done: {len(predictions)} samples in {elapsed:.0f}s")

    # ---- String-based EM ----
    em_count = 0
    for p in predictions:
        if normalize_sql(p["pred_sql"]) == normalize_sql(p["gold_sql"]):
            em_count += 1
            p["string_em"] = True
        else:
            p["string_em"] = False

    print(f"\n  String EM: {em_count}/{len(predictions)} = {em_count/len(predictions)*100:.2f}%")

    # ---- Execution Accuracy ----
    ex_count = 0
    ex_errors = 0
    for p in predictions:
        db_path = os.path.join(db_dir, p["db_id"], f"{p['db_id']}.sqlite")
        if not os.path.exists(db_path):
            p["ex_match"] = None
            continue

        gold_result = execute_sqlite(db_path, p["gold_sql"])
        pred_result = execute_sqlite(db_path, p["pred_sql"])

        if isinstance(gold_result, str) or isinstance(pred_result, str):
            p["ex_match"] = False
            ex_errors += 1
        elif gold_result == pred_result:
            p["ex_match"] = True
            ex_count += 1
        else:
            p["ex_match"] = False

    ex_total = sum(1 for p in predictions if p.get("ex_match") is not None)
    print(f"  Execution Accuracy: {ex_count}/{ex_total} = {ex_count/ex_total*100:.2f}%")
    print(f"  Execution errors: {ex_errors}")

    # ---- Spider Official EM (if available) ----
    spider_em = None
    if HAS_SPIDER_EVAL:
        try:
            kmaps = build_foreign_key_map_from_json(tables_path)
            evaluator = SpiderEvaluator()

            # Build prediction format for Spider eval
            exact_match_count = 0
            for p in predictions:
                db_id = p["db_id"]
                db_path = os.path.join(db_dir, db_id, f"{db_id}.sqlite")
                if not os.path.exists(db_path):
                    continue

                schema = spider_get_schema(db_path)
                spider_schema = SpiderSchema(schema)

                try:
                    g_sql = get_sql(spider_schema, p["gold_sql"])
                    p_sql = get_sql(spider_schema, p["pred_sql"])
                    kmap = kmaps.get(db_id, {})
                    is_match = evaluator.eval_exact_match(p_sql, g_sql, kmap)
                    p["spider_official_em"] = is_match
                    if is_match:
                        exact_match_count += 1
                except Exception:
                    p["spider_official_em"] = False

            spider_em = exact_match_count / len(predictions) * 100
            print(f"  Spider Official EM: {exact_match_count}/{len(predictions)} = {spider_em:.2f}%")
        except Exception as e:
            print(f"  [WARN] Spider official eval failed: {e}")

    # ---- Per-difficulty breakdown ----
    # Compute difficulty from SQL (Spider style)
    difficulty_map = {"easy": [], "medium": [], "hard": [], "extra": []}
    for p in predictions:
        d = p.get("difficulty", compute_spider_difficulty(p["gold_sql"]))
        p["difficulty"] = d
        difficulty_map.get(d, difficulty_map["extra"]).append(p)

    print(f"\n  Per-difficulty results:")
    print(f"  {'Difficulty':<12} {'Count':>6} {'String EM':>12} {'EX':>12}")
    print(f"  {'-'*48}")
    for diff in ["easy", "medium", "hard", "extra"]:
        items = difficulty_map[diff]
        if not items:
            continue
        n = len(items)
        em = sum(1 for i in items if i.get("string_em"))
        ex = sum(1 for i in items if i.get("ex_match"))
        print(f"  {diff:<12} {n:>6} {em:>5}/{n} ({em/n*100:5.1f}%) {ex:>5}/{n} ({ex/n*100:5.1f}%)")

    # ---- Save results ----
    report = {
        "mode": "spider",
        "model": str(model.config._name_or_path if hasattr(model.config, '_name_or_path') else "unknown"),
        "gold_path": gold_path,
        "total": len(predictions),
        "string_em": em_count,
        "string_em_pct": round(em_count / len(predictions) * 100, 2),
        "execution_accuracy": ex_count,
        "execution_accuracy_pct": round(ex_count / ex_total * 100, 2) if ex_total > 0 else 0,
        "spider_official_em_pct": round(spider_em, 2) if spider_em else None,
        "predictions": predictions,
    }

    if output_path:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"\n  Results saved: {output_path}")

    return report


# ============================================================
# BIRD Evaluation
# ============================================================
def eval_bird(model, tokenizer, gold_path: str, tables_path: str, db_dir: str, output_path: str):
    """BIRD 評估流程。"""
    print(f"\n{'='*70}")
    print(f"BIRD Evaluation")
    print(f"{'='*70}")

    if not os.path.exists(gold_path):
        print(f"  [ERROR] BIRD dev set not found: {gold_path}")
        print(f"  Download BIRD from: https://bird-bench.github.io/")
        return None

    with open(gold_path, "r", encoding="utf-8") as f:
        gold_data = json.load(f)
    print(f"  Dev set: {len(gold_data)} samples")

    db_schemas = load_schemas_from_tables_json(tables_path)
    print(f"  Database schemas: {len(db_schemas)}")

    # Inference
    predictions = []
    start_time = time.time()
    for idx, sample in enumerate(gold_data):
        db_id = sample.get("db_id", "")
        question = sample.get("question", "")
        gold_sql = sample.get("SQL", sample.get("query", ""))
        evidence = sample.get("evidence", "")

        schema_text = db_schemas.get(db_id, "")
        prompt = build_inference_prompt_bird(db_id, schema_text, question, evidence, tokenizer)
        pred_sql = generate_sql(model, tokenizer, prompt)

        predictions.append({
            "idx": idx,
            "db_id": db_id,
            "question": question,
            "gold_sql": gold_sql,
            "pred_sql": pred_sql,
            "evidence": evidence,
        })

        if (idx + 1) % 50 == 0:
            elapsed = time.time() - start_time
            rate = (idx + 1) / elapsed
            remaining = (len(gold_data) - idx - 1) / rate
            print(f"  [{idx+1}/{len(gold_data)}] {rate:.1f} samples/sec, ETA: {remaining/60:.1f} min")

    elapsed = time.time() - start_time
    print(f"\n  Inference done: {len(predictions)} samples in {elapsed:.0f}s")

    # ---- Execution Accuracy (BIRD 的主要指標) ----
    ex_count = 0
    ex_errors = 0
    for p in predictions:
        db_path = os.path.join(db_dir, p["db_id"], f"{p['db_id']}.sqlite")
        if not os.path.exists(db_path):
            p["ex_match"] = None
            continue

        gold_result = execute_sqlite(db_path, p["gold_sql"])
        pred_result = execute_sqlite(db_path, p["pred_sql"])

        if isinstance(gold_result, str) or isinstance(pred_result, str):
            p["ex_match"] = False
            ex_errors += 1
        elif gold_result == pred_result:
            p["ex_match"] = True
            ex_count += 1
        else:
            p["ex_match"] = False

    ex_total = sum(1 for p in predictions if p.get("ex_match") is not None)
    print(f"\n  Execution Accuracy (EX): {ex_count}/{ex_total} = {ex_count/ex_total*100:.2f}%")
    print(f"  Execution errors: {ex_errors}")

    # ---- String EM ----
    em_count = sum(1 for p in predictions
                   if normalize_sql(p["pred_sql"]) == normalize_sql(p["gold_sql"]))
    print(f"  String EM: {em_count}/{len(predictions)} = {em_count/len(predictions)*100:.2f}%")

    # ---- Per-difficulty (BIRD uses "difficulty" field) ----
    difficulty_map = defaultdict(list)
    for p in predictions:
        d = gold_data[p["idx"]].get("difficulty", "unknown")
        p["difficulty"] = d
        difficulty_map[d].append(p)

    print(f"\n  Per-difficulty EX:")
    print(f"  {'Difficulty':<12} {'Count':>6} {'EX':>12}")
    print(f"  {'-'*36}")
    for diff in ["simple", "moderate", "challenging", "unknown"]:
        items = difficulty_map.get(diff, [])
        if not items:
            continue
        n = len(items)
        ex = sum(1 for i in items if i.get("ex_match"))
        print(f"  {diff:<12} {n:>6} {ex:>5}/{n} ({ex/n*100:5.1f}%)")

    # Save
    report = {
        "mode": "bird",
        "total": len(predictions),
        "execution_accuracy": ex_count,
        "execution_accuracy_pct": round(ex_count / ex_total * 100, 2) if ex_total > 0 else 0,
        "string_em": em_count,
        "string_em_pct": round(em_count / len(predictions) * 100, 2),
        "predictions": predictions,
    }

    if output_path:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"\n  Results saved: {output_path}")

    return report


# ============================================================
# WP_M09 Evaluation
# ============================================================
def eval_wpm09(model, tokenizer, gold_path: str, output_path: str,
               db_host: str = None, db_trusted: bool = True):
    """WP_M09 評估流程。"""
    _load_wpm09_schemas()

    print(f"\n{'='*70}")
    print(f"WP_M09 Evaluation")
    print(f"{'='*70}")

    with open(gold_path, "r", encoding="utf-8") as f:
        gold_data = json.load(f)
    print(f"  Val set: {len(gold_data)} samples")

    predictions = []
    start_time = time.time()
    for idx, sample in enumerate(gold_data):
        question = sample.get("question", "")
        gold_sql = sample.get("query", "")
        gold_table = sample.get("view", "")
        difficulty = sample.get("difficulty", "unknown")

        # 推斷目標表
        inferred_table = infer_table_from_question(question)

        prompt = build_inference_prompt_wpm09(inferred_table, question, tokenizer)
        pred_sql = generate_sql(model, tokenizer, prompt, max_new_tokens=300)

        predictions.append({
            "idx": idx,
            "question": question,
            "gold_sql": gold_sql,
            "pred_sql": pred_sql,
            "gold_table": gold_table,
            "inferred_table": inferred_table,
            "difficulty": difficulty,
        })

        if (idx + 1) % 20 == 0:
            elapsed = time.time() - start_time
            rate = (idx + 1) / elapsed
            remaining = (len(gold_data) - idx - 1) / rate
            print(f"  [{idx+1}/{len(gold_data)}] {rate:.1f} samples/sec, ETA: {remaining/60:.1f} min")

    elapsed = time.time() - start_time
    print(f"\n  Inference done: {len(predictions)} samples in {elapsed:.0f}s")

    # ---- Table inference accuracy ----
    table_correct = sum(1 for p in predictions if p["gold_table"] and p["inferred_table"] == p["gold_table"])
    table_total = sum(1 for p in predictions if p["gold_table"])
    if table_total > 0:
        print(f"  Table inference: {table_correct}/{table_total} = {table_correct/table_total*100:.1f}%")

    # ---- String EM ----
    em_count = 0
    for p in predictions:
        if normalize_sql(p["pred_sql"]) == normalize_sql(p["gold_sql"]):
            em_count += 1
            p["string_em"] = True
        else:
            p["string_em"] = False
    print(f"  String EM: {em_count}/{len(predictions)} = {em_count/len(predictions)*100:.2f}%")

    # ---- Execution Accuracy ----
    ex_count = 0
    ex_errors = 0
    if db_host:
        for p in predictions:
            gold_result = execute_sqlserver(p["gold_sql"], db_host, db_trusted)
            pred_result = execute_sqlserver(p["pred_sql"], db_host, db_trusted)

            if isinstance(gold_result, str) or isinstance(pred_result, str):
                p["ex_match"] = False
                ex_errors += 1
            elif gold_result == pred_result:
                p["ex_match"] = True
                ex_count += 1
            else:
                p["ex_match"] = False

        print(f"  Execution Accuracy: {ex_count}/{len(predictions)} = {ex_count/len(predictions)*100:.2f}%")
        print(f"  Execution errors: {ex_errors}")
    else:
        print("  [INFO] No --db-host provided, skipping Execution Accuracy")

    # ---- Per-view breakdown ----
    view_stats = defaultdict(lambda: {"total": 0, "em": 0, "ex": 0})
    for p in predictions:
        view = p.get("gold_table", "unknown")
        view_stats[view]["total"] += 1
        if p.get("string_em"):
            view_stats[view]["em"] += 1
        if p.get("ex_match"):
            view_stats[view]["ex"] += 1

    print(f"\n  Per-view results:")
    print(f"  {'View':<20} {'N':>4} {'EM':>10} {'EX':>10}")
    print(f"  {'-'*50}")
    for view in sorted(view_stats.keys()):
        s = view_stats[view]
        n = s["total"]
        em_str = f"{s['em']}/{n} ({s['em']/n*100:.1f}%)"
        ex_str = f"{s['ex']}/{n} ({s['ex']/n*100:.1f}%)" if db_host else "N/A"
        print(f"  {view:<20} {n:>4} {em_str:>10} {ex_str:>10}")

    # ---- Per-difficulty breakdown ----
    diff_stats = defaultdict(lambda: {"total": 0, "em": 0, "ex": 0})
    for p in predictions:
        d = p.get("difficulty", "unknown")
        diff_stats[d]["total"] += 1
        if p.get("string_em"):
            diff_stats[d]["em"] += 1
        if p.get("ex_match"):
            diff_stats[d]["ex"] += 1

    print(f"\n  Per-difficulty results:")
    print(f"  {'Difficulty':<12} {'N':>4} {'EM':>10} {'EX':>10}")
    print(f"  {'-'*42}")
    for d in ["easy", "medium", "hard", "unknown"]:
        s = diff_stats.get(d)
        if not s:
            continue
        n = s["total"]
        em_str = f"{s['em']}/{n} ({s['em']/n*100:.1f}%)"
        ex_str = f"{s['ex']}/{n} ({s['ex']/n*100:.1f}%)" if db_host else "N/A"
        print(f"  {d:<12} {n:>4} {em_str:>10} {ex_str:>10}")

    # Save
    report = {
        "mode": "wp_m09",
        "total": len(predictions),
        "string_em": em_count,
        "string_em_pct": round(em_count / len(predictions) * 100, 2),
        "execution_accuracy": ex_count if db_host else None,
        "execution_accuracy_pct": round(ex_count / len(predictions) * 100, 2) if db_host else None,
        "table_inference_accuracy": round(table_correct / table_total * 100, 2) if table_total > 0 else None,
        "predictions": predictions,
    }

    if output_path:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"\n  Results saved: {output_path}")

    return report


# ============================================================
# Difficulty computation (Spider style)
# ============================================================
def compute_spider_difficulty(sql: str) -> str:
    """根據 SQL 複雜度計算 Spider 風格的難度。"""
    sql_upper = sql.upper()

    component1 = 0  # where, group, order, limit, join, or, like
    component2 = 0  # except, union, intersect

    if " WHERE " in sql_upper: component1 += 1
    if " GROUP BY " in sql_upper: component1 += 1
    if " ORDER BY " in sql_upper: component1 += 1
    if " LIMIT " in sql_upper or " TOP " in sql_upper: component1 += 1
    if " JOIN " in sql_upper: component1 += 1
    if " OR " in sql_upper: component1 += 1
    if " LIKE " in sql_upper: component1 += 1

    if " EXCEPT " in sql_upper: component2 += 1
    if " UNION " in sql_upper: component2 += 1
    if " INTERSECT " in sql_upper: component2 += 1

    # Spider difficulty rules
    if component2 >= 1:
        return "extra"
    elif component1 >= 3:
        return "hard"
    elif component1 >= 2:
        return "medium"
    else:
        return "easy"


# ============================================================
# Main
# ============================================================
def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark NL-to-SQL Evaluation")
    parser.add_argument("--mode", type=str, required=True,
                        choices=["spider", "bird", "wp_m09"],
                        help="Evaluation benchmark")
    parser.add_argument("--model", type=str, required=True, help="Model path")
    parser.add_argument("--gold", type=str, required=True, help="Gold data path (dev.json / val.json)")
    parser.add_argument("--tables", type=str, default=None, help="tables.json path (for Spider/BIRD)")
    parser.add_argument("--db-dir", type=str, default=None, help="Database directory (for Spider/BIRD)")
    parser.add_argument("--db-host", type=str, default=None, help="SQL Server host (for WP_M09)")
    parser.add_argument("--db-trusted", action="store_true", help="Use Windows Auth (for WP_M09)")
    parser.add_argument("--output", type=str, default=None, help="Output JSON path")
    parser.add_argument("--max-seq-len", type=int, default=2048, help="Max sequence length for inference")
    return parser.parse_args()


def main():
    args = parse_args()

    tokenizer, model = load_model(args.model, args.max_seq_len)

    if args.mode == "spider":
        tables = args.tables or os.path.join(SPIDER_DIR, "tables.json")
        db_dir = args.db_dir or os.path.join(SPIDER_DIR, "database")
        output = args.output or f"outputs/eval_spider_{DATE_STR}.json"
        eval_spider(model, tokenizer, args.gold, tables, db_dir, output)

    elif args.mode == "bird":
        tables = args.tables or os.path.join(BIRD_DIR, "dev", "dev_tables.json")
        db_dir = args.db_dir or os.path.join(BIRD_DIR, "dev", "dev_databases")
        output = args.output or f"outputs/eval_bird_{DATE_STR}.json"
        eval_bird(model, tokenizer, args.gold, tables, db_dir, output)

    elif args.mode == "wp_m09":
        output = args.output or f"outputs/eval_wpm09_{DATE_STR}.json"
        eval_wpm09(model, tokenizer, args.gold, output,
                   db_host=args.db_host, db_trusted=args.db_trusted)


if __name__ == "__main__":
    main()
