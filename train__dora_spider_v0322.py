# train__dora_spider_v0322.py
# 使用 DoRA (Weight-Decomposed LoRA) 微調 Llama-3.1-8B-Instruct
#
# 改動重點（vs 0321）：
#   1. 新訓練格式：Llama-3.1 Chat Template + 單表精簡 Schema
#      - system prompt 只包含「目標表」的 CREATE TABLE（從 gold SQL 提取）
#      - 附帶 7 個 View 名稱列表（讓模型知道有哪些表）
#      - 包含表專屬商業規則（isDel/dtlIsDel、日期篩選、子查詢去重）
#      - 與 inference 格式一致，消除 format mismatch
#   2. 只使用 spider + claude_en（0317 最佳組合），不加 v3
#   3. MAX_SEQ_LEN=640（單表 schema 最長約 455 tokens，留 185 tokens 給 SQL）
#
# Token 長度預估：
#   - 最長（WP_vOutStock hard query）: ~455 tokens
#   - 最短（WP_vTransfer easy query）: ~307 tokens
#   - 平均: ~380 tokens，全部在 640 以內
#
# 執行方式:
#   python train__dora_spider_v0322.py
#
# 輸出:
#   outputs/models/wp_m09_dora_0322_schema/final_model/

import json
import os
import re
import torch
from datetime import datetime
from datasets import Dataset
from transformers import (
    AutoTokenizer,
    AutoModelForCausalLM,
    BitsAndBytesConfig,
)
from peft import LoraConfig, get_peft_model, TaskType
from trl import SFTTrainer, SFTConfig

# ============================================================
# Settings
# ============================================================
MODEL_PATH   = "meta-llama/Llama-3.1-8B-Instruct"

# 0317 最佳訓練資料組合（不再加 v3，避免雜訊）
TRAIN_PATHS  = [
    r"data\wp_m09\train_spider_WP_M09.json",        # 1014
    r"data\wp_m09\train_claude_en_2000.json",         # 1748
]

DATE_STR     = "0322"
OUTPUT_DIR   = f"outputs/models/wp_m09_dora_{DATE_STR}_schema"
FINAL_MODEL  = os.path.join(OUTPUT_DIR, "final_model")

# ---- DoRA / LoRA ----
LORA_R        = 16
LORA_ALPHA    = 32
LORA_DROPOUT  = 0.05
USE_DORA      = True

# ---- Training hyperparams ----
NUM_EPOCHS    = 6
BATCH_SIZE    = 4
GRAD_ACCUM    = 4              # effective batch = 16
LEARNING_RATE = 5e-5
MAX_SEQ_LEN   = 640            # 單表 schema 約 300-455 tokens + SQL 答案
LR_SCHEDULER  = "cosine"
WARMUP_RATIO  = 0.06
WEIGHT_DECAY  = 0.01
# ============================================================

# ============================================================
# 每個 View 的精簡 Schema（只保留常用欄位，移除罕用欄位）
# ============================================================
VIEW_SCHEMAS = {
    "WP_vAcctIn": """CREATE TABLE WP_vAcctIn (
  -- Accounts Receivable. isDel='N' header, dtlIsDel='N' detail. Date: LEFT(acctInId,8)='YYYYMMDD' or LEFT(acctInId,6)='YYYYMM'.
  -- SUM/AVG on amount: use (SELECT DISTINCT acctInId, amount FROM ... WHERE isDel='N') sub
  sn INT, acctInId NVARCHAR, acctInDate DATETIME, amount DECIMAL, memo NVARCHAR,
  empId NVARCHAR, isDel CHAR, dtlSn INT, OutStkId NVARCHAR, outStkAmtTotal DECIMAL,
  dtlIsDel CHAR, memSn INT, memId NVARCHAR, memName NVARCHAR,
  pNo INT, pBarcode NVARCHAR, pName NVARCHAR, pNameS NVARCHAR,
  oStkDtlAmt DECIMAL, oStkDtlQty DECIMAL, oStkDtlAmtTotal DECIMAL,
  dtlDiscnt DECIMAL, dtlDiscntShare DECIMAL, discount DECIMAL, discountShare DECIMAL
);""",

    "WP_vAcctOut": """CREATE TABLE WP_vAcctOut (
  -- Accounts Payable. isDel='N' header, dtlIsDel='N' detail. Date: LEFT(acctOutId,8)='YYYYMMDD' or LEFT(acctOutId,6)='YYYYMM'.
  -- SUM/AVG on amount: use (SELECT DISTINCT acctOutId, amount FROM ... WHERE isDel='N') sub
  sn INT, acctOutId NVARCHAR, acctOutDate DATETIME, amount DECIMAL, transAmt DECIMAL,
  memo NVARCHAR, empId NVARCHAR, empName NVARCHAR, isDel CHAR,
  dtlSn INT, InStkId NVARCHAR, dtlAmt DECIMAL, qty DECIMAL, amtTotal DECIMAL,
  dtlIsDel CHAR, pNo INT, pName NVARCHAR, pNameS NVARCHAR, pBarcode NVARCHAR,
  pvId NVARCHAR, pvName NVARCHAR, pvNameS NVARCHAR, pvSn INT, pvDiscount DECIMAL,
  inStkAmt DECIMAL, inStkAmtTotal DECIMAL, payType NVARCHAR
);""",

    "WP_vOutStock": """CREATE TABLE WP_vOutStock (
  -- Sales/Outbound Stock. isDel='N' header, dtlIsDel='N' detail. Date: LEFT(OutStkId,8)='YYYYMMDD' or LEFT(OutStkId,6)='YYYYMM'.
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
  -- Date: LEFT(TransferId,8)='YYYYMMDD' or LEFT(TransferId,6)='YYYYMM'. Use SUM(qty) or SUM(costAvg*qty) for value.
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
  -- Supplier/Provider. NO isDel/dtlIsDel. Use isStop='N' (active) or isStop='Y' (stopped). NO date filtering.
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

# 7 個 View 的簡短列表（讓模型知道有哪些表可選）
VIEW_LIST = "Available views: WP_vAcctIn (receivable), WP_vAcctOut (payable), WP_vOutStock (sales/outbound), WP_vTransfer (transfer), WP_vInventory (inventory), WP_vProduct (product), WP_vProvider (supplier)"

# 每個 View 的專屬規則（精簡版）
VIEW_RULES = {
    "WP_vAcctIn":    "Rules: isDel='N' for active header, add dtlIsDel='N' for detail columns. Date: LEFT(acctInId,8)='YYYYMMDD'. Chinese: N prefix. Prefix: WP_M09.dbo.WP_vAcctIn. SUM/AVG on amount: use (SELECT DISTINCT acctInId, amount FROM ... WHERE isDel='N') sub.",
    "WP_vAcctOut":   "Rules: isDel='N' for active header, add dtlIsDel='N' for detail columns. Date: LEFT(acctOutId,8)='YYYYMMDD'. Chinese: N prefix. Prefix: WP_M09.dbo.WP_vAcctOut. SUM/AVG on amount: use (SELECT DISTINCT acctOutId, amount FROM ... WHERE isDel='N') sub.",
    "WP_vOutStock":  "Rules: isDel='N' for active header, add dtlIsDel='N' for detail columns. Date: LEFT(OutStkId,8)='YYYYMMDD'. Chinese: N prefix. Prefix: WP_M09.dbo.WP_vOutStock. SUM/AVG on amount: use (SELECT DISTINCT OutStkId, amount FROM ... WHERE isDel='N') sub.",
    "WP_vTransfer":  "Rules: isDel='N' AND dtlIsDel='N' almost always both needed. No header amount, use SUM(qty) or SUM(costAvg*qty). Date: LEFT(TransferId,8)='YYYYMMDD'. Chinese: N prefix. Prefix: WP_M09.dbo.WP_vTransfer.",
    "WP_vInventory": "Rules: NO isDel/dtlIsDel — never add them. pNo is sequential, not a date. Chinese: N prefix. Prefix: WP_M09.dbo.WP_vInventory.",
    "WP_vProduct":   "Rules: NO isDel/dtlIsDel — never add them. pNo is sequential, not a date. Chinese: N prefix. Prefix: WP_M09.dbo.WP_vProduct.",
    "WP_vProvider":  "Rules: NO isDel/dtlIsDel — use isStop='N' (active) / isStop='Y' (stopped). Chinese: N prefix. Prefix: WP_M09.dbo.WP_vProvider.",
}


# ============================================================
# Utils
# ============================================================
def extract_table_from_sql(sql: str) -> str:
    m = re.search(r'WP_M09\.dbo\.(\w+)', sql)
    if m:
        return m.group(1)
    m = re.search(r'\[WP_M09\]\.\[dbo\]\.\[(\w+)\]', sql)
    return m.group(1) if m else ""


def normalize_query(sql: str) -> str:
    """Normalize SQL for deduplication."""
    s = sql.strip().rstrip(';').strip()
    s = re.sub(r'\s+', ' ', s)
    return s.upper()


# ============================================================
# Data preprocessing — 單表 Chat Template 格式
# ============================================================
def build_system_prompt(table: str) -> str:
    """為特定表建構 system prompt（包含單表 schema + 規則）。"""
    schema = VIEW_SCHEMAS.get(table, "")
    rules = VIEW_RULES.get(table, "")

    parts = [
        "You are an expert T-SQL assistant for WP_M09 (SQL Server). Generate ONLY the SQL query.",
        VIEW_LIST,
        schema,
        rules,
    ]
    return "\n\n".join(parts)


def build_chat_prompt(sample: dict, tokenizer) -> str:
    """
    使用 Llama-3.1 Chat Template 建構訓練資料。
    每筆資料只包含目標表的 schema。
    """
    table = extract_table_from_sql(sample.get("query", ""))
    if not table:
        table = sample.get("db_id", "WP_M09")

    question = sample.get("question", "")
    sql      = sample.get("query", "").strip().rstrip(';').strip()

    system_prompt = build_system_prompt(table)

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": question},
        {"role": "assistant", "content": sql},
    ]

    text = tokenizer.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=False,
    )
    return text


def load_and_merge_datasets(paths: list, tokenizer):
    all_samples = []
    for path in paths:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        print(f"  Loaded {path}: {len(raw)} samples")
        all_samples.extend(raw)

    print(f"\n  Total before dedup: {len(all_samples)}")

    # Deduplicate by normalized SQL
    seen_sql = set()
    deduped = []
    dup_count = 0
    for s in all_samples:
        norm = normalize_query(s.get("query", ""))
        if norm not in seen_sql:
            seen_sql.add(norm)
            deduped.append(s)
        else:
            dup_count += 1

    print(f"  Duplicates removed: {dup_count}")
    print(f"  Total after dedup: {len(deduped)}")

    # 建構 chat template 格式的訓練資料
    texts = [{"text": build_chat_prompt(s, tokenizer)} for s in deduped]
    dataset = Dataset.from_list(texts)

    print(f"\n  Sample prompt (first 600 chars):")
    print("-" * 50)
    print(dataset[0]['text'][:600])
    print("-" * 50)

    # 檢查 token 長度分布
    lengths = []
    for t in texts:
        toks = tokenizer(t['text'], truncation=False)
        lengths.append(len(toks['input_ids']))

    import statistics
    print(f"\n  Token length stats:")
    print(f"    Min: {min(lengths)}, Max: {max(lengths)}, "
          f"Mean: {statistics.mean(lengths):.0f}, Median: {statistics.median(lengths):.0f}")
    over_max = sum(1 for l in lengths if l > MAX_SEQ_LEN)
    print(f"    Over {MAX_SEQ_LEN}: {over_max} ({over_max/len(lengths)*100:.1f}%)")
    if over_max > 0:
        over_samples = [(l, i) for i, l in enumerate(lengths) if l > MAX_SEQ_LEN]
        over_samples.sort(reverse=True)
        print(f"    Top 5 longest:")
        for l, idx in over_samples[:5]:
            table = extract_table_from_sql(deduped[idx].get("query", ""))
            print(f"      [{idx}] {l} tokens — {table}: {deduped[idx]['question'][:60]}")

    from collections import Counter
    table_cnt = Counter(extract_table_from_sql(s.get("query","")) for s in deduped)
    subquery_cnt = sum(1 for s in deduped if s.get("query","").upper().count("SELECT") >= 2)
    isdel_cnt = sum(1 for s in deduped if "isDel" in s.get("query",""))

    print(f"\n  View distribution: {dict(table_cnt)}")
    print(f"  Subqueries: {subquery_cnt}  isDel: {isdel_cnt}")

    # Sanity checks
    bad_isdel = [s for s in deduped
           if re.search(r'\bisdel\b|\bdtlisdel\b', s.get("query",""), re.IGNORECASE)
           and any(v in s.get("query","") for v in ["WP_vInventory","WP_vProduct","WP_vProvider"])]

    if bad_isdel:
        print(f"\n[WARN] {len(bad_isdel)} samples have illegal isDel!")
    else:
        print("\n[OK] WP_vInventory/WP_vProduct/WP_vProvider: no illegal isDel")

    return dataset, len(deduped)


# ============================================================
# Model loading
# ============================================================
def load_model_and_tokenizer():
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
    tokenizer.model_max_length = MAX_SEQ_LEN

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
def train(model, tokenizer, dataset):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    sft_cfg = SFTConfig(
        output_dir=OUTPUT_DIR,
        num_train_epochs=NUM_EPOCHS,
        per_device_train_batch_size=BATCH_SIZE,
        gradient_accumulation_steps=GRAD_ACCUM,
        learning_rate=LEARNING_RATE,
        lr_scheduler_type=LR_SCHEDULER,
        warmup_ratio=WARMUP_RATIO,
        weight_decay=WEIGHT_DECAY,
        optim="paged_adamw_8bit",
        bf16=True,
        logging_steps=10,
        save_strategy="epoch",
        save_total_limit=3,
        report_to="none",
        dataloader_num_workers=0,
        dataset_text_field="text",
        packing=False,
    )

    trainer = SFTTrainer(
        model=model,
        processing_class=tokenizer,
        train_dataset=dataset,
        args=sft_cfg,
    )

    effective_batch = BATCH_SIZE * GRAD_ACCUM
    steps_per_epoch = len(dataset) // effective_batch
    total_steps     = steps_per_epoch * NUM_EPOCHS

    print(f"\nStarting training...")
    print(f"  Epochs:           {NUM_EPOCHS}")
    print(f"  Batch size:       {BATCH_SIZE} x {GRAD_ACCUM} (accum) = {effective_batch} (effective)")
    print(f"  Steps/epoch:      ~{steps_per_epoch}")
    print(f"  Total steps:      ~{total_steps}")
    print(f"  Learning rate:    {LEARNING_RATE}  (cosine decay)")
    print(f"  Max seq len:      {MAX_SEQ_LEN}")
    print(f"  Output dir:       {OUTPUT_DIR}\n")

    # Resume from checkpoint if available
    last_ckpt = None
    if os.path.isdir(OUTPUT_DIR):
        ckpts = [d for d in os.listdir(OUTPUT_DIR) if d.startswith("checkpoint-")]
        if ckpts:
            last_ckpt = os.path.join(OUTPUT_DIR, sorted(ckpts, key=lambda x: int(x.split("-")[1]))[-1])
            print(f"  Resuming from: {last_ckpt}")

    trainer.train(resume_from_checkpoint=last_ckpt)
    return trainer


# ============================================================
# Save model
# ============================================================
def save_model(trainer, tokenizer, train_samples: int):
    os.makedirs(FINAL_MODEL, exist_ok=True)

    trainer.model.save_pretrained(FINAL_MODEL)
    tokenizer.save_pretrained(FINAL_MODEL)

    info = {
        "base_model":      MODEL_PATH,
        "train_data":      str(TRAIN_PATHS),
        "train_samples":   train_samples,
        "train_format":    "Chat Template + Single-Table Schema",
        "train_script":    f"train__dora_spider_v{DATE_STR}.py",
        "method":          "DoRA" if USE_DORA else "LoRA",
        "lora_r":          LORA_R,
        "lora_alpha":      LORA_ALPHA,
        "epochs":          NUM_EPOCHS,
        "effective_batch":  BATCH_SIZE * GRAD_ACCUM,
        "learning_rate":   LEARNING_RATE,
        "weight_decay":    WEIGHT_DECAY,
        "warmup_ratio":    WARMUP_RATIO,
        "max_seq_len":     MAX_SEQ_LEN,
        "date":            DATE_STR,
        "changes":         "Single-table schema per sample (not all 7 views). "
                           "Llama-3.1 Chat Template. "
                           "Compact schema with key columns only. "
                           "View list in system prompt for table awareness. "
                           "Table-specific rules (isDel, date filter, subquery dedup). "
                           "MAX_SEQ_LEN=640, all samples fit within limit.",
        "final_loss":      round(trainer.state.log_history[-1].get("loss", 0), 4),
    }
    with open(os.path.join(FINAL_MODEL, "training_info.json"), "w") as f:
        json.dump(info, f, indent=2)

    print(f"\nModel saved: {FINAL_MODEL}")
    print("Training info:")
    for k, v in info.items():
        print(f"  {k}: {v}")


# ============================================================
# Main
# ============================================================
def main():
    print("=" * 60)
    print(f"WP_M09 DoRA Training v{DATE_STR} (Single-Table Schema Chat Template)")
    print(f"Training data: {len(TRAIN_PATHS)} files")
    print("=" * 60)

    tokenizer, model    = load_model_and_tokenizer()
    dataset, n_samples  = load_and_merge_datasets(TRAIN_PATHS, tokenizer)
    model               = apply_dora(model)
    trainer             = train(model, tokenizer, dataset)
    save_model(trainer, tokenizer, n_samples)

    print("\nTraining complete!")
    print(f"  Model path: {FINAL_MODEL}")
    print(f"  Evaluate: python eval__em_and_execution_accuracy_v2.py "
          f"--model {FINAL_MODEL} "
          f"--gold data/wp_m09/val_claude_en_spider_v2.json "
          f"--output outputs/evaluation_{DATE_STR}_val.json "
          f"--db-host \"SHANE\\SQLEXPRESS\" --db-trusted")


if __name__ == "__main__":
    main()
