# train__dora_spider_v0321.py
# 使用 DoRA (Weight-Decomposed LoRA) 微調 Llama-3.1-8B-Instruct
#
# 改動重點（vs 0320）：
#   - 合併 0317 訓練資料 + v3 清理版訓練資料
#   - 0317 資料：train_spider_WP_M09(1014) + train_claude_en_2000(1748) = 2762 筆
#   - v3 清理版：train_claude_en_2000_v3_clean(1999) = 1999 筆
#   - 總計：~4761 筆（去重後）
#   - 解決 v3 模型 SQL 風格與驗證集不一致的問題
#   - v3 清理版已移除分號、AS 別名、多餘 ORDER BY
#
# 執行方式:
#   python train__dora_spider_v0321.py
#
# 輸出:
#   outputs/models/wp_m09_dora_0321_combined/final_model/

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

# Multiple training data files to combine
TRAIN_PATHS  = [
    r"data\wp_m09\train_spider_WP_M09.json",        # 1014 (0317 best)
    r"data\wp_m09\train_claude_en_2000.json",         # 1748 (0317 best)
    r"data\wp_m09\train_claude_en_2000_v3_clean.json", # 1999 (v3 cleaned)
]

DATE_STR     = "0321"
OUTPUT_DIR   = f"outputs/models/wp_m09_dora_{DATE_STR}_combined"
FINAL_MODEL  = os.path.join(OUTPUT_DIR, "final_model")

# ---- DoRA / LoRA ----
LORA_R        = 16
LORA_ALPHA    = 32
LORA_DROPOUT  = 0.05
USE_DORA      = True

# ---- Training hyperparams ----
NUM_EPOCHS    = 6             # More data -> fewer epochs to avoid overfit
BATCH_SIZE    = 4
GRAD_ACCUM    = 4             # effective batch = 16
LEARNING_RATE = 6e-5          # Lower LR for larger combined dataset
MAX_SEQ_LEN   = 512
LR_SCHEDULER  = "cosine"
WARMUP_RATIO  = 0.06
WEIGHT_DECAY  = 0.01
# ============================================================

# ============================================================
# TABLE_NOTES
# ============================================================
TABLE_NOTES = {
    "WP_vProduct":   "Note: pNo is a sequential product number (1, 2, 3...), NOT a date. "
                     "This view has no date filtering capability. "
                     "CRITICAL: This view has NO isDel or dtlIsDel column -- never add them.",
    "WP_vInventory": "Note: pNo is a sequential product number, not a date. "
                     "This view has no date filtering capability. "
                     "CRITICAL: This view has NO isDel or dtlIsDel column -- never add them.",
    "WP_vProvider":  "Note: Supplier info table. Use sn as the primary key (NOT pvSn -- pvSn does NOT exist in this view). "
                     "Use isStop to check if a supplier is active (isStop='N') or inactive (isStop='Y'). "
                     "CRITICAL: This view has NO isDel, dtlIsDel, or pvSn column -- never use them.",
    "WP_vTransfer":  "Note: Filter by date using LEFT(TransferId,8)='YYYYMMDD' "
                     "or LEFT(TransferId,6)='YYYYMM'. ALWAYS add isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vAcctIn":    "Note: Filter by date using LEFT(acctInId,8)='YYYYMMDD' or LEFT(acctInId,6)='YYYYMM'. "
                     "ALWAYS add isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vAcctOut":   "Note: Filter by date using LEFT(acctOutId,8)='YYYYMMDD' or LEFT(acctOutId,6)='YYYYMM'. "
                     "ALWAYS add isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vOutStock":  "Note: Filter by date using LEFT(OutStkId,8)='YYYYMMDD' or LEFT(OutStkId,6)='YYYYMM'. "
                     "ALWAYS add isDel='N' AND dtlIsDel='N' for active records.",
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
# Data preprocessing
# ============================================================
def build_prompt(sample: dict) -> str:
    table = extract_table_from_sql(sample.get("query", ""))
    if not table:
        table = sample.get("db_id", "WP_M09")
    question = sample.get("question", "")
    sql      = sample.get("query", "")
    note     = TABLE_NOTES.get(table, "")

    lines = [f"Table: {table}"]
    if note:
        lines.append(note)
    lines.append(f"Question: {question}")
    lines.append(f"SQL: {sql}")
    return "\n".join(lines)


def load_and_merge_datasets(paths: list):
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

    texts = [{"text": build_prompt(s)} for s in deduped]
    dataset = Dataset.from_list(texts)

    print(f"\n  Sample prompt:")
    print("-" * 50)
    print(dataset[0]['text'])
    print("-" * 50)

    from collections import Counter
    table_cnt = Counter(extract_table_from_sql(s.get("query","")) for s in deduped)
    left_cnt = sum(1 for s in deduped if "LEFT(" in s.get("query","").upper())
    like_cnt = sum(1 for s in deduped if "LIKE"  in s.get("query","").upper())
    isdel_cnt = sum(1 for s in deduped if "isDel" in s.get("query",""))
    groupby_cnt = sum(1 for s in deduped if "GROUP BY" in s.get("query","").upper())
    top_cnt = sum(1 for s in deduped if "TOP " in s.get("query","").upper())
    distinct_cnt = sum(1 for s in deduped if "DISTINCT" in s.get("query","").upper())

    print(f"\n  View distribution: {dict(table_cnt)}")
    print(f"  LEFT(): {left_cnt}  LIKE: {like_cnt}  isDel: {isdel_cnt}")
    print(f"  GROUP BY: {groupby_cnt}  TOP: {top_cnt}  DISTINCT: {distinct_cnt}")

    # Sanity checks
    bad_isdel = [s for s in deduped
           if re.search(r'\bisdel\b|\bdtlisdel\b', s.get("query",""), re.IGNORECASE)
           and any(v in s.get("query","") for v in ["WP_vInventory","WP_vProduct","WP_vProvider"])]
    bad_pvsn = [s for s in deduped
           if "pvSn" in s.get("query","")
           and "WP_vProvider" in s.get("query","")]

    if bad_isdel:
        print(f"\n[WARN] {len(bad_isdel)} samples have illegal isDel!")
    else:
        print("\n[OK] WP_vInventory/WP_vProduct/WP_vProvider: no illegal isDel")

    if bad_pvsn:
        print(f"[WARN] {len(bad_pvsn)} samples have illegal pvSn in WP_vProvider!")
    else:
        print("[OK] WP_vProvider: no illegal pvSn")

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
    print(f"  Warmup ratio:     {WARMUP_RATIO}")
    print(f"  Weight decay:     {WEIGHT_DECAY}")
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
        "train_format":    "Spider 1.0",
        "train_script":    f"train__dora_spider_v{DATE_STR}.py",
        "method":          "DoRA" if USE_DORA else "LoRA",
        "lora_r":          LORA_R,
        "lora_alpha":      LORA_ALPHA,
        "epochs":          NUM_EPOCHS,
        "effective_batch":  BATCH_SIZE * GRAD_ACCUM,
        "learning_rate":   LEARNING_RATE,
        "weight_decay":    WEIGHT_DECAY,
        "warmup_ratio":    WARMUP_RATIO,
        "date":            DATE_STR,
        "changes":         "Combined 0317 best training data + v3 clean data, "
                           "deduped by normalized SQL, "
                           "v3 cleaned: removed semicolons/AS aliases/unnecessary ORDER BY, "
                           "lower LR (6e-5) and fewer epochs (6) for larger dataset",
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
    print(f"WP_M09 DoRA Training v{DATE_STR} (Combined 0317 + v3 clean)")
    print(f"Training data: {len(TRAIN_PATHS)} files combined")
    print("=" * 60)

    dataset, n_samples  = load_and_merge_datasets(TRAIN_PATHS)
    tokenizer, model    = load_model_and_tokenizer()
    model               = apply_dora(model)
    trainer             = train(model, tokenizer, dataset)
    save_model(trainer, tokenizer, n_samples)

    print("\nTraining complete!")
    print(f"  Model path: {FINAL_MODEL}")
    print(f"  Evaluate: python eval__em_and_execution_accuracy.py "
          f"--model {FINAL_MODEL} "
          f"--gold data/wp_m09/val_claude_en_spider_v2.json "
          f"--output outputs/evaluation_{DATE_STR}_val.json")


if __name__ == "__main__":
    main()
