# train_dora_spider0312.py
# 使用 DoRA (Weight-Decomposed LoRA) 微調 Llama-3.1-8B-Instruct
# 訓練資料: data/wp_m09/train_spider_WP_M09.json  (Spider 1 格式, 0312版)
#
# 執行方式:
#   python train_dora_spider0312.py
#
# 輸出:
#   outputs/models/wp_m09_dora_0312_spider/final_model/

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
# 設定區
# ============================================================
MODEL_PATH   = "meta-llama/Llama-3.1-8B-Instruct"
TRAIN_PATH   = r"data\wp_m09\train_spider_WP_M09.json"
DATE_STR     = "0312"
OUTPUT_DIR   = f"outputs/models/wp_m09_dora_{DATE_STR}_spider"
FINAL_MODEL  = os.path.join(OUTPUT_DIR, "final_model")

# ---- DoRA / LoRA 超參數 ----
LORA_R        = 16
LORA_ALPHA    = 32
LORA_DROPOUT  = 0.05
USE_DORA      = True      # ← False 退回純 LoRA

# ---- 訓練超參數 ----
NUM_EPOCHS    = 15
BATCH_SIZE    = 4         # per-device batch
GRAD_ACCUM    = 4         # effective batch = 4×4 = 16
LEARNING_RATE = 1e-4
MAX_SEQ_LEN   = 512
LR_SCHEDULER  = "cosine"
WARMUP_RATIO  = 0.08
WEIGHT_DECAY  = 0.01
# ============================================================

# ============================================================
# 特定表的額外說明 (TABLE_NOTES)
# ============================================================
TABLE_NOTES = {
    "WP_vProduct":   "Note: pNo is product number, NOT a date field. Filter by date using pNo LIKE 'YYYYMMDD%'. No separate date column.",
    "WP_vInventory": "Note: Filter by date using pNo LIKE 'YYYYMMDD%'. No separate date column.",
    "WP_vTransfer":  "Note: Filter by date using LEFT(TransferId,8)='YYYYMMDD' or LEFT(TransferId,6)='YYYYMM'.",
    "WP_vAcctIn":    "Note: Filter by date using LEFT(acctInId,8)='YYYYMMDD'. isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vAcctOut":   "Note: Filter by date using LEFT(acctOutId,8)='YYYYMMDD'. isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vOutStock":  "Note: Filter by date using LEFT(OutStkId,8)='YYYYMMDD'. isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vProvider":  "Note: isSale='Y' means active provider. Boolean fields use 'Y'/'N' encoding.",
}


# ============================================================
# 工具函式：從 SQL 提取主要 table/view 名稱
# ============================================================
def extract_table_from_sql(sql: str) -> str:
    """從 SQL 中提取第一個 [WP_M09].[dbo].[XXX] 裡的 view/table 名稱。"""
    m = re.search(r'\[WP_M09\]\.\[dbo\]\.\[(\w+)\]', sql)
    return m.group(1) if m else ""


# ============================================================
# 資料前處理
# ============================================================
def build_prompt(sample: dict) -> str:
    """
    Spider 格式 prompt（訓練時含 SQL，推論時截到 SQL: 讓模型補全）：
      Table: <table_view>
      Note: <table_note>        ← 有對應表才加
      Question: <question>
      SQL: <query>
    """
    # 從 SQL 提取實際的 view 名稱，比 db_id='WP_M09' 更精確
    table    = extract_table_from_sql(sample.get("query", ""))
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


def load_dataset(path: str) -> Dataset:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    texts = [{"text": build_prompt(s)} for s in raw]
    dataset = Dataset.from_list(texts)

    print(f"訓練集載入: {len(dataset)} 筆")
    print(f"\n範例 prompt:")
    print("-" * 50)
    print(dataset[0]['text'])
    print("-" * 50)

    # 統計資訊
    from collections import Counter
    table_cnt = Counter(extract_table_from_sql(s.get("query","")) for s in raw)
    left_cnt = sum(1 for s in raw if "LEFT(" in s.get("query","").upper())
    like_cnt = sum(1 for s in raw if "LIKE"  in s.get("query","").upper())
    print(f"\n表/View 分佈: {dict(table_cnt)}")
    print(f"含 LEFT(): {left_cnt}  含 LIKE: {like_cnt}")
    return dataset


# ============================================================
# 模型載入
# ============================================================
def load_model_and_tokenizer():
    print(f"\n載入基礎模型: {MODEL_PATH} ...")

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

    print("基礎模型載入完成 ✅")
    return tokenizer, model


# ============================================================
# DoRA 設定
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
    print(f"微調方法: {mode}  (r={LORA_R}, alpha={LORA_ALPHA})")
    return model


# ============================================================
# 訓練
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

    print(f"\n開始訓練...")
    print(f"  Epochs:           {NUM_EPOCHS}")
    print(f"  Batch size:       {BATCH_SIZE} × {GRAD_ACCUM} (accum) = {effective_batch} (effective)")
    print(f"  Steps/epoch:      ~{steps_per_epoch}")
    print(f"  Total steps:      ~{total_steps}")
    print(f"  Learning rate:    {LEARNING_RATE}  (cosine decay)")
    print(f"  Warmup ratio:     {WARMUP_RATIO}  (~{int(total_steps*WARMUP_RATIO)} steps)")
    print(f"  Weight decay:     {WEIGHT_DECAY}")
    print(f"  Max seq len:      {MAX_SEQ_LEN}")
    print(f"  輸出目錄:         {OUTPUT_DIR}\n")

    trainer.train()
    return trainer


# ============================================================
# 儲存權重
# ============================================================
def save_model(trainer, tokenizer):
    os.makedirs(FINAL_MODEL, exist_ok=True)

    trainer.model.save_pretrained(FINAL_MODEL)
    tokenizer.save_pretrained(FINAL_MODEL)

    info = {
        "base_model":      MODEL_PATH,
        "train_data":      TRAIN_PATH,
        "train_samples":   700,
        "train_format":    "Spider 1.0",
        "train_script":    "train_dora_spider0312.py",
        "method":          "DoRA" if USE_DORA else "LoRA",
        "lora_r":          LORA_R,
        "lora_alpha":      LORA_ALPHA,
        "epochs":          NUM_EPOCHS,
        "effective_batch": BATCH_SIZE * GRAD_ACCUM,
        "learning_rate":   LEARNING_RATE,
        "weight_decay":    WEIGHT_DECAY,
        "warmup_ratio":    WARMUP_RATIO,
        "date":            DATE_STR,
        "final_loss":      round(trainer.state.log_history[-1].get("loss", 0), 4),
    }
    with open(os.path.join(FINAL_MODEL, "training_info.json"), "w") as f:
        json.dump(info, f, indent=2)

    print(f"\n模型已儲存: {FINAL_MODEL}")
    print("訓練資訊:")
    for k, v in info.items():
        print(f"  {k}: {v}")


# ============================================================
# 主流程
# ============================================================
def main():
    print("=" * 60)
    print(f"WP_M09 DoRA 訓練 (Spider 格式)  ({DATE_STR})")
    print(f"腳本: train_dora_spider0312.py")
    print("=" * 60)

    dataset          = load_dataset(TRAIN_PATH)
    tokenizer, model = load_model_and_tokenizer()
    model            = apply_dora(model)
    trainer          = train(model, tokenizer, dataset)
    save_model(trainer, tokenizer)

    print("\n✅ 訓練完成！")
    print(f"   權重位置: {FINAL_MODEL}")
    print(f"   評估指令: python evaluate_correct_test.py")


if __name__ == "__main__":
    main()
