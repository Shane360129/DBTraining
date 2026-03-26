# train_dora_0306.py
# 使用 DoRA (Weight-Decomposed LoRA) 微調 Llama-3.1-8B-Instruct
# 訓練資料: data/wp_m09/train0306_clean.json
#
# 執行方式:
#   python train_dora_0306.py
#
# 輸出:
#   outputs/models/wp_m09_dora_{MMDD}/final_model/

import json
import os
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
TRAIN_PATH   = r"data\wp_m09\train0306_fixed.json"   # ← 使用修正後的資料
DATE_STR     = datetime.now().strftime("%m%d")
OUTPUT_DIR   = f"outputs/models/wp_m09_dora_{DATE_STR}_v2"
FINAL_MODEL  = os.path.join(OUTPUT_DIR, "final_model")

# ---- DoRA / LoRA 超參數 ----
LORA_R        = 16
LORA_ALPHA    = 32
LORA_DROPOUT  = 0.05
USE_DORA      = True      # ← 這裡開啟 DoRA，改 False 就退回純 LoRA

# ---- 訓練超參數 ----
NUM_EPOCHS    = 7
BATCH_SIZE    = 4
GRAD_ACCUM    = 4         # effective batch = BATCH_SIZE * GRAD_ACCUM = 16
LEARNING_RATE = 2e-4
MAX_SEQ_LEN   = 512
LR_SCHEDULER  = "cosine"
WARMUP_RATIO  = 0.05
# ============================================================


# ============================================================
# 資料前處理
# ============================================================
# 特定表的額外說明：告訴模型這些表沒有獨立日期欄位，日期編碼在 ID 裡
TABLE_NOTES = {
    "WP_vProduct":   "Note: Filter by date using pNo LIKE 'YYYYMMDD%'. No separate date column.",
    "WP_vInventory": "Note: Filter by date using pNo LIKE 'YYYYMMDD%'. No separate date column.",
    "WP_vTransfer":  "Note: Filter by date using TransferId LIKE 'YYYYMMDD%'. No separate date column.",
}


def build_prompt(sample: dict) -> str:
    """
    Schema-Aware prompt 格式
    支援兩種 JSON 格式：
      A. {"schema": "...", "question": "...", "query": "..."}
      B. {"table": "...", "question": "...", "query": "..."}
    對 WP_vProduct / WP_vInventory / WP_vTransfer 加入日期欄位說明
    """
    schema   = sample.get("schema", "")
    table    = sample.get("table", "")
    question = sample.get("question", "")
    sql      = sample.get("query") or sample.get("sql", "")

    # schema 部分
    if schema:
        schema_part = f"Schema: {schema}\n"
    elif table:
        schema_part = f"Table: {table}\n"
    else:
        schema_part = ""

    # 特定表加入注意事項
    note = TABLE_NOTES.get(table, "")
    note_part = f"{note}\n" if note else ""

    return (
        f"{schema_part}"
        f"{note_part}"
        f"Question: {question}\n"
        f"SQL: {sql}"
    )


def load_dataset(path: str) -> Dataset:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    texts = [{"text": build_prompt(s)} for s in raw]
    dataset = Dataset.from_list(texts)
    print(f"訓練集載入: {len(dataset)} 筆")
    print(f"範例:\n{dataset[0]['text']}\n")
    return dataset


# ============================================================
# 模型載入
# ============================================================
def load_model_and_tokenizer():
    print(f"載入基礎模型: {MODEL_PATH} ...")

    bnb_cfg = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16,
        bnb_4bit_use_double_quant=True,
    )

    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, use_fast=True)
    tokenizer.pad_token        = tokenizer.eos_token
    tokenizer.padding_side     = "right"
    tokenizer.model_max_length = MAX_SEQ_LEN   # 截斷長度設在 tokenizer

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
        use_dora=USE_DORA,      # DoRA 開關
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

    # SFTConfig = TrainingArguments + SFT 專屬參數合併（trl >= 0.12）
    sft_cfg = SFTConfig(
        output_dir=OUTPUT_DIR,
        num_train_epochs=NUM_EPOCHS,
        per_device_train_batch_size=BATCH_SIZE,
        gradient_accumulation_steps=GRAD_ACCUM,
        learning_rate=LEARNING_RATE,
        lr_scheduler_type=LR_SCHEDULER,
        warmup_ratio=WARMUP_RATIO,
        optim="paged_adamw_8bit",
        bf16=True,
        logging_steps=10,
        save_strategy="epoch",
        save_total_limit=2,
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

    print(f"\n開始訓練...")
    print(f"  Epochs:          {NUM_EPOCHS}")
    print(f"  Batch size:      {BATCH_SIZE} × {GRAD_ACCUM} = {BATCH_SIZE*GRAD_ACCUM} (effective)")
    print(f"  Learning rate:   {LEARNING_RATE}")
    print(f"  Max seq len:     {MAX_SEQ_LEN}")
    print(f"  輸出目錄:        {OUTPUT_DIR}\n")

    trainer.train()
    return trainer


# ============================================================
# 儲存權重
# ============================================================
def save_model(trainer, tokenizer):
    os.makedirs(FINAL_MODEL, exist_ok=True)

    trainer.model.save_pretrained(FINAL_MODEL)
    tokenizer.save_pretrained(FINAL_MODEL)

    # 訓練資訊
    info = {
        "base_model":    MODEL_PATH,
        "train_data":    TRAIN_PATH,
        "method":        "DoRA" if USE_DORA else "LoRA",
        "lora_r":        LORA_R,
        "lora_alpha":    LORA_ALPHA,
        "epochs":        NUM_EPOCHS,
        "batch_size":    BATCH_SIZE * GRAD_ACCUM,
        "learning_rate": LEARNING_RATE,
        "date":          DATE_STR,
        "final_loss":    round(trainer.state.log_history[-1].get("loss", 0), 4),
    }
    import json as _json
    with open(os.path.join(FINAL_MODEL, "training_info.json"), "w") as f:
        _json.dump(info, f, indent=2)

    print(f"\n模型已儲存: {FINAL_MODEL}")
    print(f"訓練資訊:")
    for k, v in info.items():
        print(f"  {k}: {v}")


# ============================================================
# 主流程
# ============================================================
def main():
    print("=" * 60)
    print(f"WP_M09 DoRA 訓練  ({DATE_STR})")
    print("=" * 60)

    # 1. 資料
    dataset = load_dataset(TRAIN_PATH)

    # 2. 模型
    tokenizer, model = load_model_and_tokenizer()

    # 3. DoRA
    model = apply_dora(model)

    # 4. 訓練
    trainer = train(model, tokenizer, dataset)

    # 5. 儲存
    save_model(trainer, tokenizer)

    print("\n✅ 訓練完成！")
    print(f"   權重位置: {FINAL_MODEL}")
    print(f"   評估指令: python evaluate_correct_test.py")


if __name__ == "__main__":
    main()
