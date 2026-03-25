# train__dora_spider_v0320.py
# 使用 DoRA (Weight-Decomposed LoRA) 微調 Llama-3.1-8B-Instruct
#
# 改動重點（vs 0318）：
#   - 全新訓練集 v3: 1999 筆，100% 可執行且有回傳資料
#   - 平衡 SQL 模式分布：GROUP BY 7.4%, TOP+GROUP BY 6.3%
#   - 修正 WP_vProvider: 用 sn 不是 pvSn
#   - 所有欄位來自真實 schema，無幻覺欄位
#   - 0 重複問題、0 重複 SQL
#   - TABLE_NOTES 更新 WP_vProvider 的欄位說明
#
# 執行方式:
#   python train__dora_spider_v0320.py
#
# 輸出:
#   outputs/models/wp_m09_dora_0320_spider_v3/final_model/

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
TRAIN_PATH   = r"data\wp_m09\train_claude_en_2000_v3.json"
DATE_STR     = "0320"
OUTPUT_DIR   = f"outputs/models/wp_m09_dora_{DATE_STR}_spider_v3"
FINAL_MODEL  = os.path.join(OUTPUT_DIR, "final_model")

# ---- DoRA / LoRA 超參數 ----
LORA_R        = 16
LORA_ALPHA    = 32
LORA_DROPOUT  = 0.05
USE_DORA      = True

# ---- 訓練超參數 ----
NUM_EPOCHS    = 10            # 資料量增加(1999筆)，減少 epochs 避免 overfit
BATCH_SIZE    = 4
GRAD_ACCUM    = 4             # effective batch = 16
LEARNING_RATE = 8e-5          # 稍低的 LR，配合更大資料集
MAX_SEQ_LEN   = 512
LR_SCHEDULER  = "cosine"
WARMUP_RATIO  = 0.06
WEIGHT_DECAY  = 0.01
# ============================================================

# ============================================================
# 特定表的額外說明 (TABLE_NOTES) - v3 修正版
# ============================================================
TABLE_NOTES = {
    # ── 無 isDel 欄的 view ──
    "WP_vProduct":   "Note: pNo is a sequential product number (1, 2, 3...), NOT a date. "
                     "This view has no date filtering capability. "
                     "CRITICAL: This view has NO isDel or dtlIsDel column — never add them.",
    "WP_vInventory": "Note: pNo is a sequential product number, not a date. "
                     "This view has no date filtering capability. "
                     "CRITICAL: This view has NO isDel or dtlIsDel column — never add them.",
    "WP_vProvider":  "Note: Supplier info table. Use sn as the primary key (NOT pvSn — pvSn does NOT exist in this view). "
                     "Use isStop to check if a supplier is active (isStop='N') or inactive (isStop='Y'). "
                     "CRITICAL: This view has NO isDel, dtlIsDel, or pvSn column — never use them.",
    # ── 有 isDel + dtlIsDel 的 view ──
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
# 工具函式
# ============================================================
def extract_table_from_sql(sql: str) -> str:
    m = re.search(r'WP_M09\.dbo\.(\w+)', sql)
    if m:
        return m.group(1)
    m = re.search(r'\[WP_M09\]\.\[dbo\]\.\[(\w+)\]', sql)
    return m.group(1) if m else ""


# ============================================================
# 資料前處理
# ============================================================
def build_prompt(sample: dict) -> str:
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


def load_dataset(path: str):
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    texts = [{"text": build_prompt(s)} for s in raw]
    dataset = Dataset.from_list(texts)

    print(f"訓練集載入: {len(dataset)} 筆")
    print(f"\n範例 prompt:")
    print("-" * 50)
    print(dataset[0]['text'])
    print("-" * 50)

    from collections import Counter
    table_cnt = Counter(extract_table_from_sql(s.get("query","")) for s in raw)
    left_cnt = sum(1 for s in raw if "LEFT(" in s.get("query","").upper())
    like_cnt = sum(1 for s in raw if "LIKE"  in s.get("query","").upper())
    isdel_cnt = sum(1 for s in raw if "isDel" in s.get("query",""))
    groupby_cnt = sum(1 for s in raw if "GROUP BY" in s.get("query","").upper())
    top_cnt = sum(1 for s in raw if "TOP " in s.get("query","").upper())

    print(f"\n表/View 分佈: {dict(table_cnt)}")
    print(f"含 LEFT(): {left_cnt}  含 LIKE: {like_cnt}  含 isDel: {isdel_cnt}")
    print(f"含 GROUP BY: {groupby_cnt}  含 TOP: {top_cnt}")

    # 確認沒有錯誤樣本
    bad_isdel = [s for s in raw
           if re.search(r'\bisdel\b|\bdtlisdel\b', s.get("query",""), re.IGNORECASE)
           and any(v in s.get("query","") for v in ["WP_vInventory","WP_vProduct","WP_vProvider"])]
    bad_pvsn = [s for s in raw
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

    return dataset, len(raw)


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

    print("基礎模型載入完成")
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
    print(f"  Batch size:       {BATCH_SIZE} x {GRAD_ACCUM} (accum) = {effective_batch} (effective)")
    print(f"  Steps/epoch:      ~{steps_per_epoch}")
    print(f"  Total steps:      ~{total_steps}")
    print(f"  Learning rate:    {LEARNING_RATE}  (cosine decay)")
    print(f"  Warmup ratio:     {WARMUP_RATIO}")
    print(f"  Weight decay:     {WEIGHT_DECAY}")
    print(f"  Max seq len:      {MAX_SEQ_LEN}")
    print(f"  Output dir:       {OUTPUT_DIR}\n")

    trainer.train()
    return trainer


# ============================================================
# 儲存權重
# ============================================================
def save_model(trainer, tokenizer, train_samples: int):
    os.makedirs(FINAL_MODEL, exist_ok=True)

    trainer.model.save_pretrained(FINAL_MODEL)
    tokenizer.save_pretrained(FINAL_MODEL)

    info = {
        "base_model":      MODEL_PATH,
        "train_data":      TRAIN_PATH,
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
        "changes":         "v3 training set: 1999 samples, balanced SQL patterns, "
                           "fixed pvSn->sn in WP_vProvider, all SQL verified executable with data, "
                           "GROUP BY 7.4%, TOP+GROUP 6.3%, 0 duplicates",
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
    print(f"WP_M09 DoRA 訓練 v{DATE_STR}（全新 v3 訓練集）")
    print(f"訓練集: {TRAIN_PATH} (1999 筆)")
    print(f"改進: 平衡 SQL 模式、修正 pvSn、0 幻覺欄位")
    print("=" * 60)

    dataset, n_samples  = load_dataset(TRAIN_PATH)
    tokenizer, model    = load_model_and_tokenizer()
    model               = apply_dora(model)
    trainer             = train(model, tokenizer, dataset)
    save_model(trainer, tokenizer, n_samples)

    print("\n訓練完成！")
    print(f"  權重位置: {FINAL_MODEL}")
    print(f"  評估指令: python eval__em_and_execution_accuracy.py "
          f"--model {FINAL_MODEL} "
          f"--gold data/wp_m09/val_claude_en_v3.json "
          f"--output outputs/evaluation_{DATE_STR}_val_v3.json")


if __name__ == "__main__":
    main()
