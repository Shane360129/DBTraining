# train__9views_v0324.py
# ============================================================
# 9 Views Text-to-SQL 訓練腳本 v0324
#
# 資料來源：data/wp_m09/train_9views_20k.json（19,993 筆）
# 切分策略：80% train / 10% val / 10% test（按 view + difficulty 分層抽樣）
#
# vs v0324 enterprise 版本的改動：
#   1. 單一 20K 資料集，內建 80/10/10 分層切分
#   2. 9 個 View（新增 WP_vMemberDeposit, WP_vPdCombine）
#   3. Schema 更新以涵蓋 9 個 View
#   4. 沿用 anti-overfit 策略：3 epochs, LR 2e-5, EarlyStopping
#
# 用法:
#   python train__9views_v0324.py
#   python train__9views_v0324.py --no-rules        # 不含規則（ablation）
#   python train__9views_v0324.py --split-only       # 只切分資料，不訓練
#   python train__9views_v0324.py --epochs 2         # 自訂 epoch 數
#
# 輸出:
#   outputs/models/9views_full_0324/final_model/
#   data/wp_m09/split_9views_train.json   (80%)
#   data/wp_m09/split_9views_val.json     (10%)
#   data/wp_m09/split_9views_test.json    (10%)
# ============================================================

import json
import os
import re
import sys
import argparse
import random
import torch
import statistics
from datetime import datetime
from collections import Counter, defaultdict
from datasets import Dataset
from transformers import (
    AutoTokenizer,
    AutoModelForCausalLM,
    BitsAndBytesConfig,
    EarlyStoppingCallback,
)
from peft import LoraConfig, get_peft_model, TaskType
from trl import SFTTrainer, SFTConfig


# ============================================================
# Settings
# ============================================================
MODEL_PATH   = "meta-llama/Llama-3.1-8B-Instruct"
DATE_STR     = "0324"

DATA_PATH    = r"data\wp_m09\train_9views_20k.json"

# 切分輸出路徑
SPLIT_DIR    = r"data\wp_m09"
SPLIT_TRAIN  = os.path.join(SPLIT_DIR, "split_9views_train.json")
SPLIT_VAL    = os.path.join(SPLIT_DIR, "split_9views_val.json")
SPLIT_TEST   = os.path.join(SPLIT_DIR, "split_9views_test.json")

# 切分比例
TRAIN_RATIO  = 0.80
VAL_RATIO    = 0.10
TEST_RATIO   = 0.10

# ---- DoRA ----
LORA_R        = 16
LORA_ALPHA    = 32
LORA_DROPOUT  = 0.05
USE_DORA      = True

# ---- Training hyperparams ----
NUM_EPOCHS    = 3
BATCH_SIZE    = 2
GRAD_ACCUM    = 8           # effective batch = 16
EARLY_STOPPING_PATIENCE = 3
LEARNING_RATE = 2e-5
MAX_SEQ_LEN   = 1280
LR_SCHEDULER  = "cosine"
WARMUP_RATIO  = 0.06
WEIGHT_DECAY  = 0.01

SEED = 42
random.seed(SEED)


# ============================================================
# Full-Schema System Prompt（9 Views）
# ============================================================
FULL_SCHEMA = (
    "-- WP_M09 (SQL Server T-SQL). Prefix: WP_M09.dbo.<View>. Chinese: N'str'.\n\n"
    "WP_vAcctIn(sn, acctInId, acctInDate, amount, memo, empId, isDel, dtlSn, OutStkId, outStkAmtTotal, dtlIsDel, memSn, memId, memName, pNo, pBarcode, pName, pNameS, oStkDtlAmt, oStkDtlQty, oStkDtlAmtTotal, dtlDiscnt, dtlDiscntShare, discount, discountShare)"
    " -- Receivable. LEFT(acctInId,8)=date. isDel+dtlIsDel.\n"
    "WP_vAcctOut(sn, acctOutId, acctOutDate, amount, transAmt, memo, empId, empName, isDel, dtlSn, InStkId, dtlAmt, qty, amtTotal, dtlIsDel, pNo, pName, pNameS, pBarcode, pvId, pvName, pvNameS, pvSn, pvDiscount, inStkAmt, inStkAmtTotal, payType)"
    " -- Payable. LEFT(acctOutId,8)=date. isDel+dtlIsDel.\n"
    "WP_vOutStock(sn, OutStkId, OutStkDate, amount, tax, amtNoneTax, isDel, empId, empName, memo, memSn, memId, memName, outType, dtlSn, pNo, qty, dtlAmt, amtTotal, dtlIsDel, dtlCostAvg, dtlCostStd, dtlDiscnt, dtlDiscntPer, dtlDiscntShare, pName, pBarcode, pUName, costStd, discount, discountShare, memTel, memCityName, memZoneName)"
    " -- Sales/Outbound. LEFT(OutStkId,8)=date. isDel+dtlIsDel.\n"
    "WP_vTransfer(sn, TransferId, empId, dtlSn, FromWhSn, fWhId, fWhName, ToWhSn, tfWhId, tfWhName, TransferDate, pNo, qty, pName, pNameS, pBarcode, pCode, isDel, dtlIsDel, costAvg)"
    " -- Transfer. LEFT(TransferId,8)=date. isDel+dtlIsDel. No header amount. fWhName=source, tfWhName=dest.\n"
    "WP_vInventory(whSn, WarehouseId, WarehouseName, pNo, pName, pNameS, pBarcode, pUnit, pUName, priceStd, priceLow, priceMem, priceBat, costStd, costAvg, isSale, pvName, pvNameS, qtyNow, pvSn, qtySafe, qty)"
    " -- Inventory. NO isDel. NO date. pNo=seq#. isSale: 0=normal, 1=stop-purchase, 2=stop-sell, 3=stop-both.\n"
    "WP_vProduct(pNo, pName, pNameS, pBarcode, pCode, pUnit, pUName, priceStd, priceLow, priceMem, priceBat, isPvDiscount, isSale, costStd, costAvg, pvSn, pvId, pvName, pvNameS, qtyNow, qtySafe, pvDiscount)"
    " -- Product. NO isDel. NO date. pNo=seq#. isSale: 0=normal, 1=stop-purchase, 2=stop-sell, 3=stop-both.\n"
    "WP_vProvider(sn, pvId, pvName, pvNameS, pvKId, pvBoss, pvTel, pvCityId, pvZoneId, pvCity, pvZone, pvAddr, ctactName, ctactTel, fax, email, taxId, isStop, invoTitle, bankId, bankName, bankAccount, bankAcctName, memo, pvKName, pvDiscount)"
    " -- Supplier. NO isDel. isStop=N/Y. NO date. SELECT pvId (not pvSn).\n"
    "WP_vMemberDeposit(sn, memSn, memId, memName, depositAmt, depositDate, memo, empId, empName)"
    " -- Member Deposit. depositDate for date filter.\n"
    "WP_vPdCombine(sn, combineId, pNo, pName, pNameS, pBarcode, qty, costStd, costAvg, subPNo, subPName, subPNameS, subPBarcode, subQty, subCostStd, subCostAvg)"
    " -- Product Combo. pNo=main product, subPNo=sub product."
)

BUSINESS_RULES = (
    "Rules:\n"
    "1. isDel views (AcctIn/AcctOut/OutStock/Transfer): isDel='N' header, +dtlIsDel='N' detail columns.\n"
    "2. No-isDel views (Inventory/Product/Provider/MemberDeposit/PdCombine): NEVER add isDel.\n"
    "3. Provider: isStop='N' active, isStop='Y' inactive.\n"
    "4. isSale (Product/Inventory): 0=normal, 1=stop-purchase-only, 2=stop-sell-only, 3=stop-both. Sellable=IN('0','1'), Non-sellable=IN('2','3').\n"
    "5. SUM/AVG header amount: SELECT SUM(amount) FROM (SELECT DISTINCT xxxId, amount FROM ... WHERE isDel='N') sub\n"
    "6. NEVER use SUM(DISTINCT amount) or AVG(DISTINCT amount) — always use subquery dedup.\n"
    "7. Count orders: COUNT(DISTINCT xxxId), never COUNT(*) on header-detail views.\n"
    "8. Date: LEFT(xxxId,8)='YYYYMMDD'. Only isDel views. pNo=seq number, NOT date.\n"
    "9. T-SQL only: use TOP N, never LIMIT. Use N'str' for Chinese strings.\n"
    "10. Provider SELECT: use pvId (not pvSn). pvSn is for JOIN only."
)


# ============================================================
# Utils
# ============================================================
def extract_view_from_sql(sql):
    """從 SQL 中提取 View 名稱"""
    m = re.search(r'WP_M09\.dbo\.(\w+)', sql)
    if m:
        return m.group(1)
    m = re.search(r'\[WP_M09\]\.\[dbo\]\.\[(\w+)\]', sql)
    return m.group(1) if m else ""


def normalize_query(sql):
    s = sql.strip().rstrip(';').strip()
    s = re.sub(r'\s+', ' ', s)
    return s.upper()


# ============================================================
# Stratified Split（按 view + difficulty 分層抽樣）
# ============================================================
def stratified_split(data, train_ratio=0.8, val_ratio=0.1, test_ratio=0.1, seed=42):
    """
    按 (view, difficulty) 進行分層抽樣，確保各切分中的分布一致。
    """
    assert abs(train_ratio + val_ratio + test_ratio - 1.0) < 1e-6, "比例之和必須為 1"

    rng = random.Random(seed)

    # 按 (view, difficulty) 分組
    groups = defaultdict(list)
    for item in data:
        view = extract_view_from_sql(item.get("query", ""))
        diff = item.get("difficulty", "unknown")
        groups[(view, diff)].append(item)

    train_set, val_set, test_set = [], [], []

    for key, items in sorted(groups.items()):
        rng.shuffle(items)
        n = len(items)
        n_train = max(1, round(n * train_ratio))
        n_val   = max(1, round(n * val_ratio))
        n_test  = n - n_train - n_val

        # 保底：至少各分到 1 筆
        if n_test < 1:
            n_test = 1
            n_train = n - n_val - n_test

        train_set.extend(items[:n_train])
        val_set.extend(items[n_train:n_train + n_val])
        test_set.extend(items[n_train + n_val:])

    # 各切分內部打亂
    rng.shuffle(train_set)
    rng.shuffle(val_set)
    rng.shuffle(test_set)

    return train_set, val_set, test_set


def print_split_stats(name, data):
    """印出切分的統計資訊"""
    views = Counter(extract_view_from_sql(d.get("query", "")) for d in data)
    diffs = Counter(d.get("difficulty", "unknown") for d in data)

    print(f"\n  {name}: {len(data)} 筆")
    print(f"    View 分布:")
    for v, cnt in sorted(views.items()):
        print(f"      {v}: {cnt} ({cnt/len(data)*100:.1f}%)")
    print(f"    Difficulty 分布:")
    for d, cnt in sorted(diffs.items()):
        print(f"      {d}: {cnt} ({cnt/len(data)*100:.1f}%)")


# ============================================================
# Build Chat Template prompts
# ============================================================
def build_system_prompt(include_rules=True):
    """建構 system prompt"""
    parts = [
        "You are an expert T-SQL assistant for WP_M09 database (SQL Server). "
        "Generate ONLY the SQL query. Do not explain."
    ]
    parts.append(FULL_SCHEMA)
    if include_rules:
        parts.append(BUSINESS_RULES)
    return "\n\n".join(parts)


def build_chat_text(system_prompt, question, sql, tokenizer):
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": question},
        {"role": "assistant", "content": sql},
    ]
    return tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=False)


# ============================================================
# Data Loading & Processing
# ============================================================
def load_and_split(force_resplit=False):
    """載入資料並切分，如果切分檔案已存在則直接載入"""

    if not force_resplit and all(os.path.exists(p) for p in [SPLIT_TRAIN, SPLIT_VAL, SPLIT_TEST]):
        print("  載入已有的切分檔案 ...")
        with open(SPLIT_TRAIN, "r", encoding="utf-8") as f:
            train_data = json.load(f)
        with open(SPLIT_VAL, "r", encoding="utf-8") as f:
            val_data = json.load(f)
        with open(SPLIT_TEST, "r", encoding="utf-8") as f:
            test_data = json.load(f)
        print(f"  Train: {len(train_data)}, Val: {len(val_data)}, Test: {len(test_data)}")
        return train_data, val_data, test_data

    # 載入完整資料集
    print(f"  載入: {DATA_PATH}")
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        all_data = json.load(f)
    print(f"  Total: {len(all_data)} 筆")

    # 去重
    seen = set()
    deduped = []
    for item in all_data:
        norm = normalize_query(item.get("query", ""))
        if norm and norm not in seen:
            seen.add(norm)
            deduped.append(item)
    print(f"  After dedup: {len(deduped)} (removed {len(all_data) - len(deduped)})")

    # 分層切分
    train_data, val_data, test_data = stratified_split(
        deduped,
        train_ratio=TRAIN_RATIO,
        val_ratio=VAL_RATIO,
        test_ratio=TEST_RATIO,
        seed=SEED,
    )

    # 儲存切分結果
    os.makedirs(SPLIT_DIR, exist_ok=True)
    for path, data, name in [
        (SPLIT_TRAIN, train_data, "Train"),
        (SPLIT_VAL,   val_data,   "Val"),
        (SPLIT_TEST,  test_data,  "Test"),
    ]:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"  已儲存 {name}: {path}")

    print_split_stats("Train", train_data)
    print_split_stats("Val",   val_data)
    print_split_stats("Test",  test_data)

    return train_data, val_data, test_data


def build_datasets(tokenizer, train_data, val_data, include_rules=True):
    """將 JSON 資料轉換為 HuggingFace Dataset"""
    system_prompt = build_system_prompt(include_rules=include_rules)

    def process_split(data, split_name):
        texts = []
        skipped = 0
        for item in data:
            question = item.get("question", "")
            sql = item.get("query", "").strip().rstrip(';').strip()
            view = extract_view_from_sql(sql)

            if not view or not question or not sql:
                skipped += 1
                continue

            text = build_chat_text(system_prompt, question, sql, tokenizer)
            tok_len = len(tokenizer(text, truncation=False)["input_ids"])

            if tok_len > MAX_SEQ_LEN:
                skipped += 1
                continue

            texts.append({"text": text, "tok_len": tok_len, "view": view})

        print(f"\n  {split_name}: {len(texts)} included, {skipped} skipped")
        if texts:
            lengths = [t["tok_len"] for t in texts]
            print(f"    Token length: Min={min(lengths)}, Max={max(lengths)}, "
                  f"Mean={statistics.mean(lengths):.0f}, Median={statistics.median(lengths):.0f}")
            view_cnt = Counter(t["view"] for t in texts)
            print(f"    View distribution:")
            for v, cnt in sorted(view_cnt.items()):
                print(f"      {v}: {cnt}")

        return texts

    train_texts = process_split(train_data, "Train")
    val_texts   = process_split(val_data, "Val")

    # 顯示 sample prompt
    if train_texts:
        print(f"\n  Sample prompt (first 800 chars):")
        print("-" * 60)
        print(train_texts[0]["text"][:800])
        print("-" * 60)

    train_dataset = Dataset.from_list([{"text": t["text"]} for t in train_texts])
    val_dataset   = Dataset.from_list([{"text": t["text"]} for t in val_texts]) if val_texts else None

    return train_dataset, val_dataset, len(train_texts)


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
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
        lora_dropout=LORA_DROPOUT,
        bias="none",
        task_type=TaskType.CAUSAL_LM,
        use_dora=USE_DORA,
    )
    model = get_peft_model(model, lora_cfg)
    model.print_trainable_parameters()
    print(f"Fine-tune: {'DoRA' if USE_DORA else 'LoRA'} (r={LORA_R}, alpha={LORA_ALPHA})")
    return model


# ============================================================
# Training
# ============================================================
def train(model, tokenizer, train_dataset, output_dir, val_dataset=None, num_epochs=NUM_EPOCHS):
    os.makedirs(output_dir, exist_ok=True)
    has_eval = val_dataset is not None and len(val_dataset) > 0

    eff_batch = BATCH_SIZE * GRAD_ACCUM
    steps_per_epoch = max(1, len(train_dataset) // eff_batch)
    eval_save_steps = max(1, steps_per_epoch // 2)  # 每 0.5 epoch

    sft_cfg = SFTConfig(
        output_dir=output_dir,
        num_train_epochs=num_epochs,
        per_device_train_batch_size=BATCH_SIZE,
        gradient_accumulation_steps=GRAD_ACCUM,
        learning_rate=LEARNING_RATE,
        lr_scheduler_type=LR_SCHEDULER,
        warmup_ratio=WARMUP_RATIO,
        weight_decay=WEIGHT_DECAY,
        optim="paged_adamw_8bit",
        bf16=True,
        logging_steps=10,
        save_strategy="steps" if has_eval else "epoch",
        save_steps=eval_save_steps,
        save_total_limit=5,
        eval_strategy="steps" if has_eval else "no",
        eval_steps=eval_save_steps,
        load_best_model_at_end=has_eval,
        metric_for_best_model="eval_loss" if has_eval else None,
        greater_is_better=False,
        report_to="none",
        dataloader_num_workers=0,
        dataset_text_field="text",
        packing=False,
    )

    callbacks = []
    if has_eval:
        callbacks.append(EarlyStoppingCallback(
            early_stopping_patience=EARLY_STOPPING_PATIENCE,
        ))
        print(f"\n  Early Stopping: patience={EARLY_STOPPING_PATIENCE}, metric=eval_loss")

    trainer = SFTTrainer(
        model=model,
        processing_class=tokenizer,
        train_dataset=train_dataset,
        eval_dataset=val_dataset if has_eval else None,
        args=sft_cfg,
        callbacks=callbacks,
    )

    print(f"\nTraining:")
    print(f"  Train samples: {len(train_dataset)}")
    print(f"  Val samples:   {len(val_dataset) if has_eval else 'N/A'}")
    print(f"  Epochs:        {num_epochs}")
    print(f"  Batch:         {BATCH_SIZE} x {GRAD_ACCUM} = {eff_batch}")
    print(f"  Steps/epoch:   ~{steps_per_epoch}")
    print(f"  Total steps:   ~{steps_per_epoch * num_epochs}")
    print(f"  Eval every:    {eval_save_steps} steps (~0.5 epoch)")
    print(f"  LR:            {LEARNING_RATE} ({LR_SCHEDULER})")
    print(f"  MAX_SEQ_LEN:   {MAX_SEQ_LEN}")
    print(f"  Output:        {output_dir}\n")

    trainer.train()
    return trainer


# ============================================================
# Save
# ============================================================
def save_model(trainer, tokenizer, n_samples, output_dir, args):
    final_dir = os.path.join(output_dir, "final_model")
    os.makedirs(final_dir, exist_ok=True)

    trainer.model.save_pretrained(final_dir)
    tokenizer.save_pretrained(final_dir)

    info = {
        "base_model":       MODEL_PATH,
        "train_script":     "train__9views_v0324.py",
        "methodology":      "Spider/BIRD-style: Full-DB schema (9 views) + Business rules + Chat Template",
        "include_rules":    not args.no_rules,
        "method":           "DoRA" if USE_DORA else "LoRA",
        "lora_r":           LORA_R,
        "lora_alpha":       LORA_ALPHA,
        "train_samples":    n_samples,
        "split":            f"{TRAIN_RATIO}/{VAL_RATIO}/{TEST_RATIO}",
        "epochs":           args.epochs,
        "effective_batch":  BATCH_SIZE * GRAD_ACCUM,
        "learning_rate":    LEARNING_RATE,
        "max_seq_len":      MAX_SEQ_LEN,
        "date":             DATE_STR,
        "early_stopping":   f"patience={EARLY_STOPPING_PATIENCE}",
        "data_source":      DATA_PATH,
        "views":            "9 views (AcctIn/AcctOut/OutStock/Transfer/Inventory/Product/Provider/MemberDeposit/PdCombine)",
    }
    with open(os.path.join(final_dir, "training_info.json"), "w") as f:
        json.dump(info, f, indent=2, ensure_ascii=False)

    print(f"\nModel saved: {final_dir}")
    for k, v in info.items():
        print(f"  {k}: {v}")
    return final_dir


# ============================================================
# Main
# ============================================================
def parse_args():
    p = argparse.ArgumentParser(description="9 Views Text-to-SQL Training (Spider/BIRD methodology)")
    p.add_argument("--no-rules", action="store_true", help="Disable business rules (ablation)")
    p.add_argument("--epochs", type=int, default=NUM_EPOCHS, help=f"Number of epochs (default: {NUM_EPOCHS})")
    p.add_argument("--split-only", action="store_true", help="Only split data, do not train")
    p.add_argument("--force-resplit", action="store_true", help="Force re-split even if split files exist")
    p.add_argument("--output-suffix", type=str, default="", help="Custom output directory suffix")
    return p.parse_args()


def main():
    args = parse_args()

    suffix = args.output_suffix or "full"
    if args.no_rules: suffix += "_norule"
    output_dir = f"outputs/models/9views_{suffix}_{DATE_STR}"

    print("=" * 70)
    print(f"9 Views Text-to-SQL Training v{DATE_STR}")
    print(f"  Data:     {DATA_PATH}")
    print(f"  Split:    {TRAIN_RATIO*100:.0f}% / {VAL_RATIO*100:.0f}% / {TEST_RATIO*100:.0f}%")
    print(f"  Rules:    {'Yes' if not args.no_rules else 'No (ablation)'}")
    print(f"  Epochs:   {args.epochs}")
    print(f"  Output:   {output_dir}")
    print("=" * 70)

    # Step 1: Split data
    print("\n[Step 1] 資料切分")
    train_data, val_data, test_data = load_and_split(force_resplit=args.force_resplit)
    print_split_stats("Train", train_data)
    print_split_stats("Val",   val_data)
    print_split_stats("Test",  test_data)

    if args.split_only:
        print("\n[--split-only] 資料切分完成，不進行訓練。")
        print(f"  Train: {SPLIT_TRAIN}")
        print(f"  Val:   {SPLIT_VAL}")
        print(f"  Test:  {SPLIT_TEST}")
        return

    # Step 2: Load model & tokenizer
    print("\n[Step 2] 載入模型")
    tokenizer, model = load_model_and_tokenizer()

    # Step 3: Build datasets
    print("\n[Step 3] 建構 Dataset")
    train_dataset, val_dataset, n_samples = build_datasets(
        tokenizer, train_data, val_data, include_rules=not args.no_rules
    )

    # Step 4: Apply DoRA
    print("\n[Step 4] DoRA 設定")
    model = apply_dora(model)

    # Step 5: Train
    print("\n[Step 5] 開始訓練")
    trainer = train(model, tokenizer, train_dataset, output_dir,
                    val_dataset=val_dataset, num_epochs=args.epochs)

    # Step 6: Save
    print("\n[Step 6] 儲存模型")
    final_dir = save_model(trainer, tokenizer, n_samples, output_dir, args)

    # Print evaluation commands
    print("\n" + "=" * 70)
    print("Training complete! Evaluation commands:")
    print("=" * 70)
    print(f"\n# Val set evaluation:")
    print(f"python eval__spider_style.py `")
    print(f"    --model {final_dir} `")
    print(f"    --gold {SPLIT_VAL} `")
    print(f"    --output outputs/eval_9views_{suffix}_{DATE_STR}_val.json")
    print(f"\n# Test set evaluation:")
    print(f"python eval__spider_style.py `")
    print(f"    --model {final_dir} `")
    print(f"    --gold {SPLIT_TEST} `")
    print(f"    --output outputs/eval_9views_{suffix}_{DATE_STR}_test.json")


if __name__ == "__main__":
    main()
