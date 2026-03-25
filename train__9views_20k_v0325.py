# train__9views_20k_v0325.py
# ============================================================
# 9 Views × 20K 訓練腳本 v0325
#
# vs v0324 關鍵改動：
#   1. 使用 train_9views_20k.json（19,993 筆），80/10/10 分層切分
#   2. 新增 WP_vMemberDeposit、WP_vPdCombine 兩個 View 的 schema
#   3. 更新 BUSINESS_RULES 涵蓋 9 個 View
#   4. 移除外部 augmentation（20K 資料量已足夠）
#   5. Epochs 3→2（20K 資料量大，v0324 分析顯示 epoch 1.5 後即無效）
#   6. 加嚴 Early Stopping patience 3→2
#
# v0324 失敗分析：
#   - 模型輸出背誦 schema（如 "WP_vAcctIn (sn, acctInId, ...)"）
#   - 96% 的預測缺少 WP_M09.dbo. 前綴
#   - 原因：~2,900 筆資料不足以學會 9 表全 schema 的任務
#   - 解法：用 20K 資料量 + 更保守的超參數
#
# 資料切分：
#   - 按 View × Difficulty 分層抽樣（stratified split）
#   - 80% Train / 10% Val / 10% Test
#   - Test 集存檔供最終評估，訓練過程中不使用
#
# 用法:
#   python train__9views_20k_v0325.py
#   python train__9views_20k_v0325.py --no-rules        # 不含規則（ablation）
#   python train__9views_20k_v0325.py --epochs 3        # 自訂 epoch 數
#   python train__9views_20k_v0325.py --lr 1e-5         # 自訂學習率
#
# 輸出:
#   outputs/models/9views_20k_0325/final_model/
#   data/wp_m09/split_9views_20k_val.json    （驗證集）
#   data/wp_m09/split_9views_20k_test.json   （測試集）
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
DATE_STR     = "0325"

DATA_PATH    = r"data\wp_m09\train_9views_20k.json"
SPLIT_SEED   = 42          # 固定種子確保可重現
TRAIN_RATIO  = 0.80
VAL_RATIO    = 0.10
TEST_RATIO   = 0.10

# 切分後的輸出路徑
SPLIT_VAL_PATH  = r"data\wp_m09\split_9views_20k_val.json"
SPLIT_TEST_PATH = r"data\wp_m09\split_9views_20k_test.json"

# ---- DoRA ----
LORA_R        = 16
LORA_ALPHA    = 32
LORA_DROPOUT  = 0.05
USE_DORA      = True

# ---- Training hyperparams ----
NUM_EPOCHS    = 2           # 20K 資料量大，2 epochs 應足夠（v0324: epoch 1.5 後即停滯）
BATCH_SIZE    = 2
GRAD_ACCUM    = 8           # effective batch = 16
EARLY_STOPPING_PATIENCE = 2 # 更嚴格：連續 2 次未改善即停
LEARNING_RATE = 2e-5
MAX_SEQ_LEN   = 1536        # 略增（9 views schema 較長）
LR_SCHEDULER  = "cosine"
WARMUP_RATIO  = 0.06
WEIGHT_DECAY  = 0.01


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
    " -- Inventory. NO isDel. NO date. pNo=seq#. isSale: 0=normal, 1=stop-purchase, 2=stop-sale, 3=stop-both.\n"

    "WP_vProduct(pNo, pName, pNameS, pBarcode, pCode, pUnit, pUName, priceStd, priceLow, priceMem, priceBat, isPvDiscount, isSale, costStd, costAvg, pvSn, pvId, pvName, pvNameS, qtyNow, qtySafe, pvDiscount)"
    " -- Product. NO isDel. NO date. pNo=seq#. isSale: 0=normal, 1=stop-purchase, 2=stop-sale, 3=stop-both.\n"

    "WP_vProvider(sn, pvId, pvName, pvNameS, pvKId, pvBoss, pvTel, pvCityId, pvZoneId, pvCity, pvZone, pvAddr, ctactName, ctactTel, fax, email, taxId, isStop, invoTitle, bankId, bankName, bankAccount, bankAcctName, memo, pvKName, pvDiscount)"
    " -- Supplier. NO isDel. isStop=N/Y. NO date. SELECT pvId (not pvSn).\n"

    "WP_vMemberDeposit(sn, memId, memName, isStop, empId, isDel, amount, endDate, OutStkId)"
    " -- Member deposit. isDel=N for active. endDate=expiry. isStop=N/Y.\n"

    "WP_vPdCombine(sn, pNo, pName, pNameS, pBarcode, priceStd, isUpdStock, pNoS, pQty, isDel, kind, timeCreate, timeUpdate, sPName, sPNameS, sPBarcode, sPriceStd, sPUName, sIsTax, sCostStd)"
    " -- Combo product. isDel=N active. pNo=main product, pNoS=sub-product seq#, pQty=sub qty."
)

BUSINESS_RULES = (
    "Rules:\n"
    "1. isDel views (AcctIn/AcctOut/OutStock/Transfer/MemberDeposit/PdCombine): isDel='N' header, +dtlIsDel='N' detail columns (if exists).\n"
    "2. No-isDel views (Inventory/Product): NEVER add isDel. isSale: 0=normal, 1=stop-purchase-only, 2=stop-sale-only, 3=stop-both. Sellable=IN('0','1'). Non-sellable=IN('2','3').\n"
    "3. Provider: isStop='N' for active. SELECT pvId (not pvSn). pvSn is for JOIN only.\n"
    "4. SUM/AVG header amount: SELECT SUM(amount) FROM (SELECT DISTINCT xxxId, amount FROM ... WHERE isDel='N') sub.\n"
    "5. NEVER use SUM(DISTINCT amount) or AVG(DISTINCT amount) — always use subquery dedup.\n"
    "6. Count orders: COUNT(DISTINCT xxxId), never COUNT(*) on header-detail views.\n"
    "7. Date: LEFT(xxxId,8)='YYYYMMDD'. Only isDel views. pNo=seq number, NOT date.\n"
    "8. T-SQL only: use TOP N, never LIMIT. Use N'str' for Chinese strings.\n"
    "9. MemberDeposit: endDate is datetime for expiry. Use endDate>GETDATE() for active deposits.\n"
    "10. PdCombine: pNo is main combo product, pNoS is sub-product. pQty is quantity of sub-product in combo."
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
# Stratified Split（分層抽樣）
# ============================================================
def stratified_split(data, train_ratio, val_ratio, test_ratio, seed=42):
    """
    按 View × Difficulty 分層切分資料。

    確保每個 (view, difficulty) 組合在 train/val/test 中的比例一致。
    """
    assert abs(train_ratio + val_ratio + test_ratio - 1.0) < 1e-6

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
        n_val = max(1, round(n * val_ratio))
        n_test = max(1, round(n * test_ratio))
        n_train = n - n_val - n_test

        if n_train < 1:
            # 資料太少，全部放 train
            train_set.extend(items)
            continue

        train_set.extend(items[:n_train])
        val_set.extend(items[n_train:n_train + n_val])
        test_set.extend(items[n_train + n_val:])

    return train_set, val_set, test_set


# ============================================================
# Build Chat Template prompts
# ============================================================
def build_system_prompt(include_rules=True):
    """建構 system prompt（全 9 表 schema）"""
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
# Data Loading & Split
# ============================================================
def load_and_split_data(tokenizer, include_rules=True):
    """載入 20K 資料，80/10/10 分層切分，建立 chat template"""

    # 1. 載入
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        raw = json.load(f)
    print(f"  Loaded: {DATA_PATH} ({len(raw)} samples)")

    # 2. 去重
    seen = set()
    deduped = []
    for s in raw:
        norm = normalize_query(s.get("query", ""))
        if norm and norm not in seen:
            seen.add(norm)
            deduped.append(s)
    print(f"  After dedup: {len(deduped)} (removed {len(raw) - len(deduped)})")

    # 3. 分層切分
    train_data, val_data, test_data = stratified_split(
        deduped, TRAIN_RATIO, VAL_RATIO, TEST_RATIO, seed=SPLIT_SEED
    )
    print(f"\n  Stratified split (seed={SPLIT_SEED}):")
    print(f"    Train: {len(train_data)} ({len(train_data)/len(deduped)*100:.1f}%)")
    print(f"    Val:   {len(val_data)} ({len(val_data)/len(deduped)*100:.1f}%)")
    print(f"    Test:  {len(test_data)} ({len(test_data)/len(deduped)*100:.1f}%)")

    # 4. 存檔 val/test（供後續評估使用）
    os.makedirs(os.path.dirname(SPLIT_VAL_PATH), exist_ok=True)
    with open(SPLIT_VAL_PATH, "w", encoding="utf-8") as f:
        json.dump(val_data, f, ensure_ascii=False, indent=2)
    with open(SPLIT_TEST_PATH, "w", encoding="utf-8") as f:
        json.dump(test_data, f, ensure_ascii=False, indent=2)
    print(f"    Val  saved: {SPLIT_VAL_PATH}")
    print(f"    Test saved: {SPLIT_TEST_PATH}")

    # 5. 顯示切分分布
    for split_name, split_data in [("Train", train_data), ("Val", val_data), ("Test", test_data)]:
        views = Counter(extract_view_from_sql(s.get("query", "")) for s in split_data)
        diffs = Counter(s.get("difficulty", "unknown") for s in split_data)
        print(f"\n    {split_name} distribution:")
        print(f"      Views:      {dict(sorted(views.items()))}")
        print(f"      Difficulty:  {dict(sorted(diffs.items()))}")

    # 6. 建立 chat template
    system_prompt = build_system_prompt(include_rules=include_rules)

    def build_dataset(samples, label):
        texts = []
        skipped = 0
        for s in samples:
            question = s.get("question", "")
            sql = s.get("query", "").strip().rstrip(';').strip()
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

        if texts:
            lengths = [t["tok_len"] for t in texts]
            print(f"\n  {label}: {len(texts)} samples (skipped: {skipped})")
            print(f"    Token len: Min={min(lengths)}, Max={max(lengths)}, "
                  f"Mean={statistics.mean(lengths):.0f}, Median={statistics.median(lengths):.0f}")
        return texts

    train_texts = build_dataset(train_data, "Train dataset")
    val_texts = build_dataset(val_data, "Val dataset")

    # 7. 顯示 sample
    print(f"\n  Sample prompt (first 500 chars):")
    print("-" * 60)
    print(train_texts[0]["text"][:500])
    print("-" * 60)

    train_dataset = Dataset.from_list([{"text": t["text"]} for t in train_texts])
    val_dataset = Dataset.from_list([{"text": t["text"]} for t in val_texts]) if val_texts else None

    return train_dataset, len(train_texts), val_dataset


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
def train(model, tokenizer, dataset, output_dir, val_dataset=None):
    os.makedirs(output_dir, exist_ok=True)
    has_eval = val_dataset is not None and len(val_dataset) > 0

    eff_batch = BATCH_SIZE * GRAD_ACCUM
    steps_per_epoch = max(1, len(dataset) // eff_batch)
    eval_save_steps = max(1, steps_per_epoch // 2)  # 每 0.5 epoch

    sft_cfg = SFTConfig(
        output_dir=output_dir,
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
        train_dataset=dataset,
        eval_dataset=val_dataset if has_eval else None,
        args=sft_cfg,
        callbacks=callbacks,
    )

    print(f"\nTraining:")
    print(f"  Samples:     {len(dataset)}")
    print(f"  Epochs:      {NUM_EPOCHS}")
    print(f"  Batch:       {BATCH_SIZE} x {GRAD_ACCUM} = {eff_batch}")
    print(f"  Steps/epoch: ~{steps_per_epoch}")
    print(f"  Total steps: ~{steps_per_epoch * NUM_EPOCHS}")
    print(f"  Eval every:  {eval_save_steps} steps (~0.5 epoch)")
    print(f"  LR:          {LEARNING_RATE} ({LR_SCHEDULER})")
    print(f"  MAX_SEQ_LEN: {MAX_SEQ_LEN}")
    print(f"  Val samples: {len(val_dataset) if val_dataset else 0}")
    print(f"  Output:      {output_dir}\n")

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
        "train_script":     "train__9views_20k_v0325.py",
        "methodology":      "Spider/BIRD-style: Full-DB 9-view schema + Business rules + Chat Template",
        "include_rules":    not args.no_rules,
        "method":           "DoRA" if USE_DORA else "LoRA",
        "lora_r":           LORA_R,
        "lora_alpha":       LORA_ALPHA,
        "train_samples":    n_samples,
        "data_split":       f"{TRAIN_RATIO}/{VAL_RATIO}/{TEST_RATIO} (stratified by view×difficulty)",
        "split_seed":       SPLIT_SEED,
        "epochs":           NUM_EPOCHS,
        "effective_batch":  BATCH_SIZE * GRAD_ACCUM,
        "learning_rate":    LEARNING_RATE,
        "max_seq_len":      MAX_SEQ_LEN,
        "date":             DATE_STR,
        "early_stopping":   f"patience={EARLY_STOPPING_PATIENCE}",
        "views":            "9 (AcctIn, AcctOut, OutStock, Transfer, Inventory, Product, Provider, MemberDeposit, PdCombine)",
        "val_path":         SPLIT_VAL_PATH,
        "test_path":        SPLIT_TEST_PATH,
    }

    # 記錄最終 loss
    for entry in reversed(trainer.state.log_history):
        if "loss" in entry:
            info["final_train_loss"] = round(entry["loss"], 4)
            break
    for entry in reversed(trainer.state.log_history):
        if "eval_loss" in entry:
            info["best_eval_loss"] = round(entry["eval_loss"], 6)
            break

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
    p = argparse.ArgumentParser(description="9 Views × 20K Training (v0325)")
    p.add_argument("--no-rules", action="store_true", help="Disable business rules (ablation)")
    p.add_argument("--epochs", type=int, default=None, help=f"Override NUM_EPOCHS (default: {NUM_EPOCHS})")
    p.add_argument("--lr", type=float, default=None, help=f"Override learning rate (default: {LEARNING_RATE})")
    p.add_argument("--output-suffix", type=str, default="", help="Custom output directory suffix")
    return p.parse_args()


def main():
    args = parse_args()

    # 允許命令列覆蓋超參數
    global NUM_EPOCHS, LEARNING_RATE
    if args.epochs is not None:
        NUM_EPOCHS = args.epochs
    if args.lr is not None:
        LEARNING_RATE = args.lr

    suffix = args.output_suffix or "9views_20k"
    if args.no_rules:
        suffix += "_norule"
    output_dir = f"outputs/models/{suffix}_{DATE_STR}"

    print("=" * 70)
    print(f"9 Views × 20K Training v{DATE_STR}")
    print(f"  Data:    {DATA_PATH}")
    print(f"  Split:   {TRAIN_RATIO}/{VAL_RATIO}/{TEST_RATIO} (stratified)")
    print(f"  Views:   9 (7 original + MemberDeposit + PdCombine)")
    print(f"  Rules:   {'Yes' if not args.no_rules else 'No (ablation)'}")
    print(f"  Epochs:  {NUM_EPOCHS}")
    print(f"  LR:      {LEARNING_RATE}")
    print(f"  Output:  {output_dir}")
    print("=" * 70)

    tokenizer, model = load_model_and_tokenizer()
    dataset, n_samples, val_dataset = load_and_split_data(
        tokenizer, include_rules=not args.no_rules
    )
    model = apply_dora(model)
    trainer = train(model, tokenizer, dataset, output_dir, val_dataset=val_dataset)
    final_dir = save_model(trainer, tokenizer, n_samples, output_dir, args)

    # 印出評估指令
    print("\n" + "=" * 70)
    print("Training complete! Evaluation commands:")
    print("=" * 70)

    print(f"\n# Val 評估（訓練過程中已用，確認基本品質）:")
    print(f"python eval__9views_v0325.py `")
    print(f"    --model {final_dir} `")
    print(f"    --gold {SPLIT_VAL_PATH} `")
    print(f"    --output outputs/eval_9views_20k_{DATE_STR}_val.json")

    print(f"\n# Test 評估（最終成績，訓練過程中未見過）:")
    print(f"python eval__9views_v0325.py `")
    print(f"    --model {final_dir} `")
    print(f"    --gold {SPLIT_TEST_PATH} `")
    print(f"    --output outputs/eval_9views_20k_{DATE_STR}_test.json")

    print()


if __name__ == "__main__":
    main()
