# train__qwen_9views_v0326.py
# ============================================================
# Qwen 系列模型 × 9 Views × 20K 訓練腳本 v0326
#
# 支援模型切換（--model 參數）：
#   qwen3.5-9b       → Qwen/Qwen3.5-9B              （首選推薦）
#   qwen2.5-coder-7b → Qwen/Qwen2.5-Coder-7B-Instruct（保守穩定）
#   llama3.1-8b      → meta-llama/Llama-3.1-8B-Instruct（舊版基線）
#   自訂路徑          → 任意 HuggingFace 模型路徑
#
# 基於 v0325 改進：
#   1. 模型切換機制（--model 參數）
#   2. 自動偵測 Qwen vs Llama 的 tokenizer 差異（pad/eos token）
#   3. Qwen 的 chat template 自動適配（apply_chat_template 統一處理）
#   4. 保留 v0325 所有防過擬合 + Blackwell 加速設定
#
# 用法:
#   python train__qwen_9views_v0326.py                           # 預設 Qwen3.5-9B
#   python train__qwen_9views_v0326.py --model qwen2.5-coder-7b # Qwen2.5 Coder
#   python train__qwen_9views_v0326.py --model llama3.1-8b       # Llama 基線
#   python train__qwen_9views_v0326.py --model Qwen/Qwen3.5-4B  # 自訂路徑
#   python train__qwen_9views_v0326.py --no-rules                # 不含規則
#   python train__qwen_9views_v0326.py --epochs 2 --lr 1e-5     # 覆蓋超參數
#
# 輸出:
#   outputs/models/qwen35_9b_9views_0326/final_model/
#   data/wp_m09/split_9views_20k_val.json
#   data/wp_m09/split_9views_20k_test.json
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
# Blackwell Optimizations
# ============================================================
try:
    torch.backends.cudnn.conv.fp32_precision = "tf32"
    torch.backends.cuda.matmul.fp32_precision = "tf32"
except AttributeError:
    torch.backends.cuda.matmul.allow_tf32 = True
    torch.backends.cudnn.allow_tf32 = True
torch.backends.cudnn.benchmark = True


# ============================================================
# Model Presets（模型預設配置）
# ============================================================
MODEL_PRESETS = {
    "qwen3.5-9b": {
        "path": "Qwen/Qwen3.5-9B",
        "short_name": "qwen35_9b",
        "family": "qwen",
    },
    "qwen2.5-coder-7b": {
        "path": "Qwen/Qwen2.5-Coder-7B-Instruct",
        "short_name": "qwen25_coder_7b",
        "family": "qwen",
    },
    "llama3.1-8b": {
        "path": "meta-llama/Llama-3.1-8B-Instruct",
        "short_name": "llama31_8b",
        "family": "llama",
    },
}


def resolve_model(model_key: str) -> dict:
    """解析模型參數，支援預設名稱或自訂 HuggingFace 路徑。"""
    key = model_key.lower().strip()
    if key in MODEL_PRESETS:
        return MODEL_PRESETS[key]

    # 自訂路徑：從路徑推斷 family 和 short_name
    path = model_key
    name_lower = path.lower()
    if "qwen" in name_lower:
        family = "qwen"
    elif "llama" in name_lower:
        family = "llama"
    else:
        family = "unknown"

    # 從路徑生成簡短名稱
    short = path.split("/")[-1].lower().replace("-", "_").replace(".", "")[:20]

    return {
        "path": path,
        "short_name": short,
        "family": family,
    }


# ============================================================
# Settings
# ============================================================
DATE_STR     = "0326"

DATA_PATH    = r"data\wp_m09\train_9views_20k.json"
SPLIT_SEED   = 42
TRAIN_RATIO  = 0.80
VAL_RATIO    = 0.10
TEST_RATIO   = 0.10

SPLIT_VAL_PATH  = r"data\wp_m09\split_9views_20k_val.json"
SPLIT_TEST_PATH = r"data\wp_m09\split_9views_20k_test.json"

# ---- DoRA ----
LORA_R        = 8
LORA_ALPHA    = 16
LORA_DROPOUT  = 0.15
USE_DORA      = True

# ---- Training hyperparams ----
NUM_EPOCHS    = 1
BATCH_SIZE    = 4
GRAD_ACCUM    = 4           # effective batch = 16
EARLY_STOPPING_PATIENCE = 3
LEARNING_RATE = 5e-6
MAX_SEQ_LEN   = 1536
LR_SCHEDULER  = "cosine"
WARMUP_RATIO  = 0.10
WEIGHT_DECAY  = 0.05

# ---- Eval 抽樣 ----
VAL_EVAL_MAX  = 200


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
# Stratified Split
# ============================================================
def stratified_split(data, train_ratio, val_ratio, test_ratio, seed=42):
    assert abs(train_ratio + val_ratio + test_ratio - 1.0) < 1e-6
    rng = random.Random(seed)

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
    """建構 system prompt（全 9 表 schema）。供 eval 腳本 import 使用。"""
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

    # 4. 存檔 val/test
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

    # 7. Val 抽樣
    if len(val_texts) > VAL_EVAL_MAX:
        rng = random.Random(SPLIT_SEED)
        val_texts_eval = rng.sample(val_texts, VAL_EVAL_MAX)
        print(f"  Val eval subset: {len(val_texts_eval)} / {len(val_texts)} (sampled for faster eval)")
    else:
        val_texts_eval = val_texts

    # 8. 顯示 sample
    print(f"\n  Sample prompt (first 500 chars):")
    print("-" * 60)
    print(train_texts[0]["text"][:500])
    print("-" * 60)

    train_dataset = Dataset.from_list([{"text": t["text"]} for t in train_texts])
    val_dataset = Dataset.from_list([{"text": t["text"]} for t in val_texts_eval]) if val_texts_eval else None

    return train_dataset, len(train_texts), val_dataset


# ============================================================
# Model loading
# ============================================================
def load_model_and_tokenizer(model_info: dict):
    model_path = model_info["path"]
    family = model_info["family"]

    print(f"\nLoading base model: {model_path} (family: {family}) ...")

    # GPU 資訊
    if torch.cuda.is_available():
        gpu_name = torch.cuda.get_device_name(0)
        cc = torch.cuda.get_device_capability(0)
        vram = torch.cuda.get_device_properties(0).total_memory / 1024**3
        print(f"  GPU: {gpu_name} (CC {cc[0]}.{cc[1]}, {vram:.1f} GB)")
        print(f"  TF32: enabled, cuDNN benchmark: enabled")

    bnb_cfg = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16,
        bnb_4bit_use_double_quant=True,
    )

    tokenizer = AutoTokenizer.from_pretrained(model_path, use_fast=True)

    # ---- Tokenizer 設定（Qwen vs Llama 差異處理）----
    if tokenizer.pad_token is None:
        if family == "qwen":
            # Qwen 系列通常有 <|endoftext|> 或 <|end|> 作為 eos
            # 使用 eos_token 作為 pad_token
            tokenizer.pad_token = tokenizer.eos_token
            print(f"  Tokenizer: pad_token set to eos_token = {repr(tokenizer.eos_token)}")
        else:
            tokenizer.pad_token = tokenizer.eos_token
            print(f"  Tokenizer: pad_token set to eos_token = {repr(tokenizer.eos_token)}")
    else:
        print(f"  Tokenizer: pad_token = {repr(tokenizer.pad_token)}, eos_token = {repr(tokenizer.eos_token)}")

    tokenizer.padding_side = "right"
    tokenizer.model_max_length = MAX_SEQ_LEN

    # 載入模型
    model = AutoModelForCausalLM.from_pretrained(
        model_path,
        quantization_config=bnb_cfg,
        device_map="auto",
        dtype=torch.bfloat16,
        attn_implementation="sdpa",
    )
    model.config.use_cache = False

    # Gradient checkpointing
    model.gradient_checkpointing_enable(
        gradient_checkpointing_kwargs={"use_reentrant": False}
    )

    print(f"  Attention: SDPA")
    print(f"  Gradient checkpointing: enabled")

    # 驗證 chat template
    try:
        test_msgs = [
            {"role": "system", "content": "Test"},
            {"role": "user", "content": "Hello"},
        ]
        test_prompt = tokenizer.apply_chat_template(test_msgs, tokenize=False, add_generation_prompt=True)
        test_tokens = len(tokenizer(test_prompt)["input_ids"])
        print(f"  Chat template: OK (test prompt = {test_tokens} tokens)")
    except Exception as e:
        print(f"  [WARN] Chat template test failed: {e}")
        print(f"  This model may not support chat template properly.")

    print("Base model loaded")
    return tokenizer, model


# ============================================================
# DoRA / LoRA setup
# ============================================================
def apply_dora(model, model_info: dict):
    family = model_info["family"]

    # Qwen 和 Llama 的 target modules 名稱相同（都是 q/k/v/o/gate/up/down_proj）
    target_modules = ["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"]

    lora_cfg = LoraConfig(
        r=LORA_R,
        lora_alpha=LORA_ALPHA,
        target_modules=target_modules,
        lora_dropout=LORA_DROPOUT,
        bias="none",
        task_type=TaskType.CAUSAL_LM,
        use_dora=USE_DORA,
    )
    model = get_peft_model(model, lora_cfg)
    model.print_trainable_parameters()
    print(f"Fine-tune: {'DoRA' if USE_DORA else 'LoRA'} (r={LORA_R}, alpha={LORA_ALPHA}, dropout={LORA_DROPOUT})")
    return model


# ============================================================
# Training
# ============================================================
def train(model, tokenizer, dataset, output_dir, val_dataset=None):
    os.makedirs(output_dir, exist_ok=True)
    has_eval = val_dataset is not None and len(val_dataset) > 0

    eff_batch = BATCH_SIZE * GRAD_ACCUM
    steps_per_epoch = max(1, len(dataset) // eff_batch)
    eval_save_steps = max(1, steps_per_epoch // 4)

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
        # --- Blackwell 加速 ---
        dataloader_num_workers=2,
        dataloader_pin_memory=True,
        gradient_checkpointing=True,
        gradient_checkpointing_kwargs={"use_reentrant": False},
        torch_compile=False,           # Windows 無 Triton
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
    print(f"  Eval every:  {eval_save_steps} steps (~0.25 epoch)")
    print(f"  LR:          {LEARNING_RATE} ({LR_SCHEDULER}, warmup={WARMUP_RATIO})")
    print(f"  Weight decay: {WEIGHT_DECAY}")
    print(f"  MAX_SEQ_LEN: {MAX_SEQ_LEN}")
    print(f"  Val samples: {len(val_dataset) if val_dataset else 0} (eval subset)")
    print(f"  Optimizations: SDPA + TF32 + gradient_ckpt + pin_memory")
    print(f"  Output:      {output_dir}\n")

    trainer.train()
    return trainer


# ============================================================
# Save
# ============================================================
def save_model(trainer, tokenizer, n_samples, output_dir, args, model_info):
    final_dir = os.path.join(output_dir, "final_model")
    os.makedirs(final_dir, exist_ok=True)

    trainer.model.save_pretrained(final_dir)
    tokenizer.save_pretrained(final_dir)

    info = {
        "base_model":       model_info["path"],
        "model_family":     model_info["family"],
        "train_script":     "train__qwen_9views_v0326.py",
        "methodology":      "Spider/BIRD-style: Full-DB 9-view schema + Business rules + Chat Template",
        "include_rules":    not args.no_rules,
        "method":           "DoRA" if USE_DORA else "LoRA",
        "lora_r":           LORA_R,
        "lora_alpha":       LORA_ALPHA,
        "lora_dropout":     LORA_DROPOUT,
        "train_samples":    n_samples,
        "data_split":       f"{TRAIN_RATIO}/{VAL_RATIO}/{TEST_RATIO} (stratified by view x difficulty)",
        "split_seed":       SPLIT_SEED,
        "epochs":           NUM_EPOCHS,
        "effective_batch":  BATCH_SIZE * GRAD_ACCUM,
        "learning_rate":    LEARNING_RATE,
        "weight_decay":     WEIGHT_DECAY,
        "max_seq_len":      MAX_SEQ_LEN,
        "date":             DATE_STR,
        "early_stopping":   f"patience={EARLY_STOPPING_PATIENCE}",
        "views":            "9 (AcctIn, AcctOut, OutStock, Transfer, Inventory, Product, Provider, MemberDeposit, PdCombine)",
        "val_path":         SPLIT_VAL_PATH,
        "test_path":        SPLIT_TEST_PATH,
        "optimizations":    "SDPA + TF32 + gradient_checkpointing + pin_memory",
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
    p = argparse.ArgumentParser(
        description="Qwen/Llama × 9 Views × 20K Training (v0326)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
模型預設名稱:
  qwen3.5-9b        Qwen/Qwen3.5-9B               (首選推薦)
  qwen2.5-coder-7b  Qwen/Qwen2.5-Coder-7B-Instruct (保守穩定)
  llama3.1-8b       meta-llama/Llama-3.1-8B-Instruct (舊版基線)

範例:
  python train__qwen_9views_v0326.py                           # Qwen3.5-9B
  python train__qwen_9views_v0326.py --model qwen2.5-coder-7b
  python train__qwen_9views_v0326.py --model Qwen/Qwen3.5-4B  # 自訂路徑
        """
    )
    p.add_argument("--model", type=str, default="qwen3.5-9b",
                   help="模型名稱或 HuggingFace 路徑 (default: qwen3.5-9b)")
    p.add_argument("--no-rules", action="store_true",
                   help="Disable business rules (ablation)")
    p.add_argument("--epochs", type=int, default=None,
                   help=f"Override NUM_EPOCHS (default: {NUM_EPOCHS})")
    p.add_argument("--lr", type=float, default=None,
                   help=f"Override learning rate (default: {LEARNING_RATE})")
    p.add_argument("--output-suffix", type=str, default="",
                   help="Custom output directory suffix")
    return p.parse_args()


def main():
    args = parse_args()

    # 解析模型
    model_info = resolve_model(args.model)

    # 覆蓋超參數
    global NUM_EPOCHS, LEARNING_RATE
    if args.epochs is not None:
        NUM_EPOCHS = args.epochs
    if args.lr is not None:
        LEARNING_RATE = args.lr

    # 輸出目錄
    suffix = args.output_suffix or f"{model_info['short_name']}_9views"
    if args.no_rules:
        suffix += "_norule"
    output_dir = f"outputs/models/{suffix}_{DATE_STR}"

    print("=" * 70)
    print(f"Qwen/Llama × 9 Views Training v{DATE_STR}")
    print(f"  Model:   {model_info['path']} ({model_info['family']})")
    print(f"  Data:    {DATA_PATH}")
    print(f"  Split:   {TRAIN_RATIO}/{VAL_RATIO}/{TEST_RATIO} (stratified)")
    print(f"  Views:   9 (7 original + MemberDeposit + PdCombine)")
    print(f"  Rules:   {'Yes' if not args.no_rules else 'No (ablation)'}")
    print(f"  Epochs:  {NUM_EPOCHS}")
    print(f"  LR:      {LEARNING_RATE}")
    print(f"  DoRA:    r={LORA_R}, alpha={LORA_ALPHA}, dropout={LORA_DROPOUT}")
    print(f"  Output:  {output_dir}")
    print("=" * 70)

    tokenizer, model = load_model_and_tokenizer(model_info)
    dataset, n_samples, val_dataset = load_and_split_data(
        tokenizer, include_rules=not args.no_rules
    )
    model = apply_dora(model, model_info)
    trainer = train(model, tokenizer, dataset, output_dir, val_dataset=val_dataset)
    final_dir = save_model(trainer, tokenizer, n_samples, output_dir, args, model_info)

    # 印出評估指令
    print("\n" + "=" * 70)
    print("Training complete! Evaluation commands:")
    print("=" * 70)

    print(f"\n# Val 評估:")
    print(f"python eval__9views_v0326.py `")
    print(f"    --model {final_dir} `")
    print(f"    --gold {SPLIT_VAL_PATH} `")
    print(f"    --output outputs/eval_{model_info['short_name']}_{DATE_STR}_val.json")

    print(f"\n# Test 評估:")
    print(f"python eval__9views_v0326.py `")
    print(f"    --model {final_dir} `")
    print(f"    --gold {SPLIT_TEST_PATH} `")
    print(f"    --output outputs/eval_{model_info['short_name']}_{DATE_STR}_test.json")

    print()


if __name__ == "__main__":
    main()
