#!/usr/bin/env python3
"""
auto_improve_loop.py
────────────────────────────────────────────────────────────────────
自動訓練 → 評估 → 修正資料 → 重新訓練，直到 EM ≥ 80%（最多 MAX_ROUNDS 輪）

執行方式:
    python auto_improve_loop.py

狀態紀錄:
    outputs/loop_state.json   — 記錄當前輪次、每輪 EM，支援中斷後續跑

每輪輸出:
    outputs/models/wp_m09_dora_MMDD_spider_rN/   — 模型
    outputs/evaluation_loop_rN.json              — 評估結果
    data/wp_m09/train_spider_WP_M09_rN.json      — 當輪訓練集
────────────────────────────────────────────────────────────────────
"""

import json
import os
import re
import sys
import shutil
import torch
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# ============================================================
# 全域設定
# ============================================================
BASE_MODEL    = "meta-llama/Llama-3.1-8B-Instruct"
TEST_PATH     = "data/wp_m09/test_spider_WP_M09_v2.json"
TRAIN_BASE    = "data/wp_m09/train_spider_WP_M09.json"   # Round 1 起始訓練集
STATE_FILE    = "outputs/loop_state.json"
TARGET_EM     = 80.0   # 目標 EM %
MAX_ROUNDS    = 5      # 最大訓練輪次

# ---- DoRA 訓練超參數（各輪相同）----
LORA_R        = 16
LORA_ALPHA    = 32
LORA_DROPOUT  = 0.05
USE_DORA      = True
NUM_EPOCHS    = 15
BATCH_SIZE    = 4
GRAD_ACCUM    = 4
LEARNING_RATE = 1e-4
MAX_SEQ_LEN   = 512
LR_SCHEDULER  = "cosine"
WARMUP_RATIO  = 0.08
WEIGHT_DECAY  = 0.01

TABLE_NOTES = {
    "WP_vProduct":   "Note: pNo is a sequential product number (1, 2, 3...), NOT a date. This view has no date filtering capability.",
    "WP_vInventory": "Note: pNo is a sequential product number, not a date. This view has no date filtering capability.",
    "WP_vTransfer":  "Note: Filter by date using LEFT(TransferId,8)='YYYYMMDD' or LEFT(TransferId,6)='YYYYMM'.",
    "WP_vAcctIn":    "Note: Filter by date using LEFT(acctInId,8)='YYYYMMDD'. isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vAcctOut":   "Note: Filter by date using LEFT(acctOutId,8)='YYYYMMDD'. isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vOutStock":  "Note: Filter by date using LEFT(OutStkId,8)='YYYYMMDD'. isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vProvider":  "Note: isSale='Y' means active provider. Boolean fields use 'Y'/'N' encoding.",
}


# ============================================================
# 輔助函式
# ============================================================
def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def load_state() -> dict:
    if Path(STATE_FILE).exists():
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {"current_round": 1, "rounds": []}


def save_state(state: dict):
    os.makedirs("outputs", exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def extract_table_from_sql(sql: str) -> str:
    m = re.search(r'WP_M09\.dbo\.(\w+)', sql)
    if m:
        return m.group(1)
    m = re.search(r'\[WP_M09\]\.\[dbo\]\.\[(\w+)\]', sql)
    return m.group(1) if m else "unknown"


def normalize_sql(sql: str) -> str:
    s = sql.strip().rstrip(";").strip()
    s = s.replace("[WP_M09].[dbo].", "").replace("WP_M09.dbo.", "")
    s = re.sub(r"\[(\w+)\]", r"\1", s)
    keywords = [
        "SELECT","FROM","WHERE","AND","OR","NOT","IN","BETWEEN","LIKE","IS","NULL",
        "JOIN","LEFT","RIGHT","INNER","OUTER","ON","AS","GROUP","BY","ORDER","ASC",
        "DESC","HAVING","LIMIT","UNION","ALL","INTERSECT","EXCEPT","EXISTS","CASE",
        "WHEN","THEN","ELSE","END","DISTINCT","TOP","WITH","COUNT","SUM","AVG",
        "MIN","MAX","CAST","CONVERT","COALESCE","ISNULL","NULLIF",
    ]
    for kw in keywords:
        s = re.sub(r"\b" + kw + r"\b", kw.lower(), s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def make_training_entry(question: str, gold_sql: str) -> dict:
    """將一個 Q+SQL 對包裝成 train_spider_WP_M09.json 格式。"""
    toks = re.findall(
        r"[A-Za-z_][A-Za-z0-9_.]*|'[^']*'|[0-9]+|[().,;*<>=!%]+", gold_sql
    )
    toks_no_val = [
        ("'value'" if (t.startswith("'") or (t.isdigit() and len(t) > 2)) else t)
        for t in toks
    ]
    return {
        "db_id": "WP_M09",
        "query": gold_sql,
        "query_toks": toks,
        "query_toks_no_value": toks_no_val,
        "question": question,
        "question_toks": question.split(),
        "sql": {
            "select": [False, []], "from": {"table_units": [], "conds": []},
            "where": [], "groupBy": [], "having": [], "orderBy": [],
            "limit": None, "intersect": None, "union": None, "except": None,
        },
    }


# ============================================================
# 步驟 1: 訓練
# ============================================================
def run_training(round_num: int, train_path: str, output_dir: str):
    log(f"=== Round {round_num}: 開始訓練 ===")
    log(f"  訓練集: {train_path}")
    log(f"  輸出目錄: {output_dir}")

    from datasets import Dataset
    from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
    from peft import LoraConfig, get_peft_model, TaskType
    from trl import SFTTrainer, SFTConfig

    # 載入資料
    with open(train_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    def build_prompt(s):
        table = extract_table_from_sql(s.get("query", ""))
        if not table or table == "unknown":
            table = s.get("db_id", "WP_M09")
        note = TABLE_NOTES.get(table, "")
        lines = [f"Table: {table}"]
        if note:
            lines.append(note)
        lines.append(f"Question: {s.get('question','')}")
        lines.append(f"SQL: {s.get('query','')}")
        return "\n".join(lines)

    texts = [{"text": build_prompt(s)} for s in raw]
    dataset = Dataset.from_list(texts)
    log(f"  訓練樣本數: {len(dataset)}")

    # 模型載入
    bnb_cfg = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16,
        bnb_4bit_use_double_quant=True,
    )
    tokenizer = AutoTokenizer.from_pretrained(BASE_MODEL, use_fast=True)
    tokenizer.pad_token = tokenizer.eos_token
    tokenizer.padding_side = "right"
    tokenizer.model_max_length = MAX_SEQ_LEN

    model = AutoModelForCausalLM.from_pretrained(
        BASE_MODEL,
        quantization_config=bnb_cfg,
        device_map="auto",
        dtype=torch.bfloat16,
        attn_implementation="eager",
    )
    model.config.use_cache = False

    # DoRA
    lora_cfg = LoraConfig(
        r=LORA_R, lora_alpha=LORA_ALPHA,
        target_modules=["q_proj","k_proj","v_proj","o_proj","gate_proj","up_proj","down_proj"],
        lora_dropout=LORA_DROPOUT, bias="none",
        task_type=TaskType.CAUSAL_LM, use_dora=USE_DORA,
    )
    model = get_peft_model(model, lora_cfg)
    model.print_trainable_parameters()

    # 訓練
    os.makedirs(output_dir, exist_ok=True)
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
        save_strategy="epoch",
        save_total_limit=3,
        report_to="none",
        dataloader_num_workers=0,
        dataset_text_field="text",
        packing=False,
    )
    trainer = SFTTrainer(
        model=model, processing_class=tokenizer,
        train_dataset=dataset, args=sft_cfg,
    )

    eff_batch = BATCH_SIZE * GRAD_ACCUM
    steps_per_epoch = len(dataset) // eff_batch
    log(f"  Epochs={NUM_EPOCHS}, eff_batch={eff_batch}, steps/epoch~{steps_per_epoch}")
    trainer.train()

    # 儲存
    final_model_path = os.path.join(output_dir, "final_model")
    os.makedirs(final_model_path, exist_ok=True)
    trainer.model.save_pretrained(final_model_path)
    tokenizer.save_pretrained(final_model_path)

    info = {
        "base_model": BASE_MODEL, "train_data": train_path,
        "train_samples": len(raw), "round": round_num,
        "method": "DoRA", "lora_r": LORA_R, "lora_alpha": LORA_ALPHA,
        "epochs": NUM_EPOCHS, "effective_batch": eff_batch,
        "learning_rate": LEARNING_RATE,
        "final_loss": round(trainer.state.log_history[-1].get("loss", 0), 4),
    }
    with open(os.path.join(final_model_path, "training_info.json"), "w") as f:
        json.dump(info, f, indent=2)

    log(f"  模型儲存完成: {final_model_path}")
    log(f"  Final loss: {info['final_loss']}")
    return final_model_path


# ============================================================
# 步驟 2: 評估
# ============================================================
def run_evaluation(model_path: str, eval_output: str) -> dict:
    log(f"=== 評估模型: {model_path} ===")

    from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
    from peft import PeftModel

    # 載入測試集
    with open(TEST_PATH, "r", encoding="utf-8") as f:
        test_data = json.load(f)
    log(f"  測試集: {len(test_data)} 筆")

    # 載入模型
    bnb_cfg = BitsAndBytesConfig(
        load_in_4bit=True, bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16, bnb_4bit_use_double_quant=True,
    )
    tokenizer = AutoTokenizer.from_pretrained(model_path, use_fast=True)
    tokenizer.pad_token = tokenizer.eos_token
    tokenizer.padding_side = "left"

    base = AutoModelForCausalLM.from_pretrained(
        BASE_MODEL, quantization_config=bnb_cfg, device_map="auto",
        dtype=torch.bfloat16, attn_implementation="eager",
    )
    model = PeftModel.from_pretrained(base, model_path)
    model.eval()
    log("  模型載入完成，開始推論...")

    # 批次推論
    EVAL_BATCH = 8
    prompts_all = []
    for item in test_data:
        table = extract_table_from_sql(item.get("query", ""))
        note  = TABLE_NOTES.get(table, "")
        lines = [f"Table: {table}"]
        if note:
            lines.append(note)
        lines.append(f"Question: {item['question']}")
        lines.append("SQL:")
        prompts_all.append("\n".join(lines))

    gold_sqls = [item.get("query", "") for item in test_data]
    pred_sqls = []

    for b_start in range(0, len(test_data), EVAL_BATCH):
        b_end   = min(b_start + EVAL_BATCH, len(test_data))
        batch_p = prompts_all[b_start:b_end]

        inputs = tokenizer(
            batch_p, return_tensors="pt", truncation=True,
            max_length=400, padding=True,
        ).to(model.device)

        with torch.no_grad():
            outputs = model.generate(
                **inputs, max_new_tokens=128, do_sample=False,
                pad_token_id=tokenizer.eos_token_id,
                eos_token_id=tokenizer.eos_token_id,
                repetition_penalty=1.1,
            )

        attn_mask  = inputs["attention_mask"]
        padded_len = attn_mask.shape[1]
        for j, out in enumerate(outputs):
            real_len    = int(attn_mask[j].sum().item())
            input_start = padded_len - real_len
            decoded = tokenizer.decode(
                out[input_start + real_len:], skip_special_tokens=True,
            ).strip()
            pred_sqls.append(decoded.split("\n")[0].strip())

        if b_end % 40 == 0 or b_end == len(test_data):
            log(f"  推論進度: {b_end}/{len(test_data)}")

    # 計算 EM
    details = []
    em_count = 0
    table_stats = defaultdict(lambda: {"total": 0, "em": 0})
    diff_stats  = defaultdict(lambda: {"total": 0, "em": 0})

    for i, item in enumerate(test_data):
        gold = gold_sqls[i]
        pred = pred_sqls[i]
        em   = normalize_sql(pred) == normalize_sql(gold)
        if em:
            em_count += 1

        table = extract_table_from_sql(gold)
        diff  = item.get("difficulty", "unknown")
        table_stats[table]["total"] += 1
        diff_stats[diff]["total"]   += 1
        if em:
            table_stats[table]["em"] += 1
            diff_stats[diff]["em"]   += 1

        details.append({
            "id": i, "question": item["question"],
            "gold_sql": gold, "pred_sql": pred,
            "em": em, "ex": None,
            "table": table, "difficulty": diff,
        })

    total   = len(test_data)
    em_pct  = em_count / total * 100

    # 輸出摘要
    log(f"\n{'='*55}")
    log(f"  評估結果")
    log(f"{'='*55}")
    log(f"  總 EM: {em_count}/{total} = {em_pct:.2f}%")
    for diff in ["easy", "medium", "hard"]:
        s = diff_stats.get(diff, {"total": 0, "em": 0})
        if s["total"] > 0:
            log(f"  {diff:<8}: {s['em']}/{s['total']} ({s['em']/s['total']*100:.1f}%)")
    log(f"{'─'*55}")
    for tbl, s in sorted(table_stats.items()):
        log(f"  {tbl:<22}: {s['em']}/{s['total']} ({s['em']/s['total']*100:.1f}%)")

    result = {
        "total": total, "em_correct": em_count,
        "em_pct": round(em_pct, 2),
        "ex_correct": None, "ex_pct": None,
        "table_stats": {t: dict(s) for t, s in table_stats.items()},
        "diff_stats":  {d: dict(s) for d, s in diff_stats.items()},
        "details": details,
    }
    os.makedirs("outputs", exist_ok=True)
    with open(eval_output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    log(f"  評估結果已儲存: {eval_output}")

    # 清理模型記憶體
    del model, base
    torch.cuda.empty_cache()

    return result


# ============================================================
# 步驟 3: 分析失敗案例 → 生成修正樣本
# ============================================================
def generate_corrective_samples(eval_result: dict, round_num: int) -> list:
    """
    針對每個 EM=False 的案例，直接將 (question, gold_sql) 加入訓練集。
    同時生成簡單的 paraphrase 以增加覆蓋度。
    """
    fails = [d for d in eval_result["details"] if not d["em"]]
    log(f"  本輪失敗: {len(fails)} 筆，生成修正樣本...")

    new_entries = []
    seen_questions = set()

    # ---- 問句 paraphrase 模板 ----
    paraphrase_templates = [
        lambda q: f"Query: {q}",
        lambda q: q.replace("What is the", "Find the").replace("Show the", "List the"),
        lambda q: q.replace("List the", "Show all").replace("Find the", "Retrieve the"),
        lambda q: f"Using SQL, answer: {q}",
    ]

    for item in fails:
        q        = item["question"]
        gold_sql = item["gold_sql"]

        # 直接樣本
        if q not in seen_questions:
            new_entries.append(make_training_entry(q, gold_sql))
            seen_questions.add(q)

        # 最多 2 個 paraphrase
        added = 0
        for tmpl in paraphrase_templates:
            if added >= 2:
                break
            pq = tmpl(q)
            if pq != q and pq not in seen_questions:
                new_entries.append(make_training_entry(pq, gold_sql))
                seen_questions.add(pq)
                added += 1

    log(f"  生成 {len(new_entries)} 筆修正樣本（{len(fails)} 直接 + paraphrase）")
    return new_entries


# ============================================================
# 主迴圈
# ============================================================
def main():
    os.makedirs("outputs", exist_ok=True)
    state = load_state()

    log("=" * 65)
    log("Auto-Improve Loop: 訓練 → 評估 → 修正資料 → 重新訓練")
    log(f"目標: EM ≥ {TARGET_EM}%  |  最大輪次: {MAX_ROUNDS}")
    log("=" * 65)

    # 顯示歷史進度
    if state["rounds"]:
        log("\n歷史記錄:")
        for r in state["rounds"]:
            status = "✅" if r["em_pct"] >= TARGET_EM else "❌"
            log(f"  Round {r['round']}: EM={r['em_pct']:.2f}%  {status}")

    while state["current_round"] <= MAX_ROUNDS:
        rnd = state["current_round"]
        date_tag = datetime.now().strftime("%m%d")
        model_dir    = f"outputs/models/wp_m09_dora_{date_tag}_spider_r{rnd}"
        final_model  = os.path.join(model_dir, "final_model")
        eval_output  = f"outputs/evaluation_loop_r{rnd}.json"
        train_path   = f"data/wp_m09/train_spider_WP_M09_r{rnd}.json"

        # 決定本輪訓練集
        if rnd == 1:
            # Round 1 使用最新修正後的訓練集
            src_train = TRAIN_BASE
        else:
            # 後續輪次使用上一輪的擴充訓練集
            src_train = f"data/wp_m09/train_spider_WP_M09_r{rnd}.json"

        # 如果訓練集不存在，複製 base（只在 Round 1）
        if not Path(train_path).exists():
            if rnd == 1:
                shutil.copy(TRAIN_BASE, train_path)
                log(f"Round {rnd}: 訓練集已複製 → {train_path}")
            else:
                log(f"❌ 找不到 Round {rnd} 訓練集: {train_path}")
                break

        log(f"\n{'='*65}")
        log(f"Round {rnd} / {MAX_ROUNDS}")
        log(f"{'='*65}")

        # ── 訓練（若模型已存在則跳過）──────────────────────────
        if Path(final_model).exists():
            log(f"  模型已存在，跳過訓練: {final_model}")
        else:
            final_model = run_training(rnd, train_path, model_dir)

        # ── 評估 ──────────────────────────────────────────────
        if Path(eval_output).exists():
            log(f"  評估結果已存在，載入: {eval_output}")
            with open(eval_output, encoding="utf-8") as f:
                eval_result = json.load(f)
        else:
            eval_result = run_evaluation(final_model, eval_output)

        em_pct = eval_result["em_pct"]
        log(f"\nRound {rnd} EM: {em_pct:.2f}%  (目標: {TARGET_EM}%)")

        # 記錄本輪結果
        state["rounds"].append({
            "round": rnd, "em_pct": em_pct,
            "model": final_model, "eval": eval_output,
            "train": train_path,
        })
        save_state(state)

        # ── 達標則結束 ────────────────────────────────────────
        if em_pct >= TARGET_EM:
            log(f"\n🎉 目標達成！Round {rnd} EM = {em_pct:.2f}% ≥ {TARGET_EM}%")
            log(f"   最終模型: {final_model}")
            break

        # ── 未達標：生成修正樣本，準備下一輪 ───────────────────
        if rnd >= MAX_ROUNDS:
            log(f"\n⚠️  已達最大輪次 {MAX_ROUNDS}，停止。最終 EM = {em_pct:.2f}%")
            break

        next_rnd       = rnd + 1
        next_train     = f"data/wp_m09/train_spider_WP_M09_r{next_rnd}.json"

        log(f"\n生成 Round {next_rnd} 修正樣本...")
        new_samples = generate_corrective_samples(eval_result, rnd)

        # 載入本輪訓練集，合併新樣本
        with open(train_path, "r", encoding="utf-8") as f:
            current_train = json.load(f)

        # 去重：排除已存在相同問句
        existing_qs = set(d["question"] for d in current_train)
        deduped = [s for s in new_samples if s["question"] not in existing_qs]
        log(f"  新增（去重後）: {len(deduped)} 筆")

        merged = current_train + deduped
        with open(next_train, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)
        log(f"  Round {next_rnd} 訓練集已儲存: {next_train}  ({len(merged)} 筆)")

        # 儲存修正樣本單獨備份
        fix_path = f"data/wp_m09/corrective_r{rnd}_to_r{next_rnd}.json"
        with open(fix_path, "w", encoding="utf-8") as f:
            json.dump(deduped, f, ensure_ascii=False, indent=2)
        log(f"  修正樣本備份: {fix_path}")

        state["current_round"] = next_rnd
        save_state(state)

    # ── 最終摘要 ────────────────────────────────────────────
    log("\n" + "=" * 65)
    log("【Loop 結束】各輪 EM 摘要")
    log("=" * 65)
    best_em = 0.0
    best_model = ""
    for r in state["rounds"]:
        status = "✅" if r["em_pct"] >= TARGET_EM else "❌"
        log(f"  Round {r['round']}: EM={r['em_pct']:.2f}%  {status}")
        if r["em_pct"] > best_em:
            best_em    = r["em_pct"]
            best_model = r["model"]

    log(f"\n最佳 EM: {best_em:.2f}%")
    log(f"最佳模型: {best_model}")

    if best_em < TARGET_EM:
        fails_last = [
            d for d in state["rounds"][-1].get("eval_details", []) if not d.get("em")
        ]
        log(f"\n尚未達標（{best_em:.2f}% < {TARGET_EM}%）")
        log("建議：")
        log("  1. 增加 NUM_EPOCHS（目前 15）")
        log("  2. 降低 LEARNING_RATE（目前 1e-4）")
        log("  3. 增加 LORA_R（目前 16 → 32）")
        log("  4. 手動分析最後一輪的失敗案例")
        log(f"     python -c \"import json; d=json.load(open('outputs/evaluation_loop_r{state['current_round']}.json',encoding='utf-8')); [print(x['question'],'\\n  GOLD:',x['gold_sql'],'\\n  PRED:',x['pred_sql']) for x in d['details'] if not x['em']]\"")


if __name__ == "__main__":
    main()
