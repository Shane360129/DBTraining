# evaluate_correct_test.py
# 評估 DoRA 訓練模型在測試集上的 Exact Match 準確率
#
# 執行方式:
#   python evaluate_correct_test.py
#
# 輸出:
#   outputs/evaluation_{MMDD}.json

import json
import re
import os
import torch
from datetime import datetime
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from peft import PeftModel

# ============================================================
# 設定區
# ============================================================
BASE_MODEL  = "meta-llama/Llama-3.1-8B-Instruct"

# ← 固定指向 Spider 格式訓練的模型（不再自動尋找最新）
MODEL_PATH  = r"outputs\models\wp_m09_dora_0312_spider\final_model"

TEST_PATH      = r"data\wp_m09\test.json"       # 測試集 (182 筆)
DATE_STR       = datetime.now().strftime("%m%d")
OUTPUT_PATH    = f"outputs/evaluation_{DATE_STR}_dora.json"
MAX_NEW_TOKENS = 128
# ============================================================


def normalize_sql(sql: str) -> str:
    sql = sql.upper().strip().rstrip(";")
    sql = re.sub(r"\s+", " ", sql)
    return sql


def load_model(model_path: str):
    print(f"基礎模型: {BASE_MODEL}")
    print(f"LoRA 權重: {model_path}")

    bnb_cfg = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16,
        bnb_4bit_use_double_quant=True,
    )

    tokenizer = AutoTokenizer.from_pretrained(model_path)
    tokenizer.pad_token    = tokenizer.eos_token
    tokenizer.padding_side = "left"   # 推論時用 left padding

    base = AutoModelForCausalLM.from_pretrained(
        BASE_MODEL,
        quantization_config=bnb_cfg,
        device_map="auto",
        dtype=torch.bfloat16,
    )
    model = PeftModel.from_pretrained(base, model_path)
    model.eval()
    print("模型載入完成 ✅\n")
    return tokenizer, model


# ============================================================
# TABLE_NOTES — 與 train_dora_spider.py 完全一致（7 張 view）
# ============================================================
TABLE_NOTES = {
    "WP_vProduct":       "Note: Filter by date using pNo LIKE 'YYYYMMDD%'. No separate date column.",
    "WP_vInventory":     "Note: Filter by date using pNo LIKE 'YYYYMMDD%'. No separate date column.",
    "WP_vTransfer":      "Note: Filter by date using LEFT(TransferId,8)='YYYYMMDD' or LEFT(TransferId,6)='YYYYMM'.",
    "WP_vAcctIn":        "Note: Filter by date using LEFT(acctInId,8)='YYYYMMDD'. isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vAcctOut":       "Note: Filter by date using LEFT(acctOutId,8)='YYYYMMDD'. isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vInStock":       "Note: Filter by date using LEFT(InStkId,8)='YYYYMMDD'. isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vOutStockUnion": "Note: Filter by date using LEFT(OutStkId,8)='YYYYMMDD'. isDel='N' AND dtlIsDel='N' for active records.",
}


def build_prompt(sample: dict) -> str:
    """推論時的 prompt（不含 SQL answer，與訓練格式一致）"""
    table    = sample.get("table", sample.get("db_id", ""))
    question = sample.get("question", "")
    note     = TABLE_NOTES.get(table, "")

    lines = [f"Table: {table}"]
    if note:
        lines.append(note)
    lines.append(f"Question: {question}")
    lines.append("SQL:")   # ← 讓模型補全

    return "\n".join(lines)


def main():
    print("=" * 60)
    print(f"DoRA 模型評估  ({DATE_STR})")
    print("=" * 60)

    if not os.path.exists(MODEL_PATH):
        print(f"❌ 找不到模型: {MODEL_PATH}")
        return

    if not os.path.exists(TEST_PATH):
        print(f"❌ 找不到測試集: {TEST_PATH}")
        return

    # 載入測試集
    with open(TEST_PATH, "r", encoding="utf-8") as f:
        test_samples = json.load(f)
    print(f"測試集: {TEST_PATH}  ({len(test_samples)} 筆)\n")

    # 載入模型
    tokenizer, model = load_model(MODEL_PATH)

    # ── 批次推論 ──────────────────────────────────────────
    EVAL_BATCH  = 8   # VRAM 不夠可改 4
    prompts_all = [build_prompt(s) for s in test_samples]
    gold_sqls   = [s.get("query") or s.get("sql", "") for s in test_samples]
    pred_sqls   = []

    print(f"開始推論 (batch={EVAL_BATCH})...")
    for b_start in range(0, len(test_samples), EVAL_BATCH):
        b_end   = min(b_start + EVAL_BATCH, len(test_samples))
        batch_p = prompts_all[b_start:b_end]

        inputs = tokenizer(
            batch_p,
            return_tensors="pt",
            truncation=True,
            max_length=400,
            padding=True,
        ).to(model.device)

        with torch.no_grad():
            outputs = model.generate(
                **inputs,
                max_new_tokens=MAX_NEW_TOKENS,
                do_sample=False,
                pad_token_id=tokenizer.eos_token_id,
                eos_token_id=tokenizer.eos_token_id,
                repetition_penalty=1.1,
            )

        # left padding：用 attention_mask 計算每筆真實 prompt 長度
        attn_mask  = inputs["attention_mask"]
        padded_len = attn_mask.shape[1]
        for j, out in enumerate(outputs):
            real_len    = int(attn_mask[j].sum().item())
            input_start = padded_len - real_len        # pad 在左側
            decoded = tokenizer.decode(
                out[input_start + real_len:],
                skip_special_tokens=True,
            ).strip()
            pred_sqls.append(decoded.split("\n")[0].strip())

        print(f"  批次 [{b_end:>3}/{len(test_samples)}] 完成", flush=True)

    # ── 統計 ─────────────────────────────────────────────
    results       = []
    em_count      = 0
    by_difficulty: dict[str, dict] = {}
    by_table:      dict[str, dict] = {}

    for i, sample in enumerate(test_samples):
        gold_sql = gold_sqls[i]
        pred_sql = pred_sqls[i]

        em = normalize_sql(pred_sql) == normalize_sql(gold_sql)
        if em:
            em_count += 1

        diff  = sample.get("difficulty", "unknown")
        table = sample.get("table", "unknown")

        by_difficulty.setdefault(diff,  {"total": 0, "em": 0})
        by_table.setdefault(table,      {"total": 0, "em": 0})
        by_difficulty[diff]["total"] += 1
        by_table[table]["total"]     += 1
        if em:
            by_difficulty[diff]["em"] += 1
            by_table[table]["em"]     += 1

        results.append({
            "index":       i,
            "table":       table,
            "difficulty":  diff,
            "question":    sample.get("question", ""),
            "gold_sql":    gold_sql,
            "pred_sql":    pred_sql,
            "exact_match": em,
        })

        # 每 20 筆印一次進度
        if (i + 1) % 20 == 0 or (i + 1) == len(test_samples):
            acc = em_count / (i + 1) * 100
            print(f"  [{i+1}/{len(test_samples)}]  EM: {em_count}/{i+1} ({acc:.1f}%)")

    total       = len(test_samples)
    overall_acc = em_count / total * 100

    # ── 難度摘要 ─────────────────────────────────────────
    diff_summary = {
        diff: {
            "total":    stat["total"],
            "em":       stat["em"],
            "accuracy": round(stat["em"] / stat["total"] * 100, 1),
        }
        for diff, stat in sorted(by_difficulty.items())
    }

    # ── 表格摘要 ─────────────────────────────────────────
    table_summary = {
        table: {
            "total":    stat["total"],
            "em":       stat["em"],
            "accuracy": round(stat["em"] / stat["total"] * 100, 1),
        }
        for table, stat in sorted(by_table.items(), key=lambda x: -x[1]["total"])
    }

    # ── 儲存報告 ─────────────────────────────────────────
    report = {
        "overall": {
            "model":    MODEL_PATH,
            "test_set": TEST_PATH,
            "total":    total,
            "em":       em_count,
            "accuracy": round(overall_acc, 2),
        },
        "by_difficulty": diff_summary,
        "by_table":      table_summary,
        "details":       results,
    }

    os.makedirs("outputs", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # ── 終端摘要 ─────────────────────────────────────────
    print(f"\n{'='*60}")
    print("【評估結果】")
    print(f"{'='*60}")
    print(f"  模型:      {MODEL_PATH}")
    print(f"  測試集:    {TEST_PATH}")
    print(f"  總體 EM:   {em_count}/{total}  ({overall_acc:.2f}%)")

    print(f"\n  按難度:")
    for diff, s in diff_summary.items():
        print(f"    {diff:<12} {s['em']:>4}/{s['total']:<4}  ({s['accuracy']}%)")

    print(f"\n  按資料表:")
    for table, s in table_summary.items():
        print(f"    {table:<30} {s['em']:>4}/{s['total']:<4}  ({s['accuracy']}%)")

    print(f"\n  報告已儲存: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()