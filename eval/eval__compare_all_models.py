# evaluate_all_models.py
# 一次跑完 outputs/models/ 下所有模型，輸出比較報告
#
# 執行方式:
#   python evaluate_all_models.py
#
# 輸出:
#   outputs/eval_all_{MMDD}.json      （詳細）
#   outputs/eval_all_{MMDD}_summary.txt （終端摘要，方便截圖）

import json
import re
import os
import gc
import torch
from datetime import datetime
from pathlib import Path
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig, StoppingCriteria, StoppingCriteriaList
from peft import PeftModel

# ============================================================
# 設定區
# ============================================================
BASE_MODEL     = "meta-llama/Llama-3.1-8B-Instruct"
MODELS_ROOT    = r"outputs\models"
TEST_PATH      = r"data\wp_m09\test.json"   # 主測試集
MAX_NEW_TOKENS = 64   # SQL 很少超過 60 token
EVAL_BATCH     = 8     # 每批筆數，VRAM 不足改 4
DATE_STR       = datetime.now().strftime("%m%d")
OUTPUT_JSON    = f"outputs/eval_all_{DATE_STR}.json"
OUTPUT_TXT     = f"outputs/eval_all_{DATE_STR}_summary.txt"

# 若需要略過某些資料夾，填在這裡（資料夾名稱，不含路徑）
SKIP_MODELS = [
    # "wp_m09_0228_failed",   # 已知壞掉的
]
# ============================================================


TABLE_NOTES = {
    "WP_vProduct":   "Note: Filter by date using pNo LIKE 'YYYYMMDD%'. No separate date column.",
    "WP_vInventory": "Note: Filter by date using pNo LIKE 'YYYYMMDD%'. No separate date column.",
    "WP_vTransfer":  "Note: Filter by date using TransferId LIKE 'YYYYMMDD%'. No separate date column.",
}


def normalize_sql(sql: str) -> str:
    sql = sql.upper().strip().rstrip(";")
    sql = re.sub(r"\s+", " ", sql)
    return sql


def build_prompt(sample: dict) -> str:
    schema   = sample.get("schema", "")
    table    = sample.get("table", "")
    question = sample.get("question", "")

    prefix    = f"Schema: {schema}\n" if schema else (f"Table: {table}\n" if table else "")
    note      = TABLE_NOTES.get(table, "")
    note_part = f"{note}\n" if note else ""

    return f"{prefix}{note_part}Question: {question}\nSQL:"


def find_all_models() -> list[tuple[str, str]]:
    """
    回傳 [(model_name, model_path), ...]
    優先找 final_model 子目錄，沒有就用根目錄本身
    """
    root = Path(MODELS_ROOT)
    if not root.exists():
        print(f"❌ 找不到 {MODELS_ROOT}")
        return []

    entries = sorted(root.iterdir())
    result  = []
    for entry in entries:
        if not entry.is_dir():
            continue
        name = entry.name
        if name in SKIP_MODELS:
            print(f"  ⏭ 略過: {name}")
            continue

        # 優先使用 final_model 子目錄
        final = entry / "final_model"
        if final.exists() and (final / "adapter_config.json").exists():
            result.append((name, str(final)))
        elif (entry / "adapter_config.json").exists():
            result.append((name, str(entry)))
        else:
            print(f"  ⚠ 無有效權重，略過: {name}")

    return result


def load_model(model_path: str):
    bnb_cfg = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16,
        bnb_4bit_use_double_quant=True,
    )
    tokenizer = AutoTokenizer.from_pretrained(model_path)
    tokenizer.pad_token    = tokenizer.eos_token
    tokenizer.padding_side = "left"

    base = AutoModelForCausalLM.from_pretrained(
        BASE_MODEL,
        quantization_config=bnb_cfg,
        device_map="auto",
        dtype=torch.bfloat16,
    )
    model = PeftModel.from_pretrained(base, model_path)
    model.eval()
    return tokenizer, model


def unload_model(model, tokenizer):
    """釋放 VRAM，避免跑下一個模型時爆記憶體"""
    del model, tokenizer
    gc.collect()
    torch.cuda.empty_cache()


def evaluate_one(tokenizer, model, test_samples: list) -> dict:
    """對一個模型跑完整評估，回傳統計結果"""
    prompts_all = [build_prompt(s) for s in test_samples]
    gold_sqls   = [s.get("query") or s.get("sql", "") for s in test_samples]
    pred_sqls   = []

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

        # 遇到換行就停止（SQL 是單行，生成到 \n 即完成）
        newline_id = tokenizer.encode("\n", add_special_tokens=False)[-1]

        with torch.no_grad():
            outputs = model.generate(
                **inputs,
                max_new_tokens=MAX_NEW_TOKENS,
                do_sample=False,
                pad_token_id=tokenizer.eos_token_id,
                eos_token_id=[tokenizer.eos_token_id, newline_id],
            )

        attn_mask  = inputs["attention_mask"]
        padded_len = attn_mask.shape[1]
        for j, out in enumerate(outputs):
            real_len    = int(attn_mask[j].sum().item())
            input_start = padded_len - real_len
            decoded = tokenizer.decode(
                out[input_start + real_len:], skip_special_tokens=True
            ).strip()
            pred_sqls.append(decoded.split("\n")[0].strip())

        print(f"    批次 [{b_end:>3}/{len(test_samples)}]", flush=True)

    # 統計
    em_count      = 0
    by_difficulty = {}
    by_table      = {}
    details       = []

    for i, sample in enumerate(test_samples):
        gold = gold_sqls[i]
        pred = pred_sqls[i]
        em   = normalize_sql(pred) == normalize_sql(gold)
        if em:
            em_count += 1

        diff  = sample.get("difficulty", "unknown")
        table = sample.get("table", "unknown")

        by_difficulty.setdefault(diff,  {"total": 0, "em": 0})
        by_table.setdefault(table,       {"total": 0, "em": 0})
        by_difficulty[diff]["total"]  += 1
        by_table[table]["total"]      += 1
        if em:
            by_difficulty[diff]["em"] += 1
            by_table[table]["em"]     += 1

        details.append({
            "index": i, "table": table, "difficulty": diff,
            "question": sample.get("question", ""),
            "gold_sql": gold, "pred_sql": pred, "exact_match": em,
        })

    total   = len(test_samples)
    overall = round(em_count / total * 100, 2)

    return {
        "overall":       {"total": total, "em": em_count, "accuracy": overall},
        "by_difficulty": {
            k: {"total": v["total"], "em": v["em"],
                "accuracy": round(v["em"] / v["total"] * 100, 1)}
            for k, v in sorted(by_difficulty.items())
        },
        "by_table": {
            k: {"total": v["total"], "em": v["em"],
                "accuracy": round(v["em"] / v["total"] * 100, 1)}
            for k, v in sorted(by_table.items(), key=lambda x: -x[1]["total"])
        },
        "details": details,
    }


def print_model_result(name: str, result: dict, fout=None):
    o   = result["overall"]
    lines = [
        f"\n{'─'*60}",
        f"  模型: {name}",
        f"  總體 EM: {o['em']}/{o['total']}  ({o['accuracy']}%)",
        f"  按難度:",
    ]
    for diff, s in result["by_difficulty"].items():
        lines.append(f"    {diff:<12} {s['em']:>3}/{s['total']:<4} ({s['accuracy']}%)")
    lines.append(f"  按資料表:")
    for table, s in result["by_table"].items():
        lines.append(f"    {table:<30} {s['em']:>3}/{s['total']:<4} ({s['accuracy']}%)")

    text = "\n".join(lines)
    print(text)
    if fout:
        fout.write(text + "\n")


def main():
    if not os.path.exists(TEST_PATH):
        print(f"❌ 找不到測試集: {TEST_PATH}")
        return

    with open(TEST_PATH, "r", encoding="utf-8") as f:
        test_samples = json.load(f)

    print(f"測試集: {TEST_PATH}  ({len(test_samples)} 筆)")

    models = find_all_models()
    if not models:
        print("❌ 沒有找到任何可用模型")
        return

    print(f"\n找到 {len(models)} 個模型:")
    for name, path in models:
        print(f"  • {name}  ({path})")

    print(f"\n{'='*60}")
    print(f"開始逐一評估...")
    print(f"{'='*60}")

    all_results = {}
    os.makedirs("outputs", exist_ok=True)

    with open(OUTPUT_TXT, "w", encoding="utf-8") as fout:
        fout.write(f"WP_M09 全模型評估報告  {DATE_STR}\n")
        fout.write(f"測試集: {TEST_PATH}  ({len(test_samples)} 筆)\n")

        for idx, (name, path) in enumerate(models):
            print(f"\n[{idx+1}/{len(models)}] 載入: {name}")
            print(f"  路徑: {path}")

            try:
                tokenizer, model = load_model(path)
                print(f"  模型載入完成 ✅，開始推理...")

                result = evaluate_one(tokenizer, model, test_samples)
                all_results[name] = {"model_path": path, **result}

                print_model_result(name, result, fout)

                # 釋放記憶體
                unload_model(model, tokenizer)
                print(f"  VRAM 已釋放 ✅")

            except Exception as e:
                print(f"  ❌ 評估失敗: {e}")
                all_results[name] = {"error": str(e)}
                try:
                    unload_model(model, tokenizer)
                except Exception:
                    pass

        # ── 最終排行榜 ──────────────────────────────────────
        ranking_lines = [
            f"\n{'='*60}",
            f"【排行榜】",
            f"{'='*60}",
            f"  {'排名':<4} {'模型':<35} {'EM':>10}",
            f"  {'─'*55}",
        ]

        ranked = sorted(
            [(n, r) for n, r in all_results.items() if "overall" in r],
            key=lambda x: -x[1]["overall"]["accuracy"]
        )
        for rank, (name, result) in enumerate(ranked, 1):
            o = result["overall"]
            ranking_lines.append(
                f"  {rank:<4} {name:<35} {o['em']:>3}/{o['total']:<4} ({o['accuracy']:>6}%)"
            )

        ranking_text = "\n".join(ranking_lines)
        print(ranking_text)
        fout.write(ranking_text + "\n")

    # 儲存完整 JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    print(f"\n  詳細報告: {OUTPUT_JSON}")
    print(f"  文字摘要: {OUTPUT_TXT}")


if __name__ == "__main__":
    main()