# eval__enterprise_v0322.py
# ============================================================
# 企業 Text-to-SQL 落地實驗 — 評估腳本
# 配合 train__enterprise_v0322.py 的全 7 表 schema 格式
#
# 關鍵改動 vs eval_v2：
#   - 推論時使用全 7 表 schema（與訓練一致）
#   - 不再需要 keyword-based 表推斷
#   - 模型自行從 schema 選擇正確的表
#   - 同時輸出 EM、EX、per-view、per-difficulty 結果
#
# 用法:
#   python eval__enterprise_v0322.py \
#       --model outputs/models/enterprise_full_0322/final_model \
#       --gold data/wp_m09/val_claude_en_spider_v2.json \
#       --output outputs/eval_enterprise_full_0322.json \
#       --db-host "SHANE\SQLEXPRESS" --db-trusted
# ============================================================

import json
import os
import re
import sys
import time
import argparse
import torch
from collections import defaultdict
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from peft import PeftModel


# ============================================================
# Import schema from training script (single source of truth)
# ============================================================
try:
    from train__enterprise_v0322 import (
        FULL_SCHEMA,
        BUSINESS_RULES,
        SINGLE_VIEW_SCHEMAS,
        build_system_prompt,
    )
    print("[OK] Imported schema from train__enterprise_v0322.py")
except ImportError:
    print("[WARN] Cannot import from training script, using inline fallback")
    FULL_SCHEMA = ""
    BUSINESS_RULES = ""
    SINGLE_VIEW_SCHEMAS = {}
    def build_system_prompt(schema_mode="full", table=None, include_rules=True):
        return "You are an expert T-SQL assistant for WP_M09. Generate ONLY the SQL query."


MODEL_PATH = "meta-llama/Llama-3.1-8B-Instruct"


# ============================================================
# SQL Normalization
# ============================================================
def normalize_sql(sql):
    s = sql.strip().rstrip(';').strip()
    s = re.sub(r'\[?WP_M09\]?\.\[?dbo\]?\.', '', s)
    s = re.sub(r'\[(\w+)\]', r'\1', s)
    s = re.sub(r'\s+', ' ', s)
    keywords = ['SELECT','FROM','WHERE','AND','OR','GROUP','BY','ORDER','HAVING',
                'JOIN','LEFT','RIGHT','INNER','ON','AS','IN','NOT','NULL','IS',
                'BETWEEN','LIKE','EXISTS','DISTINCT','TOP','COUNT','SUM','AVG',
                'MAX','MIN','LIMIT','ASC','DESC','CASE','WHEN','THEN','ELSE','END']
    for kw in keywords:
        s = re.sub(r'\b' + kw + r'\b', kw, s, flags=re.IGNORECASE)
    return s.strip()


def extract_table_from_sql(sql):
    m = re.search(r'WP_M09\.dbo\.(\w+)', sql)
    if m:
        return m.group(1)
    m = re.search(r'\[WP_M09\]\.\[dbo\]\.\[(\w+)\]', sql)
    return m.group(1) if m else ""


# ============================================================
# Model loading
# ============================================================
def load_model(model_path):
    print(f"\nLoading model: {model_path}")

    # Read training info
    info_path = os.path.join(model_path, "training_info.json")
    schema_mode = "full"
    include_rules = True
    base_model = MODEL_PATH

    if os.path.exists(info_path):
        with open(info_path, "r") as f:
            info = json.load(f)
        base_model = info.get("base_model", MODEL_PATH)
        schema_mode = info.get("schema_mode", "full")
        include_rules = info.get("include_rules", True)
        print(f"  Base model:   {base_model}")
        print(f"  Schema mode:  {schema_mode}")
        print(f"  Rules:        {include_rules}")
        print(f"  Train samples:{info.get('train_samples', '?')}")
        print(f"  Methodology:  {info.get('methodology', '?')}")

    bnb_cfg = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16,
        bnb_4bit_use_double_quant=True,
    )
    tokenizer = AutoTokenizer.from_pretrained(model_path, use_fast=True)
    tokenizer.pad_token = tokenizer.eos_token
    tokenizer.padding_side = "left"

    model = AutoModelForCausalLM.from_pretrained(
        base_model,
        quantization_config=bnb_cfg,
        device_map="auto",
        torch_dtype=torch.bfloat16,
    )
    model = PeftModel.from_pretrained(model, model_path)
    model.eval()
    print("Model loaded")
    return tokenizer, model, schema_mode, include_rules


# ============================================================
# Inference
# ============================================================
def build_inference_prompt(question, tokenizer, schema_mode="full", include_rules=True):
    """建構推論 prompt — 全 7 表 schema（與訓練一致）。"""
    system = build_system_prompt(schema_mode=schema_mode, include_rules=include_rules)
    messages = [
        {"role": "system", "content": system},
        {"role": "user",   "content": question},
    ]
    return tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)


def generate_sql(model, tokenizer, prompt, max_new_tokens=300):
    inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=2048)
    inputs = {k: v.to(model.device) for k, v in inputs.items()}

    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=max_new_tokens,
            do_sample=False,
            temperature=1.0,
            pad_token_id=tokenizer.eos_token_id,
        )

    new_tokens = outputs[0][inputs["input_ids"].shape[1]:]
    raw = tokenizer.decode(new_tokens, skip_special_tokens=True).strip()

    # Extract SQL
    lines = raw.split("\n")
    sql_lines = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith("```") or line.startswith("--") or line.lower().startswith("note"):
            break
        sql_lines.append(line)
        if line.endswith(";"):
            break

    return " ".join(sql_lines).strip().rstrip(";").strip()


# ============================================================
# Execution on SQL Server
# ============================================================
def execute_sql(sql, db_host, db_trusted=True):
    try:
        import pyodbc
        conn_str = (f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_host};"
                    f"DATABASE=WP_M09;Trusted_Connection={'yes' if db_trusted else 'no'};")
        conn = pyodbc.connect(conn_str, timeout=30)
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        conn.close()
        return sorted([tuple(str(x) for x in row) for row in results])
    except Exception as e:
        return f"ERROR: {e}"


# ============================================================
# Main evaluation
# ============================================================
def evaluate(model, tokenizer, gold_path, output_path,
             db_host=None, db_trusted=True,
             schema_mode="full", include_rules=True):

    with open(gold_path, "r", encoding="utf-8") as f:
        gold_data = json.load(f)

    print(f"\n{'='*70}")
    print(f"Enterprise Text-to-SQL Evaluation")
    print(f"  Val set:     {gold_path} ({len(gold_data)} samples)")
    print(f"  Schema mode: {schema_mode}")
    print(f"  Rules:       {include_rules}")
    print(f"  DB:          {db_host or 'N/A (no EX)'}")
    print(f"{'='*70}\n")

    predictions = []
    start_time = time.time()

    for idx, sample in enumerate(gold_data):
        question = sample.get("question", "")
        gold_sql = sample.get("query", "")
        gold_table = sample.get("view", "")
        difficulty = sample.get("difficulty", "unknown")

        prompt = build_inference_prompt(question, tokenizer, schema_mode, include_rules)
        pred_sql = generate_sql(model, tokenizer, prompt)

        # Extract predicted table from SQL
        pred_table = extract_table_from_sql(pred_sql)

        predictions.append({
            "idx": idx,
            "question": question,
            "gold_sql": gold_sql,
            "pred_sql": pred_sql,
            "gold_table": gold_table,
            "pred_table": pred_table,
            "difficulty": difficulty,
        })

        if (idx + 1) % 20 == 0:
            elapsed = time.time() - start_time
            rate = (idx + 1) / elapsed
            remaining = (len(gold_data) - idx - 1) / rate
            print(f"  [{idx+1}/{len(gold_data)}] {rate:.1f} samples/sec, ETA: {remaining/60:.1f} min")

    elapsed = time.time() - start_time
    print(f"\n  Inference: {len(predictions)} samples in {elapsed:.0f}s ({len(predictions)/elapsed:.1f}/s)")

    # ---- Table Selection Accuracy ----
    table_correct = sum(1 for p in predictions
                        if p["gold_table"] and p["pred_table"] == p["gold_table"])
    table_total = sum(1 for p in predictions if p["gold_table"])
    if table_total > 0:
        print(f"\n  Table Selection: {table_correct}/{table_total} = {table_correct/table_total*100:.1f}%")

        # Per-view table selection
        view_sel = defaultdict(lambda: {"total": 0, "correct": 0})
        for p in predictions:
            if p["gold_table"]:
                view_sel[p["gold_table"]]["total"] += 1
                if p["pred_table"] == p["gold_table"]:
                    view_sel[p["gold_table"]]["correct"] += 1

        print(f"  Per-view table selection:")
        for v in sorted(view_sel.keys()):
            s = view_sel[v]
            print(f"    {v}: {s['correct']}/{s['total']} ({s['correct']/s['total']*100:.1f}%)")

    # ---- String EM ----
    em_count = 0
    for p in predictions:
        if normalize_sql(p["pred_sql"]) == normalize_sql(p["gold_sql"]):
            em_count += 1
            p["em"] = True
        else:
            p["em"] = False

    print(f"\n  String EM: {em_count}/{len(predictions)} = {em_count/len(predictions)*100:.2f}%")

    # ---- Execution Accuracy ----
    ex_count = 0
    ex_errors = 0
    if db_host:
        for p in predictions:
            gold_result = execute_sql(p["gold_sql"], db_host, db_trusted)
            pred_result = execute_sql(p["pred_sql"], db_host, db_trusted)

            if isinstance(gold_result, str) or isinstance(pred_result, str):
                p["ex"] = False
                ex_errors += 1
                if isinstance(pred_result, str):
                    p["ex_error"] = pred_result
            elif gold_result == pred_result:
                p["ex"] = True
                ex_count += 1
            else:
                p["ex"] = False

        print(f"  Execution Accuracy: {ex_count}/{len(predictions)} = {ex_count/len(predictions)*100:.2f}%")
        print(f"  Execution errors: {ex_errors}")
    else:
        print("  [INFO] No DB host provided, skipping EX")

    # ---- Per-View Results ----
    view_stats = defaultdict(lambda: {"total": 0, "em": 0, "ex": 0, "table_ok": 0})
    for p in predictions:
        view = p.get("gold_table", "unknown")
        view_stats[view]["total"] += 1
        if p.get("em"):
            view_stats[view]["em"] += 1
        if p.get("ex"):
            view_stats[view]["ex"] += 1
        if p["pred_table"] == p["gold_table"]:
            view_stats[view]["table_ok"] += 1

    print(f"\n  {'View':<20} {'N':>4} {'Table':>8} {'EM':>8} {'EX':>8}")
    print(f"  {'-'*52}")
    for view in sorted(view_stats.keys()):
        s = view_stats[view]
        n = s["total"]
        tbl = f"{s['table_ok']/n*100:.0f}%" if n > 0 else "N/A"
        em = f"{s['em']/n*100:.1f}%" if n > 0 else "N/A"
        ex = f"{s['ex']/n*100:.1f}%" if db_host and n > 0 else "N/A"
        print(f"  {view:<20} {n:>4} {tbl:>8} {em:>8} {ex:>8}")

    # ---- Per-Difficulty Results ----
    diff_stats = defaultdict(lambda: {"total": 0, "em": 0, "ex": 0})
    for p in predictions:
        d = p.get("difficulty", "unknown")
        diff_stats[d]["total"] += 1
        if p.get("em"):
            diff_stats[d]["em"] += 1
        if p.get("ex"):
            diff_stats[d]["ex"] += 1

    print(f"\n  {'Difficulty':<12} {'N':>4} {'EM':>8} {'EX':>8}")
    print(f"  {'-'*36}")
    for d in ["easy", "medium", "hard", "unknown"]:
        s = diff_stats.get(d)
        if not s:
            continue
        n = s["total"]
        em = f"{s['em']/n*100:.1f}%" if n > 0 else "N/A"
        ex = f"{s['ex']/n*100:.1f}%" if db_host and n > 0 else "N/A"
        print(f"  {d:<12} {n:>4} {em:>8} {ex:>8}")

    # ---- Error Analysis ----
    print(f"\n  Error Analysis (first 10 wrong predictions):")
    wrong = [p for p in predictions if not p.get("em")]
    for p in wrong[:10]:
        print(f"    [{p['idx']}] Table: gold={p['gold_table']} pred={p['pred_table']}")
        print(f"         Q: {p['question'][:80]}")
        print(f"         Gold: {p['gold_sql'][:100]}")
        print(f"         Pred: {p['pred_sql'][:100]}")
        print()

    # ---- Save report ----
    report = {
        "mode": "enterprise",
        "schema_mode": schema_mode,
        "include_rules": include_rules,
        "total": len(predictions),
        "table_selection_accuracy": round(table_correct / table_total * 100, 2) if table_total > 0 else None,
        "string_em": em_count,
        "string_em_pct": round(em_count / len(predictions) * 100, 2),
        "execution_accuracy": ex_count if db_host else None,
        "execution_accuracy_pct": round(ex_count / len(predictions) * 100, 2) if db_host else None,
        "view_results": {v: dict(s) for v, s in view_stats.items()},
        "difficulty_results": {d: dict(s) for d, s in diff_stats.items()},
        "predictions": predictions,
    }

    if output_path:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"\n  Results saved: {output_path}")

    return report


# ============================================================
# Main
# ============================================================
def parse_args():
    p = argparse.ArgumentParser(description="Enterprise Text-to-SQL Evaluation")
    p.add_argument("--model", required=True, help="Model path")
    p.add_argument("--gold", required=True, help="Gold data path")
    p.add_argument("--output", default=None, help="Output JSON path")
    p.add_argument("--db-host", default=None, help="SQL Server host")
    p.add_argument("--db-trusted", action="store_true", help="Windows Auth")
    return p.parse_args()


def main():
    args = parse_args()
    tokenizer, model, schema_mode, include_rules = load_model(args.model)
    output = args.output or f"outputs/eval_enterprise_{schema_mode}.json"
    evaluate(model, tokenizer, args.gold, output,
             db_host=args.db_host, db_trusted=args.db_trusted,
             schema_mode=schema_mode, include_rules=include_rules)


if __name__ == "__main__":
    main()
