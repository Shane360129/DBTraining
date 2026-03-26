#!/usr/bin/env python3
"""
eval__em_and_execution_accuracy_v2.py
評估使用 Chat Template + Schema 格式訓練的 WP_M09 模型。

與 v1 的差異：
  1. 使用 Llama-3.1 Chat Template 格式（與 train__dora_spider_v0322.py 一致）
  2. System prompt 包含完整 7 個 View 的 Schema + 商業邏輯
  3. 不再需要 table inference（schema 包含所有表，模型自行選表）
  4. 輸出提取使用 assistant 回覆而非 "SQL:" 分割

用法:
  # EM + EX
  python eval__em_and_execution_accuracy_v2.py \
      --model outputs/models/wp_m09_dora_0322_schema/final_model \
      --gold data/wp_m09/val_claude_en_spider_v2.json \
      --output outputs/evaluation_0322_val.json \
      --db-host "SHANE\\SQLEXPRESS" --db-trusted
"""

import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict
from typing import Optional

# ============================================================
# SQL 正規化 (for EM) — 與 v1 完全相同
# ============================================================
def normalize_sql(sql: str) -> str:
    s = sql.strip()
    s = s.rstrip(';').strip()

    s = s.replace('[WP_M09].[dbo].', '')
    s = s.replace('WP_M09.dbo.', '')
    s = re.sub(r'\[(\w+)\]', r'\1', s)

    keywords = [
        'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'BETWEEN',
        'LIKE', 'IS', 'NULL', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
        'CROSS', 'ON', 'AS', 'GROUP', 'BY', 'ORDER', 'ASC', 'DESC',
        'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'ALL', 'INTERSECT', 'EXCEPT',
        'EXISTS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'DISTINCT',
        'TOP', 'WITH', 'INTO', 'INSERT', 'UPDATE', 'DELETE', 'SET',
        'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'CAST', 'CONVERT',
        'COALESCE', 'ISNULL', 'NULLIF', 'IIF',
    ]
    for kw in keywords:
        s = re.sub(r'\b' + kw + r'\b', kw.lower(), s, flags=re.IGNORECASE)

    s = re.sub(r'\s+', ' ', s).strip()
    return s


def compute_em(pred_sql: str, gold_sql: str) -> bool:
    return normalize_sql(pred_sql) == normalize_sql(gold_sql)


def execute_sql(cursor, sql: str) -> Optional[list]:
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        result = sorted([tuple(str(c) for c in row) for row in rows])
        return result
    except Exception:
        return None


def compute_ex(cursor, pred_sql: str, gold_sql: str) -> bool:
    gold_result = execute_sql(cursor, gold_sql)
    pred_result = execute_sql(cursor, pred_sql)
    if gold_result is None or pred_result is None:
        return False
    return gold_result == pred_result


def extract_table_from_sql(sql: str) -> str:
    m = re.search(r'WP_M09\.dbo\.(\w+)', sql)
    if m:
        return m.group(1)
    m = re.search(r'\[WP_M09\]\.\[dbo\]\.\[(\w+)\]', sql)
    return m.group(1) if m else "unknown"


# ============================================================
# Schema + System Prompt（與 train__dora_spider_v0322.py 完全一致）
# ============================================================
# Import schemas and prompt builder from training script (single source of truth)
from train__dora_spider_v0322 import VIEW_SCHEMAS, VIEW_LIST, VIEW_RULES

# For eval, we need to infer the table from the question (can't peek at gold SQL)
TABLE_KEYWORD_MAP = {
    "WP_vAcctIn":    ["receivable", "accounts receivable", "receipt", "收款", "應收"],
    "WP_vAcctOut":   ["payable", "accounts payable", "payment", "付款", "應付", "purchase order"],
    "WP_vInventory": ["inventory", "warehouse", "stock level", "庫存", "倉庫", "safe quantity"],
    "WP_vOutStock":  ["outbound", "out-stock", "outstkid", "sales order", "sales amount",
                      "sales total", "sales quantity", "member city", "出庫", "銷貨",
                      "cost of goods sold", "tax amount collected"],
    "WP_vProduct":   ["product", "barcode", "isSale", "產品", "商品", "unit name"],
    "WP_vProvider":  ["vendor", "supplier", "provider", "pvBoss", "供應商", "廠商",
                      "isstop", "ctacttel", "pvtel", "bankacc", "bankname", "taxid"],
    "WP_vTransfer":  ["transfer", "調撥", "destination warehouse", "source warehouse"],
}


def infer_table_from_question(question: str) -> str:
    """從問句推斷目標 view（推論時不能偷看 gold SQL）。"""
    q = question.lower()

    # 高優先級精確匹配
    if "transfer" in q or "調撥" in q:
        return "WP_vTransfer"
    if any(kw in q for kw in ["receivable", "receipt", "收款", "應收"]):
        return "WP_vAcctIn"
    if any(kw in q for kw in ["payable", "payment", "付款", "應付", "purchase order"]):
        return "WP_vAcctOut"
    if any(kw in q for kw in ["inventory", "warehouse", "庫存", "倉庫"]):
        return "WP_vInventory"
    if any(kw in q for kw in ["outbound", "out-stock", "outstkid", "sales order",
                               "sales amount", "sales total", "member city",
                               "出庫", "銷貨", "cost of goods sold"]):
        return "WP_vOutStock"
    if any(kw in q for kw in ["vendor", "supplier", "provider", "供應商", "廠商",
                               "isstop", "pvboss", "ctacttel", "pvtel", "bankname", "taxid"]):
        if "product" not in q and "barcode" not in q:
            return "WP_vProvider"
    if any(kw in q for kw in ["product", "barcode", "商品", "產品"]):
        return "WP_vProduct"

    # Weighted scoring fallback
    scores = {t: 0 for t in TABLE_KEYWORD_MAP}
    for table, keywords in TABLE_KEYWORD_MAP.items():
        for kw in keywords:
            if kw.lower() in q:
                scores[table] += 1
    best = max(scores, key=scores.get)
    if scores[best] > 0:
        return best
    return "WP_vOutStock"  # Default to OutStock instead of WP_M09


def build_system_prompt_for_table(table: str) -> str:
    """與訓練格式完全一致的 system prompt。"""
    schema = VIEW_SCHEMAS.get(table, "")
    rules = VIEW_RULES.get(table, "")
    parts = [
        "You are an expert T-SQL assistant for WP_M09 (SQL Server). Generate ONLY the SQL query.",
        VIEW_LIST,
        schema,
        rules,
    ]
    return "\n\n".join(parts)


# ============================================================
# Chat Template 推論
# ============================================================
def run_inference(model_path: str, test_data: list) -> tuple:
    import torch
    from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
    from peft import PeftModel

    print(f"\n載入模型: {model_path} ...")

    info_path = os.path.join(model_path, "training_info.json")
    if os.path.exists(info_path):
        with open(info_path) as f:
            info = json.load(f)
        base_model = info.get("base_model", "meta-llama/Llama-3.1-8B-Instruct")
    else:
        base_model = "meta-llama/Llama-3.1-8B-Instruct"

    bnb_cfg = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16,
        bnb_4bit_use_double_quant=True,
    )

    tokenizer = AutoTokenizer.from_pretrained(model_path, use_fast=True)
    base = AutoModelForCausalLM.from_pretrained(
        base_model,
        quantization_config=bnb_cfg,
        device_map="auto",
        torch_dtype=torch.bfloat16,
        attn_implementation="eager",
    )
    model = PeftModel.from_pretrained(base, model_path)
    model.eval()

    print(f"模型載入完成，開始推論 {len(test_data)} 筆 ...")

    predictions = []
    pred_records = []

    for i, item in enumerate(test_data):
        question = item["question"]
        gold_sql = item.get("query", "")

        # 推論時從問句推斷 table（不偷看 gold SQL）
        inferred_table = infer_table_from_question(question)
        system_prompt = build_system_prompt_for_table(inferred_table)

        # 使用 Chat Template（與訓練格式一致）
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ]

        prompt = tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
        )

        inputs = tokenizer(prompt, return_tensors="pt").to(model.device)

        with torch.no_grad():
            outputs = model.generate(
                **inputs,
                max_new_tokens=300,
                do_sample=False,
                temperature=1.0,
                pad_token_id=tokenizer.eos_token_id,
            )

        # 提取 assistant 回覆（generation prompt 之後的部分）
        full_output = tokenizer.decode(outputs[0], skip_special_tokens=True)
        prompt_text_decoded = tokenizer.decode(
            tokenizer(prompt, return_tensors="pt")["input_ids"][0],
            skip_special_tokens=True
        )
        pred_sql = full_output[len(prompt_text_decoded):].strip()

        # 清理：取第一行有 SELECT 的，移除多餘內容
        lines = pred_sql.split("\n")
        pred_sql = ""
        for line in lines:
            line = line.strip()
            if line.upper().startswith("SELECT") or line.upper().startswith("WITH"):
                pred_sql = line
                break
        if not pred_sql and lines:
            pred_sql = lines[0].strip()

        # 清理尾部
        pred_sql = pred_sql.rstrip(';').strip()
        if pred_sql:
            pred_sql += ";"

        predictions.append(pred_sql)

        # 從 gold SQL 取正確表名（用於報告，不影響推論）
        gold_table = extract_table_from_sql(gold_sql)

        pred_records.append({
            "id":              i,
            "question":        question,
            "inferred_table":  inferred_table,
            "gold_table":      gold_table,
            "pred_sql":        pred_sql,
            "gold_sql":        gold_sql,
        })

        if (i + 1) % 10 == 0:
            print(f"  已推論: {i+1}/{len(test_data)}", flush=True)

    # 檢查 table 推斷準確率（從 pred SQL 提取）
    table_correct = sum(1 for r in pred_records if r["inferred_table"] == r["gold_table"])
    print(f"\nTable 推斷準確率 (from pred SQL): {table_correct}/{len(pred_records)} "
          f"= {table_correct/len(pred_records)*100:.1f}%")
    table_errors = [r for r in pred_records if r["inferred_table"] != r["gold_table"]]
    if table_errors:
        print("  Table 推斷錯誤 (前 10 筆):")
        for r in table_errors[:10]:
            print(f"    [{r['id']}] Q: {r['question'][:80]}...")
            print(f"         推斷={r['inferred_table']}  正確={r['gold_table']}")

    return predictions, pred_records


# ============================================================
# 主評估流程（與 v1 完全相同）
# ============================================================
def evaluate(gold_data: list, predictions: list, cursor=None):
    assert len(gold_data) == len(predictions), \
        f"Gold ({len(gold_data)}) 與 Pred ({len(predictions)}) 數量不符！"

    n = len(gold_data)
    em_correct = 0
    ex_correct = 0
    ex_evaluated = 0

    table_stats = defaultdict(lambda: {"total": 0, "em": 0, "ex": 0})
    diff_stats  = defaultdict(lambda: {"total": 0, "em": 0, "ex": 0})
    results_detail = []

    for i in range(n):
        gold_sql   = gold_data[i]["query"]
        pred_sql   = predictions[i]
        question   = gold_data[i]["question"]
        difficulty = gold_data[i].get("difficulty", "unknown")
        table      = extract_table_from_sql(gold_sql)

        em = compute_em(pred_sql, gold_sql)
        if em:
            em_correct += 1

        ex = False
        if cursor:
            ex = compute_ex(cursor, pred_sql, gold_sql)
            ex_evaluated += 1
            if ex:
                ex_correct += 1

        table_stats[table]["total"] += 1
        diff_stats[difficulty]["total"] += 1
        if em:
            table_stats[table]["em"] += 1
            diff_stats[difficulty]["em"] += 1
        if ex:
            table_stats[table]["ex"] += 1
            diff_stats[difficulty]["ex"] += 1

        results_detail.append({
            "id":         i,
            "question":   question,
            "gold_sql":   gold_sql,
            "pred_sql":   pred_sql,
            "em":         em,
            "ex":         ex if cursor else None,
            "table":      table,
            "difficulty":  difficulty,
        })

    # ---- 輸出報告 ----
    print("\n" + "=" * 70)
    print("WP_M09 Text-to-SQL 評估報告 (v2 — Chat Template + Schema)")
    print("=" * 70)

    em_pct = em_correct / n * 100
    print(f"\n測試筆數:                     {n}")
    print(f"整體 EM (Exact Match):        {em_correct}/{n} = {em_pct:.2f}%")

    if cursor:
        ex_pct = ex_correct / ex_evaluated * 100 if ex_evaluated else 0
        print(f"整體 EX (Execution Accuracy): {ex_correct}/{ex_evaluated} = {ex_pct:.2f}%")

    print(f"\n{'─'*60}")
    print(f"  按 View 分組")
    print(f"{'─'*60}")
    header = f"  {'View':<20} {'Total':>5} {'EM':>5} {'EM%':>8}"
    if cursor:
        header += f" {'EX':>5} {'EX%':>8}"
    print(header)
    print(f"  {'─'*55}")

    for table in sorted(table_stats.keys()):
        s = table_stats[table]
        em_rate = s['em'] / s['total'] * 100 if s['total'] else 0
        line = f"  {table:<20} {s['total']:>5} {s['em']:>5} {em_rate:>7.2f}%"
        if cursor:
            ex_rate = s['ex'] / s['total'] * 100 if s['total'] else 0
            line += f" {s['ex']:>5} {ex_rate:>7.2f}%"
        print(line)

    print(f"\n{'─'*50}")
    print(f"  按難度分組")
    print(f"{'─'*50}")
    header = f"  {'Difficulty':<12} {'Total':>5} {'EM':>5} {'EM%':>8}"
    if cursor:
        header += f" {'EX':>5} {'EX%':>8}"
    print(header)
    print(f"  {'─'*45}")

    for diff in ["easy", "medium", "hard", "unknown"]:
        if diff not in diff_stats:
            continue
        s = diff_stats[diff]
        em_rate = s['em'] / s['total'] * 100 if s['total'] else 0
        line = f"  {diff:<12} {s['total']:>5} {s['em']:>5} {em_rate:>7.2f}%"
        if cursor:
            ex_rate = s['ex'] / s['total'] * 100 if s['total'] else 0
            line += f" {s['ex']:>5} {ex_rate:>7.2f}%"
        print(line)

    # EM 正確樣本
    correct = [r for r in results_detail if r["em"]]
    if correct:
        print(f"\n{'─'*60}")
        print(f"  [OK] EM 正確樣本 (前 5 筆)")
        print(f"{'─'*60}")
        for r in correct[:5]:
            print(f"\n  [{r['id']}] ({r['table']}, {r['difficulty']})")
            print(f"    Q:    {r['question']}")
            print(f"    SQL:  {r['gold_sql'][:100]}")

    # EM 錯誤樣本
    errors = [r for r in results_detail if not r["em"]]
    if errors:
        print(f"\n{'─'*60}")
        print(f"  [FAIL] EM 錯誤樣本 (前 15 筆，共 {len(errors)} 筆)")
        print(f"{'─'*60}")
        for r in errors[:15]:
            print(f"\n  [{r['id']}] ({r['table']}, {r['difficulty']})")
            print(f"    Q:    {r['question']}")
            print(f"    Gold: {r['gold_sql'][:120]}")
            print(f"    Pred: {r['pred_sql'][:120]}")

    return {
        "total":       n,
        "em_correct":  em_correct,
        "em_pct":      round(em_pct, 2),
        "ex_correct":  ex_correct if cursor else None,
        "ex_pct":      round(ex_correct / ex_evaluated * 100, 2) if cursor and ex_evaluated else None,
        "table_stats": dict(table_stats),
        "diff_stats":  dict(diff_stats),
        "details":     results_detail,
    }


# ============================================================
# CLI
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="WP_M09 Text-to-SQL EM/EX 評估 (v2 Chat Template)")

    parser.add_argument("--gold", required=True, help="Gold 測試集 JSON")
    parser.add_argument("--pred", default=None, help="預測結果 JSON")
    parser.add_argument("--model", default=None, help="模型路徑")

    parser.add_argument("--db-host", default=None, help="SQL Server host")
    parser.add_argument("--db-name", default="WP_M09", help="Database name")
    parser.add_argument("--db-user", default=None, help="DB user")
    parser.add_argument("--db-pass", default=None, help="DB password")
    parser.add_argument("--db-trusted", action="store_true", help="Windows Auth")

    parser.add_argument("--output", default="eval_results.json", help="輸出路徑")

    args = parser.parse_args()

    with open(args.gold, "r", encoding="utf-8") as f:
        gold_data = json.load(f)
    print(f"Gold 測試集: {len(gold_data)} 筆")

    pred_records = None
    if args.pred:
        with open(args.pred, "r", encoding="utf-8") as f:
            raw_pred = json.load(f)
        if raw_pred and isinstance(raw_pred[0], dict):
            predictions = [p.get("pred_sql", p.get("sql", "")) for p in raw_pred]
        else:
            predictions = raw_pred
        print(f"\nPredictions 載入: {len(predictions)} 筆")
    elif args.model:
        predictions, pred_records = run_inference(args.model, gold_data)

        pred_out = args.output.replace(".json", "_predictions.json")
        with open(pred_out, "w", encoding="utf-8") as f:
            json.dump(pred_records, f, indent=2, ensure_ascii=False)
        print(f"\n推論紀錄已儲存: {pred_out}")
    else:
        print("錯誤: 請提供 --pred 或 --model")
        sys.exit(1)

    cursor = None
    if args.db_trusted and args.db_host:
        try:
            import pyodbc
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={args.db_host};"
                f"DATABASE={args.db_name};"
                f"Trusted_Connection=yes;"
            )
            conn = pyodbc.connect(conn_str, timeout=30)
            cursor = conn.cursor()
            print(f"已連線資料庫 (Windows Auth): {args.db_host}/{args.db_name}")
        except Exception as e:
            print(f"⚠️ 資料庫連線失敗: {e}")
    elif args.db_host and args.db_user:
        try:
            import pyodbc
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={args.db_host};"
                f"DATABASE={args.db_name};"
                f"UID={args.db_user};"
                f"PWD={args.db_pass};"
            )
            conn = pyodbc.connect(conn_str, timeout=30)
            cursor = conn.cursor()
            print(f"已連線資料庫: {args.db_host}/{args.db_name}")
        except Exception as e:
            print(f"⚠️ 資料庫連線失敗: {e}")

    results = evaluate(gold_data, predictions, cursor)

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n評估結果已儲存: {args.output}")

    if cursor:
        cursor.close()


if __name__ == "__main__":
    main()
