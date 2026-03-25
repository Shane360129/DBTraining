#!/usr/bin/env python3
"""
evaluate_em_ex.py
評估 WP_M09 DoRA/LoRA 微調模型的 Text-to-SQL 效果。

指標:
  EM (Exact Match): 正規化後的 SQL 字串完全一致
  EX (Execution Accuracy): 在資料庫上執行結果一致（需連線 SQL Server）

用法:
  # 僅 EM（不需資料庫）— 用已有的預測結果
  python evaluate_em_ex.py --pred predictions.json --gold test_spider_WP_M09.json

  # EM + EX（需資料庫連線）
  python evaluate_em_ex.py --pred predictions.json --gold test_spider_WP_M09.json \
      --db-host localhost --db-name WP_M09 --db-user sa --db-pass YOUR_PASSWORD

  # 用模型直接推論 + 評估（會同時輸出 predictions.json）
  python evaluate_em_ex.py --model outputs/models/wp_m09_dora_0312_spider/final_model \
      --gold test_spider_WP_M09.json

predictions.json 格式（含問句，方便追溯）:
  [
    {
      "id": 0,
      "question": "How many distinct ...",
      "inferred_table": "WP_vAcctIn",
      "gold_table": "WP_vAcctIn",
      "prompt": "Table: WP_vAcctIn\\nNote: ...\\nQuestion: ...\\nSQL:",
      "pred_sql": "SELECT COUNT(...) ...",
      "gold_sql": "SELECT COUNT(...) ..."
    },
    ...
  ]
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
# SQL 正規化 (for EM)
# ============================================================
def normalize_sql(sql: str) -> str:
    """
    正規化 SQL 以進行 Exact Match 比對:
      1. 移除尾部分號
      2. 移除 [WP_M09].[dbo]. 前綴
      3. 移除方括號
      4. SQL 關鍵字小寫化（保留中文/值不變）
      5. 壓縮空白
    """
    s = sql.strip()
    s = s.rstrip(';').strip()

    # 移除 table prefix（支援兩種格式）
    s = s.replace('[WP_M09].[dbo].', '')
    s = s.replace('WP_M09.dbo.', '')

    # 移除方括號
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


# ============================================================
# EM 評估
# ============================================================
def compute_em(pred_sql: str, gold_sql: str) -> bool:
    return normalize_sql(pred_sql) == normalize_sql(gold_sql)


# ============================================================
# EX 評估 (Execution Accuracy)
# ============================================================
def execute_sql(cursor, sql: str) -> Optional[list]:
    """執行 SQL 並回傳結果集（排序後），失敗回傳 None。"""
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        result = sorted([tuple(str(c) for c in row) for row in rows])
        return result
    except Exception:
        return None


def compute_ex(cursor, pred_sql: str, gold_sql: str) -> bool:
    """比較兩個 SQL 的執行結果是否一致。"""
    gold_result = execute_sql(cursor, gold_sql)
    pred_result = execute_sql(cursor, pred_sql)

    if gold_result is None:
        return False
    if pred_result is None:
        return False

    return gold_result == pred_result


# ============================================================
# 推論用 Prompt 建構
# ============================================================
TABLE_NOTES = {
    "WP_vProduct":   "Note: pNo is a sequential product number (1, 2, 3...), NOT a date. This view has no date filtering capability.",
    "WP_vInventory": "Note: pNo is a sequential product number, not a date. This view has no date filtering capability.",
    "WP_vTransfer":  "Note: Filter by date using LEFT(TransferId,8)='YYYYMMDD' or LEFT(TransferId,6)='YYYYMM'.",
    "WP_vAcctIn":    "Note: Filter by date using LEFT(acctInId,8)='YYYYMMDD'. isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vAcctOut":   "Note: Filter by date using LEFT(acctOutId,8)='YYYYMMDD'. isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vOutStock":  "Note: Filter by date using LEFT(OutStkId,8)='YYYYMMDD'. isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vProvider":  "Note: isSale='Y' means active provider. Boolean fields use 'Y'/'N' encoding.",
}

# 從問句關鍵字推斷目標 table（推論時不能偷看 gold SQL）
TABLE_KEYWORD_MAP = {
    "WP_vAcctIn":    ["receivable", "accounts receivable", "receipt", "收款", "應收"],
    "WP_vAcctOut":   ["payable", "accounts payable", "payment", "付款", "應付"],
    "WP_vInventory": ["inventory", "warehouse", "stock level", "庫存", "倉庫", "safe quantity"],
    "WP_vOutStock":  ["sales order", "out-stock", "sold", "sale", "銷貨", "出庫", "member city", "member name"],
    "WP_vProduct":   ["product", "barcode", "isSale", "vendor supply", "產品", "商品", "unit name"],
    "WP_vProvider":  ["vendor", "supplier", "provider", "pvBoss", "供應商", "廠商"],
    "WP_vTransfer":  ["transfer", "調撥", "destination warehouse", "source warehouse"],
}


def infer_table_from_question(question: str) -> str:
    """
    從自然語言問句推斷目標 view 名稱。
    使用加權關鍵字計分，處理歧義問句（如 "products supplied by vendor" → WP_vProduct）。
    若無法判斷則回傳 'WP_M09' 作為 fallback。
    """
    q_lower = question.lower()

    # ---- 第一層：高信度精確匹配（獨有欄位/術語）----
    # Transfer 專有
    if "transfer" in q_lower or "調撥" in q_lower:
        return "WP_vTransfer"

    # AcctIn 專有欄位
    if any(kw in q_lower for kw in [
        "receivable", "accounts receivable", "receipt id", "收款", "應收",
        "ostkdtlamt", "ostkdtlqty", "ostkdtlamttotal", "credit sales order",
        "outstkamttotal", "discountshare",
    ]):
        return "WP_vAcctIn"

    # AcctOut 專有欄位
    if any(kw in q_lower for kw in [
        "payable", "accounts payable", "付款", "應付",
        "instkamt", "instkpayleft", "instkpaytype", "instktax",
        "purchase order",
    ]):
        return "WP_vAcctOut"

    # Inventory 專有
    if any(kw in q_lower for kw in [
        "inventory", "warehouse", "庫存", "倉庫", "stock level",
        "warehouseid", "warehousename",
    ]):
        return "WP_vInventory"

    # Provider 專有（只在問「廠商本身資訊」時才選，不包含 "supplied by vendor"）
    if any(kw in q_lower for kw in [
        "pvboss", "isstop", "ctacttel", "invoitle", "bankacc", "bankname",
        "taxid", "pvkname", "pvtel", "pvaddr", "pvcity",
    ]):
        return "WP_vProvider"
    # "vendor" 單獨出現且沒有 "product" → Provider
    if ("vendor" in q_lower or "supplier" in q_lower) and \
       "product" not in q_lower and "supplied" not in q_lower and \
       "supply" not in q_lower and "barcode" not in q_lower and \
       "quantity" not in q_lower and "price" not in q_lower and \
       "purchase" not in q_lower:
        return "WP_vProvider"

    # OutStock 專有
    if any(kw in q_lower for kw in [
        "out-stock", "outstkid", "sales order", "member city",
        "memcity", "dtlcost", "dtldiscntper", "cost of goods sold",
        "sales amount", "sales total", "sales quantity",
        "tax amount collected", "total discount applied to sales",
    ]):
        return "WP_vOutStock"

    # ---- 第二層：加權計分 ----
    # (keyword, table, weight) — 高權重 = 強信號
    weighted_keywords = [
        # AcctIn 信號
        ("receivable",    "WP_vAcctIn",    10),
        ("receipt",       "WP_vAcctIn",    8),
        ("credit sales",  "WP_vAcctIn",    8),
        ("detail discount", "WP_vAcctIn",  5),
        ("detail lines",  "WP_vAcctIn",    3),

        # AcctOut 信號
        ("payable",       "WP_vAcctOut",   10),
        ("purchase",      "WP_vAcctOut",   5),
        ("in-stock amount", "WP_vAcctOut", 8),

        # Inventory 信號
        ("inventory",     "WP_vInventory", 10),
        ("warehouse",     "WP_vInventory", 8),
        ("safe quantity", "WP_vInventory", 8),
        ("stocked",       "WP_vInventory", 5),

        # OutStock 信號
        ("out-stock",     "WP_vOutStock",  10),
        ("sales order",   "WP_vOutStock",  8),
        ("sold",          "WP_vOutStock",  4),
        ("sale",          "WP_vOutStock",  3),
        ("member city",   "WP_vOutStock",  8),
        ("member name",   "WP_vOutStock",  5),
        ("employee",      "WP_vOutStock",  2),

        # Product 信號（含 "supplied by vendor" 類）
        ("product",       "WP_vProduct",   6),
        ("barcode",       "WP_vProduct",   5),
        ("issale",        "WP_vProduct",   8),
        ("isupdstock",    "WP_vProduct",   8),
        ("unit name",     "WP_vProduct",   6),
        ("pricemem",      "WP_vProduct",   6),
        ("pricebat",      "WP_vProduct",   6),
        ("pricelw",       "WP_vProduct",   6),
        ("qtyinitial",    "WP_vProduct",   6),
        ("qtynow",        "WP_vProduct",   8),
        ("costinit",      "WP_vProduct",   6),
        ("vendor supply",  "WP_vProduct",  5),
        ("supplied by",   "WP_vProduct",   5),
        ("supply product", "WP_vProduct",  5),
        ("vendors supply", "WP_vProduct",  5),
        ("vendor with the most products", "WP_vProduct", 10),
        ("vendor discount", "WP_vProduct", 4),

        # Provider 信號
        ("vendor",        "WP_vProvider",  3),
        ("supplier",      "WP_vProvider",  3),
        ("provider",      "WP_vProvider",  3),
        ("active vendor", "WP_vProvider",  8),
        ("suspended",     "WP_vProvider",  8),
        ("bank",          "WP_vProvider",  6),
        ("fax",           "WP_vProvider",  6),
        ("email",         "WP_vProvider",  5),
        ("invoice title", "WP_vProvider",  8),
        ("category name", "WP_vProvider",  5),

        # Transfer 信號
        ("transfer",      "WP_vTransfer",  10),
        ("destination warehouse", "WP_vTransfer", 10),
        ("source warehouse", "WP_vTransfer", 10),
    ]

    scores = defaultdict(int)
    for kw, table, weight in weighted_keywords:
        if kw in q_lower:
            scores[table] += weight

    if scores:
        return max(scores, key=scores.get)

    return "WP_M09"


def extract_table_from_sql(sql: str) -> str:
    """從 gold SQL 提取 table 名稱（支援有/無方括號格式）。"""
    m = re.search(r'WP_M09\.dbo\.(\w+)', sql)
    if m:
        return m.group(1)
    m = re.search(r'\[WP_M09\]\.\[dbo\]\.\[(\w+)\]', sql)
    return m.group(1) if m else "unknown"


def build_inference_prompt(question: str, table: str) -> str:
    """
    建立推論用 prompt（不含 SQL 答案）。
    table 參數由 infer_table_from_question() 推斷，不依賴 gold SQL。
    """
    note = TABLE_NOTES.get(table, "")

    lines = [f"Table: {table}"]
    if note:
        lines.append(note)
    lines.append(f"Question: {question}")
    lines.append("SQL:")

    return "\n".join(lines)


# ============================================================
# 模型推論
# ============================================================
def run_inference(model_path: str, test_data: list) -> tuple:
    """
    使用微調模型進行推論。
    回傳:
      predictions:  list[str]   — 預測 SQL 列表
      pred_records: list[dict]  — 完整推論紀錄（含問句、prompt）
    """
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
        prompt = build_inference_prompt(question, inferred_table)

        inputs = tokenizer(prompt, return_tensors="pt").to(model.device)

        with torch.no_grad():
            outputs = model.generate(
                **inputs,
                max_new_tokens=256,
                do_sample=False,
                temperature=1.0,
                pad_token_id=tokenizer.eos_token_id,
            )

        full_output = tokenizer.decode(outputs[0], skip_special_tokens=True)

        # 提取 SQL: 之後的部分
        if "SQL:" in full_output:
            pred_sql = full_output.split("SQL:")[-1].strip()
        else:
            pred_sql = full_output[len(prompt):].strip()

        # 清理：取第一行、移除尾部雜訊
        pred_sql = pred_sql.split("\n")[0].strip()
        pred_sql = pred_sql.rstrip(';').strip() + ";"

        predictions.append(pred_sql)

        # 紀錄完整推論資訊（含問句）
        pred_records.append({
            "id":              i,
            "question":        question,
            "inferred_table":  inferred_table,
            "gold_table":      extract_table_from_sql(gold_sql),
            "prompt":          prompt,
            "pred_sql":        pred_sql,
            "gold_sql":        gold_sql,
        })

        if (i + 1) % 10 == 0:
            print(f"  已推論: {i+1}/{len(test_data)}", flush=True)

    # 檢查 table 推斷準確率
    table_correct = sum(1 for r in pred_records if r["inferred_table"] == r["gold_table"])
    print(f"\nTable 推斷準確率: {table_correct}/{len(pred_records)} "
          f"= {table_correct/len(pred_records)*100:.1f}%")
    table_errors = [r for r in pred_records if r["inferred_table"] != r["gold_table"]]
    if table_errors:
        print("  Table 推斷錯誤:")
        for r in table_errors[:5]:
            print(f"    [{r['id']}] Q: {r['question'][:80]}...")
            print(f"         推斷={r['inferred_table']}  正確={r['gold_table']}")

    return predictions, pred_records


# ============================================================
# 主評估流程
# ============================================================
def evaluate(gold_data: list, predictions: list, cursor=None):
    """計算 EM 和 EX，按表和難度分組報告。"""

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

        # EM
        em = compute_em(pred_sql, gold_sql)
        if em:
            em_correct += 1

        # EX
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
    print("WP_M09 Text-to-SQL 評估報告")
    print("=" * 70)

    em_pct = em_correct / n * 100
    print(f"\n測試筆數:                     {n}")
    print(f"整體 EM (Exact Match):        {em_correct}/{n} = {em_pct:.2f}%")

    if cursor:
        ex_pct = ex_correct / ex_evaluated * 100 if ex_evaluated else 0
        print(f"整體 EX (Execution Accuracy): {ex_correct}/{ex_evaluated} = {ex_pct:.2f}%")
    else:
        print("整體 EX (Execution Accuracy): N/A（未連線資料庫）")

    # 按表報告
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

    # 按難度報告
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

    # EM 正確樣本（前 5 筆）
    correct = [r for r in results_detail if r["em"]]
    if correct:
        print(f"\n{'─'*60}")
        print(f"  [OK] EM 正確樣本 (前 5 筆)")
        print(f"{'─'*60}")
        for r in correct[:5]:
            print(f"\n  [{r['id']}] ({r['table']}, {r['difficulty']})")
            print(f"    Q:    {r['question']}")
            print(f"    SQL:  {r['gold_sql'][:100]}")

    # EM 錯誤樣本（前 15 筆）
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
    parser = argparse.ArgumentParser(description="WP_M09 Text-to-SQL EM/EX 評估")

    parser.add_argument("--gold", required=True,
                        help="Gold 測試集 (Spider 格式 JSON)")
    parser.add_argument("--pred", default=None,
                        help="預測結果 JSON: list[str] 或 list[dict] 含 'pred_sql'")
    parser.add_argument("--model", default=None,
                        help="模型路徑 (若無 --pred 則用此模型推論)")

    parser.add_argument("--db-host", default=None, help="SQL Server host")
    parser.add_argument("--db-name", default="WP_M09", help="Database name")
    parser.add_argument("--db-user", default=None, help="DB user")
    parser.add_argument("--db-pass", default=None, help="DB password")
    parser.add_argument("--db-trusted", action="store_true",
                        help="Use Windows Authentication (Trusted_Connection)")

    parser.add_argument("--output", default="eval_results.json",
                        help="評估結果 JSON 輸出路徑")

    args = parser.parse_args()

    # ---- 載入 gold ----
    with open(args.gold, "r", encoding="utf-8") as f:
        gold_data = json.load(f)
    print(f"Gold 測試集: {len(gold_data)} 筆")

    # ---- 列出測試問句 ----
    print(f"\n{'─'*70}")
    print(f"  測試問句一覽 ({len(gold_data)} 筆)")
    print(f"{'─'*70}")
    for i, item in enumerate(gold_data):
        table = extract_table_from_sql(item["query"])
        diff  = item.get("difficulty", "?")
        print(f"  [{i:>2}] ({table:<16} {diff:<6}) {item['question']}")

    # ---- 取得 predictions ----
    pred_records = None
    if args.pred:
        with open(args.pred, "r", encoding="utf-8") as f:
            raw_pred = json.load(f)
        # 支援兩種格式: list[str] 或 list[dict]
        if raw_pred and isinstance(raw_pred[0], dict):
            predictions = [p.get("pred_sql", p.get("sql", "")) for p in raw_pred]
        else:
            predictions = raw_pred
        print(f"\nPredictions 載入: {len(predictions)} 筆")
    elif args.model:
        predictions, pred_records = run_inference(args.model, gold_data)

        # 儲存推論紀錄（含問句 + prompt）
        pred_out = args.output.replace("eval_results", "predictions")
        if pred_out == args.output:
            pred_out = "predictions.json"
        with open(pred_out, "w", encoding="utf-8") as f:
            json.dump(pred_records, f, indent=2, ensure_ascii=False)
        print(f"\n推論紀錄已儲存: {pred_out}")
    else:
        print("錯誤: 請提供 --pred 或 --model")
        sys.exit(1)

    # ---- 資料庫連線 (optional for EX) ----
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
            print("  將僅計算 EM，跳過 EX")
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
            print("  將僅計算 EM，跳過 EX")

    # ---- 評估 ----
    results = evaluate(gold_data, predictions, cursor)

    # ---- 儲存結果 ----
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n評估結果已儲存: {args.output}")

    if cursor:
        cursor.close()


if __name__ == "__main__":
    main()
