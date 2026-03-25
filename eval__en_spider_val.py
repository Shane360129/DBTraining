#!/usr/bin/env python3
"""
eval__en_spider_val.py
══════════════════════════════════════════════════════════════════
針對英文 Spider 驗證集 (val_claude_en_spider.json) 的評估腳本。

功能:
  1. 從 val_claude_en_spider.json 讀取 question + gold SQL
  2. 用微調模型推論每道問題的 SQL（自動推斷目標 view）
  3. 計算 EM (Exact Match) — 不需資料庫
  4. 選配 EX (Execution Accuracy) — 需 SQL Server 連線
  5. 按 View / Difficulty 分組報告
  6. 輸出詳細 JSON 結果

用法:
  # 用模型推論並評估（僅 EM）
  python eval__en_spider_val.py \
      --model outputs/models/wp_m09_dora_0318_spider/final_model

  # 用模型推論並評估（EM + EX）
  python eval__en_spider_val.py \
      --model outputs/models/wp_m09_dora_0318_spider/final_model \
      --db-host localhost --db-user sa --db-pass YOUR_PASSWORD

  # 用已有預測結果評估（跳過推論）
  python eval__en_spider_val.py \
      --pred outputs/predictions/pred_en_val.json

  # 指定不同驗證集
  python eval__en_spider_val.py \
      --gold data/wp_m09/val_claude_en_spider.json \
      --model outputs/models/wp_m09_dora_0319_spider/final_model

輸出檔案（自動命名）:
  outputs/predictions/pred_en_<DATE>.json   ← 推論紀錄
  outputs/eval_en_<DATE>.json               ← 評估結果
══════════════════════════════════════════════════════════════════
"""

import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional


# ════════════════════════════════════════════════════════════════
#  預設路徑
# ════════════════════════════════════════════════════════════════
DEFAULT_GOLD = "data/wp_m09/val_claude_en_spider.json"


# ════════════════════════════════════════════════════════════════
#  TABLE NOTES（推論 prompt 用）
# ════════════════════════════════════════════════════════════════
TABLE_NOTES = {
    "WP_vProduct":   ("Note: pNo is a sequential product number (1, 2, 3...), NOT a date. "
                      "This view has no date filtering capability. "
                      "CRITICAL: This view has NO isDel or dtlIsDel column — never add them."),
    "WP_vInventory": ("Note: pNo is a sequential product number, not a date. "
                      "This view has no date filtering capability. "
                      "CRITICAL: This view has NO isDel or dtlIsDel column — never add them."),
    "WP_vProvider":  ("Note: Main lookup table for supplier info. Join with other views using pvSn. "
                      "CRITICAL: This view has NO isDel or dtlIsDel column — never add them."),
    "WP_vTransfer":  ("Note: Filter by date using LEFT(TransferId,8)='YYYYMMDD' "
                      "or LEFT(TransferId,6)='YYYYMM'. isDel='N' AND dtlIsDel='N' for active records."),
    "WP_vAcctIn":    ("Note: Filter by date using LEFT(acctInId,8)='YYYYMMDD'. "
                      "ALWAYS add isDel='N' AND dtlIsDel='N' for active records."),
    "WP_vAcctOut":   ("Note: Filter by date using LEFT(acctOutId,8)='YYYYMMDD'. "
                      "ALWAYS add isDel='N' AND dtlIsDel='N' for active records."),
    "WP_vOutStock":  ("Note: Filter by date using LEFT(OutStkId,8)='YYYYMMDD'. "
                      "ALWAYS add isDel='N' AND dtlIsDel='N' for active records."),
}


# ════════════════════════════════════════════════════════════════
#  View 推斷（英文問句優化版）
# ════════════════════════════════════════════════════════════════
def infer_view(question: str) -> str:
    """
    從英文問句推斷目標 view。
    優先順序：精確關鍵字 > 加權計分 > fallback
    """
    q = question.lower()

    # ── Tier 1: 高信度唯一關鍵字 ──────────────────────────
    # WP_vTransfer
    if any(k in q for k in ["transfer", "transferred", "source warehouse", "destination warehouse",
                              "fwhname", "tfwhname", "transferid"]):
        return "WP_vTransfer"

    # WP_vAcctIn（應收）
    if any(k in q for k in ["accounts receivable", "receivable", "acctinid", "acct in",
                              "outstkamttotal", "ostkdtlqty", "ostkdtlamt", "credit sales order"]):
        return "WP_vAcctIn"

    # WP_vAcctOut（應付）
    if any(k in q for k in ["accounts payable", "payable", "acctoutid", "acct out",
                              "transamt", "instktax", "purchase order", "paid in"]):
        return "WP_vAcctOut"

    # WP_vInventory（庫存）
    if any(k in q for k in ["inventory", "warehouse", "warehousename", "warehouseid",
                              "stock level", "safe stock", "qtyshelf", "qtysafe",
                              "in stock", "in warehouse"]):
        return "WP_vInventory"

    # WP_vOutStock（銷貨）
    if any(k in q for k in ["sales order", "outstockid", "outstkid", "out-stock",
                              "member spend", "member spent", "total spent", "member city",
                              "outtype", "outleft", "settlement", "settled", "unsettled",
                              "outstk", "sales total", "revenue from"]):
        return "WP_vOutStock"

    # ── Tier 2: 加權計分 ────────────────────────────────────
    W = [
        # AcctIn 信號
        ("receivable",        "WP_vAcctIn",    10),
        ("receipt",           "WP_vAcctIn",     6),
        ("line item",         "WP_vAcctIn",     5),
        ("outstock amount",   "WP_vAcctIn",     8),

        # AcctOut 信號
        ("payable",           "WP_vAcctOut",   10),
        ("purchase",          "WP_vAcctOut",    5),
        ("pay ",              "WP_vAcctOut",    4),
        ("non-taxable",       "WP_vAcctOut",    6),
        ("taxable",           "WP_vAcctOut",    5),
        ("tax",               "WP_vAcctOut",    3),
        ("transfer amount",   "WP_vAcctOut",    7),

        # Inventory 信號
        ("inventory",         "WP_vInventory", 10),
        ("warehouse",         "WP_vInventory",  9),
        ("in stock",          "WP_vInventory",  8),
        ("safe",              "WP_vInventory",  5),
        ("stock level",       "WP_vInventory",  8),
        ("qty",               "WP_vInventory",  2),
        ("cost",              "WP_vInventory",  2),

        # OutStock 信號
        ("sales order",       "WP_vOutStock",  10),
        ("sold",              "WP_vOutStock",   6),
        ("sale",              "WP_vOutStock",   4),
        ("spend",             "WP_vOutStock",   6),
        ("spent",             "WP_vOutStock",   6),
        ("member",            "WP_vOutStock",   3),
        ("out-stock",         "WP_vOutStock",  10),
        ("settlement",        "WP_vOutStock",   8),
        ("total revenue",     "WP_vOutStock",   7),

        # Product 信號
        ("product",           "WP_vProduct",    6),
        ("barcode",           "WP_vProduct",    8),
        ("qtynow",            "WP_vProduct",    9),
        ("current stock",     "WP_vProduct",    8),
        ("catalog",           "WP_vProduct",    7),
        ("product catalog",   "WP_vProduct",   10),
        ("standard price",    "WP_vProduct",    6),
        ("average cost",      "WP_vProduct",    5),
        ("pricestd",          "WP_vProduct",    7),
        ("costavg",           "WP_vProduct",    7),

        # Provider 信號（不含 product 脈絡）
        ("supplier",          "WP_vProvider",   4),
        ("vendor",            "WP_vProvider",   3),
        ("provider",          "WP_vProvider",   3),
        ("pvname",            "WP_vProvider",   9),
        ("pvsn",              "WP_vProvider",   9),
        ("discount rate",     "WP_vProvider",   8),
        ("active supplier",   "WP_vProvider",   9),
        ("inactive supplier", "WP_vProvider",   9),
        ("isSale",            "WP_vProvider",   8),
        ("pvtel",             "WP_vProvider",   9),
        ("pvaddr",            "WP_vProvider",   9),
        ("pvboss",            "WP_vProvider",   9),

        # Transfer 信號
        ("transfer",          "WP_vTransfer",  10),
        ("transferred",       "WP_vTransfer",  10),
        ("route",             "WP_vTransfer",   6),
    ]

    scores = defaultdict(int)
    for kw, view, w in W:
        if kw in q:
            scores[view] += w

    # Provider 補正：若 product/inventory/sales 也有分，降低 Provider 權重
    if scores.get("WP_vProvider", 0) > 0:
        non_prov = sum(v for k, v in scores.items() if k != "WP_vProvider")
        if non_prov >= scores["WP_vProvider"]:
            del scores["WP_vProvider"]

    if scores:
        return max(scores, key=scores.get)

    # ── Tier 3: Fallback 簡單規則 ───────────────────────────
    if "supplier" in q or "vendor" in q:
        return "WP_vProvider"
    if "product" in q:
        return "WP_vProduct"
    if "order" in q:
        return "WP_vOutStock"

    return "WP_vInventory"   # 最終 fallback


def extract_view(sql: str) -> str:
    m = re.search(r'WP_M09\.dbo\.(\w+)', sql)
    if m:
        return m.group(1)
    m = re.search(r'\[WP_M09\]\.\[dbo\]\.\[(\w+)\]', sql)
    return m.group(1) if m else "unknown"


# ════════════════════════════════════════════════════════════════
#  推論 Prompt
# ════════════════════════════════════════════════════════════════
def build_prompt(question: str, view: str) -> str:
    note = TABLE_NOTES.get(view, "")
    lines = [f"Table: {view}"]
    if note:
        lines.append(note)
    lines.append(f"Question: {question}")
    lines.append("SQL:")
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════
#  SQL 正規化（EM 用）
# ════════════════════════════════════════════════════════════════
_KEYWORDS = [
    'SELECT','FROM','WHERE','AND','OR','NOT','IN','BETWEEN','LIKE','IS','NULL',
    'JOIN','LEFT','RIGHT','INNER','OUTER','ON','AS','GROUP','BY','ORDER','ASC',
    'DESC','HAVING','DISTINCT','TOP','COUNT','SUM','AVG','MIN','MAX',
    'CAST','CONVERT','COALESCE','ISNULL','UNION','ALL','CASE','WHEN','THEN',
    'ELSE','END','WITH','NULLIF','IIF',
]

def normalize_sql(sql: str) -> str:
    s = sql.strip().rstrip(';').strip()
    # 移除 DB prefix（兩種格式）
    s = re.sub(r'\[WP_M09\]\.\[dbo\]\.', '', s)
    s = re.sub(r'WP_M09\.dbo\.', '', s)
    # 移除方括號
    s = re.sub(r'\[(\w+)\]', r'\1', s)
    # 關鍵字小寫
    for kw in _KEYWORDS:
        s = re.sub(r'\b' + kw + r'\b', kw.lower(), s, flags=re.IGNORECASE)
    # 壓縮空白
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def compute_em(pred: str, gold: str) -> bool:
    return normalize_sql(pred) == normalize_sql(gold)


# ════════════════════════════════════════════════════════════════
#  EX（執行準確率）
# ════════════════════════════════════════════════════════════════
def execute_sql(cursor, sql: str) -> Optional[list]:
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        return sorted([tuple(str(c) for c in row) for row in rows])
    except Exception:
        return None


def compute_ex(cursor, pred: str, gold: str) -> bool:
    gr = execute_sql(cursor, gold)
    pr = execute_sql(cursor, pred)
    return gr is not None and pr is not None and gr == pr


# ════════════════════════════════════════════════════════════════
#  模型推論
# ════════════════════════════════════════════════════════════════
def run_inference(model_path: str, gold_data: list) -> tuple[list, list]:
    import torch
    from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
    from peft import PeftModel

    print(f"\n載入模型: {model_path}")

    # 讀取 training_info.json 取得 base model
    info_path = os.path.join(model_path, "training_info.json")
    base_model = "meta-llama/Llama-3.1-8B-Instruct"
    if os.path.exists(info_path):
        with open(info_path) as f:
            info = json.load(f)
        base_model = info.get("base_model", base_model)
        print(f"  Base model : {base_model}")
        print(f"  Trained on : {info.get('train_data','?')}  ({info.get('train_samples','?')} samples)")
        print(f"  Method     : {info.get('method','?')}  r={info.get('lora_r','?')}")

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

    n = len(gold_data)
    print(f"\n開始推論 {n} 筆驗證資料 ...")

    predictions  = []
    pred_records = []
    t0 = time.time()

    for i, item in enumerate(gold_data):
        question  = item["question"]
        gold_sql  = item.get("query", "")
        gold_view = item.get("view") or extract_view(gold_sql)

        # 從問句推斷 view（不偷看 gold SQL）
        inferred_view = infer_view(question)
        prompt        = build_prompt(question, inferred_view)

        inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
        with torch.no_grad():
            outputs = model.generate(
                **inputs,
                max_new_tokens=256,
                do_sample=False,
                temperature=1.0,
                pad_token_id=tokenizer.eos_token_id,
            )

        full = tokenizer.decode(outputs[0], skip_special_tokens=True)

        # 擷取 SQL 部分（SQL: 之後）
        if "SQL:" in full:
            pred_sql = full.split("SQL:")[-1].strip()
        else:
            pred_sql = full[len(prompt):].strip()

        # 取第一完整 SQL 語句（到 ; 或換行）
        pred_sql = pred_sql.split("\n")[0].strip()
        if not pred_sql.endswith(";"):
            pred_sql = pred_sql + ";"

        predictions.append(pred_sql)
        pred_records.append({
            "id":            i,
            "question":      question,
            "gold_view":     gold_view,
            "inferred_view": inferred_view,
            "view_correct":  (inferred_view == gold_view),
            "prompt":        prompt,
            "pred_sql":      pred_sql,
            "gold_sql":      gold_sql,
            "difficulty":    item.get("difficulty", "unknown"),
        })

        if (i + 1) % 20 == 0 or (i + 1) == n:
            elapsed = time.time() - t0
            rate    = (i + 1) / elapsed
            remain  = (n - i - 1) / rate if rate > 0 else 0
            print(f"  [{i+1:>3}/{n}]  {elapsed:.0f}s 已過  預計剩 {remain:.0f}s")

    # View 推斷準確率
    view_ok  = sum(1 for r in pred_records if r["view_correct"])
    view_err = [r for r in pred_records if not r["view_correct"]]
    print(f"\nView 推斷準確率: {view_ok}/{n} = {view_ok/n*100:.1f}%")
    if view_err:
        print(f"  錯誤樣本（前 5 筆）:")
        for r in view_err[:5]:
            print(f"    [{r['id']}] Q: {r['question'][:70]}")
            print(f"           推斷={r['inferred_view']}  正確={r['gold_view']}")

    return predictions, pred_records


# ════════════════════════════════════════════════════════════════
#  評估
# ════════════════════════════════════════════════════════════════
def evaluate(gold_data: list, predictions: list, pred_records: list, cursor=None) -> dict:
    n = len(gold_data)
    assert n == len(predictions), f"gold={n} pred={len(predictions)}"

    em_total = ex_total = ex_count = 0
    view_stats  = defaultdict(lambda: {"total":0,"em":0,"ex":0})
    diff_stats  = defaultdict(lambda: {"total":0,"em":0,"ex":0})
    details     = []

    for i in range(n):
        gold_sql   = gold_data[i]["query"]
        pred_sql   = predictions[i]
        question   = gold_data[i]["question"]
        difficulty = gold_data[i].get("difficulty", "unknown")
        view       = gold_data[i].get("view") or extract_view(gold_sql)

        em = compute_em(pred_sql, gold_sql)
        if em:
            em_total += 1

        ex = None
        if cursor:
            ex = compute_ex(cursor, pred_sql, gold_sql)
            ex_count += 1
            if ex:
                ex_total += 1

        view_stats[view]["total"] += 1
        diff_stats[difficulty]["total"] += 1
        if em:
            view_stats[view]["em"] += 1
            diff_stats[difficulty]["em"] += 1
        if ex:
            view_stats[view]["ex"] += 1
            diff_stats[difficulty]["ex"] += 1

        rec = {
            "id":         i,
            "question":   question,
            "gold_sql":   gold_sql,
            "pred_sql":   pred_sql,
            "em":         em,
            "ex":         ex,
            "view":       view,
            "difficulty": difficulty,
        }
        if pred_records:
            rec["inferred_view"] = pred_records[i].get("inferred_view","?")
            rec["view_correct"]  = pred_records[i].get("view_correct", True)
        details.append(rec)

    # ── 報告 ──────────────────────────────────────────────
    SEP = "=" * 68
    sep = "─" * 68
    print(f"\n{SEP}")
    print("  WP_M09 English Validation — Text-to-SQL Evaluation Report")
    print(f"{SEP}")
    em_pct = em_total / n * 100
    print(f"\n  Total samples  : {n}")
    print(f"  EM (Exact Match): {em_total}/{n} = {em_pct:.2f}%")
    if cursor:
        ex_pct = ex_total / ex_count * 100 if ex_count else 0
        print(f"  EX (Execution) : {ex_total}/{ex_count} = {ex_pct:.2f}%")
    else:
        print(f"  EX (Execution) : N/A  (no DB connection)")

    # 按 View
    print(f"\n  {sep[:50]}")
    print(f"  Results by View")
    print(f"  {sep[:50]}")
    hdr = f"  {'View':<22} {'N':>4} {'EM':>4} {'EM%':>7}"
    if cursor: hdr += f" {'EX':>4} {'EX%':>7}"
    print(hdr)
    print(f"  {'─'*50}")
    for v in sorted(view_stats):
        s  = view_stats[v]
        r  = s['em'] / s['total'] * 100 if s['total'] else 0
        ln = f"  {v:<22} {s['total']:>4} {s['em']:>4} {r:>6.1f}%"
        if cursor:
            xr = s['ex'] / s['total'] * 100 if s['total'] else 0
            ln += f" {s['ex']:>4} {xr:>6.1f}%"
        print(ln)

    # 按 Difficulty
    print(f"\n  {sep[:50]}")
    print(f"  Results by Difficulty")
    print(f"  {sep[:50]}")
    hdr = f"  {'Difficulty':<12} {'N':>4} {'EM':>4} {'EM%':>7}"
    if cursor: hdr += f" {'EX':>4} {'EX%':>7}"
    print(hdr)
    print(f"  {'─'*40}")
    for d in ["easy", "medium", "hard", "unknown"]:
        if d not in diff_stats: continue
        s  = diff_stats[d]
        r  = s['em'] / s['total'] * 100 if s['total'] else 0
        ln = f"  {d:<12} {s['total']:>4} {s['em']:>4} {r:>6.1f}%"
        if cursor:
            xr = s['ex'] / s['total'] * 100 if s['total'] else 0
            ln += f" {s['ex']:>4} {xr:>6.1f}%"
        print(ln)

    # EM 錯誤樣本（前 20 筆）
    errors = [r for r in details if not r["em"]]
    if errors:
        print(f"\n  {sep[:50]}")
        print(f"  EM Failures  ({len(errors)} total, showing first 20)")
        print(f"  {sep[:50]}")
        for r in errors[:20]:
            vi = f"[view_infer={r.get('inferred_view','?')}]" if not r.get("view_correct", True) else ""
            print(f"\n  [{r['id']:>3}] ({r['view']}, {r['difficulty']}) {vi}")
            print(f"    Q   : {r['question']}")
            print(f"    Gold: {normalize_sql(r['gold_sql'])[:110]}")
            print(f"    Pred: {normalize_sql(r['pred_sql'])[:110]}")

    # EM 正確樣本（前 5 筆）
    correct = [r for r in details if r["em"]]
    if correct:
        print(f"\n  {sep[:50]}")
        print(f"  EM Correct  ({len(correct)} total, showing first 5)")
        print(f"  {sep[:50]}")
        for r in correct[:5]:
            print(f"\n  [{r['id']:>3}] ({r['view']}, {r['difficulty']})")
            print(f"    Q  : {r['question']}")
            print(f"    SQL: {normalize_sql(r['gold_sql'])[:110]}")

    return {
        "total":         n,
        "em_correct":    em_total,
        "em_pct":        round(em_pct, 2),
        "ex_correct":    ex_total if cursor else None,
        "ex_pct":        round(ex_total / ex_count * 100, 2) if cursor and ex_count else None,
        "view_stats":    {k: dict(v) for k, v in view_stats.items()},
        "diff_stats":    {k: dict(v) for k, v in diff_stats.items()},
        "details":       details,
    }


# ════════════════════════════════════════════════════════════════
#  CLI
# ════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(
        description="Evaluate fine-tuned model on English Spider validation set"
    )
    parser.add_argument("--gold",    default=DEFAULT_GOLD,
                        help=f"Gold Spider JSON (default: {DEFAULT_GOLD})")
    parser.add_argument("--model",   default=None,
                        help="LoRA/DoRA model path for inference")
    parser.add_argument("--pred",    default=None,
                        help="Pre-computed predictions JSON (list[dict] with pred_sql field)")
    parser.add_argument("--output",  default=None,
                        help="Output JSON path (auto-named if omitted)")
    parser.add_argument("--db-host", default=None)
    parser.add_argument("--db-name", default="WP_M09")
    parser.add_argument("--db-user", default=None)
    parser.add_argument("--db-pass", default=None)
    args = parser.parse_args()

    if args.model is None and args.pred is None:
        parser.print_help()
        print("\nError: Provide --model <path> or --pred <json>")
        sys.exit(1)

    # ── 自動輸出路徑 ────────────────────────────────────────
    date_str  = datetime.now().strftime("%m%d")
    out_dir   = Path("outputs")
    pred_dir  = out_dir / "predictions"
    out_dir.mkdir(exist_ok=True)
    pred_dir.mkdir(exist_ok=True)

    if args.output is None:
        args.output = str(out_dir / f"eval_en_{date_str}.json")

    # ── 載入 gold ────────────────────────────────────────────
    with open(args.gold, "r", encoding="utf-8") as f:
        gold_data = json.load(f)
    print(f"Gold validation set : {args.gold}")
    print(f"  Total samples     : {len(gold_data)}")
    diff_cnt = defaultdict(int)
    view_cnt = defaultdict(int)
    for g in gold_data:
        diff_cnt[g.get("difficulty","unknown")] += 1
        view_cnt[g.get("view") or extract_view(g["query"])] += 1
    print(f"  Difficulty        : " +
          "  ".join(f"{d}={diff_cnt[d]}" for d in ["easy","medium","hard"] if d in diff_cnt))
    print(f"  Views             : " +
          "  ".join(f"{v.replace('WP_v','')}={view_cnt[v]}" for v in sorted(view_cnt)))

    # ── 推論 or 載入預測 ─────────────────────────────────────
    pred_records = []
    if args.model:
        predictions, pred_records = run_inference(args.model, gold_data)
        # 儲存推論紀錄
        pred_path = pred_dir / f"pred_en_{date_str}.json"
        with open(pred_path, "w", encoding="utf-8") as f:
            json.dump(pred_records, f, ensure_ascii=False, indent=2)
        print(f"\n推論紀錄儲存: {pred_path}")
    else:
        with open(args.pred, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if raw and isinstance(raw[0], dict):
            predictions  = [r.get("pred_sql", "") for r in raw]
            pred_records = raw
        else:
            predictions = raw
        print(f"預測結果載入: {args.pred}  ({len(predictions)} 筆)")

    # ── 資料庫連線（選配）────────────────────────────────────
    cursor = None
    if args.db_host and args.db_user:
        try:
            import pyodbc
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={args.db_host};"
                f"DATABASE={args.db_name};"
                f"UID={args.db_user};"
                f"PWD={args.db_pass};"
            )
            cursor = pyodbc.connect(conn_str, timeout=30).cursor()
            print(f"DB connected: {args.db_host}/{args.db_name}")
        except Exception as e:
            print(f"DB connection failed: {e}  (EM only)")

    # ── 評估 ─────────────────────────────────────────────────
    results = evaluate(gold_data, predictions, pred_records, cursor)

    # ── 儲存結果 ─────────────────────────────────────────────
    save_data = {
        "meta": {
            "gold_file":    args.gold,
            "model_path":   args.model or "(from pred file)",
            "timestamp":    datetime.now().isoformat(),
            "total":        results["total"],
            "em_pct":       results["em_pct"],
            "ex_pct":       results["ex_pct"],
        },
        **results,
    }
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(save_data, f, ensure_ascii=False, indent=2)
    print(f"\n評估結果儲存: {args.output}")

    if cursor:
        cursor.close()

    # ── 最終摘要 ─────────────────────────────────────────────
    print(f"\n{'='*50}")
    print(f"  FINAL RESULT")
    print(f"  EM : {results['em_correct']}/{results['total']} = {results['em_pct']:.2f}%")
    if results["ex_pct"] is not None:
        print(f"  EX : {results['ex_correct']}/{results['total']} = {results['ex_pct']:.2f}%")
    print(f"{'='*50}")


if __name__ == "__main__":
    import os
    os.chdir(Path(__file__).parent)
    main()
