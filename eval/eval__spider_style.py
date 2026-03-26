#!/usr/bin/env python3
"""
eval__spider_style.py
Spider 1.0 風格的 WP_M09 Text-to-SQL 評估。

Spider 1.0 評估特點（本腳本完整實作）：
  1. Component-based Exact Match（元件級精確匹配）
     - 將 SQL 拆解為 select / where / groupBy / having / orderBy / keywords 等元件
     - 逐個元件比對，全部匹配才算 EM=1
  2. Partial Match（部分匹配）
     - 每個元件分別報告 Accuracy / Recall / F1
  3. Value-agnostic（忽略值）
     - WHERE 條件只比較欄位名和運算子，不比較具體值
     - 例如 isDel='N' 和 isDel='Y' 視為同一個 condition 結構
  4. Distinct-agnostic（忽略 DISTINCT）
  5. Execution Accuracy（EX，需 DB 連線）
  6. 按難度分組報告（easy / medium / hard / extra）

與我們舊的 eval__em_and_execution_accuracy.py 的差異：
  - 舊版：字串正規化後比對 → 分號、空格、AS 別名不同就判錯
  - 新版：元件級比對 → 只要結構正確就算對，不受格式影響

用法:
  python eval__spider_style.py \
      --model outputs/models/wp_m09_dora_0317_spider_r1/final_model \
      --gold data/wp_m09/val_claude_en_spider_v2.json \
      --output outputs/evaluation_spider_style_0317.json \
      --db-host "SHANE\\SQLEXPRESS" --db-trusted
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from typing import Optional, List, Tuple, Dict, Any

# ============================================================
# T-SQL Parser for WP_M09（將 SQL 字串拆解為結構化元件）
# ============================================================

def normalize_col(col: str) -> str:
    """正規化欄位名：小寫、移除表前綴和方括號。"""
    col = col.strip().lower()
    col = re.sub(r'\[(\w+)\]', r'\1', col)
    # 移除 WP_M09.dbo.XXX. 前綴
    col = re.sub(r'wp_m09\.dbo\.\w+\.', '', col)
    # 移除表別名前綴 (如 sub.xxx, t1.xxx)
    col = re.sub(r'^\w+\.', '', col)
    return col


def extract_table(sql: str) -> str:
    """提取 FROM 子句中的表名。"""
    m = re.search(r'FROM\s+(?:\(|WP_M09\.dbo\.(\w+)|\[WP_M09\]\.\[dbo\]\.\[(\w+)\])', sql, re.IGNORECASE)
    if m:
        return (m.group(1) or m.group(2) or "").lower()
    return ""


def extract_all_tables(sql: str) -> set:
    """提取所有引用的表名。"""
    tables = set()
    for m in re.finditer(r'WP_M09\.dbo\.(\w+)', sql, re.IGNORECASE):
        tables.add(m.group(1).lower())
    for m in re.finditer(r'\[WP_M09\]\.\[dbo\]\.\[(\w+)\]', sql, re.IGNORECASE):
        tables.add(m.group(1).lower())
    return tables


def tokenize_sql(sql: str) -> list:
    """將 SQL 拆成 token 列表。"""
    # 保護字串值
    tokens = []
    i = 0
    s = sql.strip()
    while i < len(s):
        # 跳過空白
        if s[i].isspace():
            i += 1
            continue
        # 字串（含 N 前綴）
        if s[i] == "'" or (s[i] in ('N', 'n') and i + 1 < len(s) and s[i + 1] == "'"):
            start = i
            if s[i] in ('N', 'n'):
                i += 1
            i += 1  # skip opening quote
            while i < len(s) and s[i] != "'":
                i += 1
            i += 1  # skip closing quote
            tokens.append(s[start:i])
            continue
        # 運算子
        if s[i:i+2] in ('>=', '<=', '!=', '<>'):
            tokens.append(s[i:i+2])
            i += 2
            continue
        if s[i] in ('(', ')', ',', '=', '<', '>', '+', '-', '*', '/'):
            tokens.append(s[i])
            i += 1
            continue
        # 標識符或關鍵字
        start = i
        while i < len(s) and not s[i].isspace() and s[i] not in ('(', ')', ',', '=', '<', '>', '+', '-', '*', '/', "'"):
            if s[i:i+2] in ('>=', '<=', '!=', '<>'):
                break
            i += 1
        if i > start:
            tokens.append(s[start:i])
    return tokens


def is_value_token(token: str) -> bool:
    """判斷 token 是否為值（字串或數字）。"""
    if not token:
        return False
    if token.startswith("'") or token.startswith("N'") or token.startswith("n'"):
        return True
    try:
        float(token)
        return True
    except ValueError:
        return False


def parse_select_columns(sql: str) -> list:
    """
    提取 SELECT 子句的欄位。
    回傳: [(agg, col_name), ...] 正規化後
    agg: 'none'/'count'/'sum'/'avg'/'min'/'max'
    """
    # 取 SELECT ... FROM 之間的內容
    m = re.match(r'SELECT\s+(.*?)\s+FROM\s', sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return []

    sel_str = m.group(1).strip()

    # 移除 DISTINCT / TOP N
    sel_str = re.sub(r'^DISTINCT\s+', '', sel_str, flags=re.IGNORECASE)
    sel_str = re.sub(r'^TOP\s+\d+\s+', '', sel_str, flags=re.IGNORECASE)

    # 處理子查詢中的 SELECT — 如果 SELECT 後面跟著子查詢，需要特別處理
    # 簡單方式：按逗號分割（但要注意括號層級）
    columns = []
    depth = 0
    current = ""
    for ch in sel_str:
        if ch == '(':
            depth += 1
            current += ch
        elif ch == ')':
            depth -= 1
            current += ch
        elif ch == ',' and depth == 0:
            columns.append(current.strip())
            current = ""
        else:
            current += ch
    if current.strip():
        columns.append(current.strip())

    result = []
    for col_expr in columns:
        # 移除 AS 別名
        col_clean = re.sub(r'\s+AS\s+\w+', '', col_expr, flags=re.IGNORECASE).strip()

        # 檢查聚合函數
        agg_match = re.match(r'(COUNT|SUM|AVG|MIN|MAX)\s*\((.*)\)', col_clean, re.IGNORECASE)
        if agg_match:
            agg = agg_match.group(1).lower()
            inner = agg_match.group(2).strip()
            # COUNT(DISTINCT xxx) → count, xxx
            inner = re.sub(r'^DISTINCT\s+', '', inner, flags=re.IGNORECASE)
            inner = normalize_col(inner)
            result.append((agg, inner))
        else:
            col_name = normalize_col(col_clean)
            result.append(('none', col_name))

    return sorted(result)


def parse_where_conditions(sql: str) -> list:
    """
    提取 WHERE 子句的條件。
    回傳 value-agnostic 的條件列表: [(col, op), ...]
    """
    # 找 WHERE 子句（到 GROUP BY / ORDER BY / HAVING 或結尾）
    m = re.search(r'\bWHERE\s+(.*?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+HAVING|\s*;?\s*$)',
                  sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return []

    where_str = m.group(1).strip()

    # 處理子查詢中的 WHERE — 只取最外層
    # 簡化：按 AND/OR 分割（忽略括號內的）
    conditions = []
    depth = 0
    current = ""
    tokens = re.split(r'(\bAND\b|\bOR\b)', where_str, flags=re.IGNORECASE)

    for token in tokens:
        t = token.strip()
        if t.upper() in ('AND', 'OR'):
            continue
        if not t:
            continue

        # 移除外層括號
        while t.startswith('(') and t.endswith(')'):
            # 檢查是否匹配的括號
            d = 0
            matched = True
            for i, ch in enumerate(t):
                if ch == '(':
                    d += 1
                elif ch == ')':
                    d -= 1
                if d == 0 and i < len(t) - 1:
                    matched = False
                    break
            if matched:
                t = t[1:-1].strip()
            else:
                break

        # 提取 col op 結構（忽略值）
        # 支援: col = val, col > val, col LIKE val, col IS NULL, col IS NOT NULL,
        #        col BETWEEN val AND val, col IN (...)
        if re.match(r'.+\bIS\s+NOT\s+NULL', t, re.IGNORECASE):
            col = re.match(r'(.+?)\s+IS\s+NOT\s+NULL', t, re.IGNORECASE).group(1)
            conditions.append((normalize_col(col), 'is not null'))
        elif re.match(r'.+\bIS\s+NULL', t, re.IGNORECASE):
            col = re.match(r'(.+?)\s+IS\s+NULL', t, re.IGNORECASE).group(1)
            conditions.append((normalize_col(col), 'is null'))
        elif re.match(r'.+\bBETWEEN\b', t, re.IGNORECASE):
            col = re.match(r'(.+?)\s+BETWEEN\b', t, re.IGNORECASE).group(1)
            conditions.append((normalize_col(col), 'between'))
        elif re.match(r'.+\bLIKE\b', t, re.IGNORECASE):
            col = re.match(r'(.+?)\s+LIKE\b', t, re.IGNORECASE).group(1)
            conditions.append((normalize_col(col), 'like'))
        elif re.match(r'.+\bIN\s*\(', t, re.IGNORECASE):
            col = re.match(r'(.+?)\s+IN\s*\(', t, re.IGNORECASE).group(1)
            conditions.append((normalize_col(col), 'in'))
        elif re.match(r'.+?\s*(<>|!=|>=|<=|>|<|=)\s*', t):
            m2 = re.match(r'(.+?)\s*(<>|!=|>=|<=|>|<|=)\s*', t)
            col = m2.group(1)
            op = m2.group(2)
            if op in ('!=', '<>'):
                op = '!='
            conditions.append((normalize_col(col), op))

    return sorted(conditions)


def parse_group_by(sql: str) -> list:
    """提取 GROUP BY 欄位。"""
    m = re.search(r'GROUP\s+BY\s+(.*?)(?:\s+HAVING|\s+ORDER|\s*;?\s*$)',
                  sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return []
    cols = [normalize_col(c.strip()) for c in m.group(1).split(',')]
    return sorted(cols)


def parse_having(sql: str) -> list:
    """提取 HAVING 條件（value-agnostic）。"""
    m = re.search(r'HAVING\s+(.*?)(?:\s+ORDER|\s*;?\s*$)',
                  sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return []
    # 簡化：提取聚合函數和運算子
    having_str = m.group(1).strip()
    conditions = []
    agg_m = re.match(r'(COUNT|SUM|AVG|MIN|MAX)\s*\(([^)]*)\)\s*(>=|<=|>|<|=|!=)\s*', having_str, re.IGNORECASE)
    if agg_m:
        conditions.append((agg_m.group(1).lower(), normalize_col(agg_m.group(2)), agg_m.group(3)))
    return conditions


def parse_order_by(sql: str) -> list:
    """提取 ORDER BY：[(col, direction), ...]"""
    m = re.search(r'ORDER\s+BY\s+(.*?)(?:\s*;?\s*$)', sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return []
    order_str = m.group(1).strip()
    parts = order_str.split(',')
    result = []
    for p in parts:
        p = p.strip()
        # 移除 AS 別名引用
        direction = 'asc'
        if re.search(r'\bDESC\b', p, re.IGNORECASE):
            direction = 'desc'
        col = re.sub(r'\b(ASC|DESC)\b', '', p, flags=re.IGNORECASE).strip()
        col = normalize_col(col)
        result.append((col, direction))
    return result


def has_distinct(sql: str) -> bool:
    """是否使用 DISTINCT。"""
    return bool(re.search(r'\bSELECT\s+DISTINCT\b', sql, re.IGNORECASE))


def has_top(sql: str) -> Optional[int]:
    """提取 TOP N。"""
    m = re.search(r'\bTOP\s+(\d+)\b', sql, re.IGNORECASE)
    return int(m.group(1)) if m else None


def has_subquery(sql: str) -> bool:
    """是否有子查詢。"""
    return sql.upper().count('SELECT') >= 2


def get_keywords(sql: str) -> set:
    """提取使用的 SQL 關鍵字集合。"""
    kws = set()
    upper = sql.upper()
    if 'WHERE' in upper:
        kws.add('where')
    if 'GROUP BY' in upper:
        kws.add('group')
    if 'HAVING' in upper:
        kws.add('having')
    if 'ORDER BY' in upper:
        kws.add('order')
    if re.search(r'\bTOP\b', upper):
        kws.add('top')
    if re.search(r'\bDISTINCT\b', upper):
        kws.add('distinct')
    if re.search(r'\bLIKE\b', upper):
        kws.add('like')
    if re.search(r'\bBETWEEN\b', upper):
        kws.add('between')
    if re.search(r'\bIN\s*\(', upper):
        kws.add('in')
    if upper.count('SELECT') >= 2:
        kws.add('subquery')
    # AND/OR
    where_m = re.search(r'WHERE\s+(.*?)(?:GROUP|ORDER|HAVING|$)', upper, re.DOTALL)
    if where_m:
        if ' OR ' in where_m.group(1):
            kws.add('or')
    return kws


# ============================================================
# 元件級比較
# ============================================================

def compare_sets(pred_set, gold_set):
    """比較兩個集合，回傳 (gold_total, pred_total, match_count)。"""
    pred_list = list(pred_set) if not isinstance(pred_set, list) else pred_set
    gold_list = list(gold_set) if not isinstance(gold_set, list) else gold_set

    gold_total = len(gold_list)
    pred_total = len(pred_list)

    gold_copy = list(gold_list)
    cnt = 0
    for item in pred_list:
        if item in gold_copy:
            cnt += 1
            gold_copy.remove(item)

    return gold_total, pred_total, cnt


def get_scores(cnt, pred_total, gold_total):
    """計算 Accuracy, Recall, F1。"""
    if pred_total == 0 and gold_total == 0:
        return 1.0, 1.0, 1.0
    acc = cnt / pred_total if pred_total > 0 else 0.0
    rec = cnt / gold_total if gold_total > 0 else 0.0
    f1 = 2 * acc * rec / (acc + rec) if (acc + rec) > 0 else 0.0
    return acc, rec, f1


# ============================================================
# Spider-style 難度計算
# ============================================================

def calc_difficulty(sql: str) -> str:
    """
    模仿 Spider 1.0 的難度計算。
    component1: where, group, order, top, like, or
    component2: subquery
    others: multiple agg, multiple select cols, multiple where conds, multiple group cols
    """
    upper = sql.upper()

    comp1 = 0
    if 'WHERE' in upper:
        comp1 += 1
    if 'GROUP BY' in upper:
        comp1 += 1
    if 'ORDER BY' in upper:
        comp1 += 1
    if re.search(r'\bTOP\b', upper):
        comp1 += 1
    if re.search(r'\bLIKE\b', upper):
        comp1 += 1
    # OR in WHERE
    where_m = re.search(r'WHERE\s+(.*?)(?:GROUP|ORDER|HAVING|$)', upper, re.DOTALL)
    if where_m and ' OR ' in where_m.group(1):
        comp1 += 1

    comp2 = 1 if upper.count('SELECT') >= 2 else 0

    others = 0
    # Multiple aggregations
    agg_count = len(re.findall(r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\(', upper))
    if agg_count > 1:
        others += 1
    # Multiple select columns
    sel_m = re.match(r'SELECT\s+(?:DISTINCT\s+)?(?:TOP\s+\d+\s+)?(.*?)\s+FROM\s', upper, re.DOTALL)
    if sel_m:
        # 簡單計算逗號數（在括號外）
        depth = 0
        comma_count = 0
        for ch in sel_m.group(1):
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif ch == ',' and depth == 0:
                comma_count += 1
        if comma_count > 0:
            others += 1
    # Multiple where conditions
    where_conds = parse_where_conditions(sql)
    if len(where_conds) > 1:
        others += 1
    # Multiple group by
    group_cols = parse_group_by(sql)
    if len(group_cols) > 1:
        others += 1

    if comp1 <= 1 and others == 0 and comp2 == 0:
        return "easy"
    elif (others <= 2 and comp1 <= 1 and comp2 == 0) or \
            (comp1 <= 2 and others < 2 and comp2 == 0):
        return "medium"
    elif (others > 2 and comp1 <= 2 and comp2 == 0) or \
            (2 < comp1 <= 3 and others <= 2 and comp2 == 0) or \
            (comp1 <= 1 and others == 0 and comp2 <= 1):
        return "hard"
    else:
        return "extra"


# ============================================================
# Component-based Exact Match
# ============================================================

def eval_component_match(pred_sql: str, gold_sql: str) -> dict:
    """
    Spider 風格的元件級匹配。
    回傳每個元件的 F1 分數和整體 exact match。
    """
    results = {}

    # 1. SELECT columns（value-agnostic: 只比較欄位和聚合函數）
    pred_sel = parse_select_columns(pred_sql)
    gold_sel = parse_select_columns(gold_sql)
    gt, pt, cnt = compare_sets(pred_sel, gold_sel)
    acc, rec, f1 = get_scores(cnt, pt, gt)
    results['select'] = {'acc': acc, 'rec': rec, 'f1': f1, 'gold': gt, 'pred': pt, 'match': cnt}

    # 2. WHERE conditions（value-agnostic: 只比較欄位和運算子）
    pred_where = parse_where_conditions(pred_sql)
    gold_where = parse_where_conditions(gold_sql)
    gt, pt, cnt = compare_sets(pred_where, gold_where)
    acc, rec, f1 = get_scores(cnt, pt, gt)
    results['where'] = {'acc': acc, 'rec': rec, 'f1': f1, 'gold': gt, 'pred': pt, 'match': cnt}

    # 3. GROUP BY
    pred_gb = parse_group_by(pred_sql)
    gold_gb = parse_group_by(gold_sql)
    gt, pt, cnt = compare_sets(pred_gb, gold_gb)
    acc, rec, f1 = get_scores(cnt, pt, gt)
    results['groupBy'] = {'acc': acc, 'rec': rec, 'f1': f1, 'gold': gt, 'pred': pt, 'match': cnt}

    # 4. HAVING
    pred_hv = parse_having(pred_sql)
    gold_hv = parse_having(gold_sql)
    gt, pt, cnt = compare_sets(pred_hv, gold_hv)
    acc, rec, f1 = get_scores(cnt, pt, gt)
    results['having'] = {'acc': acc, 'rec': rec, 'f1': f1, 'gold': gt, 'pred': pt, 'match': cnt}

    # 5. ORDER BY
    pred_ob = parse_order_by(pred_sql)
    gold_ob = parse_order_by(gold_sql)
    gt, pt, cnt = compare_sets(pred_ob, gold_ob)
    acc, rec, f1 = get_scores(cnt, pt, gt)
    results['orderBy'] = {'acc': acc, 'rec': rec, 'f1': f1, 'gold': gt, 'pred': pt, 'match': cnt}

    # 6. Table（FROM）
    pred_tables = extract_all_tables(pred_sql)
    gold_tables = extract_all_tables(gold_sql)
    gt, pt, cnt = compare_sets(list(pred_tables), list(gold_tables))
    acc, rec, f1 = get_scores(cnt, pt, gt)
    results['table'] = {'acc': acc, 'rec': rec, 'f1': f1, 'gold': gt, 'pred': pt, 'match': cnt}

    # 7. Keywords
    pred_kw = get_keywords(pred_sql)
    gold_kw = get_keywords(gold_sql)
    gt, pt, cnt = compare_sets(list(pred_kw), list(gold_kw))
    acc, rec, f1 = get_scores(cnt, pt, gt)
    results['keywords'] = {'acc': acc, 'rec': rec, 'f1': f1, 'gold': gt, 'pred': pt, 'match': cnt}

    # Overall Exact Match: 所有關鍵元件的 F1 都必須 = 1
    key_components = ['select', 'where', 'groupBy', 'having', 'orderBy', 'table']
    exact_match = all(results[k]['f1'] == 1.0 for k in key_components)
    results['exact_match'] = exact_match

    return results


# ============================================================
# EX 評估
# ============================================================

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


# ============================================================
# Table Inference（同 eval v1，用於非 chat template 模型）
# ============================================================

TABLE_NOTES = {
    "WP_vProduct":   "Note: pNo is a sequential product number (1, 2, 3...), NOT a date.",
    "WP_vInventory": "Note: pNo is a sequential product number, not a date.",
    "WP_vTransfer":  "Note: Filter by date using LEFT(TransferId,8)='YYYYMMDD'.",
    "WP_vAcctIn":    "Note: Filter by date using LEFT(acctInId,8)='YYYYMMDD'. isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vAcctOut":   "Note: Filter by date using LEFT(acctOutId,8)='YYYYMMDD'. isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vOutStock":  "Note: Filter by date using LEFT(OutStkId,8)='YYYYMMDD'. isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vProvider":  "Note: Use isStop for active/inactive status.",
}


def infer_table_from_question(question: str) -> str:
    q = question.lower()
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
                               "出庫", "銷貨"]):
        return "WP_vOutStock"
    if any(kw in q for kw in ["vendor", "supplier", "provider", "供應商", "廠商",
                               "isstop", "pvboss", "ctacttel"]):
        if "product" not in q and "barcode" not in q:
            return "WP_vProvider"
    if any(kw in q for kw in ["product", "barcode", "商品", "產品"]):
        return "WP_vProduct"
    return "WP_M09"


def build_inference_prompt(question: str, table: str) -> str:
    note = TABLE_NOTES.get(table, "")
    lines = [f"Table: {table}"]
    if note:
        lines.append(note)
    lines.append(f"Question: {question}")
    lines.append("SQL:")
    return "\n".join(lines)


# ============================================================
# 模型推論（支援舊格式和新 Chat Template 格式）
# ============================================================

def detect_model_format(model_path: str) -> str:
    """偵測模型訓練格式。檢查 train_format 和 methodology 欄位。"""
    info_path = os.path.join(model_path, "training_info.json")
    if os.path.exists(info_path):
        with open(info_path) as f:
            info = json.load(f)
        # 優先檢查 train_format，再 fallback 檢查 methodology
        for key in ("train_format", "methodology"):
            val = info.get(key, "")
            if "Chat Template" in val:
                return "chat"
    return "plain"


def run_inference(model_path: str, test_data: list) -> tuple:
    import torch
    from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
    from peft import PeftModel

    print(f"\n載入模型: {model_path} ...")

    model_format = detect_model_format(model_path)
    print(f"偵測到訓練格式: {model_format}")

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

    # 如果是 chat 格式，載入 system prompt
    system_prompt = None
    if model_format == "chat":
        # 根據 training_info.json 的 train_script 動態載入對應的 system prompt
        info_path = os.path.join(model_path, "training_info.json")
        train_script = ""
        schema_mode = "full"
        include_rules = True
        if os.path.exists(info_path):
            with open(info_path) as f:
                info = json.load(f)
            train_script = info.get("train_script", "")
            schema_mode = info.get("schema_mode", "full")
            include_rules = info.get("include_rules", True)

        loaded = False
        # 嘗試從訓練腳本載入 build_system_prompt
        if train_script:
            module_name = train_script.replace(".py", "")
            try:
                import importlib
                mod = importlib.import_module(module_name)
                if hasattr(mod, "build_system_prompt"):
                    system_prompt = mod.build_system_prompt(
                        schema_mode=schema_mode, include_rules=include_rules
                    )
                    loaded = True
                    print(f"  System prompt 載入自: {module_name}.build_system_prompt()")
            except Exception as e:
                print(f"  [WARN] 無法從 {module_name} 載入: {e}")

        if not loaded:
            # Fallback: 嘗試舊版 v0322
            try:
                from train__dora_spider_v0322 import FULL_SYSTEM_PROMPT
                system_prompt = FULL_SYSTEM_PROMPT
                loaded = True
            except ImportError:
                pass

        if not loaded:
            print("  [WARN] 無法載入任何 system prompt，fallback 到 plain 格式推論")
            model_format = "plain"

    import time as _time

    # 先測試第 1 筆，確認推論正常
    first_q = test_data[0]["question"]
    if model_format == "chat":
        _test_msgs = [{"role": "system", "content": system_prompt}, {"role": "user", "content": first_q}]
        _test_prompt = tokenizer.apply_chat_template(_test_msgs, tokenize=False, add_generation_prompt=True)
    else:
        _test_prompt = build_inference_prompt(first_q, infer_table_from_question(first_q))
    _test_inputs = tokenizer(_test_prompt, return_tensors="pt")
    print(f"模型載入完成，開始推論 {len(test_data)} 筆 ...")
    print(f"  Prompt tokens: {_test_inputs['input_ids'].shape[1]}, max_new_tokens: 300")
    print(f"  第 1 筆推論中 ...", end="", flush=True)

    predictions = []
    pred_records = []

    for i, item in enumerate(test_data):
        t0 = _time.time()
        question = item["question"]
        gold_sql = item.get("query", "")

        if model_format == "chat":
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": question},
            ]
            prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
        else:
            inferred_table = infer_table_from_question(question)
            prompt = build_inference_prompt(question, inferred_table)

        inputs = tokenizer(prompt, return_tensors="pt").to(model.device)

        with torch.no_grad():
            outputs = model.generate(
                **inputs,
                max_new_tokens=300,
                do_sample=False,
                temperature=1.0,
                pad_token_id=tokenizer.eos_token_id,
            )

        gen_tokens = outputs[0].shape[0] - inputs["input_ids"].shape[1]

        # 用 token 級別切割，避免字串長度偏差
        generated_ids = outputs[0][inputs["input_ids"].shape[1]:]
        pred_sql = tokenizer.decode(generated_ids, skip_special_tokens=True).strip()

        if model_format != "chat":
            # plain 格式的 fallback 解析
            full_output = tokenizer.decode(outputs[0], skip_special_tokens=True)
            if "SQL:" in full_output:
                pred_sql = full_output.split("SQL:")[-1].strip()

        # 清理：模型可能在 SQL 前輸出 schema 資訊（如 "WP_vAcctIn (isDel='N') SELECT ..."）
        # 策略：用 regex 找到第一個 SELECT/WITH 開頭的 SQL 語句
        raw_pred = pred_sql

        # 方法1：從文本中提取 SELECT ... 或 WITH ... 開頭的 SQL
        sql_match = re.search(r'\b(SELECT\b.+|WITH\b.+)', raw_pred, re.IGNORECASE | re.DOTALL)
        if sql_match:
            pred_sql = sql_match.group(1).strip()
        else:
            # fallback: 原始邏輯
            lines = raw_pred.split("\n")
            pred_sql = ""
            for line in lines:
                line = line.strip()
                if line.upper().startswith("SELECT") or line.upper().startswith("WITH"):
                    pred_sql = line
                    break
            if not pred_sql and lines:
                pred_sql = lines[0].strip()

        # 只取第一個完整 SQL（到分號或換行）
        pred_sql = pred_sql.split("\n")[0].strip()

        pred_sql = pred_sql.rstrip(';').strip()
        if pred_sql:
            pred_sql += ";"

        predictions.append(pred_sql)

        gold_table = re.search(r'WP_M09\.dbo\.(\w+)', gold_sql)
        gold_table = gold_table.group(1) if gold_table else "unknown"
        pred_table = re.search(r'WP_M09\.dbo\.(\w+)', pred_sql) if pred_sql else None
        pred_table = pred_table.group(1) if pred_table else "unknown"

        pred_records.append({
            "id": i,
            "question": question,
            "inferred_table": pred_table,
            "gold_table": gold_table,
            "pred_sql": pred_sql,
            "gold_sql": gold_sql,
        })

        elapsed = _time.time() - t0
        if i == 0:
            print(f" {elapsed:.1f}s, 生成 {gen_tokens} tokens", flush=True)
            est_total = elapsed * len(test_data) / 60
            print(f"  預估總時間: {est_total:.1f} 分鐘", flush=True)
        if (i + 1) % 10 == 0:
            print(f"  已推論: {i+1}/{len(test_data)} ({elapsed:.1f}s)", flush=True)

    table_correct = sum(1 for r in pred_records if r["inferred_table"] == r["gold_table"])
    print(f"\nTable 推斷準確率: {table_correct}/{len(pred_records)} "
          f"= {table_correct/len(pred_records)*100:.1f}%")

    return predictions, pred_records


# ============================================================
# 主評估流程
# ============================================================

def evaluate(gold_data: list, predictions: list, cursor=None):
    n = len(gold_data)

    # 難度等級
    levels = ['easy', 'medium', 'hard', 'extra', 'all']
    component_names = ['select', 'where', 'groupBy', 'having', 'orderBy', 'table', 'keywords']

    scores = {}
    for level in levels:
        scores[level] = {
            'count': 0,
            'em_string': 0,     # 舊版字串 EM
            'em_component': 0,  # Spider 風格元件 EM
            'ex': 0,
            'partial': {c: {'acc_sum': 0, 'rec_sum': 0, 'f1_sum': 0, 'count': 0}
                        for c in component_names}
        }

    # Per-view stats
    view_stats = defaultdict(lambda: {
        'count': 0, 'em_string': 0, 'em_component': 0, 'ex': 0
    })

    details = []

    for i in range(n):
        gold_sql = gold_data[i]["query"]
        pred_sql = predictions[i]
        question = gold_data[i]["question"]

        # 使用 gold 的 difficulty 或自動計算
        difficulty = gold_data[i].get("difficulty")
        if not difficulty:
            difficulty = calc_difficulty(gold_sql)

        view = gold_data[i].get("view", "")
        if not view:
            m = re.search(r'WP_M09\.dbo\.(\w+)', gold_sql)
            view = m.group(1) if m else "unknown"

        # --- 字串 EM（舊方法）---
        from eval__em_and_execution_accuracy import normalize_sql
        em_string = normalize_sql(pred_sql) == normalize_sql(gold_sql)

        # --- 元件 EM（Spider 風格）---
        comp_results = eval_component_match(pred_sql, gold_sql)
        em_component = comp_results['exact_match']

        # --- EX ---
        ex = False
        if cursor:
            ex = compute_ex(cursor, pred_sql, gold_sql)

        # 累計
        for level_key in [difficulty, 'all']:
            if level_key not in scores:
                scores[level_key] = {
                    'count': 0, 'em_string': 0, 'em_component': 0, 'ex': 0,
                    'partial': {c: {'acc_sum': 0, 'rec_sum': 0, 'f1_sum': 0, 'count': 0}
                                for c in component_names}
                }
            scores[level_key]['count'] += 1
            if em_string:
                scores[level_key]['em_string'] += 1
            if em_component:
                scores[level_key]['em_component'] += 1
            if ex:
                scores[level_key]['ex'] += 1

            for comp in component_names:
                if comp in comp_results:
                    scores[level_key]['partial'][comp]['acc_sum'] += comp_results[comp]['acc']
                    scores[level_key]['partial'][comp]['rec_sum'] += comp_results[comp]['rec']
                    scores[level_key]['partial'][comp]['f1_sum'] += comp_results[comp]['f1']
                    scores[level_key]['partial'][comp]['count'] += 1

        view_stats[view]['count'] += 1
        if em_string:
            view_stats[view]['em_string'] += 1
        if em_component:
            view_stats[view]['em_component'] += 1
        if ex:
            view_stats[view]['ex'] += 1

        details.append({
            "id": i,
            "question": question,
            "gold_sql": gold_sql,
            "pred_sql": pred_sql,
            "view": view,
            "difficulty": difficulty,
            "em_string": em_string,
            "em_component": em_component,
            "ex": ex if cursor else None,
            "components": {k: v for k, v in comp_results.items() if k != 'exact_match'},
        })

    # ---- 輸出報告 ----
    print("\n" + "=" * 80)
    print("WP_M09 Text-to-SQL 評估報告（Spider 1.0 風格）")
    print("=" * 80)

    print(f"\n測試筆數: {n}")

    # 整體指標
    a = scores['all']
    print(f"\n{'='*60}")
    print(f"  整體指標")
    print(f"{'='*60}")
    print(f"  String EM（舊方法）:     {a['em_string']}/{a['count']} = {a['em_string']/a['count']*100:.2f}%")
    print(f"  Component EM（Spider式）: {a['em_component']}/{a['count']} = {a['em_component']/a['count']*100:.2f}%")
    if cursor:
        print(f"  Execution Accuracy (EX): {a['ex']}/{a['count']} = {a['ex']/a['count']*100:.2f}%")

    # 按難度
    print(f"\n{'─'*70}")
    print(f"  按難度分組")
    print(f"{'─'*70}")
    header = f"  {'Difficulty':<10} {'Count':>6} {'Str-EM':>8} {'Str-EM%':>8} {'Comp-EM':>8} {'Comp-EM%':>9}"
    if cursor:
        header += f" {'EX':>6} {'EX%':>8}"
    print(header)
    print(f"  {'─'*65}")
    for diff in ['easy', 'medium', 'hard', 'extra']:
        if diff not in scores or scores[diff]['count'] == 0:
            continue
        s = scores[diff]
        line = (f"  {diff:<10} {s['count']:>6} {s['em_string']:>8} "
                f"{s['em_string']/s['count']*100:>7.2f}% {s['em_component']:>8} "
                f"{s['em_component']/s['count']*100:>8.2f}%")
        if cursor:
            line += f" {s['ex']:>6} {s['ex']/s['count']*100:>7.2f}%"
        print(line)

    # 按 View
    print(f"\n{'─'*70}")
    print(f"  按 View 分組")
    print(f"{'─'*70}")
    header = f"  {'View':<18} {'Count':>6} {'Str-EM':>8} {'Str-EM%':>8} {'Comp-EM':>8} {'Comp-EM%':>9}"
    if cursor:
        header += f" {'EX':>6} {'EX%':>8}"
    print(header)
    print(f"  {'─'*65}")
    for view in sorted(view_stats.keys()):
        s = view_stats[view]
        line = (f"  {view:<18} {s['count']:>6} {s['em_string']:>8} "
                f"{s['em_string']/s['count']*100:>7.2f}% {s['em_component']:>8} "
                f"{s['em_component']/s['count']*100:>8.2f}%")
        if cursor:
            line += f" {s['ex']:>6} {s['ex']/s['count']*100:>7.2f}%"
        print(line)

    # Partial Match（元件級）
    print(f"\n{'─'*70}")
    print(f"  Partial Match（元件級 F1 平均）")
    print(f"{'─'*70}")
    header = f"  {'Component':<12}"
    for diff in ['easy', 'medium', 'hard', 'all']:
        header += f" {diff:>10}"
    print(header)
    print(f"  {'─'*55}")
    for comp in component_names:
        line = f"  {comp:<12}"
        for diff in ['easy', 'medium', 'hard', 'all']:
            if diff in scores and scores[diff]['partial'][comp]['count'] > 0:
                avg_f1 = scores[diff]['partial'][comp]['f1_sum'] / scores[diff]['partial'][comp]['count']
                line += f" {avg_f1:>9.3f}"
            else:
                line += f" {'N/A':>10}"
        print(line)

    # 錯誤分析
    em_comp_fail = [d for d in details if not d['em_component']]
    if em_comp_fail:
        print(f"\n{'─'*70}")
        print(f"  Component EM 失敗分析（前 10 筆，共 {len(em_comp_fail)} 筆）")
        print(f"{'─'*70}")
        for d in em_comp_fail[:10]:
            failed_comps = [k for k, v in d['components'].items() if v.get('f1', 1) < 1.0]
            print(f"\n  [{d['id']}] ({d['view']}, {d['difficulty']}) 失敗元件: {failed_comps}")
            print(f"    Q: {d['question'][:80]}")
            print(f"    Gold: {d['gold_sql'][:100]}")
            print(f"    Pred: {d['pred_sql'][:100]}")

    return {
        "total": n,
        "em_string": a['em_string'],
        "em_string_pct": round(a['em_string'] / a['count'] * 100, 2),
        "em_component": a['em_component'],
        "em_component_pct": round(a['em_component'] / a['count'] * 100, 2),
        "ex_correct": a['ex'] if cursor else None,
        "ex_pct": round(a['ex'] / a['count'] * 100, 2) if cursor else None,
        "scores_by_difficulty": {
            diff: {
                "count": scores[diff]['count'],
                "em_string": scores[diff]['em_string'],
                "em_component": scores[diff]['em_component'],
                "ex": scores[diff]['ex'],
            }
            for diff in ['easy', 'medium', 'hard', 'extra', 'all']
            if diff in scores and scores[diff]['count'] > 0
        },
        "scores_by_view": dict(view_stats),
        "partial_match": {
            comp: {
                diff: round(scores[diff]['partial'][comp]['f1_sum'] /
                            scores[diff]['partial'][comp]['count'], 4)
                if diff in scores and scores[diff]['partial'][comp]['count'] > 0 else None
                for diff in ['easy', 'medium', 'hard', 'all']
            }
            for comp in component_names
        },
        "details": details,
    }


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="WP_M09 Spider-style 評估")
    parser.add_argument("--gold", required=True, help="Gold 測試集 JSON")
    parser.add_argument("--pred", default=None, help="預測結果 JSON")
    parser.add_argument("--model", default=None, help="模型路徑")
    parser.add_argument("--db-host", default=None, help="SQL Server host")
    parser.add_argument("--db-name", default="WP_M09", help="Database name")
    parser.add_argument("--db-user", default=None, help="DB user")
    parser.add_argument("--db-pass", default=None, help="DB password")
    parser.add_argument("--db-trusted", action="store_true", help="Windows Auth")
    parser.add_argument("--output", default="eval_spider_style.json", help="輸出路徑")
    args = parser.parse_args()

    with open(args.gold, "r", encoding="utf-8") as f:
        gold_data = json.load(f)
    print(f"Gold 測試集: {len(gold_data)} 筆")

    if args.pred:
        with open(args.pred, "r", encoding="utf-8") as f:
            raw_pred = json.load(f)
        if raw_pred and isinstance(raw_pred[0], dict):
            predictions = [p.get("pred_sql", p.get("sql", "")) for p in raw_pred]
        else:
            predictions = raw_pred
    elif args.model:
        predictions, pred_records = run_inference(args.model, gold_data)
        pred_out = args.output.replace(".json", "_predictions.json")
        with open(pred_out, "w", encoding="utf-8") as f:
            json.dump(pred_records, f, indent=2, ensure_ascii=False)
        print(f"推論紀錄: {pred_out}")
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
            print(f"已連線資料庫: {args.db_host}/{args.db_name}")
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
    print(f"\n評估結果: {args.output}")

    if cursor:
        cursor.close()


if __name__ == "__main__":
    main()
