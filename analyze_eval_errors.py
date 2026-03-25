"""Analyze eval_enterprise_full_0322.json: classify all error types."""
import json
import re
from collections import defaultdict, Counter

with open("D:/spider1_training/outputs/eval_enterprise_full_0322.json", "r", encoding="utf-8") as f:
    data = json.load(f)

predictions = data["predictions"]
total = len(predictions)

# Split correct vs wrong EX
correct_ex = [p for p in predictions if p.get("ex") == True]
wrong_ex = [p for p in predictions if p.get("ex") == False]

print(f"=" * 80)
print(f"EVALUATION ANALYSIS: eval_enterprise_full_0322.json")
print(f"=" * 80)
print(f"\n1. OVERALL BREAKDOWN")
print(f"   Total predictions: {total}")
print(f"   Correct EX: {len(correct_ex)} ({len(correct_ex)/total*100:.1f}%)")
print(f"   Wrong EX:   {len(wrong_ex)} ({len(wrong_ex)/total*100:.1f}%)")

# ---- Analyze correct EX: EM vs non-EM ----
correct_em = [p for p in correct_ex if p.get("em") == True]
correct_ex_not_em = [p for p in correct_ex if p.get("em") == False]
print(f"\n5. CORRECT EX ANALYSIS")
print(f"   Correct EX total: {len(correct_ex)}")
print(f"   Also correct EM:  {len(correct_em)} ({len(correct_em)/len(correct_ex)*100:.1f}%)")
print(f"   Different SQL but same result: {len(correct_ex_not_em)} ({len(correct_ex_not_em)/len(correct_ex)*100:.1f}%)")

# ---- Classify wrong EX ----
# Known columns per view (approximate - for hallucinated column detection)
known_columns = {
    "WP_vAcctIn": {"acctInId", "pvSn", "pvName", "amount", "tax", "totalAmount", "acctInDate",
                    "isDel", "dtlSn", "pSn", "pName", "pUnit", "oStkDtlQty", "oStkDtlAmt",
                    "oStkDtlPrice", "dtlIsDel", "remark"},
    "WP_vAcctOut": {"acctOutId", "pvSn", "pvName", "amount", "tax", "totalAmount", "acctOutDate",
                     "isDel", "dtlSn", "pSn", "pName", "pUnit", "oStkDtlQty", "oStkDtlAmt",
                     "oStkDtlPrice", "dtlIsDel", "remark"},
    "WP_vOutStock": {"OutStkId", "pvSn", "pvName", "amount", "tax", "totalAmount", "OutStkDate",
                      "isDel", "dtlSn", "pSn", "pName", "pUnit", "oStkDtlQty", "oStkDtlAmt",
                      "oStkDtlPrice", "dtlIsDel", "remark"},
    "WP_vTransfer": {"TransferId", "fromWh", "toWh", "TransferDate", "isDel",
                      "dtlSn", "pSn", "pName", "pUnit", "TransferQty", "dtlIsDel", "remark"},
    "WP_vInventory": {"pSn", "pNo", "pName", "pUnit", "whNo", "whName", "pStock"},
    "WP_vProduct": {"pSn", "pNo", "pName", "pUnit", "pPrice", "pCost", "pSafeStock",
                     "pBarcode", "pSpec", "pCategory"},
    "WP_vProvider": {"pvSn", "pvNo", "pvName", "pvTel", "pvFax", "pvAddr", "pvContact",
                      "pvEmail", "pvDiscount", "isStop", "pvRemark"},
}

# Views that should have isDel
views_with_isDel = {"WP_vAcctIn", "WP_vAcctOut", "WP_vOutStock", "WP_vTransfer"}
views_without_isDel = {"WP_vInventory", "WP_vProduct", "WP_vProvider"}

def extract_columns_from_sql(sql):
    """Extract column names referenced in SQL (rough)."""
    # Remove string literals
    s = re.sub(r"'[^']*'", "", sql)
    # Find word-like tokens that could be column names
    tokens = re.findall(r'\b([a-zA-Z_]\w*)\b', s)
    # Filter out SQL keywords
    keywords = {'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN',
                'IS', 'NULL', 'AS', 'ON', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
                'GROUP', 'BY', 'ORDER', 'ASC', 'DESC', 'HAVING', 'DISTINCT', 'TOP',
                'LIMIT', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'CASE', 'WHEN', 'THEN',
                'ELSE', 'END', 'CAST', 'CONVERT', 'INT', 'VARCHAR', 'FLOAT', 'N',
                'WP_M09', 'dbo', 'WP_vAcctIn', 'WP_vAcctOut', 'WP_vOutStock',
                'WP_vTransfer', 'WP_vInventory', 'WP_vProduct', 'WP_vProvider',
                'sub', 'subquery', 'a', 'b', 't', 't1', 't2', 'total', 'cnt', 'count',
                'amount', 'total_amount', 'WITH', 'UNION', 'ALL', 'EXISTS', 'ANY',
                'UPPER', 'LOWER', 'LEN', 'SUBSTRING', 'LEFT', 'RIGHT', 'TRIM',
                'ISNULL', 'COALESCE', 'OVER', 'PARTITION', 'ROW_NUMBER', 'RANK',
                'DENSE_RANK', 'LAG', 'LEAD', 'OFFSET', 'FETCH', 'NEXT', 'ROWS',
                'ONLY', 'PERCENT', 'TIES', 'SET', 'UPDATE', 'INSERT', 'DELETE',
                'CREATE', 'ALTER', 'DROP', 'TABLE', 'VIEW', 'INDEX', 'CROSS', 'APPLY',
                'STUFF', 'XML', 'PATH', 'FOR', 'CONCAT'}
    return [t for t in tokens if t.upper() not in keywords]

def get_pred_table(p):
    """Get the table from pred_sql."""
    return p.get("pred_table", "")

def classify_error(p):
    """Classify a wrong-EX prediction into error categories. Returns list of categories."""
    gold = p.get("gold_sql", "")
    pred = p.get("pred_sql", "")
    gold_table = p.get("gold_table", "")
    pred_table = get_pred_table(p)
    ex_error = p.get("ex_error", "")
    categories = []

    pred_upper = pred.upper()
    gold_upper = gold.upper()

    # 1. syntax_error - SQL execution error
    if ex_error and ("ERROR" in ex_error or "error" in ex_error.lower()):
        # Check if it's specifically a hallucinated column error
        if "無效的資料行名稱" in ex_error or "Invalid column name" in ex_error:
            categories.append("hallucinated_column")
        else:
            categories.append("syntax_error")

    # 2. wrong_table
    if gold_table != pred_table and pred_table:
        categories.append("wrong_table")

    # 3. limit_vs_top
    if "LIMIT" in pred_upper and "LIMIT" not in gold_upper:
        categories.append("limit_vs_top")

    # 4. subquery_dedup - gold uses subquery for dedup but pred uses SUM(DISTINCT)
    if ("SUM(DISTINCT" in pred_upper or "SUM( DISTINCT" in pred_upper) and "sub" in gold.lower():
        categories.append("subquery_dedup")
    elif re.search(r'SUM\s*\(\s*DISTINCT', pred_upper) and "SELECT" in gold_upper.split("FROM")[0] and gold_upper.count("SELECT") > 1:
        categories.append("subquery_dedup")

    # 5. missing_isDel - gold has isDel but pred doesn't
    gold_has_isDel = "isDel" in gold and gold_table in views_with_isDel
    pred_has_isDel = "isDel" in pred
    gold_has_dtlIsDel = "dtlIsDel" in gold
    pred_has_dtlIsDel = "dtlIsDel" in pred

    if gold_has_isDel and not pred_has_isDel:
        categories.append("missing_isDel")
    elif gold_has_dtlIsDel and not pred_has_dtlIsDel and gold_table in views_with_isDel:
        categories.append("missing_isDel")

    # 6. extra_isDel - pred has isDel but gold doesn't, or pred adds it on tables without isDel
    if not gold_has_isDel and pred_has_isDel and pred_table in views_without_isDel:
        categories.append("extra_isDel")
    elif "isDel" in pred and "isDel" not in gold and pred_table in views_without_isDel:
        categories.append("extra_isDel")

    # 7. wrong_aggregation - different aggregation functions
    agg_funcs = ["SUM", "COUNT", "AVG", "MIN", "MAX"]
    for func in agg_funcs:
        gold_has = func + "(" in gold_upper.replace(" ", "")
        pred_has = func + "(" in pred_upper.replace(" ", "")
        if gold_has != pred_has:
            if "subquery_dedup" not in categories:  # Don't double-count
                categories.append("wrong_aggregation")
            break

    # 8. wrong_filter - different WHERE conditions (beyond isDel)
    # Extract WHERE clause comparison
    def get_where_conditions(sql):
        m = re.search(r'WHERE\s+(.+?)(?:GROUP|ORDER|HAVING|$)', sql, re.IGNORECASE | re.DOTALL)
        if m:
            conds = m.group(1).strip()
            # Remove isDel conditions for comparison
            conds = re.sub(r"(AND\s+)?isDel\s*=\s*'N'", "", conds, flags=re.IGNORECASE)
            conds = re.sub(r"(AND\s+)?dtlIsDel\s*=\s*'N'", "", conds, flags=re.IGNORECASE)
            return conds.strip()
        return ""

    gold_where = get_where_conditions(gold)
    pred_where = get_where_conditions(pred)

    # Check for meaningful WHERE difference (not just formatting)
    if gold_where and pred_where:
        # Normalize for comparison
        gw = re.sub(r'\s+', ' ', gold_where).strip().lower()
        pw = re.sub(r'\s+', ' ', pred_where).strip().lower()
        if gw != pw:
            # Check if it's a date filter difference (LIKE pattern)
            gold_dates = re.findall(r"like\s*'(\d{6,8})%'", gold.lower())
            pred_dates = re.findall(r"like\s*'(\d{6,8})%'", pred.lower())
            if gold_dates != pred_dates and (gold_dates or pred_dates):
                if "wrong_filter" not in categories:
                    categories.append("wrong_filter")
    elif gold_where and not pred_where:
        if "missing_isDel" not in categories:
            categories.append("wrong_filter")
    elif not gold_where and pred_where:
        if "extra_isDel" not in categories:
            categories.append("wrong_filter")

    # 9. wrong_columns - different SELECT columns (same table)
    if gold_table == pred_table and "hallucinated_column" not in categories:
        # Extract SELECT part
        def get_select_part(sql):
            m = re.match(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
            if m:
                return m.group(1).strip().lower()
            return ""
        gold_sel = get_select_part(gold)
        pred_sel = get_select_part(pred)
        if gold_sel and pred_sel:
            gs = re.sub(r'\s+', ' ', gold_sel).strip()
            ps = re.sub(r'\s+', ' ', pred_sel).strip()
            # Check if columns are meaningfully different
            if gs != ps and "wrong_aggregation" not in categories:
                # Check if one has * and other doesn't
                if ("*" in gs) != ("*" in ps):
                    categories.append("wrong_columns")
                elif gs != ps:
                    # More detailed check - extract column names
                    g_cols = set(re.findall(r'\b(\w+)\b', gs))
                    p_cols = set(re.findall(r'\b(\w+)\b', ps))
                    sql_words = {'select', 'distinct', 'top', 'as', 'count', 'sum', 'avg', 'min', 'max', 'case', 'when', 'then', 'else', 'end'}
                    g_cols -= sql_words
                    p_cols -= sql_words
                    if g_cols != p_cols:
                        categories.append("wrong_columns")

    # If no category found, mark as other
    if not categories:
        categories.append("other")

    return categories

# Classify all wrong predictions
error_classifications = {}
category_examples = defaultdict(list)
category_counts = Counter()

for p in wrong_ex:
    idx = p["idx"]
    cats = classify_error(p)
    error_classifications[idx] = cats
    for cat in cats:
        category_counts[cat] += 1
        category_examples[cat].append(p)

# Print error breakdown
print(f"\n{'=' * 80}")
print(f"2. ERROR CLASSIFICATION (Wrong EX: {len(wrong_ex)} predictions)")
print(f"{'=' * 80}")
print(f"   Note: A prediction may have multiple error types.\n")

ordered_cats = [
    "subquery_dedup", "limit_vs_top", "wrong_columns", "hallucinated_column",
    "missing_isDel", "extra_isDel", "wrong_table", "wrong_aggregation",
    "wrong_filter", "syntax_error", "other"
]

for cat in ordered_cats:
    cnt = category_counts.get(cat, 0)
    pct = cnt / len(wrong_ex) * 100 if wrong_ex else 0
    print(f"   {cat:25s}: {cnt:3d} ({pct:5.1f}%)")

# Print total (with note about multi-labeling)
total_labels = sum(category_counts.values())
print(f"\n   Total labels: {total_labels} (some predictions have multiple error types)")
print(f"   Unique wrong predictions: {len(wrong_ex)}")

# Print examples for each category
print(f"\n{'=' * 80}")
print(f"3. REPRESENTATIVE EXAMPLES PER CATEGORY")
print(f"{'=' * 80}")

for cat in ordered_cats:
    examples = category_examples.get(cat, [])
    if not examples:
        continue
    print(f"\n--- {cat} ({category_counts[cat]} occurrences) ---")
    for i, ex in enumerate(examples[:3]):
        print(f"\n  Example {i+1} (idx={ex['idx']}, difficulty={ex.get('difficulty','?')}):")
        print(f"    Q: {ex['question']}")
        print(f"    Gold: {ex['gold_sql']}")
        print(f"    Pred: {ex['pred_sql']}")
        if ex.get("ex_error"):
            print(f"    Error: {ex['ex_error'][:200]}")

# Detailed EM analysis for correct EX
print(f"\n{'=' * 80}")
print(f"4. CORRECT EX - EM ANALYSIS")
print(f"{'=' * 80}")
print(f"   Correct EX total: {len(correct_ex)}")
print(f"   Also correct EM:  {len(correct_em)} ({len(correct_em)/len(correct_ex)*100:.1f}%)")
print(f"   Different SQL, same result: {len(correct_ex_not_em)} ({len(correct_ex_not_em)/len(correct_ex)*100:.1f}%)")

# Show some examples of EX-correct but EM-wrong
print(f"\n   Examples of correct EX but wrong EM (different SQL, same result):")
for i, p in enumerate(correct_ex_not_em[:5]):
    print(f"\n   Example {i+1} (idx={p['idx']}):")
    print(f"     Q: {p['question']}")
    print(f"     Gold: {p['gold_sql']}")
    print(f"     Pred: {p['pred_sql']}")

# Breakdown by difficulty
print(f"\n{'=' * 80}")
print(f"5. ERROR DISTRIBUTION BY DIFFICULTY")
print(f"{'=' * 80}")

for diff in ["easy", "medium", "hard"]:
    wrong_diff = [p for p in wrong_ex if p.get("difficulty") == diff]
    print(f"\n   {diff.upper()} ({len(wrong_diff)} wrong):")
    diff_cats = Counter()
    for p in wrong_diff:
        for cat in error_classifications.get(p["idx"], []):
            diff_cats[cat] += 1
    for cat in ordered_cats:
        cnt = diff_cats.get(cat, 0)
        if cnt > 0:
            print(f"     {cat:25s}: {cnt:3d}")

# Breakdown by view
print(f"\n{'=' * 80}")
print(f"6. ERROR DISTRIBUTION BY VIEW")
print(f"{'=' * 80}")

for view in ["WP_vAcctIn", "WP_vAcctOut", "WP_vOutStock", "WP_vTransfer",
             "WP_vInventory", "WP_vProduct", "WP_vProvider"]:
    wrong_view = [p for p in wrong_ex if p.get("gold_table") == view]
    print(f"\n   {view} ({len(wrong_view)} wrong):")
    view_cats = Counter()
    for p in wrong_view:
        for cat in error_classifications.get(p["idx"], []):
            view_cats[cat] += 1
    for cat in ordered_cats:
        cnt = view_cats.get(cat, 0)
        if cnt > 0:
            print(f"     {cat:25s}: {cnt:3d}")

# List all "other" cases for manual review
print(f"\n{'=' * 80}")
print(f"7. ALL 'OTHER' CASES (for manual review)")
print(f"{'=' * 80}")

others = category_examples.get("other", [])
for p in others:
    cats = error_classifications.get(p["idx"], [])
    # Only show if the ONLY category is "other"
    if cats == ["other"]:
        print(f"\n  idx={p['idx']} (difficulty={p.get('difficulty','?')}, table={p.get('gold_table','?')})")
        print(f"    Q: {p['question']}")
        print(f"    Gold: {p['gold_sql']}")
        print(f"    Pred: {p['pred_sql']}")
        if p.get("ex_error"):
            print(f"    Error: {p['ex_error'][:200]}")
