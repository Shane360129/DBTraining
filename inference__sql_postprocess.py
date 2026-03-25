"""
inference__sql_postprocess.py
Deterministic post-processing for model-generated T-SQL queries targeting WP_M09.

Applies rules in order:
  1. LIMIT -> TOP conversion (MySQL/PostgreSQL syntax -> T-SQL)
  2. Hallucinated column repair (model-invented columns -> valid columns)
  3. SUM(DISTINCT x) / AVG(DISTINCT x) -> subquery fallback
  4. Trailing semicolons, markdown fences, explanatory text cleanup
"""

import re

# ---------------------------------------------------------------------------
# Valid columns per view
# ---------------------------------------------------------------------------
VALID_COLUMNS = {
    "WP_vAcctIn": {"sn", "acctInId", "acctInDate", "amount", "memo", "empId", "isDel", "dtlSn", "OutStkId", "outStkAmtTotal", "dtlIsDel", "memSn", "memId", "memName", "pNo", "pBarcode", "pName", "pNameS", "oStkDtlAmt", "oStkDtlQty", "oStkDtlAmtTotal", "dtlDiscnt", "dtlDiscntShare", "discount", "discountShare"},
    "WP_vAcctOut": {"sn", "acctOutId", "acctOutDate", "amount", "transAmt", "memo", "empId", "empName", "isDel", "dtlSn", "InStkId", "dtlAmt", "qty", "amtTotal", "dtlIsDel", "pNo", "pName", "pNameS", "pBarcode", "pvId", "pvName", "pvNameS", "pvSn", "pvDiscount", "inStkAmt", "inStkAmtTotal", "payType"},
    "WP_vOutStock": {"sn", "OutStkId", "OutStkDate", "amount", "tax", "amtNoneTax", "isDel", "empId", "empName", "memo", "memSn", "memId", "memName", "outType", "dtlSn", "pNo", "qty", "dtlAmt", "amtTotal", "dtlIsDel", "dtlCostAvg", "dtlCostStd", "dtlDiscnt", "dtlDiscntPer", "dtlDiscntShare", "pName", "pBarcode", "pUName", "costStd", "discount", "discountShare", "memTel", "memCityName", "memZoneName"},
    "WP_vTransfer": {"sn", "TransferId", "empId", "dtlSn", "FromWhSn", "fWhId", "fWhName", "ToWhSn", "tfWhId", "tfWhName", "TransferDate", "pNo", "qty", "pName", "pNameS", "pBarcode", "pCode", "isDel", "dtlIsDel", "costAvg"},
    "WP_vInventory": {"whSn", "WarehouseId", "WarehouseName", "pNo", "pName", "pNameS", "pBarcode", "pUnit", "pUName", "priceStd", "priceLow", "priceMem", "priceBat", "costStd", "costAvg", "isSale", "pvName", "pvNameS", "qtyNow", "pvSn", "qtySafe", "qty"},
    "WP_vProduct": {"pNo", "pName", "pNameS", "pBarcode", "pCode", "pUnit", "pUName", "priceStd", "priceLow", "priceMem", "priceBat", "isPvDiscount", "isSale", "costStd", "costAvg", "pvSn", "pvId", "pvName", "pvNameS", "qtyNow", "qtySafe", "pvDiscount"},
    "WP_vProvider": {"sn", "pvId", "pvName", "pvNameS", "pvKId", "pvBoss", "pvTel", "pvCityId", "pvZoneId", "pvCity", "pvZone", "pvAddr", "ctactName", "ctactTel", "fax", "email", "taxId", "isStop", "invoTitle", "bankId", "bankName", "bankAccount", "bankAcctName", "memo", "pvKName", "pvDiscount"},
}

# Known hallucinated column -> correct column, scoped by view
COLUMN_CORRECTIONS = {
    "WP_vAcctIn": {
        "dtlAmtTotal": "oStkDtlAmtTotal",
    },
    "WP_vTransfer": {
        "TransferQty": "qty",
        "fromWh": "fWhName",
        "toWh": "tfWhName",
    },
}

# Build a case-insensitive lookup: lowered bad col -> (view, correct col)
_CORRECTIONS_CI = {}
for _view, _mapping in COLUMN_CORRECTIONS.items():
    for _bad, _good in _mapping.items():
        _CORRECTIONS_CI[_bad.lower()] = (_view, _good)

# ID column used for DISTINCT subquery rewrite per view
_ID_COLUMN = {
    "WP_vAcctIn": "acctInId",
    "WP_vAcctOut": "acctOutId",
    "WP_vOutStock": "OutStkId",
    "WP_vTransfer": "TransferId",
}


# ---------------------------------------------------------------------------
# Rule 1: LIMIT N -> TOP N
# ---------------------------------------------------------------------------
def _rule_limit_to_top(sql: str) -> str:
    """Convert trailing LIMIT N to SELECT TOP N (T-SQL)."""
    # Match LIMIT <number> at the end (possibly followed by whitespace/semicolons)
    m = re.search(r'\bLIMIT\s+(\d+)\s*;?\s*$', sql, re.IGNORECASE)
    if not m:
        return sql

    n = m.group(1)
    # Remove the LIMIT clause
    sql_no_limit = sql[:m.start()].rstrip()

    # Insert TOP N after SELECT [DISTINCT]
    def insert_top(s, n):
        pattern = re.compile(r'^(\s*SELECT\s+)(DISTINCT\s+)?', re.IGNORECASE)
        m2 = pattern.match(s)
        if m2:
            select_part = m2.group(1)  # "SELECT "
            distinct_part = m2.group(2) or ""  # "DISTINCT " or ""
            rest = s[m2.end():]
            return f"{select_part}{distinct_part}TOP {n} {rest}"
        return s

    return insert_top(sql_no_limit, n)


# ---------------------------------------------------------------------------
# Rule 2: Hallucinated column repair
# ---------------------------------------------------------------------------
def _detect_views(sql: str) -> set:
    """Return set of view names referenced in the SQL (case-insensitive match)."""
    views = set()
    sql_upper = sql.upper()
    for view_name in VALID_COLUMNS:
        if view_name.upper() in sql_upper:
            views.add(view_name)
    return views


def _rule_fix_columns(sql: str) -> str:
    """Replace known hallucinated columns with correct ones."""
    views = _detect_views(sql)
    if not views:
        return sql

    for view in views:
        corrections = COLUMN_CORRECTIONS.get(view, {})
        for bad_col, good_col in corrections.items():
            # Replace whole-word occurrences (case-insensitive)
            pattern = re.compile(r'\b' + re.escape(bad_col) + r'\b', re.IGNORECASE)
            sql = pattern.sub(good_col, sql)

    return sql


# ---------------------------------------------------------------------------
# Rule 3: SUM(DISTINCT x) / AVG(DISTINCT x) -> subquery
# ---------------------------------------------------------------------------
def _rule_distinct_agg_to_subquery(sql: str) -> str:
    """
    Rewrite SUM(DISTINCT col) or AVG(DISTINCT col) to a subquery pattern.
    Example:
        SELECT SUM(DISTINCT amount) FROM WP_vAcctIn WHERE ...
        ->
        SELECT SUM(amount) FROM (SELECT DISTINCT acctInId, amount FROM WP_vAcctIn WHERE ...) AS sub
    """
    # Check if pattern exists
    agg_pattern = re.compile(
        r'\b(SUM|AVG)\s*\(\s*DISTINCT\s+(\w+)\s*\)',
        re.IGNORECASE
    )
    match = agg_pattern.search(sql)
    if not match:
        return sql

    views = _detect_views(sql)
    if not views:
        return sql

    # Pick the view that has an ID column for dedup
    target_view = None
    id_col = None
    for v in views:
        if v in _ID_COLUMN:
            target_view = v
            id_col = _ID_COLUMN[v]
            break

    if not target_view or not id_col:
        return sql

    agg_func = match.group(1).upper()  # SUM or AVG
    agg_col = match.group(2)

    # Extract the FROM ... WHERE ... portion (everything after the SELECT columns,
    # up to GROUP BY / ORDER BY / end)
    from_match = re.search(
        r'\bFROM\b(.+?)(?:\bGROUP\s+BY\b|\bORDER\s+BY\b|\bHAVING\b|$)',
        sql, re.IGNORECASE | re.DOTALL
    )
    if not from_match:
        return sql

    from_clause = "FROM" + from_match.group(1).strip()

    # Build the subquery
    subquery = f"SELECT DISTINCT {id_col}, {agg_col} {from_clause}"

    # Replace the aggregate with non-DISTINCT version
    new_agg = f"{agg_func}({agg_col})"

    # Rebuild: replace the original FROM...WHERE with FROM (subquery) AS sub
    # and replace the agg function
    new_sql = agg_pattern.sub(new_agg, sql, count=1)

    # Now replace the FROM clause to use subquery
    # Find the FROM <view> ... WHERE ... portion and wrap it
    view_from_pattern = re.compile(
        r'\bFROM\s+' + re.escape(target_view) + r'\b(.*?)(?=\bGROUP\s+BY\b|\bORDER\s+BY\b|\bHAVING\b|$)',
        re.IGNORECASE | re.DOTALL
    )
    vm = view_from_pattern.search(new_sql)
    if vm:
        where_clause = vm.group(1).strip()
        inner = f"SELECT DISTINCT {id_col}, {agg_col} FROM {target_view} {where_clause}"
        replacement = f"FROM ({inner}) AS sub"
        new_sql = new_sql[:vm.start()] + replacement + new_sql[vm.end():]

    return new_sql


# ---------------------------------------------------------------------------
# Rule 4: Cleanup
# ---------------------------------------------------------------------------
def _rule_cleanup(sql: str) -> str:
    """Remove markdown fences, trailing semicolons, explanatory text."""
    # Remove markdown code fences
    sql = re.sub(r'^```\w*\s*', '', sql, flags=re.MULTILINE)
    sql = re.sub(r'\s*```\s*$', '', sql, flags=re.MULTILINE)

    # Remove everything after a line that looks like explanatory text
    # (lines starting with common explanation patterns after the SQL)
    lines = sql.strip().split('\n')
    clean_lines = []
    for line in lines:
        stripped = line.strip()
        # Stop if we hit explanatory text
        if stripped and not stripped.startswith('--') and re.match(
            r'^(This|Note|Explanation|The above|Here|Where |--|#|//|\*)',
            stripped, re.IGNORECASE
        ):
            break
        clean_lines.append(line)

    sql = '\n'.join(clean_lines).strip()

    # Remove trailing semicolons
    sql = re.sub(r'\s*;\s*$', '', sql)

    return sql.strip()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def postprocess_sql(sql: str) -> str:
    """
    Apply deterministic post-processing rules to fix common model output errors.

    Rules applied in order:
      1. LIMIT N -> SELECT TOP N (T-SQL conversion)
      2. Hallucinated column repair
      3. SUM(DISTINCT x) / AVG(DISTINCT x) -> subquery fallback
      4. Trailing semicolons, markdown fences, explanatory text cleanup

    Args:
        sql: Raw SQL string from model output.

    Returns:
        Cleaned T-SQL string.
    """
    if not sql or not sql.strip():
        return sql

    sql = sql.strip()
    sql = _rule_cleanup(sql)          # Rule 4 first to strip fences/junk
    sql = _rule_limit_to_top(sql)     # Rule 1
    sql = _rule_fix_columns(sql)      # Rule 2
    sql = _rule_distinct_agg_to_subquery(sql)  # Rule 3
    sql = _rule_cleanup(sql)          # Rule 4 again for final cleanup

    return sql


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    tests = [
        # Rule 1: LIMIT -> TOP
        {
            "name": "LIMIT -> TOP basic",
            "input": "SELECT pName, qty FROM WP_vInventory LIMIT 5",
            "expected": "SELECT TOP 5 pName, qty FROM WP_vInventory",
        },
        {
            "name": "LIMIT -> TOP with DISTINCT",
            "input": "SELECT DISTINCT pName FROM WP_vProduct LIMIT 10",
            "expected": "SELECT DISTINCT TOP 10 pName FROM WP_vProduct",
        },
        {
            "name": "LIMIT -> TOP with WHERE",
            "input": "SELECT pName FROM WP_vOutStock WHERE isDel = 0 LIMIT 3",
            "expected": "SELECT TOP 3 pName FROM WP_vOutStock WHERE isDel = 0",
        },
        # Rule 2: Hallucinated column repair
        {
            "name": "dtlAmtTotal -> oStkDtlAmtTotal in WP_vAcctIn",
            "input": "SELECT SUM(dtlAmtTotal) FROM WP_vAcctIn WHERE isDel = 0",
            "expected": "SELECT SUM(oStkDtlAmtTotal) FROM WP_vAcctIn WHERE isDel = 0",
        },
        {
            "name": "TransferQty -> qty in WP_vTransfer",
            "input": "SELECT TransferQty FROM WP_vTransfer WHERE isDel = 0",
            "expected": "SELECT qty FROM WP_vTransfer WHERE isDel = 0",
        },
        {
            "name": "fromWh/toWh -> fWhName/tfWhName in WP_vTransfer",
            "input": "SELECT fromWh, toWh FROM WP_vTransfer WHERE isDel = 0",
            "expected": "SELECT fWhName, tfWhName FROM WP_vTransfer WHERE isDel = 0",
        },
        # Rule 3: SUM(DISTINCT amount) -> subquery
        {
            "name": "SUM(DISTINCT amount) subquery rewrite",
            "input": "SELECT SUM(DISTINCT amount) FROM WP_vAcctIn WHERE isDel = 0",
            "expected_contains": "SELECT DISTINCT acctInId, amount FROM WP_vAcctIn",
        },
        # Rule 4: Cleanup
        {
            "name": "Remove trailing semicolon",
            "input": "SELECT * FROM WP_vProduct;",
            "expected": "SELECT * FROM WP_vProduct",
        },
        {
            "name": "Remove markdown fences",
            "input": "```sql\nSELECT * FROM WP_vProduct\n```",
            "expected": "SELECT * FROM WP_vProduct",
        },
        {
            "name": "Remove explanatory text",
            "input": "SELECT * FROM WP_vProduct\nThis query returns all products.",
            "expected": "SELECT * FROM WP_vProduct",
        },
        # Combined rules
        {
            "name": "Combined: fence + LIMIT + semicolon",
            "input": "```sql\nSELECT pName FROM WP_vProduct LIMIT 5;\n```",
            "expected": "SELECT TOP 5 pName FROM WP_vProduct",
        },
        {
            "name": "Combined: hallucinated col + LIMIT",
            "input": "SELECT fromWh, toWh, TransferQty FROM WP_vTransfer WHERE isDel = 0 LIMIT 10",
            "expected": "SELECT TOP 10 fWhName, tfWhName, qty FROM WP_vTransfer WHERE isDel = 0",
        },
    ]

    passed = 0
    failed = 0

    for t in tests:
        result = postprocess_sql(t["input"])
        if "expected" in t:
            ok = result == t["expected"]
        elif "expected_contains" in t:
            ok = t["expected_contains"] in result
        else:
            ok = False

        status = "PASS" if ok else "FAIL"
        if ok:
            passed += 1
        else:
            failed += 1

        print(f"[{status}] {t['name']}")
        print(f"  Input:    {t['input'][:80]}{'...' if len(t['input']) > 80 else ''}")
        print(f"  Output:   {result[:80]}{'...' if len(result) > 80 else ''}")
        if not ok:
            if "expected" in t:
                print(f"  Expected: {t['expected'][:80]}")
            else:
                print(f"  Expected to contain: {t['expected_contains'][:80]}")
        print()

    print(f"Results: {passed} passed, {failed} failed, {passed + failed} total")
