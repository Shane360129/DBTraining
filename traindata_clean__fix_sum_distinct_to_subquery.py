"""
traindata_clean__fix_sum_distinct_to_subquery.py

Fixes three issues in training data:
1. SUM(DISTINCT ...) / AVG(DISTINCT ...) -> subquery dedup pattern
2. COUNT(*) on header-detail views -> COUNT(DISTINCT xxxId) when counting orders
3. pvSn -> pvId in SELECT clause of WP_vProvider queries only

Reads:
  - data/wp_m09/train_spider_WP_M09.json
  - data/wp_m09/train_claude_en_2000.json

Writes:
  - data/wp_m09/train_spider_WP_M09_v2.json
  - data/wp_m09/train_claude_en_2000_v2.json
"""

import json
import re
import os
import copy

# ID column mapping for header-detail views
VIEW_ID_MAP = {
    'WP_vAcctIn': 'acctInId',
    'WP_vAcctOut': 'acctOutId',
    'WP_vOutStock': 'OutStkId',
    'WP_vTransfer': 'TransferId',
}

# Views that are header-detail (have isDel)
HEADER_DETAIL_VIEWS = set(VIEW_ID_MAP.keys())

# Views where COUNT(*) should NOT be changed
NON_HEADER_VIEWS = {'WP_vInventory', 'WP_vProduct', 'WP_vProvider'}


def detect_view(query):
    """Detect which view is used in the query."""
    m = re.search(r'WP_M09\.dbo\.(WP_v\w+)', query)
    if m:
        return m.group(1)
    return None


def tokenize_simple(query):
    """Simple SQL tokenizer for rebuilding query_toks."""
    tokens = []
    i = 0
    s = query.strip()
    while i < len(s):
        # Skip whitespace
        if s[i].isspace():
            i += 1
            continue
        # String literal
        if s[i] == "'":
            j = i + 1
            while j < len(s) and s[j] != "'":
                j += 1
            tokens.append(s[i:j+1])
            i = j + 1
            continue
        # N'...' string
        if s[i] == 'N' and i + 1 < len(s) and s[i+1] == "'":
            tokens.append('N')
            j = i + 2
            while j < len(s) and s[j] != "'":
                j += 1
            tokens.append(s[i+1:j+1])
            i = j + 1
            continue
        # Punctuation
        if s[i] in '(),;':
            tokens.append(s[i])
            i += 1
            continue
        # Operators
        if s[i:i+2] in ('<=', '>=', '<>'):
            tokens.append(s[i:i+2])
            i += 2
            continue
        if s[i] in '=<>':
            tokens.append(s[i])
            i += 1
            continue
        # Word/identifier
        j = i
        while j < len(s) and not s[j].isspace() and s[j] not in "(),;=<>'":
            j += 1
        tokens.append(s[j-j+i:j] if j > i else s[i])
        if j > i:
            tokens.append(s[i:j])
            i = j
        else:
            i += 1
    # Deduplicate consecutive duplicates from the N'...' logic above
    return tokens


def rebuild_query_toks(query):
    """Rebuild query_toks from query string."""
    tokens = []
    i = 0
    s = query.strip()
    while i < len(s):
        if s[i].isspace():
            i += 1
            continue
        # N'...' pattern
        if s[i] == 'N' and i + 1 < len(s) and s[i+1] == "'":
            tokens.append('N')
            j = i + 2
            while j < len(s) and s[j] != "'":
                j += 1
            tokens.append(s[i+1:j+1])
            i = j + 1
            continue
        # String literal
        if s[i] == "'":
            j = i + 1
            while j < len(s) and s[j] != "'":
                j += 1
            tokens.append(s[i:j+1])
            i = j + 1
            continue
        # Single-char punctuation
        if s[i] in '(),;*':
            tokens.append(s[i])
            i += 1
            continue
        # Two-char operators
        if i + 1 < len(s) and s[i:i+2] in ('<=', '>=', '<>'):
            tokens.append(s[i:i+2])
            i += 2
            continue
        if s[i] in '=<>':
            tokens.append(s[i])
            i += 1
            continue
        # Word/identifier (including dots for WP_M09.dbo.xxx)
        j = i
        while j < len(s) and not s[j].isspace() and s[j] not in "(),;=<>'*":
            j += 1
        if j > i:
            tokens.append(s[i:j])
        i = j
    return tokens


def rebuild_query_toks_no_value(query):
    """Rebuild query_toks_no_value - replace literal values with 'value'."""
    tokens = rebuild_query_toks(query)
    result = []
    for t in tokens:
        if t.startswith("'") and t.endswith("'"):
            result.append("'value'")
        elif t == 'N':
            # Keep N prefix, next token will be the string
            result.append(t)
        else:
            # Try to detect numeric literals after = or comparison
            result.append(t)
    return result


def extract_where_clause(query):
    """Extract WHERE clause from query (without GROUP BY/ORDER BY)."""
    # Find WHERE
    m = re.search(r'\bWHERE\b(.+?)(?:\bGROUP\s+BY\b|\bORDER\s+BY\b|\bHAVING\b|;|$)', query, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None


def extract_group_by_cols(query):
    """Extract GROUP BY columns."""
    m = re.search(r'\bGROUP\s+BY\b\s+(.+?)(?:\bHAVING\b|\bORDER\s+BY\b|;|$)', query, re.IGNORECASE)
    if m:
        cols_str = m.group(1).strip()
        # Split by comma, strip each
        return [c.strip() for c in cols_str.split(',')]
    return []


def extract_order_by(query):
    """Extract ORDER BY clause including the keywords."""
    m = re.search(r'(\bORDER\s+BY\b\s+.+?)(?:;|$)', query, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None


def extract_having(query):
    """Extract HAVING clause."""
    m = re.search(r'(\bHAVING\b\s+.+?)(?:\bORDER\s+BY\b|;|$)', query, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None


def fix_sum_distinct(query):
    """
    Fix SUM(DISTINCT col) and AVG(DISTINCT col) patterns to use subquery.
    Returns (new_query, was_modified).
    """
    view = detect_view(query)
    if not view or view not in VIEW_ID_MAP:
        return query, False

    id_col = VIEW_ID_MAP[view]

    # Check if query has SUM(DISTINCT or AVG(DISTINCT anywhere (SELECT or HAVING)
    if not re.search(r'\b(SUM|AVG)\s*\(\s*DISTINCT\b', query, re.IGNORECASE):
        return query, False

    # Parse the query structure
    from_table = f'WP_M09.dbo.{view}'

    # Get WHERE clause
    where_clause = extract_where_clause(query)
    group_by_cols = extract_group_by_cols(query)
    order_by = extract_order_by(query)
    having = extract_having(query)

    # Extract SELECT clause (handle SELECT DISTINCT ...)
    select_match = re.match(r'SELECT\s+(DISTINCT\s+)?(.+?)\s+FROM\s+', query, re.IGNORECASE)
    if not select_match:
        return query, False
    select_distinct_prefix = select_match.group(1) or ''  # 'DISTINCT ' or ''
    select_clause = select_match.group(2)

    # Find all SUM(DISTINCT col) and AVG(DISTINCT col) patterns across entire query
    agg_cols = set()
    for m in re.finditer(r'\b(?:SUM|AVG)\s*\(\s*DISTINCT\s+(\w+)\s*\)', query, re.IGNORECASE):
        agg_cols.add(m.group(1))

    # Rewrite SELECT: replace SUM(DISTINCT col) -> SUM(col), AVG(DISTINCT col) -> AVG(col)
    new_select = re.sub(
        r'\b(SUM|AVG)\s*\(\s*DISTINCT\s+(\w+)\s*\)',
        r'\1(\2)',
        select_clause,
        flags=re.IGNORECASE
    )

    # Also rewrite HAVING clause if it contains SUM(DISTINCT) / AVG(DISTINCT)
    new_having = None
    if having:
        new_having = re.sub(
            r'\b(SUM|AVG)\s*\(\s*DISTINCT\s+(\w+)\s*\)',
            r'\1(\2)',
            having,
            flags=re.IGNORECASE
        )

    # Build subquery SELECT list: DISTINCT id_col, agg_cols, group_by_cols
    subquery_select_cols = [id_col]
    # Add group by columns
    for gc in group_by_cols:
        if gc not in subquery_select_cols:
            subquery_select_cols.append(gc)
    # Also add non-aggregated columns from SELECT (e.g., pvName)
    for item in select_clause.split(','):
        item = item.strip()
        # Skip 'DISTINCT' keyword if it appears as a standalone token (from SELECT DISTINCT)
        if item.upper() == 'DISTINCT':
            continue
        # Skip aggregation expressions
        if re.search(r'\b(SUM|AVG|COUNT|MIN|MAX)\s*\(', item, re.IGNORECASE):
            continue
        # Get the column name (before AS if present)
        col_match = re.match(r'(\w[\w()\']*)', item)
        if col_match:
            col = col_match.group(1)
            # Skip if it's a function like LEFT(...)
            if '(' in item and not re.search(r'\b(SUM|AVG|COUNT|MIN|MAX)\s*\(', item, re.IGNORECASE):
                # It's a function expression like LEFT(acctInId,6), include as-is
                expr = item.split(' AS ')[0].strip() if ' AS ' in item.upper() else item.strip()
                # Remove alias
                expr_no_alias = re.sub(r'\s+AS\s+\w+', '', expr, flags=re.IGNORECASE).strip()
                if expr_no_alias not in subquery_select_cols:
                    subquery_select_cols.append(expr_no_alias)
            elif col not in subquery_select_cols:
                subquery_select_cols.append(col)
    # Add aggregated columns
    for ac in agg_cols:
        if ac not in subquery_select_cols:
            subquery_select_cols.append(ac)

    # Build subquery
    subquery = f"SELECT DISTINCT {', '.join(subquery_select_cols)} FROM {from_table}"
    if where_clause:
        subquery += f" WHERE {where_clause}"

    # Build final query - preserve SELECT DISTINCT if original had it
    new_query = f"SELECT {select_distinct_prefix}{new_select} FROM ({subquery}) sub"
    if group_by_cols:
        new_query += f" GROUP BY {', '.join(group_by_cols)}"
    if new_having:
        new_query += f" {new_having}"
    elif having:
        new_query += f" {having}"
    if order_by:
        new_query += f" {order_by}"
    new_query += ";"

    return new_query, True


def fix_count_star(query, question):
    """
    Fix COUNT(*) on header-detail views when counting orders/records.
    Returns (new_query, was_modified).
    """
    view = detect_view(query)
    if not view or view not in HEADER_DETAIL_VIEWS:
        return query, False

    if 'COUNT(*)' not in query:
        return query, False

    id_col = VIEW_ID_MAP[view]

    # Heuristic: If query has GROUP BY on detail columns (pName, etc.),
    # it's counting detail rows per group - don't change
    group_by_cols = extract_group_by_cols(query)

    # If GROUP BY includes the ID column itself (e.g., GROUP BY acctInId),
    # it's counting detail lines per order - don't change
    if id_col in group_by_cols:
        return query, False

    # If GROUP BY contains detail-level columns (pName, pCode, etc.), don't change
    detail_cols = {'pName', 'pCode', 'pNo', 'pUnit', 'qty', 'price', 'amtTotal',
                   'oStkDtlAmt', 'amtNoneTax', 'tax', 'isTax'}
    if any(c in detail_cols for c in group_by_cols):
        return query, False

    # Check question for clues about counting orders vs detail rows
    q_lower = question.lower() if question else ''

    # Keywords suggesting counting lines/details (should NOT change)
    detail_keywords = ['line', 'item', 'detail', 'product', 'row', '明細', '品項',
                       '筆明細', '幾筆', '商品', 'lines']
    # Check if alias suggests detail counting
    if re.search(r'AS\s+lines', query, re.IGNORECASE):
        return query, False

    for kw in detail_keywords:
        if kw in q_lower:
            return query, False

    # Keywords suggesting counting orders/records (should change)
    order_keywords = ['order', 'record', 'receipt', 'transaction', '單', '筆',
                      'how many', 'count', '幾張', '張', '訂單', '進貨', '出貨',
                      '調撥', 'purchase', 'shipment', 'transfer', 'invoice',
                      'total number']

    # If no GROUP BY and it's a simple COUNT(*), and question suggests order counting
    should_fix = False
    for kw in order_keywords:
        if kw in q_lower:
            should_fix = True
            break

    # Also fix if there's no GROUP BY at all and the WHERE uses isDel
    # (suggests header-level filtering = counting orders)
    if not should_fix and not group_by_cols:
        if "isDel='N'" in query or 'isDel = ' in query:
            # Simple count on header-detail with isDel filter = likely counting orders
            # But check: if WHERE has detail-level filters, probably counting details
            where = extract_where_clause(query)
            if where:
                has_detail_filter = any(dc in where for dc in ['pName', 'pCode', 'qty', 'price',
                                                                'oStkDtlAmt', 'amtTotal',
                                                                'amtNoneTax', 'isTax', 'dtlIsDel'])
                if not has_detail_filter:
                    should_fix = True

    if not should_fix:
        return query, False

    # Replace COUNT(*) with COUNT(DISTINCT id_col)
    new_query = query.replace('COUNT(*)', f'COUNT(DISTINCT {id_col})')
    return new_query, True


def fix_pvsn_to_pvid(query):
    """
    In WP_vProvider queries, replace pvSn in SELECT clause with pvId.
    Do NOT change pvSn in WHERE/JOIN clauses.
    Returns (new_query, was_modified).
    """
    view = detect_view(query)
    if view != 'WP_vProvider':
        return query, False

    # Check if pvSn is in the SELECT clause
    select_match = re.match(r'(SELECT\s+)(.*?)(\s+FROM\s+)', query, re.IGNORECASE | re.DOTALL)
    if not select_match:
        return query, False

    select_prefix = select_match.group(1)
    select_cols = select_match.group(2)
    from_part = select_match.group(3)
    rest = query[select_match.end():]

    if 'pvSn' not in select_cols:
        return query, False

    # Replace pvSn with pvId in SELECT clause only
    new_select_cols = re.sub(r'\bpvSn\b', 'pvId', select_cols)

    if new_select_cols == select_cols:
        return query, False

    new_query = select_prefix + new_select_cols + from_part + rest
    return new_query, True


def basic_sql_check(query):
    """Basic SQL structure check."""
    q = query.upper().strip()
    if not q.startswith('SELECT'):
        return False, "Does not start with SELECT"
    if 'FROM' not in q:
        return False, "Missing FROM"
    if q.count('(') != q.count(')'):
        return False, f"Unbalanced parentheses: {q.count('(')} open, {q.count(')')} close"
    return True, "OK"


def process_file(input_path, output_path):
    """Process a single training data file."""
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    stats = {
        'total': len(data),
        'sum_distinct_fixed': 0,
        'count_star_fixed': 0,
        'pvsn_fixed': 0,
        'total_modified': 0,
        'broken': 0,
        'examples': [],
    }

    for i, sample in enumerate(data):
        original_query = sample['query']
        query = original_query
        question = sample.get('question', '')
        modifications = []

        # Fix 1: SUM(DISTINCT) / AVG(DISTINCT) -> subquery
        query, modified1 = fix_sum_distinct(query)
        if modified1:
            stats['sum_distinct_fixed'] += 1
            modifications.append('SUM/AVG(DISTINCT)')

        # Fix 2: COUNT(*) -> COUNT(DISTINCT id)
        query, modified2 = fix_count_star(query, question)
        if modified2:
            stats['count_star_fixed'] += 1
            modifications.append('COUNT(*)')

        # Fix 3: pvSn -> pvId in Provider SELECT
        query, modified3 = fix_pvsn_to_pvid(query)
        if modified3:
            stats['pvsn_fixed'] += 1
            modifications.append('pvSn->pvId')

        if modifications:
            stats['total_modified'] += 1

            # Verify SQL structure
            ok, msg = basic_sql_check(query)
            if not ok:
                stats['broken'] += 1
                print(f"  WARNING: Broken SQL at index {i}: {msg}")
                print(f"    Original: {original_query}")
                print(f"    Fixed:    {query}")
                continue

            # Save example (up to 5 per fix type)
            if len(stats['examples']) < 15:
                stats['examples'].append({
                    'index': i,
                    'fixes': modifications,
                    'question': question[:80],
                    'before': original_query,
                    'after': query,
                })

            # Update sample
            sample['query'] = query
            sample['query_toks'] = rebuild_query_toks(query)
            sample['query_toks_no_value'] = rebuild_query_toks_no_value(query)

    # Write output
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return stats


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, 'data', 'wp_m09')

    files = [
        ('train_spider_WP_M09.json', 'train_spider_WP_M09_v2.json'),
        ('train_claude_en_2000.json', 'train_claude_en_2000_v2.json'),
    ]

    all_examples = []

    for input_name, output_name in files:
        input_path = os.path.join(data_dir, input_name)
        output_path = os.path.join(data_dir, output_name)

        print(f"\n{'='*70}")
        print(f"Processing: {input_name}")
        print(f"{'='*70}")

        if not os.path.exists(input_path):
            print(f"  ERROR: File not found: {input_path}")
            continue

        stats = process_file(input_path, output_path)

        print(f"  Total samples:        {stats['total']}")
        print(f"  SUM/AVG(DISTINCT):    {stats['sum_distinct_fixed']} fixed")
        print(f"  COUNT(*) -> DISTINCT: {stats['count_star_fixed']} fixed")
        print(f"  pvSn -> pvId:         {stats['pvsn_fixed']} fixed")
        print(f"  Total modified:       {stats['total_modified']}")
        print(f"  Broken SQL:           {stats['broken']}")
        print(f"  Output: {output_name}")

        all_examples.extend(stats['examples'])

    # Show examples (up to 5 per fix type)
    print(f"\n{'='*70}")
    print("BEFORE / AFTER EXAMPLES")
    print(f"{'='*70}")

    shown = {'SUM/AVG(DISTINCT)': 0, 'COUNT(*)': 0, 'pvSn->pvId': 0}
    count = 0
    for ex in all_examples:
        if count >= 15:
            break
        # Show if we haven't shown enough of this type
        fix_type = ex['fixes'][0]
        if shown.get(fix_type, 0) >= 5:
            continue
        shown[fix_type] = shown.get(fix_type, 0) + 1
        count += 1

        print(f"\n--- Example {count} [{', '.join(ex['fixes'])}] (index {ex['index']}) ---")
        print(f"  Q: {ex['question']}")
        print(f"  BEFORE: {ex['before']}")
        print(f"  AFTER:  {ex['after']}")

    # Verify output files
    print(f"\n{'='*70}")
    print("VERIFICATION")
    print(f"{'='*70}")
    for _, output_name in files:
        output_path = os.path.join(data_dir, output_name)
        if os.path.exists(output_path):
            with open(output_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"  {output_name}: {len(data)} samples (file OK)")

            # Count remaining issues
            remaining_sum_distinct = sum(1 for s in data if re.search(r'\b(SUM|AVG)\s*\(\s*DISTINCT\b', s['query'], re.IGNORECASE))
            print(f"    Remaining SUM/AVG(DISTINCT): {remaining_sum_distinct}")
        else:
            print(f"  {output_name}: NOT FOUND")


if __name__ == '__main__':
    main()
