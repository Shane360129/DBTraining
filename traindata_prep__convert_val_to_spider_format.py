#!/usr/bin/env python3
"""
traindata_prep__convert_val_to_spider_format.py
─────────────────────────────────────────────────────────────
將 validation_claude_en.json 轉換為完整 Spider 1.0 驗證集格式：

輸出：
  data/wp_m09/val_claude_en_spider.json  ← Spider JSON（含 difficulty）
  data/wp_m09/val_claude_en_gold.sql     ← Spider 官方 gold.sql 格式
                                            每行: SQL\tdb_id

Difficulty 分類規則（基於 SQL 特徵）：
  easy   : 只有 SELECT/FROM/WHERE，無聚合/分組/子查詢
  medium : 含 GROUP BY、HAVING、ORDER BY、或多重聚合函式
  hard   : 含子查詢、CASE WHEN、多 JOIN、BETWEEN+GROUP 複合
─────────────────────────────────────────────────────────────
"""

import json, re
from pathlib import Path
from collections import Counter

# ── 路徑 ─────────────────────────────────────────────────
BASE      = Path(__file__).parent
DATA_DIR  = BASE / "data" / "wp_m09"
IN_FILE   = DATA_DIR / "validation_claude_en.json"
OUT_JSON  = DATA_DIR / "val_claude_en_spider.json"
OUT_SQL   = DATA_DIR / "val_claude_en_gold.sql"


# ════════════════════════════════════════════════════════
#  Difficulty 分類
# ════════════════════════════════════════════════════════
def classify_difficulty(sql: str) -> str:
    """
    依 SQL 特徵判斷難度：
      hard   → 含子查詢 / CASE WHEN / 多 JOIN / BETWEEN+GROUP BY 複合
      medium → GROUP BY / HAVING / ORDER BY+聚合 / 含複數條件 AND+聚合
      easy   → 其餘
    """
    s = sql.upper()

    # ── hard 條件 ──
    if re.search(r'\bSELECT\b.*\bSELECT\b', s):         # 子查詢
        return "hard"
    if re.search(r'\bCASE\b.*\bWHEN\b', s):
        return "hard"
    if s.count('JOIN') >= 2:
        return "hard"
    if re.search(r'\bBETWEEN\b', s) and re.search(r'\bGROUP\s+BY\b', s):
        return "hard"
    if re.search(r'\bHAVING\b', s) and re.search(r'COUNT|SUM|AVG|MIN|MAX', s):
        # HAVING + aggregate = hard
        return "hard"

    # ── medium 條件 ──
    if re.search(r'\bGROUP\s+BY\b', s):
        return "medium"
    if re.search(r'\bORDER\s+BY\b', s) and re.search(r'COUNT|SUM|AVG|MIN|MAX', s):
        return "medium"
    if re.search(r'\bHAVING\b', s):
        return "medium"
    # 複數聚合函式
    agg_matches = re.findall(r'\b(COUNT|SUM|AVG|MIN|MAX)\b', s)
    if len(agg_matches) >= 2:
        return "medium"
    # 多個 AND 條件（≥ 3）
    if s.count(' AND ') >= 3:
        return "medium"
    # 含 TOP 並有聚合
    if re.search(r'\bTOP\s+\d+\b', s) and re.search(r'COUNT|SUM|AVG', s):
        return "medium"

    return "easy"


# ════════════════════════════════════════════════════════
#  View 名稱擷取
# ════════════════════════════════════════════════════════
def view_of(sql: str) -> str:
    m = re.search(r'WP_M09\.dbo\.(\w+)', sql)
    return m.group(1) if m else "unknown"


# ════════════════════════════════════════════════════════
#  主程式
# ════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("Convert validation_claude_en.json → Spider 1.0 format")
    print("=" * 60)

    with open(IN_FILE, "r", encoding="utf-8") as f:
        raw = json.load(f)
    print(f"輸入: {len(raw)} 筆")

    spider_records = []
    gold_lines     = []
    diff_cnt       = Counter()
    view_cnt       = Counter()

    for i, item in enumerate(raw):
        sql        = item.get("query", "").strip()
        question   = item.get("question", "").strip()
        db_id      = item.get("db_id", "WP_M09")
        difficulty = classify_difficulty(sql)
        view       = view_of(sql)

        diff_cnt[difficulty] += 1
        view_cnt[view]       += 1

        # ── Spider 1.0 record ──
        record = {
            "db_id":               db_id,
            "query":               sql,
            "query_toks":          item.get("query_toks", []),
            "query_toks_no_value": item.get("query_toks_no_value", []),
            "question":            question,
            "question_toks":       item.get("question_toks", question.split()),
            "sql":                 item.get("sql", {}),
            "difficulty":          difficulty,
            "view":                view,
        }
        spider_records.append(record)

        # ── gold.sql 行: SQL\tdb_id ──
        gold_lines.append(f"{sql}\t{db_id}")

    # ── 寫出 JSON ──
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(spider_records, f, ensure_ascii=False, indent=2)

    # ── 寫出 gold.sql ──
    with open(OUT_SQL, "w", encoding="utf-8") as f:
        f.write("\n".join(gold_lines) + "\n")

    # ── 摘要 ──
    print(f"\n輸出 JSON : {OUT_JSON}  ({len(spider_records)} 筆)")
    print(f"輸出 SQL  : {OUT_SQL}")

    print(f"\nDifficulty 分布:")
    for d in ["easy", "medium", "hard"]:
        n = diff_cnt[d]
        print(f"  {d:<8} {n:>4}  ({n/len(raw)*100:.1f}%)")

    print(f"\nView 分布:")
    for v in sorted(view_cnt):
        print(f"  {v:<20} {view_cnt[v]:>4}")

    print(f"\n前 3 筆範例:")
    for r in spider_records[:3]:
        print(f"  [{r['difficulty']}] [{r['view']}]")
        print(f"    Q: {r['question']}")
        print(f"    S: {r['query'][:90]}")
        print()

    print("完成！")


if __name__ == "__main__":
    import os
    os.chdir(Path(__file__).parent)
    main()
