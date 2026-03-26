# traindata_clean__fix_isSale_9views.py
# ============================================================
# 修正 train_9views_20k.json 中 isSale 的錯誤用法
#
# 錯誤：訓練集把 isSale 當成 'Y'/'N' 布林值
# 正確：isSale 是銷售狀態碼（字元型）
#   0 = 正常進銷貨
#   1 = 只停止進貨（仍可銷售）
#   2 = 只停止銷貨（仍可進貨）
#   3 = 停止進銷貨
#
# 修正規則：
#   isSale='Y' (可銷售)  → isSale IN ('0','1')  — 0,1 都能銷貨
#   isSale='N' (不可銷售) → isSale IN ('2','3')  — 2,3 都停止銷貨
#   SELECT isSale (查狀態) → 不改（直接查值讓使用者判讀）
#
# 用法:
#   python traindata_clean__fix_isSale_9views.py
#   python traindata_clean__fix_isSale_9views.py --dry-run   # 只預覽不寫入
# ============================================================

import json
import re
import argparse
import copy


INPUT_PATH  = r"data\wp_m09\train_9views_20k.json"
OUTPUT_PATH = r"data\wp_m09\train_9views_20k.json"  # 原地覆蓋


def fix_issale_in_sql(sql):
    """
    修正 SQL 中的 isSale 條件。

    isSale='Y' → isSale IN ('0','1')
    isSale='N' → isSale IN ('2','3')
    """
    # isSale='Y' or isSale = 'Y' (各種空白)
    sql = re.sub(
        r"isSale\s*=\s*'Y'",
        "isSale IN ('0','1')",
        sql
    )
    # isSale='N' or isSale = 'N'
    sql = re.sub(
        r"isSale\s*=\s*'N'",
        "isSale IN ('2','3')",
        sql
    )
    return sql


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="只預覽不寫入")
    args = parser.parse_args()

    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"載入: {INPUT_PATH} ({len(data)} 筆)")

    modified = 0
    changes = []

    for i, item in enumerate(data):
        sql = item.get("query", "")
        if "isSale" not in sql:
            continue

        # 只修正 WHERE 條件中的 isSale='Y'/'N'，不動 SELECT isSale
        if "isSale='Y'" not in sql and "isSale='N'" not in sql and \
           "isSale = 'Y'" not in sql and "isSale = 'N'" not in sql:
            continue

        new_sql = fix_issale_in_sql(sql)
        if new_sql != sql:
            changes.append({
                "index": i,
                "question": item["question"][:80],
                "old_sql": sql[:150],
                "new_sql": new_sql[:150],
            })
            item["query"] = new_sql
            modified += 1

    # 報告
    print(f"\n修正筆數: {modified}")
    print(f"未修改（SELECT isSale 查詢等）: {sum(1 for d in data if 'isSale' in d.get('query','')) - 0}")

    if changes:
        print(f"\n{'='*70}")
        print(f"修正範例（前 10 筆）:")
        print(f"{'='*70}")
        for c in changes[:10]:
            print(f"  [{c['index']}] Q: {c['question']}")
            print(f"    OLD: {c['old_sql']}")
            print(f"    NEW: {c['new_sql']}")
            print()

    # 統計修正後的 isSale 用法
    after_patterns = []
    for d in data:
        q = d.get("query", "")
        if "isSale" in q:
            matches = re.findall(r"isSale\s*(?:IN\s*\([^)]+\)|[=!<>]+\s*[^\s,)]+)", q, re.IGNORECASE)
            after_patterns.extend(matches)

    from collections import Counter
    print(f"\n修正後 isSale 使用模式:")
    for p, cnt in Counter(after_patterns).most_common():
        print(f"  {p}: {cnt}")

    if args.dry_run:
        print(f"\n[DRY RUN] 未寫入檔案")
    else:
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"\n已寫入: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
