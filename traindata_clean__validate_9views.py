#!/usr/bin/env python
# traindata_clean__validate_9views.py
# 驗證 train_9views_20k.json 中的 SQL 是否能在 DB 上執行
# --all: 驗證全部（預設抽查 500 筆）
# 輸出空結果的 SQL 到 outputs/validate_9views_empty.json

import json
import random
import pyodbc
import sys
import time
import argparse
from collections import defaultdict

DATA_PATH = r"data\wp_m09\train_9views_20k.json"

DB_CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=SHANE\SQLEXPRESS;DATABASE=WP_M09;Trusted_Connection=yes;"
)

VIEW_NAMES = ["WP_vAcctIn", "WP_vAcctOut", "WP_vOutStock", "WP_vTransfer",
              "WP_vInventory", "WP_vProduct", "WP_vProvider",
              "WP_vMemberDeposit", "WP_vPdCombine"]


def detect_view(sql):
    for vn in VIEW_NAMES:
        if vn in sql:
            return vn
    return "Unknown"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true", help="Validate all samples (default: sample 500)")
    parser.add_argument("--sample", type=int, default=500, help="Sample size when not --all")
    args = parser.parse_args()

    with open(DATA_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"Total samples: {len(data)}")

    # Classify by view
    by_view = defaultdict(list)
    for i, s in enumerate(data):
        vn = detect_view(s.get("query", ""))
        by_view[vn].append(i)

    if args.all:
        indices_to_test = list(range(len(data)))
        print(f"Mode: ALL ({len(data)} samples)\n")
    else:
        indices_to_test = []
        for vn, indices in sorted(by_view.items()):
            n = max(10, int(args.sample * len(indices) / len(data)))
            chosen = random.sample(indices, min(n, len(indices)))
            indices_to_test.extend(chosen)
            print(f"  {vn}: sampling {len(chosen)}/{len(indices)}")
        random.shuffle(indices_to_test)
        print(f"\nMode: SAMPLE ({len(indices_to_test)} samples)\n")

    # Connect to DB
    conn = pyodbc.connect(DB_CONN_STR, timeout=30)
    cursor = conn.cursor()

    ok = 0
    empty_count = 0
    empty_list = []
    errors = []
    start = time.time()

    for count, idx in enumerate(indices_to_test):
        s = data[idx]
        sql = s["query"]
        question = s["question"]

        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            if len(rows) == 0:
                empty_count += 1
                empty_list.append({
                    "idx": idx,
                    "view": detect_view(sql),
                    "question": question,
                    "query": sql,
                    "difficulty": s.get("difficulty", ""),
                    "source": s.get("source", ""),
                })
            ok += 1
        except Exception as e:
            err_msg = str(e)
            errors.append({
                "idx": idx,
                "view": detect_view(sql),
                "question": question[:100],
                "query": sql[:200],
                "error": err_msg[:200],
            })

        if (count + 1) % 1000 == 0:
            elapsed = time.time() - start
            print(f"  [{count+1}/{len(indices_to_test)}] OK={ok} Empty={empty_count} Errors={len(errors)} ({elapsed:.0f}s)")

    elapsed = time.time() - start
    conn.close()

    print(f"\n{'='*60}")
    print(f"VALIDATION RESULTS")
    print(f"{'='*60}")
    print(f"  Total tested:  {len(indices_to_test)}")
    print(f"  OK (has data): {ok - empty_count}")
    print(f"  OK (empty):    {empty_count}")
    print(f"  ERRORS:        {len(errors)}")
    print(f"  Success rate:  {ok/len(indices_to_test)*100:.1f}%")
    print(f"  Time:          {elapsed:.0f}s")

    # Per-view summary
    print(f"\n{'='*60}")
    print(f"PER-VIEW BREAKDOWN")
    print(f"{'='*60}")
    view_tested = defaultdict(int)
    view_empty = defaultdict(int)
    view_errors = defaultdict(int)
    for idx in indices_to_test:
        vn = detect_view(data[idx]["query"])
        view_tested[vn] += 1
    for e in empty_list:
        view_empty[e["view"]] += 1
    for e in errors:
        view_errors[e["view"]] += 1
    for vn in sorted(by_view.keys()):
        tested = view_tested.get(vn, 0)
        emp = view_empty.get(vn, 0)
        errs = view_errors.get(vn, 0)
        has_data = tested - emp - errs
        print(f"  {vn:<25} tested={tested:>5}  data={has_data:>5}  empty={emp:>5}  errors={errs:>3}")

    # Save empty results
    out_path = r"outputs\validate_9views_empty.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(empty_list, f, ensure_ascii=False, indent=2)
    print(f"\nEmpty results saved to: {out_path} ({len(empty_list)} items)")

    # Error details
    if errors:
        print(f"\n{'='*60}")
        print(f"ERROR DETAILS ({len(errors)} errors)")
        print(f"{'='*60}")
        error_types = defaultdict(list)
        for e in errors:
            msg = e["error"]
            if "Invalid column name" in msg:
                col = msg.split("Invalid column name")[1].strip()[:30]
                error_types[f"Invalid column: {col}"].append(e)
            elif "Invalid object name" in msg:
                obj = msg.split("Invalid object name")[1].strip()[:30]
                error_types[f"Invalid object: {obj}"].append(e)
            elif "Incorrect syntax" in msg:
                error_types["Syntax error"].append(e)
            else:
                error_types[msg[:60]].append(e)

        for etype, items in sorted(error_types.items(), key=lambda x: -len(x[1])):
            print(f"\n  [{len(items)}x] {etype}")
            for item in items[:3]:
                print(f"    Q: {item['question']}")
                print(f"    SQL: {item['query']}")
                print()

    # Save error results
    if errors:
        err_path = r"outputs\validate_9views_errors.json"
        with open(err_path, "w", encoding="utf-8") as f:
            json.dump(errors, f, ensure_ascii=False, indent=2)
        print(f"Errors saved to: {err_path} ({len(errors)} items)")


if __name__ == "__main__":
    main()
