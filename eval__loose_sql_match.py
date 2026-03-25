import json, re

def normalize(sql):
    sql = sql.upper().strip().rstrip(';')
    sql = re.sub(r'\s+', ' ', sql)
    # LEFT(col,N)='XXXX' 視同 col LIKE 'XXXX%'
    sql = re.sub(
        r"LEFT\((\w+),\s*\d+\)\s*=\s*'(\w+)'",
        lambda m: f"{m.group(1)} LIKE '{m.group(2)}%'",
        sql
    )
    sql = re.sub(r'WHERE 1=1 AND ', 'WHERE ', sql)
    return sql

# 自動找最新的評估報告（過濾掉 detail/uploaded 舊檔）
import glob, os
reports = sorted(glob.glob('outputs/evaluation_*.json'))
reports = [r for r in reports if 'detail' not in r and 'uploaded' not in r]
if not reports:
    print("找不到評估報告")
    exit()

path = r'outputs\evaluation_0310_dora.json'
print(f"讀取報告: {path}\n")

d = json.load(open(path, encoding='utf-8'))
details = d['details']
total   = len(details)

strict = sum(1 for r in details if r['exact_match'])
loose  = sum(1 for r in details if normalize(r['pred_sql']) == normalize(r['gold_sql']))

print(f"Strict EM : {strict}/{total}  ({strict/total*100:.1f}%)")
print(f"Loose  EM : {loose}/{total}   ({loose/total*100:.1f}%)")
print(f"Loose gain: +{loose-strict} 筆  (+{(loose-strict)/total*100:.1f}%)")

# 按表顯示 loose
by_table = {}
for r in details:
    t = r['table']
    by_table.setdefault(t, {'strict':0,'loose':0,'total':0})
    by_table[t]['total'] += 1
    if r['exact_match']:
        by_table[t]['strict'] += 1
    if normalize(r['pred_sql']) == normalize(r['gold_sql']):
        by_table[t]['loose'] += 1

print(f"\n{'表':<30} {'Strict':>8} {'Loose':>8} {'Gain':>6}")
print("-" * 55)
for t, s in sorted(by_table.items(), key=lambda x: -x[1]['total']):
    gain = s['loose'] - s['strict']
    print(f"{t:<30} {s['strict']:>3}/{s['total']:<4}  {s['loose']:>3}/{s['total']:<4}  +{gain}")