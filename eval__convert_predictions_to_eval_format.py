"""
修正評估文件格式
"""
import json

# 讀取預測結果
with open('outputs/predictions/predictions.json', 'r', encoding='utf-8') as f:
    preds = json.load(f)

# 生成預測文件（SQL \t db_id）
with open('outputs/predictions/predictions_eval.txt', 'w', encoding='utf-8') as f:
    for item in preds:
        f.write(f"{item['predicted']}\t{item['db_id']}\n")

print(f"✅ 預測文件已生成: {len(preds)} 行")

# 生成 gold 文件（SQL \t db_id）
with open('data/spider/dev_gold.txt', 'w', encoding='utf-8') as f:
    for item in preds:
        f.write(f"{item['gold']}\t{item['db_id']}\n")

print(f"✅ Gold 文件已生成: {len(preds)} 行")

# 驗證格式
print("\n預測文件前 3 行:")
with open('outputs/predictions/predictions_eval.txt', 'r', encoding='utf-8') as f:
    for i, line in enumerate(f):
        if i < 3:
            parts = line.strip().split('\t')
            print(f"  {i+1}. SQL: {parts[0][:60]}... | db_id: {parts[1]}")

print("\nGold 文件前 3 行:")
with open('data/spider/dev_gold.txt', 'r', encoding='utf-8') as f:
    for i, line in enumerate(f):
        if i < 3:
            parts = line.strip().split('\t')
            print(f"  {i+1}. SQL: {parts[0][:60]}... | db_id: {parts[1]}")