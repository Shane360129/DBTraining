"""
檢查實際的表名稱
"""
import json

with open('data/wp_m09/tables.json', 'r', encoding='utf-8') as f:
    schema = json.load(f)[0]

tables = schema['table_names_original']

print(f"📋 WP_M09 資料庫實際的表名稱:\n")
print(f"總共 {len(tables)} 個表\n")

# 分類顯示
categories = {
    'Product': [],
    'Receipt': [],
    'Payment': [],
    'Acct': [],
    'Other': []
}

for table in tables:
    if 'product' in table.lower():
        categories['Product'].append(table)
    elif 'receipt' in table.lower():
        categories['Receipt'].append(table)
    elif 'payment' in table.lower():
        categories['Payment'].append(table)
    elif 'acct' in table.lower():
        categories['Acct'].append(table)
    else:
        categories['Other'].append(table)

for cat, table_list in categories.items():
    if table_list:
        print(f"{cat} 相關:")
        for t in table_list:
            print(f"  - {t}")
        print()

# 檢查測試集中使用的表是否存在
test_tables = ['WP_Product', 'WP_vReceipt', 'WP_vPayment', 'WP_vProduct']

print(f"❓ 檢查測試集中的表是否存在:")
for test_table in test_tables:
    if test_table in tables:
        print(f"  ✅ {test_table} - 存在")
    else:
        print(f"  ❌ {test_table} - 不存在")
        # 尋找相似的表
        similar = [t for t in tables if test_table.lower() in t.lower() or t.lower() in test_table.lower()]
        if similar:
            print(f"     建議使用: {similar[0]}")