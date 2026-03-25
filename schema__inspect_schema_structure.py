"""
檢查 schema 中的表名
"""
import json

with open('data/wp_m09/tables.json', 'r', encoding='utf-8') as f:
    schema = json.load(f)[0]

tables = schema['table_names_original']

print(f"📋 資料庫中的表 ({len(tables)} 個):\n")

# 查找包含 Product 的表
product_tables = [t for t in tables if 'product' in t.lower()]
print(f"包含 'Product' 的表:")
for t in product_tables:
    print(f"  - {t}")

# 查找包含 Receipt 的表
receipt_tables = [t for t in tables if 'receipt' in t.lower()]
print(f"\n包含 'Receipt' 的表:")
for t in receipt_tables:
    print(f"  - {t}")

# 查找包含 Payment 的表
payment_tables = [t for t in tables if 'payment' in t.lower()]
print(f"\n包含 'Payment' 的表:")
for t in payment_tables:
    print(f"  - {t}")

# 檢查是否有 WP_Product
if 'WP_Product' in tables:
    print(f"\n✅ WP_Product 存在")
else:
    print(f"\n❌ WP_Product 不存在")
    print(f"\n💡 可能的替代表:")
    for t in tables[:20]:
        print(f"  - {t}")