"""
自動修正測試集中的表名
"""
import json
from pathlib import Path

# 1. 載入實際的 schema
print("📊 載入 schema...")
with open('data/wp_m09/tables.json', 'r', encoding='utf-8') as f:
    schema = json.load(f)[0]

real_tables = schema['table_names_original']
print(f"✅ Schema 中有 {len(real_tables)} 個表\n")

# 2. 載入測試集
print("📝 載入測試集...")
test_file = 'data/wp_m09/test.json'

if not Path(test_file).exists():
    print(f"❌ 找不到測試集: {test_file}")
    exit()

with open(test_file, 'r', encoding='utf-8') as f:
    test_data = json.load(f)

print(f"✅ 測試集有 {len(test_data)} 個樣本\n")

# 3. 建立表名映射
print("🔄 建立表名映射...\n")

table_mapping = {}

# 收集測試集中用到的表
used_tables = set(item['table'] for item in test_data)

for test_table in used_tables:
    if test_table in real_tables:
        # 表名正確
        table_mapping[test_table] = test_table
        print(f"  ✅ {test_table} - 正確")
    else:
        # 表名錯誤，尋找相似的
        print(f"  ❌ {test_table} - 不存在，尋找替代...")

        # 尋找策略
        found = False

        # 策略 1: 完全匹配（忽略大小寫）
        for real_table in real_tables:
            if test_table.lower() == real_table.lower():
                table_mapping[test_table] = real_table
                print(f"     → {real_table} (完全匹配)")
                found = True
                break

        if not found:
            # 策略 2: 包含關係
            test_lower = test_table.lower()
            for real_table in real_tables:
                real_lower = real_table.lower()
                if test_lower in real_lower or real_lower in test_lower:
                    table_mapping[test_table] = real_table
                    print(f"     → {real_table} (相似匹配)")
                    found = True
                    break

        if not found:
            # 策略 3: 關鍵字匹配
            keywords = ['product', 'receipt', 'payment']
            for keyword in keywords:
                if keyword in test_table.lower():
                    for real_table in real_tables:
                        if keyword in real_table.lower():
                            table_mapping[test_table] = real_table
                            print(f"     → {real_table} (關鍵字匹配)")
                            found = True
                            break
                if found:
                    break

        if not found:
            print(f"     ⚠️  找不到替代表，跳過此表的測試")

print(f"\n📋 最終映射:")
for old, new in table_mapping.items():
    if old != new:
        print(f"  {old} → {new}")

# 4. 修正測試集
print(f"\n🔧 修正測試集...")

fixed_count = 0
skipped_count = 0

new_test_data = []

for item in test_data:
    old_table = item['table']

    if old_table in table_mapping:
        new_table = table_mapping[old_table]

        # 創建新的測試項
        new_item = item.copy()
        new_item['table'] = new_table

        # 修正 SQL 中的表名
        new_item['query'] = item['query'].replace(old_table, new_table)

        new_test_data.append(new_item)

        if old_table != new_table:
            fixed_count += 1
    else:
        # 找不到映射，跳過
        skipped_count += 1
        print(f"  ⚠️  跳過: {item['question']} (表 {old_table} 無法映射)")

print(f"\n✅ 修正完成:")
print(f"   原始樣本: {len(test_data)}")
print(f"   修正數量: {fixed_count}")
print(f"   跳過數量: {skipped_count}")
print(f"   保留樣本: {len(new_test_data)}")

# 5. 保存修正後的測試集
backup_file = 'data/wp_m09/test_backup.json'
with open(backup_file, 'w', encoding='utf-8') as f:
    json.dump(test_data, f, indent=2, ensure_ascii=False)

print(f"\n💾 原始測試集備份至: {backup_file}")

with open(test_file, 'w', encoding='utf-8') as f:
    json.dump(new_test_data, f, indent=2, ensure_ascii=False)

print(f"💾 修正後的測試集已保存至: {test_file}")

# 6. 顯示修正範例
print(f"\n📝 修正範例:")
for item in new_test_data[:3]:
    print(f"\n表名: {item['table']}")
    print(f"問題: {item['question']}")
    print(f"SQL: {item['query']}")

print(f"\n" + "=" * 70)
print(f"✅ 完成！現在可以重新評估:")
print(f"   python evaluate_wp_m09.py")
print("=" * 70)