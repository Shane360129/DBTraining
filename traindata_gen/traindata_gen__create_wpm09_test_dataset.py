"""
創建 WP_M09 大型測試集
生成 500+ 自然語言問答
"""
import json
from pathlib import Path
import random

def create_test_set():
    """創建大型測試集"""

    # 載入 schema
    print("📊 載入 schema...")
    with open('data/wp_m09/tables.json', 'r', encoding='utf-8') as f:
        schema = json.load(f)[0]

    tables = schema['table_names_original']
    columns = schema['column_names_original']
    column_types = schema['column_types']

    print(f"✅ 資料庫: {schema['db_id']}")
    print(f"✅ 表數量: {len(tables)}\n")

    # 輔助函數
    def get_table_columns(table_name):
        """獲取表的所有欄位"""
        if table_name not in tables:
            return []

        table_idx = tables.index(table_name)
        cols = []

        for col_idx, (t_idx, col_name) in enumerate(columns):
            if t_idx == table_idx:
                col_type = column_types[col_idx]
                cols.append({'name': col_name, 'type': col_type})

        return cols

    def find_columns(cols, keywords, col_type=None):
        """查找所有匹配的欄位（返回列表）"""
        matches = []
        for col in cols:
            if col_type and col['type'] != col_type:
                continue
            for keyword in keywords:
                if keyword.lower() in col['name'].lower():
                    matches.append(col['name'])
                    break
        return matches

    test_data = []

    print(f"🔄 為每個表生成測試...\n")

    # 為每個表生成測試
    for table_idx, table_name in enumerate(tables[:20]):  # 取前 20 個表
        print(f"📋 {table_idx + 1}. {table_name}")

        cols = get_table_columns(table_name)

        if not cols:
            continue

        # 推測業務名稱
        if 'product' in table_name.lower():
            business_name = "products"
            singular = "product"
        elif 'receipt' in table_name.lower():
            business_name = "receipts"
            singular = "receipt"
        elif 'payment' in table_name.lower():
            business_name = "payments"
            singular = "payment"
        elif 'customer' in table_name.lower() or 'cust' in table_name.lower():
            business_name = "customers"
            singular = "customer"
        elif 'supplier' in table_name.lower() or 'vendor' in table_name.lower():
            business_name = "suppliers"
            singular = "supplier"
        elif 'employee' in table_name.lower():
            business_name = "employees"
            singular = "employee"
        elif 'account' in table_name.lower() or 'acct' in table_name.lower():
            business_name = "accounts"
            singular = "account"
        elif 'item' in table_name.lower():
            business_name = "items"
            singular = "item"
        else:
            business_name = "records"
            singular = "record"

        # === 1. 簡單計數（10+ 變化）===
        count_questions = [
            f"How many {business_name} are there?",
            f"How many {business_name} do we have?",
            f"Count all {business_name}",
            f"What is the total number of {business_name}?",
            f"How many {business_name} in total?",
            f"Give me the count of {business_name}",
            f"Tell me how many {business_name}",
            f"What's the {business_name} count?",
            f"Total {business_name}?",
            f"Number of {business_name}?",
        ]

        for q in count_questions:
            test_data.append({
                "db_id": "WP_M09",
                "question": q,
                "query": f"SELECT COUNT(*) FROM {table_name}",
                "table": table_name,
                "difficulty": "easy",
                "query_type": "simple_count"
            })

        # === 2. SELECT *（10+ 變化）===
        list_questions = [
            f"Show all {business_name}",
            f"List all {business_name}",
            f"Display all {business_name}",
            f"Give me all {business_name}",
            f"Show me all {business_name}",
            f"Get all {business_name}",
            f"Retrieve all {business_name}",
            f"Show everything from {business_name}",
            f"List everything",
            f"Display all data",
        ]

        for q in list_questions:
            test_data.append({
                "db_id": "WP_M09",
                "question": q,
                "query": f"SELECT * FROM {table_name}",
                "table": table_name,
                "difficulty": "easy",
                "query_type": "simple_list"
            })

        # === 3. 欄位級查詢 ===

        # 名稱欄位
        name_cols = find_columns(cols, ['name'], 'text')
        for name_col in name_cols[:2]:  # 最多 2 個名稱欄位
            name_questions = [
                f"List all {name_col.lower()}",
                f"Show all {name_col.lower()}",
                f"What are the {name_col.lower()}?",
                f"Give me all {name_col.lower()}",
                f"Show me the {name_col.lower()}",
            ]

            for q in name_questions:
                test_data.append({
                    "db_id": "WP_M09",
                    "question": q,
                    "query": f"SELECT {name_col} FROM {table_name}",
                    "table": table_name,
                    "difficulty": "easy",
                    "query_type": "select_column"
                })

            # DISTINCT
            distinct_questions = [
                f"Show distinct {name_col.lower()}",
                f"What are the unique {name_col.lower()}?",
                f"List unique {name_col.lower()}",
                f"Show me distinct {name_col.lower()}",
                f"Get unique {name_col.lower()}",
            ]

            for q in distinct_questions:
                test_data.append({
                    "db_id": "WP_M09",
                    "question": q,
                    "query": f"SELECT DISTINCT {name_col} FROM {table_name}",
                    "table": table_name,
                    "difficulty": "medium",
                    "query_type": "distinct"
                })

        # 數值欄位（價格、金額、數量等）
        number_cols = [c for c in cols if c['type'] == 'number']

        for num_col_info in number_cols[:3]:  # 最多 3 個數值欄位
            num_col = num_col_info['name']

            # 判斷欄位類型
            if any(kw in num_col.lower() for kw in ['price', 'cost', 'amount', 'total', 'value']):
                field_type = "price" if 'price' in num_col.lower() else "amount"

                # 聚合查詢
                agg_questions = [
                    f"What is the average {num_col.lower()}?",
                    f"Calculate average {num_col.lower()}",
                    f"Average {num_col.lower()}?",
                    f"What's the mean {num_col.lower()}?",
                    f"Find the average {num_col.lower()}",
                ]

                for q in agg_questions:
                    test_data.append({
                        "db_id": "WP_M09",
                        "question": q,
                        "query": f"SELECT AVG({num_col}) FROM {table_name}",
                        "table": table_name,
                        "difficulty": "medium",
                        "query_type": "aggregation"
                    })

                sum_questions = [
                    f"What is the total {num_col.lower()}?",
                    f"Sum all {num_col.lower()}",
                    f"Total {num_col.lower()}?",
                    f"Calculate total {num_col.lower()}",
                    f"Add up all {num_col.lower()}",
                ]

                for q in sum_questions:
                    test_data.append({
                        "db_id": "WP_M09",
                        "question": q,
                        "query": f"SELECT SUM({num_col}) FROM {table_name}",
                        "table": table_name,
                        "difficulty": "medium",
                        "query_type": "aggregation"
                    })

                max_questions = [
                    f"What is the maximum {num_col.lower()}?",
                    f"What's the highest {num_col.lower()}?",
                    f"Find the max {num_col.lower()}",
                    f"Maximum {num_col.lower()}?",
                    f"Highest {num_col.lower()}?",
                ]

                for q in max_questions:
                    test_data.append({
                        "db_id": "WP_M09",
                        "question": q,
                        "query": f"SELECT MAX({num_col}) FROM {table_name}",
                        "table": table_name,
                        "difficulty": "medium",
                        "query_type": "aggregation"
                    })

                min_questions = [
                    f"What is the minimum {num_col.lower()}?",
                    f"What's the lowest {num_col.lower()}?",
                    f"Find the min {num_col.lower()}",
                    f"Minimum {num_col.lower()}?",
                    f"Lowest {num_col.lower()}?",
                ]

                for q in min_questions:
                    test_data.append({
                        "db_id": "WP_M09",
                        "question": q,
                        "query": f"SELECT MIN({num_col}) FROM {table_name}",
                        "table": table_name,
                        "difficulty": "medium",
                        "query_type": "aggregation"
                    })

                # 條件查詢
                conditional_questions = [
                    f"Show {business_name} with {num_col.lower()} greater than 1000",
                    f"Which {business_name} have {num_col.lower()} over 1000?",
                    f"Find {business_name} where {num_col.lower()} exceeds 1000",
                    f"{business_name.capitalize()} with {num_col.lower()} above 1000",
                ]

                for q in conditional_questions:
                    test_data.append({
                        "db_id": "WP_M09",
                        "question": q,
                        "query": f"SELECT * FROM {table_name} WHERE {num_col} > 1000",
                        "table": table_name,
                        "difficulty": "medium",
                        "query_type": "conditional"
                    })

                # 排序查詢
                order_questions = [
                    f"Show top 10 {business_name} by {num_col.lower()}",
                    f"Top 10 {business_name} sorted by {num_col.lower()}",
                    f"Show highest {num_col.lower()} {business_name}",
                    f"Sort {business_name} by {num_col.lower()} descending",
                    f"{business_name.capitalize()} with highest {num_col.lower()}",
                ]

                for q in order_questions:
                    test_data.append({
                        "db_id": "WP_M09",
                        "question": q,
                        "query": f"SELECT TOP 10 * FROM {table_name} ORDER BY {num_col} DESC",
                        "table": table_name,
                        "difficulty": "medium",
                        "query_type": "order_by"
                    })

            elif any(kw in num_col.lower() for kw in ['qty', 'quantity', 'stock', 'count']):
                # 數量欄位
                qty_questions = [
                    f"What is the total {num_col.lower()}?",
                    f"Sum of {num_col.lower()}",
                    f"Total {num_col.lower()}?",
                    f"How much {num_col.lower()}?",
                ]

                for q in qty_questions:
                    test_data.append({
                        "db_id": "WP_M09",
                        "question": q,
                        "query": f"SELECT SUM({num_col}) FROM {table_name}",
                        "table": table_name,
                        "difficulty": "medium",
                        "query_type": "aggregation"
                    })

                zero_questions = [
                    f"Show {business_name} with zero {num_col.lower()}",
                    f"Which {business_name} have no {num_col.lower()}?",
                    f"Find {business_name} where {num_col.lower()} is zero",
                    f"{business_name.capitalize()} with {num_col.lower()} = 0",
                ]

                for q in zero_questions:
                    test_data.append({
                        "db_id": "WP_M09",
                        "question": q,
                        "query": f"SELECT * FROM {table_name} WHERE {num_col} = 0",
                        "table": table_name,
                        "difficulty": "medium",
                        "query_type": "conditional"
                    })

                count_conditional = [
                    f"How many {business_name} have zero {num_col.lower()}?",
                    f"Count {business_name} with no {num_col.lower()}",
                    f"Number of {business_name} where {num_col.lower()} is 0",
                ]

                for q in count_conditional:
                    test_data.append({
                        "db_id": "WP_M09",
                        "question": q,
                        "query": f"SELECT COUNT(*) FROM {table_name} WHERE {num_col} = 0",
                        "table": table_name,
                        "difficulty": "medium",
                        "query_type": "count_with_condition"
                    })

        # 時間欄位
        time_cols = [c for c in cols if c['type'] == 'time']

        for time_col_info in time_cols[:2]:
            time_col = time_col_info['name']

            recent_questions = [
                f"Show recent {business_name}",
                f"What are the latest {business_name}?",
                f"Most recent {business_name}",
                f"Show newest {business_name}",
                f"Latest {business_name}",
            ]

            for q in recent_questions:
                test_data.append({
                    "db_id": "WP_M09",
                    "question": q,
                    "query": f"SELECT TOP 10 * FROM {table_name} ORDER BY {time_col} DESC",
                    "table": table_name,
                    "difficulty": "medium",
                    "query_type": "order_by"
                })

            today_questions = [
                f"Show {business_name} from today",
                f"Today's {business_name}",
                f"What {business_name} were created today?",
                f"{business_name.capitalize()} added today",
            ]

            for q in today_questions:
                test_data.append({
                    "db_id": "WP_M09",
                    "question": q,
                    "query": f"SELECT * FROM {table_name} WHERE CAST({time_col} AS DATE) = CAST(GETDATE() AS DATE)",
                    "table": table_name,
                    "difficulty": "medium",
                    "query_type": "conditional"
                })

        # 布林欄位
        bool_cols = [c for c in cols if c['name'].startswith('is') and c['type'] == 'text']

        for bool_col_info in bool_cols[:3]:
            bool_col = bool_col_info['name']

            bool_questions = [
                f"Show {business_name} where {bool_col} is true",
                f"Which {business_name} have {bool_col}?",
                f"Find {business_name} with {bool_col} = 1",
                f"{business_name.capitalize()} that are {bool_col.lower()}",
            ]

            for q in bool_questions:
                test_data.append({
                    "db_id": "WP_M09",
                    "question": q,
                    "query": f"SELECT * FROM {table_name} WHERE {bool_col} = 1",
                    "table": table_name,
                    "difficulty": "medium",
                    "query_type": "conditional"
                })

        # GROUP BY（如果有合適的欄位）
        text_cols = [c for c in cols if c['type'] == 'text' and not c['name'].startswith('is')]

        if text_cols and number_cols:
            group_col = text_cols[0]['name']

            group_questions = [
                f"Count {business_name} by {group_col.lower()}",
                f"Group {business_name} by {group_col.lower()}",
                f"How many {business_name} per {group_col.lower()}?",
            ]

            for q in group_questions:
                test_data.append({
                    "db_id": "WP_M09",
                    "question": q,
                    "query": f"SELECT {group_col}, COUNT(*) FROM {table_name} GROUP BY {group_col}",
                    "table": table_name,
                    "difficulty": "hard",
                    "query_type": "group_by"
                })

            # GROUP BY with AGG
            if number_cols:
                agg_col = number_cols[0]['name']

                group_agg_questions = [
                    f"Average {agg_col.lower()} by {group_col.lower()}",
                    f"What is the average {agg_col.lower()} per {group_col.lower()}?",
                ]

                for q in group_agg_questions:
                    test_data.append({
                        "db_id": "WP_M09",
                        "question": q,
                        "query": f"SELECT {group_col}, AVG({agg_col}) FROM {table_name} GROUP BY {group_col}",
                        "table": table_name,
                        "difficulty": "hard",
                        "query_type": "group_by_agg"
                    })

        current_total = len(test_data)
        print(f"   生成 {current_total} 個測試")

        # 如果已經超過 500，可以停止
        if current_total > 500:
            print(f"\n✅ 已達到 500+ 測試，停止生成")
            break

    # 保存
    output_file = "data/wp_m09/test.json"
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(test_data, f, indent=2, ensure_ascii=False)

    # 統計
    print(f"\n{'='*70}")
    print(f"✅ 測試集已創建: {output_file}")
    print(f"{'='*70}")
    print(f"\n📊 統計:")
    print(f"   總樣本數: {len(test_data)}")

    # 按表統計
    by_table = {}
    for item in test_data:
        table = item['table']
        by_table[table] = by_table.get(table, 0) + 1

    print(f"\n按表分布（前 10）:")
    for table, count in sorted(by_table.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"   {table}: {count}")

    # 按難度統計
    by_difficulty = {}
    for item in test_data:
        diff = item['difficulty']
        by_difficulty[diff] = by_difficulty.get(diff, 0) + 1

    print(f"\n按難度分布:")
    for diff in ['easy', 'medium', 'hard']:
        if diff in by_difficulty:
            print(f"   {diff}: {by_difficulty[diff]}")

    # 按查詢類型統計
    by_type = {}
    for item in test_data:
        qtype = item['query_type']
        by_type[qtype] = by_type.get(qtype, 0) + 1

    print(f"\n按查詢類型分布:")
    for qtype, count in sorted(by_type.items()):
        print(f"   {qtype}: {count}")

    return test_data

if __name__ == "__main__":
    print("="*70)
    print("🚀 創建 WP_M09 大型測試集（500+ 題）")
    print("="*70)
    print()

    test_data = create_test_set()

    print("\n" + "="*70)
    print(f"🎉 完成！共生成 {len(test_data)} 個測試")
    print("="*70)
    print(f"\n📝 下一步:")
    print(f"   python evaluate_wp_m09.py")