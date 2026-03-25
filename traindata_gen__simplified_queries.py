"""
改進訓練資料生成
重點：簡化查詢，避免過度複雜的 WHERE 條件
"""
import json
from pathlib import Path


def generate_improved_training_data():
    """生成改進的訓練資料"""

    # 載入 schema
    with open('data/wp_m09/tables.json', 'r', encoding='utf-8') as f:
        schema = json.load(f)[0]

    tables = schema['table_names_original']
    columns = schema['column_names_original']
    column_types = schema['column_types']

    training_data = []

    def get_table_columns(table_name):
        """獲取表的欄位"""
        if table_name not in tables:
            return []

        table_idx = tables.index(table_name)
        cols = []

        for col_idx, (t_idx, col_name) in enumerate(columns):
            if t_idx == table_idx:
                col_type = column_types[col_idx]
                cols.append({'name': col_name, 'type': col_type})

        return cols

    def build_schema(table_name):
        """構建 schema"""
        if table_name not in tables:
            return ""

        table_idx = tables.index(table_name)
        table_cols = []

        for col_idx, (t_idx, col_name) in enumerate(columns):
            if t_idx == table_idx:
                col_type = column_types[col_idx]
                table_cols.append(f"{col_name} ({col_type})")

        if len(table_cols) <= 15:
            cols_str = ', '.join(table_cols)
        else:
            cols_str = ', '.join(table_cols[:15]) + f', ... ({len(table_cols) - 15} more)'

        return f"Table {table_name}: {cols_str}"

    print(f"🔄 為每個表生成改進的訓練資料...\n")

    # 為每個表生成訓練資料
    for table_name in tables[:15]:  # 前 15 個表
        print(f"📋 {table_name}")

        cols = get_table_columns(table_name)
        schema_desc = build_schema(table_name)

        # === 1. 簡單計數（強調無條件）===
        simple_count_variations = [
            "How many records are there?",
            "Count all records",
            "What is the total count?",
            "Total number of records",
            "How many items?",
        ]

        for q in simple_count_variations:
            training_data.append({
                "schema": schema_desc,
                "question": q,
                "query": f"SELECT COUNT(*) FROM {table_name}",
                "db_id": "WP_M09",
                "source": "improved"
            })

        # === 2. SELECT *（強調無條件）===
        select_all_variations = [
            "Show all records",
            "List everything",
            "Display all",
            "Get all data",
            "Show me all",
        ]

        for q in select_all_variations:
            training_data.append({
                "schema": schema_desc,
                "question": q,
                "query": f"SELECT * FROM {table_name}",
                "db_id": "WP_M09",
                "source": "improved"
            })

        # === 3. 選擇特定欄位（無條件）===
        for col in cols[:5]:  # 前 5 個欄位
            col_name = col['name']

            column_select_variations = [
                f"Show all {col_name}",
                f"List {col_name}",
                f"Get {col_name}",
                f"Display {col_name}",
            ]

            for q in column_select_variations:
                training_data.append({
                    "schema": schema_desc,
                    "question": q,
                    "query": f"SELECT {col_name} FROM {table_name}",
                    "db_id": "WP_M09",
                    "source": "improved"
                })

        # === 4. DISTINCT（無條件）===
        text_cols = [c for c in cols if c['type'] == 'text']
        for col in text_cols[:3]:
            col_name = col['name']

            distinct_variations = [
                f"Show distinct {col_name}",
                f"Unique {col_name}",
                f"What are the different {col_name}?",
                f"List unique {col_name}",
            ]

            for q in distinct_variations:
                training_data.append({
                    "schema": schema_desc,
                    "question": q,
                    "query": f"SELECT DISTINCT {col_name} FROM {table_name}",
                    "db_id": "WP_M09",
                    "source": "improved"
                })

        # === 5. 聚合（無條件）===
        number_cols = [c for c in cols if c['type'] == 'number']
        for col in number_cols[:2]:
            col_name = col['name']

            # AVG
            training_data.extend([
                {
                    "schema": schema_desc,
                    "question": f"What is the average {col_name}?",
                    "query": f"SELECT AVG({col_name}) FROM {table_name}",
                    "db_id": "WP_M09",
                    "source": "improved"
                },
                {
                    "schema": schema_desc,
                    "question": f"Average {col_name}",
                    "query": f"SELECT AVG({col_name}) FROM {table_name}",
                    "db_id": "WP_M09",
                    "source": "improved"
                },
            ])

            # SUM
            training_data.extend([
                {
                    "schema": schema_desc,
                    "question": f"Total {col_name}",
                    "query": f"SELECT SUM({col_name}) FROM {table_name}",
                    "db_id": "WP_M09",
                    "source": "improved"
                },
                {
                    "schema": schema_desc,
                    "question": f"Sum of {col_name}",
                    "query": f"SELECT SUM({col_name}) FROM {table_name}",
                    "db_id": "WP_M09",
                    "source": "improved"
                },
            ])

        # === 6. 簡單條件（單一條件）===
        # isDel 欄位
        if any(c['name'] == 'isDel' for c in cols):
            training_data.extend([
                {
                    "schema": schema_desc,
                    "question": "Show active records",
                    "query": f"SELECT * FROM {table_name} WHERE isDel = 'N'",
                    "db_id": "WP_M09",
                    "source": "improved"
                },
                {
                    "schema": schema_desc,
                    "question": "Count active records",
                    "query": f"SELECT COUNT(*) FROM {table_name} WHERE isDel = 'N'",
                    "db_id": "WP_M09",
                    "source": "improved"
                },
            ])

        # 數值條件（單一簡單條件）
        for col in number_cols[:2]:
            col_name = col['name']

            training_data.extend([
                {
                    "schema": schema_desc,
                    "question": f"Show records where {col_name} is greater than 100",
                    "query": f"SELECT * FROM {table_name} WHERE {col_name} > 100",
                    "db_id": "WP_M09",
                    "source": "improved"
                },
                {
                    "schema": schema_desc,
                    "question": f"Count where {col_name} equals zero",
                    "query": f"SELECT COUNT(*) FROM {table_name} WHERE {col_name} = 0",
                    "db_id": "WP_M09",
                    "source": "improved"
                },
            ])

        # === 7. ORDER BY（無條件）===
        for col in number_cols[:2]:
            col_name = col['name']

            training_data.extend([
                {
                    "schema": schema_desc,
                    "question": f"Show top 10 by {col_name}",
                    "query": f"SELECT TOP 10 * FROM {table_name} ORDER BY {col_name} DESC",
                    "db_id": "WP_M09",
                    "source": "improved"
                },
                {
                    "schema": schema_desc,
                    "question": f"Top 5 sorted by {col_name}",
                    "query": f"SELECT TOP 5 * FROM {table_name} ORDER BY {col_name} DESC",
                    "db_id": "WP_M09",
                    "source": "improved"
                },
            ])

    # 保存
    output_file = "data/wp_m09/train_improved.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(training_data, f, indent=2, ensure_ascii=False)

    print(f"\n✅ 生成 {len(training_data)} 個改進的訓練樣本")
    print(f"💾 保存至: {output_file}")

    # 統計
    by_type = {}
    for item in training_data:
        query = item['query'].upper()
        if 'COUNT(*)' in query and 'WHERE' not in query:
            qtype = 'simple_count'
        elif 'SELECT *' in query and 'WHERE' not in query and 'ORDER BY' not in query:
            qtype = 'simple_list'
        elif 'SELECT DISTINCT' in query:
            qtype = 'distinct'
        elif 'SELECT' in query and 'FROM' in query and 'WHERE' not in query and 'GROUP BY' not in query:
            if 'AVG' in query or 'SUM' in query or 'MAX' in query or 'MIN' in query:
                qtype = 'aggregation'
            else:
                qtype = 'select_column'
        else:
            qtype = 'other'

        by_type[qtype] = by_type.get(qtype, 0) + 1

    print(f"\n按類型統計:")
    for qtype, count in sorted(by_type.items()):
        print(f"   {qtype}: {count}")

    return training_data


if __name__ == "__main__":
    generate_improved_training_data()