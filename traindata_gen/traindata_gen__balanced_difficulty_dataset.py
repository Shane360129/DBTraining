"""
創建平衡的訓練資料
避免過度針對性導致災難性遺忘
"""
import json
from pathlib import Path
import random


def create_balanced_training():
    """創建平衡的訓練資料"""

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

    print(f"📊 生成平衡的訓練資料...\n")

    # 為每個表生成 - 平衡分配
    for table_name in tables[:15]:
        print(f"📋 {table_name}")

        cols = get_table_columns(table_name)
        schema_desc = build_schema(table_name)

        # 每種類型限制數量，避免過度訓練

        # === 1. COUNT（5 個變化）===
        count_qs = [
            "How many records?",
            "Count all",
            "Total count",
            "How many?",
            "Number of records",
        ]

        for q in count_qs:
            training_data.append({
                "schema": schema_desc,
                "question": q,
                "query": f"SELECT COUNT(*) FROM {table_name}",
                "db_id": "WP_M09",
                "source": "balanced"
            })

        # === 2. SELECT *（5 個變化）===
        list_qs = [
            "Show all",
            "List all",
            "Display all",
            "Get all",
            "Show everything",
        ]

        for q in list_qs:
            training_data.append({
                "schema": schema_desc,
                "question": q,
                "query": f"SELECT * FROM {table_name}",
                "db_id": "WP_M09",
                "source": "balanced"
            })

        # === 3. SELECT column（每個欄位 2 個變化）===
        for col in cols[:5]:  # 只取前 5 個欄位
            col_name = col['name']

            col_qs = [
                f"Show {col_name}",
                f"List {col_name}",
            ]

            for q in col_qs:
                training_data.append({
                    "schema": schema_desc,
                    "question": q,
                    "query": f"SELECT {col_name} FROM {table_name}",
                    "db_id": "WP_M09",
                    "source": "balanced"
                })

        # === 4. DISTINCT（每個文字欄位 2 個）===
        text_cols = [c for c in cols if c['type'] == 'text']
        for col in text_cols[:3]:
            col_name = col['name']

            dist_qs = [
                f"Unique {col_name}",
                f"Distinct {col_name}",
            ]

            for q in dist_qs:
                training_data.append({
                    "schema": schema_desc,
                    "question": q,
                    "query": f"SELECT DISTINCT {col_name} FROM {table_name}",
                    "db_id": "WP_M09",
                    "source": "balanced"
                })

        # === 5. 聚合（每個數值欄位 2 個 AVG + 2 個 SUM）===
        number_cols = [c for c in cols if c['type'] == 'number']
        for col in number_cols[:2]:
            col_name = col['name']

            # AVG
            training_data.extend([
                {
                    "schema": schema_desc,
                    "question": f"Average {col_name}",
                    "query": f"SELECT AVG({col_name}) FROM {table_name}",
                    "db_id": "WP_M09",
                    "source": "balanced"
                },
                {
                    "schema": schema_desc,
                    "question": f"Mean {col_name}",
                    "query": f"SELECT AVG({col_name}) FROM {table_name}",
                    "db_id": "WP_M09",
                    "source": "balanced"
                },
            ])

            # SUM
            training_data.extend([
                {
                    "schema": schema_desc,
                    "question": f"Total {col_name}",
                    "query": f"SELECT SUM({col_name}) FROM {table_name}",
                    "db_id": "WP_M09",
                    "source": "balanced"
                },
                {
                    "schema": schema_desc,
                    "question": f"Sum {col_name}",
                    "query": f"SELECT SUM({col_name}) FROM {table_name}",
                    "db_id": "WP_M09",
                    "source": "balanced"
                },
            ])

            # MAX/MIN
            training_data.extend([
                {
                    "schema": schema_desc,
                    "question": f"Maximum {col_name}",
                    "query": f"SELECT MAX({col_name}) FROM {table_name}",
                    "db_id": "WP_M09",
                    "source": "balanced"
                },
                {
                    "schema": schema_desc,
                    "question": f"Minimum {col_name}",
                    "query": f"SELECT MIN({col_name}) FROM {table_name}",
                    "db_id": "WP_M09",
                    "source": "balanced"
                },
            ])

        # === 6. 條件查詢（2 個簡單條件）===
        if number_cols:
            col_name = number_cols[0]['name']
            training_data.extend([
                {
                    "schema": schema_desc,
                    "question": f"Where {col_name} greater than 100",
                    "query": f"SELECT * FROM {table_name} WHERE {col_name} > 100",
                    "db_id": "WP_M09",
                    "source": "balanced"
                },
                {
                    "schema": schema_desc,
                    "question": f"Where {col_name} equals zero",
                    "query": f"SELECT * FROM {table_name} WHERE {col_name} = 0",
                    "db_id": "WP_M09",
                    "source": "balanced"
                },
            ])

        # === 7. ORDER BY（2 個）===
        if number_cols:
            col_name = number_cols[0]['name']
            training_data.extend([
                {
                    "schema": schema_desc,
                    "question": f"Top 10 by {col_name}",
                    "query": f"SELECT TOP 10 * FROM {table_name} ORDER BY {col_name} DESC",
                    "db_id": "WP_M09",
                    "source": "balanced"
                },
                {
                    "schema": schema_desc,
                    "question": f"Sort by {col_name} descending",
                    "query": f"SELECT * FROM {table_name} ORDER BY {col_name} DESC",
                    "db_id": "WP_M09",
                    "source": "balanced"
                },
            ])

        # === 8. GROUP BY（1-2 個）===
        if text_cols and number_cols:
            group_col = text_cols[0]['name']
            agg_col = number_cols[0]['name']

            training_data.extend([
                {
                    "schema": schema_desc,
                    "question": f"Average {agg_col} by {group_col}",
                    "query": f"SELECT {group_col}, AVG({agg_col}) FROM {table_name} GROUP BY {group_col}",
                    "db_id": "WP_M09",
                    "source": "balanced"
                },
                {
                    "schema": schema_desc,
                    "question": f"Count by {group_col}",
                    "query": f"SELECT {group_col}, COUNT(*) FROM {table_name} GROUP BY {group_col}",
                    "db_id": "WP_M09",
                    "source": "balanced"
                },
            ])

    # 隨機打亂
    random.shuffle(training_data)

    # 保存
    output_file = "data/wp_m09/train_final.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(training_data, f, indent=2, ensure_ascii=False)

    print(f"\n✅ 生成 {len(training_data)} 個平衡訓練樣本")
    print(f"💾 保存至: {output_file}")

    # 統計
    query_types = {}
    for item in training_data:
        query = item['query'].upper()

        if 'GROUP BY' in query:
            qtype = 'group_by'
        elif 'ORDER BY' in query:
            qtype = 'order_by'
        elif 'DISTINCT' in query:
            qtype = 'distinct'
        elif 'WHERE' in query:
            if 'COUNT' in query:
                qtype = 'conditional_count'
            else:
                qtype = 'conditional'
        elif 'COUNT(*)' in query:
            qtype = 'simple_count'
        elif 'SELECT *' in query:
            qtype = 'simple_list'
        elif 'AVG' in query or 'SUM' in query or 'MAX' in query or 'MIN' in query:
            qtype = 'aggregation'
        elif 'SELECT' in query:
            qtype = 'select_column'
        else:
            qtype = 'other'

        query_types[qtype] = query_types.get(qtype, 0) + 1

    print(f"\n查詢類型分布:")
    for qtype, count in sorted(query_types.items(), key=lambda x: x[1], reverse=True):
        print(f"   {qtype}: {count}")

    return training_data


if __name__ == "__main__":
    random.seed(42)
    create_balanced_training()