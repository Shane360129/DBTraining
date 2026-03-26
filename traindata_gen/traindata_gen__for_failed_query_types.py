"""
針對失敗類型生成大量訓練資料
重點：simple_list, select_column, group_by_agg
"""
import json
from pathlib import Path


def generate_targeted_training():
    """生成針對性訓練資料"""

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

    print(f"🎯 生成針對性訓練資料...\n")

    # 為每個表生成
    for table_name in tables[:20]:
        print(f"📋 {table_name}")

        cols = get_table_columns(table_name)
        schema_desc = build_schema(table_name)

        # ========== 1. SELECT * 大量變化（目標：60 個正確案例）==========
        select_all_questions = [
            "Show all",
            "Show all records",
            "Show all data",
            "List all",
            "List all records",
            "List all data",
            "List everything",
            "Display all",
            "Display all records",
            "Display all data",
            "Get all",
            "Get all records",
            "Get all data",
            "Retrieve all",
            "Retrieve all records",
            "Show everything",
            "Show me all",
            "Show me all records",
            "Show me everything",
            "Give me all",
            "Give me all records",
            "Give me everything",
            "Fetch all",
            "Fetch all records",
            "Pull all data",
        ]

        for q in select_all_questions:
            training_data.append({
                "schema": schema_desc,
                "question": q,
                "query": f"SELECT * FROM {table_name}",
                "db_id": "WP_M09",
                "source": "targeted_simple_list"
            })

        # ========== 2. SELECT 特定欄位大量變化（目標：每個欄位 10 個）==========
        for col in cols:
            col_name = col['name']

            select_column_questions = [
                f"Show {col_name}",
                f"Show all {col_name}",
                f"List {col_name}",
                f"List all {col_name}",
                f"Display {col_name}",
                f"Display all {col_name}",
                f"Get {col_name}",
                f"Get all {col_name}",
                f"Retrieve {col_name}",
                f"Show me {col_name}",
                f"Show me all {col_name}",
                f"Give me {col_name}",
                f"Fetch {col_name}",
                f"What are the {col_name}?",
                f"I want to see {col_name}",
            ]

            for q in select_column_questions:
                training_data.append({
                    "schema": schema_desc,
                    "question": q,
                    "query": f"SELECT {col_name} FROM {table_name}",
                    "db_id": "WP_M09",
                    "source": "targeted_select_column"
                })

        # ========== 3. GROUP BY + AGG 大量變化 ==========
        text_cols = [c for c in cols if c['type'] == 'text' and not c['name'].lower().startswith('is')]
        number_cols = [c for c in cols if c['type'] == 'number']

        if text_cols and number_cols:
            for group_col in text_cols[:3]:
                for agg_col in number_cols[:2]:
                    group_name = group_col['name']
                    agg_name = agg_col['name']

                    group_by_questions = [
                        f"Average {agg_name} by {group_name}",
                        f"What is the average {agg_name} by {group_name}?",
                        f"Calculate average {agg_name} for each {group_name}",
                        f"Show average {agg_name} grouped by {group_name}",
                        f"Average {agg_name} per {group_name}",
                        f"Mean {agg_name} by {group_name}",
                        f"Group {agg_name} by {group_name} and average",
                    ]

                    for q in group_by_questions:
                        training_data.append({
                            "schema": schema_desc,
                            "question": q,
                            "query": f"SELECT {group_name}, AVG({agg_name}) FROM {table_name} GROUP BY {group_name}",
                            "db_id": "WP_M09",
                            "source": "targeted_group_by_agg"
                        })

                    # SUM variant
                    sum_questions = [
                        f"Total {agg_name} by {group_name}",
                        f"Sum {agg_name} by {group_name}",
                        f"Sum of {agg_name} per {group_name}",
                    ]

                    for q in sum_questions:
                        training_data.append({
                            "schema": schema_desc,
                            "question": q,
                            "query": f"SELECT {group_name}, SUM({agg_name}) FROM {table_name} GROUP BY {group_name}",
                            "db_id": "WP_M09",
                            "source": "targeted_group_by_agg"
                        })

    # 保存
    output_file = "data/wp_m09/train_targeted.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(training_data, f, indent=2, ensure_ascii=False)

    print(f"\n✅ 生成 {len(training_data)} 個針對性訓練樣本")
    print(f"💾 保存至: {output_file}")

    # 統計
    by_source = {}
    for item in training_data:
        source = item['source']
        by_source[source] = by_source.get(source, 0) + 1

    print(f"\n按類型統計:")
    for source, count in sorted(by_source.items()):
        print(f"   {source}: {count}")

    return training_data


if __name__ == "__main__":
    generate_targeted_training()