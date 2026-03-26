"""
從 Excel schema 生成自然語言訓練資料（優化版）
使用真正的自然語言問句，不直接暴露資料庫名稱和欄位名
"""
import json
from pathlib import Path

def build_table_schema_description(schema, table_name):
    """為特定表構建 schema 描述"""
    tables = schema['table_names_original']
    columns = schema['column_names_original']
    column_types = schema['column_types']

    if table_name not in tables:
        return f"Table {table_name}"

    table_idx = tables.index(table_name)

    # 收集該表的欄位
    table_columns = []
    for col_idx, (t_idx, col_name) in enumerate(columns):
        if t_idx == table_idx:
            col_type = column_types[col_idx] if col_idx < len(column_types) else 'text'
            table_columns.append(f"{col_name} ({col_type})")

    # 只顯示前 15 個欄位
    if len(table_columns) <= 15:
        cols_str = ', '.join(table_columns)
    else:
        cols_str = ', '.join(table_columns[:15]) + f', ... ({len(table_columns)-15} more)'

    return f"Table {table_name}: {cols_str}"

def generate_natural_language_training(schema_file, output_file):
    """生成自然語言問答對（真正的自然語言）"""

    # 載入 schema
    with open(schema_file, 'r', encoding='utf-8') as f:
        schema = json.load(f)[0]

    db_id = schema['db_id']
    tables = schema['table_names_original']
    columns = schema['column_names_original']
    column_types = schema['column_types']

    training_data = []

    print(f"📊 生成自然語言訓練資料: {db_id}")
    print(f"   表數量: {len(tables)}\n")

    # 為每個表生成自然語言問答
    for table_idx, table_name in enumerate(tables):
        print(f"📋 處理表: {table_name}")

        # 獲取該表的欄位
        table_columns = []
        for idx, (t_idx, col_name) in enumerate(columns):
            if t_idx == table_idx:
                col_type = column_types[idx] if idx < len(column_types) else 'text'
                table_columns.append((col_name, col_type))

        print(f"   欄位數: {len(table_columns)}")

        # 推測表的業務含義
        table_business_name = infer_table_business_name(table_name)

        # 構建該表的 schema 描述
        table_schema = build_table_schema_description(
            {'table_names_original': tables, 'column_names_original': columns, 'column_types': column_types},
            table_name
        )

        # === 1. 基礎統計（自然語言）===
        training_data.extend([
            {
                "schema": table_schema,
                "db_id": db_id,
                "question": f"How many {table_business_name} do we have?",
                "query": f"SELECT COUNT(*) FROM {table_name}",
                "source": "wp_m09"
            },
            {
                "schema": table_schema,
                "db_id": db_id,
                "question": f"What is the total count of {table_business_name}?",
                "query": f"SELECT COUNT(*) FROM {table_name}",
                "source": "wp_m09"
            },
            {
                "schema": table_schema,
                "db_id": db_id,
                "question": f"Count the number of {table_business_name}",
                "query": f"SELECT COUNT(*) FROM {table_name}",
                "source": "wp_m09"
            },
            {
                "schema": table_schema,
                "db_id": db_id,
                "question": f"How many items are there?",  # 完全不提表名
                "query": f"SELECT COUNT(*) FROM {table_name}",
                "source": "wp_m09"
            },
        ])

        # === 2. 列表查詢（自然語言）===
        training_data.extend([
            {
                "schema": table_schema,
                "db_id": db_id,
                "question": f"Show me all {table_business_name}",
                "query": f"SELECT * FROM {table_name}",
                "source": "wp_m09"
            },
            {
                "schema": table_schema,
                "db_id": db_id,
                "question": f"List all {table_business_name}",
                "query": f"SELECT * FROM {table_name}",
                "source": "wp_m09"
            },
            {
                "schema": table_schema,
                "db_id": db_id,
                "question": f"Display all available items",  # 完全不提表名
                "query": f"SELECT * FROM {table_name}",
                "source": "wp_m09"
            },
            {
                "schema": table_schema,
                "db_id": db_id,
                "question": f"Give me a complete list",  # 完全不提表名
                "query": f"SELECT * FROM {table_name}",
                "source": "wp_m09"
            },
        ])

        # === 3. 根據欄位生成業務問題（自然語言）===
        for col_name, col_type in table_columns:
            # 跳過技術欄位
            if col_name.lower() in ['sn', 'id', 'createtime', 'updatetime', 'createdate', 'updatedate', 'rowguid', 'empid']:
                continue

            # 推測欄位的業務含義
            field_business_name = infer_field_business_name(col_name)

            # 數值欄位
            if col_type == 'number':
                # 價格/成本/金額欄位
                if any(keyword in col_name.lower() for keyword in ['price', 'cost', 'amount', 'total', 'value']):
                    training_data.extend([
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"What is the average {field_business_name}?",  # 不提欄位名
                            "query": f"SELECT AVG({col_name}) FROM {table_name}",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"Calculate the average price",  # 通用問法
                            "query": f"SELECT AVG({col_name}) FROM {table_name}",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"What is the total {field_business_name}?",
                            "query": f"SELECT SUM({col_name}) FROM {table_name}",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"Sum up all prices",  # 通用問法
                            "query": f"SELECT SUM({col_name}) FROM {table_name}",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"What is the highest {field_business_name}?",
                            "query": f"SELECT MAX({col_name}) FROM {table_name}",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"Find the maximum price",  # 通用問法
                            "query": f"SELECT MAX({col_name}) FROM {table_name}",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"What is the lowest {field_business_name}?",
                            "query": f"SELECT MIN({col_name}) FROM {table_name}",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"Show me the cheapest items",  # 業務語言
                            "query": f"SELECT TOP 10 * FROM {table_name} ORDER BY {col_name} ASC",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"Show me the most expensive items",  # 業務語言
                            "query": f"SELECT TOP 10 * FROM {table_name} ORDER BY {col_name} DESC",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"Which items cost more than 1000?",  # 業務語言
                            "query": f"SELECT * FROM {table_name} WHERE {col_name} > 1000",
                            "source": "wp_m09"
                        },
                    ])

                # 數量欄位
                elif any(keyword in col_name.lower() for keyword in ['qty', 'quantity', 'stock', 'count']):
                    training_data.extend([
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"What is the total {field_business_name}?",
                            "query": f"SELECT SUM({col_name}) FROM {table_name}",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"How much stock do we have?",  # 業務語言
                            "query": f"SELECT SUM({col_name}) FROM {table_name}",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"Show items with no stock",  # 業務語言
                            "query": f"SELECT * FROM {table_name} WHERE {col_name} = 0",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"Which items are out of stock?",  # 業務語言
                            "query": f"SELECT * FROM {table_name} WHERE {col_name} = 0",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"Show items with the most inventory",  # 業務語言
                            "query": f"SELECT TOP 10 * FROM {table_name} ORDER BY {col_name} DESC",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"What is the average inventory level?",  # 業務語言
                            "query": f"SELECT AVG({col_name}) FROM {table_name}",
                            "source": "wp_m09"
                        },
                    ])

            # 文字欄位
            elif col_type == 'text':
                # 名稱欄位
                if 'name' in col_name.lower():
                    training_data.extend([
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"What are the different names?",  # 不提欄位名
                            "query": f"SELECT DISTINCT {col_name} FROM {table_name}",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"List all names",  # 簡化
                            "query": f"SELECT {col_name} FROM {table_name}",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"Show me unique names",  # 簡化
                            "query": f"SELECT DISTINCT {col_name} FROM {table_name}",
                            "source": "wp_m09"
                        },
                    ])

                # 代碼/編號欄位
                elif any(keyword in col_name.lower() for keyword in ['code', 'no', 'barcode']):
                    training_data.extend([
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"Show all product codes",  # 業務語言
                            "query": f"SELECT {col_name} FROM {table_name}",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"List all item numbers",  # 業務語言
                            "query": f"SELECT {col_name} FROM {table_name}",
                            "source": "wp_m09"
                        },
                    ])

                # 布林欄位 (is 開頭)
                elif col_name.startswith('is') or col_name.startswith('has'):
                    # 推測布林欄位的業務含義
                    bool_meaning = infer_boolean_meaning(col_name)

                    training_data.extend([
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"Show {table_business_name} that are {bool_meaning}",  # 業務語言
                            "query": f"SELECT * FROM {table_name} WHERE {col_name} = 1",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"Which items are {bool_meaning}?",  # 業務語言
                            "query": f"SELECT * FROM {table_name} WHERE {col_name} = 1",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"How many are {bool_meaning}?",  # 業務語言
                            "query": f"SELECT COUNT(*) FROM {table_name} WHERE {col_name} = 1",
                            "source": "wp_m09"
                        },
                    ])

            # 時間欄位
            elif col_type == 'time':
                if 'date' in col_name.lower() or 'time' in col_name.lower():
                    training_data.extend([
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"Show recent {table_business_name}",
                            "query": f"SELECT TOP 10 * FROM {table_name} ORDER BY {col_name} DESC",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"What are the latest items?",  # 業務語言
                            "query": f"SELECT TOP 10 * FROM {table_name} ORDER BY {col_name} DESC",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"Show items from today",  # 業務語言
                            "query": f"SELECT * FROM {table_name} WHERE CAST({col_name} AS DATE) = CAST(GETDATE() AS DATE)",
                            "source": "wp_m09"
                        },
                        {
                            "schema": table_schema,
                            "db_id": db_id,
                            "question": f"What was added today?",  # 業務語言
                            "query": f"SELECT * FROM {table_name} WHERE CAST({col_name} AS DATE) = CAST(GETDATE() AS DATE)",
                            "source": "wp_m09"
                        },
                    ])

        # === 4. 複雜查詢（自然語言）===
        if len(table_columns) >= 3:
            text_cols = [col for col, typ in table_columns if typ == 'text' and 'name' not in col.lower() and not col.startswith('is')]
            if text_cols:
                group_col = text_cols[0]
                field_name = infer_field_business_name(group_col)
                training_data.extend([
                    {
                        "schema": table_schema,
                        "db_id": db_id,
                        "question": f"Group by {field_name}",
                        "query": f"SELECT {group_col}, COUNT(*) FROM {table_name} GROUP BY {group_col}",
                        "source": "wp_m09"
                    },
                    {
                        "schema": table_schema,
                        "db_id": db_id,
                        "question": f"Count by category",  # 業務語言
                        "query": f"SELECT {group_col}, COUNT(*) FROM {table_name} GROUP BY {group_col}",
                        "source": "wp_m09"
                    },
                ])

        count = len([d for d in training_data if d.get('source') == 'wp_m09'])
        print(f"   目前總計: {count} 個問答對")

    # 保存
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(training_data, f, indent=2, ensure_ascii=False)

    wp_count = len([d for d in training_data if d.get('source') == 'wp_m09'])

    print(f"\n✅ 總共生成 {wp_count} 個自然語言問答對")
    print(f"💾 保存至: {output_path}")

    # 顯示範例
    print(f"\n📝 自然語言範例:")
    for i, item in enumerate(training_data[:5]):
        print(f"\n{i+1}. Question: {item['question']}")
        print(f"   SQL: {item['query']}")

    return training_data

def infer_boolean_meaning(col_name):
    """推測布林欄位的業務含義"""
    bool_mappings = {
        'issale': 'on sale',
        'istax': 'taxable',
        'isdel': 'deleted',
        'isstop': 'discontinued',
        'ischeck': 'checked',
        'iscredit': 'credit',
        'isdebit': 'debit',
        'iscash': 'cash',
        'isref': 'refunded',
    }

    col_lower = col_name.lower()

    if col_lower in bool_mappings:
        return bool_mappings[col_lower]

    # 移除 is/has 前綴
    if col_lower.startswith('is'):
        return col_lower[2:]
    elif col_lower.startswith('has'):
        return col_lower[3:]

    return col_lower

def infer_table_business_name(table_name):
    """推測表的業務名稱"""
    name = table_name
    if '_' in name:
        name = name.split('_')[-1]

    business_names = {
        'product': 'products',
        'customer': 'customers',
        'order': 'orders',
        'sale': 'sales',
        'purchase': 'purchases',
        'inventory': 'inventory items',
        'employee': 'employees',
        'supplier': 'suppliers',
        'invoice': 'invoices',
        'payment': 'payments',
        'item': 'items',
        'account': 'accounts',
        'transaction': 'transactions',
        'stock': 'stock items',
        'receipt': 'receipts',
    }

    name_lower = name.lower()
    for key, value in business_names.items():
        if key in name_lower:
            return value

    return name.lower() + 's' if not name.lower().endswith('s') else name.lower()

def infer_field_business_name(field_name):
    """推測欄位的業務名稱"""
    field_mappings = {
        'pricestd': 'standard price',
        'pricelow': 'lowest price',
        'pricemem': 'member price',
        'pricebat': 'batch price',
        'pricebad': 'defective price',
        'qtynow': 'current quantity',
        'qtysafe': 'safety stock',
        'qtyinitial': 'initial quantity',
        'costavg': 'average cost',
        'coststd': 'standard cost',
        'costinitial': 'initial cost',
        'pname': 'product name',
        'pnames': 'short name',
        'pcode': 'product code',
        'pbarcode': 'barcode',
        'punit': 'unit',
        'amount': 'amount',
        'qty': 'quantity',
    }

    field_lower = field_name.lower()

    if field_lower in field_mappings:
        return field_mappings[field_lower]

    for key, value in field_mappings.items():
        if key in field_lower:
            return value

    import re
    spaced = re.sub('([A-Z])', r' \1', field_name).strip().lower()

    return spaced if spaced else field_name.lower()

if __name__ == "__main__":
    schema_file = "data/wp_m09/tables.json"
    output_file = "data/wp_m09/train_final.json"

    if not Path(schema_file).exists():
        print(f"❌ Schema file not found: {schema_file}")
        print("Please run first: python convert_excel_to_json.py")
    else:
        training_data = generate_natural_language_training(schema_file, output_file)

        print(f"\n📝 下一步:")
        print(f"   python train_wp_m09.py")