"""
生成完整訓練資料（Spider + WP_M09）
"""
import json
from pathlib import Path


def generate_complete_training_data():
    """生成完整的訓練資料"""

    training_data = []

    # ========== 1. 載入 Spider 訓練資料（基礎）==========
    print("📚 載入 Spider 訓練資料...")

    spider_file = "data/spider/train_spider.json"
    if Path(spider_file).exists():
        with open(spider_file, 'r', encoding='utf-8') as f:
            spider_data = json.load(f)

        # 取前 1000 個 Spider 樣本
        for item in spider_data[:1000]:
            training_data.append({
                "db_id": item['db_id'],
                "question": item['question'],
                "query": item['query']
            })

        print(f"✅ 載入 {len(training_data)} 個 Spider 樣本\n")
    else:
        print(f"⚠️  找不到 Spider 訓練資料: {spider_file}\n")

    # ========== 2. 載入 WP_M09 Schema ==========
    print("📊 載入 WP_M09 Schema...")

    schema_file = "data/wp_m09/tables.json"
    if not Path(schema_file).exists():
        print(f"❌ 找不到 schema: {schema_file}")
        return None

    with open(schema_file, 'r', encoding='utf-8') as f:
        schema = json.load(f)[0]

    db_id = schema['db_id']
    tables = schema['table_names_original']
    columns = schema['column_names_original']
    column_types = schema['column_types']

    print(f"✅ 載入 {len(tables)} 個表\n")

    # ========== 3. 為 WP_M09 生成問答對 ==========
    print("🔄 生成 WP_M09 訓練資料...\n")

    wp_start_count = len(training_data)

    for table_idx, table_name in enumerate(tables):
        print(f"📋 處理表: {table_name}")

        # 獲取該表的欄位
        table_columns = []
        for idx, (t_idx, col_name) in enumerate(columns):
            if t_idx == table_idx:
                col_type = column_types[idx] if idx < len(column_types) else 'text'
                table_columns.append((col_name, col_type))

        # === 基礎查詢（每個表至少 10 個） ===

        # COUNT
        training_data.extend([
            {
                "db_id": db_id,
                "question": f"How many records in {table_name}?",
                "query": f"SELECT COUNT(*) FROM {table_name}"
            },
            {
                "db_id": db_id,
                "question": f"Count rows in {table_name}",
                "query": f"SELECT COUNT(*) FROM {table_name}"
            },
        ])

        # SELECT ALL
        training_data.extend([
            {
                "db_id": db_id,
                "question": f"Show all from {table_name}",
                "query": f"SELECT * FROM {table_name}"
            },
            {
                "db_id": db_id,
                "question": f"List {table_name}",
                "query": f"SELECT * FROM {table_name}"
            },
        ])

        # 找數值欄位
        number_cols = [col for col, typ in table_columns if typ == 'number']
        if number_cols:
            col = number_cols[0]
            training_data.extend([
                {
                    "db_id": db_id,
                    "question": f"What is the average {col}?",
                    "query": f"SELECT AVG({col}) FROM {table_name}"
                },
                {
                    "db_id": db_id,
                    "question": f"What is the maximum {col}?",
                    "query": f"SELECT MAX({col}) FROM {table_name}"
                },
                {
                    "db_id": db_id,
                    "question": f"Show top 10 by {col}",
                    "query": f"SELECT TOP 10 * FROM {table_name} ORDER BY {col} DESC"
                },
            ])

        # 找文字欄位
        text_cols = [col for col, typ in table_columns if typ == 'text' and 'name' in col.lower()]
        if text_cols:
            col = text_cols[0]
            training_data.extend([
                {
                    "db_id": db_id,
                    "question": f"Show {col} from {table_name}",
                    "query": f"SELECT {col} FROM {table_name}"
                },
                {
                    "db_id": db_id,
                    "question": f"List distinct {col}",
                    "query": f"SELECT DISTINCT {col} FROM {table_name}"
                },
            ])

        # 找時間欄位
        time_cols = [col for col, typ in table_columns if typ == 'time']
        if time_cols:
            col = time_cols[0]
            training_data.append({
                "db_id": db_id,
                "question": f"Show recent records from {table_name}",
                "query": f"SELECT TOP 10 * FROM {table_name} ORDER BY {col} DESC"
            })

        count = len([d for d in training_data if d['db_id'] == db_id])
        print(f"   生成 {count - wp_start_count} 個樣本")

    wp_count = len(training_data) - wp_start_count

    print(f"\n✅ WP_M09 總樣本: {wp_count}")

    # ========== 4. 保存 ==========
    output_file = "data/wp_m09/train_natural.json"
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(training_data, f, indent=2, ensure_ascii=False)

    print(f"\n💾 已保存至: {output_file}")
    print(f"\n📊 統計:")
    print(f"   Spider 樣本: {len(training_data) - wp_count}")
    print(f"   WP_M09 樣本: {wp_count}")
    print(f"   總計: {len(training_data)}")

    return training_data


if __name__ == "__main__":
    print("=" * 70)
    print("🚀 生成完整訓練資料")
    print("=" * 70)
    print()

    data = generate_complete_training_data()

    if data:
        print("\n" + "=" * 70)
        print("✅ 完成！")
        print("=" * 70)
        print("\n📝 下一步:")
        print("1. python train_wp_m09.py")
        print("2. python test_wp_m09.py")