import json

# 定義檔案路徑
input_file = r"D:\spider1_training\data\wp_m09\train_9views_20k.json"
output_file = '/data/wp_m09/spider_format_light.json'
database_name = 'WP_M09'

# 讀取原始檔案
with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

spider_dataset = []

# 進行格式映射與轉換
for item in data:
    spider_item = {
        "db_id": database_name,
        "question": item["question"],
        "query": item["query"]
    }
    # 若訓練框架支援，可選擇保留難易度欄位，否則標準寫法只需上述三個鍵值

    spider_dataset.append(spider_item)

# 輸出為標準 JSON 格式
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(spider_dataset, f, ensure_ascii=False, indent=2)

print(f"成功轉換 {len(spider_dataset)} 筆資料，已儲存至 {output_file}")