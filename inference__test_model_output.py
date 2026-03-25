"""
測試 WP_M09 模型（修正版 - 使用訓練時的格式）
"""
import torch
import json
import re
from pathlib import Path
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from peft import PeftModel

MODEL_PATH = "outputs/models/wp_m09_from_scratch/final_model"
SCHEMA_FILE = "data/wp_m09/tables.json"

print("="*70)
print("🧪 測試 WP_M09 模型")
print("="*70)

# 載入模型
if not Path(MODEL_PATH).exists():
    print(f"❌ 模型不存在: {MODEL_PATH}")
    print(f"等待訓練完成...")
    exit()

print(f"\n📥 載入模型: {MODEL_PATH}")

tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
if tokenizer.pad_token is None:
    tokenizer.pad_token = tokenizer.eos_token

bnb_config = BitsAndBytesConfig(
    load_in_4bit=True,
    bnb_4bit_quant_type="nf4",
    bnb_4bit_compute_dtype=torch.bfloat16,
)

base_model = AutoModelForCausalLM.from_pretrained(
    "meta-llama/Llama-3.1-8B-Instruct",
    quantization_config=bnb_config,
    device_map="auto",
    torch_dtype=torch.bfloat16,
)

model = PeftModel.from_pretrained(base_model, MODEL_PATH)
model.eval()

print("✅ 模型載入完成")

# 載入 schema
print(f"\n📊 載入 Schema...")
with open(SCHEMA_FILE, 'r', encoding='utf-8') as f:
    schema = json.load(f)[0]

tables = schema['table_names_original']
columns = schema['column_names_original']
column_types = schema['column_types']

print(f"✅ 資料庫: {schema['db_id']}")
print(f"✅ 表數量: {len(tables)}\n")

# 構建 schema 描述的函數
def build_schema_for_table(table_name):
    """為特定表構建 schema 描述"""
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
        cols_str = ', '.join(table_cols[:15]) + f', ... ({len(table_cols)-15} more)'

    return f"Table {table_name}: {cols_str}"

# === 測試問題（針對 WP_M09 的真實問題）===
test_cases = [
    {
        "table": "WP_Product",
        "question": "How many products do we have?",
        "expected_contains": ["SELECT", "COUNT", "WP_Product"]
    },
    {
        "table": "WP_Product",
        "question": "What is the average price?",
        "expected_contains": ["SELECT", "AVG", "price"]
    },
    {
        "table": "WP_Product",
        "question": "Show me the most expensive items",
        "expected_contains": ["SELECT", "ORDER BY", "DESC"]
    },
    {
        "table": "WP_Product",
        "question": "Which items are out of stock?",
        "expected_contains": ["SELECT", "WHERE", "= 0"]
    },
    {
        "table": "WP_Product",
        "question": "List all product names",
        "expected_contains": ["SELECT", "pName", "WP_Product"]
    },
    {
        "table": "WP_vReceipt",
        "question": "How many receipts are there?",
        "expected_contains": ["SELECT", "COUNT", "Receipt"]
    },
    {
        "table": "WP_vReceipt",
        "question": "What is the total amount?",
        "expected_contains": ["SELECT", "SUM", "amount"]
    },
    {
        "table": "WP_vReceipt",
        "question": "Show recent receipts",
        "expected_contains": ["SELECT", "ORDER BY", "DESC"]
    },
    {
        "table": "WP_vPayment",
        "question": "How much in total payments?",
        "expected_contains": ["SELECT", "SUM", "amount"]
    },
    {
        "table": "WP_vPayment",
        "question": "Show cash payments",
        "expected_contains": ["SELECT", "WHERE", "Cash"]
    },
]

print("🔬 測試結果:\n")

valid = 0
total = len(test_cases)

for i, test in enumerate(test_cases, 1):
    table = test['table']
    question = test['question']

    # 構建該表的 schema（使用訓練時的格式）
    schema_desc = build_schema_for_table(table)

    # 使用與訓練相同的 prompt 格式
    prompt = f"Schema: {schema_desc}\nQuestion: {question}\nSQL:"

    # Tokenize
    inputs = tokenizer(
        prompt,
        return_tensors="pt",
        truncation=True,
        max_length=512
    ).to(model.device)

    # 生成
    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=128,
            do_sample=False,
            pad_token_id=tokenizer.pad_token_id,
            eos_token_id=tokenizer.eos_token_id,
        )

    # 解碼
    sql = tokenizer.decode(
        outputs[0][inputs['input_ids'].shape[1]:],
        skip_special_tokens=True
    ).strip()

    # 清理
    sql = re.sub(r'```sql\s*', '', sql)
    sql = re.sub(r'```\s*', '', sql)
    sql = sql.split('\n')[0].strip()

    # 驗證
    has_select = bool(re.search(r'\bSELECT\b', sql, re.IGNORECASE))
    has_from = bool(re.search(r'\bFROM\b', sql, re.IGNORECASE))

    # 檢查預期內容
    contains_expected = all(
        keyword.lower() in sql.lower()
        for keyword in test['expected_contains'][:2]  # 至少包含前 2 個關鍵字
    )

    is_valid = has_select and has_from and contains_expected

    if is_valid:
        valid += 1
        status = "✅"
    else:
        status = "❌"

    print(f"{i}. {status} Table: {table}")
    print(f"      Q: {question}")
    print(f"      SQL: {sql}")

    if not is_valid:
        if not has_select:
            print(f"      ⚠️  Missing SELECT")
        if not has_from:
            print(f"      ⚠️  Missing FROM")
        if not contains_expected:
            print(f"      ⚠️  Missing expected keywords: {test['expected_contains']}")

    print()

    # 釋放記憶體
    del inputs, outputs
    torch.cuda.empty_cache()

# 統計
print("="*70)
print(f"📊 測試結果: {valid}/{total} 正確 ({valid/total*100:.1f}%)")
print("="*70)

if valid == 0:
    print(f"\n❌ 所有測試失敗")
    print(f"\n可能原因:")
    print(f"1. 模型還在訓練中（等待訓練完成）")
    print(f"2. 訓練資料格式問題")
    print(f"3. 模型沒有收斂")
elif valid < total * 0.5:
    print(f"\n⚠️  準確率偏低")
    print(f"建議:")
    print(f"1. 增加訓練 epochs")
    print(f"2. 增加訓練資料")
    print(f"3. 檢查訓練 loss 是否收斂")
elif valid < total * 0.8:
    print(f"\n✅ 表現尚可")
    print(f"可以繼續訓練或增加資料以提升")
else:
    print(f"\n🎉 表現優秀！")
    print(f"模型已準備好使用")

print(f"\n📝 下一步:")
print(f"如果準確率 < 60%:")
print(f"  - 等待訓練完成")
print(f"  - 增加訓練 epochs")
print(f"  - 檢查訓練資料質量")