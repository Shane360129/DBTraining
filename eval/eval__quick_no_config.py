"""
快速評估腳本（不依賴配置文件）
"""
import os
import sys
import json
import torch
from pathlib import Path
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from peft import PeftModel

# 添加項目路徑
sys.path.append(str(Path(__file__).parent.parent))
from src.data_processing.prompt_builder import PromptBuilder


def load_spider_data(dev_file, tables_file):
    """載入 Spider 驗證集"""
    with open(dev_file, 'r', encoding='utf-8') as f:
        dev_data = json.load(f)

    with open(tables_file, 'r', encoding='utf-8') as f:
        tables = json.load(f)

    db_schemas = {table['db_id']: table for table in tables}
    return dev_data, db_schemas


def generate_sql(model, tokenizer, prompt_builder, question, db_schema):
    """生成 SQL"""
    messages = prompt_builder.build_training_messages(question, db_schema, sql=None)

    prompt = tokenizer.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=True
    )

    inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=1024).to(model.device)

    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=256,
            do_sample=False,
            pad_token_id=tokenizer.pad_token_id,
            eos_token_id=tokenizer.eos_token_id,
        )

    generated_text = tokenizer.decode(
        outputs[0][inputs['input_ids'].shape[1]:],
        skip_special_tokens=True
    ).strip()

    del inputs, outputs
    torch.cuda.empty_cache()

    return generated_text


def main():
    # 配置
    MODEL_PATH = "outputs/models/spider1-llama31-dora-v3/final_model"
    BASE_MODEL = "meta-llama/Llama-3.1-8B-Instruct"
    DEV_FILE = "data/spider/dev.json"
    TABLES_FILE = "data/spider/tables.json"
    OUTPUT_FILE = "outputs/predictions/predictions_v3.json"

    print("=" * 60)
    print("🔍 快速評估")
    print("=" * 60)

    # 1. 載入模型
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
        BASE_MODEL,
        quantization_config=bnb_config,
        device_map="auto",
        torch_dtype=torch.bfloat16,
        trust_remote_code=True,
    )

    model = PeftModel.from_pretrained(base_model, MODEL_PATH)
    model.eval()

    print("✅ 模型載入完成")

    # 2. 載入資料
    print(f"\n📊 載入驗證集...")
    dev_data, db_schemas = load_spider_data(DEV_FILE, TABLES_FILE)
    print(f"✅ 驗證集: {len(dev_data)} 樣本")

    # 3. 初始化 prompt builder
    config = {'prompt': {'use_cot': True}}
    prompt_builder = PromptBuilder(config)

    # 4. 生成預測
    print(f"\n🔄 生成預測...")
    predictions = []

    for item in tqdm(dev_data, desc="生成預測"):
        db_id = item['db_id']
        question = item['question']
        gold_sql = item['query']

        schema = db_schemas.get(db_id, {})

        # 生成 SQL
        predicted_sql = generate_sql(model, tokenizer, prompt_builder, question, schema)

        predictions.append({
            'db_id': db_id,
            'question': question,
            'gold': gold_sql,
            'predicted': predicted_sql
        })

    # 5. 保存結果
    print(f"\n💾 保存預測結果...")
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(predictions, f, indent=2, ensure_ascii=False)

    print(f"✅ 預測已保存: {OUTPUT_FILE}")

    # 6. 生成評估格式
    eval_file = OUTPUT_FILE.replace('.json', '_eval.txt')
    with open(eval_file, 'w', encoding='utf-8') as f:
        for pred in predictions:
            f.write(f"{pred['predicted']}\t{pred['db_id']}\n")

    print(f"✅ 評估格式已保存: {eval_file}")

    # 7. 運行官方評估
    print(f"\n🎯 運行官方評估...")

    # 創建 gold 文件
    gold_file = "outputs/predictions/dev_gold_v3.txt"
    with open(gold_file, 'w', encoding='utf-8') as f:
        for pred in predictions:
            f.write(f"{pred['gold']}\t{pred['db_id']}\n")

    # 運行評估
    import subprocess

    eval_cmd = f"""python data/spider/evaluation.py \
        --gold {gold_file} \
        --pred {eval_file} \
        --db data/spider/database \
        --table {TABLES_FILE} \
        --etype all"""

    print(f"\n執行評估命令...")
    result = subprocess.run(eval_cmd, shell=True, capture_output=True, text=True)

    print("\n" + "=" * 60)
    print("評估結果")
    print("=" * 60)
    print(result.stdout)

    if result.stderr:
        print("錯誤訊息:")
        print(result.stderr)


if __name__ == "__main__":
    main()