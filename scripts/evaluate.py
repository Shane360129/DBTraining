"""
使用官方 Spider 評估腳本進行完整評估
"""
import os
import sys
import json
import yaml
import torch
import copy
import re
from pathlib import Path
from tqdm import tqdm
from typing import List, Dict

from transformers import AutoModelForCausalLM, AutoTokenizer
from peft import PeftModel

# 添加項目路徑
sys.path.append(str(Path(__file__).parent.parent))
from src.data_processing.preprocessor import SpiderPreprocessor
from src.data_processing.prompt_builder import PromptBuilder


def load_model_and_tokenizer(model_path, base_model_name=None):
    """載入微調後的模型"""
    print(f"🔧 載入模型: {model_path}")

    if base_model_name is None:
        config_path = Path("config/training_config.yaml")
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        base_model_name = config['model']['name']

    print("  載入基礎模型（使用 4-bit 量化節省顯存）...")

    # 使用量化配置節省顯存
    from transformers import BitsAndBytesConfig

    bnb_config = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16,
        bnb_4bit_use_double_quant=True,
    )

    model = AutoModelForCausalLM.from_pretrained(
        base_model_name,
        quantization_config=bnb_config,
        device_map="auto",
        torch_dtype=torch.bfloat16,
        trust_remote_code=True,
    )

    print("  載入微調權重...")
    model = PeftModel.from_pretrained(model, model_path)
    # 載入 tokenizer
    tokenizer = AutoTokenizer.from_pretrained(
        model_path,
        trust_remote_code=True,
        padding_side="left",
    )
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model.eval()
    print("✅ 模型載入完成！")

    return model, tokenizer
    # 不需要 merge，直接使用 LoRA 權重進行推理
    # model = model.merge_and_unload()  # 註解掉這行


def generate_sql(model, tokenizer, prompt_messages, max_length=256, max_attempts=3):
    """
    生成 SQL 查詢（帶自我修正）

    Args:
        model: 模型
        tokenizer: tokenizer
        prompt_messages: 初始 prompt messages
        max_length: 最大生成長度
        max_attempts: 最大修正次數

    Returns:
        sql: 最終生成的 SQL
    """
    import re
    import copy

    # 複製 messages 避免修改原始數據
    messages = copy.deepcopy(prompt_messages)

    for attempt in range(1, max_attempts + 1):
        # 生成 SQL
        prompt = tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True
        )

        inputs = tokenizer(
            prompt,
            return_tensors="pt",
            truncation=True,
            max_length=1024
        ).to(model.device)

        with torch.no_grad():
            outputs = model.generate(
                **inputs,
                max_new_tokens=max_length,
                do_sample=False,
                pad_token_id=tokenizer.pad_token_id,
                eos_token_id=tokenizer.eos_token_id,
            )

        generated_text = tokenizer.decode(
            outputs[0][inputs['input_ids'].shape[1]:],
            skip_special_tokens=True
        ).strip()

        # 清理 SQL
        sql = clean_sql(generated_text)

        # 驗證語法
        is_valid, error_msg = validate_sql_syntax(sql)

        if is_valid or attempt == max_attempts:
            # 語法正確或達到最大嘗試次數，返回結果
            return sql

        # 語法錯誤，添加修正 prompt
        messages.append({
            "role": "assistant",
            "content": sql
        })
        messages.append({
            "role": "user",
            "content": f"Error detected: {error_msg}\n\nPlease generate a corrected SQL query:"
        })

    return sql


def clean_sql(sql):
    """
    清理 SQL（移除 markdown、註解等）

    Args:
        sql: 原始 SQL 字串

    Returns:
        cleaned_sql: 清理後的 SQL
    """
    import re

    # 移除 markdown 代碼塊標記
    sql = re.sub(r'```sql\s*', '', sql)
    sql = re.sub(r'```\s*', '', sql)

    # 移除前後空白
    sql = sql.strip()

    # 移除單行註解
    lines = sql.split('\n')
    lines = [line for line in lines if not line.strip().startswith('--')]
    sql = '\n'.join(lines)

    # 移除多餘的空行
    sql = re.sub(r'\n\s*\n', '\n', sql)

    return sql.strip()


def validate_sql_syntax(sql):
    """
    驗證 SQL 基本語法

    Args:
        sql: SQL 查詢字串

    Returns:
        (is_valid, error_message): (是否有效, 錯誤訊息)
    """
    import re

    # 檢查 1: 是否為空
    if not sql or sql.strip() == "":
        return False, "SQL is empty"

    # 檢查 2: 必須包含 SELECT
    if not re.search(r'\bSELECT\b', sql, re.IGNORECASE):
        return False, "SQL must contain SELECT keyword"

    # 檢查 3: 必須包含 FROM
    if not re.search(r'\bFROM\b', sql, re.IGNORECASE):
        return False, "SQL must contain FROM clause"

    # 檢查 4: 括號是否匹配
    if sql.count('(') != sql.count(')'):
        return False, "Unmatched parentheses - opening and closing parentheses count mismatch"

    # 檢查 5: JOIN 必須有 ON
    has_join = re.search(r'\bJOIN\b', sql, re.IGNORECASE)
    has_on = re.search(r'\bON\b', sql, re.IGNORECASE)
    if has_join and not has_on:
        return False, "JOIN clause requires ON condition"

    # 檢查 6: 引號是否匹配
    single_quotes = sql.count("'")
    if single_quotes % 2 != 0:
        return False, "Unmatched single quotes"

    double_quotes = sql.count('"')
    if double_quotes % 2 != 0:
        return False, "Unmatched double quotes"

    # 通過所有檢查
    return True, None


def evaluate_on_spider(model, tokenizer, dev_data, db_schemas, output_file="predictions.json"):
    """在 Spider 驗證集上評估"""
    print(f"\n📊 開始評估 {len(dev_data)} 個樣本...")

    # 正確的初始化方式
    config = {'prompt': {'use_cot': True}}
    prompt_builder = PromptBuilder(config)
    predictions = []

    for item in tqdm(dev_data, desc="生成預測"):
        db_id = item['db_id']
        question = item['question']
        gold_sql = item['query']

        if db_id not in db_schemas:
            continue

        db_schema = db_schemas[db_id]

        prompt_messages = prompt_builder.build_training_messages(
            question=question,
            db_schema=db_schema,
            sql=None
        )

        try:
            # 使用帶自我修正的生成（最多嘗試 3 次）
            predicted_sql = generate_sql(
                model,
                tokenizer,
                prompt_messages,
                max_length=256,
                max_attempts=3  # 可調整修正次數
            )
        except Exception as e:
            print(f"❌ 生成失敗: {e}")
            predicted_sql = "SELECT *"

        predictions.append({
            "db_id": db_id,
            "question": question,
            "gold": gold_sql,
            "predicted": predicted_sql
        })

    output_path = Path("outputs/predictions") / output_file
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(predictions, f, indent=2, ensure_ascii=False)

    print(f"✅ 預測結果已保存至: {output_path}")

    return predictions


def run_official_evaluation(predictions_file, gold_file, db_dir, tables_file):
    """運行官方 Spider 評估腳本"""
    print("\n🎯 運行官方評估腳本...")

    # 準備評估格式（官方格式：SQL \t db_id）
    with open(predictions_file, 'r', encoding='utf-8') as f:
        predictions = json.load(f)

    eval_input = predictions_file.replace('.json', '_eval.txt')
    with open(eval_input, 'w', encoding='utf-8') as f:
        for item in predictions:
            # 官方格式：predicted_sql \t db_id
            f.write(f"{item['predicted']}\t{item['db_id']}\n")

    print(f"✅ 評估文件已生成: {eval_input}")

def main():
    import argparse

    parser = argparse.ArgumentParser(description="評估 Spider Text-to-SQL 模型")
    parser.add_argument("--model_path", type=str, required=True, help="微調模型路徑")
    parser.add_argument("--dev_file", type=str, default="data/spider/dev.json")
    parser.add_argument("--tables_file", type=str, default="data/spider/tables.json")
    parser.add_argument("--db_dir", type=str, default="data/spider/database")
    parser.add_argument("--output_file", type=str, default="predictions.json")
    parser.add_argument("--config_path", type=str, default="config/training_config.yaml", help="配置文件路徑")  # ← 添加這個參數

    args = parser.parse_args()

    # 載入配置
    config_path = Path(args.config_path)  # ← 改為 args.config_path
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # 載入模型
    model, tokenizer = load_model_and_tokenizer(args.model_path, config['model']['name'])

    # 載入資料
    preprocessor = SpiderPreprocessor(config)
    dev_data, db_schemas = preprocessor.load_spider_data(args.dev_file, args.tables_file)

    # 評估
    predictions = evaluate_on_spider(model, tokenizer, dev_data, db_schemas, args.output_file)

    # 運行官方評估
    predictions_path = Path("outputs/predictions") / args.output_file
    run_official_evaluation(
        str(predictions_path),
        args.dev_file,
        args.db_dir,
        args.tables_file
    )

    print("\n🎉 評估完成！")
if __name__ == "__main__":
    main()