"""
使用上傳的 train.json 訓練模型
創建全新權重
"""
import json
import torch
import copy
from pathlib import Path
from datetime import datetime
from transformers import (
    AutoTokenizer,
    AutoModelForCausalLM,
    BitsAndBytesConfig,
    TrainingArguments,
    Trainer,
    DataCollatorForLanguageModeling
)
from peft import LoraConfig, get_peft_model, prepare_model_for_kbit_training
from datasets import Dataset

def load_schema():
    """載入 schema 用於構建 schema 描述"""
    schema_file = "data/wp_m09/tables.json"

    if not Path(schema_file).exists():
        print(f"⚠️  找不到 schema 文件: {schema_file}")
        print(f"將不包含 schema 資訊")
        return None

    with open(schema_file, 'r', encoding='utf-8') as f:
        schema = json.load(f)[0]

    return schema

def build_schema_for_table(table_name, schema):
    """為特定表構建 schema 描述"""
    if not schema or table_name not in schema['table_names_original']:
        return ""

    tables = schema['table_names_original']
    columns = schema['column_names_original']
    column_types = schema['column_types']

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

def main():
    # 使用今天的日期命名
    today = datetime.now().strftime("%m%d")

    print("="*70)
    print(f"🚀 WP_M09 訓練（使用上傳的 train.json）")
    print(f"📅 訓練日期: {today}")
    print("="*70)

    # ========== 配置 ==========
    BASE_MODEL = "meta-llama/Llama-3.1-8B-Instruct"
    OUTPUT_DIR = f"outputs/models/wp_m09_uploaded_{today}"

    print(f"\n📁 輸出目錄: {OUTPUT_DIR}")

    # ========== 1. 尋找訓練文件 ==========
    print(f"\n📋 尋找訓練文件...")

    # 可能的文件位置
    possible_paths = [
        "train.json",  # 當前目錄
        "data/wp_m09/train.json",  # data 目錄
        "data/wp_m09/train_uploaded.json",  # 上傳後的位置
    ]

    train_file = None
    for path in possible_paths:
        if Path(path).exists():
            train_file = path
            print(f"✅ 找到訓練文件: {path}")
            break

    if not train_file:
        print(f"\n❌ 找不到訓練文件！")
        print(f"\n請將 train.json 放在以下任一位置:")
        for path in possible_paths:
            print(f"   - {path}")
        print(f"\n或執行以下命令:")
        print(f"   將 train.json 複製到當前目錄")
        return

    # ========== 2. 載入訓練資料 ==========
    print(f"\n📊 載入訓練資料...")

    with open(train_file, 'r', encoding='utf-8') as f:
        train_data = json.load(f)

    print(f"✅ 訓練樣本數: {len(train_data)}")

    # 載入 schema（如果有）
    schema = load_schema()

    if schema:
        print(f"✅ Schema 已載入")

    # 顯示範例
    print(f"\n📝 訓練資料範例:")
    sample = train_data[0]
    print(f"   Question: {sample['question']}")
    print(f"   Query: {sample['query']}")
    print(f"   Table: {sample.get('table', 'N/A')}")
    print(f"   Difficulty: {sample.get('difficulty', 'N/A')}")

    # 統計
    by_difficulty = {}
    by_table = {}

    for item in train_data:
        diff = item.get('difficulty', 'unknown')
        by_difficulty[diff] = by_difficulty.get(diff, 0) + 1

        table = item.get('table', 'unknown')
        by_table[table] = by_table.get(table, 0) + 1

    print(f"\n按難度統計:")
    for diff, count in sorted(by_difficulty.items()):
        print(f"   {diff}: {count}")

    print(f"\n按表統計（前 10）:")
    for table, count in sorted(by_table.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"   {table}: {count}")

    # ========== 3. 載入基礎模型 ==========
    print(f"\n📥 載入基礎模型...")
    print(f"   模型: {BASE_MODEL}")
    print(f"   訓練方式: 從頭開始（添加 LoRA）")

    tokenizer = AutoTokenizer.from_pretrained(BASE_MODEL)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    bnb_config = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16,
    )

    print("   載入模型...")
    model = AutoModelForCausalLM.from_pretrained(
        BASE_MODEL,
        quantization_config=bnb_config,
        device_map="auto",
        torch_dtype=torch.bfloat16,
        trust_remote_code=True,
    )

    model = prepare_model_for_kbit_training(model)

    # ========== 4. 添加 LoRA 配置 ==========
    print("   添加 LoRA 適配器...")

    lora_config = LoraConfig(
        r=16,
        lora_alpha=32,
        target_modules=[
            "q_proj",
            "k_proj",
            "v_proj",
            "o_proj",
            "gate_proj",
            "up_proj",
            "down_proj",
        ],
        lora_dropout=0.05,
        bias="none",
        task_type="CAUSAL_LM"
    )

    model = get_peft_model(model, lora_config)
    model.print_trainable_parameters()

    print("✅ 模型準備完成")

    # ========== 5. 準備訓練資料 ==========
    print(f"\n🔄 準備訓練資料...")

    def format_training_text(item):
        """格式化訓練文本"""
        table = item.get('table', '')
        question = item['question']
        answer = item['query']

        # 構建 schema 描述
        if schema and table:
            schema_desc = build_schema_for_table(table, schema)
            if schema_desc:
                text = f"Schema: {schema_desc}\nQuestion: {question}\nSQL: {answer}"
            else:
                text = f"Question: {question}\nSQL: {answer}"
        else:
            text = f"Question: {question}\nSQL: {answer}"

        return text

    # 格式化
    texts = [format_training_text(item) for item in train_data]

    # 顯示範例
    print(f"\n📝 訓練格式範例:")
    print(f"{'='*70}")
    print(texts[0])
    print(f"{'='*70}")

    # 創建 Dataset
    dataset = Dataset.from_dict({'text': texts})

    print(f"\n✅ 資料集大小: {len(dataset)}")

    # ========== 6. Tokenize ==========
    print(f"\n🔄 Tokenizing...")

    def tokenize_function(examples):
        results = tokenizer(
            examples['text'],
            truncation=True,
            max_length=512,
            padding='max_length',
            return_tensors=None
        )
        results['labels'] = copy.deepcopy(results['input_ids'])
        return results

    tokenized_dataset = dataset.map(
        tokenize_function,
        batched=True,
        remove_columns=['text'],
        desc="Tokenizing"
    )

    print(f"✅ Tokenization 完成")

    # ========== 7. 訓練配置 ==========
    print(f"\n🔧 訓練配置...")

    # 根據資料量調整
    if len(train_data) < 500:
        num_epochs = 10
        print(f"   樣本較少 (<500)，增加 epochs 到 10")
    elif len(train_data) < 1000:
        num_epochs = 7
        print(f"   樣本中等 (<1000)，使用 7 epochs")
    else:
        num_epochs = 5
        print(f"   樣本較多 (≥1000)，使用 5 epochs")

    print(f"   訓練樣本: {len(train_data)}")
    print(f"   Epochs: {num_epochs}")

    training_args = TrainingArguments(
        output_dir=OUTPUT_DIR,
        num_train_epochs=num_epochs,
        per_device_train_batch_size=4,
        gradient_accumulation_steps=4,
        learning_rate=3e-4,
        weight_decay=0.01,
        warmup_ratio=0.1,
        lr_scheduler_type="cosine",
        optim="paged_adamw_8bit",
        max_grad_norm=1.0,
        gradient_checkpointing=True,
        save_strategy="epoch",
        save_total_limit=3,
        logging_steps=20,
        logging_dir=f"{OUTPUT_DIR}/logs",
        bf16=True,
        seed=42,
        report_to="none",
    )

    data_collator = DataCollatorForLanguageModeling(
        tokenizer=tokenizer,
        mlm=False,
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=tokenized_dataset,
        data_collator=data_collator,
    )

    # ========== 8. 開始訓練 ==========
    print("\n" + "="*70)
    print("🚀 開始訓練...")
    print("="*70)
    print(f"訓練日期: {today}")
    print(f"訓練樣本: {len(train_data)}")
    print(f"Epochs: {num_epochs}")
    print(f"Batch size: {training_args.per_device_train_batch_size}")
    print(f"Gradient accumulation: {training_args.gradient_accumulation_steps}")
    print(f"Effective batch size: {training_args.per_device_train_batch_size * training_args.gradient_accumulation_steps}")
    print(f"Learning rate: {training_args.learning_rate}")
    print(f"輸出: {OUTPUT_DIR}")
    print(f"使用上傳的訓練資料: ✅")
    print("="*70)

    print(f"\n💡 預期訓練 Loss 變化:")
    print(f"   Epoch 1: 2.0 → 1.5")
    print(f"   Epoch 3: 1.5 → 1.0")
    print(f"   Epoch 5: 1.0 → 0.7")
    print(f"\n⚠️  如果 Loss > 3.0 不降，請停止訓練並檢查資料\n")

    # 訓練
    trainer.train()

    # ========== 9. 保存模型 ==========
    print(f"\n💾 保存模型...")

    final_path = Path(OUTPUT_DIR) / "final_model"
    final_path.mkdir(parents=True, exist_ok=True)

    trainer.save_model(final_path)
    tokenizer.save_pretrained(final_path)

    print(f"✅ 模型已保存至: {final_path}")

    # 保存訓練資訊
    info = {
        "training_date": today,
        "base_model": BASE_MODEL,
        "training_from_scratch": True,
        "training_samples": len(train_data),
        "data_source": train_file,
        "by_difficulty": by_difficulty,
        "by_table": {k: v for k, v in sorted(by_table.items(), key=lambda x: x[1], reverse=True)[:20]},
        "epochs": num_epochs,
        "learning_rate": training_args.learning_rate,
        "has_schema": schema is not None,
        "lora_r": lora_config.r,
        "lora_alpha": lora_config.lora_alpha,
        "output_dir": str(OUTPUT_DIR),
        "final_model_path": str(final_path),
        "note": "Trained using uploaded train.json file"
    }

    info_file = final_path / "training_info.json"
    with open(info_file, 'w', encoding='utf-8') as f:
        json.dump(info, f, indent=2)

    print(f"✅ 訓練資訊已保存")

    # 顯示 checkpoints
    checkpoints = sorted(Path(OUTPUT_DIR).glob("checkpoint-*"))
    if checkpoints:
        print(f"\n📋 Checkpoints:")
        for ckpt in checkpoints:
            print(f"   - {ckpt.name}")
    print(f"   - final_model ✅")

    # ========== 10. 完成 ==========
    print("\n" + "="*70)
    print("🎉 訓練完成！")
    print("="*70)
    print(f"\n📁 模型位置:")
    print(f"   {final_path}")

    print(f"\n📝 下一步:")
    print(f"   python evaluate_uploaded_data.py")

if __name__ == "__main__":
    main()