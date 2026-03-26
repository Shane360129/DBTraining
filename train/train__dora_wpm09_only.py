"""
WP_M09 訓練（純 WP_M09 資料，不使用 Spider）
使用日期命名權重
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


def main():
    # 使用今天的日期命名
    today = datetime.now().strftime("%m%d")  # 格式: 0228

    print("=" * 70)
    print(f"🚀 WP_M09 訓練（純 WP_M09 資料）")
    print(f"📅 訓練日期: {today}")
    print("=" * 70)

    # ========== 配置 ==========
    BASE_MODEL = "meta-llama/Llama-3.1-8B-Instruct"
    SCHEMA_FILE = "data/wp_m09/tables.json"
    TRAIN_FILE = "data/wp_m09/train_final.json"
    OUTPUT_DIR = f"outputs/models/wp_m09_{today}"  # 使用日期命名

    print(f"\n📁 輸出目錄: {OUTPUT_DIR}")

    # ========== 1. 載入 Schema ==========
    print(f"\n📊 載入 Schema...")

    if not Path(SCHEMA_FILE).exists():
        print(f"❌ 找不到 Schema: {SCHEMA_FILE}")
        return

    with open(SCHEMA_FILE, 'r', encoding='utf-8') as f:
        schema_data = json.load(f)[0]

    tables = schema_data['table_names_original']
    print(f"✅ Schema 已載入")
    print(f"   資料庫: {schema_data['db_id']}")
    print(f"   表數量: {len(tables)}")

    # ========== 2. 載入訓練資料 ==========
    print(f"\n📊 載入訓練資料...")

    train_data = []

    # 載入 train_final.json
    if Path(TRAIN_FILE).exists():
        print(f"   載入 {TRAIN_FILE}...")
        with open(TRAIN_FILE, 'r', encoding='utf-8') as f:
            train_final = json.load(f)
        train_data.extend(train_final)
        print(f"   ✅ {len(train_final)} 個樣本")
    else:
        print(f"   ❌ 找不到 {TRAIN_FILE}")
        print(f"   請先生成訓練資料:")
        print(f"      python generate_natural_training_data.py")
        return

    if not train_data:
        print(f"\n❌ 沒有訓練資料")
        return

    print(f"\n✅ 總訓練樣本: {len(train_data)}")

    # 統計來源
    by_source = {}
    for item in train_data:
        source = item.get('source', 'unknown')
        by_source[source] = by_source.get(source, 0) + 1

    print(f"\n📊 訓練資料來源:")
    for source, count in sorted(by_source.items()):
        print(f"   {source}: {count}")

    # 檢查資料格式
    sample = train_data[0]
    has_schema = 'schema' in sample

    if has_schema:
        print(f"\n✅ 資料包含 Schema 資訊")
    else:
        print(f"\n⚠️  資料不包含 Schema 資訊")
        print(f"   建議重新生成訓練資料以包含 Schema")

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

    # 準備模型
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
        schema = item.get('schema', '')
        question = item['question']
        answer = item['query']

        if schema:
            text = f"Schema: {schema}\nQuestion: {question}\nSQL: {answer}"
        else:
            text = f"Question: {question}\nSQL: {answer}"

        return text

    # 格式化
    texts = [format_training_text(item) for item in train_data]

    # 顯示範例
    print(f"\n📝 訓練格式範例:")
    print(f"{'=' * 70}")
    print(texts[0])
    print(f"{'=' * 70}")

    if len(texts) > 1:
        print(f"\n{'=' * 70}")
        print(texts[-1])
        print(f"{'=' * 70}")

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
    elif len(train_data) < 2000:
        num_epochs = 5
        print(f"   樣本較多 (<2000)，使用 5 epochs")
    else:
        num_epochs = 3
        print(f"   樣本很多 (≥2000)，使用 3 epochs")

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
    print("\n" + "=" * 70)
    print("🚀 開始訓練...")
    print("=" * 70)
    print(f"訓練日期: {today}")
    print(f"訓練樣本: {len(train_data)}")
    print(f"Epochs: {num_epochs}")
    print(f"Batch size: {training_args.per_device_train_batch_size}")
    print(f"Gradient accumulation: {training_args.gradient_accumulation_steps}")
    print(
        f"Effective batch size: {training_args.per_device_train_batch_size * training_args.gradient_accumulation_steps}")
    print(f"Learning rate: {training_args.learning_rate}")
    print(f"輸出: {OUTPUT_DIR}")
    print(f"僅使用 WP_M09 資料: ✅")
    print("=" * 70)

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
        "data_sources": by_source,
        "epochs": num_epochs,
        "learning_rate": training_args.learning_rate,
        "has_schema": has_schema,
        "lora_r": lora_config.r,
        "lora_alpha": lora_config.lora_alpha,
        "output_dir": str(OUTPUT_DIR),
        "final_model_path": str(final_path),
        "wp_m09_only": True,
        "spider_included": False,
        "note": "Pure WP_M09 training without Spider data"
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
    print("\n" + "=" * 70)
    print("🎉 訓練完成！")
    print("=" * 70)
    print(f"\n📁 模型位置:")
    print(f"   {final_path}")

    print(f"\n📝 下一步:")
    print(f"   python evaluate_wp_m09.py")


if __name__ == "__main__":
    main()