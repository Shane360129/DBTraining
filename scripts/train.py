"""
完整的 GPU 訓練程式，支援 DoRA 和多GPU訓練
"""
import os

# 強制清除離線模式
for key in list(os.environ.keys()):
    if 'OFFLINE' in key:
        del os.environ[key]

import sys
import yaml
import torch
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

import transformers
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    TrainingArguments,
    BitsAndBytesConfig,
    HfArgumentParser,
    Trainer,
    DataCollatorWithPadding
)
from peft import LoraConfig, get_peft_model, prepare_model_for_kbit_training, TaskType

# 添加項目路徑
sys.path.append(str(Path(__file__).parent.parent))
from src.data_processing.preprocessor import SpiderPreprocessor

@dataclass
class ScriptArguments:
    """訓練腳本參數"""
    config_path: str = field(
        default="config/training_config.yaml",
        metadata={"help": "配置文件路徑"}
    )
    use_wandb: bool = field(
        default=False,
        metadata={"help": "是否使用 Weights & Biases 記錄"}
    )
    wandb_project: str = field(
        default="spider1-text2sql",
        metadata={"help": "W&B 項目名稱"}
    )
    resume_from_checkpoint: Optional[str] = field(
        default=None,
        metadata={"help": "從檢查點恢復訓練"}
    )

def load_config(config_path):
    """載入配置文件"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def setup_model_and_tokenizer(config):
    """設置模型和分詞器（GPU 優化）"""
    print("🔧 正在載入模型和分詞器...")

    # 檢查 GPU
    if not torch.cuda.is_available():
        raise RuntimeError("❌ 未檢測到 GPU！請確認 CUDA 安裝正確。")

    print(f"✅ 檢測到 GPU: {torch.cuda.get_device_name(0)}")
    print(f"   CUDA 版本: {torch.version.cuda}")
    print(f"   可用顯存: {torch.cuda.get_device_properties(0).total_memory / 1024**3:.2f} GB")

    # 4-bit 量化配置
    bnb_config = BitsAndBytesConfig(
        load_in_4bit=config['model']['load_in_4bit'],
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16,
        bnb_4bit_use_double_quant=True,
    )

    # 載入模型
    print(f"📥 載入模型: {config['model']['name']}")
    model = AutoModelForCausalLM.from_pretrained(
        config['model']['name'],
        quantization_config=bnb_config,
        device_map=config['model']['device_map'],
        torch_dtype=torch.bfloat16,
        trust_remote_code=True,
        use_cache=False,
    )

    # 載入分詞器
    tokenizer = AutoTokenizer.from_pretrained(
        config['model']['name'],
        trust_remote_code=True,
        padding_side="right",
        add_eos_token=True,
    )

    # 設置 pad token
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
        model.config.pad_token_id = tokenizer.eos_token_id

    # 準備模型進行 k-bit 訓練
    model = prepare_model_for_kbit_training(model)

    return model, tokenizer

def setup_lora_config(config):
    """設置 LoRA/DoRA 配置"""
    lora_config_dict = config['lora']

    # 檢查是否使用 DoRA
    use_dora = lora_config_dict.get('method', 'lora').lower() == 'dora'

    print(f"🎯 使用 {'DoRA' if use_dora else 'LoRA'} 方法")
    print(f"   Rank: {lora_config_dict['r']}")
    print(f"   Alpha: {lora_config_dict['lora_alpha']}")

    lora_config = LoraConfig(
        r=lora_config_dict['r'],
        lora_alpha=lora_config_dict['lora_alpha'],
        target_modules=lora_config_dict['target_modules'],
        lora_dropout=lora_config_dict['lora_dropout'],
        bias=lora_config_dict['bias'],
        task_type=TaskType.CAUSAL_LM,
        use_rslora=lora_config_dict.get('use_rslora', False),
        use_dora=use_dora,
    )

    return lora_config

def preprocess_function(examples, tokenizer, max_length):
    """
    預處理函數：將 messages 轉換為 tokenized inputs
    """
    texts = []
    for messages in examples['messages']:
        # 使用聊天模板格式化
        text = tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=False
        )
        texts.append(text)

    # Tokenize 文本
    model_inputs = tokenizer(
        texts,
        max_length=max_length,
        truncation=True,
        padding="max_length",  # 直接 padding 到最大長度
        return_tensors=None,
    )

    # 設置 labels（深拷貝避免共享引用）
    import copy
    model_inputs["labels"] = copy.deepcopy(model_inputs["input_ids"])

    return model_inputs

def main():
    # 解析參數
    parser = HfArgumentParser(ScriptArguments)
    script_args = parser.parse_args_into_dataclasses()[0]

    # 載入配置
    config = load_config(script_args.config_path)

    # 初始化 W&B
    if script_args.use_wandb:
        import wandb
        wandb.init(
            project=script_args.wandb_project,
            name=f"spider1-{config['lora']['method']}-r{config['lora']['r']}",
            config=config
        )

    # 設置模型和分詞器
    model, tokenizer = setup_model_and_tokenizer(config)

    # 設置 LoRA
    lora_config = setup_lora_config(config)
    model = get_peft_model(model, lora_config)

    # 打印可訓練參數
    model.print_trainable_parameters()

    # 準備資料集
    print("\n📊 正在載入和處理資料集...")
    preprocessor = SpiderPreprocessor(config)

    # 載入訓練資料
    print("   載入訓練資料...")
    train_data, db_schemas = preprocessor.load_spider_data(
        json_path=config['data']['train_file'],
        tables_path=config['data']['tables_file']
    )

    # 載入驗證集
    print("   載入驗證集...")
    dev_data, _ = preprocessor.load_spider_data(
        json_path=config['data']['dev_file'],
        tables_path=config['data']['tables_file']
    )

    # 準備訓練和驗證資料集
    print("   處理訓練集...")
    train_dataset = preprocessor.prepare_dataset(
        train_data,
        db_schemas,
        augment=True  # 啟用困難樣本加權
    )

    print("   處理驗證集...")
    eval_dataset = preprocessor.prepare_dataset(
        dev_data[:500],  # 使用部分驗證集加快評估
        db_schemas,
        augment=False
    )

    print(f"✅ 原始訓練集大小: {len(train_data)}")
    print(f"✅ 增強後訓練集大小: {len(train_dataset)}")
    print(f"✅ 驗證集大小: {len(eval_dataset)}")

    # Tokenize 資料集
    print("\n   Tokenizing 資料集...")
    max_length = config['data']['max_source_length']

    # 使用 lambda 包裝以傳遞額外參數
    train_dataset = train_dataset.map(
        lambda examples: preprocess_function(examples, tokenizer, max_length),
        batched=True,
        remove_columns=train_dataset.column_names,
        desc="Tokenizing training data"
    )

    eval_dataset = eval_dataset.map(
        lambda examples: preprocess_function(examples, tokenizer, max_length),
        batched=True,
        remove_columns=eval_dataset.column_names,
        desc="Tokenizing evaluation data"
    )

    print(f"✅ Tokenized 訓練集大小: {len(train_dataset)}")
    print(f"✅ Tokenized 驗證集大小: {len(eval_dataset)}")

    # 訓練參數
    training_config = config['training']

    training_args = TrainingArguments(
        output_dir=training_config['output_dir'],
        num_train_epochs=training_config['num_train_epochs'],
        per_device_train_batch_size=training_config['per_device_train_batch_size'],
        per_device_eval_batch_size=training_config['per_device_eval_batch_size'],
        gradient_accumulation_steps=training_config['gradient_accumulation_steps'],
        learning_rate=training_config['learning_rate'],
        weight_decay=training_config['weight_decay'],
        warmup_ratio=training_config['warmup_ratio'],
        lr_scheduler_type=training_config['lr_scheduler_type'],
        optim=training_config['optim'],
        max_grad_norm=training_config['max_grad_norm'],
        gradient_checkpointing=training_config['gradient_checkpointing'],

        # 評估和保存
        eval_strategy=training_config['evaluation_strategy'],
        eval_steps=training_config['eval_steps'],
        save_strategy=training_config['save_strategy'],
        save_steps=training_config['save_steps'],
        save_total_limit=training_config['save_total_limit'],
        load_best_model_at_end=training_config['load_best_model_at_end'],
        metric_for_best_model=training_config['metric_for_best_model'],

        # 日誌
        logging_steps=training_config['logging_steps'],
        logging_dir=training_config['logging_dir'],
        report_to=training_config['report_to'] if script_args.use_wandb else [],

        # 精度
        fp16=training_config['fp16'],
        bf16=training_config['bf16'],

        # 其他
        seed=training_config['seed'],
        ddp_find_unused_parameters=False,
        group_by_length=True,
        dataloader_num_workers=4,
    )

    # 創建自定義 data collator 處理可變長度序列
    data_collator = DataCollatorWithPadding(
        tokenizer=tokenizer,
        padding=True,
        max_length=None,
        pad_to_multiple_of=8,
        return_tensors="pt"
    )

    # 建立訓練器
    print("\n🔨 建立訓練器...")
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=eval_dataset,
        data_collator=data_collator,
    )

    # 開始訓練
    print("\n" + "="*50)
    print("🚀 開始訓練...")
    print("="*50)
    print()

    if script_args.resume_from_checkpoint:
        print(f"📂 從檢查點恢復: {script_args.resume_from_checkpoint}")
        trainer.train(resume_from_checkpoint=script_args.resume_from_checkpoint)
    else:
        trainer.train()

    # 保存最終模型
    print("\n💾 保存最終模型...")
    final_model_path = Path(training_config['output_dir']) / "final_model"
    trainer.save_model(final_model_path)
    tokenizer.save_pretrained(final_model_path)

    print(f"✅ 模型已保存至: {final_model_path}")

    if script_args.use_wandb:
        import wandb
        wandb.finish()

    print("\n" + "="*50)
    print("🎉 訓練完成！")
    print("="*50)
    print(f"\n最終模型保存在: {final_model_path}")
    print("\n下一步：")
    print(f"python scripts/evaluate.py --model_path {final_model_path}")

if __name__ == "__main__":
    main()