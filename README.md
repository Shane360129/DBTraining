# Spider 1.0 Text-to-SQL 訓練專案

## 專案目標
使用 Llama 3.1 8B + DoRA 在 Spider 1.0 資料集上進行微調，目標超越 72% 的準確率。

## 專案結構
```
spider1_training/
├── config/                    # 配置文件
│   └── training_config.yaml
├── data/
│   └── spider/               # Spider 資料集
├── src/
│   └── data_processing/      # 資料處理模組
│       ├── prompt_builder.py
│       └── preprocessor.py
├── scripts/                  # 執行腳本
│   ├── download_spider.py
│   ├── check_gpu.py
│   ├── train.py
│   └── evaluate.py
├── outputs/                  # 輸出文件
│   ├── models/              # 訓練好的模型
│   ├── predictions/         # 預測結果
│   └── logs/                # 訓練日誌
└── requirements.txt          # 依賴列表
```

## 快速開始

### 1. 安裝依賴
```bash
pip install -r requirements.txt
```

### 2. 下載 Spider 資料集
```bash
python scripts/download_spider.py
```

### 3. 檢查 GPU
```bash
python scripts/check_gpu.py
```

### 4. 開始訓練
```bash
python scripts/train.py
```

### 5. 評估模型
```bash
python scripts/evaluate.py --model_path outputs/models/spider1-llama31-dora/final_model
```

## 訓練配置

修改 `config/training_config.yaml` 來調整訓練參數：
- 模型選擇：Llama 3.1/3.2 8B
- LoRA/DoRA 配置
- 訓練超參數
- 資料處理設置

## 優化策略

| 優化方法 | 預期提升 | 說明 |
|---------|---------|------|
| Llama 3.1 8B (基準) | - | 比舊版更強 |
| DoRA (vs QLoRA) | +2-3% | 更好的參數效率 |
| CoT Prompting | +2-3% | 思維鏈推理 |
| 資料增強 | +1-2% | 平衡難度分佈 |
| Rank 32 | +1-2% | 提升表達能力 |
| **預期總提升** | **72% → 78-82%** | |

## 訓練監控

### 使用 TensorBoard
```bash
tensorboard --logdir outputs/logs --port 6006
```
瀏覽器打開: http://localhost:6006

### 使用 Weights & Biases
```bash
# 首次使用需登入
wandb login

# 訓練時啟用
python scripts/train.py --use_wandb True
```

## 系統需求

- **GPU**: 至少 16GB 顯存 (RTX 3090, 4090, 5070 Ti 等)
- **RAM**: 32GB 以上推薦
- **磁碟**: 至少 50GB 可用空間
- **CUDA**: 11.8 或 12.1

## 後續計劃

1. ✅ **階段一**: 英文 Spider 資料集訓練 (目標 >72%)
2. 🔄 **階段二**: 繁體中文適配
3. 🔄 **階段三**: 公司資料庫實作

## 常見問題

### Q: 訓練時顯存不足怎麼辦？
A: 在 `config/training_config.yaml` 中調整：
```yaml
training:
  per_device_train_batch_size: 2  # 降低批次大小
  gradient_accumulation_steps: 16  # 增加累積步數
```

### Q: 訓練需要多長時間？
A: 使用 RTX 4090 約 4-6 小時完成 4 epochs

### Q: 如何從檢查點恢復訓練？
A: 
```bash
python scripts/train.py --resume_from_checkpoint outputs/models/spider1-llama31-dora/checkpoint-500
```

## 授權

本專案使用 Spider 資料集，請遵守其授權條款。

## 聯絡方式

如有問題請提交 Issue。