# WP_M09 Text-to-SQL 微調專案

## 專案目標
使用 Llama-3.1-8B-Instruct + DoRA 微調，將自然語言問句轉換為 WP_M09 資料庫的 T-SQL 查詢。

## 目前狀態（2026-03-26）
- **當前訓練腳本**：`train/train__9views_20k_v0325.py`（9 Views × 20K，Blackwell 加速版）
- **訓練資料**：`data/wp_m09/spider_format_light.json`（~20K 筆，80/10/10 分層切分）
- **切分後驗證集**：`data/wp_m09/split_9views_20k_val.json`
- **切分後測試集**：`data/wp_m09/split_9views_20k_test.json`
- **輸出目錄**：`outputs/models/9views_20k_0325/final_model/`
- **狀態**：訓練中，30% 時 loss=0.0216 / token_acc=0.998 疑似過擬合，已停止並加入 val loss 監控 + early stopping，準備重新訓練

### 當前訓練超參數（v0325 防過擬合 v2）
| 參數 | 值 | 說明 |
|------|-----|------|
| Epochs | 1（上限） | 靠 early stopping 提前停 |
| LR | 2e-6 | 極低 LR，0.25 epoch 就飽和所以再降 |
| LoRA r | 8 | 減少微調容量 |
| Dropout | 0.15 | 強正則化 |
| Weight decay | 0.05 | 強權重衰減 |
| Batch | 4×4=16 | effective batch |
| MAX_SEQ_LEN | 1536 | 9 views schema 需較長序列 |
| Early Stopping | patience=2 | 連續 2 次沒改善即停 |
| Eval 頻率 | 每 epoch 10 次 | ~125 steps eval 一次，細緻偵測拐點 |
| Eval 抽樣 | 200 筆 | 避免 eval 太久 |

### 舊版訓練（已不使用）
- `train/train__dora_spider_v0318.py` — 7 Views × 1K 資料，已被 v0325 取代

## 資料庫：WP_M09（SQL Server — SHANE\SQLEXPRESS）
9 個 View：

| View | isDel? | 日期篩選 | 備註 |
|------|--------|----------|------|
| WP_vAcctIn | ✅ | LEFT(acctInId,8) | 應收帳款 |
| WP_vAcctOut | ✅ | LEFT(acctOutId,8) | 應付帳款 |
| WP_vOutStock | ✅ | LEFT(OutStkId,8) | 銷售/出庫 |
| WP_vTransfer | ✅ | LEFT(TransferId,8) | 調撥 |
| WP_vMemberDeposit | ✅ | endDate（到期日） | 會員儲值，isDel='N' |
| WP_vPdCombine | ✅ | — | 組合商品，isDel='N' |
| WP_vInventory | ❌ | 無 | pNo=流水號非日期 |
| WP_vProduct | ❌ | 無 | pNo=流水號非日期 |
| WP_vProvider | ❌ | 無 | 用 isStop 判停用，pvSn 僅 JOIN 用 |

### 關鍵規則
- pNo 是商品流水號（1, 2, 3...），**不是日期**
- 無 isDel 的 View（Inventory/Product）**嚴禁**加 isDel
- Provider 用 `isStop='N'` 判斷啟用，SELECT pvId（非 pvSn）
- SUM/AVG header amount 必須用 subquery dedup，禁用 SUM(DISTINCT amount)
- isSale: 0=正常, 1=停進, 2=停銷, 3=停進停銷

## 常用指令

### 訓練（當前版本）
```bash
python train/train__9views_20k_v0325.py
# 可選參數：--no-rules（ablation）、--epochs 2、--lr 1e-5
```

### 評估
```bash
python eval/eval__9views_v0325.py \
  --model outputs/models/9views_20k_0325/final_model \
  --gold data/wp_m09/split_9views_20k_test.json \
  --output outputs/eval_9views_20k_0325_test.json
```

### 查詢資料庫（互動模式）
```bash
python inference/inference__query_and_execute_on_db.py
```

## 資料夾結構
| 資料夾 | 前綴 | 功能 |
|--------|------|------|
| `auto/` | `auto__` | 自動化流程 |
| `eval/` | `eval__` | 模型評估 |
| `inference/` | `inference__` | 推論/查詢 |
| `schema/` | `schema__` | Schema 檢查 |
| `train/` | `train__` | 模型訓練 |
| `traindata_clean/` | `traindata_clean__` | 訓練資料清理 |
| `traindata_gen/` | `traindata_gen__` | 訓練資料生成 |
| `traindata_prep/` | `traindata_prep__` | 訓練資料準備 |
| `docs/` | — | 專案文件 |

## 重要提醒
- 訓練硬體：RTX 5070 Ti（16GB VRAM），v0325 預估 2-3 小時
- 模型儲存在 `outputs/models/` 下
- **每次新增訓練腳本時必須更新此 CLAUDE.md**
