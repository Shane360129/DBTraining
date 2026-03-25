# WP_M09 Text-to-SQL 微調專案

## 專案目標
使用 Llama-3.1-8B-Instruct + DoRA 微調，將繁體中文自然語言問句轉換為 WP_M09 資料庫的 T-SQL 查詢。目標 EM ≥ 80%（已達成：Auto Loop R1 = **91.76%**）。

## 目前狀態（2026-03-20）
- **最佳模型**：`outputs/models/wp_m09_dora_0317_spider_r1/final_model`（EM 91.76%）
- **訓練集**：`data/wp_m09/train_spider_WP_M09.json`（1014 筆）、`data/wp_m09/train_claude_en_2000.json`（1748 筆）
- **驗證集**：`data/wp_m09/val_claude_en_spider_v2.json`（238 筆，含 easy/medium/hard）
- **測試集**：`data/wp_m09/test_spider_WP_M09_v2.json`（182 筆）

## 資料庫：WP_M09（SQL Server — SHANE\SQLEXPRESS）
7 個 View，其中 isDel/dtlIsDel 欄位分布如下：

| View | 有 isDel? | 說明 |
|------|-----------|------|
| WP_vAcctIn | ✅ 有 | LEFT(acctInId,8) 篩日期 |
| WP_vAcctOut | ✅ 有 | LEFT(acctOutId,8) 篩日期 |
| WP_vOutStock | ✅ 有 | LEFT(OutStkId,8) 篩日期 |
| WP_vTransfer | ✅ 有 | LEFT(TransferId,8) 篩日期 |
| WP_vInventory | ❌ 無 | 無日期篩選欄位 |
| WP_vProduct | ❌ 無 | 無日期篩選欄位 |
| WP_vProvider | ❌ 無 | 供應商主表，用 pvSn JOIN；用 isStop 判斷是否停用，pvDiscount 為折扣欄位 |

### ⚠️ 重要：pNo 是商品流水號（1, 2, 3...），不是日期！
- pNo 不可用 `pNo LIKE 'YYYYMMDD%'` 來篩日期
- WP_vInventory / WP_vProduct / WP_vProvider 本身沒有日期篩選機制

## 最近修正（2026-03-20）
1. 移除訓練集中錯誤的 `pNo LIKE '日期%'` 樣本（train_spider_WP_M09: -3 筆, train_claude_en_2000: -27 筆）
2. 建立新版驗證集 `val_claude_en_spider_v2.json`（238 筆，全部通過 DB 執行驗證）
3. 刪除已不使用的一次性資料準備腳本

## 常用指令

### 查詢資料庫（互動模式）
```bash
python inference__query_and_execute_on_db.py
```

### 重新訓練（修正版）
```bash
python train__dora_spider_v0318.py
```

### 評估最新模型
```bash
python eval__em_and_execution_accuracy.py \
  --model outputs/models/wp_m09_dora_0317_spider_r1/final_model \
  --gold data/wp_m09/test_spider_WP_M09_v2.json \
  --output outputs/evaluation_0318_v1.json
```

### 自動訓練迴圈（目標 EM ≥ 80%）
```bash
python auto__train_loop_until_target_em.py
```

## 檔案命名規則
`分類前綴__功能描述.py`

| 前綴 | 功能 |
|------|------|
| `data_prep__` | 資料準備 |
| `schema__` | Schema 檢查 |
| `traindata_gen__` | 訓練資料生成 |
| `traindata_clean__` | 訓練資料清理 |
| `traindata_merge__` | 訓練資料合併 |
| `train__` | 模型訓練 |
| `eval__` | 模型評估 |
| `debug__` | 失敗診斷 |
| `auto__` | 自動化流程 |
| `inference__` | 推論/查詢 |

## 重要提醒
- 訓練需要 RTX 4090，約 4-6 小時
- 模型儲存在 `outputs/models/` 下
- 評估結果在 `outputs/evaluation_*.json`
- loop_state.json 記錄自動訓練迴圈的最佳結果
