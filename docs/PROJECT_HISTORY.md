# WP_M09 Text-to-SQL 專案完整演進紀錄

> 從 2026-02 初始版本到 2026-03-22 的完整發展歷程

---

## 一、專案概覽

| 項目 | 說明 |
|------|------|
| **目標** | 將繁體中文/英文自然語言問句轉換為 WP_M09 資料庫的 T-SQL 查詢 |
| **基底模型** | meta-llama/Llama-3.1-8B-Instruct |
| **微調方法** | DoRA (Weight-Decomposed LoRA)，r=16, alpha=32 |
| **GPU** | RTX 5070 Ti (16GB) |
| **資料庫** | WP_M09 (SQL Server — SHANE\SQLEXPRESS)，7 個 View |

---

## 二、訓練程式演進（16 個版本）

### Phase 1：探索期（02月底 — 03/06）

| 版本 | 訓練集 | 筆數 | Epochs | LR | 方法 | 格式 | 重點 |
|------|--------|------|--------|-----|------|------|------|
| wpm09_only | train_final.json | 660 | 3-7(動態) | 3e-4 | LoRA→DoRA | 純文字 | 最初版本，動態 epoch |
| uploaded | train.json | 721 | 5-10(動態) | 3e-4 | LoRA | 純文字+schema | 加入 schema 資訊 |
| **v0306** | train0306_fixed.json | 1,322 | 7 | 2e-4 | **DoRA** | 純文字 | **首次使用 SFTTrainer + DoRA** |

### Phase 2：Spider 格式標準化（03/08 — 03/15）

| 版本 | 訓練集 | 筆數 | Epochs | LR | 格式 | 重點 |
|------|--------|------|--------|-----|------|------|
| **v0308 DoRA** | train_spider_format_fixed.json | 1,322 | 15 | 1e-4 | Spider 1.0 | **首次 Spider 格式**，LR 降至 1e-4 |
| v0308 LoRA | 同上 | 1,322 | 15 | 2e-4 | Spider 1.0 | LoRA 對照實驗 |
| v0312 | train_spider_WP_M09.json | 700 | 15 | 1e-4 | Spider 1.0 | 重新整理訓練集 |
| v0313 | 同上 | 700 | 15 | 1e-4 | Spider 1.0 | 移除方括號 |
| v0314 | 同上 | 790 | 15 | 1e-4 | Spider 1.0 | +90 筆新資料 |
| v0315 | 同上 | 872 | 15 | 1e-4 | Spider 1.0 | +82 筆，加入修正樣本 |

### Phase 3：最佳模型誕生 + 資料擴增（03/17 — 03/20）

| 版本 | 訓練集 | 筆數 | Epochs | LR | 格式 | 重點 |
|------|--------|------|--------|-----|------|------|
| **v0317** | train_spider_WP_M09_r1.json | 975 | 15 | 1e-4 | Spider 1.0 | **Auto Loop R1 = EM 91.76%** (舊測試集) |
| v0318 | train_spider_WP_M09.json | 1,017 | 15 | 1e-4 | Spider 1.0 | +42 筆 isDel 修正樣本 |
| v0319 | train_claude_en_2000.json | 1,775 | 15 | 1e-4 | Spider 1.0 | **全新英文訓練集（Claude 生成）** |
| v0320 v3 | train_claude_en_2000_v3.json | 1,999 | 10 | 8e-5 | Spider 1.0 | 第三版訓練集，平衡 SQL 模式 |

### Phase 4：合併實驗 + 格式革新（03/21 — 03/22）

| 版本 | 訓練集 | 筆數 | Epochs | LR | SEQ_LEN | 格式 | 重點 |
|------|--------|------|--------|-----|---------|------|------|
| v0321 | spider + claude + v3 合併 | 4,533 | 6 | 6e-5 | 512 | 純文字 | 三資料集去重合併 |
| **v0322** | spider + claude_en | 2,540 | 6 | 5e-5 | **640** | **Chat Template + 單表 Schema** | **格式革新** |

### 訓練格式演進圖

```
純文字 (Table: xxx / Note: xxx / Question: xxx / SQL: xxx)
  │
  ├─ v0306 ~ v0321：所有版本都用此格式
  │   - 只給表名，不給 schema
  │   - 訓練/推論格式一致（都是純文字）
  │   - 但與 inference__query_and_execute_on_db.py 的 Chat Template 格式不一致
  │
  └─ v0322：Llama-3.1 Chat Template
      - System: 單表 CREATE TABLE + 7 表名稱列表 + 表專屬規則
      - User: 問句
      - Assistant: SQL
      - 訓練/推論格式完全一致
```

---

## 三、訓練集演進

### 3.1 訓練集版本

| 階段 | 檔案 | 筆數 | 語言 | 來源 | 狀態 |
|------|------|------|------|------|------|
| 初期 | train_final.json | 660 | EN | 手工+生成 | 已棄用 |
| 初期 | train.json | 721 | EN | 上傳 | 已棄用 |
| 0306 | train0306_fixed.json | 1,322 | EN | 批量生成 | 已棄用 |
| 0308 | train_spider_format_fixed.json | 1,322 | EN | Spider 格式轉換 | 已棄用 |
| 0312-0315 | train_spider_WP_M09.json | 700→872 | EN | 逐步擴增 | 演進中 |
| **0317** | **train_spider_WP_M09.json** | **1,014** | EN 97% / CN 3% | 975 原始 + 修正 | **✅ 現用** |
| 0319 | train_claude_en_2000.json | 1,775 | EN 93% / CN 7% | Claude 生成 | 0320 清理前 |
| **0320** | **train_claude_en_2000.json** | **1,748** | EN 93% / CN 7% | 移除 pNo 日期錯誤 | **✅ 現用** |
| 0320 | train_claude_en_2000_v3.json | 1,999 | CN 99% | 第三版（中文） | 已驗證不佳 |
| 0320 | train_claude_en_2000_v3_clean.json | 1,999 | CN 99% | v3 清理版 | 已驗證不佳 |

### 3.2 現用訓練集特性比較

| 特性 | train_spider_WP_M09 (1,014) | train_claude_en_2000 (1,748) |
|------|-----|------|
| View 分布 | 均勻（132-155/view） | 略不均（206-288/view） |
| 語言 | EN 81% / CN 19% | EN 54% / CN 46% |
| isDel 一致性 | ❌ 不一致（212 筆 dtlIsDel 沒配 isDel） | ✅ 100% 一致 |
| 子查詢 | 55 筆 (5.4%) | **0 筆** ⚠️ |
| AS 別名 | 8.7% | 41.0% |
| 刪除記錄 (isDel='Y') | 38 筆 | 0 筆 |
| 問題多樣性 | 平衡 | COUNT 偏重 (37%) |

### 3.3 資料清理紀錄

| 日期 | 操作 | 影響 |
|------|------|------|
| 03/15 | 加入 103 筆修正樣本 (corrective_fixes_0315.json) | 975 筆 |
| 03/18 | 加入 42 筆 isDel 修正樣本 (corrective_no_isdel.json) | 1,017 筆 |
| **03/20** | 移除 pNo LIKE '日期%' 錯誤樣本 | spider: -3 筆→1,014 / claude: -27 筆→1,748 |
| 03/20 | 移除 WP_vProvider 使用 pvSn 的錯誤樣本 | 已清理 |

---

## 四、驗證集 / 測試集演進

### 4.1 測試集版本

| 檔案 | 筆數 | 類型 | 難度 | View 平衡 | 用途 | 問題 |
|------|------|------|------|-----------|------|------|
| test.json | 182 | 測試 | easy/med/hard/extra | 不明 | 初期測試 | |
| test_.json | 710 | 測試 | 有 + query_type | 不明 | 大型測試集 | |
| test_spider_WP_M09.json | 91 | 測試 | 有 | 13/view | v1 Spider 測試 | |
| **test_spider_WP_M09_v2.json** | **182** | 測試 | easy/med/hard | **26/view** | EM 91.76% 的測試集 | **⚠️ 66.5% 資料洩漏** |

### 4.2 驗證集版本

| 檔案 | 筆數 | 類型 | 難度 | View 欄位 | DB 驗證 | 用途 |
|------|------|------|------|-----------|---------|------|
| validation_claude_en.json | 242 | 驗證 | 無 | 無 | 未驗證 | 初版驗證 |
| val_claude_en_spider.json | 242 | 驗證 | easy/med | 有 | 未驗證 | Spider 格式驗證 |
| val_claude_en_v3.json | 371 | 驗證 | 有 | 無 | 未驗證 | v3 中文驗證 |
| **val_claude_en_spider_v2.json** | **238** | 驗證 | easy/med/hard | **有** | **✅ 全部通過** | **✅ 現用，無資料洩漏** |

### 4.3 舊測試集 vs 新驗證集（關鍵發現）

| | test_spider_WP_M09_v2 (舊) | val_claude_en_spider_v2 (新) |
|---|---|---|
| **SQL 與訓練集重複率** | **66.5%** 🔴 | **0%** ✅ |
| **SQL 模板重複率** | **73.6%** 🔴 | **0%** ✅ |
| 分號 | 100% 有 | 0% 無 |
| 子查詢去重模式 | 0 筆 | 28 筆 (11.8%) |
| DISTINCT 使用率 | 25.3% | 52.5% |

**結論：舊測試集 91.76% EM 是虛假高分（66.5% 資料洩漏）。新驗證集才是真實評估基準。**

---

## 五、所有模型評估結果

### 5.1 舊測試集 (test_spider_WP_M09_v2.json, 182 筆) — 僅 EM

| 日期 | 模型 | EM | EM% | 備註 |
|------|------|-----|------|------|
| 02/28 | (早期) | - | 34.37% | 710 筆測試集 |
| 03/03 | (早期) | - | 53.52% | 710 筆測試集 |
| 03/07 | dora_0307_v2 | 63 | 34.62% | 182 筆 |
| 03/13 | dora_0313 | 47 | 51.65% | 91 筆 |
| 03/14 | dora_0314 | - | 57.14% | 182 筆 |
| 03/15 | dora_0315 | 122 | 67.03% | 182 筆 |
| **03/17** | **dora_0317_r1** | **167** | **91.76%** | **Auto Loop R1（含 66.5% 洩漏）** |

### 5.2 新驗證集 (val_claude_en_spider_v2.json, 238 筆) — EM + EX

| 模型 | 訓練資料 | 筆數 | EM | EM% | EX | EX% |
|------|----------|------|-----|------|-----|------|
| **0317 R1（目前最佳）** | spider + claude_en | 2,762 | 18 | **7.56%** | 105 | **44.12%** |
| 0320 val_v2 | spider + claude_en (重跑) | 2,762 | 14 | 5.88% | 97 | 40.76% |
| 0321 combined | spider + claude + v3 | 4,533 | 4 | 1.68% | 97 | 40.76% |
| 0320 v3 | v3_clean only | 1,999 | 3 | 1.26% | 62 | 26.05% |

### 5.3 新驗證集各 View EX 比較

| View | 0317 R1 | 0320 val | 0321 | 0320 v3 |
|------|---------|----------|------|---------|
| WP_vInventory | **61.8%** | 52.9% | 50.0% | 47.1% |
| WP_vProduct | 52.9% | **64.7%** | **67.6%** | 50.0% |
| WP_vProvider | **50.0%** | 17.6% | 20.6% | 5.9% |
| WP_vAcctOut | 47.1% | **61.8%** | 58.8% | 32.4% |
| WP_vTransfer | **47.1%** | 23.5% | 23.5% | 11.8% |
| WP_vAcctIn | 44.1% | **58.8%** | **58.8%** | 29.4% |
| WP_vOutStock | 5.9% | 5.9% | 5.9% | 5.9% |

**WP_vOutStock 所有模型都只有 5.9%** — 模型無法辨識此表。

### 5.4 新驗證集各難度 EX 比較

| 難度 | 0317 R1 | 0320 val | 0321 | 0320 v3 |
|------|---------|----------|------|---------|
| easy (98) | **52.0%** | 51.0% | 51.0% | 31.6% |
| medium (84) | **42.9%** | 33.3% | 36.9% | 23.8% |
| hard (56) | 32.1% | **33.9%** | 28.6% | 19.6% |

---

## 六、錯誤分析（0317 最佳模型 on 新驗證集）

### 6.1 錯誤類型分布

| 錯誤類型 | 數量 | 佔比 | 說明 |
|----------|------|------|------|
| **錯誤表推斷** | 34 | 25.6% | 其中 31 筆是 OutStock |
| **SELECT 欄位不完整** | 67 | 50.4% | 模型少選欄位 |
| **缺少 DISTINCT** | 51 | 38.3% | 該加 DISTINCT 沒加 |
| **缺少 NULL/非空檢查** | 33 | 24.8% | IS NOT NULL、<> '' |
| **缺少 dtlIsDel** | 30 | 22.6% | 查明細欄位時漏掉 |
| **缺少 isDel** | 27 | 20.3% | 主要是 OutStock 表推斷錯 |
| **多加 dtlIsDel** | 18 | 13.5% | 子查詢去重時不該加 |
| **子查詢去重 0% 成功** | 17 | 12.8% | Claude 訓練集完全沒有此模式 |
| **COUNT(*) vs COUNT(DISTINCT)** | 17 | 12.8% | 應用 DISTINCT 計數 |
| **欄位名稱錯誤** | 20 | 15.0% | pUnit→pUName, price→priceStd |

### 6.2 WP_vOutStock 災難性失敗

- 32/34 失敗案例中，**31 筆預測了錯誤的表名**
- 25 筆用 `WP_M09`（資料庫名），7 筆用 `WP_vProduct`
- 根因：eval 的 `infer_table_from_question()` 關鍵字權重不足

### 6.3 訓練集 vs 驗證集的關鍵缺口

| 模式 | 訓練集 | 驗證集 | 缺口 |
|------|--------|--------|------|
| 子查詢去重 | Spider 55 筆 / Claude 0 筆 | 28 筆 | 🔴 嚴重 |
| OutStock 辨識 | 有資料但 eval 推斷失敗 | 34 筆 | 🔴 嚴重 |
| isDel 一個 vs 兩個 | 混淆 | 明確區分 | 🟡 中等 |
| HAVING | 少量 | 18 筆 | 🟡 中等 |
| NULL/非空檢查 | 少量 | 33 筆 | 🟡 中等 |

---

## 七、評估腳本演進

### 7.1 主要評估腳本

| 檔案 | 版本 | 指標 | 推論格式 | 表推斷 | 用途 |
|------|------|------|----------|--------|------|
| eval__exact_match_accuracy.py | 初期 | EM only | 純文字 | 規則 | 早期快速評估 |
| **eval__em_and_execution_accuracy.py** | v1 | EM + EX | 純文字 | 規則 | **0317 等所有模型評估** |
| **eval__em_and_execution_accuracy_v2.py** | v2 | EM + EX | **Chat Template** | 規則 | **v0322 模型專用** |
| **eval__spider_style.py** | Spider | EM(元件級) + EX | 自動偵測 | 規則/自動 | **Spider 1.0 風格評估** |

### 7.2 Spider 風格評估 vs 舊版差異

| | 舊版 EM（字串匹配） | Spider 風格 EM（元件匹配） |
|---|---|---|
| **比較方式** | 正規化後完整字串比對 | 拆解為 select/where/groupBy/having/orderBy 逐元件比對 |
| **值敏感** | ✅ 敏感（isDel='N' vs isDel='Y' 不同） | ❌ 忽略值（value-agnostic） |
| **DISTINCT 敏感** | ✅ 有無 DISTINCT 不同 | ❌ 忽略 |
| **分號/空格敏感** | ✅ 格式必須一致 | ❌ 不影響 |
| **部分匹配** | 無 | 每個元件分別報告 F1 |
| **難度計算** | 從資料取 | 自動計算（component1/component2/others） |

---

## 八、WP_M09 資料庫商業邏輯

### 8.1 七個 View 的特性

| View | 用途 | isDel | dtlIsDel | 日期篩選 | 特殊規則 |
|------|------|-------|----------|----------|----------|
| WP_vAcctIn | 進貨/應收 | ✅ | ✅ | LEFT(acctInId,8) | SUM(amount) 需子查詢去重 |
| WP_vAcctOut | 出貨/應付 | ✅ | ✅ | LEFT(acctOutId,8) | SUM(amount) 需子查詢去重 |
| WP_vOutStock | 出庫/銷貨 | ✅ | ✅ | LEFT(OutStkId,8) | SUM(amount) 需子查詢去重 |
| WP_vTransfer | 調撥 | ✅ | ✅(幾乎必加) | LEFT(TransferId,8) | 無 header amount，用 SUM(qty) |
| WP_vInventory | 庫存 | ❌ | ❌ | ❌ | pNo 是流水號不是日期 |
| WP_vProduct | 商品 | ❌ | ❌ | ❌ | pNo 是流水號不是日期 |
| WP_vProvider | 供應商 | ❌ | ❌ | ❌ | 用 isStop='N'/'Y' |

### 8.2 核心規則

1. **isDel/dtlIsDel 使用時機**
   - 聚合 header 欄位（amount, xxxId）→ 只要 `isDel='N'`
   - 查詢 detail 欄位（pName, qty, dtlAmt）→ 需要 `isDel='N' AND dtlIsDel='N'`

2. **SUM/AVG 子查詢去重**（Header-Detail View 特有）
   ```sql
   SELECT SUM(amount) FROM (
     SELECT DISTINCT acctInId, amount
     FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N'
   ) sub
   ```

3. **中文字串必須加 N 前綴**
   ```sql
   WHERE memName = N'王小明'
   WHERE pName LIKE N'%玫瑰%'
   ```

4. **日期篩選用 LEFT()**
   ```sql
   WHERE LEFT(acctInId, 8) = '20251205'   -- 特定日期
   WHERE LEFT(acctInId, 6) = '202512'     -- 特定月份
   ```

---

## 九、Spider 1.0 vs WP_M09 差異

| 維度 | Spider 1.0 Dev (1,034) | WP_M09 Val v2 (238) |
|------|------------------------|----------------------|
| 資料庫數 | 20 個 | 1 個 |
| SQL 方言 | SQLite | T-SQL (SQL Server) |
| **JOIN** | **39.5%** | **0%** |
| INTERSECT/UNION/EXCEPT | 7.9% | 0% |
| **DISTINCT** | 8.4% | **52.5%** |
| 子查詢 | 15.4% | 13.4% |
| N 前綴 | 0% | 67.2% |
| isDel 軟刪除 | 0% | 57.1% |
| **核心挑戰** | 跨表 JOIN、集合運算 | 商業邏輯、中文、T-SQL 語法 |

---

## 十、v0322 改進方案

### 10.1 解決的問題

| 問題 | 舊版（v0317-v0321） | v0322 解法 |
|------|---------------------|------------|
| 訓練/推論格式不一致 | 純文字 vs Chat Template | 統一用 Llama-3.1 Chat Template |
| 模型不知道欄位名 | 只給表名 | 給 CREATE TABLE schema |
| OutStock 推斷失敗 | 規則式推斷 | Schema 中列出所有 7 表 |
| 子查詢去重不會 | 訓練集缺少 | Schema 註解中明確教導 |
| isDel 規則混淆 | TABLE_NOTES 過簡 | 表專屬 Rules 明確說明 |

### 10.2 Token 長度方案

| | 初版 v0322（失敗） | 修正版 v0322 |
|---|---|---|
| Schema 策略 | 7 表全放 | **只放目標表** + 7 表名列表 |
| Token 長度 | 2,698-2,798 | **307-455** |
| 超過 MAX_SEQ_LEN | 100% ❌ | **0%** ✅ |
| MAX_SEQ_LEN | 768 | **640** |

### 10.3 執行指令

```bash
# 訓練
python train__dora_spider_v0322.py

# 評估（Chat Template 格式）
python eval__em_and_execution_accuracy_v2.py \
    --model outputs/models/wp_m09_dora_0322_schema/final_model \
    --gold data/wp_m09/val_claude_en_spider_v2.json \
    --output outputs/evaluation_0322_val.json \
    --db-host "SHANE\SQLEXPRESS" --db-trusted

# 評估（Spider 1.0 風格元件級 EM）
python eval__spider_style.py \
    --model outputs/models/wp_m09_dora_0322_schema/final_model \
    --gold data/wp_m09/val_claude_en_spider_v2.json \
    --output outputs/evaluation_0322_spider_style.json \
    --db-host "SHANE\SQLEXPRESS" --db-trusted
```

---

## 十一、已訓練模型清單（27 個）

| # | 目錄名 | 日期 | 訓練集筆數 | 方法 | Epochs | LR | 最佳 EM |
|---|--------|------|-----------|------|--------|-----|---------|
| 1 | spider1-llama31-dora | 早期 | - | DoRA | - | - | - |
| 2 | spider1-llama31-dora-v3 | 早期 | - | DoRA | - | - | - |
| 3 | wp_m09_model | 02月 | 1,360 | LoRA | 3 | 2e-5 | - |
| 4 | wp_m09_from_scratch | 02月 | 5,258 | LoRA | 5 | 3e-4 | - |
| 5 | wp_m09_0228_failed | 02/28 | 4,152 | LoRA | 3 | 3e-4 | - |
| 6 | wp_m09_0301 | 03/01 | 12,579 | LoRA | 3 | 3e-4 | - |
| 7 | wp_m09_0303 | 03/03 | 660 | LoRA | 7 | 3e-4 | 53.52% |
| 8 | wp_m09_0304 | 03/04 | 660 | LoRA | 7 | 3e-4 | - |
| 9 | wp_m09_uploaded_0305 | 03/05 | 721 | LoRA | 7 | 3e-4 | - |
| 10 | wp_m09_dora_0306 | 03/06 | 1,322 | DoRA | 7 | 2e-4 | - |
| 11 | wp_m09_dora_0307 | 03/07 | 1,322 | DoRA | 7 | 2e-4 | - |
| 12 | wp_m09_dora_0307_v2 | 03/07 | 1,322 | DoRA | 7 | 2e-4 | 34.62% |
| 13 | wp_m09_dora_0308_spider | 03/08 | 1,322 | DoRA | 20 | 1e-4 | - |
| 14 | wp_m09_dora_0309_spider | 03/09 | 1,322 | DoRA | 15 | 1e-4 | - |
| 15 | wp_m09_lora_0310_spider | 03/10 | 1,322 | LoRA | 15 | 2e-4 | 34.62% |
| 16 | wp_m09_dora_0312_spider | 03/12 | 700 | DoRA | 15 | 1e-4 | - |
| 17 | wp_m09_dora_0313_spider | 03/13 | 700 | DoRA | 15 | 1e-4 | 51.65% |
| 18 | wp_m09_dora_0314_spider | 03/14 | 790 | DoRA | 15 | 1e-4 | 57.14% |
| 19 | wp_m09_dora_0315_spider | 03/15 | 872 | DoRA | 15 | 1e-4 | 67.03% |
| 20 | **wp_m09_dora_0317_spider_r1** | **03/17** | **975** | **DoRA** | **15** | **1e-4** | **91.76%** ⭐ |
| 21 | wp_m09_dora_0318_spider | 03/18 | 1,017 | DoRA | 15 | 1e-4 | - |
| 22 | wp_m09_dora_0319_spider | 03/19 | 1,775 | DoRA | 15 | 1e-4 | - |
| 23 | wp_m09_dora_0320_spider | 03/20 | 1,748 | DoRA | 15 | 1e-4 | - |
| 24 | wp_m09_dora_0320_spider_v3 | 03/20 | 1,999 | DoRA | 10 | 8e-5 | - |
| 25 | wp_m09_dora_0321_combined | 03/21 | 4,533 | DoRA | 6 | 6e-5 | - |
| 26 | wp_m09_pure | 早期 | - | - | - | - | - |
| 27 | wp_m09_dora_0322_schema | 03/22 | 2,540 | DoRA | 6 | 5e-5 | 訓練中 |

---

## 十二、關鍵里程碑

| 日期 | 里程碑 | 說明 |
|------|--------|------|
| 02月底 | 專案啟動 | 首次 LoRA 微調嘗試 |
| 03/06 | 轉用 DoRA + SFTTrainer | 訓練框架穩定 |
| 03/08 | 採用 Spider 1.0 格式 | 標準化資料格式 |
| 03/15 | EM 67% | 修正樣本策略生效 |
| **03/17** | **EM 91.76%（舊測試集）** | **看似達標，實為資料洩漏** |
| 03/20 | 建立新驗證集 v2 | 238 筆，無洩漏，全部 DB 驗證 |
| 03/20 | 移除 pNo 日期錯誤 | 訓練資料品質修正 |
| 03/21 | 發現 91% 是虛假高分 | 舊測試集 66.5% SQL 與訓練集重複 |
| 03/21 | 真實評估基準建立 | 0317 最佳模型：EM 7.56% / EX 44.12% |
| **03/22** | **v0322 格式革新** | Chat Template + Schema，解決 5 大根因 |
| 03/22 | Spider 風格評估建立 | 元件級 EM + Partial Match |

---

*最後更新：2026-03-22*
