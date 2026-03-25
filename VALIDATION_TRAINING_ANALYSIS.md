# 驗證集 & 訓練集分析報告（2026-03-21）

## 一、所有模型在驗證集的結果統整

驗證集：`val_claude_en_spider_v2.json`（238 筆，每 View 34 筆，easy 98 / medium 84 / hard 56）

### 1.1 整體指標

| 模型 | 訓練資料 | 筆數 | EM | EM% | EX | EX% |
|------|----------|------|-----|------|-----|------|
| 0317 R1（最佳） | spider + claude_en | 2,762 | 18 | **7.56%** | 105 | **44.12%** |
| 0320 val_v2 | spider + claude_en (重跑) | 2,762 | 14 | 5.88% | 97 | 40.76% |
| **0321 combined** | spider + claude_en + v3 | 4,533 | 4 | 1.68% | 97 | 40.76% |
| 0320 v3 | v3_clean only | 1,999 | 3 | 1.26% | 62 | 26.05% |

### 1.2 各 View EX 比較

| View | 0317 R1 | 0320 val_v2 | 0321 combined | 0320 v3 |
|------|---------|-------------|---------------|---------|
| WP_vAcctIn | 15 (44.1%) | 20 (58.8%) | 20 (58.8%) | 10 (29.4%) |
| WP_vAcctOut | 16 (47.1%) | 21 (61.8%) | 20 (58.8%) | 11 (32.4%) |
| WP_vOutStock | **2 (5.9%)** | **2 (5.9%)** | **2 (5.9%)** | **2 (5.9%)** |
| WP_vTransfer | 16 (47.1%) | 8 (23.5%) | 8 (23.5%) | 4 (11.8%) |
| WP_vInventory | 21 (61.8%) | 18 (52.9%) | 17 (50.0%) | 16 (47.1%) |
| WP_vProduct | 18 (52.9%) | 22 (64.7%) | 23 (67.6%) | 17 (50.0%) |
| WP_vProvider | 17 (50.0%) | 6 (17.6%) | 7 (20.6%) | 2 (5.9%) |

### 1.3 各難度 EX 比較

| 難度 | 0317 R1 | 0320 val_v2 | 0321 combined | 0320 v3 |
|------|---------|-------------|---------------|---------|
| easy (98) | 51 (52.0%) | 50 (51.0%) | 50 (51.0%) | 31 (31.6%) |
| medium (84) | 36 (42.9%) | 28 (33.3%) | 31 (36.9%) | 20 (23.8%) |
| hard (56) | 18 (32.1%) | 19 (33.9%) | 16 (28.6%) | 11 (19.6%) |

---

## 二、為何分數有落差？— 錯誤分析

### 2.1 WP_vOutStock 全面崩潰（所有模型 EX = 5.9%）

**根因：模型不認識 WP_vOutStock 這張表。**

- 32/34 個失敗案例中，**31 筆預測了錯誤的表名**（25 筆用 `WP_M09` 資料庫名、7 筆用 `WP_vProduct`）
- 只有 1 筆正確定位到 `WP_vOutStock`，但欄位名稱也寫錯
- 驗證集問句用 "outbound stock"、"sales order"、"member" 等詞描述 OutStock
- 但 `infer_table_from_question()` 的關鍵字權重不足以區分 OutStock vs AcctIn（兩者都有 memName、amount）

### 2.2 子查詢去重模式 0% 成功率

驗證集中有 **28 筆**需要此模式（22 SUM + 6 AVG）：
```sql
-- 正確（去重 header amount）
SELECT SUM(amount) FROM (
  SELECT DISTINCT acctInId, amount
  FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N'
) sub

-- 模型預測（錯誤，重複計算）
SELECT SUM(amount) FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N'
```

**原因：Claude 訓練集 (1,748 筆) 完全沒有子查詢！Spider 訓練集僅 55 筆 (5.4%) 有子查詢。**

### 2.3 isDel / dtlIsDel 使用時機混淆

| 錯誤類型 | 數量 | 說明 |
|----------|------|------|
| 該加 dtlIsDel 卻沒加 | 30 筆 | 查詢明細欄位（pName, qty）時漏掉 |
| 不該加 dtlIsDel 卻多加 | 18 筆 | SUM(amount) 子查詢去重時只需 isDel |
| 兩者都沒加 | 27 筆 | 主要是 OutStock 表推斷錯誤導致 |

**規則**：
- 聚合 header 欄位（amount, acctInId）→ 只要 `isDel='N'`
- 查詢 detail 欄位（pName, qty, dtlAmt）→ 需要 `isDel='N' AND dtlIsDel='N'`

### 2.4 SELECT 欄位不完整（67 筆錯誤）

模型傾向少選欄位：
```sql
-- Gold: SELECT pNo, pName, priceStd FROM ...
-- Pred: SELECT pNo, pName FROM ...（漏掉 priceStd）
```

### 2.5 欄位名稱錯誤（20 筆）

| 模型預測 | 正確欄位 | View |
|----------|----------|------|
| pUnit | pUName | Inventory, Product |
| price | priceStd | Product |
| amt | amount | AcctIn, AcctOut |
| pvTel/pvFax | fax, email | Provider |
| unitType | pUName | Product |

### 2.6 COUNT(*) vs COUNT(DISTINCT xxxId)（17 筆）

```sql
-- Gold: SELECT COUNT(DISTINCT acctInId) ...   → 計算不重複的單據數
-- Pred: SELECT COUNT(*) ...                    → 計算所有明細列數（錯誤）
```

### 2.7 模型之間差異原因

| 比較 | 原因分析 |
|------|----------|
| **0317 > 0321** | 加入 v3 資料引入雜訊（v3 資料風格不一致、模板化嚴重、缺少子查詢），反而拖累表現 |
| **0317 > 0320_val_v2** | 0317 R1 的 Provider (50%) 和 Transfer (47.1%) 遠優於 0320，可能是隨機種子差異 |
| **0320_v3 墊底** | 僅用 v3 資料訓練，COUNT 偏重 (37%)，無子查詢，isDel 覆蓋率問題 |
| **所有模型 OutStock = 5.9%** | 系統性問題：模型不認識此表，且 eval 的 table inference 也有漏洞 |

---

## 三、訓練集資料特性

### 3.1 Spider 訓練集（1,014 筆）

| 特徵 | 數值 |
|------|------|
| View 分布 | 均勻（132-155 筆/view） |
| 語言 | EN 81% / CN 19% |
| isDel 一致性 | ❌ 不一致（212 筆有 dtlIsDel 卻沒 isDel） |
| 子查詢 | 55 筆 (5.4%) |
| AS 別名 | 8.7% |
| 已刪除記錄 (isDel='Y') | 38 筆 |

### 3.2 Claude 訓練集（1,748 筆）

| 特徵 | 數值 |
|------|------|
| View 分布 | 略不均（206-288 筆/view） |
| 語言 | EN 54% / CN 46% |
| isDel 一致性 | ✅ 100% 一致 |
| 子查詢 | **0 筆** ⚠️ |
| AS 別名 | 41.0% |
| 已刪除記錄 | 0 筆 |
| 問題多樣性 | COUNT 偏重 (37%) |

### 3.3 訓練集 vs 驗證集的關鍵缺口

| 模式 | 訓練集覆蓋 | 驗證集需求 | 缺口嚴重度 |
|------|------------|------------|-----------|
| 子查詢去重 | Spider 55 筆 / Claude 0 筆 | 28 筆 (11.8%) | 🔴 嚴重 |
| OutStock 表辨識 | 有但 eval 推斷失敗 | 34 筆 (14.3%) | 🔴 嚴重 |
| isDel 只加一個 vs 兩個 | 混淆 | 明確區分 | 🟡 中等 |
| 完整 SELECT 欄位 | 不夠明確 | 要求完整 | 🟡 中等 |
| HAVING 子句 | 少量 | 18 筆 (7.6%) | 🟡 中等 |
| NULL/非空檢查 | 少量 | 33 筆 (13.9%) | 🟡 中等 |

---

## 四、新訓練格式設計方案

### 問題診斷：當前格式的缺陷

1. **訓練用純文字格式，推論用 Chat Template** → format mismatch
2. **只給表名，不給 schema** → 模型無法學習正確欄位名
3. **TABLE_NOTES 過於簡略** → isDel/dtlIsDel 使用時機不明確
4. **Eval 的 table inference 是 rule-based** → OutStock 幾乎全部推斷錯誤
5. **缺少子查詢去重的訓練樣本** → 模型完全不會此模式

### 新格式：Schema-Aware Chat Template

改用 Llama-3.1 Chat Template + 完整 Schema + 商業邏輯規則。

見 `train__dora_spider_v0322.py` 的實作。
