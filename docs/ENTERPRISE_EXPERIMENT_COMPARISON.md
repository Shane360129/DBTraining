# WP_M09 企業 Text-to-SQL 落地實驗 — 版本比較與商業邏輯文件

> 文件建立：2026-03-22
> 目的：完整記錄 Enterprise v0322 與先前版本的差異，含商業邏輯、訓練集分析、方法論對比
> 原則：所有數據皆來自實際程式碼與訓練紀錄，無任何推測或虛構

---

## 一、WP_M09 資料庫商業邏輯

### 1.1 資料庫概觀

| 項目 | 內容 |
|------|------|
| 資料庫名稱 | WP_M09 |
| 資料庫引擎 | SQL Server（SHANE\SQLEXPRESS） |
| 資料表數量 | 7 個 View（檢視表） |
| SQL 方言 | T-SQL |
| 表名前綴 | `WP_M09.dbo.<ViewName>` |
| 中文字串 | 必須加 `N` 前綴，如 `N'王小明'` |

### 1.2 七個 View 詳細說明

#### 📊 有 isDel 欄位的 View（4 個）— 有日期篩選能力

| View | 功能 | 主鍵 ID | 日期篩選方式 | header 金額 | 特殊欄位 |
|------|------|---------|-------------|-------------|----------|
| **WP_vAcctIn** | 應收帳款 | acctInId | `LEFT(acctInId,8)='YYYYMMDD'` | amount | memName（會員）, pName（商品） |
| **WP_vAcctOut** | 應付帳款 | acctOutId | `LEFT(acctOutId,8)='YYYYMMDD'` | amount | pvName（供應商）, payType（付款方式） |
| **WP_vOutStock** | 銷貨出庫 | OutStkId | `LEFT(OutStkId,8)='YYYYMMDD'` | amount, tax | memName（會員）, outType, dtlDiscnt |
| **WP_vTransfer** | 調撥 | TransferId | `LEFT(TransferId,8)='YYYYMMDD'` | ❌ 無 header amount | fWhName（來源倉）, tfWhName（目標倉）, costAvg |

#### 📊 無 isDel 欄位的 View（3 個）— 無日期篩選能力

| View | 功能 | 特殊邏輯 | 關鍵欄位 |
|------|------|---------|----------|
| **WP_vInventory** | 庫存 | pNo 是流水號（1,2,3...），**不是日期** | qtyNow（現有量）, qtySafe（安全量）, WarehouseName |
| **WP_vProduct** | 商品 | pNo 是流水號，**不是日期** | priceStd, priceLow, priceMem, costStd, costAvg |
| **WP_vProvider** | 供應商 | 用 `isStop='N'/'Y'` 判斷停用狀態（非 isDel） | pvDiscount（折扣）, pvSn（用於 JOIN） |

### 1.3 核心商業規則

#### 規則 1：isDel / dtlIsDel 過濾邏輯

```
isDel 有的 View（AcctIn, AcctOut, OutStock, Transfer）：
  - 查詢 header 級別欄位（如 orderId, amount, date）→ 只需 isDel='N'
  - 查詢 detail 級別欄位（如 pName, qty, dtlAmt）→ 需 isDel='N' AND dtlIsDel='N'

isDel 沒有的 View（Inventory, Product, Provider）：
  - 絕對不能加 isDel 或 dtlIsDel 條件
  - Provider 使用 isStop='N'/'Y' 判斷是否停用
```

#### 規則 2：子查詢去重（Subquery Dedup）

```sql
-- ❌ 錯誤：header amount 直接 SUM 會因 detail 行數重複加總
SELECT SUM(amount) FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N'

-- ✅ 正確：先用子查詢取 DISTINCT header，再 SUM
SELECT SUM(amount) FROM (
  SELECT DISTINCT OutStkId, amount
  FROM WP_M09.dbo.WP_vOutStock
  WHERE isDel='N'
) sub
```

**適用場景**：對 header 金額（amount, tax, transAmt）做 SUM/AVG 時，因為一個 header 對應多個 detail 行，直接聚合會重複計算。

#### 規則 3：日期篩選

```sql
-- 篩選特定日期（8碼）
WHERE LEFT(acctInId, 8) = '20251205'

-- 篩選月份（6碼）
WHERE LEFT(OutStkId, 6) = '202512'

-- 篩選年份（4碼）
WHERE LEFT(TransferId, 4) = '2025'
```

⚠️ **pNo 是商品流水號（1, 2, 3...），絕對不可用 `pNo LIKE 'YYYYMMDD%'` 來篩選日期**

#### 規則 4：Transfer 特殊性

- WP_vTransfer **沒有** header amount 欄位
- 計算金額需用 `SUM(costAvg * qty)`
- 有 fWhName（來源倉庫）和 tfWhName（目標倉庫）兩個倉庫欄位

---

## 二、訓練版本比較

### 2.1 關鍵版本一覽

| 維度 | v0317 (Auto Loop R1) | v0322 (Chat Template) | Enterprise v0322 |
|------|----------------------|----------------------|-------------------|
| **訓練腳本** | `train__dora_spider_v0317.py` | `train__dora_chat_v0322.py` | `train__enterprise_v0322.py` |
| **方法論** | Spider-format（Schema+Q→SQL） | Chat Template + 單表 schema | **Spider/BIRD-style 全 schema** |
| **Prompt 格式** | `Schema:\n{schema}\nQuestion:\n{q}\nSQL:\n{sql}` | Llama Chat Template + 單目標表 schema | Llama Chat Template + **全 7 表 schema** |
| **Schema 策略** | 單表 CREATE TABLE | 單表 CREATE TABLE（~129 tokens） | **緊湊全 7 表列表**（~937 tokens） |
| **表選擇機制** | 推論時靠 keyword 推斷 | 推論時靠 keyword 推斷 | **模型自行從 schema 選表** |
| **商業規則** | ❌ 無 | ❌ 無 | ✅ BIRD evidence 概念 |
| **資料擴增** | ❌ 無 | ❌ 無 | ✅ 50 筆（5 類弱模式） |
| **MAX_SEQ_LEN** | 512 | 1536 | 1280 |
| **Batch（effective）** | 4×4=16 | 2×8=16 | 2×8=16 |
| **Epochs** | 6 | 6 | 6 |
| **LR** | 5e-5 | 5e-5 | 5e-5 |
| **DoRA** | r=16, α=32 | r=16, α=32 | r=16, α=32 |
| **Ablation 支援** | ❌ | ❌ | ✅ `--schema`/`--no-rules`/`--no-augment` |

### 2.2 Prompt 格式差異（核心差異）

#### v0317：Spider Format（純文字拼接）

```
Schema:
CREATE TABLE WP_vOutStock (
  sn INT, OutStkId NVARCHAR, ...
);

Question:
What is the total sales amount in December 2025?

SQL:
SELECT SUM(amount) FROM ...
```

- 每個樣本只含**目標表**的 schema
- 推論時需要先用 keyword 推斷目標表，再組裝 prompt
- 如果推斷錯誤（如 OutStock 被推斷為 AcctIn），整個 SQL 必然錯誤

#### v0322：Chat Template（單表 schema）

```
<|begin_of_text|><|start_header_id|>system<|end_header_id|>
You are an expert T-SQL assistant...
CREATE TABLE WP_vOutStock (sn INT, OutStkId NVARCHAR, ...)
<|start_header_id|>user<|end_header_id|>
What is the total sales amount in December 2025?
<|start_header_id|>assistant<|end_header_id|>
SELECT SUM(amount) FROM ...
```

- 使用 Llama-3.1 原生 Chat Template（更接近預訓練格式）
- **仍然是單表 schema** → 推論時仍需 keyword 推斷
- Token 長度 ~129 tokens（schema 部分）

#### Enterprise v0322：全 Schema + 商業規則

```
<|begin_of_text|><|start_header_id|>system<|end_header_id|>
You are an expert T-SQL assistant for WP_M09 database...

-- WP_M09 (SQL Server T-SQL). Prefix: WP_M09.dbo.<View>. Chinese: N'str'.

WP_vAcctIn(sn, acctInId, ...) -- Receivable. LEFT(acctInId,8)=date. isDel+dtlIsDel.
WP_vAcctOut(sn, acctOutId, ...) -- Payable. LEFT(acctOutId,8)=date. isDel+dtlIsDel.
WP_vOutStock(sn, OutStkId, ...) -- Sales/Outbound. LEFT(OutStkId,8)=date. isDel+dtlIsDel.
WP_vTransfer(sn, TransferId, ...) -- Transfer. No header amount.
WP_vInventory(whSn, ...) -- NO isDel. NO date. pNo=seq#.
WP_vProduct(pNo, ...) -- NO isDel. NO date. pNo=seq#.
WP_vProvider(sn, ...) -- Supplier. NO isDel. isStop=N/Y.

Rules:
1. isDel views: isDel='N' header, +dtlIsDel='N' detail columns.
2. No-isDel views: NEVER add isDel. Provider: isStop.
3. SUM/AVG header amount: subquery dedup.
4. Date: LEFT(xxxId,8). pNo=seq, NOT date.

<|start_header_id|>user<|end_header_id|>
What is the total sales amount in December 2025?
<|start_header_id|>assistant<|end_header_id|>
SELECT SUM(amount) FROM (SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND LEFT(OutStkId,6)='202512') sub
```

- **全 7 表 schema** 放在每個樣本的 system prompt → 模型自行學會選正確的表
- **緊湊格式**：去除資料型別，只列欄位名 → ~937 tokens（vs CREATE TABLE 1,614 tokens）
- **商業規則**：統一 4 條規則，類似 BIRD benchmark 的 evidence 欄位
- **推論時完全不需要 keyword 推斷** → 消除 OutStock 31/32 推斷失敗的致命問題

### 2.3 Schema 壓縮對比

| 格式 | 範例 | Token 數 |
|------|------|---------|
| CREATE TABLE + 類型 | `CREATE TABLE WP_vOutStock (sn INT, OutStkId NVARCHAR, ...)` × 7 表 | ~1,614 tokens |
| **緊湊格式（採用）** | `WP_vOutStock(sn, OutStkId, ...) -- Sales/Outbound.` × 7 表 | **~937 tokens** |
| 單表 CREATE TABLE | `CREATE TABLE WP_vOutStock (sn INT, ...)` × 1 表 | ~129 tokens |

Enterprise 使用緊湊格式，在 MAX_SEQ_LEN=1280 內有 ~214 tokens 的裕度空間。

---

## 三、訓練資料集分析

### 3.1 資料來源

| 資料集 | 檔案 | 筆數 | 來源 | 備註 |
|--------|------|------|------|------|
| Spider 格式訓練集 | `train_spider_WP_M09.json` | 1,014 | 人工 + GPT 生成 | 7 表均衡分布 |
| Claude 英文訓練集 | `train_claude_en_2000.json` | 1,748 | Claude 生成 | 較多 isDel 樣本 |
| **Enterprise 擴增** | 腳本內建 | **50** | 手工設計 | 針對 5 類弱模式 |
| **Enterprise 合計** | 去重後 | **2,585** | — | 去除 227 筆重複 |

### 3.2 各資料集特性比較

| 特性 | train_spider (1,014) | train_claude (1,748) | 擴增 (50) | 驗證集 val_v2 (238) |
|------|---------------------|---------------------|-----------|-------------------|
| **語言** | EN 81% / CN 19% | EN 54% / CN 46% | EN 72% / CN 28% | EN + CN 混合 |
| **View 分布** | 均衡（132-155/view） | 偏多 AcctOut/OutStock | 聚焦弱模式 | 完全均衡（34/view） |
| **isDel 覆蓋** | 35.5% | 58.6% | 100%（isDel 有的表） | — |
| **子查詢（subquery）** | 5.4% | **0%** | **22%（11/50）** | **13.4%** |
| **DISTINCT** | — | — | **10%（5/50）** | 52.5% |

### 3.3 Enterprise 擴增 50 筆分類

| 類別 | 筆數 | 目的 | 對應已知問題 |
|------|------|------|-------------|
| OutStock 表辨識 | 18 | 多樣化問句（含不直接提到「出庫」的表述） | OutStock EX 5.9%，31/32 keyword 推斷失敗 |
| 子查詢去重 | 11 | SUM/AVG header amount 的正確子查詢寫法 | Claude 訓練集 0 筆子查詢 |
| isDel/dtlIsDel 辨別 | 12 | header vs detail 的成對對比 + 無 isDel 表 | isDel 混淆 |
| DISTINCT 使用 | 5 | COUNT(DISTINCT) 和 SELECT DISTINCT 場景 | 驗證集 52.5% 需要 DISTINCT |
| Transfer 特殊場景 | 4 | 雙倉庫、無 header amount | Transfer 倉庫欄位易混淆 |

### 3.4 View 分布（Enterprise 去重後 2,585 筆）

| View | 筆數 | 佔比 |
|------|------|------|
| WP_vAcctIn | 424 | 16.4% |
| WP_vAcctOut | 368 | 14.2% |
| WP_vOutStock | 449 | 17.4% |
| WP_vTransfer | 338 | 13.1% |
| WP_vInventory | 359 | 13.9% |
| WP_vProduct | 344 | 13.3% |
| WP_vProvider | 303 | 11.7% |

### 3.5 驗證集（val_claude_en_spider_v2.json, 238 筆）

| 維度 | 數據 |
|------|------|
| 每個 View | 完全均衡：34 筆/view |
| 難度分布 | easy 98 / medium 84 / hard 56 |
| DB 執行驗證 | ✅ 238/238 全部通過 SQL Server 執行 |
| 與訓練集重疊 | ~5%（SQL 正規化後比對） |

---

## 四、已知問題與 Enterprise 版本的解決方案

### 4.1 問題 1：OutStock 表推斷失敗

| 項目 | 說明 |
|------|------|
| **現象** | v0320 (v3) 評估：OutStock 相關題目 EX 僅 5.9%（34 題中 32 題錯誤） |
| **根因** | 推論時用 keyword 推斷目標表，「銷貨」「出庫」等關鍵字未命中 → 推斷為錯誤的表 |
| **數據** | 32/34 題被推斷為錯誤的表（預測表選擇準確率接近 0%） |
| **Enterprise 解決** | 全 7 表 schema 放入 prompt → 模型自行選表 → 不再依賴 keyword 推斷 |

### 4.2 問題 2：子查詢去重 0% 成功率

| 項目 | 說明 |
|------|------|
| **現象** | 需要子查詢去重的 SQL 全部失敗 |
| **根因** | train_claude_en_2000.json 中子查詢樣本為 **0 筆** |
| **Enterprise 解決** | 擴增 11 筆子查詢去重樣本（覆蓋 AcctIn, AcctOut, OutStock, Transfer） |

### 4.3 問題 3：isDel / dtlIsDel 混淆

| 項目 | 說明 |
|------|------|
| **現象** | 無 isDel 的表被加上 `WHERE isDel='N'`；detail 查詢漏加 `dtlIsDel='N'` |
| **Enterprise 解決** | (1) 商業規則明確標示哪些表有/無 isDel (2) 擴增 12 筆成對對比樣本 |

### 4.4 問題 4：訓練集 pNo 日期誤用

| 項目 | 說明 |
|------|------|
| **現象** | 部分訓練樣本用 `pNo LIKE '20251205%'` 篩選日期（pNo 是流水號 1,2,3...） |
| **處理** | 2026-03-20 已從訓練集移除（spider: -3 筆, claude: -27 筆） |
| **Enterprise 規則** | Rules 第 4 條明確標示 `pNo=seq number, NOT date` |

---

## 五、方法論對比：Spider / BIRD vs Enterprise

### 5.1 Spider 1.0 方法論

```
Input:  全資料庫 CREATE TABLE schema + 自然語言問句
Output: SQL 查詢
特點:   模型需要從多張表中選擇正確的表
評估:   EM (Exact Match) + EX (Execution Accuracy)
```

### 5.2 BIRD Benchmark 方法論

```
Input:  全資料庫 schema + evidence (領域知識提示) + 自然語言問句
Output: SQL 查詢
特點:   evidence 提供商業邏輯，幫助模型理解 domain-specific 概念
評估:   EX (Execution Accuracy) + VES (Valid Efficiency Score)
```

### 5.3 Enterprise v0322 方法論

```
Input:  緊湊全 7 View schema + 商業規則（BIRD evidence）+ 自然語言問句
Output: T-SQL 查詢
特點:
  - 模仿 Spider：全資料庫 schema，模型自選表
  - 模仿 BIRD：rules 區塊提供企業商業邏輯
  - 企業特有：isDel 過濾、子查詢去重、中文字串 N 前綴
評估:   Table Selection Accuracy + String EM + EX + per-view + per-difficulty
```

### 5.4 Enterprise 的學術價值

| 對比維度 | Spider/BIRD | Enterprise |
|---------|-------------|------------|
| 資料庫來源 | 公開學術資料庫 | **企業生產環境** |
| Schema 複雜度 | 多樣（2-20+ 表） | 固定 7 View，但有複雜商業邏輯 |
| SQL 方言 | SQLite（Spider）/各種（BIRD） | **T-SQL (SQL Server)** |
| 語言 | 英文 | **繁體中文 + 英文** |
| isDel 軟刪除 | ❌ | ✅ 核心挑戰 |
| 子查詢去重 | 少見 | ✅ 必要（header-detail 結構） |
| 訓練方法 | 全 schema + 原始問句 | **全 schema + evidence + 弱模式擴增** |

---

## 六、Ablation 實驗設計

Enterprise v0322 內建 ablation 支援，用於論文實驗：

| 實驗 | 指令 | 預期影響 |
|------|------|---------|
| **Baseline（全配置）** | `python train__enterprise_v0322.py` | — |
| **w/o Full Schema** | `python train__enterprise_v0322.py --schema single` | 退化為單表，需 keyword 推斷 |
| **w/o Rules** | `python train__enterprise_v0322.py --no-rules` | 移除商業規則 |
| **w/o Augmentation** | `python train__enterprise_v0322.py --no-augment` | 移除 50 筆擴增 |
| **w/o Schema** | `python train__enterprise_v0322.py --schema none` | 無 schema baseline |

---

## 七、評估腳本

| 腳本 | 用途 | 關鍵差異 |
|------|------|---------|
| `eval__em_and_execution_accuracy.py` | 舊版評估（v0317 等） | keyword 推斷表、EM + EX |
| `eval__enterprise_v0322.py` | Enterprise 評估 | **全 schema 推論**、Table Selection + EM + EX + per-view + per-difficulty |

Enterprise 評估腳本從 `train__enterprise_v0322.py` 直接 import schema（single source of truth），確保推論時與訓練時使用完全一致的 schema 和規則。

---

## 八、歷史評估結果（已驗證數據）

### 8.1 舊測試集結果（test_spider_WP_M09.json, 182 筆）

| 版本 | 日期 | EM | EX | 備註 |
|------|------|-----|-----|------|
| v0308 (DoRA) | 03/09 | 34.07% | — | 首次 Spider 格式 |
| v0312 | 03/12 | 33.52% | — | 效果反降 |
| v0315 | 03/15 | 67.03% | — | 增加 train_claude_en_2000 |
| v0317 (R1) | 03/17 | **91.76%** | — | Auto Loop 最佳，但測試集有洩漏風險 |

### 8.2 新驗證集結果（val_claude_en_spider_v2.json, 238 筆）

| 版本 | 日期 | EM | EX | 備註 |
|------|------|-----|-----|------|
| v0320 (v3) | 03/20 | 34.03% | 44.12% | OutStock 5.9% 災難性失敗 |
| **Enterprise v0322** | 03/22 | **待評估** | **待評估** | 🔄 訓練中 |

### 8.3 v0320 (v3) Per-View EX（新驗證集，暴露問題）

| View | EX | 問題 |
|------|-----|------|
| WP_vAcctIn | 58.8% | — |
| WP_vAcctOut | 41.2% | — |
| **WP_vOutStock** | **5.9%** | 31/32 表推斷錯誤 |
| WP_vTransfer | 35.3% | — |
| WP_vInventory | 58.8% | — |
| WP_vProduct | 50.0% | — |
| WP_vProvider | 58.8% | — |

---

## 九、當前訓練進度

| 項目 | 狀態 |
|------|------|
| 腳本 | `train__enterprise_v0322.py` |
| 訓練樣本 | 2,585 筆（去重後） |
| 總步數 | 972 steps |
| 速度 | ~28.14 秒/step |
| 預計總時間 | ~7.6 小時 |
| **目前進度** | 🔄 執行中（Epoch 1/6） |
| 輸出目錄 | `outputs/models/enterprise_full_0322/` |

### 訓練完成後下一步

```bash
# 評估
python eval__enterprise_v0322.py \
    --model outputs/models/enterprise_full_0322/final_model \
    --gold data/wp_m09/val_claude_en_spider_v2.json \
    --output outputs/eval_enterprise_full_0322.json \
    --db-host "SHANE\SQLEXPRESS" --db-trusted
```

評估將產出：
- **Table Selection Accuracy**（新指標：模型選對表的準確率）
- **String EM**（SQL 字串精確匹配）
- **Execution Accuracy**（SQL Server 實際執行結果比對）
- **Per-View 結果**（每個 View 的獨立表現）
- **Per-Difficulty 結果**（easy / medium / hard）

---

## 十、檔案索引

| 檔案 | 用途 |
|------|------|
| `train__enterprise_v0322.py` | Enterprise 訓練腳本（全 schema + rules + augment） |
| `eval__enterprise_v0322.py` | Enterprise 評估腳本（Table Selection + EM + EX） |
| `data/wp_m09/train_spider_WP_M09.json` | Spider 格式訓練集（1,014 筆） |
| `data/wp_m09/train_claude_en_2000.json` | Claude 英文訓練集（1,748 筆） |
| `data/wp_m09/val_claude_en_spider_v2.json` | 驗證集（238 筆，34/view） |
| `VERSION_HISTORY.md` | 所有版本演進紀錄 |
| `PROJECT_HISTORY.md` | 專案開發日誌 |
| `ENTERPRISE_EXPERIMENT_COMPARISON.md` | 本文件 |
