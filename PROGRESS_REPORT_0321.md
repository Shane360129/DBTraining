# WP_M09 Text-to-SQL Project Progress Report

> Date: 2026-03-21
> Author: Shane / Claude AI Assistant
> Project: Traditional Chinese Natural Language to T-SQL (WP_M09 Database)

---

## 1. Project Overview

### Goal
Fine-tune **Llama-3.1-8B-Instruct** with **DoRA (Weight-Decomposed LoRA)** to convert Traditional Chinese / English natural language questions into T-SQL queries for the **WP_M09** business database.

### Target Metrics
- **EM (Exact Match)** >= 80% (string-level match after normalization)
- **EX (Execution Accuracy)** >= 90% (query results match)

### Tech Stack
| Component | Detail |
|-----------|--------|
| Base Model | meta-llama/Llama-3.1-8B-Instruct |
| Fine-tune Method | DoRA (r=16, alpha=32) |
| Quantization | QLoRA 4-bit (NF4 + double quant) |
| Hardware | NVIDIA RTX 5070 Ti (16GB VRAM) |
| Database | SQL Server (SHANE\SQLEXPRESS) |
| Training Framework | HuggingFace TRL (SFTTrainer) |

---

## 2. Business Domain: WP_M09 Database

### 2.1 Database Schema (7 Views)

| View | Description | Key ID Column | Has isDel? | Date Filter Method |
|------|------------|---------------|------------|-------------------|
| **WP_vAcctIn** | Accounts Receivable (Sales Invoices) | acctInId | Yes | `LEFT(acctInId, 8)='YYYYMMDD'` |
| **WP_vAcctOut** | Accounts Payable (Purchase Invoices) | acctOutId | Yes | `LEFT(acctOutId, 8)='YYYYMMDD'` |
| **WP_vOutStock** | Sales / Outbound Stock | OutStkId | Yes | `LEFT(OutStkId, 8)='YYYYMMDD'` |
| **WP_vTransfer** | Inventory Transfers | TransferId | Yes | `LEFT(TransferId, 8)='YYYYMMDD'` |
| **WP_vInventory** | Current Inventory Levels | (none) | No | N/A |
| **WP_vProduct** | Product Master Data | pNo (sequential) | No | N/A |
| **WP_vProvider** | Supplier Master Data | sn (sequential) | No | N/A (uses `isStop`) |

### 2.2 Critical Business Rules

1. **isDel / dtlIsDel Pattern**
   - 4 transactional views (AcctIn, AcctOut, OutStock, Transfer) have soft-delete flags
   - Active records require: `WHERE isDel='N' AND dtlIsDel='N'`
   - WP_vInventory, WP_vProduct, WP_vProvider **do NOT have** these columns

2. **ID = Document Number (First 8 Chars = Date)**
   - `acctInId`, `acctOutId`, `OutStkId`, `TransferId` are document numbers
   - First 8 characters encode the date: `20260311xxxx`
   - Date filtering uses `LEFT(Id, 8)='YYYYMMDD'` or `LEFT(Id, 6)='YYYYMM'`

3. **pNo is NOT a Date**
   - `pNo` is a sequential product ID (1, 2, 3...)
   - Must NEVER use `pNo LIKE 'YYYYMMDD%'` for date filtering

4. **WP_vProvider Special Rules**
   - Primary key: `sn` (NOT `pvSn` which does not exist)
   - Active/inactive: `isStop='N'` (active) / `isStop='Y'` (stopped)
   - `pvDiscount` for supplier discount rate
   - Join to other tables via `pvSn` in other views

5. **N-prefix for Unicode Strings**
   - Chinese text values must use `N'...'` prefix: `WHERE pvName=N'xxx'`

---

## 3. Evolution Timeline

### Phase 1: Initial Attempts (02/28 - 03/07)

| Date | Model | Training Data | EM | Notes |
|------|-------|--------------|-----|-------|
| 02/28 | wp_m09_0228_failed | Manual | - | First attempt, failed |
| 03/01 | wp_m09_0301 | Manual | - | Basic fine-tune |
| 03/03 | wp_m09_0303 | Manual | - | Iterative improvement |
| 03/04 | wp_m09_0304 | Manual | - | More training data |
| 03/05 | wp_m09_uploaded_0305 | Uploaded dataset | - | External dataset test |
| 03/06 | wp_m09_dora_0306 | Manual | - | First DoRA attempt |
| 03/07 | wp_m09_dora_0307 | Manual | ~34% | Baseline DoRA model |

**Key Learnings**: Manual training data creation was slow and inconsistent.

### Phase 2: Spider Format + Systematic Training (03/08 - 03/15)

| Date | Model | Training Samples | EM (test 182) | Notes |
|------|-------|-----------------|------|-------|
| 03/08 | dora_0307_v2 | Spider format | 34.62% | Converted to Spider format |
| 03/09 | dora_0308_spider | Revised | 12.09% | Regression - bad data |
| 03/10 | dora_0309_spider | Fixed | 34.62% | Recovered, tried LoRA too |
| 03/12 | dora_0312_spider | Expanded | - | More diverse queries |
| 03/13 | dora_0313_spider | 91 test | 51.65% | First >50% milestone |
| 03/14 | dora_0314_spider | 182 test | 57.14% | Expanded test set |
| 03/15 | dora_0315_spider | Cleaned | 67.03% | Data cleaning helped |

**Key Learnings**: Data quality >> data quantity. Cleaning invalid samples significantly improved EM.

### Phase 3: Auto Training Loop + Best Model (03/17)

| Date | Model | Training Samples | EM (test 182) | Notes |
|------|-------|-----------------|------|-------|
| 03/17 | dora_0317_spider_r1 | 975 (auto R1) | **91.76%** | Best model - Auto loop |

**Breakthrough**: Auto training loop (`auto__train_loop_until_target_em.py`) with corrective sampling achieved **91.76% EM** on test set (182 samples), far exceeding the 80% target.

### Phase 4: Validation Set + Analysis (03/18 - 03/20)

| Date | Model | Val Set (238) EM | Val Set EX | Notes |
|------|-------|-----------------|-----------|-------|
| 03/17 r1 | dora_0317_spider_r1 | 7.56% | 44.12% | New val set revealed overfitting |
| 03/19 | dora_0319_spider | 62.4% (242) | - | English questions improved |
| 03/20 | dora_0320_spider | 5.88% | 40.76% | Regression |

**Critical Discovery**: The 91.76% EM on test set was overfitting. On a new independent validation set (238 samples with easy/medium/hard), EM dropped to 7.56%. However, **EX was 44.12%**, suggesting the SQL was often semantically correct but syntactically different.

### Phase 5: Complete Dataset Rebuild (03/20 - 03/21)

| Date | Action | Result |
|------|--------|--------|
| 03/20 | Queried all 7 views for real data (20 rows each) | Reference JSON created |
| 03/20 | Built 2000 training Q&A from real DB data | train_claude_en_2000_v3.json (1999 samples) |
| 03/20 | Built validation set from separate 30 rows/view | val_claude_en_v3.json |
| 03/20 | Validated all SQL returns >= 1 row | 100% pass |
| 03/20 | Removed duplicate NL questions | 0 duplicates |
| 03/20 | Trained v3 model (10 epochs, LR=8e-5) | Final loss ~0 |
| 03/21 | Evaluated v3 on val_v2 (238 samples) | **EM=1.26%, EX=26.05%** |

**Root Cause Analysis** (v3 failure):

The v3 training data had a completely different SQL style than the validation set:

| Style Feature | v3 Training (1999) | Validation (238) | Mismatch |
|--------------|-------------------|-----------------|----------|
| Semicolons `;` | 100% (1999) | 0% (0) | Critical |
| `AS` aliases | 46.5% (930) | 18.9% (45) | High |
| `DISTINCT` | 8.2% (164) | 52.5% (125) | Critical |
| `ORDER BY` | 27.1% (542) | 13.9% (33) | Medium |
| `SELECT *` | 0% (0) | 2.1% (5) | Low |

Additionally, semantic errors: model confused `memName` (member) with `pvName` (vendor), selected wrong columns.

### Phase 6: Style Cleanup + Combined Training (03/21, In Progress)

| Date | Action | Status |
|------|--------|--------|
| 03/21 | Cleaned v3 SQL style (removed `;`, `AS`, extra `ORDER BY`) | Done |
| 03/21 | Combined 3 training sets (0317 + claude + v3 clean) | 4533 samples after dedup |
| 03/21 | Started v0321 combined training (6 epochs, LR=6e-5) | **In progress (24%)** |
| 03/21 | 0317 best model EX re-evaluation with DB connection | **In progress** |

---

## 4. Current Model Comparison

| Model | Train Samples | Test EM (182) | Val EM (238) | Val EX (238) |
|-------|--------------|---------------|-------------|-------------|
| dora_0317_r1 (best) | 975 | **91.76%** | 7.56% | 44.12% |
| dora_0319 | ~2700 | - | 62.4% (242) | - |
| dora_0320 | ~2700 | - | 5.88% | 40.76% |
| dora_0320_v3 | 1999 | - | 1.26% | 26.05% |
| **dora_0321_combined** | **4533** | **-** | **TBD** | **TBD** |

---

## 5. Training Data Summary

### Active Datasets

| File | Samples | Description |
|------|---------|-------------|
| train_spider_WP_M09.json | 1,014 | Original Spider-format, high DISTINCT (35.7%) |
| train_claude_en_2000.json | 1,748 | Claude-generated English Q&A |
| train_claude_en_2000_v3.json | 1,999 | Rebuilt from real DB data, all validated |
| train_claude_en_2000_v3_clean.json | 1,999 | v3 with SQL style cleaned |
| **Combined (0321)** | **4,533** | **Merged + deduplicated** |

### Validation & Test Sets

| File | Samples | Description |
|------|---------|-------------|
| val_claude_en_spider_v2.json | 238 | Primary validation (easy/medium/hard) |
| test_spider_WP_M09_v2.json | 182 | Original test set |

---

## 6. Key Insights & Lessons Learned

### 6.1 EM vs EX Gap
EM (string match) is extremely strict. Semantically equivalent SQL (same results) can fail EM due to:
- Extra `ORDER BY` (doesn't change unordered result sets)
- `AS` column aliases (doesn't change query results)
- `SELECT *` vs explicit column list
- `MAX(x)` vs `TOP 1 x ORDER BY x DESC`
- Subquery `SUM() FROM (SELECT DISTINCT ...)` vs direct `SUM()`

**Recommendation**: EX (Execution Accuracy) is the more meaningful metric for real-world use.

### 6.2 Training Data Style Must Match Evaluation
The most important factor is **SQL style consistency** between training data and evaluation gold standard. v3 had 100% valid SQL that returns data, but its style was incompatible with the validation set.

### 6.3 Overfitting on Small Test Sets
The 91.76% EM on 182-sample test set didn't generalize to 238-sample validation set. This indicates the model memorized patterns rather than learning general SQL generation.

### 6.4 Data Quality Hierarchy
1. **Correct business logic** (isDel, date filtering, column names)
2. **SQL style consistency** (DISTINCT, aliases, semicolons)
3. **Query diversity** (all 7 views, various difficulty levels)
4. **Volume** (more data helps, but only if quality is maintained)

---

## 7. Current Active Tasks

| Task | Status | Expected Completion |
|------|--------|-------------------|
| v0321 combined training (4533 samples, 6 epochs) | 24% (405/1704 steps) | ~4-5 hours |
| 0317 best model EX re-evaluation | 42% (100/238) | ~20 minutes |
| v0321 model evaluation (EM + EX) | Pending | After training |

---

## 8. Next Steps

1. **Complete v0321 training** and evaluate EM + EX on validation set
2. **Compare results** against 0317 baseline (target: EX > 44%, EM improvement)
3. If EX >= 90%: **Success** - model is production-ready
4. If EX < 90%: Consider:
   - Rebuild validation set gold SQL to match a consistent style
   - Focus on fixing semantic errors (wrong column names, wrong JOINs)
   - Add more DISTINCT-heavy training samples
   - Try different hyperparameters (higher r, more epochs)

---

## 9. File Organization

```
D:\spider1_training\
  |-- CLAUDE.md                          # Project instructions
  |-- PROGRESS_REPORT_0321.md            # This report
  |
  |-- data/wp_m09/
  |   |-- train_spider_WP_M09.json       # 1,014 samples (0317 best)
  |   |-- train_claude_en_2000.json       # 1,748 samples (0317 best)
  |   |-- train_claude_en_2000_v3.json    # 1,999 samples (v3 rebuild)
  |   |-- train_claude_en_2000_v3_clean.json  # 1,999 (v3 cleaned)
  |   |-- val_claude_en_spider_v2.json    # 238 samples (validation)
  |   |-- test_spider_WP_M09_v2.json      # 182 samples (test)
  |   `-- ...backups and older versions
  |
  |-- outputs/
  |   |-- models/
  |   |   |-- wp_m09_dora_0317_spider_r1/ # Best model (91.76% test EM)
  |   |   |-- wp_m09_dora_0320_spider_v3/ # v3 model (1.26% val EM)
  |   |   |-- wp_m09_dora_0321_combined/  # Currently training
  |   |   `-- ...25 model versions total
  |   |
  |   `-- evaluation_*.json               # 20 evaluation result files
  |
  |-- 68 Python scripts organized by prefix:
      |-- auto__     (1)   Automated training loops
      |-- data_prep__(2)   Data preparation
      |-- debug__    (2)   Failure diagnosis
      |-- eval__     (15)  Model evaluation
      |-- inference__(4)   Interactive query / web UI
      |-- schema__   (4)   Database schema inspection
      |-- train__    (15)  Model training scripts
      |-- traindata_gen__  (17)  Training data generation
      |-- traindata_clean__(9)   Training data cleaning
      `-- traindata_merge__(2)   Training data merging
```

---

## 10. Glossary

| Term | Definition |
|------|-----------|
| **EM** | Exact Match - normalized SQL string must be identical |
| **EX** | Execution Accuracy - query results must be identical |
| **DoRA** | Weight-Decomposed LoRA - improved LoRA fine-tuning method |
| **QLoRA** | Quantized LoRA - 4-bit quantization during training |
| **Spider** | Standard Text-to-SQL benchmark dataset format |
| **isDel** | Soft-delete flag ('N' = active, 'Y' = deleted) |
| **dtlIsDel** | Detail-level soft-delete flag |
| **pNo** | Product sequential number (NOT a date) |
| **pvSn** | Provider serial number in other views (NOT in WP_vProvider itself) |

---

*Report generated: 2026-03-21*
*Training in progress: v0321 combined model (4,533 samples)*
