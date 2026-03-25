# train__enterprise_v0324.py
# ============================================================
# 企業 Text-to-SQL 落地實驗 — 訓練腳本 v0324
#
# 方法論：仿照 Spider 1.0 / BIRD Benchmark
#
# vs v0323 關鍵改動（防止過擬合）：
#   1. Epochs 8→3（v0323 分析顯示 epoch 0.8 即收斂，後續純背誦）
#   2. Learning Rate 5e-5→2e-5（降低學習速度，更穩定）
#   3. 加入 EarlyStoppingCallback（patience=3，以 eval_loss 監控）
#   4. eval 頻率從每 epoch → 每 0.5 epoch（更早偵測過擬合）
#   5. save_strategy 與 eval 同步
#
# 過擬合分析（v0323）：
#   - Train loss 在 epoch 0.8 降至 0.0018，之後 7.2 epochs 停滯
#   - Token accuracy 99.90% 持平 > 3 epochs
#   - Grad norm < 0.01，幾乎無梯度
#   結論：模型記住所有訓練資料，喪失泛化能力
#
# 用法:
#   python train__enterprise_v0324.py
#   python train__enterprise_v0324.py --no-augment     # 不擴增（ablation）
#   python train__enterprise_v0324.py --schema single   # 單表模式（ablation）
#   python train__enterprise_v0324.py --no-rules        # 不含規則（ablation）
#
# 輸出:
#   outputs/models/enterprise_full_0324/final_model/
# ============================================================

import json
import os
import re
import sys
import copy
import argparse
import random
import torch
import statistics
from datetime import datetime
from collections import Counter
from datasets import Dataset
from transformers import (
    AutoTokenizer,
    AutoModelForCausalLM,
    BitsAndBytesConfig,
    EarlyStoppingCallback,
)
from peft import LoraConfig, get_peft_model, TaskType
from trl import SFTTrainer, SFTConfig


# ============================================================
# Settings
# ============================================================
MODEL_PATH   = "meta-llama/Llama-3.1-8B-Instruct"
DATE_STR     = "0324"

TRAIN_PATHS  = [
    r"data\wp_m09\train_spider_WP_M09_v2.json",     # 1,014 (fixed: subquery dedup, pvId, COUNT DISTINCT)
    r"data\wp_m09\train_claude_en_2000_v2.json",     # 1,748 (fixed: 435 samples corrected)
    r"data\wp_m09\train_augment_v2.json",            # 201 (column disambiguation, TOP N, SELECT *, etc.)
]
VAL_PATH     = r"data\wp_m09\val_claude_en_spider_v2.json"  # 238 (for reference)

# ---- DoRA ----
LORA_R        = 16
LORA_ALPHA    = 32
LORA_DROPOUT  = 0.05
USE_DORA      = True

# ---- Training hyperparams ----
NUM_EPOCHS    = 3           # v0323 分析：epoch 0.8 即收斂，8 epochs 嚴重過擬合
BATCH_SIZE    = 2           # 配合較長序列
GRAD_ACCUM    = 8           # effective batch = 16
EARLY_STOPPING_PATIENCE = 3 # eval_loss 連續 3 次未改善則停止
LEARNING_RATE = 2e-5        # v0323 用 5e-5 收斂過快，降低讓學習更穩定
MAX_SEQ_LEN   = 1280        # 緊湊 schema ~937 tokens + Q+SQL ~130 tokens
LR_SCHEDULER  = "cosine"
WARMUP_RATIO  = 0.06
WEIGHT_DECAY  = 0.01

random.seed(42)


# ============================================================
# Full-Schema System Prompt（仿 Spider/BIRD：全資料庫 schema）
# ============================================================
FULL_SCHEMA = (
    "-- WP_M09 (SQL Server T-SQL). Prefix: WP_M09.dbo.<View>. Chinese: N'str'.\n\n"
    "WP_vAcctIn(sn, acctInId, acctInDate, amount, memo, empId, isDel, dtlSn, OutStkId, outStkAmtTotal, dtlIsDel, memSn, memId, memName, pNo, pBarcode, pName, pNameS, oStkDtlAmt, oStkDtlQty, oStkDtlAmtTotal, dtlDiscnt, dtlDiscntShare, discount, discountShare)"
    " -- Receivable. LEFT(acctInId,8)=date. isDel+dtlIsDel.\n"
    "WP_vAcctOut(sn, acctOutId, acctOutDate, amount, transAmt, memo, empId, empName, isDel, dtlSn, InStkId, dtlAmt, qty, amtTotal, dtlIsDel, pNo, pName, pNameS, pBarcode, pvId, pvName, pvNameS, pvSn, pvDiscount, inStkAmt, inStkAmtTotal, payType)"
    " -- Payable. LEFT(acctOutId,8)=date. isDel+dtlIsDel.\n"
    "WP_vOutStock(sn, OutStkId, OutStkDate, amount, tax, amtNoneTax, isDel, empId, empName, memo, memSn, memId, memName, outType, dtlSn, pNo, qty, dtlAmt, amtTotal, dtlIsDel, dtlCostAvg, dtlCostStd, dtlDiscnt, dtlDiscntPer, dtlDiscntShare, pName, pBarcode, pUName, costStd, discount, discountShare, memTel, memCityName, memZoneName)"
    " -- Sales/Outbound. LEFT(OutStkId,8)=date. isDel+dtlIsDel.\n"
    "WP_vTransfer(sn, TransferId, empId, dtlSn, FromWhSn, fWhId, fWhName, ToWhSn, tfWhId, tfWhName, TransferDate, pNo, qty, pName, pNameS, pBarcode, pCode, isDel, dtlIsDel, costAvg)"
    " -- Transfer. LEFT(TransferId,8)=date. isDel+dtlIsDel. No header amount. fWhName=source, tfWhName=dest.\n"
    "WP_vInventory(whSn, WarehouseId, WarehouseName, pNo, pName, pNameS, pBarcode, pUnit, pUName, priceStd, priceLow, priceMem, priceBat, costStd, costAvg, isSale, pvName, pvNameS, qtyNow, pvSn, qtySafe, qty)"
    " -- Inventory. NO isDel. NO date. pNo=seq#.\n"
    "WP_vProduct(pNo, pName, pNameS, pBarcode, pCode, pUnit, pUName, priceStd, priceLow, priceMem, priceBat, isPvDiscount, isSale, costStd, costAvg, pvSn, pvId, pvName, pvNameS, qtyNow, qtySafe, pvDiscount)"
    " -- Product. NO isDel. NO date. pNo=seq#.\n"
    "WP_vProvider(sn, pvId, pvName, pvNameS, pvKId, pvBoss, pvTel, pvCityId, pvZoneId, pvCity, pvZone, pvAddr, ctactName, ctactTel, fax, email, taxId, isStop, invoTitle, bankId, bankName, bankAccount, bankAcctName, memo, pvKName, pvDiscount)"
    " -- Supplier. NO isDel. isStop=N/Y. NO date. SELECT pvId (not pvSn)."
)

BUSINESS_RULES = (
    "Rules:\n"
    "1. isDel views (AcctIn/AcctOut/OutStock/Transfer): isDel='N' header, +dtlIsDel='N' detail columns.\n"
    "2. No-isDel views (Inventory/Product/Provider): NEVER add isDel. Provider: isStop='N' active.\n"
    "3. SUM/AVG header amount: SELECT SUM(amount) FROM (SELECT DISTINCT xxxId, amount FROM ... WHERE isDel='N') sub\n"
    "4. NEVER use SUM(DISTINCT amount) or AVG(DISTINCT amount) — always use subquery dedup.\n"
    "5. Count orders: COUNT(DISTINCT xxxId), never COUNT(*) on header-detail views.\n"
    "6. Date: LEFT(xxxId,8)='YYYYMMDD'. Only isDel views. pNo=seq number, NOT date.\n"
    "7. T-SQL only: use TOP N, never LIMIT. Use N'str' for Chinese strings.\n"
    "8. Provider SELECT: use pvId (not pvSn). pvSn is for JOIN only."
)


# ============================================================
# Single-Table Schema（用於 Ablation 實驗）
# ============================================================
SINGLE_VIEW_SCHEMAS = {
    "WP_vAcctIn": "CREATE TABLE WP_vAcctIn (\n  sn INT, acctInId NVARCHAR, acctInDate DATETIME, amount DECIMAL, memo NVARCHAR, empId NVARCHAR, isDel CHAR, dtlSn INT, OutStkId NVARCHAR, outStkAmtTotal DECIMAL, dtlIsDel CHAR, memSn INT, memId NVARCHAR, memName NVARCHAR, pNo INT, pBarcode NVARCHAR, pName NVARCHAR, pNameS NVARCHAR, oStkDtlAmt DECIMAL, oStkDtlQty DECIMAL, oStkDtlAmtTotal DECIMAL, dtlDiscnt DECIMAL, dtlDiscntShare DECIMAL, discount DECIMAL, discountShare DECIMAL\n);",
    "WP_vAcctOut": "CREATE TABLE WP_vAcctOut (\n  sn INT, acctOutId NVARCHAR, acctOutDate DATETIME, amount DECIMAL, transAmt DECIMAL, memo NVARCHAR, empId NVARCHAR, empName NVARCHAR, isDel CHAR, dtlSn INT, InStkId NVARCHAR, dtlAmt DECIMAL, qty DECIMAL, amtTotal DECIMAL, dtlIsDel CHAR, pNo INT, pName NVARCHAR, pNameS NVARCHAR, pBarcode NVARCHAR, pvId NVARCHAR, pvName NVARCHAR, pvNameS NVARCHAR, pvSn INT, pvDiscount DECIMAL, inStkAmt DECIMAL, inStkAmtTotal DECIMAL, payType NVARCHAR\n);",
    "WP_vOutStock": "CREATE TABLE WP_vOutStock (\n  sn INT, OutStkId NVARCHAR, OutStkDate DATETIME, amount DECIMAL, tax DECIMAL, amtNoneTax DECIMAL, isDel CHAR, empId NVARCHAR, empName NVARCHAR, memo NVARCHAR, memSn INT, memId NVARCHAR, memName NVARCHAR, outType NVARCHAR, dtlSn INT, pNo INT, qty DECIMAL, dtlAmt DECIMAL, amtTotal DECIMAL, dtlIsDel CHAR, dtlCostAvg DECIMAL, dtlCostStd DECIMAL, dtlDiscnt DECIMAL, dtlDiscntPer DECIMAL, dtlDiscntShare DECIMAL, pName NVARCHAR, pBarcode NVARCHAR, pUName NVARCHAR, costStd DECIMAL, discount DECIMAL, discountShare DECIMAL, memTel NVARCHAR, memCityName NVARCHAR, memZoneName NVARCHAR\n);",
    "WP_vTransfer": "CREATE TABLE WP_vTransfer (\n  sn INT, TransferId NVARCHAR, empId NVARCHAR, dtlSn INT, FromWhSn INT, fWhId NVARCHAR, fWhName NVARCHAR, ToWhSn INT, tfWhId NVARCHAR, tfWhName NVARCHAR, TransferDate DATETIME, pNo INT, qty DECIMAL, pName NVARCHAR, pNameS NVARCHAR, pBarcode NVARCHAR, pCode NVARCHAR, isDel CHAR, dtlIsDel CHAR, costAvg DECIMAL\n);",
    "WP_vInventory": "CREATE TABLE WP_vInventory (\n  whSn INT, WarehouseId NVARCHAR, WarehouseName NVARCHAR, pNo INT, pName NVARCHAR, pNameS NVARCHAR, pBarcode NVARCHAR, pUnit NVARCHAR, pUName NVARCHAR, priceStd DECIMAL, priceLow DECIMAL, priceMem DECIMAL, priceBat DECIMAL, costStd DECIMAL, costAvg DECIMAL, isSale CHAR, pvName NVARCHAR, pvNameS NVARCHAR, qtyNow DECIMAL, pvSn INT, qtySafe DECIMAL, qty DECIMAL\n);",
    "WP_vProduct": "CREATE TABLE WP_vProduct (\n  pNo INT, pName NVARCHAR, pNameS NVARCHAR, pBarcode NVARCHAR, pCode NVARCHAR, pUnit NVARCHAR, pUName NVARCHAR, priceStd DECIMAL, priceLow DECIMAL, priceMem DECIMAL, priceBat DECIMAL, isPvDiscount CHAR, isSale CHAR, costStd DECIMAL, costAvg DECIMAL, pvSn INT, pvId NVARCHAR, pvName NVARCHAR, pvNameS NVARCHAR, qtyNow DECIMAL, qtySafe DECIMAL, pvDiscount DECIMAL\n);",
    "WP_vProvider": "CREATE TABLE WP_vProvider (\n  sn INT, pvId NVARCHAR, pvName NVARCHAR, pvNameS NVARCHAR, pvKId NVARCHAR, pvBoss NVARCHAR, pvTel NVARCHAR, pvCityId NVARCHAR, pvZoneId NVARCHAR, pvCity NVARCHAR, pvZone NVARCHAR, pvAddr NVARCHAR, ctactName NVARCHAR, ctactTel NVARCHAR, fax NVARCHAR, email NVARCHAR, taxId NVARCHAR, isStop CHAR, invoTitle NVARCHAR, bankId NVARCHAR, bankName NVARCHAR, bankAccount NVARCHAR, bankAcctName NVARCHAR, memo NVARCHAR, pvKName NVARCHAR, pvDiscount DECIMAL\n);",
}


# ============================================================
# Data Augmentation — 針對弱模式生成訓練樣本
# ============================================================
def generate_augmented_data():
    """
    生成弱模式的訓練資料擴增。
    針對目前模型的 5 大瓶頸：
      1. WP_vOutStock 辨識失敗 (5.9% EX)
      2. 子查詢去重 0% 成功率
      3. isDel vs dtlIsDel 混淆
      4. DISTINCT 缺失
      5. OutStock/Transfer 特殊欄位
    """
    augmented = []

    # ================================================================
    # 1. OutStock 表辨識 — 多樣化問句表述（解決 31/32 推斷失敗）
    # ================================================================
    outstock_samples = [
        # 英文問句 — 各種表述方式
        ("What is the total sales amount in December 2025?",
         "SELECT SUM(amount) FROM (SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND LEFT(OutStkId,6)='202512') sub"),
        ("How many outbound stock orders were there in 2025?",
         "SELECT COUNT(DISTINCT OutStkId) FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND LEFT(OutStkId,4)='2025'"),
        ("List all products in the outbound stock records",
         "SELECT DISTINCT pName FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND dtlIsDel='N'"),
        ("What are the total outbound stock sales for each product?",
         "SELECT pName, SUM(amtTotal) FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName"),
        ("Which member has the most outbound stock orders?",
         "SELECT TOP 1 memName, COUNT(DISTINCT OutStkId) AS cnt FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' GROUP BY memName ORDER BY cnt DESC"),
        ("What is the average outbound stock amount per order?",
         "SELECT AVG(amount) FROM (SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N') sub"),
        ("How many products were sold through outbound stock in January 2026?",
         "SELECT COUNT(DISTINCT pName) FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' AND LEFT(OutStkId,6)='202601'"),
        ("Show the total quantity sold for each product in outbound stock",
         "SELECT pName, SUM(qty) FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY SUM(qty) DESC"),
        ("What is the total tax amount in outbound stock orders for 2025?",
         "SELECT SUM(tax) FROM (SELECT DISTINCT OutStkId, tax FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND LEFT(OutStkId,4)='2025') sub"),
        ("List all outbound stock orders for member 王小明",
         "SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE memName=N'王小明' AND isDel='N'"),
        # 中文問句
        ("2025年12月的銷貨出庫總金額是多少?",
         "SELECT SUM(amount) FROM (SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND LEFT(OutStkId,6)='202512') sub"),
        ("列出所有出庫單號和金額",
         "SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N'"),
        ("哪個商品的出庫數量最多?",
         "SELECT TOP 1 pName, SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY total_qty DESC"),
        ("出庫紀錄中有哪些會員?",
         "SELECT DISTINCT memName FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND memName IS NOT NULL"),
        ("計算每個月的銷貨出庫筆數",
         "SELECT LEFT(OutStkId,6) AS month, COUNT(DISTINCT OutStkId) AS order_count FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' GROUP BY LEFT(OutStkId,6) ORDER BY month"),
        # 不直接提到 "outstock/出庫/銷貨" 的問句（最困難的場景）
        ("What were the total sales to 王小明 in 2025?",
         "SELECT SUM(amount) FROM (SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND memName=N'王小明' AND LEFT(OutStkId,4)='2025') sub"),
        ("How much revenue did we generate from 玫瑰花茶?",
         "SELECT SUM(amtTotal) FROM WP_M09.dbo.WP_vOutStock WHERE pName=N'玫瑰花茶' AND isDel='N' AND dtlIsDel='N'"),
        ("What items were sold on 20251205?",
         "SELECT DISTINCT pName, qty, dtlAmt FROM WP_M09.dbo.WP_vOutStock WHERE LEFT(OutStkId,8)='20251205' AND isDel='N' AND dtlIsDel='N'"),
    ]

    # ================================================================
    # 2. 子查詢去重（SUM/AVG on header amount must use subquery）
    # ================================================================
    dedup_templates = [
        # AcctIn
        ("What is the total receivable amount in 2025?",
         "SELECT SUM(amount) FROM (SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND LEFT(acctInId,4)='2025') sub"),
        ("What is the average receivable amount per order in December 2025?",
         "SELECT AVG(amount) FROM (SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND LEFT(acctInId,6)='202512') sub"),
        # AcctOut
        ("What is the total payable amount in 2025?",
         "SELECT SUM(amount) FROM (SELECT DISTINCT acctOutId, amount FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' AND LEFT(acctOutId,4)='2025') sub"),
        ("What is the average payable amount per order?",
         "SELECT AVG(amount) FROM (SELECT DISTINCT acctOutId, amount FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N') sub"),
        ("What is the total purchase amount for supplier 花草堂?",
         "SELECT SUM(amount) FROM (SELECT DISTINCT acctOutId, amount FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' AND pvName=N'花草堂') sub"),
        # OutStock
        ("What is the total outbound stock amount in January 2026?",
         "SELECT SUM(amount) FROM (SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND LEFT(OutStkId,6)='202601') sub"),
        ("What is the average sales order amount for member 李小華?",
         "SELECT AVG(amount) FROM (SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND memName=N'李小華') sub"),
        # Transfer (no header amount, use SUM(qty) or SUM(costAvg*qty))
        ("What is the total transferred quantity in December 2025?",
         "SELECT SUM(qty) FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' AND LEFT(TransferId,6)='202512'"),
        ("What is the total cost of transferred items in 2025?",
         "SELECT SUM(costAvg * qty) FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' AND LEFT(TransferId,4)='2025'"),
        # 中文
        ("2025年的應收帳款總額是多少?",
         "SELECT SUM(amount) FROM (SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND LEFT(acctInId,4)='2025') sub"),
        ("計算每個供應商的應付帳款總額",
         "SELECT pvName, SUM(amount) FROM (SELECT DISTINCT acctOutId, amount, pvName FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N') sub GROUP BY pvName ORDER BY SUM(amount) DESC"),
    ]

    # ================================================================
    # 3. isDel vs dtlIsDel 辨別（成對對比）
    # ================================================================
    isdel_pairs = [
        # Header-only: 只需 isDel='N'
        ("How many receivable orders are there in 2025?",
         "SELECT COUNT(DISTINCT acctInId) FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND LEFT(acctInId,4)='2025'"),
        ("List all payable order IDs in December 2025",
         "SELECT DISTINCT acctOutId FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' AND LEFT(acctOutId,6)='202512'"),
        ("How many outbound stock orders does member 王小明 have?",
         "SELECT COUNT(DISTINCT OutStkId) FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND memName=N'王小明'"),
        # Detail: 需要 isDel='N' AND dtlIsDel='N'
        ("What products were included in receivable orders in 2025?",
         "SELECT DISTINCT pName FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' AND LEFT(acctInId,4)='2025'"),
        ("What is the total quantity of each product in payable orders?",
         "SELECT pName, SUM(qty) FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName"),
        ("List all products and quantities sold in outbound stock for 20251205",
         "SELECT pName, qty, dtlAmt FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' AND LEFT(OutStkId,8)='20251205'"),
        # No-isDel: 絕對不能加 isDel
        ("How many products are there?",
         "SELECT COUNT(*) FROM WP_M09.dbo.WP_vProduct"),
        ("What is the standard price of 玫瑰花茶?",
         "SELECT priceStd FROM WP_M09.dbo.WP_vProduct WHERE pName=N'玫瑰花茶'"),
        ("List all active suppliers",
         "SELECT pvName, pvTel FROM WP_M09.dbo.WP_vProvider WHERE isStop='N'"),
        ("What is the current inventory quantity for each product?",
         "SELECT pName, SUM(qtyNow) FROM WP_M09.dbo.WP_vInventory GROUP BY pName"),
        ("How many warehouses have 玫瑰花茶 in stock?",
         "SELECT COUNT(DISTINCT WarehouseName) FROM WP_M09.dbo.WP_vInventory WHERE pName=N'玫瑰花茶' AND qtyNow > 0"),
        ("哪些供應商已經停用?",
         "SELECT pvName, pvTel FROM WP_M09.dbo.WP_vProvider WHERE isStop='Y'"),
    ]

    # ================================================================
    # 4. DISTINCT 使用場景
    # ================================================================
    distinct_samples = [
        ("How many unique products were sold in outbound stock?",
         "SELECT COUNT(DISTINCT pName) FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND dtlIsDel='N'"),
        ("How many unique members are in receivable records?",
         "SELECT COUNT(DISTINCT memName) FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N'"),
        ("List all unique suppliers in payable records",
         "SELECT DISTINCT pvName FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N'"),
        ("How many unique products were transferred?",
         "SELECT COUNT(DISTINCT pName) FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N' AND dtlIsDel='N'"),
        ("列出所有出庫過的不重複商品名稱",
         "SELECT DISTINCT pName FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND dtlIsDel='N'"),
    ]

    # ================================================================
    # 5. Transfer 特殊場景（無 header amount）
    # ================================================================
    transfer_samples = [
        ("List all transfers from warehouse 台北倉",
         "SELECT DISTINCT TransferId, tfWhName, pName, qty FROM WP_M09.dbo.WP_vTransfer WHERE fWhName=N'台北倉' AND isDel='N' AND dtlIsDel='N'"),
        ("What products were transferred to warehouse 高雄倉 in December 2025?",
         "SELECT DISTINCT pName, SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vTransfer WHERE tfWhName=N'高雄倉' AND isDel='N' AND dtlIsDel='N' AND LEFT(TransferId,6)='202512' GROUP BY pName"),
        ("How many transfer orders were there in 2025?",
         "SELECT COUNT(DISTINCT TransferId) FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N' AND LEFT(TransferId,4)='2025'"),
        ("2025年12月從台北倉調撥了多少數量?",
         "SELECT SUM(qty) FROM WP_M09.dbo.WP_vTransfer WHERE fWhName=N'台北倉' AND isDel='N' AND dtlIsDel='N' AND LEFT(TransferId,6)='202512'"),
    ]

    # 合併所有擴增資料
    all_augmented = (
        outstock_samples +
        dedup_templates +
        isdel_pairs +
        distinct_samples +
        transfer_samples
    )

    for question, sql in all_augmented:
        augmented.append({
            "db_id": "WP_M09",
            "question": question,
            "query": sql,
            "source": "augmented",
        })

    print(f"\n  Data augmentation: {len(augmented)} samples generated")
    print(f"    OutStock:    {len(outstock_samples)}")
    print(f"    Subquery:    {len(dedup_templates)}")
    print(f"    isDel pairs: {len(isdel_pairs)}")
    print(f"    DISTINCT:    {len(distinct_samples)}")
    print(f"    Transfer:    {len(transfer_samples)}")

    return augmented


# ============================================================
# Utils
# ============================================================
def extract_table_from_sql(sql):
    m = re.search(r'WP_M09\.dbo\.(\w+)', sql)
    if m:
        return m.group(1)
    m = re.search(r'\[WP_M09\]\.\[dbo\]\.\[(\w+)\]', sql)
    return m.group(1) if m else ""


def normalize_query(sql):
    s = sql.strip().rstrip(';').strip()
    s = re.sub(r'\s+', ' ', s)
    return s.upper()


# ============================================================
# Build Chat Template prompts
# ============================================================
def build_system_prompt(schema_mode="full", table=None, include_rules=True):
    """
    建構 system prompt。
      schema_mode="full":   全 7 表 schema（Spider/BIRD 方式）
      schema_mode="single": 只目標表 schema（ablation）
      schema_mode="none":   無 schema（ablation baseline）
    """
    parts = ["You are an expert T-SQL assistant for WP_M09 database (SQL Server). Generate ONLY the SQL query. Do not explain."]

    if schema_mode == "full":
        parts.append(FULL_SCHEMA)
    elif schema_mode == "single" and table:
        schema = SINGLE_VIEW_SCHEMAS.get(table, "")
        view_list = "Available views: WP_vAcctIn, WP_vAcctOut, WP_vOutStock, WP_vTransfer, WP_vInventory, WP_vProduct, WP_vProvider"
        parts.append(view_list)
        parts.append(schema)
    # schema_mode=="none": 只有指示語句

    if include_rules:
        parts.append(BUSINESS_RULES)

    return "\n\n".join(parts)


def build_chat_text(system_prompt, question, sql, tokenizer):
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": question},
        {"role": "assistant", "content": sql},
    ]
    return tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=False)


# ============================================================
# Data Loading
# ============================================================
def load_data(tokenizer, schema_mode="full", include_rules=True, use_augment=True):
    # 1. Load original training data
    all_samples = []
    val_samples = []
    for path in TRAIN_PATHS:
        if not os.path.exists(path):
            print(f"  [WARN] {path} not found")
            continue
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        print(f"  Loaded {path}: {len(raw)} samples")
        all_samples.extend(raw)

    # 2. Augmentation (inline + external file already loaded via TRAIN_PATHS)
    if use_augment:
        aug_data = generate_augmented_data()
        all_samples.extend(aug_data)
    else:
        # Remove external augment file samples if --no-augment
        all_samples = [s for s in all_samples if s.get("source") != "augmented"]
        print("  [INFO] Data augmentation disabled (--no-augment)")

    print(f"\n  Total before dedup: {len(all_samples)}")

    # 3. Deduplicate
    seen = set()
    deduped = []
    for s in all_samples:
        norm = normalize_query(s.get("query", ""))
        if norm not in seen:
            seen.add(norm)
            deduped.append(s)
    print(f"  After dedup: {len(deduped)} (removed {len(all_samples) - len(deduped)})")

    # 4. Build chat template texts
    system_prompt_full = build_system_prompt(schema_mode=schema_mode, include_rules=include_rules)
    texts = []
    skipped = 0

    for s in deduped:
        question = s.get("question", "")
        sql = s.get("query", "").strip().rstrip(';').strip()
        table = extract_table_from_sql(sql)

        if not table:
            skipped += 1
            continue

        if schema_mode == "single":
            sys_prompt = build_system_prompt(schema_mode="single", table=table, include_rules=include_rules)
        else:
            sys_prompt = system_prompt_full

        text = build_chat_text(sys_prompt, question, sql, tokenizer)
        tok_len = len(tokenizer(text, truncation=False)["input_ids"])

        if tok_len > MAX_SEQ_LEN:
            skipped += 1
            continue

        texts.append({"text": text, "tok_len": tok_len, "table": table})

    print(f"  Included: {len(texts)}, Skipped (no table or too long): {skipped}")

    # 5. Statistics
    lengths = [t["tok_len"] for t in texts]
    print(f"\n  Token length: Min={min(lengths)}, Max={max(lengths)}, "
          f"Mean={statistics.mean(lengths):.0f}, Median={statistics.median(lengths):.0f}")
    over_max = sum(1 for l in lengths if l > MAX_SEQ_LEN)
    print(f"  Over {MAX_SEQ_LEN}: {over_max} ({over_max/len(lengths)*100:.1f}%)")

    table_cnt = Counter(t["table"] for t in texts)
    print(f"\n  View distribution:")
    for view, cnt in sorted(table_cnt.items()):
        print(f"    {view}: {cnt}")

    aug_cnt = sum(1 for s in deduped if s.get("source") == "augmented")
    subq_cnt = sum(1 for t in texts if t["text"].upper().count("SELECT") >= 3)  # system has SELECT in rules
    print(f"\n  Augmented samples included: {aug_cnt}")
    print(f"  Samples with subqueries: {subq_cnt}")

    # 6. Show sample
    print(f"\n  Sample prompt (first 800 chars):")
    print("-" * 60)
    print(texts[0]["text"][:800])
    print("-" * 60)

    dataset = Dataset.from_list([{"text": t["text"]} for t in texts])

    # Load validation dataset
    val_texts = []
    if os.path.exists(VAL_PATH):
        with open(VAL_PATH, "r", encoding="utf-8") as f:
            val_samples = json.load(f)
        system_prompt_val = build_system_prompt(schema_mode=schema_mode, include_rules=include_rules)
        for s in val_samples:
            question = s.get("question", "")
            sql = s.get("query", "").strip().rstrip(';').strip()
            if not question or not sql:
                continue
            if schema_mode == "single":
                table = extract_table_from_sql(sql)
                sys_prompt = build_system_prompt(schema_mode="single", table=table, include_rules=include_rules) if table else system_prompt_val
            else:
                sys_prompt = system_prompt_val
            text = build_chat_text(sys_prompt, question, sql, tokenizer)
            tok_len = len(tokenizer(text, truncation=False)["input_ids"])
            if tok_len <= MAX_SEQ_LEN:
                val_texts.append({"text": text})
        print(f"\n  Val samples loaded: {len(val_texts)} (from {VAL_PATH})")
    else:
        print(f"\n  [WARN] VAL_PATH not found: {VAL_PATH}")

    val_dataset = Dataset.from_list(val_texts) if val_texts else None
    return dataset, len(texts), val_dataset


# ============================================================
# Model loading
# ============================================================
def load_model_and_tokenizer():
    print(f"\nLoading base model: {MODEL_PATH} ...")
    bnb_cfg = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16,
        bnb_4bit_use_double_quant=True,
    )
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, use_fast=True)
    tokenizer.pad_token        = tokenizer.eos_token
    tokenizer.padding_side     = "right"
    tokenizer.model_max_length = MAX_SEQ_LEN

    model = AutoModelForCausalLM.from_pretrained(
        MODEL_PATH,
        quantization_config=bnb_cfg,
        device_map="auto",
        dtype=torch.bfloat16,
        attn_implementation="eager",
    )
    model.config.use_cache = False
    print("Base model loaded")
    return tokenizer, model


# ============================================================
# DoRA setup
# ============================================================
def apply_dora(model):
    lora_cfg = LoraConfig(
        r=LORA_R,
        lora_alpha=LORA_ALPHA,
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
        lora_dropout=LORA_DROPOUT,
        bias="none",
        task_type=TaskType.CAUSAL_LM,
        use_dora=USE_DORA,
    )
    model = get_peft_model(model, lora_cfg)
    model.print_trainable_parameters()
    print(f"Fine-tune: {'DoRA' if USE_DORA else 'LoRA'} (r={LORA_R}, alpha={LORA_ALPHA})")
    return model


# ============================================================
# Training
# ============================================================
def train(model, tokenizer, dataset, output_dir, val_dataset=None):
    os.makedirs(output_dir, exist_ok=True)
    has_eval = val_dataset is not None and len(val_dataset) > 0

    # 計算每 0.5 epoch 的 steps 數（更頻繁的 eval 以偵測過擬合）
    eff_batch = BATCH_SIZE * GRAD_ACCUM
    steps_per_epoch = max(1, len(dataset) // eff_batch)
    eval_save_steps = max(1, steps_per_epoch // 2)  # 每 0.5 epoch

    sft_cfg = SFTConfig(
        output_dir=output_dir,
        num_train_epochs=NUM_EPOCHS,
        per_device_train_batch_size=BATCH_SIZE,
        gradient_accumulation_steps=GRAD_ACCUM,
        learning_rate=LEARNING_RATE,
        lr_scheduler_type=LR_SCHEDULER,
        warmup_ratio=WARMUP_RATIO,
        weight_decay=WEIGHT_DECAY,
        optim="paged_adamw_8bit",
        bf16=True,
        logging_steps=10,
        save_strategy="steps" if has_eval else "epoch",
        save_steps=eval_save_steps,
        save_total_limit=5,
        eval_strategy="steps" if has_eval else "no",
        eval_steps=eval_save_steps,
        load_best_model_at_end=has_eval,
        metric_for_best_model="eval_loss" if has_eval else None,
        greater_is_better=False,
        report_to="none",
        dataloader_num_workers=0,
        dataset_text_field="text",
        packing=False,
    )

    # Early Stopping callback
    callbacks = []
    if has_eval:
        callbacks.append(EarlyStoppingCallback(
            early_stopping_patience=EARLY_STOPPING_PATIENCE,
        ))
        print(f"\n  Early Stopping: patience={EARLY_STOPPING_PATIENCE}, metric=eval_loss")

    trainer = SFTTrainer(
        model=model,
        processing_class=tokenizer,
        train_dataset=dataset,
        eval_dataset=val_dataset if has_eval else None,
        args=sft_cfg,
        callbacks=callbacks,
    )

    print(f"\nTraining:")
    print(f"  Samples:     {len(dataset)}")
    print(f"  Epochs:      {NUM_EPOCHS}")
    print(f"  Batch:       {BATCH_SIZE} x {GRAD_ACCUM} = {eff_batch}")
    print(f"  Steps/epoch: ~{steps_per_epoch}")
    print(f"  Total steps: ~{steps_per_epoch * NUM_EPOCHS}")
    print(f"  Eval every:  {eval_save_steps} steps (~0.5 epoch)")
    print(f"  LR:          {LEARNING_RATE} ({LR_SCHEDULER})")
    print(f"  MAX_SEQ_LEN: {MAX_SEQ_LEN}")
    print(f"  Output:      {output_dir}\n")

    # 不 resume — 這是全新訓練
    trainer.train()
    return trainer


# ============================================================
# Save
# ============================================================
def save_model(trainer, tokenizer, n_samples, output_dir, args):
    final_dir = os.path.join(output_dir, "final_model")
    os.makedirs(final_dir, exist_ok=True)

    trainer.model.save_pretrained(final_dir)
    tokenizer.save_pretrained(final_dir)

    info = {
        "base_model":       MODEL_PATH,
        "train_script":     "train__enterprise_v0324.py",
        "methodology":      "Spider/BIRD-style: Full-DB schema + Business rules (evidence) + Chat Template",
        "schema_mode":      args.schema,
        "include_rules":    not args.no_rules,
        "data_augmentation": not args.no_augment,
        "method":           "DoRA" if USE_DORA else "LoRA",
        "lora_r":           LORA_R,
        "lora_alpha":       LORA_ALPHA,
        "train_samples":    n_samples,
        "epochs":           NUM_EPOCHS,
        "effective_batch":  BATCH_SIZE * GRAD_ACCUM,
        "learning_rate":    LEARNING_RATE,
        "max_seq_len":      MAX_SEQ_LEN,
        "date":             DATE_STR,
        "early_stopping":   f"patience={EARLY_STOPPING_PATIENCE}",
        "final_loss":       round(trainer.state.log_history[-1].get("loss", 0), 4),
        "experiment_notes": (
            "Full 7-view schema in every prompt (Spider/BIRD methodology). "
            "Model learns table selection without keyword inference. "
            f"Augmented: {'Yes' if not args.no_augment else 'No'}. "
            f"Rules: {'Yes' if not args.no_rules else 'No'}."
        ),
    }
    with open(os.path.join(final_dir, "training_info.json"), "w") as f:
        json.dump(info, f, indent=2, ensure_ascii=False)

    print(f"\nModel saved: {final_dir}")
    for k, v in info.items():
        print(f"  {k}: {v}")
    return final_dir


# ============================================================
# Main
# ============================================================
def parse_args():
    p = argparse.ArgumentParser(description="Enterprise Text-to-SQL Training (Spider/BIRD methodology)")
    p.add_argument("--schema", choices=["full", "single", "none"], default="full",
                   help="Schema mode: full=all 7 views (default), single=target only, none=no schema (ablation)")
    p.add_argument("--no-augment", action="store_true", help="Disable data augmentation (ablation)")
    p.add_argument("--no-rules", action="store_true", help="Disable business rules (ablation)")
    p.add_argument("--output-suffix", type=str, default="", help="Custom output directory suffix")
    return p.parse_args()


def main():
    args = parse_args()

    suffix = args.output_suffix or f"{args.schema}"
    if args.no_augment: suffix += "_noaug"
    if args.no_rules: suffix += "_norule"
    output_dir = f"outputs/models/enterprise_{suffix}_{DATE_STR}"

    print("=" * 70)
    print(f"Enterprise Text-to-SQL Training v{DATE_STR} (anti-overfit)")
    print(f"  Methodology: Spider/BIRD-style (Full-Schema + Evidence)")
    print(f"  Schema mode: {args.schema}")
    print(f"  Rules:       {'Yes' if not args.no_rules else 'No (ablation)'}")
    print(f"  Augment:     {'Yes' if not args.no_augment else 'No (ablation)'}")
    print(f"  Output:      {output_dir}")
    print("=" * 70)

    tokenizer, model = load_model_and_tokenizer()
    dataset, n_samples, val_dataset = load_data(tokenizer, schema_mode=args.schema,
                                                 include_rules=not args.no_rules,
                                                 use_augment=not args.no_augment)
    model = apply_dora(model)
    trainer = train(model, tokenizer, dataset, output_dir, val_dataset=val_dataset)
    final_dir = save_model(trainer, tokenizer, n_samples, output_dir, args)

    # Print evaluation commands
    print("\n" + "=" * 70)
    print("Training complete! Evaluation commands:")
    print("=" * 70)
    print(f"\n# Main evaluation (EM + EX):")
    print(f"python eval__benchmark_official.py --mode wp_m09 \\")
    print(f"    --model {final_dir} \\")
    print(f"    --gold {VAL_PATH} \\")
    print(f"    --output outputs/eval_enterprise_{suffix}_{DATE_STR}.json \\")
    print(f'    --db-host "SHANE\\SQLEXPRESS" --db-trusted')

    print(f"\n# Spider-style component evaluation:")
    print(f"python eval__spider_style.py \\")
    print(f"    --model {final_dir} \\")
    print(f"    --gold {VAL_PATH} \\")
    print(f"    --output outputs/eval_enterprise_{suffix}_{DATE_STR}_spider.json")

    if args.schema == "full":
        print(f"\n# Ablation experiments:")
        print(f"python train__enterprise_v0324.py --schema single       # Single-table (v0322 style)")
        print(f"python train__enterprise_v0324.py --schema none         # No schema baseline")
        print(f"python train__enterprise_v0324.py --no-augment          # No augmentation")
        print(f"python train__enterprise_v0324.py --no-rules            # No business rules")


if __name__ == "__main__":
    main()
