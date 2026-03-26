#!/usr/bin/env python3
"""
traindata_gen__supplement.py
Generates additional ~1200 English Spider-format samples to supplement
the existing 1192 from traindata_gen__claude_2000_english.py.
Merges into train_claude_en_2000.json + validation_claude_en.json.
"""

import json, re, random
from pathlib import Path

random.seed(99)
DB  = "WP_M09"
PRE = f"{DB}.dbo."
V   = PRE

def toks(q):
    return re.findall(r"[A-Za-z_][A-Za-z0-9_.]*|'[^']*'|[0-9]+|[().,;*<>=!%]+", q)

def entry(question, sql):
    t = toks(sql)
    tnv = ["'value'" if (x.startswith("'") or (x.isdigit() and len(x) > 2)) else x for x in t]
    return {"db_id": DB, "query": sql, "query_toks": t,
            "query_toks_no_value": tnv, "question": question,
            "question_toks": question.split(), "sql": {}}

def multi(pairs):
    return [entry(q, s) for q, s in pairs]

# ── Constants ──
MEM_IDS   = ["A006","A007","A008","A009","A010","A011","A012"]
MEM_NAMES = ["麻竹園","旅遊部","食品公司","茶葉行","農產品行","觀光農場","山林農業"]
PV_SNS    = ["1","2","3","5","8","10","15","21","33"]
PV_NAMES  = ["實垣有限公司","永豐農產","台灣好茶","綠野農業","山林食品",
             "天然農場","有機世界","台灣農業","健康食品","品茶苑"]
PROD_NAMES = ["益全香米","竹炭冬筍餅","烏龍茶葉","金萱茶包","台灣高山茶",
              "有機烏龍茶","碧螺春","龍井茶","普洱茶","紅豆湯圓",
              "花蓮米","黑糖茶磚","東方美人茶","阿里山茶","玫瑰花茶"]
WH_NAMES  = ["特產中心","北區倉庫","南區倉庫","中央倉庫","冷藏倉","主倉","東區倉庫"]
MONTHS_2025 = ["202501","202502","202503","202504","202505","202506",
               "202507","202508","202509","202510","202511","202512"]
MONTHS_2024 = ["202401","202403","202405","202406","202407","202409","202411","202412"]
KEYWORDS   = ["茶","米","水","冬","春","香","竹","有機","花","豆","高山","烏龍","金萱","龍井"]
BARCODES   = ["4712070722015","4710953082676","4710632001318","4719865002441","4715820003021"]
AMT_THRESHOLDS   = [500,1000,2000,3000,5000,8000,10000,15000,20000]
QTY_THRESHOLDS   = [0,5,10,15,20,30,50,100]
PRICE_THRESHOLDS = [30,50,80,100,150,200,300,500]
EMP_IDS    = ["001","002","003","A01","B02"]
DATES_ALL  = ["20251201","20251205","20251210","20251215","20251220","20251225",
              "20251101","20251110","20251115","20251120","20251125",
              "20251001","20251015","20251020","20250901","20250915",
              "20250801","20250815","20250701","20250615"]


# ════════════════════════════════════════════════════════════════
#  EXTRA: WP_vAcctIn  (~200 new)
# ════════════════════════════════════════════════════════════════
def extra_acct_in():
    s = []
    # month × member combos
    for m in MONTHS_2025[:6]:
        for mid in MEM_IDS[:4]:
            s.append(entry(
                f"What is the accounts receivable total for member {mid} in {m}?",
                f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vAcctIn WHERE memId='{mid}' AND LEFT(acctInId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"
            ))
    # 2024 months
    for m in MONTHS_2024:
        s += multi([
            (f"List accounts receivable from {m}",
             f"SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE LEFT(acctInId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
            (f"Total accounts receivable for {m}",
             f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vAcctIn WHERE LEFT(acctInId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # remaining keywords
    for kw in KEYWORDS[8:]:
        s.append(entry(
            f"Show accounts receivable containing products with keyword {kw}",
            f"SELECT DISTINCT acctInId, pName FROM {V}WP_vAcctIn WHERE pName LIKE N'%{kw}%' AND isDel='N' AND dtlIsDel='N';"
        ))
    # remaining products
    for pn in PROD_NAMES[8:]:
        s += multi([
            (f"What accounts receivable contain {pn}?",
             f"SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"),
            (f"Total revenue from {pn} in accounts receivable",
             f"SELECT SUM(oStkDtlAmt) AS total FROM {V}WP_vAcctIn WHERE pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # additional dates
    for d in DATES_ALL[8:16]:
        s.append(entry(
            f"What accounts receivable were issued on {d}?",
            f"SELECT DISTINCT acctInId, amount, memName FROM {V}WP_vAcctIn WHERE LEFT(acctInId,8)='{d}' AND isDel='N' AND dtlIsDel='N';"
        ))
    # amount range combinations
    s += multi([
        ("Show accounts receivable between 1000 and 5000",
         f"SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE amount BETWEEN 1000 AND 5000 AND isDel='N' AND dtlIsDel='N';"),
        ("Find accounts receivable under 500",
         f"SELECT DISTINCT acctInId, amount, memName FROM {V}WP_vAcctIn WHERE amount < 500 AND isDel='N' AND dtlIsDel='N';"),
        ("Find accounts receivable with amount under 1000",
         f"SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE amount < 1000 AND isDel='N' AND dtlIsDel='N';"),
        ("Show accounts receivable equal to 14700",
         f"SELECT DISTINCT acctInId, memName FROM {V}WP_vAcctIn WHERE amount = 14700 AND isDel='N' AND dtlIsDel='N';"),
        ("Show total outstock amounts per member",
         f"SELECT memId, memName, SUM(outStkAmtTotal) AS total FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY memId, memName ORDER BY total DESC;"),
        ("Which accounts receivable have the highest discount?",
         f"SELECT TOP 5 acctInId, pName, dtlDiscnt FROM {V}WP_vAcctIn WHERE dtlDiscnt > 0 AND isDel='N' AND dtlIsDel='N' ORDER BY dtlDiscnt DESC;"),
        ("Show accounts receivable with memo",
         f"SELECT DISTINCT acctInId, memo FROM {V}WP_vAcctIn WHERE memo IS NOT NULL AND memo <> '' AND isDel='N' AND dtlIsDel='N';"),
        ("List all distinct employee IDs in accounts receivable",
         f"SELECT DISTINCT empId FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';"),
        ("Count accounts receivable by employee",
         f"SELECT empId, empName, COUNT(DISTINCT acctInId) AS count FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY empId, empName ORDER BY count DESC;"),
        ("Show total by payment type in accounts receivable",
         f"SELECT payType, SUM(DISTINCT amount) AS total FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY payType;"),
        ("How many accounts receivable have quantity above 20?",
         f"SELECT COUNT(DISTINCT acctInId) AS count FROM {V}WP_vAcctIn WHERE oStkDtlQty > 20 AND isDel='N' AND dtlIsDel='N';"),
        ("What is the total accounts receivable per year?",
         f"SELECT LEFT(acctInId,4) AS year, SUM(DISTINCT amount) AS total FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY LEFT(acctInId,4) ORDER BY year;"),
    ])
    return s


# ════════════════════════════════════════════════════════════════
#  EXTRA: WP_vAcctOut  (~200 new)
# ════════════════════════════════════════════════════════════════
def extra_acct_out():
    s = []
    # month × supplier combos
    for m in MONTHS_2025[:6]:
        for pvn in PV_NAMES[:4]:
            s.append(entry(
                f"What is the total payable to {pvn} in {m}?",
                f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vAcctOut WHERE pvName=N'{pvn}' AND LEFT(acctOutId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"
            ))
    # 2024 months
    for m in MONTHS_2024:
        s += multi([
            (f"List accounts payable from {m}",
             f"SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE LEFT(acctOutId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
            (f"How much was paid in {m}?",
             f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vAcctOut WHERE LEFT(acctOutId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # remaining products
    for pn in PROD_NAMES[8:]:
        s += multi([
            (f"Find accounts payable that include {pn}",
             f"SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"),
            (f"What is the total purchase amount for {pn}?",
             f"SELECT SUM(amtTotal) AS total FROM {V}WP_vAcctOut WHERE pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # remaining keywords
    for kw in KEYWORDS[6:]:
        s.append(entry(
            f"Find accounts payable for products related to {kw}",
            f"SELECT DISTINCT acctOutId, pName FROM {V}WP_vAcctOut WHERE pName LIKE N'%{kw}%' AND isDel='N' AND dtlIsDel='N';"
        ))
    # extra analytics
    s += multi([
        ("Show accounts payable under 1000",
         f"SELECT DISTINCT acctOutId, amount, pvName FROM {V}WP_vAcctOut WHERE amount < 1000 AND isDel='N' AND dtlIsDel='N';"),
        ("Find accounts payable between 3000 and 8000",
         f"SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE amount BETWEEN 3000 AND 8000 AND isDel='N' AND dtlIsDel='N';"),
        ("Show accounts payable sorted by date ascending",
         f"SELECT DISTINCT acctOutId, amount, pvName FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' ORDER BY acctOutId ASC;"),
        ("Which supplier received the largest single payment?",
         f"SELECT TOP 1 acctOutId, pvName, amount FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' ORDER BY amount DESC;"),
        ("Show total purchase per product category",
         f"SELECT pkName, SUM(amtTotal) AS total FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' GROUP BY pkName ORDER BY total DESC;"),
        ("How many distinct products appear in accounts payable?",
         f"SELECT COUNT(DISTINCT pName) AS count FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N';"),
        ("Show accounts payable with transfer amount over 5000",
         f"SELECT DISTINCT acctOutId, transAmt FROM {V}WP_vAcctOut WHERE transAmt > 5000 AND isDel='N' AND dtlIsDel='N';"),
        ("Show accounts payable with transfer amount over 10000",
         f"SELECT DISTINCT acctOutId, transAmt FROM {V}WP_vAcctOut WHERE transAmt > 10000 AND isDel='N' AND dtlIsDel='N';"),
        ("List accounts payable with non-taxable amounts",
         f"SELECT DISTINCT acctOutId, amtNoneTax FROM {V}WP_vAcctOut WHERE amtNoneTax > 0 AND isDel='N' AND dtlIsDel='N';"),
        ("Show average purchase amount per supplier",
         f"SELECT pvName, AVG(DISTINCT amount) AS avg_amt FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' GROUP BY pvName ORDER BY avg_amt DESC;"),
        ("Total accounts payable per year",
         f"SELECT LEFT(acctOutId,4) AS year, SUM(DISTINCT amount) AS total FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' GROUP BY LEFT(acctOutId,4) ORDER BY year;"),
        ("Show suppliers appearing in accounts payable in 2025",
         f"SELECT DISTINCT pvName FROM {V}WP_vAcctOut WHERE LEFT(acctOutId,4)='2025' AND isDel='N' AND dtlIsDel='N';"),
        ("Count accounts payable per supplier in 2025",
         f"SELECT pvName, COUNT(DISTINCT acctOutId) AS count FROM {V}WP_vAcctOut WHERE LEFT(acctOutId,4)='2025' AND isDel='N' AND dtlIsDel='N' GROUP BY pvName ORDER BY count DESC;"),
    ])
    return s


# ════════════════════════════════════════════════════════════════
#  EXTRA: WP_vOutStock  (~180 new)
# ════════════════════════════════════════════════════════════════
def extra_out_stock():
    s = []
    # month × member
    for m in MONTHS_2025[:6]:
        for mid in MEM_IDS[:4]:
            s.append(entry(
                f"How much did member {mid} spend on sales orders in {m}?",
                f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vOutStock WHERE memId='{mid}' AND LEFT(OutStkId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"
            ))
    # 2024 months
    for m in MONTHS_2024:
        s += multi([
            (f"Show sales orders from {m}",
             f"SELECT DISTINCT OutStkId, amount FROM {V}WP_vOutStock WHERE LEFT(OutStkId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
            (f"Total sales revenue for {m}",
             f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vOutStock WHERE LEFT(OutStkId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # remaining keywords
    for kw in KEYWORDS[8:]:
        s.append(entry(
            f"Find sales orders with product containing keyword {kw}",
            f"SELECT DISTINCT OutStkId, pName FROM {V}WP_vOutStock WHERE pName LIKE N'%{kw}%' AND isDel='N' AND dtlIsDel='N';"
        ))
    # extra analytics
    s += multi([
        ("Show sales orders from 2024",
         f"SELECT DISTINCT OutStkId, amount, memName FROM {V}WP_vOutStock WHERE LEFT(OutStkId,4)='2024' AND isDel='N' AND dtlIsDel='N';"),
        ("Compare total sales between 2024 and 2025",
         f"SELECT LEFT(OutStkId,4) AS year, SUM(DISTINCT amount) AS total FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' GROUP BY LEFT(OutStkId,4) ORDER BY year;"),
        ("Find members with zero sales orders",
         f"SELECT DISTINCT memId, memName FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' GROUP BY memId, memName HAVING SUM(DISTINCT amount) = 0;"),
        ("Show total unsettled outstanding amount",
         f"SELECT SUM(outLeft) AS total_outstanding FROM {V}WP_vOutStock WHERE outType <> 2 AND isDel='N' AND dtlIsDel='N';"),
        ("Which products have the highest discount totals?",
         f"SELECT TOP 5 pName, SUM(dtlDiscnt) AS total_disc FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY total_disc DESC;"),
        ("Show total tax collected per month in 2025",
         f"SELECT LEFT(OutStkId,6) AS month, SUM(DISTINCT tax) AS total_tax FROM {V}WP_vOutStock WHERE LEFT(OutStkId,4)='2025' AND isDel='N' AND dtlIsDel='N' GROUP BY LEFT(OutStkId,6) ORDER BY month;"),
        ("List sales orders below 500",
         f"SELECT DISTINCT OutStkId, amount, memName FROM {V}WP_vOutStock WHERE amount < 500 AND isDel='N' AND dtlIsDel='N';"),
        ("Find sales orders above 20000",
         f"SELECT DISTINCT OutStkId, amount, memName FROM {V}WP_vOutStock WHERE amount > 20000 AND isDel='N' AND dtlIsDel='N';"),
        ("What is the total quantity sold per product in 2025?",
         f"SELECT pName, SUM(qty) AS total_qty FROM {V}WP_vOutStock WHERE LEFT(OutStkId,4)='2025' AND isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY total_qty DESC;"),
        ("Show sales order count and revenue by member",
         f"SELECT memId, memName, COUNT(DISTINCT OutStkId) AS orders, SUM(DISTINCT amount) AS total FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' GROUP BY memId, memName ORDER BY total DESC;"),
        ("What percentage of sales orders are fully settled?",
         f"SELECT outType, COUNT(DISTINCT OutStkId) AS count FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' GROUP BY outType ORDER BY outType;"),
        ("Show sales orders sorted by amount descending",
         f"SELECT DISTINCT OutStkId, amount, memName FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' ORDER BY amount DESC;"),
    ])
    return s


# ════════════════════════════════════════════════════════════════
#  EXTRA: WP_vTransfer  (~200 new)
# ════════════════════════════════════════════════════════════════
def extra_transfer():
    s = []
    # product × warehouse combos
    for pn in PROD_NAMES[10:]:
        s += multi([
            (f"Show all transfers involving product {pn}",
             f"SELECT TransferId, qty, fWhName, tfWhName FROM {V}WP_vTransfer WHERE pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"),
            (f"How many units of {pn} were transferred in total?",
             f"SELECT SUM(qty) AS total_qty FROM {V}WP_vTransfer WHERE pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # from→to warehouse pairs
    for i, wf in enumerate(WH_NAMES[:4]):
        for wt in WH_NAMES[i+1:5]:
            s.append(entry(
                f"Show transfers from {wf} to {wt}",
                f"SELECT TransferId, pName, qty FROM {V}WP_vTransfer WHERE fWhName=N'{wf}' AND tfWhName=N'{wt}' AND isDel='N' AND dtlIsDel='N';"
            ))
    # 2024 months
    for m in MONTHS_2024:
        s += multi([
            (f"List transfers from {m}",
             f"SELECT TransferId, pName, qty FROM {V}WP_vTransfer WHERE LEFT(TransferId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
            (f"Total quantity transferred in {m}",
             f"SELECT SUM(qty) AS total_qty FROM {V}WP_vTransfer WHERE LEFT(TransferId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # extra dates
    for d in DATES_ALL[8:14]:
        s.append(entry(
            f"List all transfers on {d}",
            f"SELECT TransferId, pName, qty, fWhName, tfWhName FROM {V}WP_vTransfer WHERE LEFT(TransferId,8)='{d}' AND isDel='N' AND dtlIsDel='N';"
        ))
    # remaining keywords
    for kw in KEYWORDS[6:]:
        s.append(entry(
            f"Show transfers for products containing {kw}",
            f"SELECT TransferId, pName, qty FROM {V}WP_vTransfer WHERE pName LIKE N'%{kw}%' AND isDel='N' AND dtlIsDel='N';"
        ))
    # extra analytics
    s += multi([
        ("Show all transfers sorted by quantity descending",
         f"SELECT TransferId, pName, qty FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' ORDER BY qty DESC;"),
        ("How many transfers happened each month in 2025?",
         f"SELECT LEFT(TransferId,6) AS month, COUNT(*) AS count FROM {V}WP_vTransfer WHERE LEFT(TransferId,4)='2025' AND isDel='N' AND dtlIsDel='N' GROUP BY LEFT(TransferId,6) ORDER BY month;"),
        ("Which route (source-dest pair) has the most transfers?",
         f"SELECT TOP 1 fWhName, tfWhName, COUNT(*) AS count FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY fWhName, tfWhName ORDER BY count DESC;"),
        ("Show transfers with qty between 1 and 5",
         f"SELECT TransferId, pName, qty FROM {V}WP_vTransfer WHERE qty BETWEEN 1 AND 5 AND isDel='N' AND dtlIsDel='N';"),
        ("Show transfers with qty between 20 and 100",
         f"SELECT TransferId, pName, qty FROM {V}WP_vTransfer WHERE qty BETWEEN 20 AND 100 AND isDel='N' AND dtlIsDel='N';"),
        ("Compare transfer volumes across years",
         f"SELECT LEFT(TransferId,4) AS year, COUNT(*) AS count, SUM(qty) AS total_qty FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY LEFT(TransferId,4) ORDER BY year;"),
        ("List employees with most transfer activity",
         f"SELECT empId, COUNT(*) AS count FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY empId ORDER BY count DESC;"),
        ("Which product was transferred to the most different warehouses?",
         f"SELECT TOP 1 pName, COUNT(DISTINCT tfWhName) AS dest_count FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY dest_count DESC;"),
        ("Show average transfer quantity per warehouse route",
         f"SELECT fWhName, tfWhName, AVG(qty) AS avg_qty FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY fWhName, tfWhName ORDER BY avg_qty DESC;"),
        ("Show top 10 largest single transfers by quantity",
         f"SELECT TOP 10 TransferId, pName, qty, fWhName, tfWhName FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' ORDER BY qty DESC;"),
        ("Find transfers in 2024",
         f"SELECT TransferId, pName, qty FROM {V}WP_vTransfer WHERE LEFT(TransferId,4)='2024' AND isDel='N' AND dtlIsDel='N';"),
    ])
    return s


# ════════════════════════════════════════════════════════════════
#  EXTRA: WP_vInventory  — NO isDel  (~200 new)
# ════════════════════════════════════════════════════════════════
def extra_inventory():
    s = []
    # warehouse × price threshold combos
    for wh in WH_NAMES:
        for pt in PRICE_THRESHOLDS[:3]:
            s.append(entry(
                f"List products in warehouse {wh} with standard price above {pt}",
                f"SELECT pName, qty, priceStd FROM {V}WP_vInventory WHERE WarehouseName=N'{wh}' AND priceStd > {pt};"
            ))
    # remaining keywords
    for kw in KEYWORDS:
        s += multi([
            (f"Show inventory for products with name containing {kw}",
             f"SELECT pName, qty, WarehouseName FROM {V}WP_vInventory WHERE pName LIKE N'%{kw}%';"),
            (f"What is the total stock of products containing {kw}?",
             f"SELECT SUM(qty) AS total_qty FROM {V}WP_vInventory WHERE pName LIKE N'%{kw}%';"),
        ])
    # all products
    for pn in PROD_NAMES:
        s += multi([
            (f"What is the total stock for {pn}?",
             f"SELECT SUM(qty) AS total_qty FROM {V}WP_vInventory WHERE pName=N'{pn}';"),
            (f"Show {pn} inventory by warehouse",
             f"SELECT WarehouseName, SUM(qty) AS qty FROM {V}WP_vInventory WHERE pName=N'{pn}' GROUP BY WarehouseName ORDER BY qty DESC;"),
        ])
    # extra analytics
    s += multi([
        ("Show inventory grouped by product category",
         f"SELECT pkName, SUM(qty) AS total_qty FROM {V}WP_vInventory GROUP BY pkName ORDER BY total_qty DESC;"),
        ("What is the inventory value per warehouse?",
         f"SELECT WarehouseName, SUM(qty * costAvg) AS total_value FROM {V}WP_vInventory GROUP BY WarehouseName ORDER BY total_value DESC;"),
        ("List products whose average cost exceeds 200",
         f"SELECT DISTINCT pName, costAvg FROM {V}WP_vInventory WHERE costAvg > 200 ORDER BY costAvg DESC;"),
        ("Show products with average cost between 50 and 100",
         f"SELECT DISTINCT pName, costAvg FROM {V}WP_vInventory WHERE costAvg BETWEEN 50 AND 100;"),
        ("Show inventory where price is double the average cost",
         f"SELECT DISTINCT pName, priceStd, costAvg FROM {V}WP_vInventory WHERE priceStd > costAvg * 2;"),
        ("List top 10 products by inventory quantity",
         f"SELECT TOP 10 pName, SUM(qty) AS total_qty FROM {V}WP_vInventory GROUP BY pName ORDER BY total_qty DESC;"),
        ("Which warehouse has the highest inventory value?",
         f"SELECT TOP 1 WarehouseName, SUM(qty * costAvg) AS value FROM {V}WP_vInventory GROUP BY WarehouseName ORDER BY value DESC;"),
        ("Show products with quantity above safe stock in all warehouses",
         f"SELECT pName, SUM(qty) AS total_qty, SUM(qtySafe) AS total_safe FROM {V}WP_vInventory GROUP BY pName HAVING SUM(qty) > SUM(qtySafe);"),
        ("List products supplied by 實垣有限公司 in inventory",
         f"SELECT DISTINCT pName, qty FROM {V}WP_vInventory WHERE pvName=N'實垣有限公司';"),
        ("List products supplied by 台灣好茶 in inventory",
         f"SELECT DISTINCT pName, qty FROM {V}WP_vInventory WHERE pvName=N'台灣好茶';"),
        ("Show count of product types per warehouse",
         f"SELECT WarehouseName, COUNT(DISTINCT pNo) AS product_types FROM {V}WP_vInventory GROUP BY WarehouseName ORDER BY product_types DESC;"),
        ("What is the highest priced product in inventory?",
         f"SELECT TOP 1 pName, priceStd FROM {V}WP_vInventory ORDER BY priceStd DESC;"),
        ("Show inventory with standard price under 50",
         f"SELECT DISTINCT pName, priceStd, qty FROM {V}WP_vInventory WHERE priceStd < 50 ORDER BY priceStd;"),
        ("List inventory records with barcodes",
         f"SELECT DISTINCT pName, pBarcode FROM {V}WP_vInventory WHERE pBarcode IS NOT NULL AND pBarcode <> '' ORDER BY pName;"),
        ("Show total inventory value by supplier",
         f"SELECT pvName, SUM(qty * costAvg) AS total_value FROM {V}WP_vInventory GROUP BY pvName ORDER BY total_value DESC;"),
    ])
    return s


# ════════════════════════════════════════════════════════════════
#  EXTRA: WP_vProduct  — NO isDel  (~200 new)
# ════════════════════════════════════════════════════════════════
def extra_product():
    s = []
    # all suppliers × price thresholds
    for pvn in PV_NAMES:
        s += multi([
            (f"List all products supplied by {pvn}",
             f"SELECT pNo, pName FROM {V}WP_vProduct WHERE pvName=N'{pvn}';"),
            (f"What is the total inventory for products from {pvn}?",
             f"SELECT SUM(qtyNow) AS total_qty FROM {V}WP_vProduct WHERE pvName=N'{pvn}';"),
            (f"How many products does {pvn} supply?",
             f"SELECT COUNT(*) AS count FROM {V}WP_vProduct WHERE pvName=N'{pvn}';"),
        ])
    # all keywords
    for kw in KEYWORDS:
        s += multi([
            (f"Show products with name containing {kw}",
             f"SELECT pNo, pName FROM {V}WP_vProduct WHERE pName LIKE N'%{kw}%';"),
            (f"How many products contain {kw} in their name?",
             f"SELECT COUNT(*) AS count FROM {V}WP_vProduct WHERE pName LIKE N'%{kw}%';"),
        ])
    # price thresholds
    for pt in PRICE_THRESHOLDS:
        s += multi([
            (f"List products with standard price above {pt}",
             f"SELECT pNo, pName, priceStd FROM {V}WP_vProduct WHERE priceStd > {pt} ORDER BY priceStd DESC;"),
            (f"Show products priced below {pt}",
             f"SELECT pNo, pName, priceStd FROM {V}WP_vProduct WHERE priceStd < {pt} ORDER BY priceStd;"),
        ])
    # extra analytics
    s += multi([
        ("How many products are below safe stock?",
         f"SELECT COUNT(*) AS count FROM {V}WP_vProduct WHERE qtyNow < qtySafe;"),
        ("What is the total inventory value in the product catalog?",
         f"SELECT SUM(qtyNow * costAvg) AS total_value FROM {V}WP_vProduct;"),
        ("Show products grouped by category with average price",
         f"SELECT pkName, COUNT(*) AS count, AVG(priceStd) AS avg_price FROM {V}WP_vProduct GROUP BY pkName ORDER BY avg_price DESC;"),
        ("Which product has the highest average cost?",
         f"SELECT TOP 1 pNo, pName, costAvg FROM {V}WP_vProduct ORDER BY costAvg DESC;"),
        ("Show distinct product categories",
         f"SELECT DISTINCT pkName FROM {V}WP_vProduct ORDER BY pkName;"),
        ("How many distinct categories exist in the product catalog?",
         f"SELECT COUNT(DISTINCT pkName) AS count FROM {V}WP_vProduct;"),
        ("Show products with zero current stock",
         f"SELECT pNo, pName FROM {V}WP_vProduct WHERE qtyNow = 0;"),
        ("List products with stock equal to safe stock",
         f"SELECT pNo, pName, qtyNow FROM {V}WP_vProduct WHERE qtyNow = qtySafe;"),
        ("Show top 5 most expensive products",
         f"SELECT TOP 5 pNo, pName, priceStd FROM {V}WP_vProduct ORDER BY priceStd DESC;"),
        ("Show top 5 cheapest products",
         f"SELECT TOP 5 pNo, pName, priceStd FROM {V}WP_vProduct ORDER BY priceStd ASC;"),
        ("What is the product with the highest current stock?",
         f"SELECT TOP 1 pNo, pName, qtyNow FROM {V}WP_vProduct ORDER BY qtyNow DESC;"),
        ("List products with barcode",
         f"SELECT pNo, pName, pBarcode FROM {V}WP_vProduct WHERE pBarcode IS NOT NULL AND pBarcode <> '';"),
        ("Show product added in 202501",
         f"SELECT pNo, pName FROM {V}WP_vProduct WHERE pNo LIKE '202501%';"),
        ("Show products added in 202502",
         f"SELECT pNo, pName FROM {V}WP_vProduct WHERE pNo LIKE '202502%';"),
        ("Show products added in 202503",
         f"SELECT pNo, pName FROM {V}WP_vProduct WHERE pNo LIKE '202503%';"),
        ("Show products added in 202504",
         f"SELECT pNo, pName FROM {V}WP_vProduct WHERE pNo LIKE '202504%';"),
        ("Show products added in 202410",
         f"SELECT pNo, pName FROM {V}WP_vProduct WHERE pNo LIKE '202410%';"),
        ("Show products added in 202411",
         f"SELECT pNo, pName FROM {V}WP_vProduct WHERE pNo LIKE '202411%';"),
        ("Show products added in 202412",
         f"SELECT pNo, pName FROM {V}WP_vProduct WHERE pNo LIKE '202412%';"),
        ("List products with cost above 300",
         f"SELECT pNo, pName, costAvg FROM {V}WP_vProduct WHERE costAvg > 300 ORDER BY costAvg DESC;"),
        ("Show products with standard price between 100 and 300",
         f"SELECT pNo, pName, priceStd FROM {V}WP_vProduct WHERE priceStd BETWEEN 100 AND 300;"),
        ("Which supplier has the highest average product price?",
         f"SELECT TOP 1 pvName, AVG(priceStd) AS avg_price FROM {V}WP_vProduct GROUP BY pvName ORDER BY avg_price DESC;"),
        ("Show product count per supplier",
         f"SELECT pvName, COUNT(*) AS count FROM {V}WP_vProduct GROUP BY pvName ORDER BY count DESC;"),
    ])
    return s


# ════════════════════════════════════════════════════════════════
#  EXTRA: WP_vProvider  — NO isDel  (~250 new)
# ════════════════════════════════════════════════════════════════
def extra_provider():
    s = []
    # all suppliers
    for pvn in PV_NAMES:
        s += multi([
            (f"What is the contact information for supplier {pvn}?",
             f"SELECT pvSn, pvName, pvTel FROM {V}WP_vProvider WHERE pvName=N'{pvn}';"),
            (f"Show address details for supplier {pvn}",
             f"SELECT pvSn, pvName, pvAddr FROM {V}WP_vProvider WHERE pvName=N'{pvn}';"),
            (f"Is supplier {pvn} active?",
             f"SELECT pvSn, pvName, isSale FROM {V}WP_vProvider WHERE pvName=N'{pvn}';"),
            (f"What is the discount rate for supplier {pvn}?",
             f"SELECT pvSn, pvName, discount FROM {V}WP_vProvider WHERE pvName=N'{pvn}';"),
        ])
    # all pvSn
    for pvs in PV_SNS:
        s += multi([
            (f"Show details for supplier with ID {pvs}",
             f"SELECT pvSn, pvName, pvTel, pvAddr FROM {V}WP_vProvider WHERE pvSn='{pvs}';"),
            (f"What is the discount for supplier {pvs}?",
             f"SELECT pvSn, pvName, discount FROM {V}WP_vProvider WHERE pvSn='{pvs}';"),
        ])
    # all keywords
    for kw in KEYWORDS:
        s.append(entry(
            f"Find suppliers with name containing {kw}",
            f"SELECT pvSn, pvName FROM {V}WP_vProvider WHERE pvName LIKE N'%{kw}%';"
        ))
    # discount thresholds
    for dt in [5, 10, 15, 20, 25, 30]:
        s += multi([
            (f"List suppliers with discount above {dt}%",
             f"SELECT pvSn, pvName, discount FROM {V}WP_vProvider WHERE discount > {dt} ORDER BY discount DESC;"),
            (f"Show suppliers offering less than {dt}% discount",
             f"SELECT pvSn, pvName, discount FROM {V}WP_vProvider WHERE discount < {dt} ORDER BY discount;"),
        ])
    # extra analytics
    s += multi([
        ("List all active suppliers",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider WHERE isSale='Y' ORDER BY pvName;"),
        ("List all inactive suppliers",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider WHERE isSale='N' ORDER BY pvName;"),
        ("How many active suppliers are there?",
         f"SELECT COUNT(*) AS count FROM {V}WP_vProvider WHERE isSale='Y';"),
        ("How many inactive suppliers are there?",
         f"SELECT COUNT(*) AS count FROM {V}WP_vProvider WHERE isSale='N';"),
        ("Show suppliers with no discount",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider WHERE discount = 0 OR discount IS NULL;"),
        ("What is the average discount across all suppliers?",
         f"SELECT AVG(discount) AS avg_discount FROM {V}WP_vProvider;"),
        ("Which supplier has the highest discount?",
         f"SELECT TOP 1 pvSn, pvName, discount FROM {V}WP_vProvider ORDER BY discount DESC;"),
        ("Show all supplier names and their telephone numbers",
         f"SELECT pvName, pvTel FROM {V}WP_vProvider ORDER BY pvName;"),
        ("List suppliers sorted by name alphabetically",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider ORDER BY pvName;"),
        ("How many total suppliers are registered?",
         f"SELECT COUNT(*) AS total FROM {V}WP_vProvider;"),
        ("Show supplier IDs and names",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider ORDER BY pvSn;"),
        ("Show suppliers with discount between 10 and 20",
         f"SELECT pvSn, pvName, discount FROM {V}WP_vProvider WHERE discount BETWEEN 10 AND 20;"),
        ("List suppliers with addresses",
         f"SELECT pvSn, pvName, pvAddr FROM {V}WP_vProvider WHERE pvAddr IS NOT NULL AND pvAddr <> '' ORDER BY pvName;"),
        ("Show supplier with the lowest discount",
         f"SELECT TOP 1 pvSn, pvName, discount FROM {V}WP_vProvider WHERE discount > 0 ORDER BY discount ASC;"),
        ("Show all supplier information",
         f"SELECT * FROM {V}WP_vProvider ORDER BY pvSn;"),
        ("Find suppliers containing '有限' in their name",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider WHERE pvName LIKE N'%有限%';"),
        ("Find suppliers containing '農' in their name",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider WHERE pvName LIKE N'%農%';"),
        ("Find suppliers containing '茶' in their name",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider WHERE pvName LIKE N'%茶%';"),
        ("Show suppliers whose names contain '食品'",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider WHERE pvName LIKE N'%食品%';"),
        ("Show distinct supplier name prefixes",
         f"SELECT DISTINCT LEFT(pvName, 2) AS prefix FROM {V}WP_vProvider ORDER BY prefix;"),
    ])
    return s


# ════════════════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("Generating supplement samples...")
    print("=" * 60)

    new_samples = (
        extra_acct_in() +
        extra_acct_out() +
        extra_out_stock() +
        extra_transfer() +
        extra_inventory() +
        extra_product() +
        extra_provider()
    )

    # Count per view
    from collections import Counter
    def view_of(s):
        m = re.search(r'WP_v\w+', s.get("query",""))
        return m.group(0) if m else "?"
    cnt = Counter(view_of(s) for s in new_samples)

    print(f"\nNew supplement samples: {len(new_samples)}")
    for k, v in sorted(cnt.items()):
        print(f"  {k}: {v}")

    # Verify no isDel in no-isDel views
    bad = [s for s in new_samples
           if re.search(r'\bisdel\b|\bdtlisdel\b', s["query"], re.IGNORECASE)
           and any(v in s["query"] for v in ["WP_vInventory","WP_vProduct","WP_vProvider"])]
    if bad:
        print(f"\nWARNING: {len(bad)} bad samples with isDel in no-isDel views!")
        for b in bad:
            print(" ", b["query"][:100])
    else:
        print("\nVerification PASSED: Zero isDel in WP_vInventory/WP_vProduct/WP_vProvider")

    # Load existing data
    data_dir = Path("data/wp_m09")
    train_file = data_dir / "train_claude_en_2000.json"
    val_file   = data_dir / "validation_claude_en.json"

    with open(train_file, "r", encoding="utf-8") as f:
        existing_train = json.load(f)
    with open(val_file, "r", encoding="utf-8") as f:
        existing_val = json.load(f)

    combined_old = existing_train + existing_val
    print(f"\nExisting samples: {len(combined_old)} (train={len(existing_train)}, val={len(existing_val)})")

    # Merge and shuffle
    all_samples = combined_old + new_samples
    random.shuffle(all_samples)
    print(f"Total after merge: {len(all_samples)}")

    # Split 88/12
    n_val   = max(200, int(len(all_samples) * 0.12))
    n_train = len(all_samples) - n_val
    train_data = all_samples[:n_train]
    val_data   = all_samples[n_train:]

    # Write
    with open(train_file, "w", encoding="utf-8") as f:
        json.dump(train_data, f, ensure_ascii=False, indent=2)
    with open(val_file, "w", encoding="utf-8") as f:
        json.dump(val_data, f, ensure_ascii=False, indent=2)

    print(f"\nFinal split: {n_train} train  /  {n_val} validation")
    print(f"Files updated:")
    print(f"  {train_file}  ({n_train} samples)")
    print(f"  {val_file}  ({n_val} samples)")
    print("\nDone!")


if __name__ == "__main__":
    import os
    os.chdir(Path(__file__).parent)
    main()
