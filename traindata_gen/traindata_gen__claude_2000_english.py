#!/usr/bin/env python3
"""
traindata_gen__claude_2000_english.py
Generate 2000+ English Spider-format training/validation samples
for the 7 target WP_M09 views.

HAS isDel + dtlIsDel: WP_vAcctIn, WP_vAcctOut, WP_vOutStock, WP_vTransfer
NO isDel at all:      WP_vInventory, WP_vProduct, WP_vProvider
"""

import json
import re
import random
from pathlib import Path

random.seed(42)
DB  = "WP_M09"
PRE = f"{DB}.dbo."

def toks(q):
    return re.findall(r"[A-Za-z_][A-Za-z0-9_.]*|'[^']*'|[0-9]+|[().,;*<>=!%]+", q)

def entry(question, sql):
    t = toks(sql)
    tnv = ["'value'" if (x.startswith("'") or (x.isdigit() and len(x) > 2)) else x for x in t]
    return {"db_id": DB, "query": sql, "query_toks": t,
            "query_toks_no_value": tnv, "question": question,
            "question_toks": question.split(), "sql": {}}

V = PRE

# ── Constants from real DB data ──
ACCT_IN_IDS  = ["202512050001","202512160001","202511200001","202510150002","202509300001"]
ACCT_OUT_IDS = ["202512050001","202512100002","202511300001","202510050003","202508200001"]
OUT_STK_IDS  = ["202511140030","202511210055","202512020010","202512110013","202510080025",
                "202509150012","202508200005","202507100018"]
TRANSFER_IDS = ["202512010001","202511250002","202510300003","202509200004"]
MEM_IDS      = ["A006","A007","A008","A009","A010","A011","A012"]
MEM_NAMES    = ["麻竹園","旅遊部","食品公司","茶葉行","農產品行","觀光農場","山林農業"]
PV_SNS       = ["1","2","3","5","8","10","15","21","33"]
PV_NAMES     = ["實垣有限公司","永豐農產","台灣好茶","綠野農業","山林食品",
                "天然農場","有機世界","台灣農業","健康食品","品茶苑"]
BARCODES     = ["4712070722015","4710953082676","4710632001318","4719865002441","4715820003021"]
PROD_NAMES   = ["益全香米","竹炭冬筍餅","烏龍茶葉","金萱茶包","台灣高山茶",
                "有機烏龍茶","碧螺春","龍井茶","普洱茶","紅豆湯圓",
                "花蓮米","黑糖茶磚","東方美人茶","阿里山茶","玫瑰花茶"]
WH_NAMES     = ["特產中心","北區倉庫","南區倉庫","中央倉庫","冷藏倉","主倉","東區倉庫"]
EMP_IDS      = ["001","002","003","A01","B02"]
MONTHS_2025  = ["202501","202502","202503","202504","202505","202506",
                "202507","202508","202509","202510","202511","202512"]
MONTHS_2024  = ["202401","202404","202406","202407","202409","202410","202412"]
DATES        = ["20251201","20251205","20251210","20251215","20251220","20251225",
                "20251101","20251110","20251115","20251120","20251125",
                "20251001","20251015","20251020",
                "20250901","20250915"]
AMT_THRESHOLDS = [500,1000,2000,3000,5000,8000,10000,15000,20000]
QTY_THRESHOLDS = [0,5,10,15,20,30,50,100]
PRICE_THRESHOLDS = [30,50,80,100,150,200,300,500]
KEYWORDS     = ["茶","米","水","冬","春","香","竹","有機","花","豆","高山","烏龍","金萱","龍井"]

# ════════════════════════════════════════════════════════════════
#  HELPER: build many question phrasings for the same SQL
# ════════════════════════════════════════════════════════════════
def multi(pairs):
    """Accept list of (question, sql) and return entries."""
    return [entry(q, s) for q, s in pairs]


# ════════════════════════════════════════════════════════════════
#  WP_vAcctIn  (accounts receivable)
# ════════════════════════════════════════════════════════════════
def gen_acct_in():
    s = []
    # ── basic select/count ──
    s += multi([
        ("List all active accounts receivable IDs",
         f"SELECT DISTINCT acctInId FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';"),
        ("Show all active accounts receivable record IDs",
         f"SELECT DISTINCT acctInId FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';"),
        ("Retrieve every active accounts receivable ID",
         f"SELECT DISTINCT acctInId FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';"),
        ("How many active accounts receivable records are there?",
         f"SELECT COUNT(DISTINCT acctInId) AS total FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';"),
        ("Count all active accounts receivable",
         f"SELECT COUNT(DISTINCT acctInId) AS count FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';"),
        ("What is the total number of active accounts receivable?",
         f"SELECT COUNT(DISTINCT acctInId) AS total FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';"),
    ])
    # ── amount aggregates ──
    s += multi([
        ("What is the total amount of all active accounts receivable?",
         f"SELECT SUM(DISTINCT amount) AS total_amount FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';"),
        ("What is the sum of all active accounts receivable amounts?",
         f"SELECT SUM(DISTINCT amount) AS total_amount FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';"),
        ("Calculate the total active accounts receivable value",
         f"SELECT SUM(DISTINCT amount) AS total_amount FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';"),
        ("What is the maximum accounts receivable amount?",
         f"SELECT MAX(DISTINCT amount) AS max_amount FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';"),
        ("What is the minimum accounts receivable amount?",
         f"SELECT MIN(DISTINCT amount) AS min_amount FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';"),
        ("What is the average accounts receivable amount?",
         f"SELECT AVG(DISTINCT amount) AS avg_amount FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';"),
        ("Show the 5 largest accounts receivable amounts",
         f"SELECT TOP 5 DISTINCT acctInId, amount, memName FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' ORDER BY amount DESC;"),
        ("Show the 10 largest accounts receivable records",
         f"SELECT TOP 10 DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' ORDER BY amount DESC;"),
    ])
    # ── per-member (all MEM_IDS and MEM_NAMES) ──
    for mid in MEM_IDS:
        s += multi([
            (f"List accounts receivable for member {mid}",
             f"SELECT DISTINCT acctInId, amount, acctInDate FROM {V}WP_vAcctIn WHERE memId='{mid}' AND isDel='N' AND dtlIsDel='N';"),
            (f"Show all accounts receivable belonging to member {mid}",
             f"SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE memId='{mid}' AND isDel='N' AND dtlIsDel='N';"),
            (f"What is the total accounts receivable for member {mid}?",
             f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vAcctIn WHERE memId='{mid}' AND isDel='N' AND dtlIsDel='N';"),
            (f"How many accounts receivable records does member {mid} have?",
             f"SELECT COUNT(DISTINCT acctInId) AS count FROM {V}WP_vAcctIn WHERE memId='{mid}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    for mn in MEM_NAMES:
        s += multi([
            (f"Show accounts receivable for member {mn}",
             f"SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE memName=N'{mn}' AND isDel='N' AND dtlIsDel='N';"),
            (f"What is the total receivable from member {mn}?",
             f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vAcctIn WHERE memName=N'{mn}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # ── per-month ──
    for m in MONTHS_2025:
        s += multi([
            (f"List accounts receivable for month {m}",
             f"SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE LEFT(acctInId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
            (f"What is the total accounts receivable amount in {m}?",
             f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vAcctIn WHERE LEFT(acctInId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
            (f"How many accounts receivable records were created in {m}?",
             f"SELECT COUNT(DISTINCT acctInId) AS count FROM {V}WP_vAcctIn WHERE LEFT(acctInId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # ── per-date ──
    for d in DATES[:8]:
        s.append(entry(f"List accounts receivable created on {d}",
                       f"SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE LEFT(acctInId,8)='{d}' AND isDel='N' AND dtlIsDel='N';"))
    # ── per-acctInId ──
    for aid in ACCT_IN_IDS:
        s += multi([
            (f"Show details for accounts receivable {aid}",
             f"SELECT * FROM {V}WP_vAcctIn WHERE acctInId='{aid}' AND isDel='N' AND dtlIsDel='N';"),
            (f"What products are in accounts receivable {aid}?",
             f"SELECT pName, oStkDtlQty, oStkDtlAmt FROM {V}WP_vAcctIn WHERE acctInId='{aid}' AND isDel='N' AND dtlIsDel='N';"),
            (f"How many line items does accounts receivable {aid} have?",
             f"SELECT COUNT(*) AS lines FROM {V}WP_vAcctIn WHERE acctInId='{aid}' AND isDel='N' AND dtlIsDel='N';"),
            (f"What is the total amount of accounts receivable {aid}?",
             f"SELECT DISTINCT amount FROM {V}WP_vAcctIn WHERE acctInId='{aid}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # ── per-product ──
    for pn in PROD_NAMES[:8]:
        s += multi([
            (f"Find accounts receivable that include product {pn}",
             f"SELECT DISTINCT acctInId, amount, memName FROM {V}WP_vAcctIn WHERE pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"),
            (f"What is the total quantity of {pn} across all accounts receivable?",
             f"SELECT SUM(oStkDtlQty) AS total_qty FROM {V}WP_vAcctIn WHERE pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"),
            (f"Show all records containing product {pn} in accounts receivable",
             f"SELECT DISTINCT acctInId, oStkDtlQty, oStkDtlAmt FROM {V}WP_vAcctIn WHERE pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # ── per-amount threshold ──
    for amt in AMT_THRESHOLDS:
        s += multi([
            (f"List accounts receivable with amount greater than {amt}",
             f"SELECT DISTINCT acctInId, amount, memName FROM {V}WP_vAcctIn WHERE amount > {amt} AND isDel='N' AND dtlIsDel='N';"),
            (f"Find accounts receivable exceeding {amt}",
             f"SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE amount > {amt} AND isDel='N' AND dtlIsDel='N';"),
        ])
    # ── per-employee ──
    for eid in EMP_IDS:
        s.append(entry(f"Show accounts receivable created by employee {eid}",
                       f"SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE empId='{eid}' AND isDel='N' AND dtlIsDel='N';"))
    # ── per-qty threshold ──
    for q in QTY_THRESHOLDS[1:5]:
        s.append(entry(f"Show accounts receivable with product quantity over {q}",
                       f"SELECT DISTINCT acctInId, pName, oStkDtlQty FROM {V}WP_vAcctIn WHERE oStkDtlQty > {q} AND isDel='N' AND dtlIsDel='N';"))
    # ── keyword search ──
    for kw in KEYWORDS[:8]:
        s.append(entry(f"Find accounts receivable where product name contains {kw}",
                       f"SELECT DISTINCT acctInId, pName FROM {V}WP_vAcctIn WHERE pName LIKE N'%{kw}%' AND isDel='N' AND dtlIsDel='N';"))
    # ── aggregation / grouping ──
    s += multi([
        ("Show total accounts receivable per member",
         f"SELECT memId, memName, SUM(DISTINCT amount) AS total FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY memId, memName ORDER BY total DESC;"),
        ("Summarize accounts receivable by member with total amounts",
         f"SELECT memId, memName, SUM(DISTINCT amount) AS total FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY memId, memName ORDER BY total DESC;"),
        ("Show accounts receivable count and total per month in 2025",
         f"SELECT LEFT(acctInId,6) AS month, COUNT(DISTINCT acctInId) AS count, SUM(DISTINCT amount) AS total FROM {V}WP_vAcctIn WHERE LEFT(acctInId,4)='2025' AND isDel='N' AND dtlIsDel='N' GROUP BY LEFT(acctInId,6) ORDER BY month;"),
        ("Monthly breakdown of accounts receivable in 2025",
         f"SELECT LEFT(acctInId,6) AS month, COUNT(DISTINCT acctInId) AS count, SUM(DISTINCT amount) AS total FROM {V}WP_vAcctIn WHERE LEFT(acctInId,4)='2025' AND isDel='N' AND dtlIsDel='N' GROUP BY LEFT(acctInId,6) ORDER BY month;"),
        ("Show total quantity per product in accounts receivable",
         f"SELECT pName, SUM(oStkDtlQty) AS total_qty FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY total_qty DESC;"),
        ("Which member has the highest total accounts receivable?",
         f"SELECT TOP 1 memId, memName, SUM(DISTINCT amount) AS total FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY memId, memName ORDER BY total DESC;"),
        ("Who has the largest accounts receivable balance?",
         f"SELECT TOP 1 memId, memName, SUM(DISTINCT amount) AS total FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY memId, memName ORDER BY total DESC;"),
        ("Find accounts receivable with more than 3 line items",
         f"SELECT acctInId, COUNT(*) AS lines FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY acctInId HAVING COUNT(*) > 3;"),
        ("Find accounts receivable with more than 5 line items",
         f"SELECT acctInId, COUNT(*) AS lines FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY acctInId HAVING COUNT(*) > 5;"),
        ("Which product appears in the most accounts receivable?",
         f"SELECT TOP 1 pName, COUNT(DISTINCT acctInId) AS count FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY count DESC;"),
        ("Show average line item amount per accounts receivable record",
         f"SELECT acctInId, AVG(oStkDtlAmt) AS avg_line FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY acctInId ORDER BY avg_line DESC;"),
        ("Show total outstock amount per accounts receivable",
         f"SELECT acctInId, SUM(outStkAmtTotal) AS total_out FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY acctInId ORDER BY total_out DESC;"),
        ("List top 3 members by number of accounts receivable records",
         f"SELECT TOP 3 memId, memName, COUNT(DISTINCT acctInId) AS count FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY memId, memName ORDER BY count DESC;"),
        ("Find accounts receivable with discount applied to any line",
         f"SELECT DISTINCT acctInId, pName, dtlDiscnt FROM {V}WP_vAcctIn WHERE dtlDiscnt > 0 AND isDel='N' AND dtlIsDel='N';"),
        ("Show accounts receivable between 5000 and 15000",
         f"SELECT DISTINCT acctInId, amount, memName FROM {V}WP_vAcctIn WHERE amount BETWEEN 5000 AND 15000 AND isDel='N' AND dtlIsDel='N';"),
        ("List distinct product names in active accounts receivable",
         f"SELECT DISTINCT pName FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';"),
        ("Show all outstock IDs linked to accounts receivable",
         f"SELECT DISTINCT OutStkId FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';"),
        ("Show accounts receivable where outstock total exceeds 5000",
         f"SELECT DISTINCT acctInId, outStkAmtTotal FROM {V}WP_vAcctIn WHERE outStkAmtTotal > 5000 AND isDel='N' AND dtlIsDel='N';"),
        ("Find accounts receivable linked to November 2025 outstocks",
         f"SELECT DISTINCT acctInId, OutStkId FROM {V}WP_vAcctIn WHERE LEFT(OutStkId,6)='202511' AND isDel='N' AND dtlIsDel='N';"),
        ("Show all accounts receivable sorted by date descending",
         f"SELECT DISTINCT acctInId, amount, acctInDate FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' ORDER BY acctInDate DESC;"),
        ("How many distinct members are in accounts receivable?",
         f"SELECT COUNT(DISTINCT memId) AS count FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';"),
        ("Show accounts receivable with no memo",
         f"SELECT DISTINCT acctInId FROM {V}WP_vAcctIn WHERE (memo IS NULL OR memo='') AND isDel='N' AND dtlIsDel='N';"),
        ("List all accounts receivable from 2025",
         f"SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE LEFT(acctInId,4)='2025' AND isDel='N' AND dtlIsDel='N' ORDER BY acctInId;"),
        ("Show the most recent accounts receivable records",
         f"SELECT TOP 5 DISTINCT acctInId, amount, acctInDate FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' ORDER BY acctInDate DESC;"),
    ])
    return s


# ════════════════════════════════════════════════════════════════
#  WP_vAcctOut  (accounts payable)
# ════════════════════════════════════════════════════════════════
def gen_acct_out():
    s = []
    # ── basic ──
    s += multi([
        ("List all active accounts payable IDs",
         f"SELECT DISTINCT acctOutId FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N';"),
        ("Show all active accounts payable",
         f"SELECT DISTINCT acctOutId FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N';"),
        ("How many active accounts payable records are there?",
         f"SELECT COUNT(DISTINCT acctOutId) AS total FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N';"),
        ("Count all active accounts payable",
         f"SELECT COUNT(DISTINCT acctOutId) AS count FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N';"),
        ("What is the total amount of all accounts payable?",
         f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N';"),
        ("Calculate the total accounts payable",
         f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N';"),
        ("What is the maximum accounts payable amount?",
         f"SELECT MAX(DISTINCT amount) AS max_amount FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N';"),
        ("What is the minimum accounts payable amount?",
         f"SELECT MIN(DISTINCT amount) AS min_amount FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N';"),
        ("What is the average accounts payable amount?",
         f"SELECT AVG(DISTINCT amount) AS avg_amount FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N';"),
        ("Show top 5 accounts payable by amount",
         f"SELECT TOP 5 DISTINCT acctOutId, amount, pvName FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' ORDER BY amount DESC;"),
        ("List distinct supplier names in accounts payable",
         f"SELECT DISTINCT pvName FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N';"),
        ("How many distinct suppliers appear in accounts payable?",
         f"SELECT COUNT(DISTINCT pvSn) AS count FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N';"),
        ("Show accounts payable for taxable purchases",
         f"SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE isTax='Y' AND isDel='N' AND dtlIsDel='N';"),
        ("Show accounts payable for non-taxable purchases",
         f"SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE isTax='N' AND isDel='N' AND dtlIsDel='N';"),
        ("List accounts payable with receipt invoices",
         f"SELECT DISTINCT acctOutId, recptId FROM {V}WP_vAcctOut WHERE recptId IS NOT NULL AND recptId<>'' AND isDel='N' AND dtlIsDel='N';"),
        ("Show accounts payable with transfer amounts",
         f"SELECT DISTINCT acctOutId, transAmt FROM {V}WP_vAcctOut WHERE transAmt > 0 AND isDel='N' AND dtlIsDel='N';"),
    ])
    # ── per-supplier ──
    for pvn in PV_NAMES:
        s += multi([
            (f"Show accounts payable for supplier {pvn}",
             f"SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE pvName=N'{pvn}' AND isDel='N' AND dtlIsDel='N';"),
            (f"What is the total accounts payable for supplier {pvn}?",
             f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vAcctOut WHERE pvName=N'{pvn}' AND isDel='N' AND dtlIsDel='N';"),
            (f"How many accounts payable records exist for supplier {pvn}?",
             f"SELECT COUNT(DISTINCT acctOutId) AS count FROM {V}WP_vAcctOut WHERE pvName=N'{pvn}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    for pvs in PV_SNS[:5]:
        s.append(entry(f"List accounts payable for supplier pvSn {pvs}",
                       f"SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE pvSn='{pvs}' AND isDel='N' AND dtlIsDel='N';"))
    # ── per-month ──
    for m in MONTHS_2025:
        s += multi([
            (f"What is the total accounts payable for {m}?",
             f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vAcctOut WHERE LEFT(acctOutId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
            (f"How many accounts payable records were created in {m}?",
             f"SELECT COUNT(DISTINCT acctOutId) AS count FROM {V}WP_vAcctOut WHERE LEFT(acctOutId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
            (f"List accounts payable created in {m}",
             f"SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE LEFT(acctOutId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # ── per-date ──
    for d in DATES[:6]:
        s.append(entry(f"List accounts payable created on {d}",
                       f"SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE LEFT(acctOutId,8)='{d}' AND isDel='N' AND dtlIsDel='N';"))
    # ── per-acctOutId ──
    for aid in ACCT_OUT_IDS:
        s += multi([
            (f"Show details for accounts payable {aid}",
             f"SELECT * FROM {V}WP_vAcctOut WHERE acctOutId='{aid}' AND isDel='N' AND dtlIsDel='N';"),
            (f"What products are in accounts payable {aid}?",
             f"SELECT pName, qty, amtTotal FROM {V}WP_vAcctOut WHERE acctOutId='{aid}' AND isDel='N' AND dtlIsDel='N';"),
            (f"How many line items does accounts payable {aid} contain?",
             f"SELECT COUNT(*) AS lines FROM {V}WP_vAcctOut WHERE acctOutId='{aid}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # ── per-product ──
    for pn in PROD_NAMES[:8]:
        s += multi([
            (f"Find accounts payable that include product {pn}",
             f"SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"),
            (f"What is the total quantity of {pn} in accounts payable?",
             f"SELECT SUM(qty) AS total_qty FROM {V}WP_vAcctOut WHERE pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # ── amount thresholds ──
    for amt in AMT_THRESHOLDS:
        s.append(entry(f"List accounts payable with amount over {amt}",
                       f"SELECT DISTINCT acctOutId, amount, pvName FROM {V}WP_vAcctOut WHERE amount > {amt} AND isDel='N' AND dtlIsDel='N';"))
    # ── qty thresholds ──
    for q in QTY_THRESHOLDS[1:5]:
        s.append(entry(f"Show accounts payable where product quantity exceeds {q}",
                       f"SELECT DISTINCT acctOutId, pName, qty FROM {V}WP_vAcctOut WHERE qty > {q} AND isDel='N' AND dtlIsDel='N';"))
    # ── per-employee ──
    for eid in EMP_IDS:
        s.append(entry(f"Show accounts payable created by employee {eid}",
                       f"SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE empId='{eid}' AND isDel='N' AND dtlIsDel='N';"))
    # ── keyword ──
    for kw in KEYWORDS[:6]:
        s.append(entry(f"Find accounts payable with product name containing {kw}",
                       f"SELECT DISTINCT acctOutId, pName FROM {V}WP_vAcctOut WHERE pName LIKE N'%{kw}%' AND isDel='N' AND dtlIsDel='N';"))
    # ── aggregation ──
    s += multi([
        ("Show total accounts payable per supplier",
         f"SELECT pvSn, pvName, SUM(DISTINCT amount) AS total FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' GROUP BY pvSn, pvName ORDER BY total DESC;"),
        ("Summarize accounts payable by supplier",
         f"SELECT pvSn, pvName, SUM(DISTINCT amount) AS total FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' GROUP BY pvSn, pvName ORDER BY total DESC;"),
        ("Show monthly accounts payable summary in 2025",
         f"SELECT LEFT(acctOutId,6) AS month, COUNT(DISTINCT acctOutId) AS count, SUM(DISTINCT amount) AS total FROM {V}WP_vAcctOut WHERE LEFT(acctOutId,4)='2025' AND isDel='N' AND dtlIsDel='N' GROUP BY LEFT(acctOutId,6) ORDER BY month;"),
        ("Which supplier has the highest total accounts payable?",
         f"SELECT TOP 1 pvSn, pvName, SUM(DISTINCT amount) AS total FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' GROUP BY pvSn, pvName ORDER BY total DESC;"),
        ("Show taxable vs non-taxable accounts payable totals",
         f"SELECT isTax, SUM(DISTINCT amount) AS total FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' GROUP BY isTax;"),
        ("Find products in more than 2 accounts payable records",
         f"SELECT pName, COUNT(DISTINCT acctOutId) AS count FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName HAVING COUNT(DISTINCT acctOutId) > 2;"),
        ("Show total quantity per product in accounts payable",
         f"SELECT pName, SUM(qty) AS total_qty FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY total_qty DESC;"),
        ("Show top 5 products by total amount in accounts payable",
         f"SELECT TOP 5 pName, SUM(amtTotal) AS total_amt FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY total_amt DESC;"),
        ("Show accounts payable per employee",
         f"SELECT empId, empName, COUNT(DISTINCT acctOutId) AS count FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' GROUP BY empId, empName ORDER BY count DESC;"),
        ("Show average line item amount per supplier in accounts payable",
         f"SELECT pvName, AVG(amtTotal) AS avg_amt FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' GROUP BY pvName ORDER BY avg_amt DESC;"),
        ("Show total non-taxable amounts per supplier",
         f"SELECT pvName, SUM(amtNoneTax) AS total_non_tax FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' GROUP BY pvName ORDER BY total_non_tax DESC;"),
        ("List accounts payable between 2000 and 10000",
         f"SELECT DISTINCT acctOutId, amount, pvName FROM {V}WP_vAcctOut WHERE amount BETWEEN 2000 AND 10000 AND isDel='N' AND dtlIsDel='N';"),
        ("Show payment type breakdown for accounts payable",
         f"SELECT payType, COUNT(DISTINCT acctOutId) AS count FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' GROUP BY payType;"),
        ("Show distinct products in accounts payable",
         f"SELECT DISTINCT pName FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N';"),
    ])
    return s


# ════════════════════════════════════════════════════════════════
#  WP_vOutStock  (sales orders)
# ════════════════════════════════════════════════════════════════
def gen_out_stock():
    s = []
    # ── basic ──
    s += multi([
        ("List all active sales order IDs",
         f"SELECT DISTINCT OutStkId FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N';"),
        ("Show all active sales orders",
         f"SELECT DISTINCT OutStkId FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N';"),
        ("How many active sales orders are there?",
         f"SELECT COUNT(DISTINCT OutStkId) AS total FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N';"),
        ("Count all active sales orders",
         f"SELECT COUNT(DISTINCT OutStkId) AS count FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N';"),
        ("What is the total revenue from all active sales?",
         f"SELECT SUM(DISTINCT amount) AS total_revenue FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N';"),
        ("Calculate total sales amount",
         f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N';"),
        ("What is the maximum sales order amount?",
         f"SELECT MAX(DISTINCT amount) AS max FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N';"),
        ("What is the minimum sales order amount?",
         f"SELECT MIN(DISTINCT amount) AS min FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N';"),
        ("What is the average sales order amount?",
         f"SELECT AVG(DISTINCT amount) AS avg FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N';"),
        ("Show 10 most recent sales orders",
         f"SELECT TOP 10 DISTINCT OutStkId, amount, OutStkDate FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' ORDER BY OutStkDate DESC;"),
        ("List distinct products sold in active orders",
         f"SELECT DISTINCT pName FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N';"),
        ("List distinct member names in sales orders",
         f"SELECT DISTINCT memName FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N';"),
        ("Count distinct members with active sales orders",
         f"SELECT COUNT(DISTINCT memId) AS count FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N';"),
        ("Show fully settled sales orders",
         f"SELECT DISTINCT OutStkId, amount FROM {V}WP_vOutStock WHERE outType=2 AND isDel='N' AND dtlIsDel='N';"),
        ("Show unsettled sales orders",
         f"SELECT DISTINCT OutStkId, amount, outLeft FROM {V}WP_vOutStock WHERE outType=0 AND isDel='N' AND dtlIsDel='N';"),
        ("List partially settled sales orders",
         f"SELECT DISTINCT OutStkId, amount, outLeft FROM {V}WP_vOutStock WHERE outType=1 AND isDel='N' AND dtlIsDel='N';"),
        ("Show sales orders with receipt invoices",
         f"SELECT DISTINCT OutStkId, reciptNo FROM {V}WP_vOutStock WHERE reciptNo IS NOT NULL AND reciptNo<>'' AND isDel='N' AND dtlIsDel='N';"),
    ])
    # ── per-member ──
    for mid in MEM_IDS:
        s += multi([
            (f"Show sales orders for member {mid}",
             f"SELECT DISTINCT OutStkId, amount, OutStkDate FROM {V}WP_vOutStock WHERE memId='{mid}' AND isDel='N' AND dtlIsDel='N';"),
            (f"What is the total sales amount for member {mid}?",
             f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vOutStock WHERE memId='{mid}' AND isDel='N' AND dtlIsDel='N';"),
            (f"How many sales orders does member {mid} have?",
             f"SELECT COUNT(DISTINCT OutStkId) AS count FROM {V}WP_vOutStock WHERE memId='{mid}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    for mn in MEM_NAMES:
        s += multi([
            (f"List sales orders for {mn}",
             f"SELECT DISTINCT OutStkId, amount FROM {V}WP_vOutStock WHERE memName=N'{mn}' AND isDel='N' AND dtlIsDel='N';"),
            (f"What is the total spent by {mn}?",
             f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vOutStock WHERE memName=N'{mn}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # ── per-month ──
    for m in MONTHS_2025:
        s += multi([
            (f"What is the total sales for {m}?",
             f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vOutStock WHERE LEFT(OutStkId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
            (f"How many sales orders were created in {m}?",
             f"SELECT COUNT(DISTINCT OutStkId) AS count FROM {V}WP_vOutStock WHERE LEFT(OutStkId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
            (f"List sales orders from {m}",
             f"SELECT DISTINCT OutStkId, amount FROM {V}WP_vOutStock WHERE LEFT(OutStkId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # ── per-date ──
    for d in DATES:
        s.append(entry(f"List sales orders created on {d}",
                       f"SELECT DISTINCT OutStkId, amount FROM {V}WP_vOutStock WHERE LEFT(OutStkId,8)='{d}' AND isDel='N' AND dtlIsDel='N';"))
    # ── per-OutStkId ──
    for oid in OUT_STK_IDS:
        s += multi([
            (f"Show all products in sales order {oid}",
             f"SELECT pName, qty, amtTotal FROM {V}WP_vOutStock WHERE OutStkId='{oid}' AND isDel='N' AND dtlIsDel='N';"),
            (f"What is the total amount of sales order {oid}?",
             f"SELECT DISTINCT amount FROM {V}WP_vOutStock WHERE OutStkId='{oid}' AND isDel='N' AND dtlIsDel='N';"),
            (f"How many product lines are in sales order {oid}?",
             f"SELECT COUNT(*) AS lines FROM {V}WP_vOutStock WHERE OutStkId='{oid}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # ── per-product ──
    for pn in PROD_NAMES:
        s += multi([
            (f"Show sales orders for product {pn}",
             f"SELECT DISTINCT OutStkId, qty, amtTotal FROM {V}WP_vOutStock WHERE pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"),
            (f"What is the total quantity sold for {pn}?",
             f"SELECT SUM(qty) AS total_qty FROM {V}WP_vOutStock WHERE pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"),
            (f"What is the total revenue from selling {pn}?",
             f"SELECT SUM(amtTotal) AS total_revenue FROM {V}WP_vOutStock WHERE pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # ── per-employee ──
    for eid in EMP_IDS:
        s.append(entry(f"Show sales orders created by employee {eid}",
                       f"SELECT DISTINCT OutStkId, amount FROM {V}WP_vOutStock WHERE empId='{eid}' AND isDel='N' AND dtlIsDel='N';"))
    # ── amount thresholds ──
    for amt in AMT_THRESHOLDS:
        s += multi([
            (f"List sales orders with amount over {amt}",
             f"SELECT DISTINCT OutStkId, amount, memName FROM {V}WP_vOutStock WHERE amount > {amt} AND isDel='N' AND dtlIsDel='N';"),
            (f"Find sales orders exceeding {amt} in amount",
             f"SELECT DISTINCT OutStkId, amount FROM {V}WP_vOutStock WHERE amount > {amt} AND isDel='N' AND dtlIsDel='N';"),
        ])
    # ── qty thresholds ──
    for q in QTY_THRESHOLDS[1:6]:
        s.append(entry(f"Show sales orders where product quantity exceeds {q}",
                       f"SELECT DISTINCT OutStkId, pName, qty FROM {V}WP_vOutStock WHERE qty > {q} AND isDel='N' AND dtlIsDel='N';"))
    # ── keyword ──
    for kw in KEYWORDS[:8]:
        s.append(entry(f"Find sales orders with product name containing {kw}",
                       f"SELECT DISTINCT OutStkId, pName, qty FROM {V}WP_vOutStock WHERE pName LIKE N'%{kw}%' AND isDel='N' AND dtlIsDel='N';"))
    # ── aggregation ──
    s += multi([
        ("Show total sales per member",
         f"SELECT memId, memName, SUM(DISTINCT amount) AS total FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' GROUP BY memId, memName ORDER BY total DESC;"),
        ("Monthly sales summary for 2025",
         f"SELECT LEFT(OutStkId,6) AS month, COUNT(DISTINCT OutStkId) AS orders, SUM(DISTINCT amount) AS total FROM {V}WP_vOutStock WHERE LEFT(OutStkId,4)='2025' AND isDel='N' AND dtlIsDel='N' GROUP BY LEFT(OutStkId,6) ORDER BY month;"),
        ("Show top 5 best-selling products by total quantity",
         f"SELECT TOP 5 pName, SUM(qty) AS total_qty FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY total_qty DESC;"),
        ("Show top 5 best-selling products by total revenue",
         f"SELECT TOP 5 pName, SUM(amtTotal) AS total_revenue FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY total_revenue DESC;"),
        ("Which member had the most sales orders?",
         f"SELECT TOP 1 memId, memName, COUNT(DISTINCT OutStkId) AS count FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' GROUP BY memId, memName ORDER BY count DESC;"),
        ("Show total discount per product in active sales",
         f"SELECT pName, SUM(dtlDiscnt) AS total_discount FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY total_discount DESC;"),
        ("Show total tax per month in 2025",
         f"SELECT LEFT(OutStkId,6) AS month, SUM(DISTINCT tax) AS total_tax FROM {V}WP_vOutStock WHERE LEFT(OutStkId,4)='2025' AND isDel='N' AND dtlIsDel='N' GROUP BY LEFT(OutStkId,6) ORDER BY month;"),
        ("Find products sold in more than 3 sales orders",
         f"SELECT pName, COUNT(DISTINCT OutStkId) AS count FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName HAVING COUNT(DISTINCT OutStkId) > 3 ORDER BY count DESC;"),
        ("Show sales per member for December 2025",
         f"SELECT memId, memName, SUM(DISTINCT amount) AS total FROM {V}WP_vOutStock WHERE LEFT(OutStkId,6)='202512' AND isDel='N' AND dtlIsDel='N' GROUP BY memId, memName ORDER BY total DESC;"),
        ("Find the order with the most product lines",
         f"SELECT TOP 1 OutStkId, COUNT(*) AS lines FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' GROUP BY OutStkId ORDER BY lines DESC;"),
        ("Show top 3 members by total spending in 2025",
         f"SELECT TOP 3 memId, memName, SUM(DISTINCT amount) AS total FROM {V}WP_vOutStock WHERE LEFT(OutStkId,4)='2025' AND isDel='N' AND dtlIsDel='N' GROUP BY memId, memName ORDER BY total DESC;"),
        ("Show settlement status breakdown of sales orders",
         f"SELECT outType, COUNT(DISTINCT OutStkId) AS count FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' GROUP BY outType;"),
        ("Show daily sales totals for December 2025",
         f"SELECT LEFT(OutStkId,8) AS date, SUM(DISTINCT amount) AS total FROM {V}WP_vOutStock WHERE LEFT(OutStkId,6)='202512' AND isDel='N' AND dtlIsDel='N' GROUP BY LEFT(OutStkId,8) ORDER BY date;"),
    ])
    return s


# ════════════════════════════════════════════════════════════════
#  WP_vTransfer  (warehouse transfers)
# ════════════════════════════════════════════════════════════════
def gen_transfer():
    s = []
    # ── basic ──
    s += multi([
        ("List all active transfer record IDs",
         f"SELECT DISTINCT TransferId FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N';"),
        ("How many active transfers are there?",
         f"SELECT COUNT(*) AS total FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N';"),
        ("What products have been transferred?",
         f"SELECT DISTINCT pName FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N';"),
        ("List distinct source warehouse names",
         f"SELECT DISTINCT fWhName FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N';"),
        ("List distinct destination warehouse names",
         f"SELECT DISTINCT tfWhName FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N';"),
        ("What is the total quantity transferred?",
         f"SELECT SUM(qty) AS total_qty FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N';"),
        ("Show the transfer with the highest quantity",
         f"SELECT TOP 1 TransferId, pName, qty FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' ORDER BY qty DESC;"),
        ("Show the 5 most recent transfers",
         f"SELECT TOP 5 TransferId, pName, qty, TransferDate FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' ORDER BY TransferDate DESC;"),
        ("Show transfers with quantity over 10",
         f"SELECT TransferId, pName, qty FROM {V}WP_vTransfer WHERE qty > 10 AND isDel='N' AND dtlIsDel='N';"),
        ("Show transfers with quantity over 20",
         f"SELECT TransferId, pName, qty FROM {V}WP_vTransfer WHERE qty > 20 AND isDel='N' AND dtlIsDel='N';"),
        ("Show transfers with quantity over 50",
         f"SELECT TransferId, pName, qty FROM {V}WP_vTransfer WHERE qty > 50 AND isDel='N' AND dtlIsDel='N';"),
    ])
    # ── per-product ──
    for pn in PROD_NAMES[:10]:
        s += multi([
            (f"Show transfers for product {pn}",
             f"SELECT TransferId, qty, fWhName, tfWhName FROM {V}WP_vTransfer WHERE pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"),
            (f"What is the total quantity of {pn} transferred?",
             f"SELECT SUM(qty) AS total_qty FROM {V}WP_vTransfer WHERE pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"),
            (f"How many times was {pn} transferred?",
             f"SELECT COUNT(*) AS count FROM {V}WP_vTransfer WHERE pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # ── per-warehouse ──
    for wh in WH_NAMES:
        s += multi([
            (f"Show transfers from warehouse {wh}",
             f"SELECT TransferId, pName, qty FROM {V}WP_vTransfer WHERE fWhName=N'{wh}' AND isDel='N' AND dtlIsDel='N';"),
            (f"Show transfers to warehouse {wh}",
             f"SELECT TransferId, pName, qty FROM {V}WP_vTransfer WHERE tfWhName=N'{wh}' AND isDel='N' AND dtlIsDel='N';"),
            (f"What is the total quantity transferred from {wh}?",
             f"SELECT SUM(qty) AS total_qty FROM {V}WP_vTransfer WHERE fWhName=N'{wh}' AND isDel='N' AND dtlIsDel='N';"),
            (f"What is the total quantity transferred to {wh}?",
             f"SELECT SUM(qty) AS total_qty FROM {V}WP_vTransfer WHERE tfWhName=N'{wh}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # ── per-month ──
    for m in MONTHS_2025:
        s += multi([
            (f"List transfers in {m}",
             f"SELECT TransferId, pName, qty FROM {V}WP_vTransfer WHERE LEFT(TransferId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
            (f"What is the total quantity transferred in {m}?",
             f"SELECT SUM(qty) AS total_qty FROM {V}WP_vTransfer WHERE LEFT(TransferId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
            (f"How many transfers happened in {m}?",
             f"SELECT COUNT(*) AS count FROM {V}WP_vTransfer WHERE LEFT(TransferId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # ── per-date ──
    for d in DATES[:8]:
        s.append(entry(f"Show transfers on date {d}",
                       f"SELECT TransferId, pName, qty FROM {V}WP_vTransfer WHERE LEFT(TransferId,8)='{d}' AND isDel='N' AND dtlIsDel='N';"))
    # ── per-employee ──
    for eid in EMP_IDS:
        s.append(entry(f"Show transfers done by employee {eid}",
                       f"SELECT DISTINCT TransferId, pName, qty FROM {V}WP_vTransfer WHERE empId='{eid}' AND isDel='N' AND dtlIsDel='N';"))
    # ── per-barcode ──
    for bc in BARCODES[:3]:
        s.append(entry(f"Show transfers for product with barcode {bc}",
                       f"SELECT TransferId, pName, qty FROM {V}WP_vTransfer WHERE pBarcode='{bc}' AND isDel='N' AND dtlIsDel='N';"))
    # ── qty range ──
    s += multi([
        ("Show transfers with quantity between 5 and 15",
         f"SELECT TransferId, pName, qty FROM {V}WP_vTransfer WHERE qty BETWEEN 5 AND 15 AND isDel='N' AND dtlIsDel='N';"),
        ("Show transfers with quantity between 10 and 30",
         f"SELECT TransferId, pName, qty FROM {V}WP_vTransfer WHERE qty BETWEEN 10 AND 30 AND isDel='N' AND dtlIsDel='N';"),
    ])
    # ── keyword ──
    for kw in KEYWORDS[:6]:
        s.append(entry(f"Find transfers for products containing {kw}",
                       f"SELECT TransferId, pName, qty FROM {V}WP_vTransfer WHERE pName LIKE N'%{kw}%' AND isDel='N' AND dtlIsDel='N';"))
    # ── aggregation ──
    s += multi([
        ("Show total quantity transferred per product",
         f"SELECT pName, SUM(qty) AS total_qty FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY total_qty DESC;"),
        ("Show transfer count by source warehouse",
         f"SELECT fWhName, COUNT(*) AS count FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY fWhName ORDER BY count DESC;"),
        ("Show transfer count by destination warehouse",
         f"SELECT tfWhName, COUNT(*) AS count FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY tfWhName ORDER BY count DESC;"),
        ("Monthly transfer volume for 2025",
         f"SELECT LEFT(TransferId,6) AS month, COUNT(*) AS count, SUM(qty) AS total_qty FROM {V}WP_vTransfer WHERE LEFT(TransferId,4)='2025' AND isDel='N' AND dtlIsDel='N' GROUP BY LEFT(TransferId,6) ORDER BY month;"),
        ("Which product has the highest total transferred quantity?",
         f"SELECT TOP 1 pName, SUM(qty) AS total_qty FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY total_qty DESC;"),
        ("Show average transferred quantity per product",
         f"SELECT pName, AVG(qty) AS avg_qty FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY avg_qty DESC;"),
        ("Find products transferred more than 3 times",
         f"SELECT pName, COUNT(*) AS count FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName HAVING COUNT(*) > 3;"),
        ("Show the top 5 most active source warehouses",
         f"SELECT TOP 5 fWhName, COUNT(*) AS count FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY fWhName ORDER BY count DESC;"),
        ("Show count of distinct products transferred per month in 2025",
         f"SELECT LEFT(TransferId,6) AS month, COUNT(DISTINCT pNo) AS products FROM {V}WP_vTransfer WHERE LEFT(TransferId,4)='2025' AND isDel='N' AND dtlIsDel='N' GROUP BY LEFT(TransferId,6) ORDER BY month;"),
        ("Show transfer activity statistics for November 2025",
         f"SELECT COUNT(*) AS transfers, SUM(qty) AS total_qty FROM {V}WP_vTransfer WHERE LEFT(TransferId,6)='202511' AND isDel='N' AND dtlIsDel='N';"),
        ("Show transfer activity statistics for December 2025",
         f"SELECT COUNT(*) AS transfers, SUM(qty) AS total_qty FROM {V}WP_vTransfer WHERE LEFT(TransferId,6)='202512' AND isDel='N' AND dtlIsDel='N';"),
    ])
    return s


# ════════════════════════════════════════════════════════════════
#  WP_vInventory  — NO isDel
# ════════════════════════════════════════════════════════════════
def gen_inventory():
    s = []
    # ── basic ──
    s += multi([
        ("What is the total inventory quantity?",
         f"SELECT SUM(qty) AS total_qty FROM {V}WP_vInventory;"),
        ("How much total inventory is there?",
         f"SELECT SUM(qty) AS total_qty FROM {V}WP_vInventory;"),
        ("Calculate the total inventory quantity across all products",
         f"SELECT SUM(qty) AS total_qty FROM {V}WP_vInventory;"),
        ("How many distinct products are in inventory?",
         f"SELECT COUNT(DISTINCT pNo) AS count FROM {V}WP_vInventory;"),
        ("Count distinct products in inventory",
         f"SELECT COUNT(DISTINCT pNo) AS count FROM {V}WP_vInventory;"),
        ("List all distinct warehouse names",
         f"SELECT DISTINCT WarehouseName FROM {V}WP_vInventory;"),
        ("Show all warehouses that have inventory",
         f"SELECT DISTINCT WarehouseName FROM {V}WP_vInventory;"),
        ("What is the total inventory value (quantity × average cost)?",
         f"SELECT SUM(qty * costAvg) AS total_value FROM {V}WP_vInventory;"),
        ("Calculate total inventory value",
         f"SELECT SUM(qty * costAvg) AS total_value FROM {V}WP_vInventory;"),
        ("What products have zero stock?",
         f"SELECT DISTINCT pName FROM {V}WP_vInventory WHERE qty = 0;"),
        ("List products with no inventory",
         f"SELECT DISTINCT pName FROM {V}WP_vInventory WHERE qty = 0;"),
        ("What is the average inventory quantity per product line?",
         f"SELECT AVG(qty) AS avg_qty FROM {V}WP_vInventory;"),
        ("Show maximum inventory quantity for any single record",
         f"SELECT MAX(qty) AS max_qty FROM {V}WP_vInventory;"),
        ("How many total inventory records exist?",
         f"SELECT COUNT(*) AS total FROM {V}WP_vInventory;"),
        ("List all distinct supplier names in inventory",
         f"SELECT DISTINCT pvName FROM {V}WP_vInventory ORDER BY pvName;"),
        ("How many distinct warehouses have inventory?",
         f"SELECT COUNT(DISTINCT WarehouseId) AS count FROM {V}WP_vInventory;"),
    ])
    # ── products below threshold ──
    for q in QTY_THRESHOLDS[:5]:
        s += multi([
            (f"List products with inventory quantity below {q}",
             f"SELECT DISTINCT pName, qty FROM {V}WP_vInventory WHERE qty < {q};"),
            (f"Show products with less than {q} units in stock",
             f"SELECT DISTINCT pName, qty FROM {V}WP_vInventory WHERE qty < {q};"),
        ])
    # ── products above threshold ──
    for q in [10, 20, 50, 100, 200]:
        s.append(entry(f"List products with inventory quantity above {q}",
                       f"SELECT DISTINCT pName, SUM(qty) AS total_qty FROM {V}WP_vInventory WHERE qty > {q} GROUP BY pName;"))
    # ── safe stock ──
    s += multi([
        ("Show products below safe stock level",
         f"SELECT pName, qty, qtySafe FROM {V}WP_vInventory WHERE qty < qtySafe;"),
        ("List products that are below their safety stock threshold",
         f"SELECT pName, qty, qtySafe FROM {V}WP_vInventory WHERE qty < qtySafe;"),
        ("Show products that exceed safe stock by more than 50",
         f"SELECT DISTINCT pName, qty, qtySafe FROM {V}WP_vInventory WHERE qty - qtySafe > 50;"),
    ])
    # ── per-warehouse ──
    for wh in WH_NAMES:
        s += multi([
            (f"Show inventory for warehouse {wh}",
             f"SELECT pName, qty FROM {V}WP_vInventory WHERE WarehouseName=N'{wh}';"),
            (f"What is the total inventory quantity in warehouse {wh}?",
             f"SELECT SUM(qty) AS total_qty FROM {V}WP_vInventory WHERE WarehouseName=N'{wh}';"),
            (f"What is the inventory value in warehouse {wh}?",
             f"SELECT SUM(qty * costAvg) AS total_value FROM {V}WP_vInventory WHERE WarehouseName=N'{wh}';"),
            (f"How many distinct products are in warehouse {wh}?",
             f"SELECT COUNT(DISTINCT pNo) AS count FROM {V}WP_vInventory WHERE WarehouseName=N'{wh}';"),
            (f"List all products in warehouse {wh}",
             f"SELECT DISTINCT pName, qty FROM {V}WP_vInventory WHERE WarehouseName=N'{wh}' ORDER BY qty DESC;"),
        ])
    # ── per-product ──
    for pn in PROD_NAMES:
        s += multi([
            (f"Show inventory for product {pn}",
             f"SELECT WarehouseName, qty FROM {V}WP_vInventory WHERE pName=N'{pn}';"),
            (f"What is the total inventory quantity of {pn}?",
             f"SELECT SUM(qty) AS total_qty FROM {V}WP_vInventory WHERE pName=N'{pn}';"),
            (f"In which warehouses is {pn} stored?",
             f"SELECT WarehouseName, qty FROM {V}WP_vInventory WHERE pName=N'{pn}';"),
        ])
    # ── per-supplier ──
    for pvn in PV_NAMES[:6]:
        s += multi([
            (f"Show all products from supplier {pvn} in inventory",
             f"SELECT DISTINCT pName, SUM(qty) AS total_qty FROM {V}WP_vInventory WHERE pvName=N'{pvn}' GROUP BY pName;"),
            (f"What is the total inventory quantity for supplier {pvn}?",
             f"SELECT SUM(qty) AS total_qty FROM {V}WP_vInventory WHERE pvName=N'{pvn}';"),
        ])
    for pvs in PV_SNS[:5]:
        s.append(entry(f"Show inventory for supplier pvSn {pvs}",
                       f"SELECT DISTINCT pName, qty FROM {V}WP_vInventory WHERE pvSn='{pvs}';"))
    # ── per-barcode ──
    for bc in BARCODES:
        s.append(entry(f"Show inventory for barcode {bc}",
                       f"SELECT WarehouseName, pName, qty FROM {V}WP_vInventory WHERE pBarcode='{bc}';"))
    # ── price thresholds ──
    for p in PRICE_THRESHOLDS:
        s += multi([
            (f"List products with standard price above {p}",
             f"SELECT DISTINCT pName, priceStd FROM {V}WP_vInventory WHERE priceStd > {p} ORDER BY priceStd DESC;"),
            (f"Show inventory for products priced over {p}",
             f"SELECT DISTINCT pName, priceStd, SUM(qty) AS total_qty FROM {V}WP_vInventory WHERE priceStd > {p} GROUP BY pName, priceStd;"),
        ])
    # ── keyword ──
    for kw in KEYWORDS:
        s += multi([
            (f"Find inventory items with product name containing {kw}",
             f"SELECT DISTINCT pName, SUM(qty) AS total_qty FROM {V}WP_vInventory WHERE pName LIKE N'%{kw}%' GROUP BY pName;"),
        ])
    # ── isSale / isTax ──
    s += multi([
        ("Show active products for sale in inventory (isSale=0)",
         f"SELECT DISTINCT pName, qty FROM {V}WP_vInventory WHERE isSale='0';"),
        ("Show non-sale products in inventory",
         f"SELECT DISTINCT pName, isSale FROM {V}WP_vInventory WHERE isSale<>'0';"),
        ("Show taxable products in inventory",
         f"SELECT DISTINCT pName, SUM(qty) AS total_qty FROM {V}WP_vInventory WHERE isTax='Y' GROUP BY pName;"),
    ])
    # ── aggregation ──
    s += multi([
        ("Show total inventory per warehouse",
         f"SELECT WarehouseName, SUM(qty) AS total_qty FROM {V}WP_vInventory GROUP BY WarehouseName ORDER BY total_qty DESC;"),
        ("Show inventory value per warehouse",
         f"SELECT WarehouseName, SUM(qty * costAvg) AS total_value FROM {V}WP_vInventory GROUP BY WarehouseName ORDER BY total_value DESC;"),
        ("Show product count and quantity per supplier",
         f"SELECT pvName, COUNT(DISTINCT pName) AS products, SUM(qty) AS total_qty FROM {V}WP_vInventory GROUP BY pvName ORDER BY total_qty DESC;"),
        ("Which warehouse has the highest total inventory value?",
         f"SELECT TOP 1 WarehouseName, SUM(qty * costAvg) AS value FROM {V}WP_vInventory GROUP BY WarehouseName ORDER BY value DESC;"),
        ("Find products present in more than one warehouse",
         f"SELECT pName, COUNT(DISTINCT WarehouseId) AS wh_count FROM {V}WP_vInventory GROUP BY pName HAVING COUNT(DISTINCT WarehouseId) > 1;"),
        ("Show total inventory shortage per product",
         f"SELECT pName, qtySafe - SUM(qty) AS shortage FROM {V}WP_vInventory WHERE qty < qtySafe GROUP BY pName, qtySafe ORDER BY shortage DESC;"),
        ("Show top 10 products by total inventory",
         f"SELECT TOP 10 pName, SUM(qty) AS total_qty FROM {V}WP_vInventory GROUP BY pName ORDER BY total_qty DESC;"),
        ("Show average standard price per supplier",
         f"SELECT pvName, AVG(priceStd) AS avg_price FROM {V}WP_vInventory GROUP BY pvName ORDER BY avg_price DESC;"),
        ("Find products where member price exceeds standard price",
         f"SELECT DISTINCT pName, priceStd, priceMem FROM {V}WP_vInventory WHERE priceMem > priceStd;"),
        ("Show inventory summary: total products, quantity and value",
         f"SELECT COUNT(DISTINCT pNo) AS products, SUM(qty) AS total_qty, SUM(qty * costAvg) AS total_value FROM {V}WP_vInventory;"),
    ])
    return s


# ════════════════════════════════════════════════════════════════
#  WP_vProduct  — NO isDel
# ════════════════════════════════════════════════════════════════
def gen_product():
    s = []
    # ── basic ──
    s += multi([
        ("List all product names",
         f"SELECT DISTINCT pName FROM {V}WP_vProduct ORDER BY pName;"),
        ("Show all products",
         f"SELECT pNo, pName FROM {V}WP_vProduct ORDER BY pName;"),
        ("How many products are in the catalog?",
         f"SELECT COUNT(*) AS total FROM {V}WP_vProduct;"),
        ("Count total products",
         f"SELECT COUNT(*) AS total FROM {V}WP_vProduct;"),
        ("What is the total current stock quantity across all products?",
         f"SELECT SUM(qtyNow) AS total_qty FROM {V}WP_vProduct;"),
        ("Calculate total stock quantity",
         f"SELECT SUM(qtyNow) AS total_qty FROM {V}WP_vProduct;"),
        ("What is the average standard price?",
         f"SELECT AVG(priceStd) AS avg_price FROM {V}WP_vProduct;"),
        ("What is the highest standard price?",
         f"SELECT MAX(priceStd) AS max_price FROM {V}WP_vProduct;"),
        ("What is the lowest standard price?",
         f"SELECT MIN(priceStd) AS min_price FROM {V}WP_vProduct;"),
        ("Show all products with their standard prices",
         f"SELECT pNo, pName, priceStd FROM {V}WP_vProduct ORDER BY priceStd DESC;"),
        ("Show products with no current stock",
         f"SELECT pNo, pName FROM {V}WP_vProduct WHERE qtyNow = 0;"),
        ("Find products with zero inventory",
         f"SELECT pNo, pName FROM {V}WP_vProduct WHERE qtyNow = 0;"),
        ("Show products below safe stock level",
         f"SELECT pNo, pName, qtyNow, qtySafe FROM {V}WP_vProduct WHERE qtyNow < qtySafe;"),
        ("List products that track inventory",
         f"SELECT pNo, pName FROM {V}WP_vProduct WHERE isUpdStock='Y';"),
        ("List taxable products",
         f"SELECT pNo, pName FROM {V}WP_vProduct WHERE isTax='Y';"),
        ("List non-taxable products",
         f"SELECT pNo, pName FROM {V}WP_vProduct WHERE isTax='N';"),
        ("Show active products for sale",
         f"SELECT pNo, pName FROM {V}WP_vProduct WHERE isSale='0';"),
        ("Show products that cannot be sold",
         f"SELECT pNo, pName, isSale FROM {V}WP_vProduct WHERE isSale IN ('2','3');"),
        ("List all distinct supplier names",
         f"SELECT DISTINCT pvName FROM {V}WP_vProduct ORDER BY pvName;"),
        ("How many distinct suppliers are there?",
         f"SELECT COUNT(DISTINCT pvSn) AS count FROM {V}WP_vProduct;"),
        ("Show products with supplier discount",
         f"SELECT pNo, pName, pvDiscount FROM {V}WP_vProduct WHERE isPvDiscount='Y' ORDER BY pvDiscount DESC;"),
        ("What is the total inventory value of all products?",
         f"SELECT SUM(qtyNow * costAvg) AS total_value FROM {V}WP_vProduct;"),
    ])
    # ── per-supplier ──
    for pvn in PV_NAMES:
        s += multi([
            (f"List all products supplied by {pvn}",
             f"SELECT pNo, pName FROM {V}WP_vProduct WHERE pvName=N'{pvn}';"),
            (f"How many products does supplier {pvn} supply?",
             f"SELECT COUNT(*) AS count FROM {V}WP_vProduct WHERE pvName=N'{pvn}';"),
            (f"What is the total inventory value for supplier {pvn}?",
             f"SELECT SUM(qtyNow * costAvg) AS total_value FROM {V}WP_vProduct WHERE pvName=N'{pvn}';"),
            (f"What is the average standard price for supplier {pvn} products?",
             f"SELECT AVG(priceStd) AS avg_price FROM {V}WP_vProduct WHERE pvName=N'{pvn}';"),
        ])
    for pvs in PV_SNS[:5]:
        s += multi([
            (f"List products from supplier pvSn {pvs}",
             f"SELECT pNo, pName, qtyNow FROM {V}WP_vProduct WHERE pvSn='{pvs}';"),
        ])
    # ── per-barcode ──
    for bc in BARCODES:
        s += multi([
            (f"Show product details for barcode {bc}",
             f"SELECT pNo, pName, priceStd, costAvg, qtyNow FROM {V}WP_vProduct WHERE pBarcode='{bc}';"),
        ])
    # ── per-product name ──
    for pn in PROD_NAMES:
        s += multi([
            (f"Show details for product {pn}",
             f"SELECT pNo, pName, priceStd, costAvg, qtyNow FROM {V}WP_vProduct WHERE pName=N'{pn}';"),
            (f"What is the current stock for {pn}?",
             f"SELECT pNo, pName, qtyNow FROM {V}WP_vProduct WHERE pName=N'{pn}';"),
            (f"What is the standard price for {pn}?",
             f"SELECT pNo, pName, priceStd FROM {V}WP_vProduct WHERE pName=N'{pn}';"),
        ])
    # ── price thresholds ──
    for p in PRICE_THRESHOLDS:
        s += multi([
            (f"List products with standard price above {p}",
             f"SELECT pNo, pName, priceStd FROM {V}WP_vProduct WHERE priceStd > {p} ORDER BY priceStd DESC;"),
            (f"Show products priced below {p}",
             f"SELECT pNo, pName, priceStd FROM {V}WP_vProduct WHERE priceStd < {p} ORDER BY priceStd;"),
        ])
    # ── month additions ──
    for m in MONTHS_2024 + MONTHS_2025[:4]:
        s.append(entry(f"List products added in {m}",
                       f"SELECT pNo, pName FROM {V}WP_vProduct WHERE pNo LIKE '{m}%';"))
    s += multi([
        ("Show products added in 2025",
         f"SELECT pNo, pName FROM {V}WP_vProduct WHERE pNo LIKE '2025%';"),
        ("Show products added in 2024",
         f"SELECT pNo, pName FROM {V}WP_vProduct WHERE pNo LIKE '2024%';"),
    ])
    # ── keyword ──
    for kw in KEYWORDS:
        s += multi([
            (f"Find products with name containing {kw}",
             f"SELECT pNo, pName FROM {V}WP_vProduct WHERE pName LIKE N'%{kw}%';"),
        ])
    # ── qty thresholds ──
    for q in QTY_THRESHOLDS[:5]:
        s += multi([
            (f"List products with current stock below {q}",
             f"SELECT pNo, pName, qtyNow FROM {V}WP_vProduct WHERE qtyNow < {q};"),
            (f"Show products with more than {q} units in stock",
             f"SELECT pNo, pName, qtyNow FROM {V}WP_vProduct WHERE qtyNow > {q} ORDER BY qtyNow DESC;"),
        ])
    # ── aggregation ──
    s += multi([
        ("Show product count per supplier",
         f"SELECT pvName, COUNT(*) AS count FROM {V}WP_vProduct GROUP BY pvName ORDER BY count DESC;"),
        ("Show top 5 most expensive products",
         f"SELECT TOP 5 pNo, pName, priceStd FROM {V}WP_vProduct ORDER BY priceStd DESC;"),
        ("Show top 10 products by current stock",
         f"SELECT TOP 10 pNo, pName, qtyNow FROM {V}WP_vProduct ORDER BY qtyNow DESC;"),
        ("Show average prices by supplier",
         f"SELECT pvName, AVG(priceStd) AS avg_std FROM {V}WP_vProduct GROUP BY pvName ORDER BY avg_std DESC;"),
        ("Which supplier's products have the highest total inventory value?",
         f"SELECT TOP 1 pvName, SUM(qtyNow * costAvg) AS total_value FROM {V}WP_vProduct GROUP BY pvName ORDER BY total_value DESC;"),
        ("Show profit margin per product",
         f"SELECT pNo, pName, priceStd - costAvg AS margin FROM {V}WP_vProduct ORDER BY margin DESC;"),
        ("Show products where average cost exceeds 80% of standard price",
         f"SELECT pNo, pName, costAvg, priceStd FROM {V}WP_vProduct WHERE costAvg > priceStd * 0.8 ORDER BY pName;"),
        ("Show products with total value over 5000",
         f"SELECT pNo, pName, qtyNow * costAvg AS value FROM {V}WP_vProduct WHERE qtyNow * costAvg > 5000 ORDER BY value DESC;"),
        ("Show products where member price is less than standard price",
         f"SELECT pNo, pName, priceStd, priceMem FROM {V}WP_vProduct WHERE priceMem < priceStd;"),
        ("Count products by sale status",
         f"SELECT isSale, COUNT(*) AS count FROM {V}WP_vProduct GROUP BY isSale;"),
        ("Count products by tax status",
         f"SELECT isTax, COUNT(*) AS count FROM {V}WP_vProduct GROUP BY isTax;"),
        ("Show top 5 products with highest supplier discount",
         f"SELECT TOP 5 pNo, pName, pvDiscount FROM {V}WP_vProduct WHERE pvDiscount > 0 ORDER BY pvDiscount DESC;"),
        ("Show products where initial stock is more than current stock",
         f"SELECT pNo, pName, qtyInitial, qtyNow FROM {V}WP_vProduct WHERE qtyInitial > qtyNow;"),
        ("Find products with safe stock set but current stock is zero",
         f"SELECT pNo, pName, qtyNow, qtySafe FROM {V}WP_vProduct WHERE qtyNow = 0 AND qtySafe > 0;"),
        ("Show products with batch price defined",
         f"SELECT pNo, pName, priceBat FROM {V}WP_vProduct WHERE priceBat > 0 ORDER BY priceBat DESC;"),
    ])
    return s


# ════════════════════════════════════════════════════════════════
#  WP_vProvider  — NO isDel
# ════════════════════════════════════════════════════════════════
def gen_provider():
    s = []
    # ── basic ──
    s += multi([
        ("List all supplier names",
         f"SELECT DISTINCT pvName FROM {V}WP_vProvider ORDER BY pvName;"),
        ("Show all suppliers",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider ORDER BY pvName;"),
        ("How many suppliers are there?",
         f"SELECT COUNT(*) AS total FROM {V}WP_vProvider;"),
        ("Count all suppliers",
         f"SELECT COUNT(*) AS total FROM {V}WP_vProvider;"),
        ("List all active suppliers",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider WHERE isStop='N' ORDER BY pvName;"),
        ("Show only active suppliers",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider WHERE isStop='N' ORDER BY pvName;"),
        ("List all inactive suppliers",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider WHERE isStop='Y';"),
        ("Show stopped suppliers",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider WHERE isStop='Y';"),
        ("How many active suppliers are there?",
         f"SELECT COUNT(*) AS count FROM {V}WP_vProvider WHERE isStop='N';"),
        ("How many inactive suppliers are there?",
         f"SELECT COUNT(*) AS count FROM {V}WP_vProvider WHERE isStop='Y';"),
        ("Show suppliers with email addresses",
         f"SELECT pvSn, pvName, email FROM {V}WP_vProvider WHERE email IS NOT NULL AND email<>'';"),
        ("List suppliers that have email contact",
         f"SELECT pvSn, pvName, email FROM {V}WP_vProvider WHERE email IS NOT NULL AND email<>'';"),
        ("Show suppliers with phone numbers",
         f"SELECT pvSn, pvName, pvTel FROM {V}WP_vProvider WHERE pvTel IS NOT NULL AND pvTel<>'';"),
        ("Show suppliers with bank account information",
         f"SELECT pvSn, pvName, bankName, bankAccount FROM {V}WP_vProvider WHERE bankAccount IS NOT NULL AND bankAccount<>'';"),
        ("List suppliers with registered tax IDs",
         f"SELECT pvSn, pvName, taxId FROM {V}WP_vProvider WHERE taxId IS NOT NULL AND taxId<>'';"),
        ("Show all supplier categories",
         f"SELECT DISTINCT pvKId, pvKName FROM {V}WP_vProvider ORDER BY pvKName;"),
        ("Count suppliers per category",
         f"SELECT pvKId, pvKName, COUNT(*) AS count FROM {V}WP_vProvider GROUP BY pvKId, pvKName ORDER BY count DESC;"),
        ("Show suppliers sorted by discount descending",
         f"SELECT pvSn, pvName, pvDiscount FROM {V}WP_vProvider WHERE pvDiscount > 0 ORDER BY pvDiscount DESC;"),
        ("List suppliers with no discount",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider WHERE pvDiscount = 0 OR pvDiscount IS NULL ORDER BY pvName;"),
        ("Show top 5 suppliers by discount",
         f"SELECT TOP 5 pvSn, pvName, pvDiscount FROM {V}WP_vProvider ORDER BY pvDiscount DESC;"),
        ("Show suppliers with contact person information",
         f"SELECT pvSn, pvName, ctactName, ctactTel FROM {V}WP_vProvider WHERE ctactName IS NOT NULL AND ctactName<>'';"),
    ])
    # ── per-pvSn ──
    for pvs in PV_SNS:
        s += multi([
            (f"Show details for supplier pvSn {pvs}",
             f"SELECT pvSn, pvName, pvTel, pvAddr, email FROM {V}WP_vProvider WHERE pvSn='{pvs}';"),
            (f"Is supplier pvSn {pvs} active?",
             f"SELECT pvSn, pvName, isStop FROM {V}WP_vProvider WHERE pvSn='{pvs}';"),
        ])
    # ── per-supplier name ──
    for pvn in PV_NAMES:
        s += multi([
            (f"Show details for supplier {pvn}",
             f"SELECT pvSn, pvName, pvTel, email, pvAddr FROM {V}WP_vProvider WHERE pvName=N'{pvn}';"),
            (f"What is the discount for supplier {pvn}?",
             f"SELECT pvSn, pvName, pvDiscount FROM {V}WP_vProvider WHERE pvName=N'{pvn}';"),
            (f"Is supplier {pvn} active?",
             f"SELECT pvSn, pvName, isStop FROM {V}WP_vProvider WHERE pvName=N'{pvn}';"),
        ])
    # ── keyword ──
    for kw in ["有限公司","農產","茶","食品","國際","企業","工業","天然","有機"]:
        s.append(entry(f"Find suppliers with name containing {kw}",
                       f"SELECT pvSn, pvName FROM {V}WP_vProvider WHERE pvName LIKE N'%{kw}%';"))
    # ── discount thresholds ──
    for d in [3, 5, 8, 10, 15]:
        s += multi([
            (f"List suppliers with discount above {d}%",
             f"SELECT pvSn, pvName, pvDiscount FROM {V}WP_vProvider WHERE pvDiscount > {d} ORDER BY pvDiscount DESC;"),
            (f"Find suppliers offering more than {d}% discount",
             f"SELECT pvSn, pvName, pvDiscount FROM {V}WP_vProvider WHERE pvDiscount > {d} ORDER BY pvDiscount DESC;"),
        ])
    # ── aggregation ──
    s += multi([
        ("Count active vs inactive suppliers",
         f"SELECT isStop, COUNT(*) AS count FROM {V}WP_vProvider GROUP BY isStop;"),
        ("Show average discount per supplier category",
         f"SELECT pvKName, AVG(pvDiscount) AS avg_discount FROM {V}WP_vProvider GROUP BY pvKName ORDER BY avg_discount DESC;"),
        ("Which supplier category has the most members?",
         f"SELECT TOP 1 pvKName, COUNT(*) AS count FROM {V}WP_vProvider GROUP BY pvKName ORDER BY count DESC;"),
        ("List suppliers with invoice title",
         f"SELECT pvSn, pvName, invoTitle FROM {V}WP_vProvider WHERE invoTitle IS NOT NULL AND invoTitle<>'';"),
        ("Show suppliers with fax numbers",
         f"SELECT pvSn, pvName, fax FROM {V}WP_vProvider WHERE fax IS NOT NULL AND fax<>'';"),
        ("Show all suppliers with registered boss name",
         f"SELECT pvSn, pvName, pvBoss FROM {V}WP_vProvider WHERE pvBoss IS NOT NULL AND pvBoss<>'';"),
        ("Show suppliers with their abbreviations",
         f"SELECT pvSn, pvName, pvNameS FROM {V}WP_vProvider ORDER BY pvName;"),
        ("Find suppliers with discount between 5% and 10%",
         f"SELECT pvSn, pvName, pvDiscount FROM {V}WP_vProvider WHERE pvDiscount BETWEEN 5 AND 10 ORDER BY pvDiscount DESC;"),
        ("Show suppliers with bank information",
         f"SELECT pvSn, pvName, bankId, bankName FROM {V}WP_vProvider WHERE bankId IS NOT NULL AND bankId<>'';"),
        ("Find active suppliers with email",
         f"SELECT pvSn, pvName, email FROM {V}WP_vProvider WHERE isStop='N' AND email IS NOT NULL AND email<>'';"),
        ("Show suppliers sorted by name",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider ORDER BY pvName;"),
        ("List suppliers in a specific category pvKId 1",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider WHERE pvKId='1' ORDER BY pvName;"),
        ("Show suppliers with contact address same city as company",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider WHERE pvCityId = ctactCityId;"),
    ])
    return s


# ════════════════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════════════════
def main():
    acct_in   = gen_acct_in()
    acct_out  = gen_acct_out()
    out_stock = gen_out_stock()
    transfer  = gen_transfer()
    inventory = gen_inventory()
    product   = gen_product()
    provider  = gen_provider()

    ALL = acct_in + acct_out + out_stock + transfer + inventory + product + provider

    print("=" * 60)
    print(f"Total generated samples: {len(ALL)}")
    print(f"  WP_vAcctIn:   {len(acct_in)}")
    print(f"  WP_vAcctOut:  {len(acct_out)}")
    print(f"  WP_vOutStock: {len(out_stock)}")
    print(f"  WP_vTransfer: {len(transfer)}")
    print(f"  WP_vInventory:{len(inventory)}")
    print(f"  WP_vProduct:  {len(product)}")
    print(f"  WP_vProvider: {len(provider)}")
    print("=" * 60)

    # ── Verify no isDel in no-isDel views ──
    NO_ISDEL = {"WP_vInventory", "WP_vProduct", "WP_vProvider"}
    bad = []
    for sample in ALL:
        sql = sample["query"]
        views_in = set(re.findall(r"WP_v\w+", sql))
        if views_in & NO_ISDEL and re.search(r"\bisdel\b|\bdtlisdel\b", sql, re.IGNORECASE):
            bad.append(sample)
    if bad:
        print(f"\nWARNING: {len(bad)} samples wrongly use isDel in no-isDel views!")
        for b in bad[:3]:
            print(f"  Q: {b['question']}")
            print(f"  SQL: {b['query']}")
    else:
        print("\nVerification PASSED: Zero isDel in WP_vInventory/WP_vProduct/WP_vProvider")

    # Shuffle with seed for reproducibility
    random.shuffle(ALL)

    # Split 88% train / 12% validation
    split = int(len(ALL) * 0.88)
    train_set = ALL[:split]
    val_set   = ALL[split:]

    print(f"\nSplit: {len(train_set)} train  /  {len(val_set)} validation")

    out_dir = Path("data/wp_m09")
    out_dir.mkdir(parents=True, exist_ok=True)
    train_file = out_dir / "train_claude_en_2000.json"
    val_file   = out_dir / "validation_claude_en.json"

    with open(train_file, "w", encoding="utf-8") as f:
        json.dump(train_set, f, ensure_ascii=False, indent=2)
    with open(val_file, "w", encoding="utf-8") as f:
        json.dump(val_set, f, ensure_ascii=False, indent=2)

    print(f"\nFiles written:")
    print(f"  {train_file}  ({len(train_set)} samples)")
    print(f"  {val_file}  ({len(val_set)} samples)")
    print("\nDone!")


if __name__ == "__main__":
    import os
    os.chdir(Path(__file__).parent)
    main()
