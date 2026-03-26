#!/usr/bin/env python
# traindata_gen__9views_20k.py
# ============================================================
# Generate 20,000+ training samples for 9-view WP_M09 Text-to-SQL
# Natural language: English | SQL dialect: T-SQL
# ============================================================

import json
import os
import random
import sys
import time
import pyodbc
from collections import defaultdict
from datetime import datetime, timedelta

random.seed(42)

DB_CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=SHANE\SQLEXPRESS;DATABASE=WP_M09;Trusted_Connection=yes;"
)
OUTPUT_PATH = r"data\wp_m09\train_9views_20k.json"
TARGET_COUNT = 22000  # generate extra, then deduplicate

PREFIX = "WP_M09.dbo."

# ============================================================
# Full Schema with Column Types (for training prompt)
# ============================================================
FULL_SCHEMA_TYPED = (
    "-- WP_M09 (SQL Server T-SQL). Prefix: WP_M09.dbo.<View>. Chinese: N'str'.\n\n"
    "WP_vAcctIn(sn INT, acctInId VARCHAR(20), acctInDate DATETIME, amount MONEY, memo NVARCHAR(500), "
    "empId VARCHAR(10), isDel CHAR(1), dtlSn INT, OutStkId VARCHAR(20), outStkAmtTotal MONEY, "
    "dtlIsDel CHAR(1), memSn INT, memId VARCHAR(10), memName NVARCHAR(50), pNo INT, "
    "pBarcode VARCHAR(20), pName NVARCHAR(100), pNameS NVARCHAR(20), "
    "oStkDtlAmt MONEY, oStkDtlQty INT, oStkDtlAmtTotal MONEY, "
    "dtlDiscnt MONEY, dtlDiscntShare MONEY, discount MONEY, discountShare MONEY)"
    " -- Receivable. LEFT(acctInId,8)=date. isDel+dtlIsDel.\n"

    "WP_vAcctOut(sn INT, acctOutId VARCHAR(20), acctOutDate DATETIME, amount MONEY, transAmt MONEY, "
    "memo NVARCHAR(500), empId VARCHAR(10), empName NVARCHAR(50), isDel CHAR(1), dtlSn INT, "
    "InStkId VARCHAR(20), dtlAmt MONEY, qty INT, amtTotal MONEY, dtlIsDel CHAR(1), "
    "pNo INT, pName NVARCHAR(100), pNameS NVARCHAR(40), pBarcode VARCHAR(20), "
    "pvId VARCHAR(6), pvName NVARCHAR(100), pvNameS NVARCHAR(20), pvSn INT, "
    "pvDiscount FLOAT, inStkAmt MONEY, inStkAmtTotal MONEY, payType CHAR(1))"
    " -- Payable. LEFT(acctOutId,8)=date. isDel+dtlIsDel.\n"

    "WP_vOutStock(sn INT, OutStkId VARCHAR(20), OutStkDate DATETIME, amount MONEY, tax MONEY, "
    "amtNoneTax MONEY, isDel CHAR(1), empId VARCHAR(10), empName NVARCHAR(50), memo NVARCHAR(500), "
    "memSn INT, memId VARCHAR(10), memName NVARCHAR(50), outType CHAR(1), dtlSn INT, pNo INT, "
    "qty INT, dtlAmt MONEY, amtTotal MONEY, dtlIsDel CHAR(1), dtlCostAvg MONEY, dtlCostStd MONEY, "
    "dtlDiscnt MONEY, dtlDiscntPer MONEY, dtlDiscntShare MONEY, pName NVARCHAR(100), "
    "pBarcode VARCHAR(20), pUName NVARCHAR(10), costStd MONEY, discount MONEY, discountShare MONEY, "
    "memTel VARCHAR(40), memCityName VARCHAR(20), memZoneName VARCHAR(20), empName NVARCHAR(50))"
    " -- Sales/Outbound. LEFT(OutStkId,8)=date. isDel+dtlIsDel.\n"

    "WP_vTransfer(sn INT, TransferId VARCHAR(20), empId VARCHAR(10), dtlSn INT, "
    "FromWhSn INT, fWhId VARCHAR(20), fWhName VARCHAR(50), ToWhSn INT, tfWhId VARCHAR(20), "
    "tfWhName VARCHAR(50), TransferDate DATETIME, pNo INT, qty INT, pName NVARCHAR(100), "
    "pNameS NVARCHAR(40), pBarcode VARCHAR(20), pCode VARCHAR(20), isDel CHAR(1), "
    "dtlIsDel CHAR(1), costAvg MONEY)"
    " -- Transfer. LEFT(TransferId,8)=date. isDel+dtlIsDel. fWhName=source, tfWhName=dest.\n"

    "WP_vInventory(whSn INT, WarehouseId VARCHAR(20), WarehouseName VARCHAR(50), pNo INT, "
    "pName NVARCHAR(100), pNameS NVARCHAR(40), pBarcode VARCHAR(20), pUnit INT, "
    "pUName NVARCHAR(10), priceStd INT, priceLow INT, priceMem INT, priceBat INT, "
    "costStd MONEY, costAvg MONEY, isSale CHAR(1), pvName NVARCHAR(100), pvNameS NVARCHAR(20), "
    "qtyNow INT, pvSn INT, qtySafe INT, qty DECIMAL)"
    " -- Inventory. NO isDel. NO date. pNo=seq#.\n"

    "WP_vProduct(pNo INT, pName NVARCHAR(100), pNameS NVARCHAR(20), pBarcode VARCHAR(20), "
    "pCode VARCHAR(20), pUnit INT, pUName NVARCHAR(10), priceStd INT, priceLow INT, "
    "priceMem INT, priceBat INT, isPvDiscount CHAR(1), isSale CHAR(1), costStd MONEY, "
    "costAvg MONEY, pvSn INT, pvId VARCHAR(6), pvName NVARCHAR(100), pvNameS NVARCHAR(20), "
    "qtyNow INT, qtySafe INT, pvDiscount INT)"
    " -- Product. NO isDel. NO date. pNo=seq#.\n"

    "WP_vProvider(sn INT, pvId VARCHAR(6), pvName NVARCHAR(100), pvNameS NVARCHAR(20), "
    "pvKId VARCHAR(3), pvBoss NVARCHAR(50), pvTel VARCHAR(50), pvCityId VARCHAR(2), "
    "pvZoneId INT, pvCity VARCHAR(20), pvZone VARCHAR(20), pvAddr NVARCHAR(150), "
    "ctactName NVARCHAR(50), ctactTel VARCHAR(50), fax VARCHAR(50), email VARCHAR(100), "
    "taxId VARCHAR(8), isStop CHAR(1), invoTitle NVARCHAR(100), bankId VARCHAR(20), "
    "bankName NVARCHAR(50), bankAccount VARCHAR(50), bankAcctName NVARCHAR(80), "
    "memo NVARCHAR, pvKName NVARCHAR(80), pvDiscount INT)"
    " -- Supplier. NO isDel. isStop=N/Y. NO date. SELECT pvId (not pvSn).\n"

    "WP_vMemberDeposit(sn INT, memId VARCHAR(10), memName NVARCHAR(50), isStop CHAR(1), "
    "empId VARCHAR(10), isDel CHAR(1), amount INT, endDate DATETIME, OutStkId VARCHAR(20))"
    " -- MemberDeposit. isDel='N' active. isStop=N/Y member status. endDate=expiry. "
    "LEFT(OutStkId,8)=date (only when used for sale deduction). No dtlIsDel.\n"

    "WP_vPdCombine(sn INT, pNo INT, pName NVARCHAR(100), pNameS NVARCHAR(40), "
    "pBarcode VARCHAR(20), priceStd INT, isUpdStock CHAR(1), pNoS INT, pQty INT, "
    "isDel CHAR(1), sPName NVARCHAR(100), sPNameS NVARCHAR(40), sPBarcode VARCHAR(20), "
    "sPriceStd INT, sPUName NVARCHAR(10), sIsTax CHAR(1), sCostStd MONEY)"
    " -- CombinedProduct. isDel='N' active. pNo=combo product#, pNoS=sub product#, "
    "pQty=sub product qty per combo. No dtlIsDel. No date."
)


# ============================================================
# View Configuration
# ============================================================
VIEW_CFG = {
    "WP_vAcctIn": {
        "has_isDel": True, "has_dtlIsDel": True,
        "id_col": "acctInId", "date_col_prefix": "acctInId",
        "header_amount": "amount",
        "detail_amounts": ["oStkDtlAmt", "oStkDtlAmtTotal"],
        "entity": "accounts receivable", "entity_short": "receivable",
        "select_cols": ["acctInId", "acctInDate", "amount", "memName", "pName", "oStkDtlAmt", "oStkDtlQty"],
        "group_cols": ["memName", "pName", "empId"],
        "name_col": "memName", "name_type": "member",
    },
    "WP_vAcctOut": {
        "has_isDel": True, "has_dtlIsDel": True,
        "id_col": "acctOutId", "date_col_prefix": "acctOutId",
        "header_amount": "amount",
        "detail_amounts": ["dtlAmt", "amtTotal", "inStkAmt", "inStkAmtTotal"],
        "entity": "accounts payable", "entity_short": "payable",
        "select_cols": ["acctOutId", "acctOutDate", "amount", "pvName", "pName", "dtlAmt", "qty"],
        "group_cols": ["pvName", "pName", "empName", "payType"],
        "name_col": "pvName", "name_type": "supplier",
    },
    "WP_vOutStock": {
        "has_isDel": True, "has_dtlIsDel": True,
        "id_col": "OutStkId", "date_col_prefix": "OutStkId",
        "header_amount": "amount",
        "detail_amounts": ["dtlAmt", "amtTotal", "dtlCostAvg", "dtlCostStd"],
        "entity": "sales orders", "entity_short": "sale",
        "select_cols": ["OutStkId", "OutStkDate", "amount", "memName", "pName", "qty", "dtlAmt"],
        "group_cols": ["memName", "pName", "empName", "outType"],
        "name_col": "memName", "name_type": "member",
    },
    "WP_vTransfer": {
        "has_isDel": True, "has_dtlIsDel": True,
        "id_col": "TransferId", "date_col_prefix": "TransferId",
        "header_amount": None,  # Transfer has no header amount
        "detail_amounts": ["costAvg"],
        "entity": "transfer orders", "entity_short": "transfer",
        "select_cols": ["TransferId", "TransferDate", "fWhName", "tfWhName", "pName", "qty"],
        "group_cols": ["fWhName", "tfWhName", "pName"],
        "name_col": "pName", "name_type": "product",
    },
    "WP_vInventory": {
        "has_isDel": False, "has_dtlIsDel": False,
        "id_col": None, "date_col_prefix": None,
        "header_amount": None,
        "detail_amounts": ["costStd", "costAvg", "priceStd", "priceLow", "priceMem", "priceBat"],
        "entity": "inventory records", "entity_short": "inventory",
        "select_cols": ["WarehouseName", "pName", "pvName", "qtyNow", "priceStd", "costAvg"],
        "group_cols": ["WarehouseName", "pvName", "pName", "isSale"],
        "name_col": "pName", "name_type": "product",
    },
    "WP_vProduct": {
        "has_isDel": False, "has_dtlIsDel": False,
        "id_col": None, "date_col_prefix": None,
        "header_amount": None,
        "detail_amounts": ["costStd", "costAvg", "priceStd", "priceLow", "priceMem", "priceBat"],
        "entity": "products", "entity_short": "product",
        "select_cols": ["pNo", "pName", "pvName", "priceStd", "costStd", "qtyNow", "isSale"],
        "group_cols": ["pvName", "isSale", "pUName"],
        "name_col": "pName", "name_type": "product",
    },
    "WP_vProvider": {
        "has_isDel": False, "has_dtlIsDel": False, "has_isStop": True,
        "id_col": None, "date_col_prefix": None,
        "header_amount": None,
        "detail_amounts": ["pvDiscount"],
        "entity": "suppliers", "entity_short": "supplier",
        "select_cols": ["pvId", "pvName", "pvBoss", "pvTel", "pvCity", "pvZone", "pvAddr"],
        "group_cols": ["pvCity", "pvZone", "pvKName"],
        "name_col": "pvName", "name_type": "supplier",
    },
    "WP_vMemberDeposit": {
        "has_isDel": True, "has_dtlIsDel": False,
        "id_col": None, "date_col_prefix": None,
        "has_endDate": True, "has_OutStkId_date": True,
        "header_amount": None,
        "detail_amounts": ["amount"],
        "entity": "member deposits", "entity_short": "deposit",
        "select_cols": ["memId", "memName", "amount", "endDate", "isStop"],
        "group_cols": ["memName", "isStop"],
        "name_col": "memName", "name_type": "member",
    },
    "WP_vPdCombine": {
        "has_isDel": True, "has_dtlIsDel": False,
        "id_col": None, "date_col_prefix": None,
        "header_amount": None,
        "detail_amounts": ["sCostStd", "priceStd", "sPriceStd"],
        "entity": "combined products", "entity_short": "combo",
        "select_cols": ["pNo", "pName", "pNoS", "sPName", "pQty", "priceStd", "sCostStd"],
        "group_cols": ["pName", "sPName"],
        "name_col": "pName", "name_type": "combo product",
    },
}


# ============================================================
# Question phrase variants
# ============================================================
LIST_V   = ["List", "Show", "Display", "Get", "Retrieve", "Find", "Return", "Fetch"]
COUNT_V  = ["How many", "Count the number of", "What is the total count of", "Find the count of", "Get the count of"]
SUM_V    = ["What is the total", "Calculate the sum of", "Find the total", "Get the sum of", "Compute the total"]
AVG_V    = ["What is the average", "Calculate the average", "Find the average", "Get the mean", "Compute the average"]
MAX_V    = ["What is the maximum", "Find the highest", "Get the maximum", "What is the largest", "Find the max"]
MIN_V    = ["What is the minimum", "Find the lowest", "Get the minimum", "What is the smallest", "Find the min"]
TOP_V    = ["Show the top", "List the top", "Get the top", "Display the top", "Find the top"]
SORT_V   = ["sorted by", "ordered by", "arranged by", "ranked by"]
GROUP_V  = ["grouped by", "for each", "broken down by", "categorized by", "per"]
ACTIVE_PHRASES = [
    "active", "valid", "non-deleted", "existing", "current"
]


def del_filter(cfg):
    """Return WHERE clause for isDel/dtlIsDel filtering."""
    parts = []
    if cfg.get("has_isDel"):
        parts.append("isDel='N'")
    if cfg.get("has_dtlIsDel"):
        parts.append("dtlIsDel='N'")
    if cfg.get("has_isStop"):
        parts.append("isStop='N'")
    return " AND ".join(parts)


def del_filter_header_only(cfg):
    """For header-level queries (COUNT DISTINCT id, subquery dedup)."""
    parts = []
    if cfg.get("has_isDel"):
        parts.append("isDel='N'")
    return " AND ".join(parts)


# ============================================================
# Data Sampling from DB
# ============================================================
def sample_db_values(conn):
    """Sample real values from each view for template interpolation."""
    cursor = conn.cursor()
    samples = {}

    # AcctIn
    cursor.execute("SELECT DISTINCT TOP 30 acctInId FROM WP_vAcctIn WHERE isDel='N'")
    samples["acctInIds"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 30 memName FROM WP_vAcctIn WHERE isDel='N' AND memName != ''")
    samples["acctIn_memNames"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 30 pName FROM WP_vAcctIn WHERE isDel='N' AND pName != ''")
    samples["acctIn_pNames"] = [r[0] for r in cursor.fetchall()]

    # AcctOut
    cursor.execute("SELECT DISTINCT TOP 50 acctOutId FROM WP_vAcctOut WHERE isDel='N'")
    samples["acctOutIds"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 50 pvName FROM WP_vAcctOut WHERE isDel='N' AND pvName != ''")
    samples["acctOut_pvNames"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 50 pName FROM WP_vAcctOut WHERE isDel='N' AND pName != ''")
    samples["acctOut_pNames"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 10 empName FROM WP_vAcctOut WHERE isDel='N' AND empName != ''")
    samples["acctOut_empNames"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT payType FROM WP_vAcctOut WHERE isDel='N'")
    samples["payTypes"] = [r[0] for r in cursor.fetchall()]

    # OutStock
    cursor.execute("SELECT DISTINCT TOP 80 OutStkId FROM WP_vOutStock WHERE isDel='N'")
    samples["outStkIds"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 80 memName FROM WP_vOutStock WHERE isDel='N' AND memName != ''")
    samples["outStk_memNames"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 80 pName FROM WP_vOutStock WHERE isDel='N' AND pName != ''")
    samples["outStk_pNames"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 10 empName FROM WP_vOutStock WHERE isDel='N' AND empName != ''")
    samples["outStk_empNames"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT outType FROM WP_vOutStock WHERE isDel='N'")
    samples["outTypes"] = [r[0] for r in cursor.fetchall()]

    # Transfer
    cursor.execute("SELECT DISTINCT TOP 50 TransferId FROM WP_vTransfer WHERE isDel='N'")
    samples["transferIds"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 20 fWhName FROM WP_vTransfer WHERE isDel='N' AND fWhName != ''")
    samples["fWhNames"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 20 tfWhName FROM WP_vTransfer WHERE isDel='N' AND tfWhName != ''")
    samples["tfWhNames"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 50 pName FROM WP_vTransfer WHERE isDel='N' AND pName != ''")
    samples["transfer_pNames"] = [r[0] for r in cursor.fetchall()]

    # Inventory
    cursor.execute("SELECT DISTINCT TOP 20 WarehouseName FROM WP_vInventory WHERE WarehouseName IS NOT NULL AND WarehouseName != ''")
    samples["warehouseNames"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 80 pName FROM WP_vInventory WHERE pName != ''")
    samples["inv_pNames"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 50 pvName FROM WP_vInventory WHERE pvName != ''")
    samples["inv_pvNames"] = [r[0] for r in cursor.fetchall()]

    # Product
    cursor.execute("SELECT DISTINCT TOP 80 pName FROM WP_vProduct WHERE pName != ''")
    samples["prod_pNames"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 50 pvName FROM WP_vProduct WHERE pvName != ''")
    samples["prod_pvNames"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 20 pBarcode FROM WP_vProduct WHERE pBarcode != ''")
    samples["prod_barcodes"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 10 pUName FROM WP_vProduct WHERE pUName != ''")
    samples["prod_uNames"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 10 pvId FROM WP_vProduct WHERE pvId != ''")
    samples["pvIds"] = [r[0] for r in cursor.fetchall()]

    # Provider
    cursor.execute("SELECT DISTINCT TOP 50 pvName FROM WP_vProvider WHERE pvName != ''")
    samples["prov_pvNames"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 20 pvCity FROM WP_vProvider WHERE pvCity != ''")
    samples["pvCities"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 20 pvZone FROM WP_vProvider WHERE pvZone != ''")
    samples["pvZones"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 20 pvBoss FROM WP_vProvider WHERE pvBoss != ''")
    samples["pvBosses"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 10 pvKName FROM WP_vProvider WHERE pvKName != ''")
    samples["pvKNames"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 30 pvId FROM WP_vProvider WHERE pvId != ''")
    samples["prov_pvIds"] = [r[0] for r in cursor.fetchall()]

    # MemberDeposit
    cursor.execute("SELECT DISTINCT TOP 50 memName FROM WP_vMemberDeposit WHERE isDel='N' AND memName != ''")
    samples["dep_memNames"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 50 memId FROM WP_vMemberDeposit WHERE isDel='N' AND memId != ''")
    samples["dep_memIds"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 10 empId FROM WP_vMemberDeposit WHERE isDel='N'")
    samples["dep_empIds"] = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT TOP 30 OutStkId FROM WP_vMemberDeposit WHERE isDel='N' AND OutStkId != ''")
    samples["dep_outStkIds"] = [r[0] for r in cursor.fetchall()]

    # PdCombine
    cursor.execute("SELECT pNo, pName, pNoS, sPName, pQty, priceStd, sCostStd FROM WP_vPdCombine WHERE isDel='N'")
    rows = cursor.fetchall()
    samples["combo_rows"] = [{"pNo": r[0], "pName": r[1], "pNoS": r[2], "sPName": r[3],
                              "pQty": r[4], "priceStd": r[5], "sCostStd": float(r[6]) if r[6] else 0} for r in rows]
    samples["combo_pNames"] = list(set(r[1] for r in rows))
    samples["combo_sPNames"] = list(set(r[3] for r in rows))
    samples["combo_pNos"] = list(set(r[0] for r in rows))
    samples["combo_pNoSs"] = list(set(r[2] for r in rows))

    # Extract unique date prefixes from IDs
    dates = set()
    for id_list in [samples.get("acctInIds", []), samples.get("acctOutIds", []),
                    samples.get("outStkIds", []), samples.get("transferIds", [])]:
        for id_val in id_list:
            if len(id_val) >= 8:
                dates.add(id_val[:8])
    samples["date_prefixes"] = sorted(dates)

    # Generate year-month prefixes
    ym_set = set()
    for d in samples["date_prefixes"]:
        if len(d) >= 6:
            ym_set.add(d[:6])
    samples["year_month_prefixes"] = sorted(ym_set)

    # Common numeric values
    samples["top_ns"] = [1, 3, 5, 10, 15, 20, 50, 100]
    samples["amounts"] = [100, 500, 1000, 2000, 5000, 10000, 50000, 100000]
    samples["quantities"] = [1, 2, 3, 5, 10, 20, 50, 100]

    return samples


# ============================================================
# Template-based Generation
# ============================================================
def generate_all_samples(samples):
    """Generate all training samples from templates."""
    all_samples = []

    # ---- WP_vAcctIn ----
    all_samples.extend(gen_isDel_dtlIsDel_view(
        view="WP_vAcctIn", cfg=VIEW_CFG["WP_vAcctIn"], samples=samples,
        names_key="acctIn_memNames", pnames_key="acctIn_pNames", ids_key="acctInIds",
    ))

    # ---- WP_vAcctOut ----
    all_samples.extend(gen_isDel_dtlIsDel_view(
        view="WP_vAcctOut", cfg=VIEW_CFG["WP_vAcctOut"], samples=samples,
        names_key="acctOut_pvNames", pnames_key="acctOut_pNames", ids_key="acctOutIds",
        extra_empnames_key="acctOut_empNames",
    ))

    # ---- WP_vOutStock ----
    all_samples.extend(gen_isDel_dtlIsDel_view(
        view="WP_vOutStock", cfg=VIEW_CFG["WP_vOutStock"], samples=samples,
        names_key="outStk_memNames", pnames_key="outStk_pNames", ids_key="outStkIds",
        extra_empnames_key="outStk_empNames",
    ))

    # ---- WP_vTransfer ----
    all_samples.extend(gen_transfer(samples))

    # ---- WP_vInventory ----
    all_samples.extend(gen_inventory(samples))

    # ---- WP_vProduct ----
    all_samples.extend(gen_product(samples))

    # ---- WP_vProvider ----
    all_samples.extend(gen_provider(samples))

    # ---- WP_vMemberDeposit ----
    all_samples.extend(gen_member_deposit(samples))

    # ---- WP_vPdCombine ----
    all_samples.extend(gen_pd_combine(samples))

    return all_samples


# ============================================================
# Generator: isDel + dtlIsDel views (AcctIn, AcctOut, OutStock)
# ============================================================
def gen_isDel_dtlIsDel_view(view, cfg, samples, names_key, pnames_key, ids_key,
                             extra_empnames_key=None):
    """Generate samples for views with isDel + dtlIsDel + date prefix."""
    result = []
    v = f"{PREFIX}{view}"
    id_col = cfg["id_col"]
    hdr_amt = cfg["header_amount"]
    entity = cfg["entity"]
    entity_s = cfg["entity_short"]
    name_col = cfg["name_col"]
    name_type = cfg["name_type"]
    names = samples.get(names_key, [])
    pnames = samples.get(pnames_key, [])
    ids = samples.get(ids_key, [])
    dates = samples.get("date_prefixes", [])
    yms = samples.get("year_month_prefixes", [])
    top_ns = samples["top_ns"]
    amounts = samples["amounts"]
    qtys = samples["quantities"]

    delf = "isDel='N' AND dtlIsDel='N'"
    delf_hdr = "isDel='N'"

    # --- EASY: Basic queries ---
    for lv in LIST_V:
        result.append({"question": f"{lv} all {random.choice(ACTIVE_PHRASES)} {entity} IDs.",
                        "query": f"SELECT DISTINCT {id_col} FROM {v} WHERE {delf}",
                        "difficulty": "easy"})

    for cv in COUNT_V:
        result.append({"question": f"{cv} {random.choice(ACTIVE_PHRASES)} {entity} orders?",
                        "query": f"SELECT COUNT(DISTINCT {id_col}) FROM {v} WHERE {delf}",
                        "difficulty": "easy"})

    # SELECT * for specific ID
    for sid in random.sample(ids, min(40, len(ids))):
        lv = random.choice(LIST_V)
        result.append({
            "question": f"{lv} the details of {entity_s} ID '{sid}'.",
            "query": f"SELECT * FROM {v} WHERE {id_col}='{sid}' AND {delf}",
            "difficulty": "easy"
        })

    # COUNT for specific ID
    for sid in random.sample(ids, min(20, len(ids))):
        cv = random.choice(COUNT_V)
        result.append({
            "question": f"{cv} detail items in {entity_s} order '{sid}'?",
            "query": f"SELECT COUNT(*) FROM {v} WHERE {id_col}='{sid}' AND {delf}",
            "difficulty": "easy"
        })

    # --- EASY: Date queries ---
    for d in random.sample(dates, min(40, len(dates))):
        lv = random.choice(LIST_V)
        date_str = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d
        result.append({
            "question": f"{lv} all {entity} on {date_str}.",
            "query": f"SELECT * FROM {v} WHERE LEFT({id_col},8)='{d}' AND {delf}",
            "difficulty": "easy"
        })

    for d in random.sample(dates, min(30, len(dates))):
        cv = random.choice(COUNT_V)
        date_str = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d
        result.append({
            "question": f"{cv} {entity} orders on {date_str}?",
            "query": f"SELECT COUNT(DISTINCT {id_col}) FROM {v} WHERE LEFT({id_col},8)='{d}' AND {delf}",
            "difficulty": "easy"
        })

    # Year-month queries
    for ym in random.sample(yms, min(20, len(yms))):
        lv = random.choice(LIST_V)
        ym_str = f"{ym[:4]}-{ym[4:6]}"
        result.append({
            "question": f"{lv} all {entity} in {ym_str}.",
            "query": f"SELECT * FROM {v} WHERE LEFT({id_col},6)='{ym}' AND {delf}",
            "difficulty": "easy"
        })
        cv = random.choice(COUNT_V)
        result.append({
            "question": f"{cv} {entity} orders in {ym_str}?",
            "query": f"SELECT COUNT(DISTINCT {id_col}) FROM {v} WHERE LEFT({id_col},6)='{ym}' AND {delf}",
            "difficulty": "easy"
        })

    # --- EASY: Name/product filter ---
    for nm in random.sample(names, min(50, len(names))):
        lv = random.choice(LIST_V)
        result.append({
            "question": f"{lv} all {entity} for {name_type} N'{nm}'.",
            "query": f"SELECT * FROM {v} WHERE {name_col}=N'{nm}' AND {delf}",
            "difficulty": "easy"
        })

    for pn in random.sample(pnames, min(50, len(pnames))):
        lv = random.choice(LIST_V)
        result.append({
            "question": f"{lv} all {entity} for product N'{pn}'.",
            "query": f"SELECT * FROM {v} WHERE pName=N'{pn}' AND {delf}",
            "difficulty": "easy"
        })

    # LIKE queries
    for nm in random.sample(names, min(30, len(names))):
        char = nm[0] if nm else ''
        lv = random.choice(LIST_V)
        result.append({
            "question": f"{lv} {entity} where {name_type} name starts with N'{char}'.",
            "query": f"SELECT * FROM {v} WHERE {name_col} LIKE N'{char}%' AND {delf}",
            "difficulty": "easy"
        })

    for pn in random.sample(pnames, min(30, len(pnames))):
        keyword = pn[:2] if len(pn) >= 2 else pn
        lv = random.choice(LIST_V)
        result.append({
            "question": f"{lv} {entity} where product name contains N'{keyword}'.",
            "query": f"SELECT * FROM {v} WHERE pName LIKE N'%{keyword}%' AND {delf}",
            "difficulty": "easy"
        })

    # --- MEDIUM: Aggregation with subquery dedup ---
    if hdr_amt:
        for sv in SUM_V:
            result.append({
                "question": f"{sv} {hdr_amt} of all {random.choice(ACTIVE_PHRASES)} {entity}?",
                "query": f"SELECT SUM({hdr_amt}) FROM (SELECT DISTINCT {id_col}, {hdr_amt} FROM {v} WHERE {delf_hdr}) sub",
                "difficulty": "medium"
            })
        for av in AVG_V:
            result.append({
                "question": f"{av} {hdr_amt} of {entity}?",
                "query": f"SELECT AVG({hdr_amt}) FROM (SELECT DISTINCT {id_col}, {hdr_amt} FROM {v} WHERE {delf_hdr}) sub",
                "difficulty": "medium"
            })
        for mv in MAX_V:
            result.append({
                "question": f"{mv} {hdr_amt} among all {entity}?",
                "query": f"SELECT MAX({hdr_amt}) FROM (SELECT DISTINCT {id_col}, {hdr_amt} FROM {v} WHERE {delf_hdr}) sub",
                "difficulty": "medium"
            })
        for mv in MIN_V:
            result.append({
                "question": f"{mv} {hdr_amt} among all {entity}?",
                "query": f"SELECT MIN({hdr_amt}) FROM (SELECT DISTINCT {id_col}, {hdr_amt} FROM {v} WHERE {delf_hdr}) sub",
                "difficulty": "medium"
            })

        # SUM/AVG by date
        for d in random.sample(dates, min(30, len(dates))):
            date_str = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d
            sv = random.choice(SUM_V)
            result.append({
                "question": f"{sv} {hdr_amt} of {entity} on {date_str}?",
                "query": f"SELECT SUM({hdr_amt}) FROM (SELECT DISTINCT {id_col}, {hdr_amt} FROM {v} WHERE LEFT({id_col},8)='{d}' AND {delf_hdr}) sub",
                "difficulty": "medium"
            })

        # SUM by year-month
        for ym in random.sample(yms, min(15, len(yms))):
            ym_str = f"{ym[:4]}-{ym[4:6]}"
            sv = random.choice(SUM_V)
            result.append({
                "question": f"{sv} {hdr_amt} of {entity} in {ym_str}?",
                "query": f"SELECT SUM({hdr_amt}) FROM (SELECT DISTINCT {id_col}, {hdr_amt} FROM {v} WHERE LEFT({id_col},6)='{ym}' AND {delf_hdr}) sub",
                "difficulty": "medium"
            })

        # SUM by name
        for nm in random.sample(names, min(30, len(names))):
            sv = random.choice(SUM_V)
            result.append({
                "question": f"{sv} {hdr_amt} of {entity} for {name_type} N'{nm}'?",
                "query": f"SELECT SUM({hdr_amt}) FROM (SELECT DISTINCT {id_col}, {hdr_amt} FROM {v} WHERE {name_col}=N'{nm}' AND {delf_hdr}) sub",
                "difficulty": "medium"
            })

    # Detail-level aggregation (no subquery needed)
    for da in cfg.get("detail_amounts", [])[:2]:
        for sv in SUM_V[:3]:
            result.append({
                "question": f"{sv} {da} across all {random.choice(ACTIVE_PHRASES)} {entity} details?",
                "query": f"SELECT SUM({da}) FROM {v} WHERE {delf}",
                "difficulty": "medium"
            })

    # --- MEDIUM: GROUP BY ---
    for gc in cfg["group_cols"]:
        lv = random.choice(LIST_V)
        gv = random.choice(GROUP_V)
        result.append({
            "question": f"{lv} the number of {entity} orders {gv} {gc}.",
            "query": f"SELECT {gc}, COUNT(DISTINCT {id_col}) FROM {v} WHERE {delf} GROUP BY {gc}",
            "difficulty": "medium"
        })

    if hdr_amt:
        for gc in cfg["group_cols"]:
            lv = random.choice(LIST_V)
            gv = random.choice(GROUP_V)
            result.append({
                "question": f"{lv} the total {hdr_amt} {gv} {gc}.",
                "query": f"SELECT sub.{gc}, SUM(sub.{hdr_amt}) FROM (SELECT DISTINCT {id_col}, {hdr_amt}, {gc} FROM {v} WHERE {delf_hdr}) sub GROUP BY sub.{gc}",
                "difficulty": "medium"
            })

    # GROUP BY with date
    for ym in random.sample(yms, min(10, len(yms))):
        ym_str = f"{ym[:4]}-{ym[4:6]}"
        result.append({
            "question": f"Show the number of {entity} orders {random.choice(GROUP_V)} {name_col} in {ym_str}.",
            "query": f"SELECT {name_col}, COUNT(DISTINCT {id_col}) FROM {v} WHERE LEFT({id_col},6)='{ym}' AND {delf} GROUP BY {name_col}",
            "difficulty": "medium"
        })

    # --- MEDIUM: ORDER BY + TOP N ---
    for n in random.sample(top_ns, min(5, len(top_ns))):
        tv = random.choice(TOP_V)
        result.append({
            "question": f"{tv} {n} {entity} orders by {id_col} descending.",
            "query": f"SELECT DISTINCT TOP {n} {id_col}, {hdr_amt or 'sn'} FROM {v} WHERE {delf} ORDER BY {id_col} DESC",
            "difficulty": "medium"
        })

    if hdr_amt:
        for n in random.sample(top_ns, min(5, len(top_ns))):
            tv = random.choice(TOP_V)
            result.append({
                "question": f"{tv} {n} highest {hdr_amt} {entity} orders.",
                "query": f"SELECT DISTINCT TOP {n} {id_col}, {hdr_amt} FROM {v} WHERE {delf_hdr} ORDER BY {hdr_amt} DESC",
                "difficulty": "medium"
            })

    # --- MEDIUM: DISTINCT columns ---
    for col in cfg["select_cols"][:4]:
        lv = random.choice(LIST_V)
        result.append({
            "question": f"{lv} all distinct {col} values from {entity}.",
            "query": f"SELECT DISTINCT {col} FROM {v} WHERE {delf}",
            "difficulty": "easy"
        })

    # --- HARD: Combined conditions ---
    for nm in random.sample(names, min(20, len(names))):
        for d in random.sample(dates, min(3, len(dates))):
            date_str = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d
            lv = random.choice(LIST_V)
            result.append({
                "question": f"{lv} {entity} for {name_type} N'{nm}' on {date_str}.",
                "query": f"SELECT * FROM {v} WHERE {name_col}=N'{nm}' AND LEFT({id_col},8)='{d}' AND {delf}",
                "difficulty": "hard"
            })

    for pn in random.sample(pnames, min(20, len(pnames))):
        for d in random.sample(dates, min(3, len(dates))):
            date_str = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d
            lv = random.choice(LIST_V)
            result.append({
                "question": f"{lv} {entity} for product N'{pn}' on {date_str}.",
                "query": f"SELECT * FROM {v} WHERE pName=N'{pn}' AND LEFT({id_col},8)='{d}' AND {delf}",
                "difficulty": "hard"
            })

    # --- HARD: HAVING ---
    if hdr_amt:
        for amt in random.sample(amounts, min(5, len(amounts))):
            result.append({
                "question": f"Which {name_col}s have total {hdr_amt} greater than {amt} in {entity}?",
                "query": f"SELECT sub.{name_col}, SUM(sub.{hdr_amt}) FROM (SELECT DISTINCT {id_col}, {hdr_amt}, {name_col} FROM {v} WHERE {delf_hdr}) sub GROUP BY sub.{name_col} HAVING SUM(sub.{hdr_amt}) > {amt}",
                "difficulty": "hard"
            })

    # --- HARD: Amount range with BETWEEN ---
    if hdr_amt:
        for i in range(15):
            lo = random.choice([100, 500, 1000, 2000, 5000])
            hi = lo * random.choice([2, 5, 10])
            lv = random.choice(LIST_V)
            result.append({
                "question": f"{lv} {entity} orders with {hdr_amt} between {lo} and {hi}.",
                "query": f"SELECT DISTINCT {id_col}, {hdr_amt} FROM {v} WHERE {hdr_amt} BETWEEN {lo} AND {hi} AND {delf_hdr}",
                "difficulty": "hard"
            })

    # --- HARD: Subquery (name in date range) ---
    for ym in random.sample(yms, min(10, len(yms))):
        ym_str = f"{ym[:4]}-{ym[4:6]}"
        result.append({
            "question": f"Which {name_type}s had {entity} in {ym_str}?",
            "query": f"SELECT DISTINCT {name_col} FROM {v} WHERE LEFT({id_col},6)='{ym}' AND {delf}",
            "difficulty": "medium"
        })

    # --- HARD: Count per product per date ---
    for d in random.sample(dates, min(10, len(dates))):
        date_str = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d
        result.append({
            "question": f"Show the quantity of each product sold in {entity} on {date_str}.",
            "query": f"SELECT pName, SUM(qty) FROM {v} WHERE LEFT({id_col},8)='{d}' AND {delf} GROUP BY pName"
                     if "qty" in str(cfg["select_cols"]) else
                     f"SELECT pName, COUNT(*) FROM {v} WHERE LEFT({id_col},8)='{d}' AND {delf} GROUP BY pName",
            "difficulty": "hard"
        })

    # empName queries (AcctOut, OutStock)
    if extra_empnames_key:
        empnames = samples.get(extra_empnames_key, [])
        for en in random.sample(empnames, min(20, len(empnames))):
            lv = random.choice(LIST_V)
            result.append({
                "question": f"{lv} all {entity} handled by employee N'{en}'.",
                "query": f"SELECT * FROM {v} WHERE empName=N'{en}' AND {delf}",
                "difficulty": "easy"
            })
            cv = random.choice(COUNT_V)
            result.append({
                "question": f"{cv} {entity} orders handled by employee N'{en}'?",
                "query": f"SELECT COUNT(DISTINCT {id_col}) FROM {v} WHERE empName=N'{en}' AND {delf}",
                "difficulty": "easy"
            })

    # AcctOut specific: payType
    if view == "WP_vAcctOut":
        for pt in samples.get("payTypes", []):
            lv = random.choice(LIST_V)
            result.append({
                "question": f"{lv} all payable records with payment type '{pt}'.",
                "query": f"SELECT * FROM {v} WHERE payType='{pt}' AND {delf}",
                "difficulty": "easy"
            })
            cv = random.choice(COUNT_V)
            result.append({
                "question": f"{cv} payable orders with payment type '{pt}'?",
                "query": f"SELECT COUNT(DISTINCT acctOutId) FROM {v} WHERE payType='{pt}' AND {delf}",
                "difficulty": "easy"
            })

    # OutStock specific: outType
    if view == "WP_vOutStock":
        for ot in samples.get("outTypes", []):
            lv = random.choice(LIST_V)
            result.append({
                "question": f"{lv} all sales with out type '{ot}'.",
                "query": f"SELECT * FROM {v} WHERE outType='{ot}' AND {delf}",
                "difficulty": "easy"
            })

    # --- Qty-based queries for OutStock ---
    if view == "WP_vOutStock":
        for q in random.sample(qtys, min(5, len(qtys))):
            result.append({
                "question": f"Find sales details where quantity is greater than {q}.",
                "query": f"SELECT * FROM {v} WHERE qty > {q} AND {delf}",
                "difficulty": "easy"
            })
        # SUM qty by product
        result.append({
            "question": "Show total quantity sold for each product.",
            "query": f"SELECT pName, SUM(qty) FROM {v} WHERE {delf} GROUP BY pName",
            "difficulty": "medium"
        })
        # SUM qty by product in date range
        for ym in random.sample(yms, min(10, len(yms))):
            ym_str = f"{ym[:4]}-{ym[4:6]}"
            result.append({
                "question": f"Show total quantity sold for each product in {ym_str}.",
                "query": f"SELECT pName, SUM(qty) FROM {v} WHERE LEFT(OutStkId,6)='{ym}' AND {delf} GROUP BY pName",
                "difficulty": "medium"
            })

    # --- Additional phrasing variants for diversity ---
    for nm in random.sample(names, min(20, len(names))):
        result.append({
            "question": f"Does {name_type} N'{nm}' have any {entity} records?",
            "query": f"SELECT COUNT(DISTINCT {id_col}) FROM {v} WHERE {name_col}=N'{nm}' AND {delf}",
            "difficulty": "easy"
        })

    for sid in random.sample(ids, min(15, len(ids))):
        result.append({
            "question": f"What products are in {entity_s} order '{sid}'?",
            "query": f"SELECT DISTINCT pName FROM {v} WHERE {id_col}='{sid}' AND {delf}",
            "difficulty": "easy"
        })

    return result


# ============================================================
# Generator: WP_vTransfer
# ============================================================
def gen_transfer(samples):
    result = []
    v = f"{PREFIX}WP_vTransfer"
    delf = "isDel='N' AND dtlIsDel='N'"
    ids = samples.get("transferIds", [])
    dates = samples.get("date_prefixes", [])
    yms = samples.get("year_month_prefixes", [])
    fwhs = samples.get("fWhNames", [])
    twhs = samples.get("tfWhNames", [])
    pnames = samples.get("transfer_pNames", [])

    # Basic queries
    for lv in LIST_V:
        result.append({"question": f"{lv} all active transfer orders.", "query": f"SELECT * FROM {v} WHERE {delf}", "difficulty": "easy"})
    for cv in COUNT_V:
        result.append({"question": f"{cv} active transfer orders?", "query": f"SELECT COUNT(DISTINCT TransferId) FROM {v} WHERE {delf}", "difficulty": "easy"})

    # By ID
    for sid in random.sample(ids, min(40, len(ids))):
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} transfer order '{sid}'.", "query": f"SELECT * FROM {v} WHERE TransferId='{sid}' AND {delf}", "difficulty": "easy"})

    # By date
    for d in random.sample(dates, min(40, len(dates))):
        ds = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} all transfers on {ds}.", "query": f"SELECT * FROM {v} WHERE LEFT(TransferId,8)='{d}' AND {delf}", "difficulty": "easy"})
        cv = random.choice(COUNT_V)
        result.append({"question": f"{cv} transfer orders on {ds}?", "query": f"SELECT COUNT(DISTINCT TransferId) FROM {v} WHERE LEFT(TransferId,8)='{d}' AND {delf}", "difficulty": "easy"})

    for ym in random.sample(yms, min(15, len(yms))):
        yms_ = f"{ym[:4]}-{ym[4:6]}"
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} all transfers in {yms_}.", "query": f"SELECT * FROM {v} WHERE LEFT(TransferId,6)='{ym}' AND {delf}", "difficulty": "easy"})

    # By warehouse
    for wh in random.sample(fwhs, min(15, len(fwhs))):
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} all transfers from warehouse N'{wh}'.", "query": f"SELECT * FROM {v} WHERE fWhName=N'{wh}' AND {delf}", "difficulty": "easy"})
        cv = random.choice(COUNT_V)
        result.append({"question": f"{cv} transfers from warehouse N'{wh}'?", "query": f"SELECT COUNT(DISTINCT TransferId) FROM {v} WHERE fWhName=N'{wh}' AND {delf}", "difficulty": "easy"})

    for wh in random.sample(twhs, min(15, len(twhs))):
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} all transfers to warehouse N'{wh}'.", "query": f"SELECT * FROM {v} WHERE tfWhName=N'{wh}' AND {delf}", "difficulty": "easy"})

    # By product
    for pn in random.sample(pnames, min(40, len(pnames))):
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} all transfers of product N'{pn}'.", "query": f"SELECT * FROM {v} WHERE pName=N'{pn}' AND {delf}", "difficulty": "easy"})

    # GROUP BY
    result.append({"question": "Show transfer count by source warehouse.", "query": f"SELECT fWhName, COUNT(DISTINCT TransferId) FROM {v} WHERE {delf} GROUP BY fWhName", "difficulty": "medium"})
    result.append({"question": "Show transfer count by destination warehouse.", "query": f"SELECT tfWhName, COUNT(DISTINCT TransferId) FROM {v} WHERE {delf} GROUP BY tfWhName", "difficulty": "medium"})
    result.append({"question": "Show total quantity transferred per product.", "query": f"SELECT pName, SUM(qty) FROM {v} WHERE {delf} GROUP BY pName", "difficulty": "medium"})

    for ym in random.sample(yms, min(10, len(yms))):
        yms_ = f"{ym[:4]}-{ym[4:6]}"
        result.append({"question": f"Show total quantity transferred per product in {yms_}.", "query": f"SELECT pName, SUM(qty) FROM {v} WHERE LEFT(TransferId,6)='{ym}' AND {delf} GROUP BY pName", "difficulty": "medium"})

    # Combined conditions
    for wh in random.sample(fwhs, min(10, len(fwhs))):
        for d in random.sample(dates, min(3, len(dates))):
            ds = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d
            lv = random.choice(LIST_V)
            result.append({"question": f"{lv} transfers from N'{wh}' on {ds}.", "query": f"SELECT * FROM {v} WHERE fWhName=N'{wh}' AND LEFT(TransferId,8)='{d}' AND {delf}", "difficulty": "hard"})

    for fwh in random.sample(fwhs, min(5, len(fwhs))):
        for twh in random.sample(twhs, min(5, len(twhs))):
            lv = random.choice(LIST_V)
            result.append({"question": f"{lv} transfers from N'{fwh}' to N'{twh}'.", "query": f"SELECT * FROM {v} WHERE fWhName=N'{fwh}' AND tfWhName=N'{twh}' AND {delf}", "difficulty": "hard"})

    # SUM costAvg
    result.append({"question": "What is the total transfer cost (costAvg) for all active transfers?", "query": f"SELECT SUM(costAvg * qty) FROM {v} WHERE {delf}", "difficulty": "medium"})

    # TOP N
    for n in [5, 10, 20]:
        result.append({"question": f"Show the top {n} transfers by quantity.", "query": f"SELECT TOP {n} TransferId, pName, qty FROM {v} WHERE {delf} ORDER BY qty DESC", "difficulty": "medium"})

    return result


# ============================================================
# Generator: WP_vInventory
# ============================================================
def gen_inventory(samples):
    result = []
    v = f"{PREFIX}WP_vInventory"
    whs = samples.get("warehouseNames", [])
    pnames = samples.get("inv_pNames", [])
    pvnames = samples.get("inv_pvNames", [])

    # Basic
    for lv in LIST_V:
        result.append({"question": f"{lv} all inventory records.", "query": f"SELECT * FROM {v}", "difficulty": "easy"})
    for cv in COUNT_V:
        result.append({"question": f"{cv} inventory records?", "query": f"SELECT COUNT(*) FROM {v}", "difficulty": "easy"})

    # By warehouse
    for wh in random.sample(whs, min(15, len(whs))):
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} all inventory in warehouse N'{wh}'.", "query": f"SELECT * FROM {v} WHERE WarehouseName=N'{wh}'", "difficulty": "easy"})
        cv = random.choice(COUNT_V)
        result.append({"question": f"{cv} products in warehouse N'{wh}'?", "query": f"SELECT COUNT(*) FROM {v} WHERE WarehouseName=N'{wh}'", "difficulty": "easy"})

    # By product
    for pn in random.sample(pnames, min(60, len(pnames))):
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} inventory for product N'{pn}'.", "query": f"SELECT * FROM {v} WHERE pName=N'{pn}'", "difficulty": "easy"})

    # By supplier
    for pvn in random.sample(pvnames, min(40, len(pvnames))):
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} inventory from supplier N'{pvn}'.", "query": f"SELECT * FROM {v} WHERE pvName=N'{pvn}'", "difficulty": "easy"})

    # qtyNow queries
    result.append({"question": "Show products with zero inventory.", "query": f"SELECT pName, WarehouseName, qtyNow FROM {v} WHERE qtyNow=0", "difficulty": "easy"})
    result.append({"question": "Show products where current stock is below safety stock.", "query": f"SELECT pName, WarehouseName, qtyNow, qtySafe FROM {v} WHERE qtyNow < qtySafe", "difficulty": "medium"})
    for q in [0, 5, 10, 50, 100]:
        result.append({"question": f"Show products with inventory greater than {q}.", "query": f"SELECT pName, WarehouseName, qtyNow FROM {v} WHERE qtyNow > {q}", "difficulty": "easy"})

    # isSale filter
    result.append({"question": "Show all sellable products in inventory.", "query": f"SELECT * FROM {v} WHERE isSale='Y'", "difficulty": "easy"})
    result.append({"question": "Show all non-sellable products in inventory.", "query": f"SELECT * FROM {v} WHERE isSale='N'", "difficulty": "easy"})

    # GROUP BY warehouse
    result.append({"question": "Show total inventory quantity per warehouse.", "query": f"SELECT WarehouseName, SUM(qtyNow) FROM {v} GROUP BY WarehouseName", "difficulty": "medium"})
    result.append({"question": "Show the number of products per warehouse.", "query": f"SELECT WarehouseName, COUNT(*) FROM {v} GROUP BY WarehouseName", "difficulty": "medium"})
    result.append({"question": "Show total inventory value (costAvg * qtyNow) per warehouse.", "query": f"SELECT WarehouseName, SUM(costAvg * qtyNow) FROM {v} GROUP BY WarehouseName", "difficulty": "medium"})

    # GROUP BY supplier
    result.append({"question": "Show total inventory quantity per supplier.", "query": f"SELECT pvName, SUM(qtyNow) FROM {v} GROUP BY pvName", "difficulty": "medium"})
    result.append({"question": "Show the number of products per supplier in inventory.", "query": f"SELECT pvName, COUNT(*) FROM {v} GROUP BY pvName", "difficulty": "medium"})

    # Aggregation
    for mv in MAX_V[:3]:
        result.append({"question": f"{mv} standard price in inventory?", "query": f"SELECT MAX(priceStd) FROM {v}", "difficulty": "easy"})
    for mv in MIN_V[:3]:
        result.append({"question": f"{mv} standard price in inventory?", "query": f"SELECT MIN(priceStd) FROM {v} WHERE priceStd > 0", "difficulty": "easy"})
    for av in AVG_V[:3]:
        result.append({"question": f"{av} cost (costAvg) in inventory?", "query": f"SELECT AVG(costAvg) FROM {v}", "difficulty": "easy"})

    # LIKE
    for pn in random.sample(pnames, min(30, len(pnames))):
        kw = pn[:2] if len(pn) >= 2 else pn
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} inventory where product name contains N'{kw}'.", "query": f"SELECT * FROM {v} WHERE pName LIKE N'%{kw}%'", "difficulty": "easy"})

    # Combined: warehouse + supplier
    for wh in random.sample(whs, min(5, len(whs))):
        for pvn in random.sample(pvnames, min(5, len(pvnames))):
            lv = random.choice(LIST_V)
            result.append({"question": f"{lv} inventory in warehouse N'{wh}' from supplier N'{pvn}'.", "query": f"SELECT * FROM {v} WHERE WarehouseName=N'{wh}' AND pvName=N'{pvn}'", "difficulty": "hard"})

    # TOP N
    for n in [5, 10, 20]:
        result.append({"question": f"Show top {n} products by inventory quantity.", "query": f"SELECT TOP {n} pName, WarehouseName, qtyNow FROM {v} ORDER BY qtyNow DESC", "difficulty": "medium"})
        result.append({"question": f"Show top {n} most expensive products by standard price.", "query": f"SELECT TOP {n} pName, priceStd FROM {v} ORDER BY priceStd DESC", "difficulty": "medium"})

    # Price comparison
    result.append({"question": "Show products where member price is lower than standard price.", "query": f"SELECT pName, priceStd, priceMem FROM {v} WHERE priceMem < priceStd", "difficulty": "medium"})
    result.append({"question": "Show products where cost average exceeds standard cost.", "query": f"SELECT pName, costStd, costAvg FROM {v} WHERE costAvg > costStd", "difficulty": "medium"})

    # HAVING
    for q in [100, 500, 1000]:
        result.append({"question": f"Which warehouses have total inventory exceeding {q}?", "query": f"SELECT WarehouseName, SUM(qtyNow) FROM {v} GROUP BY WarehouseName HAVING SUM(qtyNow) > {q}", "difficulty": "hard"})

    return result


# ============================================================
# Generator: WP_vProduct
# ============================================================
def gen_product(samples):
    result = []
    v = f"{PREFIX}WP_vProduct"
    pnames = samples.get("prod_pNames", [])
    pvnames = samples.get("prod_pvNames", [])
    barcodes = samples.get("prod_barcodes", [])
    unames = samples.get("prod_uNames", [])
    pvids = samples.get("pvIds", [])

    # Basic
    for lv in LIST_V:
        result.append({"question": f"{lv} all products.", "query": f"SELECT * FROM {v}", "difficulty": "easy"})
    for cv in COUNT_V:
        result.append({"question": f"{cv} products?", "query": f"SELECT COUNT(*) FROM {v}", "difficulty": "easy"})

    # By name
    for pn in random.sample(pnames, min(60, len(pnames))):
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} product N'{pn}'.", "query": f"SELECT * FROM {v} WHERE pName=N'{pn}'", "difficulty": "easy"})

    # By supplier
    for pvn in random.sample(pvnames, min(40, len(pvnames))):
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} all products from supplier N'{pvn}'.", "query": f"SELECT * FROM {v} WHERE pvName=N'{pvn}'", "difficulty": "easy"})
        cv = random.choice(COUNT_V)
        result.append({"question": f"{cv} products from supplier N'{pvn}'?", "query": f"SELECT COUNT(*) FROM {v} WHERE pvName=N'{pvn}'", "difficulty": "easy"})

    # By barcode
    for bc in random.sample(barcodes, min(20, len(barcodes))):
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} the product with barcode '{bc}'.", "query": f"SELECT * FROM {v} WHERE pBarcode='{bc}'", "difficulty": "easy"})

    # By pvId
    for pid in random.sample(pvids, min(10, len(pvids))):
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} products from supplier ID '{pid}'.", "query": f"SELECT * FROM {v} WHERE pvId='{pid}'", "difficulty": "easy"})

    # isSale
    result.append({"question": "Show all sellable products.", "query": f"SELECT * FROM {v} WHERE isSale='Y'", "difficulty": "easy"})
    result.append({"question": "Show all non-sellable products.", "query": f"SELECT * FROM {v} WHERE isSale='N'", "difficulty": "easy"})
    result.append({"question": "How many products are currently sellable?", "query": f"SELECT COUNT(*) FROM {v} WHERE isSale='Y'", "difficulty": "easy"})

    # Price queries
    for mv in MAX_V[:3]:
        result.append({"question": f"{mv} standard price among all products?", "query": f"SELECT MAX(priceStd) FROM {v}", "difficulty": "easy"})
    for mv in MIN_V[:3]:
        result.append({"question": f"{mv} standard price among all products?", "query": f"SELECT MIN(priceStd) FROM {v} WHERE priceStd > 0", "difficulty": "easy"})
    for av in AVG_V[:3]:
        result.append({"question": f"{av} standard price of all products?", "query": f"SELECT AVG(priceStd) FROM {v}", "difficulty": "easy"})

    # Price ranges
    for lo, hi in [(0, 100), (100, 500), (500, 1000), (1000, 5000), (5000, 10000)]:
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} products with standard price between {lo} and {hi}.", "query": f"SELECT * FROM {v} WHERE priceStd BETWEEN {lo} AND {hi}", "difficulty": "easy"})

    # GROUP BY supplier
    result.append({"question": "Show the number of products per supplier.", "query": f"SELECT pvName, COUNT(*) FROM {v} GROUP BY pvName", "difficulty": "medium"})
    result.append({"question": "Show average standard price per supplier.", "query": f"SELECT pvName, AVG(priceStd) FROM {v} GROUP BY pvName", "difficulty": "medium"})

    # GROUP BY unit
    for un in random.sample(unames, min(8, len(unames))):
        cv = random.choice(COUNT_V)
        result.append({"question": f"{cv} products with unit N'{un}'?", "query": f"SELECT COUNT(*) FROM {v} WHERE pUName=N'{un}'", "difficulty": "easy"})

    result.append({"question": "Show product count by unit.", "query": f"SELECT pUName, COUNT(*) FROM {v} GROUP BY pUName", "difficulty": "medium"})

    # LIKE
    for pn in random.sample(pnames, min(30, len(pnames))):
        kw = pn[:2] if len(pn) >= 2 else pn
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} products with name containing N'{kw}'.", "query": f"SELECT * FROM {v} WHERE pName LIKE N'%{kw}%'", "difficulty": "easy"})

    # Cost comparison
    result.append({"question": "Show products where average cost exceeds standard cost.", "query": f"SELECT pName, costStd, costAvg FROM {v} WHERE costAvg > costStd", "difficulty": "medium"})
    result.append({"question": "Show products where member price is lower than standard price.", "query": f"SELECT pName, priceStd, priceMem FROM {v} WHERE priceMem < priceStd AND priceMem > 0", "difficulty": "medium"})

    # TOP N
    for n in [5, 10, 20]:
        result.append({"question": f"Show the top {n} most expensive products.", "query": f"SELECT TOP {n} pName, priceStd FROM {v} ORDER BY priceStd DESC", "difficulty": "medium"})
        result.append({"question": f"Show the top {n} products with highest inventory.", "query": f"SELECT TOP {n} pName, qtyNow FROM {v} ORDER BY qtyNow DESC", "difficulty": "medium"})

    # HAVING
    for cnt in [5, 10, 20]:
        result.append({"question": f"Which suppliers have more than {cnt} products?", "query": f"SELECT pvName, COUNT(*) FROM {v} GROUP BY pvName HAVING COUNT(*) > {cnt}", "difficulty": "hard"})

    # pvDiscount
    result.append({"question": "Show products with supplier discount.", "query": f"SELECT pName, pvName, pvDiscount FROM {v} WHERE isPvDiscount='Y'", "difficulty": "easy"})
    result.append({"question": "Show products with supplier discount greater than 0.", "query": f"SELECT pName, pvName, pvDiscount FROM {v} WHERE pvDiscount > 0", "difficulty": "easy"})

    # Specific columns
    result.append({"question": "List all product names and their barcodes.", "query": f"SELECT pName, pBarcode FROM {v}", "difficulty": "easy"})
    result.append({"question": "List product number, name, and standard price for all products.", "query": f"SELECT pNo, pName, priceStd FROM {v}", "difficulty": "easy"})

    return result


# ============================================================
# Generator: WP_vProvider
# ============================================================
def gen_provider(samples):
    result = []
    v = f"{PREFIX}WP_vProvider"
    pvnames = samples.get("prov_pvNames", [])
    cities = samples.get("pvCities", [])
    zones = samples.get("pvZones", [])
    bosses = samples.get("pvBosses", [])
    knames = samples.get("pvKNames", [])
    pvids = samples.get("prov_pvIds", [])

    # Basic
    for lv in LIST_V:
        result.append({"question": f"{lv} all active suppliers.", "query": f"SELECT * FROM {v} WHERE isStop='N'", "difficulty": "easy"})
    for cv in COUNT_V:
        result.append({"question": f"{cv} active suppliers?", "query": f"SELECT COUNT(*) FROM {v} WHERE isStop='N'", "difficulty": "easy"})

    result.append({"question": "Show all stopped suppliers.", "query": f"SELECT * FROM {v} WHERE isStop='Y'", "difficulty": "easy"})
    result.append({"question": "How many suppliers are stopped?", "query": f"SELECT COUNT(*) FROM {v} WHERE isStop='Y'", "difficulty": "easy"})

    # By name
    for pvn in random.sample(pvnames, min(40, len(pvnames))):
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} supplier N'{pvn}'.", "query": f"SELECT * FROM {v} WHERE pvName=N'{pvn}'", "difficulty": "easy"})

    # By pvId (important: SELECT pvId not pvSn)
    for pid in random.sample(pvids, min(25, len(pvids))):
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} supplier with ID '{pid}'.", "query": f"SELECT * FROM {v} WHERE pvId='{pid}'", "difficulty": "easy"})

    # By city
    for city in random.sample(cities, min(15, len(cities))):
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} all suppliers in city N'{city}'.", "query": f"SELECT * FROM {v} WHERE pvCity=N'{city}' AND isStop='N'", "difficulty": "easy"})
        cv = random.choice(COUNT_V)
        result.append({"question": f"{cv} active suppliers in city N'{city}'?", "query": f"SELECT COUNT(*) FROM {v} WHERE pvCity=N'{city}' AND isStop='N'", "difficulty": "easy"})

    # By zone
    for zone in random.sample(zones, min(15, len(zones))):
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} all suppliers in zone N'{zone}'.", "query": f"SELECT * FROM {v} WHERE pvZone=N'{zone}' AND isStop='N'", "difficulty": "easy"})

    # By category
    for kn in random.sample(knames, min(8, len(knames))):
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} all suppliers in category N'{kn}'.", "query": f"SELECT * FROM {v} WHERE pvKName=N'{kn}' AND isStop='N'", "difficulty": "easy"})

    # GROUP BY city
    result.append({"question": "Show supplier count by city.", "query": f"SELECT pvCity, COUNT(*) FROM {v} WHERE isStop='N' GROUP BY pvCity", "difficulty": "medium"})
    result.append({"question": "Show supplier count by zone.", "query": f"SELECT pvZone, COUNT(*) FROM {v} WHERE isStop='N' GROUP BY pvZone", "difficulty": "medium"})
    result.append({"question": "Show supplier count by category.", "query": f"SELECT pvKName, COUNT(*) FROM {v} WHERE isStop='N' GROUP BY pvKName", "difficulty": "medium"})

    # pvDiscount
    result.append({"question": "Show suppliers with discount greater than 0.", "query": f"SELECT pvId, pvName, pvDiscount FROM {v} WHERE pvDiscount > 0 AND isStop='N'", "difficulty": "easy"})
    for av in AVG_V[:3]:
        result.append({"question": f"{av} discount of active suppliers?", "query": f"SELECT AVG(pvDiscount) FROM {v} WHERE isStop='N'", "difficulty": "easy"})

    # Contact info
    for pvn in random.sample(pvnames, min(20, len(pvnames))):
        result.append({"question": f"What is the phone number of supplier N'{pvn}'?", "query": f"SELECT pvTel FROM {v} WHERE pvName=N'{pvn}'", "difficulty": "easy"})
        result.append({"question": f"What is the address of supplier N'{pvn}'?", "query": f"SELECT pvAddr FROM {v} WHERE pvName=N'{pvn}'", "difficulty": "easy"})
        result.append({"question": f"Who is the boss of supplier N'{pvn}'?", "query": f"SELECT pvBoss FROM {v} WHERE pvName=N'{pvn}'", "difficulty": "easy"})

    # LIKE
    for pvn in random.sample(pvnames, min(20, len(pvnames))):
        kw = pvn[:2] if len(pvn) >= 2 else pvn
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} suppliers with name containing N'{kw}'.", "query": f"SELECT * FROM {v} WHERE pvName LIKE N'%{kw}%' AND isStop='N'", "difficulty": "easy"})

    # Specific SELECT pvId
    result.append({"question": "List all active supplier IDs and names.", "query": f"SELECT pvId, pvName FROM {v} WHERE isStop='N'", "difficulty": "easy"})
    result.append({"question": "List supplier IDs, names, and cities.", "query": f"SELECT pvId, pvName, pvCity FROM {v} WHERE isStop='N'", "difficulty": "easy"})

    # Bank info
    for pvn in random.sample(pvnames, min(10, len(pvnames))):
        result.append({"question": f"What is the bank account info for supplier N'{pvn}'?", "query": f"SELECT bankName, bankAccount, bankAcctName FROM {v} WHERE pvName=N'{pvn}'", "difficulty": "easy"})

    # HAVING
    for cnt in [3, 5, 10]:
        result.append({"question": f"Which cities have more than {cnt} active suppliers?", "query": f"SELECT pvCity, COUNT(*) FROM {v} WHERE isStop='N' GROUP BY pvCity HAVING COUNT(*) > {cnt}", "difficulty": "hard"})

    # Combined city+zone
    for city in random.sample(cities, min(5, len(cities))):
        for zone in random.sample(zones, min(3, len(zones))):
            lv = random.choice(LIST_V)
            result.append({"question": f"{lv} suppliers in N'{city}' N'{zone}'.", "query": f"SELECT * FROM {v} WHERE pvCity=N'{city}' AND pvZone=N'{zone}' AND isStop='N'", "difficulty": "hard"})

    return result


# ============================================================
# Generator: WP_vMemberDeposit
# ============================================================
def gen_member_deposit(samples):
    result = []
    v = f"{PREFIX}WP_vMemberDeposit"
    delf = "isDel='N'"
    memnames = samples.get("dep_memNames", [])
    memids = samples.get("dep_memIds", [])
    empids = samples.get("dep_empIds", [])
    dep_osids = samples.get("dep_outStkIds", [])

    # Basic
    for lv in LIST_V:
        result.append({"question": f"{lv} all active member deposits.", "query": f"SELECT * FROM {v} WHERE {delf}", "difficulty": "easy"})
    for cv in COUNT_V:
        result.append({"question": f"{cv} active member deposit records?", "query": f"SELECT COUNT(*) FROM {v} WHERE {delf}", "difficulty": "easy"})

    # By member name
    for mn in random.sample(memnames, min(40, len(memnames))):
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} deposits for member N'{mn}'.", "query": f"SELECT * FROM {v} WHERE memName=N'{mn}' AND {delf}", "difficulty": "easy"})
        cv = random.choice(COUNT_V)
        result.append({"question": f"{cv} deposit records for member N'{mn}'?", "query": f"SELECT COUNT(*) FROM {v} WHERE memName=N'{mn}' AND {delf}", "difficulty": "easy"})

    # By member ID
    for mid in random.sample(memids, min(30, len(memids))):
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} deposits for member ID '{mid}'.", "query": f"SELECT * FROM {v} WHERE memId='{mid}' AND {delf}", "difficulty": "easy"})

    # isStop filter
    result.append({"question": "Show deposits for active members.", "query": f"SELECT * FROM {v} WHERE isStop='N' AND {delf}", "difficulty": "easy"})
    result.append({"question": "Show deposits for stopped members.", "query": f"SELECT * FROM {v} WHERE isStop='Y' AND {delf}", "difficulty": "easy"})
    result.append({"question": "How many active members have deposits?", "query": f"SELECT COUNT(DISTINCT memId) FROM {v} WHERE isStop='N' AND {delf}", "difficulty": "easy"})
    result.append({"question": "How many stopped members have deposits?", "query": f"SELECT COUNT(DISTINCT memId) FROM {v} WHERE isStop='Y' AND {delf}", "difficulty": "easy"})

    # Amount aggregation (no header-detail, direct SUM OK)
    for sv in SUM_V:
        result.append({"question": f"{sv} deposit amount for all active records?", "query": f"SELECT SUM(amount) FROM {v} WHERE {delf}", "difficulty": "easy"})
    for av in AVG_V:
        result.append({"question": f"{av} deposit amount?", "query": f"SELECT AVG(amount) FROM {v} WHERE {delf}", "difficulty": "easy"})
    for mv in MAX_V:
        result.append({"question": f"{mv} deposit amount?", "query": f"SELECT MAX(amount) FROM {v} WHERE {delf}", "difficulty": "easy"})
    for mv in MIN_V:
        result.append({"question": f"{mv} deposit amount?", "query": f"SELECT MIN(amount) FROM {v} WHERE {delf}", "difficulty": "easy"})

    # Amount by member
    for mn in random.sample(memnames, min(30, len(memnames))):
        sv = random.choice(SUM_V)
        result.append({"question": f"{sv} deposit amount for member N'{mn}'?", "query": f"SELECT SUM(amount) FROM {v} WHERE memName=N'{mn}' AND {delf}", "difficulty": "medium"})

    # GROUP BY member
    result.append({"question": "Show total deposit amount per member.", "query": f"SELECT memName, SUM(amount) FROM {v} WHERE {delf} GROUP BY memName", "difficulty": "medium"})
    result.append({"question": "Show deposit count per member.", "query": f"SELECT memName, COUNT(*) FROM {v} WHERE {delf} GROUP BY memName", "difficulty": "medium"})
    result.append({"question": "Show total deposit amount per member for active members.", "query": f"SELECT memName, SUM(amount) FROM {v} WHERE isStop='N' AND {delf} GROUP BY memName", "difficulty": "medium"})

    # endDate queries
    result.append({"question": "Show deposits expiring before 2026-12-31.", "query": f"SELECT * FROM {v} WHERE endDate < '2026-12-31' AND {delf}", "difficulty": "medium"})
    result.append({"question": "Show deposits expiring after 2027-01-01.", "query": f"SELECT * FROM {v} WHERE endDate > '2027-01-01' AND {delf}", "difficulty": "medium"})
    result.append({"question": "Show deposits that have already expired.", "query": f"SELECT * FROM {v} WHERE endDate < GETDATE() AND {delf}", "difficulty": "medium"})
    result.append({"question": "Show deposits that are still valid (not expired).", "query": f"SELECT * FROM {v} WHERE endDate >= GETDATE() AND {delf}", "difficulty": "medium"})

    # endDate by member
    for mn in random.sample(memnames, min(20, len(memnames))):
        result.append({"question": f"When do member N'{mn}''s deposits expire?", "query": f"SELECT memName, amount, endDate FROM {v} WHERE memName=N'{mn}' AND {delf}", "difficulty": "easy"})

    # HAVING
    for amt in [10000, 50000, 100000]:
        result.append({"question": f"Which members have total deposits exceeding {amt}?", "query": f"SELECT memName, SUM(amount) FROM {v} WHERE {delf} GROUP BY memName HAVING SUM(amount) > {amt}", "difficulty": "hard"})

    # TOP N
    for n in [5, 10, 20]:
        result.append({"question": f"Show top {n} members by deposit amount.", "query": f"SELECT TOP {n} memName, SUM(amount) AS total FROM {v} WHERE {delf} GROUP BY memName ORDER BY total DESC", "difficulty": "medium"})

    # Amount ranges
    for lo, hi in [(0, 10000), (10000, 50000), (50000, 100000), (100000, 200000)]:
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} deposits with amount between {lo} and {hi}.", "query": f"SELECT * FROM {v} WHERE amount BETWEEN {lo} AND {hi} AND {delf}", "difficulty": "easy"})

    # OutStkId date filter (when not empty)
    if dep_osids:
        for oid in random.sample(dep_osids, min(20, len(dep_osids))):
            if len(oid) >= 8:
                d = oid[:8]
                ds = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
                lv = random.choice(LIST_V)
                result.append({"question": f"{lv} deposit deductions made on {ds}.", "query": f"SELECT * FROM {v} WHERE LEFT(OutStkId,8)='{d}' AND {delf}", "difficulty": "medium"})

    # Deposits used for sales (OutStkId not empty)
    result.append({"question": "Show deposits that have been used for sales deduction.", "query": f"SELECT * FROM {v} WHERE OutStkId != '' AND {delf}", "difficulty": "medium"})
    result.append({"question": "Show deposits that have not been used for any sale.", "query": f"SELECT * FROM {v} WHERE OutStkId = '' AND {delf}", "difficulty": "medium"})
    result.append({"question": "How many deposit records have been used for sales?", "query": f"SELECT COUNT(*) FROM {v} WHERE OutStkId != '' AND {delf}", "difficulty": "medium"})

    # By employee
    for eid in random.sample(empids, min(8, len(empids))):
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} deposits handled by employee '{eid}'.", "query": f"SELECT * FROM {v} WHERE empId='{eid}' AND {delf}", "difficulty": "easy"})

    # DISTINCT members
    result.append({"question": "List all distinct member names with deposits.", "query": f"SELECT DISTINCT memName FROM {v} WHERE {delf}", "difficulty": "easy"})
    result.append({"question": "List all distinct member IDs with deposits.", "query": f"SELECT DISTINCT memId FROM {v} WHERE {delf}", "difficulty": "easy"})

    # Combined conditions
    for mn in random.sample(memnames, min(10, len(memnames))):
        result.append({"question": f"Show active deposits for member N'{mn}' that have not expired.", "query": f"SELECT * FROM {v} WHERE memName=N'{mn}' AND endDate >= GETDATE() AND {delf}", "difficulty": "hard"})
        result.append({"question": f"What is the total deposit amount for active member N'{mn}'?", "query": f"SELECT SUM(amount) FROM {v} WHERE memName=N'{mn}' AND isStop='N' AND {delf}", "difficulty": "hard"})

    return result


# ============================================================
# Generator: WP_vPdCombine
# ============================================================
def gen_pd_combine(samples):
    result = []
    v = f"{PREFIX}WP_vPdCombine"
    delf = "isDel='N'"
    combo_pnames = samples.get("combo_pNames", [])
    combo_spnames = samples.get("combo_sPNames", [])
    combo_pnos = samples.get("combo_pNos", [])
    combo_pnoss = samples.get("combo_pNoSs", [])
    combo_rows = samples.get("combo_rows", [])

    # Basic
    for lv in LIST_V:
        result.append({"question": f"{lv} all active combined products.", "query": f"SELECT * FROM {v} WHERE {delf}", "difficulty": "easy"})
    for cv in COUNT_V:
        result.append({"question": f"{cv} combined product records?", "query": f"SELECT COUNT(*) FROM {v} WHERE {delf}", "difficulty": "easy"})

    # DISTINCT combo products
    result.append({"question": "List all distinct combo product names.", "query": f"SELECT DISTINCT pName FROM {v} WHERE {delf}", "difficulty": "easy"})
    result.append({"question": "List all distinct combo product numbers.", "query": f"SELECT DISTINCT pNo FROM {v} WHERE {delf}", "difficulty": "easy"})
    result.append({"question": "List all distinct sub-product names.", "query": f"SELECT DISTINCT sPName FROM {v} WHERE {delf}", "difficulty": "easy"})
    result.append({"question": "List all distinct sub-product numbers.", "query": f"SELECT DISTINCT pNoS FROM {v} WHERE {delf}", "difficulty": "easy"})
    result.append({"question": "How many distinct combo products are there?", "query": f"SELECT COUNT(DISTINCT pNo) FROM {v} WHERE {delf}", "difficulty": "easy"})
    result.append({"question": "How many distinct sub-products are used in combos?", "query": f"SELECT COUNT(DISTINCT pNoS) FROM {v} WHERE {delf}", "difficulty": "easy"})

    # By combo product name
    for cpn in combo_pnames:
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} all sub-products in combo N'{cpn}'.", "query": f"SELECT pNoS, sPName, pQty FROM {v} WHERE pName=N'{cpn}' AND {delf}", "difficulty": "easy"})
        result.append({"question": f"What sub-products make up combo N'{cpn}'?", "query": f"SELECT sPName, pQty FROM {v} WHERE pName=N'{cpn}' AND {delf}", "difficulty": "easy"})
        result.append({"question": f"How many sub-product types are in combo N'{cpn}'?", "query": f"SELECT COUNT(*) FROM {v} WHERE pName=N'{cpn}' AND {delf}", "difficulty": "easy"})

    # By combo product number
    for cpno in combo_pnos:
        lv = random.choice(LIST_V)
        result.append({"question": f"{lv} sub-products for combo product number {cpno}.", "query": f"SELECT pNoS, sPName, pQty FROM {v} WHERE pNo={cpno} AND {delf}", "difficulty": "easy"})
        result.append({"question": f"What is the name of combo product number {cpno}?", "query": f"SELECT DISTINCT pName FROM {v} WHERE pNo={cpno} AND {delf}", "difficulty": "easy"})

    # By sub-product name (reverse lookup)
    for spn in combo_spnames:
        lv = random.choice(LIST_V)
        result.append({"question": f"Which combo products contain sub-product N'{spn}'?", "query": f"SELECT DISTINCT pNo, pName FROM {v} WHERE sPName=N'{spn}' AND {delf}", "difficulty": "easy"})
        result.append({"question": f"How many combo products include sub-product N'{spn}'?", "query": f"SELECT COUNT(DISTINCT pNo) FROM {v} WHERE sPName=N'{spn}' AND {delf}", "difficulty": "easy"})
        result.append({"question": f"What quantity of N'{spn}' is needed per combo?", "query": f"SELECT pName, pQty FROM {v} WHERE sPName=N'{spn}' AND {delf}", "difficulty": "easy"})

    # By sub-product number
    for spno in combo_pnoss:
        lv = random.choice(LIST_V)
        result.append({"question": f"Which combos contain sub-product number {spno}?", "query": f"SELECT DISTINCT pNo, pName FROM {v} WHERE pNoS={spno} AND {delf}", "difficulty": "easy"})

    # Cost calculations
    result.append({"question": "Show the total sub-product cost for each combo.", "query": f"SELECT pName, SUM(sCostStd * pQty) AS total_cost FROM {v} WHERE {delf} GROUP BY pName", "difficulty": "medium"})
    result.append({"question": "Show the total sub-product cost for each combo product number.", "query": f"SELECT pNo, SUM(sCostStd * pQty) AS total_cost FROM {v} WHERE {delf} GROUP BY pNo", "difficulty": "medium"})

    for cpn in combo_pnames:
        result.append({"question": f"What is the total cost of combo N'{cpn}'?", "query": f"SELECT SUM(sCostStd * pQty) FROM {v} WHERE pName=N'{cpn}' AND {delf}", "difficulty": "medium"})
        result.append({"question": f"What is the standard price vs total cost for combo N'{cpn}'?", "query": f"SELECT DISTINCT priceStd, (SELECT SUM(sCostStd * pQty) FROM {v} t2 WHERE t2.pName=t1.pName AND t2.isDel='N') AS total_cost FROM {v} t1 WHERE pName=N'{cpn}' AND {delf}", "difficulty": "hard"})

    # Price queries
    result.append({"question": "Show standard price of each combo product.", "query": f"SELECT DISTINCT pNo, pName, priceStd FROM {v} WHERE {delf}", "difficulty": "easy"})
    result.append({"question": "Show standard price of each sub-product.", "query": f"SELECT DISTINCT pNoS, sPName, sPriceStd FROM {v} WHERE {delf}", "difficulty": "easy"})

    # Quantity queries
    result.append({"question": "Show total sub-product quantity per combo.", "query": f"SELECT pName, SUM(pQty) FROM {v} WHERE {delf} GROUP BY pName", "difficulty": "medium"})
    for cpn in combo_pnames:
        result.append({"question": f"What is the total quantity of sub-products in combo N'{cpn}'?", "query": f"SELECT SUM(pQty) FROM {v} WHERE pName=N'{cpn}' AND {delf}", "difficulty": "easy"})

    # isUpdStock
    result.append({"question": "Show combo products that update stock.", "query": f"SELECT * FROM {v} WHERE isUpdStock='Y' AND {delf}", "difficulty": "easy"})
    result.append({"question": "Show combo products that do not update stock.", "query": f"SELECT * FROM {v} WHERE isUpdStock='N' AND {delf}", "difficulty": "easy"})

    # pBarcode queries
    for row in combo_rows:
        if row.get("pName"):
            result.append({"question": f"What is the barcode of combo product N'{row['pName']}'?", "query": f"SELECT DISTINCT pBarcode FROM {v} WHERE pName=N'{row['pName']}' AND {delf}", "difficulty": "easy"})

    # Tax info
    result.append({"question": "Show sub-products that are taxable.", "query": f"SELECT DISTINCT pNoS, sPName FROM {v} WHERE sIsTax='Y' AND {delf}", "difficulty": "easy"})
    result.append({"question": "Show sub-products that are not taxable.", "query": f"SELECT DISTINCT pNoS, sPName FROM {v} WHERE sIsTax='N' AND {delf}", "difficulty": "easy"})

    # MAX/MIN cost
    for mv in MAX_V[:3]:
        result.append({"question": f"{mv} sub-product cost in combined products?", "query": f"SELECT MAX(sCostStd) FROM {v} WHERE {delf}", "difficulty": "easy"})
    for mv in MIN_V[:3]:
        result.append({"question": f"{mv} sub-product cost in combined products?", "query": f"SELECT MIN(sCostStd) FROM {v} WHERE {delf} AND sCostStd > 0", "difficulty": "easy"})

    # Additional phrasing variants for diversity
    for cpn in combo_pnames:
        result.append({"question": f"Break down the contents of combo product N'{cpn}'.", "query": f"SELECT sPName, pQty, sCostStd FROM {v} WHERE pName=N'{cpn}' AND {delf}", "difficulty": "easy"})
        result.append({"question": f"What are the individual items in N'{cpn}'?", "query": f"SELECT pNoS, sPName, pQty FROM {v} WHERE pName=N'{cpn}' AND {delf}", "difficulty": "easy"})

    for spn in combo_spnames:
        result.append({"question": f"Is N'{spn}' part of any combo product?", "query": f"SELECT DISTINCT pName FROM {v} WHERE sPName=N'{spn}' AND {delf}", "difficulty": "easy"})

    return result


# ============================================================
# Main
# ============================================================
def main():
    print("Connecting to DB...")
    conn = pyodbc.connect(DB_CONN_STR, timeout=30)
    print("Connected.\n")

    print("Sampling data from DB...")
    samples = sample_db_values(conn)
    print(f"  Date prefixes: {len(samples.get('date_prefixes', []))}")
    print(f"  Year-month prefixes: {len(samples.get('year_month_prefixes', []))}")
    print(f"  Combo products: {len(samples.get('combo_pNames', []))}")
    print()

    print("Generating samples...")
    all_samples = generate_all_samples(samples)
    print(f"  Raw generated: {len(all_samples)}")

    # Deduplicate by question text
    seen_questions = set()
    deduped = []
    for s in all_samples:
        q = s["question"].strip()
        if q not in seen_questions:
            seen_questions.add(q)
            deduped.append(s)
    print(f"  After dedup: {len(deduped)}")

    # Add source tag
    for s in deduped:
        s["source"] = "gen_9views_20k"

    # Shuffle
    random.shuffle(deduped)

    # Stats
    view_counts = defaultdict(int)
    diff_counts = defaultdict(int)
    for s in deduped:
        # Detect view from SQL
        for vn in VIEW_CFG.keys():
            if vn in s["query"]:
                view_counts[vn] += 1
                break
        diff_counts[s.get("difficulty", "unknown")] += 1

    print(f"\n  Per-view distribution:")
    for vn in sorted(view_counts.keys()):
        print(f"    {vn}: {view_counts[vn]}")
    print(f"\n  Per-difficulty distribution:")
    for d in ["easy", "medium", "hard"]:
        print(f"    {d}: {diff_counts.get(d, 0)}")

    # Save
    os.makedirs(os.path.dirname(OUTPUT_PATH) or ".", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(deduped, f, indent=2, ensure_ascii=False)
    print(f"\n  Saved: {OUTPUT_PATH} ({len(deduped)} samples)")

    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()
