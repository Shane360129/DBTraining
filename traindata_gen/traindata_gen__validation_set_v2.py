#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
traindata_gen__validation_set_v2.py
Generate 240 unique English Spider-format validation samples for WP_M09.

Connects to SHANE\\SQLEXPRESS / WP_M09 to fetch real data values,
then generates balanced questions across all 7 views.

Output: data/wp_m09/val_claude_en_spider_v2.json
"""

import json
import re
import random
import pyodbc
from pathlib import Path

random.seed(2026)

DB = "WP_M09"
PRE = f"{DB}.dbo."
OUTPUT_PATH = Path(__file__).parent / "data" / "wp_m09" / "val_claude_en_spider_v2.json"

CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=SHANE\SQLEXPRESS;"
    f"DATABASE={DB};"
    r"Trusted_Connection=yes;"
)

SQL_PLACEHOLDER = {
    "select": [False, []], "from": {"table_units": [], "conds": []},
    "where": [], "groupBy": [], "having": [], "orderBy": [],
    "limit": None, "intersect": None, "union": None, "except": None
}

# ─── Tokenization ────────────────────────────────────────────────────────

def tokenize_sql(sql: str) -> list:
    tokens = []
    i = 0
    s = sql.strip()
    while i < len(s):
        if s[i].isspace():
            i += 1
            continue
        if s[i] == 'N' and i + 1 < len(s) and s[i + 1] == "'":
            j = i + 2
            while j < len(s):
                if s[j] == "'" and (j + 1 >= len(s) or s[j + 1] != "'"):
                    break
                if s[j] == "'" and j + 1 < len(s) and s[j + 1] == "'":
                    j += 2
                    continue
                j += 1
            tokens.append(s[i:j + 1])
            i = j + 1
            continue
        if s[i] == "'":
            j = i + 1
            while j < len(s):
                if s[j] == "'" and (j + 1 >= len(s) or s[j + 1] != "'"):
                    break
                if s[j] == "'" and j + 1 < len(s) and s[j + 1] == "'":
                    j += 2
                    continue
                j += 1
            tokens.append(s[i:j + 1])
            i = j + 1
            continue
        if s[i] in '(),.;=<>!*':
            if s[i] in '<>!' and i + 1 < len(s) and s[i + 1] == '=':
                tokens.append(s[i:i + 2])
                i += 2
            else:
                tokens.append(s[i])
                i += 1
            continue
        j = i
        while j < len(s) and not s[j].isspace() and s[j] not in "(),.;=<>!'*":
            j += 1
        if j > i:
            tokens.append(s[i:j])
            i = j
        else:
            i += 1
    return tokens


def no_value_toks(toks: list) -> list:
    result = []
    for t in toks:
        if re.match(r"^N?'.*'$", t):
            result.append("'value'")
        elif re.match(r"^-?\d+(\.\d+)?$", t):
            result.append("value")
        else:
            result.append(t)
    return result


def make_entry(question, sql, difficulty, view):
    t = tokenize_sql(sql)
    return {
        "db_id": DB,
        "query": sql,
        "query_toks": t,
        "query_toks_no_value": no_value_toks(t),
        "question": question,
        "question_toks": question.split(),
        "sql": SQL_PLACEHOLDER,
        "difficulty": difficulty,
        "view": view,
    }


# ─── Database helpers ────────────────────────────────────────────────────

def fetch_values(conn):
    """Fetch real sample values from all 7 views."""
    cur = conn.cursor()
    data = {}

    def q(sql):
        cur.execute(sql)
        return [row[0] for row in cur.fetchall() if row[0] is not None]

    def q2(sql):
        cur.execute(sql)
        return [(row[0], row[1]) for row in cur.fetchall() if row[0] is not None]

    # WP_vAcctIn
    data['ai_ids'] = q("SELECT DISTINCT TOP 15 acctInId FROM WP_vAcctIn WHERE isDel='N' ORDER BY acctInId DESC")
    data['ai_months'] = q("SELECT DISTINCT LEFT(acctInId,6) FROM WP_vAcctIn WHERE isDel='N'")
    data['ai_dates'] = q("SELECT DISTINCT LEFT(acctInId,8) FROM WP_vAcctIn WHERE isDel='N'")
    data['ai_members'] = q2("SELECT DISTINCT TOP 10 memId, memName FROM WP_vAcctIn WHERE isDel='N' AND memId IS NOT NULL")
    data['ai_emp_ids'] = q("SELECT DISTINCT empId FROM WP_vAcctIn WHERE isDel='N' AND empId IS NOT NULL")

    # WP_vAcctOut
    data['ao_ids'] = q("SELECT DISTINCT TOP 15 acctOutId FROM WP_vAcctOut WHERE isDel='N' ORDER BY acctOutId DESC")
    data['ao_months'] = q("SELECT DISTINCT LEFT(acctOutId,6) FROM WP_vAcctOut WHERE isDel='N'")
    data['ao_dates'] = q("SELECT DISTINCT TOP 15 LEFT(acctOutId,8) FROM WP_vAcctOut WHERE isDel='N' ORDER BY 1 DESC")
    data['ao_vendors'] = q2("SELECT DISTINCT TOP 15 pvId, pvName FROM WP_vAcctOut WHERE isDel='N' AND pvId IS NOT NULL")
    data['ao_emp_ids'] = q("SELECT DISTINCT empId FROM WP_vAcctOut WHERE isDel='N' AND empId IS NOT NULL")
    data['ao_emp_names'] = q("SELECT DISTINCT empName FROM WP_vAcctOut WHERE isDel='N' AND empName IS NOT NULL")
    data['ao_pay_types'] = q("SELECT DISTINCT payType FROM WP_vAcctOut WHERE isDel='N' AND payType IS NOT NULL")

    # WP_vOutStock
    data['os_ids'] = q("SELECT DISTINCT TOP 15 OutStkId FROM WP_vOutStock WHERE isDel='N' ORDER BY OutStkId DESC")
    data['os_months'] = q("SELECT DISTINCT LEFT(OutStkId,6) FROM WP_vOutStock WHERE isDel='N'")
    data['os_dates'] = q("SELECT DISTINCT TOP 15 LEFT(OutStkId,8) FROM WP_vOutStock WHERE isDel='N' ORDER BY 1 DESC")
    data['os_members'] = q2("SELECT DISTINCT TOP 10 memId, memName FROM WP_vOutStock WHERE isDel='N' AND memId IS NOT NULL")
    data['os_emp_names'] = q("SELECT DISTINCT empName FROM WP_vOutStock WHERE isDel='N' AND empName IS NOT NULL")
    data['os_out_types'] = q("SELECT DISTINCT outType FROM WP_vOutStock WHERE isDel='N' AND outType IS NOT NULL")

    # WP_vTransfer
    data['tr_ids'] = q("SELECT DISTINCT TOP 15 TransferId FROM WP_vTransfer WHERE isDel='N' ORDER BY TransferId DESC")
    data['tr_months'] = q("SELECT DISTINCT LEFT(TransferId,6) FROM WP_vTransfer WHERE isDel='N'")
    data['tr_dates'] = q("SELECT DISTINCT TOP 10 LEFT(TransferId,8) FROM WP_vTransfer WHERE isDel='N' ORDER BY 1 DESC")
    data['tr_warehouses'] = q2("SELECT DISTINCT TOP 10 FromWhSn, fWhName FROM WP_vTransfer WHERE isDel='N' AND fWhName IS NOT NULL")
    data['tr_to_warehouses'] = q2("SELECT DISTINCT TOP 10 ToWhSn, tfWhName FROM WP_vTransfer WHERE isDel='N' AND tfWhName IS NOT NULL")
    data['tr_emp_ids'] = q("SELECT DISTINCT empId FROM WP_vTransfer WHERE isDel='N' AND empId IS NOT NULL")

    # WP_vInventory
    data['inv_warehouses'] = q2("SELECT DISTINCT TOP 10 WarehouseId, WarehouseName FROM WP_vInventory")
    data['inv_pnos'] = q("SELECT DISTINCT TOP 15 pNo FROM WP_vInventory ORDER BY pNo DESC")
    data['inv_pnames'] = q("SELECT DISTINCT TOP 20 pName FROM WP_vInventory WHERE pName IS NOT NULL")
    data['inv_pvnames'] = q("SELECT DISTINCT TOP 15 pvName FROM WP_vInventory WHERE pvName IS NOT NULL")

    # WP_vProduct
    data['pr_pnos'] = q("SELECT DISTINCT TOP 15 pNo FROM WP_vProduct ORDER BY pNo DESC")
    data['pr_pnames'] = q("SELECT DISTINCT TOP 25 pName FROM WP_vProduct WHERE pName IS NOT NULL")
    data['pr_pvnames'] = q("SELECT DISTINCT TOP 15 pvName FROM WP_vProduct WHERE pvName IS NOT NULL")
    data['pr_pvids'] = q("SELECT DISTINCT TOP 15 pvId FROM WP_vProduct WHERE pvId IS NOT NULL")
    data['pr_barcodes'] = q("SELECT DISTINCT TOP 10 pBarcode FROM WP_vProduct WHERE pBarcode IS NOT NULL AND pBarcode <> ''")
    data['pr_punits'] = q("SELECT DISTINCT pUnit FROM WP_vProduct WHERE pUnit IS NOT NULL")
    data['pr_punames'] = q("SELECT DISTINCT pUName FROM WP_vProduct WHERE pUName IS NOT NULL")
    data['pr_sale_vals'] = q("SELECT DISTINCT isSale FROM WP_vProduct WHERE isSale IS NOT NULL")

    # WP_vProvider
    data['pv_pvids'] = q("SELECT DISTINCT TOP 20 pvId FROM WP_vProvider")
    data['pv_pvnames'] = q("SELECT DISTINCT TOP 20 pvName FROM WP_vProvider WHERE pvName IS NOT NULL")
    data['pv_cities'] = q("SELECT DISTINCT pvCity FROM WP_vProvider WHERE pvCity IS NOT NULL AND pvCity <> ''")
    data['pv_zones'] = q("SELECT DISTINCT TOP 15 pvZone FROM WP_vProvider WHERE pvZone IS NOT NULL AND pvZone <> ''")
    data['pv_knames'] = q("SELECT DISTINCT pvKName FROM WP_vProvider WHERE pvKName IS NOT NULL AND pvKName <> ''")
    data['pv_banknames'] = q("SELECT DISTINCT TOP 10 bankName FROM WP_vProvider WHERE bankName IS NOT NULL AND bankName <> ''")

    # Common product names across views
    data['common_pnames'] = q("SELECT DISTINCT TOP 30 pName FROM WP_vProduct WHERE pName IS NOT NULL")
    data['common_keywords'] = ['茶', '米', '水', '冬', '春', '香', '竹', '花', '豆', '高山', '烏龍', '金萱', '蜂蜜', '梅']

    cur.close()
    return data


# ─── Generators per view ─────────────────────────────────────────────────

V = PRE


def gen_acct_in(d):
    """WP_vAcctIn — 34 questions. Has isDel, dtlIsDel. Date: LEFT(acctInId,8/6)."""
    samples = []
    ids = d['ai_ids']
    months = d['ai_months']
    dates = d['ai_dates']
    members = d['ai_members']  # (memId, memName)
    emp_ids = d['ai_emp_ids']
    kw = d['common_keywords']

    # ── Easy (14) ──
    samples.append(make_entry(
        "List all active accounts receivable IDs.",
        f"SELECT DISTINCT acctInId FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N'",
        "easy", "WP_vAcctIn"))

    samples.append(make_entry(
        "How many active accounts receivable records are there?",
        f"SELECT COUNT(DISTINCT acctInId) FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N'",
        "easy", "WP_vAcctIn"))

    if ids:
        samples.append(make_entry(
            f"Show the details of accounts receivable ID '{ids[0]}'.",
            f"SELECT * FROM {V}WP_vAcctIn WHERE acctInId='{ids[0]}' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vAcctIn"))

    samples.append(make_entry(
        "What is the total amount of all active accounts receivable?",
        f"SELECT SUM(amount) FROM (SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE isDel='N') sub",
        "easy", "WP_vAcctIn"))

    if members:
        mid, mname = members[0]
        samples.append(make_entry(
            f"List accounts receivable for member '{mname}'.",
            f"SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE memName=N'{mname}' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vAcctIn"))

    if months:
        m = months[0]
        samples.append(make_entry(
            f"How many accounts receivable were created in month {m}?",
            f"SELECT COUNT(DISTINCT acctInId) FROM {V}WP_vAcctIn WHERE LEFT(acctInId,6)='{m}' AND isDel='N'",
            "easy", "WP_vAcctIn"))

    samples.append(make_entry(
        "List all distinct member names in accounts receivable.",
        f"SELECT DISTINCT memName FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' AND memName IS NOT NULL",
        "easy", "WP_vAcctIn"))

    if dates:
        dt = dates[0]
        samples.append(make_entry(
            f"Show accounts receivable created on date {dt}.",
            f"SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE LEFT(acctInId,8)='{dt}' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vAcctIn"))

    samples.append(make_entry(
        "What is the maximum amount in a single accounts receivable record?",
        f"SELECT MAX(amount) FROM {V}WP_vAcctIn WHERE isDel='N'",
        "easy", "WP_vAcctIn"))

    samples.append(make_entry(
        "What is the minimum amount in accounts receivable?",
        f"SELECT MIN(amount) FROM {V}WP_vAcctIn WHERE isDel='N'",
        "easy", "WP_vAcctIn"))

    if emp_ids:
        samples.append(make_entry(
            f"List accounts receivable handled by employee '{emp_ids[0]}'.",
            f"SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE empId='{emp_ids[0]}' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vAcctIn"))

    samples.append(make_entry(
        "Show all distinct employee IDs in accounts receivable.",
        f"SELECT DISTINCT empId FROM {V}WP_vAcctIn WHERE isDel='N' AND empId IS NOT NULL",
        "easy", "WP_vAcctIn"))

    if members and len(members) > 1:
        mid2, mname2 = members[1]
        samples.append(make_entry(
            f"Show the total amount for member '{mname2}' in accounts receivable.",
            f"SELECT SUM(amount) FROM (SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE memName=N'{mname2}' AND isDel='N') sub",
            "easy", "WP_vAcctIn"))

    if kw:
        samples.append(make_entry(
            f"List accounts receivable with product names containing '{kw[0]}'.",
            f"SELECT DISTINCT acctInId, pName FROM {V}WP_vAcctIn WHERE pName LIKE N'%{kw[0]}%' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vAcctIn"))

    # ── Medium (12) ──
    if months:
        m = months[0]
        samples.append(make_entry(
            f"What is the total amount of accounts receivable in month {m}?",
            f"SELECT SUM(amount) FROM (SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE LEFT(acctInId,6)='{m}' AND isDel='N') sub",
            "medium", "WP_vAcctIn"))

    samples.append(make_entry(
        "Show the count of accounts receivable grouped by member name.",
        f"SELECT memName, COUNT(DISTINCT acctInId) FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' AND memName IS NOT NULL GROUP BY memName",
        "medium", "WP_vAcctIn"))

    samples.append(make_entry(
        "List the total amount per member in accounts receivable.",
        f"SELECT memName, SUM(amt) FROM (SELECT DISTINCT acctInId, memName, amount AS amt FROM {V}WP_vAcctIn WHERE isDel='N' AND memName IS NOT NULL) sub GROUP BY memName",
        "medium", "WP_vAcctIn"))

    samples.append(make_entry(
        "Show accounts receivable sorted by amount in descending order, top 5.",
        f"SELECT DISTINCT TOP 5 acctInId, amount FROM {V}WP_vAcctIn WHERE isDel='N' ORDER BY amount DESC",
        "medium", "WP_vAcctIn"))

    samples.append(make_entry(
        "Count the number of distinct products in accounts receivable.",
        f"SELECT COUNT(DISTINCT pNo) FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N'",
        "medium", "WP_vAcctIn"))

    if months and len(months) > 1:
        m1, m2 = months[0], months[-1]
        samples.append(make_entry(
            f"List accounts receivable created between months {m2} and {m1}.",
            f"SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE LEFT(acctInId,6) BETWEEN '{m2}' AND '{m1}' AND isDel='N' AND dtlIsDel='N'",
            "medium", "WP_vAcctIn"))

    samples.append(make_entry(
        "Show the total quantity per product in accounts receivable.",
        f"SELECT pName, SUM(oStkDtlQty) FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName",
        "medium", "WP_vAcctIn"))

    samples.append(make_entry(
        "Count accounts receivable per month.",
        f"SELECT LEFT(acctInId,6), COUNT(DISTINCT acctInId) FROM {V}WP_vAcctIn WHERE isDel='N' GROUP BY LEFT(acctInId,6)",
        "medium", "WP_vAcctIn"))

    samples.append(make_entry(
        "What is the average amount of accounts receivable?",
        f"SELECT AVG(amount) FROM (SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE isDel='N') sub",
        "medium", "WP_vAcctIn"))

    if ids and len(ids) > 1:
        samples.append(make_entry(
            f"Show the product details for accounts receivable '{ids[1]}'.",
            f"SELECT pNo, pName, oStkDtlQty, oStkDtlAmtTotal FROM {V}WP_vAcctIn WHERE acctInId='{ids[1]}' AND isDel='N' AND dtlIsDel='N'",
            "medium", "WP_vAcctIn"))

    samples.append(make_entry(
        "List distinct member IDs and names in accounts receivable.",
        f"SELECT DISTINCT memId, memName FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' AND memId IS NOT NULL",
        "medium", "WP_vAcctIn"))

    samples.append(make_entry(
        "Show accounts receivable with amount greater than 1000.",
        f"SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE isDel='N' AND amount > 1000",
        "medium", "WP_vAcctIn"))

    # ── Hard (8) ──
    samples.append(make_entry(
        "Which member has the highest total accounts receivable amount?",
        f"SELECT TOP 1 memName, SUM(amt) AS total FROM (SELECT DISTINCT acctInId, memName, amount AS amt FROM {V}WP_vAcctIn WHERE isDel='N' AND memName IS NOT NULL) sub GROUP BY memName ORDER BY total DESC",
        "hard", "WP_vAcctIn"))

    samples.append(make_entry(
        "List members who have more than 1 accounts receivable record.",
        f"SELECT memName, COUNT(DISTINCT acctInId) AS cnt FROM {V}WP_vAcctIn WHERE isDel='N' AND memName IS NOT NULL GROUP BY memName HAVING COUNT(DISTINCT acctInId) > 1",
        "hard", "WP_vAcctIn"))

    samples.append(make_entry(
        "Show accounts receivable whose amount is above the average.",
        f"SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE isDel='N' AND amount > (SELECT AVG(amount) FROM (SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE isDel='N') sub)",
        "hard", "WP_vAcctIn"))

    if members:
        mid, mname = members[0]
        samples.append(make_entry(
            f"What products did member '{mname}' purchase across all accounts receivable?",
            f"SELECT DISTINCT pNo, pName FROM {V}WP_vAcctIn WHERE memName=N'{mname}' AND isDel='N' AND dtlIsDel='N'",
            "hard", "WP_vAcctIn"))

    samples.append(make_entry(
        "Show the month with the highest total accounts receivable amount.",
        f"SELECT TOP 1 LEFT(acctInId,6) AS mon, SUM(amt) AS total FROM (SELECT DISTINCT acctInId, amount AS amt FROM {V}WP_vAcctIn WHERE isDel='N') sub GROUP BY LEFT(acctInId,6) ORDER BY total DESC",
        "hard", "WP_vAcctIn"))

    samples.append(make_entry(
        "List products that appear in more than 2 accounts receivable records.",
        f"SELECT pName, COUNT(DISTINCT acctInId) AS cnt FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' AND pName IS NOT NULL GROUP BY pName HAVING COUNT(DISTINCT acctInId) > 2",
        "hard", "WP_vAcctIn"))

    samples.append(make_entry(
        "Show the top 3 products by total quantity in accounts receivable.",
        f"SELECT TOP 3 pName, SUM(oStkDtlQty) AS totalQty FROM {V}WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY totalQty DESC",
        "hard", "WP_vAcctIn"))

    samples.append(make_entry(
        "List members whose total accounts receivable amount exceeds 5000.",
        f"SELECT memName, SUM(amt) AS total FROM (SELECT DISTINCT acctInId, memName, amount AS amt FROM {V}WP_vAcctIn WHERE isDel='N' AND memName IS NOT NULL) sub GROUP BY memName HAVING SUM(amt) > 5000",
        "hard", "WP_vAcctIn"))

    return samples


def gen_acct_out(d):
    """WP_vAcctOut — 34 questions. Has isDel, dtlIsDel. Date: LEFT(acctOutId,8/6)."""
    samples = []
    ids = d['ao_ids']
    months = d['ao_months']
    dates = d['ao_dates']
    vendors = d['ao_vendors']  # (pvId, pvName)
    emp_ids = d['ao_emp_ids']
    emp_names = d['ao_emp_names']
    pay_types = d['ao_pay_types']

    # ── Easy (14) ──
    samples.append(make_entry(
        "List all active accounts payable IDs.",
        f"SELECT DISTINCT acctOutId FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N'",
        "easy", "WP_vAcctOut"))

    samples.append(make_entry(
        "How many active accounts payable records are there?",
        f"SELECT COUNT(DISTINCT acctOutId) FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N'",
        "easy", "WP_vAcctOut"))

    if ids:
        samples.append(make_entry(
            f"Show the details of accounts payable ID '{ids[0]}'.",
            f"SELECT * FROM {V}WP_vAcctOut WHERE acctOutId='{ids[0]}' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vAcctOut"))

    samples.append(make_entry(
        "What is the total amount of all active accounts payable?",
        f"SELECT SUM(amount) FROM (SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE isDel='N') sub",
        "easy", "WP_vAcctOut"))

    if vendors:
        vid, vname = vendors[0]
        samples.append(make_entry(
            f"List accounts payable for vendor '{vname}'.",
            f"SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE pvName=N'{vname}' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vAcctOut"))

    if months:
        m = months[0]
        samples.append(make_entry(
            f"How many accounts payable were created in month {m}?",
            f"SELECT COUNT(DISTINCT acctOutId) FROM {V}WP_vAcctOut WHERE LEFT(acctOutId,6)='{m}' AND isDel='N'",
            "easy", "WP_vAcctOut"))

    samples.append(make_entry(
        "List all distinct vendor names in accounts payable.",
        f"SELECT DISTINCT pvName FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' AND pvName IS NOT NULL",
        "easy", "WP_vAcctOut"))

    if dates:
        dt = dates[0]
        samples.append(make_entry(
            f"Show accounts payable created on date {dt}.",
            f"SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE LEFT(acctOutId,8)='{dt}' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vAcctOut"))

    samples.append(make_entry(
        "What is the maximum amount in accounts payable?",
        f"SELECT MAX(amount) FROM {V}WP_vAcctOut WHERE isDel='N'",
        "easy", "WP_vAcctOut"))

    samples.append(make_entry(
        "What is the minimum accounts payable amount?",
        f"SELECT MIN(amount) FROM {V}WP_vAcctOut WHERE isDel='N'",
        "easy", "WP_vAcctOut"))

    if emp_names:
        samples.append(make_entry(
            f"List accounts payable handled by employee '{emp_names[0]}'.",
            f"SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE empName=N'{emp_names[0]}' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vAcctOut"))

    samples.append(make_entry(
        "Show all distinct employee names in accounts payable.",
        f"SELECT DISTINCT empName FROM {V}WP_vAcctOut WHERE isDel='N' AND empName IS NOT NULL",
        "easy", "WP_vAcctOut"))

    if vendors and len(vendors) > 1:
        vid2, vname2 = vendors[1]
        samples.append(make_entry(
            f"Show the total amount for vendor '{vname2}' in accounts payable.",
            f"SELECT SUM(amount) FROM (SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE pvName=N'{vname2}' AND isDel='N') sub",
            "easy", "WP_vAcctOut"))

    samples.append(make_entry(
        "Show all distinct product names in accounts payable.",
        f"SELECT DISTINCT pName FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' AND pName IS NOT NULL",
        "easy", "WP_vAcctOut"))

    # ── Medium (12) ──
    if months:
        m = months[0]
        samples.append(make_entry(
            f"What is the total amount of accounts payable in month {m}?",
            f"SELECT SUM(amount) FROM (SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE LEFT(acctOutId,6)='{m}' AND isDel='N') sub",
            "medium", "WP_vAcctOut"))

    samples.append(make_entry(
        "Show the count of accounts payable grouped by vendor.",
        f"SELECT pvName, COUNT(DISTINCT acctOutId) FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' AND pvName IS NOT NULL GROUP BY pvName",
        "medium", "WP_vAcctOut"))

    samples.append(make_entry(
        "List the total amount per vendor in accounts payable.",
        f"SELECT pvName, SUM(amt) FROM (SELECT DISTINCT acctOutId, pvName, amount AS amt FROM {V}WP_vAcctOut WHERE isDel='N' AND pvName IS NOT NULL) sub GROUP BY pvName",
        "medium", "WP_vAcctOut"))

    samples.append(make_entry(
        "Show the top 5 accounts payable by amount.",
        f"SELECT DISTINCT TOP 5 acctOutId, amount FROM {V}WP_vAcctOut WHERE isDel='N' ORDER BY amount DESC",
        "medium", "WP_vAcctOut"))

    samples.append(make_entry(
        "Count the number of distinct products in accounts payable.",
        f"SELECT COUNT(DISTINCT pNo) FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N'",
        "medium", "WP_vAcctOut"))

    if months and len(months) > 1:
        m1, m2 = sorted(months)[0], sorted(months)[-1]
        samples.append(make_entry(
            f"List accounts payable created between months {m1} and {m2}.",
            f"SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE LEFT(acctOutId,6) BETWEEN '{m1}' AND '{m2}' AND isDel='N' AND dtlIsDel='N'",
            "medium", "WP_vAcctOut"))

    samples.append(make_entry(
        "Show the total quantity per product in accounts payable.",
        f"SELECT pName, SUM(qty) FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName",
        "medium", "WP_vAcctOut"))

    samples.append(make_entry(
        "Count accounts payable per month.",
        f"SELECT LEFT(acctOutId,6), COUNT(DISTINCT acctOutId) FROM {V}WP_vAcctOut WHERE isDel='N' GROUP BY LEFT(acctOutId,6)",
        "medium", "WP_vAcctOut"))

    samples.append(make_entry(
        "What is the average amount of accounts payable?",
        f"SELECT AVG(amount) FROM (SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE isDel='N') sub",
        "medium", "WP_vAcctOut"))

    if vendors and len(vendors) > 2:
        vid3, vname3 = vendors[2]
        samples.append(make_entry(
            f"Show the product details for accounts payable to vendor '{vname3}'.",
            f"SELECT DISTINCT pNo, pName, qty, amtTotal FROM {V}WP_vAcctOut WHERE pvName=N'{vname3}' AND isDel='N' AND dtlIsDel='N'",
            "medium", "WP_vAcctOut"))

    samples.append(make_entry(
        "List distinct vendor IDs and names in accounts payable.",
        f"SELECT DISTINCT pvId, pvName FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' AND pvId IS NOT NULL",
        "medium", "WP_vAcctOut"))

    samples.append(make_entry(
        "Show accounts payable with amount greater than 5000.",
        f"SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE isDel='N' AND amount > 5000",
        "medium", "WP_vAcctOut"))

    # ── Hard (8) ──
    samples.append(make_entry(
        "Which vendor has the highest total accounts payable amount?",
        f"SELECT TOP 1 pvName, SUM(amt) AS total FROM (SELECT DISTINCT acctOutId, pvName, amount AS amt FROM {V}WP_vAcctOut WHERE isDel='N' AND pvName IS NOT NULL) sub GROUP BY pvName ORDER BY total DESC",
        "hard", "WP_vAcctOut"))

    samples.append(make_entry(
        "List vendors who have more than 3 accounts payable records.",
        f"SELECT pvName, COUNT(DISTINCT acctOutId) AS cnt FROM {V}WP_vAcctOut WHERE isDel='N' AND pvName IS NOT NULL GROUP BY pvName HAVING COUNT(DISTINCT acctOutId) > 3",
        "hard", "WP_vAcctOut"))

    samples.append(make_entry(
        "Show accounts payable whose amount is above the average.",
        f"SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE isDel='N' AND amount > (SELECT AVG(amount) FROM (SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE isDel='N') sub)",
        "hard", "WP_vAcctOut"))

    if vendors:
        vid, vname = vendors[0]
        samples.append(make_entry(
            f"What products were purchased from vendor '{vname}' in accounts payable?",
            f"SELECT DISTINCT pNo, pName FROM {V}WP_vAcctOut WHERE pvName=N'{vname}' AND isDel='N' AND dtlIsDel='N'",
            "hard", "WP_vAcctOut"))

    samples.append(make_entry(
        "Show the month with the highest total accounts payable amount.",
        f"SELECT TOP 1 LEFT(acctOutId,6) AS mon, SUM(amt) AS total FROM (SELECT DISTINCT acctOutId, amount AS amt FROM {V}WP_vAcctOut WHERE isDel='N') sub GROUP BY LEFT(acctOutId,6) ORDER BY total DESC",
        "hard", "WP_vAcctOut"))

    samples.append(make_entry(
        "List products that appear in more than 5 accounts payable records.",
        f"SELECT pName, COUNT(DISTINCT acctOutId) AS cnt FROM {V}WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' AND pName IS NOT NULL GROUP BY pName HAVING COUNT(DISTINCT acctOutId) > 5",
        "hard", "WP_vAcctOut"))

    samples.append(make_entry(
        "Show the top 3 vendors by total accounts payable amount.",
        f"SELECT TOP 3 pvName, SUM(amt) AS total FROM (SELECT DISTINCT acctOutId, pvName, amount AS amt FROM {V}WP_vAcctOut WHERE isDel='N' AND pvName IS NOT NULL) sub GROUP BY pvName ORDER BY total DESC",
        "hard", "WP_vAcctOut"))

    samples.append(make_entry(
        "List vendors whose total accounts payable amount exceeds 10000.",
        f"SELECT pvName, SUM(amt) AS total FROM (SELECT DISTINCT acctOutId, pvName, amount AS amt FROM {V}WP_vAcctOut WHERE isDel='N' AND pvName IS NOT NULL) sub GROUP BY pvName HAVING SUM(amt) > 10000",
        "hard", "WP_vAcctOut"))

    return samples


def gen_outstock(d):
    """WP_vOutStock — 34 questions. Has isDel, dtlIsDel. Date: LEFT(OutStkId,8/6)."""
    samples = []
    ids = d['os_ids']
    months = d['os_months']
    dates = d['os_dates']
    members = d['os_members']
    emp_names = d['os_emp_names']
    out_types = d['os_out_types']
    kw = d['common_keywords']

    # ── Easy (14) ──
    samples.append(make_entry(
        "List all active outbound stock IDs.",
        f"SELECT DISTINCT OutStkId FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N'",
        "easy", "WP_vOutStock"))

    samples.append(make_entry(
        "How many active outbound stock records are there?",
        f"SELECT COUNT(DISTINCT OutStkId) FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N'",
        "easy", "WP_vOutStock"))

    if ids:
        samples.append(make_entry(
            f"Show the details of outbound stock ID '{ids[0]}'.",
            f"SELECT * FROM {V}WP_vOutStock WHERE OutStkId='{ids[0]}' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vOutStock"))

    samples.append(make_entry(
        "What is the total amount of all active outbound stock?",
        f"SELECT SUM(amount) FROM (SELECT DISTINCT OutStkId, amount FROM {V}WP_vOutStock WHERE isDel='N') sub",
        "easy", "WP_vOutStock"))

    if members:
        mid, mname = members[0]
        samples.append(make_entry(
            f"List outbound stock for member '{mname}'.",
            f"SELECT DISTINCT OutStkId, amount FROM {V}WP_vOutStock WHERE memName=N'{mname}' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vOutStock"))

    if months:
        m = months[0]
        samples.append(make_entry(
            f"How many outbound stock records were created in month {m}?",
            f"SELECT COUNT(DISTINCT OutStkId) FROM {V}WP_vOutStock WHERE LEFT(OutStkId,6)='{m}' AND isDel='N'",
            "easy", "WP_vOutStock"))

    samples.append(make_entry(
        "List all distinct member names in outbound stock.",
        f"SELECT DISTINCT memName FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' AND memName IS NOT NULL",
        "easy", "WP_vOutStock"))

    if dates:
        dt = dates[0]
        samples.append(make_entry(
            f"Show outbound stock created on date {dt}.",
            f"SELECT DISTINCT OutStkId, amount FROM {V}WP_vOutStock WHERE LEFT(OutStkId,8)='{dt}' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vOutStock"))

    samples.append(make_entry(
        "What is the maximum amount in outbound stock?",
        f"SELECT MAX(amount) FROM {V}WP_vOutStock WHERE isDel='N'",
        "easy", "WP_vOutStock"))

    samples.append(make_entry(
        "What is the minimum outbound stock amount?",
        f"SELECT MIN(amount) FROM {V}WP_vOutStock WHERE isDel='N'",
        "easy", "WP_vOutStock"))

    if emp_names:
        samples.append(make_entry(
            f"List outbound stock handled by employee '{emp_names[0]}'.",
            f"SELECT DISTINCT OutStkId, amount FROM {V}WP_vOutStock WHERE empName=N'{emp_names[0]}' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vOutStock"))

    samples.append(make_entry(
        "Show all distinct employee names in outbound stock.",
        f"SELECT DISTINCT empName FROM {V}WP_vOutStock WHERE isDel='N' AND empName IS NOT NULL",
        "easy", "WP_vOutStock"))

    if members and len(members) > 1:
        mid2, mname2 = members[1]
        samples.append(make_entry(
            f"Show the total amount for member '{mname2}' in outbound stock.",
            f"SELECT SUM(amount) FROM (SELECT DISTINCT OutStkId, amount FROM {V}WP_vOutStock WHERE memName=N'{mname2}' AND isDel='N') sub",
            "easy", "WP_vOutStock"))

    if kw:
        samples.append(make_entry(
            f"List outbound stock with product names containing '{kw[1]}'.",
            f"SELECT DISTINCT OutStkId, pName FROM {V}WP_vOutStock WHERE pName LIKE N'%{kw[1]}%' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vOutStock"))

    # ── Medium (12) ──
    if months:
        m = months[0]
        samples.append(make_entry(
            f"What is the total outbound stock amount in month {m}?",
            f"SELECT SUM(amount) FROM (SELECT DISTINCT OutStkId, amount FROM {V}WP_vOutStock WHERE LEFT(OutStkId,6)='{m}' AND isDel='N') sub",
            "medium", "WP_vOutStock"))

    samples.append(make_entry(
        "Show the count of outbound stock records grouped by member.",
        f"SELECT memName, COUNT(DISTINCT OutStkId) FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' AND memName IS NOT NULL GROUP BY memName",
        "medium", "WP_vOutStock"))

    samples.append(make_entry(
        "List the total amount per member in outbound stock.",
        f"SELECT memName, SUM(amt) FROM (SELECT DISTINCT OutStkId, memName, amount AS amt FROM {V}WP_vOutStock WHERE isDel='N' AND memName IS NOT NULL) sub GROUP BY memName",
        "medium", "WP_vOutStock"))

    samples.append(make_entry(
        "Show the top 5 outbound stock records by amount.",
        f"SELECT DISTINCT TOP 5 OutStkId, amount FROM {V}WP_vOutStock WHERE isDel='N' ORDER BY amount DESC",
        "medium", "WP_vOutStock"))

    samples.append(make_entry(
        "Count the number of distinct products in outbound stock.",
        f"SELECT COUNT(DISTINCT pNo) FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N'",
        "medium", "WP_vOutStock"))

    if months and len(months) > 1:
        m1, m2 = sorted(months)[0], sorted(months)[-1]
        samples.append(make_entry(
            f"List outbound stock created between months {m1} and {m2}.",
            f"SELECT DISTINCT OutStkId, amount FROM {V}WP_vOutStock WHERE LEFT(OutStkId,6) BETWEEN '{m1}' AND '{m2}' AND isDel='N' AND dtlIsDel='N'",
            "medium", "WP_vOutStock"))

    samples.append(make_entry(
        "Show the total quantity per product in outbound stock.",
        f"SELECT pName, SUM(qty) FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName",
        "medium", "WP_vOutStock"))

    samples.append(make_entry(
        "Count outbound stock records per month.",
        f"SELECT LEFT(OutStkId,6), COUNT(DISTINCT OutStkId) FROM {V}WP_vOutStock WHERE isDel='N' GROUP BY LEFT(OutStkId,6)",
        "medium", "WP_vOutStock"))

    samples.append(make_entry(
        "What is the average outbound stock amount?",
        f"SELECT AVG(amount) FROM (SELECT DISTINCT OutStkId, amount FROM {V}WP_vOutStock WHERE isDel='N') sub",
        "medium", "WP_vOutStock"))

    if ids and len(ids) > 1:
        samples.append(make_entry(
            f"Show product details for outbound stock '{ids[1]}'.",
            f"SELECT pNo, pName, qty, dtlAmt FROM {V}WP_vOutStock WHERE OutStkId='{ids[1]}' AND isDel='N' AND dtlIsDel='N'",
            "medium", "WP_vOutStock"))

    samples.append(make_entry(
        "List distinct member IDs and names in outbound stock.",
        f"SELECT DISTINCT memId, memName FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' AND memId IS NOT NULL",
        "medium", "WP_vOutStock"))

    if out_types:
        samples.append(make_entry(
            f"Show outbound stock records with outType='{out_types[0]}'.",
            f"SELECT DISTINCT OutStkId, amount FROM {V}WP_vOutStock WHERE outType='{out_types[0]}' AND isDel='N' AND dtlIsDel='N'",
            "medium", "WP_vOutStock"))

    # ── Hard (8) ──
    samples.append(make_entry(
        "Which member has the highest total outbound stock amount?",
        f"SELECT TOP 1 memName, SUM(amt) AS total FROM (SELECT DISTINCT OutStkId, memName, amount AS amt FROM {V}WP_vOutStock WHERE isDel='N' AND memName IS NOT NULL) sub GROUP BY memName ORDER BY total DESC",
        "hard", "WP_vOutStock"))

    samples.append(make_entry(
        "List members who have more than 5 outbound stock records.",
        f"SELECT memName, COUNT(DISTINCT OutStkId) AS cnt FROM {V}WP_vOutStock WHERE isDel='N' AND memName IS NOT NULL GROUP BY memName HAVING COUNT(DISTINCT OutStkId) > 5",
        "hard", "WP_vOutStock"))

    samples.append(make_entry(
        "Show outbound stock records whose amount is above the average.",
        f"SELECT DISTINCT OutStkId, amount FROM {V}WP_vOutStock WHERE isDel='N' AND amount > (SELECT AVG(amount) FROM (SELECT DISTINCT OutStkId, amount FROM {V}WP_vOutStock WHERE isDel='N') sub)",
        "hard", "WP_vOutStock"))

    if members:
        mid, mname = members[0]
        samples.append(make_entry(
            f"What products did member '{mname}' buy in outbound stock?",
            f"SELECT DISTINCT pNo, pName FROM {V}WP_vOutStock WHERE memName=N'{mname}' AND isDel='N' AND dtlIsDel='N'",
            "hard", "WP_vOutStock"))

    samples.append(make_entry(
        "Show the month with the highest total outbound stock amount.",
        f"SELECT TOP 1 LEFT(OutStkId,6) AS mon, SUM(amt) AS total FROM (SELECT DISTINCT OutStkId, amount AS amt FROM {V}WP_vOutStock WHERE isDel='N') sub GROUP BY LEFT(OutStkId,6) ORDER BY total DESC",
        "hard", "WP_vOutStock"))

    samples.append(make_entry(
        "List products that appear in more than 10 outbound stock records.",
        f"SELECT pName, COUNT(DISTINCT OutStkId) AS cnt FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' AND pName IS NOT NULL GROUP BY pName HAVING COUNT(DISTINCT OutStkId) > 10",
        "hard", "WP_vOutStock"))

    samples.append(make_entry(
        "Show the top 3 products by total quantity sold in outbound stock.",
        f"SELECT TOP 3 pName, SUM(qty) AS totalQty FROM {V}WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY totalQty DESC",
        "hard", "WP_vOutStock"))

    samples.append(make_entry(
        "List members whose total outbound stock amount exceeds 50000.",
        f"SELECT memName, SUM(amt) AS total FROM (SELECT DISTINCT OutStkId, memName, amount AS amt FROM {V}WP_vOutStock WHERE isDel='N' AND memName IS NOT NULL) sub GROUP BY memName HAVING SUM(amt) > 50000",
        "hard", "WP_vOutStock"))

    return samples


def gen_transfer(d):
    """WP_vTransfer — 34 questions. Has isDel, dtlIsDel. Date: LEFT(TransferId,8/6). Has costAvg."""
    samples = []
    ids = d['tr_ids']
    months = d['tr_months']
    dates = d['tr_dates']
    wh_from = d['tr_warehouses']  # (FromWhSn, fWhName)
    wh_to = d['tr_to_warehouses']  # (ToWhSn, tfWhName)
    emp_ids = d['tr_emp_ids']
    kw = d['common_keywords']

    # ── Easy (14) ──
    samples.append(make_entry(
        "List all active transfer IDs.",
        f"SELECT DISTINCT TransferId FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N'",
        "easy", "WP_vTransfer"))

    samples.append(make_entry(
        "How many active transfer records are there?",
        f"SELECT COUNT(DISTINCT TransferId) FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N'",
        "easy", "WP_vTransfer"))

    if ids:
        samples.append(make_entry(
            f"Show the details of transfer ID '{ids[0]}'.",
            f"SELECT * FROM {V}WP_vTransfer WHERE TransferId='{ids[0]}' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vTransfer"))

    if wh_from:
        sn, name = wh_from[0]
        samples.append(make_entry(
            f"List transfers from warehouse '{name}'.",
            f"SELECT DISTINCT TransferId FROM {V}WP_vTransfer WHERE fWhName=N'{name}' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vTransfer"))

    if wh_to:
        sn, name = wh_to[0]
        samples.append(make_entry(
            f"List transfers to warehouse '{name}'.",
            f"SELECT DISTINCT TransferId FROM {V}WP_vTransfer WHERE tfWhName=N'{name}' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vTransfer"))

    if months:
        m = months[0]
        samples.append(make_entry(
            f"How many transfers were created in month {m}?",
            f"SELECT COUNT(DISTINCT TransferId) FROM {V}WP_vTransfer WHERE LEFT(TransferId,6)='{m}' AND isDel='N'",
            "easy", "WP_vTransfer"))

    samples.append(make_entry(
        "List all distinct source warehouse names in transfers.",
        f"SELECT DISTINCT fWhName FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' AND fWhName IS NOT NULL",
        "easy", "WP_vTransfer"))

    samples.append(make_entry(
        "List all distinct destination warehouse names in transfers.",
        f"SELECT DISTINCT tfWhName FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' AND tfWhName IS NOT NULL",
        "easy", "WP_vTransfer"))

    if dates:
        dt = dates[0]
        samples.append(make_entry(
            f"Show transfers created on date {dt}.",
            f"SELECT DISTINCT TransferId FROM {V}WP_vTransfer WHERE LEFT(TransferId,8)='{dt}' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vTransfer"))

    samples.append(make_entry(
        "What is the total quantity of all active transfers?",
        f"SELECT SUM(qty) FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N'",
        "easy", "WP_vTransfer"))

    if emp_ids:
        samples.append(make_entry(
            f"List transfers handled by employee '{emp_ids[0]}'.",
            f"SELECT DISTINCT TransferId FROM {V}WP_vTransfer WHERE empId='{emp_ids[0]}' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vTransfer"))

    samples.append(make_entry(
        "Show all distinct employee IDs in transfers.",
        f"SELECT DISTINCT empId FROM {V}WP_vTransfer WHERE isDel='N' AND empId IS NOT NULL",
        "easy", "WP_vTransfer"))

    samples.append(make_entry(
        "Show all distinct product names in transfers.",
        f"SELECT DISTINCT pName FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' AND pName IS NOT NULL",
        "easy", "WP_vTransfer"))

    if kw:
        samples.append(make_entry(
            f"List transfers with product names containing '{kw[2]}'.",
            f"SELECT DISTINCT TransferId, pName FROM {V}WP_vTransfer WHERE pName LIKE N'%{kw[2]}%' AND isDel='N' AND dtlIsDel='N'",
            "easy", "WP_vTransfer"))

    # ── Medium (12) ──
    if months:
        m = months[0]
        samples.append(make_entry(
            f"What is the total transfer quantity in month {m}?",
            f"SELECT SUM(qty) FROM {V}WP_vTransfer WHERE LEFT(TransferId,6)='{m}' AND isDel='N' AND dtlIsDel='N'",
            "medium", "WP_vTransfer"))

    samples.append(make_entry(
        "Show the count of transfers grouped by source warehouse.",
        f"SELECT fWhName, COUNT(DISTINCT TransferId) FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY fWhName",
        "medium", "WP_vTransfer"))

    samples.append(make_entry(
        "List the total quantity per destination warehouse in transfers.",
        f"SELECT tfWhName, SUM(qty) FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY tfWhName",
        "medium", "WP_vTransfer"))

    samples.append(make_entry(
        "Show the top 5 transfers by quantity.",
        f"SELECT TOP 5 TransferId, pName, qty FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' ORDER BY qty DESC",
        "medium", "WP_vTransfer"))

    samples.append(make_entry(
        "Count the number of distinct products in transfers.",
        f"SELECT COUNT(DISTINCT pNo) FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N'",
        "medium", "WP_vTransfer"))

    if months and len(months) > 1:
        m1, m2 = sorted(months)[0], sorted(months)[-1]
        samples.append(make_entry(
            f"List transfers created between months {m1} and {m2}.",
            f"SELECT DISTINCT TransferId FROM {V}WP_vTransfer WHERE LEFT(TransferId,6) BETWEEN '{m1}' AND '{m2}' AND isDel='N' AND dtlIsDel='N'",
            "medium", "WP_vTransfer"))

    samples.append(make_entry(
        "Show the total quantity per product in transfers.",
        f"SELECT pName, SUM(qty) FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName",
        "medium", "WP_vTransfer"))

    samples.append(make_entry(
        "Count transfer records per month.",
        f"SELECT LEFT(TransferId,6), COUNT(DISTINCT TransferId) FROM {V}WP_vTransfer WHERE isDel='N' GROUP BY LEFT(TransferId,6)",
        "medium", "WP_vTransfer"))

    samples.append(make_entry(
        "What is the average costAvg of transfer items?",
        f"SELECT AVG(costAvg) FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N'",
        "medium", "WP_vTransfer"))

    if ids and len(ids) > 1:
        samples.append(make_entry(
            f"Show product details for transfer '{ids[1]}'.",
            f"SELECT pNo, pName, qty, costAvg FROM {V}WP_vTransfer WHERE TransferId='{ids[1]}' AND isDel='N' AND dtlIsDel='N'",
            "medium", "WP_vTransfer"))

    samples.append(make_entry(
        "Show the total costAvg value per source warehouse.",
        f"SELECT fWhName, SUM(costAvg * qty) FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY fWhName",
        "medium", "WP_vTransfer"))

    if wh_from and len(wh_from) > 1:
        sn2, name2 = wh_from[1]
        samples.append(make_entry(
            f"Show transfers from warehouse '{name2}' with quantity greater than 5.",
            f"SELECT TransferId, pName, qty FROM {V}WP_vTransfer WHERE fWhName=N'{name2}' AND qty > 5 AND isDel='N' AND dtlIsDel='N'",
            "medium", "WP_vTransfer"))

    # ── Hard (8) ──
    samples.append(make_entry(
        "Which source warehouse has the most transfer records?",
        f"SELECT TOP 1 fWhName, COUNT(DISTINCT TransferId) AS cnt FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY fWhName ORDER BY cnt DESC",
        "hard", "WP_vTransfer"))

    samples.append(make_entry(
        "List products transferred more than 3 times.",
        f"SELECT pName, COUNT(DISTINCT TransferId) AS cnt FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' AND pName IS NOT NULL GROUP BY pName HAVING COUNT(DISTINCT TransferId) > 3",
        "hard", "WP_vTransfer"))

    samples.append(make_entry(
        "Show transfers whose total costAvg per item exceeds the average.",
        f"SELECT TransferId, pName, costAvg FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' AND costAvg > (SELECT AVG(costAvg) FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N')",
        "hard", "WP_vTransfer"))

    if wh_from:
        sn, name = wh_from[0]
        samples.append(make_entry(
            f"What products were transferred from warehouse '{name}'?",
            f"SELECT DISTINCT pNo, pName FROM {V}WP_vTransfer WHERE fWhName=N'{name}' AND isDel='N' AND dtlIsDel='N'",
            "hard", "WP_vTransfer"))

    samples.append(make_entry(
        "Show the month with the most transfers.",
        f"SELECT TOP 1 LEFT(TransferId,6) AS mon, COUNT(DISTINCT TransferId) AS cnt FROM {V}WP_vTransfer WHERE isDel='N' GROUP BY LEFT(TransferId,6) ORDER BY cnt DESC",
        "hard", "WP_vTransfer"))

    samples.append(make_entry(
        "Show the top 3 products by total quantity transferred.",
        f"SELECT TOP 3 pName, SUM(qty) AS totalQty FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY totalQty DESC",
        "hard", "WP_vTransfer"))

    samples.append(make_entry(
        "List source warehouses that have transferred more than 2 distinct products.",
        f"SELECT fWhName, COUNT(DISTINCT pNo) AS productCount FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY fWhName HAVING COUNT(DISTINCT pNo) > 2",
        "hard", "WP_vTransfer"))

    samples.append(make_entry(
        "Which destination warehouse received the highest total quantity?",
        f"SELECT TOP 1 tfWhName, SUM(qty) AS totalQty FROM {V}WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY tfWhName ORDER BY totalQty DESC",
        "hard", "WP_vTransfer"))

    return samples


def gen_inventory(d):
    """WP_vInventory — 34 questions. NO isDel. Date filter: pNo LIKE 'YYYYMMDD%'."""
    samples = []
    whs = d['inv_warehouses']  # (WarehouseId, WarehouseName)
    pnos = d['inv_pnos']
    pnames = d['inv_pnames']
    pvnames = d['inv_pvnames']
    kw = d['common_keywords']

    # ── Easy (14) ──
    samples.append(make_entry(
        "How many inventory records are there?",
        f"SELECT COUNT(*) FROM {V}WP_vInventory",
        "easy", "WP_vInventory"))

    samples.append(make_entry(
        "List all distinct warehouse names in inventory.",
        f"SELECT DISTINCT WarehouseName FROM {V}WP_vInventory",
        "easy", "WP_vInventory"))

    if whs:
        wid, wname = whs[0]
        samples.append(make_entry(
            f"List all products in warehouse '{wname}'.",
            f"SELECT pNo, pName, qty FROM {V}WP_vInventory WHERE WarehouseName=N'{wname}'",
            "easy", "WP_vInventory"))

    if pnames:
        samples.append(make_entry(
            f"Show the inventory for product '{pnames[0]}'.",
            f"SELECT WarehouseName, qty, costAvg FROM {V}WP_vInventory WHERE pName=N'{pnames[0]}'",
            "easy", "WP_vInventory"))

    samples.append(make_entry(
        "What is the total quantity of all inventory?",
        f"SELECT SUM(qty) FROM {V}WP_vInventory",
        "easy", "WP_vInventory"))

    samples.append(make_entry(
        "Show distinct vendor names in inventory.",
        f"SELECT DISTINCT pvName FROM {V}WP_vInventory WHERE pvName IS NOT NULL",
        "easy", "WP_vInventory"))

    if pvnames:
        samples.append(make_entry(
            f"List inventory items from vendor '{pvnames[0]}'.",
            f"SELECT pNo, pName, qty FROM {V}WP_vInventory WHERE pvName=N'{pvnames[0]}'",
            "easy", "WP_vInventory"))

    samples.append(make_entry(
        "What is the maximum quantity of a single inventory record?",
        f"SELECT MAX(qty) FROM {V}WP_vInventory",
        "easy", "WP_vInventory"))

    samples.append(make_entry(
        "What is the minimum costAvg in inventory?",
        f"SELECT MIN(costAvg) FROM {V}WP_vInventory WHERE costAvg > 0",
        "easy", "WP_vInventory"))

    if kw:
        samples.append(make_entry(
            f"List inventory items with product names containing '{kw[0]}'.",
            f"SELECT pNo, pName, qty FROM {V}WP_vInventory WHERE pName LIKE N'%{kw[0]}%'",
            "easy", "WP_vInventory"))

    samples.append(make_entry(
        "How many distinct products are in inventory?",
        f"SELECT COUNT(DISTINCT pNo) FROM {V}WP_vInventory",
        "easy", "WP_vInventory"))

    samples.append(make_entry(
        "Show all distinct product units in inventory.",
        f"SELECT DISTINCT pUName FROM {V}WP_vInventory WHERE pUName IS NOT NULL",
        "easy", "WP_vInventory"))

    samples.append(make_entry(
        "List inventory items where quantity is zero.",
        f"SELECT pNo, pName, WarehouseName FROM {V}WP_vInventory WHERE qty = 0",
        "easy", "WP_vInventory"))

    samples.append(make_entry(
        "What is the average costStd in inventory?",
        f"SELECT AVG(costStd) FROM {V}WP_vInventory WHERE costStd > 0",
        "easy", "WP_vInventory"))

    # ── Medium (12) ──
    samples.append(make_entry(
        "Show the total quantity per warehouse in inventory.",
        f"SELECT WarehouseName, SUM(qty) FROM {V}WP_vInventory GROUP BY WarehouseName",
        "medium", "WP_vInventory"))

    samples.append(make_entry(
        "Show the count of products per warehouse.",
        f"SELECT WarehouseName, COUNT(*) FROM {V}WP_vInventory GROUP BY WarehouseName",
        "medium", "WP_vInventory"))

    samples.append(make_entry(
        "List the total inventory quantity per vendor.",
        f"SELECT pvName, SUM(qty) FROM {V}WP_vInventory WHERE pvName IS NOT NULL GROUP BY pvName",
        "medium", "WP_vInventory"))

    samples.append(make_entry(
        "Show the top 10 products by quantity in inventory.",
        f"SELECT TOP 10 pName, qty FROM {V}WP_vInventory ORDER BY qty DESC",
        "medium", "WP_vInventory"))

    if whs:
        wid, wname = whs[0]
        samples.append(make_entry(
            f"Count distinct products in warehouse '{wname}'.",
            f"SELECT COUNT(DISTINCT pNo) FROM {V}WP_vInventory WHERE WarehouseName=N'{wname}'",
            "medium", "WP_vInventory"))

    samples.append(make_entry(
        "Show inventory items with quantity greater than 100.",
        f"SELECT pNo, pName, qty, WarehouseName FROM {V}WP_vInventory WHERE qty > 100",
        "medium", "WP_vInventory"))

    samples.append(make_entry(
        "List the average costAvg per warehouse.",
        f"SELECT WarehouseName, AVG(costAvg) FROM {V}WP_vInventory GROUP BY WarehouseName",
        "medium", "WP_vInventory"))

    samples.append(make_entry(
        "Show inventory items where isSale is '1'.",
        f"SELECT pNo, pName, qty FROM {V}WP_vInventory WHERE isSale='1'",
        "medium", "WP_vInventory"))

    samples.append(make_entry(
        "List inventory items with costAvg greater than 500.",
        f"SELECT pNo, pName, costAvg, qty FROM {V}WP_vInventory WHERE costAvg > 500",
        "medium", "WP_vInventory"))

    if pvnames and len(pvnames) > 1:
        samples.append(make_entry(
            f"Show inventory for vendor '{pvnames[1]}' sorted by quantity descending.",
            f"SELECT pNo, pName, qty FROM {V}WP_vInventory WHERE pvName=N'{pvnames[1]}' ORDER BY qty DESC",
            "medium", "WP_vInventory"))

    samples.append(make_entry(
        "Count distinct vendors per warehouse in inventory.",
        f"SELECT WarehouseName, COUNT(DISTINCT pvName) FROM {V}WP_vInventory WHERE pvName IS NOT NULL GROUP BY WarehouseName",
        "medium", "WP_vInventory"))

    samples.append(make_entry(
        "Show inventory items where qtySafe is greater than qtyNow.",
        f"SELECT pNo, pName, qtyNow, qtySafe FROM {V}WP_vInventory WHERE qtySafe > qtyNow",
        "medium", "WP_vInventory"))

    # ── Hard (8) ──
    samples.append(make_entry(
        "Which warehouse has the highest total inventory quantity?",
        f"SELECT TOP 1 WarehouseName, SUM(qty) AS totalQty FROM {V}WP_vInventory GROUP BY WarehouseName ORDER BY totalQty DESC",
        "hard", "WP_vInventory"))

    samples.append(make_entry(
        "List vendors who have more than 20 products in inventory.",
        f"SELECT pvName, COUNT(DISTINCT pNo) AS cnt FROM {V}WP_vInventory WHERE pvName IS NOT NULL GROUP BY pvName HAVING COUNT(DISTINCT pNo) > 20",
        "hard", "WP_vInventory"))

    samples.append(make_entry(
        "Show inventory items with costAvg above the overall average.",
        f"SELECT pNo, pName, costAvg FROM {V}WP_vInventory WHERE costAvg > (SELECT AVG(costAvg) FROM {V}WP_vInventory WHERE costAvg > 0)",
        "hard", "WP_vInventory"))

    if whs:
        wid, wname = whs[0]
        samples.append(make_entry(
            f"What is the total inventory value (costAvg * qty) for warehouse '{wname}'?",
            f"SELECT SUM(costAvg * qty) FROM {V}WP_vInventory WHERE WarehouseName=N'{wname}'",
            "hard", "WP_vInventory"))

    samples.append(make_entry(
        "Show the warehouse with the most distinct products.",
        f"SELECT TOP 1 WarehouseName, COUNT(DISTINCT pNo) AS cnt FROM {V}WP_vInventory GROUP BY WarehouseName ORDER BY cnt DESC",
        "hard", "WP_vInventory"))

    samples.append(make_entry(
        "List products that appear in more than 1 warehouse.",
        f"SELECT pName, COUNT(DISTINCT WarehouseName) AS whCount FROM {V}WP_vInventory GROUP BY pName HAVING COUNT(DISTINCT WarehouseName) > 1",
        "hard", "WP_vInventory"))

    samples.append(make_entry(
        "Show the top 3 vendors by total inventory quantity.",
        f"SELECT TOP 3 pvName, SUM(qty) AS totalQty FROM {V}WP_vInventory WHERE pvName IS NOT NULL GROUP BY pvName ORDER BY totalQty DESC",
        "hard", "WP_vInventory"))

    samples.append(make_entry(
        "List warehouses where average costAvg exceeds 300.",
        f"SELECT WarehouseName, AVG(costAvg) AS avgCost FROM {V}WP_vInventory GROUP BY WarehouseName HAVING AVG(costAvg) > 300",
        "hard", "WP_vInventory"))

    return samples


def gen_product(d):
    """WP_vProduct — 34 questions. NO isDel. Date filter: pNo LIKE 'YYYYMMDD%'."""
    samples = []
    pnos = d['pr_pnos']
    pnames = d['pr_pnames']
    pvnames = d['pr_pvnames']
    pvids = d['pr_pvids']
    barcodes = d['pr_barcodes']
    punames = d['pr_punames']
    sale_vals = d['pr_sale_vals']
    kw = d['common_keywords']

    # ── Easy (14) ──
    samples.append(make_entry(
        "How many products are there?",
        f"SELECT COUNT(*) FROM {V}WP_vProduct",
        "easy", "WP_vProduct"))

    samples.append(make_entry(
        "List all distinct product names.",
        f"SELECT DISTINCT pName FROM {V}WP_vProduct",
        "easy", "WP_vProduct"))

    if pnames:
        samples.append(make_entry(
            f"Show the details of product '{pnames[0]}'.",
            f"SELECT pNo, pName, priceStd, costAvg, pvName FROM {V}WP_vProduct WHERE pName=N'{pnames[0]}'",
            "easy", "WP_vProduct"))

    samples.append(make_entry(
        "List all distinct vendor names in products.",
        f"SELECT DISTINCT pvName FROM {V}WP_vProduct WHERE pvName IS NOT NULL",
        "easy", "WP_vProduct"))

    if pvnames:
        samples.append(make_entry(
            f"List products from vendor '{pvnames[0]}'.",
            f"SELECT pNo, pName, priceStd FROM {V}WP_vProduct WHERE pvName=N'{pvnames[0]}'",
            "easy", "WP_vProduct"))

    samples.append(make_entry(
        "What is the maximum standard price among all products?",
        f"SELECT MAX(priceStd) FROM {V}WP_vProduct",
        "easy", "WP_vProduct"))

    samples.append(make_entry(
        "What is the minimum standard price among all products?",
        f"SELECT MIN(priceStd) FROM {V}WP_vProduct WHERE priceStd > 0",
        "easy", "WP_vProduct"))

    if kw:
        samples.append(make_entry(
            f"List products with names containing '{kw[0]}'.",
            f"SELECT pNo, pName, priceStd FROM {V}WP_vProduct WHERE pName LIKE N'%{kw[0]}%'",
            "easy", "WP_vProduct"))

    samples.append(make_entry(
        "Show all distinct product units.",
        f"SELECT DISTINCT pUName FROM {V}WP_vProduct WHERE pUName IS NOT NULL",
        "easy", "WP_vProduct"))

    if barcodes:
        samples.append(make_entry(
            f"Find the product with barcode '{barcodes[0]}'.",
            f"SELECT pNo, pName, priceStd FROM {V}WP_vProduct WHERE pBarcode='{barcodes[0]}'",
            "easy", "WP_vProduct"))

    samples.append(make_entry(
        "How many distinct vendors supply products?",
        f"SELECT COUNT(DISTINCT pvName) FROM {V}WP_vProduct WHERE pvName IS NOT NULL",
        "easy", "WP_vProduct"))

    if sale_vals:
        sv = sale_vals[0]
        samples.append(make_entry(
            f"List products where isSale is '{sv}'.",
            f"SELECT pNo, pName FROM {V}WP_vProduct WHERE isSale='{sv}'",
            "easy", "WP_vProduct"))

    samples.append(make_entry(
        "What is the average standard price of all products?",
        f"SELECT AVG(priceStd) FROM {V}WP_vProduct WHERE priceStd > 0",
        "easy", "WP_vProduct"))

    if kw and len(kw) > 3:
        samples.append(make_entry(
            f"List products with names containing '{kw[3]}'.",
            f"SELECT pNo, pName, priceStd FROM {V}WP_vProduct WHERE pName LIKE N'%{kw[3]}%'",
            "easy", "WP_vProduct"))

    # ── Medium (12) ──
    samples.append(make_entry(
        "Show the count of products per vendor.",
        f"SELECT pvName, COUNT(*) FROM {V}WP_vProduct WHERE pvName IS NOT NULL GROUP BY pvName",
        "medium", "WP_vProduct"))

    samples.append(make_entry(
        "List the average standard price per vendor.",
        f"SELECT pvName, AVG(priceStd) FROM {V}WP_vProduct WHERE pvName IS NOT NULL GROUP BY pvName",
        "medium", "WP_vProduct"))

    samples.append(make_entry(
        "Show the top 10 most expensive products by standard price.",
        f"SELECT TOP 10 pName, priceStd FROM {V}WP_vProduct ORDER BY priceStd DESC",
        "medium", "WP_vProduct"))

    samples.append(make_entry(
        "Count products per unit type.",
        f"SELECT pUName, COUNT(*) FROM {V}WP_vProduct WHERE pUName IS NOT NULL GROUP BY pUName",
        "medium", "WP_vProduct"))

    samples.append(make_entry(
        "List products with standard price greater than 500.",
        f"SELECT pNo, pName, priceStd FROM {V}WP_vProduct WHERE priceStd > 500",
        "medium", "WP_vProduct"))

    if pvnames and len(pvnames) > 2:
        samples.append(make_entry(
            f"Show products from vendor '{pvnames[2]}' sorted by price descending.",
            f"SELECT pNo, pName, priceStd FROM {V}WP_vProduct WHERE pvName=N'{pvnames[2]}' ORDER BY priceStd DESC",
            "medium", "WP_vProduct"))

    samples.append(make_entry(
        "Show products where costAvg is greater than priceStd.",
        f"SELECT pNo, pName, costAvg, priceStd FROM {V}WP_vProduct WHERE costAvg > priceStd AND priceStd > 0",
        "medium", "WP_vProduct"))

    samples.append(make_entry(
        "List products with pvDiscount greater than 0.",
        f"SELECT pNo, pName, pvDiscount FROM {V}WP_vProduct WHERE pvDiscount > 0",
        "medium", "WP_vProduct"))

    samples.append(make_entry(
        "Count the number of products with isSale='1'.",
        f"SELECT COUNT(*) FROM {V}WP_vProduct WHERE isSale='1'",
        "medium", "WP_vProduct"))

    samples.append(make_entry(
        "Show distinct vendor IDs and names in products.",
        f"SELECT DISTINCT pvId, pvName FROM {V}WP_vProduct WHERE pvId IS NOT NULL",
        "medium", "WP_vProduct"))

    samples.append(make_entry(
        "List products where qtyNow is less than qtySafe.",
        f"SELECT pNo, pName, qtyNow, qtySafe FROM {V}WP_vProduct WHERE qtySafe > qtyNow",
        "medium", "WP_vProduct"))

    samples.append(make_entry(
        "Show products where isPvDiscount is '1'.",
        f"SELECT pNo, pName, pvDiscount FROM {V}WP_vProduct WHERE isPvDiscount='1'",
        "medium", "WP_vProduct"))

    # ── Hard (8) ──
    samples.append(make_entry(
        "Which vendor supplies the most products?",
        f"SELECT TOP 1 pvName, COUNT(*) AS cnt FROM {V}WP_vProduct WHERE pvName IS NOT NULL GROUP BY pvName ORDER BY cnt DESC",
        "hard", "WP_vProduct"))

    samples.append(make_entry(
        "List vendors who supply more than 50 products.",
        f"SELECT pvName, COUNT(*) AS cnt FROM {V}WP_vProduct WHERE pvName IS NOT NULL GROUP BY pvName HAVING COUNT(*) > 50",
        "hard", "WP_vProduct"))

    samples.append(make_entry(
        "Show products with priceStd above the average.",
        f"SELECT pNo, pName, priceStd FROM {V}WP_vProduct WHERE priceStd > (SELECT AVG(priceStd) FROM {V}WP_vProduct WHERE priceStd > 0)",
        "hard", "WP_vProduct"))

    samples.append(make_entry(
        "What is the total standard price value of all products (sum of priceStd)?",
        f"SELECT SUM(priceStd) FROM {V}WP_vProduct",
        "hard", "WP_vProduct"))

    samples.append(make_entry(
        "Show the vendor with the highest average product price.",
        f"SELECT TOP 1 pvName, AVG(priceStd) AS avgPrice FROM {V}WP_vProduct WHERE pvName IS NOT NULL AND priceStd > 0 GROUP BY pvName ORDER BY avgPrice DESC",
        "hard", "WP_vProduct"))

    samples.append(make_entry(
        "List products that have costAvg exceeding double their costStd.",
        f"SELECT pNo, pName, costAvg, costStd FROM {V}WP_vProduct WHERE costAvg > costStd * 2 AND costStd > 0",
        "hard", "WP_vProduct"))

    samples.append(make_entry(
        "Show the top 5 vendors by total number of products, descending.",
        f"SELECT TOP 5 pvName, COUNT(*) AS cnt FROM {V}WP_vProduct WHERE pvName IS NOT NULL GROUP BY pvName ORDER BY cnt DESC",
        "hard", "WP_vProduct"))

    samples.append(make_entry(
        "List unit types that have more than 10 products.",
        f"SELECT pUName, COUNT(*) AS cnt FROM {V}WP_vProduct WHERE pUName IS NOT NULL GROUP BY pUName HAVING COUNT(*) > 10",
        "hard", "WP_vProduct"))

    return samples


def gen_provider(d):
    """WP_vProvider — 34+ questions. NO isDel. Use isStop, pvDiscount."""
    samples = []
    pvids = d['pv_pvids']
    pvnames = d['pv_pvnames']
    cities = d['pv_cities']
    zones = d['pv_zones']
    knames = d['pv_knames']
    banknames = d['pv_banknames']

    # ── Easy (14) ──
    samples.append(make_entry(
        "How many providers are there?",
        f"SELECT COUNT(*) FROM {V}WP_vProvider",
        "easy", "WP_vProvider"))

    samples.append(make_entry(
        "List all active providers.",
        f"SELECT pvId, pvName FROM {V}WP_vProvider WHERE isStop='N'",
        "easy", "WP_vProvider"))

    samples.append(make_entry(
        "How many active providers are there?",
        f"SELECT COUNT(*) FROM {V}WP_vProvider WHERE isStop='N'",
        "easy", "WP_vProvider"))

    if pvnames:
        samples.append(make_entry(
            f"Show the details of provider '{pvnames[0]}'.",
            f"SELECT * FROM {V}WP_vProvider WHERE pvName=N'{pvnames[0]}'",
            "easy", "WP_vProvider"))

    samples.append(make_entry(
        "List all distinct provider cities.",
        f"SELECT DISTINCT pvCity FROM {V}WP_vProvider WHERE pvCity IS NOT NULL AND pvCity <> ''",
        "easy", "WP_vProvider"))

    if cities:
        samples.append(make_entry(
            f"List providers in city '{cities[0]}'.",
            f"SELECT pvId, pvName FROM {V}WP_vProvider WHERE pvCity=N'{cities[0]}'",
            "easy", "WP_vProvider"))

    samples.append(make_entry(
        "List all distinct provider category names.",
        f"SELECT DISTINCT pvKName FROM {V}WP_vProvider WHERE pvKName IS NOT NULL AND pvKName <> ''",
        "easy", "WP_vProvider"))

    if pvids:
        samples.append(make_entry(
            f"Show the provider with ID '{pvids[0]}'.",
            f"SELECT pvId, pvName, pvCity, pvZone FROM {V}WP_vProvider WHERE pvId='{pvids[0]}'",
            "easy", "WP_vProvider"))

    samples.append(make_entry(
        "List all providers with their phone numbers.",
        f"SELECT pvId, pvName, pvTel FROM {V}WP_vProvider WHERE isStop='N'",
        "easy", "WP_vProvider"))

    samples.append(make_entry(
        "Show all providers who have an email address.",
        f"SELECT pvId, pvName, email FROM {V}WP_vProvider WHERE email IS NOT NULL AND email <> ''",
        "easy", "WP_vProvider"))

    samples.append(make_entry(
        "List all stopped providers.",
        f"SELECT pvId, pvName FROM {V}WP_vProvider WHERE isStop='Y'",
        "easy", "WP_vProvider"))

    if pvnames and len(pvnames) > 1:
        samples.append(make_entry(
            f"Show the contact name for provider '{pvnames[1]}'.",
            f"SELECT ctactName, ctactTel FROM {V}WP_vProvider WHERE pvName=N'{pvnames[1]}'",
            "easy", "WP_vProvider"))

    samples.append(make_entry(
        "List all distinct bank names used by providers.",
        f"SELECT DISTINCT bankName FROM {V}WP_vProvider WHERE bankName IS NOT NULL AND bankName <> ''",
        "easy", "WP_vProvider"))

    if zones:
        samples.append(make_entry(
            f"List providers in zone '{zones[0]}'.",
            f"SELECT pvId, pvName FROM {V}WP_vProvider WHERE pvZone=N'{zones[0]}'",
            "easy", "WP_vProvider"))

    # ── Medium (12) ──
    samples.append(make_entry(
        "Show the count of providers per city.",
        f"SELECT pvCity, COUNT(*) FROM {V}WP_vProvider WHERE pvCity IS NOT NULL AND pvCity <> '' GROUP BY pvCity",
        "medium", "WP_vProvider"))

    samples.append(make_entry(
        "Count providers per category.",
        f"SELECT pvKName, COUNT(*) FROM {V}WP_vProvider WHERE pvKName IS NOT NULL AND pvKName <> '' GROUP BY pvKName",
        "medium", "WP_vProvider"))

    samples.append(make_entry(
        "List providers with pvDiscount greater than 0.",
        f"SELECT pvId, pvName, pvDiscount FROM {V}WP_vProvider WHERE pvDiscount > 0",
        "medium", "WP_vProvider"))

    samples.append(make_entry(
        "Show the average pvDiscount per category.",
        f"SELECT pvKName, AVG(pvDiscount) FROM {V}WP_vProvider WHERE pvKName IS NOT NULL AND pvKName <> '' GROUP BY pvKName",
        "medium", "WP_vProvider"))

    samples.append(make_entry(
        "List active providers sorted by pvName.",
        f"SELECT pvId, pvName FROM {V}WP_vProvider WHERE isStop='N' ORDER BY pvName",
        "medium", "WP_vProvider"))

    samples.append(make_entry(
        "Show the top 5 providers by pvDiscount.",
        f"SELECT TOP 5 pvId, pvName, pvDiscount FROM {V}WP_vProvider ORDER BY pvDiscount DESC",
        "medium", "WP_vProvider"))

    if cities and len(cities) > 1:
        samples.append(make_entry(
            f"Count active providers in city '{cities[0]}'.",
            f"SELECT COUNT(*) FROM {V}WP_vProvider WHERE pvCity=N'{cities[0]}' AND isStop='N'",
            "medium", "WP_vProvider"))

    samples.append(make_entry(
        "List providers with both fax and email.",
        f"SELECT pvId, pvName, fax, email FROM {V}WP_vProvider WHERE fax IS NOT NULL AND fax <> '' AND email IS NOT NULL AND email <> ''",
        "medium", "WP_vProvider"))

    if knames:
        samples.append(make_entry(
            f"List providers in category '{knames[0]}'.",
            f"SELECT pvId, pvName FROM {V}WP_vProvider WHERE pvKName=N'{knames[0]}'",
            "medium", "WP_vProvider"))

    samples.append(make_entry(
        "Show distinct zones per city for providers.",
        f"SELECT DISTINCT pvCity, pvZone FROM {V}WP_vProvider WHERE pvCity IS NOT NULL AND pvCity <> '' AND pvZone IS NOT NULL AND pvZone <> ''",
        "medium", "WP_vProvider"))

    samples.append(make_entry(
        "Count providers per bank name.",
        f"SELECT bankName, COUNT(*) FROM {V}WP_vProvider WHERE bankName IS NOT NULL AND bankName <> '' GROUP BY bankName",
        "medium", "WP_vProvider"))

    if banknames:
        samples.append(make_entry(
            f"List providers using bank '{banknames[0]}'.",
            f"SELECT pvId, pvName, bankAccount FROM {V}WP_vProvider WHERE bankName=N'{banknames[0]}'",
            "medium", "WP_vProvider"))

    # ── Hard (8) ──
    samples.append(make_entry(
        "Which city has the most providers?",
        f"SELECT TOP 1 pvCity, COUNT(*) AS cnt FROM {V}WP_vProvider WHERE pvCity IS NOT NULL AND pvCity <> '' GROUP BY pvCity ORDER BY cnt DESC",
        "hard", "WP_vProvider"))

    samples.append(make_entry(
        "List cities with more than 5 providers.",
        f"SELECT pvCity, COUNT(*) AS cnt FROM {V}WP_vProvider WHERE pvCity IS NOT NULL AND pvCity <> '' GROUP BY pvCity HAVING COUNT(*) > 5",
        "hard", "WP_vProvider"))

    samples.append(make_entry(
        "Show providers with pvDiscount above the average.",
        f"SELECT pvId, pvName, pvDiscount FROM {V}WP_vProvider WHERE pvDiscount > (SELECT AVG(pvDiscount) FROM {V}WP_vProvider WHERE pvDiscount > 0)",
        "hard", "WP_vProvider"))

    samples.append(make_entry(
        "Which category has the highest average pvDiscount?",
        f"SELECT TOP 1 pvKName, AVG(pvDiscount) AS avgDisc FROM {V}WP_vProvider WHERE pvKName IS NOT NULL AND pvKName <> '' GROUP BY pvKName ORDER BY avgDisc DESC",
        "hard", "WP_vProvider"))

    samples.append(make_entry(
        "List categories with more than 10 providers.",
        f"SELECT pvKName, COUNT(*) AS cnt FROM {V}WP_vProvider WHERE pvKName IS NOT NULL AND pvKName <> '' GROUP BY pvKName HAVING COUNT(*) > 10",
        "hard", "WP_vProvider"))

    samples.append(make_entry(
        "Show the top 3 cities by number of active providers.",
        f"SELECT TOP 3 pvCity, COUNT(*) AS cnt FROM {V}WP_vProvider WHERE isStop='N' AND pvCity IS NOT NULL AND pvCity <> '' GROUP BY pvCity ORDER BY cnt DESC",
        "hard", "WP_vProvider"))

    samples.append(make_entry(
        "List providers whose taxId is not empty, grouped by city.",
        f"SELECT pvCity, COUNT(*) AS cnt FROM {V}WP_vProvider WHERE taxId IS NOT NULL AND taxId <> '' AND pvCity IS NOT NULL AND pvCity <> '' GROUP BY pvCity",
        "hard", "WP_vProvider"))

    samples.append(make_entry(
        "Which bank is used by the most providers?",
        f"SELECT TOP 1 bankName, COUNT(*) AS cnt FROM {V}WP_vProvider WHERE bankName IS NOT NULL AND bankName <> '' GROUP BY bankName ORDER BY cnt DESC",
        "hard", "WP_vProvider"))

    return samples


# ─── Main ────────────────────────────────────────────────────────────────

def main():
    print("Connecting to database...")
    conn = pyodbc.connect(CONN_STR)
    print("Connected. Fetching real data values...")
    data = fetch_values(conn)
    conn.close()
    print("Data fetched. Generating questions...")

    all_samples = []
    all_samples.extend(gen_acct_in(data))
    all_samples.extend(gen_acct_out(data))
    all_samples.extend(gen_outstock(data))
    all_samples.extend(gen_transfer(data))
    all_samples.extend(gen_inventory(data))
    all_samples.extend(gen_product(data))
    all_samples.extend(gen_provider(data))

    # Deduplicate by question text
    seen_q = set()
    unique = []
    for s in all_samples:
        if s['question'] not in seen_q:
            seen_q.add(s['question'])
            unique.append(s)

    # Also deduplicate by SQL
    seen_sql = set()
    final = []
    for s in unique:
        norm = s['query'].strip().rstrip(';').lower()
        if norm not in seen_sql:
            seen_sql.add(norm)
            final.append(s)

    # Report distribution
    view_counts = {}
    diff_counts = {}
    for s in final:
        v = s['view']
        d = s['difficulty']
        view_counts[v] = view_counts.get(v, 0) + 1
        diff_counts[d] = diff_counts.get(d, 0) + 1

    print(f"\nTotal unique samples: {len(final)}")
    print("\nPer view:")
    for v in sorted(view_counts):
        print(f"  {v}: {view_counts[v]}")
    print("\nPer difficulty:")
    for d in ['easy', 'medium', 'hard']:
        print(f"  {d}: {diff_counts.get(d, 0)}")

    # Trim to 240 if needed (take proportionally from each view)
    if len(final) > 240:
        random.shuffle(final)
        # Group by view
        by_view = {}
        for s in final:
            by_view.setdefault(s['view'], []).append(s)
        trimmed = []
        per_view = 240 // 7  # 34
        remainder = 240 % 7  # 2
        for i, v in enumerate(sorted(by_view)):
            limit = per_view + (1 if i < remainder else 0)
            trimmed.extend(by_view[v][:limit])
        final = trimmed[:240]

    # Save
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(final, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(final)} samples to {OUTPUT_PATH}")


if __name__ == '__main__':
    main()
