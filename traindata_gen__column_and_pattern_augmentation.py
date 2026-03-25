"""
traindata_gen__column_and_pattern_augmentation.py

Generates additional training samples targeting specific error patterns
found in evaluation. Outputs Spider-compatible format to
data/wp_m09/train_augment_v2.json
"""

import json
import re
import random
import os

random.seed(42)

# ── Realistic values ──────────────────────────────────────────────────────
PRODUCT_NAMES = [
    "玫瑰花茶", "薰衣草精油", "洋甘菊茶", "檸檬草", "薄荷",
    "茉莉花茶", "桂花釀", "枸杞茶", "紅棗茶", "菊花茶",
    "迷迭香精油", "尤加利精油", "茶樹精油", "佛手柑精油", "甜橙精油",
    "蘆薈膠", "乳木果油", "椰子油", "荷荷巴油", "玫瑰果油",
]
MEMBER_NAMES = ["王小明", "李小華", "張美玲", "陳大方", "林志偉", "黃淑芬", "吳佳穎", "劉建國", "蔡雅婷", "鄭文傑"]
WAREHOUSE_NAMES = ["台北倉", "高雄倉", "台中倉", "新竹倉", "桃園倉"]
SUPPLIER_NAMES = ["花草堂", "天然坊", "綠野仙蹤", "芳療世界", "香氛小舖", "草本家園", "自然堂", "本草集"]
DATES_8 = ["20250101", "20250215", "20250310", "20250405", "20250520", "20250601", "20250715", "20250803", "20250918", "20251025", "20251201", "20241115"]
DATES_6 = ["202501", "202502", "202503", "202504", "202505", "202506", "202507", "202508", "202509", "202510", "202511", "202512"]
YEARS = ["2024", "2025"]
ACCT_IN_IDS = [f"{d}000{i}" for d in DATES_8[:6] for i in range(1, 4)]
ACCT_OUT_IDS = [f"{d}000{i}" for d in DATES_8[:6] for i in range(1, 4)]
OUTSTOCK_IDS = [f"{d}000{i}" for d in DATES_8[:6] for i in range(1, 4)]
TRANSFER_IDS = [f"{d}000{i}" for d in DATES_8[:6] for i in range(1, 4)]
PNO_VALUES = list(range(1, 30))


def make_sample(question: str, query: str) -> dict:
    """Create a Spider-compatible sample."""
    query = query.strip().rstrip(";") + ";"
    # Tokenize query
    toks = re.findall(r"[A-Za-z_]+|[0-9]+|'[^']*'|N'[^']*'|[.,;()=<>!*+\-/]", query)
    # query_toks_no_value: replace string/number literals with 'value'
    toks_no_val = []
    for t in toks:
        if t.startswith("'") or t.startswith("N'"):
            toks_no_val.append("'value'")
        elif re.match(r"^[0-9]+$", t) and t not in ("1", "5", "10", "3"):
            toks_no_val.append("value")
        else:
            toks_no_val.append(t)
    # question toks
    q_toks = list(question)
    return {
        "db_id": "WP_M09",
        "question": question,
        "query": query,
        "query_toks": toks,
        "query_toks_no_value": toks_no_val,
        "question_toks": q_toks,
        "sql": {}
    }


samples = []

# ════════════════════════════════════════════════════════════════════════════
# Category 1: SELECT * patterns (~21 samples)
# ════════════════════════════════════════════════════════════════════════════

# isDel views: WP_vAcctIn, WP_vAcctOut, WP_vOutStock, WP_vTransfer
isDel_views = [
    ("WP_vAcctIn", "acctInId", ACCT_IN_IDS, "應收"),
    ("WP_vAcctOut", "acctOutId", ACCT_OUT_IDS, "應付"),
    ("WP_vOutStock", "OutStkId", OUTSTOCK_IDS, "出貨"),
    ("WP_vTransfer", "TransferId", TRANSFER_IDS, "調撥"),
]

for view, id_col, ids, label in isDel_views:
    for i in range(3):
        chosen_id = random.choice(ids)
        templates = [
            (f"Show the details of {label} order {chosen_id}",
             f"SELECT * FROM WP_M09.dbo.{view} WHERE {id_col}='{chosen_id}' AND isDel='N' AND dtlIsDel='N'"),
            (f"Display all information for {label} ID {chosen_id}",
             f"SELECT * FROM WP_M09.dbo.{view} WHERE {id_col}='{chosen_id}' AND isDel='N' AND dtlIsDel='N'"),
            (f"查詢{label}單號 {chosen_id} 的所有資料",
             f"SELECT * FROM WP_M09.dbo.{view} WHERE {id_col}='{chosen_id}' AND isDel='N' AND dtlIsDel='N'"),
        ]
        q, s = templates[i % 3]
        samples.append(make_sample(q, s))

# non-isDel views
for i in range(3):
    pn = random.choice(PRODUCT_NAMES)
    templates = [
        (f"Show all info about product {pn}",
         f"SELECT * FROM WP_M09.dbo.WP_vProduct WHERE pName=N'{pn}'"),
        (f"Display product details for {pn}",
         f"SELECT * FROM WP_M09.dbo.WP_vProduct WHERE pName=N'{pn}'"),
        (f"查詢商品 {pn} 的完整資料",
         f"SELECT * FROM WP_M09.dbo.WP_vProduct WHERE pName=N'{pn}'"),
    ]
    q, s = templates[i]
    samples.append(make_sample(q, s))

for i in range(3):
    sv = random.choice(SUPPLIER_NAMES)
    templates = [
        (f"Show all info about supplier {sv}",
         f"SELECT * FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'{sv}' AND isStop='N'"),
        (f"Display all details for supplier {sv}",
         f"SELECT * FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'{sv}' AND isStop='N'"),
        (f"列出供應商 {sv} 的所有資訊",
         f"SELECT * FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'{sv}' AND isStop='N'"),
    ]
    q, s = templates[i]
    samples.append(make_sample(q, s))

for i in range(3):
    pn = random.choice(PRODUCT_NAMES)
    wh = random.choice(WAREHOUSE_NAMES)
    templates = [
        (f"What is the inventory of {pn}?",
         f"SELECT WarehouseName, qty FROM WP_M09.dbo.WP_vInventory WHERE pName=N'{pn}'"),
        (f"Show inventory for {pn} in {wh}",
         f"SELECT pName, qty FROM WP_M09.dbo.WP_vInventory WHERE pName=N'{pn}' AND WarehouseName=N'{wh}'"),
        (f"查詢 {pn} 的庫存",
         f"SELECT WarehouseName, qty FROM WP_M09.dbo.WP_vInventory WHERE pName=N'{pn}'"),
    ]
    q, s = templates[i]
    samples.append(make_sample(q, s))

print(f"Category 1 (SELECT *): {len(samples)} samples")
cat1_count = len(samples)

# ════════════════════════════════════════════════════════════════════════════
# Category 2: Correct column selection (~60 samples)
# ════════════════════════════════════════════════════════════════════════════

cat2_start = len(samples)

# -- WP_vProvider (worst view) --
provider_templates = [
    ("List all active suppliers", "SELECT pvId, pvName FROM WP_M09.dbo.WP_vProvider WHERE isStop='N'"),
    ("Show supplier names and IDs", "SELECT pvId, pvName FROM WP_M09.dbo.WP_vProvider WHERE isStop='N'"),
    ("List all suppliers that are not stopped", "SELECT pvId, pvName FROM WP_M09.dbo.WP_vProvider WHERE isStop='N'"),
    ("Which suppliers are currently active?", "SELECT pvName FROM WP_M09.dbo.WP_vProvider WHERE isStop='N'"),
    ("Show all supplier names", "SELECT pvName FROM WP_M09.dbo.WP_vProvider WHERE isStop='N'"),
]
for q, s in provider_templates:
    samples.append(make_sample(q, s))

# Contact info
for sv in SUPPLIER_NAMES[:5]:
    samples.append(make_sample(
        f"Show contact info for supplier {sv}",
        f"SELECT pvName, ctactName, ctactTel FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'{sv}' AND isStop='N'"
    ))

# Bank details
for sv in SUPPLIER_NAMES[:4]:
    samples.append(make_sample(
        f"What are the bank details for {sv}?",
        f"SELECT pvName, bankName, bankAccount FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'{sv}' AND isStop='N'"
    ))

# Tax ID
for sv in SUPPLIER_NAMES[:4]:
    samples.append(make_sample(
        f"What is the tax ID of supplier {sv}?",
        f"SELECT taxId FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'{sv}' AND isStop='N'"
    ))

# Discount
for sv in SUPPLIER_NAMES[:3]:
    samples.append(make_sample(
        f"What discount does supplier {sv} offer?",
        f"SELECT pvName, pvDiscount FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'{sv}' AND isStop='N'"
    ))

# pvSn for JOIN
samples.append(make_sample(
    "Show supplier serial numbers and names",
    "SELECT pvSn, pvName FROM WP_M09.dbo.WP_vProvider WHERE isStop='N'"
))

# -- WP_vTransfer (second worst) --
for dt in DATES_8[:5]:
    samples.append(make_sample(
        f"List transfers on date {dt[:4]}/{dt[4:6]}/{dt[6:]}",
        f"SELECT DISTINCT TransferId FROM WP_M09.dbo.WP_vTransfer WHERE LEFT(TransferId,8)='{dt}' AND isDel='N'"
    ))

for wh in WAREHOUSE_NAMES[:4]:
    samples.append(make_sample(
        f"Which warehouse transferred products to {wh}?",
        f"SELECT DISTINCT fWhName FROM WP_M09.dbo.WP_vTransfer WHERE tfWhName=N'{wh}' AND isDel='N' AND dtlIsDel='N'"
    ))

for dt in DATES_8[:3]:
    samples.append(make_sample(
        f"Show transferred products and quantities on {dt[:4]}/{dt[4:6]}/{dt[6:]}",
        f"SELECT pName, qty FROM WP_M09.dbo.WP_vTransfer WHERE LEFT(TransferId,8)='{dt}' AND isDel='N' AND dtlIsDel='N'"
    ))

samples.append(make_sample(
    "What products were transferred from 台北倉 to 高雄倉?",
    "SELECT pName, qty FROM WP_M09.dbo.WP_vTransfer WHERE fWhName=N'台北倉' AND tfWhName=N'高雄倉' AND isDel='N' AND dtlIsDel='N'"
))

samples.append(make_sample(
    "Show the total quantity transferred per product",
    "SELECT pName, SUM(qty) AS totalQty FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName"
))

# -- WP_vAcctIn --
for dt in DATES_8[:3]:
    samples.append(make_sample(
        f"Show product details in receivable order {dt}0001",
        f"SELECT pNo, pName, qty, price, amount FROM WP_M09.dbo.WP_vAcctIn WHERE acctInId='{dt}0001' AND isDel='N' AND dtlIsDel='N'"
    ))

for mem in MEMBER_NAMES[:3]:
    samples.append(make_sample(
        f"What is the total receivable amount for member {mem}?",
        f"SELECT SUM(amount) AS totalAmount FROM (SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE memName=N'{mem}' AND isDel='N') sub"
    ))

# -- WP_vAcctOut --
for dt in DATES_8[:3]:
    samples.append(make_sample(
        f"Show product details in payable order {dt}0001",
        f"SELECT pNo, pName, qty, price, amount FROM WP_M09.dbo.WP_vAcctOut WHERE acctOutId='{dt}0001' AND isDel='N' AND dtlIsDel='N'"
    ))

for sv in SUPPLIER_NAMES[:3]:
    samples.append(make_sample(
        f"Total payable amount for supplier {sv}",
        f"SELECT SUM(amount) AS totalAmount FROM (SELECT DISTINCT acctOutId, amount FROM WP_M09.dbo.WP_vAcctOut WHERE pvName=N'{sv}' AND isDel='N') sub"
    ))

# -- WP_vOutStock --
for dt in DATES_8[:3]:
    samples.append(make_sample(
        f"Show details of shipment {dt}0001",
        f"SELECT pNo, pName, oStkDtlQty FROM WP_M09.dbo.WP_vOutStock WHERE OutStkId='{dt}0001' AND isDel='N' AND dtlIsDel='N'"
    ))

for wh in WAREHOUSE_NAMES[:3]:
    samples.append(make_sample(
        f"How many items were shipped from {wh}?",
        f"SELECT SUM(oStkDtlQty) AS totalQty FROM WP_M09.dbo.WP_vOutStock WHERE WhName=N'{wh}' AND isDel='N' AND dtlIsDel='N'"
    ))

print(f"Category 2 (Column selection): {len(samples) - cat2_start} samples")

# ════════════════════════════════════════════════════════════════════════════
# Category 3: TOP N patterns (~20 samples)
# ════════════════════════════════════════════════════════════════════════════

cat3_start = len(samples)

top_n_templates = [
    ("Show top 5 products by total receivable quantity",
     "SELECT TOP 5 pName, SUM(qty) AS totalQty FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY SUM(qty) DESC"),
    ("Which member has the highest receivable amount?",
     "SELECT TOP 1 memName, SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctInId, memName, amount FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N') sub GROUP BY memName ORDER BY SUM(sub.amount) DESC"),
    ("Top 3 suppliers by payable amount",
     "SELECT TOP 3 pvName, SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctOutId, pvName, amount FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N') sub GROUP BY pvName ORDER BY SUM(sub.amount) DESC"),
    ("Show the top 10 products by total shipped quantity",
     "SELECT TOP 10 pName, SUM(oStkDtlQty) AS totalQty FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY SUM(oStkDtlQty) DESC"),
    ("What are the top 5 products by transfer quantity?",
     "SELECT TOP 5 pName, SUM(qty) AS totalQty FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY SUM(qty) DESC"),
    ("Show top 3 warehouses by shipment count",
     "SELECT TOP 3 WhName, COUNT(DISTINCT OutStkId) AS orderCount FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' GROUP BY WhName ORDER BY COUNT(DISTINCT OutStkId) DESC"),
    ("Which 5 products have the highest inventory?",
     "SELECT TOP 5 pName, SUM(qty) AS totalQty FROM WP_M09.dbo.WP_vInventory GROUP BY pName ORDER BY SUM(qty) DESC"),
    ("Top 3 products with the most receivable orders",
     "SELECT TOP 3 pName, COUNT(DISTINCT acctInId) AS orderCount FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY COUNT(DISTINCT acctInId) DESC"),
    ("Show the supplier with the most payable orders",
     "SELECT TOP 1 pvName, COUNT(DISTINCT acctOutId) AS orderCount FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' GROUP BY pvName ORDER BY COUNT(DISTINCT acctOutId) DESC"),
    ("What are the top 5 members by number of receivable orders?",
     "SELECT TOP 5 memName, COUNT(DISTINCT acctInId) AS orderCount FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' GROUP BY memName ORDER BY COUNT(DISTINCT acctInId) DESC"),
]

for q, s in top_n_templates:
    samples.append(make_sample(q, s))

# Additional TOP N with different N values
more_top = [
    ("List the top 3 members by total receivable amount",
     "SELECT TOP 3 memName, SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctInId, memName, amount FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N') sub GROUP BY memName ORDER BY SUM(sub.amount) DESC"),
    ("Show top 5 suppliers by payable quantity",
     "SELECT TOP 5 pvName, SUM(qty) AS totalQty FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' GROUP BY pvName ORDER BY SUM(qty) DESC"),
    ("Which warehouse has the most inventory?",
     "SELECT TOP 1 WarehouseName, SUM(qty) AS totalQty FROM WP_M09.dbo.WP_vInventory GROUP BY WarehouseName ORDER BY SUM(qty) DESC"),
    ("Top 10 products by inventory quantity",
     "SELECT TOP 10 pName, SUM(qty) AS totalQty FROM WP_M09.dbo.WP_vInventory GROUP BY pName ORDER BY SUM(qty) DESC"),
    ("Show the 3 most transferred products this year",
     "SELECT TOP 3 pName, SUM(qty) AS totalQty FROM WP_M09.dbo.WP_vTransfer WHERE LEFT(TransferId,4)='2025' AND isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY SUM(qty) DESC"),
    ("Which product has the highest payable amount?",
     "SELECT TOP 1 pName, SUM(dtlAmount) AS totalAmount FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY SUM(dtlAmount) DESC"),
    ("Top 5 products shipped from 台北倉",
     "SELECT TOP 5 pName, SUM(oStkDtlQty) AS totalQty FROM WP_M09.dbo.WP_vOutStock WHERE WhName=N'台北倉' AND isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY SUM(oStkDtlQty) DESC"),
    ("Show the 3 highest receivable orders by amount",
     "SELECT TOP 3 acctInId, amount FROM (SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N') sub ORDER BY amount DESC"),
    ("Which 5 products have the lowest inventory?",
     "SELECT TOP 5 pName, SUM(qty) AS totalQty FROM WP_M09.dbo.WP_vInventory GROUP BY pName ORDER BY SUM(qty) ASC"),
    ("Top 3 warehouses by transfer quantity",
     "SELECT TOP 3 fWhName, SUM(qty) AS totalQty FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY fWhName ORDER BY SUM(qty) DESC"),
]

for q, s in more_top:
    samples.append(make_sample(q, s))

print(f"Category 3 (TOP N): {len(samples) - cat3_start} samples")

# ════════════════════════════════════════════════════════════════════════════
# Category 4: COUNT(DISTINCT xxxId) patterns (~20 samples)
# ════════════════════════════════════════════════════════════════════════════

cat4_start = len(samples)

# WP_vAcctIn
for yr in YEARS:
    samples.append(make_sample(
        f"How many receivable orders in {yr}?",
        f"SELECT COUNT(DISTINCT acctInId) FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND LEFT(acctInId,4)='{yr}'"
    ))
for ym in DATES_6[:3]:
    samples.append(make_sample(
        f"How many receivable orders in {ym[:4]}/{ym[4:]}?",
        f"SELECT COUNT(DISTINCT acctInId) FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND LEFT(acctInId,6)='{ym}'"
    ))

# WP_vAcctOut
for yr in YEARS:
    samples.append(make_sample(
        f"How many payable orders in {yr}?",
        f"SELECT COUNT(DISTINCT acctOutId) FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' AND LEFT(acctOutId,4)='{yr}'"
    ))
samples.append(make_sample(
    "Count all payable orders for supplier 花草堂",
    "SELECT COUNT(DISTINCT acctOutId) FROM WP_M09.dbo.WP_vAcctOut WHERE pvName=N'花草堂' AND isDel='N'"
))

# WP_vOutStock
for yr in YEARS:
    samples.append(make_sample(
        f"How many shipment orders in {yr}?",
        f"SELECT COUNT(DISTINCT OutStkId) FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND LEFT(OutStkId,4)='{yr}'"
    ))
samples.append(make_sample(
    "Count shipments from 台北倉 in 2025",
    "SELECT COUNT(DISTINCT OutStkId) FROM WP_M09.dbo.WP_vOutStock WHERE WhName=N'台北倉' AND isDel='N' AND LEFT(OutStkId,4)='2025'"
))

# WP_vTransfer
for yr in YEARS:
    samples.append(make_sample(
        f"How many transfer orders in {yr}?",
        f"SELECT COUNT(DISTINCT TransferId) FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N' AND LEFT(TransferId,4)='{yr}'"
    ))
samples.append(make_sample(
    "How many transfers were made?",
    "SELECT COUNT(DISTINCT TransferId) FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N'"
))
samples.append(make_sample(
    "Count distinct products in receivable orders",
    "SELECT COUNT(DISTINCT pNo) FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N'"
))
samples.append(make_sample(
    "How many unique products were shipped?",
    "SELECT COUNT(DISTINCT pNo) FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND dtlIsDel='N'"
))
samples.append(make_sample(
    "How many distinct members have receivable orders in 2025?",
    "SELECT COUNT(DISTINCT memName) FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND LEFT(acctInId,4)='2025'"
))

print(f"Category 4 (COUNT DISTINCT): {len(samples) - cat4_start} samples")

# ════════════════════════════════════════════════════════════════════════════
# Category 5: Subquery dedup reinforcement (~40 samples)
# ════════════════════════════════════════════════════════════════════════════

cat5_start = len(samples)

# GROUP BY with subquery dedup
for sv in SUPPLIER_NAMES[:5]:
    samples.append(make_sample(
        f"Total payable amount per supplier",
        f"SELECT pvName, SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctOutId, pvName, amount FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N') sub GROUP BY pvName"
    ))
    break  # only 1 generic

for mem in MEMBER_NAMES[:5]:
    samples.append(make_sample(
        f"Total receivable amount for each member",
        f"SELECT memName, SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctInId, memName, amount FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N') sub GROUP BY memName"
    ))
    break

# Specific member totals
for mem in MEMBER_NAMES[:5]:
    samples.append(make_sample(
        f"What is the total receivable for {mem}?",
        f"SELECT SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE memName=N'{mem}' AND isDel='N') sub"
    ))

# Specific supplier totals
for sv in SUPPLIER_NAMES[:5]:
    samples.append(make_sample(
        f"What is the total payable for {sv}?",
        f"SELECT SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctOutId, amount FROM WP_M09.dbo.WP_vAcctOut WHERE pvName=N'{sv}' AND isDel='N') sub"
    ))

# HAVING
samples.append(make_sample(
    "Which members have total receivable over 10000?",
    "SELECT memName, SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctInId, memName, amount FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N') sub GROUP BY memName HAVING SUM(sub.amount) > 10000"
))
samples.append(make_sample(
    "Which suppliers have total payable over 50000?",
    "SELECT pvName, SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctOutId, pvName, amount FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N') sub GROUP BY pvName HAVING SUM(sub.amount) > 50000"
))
samples.append(make_sample(
    "Members with more than 5 receivable orders",
    "SELECT memName, COUNT(*) AS orderCount FROM (SELECT DISTINCT acctInId, memName FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N') sub GROUP BY memName HAVING COUNT(*) > 5"
))
samples.append(make_sample(
    "Suppliers with payable amount over 100000",
    "SELECT pvName, SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctOutId, pvName, amount FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N') sub GROUP BY pvName HAVING SUM(sub.amount) > 100000"
))

# ORDER BY + TOP with subquery
samples.append(make_sample(
    "Top 5 members by total receivable amount",
    "SELECT TOP 5 memName, SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctInId, memName, amount FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N') sub GROUP BY memName ORDER BY SUM(sub.amount) DESC"
))
samples.append(make_sample(
    "Top 3 suppliers by total payable amount",
    "SELECT TOP 3 pvName, SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctOutId, pvName, amount FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N') sub GROUP BY pvName ORDER BY SUM(sub.amount) DESC"
))

# AVG with subquery
samples.append(make_sample(
    "What is the average receivable order amount?",
    "SELECT AVG(sub.amount) AS avgAmount FROM (SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N') sub"
))
samples.append(make_sample(
    "Average payable order amount per supplier",
    "SELECT pvName, AVG(sub.amount) AS avgAmount FROM (SELECT DISTINCT acctOutId, pvName, amount FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N') sub GROUP BY pvName"
))
samples.append(make_sample(
    "What is the average shipment amount?",
    "SELECT AVG(sub.amount) AS avgAmount FROM (SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N') sub"
))

# Monthly aggregation with subquery (nested month)
for yr in YEARS:
    samples.append(make_sample(
        f"Show monthly receivable totals for {yr}",
        f"SELECT LEFT(sub.acctInId,6) AS month, SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND LEFT(acctInId,4)='{yr}') sub GROUP BY LEFT(sub.acctInId,6) ORDER BY LEFT(sub.acctInId,6)"
    ))
    samples.append(make_sample(
        f"Monthly payable totals for {yr}",
        f"SELECT LEFT(sub.acctOutId,6) AS month, SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctOutId, amount FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' AND LEFT(acctOutId,4)='{yr}') sub GROUP BY LEFT(sub.acctOutId,6) ORDER BY LEFT(sub.acctOutId,6)"
    ))
    samples.append(make_sample(
        f"Monthly shipment totals for {yr}",
        f"SELECT LEFT(sub.OutStkId,6) AS month, SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND LEFT(OutStkId,4)='{yr}') sub GROUP BY LEFT(sub.OutStkId,6) ORDER BY LEFT(sub.OutStkId,6)"
    ))

# Date-filtered subquery dedup
for dt in DATES_8[:4]:
    samples.append(make_sample(
        f"Total receivable amount on {dt[:4]}/{dt[4:6]}/{dt[6:]}",
        f"SELECT SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE LEFT(acctInId,8)='{dt}' AND isDel='N') sub"
    ))

# MIN/MAX with subquery
samples.append(make_sample(
    "What is the largest receivable order amount?",
    "SELECT MAX(sub.amount) AS maxAmount FROM (SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N') sub"
))
samples.append(make_sample(
    "What is the smallest payable order amount?",
    "SELECT MIN(sub.amount) AS minAmount FROM (SELECT DISTINCT acctOutId, amount FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N') sub"
))

# Transfer subquery dedup
samples.append(make_sample(
    "Total transfer quantity per product",
    "SELECT pName, SUM(sub.qty) AS totalQty FROM (SELECT DISTINCT TransferId, pName, qty FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N' AND dtlIsDel='N') sub GROUP BY pName"
))
samples.append(make_sample(
    "Average transfer quantity per order",
    "SELECT AVG(sub.totalQty) AS avgQty FROM (SELECT TransferId, SUM(qty) AS totalQty FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY TransferId) sub"
))

print(f"Category 5 (Subquery dedup): {len(samples) - cat5_start} samples")

# ════════════════════════════════════════════════════════════════════════════
# Category 6: No-isDel views reinforcement (~20 samples)
# ════════════════════════════════════════════════════════════════════════════

cat6_start = len(samples)

# WP_vProduct -- NO isDel!
samples.append(make_sample("List all products", "SELECT pNo, pName FROM WP_M09.dbo.WP_vProduct"))
samples.append(make_sample("Show all product names", "SELECT pName FROM WP_M09.dbo.WP_vProduct"))
samples.append(make_sample("How many products are there?", "SELECT COUNT(*) FROM WP_M09.dbo.WP_vProduct"))
for pn in PRODUCT_NAMES[:3]:
    samples.append(make_sample(
        f"What is the product number for {pn}?",
        f"SELECT pNo FROM WP_M09.dbo.WP_vProduct WHERE pName=N'{pn}'"
    ))

# WP_vInventory -- NO isDel!
samples.append(make_sample("Show all inventory", "SELECT pName, WarehouseName, qty FROM WP_M09.dbo.WP_vInventory"))
samples.append(make_sample("List inventory by warehouse", "SELECT WarehouseName, pName, qty FROM WP_M09.dbo.WP_vInventory ORDER BY WarehouseName"))
for wh in WAREHOUSE_NAMES[:3]:
    samples.append(make_sample(
        f"Show inventory in {wh}",
        f"SELECT pName, qty FROM WP_M09.dbo.WP_vInventory WHERE WarehouseName=N'{wh}'"
    ))
for pn in PRODUCT_NAMES[:3]:
    samples.append(make_sample(
        f"Total inventory for {pn} across all warehouses",
        f"SELECT SUM(qty) AS totalQty FROM WP_M09.dbo.WP_vInventory WHERE pName=N'{pn}'"
    ))

# WP_vProvider -- use isStop, NOT isDel!
samples.append(make_sample("List active suppliers", "SELECT pvName FROM WP_M09.dbo.WP_vProvider WHERE isStop='N'"))
samples.append(make_sample("Show stopped suppliers", "SELECT pvName FROM WP_M09.dbo.WP_vProvider WHERE isStop='Y'"))
samples.append(make_sample("How many active suppliers?", "SELECT COUNT(*) FROM WP_M09.dbo.WP_vProvider WHERE isStop='N'"))
samples.append(make_sample("How many suppliers are stopped?", "SELECT COUNT(*) FROM WP_M09.dbo.WP_vProvider WHERE isStop='Y'"))

# Extra no-isDel reinforcement
samples.append(make_sample("List all product numbers and names", "SELECT pNo, pName FROM WP_M09.dbo.WP_vProduct"))
samples.append(make_sample("Show total inventory per warehouse", "SELECT WarehouseName, SUM(qty) AS totalQty FROM WP_M09.dbo.WP_vInventory GROUP BY WarehouseName"))
for pn in PRODUCT_NAMES[3:6]:
    samples.append(make_sample(
        f"Which warehouses stock {pn}?",
        f"SELECT WarehouseName FROM WP_M09.dbo.WP_vInventory WHERE pName=N'{pn}'"
    ))
for sv in SUPPLIER_NAMES[3:6]:
    samples.append(make_sample(
        f"Is supplier {sv} active?",
        f"SELECT pvName, isStop FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'{sv}'"
    ))
samples.append(make_sample(
    "Show all supplier discounts",
    "SELECT pvName, pvDiscount FROM WP_M09.dbo.WP_vProvider WHERE isStop='N'"
))
samples.append(make_sample(
    "Which products have zero inventory?",
    "SELECT pName, WarehouseName, qty FROM WP_M09.dbo.WP_vInventory WHERE qty=0"
))
samples.append(make_sample(
    "Count products per warehouse",
    "SELECT WarehouseName, COUNT(*) AS productCount FROM WP_M09.dbo.WP_vInventory GROUP BY WarehouseName"
))

# Extra COUNT DISTINCT
samples.append(make_sample(
    "How many distinct suppliers in payable orders?",
    "SELECT COUNT(DISTINCT pvName) FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N'"
))
samples.append(make_sample(
    "How many warehouses have shipment orders?",
    "SELECT COUNT(DISTINCT WhName) FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N'"
))
samples.append(make_sample(
    "How many receivable orders on 20250310?",
    "SELECT COUNT(DISTINCT acctInId) FROM WP_M09.dbo.WP_vAcctIn WHERE LEFT(acctInId,8)='20250310' AND isDel='N'"
))

print(f"Category 6 (No-isDel views): {len(samples) - cat6_start} samples")

# ════════════════════════════════════════════════════════════════════════════
# Category 7: Chinese questions (~20+ samples)
# ════════════════════════════════════════════════════════════════════════════

cat7_start = len(samples)

chinese_templates = [
    ("列出所有應收單號", "SELECT DISTINCT acctInId FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N'"),
    ("2025年有幾筆應付單？", "SELECT COUNT(DISTINCT acctOutId) FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' AND LEFT(acctOutId,4)='2025'"),
    ("查詢台北倉的庫存", "SELECT pName, qty FROM WP_M09.dbo.WP_vInventory WHERE WarehouseName=N'台北倉'"),
    ("哪些供應商是有效的？", "SELECT pvName FROM WP_M09.dbo.WP_vProvider WHERE isStop='N'"),
    ("列出所有商品名稱", "SELECT pName FROM WP_M09.dbo.WP_vProduct"),
    ("2025年01月的應收總額是多少？",
     "SELECT SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE LEFT(acctInId,6)='202501' AND isDel='N') sub"),
    ("從台北倉調撥到高雄倉的商品有哪些？",
     "SELECT pName, qty FROM WP_M09.dbo.WP_vTransfer WHERE fWhName=N'台北倉' AND tfWhName=N'高雄倉' AND isDel='N' AND dtlIsDel='N'"),
    ("花草堂的應付總額是多少？",
     "SELECT SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctOutId, amount FROM WP_M09.dbo.WP_vAcctOut WHERE pvName=N'花草堂' AND isDel='N') sub"),
    ("王小明有幾筆應收單？",
     "SELECT COUNT(DISTINCT acctInId) FROM WP_M09.dbo.WP_vAcctIn WHERE memName=N'王小明' AND isDel='N'"),
    ("每個會員的應收總額是多少？",
     "SELECT memName, SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctInId, memName, amount FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N') sub GROUP BY memName"),
    ("前五名應收金額最高的會員",
     "SELECT TOP 5 memName, SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctInId, memName, amount FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N') sub GROUP BY memName ORDER BY SUM(sub.amount) DESC"),
    ("2025年每月的出貨金額",
     "SELECT LEFT(sub.OutStkId,6) AS month, SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND LEFT(OutStkId,4)='2025') sub GROUP BY LEFT(sub.OutStkId,6) ORDER BY LEFT(sub.OutStkId,6)"),
    ("薰衣草精油的庫存有多少？",
     "SELECT WarehouseName, qty FROM WP_M09.dbo.WP_vInventory WHERE pName=N'薰衣草精油'"),
    ("天然坊的聯絡人是誰？",
     "SELECT ctactName, ctactTel FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'天然坊' AND isStop='N'"),
    ("有多少種商品？",
     "SELECT COUNT(*) FROM WP_M09.dbo.WP_vProduct"),
    ("哪個倉庫的庫存最多？",
     "SELECT TOP 1 WarehouseName, SUM(qty) AS totalQty FROM WP_M09.dbo.WP_vInventory GROUP BY WarehouseName ORDER BY SUM(qty) DESC"),
    ("查詢20250310的調撥單",
     "SELECT DISTINCT TransferId FROM WP_M09.dbo.WP_vTransfer WHERE LEFT(TransferId,8)='20250310' AND isDel='N'"),
    ("出貨單202501010001的商品明細",
     "SELECT pNo, pName, oStkDtlQty FROM WP_M09.dbo.WP_vOutStock WHERE OutStkId='202501010001' AND isDel='N' AND dtlIsDel='N'"),
    ("供應商花草堂的統一編號是什麼？",
     "SELECT taxId FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'花草堂' AND isStop='N'"),
    ("2025年的調撥單有幾筆？",
     "SELECT COUNT(DISTINCT TransferId) FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N' AND LEFT(TransferId,4)='2025'"),
    ("應收金額超過50000的會員有哪些？",
     "SELECT memName, SUM(sub.amount) AS totalAmount FROM (SELECT DISTINCT acctInId, memName, amount FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N') sub GROUP BY memName HAVING SUM(sub.amount) > 50000"),
    ("列出停用的供應商",
     "SELECT pvName FROM WP_M09.dbo.WP_vProvider WHERE isStop='Y'"),
]

for q, s in chinese_templates:
    samples.append(make_sample(q, s))

print(f"Category 7 (Chinese questions): {len(samples) - cat7_start} samples")

# ════════════════════════════════════════════════════════════════════════════
# Save
# ════════════════════════════════════════════════════════════════════════════

output_path = os.path.join("data", "wp_m09", "train_augment_v2.json")
os.makedirs(os.path.dirname(output_path), exist_ok=True)

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(samples, f, ensure_ascii=False, indent=2)

print(f"\n{'='*50}")
print(f"Total samples generated: {len(samples)}")
print(f"Saved to: {output_path}")
print(f"{'='*50}")
print(f"\nBreakdown:")
print(f"  Cat 1 (SELECT *):          {cat1_count}")
print(f"  Cat 2 (Column selection):   {cat2_start + (len(samples) - cat2_start) - (len(samples) - cat2_start) + (cat3_start - cat2_start)}")
print(f"  Cat 3 (TOP N):              {cat4_start - cat3_start}")
print(f"  Cat 4 (COUNT DISTINCT):     {cat5_start - cat4_start}")
print(f"  Cat 5 (Subquery dedup):     {cat6_start - cat5_start}")
print(f"  Cat 6 (No-isDel views):     {cat7_start - cat6_start}")
print(f"  Cat 7 (Chinese questions):  {len(samples) - cat7_start}")
