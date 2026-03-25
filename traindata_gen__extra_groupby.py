"""
補充大量 GROUP BY 和 TOP+GROUP BY 模板，合併進 v3 訓練集。
目標：GROUP BY ~300 筆, TOP+GROUP BY ~200 筆
"""
import json, pyodbc, random, sys, io
from collections import Counter

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
random.seed(42)

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=SHANE\\SQLEXPRESS;DATABASE=WP_M09;Trusted_Connection=yes;",
    timeout=30,
)


def fv(sql):
    c = conn.cursor(); c.execute(sql)
    return [row[0] for row in c.fetchall() if row[0] is not None]


WAREHOUSES = fv("SELECT DISTINCT WarehouseName FROM WP_M09.dbo.WP_vInventory WHERE WarehouseName IS NOT NULL")
AO_MONTHS = fv("SELECT DISTINCT LEFT(acctOutId,6) FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N'")
OS_MONTHS = fv("SELECT DISTINCT LEFT(OutStkId,6) FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N'")
TF_MONTHS = fv("SELECT DISTINCT LEFT(TransferId,6) FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N'")
AI_MONTHS = fv("SELECT DISTINCT LEFT(acctInId,6) FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N'")
AO_PVS = fv("SELECT DISTINCT TOP 80 pvName FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' AND pvName IS NOT NULL AND pvName<>''")
INV_PVS = fv("SELECT DISTINCT TOP 80 pvName FROM WP_M09.dbo.WP_vInventory WHERE pvName IS NOT NULL AND pvName<>''")

def fm(m): return f"{m[:4]}年{int(m[4:])}月"

T = []

# ═══════════════════════════════════════════
# GROUP BY 模板 — 大量增加
# ═══════════════════════════════════════════

# -- WP_vAcctIn --
V = "WP_M09.dbo.WP_vAcctIn"; F = "isDel='N' AND dtlIsDel='N'"
T += [
    ("收款單按員工統計金額", f"SELECT empId, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY empId ORDER BY total DESC;"),
    ("收款單按員工統計筆數", f"SELECT empId, COUNT(DISTINCT acctInId) AS cnt FROM {V} WHERE {F} GROUP BY empId;"),
    ("各收款單的商品數量統計", f"SELECT acctInId, COUNT(DISTINCT pName) AS prod_cnt FROM {V} WHERE {F} GROUP BY acctInId ORDER BY prod_cnt DESC;"),
    ("各收款單的明細金額合計", f"SELECT acctInId, SUM(oStkDtlAmtTotal) AS total FROM {V} WHERE {F} GROUP BY acctInId ORDER BY total DESC;"),
    ("各月收款的最大單筆金額", f"SELECT LEFT(acctInId,6) AS ym, MAX(amount) AS max_amt FROM {V} WHERE {F} GROUP BY LEFT(acctInId,6) ORDER BY ym;"),
    ("各月收款的最小單筆金額", f"SELECT LEFT(acctInId,6) AS ym, MIN(amount) AS min_amt FROM {V} WHERE {F} GROUP BY LEFT(acctInId,6) ORDER BY ym;"),
]

# -- WP_vAcctOut --
V = "WP_M09.dbo.WP_vAcctOut"; F = "isDel='N' AND dtlIsDel='N'"
T += [
    ("每月的最大付款金額", f"SELECT LEFT(acctOutId,6) AS ym, MAX(amount) AS max_amt FROM {V} WHERE {F} GROUP BY LEFT(acctOutId,6) ORDER BY ym;"),
    ("每月的最小付款金額", f"SELECT LEFT(acctOutId,6) AS ym, MIN(amount) AS min_amt FROM {V} WHERE {F} GROUP BY LEFT(acctOutId,6) ORDER BY ym;"),
    ("每個供應商的最大付款金額", f"SELECT pvName, MAX(amount) AS max_amt FROM {V} WHERE {F} GROUP BY pvName ORDER BY max_amt DESC;"),
    ("每個供應商的最小付款金額", f"SELECT pvName, MIN(amount) AS min_amt FROM {V} WHERE {F} GROUP BY pvName ORDER BY min_amt ASC;"),
    ("各員工處理的付款總額", f"SELECT empId, empName, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY empId, empName ORDER BY total DESC;"),
    ("每個商品的進貨次數", f"SELECT pName, COUNT(*) AS cnt FROM {V} WHERE {F} GROUP BY pName ORDER BY cnt DESC;"),
    ("每個商品的平均進貨單價", f"SELECT pName, AVG(amtTotal) AS avg_price FROM {V} WHERE {F} GROUP BY pName ORDER BY avg_price DESC;"),
    ("各月付款的商品種類數", f"SELECT LEFT(acctOutId,6) AS ym, COUNT(DISTINCT pName) AS prod_cnt FROM {V} WHERE {F} GROUP BY LEFT(acctOutId,6) ORDER BY ym;"),
    ("含稅與未稅的付款筆數", f"SELECT isTax, COUNT(*) AS cnt FROM {V} WHERE {F} GROUP BY isTax;"),
    ("含稅與未稅的付款金額", f"SELECT isTax, SUM(amtTotal) AS total FROM {V} WHERE {F} GROUP BY isTax;"),
]
# 每月+每供應商
for m in AO_MONTHS:
    T.append((f"{fm(m)}各供應商的付款總額", f"SELECT pvName, SUM(amount) AS total FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}' GROUP BY pvName ORDER BY total DESC;"))
    T.append((f"{fm(m)}各供應商的付款筆數", f"SELECT pvName, COUNT(DISTINCT acctOutId) AS cnt FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}' GROUP BY pvName ORDER BY cnt DESC;"))
    T.append((f"{fm(m)}各商品的進貨數量", f"SELECT pName, SUM(qty) AS total_qty FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}' GROUP BY pName ORDER BY total_qty DESC;"))

# -- WP_vOutStock --
V = "WP_M09.dbo.WP_vOutStock"; F = "isDel='N' AND dtlIsDel='N'"
T += [
    ("每月的最大銷售金額", f"SELECT LEFT(OutStkId,6) AS ym, MAX(amount) AS max_amt FROM {V} WHERE {F} GROUP BY LEFT(OutStkId,6) ORDER BY ym;"),
    ("每月的最小銷售金額", f"SELECT LEFT(OutStkId,6) AS ym, MIN(amount) AS min_amt FROM {V} WHERE {F} GROUP BY LEFT(OutStkId,6) ORDER BY ym;"),
    ("各員工的銷售總額", f"SELECT empId, empName, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY empId, empName ORDER BY total DESC;"),
    ("各員工的銷售筆數", f"SELECT empId, empName, COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} GROUP BY empId, empName ORDER BY cnt DESC;"),
    ("每個商品的銷售次數", f"SELECT pName, COUNT(*) AS cnt FROM {V} WHERE {F} GROUP BY pName ORDER BY cnt DESC;"),
    ("每個商品的平均銷售數量", f"SELECT pName, AVG(qty) AS avg_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY avg_qty DESC;"),
    ("每個商品的平均銷售單價", f"SELECT pName, AVG(dtlAmt) AS avg_price FROM {V} WHERE {F} GROUP BY pName ORDER BY avg_price DESC;"),
    ("各倉庫的銷售筆數", f"SELECT whSn, COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} GROUP BY whSn ORDER BY cnt DESC;"),
    ("各倉庫的銷售總額排名", f"SELECT whSn, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY whSn ORDER BY total DESC;"),
    ("有無折扣的銷售筆數", f"SELECT CASE WHEN dtlDiscnt>0 THEN 'Y' ELSE 'N' END AS has_discount, COUNT(*) AS cnt FROM {V} WHERE {F} GROUP BY CASE WHEN dtlDiscnt>0 THEN 'Y' ELSE 'N' END;"),
    ("每個會員買了幾種商品", f"SELECT memName, COUNT(DISTINCT pName) AS prod_cnt FROM {V} WHERE {F} AND memName IS NOT NULL AND memName<>'' GROUP BY memName ORDER BY prod_cnt DESC;"),
    ("每個會員的消費次數", f"SELECT memName, COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} AND memName IS NOT NULL AND memName<>'' GROUP BY memName ORDER BY cnt DESC;"),
    ("各銷售類型的銷售金額", f"SELECT outType, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY outType ORDER BY total DESC;"),
]
# 每月統計
for m in OS_MONTHS:
    T += [
        (f"{fm(m)}各商品的銷售數量", f"SELECT pName, SUM(qty) AS total_qty FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}' GROUP BY pName ORDER BY total_qty DESC;"),
        (f"{fm(m)}各商品的銷售金額", f"SELECT pName, SUM(amtTotal) AS total FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}' GROUP BY pName ORDER BY total DESC;"),
        (f"{fm(m)}各員工的銷售筆數", f"SELECT empId, COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}' GROUP BY empId ORDER BY cnt DESC;"),
        (f"{fm(m)}各會員的消費金額", f"SELECT memName, SUM(amount) AS total FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}' AND memName IS NOT NULL AND memName<>'' GROUP BY memName ORDER BY total DESC;"),
    ]

# -- WP_vTransfer --
V = "WP_M09.dbo.WP_vTransfer"; F = "isDel='N' AND dtlIsDel='N'"
T += [
    ("各員工的調撥筆數", f"SELECT empId, COUNT(DISTINCT TransferId) AS cnt FROM {V} WHERE {F} GROUP BY empId ORDER BY cnt DESC;"),
    ("各員工的調撥總量", f"SELECT empId, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY empId ORDER BY total_qty DESC;"),
    ("每月調撥的總數量", f"SELECT LEFT(TransferId,6) AS ym, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY LEFT(TransferId,6) ORDER BY ym;"),
    ("每月調撥的總成本", f"SELECT LEFT(TransferId,6) AS ym, SUM(qty * costAvg) AS total_cost FROM {V} WHERE {F} GROUP BY LEFT(TransferId,6) ORDER BY ym;"),
    ("各倉庫間的調撥數量", f"SELECT fWhName, tfWhName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY fWhName, tfWhName ORDER BY total_qty DESC;"),
    ("各倉庫的調撥商品種類數", f"SELECT fWhName, COUNT(DISTINCT pName) AS prod_cnt FROM {V} WHERE {F} GROUP BY fWhName ORDER BY prod_cnt DESC;"),
]
for m in TF_MONTHS:
    T += [
        (f"{fm(m)}各倉庫調出的數量", f"SELECT fWhName, SUM(qty) AS total FROM {V} WHERE {F} AND LEFT(TransferId,6)='{m}' GROUP BY fWhName ORDER BY total DESC;"),
        (f"{fm(m)}各商品的調撥數量", f"SELECT pName, SUM(qty) AS total_qty FROM {V} WHERE {F} AND LEFT(TransferId,6)='{m}' GROUP BY pName ORDER BY total_qty DESC;"),
    ]

# -- WP_vInventory --
V = "WP_M09.dbo.WP_vInventory"
T += [
    ("各倉庫的平均庫存量", f"SELECT WarehouseName, AVG(qty) AS avg_qty FROM {V} GROUP BY WarehouseName ORDER BY avg_qty DESC;"),
    ("各倉庫的最大庫存商品量", f"SELECT WarehouseName, MAX(qty) AS max_qty FROM {V} GROUP BY WarehouseName ORDER BY max_qty DESC;"),
    ("各供應商的庫存總金額", f"SELECT pvName, SUM(qty * costAvg) AS total_cost FROM {V} WHERE pvName IS NOT NULL AND qty>0 GROUP BY pvName ORDER BY total_cost DESC;"),
    ("各倉庫庫存為零的商品數", f"SELECT WarehouseName, COUNT(*) AS cnt FROM {V} WHERE qty=0 GROUP BY WarehouseName ORDER BY cnt DESC;"),
    ("各倉庫可銷售的商品數", f"SELECT WarehouseName, COUNT(*) AS cnt FROM {V} WHERE isSale='Y' AND qty>0 GROUP BY WarehouseName ORDER BY cnt DESC;"),
    ("各供應商的平均庫存量", f"SELECT pvName, AVG(qty) AS avg_qty FROM {V} WHERE pvName IS NOT NULL GROUP BY pvName ORDER BY avg_qty DESC;"),
    ("各倉庫的庫存安全警示數", f"SELECT WarehouseName, COUNT(*) AS cnt FROM {V} WHERE qty < qtySafe AND qtySafe > 0 GROUP BY WarehouseName ORDER BY cnt DESC;"),
]
for wh in WAREHOUSES:
    T += [
        (f"「{wh}」各供應商的庫存量", f"SELECT pvName, SUM(qty) AS total_qty FROM {V} WHERE WarehouseName=N'{wh}' AND pvName IS NOT NULL GROUP BY pvName ORDER BY total_qty DESC;"),
        (f"「{wh}」各商品的庫存金額", f"SELECT pName, qty * costAvg AS cost FROM {V} WHERE WarehouseName=N'{wh}' AND qty>0 ORDER BY cost DESC;"),
    ]

# -- WP_vProduct --
V = "WP_M09.dbo.WP_vProduct"
T += [
    ("各供應商的商品總庫存", f"SELECT pvName, SUM(qtyNow) AS total_qty FROM {V} GROUP BY pvName ORDER BY total_qty DESC;"),
    ("各供應商的商品平均成本", f"SELECT pvName, AVG(costStd) AS avg_cost FROM {V} GROUP BY pvName ORDER BY avg_cost DESC;"),
    ("各單位類別的平均售價", f"SELECT pUName, AVG(priceStd) AS avg_price FROM {V} GROUP BY pUName ORDER BY avg_price DESC;"),
    ("含稅與免稅的商品數", f"SELECT isTax, COUNT(*) AS cnt FROM {V} GROUP BY isTax;"),
    ("可銷售與不可銷售的商品數", f"SELECT isSale, COUNT(*) AS cnt FROM {V} GROUP BY isSale;"),
    ("有無供應商折扣的商品數", f"SELECT isPvDiscount, COUNT(*) AS cnt FROM {V} GROUP BY isPvDiscount;"),
    ("各供應商的最高售價商品", f"SELECT pvName, MAX(priceStd) AS max_price FROM {V} GROUP BY pvName ORDER BY max_price DESC;"),
    ("各供應商的最低成本商品", f"SELECT pvName, MIN(costStd) AS min_cost FROM {V} WHERE costStd>0 GROUP BY pvName ORDER BY min_cost ASC;"),
]

# -- WP_vProvider --
V = "WP_M09.dbo.WP_vProvider"
T += [
    ("各縣市的供應商名單", f"SELECT pvCity, pvName FROM {V} WHERE sn > 0 ORDER BY pvCity, pvName;"),
    ("各供應商類別的名單", f"SELECT pvKName, pvName FROM {V} WHERE sn > 0 ORDER BY pvKName, pvName;"),
]


# ═══════════════════════════════════════════
# TOP + GROUP BY 模板 — 大量增加
# ═══════════════════════════════════════════

# -- WP_vAcctIn --
V = "WP_M09.dbo.WP_vAcctIn"; F = "isDel='N' AND dtlIsDel='N'"
T += [
    ("收款金額最高的前3個月份", f"SELECT TOP 3 LEFT(acctInId,6) AS ym, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY LEFT(acctInId,6) ORDER BY total DESC;"),
    ("收款筆數最多的月份", f"SELECT TOP 1 LEFT(acctInId,6) AS ym, COUNT(DISTINCT acctInId) AS cnt FROM {V} WHERE {F} GROUP BY LEFT(acctInId,6) ORDER BY cnt DESC;"),
]

# -- WP_vAcctOut --
V = "WP_M09.dbo.WP_vAcctOut"; F = "isDel='N' AND dtlIsDel='N'"
T += [
    ("付款金額最高的月份", f"SELECT TOP 1 LEFT(acctOutId,6) AS ym, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY LEFT(acctOutId,6) ORDER BY total DESC;"),
    ("付款金額最高的前3個月", f"SELECT TOP 3 LEFT(acctOutId,6) AS ym, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY LEFT(acctOutId,6) ORDER BY total DESC;"),
    ("付款筆數最多的月份", f"SELECT TOP 1 LEFT(acctOutId,6) AS ym, COUNT(DISTINCT acctOutId) AS cnt FROM {V} WHERE {F} GROUP BY LEFT(acctOutId,6) ORDER BY cnt DESC;"),
    ("平均付款金額最高的供應商", f"SELECT TOP 1 pvName, AVG(amount) AS avg_amt FROM {V} WHERE {F} GROUP BY pvName ORDER BY avg_amt DESC;"),
    ("平均付款金額最高的前5個供應商", f"SELECT TOP 5 pvName, AVG(amount) AS avg_amt FROM {V} WHERE {F} GROUP BY pvName ORDER BY avg_amt DESC;"),
    ("進貨數量最多的前5個商品", f"SELECT TOP 5 pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"),
    ("進貨次數最多的前10個商品", f"SELECT TOP 10 pName, COUNT(*) AS cnt FROM {V} WHERE {F} GROUP BY pName ORDER BY cnt DESC;"),
]
for m in AO_MONTHS:
    T += [
        (f"{fm(m)}付款最多的供應商", f"SELECT TOP 1 pvName, SUM(amount) AS total FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}' GROUP BY pvName ORDER BY total DESC;"),
        (f"{fm(m)}付款最多的前5個供應商", f"SELECT TOP 5 pvName, SUM(amount) AS total FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}' GROUP BY pvName ORDER BY total DESC;"),
        (f"{fm(m)}進貨最多的前5個商品", f"SELECT TOP 5 pName, SUM(qty) AS total_qty FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}' GROUP BY pName ORDER BY total_qty DESC;"),
    ]

# -- WP_vOutStock --
V = "WP_M09.dbo.WP_vOutStock"; F = "isDel='N' AND dtlIsDel='N'"
T += [
    ("銷售金額最高的月份", f"SELECT TOP 1 LEFT(OutStkId,6) AS ym, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY LEFT(OutStkId,6) ORDER BY total DESC;"),
    ("銷售金額最高的前3個月", f"SELECT TOP 3 LEFT(OutStkId,6) AS ym, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY LEFT(OutStkId,6) ORDER BY total DESC;"),
    ("銷售筆數最多的月份", f"SELECT TOP 1 LEFT(OutStkId,6) AS ym, COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} GROUP BY LEFT(OutStkId,6) ORDER BY cnt DESC;"),
    ("平均消費最高的會員", f"SELECT TOP 1 memName, AVG(amount) AS avg_amt FROM {V} WHERE {F} AND memName IS NOT NULL AND memName<>'' GROUP BY memName ORDER BY avg_amt DESC;"),
    ("消費次數最多的前5個會員", f"SELECT TOP 5 memName, COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} AND memName IS NOT NULL AND memName<>'' GROUP BY memName ORDER BY cnt DESC;"),
    ("消費次數最多的前10個會員", f"SELECT TOP 10 memName, COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} AND memName IS NOT NULL AND memName<>'' GROUP BY memName ORDER BY cnt DESC;"),
    ("銷售次數最多的前10個商品", f"SELECT TOP 10 pName, COUNT(*) AS cnt FROM {V} WHERE {F} GROUP BY pName ORDER BY cnt DESC;"),
    ("銷售次數最多的前5個商品", f"SELECT TOP 5 pName, COUNT(*) AS cnt FROM {V} WHERE {F} GROUP BY pName ORDER BY cnt DESC;"),
    ("平均銷售數量最高的前10個商品", f"SELECT TOP 10 pName, AVG(qty) AS avg_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY avg_qty DESC;"),
    ("銷售金額最高的倉庫", f"SELECT TOP 1 whSn, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY whSn ORDER BY total DESC;"),
]
for m in OS_MONTHS:
    T += [
        (f"{fm(m)}銷售最多的商品", f"SELECT TOP 1 pName, SUM(qty) AS total_qty FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}' GROUP BY pName ORDER BY total_qty DESC;"),
        (f"{fm(m)}銷售金額最高的前5個商品", f"SELECT TOP 5 pName, SUM(amtTotal) AS total FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}' GROUP BY pName ORDER BY total DESC;"),
        (f"{fm(m)}銷售數量最多的前10個商品", f"SELECT TOP 10 pName, SUM(qty) AS total_qty FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}' GROUP BY pName ORDER BY total_qty DESC;"),
        (f"{fm(m)}消費最多的會員", f"SELECT TOP 1 memName, SUM(amount) AS total FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}' AND memName IS NOT NULL AND memName<>'' GROUP BY memName ORDER BY total DESC;"),
    ]

# -- WP_vTransfer --
V = "WP_M09.dbo.WP_vTransfer"; F = "isDel='N' AND dtlIsDel='N'"
T += [
    ("調撥筆數最多的月份", f"SELECT TOP 1 LEFT(TransferId,6) AS ym, COUNT(DISTINCT TransferId) AS cnt FROM {V} WHERE {F} GROUP BY LEFT(TransferId,6) ORDER BY cnt DESC;"),
    ("調撥數量最多的前3個月", f"SELECT TOP 3 LEFT(TransferId,6) AS ym, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY LEFT(TransferId,6) ORDER BY total_qty DESC;"),
    ("調撥商品種類最多的倉庫", f"SELECT TOP 1 fWhName, COUNT(DISTINCT pName) AS prod_cnt FROM {V} WHERE {F} GROUP BY fWhName ORDER BY prod_cnt DESC;"),
]
for m in TF_MONTHS:
    T += [
        (f"{fm(m)}調撥最多的商品", f"SELECT TOP 1 pName, SUM(qty) AS total_qty FROM {V} WHERE {F} AND LEFT(TransferId,6)='{m}' GROUP BY pName ORDER BY total_qty DESC;"),
        (f"{fm(m)}調撥最多的前5個商品", f"SELECT TOP 5 pName, SUM(qty) AS total_qty FROM {V} WHERE {F} AND LEFT(TransferId,6)='{m}' GROUP BY pName ORDER BY total_qty DESC;"),
        (f"{fm(m)}調出最多的倉庫", f"SELECT TOP 1 fWhName, SUM(qty) AS total FROM {V} WHERE {F} AND LEFT(TransferId,6)='{m}' GROUP BY fWhName ORDER BY total DESC;"),
    ]

# -- WP_vInventory --
V = "WP_M09.dbo.WP_vInventory"
T += [
    ("平均庫存最高的倉庫", f"SELECT TOP 1 WarehouseName, AVG(qty) AS avg_qty FROM {V} GROUP BY WarehouseName ORDER BY avg_qty DESC;"),
    ("庫存為零商品最多的倉庫", f"SELECT TOP 1 WarehouseName, COUNT(*) AS cnt FROM {V} WHERE qty=0 GROUP BY WarehouseName ORDER BY cnt DESC;"),
    ("庫存金額最高的前3個倉庫", f"SELECT TOP 3 WarehouseName, SUM(qty * costAvg) AS total_cost FROM {V} WHERE qty>0 GROUP BY WarehouseName ORDER BY total_cost DESC;"),
    ("庫存商品最多的前5個供應商", f"SELECT TOP 5 pvName, COUNT(DISTINCT pNo) AS cnt FROM {V} WHERE pvName IS NOT NULL GROUP BY pvName ORDER BY cnt DESC;"),
    ("庫存總量最多的前10個供應商", f"SELECT TOP 10 pvName, SUM(qty) AS total_qty FROM {V} WHERE pvName IS NOT NULL GROUP BY pvName ORDER BY total_qty DESC;"),
    ("庫存金額最高的前5個供應商", f"SELECT TOP 5 pvName, SUM(qty * costAvg) AS total_cost FROM {V} WHERE pvName IS NOT NULL AND qty>0 GROUP BY pvName ORDER BY total_cost DESC;"),
]
for wh in WAREHOUSES:
    T += [
        (f"「{wh}」庫存最多的前5個商品", f"SELECT TOP 5 pName, qty FROM {V} WHERE WarehouseName=N'{wh}' ORDER BY qty DESC;"),
        (f"「{wh}」庫存金額最高的前5個商品", f"SELECT TOP 5 pName, qty * costAvg AS cost FROM {V} WHERE WarehouseName=N'{wh}' AND qty>0 ORDER BY cost DESC;"),
    ]

# -- WP_vProduct --
V = "WP_M09.dbo.WP_vProduct"
T += [
    ("平均售價最高的前10個供應商", f"SELECT TOP 10 pvName, AVG(priceStd) AS avg_price FROM {V} GROUP BY pvName ORDER BY avg_price DESC;"),
    ("總庫存最多的前5個供應商", f"SELECT TOP 5 pvName, SUM(qtyNow) AS total_qty FROM {V} GROUP BY pvName ORDER BY total_qty DESC;"),
    ("平均成本最高的前5個供應商", f"SELECT TOP 5 pvName, AVG(costStd) AS avg_cost FROM {V} GROUP BY pvName ORDER BY avg_cost DESC;"),
    ("商品最多的前3個單位類別", f"SELECT TOP 3 pUName, COUNT(*) AS cnt FROM {V} GROUP BY pUName ORDER BY cnt DESC;"),
    ("平均售價最高的單位類別", f"SELECT TOP 1 pUName, AVG(priceStd) AS avg_price FROM {V} GROUP BY pUName ORDER BY avg_price DESC;"),
]

# -- WP_vProvider --
V = "WP_M09.dbo.WP_vProvider"
T += [
    ("供應商最多的前5個縣市", f"SELECT TOP 5 pvCity, COUNT(*) AS cnt FROM {V} WHERE sn > 0 GROUP BY pvCity ORDER BY cnt DESC;"),
    ("供應商最多的前10個縣市", f"SELECT TOP 10 pvCity, COUNT(*) AS cnt FROM {V} WHERE sn > 0 GROUP BY pvCity ORDER BY cnt DESC;"),
]


# ═══════════════════════════════════════════
# Validate and merge with v3
# ═══════════════════════════════════════════
print(f"Extra templates: {len(T)}")

# Load existing v3
with open("data/wp_m09/train_claude_en_2000_v3.json", "r", encoding="utf-8") as f:
    existing = json.load(f)

existing_q = {item["question"] for item in existing}
existing_sql = {item["query"] for item in existing}

# Dedup against existing
new_only = []
for q, sql in T:
    if q not in existing_q and sql not in existing_sql:
        new_only.append((q, sql))
print(f"After removing duplicates with v3: {len(new_only)}")

# Validate
print("Validating...")
valid_new = []
errors = []
for i, (q, sql) in enumerate(new_only):
    try:
        c = conn.cursor()
        c.execute(sql)
        rows = c.fetchall()
        c.close()
        if len(rows) > 0 and not (len(rows) == 1 and all(v is None for v in rows[0])):
            valid_new.append((q, sql))
    except Exception as e:
        errors.append((q, sql, str(e)))

print(f"Valid new: {len(valid_new)}, Errors: {len(errors)}")
if errors:
    for q, sql, err in errors[:10]:
        print(f"  ERR: {q}\n  SQL: {sql}\n  {err}\n")

# Classify
def classify(sql):
    s = sql.upper()
    if "GROUP BY" in s and "TOP" in s: return "TOP+GROUP"
    elif "GROUP BY" in s: return "GROUP"
    elif any(f in s for f in ["SUM(","COUNT(","AVG(","MAX(","MIN("]): return "AGG"
    elif "TOP " in s: return "TOP"
    else: return "SIMPLE"

# Merge: existing + new
all_items = [(item["question"], item["query"]) for item in existing] + valid_new
random.shuffle(all_items)

# Re-balance for 2000
by_pat = {}
for q, sql in all_items:
    p = classify(sql)
    by_pat.setdefault(p, []).append((q, sql))

print("\nAvailable after merge:")
for p, items in sorted(by_pat.items()):
    print(f"  {p}: {len(items)}")

# New targets
targets = {"SIMPLE": 650, "AGG": 600, "GROUP": 350, "TOP": 150, "TOP+GROUP": 250}
final = []
for pat, target in targets.items():
    pool = by_pat.get(pat, [])
    random.shuffle(pool)
    take = min(target, len(pool))
    final.extend(pool[:take])
    print(f"  {pat}: target={target}, taken={take}")

random.shuffle(final)
print(f"\nFinal: {len(final)}")

# Output
training = []
for q, sql in final:
    toks = sql.replace("(", " ( ").replace(")", " ) ").replace(",", " , ").replace(";", " ;").split()
    training.append({
        "db_id": "WP_M09",
        "query": sql,
        "query_toks": toks,
        "query_toks_no_value": toks,
        "question": q,
        "question_toks": list(q),
        "sql": {"from":{"table_units":[],"conds":[]},"select":[False,[]],"where":[],"groupBy":[],"having":[],"orderBy":[],"limit":None,"intersect":None,"union":None,"except":None},
    })

out = "data/wp_m09/train_claude_en_2000_v3.json"
with open(out, "w", encoding="utf-8") as f:
    json.dump(training, f, ensure_ascii=False, indent=2)

print(f"\nSaved {len(training)} to {out}")

vc = Counter()
pc = Counter()
for q, sql in final:
    pc[classify(sql)] += 1
    for vn in ["WP_vAcctIn","WP_vAcctOut","WP_vOutStock","WP_vTransfer","WP_vInventory","WP_vProduct","WP_vProvider"]:
        if vn in sql:
            vc[vn] += 1
            break

print("\nView distribution:")
for vn, cnt in sorted(vc.items()):
    print(f"  {vn}: {cnt} ({cnt*100/len(final):.1f}%)")

print("\nPattern distribution:")
for p, cnt in sorted(pc.items()):
    print(f"  {p}: {cnt} ({cnt*100/len(final):.1f}%)")

conn.close()
print("\nDone!")
