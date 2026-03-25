"""
v3 訓練集：平衡 SQL 模式分布，目標 EM/EX ≥ 90%。

模式目標分布：
- 簡單查詢 (SELECT WHERE): ~35%
- 聚合無GROUP (SUM/COUNT/AVG): ~30%
- GROUP BY 統計: ~15%
- TOP N: ~10%
- TOP + GROUP BY 排名: ~10%

關鍵改進：
1. SQL 不重複
2. 增加 GROUP BY / TOP+GROUP BY 比例
3. 一致的 SQL 格式
4. 每個 View 覆蓋所有 query pattern
5. 多樣化問句措辭
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
    r = [row[0] for row in c.fetchall() if row[0] is not None]; c.close()
    return r


# ── Real data pools ──
PRODUCTS = fv("SELECT DISTINCT TOP 200 pName FROM WP_M09.dbo.WP_vProduct WHERE pName IS NOT NULL AND pName<>''")
PROVIDERS = fv("SELECT DISTINCT pvName FROM WP_M09.dbo.WP_vProvider WHERE sn > 0 AND pvName<>''")
WAREHOUSES = fv("SELECT DISTINCT WarehouseName FROM WP_M09.dbo.WP_vInventory WHERE WarehouseName IS NOT NULL")
MEMBERS = fv("SELECT DISTINCT TOP 80 memName FROM WP_M09.dbo.WP_vOutStock WHERE memName IS NOT NULL AND memName<>''")
BARCODES = fv("SELECT DISTINCT TOP 100 pBarcode FROM WP_M09.dbo.WP_vProduct WHERE pBarcode IS NOT NULL AND pBarcode<>''")
EMP_IDS = fv("SELECT DISTINCT empId FROM WP_M09.dbo.WP_vAcctOut WHERE empId IS NOT NULL")
PV_IDS = fv("SELECT DISTINCT pvId FROM WP_M09.dbo.WP_vProvider WHERE sn > 0 AND pvId<>''")

AI_MONTHS = fv("SELECT DISTINCT LEFT(acctInId,6) FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N'")
AO_MONTHS = fv("SELECT DISTINCT LEFT(acctOutId,6) FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N'")
OS_MONTHS = fv("SELECT DISTINCT LEFT(OutStkId,6) FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N'")
TF_MONTHS = fv("SELECT DISTINCT LEFT(TransferId,6) FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N'")

AI_DAYS = fv("SELECT DISTINCT LEFT(acctInId,8) FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N'")
AO_DAYS = fv("SELECT DISTINCT LEFT(acctOutId,8) FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N'")
OS_DAYS = fv("SELECT DISTINCT LEFT(OutStkId,8) FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N'")
TF_DAYS = fv("SELECT DISTINCT LEFT(TransferId,8) FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N'")

AI_PRODS = fv("SELECT DISTINCT TOP 50 pName FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND pName IS NOT NULL AND pName<>''")
AO_PRODS = fv("SELECT DISTINCT TOP 100 pName FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' AND pName IS NOT NULL AND pName<>''")
OS_PRODS = fv("SELECT DISTINCT TOP 100 pName FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND pName IS NOT NULL AND pName<>''")
TF_PRODS = fv("SELECT DISTINCT TOP 80 pName FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N' AND pName IS NOT NULL AND pName<>''")
INV_PRODS = fv("SELECT DISTINCT TOP 150 pName FROM WP_M09.dbo.WP_vInventory WHERE pName IS NOT NULL AND pName<>''")
AO_PVS = fv("SELECT DISTINCT TOP 80 pvName FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' AND pvName IS NOT NULL AND pvName<>''")
INV_PVS = fv("SELECT DISTINCT TOP 80 pvName FROM WP_M09.dbo.WP_vInventory WHERE pvName IS NOT NULL AND pvName<>''")

print(f"Pools: Products={len(PRODUCTS)}, Providers={len(PROVIDERS)}, Warehouses={len(WAREHOUSES)}")


def fd(d): return f"{d[:4]}年{int(d[4:6])}月{int(d[6:])}日"
def fm(m): return f"{m[:4]}年{int(m[4:])}月"


T = []  # (question, sql)

# ═══════════════════════════════════════════════════════════
# WP_vAcctIn (收款單) — 目標 ~120 題
# ═══════════════════════════════════════════════════════════
V = "WP_M09.dbo.WP_vAcctIn"; F = "isDel='N' AND dtlIsDel='N'"

# --- 簡單查詢 ---
T += [
    ("列出所有收款單號及日期", f"SELECT DISTINCT acctInId, acctInDate FROM {V} WHERE {F} ORDER BY acctInId;"),
    ("查詢所有收款明細", f"SELECT acctInId, pName, oStkDtlQty, oStkDtlAmt FROM {V} WHERE {F} ORDER BY acctInId;"),
    ("收款單對應的出貨單號", f"SELECT DISTINCT acctInId, OutStkId FROM {V} WHERE {F} ORDER BY acctInId;"),
    ("有折扣的收款明細", f"SELECT acctInId, pName, oStkDtlAmt, discount, discountShare FROM {V} WHERE {F} AND discount>0;"),
]
for d in AI_DAYS:
    T.append((f"查詢{fd(d)}的收款單", f"SELECT DISTINCT acctInId, acctInDate, amount FROM {V} WHERE {F} AND LEFT(acctInId,8)='{d}' ORDER BY acctInId;"))
for m in AI_MONTHS:
    T.append((f"查詢{fm(m)}的收款單", f"SELECT DISTINCT acctInId, acctInDate, amount FROM {V} WHERE {F} AND LEFT(acctInId,6)='{m}' ORDER BY acctInId;"))
for p in AI_PRODS:
    T.append((f"查詢商品「{p}」的收款紀錄", f"SELECT acctInId, acctInDate, amount, pName, oStkDtlAmt, oStkDtlQty FROM {V} WHERE {F} AND pName=N'{p}';"))
for mem in MEMBERS[:10]:
    T.append((f"會員「{mem}」的收款紀錄", f"SELECT acctInId, acctInDate, amount, memName FROM {V} WHERE {F} AND memName=N'{mem}';"))
for eid in EMP_IDS:
    T.append((f"員工{eid}處理的收款單", f"SELECT DISTINCT acctInId, acctInDate, amount FROM {V} WHERE {F} AND empId='{eid}' ORDER BY acctInId;"))
for amt in [1000, 5000, 10000, 20000, 50000]:
    T.append((f"收款金額超過{amt}的單據", f"SELECT DISTINCT acctInId, acctInDate, amount FROM {V} WHERE {F} AND amount>{amt} ORDER BY amount DESC;"))

# --- 聚合 ---
T += [
    ("所有收款單的總金額", f"SELECT SUM(amount) AS total_amount FROM {V} WHERE {F};"),
    ("收款單共有幾筆", f"SELECT COUNT(DISTINCT acctInId) AS cnt FROM {V} WHERE {F};"),
    ("收款單的平均金額", f"SELECT AVG(amount) AS avg_amount FROM {V} WHERE {F};"),
]
for m in AI_MONTHS:
    T += [
        (f"{fm(m)}收款總額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F} AND LEFT(acctInId,6)='{m}';"),
        (f"{fm(m)}收款幾筆", f"SELECT COUNT(DISTINCT acctInId) AS cnt FROM {V} WHERE {F} AND LEFT(acctInId,6)='{m}';"),
    ]
for p in AI_PRODS:
    T.append((f"商品「{p}」的收款總額", f"SELECT SUM(oStkDtlAmtTotal) AS total FROM {V} WHERE {F} AND pName=N'{p}';"))
for p in AI_PRODS[:15]:
    T.append((f"商品「{p}」的收款數量", f"SELECT SUM(oStkDtlQty) AS total_qty FROM {V} WHERE {F} AND pName=N'{p}';"))

# --- TOP N ---
for n in [1, 3, 5, 10]:
    T.append((f"收款金額最高的前{n}筆", f"SELECT TOP {n} acctInId, acctInDate, amount FROM {V} WHERE {F} ORDER BY amount DESC;"))
T.append(("收款金額最低的收款單", f"SELECT TOP 1 acctInId, amount FROM {V} WHERE {F} ORDER BY amount ASC;"))

# --- GROUP BY ---
T += [
    ("統計每月收款總額", f"SELECT LEFT(acctInId,6) AS ym, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY LEFT(acctInId,6) ORDER BY ym;"),
    ("統計每月收款筆數", f"SELECT LEFT(acctInId,6) AS ym, COUNT(DISTINCT acctInId) AS cnt FROM {V} WHERE {F} GROUP BY LEFT(acctInId,6) ORDER BY ym;"),
    ("統計每月收款平均金額", f"SELECT LEFT(acctInId,6) AS ym, AVG(amount) AS avg_amt FROM {V} WHERE {F} GROUP BY LEFT(acctInId,6) ORDER BY ym;"),
    ("每筆收款單的明細數量", f"SELECT acctInId, COUNT(*) AS detail_cnt FROM {V} WHERE {F} GROUP BY acctInId ORDER BY acctInId;"),
    ("每個商品的收款總金額", f"SELECT pName, SUM(oStkDtlAmtTotal) AS total FROM {V} WHERE {F} GROUP BY pName ORDER BY total DESC;"),
    ("每個商品的收款數量", f"SELECT pName, SUM(oStkDtlQty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"),
    ("每個員工的收款筆數", f"SELECT empId, COUNT(DISTINCT acctInId) AS cnt FROM {V} WHERE {F} GROUP BY empId ORDER BY cnt DESC;"),
]

# --- TOP + GROUP BY ---
T += [
    ("收款金額最高的商品", f"SELECT TOP 1 pName, SUM(oStkDtlAmtTotal) AS total FROM {V} WHERE {F} GROUP BY pName ORDER BY total DESC;"),
    ("收款數量最多的前5個商品", f"SELECT TOP 5 pName, SUM(oStkDtlQty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"),
    ("收款數量最多的前10個商品", f"SELECT TOP 10 pName, SUM(oStkDtlQty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"),
]


# ═══════════════════════════════════════════════════════════
# WP_vAcctOut (付款單) — 目標 ~300 題
# ═══════════════════════════════════════════════════════════
V = "WP_M09.dbo.WP_vAcctOut"; F = "isDel='N' AND dtlIsDel='N'"

# --- 簡單查詢 ---
T += [
    ("列出所有付款單號及日期", f"SELECT DISTINCT acctOutId, acctOutDate, amount FROM {V} WHERE {F} ORDER BY acctOutId;"),
    ("付款單對應的進貨單號", f"SELECT DISTINCT acctOutId, InStkId FROM {V} WHERE {F} ORDER BY acctOutId;"),
    ("含稅的付款明細", f"SELECT acctOutId, pName, amtTotal, amtNoneTax, isTax FROM {V} WHERE {F} AND isTax='Y';"),
    ("未稅的付款明細", f"SELECT acctOutId, pName, amtTotal, amtNoneTax, isTax FROM {V} WHERE {F} AND isTax='N';"),
    ("有轉帳金額的付款單", f"SELECT DISTINCT acctOutId, amount, transAmt FROM {V} WHERE {F} AND transAmt>0;"),
]
for d in random.sample(AO_DAYS, min(25, len(AO_DAYS))):
    T.append((f"查詢{fd(d)}的付款單", f"SELECT DISTINCT acctOutId, acctOutDate, amount FROM {V} WHERE {F} AND LEFT(acctOutId,8)='{d}' ORDER BY acctOutId;"))
for m in AO_MONTHS:
    T.append((f"查詢{fm(m)}的付款單", f"SELECT DISTINCT acctOutId, acctOutDate, amount FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}' ORDER BY acctOutId;"))
for pv in AO_PVS:
    T.append((f"供應商「{pv}」的付款紀錄", f"SELECT acctOutId, acctOutDate, amount, pvName FROM {V} WHERE {F} AND pvName=N'{pv}' ORDER BY acctOutId;"))
for p in AO_PRODS[:40]:
    T.append((f"商品「{p}」的付款明細", f"SELECT acctOutId, acctOutDate, pName, qty, amtTotal FROM {V} WHERE {F} AND pName=N'{p}';"))
for eid in EMP_IDS:
    T.append((f"員工{eid}處理的付款單", f"SELECT DISTINCT acctOutId, acctOutDate, amount, empName FROM {V} WHERE {F} AND empId='{eid}' ORDER BY acctOutId;"))
for amt in [1000, 5000, 10000, 20000, 50000]:
    T.append((f"付款金額超過{amt}的單據", f"SELECT DISTINCT acctOutId, acctOutDate, amount, pvName FROM {V} WHERE {F} AND amount>{amt} ORDER BY amount DESC;"))
# 月+供應商
for m in AO_MONTHS:
    for pv in random.sample(AO_PVS, min(3, len(AO_PVS))):
        T.append((f"{fm(m)}供應商「{pv}」的付款紀錄", f"SELECT acctOutId, acctOutDate, amount, pvName FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}' AND pvName=N'{pv}' ORDER BY acctOutId;"))

# --- 聚合 ---
T += [
    ("所有付款單的總金額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F};"),
    ("付款單共有幾筆", f"SELECT COUNT(DISTINCT acctOutId) AS cnt FROM {V} WHERE {F};"),
    ("付款單的平均金額", f"SELECT AVG(amount) AS avg_amount FROM {V} WHERE {F};"),
]
for m in AO_MONTHS:
    T += [
        (f"{fm(m)}付款總額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}';"),
        (f"{fm(m)}付款幾筆", f"SELECT COUNT(DISTINCT acctOutId) AS cnt FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}';"),
    ]
for pv in AO_PVS:
    T.append((f"供應商「{pv}」的付款總額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F} AND pvName=N'{pv}';"))
for pv in AO_PVS[:30]:
    T.append((f"供應商「{pv}」付款幾筆", f"SELECT COUNT(DISTINCT acctOutId) AS cnt FROM {V} WHERE {F} AND pvName=N'{pv}';"))
for p in AO_PRODS[:40]:
    T.append((f"商品「{p}」的進貨付款總額", f"SELECT SUM(amtTotal) AS total FROM {V} WHERE {F} AND pName=N'{p}';"))
# 月+供應商 聚合
for m in AO_MONTHS:
    for pv in random.sample(AO_PVS, min(3, len(AO_PVS))):
        T.append((f"{fm(m)}付給「{pv}」的總額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}' AND pvName=N'{pv}';"))

# --- TOP N ---
for n in [1, 3, 5, 10]:
    T.append((f"付款金額最高的前{n}筆", f"SELECT TOP {n} acctOutId, acctOutDate, amount, pvName FROM {V} WHERE {F} ORDER BY amount DESC;"))
T.append(("付款金額最低的付款單", f"SELECT TOP 1 acctOutId, amount FROM {V} WHERE {F} ORDER BY amount ASC;"))

# --- GROUP BY ---
T += [
    ("統計每月付款總額", f"SELECT LEFT(acctOutId,6) AS ym, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY LEFT(acctOutId,6) ORDER BY ym;"),
    ("統計每月付款筆數", f"SELECT LEFT(acctOutId,6) AS ym, COUNT(DISTINCT acctOutId) AS cnt FROM {V} WHERE {F} GROUP BY LEFT(acctOutId,6) ORDER BY ym;"),
    ("統計每月付款平均金額", f"SELECT LEFT(acctOutId,6) AS ym, AVG(amount) AS avg_amt FROM {V} WHERE {F} GROUP BY LEFT(acctOutId,6) ORDER BY ym;"),
    ("每個供應商的付款總額", f"SELECT pvName, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY pvName ORDER BY total DESC;"),
    ("每個供應商的付款筆數", f"SELECT pvName, COUNT(DISTINCT acctOutId) AS cnt FROM {V} WHERE {F} GROUP BY pvName ORDER BY cnt DESC;"),
    ("每個供應商的平均付款金額", f"SELECT pvName, AVG(amount) AS avg_amt FROM {V} WHERE {F} GROUP BY pvName ORDER BY avg_amt DESC;"),
    ("每個商品的付款總額", f"SELECT pName, SUM(amtTotal) AS total FROM {V} WHERE {F} GROUP BY pName ORDER BY total DESC;"),
    ("每個商品的付款數量", f"SELECT pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"),
    ("每個員工的付款筆數", f"SELECT empId, empName, COUNT(DISTINCT acctOutId) AS cnt FROM {V} WHERE {F} GROUP BY empId, empName ORDER BY cnt DESC;"),
    ("每月付款給幾個不同供應商", f"SELECT LEFT(acctOutId,6) AS ym, COUNT(DISTINCT pvName) AS pv_cnt FROM {V} WHERE {F} GROUP BY LEFT(acctOutId,6) ORDER BY ym;"),
]

# --- TOP + GROUP BY ---
T += [
    ("付款最多的供應商", f"SELECT TOP 1 pvName, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY pvName ORDER BY total DESC;"),
    ("付款最多的前5個供應商", f"SELECT TOP 5 pvName, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY pvName ORDER BY total DESC;"),
    ("付款最多的前10個供應商", f"SELECT TOP 10 pvName, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY pvName ORDER BY total DESC;"),
    ("付款筆數最多的供應商", f"SELECT TOP 1 pvName, COUNT(DISTINCT acctOutId) AS cnt FROM {V} WHERE {F} GROUP BY pvName ORDER BY cnt DESC;"),
    ("付款總額最高的前10個商品", f"SELECT TOP 10 pName, SUM(amtTotal) AS total FROM {V} WHERE {F} GROUP BY pName ORDER BY total DESC;"),
    ("付款總額最高的前5個商品", f"SELECT TOP 5 pName, SUM(amtTotal) AS total FROM {V} WHERE {F} GROUP BY pName ORDER BY total DESC;"),
    ("進貨數量最多的前10個商品", f"SELECT TOP 10 pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"),
]


# ═══════════════════════════════════════════════════════════
# WP_vOutStock (銷售/出貨) — 目標 ~400 題
# ═══════════════════════════════════════════════════════════
V = "WP_M09.dbo.WP_vOutStock"; F = "isDel='N' AND dtlIsDel='N'"

# --- 簡單查詢 ---
T.append(("查詢一般銷售紀錄", f"SELECT DISTINCT OutStkId, OutStkDate, amount FROM {V} WHERE {F} AND outType='0' ORDER BY OutStkId;"))
T.append(("查詢有折扣的銷售明細", f"SELECT OutStkId, pName, dtlAmt, dtlDiscnt, dtlDiscntShare FROM {V} WHERE {F} AND dtlDiscnt>0;"))
for d in random.sample(OS_DAYS, min(40, len(OS_DAYS))):
    T.append((f"查詢{fd(d)}的銷售紀錄", f"SELECT DISTINCT OutStkId, OutStkDate, amount FROM {V} WHERE {F} AND LEFT(OutStkId,8)='{d}' ORDER BY OutStkId;"))
for m in OS_MONTHS:
    T.append((f"查詢{fm(m)}的銷售紀錄", f"SELECT DISTINCT OutStkId, OutStkDate, amount FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}' ORDER BY OutStkId;"))
for p in OS_PRODS:
    T.append((f"商品「{p}」的銷售紀錄", f"SELECT OutStkId, OutStkDate, pName, qty, dtlAmt, amtTotal FROM {V} WHERE {F} AND pName=N'{p}' ORDER BY OutStkId;"))
for mem in MEMBERS[:30]:
    T.append((f"會員「{mem}」的消費紀錄", f"SELECT OutStkId, OutStkDate, amount, memName FROM {V} WHERE {F} AND memName=N'{mem}' ORDER BY OutStkId;"))
for eid in EMP_IDS:
    T.append((f"員工{eid}的銷售紀錄", f"SELECT DISTINCT OutStkId, OutStkDate, amount, empName FROM {V} WHERE {F} AND empId='{eid}' ORDER BY OutStkId;"))
for amt in [100, 500, 1000, 5000, 10000]:
    T.append((f"銷售金額超過{amt}的單據", f"SELECT DISTINCT OutStkId, OutStkDate, amount FROM {V} WHERE {F} AND amount>{amt} ORDER BY amount DESC;"))
for bc in BARCODES[:20]:
    T.append((f"條碼{bc}的銷售紀錄", f"SELECT OutStkId, OutStkDate, pBarcode, pName, qty, amtTotal FROM {V} WHERE {F} AND pBarcode='{bc}' ORDER BY OutStkId;"))
# 月+商品
for m in OS_MONTHS:
    for p in random.sample(OS_PRODS, min(5, len(OS_PRODS))):
        T.append((f"{fm(m)}商品「{p}」的銷售紀錄", f"SELECT OutStkId, OutStkDate, pName, qty, amtTotal FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}' AND pName=N'{p}';"))

# --- 聚合 ---
T += [
    ("所有銷售單的總金額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F};"),
    ("銷售單共有幾筆", f"SELECT COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F};"),
    ("銷售的平均金額", f"SELECT AVG(amount) AS avg_amount FROM {V} WHERE {F};"),
    ("一般銷售的筆數", f"SELECT COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} AND outType='0';"),
]
for m in OS_MONTHS:
    T += [
        (f"{fm(m)}銷售總額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}';"),
        (f"{fm(m)}銷售幾筆", f"SELECT COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}';"),
    ]
for p in OS_PRODS:
    T += [
        (f"商品「{p}」總共賣了多少數量", f"SELECT SUM(qty) AS total_qty FROM {V} WHERE {F} AND pName=N'{p}';"),
        (f"商品「{p}」的銷售總額", f"SELECT SUM(amtTotal) AS total FROM {V} WHERE {F} AND pName=N'{p}';"),
    ]
for mem in MEMBERS[:30]:
    T.append((f"會員「{mem}」的消費總額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F} AND memName=N'{mem}';"))
# 月+商品 聚合
for m in OS_MONTHS:
    for p in random.sample(OS_PRODS, min(5, len(OS_PRODS))):
        T.append((f"{fm(m)}商品「{p}」賣了多少", f"SELECT SUM(qty) AS total_qty FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}' AND pName=N'{p}';"))

# --- TOP N ---
for n in [1, 3, 5, 10, 20]:
    T.append((f"銷售金額最高的前{n}筆", f"SELECT TOP {n} OutStkId, OutStkDate, amount FROM {V} WHERE {F} ORDER BY amount DESC;"))
T.append(("銷售金額最低的單據", f"SELECT TOP 1 OutStkId, OutStkDate, amount FROM {V} WHERE {F} ORDER BY amount ASC;"))

# --- GROUP BY ---
T += [
    ("統計每月銷售總額", f"SELECT LEFT(OutStkId,6) AS ym, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY LEFT(OutStkId,6) ORDER BY ym;"),
    ("統計每月銷售筆數", f"SELECT LEFT(OutStkId,6) AS ym, COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} GROUP BY LEFT(OutStkId,6) ORDER BY ym;"),
    ("統計每月銷售平均金額", f"SELECT LEFT(OutStkId,6) AS ym, AVG(amount) AS avg_amt FROM {V} WHERE {F} GROUP BY LEFT(OutStkId,6) ORDER BY ym;"),
    ("每個商品的銷售總數量", f"SELECT pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"),
    ("每個商品的銷售總金額", f"SELECT pName, SUM(amtTotal) AS total FROM {V} WHERE {F} GROUP BY pName ORDER BY total DESC;"),
    ("每個會員的消費金額", f"SELECT memName, SUM(amount) AS total FROM {V} WHERE {F} AND memName IS NOT NULL AND memName<>'' GROUP BY memName ORDER BY total DESC;"),
    ("每個會員的消費筆數", f"SELECT memName, COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} AND memName IS NOT NULL AND memName<>'' GROUP BY memName ORDER BY cnt DESC;"),
    ("各銷售類型的筆數", f"SELECT outType, COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} GROUP BY outType;"),
    ("各倉庫的銷售金額", f"SELECT whSn, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY whSn ORDER BY total DESC;"),
    ("各員工的銷售筆數", f"SELECT empId, empName, COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} GROUP BY empId, empName ORDER BY cnt DESC;"),
    ("每月銷售了幾種不同商品", f"SELECT LEFT(OutStkId,6) AS ym, COUNT(DISTINCT pName) AS prod_cnt FROM {V} WHERE {F} GROUP BY LEFT(OutStkId,6) ORDER BY ym;"),
    ("每月有幾個不同會員消費", f"SELECT LEFT(OutStkId,6) AS ym, COUNT(DISTINCT memName) AS mem_cnt FROM {V} WHERE {F} AND memName IS NOT NULL AND memName<>'' GROUP BY LEFT(OutStkId,6) ORDER BY ym;"),
]

# --- TOP + GROUP BY ---
T += [
    ("銷售數量最多的商品", f"SELECT TOP 1 pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"),
    ("銷售金額最高的商品", f"SELECT TOP 1 pName, SUM(amtTotal) AS total FROM {V} WHERE {F} GROUP BY pName ORDER BY total DESC;"),
    ("銷售數量最多的前5個商品", f"SELECT TOP 5 pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"),
    ("銷售數量最多的前10個商品", f"SELECT TOP 10 pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"),
    ("銷售金額最高的前5個商品", f"SELECT TOP 5 pName, SUM(amtTotal) AS total FROM {V} WHERE {F} GROUP BY pName ORDER BY total DESC;"),
    ("銷售金額最高的前10個商品", f"SELECT TOP 10 pName, SUM(amtTotal) AS total FROM {V} WHERE {F} GROUP BY pName ORDER BY total DESC;"),
    ("銷售金額最高的前20個商品", f"SELECT TOP 20 pName, SUM(amtTotal) AS total FROM {V} WHERE {F} GROUP BY pName ORDER BY total DESC;"),
    ("消費最多的會員", f"SELECT TOP 1 memName, SUM(amount) AS total FROM {V} WHERE {F} AND memName IS NOT NULL AND memName<>'' GROUP BY memName ORDER BY total DESC;"),
    ("消費最多的前5個會員", f"SELECT TOP 5 memName, SUM(amount) AS total FROM {V} WHERE {F} AND memName IS NOT NULL AND memName<>'' GROUP BY memName ORDER BY total DESC;"),
    ("消費最多的前10個會員", f"SELECT TOP 10 memName, SUM(amount) AS total FROM {V} WHERE {F} AND memName IS NOT NULL AND memName<>'' GROUP BY memName ORDER BY total DESC;"),
]


# ═══════════════════════════════════════════════════════════
# WP_vTransfer (調撥單) — 目標 ~200 題
# ═══════════════════════════════════════════════════════════
V = "WP_M09.dbo.WP_vTransfer"; F = "isDel='N' AND dtlIsDel='N'"

# --- 簡單查詢 ---
T.append(("列出所有調撥單", f"SELECT DISTINCT TransferId, TransferDate FROM {V} WHERE {F} ORDER BY TransferId;"))
T.append(("所有調撥的商品清單", f"SELECT TransferId, pName, qty, costAvg FROM {V} WHERE {F} ORDER BY TransferId;"))
for d in random.sample(TF_DAYS, min(20, len(TF_DAYS))):
    T.append((f"查詢{fd(d)}的調撥紀錄", f"SELECT DISTINCT TransferId, TransferDate FROM {V} WHERE {F} AND LEFT(TransferId,8)='{d}' ORDER BY TransferId;"))
for m in TF_MONTHS:
    T.append((f"查詢{fm(m)}的調撥紀錄", f"SELECT DISTINCT TransferId, TransferDate FROM {V} WHERE {F} AND LEFT(TransferId,6)='{m}' ORDER BY TransferId;"))
for wh in WAREHOUSES:
    T += [
        (f"從「{wh}」調出的紀錄", f"SELECT TransferId, TransferDate, fWhName, tfWhName, pName, qty FROM {V} WHERE {F} AND fWhName=N'{wh}' ORDER BY TransferId;"),
        (f"調入「{wh}」的紀錄", f"SELECT TransferId, TransferDate, fWhName, tfWhName, pName, qty FROM {V} WHERE {F} AND tfWhName=N'{wh}' ORDER BY TransferId;"),
    ]
for p in TF_PRODS:
    T.append((f"商品「{p}」的調撥紀錄", f"SELECT TransferId, TransferDate, fWhName, tfWhName, pName, qty FROM {V} WHERE {F} AND pName=N'{p}';"))
for eid in EMP_IDS:
    T.append((f"員工{eid}的調撥紀錄", f"SELECT DISTINCT TransferId, TransferDate, empId FROM {V} WHERE {F} AND empId='{eid}' ORDER BY TransferId;"))
for bc in BARCODES[:10]:
    T.append((f"條碼{bc}的調撥紀錄", f"SELECT TransferId, TransferDate, pBarcode, pName, qty FROM {V} WHERE {F} AND pBarcode='{bc}';"))
# 月+倉庫
for m in TF_MONTHS:
    for wh in WAREHOUSES:
        T.append((f"{fm(m)}從「{wh}」調出的紀錄", f"SELECT TransferId, TransferDate, pName, qty FROM {V} WHERE {F} AND LEFT(TransferId,6)='{m}' AND fWhName=N'{wh}' ORDER BY TransferId;"))

# --- 聚合 ---
T += [
    ("調撥單共有幾筆", f"SELECT COUNT(DISTINCT TransferId) AS cnt FROM {V} WHERE {F};"),
    ("調撥成本總額", f"SELECT SUM(qty * costAvg) AS total_cost FROM {V} WHERE {F};"),
]
for m in TF_MONTHS:
    T.append((f"{fm(m)}調撥幾筆", f"SELECT COUNT(DISTINCT TransferId) AS cnt FROM {V} WHERE {F} AND LEFT(TransferId,6)='{m}';"))
for wh in WAREHOUSES:
    T += [
        (f"「{wh}」調出的總數量", f"SELECT SUM(qty) AS total FROM {V} WHERE {F} AND fWhName=N'{wh}';"),
        (f"「{wh}」調入的總數量", f"SELECT SUM(qty) AS total FROM {V} WHERE {F} AND tfWhName=N'{wh}';"),
    ]
for p in TF_PRODS:
    T.append((f"商品「{p}」的調撥總量", f"SELECT SUM(qty) AS total_qty FROM {V} WHERE {F} AND pName=N'{p}';"))

# --- GROUP BY ---
T += [
    ("統計每月調撥筆數", f"SELECT LEFT(TransferId,6) AS ym, COUNT(DISTINCT TransferId) AS cnt FROM {V} WHERE {F} GROUP BY LEFT(TransferId,6) ORDER BY ym;"),
    ("每個商品的調撥總量", f"SELECT pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"),
    ("各倉庫調出的總數量", f"SELECT fWhName, SUM(qty) AS total FROM {V} WHERE {F} GROUP BY fWhName ORDER BY total DESC;"),
    ("各倉庫調入的總數量", f"SELECT tfWhName, SUM(qty) AS total FROM {V} WHERE {F} GROUP BY tfWhName ORDER BY total DESC;"),
    ("各倉庫的調撥筆數", f"SELECT fWhName, COUNT(DISTINCT TransferId) AS cnt FROM {V} WHERE {F} GROUP BY fWhName ORDER BY cnt DESC;"),
    ("每月調撥了幾種不同商品", f"SELECT LEFT(TransferId,6) AS ym, COUNT(DISTINCT pName) AS prod_cnt FROM {V} WHERE {F} GROUP BY LEFT(TransferId,6) ORDER BY ym;"),
]

# --- TOP + GROUP BY ---
T += [
    ("調撥數量最多的商品", f"SELECT TOP 1 pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"),
    ("調撥數量最多的前5個商品", f"SELECT TOP 5 pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"),
    ("調撥數量最多的前10個商品", f"SELECT TOP 10 pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"),
    ("調出最多的倉庫", f"SELECT TOP 1 fWhName, SUM(qty) AS total FROM {V} WHERE {F} GROUP BY fWhName ORDER BY total DESC;"),
    ("調入最多的倉庫", f"SELECT TOP 1 tfWhName, SUM(qty) AS total FROM {V} WHERE {F} GROUP BY tfWhName ORDER BY total DESC;"),
]


# ═══════════════════════════════════════════════════════════
# WP_vInventory (庫存) — 目標 ~350 題
# ═══════════════════════════════════════════════════════════
V = "WP_M09.dbo.WP_vInventory"

# --- 簡單查詢 ---
T += [
    ("列出所有庫存商品", f"SELECT pNo, pName, pBarcode, qty, costAvg FROM {V} ORDER BY pNo;"),
    ("庫存為零的商品", f"SELECT pName, WarehouseName, qty FROM {V} WHERE qty=0;"),
    ("庫存低於安全庫存的商品", f"SELECT pName, WarehouseName, qty, qtySafe FROM {V} WHERE qty < qtySafe AND qtySafe > 0;"),
    ("庫存大於100的商品", f"SELECT pName, WarehouseName, qty FROM {V} WHERE qty>100 ORDER BY qty DESC;"),
    ("庫存大於50的商品", f"SELECT pName, WarehouseName, qty FROM {V} WHERE qty>50 ORDER BY qty DESC;"),
    ("庫存大於200的商品", f"SELECT pName, WarehouseName, qty FROM {V} WHERE qty>200 ORDER BY qty DESC;"),
    ("可銷售的庫存商品", f"SELECT pName, WarehouseName, qty FROM {V} WHERE isSale='Y' AND qty>0 ORDER BY qty DESC;"),
    ("不可銷售的庫存商品", f"SELECT pName, WarehouseName, qty FROM {V} WHERE isSale='N';"),
]
for wh in WAREHOUSES:
    T.append((f"「{wh}」的庫存清單", f"SELECT pNo, pName, qty, costAvg FROM {V} WHERE WarehouseName=N'{wh}' ORDER BY pNo;"))
for p in INV_PRODS:
    T.append((f"商品「{p}」的各倉庫存", f"SELECT WarehouseName, pName, qty, costAvg FROM {V} WHERE pName=N'{p}';"))
for bc in BARCODES[:20]:
    T.append((f"條碼{bc}的庫存", f"SELECT WarehouseName, pName, pBarcode, qty FROM {V} WHERE pBarcode='{bc}';"))
for pv in INV_PVS[:30]:
    T.append((f"供應商「{pv}」的庫存商品", f"SELECT pName, WarehouseName, qty, costAvg FROM {V} WHERE pvName=N'{pv}';"))
# 倉庫+商品
for wh in WAREHOUSES:
    for p in random.sample(INV_PRODS, min(5, len(INV_PRODS))):
        T.append((f"「{wh}」的「{p}」庫存", f"SELECT pName, qty, costAvg FROM {V} WHERE WarehouseName=N'{wh}' AND pName=N'{p}';"))

# --- 聚合 ---
T += [
    ("庫存商品共幾種", f"SELECT COUNT(DISTINCT pNo) AS cnt FROM {V};"),
    ("庫存總數量", f"SELECT SUM(qty) AS total_qty FROM {V};"),
    ("庫存總金額", f"SELECT SUM(qty * costAvg) AS total_cost FROM {V} WHERE qty>0;"),
]
for wh in WAREHOUSES:
    T += [
        (f"「{wh}」有幾種商品", f"SELECT COUNT(DISTINCT pNo) AS cnt FROM {V} WHERE WarehouseName=N'{wh}';"),
        (f"「{wh}」庫存總量", f"SELECT SUM(qty) AS total_qty FROM {V} WHERE WarehouseName=N'{wh}';"),
        (f"「{wh}」庫存金額", f"SELECT SUM(qty * costAvg) AS total_cost FROM {V} WHERE WarehouseName=N'{wh}' AND qty>0;"),
    ]
for p in INV_PRODS:
    T.append((f"商品「{p}」的總庫存", f"SELECT SUM(qty) AS total_qty FROM {V} WHERE pName=N'{p}';"))

# --- TOP N ---
for n in [5, 10, 20]:
    T += [
        (f"庫存最多的前{n}個商品", f"SELECT TOP {n} pName, WarehouseName, qty FROM {V} ORDER BY qty DESC;"),
        (f"售價最高的前{n}個庫存商品", f"SELECT TOP {n} pName, priceStd FROM {V} ORDER BY priceStd DESC;"),
        (f"成本最高的前{n}個庫存商品", f"SELECT TOP {n} pName, costStd FROM {V} ORDER BY costStd DESC;"),
    ]

# --- GROUP BY ---
T += [
    ("各倉庫庫存總量", f"SELECT WarehouseName, SUM(qty) AS total FROM {V} GROUP BY WarehouseName ORDER BY total DESC;"),
    ("各倉庫商品種類數", f"SELECT WarehouseName, COUNT(DISTINCT pNo) AS cnt FROM {V} GROUP BY WarehouseName ORDER BY cnt DESC;"),
    ("各倉庫庫存金額", f"SELECT WarehouseName, SUM(qty * costAvg) AS total_cost FROM {V} WHERE qty>0 GROUP BY WarehouseName ORDER BY total_cost DESC;"),
    ("各供應商庫存商品數", f"SELECT pvName, COUNT(DISTINCT pNo) AS cnt FROM {V} WHERE pvName IS NOT NULL GROUP BY pvName ORDER BY cnt DESC;"),
    ("各供應商庫存總量", f"SELECT pvName, SUM(qty) AS total_qty FROM {V} WHERE pvName IS NOT NULL GROUP BY pvName ORDER BY total_qty DESC;"),
    ("各商品在幾個倉庫有庫存", f"SELECT pName, COUNT(DISTINCT WarehouseName) AS wh_cnt FROM {V} WHERE qty>0 GROUP BY pName ORDER BY wh_cnt DESC;"),
]

# --- TOP + GROUP BY ---
T += [
    ("庫存金額最高的倉庫", f"SELECT TOP 1 WarehouseName, SUM(qty * costAvg) AS total_cost FROM {V} WHERE qty>0 GROUP BY WarehouseName ORDER BY total_cost DESC;"),
    ("商品種類最多的倉庫", f"SELECT TOP 1 WarehouseName, COUNT(DISTINCT pNo) AS cnt FROM {V} GROUP BY WarehouseName ORDER BY cnt DESC;"),
    ("庫存商品種類最多的前3個供應商", f"SELECT TOP 3 pvName, COUNT(DISTINCT pNo) AS cnt FROM {V} WHERE pvName IS NOT NULL GROUP BY pvName ORDER BY cnt DESC;"),
    ("庫存總量最多的前5個供應商", f"SELECT TOP 5 pvName, SUM(qty) AS total_qty FROM {V} WHERE pvName IS NOT NULL GROUP BY pvName ORDER BY total_qty DESC;"),
]


# ═══════════════════════════════════════════════════════════
# WP_vProduct (商品主檔) — 目標 ~350 題
# ═══════════════════════════════════════════════════════════
V = "WP_M09.dbo.WP_vProduct"

# --- 簡單查詢 ---
T += [
    ("列出所有商品名稱和售價", f"SELECT pNo, pName, pBarcode, priceStd FROM {V} ORDER BY pNo;"),
    ("可銷售的商品", f"SELECT pName, priceStd FROM {V} WHERE isSale='Y' ORDER BY pNo;"),
    ("不可銷售的商品", f"SELECT pName, priceStd FROM {V} WHERE isSale='N';"),
    ("需更新庫存的商品", f"SELECT pName FROM {V} WHERE isUpdStock='Y' ORDER BY pNo;"),
    ("含稅的商品", f"SELECT pName, priceStd, isTax FROM {V} WHERE isTax='Y' ORDER BY pNo;"),
    ("免稅的商品", f"SELECT pName, priceStd, isTax FROM {V} WHERE isTax='N' ORDER BY pNo;"),
    ("有供應商折扣的商品", f"SELECT pName, pvName, isPvDiscount FROM {V} WHERE isPvDiscount='Y';"),
    ("庫存為零的商品", f"SELECT pName, qtyNow FROM {V} WHERE qtyNow=0;"),
]
for p in PRODUCTS:
    T.append((f"商品「{p}」的詳細資料", f"SELECT pNo, pName, pBarcode, pCode, pUName, priceStd, priceLow, priceMem, costStd, costAvg, pvName FROM {V} WHERE pName=N'{p}';"))
for p in PRODUCTS:
    T.append((f"商品「{p}」的售價", f"SELECT pName, priceStd FROM {V} WHERE pName=N'{p}';"))
for p in random.sample(PRODUCTS, min(60, len(PRODUCTS))):
    T += [
        (f"商品「{p}」的成本", f"SELECT pName, costStd, costAvg FROM {V} WHERE pName=N'{p}';"),
        (f"商品「{p}」的會員價", f"SELECT pName, priceMem FROM {V} WHERE pName=N'{p}';"),
    ]
for bc in BARCODES[:30]:
    T.append((f"條碼{bc}是什麼商品", f"SELECT pName, pBarcode, priceStd FROM {V} WHERE pBarcode='{bc}';"))
for pv in random.sample(PROVIDERS, min(40, len(PROVIDERS))):
    T.append((f"供應商「{pv}」提供的商品", f"SELECT pNo, pName, priceStd, costStd FROM {V} WHERE pvName=N'{pv}' ORDER BY pNo;"))
for price in [50, 100, 200, 500, 1000]:
    T += [
        (f"售價超過{price}元的商品", f"SELECT pName, priceStd FROM {V} WHERE priceStd>{price} ORDER BY priceStd DESC;"),
        (f"售價低於{price}元的商品", f"SELECT pName, priceStd FROM {V} WHERE priceStd<{price} ORDER BY priceStd ASC;"),
    ]

# --- 聚合 ---
T += [
    ("商品總共幾種", f"SELECT COUNT(*) AS cnt FROM {V};"),
    ("平均售價", f"SELECT AVG(priceStd) AS avg_price FROM {V};"),
    ("平均成本", f"SELECT AVG(costStd) AS avg_cost FROM {V};"),
    ("庫存大於零的商品數", f"SELECT COUNT(*) AS cnt FROM {V} WHERE qtyNow>0;"),
]
for pv in random.sample(PROVIDERS, min(30, len(PROVIDERS))):
    T.append((f"供應商「{pv}」有幾種商品", f"SELECT COUNT(*) AS cnt FROM {V} WHERE pvName=N'{pv}';"))
for price in [30, 150, 300, 800]:
    T.append((f"售價超過{price}的商品有幾種", f"SELECT COUNT(*) AS cnt FROM {V} WHERE priceStd>{price};"))

# --- TOP N ---
for n in [1, 5, 10, 20]:
    T += [
        (f"售價最高的前{n}個商品", f"SELECT TOP {n} pName, priceStd FROM {V} ORDER BY priceStd DESC;"),
        (f"成本最高的前{n}個商品", f"SELECT TOP {n} pName, costStd FROM {V} ORDER BY costStd DESC;"),
    ]
for n in [5, 10, 20]:
    T.append((f"庫存最多的前{n}個商品(主檔)", f"SELECT TOP {n} pName, qtyNow FROM {V} ORDER BY qtyNow DESC;"))
T.append(("售價最低的商品", f"SELECT TOP 1 pName, priceStd FROM {V} WHERE priceStd>0 ORDER BY priceStd ASC;"))

# --- GROUP BY ---
T += [
    ("每個供應商的商品數", f"SELECT pvName, COUNT(*) AS cnt FROM {V} GROUP BY pvName ORDER BY cnt DESC;"),
    ("每個單位類別的商品數", f"SELECT pUName, COUNT(*) AS cnt FROM {V} GROUP BY pUName ORDER BY cnt DESC;"),
    ("每個供應商的平均售價", f"SELECT pvName, AVG(priceStd) AS avg_price FROM {V} GROUP BY pvName ORDER BY avg_price DESC;"),
    ("每個供應商的平均成本", f"SELECT pvName, AVG(costStd) AS avg_cost FROM {V} GROUP BY pvName ORDER BY avg_cost DESC;"),
]

# --- TOP + GROUP BY ---
T += [
    ("商品最多的供應商", f"SELECT TOP 1 pvName, COUNT(*) AS cnt FROM {V} GROUP BY pvName ORDER BY cnt DESC;"),
    ("商品最多的前5個供應商", f"SELECT TOP 5 pvName, COUNT(*) AS cnt FROM {V} GROUP BY pvName ORDER BY cnt DESC;"),
    ("商品最多的前10個供應商", f"SELECT TOP 10 pvName, COUNT(*) AS cnt FROM {V} GROUP BY pvName ORDER BY cnt DESC;"),
    ("平均售價最高的前5個供應商", f"SELECT TOP 5 pvName, AVG(priceStd) AS avg_price FROM {V} GROUP BY pvName ORDER BY avg_price DESC;"),
    ("商品種類最多的單位", f"SELECT TOP 1 pUName, COUNT(*) AS cnt FROM {V} GROUP BY pUName ORDER BY cnt DESC;"),
]


# ═══════════════════════════════════════════════════════════
# WP_vProvider (供應商) — 目標 ~280 題，用 sn 不是 pvSn
# ═══════════════════════════════════════════════════════════
V = "WP_M09.dbo.WP_vProvider"

# --- 簡單查詢 ---
T += [
    ("列出所有供應商", f"SELECT sn, pvId, pvName, pvTel FROM {V} WHERE sn > 0 ORDER BY sn;"),
    ("啟用的供應商", f"SELECT sn, pvId, pvName FROM {V} WHERE isStop='N' AND sn > 0 ORDER BY sn;"),
    ("已停用的供應商", f"SELECT sn, pvId, pvName FROM {V} WHERE isStop='Y';"),
    ("有銀行帳戶的供應商", f"SELECT pvName, bankName, bankAccount FROM {V} WHERE bankAccount IS NOT NULL AND bankAccount<>'' AND sn > 0;"),
    ("有聯絡人的供應商", f"SELECT pvName, ctactName, ctactTel FROM {V} WHERE ctactName IS NOT NULL AND ctactName<>'' AND sn > 0;"),
    ("有Email的供應商", f"SELECT pvName, email FROM {V} WHERE email IS NOT NULL AND email<>'' AND sn > 0;"),
    ("有傳真的供應商", f"SELECT pvName, fax FROM {V} WHERE fax IS NOT NULL AND fax<>'' AND sn > 0;"),
]
for pv in PROVIDERS:
    T.append((f"供應商「{pv}」的詳細資料", f"SELECT sn, pvId, pvName, pvBoss, pvTel, pvAddr, email, taxId FROM {V} WHERE pvName=N'{pv}';"))
for pv in PROVIDERS:
    T.append((f"供應商「{pv}」的電話", f"SELECT pvName, pvTel FROM {V} WHERE pvName=N'{pv}';"))
for pv in random.sample(PROVIDERS, min(50, len(PROVIDERS))):
    T += [
        (f"供應商「{pv}」的地址", f"SELECT pvName, pvCity, pvZone, pvAddr FROM {V} WHERE pvName=N'{pv}';"),
        (f"供應商「{pv}」的聯絡人", f"SELECT pvName, ctactName, ctactTel FROM {V} WHERE pvName=N'{pv}';"),
        (f"供應商「{pv}」是否停用", f"SELECT pvName, isStop FROM {V} WHERE pvName=N'{pv}';"),
    ]
for pv in random.sample(PROVIDERS, min(30, len(PROVIDERS))):
    T += [
        (f"供應商「{pv}」的銀行帳戶", f"SELECT pvName, bankName, bankAccount, bankAcctName FROM {V} WHERE pvName=N'{pv}';"),
        (f"供應商「{pv}」的統編", f"SELECT pvName, taxId FROM {V} WHERE pvName=N'{pv}';"),
        (f"供應商「{pv}」的負責人", f"SELECT pvName, pvBoss FROM {V} WHERE pvName=N'{pv}';"),
        (f"供應商「{pv}」的發票抬頭", f"SELECT pvName, invoTitle FROM {V} WHERE pvName=N'{pv}';"),
    ]
for sn in range(1, 25):
    T.append((f"供應商編號{sn}的資料", f"SELECT sn, pvId, pvName, pvTel, pvAddr FROM {V} WHERE sn={sn};"))
for pvid in PV_IDS[:20]:
    T.append((f"供應商代號{pvid}的資料", f"SELECT sn, pvId, pvName, pvTel FROM {V} WHERE pvId='{pvid}';"))

# --- 聚合 ---
T += [
    ("供應商總共幾家", f"SELECT COUNT(*) AS cnt FROM {V} WHERE sn > 0;"),
    ("啟用的供應商幾家", f"SELECT COUNT(*) AS cnt FROM {V} WHERE isStop='N' AND sn > 0;"),
    ("停用的供應商幾家", f"SELECT COUNT(*) AS cnt FROM {V} WHERE isStop='Y';"),
    ("有統編的供應商幾家", f"SELECT COUNT(*) AS cnt FROM {V} WHERE taxId IS NOT NULL AND taxId<>'' AND sn > 0;"),
    ("有Email的供應商幾家", f"SELECT COUNT(*) AS cnt FROM {V} WHERE email IS NOT NULL AND email<>'' AND sn > 0;"),
]

# --- GROUP BY ---
T += [
    ("各縣市供應商數量", f"SELECT pvCity, COUNT(*) AS cnt FROM {V} WHERE sn > 0 GROUP BY pvCity ORDER BY cnt DESC;"),
    ("啟用與停用的供應商數量", f"SELECT isStop, COUNT(*) AS cnt FROM {V} WHERE sn > 0 GROUP BY isStop;"),
    ("各供應商類別數量", f"SELECT pvKName, COUNT(*) AS cnt FROM {V} WHERE sn > 0 GROUP BY pvKName ORDER BY cnt DESC;"),
]

# --- TOP + GROUP BY ---
T += [
    ("供應商最多的縣市", f"SELECT TOP 1 pvCity, COUNT(*) AS cnt FROM {V} WHERE sn > 0 GROUP BY pvCity ORDER BY cnt DESC;"),
    ("供應商最多的前3個縣市", f"SELECT TOP 3 pvCity, COUNT(*) AS cnt FROM {V} WHERE sn > 0 GROUP BY pvCity ORDER BY cnt DESC;"),
    ("供應商最多的前5個縣市", f"SELECT TOP 5 pvCity, COUNT(*) AS cnt FROM {V} WHERE sn > 0 GROUP BY pvCity ORDER BY cnt DESC;"),
]


# ═══════════════════════════════════════════════════════════
# Dedup (by question AND by SQL)
# ═══════════════════════════════════════════════════════════
print(f"\nTotal raw templates: {len(T)}")

# Dedup by question
seen_q = set()
seen_sql = set()
unique = []
for q, sql in T:
    if q not in seen_q and sql not in seen_sql:
        seen_q.add(q)
        seen_sql.add(sql)
        unique.append((q, sql))

print(f"After dedup (Q+SQL): {len(unique)}")

# ═══════════════════════════════════════════════════════════
# Validate on DB
# ═══════════════════════════════════════════════════════════
print("Validating on database...")
valid = []
empty = []
errors = []

for i, (q, sql) in enumerate(unique):
    try:
        c = conn.cursor()
        c.execute(sql)
        rows = c.fetchall()
        c.close()
        if len(rows) == 0:
            empty.append((q, sql))
        elif len(rows) == 1 and all(v is None for v in rows[0]):
            empty.append((q, sql))
        else:
            valid.append((q, sql))
    except Exception as e:
        errors.append((q, sql, str(e)))
    if (i + 1) % 500 == 0:
        print(f"  {i+1}/{len(unique)}...")

print(f"\nValid={len(valid)}, Empty={len(empty)}, Errors={len(errors)}")

if errors:
    print("\n=== ERRORS ===")
    for q, sql, err in errors[:20]:
        print(f"  Q: {q}\n  SQL: {sql}\n  ERR: {err}\n")

# ═══════════════════════════════════════════════════════════
# Balance and select 2000
# ═══════════════════════════════════════════════════════════

# Classify patterns
def classify(sql):
    s = sql.upper()
    if "GROUP BY" in s and "TOP" in s:
        return "TOP+GROUP"
    elif "GROUP BY" in s:
        return "GROUP"
    elif any(f in s for f in ["SUM(", "COUNT(", "AVG(", "MAX(", "MIN("]):
        return "AGG"
    elif "TOP " in s:
        return "TOP"
    else:
        return "SIMPLE"

by_pattern = {}
for q, sql in valid:
    pat = classify(sql)
    by_pattern.setdefault(pat, []).append((q, sql))

print("\nAvailable by pattern:")
for p, items in sorted(by_pattern.items()):
    print(f"  {p}: {len(items)}")

# Target distribution for 2000:
# SIMPLE: ~700 (35%), AGG: ~600 (30%), GROUP: ~300 (15%), TOP: ~200 (10%), TOP+GROUP: ~200 (10%)
targets = {
    "SIMPLE": 700,
    "AGG": 600,
    "GROUP": 300,
    "TOP": 200,
    "TOP+GROUP": 200,
}

final = []
for pat, target in targets.items():
    pool = by_pattern.get(pat, [])
    random.shuffle(pool)
    take = min(target, len(pool))
    final.extend(pool[:take])
    print(f"  {pat}: target={target}, available={len(pool)}, taken={take}")

random.shuffle(final)
print(f"\nFinal dataset size: {len(final)}")

# ═══════════════════════════════════════════════════════════
# Output
# ═══════════════════════════════════════════════════════════
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
        "sql": {"from": {"table_units": [], "conds": []}, "select": [False, []], "where": [], "groupBy": [], "having": [], "orderBy": [], "limit": None, "intersect": None, "union": None, "except": None},
    })

out = "data/wp_m09/train_claude_en_2000_v3.json"
with open(out, "w", encoding="utf-8") as f:
    json.dump(training, f, ensure_ascii=False, indent=2)

print(f"\nSaved {len(training)} items to {out}")

# Final stats
vc = Counter()
pc = Counter()
for q, sql in final:
    pc[classify(sql)] += 1
    for vn in ["WP_vAcctIn", "WP_vAcctOut", "WP_vOutStock", "WP_vTransfer", "WP_vInventory", "WP_vProduct", "WP_vProvider"]:
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
