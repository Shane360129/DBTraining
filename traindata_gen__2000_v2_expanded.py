"""
擴充版：從 WP_M09 資料庫 7 個 View 真實資料產生 2000 題訓練集。
策略：用真實存在的商品名/供應商名/日期，確保每段 SQL 可執行且有回傳資料。

欄位規則：
- 只使用各 View 實際存在的欄位
- WP_vProvider 沒有 pvSn，用 sn
- *Id 結尾欄位為單號，前八碼 = 日期 (YYYYMMDD)
- 有 isDel 的 View 必須加 isDel='N' AND dtlIsDel='N'
"""
import json, pyodbc, random, sys, io
from collections import Counter

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=SHANE\\SQLEXPRESS;DATABASE=WP_M09;Trusted_Connection=yes;",
    timeout=30,
)


def fv(sql):
    c = conn.cursor(); c.execute(sql)
    r = [row[0] for row in c.fetchall() if row[0] is not None]; c.close()
    return r


# ── Real data pools (only use values that actually exist) ──
PRODUCTS = fv("SELECT DISTINCT TOP 200 pName FROM WP_M09.dbo.WP_vProduct WHERE pName IS NOT NULL AND pName<>''")
PROVIDERS = fv("SELECT DISTINCT pvName FROM WP_M09.dbo.WP_vProvider WHERE sn > 0 AND pvName<>''")
WAREHOUSES = fv("SELECT DISTINCT WarehouseName FROM WP_M09.dbo.WP_vInventory WHERE WarehouseName IS NOT NULL")
MEMBERS = fv("SELECT DISTINCT TOP 80 memName FROM WP_M09.dbo.WP_vOutStock WHERE memName IS NOT NULL AND memName<>''")
BARCODES = fv("SELECT DISTINCT TOP 100 pBarcode FROM WP_M09.dbo.WP_vProduct WHERE pBarcode IS NOT NULL AND pBarcode<>''")
EMP_IDS = fv("SELECT DISTINCT empId FROM WP_M09.dbo.WP_vAcctOut WHERE empId IS NOT NULL")
PV_IDS = fv("SELECT DISTINCT pvId FROM WP_M09.dbo.WP_vProvider WHERE sn > 0 AND pvId<>''")

# Per-view real data
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

print(f"Data pools loaded. Products={len(PRODUCTS)}, Providers={len(PROVIDERS)}, Warehouses={len(WAREHOUSES)}")


def fd(d):
    return f"{d[:4]}年{int(d[4:6])}月{int(d[6:])}日"


def fm(m):
    return f"{m[:4]}年{int(m[4:])}月"


T = []  # (question, sql)

# ═══════════════════════════════════════════
# WP_vAcctIn (收款單)
# ═══════════════════════════════════════════
V = "WP_M09.dbo.WP_vAcctIn"
F = "isDel='N' AND dtlIsDel='N'"

T += [
    ("查詢所有收款單的總金額", f"SELECT SUM(amount) AS total_amount FROM {V} WHERE {F};"),
    ("收款單共有幾筆", f"SELECT COUNT(DISTINCT acctInId) AS cnt FROM {V} WHERE {F};"),
    ("列出所有收款單號及日期", f"SELECT DISTINCT acctInId, acctInDate FROM {V} WHERE {F} ORDER BY acctInId;"),
    ("收款單的平均金額", f"SELECT AVG(amount) AS avg_amount FROM {V} WHERE {F};"),
    ("收款金額最高的收款單", f"SELECT TOP 1 acctInId, amount FROM {V} WHERE {F} ORDER BY amount DESC;"),
    ("收款金額最低的收款單", f"SELECT TOP 1 acctInId, amount FROM {V} WHERE {F} ORDER BY amount ASC;"),
    ("What is the total collection amount?", f"SELECT SUM(amount) AS total_amount FROM {V} WHERE {F};"),
    ("How many collection records are there?", f"SELECT COUNT(DISTINCT acctInId) AS cnt FROM {V} WHERE {F};"),
    ("List all collection IDs and dates", f"SELECT DISTINCT acctInId, acctInDate, amount FROM {V} WHERE {F} ORDER BY acctInId;"),
    ("統計每月收款總額", f"SELECT LEFT(acctInId,6) AS ym, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY LEFT(acctInId,6) ORDER BY ym;"),
    ("統計每月收款筆數", f"SELECT LEFT(acctInId,6) AS ym, COUNT(DISTINCT acctInId) AS cnt FROM {V} WHERE {F} GROUP BY LEFT(acctInId,6) ORDER BY ym;"),
    ("查詢收款單對應的出貨單號", f"SELECT DISTINCT acctInId, OutStkId FROM {V} WHERE {F} ORDER BY acctInId;"),
    ("查詢有折扣的收款明細", f"SELECT acctInId, pName, oStkDtlAmt, discount, discountShare FROM {V} WHERE {F} AND discount>0;"),
    ("收款金額最高的前5筆", f"SELECT TOP 5 acctInId, acctInDate, amount FROM {V} WHERE {F} ORDER BY amount DESC;"),
    ("收款金額最高的前10筆", f"SELECT TOP 10 acctInId, acctInDate, amount FROM {V} WHERE {F} ORDER BY amount DESC;"),
    ("查詢所有收款的商品明細", f"SELECT acctInId, pName, oStkDtlQty, oStkDtlAmt FROM {V} WHERE {F} ORDER BY acctInId;"),
    ("每筆收款單的明細數量", f"SELECT acctInId, COUNT(*) AS detail_cnt FROM {V} WHERE {F} GROUP BY acctInId ORDER BY acctInId;"),
]

for m in AI_MONTHS:
    T += [
        (f"查詢{fm(m)}的收款單", f"SELECT DISTINCT acctInId, acctInDate, amount FROM {V} WHERE {F} AND LEFT(acctInId,6)='{m}' ORDER BY acctInId;"),
        (f"{fm(m)}的收款總額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F} AND LEFT(acctInId,6)='{m}';"),
        (f"{fm(m)}共有幾筆收款", f"SELECT COUNT(DISTINCT acctInId) AS cnt FROM {V} WHERE {F} AND LEFT(acctInId,6)='{m}';"),
        (f"Collections in {fm(m)}", f"SELECT DISTINCT acctInId, acctInDate, amount FROM {V} WHERE {F} AND LEFT(acctInId,6)='{m}' ORDER BY acctInId;"),
    ]

for d in AI_DAYS:
    T.append((f"查詢{fd(d)}的收款單", f"SELECT DISTINCT acctInId, acctInDate, amount FROM {V} WHERE {F} AND LEFT(acctInId,8)='{d}' ORDER BY acctInId;"))

for p in AI_PRODS:
    T += [
        (f"商品「{p}」的收款紀錄", f"SELECT acctInId, acctInDate, amount, pName, oStkDtlAmt, oStkDtlQty FROM {V} WHERE {F} AND pName=N'{p}';"),
        (f"商品「{p}」的收款總額", f"SELECT SUM(oStkDtlAmtTotal) AS total FROM {V} WHERE {F} AND pName=N'{p}';"),
    ]

for amt in [1000, 5000, 10000, 20000, 50000]:
    T += [
        (f"金額超過{amt}的收款單", f"SELECT DISTINCT acctInId, acctInDate, amount FROM {V} WHERE {F} AND amount>{amt} ORDER BY amount DESC;"),
        (f"金額低於{amt}的收款單", f"SELECT DISTINCT acctInId, acctInDate, amount FROM {V} WHERE {F} AND amount<{amt} ORDER BY amount ASC;"),
    ]

for mem in MEMBERS[:20]:
    T.append((f"會員「{mem}」的收款紀錄", f"SELECT acctInId, acctInDate, amount, memName FROM {V} WHERE {F} AND memName=N'{mem}';"))

for eid in EMP_IDS:
    T.append((f"員工{eid}的收款紀錄", f"SELECT DISTINCT acctInId, acctInDate, amount FROM {V} WHERE {F} AND empId='{eid}' ORDER BY acctInId;"))


# ═══════════════════════════════════════════
# WP_vAcctOut (付款單)
# ═══════════════════════════════════════════
V = "WP_M09.dbo.WP_vAcctOut"
F = "isDel='N' AND dtlIsDel='N'"

T += [
    ("查詢所有付款單的總金額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F};"),
    ("付款單共有幾筆", f"SELECT COUNT(DISTINCT acctOutId) AS cnt FROM {V} WHERE {F};"),
    ("付款金額最高的付款單", f"SELECT TOP 1 acctOutId, amount FROM {V} WHERE {F} ORDER BY amount DESC;"),
    ("付款金額最低的付款單", f"SELECT TOP 1 acctOutId, amount FROM {V} WHERE {F} ORDER BY amount ASC;"),
    ("付款單的平均金額", f"SELECT AVG(amount) AS avg_amount FROM {V} WHERE {F};"),
    ("列出所有付款單號及日期", f"SELECT DISTINCT acctOutId, acctOutDate, amount FROM {V} WHERE {F} ORDER BY acctOutId;"),
    ("What is the total payment amount?", f"SELECT SUM(amount) AS total FROM {V} WHERE {F};"),
    ("How many payment records?", f"SELECT COUNT(DISTINCT acctOutId) AS cnt FROM {V} WHERE {F};"),
    ("統計每月付款總額", f"SELECT LEFT(acctOutId,6) AS ym, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY LEFT(acctOutId,6) ORDER BY ym;"),
    ("統計每月付款筆數", f"SELECT LEFT(acctOutId,6) AS ym, COUNT(DISTINCT acctOutId) AS cnt FROM {V} WHERE {F} GROUP BY LEFT(acctOutId,6) ORDER BY ym;"),
    ("統計每個供應商的付款總額", f"SELECT pvName, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY pvName ORDER BY total DESC;"),
    ("統計每個供應商的付款筆數", f"SELECT pvName, COUNT(DISTINCT acctOutId) AS cnt FROM {V} WHERE {F} GROUP BY pvName ORDER BY cnt DESC;"),
    ("付款最多的前5個供應商", f"SELECT TOP 5 pvName, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY pvName ORDER BY total DESC;"),
    ("付款最多的前10個供應商", f"SELECT TOP 10 pvName, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY pvName ORDER BY total DESC;"),
    ("查詢付款單對應的進貨單號", f"SELECT DISTINCT acctOutId, InStkId FROM {V} WHERE {F} ORDER BY acctOutId;"),
    ("含稅的付款明細", f"SELECT acctOutId, pName, amtTotal, amtNoneTax, isTax FROM {V} WHERE {F} AND isTax='Y';"),
    ("未稅的付款明細", f"SELECT acctOutId, pName, amtTotal, amtNoneTax, isTax FROM {V} WHERE {F} AND isTax='N';"),
    ("有轉帳金額的付款單", f"SELECT DISTINCT acctOutId, amount, transAmt FROM {V} WHERE {F} AND transAmt>0;"),
    ("付款金額最高的前5筆", f"SELECT TOP 5 acctOutId, acctOutDate, amount, pvName FROM {V} WHERE {F} ORDER BY amount DESC;"),
    ("付款金額最高的前10筆", f"SELECT TOP 10 acctOutId, acctOutDate, amount, pvName FROM {V} WHERE {F} ORDER BY amount DESC;"),
    ("每個商品的付款總量", f"SELECT pName, SUM(qty) AS total_qty, SUM(amtTotal) AS total_amt FROM {V} WHERE {F} GROUP BY pName ORDER BY total_amt DESC;"),
    ("付款總額最高的前10個商品", f"SELECT TOP 10 pName, SUM(amtTotal) AS total FROM {V} WHERE {F} GROUP BY pName ORDER BY total DESC;"),
]

for m in AO_MONTHS:
    T += [
        (f"查詢{fm(m)}的付款單", f"SELECT DISTINCT acctOutId, acctOutDate, amount FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}' ORDER BY acctOutId;"),
        (f"{fm(m)}的付款總額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}';"),
        (f"{fm(m)}共有幾筆付款", f"SELECT COUNT(DISTINCT acctOutId) AS cnt FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}';"),
        (f"Payments in {fm(m)}", f"SELECT DISTINCT acctOutId, acctOutDate, amount FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}' ORDER BY acctOutId;"),
    ]

for d in random.sample(AO_DAYS, min(30, len(AO_DAYS))):
    T.append((f"查詢{fd(d)}的付款單", f"SELECT DISTINCT acctOutId, acctOutDate, amount FROM {V} WHERE {F} AND LEFT(acctOutId,8)='{d}' ORDER BY acctOutId;"))

for pv in AO_PVS:
    T += [
        (f"供應商「{pv}」的付款紀錄", f"SELECT acctOutId, acctOutDate, amount, pvName FROM {V} WHERE {F} AND pvName=N'{pv}' ORDER BY acctOutId;"),
        (f"供應商「{pv}」的付款總額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F} AND pvName=N'{pv}';"),
    ]

for p in AO_PRODS[:60]:
    T += [
        (f"商品「{p}」的付款明細", f"SELECT acctOutId, acctOutDate, pName, qty, amtTotal FROM {V} WHERE {F} AND pName=N'{p}';"),
        (f"商品「{p}」的進貨付款總額", f"SELECT SUM(amtTotal) AS total FROM {V} WHERE {F} AND pName=N'{p}';"),
    ]

for amt in [1000, 5000, 10000, 20000, 50000]:
    T += [
        (f"付款金額超過{amt}的單據", f"SELECT DISTINCT acctOutId, acctOutDate, amount, pvName FROM {V} WHERE {F} AND amount>{amt} ORDER BY amount DESC;"),
        (f"Payments over {amt}", f"SELECT DISTINCT acctOutId, acctOutDate, amount, pvName FROM {V} WHERE {F} AND amount>{amt} ORDER BY amount DESC;"),
    ]

for eid in EMP_IDS:
    T.append((f"員工{eid}處理的付款單", f"SELECT DISTINCT acctOutId, acctOutDate, amount, empName FROM {V} WHERE {F} AND empId='{eid}' ORDER BY acctOutId;"))

for m in AO_MONTHS:
    for pv in random.sample(AO_PVS, min(5, len(AO_PVS))):
        T.append((f"{fm(m)}供應商「{pv}」的付款", f"SELECT acctOutId, acctOutDate, amount, pvName FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}' AND pvName=N'{pv}' ORDER BY acctOutId;"))


# ═══════════════════════════════════════════
# WP_vOutStock (銷售/出貨單)
# ═══════════════════════════════════════════
V = "WP_M09.dbo.WP_vOutStock"
F = "isDel='N' AND dtlIsDel='N'"

T += [
    ("查詢所有銷售單的總金額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F};"),
    ("銷售單共有幾筆", f"SELECT COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F};"),
    ("銷售金額最高的單據", f"SELECT TOP 1 OutStkId, OutStkDate, amount FROM {V} WHERE {F} ORDER BY amount DESC;"),
    ("銷售金額最低的單據", f"SELECT TOP 1 OutStkId, OutStkDate, amount FROM {V} WHERE {F} ORDER BY amount ASC;"),
    ("銷售的平均金額", f"SELECT AVG(amount) AS avg_amount FROM {V} WHERE {F};"),
    ("What is the total sales amount?", f"SELECT SUM(amount) AS total FROM {V} WHERE {F};"),
    ("How many sales records?", f"SELECT COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F};"),
    ("統計每月銷售總額", f"SELECT LEFT(OutStkId,6) AS ym, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY LEFT(OutStkId,6) ORDER BY ym;"),
    ("統計每月銷售筆數", f"SELECT LEFT(OutStkId,6) AS ym, COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} GROUP BY LEFT(OutStkId,6) ORDER BY ym;"),
    ("統計每個商品的銷售數量", f"SELECT pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"),
    ("統計每個商品的銷售金額", f"SELECT pName, SUM(amtTotal) AS total FROM {V} WHERE {F} GROUP BY pName ORDER BY total DESC;"),
    ("銷售數量最多的前10個商品", f"SELECT TOP 10 pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"),
    ("銷售金額最高的前10個商品", f"SELECT TOP 10 pName, SUM(amtTotal) AS total FROM {V} WHERE {F} GROUP BY pName ORDER BY total DESC;"),
    ("銷售金額最高的前5個商品", f"SELECT TOP 5 pName, SUM(amtTotal) AS total FROM {V} WHERE {F} GROUP BY pName ORDER BY total DESC;"),
    ("銷售金額最高的前20個商品", f"SELECT TOP 20 pName, SUM(amtTotal) AS total FROM {V} WHERE {F} GROUP BY pName ORDER BY total DESC;"),
    ("一般銷售的紀錄筆數", f"SELECT COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} AND outType='0';"),
    ("各銷售類型的筆數", f"SELECT outType, COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} GROUP BY outType;"),
    ("有折扣的銷售明細", f"SELECT OutStkId, pName, dtlAmt, dtlDiscnt, dtlDiscntShare FROM {V} WHERE {F} AND dtlDiscnt>0;"),
    ("各倉庫的銷售金額", f"SELECT whSn, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY whSn ORDER BY total DESC;"),
    ("銷售金額最高的前5筆", f"SELECT TOP 5 OutStkId, OutStkDate, amount FROM {V} WHERE {F} ORDER BY amount DESC;"),
    ("銷售金額最高的前10筆", f"SELECT TOP 10 OutStkId, OutStkDate, amount FROM {V} WHERE {F} ORDER BY amount DESC;"),
    ("銷售金額最高的前20筆", f"SELECT TOP 20 OutStkId, OutStkDate, amount FROM {V} WHERE {F} ORDER BY amount DESC;"),
    ("統計每個會員的消費金額", f"SELECT memName, SUM(amount) AS total FROM {V} WHERE {F} AND memName IS NOT NULL AND memName<>'' GROUP BY memName ORDER BY total DESC;"),
    ("消費最多的前10個會員", f"SELECT TOP 10 memName, SUM(amount) AS total FROM {V} WHERE {F} AND memName IS NOT NULL AND memName<>'' GROUP BY memName ORDER BY total DESC;"),
]

for m in OS_MONTHS:
    T += [
        (f"查詢{fm(m)}的銷售紀錄", f"SELECT DISTINCT OutStkId, OutStkDate, amount FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}' ORDER BY OutStkId;"),
        (f"{fm(m)}的銷售總額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}';"),
        (f"{fm(m)}共有幾筆銷售", f"SELECT COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}';"),
        (f"Sales in {fm(m)}", f"SELECT DISTINCT OutStkId, OutStkDate, amount FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}' ORDER BY OutStkId;"),
    ]

for d in random.sample(OS_DAYS, min(50, len(OS_DAYS))):
    T.append((f"查詢{fd(d)}的銷售紀錄", f"SELECT DISTINCT OutStkId, OutStkDate, amount FROM {V} WHERE {F} AND LEFT(OutStkId,8)='{d}' ORDER BY OutStkId;"))

for p in OS_PRODS:
    T += [
        (f"商品「{p}」的銷售紀錄", f"SELECT OutStkId, OutStkDate, pName, qty, dtlAmt, amtTotal FROM {V} WHERE {F} AND pName=N'{p}' ORDER BY OutStkId;"),
        (f"商品「{p}」總共賣了多少", f"SELECT SUM(qty) AS total_qty FROM {V} WHERE {F} AND pName=N'{p}';"),
        (f"商品「{p}」的銷售總額", f"SELECT SUM(amtTotal) AS total FROM {V} WHERE {F} AND pName=N'{p}';"),
    ]

for mem in MEMBERS[:50]:
    T += [
        (f"會員「{mem}」的消費紀錄", f"SELECT OutStkId, OutStkDate, amount, memName FROM {V} WHERE {F} AND memName=N'{mem}' ORDER BY OutStkId;"),
        (f"會員「{mem}」的消費總額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F} AND memName=N'{mem}';"),
    ]

for eid in EMP_IDS:
    T.append((f"員工{eid}的銷售紀錄", f"SELECT DISTINCT OutStkId, OutStkDate, amount, empName FROM {V} WHERE {F} AND empId='{eid}' ORDER BY OutStkId;"))

for amt in [100, 500, 1000, 5000, 10000]:
    T += [
        (f"銷售金額超過{amt}的單據", f"SELECT DISTINCT OutStkId, OutStkDate, amount FROM {V} WHERE {F} AND amount>{amt} ORDER BY amount DESC;"),
        (f"Sales over {amt}", f"SELECT DISTINCT OutStkId, OutStkDate, amount FROM {V} WHERE {F} AND amount>{amt} ORDER BY amount DESC;"),
    ]

for bc in BARCODES[:40]:
    T.append((f"條碼{bc}的銷售紀錄", f"SELECT OutStkId, OutStkDate, pBarcode, pName, qty, amtTotal FROM {V} WHERE {F} AND pBarcode='{bc}' ORDER BY OutStkId;"))

for m in OS_MONTHS:
    for p in random.sample(OS_PRODS, min(10, len(OS_PRODS))):
        T.append((f"{fm(m)}商品「{p}」的銷售", f"SELECT OutStkId, OutStkDate, pName, qty, amtTotal FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}' AND pName=N'{p}';"))


# ═══════════════════════════════════════════
# WP_vTransfer (調撥單)
# ═══════════════════════════════════════════
V = "WP_M09.dbo.WP_vTransfer"
F = "isDel='N' AND dtlIsDel='N'"

T += [
    ("查詢所有調撥單", f"SELECT DISTINCT TransferId, TransferDate FROM {V} WHERE {F} ORDER BY TransferId;"),
    ("調撥單共有幾筆", f"SELECT COUNT(DISTINCT TransferId) AS cnt FROM {V} WHERE {F};"),
    ("所有調撥的商品清單", f"SELECT TransferId, pName, qty, costAvg FROM {V} WHERE {F} ORDER BY TransferId;"),
    ("How many transfers?", f"SELECT COUNT(DISTINCT TransferId) AS cnt FROM {V} WHERE {F};"),
    ("統計每月調撥筆數", f"SELECT LEFT(TransferId,6) AS ym, COUNT(DISTINCT TransferId) AS cnt FROM {V} WHERE {F} GROUP BY LEFT(TransferId,6) ORDER BY ym;"),
    ("每個商品的調撥總量", f"SELECT pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"),
    ("各倉庫調出的總數量", f"SELECT fWhName, SUM(qty) AS total FROM {V} WHERE {F} GROUP BY fWhName ORDER BY total DESC;"),
    ("各倉庫調入的總數量", f"SELECT tfWhName, SUM(qty) AS total FROM {V} WHERE {F} GROUP BY tfWhName ORDER BY total DESC;"),
    ("調撥數量最多的前10個商品", f"SELECT TOP 10 pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"),
    ("調撥成本總額", f"SELECT SUM(qty * costAvg) AS total_cost FROM {V} WHERE {F};"),
    ("調撥數量最多的前5個商品", f"SELECT TOP 5 pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"),
]

for m in TF_MONTHS:
    T += [
        (f"查詢{fm(m)}的調撥紀錄", f"SELECT DISTINCT TransferId, TransferDate FROM {V} WHERE {F} AND LEFT(TransferId,6)='{m}' ORDER BY TransferId;"),
        (f"{fm(m)}的調撥筆數", f"SELECT COUNT(DISTINCT TransferId) AS cnt FROM {V} WHERE {F} AND LEFT(TransferId,6)='{m}';"),
        (f"Transfers in {fm(m)}", f"SELECT DISTINCT TransferId, TransferDate FROM {V} WHERE {F} AND LEFT(TransferId,6)='{m}' ORDER BY TransferId;"),
    ]

for d in random.sample(TF_DAYS, min(25, len(TF_DAYS))):
    T.append((f"查詢{fd(d)}的調撥紀錄", f"SELECT DISTINCT TransferId, TransferDate FROM {V} WHERE {F} AND LEFT(TransferId,8)='{d}' ORDER BY TransferId;"))

for wh in WAREHOUSES:
    T += [
        (f"從「{wh}」調出的紀錄", f"SELECT TransferId, TransferDate, fWhName, tfWhName, pName, qty FROM {V} WHERE {F} AND fWhName=N'{wh}' ORDER BY TransferId;"),
        (f"調入「{wh}」的紀錄", f"SELECT TransferId, TransferDate, fWhName, tfWhName, pName, qty FROM {V} WHERE {F} AND tfWhName=N'{wh}' ORDER BY TransferId;"),
        (f"「{wh}」調出的商品總量", f"SELECT SUM(qty) AS total FROM {V} WHERE {F} AND fWhName=N'{wh}';"),
        (f"「{wh}」調入的商品總量", f"SELECT SUM(qty) AS total FROM {V} WHERE {F} AND tfWhName=N'{wh}';"),
    ]

for p in TF_PRODS:
    T += [
        (f"商品「{p}」的調撥紀錄", f"SELECT TransferId, TransferDate, fWhName, tfWhName, pName, qty FROM {V} WHERE {F} AND pName=N'{p}';"),
        (f"商品「{p}」的調撥總量", f"SELECT SUM(qty) AS total_qty FROM {V} WHERE {F} AND pName=N'{p}';"),
    ]

for eid in EMP_IDS:
    T.append((f"員工{eid}的調撥紀錄", f"SELECT DISTINCT TransferId, TransferDate, empId FROM {V} WHERE {F} AND empId='{eid}' ORDER BY TransferId;"))

for bc in BARCODES[:20]:
    T.append((f"條碼{bc}的調撥紀錄", f"SELECT TransferId, TransferDate, pBarcode, pName, qty FROM {V} WHERE {F} AND pBarcode='{bc}';"))

for m in TF_MONTHS:
    for wh in WAREHOUSES:
        T.append((f"{fm(m)}從「{wh}」調出的紀錄", f"SELECT TransferId, TransferDate, pName, qty FROM {V} WHERE {F} AND LEFT(TransferId,6)='{m}' AND fWhName=N'{wh}' ORDER BY TransferId;"))


# ═══════════════════════════════════════════
# WP_vInventory (庫存)
# ═══════════════════════════════════════════
V = "WP_M09.dbo.WP_vInventory"

T += [
    ("庫存商品共幾種", f"SELECT COUNT(DISTINCT pNo) AS cnt FROM {V};"),
    ("庫存總數量", f"SELECT SUM(qty) AS total_qty FROM {V};"),
    ("列出所有庫存商品", f"SELECT pNo, pName, pBarcode, qty, costAvg FROM {V} ORDER BY pNo;"),
    ("How many products in inventory?", f"SELECT COUNT(DISTINCT pNo) AS cnt FROM {V};"),
    ("Total inventory quantity?", f"SELECT SUM(qty) AS total_qty FROM {V};"),
    ("庫存為零的商品", f"SELECT pName, WarehouseName, qty FROM {V} WHERE qty=0;"),
    ("庫存低於安全庫存的商品", f"SELECT pName, WarehouseName, qty, qtySafe FROM {V} WHERE qty < qtySafe AND qtySafe > 0;"),
    ("庫存大於100的商品", f"SELECT pName, WarehouseName, qty FROM {V} WHERE qty>100 ORDER BY qty DESC;"),
    ("庫存大於50的商品", f"SELECT pName, WarehouseName, qty FROM {V} WHERE qty>50 ORDER BY qty DESC;"),
    ("庫存大於200的商品", f"SELECT pName, WarehouseName, qty FROM {V} WHERE qty>200 ORDER BY qty DESC;"),
    ("各倉庫庫存總量", f"SELECT WarehouseName, SUM(qty) AS total FROM {V} GROUP BY WarehouseName ORDER BY total DESC;"),
    ("各倉庫商品種類數", f"SELECT WarehouseName, COUNT(DISTINCT pNo) AS cnt FROM {V} GROUP BY WarehouseName ORDER BY cnt DESC;"),
    ("各供應商庫存商品數", f"SELECT pvName, COUNT(DISTINCT pNo) AS cnt FROM {V} WHERE pvName IS NOT NULL GROUP BY pvName ORDER BY cnt DESC;"),
    ("可銷售的庫存商品", f"SELECT pName, WarehouseName, qty FROM {V} WHERE isSale='Y' AND qty>0 ORDER BY qty DESC;"),
    ("不可銷售的庫存商品", f"SELECT pName, WarehouseName, qty FROM {V} WHERE isSale='N';"),
    ("庫存總金額", f"SELECT SUM(qty * costAvg) AS total_cost FROM {V} WHERE qty>0;"),
    ("各倉庫庫存金額", f"SELECT WarehouseName, SUM(qty * costAvg) AS total_cost FROM {V} WHERE qty>0 GROUP BY WarehouseName ORDER BY total_cost DESC;"),
    ("售價最高的前10個庫存商品", f"SELECT TOP 10 pName, priceStd FROM {V} ORDER BY priceStd DESC;"),
    ("成本最高的前10個庫存商品", f"SELECT TOP 10 pName, costStd FROM {V} ORDER BY costStd DESC;"),
    ("庫存最多的前10個商品", f"SELECT TOP 10 pName, WarehouseName, qty FROM {V} ORDER BY qty DESC;"),
    ("庫存最多的前20個商品", f"SELECT TOP 20 pName, WarehouseName, qty FROM {V} ORDER BY qty DESC;"),
]

for wh in WAREHOUSES:
    T += [
        (f"「{wh}」的庫存", f"SELECT pNo, pName, qty, costAvg FROM {V} WHERE WarehouseName=N'{wh}' ORDER BY pNo;"),
        (f"「{wh}」有多少種商品", f"SELECT COUNT(DISTINCT pNo) AS cnt FROM {V} WHERE WarehouseName=N'{wh}';"),
        (f"「{wh}」的庫存總量", f"SELECT SUM(qty) AS total_qty FROM {V} WHERE WarehouseName=N'{wh}';"),
        (f"「{wh}」的庫存金額", f"SELECT SUM(qty * costAvg) AS total_cost FROM {V} WHERE WarehouseName=N'{wh}' AND qty>0;"),
        (f"Inventory in {wh}", f"SELECT pNo, pName, qty, costAvg FROM {V} WHERE WarehouseName=N'{wh}' ORDER BY pNo;"),
    ]

for p in INV_PRODS:
    T += [
        (f"商品「{p}」的庫存", f"SELECT WarehouseName, pName, qty, costAvg FROM {V} WHERE pName=N'{p}';"),
        (f"商品「{p}」的總庫存", f"SELECT SUM(qty) AS total_qty FROM {V} WHERE pName=N'{p}';"),
    ]

for bc in BARCODES[:25]:
    T.append((f"條碼{bc}的庫存", f"SELECT WarehouseName, pName, pBarcode, qty FROM {V} WHERE pBarcode='{bc}';"))

for pv in INV_PVS[:50]:
    T.append((f"供應商「{pv}」的庫存商品", f"SELECT pName, WarehouseName, qty, costAvg FROM {V} WHERE pvName=N'{pv}';"))

for wh in WAREHOUSES:
    for p in random.sample(INV_PRODS, min(8, len(INV_PRODS))):
        T.append((f"「{wh}」的「{p}」庫存", f"SELECT pName, qty, costAvg FROM {V} WHERE WarehouseName=N'{wh}' AND pName=N'{p}';"))


# ═══════════════════════════════════════════
# WP_vProduct (商品主檔)
# ═══════════════════════════════════════════
V = "WP_M09.dbo.WP_vProduct"

T += [
    ("共有多少種商品", f"SELECT COUNT(*) AS cnt FROM {V};"),
    ("列出所有商品名稱", f"SELECT pNo, pName, pBarcode FROM {V} ORDER BY pNo;"),
    ("所有商品及售價", f"SELECT pNo, pName, priceStd, priceMem FROM {V} ORDER BY pNo;"),
    ("How many products?", f"SELECT COUNT(*) AS cnt FROM {V};"),
    ("List all products and prices", f"SELECT pNo, pName, priceStd FROM {V} ORDER BY pNo;"),
    ("可銷售的商品清單", f"SELECT pName, priceStd FROM {V} WHERE isSale='Y' ORDER BY pNo;"),
    ("不可銷售的商品", f"SELECT pName, priceStd FROM {V} WHERE isSale='N';"),
    ("需更新庫存的商品", f"SELECT pName FROM {V} WHERE isUpdStock='Y' ORDER BY pNo;"),
    ("含稅的商品", f"SELECT pName, priceStd, isTax FROM {V} WHERE isTax='Y' ORDER BY pNo;"),
    ("免稅的商品", f"SELECT pName, priceStd, isTax FROM {V} WHERE isTax='N' ORDER BY pNo;"),
    ("每個供應商的商品數", f"SELECT pvName, COUNT(*) AS cnt FROM {V} GROUP BY pvName ORDER BY cnt DESC;"),
    ("每個單位的商品數", f"SELECT pUName, COUNT(*) AS cnt FROM {V} GROUP BY pUName ORDER BY cnt DESC;"),
    ("平均售價", f"SELECT AVG(priceStd) AS avg_price FROM {V};"),
    ("平均成本", f"SELECT AVG(costStd) AS avg_cost FROM {V};"),
    ("庫存為零的商品", f"SELECT pName, qtyNow FROM {V} WHERE qtyNow=0;"),
    ("庫存大於零的商品數", f"SELECT COUNT(*) AS cnt FROM {V} WHERE qtyNow>0;"),
    ("有供應商折扣的商品", f"SELECT pName, pvName, isPvDiscount FROM {V} WHERE isPvDiscount='Y';"),
]

for n in [5, 10, 20]:
    T += [
        (f"售價最高的前{n}個商品", f"SELECT TOP {n} pName, priceStd FROM {V} ORDER BY priceStd DESC;"),
        (f"成本最高的前{n}個商品", f"SELECT TOP {n} pName, costStd FROM {V} ORDER BY costStd DESC;"),
        (f"庫存最多的前{n}個商品(商品主檔)", f"SELECT TOP {n} pName, qtyNow FROM {V} ORDER BY qtyNow DESC;"),
    ]

for p in PRODUCTS:
    T += [
        (f"商品「{p}」的詳細資料", f"SELECT pNo, pName, pBarcode, pCode, pUName, priceStd, priceLow, priceMem, costStd, costAvg, pvName FROM {V} WHERE pName=N'{p}';"),
        (f"商品「{p}」的售價", f"SELECT pName, priceStd FROM {V} WHERE pName=N'{p}';"),
    ]

for p in random.sample(PRODUCTS, min(60, len(PRODUCTS))):
    T += [
        (f"商品「{p}」的成本", f"SELECT pName, costStd, costAvg FROM {V} WHERE pName=N'{p}';"),
        (f"商品「{p}」的會員價", f"SELECT pName, priceMem FROM {V} WHERE pName=N'{p}';"),
    ]

for bc in BARCODES[:40]:
    T.append((f"條碼{bc}是什麼商品", f"SELECT pName, pBarcode, priceStd FROM {V} WHERE pBarcode='{bc}';"))

for pv in random.sample(PROVIDERS, min(50, len(PROVIDERS))):
    T += [
        (f"供應商「{pv}」提供的商品", f"SELECT pNo, pName, priceStd, costStd FROM {V} WHERE pvName=N'{pv}' ORDER BY pNo;"),
        (f"供應商「{pv}」有幾種商品", f"SELECT COUNT(*) AS cnt FROM {V} WHERE pvName=N'{pv}';"),
    ]

for price in [50, 100, 200, 500, 1000]:
    T += [
        (f"售價超過{price}元的商品", f"SELECT pName, priceStd FROM {V} WHERE priceStd>{price} ORDER BY priceStd DESC;"),
        (f"售價低於{price}元的商品", f"SELECT pName, priceStd FROM {V} WHERE priceStd<{price} ORDER BY priceStd ASC;"),
        (f"Products over {price}", f"SELECT pName, priceStd FROM {V} WHERE priceStd>{price} ORDER BY priceStd DESC;"),
    ]


# ═══════════════════════════════════════════
# WP_vProvider (供應商) - 注意：用 sn 不是 pvSn
# ═══════════════════════════════════════════
V = "WP_M09.dbo.WP_vProvider"

T += [
    ("共有多少供應商", f"SELECT COUNT(*) AS cnt FROM {V} WHERE sn > 0;"),
    ("列出所有供應商", f"SELECT sn, pvId, pvName, pvTel FROM {V} WHERE sn > 0 ORDER BY sn;"),
    ("啟用的供應商", f"SELECT sn, pvId, pvName FROM {V} WHERE isStop='N' AND sn > 0 ORDER BY sn;"),
    ("已停用的供應商", f"SELECT sn, pvId, pvName FROM {V} WHERE isStop='Y';"),
    ("How many suppliers?", f"SELECT COUNT(*) AS cnt FROM {V} WHERE sn > 0;"),
    ("Active suppliers", f"SELECT sn, pvId, pvName FROM {V} WHERE isStop='N' AND sn > 0 ORDER BY sn;"),
    ("各縣市供應商數量", f"SELECT pvCity, COUNT(*) AS cnt FROM {V} WHERE sn > 0 GROUP BY pvCity ORDER BY cnt DESC;"),
    ("有銀行帳戶的供應商", f"SELECT pvName, bankName, bankAccount FROM {V} WHERE bankAccount IS NOT NULL AND bankAccount<>'' AND sn > 0;"),
    ("有折扣的供應商", f"SELECT pvName, pvDiscount FROM {V} WHERE pvDiscount > 0 AND sn > 0;"),
    ("有聯絡人的供應商", f"SELECT pvName, ctactName, ctactTel FROM {V} WHERE ctactName IS NOT NULL AND ctactName<>'' AND sn > 0;"),
    ("有Email的供應商", f"SELECT pvName, email FROM {V} WHERE email IS NOT NULL AND email<>'' AND sn > 0;"),
    ("啟用與停用的供應商數量", f"SELECT isStop, COUNT(*) AS cnt FROM {V} WHERE sn > 0 GROUP BY isStop;"),
    ("各供應商類別的數量", f"SELECT pvKName, COUNT(*) AS cnt FROM {V} WHERE sn > 0 GROUP BY pvKName ORDER BY cnt DESC;"),
    ("有傳真號碼的供應商", f"SELECT pvName, fax FROM {V} WHERE fax IS NOT NULL AND fax<>'' AND sn > 0;"),
]

for pv in PROVIDERS:
    T += [
        (f"供應商「{pv}」的詳細資料", f"SELECT sn, pvId, pvName, pvBoss, pvTel, pvAddr, email, taxId FROM {V} WHERE pvName=N'{pv}';"),
        (f"供應商「{pv}」的電話", f"SELECT pvName, pvTel FROM {V} WHERE pvName=N'{pv}';"),
    ]

for pv in random.sample(PROVIDERS, min(60, len(PROVIDERS))):
    T += [
        (f"供應商「{pv}」的地址", f"SELECT pvName, pvCity, pvZone, pvAddr FROM {V} WHERE pvName=N'{pv}';"),
        (f"供應商「{pv}」的聯絡人", f"SELECT pvName, ctactName, ctactTel FROM {V} WHERE pvName=N'{pv}';"),
        (f"供應商「{pv}」是否停用", f"SELECT pvName, isStop FROM {V} WHERE pvName=N'{pv}';"),
    ]

for pv in random.sample(PROVIDERS, min(40, len(PROVIDERS))):
    T += [
        (f"供應商「{pv}」的銀行帳戶", f"SELECT pvName, bankName, bankAccount, bankAcctName FROM {V} WHERE pvName=N'{pv}';"),
        (f"供應商「{pv}」的統編", f"SELECT pvName, taxId FROM {V} WHERE pvName=N'{pv}';"),
        (f"供應商「{pv}」的負責人", f"SELECT pvName, pvBoss FROM {V} WHERE pvName=N'{pv}';"),
        (f"供應商「{pv}」的發票抬頭", f"SELECT pvName, invoTitle FROM {V} WHERE pvName=N'{pv}';"),
    ]

for sn in range(1, 25):
    T.append((f"供應商編號{sn}的資料", f"SELECT sn, pvId, pvName, pvTel, pvAddr FROM {V} WHERE sn={sn};"))

for pvid in PV_IDS[:25]:
    T.append((f"供應商代號{pvid}的資料", f"SELECT sn, pvId, pvName, pvTel FROM {V} WHERE pvId='{pvid}';"))


# ═══════════════════════════════════════════
# Dedup + Validate + Output
# ═══════════════════════════════════════════
print(f"\nTotal templates: {len(T)}")

seen = set()
unique = []
for q, sql in T:
    if q not in seen:
        seen.add(q)
        unique.append((q, sql))
print(f"After dedup: {len(unique)}")

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
    for q, sql, err in errors[:10]:
        print(f"  Q: {q}\n  SQL: {sql}\n  ERR: {err}\n")

# Shuffle and cap at 2000
random.shuffle(valid)
final = valid[:2000]

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

out = "data/wp_m09/train_claude_en_2000_v2.json"
with open(out, "w", encoding="utf-8") as f:
    json.dump(training, f, ensure_ascii=False, indent=2)

print(f"\nSaved {len(training)} items to {out}")

vc = Counter()
for q, sql in final:
    for vn in ["WP_vAcctIn", "WP_vAcctOut", "WP_vOutStock", "WP_vTransfer", "WP_vInventory", "WP_vProduct", "WP_vProvider"]:
        if vn in sql:
            vc[vn] += 1
            break

print("\nDistribution:")
for vn, cnt in sorted(vc.items()):
    print(f"  {vn}: {cnt}")

conn.close()
print("\nDone!")
