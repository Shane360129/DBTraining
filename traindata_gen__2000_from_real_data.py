"""
從 WP_M09 資料庫的 7 個 View 真實資料，生成 2000 題 Text-to-SQL 訓練集。
每段 SQL 都會在資料庫上驗證可執行且有回傳資料。

欄位規則：
- 只使用各 View 實際存在的欄位，不產生幻覺欄位
- WP_vProvider 沒有 pvSn 欄位，用 sn 代替
- *Id 結尾欄位為單號，前八碼 = 日期 (YYYYMMDD)
- 有 isDel 的 View 必須加 isDel='N' AND dtlIsDel='N'
- WP_vInventory / WP_vProduct / WP_vProvider 無日期篩選機制
"""

import json
import pyodbc
import random
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# ── DB Connection ──
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=SHANE\\SQLEXPRESS;DATABASE=WP_M09;Trusted_Connection=yes;",
    timeout=30,
)

# ── Schema: each view's actual columns ──
VIEW_COLUMNS = {}
cursor = conn.cursor()
for vn in [
    "WP_vAcctIn", "WP_vAcctOut", "WP_vOutStock",
    "WP_vTransfer", "WP_vInventory", "WP_vProduct", "WP_vProvider",
]:
    cursor.execute(
        f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_NAME='{vn}' ORDER BY ORDINAL_POSITION"
    )
    VIEW_COLUMNS[vn] = [r[0] for r in cursor.fetchall()]
cursor.close()


# ── Helper: fetch real values ──
def fetch_values(sql):
    c = conn.cursor()
    c.execute(sql)
    vals = [r[0] for r in c.fetchall() if r[0] is not None]
    c.close()
    return vals


# Real data pools
PRODUCTS = fetch_values(
    "SELECT DISTINCT TOP 100 pName FROM WP_M09.dbo.WP_vProduct "
    "WHERE pName IS NOT NULL AND pName <> ''"
)
PROVIDERS = fetch_values(
    "SELECT DISTINCT pvName FROM WP_M09.dbo.WP_vProvider WHERE sn > 0 AND pvName <> ''"
)
WAREHOUSES = fetch_values(
    "SELECT DISTINCT WarehouseName FROM WP_M09.dbo.WP_vInventory "
    "WHERE WarehouseName IS NOT NULL"
)
MEMBERS = fetch_values(
    "SELECT DISTINCT TOP 50 memName FROM WP_M09.dbo.WP_vOutStock "
    "WHERE memName IS NOT NULL AND memName <> ''"
)
BARCODES = fetch_values(
    "SELECT DISTINCT TOP 50 pBarcode FROM WP_M09.dbo.WP_vProduct "
    "WHERE pBarcode IS NOT NULL AND pBarcode <> ''"
)
EMPLOYEE_IDS = fetch_values(
    "SELECT DISTINCT empId FROM WP_M09.dbo.WP_vAcctOut WHERE empId IS NOT NULL"
)

# Date months available (YYYYMM)
DATE_MONTHS = [
    "202510", "202511", "202512", "202601", "202602", "202603",
]
DATE_DAYS = []
for m in DATE_MONTHS:
    for d in range(1, 29):
        DATE_DAYS.append(f"{m}{d:02d}")


def rp():
    return random.choice(PRODUCTS)


def rpv():
    return random.choice(PROVIDERS)


def rwh():
    return random.choice(WAREHOUSES)


def rmem():
    return random.choice(MEMBERS)


def rbc():
    return random.choice(BARCODES)


def rmonth():
    return random.choice(DATE_MONTHS)


def rday():
    return random.choice(DATE_DAYS)


def remp():
    return random.choice(EMPLOYEE_IDS)


# ── Template definitions per view ──
# Each template: (question_template, sql_template)
# Use {placeholders} that will be filled with real values


def gen_acct_in_templates():
    """WP_vAcctIn: 收款單 (30 cols, has isDel/dtlIsDel, acctInId前8碼=日期)"""
    templates = []
    V = "WP_M09.dbo.WP_vAcctIn"
    F = "isDel='N' AND dtlIsDel='N'"

    # -- 基本查詢 --
    templates.append((
        "查詢所有收款單的總金額",
        f"SELECT SUM(amount) AS total_amount FROM {V} WHERE {F};"
    ))
    templates.append((
        "查詢收款單共有幾筆",
        f"SELECT COUNT(DISTINCT acctInId) AS cnt FROM {V} WHERE {F};"
    ))
    templates.append((
        "列出所有收款單號及日期",
        f"SELECT DISTINCT acctInId, acctInDate FROM {V} WHERE {F} ORDER BY acctInId;"
    ))
    templates.append((
        "查詢收款單的平均金額",
        f"SELECT AVG(amount) AS avg_amount FROM {V} WHERE {F};"
    ))
    templates.append((
        "查詢收款金額最高的收款單",
        f"SELECT TOP 1 acctInId, amount FROM {V} WHERE {F} ORDER BY amount DESC;"
    ))
    templates.append((
        "查詢收款金額最低的收款單",
        f"SELECT TOP 1 acctInId, amount FROM {V} WHERE {F} ORDER BY amount ASC;"
    ))

    # -- 日期篩選 (用 LEFT(acctInId,8)) --
    for _ in range(15):
        m = rmonth()
        y, mm = m[:4], m[4:]
        templates.append((
            f"查詢{y}年{int(mm)}月的收款單",
            f"SELECT DISTINCT acctInId, acctInDate, amount FROM {V} WHERE {F} AND LEFT(acctInId,6)='{m}' ORDER BY acctInId;"
        ))
    for _ in range(10):
        d = rday()
        y, mm, dd = d[:4], d[4:6], d[6:]
        templates.append((
            f"查詢{y}年{int(mm)}月{int(dd)}日的收款單",
            f"SELECT DISTINCT acctInId, acctInDate, amount FROM {V} WHERE {F} AND LEFT(acctInId,8)='{d}' ORDER BY acctInId;"
        ))

    # -- 商品相關 --
    for _ in range(15):
        p = rp()
        templates.append((
            f"查詢商品「{p}」的收款紀錄",
            f"SELECT acctInId, acctInDate, amount, pName, oStkDtlAmt, oStkDtlQty FROM {V} WHERE {F} AND pName=N'{p}';"
        ))

    # -- 金額篩選 --
    for amt in [1000, 5000, 10000, 20000, 50000]:
        templates.append((
            f"查詢金額超過{amt}的收款單",
            f"SELECT DISTINCT acctInId, acctInDate, amount FROM {V} WHERE {F} AND amount>{amt} ORDER BY amount DESC;"
        ))
        templates.append((
            f"查詢金額低於{amt}的收款單",
            f"SELECT DISTINCT acctInId, acctInDate, amount FROM {V} WHERE {F} AND amount<{amt} ORDER BY amount ASC;"
        ))

    # -- 員工篩選 --
    for eid in EMPLOYEE_IDS:
        templates.append((
            f"查詢員工{eid}的收款紀錄",
            f"SELECT DISTINCT acctInId, acctInDate, amount FROM {V} WHERE {F} AND empId='{eid}' ORDER BY acctInId;"
        ))

    # -- 月份統計 --
    templates.append((
        "統計每月收款總額",
        f"SELECT LEFT(acctInId,6) AS ym, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY LEFT(acctInId,6) ORDER BY ym;"
    ))
    templates.append((
        "統計每月收款筆數",
        f"SELECT LEFT(acctInId,6) AS ym, COUNT(DISTINCT acctInId) AS cnt FROM {V} WHERE {F} GROUP BY LEFT(acctInId,6) ORDER BY ym;"
    ))

    # -- 會員相關 --
    for _ in range(5):
        mem = rmem()
        templates.append((
            f"查詢會員「{mem}」的收款紀錄",
            f"SELECT acctInId, acctInDate, amount, memName FROM {V} WHERE {F} AND memName=N'{mem}';"
        ))

    # -- 折扣相關 --
    templates.append((
        "查詢有折扣的收款明細",
        f"SELECT acctInId, pName, oStkDtlAmt, discount, discountShare FROM {V} WHERE {F} AND discount>0;"
    ))

    # -- 關聯出貨單 --
    templates.append((
        "查詢收款單對應的出貨單號",
        f"SELECT DISTINCT acctInId, OutStkId FROM {V} WHERE {F} ORDER BY acctInId;"
    ))

    return templates


def gen_acct_out_templates():
    """WP_vAcctOut: 付款單 (40 cols, has isDel/dtlIsDel, acctOutId前8碼=日期)"""
    templates = []
    V = "WP_M09.dbo.WP_vAcctOut"
    F = "isDel='N' AND dtlIsDel='N'"

    # -- 基本 --
    templates.append(("查詢所有付款單的總金額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F};"))
    templates.append(("查詢付款單共有幾筆", f"SELECT COUNT(DISTINCT acctOutId) AS cnt FROM {V} WHERE {F};"))
    templates.append(("查詢付款金額最高的付款單", f"SELECT TOP 1 acctOutId, amount FROM {V} WHERE {F} ORDER BY amount DESC;"))
    templates.append(("查詢付款金額最低的付款單", f"SELECT TOP 1 acctOutId, amount FROM {V} WHERE {F} ORDER BY amount ASC;"))
    templates.append(("查詢付款單的平均金額", f"SELECT AVG(amount) AS avg_amount FROM {V} WHERE {F};"))
    templates.append(("列出所有付款單號及日期", f"SELECT DISTINCT acctOutId, acctOutDate, amount FROM {V} WHERE {F} ORDER BY acctOutId;"))

    # -- 日期 --
    for _ in range(20):
        m = rmonth()
        y, mm = m[:4], m[4:]
        templates.append((
            f"查詢{y}年{int(mm)}月的付款單",
            f"SELECT DISTINCT acctOutId, acctOutDate, amount FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}' ORDER BY acctOutId;"
        ))
    for _ in range(15):
        d = rday()
        y, mm, dd = d[:4], d[4:6], d[6:]
        templates.append((
            f"查詢{y}年{int(mm)}月{int(dd)}日的付款單",
            f"SELECT DISTINCT acctOutId, acctOutDate, amount FROM {V} WHERE {F} AND LEFT(acctOutId,8)='{d}' ORDER BY acctOutId;"
        ))

    # -- 供應商 --
    for _ in range(30):
        pv = rpv()
        templates.append((
            f"查詢供應商「{pv}」的付款紀錄",
            f"SELECT acctOutId, acctOutDate, amount, pvName FROM {V} WHERE {F} AND pvName=N'{pv}' ORDER BY acctOutId;"
        ))
    for _ in range(10):
        pv = rpv()
        templates.append((
            f"供應商「{pv}」的付款總額是多少",
            f"SELECT SUM(amount) AS total FROM {V} WHERE {F} AND pvName=N'{pv}';"
        ))

    # -- 商品 --
    for _ in range(25):
        p = rp()
        templates.append((
            f"查詢商品「{p}」的付款明細",
            f"SELECT acctOutId, acctOutDate, pName, qty, amtTotal FROM {V} WHERE {F} AND pName=N'{p}';"
        ))

    # -- 金額 --
    for amt in [1000, 5000, 10000, 20000, 50000]:
        templates.append((
            f"查詢付款金額超過{amt}的單據",
            f"SELECT DISTINCT acctOutId, acctOutDate, amount, pvName FROM {V} WHERE {F} AND amount>{amt} ORDER BY amount DESC;"
        ))

    # -- 員工 --
    for eid in EMPLOYEE_IDS:
        templates.append((
            f"查詢員工{eid}處理的付款單",
            f"SELECT DISTINCT acctOutId, acctOutDate, amount, empName FROM {V} WHERE {F} AND empId='{eid}' ORDER BY acctOutId;"
        ))

    # -- 統計 --
    templates.append(("統計每月付款總額", f"SELECT LEFT(acctOutId,6) AS ym, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY LEFT(acctOutId,6) ORDER BY ym;"))
    templates.append(("統計每個供應商的付款總額", f"SELECT pvName, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY pvName ORDER BY total DESC;"))
    templates.append(("統計每個供應商的付款筆數", f"SELECT pvName, COUNT(DISTINCT acctOutId) AS cnt FROM {V} WHERE {F} GROUP BY pvName ORDER BY cnt DESC;"))
    templates.append(("查詢付款最多的前5個供應商", f"SELECT TOP 5 pvName, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY pvName ORDER BY total DESC;"))

    # -- 進貨單關聯 --
    templates.append(("查詢付款單對應的進貨單號", f"SELECT DISTINCT acctOutId, InStkId FROM {V} WHERE {F} ORDER BY acctOutId;"))

    # -- 含稅/未稅 --
    templates.append(("查詢含稅的付款明細", f"SELECT acctOutId, pName, amtTotal, amtNoneTax, isTax FROM {V} WHERE {F} AND isTax='Y';"))
    templates.append(("查詢未稅的付款明細", f"SELECT acctOutId, pName, amtTotal, amtNoneTax, isTax FROM {V} WHERE {F} AND isTax='N';"))

    # -- 轉帳金額 --
    templates.append(("查詢有轉帳金額的付款單", f"SELECT DISTINCT acctOutId, amount, transAmt FROM {V} WHERE {F} AND transAmt>0;"))

    # -- 日期+供應商 --
    for _ in range(10):
        m = rmonth()
        pv = rpv()
        y, mm = m[:4], m[4:]
        templates.append((
            f"查詢{y}年{int(mm)}月供應商「{pv}」的付款",
            f"SELECT acctOutId, acctOutDate, amount, pvName FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}' AND pvName=N'{pv}' ORDER BY acctOutId;"
        ))

    # -- TOP N --
    for n in [5, 10]:
        templates.append((
            f"查詢金額最高的{n}筆付款單",
            f"SELECT TOP {n} acctOutId, acctOutDate, amount, pvName FROM {V} WHERE {F} ORDER BY amount DESC;"
        ))

    return templates


def gen_outstock_templates():
    """WP_vOutStock: 出貨/銷售單 (67 cols, has isDel/dtlIsDel, OutStkId前8碼=日期)"""
    templates = []
    V = "WP_M09.dbo.WP_vOutStock"
    F = "isDel='N' AND dtlIsDel='N'"

    # -- 基本 --
    templates.append(("查詢所有銷售單的總金額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F};"))
    templates.append(("查詢銷售單共有幾筆", f"SELECT COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F};"))
    templates.append(("查詢銷售金額最高的單據", f"SELECT TOP 1 OutStkId, OutStkDate, amount FROM {V} WHERE {F} ORDER BY amount DESC;"))
    templates.append(("查詢銷售金額最低的單據", f"SELECT TOP 1 OutStkId, OutStkDate, amount FROM {V} WHERE {F} ORDER BY amount ASC;"))
    templates.append(("查詢銷售的平均金額", f"SELECT AVG(amount) AS avg_amount FROM {V} WHERE {F};"))

    # -- 日期 --
    for _ in range(25):
        m = rmonth()
        y, mm = m[:4], m[4:]
        templates.append((
            f"查詢{y}年{int(mm)}月的銷售紀錄",
            f"SELECT DISTINCT OutStkId, OutStkDate, amount FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}' ORDER BY OutStkId;"
        ))
    for _ in range(15):
        d = rday()
        y, mm, dd = d[:4], d[4:6], d[6:]
        templates.append((
            f"查詢{y}年{int(mm)}月{int(dd)}日的銷售紀錄",
            f"SELECT DISTINCT OutStkId, OutStkDate, amount FROM {V} WHERE {F} AND LEFT(OutStkId,8)='{d}' ORDER BY OutStkId;"
        ))

    # -- 商品 --
    for _ in range(30):
        p = rp()
        templates.append((
            f"查詢商品「{p}」的銷售紀錄",
            f"SELECT OutStkId, OutStkDate, pName, qty, dtlAmt, amtTotal FROM {V} WHERE {F} AND pName=N'{p}' ORDER BY OutStkId;"
        ))
    for _ in range(15):
        p = rp()
        templates.append((
            f"商品「{p}」總共賣了多少數量",
            f"SELECT SUM(qty) AS total_qty FROM {V} WHERE {F} AND pName=N'{p}';"
        ))
    for _ in range(15):
        p = rp()
        templates.append((
            f"商品「{p}」的銷售總額",
            f"SELECT SUM(amtTotal) AS total FROM {V} WHERE {F} AND pName=N'{p}';"
        ))

    # -- 會員 --
    for _ in range(20):
        mem = rmem()
        templates.append((
            f"查詢會員「{mem}」的消費紀錄",
            f"SELECT OutStkId, OutStkDate, amount, memName FROM {V} WHERE {F} AND memName=N'{mem}' ORDER BY OutStkId;"
        ))
    for _ in range(10):
        mem = rmem()
        templates.append((
            f"會員「{mem}」的消費總額是多少",
            f"SELECT SUM(amount) AS total FROM {V} WHERE {F} AND memName=N'{mem}';"
        ))

    # -- 員工 --
    for eid in EMPLOYEE_IDS:
        templates.append((
            f"查詢員工{eid}的銷售紀錄",
            f"SELECT DISTINCT OutStkId, OutStkDate, amount, empName FROM {V} WHERE {F} AND empId='{eid}' ORDER BY OutStkId;"
        ))

    # -- 金額篩選 --
    for amt in [100, 500, 1000, 5000, 10000]:
        templates.append((
            f"查詢銷售金額超過{amt}的單據",
            f"SELECT DISTINCT OutStkId, OutStkDate, amount FROM {V} WHERE {F} AND amount>{amt} ORDER BY amount DESC;"
        ))

    # -- 統計 --
    templates.append(("統計每月銷售總額", f"SELECT LEFT(OutStkId,6) AS ym, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY LEFT(OutStkId,6) ORDER BY ym;"))
    templates.append(("統計每月銷售筆數", f"SELECT LEFT(OutStkId,6) AS ym, COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} GROUP BY LEFT(OutStkId,6) ORDER BY ym;"))
    templates.append(("統計每個商品的銷售數量", f"SELECT pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"))
    templates.append(("統計每個商品的銷售金額", f"SELECT pName, SUM(amtTotal) AS total FROM {V} WHERE {F} GROUP BY pName ORDER BY total DESC;"))
    templates.append(("查詢銷售數量最多的前10個商品", f"SELECT TOP 10 pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"))
    templates.append(("查詢銷售金額最高的前10個商品", f"SELECT TOP 10 pName, SUM(amtTotal) AS total FROM {V} WHERE {F} GROUP BY pName ORDER BY total DESC;"))

    # -- 銷售類型 --
    templates.append(("查詢一般銷售的紀錄", f"SELECT DISTINCT OutStkId, OutStkDate, amount FROM {V} WHERE {F} AND outType='0' ORDER BY OutStkId;"))
    templates.append(("統計各銷售類型的筆數", f"SELECT outType, COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} GROUP BY outType;"))

    # -- 日期+商品 --
    for _ in range(15):
        m = rmonth()
        p = rp()
        y, mm = m[:4], m[4:]
        templates.append((
            f"查詢{y}年{int(mm)}月商品「{p}」的銷售",
            f"SELECT OutStkId, OutStkDate, pName, qty, amtTotal FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}' AND pName=N'{p}';"
        ))

    # -- 條碼查詢 --
    for _ in range(10):
        bc = rbc()
        templates.append((
            f"查詢條碼{bc}的銷售紀錄",
            f"SELECT OutStkId, OutStkDate, pBarcode, pName, qty, amtTotal FROM {V} WHERE {F} AND pBarcode='{bc}' ORDER BY OutStkId;"
        ))

    # -- 折扣 --
    templates.append(("查詢有折扣的銷售明細", f"SELECT OutStkId, pName, dtlAmt, dtlDiscnt, dtlDiscntShare FROM {V} WHERE {F} AND dtlDiscnt>0;"))

    # -- 倉庫 --
    templates.append(("統計各倉庫的銷售金額", f"SELECT whSn, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY whSn ORDER BY total DESC;"))

    # -- TOP N 金額 --
    for n in [5, 10, 20]:
        templates.append((
            f"查詢金額最高的前{n}筆銷售單",
            f"SELECT TOP {n} OutStkId, OutStkDate, amount FROM {V} WHERE {F} ORDER BY amount DESC;"
        ))

    return templates


def gen_transfer_templates():
    """WP_vTransfer: 調撥單 (20 cols, has isDel/dtlIsDel, TransferId前8碼=日期)"""
    templates = []
    V = "WP_M09.dbo.WP_vTransfer"
    F = "isDel='N' AND dtlIsDel='N'"

    # -- 基本 --
    templates.append(("查詢所有調撥單", f"SELECT DISTINCT TransferId, TransferDate FROM {V} WHERE {F} ORDER BY TransferId;"))
    templates.append(("查詢調撥單共有幾筆", f"SELECT COUNT(DISTINCT TransferId) AS cnt FROM {V} WHERE {F};"))
    templates.append(("查詢所有調撥的商品清單", f"SELECT TransferId, pName, qty, costAvg FROM {V} WHERE {F} ORDER BY TransferId;"))

    # -- 日期 --
    for _ in range(20):
        m = rmonth()
        y, mm = m[:4], m[4:]
        templates.append((
            f"查詢{y}年{int(mm)}月的調撥紀錄",
            f"SELECT DISTINCT TransferId, TransferDate FROM {V} WHERE {F} AND LEFT(TransferId,6)='{m}' ORDER BY TransferId;"
        ))
    for _ in range(10):
        d = rday()
        y, mm, dd = d[:4], d[4:6], d[6:]
        templates.append((
            f"查詢{y}年{int(mm)}月{int(dd)}日的調撥紀錄",
            f"SELECT DISTINCT TransferId, TransferDate FROM {V} WHERE {F} AND LEFT(TransferId,8)='{d}' ORDER BY TransferId;"
        ))

    # -- 倉庫 --
    for wh in WAREHOUSES:
        templates.append((
            f"查詢從「{wh}」調出的紀錄",
            f"SELECT TransferId, TransferDate, fWhName, tfWhName, pName, qty FROM {V} WHERE {F} AND fWhName=N'{wh}' ORDER BY TransferId;"
        ))
        templates.append((
            f"查詢調入「{wh}」的紀錄",
            f"SELECT TransferId, TransferDate, fWhName, tfWhName, pName, qty FROM {V} WHERE {F} AND tfWhName=N'{wh}' ORDER BY TransferId;"
        ))

    # -- 商品 --
    for _ in range(25):
        p = rp()
        templates.append((
            f"查詢商品「{p}」的調撥紀錄",
            f"SELECT TransferId, TransferDate, fWhName, tfWhName, pName, qty FROM {V} WHERE {F} AND pName=N'{p}';"
        ))
    for _ in range(10):
        p = rp()
        templates.append((
            f"商品「{p}」總共調撥了多少數量",
            f"SELECT SUM(qty) AS total_qty FROM {V} WHERE {F} AND pName=N'{p}';"
        ))

    # -- 員工 --
    for eid in EMPLOYEE_IDS:
        templates.append((
            f"查詢員工{eid}的調撥紀錄",
            f"SELECT DISTINCT TransferId, TransferDate, empId FROM {V} WHERE {F} AND empId='{eid}' ORDER BY TransferId;"
        ))

    # -- 統計 --
    templates.append(("統計每月調撥筆數", f"SELECT LEFT(TransferId,6) AS ym, COUNT(DISTINCT TransferId) AS cnt FROM {V} WHERE {F} GROUP BY LEFT(TransferId,6) ORDER BY ym;"))
    templates.append(("統計每個商品的調撥總量", f"SELECT pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"))
    templates.append(("統計各倉庫調出的總數量", f"SELECT fWhName, SUM(qty) AS total FROM {V} WHERE {F} GROUP BY fWhName ORDER BY total DESC;"))
    templates.append(("統計各倉庫調入的總數量", f"SELECT tfWhName, SUM(qty) AS total FROM {V} WHERE {F} GROUP BY tfWhName ORDER BY total DESC;"))
    templates.append(("查詢調撥數量最多的前10個商品", f"SELECT TOP 10 pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;"))

    # -- 日期+倉庫 --
    for _ in range(10):
        m = rmonth()
        wh = rwh()
        y, mm = m[:4], m[4:]
        templates.append((
            f"查詢{y}年{int(mm)}月從「{wh}」調出的紀錄",
            f"SELECT TransferId, TransferDate, pName, qty FROM {V} WHERE {F} AND LEFT(TransferId,6)='{m}' AND fWhName=N'{wh}' ORDER BY TransferId;"
        ))

    # -- 條碼 --
    for _ in range(5):
        bc = rbc()
        templates.append((
            f"查詢條碼{bc}的調撥紀錄",
            f"SELECT TransferId, TransferDate, pBarcode, pName, qty FROM {V} WHERE {F} AND pBarcode='{bc}';"
        ))

    # -- 成本 --
    templates.append(("查詢調撥成本總額", f"SELECT SUM(qty * costAvg) AS total_cost FROM {V} WHERE {F};"))

    return templates


def gen_inventory_templates():
    """WP_vInventory: 庫存 (25 cols, 無isDel, 無日期篩選)"""
    templates = []
    V = "WP_M09.dbo.WP_vInventory"

    # -- 基本 --
    templates.append(("查詢所有庫存商品數量", f"SELECT COUNT(DISTINCT pNo) AS cnt FROM {V};"))
    templates.append(("查詢庫存總數量", f"SELECT SUM(qty) AS total_qty FROM {V};"))
    templates.append(("列出所有庫存商品", f"SELECT pNo, pName, pBarcode, qty, costAvg FROM {V} ORDER BY pNo;"))

    # -- 倉庫 --
    for wh in WAREHOUSES:
        templates.append((
            f"查詢「{wh}」的庫存",
            f"SELECT pNo, pName, qty, costAvg FROM {V} WHERE WarehouseName=N'{wh}' ORDER BY pNo;"
        ))
        templates.append((
            f"「{wh}」有多少種商品",
            f"SELECT COUNT(DISTINCT pNo) AS cnt FROM {V} WHERE WarehouseName=N'{wh}';"
        ))
        templates.append((
            f"「{wh}」的庫存總量",
            f"SELECT SUM(qty) AS total_qty FROM {V} WHERE WarehouseName=N'{wh}';"
        ))

    # -- 商品 --
    for _ in range(30):
        p = rp()
        templates.append((
            f"查詢商品「{p}」的庫存",
            f"SELECT WarehouseName, pName, qty, costAvg FROM {V} WHERE pName=N'{p}';"
        ))
    for _ in range(15):
        p = rp()
        templates.append((
            f"商品「{p}」的總庫存是多少",
            f"SELECT SUM(qty) AS total_qty FROM {V} WHERE pName=N'{p}';"
        ))

    # -- 條碼 --
    for _ in range(10):
        bc = rbc()
        templates.append((
            f"查詢條碼{bc}的庫存",
            f"SELECT WarehouseName, pName, pBarcode, qty FROM {V} WHERE pBarcode='{bc}';"
        ))

    # -- 價格 --
    templates.append(("查詢售價最高的前10個商品", f"SELECT TOP 10 pName, priceStd FROM {V} ORDER BY priceStd DESC;"))
    templates.append(("查詢成本最高的前10個商品", f"SELECT TOP 10 pName, costStd FROM {V} ORDER BY costStd DESC;"))

    # -- 庫存警示 --
    templates.append(("查詢庫存為零的商品", f"SELECT pName, WarehouseName, qty FROM {V} WHERE qty=0;"))
    templates.append(("查詢庫存低於安全庫存的商品", f"SELECT pName, WarehouseName, qty, qtySafe FROM {V} WHERE qty < qtySafe AND qtySafe > 0;"))
    templates.append(("查詢庫存大於100的商品", f"SELECT pName, WarehouseName, qty FROM {V} WHERE qty>100 ORDER BY qty DESC;"))

    # -- 統計 --
    templates.append(("統計各倉庫的庫存總量", f"SELECT WarehouseName, SUM(qty) AS total FROM {V} GROUP BY WarehouseName ORDER BY total DESC;"))
    templates.append(("統計各倉庫的商品種類數", f"SELECT WarehouseName, COUNT(DISTINCT pNo) AS cnt FROM {V} GROUP BY WarehouseName ORDER BY cnt DESC;"))
    templates.append(("統計各供應商的庫存商品數", f"SELECT pvName, COUNT(DISTINCT pNo) AS cnt FROM {V} WHERE pvName IS NOT NULL GROUP BY pvName ORDER BY cnt DESC;"))

    # -- 供應商 --
    for _ in range(15):
        pv = rpv()
        templates.append((
            f"查詢供應商「{pv}」的庫存商品",
            f"SELECT pName, WarehouseName, qty, costAvg FROM {V} WHERE pvName=N'{pv}';"
        ))

    # -- 銷售狀態 --
    templates.append(("查詢可銷售的庫存商品", f"SELECT pName, WarehouseName, qty FROM {V} WHERE isSale='Y' AND qty>0 ORDER BY qty DESC;"))
    templates.append(("查詢不可銷售的庫存商品", f"SELECT pName, WarehouseName, qty FROM {V} WHERE isSale='N';"))

    # -- 倉庫+商品 --
    for _ in range(10):
        wh = rwh()
        p = rp()
        templates.append((
            f"查詢「{wh}」的商品「{p}」庫存",
            f"SELECT pName, qty, costAvg FROM {V} WHERE WarehouseName=N'{wh}' AND pName=N'{p}';"
        ))

    # -- 庫存金額 --
    templates.append(("查詢庫存總金額（成本）", f"SELECT SUM(qty * costAvg) AS total_cost FROM {V} WHERE qty>0;"))
    templates.append(("各倉庫的庫存總金額", f"SELECT WarehouseName, SUM(qty * costAvg) AS total_cost FROM {V} WHERE qty>0 GROUP BY WarehouseName ORDER BY total_cost DESC;"))

    return templates


def gen_product_templates():
    """WP_vProduct: 商品主檔 (30 cols, 無isDel, 無日期篩選)"""
    templates = []
    V = "WP_M09.dbo.WP_vProduct"

    # -- 基本 --
    templates.append(("查詢共有多少種商品", f"SELECT COUNT(*) AS cnt FROM {V};"))
    templates.append(("列出所有商品名稱", f"SELECT pNo, pName, pBarcode FROM {V} ORDER BY pNo;"))
    templates.append(("查詢所有商品及售價", f"SELECT pNo, pName, priceStd, priceMem FROM {V} ORDER BY pNo;"))

    # -- 商品查詢 --
    for _ in range(30):
        p = rp()
        templates.append((
            f"查詢商品「{p}」的詳細資料",
            f"SELECT pNo, pName, pBarcode, pCode, pUName, priceStd, priceLow, priceMem, costStd, costAvg, pvName FROM {V} WHERE pName=N'{p}';"
        ))
    for _ in range(10):
        p = rp()
        templates.append((
            f"商品「{p}」的售價是多少",
            f"SELECT pName, priceStd FROM {V} WHERE pName=N'{p}';"
        ))
    for _ in range(10):
        p = rp()
        templates.append((
            f"商品「{p}」的成本是多少",
            f"SELECT pName, costStd, costAvg FROM {V} WHERE pName=N'{p}';"
        ))
    for _ in range(10):
        p = rp()
        templates.append((
            f"商品「{p}」的會員價是多少",
            f"SELECT pName, priceMem FROM {V} WHERE pName=N'{p}';"
        ))

    # -- 條碼 --
    for _ in range(15):
        bc = rbc()
        templates.append((
            f"查詢條碼{bc}是什麼商品",
            f"SELECT pName, pBarcode, priceStd FROM {V} WHERE pBarcode='{bc}';"
        ))

    # -- 供應商 --
    for _ in range(20):
        pv = rpv()
        templates.append((
            f"查詢供應商「{pv}」提供的商品",
            f"SELECT pNo, pName, priceStd, costStd FROM {V} WHERE pvName=N'{pv}' ORDER BY pNo;"
        ))
    for _ in range(10):
        pv = rpv()
        templates.append((
            f"供應商「{pv}」有多少種商品",
            f"SELECT COUNT(*) AS cnt FROM {V} WHERE pvName=N'{pv}';"
        ))

    # -- 價格篩選 --
    for price in [50, 100, 200, 500, 1000]:
        templates.append((
            f"查詢售價超過{price}元的商品",
            f"SELECT pName, priceStd FROM {V} WHERE priceStd>{price} ORDER BY priceStd DESC;"
        ))
        templates.append((
            f"查詢售價低於{price}元的商品",
            f"SELECT pName, priceStd FROM {V} WHERE priceStd<{price} ORDER BY priceStd ASC;"
        ))

    # -- TOP N --
    for n in [5, 10]:
        templates.append((f"查詢售價最高的前{n}個商品", f"SELECT TOP {n} pName, priceStd FROM {V} ORDER BY priceStd DESC;"))
        templates.append((f"查詢成本最高的前{n}個商品", f"SELECT TOP {n} pName, costStd FROM {V} ORDER BY costStd DESC;"))
        templates.append((f"查詢庫存最多的前{n}個商品", f"SELECT TOP {n} pName, qtyNow FROM {V} ORDER BY qtyNow DESC;"))

    # -- 銷售狀態 --
    templates.append(("查詢可銷售的商品", f"SELECT pName, priceStd FROM {V} WHERE isSale='Y' ORDER BY pNo;"))
    templates.append(("查詢不可銷售的商品", f"SELECT pName, priceStd FROM {V} WHERE isSale='N';"))
    templates.append(("查詢需更新庫存的商品", f"SELECT pName FROM {V} WHERE isUpdStock='Y' ORDER BY pNo;"))

    # -- 含稅 --
    templates.append(("查詢含稅的商品", f"SELECT pName, priceStd, isTax FROM {V} WHERE isTax='Y' ORDER BY pNo;"))
    templates.append(("查詢免稅的商品", f"SELECT pName, priceStd, isTax FROM {V} WHERE isTax='N' ORDER BY pNo;"))

    # -- 統計 --
    templates.append(("統計每個供應商的商品數", f"SELECT pvName, COUNT(*) AS cnt FROM {V} GROUP BY pvName ORDER BY cnt DESC;"))
    templates.append(("統計每個單位的商品數", f"SELECT pUName, COUNT(*) AS cnt FROM {V} GROUP BY pUName ORDER BY cnt DESC;"))
    templates.append(("查詢平均售價", f"SELECT AVG(priceStd) AS avg_price FROM {V};"))
    templates.append(("查詢平均成本", f"SELECT AVG(costStd) AS avg_cost FROM {V};"))

    # -- 庫存為零 --
    templates.append(("查詢目前庫存為零的商品", f"SELECT pName, qtyNow FROM {V} WHERE qtyNow=0;"))
    templates.append(("查詢庫存大於零的商品數量", f"SELECT COUNT(*) AS cnt FROM {V} WHERE qtyNow>0;"))

    # -- 有供應商折扣 --
    templates.append(("查詢有供應商折扣的商品", f"SELECT pName, pvName, isPvDiscount FROM {V} WHERE isPvDiscount='Y';"))

    return templates


def gen_provider_templates():
    """WP_vProvider: 供應商 (32 cols, 無isDel, 用isStop判斷停用, 用sn不是pvSn!)"""
    templates = []
    V = "WP_M09.dbo.WP_vProvider"

    # -- 基本 --
    templates.append(("查詢共有多少供應商", f"SELECT COUNT(*) AS cnt FROM {V} WHERE sn > 0;"))
    templates.append(("列出所有供應商", f"SELECT sn, pvId, pvName, pvTel FROM {V} WHERE sn > 0 ORDER BY sn;"))
    templates.append(("查詢目前啟用的供應商", f"SELECT sn, pvId, pvName FROM {V} WHERE isStop='N' AND sn > 0 ORDER BY sn;"))
    templates.append(("查詢已停用的供應商", f"SELECT sn, pvId, pvName FROM {V} WHERE isStop='Y';"))

    # -- 個別供應商 --
    for _ in range(30):
        pv = rpv()
        templates.append((
            f"查詢供應商「{pv}」的詳細資料",
            f"SELECT sn, pvId, pvName, pvBoss, pvTel, pvAddr, email, taxId FROM {V} WHERE pvName=N'{pv}';"
        ))
    for _ in range(10):
        pv = rpv()
        templates.append((
            f"供應商「{pv}」的電話是什麼",
            f"SELECT pvName, pvTel FROM {V} WHERE pvName=N'{pv}';"
        ))
    for _ in range(10):
        pv = rpv()
        templates.append((
            f"供應商「{pv}」的地址",
            f"SELECT pvName, pvCity, pvZone, pvAddr FROM {V} WHERE pvName=N'{pv}';"
        ))
    for _ in range(10):
        pv = rpv()
        templates.append((
            f"供應商「{pv}」的聯絡人資訊",
            f"SELECT pvName, ctactName, ctactTel FROM {V} WHERE pvName=N'{pv}';"
        ))
    for _ in range(10):
        pv = rpv()
        templates.append((
            f"供應商「{pv}」是否停用",
            f"SELECT pvName, isStop FROM {V} WHERE pvName=N'{pv}';"
        ))
    for _ in range(10):
        pv = rpv()
        templates.append((
            f"供應商「{pv}」的銀行帳戶",
            f"SELECT pvName, bankName, bankAccount, bankAcctName FROM {V} WHERE pvName=N'{pv}';"
        ))
    for _ in range(5):
        pv = rpv()
        templates.append((
            f"供應商「{pv}」的統一編號",
            f"SELECT pvName, taxId FROM {V} WHERE pvName=N'{pv}';"
        ))

    # -- 用 sn 查詢 --
    for sn in range(1, 11):
        templates.append((
            f"查詢供應商編號{sn}的資料",
            f"SELECT sn, pvId, pvName, pvTel, pvAddr FROM {V} WHERE sn={sn};"
        ))

    # -- 地區 --
    templates.append(("統計各縣市的供應商數量", f"SELECT pvCity, COUNT(*) AS cnt FROM {V} WHERE sn > 0 GROUP BY pvCity ORDER BY cnt DESC;"))

    # -- 銀行帳戶 --
    templates.append(("查詢有銀行帳戶的供應商", f"SELECT pvName, bankName, bankAccount FROM {V} WHERE bankAccount IS NOT NULL AND bankAccount<>'' AND sn > 0;"))

    # -- 有折扣 --
    templates.append(("查詢有折扣的供應商", f"SELECT pvName, pvDiscount FROM {V} WHERE pvDiscount > 0 AND sn > 0;"))

    # -- 聯絡人 --
    templates.append(("查詢有聯絡人的供應商", f"SELECT pvName, ctactName, ctactTel FROM {V} WHERE ctactName IS NOT NULL AND ctactName<>'' AND sn > 0;"))

    # -- Email --
    templates.append(("查詢有Email的供應商", f"SELECT pvName, email FROM {V} WHERE email IS NOT NULL AND email<>'' AND sn > 0;"))

    # -- 統計 --
    templates.append(("統計啟用與停用的供應商數量", f"SELECT isStop, COUNT(*) AS cnt FROM {V} WHERE sn > 0 GROUP BY isStop;"))
    templates.append(("統計各供應商類別的數量", f"SELECT pvKName, COUNT(*) AS cnt FROM {V} WHERE sn > 0 GROUP BY pvKName ORDER BY cnt DESC;"))

    # -- 負責人 --
    for _ in range(5):
        pv = rpv()
        templates.append((
            f"供應商「{pv}」的負責人是誰",
            f"SELECT pvName, pvBoss FROM {V} WHERE pvName=N'{pv}';"
        ))

    # -- 發票抬頭 --
    for _ in range(5):
        pv = rpv()
        templates.append((
            f"供應商「{pv}」的發票抬頭",
            f"SELECT pvName, invoTitle FROM {V} WHERE pvName=N'{pv}';"
        ))

    return templates


# ── Generate all templates ──
print("Generating templates...")
all_templates = []
generators = [
    ("WP_vAcctIn", gen_acct_in_templates),
    ("WP_vAcctOut", gen_acct_out_templates),
    ("WP_vOutStock", gen_outstock_templates),
    ("WP_vTransfer", gen_transfer_templates),
    ("WP_vInventory", gen_inventory_templates),
    ("WP_vProduct", gen_product_templates),
    ("WP_vProvider", gen_provider_templates),
]

for view_name, gen_fn in generators:
    templates = gen_fn()
    for q, sql in templates:
        all_templates.append((view_name, q, sql))
    print(f"  {view_name}: {len(templates)} templates")

print(f"Total templates: {len(all_templates)}")

# ── Deduplicate by question ──
seen_questions = set()
unique_templates = []
for view_name, q, sql in all_templates:
    if q not in seen_questions:
        seen_questions.add(q)
        unique_templates.append((view_name, q, sql))

print(f"After dedup: {len(unique_templates)}")

# ── Validate each SQL on DB ──
print("Validating SQL on database...")
valid_items = []
error_items = []
empty_items = []

for i, (view_name, question, sql) in enumerate(unique_templates):
    try:
        c = conn.cursor()
        c.execute(sql)
        rows = c.fetchall()
        c.close()
        if len(rows) == 0:
            empty_items.append((view_name, question, sql, "empty"))
        elif len(rows) == 1 and all(v is None for v in rows[0]):
            empty_items.append((view_name, question, sql, "all_null"))
        else:
            valid_items.append((view_name, question, sql))
    except Exception as e:
        error_items.append((view_name, question, sql, str(e)))

    if (i + 1) % 200 == 0:
        print(f"  Validated {i+1}/{len(unique_templates)}...")

print(f"\nValidation results:")
print(f"  Valid (has data): {len(valid_items)}")
print(f"  Empty/NULL: {len(empty_items)}")
print(f"  Errors: {len(error_items)}")

if error_items:
    print("\n=== ERRORS ===")
    for vn, q, sql, err in error_items[:20]:
        print(f"  [{vn}] {q}")
        print(f"    SQL: {sql}")
        print(f"    ERR: {err}\n")

# ── Build Spider format output ──
print("\nBuilding training set...")
random.shuffle(valid_items)

# Cap at 2000
if len(valid_items) > 2000:
    valid_items = valid_items[:2000]

training_data = []
for view_name, question, sql in valid_items:
    # Tokenize
    query_toks = []
    for token in sql.replace("(", " ( ").replace(")", " ) ").replace(",", " , ").replace(";", " ;").split():
        query_toks.append(token)

    item = {
        "db_id": "WP_M09",
        "query": sql,
        "query_toks": query_toks,
        "query_toks_no_value": query_toks,  # simplified
        "question": question,
        "question_toks": list(question),
        "sql": {
            "from": {"table_units": [], "conds": []},
            "select": [False, []],
            "where": [],
            "groupBy": [],
            "having": [],
            "orderBy": [],
            "limit": None,
            "intersect": None,
            "union": None,
            "except": None,
        },
    }
    training_data.append(item)

output_path = "data/wp_m09/train_claude_en_2000_v2.json"
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(training_data, f, ensure_ascii=False, indent=2)

print(f"\nSaved {len(training_data)} items to {output_path}")

# Stats by view
from collections import Counter
view_counts = Counter(vn for vn, _, _ in valid_items[:len(training_data)])
print("\nDistribution by view:")
for vn, cnt in sorted(view_counts.items()):
    print(f"  {vn}: {cnt}")

conn.close()
