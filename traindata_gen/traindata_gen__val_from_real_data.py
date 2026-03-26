"""
從 7 個 View 的第二批真實資料建立驗證集。
使用與訓練集不同的資料（offset 20 起），確保不重疊。
每段 SQL 驗證可執行且有回傳資料。
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


# ── 專門撈驗證用的不同資料 (offset / 不同範圍) ──
# 用與訓練集不同的商品名、供應商名、日期
VAL_PRODS_PRODUCT = fv("SELECT pName FROM WP_M09.dbo.WP_vProduct ORDER BY pNo OFFSET 20 ROWS FETCH NEXT 30 ROWS ONLY")
VAL_PRODS_OS = fv("SELECT DISTINCT TOP 30 pName FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND pName IS NOT NULL AND pName<>'' ORDER BY pName DESC")
VAL_PRODS_AO = fv("SELECT DISTINCT TOP 30 pName FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' AND pName IS NOT NULL AND pName<>'' ORDER BY pName DESC")
VAL_PRODS_TF = fv("SELECT DISTINCT TOP 30 pName FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N' AND pName IS NOT NULL AND pName<>'' ORDER BY pName DESC")
VAL_PRODS_INV = fv("SELECT DISTINCT TOP 30 pName FROM WP_M09.dbo.WP_vInventory WHERE pName IS NOT NULL ORDER BY pName DESC")
VAL_PRODS_AI = fv("SELECT DISTINCT TOP 20 pName FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND pName IS NOT NULL AND pName<>'' ORDER BY pName DESC")

VAL_PVS = fv("SELECT pvName FROM WP_M09.dbo.WP_vProvider WHERE sn > 0 ORDER BY sn OFFSET 20 ROWS FETCH NEXT 30 ROWS ONLY")
VAL_AO_PVS = fv("SELECT DISTINCT TOP 30 pvName FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' AND pvName IS NOT NULL AND pvName<>'' ORDER BY pvName DESC")
VAL_INV_PVS = fv("SELECT DISTINCT TOP 30 pvName FROM WP_M09.dbo.WP_vInventory WHERE pvName IS NOT NULL ORDER BY pvName DESC")

VAL_MEMBERS = fv("SELECT DISTINCT TOP 30 memName FROM WP_M09.dbo.WP_vOutStock WHERE memName IS NOT NULL AND memName<>'' ORDER BY memName DESC")

VAL_BARCODES = fv("SELECT pBarcode FROM WP_M09.dbo.WP_vProduct WHERE pBarcode IS NOT NULL AND pBarcode<>'' ORDER BY pNo OFFSET 20 ROWS FETCH NEXT 30 ROWS ONLY")

WAREHOUSES = fv("SELECT DISTINCT WarehouseName FROM WP_M09.dbo.WP_vInventory WHERE WarehouseName IS NOT NULL")
EMP_IDS = fv("SELECT DISTINCT empId FROM WP_M09.dbo.WP_vAcctOut WHERE empId IS NOT NULL")

# 用不同的日期
AI_DAYS = fv("SELECT DISTINCT LEFT(acctInId,8) FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' ORDER BY LEFT(acctInId,8) DESC")
AO_DAYS = fv("SELECT DISTINCT TOP 20 LEFT(acctOutId,8) FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' ORDER BY LEFT(acctOutId,8) DESC")
OS_DAYS = fv("SELECT DISTINCT TOP 30 LEFT(OutStkId,8) FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' ORDER BY LEFT(OutStkId,8) DESC")
TF_DAYS = fv("SELECT DISTINCT TOP 20 LEFT(TransferId,8) FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N' ORDER BY LEFT(TransferId,8) DESC")

AI_MONTHS = fv("SELECT DISTINCT LEFT(acctInId,6) FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N'")
AO_MONTHS = fv("SELECT DISTINCT LEFT(acctOutId,6) FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N'")
OS_MONTHS = fv("SELECT DISTINCT LEFT(OutStkId,6) FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N'")
TF_MONTHS = fv("SELECT DISTINCT LEFT(TransferId,6) FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N'")

print(f"Val products: Product={len(VAL_PRODS_PRODUCT)}, OS={len(VAL_PRODS_OS)}, AO={len(VAL_PRODS_AO)}")
print(f"Val providers: PV={len(VAL_PVS)}, AO={len(VAL_AO_PVS)}")
print(f"Val members={len(VAL_MEMBERS)}, barcodes={len(VAL_BARCODES)}")


def fd(d): return f"{d[:4]}年{int(d[4:6])}月{int(d[6:])}日"
def fm(m): return f"{m[:4]}年{int(m[4:])}月"


T = []  # (question, sql, difficulty)

# ═══════════════════════════════════════════
# WP_vAcctIn - 驗證題
# ═══════════════════════════════════════════
V = "WP_M09.dbo.WP_vAcctIn"; F = "isDel='N' AND dtlIsDel='N'"

# easy
T += [
    ("查詢全部收款單總金額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F};", "easy"),
    ("收款單總共幾筆", f"SELECT COUNT(DISTINCT acctInId) AS cnt FROM {V} WHERE {F};", "easy"),
    ("收款單的最高金額", f"SELECT TOP 1 acctInId, amount FROM {V} WHERE {F} ORDER BY amount DESC;", "easy"),
    ("列出全部收款單號和金額", f"SELECT DISTINCT acctInId, acctInDate, amount FROM {V} WHERE {F} ORDER BY acctInId;", "easy"),
    ("What is the total amount of all collections?", f"SELECT SUM(amount) AS total FROM {V} WHERE {F};", "easy"),
]

# medium
for d in AI_DAYS[:5]:
    T.append((f"{fd(d)}有哪些收款單", f"SELECT DISTINCT acctInId, acctInDate, amount FROM {V} WHERE {F} AND LEFT(acctInId,8)='{d}' ORDER BY acctInId;", "medium"))

for m in AI_MONTHS:
    T.append((f"{fm(m)}收款了多少錢", f"SELECT SUM(amount) AS total FROM {V} WHERE {F} AND LEFT(acctInId,6)='{m}';", "medium"))

for p in VAL_PRODS_AI[:8]:
    T.append((f"「{p}」的收款有幾筆", f"SELECT COUNT(*) AS cnt FROM {V} WHERE {F} AND pName=N'{p}';", "medium"))

# hard
T.append(("每月收款平均金額", f"SELECT LEFT(acctInId,6) AS ym, AVG(amount) AS avg_amt FROM {V} WHERE {F} GROUP BY LEFT(acctInId,6) ORDER BY ym;", "hard"))
T.append(("每筆收款單包含幾個明細", f"SELECT acctInId, COUNT(*) AS detail_cnt FROM {V} WHERE {F} GROUP BY acctInId ORDER BY detail_cnt DESC;", "hard"))


# ═══════════════════════════════════════════
# WP_vAcctOut - 驗證題
# ═══════════════════════════════════════════
V = "WP_M09.dbo.WP_vAcctOut"; F = "isDel='N' AND dtlIsDel='N'"

# easy
T += [
    ("全部付款單總金額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F};", "easy"),
    ("付款單總共幾筆", f"SELECT COUNT(DISTINCT acctOutId) AS cnt FROM {V} WHERE {F};", "easy"),
    ("付款最高金額的單據", f"SELECT TOP 1 acctOutId, amount FROM {V} WHERE {F} ORDER BY amount DESC;", "easy"),
    ("列出全部付款單", f"SELECT DISTINCT acctOutId, acctOutDate, amount FROM {V} WHERE {F} ORDER BY acctOutId;", "easy"),
    ("Total payment amount?", f"SELECT SUM(amount) AS total FROM {V} WHERE {F};", "easy"),
]

# medium - 日期
for d in AO_DAYS[:8]:
    T.append((f"{fd(d)}的付款單明細", f"SELECT DISTINCT acctOutId, acctOutDate, amount, pvName FROM {V} WHERE {F} AND LEFT(acctOutId,8)='{d}' ORDER BY acctOutId;", "medium"))

for m in AO_MONTHS:
    T.append((f"{fm(m)}付了多少錢", f"SELECT SUM(amount) AS total FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}';", "medium"))

# medium - 供應商
for pv in VAL_AO_PVS[:10]:
    T.append((f"「{pv}」的付款共幾筆", f"SELECT COUNT(DISTINCT acctOutId) AS cnt FROM {V} WHERE {F} AND pvName=N'{pv}';", "medium"))

# medium - 商品
for p in VAL_PRODS_AO[:8]:
    T.append((f"「{p}」的進貨付款金額", f"SELECT SUM(amtTotal) AS total FROM {V} WHERE {F} AND pName=N'{p}';", "medium"))

# hard
T.append(("哪個供應商付款金額最高", f"SELECT TOP 1 pvName, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY pvName ORDER BY total DESC;", "hard"))
T.append(("每個供應商的平均付款金額", f"SELECT pvName, AVG(amount) AS avg_amt FROM {V} WHERE {F} GROUP BY pvName ORDER BY avg_amt DESC;", "hard"))
T.append(("每月付款給幾個不同供應商", f"SELECT LEFT(acctOutId,6) AS ym, COUNT(DISTINCT pvName) AS pv_cnt FROM {V} WHERE {F} GROUP BY LEFT(acctOutId,6) ORDER BY ym;", "hard"))

# medium - 日期+供應商
for m in AO_MONTHS[:2]:
    for pv in VAL_AO_PVS[:3]:
        T.append((f"{fm(m)}付給「{pv}」多少", f"SELECT SUM(amount) AS total FROM {V} WHERE {F} AND LEFT(acctOutId,6)='{m}' AND pvName=N'{pv}';", "medium"))


# ═══════════════════════════════════════════
# WP_vOutStock - 驗證題
# ═══════════════════════════════════════════
V = "WP_M09.dbo.WP_vOutStock"; F = "isDel='N' AND dtlIsDel='N'"

# easy
T += [
    ("全部銷售總金額", f"SELECT SUM(amount) AS total FROM {V} WHERE {F};", "easy"),
    ("銷售單總共幾筆", f"SELECT COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F};", "easy"),
    ("銷售最高金額的單據", f"SELECT TOP 1 OutStkId, OutStkDate, amount FROM {V} WHERE {F} ORDER BY amount DESC;", "easy"),
    ("Total sales amount?", f"SELECT SUM(amount) AS total FROM {V} WHERE {F};", "easy"),
    ("How many sales transactions?", f"SELECT COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F};", "easy"),
]

# medium - 日期
for d in OS_DAYS[:12]:
    T.append((f"{fd(d)}的銷售明細", f"SELECT DISTINCT OutStkId, OutStkDate, amount FROM {V} WHERE {F} AND LEFT(OutStkId,8)='{d}' ORDER BY OutStkId;", "medium"))

for m in OS_MONTHS:
    T.append((f"{fm(m)}銷售了多少", f"SELECT SUM(amount) AS total FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}';", "medium"))
    T.append((f"{fm(m)}銷售幾筆", f"SELECT COUNT(DISTINCT OutStkId) AS cnt FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}';", "medium"))

# medium - 商品
for p in VAL_PRODS_OS[:12]:
    T.append((f"「{p}」賣了幾個", f"SELECT SUM(qty) AS total_qty FROM {V} WHERE {F} AND pName=N'{p}';", "medium"))
    T.append((f"「{p}」的銷售額", f"SELECT SUM(amtTotal) AS total FROM {V} WHERE {F} AND pName=N'{p}';", "medium"))

# medium - 會員
for mem in VAL_MEMBERS[:8]:
    T.append((f"「{mem}」消費了多少", f"SELECT SUM(amount) AS total FROM {V} WHERE {F} AND memName=N'{mem}';", "medium"))

# medium - 條碼
for bc in VAL_BARCODES[:5]:
    T.append((f"條碼{bc}賣了幾個", f"SELECT SUM(qty) AS total_qty FROM {V} WHERE {F} AND pBarcode='{bc}';", "medium"))

# hard
T.append(("哪個商品銷售金額最高", f"SELECT TOP 1 pName, SUM(amtTotal) AS total FROM {V} WHERE {F} GROUP BY pName ORDER BY total DESC;", "hard"))
T.append(("哪個會員消費最多", f"SELECT TOP 1 memName, SUM(amount) AS total FROM {V} WHERE {F} AND memName IS NOT NULL AND memName<>'' GROUP BY memName ORDER BY total DESC;", "hard"))
T.append(("每月銷售了幾種不同商品", f"SELECT LEFT(OutStkId,6) AS ym, COUNT(DISTINCT pName) AS prod_cnt FROM {V} WHERE {F} GROUP BY LEFT(OutStkId,6) ORDER BY ym;", "hard"))
T.append(("各倉庫銷售金額排名", f"SELECT whSn, SUM(amount) AS total FROM {V} WHERE {F} GROUP BY whSn ORDER BY total DESC;", "hard"))

# hard - 日期+商品
for m in OS_MONTHS[:3]:
    for p in VAL_PRODS_OS[:2]:
        T.append((f"{fm(m)}「{p}」賣了多少", f"SELECT SUM(qty) AS total_qty FROM {V} WHERE {F} AND LEFT(OutStkId,6)='{m}' AND pName=N'{p}';", "hard"))


# ═══════════════════════════════════════════
# WP_vTransfer - 驗證題
# ═══════════════════════════════════════════
V = "WP_M09.dbo.WP_vTransfer"; F = "isDel='N' AND dtlIsDel='N'"

# easy
T += [
    ("調撥單總共幾筆", f"SELECT COUNT(DISTINCT TransferId) AS cnt FROM {V} WHERE {F};", "easy"),
    ("列出全部調撥單", f"SELECT DISTINCT TransferId, TransferDate FROM {V} WHERE {F} ORDER BY TransferId;", "easy"),
    ("How many transfer records?", f"SELECT COUNT(DISTINCT TransferId) AS cnt FROM {V} WHERE {F};", "easy"),
]

# medium - 日期
for d in TF_DAYS[:8]:
    T.append((f"{fd(d)}的調撥紀錄", f"SELECT DISTINCT TransferId, TransferDate FROM {V} WHERE {F} AND LEFT(TransferId,8)='{d}' ORDER BY TransferId;", "medium"))

for m in TF_MONTHS:
    T.append((f"{fm(m)}調撥了幾筆", f"SELECT COUNT(DISTINCT TransferId) AS cnt FROM {V} WHERE {F} AND LEFT(TransferId,6)='{m}';", "medium"))

# medium - 倉庫
for wh in WAREHOUSES:
    T.append((f"從「{wh}」調出了多少數量", f"SELECT SUM(qty) AS total FROM {V} WHERE {F} AND fWhName=N'{wh}';", "medium"))

# medium - 商品
for p in VAL_PRODS_TF[:10]:
    T.append((f"「{p}」調撥了多少", f"SELECT SUM(qty) AS total_qty FROM {V} WHERE {F} AND pName=N'{p}';", "medium"))

# hard
T.append(("哪個倉庫調出最多", f"SELECT TOP 1 fWhName, SUM(qty) AS total FROM {V} WHERE {F} GROUP BY fWhName ORDER BY total DESC;", "hard"))
T.append(("哪個商品調撥量最大", f"SELECT TOP 1 pName, SUM(qty) AS total_qty FROM {V} WHERE {F} GROUP BY pName ORDER BY total_qty DESC;", "hard"))
T.append(("調撥總成本多少", f"SELECT SUM(qty * costAvg) AS total_cost FROM {V} WHERE {F};", "hard"))


# ═══════════════════════════════════════════
# WP_vInventory - 驗證題
# ═══════════════════════════════════════════
V = "WP_M09.dbo.WP_vInventory"

# easy
T += [
    ("庫存商品共幾種", f"SELECT COUNT(DISTINCT pNo) AS cnt FROM {V};", "easy"),
    ("全部庫存總量", f"SELECT SUM(qty) AS total_qty FROM {V};", "easy"),
    ("Total inventory quantity?", f"SELECT SUM(qty) AS total_qty FROM {V};", "easy"),
]

# medium - 倉庫
for wh in WAREHOUSES:
    T.append((f"「{wh}」庫存幾種商品", f"SELECT COUNT(DISTINCT pNo) AS cnt FROM {V} WHERE WarehouseName=N'{wh}';", "medium"))
    T.append((f"「{wh}」庫存總量是多少", f"SELECT SUM(qty) AS total FROM {V} WHERE WarehouseName=N'{wh}';", "medium"))

# medium - 商品
for p in VAL_PRODS_INV[:12]:
    T.append((f"「{p}」還有多少庫存", f"SELECT SUM(qty) AS total_qty FROM {V} WHERE pName=N'{p}';", "medium"))

# medium - 供應商
for pv in VAL_INV_PVS[:8]:
    T.append((f"供應商「{pv}」的商品庫存", f"SELECT pName, WarehouseName, qty FROM {V} WHERE pvName=N'{pv}';", "medium"))

# medium - 條碼
for bc in VAL_BARCODES[:5]:
    T.append((f"條碼{bc}的庫存量", f"SELECT WarehouseName, pName, qty FROM {V} WHERE pBarcode='{bc}';", "medium"))

# hard
T.append(("哪個倉庫庫存金額最高", f"SELECT TOP 1 WarehouseName, SUM(qty * costAvg) AS total_cost FROM {V} WHERE qty>0 GROUP BY WarehouseName ORDER BY total_cost DESC;", "hard"))
T.append(("哪個供應商的庫存商品種類最多", f"SELECT TOP 1 pvName, COUNT(DISTINCT pNo) AS cnt FROM {V} WHERE pvName IS NOT NULL GROUP BY pvName ORDER BY cnt DESC;", "hard"))
T.append(("庫存低於安全量的商品有幾種", f"SELECT COUNT(*) AS cnt FROM {V} WHERE qty < qtySafe AND qtySafe > 0;", "hard"))

# hard - 倉庫+商品
for wh in WAREHOUSES[:3]:
    for p in VAL_PRODS_INV[:2]:
        T.append((f"「{wh}」的「{p}」有多少庫存", f"SELECT qty FROM {V} WHERE WarehouseName=N'{wh}' AND pName=N'{p}';", "hard"))


# ═══════════════════════════════════════════
# WP_vProduct - 驗證題
# ═══════════════════════════════════════════
V = "WP_M09.dbo.WP_vProduct"

# easy
T += [
    ("商品總共幾種", f"SELECT COUNT(*) AS cnt FROM {V};", "easy"),
    ("列出全部商品和售價", f"SELECT pNo, pName, priceStd FROM {V} ORDER BY pNo;", "easy"),
    ("How many product types?", f"SELECT COUNT(*) AS cnt FROM {V};", "easy"),
]

# medium - 商品
for p in VAL_PRODS_PRODUCT:
    T.append((f"「{p}」售價多少", f"SELECT pName, priceStd FROM {V} WHERE pName=N'{p}';", "medium"))
    T.append((f"「{p}」的成本", f"SELECT pName, costStd, costAvg FROM {V} WHERE pName=N'{p}';", "medium"))

# medium - 條碼
for bc in VAL_BARCODES[:10]:
    T.append((f"條碼{bc}的商品名稱和價格", f"SELECT pName, pBarcode, priceStd FROM {V} WHERE pBarcode='{bc}';", "medium"))

# medium - 供應商
for pv in VAL_PVS[:10]:
    T.append((f"「{pv}」提供哪些商品", f"SELECT pNo, pName, priceStd FROM {V} WHERE pvName=N'{pv}' ORDER BY pNo;", "medium"))

# medium - 價格篩選
for price in [30, 150, 300, 800]:
    T.append((f"售價超過{price}的商品有幾種", f"SELECT COUNT(*) AS cnt FROM {V} WHERE priceStd>{price};", "medium"))

# hard
T.append(("哪個供應商提供最多種商品", f"SELECT TOP 1 pvName, COUNT(*) AS cnt FROM {V} GROUP BY pvName ORDER BY cnt DESC;", "hard"))
T.append(("售價最高和最低的商品", f"SELECT TOP 1 pName, priceStd FROM {V} ORDER BY priceStd DESC;", "hard"))
T.append(("平均售價和平均成本的差異", f"SELECT AVG(priceStd) AS avg_price, AVG(costStd) AS avg_cost FROM {V};", "hard"))
T.append(("每個單位類別有幾種商品", f"SELECT pUName, COUNT(*) AS cnt FROM {V} GROUP BY pUName ORDER BY cnt DESC;", "hard"))


# ═══════════════════════════════════════════
# WP_vProvider - 驗證題 (用 sn 不是 pvSn)
# ═══════════════════════════════════════════
V = "WP_M09.dbo.WP_vProvider"

# easy
T += [
    ("供應商總共幾家", f"SELECT COUNT(*) AS cnt FROM {V} WHERE sn > 0;", "easy"),
    ("列出全部供應商名稱", f"SELECT sn, pvId, pvName FROM {V} WHERE sn > 0 ORDER BY sn;", "easy"),
    ("目前啟用的供應商幾家", f"SELECT COUNT(*) AS cnt FROM {V} WHERE isStop='N' AND sn > 0;", "easy"),
    ("How many active suppliers?", f"SELECT COUNT(*) AS cnt FROM {V} WHERE isStop='N' AND sn > 0;", "easy"),
]

# medium - 個別供應商
for pv in VAL_PVS:
    T.append((f"「{pv}」的電話和地址", f"SELECT pvName, pvTel, pvCity, pvZone, pvAddr FROM {V} WHERE pvName=N'{pv}';", "medium"))

for pv in VAL_PVS[:15]:
    T.append((f"「{pv}」的負責人是誰", f"SELECT pvName, pvBoss FROM {V} WHERE pvName=N'{pv}';", "medium"))
    T.append((f"「{pv}」還在營業嗎", f"SELECT pvName, isStop FROM {V} WHERE pvName=N'{pv}';", "medium"))

# medium - 編號
for sn in range(21, 31):
    T.append((f"編號{sn}的供應商", f"SELECT sn, pvId, pvName, pvTel FROM {V} WHERE sn={sn};", "medium"))

# hard
T.append(("哪個縣市供應商最多", f"SELECT TOP 1 pvCity, COUNT(*) AS cnt FROM {V} WHERE sn > 0 GROUP BY pvCity ORDER BY cnt DESC;", "hard"))
T.append(("有統編的供應商幾家", f"SELECT COUNT(*) AS cnt FROM {V} WHERE taxId IS NOT NULL AND taxId<>'' AND sn > 0;", "hard"))
T.append(("各供應商類別的數量分布", f"SELECT pvKName, COUNT(*) AS cnt FROM {V} WHERE sn > 0 GROUP BY pvKName ORDER BY cnt DESC;", "hard"))


# ═══════════════════════════════════════════
# Dedup + Validate + Output
# ═══════════════════════════════════════════
print(f"\nTotal templates: {len(T)}")

seen = set()
unique = []
for item in T:
    q = item[0]
    if q not in seen:
        seen.add(q)
        unique.append(item)
print(f"After dedup: {len(unique)}")

# Also load training set questions to avoid overlap
with open("data/wp_m09/train_claude_en_2000_v2.json", "r", encoding="utf-8") as f:
    train_data = json.load(f)
train_questions = {item["question"] for item in train_data}

no_overlap = [item for item in unique if item[0] not in train_questions]
print(f"After removing overlap with training set: {len(no_overlap)}")

# Validate on DB
print("Validating on database...")
valid = []
empty = []
errors = []

for i, (q, sql, diff) in enumerate(no_overlap):
    try:
        c = conn.cursor()
        c.execute(sql)
        rows = c.fetchall()
        c.close()
        if len(rows) == 0:
            empty.append((q, sql, diff))
        elif len(rows) == 1 and all(v is None for v in rows[0]):
            empty.append((q, sql, diff))
        else:
            valid.append((q, sql, diff))
    except Exception as e:
        errors.append((q, sql, diff, str(e)))

print(f"\nValid={len(valid)}, Empty={len(empty)}, Errors={len(errors)}")

if errors:
    print("\n=== ERRORS ===")
    for q, sql, diff, err in errors[:10]:
        print(f"  [{diff}] Q: {q}\n  SQL: {sql}\n  ERR: {err}\n")

# Build output
training = []
for q, sql, diff in valid:
    toks = sql.replace("(", " ( ").replace(")", " ) ").replace(",", " , ").replace(";", " ;").split()
    training.append({
        "db_id": "WP_M09",
        "query": sql,
        "query_toks": toks,
        "query_toks_no_value": toks,
        "question": q,
        "question_toks": list(q),
        "difficulty": diff,
        "sql": {"from": {"table_units": [], "conds": []}, "select": [False, []], "where": [], "groupBy": [], "having": [], "orderBy": [], "limit": None, "intersect": None, "union": None, "except": None},
    })

out = "data/wp_m09/val_claude_en_v3.json"
with open(out, "w", encoding="utf-8") as f:
    json.dump(training, f, ensure_ascii=False, indent=2)

print(f"\nSaved {len(training)} items to {out}")

# Stats
vc = Counter()
dc = Counter()
for q, sql, diff in valid:
    dc[diff] += 1
    for vn in ["WP_vAcctIn", "WP_vAcctOut", "WP_vOutStock", "WP_vTransfer", "WP_vInventory", "WP_vProduct", "WP_vProvider"]:
        if vn in sql:
            vc[vn] += 1
            break

print("\nBy view:")
for vn, cnt in sorted(vc.items()):
    print(f"  {vn}: {cnt}")

print("\nBy difficulty:")
for d, cnt in sorted(dc.items()):
    print(f"  {d}: {cnt}")

conn.close()
print("\nDone!")
