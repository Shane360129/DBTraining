#!/usr/bin/env python3
"""
traindata_gen__supplement2.py
Third-round supplement to push total above 2000.
Target: ~400 new samples covering cross-dimensional queries.
"""

import json, re, random
from pathlib import Path

random.seed(77)
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
ALL_MONTHS  = MONTHS_2025 + MONTHS_2024
KEYWORDS    = ["茶","米","水","冬","春","香","竹","有機","花","豆","高山","烏龍","金萱","龍井"]
AMT_THRESHOLDS = [500,1000,2000,3000,5000,8000,10000,15000,20000]
QTY_THRESHOLDS = [0,5,10,15,20,30,50,100]
PRICE_THRESHOLDS = [30,50,80,100,150,200,300,500]


def s3_acct_in():
    s = []
    # All months × all members (6 months × 3 members = 18 extra queries each with 2 phrasings)
    for m in MONTHS_2025[6:]:
        for mid in MEM_IDS[:3]:
            s.append(entry(
                f"Total accounts receivable for member {mid} in {m}",
                f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vAcctIn WHERE memId='{mid}' AND LEFT(acctInId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"
            ))
    # all members × all products
    for mn in MEM_NAMES[:4]:
        for pn in PROD_NAMES[:4]:
            s.append(entry(
                f"List accounts receivable for member {mn} that include {pn}",
                f"SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE memName=N'{mn}' AND pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"
            ))
    # Year comparison
    for yr in ["2024","2025","2023"]:
        s += multi([
            (f"Show accounts receivable from year {yr}",
             f"SELECT DISTINCT acctInId, amount, memName FROM {V}WP_vAcctIn WHERE LEFT(acctInId,4)='{yr}' AND isDel='N' AND dtlIsDel='N';"),
            (f"Count accounts receivable from {yr}",
             f"SELECT COUNT(DISTINCT acctInId) AS count FROM {V}WP_vAcctIn WHERE LEFT(acctInId,4)='{yr}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    # remaining EMP_IDS combos
    for eid in ["A01","B02"]:
        for m in ["202511","202512"]:
            s.append(entry(
                f"Accounts receivable created by {eid} in {m}",
                f"SELECT DISTINCT acctInId, amount FROM {V}WP_vAcctIn WHERE empId='{eid}' AND LEFT(acctInId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"
            ))
    return s


def s3_acct_out():
    s = []
    # months × suppliers
    for m in MONTHS_2025[6:]:
        for pvn in PV_NAMES[:3]:
            s.append(entry(
                f"Total accounts payable for {pvn} in {m}",
                f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vAcctOut WHERE pvName=N'{pvn}' AND LEFT(acctOutId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"
            ))
    # supplier × product
    for pvn in PV_NAMES[:4]:
        for pn in PROD_NAMES[:3]:
            s.append(entry(
                f"Accounts payable for {pn} from supplier {pvn}",
                f"SELECT DISTINCT acctOutId, amount FROM {V}WP_vAcctOut WHERE pvName=N'{pvn}' AND pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"
            ))
    # year comparison
    for yr in ["2024","2025"]:
        s += multi([
            (f"Total accounts payable in {yr}",
             f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vAcctOut WHERE LEFT(acctOutId,4)='{yr}' AND isDel='N' AND dtlIsDel='N';"),
            (f"Number of accounts payable records in {yr}",
             f"SELECT COUNT(DISTINCT acctOutId) AS count FROM {V}WP_vAcctOut WHERE LEFT(acctOutId,4)='{yr}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    return s


def s3_out_stock():
    s = []
    # months × members (remaining months)
    for m in MONTHS_2025[6:]:
        for mid in MEM_IDS[:3]:
            s.append(entry(
                f"Sales orders for member {mid} in {m}",
                f"SELECT DISTINCT OutStkId, amount FROM {V}WP_vOutStock WHERE memId='{mid}' AND LEFT(OutStkId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"
            ))
    # member × product
    for mn in MEM_NAMES[:4]:
        for pn in PROD_NAMES[:3]:
            s.append(entry(
                f"Sales orders from {mn} that include {pn}",
                f"SELECT DISTINCT OutStkId, qty, amtTotal FROM {V}WP_vOutStock WHERE memName=N'{mn}' AND pName=N'{pn}' AND isDel='N' AND dtlIsDel='N';"
            ))
    # year stats
    for yr in ["2024","2025"]:
        s += multi([
            (f"Total sales revenue in {yr}",
             f"SELECT SUM(DISTINCT amount) AS total FROM {V}WP_vOutStock WHERE LEFT(OutStkId,4)='{yr}' AND isDel='N' AND dtlIsDel='N';"),
            (f"How many sales orders were placed in {yr}?",
             f"SELECT COUNT(DISTINCT OutStkId) AS count FROM {V}WP_vOutStock WHERE LEFT(OutStkId,4)='{yr}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    return s


def s3_transfer():
    s = []
    # product × month
    for pn in PROD_NAMES[:6]:
        for m in ["202511","202512","202510"]:
            s.append(entry(
                f"How much {pn} was transferred in {m}?",
                f"SELECT SUM(qty) AS total_qty FROM {V}WP_vTransfer WHERE pName=N'{pn}' AND LEFT(TransferId,6)='{m}' AND isDel='N' AND dtlIsDel='N';"
            ))
    # warehouse × product
    for wh in WH_NAMES[:4]:
        for pn in PROD_NAMES[:3]:
            s.append(entry(
                f"How much {pn} was received by warehouse {wh}?",
                f"SELECT SUM(qty) AS total_qty FROM {V}WP_vTransfer WHERE pName=N'{pn}' AND tfWhName=N'{wh}' AND isDel='N' AND dtlIsDel='N';"
            ))
    # year totals
    for yr in ["2024","2025"]:
        s += multi([
            (f"Total quantity transferred in {yr}",
             f"SELECT SUM(qty) AS total_qty FROM {V}WP_vTransfer WHERE LEFT(TransferId,4)='{yr}' AND isDel='N' AND dtlIsDel='N';"),
            (f"Count of transfers in {yr}",
             f"SELECT COUNT(*) AS count FROM {V}WP_vTransfer WHERE LEFT(TransferId,4)='{yr}' AND isDel='N' AND dtlIsDel='N';"),
        ])
    return s


def s3_inventory():
    s = []
    # warehouse × supplier
    for wh in WH_NAMES[:4]:
        for pvn in PV_NAMES[:4]:
            s.append(entry(
                f"Show inventory in {wh} from supplier {pvn}",
                f"SELECT pName, qty FROM {V}WP_vInventory WHERE WarehouseName=N'{wh}' AND pvName=N'{pvn}';"
            ))
    # supplier × quantity threshold
    for pvn in PV_NAMES[:5]:
        for qt in [10, 50]:
            s.append(entry(
                f"List {pvn} products with stock above {qt}",
                f"SELECT pName, qty FROM {V}WP_vInventory WHERE pvName=N'{pvn}' AND qty > {qt};"
            ))
    # product × warehouse value
    for pn in PROD_NAMES[:6]:
        s.append(entry(
            f"What is the total inventory value for {pn}?",
            f"SELECT SUM(qty * costAvg) AS total_value FROM {V}WP_vInventory WHERE pName=N'{pn}';"
        ))
    # extra aggregations
    s += multi([
        ("Show inventory records sorted by value descending",
         f"SELECT pName, WarehouseName, qty, qty * costAvg AS value FROM {V}WP_vInventory ORDER BY value DESC;"),
        ("Which product has the highest unit cost in inventory?",
         f"SELECT TOP 1 pName, costAvg FROM {V}WP_vInventory ORDER BY costAvg DESC;"),
        ("List inventory where standard price exceeds cost by more than 50%",
         f"SELECT DISTINCT pName, priceStd, costAvg FROM {V}WP_vInventory WHERE priceStd > costAvg * 1.5;"),
        ("Show inventory grouped by category",
         f"SELECT pkName, SUM(qty) AS total_qty, COUNT(DISTINCT pNo) AS products FROM {V}WP_vInventory GROUP BY pkName ORDER BY total_qty DESC;"),
        ("Find inventory where qty equals qtySafe",
         f"SELECT pName, WarehouseName, qty FROM {V}WP_vInventory WHERE qty = qtySafe;"),
    ])
    return s


def s3_product():
    s = []
    # supplier × quantity threshold
    for pvn in PV_NAMES[:5]:
        for qt in [10, 50, 100]:
            s.append(entry(
                f"Products from {pvn} with current stock above {qt}",
                f"SELECT pNo, pName, qtyNow FROM {V}WP_vProduct WHERE pvName=N'{pvn}' AND qtyNow > {qt};"
            ))
    # keyword × price
    for kw in KEYWORDS[:6]:
        for pt in [50, 100, 200]:
            s.append(entry(
                f"Products containing {kw} with price above {pt}",
                f"SELECT pNo, pName, priceStd FROM {V}WP_vProduct WHERE pName LIKE N'%{kw}%' AND priceStd > {pt};"
            ))
    # months
    for m in MONTHS_2025[1:6]:
        s += multi([
            (f"Products added in {m}",
             f"SELECT pNo, pName FROM {V}WP_vProduct WHERE pNo LIKE '{m}%';"),
            (f"How many products were added in {m}?",
             f"SELECT COUNT(*) AS count FROM {V}WP_vProduct WHERE pNo LIKE '{m}%';"),
        ])
    # extra queries
    s += multi([
        ("Show product names and barcodes sorted by name",
         f"SELECT pName, pBarcode FROM {V}WP_vProduct WHERE pBarcode IS NOT NULL AND pBarcode <> '' ORDER BY pName;"),
        ("List the top 3 categories by product count",
         f"SELECT TOP 3 pkName, COUNT(*) AS count FROM {V}WP_vProduct GROUP BY pkName ORDER BY count DESC;"),
        ("Show products with price equal to 100",
         f"SELECT pNo, pName FROM {V}WP_vProduct WHERE priceStd = 100;"),
        ("Find products where current stock is more than double safe stock",
         f"SELECT pNo, pName, qtyNow, qtySafe FROM {V}WP_vProduct WHERE qtyNow > qtySafe * 2;"),
        ("List products with no supplier information",
         f"SELECT pNo, pName FROM {V}WP_vProduct WHERE pvName IS NULL OR pvName = '';"),
    ])
    return s


def s3_provider():
    s = []
    # more discount threshold combos
    for dt in [8, 12, 18, 22]:
        s += multi([
            (f"Suppliers with exactly {dt}% discount",
             f"SELECT pvSn, pvName FROM {V}WP_vProvider WHERE discount = {dt};"),
            (f"Show suppliers with discount at least {dt}%",
             f"SELECT pvSn, pvName, discount FROM {V}WP_vProvider WHERE discount >= {dt} ORDER BY discount DESC;"),
        ])
    # pvSn ranges
    for pvs in PV_SNS:
        s.append(entry(
            f"Is supplier {pvs} currently active?",
            f"SELECT pvSn, pvName, isSale FROM {V}WP_vProvider WHERE pvSn='{pvs}';"
        ))
    # extra analytics
    s += multi([
        ("Show all supplier contact details",
         f"SELECT pvSn, pvName, pvTel, pvAddr FROM {V}WP_vProvider ORDER BY pvName;"),
        ("Count how many suppliers have phone numbers on file",
         f"SELECT COUNT(*) AS count FROM {V}WP_vProvider WHERE pvTel IS NOT NULL AND pvTel <> '';"),
        ("Count suppliers with addresses on file",
         f"SELECT COUNT(*) AS count FROM {V}WP_vProvider WHERE pvAddr IS NOT NULL AND pvAddr <> '';"),
        ("Show supplier discount statistics",
         f"SELECT MIN(discount) AS min, MAX(discount) AS max, AVG(discount) AS avg FROM {V}WP_vProvider WHERE discount > 0;"),
        ("Find suppliers without a discount",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider WHERE discount = 0 OR discount IS NULL;"),
        ("List suppliers with discount in descending order",
         f"SELECT pvSn, pvName, discount FROM {V}WP_vProvider ORDER BY discount DESC;"),
        ("Show active supplier count vs inactive count",
         f"SELECT isSale, COUNT(*) AS count FROM {V}WP_vProvider GROUP BY isSale;"),
        ("Which suppliers have both a phone number and address?",
         f"SELECT pvSn, pvName FROM {V}WP_vProvider WHERE (pvTel IS NOT NULL AND pvTel <> '') AND (pvAddr IS NOT NULL AND pvAddr <> '');"),
    ])
    return s


def main():
    print("=" * 60)
    print("Generating supplement 2 samples...")
    print("=" * 60)

    new_samples = (
        s3_acct_in() +
        s3_acct_out() +
        s3_out_stock() +
        s3_transfer() +
        s3_inventory() +
        s3_product() +
        s3_provider()
    )

    from collections import Counter
    def view_of(s):
        m = re.search(r'WP_v\w+', s.get("query",""))
        return m.group(0) if m else "?"
    cnt = Counter(view_of(s) for s in new_samples)
    print(f"\nNew supplement2 samples: {len(new_samples)}")
    for k, v in sorted(cnt.items()):
        print(f"  {k}: {v}")

    # Verify no isDel in no-isDel views
    bad = [s for s in new_samples
           if re.search(r'\bisdel\b|\bdtlisdel\b', s["query"], re.IGNORECASE)
           and any(v in s["query"] for v in ["WP_vInventory","WP_vProduct","WP_vProvider"])]
    if bad:
        print(f"\nWARNING: {len(bad)} bad samples!")
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
    print(f"\nExisting samples: {len(combined_old)}")

    all_samples = combined_old + new_samples
    random.shuffle(all_samples)
    print(f"Total after merge: {len(all_samples)}")

    n_val   = max(200, int(len(all_samples) * 0.12))
    n_train = len(all_samples) - n_val
    train_data = all_samples[:n_train]
    val_data   = all_samples[n_train:]

    with open(train_file, "w", encoding="utf-8") as f:
        json.dump(train_data, f, ensure_ascii=False, indent=2)
    with open(val_file, "w", encoding="utf-8") as f:
        json.dump(val_data, f, ensure_ascii=False, indent=2)

    print(f"\nFinal split: {n_train} train  /  {n_val} validation")
    print(f"Files written:")
    print(f"  {train_file}  ({n_train} samples)")
    print(f"  {val_file}  ({n_val} samples)")
    print("\nDone!")


if __name__ == "__main__":
    import os
    os.chdir(Path(__file__).parent)
    main()
