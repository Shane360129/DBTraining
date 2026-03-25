import json
import re

def make_toks(query):
    return re.findall(r"[A-Za-z_][A-Za-z0-9_.]*|'[^']*'|[0-9]+|[().,;*<>=!%]+", query)

def make_entry(question, gold_sql):
    toks = make_toks(gold_sql)
    toks_no_val = []
    for t in toks:
        if t.startswith("'") or (t.isdigit() and len(t) > 2):
            toks_no_val.append("'value'")
        else:
            toks_no_val.append(t)
    return {
        "db_id": "WP_M09",
        "query": gold_sql,
        "query_toks": toks,
        "query_toks_no_value": toks_no_val,
        "question": question,
        "question_toks": question.split(),
        "sql": {"select": [False, []], "from": {"table_units": [], "conds": []},
                "where": [], "groupBy": [], "having": [], "orderBy": [],
                "limit": None, "intersect": None, "union": None, "except": None}
    }

fixes = [
    # === WP_vAcctIn ===
    # isDel confusion: dtlIsDel only for detail-level aggregates
    ("What is the minimum sales quantity (oStkDtlQty) in active receivable details?",
     "SELECT MIN(oStkDtlQty) FROM WP_M09.dbo.WP_vAcctIn WHERE dtlIsDel = 'N';"),
    ("Find the smallest oStkDtlQty among non-deleted receivable detail lines.",
     "SELECT MIN(oStkDtlQty) FROM WP_M09.dbo.WP_vAcctIn WHERE dtlIsDel = 'N';"),
    # correct column oStkDtlSn (not dtlSn)
    ("Retrieve the product short name and unit price for receivable detail serial number 14274.",
     "SELECT pNameS, oStkDtlAmt FROM WP_M09.dbo.WP_vAcctIn WHERE oStkDtlSn = '14274' AND dtlIsDel = 'N';"),
    ("Get product short name and detail amount for oStkDtlSn 14274 in active receivable details.",
     "SELECT pNameS, oStkDtlAmt FROM WP_M09.dbo.WP_vAcctIn WHERE oStkDtlSn = '14274' AND dtlIsDel = 'N';"),
    # AVG with BOTH isDel AND dtlIsDel
    ("Calculate the average quantity per line item for receivables in December 2025.",
     "SELECT AVG(oStkDtlQty) FROM WP_M09.dbo.WP_vAcctIn WHERE LEFT(acctInId, 6) = '202512' AND isDel = 'N' AND dtlIsDel = 'N';"),
    ("What is the average oStkDtlQty for acctIn records in December 2025 with both header and detail active?",
     "SELECT AVG(oStkDtlQty) FROM WP_M09.dbo.WP_vAcctIn WHERE LEFT(acctInId, 6) = '202512' AND isDel = 'N' AND dtlIsDel = 'N';"),
    # SUM(oStkDtlAmtTotal) GROUP BY memName
    ("What is the total receivable amount grouped by member name for 2025?",
     "SELECT memName, SUM(oStkDtlAmtTotal) FROM WP_M09.dbo.WP_vAcctIn WHERE LEFT(acctInId, 4) = '2025' AND isDel = 'N' AND dtlIsDel = 'N' GROUP BY memName;"),
    ("Show each member name and their total oStkDtlAmtTotal for year 2025 receivables.",
     "SELECT memName, SUM(oStkDtlAmtTotal) FROM WP_M09.dbo.WP_vAcctIn WHERE LEFT(acctInId, 4) = '2025' AND isDel = 'N' AND dtlIsDel = 'N' GROUP BY memName;"),
    # SUM(oStkDtlQty) GROUP BY pName for member
    ("Show the product names and their total receivable quantities sold to member serial number 44.",
     "SELECT pName, SUM(oStkDtlQty) FROM WP_M09.dbo.WP_vAcctIn WHERE memSn = '44' AND dtlIsDel = 'N' GROUP BY pName;"),
    ("List pName and total oStkDtlQty for memSn 44 in active receivable details.",
     "SELECT pName, SUM(oStkDtlQty) FROM WP_M09.dbo.WP_vAcctIn WHERE memSn = '44' AND dtlIsDel = 'N' GROUP BY pName;"),
    # outStkAmtTotal field
    ("List the credit sales order IDs where the total order amount exceeds 3000.",
     "SELECT DISTINCT OutStkId FROM WP_M09.dbo.WP_vAcctIn WHERE outStkAmtTotal > 3000 AND isDel = 'N';"),
    ("Find distinct OutStkId from receivables where outStkAmtTotal is greater than 3000 and not deleted.",
     "SELECT DISTINCT OutStkId FROM WP_M09.dbo.WP_vAcctIn WHERE outStkAmtTotal > 3000 AND isDel = 'N';"),
    # TOP 1 with totalAmt alias
    ("Which member has the highest total receivable amount in 2025?",
     "SELECT TOP 1 memName, SUM(oStkDtlAmtTotal) AS totalAmt FROM WP_M09.dbo.WP_vAcctIn WHERE LEFT(acctInId, 4) = '2025' AND isDel = 'N' AND dtlIsDel = 'N' GROUP BY memName ORDER BY totalAmt DESC;"),
    ("Find the member with the largest sum of oStkDtlAmtTotal in 2025.",
     "SELECT TOP 1 memName, SUM(oStkDtlAmtTotal) AS totalAmt FROM WP_M09.dbo.WP_vAcctIn WHERE LEFT(acctInId, 4) = '2025' AND isDel = 'N' AND dtlIsDel = 'N' GROUP BY memName ORDER BY totalAmt DESC;"),
    # HAVING SUM(oStkDtlQty) > 20
    ("Show the receivable IDs with total detail quantity exceeding 20.",
     "SELECT acctInId FROM WP_M09.dbo.WP_vAcctIn WHERE isDel = 'N' AND dtlIsDel = 'N' GROUP BY acctInId HAVING SUM(oStkDtlQty) > 20;"),
    ("Find acctInId where the sum of oStkDtlQty exceeds 20 among active records.",
     "SELECT acctInId FROM WP_M09.dbo.WP_vAcctIn WHERE isDel = 'N' AND dtlIsDel = 'N' GROUP BY acctInId HAVING SUM(oStkDtlQty) > 20;"),

    # === WP_vAcctOut ===
    # dtlIsDel only for MAX(qty)
    ("What is the maximum purchase quantity in active payable details?",
     "SELECT MAX(qty) FROM WP_M09.dbo.WP_vAcctOut WHERE dtlIsDel = 'N';"),
    ("Find the largest qty value among non-deleted payable detail lines.",
     "SELECT MAX(qty) FROM WP_M09.dbo.WP_vAcctOut WHERE dtlIsDel = 'N';"),
    # SUM(pvDiscount) dtlIsDel only
    ("Get the total vendor discount applied across all active payable details.",
     "SELECT SUM(pvDiscount) FROM WP_M09.dbo.WP_vAcctOut WHERE dtlIsDel = 'N';"),
    ("What is the sum of pvDiscount for non-deleted payable detail lines?",
     "SELECT SUM(pvDiscount) FROM WP_M09.dbo.WP_vAcctOut WHERE dtlIsDel = 'N';"),
    # AVG(dtlAmt) - correct column for unit price
    ("Calculate the average unit price in payable details grouped by vendor name.",
     "SELECT pvName, AVG(dtlAmt) FROM WP_M09.dbo.WP_vAcctOut WHERE dtlIsDel = 'N' GROUP BY pvName;"),
    ("Show pvName and average dtlAmt per vendor in non-deleted payable details.",
     "SELECT pvName, AVG(dtlAmt) FROM WP_M09.dbo.WP_vAcctOut WHERE dtlIsDel = 'N' GROUP BY pvName;"),
    # inStkPayLeft field
    ("Find payable IDs where the remaining payment is greater than zero.",
     "SELECT DISTINCT acctOutId FROM WP_M09.dbo.WP_vAcctOut WHERE inStkPayLeft > 0 AND isDel = 'N';"),
    ("List distinct acctOutId where inStkPayLeft exceeds 0 and is not deleted.",
     "SELECT DISTINCT acctOutId FROM WP_M09.dbo.WP_vAcctOut WHERE inStkPayLeft > 0 AND isDel = 'N';"),
    # pNameS column filter
    ("Find the total purchase quantity for a product by its short name in payable records.",
     "SELECT SUM(qty) FROM WP_M09.dbo.WP_vAcctOut WHERE pNameS = N'product_short_name' AND dtlIsDel = 'N';"),
    # Subquery for AVG with amount > 10000
    ("Find the average quantity for payables with total amount exceeding 10000.",
     "SELECT AVG(qty) FROM WP_M09.dbo.WP_vAcctOut WHERE acctOutId IN (SELECT acctOutId FROM WP_M09.dbo.WP_vAcctOut WHERE isDel = 'N' GROUP BY acctOutId HAVING MAX(amount) > 10000) AND dtlIsDel = 'N';"),
    ("What is the average purchase qty for acctOutId records whose total amount is over 10000?",
     "SELECT AVG(qty) FROM WP_M09.dbo.WP_vAcctOut WHERE acctOutId IN (SELECT acctOutId FROM WP_M09.dbo.WP_vAcctOut WHERE isDel = 'N' GROUP BY acctOutId HAVING MAX(amount) > 10000) AND dtlIsDel = 'N';"),
    # SUM(qty * dtlAmt) for highest vendor
    ("Which vendor has the highest total purchase amount in 2025?",
     "SELECT TOP 1 pvName, SUM(qty * dtlAmt) AS totalAmt FROM WP_M09.dbo.WP_vAcctOut WHERE LEFT(acctOutId, 4) = '2025' AND isDel = 'N' AND dtlIsDel = 'N' GROUP BY pvName ORDER BY totalAmt DESC;"),
    ("Find the vendor with the largest total purchase value (qty times dtlAmt) for 2025.",
     "SELECT TOP 1 pvName, SUM(qty * dtlAmt) AS totalAmt FROM WP_M09.dbo.WP_vAcctOut WHERE LEFT(acctOutId, 4) = '2025' AND isDel = 'N' AND dtlIsDel = 'N' GROUP BY pvName ORDER BY totalAmt DESC;"),

    # === WP_vInventory ===
    # COUNT(DISTINCT pNo) not pName
    ("How many distinct products are stocked across all warehouses?",
     "SELECT COUNT(DISTINCT pNo) FROM WP_M09.dbo.WP_vInventory;"),
    ("Count the number of unique product codes in the inventory.",
     "SELECT COUNT(DISTINCT pNo) FROM WP_M09.dbo.WP_vInventory;"),
    # isSale = 'Y' string not 1
    ("What is the total inventory quantity for products marked active for sale?",
     "SELECT SUM(qty) FROM WP_M09.dbo.WP_vInventory WHERE isSale = 'Y';"),
    ("Sum up qty for products where isSale is Y in inventory.",
     "SELECT SUM(qty) FROM WP_M09.dbo.WP_vInventory WHERE isSale = 'Y';"),
    # SELECT includes qtySafe
    ("Show the product number, name, and quantity where stock is below safe level in warehouse 001.",
     "SELECT pNo, pName, qty, qtySafe FROM WP_M09.dbo.WP_vInventory WHERE WarehouseId = '001' AND qty < qtySafe;"),
    ("List pNo, pName, qty, qtySafe for products in warehouse 001 where qty is less than qtySafe.",
     "SELECT pNo, pName, qty, qtySafe FROM WP_M09.dbo.WP_vInventory WHERE WarehouseId = '001' AND qty < qtySafe;"),
    # COUNT(DISTINCT WarehouseId) with qty > 0
    ("How many distinct warehouses hold a product with stock greater than zero?",
     "SELECT COUNT(DISTINCT WarehouseId) FROM WP_M09.dbo.WP_vInventory WHERE pName = N'product_name' AND qty > 0;"),
    # SELECT pNo, pName only for priceMem < priceStd
    ("Show the products where member price is lower than standard price in warehouse 001.",
     "SELECT pNo, pName FROM WP_M09.dbo.WP_vInventory WHERE WarehouseId = '001' AND priceMem < priceStd;"),
    ("Find pNo and pName where priceMem is less than priceStd in warehouse 001.",
     "SELECT pNo, pName FROM WP_M09.dbo.WP_vInventory WHERE WarehouseId = '001' AND priceMem < priceStd;"),
    # SUM(qty * (costStd - costAvg))
    ("Calculate the total valuation difference between standard cost and average cost for warehouse 001.",
     "SELECT SUM(qty * (costStd - costAvg)) FROM WP_M09.dbo.WP_vInventory WHERE WarehouseId = '001';"),
    ("What is the total qty times (costStd minus costAvg) for inventory in warehouse 001?",
     "SELECT SUM(qty * (costStd - costAvg)) FROM WP_M09.dbo.WP_vInventory WHERE WarehouseId = '001';"),
    # TOP 1 pvName COUNT(*) alias cnt - no LIMIT
    ("Which vendor supplies the most products to warehouse 001?",
     "SELECT TOP 1 pvName, COUNT(*) AS cnt FROM WP_M09.dbo.WP_vInventory WHERE WarehouseId = '001' GROUP BY pvName ORDER BY cnt DESC;"),
    # COUNT(DISTINCT WarehouseId) > 2 with qty > 0
    ("List the product names stocked in more than 2 warehouses.",
     "SELECT pName FROM WP_M09.dbo.WP_vInventory WHERE qty > 0 GROUP BY pName HAVING COUNT(DISTINCT WarehouseId) > 2;"),
    ("Find products available in over 2 distinct warehouses with positive stock.",
     "SELECT pName FROM WP_M09.dbo.WP_vInventory WHERE qty > 0 GROUP BY pName HAVING COUNT(DISTINCT WarehouseId) > 2;"),
    # alias margin
    ("Find the product with the highest price margin (priceStd minus costAvg) in inventory.",
     "SELECT TOP 1 pName, (priceStd - costAvg) AS margin FROM WP_M09.dbo.WP_vInventory ORDER BY margin DESC;"),
    # alias zeroCnt
    ("Which warehouse has the most distinct products with zero stock?",
     "SELECT TOP 1 WarehouseName, COUNT(*) AS zeroCnt FROM WP_M09.dbo.WP_vInventory WHERE qty = 0 GROUP BY WarehouseName ORDER BY zeroCnt DESC;"),
    ("Find the warehouse with the highest count of products where qty equals 0.",
     "SELECT TOP 1 WarehouseName, COUNT(*) AS zeroCnt FROM WP_M09.dbo.WP_vInventory WHERE qty = 0 GROUP BY WarehouseName ORDER BY zeroCnt DESC;"),

    # === WP_vOutStock ===
    # isDel AND dtlIsDel order
    ("How many active out-stock detail lines exist?",
     "SELECT COUNT(*) FROM WP_M09.dbo.WP_vOutStock WHERE isDel = 'N' AND dtlIsDel = 'N';"),
    # LEFT(OutStkId, 4) for year filter in city grouping
    ("Show the member city names and the count of distinct sales orders per city in 2025.",
     "SELECT memCityName, COUNT(DISTINCT OutStkId) FROM WP_M09.dbo.WP_vOutStock WHERE LEFT(OutStkId, 4) = '2025' AND isDel = 'N' GROUP BY memCityName;"),
    ("List memCityName and number of distinct OutStkId for orders from 2025.",
     "SELECT memCityName, COUNT(DISTINCT OutStkId) FROM WP_M09.dbo.WP_vOutStock WHERE LEFT(OutStkId, 4) = '2025' AND isDel = 'N' GROUP BY memCityName;"),
    # empId not empName
    ("List the employees who processed deleted detail records in October 2025.",
     "SELECT DISTINCT empId FROM WP_M09.dbo.WP_vOutStock WHERE dtlIsDel = 'Y' AND LEFT(OutStkId, 6) = '202510';"),
    ("Find distinct empId for out-stock records with deleted details in October 2025.",
     "SELECT DISTINCT empId FROM WP_M09.dbo.WP_vOutStock WHERE dtlIsDel = 'Y' AND LEFT(OutStkId, 6) = '202510';"),
    # Subquery alias Sales_Totals
    ("What is the true total sales amount for October 2025?",
     "SELECT SUM(UniqueAmount) FROM (SELECT OutStkId, MAX(amount) AS UniqueAmount FROM WP_M09.dbo.WP_vOutStock WHERE LEFT(OutStkId, 6) = '202510' AND isDel = 'N' GROUP BY OutStkId) AS Sales_Totals;"),
    ("Calculate the deduplicated total sales amount for October 2025 using subquery alias Sales_Totals.",
     "SELECT SUM(UniqueAmount) FROM (SELECT OutStkId, MAX(amount) AS UniqueAmount FROM WP_M09.dbo.WP_vOutStock WHERE LEFT(OutStkId, 6) = '202510' AND isDel = 'N' GROUP BY OutStkId) AS Sales_Totals;"),
    # SUM(qty) for top product
    ("Find the top 1 product with the highest total sales quantity.",
     "SELECT TOP 1 pName, SUM(qty) AS totalQty FROM WP_M09.dbo.WP_vOutStock WHERE dtlIsDel = 'N' AND isDel = 'N' GROUP BY pName ORDER BY totalQty DESC;"),
    # LEFT(OutStkId, 4) for year member order count
    ("Which member has the most distinct sales orders in 2025?",
     "SELECT TOP 1 memName, COUNT(DISTINCT OutStkId) AS orderCnt FROM WP_M09.dbo.WP_vOutStock WHERE LEFT(OutStkId, 4) = '2025' AND isDel = 'N' GROUP BY memName ORDER BY orderCnt DESC;"),
    ("Find the member with the most unique OutStkId in year 2025.",
     "SELECT TOP 1 memName, COUNT(DISTINCT OutStkId) AS orderCnt FROM WP_M09.dbo.WP_vOutStock WHERE LEFT(OutStkId, 4) = '2025' AND isDel = 'N' GROUP BY memName ORDER BY orderCnt DESC;"),
    # Sales_Totals alias for emp subquery
    ("Find the true total sales amount processed by a specific employee.",
     "SELECT SUM(UniqueAmount) FROM (SELECT OutStkId, MAX(amount) AS UniqueAmount FROM WP_M09.dbo.WP_vOutStock WHERE empName = N'emp_name' AND isDel = 'N' GROUP BY OutStkId) AS Sales_Totals;"),

    # === WP_vProduct ===
    # SELECT pName only for costAvg > costInitial
    ("Find products where average cost exceeds initial cost.",
     "SELECT pName FROM WP_M09.dbo.WP_vProduct WHERE costAvg > costInitial;"),
    ("Which products have a costAvg greater than costInitial?",
     "SELECT pName FROM WP_M09.dbo.WP_vProduct WHERE costAvg > costInitial;"),
    # SELECT pBarcode, pName for priceMem = priceBat
    ("Show products where member price equals batch price.",
     "SELECT pBarcode, pName FROM WP_M09.dbo.WP_vProduct WHERE priceMem = priceBat;"),
    ("Find pBarcode and pName where priceMem equals priceBat.",
     "SELECT pBarcode, pName FROM WP_M09.dbo.WP_vProduct WHERE priceMem = priceBat;"),
    # WHERE order: qtyNow < qtySafe AND isSale = Y
    ("In the product master, list the products with qtyNow and qtySafe where qtyNow is below qtySafe and isSale is Y.",
     "SELECT pNo, pName, qtyNow, qtySafe FROM WP_M09.dbo.WP_vProduct WHERE qtyNow < qtySafe AND isSale = 'Y';"),
    # MAX(priceStd - priceLow) direct
    ("In the product master, what is the maximum price difference between standard price and lowest price?",
     "SELECT MAX(priceStd - priceLow) FROM WP_M09.dbo.WP_vProduct;"),
    ("Find the maximum of (priceStd minus priceLow) across all products.",
     "SELECT MAX(priceStd - priceLow) FROM WP_M09.dbo.WP_vProduct;"),
    # COUNT(*) not COUNT(pNo)
    ("Find the product count grouped by vendor name.",
     "SELECT pvName, COUNT(*) FROM WP_M09.dbo.WP_vProduct GROUP BY pvName;"),
    # TOP 1 margin alias no LIMIT
    ("Which product has the largest price margin (priceStd minus costAvg)?",
     "SELECT TOP 1 pName, (priceStd - costAvg) AS margin FROM WP_M09.dbo.WP_vProduct ORDER BY margin DESC;"),
    ("Find the product with the highest (priceStd - costAvg) value.",
     "SELECT TOP 1 pName, (priceStd - costAvg) AS margin FROM WP_M09.dbo.WP_vProduct ORDER BY margin DESC;"),
    # totalValue alias
    ("In the product master, which vendor has the highest total stock value (qtyNow times costAvg)?",
     "SELECT TOP 1 pvName, SUM(qtyNow * costAvg) AS totalValue FROM WP_M09.dbo.WP_vProduct GROUP BY pvName ORDER BY totalValue DESC;"),

    # === WP_vProvider ===
    # ctactTel field name
    ("Find the telephone and fax for a vendor by name.",
     "SELECT ctactTel, fax FROM WP_M09.dbo.WP_vProvider WHERE pvName = N'vendor_name';"),
    ("Get ctactTel and fax for vendor named vendor_name.",
     "SELECT ctactTel, fax FROM WP_M09.dbo.WP_vProvider WHERE pvName = N'vendor_name';"),
    # bankName field
    ("List vendor names with a bank account at a bank containing a specific keyword.",
     "SELECT pvName FROM WP_M09.dbo.WP_vProvider WHERE bankName LIKE N'%keyword%';"),
    # ctactCity field
    ("Show the vendor name and telephone for vendors in a city containing a specific name.",
     "SELECT pvName, ctactTel FROM WP_M09.dbo.WP_vProvider WHERE ctactCity LIKE N'%city_name%';"),
    ("Find pvName and ctactTel where ctactCity matches a pattern.",
     "SELECT pvName, ctactTel FROM WP_M09.dbo.WP_vProvider WHERE ctactCity LIKE N'%pattern%';"),
    # pvName only, not pvId + pvName
    ("Find vendors that have both telephone and fax numbers.",
     "SELECT pvName FROM WP_M09.dbo.WP_vProvider WHERE pvTel IS NOT NULL AND pvTel <> '' AND fax IS NOT NULL AND fax <> '';"),
    ("List vendor names where pvTel and fax are both non-empty.",
     "SELECT pvName FROM WP_M09.dbo.WP_vProvider WHERE pvTel IS NOT NULL AND pvTel <> '' AND fax IS NOT NULL AND fax <> '';"),
    # pvDiscount > 0 no extra filter
    ("List the vendor names with a discount greater than zero.",
     "SELECT pvName FROM WP_M09.dbo.WP_vProvider WHERE pvDiscount > 0;"),
    # COUNT(*) not COUNT(pvId)
    ("Find the total number of vendors in each city.",
     "SELECT pvCity, COUNT(*) FROM WP_M09.dbo.WP_vProvider GROUP BY pvCity;"),
    # TOP 1 pvKName COUNT(*) alias cnt
    ("Which vendor category has the most members?",
     "SELECT TOP 1 pvKName, COUNT(*) AS cnt FROM WP_M09.dbo.WP_vProvider GROUP BY pvKName ORDER BY cnt DESC;"),
    # pvDiscount field correct name
    ("Which active vendor has the highest discount?",
     "SELECT TOP 1 pvName, pvDiscount FROM WP_M09.dbo.WP_vProvider WHERE isStop = 'N' ORDER BY pvDiscount DESC;"),
    ("Find the top vendor by pvDiscount among non-stopped vendors.",
     "SELECT TOP 1 pvName, pvDiscount FROM WP_M09.dbo.WP_vProvider WHERE isStop = 'N' ORDER BY pvDiscount DESC;"),
    # memo field with IS NOT NULL filter
    ("Which vendor has the longest memo text?",
     "SELECT TOP 1 pvName FROM WP_M09.dbo.WP_vProvider WHERE memo IS NOT NULL AND memo <> '' ORDER BY LEN(memo) DESC;"),
    ("Find the vendor with the most characters in the memo field.",
     "SELECT TOP 1 pvName FROM WP_M09.dbo.WP_vProvider WHERE memo IS NOT NULL AND memo <> '' ORDER BY LEN(memo) DESC;"),
    # pvTel + email + fax, pvName only
    ("List vendors who have all contact information (telephone, email, and fax).",
     "SELECT pvName FROM WP_M09.dbo.WP_vProvider WHERE pvTel IS NOT NULL AND pvTel <> '' AND email IS NOT NULL AND email <> '' AND fax IS NOT NULL AND fax <> '';"),
    ("Find vendors with non-empty pvTel, email, and fax.",
     "SELECT pvName FROM WP_M09.dbo.WP_vProvider WHERE pvTel IS NOT NULL AND pvTel <> '' AND email IS NOT NULL AND email <> '' AND fax IS NOT NULL AND fax <> '';"),

    # === WP_vTransfer ===
    # empId not empName for employee lookup
    ("Find the employee who processed a specific transfer.",
     "SELECT DISTINCT empId FROM WP_M09.dbo.WP_vTransfer WHERE TransferId = '202510270001' AND isDel = 'N';"),
    ("Which empId handled transfer 202510270001?",
     "SELECT DISTINCT empId FROM WP_M09.dbo.WP_vTransfer WHERE TransferId = '202510270001' AND isDel = 'N';"),
    # dtlIsDel only for product MAX qty
    ("Find the maximum transfer quantity for a product in active transfer details.",
     "SELECT MAX(qty) FROM WP_M09.dbo.WP_vTransfer WHERE pName = N'product_name' AND dtlIsDel = 'N';"),
    ("Get the largest qty for a specific product in non-deleted transfer details.",
     "SELECT MAX(qty) FROM WP_M09.dbo.WP_vTransfer WHERE pName = N'product_name' AND dtlIsDel = 'N';"),
    # tfWhName for destination warehouse name
    ("Find the total quantity transferred into a specific destination warehouse in 2025.",
     "SELECT SUM(qty) FROM WP_M09.dbo.WP_vTransfer WHERE tfWhName = N'warehouse_name' AND LEFT(TransferId, 4) = '2025' AND isDel = 'N' AND dtlIsDel = 'N';"),
    ("Sum qty transferred to tfWhName in 2025 with both header and detail active.",
     "SELECT SUM(qty) FROM WP_M09.dbo.WP_vTransfer WHERE tfWhName = N'warehouse_name' AND LEFT(TransferId, 4) = '2025' AND isDel = 'N' AND dtlIsDel = 'N';"),
    # pName filter for source/dest warehouses
    ("Show the source and destination warehouses for transfers containing a specific product.",
     "SELECT DISTINCT fWhName, tfWhName FROM WP_M09.dbo.WP_vTransfer WHERE pName = N'product_name' AND isDel = 'N' AND dtlIsDel = 'N';"),
    # fWhId correct field name
    ("Show the total transfer quantity grouped by source warehouse ID.",
     "SELECT fWhId, SUM(qty) FROM WP_M09.dbo.WP_vTransfer WHERE isDel = 'N' AND dtlIsDel = 'N' GROUP BY fWhId;"),
    ("Group non-deleted transfer records by fWhId and sum the qty.",
     "SELECT fWhId, SUM(qty) FROM WP_M09.dbo.WP_vTransfer WHERE isDel = 'N' AND dtlIsDel = 'N' GROUP BY fWhId;"),
    # empId for month filter
    ("List the distinct employees who processed transfers in October 2025.",
     "SELECT DISTINCT empId FROM WP_M09.dbo.WP_vTransfer WHERE LEFT(TransferId, 6) = '202510' AND isDel = 'N';"),
    ("Find distinct empId for transfers in October 2025.",
     "SELECT DISTINCT empId FROM WP_M09.dbo.WP_vTransfer WHERE LEFT(TransferId, 6) = '202510' AND isDel = 'N';"),
    # COUNT(DISTINCT pNo) for product count per destination
    ("Show the total product count transferred grouped by destination warehouse.",
     "SELECT tfWhName, COUNT(DISTINCT pNo) FROM WP_M09.dbo.WP_vTransfer WHERE isDel = 'N' AND dtlIsDel = 'N' GROUP BY tfWhName;"),
    ("List tfWhName and distinct pNo count for non-deleted transfer details.",
     "SELECT tfWhName, COUNT(DISTINCT pNo) FROM WP_M09.dbo.WP_vTransfer WHERE isDel = 'N' AND dtlIsDel = 'N' GROUP BY tfWhName;"),
    # empId IN subquery for both months
    ("Find employees who handled transfers in both October and November 2025.",
     "SELECT DISTINCT empId FROM WP_M09.dbo.WP_vTransfer WHERE LEFT(TransferId, 6) = '202510' AND isDel = 'N' AND empId IN (SELECT DISTINCT empId FROM WP_M09.dbo.WP_vTransfer WHERE LEFT(TransferId, 6) = '202511' AND isDel = 'N');"),
    # alias cnt for most transferred product
    ("Which product was transferred in the most distinct transfer orders?",
     "SELECT TOP 1 pName, COUNT(DISTINCT TransferId) AS cnt FROM WP_M09.dbo.WP_vTransfer WHERE isDel = 'N' AND dtlIsDel = 'N' GROUP BY pName ORDER BY cnt DESC;"),
    # SUM(qty * costAvg) for transfer cost
    ("Find the total transfer cost grouped by transfer ID for transfers from warehouse 001.",
     "SELECT TransferId, SUM(qty * costAvg) FROM WP_M09.dbo.WP_vTransfer WHERE fWhId = '001' AND isDel = 'N' AND dtlIsDel = 'N' GROUP BY TransferId;"),
    ("Calculate total qty times costAvg per TransferId for source warehouse 001.",
     "SELECT TransferId, SUM(qty * costAvg) FROM WP_M09.dbo.WP_vTransfer WHERE fWhId = '001' AND isDel = 'N' AND dtlIsDel = 'N' GROUP BY TransferId;"),
    # destCnt alias, no dtlIsDel for header query
    ("Which source warehouse has the most distinct transfer destination warehouses?",
     "SELECT TOP 1 fWhName, COUNT(DISTINCT tfWhName) AS destCnt FROM WP_M09.dbo.WP_vTransfer WHERE isDel = 'N' GROUP BY fWhName ORDER BY destCnt DESC;"),
    ("Find fWhName with the highest count of distinct tfWhName values.",
     "SELECT TOP 1 fWhName, COUNT(DISTINCT tfWhName) AS destCnt FROM WP_M09.dbo.WP_vTransfer WHERE isDel = 'N' GROUP BY fWhName ORDER BY destCnt DESC;"),
]

entries = [make_entry(q, sql) for q, sql in fixes]
print(f"Generated {len(entries)} corrective training samples")

# Save to file
with open("data/wp_m09/corrective_fixes_0315.json", "w", encoding="utf-8") as f:
    json.dump(entries, f, ensure_ascii=False, indent=2)
print("Saved to data/wp_m09/corrective_fixes_0315.json")

# Show breakdown by table
from collections import Counter
table_counts = Counter()
for q, sql in fixes:
    for t in ["WP_vAcctIn", "WP_vAcctOut", "WP_vInventory", "WP_vOutStock", "WP_vProduct", "WP_vProvider", "WP_vTransfer"]:
        if t in sql:
            table_counts[t] += 1
            break
print("\nBreakdown by table:")
for t, c in sorted(table_counts.items()):
    print(f"  {t}: {c}")
