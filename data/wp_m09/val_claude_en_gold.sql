SELECT * FROM WP_M09.dbo.WP_vAcctOut WHERE acctOutId='202508200001' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vOutStock WHERE pName=N'竹炭冬筍餅' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pName, qty FROM WP_M09.dbo.WP_vInventory WHERE pvName=N'永豐農產' AND qty > 50;	WP_M09
SELECT DISTINCT acctOutId, amount FROM WP_M09.dbo.WP_vAcctOut WHERE LEFT(acctOutId,8)='20251225' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT COUNT(DISTINCT pNo) AS count FROM WP_M09.dbo.WP_vInventory WHERE WarehouseName=N'南區倉庫';	WP_M09
SELECT pName, qty FROM WP_M09.dbo.WP_vInventory WHERE WarehouseName=N'北區倉庫' AND pvName=N'實垣有限公司';	WP_M09
SELECT pNo, pName, qtyNow FROM WP_M09.dbo.WP_vProduct WHERE qtyNow > 15 ORDER BY qtyNow DESC;	WP_M09
SELECT pName, qty, priceStd FROM WP_M09.dbo.WP_vInventory WHERE WarehouseName=N'東區倉庫' AND priceStd > 30;	WP_M09
SELECT pNo, pName, qtyNow FROM WP_M09.dbo.WP_vProduct WHERE qtyNow > 10 ORDER BY qtyNow DESC;	WP_M09
SELECT pNo, pName, qtyNow, qtySafe FROM WP_M09.dbo.WP_vProduct WHERE qtyNow = 0 AND qtySafe > 0;	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vOutStock WHERE LEFT(OutStkId,6)='202511' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT acctInId FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pName, qty FROM WP_M09.dbo.WP_vInventory WHERE WarehouseName=N'特產中心' AND pvName=N'台灣好茶';	WP_M09
SELECT SUM(amtTotal) AS total_revenue FROM WP_M09.dbo.WP_vOutStock WHERE pName=N'東方美人茶' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pName, AVG(qty) AS avg_qty FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY pName ORDER BY avg_qty DESC;	WP_M09
SELECT DISTINCT acctOutId, pName FROM WP_M09.dbo.WP_vAcctOut WHERE pName LIKE N'%高山%' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctIn WHERE memName=N'觀光農場' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT * FROM WP_M09.dbo.WP_vProvider ORDER BY pvSn;	WP_M09
SELECT pNo, pName, qtyNow FROM WP_M09.dbo.WP_vProduct WHERE pName=N'台灣高山茶';	WP_M09
SELECT memId, memName, SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY memId, memName ORDER BY total DESC;	WP_M09
SELECT pvSn, pvName, isStop FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'健康食品';	WP_M09
SELECT COUNT(*) AS count FROM WP_M09.dbo.WP_vTransfer WHERE pName=N'普洱茶' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT COUNT(DISTINCT acctOutId) AS count FROM WP_M09.dbo.WP_vAcctOut WHERE LEFT(acctOutId,6)='202511' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pNo, pName FROM WP_M09.dbo.WP_vProduct WHERE pvName=N'健康食品';	WP_M09
SELECT DISTINCT pName, SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory WHERE pvName=N'永豐農產' GROUP BY pName;	WP_M09
SELECT COUNT(*) AS count FROM WP_M09.dbo.WP_vProduct WHERE pName LIKE N'%竹%';	WP_M09
SELECT DISTINCT pName, SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory WHERE pvName=N'山林食品' GROUP BY pName;	WP_M09
SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE amount > 20000 AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT pName, priceStd FROM WP_M09.dbo.WP_vInventory WHERE priceStd > 30 ORDER BY priceStd DESC;	WP_M09
SELECT WarehouseName, qty FROM WP_M09.dbo.WP_vInventory WHERE pName=N'有機烏龍茶';	WP_M09
SELECT pNo, pName, priceStd FROM WP_M09.dbo.WP_vProduct WHERE priceStd < 30 ORDER BY priceStd;	WP_M09
SELECT DISTINCT pName, qty FROM WP_M09.dbo.WP_vInventory WHERE qty < 5;	WP_M09
SELECT * FROM WP_M09.dbo.WP_vAcctOut WHERE acctOutId='202512100002' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE memName=N'觀光農場' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT COUNT(*) AS count FROM WP_M09.dbo.WP_vProduct WHERE pvName=N'品茶苑';	WP_M09
SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE memName=N'食品公司' AND pName=N'烏龍茶葉' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT MAX(DISTINCT amount) AS max_amount FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pvSn, pvName, pvTel, pvAddr, email FROM WP_M09.dbo.WP_vProvider WHERE pvSn='21';	WP_M09
SELECT WarehouseName, qty FROM WP_M09.dbo.WP_vInventory WHERE pName=N'阿里山茶';	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctOut WHERE pvName=N'綠野農業' AND LEFT(acctOutId,6)='202504' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pvSn, pvName FROM WP_M09.dbo.WP_vProvider WHERE pvName LIKE N'%茶%';	WP_M09
SELECT COUNT(DISTINCT acctInId) AS count FROM WP_M09.dbo.WP_vAcctIn WHERE memId='A008' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT acctOutId, amount FROM WP_M09.dbo.WP_vAcctOut WHERE LEFT(acctOutId,6)='202403' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vTransfer WHERE pName=N'有機烏龍茶' AND LEFT(TransferId,6)='202512' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE memId='A006' AND LEFT(OutStkId,6)='202511' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctOut WHERE pvName=N'台灣好茶' AND LEFT(acctOutId,6)='202509' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT OutStkId, pName FROM WP_M09.dbo.WP_vOutStock WHERE pName LIKE N'%花%' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT pName, priceStd, SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory WHERE priceStd > 500 GROUP BY pName, priceStd;	WP_M09
SELECT pName, qty, WarehouseName FROM WP_M09.dbo.WP_vInventory WHERE pName LIKE N'%茶%';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory WHERE pName LIKE N'%龍井%';	WP_M09
SELECT pNo, pName, costAvg, priceStd FROM WP_M09.dbo.WP_vProduct WHERE costAvg > priceStd * 0.8 ORDER BY pName;	WP_M09
SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE LEFT(acctInId,6)='202501' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE LEFT(acctInId,8)='20251225' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(qtyNow * costAvg) AS total_value FROM WP_M09.dbo.WP_vProduct WHERE pvName=N'永豐農產';	WP_M09
SELECT pvSn, pvName, pvBoss FROM WP_M09.dbo.WP_vProvider WHERE pvBoss IS NOT NULL AND pvBoss<>'';	WP_M09
SELECT pvSn, pvName, isSale FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'天然農場';	WP_M09
SELECT pNo, pName, qtyNow FROM WP_M09.dbo.WP_vProduct WHERE pName=N'龍井茶';	WP_M09
SELECT pNo, pName FROM WP_M09.dbo.WP_vProduct WHERE pNo LIKE '202504%';	WP_M09
SELECT SUM(amtTotal) AS total FROM WP_M09.dbo.WP_vAcctOut WHERE pName=N'阿里山茶' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT TransferId, pName, qty FROM WP_M09.dbo.WP_vTransfer WHERE pBarcode='4712070722015' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pNo, pName, priceStd, costAvg, qtyNow FROM WP_M09.dbo.WP_vProduct WHERE pBarcode='4719865002441';	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctIn WHERE memId='A006' AND LEFT(acctInId,6)='202512' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pvSn, pvName, pvTel FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'有機世界';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory WHERE pName LIKE N'%高山%';	WP_M09
SELECT DISTINCT pName, SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory WHERE pName LIKE N'%冬%' GROUP BY pName;	WP_M09
SELECT DISTINCT acctInId, oStkDtlQty, oStkDtlAmt FROM WP_M09.dbo.WP_vAcctIn WHERE pName=N'有機烏龍茶' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT COUNT(DISTINCT pNo) AS count FROM WP_M09.dbo.WP_vInventory WHERE WarehouseName=N'冷藏倉';	WP_M09
SELECT DISTINCT acctOutId, amount FROM WP_M09.dbo.WP_vAcctOut WHERE pName=N'東方美人茶' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pvSn, pvName, pvDiscount FROM WP_M09.dbo.WP_vProvider WHERE pvDiscount > 8 ORDER BY pvDiscount DESC;	WP_M09
SELECT pvSn, pvName, pvTel, pvAddr FROM WP_M09.dbo.WP_vProvider ORDER BY pvName;	WP_M09
SELECT pNo, pName FROM WP_M09.dbo.WP_vProduct WHERE pNo LIKE '202412%';	WP_M09
SELECT DISTINCT pName, qty FROM WP_M09.dbo.WP_vInventory WHERE pvSn='3';	WP_M09
SELECT DISTINCT acctOutId, pName FROM WP_M09.dbo.WP_vAcctOut WHERE pName LIKE N'%香%' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pName, qty, WarehouseName FROM WP_M09.dbo.WP_vInventory WHERE pName LIKE N'%米%';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory WHERE pvName=N'山林食品';	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctOut WHERE pvName=N'永豐農產' AND LEFT(acctOutId,6)='202508' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pvSn, pvName FROM WP_M09.dbo.WP_vProvider ORDER BY pvName;	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vOutStock WHERE LEFT(OutStkId,6)='202502' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pName, qty, amtTotal FROM WP_M09.dbo.WP_vAcctOut WHERE acctOutId='202512100002' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctIn WHERE memId='A008' AND LEFT(acctInId,6)='202509' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vTransfer WHERE pName=N'阿里山茶' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT TransferId, qty, fWhName, tfWhName FROM WP_M09.dbo.WP_vTransfer WHERE pName=N'花蓮米' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT acctInId, amount, memName FROM WP_M09.dbo.WP_vAcctIn WHERE pName=N'烏龍茶葉' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT amount FROM WP_M09.dbo.WP_vAcctIn WHERE acctInId='202511200001' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pNo, pName FROM WP_M09.dbo.WP_vProduct WHERE pvName=N'健康食品';	WP_M09
SELECT DISTINCT acctInId, amount, memName FROM WP_M09.dbo.WP_vAcctIn WHERE amount > 10000 AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vAcctOut WHERE pName=N'龍井茶' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vTransfer WHERE fWhName=N'北區倉庫' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vTransfer WHERE pName=N'竹炭冬筍餅' AND tfWhName=N'北區倉庫' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT fWhName, tfWhName, AVG(qty) AS avg_qty FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY fWhName, tfWhName ORDER BY avg_qty DESC;	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctIn WHERE memId='A008' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(oStkDtlAmt) AS total FROM WP_M09.dbo.WP_vAcctIn WHERE pName=N'玫瑰花茶' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctOut WHERE pvName=N'綠野農業' AND LEFT(acctOutId,6)='202501' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT OutStkId, pName, qty FROM WP_M09.dbo.WP_vOutStock WHERE qty > 15 AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT LEFT(TransferId,6) AS month, COUNT(DISTINCT pNo) AS products FROM WP_M09.dbo.WP_vTransfer WHERE LEFT(TransferId,4)='2025' AND isDel='N' AND dtlIsDel='N' GROUP BY LEFT(TransferId,6) ORDER BY month;	WP_M09
SELECT pvSn, pvName FROM WP_M09.dbo.WP_vProvider WHERE discount = 0 OR discount IS NULL;	WP_M09
SELECT TransferId, pName, qty FROM WP_M09.dbo.WP_vTransfer WHERE fWhName=N'中央倉庫' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT COUNT(*) AS count FROM WP_M09.dbo.WP_vTransfer WHERE LEFT(TransferId,6)='202512' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pNo, pName FROM WP_M09.dbo.WP_vProduct WHERE pName LIKE N'%春%';	WP_M09
SELECT SUM(amtTotal) AS total_revenue FROM WP_M09.dbo.WP_vOutStock WHERE pName=N'龍井茶' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE amount > 10000 AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT pName, priceStd, SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory WHERE priceStd > 300 GROUP BY pName, priceStd;	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vTransfer WHERE pName=N'益全香米' AND LEFT(TransferId,6)='202512' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT memId, memName, SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N' GROUP BY memId, memName ORDER BY total DESC;	WP_M09
SELECT DISTINCT OutStkId, qty, amtTotal FROM WP_M09.dbo.WP_vOutStock WHERE pName=N'東方美人茶' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT OutStkId FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctOut WHERE LEFT(acctOutId,6)='202503' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pName, qty FROM WP_M09.dbo.WP_vInventory WHERE WarehouseName=N'中央倉庫' AND pvName=N'實垣有限公司';	WP_M09
SELECT pNo, pName FROM WP_M09.dbo.WP_vProduct WHERE pName LIKE N'%米%';	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctIn WHERE memId='A009' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pNo, pName, qtyNow FROM WP_M09.dbo.WP_vProduct WHERE pvName=N'永豐農產' AND qtyNow > 10;	WP_M09
SELECT pNo, pName, priceStd, costAvg, qtyNow FROM WP_M09.dbo.WP_vProduct WHERE pName=N'普洱茶';	WP_M09
SELECT pNo, pName, priceStd FROM WP_M09.dbo.WP_vProduct WHERE pName=N'有機烏龍茶';	WP_M09
SELECT COUNT(*) AS count FROM WP_M09.dbo.WP_vProvider WHERE pvTel IS NOT NULL AND pvTel <> '';	WP_M09
SELECT DISTINCT acctInId, amount, memName FROM WP_M09.dbo.WP_vAcctIn WHERE amount > 500 AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pNo, pName, priceStd FROM WP_M09.dbo.WP_vProduct WHERE pName=N'龍井茶';	WP_M09
SELECT pvSn, pvName, bankId, bankName FROM WP_M09.dbo.WP_vProvider WHERE bankId IS NOT NULL AND bankId<>'';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vTransfer WHERE LEFT(TransferId,6)='202401' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT TransferId, pName, qty FROM WP_M09.dbo.WP_vTransfer WHERE fWhName=N'特產中心' AND tfWhName=N'南區倉庫' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pvSn, pvName FROM WP_M09.dbo.WP_vProvider WHERE pvName LIKE N'%龍井%';	WP_M09
SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE memId='A008' AND LEFT(OutStkId,6)='202511' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT acctOutId, amount FROM WP_M09.dbo.WP_vAcctOut WHERE pName=N'益全香米' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT COUNT(DISTINCT pNo) AS products, SUM(qty) AS total_qty, SUM(qty * costAvg) AS total_value FROM WP_M09.dbo.WP_vInventory;	WP_M09
SELECT pvSn, pvName, pvTel, pvAddr, email FROM WP_M09.dbo.WP_vProvider WHERE pvSn='2';	WP_M09
SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE LEFT(OutStkId,6)='202506' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pNo, pName, priceStd FROM WP_M09.dbo.WP_vProduct WHERE priceStd > 150 ORDER BY priceStd DESC;	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctIn WHERE LEFT(acctInId,6)='202503' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT TransferId, pName, qty, fWhName, tfWhName FROM WP_M09.dbo.WP_vTransfer WHERE LEFT(TransferId,8)='20251020' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT empId, COUNT(*) AS count FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY empId ORDER BY count DESC;	WP_M09
SELECT COUNT(DISTINCT OutStkId) AS count FROM WP_M09.dbo.WP_vOutStock WHERE LEFT(OutStkId,6)='202508' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT AVG(priceStd) AS avg_price FROM WP_M09.dbo.WP_vProduct WHERE pvName=N'品茶苑';	WP_M09
SELECT pvSn, pvName, isStop FROM WP_M09.dbo.WP_vProvider WHERE pvSn='33';	WP_M09
SELECT DISTINCT acctInId, pName FROM WP_M09.dbo.WP_vAcctIn WHERE pName LIKE N'%高山%' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT pName, qty FROM WP_M09.dbo.WP_vInventory WHERE qty < 0;	WP_M09
SELECT pNo, pName FROM WP_M09.dbo.WP_vProduct WHERE pvName=N'永豐農產';	WP_M09
SELECT pName, qty, priceStd FROM WP_M09.dbo.WP_vInventory WHERE WarehouseName=N'北區倉庫' AND priceStd > 80;	WP_M09
SELECT DISTINCT pName, priceStd, priceMem FROM WP_M09.dbo.WP_vInventory WHERE priceMem > priceStd;	WP_M09
SELECT DISTINCT pName, SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory WHERE pName LIKE N'%龍井%' GROUP BY pName;	WP_M09
SELECT DISTINCT acctInId, amount, memName FROM WP_M09.dbo.WP_vAcctIn WHERE pName=N'龍井茶' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vOutStock WHERE memId='A007' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE LEFT(OutStkId,6)='202412' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT WarehouseName FROM WP_M09.dbo.WP_vInventory;	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vTransfer WHERE LEFT(TransferId,6)='202405' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT COUNT(*) AS total FROM WP_M09.dbo.WP_vProvider;	WP_M09
SELECT DISTINCT pName, qty FROM WP_M09.dbo.WP_vInventory WHERE qty < 15;	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctOut WHERE LEFT(acctOutId,6)='202409' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT pName, SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory WHERE pName LIKE N'%茶%' GROUP BY pName;	WP_M09
SELECT TransferId, pName, qty FROM WP_M09.dbo.WP_vTransfer WHERE fWhName=N'南區倉庫' AND tfWhName=N'中央倉庫' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT TOP 1 pvName, AVG(priceStd) AS avg_price FROM WP_M09.dbo.WP_vProduct GROUP BY pvName ORDER BY avg_price DESC;	WP_M09
SELECT SUM(qtyNow) AS total_qty FROM WP_M09.dbo.WP_vProduct WHERE pvName=N'台灣農業';	WP_M09
SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE LEFT(acctInId,8)='20251215' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE amount > 1000 AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory WHERE pName LIKE N'%豆%';	WP_M09
SELECT WarehouseName, qty FROM WP_M09.dbo.WP_vInventory WHERE pName=N'台灣高山茶';	WP_M09
SELECT pName, qty FROM WP_M09.dbo.WP_vInventory WHERE WarehouseName=N'南區倉庫' AND pvName=N'綠野農業';	WP_M09
SELECT pvSn, pvName, email FROM WP_M09.dbo.WP_vProvider WHERE email IS NOT NULL AND email<>'';	WP_M09
SELECT DISTINCT acctOutId, amount FROM WP_M09.dbo.WP_vAcctOut WHERE LEFT(acctOutId,6)='202511' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vTransfer WHERE pName=N'有機烏龍茶' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vTransfer WHERE pName=N'金萱茶包' AND LEFT(TransferId,6)='202512' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pName, qty, priceStd FROM WP_M09.dbo.WP_vInventory WHERE WarehouseName=N'南區倉庫' AND priceStd > 80;	WP_M09
SELECT COUNT(DISTINCT acctOutId) AS total FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N';	WP_M09
SELECT TransferId, pName, qty FROM WP_M09.dbo.WP_vTransfer WHERE fWhName=N'北區倉庫' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT COUNT(DISTINCT acctOutId) AS count FROM WP_M09.dbo.WP_vAcctOut WHERE LEFT(acctOutId,6)='202510' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(amtTotal) AS total_revenue FROM WP_M09.dbo.WP_vOutStock WHERE pName=N'玫瑰花茶' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pvSn, pvName, isSale FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'綠野農業';	WP_M09
SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE amount > 3000 AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT COUNT(*) AS count FROM WP_M09.dbo.WP_vTransfer WHERE LEFT(TransferId,6)='202509' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vTransfer WHERE pName=N'竹炭冬筍餅' AND LEFT(TransferId,6)='202512' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT COUNT(*) AS count FROM WP_M09.dbo.WP_vProduct WHERE pvName=N'健康食品';	WP_M09
SELECT TOP 10 TransferId, pName, qty, fWhName, tfWhName FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' ORDER BY qty DESC;	WP_M09
SELECT pvSn, pvName, isStop FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'綠野農業';	WP_M09
SELECT COUNT(*) AS count FROM WP_M09.dbo.WP_vProduct WHERE pvName=N'綠野農業';	WP_M09
SELECT COUNT(*) AS count FROM WP_M09.dbo.WP_vProduct WHERE pvName=N'品茶苑';	WP_M09
SELECT pNo, pName, priceStd FROM WP_M09.dbo.WP_vProduct WHERE pName=N'台灣高山茶';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory WHERE pName=N'玫瑰花茶';	WP_M09
SELECT DISTINCT pName, priceStd, SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory WHERE priceStd > 80 GROUP BY pName, priceStd;	WP_M09
SELECT DISTINCT OutStkId, amount, OutStkDate FROM WP_M09.dbo.WP_vOutStock WHERE memId='A011' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pName, qty, WarehouseName FROM WP_M09.dbo.WP_vInventory WHERE pName LIKE N'%烏龍%';	WP_M09
SELECT DISTINCT pName, SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory WHERE pvName=N'實垣有限公司' GROUP BY pName;	WP_M09
SELECT pNo, pName, priceStd FROM WP_M09.dbo.WP_vProduct WHERE pName=N'阿里山茶';	WP_M09
SELECT SUM(oStkDtlAmt) AS total FROM WP_M09.dbo.WP_vAcctIn WHERE pName=N'黑糖茶磚' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(qtyNow * costAvg) AS total_value FROM WP_M09.dbo.WP_vProduct WHERE pvName=N'台灣農業';	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctIn WHERE memId='A009' AND LEFT(acctInId,6)='202506' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pvSn, pvName, discount FROM WP_M09.dbo.WP_vProvider WHERE discount < 10 ORDER BY discount;	WP_M09
SELECT COUNT(*) AS lines FROM WP_M09.dbo.WP_vAcctIn WHERE acctInId='202512050001' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT pName FROM WP_M09.dbo.WP_vProduct ORDER BY pName;	WP_M09
SELECT pvSn, pvName, isStop FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'有機世界';	WP_M09
SELECT DISTINCT acctOutId, amount FROM WP_M09.dbo.WP_vAcctOut WHERE empId='B02' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE memId='A006' AND LEFT(OutStkId,6)='202509' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pName, qty, amtTotal FROM WP_M09.dbo.WP_vOutStock WHERE OutStkId='202511210055' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT COUNT(*) AS count FROM WP_M09.dbo.WP_vProduct WHERE pvName=N'台灣好茶';	WP_M09
SELECT DISTINCT acctInId FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pvSn, pvName, discount FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'有機世界';	WP_M09
SELECT pNo, pName, priceStd, costAvg, qtyNow FROM WP_M09.dbo.WP_vProduct WHERE pName=N'金萱茶包';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory WHERE pName=N'竹炭冬筍餅';	WP_M09
SELECT pvSn, pvName, discount FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'品茶苑';	WP_M09
SELECT pName, qty, WarehouseName FROM WP_M09.dbo.WP_vInventory WHERE pName LIKE N'%金萱%';	WP_M09
SELECT COUNT(DISTINCT acctOutId) AS count FROM WP_M09.dbo.WP_vAcctOut WHERE pvName=N'健康食品' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctIn WHERE LEFT(acctInId,6)='202406' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT COUNT(DISTINCT acctInId) AS total FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pNo, pName FROM WP_M09.dbo.WP_vProduct WHERE pvName=N'綠野農業';	WP_M09
SELECT DISTINCT pName, qty FROM WP_M09.dbo.WP_vInventory WHERE qty < 20;	WP_M09
SELECT COUNT(DISTINCT OutStkId) AS count FROM WP_M09.dbo.WP_vOutStock WHERE memId='A011' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE pName=N'黑糖茶磚' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT pName, qty FROM WP_M09.dbo.WP_vInventory WHERE WarehouseName=N'冷藏倉' ORDER BY qty DESC;	WP_M09
SELECT COUNT(DISTINCT OutStkId) AS count FROM WP_M09.dbo.WP_vOutStock WHERE LEFT(OutStkId,6)='202505' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT pName, isSale FROM WP_M09.dbo.WP_vInventory WHERE isSale<>'0';	WP_M09
SELECT WarehouseName, SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory GROUP BY WarehouseName ORDER BY total_qty DESC;	WP_M09
SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE LEFT(acctInId,6)='202405' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT TOP 5 pNo, pName, priceStd FROM WP_M09.dbo.WP_vProduct ORDER BY priceStd DESC;	WP_M09
SELECT pNo, pName, qtyNow, qtySafe FROM WP_M09.dbo.WP_vProduct WHERE qtyNow < qtySafe;	WP_M09
SELECT pvSn, pvName, pvDiscount FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'台灣農業';	WP_M09
SELECT SUM(DISTINCT amount) AS total_amount FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pName, qty, amtTotal FROM WP_M09.dbo.WP_vOutStock WHERE OutStkId='202510080025' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT amount FROM WP_M09.dbo.WP_vOutStock WHERE OutStkId='202507100018' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT acctOutId, amount FROM WP_M09.dbo.WP_vAcctOut WHERE LEFT(acctOutId,6)='202411' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pNo, pName, priceStd FROM WP_M09.dbo.WP_vProduct WHERE pName LIKE N'%茶%' AND priceStd > 100;	WP_M09
SELECT SUM(oStkDtlQty) AS total_qty FROM WP_M09.dbo.WP_vAcctIn WHERE pName=N'龍井茶' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pNo, pName, qtyNow FROM WP_M09.dbo.WP_vProduct WHERE pName=N'竹炭冬筍餅';	WP_M09
SELECT pvName, COUNT(DISTINCT pName) AS products, SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory GROUP BY pvName ORDER BY total_qty DESC;	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctIn WHERE LEFT(acctInId,6)='202510' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vTransfer WHERE LEFT(TransferId,6)='202504' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pvSn, pvName, pvAddr FROM WP_M09.dbo.WP_vProvider WHERE pvName=N'實垣有限公司';	WP_M09
SELECT DISTINCT acctInId, pName FROM WP_M09.dbo.WP_vAcctIn WHERE pName LIKE N'%豆%' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctOut WHERE pvName=N'實垣有限公司' AND LEFT(acctOutId,6)='202505' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT COUNT(*) AS count FROM WP_M09.dbo.WP_vTransfer WHERE pName=N'龍井茶' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vOutStock WHERE memName=N'農產品行' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT acctOutId FROM WP_M09.dbo.WP_vAcctOut WHERE isDel='N' AND dtlIsDel='N';	WP_M09
SELECT COUNT(*) AS count FROM WP_M09.dbo.WP_vProduct WHERE pvName=N'實垣有限公司';	WP_M09
SELECT WarehouseName, COUNT(DISTINCT pNo) AS product_types FROM WP_M09.dbo.WP_vInventory GROUP BY WarehouseName ORDER BY product_types DESC;	WP_M09
SELECT pNo, pName, qtyNow FROM WP_M09.dbo.WP_vProduct WHERE pvName=N'台灣好茶' AND qtyNow > 50;	WP_M09
SELECT pNo, pName, priceStd, costAvg, qtyNow FROM WP_M09.dbo.WP_vProduct WHERE pBarcode='4710632001318';	WP_M09
SELECT pNo, pName FROM WP_M09.dbo.WP_vProduct WHERE pNo LIKE '202502%';	WP_M09
SELECT DISTINCT OutStkId, qty, amtTotal FROM WP_M09.dbo.WP_vOutStock WHERE pName=N'阿里山茶' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT pName, qty FROM WP_M09.dbo.WP_vInventory WHERE pvName=N'實垣有限公司' AND qty > 50;	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctIn WHERE LEFT(acctInId,6)='202511' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vTransfer WHERE pName=N'東方美人茶' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(DISTINCT amount) AS total FROM WP_M09.dbo.WP_vAcctOut WHERE LEFT(acctOutId,6)='202407' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory WHERE pName=N'台灣高山茶';	WP_M09
SELECT SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vTransfer WHERE LEFT(TransferId,6)='202411' AND isDel='N' AND dtlIsDel='N';	WP_M09
SELECT DISTINCT pName, qty FROM WP_M09.dbo.WP_vInventory WHERE WarehouseName=N'東區倉庫' ORDER BY qty DESC;	WP_M09
SELECT SUM(qty * costAvg) AS total_value FROM WP_M09.dbo.WP_vInventory WHERE WarehouseName=N'特產中心';	WP_M09
