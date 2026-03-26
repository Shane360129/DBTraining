#!/usr/bin/env python3
"""
traindata_gen__no_isdel_corrective_samples.py
──────────────────────────────────────────────────────────────────────
為 WP_vInventory 和 WP_vProduct（以及 WP_vProvider）
生成「明確不含 isDel / dtlIsDel」的矯正訓練樣本。

目標：修正模型習慣對所有 view 添加 isDel='N' 的錯誤行為。

輸出：
  data/wp_m09/corrective_no_isdel.json   (新增樣本)
  data/wp_m09/train_spider_WP_M09.json   (合併後覆寫，原始備份為 *_backup_0318.json)
──────────────────────────────────────────────────────────────────────
"""

import json
import re
import shutil
from pathlib import Path
from datetime import datetime

# ── 路徑 ─────────────────────────────────────────────────────
SCHEMA_FILE   = "data/wp_m09/tables.json"
TRAIN_FILE    = "data/wp_m09/train_spider_WP_M09.json"
OUTPUT_NEW    = "data/wp_m09/corrective_no_isdel.json"
BACKUP_SUFFIX = "_backup_0318.json"

# ── 工具 ─────────────────────────────────────────────────────
def make_toks(query: str) -> list:
    return re.findall(r"[A-Za-z_][A-Za-z0-9_.]*|'[^']*'|[0-9]+|[().,;*<>=!%]+", query)

def make_entry(question: str, gold_sql: str, db_id: str = "WP_M09") -> dict:
    toks = make_toks(gold_sql)
    toks_no_val = []
    for t in toks:
        if t.startswith("'") or (t.isdigit() and len(t) > 2):
            toks_no_val.append("'value'")
        else:
            toks_no_val.append(t)
    return {
        "db_id": db_id,
        "query": gold_sql,
        "query_toks": toks,
        "query_toks_no_value": toks_no_val,
        "question": question,
        "question_toks": question.split(),
    }


# ════════════════════════════════════════════════════════════
# WP_vInventory 矯正樣本（絕不加 isDel）
# ════════════════════════════════════════════════════════════
INVENTORY_SAMPLES = [
    # 總數 / 聚合
    ("目前所有商品的總庫存數量是多少",
     "SELECT SUM(qty) FROM WP_M09.dbo.WP_vInventory;"),
    ("計算所有產品的庫存總量",
     "SELECT SUM(qty) FROM WP_M09.dbo.WP_vInventory;"),
    ("查詢目前商品總數",
     "SELECT COUNT(DISTINCT pNo) FROM WP_M09.dbo.WP_vInventory;"),
    ("有幾種不同的商品在庫存中",
     "SELECT COUNT(DISTINCT pNo) FROM WP_M09.dbo.WP_vInventory;"),
    ("查詢所有倉庫的庫存總金額",
     "SELECT SUM(qty * costAvg) FROM WP_M09.dbo.WP_vInventory;"),

    # 產品查詢（無條件）
    ("列出所有庫存商品的名稱",
     "SELECT DISTINCT pName FROM WP_M09.dbo.WP_vInventory;"),
    ("查詢所有庫存中的產品編號和名稱",
     "SELECT DISTINCT pNo, pName FROM WP_M09.dbo.WP_vInventory;"),
    ("顯示所有倉庫名稱",
     "SELECT DISTINCT WarehouseName FROM WP_M09.dbo.WP_vInventory;"),

    # 條件查詢（只用真實欄位）
    ("查詢倉庫 '特產中心' 的所有庫存商品",
     "SELECT pNo, pName, qty FROM WP_M09.dbo.WP_vInventory WHERE WarehouseName = N'特產中心';"),
    ("列出庫存數量為零的商品名稱",
     "SELECT DISTINCT pName FROM WP_M09.dbo.WP_vInventory WHERE qty = 0;"),
    ("查詢庫存低於安全庫存量的商品",
     "SELECT pNo, pName, qty, qtySafe FROM WP_M09.dbo.WP_vInventory WHERE qty < qtySafe;"),
    ("哪些產品的庫存數量低於 10",
     "SELECT pNo, pName, qty FROM WP_M09.dbo.WP_vInventory WHERE qty < 10;"),
    ("庫存最多的前 10 項產品",
     "SELECT TOP 10 pNo, pName, SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory GROUP BY pNo, pName ORDER BY total_qty DESC;"),
    ("查詢產品條碼 '4710632001318' 的庫存數量",
     "SELECT qty FROM WP_M09.dbo.WP_vInventory WHERE pBarcode = '4710632001318';"),
    ("有包含茶名稱的商品有哪些",
     "SELECT DISTINCT pName FROM WP_M09.dbo.WP_vInventory WHERE pName LIKE N'%茶%';"),
    ("查詢名稱含有「水」的商品庫存",
     "SELECT pNo, pName, qty FROM WP_M09.dbo.WP_vInventory WHERE pName LIKE N'%水%';"),
    ("查詢供應商編號 '21' 提供的所有庫存商品",
     "SELECT DISTINCT pName FROM WP_M09.dbo.WP_vInventory WHERE pvSn = '21';"),
    ("各倉庫的庫存總量分別是多少",
     "SELECT WarehouseName, SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory GROUP BY WarehouseName ORDER BY total_qty DESC;"),
    ("各商品類別的庫存總量",
     "SELECT pkName, SUM(qty) AS total_qty FROM WP_M09.dbo.WP_vInventory GROUP BY pkName ORDER BY total_qty DESC;"),
    ("查詢標準售價高於 100 的商品",
     "SELECT DISTINCT pNo, pName, priceStd FROM WP_M09.dbo.WP_vInventory WHERE priceStd > 100;"),

    # 日期篩選用 pNo LIKE（因為沒有日期欄）
    ("查詢 2024 年 12 月的庫存記錄",
     "SELECT pNo, pName, qty FROM WP_M09.dbo.WP_vInventory WHERE pNo LIKE '20241201%';"),
    ("列出 2025 年 1 月入庫的商品",
     "SELECT DISTINCT pNo, pName FROM WP_M09.dbo.WP_vInventory WHERE pNo LIKE '202501%';"),
]


# ════════════════════════════════════════════════════════════
# WP_vProduct 矯正樣本（絕不加 isDel）
# ════════════════════════════════════════════════════════════
PRODUCT_SAMPLES = [
    # 基本查詢
    ("查詢所有商品的名稱",
     "SELECT DISTINCT pName FROM WP_M09.dbo.WP_vProduct;"),
    ("列出所有產品的編號和名稱",
     "SELECT pNo, pName FROM WP_M09.dbo.WP_vProduct;"),
    ("商品總數有幾項",
     "SELECT COUNT(*) AS total_products FROM WP_M09.dbo.WP_vProduct;"),
    ("查詢所有商品的當前庫存總量",
     "SELECT SUM(qtyNow) FROM WP_M09.dbo.WP_vProduct;"),
    ("目前庫存最多的前 5 項產品",
     "SELECT TOP 5 pNo, pName, qtyNow FROM WP_M09.dbo.WP_vProduct ORDER BY qtyNow DESC;"),

    # 條件篩選
    ("查詢名稱含有「茶」的商品",
     "SELECT pNo, pName FROM WP_M09.dbo.WP_vProduct WHERE pName LIKE N'%茶%';"),
    ("有包含茶名稱的商品有哪些",
     "SELECT DISTINCT pName FROM WP_M09.dbo.WP_vProduct WHERE pName LIKE N'%茶%';"),
    ("查詢標準售價大於 50 的商品",
     "SELECT pNo, pName, priceStd FROM WP_M09.dbo.WP_vProduct WHERE priceStd > 50;"),
    ("庫存低於安全存量的商品有哪些",
     "SELECT pNo, pName, qtyNow, qtySafe FROM WP_M09.dbo.WP_vProduct WHERE qtyNow < qtySafe;"),
    ("查詢供應商名稱 '實垣有限公司' 供應的所有商品",
     "SELECT pNo, pName FROM WP_M09.dbo.WP_vProduct WHERE pvName = N'實垣有限公司';"),
    ("查詢條碼 '4710632001318' 的商品資料",
     "SELECT pNo, pName, priceStd, costAvg FROM WP_M09.dbo.WP_vProduct WHERE pBarcode = '4710632001318';"),

    # 聚合
    ("各類別商品的平均售價是多少",
     "SELECT pkName, AVG(priceStd) AS avg_price FROM WP_M09.dbo.WP_vProduct GROUP BY pkName ORDER BY avg_price DESC;"),
    ("成本最高的前 10 項商品",
     "SELECT TOP 10 pNo, pName, costAvg FROM WP_M09.dbo.WP_vProduct ORDER BY costAvg DESC;"),
    ("查詢所有商品的庫存總金額",
     "SELECT SUM(qtyNow * costAvg) AS total_value FROM WP_M09.dbo.WP_vProduct;"),

    # 日期用 pNo LIKE（注意：pNo 非日期欄，但 pNo 前 8 碼可為日期）
    ("查詢 2024 年 12 月新增的商品",
     "SELECT pNo, pName FROM WP_M09.dbo.WP_vProduct WHERE pNo LIKE '202412%';"),
]


# ════════════════════════════════════════════════════════════
# WP_vProvider 矯正樣本（絕不加 isDel）
# ════════════════════════════════════════════════════════════
PROVIDER_SAMPLES = [
    ("查詢所有供應商的名稱",
     "SELECT DISTINCT pvName FROM WP_M09.dbo.WP_vProvider;"),
    ("列出所有供應商的編號和名稱",
     "SELECT pvSn, pvName FROM WP_M09.dbo.WP_vProvider;"),
    ("共有幾個供應商",
     "SELECT COUNT(*) AS total_providers FROM WP_M09.dbo.WP_vProvider;"),
    ("查詢供應商名稱含有「有限公司」的廠商",
     "SELECT pvSn, pvName FROM WP_M09.dbo.WP_vProvider WHERE pvName LIKE N'%有限公司%';"),
    ("查詢供應商編號為 '21' 的廠商資料",
     "SELECT pvSn, pvName FROM WP_M09.dbo.WP_vProvider WHERE pvSn = '21';"),
]


# ════════════════════════════════════════════════════════════
# 主程式
# ════════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("生成 無 isDel 矯正訓練樣本")
    print("=" * 60)

    # 建立樣本
    all_new = []
    for q, s in INVENTORY_SAMPLES:
        all_new.append(make_entry(q, s))
    for q, s in PRODUCT_SAMPLES:
        all_new.append(make_entry(q, s))
    for q, s in PROVIDER_SAMPLES:
        all_new.append(make_entry(q, s))

    print(f"WP_vInventory 樣本: {len(INVENTORY_SAMPLES)} 筆")
    print(f"WP_vProduct   樣本: {len(PRODUCT_SAMPLES)} 筆")
    print(f"WP_vProvider  樣本: {len(PROVIDER_SAMPLES)} 筆")
    print(f"合計新增: {len(all_new)} 筆")

    # 存新樣本
    with open(OUTPUT_NEW, "w", encoding="utf-8") as f:
        json.dump(all_new, f, ensure_ascii=False, indent=2)
    print(f"\n已儲存新樣本: {OUTPUT_NEW}")

    # 讀取現有訓練集
    with open(TRAIN_FILE, "r", encoding="utf-8") as f:
        existing = json.load(f)
    print(f"現有訓練集: {len(existing)} 筆")

    # ── 移除現有訓練集中 WP_vInventory/WP_vProduct/WP_vProvider
    #    帶有 isDel / dtlIsDel 的錯誤樣本 ──────────────────────
    removed = 0
    clean_existing = []
    for item in existing:
        sql = item.get("query", "")
        views_in = set(re.findall(r"WP_v\w+", sql))
        no_del_views = views_in & {"WP_vInventory", "WP_vProduct", "WP_vProvider"}
        has_del = bool(re.search(r"\bisdel\b|\bdtlisdel\b", sql, re.IGNORECASE))
        if no_del_views and has_del:
            removed += 1
            continue
        clean_existing.append(item)
    print(f"移除錯誤樣本（不應有 isDel）: {removed} 筆")

    # 合併
    merged = clean_existing + all_new
    print(f"合併後總計: {len(merged)} 筆")

    # 備份原始訓練集
    backup_path = TRAIN_FILE.replace(".json", BACKUP_SUFFIX)
    shutil.copy(TRAIN_FILE, backup_path)
    print(f"原始訓練集已備份: {backup_path}")

    # 覆寫
    with open(TRAIN_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    print(f"已更新訓練集: {TRAIN_FILE}")
    print("\n完成！接下來請執行：")
    print("  python train__dora_spider_v0318.py")


if __name__ == "__main__":
    import os
    os.chdir(Path(__file__).parent)
    main()
