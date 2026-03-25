#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate Spider1-format training and validation data for WP_M09 database.
Uses real values from actual database samples. All questions in 繁體中文.
Output:
  data/wp_m09/train_from_samples.json  (~350 samples, ~50 per view)
  data/wp_m09/test_from_samples.json   (~105 samples, ~15 per view)
"""

import json
import re
import os

# ---------------------------------------------------------------------------
# Tokenization helpers
# ---------------------------------------------------------------------------

def tokenize_sql(sql: str) -> list:
    """Split SQL into tokens, keeping N'...' and '...' as single tokens."""
    tokens = []
    i = 0
    s = sql.strip()
    while i < len(s):
        # Skip whitespace
        if s[i].isspace():
            i += 1
            continue
        # N'...' string
        if s[i] == 'N' and i + 1 < len(s) and s[i+1] == "'":
            j = i + 2
            while j < len(s):
                if s[j] == "'" and (j+1 >= len(s) or s[j+1] != "'"):
                    break
                if s[j] == "'" and j+1 < len(s) and s[j+1] == "'":
                    j += 2
                    continue
                j += 1
            tokens.append(s[i:j+1])
            i = j + 1
            continue
        # Regular '...' string
        if s[i] == "'":
            j = i + 1
            while j < len(s):
                if s[j] == "'" and (j+1 >= len(s) or s[j+1] != "'"):
                    break
                if s[j] == "'" and j+1 < len(s) and s[j+1] == "'":
                    j += 2
                    continue
                j += 1
            tokens.append(s[i:j+1])
            i = j + 1
            continue
        # Operators / punctuation (single char)
        if s[i] in '(),.;=<>!*':
            # Handle !=, <=, >=
            if s[i] in '<>!' and i+1 < len(s) and s[i+1] == '=':
                tokens.append(s[i:i+2])
                i += 2
            else:
                tokens.append(s[i])
                i += 1
            continue
        # Word / number
        j = i
        while j < len(s) and not s[j].isspace() and s[j] not in "(),.;=<>!'*":
            j += 1
        if j > i:
            tokens.append(s[i:j])
            i = j
        else:
            i += 1
    return tokens


def no_value_toks(toks: list) -> list:
    """Replace literal values in token list with 'value'."""
    result = []
    for t in toks:
        # String literals: '...' or N'...'
        if re.match(r"^N?'.*'$", t):
            result.append("'value'")
        # Pure numeric (int or float)
        elif re.match(r"^-?\d+(\.\d+)?$", t):
            result.append("value")
        else:
            result.append(t)
    return result


def tokenize_chinese(text: str) -> list:
    """
    Simple character-level tokenizer for Chinese text.
    ASCII words are kept as whole tokens; Chinese chars are individual.
    """
    tokens = []
    buf = ""
    for ch in text:
        if ord(ch) > 127:
            if buf:
                tokens.append(buf)
                buf = ""
            tokens.append(ch)
        elif ch in " \t\n":
            if buf:
                tokens.append(buf)
                buf = ""
        else:
            buf += ch
    if buf:
        tokens.append(buf)
    return tokens


def make_sample(question: str, query: str) -> dict:
    toks = tokenize_sql(query)
    return {
        "db_id": "WP_M09",
        "query": query,
        "query_toks": toks,
        "query_toks_no_value": no_value_toks(toks),
        "question": question,
        "question_toks": tokenize_chinese(question),
    }


# ===========================================================================
# WP_vAcctIn  (應付帳款收款)  — has isDel, dtlIsDel
# ===========================================================================
ISDEL = "isDel = 'N' AND dtlIsDel = 'N'"

def gen_acct_in_train():
    tbl = "WP_M09.dbo.WP_vAcctIn"
    samples = []

    def s(q, sql): return make_sample(q, sql)

    # 1. 查詢特定 acctInId 的 amount
    samples.append(s(
        "請問應付帳款編號 '202512050001' 的應付金額是多少？",
        f"SELECT TOP 1 amount FROM {tbl} WHERE acctInId = '202512050001' AND {ISDEL};"
    ))
    samples.append(s(
        "查詢 acctInId 為 '202512160001' 的應付金額。",
        f"SELECT TOP 1 amount FROM {tbl} WHERE acctInId = '202512160001' AND {ISDEL};"
    ))
    samples.append(s(
        "應付帳款編號 '202602090001' 的金額是多少？",
        f"SELECT TOP 1 amount FROM {tbl} WHERE acctInId = '202602090001' AND {ISDEL};"
    ))

    # 2. 查詢 memName
    samples.append(s(
        "請列出應付帳款編號 '202512050001' 的會員名稱。",
        f"SELECT TOP 1 memName FROM {tbl} WHERE acctInId = '202512050001' AND {ISDEL};"
    ))
    samples.append(s(
        "acctInId='202512160001' 的會員名稱是什麼？",
        f"SELECT TOP 1 memName FROM {tbl} WHERE acctInId = '202512160001' AND {ISDEL};"
    ))

    # 3. 依 memId 查詢
    samples.append(s(
        "查詢會員代號 'A006' 的所有應付帳款記錄。",
        f"SELECT acctInId, amount FROM {tbl} WHERE memId = 'A006' AND {ISDEL};"
    ))
    samples.append(s(
        "會員 'A007' 有哪些應付帳款？",
        f"SELECT acctInId, amount FROM {tbl} WHERE memId = 'A007' AND {ISDEL};"
    ))
    samples.append(s(
        "列出會員代號 'A002' 的所有應付帳款金額。",
        f"SELECT acctInId, amount FROM {tbl} WHERE memId = 'A002' AND {ISDEL};"
    ))

    # 4. COUNT
    samples.append(s(
        "應付帳款共有幾筆有效記錄？",
        f"SELECT COUNT(*) FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "會員 '麻竹園' 有幾筆應付帳款？",
        f"SELECT COUNT(*) FROM {tbl} WHERE memName = N'麻竹園' AND {ISDEL};"
    ))
    samples.append(s(
        "請計算應付帳款中會員代號 'A006' 的筆數。",
        f"SELECT COUNT(*) FROM {tbl} WHERE memId = 'A006' AND {ISDEL};"
    ))

    # 5. SUM
    samples.append(s(
        "2025年12月05日的應付帳款總金額是多少？",
        f"SELECT SUM(amount) FROM {tbl} WHERE LEFT(acctInId, 8) = '20251205' AND {ISDEL};"
    ))
    samples.append(s(
        "2025年12月16日的應付金額合計為何？",
        f"SELECT SUM(amount) FROM {tbl} WHERE LEFT(acctInId, 8) = '20251216' AND {ISDEL};"
    ))
    samples.append(s(
        "2026年02月09日所有應付帳款的金額總和。",
        f"SELECT SUM(amount) FROM {tbl} WHERE LEFT(acctInId, 8) = '20260209' AND {ISDEL};"
    ))
    samples.append(s(
        "計算會員 '旅遊部' 的應付帳款總額。",
        f"SELECT SUM(amount) FROM {tbl} WHERE memName = N'旅遊部' AND {ISDEL};"
    ))
    samples.append(s(
        "會員代號 'A002' 的應付帳款金額總計是多少？",
        f"SELECT SUM(amount) FROM {tbl} WHERE memId = 'A002' AND {ISDEL};"
    ))

    # 6. 依商品名稱查詢
    samples.append(s(
        "查詢商品 '益全香米' 的應付帳款明細數量。",
        f"SELECT oStkDtlQty FROM {tbl} WHERE pName = N'益全香米' AND {ISDEL};"
    ))
    samples.append(s(
        "商品 '竹炭冬筍餅' 在應付帳款中的出庫數量。",
        f"SELECT oStkDtlQty FROM {tbl} WHERE pName = N'竹炭冬筍餅' AND {ISDEL};"
    ))
    samples.append(s(
        "查詢 '量販包冬筍餅' 的應付帳款出庫金額。",
        f"SELECT oStkDtlAmt FROM {tbl} WHERE pName = N'量販包冬筍餅' AND {ISDEL};"
    ))
    samples.append(s(
        "商品 '芝麻香米捲' 的出庫明細金額是多少？",
        f"SELECT oStkDtlAmt FROM {tbl} WHERE pName = N'芝麻香米捲' AND {ISDEL};"
    ))
    samples.append(s(
        "查詢 '池上米餅-醬燒' 的出庫數量與金額。",
        f"SELECT oStkDtlQty, oStkDtlAmt FROM {tbl} WHERE pName = N'池上米餅-醬燒' AND {ISDEL};"
    ))
    samples.append(s(
        "應付帳款中 '海苔米香180G' 的出庫明細。",
        f"SELECT oStkDtlQty, oStkDtlAmt FROM {tbl} WHERE pName = N'海苔米香180G' AND {ISDEL};"
    ))
    samples.append(s(
        "'池農米餅-椒鹽' 的出庫數量與出庫金額各為多少？",
        f"SELECT oStkDtlQty, oStkDtlAmt FROM {tbl} WHERE pName = N'池農米餅-椒鹽' AND {ISDEL};"
    ))

    # 7. ORDER BY
    samples.append(s(
        "請依應付金額由高至低列出所有應付帳款。",
        f"SELECT acctInId, amount FROM {tbl} WHERE {ISDEL} ORDER BY amount DESC;"
    ))
    samples.append(s(
        "依應付帳款編號排序，列出所有有效的帳款紀錄。",
        f"SELECT acctInId, memName, amount FROM {tbl} WHERE {ISDEL} ORDER BY acctInId;"
    ))
    samples.append(s(
        "列出應付帳款金額最低的前5筆記錄。",
        f"SELECT TOP 5 acctInId, amount FROM {tbl} WHERE {ISDEL} ORDER BY amount ASC;"
    ))

    # 8. GROUP BY
    samples.append(s(
        "依會員名稱統計各會員的應付帳款筆數。",
        f"SELECT memName, COUNT(*) FROM {tbl} WHERE {ISDEL} GROUP BY memName;"
    ))
    samples.append(s(
        "依會員代號統計各會員的應付帳款總金額。",
        f"SELECT memId, SUM(amount) FROM {tbl} WHERE {ISDEL} GROUP BY memId;"
    ))
    samples.append(s(
        "各會員的應付帳款筆數及總金額。",
        f"SELECT memName, COUNT(*), SUM(amount) FROM {tbl} WHERE {ISDEL} GROUP BY memName;"
    ))
    samples.append(s(
        "依商品名稱統計應付帳款出庫數量總和。",
        f"SELECT pName, SUM(oStkDtlQty) FROM {tbl} WHERE {ISDEL} GROUP BY pName;"
    ))
    samples.append(s(
        "各商品的應付帳款出庫金額合計。",
        f"SELECT pName, SUM(oStkDtlAmt) FROM {tbl} WHERE {ISDEL} GROUP BY pName;"
    ))

    # 9. 日期過濾多條件
    samples.append(s(
        "2025年12月05日會員 '麻竹園' 的應付帳款金額。",
        f"SELECT amount FROM {tbl} WHERE LEFT(acctInId, 8) = '20251205' AND memName = N'麻竹園' AND {ISDEL};"
    ))
    samples.append(s(
        "2025年12月16日 'A007' 的應付帳款金額。",
        f"SELECT amount FROM {tbl} WHERE LEFT(acctInId, 8) = '20251216' AND memId = 'A007' AND {ISDEL};"
    ))

    # 10. oStkDtlAmt / oStkDtlQty
    samples.append(s(
        "查詢出庫明細金額超過 100 的應付帳款記錄。",
        f"SELECT acctInId, pName, oStkDtlAmt FROM {tbl} WHERE oStkDtlAmt > 100 AND {ISDEL};"
    ))
    samples.append(s(
        "出庫明細數量超過 10 的商品有哪些？",
        f"SELECT DISTINCT pName FROM {tbl} WHERE oStkDtlQty > 10 AND {ISDEL};"
    ))
    samples.append(s(
        "oStkDtlQty 等於 12 的應付帳款記錄。",
        f"SELECT acctInId, pName FROM {tbl} WHERE oStkDtlQty = 12 AND {ISDEL};"
    ))
    samples.append(s(
        "出庫明細金額為 350 的應付帳款記錄。",
        f"SELECT acctInId, pName FROM {tbl} WHERE oStkDtlAmt = 350.0 AND {ISDEL};"
    ))
    samples.append(s(
        "出庫明細金額為 120 的應付帳款紀錄。",
        f"SELECT acctInId, pName FROM {tbl} WHERE oStkDtlAmt = 120.0 AND {ISDEL};"
    ))

    # 11. 全欄查詢
    samples.append(s(
        "列出應付帳款 '202512050001' 的所有欄位資料。",
        f"SELECT * FROM {tbl} WHERE acctInId = '202512050001' AND {ISDEL};"
    ))
    samples.append(s(
        "取得應付帳款 '202512160001' 的完整資料。",
        f"SELECT * FROM {tbl} WHERE acctInId = '202512160001' AND {ISDEL};"
    ))

    # 12. DISTINCT
    samples.append(s(
        "列出所有有效應付帳款中不重複的會員名稱。",
        f"SELECT DISTINCT memName FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "應付帳款中涉及哪些不重複的商品？",
        f"SELECT DISTINCT pName FROM {tbl} WHERE {ISDEL};"
    ))

    # 13. amount 範圍
    samples.append(s(
        "查詢應付金額超過 10000 的帳款記錄。",
        f"SELECT acctInId, amount FROM {tbl} WHERE amount > 10000 AND {ISDEL};"
    ))
    samples.append(s(
        "應付金額在 5000 到 15000 之間的帳款。",
        f"SELECT acctInId, amount FROM {tbl} WHERE amount >= 5000 AND amount <= 15000 AND {ISDEL};"
    ))
    samples.append(s(
        "應付金額最高的一筆帳款記錄。",
        f"SELECT TOP 1 acctInId, memName, amount FROM {tbl} WHERE {ISDEL} ORDER BY amount DESC;"
    ))

    # 14. memName + amount
    samples.append(s(
        "推廣部的應付帳款明細。",
        f"SELECT acctInId, pName, oStkDtlQty, oStkDtlAmt FROM {tbl} WHERE memName = N'推廣部' AND {ISDEL};"
    ))
    samples.append(s(
        "旅遊部的應付帳款記錄有哪些商品？",
        f"SELECT DISTINCT pName FROM {tbl} WHERE memName = N'旅遊部' AND {ISDEL};"
    ))

    # 15. 多欄位查詢
    samples.append(s(
        "列出所有有效應付帳款的編號、會員名稱及應付金額。",
        f"SELECT acctInId, memName, amount FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "查詢所有有效應付帳款的商品名稱、出庫數量及出庫金額。",
        f"SELECT pName, oStkDtlQty, oStkDtlAmt FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "列出各應付帳款編號下不重複的出庫單數量。",
        f"SELECT acctInId, COUNT(DISTINCT OutStkId) FROM {tbl} WHERE {ISDEL} GROUP BY acctInId;"
    ))

    return samples


def gen_acct_in_test():
    tbl = "WP_M09.dbo.WP_vAcctIn"
    samples = []
    def s(q, sql): return make_sample(q, sql)

    samples.append(s(
        "應付帳款 '202602090001' 涉及哪些商品？",
        f"SELECT DISTINCT pName FROM {tbl} WHERE acctInId = '202602090001' AND {ISDEL};"
    ))
    samples.append(s(
        "所有有效應付帳款的筆數為何？",
        f"SELECT COUNT(*) FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "各日期的應付帳款總金額。",
        f"SELECT LEFT(acctInId, 8), SUM(amount) FROM {tbl} WHERE {ISDEL} GROUP BY LEFT(acctInId, 8);"
    ))
    samples.append(s(
        "應付帳款出庫明細數量最多的商品是什麼？",
        f"SELECT TOP 1 pName, SUM(oStkDtlQty) FROM {tbl} WHERE {ISDEL} GROUP BY pName ORDER BY SUM(oStkDtlQty) DESC;"
    ))
    samples.append(s(
        "2025年12月份所有應付帳款的金額總和。",
        f"SELECT SUM(amount) FROM {tbl} WHERE LEFT(acctInId, 6) = '202512' AND {ISDEL};"
    ))
    samples.append(s(
        "查詢出庫明細金額為 130 的應付帳款編號與商品。",
        f"SELECT acctInId, pName FROM {tbl} WHERE oStkDtlAmt = 130.0 AND {ISDEL};"
    ))
    samples.append(s(
        "各會員的應付帳款金額合計，依金額由高至低排序。",
        f"SELECT memName, SUM(amount) FROM {tbl} WHERE {ISDEL} GROUP BY memName ORDER BY SUM(amount) DESC;"
    ))
    samples.append(s(
        "會員 'A006' 在 2025年12月05日的出庫明細。",
        f"SELECT pName, oStkDtlQty, oStkDtlAmt FROM {tbl} WHERE memId = 'A006' AND LEFT(acctInId, 8) = '20251205' AND {ISDEL};"
    ))
    samples.append(s(
        "應付帳款中出庫數量等於 1 的記錄有幾筆？",
        f"SELECT COUNT(*) FROM {tbl} WHERE oStkDtlQty = 1 AND {ISDEL};"
    ))
    samples.append(s(
        "應付金額最低的帳款為何？",
        f"SELECT TOP 1 acctInId, memName, amount FROM {tbl} WHERE {ISDEL} ORDER BY amount ASC;"
    ))
    samples.append(s(
        "列出有效應付帳款的不重複會員代號。",
        f"SELECT DISTINCT memId FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "應付帳款出庫數量大於 15 的商品。",
        f"SELECT pName, oStkDtlQty FROM {tbl} WHERE oStkDtlQty > 15 AND {ISDEL};"
    ))
    samples.append(s(
        "查詢 '麻竹園' 在所有日期的應付帳款編號及金額。",
        f"SELECT acctInId, amount FROM {tbl} WHERE memName = N'麻竹園' AND {ISDEL} ORDER BY acctInId;"
    ))
    samples.append(s(
        "推廣部的應付帳款總金額是多少？",
        f"SELECT SUM(amount) FROM {tbl} WHERE memName = N'推廣部' AND {ISDEL};"
    ))
    samples.append(s(
        "依商品名稱列出應付帳款出庫明細金額總和，取前3名。",
        f"SELECT TOP 3 pName, SUM(oStkDtlAmt) FROM {tbl} WHERE {ISDEL} GROUP BY pName ORDER BY SUM(oStkDtlAmt) DESC;"
    ))

    return samples


# ===========================================================================
# WP_vAcctOut  (應付帳款付款)  — has isDel, dtlIsDel
# ===========================================================================

def gen_acct_out_train():
    tbl = "WP_M09.dbo.WP_vAcctOut"
    samples = []
    def s(q, sql): return make_sample(q, sql)

    # 基本查詢
    samples.append(s(
        "應付帳款 '202512300001' 的付款金額是多少？",
        f"SELECT TOP 1 amount FROM {tbl} WHERE acctOutId = '202512300001' AND {ISDEL};"
    ))
    samples.append(s(
        "查詢 acctOutId='202512260063' 的應付帳款金額。",
        f"SELECT TOP 1 amount FROM {tbl} WHERE acctOutId = '202512260063' AND {ISDEL};"
    ))
    samples.append(s(
        "應付帳款 '202512190003' 的金額。",
        f"SELECT TOP 1 amount FROM {tbl} WHERE acctOutId = '202512190003' AND {ISDEL};"
    ))

    # pvName
    samples.append(s(
        "查詢廠商 '新福華商行' 的所有應付帳款記錄。",
        f"SELECT acctOutId, amount FROM {tbl} WHERE pvName = N'新福華商行' AND {ISDEL};"
    ))
    samples.append(s(
        "廠商 '優豆食品' 有哪些應付帳款？",
        f"SELECT acctOutId, amount FROM {tbl} WHERE pvName = N'優豆食品' AND {ISDEL};"
    ))
    samples.append(s(
        "列出廠商 '宏碁蜂蜜' 的應付帳款明細。",
        f"SELECT acctOutId, pName, qty, amount FROM {tbl} WHERE pvName = N'宏碁蜂蜜' AND {ISDEL};"
    ))

    # pvSn
    samples.append(s(
        "廠商序號 22 的應付帳款記錄。",
        f"SELECT acctOutId, pvName, amount FROM {tbl} WHERE pvSn = 22 AND {ISDEL};"
    ))
    samples.append(s(
        "廠商序號 143 的應付帳款金額。",
        f"SELECT TOP 1 amount FROM {tbl} WHERE pvSn = 143 AND {ISDEL};"
    ))

    # COUNT
    samples.append(s(
        "應付帳款付款共有幾筆有效記錄？",
        f"SELECT COUNT(*) FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "廠商 '新福華商行' 有幾筆應付帳款？",
        f"SELECT COUNT(*) FROM {tbl} WHERE pvName = N'新福華商行' AND {ISDEL};"
    ))

    # SUM
    samples.append(s(
        "2025年12月30日的應付帳款總金額。",
        f"SELECT SUM(amount) FROM {tbl} WHERE LEFT(acctOutId, 8) = '20251230' AND {ISDEL};"
    ))
    samples.append(s(
        "2025年12月26日的應付帳款付款金額合計。",
        f"SELECT SUM(amount) FROM {tbl} WHERE LEFT(acctOutId, 8) = '20251226' AND {ISDEL};"
    ))
    samples.append(s(
        "2025年12月19日的應付帳款總額。",
        f"SELECT SUM(amount) FROM {tbl} WHERE LEFT(acctOutId, 8) = '20251219' AND {ISDEL};"
    ))
    samples.append(s(
        "廠商 '優豆食品' 的應付帳款總金額。",
        f"SELECT SUM(amount) FROM {tbl} WHERE pvName = N'優豆食品' AND {ISDEL};"
    ))

    # isTax
    samples.append(s(
        "有含稅（isTax='Y'）的應付帳款記錄有哪些？",
        f"SELECT acctOutId, pvName, amount FROM {tbl} WHERE isTax = 'Y' AND {ISDEL};"
    ))
    samples.append(s(
        "不含稅的應付帳款記錄。",
        f"SELECT acctOutId, pvName, amount FROM {tbl} WHERE isTax = 'N' AND {ISDEL};"
    ))
    samples.append(s(
        "含稅的應付帳款共有幾筆？",
        f"SELECT COUNT(*) FROM {tbl} WHERE isTax = 'Y' AND {ISDEL};"
    ))
    samples.append(s(
        "含稅與不含稅的應付帳款各有幾筆？",
        f"SELECT isTax, COUNT(*) FROM {tbl} WHERE {ISDEL} GROUP BY isTax;"
    ))

    # pName
    samples.append(s(
        "查詢商品 '台東來的小魚' 的應付帳款數量與金額。",
        f"SELECT qty, amount FROM {tbl} WHERE pName = N'台東來的小魚' AND {ISDEL};"
    ))
    samples.append(s(
        "商品 '池農米餅' 在應付帳款中的數量。",
        f"SELECT qty FROM {tbl} WHERE pName = N'池農米餅' AND {ISDEL};"
    ))

    # qty
    samples.append(s(
        "查詢數量為 10 的應付帳款記錄。",
        f"SELECT acctOutId, pName, amount FROM {tbl} WHERE qty = 10 AND {ISDEL};"
    ))
    samples.append(s(
        "數量等於 6 的應付帳款記錄。",
        f"SELECT acctOutId, pName, amount FROM {tbl} WHERE qty = 6 AND {ISDEL};"
    ))

    # ORDER BY
    samples.append(s(
        "依金額由高至低列出所有應付帳款付款記錄。",
        f"SELECT acctOutId, pvName, amount FROM {tbl} WHERE {ISDEL} ORDER BY amount DESC;"
    ))
    samples.append(s(
        "依廠商名稱排序應付帳款記錄。",
        f"SELECT acctOutId, pvName, amount FROM {tbl} WHERE {ISDEL} ORDER BY pvName;"
    ))

    # GROUP BY
    samples.append(s(
        "依廠商名稱統計應付帳款筆數與總金額。",
        f"SELECT pvName, COUNT(*), SUM(amount) FROM {tbl} WHERE {ISDEL} GROUP BY pvName;"
    ))
    samples.append(s(
        "各廠商的應付帳款總金額。",
        f"SELECT pvName, SUM(amount) FROM {tbl} WHERE {ISDEL} GROUP BY pvName;"
    ))
    samples.append(s(
        "依商品名稱統計應付帳款數量總和。",
        f"SELECT pName, SUM(qty) FROM {tbl} WHERE {ISDEL} GROUP BY pName;"
    ))

    # amount range
    samples.append(s(
        "應付帳款金額超過 15000 的記錄。",
        f"SELECT acctOutId, pvName, amount FROM {tbl} WHERE amount > 15000 AND {ISDEL};"
    ))
    samples.append(s(
        "金額在 10000 到 20000 之間的應付帳款記錄。",
        f"SELECT acctOutId, pvName, amount FROM {tbl} WHERE amount >= 10000 AND amount <= 20000 AND {ISDEL};"
    ))
    samples.append(s(
        "應付帳款金額最高的一筆。",
        f"SELECT TOP 1 acctOutId, pvName, amount FROM {tbl} WHERE {ISDEL} ORDER BY amount DESC;"
    ))

    # multi-condition
    samples.append(s(
        "2025年12月26日廠商 '新福華商行' 的應付帳款金額。",
        f"SELECT amount FROM {tbl} WHERE LEFT(acctOutId, 8) = '20251226' AND pvName = N'新福華商行' AND {ISDEL};"
    ))
    samples.append(s(
        "含稅且金額超過 15000 的應付帳款記錄。",
        f"SELECT acctOutId, pvName, amount FROM {tbl} WHERE isTax = 'Y' AND amount > 15000 AND {ISDEL};"
    ))

    # DISTINCT
    samples.append(s(
        "應付帳款中涉及哪些廠商？",
        f"SELECT DISTINCT pvName FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "應付帳款中涉及哪些商品？",
        f"SELECT DISTINCT pName FROM {tbl} WHERE {ISDEL};"
    ))

    # 全欄
    samples.append(s(
        "取得應付帳款 '202512300001' 的完整資料。",
        f"SELECT * FROM {tbl} WHERE acctOutId = '202512300001' AND {ISDEL};"
    ))
    samples.append(s(
        "列出所有有效應付帳款付款的編號、廠商及金額。",
        f"SELECT acctOutId, pvName, amount FROM {tbl} WHERE {ISDEL};"
    ))

    # TOP N
    samples.append(s(
        "列出金額最高的前3筆應付帳款付款。",
        f"SELECT TOP 3 acctOutId, pvName, amount FROM {tbl} WHERE {ISDEL} ORDER BY amount DESC;"
    ))
    samples.append(s(
        "最近的應付帳款付款記錄（依編號倒序）。",
        f"SELECT TOP 1 acctOutId, pvName, amount FROM {tbl} WHERE {ISDEL} ORDER BY acctOutId DESC;"
    ))

    # pvSn + pvName 結合
    samples.append(s(
        "廠商序號 22 且含稅的應付帳款記錄。",
        f"SELECT acctOutId, amount FROM {tbl} WHERE pvSn = 22 AND isTax = 'Y' AND {ISDEL};"
    ))
    samples.append(s(
        "廠商 '優豆食品' 的應付帳款商品名稱。",
        f"SELECT DISTINCT pName FROM {tbl} WHERE pvName = N'優豆食品' AND {ISDEL};"
    ))

    # qty < threshold
    samples.append(s(
        "應付帳款數量小於 8 的記錄。",
        f"SELECT acctOutId, pName, qty FROM {tbl} WHERE qty < 8 AND {ISDEL};"
    ))
    samples.append(s(
        "應付帳款中數量最多的商品。",
        f"SELECT TOP 1 pName, SUM(qty) FROM {tbl} WHERE {ISDEL} GROUP BY pName ORDER BY SUM(qty) DESC;"
    ))

    # date 2025-12
    samples.append(s(
        "2025年12月份的應付帳款付款總金額。",
        f"SELECT SUM(amount) FROM {tbl} WHERE LEFT(acctOutId, 6) = '202512' AND {ISDEL};"
    ))
    samples.append(s(
        "2025年12月份有幾筆應付帳款付款？",
        f"SELECT COUNT(*) FROM {tbl} WHERE LEFT(acctOutId, 6) = '202512' AND {ISDEL};"
    ))

    # extra to reach ~50
    samples.append(s(
        "應付帳款的不重複廠商序號有幾個？",
        f"SELECT COUNT(DISTINCT pvSn) FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "廠商 '優豆食品' 的應付帳款含稅情況。",
        f"SELECT acctOutId, isTax, amount FROM {tbl} WHERE pvName = N'優豆食品' AND {ISDEL};"
    ))
    samples.append(s(
        "2025年12月30日含稅的應付帳款金額。",
        f"SELECT amount FROM {tbl} WHERE LEFT(acctOutId, 8) = '20251230' AND isTax = 'Y' AND {ISDEL};"
    ))
    samples.append(s(
        "應付帳款中各商品的平均數量。",
        f"SELECT pName, AVG(qty) FROM {tbl} WHERE {ISDEL} GROUP BY pName;"
    ))
    samples.append(s(
        "廠商序號 22 的應付帳款商品清單。",
        f"SELECT DISTINCT pName FROM {tbl} WHERE pvSn = 22 AND {ISDEL};"
    ))
    samples.append(s(
        "所有有效應付帳款付款的完整明細。",
        f"SELECT acctOutId, pvName, pName, qty, amount, isTax FROM {tbl} WHERE {ISDEL};"
    ))

    return samples


def gen_acct_out_test():
    tbl = "WP_M09.dbo.WP_vAcctOut"
    samples = []
    def s(q, sql): return make_sample(q, sql)

    samples.append(s(
        "所有有效應付帳款付款記錄的筆數。",
        f"SELECT COUNT(*) FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "應付帳款付款中不重複的廠商序號有哪些？",
        f"SELECT DISTINCT pvSn FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "含稅的應付帳款總金額。",
        f"SELECT SUM(amount) FROM {tbl} WHERE isTax = 'Y' AND {ISDEL};"
    ))
    samples.append(s(
        "廠商 '宏碁蜂蜜' 的應付帳款總金額。",
        f"SELECT SUM(amount) FROM {tbl} WHERE pvName = N'宏碁蜂蜜' AND {ISDEL};"
    ))
    samples.append(s(
        "各廠商的應付帳款筆數，依筆數由多至少排序。",
        f"SELECT pvName, COUNT(*) FROM {tbl} WHERE {ISDEL} GROUP BY pvName ORDER BY COUNT(*) DESC;"
    ))
    samples.append(s(
        "應付帳款金額最低的一筆記錄。",
        f"SELECT TOP 1 acctOutId, pvName, amount FROM {tbl} WHERE {ISDEL} ORDER BY amount ASC;"
    ))
    samples.append(s(
        "商品 '台東來的小魚' 的應付帳款廠商名稱。",
        f"SELECT DISTINCT pvName FROM {tbl} WHERE pName = N'台東來的小魚' AND {ISDEL};"
    ))
    samples.append(s(
        "2025年12月19日廠商 '優豆食品' 的應付帳款金額。",
        f"SELECT amount FROM {tbl} WHERE LEFT(acctOutId, 8) = '20251219' AND pvName = N'優豆食品' AND {ISDEL};"
    ))
    samples.append(s(
        "應付帳款中每筆記錄的商品數量是否含稅。",
        f"SELECT acctOutId, pName, qty, isTax FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "列出 acctOutId、廠商名稱、金額，依日期排序。",
        f"SELECT acctOutId, pvName, amount FROM {tbl} WHERE {ISDEL} ORDER BY acctOutId;"
    ))
    samples.append(s(
        "廠商序號 143 的應付帳款是否含稅？",
        f"SELECT TOP 1 isTax FROM {tbl} WHERE pvSn = 143 AND {ISDEL};"
    ))
    samples.append(s(
        "不含稅的應付帳款總金額。",
        f"SELECT SUM(amount) FROM {tbl} WHERE isTax = 'N' AND {ISDEL};"
    ))
    samples.append(s(
        "廠商 '新福華商行' 的應付帳款商品名稱有哪些？",
        f"SELECT DISTINCT pName FROM {tbl} WHERE pvName = N'新福華商行' AND {ISDEL};"
    ))
    samples.append(s(
        "各日期的應付帳款付款筆數。",
        f"SELECT LEFT(acctOutId, 8), COUNT(*) FROM {tbl} WHERE {ISDEL} GROUP BY LEFT(acctOutId, 8);"
    ))
    samples.append(s(
        "數量超過 5 的應付帳款記錄廠商名稱與金額。",
        f"SELECT pvName, amount FROM {tbl} WHERE qty > 5 AND {ISDEL};"
    ))

    return samples


# ===========================================================================
# WP_vOutStock  (銷貨出庫)  — has isDel, dtlIsDel
# ===========================================================================

def gen_out_stock_train():
    tbl = "WP_M09.dbo.WP_vOutStock"
    samples = []
    def s(q, sql): return make_sample(q, sql)

    # 基本查詢
    samples.append(s(
        "銷貨出庫單 '202510230009' 的金額是多少？",
        f"SELECT TOP 1 amount FROM {tbl} WHERE OutStkId = '202510230009' AND {ISDEL};"
    ))
    samples.append(s(
        "查詢出庫單 '202510230022' 的出庫金額。",
        f"SELECT TOP 1 amount FROM {tbl} WHERE OutStkId = '202510230022' AND {ISDEL};"
    ))
    samples.append(s(
        "出庫單 '202510240006' 的金額。",
        f"SELECT TOP 1 amount FROM {tbl} WHERE OutStkId = '202510240006' AND {ISDEL};"
    ))

    # memName
    samples.append(s(
        "查詢會員 '麻竹園' 的所有銷貨出庫記錄。",
        f"SELECT OutStkId, amount FROM {tbl} WHERE memName = N'麻竹園' AND {ISDEL};"
    ))
    samples.append(s(
        "非會員的銷貨出庫記錄有哪些？",
        f"SELECT OutStkId, amount FROM {tbl} WHERE memName = N'非會員' AND {ISDEL};"
    ))
    samples.append(s(
        "一般會員的銷貨出庫總金額。",
        f"SELECT SUM(amount) FROM {tbl} WHERE memName = N'會員' AND {ISDEL};"
    ))

    # empName
    samples.append(s(
        "鳳凰分部的銷貨出庫記錄。",
        f"SELECT OutStkId, amount FROM {tbl} WHERE empName = N'鳳凰分部' AND {ISDEL};"
    ))
    samples.append(s(
        "特產中心的銷貨出庫記錄有幾筆？",
        f"SELECT COUNT(*) FROM {tbl} WHERE empName = N'特產中心' AND {ISDEL};"
    ))
    samples.append(s(
        "鳳凰分部的銷貨出庫總金額。",
        f"SELECT SUM(amount) FROM {tbl} WHERE empName = N'鳳凰分部' AND {ISDEL};"
    ))

    # COUNT
    samples.append(s(
        "銷貨出庫共有幾筆有效記錄？",
        f"SELECT COUNT(*) FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "2025年10月23日有幾筆銷貨出庫？",
        f"SELECT COUNT(*) FROM {tbl} WHERE LEFT(OutStkId, 8) = '20251023' AND {ISDEL};"
    ))

    # SUM
    samples.append(s(
        "2025年10月23日的銷貨出庫總金額。",
        f"SELECT SUM(amount) FROM {tbl} WHERE LEFT(OutStkId, 8) = '20251023' AND {ISDEL};"
    ))
    samples.append(s(
        "2025年10月24日的銷貨出庫金額合計。",
        f"SELECT SUM(amount) FROM {tbl} WHERE LEFT(OutStkId, 8) = '20251024' AND {ISDEL};"
    ))
    samples.append(s(
        "2025年10月份的銷貨出庫總金額。",
        f"SELECT SUM(amount) FROM {tbl} WHERE LEFT(OutStkId, 6) = '202510' AND {ISDEL};"
    ))

    # outType
    samples.append(s(
        "查詢結帳狀態為未結（outType='0'）的出庫記錄。",
        f"SELECT OutStkId, amount FROM {tbl} WHERE outType = '0' AND {ISDEL};"
    ))
    samples.append(s(
        "未結完（outType='1'）的出庫記錄有幾筆？",
        f"SELECT COUNT(*) FROM {tbl} WHERE outType = '1' AND {ISDEL};"
    ))
    samples.append(s(
        "全結（outType='2'）的銷貨出庫總金額。",
        f"SELECT SUM(amount) FROM {tbl} WHERE outType = '2' AND {ISDEL};"
    ))
    samples.append(s(
        "各結帳狀態的出庫筆數。",
        f"SELECT outType, COUNT(*) FROM {tbl} WHERE {ISDEL} GROUP BY outType;"
    ))

    # pName
    samples.append(s(
        "商品 '毛寶椰子洗碗精800補充包' 的出庫記錄。",
        f"SELECT OutStkId, qty, amount FROM {tbl} WHERE pName = N'毛寶椰子洗碗精800補充包' AND {ISDEL};"
    ))
    samples.append(s(
        "查詢 '魔術靈廚房500ml(噴)' 的銷貨出庫數量與金額。",
        f"SELECT qty, amount FROM {tbl} WHERE pName = N'魔術靈廚房500ml(噴)' AND {ISDEL};"
    ))
    samples.append(s(
        "'春風抽衛110抽10包7串' 的出庫記錄。",
        f"SELECT OutStkId, qty, amount FROM {tbl} WHERE pName = N'春風抽衛110抽10包7串' AND {ISDEL};"
    ))
    samples.append(s(
        "商品 '玄米葵瓜子' 的銷貨出庫金額。",
        f"SELECT amount FROM {tbl} WHERE pName = N'玄米葵瓜子' AND {ISDEL};"
    ))
    samples.append(s(
        "'香菇脆片-原味' 的出庫數量。",
        f"SELECT qty FROM {tbl} WHERE pName = N'香菇脆片-原味' AND {ISDEL};"
    ))
    samples.append(s(
        "'牛蒡香鬆海苔' 的銷貨出庫記錄。",
        f"SELECT OutStkId, amount FROM {tbl} WHERE pName = N'牛蒡香鬆海苔' AND {ISDEL};"
    ))
    samples.append(s(
        "查詢 '香菇脆片-芥末' 的出庫金額。",
        f"SELECT amount FROM {tbl} WHERE pName = N'香菇脆片-芥末' AND {ISDEL};"
    ))

    # ORDER BY
    samples.append(s(
        "依出庫金額由高至低列出所有銷貨出庫記錄。",
        f"SELECT OutStkId, pName, amount FROM {tbl} WHERE {ISDEL} ORDER BY amount DESC;"
    ))
    samples.append(s(
        "依出庫單號排序列出所有有效出庫記錄。",
        f"SELECT OutStkId, memName, amount FROM {tbl} WHERE {ISDEL} ORDER BY OutStkId;"
    ))

    # GROUP BY
    samples.append(s(
        "依會員名稱統計銷貨出庫筆數與總金額。",
        f"SELECT memName, COUNT(*), SUM(amount) FROM {tbl} WHERE {ISDEL} GROUP BY memName;"
    ))
    samples.append(s(
        "各員工（部門）的銷貨出庫總金額。",
        f"SELECT empName, SUM(amount) FROM {tbl} WHERE {ISDEL} GROUP BY empName;"
    ))
    samples.append(s(
        "各商品的銷貨出庫次數。",
        f"SELECT pName, COUNT(*) FROM {tbl} WHERE {ISDEL} GROUP BY pName;"
    ))

    # amount range
    samples.append(s(
        "出庫金額超過 500 的記錄。",
        f"SELECT OutStkId, pName, amount FROM {tbl} WHERE amount > 500 AND {ISDEL};"
    ))
    samples.append(s(
        "出庫金額在 200 到 300 之間的記錄。",
        f"SELECT OutStkId, pName, amount FROM {tbl} WHERE amount >= 200 AND amount <= 300 AND {ISDEL};"
    ))
    samples.append(s(
        "出庫金額最高的一筆。",
        f"SELECT TOP 1 OutStkId, pName, amount FROM {tbl} WHERE {ISDEL} ORDER BY amount DESC;"
    ))

    # DISTINCT
    samples.append(s(
        "銷貨出庫中涉及哪些不重複的商品？",
        f"SELECT DISTINCT pName FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "銷貨出庫中涉及哪些不重複的會員？",
        f"SELECT DISTINCT memName FROM {tbl} WHERE {ISDEL};"
    ))

    # qty
    samples.append(s(
        "出庫數量等於 1 的記錄有幾筆？",
        f"SELECT COUNT(*) FROM {tbl} WHERE qty = 1 AND {ISDEL};"
    ))
    samples.append(s(
        "查詢出庫數量大於 1 的銷貨記錄。",
        f"SELECT OutStkId, pName, qty FROM {tbl} WHERE qty > 1 AND {ISDEL};"
    ))

    # multi-condition
    samples.append(s(
        "鳳凰分部在 2025年10月23日的銷貨出庫記錄。",
        f"SELECT OutStkId, pName, amount FROM {tbl} WHERE empName = N'鳳凰分部' AND LEFT(OutStkId, 8) = '20251023' AND {ISDEL};"
    ))
    samples.append(s(
        "麻竹園的未結出庫記錄。",
        f"SELECT OutStkId, amount FROM {tbl} WHERE memName = N'麻竹園' AND outType = '0' AND {ISDEL};"
    ))

    # 全欄
    samples.append(s(
        "列出所有有效銷貨出庫的出庫單號、會員名稱及金額。",
        f"SELECT OutStkId, memName, amount FROM {tbl} WHERE {ISDEL};"
    ))

    # extra to reach ~50
    samples.append(s(
        "依會員名稱統計各會員的出庫總金額，依金額由高至低排序。",
        f"SELECT memName, SUM(amount) FROM {tbl} WHERE {ISDEL} GROUP BY memName ORDER BY SUM(amount) DESC;"
    ))
    samples.append(s(
        "2025年10月份各員工的出庫筆數。",
        f"SELECT empName, COUNT(*) FROM {tbl} WHERE LEFT(OutStkId, 6) = '202510' AND {ISDEL} GROUP BY empName;"
    ))
    samples.append(s(
        "特產中心的出庫商品種數。",
        f"SELECT COUNT(DISTINCT pName) FROM {tbl} WHERE empName = N'特產中心' AND {ISDEL};"
    ))
    samples.append(s(
        "查詢金額為 200 的銷貨出庫記錄。",
        f"SELECT OutStkId, pName, memName FROM {tbl} WHERE amount = 200.0 AND {ISDEL};"
    ))
    samples.append(s(
        "未結（outType='0'）且出庫金額超過 200 的記錄。",
        f"SELECT OutStkId, pName, amount FROM {tbl} WHERE outType = '0' AND amount > 200 AND {ISDEL};"
    ))
    samples.append(s(
        "依出庫日期統計各日期的出庫筆數。",
        f"SELECT LEFT(OutStkId, 8), COUNT(*) FROM {tbl} WHERE {ISDEL} GROUP BY LEFT(OutStkId, 8);"
    ))
    samples.append(s(
        "銷貨出庫中不重複的出庫日期有幾個？",
        f"SELECT COUNT(DISTINCT LEFT(OutStkId, 8)) FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "鳳凰分部的出庫商品種數。",
        f"SELECT COUNT(DISTINCT pName) FROM {tbl} WHERE empName = N'鳳凰分部' AND {ISDEL};"
    ))
    samples.append(s(
        "2025年10月份的銷貨出庫筆數。",
        f"SELECT COUNT(*) FROM {tbl} WHERE LEFT(OutStkId, 6) = '202510' AND {ISDEL};"
    ))
    samples.append(s(
        "出庫金額最高的前3筆銷貨記錄。",
        f"SELECT TOP 3 OutStkId, pName, amount FROM {tbl} WHERE {ISDEL} ORDER BY amount DESC;"
    ))

    return samples


def gen_out_stock_test():
    tbl = "WP_M09.dbo.WP_vOutStock"
    samples = []
    def s(q, sql): return make_sample(q, sql)

    samples.append(s(
        "銷貨出庫共有幾筆有效記錄？",
        f"SELECT COUNT(*) FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "全結（outType='2'）的出庫記錄有幾筆？",
        f"SELECT COUNT(*) FROM {tbl} WHERE outType = '2' AND {ISDEL};"
    ))
    samples.append(s(
        "各結帳狀態的出庫總金額。",
        f"SELECT outType, SUM(amount) FROM {tbl} WHERE {ISDEL} GROUP BY outType;"
    ))
    samples.append(s(
        "特產中心 2025年10月23日的銷貨出庫總金額。",
        f"SELECT SUM(amount) FROM {tbl} WHERE empName = N'特產中心' AND LEFT(OutStkId, 8) = '20251023' AND {ISDEL};"
    ))
    samples.append(s(
        "各員工部門的出庫筆數，依筆數由多至少排序。",
        f"SELECT empName, COUNT(*) FROM {tbl} WHERE {ISDEL} GROUP BY empName ORDER BY COUNT(*) DESC;"
    ))
    samples.append(s(
        "出庫金額最低的一筆銷貨記錄。",
        f"SELECT TOP 1 OutStkId, pName, amount FROM {tbl} WHERE {ISDEL} ORDER BY amount ASC;"
    ))
    samples.append(s(
        "商品 '香菇脆片-芥末' 的出庫記錄有幾筆？",
        f"SELECT COUNT(*) FROM {tbl} WHERE pName = N'香菇脆片-芥末' AND {ISDEL};"
    ))
    samples.append(s(
        "非會員在各結帳狀態的出庫筆數。",
        f"SELECT outType, COUNT(*) FROM {tbl} WHERE memName = N'非會員' AND {ISDEL} GROUP BY outType;"
    ))
    samples.append(s(
        "2025年10月份各商品的出庫次數。",
        f"SELECT pName, COUNT(*) FROM {tbl} WHERE LEFT(OutStkId, 6) = '202510' AND {ISDEL} GROUP BY pName;"
    ))
    samples.append(s(
        "出庫金額為 826 的銷貨出庫記錄。",
        f"SELECT OutStkId, pName, memName FROM {tbl} WHERE amount = 826.0 AND {ISDEL};"
    ))
    samples.append(s(
        "出庫金額為 265 的銷貨出庫記錄。",
        f"SELECT OutStkId, pName, memName FROM {tbl} WHERE amount = 265.0 AND {ISDEL};"
    ))
    samples.append(s(
        "列出不重複的出庫員工（部門）名稱。",
        f"SELECT DISTINCT empName FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "2025年10月24日的銷貨出庫記錄有幾筆？",
        f"SELECT COUNT(*) FROM {tbl} WHERE LEFT(OutStkId, 8) = '20251024' AND {ISDEL};"
    ))
    samples.append(s(
        "依商品名稱統計銷貨出庫總金額，取前5名。",
        f"SELECT TOP 5 pName, SUM(amount) FROM {tbl} WHERE {ISDEL} GROUP BY pName ORDER BY SUM(amount) DESC;"
    ))
    samples.append(s(
        "麻竹園的銷貨出庫總金額。",
        f"SELECT SUM(amount) FROM {tbl} WHERE memName = N'麻竹園' AND {ISDEL};"
    ))

    return samples


# ===========================================================================
# WP_vTransfer  (調撥)  — has isDel, dtlIsDel
# ===========================================================================

def gen_transfer_train():
    tbl = "WP_M09.dbo.WP_vTransfer"
    samples = []
    def s(q, sql): return make_sample(q, sql)

    # 基本查詢
    samples.append(s(
        "調撥單 '202510270001' 的調撥數量。",
        f"SELECT TOP 1 qty FROM {tbl} WHERE TransferId = '202510270001' AND {ISDEL};"
    ))
    samples.append(s(
        "調撥單 '202510270003' 的調撥記錄。",
        f"SELECT * FROM {tbl} WHERE TransferId = '202510270003' AND {ISDEL};"
    ))
    samples.append(s(
        "查詢調撥單 '202510270006' 的商品與數量。",
        f"SELECT pName, qty FROM {tbl} WHERE TransferId = '202510270006' AND {ISDEL};"
    ))

    # fWhName / tfWhName
    samples.append(s(
        "從特產中心調撥出去的所有記錄。",
        f"SELECT TransferId, tfWhName, pName, qty FROM {tbl} WHERE fWhName = N'特產中心' AND {ISDEL};"
    ))
    samples.append(s(
        "調撥至初鄉分部的記錄有哪些？",
        f"SELECT TransferId, pName, qty FROM {tbl} WHERE tfWhName = N'初鄉分部' AND {ISDEL};"
    ))
    samples.append(s(
        "調撥至永隆分部的調撥記錄。",
        f"SELECT TransferId, pName, qty FROM {tbl} WHERE tfWhName = N'永隆分部' AND {ISDEL};"
    ))
    samples.append(s(
        "坪頂分部收到哪些調撥商品？",
        f"SELECT pName, qty FROM {tbl} WHERE tfWhName = N'坪頂分部' AND {ISDEL};"
    ))
    samples.append(s(
        "鳳凰分部的調撥記錄。",
        f"SELECT TransferId, pName, qty FROM {tbl} WHERE tfWhName = N'鳳凰分部' AND {ISDEL};"
    ))
    samples.append(s(
        "廣興分部收到哪些商品及數量？",
        f"SELECT pName, qty FROM {tbl} WHERE tfWhName = N'廣興分部' AND {ISDEL};"
    ))

    # COUNT
    samples.append(s(
        "調撥記錄共有幾筆有效記錄？",
        f"SELECT COUNT(*) FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "2025年10月27日有幾筆調撥記錄？",
        f"SELECT COUNT(*) FROM {tbl} WHERE LEFT(TransferId, 8) = '20251027' AND {ISDEL};"
    ))
    samples.append(s(
        "調撥至初鄉分部的記錄有幾筆？",
        f"SELECT COUNT(*) FROM {tbl} WHERE tfWhName = N'初鄉分部' AND {ISDEL};"
    ))

    # SUM qty
    samples.append(s(
        "2025年10月27日的調撥總數量。",
        f"SELECT SUM(qty) FROM {tbl} WHERE LEFT(TransferId, 8) = '20251027' AND {ISDEL};"
    ))
    samples.append(s(
        "調撥至鳳凰分部的商品總數量。",
        f"SELECT SUM(qty) FROM {tbl} WHERE tfWhName = N'鳳凰分部' AND {ISDEL};"
    ))
    samples.append(s(
        "廣興分部收到的調撥商品總數量。",
        f"SELECT SUM(qty) FROM {tbl} WHERE tfWhName = N'廣興分部' AND {ISDEL};"
    ))

    # pName
    samples.append(s(
        "商品 '7kg白米' 的調撥記錄。",
        f"SELECT TransferId, tfWhName, qty FROM {tbl} WHERE pName = N'7kg白米' AND {ISDEL};"
    ))
    samples.append(s(
        "查詢 '濁水米14kg' 的調撥數量。",
        f"SELECT qty FROM {tbl} WHERE pName = N'濁水米14kg' AND {ISDEL};"
    ))
    samples.append(s(
        "'舒跑鋁箔（箱）' 的調撥記錄有幾筆？",
        f"SELECT COUNT(*) FROM {tbl} WHERE pName = N'舒跑鋁箔（箱）' AND {ISDEL};"
    ))
    samples.append(s(
        "查詢 '春風抽衛110抽10包7串' 的調撥目的倉庫。",
        f"SELECT DISTINCT tfWhName FROM {tbl} WHERE pName = N'春風抽衛110抽10包7串' AND {ISDEL};"
    ))
    samples.append(s(
        "'超吸收廚房紙巾6*8' 的調撥數量。",
        f"SELECT qty FROM {tbl} WHERE pName = N'超吸收廚房紙巾6*8' AND {ISDEL};"
    ))
    samples.append(s(
        "'春風1秒抽廚房紙巾' 的調撥記錄。",
        f"SELECT TransferId, tfWhName, qty FROM {tbl} WHERE pName = N'春風1秒抽廚房紙巾' AND {ISDEL};"
    ))

    # costAvg
    samples.append(s(
        "查詢 '7kg白米' 的調撥平均成本。",
        f"SELECT TOP 1 costAvg FROM {tbl} WHERE pName = N'7kg白米' AND {ISDEL};"
    ))
    samples.append(s(
        "'濁水米14kg' 的調撥平均成本是多少？",
        f"SELECT TOP 1 costAvg FROM {tbl} WHERE pName = N'濁水米14kg' AND {ISDEL};"
    ))
    samples.append(s(
        "平均成本超過 300 的調撥商品。",
        f"SELECT DISTINCT pName, costAvg FROM {tbl} WHERE costAvg > 300 AND {ISDEL};"
    ))
    samples.append(s(
        "調撥平均成本在 80 到 100 之間的商品。",
        f"SELECT DISTINCT pName, costAvg FROM {tbl} WHERE costAvg >= 80 AND costAvg <= 100 AND {ISDEL};"
    ))

    # ORDER BY
    samples.append(s(
        "依調撥數量由多至少列出所有調撥記錄。",
        f"SELECT TransferId, pName, qty FROM {tbl} WHERE {ISDEL} ORDER BY qty DESC;"
    ))
    samples.append(s(
        "依調撥單號排序列出所有有效調撥記錄。",
        f"SELECT TransferId, tfWhName, pName, qty FROM {tbl} WHERE {ISDEL} ORDER BY TransferId;"
    ))

    # GROUP BY
    samples.append(s(
        "依目的倉庫統計調撥筆數。",
        f"SELECT tfWhName, COUNT(*) FROM {tbl} WHERE {ISDEL} GROUP BY tfWhName;"
    ))
    samples.append(s(
        "各目的倉庫收到的調撥總數量。",
        f"SELECT tfWhName, SUM(qty) FROM {tbl} WHERE {ISDEL} GROUP BY tfWhName;"
    ))
    samples.append(s(
        "各商品的調撥總數量。",
        f"SELECT pName, SUM(qty) FROM {tbl} WHERE {ISDEL} GROUP BY pName;"
    ))
    samples.append(s(
        "依商品統計調撥次數，依次數由多至少排序。",
        f"SELECT pName, COUNT(*) FROM {tbl} WHERE {ISDEL} GROUP BY pName ORDER BY COUNT(*) DESC;"
    ))

    # DISTINCT
    samples.append(s(
        "調撥記錄中涉及哪些不重複的商品？",
        f"SELECT DISTINCT pName FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "調撥記錄涉及哪些目的倉庫？",
        f"SELECT DISTINCT tfWhName FROM {tbl} WHERE {ISDEL};"
    ))

    # qty filter
    samples.append(s(
        "調撥數量超過 100 的記錄。",
        f"SELECT TransferId, pName, tfWhName, qty FROM {tbl} WHERE qty > 100 AND {ISDEL};"
    ))
    samples.append(s(
        "調撥數量等於 4 的記錄。",
        f"SELECT TransferId, pName, tfWhName FROM {tbl} WHERE qty = 4 AND {ISDEL};"
    ))
    samples.append(s(
        "調撥數量最大的一筆記錄。",
        f"SELECT TOP 1 TransferId, pName, tfWhName, qty FROM {tbl} WHERE {ISDEL} ORDER BY qty DESC;"
    ))

    # multi-condition
    samples.append(s(
        "特產中心調撥至初鄉分部的記錄。",
        f"SELECT TransferId, pName, qty FROM {tbl} WHERE fWhName = N'特產中心' AND tfWhName = N'初鄉分部' AND {ISDEL};"
    ))
    samples.append(s(
        "特產中心調撥至廣興分部的商品名稱與數量。",
        f"SELECT pName, qty FROM {tbl} WHERE fWhName = N'特產中心' AND tfWhName = N'廣興分部' AND {ISDEL};"
    ))

    # 全欄
    samples.append(s(
        "列出所有有效調撥記錄的編號、來源、目的及商品。",
        f"SELECT TransferId, fWhName, tfWhName, pName, qty FROM {tbl} WHERE {ISDEL};"
    ))

    # costAvg total
    samples.append(s(
        "計算所有調撥記錄的數量乘以平均成本的總和。",
        f"SELECT SUM(qty * costAvg) FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "調撥記錄中涉及幾種不重複的商品？",
        f"SELECT COUNT(DISTINCT pName) FROM {tbl} WHERE {ISDEL};"
    ))

    # extra to reach ~50
    samples.append(s(
        "調撥至坪頂分部的調撥總數量。",
        f"SELECT SUM(qty) FROM {tbl} WHERE tfWhName = N'坪頂分部' AND {ISDEL};"
    ))
    samples.append(s(
        "特產中心調撥至永隆分部的記錄。",
        f"SELECT pName, qty FROM {tbl} WHERE fWhName = N'特產中心' AND tfWhName = N'永隆分部' AND {ISDEL};"
    ))
    samples.append(s(
        "調撥數量等於 20 的商品名稱與目的倉庫。",
        f"SELECT pName, tfWhName FROM {tbl} WHERE qty = 20 AND {ISDEL};"
    ))
    samples.append(s(
        "平均成本為 85 的商品調撥至哪些倉庫？",
        f"SELECT DISTINCT tfWhName FROM {tbl} WHERE costAvg = 85.0 AND {ISDEL};"
    ))
    samples.append(s(
        "'舒跑鋁箔（箱）' 的調撥總數量。",
        f"SELECT SUM(qty) FROM {tbl} WHERE pName = N'舒跑鋁箔（箱）' AND {ISDEL};"
    ))
    samples.append(s(
        "調撥記錄的不重複來源倉庫有幾個？",
        f"SELECT COUNT(DISTINCT fWhName) FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "特產中心調撥至鳳凰分部的商品名稱及數量。",
        f"SELECT pName, qty FROM {tbl} WHERE fWhName = N'特產中心' AND tfWhName = N'鳳凰分部' AND {ISDEL};"
    ))
    samples.append(s(
        "調撥日期為 2025年10月27日的所有記錄。",
        f"SELECT TransferId, tfWhName, pName, qty FROM {tbl} WHERE LEFT(TransferId, 8) = '20251027' AND {ISDEL};"
    ))
    samples.append(s(
        "平均成本為 130 的商品調撥記錄。",
        f"SELECT pName, tfWhName, qty FROM {tbl} WHERE costAvg = 130.0 AND {ISDEL};"
    ))

    return samples


def gen_transfer_test():
    tbl = "WP_M09.dbo.WP_vTransfer"
    samples = []
    def s(q, sql): return make_sample(q, sql)

    samples.append(s(
        "調撥記錄共有幾筆有效記錄？",
        f"SELECT COUNT(*) FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "各目的倉庫收到的調撥總數量，依數量由多至少排序。",
        f"SELECT tfWhName, SUM(qty) FROM {tbl} WHERE {ISDEL} GROUP BY tfWhName ORDER BY SUM(qty) DESC;"
    ))
    samples.append(s(
        "調撥數量最多的商品是哪一個？",
        f"SELECT TOP 1 pName, SUM(qty) FROM {tbl} WHERE {ISDEL} GROUP BY pName ORDER BY SUM(qty) DESC;"
    ))
    samples.append(s(
        "2025年10月27日調撥至鳳凰分部的商品。",
        f"SELECT pName, qty FROM {tbl} WHERE LEFT(TransferId, 8) = '20251027' AND tfWhName = N'鳳凰分部' AND {ISDEL};"
    ))
    samples.append(s(
        "平均成本最高的調撥商品。",
        f"SELECT TOP 1 pName, costAvg FROM {tbl} WHERE {ISDEL} ORDER BY costAvg DESC;"
    ))
    samples.append(s(
        "調撥記錄中數量等於 8 的商品與目的倉庫。",
        f"SELECT pName, tfWhName FROM {tbl} WHERE qty = 8 AND {ISDEL};"
    ))
    samples.append(s(
        "'春風1秒抽廚房紙巾' 的調撥目的倉庫有哪些？",
        f"SELECT DISTINCT tfWhName FROM {tbl} WHERE pName = N'春風1秒抽廚房紙巾' AND {ISDEL};"
    ))
    samples.append(s(
        "廣興分部收到的調撥商品名稱及數量，依數量排序。",
        f"SELECT pName, qty FROM {tbl} WHERE tfWhName = N'廣興分部' AND {ISDEL} ORDER BY qty DESC;"
    ))
    samples.append(s(
        "特產中心調撥至坪頂分部的記錄有幾筆？",
        f"SELECT COUNT(*) FROM {tbl} WHERE fWhName = N'特產中心' AND tfWhName = N'坪頂分部' AND {ISDEL};"
    ))
    samples.append(s(
        "平均成本為 330 的商品調撥記錄。",
        f"SELECT TransferId, pName, tfWhName, qty FROM {tbl} WHERE costAvg = 330.0 AND {ISDEL};"
    ))
    samples.append(s(
        "調撥數量為 140 的記錄是什麼商品？",
        f"SELECT pName, tfWhName FROM {tbl} WHERE qty = 140 AND {ISDEL};"
    ))
    samples.append(s(
        "初鄉分部收到的調撥商品及數量。",
        f"SELECT pName, qty FROM {tbl} WHERE tfWhName = N'初鄉分部' AND {ISDEL};"
    ))
    samples.append(s(
        "永隆分部收到的調撥總數量。",
        f"SELECT SUM(qty) FROM {tbl} WHERE tfWhName = N'永隆分部' AND {ISDEL};"
    ))
    samples.append(s(
        "各商品的調撥平均成本（不重複）。",
        f"SELECT DISTINCT pName, costAvg FROM {tbl} WHERE {ISDEL};"
    ))
    samples.append(s(
        "調撥數量小於 10 的記錄有幾筆？",
        f"SELECT COUNT(*) FROM {tbl} WHERE qty < 10 AND {ISDEL};"
    ))

    return samples


# ===========================================================================
# WP_vInventory  (庫存)  — NO isDel, NO dtlIsDel
# ===========================================================================

def gen_inventory_train():
    tbl = "WP_M09.dbo.WP_vInventory"
    samples = []
    def s(q, sql): return make_sample(q, sql)

    # 基本查詢
    samples.append(s(
        "查詢商品 '悅氏小水' 的庫存數量。",
        f"SELECT qty FROM {tbl} WHERE pName = N'悅氏小水';"
    ))
    samples.append(s(
        "'南瓜酥' 目前的庫存數量是多少？",
        f"SELECT qtyNow FROM {tbl} WHERE pName = N'南瓜酥';"
    ))
    samples.append(s(
        "查詢 '冰棒' 的庫存資訊。",
        f"SELECT pName, qty, qtyNow, qtySafe FROM {tbl} WHERE pName = N'冰棒';"
    ))
    samples.append(s(
        "'蔭瓜' 的安全庫存量是多少？",
        f"SELECT qtySafe FROM {tbl} WHERE pName = N'蔭瓜';"
    ))
    samples.append(s(
        "查詢 '醬筍' 的庫存數量與安全庫存。",
        f"SELECT qty, qtySafe FROM {tbl} WHERE pName = N'醬筍';"
    ))
    samples.append(s(
        "'剝皮辣椒' 的庫存資料。",
        f"SELECT pName, qty, qtyNow, qtySafe FROM {tbl} WHERE pName = N'剝皮辣椒';"
    ))
    samples.append(s(
        "查詢 '黑芝麻酥罐裝' 的庫存數量。",
        f"SELECT qty FROM {tbl} WHERE pName = N'黑芝麻酥罐裝';"
    ))
    samples.append(s(
        "'南瓜酥罐裝' 的目前庫存量。",
        f"SELECT qtyNow FROM {tbl} WHERE pName = N'南瓜酥罐裝';"
    ))
    samples.append(s(
        "查詢 '珍珠醬' 的庫存資訊。",
        f"SELECT pName, qty, qtyNow, qtySafe FROM {tbl} WHERE pName = N'珍珠醬';"
    ))
    samples.append(s(
        "'柴魚花' 的庫存數量是多少？",
        f"SELECT qty FROM {tbl} WHERE pName = N'柴魚花';"
    ))

    # 庫存低於安全庫存
    samples.append(s(
        "哪些商品的庫存數量低於安全庫存？",
        f"SELECT pName, qty, qtySafe FROM {tbl} WHERE qty < qtySafe;"
    ))
    samples.append(s(
        "目前庫存低於安全庫存的商品名稱。",
        f"SELECT pName FROM {tbl} WHERE qtyNow < qtySafe;"
    ))
    samples.append(s(
        "庫存數量等於安全庫存的商品。",
        f"SELECT pName, qty, qtySafe FROM {tbl} WHERE qty = qtySafe;"
    ))

    # 依廠商查詢
    samples.append(s(
        "廠商 '實垣有限公司' 的庫存商品。",
        f"SELECT pName, qty FROM {tbl} WHERE pvName = N'實垣有限公司';"
    ))
    samples.append(s(
        "'新福華商行' 供應的庫存商品有哪些？",
        f"SELECT pName, qty FROM {tbl} WHERE pvName = N'新福華商行';"
    ))
    samples.append(s(
        "廠商 '碧雲冰城' 的商品庫存資訊。",
        f"SELECT pName, qty, qtyNow FROM {tbl} WHERE pvName = N'碧雲冰城';"
    ))
    samples.append(s(
        "'龍宏醬業' 的庫存商品。",
        f"SELECT pName, qty FROM {tbl} WHERE pvName = N'龍宏醬業';"
    ))
    samples.append(s(
        "廠商 '百寶食品' 的商品庫存數量。",
        f"SELECT pName, qty FROM {tbl} WHERE pvName = N'百寶食品';"
    ))
    samples.append(s(
        "'台東縣農會' 的庫存商品名稱與數量。",
        f"SELECT pName, qty FROM {tbl} WHERE pvName = N'台東縣農會';"
    ))

    # 條碼查詢
    samples.append(s(
        "條碼 '4710632001318' 的商品庫存資訊。",
        f"SELECT pName, qty, qtyNow FROM {tbl} WHERE pBarcode = '4710632001318';"
    ))
    samples.append(s(
        "條碼 '4716133178320' 的商品名稱及庫存數量。",
        f"SELECT pName, qty FROM {tbl} WHERE pBarcode = '4716133178320';"
    ))
    samples.append(s(
        "條碼 '00950015' 對應哪個商品？",
        f"SELECT pName FROM {tbl} WHERE pBarcode = '00950015';"
    ))

    # COUNT
    samples.append(s(
        "庫存中共有幾種商品？",
        f"SELECT COUNT(*) FROM {tbl};"
    ))
    samples.append(s(
        "廠商 '新福華商行' 有幾種庫存商品？",
        f"SELECT COUNT(*) FROM {tbl} WHERE pvName = N'新福華商行';"
    ))

    # priceStd
    samples.append(s(
        "查詢 '南瓜酥' 的標準售價。",
        f"SELECT priceStd FROM {tbl} WHERE pName = N'南瓜酥';"
    ))
    samples.append(s(
        "標準售價超過 130 的庫存商品。",
        f"SELECT pName, priceStd FROM {tbl} WHERE priceStd > 130;"
    ))
    samples.append(s(
        "標準售價等於 15 的商品有哪些？",
        f"SELECT pName FROM {tbl} WHERE priceStd = 15;"
    ))

    # qty range
    samples.append(s(
        "庫存數量超過 100 的商品。",
        f"SELECT pName, qty FROM {tbl} WHERE qty > 100;"
    ))
    samples.append(s(
        "庫存數量低於 20 的商品。",
        f"SELECT pName, qty FROM {tbl} WHERE qty < 20;"
    ))
    samples.append(s(
        "庫存數量在 10 到 50 之間的商品。",
        f"SELECT pName, qty FROM {tbl} WHERE qty >= 10 AND qty <= 50;"
    ))

    # ORDER BY
    samples.append(s(
        "依庫存數量由多至少列出所有商品。",
        f"SELECT pName, qty FROM {tbl} ORDER BY qty DESC;"
    ))
    samples.append(s(
        "依標準售價由低至高列出庫存商品。",
        f"SELECT pName, priceStd FROM {tbl} ORDER BY priceStd ASC;"
    ))

    # GROUP BY
    samples.append(s(
        "依廠商統計庫存商品種數。",
        f"SELECT pvName, COUNT(*) FROM {tbl} GROUP BY pvName;"
    ))
    samples.append(s(
        "各廠商的庫存總數量。",
        f"SELECT pvName, SUM(qty) FROM {tbl} GROUP BY pvName;"
    ))

    # 倉庫
    samples.append(s(
        "倉庫 '特產中心' 的所有庫存商品。",
        f"SELECT pName, qty FROM {tbl} WHERE WarehouseName = N'特產中心';"
    ))
    samples.append(s(
        "倉庫編號 '001' 的庫存商品種數。",
        f"SELECT COUNT(*) FROM {tbl} WHERE WarehouseId = '001';"
    ))

    # pNo LIKE
    samples.append(s(
        "查詢商品編號以 'F' 開頭的庫存記錄。",
        f"SELECT pName, qty FROM {tbl} WHERE pNo LIKE 'F%';"
    ))

    # SUM
    samples.append(s(
        "所有庫存商品的數量總和。",
        f"SELECT SUM(qty) FROM {tbl};"
    ))
    samples.append(s(
        "目前庫存（qtyNow）超過安全庫存（qtySafe）5倍的商品。",
        f"SELECT pName, qtyNow, qtySafe FROM {tbl} WHERE qtyNow > qtySafe * 5;"
    ))

    # DISTINCT
    samples.append(s(
        "庫存中涉及哪些不重複的廠商？",
        f"SELECT DISTINCT pvName FROM {tbl};"
    ))

    # extra to reach ~50
    samples.append(s(
        "庫存數量最多的前3種商品。",
        f"SELECT TOP 3 pName, qty FROM {tbl} ORDER BY qty DESC;"
    ))
    samples.append(s(
        "條碼 '4717044600030' 的商品名稱與廠商。",
        f"SELECT pName, pvName FROM {tbl} WHERE pBarcode = '4717044600030';"
    ))
    samples.append(s(
        "安全庫存為 5 的庫存商品清單。",
        f"SELECT pName, qty, qtySafe FROM {tbl} WHERE qtySafe = 5;"
    ))
    samples.append(s(
        "庫存中目前數量（qtyNow）為 20 的商品。",
        f"SELECT pName FROM {tbl} WHERE qtyNow = 20;"
    ))
    samples.append(s(
        "倉庫 '特產中心' 的庫存總數量。",
        f"SELECT SUM(qty) FROM {tbl} WHERE WarehouseName = N'特產中心';"
    ))
    samples.append(s(
        "廠商 '碧雲冰城' 的商品庫存是否有低於安全庫存的情形？",
        f"SELECT pName, qty, qtySafe FROM {tbl} WHERE pvName = N'碧雲冰城' AND qty < qtySafe;"
    ))
    samples.append(s(
        "目前庫存（qtyNow）為 48 的商品名稱。",
        f"SELECT pName FROM {tbl} WHERE qtyNow = 48;"
    ))
    samples.append(s(
        "庫存中標準售價最高的商品是哪一個？",
        f"SELECT TOP 1 pName, priceStd FROM {tbl} ORDER BY priceStd DESC;"
    ))
    samples.append(s(
        "各廠商庫存商品的平均標準售價。",
        f"SELECT pvName, AVG(priceStd) FROM {tbl} GROUP BY pvName;"
    ))
    samples.append(s(
        "倉庫編號 '001' 中庫存數量超過 50 的商品。",
        f"SELECT pName, qty FROM {tbl} WHERE WarehouseId = '001' AND qty > 50;"
    ))

    return samples


def gen_inventory_test():
    tbl = "WP_M09.dbo.WP_vInventory"
    samples = []
    def s(q, sql): return make_sample(q, sql)

    samples.append(s(
        "庫存中共有幾種商品？",
        f"SELECT COUNT(*) FROM {tbl};"
    ))
    samples.append(s(
        "庫存數量最多的商品是哪一個？",
        f"SELECT TOP 1 pName, qty FROM {tbl} ORDER BY qty DESC;"
    ))
    samples.append(s(
        "哪些商品的目前庫存低於安全庫存？",
        f"SELECT pName, qtyNow, qtySafe FROM {tbl} WHERE qtyNow < qtySafe;"
    ))
    samples.append(s(
        "各廠商的庫存商品數與總庫存量。",
        f"SELECT pvName, COUNT(*), SUM(qty) FROM {tbl} GROUP BY pvName;"
    ))
    samples.append(s(
        "條碼 '4717044700259' 的商品庫存資訊。",
        f"SELECT pName, qty, qtyNow, qtySafe FROM {tbl} WHERE pBarcode = '4717044700259';"
    ))
    samples.append(s(
        "標準售價最高的前3種庫存商品。",
        f"SELECT TOP 3 pName, priceStd FROM {tbl} ORDER BY priceStd DESC;"
    ))
    samples.append(s(
        "安全庫存為 0 的商品有哪些？",
        f"SELECT pName, qty FROM {tbl} WHERE qtySafe = 0;"
    ))
    samples.append(s(
        "廠商 '龍宏醬業' 的商品庫存數量合計。",
        f"SELECT SUM(qty) FROM {tbl} WHERE pvName = N'龍宏醬業';"
    ))
    samples.append(s(
        "'悅氏小水' 的目前庫存是否低於安全庫存？",
        f"SELECT pName, qtyNow, qtySafe FROM {tbl} WHERE pName = N'悅氏小水';"
    ))
    samples.append(s(
        "庫存數量為 268 的商品名稱。",
        f"SELECT pName FROM {tbl} WHERE qty = 268.0;"
    ))
    samples.append(s(
        "條碼 '4717044600030' 對應的商品名稱與庫存。",
        f"SELECT pName, qty FROM {tbl} WHERE pBarcode = '4717044600030';"
    ))
    samples.append(s(
        "依廠商名稱排序列出所有庫存商品。",
        f"SELECT pName, pvName, qty FROM {tbl} ORDER BY pvName;"
    ))
    samples.append(s(
        "標準售價為 140 的庫存商品有哪些？",
        f"SELECT pName, qty FROM {tbl} WHERE priceStd = 140;"
    ))
    samples.append(s(
        "庫存數量最少的前5種商品。",
        f"SELECT TOP 5 pName, qty FROM {tbl} ORDER BY qty ASC;"
    ))
    samples.append(s(
        "廠商 '百寶食品' 的商品庫存是否低於安全庫存？",
        f"SELECT pName, qty, qtySafe FROM {tbl} WHERE pvName = N'百寶食品' AND qty < qtySafe;"
    ))

    return samples


# ===========================================================================
# WP_vProduct  (商品主檔)  — NO isDel, NO dtlIsDel
# ===========================================================================

def gen_product_train():
    tbl = "WP_M09.dbo.WP_vProduct"
    samples = []
    def s(q, sql): return make_sample(q, sql)

    # 基本查詢
    samples.append(s(
        "查詢商品 '悅氏小水' 的標準售價。",
        f"SELECT priceStd FROM {tbl} WHERE pName = N'悅氏小水';"
    ))
    samples.append(s(
        "'南瓜酥' 的最低售價是多少？",
        f"SELECT priceLow FROM {tbl} WHERE pName = N'南瓜酥';"
    ))
    samples.append(s(
        "查詢 '冰棒' 的會員價格。",
        f"SELECT priceMem FROM {tbl} WHERE pName = N'冰棒';"
    ))
    samples.append(s(
        "'蔭瓜' 的批量售價是多少？",
        f"SELECT priceBat FROM {tbl} WHERE pName = N'蔭瓜';"
    ))
    samples.append(s(
        "查詢 '醬筍' 的所有價格資訊。",
        f"SELECT priceStd, priceLow, priceMem, priceBat FROM {tbl} WHERE pName = N'醬筍';"
    ))
    samples.append(s(
        "'剝皮辣椒' 的標準成本是多少？",
        f"SELECT costStd FROM {tbl} WHERE pName = N'剝皮辣椒';"
    ))
    samples.append(s(
        "查詢 '黑芝麻酥罐裝' 的平均成本。",
        f"SELECT costAvg FROM {tbl} WHERE pName = N'黑芝麻酥罐裝';"
    ))
    samples.append(s(
        "'南瓜酥罐裝' 的目前庫存量。",
        f"SELECT qtyNow FROM {tbl} WHERE pName = N'南瓜酥罐裝';"
    ))
    samples.append(s(
        "查詢 '珍珠醬' 的商品資訊。",
        f"SELECT * FROM {tbl} WHERE pName = N'珍珠醬';"
    ))
    samples.append(s(
        "'柴魚花' 的安全庫存與目前庫存。",
        f"SELECT qtySafe, qtyNow FROM {tbl} WHERE pName = N'柴魚花';"
    ))

    # isSale
    samples.append(s(
        "查詢正常銷售（isSale='0'）的商品清單。",
        f"SELECT pName FROM {tbl} WHERE isSale = '0';"
    ))
    samples.append(s(
        "只停止進貨（isSale='1'）的商品有哪些？",
        f"SELECT pName FROM {tbl} WHERE isSale = '1';"
    ))
    samples.append(s(
        "停止銷貨（isSale='2'）的商品清單。",
        f"SELECT pName FROM {tbl} WHERE isSale = '2';"
    ))
    samples.append(s(
        "停止進銷貨（isSale='3'）的商品。",
        f"SELECT pName FROM {tbl} WHERE isSale = '3';"
    ))
    samples.append(s(
        "各銷售狀態的商品數量統計。",
        f"SELECT isSale, COUNT(*) FROM {tbl} GROUP BY isSale;"
    ))

    # 廠商
    samples.append(s(
        "廠商 '新福華商行' 的商品清單。",
        f"SELECT pName, priceStd FROM {tbl} WHERE pvName = N'新福華商行';"
    ))
    samples.append(s(
        "'台東縣農會' 供應哪些商品？",
        f"SELECT pName FROM {tbl} WHERE pvName = N'台東縣農會';"
    ))
    samples.append(s(
        "廠商 '百寶食品' 的商品標準售價。",
        f"SELECT pName, priceStd FROM {tbl} WHERE pvName = N'百寶食品';"
    ))
    samples.append(s(
        "'龍宏醬業' 的商品成本資訊。",
        f"SELECT pName, costStd, costAvg FROM {tbl} WHERE pvName = N'龍宏醬業';"
    ))

    # COUNT
    samples.append(s(
        "商品主檔共有幾筆商品？",
        f"SELECT COUNT(*) FROM {tbl};"
    ))
    samples.append(s(
        "正常銷售的商品共有幾種？",
        f"SELECT COUNT(*) FROM {tbl} WHERE isSale = '0';"
    ))

    # 價格條件
    samples.append(s(
        "標準售價超過 130 的商品。",
        f"SELECT pName, priceStd FROM {tbl} WHERE priceStd > 130;"
    ))
    samples.append(s(
        "標準售價低於 50 的商品清單。",
        f"SELECT pName, priceStd FROM {tbl} WHERE priceStd < 50;"
    ))
    samples.append(s(
        "會員價與標準售價相同的商品。",
        f"SELECT pName, priceStd, priceMem FROM {tbl} WHERE priceMem = priceStd;"
    ))

    # 庫存條件
    samples.append(s(
        "目前庫存低於安全庫存的商品。",
        f"SELECT pName, qtyNow, qtySafe FROM {tbl} WHERE qtyNow < qtySafe;"
    ))
    samples.append(s(
        "庫存為零的商品清單。",
        f"SELECT pName FROM {tbl} WHERE qtyNow = 0;"
    ))

    # ORDER BY
    samples.append(s(
        "依標準售價由高至低排列所有商品。",
        f"SELECT pName, priceStd FROM {tbl} ORDER BY priceStd DESC;"
    ))
    samples.append(s(
        "依目前庫存由多至少列出商品清單。",
        f"SELECT pName, qtyNow FROM {tbl} ORDER BY qtyNow DESC;"
    ))

    # GROUP BY
    samples.append(s(
        "依廠商統計商品種數。",
        f"SELECT pvName, COUNT(*) FROM {tbl} GROUP BY pvName;"
    ))
    samples.append(s(
        "各廠商商品的平均標準售價。",
        f"SELECT pvName, AVG(priceStd) FROM {tbl} GROUP BY pvName;"
    ))

    # TOP N
    samples.append(s(
        "標準售價最高的前5種商品。",
        f"SELECT TOP 5 pName, priceStd FROM {tbl} ORDER BY priceStd DESC;"
    ))
    samples.append(s(
        "目前庫存最多的前3種商品。",
        f"SELECT TOP 3 pName, qtyNow FROM {tbl} ORDER BY qtyNow DESC;"
    ))

    # DISTINCT
    samples.append(s(
        "商品主檔中涉及哪些不重複的廠商？",
        f"SELECT DISTINCT pvName FROM {tbl};"
    ))

    # multi-condition
    samples.append(s(
        "廠商 '台東縣農會' 且正常銷售的商品。",
        f"SELECT pName, priceStd FROM {tbl} WHERE pvName = N'台東縣農會' AND isSale = '0';"
    ))
    samples.append(s(
        "標準售價高於 100 且目前庫存大於 10 的商品。",
        f"SELECT pName, priceStd, qtyNow FROM {tbl} WHERE priceStd > 100 AND qtyNow > 10;"
    ))

    # SUM
    samples.append(s(
        "所有商品的目前庫存總量。",
        f"SELECT SUM(qtyNow) FROM {tbl};"
    ))

    # extra to reach ~50
    samples.append(s(
        "商品主檔中哪些商品的會員價低於最低售價？",
        f"SELECT pName, priceMem, priceLow FROM {tbl} WHERE priceMem < priceLow;"
    ))
    samples.append(s(
        "標準成本超過 100 的商品清單。",
        f"SELECT pName, costStd FROM {tbl} WHERE costStd > 100;"
    ))
    samples.append(s(
        "廠商 '碧雲冰城' 的商品銷售狀態。",
        f"SELECT pName, isSale FROM {tbl} WHERE pvName = N'碧雲冰城';"
    ))
    samples.append(s(
        "查詢商品 '剝皮辣椒' 的所有售價資訊。",
        f"SELECT priceStd, priceLow, priceMem, priceBat FROM {tbl} WHERE pName = N'剝皮辣椒';"
    ))
    samples.append(s(
        "商品主檔中目前庫存為 0 的商品有幾種？",
        f"SELECT COUNT(*) FROM {tbl} WHERE qtyNow = 0;"
    ))
    samples.append(s(
        "廠商 '實垣有限公司' 的商品最低售價清單。",
        f"SELECT pName, priceLow FROM {tbl} WHERE pvName = N'實垣有限公司';"
    ))
    samples.append(s(
        "所有商品的平均標準售價。",
        f"SELECT AVG(priceStd) FROM {tbl};"
    ))
    samples.append(s(
        "商品 '南瓜酥罐裝' 的標準成本與平均成本。",
        f"SELECT costStd, costAvg FROM {tbl} WHERE pName = N'南瓜酥罐裝';"
    ))
    samples.append(s(
        "依廠商統計正常銷售商品的種數。",
        f"SELECT pvName, COUNT(*) FROM {tbl} WHERE isSale = '0' GROUP BY pvName;"
    ))
    samples.append(s(
        "所有商品的批量售價列表，依售價由低至高排序。",
        f"SELECT pName, priceBat FROM {tbl} ORDER BY priceBat ASC;"
    ))
    samples.append(s(
        "廠商 '百寶食品' 商品的平均成本。",
        f"SELECT AVG(costAvg) FROM {tbl} WHERE pvName = N'百寶食品';"
    ))
    samples.append(s(
        "查詢商品 '黑芝麻酥罐裝' 的完整售價與成本資訊。",
        f"SELECT priceStd, priceLow, priceMem, priceBat, costStd, costAvg FROM {tbl} WHERE pName = N'黑芝麻酥罐裝';"
    ))
    samples.append(s(
        "商品主檔中安全庫存量為 0 的商品有哪些？",
        f"SELECT pName FROM {tbl} WHERE qtySafe = 0;"
    ))
    samples.append(s(
        "各廠商的商品目前庫存總量。",
        f"SELECT pvName, SUM(qtyNow) FROM {tbl} GROUP BY pvName;"
    ))

    return samples


def gen_product_test():
    tbl = "WP_M09.dbo.WP_vProduct"
    samples = []
    def s(q, sql): return make_sample(q, sql)

    samples.append(s(
        "商品主檔共有幾筆商品？",
        f"SELECT COUNT(*) FROM {tbl};"
    ))
    samples.append(s(
        "停止進銷貨的商品有幾種？",
        f"SELECT COUNT(*) FROM {tbl} WHERE isSale = '3';"
    ))
    samples.append(s(
        "各廠商提供的商品種數，依種數由多至少排序。",
        f"SELECT pvName, COUNT(*) FROM {tbl} GROUP BY pvName ORDER BY COUNT(*) DESC;"
    ))
    samples.append(s(
        "標準售價最高的商品是哪一個？",
        f"SELECT TOP 1 pName, priceStd FROM {tbl} ORDER BY priceStd DESC;"
    ))
    samples.append(s(
        "廠商 '實垣有限公司' 的商品平均售價。",
        f"SELECT AVG(priceStd) FROM {tbl} WHERE pvName = N'實垣有限公司';"
    ))
    samples.append(s(
        "目前庫存低於安全庫存的商品有幾種？",
        f"SELECT COUNT(*) FROM {tbl} WHERE qtyNow < qtySafe;"
    ))
    samples.append(s(
        "只停止進貨（isSale='1'）的商品廠商名稱。",
        f"SELECT pName, pvName FROM {tbl} WHERE isSale = '1';"
    ))
    samples.append(s(
        "商品 '南瓜酥' 的完整價格資訊（標準、最低、會員、批量）。",
        f"SELECT priceStd, priceLow, priceMem, priceBat FROM {tbl} WHERE pName = N'南瓜酥';"
    ))
    samples.append(s(
        "平均成本最高的前3種商品。",
        f"SELECT TOP 3 pName, costAvg FROM {tbl} ORDER BY costAvg DESC;"
    ))
    samples.append(s(
        "廠商 '碧雲冰城' 的商品銷售狀態。",
        f"SELECT pName, isSale FROM {tbl} WHERE pvName = N'碧雲冰城';"
    ))
    samples.append(s(
        "會員價低於標準售價的商品清單。",
        f"SELECT pName, priceStd, priceMem FROM {tbl} WHERE priceMem < priceStd;"
    ))
    samples.append(s(
        "所有商品的目前庫存總量。",
        f"SELECT SUM(qtyNow) FROM {tbl};"
    ))
    samples.append(s(
        "安全庫存為 5 的商品有哪些？",
        f"SELECT pName FROM {tbl} WHERE qtySafe = 5;"
    ))
    samples.append(s(
        "批量售價最低的商品。",
        f"SELECT TOP 1 pName, priceBat FROM {tbl} ORDER BY priceBat ASC;"
    ))
    samples.append(s(
        "廠商 '新福華商行' 的商品中，目前庫存最多的是哪一個？",
        f"SELECT TOP 1 pName, qtyNow FROM {tbl} WHERE pvName = N'新福華商行' ORDER BY qtyNow DESC;"
    ))

    return samples


# ===========================================================================
# WP_vProvider  (廠商主檔)  — NO isDel, NO dtlIsDel
# ===========================================================================

def gen_provider_train():
    tbl = "WP_M09.dbo.WP_vProvider"
    samples = []
    def s(q, sql): return make_sample(q, sql)

    # 基本查詢
    samples.append(s(
        "查詢廠商編號 '001' 的基本資料。",
        f"SELECT * FROM {tbl} WHERE pvId = '001';"
    ))
    samples.append(s(
        "廠商編號 '003' 的廠商名稱是什麼？",
        f"SELECT TOP 1 pvName FROM {tbl} WHERE pvId = '003';"
    ))
    samples.append(s(
        "廠商編號 '004' 的聯絡資訊。",
        f"SELECT pvName, pvTel, ctactName FROM {tbl} WHERE pvId = '004';"
    ))
    samples.append(s(
        "廠商 '213' 的銀行帳戶資訊。",
        f"SELECT bankName, bankAccount FROM {tbl} WHERE pvId = '213';"
    ))
    samples.append(s(
        "廠商編號 '207' 的負責人是誰？",
        f"SELECT pvBoss FROM {tbl} WHERE pvId = '207';"
    ))

    # pvName 查詢
    samples.append(s(
        "查詢廠商 '宏碁蜂蜜' 的電話號碼。",
        f"SELECT pvTel FROM {tbl} WHERE pvName = N'宏碁蜂蜜';"
    ))
    samples.append(s(
        "'南投縣農會' 的廠商基本資料。",
        f"SELECT * FROM {tbl} WHERE pvName = N'南投縣農會';"
    ))
    samples.append(s(
        "廠商 '信義鄉農會' 的聯絡人姓名。",
        f"SELECT ctactName FROM {tbl} WHERE pvName = N'信義鄉農會';"
    ))
    samples.append(s(
        "'鹽水區農會' 的銀行名稱。",
        f"SELECT bankName FROM {tbl} WHERE pvName = N'鹽水區農會';"
    ))
    samples.append(s(
        "廠商 '溪湖鎮農會購物中心' 的帳戶號碼。",
        f"SELECT bankAccount FROM {tbl} WHERE pvName = N'溪湖鎮農會購物中心';"
    ))
    samples.append(s(
        "'花蓮富里鄉農會' 的廠商資料。",
        f"SELECT pvName, pvTel, ctactName FROM {tbl} WHERE pvName = N'花蓮富里鄉農會';"
    ))
    samples.append(s(
        "廠商 '龍宏醬業' 的聯絡人及電話。",
        f"SELECT ctactName, pvTel FROM {tbl} WHERE pvName = N'龍宏醬業';"
    ))
    samples.append(s(
        "'百寶食品' 的廠商編號。",
        f"SELECT pvId FROM {tbl} WHERE pvName = N'百寶食品';"
    ))
    samples.append(s(
        "廠商 '台東縣農會' 的銀行資訊。",
        f"SELECT bankName, bankAccount FROM {tbl} WHERE pvName = N'台東縣農會';"
    ))

    # isStop
    samples.append(s(
        "所有正常往來（isStop='N'）的廠商清單。",
        f"SELECT pvId, pvName FROM {tbl} WHERE isStop = 'N';"
    ))
    samples.append(s(
        "停止往來（isStop='Y'）的廠商有哪些？",
        f"SELECT pvId, pvName FROM {tbl} WHERE isStop = 'Y';"
    ))
    samples.append(s(
        "正常往來的廠商共有幾家？",
        f"SELECT COUNT(*) FROM {tbl} WHERE isStop = 'N';"
    ))
    samples.append(s(
        "停止往來的廠商數量。",
        f"SELECT COUNT(*) FROM {tbl} WHERE isStop = 'Y';"
    ))

    # pvBoss
    samples.append(s(
        "負責人為 '賴朝賢' 的廠商名稱。",
        f"SELECT pvName FROM {tbl} WHERE pvBoss = N'賴朝賢';"
    ))

    # ctactName
    samples.append(s(
        "聯絡人為 '宥萍' 的廠商資料。",
        f"SELECT pvName, pvTel FROM {tbl} WHERE ctactName = N'宥萍';"
    ))
    samples.append(s(
        "聯絡人為 '陳文浩' 的廠商名稱。",
        f"SELECT pvName FROM {tbl} WHERE ctactName = N'陳文浩';"
    ))
    samples.append(s(
        "查詢聯絡人 '潘玉玲' 所屬廠商的電話。",
        f"SELECT pvName, pvTel FROM {tbl} WHERE ctactName = N'潘玉玲';"
    ))
    samples.append(s(
        "聯絡人為 '陳靜芬' 的廠商名稱與編號。",
        f"SELECT pvId, pvName FROM {tbl} WHERE ctactName = N'陳靜芬';"
    ))

    # bankName
    samples.append(s(
        "使用 '一銀埔里分行' 的廠商有哪些？",
        f"SELECT pvName FROM {tbl} WHERE bankName = N'一銀埔里分行';"
    ))
    samples.append(s(
        "銀行為 '兆豐銀行南投分行' 的廠商名稱。",
        f"SELECT pvName FROM {tbl} WHERE bankName = N'兆豐銀行南投分行';"
    ))

    # pvTel
    samples.append(s(
        "電話為 '2251170' 的廠商名稱。",
        f"SELECT pvName FROM {tbl} WHERE pvTel = '2251170';"
    ))
    samples.append(s(
        "電話號碼 '2791949' 對應哪個廠商？",
        f"SELECT pvName FROM {tbl} WHERE pvTel = '2791949';"
    ))

    # bankAccount
    samples.append(s(
        "銀行帳號 '06409012660' 屬於哪個廠商？",
        f"SELECT pvName FROM {tbl} WHERE bankAccount = '06409012660';"
    ))
    samples.append(s(
        "帳號 '58101010328251' 的廠商名稱。",
        f"SELECT pvName FROM {tbl} WHERE bankAccount = '58101010328251';"
    ))

    # COUNT
    samples.append(s(
        "廠商主檔共有幾家廠商？",
        f"SELECT COUNT(*) FROM {tbl};"
    ))

    # ORDER BY
    samples.append(s(
        "依廠商編號排序列出所有廠商。",
        f"SELECT pvId, pvName FROM {tbl} ORDER BY pvId;"
    ))
    samples.append(s(
        "依廠商名稱排序列出所有廠商的電話。",
        f"SELECT pvName, pvTel FROM {tbl} ORDER BY pvName;"
    ))

    # multi-condition
    samples.append(s(
        "正常往來且有提供電話的廠商。",
        f"SELECT pvName, pvTel FROM {tbl} WHERE isStop = 'N' AND pvTel IS NOT NULL;"
    ))
    samples.append(s(
        "廠商編號 '208' 的往來狀態。",
        f"SELECT pvName, isStop FROM {tbl} WHERE pvId = '208';"
    ))

    # 全欄
    samples.append(s(
        "列出所有廠商的編號、名稱及往來狀態。",
        f"SELECT pvId, pvName, isStop FROM {tbl};"
    ))
    samples.append(s(
        "廠商 '大城鄉農會展售中心' 的完整資料。",
        f"SELECT * FROM {tbl} WHERE pvName = N'大城鄉農會展售中心';"
    ))

    # extra to reach ~50
    samples.append(s(
        "廠商主檔中有幾種不同的銀行？",
        f"SELECT COUNT(DISTINCT bankName) FROM {tbl};"
    ))
    samples.append(s(
        "廠商 '信義鄉農會' 的電話號碼。",
        f"SELECT pvTel FROM {tbl} WHERE pvName = N'信義鄉農會';"
    ))
    samples.append(s(
        "廠商編號 '207' 的廠商聯絡人姓名。",
        f"SELECT ctactName FROM {tbl} WHERE pvId = '207';"
    ))
    samples.append(s(
        "廠商 '花蓮富里鄉農會' 的往來狀態。",
        f"SELECT isStop FROM {tbl} WHERE pvName = N'花蓮富里鄉農會';"
    ))
    samples.append(s(
        "廠商主檔中負責人不為空的廠商清單。",
        f"SELECT pvName, pvBoss FROM {tbl} WHERE pvBoss IS NOT NULL;"
    ))
    samples.append(s(
        "廠商 '溪湖鎮農會購物中心' 的往來狀態與聯絡人。",
        f"SELECT isStop, ctactName FROM {tbl} WHERE pvName = N'溪湖鎮農會購物中心';"
    ))
    samples.append(s(
        "廠商編號 '003' 的銀行帳戶資訊。",
        f"SELECT bankName, bankAccount FROM {tbl} WHERE pvId = '003';"
    ))
    samples.append(s(
        "廠商 '鹽水區農會' 的廠商編號與往來狀態。",
        f"SELECT pvId, isStop FROM {tbl} WHERE pvName = N'鹽水區農會';"
    ))
    samples.append(s(
        "所有廠商的電話號碼清單（不含空值）。",
        f"SELECT pvName, pvTel FROM {tbl} WHERE pvTel IS NOT NULL;"
    ))
    samples.append(s(
        "廠商 '南投縣農會' 的聯絡人與電話。",
        f"SELECT ctactName, pvTel FROM {tbl} WHERE pvName = N'南投縣農會';"
    ))
    samples.append(s(
        "正常往來廠商中有哪些聯絡人？",
        f"SELECT DISTINCT ctactName FROM {tbl} WHERE isStop = 'N';"
    ))
    samples.append(s(
        "廠商編號 '004' 的完整資料。",
        f"SELECT * FROM {tbl} WHERE pvId = '004';"
    ))
    samples.append(s(
        "廠商主檔中各往來狀態的廠商數量統計。",
        f"SELECT isStop, COUNT(*) FROM {tbl} GROUP BY isStop;"
    ))
    samples.append(s(
        "廠商 '大城鄉農會展售中心' 的聯絡人與電話。",
        f"SELECT ctactName, pvTel FROM {tbl} WHERE pvName = N'大城鄉農會展售中心';"
    ))

    return samples


def gen_provider_test():
    tbl = "WP_M09.dbo.WP_vProvider"
    samples = []
    def s(q, sql): return make_sample(q, sql)

    samples.append(s(
        "廠商主檔共有幾家廠商？",
        f"SELECT COUNT(*) FROM {tbl};"
    ))
    samples.append(s(
        "正常往來的廠商有幾家？",
        f"SELECT COUNT(*) FROM {tbl} WHERE isStop = 'N';"
    ))
    samples.append(s(
        "廠商 '宏碁蜂蜜' 的往來狀態是否正常？",
        f"SELECT pvName, isStop FROM {tbl} WHERE pvName = N'宏碁蜂蜜';"
    ))
    samples.append(s(
        "使用 '信義鄉農會' 作為銀行的廠商有哪些？",
        f"SELECT pvName FROM {tbl} WHERE bankName = N'信義鄉農會';"
    ))
    samples.append(s(
        "廠商編號 '001' 到 '004' 的廠商清單。",
        f"SELECT pvId, pvName FROM {tbl} WHERE pvId >= '001' AND pvId <= '004';"
    ))
    samples.append(s(
        "所有停止往來廠商的名稱與電話。",
        f"SELECT pvName, pvTel FROM {tbl} WHERE isStop = 'Y';"
    ))
    samples.append(s(
        "廠商 '花蓮縣富里鄉農會' 的銀行帳號。",
        f"SELECT bankAccount FROM {tbl} WHERE bankName = N'花蓮縣富里鄉農會';"
    ))
    samples.append(s(
        "列出所有廠商的名稱及聯絡人，依廠商名稱排序。",
        f"SELECT pvName, ctactName FROM {tbl} ORDER BY pvName;"
    ))
    samples.append(s(
        "廠商 '南投縣農會' 的負責人姓名。",
        f"SELECT pvBoss FROM {tbl} WHERE pvName = N'南投縣農會';"
    ))
    samples.append(s(
        "廠商編號 '207' 的往來狀態與銀行資訊。",
        f"SELECT pvName, isStop, bankName, bankAccount FROM {tbl} WHERE pvId = '207';"
    ))
    samples.append(s(
        "負責人為 '賴朝賢' 的廠商往來狀態。",
        f"SELECT pvName, isStop FROM {tbl} WHERE pvBoss = N'賴朝賢';"
    ))
    samples.append(s(
        "廠商中有哪些不同的銀行名稱？",
        f"SELECT DISTINCT bankName FROM {tbl};"
    ))
    samples.append(s(
        "廠商 '鹽水區農會' 的完整聯絡資訊。",
        f"SELECT pvName, pvTel, pvBoss, ctactName FROM {tbl} WHERE pvName = N'鹽水區農會';"
    ))
    samples.append(s(
        "廠商主檔中有哪些不同的聯絡人？",
        f"SELECT DISTINCT ctactName FROM {tbl};"
    ))
    samples.append(s(
        "廠商編號為 '208' 的廠商名稱與銀行資訊。",
        f"SELECT pvName, bankName, bankAccount FROM {tbl} WHERE pvId = '208';"
    ))

    return samples


# ===========================================================================
# Main: assemble and save
# ===========================================================================

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    out_dir = os.path.join(base_dir, "data", "wp_m09")
    os.makedirs(out_dir, exist_ok=True)

    # Build datasets
    view_train = {
        "WP_vAcctIn":    gen_acct_in_train(),
        "WP_vAcctOut":   gen_acct_out_train(),
        "WP_vOutStock":  gen_out_stock_train(),
        "WP_vTransfer":  gen_transfer_train(),
        "WP_vInventory": gen_inventory_train(),
        "WP_vProduct":   gen_product_train(),
        "WP_vProvider":  gen_provider_train(),
    }
    view_test = {
        "WP_vAcctIn":    gen_acct_in_test(),
        "WP_vAcctOut":   gen_acct_out_test(),
        "WP_vOutStock":  gen_out_stock_test(),
        "WP_vTransfer":  gen_transfer_test(),
        "WP_vInventory": gen_inventory_test(),
        "WP_vProduct":   gen_product_test(),
        "WP_vProvider":  gen_provider_test(),
    }

    train_data = []
    for v, samples in view_train.items():
        train_data.extend(samples)

    test_data = []
    for v, samples in view_test.items():
        test_data.extend(samples)

    train_path = os.path.join(out_dir, "train_from_samples.json")
    test_path  = os.path.join(out_dir, "test_from_samples.json")

    with open(train_path, "w", encoding="utf-8") as f:
        json.dump(train_data, f, ensure_ascii=False, indent=2)

    with open(test_path, "w", encoding="utf-8") as f:
        json.dump(test_data, f, ensure_ascii=False, indent=2)

    # --------------- Statistics ---------------
    print("=" * 60)
    print("TRAIN samples per view:")
    total_train = 0
    for v, samples in view_train.items():
        print(f"  {v:25s}: {len(samples):3d}")
        total_train += len(samples)
    print(f"  {'TOTAL':25s}: {total_train:3d}")

    print()
    print("TEST samples per view:")
    total_test = 0
    for v, samples in view_test.items():
        print(f"  {v:25s}: {len(samples):3d}")
        total_test += len(samples)
    print(f"  {'TOTAL':25s}: {total_test:3d}")

    # isDel distribution
    views_with_isdel = {"WP_vAcctIn", "WP_vAcctOut", "WP_vOutStock", "WP_vTransfer"}
    views_without_isdel = {"WP_vInventory", "WP_vProduct", "WP_vProvider"}

    train_with = sum(len(v) for k, v in view_train.items() if k in views_with_isdel)
    train_without = sum(len(v) for k, v in view_train.items() if k in views_without_isdel)
    test_with = sum(len(v) for k, v in view_test.items() if k in views_with_isdel)
    test_without = sum(len(v) for k, v in view_test.items() if k in views_without_isdel)

    print()
    print("isDel / dtlIsDel distribution:")
    print(f"  TRAIN - views WITH isDel    : {train_with:3d} samples (views: AcctIn, AcctOut, OutStock, Transfer)")
    print(f"  TRAIN - views WITHOUT isDel : {train_without:3d} samples (views: Inventory, Product, Provider)")
    print(f"  TEST  - views WITH isDel    : {test_with:3d} samples")
    print(f"  TEST  - views WITHOUT isDel : {test_without:3d} samples")

    print()
    print(f"Saved TRAIN -> {train_path}")
    print(f"Saved TEST  -> {test_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
