#!/usr/bin/env python3
"""
eval__9views_v0326.py
通用 9 Views 評估腳本 — 自動適配 Qwen / Llama 模型

功能：
  1. 自動從 training_info.json 讀取 base_model + train_script
  2. 自動載入對應的 system prompt（from train script）
  3. 自動處理 Qwen / Llama 不同的 chat template
  4. Spider 1.0 風格 Component EM + Partial Match + EX（可選）
  5. 每筆推論顯示進度 + 預估時間

用法:
  python eval__9views_v0326.py ^
      --model outputs/models/qwen35_9b_9views_0326/final_model ^
      --gold data/wp_m09/split_9views_20k_test.json ^
      --output outputs/eval_qwen35_9b_0326_test.json

  # 含 DB 執行驗證（EX）
  python eval__9views_v0326.py ^
      --model outputs/models/qwen35_9b_9views_0326/final_model ^
      --gold data/wp_m09/split_9views_20k_test.json ^
      --output outputs/eval_qwen35_9b_0326_test.json ^
      --db-trusted

  # 使用已有的預測結果（跳過推論）
  python eval__9views_v0326.py ^
      --gold data/wp_m09/split_9views_20k_test.json ^
      --pred outputs/predictions.json ^
      --output outputs/eval_result.json
"""

import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict, Counter
from typing import Optional


# ============================================================
# SQL Parser（從 eval__spider_style.py 精簡移植）
# ============================================================

def normalize_col(col: str) -> str:
    col = col.strip().lower()
    col = re.sub(r'\[(\w+)\]', r'\1', col)
    col = re.sub(r'wp_m09\.dbo\.\w+\.', '', col)
    col = re.sub(r'^\w+\.', '', col)
    return col


def extract_all_tables(sql: str) -> set:
    tables = set()
    for m in re.finditer(r'WP_M09\.dbo\.(\w+)', sql, re.IGNORECASE):
        tables.add(m.group(1).lower())
    for m in re.finditer(r'\[WP_M09\]\.\[dbo\]\.\[(\w+)\]', sql, re.IGNORECASE):
        tables.add(m.group(1).lower())
    return tables


def parse_select_columns(sql: str) -> list:
    m = re.match(r'SELECT\s+(.*?)\s+FROM\s', sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return []
    sel_str = m.group(1).strip()
    sel_str = re.sub(r'^DISTINCT\s+', '', sel_str, flags=re.IGNORECASE)
    sel_str = re.sub(r'^TOP\s+\d+\s+', '', sel_str, flags=re.IGNORECASE)

    columns = []
    depth = 0
    current = ""
    for ch in sel_str:
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        if ch == ',' and depth == 0:
            columns.append(current.strip())
            current = ""
        else:
            current += ch
    if current.strip():
        columns.append(current.strip())

    result = []
    for col_expr in columns:
        col_clean = re.sub(r'\s+AS\s+\w+', '', col_expr, flags=re.IGNORECASE).strip()
        agg_match = re.match(r'(COUNT|SUM|AVG|MIN|MAX)\s*\((.*)\)', col_clean, re.IGNORECASE)
        if agg_match:
            agg = agg_match.group(1).lower()
            inner = re.sub(r'^DISTINCT\s+', '', agg_match.group(2).strip(), flags=re.IGNORECASE)
            result.append((agg, normalize_col(inner)))
        else:
            result.append(('none', normalize_col(col_clean)))

    return sorted(result)


def parse_where_conditions(sql: str) -> list:
    m = re.search(r'\bWHERE\s+(.*?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+HAVING|\s*;?\s*$)',
                  sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return []
    where_str = m.group(1).strip()
    conditions = []
    tokens = re.split(r'(\bAND\b|\bOR\b)', where_str, flags=re.IGNORECASE)

    for token in tokens:
        t = token.strip()
        if t.upper() in ('AND', 'OR') or not t:
            continue

        while t.startswith('(') and t.endswith(')'):
            d = 0
            matched = True
            for i, ch in enumerate(t):
                if ch == '(':
                    d += 1
                elif ch == ')':
                    d -= 1
                if d == 0 and i < len(t) - 1:
                    matched = False
                    break
            if matched:
                t = t[1:-1].strip()
            else:
                break

        if re.match(r'.+\bIS\s+NOT\s+NULL', t, re.IGNORECASE):
            col = re.match(r'(.+?)\s+IS\s+NOT\s+NULL', t, re.IGNORECASE).group(1)
            conditions.append((normalize_col(col), 'is not null'))
        elif re.match(r'.+\bIS\s+NULL', t, re.IGNORECASE):
            col = re.match(r'(.+?)\s+IS\s+NULL', t, re.IGNORECASE).group(1)
            conditions.append((normalize_col(col), 'is null'))
        elif re.match(r'.+\bBETWEEN\b', t, re.IGNORECASE):
            col = re.match(r'(.+?)\s+BETWEEN\b', t, re.IGNORECASE).group(1)
            conditions.append((normalize_col(col), 'between'))
        elif re.match(r'.+\bLIKE\b', t, re.IGNORECASE):
            col = re.match(r'(.+?)\s+LIKE\b', t, re.IGNORECASE).group(1)
            conditions.append((normalize_col(col), 'like'))
        elif re.match(r'.+\bIN\s*\(', t, re.IGNORECASE):
            col = re.match(r'(.+?)\s+IN\s*\(', t, re.IGNORECASE).group(1)
            conditions.append((normalize_col(col), 'in'))
        elif re.match(r'.+?\s*(<>|!=|>=|<=|>|<|=)\s*', t):
            m2 = re.match(r'(.+?)\s*(<>|!=|>=|<=|>|<|=)\s*', t)
            col, op = m2.group(1), m2.group(2)
            if op in ('!=', '<>'):
                op = '!='
            conditions.append((normalize_col(col), op))

    return sorted(conditions)


def parse_group_by(sql: str) -> list:
    m = re.search(r'GROUP\s+BY\s+(.*?)(?:\s+HAVING|\s+ORDER|\s*;?\s*$)',
                  sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return []
    return sorted([normalize_col(c.strip()) for c in m.group(1).split(',')])


def parse_having(sql: str) -> list:
    m = re.search(r'HAVING\s+(.*?)(?:\s+ORDER|\s*;?\s*$)',
                  sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return []
    having_str = m.group(1).strip()
    agg_m = re.match(r'(COUNT|SUM|AVG|MIN|MAX)\s*\(([^)]*)\)\s*(>=|<=|>|<|=|!=)\s*',
                     having_str, re.IGNORECASE)
    if agg_m:
        return [(agg_m.group(1).lower(), normalize_col(agg_m.group(2)), agg_m.group(3))]
    return []


def parse_order_by(sql: str) -> list:
    m = re.search(r'ORDER\s+BY\s+(.*?)(?:\s*;?\s*$)', sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return []
    result = []
    for p in m.group(1).split(','):
        p = p.strip()
        direction = 'desc' if re.search(r'\bDESC\b', p, re.IGNORECASE) else 'asc'
        col = re.sub(r'\b(ASC|DESC)\b', '', p, flags=re.IGNORECASE).strip()
        col = normalize_col(col)
        if col:
            result.append((col, direction))
    return result


def extract_keywords(sql: str) -> set:
    kw = set()
    s = sql.upper()
    if 'DISTINCT' in s:
        kw.add('distinct')
    if re.search(r'\bTOP\s+\d+', s):
        kw.add('top')
    if 'GROUP BY' in s:
        kw.add('group')
    if 'ORDER BY' in s:
        kw.add('order')
    if 'HAVING' in s:
        kw.add('having')
    if 'JOIN' in s:
        kw.add('join')
    for agg in ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']:
        if agg + '(' in s:
            kw.add(agg.lower())
    if 'WHERE' in s:
        kw.add('where')
    if 'BETWEEN' in s:
        kw.add('between')
    if 'LIKE' in s:
        kw.add('like')
    if ' IN (' in s or ' IN(' in s:
        kw.add('in')
    return kw


def set_f1(pred_set, gold_set):
    if not pred_set and not gold_set:
        return 1.0, 1.0, 1.0
    if not pred_set or not gold_set:
        return 0.0, 0.0, 0.0
    common = pred_set & gold_set if isinstance(pred_set, set) else set(pred_set) & set(gold_set)
    prec = len(common) / len(pred_set) if pred_set else 0
    rec = len(common) / len(gold_set) if gold_set else 0
    f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0
    return prec, rec, f1


def eval_component_match(pred_sql: str, gold_sql: str) -> dict:
    """Spider 風格 Component-level 比對。"""
    result = {}

    # select
    pred_sel = set(parse_select_columns(pred_sql))
    gold_sel = set(parse_select_columns(gold_sql))
    acc, rec, f1 = set_f1(pred_sel, gold_sel)
    result['select'] = {'acc': acc, 'rec': rec, 'f1': f1}

    # where (value-agnostic)
    pred_where = set(parse_where_conditions(pred_sql))
    gold_where = set(parse_where_conditions(gold_sql))
    acc, rec, f1 = set_f1(pred_where, gold_where)
    result['where'] = {'acc': acc, 'rec': rec, 'f1': f1}

    # group by
    pred_gb = set(parse_group_by(pred_sql))
    gold_gb = set(parse_group_by(gold_sql))
    acc, rec, f1 = set_f1(pred_gb, gold_gb)
    result['groupBy'] = {'acc': acc, 'rec': rec, 'f1': f1}

    # having
    pred_hv = set(tuple(x) for x in parse_having(pred_sql))
    gold_hv = set(tuple(x) for x in parse_having(gold_sql))
    acc, rec, f1 = set_f1(pred_hv, gold_hv)
    result['having'] = {'acc': acc, 'rec': rec, 'f1': f1}

    # order by
    pred_ob = set(parse_order_by(pred_sql))
    gold_ob = set(parse_order_by(gold_sql))
    acc, rec, f1 = set_f1(pred_ob, gold_ob)
    result['orderBy'] = {'acc': acc, 'rec': rec, 'f1': f1}

    # table
    pred_tbl = extract_all_tables(pred_sql)
    gold_tbl = extract_all_tables(gold_sql)
    acc, rec, f1 = set_f1(pred_tbl, gold_tbl)
    result['table'] = {'acc': acc, 'rec': rec, 'f1': f1}

    # keywords
    pred_kw = extract_keywords(pred_sql)
    gold_kw = extract_keywords(gold_sql)
    acc, rec, f1 = set_f1(pred_kw, gold_kw)
    result['keywords'] = {'acc': acc, 'rec': rec, 'f1': f1}

    # exact match = 所有 component F1 == 1.0
    result['exact_match'] = all(v['f1'] == 1.0 for k, v in result.items() if k != 'exact_match')
    return result


def normalize_sql_for_em(sql: str) -> str:
    """正規化 SQL 用於字串 EM 比對。"""
    s = sql.strip().rstrip(';').strip()
    s = re.sub(r'\s+', ' ', s)
    s = s.upper()
    # 移除 AS 別名
    s = re.sub(r'\s+AS\s+\w+', '', s)
    return s


# ============================================================
# Execution Accuracy (EX)
# ============================================================

def compute_ex(cursor, pred_sql: str, gold_sql: str) -> bool:
    """比較兩個 SQL 的執行結果是否相同。"""
    if not pred_sql or pred_sql == ";":
        return False
    try:
        cursor.execute(gold_sql.rstrip(';'))
        gold_rows = cursor.fetchall()
    except Exception:
        return False
    try:
        cursor.execute(pred_sql.rstrip(';'))
        pred_rows = cursor.fetchall()
    except Exception:
        return False

    # 比對（排序後）
    def to_sorted(rows):
        return sorted([tuple(str(c) for c in r) for r in rows])

    return to_sorted(pred_rows) == to_sorted(gold_rows)


# ============================================================
# Model Inference
# ============================================================

def extract_sql_from_output(raw_text: str) -> str:
    """從模型輸出中提取乾淨的 SQL。

    處理模型可能在 SQL 前輸出 schema 資訊的情況：
      "WP_vAcctIn (isDel='N') SELECT ..." → "SELECT ..."
    """
    # 策略1：regex 找第一個 SELECT/WITH
    sql_match = re.search(r'\b(SELECT\b.+|WITH\b.+)', raw_text, re.IGNORECASE | re.DOTALL)
    if sql_match:
        sql = sql_match.group(1).strip()
    else:
        # 策略2：取第一行
        lines = raw_text.strip().split("\n")
        sql = lines[0].strip() if lines else ""

    # 只取到第一個換行（單一 SQL 語句）
    sql = sql.split("\n")[0].strip()

    # 移除尾部分號後重加（標準化）
    sql = sql.rstrip(';').strip()
    if sql:
        sql += ";"
    return sql


def run_inference(model_path: str, test_data: list) -> list:
    """載入模型並對測試資料進行推論。"""
    import torch
    from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
    from peft import PeftModel

    print(f"\n載入模型: {model_path} ...")

    # 讀取 training_info.json
    info_path = os.path.join(model_path, "training_info.json")
    if os.path.exists(info_path):
        with open(info_path) as f:
            info = json.load(f)
        base_model = info.get("base_model", "")
        train_script = info.get("train_script", "")
        include_rules = info.get("include_rules", True)
        model_family = info.get("model_family", "unknown")
    else:
        print("  [WARN] training_info.json not found, using defaults")
        base_model = "meta-llama/Llama-3.1-8B-Instruct"
        train_script = ""
        include_rules = True
        model_family = "unknown"

    print(f"  Base model: {base_model}")
    print(f"  Family: {model_family}")
    print(f"  Train script: {train_script}")

    # 載入 system prompt（從 train script 動態 import）
    system_prompt = None
    if train_script:
        module_name = train_script.replace(".py", "")
        try:
            import importlib
            mod = importlib.import_module(module_name)
            if hasattr(mod, "build_system_prompt"):
                system_prompt = mod.build_system_prompt(include_rules=include_rules)
                print(f"  System prompt: loaded from {module_name}.build_system_prompt()")
        except Exception as e:
            print(f"  [WARN] Cannot import {module_name}: {e}")

    if system_prompt is None:
        # Fallback：使用本腳本內建（與 v0326 train 相同）
        try:
            from train__qwen_9views_v0326 import build_system_prompt
            system_prompt = build_system_prompt(include_rules=include_rules)
            print(f"  System prompt: loaded from train__qwen_9views_v0326 (fallback)")
        except ImportError:
            try:
                from train__9views_20k_v0325 import build_system_prompt
                system_prompt = build_system_prompt(include_rules=include_rules)
                print(f"  System prompt: loaded from train__9views_20k_v0325 (fallback)")
            except ImportError:
                print("  [ERROR] Cannot load system prompt from any source!")
                sys.exit(1)

    # 載入 tokenizer + model
    bnb_cfg = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16,
        bnb_4bit_use_double_quant=True,
    )

    tokenizer = AutoTokenizer.from_pretrained(model_path, use_fast=True)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    base = AutoModelForCausalLM.from_pretrained(
        base_model,
        quantization_config=bnb_cfg,
        device_map="auto",
        dtype=torch.bfloat16,
        attn_implementation="sdpa",
    )
    model = PeftModel.from_pretrained(base, model_path)
    model.eval()

    # 測試 prompt
    test_msgs = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": test_data[0]["question"]},
    ]
    test_prompt = tokenizer.apply_chat_template(test_msgs, tokenize=False, add_generation_prompt=True)
    test_tokens = len(tokenizer(test_prompt)["input_ids"])
    print(f"\n模型載入完成，開始推論 {len(test_data)} 筆 ...")
    print(f"  Prompt tokens: {test_tokens}, max_new_tokens: 300")

    predictions = []

    for i, item in enumerate(test_data):
        t0 = time.time()
        question = item["question"]

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ]
        prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
        inputs = tokenizer(prompt, return_tensors="pt").to(model.device)

        with torch.no_grad():
            outputs = model.generate(
                **inputs,
                max_new_tokens=300,
                do_sample=False,
                pad_token_id=tokenizer.eos_token_id,
            )

        gen_tokens = outputs[0].shape[0] - inputs["input_ids"].shape[1]
        generated_ids = outputs[0][inputs["input_ids"].shape[1]:]
        raw_output = tokenizer.decode(generated_ids, skip_special_tokens=True).strip()

        pred_sql = extract_sql_from_output(raw_output)
        predictions.append(pred_sql)

        elapsed = time.time() - t0
        if i == 0:
            print(f"  第 1 筆: {elapsed:.1f}s, 生成 {gen_tokens} tokens")
            est = elapsed * len(test_data) / 60
            print(f"  預估總時間: {est:.1f} 分鐘")
        if (i + 1) % 20 == 0 or (i + 1) == len(test_data):
            print(f"  已推論: {i+1}/{len(test_data)} ({elapsed:.1f}s/筆)", flush=True)

    return predictions


# ============================================================
# Evaluate
# ============================================================

def evaluate(gold_data: list, predictions: list, cursor=None):
    n = len(gold_data)
    component_names = ['select', 'where', 'groupBy', 'having', 'orderBy', 'table', 'keywords']

    scores = {}
    for level in ['easy', 'medium', 'hard', 'extra', 'all']:
        scores[level] = {
            'count': 0, 'em_string': 0, 'em_component': 0, 'ex': 0,
            'partial': {c: {'f1_sum': 0, 'count': 0} for c in component_names}
        }

    view_stats = defaultdict(lambda: {'count': 0, 'em_string': 0, 'em_component': 0, 'ex': 0})
    details = []

    for i in range(n):
        gold_sql = gold_data[i]["query"]
        pred_sql = predictions[i]
        question = gold_data[i]["question"]
        difficulty = gold_data[i].get("difficulty", "unknown")

        view = ""
        m = re.search(r'WP_M09\.dbo\.(\w+)', gold_sql)
        if m:
            view = m.group(1)

        # 字串 EM
        em_string = normalize_sql_for_em(pred_sql) == normalize_sql_for_em(gold_sql)

        # Component EM
        comp_results = eval_component_match(pred_sql, gold_sql)
        em_component = comp_results['exact_match']

        # EX
        ex = compute_ex(cursor, pred_sql, gold_sql) if cursor else False

        # 累計
        for level_key in [difficulty, 'all']:
            if level_key not in scores:
                scores[level_key] = {
                    'count': 0, 'em_string': 0, 'em_component': 0, 'ex': 0,
                    'partial': {c: {'f1_sum': 0, 'count': 0} for c in component_names}
                }
            scores[level_key]['count'] += 1
            if em_string:
                scores[level_key]['em_string'] += 1
            if em_component:
                scores[level_key]['em_component'] += 1
            if ex:
                scores[level_key]['ex'] += 1
            for comp in component_names:
                if comp in comp_results:
                    scores[level_key]['partial'][comp]['f1_sum'] += comp_results[comp]['f1']
                    scores[level_key]['partial'][comp]['count'] += 1

        view_stats[view]['count'] += 1
        if em_string:
            view_stats[view]['em_string'] += 1
        if em_component:
            view_stats[view]['em_component'] += 1
        if ex:
            view_stats[view]['ex'] += 1

        details.append({
            "id": i, "question": question,
            "gold_sql": gold_sql, "pred_sql": pred_sql,
            "view": view, "difficulty": difficulty,
            "em_string": em_string, "em_component": em_component,
            "ex": ex if cursor else None,
            "components": {k: v for k, v in comp_results.items() if k != 'exact_match'},
        })

    # ---- 報告 ----
    a = scores['all']
    has_ex = cursor is not None

    print("\n" + "=" * 80)
    print("WP_M09 Text-to-SQL 評估報告（Spider 1.0 風格）")
    print("=" * 80)
    print(f"\n測試筆數: {n}")

    print(f"\n{'='*60}")
    print(f"  整體指標")
    print(f"{'='*60}")
    print(f"  String EM:      {a['em_string']}/{a['count']} = {a['em_string']/max(a['count'],1)*100:.2f}%")
    print(f"  Component EM:   {a['em_component']}/{a['count']} = {a['em_component']/max(a['count'],1)*100:.2f}%")
    if has_ex:
        print(f"  Execution (EX): {a['ex']}/{a['count']} = {a['ex']/max(a['count'],1)*100:.2f}%")

    # 按難度
    print(f"\n{'─'*70}")
    print(f"  按難度分組")
    print(f"{'─'*70}")
    hdr = f"  {'Difficulty':<10} {'Count':>6} {'Str-EM':>8} {'Str%':>7} {'Comp-EM':>8} {'Comp%':>7}"
    if has_ex:
        hdr += f" {'EX':>6} {'EX%':>7}"
    print(hdr)
    print(f"  {'─'*65}")
    for diff in ['easy', 'medium', 'hard', 'extra']:
        if diff not in scores or scores[diff]['count'] == 0:
            continue
        s = scores[diff]
        c = s['count']
        line = f"  {diff:<10} {c:>6} {s['em_string']:>8} {s['em_string']/c*100:>6.1f}% {s['em_component']:>8} {s['em_component']/c*100:>6.1f}%"
        if has_ex:
            line += f" {s['ex']:>6} {s['ex']/c*100:>6.1f}%"
        print(line)

    # 按 View
    print(f"\n{'─'*70}")
    print(f"  按 View 分組")
    print(f"{'─'*70}")
    hdr = f"  {'View':<22} {'Count':>6} {'Str-EM':>8} {'Str%':>7} {'Comp-EM':>8} {'Comp%':>7}"
    if has_ex:
        hdr += f" {'EX':>6} {'EX%':>7}"
    print(hdr)
    print(f"  {'─'*65}")
    for view in sorted(view_stats.keys()):
        s = view_stats[view]
        c = s['count']
        line = f"  {view:<22} {c:>6} {s['em_string']:>8} {s['em_string']/c*100:>6.1f}% {s['em_component']:>8} {s['em_component']/c*100:>6.1f}%"
        if has_ex:
            line += f" {s['ex']:>6} {s['ex']/c*100:>6.1f}%"
        print(line)

    # Partial Match
    print(f"\n{'─'*70}")
    print(f"  Partial Match（元件級 F1 平均）")
    print(f"{'─'*70}")
    hdr = f"  {'Component':<12}"
    for diff in ['easy', 'medium', 'hard', 'all']:
        hdr += f" {diff:>10}"
    print(hdr)
    print(f"  {'─'*55}")
    for comp in component_names:
        line = f"  {comp:<12}"
        for diff in ['easy', 'medium', 'hard', 'all']:
            if diff in scores and scores[diff]['partial'][comp]['count'] > 0:
                avg = scores[diff]['partial'][comp]['f1_sum'] / scores[diff]['partial'][comp]['count']
                line += f" {avg:>9.3f}"
            else:
                line += f" {'N/A':>10}"
        print(line)

    # 錯誤分析（前 10 筆）
    fails = [d for d in details if not d['em_component']]
    if fails:
        print(f"\n{'─'*70}")
        print(f"  Component EM 失敗分析（前 10 筆，共 {len(fails)} 筆）")
        print(f"{'─'*70}")
        for d in fails[:10]:
            failed = [k for k, v in d['components'].items() if v.get('f1', 1) < 1.0]
            print(f"\n  [{d['id']}] ({d['view']}, {d['difficulty']}) 失敗: {failed}")
            print(f"    Q: {d['question'][:80]}")
            print(f"    Gold: {d['gold_sql'][:100]}")
            print(f"    Pred: {d['pred_sql'][:100]}")

    return {
        "total": n,
        "em_string": a['em_string'],
        "em_string_pct": round(a['em_string'] / max(a['count'], 1) * 100, 2),
        "em_component": a['em_component'],
        "em_component_pct": round(a['em_component'] / max(a['count'], 1) * 100, 2),
        "ex_correct": a['ex'] if has_ex else None,
        "ex_pct": round(a['ex'] / max(a['count'], 1) * 100, 2) if has_ex else None,
        "scores_by_difficulty": {
            diff: {
                "count": scores[diff]['count'],
                "em_string": scores[diff]['em_string'],
                "em_component": scores[diff]['em_component'],
                "ex": scores[diff]['ex'],
            }
            for diff in ['easy', 'medium', 'hard', 'extra', 'all']
            if diff in scores and scores[diff]['count'] > 0
        },
        "scores_by_view": dict(view_stats),
        "partial_match": {
            comp: {
                diff: round(scores[diff]['partial'][comp]['f1_sum'] /
                            scores[diff]['partial'][comp]['count'], 4)
                if diff in scores and scores[diff]['partial'][comp]['count'] > 0 else None
                for diff in ['easy', 'medium', 'hard', 'all']
            }
            for comp in component_names
        },
        "details": details,
    }


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="WP_M09 9-Views 通用評估（Spider 1.0 風格）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例:
  # 用模型推論 + 評估
  python eval__9views_v0326.py --model outputs/models/.../final_model --gold data/.../test.json --output eval.json

  # 用已有預測結果評估
  python eval__9views_v0326.py --gold data/.../test.json --pred predictions.json --output eval.json

  # 含 DB 執行驗證
  python eval__9views_v0326.py --model ... --gold ... --output ... --db-trusted
        """
    )
    parser.add_argument("--gold", required=True, help="Gold 測試集 JSON")
    parser.add_argument("--pred", default=None, help="已有的預測 JSON（跳過推論）")
    parser.add_argument("--model", default=None, help="模型路徑（與 --pred 二選一）")
    parser.add_argument("--db-host", default=r"SHANE\SQLEXPRESS", help="SQL Server host")
    parser.add_argument("--db-name", default="WP_M09", help="Database name")
    parser.add_argument("--db-trusted", action="store_true", help="Windows Auth for EX")
    parser.add_argument("--output", default="eval_result.json", help="輸出路徑")
    args = parser.parse_args()

    # 載入 gold
    with open(args.gold, "r", encoding="utf-8") as f:
        gold_data = json.load(f)
    print(f"Gold 測試集: {len(gold_data)} 筆 ({args.gold})")

    # 取得 predictions
    if args.pred:
        with open(args.pred, "r", encoding="utf-8") as f:
            pred_raw = json.load(f)
        if isinstance(pred_raw, list) and len(pred_raw) > 0:
            if isinstance(pred_raw[0], str):
                predictions = pred_raw
            elif isinstance(pred_raw[0], dict):
                predictions = [p.get("pred_sql", "") for p in pred_raw]
            else:
                predictions = [str(p) for p in pred_raw]
        else:
            predictions = []
        print(f"載入預測: {len(predictions)} 筆")
    elif args.model:
        predictions = run_inference(args.model, gold_data)
    else:
        print("ERROR: 需要 --model 或 --pred 參數")
        sys.exit(1)

    assert len(predictions) == len(gold_data), \
        f"predictions ({len(predictions)}) != gold ({len(gold_data)})"

    # DB 連線（可選）
    cursor = None
    if args.db_trusted:
        try:
            import pyodbc
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={args.db_host};"
                f"DATABASE={args.db_name};"
                f"Trusted_Connection=yes;"
            )
            conn = pyodbc.connect(conn_str, timeout=10)
            cursor = conn.cursor()
            print(f"DB 連線: {args.db_host}/{args.db_name} (Trusted)")
        except Exception as e:
            print(f"[WARN] DB 連線失敗: {e}. 跳過 EX 評估。")

    # 評估
    result = evaluate(gold_data, predictions, cursor=cursor)

    # 儲存
    os.makedirs(os.path.dirname(args.output) if os.path.dirname(args.output) else ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\n評估結果: {args.output}")


if __name__ == "__main__":
    main()
