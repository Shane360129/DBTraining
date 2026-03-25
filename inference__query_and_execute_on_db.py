#!/usr/bin/env python3
# inference__query_and_execute_on_db.py
# ──────────────────────────────────────────────────────────────────────────────
# 功能：自然語言 → SQL 生成 → SQL Server 執行 → 格式化輸出
#
# 流程：
#   1. 載入最新 DoRA 權重（可指定）
#   2. 使用者輸入中文問句
#   3. 模型生成 T-SQL
#   4. pyodbc 連線 WP_M09 執行查詢
#   5. 格式化表格輸出結果
#
# 執行方式：
#   python inference__query_and_execute_on_db.py              # 互動模式
#   python inference__query_and_execute_on_db.py --question "查詢本月出庫總量"
#   python inference__query_and_execute_on_db.py --no-exec    # 只生成 SQL 不執行
#   python inference__query_and_execute_on_db.py --model outputs/models/wp_m09_dora_0317_spider_r1/final_model
#
# 環境需求：
#   pip install torch transformers peft bitsandbytes pyodbc
#   ODBC Driver 17 for SQL Server
# ──────────────────────────────────────────────────────────────────────────────

import argparse
import json
import os
import re
import sys
import textwrap
from pathlib import Path

import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from peft import PeftModel

# ── 設定 ──────────────────────────────────────────────────────
DEFAULT_SERVER  = r"SHANE\SQLEXPRESS"
DEFAULT_DB      = "WP_M09"
SCHEMA_FILE     = "data/wp_m09/tables.json"
MODELS_DIR      = "outputs/models"

# 模型生成參數
MAX_NEW_TOKENS  = 256
TEMPERATURE     = 0.0     # 貪心解碼（確定性輸出）
REPETITION_PEN  = 1.1

# 特定 View 的提示（與訓練時一致）
TABLE_NOTES = {
    # ── isDel 不存在的 view：嚴格禁止使用 isDel/dtlIsDel ──
    "WP_vProduct":   "Note: pNo is a sequential product number (1, 2, 3...), NOT a date. "
                     "This view has no date filtering capability. "
                     "CRITICAL: This view has NO isDel or dtlIsDel column — never add them.",
    "WP_vInventory": "Note: pNo is a sequential product number, not a date. "
                     "This view has no date filtering capability. "
                     "CRITICAL: This view has NO isDel or dtlIsDel column — never add them.",
    "WP_vProvider":  "Note: Main lookup table for supplier info. Join with other views using supplierId. "
                     "CRITICAL: This view has NO isDel or dtlIsDel column — never add them.",
    # ── 有 isDel + dtlIsDel ──
    "WP_vTransfer":  "Note: Filter by date using LEFT(TransferId,8)='YYYYMMDD' "
                     "or LEFT(TransferId,6)='YYYYMM'. isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vAcctIn":    "Note: Filter by date using LEFT(acctInId,8)='YYYYMMDD'. "
                     "ALWAYS add isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vAcctOut":   "Note: Filter by date using LEFT(acctOutId,8)='YYYYMMDD'. "
                     "ALWAYS add isDel='N' AND dtlIsDel='N' for active records.",
    "WP_vOutStock":  "Note: Filter by date using LEFT(OutStkId,8)='YYYYMMDD'. "
                     "ALWAYS add isDel='N' AND dtlIsDel='N' for active records.",
}

# ── ANSI 色彩（Windows Terminal 支援）──────────────────────────
BOLD   = "\033[1m"
CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
GRAY   = "\033[90m"
RESET  = "\033[0m"


# ══════════════════════════════════════════════════════════════
# 工具函數
# ══════════════════════════════════════════════════════════════

def find_latest_model() -> str:
    """
    自動選擇最佳模型，優先順序：
      1. outputs/loop_state.json 記錄的最高 EM 模型
      2. 依日期字串排序（MMDD / MMDD_spider_rN）取最新
    """
    # ── 優先讀 loop_state.json ──────────────────────────────
    loop_state = Path("outputs/loop_state.json")
    if loop_state.exists():
        try:
            with open(loop_state, "r", encoding="utf-8") as f:
                state = json.load(f)
            rounds = state.get("rounds", [])
            if rounds:
                # 取 em_pct 最高的那輪
                best = max(rounds, key=lambda r: r.get("em_pct", 0))
                model_path = Path(best["model"])
                if model_path.exists():
                    print(f"{GRAY}  (loop_state: Round {best['round']}, EM={best['em_pct']}%){RESET}")
                    return str(model_path)
        except Exception:
            pass  # 解析失敗就退回排序方式

    # ── Fallback：依目錄名稱中的日期數字排序 ──────────────
    import re as _re
    def _date_key(p: Path) -> tuple:
        # 抽出 MMDD 與 round N，例如 wp_m09_dora_0317_spider_r1 → (317, 1)
        nums = _re.findall(r"\d+", p.parent.name)
        # 找到像 0317 這種4位日期作為主鍵
        date_num = next((int(n) for n in nums if len(n) == 4), 0)
        round_num = int(nums[-1]) if nums and len(nums[-1]) <= 2 else 0
        return (date_num, round_num)

    models = sorted(
        [d for d in Path(MODELS_DIR).glob("*/final_model") if d.is_dir()],
        key=_date_key
    )
    if not models:
        raise FileNotFoundError(f"找不到任何模型，請確認 {MODELS_DIR} 目錄")
    return str(models[-1])


def load_schema(schema_file: str) -> dict:
    """載入 tables.json，解析 schema 資訊"""
    with open(schema_file, "r", encoding="utf-8") as f:
        raw = json.load(f)
    schema = raw[0] if isinstance(raw, list) else raw
    return schema


def build_schema_prompt(schema: dict) -> str:
    """
    將 schema 轉成 Prompt 中的 CREATE TABLE 描述（與訓練時格式一致）
    """
    tables   = schema["table_names_original"]
    columns  = schema["column_names_original"]   # [ [table_idx, col_name], ... ]
    col_types = schema["column_types"]

    lines = []
    for t_idx, t_name in enumerate(tables):
        col_strs = []
        for c_idx, (cidx, cname) in enumerate(columns):
            if cidx == t_idx and cname != "*":
                ctype = col_types[c_idx] if c_idx < len(col_types) else "text"
                col_strs.append(f"  {cname} {ctype.upper()}")
        note = TABLE_NOTES.get(t_name, "")
        note_str = f"\n  -- {note}" if note else ""
        block = f"CREATE TABLE {t_name} ({note_str}\n" + ",\n".join(col_strs) + "\n);"
        lines.append(block)

    return "\n\n".join(lines)


def build_prompt(question: str, schema_str: str, db_id: str = "WP_M09") -> str:
    """
    建立與訓練一致的推論 Prompt
    """
    return (
        f"### Task\n"
        f"Generate a SQL query to answer the following question.\n\n"
        f"### Database Schema\n"
        f"The query will run on a database named `{db_id}` with the following tables:\n"
        f"{schema_str}\n\n"
        f"### Question\n"
        f"{question}\n\n"
        f"### SQL\n"
        f"Given the database schema, here is the SQL query that answers `{question}`:\n"
        f"```sql\n"
    )


def extract_sql(raw: str) -> str:
    """從模型輸出提取 SQL 語句"""
    # 嘗試抓 ```sql ... ``` 塊
    m = re.search(r"```sql\s*(.*?)(?:```|$)", raw, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # 嘗試抓第一個 SELECT/INSERT/UPDATE/DELETE 語句
    m = re.search(r"(SELECT|INSERT|UPDATE|DELETE|WITH)\s.+", raw, re.DOTALL | re.IGNORECASE)
    if m:
        sql = m.group(0)
        sql = re.split(r"```|\n\n", sql)[0]
        return sql.strip()
    return raw.strip()


# 哪些 view 不存在 isDel / dtlIsDel 欄位
_NO_ISDEL_VIEWS = {"WP_vInventory", "WP_vProduct", "WP_vProvider",
                   "WP_vPdKind", "WP_vPdExistIO", "WP_vStkTrace",
                   "WP_vOutStkTrace", "WP_vEmployee", "WP_vMember"}

def sanitize_sql(sql: str) -> tuple[str, list[str]]:
    """
    後處理：移除針對無 isDel 欄位之 view 所產生的錯誤條件。
    回傳 (cleaned_sql, list_of_warnings)
    """
    warnings = []

    # 判斷 SQL 操作的是哪些 view
    views_in_sql = set(re.findall(r"WP_v\w+", sql))
    no_del_in_query = views_in_sql & _NO_ISDEL_VIEWS

    if not no_del_in_query:
        return sql, warnings

    # 若目標 view 不含 isDel 欄，才清除
    # 移除 AND isDel = 'N' / AND dtlIsDel = 'N'（前後空白容錯）
    patterns_to_remove = [
        r"\s+AND\s+isDel\s*=\s*'[NY]'",
        r"\s+AND\s+dtlIsDel\s*=\s*'[NY]'",
        r"WHERE\s+isDel\s*=\s*'[NY]'\s+AND\s+",  # WHERE isDel=... AND ...
        r"\s+AND\s+\[isDel\]\s*=\s*'[NY]'",
        r"\s+AND\s+\[dtlIsDel\]\s*=\s*'[NY]'",
    ]
    cleaned = sql
    for pat in patterns_to_remove:
        before = cleaned
        cleaned = re.sub(pat, "", cleaned, flags=re.IGNORECASE)
        if cleaned != before:
            warnings.append(f"已移除不合法的 isDel 條件（view: {', '.join(no_del_in_query)}）")

    # 修補 WHERE  AND → WHERE（移除後可能留下 WHERE  AND）
    cleaned = re.sub(r"WHERE\s+AND\s+", "WHERE ", cleaned, flags=re.IGNORECASE)
    # 清理多餘空格
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()

    return cleaned, warnings


# ══════════════════════════════════════════════════════════════
# 模型類別
# ══════════════════════════════════════════════════════════════

class WPQueryModel:
    def __init__(self, model_path: str):
        print(f"{CYAN}[模型載入]{RESET} {model_path}")

        # 4-bit 量化設定
        bnb_config = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_compute_dtype=torch.bfloat16,
            bnb_4bit_use_double_quant=True,
        )

        # Tokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(model_path)
        if self.tokenizer.pad_token is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token
        self.tokenizer.padding_side = "left"

        # 基底模型 + DoRA adapter
        print(f"{GRAY}  > 載入 base model ...{RESET}")
        base_model = AutoModelForCausalLM.from_pretrained(
            "meta-llama/Llama-3.1-8B-Instruct",
            quantization_config=bnb_config,
            device_map="auto",
            torch_dtype=torch.bfloat16,
            trust_remote_code=True,
        )
        print(f"{GRAY}  > 套用 DoRA adapter ...{RESET}")
        self.model = PeftModel.from_pretrained(base_model, model_path)
        self.model.eval()
        print(f"{GREEN}[完成]{RESET} 模型已就緒\n")

    def generate_sql(self, question: str, schema_str: str) -> tuple[str, str]:
        """
        輸入問句，回傳 (raw_output, cleaned_sql)
        """
        prompt = build_prompt(question, schema_str)

        # Chat template 包裝（與訓練時一致）
        messages = [
            {"role": "system", "content": "You are an expert SQL assistant. Generate only the SQL query without explanation."},
            {"role": "user",   "content": prompt},
        ]
        text = self.tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
        )

        inputs = self.tokenizer(text, return_tensors="pt").to(self.model.device)

        with torch.no_grad():
            outputs = self.model.generate(
                **inputs,
                max_new_tokens=MAX_NEW_TOKENS,
                do_sample=False,
                temperature=TEMPERATURE if TEMPERATURE > 0 else None,
                repetition_penalty=REPETITION_PEN,
                pad_token_id=self.tokenizer.eos_token_id,
                eos_token_id=self.tokenizer.eos_token_id,
            )

        input_len = inputs["input_ids"].shape[1]
        generated = outputs[0][input_len:]
        raw_output = self.tokenizer.decode(generated, skip_special_tokens=True)
        sql = extract_sql(raw_output)
        return raw_output, sql


# ══════════════════════════════════════════════════════════════
# 資料庫執行
# ══════════════════════════════════════════════════════════════

def execute_sql(sql: str, server: str, database: str) -> tuple[list, list]:
    """
    在 SQL Server 上執行 SQL，回傳 (columns, rows)
    拋出例外時回傳 ([], []) 並印出錯誤
    """
    try:
        import pyodbc
    except ImportError:
        raise ImportError("請先安裝 pyodbc：pip install pyodbc")

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
    )

    try:
        conn   = pyodbc.connect(conn_str, timeout=15)
        cursor = conn.cursor()
        cursor.execute(sql)

        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows    = cursor.fetchall()
        conn.close()
        return columns, [list(r) for r in rows]

    except Exception as e:
        return [], [["ERROR", str(e)]]


# ══════════════════════════════════════════════════════════════
# 格式化輸出
# ══════════════════════════════════════════════════════════════

def format_table(columns: list, rows: list, max_rows: int = 50) -> str:
    """
    以 ASCII 格式輸出表格
    """
    if not columns:
        return "(無欄位資訊)"
    if not rows:
        return "(查詢結果為空)"

    # 轉成字串，截斷過長內容
    str_rows = [[str(v)[:60] if v is not None else "NULL" for v in row] for row in rows[:max_rows]]
    all_rows  = [columns] + str_rows
    col_widths = [max(len(str(r[i])) for r in all_rows) for i in range(len(columns))]

    sep = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"

    def fmt_row(row):
        return "|" + "|".join(f" {str(v):<{col_widths[i]}} " for i, v in enumerate(row)) + "|"

    lines = [
        sep,
        fmt_row(columns),
        sep,
        *[fmt_row(r) for r in str_rows],
        sep,
    ]
    if len(rows) > max_rows:
        lines.append(f"  （僅顯示前 {max_rows} 筆，共 {len(rows)} 筆）")

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════════

def run_query(
    question: str,
    query_model: WPQueryModel,
    schema_str: str,
    server: str,
    database: str,
    no_exec: bool = False,
    verbose: bool = False,
):
    """執行單一問句的完整流程"""
    print(f"\n{BOLD}{CYAN}{'─'*60}{RESET}")
    print(f"{BOLD}問句：{RESET}{question}")
    print(f"{CYAN}{'─'*60}{RESET}")

    # Step 1: 生成 SQL
    print(f"{YELLOW}[生成中...]{RESET}")
    raw_output, sql = query_model.generate_sql(question, schema_str)

    # Step 1b: SQL 後處理清洗（移除不合法的 isDel 條件）
    sql, san_warnings = sanitize_sql(sql)
    for w in san_warnings:
        print(f"{YELLOW}[自動修正]{RESET} {w}")

    print(f"\n{BOLD}生成的 SQL：{RESET}")
    print(f"{GREEN}{sql}{RESET}\n")

    if verbose:
        print(f"{GRAY}[原始輸出]\n{raw_output}\n{RESET}")

    # Step 2: 執行 SQL
    if no_exec:
        print(f"{GRAY}（--no-exec 模式，跳過資料庫執行）{RESET}")
        return sql, None, None

    print(f"{YELLOW}[執行中...]{RESET} → {server}/{database}")
    columns, rows = execute_sql(sql, server, database)

    if not columns:
        # 發生錯誤
        error_msg = rows[0][1] if rows else "未知錯誤"
        print(f"{RED}[執行失敗]{RESET} {error_msg}\n")
        return sql, None, error_msg

    # Step 3: 顯示結果
    row_count = len(rows)
    print(f"{GREEN}[執行成功]{RESET} 共 {row_count} 筆結果\n")
    table_str = format_table(columns, rows)
    print(table_str)
    return sql, (columns, rows), None


def interactive_mode(
    query_model: WPQueryModel,
    schema_str: str,
    server: str,
    database: str,
    no_exec: bool,
):
    """互動式查詢迴圈"""
    print(f"\n{BOLD}{'═'*60}{RESET}")
    print(f"{BOLD}{CYAN}  WP_M09 自然語言查詢系統{RESET}")
    print(f"{BOLD}{'═'*60}{RESET}")
    print(f"  模型已載入，輸入中文問句查詢資料庫")
    print(f"  輸入 {BOLD}exit{RESET} 或 {BOLD}quit{RESET} 結束，{BOLD}help{RESET} 查看範例")
    if no_exec:
        print(f"  {YELLOW}[離線模式]{RESET} 只生成 SQL，不執行資料庫查詢")
    print(f"{'─'*60}\n")

    EXAMPLES = [
        "查詢本月所有出庫記錄的總金額",
        "列出庫存數量最多的前 5 項產品",
        "2024 年 12 月的應收帳款總額是多少",
        "查詢所有供應商名稱",
        "哪些產品的庫存低於 10",
    ]

    while True:
        try:
            question = input(f"\n{BOLD}請輸入問句{RESET} > ").strip()
        except (EOFError, KeyboardInterrupt):
            print(f"\n{GRAY}已結束{RESET}")
            break

        if not question:
            continue
        if question.lower() in ("exit", "quit", "q"):
            print(f"{GRAY}再見！{RESET}")
            break
        if question.lower() == "help":
            print(f"\n{BOLD}範例問句：{RESET}")
            for ex in EXAMPLES:
                print(f"  • {ex}")
            continue

        run_query(question, query_model, schema_str, server, database, no_exec)


# ══════════════════════════════════════════════════════════════
# 進入點
# ══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="WP_M09 自然語言 → SQL → 資料庫執行",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
        使用範例：
          python inference__query_and_execute_on_db.py
          python inference__query_and_execute_on_db.py --question "本月出庫總量"
          python inference__query_and_execute_on_db.py --no-exec
          python inference__query_and_execute_on_db.py \\
              --model outputs/models/wp_m09_dora_0317_spider_r1/final_model \\
              --question "庫存最多的 10 個產品"
        """)
    )
    parser.add_argument("--model",    type=str, default=None,
                        help="DoRA 模型路徑（預設：自動選最新）")
    parser.add_argument("--server",   type=str, default=DEFAULT_SERVER,
                        help=f"SQL Server 主機（預設：{DEFAULT_SERVER}）")
    parser.add_argument("--db",       type=str, default=DEFAULT_DB,
                        help=f"資料庫名稱（預設：{DEFAULT_DB}）")
    parser.add_argument("--schema",   type=str, default=SCHEMA_FILE,
                        help=f"Schema JSON 路徑（預設：{SCHEMA_FILE}）")
    parser.add_argument("--question", type=str, default=None,
                        help="單次查詢問句（不帶則進入互動模式）")
    parser.add_argument("--no-exec",  action="store_true",
                        help="只生成 SQL，不連接資料庫執行")
    parser.add_argument("--verbose",  action="store_true",
                        help="顯示模型原始輸出")
    args = parser.parse_args()

    # ── 路徑設定 ────────────────────────────────────────────
    script_dir = Path(__file__).parent
    os.chdir(script_dir)

    # ── 選擇模型 ────────────────────────────────────────────
    model_path = args.model or find_latest_model()

    if not Path(model_path).exists():
        print(f"{RED}[錯誤]{RESET} 找不到模型路徑：{model_path}")
        print("  可用模型：")
        for m in sorted(Path(MODELS_DIR).glob("*/final_model")):
            print(f"    {m}")
        sys.exit(1)

    # ── 載入 Schema ─────────────────────────────────────────
    if not Path(args.schema).exists():
        print(f"{RED}[錯誤]{RESET} 找不到 Schema 檔：{args.schema}")
        sys.exit(1)
    schema    = load_schema(args.schema)
    schema_str = build_schema_prompt(schema)

    # ── 載入模型 ────────────────────────────────────────────
    query_model = WPQueryModel(model_path)

    # ── 執行查詢 ────────────────────────────────────────────
    if args.question:
        # 單次查詢
        run_query(
            args.question, query_model, schema_str,
            args.server, args.db, args.no_exec, args.verbose
        )
    else:
        # 互動模式
        interactive_mode(
            query_model, schema_str,
            args.server, args.db, args.no_exec
        )


if __name__ == "__main__":
    main()
