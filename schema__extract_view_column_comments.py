"""
從 WP_M09 七個檢視表提取：
  1. 欄位 schema（名稱、型別、來源表註解）
  2. 每個 View 前 10 筆資料樣本

輸出：data/wp_m09/view_schema_and_samples.json
用途：供後續生成 Spider1 格式訓練資料與驗證資料
"""
import pyodbc
import json
import re
from pathlib import Path
from decimal import Decimal
import datetime

# ============================================================
# 設定
# ============================================================
SERVER   = r"SHANE\SQLEXPRESS"
DATABASE = "WP_M09"
SAMPLE_ROWS = 10

TARGET_VIEWS = [
    "WP_vAcctIn",
    "WP_vAcctOut",
    "WP_vOutStock",
    "WP_vTransfer",
    "WP_vInventory",
    "WP_vProduct",
    "WP_vProvider",
]


# ============================================================
# 工具
# ============================================================
def get_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)


def json_serializable(val):
    """將不可序列化的型別轉為字串"""
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, (datetime.date, datetime.datetime)):
        return str(val)
    if isinstance(val, bytes):
        return val.hex()
    return str(val)


def get_view_columns(cursor, view_name):
    """取得 View 的欄位清單（名稱、型別）"""
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """, view_name)
    return [(row.COLUMN_NAME, row.DATA_TYPE) for row in cursor.fetchall()]


def get_column_comment(cursor, col_name, source_tables):
    """從來源表的 extended properties 找欄位中文描述"""
    for tbl in source_tables:
        try:
            cursor.execute("""
                SELECT CAST(ep.value AS NVARCHAR(500))
                FROM sys.columns sc
                LEFT JOIN sys.extended_properties ep
                    ON ep.major_id = sc.object_id
                    AND ep.minor_id = sc.column_id
                    AND ep.name = 'MS_Description'
                WHERE sc.object_id = OBJECT_ID(?) AND sc.name = ?
            """, tbl, col_name)
            row = cursor.fetchone()
            if row and row[0]:
                return row[0]
        except Exception:
            continue
    return ""


def get_source_tables(cursor, view_name):
    """從 View 定義解析來源資料表"""
    cursor.execute("SELECT OBJECT_DEFINITION(OBJECT_ID(?))", view_name)
    row = cursor.fetchone()
    if not row or not row[0]:
        return []
    sql = re.sub(r'--.*', '', row[0])
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    tables = set()
    for pattern in [r'FROM\s+(?:\[?\w+\]?\.)?\[?(\w+)\]?',
                    r'JOIN\s+(?:\[?\w+\]?\.)?\[?(\w+)\]?']:
        for m in re.finditer(pattern, sql, re.IGNORECASE):
            t = m.group(1)
            if t and t.upper() not in ('SELECT','WHERE','ORDER','GROUP','HAVING','AS'):
                tables.add(t)
    return sorted(tables)


def get_sample_rows(cursor, view_name, n=SAMPLE_ROWS):
    """取得 View 前 N 筆資料"""
    cursor.execute(f"SELECT TOP {n} * FROM WP_M09.dbo.[{view_name}]")
    cols = [desc[0] for desc in cursor.description]
    rows = []
    for row in cursor.fetchall():
        record = {}
        for col, val in zip(cols, row):
            if val is None:
                record[col] = None
            elif isinstance(val, (int, float, bool, str)):
                record[col] = val
            else:
                record[col] = json_serializable(val)
        rows.append(record)
    return rows


# ============================================================
# 主流程
# ============================================================
def main():
    print("=" * 60)
    print(f"WP_M09 View Schema + 資料樣本擷取")
    print(f"目標: {', '.join(TARGET_VIEWS)}")
    print("=" * 60)

    conn   = get_connection()
    cursor = conn.cursor()
    print(f"資料庫連線成功: {SERVER} / {DATABASE}\n")

    result = {}

    for view_name in TARGET_VIEWS:
        print(f"處理: {view_name}")

        # 1. 欄位清單
        columns = get_view_columns(cursor, view_name)
        print(f"  欄位數: {len(columns)}")

        # 2. 來源表（for 抓欄位註解）
        source_tables = get_source_tables(cursor, view_name)

        # 3. 欄位 schema（含中文描述）
        schema = []
        for col_name, col_type in columns:
            comment = get_column_comment(cursor, col_name, source_tables)
            schema.append({
                "name":    col_name,
                "type":    col_type,
                "comment": comment,
            })
            note = f" ({comment})" if comment else ""
            print(f"    {col_name} [{col_type}]{note}")

        # 4. 資料樣本
        try:
            samples = get_sample_rows(cursor, view_name)
            print(f"  取得 {len(samples)} 筆樣本資料")
        except Exception as e:
            samples = []
            print(f"  警告：無法取得樣本資料 - {e}")

        result[view_name] = {
            "source_tables": source_tables,
            "schema":        schema,
            "samples":       samples,
        }
        print()

    cursor.close()
    conn.close()

    # 儲存
    out_path = Path("data/wp_m09/view_schema_and_samples.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"已儲存: {out_path}")

    # 摘要
    print("\n--- 摘要 ---")
    for vn, vdata in result.items():
        has_isdel = any(c["name"] in ("isDel", "dtlIsDel") for c in vdata["schema"])
        print(f"  {vn}: {len(vdata['schema'])} 欄位, "
              f"{len(vdata['samples'])} 筆樣本, "
              f"isDel={'有' if has_isdel else '無'}")

    print(f"\n下一步: 將 {out_path} 交給 Claude 生成 Spider1 訓練資料")


if __name__ == "__main__":
    main()
