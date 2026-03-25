"""
列出 SHANE\SQLEXPRESS 上的所有資料庫
"""
import pyodbc

SERVER = r"SHANE\SQLEXPRESS"

print(f"🔌 連接伺服器: {SERVER}")
print("   使用 Windows 驗證\n")

try:
    # 不指定資料庫，讓系統自動連接到 master
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};Trusted_Connection=yes;"

    conn = pyodbc.connect(conn_str, timeout=10)
    print("✅ 連接成功！\n")

    cursor = conn.cursor()

    # 查詢所有資料庫
    cursor.execute("""
        SELECT 
            name AS [資料庫名稱],
            database_id AS [ID],
            SUSER_SNAME(owner_sid) AS [擁有者],
            create_date AS [建立日期]
        FROM sys.databases
        WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
        ORDER BY name
    """)

    print("📊 伺服器上的資料庫:\n")
    databases = []

    for row in cursor.fetchall():
        db_name, db_id, owner, create_date = row
        print(f"   📁 {db_name}")
        print(f"      ID: {db_id}")
        print(f"      擁有者: {owner}")
        print(f"      建立日期: {create_date}")
        print()
        databases.append(db_name)

    if not databases:
        print("⚠️  沒有找到使用者資料庫")

    cursor.close()
    conn.close()

    # 測試能否連接每個資料庫
    if databases:
        print("=" * 70)
        print("🔐 測試資料庫訪問權限\n")

        for db_name in databases:
            try:
                test_conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={db_name};Trusted_Connection=yes;"
                test_conn = pyodbc.connect(test_conn_str, timeout=5)

                test_cursor = test_conn.cursor()

                # 查詢表數量
                test_cursor.execute("""
                    SELECT 
                        (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE') AS [資料表],
                        (SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS) AS [檢視表]
                """)

                table_count, view_count = test_cursor.fetchone()

                print(f"✅ {db_name}")
                print(f"   資料表: {table_count} 個")
                print(f"   檢視表: {view_count} 個")
                print()

                test_conn.close()

            except Exception as e:
                print(f"❌ {db_name}")
                print(f"   無法訪問: {str(e)[:100]}...")
                print()

    print("=" * 70)
    print("✅ 完成！")

except Exception as e:
    print(f"❌ 連接失敗！")
    print(f"\n錯誤訊息:")
    print(f"{e}\n")

    print("可能的原因:")
    print("1. SQL Server 服務未啟動")
    print("2. ODBC Driver 17 未安裝")
    print("3. Windows 防火牆阻擋")
    print("4. 使用者沒有任何權限")