"""
WP_M09 互動式 SQL 查詢（優化版）
修正：避免生成過度複雜的 SQL 查詢
"""
import torch
import json
import re
from pathlib import Path
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from peft import PeftModel

class WPQueryBot:
    def __init__(self, model_path, schema_file):
        """初始化查詢機器人"""
        print("🔧 載入模型...")

        self.tokenizer = AutoTokenizer.from_pretrained(model_path)
        if self.tokenizer.pad_token is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token

        bnb_config = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_compute_dtype=torch.bfloat16,
        )

        base_model = AutoModelForCausalLM.from_pretrained(
            "meta-llama/Llama-3.1-8B-Instruct",
            quantization_config=bnb_config,
            device_map="auto",
            torch_dtype=torch.bfloat16,
        )

        self.model = PeftModel.from_pretrained(base_model, model_path)
        self.model.eval()

        # 載入 schema
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema = json.load(f)[0]

        self.tables = schema['table_names_original']
        self.columns = schema['column_names_original']
        self.column_types = schema['column_types']

        print("✅ 模型載入完成\n")

    def build_schema(self, table_name):
        """構建表的 schema 描述"""
        if table_name not in self.tables:
            return ""

        table_idx = self.tables.index(table_name)
        table_cols = []

        for col_idx, (t_idx, col_name) in enumerate(self.columns):
            if t_idx == table_idx:
                col_type = self.column_types[col_idx]
                table_cols.append(f"{col_name} ({col_type})")

        if len(table_cols) <= 15:
            cols_str = ', '.join(table_cols)
        else:
            cols_str = ', '.join(table_cols[:15]) + f', ... ({len(table_cols)-15} more)'

        return f"Table {table_name}: {cols_str}"

    def detect_query_type(self, question):
        """檢測查詢類型"""
        question_lower = question.lower()

        # 簡單計數查詢
        simple_count = [
            'how many', 'count', 'total number', 'number of',
            'how much', 'what is the count', 'total count'
        ]

        # 簡單列表查詢
        simple_list = [
            'show all', 'list all', 'display all', 'get all',
            'show me all', 'give me all', 'list everything'
        ]

        # 聚合查詢（需要保留條件）
        aggregation = [
            'average', 'sum', 'max', 'min', 'total',
            'highest', 'lowest', 'most', 'least'
        ]

        # 條件查詢（需要保留條件）
        conditional = [
            'where', 'with', 'that', 'which',
            'greater than', 'less than', 'equal to',
            'out of stock', 'in stock', 'on sale'
        ]

        if any(p in question_lower for p in simple_count):
            return 'simple_count'
        elif any(p in question_lower for p in simple_list):
            return 'simple_list'
        elif any(p in question_lower for p in conditional):
            return 'conditional'
        elif any(p in question_lower for p in aggregation):
            return 'aggregation'
        else:
            return 'general'

    def generate_sql(self, question, table_name):
        """生成 SQL（優化版）"""
        schema = self.build_schema(table_name)

        if not schema:
            return f"❌ 找不到表: {table_name}"

        # 檢測查詢類型
        query_type = self.detect_query_type(question)

        # 根據查詢類型調整 prompt
        if query_type == 'simple_count':
            prompt = f"Schema: {schema}\nQuestion: {question}\nGenerate a simple COUNT query without WHERE conditions:\nSQL:"
        elif query_type == 'simple_list':
            prompt = f"Schema: {schema}\nQuestion: {question}\nGenerate a simple SELECT * query without WHERE conditions:\nSQL:"
        else:
            prompt = f"Schema: {schema}\nQuestion: {question}\nSQL:"

        inputs = self.tokenizer(
            prompt,
            return_tensors="pt",
            truncation=True,
            max_length=512
        ).to(self.model.device)

        with torch.no_grad():
            outputs = self.model.generate(
                **inputs,
                max_new_tokens=64,              # 限制長度避免過長查詢
                do_sample=True,                  # 啟用採樣
                temperature=0.3,                 # 降低隨機性
                top_p=0.9,                       # 核採樣
                num_beams=3,                     # 束搜索
                early_stopping=True,             # 提前停止
                pad_token_id=self.tokenizer.pad_token_id,
                eos_token_id=self.tokenizer.eos_token_id,
            )

        sql = self.tokenizer.decode(
            outputs[0][inputs['input_ids'].shape[1]:],
            skip_special_tokens=True
        ).strip()

        # 清理
        sql = re.sub(r'```sql\s*', '', sql)
        sql = re.sub(r'```\s*', '', sql)
        sql = sql.split('\n')[0].strip()

        # 後處理：根據查詢類型清理 SQL
        sql = self.clean_sql(sql, query_type)

        return sql

    def clean_sql(self, sql, query_type):
        """清理過度複雜的 SQL"""

        # 簡單計數查詢：不應該有 WHERE
        if query_type == 'simple_count':
            # 移除所有 WHERE 條件
            match = re.search(r'(SELECT\s+COUNT\(\*\)\s+FROM\s+\w+)', sql, re.IGNORECASE)
            if match:
                return match.group(1)

        # 簡單列表查詢：不應該有 WHERE
        elif query_type == 'simple_list':
            # 移除所有 WHERE 條件
            match = re.search(r'(SELECT\s+\*\s+FROM\s+\w+)', sql, re.IGNORECASE)
            if match:
                return match.group(1)

        # 其他查詢：限制 WHERE 條件數量
        else:
            # 如果有過多 AND（超過 5 個），簡化
            and_count = sql.upper().count(' AND ')

            if and_count > 5:
                # 只保留主要結構
                # 嘗試移除 = 0 的條件（通常是多餘的）
                sql = re.sub(r'\s+AND\s+\w+\s*=\s*0', '', sql, flags=re.IGNORECASE)

        return sql

    def list_tables(self):
        """列出所有表"""
        print("\n📋 可用的表:")

        # 分類顯示
        product_tables = [t for t in self.tables if 'Product' in t]
        receipt_tables = [t for t in self.tables if 'Receipt' in t]
        payment_tables = [t for t in self.tables if 'Payment' in t]
        other_tables = [t for t in self.tables if t not in product_tables + receipt_tables + payment_tables]

        if product_tables:
            print("\n   📦 產品相關:")
            for table in product_tables:
                print(f"      - {table}")

        if receipt_tables:
            print("\n   🧾 收據相關:")
            for table in receipt_tables:
                print(f"      - {table}")

        if payment_tables:
            print("\n   💰 付款相關:")
            for table in payment_tables:
                print(f"      - {table}")

        if other_tables:
            print("\n   📊 其他:")
            for table in other_tables[:10]:  # 只顯示前 10 個
                print(f"      - {table}")
            if len(other_tables) > 10:
                print(f"      ... 還有 {len(other_tables) - 10} 個表")

    def show_examples(self, table_name):
        """顯示範例問題"""
        examples = {
            'WP_Product': [
                "How many products in my store?",
                "What is the average price?",
                "Show me items that are out of stock",
                "Which products cost more than 1000?",
                "List all product names",
            ],
            'WP_vProduct': [
                "How many products do we have?",
                "Show all products",
                "What is the most expensive item?",
                "Show items on sale",
            ],
            'WP_vReceipt': [
                "How many receipts?",
                "What is the total amount?",
                "Show recent receipts",
                "Show receipts from today",
            ],
            'WP_vPayment': [
                "Total payment amount?",
                "Show cash payments",
                "How many payments today?",
            ],
        }

        if table_name in examples:
            print(f"\n💡 範例問題 ({table_name}):")
            for i, ex in enumerate(examples[table_name], 1):
                print(f"   {i}. {ex}")
        else:
            print(f"\n💡 通用範例問題:")
            print(f"   1. How many records?")
            print(f"   2. Show all items")
            print(f"   3. What is the average [column]?")
            print(f"   4. Show items where [condition]")

    def run(self):
        """運行互動式查詢"""
        print("="*70)
        print("🤖 WP_M09 SQL 查詢機器人（優化版）")
        print("="*70)
        print("\n✨ 功能:")
        print("- 自動生成 SQL 查詢")
        print("- 智能簡化複雜查詢")
        print("- 支援自然語言問題")

        print("\n📝 指令:")
        print("- 'tables'   : 列出所有表")
        print("- 'examples' : 顯示範例問題")
        print("- 'change'   : 切換表")
        print("- 'quit'     : 退出")

        current_table = None

        while True:
            try:
                print("\n" + "="*70)

                # 獲取表名
                if not current_table:
                    table_input = input("📋 選擇表 (或輸入 'tables' 查看): ").strip()

                    if table_input.lower() in ['quit', 'exit', 'q']:
                        print("\n👋 再見！")
                        break

                    if table_input.lower() == 'tables':
                        self.list_tables()
                        continue

                    if table_input not in self.tables:
                        print(f"❌ 找不到表: {table_input}")
                        print(f"💡 輸入 'tables' 查看可用的表")
                        continue

                    current_table = table_input
                    self.show_examples(current_table)

                # 獲取問題
                question = input(f"\n❓ 問題 ('{current_table}'): ").strip()

                if question.lower() in ['quit', 'exit', 'q']:
                    print("\n👋 再見！")
                    break

                if question.lower() == 'change':
                    current_table = None
                    continue

                if question.lower() == 'examples':
                    self.show_examples(current_table)
                    continue

                if question.lower() == 'tables':
                    self.list_tables()
                    continue

                if not question:
                    continue

                # 生成 SQL
                print("\n🔄 生成中...")

                query_type = self.detect_query_type(question)
                print(f"🔍 檢測到查詢類型: {query_type}")

                sql = self.generate_sql(question, current_table)

                print(f"\n✅ 生成的 SQL:")
                print(f"   {sql}")

                # 顯示提示
                if query_type in ['simple_count', 'simple_list']:
                    print(f"\n💡 這是簡單查詢，已自動移除不必要的條件")

            except KeyboardInterrupt:
                print("\n\n👋 再見！")
                break
            except Exception as e:
                print(f"\n❌ 錯誤: {e}")
                import traceback
                traceback.print_exc()

if __name__ == "__main__":
    MODEL_PATH = "outputs/models/wp_m09_from_scratch/final_model"
    SCHEMA_FILE = "data/wp_m09/tables.json"

    if not Path(MODEL_PATH).exists():
        print(f"❌ 找不到模型: {MODEL_PATH}")
        print(f"請先訓練模型: python train_wp_m09.py")
        exit()

    if not Path(SCHEMA_FILE).exists():
        print(f"❌ 找不到 schema: {SCHEMA_FILE}")
        print(f"請先生成 schema: python convert_excel_to_json.py")
        exit()

    bot = WPQueryBot(MODEL_PATH, SCHEMA_FILE)
    bot.run()