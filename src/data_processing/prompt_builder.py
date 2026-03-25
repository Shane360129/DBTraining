"""
Prompt 構建器
"""

class PromptBuilder:
    def __init__(self, config):
        self.config = config
        self.use_cot = config.get('prompt', {}).get('use_cot', True)

    def build_training_messages(self, question, db_schema, sql=None):
        """
        構建訓練用的 messages

        Args:
            question: 問題
            db_schema: 資料庫 schema
            sql: SQL 查詢（訓練時提供）

        Returns:
            messages 列表
        """
        # 構建 schema 描述
        schema_desc = self.build_schema_description(db_schema)

        # System prompt
        if self.use_cot:
            system_msg = """You are an expert SQL query generator. Follow these steps:
1. Understand the question and identify key entities
2. Map entities to database tables and columns
3. Determine the required SQL operations
4. Generate the final SQL query"""
        else:
            system_msg = "You are an expert SQL query generator. Generate accurate SQL queries based on the given database schema and question."

        # User message
        user_msg = f"""Database Schema:
{schema_desc}

Question: {question}

Generate the SQL query:"""

        messages = [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_msg}
        ]

        # 如果提供了 SQL（訓練時），添加 assistant 回應
        if sql:
            messages.append({"role": "assistant", "content": sql})

        return messages

    def build_schema_description(self, db_schema):
        """
        構建資料庫 schema 描述

        Args:
            db_schema: Spider 格式的 schema

        Returns:
            schema 描述字串
        """
        if not db_schema:
            return "No schema available"

        schema_parts = []

        # 表名和欄位
        table_names = db_schema.get('table_names_original', [])
        column_names = db_schema.get('column_names_original', [])
        column_types = db_schema.get('column_types', [])

        # 按表組織欄位
        tables_info = {}
        for col_idx, (table_idx, col_name) in enumerate(column_names):
            if table_idx == -1:  # 跳過 '*'
                continue

            if table_idx not in tables_info:
                tables_info[table_idx] = []

            col_type = column_types[col_idx] if col_idx < len(column_types) else 'text'
            tables_info[table_idx].append(f"{col_name} ({col_type})")

        # 構建描述
        for table_idx, table_name in enumerate(table_names):
            columns = tables_info.get(table_idx, [])
            if columns:
                schema_parts.append(f"Table {table_name}: {', '.join(columns)}")

        # 外鍵
        foreign_keys = db_schema.get('foreign_keys', [])
        if foreign_keys:
            fk_parts = []
            for fk in foreign_keys:
                if len(fk) == 2:
                    col1_idx, col2_idx = fk
                    if col1_idx < len(column_names) and col2_idx < len(column_names):
                        col1 = column_names[col1_idx]
                        col2 = column_names[col2_idx]
                        if col1[0] < len(table_names) and col2[0] < len(table_names):
                            fk_parts.append(f"{table_names[col1[0]]}.{col1[1]} = {table_names[col2[0]]}.{col2[1]}")

            if fk_parts:
                schema_parts.append(f"\nForeign Keys: {', '.join(fk_parts)}")

        return '\n'.join(schema_parts)

    def build_inference_messages(self, question, db_schema):
        """
        構建推理用的 messages（不包含 SQL 答案）

        Args:
            question: 問題
            db_schema: 資料庫 schema

        Returns:
            messages 列表
        """
        return self.build_training_messages(question, db_schema, sql=None)