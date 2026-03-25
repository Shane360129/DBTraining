"""
Spider 資料集預處理器
"""
import json
import random
from pathlib import Path
from datasets import Dataset
from .prompt_builder import PromptBuilder

class SpiderPreprocessor:
    def __init__(self, config):
        self.config = config
        self.prompt_builder = PromptBuilder(config)

    def load_spider_data(self, json_path, tables_path):
        """載入 Spider JSON 和資料庫 schema"""
        print(f"      載入資料: {json_path}")

        # 載入資料
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # 載入 schema
        with open(tables_path, 'r', encoding='utf-8') as f:
            tables = json.load(f)

        # 建立 db_id -> schema 的映射
        db_schemas = {table['db_id']: table for table in tables}

        return data, db_schemas

    def prepare_dataset(self, data, db_schemas, augment=True):
        """準備訓練資料集"""

        # 格式化為 messages
        formatted_data = []
        for item in data:
            db_id = item['db_id']
            question = item['question']
            sql = item['query']

            # 獲取 schema
            schema = db_schemas.get(db_id, {})

            # 構建 prompt
            messages = self.prompt_builder.build_training_messages(
                question=question,
                db_schema=schema,
                sql=sql
            )

            # 添加難度信息
            difficulty = self._determine_difficulty(sql, item)

            formatted_data.append({
                'messages': messages,
                'db_id': db_id,
                'difficulty': difficulty
            })

        print(f"\n   原始資料大小: {len(formatted_data)}")

        # 資料增強（困難樣本加權）
        if augment:
            formatted_data = self.augment_with_difficulty_weight(formatted_data)

        return Dataset.from_list(formatted_data)

    def _determine_difficulty(self, sql, item):
        """判斷 SQL 難度"""
        # 如果資料中有難度標註
        if 'hardness' in item:
            return item['hardness']

        # 否則根據 SQL 特徵估計難度
        sql_lower = sql.lower()

        # Extra: 複雜查詢
        if any(keyword in sql_lower for keyword in ['intersect', 'union', 'except']):
            return 'extra'

        # Hard: 嵌套子查詢或複雜 JOIN
        if sql_lower.count('select') > 2 or sql_lower.count('join') > 2:
            return 'hard'

        # Medium: 包含 JOIN, GROUP BY, 或 HAVING
        if any(keyword in sql_lower for keyword in ['join', 'group by', 'having']):
            return 'medium'

        # Easy: 簡單查詢
        return 'easy'

    def augment_with_difficulty_weight(self, examples):
        """困難樣本過採樣"""

        # 難度權重配置
        difficulty_weights = {
            'easy': 1,
            'medium': 1,
            'hard': 2,      # Hard 樣本重複 4 次
            'extra': 3      # Extra 樣本重複 6 次
        }

        # 統計原始分佈
        difficulty_counts = {}
        for ex in examples:
            diff = ex.get('difficulty', 'medium')
            difficulty_counts[diff] = difficulty_counts.get(diff, 0) + 1

        print(f"\n   原始難度分佈:")
        for diff in ['easy', 'medium', 'hard', 'extra']:
            count = difficulty_counts.get(diff, 0)
            print(f"      {diff:8s}: {count:5d} 樣本")

        # 過採樣
        weighted_examples = []
        for ex in examples:
            difficulty = ex.get('difficulty', 'medium')
            weight = difficulty_weights.get(difficulty, 2)

            # 複製樣本
            for _ in range(weight):
                weighted_examples.append(ex.copy())  # 使用 copy 避免引用問題

        # 打亂順序
        random.shuffle(weighted_examples)

        # 統計增強後分佈
        augmented_counts = {}
        for ex in weighted_examples:
            diff = ex.get('difficulty', 'medium')
            augmented_counts[diff] = augmented_counts.get(diff, 0) + 1

        print(f"\n   增強後難度分佈:")
        for diff in ['easy', 'medium', 'hard', 'extra']:
            count = augmented_counts.get(diff, 0)
            print(f"      {diff:8s}: {count:5d} 樣本")

        print(f"\n   ✅ 資料增強完成: {len(examples)} → {len(weighted_examples)} 樣本")

        return weighted_examples