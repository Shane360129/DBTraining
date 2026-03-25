"""
WP_M09 評估腳本（自動使用最新日期模型）
使用 test.json 進行評估
"""
import torch
import json
import re
from pathlib import Path
from datetime import datetime
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from peft import PeftModel
from tqdm import tqdm

class WPEvaluator:
    def __init__(self, model_path, schema_file):
        """初始化評估器"""
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

    def build_schema_for_table(self, table_name):
        """為特定表構建 schema 描述"""
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

    def generate_sql(self, question, table_name):
        """生成 SQL"""
        schema = self.build_schema_for_table(table_name)

        if not schema:
            return None

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
                max_new_tokens=64,
                do_sample=False,
                pad_token_id=self.tokenizer.pad_token_id,
            )

        sql = self.tokenizer.decode(
            outputs[0][inputs['input_ids'].shape[1]:],
            skip_special_tokens=True
        ).strip()

        # 清理
        sql = re.sub(r'```sql\s*', '', sql)
        sql = re.sub(r'```\s*', '', sql)
        sql = sql.split('\n')[0].strip()

        return sql

    def normalize_sql(self, sql):
        """標準化 SQL（用於比較）"""
        # 轉大寫
        sql = sql.upper()

        # 移除多餘空格
        sql = re.sub(r'\s+', ' ', sql)

        # 移除前後空格
        sql = sql.strip()

        return sql

    def exact_match(self, pred_sql, gold_sql):
        """計算 Exact Match"""
        pred_norm = self.normalize_sql(pred_sql)
        gold_norm = self.normalize_sql(gold_sql)

        return pred_norm == gold_norm

    def evaluate(self, test_file, output_file=None):
        """評估測試集"""
        print(f"📊 載入測試集: {test_file}")

        with open(test_file, 'r', encoding='utf-8') as f:
            test_data = json.load(f)

        print(f"✅ 測試樣本數: {len(test_data)}\n")

        results = []
        exact_match_count = 0

        # 按難度和類型統計
        stats_by_difficulty = {}
        stats_by_type = {}
        stats_by_table = {}

        print("🔄 開始評估...\n")

        for item in tqdm(test_data, desc="評估進度"):
            question = item['question']
            gold_sql = item['query']
            table_name = item['table']
            difficulty = item.get('difficulty', 'unknown')
            query_type = item.get('query_type', 'unknown')

            # 生成 SQL
            pred_sql = self.generate_sql(question, table_name)

            if not pred_sql:
                pred_sql = "FAILED TO GENERATE"

            # Exact Match
            em = self.exact_match(pred_sql, gold_sql)
            if em:
                exact_match_count += 1

            # 記錄結果
            result = {
                'question': question,
                'gold_sql': gold_sql,
                'pred_sql': pred_sql,
                'exact_match': em,
                'table': table_name,
                'difficulty': difficulty,
                'query_type': query_type
            }

            results.append(result)

            # 統計 - 按難度
            if difficulty not in stats_by_difficulty:
                stats_by_difficulty[difficulty] = {'total': 0, 'em': 0}
            stats_by_difficulty[difficulty]['total'] += 1
            if em:
                stats_by_difficulty[difficulty]['em'] += 1

            # 統計 - 按類型
            if query_type not in stats_by_type:
                stats_by_type[query_type] = {'total': 0, 'em': 0}
            stats_by_type[query_type]['total'] += 1
            if em:
                stats_by_type[query_type]['em'] += 1

            # 統計 - 按表
            if table_name not in stats_by_table:
                stats_by_table[table_name] = {'total': 0, 'em': 0}
            stats_by_table[table_name]['total'] += 1
            if em:
                stats_by_table[table_name]['em'] += 1

        # 計算總體指標
        total = len(test_data)
        em_accuracy = exact_match_count / total * 100 if total > 0 else 0

        # 顯示結果
        print("\n" + "="*70)
        print("📊 評估結果")
        print("="*70)

        print(f"\n總體指標:")
        print(f"  測試樣本數: {total}")
        print(f"  Exact Match (EM): {exact_match_count}/{total} = {em_accuracy:.2f}%")

        # 按難度顯示
        if stats_by_difficulty:
            print(f"\n按難度分析:")
            for diff in ['easy', 'medium', 'hard']:
                if diff in stats_by_difficulty:
                    stats = stats_by_difficulty[diff]
                    em_rate = stats['em'] / stats['total'] * 100 if stats['total'] > 0 else 0
                    print(f"  {diff.capitalize()}: EM={stats['em']}/{stats['total']} ({em_rate:.1f}%)")

        # 按查詢類型顯示
        if stats_by_type:
            print(f"\n按查詢類型分析:")
            for qtype, stats in sorted(stats_by_type.items()):
                if stats['total'] > 0:
                    em_rate = stats['em'] / stats['total'] * 100
                    print(f"  {qtype}: EM={stats['em']}/{stats['total']} ({em_rate:.1f}%)")

        # 按表顯示（只顯示前 10）
        if stats_by_table:
            print(f"\n按表分析（前 10）:")
            sorted_tables = sorted(stats_by_table.items(), key=lambda x: x[1]['total'], reverse=True)
            for table, stats in sorted_tables[:10]:
                if stats['total'] > 0:
                    em_rate = stats['em'] / stats['total'] * 100
                    print(f"  {table}: EM={stats['em']}/{stats['total']} ({em_rate:.1f}%)")

        # 顯示錯誤案例（前 10 個）
        print(f"\n❌ 錯誤案例（前 10 個）:")
        error_count = 0
        for result in results:
            if not result['exact_match'] and error_count < 10:
                error_count += 1
                print(f"\n{error_count}. Question: {result['question']}")
                print(f"   Table: {result['table']}")
                print(f"   Expected: {result['gold_sql']}")
                print(f"   Got:      {result['pred_sql']}")

        if error_count == 0:
            print(f"   沒有錯誤！🎉")

        # 保存詳細結果
        if output_file:
            evaluation_results = {
                'overall': {
                    'total': total,
                    'exact_match': exact_match_count,
                    'exact_match_accuracy': em_accuracy,
                },
                'by_difficulty': stats_by_difficulty,
                'by_query_type': stats_by_type,
                'by_table': stats_by_table,
                'detailed_results': results
            }

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(evaluation_results, f, indent=2, ensure_ascii=False)

            print(f"\n💾 詳細結果已保存: {output_file}")

        print("\n" + "="*70)

        return results

def find_latest_model():
    """查找最新的模型"""
    today = datetime.now().strftime("%m%d")
    MODEL_PATH = f"outputs/models/wp_m09_{today}/final_model"

    # 如果今天的權重存在
    if Path(MODEL_PATH).exists():
        print(f"✅ 使用今天的模型: wp_m09_{today}")
        return MODEL_PATH, today

    # 查找所有可用的模型
    print(f"⚠️  今天的模型不存在: wp_m09_{today}")
    print(f"\n🔍 搜尋可用的模型權重...")

    models_dir = Path("outputs/models")
    if not models_dir.exists():
        print(f"❌ models 目錄不存在")
        return None, None

    wp_models = sorted(models_dir.glob("wp_m09_*/final_model"), reverse=True)

    if not wp_models:
        print(f"❌ 找不到任何 wp_m09 模型")
        return None, None

    print(f"\n可用的模型權重:")
    for i, model_path in enumerate(wp_models, 1):
        date = model_path.parent.name.replace("wp_m09_", "")
        print(f"   {i}. wp_m09_{date}")

    # 使用最新的
    latest_model = str(wp_models[0])
    latest_date = wp_models[0].parent.name.replace("wp_m09_", "")

    print(f"\n✅ 自動使用最新的模型: wp_m09_{latest_date}")

    return latest_model, latest_date

if __name__ == "__main__":
    print("="*70)
    print("🧪 WP_M09 模型評估")
    print("="*70)
    print()

    # 查找模型
    MODEL_PATH, model_date = find_latest_model()

    if not MODEL_PATH:
        print(f"\n請先訓練模型: python train_wp_m09.py")
        exit()

    SCHEMA_FILE = "data/wp_m09/tables.json"
    TEST_FILE = "data/wp_m09/test.json"
    OUTPUT_FILE = f"outputs/evaluation_results_{model_date}.json"

    # 檢查文件
    if not Path(SCHEMA_FILE).exists():
        print(f"❌ 找不到 schema: {SCHEMA_FILE}")
        exit()

    if not Path(TEST_FILE).exists():
        print(f"❌ 找不到測試集: {TEST_FILE}")
        print(f"請先創建測試集: python create_wp_m09_test_set.py")
        exit()

    print(f"\n📁 使用的文件:")
    print(f"   模型: {MODEL_PATH}")
    print(f"   Schema: {SCHEMA_FILE}")
    print(f"   測試集: {TEST_FILE}")
    print(f"   輸出: {OUTPUT_FILE}")
    print()

    # 創建評估器
    evaluator = WPEvaluator(MODEL_PATH, SCHEMA_FILE)

    # 執行評估
    evaluator.evaluate(TEST_FILE, OUTPUT_FILE)

    print("\n✅ 評估完成！")