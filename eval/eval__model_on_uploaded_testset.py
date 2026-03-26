"""
使用上傳的 test.json 評估模型
"""
import torch
import json
import re
from pathlib import Path
from datetime import datetime
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from peft import PeftModel
from tqdm import tqdm

class UploadedDataEvaluator:
    def __init__(self, model_path, schema_file=None):
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

        # 載入 schema（如果有）
        self.schema = None
        if schema_file and Path(schema_file).exists():
            with open(schema_file, 'r', encoding='utf-8') as f:
                self.schema = json.load(f)[0]

            self.tables = self.schema['table_names_original']
            self.columns = self.schema['column_names_original']
            self.column_types = self.schema['column_types']

            print("✅ Schema 已載入")

        print("✅ 模型載入完成\n")

    def build_schema_for_table(self, table_name):
        """為特定表構建 schema 描述"""
        if not self.schema or table_name not in self.tables:
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
        if self.schema:
            schema_desc = self.build_schema_for_table(table_name)
            if schema_desc:
                prompt = f"Schema: {schema_desc}\nQuestion: {question}\nSQL:"
            else:
                prompt = f"Question: {question}\nSQL:"
        else:
            prompt = f"Question: {question}\nSQL:"

        inputs = self.tokenizer(
            prompt,
            return_tensors="pt",
            truncation=True,
            max_length=512
        ).to(self.model.device)

        with torch.no_grad():
            outputs = self.model.generate(
                **inputs,
                max_new_tokens=128,
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
        sql = sql.upper()
        sql = re.sub(r'\s+', ' ', sql)
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

        # 統計
        stats_by_difficulty = {}
        stats_by_table = {}

        print("🔄 開始評估...\n")

        for item in tqdm(test_data, desc="評估進度"):
            question = item['question']
            gold_sql = item['query']
            table_name = item.get('table', 'unknown')
            difficulty = item.get('difficulty', 'unknown')

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
                'difficulty': difficulty
            }

            results.append(result)

            # 統計 - 按難度
            if difficulty not in stats_by_difficulty:
                stats_by_difficulty[difficulty] = {'total': 0, 'em': 0}
            stats_by_difficulty[difficulty]['total'] += 1
            if em:
                stats_by_difficulty[difficulty]['em'] += 1

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

        # 按表顯示（前 10）
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
                'by_table': stats_by_table,
                'detailed_results': results
            }

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(evaluation_results, f, indent=2, ensure_ascii=False)

            print(f"\n💾 詳細結果已保存: {output_file}")

        print("\n" + "="*70)

        return results

def find_latest_uploaded_model():
    """查找最新的 uploaded 模型"""
    today = datetime.now().strftime("%m%d")
    MODEL_PATH = f"outputs/models/wp_m09_uploaded_{today}/final_model"

    # 如果今天的權重存在
    if Path(MODEL_PATH).exists():
        print(f"✅ 使用今天的模型: wp_m09_uploaded_{today}")
        return MODEL_PATH, today

    # 查找所有可用的 uploaded 模型
    print(f"⚠️  今天的模型不存在: wp_m09_uploaded_{today}")
    print(f"\n🔍 搜尋可用的模型權重...")

    models_dir = Path("outputs/models")
    if not models_dir.exists():
        print(f"❌ models 目錄不存在")
        return None, None

    uploaded_models = sorted(models_dir.glob("wp_m09_uploaded_*/final_model"), reverse=True)

    if not uploaded_models:
        print(f"❌ 找不到任何 wp_m09_uploaded 模型")
        return None, None

    print(f"\n可用的模型權重:")
    for i, model_path in enumerate(uploaded_models, 1):
        date = model_path.parent.name.replace("wp_m09_uploaded_", "")
        print(f"   {i}. wp_m09_uploaded_{date}")

    # 使用最新的
    latest_model = str(uploaded_models[0])
    latest_date = uploaded_models[0].parent.name.replace("wp_m09_uploaded_", "")

    print(f"\n✅ 自動使用最新的模型: wp_m09_uploaded_{latest_date}")

    return latest_model, latest_date

if __name__ == "__main__":
    print("="*70)
    print("🧪 WP_M09 模型評估（使用上傳的 test.json）")
    print("="*70)
    print()

    # 查找模型
    MODEL_PATH, model_date = find_latest_uploaded_model()

    if not MODEL_PATH:
        print(f"\n請先訓練模型: python train_uploaded_data.py")
        exit()

    # 尋找測試文件
    print(f"📋 尋找測試文件...")

    possible_paths = [
        "test.json",  # 當前目錄
        "data/wp_m09/test_.json",  # data 目錄
        "data/wp_m09/test_uploaded.json",  # 上傳後的位置
    ]

    test_file = None
    for path in possible_paths:
        if Path(path).exists():
            test_file = path
            print(f"✅ 找到測試文件: {path}")
            break

    if not test_file:
        print(f"\n❌ 找不到測試文件！")
        print(f"\n請將 test.json 放在以下任一位置:")
        for path in possible_paths:
            print(f"   - {path}")
        exit()

    SCHEMA_FILE = "data/wp_m09/tables.json"
    OUTPUT_FILE = f"outputs/evaluation_uploaded_{model_date}.json"

    print(f"\n📁 使用的文件:")
    print(f"   模型: {MODEL_PATH}")
    print(f"   測試集: {test_file}")
    print(f"   輸出: {OUTPUT_FILE}")
    print()

    # 創建評估器
    evaluator = UploadedDataEvaluator(MODEL_PATH, SCHEMA_FILE)

    # 執行評估
    evaluator.evaluate(test_file, OUTPUT_FILE)

    print("\n✅ 評估完成！")