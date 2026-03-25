"""
WP_M09 Web 查詢介面
"""
import gradio as gr
import torch
import json
import re
from pathlib import Path
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from peft import PeftModel

# 全域變數
model = None
tokenizer = None
tables = []
columns = []
column_types = []


def load_model():
    """載入模型"""
    global model, tokenizer, tables, columns, column_types

    MODEL_PATH = "outputs/models/wp_m09_from_scratch/final_model"
    SCHEMA_FILE = "data/wp_m09/tables.json"

    print("🔧 載入模型...")

    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

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

    model = PeftModel.from_pretrained(base_model, MODEL_PATH)
    model.eval()

    # 載入 schema
    with open(SCHEMA_FILE, 'r', encoding='utf-8') as f:
        schema = json.load(f)[0]

    tables = schema['table_names_original']
    columns = schema['column_names_original']
    column_types = schema['column_types']

    print("✅ 模型載入完成")


def build_schema(table_name):
    """構建 schema"""
    if table_name not in tables:
        return ""

    table_idx = tables.index(table_name)
    table_cols = []

    for col_idx, (t_idx, col_name) in enumerate(columns):
        if t_idx == table_idx:
            col_type = column_types[col_idx]
            table_cols.append(f"{col_name} ({col_type})")

    if len(table_cols) <= 15:
        cols_str = ', '.join(table_cols)
    else:
        cols_str = ', '.join(table_cols[:15]) + f', ... ({len(table_cols) - 15} more)'

    return f"Table {table_name}: {cols_str}"


def generate_sql(question, table_name):
    """生成 SQL"""
    if not question or not table_name:
        return "請輸入問題和選擇表"

    schema = build_schema(table_name)

    if not schema:
        return f"找不到表: {table_name}"

    prompt = f"Schema: {schema}\nQuestion: {question}\nSQL:"

    inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=512).to(model.device)

    with torch.no_grad():
        outputs = model.generate(**inputs, max_new_tokens=128, do_sample=False, pad_token_id=tokenizer.pad_token_id)

    sql = tokenizer.decode(outputs[0][inputs['input_ids'].shape[1]:], skip_special_tokens=True).strip()

    # 清理
    sql = re.sub(r'```sql\s*', '', sql)
    sql = re.sub(r'```\s*', '', sql)
    sql = sql.split('\n')[0].strip()

    return sql


# 載入模型
load_model()

# 創建介面
with gr.Blocks(title="WP_M09 SQL 查詢") as demo:
    gr.Markdown("# 🤖 WP_M09 SQL 查詢助手")
    gr.Markdown("輸入自然語言問題，自動生成 SQL 查詢")

    with gr.Row():
        with gr.Column():
            table_dropdown = gr.Dropdown(
                choices=tables,
                label="選擇資料表",
                value=tables[0] if tables else None
            )

            question_input = gr.Textbox(
                label="輸入問題（自然語言）",
                placeholder="例如: How many products do we have?",
                lines=2
            )

            submit_btn = gr.Button("生成 SQL", variant="primary")

        with gr.Column():
            sql_output = gr.Textbox(
                label="生成的 SQL",
                lines=5,
                show_copy_button=True
            )

    # 範例
    gr.Examples(
        examples=[
            ["WP_Product", "How many products do we have?"],
            ["WP_Product", "What is the average price?"],
            ["WP_Product", "Show me items that are out of stock"],
            ["WP_vReceipt", "What is the total amount?"],
            ["WP_vPayment", "Show recent payments"],
        ],
        inputs=[table_dropdown, question_input]
    )

    submit_btn.click(
        fn=generate_sql,
        inputs=[question_input, table_dropdown],
        outputs=sql_output
    )

if __name__ == "__main__":
    demo.launch(share=False)