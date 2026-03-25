"""
自動從最新檢查點恢復訓練
"""
import subprocess
from pathlib import Path

# 找到最新檢查點
model_dir = Path("outputs/models/spider1-llama31-dora-v3")
checkpoints = sorted(
    model_dir.glob("checkpoint-*"),
    key=lambda p: int(p.name.split("-")[1])
)

if checkpoints:
    latest = checkpoints[-1]
    print(f"✅ 找到最新檢查點: {latest}")

    # 從檢查點恢復
    cmd = f"python scripts/train.py --resume_from_checkpoint {latest}"
    print(f"\n執行命令: {cmd}\n")

    subprocess.run(cmd, shell=True)
else:
    print("❌ 找不到檢查點，重新開始訓練")
    subprocess.run("python scripts/train.py", shell=True)