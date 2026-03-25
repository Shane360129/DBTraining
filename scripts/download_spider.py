"""
下載並設置 Spider 1.0 資料集
"""
import os
import subprocess
from pathlib import Path
import shutil


def setup_spider_dataset():
    """設置 Spider 資料集"""
    # 建立目錄
    data_dir = Path("./data")
    data_dir.mkdir(parents=True, exist_ok=True)

    spider_dir = data_dir / "spider"

    if spider_dir.exists():
        print("⏭️  Spider 目錄已存在")
        response = input("是否要重新下載? (y/n): ")
        if response.lower() != 'y':
            print("✅ 使用現有的 Spider 資料集")
            verify_files(spider_dir)
            return spider_dir

        # 刪除現有目錄
        shutil.rmtree(spider_dir)

    print("📥 開始從 GitHub 克隆 Spider 資料集...")
    print("   這可能需要幾分鐘...")

    try:
        # 使用 Git 克隆
        subprocess.run(
            ["git", "clone", "https://github.com/taoyds/spider.git", str(spider_dir)],
            check=True,
            cwd=data_dir
        )
        print("✅ Spider 資料集下載完成！")
    except subprocess.CalledProcessError as e:
        print(f"❌ Git 克隆失敗: {e}")
        print("\n備選方案：手動下載")
        print("1. 訪問: https://github.com/taoyds/spider")
        print("2. 點擊 'Code' > 'Download ZIP'")
        print("3. 解壓到 data/spider 目錄")
        return None
    except FileNotFoundError:
        print("❌ 未找到 Git 命令")
        print("\n請安裝 Git 或手動下載：")
        print("1. 訪問: https://github.com/taoyds/spider")
        print("2. 點擊 'Code' > 'Download ZIP'")
        print("3. 解壓到 data/spider 目錄")
        return None

    print(f"📁 資料集位置: {spider_dir}")
    verify_files(spider_dir)

    return spider_dir


def verify_files(spider_dir):
    """驗證必要文件"""
    required_files = [
        "train_spider.json",
        "train_others.json",
        "dev.json",
        "tables.json",
        "database",
        "evaluation.py"
    ]

    print("\n📋 驗證資料集文件...")
    all_exist = True
    for file in required_files:
        file_path = spider_dir / file
        if file_path.exists():
            print(f"  ✓ {file}")
        else:
            print(f"  ✗ {file} 缺失！")
            all_exist = False

    if all_exist:
        print("\n🎉 所有必要文件都已就緒！")
    else:
        print("\n⚠️  部分文件缺失，請檢查下載")

    return all_exist


if __name__ == "__main__":
    setup_spider_dataset()