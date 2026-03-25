"""
檢查 GPU 環境
"""
import torch
import sys


def check_gpu():
    print("=" * 50)
    print("🔍 GPU 環境檢查")
    print("=" * 50)

    print(f"\n📦 PyTorch 版本: {torch.__version__}")
    print(f"🐍 Python 版本: {sys.version.split()[0]}")

    cuda_available = torch.cuda.is_available()
    print(f"\n🎮 CUDA 可用: {cuda_available}")

    if cuda_available:
        print(f"   CUDA 版本: {torch.version.cuda}")
        print(f"   cuDNN 版本: {torch.backends.cudnn.version()}")
        print(f"\n🖥️  GPU 資訊:")
        print(f"   GPU 數量: {torch.cuda.device_count()}")

        for i in range(torch.cuda.device_count()):
            print(f"\n   GPU {i}:")
            print(f"   - 名稱: {torch.cuda.get_device_name(i)}")
            props = torch.cuda.get_device_properties(i)
            print(f"   - 總顯存: {props.total_memory / 1024 ** 3:.2f} GB")
            print(f"   - 計算能力: {props.major}.{props.minor}")

        # 測試 GPU
        print(f"\n🧪 GPU 測試:")
        try:
            x = torch.rand(1000, 1000).cuda()
            y = torch.rand(1000, 1000).cuda()
            z = torch.matmul(x, y)
            print(f"   ✅ GPU 計算測試通過")

            # 顯示當前顯存使用
            allocated = torch.cuda.memory_allocated(0) / 1024 ** 3
            reserved = torch.cuda.memory_reserved(0) / 1024 ** 3
            print(f"   已分配顯存: {allocated:.2f} GB")
            print(f"   已保留顯存: {reserved:.2f} GB")

            # 清理測試張量
            del x, y, z
            torch.cuda.empty_cache()

        except Exception as e:
            print(f"   ❌ GPU 計算測試失敗: {e}")
    else:
        print("\n⚠️  未檢測到 GPU")
        print("   請確認:")
        print("   1. NVIDIA 驅動已正確安裝")
        print("   2. CUDA Toolkit 已安裝")
        print("   3. PyTorch 安裝了正確的 CUDA 版本")
        print("\n   安裝支援 CUDA 的 PyTorch:")
        print("   pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121")

    print("\n" + "=" * 50)


if __name__ == "__main__":
    check_gpu()