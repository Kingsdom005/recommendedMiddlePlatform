import os, sys

# 项目根目录：E:\proj\douyin\recommendedMiddlePlatform
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# 将项目根目录加入 Python 模块搜索路径
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

print("Testing api_service.py...")
try:
    from service.api_service import run_api_service
    print("Successfully imported api_service.py")
    print("API service is ready to run")
except Exception as e:
    print(f"Error importing api_service.py: {e}")

print("Test completed")