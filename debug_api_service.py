import os, sys

# 项目根目录：E:\proj\douyin\recommendedMiddlePlatform
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# 将项目根目录加入 Python 模块搜索路径
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

print("Debugging api_service.py...")
print(f"Project root: {PROJECT_ROOT}")
print(f"Python path: {sys.path}")

try:
    # 测试导入 config 模块
    print("Testing config import...")
    from config.config import config
    print(f"Config imported successfully: {config.PROJECT_NAME}")
    
    # 测试导入 recommendation_service 模块
    print("\nTesting recommendation_service import...")
    from service.recommendation_service import recommendation_service
    print("recommendation_service imported successfully")
    
    # 测试导入 fastapi 模块
    print("\nTesting fastapi import...")
    try:
        from fastapi import FastAPI, Request
        import uvicorn
        print("FastAPI imported successfully")
    except ImportError as e:
        print(f"FastAPI import failed: {e}")
    
    # 测试导入 api_service 模块
    print("\nTesting api_service import...")
    from service.api_service import run_api_service
    print("api_service imported successfully")
    
    print("\nAll imports successful!")
    
    # 测试推荐服务
    print("\nTesting recommendation service...")
    test_data = {"user_id": "u00000001", "top_k": 3}
    result = recommendation_service.handle_recommend_request(test_data)
    print(f"Recommendation result: {result}")
    
    print("\nDebug completed successfully!")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

print("\nDebug script finished")