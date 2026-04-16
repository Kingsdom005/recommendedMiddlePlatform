import os, sys

# 项目根目录：E:\proj\douyin\recommendedMiddlePlatform
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 将项目根目录加入 Python 模块搜索路径
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.config import config
import time

try:
    from fastapi import FastAPI, Request
    from service.recommendation_service import recommendation_service
    import uvicorn
    
    app = FastAPI(
        title=config.PROJECT_NAME,
        version=config.VERSION
    )
    
    @app.post("/api/recommend")
    async def recommend(request: Request):
        """获取推荐列表"""
        start_time = time.time()
        data = await request.json()
        recommendations = recommendation_service.handle_recommend_request(data)
        end_time = time.time()
        return {
            "code": 200,
            "data": recommendations,
            "time": end_time - start_time
        }
    
    @app.get("/api/health")
    async def health_check():
        """健康检查"""
        return {"status": "ok"}
    
    def run_api_service():
        """运行 API 服务"""
        # 使用8081端口，因为8080已被占用
        port = 8081
        print(f"[APIService] Starting API service at {config.SERVICE_HOST}:{port}")
        uvicorn.run(
            "service.api_service:app",
            host=config.SERVICE_HOST,
            port=port,
            reload=True
        )
except ImportError as e:
    print(f"[APIService] Failed to import FastAPI: {e}")
    print("[APIService] Using mock implementation")
    
    class MockRecommendationService:
        """模拟推荐服务"""
        def handle_recommend_request(self, data):
            # 模拟推荐结果
            return {
                "user_id": data.get("user_id", "u00000001"),
                "recommendations": [
                    {"item_id": "v00000001", "score": 0.95},
                    {"item_id": "v00000002", "score": 0.90},
                    {"item_id": "v00000003", "score": 0.85}
                ]
            }
    
    recommendation_service = MockRecommendationService()
    
    def run_api_service():
        """运行 API 服务（模拟）"""
        # 使用8081端口，因为8080已被占用
        port = 8081
        print(f"[APIService] Starting mock API service at {config.SERVICE_HOST}:{port}")
        print("[APIService] Available endpoints:")
        print("  POST /api/recommend - Get recommendations")
        print("  GET /api/health - Health check")
        print("[APIService] Mock API service started")
        # 模拟服务运行
        while True:
            time.sleep(1)

if __name__ == "__main__":
    print("Starting api_service.py...")
    try:
        run_api_service()
    except Exception as e:
        print(f"Error running API service: {e}")
        import traceback
        traceback.print_exc()
