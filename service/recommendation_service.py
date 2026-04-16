from model.inference_service import inference_service
from feature_store.redis_utils import redis_utils
from feature_store.elasticsearch_utils import es_utils
from config.config import config
import json
import time

class RecommendationService:
    def __init__(self):
        pass
    
    def get_recommendations(self, user_id, top_k=10):
        """获取用户推荐列表"""
        # 1. 获取用户特征
        user_features = redis_utils.get_user_features(user_id)
        
        # 2. 从 ES 中获取候选视频
        query = {
            'match_all': {}
        }
        candidate_videos = es_utils.search_videos(query, size=100)
        
        # 3. 计算每个视频的推荐分数
        video_scores = []
        for video in candidate_videos:
            score = inference_service.predict(int(user_id), int(video['id']))
            video_scores.append((video, score))
        
        # 4. 排序并返回 Top K
        video_scores.sort(key=lambda x: x[1], reverse=True)
        recommendations = [video for video, score in video_scores[:top_k]]
        
        # 5. 记录推荐结果（模拟）
        print(f"[RecommendationService] Recorded recommendation for user {user_id}: {[video['id'] for video in recommendations]}")
        
        return recommendations
    
    def handle_recommend_request(self, request):
        """处理推荐请求"""
        user_id = request.get('user_id')
        top_k = request.get('top_k', 10)
        return self.get_recommendations(user_id, top_k)

recommendation_service = RecommendationService()
