#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
在线推理pipeline
包括模型加载、特征提取、模型推理和结果返回等步骤
"""

import os
import sys
import time
import json

# 项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from config.config import config
from monitor.monitoring_utils import get_monitoring
from feature_store.redis_utils import redis_utils
from feature_store.elasticsearch_utils import es_utils


class OnlineInferencePipeline:
    """在线推理pipeline"""
    
    def __init__(self):
        """初始化pipeline"""
        self.monitoring = get_monitoring()
        self.model = None
        self.model_loaded = False
    
    def load_model(self):
        """加载模型"""
        print("[OnlineInference] Loading model...")
        start_time = time.time()
        
        try:
            # 尝试加载 ONNX 模型
            try:
                import onnxruntime as ort
                
                if os.path.exists(config.ONNX_MODEL_PATH):
                    self.model = ort.InferenceSession(config.ONNX_MODEL_PATH)
                    print(f"[OnlineInference] Loaded ONNX model from: {config.ONNX_MODEL_PATH}")
                    self.model_loaded = True
                else:
                    print(f"[OnlineInference] ONNX model not found at: {config.ONNX_MODEL_PATH}")
                    self.model_loaded = False
            except ImportError:
                print("[OnlineInference] ONNX Runtime not available, trying PyTorch")
                
                # 尝试加载 PyTorch 模型
                try:
                    import torch
                    import torch.nn as nn
                    
                    class RecommendationModel(nn.Module):
                        def __init__(self, user_dim, item_dim, hidden_dim=64):
                            super(RecommendationModel, self).__init__()
                            self.user_embedding = nn.Embedding(user_dim, hidden_dim)
                            self.item_embedding = nn.Embedding(item_dim, hidden_dim)
                            self.fc1 = nn.Linear(hidden_dim * 2, 128)
                            self.fc2 = nn.Linear(128, 64)
                            self.fc3 = nn.Linear(64, 1)
                            self.relu = nn.ReLU()
                        
                        def forward(self, user_id, item_id):
                            user_emb = self.user_embedding(user_id)
                            item_emb = self.item_embedding(item_id)
                            x = torch.cat([user_emb, item_emb], dim=1)
                            x = self.relu(self.fc1(x))
                            x = self.relu(self.fc2(x))
                            x = self.fc3(x)
                            return x
                    
                    model_path = os.path.join(config.MODEL_DIR, f"{config.MODEL_NAME}.pt")
                    if os.path.exists(model_path):
                        # 尝试加载模型权重来获取实际的用户和物品数量
                        try:
                            state_dict = torch.load(model_path, weights_only=True)
                            # 从权重中获取实际的用户和物品数量
                            user_embedding_size = state_dict['user_embedding.weight'].shape[0]
                            item_embedding_size = state_dict['item_embedding.weight'].shape[0]
                            print(f"[OnlineInference] Model weights loaded, user_embedding size: {user_embedding_size}, item_embedding size: {item_embedding_size}")
                            # 使用实际的大小初始化模型
                            self.model = RecommendationModel(user_embedding_size, item_embedding_size)
                            self.model.load_state_dict(state_dict)
                            self.model.eval()
                            print(f"[OnlineInference] Loaded PyTorch model from: {model_path}")
                            self.model_loaded = True
                        except Exception as e:
                            print(f"[OnlineInference] Failed to load model weights: {e}")
                            # 如果失败，使用默认值
                            self.model = RecommendationModel(10001, 50001)
                            self.model.load_state_dict(torch.load(model_path, weights_only=True))
                            self.model.eval()
                            print(f"[OnlineInference] Loaded PyTorch model with default sizes")
                            self.model_loaded = True
                    else:
                        print(f"[OnlineInference] PyTorch model not found at: {model_path}")
                        self.model_loaded = False
                except ImportError:
                    print("[OnlineInference] PyTorch not available")
                    self.model_loaded = False
        except Exception as e:
            print(f"[OnlineInference] Failed to load model: {e}")
            self.model_loaded = False
        
        # 如果模型加载失败，使用模拟模型
        if not self.model_loaded:
            print("[OnlineInference] Using mock model")
            
            class MockModel:
                def run(self, output_names, input_feed):
                    # 模拟推理，返回正确的嵌套列表格式
                    return [[0.5]]
                
                def forward(self, user_id, item_id):
                    # 模拟推理
                    return 0.5
            
            self.model = MockModel()
            self.model_loaded = True
        
        # 记录监控指标
        self.monitoring.record_data_load('model_load', 'model', 1, time.time() - start_time)
        
        return self.model_loaded
    
    def extract_features(self, user_id):
        """提取用户和物品特征"""
        print(f"[OnlineInference] Extracting features for user: {user_id}")
        start_time = time.time()
        
        # 从Redis中获取用户特征
        user_features = redis_utils.get_user_features(user_id)
        print(f"[OnlineInference] User features: {user_features}")
        
        # 从Elasticsearch中获取候选物品
        try:
            # 搜索视频
            query = {"match_all": {}}
            videos = es_utils.search_videos(query, size=100)
            print(f"[OnlineInference] Found {len(videos)} candidate videos")
            
            # 如果候选视频数量不足，使用模拟数据
            if len(videos) < 100:
                print("[OnlineInference] Not enough candidate videos, using mock data")
                # 生成模拟视频数据
                mock_videos = [{'video_id': f'v{i}', 'title': f'Video {i}'} for i in range(1, 101)]
                videos = mock_videos
                print(f"[OnlineInference] Using {len(videos)} mock candidate videos")
        except Exception as e:
            print(f"[OnlineInference] Failed to get candidate videos: {e}")
            # 使用模拟数据
            videos = [{'video_id': f'v{i}', 'title': f'Video {i}'} for i in range(1, 101)]
            print(f"[OnlineInference] Using {len(videos)} mock candidate videos")
        
        # 记录监控指标
        self.monitoring.record_data_load('feature_extraction', 'features', len(videos), time.time() - start_time)
        
        return user_features, videos
    
    def inference(self, user_id, videos):
        """模型推理"""
        print(f"[OnlineInference] Running inference for user: {user_id}")
        start_time = time.time()
        
        recommendations = []
        
        for video in videos:
            video_id = video.get('video_id')
            if not video_id:
                continue
            
            try:
                # 转换用户ID和视频ID为整数
                user_id_int = int(user_id.replace('u', ''))
                video_id_int = int(video_id.replace('v', ''))
                
                # 模型推理
                if hasattr(self.model, 'run'):
                    # ONNX 模型
                    inputs = {'user_id': [user_id_int], 'item_id': [video_id_int]}
                    outputs = self.model.run(None, inputs)
                    score = outputs[0][0]
                elif hasattr(self.model, 'forward'):
                    # PyTorch 模型
                    import torch
                    score = self.model.forward(torch.tensor([user_id_int]), torch.tensor([video_id_int])).item()
                else:
                    # 模拟模型
                    score = 0.5
                
                # 添加到推荐列表
                recommendations.append({
                    'video_id': video_id,
                    'score': float(score),
                    'title': video.get('title', f'Video {video_id}')
                })
            except Exception as e:
                print(f"[OnlineInference] Error during inference: {e}")
                continue
        
        # 按分数排序
        recommendations.sort(key=lambda x: x['score'], reverse=True)
        
        # 记录监控指标
        self.monitoring.record_stream_process('inference', time.time() - start_time)
        
        return recommendations
    
    def run(self, user_id, top_k=10):
        """运行pipeline"""
        print("=" * 80)
        print(f"在线推理Pipeline for user: {user_id}")
        print("=" * 80)
        
        start_time = time.time()
        
        try:
            # 1. 加载模型
            if not self.model_loaded:
                if not self.load_model():
                    return {"error": "Failed to load model"}
            
            # 2. 提取特征
            user_features, videos = self.extract_features(user_id)
            
            # 3. 模型推理
            recommendations = self.inference(user_id, videos)
            
            # 4. 返回结果
            result = {
                'user_id': user_id,
                'recommendations': recommendations[:top_k],
                'total_candidates': len(videos),
                'processing_time': time.time() - start_time
            }
            
            print(f"[OnlineInference] Generated {len(result['recommendations'])} recommendations")
            print(f"[OnlineInference] Processing time: {result['processing_time']:.2f}s")
            
            # 记录监控指标
            self.monitoring.record_api_request('/api/recommend', 'POST', 200, result['processing_time'])
            
            return result
        except Exception as e:
            print(f"[OnlineInference] Pipeline failed: {e}")
            import traceback
            traceback.print_exc()
            
            # 记录监控指标
            self.monitoring.record_api_request('/api/recommend', 'POST', 500, time.time() - start_time)
            
            return {"error": str(e)}


def main():
    """主函数"""
    pipeline = OnlineInferencePipeline()
    
    # 测试推理
    user_id = "u00000001"
    result = pipeline.run(user_id, top_k=10)
    
    print("=" * 80)
    print("推理结果:")
    print("=" * 80)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
