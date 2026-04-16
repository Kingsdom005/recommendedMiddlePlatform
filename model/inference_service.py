from config.config import config

try:
    import onnxruntime as rt
    import numpy as np
    
    class InferenceService:
        def __init__(self):
            # 加载 ONNX 模型
            self.sess = rt.InferenceSession(config.ONNX_MODEL_PATH)
            self.input_user = self.sess.get_inputs()[0].name
            self.input_item = self.sess.get_inputs()[1].name
            self.output = self.sess.get_outputs()[0].name
        
        def predict(self, user_id, item_id):
            """预测用户对物品的兴趣度"""
            user_id = np.array([user_id], dtype=np.int64)
            item_id = np.array([item_id], dtype=np.int64)
            result = self.sess.run([self.output], {
                self.input_user: user_id,
                self.input_item: item_id
            })
            return float(result[0][0])
        
        def batch_predict(self, user_id, item_ids):
            """批量预测用户对多个物品的兴趣度"""
            user_ids = np.array([user_id] * len(item_ids), dtype=np.int64)
            item_ids = np.array(item_ids, dtype=np.int64)
            results = self.sess.run([self.output], {
                self.input_user: user_ids,
                self.input_item: item_ids
            })
            return [float(score) for score in results[0].flatten()]
    
    inference_service = InferenceService()
    print("[InferenceService] Initialized with ONNX Runtime")
except ImportError as e:
    print(f"[InferenceService] Failed to import dependencies: {e}")
    print("[InferenceService] Using mock implementation")
    
    class MockInferenceService:
        """模拟推理服务"""
        def __init__(self):
            print("[MockInferenceService] Initialized mock inference service")
        
        def predict(self, user_id, item_id):
            """预测用户对物品的兴趣度（模拟）"""
            # 模拟预测结果，基于用户和物品的ID生成一个分数
            return 0.5 + (hash(str(user_id) + str(item_id)) % 50) / 100.0
        
        def batch_predict(self, user_id, item_ids):
            """批量预测用户对多个物品的兴趣度（模拟）"""
            # 模拟批量预测结果
            return [0.5 + (hash(str(user_id) + str(item_id)) % 50) / 100.0 for item_id in item_ids]
    
    inference_service = MockInferenceService()
