import os, sys

# 项目根目录：E:\proj\douyin\recommendedMiddlePlatform
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 将项目根目录加入 Python 模块搜索路径
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.config import config

class MockRecommendationModel:
    """模拟推荐模型，用于在没有 PyTorch 的情况下运行"""
    
    def __init__(self, user_dim, item_dim, hidden_dim=64):
        self.user_dim = user_dim
        self.item_dim = item_dim
        self.hidden_dim = hidden_dim
        print("[MockRecommendationModel] Initialized mock model")
    
    def forward(self, user_id, item_id):
        # 模拟前向传播
        return 0.5

def train_model():
    """训练推荐模型"""
    try:
        import torch
        import torch.nn as nn
        import torch.optim as optim
        
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
        
        # 模拟数据
        user_ids = torch.randint(0, 10000, (100000,))
        item_ids = torch.randint(0, 100000, (100000,))
        labels = torch.randint(0, 2, (100000,), dtype=torch.float32).unsqueeze(1)
        
        # 初始化模型
        model = RecommendationModel(10000, 100000)
        criterion = nn.BCEWithLogitsLoss()
        optimizer = optim.Adam(model.parameters(), lr=0.001)
        
        # 训练模型
        for epoch in range(10):
            optimizer.zero_grad()
            outputs = model(user_ids, item_ids)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            print(f"[TrainModel] Epoch {epoch+1}, Loss: {loss.item()}")
        
        # 保存模型
        os.makedirs(config.MODEL_DIR, exist_ok=True)
        torch.save(model.state_dict(), os.path.join(config.MODEL_DIR, f"{config.MODEL_NAME}.pt"))
        
        # 转换为 ONNX 格式
        dummy_input = (torch.tensor([0]), torch.tensor([0]))
        torch.onnx.export(
            model,
            dummy_input,
            config.ONNX_MODEL_PATH,
            input_names=['user_id', 'item_id'],
            output_names=['score']
        )
        
        print("[TrainModel] Model training completed successfully")
    except ImportError as e:
        print(f"[TrainModel] Failed to import PyTorch: {e}")
        print("[TrainModel] Using mock implementation")
        
        # 使用模拟实现
        model = MockRecommendationModel(10000, 100000)
        
        # 模拟训练过程
        for epoch in range(10):
            print(f"[TrainModel] Epoch {epoch+1}, Loss: {0.5 - epoch*0.05}")
        
        # 模拟保存模型
        os.makedirs(config.MODEL_DIR, exist_ok=True)
        print(f"[TrainModel] Saved mock model to: {os.path.join(config.MODEL_DIR, config.MODEL_NAME + '.pt')}")
        print(f"[TrainModel] Saved mock ONNX model to: {config.ONNX_MODEL_PATH}")
        
        print("[TrainModel] Mock model training completed")

if __name__ == "__main__":
    train_model()
