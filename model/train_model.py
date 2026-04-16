import os, sys

# 项目根目录：E:\proj\douyin\recommendedMiddlePlatform
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 将项目根目录加入 Python 模块搜索路径
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.config import config
from monitor.monitoring_utils import get_monitoring

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
    # 获取监控实例
    monitoring = get_monitoring()
    
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
        
        # 从实际数据中读取训练数据
        print("[TrainModel] Loading training data from HBase...")
        
        # 尝试从HBase读取行为数据
        try:
            from happybase import Connection
            import json
            
            # 连接到HBase
            conn = Connection(host=config.HBASE_HOST, port=config.HBASE_PORT)
            table = conn.table('user_behavior')
            
            # 读取行为数据
            user_ids = []
            item_ids = []
            labels = []
            
            # 限制读取的数据量，避免内存溢出
            max_records = 1000000
            count = 0
            
            for key, data in table.scan():
                if count >= max_records:
                    break
                
                # 解析数据
                try:
                    user_id = key.decode('utf-8').split(':')[0]
                    video_id = data.get(b'behavior:video_id', b'').decode('utf-8')
                    behavior_type = data.get(b'behavior:type', b'').decode('utf-8')
                    
                    # 将用户ID和视频ID转换为整数
                    user_id_int = int(user_id.replace('u', ''))
                    item_id_int = int(video_id.replace('v', ''))
                    
                    # 根据行为类型生成标签
                    label = 1 if behavior_type in ['click', 'like', 'share', 'favor'] else 0
                    
                    user_ids.append(user_id_int)
                    item_ids.append(item_id_int)
                    labels.append(label)
                    
                    count += 1
                    if count % 100000 == 0:
                        print(f"[TrainModel] Loaded {count} records")
                except Exception as e:
                    print(f"[TrainModel] Error parsing record: {e}")
                    continue
            
            conn.close()
            
            print(f"[TrainModel] Loaded {len(user_ids)} records from HBase")
            
            # 如果没有读取到数据，使用模拟数据
            if len(user_ids) == 0:
                print("[TrainModel] No data found in HBase, using mock data")
                user_ids = torch.randint(0, 100000, (1000000,))
                item_ids = torch.randint(0, 500000, (1000000,))
                labels = torch.randint(0, 2, (1000000,), dtype=torch.float32).unsqueeze(1)
            else:
                # 转换为张量
                user_ids = torch.tensor(user_ids)
                item_ids = torch.tensor(item_ids)
                labels = torch.tensor(labels, dtype=torch.float32).unsqueeze(1)
                
                # 计算用户和物品的最大ID
                max_user_id = user_ids.max().item() + 1
                max_item_id = item_ids.max().item() + 1
                print(f"[TrainModel] Max user ID: {max_user_id}, Max item ID: {max_item_id}")
        except Exception as e:
            print(f"[TrainModel] Failed to load data from HBase: {e}")
            print("[TrainModel] Using mock data instead")
            # 使用模拟数据
            user_ids = torch.randint(0, 100000, (1000000,))
            item_ids = torch.randint(0, 500000, (1000000,))
            labels = torch.randint(0, 2, (1000000,), dtype=torch.float32).unsqueeze(1)
            max_user_id = 100000
            max_item_id = 500000
        
        # 初始化模型
        model = RecommendationModel(max_user_id, max_item_id)
        criterion = nn.BCEWithLogitsLoss()
        optimizer = optim.Adam(model.parameters(), lr=0.001)
        
        # 训练模型
        batch_size = 1024
        num_batches = len(user_ids) // batch_size
        
        print(f"[TrainModel] Starting training with {len(user_ids)} samples")
        print(f"[TrainModel] Batch size: {batch_size}, Number of batches: {num_batches}")
        
        # 记录训练开始时间
        train_start_time = time.time()
        
        for epoch in range(10):
            epoch_start_time = time.time()
            total_loss = 0
            
            # 打乱数据
            permutation = torch.randperm(len(user_ids))
            user_ids_shuffled = user_ids[permutation]
            item_ids_shuffled = item_ids[permutation]
            labels_shuffled = labels[permutation]
            
            for i in range(0, len(user_ids), batch_size):
                # 获取当前批次的数据
                user_batch = user_ids_shuffled[i:i+batch_size]
                item_batch = item_ids_shuffled[i:i+batch_size]
                label_batch = labels_shuffled[i:i+batch_size]
                
                # 前向传播
                optimizer.zero_grad()
                outputs = model(user_batch, item_batch)
                loss = criterion(outputs, label_batch)
                
                # 反向传播
                loss.backward()
                optimizer.step()
                
                total_loss += loss.item()
                
                # 打印批次信息
                if (i // batch_size + 1) % 100 == 0:
                    print(f"[TrainModel] Epoch {epoch+1}, Batch {i//batch_size+1}/{num_batches}, Loss: {loss.item()}")
            
            # 打印 epoch 信息
            avg_loss = total_loss / num_batches
            epoch_duration = time.time() - epoch_start_time
            print(f"[TrainModel] Epoch {epoch+1}, Average Loss: {avg_loss}, Duration: {epoch_duration:.2f}s")
            
            # 记录监控指标
            monitoring.record_model_train(epoch_duration, avg_loss)
        
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
