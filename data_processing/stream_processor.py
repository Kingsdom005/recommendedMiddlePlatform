import os, sys
# 项目根目录：E:\proj\douyin\recommendedMiddlePlatform
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 将项目根目录加入 Python 模块搜索路径
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.config import config
from feature_store.redis_utils import redis_utils

class MockStreamProcessor:
    """模拟流处理器，用于在没有 Flink 的情况下运行"""
    
    def __init__(self):
        self.use_mock = True
        print("[StreamProcessor] Using mock implementation (Flink not available)")
    
    def process_user_behavior(self):
        """模拟处理用户行为数据"""
        print("[StreamProcessor] Processing user behavior data (mock)")
        
        # 模拟从 Kafka 读取数据
        print(f"[StreamProcessor] Reading from Kafka topic: {config.KAFKA_TOPIC_USER_BEHAVIOR}")
        
        # 模拟处理数据并计算特征
        print("[StreamProcessor] Calculating user activity features")
        
        # 模拟将特征写入 Redis
        print(f"[StreamProcessor] Writing features to Redis: {config.REDIS_HOST}:{config.REDIS_PORT}")
    
    def run(self):
        """运行流处理作业"""
        print("[StreamProcessor] Starting stream processing job (mock)")
        self.process_user_behavior()
        print("[StreamProcessor] Stream processing job completed (mock)")

class StreamProcessor:
    """流处理器"""
    
    def __init__(self):
        try:
            from pyflink.datastream import StreamExecutionEnvironment
            from pyflink.table import StreamTableEnvironment, EnvironmentSettings
            # 创建流处理环境
            self.env = StreamExecutionEnvironment.get_execution_environment()
            settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
            self.table_env = StreamTableEnvironment.create(self.env, settings)
            self.use_mock = False
            print("[StreamProcessor] Connected to Flink environment")
        except Exception as e:
            print(f"[StreamProcessor] Failed to connect to Flink: {e}")
            print("[StreamProcessor] Using mock implementation")
            self.processor = MockStreamProcessor()
            self.use_mock = True
    
    def process_user_behavior(self):
        """处理用户行为数据"""
        if self.use_mock:
            self.processor.process_user_behavior()
            return
        
        try:
            # 从文件系统读取数据（替代 Kafka）
            behaviors_path = os.path.join(PROJECT_ROOT, "data", "mock_data", "behaviors", "behaviors_test.json").replace("\\", "/")
            print(f"[StreamProcessor] Reading data from: {behaviors_path}")
            
            self.table_env.execute_sql(f"""
                CREATE TABLE user_behavior (
                    user_id STRING,
                    item_id STRING,
                    behavior STRING,
                    `timestamp` TIMESTAMP(3),
                    WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
                ) WITH (
                    'connector' = 'filesystem',
                    'path' = 'file:///{behaviors_path}',
                    'format' = 'json',
                    'json.timestamp-format.standard' = 'ISO-8601'
                )
            """)
            print("[StreamProcessor] Created user_behavior table")
            
            # 处理数据并计算特征，输出到文件系统（替代 Redis）
            features_path = os.path.join(PROJECT_ROOT, "data", "output", "user_features").replace("\\", "/")
            print(f"[StreamProcessor] Writing features to: {features_path}")
            
            # 确保输出目录存在
            os.makedirs(os.path.join(PROJECT_ROOT, "data", "output", "user_features"), exist_ok=True)
            
            self.table_env.execute_sql(f"""
                CREATE TABLE user_features (
                    user_id STRING,
                    feature_name STRING,
                    feature_value DOUBLE,
                    update_time TIMESTAMP(3),
                    PRIMARY KEY (user_id, feature_name) NOT ENFORCED
                ) WITH (
                    'connector' = 'filesystem',
                    'path' = 'file:///{features_path}',
                    'format' = 'json'
                )
            """)
            print("[StreamProcessor] Created user_features table")
            
            # 计算用户活跃度特征并执行
            print("[StreamProcessor] Executing INSERT INTO user_features")
            result = self.table_env.execute_sql("""
                INSERT INTO user_features
                SELECT 
                    user_id,
                    'activity_score',
                    COUNT(*) OVER (PARTITION BY user_id ORDER BY `timestamp` RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW) / 3600.0 AS activity_score,
                    CURRENT_TIMESTAMP
                FROM user_behavior
            """)
            print("[StreamProcessor] User behavior processing started")
            return result
        except Exception as e:
            print(f"[StreamProcessor] Error in process_user_behavior: {e}")
            raise
    
    def run(self):
        """运行流处理作业"""
        if self.use_mock:
            self.processor.run()
            return
        
        print("[StreamProcessor] Starting stream processing job")
        try:
            # 执行流处理作业
            result = self.process_user_behavior()
            # 等待作业完成（对于流处理作业，这会一直运行）
            if result:
                print("[StreamProcessor] Stream processing job is running...")
                result.wait()
        except KeyboardInterrupt:
            print("[StreamProcessor] Stream processing job interrupted by user")
        except Exception as e:
            print(f"[StreamProcessor] Stream processing job failed: {e}")
            raise

if __name__ == "__main__":
    processor = StreamProcessor()
    processor.run()
