import os, sys
# 项目根目录：E:\proj\douyin\recommendedMiddlePlatform
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 将项目根目录加入 Python 模块搜索路径
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.config import config
from feature_store.redis_utils import redis_utils
from monitor.monitoring_utils import get_monitoring

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
        # 获取监控实例
        self.monitoring = get_monitoring()
        
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
            # 尝试从Kafka读取数据
            try:
                # 创建Kafka源表
                print(f"[StreamProcessor] Creating Kafka source table for topic: {config.KAFKA_TOPIC_USER_BEHAVIOR}")
                self.table_env.execute_sql(f"""
                    CREATE TABLE user_behavior (
                        user_id STRING,
                        item_id STRING,
                        behavior STRING,
                        `timestamp` TIMESTAMP(3),
                        WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
                    ) WITH (
                        'connector' = 'kafka',
                        'topic' = '{config.KAFKA_TOPIC_USER_BEHAVIOR}',
                        'properties.bootstrap.servers' = '{config.KAFKA_BOOTSTRAP_SERVERS}',
                        'properties.group.id' = 'user_behavior_group',
                        'format' = 'json',
                        'json.timestamp-format.standard' = 'ISO-8601',
                        'scan.startup.mode' = 'earliest-offset'
                    )
                """)
                print("[StreamProcessor] Created Kafka source table")
            except Exception as e:
                print(f"[StreamProcessor] Failed to create Kafka source table: {e}")
                print("[StreamProcessor] Using filesystem as fallback")
                # 从文件系统读取数据（替代 Kafka）
                behaviors_path = os.path.join(PROJECT_ROOT, "data", "mock_data", "behaviors", "behaviors_5000000.json").replace("\\", "/")
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
                print("[StreamProcessor] Created filesystem source table")
            
            # 尝试输出到Redis
            try:
                # 创建Redis结果表
                print(f"[StreamProcessor] Creating Redis sink table")
                self.table_env.execute_sql(f"""
                    CREATE TABLE user_features (
                        user_id STRING,
                        feature_name STRING,
                        feature_value DOUBLE,
                        update_time TIMESTAMP(3),
                        PRIMARY KEY (user_id, feature_name) NOT ENFORCED
                    ) WITH (
                        'connector' = 'redis',
                        'host' = '{config.REDIS_HOST}',
                        'port' = '{config.REDIS_PORT}',
                        'password' = '{config.REDIS_PASSWORD}',
                        'db' = '{config.REDIS_DB}',
                        'command' = 'HSET',
                        'key-pattern' = 'user:{user_id}:features',
                        'value-pattern' = '{feature_name}:{feature_value}'
                    )
                """)
                print("[StreamProcessor] Created Redis sink table")
            except Exception as e:
                print(f"[StreamProcessor] Failed to create Redis sink table: {e}")
                print("[StreamProcessor] Using filesystem as fallback")
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
                print("[StreamProcessor] Created filesystem sink table")
            
            # 记录处理开始时间
            process_start_time = time.time()
            
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
            
            # 记录处理时间
            process_duration = time.time() - process_start_time
            self.monitoring.record_stream_process('user_behavior', process_duration)
            
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
