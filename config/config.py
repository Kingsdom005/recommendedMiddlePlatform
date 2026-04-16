import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # 基础配置
    PROJECT_NAME = "Recommended Middle Platform"
    VERSION = "1.0.0"
    
    # Kafka配置
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC_USER_BEHAVIOR = "user_behavior"
    KAFKA_TOPIC_RECOMMEND_REQUEST = "recommend_request"
    KAFKA_TOPIC_RECOMMEND_RESPONSE = "recommend_response"
    
    # Redis配置
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")
    REDIS_DB = int(os.getenv("REDIS_DB", "0"))
    
    # HBase配置
    HBASE_HOST = os.getenv("HBASE_HOST", "localhost")
    HBASE_PORT = int(os.getenv("HBASE_PORT", "9090"))
    
    # Elasticsearch配置
    ES_HOSTS = [os.getenv("ES_HOST", "http://localhost:9200")]
    ES_INDEX_VIDEO = "videos"
    ES_INDEX_USER = "users"
    ES_USER = "elastic"
    ES_PASSWORD = "123456"
    
    # 模型配置
    MODEL_DIR = os.getenv("MODEL_DIR", "./model/models")
    MODEL_NAME = os.getenv("MODEL_NAME", "recommendation_model")
    ONNX_MODEL_PATH = os.path.join(MODEL_DIR, f"{MODEL_NAME}.onnx")
    
    # 服务配置
    SERVICE_HOST = os.getenv("SERVICE_HOST", "0.0.0.0")
    SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8080"))
    
    # 监控配置
    PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "9090"))
    
    # 数据处理配置
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1024"))
    WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", "3600"))

config = Config()
