from config.config import config
import json

class MockRedis:
    def __init__(self, host, port, password, db):
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.data = {}
        self.expire_times = {}
    
    def set(self, key, value, ex=None):
        self.data[key] = value
        if ex:
            import time
            self.expire_times[key] = time.time() + ex
    
    def get(self, key):
        import time
        if key in self.expire_times and time.time() > self.expire_times[key]:
            del self.data[key]
            del self.expire_times[key]
            return None
        return self.data.get(key)
    
    def delete(self, key):
        if key in self.data:
            del self.data[key]
        if key in self.expire_times:
            del self.expire_times[key]
    
    def keys(self, pattern):
        import fnmatch
        return [key.encode('utf-8') for key in self.data.keys() if fnmatch.fnmatch(key, pattern)]

class RedisUtils:
    def __init__(self):
        try:
            import redis
            self.redis_client = redis.Redis(
                host=config.REDIS_HOST,
                port=config.REDIS_PORT,
                password=config.REDIS_PASSWORD,
                db=config.REDIS_DB
            )
            self.use_mock = False
            print("[Redis] Connected to Redis server")
        except Exception as e:
            print(f"[Redis] Failed to connect to Redis server: {e}")
            print("[Redis] Using mock implementation")
            self.redis_client = MockRedis(
                host=config.REDIS_HOST,
                port=config.REDIS_PORT,
                password=config.REDIS_PASSWORD,
                db=config.REDIS_DB
            )
            self.use_mock = True
    
    def set_feature(self, key, value, expire=3600):
        """设置特征值"""
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        self.redis_client.set(key, value, ex=expire)
    
    def get_feature(self, key):
        """获取特征值"""
        value = self.redis_client.get(key)
        if value:
            try:
                return json.loads(value)
            except:
                if isinstance(value, bytes):
                    return value.decode('utf-8')
                return value
        return None
    
    def delete_feature(self, key):
        """删除特征值"""
        self.redis_client.delete(key)
    
    def get_user_features(self, user_id):
        """获取用户所有特征"""
        keys = self.redis_client.keys(f"user:{user_id}:*")
        features = {}
        for key in keys:
            feature_name = key.decode('utf-8').split(':')[-1]
            features[feature_name] = self.get_feature(key)
        return features
    
    def set_user_feature(self, user_id, feature_name, value, expire=3600):
        """设置用户特征"""
        key = f"user:{user_id}:{feature_name}"
        self.set_feature(key, value, expire)

redis_utils = RedisUtils()
