from config.config import config
import json

class MockHBaseTable:
    def __init__(self, table_name):
        self.table_name = table_name
        self.data = {}
    
    def put(self, row_key, data):
        self.data[row_key] = data
    
    def scan(self, row_prefix, limit):
        result = []
        for key, data in self.data.items():
            if key.startswith(row_prefix.decode('utf-8')):
                result.append((key.encode('utf-8'), {
                    b'behavior:data': data['behavior:data'].encode('utf-8'),
                    b'behavior:timestamp': data['behavior:timestamp'].encode('utf-8')
                }))
                if len(result) >= limit:
                    break
        return result
    
    def row(self, row_key):
        if row_key.decode('utf-8') in self.data:
            data = self.data[row_key.decode('utf-8')]
            return {
                b'profile:data': data.get('profile:data', '{}').encode('utf-8')
            }
        return {}

class MockHBaseConnection:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.tables = {}
    
    def table(self, table_name):
        if table_name not in self.tables:
            self.tables[table_name] = MockHBaseTable(table_name)
        return self.tables[table_name]
    
    def close(self):
        pass

class HBaseUtils:
    def __init__(self):
        try:
            from happybase import Connection
            self.connection = Connection(host=config.HBASE_HOST, port=config.HBASE_PORT)
            self.use_mock = False
            print("[HBase] Connected to HBase server")
        except Exception as e:
            print(f"[HBase] Failed to connect to HBase server: {e}")
            print("[HBase] Using mock implementation")
            self.connection = MockHBaseConnection(config.HBASE_HOST, config.HBASE_PORT)
            self.use_mock = True
    
    def get_table(self, table_name):
        """获取表实例"""
        return self.connection.table(table_name)
    
    def put_user_behavior(self, user_id, behavior_id, behavior_data):
        """存储用户行为数据"""
        table = self.get_table('user_behavior')
        row_key = f"{user_id}:{behavior_id}"
        data = {
            'behavior:data': json.dumps(behavior_data),
            'behavior:timestamp': str(behavior_data.get('timestamp', 0))
        }
        table.put(row_key, data)
    
    def get_user_behaviors(self, user_id, limit=100):
        """获取用户历史行为"""
        table = self.get_table('user_behavior')
        prefix = f"{user_id}:"
        behaviors = []
        for key, data in table.scan(row_prefix=prefix.encode('utf-8'), limit=limit):
            behavior_data = json.loads(data[b'behavior:data'].decode('utf-8'))
            behaviors.append(behavior_data)
        return behaviors
    
    def get_user_profile(self, user_id):
        """获取用户画像"""
        try:
            table = self.get_table('user_profile')
            row = table.row(user_id.encode('utf-8'))
            if row and b'profile:data' in row:
                return json.loads(row[b'profile:data'].decode('utf-8'))
            return None
        except Exception as e:
            print(f"[HBase] Error getting user profile: {e}")
            return None
    
    def put_user_profile(self, user_id, profile_data):
        """存储用户画像"""
        table = self.get_table('user_profile')
        row_key = user_id
        data = {
            'profile:data': json.dumps(profile_data)
        }
        table.put(row_key, data)
    
    def close(self):
        """关闭连接"""
        self.connection.close()

hbase_utils = HBaseUtils()
