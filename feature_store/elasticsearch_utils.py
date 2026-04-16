from config.config import config

class MockElasticsearch:
    def __init__(self, hosts):
        self.hosts = hosts
        self.indices = {}
    
    def index(self, index, id, body):
        if index not in self.indices:
            self.indices[index] = {}
        self.indices[index][id] = body
        print(f"[MockElasticsearch] Indexed document {id} in {index}: {body}")
    
    def search(self, index, body):
        if index not in self.indices:
            return {'hits': {'hits': []}}
        
        query = body.get('query', {})
        size = body.get('size', 10)
        
        # 简单的匹配逻辑
        results = []
        for id, doc in self.indices[index].items():
            # 这里实现一个简单的匹配逻辑，实际项目中可能需要更复杂的查询
            if self._match_query(doc, query):
                results.append({'_source': doc})
                if len(results) >= size:
                    break
        
        return {'hits': {'hits': results}}
    
    def _match_query(self, doc, query):
        # 简单的匹配逻辑，实际项目中可能需要更复杂的查询
        return True
    
    def get(self, index, id):
        if index in self.indices and id in self.indices[index]:
            return {'_source': self.indices[index][id]}
        raise Exception("Document not found")
    
    def update(self, index, id, body):
        if index in self.indices and id in self.indices[index]:
            doc = self.indices[index][id]
            doc.update(body.get('doc', {}))
            print(f"[MockElasticsearch] Updated document {id} in {index}: {body}")

class ElasticsearchUtils:
    def __init__(self, use_mock=False):
        try:
            if use_mock:
                raise Exception("Forcing mock implementation")
            from elasticsearch import Elasticsearch
            self.es = Elasticsearch(
                config.ES_HOSTS,
                basic_auth=(config.ES_USER, config.ES_PASSWORD)
            )
            self.use_mock = False
            print("[Elasticsearch] Connected to Elasticsearch server")
        except Exception as e:
            print(f"[Elasticsearch] Failed to connect to Elasticsearch server: {e}")
            print("[Elasticsearch] Using mock implementation")
            self.es = MockElasticsearch(config.ES_HOSTS)
            self.use_mock = True

    def index_video(self, video_id, video_data):
        """索引视频数据"""
        self.es.index(
            index=config.ES_INDEX_VIDEO,
            id=video_id,
            body=video_data
        )
    
    def search_videos(self, query, size=10):
        """搜索视频"""
        result = self.es.search(
            index=config.ES_INDEX_VIDEO,
            body={
                'query': query,
                'size': size
            }
        )
        return [hit['_source'] for hit in result['hits']['hits']]
    
    def get_video(self, video_id):
        """获取视频详情"""
        try:
            result = self.es.get(index=config.ES_INDEX_VIDEO, id=video_id)
            return result['_source']
        except:
            return None
    
    def update_video(self, video_id, video_data):
        """更新视频数据"""
        self.es.update(
            index=config.ES_INDEX_VIDEO,
            id=video_id,
            body={'doc': video_data}
        )

es_utils = ElasticsearchUtils(use_mock=True)
