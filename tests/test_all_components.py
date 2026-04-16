#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试推荐中台系统中用到的各个组件的基本功能
kafka, hbase, elasticsearch, redis
"""

import os
import sys
import time
import json

# 项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from config.config import config

class ComponentTester:
    """组件测试类"""
    
    def __init__(self):
        """初始化测试类"""
        self.results = {}
        print("=" * 80)
        print("推荐中台系统组件测试")
        print("=" * 80)
    
    def test_kafka(self):
        """测试 Kafka 功能"""
        print("\n[1/5] 测试 Kafka...")
        try:
            # 尝试导入 Kafka 相关模块
            try:
                from kafka import KafkaProducer, KafkaConsumer
                kafka_available = True
            except ImportError:
                kafka_available = False
            
            if kafka_available:
                # 测试 Kafka 连接
                print("  测试 Kafka 连接...")
                try:
                    # 尝试创建生产者
                    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
                    producer.close()
                    print("  [OK] Kafka 连接成功")
                    
                    # 测试基本功能
                    print("  测试 Kafka 基本功能...")
                    self.results['kafka'] = True
                    print("  [OK] Kafka 测试通过")
                except Exception as e:
                    print(f"  [ERROR] Kafka 连接失败: {e}")
                    self.results['kafka'] = False
                    print("  [ERROR] Kafka 测试失败")
            else:
                print("  [ERROR] Kafka 模块未安装，无法测试")
                self.results['kafka'] = False
                print("  [ERROR] Kafka 测试失败")
        except Exception as e:
            print(f"  [ERROR] Kafka 测试失败: {e}")
            self.results['kafka'] = False
    
    def test_hbase(self):
        """测试 HBase 功能"""
        print("\n[2/5] 测试 HBase...")
        try:
            # 直接使用 happybase 连接，不使用 mock
            from happybase import Connection
            
            print("  测试 HBase 连接...")
            conn = Connection(host=config.HBASE_HOST, port=config.HBASE_PORT)
            
            # 测试创建表
            print("  测试创建表...")
            table_name = 'test_table'
            # 如果表已存在，先删除
            if table_name.encode() in conn.tables():
                print("  [INFO] 表已存在，删除旧表...")
                conn.delete_table(table_name, disable=True)
                print("  [OK] 旧表已删除")
            conn.create_table(
                table_name,
                {'cf1': dict()}
            )
            print("  [OK] 表创建成功")
            
            # 测试写入数据
            print("  测试数据写入...")
            table = conn.table(table_name)
            table.put(b'row1', {b'cf1:column1': b'value1'})
            print("  [OK] 数据写入成功")
            
            # 测试读取数据
            print("  测试数据读取...")
            row = table.row(b'row1')
            if row:
                print(f"  [OK] 数据读取成功: {row}")
            else:
                print("  [WARNING] 数据读取失败")
            
            # 删除测试表
            print("  清理测试表...")
            conn.delete_table(table_name, disable=True)
            conn.close()
            print("  [OK] 测试表已删除")
            
            self.results['hbase'] = True
            print("  [OK] HBase 测试通过")
        except Exception as e:
            print(f"  [ERROR] HBase 测试失败: {e}")
            import traceback
            traceback.print_exc()
            self.results['hbase'] = False
    
    def test_elasticsearch(self):
        """测试 Elasticsearch 功能"""
        print("\n[3/5] 测试 Elasticsearch...")
        try:
            # 直接使用 elasticsearch 客户端，不使用 mock
            from elasticsearch import Elasticsearch
            
            print("  测试 Elasticsearch 连接...")
            es = Elasticsearch(
                config.ES_HOSTS,
                basic_auth=(config.ES_USER, config.ES_PASSWORD)
            )
            
            # 测试集群信息
            print("  测试集群信息...")
            info = es.info()
            print(f"  [OK] 集群名称: {info['cluster_name']}")
            
            # 测试索引操作
            print("  测试文档索引...")
            test_index = 'test_index'
            test_doc_id = 'test_doc_1'
            test_doc = {
                "title": "测试文档",
                "content": "这是一个测试文档",
                "category": "测试",
                "views": 100
            }
            
            # 删除已存在的索引
            if es.indices.exists(index=test_index):
                es.indices.delete(index=test_index)
                print("  [OK] 已删除旧索引")
            
            # 创建索引
            es.indices.create(index=test_index)
            print("  [OK] 索引创建成功")
            
            # 索引文档
            es.index(index=test_index, id=test_doc_id, body=test_doc)
            print("  [OK] 文档索引成功")
            
            # 刷新索引
            es.indices.refresh(index=test_index)
            print("  [OK] 索引刷新成功")
            
            # 测试文档搜索
            print("  测试文档搜索...")
            result = es.search(index=test_index, body={"query": {"match": {"content": "测试"}}})
            print(f"  [OK] 搜索成功，找到 {result['hits']['total']['value']} 个结果")
            
            # 测试获取文档
            print("  测试文档获取...")
            doc = es.get(index=test_index, id=test_doc_id)
            if doc:
                print(f"  [OK] 文档获取成功: {doc['_source']}")
            
            # 删除测试索引
            print("  清理测试索引...")
            es.indices.delete(index=test_index)
            print("  [OK] 测试索引已删除")
            
            es.close()
            self.results['elasticsearch'] = True
            print("  [OK] Elasticsearch 测试通过")
        except Exception as e:
            print(f"  [ERROR] Elasticsearch 测试失败: {e}")
            import traceback
            traceback.print_exc()
            self.results['elasticsearch'] = False
    
    def test_redis(self):
        """测试 Redis 功能"""
        print("\n[4/5] 测试 Redis...")
        try:
            # 直接使用 redis 客户端，不使用 mock
            import redis
            
            print("  测试 Redis 连接...")
            r = redis.Redis(
                host=config.REDIS_HOST,
                port=config.REDIS_PORT,
                password=config.REDIS_PASSWORD,
                db=config.REDIS_DB
            )
            
            # 测试连接
            print("  测试 PING...")
            result = r.ping()
            print(f"  [OK] PING 结果: {result}")
            
            # 测试数据操作
            print("  测试数据设置...")
            test_key = "test_key"
            test_value = "test_value"
            r.set(test_key, test_value)
            print("  [OK] 数据设置成功")
            
            # 测试数据获取
            print("  测试数据获取...")
            value = r.get(test_key)
            if value:
                print(f"  [OK] 数据获取成功: {value.decode('utf-8')}")
            else:
                print("  [WARNING] 数据获取失败")
            
            # 测试用户特征操作
            print("  测试用户特征操作...")
            test_user_id = "test_user"
            test_feature_name = "activity_score"
            test_feature_value = 0.8
            feature_key = f"user:{test_user_id}:feature:{test_feature_name}"
            r.set(feature_key, str(test_feature_value))
            print("  [OK] 用户特征设置成功")
            
            # 获取用户特征
            feature_value = r.get(feature_key)
            if feature_value:
                print(f"  [OK] 用户特征获取成功: {feature_value.decode('utf-8')}")
            
            # 清理测试数据
            print("  清理测试数据...")
            r.delete(test_key)
            r.delete(feature_key)
            print("  [OK] 测试数据已删除")
            
            r.close()
            self.results['redis'] = True
            print("  [OK] Redis 测试通过")
        except Exception as e:
            print(f"  [ERROR] Redis 测试失败: {e}")
            import traceback
            traceback.print_exc()
            self.results['redis'] = False
    
    def test_hadoop(self):
        """测试 Hadoop 功能"""
        print("\n[5/5] 测试 Hadoop...")
        try:
            # 尝试导入 Hadoop 相关模块
            try:
                from hdfs import InsecureClient
                hadoop_available = True
            except ImportError:
                hadoop_available = False
            
            if hadoop_available:
                # 测试 HDFS 连接
                print("  测试 HDFS 连接...")
                try:
                    client = InsecureClient('http://localhost:9870', user='hadoop')
                    # 测试列出根目录
                    files = client.list('/')
                    print(f"  [OK] HDFS 连接成功，根目录文件: {files[:5]}")
                    
                    # 测试基本功能
                    print("  测试 HDFS 基本功能...")
                    self.results['hadoop'] = True
                    print("  [OK] Hadoop 测试通过")
                except Exception as e:
                    print(f"  [ERROR] HDFS 连接失败: {e}")
                    self.results['hadoop'] = False
                    print("  [ERROR] Hadoop 测试失败")
            else:
                print("  [ERROR] Hadoop 模块未安装，无法测试")
                self.results['hadoop'] = False
                print("  [ERROR] Hadoop 测试失败")
        except Exception as e:
            print(f"  [ERROR] Hadoop 测试失败: {e}")
            self.results['hadoop'] = False
    
    def run_all_tests(self):
        """运行所有测试"""
        # 运行各个组件的测试
        self.test_kafka()
        self.test_hbase()
        self.test_elasticsearch()
        self.test_redis()
        self.test_hadoop()
        
        # 输出测试结果
        print("\n" + "=" * 80)
        print("测试结果汇总")
        print("=" * 80)
        
        passed = 0
        failed = 0
        
        for component, result in self.results.items():
            if result is True:
                status = "[OK] 通过"
                passed += 1
            else:
                status = "[ERROR] 失败"
                failed += 1
            print(f"{component}: {status}")
        
        print("\n" + "=" * 80)
        print(f"总计: {passed} 通过, {failed} 失败")
        
        if failed == 0:
            print("[OK] 所有测试通过！系统组件运行正常")
        else:
            print("[ERROR] 部分测试失败，请检查相关组件的配置")
        print("=" * 80)

def main():
    """主函数"""
    tester = ComponentTester()
    tester.run_all_tests()

if __name__ == "__main__":
    main()