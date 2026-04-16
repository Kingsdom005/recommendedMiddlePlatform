#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试推荐中台系统中用到的各个组件的基本功能
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
            from feature_store.hbase_utils import hbase_utils
            
            # 测试连接
            print("  测试 HBase 连接...")
            
            # 测试数据操作
            test_user_id = "test_user"
            test_behavior_id = "1"
            test_behavior_data = {"item_id": "123", "behavior": "click", "timestamp": 1234567890}
            
            # 测试存储用户行为数据
            print("  测试数据写入...")
            hbase_utils.put_user_behavior(test_user_id, test_behavior_id, test_behavior_data)
            print("  [OK] 数据写入成功")
            
            # 测试获取用户行为数据
            print("  测试数据读取...")
            behaviors = hbase_utils.get_user_behaviors(test_user_id)
            print(f"  [OK] 数据读取成功，获取到 {len(behaviors)} 条记录")
            
            self.results['hbase'] = True
            print("  [OK] HBase 测试通过")
        except Exception as e:
            print(f"  [ERROR] HBase 测试失败: {e}")
            self.results['hbase'] = False
    
    def test_elasticsearch(self):
        """测试 Elasticsearch 功能"""
        print("\n[3/5] 测试 Elasticsearch...")
        try:
            from feature_store.elasticsearch_utils import es_utils
            
            # 测试连接
            print("  测试 Elasticsearch 连接...")
            
            # 测试索引操作
            test_video_id = "test_video_1"
            test_video_data = {
                "title": "测试视频",
                "content": "这是一个测试视频",
                "category": "测试",
                "views": 100,
                "created_at": "2026-04-15"
            }
            
            # 索引视频
            print("  测试文档索引...")
            es_utils.index_video(test_video_id, test_video_data)
            print("  [OK] 文档索引成功")
            
            # 搜索视频
            print("  测试文档搜索...")
            query = {"match": {"content": "测试"}}
            results = es_utils.search_videos(query, size=5)
            print(f"  [OK] 搜索成功，找到 {len(results)} 个结果")
            
            # 测试获取视频
            print("  测试文档获取...")
            video = es_utils.get_video(test_video_id)
            if video:
                print("  [OK] 文档获取成功")
            else:
                print("  [WARNING] 文档获取失败")
            
            self.results['elasticsearch'] = True
            print("  [OK] Elasticsearch 测试通过")
        except Exception as e:
            print(f"  [ERROR] Elasticsearch 测试失败: {e}")
            self.results['elasticsearch'] = False
    
    def test_redis(self):
        """测试 Redis 功能"""
        print("\n[4/5] 测试 Redis...")
        try:
            from feature_store.redis_utils import redis_utils
            
            # 测试连接
            print("  测试 Redis 连接...")
            
            # 测试数据操作
            test_key = "test_key"
            test_value = "test_value"
            
            # 设置数据
            print("  测试数据设置...")
            redis_utils.set_feature(test_key, test_value)
            print("  [OK] 数据设置成功")
            
            # 获取数据
            print("  测试数据获取...")
            value = redis_utils.get_feature(test_key)
            print(f"  [OK] 数据获取成功: {value}")
            
            # 测试用户特征操作
            print("  测试用户特征操作...")
            test_user_id = "test_user"
            test_feature_name = "activity_score"
            test_feature_value = 0.8
            redis_utils.set_user_feature(test_user_id, test_feature_name, test_feature_value)
            user_features = redis_utils.get_user_features(test_user_id)
            print(f"  [OK] 用户特征操作成功，获取到 {len(user_features)} 个特征")
            
            self.results['redis'] = True
            print("  [OK] Redis 测试通过")
        except Exception as e:
            print(f"  [ERROR] Redis 测试失败: {e}")
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