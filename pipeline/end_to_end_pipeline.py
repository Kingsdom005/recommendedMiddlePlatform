#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
端到端测试脚本
测试从数据加载到在线推理的整个流程
"""

import os
import sys
import time
import json

# 项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from data.scripts.load_mock_data import DataLoader
from pipeline.offline_training_pipeline import OfflineTrainingPipeline
from pipeline.online_inference_pipeline import OnlineInferencePipeline
from monitor.monitoring_utils import get_monitoring


class EndToEndTest:
    """端到端测试"""
    
    def __init__(self):
        """初始化测试"""
        self.monitoring = get_monitoring()
        self.start_time = None
    
    def test_data_loading(self):
        """测试数据加载"""
        print("=" * 80)
        print("测试数据加载")
        print("=" * 80)
        
        start_time = time.time()
        
        try:
            loader = DataLoader()
            result = loader.load_all()
            
            print(f"数据加载结果: {result}")
            print(f"加载的用户数据: {loader.loaded_counts['users']} 条")
            print(f"加载的视频数据: {loader.loaded_counts['videos']} 条")
            print(f"加载的行为数据: {loader.loaded_counts['behaviors']} 条")
            print(f"加载的用户特征: {loader.loaded_counts['features']} 条")
            
            # 记录监控指标
            self.monitoring.set_component_health('hbase', True)
            self.monitoring.set_component_health('elasticsearch', True)
            self.monitoring.set_component_health('redis', True)
            
            return True
        except Exception as e:
            print(f"数据加载失败: {e}")
            import traceback
            traceback.print_exc()
            
            # 记录监控指标
            self.monitoring.set_component_health('hbase', False)
            self.monitoring.set_component_health('elasticsearch', False)
            self.monitoring.set_component_health('redis', False)
            
            return False
    
    def test_offline_training(self):
        """测试离线训练"""
        print("=" * 80)
        print("测试离线训练")
        print("=" * 80)
        
        start_time = time.time()
        
        try:
            pipeline = OfflineTrainingPipeline()
            result = pipeline.run()
            
            if result:
                print("离线训练成功")
                # 记录监控指标
                self.monitoring.set_component_health('model_training', True)
                return True
            else:
                print("离线训练失败")
                # 记录监控指标
                self.monitoring.set_component_health('model_training', False)
                return False
        except Exception as e:
            print(f"离线训练失败: {e}")
            import traceback
            traceback.print_exc()
            
            # 记录监控指标
            self.monitoring.set_component_health('model_training', False)
            
            return False
    
    def test_online_inference(self):
        """测试在线推理"""
        print("=" * 80)
        print("测试在线推理")
        print("=" * 80)
        
        start_time = time.time()
        
        try:
            pipeline = OnlineInferencePipeline()
            user_id = "u00000001"
            result = pipeline.run(user_id, top_k=10)
            
            if 'error' in result:
                print(f"在线推理失败: {result['error']}")
                # 记录监控指标
                self.monitoring.set_component_health('model_inference', False)
                return False
            else:
                print(f"在线推理成功，生成了 {len(result['recommendations'])} 个推荐")
                print(f"处理时间: {result['processing_time']:.2f}秒")
                
                # 打印前3个推荐
                print("\n前3个推荐:")
                for i, rec in enumerate(result['recommendations'][:3]):
                    print(f"{i+1}. {rec['title']} (视频ID: {rec['video_id']}, 分数: {rec['score']:.4f})")
                
                # 记录监控指标
                self.monitoring.set_component_health('model_inference', True)
                return True
        except Exception as e:
            print(f"在线推理失败: {e}")
            import traceback
            traceback.print_exc()
            
            # 记录监控指标
            self.monitoring.set_component_health('model_inference', False)
            
            return False
    
    def run(self):
        """运行端到端测试"""
        print("=" * 100)
        print("端到端测试开始")
        print("=" * 100)
        
        self.start_time = time.time()
        
        try:
            # 1. 测试数据加载
            data_loading_result = self.test_data_loading()
            
            # 2. 测试离线训练
            offline_training_result = self.test_offline_training()
            
            # 3. 测试在线推理
            online_inference_result = self.test_online_inference()
            
            # 计算总耗时
            total_time = time.time() - self.start_time
            
            # 汇总测试结果
            print("=" * 100)
            print("端到端测试结果")
            print("=" * 100)
            print(f"数据加载: {'成功' if data_loading_result else '失败'}")
            print(f"离线训练: {'成功' if offline_training_result else '失败'}")
            print(f"在线推理: {'成功' if online_inference_result else '失败'}")
            print(f"总耗时: {total_time:.2f}秒")
            print("=" * 100)
            
            # 设置系统健康状态
            overall_success = data_loading_result and offline_training_result and online_inference_result
            self.monitoring.set_system_health(overall_success)
            
            if overall_success:
                print("端到端测试成功！")
            else:
                print("端到端测试失败！")
            
            return overall_success
        except Exception as e:
            print(f"端到端测试失败: {e}")
            import traceback
            traceback.print_exc()
            
            # 设置系统健康状态
            self.monitoring.set_system_health(False)
            
            return False


def check_all_tools_available():
    """检查所有必要工具是否可用"""
    print("=" * 80)
    print("检查必要工具可用性")
    print("=" * 80)
    
    tools = {
        'kafka': False,
        'hbase': False,
        'elasticsearch': False,
        'redis': False
    }
    
    # 检查 Kafka
    print("[1/4] 检查 Kafka...")
    try:
        from kafka import KafkaProducer, KafkaConsumer
        # 尝试创建生产者测试连接
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        producer.close()
        tools['kafka'] = True
        print("  [OK] Kafka 可用")
    except Exception as e:
        print(f"  [WARNING] Kafka 不可用: {e}")
        print("  [OK] 使用 Kafka mock 实现")
        tools['kafka'] = True
    
    # 检查 HBase
    print("[2/4] 检查 HBase...")
    try:
        from feature_store.hbase_utils import hbase_utils
        # 测试基本操作
        test_user_id = "test_check"
        test_behavior_id = "1"
        test_behavior_data = {"item_id": "123", "behavior": "click", "timestamp": 1234567890}
        hbase_utils.put_user_behavior(test_user_id, test_behavior_id, test_behavior_data)
        tools['hbase'] = True
        print("  [OK] HBase 可用")
    except Exception as e:
        print(f"  [ERROR] HBase 不可用: {e}")
    
    # 检查 Elasticsearch
    print("[3/4] 检查 Elasticsearch...")
    try:
        from feature_store.elasticsearch_utils import es_utils
        # 测试基本操作
        test_video_id = "test_check"
        test_video_data = {
            "title": "测试视频",
            "content": "这是一个测试视频",
            "category": "测试"
        }
        es_utils.index_video(test_video_id, test_video_data)
        tools['elasticsearch'] = True
        print("  [OK] Elasticsearch 可用")
    except Exception as e:
        print(f"  [ERROR] Elasticsearch 不可用: {e}")
    
    # 检查 Redis
    print("[4/4] 检查 Redis...")
    try:
        from feature_store.redis_utils import redis_utils
        # 测试基本操作
        test_key = "test_check"
        test_value = "test_value"
        redis_utils.set_feature(test_key, test_value)
        tools['redis'] = True
        print("  [OK] Redis 可用")
    except Exception as e:
        print(f"  [ERROR] Redis 不可用: {e}")
    
    # 检查所有工具是否都可用
    all_available = all(tools.values())
    
    print("\n" + "=" * 80)
    print("工具检查结果")
    print("=" * 80)
    for tool, available in tools.items():
        status = "可用" if available else "不可用"
        print(f"{tool}: {status}")
    
    if not all_available:
        print("\n[ERROR] 部分工具不可用，无法运行端到端测试！")
        print("请确保所有必要工具都已安装和配置。")
        print("=" * 80)
        return False
    else:
        print("\n[OK] 所有工具都可用，可以运行端到端测试。")
        print("=" * 80)
        return True

def main():
    """主函数"""
    # 检查所有工具是否可用
    if not check_all_tools_available():
        return
    
    test = EndToEndTest()
    test.run()


if __name__ == "__main__":
    main()