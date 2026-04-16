#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
监控工具类，用于实时监控系统各个组件的运行状态
"""

import time
from prometheus_client import start_http_server, Gauge, Counter, Histogram


class MonitoringUtils:
    """监控工具类"""
    
    def __init__(self, port=9091):
        """初始化监控工具"""
        self.port = port
        self._init_metrics()
        self._start_server()
    
    def _init_metrics(self):
        """初始化监控指标"""
        # 数据加载指标
        self.data_load_counter = Counter('data_load_total', 'Total number of data loaded', ['component', 'data_type'])
        self.data_load_time = Histogram('data_load_duration_seconds', 'Time taken to load data', ['component', 'data_type'])
        
        # 模型训练指标
        self.model_train_counter = Counter('model_train_total', 'Total number of model training runs')
        self.model_train_time = Histogram('model_train_duration_seconds', 'Time taken to train model')
        self.model_train_loss = Gauge('model_train_loss', 'Current training loss')
        
        # 流处理指标
        self.stream_process_counter = Counter('stream_process_total', 'Total number of stream processing events', ['event_type'])
        self.stream_process_time = Histogram('stream_process_duration_seconds', 'Time taken to process stream events')
        
        # 服务指标
        self.api_request_counter = Counter('api_request_total', 'Total number of API requests', ['endpoint', 'method', 'status'])
        self.api_request_time = Histogram('api_request_duration_seconds', 'Time taken to process API requests', ['endpoint'])
        
        # 系统健康指标
        self.system_health = Gauge('system_health', 'System health status (1=healthy, 0=unhealthy)')
        self.component_health = Gauge('component_health', 'Component health status (1=healthy, 0=unhealthy)', ['component'])
    
    def _start_server(self):
        """启动监控服务器"""
        try:
            start_http_server(self.port)
            print(f"[Monitoring] Started Prometheus metrics server on port {self.port}")
        except Exception as e:
            print(f"[Monitoring] Failed to start Prometheus metrics server: {e}")
    
    def record_data_load(self, component, data_type, count, duration):
        """记录数据加载指标"""
        self.data_load_counter.labels(component=component, data_type=data_type).inc(count)
        self.data_load_time.labels(component=component, data_type=data_type).observe(duration)
    
    def record_model_train(self, duration, loss=None):
        """记录模型训练指标"""
        self.model_train_counter.inc()
        self.model_train_time.observe(duration)
        if loss is not None:
            self.model_train_loss.set(loss)
    
    def record_stream_process(self, event_type, duration):
        """记录流处理指标"""
        self.stream_process_counter.labels(event_type=event_type).inc()
        self.stream_process_time.observe(duration)
    
    def record_api_request(self, endpoint, method, status, duration):
        """记录API请求指标"""
        self.api_request_counter.labels(endpoint=endpoint, method=method, status=status).inc()
        self.api_request_time.labels(endpoint=endpoint).observe(duration)
    
    def set_system_health(self, healthy):
        """设置系统健康状态"""
        self.system_health.set(1 if healthy else 0)
    
    def set_component_health(self, component, healthy):
        """设置组件健康状态"""
        self.component_health.labels(component=component).set(1 if healthy else 0)


# 全局监控实例
monitoring = None

def get_monitoring():
    """获取监控实例"""
    global monitoring
    if monitoring is None:
        monitoring = MonitoringUtils()
    return monitoring


def main():
    """测试监控功能"""
    # 初始化监控
    mon = get_monitoring()
    
    # 模拟数据加载
    start_time = time.time()
    time.sleep(0.1)
    mon.record_data_load('load_mock_data', 'users', 1000, time.time() - start_time)
    
    # 模拟模型训练
    start_time = time.time()
    time.sleep(0.2)
    mon.record_model_train(time.time() - start_time, 0.5)
    
    # 模拟流处理
    start_time = time.time()
    time.sleep(0.05)
    mon.record_stream_process('user_behavior', time.time() - start_time)
    
    # 模拟API请求
    start_time = time.time()
    time.sleep(0.01)
    mon.record_api_request('/api/recommend', 'POST', 200, time.time() - start_time)
    
    # 设置系统健康状态
    mon.set_system_health(True)
    mon.set_component_health('kafka', True)
    mon.set_component_health('hbase', True)
    mon.set_component_health('elasticsearch', True)
    mon.set_component_health('redis', True)
    mon.set_component_health('hadoop', True)
    
    print("[Monitoring] Test completed. Metrics available at http://localhost:9090/metrics")
    
    # 保持运行
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[Monitoring] Exiting...")


if __name__ == "__main__":
    main()
