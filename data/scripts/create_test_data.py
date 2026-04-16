#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创建测试数据，用于调试流处理器
"""

import os
import json
from datetime import datetime, timedelta

# 获取项目根目录
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))

# 输出文件路径
output_file = os.path.join(PROJECT_ROOT, "data", "mock_data", "behaviors", "behaviors_test.json")

# 创建测试数据
test_data = []
user_ids = ["u00000001", "u00000002", "u00000003"]
video_ids = ["v00000001", "v00000002", "v00000003"]
behaviors = ["click", "like", "share", "favor"]

# 生成过去24小时内的行为数据
current_time = datetime.now()
for i in range(100):  # 生成100条测试数据
    user_id = user_ids[i % len(user_ids)]
    video_id = video_ids[i % len(video_ids)]
    behavior_type = behaviors[i % len(behaviors)]
    # 生成过去24小时内的时间戳
    timestamp = current_time - timedelta(hours=i % 24, minutes=i % 60)
    
    behavior = {
        "user_id": user_id,
        "item_id": video_id,
        "behavior": behavior_type,
        "timestamp": timestamp.isoformat()
    }
    test_data.append(behavior)

# 写入测试数据
with open(output_file, 'w', encoding='utf-8') as f:
    for behavior in test_data:
        f.write(json.dumps(behavior, ensure_ascii=False) + '\n')

print(f"测试数据创建完成，输出文件: {output_file}")
print(f"共生成 {len(test_data)} 条测试数据")