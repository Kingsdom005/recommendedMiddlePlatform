#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
转换行为数据格式，将 JSON 数组转换为每行一个 JSON 对象
"""

import os
import json

# 获取项目根目录
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))

# 输入文件路径
input_file = os.path.join(PROJECT_ROOT, "data", "mock_data", "behaviors", "behaviors_500.json")
# 输出文件路径
output_file = os.path.join(PROJECT_ROOT, "data", "mock_data", "behaviors", "behaviors_formatted.json")

print(f"读取输入文件: {input_file}")

# 读取 JSON 数组
with open(input_file, 'r', encoding='utf-8') as f:
    behaviors = json.load(f)

print(f"共读取 {len(behaviors)} 条行为数据")

# 转换为每行一个 JSON 对象的格式
with open(output_file, 'w', encoding='utf-8') as f:
    for behavior in behaviors:
        # 转换字段名以匹配流处理器中的定义
        formatted_behavior = {
            "user_id": behavior.get("user_id"),
            "item_id": behavior.get("video_id"),  # 将 video_id 映射为 item_id
            "behavior": behavior.get("behavior_type"),  # 将 behavior_type 映射为 behavior
            "timestamp": behavior.get("timestamp")
        }
        # 写入一行 JSON 对象
        f.write(json.dumps(formatted_behavior, ensure_ascii=False) + '\n')

print(f"转换完成，输出文件: {output_file}")
print(f"文件大小: {os.path.getsize(output_file) / 1024 / 1024:.2f} MB")