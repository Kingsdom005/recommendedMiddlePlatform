#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成少量的模拟数据（每个阶段几百条），用于测试整个系统
"""

import os
import json
import random
from datetime import datetime, timedelta

# 数据目录
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'mock_data')

# 确保数据目录存在
os.makedirs(os.path.join(DATA_DIR, 'users'), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, 'videos'), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, 'behaviors'), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, 'features'), exist_ok=True)

# 生成用户数据
def generate_users(count=200):
    """生成用户数据"""
    users = []
    for i in range(1, count + 1):
        user = {
            "user_id": f"user_{i}",
            "username": f"user{i}",
            "gender": random.choice(["male", "female"]),
            "age_group": random.choice(["18-24", "25-34", "35-44", "45+"]),
            "location": random.choice(["北京", "上海", "广州", "深圳", "杭州"]),
            "registration_time": (datetime.now() - timedelta(days=random.randint(1, 365))).isoformat(),
            "is_active": random.choice([True, False]),
            "follower_count": random.randint(0, 10000),
            "following_count": random.randint(0, 1000),
            "like_count": random.randint(0, 5000)
        }
        users.append(user)
    return users

# 生成视频数据
def generate_videos(count=300):
    """生成视频数据"""
    videos = []
    categories = ["娱乐", "科技", "体育", "美食", "旅游", "教育", "时尚", "游戏"]
    for i in range(1, count + 1):
        video = {
            "video_id": f"video_{i}",
            "title": f"测试视频 {i}",
            "description": f"这是测试视频 {i} 的描述",
            "category": random.choice(categories),
            "duration": random.randint(10, 600),
            "upload_time": (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat(),
            "view_count": random.randint(0, 100000),
            "like_count": random.randint(0, 10000),
            "comment_count": random.randint(0, 1000),
            "share_count": random.randint(0, 5000),
            "author_id": f"user_{random.randint(1, 200)}"
        }
        videos.append(video)
    return videos

# 生成行为数据
def generate_behaviors(count=500):
    """生成行为数据"""
    behaviors = []
    behavior_types = ["view", "like", "comment", "share", "follow"]
    for i in range(1, count + 1):
        behavior = {
            "behavior_id": f"behavior_{i}",
            "user_id": f"user_{random.randint(1, 200)}",
            "video_id": f"video_{random.randint(1, 300)}",
            "behavior_type": random.choice(behavior_types),
            "timestamp": (datetime.now() - timedelta(hours=random.randint(1, 168))).isoformat(),
            "duration": random.randint(1, 600),
            "source": random.choice(["推荐", "关注", "搜索", "热门"]),
            "device": random.choice(["手机", "平板", "电脑"])
        }
        behaviors.append(behavior)
    return behaviors

# 生成用户特征数据
def generate_user_features(count=200):
    """生成用户特征数据"""
    features = []
    for i in range(1, count + 1):
        feature = {
            "user_id": f"user_{i}",
            "features": {
                "activity_score": round(random.uniform(0, 1), 2),
                "preference_categories": random.sample(["娱乐", "科技", "体育", "美食", "旅游"], 3),
                "average_watch_duration": random.randint(10, 300),
                "interaction_rate": round(random.uniform(0, 1), 2),
                "content_consistency": round(random.uniform(0, 1), 2)
            }
        }
        features.append(feature)
    return features

# 保存数据
def save_data(data, file_path):
    """保存数据到文件"""
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# 主函数
def main():
    """主函数"""
    print("开始生成少量模拟数据...")
    
    # 生成用户数据
    users = generate_users(200)
    save_data(users, os.path.join(DATA_DIR, 'users', 'users_200.json'))
    print(f"生成了 {len(users)} 条用户数据")
    
    # 生成视频数据
    videos = generate_videos(300)
    save_data(videos, os.path.join(DATA_DIR, 'videos', 'videos_300.json'))
    print(f"生成了 {len(videos)} 条视频数据")
    
    # 生成行为数据
    behaviors = generate_behaviors(500)
    save_data(behaviors, os.path.join(DATA_DIR, 'behaviors', 'behaviors_500.json'))
    print(f"生成了 {len(behaviors)} 条行为数据")
    
    # 生成用户特征数据
    features = generate_user_features(200)
    save_data(features, os.path.join(DATA_DIR, 'features', 'user_features_200.json'))
    print(f"生成了 {len(features)} 条用户特征数据")
    
    print("少量模拟数据生成完成！")

if __name__ == "__main__":
    main()
