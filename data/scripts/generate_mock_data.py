#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成大规模模拟数据用于推荐系统测试
数据包括：用户数据、视频数据、用户行为数据、用户特征数据
"""

import os
import sys
import json
import random
import time
from datetime import datetime, timedelta

# 配置参数
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'mock_data')
USERS_DIR = os.path.join(DATA_DIR, 'users')
VIDEOS_DIR = os.path.join(DATA_DIR, 'videos')
BEHAVIORS_DIR = os.path.join(DATA_DIR, 'behaviors')
FEATURES_DIR = os.path.join(DATA_DIR, 'features')

# 数据规模配置
NUM_USERS = 10000
NUM_VIDEOS = 50000
NUM_BEHAVIORS = 500000
NUM_USER_FEATURES = 10000

# 用户相关配置
USER_GENDERS = ['male', 'female', 'unknown']
USER_AGES = ['18-24', '25-34', '35-44', '45-54', '55+']
USER_LOCATIONS = ['北京', '上海', '广州', '深圳', '杭州', '成都', '武汉', '西安', '南京', '重庆',
                  '天津', '苏州', '大连', '青岛', '宁波', '厦门', '长沙', '郑州', '沈阳', '济南']

# 视频相关配置
VIDEO_CATEGORIES = ['音乐', '舞蹈', '美食', '旅游', '科技', '教育', '娱乐', '体育', '游戏', '影视',
                    '萌宠', '时尚', '美妆', '手工', '健身', '摄影', '段子', '情感', '财经', '汽车']
VIDEO_TAGS = ['搞笑', '温馨', '创意', '实用', '惊艳', '感动', '震惊', '暖心', '励志', '正能量',
              '感人', '精彩', '神反转', '涨知识', '必看', '推荐', '收藏', '热门', '新品', '限时']

# 行为类型
BEHAVIOR_TYPES = ['click', 'like', 'share', 'comment', 'follow', 'favor']

def ensure_dirs():
    """确保数据目录存在"""
    for dir_path in [USERS_DIR, VIDEOS_DIR, BEHAVIORS_DIR, FEATURES_DIR]:
        os.makedirs(dir_path, exist_ok=True)

def generate_users(count=NUM_USERS):
    """生成用户数据"""
    print(f"Generating {count} users...")
    users = []
    start_time = datetime.now() - timedelta(days=365)
    
    for i in range(count):
        user = {
            'user_id': f'u{i+1:08d}',
            'username': f'user_{i+1}',
            'gender': random.choice(USER_GENDERS),
            'age_group': random.choice(USER_AGES),
            'location': random.choice(USER_LOCATIONS),
            'registration_time': (start_time + timedelta(days=random.randint(0, 365))).isoformat(),
            'is_active': random.choice([True, True, True, False]),
            'follower_count': random.randint(0, 10000),
            'following_count': random.randint(0, 1000),
            'like_count': random.randint(0, 50000),
        }
        users.append(user)
        
        if (i + 1) % 1000 == 0:
            print(f"  Generated {i + 1} users...")
    
    output_file = os.path.join(USERS_DIR, f'users_{count}.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(users, f, ensure_ascii=False, indent=2)
    
    print(f"  Saved to {output_file}")
    return users

def generate_videos(count=NUM_VIDEOS):
    """生成视频数据"""
    print(f"Generating {count} videos...")
    videos = []
    start_time = datetime.now() - timedelta(days=180)
    
    for i in range(count):
        video = {
            'video_id': f'v{i+1:08d}',
            'title': f'视频标题_{i+1}_{random.choice(VIDEO_TAGS)}',
            'description': f'这是视频{i+1}的描述，包含一些随机内容{random.choice(VIDEO_TAGS)}',
            'category': random.choice(VIDEO_CATEGORIES),
            'tags': random.sample(VIDEO_TAGS, k=random.randint(2, 5)),
            'duration': random.randint(5, 300),
            'view_count': random.randint(0, 1000000),
            'like_count': random.randint(0, 100000),
            'comment_count': random.randint(0, 10000),
            'share_count': random.randint(0, 5000),
            'created_at': (start_time + timedelta(days=random.randint(0, 180))).isoformat(),
            'author_id': f'u{random.randint(1, NUM_USERS):08d}',
            'is_original': random.choice([True, False]),
            'status': random.choice(['published', 'published', 'published', 'deleted']),
        }
        videos.append(video)
        
        if (i + 1) % 5000 == 0:
            print(f"  Generated {i + 1} videos...")
    
    output_file = os.path.join(VIDEOS_DIR, f'videos_{count}.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(videos, f, ensure_ascii=False, indent=2)
    
    print(f"  Saved to {output_file}")
    return videos

def generate_behaviors(count=NUM_BEHAVIORS, users=None, videos=None):
    """生成用户行为数据"""
    print(f"Generating {count} user behaviors...")
    
    if users is None:
        users = [{'user_id': f'u{i+1:08d}'} for i in range(NUM_USERS)]
    if videos is None:
        videos = [{'video_id': f'v{i+1:08d}'} for i in range(NUM_VIDEOS)]
    
    behaviors = []
    start_time = datetime.now() - timedelta(days=30)
    
    for i in range(count):
        user = random.choice(users)
        video = random.choice(videos)
        
        behavior = {
            'behavior_id': f'b{i+1:010d}',
            'user_id': user['user_id'],
            'video_id': video['video_id'],
            'behavior_type': random.choice(BEHAVIOR_TYPES),
            'timestamp': (start_time + timedelta(seconds=random.randint(0, 30*24*3600))).isoformat(),
            'duration': random.randint(0, 100) if random.random() > 0.5 else 0,
            'source': random.choice(['home', 'search', 'recommend', 'follow', 'topic']),
            'device': random.choice(['mobile', 'tablet', 'pc']),
        }
        behaviors.append(behavior)
        
        if (i + 1) % 10000 == 0:
            print(f"  Generated {i + 1} behaviors...")
    
    output_file = os.path.join(BEHAVIORS_DIR, f'behaviors_{count}.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(behaviors, f, ensure_ascii=False, indent=2)
    
    print(f"  Saved to {output_file}")
    return behaviors

def generate_user_features(count=NUM_USER_FEATURES):
    """生成用户特征数据"""
    print(f"Generating {count} user features...")
    
    feature_types = [
        'interest_scores', '偏好得分',
        'active_level', '活跃等级',
        'engagement_score', '参与得分',
        'content_preference', '内容偏好',
        'social_score', '社交得分',
    ]
    
    features = []
    for i in range(count):
        user_id = f'u{i+1:08d}'
        
        feature = {
            'user_id': user_id,
            'features': {
                'interest_music': random.uniform(0, 1),
                'interest_dance': random.uniform(0, 1),
                'interest_food': random.uniform(0, 1),
                'interest_travel': random.uniform(0, 1),
                'interest_tech': random.uniform(0, 1),
                'interest_education': random.uniform(0, 1),
                'interest_entertainment': random.uniform(0, 1),
                'interest_sports': random.uniform(0, 1),
                'interest_gaming': random.uniform(0, 1),
                'interest_movie': random.uniform(0, 1),
                'active_level': random.randint(1, 10),
                'engagement_score': random.uniform(0, 100),
                'social_score': random.uniform(0, 100),
                'consumption_level': random.randint(1, 5),
                'interaction_rate': random.uniform(0, 0.5),
            },
            'last_updated': datetime.now().isoformat(),
        }
        features.append(feature)
        
        if (i + 1) % 1000 == 0:
            print(f"  Generated {i + 1} user features...")
    
    output_file = os.path.join(FEATURES_DIR, f'user_features_{count}.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(features, f, ensure_ascii=False, indent=2)
    
    print(f"  Saved to {output_file}")
    return features

def generate_all_data():
    """生成所有数据"""
    start_time = time.time()
    
    print("=" * 60)
    print("开始生成推荐系统模拟数据")
    print("=" * 60)
    
    ensure_dirs()
    
    print("\n[1/4] 生成用户数据...")
    users = generate_users()
    
    print("\n[2/4] 生成视频数据...")
    videos = generate_videos()
    
    print("\n[3/4] 生成用户行为数据...")
    generate_behaviors(users=users, videos=videos)
    
    print("\n[4/4] 生成用户特征数据...")
    generate_user_features()
    
    elapsed_time = time.time() - start_time
    
    print("\n" + "=" * 60)
    print("数据生成完成！")
    print("=" * 60)
    print(f"总耗时: {elapsed_time:.2f} 秒")
    print(f"数据目录: {DATA_DIR}")
    print(f"  - 用户数据: {NUM_USERS} 条")
    print(f"  - 视频数据: {NUM_VIDEOS} 条")
    print(f"  - 行为数据: {NUM_BEHAVIORS} 条")
    print(f"  - 特征数据: {NUM_USER_FEATURES} 条")
    print("=" * 60)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='生成推荐系统模拟数据')
    parser.add_argument('--users', type=int, default=NUM_USERS, help=f'用户数量 (默认: {NUM_USERS})')
    parser.add_argument('--videos', type=int, default=NUM_VIDEOS, help=f'视频数量 (默认: {NUM_VIDEOS})')
    parser.add_argument('--behaviors', type=int, default=NUM_BEHAVIORS, help=f'行为数量 (默认: {NUM_BEHAVIORS})')
    parser.add_argument('--features', type=int, default=NUM_USER_FEATURES, help=f'特征数量 (默认: {NUM_USER_FEATURES})')
    
    args = parser.parse_args()
    
    # 更新全局配置
    if args.users != NUM_USERS:
        globals()['NUM_USERS'] = args.users
    if args.videos != NUM_VIDEOS:
        globals()['NUM_VIDEOS'] = args.videos
    if args.behaviors != NUM_BEHAVIORS:
        globals()['NUM_BEHAVIORS'] = args.behaviors
    if args.features != NUM_USER_FEATURES:
        globals()['NUM_USER_FEATURES'] = args.features
    
    generate_all_data()