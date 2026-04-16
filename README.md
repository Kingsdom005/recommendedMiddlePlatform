# 推荐中台系统

## 项目简介

推荐中台系统是一个基于 Python 开发的工业级推荐系统中台，为短视频平台提供智能推荐服务。系统采用模块化设计，包含数据处理、特征存储、模型训练和服务提供等核心功能。

## 快速开始（仅运行可忽略后面的内容）

1. 下载压缩包models.zip，直接解压到[proj_root_dir]/model/models即可，确保pt文件在路径如：[proj_root_dir]/model/models/\*.pt即可。
2. 如果想跑各个流程和端到端流程，可以直接python [proj_root_dir]/pipeline/[stream].py；
3. 跑离线流程需要配置kafka, hbase, elasticsearch和redis，然后改[proj_root_dir]/config/config.py或.env，可以使用工具python [proj_root_dir]\tests\test_all_components.py来测试kafka, hbase, elasticsearch和redis是否都环境配置成功；
4. 如果只跑推荐流程，只用配置常见环境，把压缩包解压到models对应位置，然后运行online_inference_pipeline.py这个文件即可；
5. 如果要封装成对外服务，可以使用python [proj_root_dir]/service/api_service.py；

## 整体目标

1. 构建一个模块化、可扩展的推荐系统中台
2. 支持实时和离线数据处理
3. 提供高性能的特征存储和访问
4. 支持深度学习模型的训练和推理
5. 提供标准化的推荐服务接口
6. 集成监控和运维能力

## 相关技术

### 1. 数据处理层

- Apache Flink - 实时流处理引擎，处理用户行为数据和特征计算
- Apache Spark - 批处理引擎，用于离线特征计算和模型训练

### 2. 特征存储层

- Redis - 实时特征缓存，支持毫秒级特征访问
- HBase - 大规模特征存储，适合存储用户历史行为数据
- Elasticsearch - 用于内容索引和检索

### 3. 模型层

- PyTorch - 深度学习模型训练框架
- ONNX - 模型标准化和部署
- ONNX Runtime - 模型推理引擎

### 4. 服务层

- FastAPI - 高性能 Web 框架，提供 HTTP 接口

### 5. 监控和运维

- Prometheus - 监控系统
- Grafana - 可视化仪表盘

## 目录结构

```
recommendedMiddlePlatform/
├── config/              # 配置文件
├── data/                # 数据目录
│   ├── mock_data/       # 模拟数据
│   ├── scripts/         # 数据处理脚本
│   └── output/          # 输出数据
├── data_processing/     # 数据处理模块
├── feature_store/       # 特征存储模块
├── model/               # 模型训练和推理模块
├── service/             # 服务模块
├── monitor/             # 监控配置
├── scripts/             # 脚本文件
├── tests/               # 测试文件
├── requirements.txt     # 依赖包
└── README.md            # 项目说明
```

## 核心功能

1. **实时数据处理**：使用 Flink 处理用户行为数据，实时计算特征
2. **特征存储**：使用 Redis 缓存实时特征，HBase 存储历史特征
3. **模型训练**：使用 PyTorch 训练推荐模型，支持深度学习
4. **模型推理**：使用 ONNX Runtime 进行模型推理
5. **推荐服务**：提供 HTTP API 接口，返回个性化推荐结果
6. **监控运维**：集成 Prometheus 和 Grafana，实时监控系统状态

## 模块说明

### 1. 配置模块 (config/)

**目的**：管理系统的所有配置项，包括服务地址、端口、模型路径等。

**作用**：

- 集中管理配置，方便统一修改
- 支持环境变量覆盖默认配置
- 为其他模块提供配置信息

**配置文件**：

- `config.py`：主要配置文件，定义了所有配置项
- `.env.example`：环境变量示例文件

**测试方法**：

```bash
# 检查配置是否正确
python -c "from config.config import config; print(config.PROJECT_NAME)"
```

### 2. 数据处理模块 (data_processing/)

**目的**：处理用户行为数据，计算特征。

**作用**：

- 实时处理用户行为数据
- 计算用户和物品特征
- 为模型训练和推荐服务提供数据支持

**核心文件**：

- `stream_processor.py`：流处理器，使用 Flink 处理实时数据

**测试方法**：

```bash
# 运行流处理器
python data_processing/stream_processor.py
```

### 3. 特征存储模块 (feature_store/)

**目的**：存储和管理特征数据。

**作用**：

- 提供特征的存储和访问接口
- 支持实时特征缓存和历史特征存储
- 为推荐服务提供特征数据

**核心文件**：

- `redis_utils.py`：Redis 工具类，用于实时特征缓存
- `hbase_utils.py`：HBase 工具类，用于存储历史特征
- `elasticsearch_utils.py`：Elasticsearch 工具类，用于内容索引和检索

**测试方法**：

```bash
# 测试特征存储模块
python tests/test_all_components.py
```

### 4. 模型模块 (model/)

**目的**：训练和部署推荐模型。

**作用**：

- 定义推荐模型结构
- 训练模型并保存
- 提供模型推理接口

**核心文件**：

- `train_model.py`：模型训练脚本
- `inference_service.py`：模型推理服务

**测试方法**：

```bash
# 训练模型
python model/train_model.py

# 测试推理服务
python -c "from model.inference_service import inference_service; print(inference_service.predict(1, 100))"
```

### 5. 服务模块 (service/)

**目的**：提供推荐服务接口。

**作用**：

- 处理推荐请求
- 调用特征存储和模型推理服务
- 返回推荐结果

**核心文件**：

- `api_service.py`：API 服务，提供 HTTP 接口
- `recommendation_service.py`：推荐服务，处理推荐逻辑

**测试方法**：

- 填写相应的服务地址和配置

# 启动 API 服务

python service/api_service.py

# 发送推荐请求

curl -X POST http://localhost:8080/api/recommend \
 -H "Content-Type: application/json" \
 -d '{"user_id": "123", "top_k": 10}'

````

## 快速开始

### 1. 环境准备

```bash
# 安装依赖
pip install -r requirements.txt

# 配置环境变量
cp .env.example .env
# 编辑 .env 文件，设置相关配置

```bash
### 2. 数据准备

```bash
# 生成少量模拟数据（用于测试）
python data/scripts/generate_small_mock_data.py

# 转换行为数据格式
python data/scripts/convert_behaviors.py

# 加载模拟数据到各个服务
python data/scripts/load_mock_data.py
````

### 3. 模型训练

```bash
# 训练模型
python model/train_model.py
```

- **离线训练**：

### 4. 启动服务

```bash
# 启动 API 服务
python service/api_service.py
```

### 5. 测试推荐接口

```bash
# 发送推荐请求
curl -X POST http://localhost:8080/api/recommend \
  -H "Content-Type: application/json" \
  -d '{"user_id": "123", "top_k": 10}'


curl -X POST http://localhost:8080/api/recommend -H "Content-Type: application/json" -d '{"user_id": "123", "top_k": 10}'

```

## 端到端测试

### 测试流程

1. **准备测试数据**：生成模拟数据并加载到各个服务
2. **启动流处理器**：处理用户行为数据，计算特征
3. **启动推荐服务**：提供推荐接口
4. **发送推荐请求**：测试推荐服务是否正常工作
5. **验证推荐结果**：检查推荐结果是否合理

### 测试脚本

```bash
# 运行所有组件测试
python tests/test_all_components.py

# 验证端到端功能
python -c "
from service.recommendation_service import recommendation_service
print(recommendation_service.get_recommendations('123', 10))
"
```

## 模块间依赖关系

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  服务模块        │◄────│  模型模块        │◄────│  数据处理模块    │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        ▲                        ▲                        ▲
        │                        │                        │
        ▼                        ▼                        ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  特征存储模块    │────►│  配置模块        │────►│  数据模块        │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

## 部署方式

### 本地部署

1. 启动 Redis、HBase、Elasticsearch 服务
2. 运行启动脚本

### 注意事项

1. 确保 Redis、HBase、Elasticsearch 服务正常运行
2. 调整配置文件中的服务地址和端口，确保与实际部署环境一致
3. 对于大规模部署，建议使用容器编排
4. 定期备份模型和特征数据，确保系统可靠性

## 开发指南

### 代码结构

- **config/**：系统配置，包括服务地址、端口等
- **data/**：数据目录，包括模拟数据和数据处理脚本
- **data_processing/**：数据处理模块，包括流处理器
- **feature_store/**：特征存储模块，包括 Redis、HBase 和 Elasticsearch 工具
- **model/**：模型训练和推理模块，包括模型定义和推理服务
- **service/**：服务模块，包括推荐服务和 API 服务
- **monitor/**：监控配置，包括 Prometheus 和 Grafana 配置
- **tests/**：测试文件，包括组件测试和端到端测试

### 扩展建议

1. **特征工程**：增加更多特征提取和处理逻辑
2. **模型优化**：尝试不同的模型架构和训练策略
3. **服务扩展**：增加更多服务接口，支持不同的推荐场景
4. **监控完善**：增加更多监控指标和告警规则
5. **数据质量**：增加数据质量监控和处理逻辑

## 测试结果

系统组件测试结果：

- kafka: [OK] 通过
- hbase: [OK] 通过
- elasticsearch: [OK] 通过
- redis: [OK] 通过
- hadoop: [INFO] 跳过

## 联系方式

如有问题或建议，请联系项目维护者。

- **模型存储**：模型文件会保存在 `model/models/` 目录

## 许可证

MIT License
