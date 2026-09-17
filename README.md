# FR 2052a — 监管流动性报表数据处理演示项目

> 一句话定位：演示如何从源系统采集 FR 2052a（美联储流动性监控报告）数据，经 ODS/OWD/OWS/ADS 四层处理后，产出合规报送文件，并支持 GL 对账、重述、Time Travel 与 DataHub 血缘。

## 为什么需要它

FR 2052a 是美联储针对大型复杂银行组织的流动性监控报告，要求按 Section A-K 报送融资分类与现金流入流出。真实环境中：
- 数据来自核心银行、资金交易、衍生品、托管、贷款、财务总账等多个源系统
- 需要 T+1 批处理报送，同时需要实时流动性预警
- 需要支持重述（scd2）与审计追溯
- 需要 GL 对账阻断报送

本演示项目用 **Core 版**（PostgreSQL + dbt + Airflow + Great Expectations）和 **Advanced 版**（Kafka + Iceberg + Spark）两套栈，完整展示从数据采集到监管报送的全链路。

## 快速开始（5 分钟上手）

### 环境准备

```bash
# Server 1: PostgreSQL + MinIO + Airflow + DataHub
cd deploy/server1 && docker compose -f docker-compose-core.yml up -d
mc alias set local http://192.168.17.22:9000 admin password123
mc mb local/fr2052a-bucket

# Server 2: Kafka + Spark
cd deploy/server2 && docker compose -f docker-compose-compute.yml up -d

# Python 环境
python3 -m venv ~/fr2052a_venv
source ~/fr2052a_venv/bin/activate
pip install dbt-postgres dbt-spark[PySpark] great-expectations psycopg2-binary pyspark==3.5.0
```

### 验证跑通

```bash
# 1. PG 连通
psql -h 192.168.17.22 -U fr2052a -d fr2052a_db -c "SELECT schema_name FROM information_schema.schemata"

# 2. MinIO 桶存在
mc ls local/

# 3. Spark UI 可访问
curl -s http://192.168.17.24:8081 | head -20

# 4. dbt 跑通最小模型
cd dbt && dbt deps && dbt run --select ads_fr2052a_report
```

### 运行完整 DAG

```bash
airflow dags trigger fr2052a_daily_batch
airflow dags list-runs --dag-id fr2052a_daily_batch
```

## 怎么用（常见任务）

| 任务 | 命令 |
|------|------|
| 生成测试数据 | `python python/generators/generate_sample_data.py` |
| 加载 ODS 到 Iceberg | `python python/lakehouse/load_ref_tables.py` |
| 运行 dbt 转换 | `cd dbt && dbt run` |
| 导出数据到 PG ADS | `python python/exporters/export_gold_to_pg.py` |
| 运行数据质量校验 | `python python/validators/run_dq_rules.py` |
| Kafka 重放 ODS 数据 | `python python/producers/replay_ods_to_kafka.py` |
| 查看 Airflow DAG | `http://192.168.17.22:8080` |
| 查看 DataHub | `http://192.168.17.22:9002` |
| 定时调度 | 见 `docs/business/PROJECT.md` §运行环境 |

## 目录速览

```text
demo-fr2052a/
├── README.md                  # 人类入口（本文）
├── AGENTS.md                  # AI 编码约束
├── requirements/              # 原始需求文档（保留参考）
├── docs/
│   ├── business/              # 工程文档（九项核心）
│   │   ├── PROJECT.md
│   │   ├── DATA-DESIGN.md
│   │   ├── MODULE-DESIGN.md
│   │   ├── INTERFACE-DESIGN.md
│   │   ├── DOMAIN-LANGUAGE.md
│   │   ├── CHANGELOG.md
│   │   └── KNOWN-ISSUE.md
│   ├── rules/                 # 规范文件
│   │   ├── PROJECT-STRUCTURE.md
│   │   ├── CODING-STANDARD.md
│   │   ├── DEVELOP-FLOW.md
│   │   └── ACCEPTANCE-CHECKLIST.md
│   └── changes/               # 变更留痕
├── deploy/
│   ├── server1/               # Server 1 部署（PG + MinIO + Airflow）
│   └── server2/               # Server 2 部署（Kafka + Spark）
├── sql/
│   └── iceberg/               # Iceberg 表 DDL
├── dbt/                       # dbt 项目（models/macros/config）
├── python/
│   ├── generators/            # 测试数据生成
│   ├── lakehouse/             # Iceberg 读写
│   ├── producers/             # Kafka Producer
│   ├── consumers/             # Kafka Consumer
│   ├── exporters/             # 导出到 PG
│   └── validators/            # 数据质量校验
├── config/
│   └── pipeline_topics.json   # Topic 配置
└── sample_data/               # 样本数据（可重建）
```

## 更多细节

| 主题 | 文档 | 何时看 |
|------|------|--------|
| 架构 / 技术栈 / 目录分层 | [docs/business/PROJECT.md](docs/business/PROJECT.md) | 要改代码、加模块、排部署问题时 |
| 数据流 / 表结构 | [docs/business/DATA-DESIGN.md](docs/business/DATA-DESIGN.md) | 看数据从哪来到哪去、字段含义时 |
| 模块边界 / 接口契约 | [docs/business/MODULE-DESIGN.md](docs/business/MODULE-DESIGN.md) | 对接某模块、改接口时 |
| 术语定义 | [docs/business/DOMAIN-LANGUAGE.md](docs/business/DOMAIN-LANGUAGE.md) | 对齐业务与技术语言时 |
| 踩过的坑 / 决策 | [docs/business/KNOWN-ISSUE.md](docs/business/KNOWN-ISSUE.md) | 遇到诡异报错、想少走弯路时 |
| 构建日志 | [docs/build-log.md](docs/build-log.md) | 了解 E0-E5 详细过程时 |
| 变更记录 | [docs/business/CHANGELOG.md](docs/business/CHANGELOG.md) | 了解演进历史时 |
| 规范文件 | [docs/rules/](docs/rules/) | 了解编码纪律与流程时 |

## 项目进度

| 阶段 | 内容 | 状态 |
|------|------|------|
| E0 | 环境侦察 | ✅ 完成 |
| E1 | Server 1 存储底座（PG + MinIO） | ✅ 完成 |
| E2 | Server 2 计算底座（Kafka + Spark） | ✅ 完成 |
| E3 | 打通穿透（Iceberg + Spark 读写） | ✅ 完成 |
| E4 | 业务开发（ODS → OWD → OWS → ADS） | ✅ 完成 |
| E5 | 编排接管（Airflow DAG + GE） | ✅ 完成 |
| E6 | 合规演示（熔断 / 重述 / Time Travel） | ⏳ 待启动 |
| E7 | 治理收口（DataHub / 监控 / 脱敏） | ⏳ 待启动 |

完整构建日志见 [docs/changes/build-log.md](docs/changes/build-log.md)。
