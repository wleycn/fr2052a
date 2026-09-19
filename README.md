# FR 2052a — 监管流动性报表数据处理演示项目

[![lint](https://github.com/wleycn/fr2052a/actions/workflows/lint.yml/badge.svg)](https://github.com/wleycn/fr2052a/actions/workflows/lint.yml)

> 一句话定位：演示如何从源系统采集 FR 2052a（美联储流动性监控报告）数据，经 ODS/OWD/OWS/ADS 四层处理后产出合规报送文件，并支持 GL 对账、重述、Time Travel 与血缘追溯。

## 为什么需要它

FR 2052a 是美联储针对大型复杂银行组织的流动性监控报告，要求按 Section A–K 报送融资分类与现金流入流出。真实环境里它意味着：

- 数据来自核心银行、资金交易、衍生品、托管、贷款、财务总账等多个源系统
- 既要 T+1 批处理报送，也要实时流动性预警
- 要支持重述（SCD2）与审计追溯
- 未对平的总账必须能阻断报送

本项目用一套链路覆盖这四件事：Kafka 接源数据 → Iceberg 分层存储 → dbt 建模 → PostgreSQL 报送服务层 → 报送文件与合规闸。

## 架构一览

| 位置 | 承载 | 组件 |
|---|---|---|
| Server 1 `192.168.17.22` | 存储与调度 | PostgreSQL 18.6、MinIO、Airflow 2.10.5 |
| Server 2 `192.168.17.24` | 计算与消息 | Kafka 4.3.1、Spark 3.5.9、Iceberg 1.11 |
| 开发机 `192.168.17.11` | 代码与驱动脚本 | 本仓库、样本数据生成器、重置与同步脚本 |

数据分层与写入方：

| 层 | 存放 | 内容 | 写入方 |
|---|---|---|---|
| Ref / Bronze | Iceberg | 字典表、源系统原始明细 | 批加载 / Kafka 消费 |
| Silver | Iceberg | OWD 明细、OWS 汇总、版本历史 | dbt 建模 / SCD2 作业 |
| Gold | Iceberg | ADS 报表、明细、总账对账 | dbt 建模 |
| ADS | PostgreSQL | 同 Gold，供报送与闸读取 | 导出作业 |
| 控制与审计 | PostgreSQL | 预警、熔断、报送台账、重述登记、血缘 | 各环节脚本 |

## 快速开始

前提：两台服务器已按 `deploy/` 部署完毕，开发机能免密 ssh 到两台。凭据都在 `deploy/*/.env`（不入版本库）。

**首选路径——一条命令回到干净基线，再一条命令跑完一轮：**

```bash
# 1. 初始化环境：重生成样本数据、清派生表、清 Kafka 与消费位点，然后重跑全链路
bash deploy/reset-demo.sh --apply

# 2. 看结果：熔断状态、质量结论、报送台账、实时事件、Kafka 滞后、磁盘
ssh hermes@192.168.17.24 'cd ~/fr2052a-infra && bash run-daily-pipeline.sh health'
```

想看清理清单但不动手，去掉 `--apply` 即可（默认演练）。

**零依赖兜底——只想确认服务活着：**

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://192.168.17.22:8080/health   # Airflow
curl -s -o /dev/null -w "%{http_code}\n" http://192.168.17.22:9001          # MinIO 控制台
curl -s -o /dev/null -w "%{http_code}\n" http://192.168.17.24:8081          # Spark 主节点界面
```

**改代码之前先过闸：**

```bash
make lint                          # ruff 检查 + 格式检查 + mypy
git config core.hooksPath .githooks   # 做一次：之后每次提交前自动跑上面这条
```

没有预装 ruff / mypy 也能跑：`make lint` 会退到 `uvx` 现取（需要 uv）。要固定版本就命令行覆盖，例如 `make lint RUFF="uvx ruff@0.14.4"`。

## 怎么用（常见任务）

| 任务 | 命令（在 Server 2 的 `~/fr2052a-infra` 下执行） |
|---|---|
| 跑整条链路 | `bash run-daily-pipeline.sh` |
| 只跑某几个环节 | `bash run-daily-pipeline.sh dbt-run dq-rules export-pg` |
| 列出全部环节及说明 | `bash run-daily-pipeline.sh --help` |
| 让 Airflow 按环节接管 | 见 `deploy/server1/airflow/dags/`（5 个 DAG，环节调同一份编排） |
| 单跑接入或实时扫描 | `bash run-daily-pipeline.sh ods-replay realtime-scan` |
| GL 对账 | 由 `dbt-run` 产出结论、`verify-ads` 核对；单跑对账 DAG 见 `deploy/server1/airflow/dags/fr2052a_gl_reconciliation.py` |
| 查看表与样本 | `bash spark-submit-fr2052a.sh /opt/fr2052a-app/python/lakehouse/inspect_catalog.py`（不带参数即列出命名空间与表；看某张表的列加 `--describe silver.owd_deposits --sample 2`） |
| 时间旅行取证 | `bash spark-submit-fr2052a.sh /opt/fr2052a-app/python/audit/time_travel.py --table silver.owd_deposits_history --list-snapshots` |
| 重置环境 | 在开发机执行 `bash deploy/reset-demo.sh --apply` |
| 代码质量闸 | 在开发机执行 `make lint`（首次先 `git config core.hooksPath .githooks`） |

Airflow 界面在 `http://192.168.17.22:8080`，MinIO 控制台在 `http://192.168.17.22:9001`。

## 目录速览

```text
demo-fr2052a/
├── README.md                  # 人类入口（本文）
├── AGENTS.md                  # AI 编码约束
├── references/                # 原始需求归档（内容已拆解进 docs/business/）
├── docs/
│   ├── business/              # 工程文档（业务文档七份）
│   ├── tables/                # 表契约（一表一份）
│   ├── rules/                 # 规范文件（结构 / 编码 / 流程 / 验收）
│   └── build-log.md           # 构建日志（E0 起逐阶段记录）
├── deploy/
│   ├── server1/               # Server 1 部署（PG + MinIO + Airflow）与 Airflow DAG
│   ├── server2/               # Server 2 部署（Kafka + Spark）与跑批编排
│   ├── sync-deploy.sh         # deploy/ 同步与漂移检查
│   └── reset-demo.sh          # 一键初始化环境（默认演练）
├── sql/
│   ├── iceberg/               # Iceberg 表 DDL 与一次性数据修复
│   ├── postgres/              # 控制与审计层表结构、权限（会被跑批自动应用）
│   └── admin/                 # 只能手动执行的运维脚本（不被自动应用）
├── dbt/                       # dbt 项目（models / macros / schema 声明）
├── python/
│   ├── generators/            # 样本数据生成（固定种子，可复现）
│   ├── lakehouse/             # Iceberg 读写、SCD2、重述、表维护
│   ├── producers/ consumers/  # Kafka 生产与消费
│   ├── exporters/             # 导出到 PostgreSQL、报送文件生成
│   ├── validators/            # 数据质量、放行闸、报送核对
│   ├── alerts/                # 熔断判定、实时敞口扫描与汇总
│   ├── governance/            # 血缘、脱敏对照表、权限核对、巡检
│   └── audit/                 # 时间旅行与追溯
├── config/                    # 主题清单、阈值声明（唯一来源）
└── sample_data/               # 样本数据（可重建）
```

## 项目进度

| 阶段 | 内容 | 状态 |
|---|---|---|
| E0 | 环境侦察 | ✅ 完成 |
| E1 | Server 1 存储底座（PostgreSQL + MinIO） | ✅ 完成 |
| E2 | Server 2 计算底座（Kafka + Spark） | ✅ 完成 |
| E3 | 端到端穿透（Iceberg + MinIO + Spark + dbt） | ✅ 完成 |
| E4 | 业务开发（ODS → OWD → OWS → ADS） | ✅ 完成 |
| E5 | 编排接管（Airflow DAG + 数据质量引擎） | ✅ 完成 |
| E6 | 合规演示（熔断 / 报送 / 重述 / Time Travel） | ✅ 完成 |
| E7 | 治理收口（血缘 / 脱敏 / 权限 / 巡检） | ✅ 完成 |

完整过程见 [docs/build-log.md](docs/build-log.md)。

## 更多细节

| 主题 | 文档 | 何时看 |
|------|------|--------|
| 架构 / 技术栈 / 目录分层 | [docs/business/PROJECT.md](docs/business/PROJECT.md) | 要改代码、加模块、排部署问题时 |
| 数据流 / 表结构 | [docs/business/DATA-DESIGN.md](docs/business/DATA-DESIGN.md) | 看数据从哪来到哪去、字段含义时 |
| 单表明细（一表一份） | [docs/tables/](docs/tables/) | 查某张表的粒度、主键、分区、脱敏与质量规则时 |
| 模块边界 / 接口契约 | [docs/business/MODULE-DESIGN.md](docs/business/MODULE-DESIGN.md) | 对接某模块、改接口时 |
| 术语定义 | [docs/business/DOMAIN-LANGUAGE.md](docs/business/DOMAIN-LANGUAGE.md) | 对齐业务与技术语言时 |
| 踩过的坑 / 决策 | [docs/business/KNOWN-ISSUE.md](docs/business/KNOWN-ISSUE.md) | 遇到诡异报错、想少走弯路时 |
| 构建日志 | [docs/build-log.md](docs/build-log.md) | 想了解每一步怎么来的、踩过什么时 |
| 变更记录 | [docs/business/CHANGELOG.md](docs/business/CHANGELOG.md) | 了解演进历史时 |
| 规范文件 | [docs/rules/](docs/rules/) | 了解编码纪律与流程时 |
