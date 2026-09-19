# FR 2052a — 工程地图

> 定位：FR 2052a 监管流动性报表数据处理演示项目。支持 Core（PG + dbt + Airflow）与 Advanced（Kafka + Iceberg + Spark）双轨架构。

## 技术栈

| 层级 | 组件 | 版本 |
|------|------|------|
| 调度 | Apache Airflow（LocalExecutor） | 2.10.5 |
| 转换 | dbt-postgres + dbt-spark[PySpark] | dbt 1.12.5 |
| 数据质量 | 自研规则引擎（规则落 `ref.ref_validation_rules`） | - |
| 元数据/血缘 | dbt `schema.yml` 的 meta 声明 + 自研渲染（`render_lineage.py`） | - |
| 消息 | Apache Kafka（KRaft 单节点） | 4.3.1 |
| 对象存储 | MinIO | RELEASE.2025-09-07T16-13-09Z |
| 开放表格式 | Apache Iceberg（JDBC catalog） | 1.11 |
| 计算引擎 | Apache Spark Standalone（容器内 JDK 17） | 3.5.9 |
| 结果库 | PostgreSQL | 18.6 |
| 脱敏 | dbt 宏 + PostgreSQL secure schema（加盐 SHA-256） | - |
| 巡检 | 自研 `pipeline_health.py`（熔断 / 校验 / 预警 / 报送 / 重述 / 实时事件 / 连接 / 滞后 / 磁盘） | - |
| 监控 | Prometheus + Grafana（指标来自巡检脚本的 `--json` 输出，Server 2 采集、Server 1 出面板） | Prometheus 2.55.1 / Grafana 11.5.1 |
| 代码质量闸 | ruff（检查 + 格式化）+ mypy，配置在项目根 `pyproject.toml` | ruff 0.14.4 / mypy 1.18.2 |

版本口径：表里的版本是**实测值**。可核的 pin 在 `deploy/server1/.env.example`（Airflow 与 MinIO 镜像）与 `deploy/server2/setup-venv.sh`（dbt 三件套与 PySpark）；PostgreSQL 镜像只 pin 到主版本 `postgres:18-alpine`，补丁版本由镜像发布方决定。

## 目录分层

```
demo-fr2052a/
├── README.md                  # 人类入口（必留根）
├── AGENTS.md                  # AI 编码约束
├── references/                # 原始需求归档（参考，不落九文档）
├── docs/
│   ├── business/              # 业务文档（七份）
│   │   ├── PROJECT.md         # 本文档（工程地图）
│   │   ├── DATA-DESIGN.md     # 数据流 + 数据结构
│   │   ├── MODULE-DESIGN.md   # 功能模块设计
│   │   ├── INTERFACE-DESIGN.md# 接口契约
│   │   ├── DOMAIN-LANGUAGE.md # 术语表
│   │   ├── CHANGELOG.md       # 变更记录
│   │   └── KNOWN-ISSUE.md     # 已知坑 + 决策记录
│   ├── tables/                # 表契约（一表一份：56 张）
│   ├── rules/                 # 规范文件（四件套）
│   │   ├── PROJECT-STRUCTURE.md
│   │   ├── CODING-STANDARD.md
│   │   ├── DEVELOP-FLOW.md
│   │   └── ACCEPTANCE-CHECKLIST.md
│   ├── CRON-DESIGN.md         # 定时任务说明（DAG 调度与服务器 cron 的单一落点）
│   ├── changes/               # 变更留痕（逐笔，每模块一份）
│   └── build-log.md           # 构建日志（逐阶段记录）
├── deploy/
│   ├── server1/               # Server 1 部署清单（PG + MinIO + Airflow + 监控）与 Airflow DAG
│   ├── server2/               # Server 2 部署清单（Kafka + Spark + 指标暴露）与跑批编排
│   ├── sync-deploy.sh         # deploy/ 同步与漂移检查
│   └── reset-demo.sh          # 一键初始化环境（默认演练）
├── sql/
│   ├── iceberg/               # Iceberg 表 DDL（引用真源）
│   ├── postgres/              # 控制与审计层表结构、权限（跑批会自动应用）
│   └── admin/                 # 只能手动执行的运维脚本（不被自动应用）
├── dbt/
│   ├── models/                # dbt 模型（OWD/OWS/ADS）与列级声明 schema.yml
│   ├── macros/                # dbt macro（脱敏、HQLA 规则）
│   ├── dbt_project.yml        # 项目配置与变量（脱敏模板、报表码）
│   └── profiles.yml           # 连接目标（pg / spark）
├── python/
│   ├── generators/            # 样本数据生成器
│   ├── lakehouse/             # Iceberg 读写、SCD2、重述、表维护
│   ├── producers/             # Kafka 生产
│   ├── consumers/             # Kafka 消费入湖
│   ├── exporters/             # 导出到 PostgreSQL、报送文件生成
│   ├── validators/            # 数据质量、放行闸、报送核对
│   ├── alerts/                # 熔断判定、实时敞口扫描与汇总
│   ├── governance/            # 血缘、脱敏对照表、权限核对、巡检
│   └── audit/                 # 时间旅行与追溯
├── config/
│   ├── pipeline_topics.json   # 主题契约（唯一来源）
│   └── liquidity_thresholds.json  # 阈值契约（唯一来源）
├── todo/                      # 交接单（会话接力，状态写在文件名后缀）
└── sample_data/               # 样本数据（可重建，不落 git）
```

### 分层纪律

| 目录 | 职责 | 谁写 |
|------|------|------|
| `references/` | 原始需求归档（参考，不落九文档） | 项目启动时产生 |
| `docs/business/` | 业务文档七份（九文档体系的正文部分，真源） | 编码前/后同步更新 |
| `docs/tables/` | 表契约（一表一份：层级 / 粒度 / 主键 / 去重 / 分区 / 金额口径 / PII / 生命周期 / SLA / 依赖 / 质量规则） | 新增或变更表时同步 |
| `docs/rules/` | 跨项目复用规范 | 架构师制定，全员遵守 |
| `docs/build-log.md` | 构建日志（逐阶段追加） | 每阶段完成时追加 |
| `docs/CRON-DESIGN.md` | 全部定时任务的单一说明（DAG 调度与服务器 cron） | 调度变更时同步 |
| `deploy/` | 部署清单（不含凭据） | 运维/DevOps |
| `sql/` | DDL（真源） | DBA/架构师 |
| `dbt/models/` | 转换逻辑（真源） | 数据工程师 |
| `python/` | 脚本工具（薄壳） | 数据工程师 |
| `config/` | 配置契约 | 架构师 |
| `sample_data/` | 样本数据（可重建） | 测试人员 |

## 接口契约摘要

完整接口定义见 [INTERFACE-DESIGN.md](INTERFACE-DESIGN.md)。

| 接口类型 | 位置 | 说明 |
|----------|------|------|
| dbt macro | `dbt/macros/` | 脱敏、HQLA haircut、到期分桶 |
| Python CLI | `python/` | 生成/加载/导出/校验 |
| Kafka Topic | `config/pipeline_topics.json` | 11 个 Topic 契约（其中 8 个有生产者，落 bronze）|
| Airflow DAG | `deploy/server1/airflow/dags/` | 5 个 DAG 定义 |

## 运行环境 + 验证命令

| 组件 | 端口 | 验证命令 |
|------|------|----------|
| PostgreSQL | 5432 | `psql -h 192.168.17.22 -U fr2052a -d fr2052a_db -c "SELECT 1"` |
| MinIO API | 9000 | `curl http://192.168.17.22:9000/minio/health/live` |
| MinIO Console | 9001 | `curl -I http://192.168.17.22:9001` |
| Airflow Web | 8080 | `curl -I http://192.168.17.22:8080` |
| Kafka（跨机） | 9094 | `kafka-topics --bootstrap-server 192.168.17.24:9094 --list` |
| Kafka（容器内） | 9092 | 只在 Server 2 的容器网络里可用，广告地址是 `kafka:9092` |
| Spark Master | 8081 | `curl -I http://192.168.17.24:8081` |
| 监控 node_exporter | 9100 | `curl -s http://192.168.17.24:9100/metrics \| grep -c '^fr2052a_'` |
| Prometheus | 9090 | `curl -s http://192.168.17.22:9090/-/healthy` |
| Grafana | 3000 | `curl -s http://192.168.17.22:3000/api/health` |

监控按简单版接上：Server 2 的 `health_to_metrics.sh` 每 5 分钟把 `pipeline_health.py --json` 的巡检结论写成 node_exporter 能读的指标文件（15 个指标），Server 1 的 Prometheus 抓它、Grafana 出面板（`http://192.168.17.22:3000`，匿名只读）。6 条告警规则在 Prometheus 与 Grafana 里可见，未接推送通道。血缘由 `render_lineage.py` 渲染成 Markdown 报告；BI 不做，报表落 PG 后直接用 `psql` 查。边界与代价见 `KNOWN-ISSUE.md` 的「监控与 BI 栈未落地」行。

Airflow 里的 DAG 默认**暂停**：`fr2052a_daily_batch`、`fr2052a_backfill_and_restate`、`fr2052a_realtime_alert`、`fr2052a_submission` 四个是暂停的，只有 `fr2052a_gl_reconciliation` 是放开的。日常跑批走 Server 2 的 `run-daily-pipeline.sh`，不经过调度器；要演示「由 Airflow 编排」时，先在 Web UI 或 `airflow dags unpause <dag_id>` 放开对应 DAG，再手动触发。

代码质量闸在 dev 机跑：`make lint`（ruff 检查 + 格式检查 + mypy）。执行一次 `git config core.hooksPath .githooks` 后，每次提交前自动跑。

验收清单见 [ACCEPTANCE-CHECKLIST.md](../rules/ACCEPTANCE-CHECKLIST.md)，构建过程与踩坑见 [build-log.md](../build-log.md)。

## 文档导航

| 主题 | 文件 | 用途 |
|------|------|------|
| 项目定位 / 技术栈 / 上手 | ../README.md | 5 秒了解项目 |
| 数据流 + 数据结构 | DATA-DESIGN.md | 端到端流与表结构 SSOT |
| 功能模块设计 | MODULE-DESIGN.md | 模块边界与接口 |
| 接口契约 | INTERFACE-DESIGN.md | dbt/Python/Kafka/Airflow 契约 |
| 术语定义 | DOMAIN-LANGUAGE.md | 中英对照术语表 |
| 已知坑 | KNOWN-ISSUE.md | 踩过的硬坑与决策 |
| 变更记录 | CHANGELOG.md | 演进历史 |

## 已知坑

详见 [KNOWN-ISSUE.md](KNOWN-ISSUE.md#已知坑与决策)。

- `#pg18-data-dir-change` — PG 18 改了数据目录约定
- `#spark-minio-endpoint` — Spark 连接 MinIO 必须用 IP，不能用 localhost
- `#gl-reconciliation-mismatch` — GL 对账需按 Section 汇总后比对
- `#hqla-cap-not-applied` — HQLA 二级资产上限需显式截断（基数口径见 `#hqla-cap-basis`）
- `#hqla-cap-basis` — HQLA 二级资产上限的基数是扣除后的 HQLA，等价于一级资产的 2/3
- `#section-a-d-empty` — 报表 Section A 与 D 的 6 列在演示环境恒为 NULL
- `#catalog-name-drift` — Iceberg catalog 改名后旧注册行还在，清理脚本成了空操作
- `#spark-decimal-division` — Spark 的 DECIMAL 除法只给 6 位小数，比例运算要先乘后整除
- `#l2-recognized-not-split` — 二级资产认列额不单列，按「认列总额 − 一级市值」取
- `#python314-incompatible` — Python 3.14 不兼容 GE 与 pyspark
- `#dockerhub-image-removed` — minio/spark 官方镜像已从 Docker Hub 下架
- `#detail-report-mismatch` — 明细与报表口径不一致（正回购/30天过滤）
- `#scd2-reversed-interval` — 版本区间不得反向（失效日早于生效日）
- `#dead-ows-tables` — 三张 OWS 汇总表无消费者（登记待下线）
- `#report-history-reset-exception` — 版本历史表不在复位清单（历史不可变是硬性质）
- `#delegate-audit-20260917` — 三路独立审查的 80 条发现与处置
- `#recon-benchmark-synthetic` — Section E 的独立基准在演示环境里是构造出来的
- `#cumulative-gap-column-removed` — 累计缺口列删除（与净缺口同值）
- `#row-hash-baseline-reset` — 改 row_hash 定义必须先重建版本基线
- `#export-after-submission-gate` — 已报送期的覆盖写闸（内容指纹判据）
- `#retired-report-id-in-ledger` — 报表身份改名后，台账会留下对不上报表的孤儿行
- `#monitoring-stack-scope` — 监控接 Prometheus 与 Grafana，指标来自巡检脚本，不推送
- `#audit-access-log-no-writer` — 读取审计表当前没有写入方
- `#read-audit-gap` — 读取级留痕未部署（pgaudit / 语句日志）
