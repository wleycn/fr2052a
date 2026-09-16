# FR 2052a 演示项目 — 构建日志

按构建步骤追加记录。每步记录：目标、产出、实测证据、踩到的坑、遗留项。

进度状态以 Studio 任务卡为准；本文件记录过程与证据，供后续查询。

## 环境坐标

| 角色 | 主机名 | IP | 职责 |
|---|---|---|---|
| dev | hm-ws | 192.168.17.11 | Hermes 所在机，开发与编排入口 |
| Server 1 | buz-data01 | 192.168.17.22 | 存储与治理：PostgreSQL、MinIO |
| Server 2 | buz-data02 | 192.168.17.24 | 计算与流处理：Kafka、Spark |

三台均为 Ubuntu 26.04.1、内核 7.0.0-31、8 核、15Gi 内存、可用磁盘 82G。

SSH：`hermes` 用户密钥免密互通，双向可达。sudo 免密已开通。Docker 29.8.x + Compose v5.5.1，`hermes` 在 `docker` 组内。

---

## E0 环境侦察（2026-09-16）

**目标**：把需求文档里的环境设想换成实测事实，先暴露阻塞项再排期。

**实测结论**

| 项 | 文档声称 | 实测 | 判定 |
|---|---|---|---|
| SSH 互通 | 是 | 密钥免密通，双向 ping 通 | 符合 |
| Docker 已装 | 是 | 29.8.1 + Compose v5.5.1，两机一致 | 符合 |
| 内存 | 建议 16GB | 15Gi，可用 13Gi | 符合 |
| CPU | 建议 4 核 | 8 核 | 优于预期 |
| 磁盘 | 未提 | 98G 总量，可用 82G | 充裕 |
| 端口 | 5432/9000/9001/8080/9002/9092/9094/8081/7077 | 全部空闲 | 无冲突 |
| sudo | 未提 | 需交互密码 | 已开通免密 |
| 系统 Python | 假定 3.x | 3.14.4 | 不兼容 GE，见下 |

**三个阻塞项与解法**

1. **sudo 需密码** → 已开通免密执行。
2. **服务器系统 Python 3.14.4 装不上 Great Expectations**（GE 要求 `>=3.10,<3.14`；另外 `pyspark==3.5.0` 也不支持 3.14）→ 在服务器上用 uv 装独立 Python。
3. **Docker Hub 拉取慢且两个关键镜像源已失效** → `minio/minio` 与 `bitnami/spark` 均已从 Docker Hub 下架；MinIO 改走 `quay.io`，Spark 改用官方 `apache/spark`。

**镜像源实测（结论与预期相反）**

同一镜像 `redis:6.0-alpine` 的拉取耗时：Docker Hub 直连 15.3s，经镜像源 19.6s，daocloud 18.8s，1ms.run 28.0s，rat.dev 22.8s。差异落在噪声范围内，镜像源没有实质加速。

真实瓶颈是网络带宽约 1 MB/s（MinIO 镜像 241MB 用时 166s）。镜像源保留作兜底，用于对冲 Docker Hub 偶发限流（实测出现过 82 秒拉 20MB）。

**遗留**

- 镜像源已写入两机的 `/etc/docker/daemon.json`，重启 docker 生效。
- 全栈镜像压缩后约 3–4GB，纯拉取预算约 1 小时；Server 1 与 Server 2 并行拉取可减半墙钟时间。

---

## E1 Server 1 存储底座（2026-09-16）

**目标**：在 Server 1 起 PostgreSQL 与 MinIO，并验证 dev 机可穿透访问。

**产出**

| 位置 | 内容 |
|---|---|
| dev `deploy/server1/` | `docker-compose.yml`、`.env.example`、`sql/init/01_schemas.sql` |
| Server 1 `~/fr2052a-infra/` | 同上，另加 `.env`（600 权限） |

部署方式：dev 侧保留可入库的源文件（不含 `.env`），部署为 `scp` + `docker compose --env-file .env up -d`。真实口令只存在于服务器上的 `.env`，不落回 dev 磁盘、不入版本库。

**版本选定**

| 组件 | 版本 | 说明 |
|---|---|---|
| PostgreSQL | `postgres:18-alpine` | 最新大版本，实测 18.6 |
| MinIO | `quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z` | 钉死版本，不跟 `latest` |
| MinIO 客户端 | `quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z` | 一次性容器，负责建桶 |

**验证证据**

| 判据 | 实测 |
|---|---|
| postgres 容器 | `Up (healthy)` |
| PostgreSQL 版本 | 18.6 |
| 初始化脚本生效 | schema `ads`、`audit` 已建 |
| 容器时区 | Asia/Shanghai |
| minio 容器 | `Up`，health 返回 200 |
| 存储桶 | `fr2052a-bucket`，private，版本化已开启 |
| dev → `22:5432` | 连通，`select` 查出 `ads,audit` |
| dev → `22:9000` | HTTP 200，1.2ms |
| dev → `22:9001` | HTTP 200，1.2ms |

**坑：PostgreSQL 18 改了数据目录约定**

首次启动直接失败，日志报 `there appears to be PostgreSQL data in /var/lib/postgresql/data (unused mount/volume)`。

原因：`postgres:18-alpine` 的 `PGDATA` 变为 `/var/lib/postgresql/18/docker`，镜像的 `VOLUME` 声明在 `/var/lib/postgresql`。按 17 及以前的习惯把卷挂在 `/var/lib/postgresql/data` 会被判定为废弃挂载并拒绝启动。

修法：卷改挂 `/var/lib/postgresql`。重建数据卷时必须沿用此路径。

**存储分工的落地**

PostgreSQL 内**只**建了 `ads` 与 `audit` 两个 schema。`ods`、`owd`、`ows`、`ref` 一个都没建——它们归 Iceberg 数据湖。分工从环境层面固化，避免后续误把 ODS 建进 PG。

---

## E2 Server 2 计算底座（2026-09-16）

**目标**：在 Server 2 起 Kafka 与 Spark，并验证 dev 机可穿透访问。

**产出**

| 位置 | 内容 |
|---|---|
| dev `deploy/server2/` | `docker-compose.yml`、`.env.example` |
| Server 2 `~/fr2052a-infra/` | 同上，另加 `.env`（600 权限） |

**版本选定**

| 组件 | 版本 | 说明 |
|---|---|---|
| Kafka | `apache/kafka:4.3.1` | KRaft 单节点，无 ZooKeeper；关闭隐式建主题 |
| Spark | `apache/spark:3.5.9` | 见下方"为什么不是 Spark 4" |

**为什么不是 Spark 4**

Iceberg 按 Spark 版本发布运行时包，目前只有 `iceberg-spark-runtime-3.5_2.12` 与 `iceberg-spark-runtime-4.0_2.13`，**没有 Spark 4.1/4.2 对应的运行时**。因此 Spark 只能在 3.5.x 与 4.0.x 之间选。3.5.9 是 3.5 线最后一个补丁版，且 dbt-spark、PySpark、Iceberg 三方都对它提供支持；4.0.4 虽然技术上可行，但 dbt-spark 对 Spark 4 的支持未经验证。取舍原则是"整条链路都有官方支持的版本里取最高"。

Kafka 无此约束，直接取最新的 4.3.1。

**资源分配**：Spark Worker 给 4 核 / 6G（文档原写 2 核，机器实际 8 核，留一半余量给 Kafka 与系统）。

**验证证据**

| 判据 | 实测 |
|---|---|
| 容器状态 | kafka `Up (healthy)`、spark-master `Up`、spark-worker `Up` |
| Spark Master | `status=ALIVE`，`aliveworkers=1`，`cores=4`，`mem=6144` |
| Worker 注册 | 日志 `Successfully registered with master spark://spark-master:7077` |
| Kafka 自产自销 | 建 2 分区主题，生产 `smoke-payload-001`，消费者取回同值 |
| 自测主题清理 | 删除后仅剩 `__consumer_offsets` |
| dev → `24:7077/8081/8082/9092/9094` | 全部连通 |
| dev → `24:8081`、`24:8082` | HTTP 200，约 0.09s |

**坑：镜像源不是加速器，是瓶颈**

两轮实测下来，这个问题花了最多时间，结论和直觉相反。

| 路径 | 实测速度 |
|---|---|
| 裸下载（阿里云 Maven，同机同时刻） | 1.66 MB/s |
| 经 daocloud 镜像源拉镜像 | 148 KB/s |
| 经 1ms.run 镜像源拉镜像 | 152–372 KB/s |

机器上行是好的，慢的是到 Docker 镜像源的链路。`apache/spark:3.5.9` 镜像 1.86GB，整个过程超过 40 分钟。

**应对策略（后续沿用）**：大镜像不走 daemon 全局镜像源，改用显式前缀拉取再打回标准标签。

```bash
docker pull docker.1ms.run/apache/spark:3.5.9
docker tag  docker.1ms.run/apache/spark:3.5.9 apache/spark:3.5.9
```

这样 compose 文件里始终写标准镜像名，镜像不与某个镜像源绑定。

**遗留**

- Server 2 目前没有 Iceberg 运行时包，E3 需补齐（从阿里云 Maven 取，该源实测 1.66 MB/s，快）。
- Kafka 主题命名方案尚未定（三套候选），E3 前需要拍板。

---

## E3 端到端穿透（2026-09-16 / 17 跨零点）

**目标**：证明 Spark（Server 2）能读写 MinIO（Server 1）上的 Iceberg 表，且 dbt 能同时连 PostgreSQL 与数据湖。

**产出**

| 位置 | 内容 |
|---|---|
| dev `deploy/server2/spark/` | `spark-defaults.conf`、`iceberg_smoke.py`、`jars/`（不入库） |
| dev `deploy/server2/` | `fetch-deps.sh`、`setup-venv.sh`、`run-dbt.sh`、`spark-submit-fr2052a.sh` |
| dev `dbt/` | `dbt_project.yml`、`profiles.yml`、`models/smoke/` |
| Server 2 `~/fr2052a-infra/` | 以上全部，另加 `venv/`、`jdk17/` |
| Server 1 PostgreSQL | 新增 `iceberg_catalog` schema（存 Iceberg 表元数据） |

**版本选定**

| 组件 | 版本 | 说明 |
|---|---|---|
| Iceberg | 1.11.0 | `iceberg-spark-runtime-3.5_2.12` + `iceberg-aws-bundle` |
| OpenJDK | 17.0.1 | 镜像自带 Java 11 不满足 Iceberg 1.11 |
| PostgreSQL JDBC | 42.7.13 | Iceberg JDBC catalog 用 |
| dbt | core 1.12.5 / spark 1.11.0 / postgres 1.11.0 | venv 基于 Python 3.11 |
| PySpark | 3.5.9 | 与 Spark 集群同版本 |

**存储分工（方案 A 的落地）**

Iceberg 的**表元数据落 PostgreSQL**（JDBC catalog），**数据文件落 MinIO**（S3FileIO）。
命名空间：`ref`、`bronze`（ODS）、`silver`（OWD/OWS）。PostgreSQL 侧另有 `ads`、`audit` 两个业务 schema。

**验证证据**

| 判据 | 实测 |
|---|---|
| Spark 穿透自检 | `iceberg_smoke.py` 通过：建 4 个命名空间、写 3 行、读回一致 |
| Iceberg 表元数据 | PG `iceberg_catalog.iceberg_tables` 有 `bronze.smoke_check`、`silver.spark_smoke` |
| 数据文件 | MinIO `warehouse/bronze/smoke_check/` 与 `warehouse/silver/spark_smoke/` 各有 parquet + metadata + snapshot |
| dbt pg target | `OK created sql view model ads.pg_smoke` |
| dbt spark target | `OK created sql table model silver.spark_smoke [OK in 3.04s]` |

**踩到的坑（按代价排序）**

1. **dbt-spark 的 Spark 配置字段名是 `server_side_parameters`，不是 `spark_conf`。**
   写错不报错，只静默忽略，会话会退化成宿主机上的本地 Spark + Hive catalog，表现为一堆莫名其妙的
   `does not support truncate in batch mode`。这是本次最贵的坑，排查了大半小时。

2. **Iceberg 1.11 要求 Java 17，`apache/spark` 镜像自带 Java 11。**
   解法是外挂一份 JDK 17 并设 `JAVA_HOME`，不重新拉镜像（镜像 1.86GB，拉取要 40 分钟）。

3. **JDK 17.0.1 在内核 7.0 上探测 cgroup 抛 NPE。**
   三个位置要分别处理：容器进程靠 `spark.driver/executor.extraJavaOptions`；
   dbt 的 session 驱动由 pyspark 拉起，靠 `JAVA_TOOL_OPTIONS`；
   执行器必须在应用配置里显式下发，否则启动即 `exited with code 50`。

4. **Hadoop catalog 走不通。** 它要求 `s3a://` 协议栈，得再拉 300MB 的 AWS SDK。
   改用 Iceberg 的 JDBC catalog，只多一个 1MB 的 PG 驱动，且元数据落 PG 更好讲。

5. **Iceberg 的 S3 区域属性名是 `client.region`，不是 `s3.region`。**

6. **必须让 Iceberg 的 SessionCatalog 接管 `spark_catalog`。**
   否则未限定的表名会落到 Spark 内置 catalog，表既不在数据湖里，覆盖写也会报
   `does not support truncate in batch mode`。

7. **大镜像不走全局镜像源。** 走显式前缀拉取再打回标准标签，见 E2 的结论。

8. **session 模式下 `spark.jars` 注入的包赶不上 DataSource 注册时机。**
   把 Iceberg 运行时包直接放进 pyspark 自带的 `jars/` 目录（`run-dbt.sh` 每次同步）。

**遗留**

- Iceberg 的命名空间（`ref`/`bronze`/`silver`）由底层 session catalog 管理，不落在 JDBC catalog；
  重建环境时需要重新建命名空间（`iceberg_smoke.py` 可重复执行）。
- dbt 的 spark target 目前只有连通性自检模型，正式模型在 E4 落地。

---

## E4.1 合成数据生成器（2026-09-17）

**目标**：产出 REF（引用数据）与 ODS（业务明细）两套 CSV，后续分别走批加载与 Kafka 流入数据湖。

**产出**

| 位置 | 内容 |
|---|---|
| dev `python/generators/config.py` | 常量、通用 IO、按表名派生的独立随机源 |
| dev `python/generators/ref_data.py` | 9 张引用数据表 |
| dev `python/generators/ods_data.py` | 7 张业务明细表 |
| dev `python/generators/generate_sample_data.py` | 命令行入口 + 39 项完整性自检 |
| dev `sample_data/` | 生成结果 16 个 CSV、380KB（已在 .gitignore 中） |

目录位置按 `[99] §七` 的建议结构落在 `python/` 下。

**相对 `[97]` 原始脚本的三处修正**

| 修正 | 原因 |
|---|---|
| 每行统一带 `report_date` | 原方案只有总账表带报告日，其余表无法按日切分与重跑，日报批次缺锚点 |
| 每行统一带 `entity_code` | 合并口径与重要实体子公司口径都要用（`[00] §1.1.2`） |
| 日期按业务含义生成 | 原方案从交易日历抽开户日，会抽出晚于报告日的"未来开户"；现值域为：开户/起始/购买/交易日早于报告日，到期日一律晚于报告日 |

**数据规模**：REF 9 表 + ODS 7 表，业务明细合计 1500 行。各表行数集中在 `config.py` 的 `VOLUMES` 里定义。

**验证证据**

| 判据 | 实测 |
|---|---|
| 完整性自检 | 39 项全 PASS，0 项 FAIL |
| 引用自洽 | 实体、币种、交易对手全部命中 REF，无悬空引用 |
| 报告日一致 | 7 张 ODS 表全部落在 2026-09-16 |
| 金额符号 | 6 张金额表的借贷、本金、额度全部非负 |
| 总账平衡 | 借方 = 贷方 = 1,899,251,812.73，差额 0.00 |
| 可复现性 | 同参数连跑两次，16 个文件逐字节一致（md5 全等） |
| 缺口开关 | `--gl-break-amount 5000000` 得到差额恰为 -5,000,000.00，退出码 1 |

**坑（三处都与同一类问题有关：数据看着对，业务上讲不通）**

1. **平账分录打在了错误的科目上。** 首版把平账额加在"最后一行"，而最后一行是股东权益科目，造出借方 -83,866,346.01 的负余额。改为：平账分录固定挂在待清算科目（9001），且只补在余额不足的一侧，任何一行都不可能出现负数。
2. **借贷两侧各自随机会让待清算科目扛下整个差额。** 首版两侧独立抽随机数，天生不等，待清算科目被迫承载 3.16 亿（占账簿 14%）。改为先记满借方科目、再把等额总量按权重分摊到贷方科目，两侧自然相等，待清算科目只剩未达账项（占比 2.45%）。
3. **自检只查"借贷合计相等"，查不出负余额。** 因此新增「金额非负」检查，把这类错误纳入闸内。

**遗留**

- CSV 尚未入湖：REF 走批加载、ODS 经 Kafka 流入 Iceberg，在 E4.2 / E4.3 完成。
- `report_date` 与 `entity_code` 是对 `[99]` ODS 表结构的扩展，E4.2 的 Iceberg 建表语句要体现，并同步到项目文档。

---

## 后续步骤

E4 业务开发（数据生成器 → ODS → OWD → OWS → ADS）→ E5 编排 → E6 合规演示剧本 → E7 治理收口。
