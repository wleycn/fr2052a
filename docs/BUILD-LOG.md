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
  → **该结论已被 E4.2 推翻并修正**：SessionCatalog 的方案换成了独立命名 catalog。
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

## E4.2 数据湖建表 + 防漂移闸（2026-09-17）

**目标**：把 16 张表建进数据湖（ref 9 张 + bronze 7 张），并用机器闸锁死表结构与生成器 CSV 表头的一致性。

**产出**

| 位置 | 内容 |
|---|---|
| dev `sql/iceberg/01_create_ref_tables.sql` | REF 9 张表，逐列带中文业务含义 |
| dev `sql/iceberg/02_create_ods_tables.sql` | ODS 7 张表，按 `days(report_date)` 分区 |
| dev `python/lakehouse/run_sql_file.py` | 把 .sql 逐条交给 Spark SQL 执行，任一失败即退出非零 |
| dev `python/lakehouse/verify_ods_schema.py` | 防漂移闸：表结构与 CSV 表头逐列比对 |
| dev `python/lakehouse/inspect_catalog.py` | 目录巡检：命名空间、表清单、抽检行数 |
| dev `deploy/server2/sync-app.sh` | 把 `python/ sql/ dbt/ sample_data/` 同步到 Server 2 |
| dev `deploy/server2/docker-compose.yml` | Spark 容器新增 `./app:/opt/fr2052a-app:ro` 挂载 |

**验证证据**

| 判据 | 实测 |
|---|---|
| 建表 | ref 9 张、bronze 7 张，`SHOW TABLES` 与 PG `iceberg_catalog.iceberg_tables` 双向一致 |
| 防漂移闸 | 16 张表全部 PASS，列序与列名逐一吻合 |
| 目录自省 | `SHOW NAMESPACES` 返回 default/ref/bronze/silver |

**坑（按代价排序）**

1. **SparkSessionCatalog 会吞掉命名空间。** 这是本步最贵的一个。
   表建成功了，但 `SHOW NAMESPACES` 只剩 `default`、`SHOW TABLES IN ref` 报 `SCHEMA_NOT_FOUND`；
   同时 `SELECT count(*) FROM ref.ref_calendar` 却能跑通 —— 表可按名寻址，只是目录自省失效，
   所以症状极隐蔽：跑数据没事，dbt、BI、血缘采集这类依赖自省的环节会集体失灵。
   根因是 SparkSessionCatalog 把命名空间交给 Spark 自身管理，命名空间不落 JDBC catalog。
   改法：**推翻 E3 的 SessionCatalog 选择**，改用独立命名 catalog（`lakehouse`），
   命名空间与表都落 PostgreSQL；未限定的 `db.table` 由 `spark.sql.defaultCatalog=lakehouse` 兜住，
   依旧不必加前缀。

2. **挂载点不能嵌在只读挂载里。** 先按 `/opt/spark/fr2052a/app` 挂，容器直接起不来：
   `mkdirat ... /opt/spark/fr2052a/app: read-only file system` —— 父挂载是只读的，runc 建不了挂载点。
   改挂 `/opt/fr2052a-app`。

3. **rsync 多源带尾斜杠会把目录摊平。** `python/ sql/ dbt/ sample_data/` 四份内容全摊到同一层，
   且只带 `--delete` 清不掉残骸（多源时它不覆盖目标根目录）。改为先清空目标、再逐树同步。

4. **自检规则套错了层。** 「必须有 `etl_load_timestamp`」是 bronze 的规矩，却套到了 ref 表上，
   9 张 ref 全 FAIL。改为按层分白名单。教训：闸报错时要先确认判据本身对不对，别急着改数据。

**遗留**

- ADS 层 PostgreSQL 建表脚本未写（E4.4 随 dbt 模型一起落）。
- E3 遗留的两个自检表（`bronze.smoke_check`、`silver.spark_smoke`）待清理。

---

## E4.3 数据入湖：ref 批加载 + ODS 经 Kafka 流入 bronze（2026-09-17）

**目标**：把两类数据按各自该走的路送进数据湖 —— REF 走批加载，ODS 走 Kafka 实时流入。

**产出**

| 位置 | 内容 |
|---|---|
| dev `config/pipeline_topics.json` | 主题与数据湖落点的唯一声明，建主题/生产者/消费者共读一份 |
| dev `deploy/server2/create-topics.sh` | 按声明建主题，幂等 |
| dev `deploy/server2/prepare-runtime-dirs.sh` | 建宿主机侧运行时目录并改属主 |
| dev `python/lakehouse/load_ref_tables.py` | REF 批加载，整表覆盖写保证幂等 |
| dev `python/lakehouse/verify_bronze.py` | bronze 层与样本 CSV 的行数核对 |
| dev `python/producers/replay_ods_to_kafka.py` | 把 ODS 明细按主题重放进 Kafka |
| dev `python/consumers/kafka_to_iceberg.py` | 消费 7 个主题，按主键 MERGE 入 bronze |
| dev `deploy/server2/fetch-deps.sh` | 新增 Kafka 连接器依赖，下载带校验与换源 |

**主题映射**（一张 ODS 表对一个主题）

| ODS 表 | Kafka 主题 | 源系统 |
|---|---|---|
| ods_deposits | core_banking_txns | CORE_BANKING |
| ods_loans | loan_book | LOAN_SYS |
| ods_repo_transactions | treasury_deals | TREASURY_SYS |
| ods_securities | custody_positions | CUSTODY_SYS |
| ods_derivatives | derivatives_trades | DERIV_SYS |
| ods_gl_balances | gl_entries | FINANCE_SYS |
| ods_off_bs_commitments | off_bs_commitments | OFFBS_SYS |

另有 3 个主题只作声明、本演示无生产者：`market_data_prices`、`reference_data_updates`、`fr2052a_alerts`（后者归 E6 使用）。

**验证证据**

| 判据 | 实测 |
|---|---|
| REF 批加载 | 9 张全成功，行数与生成器一致；重跑后行数不变（覆盖写幂等） |
| 类型落位 | `spot_rate` 落成 `decimal(18,8)`，空 `expiry_date` 落成 NULL |
| 主题 | 10 个主题建成，7 个有数据 |
| 生产者 | 1500 条消息写入，逐主题计数与 CSV 行数一致 |
| broker 落盘 | 7 个数据主题偏移量合计 1500 |
| bronze 层 | 7 张表合计 1500 行，与 CSV 逐表吻合 |
| 幂等入湖 | 重放 1500 条**重复**消息后再消费，各表行数不变（MERGE 按主键去重） |

**坑（三处，都属于"报错信息指向的地方不是真因"）**

1. **下载到的 jar 其实是 HTML。** 我新写的 `fetch_jar` 把目录路径当成了文件路径
   （只写到 `.../kafka-clients/3.9.0`，漏了文件名），镜像对这个不存在的文件返回
   **200 + 目录列表 HTML**，而 `curl -f` 只认 4xx/5xx，于是 HTML 被当 jar 存了下来。
   后果是运行时报 `Failed to find data source: kafka` —— 一个跟下载完全无关的错。
   修法：URL 带上文件名，且下载后按 **zip 魔数**验身，不合格就换下一个源。
   顺带把 Iceberg 与 PG 驱动的下载也切到同一函数，一并获得校验与兜底。
2. **检查点目录写不进去。** 绑定挂载的源目录不存在时 Docker 会自动创建，属主是 root，
   而容器内进程是 uid 185(spark)。流式作业报 `mkdir of file:/opt/fr2052a-checkpoints/... failed`，
   看起来像检查点配置问题，实际是目录权限。修法：`prepare-runtime-dirs.sh` 预建并改属主。
3. **收尾汇总会撒谎。** 消费完成后打印的 bronze 行数整列是 0，而表里其实有 1500 行；
   原因是同一会话早先解析过的表握着旧快照。修法：计数前 `refreshTable`。
   教训：**自报数字必须能被独立复核** —— 我是靠另起一个作业跑核对脚本才确认数据没丢的。

**遗留**

- dbt 四层模型（ODS → OWD → OWS → ADS）是 E4.4 的主体，尚未开始。
- `market_data_prices` 等 3 个声明主题暂无生产者，`SHOW` 时为空属预期。

---

## E4.4a dbt 项目骨架 + OWD 标准化层（2026-09-17）

**目标**：把 ODS 明细加工成 FR 2052a 口径的标准化明细（外币折算 USD、口径归一、到期分桶、HQLA 分级）。

**产出**

| 位置 | 内容 |
|---|---|
| dev `dbt/macros/fr2052a_rules.sql` | 监管口径宏：到期分桶、HQLA 分级与折扣率、客户细分、存款产品归一 |
| dev `dbt/macros/generate_schema_name.sql` | 覆盖 dbt 默认 schema 命名（否则 silver + gold 会拼成 silver_gold） |
| dev `dbt/models/sources.yml` | 声明 ref 与 bronze 两层上游 |
| dev `dbt/models/staging/` | 7 张 OWD 模型 + 汇率基准 |
| dev `python/lakehouse/verify_silver.py` | OWD 层核对：行数、分桶合法性、折算逐行重算 |
| dev `sql/iceberg/00_create_namespaces.sql` | 三个命名空间独立成脚本（silver 归 dbt 写） |

**逐层配置**：`staging` 与 `intermediate` 落 Iceberg `silver` 命名空间，`marts` 落 `gold`。

**顺带修掉的配置漂移**：`dbt/profiles.yml` 里的 spark target 仍指向 E4.2 已废弃的 SessionCatalog，
不改会在下一跑 dbt 时报表找不到。同步改成 `lakehouse` catalog + `defaultCatalog`。
`run-dbt.sh` 的工程目录也从 `~/fr2052a-infra/dbt` 改指同步过来的 `app/dbt`，并删除旧副本 ——
否则两份 dbt 工程会互相漂移。

**OWD 模型的取舍**：需求文档 `[99] §3.4` 的列比本演示的 ODS 字段多（如 `yield_to_maturity`、
`pd_pct`、`lgd_pct`、`ccp_name`）。这些列没有数据来源，我不建空列充数，只实现能从 ODS + REF
推导出来的部分。缺口记录在此，避免以后误以为已经覆盖。

**验证证据**

| 判据 | 实测 |
|---|---|
| dbt 运行 | 8 个模型全部成功（13.5 秒） |
| 行数 | 7 张 OWD 与各自 ODS 上游逐表一致（1500 行） |
| 到期分桶 | 各表分桶取值全部落在 `ref_maturity_bucket` 定义内 |
| 汇率折算 | 逐行按 ref 汇率重算后与 USD 金额比对：1450 行，0 条偏差 |
| 口径落地 | 存款 `principal_amount_usd` 已按实体本位币折算，HQLA 分级与折扣率由宏统一产出 |

**踩到的坑**

- 核对脚本把总账表也纳入到期分桶检查，而总账没有到期日概念，直接报列不存在。
  改为按表声明能力，而不是对所有表套同一套检查。
- `.groupBy(...).collect()` 是错的写法，Spark 会返回 `GroupedData` 而非结果集。改用 `select().distinct()`。

---

## E4.4b OWS / ADS 层 + 报表导出 + 四处口径修复（2026-09-17）

**目标**：汇总层与报送层落地，并把报表导出到 PostgreSQL 服务层。

**产出**

| 位置 | 内容 |
|---|---|
| dev `dbt/models/intermediate/` | 5 张 OWS：HQLA 汇总、抵押品汇总、现金头寸、现金流预测、融资汇总 |
| dev `dbt/models/marts/` | 3 张 ADS：Section A–K 报表、报表明细、GL 对账 |
| dev `python/lakehouse/verify_gold.py` | ADS 层核对：合并口径、明细回溯、监管上限、对账状态 |
| dev `python/exporters/export_gold_to_pg.py` | gold → PostgreSQL `ads` 层，导出后逐表回读 |
| dev `deploy/server2/spark-submit-fr2052a.sh` | 凭据改为 `docker exec -e` 透传，不再出现在命令行参数里 |

**三层核对结果**

| 层 | 结果 |
|---|---|
| bronze | 1505 行 = 样本 CSV 1505 行 |
| OWD（silver） | 19 项全过：行数、分桶合法性、折算逐行重算 |
| ADS（gold） | 10 项全过：合并口径、明细回溯、二级资产上限、流入上限、GL 对账 8 项 PASS |
| PostgreSQL | 报表 5 行、明细 594 行、对账 8 行，回读行数与 gold 一致 |

合并口径实证：存款 1,105,015,544.94 = 各实体 315.7 + 303.5 + 370.6 + 115.2 百万之和。

**坑（四处，共同点是「跑通不等于数字对」）**

1. **明细与报表口径不一致。** Section B 的明细把正回购与逆回购混在一起，报表只算正回购，
   两边差 25 亿；Section F 的报表按「30 天内到期」过滤，明细没过滤，明细 13 亿对报表 0。
   修法：明细按 `line_item` 拆开，并把 30 天过滤下沉到明细，口径与报表对齐。
2. **报表没应用 HQLA 二级资产 40% 上限。** 原样报出二级资产占比 83.66%，等于没做监管计算。
   补 `sec_g_hqla_capped_total_usd` = 一级全额 + 二级按 40% 截断。
   同时纠正了我自己的核对脚本：VDQ-017 在需求里是 WARNING 级，"占比高"不是错，
   "认列额没按上限截断"才是错 —— 核对脚本原本比规则本身还严。
3. **GL 对账按单科目比 Section 合计，且按实体匹配集团口径。** 两个错叠在一起导致 8 项全 FAIL。
   修法：按 Section 汇总总账后再比（一个 Section 由多个科目构成），并去掉实体匹配条件。
4. **最根本的一处：总账原本是独立随机数，与业务明细无关，对账不可能成立。**
   这是 E4.1 埋下的缺陷 —— 当时只保证总账自身借贷平衡，没让它反映业务。
   改为由业务明细倒推：资产科目取自各业务表金额，权益作轧差项（资产 = 负债 + 权益）。
   现在证券 86.4 亿、回购 22.4 亿与报表口径逐项吻合，对账 8 项全 PASS，
   `--gl-break-amount` 也从"无意义的缺口"变成"受控的对账缺口"。
5. **贷款到期区间从 30 天起**，导致 30 天内无一笔到期、Section F 恒为 0。改为 1 天起。
6. **生成器 bug**：求负债合计时把尚未算出的权益轧差项也算了进去，直接 `KeyError`。

**遗留**

- ADS 层的审计与治理表（`ads_fr2052a_validation_log`、`ads_fr2052a_submission`、
  `ads_restatement_log`）属 E6/E7 范围，尚未建。
- `ads_fr2052a_report` 的 Section A 与 Section D 全为 NULL：演示数据里没有商业票据、
  联邦基金、其他融资业务。置 NULL 而非 0，表示"没有数据来源"而不是"确实为零"。

---

## E5.1 数据质量引擎（2026-09-17）

**目标**：把 `ref` 层声明的 20 条校验规则真正跑起来，结果落审计表。

**产出**

| 位置 | 内容 |
|---|---|
| dev `python/validators/run_dq_rules.py` | 从 `ref.ref_validation_rules` 读规则并逐条执行 |
| dev `sql/` 无新增 | 规则是数据不是代码，不需要改 DDL |
| PG `ads.ads_fr2052a_validation_log` | 审计表，按批次追加，保留历史 |

**设计取舍：为什么不用 Great Expectations**

需求文档 `[99] §4.7` 建议用 GX。这里没用，理由是**规则的唯一来源**已经落在 `ref.ref_validation_rules`
（20 条规则连同 SQL 表达式都在表里），再把它翻译成 GX 的 expectation suite 就等于同一个规则维护两份，
必然漂移。引擎直接读表执行，规则增删改只动数据一处。

代价要说清楚：本引擎只处理**单表断言**型规则。需要跨表比对、join、或依赖上一期的规则，
引擎标记为 SKIPPED 并写明覆盖位置（`verify_bronze/silver/gold` 三个核对脚本）。
即"能一条 SQL 判定的走引擎，需要比对的走核对脚本"，两类合起来覆盖全部 20 条。

**验证证据**

| 判据 | 实测 |
|---|---|
| 规则加载 | 从 `ref` 读到 20 条启用规则 |
| 执行结果 | PASS 12，FAIL 1（WARNING 级），SKIPPED 7 |
| 引擎判定 | 只有 ERROR 级违规才让批次失败；本次退出码 0 |
| 审计落库 | `ads.ads_fr2052a_validation_log` 已追加，跨批次累积 |

**修掉的两处规则与实现脱节（这类问题会让"校验"变成空转）**

1. **规则表达式用的是需求文档的列名，不是模型的列名。** 例如 `LENGTH(currency) = 3`、
   `outstanding_amount <= facility_amount`，而 silver 层的实际列名是 `currency_code`、
   `outstanding_usd <= facility_amount_usd`。引擎遇到不存在的列会跳过该表 —— 表面"跑了 20 条规则"，
   实际有 3 条从未生效。修法：把表达式对齐到实际列名，并把这条经验写进 `ref_data.py` 的注释。
2. **VDQ-001 的判据 `row_count > 0` 不是列断言**，是"表里有数据"这个结构性判断，
   当成列表达式解析必然报列不存在。改为在引擎里单独处理。

**一处演示前提造成的假阳性（已消除）**

`VDQ-016` T+1 08:00 时效规则原本 7 张 bronze 表**全行违规**：入湖时间用的是真实时钟，
而数据的报告日是虚拟的 2026-09-16，于是"入湖时间"落在报告日之后，必然超期。
这不是数据缺陷，是演示前提造成的假阳性 —— 与 E4.1 里"事件时间由报告日推算而非取当前时钟"
是同一个决定，消费者侧漏改了。改为按数据自身时间线打标（报告日 T+1 凌晨 2 点）。

**一处真实的数据结构问题（已修）**

`VDQ-017` 二级资产占比原本 83.66%：证券类型等概率抽取，国债只占六分之一，
而真实银行的 HQLA 以一级资产为主。改为按权重生成（国债 60%、机构债 15%、MBS 10%、公司债 8%、
股票 4%、ABS 3%），实测 200 笔证券中国债 118 笔。

**当前规则的遗留发现**

合并口径二级资产占比 35.53%（合规），但两个子公司本级超标：ENT004 46.95%、ENT005 49.45%。
这是 WARNING 级规则应有的行为 —— 提示不阻断，留给风险条线处理。

---

## E5.2 Airflow 编排（2026-09-17）

**目标**：按需求文档把 Airflow 部署到 Server 1，并用它驱动整条报送链路。

**产出**

| 位置 | 内容 |
|---|---|
| dev `deploy/server1/docker-compose.yml` | 新增 `x-airflow-common` 共享块 + 初始化/界面/调度三个服务 |
| dev `deploy/server1/prepare-airflow.sh` | 建元数据库、DAG/日志目录、SSH 私钥（改容器用户属主） |
| dev `deploy/server1/init-airflow.sh` | 配置连接与变量（幂等） |
| dev `deploy/server1/airflow/dags/fr2052a_daily_batch.py` | 主链路 10 个任务 |
| dev `deploy/server1/airflow/dags/fr2052a_gl_reconciliation.py` | GL 对账，不平即阻断 |
| dev `deploy/server1/sql/init/02_airflow_db.sql` | Airflow 元数据库 |
| dev `deploy/server2/run-daily-pipeline.sh` | 改成**步骤分发器**：既可整跑，也可按环节调用 |

**关键设计：DAG 不复制命令**

DAG 的每个任务只是"SSH 到 Server 2 执行 `run-daily-pipeline.sh` 的某个环节"。
编排逻辑只有那一份脚本 —— 若在 DAG 里再写一遍命令，改一处忘一处必然漂移。
环节的退出码直接决定任务成败，核对脚本失败会让批次红并阻断下游。

**验证证据**

| 判据 | 实测 |
|---|---|
| 容器 | init 退出码 0、界面 Up(healthy)、调度器 Up |
| Web UI | `http://192.168.17.22:8080/health` 从 dev 返回 200，元数据库与调度器均 healthy |
| 连接与变量 | `postgres_default`、`ssh_default` 建立；4 个变量就位 |
| DAG 解析 | 两个 DAG 均被调度器识别 |
| **日批实跑** | **10 个任务全部 success，dag_run 状态 success** |
| 单步耗时 | 每步约 2 分钟（spark-submit 冷启动为主），整条约 22 分钟 |

任务级耗时（实测）：`check_source_arrival` 瞬间 → `load_ref` 2m08s → `replay_ods` 2m08s →
`load_bronze` 2m09s → `dbt_run` 2m15s → `dq_validate` 2m14s → `export_pg` 2m18s →
`verify_bronze` 2m13s → `verify_silver` 2m09s → `verify_ads` 2m09s。

**踩到的坑**

1. **compose 锚点挂在服务上会连入口一起继承。** 我把 `&airflow-common` 挂在 `airflow-init` 上，
   而该服务覆盖了 `entrypoint: /bin/bash`，于是 webserver 与 scheduler 继承后把 `webserver`
   当 shell 命令执行，容器反复以退出码 127 重启。
   修法：共享配置放到顶层 `x-airflow-common` 扩展字段，只让初始化容器覆盖入口。
2. **SSHOperator 的默认命令超时只有 10 秒。** 单个环节要跑两分钟的 spark-submit，
   用默认值必失败，报错是 `SSH command timed out` —— 与"作业跑得慢"无关，是操作符的默认值太短。
   修法：统一显式设置 `cmd_timeout=3600`，并把这条写进 DAG 的模块注释。
3. **`airflow connections list` 会把口令明文打印出来。** 我的初始化脚本原本用它输出连接清单，
   等于把数据库口令写进日志。改为从元数据库只取 `conn_id` 与 `conn_type`。
4. **密钥目录改了属主后宿主用户写不进去。** `known_hosts` 原本直接重定向写入，
   而该目录属主已改为容器用户（uid 50000），报 Permission denied。改用 `sudo tee`。

**遗留**

- `fr2052a_realtime_alert`、`fr2052a_backfill_and_restate`、`fr2052a_submission` 三个 DAG
  属 E6 范围（实时告警、重述、报送），尚未建 —— 不做空壳任务占位。
- 两个 DAG 当前处于**暂停**状态：调试期间多触发的手动运行因 `max_active_runs=1` 排队，
  暂停以止住重复占用算力（跑批幂等，重复运行不会写坏数据）。E6 需要时再放开。

---

## 后续步骤

E4 业务开发（数据生成器 → ODS → OWD → OWS → ADS）→ E5 编排 → E6 合规演示剧本 → E7 治理收口。
