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

## 后续步骤

E3 端到端穿透（Iceberg + MinIO + Spark + dbt）→ E4 业务开发 → E5 编排 → E6 合规演示剧本 → E7 治理收口。
