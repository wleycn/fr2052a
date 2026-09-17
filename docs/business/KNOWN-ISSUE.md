# 已知坑与决策

> **纪律**：
> 1. 锚点名用 `#kebab-case`，**一经写入不改拼写**（PROJECT 索引行逐字引用）
> 2. 每个锚点必须在 PROJECT.md 已知坑区有索引行（防孤儿锚点）
> 3. 决策型条目（废弃方案/改设计）也记录，标 ✅ 决策，避免重复讨论
> 4. 修复后回填"修复"列；待定标 ⏳ 待决策

| 锚点 | 日期 | 现象 | 根因 | 修复 | 影响范围 | 关联文档 |
|------|------|------|------|------|----------|----------|
| #pg18-data-dir-change | 09-16 | PG 18 启动失败，日志报 `there appears to be PostgreSQL data in /var/lib/postgresql/data (unused mount/volume)` | `postgres:18-alpine` 的 `PGDATA` 变为 `/var/lib/postgresql/18/docker`，镜像的 `VOLUME` 声明在 `/var/lib/postgresql`。按 17 及以前习惯把卷挂在 `/var/lib/postgresql/data` 会被判定为废弃挂载并拒绝启动 | ✅ 修法：卷改挂 `/var/lib/postgresql`。重建数据卷时必须沿用此路径 | Server 1 部署 | BUILD-LOG E1 |
| #spark-minio-endpoint | 09-16 | Spark 连接 MinIO 失败，报 connection refused | Spark 中必须用 `192.168.17.22:9000`，不能用 `localhost`（跨容器网络）| ✅ 修正 Spark catalog config 中的 endpoint | Server 2 Spark 配置 | BUILD-LOG E3 |
| #gl-reconciliation-mismatch | 09-16 | GL 对账 8 项全 FAIL | ① 按单科目比 Section 合计（应汇总后再比）② 按实体匹配集团口径（应去掉实体匹配）③ 总账原本独立随机数，与业务明细无关 | ✅ 修法：按 Section 汇总总账后再比；总账由业务明细倒推（资产科目取自各业务表，权益作轧差项） | GL 对账模块 | BUILD-LOG E4.1 |
| #hqla-cap-not-applied | 09-16 | 报表未应用 HQLA 二级资产 40% 上限，二级资产占比 83.66% | 原样报出，未做监管计算截断 | ✅ 补 `sec_g_hqla_capped_total_usd` = 一级全额 + 二级按 40% 截断 | ADS 报表计算 | BUILD-LOG E4.1 |
| #python314-incompatible | 09-16 | Python 3.14.4 装不上 Great Expectations 与 pyspark | GE 要求 `>=3.10,<3.14`；pyspark 3.5.0 不支持 3.14 | ✅ 在服务器上用 uv 装独立 Python 3.12 venv | Server 2 环境 | BUILD-LOG E0 |
| #dockerhub-image-removed | 09-16 | `minio/minio` 与 `bitnami/spark` 从 Docker Hub 下架 | 镜像源失效 | ✅ MinIO 改走 `quay.io/minio/minio`；Spark 改用官方 `apache/spark` | Server 1/2 部署 | BUILD-LOG E0 |
| #detail-report-mismatch | 09-16 | Section B 明细与报表口径不一致，差 25 亿；Section F 明细 13 亿 vs 报表 0 | 明细把正回购与逆回购混在一起，报表只算正回购；明细没按 30 天过滤 | ✅ 明细按 `line_item` 拆开，30 天过滤下沉到明细 | OWD/OWS 模型 | BUILD-LOG E4.1 |

<!-- PROJECT.md 索引行（复制区）：
- `#pg18-data-dir-change` — PG 18 改了数据目录约定
- `#spark-minio-endpoint` — Spark 连接 MinIO 必须用 IP，不能用 localhost
- `#gl-reconciliation-mismatch` — GL 对账需按 Section 汇总后比对
- `#hqla-cap-not-applied` — HQLA 二级资产 40% 上限需显式截断
- `#python314-incompatible` — Python 3.14 不兼容 GE 与 pyspark
- `#dockerhub-image-removed` — minio/spark 官方镜像已从 Docker Hub 下架
- `#detail-report-mismatch` — 明细与报表口径不一致（正回购/30天过滤）
-->

## 规范偏离（本项目 vs 上游）

> 本节是偏离记录的**唯一落点**：纪律、表、适用边界、代价与回退都住这里。

标准做法是**默认值**，偏离是例外。允许偏离的前提是四条**同时**成立：

1. **逐条登记**：一行一条，四列 = 问题 / 上游要求 / 本项目做法 / 处置。
2. **处置列指向真实锚点**：写成本文件里已有的 `### #slug` 定义；只写「已说明」不合格。
3. **说明为什么不采用标准做法**：「本项目特殊」这类空理由不合格。
4. **记代价与回退成本**：在本节末尾的汇总里各写一行。

**禁止无登记地默默降标准**；「这只是个例」不是免于登记的理由。

| 问题 | 上游要求 | 本项目做法 | 处置 |
|---|---|---|---|
| 双轨制（Core/Advanced） | 单一技术栈 | Core（PG+dbt+Airflow）+ Advanced（Kafka+Iceberg+Spark）并行 | ✅ 决策：演示需要展示两种架构，见 `[01]架构设计.md` §1.5 |
| 存储分工（ODS 进 Iceberg 不进 PG） | ODS 通常进 PG | ODS/OWD/OWS 进 Iceberg，仅 ADS 进 PG | ✅ 决策：湖仓一体架构，见 `[04]环境设计.md` §4.3 存储分工 |
| requirements/ 不落九文档 | 原始需求应归档 | 保留 requirements/ 参考，待收尾阶段处理 | ⏳ 待决策：收尾时移入 references/ 或 archive/ |

**适用边界**（条件条目为什么不在表里、本项目实际取了哪条路）：

- 双轨制：演示项目需要展示 Core 稳定版 + Advanced 技术深度版，非生产约束
- 存储分工：湖仓一体是现代数据栈标准做法，PG 只做报表输出层

**代价与回退汇总**（每行偏离一条）：

- **双轨制**（✅ 决策）——代价：维护两套部署清单；回退：保留 Core 版即可，Advanced 版可下线
- **存储分工**（✅ 决策）——代价：ODS 不可直接用 PG 查询，需走 Spark/Iceberg CLI；回退：把 ODS 建到 PG 需改 deploy 脚本
