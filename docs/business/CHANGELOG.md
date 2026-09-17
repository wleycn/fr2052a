# CHANGELOG.md — 变更记录

> 按里程碑记录项目演进历史。详细构建日志见 [docs/build-log.md](../build-log.md)。

## 未发布

（暂无）

## [E0-E5] 基础架构与业务开发（2026-09-16）

### 已完成

- ✅ E0 环境侦察：三服务器 SSH 互通、Docker 已装、端口无冲突
- ✅ E1 Server 1 存储底座：PostgreSQL 18.6 + MinIO + Airflow + DataHub
- ✅ E2 Server 2 计算底座：Kafka KRaft 4.3.1 + Spark 3.5.9
- ✅ E3 打通穿透：Iceberg 建表 + Spark 读写 MinIO + dbt 双 target
- ✅ E4 业务开发：生成器 → ODS → OWD → OWS → ADS，日报表数值可人工核对
- ✅ E5 编排接管：Airflow DAG + Great Expectations，DAG 端到端跑通

### 关键修复

- 修复 PG 18 数据目录变更（`#pg18-data-dir-change`）
- 修复 Spark 连接 MinIO 端点（`#spark-minio-endpoint`）
- 修复 GL 对账口径不一致（`#gl-reconciliation-mismatch`）
- 修复 HQLA 二级资产 40% 上限未应用（`#hqla-cap-not-applied`）
- 修复明细与报表口径不一致（`#detail-report-mismatch`）
- 修复 Python 3.14 不兼容问题（`#python314-incompatible`）
- 修复 Docker Hub 镜像下架问题（`#dockerhub-image-removed`）

## [待启动] E6-E7 合规与治理

- ⏳ E6 合规演示：熔断 / 重述 / Time Travel
- ⏳ E7 治理收口：DataHub / 监控 / 脱敏

---

> 详细变更记录请查看 [docs/changes/build-log.md](../changes/build-log.md)。
