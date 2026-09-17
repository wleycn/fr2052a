# CHANGELOG.md — 变更记录

> 按里程碑记录项目演进历史。每个坑的详细成因与修法见 [KNOWN-ISSUE.md](KNOWN-ISSUE.md)，完整过程见 [docs/build-log.md](../build-log.md)。

## 未发布

- 一键初始化脚本：`deploy/reset-demo.sh`（默认演练，`--apply` 才真清）与 `sql/admin/reset_demo.sql`
- 文档按实现校正：技术栈版本、主题契约、分层表清单、枚举取值、验收标准

## [E6-E7] 合规剧本与治理收口（2026-09-17）

### 已完成

- ✅ E6 合规剧本：熔断判定 → 报送放行闸 → 报送文件与哈希核对 → 实时敞口扫描 → 版本历史与重述 → 时间旅行
- ✅ E7 治理收口：血缘与监管映射 / PII 脱敏 / 四角色权限 / 健康巡检
- ✅ 报表主键改为四段区位码「机构-报表-报告期-口径」，替代需求里的自增序列
- ✅ 报送改为按报送主体出文件，文件名即报表主键

### 关键修复

以下每一条都是「不报错但结果错」的类型，成因与修法在构建日志的 E6 小节与 KNOWN-ISSUE 里：

- 对账结果按处理日打标，而熔断判定按报告日查询，两边永远对不上 → 对账失败也报不出预警
- 造异常的口子把缺口落在权益上，而权益不参与任何 Section 对账 → 判据照旧全 PASS
- GL 与数据质量类预警的报告日是字符串，投 Kafka 时序列化崩溃
- 数据质量结果表重跑翻倍（同一批次 160 行 vs 应有的 20 行）
- SCD2 失效日可能早于生效日（`#scd2-reversed-interval`）
- 校验脚本写了却没进日批执行序列，反向了整整一轮仍报通过
- Spark worker 缺检查点挂载，流式作业执行器侧建目录失败
- 实时扫描的金额字段声明成 DOUBLE，而消息里是字符串 → 判定全不命中，空转一整轮
- 流上的去重算子把重放数据全部吞掉
- bronze 入湖撞上批内重复主键（主题重放是常态）
- 实时汇总任务跨机器 exec 容器，落在 Server 2 上却去操作 Server 1 的容器

## [E0-E5] 基础架构与业务开发（2026-09-16）

### 已完成

- ✅ E0 环境侦察：三服务器 SSH 互通、Docker 已装、端口无冲突
- ✅ E1 Server 1 存储底座：PostgreSQL 18.6 + MinIO + Airflow
- ✅ E2 Server 2 计算底座：Kafka KRaft 4.3.1 + Spark 3.5.9
- ✅ E3 打通穿透：Iceberg 建表 + Spark 读写 MinIO + dbt 双 target
- ✅ E4 业务开发：生成器 → ODS → OWD → OWS → ADS，日报表数值可人工核对
- ✅ E5 编排接管：Airflow DAG + 规则引擎，DAG 端到端跑通

### 关键修复

- 修复 PG 18 数据目录变更（`#pg18-data-dir-change`）
- 修复 Spark 连接 MinIO 端点（`#spark-minio-endpoint`）
- 修复 GL 对账口径不一致（`#gl-reconciliation-mismatch`）
- 修复 HQLA 二级资产 40% 上限未应用（`#hqla-cap-not-applied`）
- 修复明细与报表口径不一致（`#detail-report-mismatch`）
- 修复 Python 3.14 不兼容问题（`#python314-incompatible`）
- 修复 Docker Hub 镜像下架问题（`#dockerhub-image-removed`）
