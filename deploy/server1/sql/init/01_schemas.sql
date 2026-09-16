-- FR 2052a — PostgreSQL 初始化脚本
--
-- 存储分工：
--   PostgreSQL 只承载 ADS 报表服务层与审计层。
--   ODS / OWD / OWS 与引用数据落在 Iceberg 数据湖（MinIO）。
--
-- 本脚本由容器的 docker-entrypoint-initdb.d 在数据卷首次初始化时执行。

CREATE SCHEMA IF NOT EXISTS ads;
CREATE SCHEMA IF NOT EXISTS audit;

COMMENT ON SCHEMA ads   IS 'FR 2052a 报表服务层：报表、明细、校验日志、报送状态、GL 对账、重述日志';
COMMENT ON SCHEMA audit IS '审计与治理层：变更日志、访问日志、数据血缘';
