-- Airflow 元数据库。
--
-- 为什么不另起一个 PostgreSQL 容器：本演示的 Airflow 用 LocalExecutor，
-- 元数据量很小，复用 Server 1 上已有的 PostgreSQL 实例即可，少一个容器少一份运维面。
-- 数据库独立命名（airflow），与业务库 fr2052a_db 分开，互不干扰。
--
-- 注意：init 目录下的脚本只在 PostgreSQL 首次初始化时执行。
-- 已有实例上需要手工执行一次：
--   docker exec fr2052a_postgres psql -U fr2052a -d fr2052a_db -c "CREATE DATABASE airflow OWNER fr2052a"

CREATE DATABASE airflow OWNER fr2052a;
