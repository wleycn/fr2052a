-- 创建数据湖的三个命名空间。
-- 先跑这个再跑建表脚本：命名空间不落 JDBC catalog 的话，
-- SHOW NAMESPACES / SHOW TABLES 这类目录自省会失效。
--
-- 运行：python python/lakehouse/run_sql_file.py sql/iceberg/00_create_namespaces.sql
-- 幂等：全部 IF NOT EXISTS，可重复执行。

CREATE NAMESPACE IF NOT EXISTS ref;
CREATE NAMESPACE IF NOT EXISTS bronze;
CREATE NAMESPACE IF NOT EXISTS silver;
