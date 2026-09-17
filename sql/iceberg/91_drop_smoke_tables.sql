-- 清理 E3 穿透自检留下的两张表。
--
-- E3 用它们验证「Spark → Iceberg → MinIO」链路可通，正式模型落地后它们只是噪声：
--   1. 会出现在 SHOW TABLES 里，干扰对目录的判读
--   2. 会让「bronze/silver 里有多少张业务表」这个问题永远答不对
--
-- 这是一次性清理脚本，正式流程不调用它。
-- 运行：python python/lakehouse/run_sql_file.py sql/iceberg/91_drop_smoke_tables.sql

DROP TABLE IF EXISTS bronze.smoke_check;
DROP TABLE IF EXISTS silver.spark_smoke;
