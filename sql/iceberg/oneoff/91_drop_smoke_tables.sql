-- 清理 E3 穿透自检留下的两张表：bronze.smoke_check 与 silver.spark_smoke。
--
-- ⚠ 本脚本对当前环境是**空操作**，实际清理走的是 PG 侧的 DELETE，原因如下：
--   E3 建这两张表时，Iceberg JDBC catalog 的名字是 spark_catalog；E4 起改用独立命名的
--   lakehouse catalog（见 deploy/server2/spark/spark-defaults.conf 顶部的注释）。
--   两张表只在 spark_catalog 名下有注册行，在 lakehouse 名下没有，
--   所以 `DROP TABLE IF EXISTS bronze.smoke_check` 解析到 lakehouse 后什么都不删，
--   PG 的 iceberg_catalog.iceberg_tables 里那两行会一直留着。
--   而 Spark 侧已经无法再冒充 spark_catalog 那个名字，DROP 走不通。
--
-- 实际执行过的清理（PG / Server 1）：
--     DELETE FROM iceberg_catalog.iceberg_tables
--      WHERE catalog_name = 'spark_catalog'
--        AND table_name IN ('smoke_check', 'spark_smoke');
--
-- 两件事不在上面这段里：
--   1. MinIO 上 warehouse/bronze/smoke_check/ 与 warehouse/silver/spark_smoke/ 的残留文件
--      —— 已用 mc rm --recursive 清掉（Iceberg 的 DROP TABLE 本来会一并清数据文件，DELETE 注册行不会）
--   2. spark_catalog 名下 9 张 ref 表的同类残留注册行，指向这些表最早一版的 metadata
--      —— 已在 09-19 第二批用同一条 PG DELETE 清掉；删后 iceberg_tables 只剩 lakehouse 的 42 行，
--      9 张表在 lakehouse 下照常可见、数据未动
--
-- 下面两条 Spark DDL 保留，用于「catalog 名恢复成 spark_catalog」或在新环境里清理同名表。

DROP TABLE IF EXISTS bronze.smoke_check;
DROP TABLE IF EXISTS silver.spark_smoke;
