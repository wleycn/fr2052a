-- 一次性迁移：对齐 silver.owd_secured_financing_history 的列名。
-- 运行：python python/lakehouse/run_sql_file.py sql/iceberg/oneoff/05_align_owd_history.sql
--
-- 背景：
--   1. 该表的业务列原本叫 start_date / end_date，与 SCD2 版本列 end_date 撞名。
--      撞名的后果是版本化作业建不出历史表（同一张表不允许两个 end_date），
--      所以业务列改名为 deal_start_date / deal_end_date。
--   2. 版本列同时从 valid_from_date / valid_to_date / is_current_flag
--      改为 begin_date / end_date / is_active（当前有效版本的 end_date 为空）。
--
-- 顺序不可颠倒：必须先把业务列挪开，才能把版本列改成 end_date。
-- 本文件是历史迁移，**只应执行一次**；重复执行会报列不存在，属预期行为。

ALTER TABLE silver.owd_secured_financing_history RENAME COLUMN start_date TO deal_start_date;
ALTER TABLE silver.owd_secured_financing_history RENAME COLUMN end_date TO deal_end_date;

ALTER TABLE silver.owd_secured_financing_history RENAME COLUMN valid_from_date TO begin_date;
ALTER TABLE silver.owd_secured_financing_history RENAME COLUMN valid_to_date TO end_date;
ALTER TABLE silver.owd_secured_financing_history RENAME COLUMN is_current_flag TO is_active;

-- 当前有效版本的失效日从哨兵值改为空值
UPDATE silver.owd_secured_financing_history
SET end_date = NULL
WHERE is_active AND end_date IS NOT NULL;
