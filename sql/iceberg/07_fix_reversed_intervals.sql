-- 一次性修数据：把「失效日早于生效日」的反向区间压成零长度区间。
--
-- 为什么会有这种数据：
--   owd_scd2.py 早期版本写失效日时直接取「本次生效日 - 1」，没有和该版本自己的
--   生效日比较。两个调用方的生效日约定一旦不一致（重述用处理日 2026-09-17、
--   日批用报告日 2026-09-16），后跑的那次就会给一个生效日为 09-17 的版本
--   写上 09-15 的失效日，算出反向区间。
--
-- 为什么压成零长度而不是删掉该版本：
--   这一版确实存在过，历史上真的落过这份数据，删掉等于抹掉审计痕迹。
--   区间长度为 0 表达的是「它被写入后立刻被替换」，与事实相符。
--
-- 写入侧已同步兜底（owd_scd2.py 用 greatest(生效日 - 1, 该版本生效日)），
-- 所以本文件只用于修已经写坏的行。幂等：再跑一次影响 0 行。
--
-- 运行（Server 2）：
--   bash spark-submit-fr2052a.sh /opt/fr2052a-app/python/lakehouse/run_sql_file.py \
--        /opt/fr2052a-app/sql/iceberg/07_fix_reversed_intervals.sql

UPDATE silver.owd_deposits_history SET end_date = begin_date
WHERE end_date IS NOT NULL AND end_date < begin_date;

UPDATE silver.owd_secured_financing_history SET end_date = begin_date
WHERE end_date IS NOT NULL AND end_date < begin_date;

UPDATE silver.owd_loans_history SET end_date = begin_date
WHERE end_date IS NOT NULL AND end_date < begin_date;

UPDATE silver.owd_securities_history SET end_date = begin_date
WHERE end_date IS NOT NULL AND end_date < begin_date;

UPDATE silver.owd_derivatives_history SET end_date = begin_date
WHERE end_date IS NOT NULL AND end_date < begin_date;

UPDATE silver.owd_off_bs_history SET end_date = begin_date
WHERE end_date IS NOT NULL AND end_date < begin_date;

UPDATE silver.owd_gl_entries_history SET end_date = begin_date
WHERE end_date IS NOT NULL AND end_date < begin_date;
