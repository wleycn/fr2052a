-- 一次性迁移：silver.owd_derivatives_history 补 mtm_currency 与 mtm_exchange_rate 两列。
--
-- 背景：OWD 衍生品模型改为「盯市价值按 mtm_currency 折算」，并把币种与所用汇率输出成列，
-- 口径可核。历史表由 owd_scd2.py 归并写入，列集合必须与模型一致，缺列会直接报错。
--
-- Spark / Iceberg 不支持 ADD COLUMN IF NOT EXISTS，本文件只跑一次；
-- 重复执行会因为列已存在而失败，这是有意的（看清报错再决定要不要跑）。
-- 用法见 sql/iceberg/08_owd_history_add_intracompany.sql 的说明。

ALTER TABLE silver.owd_derivatives_history ADD COLUMN mtm_currency STRING;
ALTER TABLE silver.owd_derivatives_history ADD COLUMN mtm_exchange_rate DECIMAL(18, 8);
