-- 08: OWD 版本历史表补 is_intracompany 列（一次性迁移 · 仅用于升级已有环境）
--
-- 背景：C3-2a 给 6 张 OWD 明细模型加了 is_intracompany（该行交易对手是否属于集团内部）。
-- dbt 重跑会重建明细表，但 silver.owd_*_history 是 SCD2 作业按增量维护的表，结构不会自己跟上：
-- 不加这一列，owd_scd2.py 的 incoming 与 active 两边列集合不一致，作业会失败
-- （实测报 AnalysisException: UNRESOLVED_COLUMN）。owd_scd2.py 已加列集合前置检查，
-- 缺列时直接给出指向本文件的错误信息，不再抛难懂的 Spark 异常。
--
-- 什么时候需要跑：
--   **仅当历史表已经存在**（升级已有环境）才需要。全新部署不需要 —— 历史表由 owd_scd2.py
--   按当前明细结构新建（CREATE TABLE IF NOT EXISTS ... AS SELECT t.* ... WHERE 1=0），
--   列天然齐备。因此本文件也不该出现在全新部署的脚本序列里。
--   已有的 7 张历史表见 06_rebuild_owd_history.sql 的清单；只有明细模型新增了列的表才在这里。
--
-- 为什么必须是显式迁移：本仓红线「schema evolution 只走迁移，禁止隐式加列」。
-- 为什么不写成幂等：Spark/Iceberg 的 ALTER TABLE 不支持 ADD COLUMN IF NOT EXISTS
-- （实测 ParseException: Syntax error at or near 'EXISTS'）。本文件与 05/06/07 同属一次性迁移，
-- **只执行一次**；重复执行会报「列已存在」，那不是数据问题，不需要处理。
-- 部分执行的情况：执行器遇错即停，前面成功的语句已经生效；重跑时把已生效的语句去掉即可。
--
-- 执行（Server 2）：
--   bash spark-submit-fr2052a.sh /opt/fr2052a-app/python/lakehouse/run_sql_file.py \
--        /opt/fr2052a-app/sql/iceberg/08_owd_history_add_intracompany.sql
--
-- 应用顺序：晚于 06_rebuild_owd_history.sql（重建基线）、早于 owd-scd2 环节。

ALTER TABLE silver.owd_deposits_history ADD COLUMN is_intracompany BOOLEAN;
ALTER TABLE silver.owd_secured_financing_history ADD COLUMN is_intracompany BOOLEAN;
ALTER TABLE silver.owd_loans_history ADD COLUMN is_intracompany BOOLEAN;
ALTER TABLE silver.owd_securities_history ADD COLUMN is_intracompany BOOLEAN;
ALTER TABLE silver.owd_derivatives_history ADD COLUMN is_intracompany BOOLEAN;
ALTER TABLE silver.owd_off_bs_history ADD COLUMN is_intracompany BOOLEAN;
