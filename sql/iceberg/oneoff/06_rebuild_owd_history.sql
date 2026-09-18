-- 一次性重建：删除 OWD 版本历史表，由 owd_scd2.py 重新建立干净的版本基线。
-- （2026-09-18 补入 owd_treasury_cash_position_history：新增第 8 张 OWD 表时同步，
--   否则重置后的基线里会残留这张表的旧版本行。）
--
-- 运行（在 Server 2 的 ~/fr2052a-infra 下，且**先**确认没有别处依赖这些表）：
--   bash spark-submit-fr2052a.sh /opt/fr2052a-app/python/lakehouse/run_sql_file.py \
--        /opt/fr2052a-app/sql/iceberg/oneoff/06_rebuild_owd_history.sql
--
-- 为什么需要重建（不是修复）：
--   owd_scd2.py 早期版本有一个惰性求值缺陷 —— 判断「哪些键是新增/变更/删除」用的是
--   Spark 临时视图，而视图是惰性的：先执行失效 MERGE、再执行插入时，插入语句会重新
--   读一遍已经被改过的历史表，于是所有键都被当成新键。后果是每个业务键被写成
--   两条 record_version = 1（一条失效、一条有效），且变更原因一律记成 ORIGINAL。
--
--   数据没有丢，行数也能对上，但**版本号与变更原因是错的**，而变更原因正是审计的
--   全部意义。逐行修复要区分「哪条是原始版本、哪条是修正版本」，等价于用错误的
--   信息去推断正确的信息，不如重建。
--
-- 为什么可以重建：
--   Iceberg 每个快照都完整保留了重建前的表状态，可随时用
--   `SELECT * FROM silver.owd_deposits_history VERSION AS OF <snapshot_id>` 取回
--   （见 python/audit/time_travel.py --list-snapshots）。重建只影响当前表状态，
--   不影响快照历史。
--
-- 顺序不可颠倒：必须先删，再跑 owd_scd2，否则新逻辑会以错的基线继续累加版本。
-- 本文件是历史修复，**只应执行一次**。

DROP TABLE IF EXISTS silver.owd_deposits_history;
DROP TABLE IF EXISTS silver.owd_secured_financing_history;
DROP TABLE IF EXISTS silver.owd_loans_history;
DROP TABLE IF EXISTS silver.owd_securities_history;
DROP TABLE IF EXISTS silver.owd_derivatives_history;
DROP TABLE IF EXISTS silver.owd_off_bs_history;
DROP TABLE IF EXISTS silver.owd_gl_entries_history;
DROP TABLE IF EXISTS silver.owd_treasury_cash_position_history;
