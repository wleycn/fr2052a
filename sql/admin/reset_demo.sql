-- 演示环境清理：把派生数据恢复到「从未跑过」的状态。
--
-- 为什么放在 sql/admin/ 而不是 sql/postgres/：
--   deploy/server1/apply-sql.sh 会把 sql/postgres/ 下的每个 .sql 都应用一遍，
--   而它被 publish-access 环节在每次跑批时调用 —— 那个文件一旦放进那个目录，
--   每次跑批都会顺手把演示数据清空一次。放这里是为了让「会被自动应用」与
--   「只能手动执行」两类脚本在目录上就分得开，不必靠人记住。
--
-- 调用方式：bash deploy/reset-demo.sh --apply（由脚本投喂，不要手工拼命令）
--
-- 清谁、为什么清：
--   报表服务层（report / detail / gl_reconciliation）
--       每轮由导出作业整表覆盖，清掉是为了能从零观察一轮导出究竟写了什么。
--   控制与审计（指标 / 预警 / 熔断 / 报送 / 重述 / 版本历史 / 实时事件 / 校验日志）
--       这些表跨轮次累积：不清就会把上一轮的预警、批次、版本带进下一轮。
--   脱敏对照表与数据血缘
--       与当前样本数据一一对应，样本数据重生成后它们必须一起清，否则对不上。
--
-- 幂等：TRUNCATE 本身可反复执行；表不存在时会报错 —— 这是有意的，
--       说明环境还没初始化过，应当先跑一次完整链路再来清。

-- ---------------------------------------------------------------------------
-- 1. 报表服务层
-- ---------------------------------------------------------------------------
TRUNCATE TABLE ads.ads_fr2052a_report;
TRUNCATE TABLE ads.ads_fr2052a_detail;
TRUNCATE TABLE ads.ads_gl_reconciliation;

-- ---------------------------------------------------------------------------
-- 2. 控制与审计层
-- ---------------------------------------------------------------------------
TRUNCATE TABLE ads.ads_liquidity_metrics;
TRUNCATE TABLE ads.ads_fr2052a_alerts;
TRUNCATE TABLE ads.ads_fr2052a_realtime_alerts;
TRUNCATE TABLE ads.ads_fr2052a_submission;
TRUNCATE TABLE ads.ads_fr2052a_submission_audit;
TRUNCATE TABLE ads.ads_restatement_log;
-- 版本历史表（ads_fr2052a_report_history）不在复位清单里。
-- 该表在文档与 schema 里被声明为「历史不可变」（SCD2，重述前后版本成对留痕）。
-- 历史不可变是硬性质：一旦允许复位清空，就等于承认「历史可以抹掉」，
-- 重述登记里引用的原报表版本将变成悬空引用，审计链断裂。
-- 如确需清空（例如重建演示环境），走单独的人工步骤并在 KNOWN-ISSUE.md
-- 登记为有意的例外，写清例外范围与代价。
TRUNCATE TABLE ads.ads_fr2052a_validation_log;
TRUNCATE TABLE ads.ads_pipeline_run_context;

-- 熔断闸不是清空而是复位：这张表任何时刻都该有且只有一行全局状态。
UPDATE ads.ads_circuit_breaker
SET state = 'OPEN',
    reason = '演示环境初始化：熔断闸复位',
    triggered_by_alert_code = NULL,
    triggered_at = NULL,
    cleared_at = CURRENT_TIMESTAMP,
    trip_count = 0,
    updated_at = CURRENT_TIMESTAMP
WHERE scope = 'GLOBAL';

-- ---------------------------------------------------------------------------
-- 3. 脱敏对照表与数据血缘
-- ---------------------------------------------------------------------------
TRUNCATE TABLE secure.fr2052a_pii_map;
TRUNCATE TABLE audit.audit_data_lineage;

-- 操作审计（改了什么、谁读了什么）也一并清：它记录的是演示过程，不是业务数据。
TRUNCATE TABLE audit.audit_change_log;
TRUNCATE TABLE audit.audit_access_log;

-- ---------------------------------------------------------------------------
-- 4. 结构残留
--    这两张是早期联调留下的探针表，不在 sql/postgres/*.sql 的声明结构里，
--    也不参与任何环节。属于「不该存在的表」，一并清掉，
--    免得下次有人在里面查数据、以为它是项目表结构的一部分。
-- ---------------------------------------------------------------------------
-- pg_smoke 是视图、scratch_probe 是表 —— 两者类型不同（实测确认），
-- 删除语句也不同；写错会报「is not a table」并因 ON_ERROR_STOP 中断清理。
DROP VIEW IF EXISTS ads.pg_smoke;
DROP TABLE IF EXISTS ads.scratch_probe;

-- ---------------------------------------------------------------------------
-- 5. 清理结果自证：把剩下的表列出来，供人核对（只读，不改数据）
-- ---------------------------------------------------------------------------
\echo 清理后 ads / audit / secure 下的表：
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('ads', 'audit', 'secure')
ORDER BY table_schema, table_name;
