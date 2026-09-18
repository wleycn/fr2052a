-- FR 2052a 控制与审计层表结构（PostgreSQL / Server 1）
-- 应用方式：bash deploy/server1/apply-sql.sh（幂等，可重复执行）
--
-- 与 sql/iceberg/ 的分工：Iceberg 承载数据湖（ref/ODS/OWD/OWS/Gold），
-- PostgreSQL 承载报送服务层与审计层。本文件只建后者。
--
-- 全部语句幂等。唯一的非 CREATE 语句是对 ads.ads_fr2052a_report 补键列 ——
-- 该表由 Spark 导出作业首次写入时创建，故此处用 DO 块判存在后再补。

-- ---------------------------------------------------------------------------
-- 1. 流动性指标：熔断判定的输入事实。一次跑批每个报告日一张快照。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads.ads_liquidity_metrics (
    report_date DATE NOT NULL,                       -- 报告日
    entity_code TEXT NOT NULL,                       -- 法人实体编码，ENT001 为集团合并口径
    is_consolidated BOOLEAN NOT NULL DEFAULT FALSE,  -- 是否合并口径
    hqla_l1_unencumbered_usd NUMERIC(20, 2),         -- 未质押一级资产市值
    hqla_l2a_unencumbered_usd NUMERIC(20, 2),        -- 未质押二级 A 类资产市值
    hqla_l2b_unencumbered_usd NUMERIC(20, 2),        -- 未质押二级 B 类资产市值
    hqla_unencumbered_capped_usd NUMERIC(20, 2),     -- 未质押 HQLA 认列额，二级资产按 40% 截断后计入
    hqla_encumbered_usd NUMERIC(20, 2),              -- 已质押资产市值，不计入 LCR 分子
    expected_inflow_30d_usd NUMERIC(20, 2),          -- 30 天预期流入（未加限制）
    expected_inflow_capped_usd NUMERIC(20, 2),       -- 30 天认列流入，上限为流出的 75%
    expected_outflow_30d_usd NUMERIC(20, 2),         -- 30 天预期流出
    net_cash_outflow_30d_usd NUMERIC(20, 2),         -- 净现金流出 = 流出 - 认列流入
    lcr_ratio NUMERIC(12, 4),                        -- 流动性覆盖率 = 认列 HQLA / 净现金流出
    l2_cap_ratio NUMERIC(12, 4),                     -- 二级资产占 HQLA 比例，监管上限 40%
    inflow_cap_ratio NUMERIC(12, 4),                 -- 流入占流出比例，超过 75% 说明认列被上限截断
    regulatory_min_ratio NUMERIC(12, 4),             -- 判定时采用的监管下限，留痕以便回溯口径
    headroom_usd NUMERIC(20, 2),                     -- 距红线余量 = 认列 HQLA - 下限 × 净现金流出
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ads_liquidity_metrics_pkey PRIMARY KEY (report_date, entity_code)
);

COMMENT ON TABLE ads.ads_liquidity_metrics IS '流动性指标快照，熔断判定的输入事实';

-- ---------------------------------------------------------------------------
-- 2. 预警明细：(报告日, 实体, 规则) 唯一，重复判定累加次数而不是堆重复行。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads.ads_fr2052a_alerts (
    alert_id BIGSERIAL PRIMARY KEY,                  -- 预警主键，交给序列生成，写入方不列此列
    report_date DATE NOT NULL,                       -- 报告日
    entity_code TEXT NOT NULL,                       -- 法人实体编码
    alert_code TEXT NOT NULL,                        -- 规则编码，如 CB-LCR-001
    severity TEXT NOT NULL,                          -- CRITICAL / WARNING / INFO
    metric_name TEXT,                                -- 触发指标名
    metric_value NUMERIC(20, 4),                     -- 触发时指标实际值
    threshold_value NUMERIC(20, 4),                  -- 判定阈值
    message TEXT NOT NULL,                           -- 人读说明
    blocks_submission BOOLEAN NOT NULL DEFAULT FALSE,-- 是否阻断报送
    occurrence_count INTEGER NOT NULL DEFAULT 1,     -- 同一规则重复触发的累计次数
    status TEXT NOT NULL DEFAULT 'OPEN',             -- OPEN / CLOSED，规则不再命中时置 CLOSED
    first_detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ads_fr2052a_alerts_uk UNIQUE (report_date, entity_code, alert_code),
    CONSTRAINT ads_fr2052a_alerts_severity_ck CHECK (severity IN ('CRITICAL', 'WARNING', 'INFO')),
    CONSTRAINT ads_fr2052a_alerts_status_ck CHECK (status IN ('OPEN', 'CLOSED'))
);

COMMENT ON TABLE ads.ads_fr2052a_alerts IS '流动性预警明细，按规则维度去重';

-- ---------------------------------------------------------------------------
-- 2b. 实时事件预警：由 Kafka 主题扫描产生，一行一个事件。
--     与上面那张表分开的理由：上面是「规则当前状态」（同一规则重复命中只累加次数），
--     这里是「事件流水」（每笔大额敞口一条）。两种语义不同，混在一张表里
--     会让「一条记录代表什么」变得说不清。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads.ads_fr2052a_realtime_alerts (
    event_id TEXT PRIMARY KEY,                       -- 事件唯一键：源记录 + 报告日 + 规则 的哈希
    report_date DATE NOT NULL,                       -- 报告日
    entity_code TEXT NOT NULL,                       -- 记账法人实体
    alert_code TEXT NOT NULL,                        -- 规则编码
    severity TEXT NOT NULL,                          -- CRITICAL / WARNING / INFO
    source_topic TEXT NOT NULL,                      -- 来源主题
    source_record_id TEXT NOT NULL,                  -- 来源记录主键
    amount_usd NUMERIC(20, 2),                       -- 敞口金额（USD）
    segment TEXT,                                    -- 客户类别，如 CORP / GOV / FI
    message TEXT NOT NULL,                           -- 人读说明
    detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE ads.ads_fr2052a_realtime_alerts IS '实时事件级预警流水，由 Kafka 扫描写入';

-- ---------------------------------------------------------------------------
-- 3. 报送熔断闸：当前状态，一个报送范围一行。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads.ads_circuit_breaker (
    scope TEXT PRIMARY KEY,                          -- 报送范围，本演示只有 GLOBAL
    state TEXT NOT NULL,                             -- OPEN = 放行 / HALTED = 熔断
    reason TEXT,                                     -- 当前状态的原因
    triggered_by_alert_code TEXT,                    -- 触发熔断的规则编码
    triggered_at TIMESTAMP,                          -- 最近一次进入熔断的时刻
    cleared_at TIMESTAMP,                            -- 最近一次解除熔断的时刻
    trip_count INTEGER NOT NULL DEFAULT 0,           -- 累计熔断次数，只在状态翻转时加一
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ads_circuit_breaker_state_ck CHECK (state IN ('OPEN', 'HALTED'))
);

COMMENT ON TABLE ads.ads_circuit_breaker IS '报送熔断闸当前状态，gate 据此放行或阻断';

-- ---------------------------------------------------------------------------
-- 4. 报表主键说明
--    report_id 是业务标识，按「机构-报表-报告期-口径」四段区位码拼装，
--    例如 ENT001-FR2052A-20260916-01，由 dbt 模型产出
--    （见 dbt/models/marts/ads_fr2052a_report.sql）。最后一段是口径码：01 = 并表，02 = 法人单体。
--
--    为什么不用自增序列：本表每轮导出是全量覆盖，序列值属于数据库状态而不是数据，
--    同一个业务报表在不同批次会拿到不同的号；而重述登记要跨批次引用「原报表 / 新报表」，
--    键一旦会变，这层对应关系就不成立。
--
--    为什么不在库侧补生成列：report_id 随 gold 表从 Iceberg 导出，库里生成则
--    Iceberg 侧没有这一列，报送台账、重述登记、血缘都拿不到这个身份。且 PostgreSQL
--    的生成列只接受 IMMUTABLE 表达式，而 date 转文本受 DateStyle 会话参数影响，
--    实测 cast / concat / to_char / format 四种写法全部被拒。
--
--    结论：报表主键的口径只写在模型里，库侧不另立一份定义。
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- 5. 报送记录：一个报送文件一行，含哈希与回执。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads.ads_fr2052a_submission (
    submission_id BIGSERIAL PRIMARY KEY,             -- 报送主键
    report_id TEXT NOT NULL,                         -- 报表主键（report_date-entity_code）
    report_date DATE NOT NULL,                       -- 报告日
    entity_code TEXT NOT NULL,                       -- 法人实体编码
    file_format TEXT NOT NULL,                       -- 文件格式：XBRL / XML / CSV
    file_path TEXT NOT NULL,                         -- 文件在报送服务端落盘路径
    file_hash TEXT NOT NULL,                         -- 当前版本的文件 SHA-256（重生成时被更新）
    file_size_bytes BIGINT,                          -- 文件字节数
    submitted_at TIMESTAMP,                          -- 提交时刻
    submission_status TEXT NOT NULL,                 -- GENERATED / SUBMITTED / ACCEPTED / REJECTED
    receipt_id TEXT,                                 -- 回执编号
    receipt_message TEXT,                            -- 回执消息
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ads_fr2052a_submission_uk
        UNIQUE (report_id, file_format)
);

COMMENT ON TABLE ads.ads_fr2052a_submission IS '报送文件当前状态台账：一个文件一行，重生成时更新哈希与回执';
COMMENT ON COLUMN ads.ads_fr2052a_submission.file_hash IS '当前版本的文件 SHA-256（重生成时被更新）';

-- ---------------------------------------------------------------------------
-- 5b. 报送重生成审计：一行一次重生成，回答「这份文件生成过几版、哈希怎么变的」。
--     台账只留当前状态，历史在这里查。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads.ads_fr2052a_submission_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    report_id TEXT NOT NULL,                         -- 报表主键，与台账同源
    file_format TEXT NOT NULL,                       -- 文件格式：XBRL / XML / CSV
    file_hash TEXT NOT NULL,                         -- 本次生成的文件 SHA-256
    previous_hash TEXT,                              -- 被替换的上一次哈希；首次生成为 NULL
    file_size_bytes BIGINT,                          -- 文件字节数
    is_content_change BOOLEAN NOT NULL,              -- 与上一条台账状态相比内容是否变化，首次生成为 true
    entry_source TEXT NOT NULL DEFAULT 'GENERATE',   -- GENERATE = 报送脚本写的，MIGRATION = 迁移回填的
    generated_at TIMESTAMP NOT NULL,                 -- 本次生成的时刻
    CONSTRAINT ads_fr2052a_submission_audit_change_ck
        CHECK (is_content_change = (previous_hash IS NULL OR previous_hash <> file_hash))
);

COMMENT ON TABLE ads.ads_fr2052a_submission_audit IS '报送文件重生成审计流水，一行一次生成';

COMMENT ON COLUMN ads.ads_fr2052a_submission_audit.previous_hash IS '被本次生成替换掉的上一次哈希；首次生成为 NULL';
COMMENT ON COLUMN ads.ads_fr2052a_submission_audit.is_content_change IS '与上一条台账状态相比内容是否变化，首次生成为 true';
COMMENT ON COLUMN ads.ads_fr2052a_submission_audit.entry_source IS 'GENERATE = 报送脚本写的，MIGRATION = 迁移回填的';

CREATE INDEX IF NOT EXISTS idx_submission_audit_key
    ON ads.ads_fr2052a_submission_audit (report_id, file_format, generated_at);

-- 台账唯一键从 (report_date, entity_code, file_format, file_hash) 改为 (report_id, file_format)。
-- 旧库里的约束名相同（ads_fr2052a_submission_uk），但列不同，需要先删旧约束再补新约束。
-- 判存在走 pg_constraint，重复执行不报错。
DO $$
BEGIN
    -- 1) 回填旧行进审计表：同一业务键下只留 submission_id 最大的一行，其余回填后删掉。
    --    表为空时整段 no-op。
    IF EXISTS (
        SELECT 1 FROM ads.ads_fr2052a_submission
        WHERE (report_id, file_format) IN (
            SELECT report_id, file_format
            FROM ads.ads_fr2052a_submission
            GROUP BY report_id, file_format
            HAVING COUNT(*) > 1
        )
    ) THEN
        INSERT INTO ads.ads_fr2052a_submission_audit
            (report_id, file_format, file_hash, previous_hash, file_size_bytes,
             is_content_change, entry_source, generated_at)
        SELECT
            s.report_id,
            s.file_format,
            s.file_hash,
            -- 被保留那一行（submission_id 最大）的哈希作为 previous_hash
            LAG(s.file_hash) OVER (
                PARTITION BY s.report_id, s.file_format
                ORDER BY s.submission_id
            ),
            s.file_size_bytes,
            -- is_content_change 按同一表达式计算
            (LAG(s.file_hash) OVER (
                PARTITION BY s.report_id, s.file_format
                ORDER BY s.submission_id
            ) IS NULL
             OR LAG(s.file_hash) OVER (
                PARTITION BY s.report_id, s.file_format
                ORDER BY s.submission_id
             ) <> s.file_hash),
            'MIGRATION',
            COALESCE(s.submitted_at, CURRENT_TIMESTAMP)
        FROM ads.ads_fr2052a_submission s
        WHERE s.submission_id NOT IN (
            SELECT MAX(submission_id)
            FROM ads.ads_fr2052a_submission
            GROUP BY report_id, file_format
        );

        -- 2) 删掉旧行，只留最新一行
        DELETE FROM ads.ads_fr2052a_submission
        WHERE submission_id NOT IN (
            SELECT MAX(submission_id)
            FROM ads.ads_fr2052a_submission
            GROUP BY report_id, file_format
        );
    END IF;

    -- 3) 旧约束还在就删掉（按 conname + conrelid 精确定位，不裸 DROP）
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ads_fr2052a_submission_uk'
          AND conrelid = 'ads.ads_fr2052a_submission'::regclass
    ) THEN
        ALTER TABLE ads.ads_fr2052a_submission
            DROP CONSTRAINT ads_fr2052a_submission_uk;
    END IF;

    -- 4) 新约束没有就补上
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ads_fr2052a_submission_uk'
          AND conrelid = 'ads.ads_fr2052a_submission'::regclass
    ) THEN
        ALTER TABLE ads.ads_fr2052a_submission
            ADD CONSTRAINT ads_fr2052a_submission_uk
            UNIQUE (report_id, file_format);
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- 5c. 运行上下文：一次跑批一行，把报告日 / 处理日 / 生效日从脚本默认值
--     变成库里的状态行。所有环节与放行闸都读它，不再各自拿一个日期。
--     日期链 CHECK 把当前口径编码进 schema：处理日 = 报告日 + 1，生效日 = 处理日。
--     口径变了就必须改约束，是一次看得见的动作而不是口头约定。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads.ads_pipeline_run_context (
    batch_id TEXT PRIMARY KEY,                         -- 批次号，一次跑批的唯一标识
    report_date DATE NOT NULL,                         -- 报告日（= 业务日期），本次跑批处理哪一天的数据
    processing_date DATE NOT NULL,                     -- 处理日，跑批实际执行的日期
    effective_date DATE NOT NULL,                      -- 生效日，数据版本从哪天开始生效
    run_type TEXT NOT NULL,                            -- DAILY = 日批 DAG 拉起；MANUAL = 人工整链跑
    status TEXT NOT NULL,                              -- RUNNING 未收口 / SUCCEEDED / FAILED
    started_at TIMESTAMP NOT NULL,                     -- 开口时刻
    finished_at TIMESTAMP,                             -- 收口时刻，未收口为 NULL
    CONSTRAINT ads_pipeline_run_context_date_ck
        CHECK (processing_date = report_date + 1 AND effective_date = processing_date),
    CONSTRAINT ads_pipeline_run_context_type_ck CHECK (run_type IN ('DAILY', 'MANUAL')),
    CONSTRAINT ads_pipeline_run_context_status_ck CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED'))
);

COMMENT ON TABLE ads.ads_pipeline_run_context IS '运行上下文：一次跑批的日期与状态，所有环节与放行闸的单源';

CREATE INDEX IF NOT EXISTS idx_run_context_report_date
    ON ads.ads_pipeline_run_context (report_date, started_at DESC);

-- ---------------------------------------------------------------------------
-- 6. 重述登记：原报表与新报表成对留痕。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads.ads_restatement_log (
    restatement_id BIGSERIAL PRIMARY KEY,            -- 重述主键
    original_report_id TEXT,                         -- 原报表主键
    new_report_id TEXT,                              -- 新报表主键
    report_date DATE NOT NULL,                       -- 报告日
    entity_code TEXT NOT NULL,                       -- 法人实体编码
    reason TEXT,                                     -- 重述原因
    requested_by TEXT,                               -- 申请人
    approved_by TEXT,                                -- 审批人
    status TEXT NOT NULL DEFAULT 'PENDING',          -- PENDING / APPROVED / APPLIED / REJECTED
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE ads.ads_restatement_log IS '重述登记，记录原报表与新报表的对应关系';

-- ---------------------------------------------------------------------------
-- 7. 报表版本历史：重述的「原报表 / 新报表」都住这里。
--    为什么快照存 JSONB 而不复制报表的 60 个 Section 列：
--    复制一份列清单等于给报表结构写第二份定义，报表加列时必然漏改一处。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads.ads_fr2052a_report_history (
    report_version_id TEXT PRIMARY KEY,              -- 版本主键：report_id || '-v' || record_version
    report_id TEXT NOT NULL,                         -- 报表身份，同一份报表的各版本共用
    record_version INTEGER NOT NULL,                 -- 版本号，从 1 起
    report_date DATE NOT NULL,                       -- 报告日
    entity_code TEXT NOT NULL,                       -- 法人实体编码
    begin_date DATE NOT NULL,                        -- 本版本生效日（处理日）
    end_date DATE,                                   -- 本版本失效日；**为空表示当前有效**
    is_active BOOLEAN NOT NULL DEFAULT TRUE,         -- end_date 的冗余列，便于建索引
    last_modified_reason TEXT NOT NULL DEFAULT 'ORIGINAL', -- ORIGINAL / CORRECTION / RESTATEMENT
    snapshot JSONB NOT NULL,                         -- 该版本报表的整行快照
    snapshot_hash TEXT,                              -- 快照整行哈希，用于比对「这一版到底变没变」
    captured_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ads_fr2052a_report_history_uk UNIQUE (report_id, record_version),
    -- is_active 是冗余列，允许冗余但不允许不一致：
    -- 两者一旦分叉，「哪一列才算数」就靠人记，早晚有人按错的那一列查数据。
    CONSTRAINT ads_fr2052a_report_history_active_ck
        CHECK ((end_date IS NULL) = is_active)
);

COMMENT ON TABLE ads.ads_fr2052a_report_history IS '报表版本历史（SCD2），重述前后的报文快照';

-- 老结构迁移（幂等：只在检测到老列时动手）。
-- 老结构用 valid_from_date / valid_to_date / is_current_flag，
-- 且当前有效版本用 9999-12-31 占位；新结构当前有效版本的 end_date 为空。
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'ads' AND table_name = 'ads_fr2052a_report_history'
          AND column_name = 'valid_from_date'
    ) THEN
        ALTER TABLE ads.ads_fr2052a_report_history RENAME COLUMN valid_from_date TO begin_date;
        ALTER TABLE ads.ads_fr2052a_report_history RENAME COLUMN valid_to_date TO end_date;
        ALTER TABLE ads.ads_fr2052a_report_history RENAME COLUMN is_current_flag TO is_active;
        ALTER TABLE ads.ads_fr2052a_report_history ALTER COLUMN end_date DROP NOT NULL;
        ALTER TABLE ads.ads_fr2052a_report_history ALTER COLUMN end_date DROP DEFAULT;
        UPDATE ads.ads_fr2052a_report_history
        SET end_date = NULL
        WHERE is_active AND end_date IS NOT NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'ads_fr2052a_report_history_active_ck'
    ) THEN
        ALTER TABLE ads.ads_fr2052a_report_history
            ADD CONSTRAINT ads_fr2052a_report_history_active_ck
            CHECK ((end_date IS NULL) = is_active);
    END IF;
END $$;

-- 版本区间不得反向：失效日为空或不得早于生效日。
-- 幂等添加：按 conname 查 pg_constraint，不存在才 ADD。
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ads_fr2052a_report_history_interval_ck'
          AND conrelid = 'ads.ads_fr2052a_report_history'::regclass
    ) THEN
        ALTER TABLE ads.ads_fr2052a_report_history
            ADD CONSTRAINT ads_fr2052a_report_history_interval_ck
            CHECK (end_date IS NULL OR end_date >= begin_date);
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- 8. 审计三表。变更留痕用触发器归档，不原地覆盖 —— 历史不可变。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.audit_change_log (
    change_id BIGSERIAL PRIMARY KEY,                 -- 变更日志主键
    table_name TEXT NOT NULL,                        -- 被变更的表
    record_key TEXT,                                 -- 被变更记录的键
    change_type TEXT NOT NULL,                       -- INSERT / UPDATE / DELETE
    old_value TEXT,                                  -- 变更前整行
    new_value TEXT,                                  -- 变更后整行
    changed_by TEXT,                                 -- 修改人
    change_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reason TEXT                                      -- 修改原因
);

CREATE TABLE IF NOT EXISTS audit.audit_access_log (
    access_id BIGSERIAL PRIMARY KEY,                 -- 访问日志主键
    user_name TEXT,                                  -- 数据库角色名
    role_name TEXT,                                  -- 角色
    object_name TEXT,                                -- 被访问对象
    action TEXT,                                     -- 操作
    access_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    client_ip TEXT                                   -- 客户端 IP
);

CREATE TABLE IF NOT EXISTS audit.audit_data_lineage (
    lineage_id BIGSERIAL PRIMARY KEY,                -- 血缘主键
    source_object TEXT NOT NULL,                     -- 源对象
    target_object TEXT NOT NULL,                     -- 目标对象
    transform_type TEXT,                             -- 转换类型
    job_name TEXT,                                   -- 作业名称
    run_id TEXT,                                     -- 运行编号
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT audit_data_lineage_uk
        UNIQUE (source_object, target_object, transform_type, job_name)
);

COMMENT ON TABLE audit.audit_data_lineage IS '技术血缘边，由 dbt manifest 渲染作业写入';

-- ---------------------------------------------------------------------------
-- 9. 索引：按「查询总是带报告日」的访问模式建，避免全表扫描。
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_alerts_report_date ON ads.ads_fr2052a_alerts (report_date, severity);
CREATE INDEX IF NOT EXISTS idx_alerts_status ON ads.ads_fr2052a_alerts (status) WHERE status = 'OPEN';
CREATE INDEX IF NOT EXISTS idx_submission_report ON ads.ads_fr2052a_submission (report_date, entity_code);
CREATE INDEX IF NOT EXISTS idx_restatement_report ON ads.ads_restatement_log (report_date, entity_code);
-- 谓词列名改了（is_current_flag → is_active），同名索引必须重建，否则谓词停在老列上。
DROP INDEX IF EXISTS ads.idx_report_history_current;
CREATE INDEX IF NOT EXISTS idx_report_history_active
    ON ads.ads_fr2052a_report_history (report_id) WHERE is_active;
CREATE INDEX IF NOT EXISTS idx_access_log_time ON audit.audit_access_log (access_time);
CREATE INDEX IF NOT EXISTS idx_change_log_table ON audit.audit_change_log (table_name, change_time);

-- ---------------------------------------------------------------------------
-- 10. 数据质量结果日志：一个批次的一条规则一行。
--
--     本表原先没有显式 DDL，靠 Spark 的 JDBC 追加写自动建表 —— 于是表结构真源里
--     看不到它，字段与类型只能靠反查库才知道。这里补上显式定义。
--
--     为什么粒度是 (batch_id, validation_rule_id)：结果按批次追加、跨批次保留历史，
--     所以重跑同一批次必须先删掉该批次的旧行
--     （python/validators/clear_dq_batch.py），否则重跑一次多一份，
--     「本批次有几条 ERROR」会随重跑次数翻倍，而且不报错。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads.ads_fr2052a_validation_log (
    batch_id TEXT NOT NULL,                          -- 批次号
    validation_rule_id TEXT NOT NULL,                -- 规则编码
    rule_description TEXT,                           -- 规则说明
    rule_category TEXT,                              -- 规则类别
    severity TEXT,                                   -- ERROR / WARNING / INFO
    check_result TEXT,                               -- PASS / FAIL / SKIPPED
    actual_value TEXT,                               -- 实际值
    expected_value TEXT,                             -- 期望值
    detail TEXT,                                     -- 明细说明
    apply_layer TEXT,                                -- 规则适用层
    affected_line_item TEXT,                         -- 受影响的报送行项目
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE ads.ads_fr2052a_validation_log IS '数据质量校验结果，按批次追加，跨批次保留历史';

CREATE INDEX IF NOT EXISTS idx_validation_log_batch
    ON ads.ads_fr2052a_validation_log (batch_id, check_result);

-- ---------------------------------------------------------------------------
-- 11. 导出表迁移：ads_fr2052a_detail 与 ads_gl_reconciliation 由 Spark JDBC
--     用 truncate=true 覆盖写。truncate=true 保留表上的授权与触发器，但不会
--     自己建列。模型结构变了必须走显式迁移（export_gold_to_pg.py 的 docstring
--     里写明这条纪律）。以下 DO 块在表已存在且缺列时幂等补列。
-- ---------------------------------------------------------------------------

-- ads_fr2052a_detail: 新增 is_intracompany 列（明细回溯核对用它排除抵销项）
DO $$
BEGIN
    IF EXISTS (
        -- 判存在走 pg_catalog 而不是 information_schema：后者只列出当前角色有权限的对象，
        -- 「表在但读不到」会被当成「表不存在」，迁移就被静默跳过（B2 批次立下的口径）。
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'ads' AND c.relname = 'ads_fr2052a_detail'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'ads' AND c.relname = 'ads_fr2052a_detail'
          AND a.attname = 'is_intracompany' AND a.attnum > 0 AND NOT a.attisdropped
    ) THEN
        ALTER TABLE ads.ads_fr2052a_detail
            ADD COLUMN is_intracompany BOOLEAN;
    END IF;
END $$;

-- ads_fr2052a_report: Section E 列名与语义对齐（C4a）
--   原来的 sec_e_central_bank_dep 挂的是库存现金 —— 列名与语义错配，读的人必然误判；
--   sec_e_cash_equiv_total 与 sec_e_cash_total 是同一个数（重复计两次）。
--   改为：库存现金单列、同业存放单列、央行存款列保持 NULL（本演示总账里没有该科目）。
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'ads' AND c.relname = 'ads_fr2052a_report'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'ads' AND c.relname = 'ads_fr2052a_report'
              AND a.attname = 'sec_e_cash_on_hand' AND a.attnum > 0 AND NOT a.attisdropped
        ) THEN
            ALTER TABLE ads.ads_fr2052a_report ADD COLUMN sec_e_cash_on_hand NUMERIC(20, 2);
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'ads' AND c.relname = 'ads_fr2052a_report'
              AND a.attname = 'sec_e_due_from_banks' AND a.attnum > 0 AND NOT a.attisdropped
        ) THEN
            ALTER TABLE ads.ads_fr2052a_report ADD COLUMN sec_e_due_from_banks NUMERIC(20, 2);
        END IF;
        IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'ads' AND c.relname = 'ads_fr2052a_report'
              AND a.attname = 'sec_e_cash_equiv_total' AND a.attnum > 0 AND NOT a.attisdropped
        ) THEN
            -- 重复列：与 sec_e_cash_total 同值，模型里已删掉，库里也一并删
            ALTER TABLE ads.ads_fr2052a_report DROP COLUMN sec_e_cash_equiv_total;
        END IF;
    END IF;
END $$;

-- ads_fr2052a_report: 新增 sec_c_insured_total 列（C4a：受保金额先折算再截断，报表新增受保合计列）
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'ads' AND c.relname = 'ads_fr2052a_report'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'ads' AND c.relname = 'ads_fr2052a_report'
          AND a.attname = 'sec_c_insured_total' AND a.attnum > 0 AND NOT a.attisdropped
    ) THEN
        ALTER TABLE ads.ads_fr2052a_report ADD COLUMN sec_c_insured_total NUMERIC(20, 2);
    END IF;
END $$;

-- ads_gl_reconciliation: 新增 entity_code 列（按视角对账需要区分实体/合并行）
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'ads' AND c.relname = 'ads_gl_reconciliation'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'ads' AND c.relname = 'ads_gl_reconciliation'
          AND a.attname = 'entity_code' AND a.attnum > 0 AND NOT a.attisdropped
    ) THEN
        ALTER TABLE ads.ads_gl_reconciliation
            ADD COLUMN entity_code TEXT;
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- 12. 报表三张表的唯一约束（C5-1 B 节）
--     三张导出表由 Spark JDBC 用 truncate=true 覆盖写，表本身没有库侧主键。
--     这里用幂等 DO 块补唯一约束，防止导出作业或手工写入产生重复行。
--     判存在一律走 pg_catalog（information_schema 只列当前角色有权限的对象，
--     本仓 B2 批次已立过这条口径）。
--
--     键的粒度依据：
--       ads_fr2052a_report: report_id 天然唯一（含「机构-报表-报告期-口径」四段），
--         同一报告期同一实体同一口径只会出一行。
--       ads_fr2052a_detail: 粒度为「报告日 × 实体 × 抵销标记 × Section × 行项目 ×
--         产品类别 × 交易对手类型 × 币种 × 到期分桶」——明细是维度组合下钻，
--         同一维度组合不应有两行。
--       ads_gl_reconciliation: 粒度为「报告日 × 实体 × Section」——每个实体每个
--         报告期每个 Section 一行对账结果（合并行 entity_code = GRP001）。
-- ---------------------------------------------------------------------------

-- ads_fr2052a_report: report_id 唯一
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'ads' AND c.relname = 'ads_fr2052a_report'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conname = 'ads_fr2052a_report_report_id_uk'
          AND conrelid = 'ads.ads_fr2052a_report'::regclass
    ) THEN
        ALTER TABLE ads.ads_fr2052a_report
            ADD CONSTRAINT ads_fr2052a_report_report_id_uk UNIQUE (report_id);
    END IF;
END $$;

-- ads_fr2052a_detail: 全维度组合唯一
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'ads' AND c.relname = 'ads_fr2052a_detail'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conname = 'ads_fr2052a_detail_dim_uk'
          AND conrelid = 'ads.ads_fr2052a_detail'::regclass
    ) THEN
        ALTER TABLE ads.ads_fr2052a_detail
            ADD CONSTRAINT ads_fr2052a_detail_dim_uk
            UNIQUE (report_date, entity_code, is_intracompany, section_code,
                    line_item, product_category, counterparty_type,
                    currency_code, maturity_bucket);
    END IF;
END $$;

-- ads_gl_reconciliation: (report_date, entity_code, section_code) 唯一
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'ads' AND c.relname = 'ads_gl_reconciliation'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conname = 'ads_gl_reconciliation_uk'
          AND conrelid = 'ads.ads_gl_reconciliation'::regclass
    ) THEN
        ALTER TABLE ads.ads_gl_reconciliation
            ADD CONSTRAINT ads_gl_reconciliation_uk
            UNIQUE (report_date, entity_code, section_code);
    END IF;
END $$;
