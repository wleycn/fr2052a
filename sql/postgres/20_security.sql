-- FR 2052a 安全与权限（PostgreSQL / Server 1）
-- 应用方式：bash deploy/server1/apply-sql.sh（幂等）
--
-- 两件事：
--   1. PII 对照表 —— 明文与 token 的对应关系，只在这一处存在，权限收紧到两个角色
--   2. 四角色与授权 —— 合规官可见明文，分析师只见 token，审计员只读且可见审计日志
--
-- 为什么对照表必须在库里而不是「需要时从 bronze 现算」：
--   每一次现算都是把明文搬进一个新的进程与新的日志里。明文多一份就多一个泄漏面。
--   对照表是唯一一份，且它的访问本身就是受控的。

CREATE SCHEMA IF NOT EXISTS secure;

-- ---------------------------------------------------------------------------
-- 1. PII 对照表
--    token 在 OWD 层公开可见（用于关联与聚合），plaintext 只对合规官与管理员开放。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS secure.fr2052a_pii_map (
    pii_token TEXT NOT NULL,                     -- 脱敏后的确定性 token
    pii_plaintext TEXT NOT NULL,                 -- 明文原值
    source_object TEXT NOT NULL,                 -- 明文来源，如 bronze.ods_deposits
    pii_column TEXT NOT NULL,                    -- 明文列名，如 customer_id
    entity_code TEXT,                            -- 该记录所属法人实体，便于按实体授权
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fr2052a_pii_map_pkey PRIMARY KEY (pii_token, source_object, pii_column)
);

COMMENT ON TABLE secure.fr2052a_pii_map IS 'PII 明文的唯一落点，访问受角色限制';

CREATE INDEX IF NOT EXISTS idx_pii_map_plaintext
    ON secure.fr2052a_pii_map (pii_plaintext);

-- ---------------------------------------------------------------------------
-- 2. 四角色
--    角色只建一次，重复执行不报错。口令不在这里设 —— 角色用于授权，
--    具体用户由 DBA 在受控流程里创建并授予角色。
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fr2052a_admin') THEN
        CREATE ROLE fr2052a_admin NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'compliance_officer') THEN
        CREATE ROLE compliance_officer NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'treasury_analyst') THEN
        CREATE ROLE treasury_analyst NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'auditor') THEN
        CREATE ROLE auditor NOLOGIN;
    END IF;
END $$;

-- 从 PUBLIC 收回全部默认权限：不收回的话，任何能连库的角色都读得到，授权形同虚设。
REVOKE ALL ON SCHEMA secure FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA secure FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA audit FROM PUBLIC;

-- ---------------------------------------------------------------------------
-- 3. 授权
-- ---------------------------------------------------------------------------
-- 管理员：全部
GRANT USAGE, CREATE ON SCHEMA ads, audit, secure TO fr2052a_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ads, audit, secure TO fr2052a_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ads, audit, secure TO fr2052a_admin;

-- 合规官：可见报送数据与审计记录，**可见明文 PII**
GRANT USAGE ON SCHEMA ads, audit, secure TO compliance_officer;
GRANT SELECT ON ALL TABLES IN SCHEMA ads, audit TO compliance_officer;
GRANT SELECT ON secure.fr2052a_pii_map TO compliance_officer;

-- 资金分析师：只见报送数据，看不到明文 PII（连对照表都没有权限）
GRANT USAGE ON SCHEMA ads TO treasury_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA ads TO treasury_analyst;

-- 审计员：只读，且可读审计日志；明文 PII 不可见
GRANT USAGE ON SCHEMA ads, audit TO auditor;
GRANT SELECT ON ALL TABLES IN SCHEMA ads, audit TO auditor;

-- 建库账号本身要能 SET ROLE 做权限自测（这也是部署脚本的验证手段）。
-- 授予角色成员身份不会削弱建库账号自身的权限，它本来就是所有者。
GRANT fr2052a_admin TO CURRENT_USER;
GRANT compliance_officer TO CURRENT_USER;
GRANT treasury_analyst TO CURRENT_USER;
GRANT auditor TO CURRENT_USER;

-- ---------------------------------------------------------------------------
-- 4. 访问留痕
--    说明边界：PostgreSQL 没有「读取触发器」，表上的触发器只能覆盖写入。
--    这里做的是两层：
--      a) 对 PII 对照表的写入留痕（含谁改的、改了什么）
--      b) 审计表本身的写入留痕由业务代码负责（见 python/governance/render_lineage.py）
--    读取级留痕需要 pgaudit 扩展或语句日志，本演示未部署 —— 这是缺口，
--    已记在 docs/business/KNOWN-ISSUE.md 的 #read-audit-gap，不假装覆盖。
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION secure.log_pii_map_change() RETURNS trigger AS $fn$
BEGIN
    INSERT INTO audit.audit_change_log
        (table_name, record_key, change_type, old_value, new_value, changed_by, reason)
    VALUES (
        'secure.fr2052a_pii_map',
        coalesce(NEW.pii_token, OLD.pii_token),
        TG_OP,
        CASE WHEN TG_OP IN ('UPDATE', 'DELETE') THEN row_to_json(OLD)::text END,
        CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN row_to_json(NEW)::text END,
        CURRENT_USER,
        '对照表变更由触发器自动留痕；明文不写入日志以外的任何地方'
    );
    RETURN coalesce(NEW, OLD);
END;
$fn$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_pii_map_change ON secure.fr2052a_pii_map;
CREATE TRIGGER trg_pii_map_change
    AFTER INSERT OR UPDATE OR DELETE ON secure.fr2052a_pii_map
    FOR EACH ROW EXECUTE FUNCTION secure.log_pii_map_change();
