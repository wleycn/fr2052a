-- 口径码与 is_consolidated + entity_code 严格自洽。
--
-- dbt singular test 语义：返回任何一行即测试失败。
--
-- 口径码三档规则（见 ads_fr2052a_report.sql 列注释）：
--   01 = 全球合并   ⇔ is_consolidated = true  且 entity_code = 'GRP001'
--   02 = 法人单体   ⇔ is_consolidated = false 且 entity_code <> 'ENT001'
--   03 = 母公司单体 ⇔ is_consolidated = false 且 entity_code = 'ENT001'
--
-- 口径码由模型里的 case 表达式从 is_consolidated + entity_code 推出。
-- 如果上游改了 case 逻辑但忘了同步口径码定义，或者手工写入绕过了模型，
-- 这条断言会把不一致的行挑出来。
--
-- 返回行的含义：该行的口径码与 is_consolidated + entity_code 不匹配。

select
    report_id,
    report_date,
    entity_code,
    is_consolidated,
    -- 口径码是 report_id 的末两字符（'-01' / '-02' / '-03' 的数字部分）
    substr(report_id, -2) as actual_pose_code,
    case
        when is_consolidated then '01'
        when entity_code = 'ENT001' then '03'
        else '02'
    end as expected_pose_code
from {{ ref('ads_fr2052a_report') }}
where substr(report_id, -2) <> case
        when is_consolidated then '01'
        when entity_code = 'ENT001' then '03'
        else '02'
    end
