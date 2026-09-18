-- 受保金额上限断言：同一客户在同一法人实体的受保金额合计不得超过存款保险限额。
--
-- 为什么需要它：逐笔按限额截断，等于给同一客户的多笔存款各发一次额度，
-- 客户的受保金额会被系统性高估，而这个错误在报表上表现为「C 项受保部分偏大」，
-- 不会报错、不会为空。这条断言把口径错误变成显式失败。
--
-- 容差 0.01：模型里金额按两位小数四舍五入，按笔数求和后可能多出分位误差。

select
    report_date,
    entity_code,
    customer_id,
    round(sum(insured_amount_usd), 2) as insured_sum
from {{ ref('owd_deposits') }}
group by report_date, entity_code, customer_id
having round(sum(insured_amount_usd), 2) > {{ var('deposit_insurance_limit_usd') }} + 0.01
