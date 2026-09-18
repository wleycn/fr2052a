-- 行为假设覆盖断言：ows_cashflow_projection 的存款流失项里出现未命中假设组合即失败。
--
-- dbt singular test 语义：返回任何一行即测试失败。
-- 与生成器自检 check_behavior_coverage 互补：
--   生成器自检在 CSV 层验维度覆盖，这里在转换后的 Silver 层验实际 join 结果。
-- coalesce 兜底已删掉，未命中时 runoff_rate 为 NULL，流失额变 0 —— 这条断言把「静默变 0」变显式。

select
    d.report_date,
    d.entity_code,
    d.product_category,
    d.customer_segment,
    d.maturity_bucket,
    '行为假设未命中' as reason
from {{ ref('owd_deposits') }} d
left join {{ source('ref', 'ref_behavior_assumptions') }} a
    on a.product_category = d.product_category
   and a.customer_segment = d.customer_segment
   and a.maturity_bucket = d.maturity_bucket
where a.runoff_rate is null
