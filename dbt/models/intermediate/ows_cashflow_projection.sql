-- OWS 现金流预测：按 Section 与到期分桶汇总预期流入与流出。
--
-- 口径来源：
--   存款流失      → Section C 流出，流失率取自 ref_behavior_assumptions（按产品类别与分桶匹配）
--   回购到期      → Section B 流出
--   贷款到期本金  → Section F 流入
--   证券到期本息  → Section G 流入
--
-- 现金流上限（流入 ≤ 流出的 75%）不在本层应用，留到 ADS 报送口径里处理，
-- 本层只做原样汇总，便于上游数据问题时能看清未加限制前的事实。

with deposit_runoff as (

    select
        d.report_date,
        d.entity_code,
        d.is_intracompany,
        'C' as section_code,
        'C-OUT' as line_item,
        d.maturity_bucket,
        cast(0 as decimal(20, 2)) as expected_inflow_usd,
        -- 不再对未命中的假设兜底 10%：缺假设是缺陷，由 dbt/tests/assert_behavior_covered.sql 拦住。
        -- coalesce 只兜「该产品类别一行假设都没有」这种全空求和，不掩盖部分缺行 —— 部分缺行由断言拦。
        round(coalesce(sum(d.principal_amount_usd * a.runoff_rate), 0), 2) as expected_outflow_usd
    from {{ ref('owd_deposits') }} d
    left join {{ source('ref', 'ref_behavior_assumptions') }} a
        on a.product_category = d.product_category
       and a.customer_segment = d.customer_segment
       and a.maturity_bucket = d.maturity_bucket
    group by d.report_date, d.entity_code, d.is_intracompany, d.maturity_bucket

),

repo_maturity as (

    select
        r.report_date,
        r.entity_code,
        r.is_intracompany,
        'B' as section_code,
        'B-OUT' as line_item,
        r.maturity_bucket,
        cast(0 as decimal(20, 2)) as expected_inflow_usd,
        round(sum(r.cash_amount_usd), 2) as expected_outflow_usd
    from {{ ref('owd_secured_financing') }} r
    where r.transaction_type = 'REPO'
    group by r.report_date, r.entity_code, r.is_intracompany, r.maturity_bucket

),

loan_maturity as (

    select
        l.report_date,
        l.entity_code,
        l.is_intracompany,
        'F' as section_code,
        'F-IN' as line_item,
        l.maturity_bucket,
        round(sum(l.outstanding_usd), 2) as expected_inflow_usd,
        cast(0 as decimal(20, 2)) as expected_outflow_usd
    from {{ ref('owd_loans') }} l
    group by l.report_date, l.entity_code, l.is_intracompany, l.maturity_bucket

),

security_maturity as (

    select
        s.report_date,
        s.entity_code,
        s.is_intracompany,
        'G' as section_code,
        'G-IN' as line_item,
        s.maturity_bucket,
        round(sum(s.market_value_usd), 2) as expected_inflow_usd,
        cast(0 as decimal(20, 2)) as expected_outflow_usd
    from {{ ref('owd_securities') }} s
    where not s.is_encumbered
    group by s.report_date, s.entity_code, s.is_intracompany, s.maturity_bucket

),

combined as (

    select * from deposit_runoff
    union all
    select * from repo_maturity
    union all
    select * from loan_maturity
    union all
    select * from security_maturity

)

select
    report_date,
    entity_code,
    is_intracompany,
    section_code,
    line_item,
    maturity_bucket,
    round(sum(expected_inflow_usd), 2) as expected_inflow_usd,
    round(sum(expected_outflow_usd), 2) as expected_outflow_usd,
    round(sum(expected_inflow_usd) - sum(expected_outflow_usd), 2) as net_cash_flow_usd

-- is_intracompany 是分组维度：合并口径要能把集团内往来从现金流预测里同样剔除，
-- 否则会出现「Section C 抵销了、Section K 没抵销」的单边不一致。
from combined
group by report_date, entity_code, is_intracompany, section_code, line_item, maturity_bucket
