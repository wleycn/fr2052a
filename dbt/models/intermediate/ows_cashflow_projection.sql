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
        'C' as section_code,
        'C-OUT' as line_item,
        d.maturity_bucket,
        cast(0 as decimal(20, 2)) as expected_inflow_usd,
        round(sum(d.principal_amount_usd * coalesce(a.runoff_rate, 0.1)), 2) as expected_outflow_usd
    from {{ ref('owd_deposits') }} d
    left join {{ source('ref', 'ref_behavior_assumptions') }} a
        on a.product_category = d.product_category
       and a.maturity_bucket = d.maturity_bucket
    group by d.report_date, d.entity_code, d.maturity_bucket

),

repo_maturity as (

    select
        r.report_date,
        r.entity_code,
        'B' as section_code,
        'B-OUT' as line_item,
        r.maturity_bucket,
        cast(0 as decimal(20, 2)) as expected_inflow_usd,
        round(sum(r.cash_amount_usd), 2) as expected_outflow_usd
    from {{ ref('owd_secured_financing') }} r
    where r.transaction_type = 'REPO'
    group by r.report_date, r.entity_code, r.maturity_bucket

),

loan_maturity as (

    select
        l.report_date,
        l.entity_code,
        'F' as section_code,
        'F-IN' as line_item,
        l.maturity_bucket,
        round(sum(l.outstanding_usd), 2) as expected_inflow_usd,
        cast(0 as decimal(20, 2)) as expected_outflow_usd
    from {{ ref('owd_loans') }} l
    group by l.report_date, l.entity_code, l.maturity_bucket

),

security_maturity as (

    select
        s.report_date,
        s.entity_code,
        'G' as section_code,
        'G-IN' as line_item,
        s.maturity_bucket,
        round(sum(s.market_value_usd), 2) as expected_inflow_usd,
        cast(0 as decimal(20, 2)) as expected_outflow_usd
    from {{ ref('owd_securities') }} s
    where not s.is_encumbered
    group by s.report_date, s.entity_code, s.maturity_bucket

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
    section_code,
    line_item,
    maturity_bucket,
    round(sum(expected_inflow_usd), 2) as expected_inflow_usd,
    round(sum(expected_outflow_usd), 2) as expected_outflow_usd,
    round(sum(expected_inflow_usd) - sum(expected_outflow_usd), 2) as net_cash_flow_usd

from combined
group by report_date, entity_code, section_code, line_item, maturity_bucket
