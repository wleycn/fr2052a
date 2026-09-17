-- OWS 抵押品汇总：按抵押品类型与 HQLA 等级汇总。
--
-- 说明：源数据只有"是否质押"的布尔标记，没有质押去向，因此本表不产出
-- pledged_to_repos / pledged_to_derivatives / pledged_to_central_bank 三个拆分列 ——
-- 没有数据来源的列不建，避免下游误用。

with securities as (

    select * from {{ ref('owd_securities') }}

)

select
    s.report_date,
    s.entity_code,
    s.security_type as collateral_type,
    s.hqla_classification,
    s.issuer_country,
    count(*) as record_count,
    round(sum(s.market_value_usd), 2) as total_market_value_usd,
    round(sum(case when s.is_encumbered then s.market_value_usd else 0 end), 2) as encumbered_value_usd,
    round(sum(case when s.is_encumbered then 0 else s.market_value_usd end), 2) as unencumbered_value_usd,
    round(sum(s.market_value_usd * (1 - s.hqla_haircut_rate)), 2) as haircuted_value_usd,
    -- 可用于质押：非受限资产的折扣后价值
    round(sum(case when s.is_encumbered then 0 else s.market_value_usd * (1 - s.hqla_haircut_rate) end), 2)
        as available_for_pledge

from securities s
group by
    s.report_date,
    s.entity_code,
    s.security_type,
    s.hqla_classification,
    s.issuer_country
