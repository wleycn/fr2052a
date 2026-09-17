-- OWS 优质流动性资产汇总：按 HQLA 等级与法人实体汇总证券持仓。
-- 折扣后价值 = 市值 × (1 - 折扣率)；受限资产不计入可用流动性。

with securities as (

    select * from {{ ref('owd_securities') }}

)

select
    s.report_date,
    s.entity_code,
    s.hqla_classification,
    s.hqla_haircut_rate,
    count(*) as record_count,
    round(sum(s.market_value_usd), 2) as market_value_usd,
    round(sum(s.market_value_usd * (1 - s.hqla_haircut_rate)), 2) as haircuted_value_usd,
    round(sum(case when s.is_encumbered then s.market_value_usd else 0 end), 2) as encumbered_value_usd,
    round(sum(case when s.is_encumbered then 0 else s.market_value_usd end), 2) as unencumbered_value_usd,
    -- 可用流动性 = 非受限资产的折扣后价值
    round(sum(case when s.is_encumbered then 0 else s.market_value_usd * (1 - s.hqla_haircut_rate) end), 2)
        as available_liquidity_usd

from securities s
group by
    s.report_date,
    s.entity_code,
    s.hqla_classification,
    s.hqla_haircut_rate
