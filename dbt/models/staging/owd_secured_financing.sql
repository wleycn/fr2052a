-- OWD 有担保融资：回购与逆回购明细标准化。
-- 加工内容：外币折算、交易对手补全、抵押品 HQLA 分级、净风险敞口、到期分桶。

with repo as (

    select * from {{ source('bronze', 'ods_repo_transactions') }}

),

counterparty as (

    select * from {{ source('ref', 'ref_counterparty') }}

),

fx as (

    select * from {{ ref('stg_fx_rates') }}

)

select
    r.source_system,
    r.source_record_id,
    r.report_date,
    r.entity_code,
    r.deal_id as financing_id,
    r.counterparty_id,
    c.counterparty_name,
    c.counterparty_type,
    c.country_code as counterparty_country,
    r.repo_type as transaction_type,
    r.currency as currency_code,
    r.cash_amount as cash_amount_lc,
    round(r.cash_amount * f.spot_rate, 2) as cash_amount_usd,
    r.collateral_market_value as collateral_mv_lc,
    round(r.collateral_market_value * f.spot_rate, 2) as collateral_mv_usd,
    r.haircut_pct,
    -- 净风险敞口：正回购是"多押了抵押品"，逆回购是"多付了现金"
    case
        when r.repo_type = 'REPO'
            then round((r.collateral_market_value - r.cash_amount) * f.spot_rate, 2)
        else round((r.cash_amount - r.collateral_market_value) * f.spot_rate, 2)
    end as net_exposure_usd,
    r.interest_rate,
    r.collateral_isin,
    r.collateral_type_raw as collateral_type,
    -- 抵押品 HQLA 分级：国债 Level 1，机构债与 MBS 归 Level 2A，公司债归 Level 2B
    case r.collateral_type_raw
        when 'UST' then 'LEVEL_1'
        when 'AGENCY' then 'LEVEL_2A'
        when 'MBS' then 'LEVEL_2A'
        when 'CORP' then 'LEVEL_2B'
        else 'NON_HQLA'
    end as collateral_hqla_level,
    -- 这两列是回购合约的起止日。原来的列名是 start_date / end_date，
    -- 与 SCD2 版本列 end_date 撞名 —— 同名会让版本化作业在建历史表时直接失败
    -- （同一张表里不允许两个 end_date）。改成语义完整的 deal_* 前缀既避开撞名，
    -- 也顺手说清了「这是哪一段的起止」。
    r.start_date as deal_start_date,
    r.end_date as deal_end_date,
    datediff(r.end_date, r.report_date) as days_to_maturity,
    {{ maturity_bucket('datediff(r.end_date, r.report_date)') }} as maturity_bucket,
    case when r.end_date is null then true else false end as is_open_ended,
    case when r.netting_agreement_id like 'GMRA%' then 'GMRA' else 'OTHER' end as master_agreement_type,
    r.netting_agreement_id,
    case when r.netting_agreement_id is null then false else true end as is_nettable,
    r.event_time,
    r.etl_batch_id

from repo r
left join counterparty c
    on c.counterparty_id = r.counterparty_id
left join fx f
    on f.currency_code = r.currency
   and f.rate_date = r.report_date
