-- OWD 衍生品：衍生品交易标准化。
-- 加工内容：外币折算、产品类别归类、净风险敞口、到期分桶。

with derivatives as (

    select * from {{ source('bronze', 'ods_derivatives') }}

),

counterparty as (

    select * from {{ source('ref', 'ref_counterparty') }}

),

fx as (

    select * from {{ ref('stg_fx_rates') }}

)

select
    d.source_system,
    d.source_record_id,
    d.report_date,
    d.entity_code,
    d.trade_id as derivative_id,
    d.counterparty_id,
    c.counterparty_type,
    c.country_code as counterparty_country,
    d.instrument_type,
    -- 产品类别：按工具类型归入 RATE/FX/CREDIT/EQUITY
    case
        when d.instrument_type in ('IRS', 'FUTURES') then 'RATE'
        when d.instrument_type in ('FX_FWD', 'FX_SWAP') then 'FX'
        when d.instrument_type = 'CDS' then 'CREDIT'
        when d.instrument_type = 'OPTION' then 'EQUITY'
        else 'OTHER'
    end as product_class,
    round(d.notional_amount * f.spot_rate, 2) as notional_usd,
    d.currency as currency_code,
    d.currency_pair,
    d.trade_date,
    d.maturity_date,
    datediff(d.maturity_date, d.report_date) as days_to_maturity,
    {{ maturity_bucket('datediff(d.maturity_date, d.report_date)') }} as maturity_bucket,
    {{ is_affiliate_counterparty('c.counterparty_type') }} as is_intracompany,
    round(d.mark_to_market * f.spot_rate, 2) as mtm_value_usd,
    d.is_central_cleared = 'Y' as is_central_cleared,
    d.csa_agreement_id,
    -- 有 CSA 即视为双边净额协议覆盖
    case when d.csa_agreement_id is null then false else true end as is_bilateral_netted,
    round(d.collateral_posted * f.spot_rate, 2) as collateral_posted_usd,
    round(d.collateral_received * f.spot_rate, 2) as collateral_received_usd,
    -- 净敞口：已收抵押品抵减盯市资产；盯市为负时由已提交抵押品抵减负债
    round(
        case
            when d.mark_to_market >= 0 then greatest(d.mark_to_market - d.collateral_received, 0)
            else least(d.mark_to_market + d.collateral_posted, 0)
        end * f.spot_rate,
        2
    ) as net_exposure_usd,
    d.event_time,
    d.etl_batch_id

from derivatives d
left join counterparty c
    on c.counterparty_id = d.counterparty_id
left join fx f
    on f.currency_code = d.currency
   and f.rate_date = d.report_date
