-- OWD 衍生品：衍生品交易标准化。
-- 加工内容：外币折算、产品类别归类、净风险敞口、到期分桶。
--
-- 折算币种不是「一个币种」：名义本金与抵押品按**交易币种**（currency）折算，
-- 盯市价值按 ODS 声明的**盯市币种**（mtm_currency）折算。跨币种交易的盯市值常直接以
-- 报告币种计价，用交易币种去乘汇率会让金额差几个量级（本样本里 mtm_currency 恒为 USD，
-- 差异不显形，但口径得对）。盯市币种与所用汇率都输出成列，口径是可核的。

with derivatives as (

    select * from {{ source('bronze', 'ods_derivatives') }}

),

counterparty as (

    select * from {{ source('ref', 'ref_counterparty') }}

),

fx_transaction as (

    select * from {{ ref('stg_fx_rates') }}

),

fx_mark_to_market as (

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
    round(d.notional_amount * ft.spot_rate, 2) as notional_usd,
    d.currency as currency_code,
    d.currency_pair,
    d.trade_date,
    d.maturity_date,
    datediff(d.maturity_date, d.report_date) as days_to_maturity,
    {{ maturity_bucket('datediff(d.maturity_date, d.report_date)') }} as maturity_bucket,
    {{ is_affiliate_counterparty('c.counterparty_type') }} as is_intracompany,
    round(d.mark_to_market * fm.spot_rate, 2) as mtm_value_usd,
    d.mtm_currency,
    fm.spot_rate as mtm_exchange_rate,
    d.is_central_cleared = 'Y' as is_central_cleared,
    d.csa_agreement_id,
    -- 有 CSA 即视为双边净额协议覆盖
    case when d.csa_agreement_id is null then false else true end as is_bilateral_netted,
    round(d.collateral_posted * ft.spot_rate, 2) as collateral_posted_usd,
    round(d.collateral_received * ft.spot_rate, 2) as collateral_received_usd,
    -- 净敞口：已收抵押品抵减盯市资产；盯市为负时由已提交抵押品抵减负债。
    -- 两侧各自按自己的币种折算后再相抵（抵押品按交易币种，ODS 没有单独的抵押品币种列）。
    round(
        case
            when d.mark_to_market >= 0
                then greatest(d.mark_to_market * fm.spot_rate - d.collateral_received * ft.spot_rate, 0)
            else least(d.mark_to_market * fm.spot_rate + d.collateral_posted * ft.spot_rate, 0)
        end,
        2
    ) as net_exposure_usd,
    d.event_time,
    d.etl_batch_id

from derivatives d
left join counterparty c
    on c.counterparty_id = d.counterparty_id
left join fx_transaction ft
    on ft.currency_code = d.currency
   and ft.rate_date = d.report_date
left join fx_mark_to_market fm
    on fm.currency_code = d.mtm_currency
   and fm.rate_date = d.report_date
