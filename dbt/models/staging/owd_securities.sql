-- OWD 证券：证券持仓标准化。
-- 加工内容：外币折算、HQLA 分级与折扣率、受限判定、未实现盈亏、到期分桶。

with securities as (

    select * from {{ source('bronze', 'ods_securities') }}

),

counterparty as (

    select * from {{ source('ref', 'ref_counterparty') }}

),

fx as (

    select * from {{ ref('stg_fx_rates') }}

)

select
    s.source_system,
    s.source_record_id,
    s.report_date,
    s.entity_code,
    s.security_id,
    s.isin,
    s.cusip,
    s.security_type,
    s.portfolio_code as portfolio_category,
    s.issuer_id,
    c.counterparty_name as issuer_name,
    c.counterparty_type as issuer_type,
    c.country_code as issuer_country,
    s.currency as currency_code,
    round(s.face_amount * f.spot_rate, 2) as face_amount_usd,
    round(s.market_value * f.spot_rate, 2) as market_value_usd,
    round(s.book_value * f.spot_rate, 2) as book_value_usd,
    round((s.market_value - s.book_value) * f.spot_rate, 2) as unrealized_gl_usd,
    s.coupon_rate,
    s.credit_rating_raw as credit_rating,
    {{ hqla_level('s.security_type', 's.credit_rating_raw') }} as hqla_classification,
    {{ hqla_haircut(hqla_level('s.security_type', 's.credit_rating_raw')) }} as hqla_haircut_rate,
    s.pledged_flag = 'Y' as is_pledged,
    -- 已质押即视为受限资产，不能再计入可用流动性
    s.pledged_flag = 'Y' as is_encumbered,
    s.purchase_date,
    s.maturity_date,
    datediff(s.maturity_date, s.report_date) as days_to_maturity,
    {{ maturity_bucket('datediff(s.maturity_date, s.report_date)') }} as maturity_bucket,
    s.event_time,
    s.etl_batch_id

from securities s
left join counterparty c
    on c.counterparty_id = s.issuer_id
left join fx f
    on f.currency_code = s.currency
   and f.rate_date = s.report_date
