-- OWD 表外项目：授信承诺、信用证、担保标准化。
-- 加工内容：外币折算、交易对手补全、到期分桶。未提取额度是或有负债的口径来源。

with commitments as (

    select * from {{ source('bronze', 'ods_off_bs_commitments') }}

),

counterparty as (

    select * from {{ source('ref', 'ref_counterparty') }}

),

fx as (

    select * from {{ ref('stg_fx_rates') }}

)

select
    o.source_system,
    o.source_record_id,
    o.report_date,
    o.entity_code,
    o.commitment_id,
    o.counterparty_id,
    c.counterparty_name,
    c.counterparty_type,
    o.commitment_type,
    round(o.facility_amount * f.spot_rate, 2) as facility_amount_usd,
    round(o.undrawn_amount * f.spot_rate, 2) as undrawn_amount_usd,
    o.currency as currency_code,
    o.maturity_date,
    datediff(o.maturity_date, o.report_date) as days_to_maturity,
    {{ maturity_bucket('datediff(o.maturity_date, o.report_date)') }} as maturity_bucket,
    o.event_time,
    o.etl_batch_id

from commitments o
left join counterparty c
    on c.counterparty_id = o.counterparty_id
left join fx f
    on f.currency_code = o.currency
   and f.rate_date = o.report_date
