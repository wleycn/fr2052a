-- OWD 存款：源系统明细标准化。
-- 加工内容：外币折算 USD、客户与产品口径归一、到期分桶、受保金额按存款保险上限截断。

with deposits as (

    select * from {{ source('bronze', 'ods_deposits') }}

),

fx as (

    select * from {{ ref('stg_fx_rates') }}

)

select
    d.source_system,
    d.source_record_id,
    d.report_date,
    d.entity_code,
    d.account_number as deposit_id,
    d.customer_id,
    {{ customer_segment('d.customer_type_raw') }} as customer_segment,
    d.customer_type_raw as customer_type,
    {{ deposit_product_category('d.deposit_type') }} as product_category,
    d.deposit_type,
    d.currency as currency_code,
    d.principal_amount as principal_amount_lc,
    round(d.principal_amount * f.spot_rate, 2) as principal_amount_usd,
    d.accrued_interest as accrued_interest_lc,
    round(d.accrued_interest * f.spot_rate, 2) as accrued_interest_usd,
    f.spot_rate as exchange_rate,
    d.interest_rate,
    d.open_date,
    d.maturity_date,
    datediff(d.maturity_date, d.report_date) as days_to_maturity,
    {{ maturity_bucket('datediff(d.maturity_date, d.report_date)') }} as maturity_bucket,
    case when d.insured_flag = 'Y' then true else false end as is_insured,
    -- 受保金额按存款保险上限（25 万美元/客户）截断，而非全额计入
    round(
        case when d.insured_flag = 'Y' then least(d.principal_amount, 250000) * f.spot_rate else 0 end,
        2
    ) as insured_amount_usd,
    d.branch_code,
    d.event_time,
    d.etl_batch_id

from deposits d
left join fx f
    on f.currency_code = d.currency
   and f.rate_date = d.report_date
