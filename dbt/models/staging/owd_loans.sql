-- OWD 贷款：贷款台账标准化。
-- 加工内容：外币折算、借款人补全、抵押标记、到期分桶、未提取额度的承诺属性。

with loans as (

    select * from {{ source('bronze', 'ods_loans') }}

),

counterparty as (

    select * from {{ source('ref', 'ref_counterparty') }}

),

fx as (

    select * from {{ ref('stg_fx_rates') }}

)

select
    l.source_system,
    l.source_record_id,
    l.report_date,
    l.entity_code,
    {{ mask_pii('l.loan_id') }} as loan_id,
    {{ mask_pii('l.borrower_id') }} as borrower_id,
    c.counterparty_name as borrower_name,
    l.borrower_type_raw as borrower_type,
    c.country_code as borrower_country,
    c.industry_code as borrower_industry,
    l.currency as currency_code,
    round(l.facility_amount * f.spot_rate, 2) as facility_amount_usd,
    round(l.outstanding_amount * f.spot_rate, 2) as outstanding_usd,
    round(l.undrawn_amount * f.spot_rate, 2) as undrawn_usd,
    l.loan_type,
    -- 有未提取额度即为承诺额度，进 Section J 的判定依据
    case when l.undrawn_amount > 0 then true else false end as is_commitment,
    case when l.undrawn_amount > 0 then 'UNCONDITIONAL' else null end as commitment_type,
    l.interest_rate,
    l.rate_type,
    l.credit_grade_raw as internal_rating,
    l.collateral_flag = 'Y' as is_collateralized,
    l.origination_date,
    l.maturity_date,
    l.next_payment_date,
    datediff(l.maturity_date, l.report_date) as days_to_maturity,
    {{ maturity_bucket('datediff(l.maturity_date, l.report_date)') }} as maturity_bucket,
    {{ is_affiliate_counterparty('c.counterparty_type') }} as is_intracompany,
    l.event_time,
    l.etl_batch_id

from loans l
left join counterparty c
    on c.counterparty_id = l.borrower_id
left join fx f
    on f.currency_code = l.currency
   and f.rate_date = l.report_date
