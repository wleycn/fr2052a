-- OWS 融资汇总：按产品类别与到期分桶汇总资金来源，并给出加权平均成本。
-- 融资方 = 存款（客户资金）+ 回购（担保融资）。

with deposits as (

    select
        report_date,
        entity_code,
        product_category,
        maturity_bucket,
        count(*) as record_count,
        sum(principal_amount_usd) as outstanding_usd,
        sum(principal_amount_usd * interest_rate) as rate_weighted_amount
    from {{ ref('owd_deposits') }}
    group by report_date, entity_code, product_category, maturity_bucket

),

repo as (

    select
        report_date,
        entity_code,
        transaction_type as product_category,
        maturity_bucket,
        count(*) as record_count,
        sum(cash_amount_usd) as outstanding_usd,
        sum(cash_amount_usd * interest_rate) as rate_weighted_amount
    from {{ ref('owd_secured_financing') }}
    where transaction_type = 'REPO'
    group by report_date, entity_code, transaction_type, maturity_bucket

),

funding as (

    select * from deposits
    union all
    select * from repo

)

select
    report_date,
    entity_code,
    product_category,
    maturity_bucket,
    sum(record_count) as record_count,
    round(sum(outstanding_usd), 2) as total_outstanding_usd,
    -- 加权平均利率 = Σ(金额 × 利率) / Σ金额
    round(sum(rate_weighted_amount) / nullif(sum(outstanding_usd), 0), 6) as weighted_avg_rate

from funding
group by report_date, entity_code, product_category, maturity_bucket
