-- 汇率覆盖断言：ODS 各表出现的 (report_date, currency) 必须在 stg_fx_rates 里有 MID 汇率。
--
-- 作用：把「折算失败」从静默变显式。缺汇率时 OWD 的 left join 落空，
-- 金额列变 NULL，sum() 跳过，报表出来是 0 而不是报错。
-- 这条断言的作用是「折算失败必须出声」，而不是「数据必须好看」。
--
-- dbt singular test 语义：返回任何一行即测试失败。
-- ods_gl_balances 的 currency 恒为 USD，不在检查范围。

with ods_currency_pairs as (
    select distinct report_date, currency
    from {{ source('bronze', 'ods_deposits') }}
    where currency is not null and currency != ''
    union
    select distinct report_date, currency
    from {{ source('bronze', 'ods_repo_transactions') }}
    where currency is not null and currency != ''
    union
    select distinct report_date, currency
    from {{ source('bronze', 'ods_loans') }}
    where currency is not null and currency != ''
    union
    select distinct report_date, currency
    from {{ source('bronze', 'ods_securities') }}
    where currency is not null and currency != ''
    union
    select distinct report_date, currency
    from {{ source('bronze', 'ods_derivatives') }}
    where currency is not null and currency != ''
    union
    select distinct report_date, currency
    from {{ source('bronze', 'ods_off_bs_commitments') }}
    where currency is not null and currency != ''
),

fx as (
    select * from {{ ref('stg_fx_rates') }}
)

select
    o.report_date,
    o.currency,
    '缺 MID 汇率' as reason
from ods_currency_pairs o
left join fx
    on fx.currency_code = o.currency
   and fx.rate_date = o.report_date
where fx.spot_rate is null
