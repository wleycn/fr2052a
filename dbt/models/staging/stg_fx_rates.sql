-- 汇率折算基准：报告日的中间价，折算目标为 USD。
-- 各 OWD 模型都 join 这张表做外币折算，避免每张表各写一遍汇率口径。
-- ref_exchange_rates 里含 USD→USD（汇率为 1），因此所有币种都能直接相乘，不需要分支。
-- 限定 to_currency = 'USD'：汇率表一旦出现多目标币种（如 EUR→GBP），
-- 不限定会导致 OWD 各表的 join 行放大，每条明细乘出多行。

select
    from_currency as currency_code,
    to_currency as report_currency,
    spot_rate,
    rate_date

from {{ source('ref', 'ref_exchange_rates') }}
where rate_type = 'MID'
  and to_currency = 'USD'
