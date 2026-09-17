-- OWD 总账余额：GL 对账的标准化口径。
-- 总账本身以集团本位币 USD 记账，不需要折算；这里只做净额计算与科目方向归一。
-- 净额 = 借方 - 贷方：资产类科目为正、负债权益类为负，便于与报表口径直接比。

with gl as (

    select * from {{ source('bronze', 'ods_gl_balances') }}

)

select
    g.source_system,
    g.source_record_id,
    g.report_date,
    g.entity_code,
    g.gl_account_id,
    g.account_name,
    g.debit_balance,
    g.credit_balance,
    g.currency as currency_code,
    round(g.debit_balance - g.credit_balance, 2) as net_balance_usd,
    case when g.debit_balance > 0 then 'DEBIT' else 'CREDIT' end as balance_side,
    g.etl_batch_id

from gl g
