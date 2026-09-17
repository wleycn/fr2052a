-- OWS 现金头寸：从总账取现金与同业存放科目，汇总为流动性口径的现金余额。
--
-- 数据来源说明：演示环境的总账只有 10 个科目，其中 1001 库存现金、1100 同业存放
-- 对应当前口径的现金项。需求文档里的央行存款、隔夜拆借、国库券等科目在本演示的
-- 总账里不存在，故不产出对应列。

with gl as (

    select * from {{ ref('owd_gl_entries') }}
    where gl_account_id in ('1001', '1100')

)

select
    g.report_date,
    g.entity_code,
    g.currency_code,
    round(sum(case when g.gl_account_id = '1001' then g.net_balance_usd else 0 end), 2) as cash_on_hand_usd,
    round(sum(case when g.gl_account_id = '1100' then g.net_balance_usd else 0 end), 2) as due_from_banks_usd,
    round(sum(g.net_balance_usd), 2) as total_cash_usd

from gl g
group by
    g.report_date,
    g.entity_code,
    g.currency_code
