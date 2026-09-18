-- OWD 司库现金头寸：库存现金盘点数与代理行对账单余额标准化。
-- 加工内容：外币折算、未达账项原币金额折算为 USD。
--
-- 这张表是 GL 对账 Section E 的**基准侧**。它与总账 1001/1100 是两个来源：
-- 对账单/盘点口径记录的是外部能看到的事实，账面口径记录的是本行的账。两侧的差
-- 由在途存款与未兑现支票逐项解释，因此对账能真正判对错 —— 两侧若同读一份数据，
-- 差异恒为零，任何错误都查不出来（审计点名的缺陷形态）。
--
-- 对账单日期恒等于报告日（日终对账单），因此不单列一列。

with positions as (

    select * from {{ source('bronze', 'ods_treasury_cash_position') }}

),

fx as (

    select * from {{ ref('stg_fx_rates') }}

)

select
    p.source_system,
    p.source_record_id,
    p.report_date,
    p.entity_code,
    p.position_type,
    p.custodian_id,
    p.account_ref,
    p.currency as currency_code,
    round(p.balance_amount * f.spot_rate, 2) as balance_usd,
    round(p.in_transit_deposits_amount * f.spot_rate, 2) as in_transit_deposits_usd,
    round(p.outstanding_checks_amount * f.spot_rate, 2) as outstanding_checks_usd,
    p.event_time,
    p.etl_batch_id

from positions p
left join fx f
    on f.currency_code = p.currency
   and f.rate_date = p.report_date
