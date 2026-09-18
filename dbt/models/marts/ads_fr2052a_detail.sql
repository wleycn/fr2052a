-- ADS 报表明细：报送报表的行项目下钻，供核对与追溯用。
-- 与汇总层的关系：汇总层出"每个 Section 一行"，本表出"每个维度组合一行"，
-- 前者必须等于后者对同一 section 的求和（VDQ-013 校验的就是这个）。
--
-- is_intracompany 标记：明细回溯核对用这一列把抵销项排除。
-- 合并口径只汇总 is_intracompany = false 的行，这一列让核对脚本能独立复算。

with deposit_detail as (

    select
        report_date,
        entity_code,
        is_intracompany,
        'C' as section_code,
        'C-DEP' as line_item,
        '存款余额' as line_description,
        product_category,
        customer_segment as counterparty_type,
        currency_code,
        maturity_bucket,
        round(sum(principal_amount_usd), 2) as outstanding_amount,
        cast(0 as decimal(20, 2)) as inflow_amount,
        cast(0 as decimal(20, 2)) as outflow_amount,
        round(sum(principal_amount_usd), 2) as net_amount,
        cast(0 as decimal(20, 2)) as market_value
    from {{ ref('owd_deposits') }}
    group by report_date, entity_code, is_intracompany, product_category, customer_segment, currency_code, maturity_bucket

),

secured_detail as (

    select
        report_date,
        entity_code,
        is_intracompany,
        'B' as section_code,
        -- 回购与逆回购分开列示：正回购是融资（负债），逆回购是资金运用（资产），
        -- 报表 Section B 只统计正回购，混在一起会让明细与报表合计对不上。
        case when transaction_type = 'REPO' then 'B-REPO' else 'B-REVERSE' end as line_item,
        '有担保融资余额' as line_description,
        transaction_type as product_category,
        counterparty_type,
        currency_code,
        maturity_bucket,
        round(sum(cash_amount_usd), 2) as outstanding_amount,
        -- 方向要落在正确的列上：正回购是融资（钱进来、未来要还）→ 流出；逆回购是资金运用
        -- （钱出去、未来收回）→ 流入。两笔都记在 outflow 上，下游必须靠 line_item 过滤才
        -- 说得通 —— 那是把口径责任推给消费者，多一个消费者就多一次踩错的机会。
        round(sum(case when transaction_type = 'REPO' then 0 else cash_amount_usd end), 2) as inflow_amount,
        round(sum(case when transaction_type = 'REPO' then cash_amount_usd else 0 end), 2) as outflow_amount,
        round(sum(cash_amount_usd), 2) as net_amount,
        cast(0 as decimal(20, 2)) as market_value
    from {{ ref('owd_secured_financing') }}
    group by report_date, entity_code, is_intracompany, transaction_type, counterparty_type, currency_code, maturity_bucket

),

loan_detail as (

    select
        report_date,
        entity_code,
        is_intracompany,
        'F' as section_code,
        'F-LOAN' as line_item,
        '贷款到期流入' as line_description,
        loan_type as product_category,
        borrower_type as counterparty_type,
        currency_code,
        maturity_bucket,
        round(sum(outstanding_usd), 2) as outstanding_amount,
        round(sum(outstanding_usd), 2) as inflow_amount,
        cast(0 as decimal(20, 2)) as outflow_amount,
        round(sum(outstanding_usd), 2) as net_amount,
        cast(0 as decimal(20, 2)) as market_value
    from {{ ref('owd_loans') }}
    -- 与报表口径保持一致：Section F 只统计 30 天内到期的贷款本金，窗口有下界
    -- （已过到期日的贷款不算未来 30 天的流入，见报表模型同一处的说明）
    where days_to_maturity between 0 and 30
    group by report_date, entity_code, is_intracompany, loan_type, borrower_type, currency_code, maturity_bucket

),

hqla_detail as (

    select
        report_date,
        entity_code,
        is_intracompany,
        'G' as section_code,
        'G-HQLA' as line_item,
        '优质流动性资产构成' as line_description,
        security_type as product_category,
        issuer_type as counterparty_type,
        currency_code,
        maturity_bucket,
        cast(0 as decimal(20, 2)) as outstanding_amount,
        cast(0 as decimal(20, 2)) as inflow_amount,
        cast(0 as decimal(20, 2)) as outflow_amount,
        round(sum(market_value_usd * (1 - hqla_haircut_rate)), 2) as net_amount,
        round(sum(market_value_usd), 2) as market_value
    from {{ ref('owd_securities') }}
    group by report_date, entity_code, is_intracompany, security_type, issuer_type, currency_code, maturity_bucket

),

contingent_detail as (

    select
        report_date,
        entity_code,
        is_intracompany,
        'J' as section_code,
        'J-OFFBS' as line_item,
        '表外或有负债' as line_description,
        commitment_type as product_category,
        counterparty_type,
        currency_code,
        maturity_bucket,
        round(sum(undrawn_amount_usd), 2) as outstanding_amount,
        cast(0 as decimal(20, 2)) as inflow_amount,
        round(sum(undrawn_amount_usd), 2) as outflow_amount,
        round(sum(undrawn_amount_usd), 2) as net_amount,
        cast(0 as decimal(20, 2)) as market_value
    from {{ ref('owd_off_bs') }}
    group by report_date, entity_code, is_intracompany, commitment_type, counterparty_type, currency_code, maturity_bucket

)

select * from deposit_detail
union all
select * from secured_detail
union all
select * from loan_detail
union all
select * from hqla_detail
union all
select * from contingent_detail
