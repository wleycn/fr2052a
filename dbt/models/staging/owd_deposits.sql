-- OWD 存款：源系统明细标准化。
-- 加工内容：外币折算 USD、客户与产品口径归一、行为分桶、受保金额按存款保险上限截断。

with deposits as (

    select * from {{ source('bronze', 'ods_deposits') }}

),

ref_cp as (

    select counterparty_id, counterparty_type from {{ source('ref', 'ref_counterparty') }}

),

fx as (

    select * from {{ ref('stg_fx_rates') }}

),

normalized as (

    select
        d.source_system,
        d.source_record_id,
        d.report_date,
        d.entity_code,
        {{ mask_pii('d.account_number') }} as deposit_id,
        {{ mask_pii('d.customer_id') }} as customer_id,
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
        {{ behavioral_bucket(deposit_product_category('d.deposit_type'), 'datediff(d.maturity_date, d.report_date)') }} as maturity_bucket,
        case when d.insured_flag = 'Y' then true else false end as is_insured,
        -- 受保金额的折算口径：存款保险上限是美元限额，必须先折算 USD 再截断。
        -- 在原币上截断会让外币存款量级错误 —— 例如 JPY 的 3,000 万存款会被先砍到 250,000
        -- 再乘汇率，受保额变成约 1,675 美元（真实值差两个量级）。这里只算「可受保金额」，
        -- 限额截断放到下一段按客户聚合后做。
        case
            when d.insured_flag = 'Y' then round(d.principal_amount * f.spot_rate, 2)
            else 0
        end as insured_eligible_usd,
        d.branch_code,
        {{ is_affiliate_counterparty('rc.counterparty_type') }} as is_intracompany,
        d.event_time,
        d.etl_batch_id
    from deposits d
    left join ref_cp rc
        on rc.counterparty_id = d.customer_id
    left join fx f
        on f.currency_code = d.currency
        and f.rate_date = d.report_date

),

insured_capped as (

    -- 存款保险限额按「客户 × 法人实体」聚合后截断：同一客户在同一家银行的多笔存款
    -- 合计受保，逐笔按限额截断等于把分开存放的多笔存款各自给足额度，整体高估。
    -- 客户的受保总额超限时，各笔按自身占该客户受保总额的比例等比缩到限额
    -- （分摊规则写在这里，不留给下游猜）。
    select
        n.*,
        sum(n.insured_eligible_usd) over (
            partition by n.report_date, n.entity_code, n.customer_id
        ) as customer_insured_eligible_usd
    from normalized n

)

select
    g.source_system,
    g.source_record_id,
    g.report_date,
    g.entity_code,
    g.deposit_id,
    g.customer_id,
    g.customer_segment,
    g.customer_type,
    g.product_category,
    g.deposit_type,
    g.currency_code,
    g.principal_amount_lc,
    g.principal_amount_usd,
    g.accrued_interest_lc,
    g.accrued_interest_usd,
    g.exchange_rate,
    g.interest_rate,
    g.open_date,
    g.maturity_date,
    g.days_to_maturity,
    g.maturity_bucket,
    g.is_insured,
    round(
        case
            when g.insured_eligible_usd = 0 then 0
            else g.insured_eligible_usd
                 * least(g.customer_insured_eligible_usd, {{ var('deposit_insurance_limit_usd') }})
                 / g.customer_insured_eligible_usd
        end,
        2
    ) as insured_amount_usd,
    g.branch_code,
    g.is_intracompany,
    g.event_time,
    g.etl_batch_id

from insured_capped g
