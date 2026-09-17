-- ADS GL 对账：总账科目余额与 FR 2052a 报送口径逐 Section 比对。
--
-- 为什么按 Section 而不是按单个科目比：报送口径的一个 Section 由多个总账科目构成
-- （Section C 存款 = 2001 活期 + 2002 定期），按单科目比永远对不平。
-- 因此本表先按 Section 汇总总账余额，再与对应口径比对；gl_account_id 列记录该
-- Section 由哪些科目构成。
--
-- 口径说明：
--   总账按借贷方向记账，负债权益类科目余额在贷方（净额为负），报送口径一律取正数，
--   因此比对时对总账取绝对值。
--   差异在容差内（报送金额的 1%，且不少于 1 分钱）判 PASS，否则 FAIL —— FAIL 会阻断报送。

with account_section as (

    select '2001' as gl_account_id, 'C' as section_code, '存款余额' as comparison_basis
    union all select '2002', 'C', '存款余额'
    union all select '2010', 'B', '正回购余额'
    union all select '1300', 'B2', '逆回购余额'
    union all select '1001', 'E', '现金头寸'
    union all select '1100', 'E', '现金头寸'
    union all select '1200', 'G', '证券市值'
    union all select '1500', 'H', '衍生品净盯市资产'
    union all select '2200', 'H2', '衍生品净盯市负债'
    union all select '2100', 'F', '贷款账面余额'

),

gl_by_section as (

    select
        m.section_code,
        max(m.comparison_basis) as comparison_basis,
        -- 一个 Section 可能由多个科目构成，列出构成便于人工追查
        array_join(sort_array(collect_set(g.gl_account_id)), '+') as gl_account_group,
        round(abs(sum(g.net_balance_usd)), 2) as gl_amount
    from {{ ref('owd_gl_entries') }} g
    join account_section m
        on m.gl_account_id = g.gl_account_id
    group by m.section_code

),

report_amounts as (

    -- 报送口径按集团（ENT001）汇总取数：总账本身就是集团口径
    select
        'C' as section_code,
        round(sum(principal_amount_usd), 2) as report_amount
    from {{ ref('owd_deposits') }}

    union all

    select
        'B' as section_code,
        round(sum(case when transaction_type = 'REPO' then cash_amount_usd else 0 end), 2) as report_amount
    from {{ ref('owd_secured_financing') }}

    union all

    select
        'B2' as section_code,
        round(sum(case when transaction_type = 'REVERSE_REPO' then cash_amount_usd else 0 end), 2) as report_amount
    from {{ ref('owd_secured_financing') }}

    union all

    select
        'E' as section_code,
        round(sum(net_balance_usd), 2) as report_amount
    from {{ ref('owd_gl_entries') }}
    where gl_account_id in ('1001', '1100')

    union all

    select
        'G' as section_code,
        round(sum(market_value_usd), 2) as report_amount
    from {{ ref('owd_securities') }}

    union all

    select
        'F' as section_code,
        round(sum(outstanding_usd), 2) as report_amount
    from {{ ref('owd_loans') }}

    union all

    select
        'H' as section_code,
        round(sum(case when mtm_value_usd > 0 then mtm_value_usd else 0 end), 2) as report_amount
    from {{ ref('owd_derivatives') }}

    union all

    select
        'H2' as section_code,
        round(-sum(case when mtm_value_usd < 0 then mtm_value_usd else 0 end), 2) as report_amount
    from {{ ref('owd_derivatives') }}

),

-- 报告日取数据自身的报告日，不用 current_date()：
-- 对账结果由熔断判定按报告日查询（liquidity_monitor 传 --report-date），
-- 用处理日打标会让两边日期对不上，查询永远查不到行 —— 于是对账失败也报不出预警，
-- 报送闸照旧放行。这条静默失效只能在「故意造一个缺口」时才暴露。
report_date as (

    select max(report_date) as report_date
    from {{ ref('owd_gl_entries') }}

)

select
    d.report_date,
    g.section_code,
    g.gl_account_group as gl_account_id,
    g.comparison_basis as account_name,
    g.gl_amount,
    coalesce(r.report_amount, 0) as fr2052a_amount,
    round(g.gl_amount - coalesce(r.report_amount, 0), 2) as variance,
    -- 容差：报送金额的 1%，且不少于 1 分钱
    case
        when abs(round(g.gl_amount - coalesce(r.report_amount, 0), 2))
            <= greatest(0.01, 0.01 * coalesce(r.report_amount, 0))
        then 'PASS'
        else 'FAIL'
    end as status

from gl_by_section g
cross join report_date d
left join report_amounts r
    on r.section_code = g.section_code
