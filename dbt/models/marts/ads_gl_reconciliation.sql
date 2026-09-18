-- ADS GL 对账：独立基准与 FR 2052a 报送口径逐 Section 比对。
--
-- 为什么按 Section 而不是按单个科目比：报送口径的一个 Section 由多个总账科目构成
-- （Section C 存款 = 2001 活期 + 2002 定期），按单科目比永远对不平。
-- 因此本表先按 Section 汇总基准金额，再与对应口径比对；gl_account_id 列记录该
-- Section 由哪些科目构成，benchmark_source 列记录基准取自哪里。
--
-- 基准侧（benchmark_amount）分两类，这是本表最关键的一处设计：
--   总账口径（benchmark_source = GL）：报送侧由业务明细汇总、基准侧由总账余额汇总，
--   两侧分别从原始数据算出，不是同一个数抄两遍。
--   Section E 现金：基准侧取司库现金头寸（benchmark_source = TREASURY_CASH_POSITION），
--   即银行对账单余额与库存现金盘点数，属于本行外部可见的事实。
--   为什么不沿用总账：报送侧的现金本身就出自总账 1001/1100，两侧同源等于自己跟自己比，
--   差异恒为零、任何错误都查不出来（审计点名的缺陷形态）。换成司库口径后两侧是两个来源，
--   差额由在途存款与未兑现支票逐项解释（见 reconciling_item_usd）。
--
-- 三档视角对账说明：
--   法人实体单体行（entity_code <> 'GRP001'）：该实体自己的基准 vs 该实体的报表行。
--   两侧都含内部往来，都不剔除 —— 单体口径下与子公司的应收应付是真实头寸。
--
--   全球合并行（entity_code = 'GRP001'）：Σ 各实体基准 − 集团内往来 vs 合并报表行。
--   两侧一致地剔除集团内往来：基准侧减去内部往来的存款腿（科目 2001/2002）与贷款腿
--   （科目 2100），报表侧只取 is_intracompany = false 的行。不是只改一边。
--
-- 口径说明：
--   总账按借贷方向记账，负债权益类科目余额在贷方（净额为负），报送口径一律取正数，
--   因此比对时对总账取绝对值。
--   调节后差异 = 基准金额 + 调节项 − 报送金额，落在容差内判 PASS，否则 FAIL —— FAIL 会阻断报送。
--   容差分两档：
--     总账口径基准：报送金额的 1%，且不少于 1 分钱（沿用既有口径）。
--     对账单口径基准（Section E）：1 分钱。差额已被调节项逐项解释，再留 1% 的余量等于
--     放行几十万的错误 —— 这正是「对账查不出错」的另一半原因。
--
--   对账按报告期逐期独立进行：每个 report_date 的基准与报表口径各自按期汇总，
--   再按 section_code 和 report_date 两边对上。多期数据共存时不会把不同期的金额混到一起。
--   口径由 entity_code 推出（GRP001 即合并），不另加口径码列。

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

-- Section E 的科目构成串：基准侧走司库头寸、报送侧走总账 1001/1100，
-- 两侧共用同一套构成定义，因此只在这里声明一次，供下面的分支引用。
section_e_accounts as (

    select
        max(m.comparison_basis) as comparison_basis,
        array_join(sort_array(collect_set(m.gl_account_id)), '+') as account_group
    from account_section m
    where m.section_code = 'E'

),

-- 集团内往来金额：从 silver 明细里 is_intracompany = true 的行算出。
-- 存款腿 → Section C 对应的科目（2001/2002）；
-- 放款腿 → Section F 对应的科目（2100）。
-- 两侧一致剔除，不是只改一边。
intracompany_amounts as (

    -- 存款腿：母公司账上的集团内存款（子公司在母公司的存款）
    select
        report_date,
        'C' as section_code,
        round(sum(principal_amount_usd), 2) as intracompany_usd
    from {{ ref('owd_deposits') }}
    where is_intracompany
    group by report_date

    union all

    -- 贷款腿：子公司账上的集团内放款（母公司对子公司的放款）
    select
        report_date,
        'F' as section_code,
        round(sum(outstanding_usd), 2) as intracompany_usd
    from {{ ref('owd_loans') }}
    where is_intracompany
    group by report_date

),

-- 基准金额：非现金 Section 取总账科目余额；Section E 取司库现金头寸（对账单 + 盘点口径）。
-- Section E 被显式排除在总账分支之外（where m.section_code <> 'E'）—— 它必须走独立来源，
-- 否则两侧同源、差异恒为零，对账退化成自证。
benchmark_amounts as (

    -- 总账口径：按 (报告期, 实体, Section) 汇总科目余额
    select
        g.report_date,
        g.entity_code,
        m.section_code,
        max(m.comparison_basis) as comparison_basis,
        -- 一个 Section 可能由多个科目构成，列出构成便于人工追查
        array_join(sort_array(collect_set(g.gl_account_id)), '+') as benchmark_reference,
        'GL' as benchmark_source,
        round(abs(sum(g.net_balance_usd)), 2) as benchmark_amount
    from {{ ref('owd_gl_entries') }} g
    join account_section m
        on m.gl_account_id = g.gl_account_id
    where m.section_code <> 'E'
    group by g.report_date, g.entity_code, m.section_code

    union all

    -- 司库口径：Section E 的银行对账单余额与库存现金盘点数
    select
        p.report_date,
        p.entity_code,
        'E' as section_code,
        e.comparison_basis,
        e.account_group as benchmark_reference,
        'TREASURY_CASH_POSITION' as benchmark_source,
        round(sum(p.balance_usd), 2) as benchmark_amount
    from {{ ref('owd_treasury_cash_position') }} p
    cross join section_e_accounts e
    group by p.report_date, p.entity_code, e.comparison_basis, e.account_group

),

-- 可解释调节项：把基准口径调整到账面口径所需的净额。
-- 只有 Section E 有：在途存款（账面已记、对账单未到）加回，未兑现支票（账面已扣、
-- 对账单未扣）扣回。其余 Section 记 0，于是判定公式对所有 Section 统一：
--     调节后差异 = 基准金额 + 调节项 − 报送金额
reconciling_items as (

    select
        p.report_date,
        p.entity_code,
        'E' as section_code,
        round(sum(p.in_transit_deposits_usd - p.outstanding_checks_usd), 2) as reconciling_item_usd
    from {{ ref('owd_treasury_cash_position') }} p
    group by p.report_date, p.entity_code

    union all

    -- 合并行：各实体现金头寸调节项之和。现金不涉及集团内往来，与合并基准同口径。
    select
        p.report_date,
        'GRP001' as entity_code,
        'E' as section_code,
        round(sum(p.in_transit_deposits_usd - p.outstanding_checks_usd), 2) as reconciling_item_usd
    from {{ ref('owd_treasury_cash_position') }} p
    group by p.report_date

),

report_amounts as (

    -- 报表口径按 (report_date, entity_code, is_intracompany) 分组取数。
    -- 法人实体单体行汇总两种标记的行；合并行只取 is_intracompany = false。
    select
        report_date,
        entity_code,
        'C' as section_code,
        is_intracompany,
        round(sum(principal_amount_usd), 2) as report_amount
    from {{ ref('owd_deposits') }}
    group by report_date, entity_code, is_intracompany

    union all

    select
        report_date,
        entity_code,
        'B' as section_code,
        is_intracompany,
        round(sum(case when transaction_type = 'REPO' then cash_amount_usd else 0 end), 2) as report_amount
    from {{ ref('owd_secured_financing') }}
    group by report_date, entity_code, is_intracompany

    union all

    select
        report_date,
        entity_code,
        'B2' as section_code,
        is_intracompany,
        round(sum(case when transaction_type = 'REVERSE_REPO' then cash_amount_usd else 0 end), 2) as report_amount
    from {{ ref('owd_secured_financing') }}
    group by report_date, entity_code, is_intracompany

    union all

    select
        report_date,
        entity_code,
        'E' as section_code,
        cast(false as boolean) as is_intracompany,
        round(sum(net_balance_usd), 2) as report_amount
    from {{ ref('owd_gl_entries') }}
    where gl_account_id in ('1001', '1100')
    group by report_date, entity_code

    union all

    select
        report_date,
        entity_code,
        'G' as section_code,
        is_intracompany,
        round(sum(market_value_usd), 2) as report_amount
    from {{ ref('owd_securities') }}
    group by report_date, entity_code, is_intracompany

    union all

    select
        report_date,
        entity_code,
        'F' as section_code,
        is_intracompany,
        round(sum(outstanding_usd), 2) as report_amount
    from {{ ref('owd_loans') }}
    group by report_date, entity_code, is_intracompany

    union all

    select
        report_date,
        entity_code,
        'H' as section_code,
        is_intracompany,
        round(sum(case when mtm_value_usd > 0 then mtm_value_usd else 0 end), 2) as report_amount
    from {{ ref('owd_derivatives') }}
    group by report_date, entity_code, is_intracompany

    union all

    select
        report_date,
        entity_code,
        'H2' as section_code,
        is_intracompany,
        round(-sum(case when mtm_value_usd < 0 then mtm_value_usd else 0 end), 2) as report_amount
    from {{ ref('owd_derivatives') }}
    group by report_date, entity_code, is_intracompany

),

-- 法人实体单体对账：该实体自己的基准 vs 该实体的报表行（两种标记都含，不剔除）
entity_recon as (

    select
        c.report_date,
        c.entity_code,
        c.section_code,
        c.gl_account_id,
        c.account_name,
        c.benchmark_amount,
        c.benchmark_source,
        c.reconciling_item_usd,
        c.fr2052a_amount,
        c.variance,
        case when abs(c.variance) <= c.tolerance_amount then 'PASS' else 'FAIL' end as status
    from (
        select
            b.report_date,
            b.entity_code,
            b.section_code,
            b.benchmark_reference as gl_account_id,
            b.comparison_basis as account_name,
            b.benchmark_amount,
            b.benchmark_source,
            coalesce(rc.reconciling_item_usd, 0) as reconciling_item_usd,
            coalesce(r.report_amount, 0) as fr2052a_amount,
            round(b.benchmark_amount + coalesce(rc.reconciling_item_usd, 0) - coalesce(r.report_amount, 0), 2) as variance,
            -- 容差：总账口径基准沿用「报送金额的 1%，且不少于 1 分钱」；
            -- 对账单口径基准只留 1 分钱 —— 差额已被调节项逐项解释，残留即是错。
            case
                when b.benchmark_source = 'GL' then greatest(0.01, 0.01 * coalesce(r.report_amount, 0))
                else 0.01
            end as tolerance_amount
        from benchmark_amounts b
        left join (
            -- 法人实体单体：汇总该实体两种标记的行（不剔除内部往来）
            select report_date, entity_code, section_code, round(sum(report_amount), 2) as report_amount
            from report_amounts
            group by report_date, entity_code, section_code
        ) r
            on r.section_code = b.section_code
            and r.entity_code = b.entity_code
            and r.report_date = b.report_date
        left join reconciling_items rc
            on rc.section_code = b.section_code
            and rc.entity_code = b.entity_code
            and rc.report_date = b.report_date
    ) c

),

-- 合并行对账：Σ 各实体基准 − 集团内往来 vs 合并报表行（只取 is_intracompany = false）
consolidated_recon as (

    select
        c.report_date,
        c.entity_code,
        c.section_code,
        c.gl_account_id,
        c.account_name,
        c.benchmark_amount,
        c.benchmark_source,
        c.reconciling_item_usd,
        c.fr2052a_amount,
        c.variance,
        case when abs(c.variance) <= c.tolerance_amount then 'PASS' else 'FAIL' end as status
    from (
        select
            b.report_date,
            'GRP001' as entity_code,
            b.section_code,
            b.benchmark_reference as gl_account_id,
            b.comparison_basis as account_name,
            -- 合并口径的基准金额 = Σ 各实体基准 − 集团内往来。
            -- 减法放在这一层做：intracompany_amounts 是「每期每 Section 一行」，
            -- 若放进下面那个带 GROUP BY 的子查询里减，它会随实体行被重复减 N 次。
            round(b.benchmark_amount - coalesce(ic.intracompany_usd, 0), 2) as benchmark_amount,
            b.benchmark_source,
            coalesce(rc.reconciling_item_usd, 0) as reconciling_item_usd,
            coalesce(r.report_amount, 0) as fr2052a_amount,
            round(b.benchmark_amount - coalesce(ic.intracompany_usd, 0) + coalesce(rc.reconciling_item_usd, 0)
                - coalesce(r.report_amount, 0), 2) as variance,
            case
                when b.benchmark_source = 'GL' then greatest(0.01, 0.01 * coalesce(r.report_amount, 0))
                else 0.01
            end as tolerance_amount
        from (
            -- 合并基准：按 (报告期, Section) 汇总各实体的基准金额
            select
                b.report_date,
                b.section_code,
                max(b.comparison_basis) as comparison_basis,
                array_join(sort_array(collect_set(b.benchmark_reference)), '+') as benchmark_reference,
                max(b.benchmark_source) as benchmark_source,
                round(sum(b.benchmark_amount), 2) as benchmark_amount
            from benchmark_amounts b
            group by b.report_date, b.section_code
        ) b
        -- 集团内往来：每期每 Section 一行，直接 join 不会行放大
        left join intracompany_amounts ic
            on ic.report_date = b.report_date
            and ic.section_code = b.section_code
        left join reconciling_items rc
            on rc.report_date = b.report_date
            and rc.section_code = b.section_code
            and rc.entity_code = 'GRP001'
        left join (
            -- 合并报表行：只取 is_intracompany = false 的行
            select report_date, section_code, round(sum(report_amount), 2) as report_amount
            from report_amounts
            where not is_intracompany
            group by report_date, section_code
        ) r
            on r.section_code = b.section_code
            and r.report_date = b.report_date
    ) c

)

select * from entity_recon
union all
select * from consolidated_recon
