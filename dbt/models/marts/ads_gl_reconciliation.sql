-- ADS GL 对账：总账科目余额与 FR 2052a 报送口径逐 Section 比对。
--
-- 为什么按 Section 而不是按单个科目比：报送口径的一个 Section 由多个总账科目构成
-- （Section C 存款 = 2001 活期 + 2002 定期），按单科目比永远对不平。
-- 因此本表先按 Section 汇总总账余额，再与对应口径比对；gl_account_id 列记录该
-- Section 由哪些科目构成。
--
-- 三档视角对账说明：
--   法人实体单体行（entity_code <> 'GRP001'）：该实体自己的总账 vs 该实体的报表行。
--   两侧都含内部往来，都不剔除 —— 单体口径下与子公司的应收应付是真实头寸。
--
--   全球合并行（entity_code = 'GRP001'）：Σ 各实体总账 − 集团内往来 vs 合并报表行。
--   两侧一致地剔除集团内往来：总账侧减去内部往来的存款腿（科目 2001/2002）与贷款腿
--   （科目 2100），报表侧只取 is_intracompany = false 的行。不是只改一边。
--
-- 口径说明：
--   总账按借贷方向记账，负债权益类科目余额在贷方（净额为负），报送口径一律取正数，
--   因此比对时对总账取绝对值。
--   差异在容差内（报送金额的 1%，且不少于 1 分钱）判 PASS，否则 FAIL —— FAIL 会阻断报送。
--
--   对账按报告期逐期独立进行：每个 report_date 的总账与报表口径各自按期汇总，
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

gl_by_section_entity as (

    -- 总账按法人实体各记一本账：按 (report_date, entity_code, section_code) 汇总
    select
        g.report_date,
        g.entity_code,
        m.section_code,
        max(m.comparison_basis) as comparison_basis,
        -- 一个 Section 可能由多个科目构成，列出构成便于人工追查
        array_join(sort_array(collect_set(g.gl_account_id)), '+') as gl_account_group,
        round(abs(sum(g.net_balance_usd)), 2) as gl_amount
    from {{ ref('owd_gl_entries') }} g
    join account_section m
        on m.gl_account_id = g.gl_account_id
    group by g.report_date, g.entity_code, m.section_code

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

-- 法人实体单体对账：该实体自己的总账 vs 该实体的报表行（两种标记都含，不剔除）
entity_recon as (

    select
        g.report_date,
        g.entity_code,
        g.section_code,
        g.gl_account_group as gl_account_id,
        g.comparison_basis as account_name,
        g.gl_amount,
        coalesce(r.report_amount, 0) as fr2052a_amount,
        round(g.gl_amount - coalesce(r.report_amount, 0), 2) as variance,
        case
            when abs(round(g.gl_amount - coalesce(r.report_amount, 0), 2))
                <= greatest(0.01, 0.01 * coalesce(r.report_amount, 0))
            then 'PASS'
            else 'FAIL'
        end as status
    from gl_by_section_entity g
    left join (
        -- 法人实体单体：汇总该实体两种标记的行（不剔除内部往来）
        select report_date, entity_code, section_code, round(sum(report_amount), 2) as report_amount
        from report_amounts
        group by report_date, entity_code, section_code
    ) r
        on r.section_code = g.section_code
        and r.entity_code = g.entity_code
        and r.report_date = g.report_date

),

-- 合并行对账：Σ 各实体总账 − 集团内往来 vs 合并报表行（只取 is_intracompany = false）
consolidated_recon as (

    select
        g.report_date,
        'GRP001' as entity_code,
        g.section_code,
        g.gl_account_group as gl_account_id,
        g.comparison_basis as account_name,
        -- 合并口径的总账金额 = Σ 各实体总账 − 集团内往来。
        -- 减法放在这一层做：intracompany_amounts 是「每期每 Section 一行」，
        -- 若放进下面那个带 GROUP BY 的子查询里减，它会随实体行被重复减 N 次。
        round(g.gl_amount - coalesce(ic.intracompany_usd, 0), 2) as gl_amount,
        coalesce(r.report_amount, 0) as fr2052a_amount,
        round(g.gl_amount - coalesce(ic.intracompany_usd, 0) - coalesce(r.report_amount, 0), 2) as variance,
        case
            when abs(round(g.gl_amount - coalesce(ic.intracompany_usd, 0) - coalesce(r.report_amount, 0), 2))
                <= greatest(0.01, 0.01 * coalesce(r.report_amount, 0))
            then 'PASS'
            else 'FAIL'
        end as status
    from (
        -- 合并总账：按 (报告期, Section) 汇总各实体总账
        select
            g.report_date,
            g.section_code,
            max(g.comparison_basis) as comparison_basis,
            array_join(sort_array(collect_set(g.gl_account_group)), '+') as gl_account_group,
            round(sum(g.gl_amount), 2) as gl_amount
        from gl_by_section_entity g
        group by g.report_date, g.section_code
    ) g
    -- 集团内往来：每期每 Section 一行，直接 join 不会行放大
    left join intracompany_amounts ic
        on ic.report_date = g.report_date
        and ic.section_code = g.section_code
    left join (
        -- 合并报表行：只取 is_intracompany = false 的行
        select report_date, section_code, round(sum(report_amount), 2) as report_amount
        from report_amounts
        where not is_intracompany
        group by report_date, section_code
    ) r
        on r.section_code = g.section_code
        and r.report_date = g.report_date

)

select * from entity_recon
union all
select * from consolidated_recon
