-- ADS FR 2052a 报送报表：按 Section A–K 装配行项目金额。
--
-- 口径说明：
--   每个法人实体一行（is_consolidated = false），外加一行集团合并口径（ENT001，is_consolidated = true）。
--   合并口径由实体口径相加得到，不重复计算。
--
--   没有数据来源的 Section 字段留 NULL，不写 0 —— 0 意味着"查过且确实为零"，
--   而本演示的 ODS 里根本没有这些业务（商业票据、联邦基金、证券借贷、经纪存款），
--   置 NULL 才是诚实的表达。
--
--   现金流上限按 FR 2052a 规则应用：30 天内的预期流入最高只认流出的 75%（VDQ-018）。

with deposits as (

    select
        entity_code,
        round(sum(principal_amount_usd), 2) as total_deposits_usd,
        round(sum(case when customer_segment = 'RETAIL' and product_category = 'DEMAND' then principal_amount_usd else 0 end), 2) as retail_demand,
        round(sum(case when customer_segment = 'RETAIL' and product_category = 'SAVINGS' then principal_amount_usd else 0 end), 2) as retail_savings,
        round(sum(case when customer_segment = 'RETAIL' and product_category in ('TIME', 'CD') then principal_amount_usd else 0 end), 2) as retail_time,
        round(sum(case when customer_segment <> 'RETAIL' and product_category = 'DEMAND' then principal_amount_usd else 0 end), 2) as wholesale_demand,
        round(sum(case when customer_segment <> 'RETAIL' and product_category in ('TIME', 'CD') then principal_amount_usd else 0 end), 2) as wholesale_time
    from {{ ref('owd_deposits') }}
    group by entity_code

),

secured_financing as (

    select
        entity_code,
        round(sum(case when transaction_type = 'REPO' then cash_amount_usd else 0 end), 2) as repo_outstanding,
        round(sum(case when transaction_type = 'REVERSE_REPO' then cash_amount_usd else 0 end), 2) as reverse_repo
    from {{ ref('owd_secured_financing') }}
    group by entity_code

),

cash as (

    select
        entity_code,
        round(sum(cash_on_hand_usd), 2) as cash_on_hand,
        round(sum(total_cash_usd), 2) as total_cash
    from {{ ref('ows_cash_position') }}
    group by entity_code

),

loan_inflows as (

    -- 30 天内到期的贷款本金构成预期流入
    select
        entity_code,
        round(sum(case when loan_type = 'COMMERCIAL' then outstanding_usd else 0 end), 2) as commercial_inflow,
        round(sum(case when loan_type = 'RETAIL' then outstanding_usd else 0 end), 2) as retail_inflow,
        round(sum(case when loan_type = 'MORTGAGE' then outstanding_usd else 0 end), 2) as mortgage_inflow,
        round(sum(outstanding_usd), 2) as total_inflow
    from {{ ref('owd_loans') }}
    where days_to_maturity <= 30
    group by entity_code

),

hqla as (

    select
        entity_code,
        round(sum(case when hqla_classification = 'LEVEL_1' then market_value_usd else 0 end), 2) as l1_mv,
        round(sum(case when hqla_classification = 'LEVEL_2A' then market_value_usd else 0 end), 2) as l2a_mv,
        round(sum(case when hqla_classification = 'LEVEL_2B' then market_value_usd else 0 end), 2) as l2b_mv,
        round(sum(case when hqla_classification = 'NON_HQLA' then market_value_usd else 0 end), 2) as non_hqla_mv,
        round(sum(market_value_usd), 2) as total_mv,
        round(sum(case when hqla_classification = 'LEVEL_1' and not is_encumbered then market_value_usd else 0 end), 2) as unencumbered_l1,
        round(sum(case when hqla_classification = 'LEVEL_2A' and not is_encumbered then market_value_usd else 0 end), 2) as unencumbered_l2a,
        round(sum(case when hqla_classification = 'LEVEL_2B' and not is_encumbered then market_value_usd else 0 end), 2) as unencumbered_l2b,
        round(sum(case when hqla_classification = 'NON_HQLA' and not is_encumbered then market_value_usd else 0 end), 2) as unencumbered_non_hqla,
        round(sum(case when is_encumbered then market_value_usd else 0 end), 2) as encumbered_total
    from {{ ref('owd_securities') }}
    group by entity_code

),

derivatives as (

    -- 用原始盯市口径而非抵押品净额口径：FR 2052a Section H 报的是净盯市，
    -- 且这样才与总账的衍生品资产负债科目（1500/2200）对得上，GL 对账才能成立。
    select
        entity_code,
        round(sum(case when mtm_value_usd > 0 then mtm_value_usd else 0 end), 2) as net_mtm_asset,
        round(-sum(case when mtm_value_usd < 0 then mtm_value_usd else 0 end), 2) as net_mtm_liability,
        round(sum(collateral_posted_usd), 2) as collateral_posted,
        round(sum(collateral_received_usd), 2) as collateral_received
    from {{ ref('owd_derivatives') }}
    group by entity_code

),

contingent as (

    select
        entity_code,
        round(sum(case when commitment_type = 'CREDIT_COMMITMENT' then undrawn_amount_usd else 0 end), 2) as credit_commitments,
        round(sum(case when commitment_type = 'LETTER_OF_CREDIT' then undrawn_amount_usd else 0 end), 2) as letters_of_credit,
        round(sum(case when commitment_type = 'GUARANTEE' then undrawn_amount_usd else 0 end), 2) as guarantees,
        round(sum(undrawn_amount_usd), 2) as total_contingent
    from {{ ref('owd_off_bs') }}
    group by entity_code

),

cashflow_30d as (

    -- 30 天内到期的现金流，含 FR 2052a 的流入上限规则
    select
        entity_code,
        round(sum(expected_inflow_usd), 2) as raw_inflow,
        round(sum(expected_outflow_usd), 2) as total_outflow
    from {{ ref('ows_cashflow_projection') }}
    where maturity_bucket in ('O/N', '1-7D', '8-30D')
    group by entity_code

),

entities as (

    select distinct report_date, entity_code from {{ ref('owd_deposits') }}

),

entity_level as (

    select
        e.entity_code,
        -- Section A：无担保融资。演示环境没有商业票据、联邦基金等业务，留空。
        cast(null as decimal(20, 2)) as sec_a_cp_outstanding,
        cast(null as decimal(20, 2)) as sec_a_cd_outstanding,
        cast(null as decimal(20, 2)) as sec_a_unsecured_borrow,
        cast(null as decimal(20, 2)) as sec_a_fed_funds,
        cast(null as decimal(20, 2)) as sec_a_total,
        -- Section B：有担保融资
        f.repo_outstanding as sec_b_repo_outstanding,
        f.reverse_repo as sec_b_reverse_repo,
        cast(null as decimal(20, 2)) as sec_b_sec_lending,
        cast(null as decimal(20, 2)) as sec_b_fhlb_advances,
        coalesce(f.repo_outstanding, 0) as sec_b_total,
        -- Section C：存款
        d.retail_demand as sec_c_retail_demand,
        d.retail_savings as sec_c_retail_savings,
        d.retail_time as sec_c_retail_time,
        d.wholesale_demand as sec_c_wholesale_demand,
        d.wholesale_time as sec_c_wholesale_time,
        cast(null as decimal(20, 2)) as sec_c_brokered,
        d.total_deposits_usd as sec_c_total,
        -- Section D：其他融资，演示环境无数据
        cast(null as decimal(20, 2)) as sec_d_total,
        -- Section E：现金
        c.total_cash as sec_e_cash_total,
        c.cash_on_hand as sec_e_central_bank_dep,
        c.total_cash as sec_e_cash_equiv_total,
        -- Section F：贷款流入（30 天内到期）
        coalesce(l.commercial_inflow, 0) as sec_f_commercial_inflow,
        coalesce(l.retail_inflow, 0) as sec_f_retail_inflow,
        coalesce(l.mortgage_inflow, 0) as sec_f_mortgage_inflow,
        coalesce(l.total_inflow, 0) as sec_f_total_inflow,
        -- Section G：HQLA 构成
        coalesce(h.l1_mv, 0) as sec_g_hqla_l1_mv,
        coalesce(h.l2a_mv, 0) as sec_g_hqla_l2a_mv,
        coalesce(h.l2b_mv, 0) as sec_g_hqla_l2b_mv,
        coalesce(h.non_hqla_mv, 0) as sec_g_non_hqla_mv,
        coalesce(h.total_mv, 0) as sec_g_total_mv,
        -- HQLA 认列总额：一级资产全额，二级资产（2A+2B）按 40% 上限截断
        round(
            coalesce(h.l1_mv, 0)
            + least(
                coalesce(h.l2a_mv, 0) + coalesce(h.l2b_mv, 0),
                0.40 * (coalesce(h.l1_mv, 0) + coalesce(h.l2a_mv, 0) + coalesce(h.l2b_mv, 0))
            ),
            2
        ) as sec_g_hqla_capped_total_usd,
        -- Section H：衍生品
        coalesce(v.net_mtm_asset, 0) as sec_h_net_mtm_asset,
        coalesce(v.net_mtm_liability, 0) as sec_h_net_mtm_liability,
        coalesce(v.collateral_posted, 0) as sec_h_collateral_posted,
        coalesce(v.collateral_received, 0) as sec_h_collateral_received,
        coalesce(cf.raw_inflow, 0) as sec_h_expected_inflow_30d,
        coalesce(cf.total_outflow, 0) as sec_h_expected_outflow_30d,
        -- Section I：非受限资产
        coalesce(h.unencumbered_l1, 0) as sec_i_unencumbered_hqla_l1,
        coalesce(h.unencumbered_l2a, 0) as sec_i_unencumbered_hqla_l2a,
        coalesce(h.unencumbered_l2b, 0) as sec_i_unencumbered_hqla_l2b,
        coalesce(h.unencumbered_non_hqla, 0) as sec_i_unencumbered_non_hqla,
        coalesce(h.encumbered_total, 0) as sec_i_encumbered_total,
        -- Section J：或有负债
        coalesce(t.credit_commitments, 0) as sec_j_credit_commitments,
        coalesce(t.letters_of_credit, 0) as sec_j_letters_of_credit,
        coalesce(t.guarantees, 0) as sec_j_guarantees,
        coalesce(t.total_contingent, 0) as sec_j_total_contingent,
        -- Section K：合计。流入按 75% 上限认列
        coalesce(f.repo_outstanding, 0) + coalesce(d.total_deposits_usd, 0) as sec_k_total_funding,
        round(least(coalesce(cf.raw_inflow, 0), 0.75 * coalesce(cf.total_outflow, 0)), 2) as sec_k_total_inflows,
        coalesce(cf.total_outflow, 0) as sec_k_total_outflows,
        round(
            least(coalesce(cf.raw_inflow, 0), 0.75 * coalesce(cf.total_outflow, 0)) - coalesce(cf.total_outflow, 0),
            2
        ) as sec_k_net_funding_gap,
        round(
            least(coalesce(cf.raw_inflow, 0), 0.75 * coalesce(cf.total_outflow, 0)) - coalesce(cf.total_outflow, 0),
            2
        ) as sec_k_cumulative_30d_gap,
        -- report_date 追加在列尾而不是列首：Iceberg 不支持列重排，
        -- 把新列插在中途会让 create or replace table 直接失败。
        e.report_date
    from entities e
    left join deposits d on d.entity_code = e.entity_code
    left join secured_financing f on f.entity_code = e.entity_code
    left join cash c on c.entity_code = e.entity_code
    left join loan_inflows l on l.entity_code = e.entity_code
    left join hqla h on h.entity_code = e.entity_code
    left join derivatives v on v.entity_code = e.entity_code
    left join contingent t on t.entity_code = e.entity_code
    left join cashflow_30d cf on cf.entity_code = e.entity_code

),

consolidated as (

    -- 合并口径：实体口径逐列相加。总资产/总负债类金额直接累加，不做内部交易抵消
    -- （演示数据里实体之间没有内部交易，抵消项为零）。
    select
        'ENT001' as entity_code,
        cast(null as decimal(20, 2)) as sec_a_cp_outstanding,
        cast(null as decimal(20, 2)) as sec_a_cd_outstanding,
        cast(null as decimal(20, 2)) as sec_a_unsecured_borrow,
        cast(null as decimal(20, 2)) as sec_a_fed_funds,
        cast(null as decimal(20, 2)) as sec_a_total,
        sum(sec_b_repo_outstanding) as sec_b_repo_outstanding,
        sum(sec_b_reverse_repo) as sec_b_reverse_repo,
        cast(null as decimal(20, 2)) as sec_b_sec_lending,
        cast(null as decimal(20, 2)) as sec_b_fhlb_advances,
        sum(sec_b_total) as sec_b_total,
        sum(sec_c_retail_demand) as sec_c_retail_demand,
        sum(sec_c_retail_savings) as sec_c_retail_savings,
        sum(sec_c_retail_time) as sec_c_retail_time,
        sum(sec_c_wholesale_demand) as sec_c_wholesale_demand,
        sum(sec_c_wholesale_time) as sec_c_wholesale_time,
        cast(null as decimal(20, 2)) as sec_c_brokered,
        sum(sec_c_total) as sec_c_total,
        cast(null as decimal(20, 2)) as sec_d_total,
        sum(sec_e_cash_total) as sec_e_cash_total,
        sum(sec_e_central_bank_dep) as sec_e_central_bank_dep,
        sum(sec_e_cash_equiv_total) as sec_e_cash_equiv_total,
        sum(sec_f_commercial_inflow) as sec_f_commercial_inflow,
        sum(sec_f_retail_inflow) as sec_f_retail_inflow,
        sum(sec_f_mortgage_inflow) as sec_f_mortgage_inflow,
        sum(sec_f_total_inflow) as sec_f_total_inflow,
        sum(sec_g_hqla_l1_mv) as sec_g_hqla_l1_mv,
        sum(sec_g_hqla_l2a_mv) as sec_g_hqla_l2a_mv,
        sum(sec_g_hqla_l2b_mv) as sec_g_hqla_l2b_mv,
        sum(sec_g_non_hqla_mv) as sec_g_non_hqla_mv,
        sum(sec_g_total_mv) as sec_g_total_mv,
        -- 合并口径的二级资产上限按合并后的总额重新计算，不能把各实体的认列额相加
        round(
            sum(sec_g_hqla_l1_mv)
            + least(
                sum(sec_g_hqla_l2a_mv) + sum(sec_g_hqla_l2b_mv),
                0.40 * (sum(sec_g_hqla_l1_mv) + sum(sec_g_hqla_l2a_mv) + sum(sec_g_hqla_l2b_mv))
            ),
            2
        ) as sec_g_hqla_capped_total_usd,
        sum(sec_h_net_mtm_asset) as sec_h_net_mtm_asset,
        sum(sec_h_net_mtm_liability) as sec_h_net_mtm_liability,
        sum(sec_h_collateral_posted) as sec_h_collateral_posted,
        sum(sec_h_collateral_received) as sec_h_collateral_received,
        sum(sec_h_expected_inflow_30d) as sec_h_expected_inflow_30d,
        sum(sec_h_expected_outflow_30d) as sec_h_expected_outflow_30d,
        sum(sec_i_unencumbered_hqla_l1) as sec_i_unencumbered_hqla_l1,
        sum(sec_i_unencumbered_hqla_l2a) as sec_i_unencumbered_hqla_l2a,
        sum(sec_i_unencumbered_hqla_l2b) as sec_i_unencumbered_hqla_l2b,
        sum(sec_i_unencumbered_non_hqla) as sec_i_unencumbered_non_hqla,
        sum(sec_i_encumbered_total) as sec_i_encumbered_total,
        sum(sec_j_credit_commitments) as sec_j_credit_commitments,
        sum(sec_j_letters_of_credit) as sec_j_letters_of_credit,
        sum(sec_j_guarantees) as sec_j_guarantees,
        sum(sec_j_total_contingent) as sec_j_total_contingent,
        sum(sec_k_total_funding) as sec_k_total_funding,
        -- 合并口径的流入上限按合并后的总额重新计算，不能把各实体的上限结果直接相加
        round(least(sum(sec_h_expected_inflow_30d), 0.75 * sum(sec_k_total_outflows)), 2) as sec_k_total_inflows,
        sum(sec_k_total_outflows) as sec_k_total_outflows,
        round(
            least(sum(sec_h_expected_inflow_30d), 0.75 * sum(sec_k_total_outflows)) - sum(sec_k_total_outflows),
            2
        ) as sec_k_net_funding_gap,
        round(
            least(sum(sec_h_expected_inflow_30d), 0.75 * sum(sec_k_total_outflows)) - sum(sec_k_total_outflows),
            2
        ) as sec_k_cumulative_30d_gap,
        max(report_date) as report_date
    from entity_level

),

-- is_consolidated 放在最后一列，避免与 entity_level 里已有的 entity_code 重名。
--
-- report_id 是业务标识，按「机构-报表-报告期-口径」四段区位码拼装，例如
--   ENT001-FR2052A-20260916-01
-- 每一段都有确定含义，读的人不必查表就知道这条报表是谁报的、什么报表、哪一期、什么口径。
-- 最后一段是口径码：01 = 并表，02 = 法人单体。
--
-- 为什么不用自增序列：本表每轮导出是全量覆盖，序列值属于数据库状态而不是数据，
-- 同一个业务报表在不同批次会拿到不同的号。而重述登记要跨批次引用「原报表 / 新报表」，
-- 键一旦会变，这层对应关系就不成立。
--
-- 为什么不把「第几次报送」编进末段：那等于把版本号塞进主键，而版本已由
-- ads.ads_fr2052a_report_history.record_version 承担。同一件事写两处，两处必然分叉。
--
-- 为什么由模型产出而不是在库里生成：report_id 随 gold 表从 Iceberg 导出，
-- 库里生成则 Iceberg 侧没有这一列，报送台账、重述登记、血缘都拿不到这个身份。
-- 且 PostgreSQL 生成列只接受 IMMUTABLE 表达式，而 date 转文本受 DateStyle 会话参数影响，
-- 实测 cast、concat、to_char、format 四种写法全部被拒。
unioned as (

    select
        e.*,
        false as is_consolidated
    from entity_level e

    union all

    select
        c.*,
        true as is_consolidated
    from consolidated c

)

select
    u.*,
    concat_ws(
        '-',
        u.entity_code,
        '{{ var("report_code") }}',
        date_format(u.report_date, 'yyyyMMdd'),
        case when u.is_consolidated then '01' else '02' end
    ) as report_id
from unioned u
