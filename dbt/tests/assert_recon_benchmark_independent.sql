-- 断言：Section E 的对账基准必须与报送侧真的不同源。
--
-- 背景：Section E（现金）曾经两侧都读总账 1001/1100 —— 校验对象与被校验对象是同一份
-- 数据，差异恒为零，对账永远 PASS。审计把这条点名为「自比对」。修法是把基准侧换成
-- 司库现金头寸（银行对账单 + 库存现金盘点），差额由在途存款与未兑现支票解释。
--
-- 为什么还需要这条断言：换来源只是「这次改对了」，改回去不会有任何症状 —— 残差照样
-- 是 0、对账照样全绿。判据必须能抓到「退回自比对」这个动作本身，而不只是抓它的后果。
-- 三条判据各管一件事：
--   1. benchmark_source 必须是 TREASURY_CASH_POSITION —— 抓「基准侧改回总账」；
--   2. 基准金额与报送金额必须不相等 —— 抓「两侧恰好同值」（同源或调节项被抹平）；
--   3. 调节项净额必须不为零 —— 抓「未达账项退化成零」。
--
-- 通过 = 零行；返回任何一行即失败。

select
    report_date,
    entity_code,
    section_code,
    benchmark_source,
    benchmark_amount,
    reconciling_item_usd,
    fr2052a_amount,
    round(benchmark_amount - fr2052a_amount, 2) as benchmark_gap,
    case
        when benchmark_source <> 'TREASURY_CASH_POSITION' then '基准来源不是司库现金头寸'
        when abs(round(benchmark_amount - fr2052a_amount, 2)) < 0.01 then '基准与报送同值'
        else '调节项为零'
    end as reason
from {{ ref('ads_gl_reconciliation') }}
where section_code = 'E'
  and (
        benchmark_source <> 'TREASURY_CASH_POSITION'
        or abs(round(benchmark_amount - fr2052a_amount, 2)) < 0.01
        or abs(reconciling_item_usd) < 0.01
  )
