-- 每个报告期恰有一行 is_consolidated = true 的合并行。
--
-- dbt singular test 语义：返回任何一行即测试失败。
--
-- 为什么需要这条断言：
--   合并行是「把集团当一家看」的唯一视角。一个报告期出两行合并行，
--   意味着要么导出重复、要么合并逻辑产出多行；出零行意味着合并视角整个缺失。
--   两者都是数据质量事故，都必须拦。
--
-- 为什么不能写成 `where is_consolidated group by report_date having count(*) <> 1`：
--   那样只能抓到「多于一行」。合并行全部缺失时，过滤后该报告期一行不剩，
--   group by 连分组都不产生，having 永远不会被求值 —— 最严重的形态恰好漏掉。
--   所以基准要取「报表里出现过的全部报告期」，再左连合并行计数，缺的补 0。
--
-- 返回行的含义：该报告期的合并行数既不是 1（缺行或重复）。

with periods as (

    select distinct report_date from {{ ref('ads_fr2052a_report') }}

),

consolidated as (

    select report_date, count(*) as consolidated_row_count
    from {{ ref('ads_fr2052a_report') }}
    where is_consolidated
    group by report_date

)

select
    p.report_date,
    coalesce(c.consolidated_row_count, 0) as consolidated_row_count
from periods p
left join consolidated c
    on c.report_date = p.report_date
where coalesce(c.consolidated_row_count, 0) <> 1
