{{ config(materialized='view') }}

-- 连通性自检：验证 dbt 能否连上 Server 1 的 PostgreSQL 并在 ads 层建对象。
-- 正式跑批时用 --exclude tag:smoke 排除。

select
    1                as check_id,
    'pg-ok'::text    as note,
    now()            as checked_at
