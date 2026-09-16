{{ config(materialized='table', file_format='iceberg') }}

-- 连通性自检：验证 dbt 能否经 Spark 集群把表写进 MinIO 上的 Iceberg。
-- 正式跑批时用 --exclude tag:smoke 排除。

select
    cast(1 as int)                as check_id,
    'spark-ok'                    as note,
    current_timestamp()           as checked_at
