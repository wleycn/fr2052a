#!/usr/bin/env bash
# 包装 spark-submit：从 .env 注入凭据后把作业提交到 Spark 集群。
#
# 为什么需要它：MinIO 与 PostgreSQL 的口令不能写进 spark-defaults.conf
# （那份配置要入库），所以统一在这里注入。
#
# 用法（在 Server 2 的 ~/fr2052a-infra 下执行）：
#   bash spark-submit-fr2052a.sh <作业脚本> [额外的 spark-submit 参数...]
#
# 例：
#   bash spark-submit-fr2052a.sh /opt/spark/fr2052a/iceberg_smoke.py

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$BASE_DIR/.env"
if [ ! -f "$ENV_FILE" ]; then
  echo "缺少 $ENV_FILE，请先按 .env.example 生成" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a

if [ "$#" -lt 1 ]; then
  echo "用法: bash spark-submit-fr2052a.sh <作业脚本> [spark-submit 参数...]" >&2
  exit 1
fi

exec docker exec fr2052a_spark_master /opt/spark/bin/spark-submit \
  --master "spark://spark-master:7077" \
  --conf "spark.sql.catalog.spark_catalog.uri=jdbc:postgresql://${SERVER1_HOST}:5432/${POSTGRES_DB}?currentSchema=iceberg_catalog" \
  --conf "spark.sql.catalog.spark_catalog.jdbc.user=${POSTGRES_USER}" \
  --conf "spark.sql.catalog.spark_catalog.jdbc.password=${POSTGRES_PASSWORD}" \
  --conf "spark.sql.catalog.spark_catalog.s3.access-key-id=${MINIO_ROOT_USER}" \
  --conf "spark.sql.catalog.spark_catalog.s3.secret-access-key=${MINIO_ROOT_PASSWORD}" \
  "$@"
