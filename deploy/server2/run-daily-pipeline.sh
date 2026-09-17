#!/usr/bin/env bash
# FR 2052a 每日跑批：一条命令跑完整条链路。
#
# 串联顺序与依赖关系：
#   1. REF 批加载         字典表先就位，后续折算与口径归一都依赖它
#   2. ODS 重放进 Kafka   模拟各源系统上报
#   3. ODS 流入 bronze    至少一次投递 + 按主键 MERGE 去重
#   4. dbt 三层建模       OWD → OWS → ADS（Iceberg）
#   5. 报表导出 PostgreSQL 报送服务层
#   6. 三层核对           任一失败即整体失败
#
# 用法（在 Server 2 的 ~/fr2052a-infra 下执行）：
#   bash run-daily-pipeline.sh
#
# 退出码：0 表示全链路与核对全部通过；非 0 表示某环节失败。
# E5 的 Airflow DAG 依赖这个退出码做阻断判定。

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="/opt/fr2052a-app"
LOG_DIR="${PIPELINE_LOG_DIR:-/tmp}"
RUN_STAMP="$(date +%Y%m%d-%H%M%S)"

step() {
  local name="$1"
  shift
  local log="${LOG_DIR}/pipeline-${RUN_STAMP}-${name}.log"
  echo "──── ${name} ────"
  if ! "$@" >"$log" 2>&1; then
    echo "  [FAIL] ${name}，日志：${log}" >&2
    tail -20 "$log" >&2
    exit 1
  fi
  echo "  [OK]   ${name}（日志 ${log}）"
}

cd "$BASE_DIR"

step "1-ref批加载" \
  bash spark-submit-fr2052a.sh "$APP_DIR/python/lakehouse/load_ref_tables.py" "$APP_DIR/sample_data/ref"

step "2-ODS重放" \
  bash spark-submit-fr2052a.sh "$APP_DIR/python/producers/replay_ods_to_kafka.py" \
  --data-dir "$APP_DIR/sample_data/ods" --config "$APP_DIR/config/pipeline_topics.json"

step "3-入湖bronze" \
  bash spark-submit-fr2052a.sh "$APP_DIR/python/consumers/kafka_to_iceberg.py" \
  --config "$APP_DIR/config/pipeline_topics.json"

step "4-dbt三层建模" \
  bash run-dbt.sh run --target spark --exclude tag:smoke

step "5-导出PostgreSQL" \
  bash spark-submit-fr2052a.sh "$APP_DIR/python/exporters/export_gold_to_pg.py"

step "6-核对bronze" \
  bash spark-submit-fr2052a.sh "$APP_DIR/python/lakehouse/verify_bronze.py" \
  --data-dir "$APP_DIR/sample_data/ods" --config "$APP_DIR/config/pipeline_topics.json"

step "7-核对OWD" \
  bash spark-submit-fr2052a.sh "$APP_DIR/python/lakehouse/verify_silver.py"

step "8-核对ADS" \
  bash spark-submit-fr2052a.sh "$APP_DIR/python/lakehouse/verify_gold.py"

echo
echo "全链路跑批完成，8 个环节全部通过（批次 ${RUN_STAMP}）"
