#!/usr/bin/env bash
# FR 2052a 每日跑批。既可整条跑，也可按环节单跑 —— Airflow DAG 按环节调用同一份编排，
# 不另写一套 DAG 内的命令，避免编排逻辑出现第二份副本。
#
# 用法（在 Server 2 的 ~/fr2052a-infra 下执行）：
#   bash run-daily-pipeline.sh                       # 按依赖顺序跑全部环节
#   bash run-daily-pipeline.sh ref-load dbt-run      # 只跑指定环节
#   bash run-daily-pipeline.sh --list                # 列出可选环节
#
# 退出码：0 通过；非 0 表示某环节失败（原样透出该环节的退出码）。
# Airflow DAG 依赖这个退出码做阻断判定。

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="${APP_DIR:-/opt/fr2052a-app}"
LOG_DIR="${PIPELINE_LOG_DIR:-/tmp}"
BATCH_ID="${BATCH_ID:-BATCH-20260916-001}"

# 依赖顺序：字典表先就位 → 明细入湖 → 建模 → 校验 → 导出 → 核对
STEPS=(
  ref-load
  ods-replay
  bronze-load
  dbt-run
  dq-rules
  export-pg
  verify-bronze
  verify-silver
  verify-ads
)

declare -A STEP_DESC=(
  [ref-load]="REF 批加载：字典表入 ref 命名空间"
  [ods-replay]="ODS 重放：样本明细按主题打进 Kafka"
  [bronze-load]="入湖 bronze：消费 Kafka，按主键 MERGE 去重"
  [dbt-run]="dbt 三层建模：OWD → OWS → ADS"
  [dq-rules]="数据质量：执行 ref 层声明的校验规则"
  [export-pg]="导出 PostgreSQL：gold 层报表进报送服务层"
  [verify-bronze]="核对 bronze：与样本 CSV 行数比对"
  [verify-silver]="核对 OWD：行数、分桶、折算逐行重算"
  [verify-ads]="核对 ADS：合并口径、明细回溯、监管上限、GL 对账"
)

execute_step() {
  case "$1" in
    ref-load)
      bash spark-submit-fr2052a.sh "$APP_DIR/python/lakehouse/load_ref_tables.py" "$APP_DIR/sample_data/ref"
      ;;
    ods-replay)
      bash spark-submit-fr2052a.sh "$APP_DIR/python/producers/replay_ods_to_kafka.py" \
        --data-dir "$APP_DIR/sample_data/ods" --config "$APP_DIR/config/pipeline_topics.json"
      ;;
    bronze-load)
      bash spark-submit-fr2052a.sh "$APP_DIR/python/consumers/kafka_to_iceberg.py" \
        --config "$APP_DIR/config/pipeline_topics.json"
      ;;
    dbt-run)
      bash run-dbt.sh run --target spark --exclude tag:smoke
      ;;
    dq-rules)
      bash spark-submit-fr2052a.sh "$APP_DIR/python/validators/run_dq_rules.py" --batch-id "$BATCH_ID"
      ;;
    export-pg)
      bash spark-submit-fr2052a.sh "$APP_DIR/python/exporters/export_gold_to_pg.py"
      ;;
    verify-bronze)
      bash spark-submit-fr2052a.sh "$APP_DIR/python/lakehouse/verify_bronze.py" \
        --data-dir "$APP_DIR/sample_data/ods" --config "$APP_DIR/config/pipeline_topics.json"
      ;;
    verify-silver)
      bash spark-submit-fr2052a.sh "$APP_DIR/python/lakehouse/verify_silver.py"
      ;;
    verify-ads)
      bash spark-submit-fr2052a.sh "$APP_DIR/python/lakehouse/verify_gold.py"
      ;;
    *)
      echo "未知环节：$1" >&2
      echo "可选环节：${STEPS[*]}" >&2
      return 2
      ;;
  esac
}

run_one() {
  local name="$1"
  local log="${LOG_DIR}/pipeline-$(date +%Y%m%d-%H%M%S)-${name}.log"
  echo "──── ${name}：${STEP_DESC[$name]:-无说明}"
  if execute_step "$name" >"$log" 2>&1; then
    echo "  [OK]   ${name}（日志 ${log}）"
  else
    local rc=$?
    echo "  [FAIL] ${name}（退出码 ${rc}，日志 ${log}）" >&2
    tail -25 "$log" >&2
    exit "$rc"
  fi
}

if [ "${1:-}" = "--list" ]; then
  for step in "${STEPS[@]}"; do
    echo "${step}	${STEP_DESC[$step]}"
  done
  exit 0
fi

cd "$BASE_DIR"

requested=("$@")
if [ "${#requested[@]}" -eq 0 ]; then
  requested=("${STEPS[@]}")
fi

for step in "${requested[@]}"; do
  run_one "$step"
done

echo
echo "完成：${#requested[@]} 个环节全部通过（批次 ${BATCH_ID}）"
