#!/usr/bin/env bash
# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
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
# 容器内路径与宿主路径不是同一个：Spark 作业跑在容器里，看到的是 /opt/fr2052a-app；
# 走 venv 直跑的环节在宿主上执行，看到的是 app/ 目录本身。混用会直接报文件不存在。
HOST_APP_DIR="${HOST_APP_DIR:-$BASE_DIR/app}"
LOG_DIR="${PIPELINE_LOG_DIR:-/tmp}"
BATCH_ID="${BATCH_ID:-BATCH-20260916-001}"
REPORT_DATE="${REPORT_DATE:-2026-09-16}"
# 版本生效日按处理时间走，本演示按 T+1 跑批，取报告日次日。
# 不取系统当前时间：同一个报告日重复跑必须算出同样的版本区间，否则不可复现。
# 为什么不用报告日本身：重述用的是处理日（次日），日批若用报告日，两个调用方
# 的生效日约定就不一致，后跑的那次会把先写下的版本压成零长度区间。
PROCESSING_DATE="${PROCESSING_DATE:-$(date -d "$REPORT_DATE +1 day" +%F)}"

# 走 venv 直跑的环节（liquidity-monitor / gate / submission）需要 PG 连接信息。
# 从同一份 .env 注入，不让凭据出现第二个来源；spark-submit 包装脚本内部也会再 source 一次，
# 重复 source 是幂等的，代价远低于「某条路径拿不到凭据」。
if [ -f "$BASE_DIR/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "$BASE_DIR/.env"
  set +a
fi

# 依赖顺序：字典表先就位 → 明细入湖 → 建模 → 校验 → 发布库对象（迁移与授权）→ 导出 → 流动性判定 → 核对
#
# 有意留在本序列之外的两个环节：
#   gate        报送放行闸，由 fr2052a_submission DAG 作为第一道任务调用，
#               不在日批里跑 —— 否则日批会因熔断而整体红，看不出是哪一环的问题
#   submission  报送文件生成，同理
STEPS=(
  run-context-open
  ref-load
  ods-replay
  bronze-load
  dbt-run
  pii-vault
  lineage
  owd-scd2
  dq-rules
  publish-access
  export-pg
  liquidity-monitor
  verify-bronze
  verify-silver
  verify-scd2
  verify-ads
  verify-rbac
  maintain-tables-apply
  run-context-close
)

declare -A STEP_DESC=(
  [check-source]="源文件到位检查：ODS 目录的 CSV 张数须与主题声明一致，数量不对即失败"
  [ref-load]="REF 批加载：字典表入 ref 命名空间"
  [ods-replay]="ODS 重放：样本明细按主题打进 Kafka"
  [bronze-load]="入湖 bronze：消费 Kafka，按主键 MERGE 去重"
  [dbt-run]="dbt 三层建模 + 断言：OWD → OWS → ADS，再跑 singular test 守汇率覆盖"
  [pii-vault]="PII 对照表：从落地数据建 token ↔ 明文映射（明文唯一落点）"
  [lineage]="血缘与监管映射：渲染报告并写审计血缘表"
  [owd-scd2]="OWD 版本历史：SCD2 归并，记录新增/变更/删除"
  [dq-rules]="数据质量：执行 ref 层声明的校验规则"
  [export-pg]="导出 PostgreSQL：gold 层报表进报送服务层"
  [publish-access]="发布库对象：先于导出施加迁移与授权，并核对无零授权对象"
  [liquidity-monitor]="流动性判定：算 LCR 与预警，翻转熔断闸"
  [gate]="报送放行闸：熔断中则拒绝报送（退出码 2）"
  [submission]="报送文件生成：XBRL/XML/CSV + 回执落库"
  [restate-capture]="重述登记（前段）：重跑前把当前报文存进版本历史"
  [restate-register]="重述登记（后段）：关闭旧版本、登记新版本与重述记录"
  [realtime-scan]="实时扫描：消费核心存款主题，识别大额未保险敞口"
  [realtime-summary]="实时敞口汇总：从事件表读本轮预警"
  [verify-submission]="核对报送：从磁盘重算哈希与台账比对"
  [verify-rbac]="权限自测：逐角色实读，与权限声明比对"
  [run-context-open]="运行上下文开口：登记本次跑批处理的报告日/处理日/生效日"
  [run-context-close]="运行上下文收口：把本次跑批标为成功"
  [health]="跑批健康巡检：熔断/质量/报送/Kafka 滞后/连接/磁盘"
  [time-travel]="时间旅行：列出 Iceberg 快照（审计用）"
  [maintain-tables]="表维护：快照保留与文件合并（默认演练）"
  [maintain-tables-apply]="表维护（日批环节）：按声明的保留策略真过期快照并合并小文件"
  [verify-bronze]="核对 bronze：与样本 CSV 行数比对"
  [verify-silver]="核对 OWD：行数、分桶、折算逐行重算"
  [verify-scd2]="核对版本历史：END_DATE 为空 ⇔ 当前有效、版本号连续、无重复"
  [verify-ads]="核对 ADS：合并口径、明细回溯、监管上限、GL 对账"
)


# 注意：一个环节里有多条命令时，必须显式串联（`&&` 或 `|| return $?`）。
# 本函数被 run_one 当 if 条件调用，而条件上下文里 `set -e` 对函数体不生效，
# 于是「最后一条命令成功」就会把前面失败的命令盖过去。单条命令的环节不受影响。
execute_step() {
  case "$1" in
    run-context-open)
      ./venv/bin/python "$HOST_APP_DIR/python/governance/run_context.py" \
        --open --batch-id "$BATCH_ID" --report-date "$REPORT_DATE" \
        --processing-date "$PROCESSING_DATE" --effective-date "${EFFECTIVE_DATE:-$PROCESSING_DATE}" \
        --run-type "${RUN_TYPE:-MANUAL}"
      ;;
    check-source)
      # 必须用宿主路径：SSH 进来执行时看不到容器内的 /opt/fr2052a-app。
      # 也不能直接用 `ls | wc -l` —— wc 恒退出 0，缺文件时这一步照样"成功"。
      # 期望张数从主题声明派生（有生产者的主题 = 应到位的源文件），不写死数字：
      # 写死的话新增一张 ODS 表就要来改这里的常量，改漏的表现是「张数对不上」被误判成缺文件。
      expected=$(./venv/bin/python -c "
import json
topics = json.load(open('$HOST_APP_DIR/config/pipeline_topics.json'))['topics']
print(sum(1 for topic in topics if topic.get('has_producer')))
")
      count=$(ls -1 "$HOST_APP_DIR/sample_data/ods/"*.csv 2>/dev/null | wc -l)
      if [ "$count" -ne "$expected" ]; then
        echo "ODS 源文件应为 $expected 张，实际 $count 张（目录 $HOST_APP_DIR/sample_data/ods）" >&2
        exit 1
      fi
      echo "ODS 源文件 $expected 张，已到位"
      ;;
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
      # 两条命令必须用 && 串起来。execute_step 是被 run_one 当 if 条件调用的，
      # 条件上下文里 `set -e` 对整个函数体不生效 —— 不串联的话，dbt run 失败会被
      # 后面 dbt test 的成功覆盖掉，环节照样报 OK。实测踩过：对账模型报
      # AMBIGUOUS_REFERENCE、gold 表没建出来，环节却是 [OK]，直到导出环节才炸。
      bash run-dbt.sh run --target spark --exclude tag:smoke \
        && bash run-dbt.sh test --target spark
      ;;
    dq-rules)
      # 先清掉本批次的旧结果再追加。结果表按批次追加、跨批次留历史，
      # 不先删就会重跑一次多一份，「本批次有几条 ERROR」静默翻倍。
      # 删除放在 venv 侧做：Spark 的 JDBC 只能整表覆盖写，不能按条件删行。
      ./venv/bin/python "$HOST_APP_DIR/python/validators/clear_dq_batch.py" \
        --batch-id "$BATCH_ID" \
        && bash spark-submit-fr2052a.sh "$APP_DIR/python/validators/run_dq_rules.py" --batch-id "$BATCH_ID"
      ;;
    export-pg)
      # 批次号显式传进去：导出作业要用它核对运行上下文（覆盖写前置闸的一条判据），
      # 而包装脚本只透传数据库凭据，不保证 BATCH_ID 会进容器环境。
      #
      # 「覆盖已报送期」的开关同样必须转成命令行参数：spark-submit 包装脚本只把白名单里的
      # 变量透进容器，靠 ALLOW_EXPORT_AFTER_SUBMISSION 环境变量传是传不到的（实测漏过一次：
      # 重述流程带了变量，容器里看不见，前置闸照样拦）。重述流程的 rebuild 步骤设这个变量。
      export_extra=()
      if [ "${ALLOW_EXPORT_AFTER_SUBMISSION:-0}" = "1" ]; then
        export_extra+=(--allow-after-submission)
      fi
      bash spark-submit-fr2052a.sh "$APP_DIR/python/exporters/export_gold_to_pg.py" \
        --batch-id "$BATCH_ID" "${export_extra[@]}"
      ;;
    publish-access)
      # 必须在 export-pg 之前：先施加结构迁移与授权，导出用 truncate=true 保住它们。
      # 脚本另外核对没有任何对象是零授权的 —— 万一有人改回删除重建的老路，这里会立刻红。
      ./venv/bin/python "$HOST_APP_DIR/python/governance/publish_access.py" \
        --sql-dir "$HOST_APP_DIR/sql/postgres"
      ;;
    liquidity-monitor)
      # 走 venv 直跑而不是 spark-submit：输入是已导出的报送服务层（几行），
      # 为一次小聚合起 Spark 会话只是多一层依赖，且 Spark 容器里没有 psycopg2。
      ./venv/bin/python "$HOST_APP_DIR/python/alerts/liquidity_monitor.py" \
        --report-date "$REPORT_DATE" --batch-id "$BATCH_ID" \
        --config "$HOST_APP_DIR/config/liquidity_thresholds.json" \
        --topics-config "$HOST_APP_DIR/config/pipeline_topics.json"
      ;;
    gate)
      # 闸用 venv 直跑，不经 Spark：判定不依赖计算集群，少一层可能失效的依赖。
      ./venv/bin/python "$HOST_APP_DIR/python/validators/check_submission_gate.py" \
        --report-date "$REPORT_DATE"
      ;;
    submission)
      # 闸先跑，通过才生成文件。把这一步并进同一条命令，是为了让「绕过闸直接报送」
      # 在流水线层面不可能 —— 否则任何一处直接调用 submission 都能跳过阻断。
      ./venv/bin/python "$HOST_APP_DIR/python/validators/check_submission_gate.py" \
        --report-date "$REPORT_DATE" \
        && ./venv/bin/python "$HOST_APP_DIR/python/exporters/generate_submission.py" \
          --report-date "$REPORT_DATE" --output-dir "${SUBMISSION_DIR:-$BASE_DIR/submissions}" \
          ${RECEIPT_FILE:+--receipt-file "$RECEIPT_FILE"}
      ;;
    owd-scd2)
      # dbt 重建出当前快照后立刻归并版本历史：迟一步就会漏掉中间态。
      # 生效日决定版本区间：日常跑批用处理日（报告日次日），重述时由调用方给真实处理日。
      bash spark-submit-fr2052a.sh "$APP_DIR/python/lakehouse/owd_scd2.py" \
        --report-date "$REPORT_DATE" \
        --effective-date "${EFFECTIVE_DATE:-$PROCESSING_DATE}" \
        --reason "${SCD2_REASON:-CORRECTION}"
      ;;
    pii-vault)
      # 读落地数据建明文对照表。不做这一步，OWD 里的 token 就没有可还原的途径，
      # 合规官「可见明文」这条就落不了地。
      ./venv/bin/python "$HOST_APP_DIR/python/governance/build_pii_vault.py" \
        --manifest "$HOST_APP_DIR/dbt/target/manifest.json" \
        --data-dir "$HOST_APP_DIR/sample_data/ods" \
        --project "$HOST_APP_DIR/dbt/dbt_project.yml"
      ;;
    lineage)
      # 血缘与监管映射：读 dbt manifest 渲染报告并写 audit.audit_data_lineage
      ./venv/bin/python "$HOST_APP_DIR/python/governance/render_lineage.py" \
        --manifest "$HOST_APP_DIR/dbt/target/manifest.json" \
        --output-dir "$HOST_APP_DIR/build/governance"
      ;;
    restate-capture)
      ./venv/bin/python "$HOST_APP_DIR/python/lakehouse/restate.py" \
        --mode capture --report-date "$REPORT_DATE" --entity-code "$ENTITY_CODE" \
        --effective-date "${EFFECTIVE_DATE:-$PROCESSING_DATE}" \
        --reason "${RESTATE_REASON:-数据修正}" --requested-by "${REQUESTED_BY:-unknown}"
      ;;
    restate-register)
      ./venv/bin/python "$HOST_APP_DIR/python/lakehouse/restate.py" \
        --mode register --report-date "$REPORT_DATE" --entity-code "$ENTITY_CODE" \
        --effective-date "${EFFECTIVE_DATE:-$PROCESSING_DATE}" \
        --reason "${RESTATE_REASON:-数据修正}" --requested-by "${REQUESTED_BY:-unknown}" \
        --approved-by "${APPROVED_BY:-}"
      ;;
    realtime-scan)
      bash spark-submit-fr2052a.sh "$APP_DIR/python/alerts/realtime_scanner.py" \
        --report-date "$REPORT_DATE" \
        --config "$APP_DIR/config/liquidity_thresholds.json" \
        --topics-config "$APP_DIR/config/pipeline_topics.json"
      ;;
    realtime-summary)
      # 汇总走 venv 直连 PostgreSQL：跨机器只走网络，不做跨机器 exec 容器
      # （PG 在 Server 1，而本环节跑在 Server 2 的宿主上）。
      ./venv/bin/python "$HOST_APP_DIR/python/alerts/summarize_realtime_alerts.py" \
        --report-date "$REPORT_DATE"
      ;;
    verify-submission)
      ./venv/bin/python "$HOST_APP_DIR/python/validators/verify_submission.py" \
        --report-date "$REPORT_DATE"
      ;;
    verify-rbac)
      ./venv/bin/python "$HOST_APP_DIR/python/governance/verify_rbac.py"
      ;;
    run-context-close)
      ./venv/bin/python "$HOST_APP_DIR/python/governance/run_context.py" \
        --close --batch-id "$BATCH_ID" --status SUCCEEDED
      ;;
    health)
      # 巡检恒返回 0：监控是观测手段，不是闸门。当闸用会让「监控挂了」与「系统有问题」无法区分。
      ./venv/bin/python "$HOST_APP_DIR/python/governance/pipeline_health.py" ${HEALTH_JSON:+--json}
      ;;
    time-travel)
      bash spark-submit-fr2052a.sh "$APP_DIR/python/audit/time_travel.py" \
        --table "${TT_TABLE:-silver.owd_deposits}" --list-snapshots
      ;;
    maintain-tables)
      # 默认演练，--apply 才真做；过期快照不可逆，见脚本头部说明。
      bash spark-submit-fr2052a.sh "$APP_DIR/python/lakehouse/maintain_tables.py" \
        ${MAINTAIN_APPLY:+--apply}
      ;;
    maintain-tables-apply)
      # 日批环节：按脚本里声明的策略（快照保留 7 天且至少 10 个）过期快照并合并小文件。
      # 授权口径见 docs/business/KNOWN-ISSUE.md 的「日批自动过期 Iceberg 快照」行。
      bash spark-submit-fr2052a.sh "$APP_DIR/python/lakehouse/maintain_tables.py" --apply
      ;;
    verify-bronze)
      bash spark-submit-fr2052a.sh "$APP_DIR/python/lakehouse/verify_bronze.py" \
        --data-dir "$APP_DIR/sample_data/ods" --config "$APP_DIR/config/pipeline_topics.json"
      ;;
    verify-silver)
      bash spark-submit-fr2052a.sh "$APP_DIR/python/lakehouse/verify_silver.py"
      ;;
    verify-scd2)
      bash spark-submit-fr2052a.sh "$APP_DIR/python/lakehouse/verify_scd2.py"
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
