#!/usr/bin/env bash
# [AI-GENERATED] model=deepseek-flash date=2026-09-19 reviewed_by=pending
# 把巡检结论转成 Prometheus 能抓的指标文件（node_exporter 的 textfile collector 读它）。
#
# 为什么复用 `pipeline_health.py --json` 而不是另写一份采集逻辑：
# 巡检的判据（熔断状态、ERROR 计数、预警、报送、滞后、磁盘占比）已经在那个脚本里，
# 面板与命令行必须看同一个结论。转换只做字段映射，不加新的判断。
#
# 用法（在 Server 2 的 ~/fr2052a-infra 下执行）：
#   bash health_to_metrics.sh
# 定时：同一台机器上由 crontab 每 5 分钟跑一次（见 docs/business/KNOWN-ISSUE.md 的登记）。
#
# 失败时的行为：不覆盖上一次的指标文件。这样「采集挂了」会由 Fr2052aHealthStale
# 告警暴露出来，而不是被一份写了一半的新数据盖掉。
#
# 中间文件走 $OUT_DIR 下的点号开头文件：同一个文件系统，且名字不以 .prom 结尾，
# node_exporter 的 textfile collector 会忽略它。

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR="${METRICS_TEXTFILE_DIR:-$BASE_DIR/monitoring/textfile}"
OUT_FILE="$OUT_DIR/fr2052a_health.prom"
JSON_FILE="$OUT_DIR/.health.json.tmp"

# 凭据与连接信息只从 .env 读，不让它们出现第二个来源。
if [ -f "$BASE_DIR/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "$BASE_DIR/.env"
  set +a
fi

mkdir -p "$OUT_DIR"
trap 'rm -f "$JSON_FILE"' EXIT

"$BASE_DIR/venv/bin/python" "$BASE_DIR/app/python/governance/pipeline_health.py" --json > "$JSON_FILE"
"$BASE_DIR/venv/bin/python" "$BASE_DIR/health_json_to_prom.py" "$JSON_FILE" "$OUT_FILE"
