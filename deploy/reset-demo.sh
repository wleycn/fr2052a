#!/usr/bin/env bash
# 一键把演示环境初始化回可复现的基线。
#
# 为什么需要它：
#   跑过一轮演示（造差异、重述、重放）之后，环境里会留下各种派生状态 ——
#   报表版本历史、重述登记、校验日志、Kafka 主题内容、流式消费位点。
#   手工清容易漏，而漏一处就会让下一轮的结果与预期不一致，且往往不报错
#   （例如只清主题不清消费位点，重放后作业一条都不会读）。
#   所以把「清哪里、按什么顺序、清完怎么回到基线」固化成一个脚本。
#
# 默认只列清单不动手（演练）。真清必须显式 --apply ——
# 与 python/lakehouse/maintain_tables.py 的强弱分级约定一致，避免手滑。
#
# 用法（在开发机执行）：
#   bash deploy/reset-demo.sh                    # 演练：列出将清理的对象
#   bash deploy/reset-demo.sh --apply             # 真清，并重跑全链路回到基线
#   bash deploy/reset-demo.sh --apply --no-run    # 只清，不重跑
#   bash deploy/reset-demo.sh --apply --report-date 2026-09-16
#
# 清理范围（三层，缺一层就会重现旧状态）：
#   1. PostgreSQL 派生表          sql/admin/reset_demo.sql（清单在该文件里，本脚本不另抄一份）
#   2. Iceberg 的 OWD 版本历史表  湖侧唯一会跨轮次累积的状态，其余由建模与入湖作业重建/归并
#   3. Kafka 主题内容 + 消费位点   deploy/server2/reset-streaming-state.sh
# 清完重新生成样本数据（固定种子），再跑一遍链路，环境即回到与首次运行一致的基线。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

SERVER1_HOST="${SERVER1_HOST:-192.168.17.22}"
SERVER2_HOST="${SERVER2_HOST:-192.168.17.24}"
REMOTE_DIR="${REMOTE_DIR:-/home/hermes/fr2052a-infra}"
POSTGRES_USER="${POSTGRES_USER:-fr2052a}"
POSTGRES_DB="${POSTGRES_DB:-fr2052a_db}"
PG_CONTAINER="${PG_CONTAINER:-fr2052a_postgres}"
REPORT_DATE="${REPORT_DATE:-2026-09-16}"

RESET_SQL="$PROJECT_ROOT/sql/admin/reset_demo.sql"
HISTORY_SQL="$PROJECT_ROOT/sql/iceberg/06_rebuild_owd_history.sql"

APPLY=0
RUN_AFTER=1

while [ "$#" -gt 0 ]; do
  case "$1" in
    --apply) APPLY=1 ;;
    --no-run) RUN_AFTER=0 ;;
    --report-date) REPORT_DATE="$2"; shift ;;
    -h|--help) sed -n '2,25p' "$0"; exit 0 ;;
    *) echo "未知参数：$1" >&2; exit 1 ;;
  esac
  shift
done

for required in "$RESET_SQL" "$HISTORY_SQL"; do
  [ -f "$required" ] || { echo "缺少文件：$required" >&2; exit 1; }
done

echo "演示环境初始化（报告日 $REPORT_DATE）"
echo

echo "1) 重新生成样本数据（固定种子，同样的输入每次得到同样的数据）"
echo "   命令：python3 python/generators/generate_sample_data.py --out sample_data"
echo
echo "2) 同步代码与样本数据到 Server 2"
echo "   命令：bash deploy/sync-deploy.sh && bash deploy/server2/sync-app.sh"
echo
echo "3) 清 PostgreSQL 派生表（清单取自 $RESET_SQL）"
grep -E '^(TRUNCATE TABLE|DROP (TABLE|VIEW)|UPDATE ads\.)' "$RESET_SQL" | sed 's/^/   /'
echo
echo "4) 清 Iceberg 的 OWD 版本历史表"
echo "   文件：sql/iceberg/06_rebuild_owd_history.sql（由 owd_scd2.py 重建干净基线）"
echo
echo "5) 清 Kafka 主题内容与流式消费位点"
python3 - "$PROJECT_ROOT/config/pipeline_topics.json" <<'PY' | sed 's/^/   /'
import json, sys
for topic in json.load(open(sys.argv[1]))["topics"]:
    print(f"主题 {topic['name']}")
PY
echo "   位点目录：$REMOTE_DIR/checkpoints"
echo
if [ "$RUN_AFTER" -eq 1 ]; then
  echo "6) 重跑全链路回到基线（bash run-daily-pipeline.sh）"
else
  echo "6) 不重跑链路（--no-run）：清完即止"
fi
echo

if [ "$APPLY" -ne 1 ]; then
  echo "以上为演练。确实要执行请加 --apply。"
  exit 0
fi

echo "──── 开始执行 ────"

echo "[1/6] 重新生成样本数据"
python3 "$PROJECT_ROOT/python/generators/generate_sample_data.py" --out "$PROJECT_ROOT/sample_data" | tail -3

echo "[2/6] 同步到服务器"
bash "$SCRIPT_DIR/sync-deploy.sh" >/dev/null
bash "$SCRIPT_DIR/server2/sync-app.sh" >/dev/null
echo "  [OK] deploy/ 与应用代码已同步"

echo "[3/6] 清 PostgreSQL 派生表"
ssh "$SERVER1_HOST" \
  "docker exec -i $PG_CONTAINER psql -v ON_ERROR_STOP=1 -q -U $POSTGRES_USER -d $POSTGRES_DB" \
  < "$RESET_SQL"

echo "[4/6] 清 Iceberg 的 OWD 版本历史表"
ssh "$SERVER2_HOST" \
  "cd $REMOTE_DIR && bash spark-submit-fr2052a.sh /opt/fr2052a-app/python/lakehouse/run_sql_file.py /opt/fr2052a-app/sql/iceberg/06_rebuild_owd_history.sql" \
  2>&1 | grep -v "^2[0-9]/[0-9][0-9]/[0-9][0-9]" | tail -12

echo "[5/6] 清 Kafka 主题与消费位点"
ssh "$SERVER2_HOST" "cd $REMOTE_DIR && bash reset-streaming-state.sh" | tail -8

if [ "$RUN_AFTER" -eq 1 ]; then
  echo "[6/6] 重跑全链路"
  ssh "$SERVER2_HOST" "cd $REMOTE_DIR && REPORT_DATE=$REPORT_DATE bash run-daily-pipeline.sh" | tail -25
else
  echo "[6/6] 跳过重跑（--no-run）"
fi

echo
echo "完成。核对环境状态："
echo "  ssh $SERVER2_HOST 'cd $REMOTE_DIR && bash run-daily-pipeline.sh health'"
