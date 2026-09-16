#!/usr/bin/env bash
# 在 Server 2 上运行 dbt：注入凭据与环境后调用 venv 里的 dbt。
#
# 用法（在 Server 2 的 ~/fr2052a-infra 下执行）：
#   bash run-dbt.sh <dbt 子命令与参数...>
#
# 例：
#   bash run-dbt.sh debug --target pg
#   bash run-dbt.sh run   --target spark --select spark_smoke

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

# dbt-spark 的 session 模式在本机起 Spark 驱动，需要 JDK 17 与 Iceberg 运行时包。
# 镜像自带的 JDK 11 不满足 Iceberg 1.11 要求，这里用外挂的 JDK 17。
export JAVA_HOME="$BASE_DIR/spark/jdk17"
export PATH="$JAVA_HOME/bin:$PATH"
export PYSPARK_PYTHON="$BASE_DIR/venv/bin/python"

# 外挂的 JDK 17.0.1 在新内核上探测 cgroup 会抛 NPE，连带导致 JMX 的
# BufferPool MBean 注册失败，SparkContext 起不来。session 模式下驱动 JVM
# 由 pyspark 拉起，只能用环境变量注入开关（spark.driver.extraJavaOptions 对
# 已启动的 JVM 无效）。
export JAVA_TOOL_OPTIONS="-XX:-UseContainerSupport"

# session 模式下驱动 JVM 由 pyspark 拉起，spark.jars 注入的包赶不上
# DataSource 注册时机（报 DATA_SOURCE_NOT_FOUND: iceberg），所以直接把
# Iceberg 运行时包放进 pyspark 自带的 jars 目录，保证 JVM 启动即在 classpath 上。
PYSPARK_JARS="$("$BASE_DIR/venv/bin/python" -c \
  'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))' 2>/dev/null || true)"
if [ -n "$PYSPARK_JARS" ] && [ -d "$PYSPARK_JARS" ]; then
  cp -u "$BASE_DIR"/spark/jars/*.jar "$PYSPARK_JARS/"
fi

exec "$BASE_DIR/venv/bin/dbt" "$@" \
  --profiles-dir "$BASE_DIR/dbt" \
  --project-dir "$BASE_DIR/dbt"
