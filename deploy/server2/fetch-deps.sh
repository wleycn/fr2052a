#!/usr/bin/env bash
# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
# 下载 Spark 侧所需的第三方二进制依赖到 ./spark/ 下。
#
# 为什么不用镜像仓库：Docker 镜像源实测被限速（约 150 KB/s），
# 而国内软件镜像站可达 1.6 MB/s。Java 侧依赖一律走软件镜像。
#
# 用法：在 Server 2 的 ~/fr2052a-infra 下执行 bash fetch-deps.sh
#
# 依赖清单：
#   spark/jars/   Iceberg Spark 运行时包 + AWS bundle + PostgreSQL 驱动 + Kafka 连接器
#   spark/jdk17/  OpenJDK 17（Iceberg 1.11 要求 Java 17，而 Spark 镜像自带 Java 11）

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
SPARK_DIR="$BASE_DIR/spark"
JARS_DIR="$SPARK_DIR/jars"
mkdir -p "$JARS_DIR"

# 优先国内镜像，镜像未同步该文件时回退 Maven 中央仓库。
MAVEN_BASES=(
  "https://maven.aliyun.com/repository/public"
  "https://repo1.maven.org/maven2"
)

# 下载并校验。校验这一步不能省：镜像目录不存在文件时可能返回 200 + 目录列表 HTML，
# curl 的 -f 只认 4xx/5xx，会把 HTML 当 jar 写盘，直到运行时才以
# "Failed to find data source" 这种毫不相关的报错暴露出来。因此按 zip 魔数验身。
fetch_jar() {
  local relative_path="$1" base file
  file="${relative_path##*/}"

  if [ -f "$JARS_DIR/$file" ] && head -c 2 "$JARS_DIR/$file" | grep -q 'PK'; then
    echo "已存在且校验通过，跳过：$file"
    return
  fi

  rm -f "$JARS_DIR/$file"
  for base in "${MAVEN_BASES[@]}"; do
    echo "下载：$file  ← ${base}"
    if curl -fsSL -o "$JARS_DIR/$file" "${base}/${relative_path}" \
      && head -c 2 "$JARS_DIR/$file" | grep -q 'PK'; then
      echo "  校验通过（$(stat -c%s "$JARS_DIR/$file") 字节）"
      return
    fi
    echo "  该源不可用（下载失败或内容不是 jar），换下一个" >&2
    rm -f "$JARS_DIR/$file"
  done

  echo "全部镜像均失败：$file" >&2
  return 1
}

# ---- Iceberg 运行时包 ----
ICEBERG_VERSION="1.11.0"
SPARK_LINE="3.5_2.12"   # 必须与 apache/spark 镜像的 Spark 主版本一致
fetch_jar "org/apache/iceberg/iceberg-spark-runtime-${SPARK_LINE}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_LINE}-${ICEBERG_VERSION}.jar"
fetch_jar "org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

# ---- PostgreSQL JDBC 驱动（Iceberg JDBC catalog 用）----
PG_JDBC_VERSION="42.7.13"
fetch_jar "org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar"

# ---- Kafka 连接器（Structured Streaming 读写 Kafka）----
# 生产与消费都用 Spark 执行，因此只需要一套 jar，不必在服务器上再装 Python Kafka 客户端。
# kafka-clients 用 3.9.0：4.x 的 broker 向后兼容该客户端；commons-pool2 是连接器池化的依赖，
# Spark 镜像自带的 jars 里没有。
KAFKA_CONNECTOR_VERSION="3.5.9"
KAFKA_CLIENTS_VERSION="3.9.0"
COMMONS_POOL2_VERSION="2.12.0"
fetch_jar "org/apache/spark/spark-sql-kafka-0-10_2.12/${KAFKA_CONNECTOR_VERSION}/spark-sql-kafka-0-10_2.12-${KAFKA_CONNECTOR_VERSION}.jar"
fetch_jar "org/apache/spark/spark-token-provider-kafka-0-10_2.12/${KAFKA_CONNECTOR_VERSION}/spark-token-provider-kafka-0-10_2.12-${KAFKA_CONNECTOR_VERSION}.jar"
fetch_jar "org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"
fetch_jar "org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"

# ---- OpenJDK 17 ----
JDK_VERSION="17.0.1"
JDK_DIR="$SPARK_DIR/jdk17"
if [ -x "$JDK_DIR/bin/java" ]; then
  echo "已存在，跳过：jdk17"
else
  echo "下载：OpenJDK ${JDK_VERSION}"
  archive="/tmp/openjdk-${JDK_VERSION}.tar.gz"
  curl -fsSL -o "$archive" \
    "https://repo.huaweicloud.com/openjdk/${JDK_VERSION}/openjdk-${JDK_VERSION}_linux-x64_bin.tar.gz"
  mkdir -p "$JDK_DIR"
  tar -xzf "$archive" -C "$JDK_DIR" --strip-components=1
  rm -f "$archive"
fi

echo "---- jars ----"
ls -lh "$JARS_DIR"
echo "---- jdk ----"
"$JDK_DIR/bin/java" -version
