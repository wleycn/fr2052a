#!/usr/bin/env bash
# 下载 Spark 侧所需的第三方二进制依赖到 ./spark/ 下。
#
# 为什么不用镜像仓库：Docker 镜像源实测被限速（约 150 KB/s），
# 而国内软件镜像站可达 1.6 MB/s。Java 侧依赖一律走软件镜像。
#
# 用法：在 Server 2 的 ~/fr2052a-infra 下执行 bash fetch-deps.sh
#
# 依赖清单：
#   spark/jars/   Iceberg Spark 运行时包 + AWS bundle
#   spark/jdk17/  OpenJDK 17（Iceberg 1.11 要求 Java 17，而 Spark 镜像自带 Java 11）

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
SPARK_DIR="$BASE_DIR/spark"

# ---- Iceberg 运行时包 ----
MAVEN_BASE="https://maven.aliyun.com/repository/public/org/apache/iceberg"
ICEBERG_VERSION="1.11.0"
SPARK_LINE="3.5_2.12"   # 必须与 apache/spark 镜像的 Spark 主版本一致

JARS_DIR="$SPARK_DIR/jars"
mkdir -p "$JARS_DIR"
cd "$JARS_DIR"
for artifact in "iceberg-spark-runtime-${SPARK_LINE}" "iceberg-aws-bundle"; do
  file="${artifact}-${ICEBERG_VERSION}.jar"
  if [ -f "$file" ]; then
    echo "已存在，跳过：$file"
    continue
  fi
  echo "下载：$file"
  curl -fsSLO "$MAVEN_BASE/${artifact}/${ICEBERG_VERSION}/${file}"
done

# ---- PostgreSQL JDBC 驱动（Iceberg JDBC catalog 用）----
JARS_DIR="$SPARK_DIR/jars"
PG_JDBC_VERSION="42.7.13"
PG_JDBC_FILE="postgresql-${PG_JDBC_VERSION}.jar"
if [ -f "$JARS_DIR/$PG_JDBC_FILE" ]; then
  echo "已存在，跳过：$PG_JDBC_FILE"
else
  echo "下载：$PG_JDBC_FILE"
  curl -fsSL -o "$JARS_DIR/$PG_JDBC_FILE" \
    "https://maven.aliyun.com/repository/public/org/postgresql/postgresql/${PG_JDBC_VERSION}/${PG_JDBC_FILE}"
fi

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
