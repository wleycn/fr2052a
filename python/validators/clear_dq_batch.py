# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""清掉某一批次的数据质量结果，让这个批次可以重跑。

为什么需要单独一步：

    DQ 结果按批次追加进 ads.ads_fr2052a_validation_log，跨批次保留历史 ——
    这是想要的。但「按批次」只有在同一批次重跑时先删掉旧行才成立：不删就变成
    重跑一次多一份，于是「本批次有几条 ERROR」这类计数会随重跑次数翻倍。
    实测踩过：同一个批次跑 8 次，表里就有 160 行（20 条规则 × 8），
    而计数查询看不出来哪 20 行才是这一批的结果，也不会报错。

为什么删除动作放在这里而不是规则脚本里：

    规则脚本跑在 Spark 容器里，通过 JDBC 读写 Iceberg 与 PostgreSQL。
    JDBC 能读表、能整表覆盖写，但不能按条件删行。所以「先删同批次旧行」
    这一步交给宿主上的 venv（psycopg2）做，紧接着再让 Spark 追加本批次结果。

用法（Server 2，用 venv 里的 python 直接跑）：
    ./venv/bin/python /opt/fr2052a-app/python/validators/clear_dq_batch.py \
        --batch-id BATCH-20260916-001
"""

from __future__ import annotations

import argparse
import os
import sys

import psycopg2

LOG_TABLE = "ads.ads_fr2052a_validation_log"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="清掉某批次的数据质量结果")
    parser.add_argument("--batch-id", required=True, help="要清空的批次号")
    return parser.parse_args()


def pg_connection():
    return psycopg2.connect(
        host=os.environ["SERVER1_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def main() -> int:
    args = parse_args()
    connection = pg_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"DELETE FROM {LOG_TABLE} WHERE batch_id = %s", (args.batch_id,))
            deleted = cursor.rowcount
            cursor.execute(f"SELECT count(*) FROM {LOG_TABLE}")
            counted = cursor.fetchone()
        connection.commit()
    finally:
        connection.close()

    print(
        f"批次 {args.batch_id}：删除 {deleted} 行；"
        f"审计表剩余 {counted[0] if counted else 0} 行（其他批次的历史保留）。"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
