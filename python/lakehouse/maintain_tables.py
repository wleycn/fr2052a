# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""Iceberg 表维护 —— 快照保留策略与文件合并。

解决两个反向的失守：策略不设就是无限保留（元数据与快照一直涨），
从不合并就是小文件堆积（每次写入都新增文件，查询要打开成百上千个小文件）。

**默认是演练模式**：不加 --apply 只打印「会做什么」，一行都不改。
快照过期是不可逆的（过期之后就回不到那个时点了），
按仓库红线，破坏性操作必须有人显式授权 —— 就是那个 --apply。

    bash spark-submit-fr2052a.sh .../maintain_tables.py                    # 演练
    bash spark-submit-fr2052a.sh .../maintain_tables.py --apply            # 真做

保留策略取值（与 python/lakehouse/owd_scd2.py 创建历史表时设的一致）：
    快照保留 7 天、至少留 10 个；元数据文件提交后清理、最多留 20 版。
    7 天覆盖一个完整月结周期内的回溯需求，同时不让存储无限增长。
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession

NAMESPACES = ("ref", "bronze", "silver", "gold")
RETENTION_DAYS = 7
MIN_SNAPSHOTS_TO_KEEP = 10
METADATA_PREVIOUS_VERSIONS_MAX = 20

RETENTION_PROPERTIES = {
    "history.expire.max-snapshot-age-ms": str(RETENTION_DAYS * 24 * 60 * 60 * 1000),
    "history.expire.min-snapshots-to-keep": str(MIN_SNAPSHOTS_TO_KEEP),
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": str(METADATA_PREVIOUS_VERSIONS_MAX),
}

# 小于这个文件数的表不做合并 —— 合并本身也要读写一遍数据，
# 对已经很整齐的表做合并只是白花算力。
MIN_INPUT_FILES_FOR_COMPACTION = "5"


def parse_args() -> argparse.Namespace:
    """解析命令行参数：默认只演练，加 --apply 才真正动手。"""
    parser = argparse.ArgumentParser(description="Iceberg 表维护")
    parser.add_argument("--apply", action="store_true", help="真正执行；不加只演练")
    parser.add_argument("--namespaces", nargs="*", default=list(NAMESPACES))
    parser.add_argument(
        "--skip-properties",
        action="store_true",
        help="跳过表属性设置（属性设置是幂等的非破坏操作，通常不需要跳过）",
    )
    return parser.parse_args()


def list_tables(spark: SparkSession, namespaces: list[str]) -> list[str]:
    """列出目标命名空间下的全部表。"""
    tables: list[str] = []
    for namespace in namespaces:
        rows = spark.sql(f"SHOW TABLES IN {namespace}").collect()
        for row in rows:
            tables.append(f"{namespace}.{row['tableName']}")
    return sorted(tables)


def snapshot_summary(spark: SparkSession, table: str) -> tuple[int, str | None, str | None]:
    """返回快照条数、最早与最新快照时间，作为保留策略的输入。"""
    rows = spark.sql(
        f"SELECT count(*) AS n, min(committed_at) AS oldest, max(committed_at) AS newest FROM {table}.snapshots"
    ).collect()
    if not rows:
        return 0, None, None
    row = rows[0]
    return row["n"] or 0, str(row["oldest"]) if row["oldest"] else None, str(row["newest"]) if row["newest"] else None


def main() -> int:
    """对 Iceberg 表做快照保留、元数据清理与小文件合并。"""
    args = parse_args()
    spark = SparkSession.builder.appName("fr2052a-maintain-tables").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    mode = "执行" if args.apply else "演练（不改动，加 --apply 才真做）"
    print(f"Iceberg 表维护：{mode}")
    print(
        f"保留策略：快照 {RETENTION_DAYS} 天 / 至少 {MIN_SNAPSHOTS_TO_KEEP} 个，"
        f"元数据最多 {METADATA_PREVIOUS_VERSIONS_MAX} 版\n"
    )

    tables = list_tables(spark, args.namespaces)
    print(f"待处理 {len(tables)} 张表")

    failures: list[str] = []
    for table in tables:
        try:
            count, oldest, newest = snapshot_summary(spark, table)
            print(f"\n  {table}")
            print(f"    快照 {count} 个（{oldest} → {newest}）")

            if not args.skip_properties:
                assignments = ", ".join(f"'{name}' = '{value}'" for name, value in RETENTION_PROPERTIES.items())
                if args.apply:
                    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ({assignments})")
                    print("    [OK] 保留策略已设置")
                else:
                    print(f"    [演练] 将设置保留策略：{list(RETENTION_PROPERTIES)}")

            if count > MIN_SNAPSHOTS_TO_KEEP:
                if args.apply:
                    spark.sql(
                        f"""
                        CALL lakehouse.system.expire_snapshots(
                            table => '{table}',
                            older_than => TIMESTAMP '{oldest}',
                            retain_last => {MIN_SNAPSHOTS_TO_KEEP}
                        )
                        """
                    )
                    remaining, _, _ = snapshot_summary(spark, table)
                    print(f"    [OK] 已过期旧快照，剩余 {remaining} 个")
                else:
                    print(f"    [演练] 将过期 {oldest} 之前、且只保留最近 {MIN_SNAPSHOTS_TO_KEEP} 个")
            else:
                print(f"    [跳过] 快照数不超过 {MIN_SNAPSHOTS_TO_KEEP}，无需过期")

            if args.apply:
                spark.sql(
                    f"""
                    CALL lakehouse.system.rewrite_data_files(
                        table => '{table}',
                        options => map('min-input-files', '{MIN_INPUT_FILES_FOR_COMPACTION}')
                    )
                    """
                )
                print(f"    [OK] 小文件合并已执行（输入文件数 ≥ {MIN_INPUT_FILES_FOR_COMPACTION} 才重写）")
            else:
                print(f"    [演练] 将合并小文件（输入文件数 ≥ {MIN_INPUT_FILES_FOR_COMPACTION}）")
        except Exception as error:  # noqa: BLE001 - 单表失败不中断，末尾统一汇总
            failures.append(table)
            print(f"    [FAIL] {type(error).__name__}: {error}")

    print()
    if failures:
        print(f"失败的表：{failures}")
        return 1
    print(f"完成：{len(tables)} 张表处理完毕（{mode}）")
    if not args.apply:
        print("这是演练结果。确认无误后加 --apply 执行 —— 快照过期不可逆。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
