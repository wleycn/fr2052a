r"""把 Iceberg gold 层的报送报表导出到 Server 1 的 PostgreSQL（ADS 服务层）。

为什么需要这一步：架构上 Iceberg 承载数据湖（ref / ODS / OWD / OWS / Gold），
PostgreSQL 只承载报送服务层。dbt 在数据湖里算出报表，再由本作业导出到关系库，
供报送服务与下游查询使用。

PG 侧表结构由本作业按 gold 模型的结构自动创建，不手写一份 DDL ——
两处各写一份结构定义必然漂移。

关于覆盖写的两种模式（这是本文件最需要读懂的一段）：

Spark 的 JDBC 覆盖写有两个分支，由 `truncate` 选项决定，默认走**删除重建**：

    truncate=false（默认）  DROP TABLE + CREATE TABLE，再灌数
                            → 表被换掉，挂在表上的东西（授权、触发器、索引、
                              库侧加的列）全部消失，且不报任何错
    truncate=true           TRUNCATE TABLE，再灌数
                            → 表本身不动，只有数据被换掉，上述元数据全部保留

文档出处：Spark SQL Guide / JDBC To Other Databases 的 Data Source Option 表，
`truncate` 一项写明「causes Spark to truncate an existing table instead of dropping
and recreating it. This can be more efficient, and prevents the table metadata
(e.g., indices) from being removed」。

实测（本机 Spark 3.5.9 + PostgreSQL 18 探测，表上有额外列 + 一个角色授权）：
    默认      → 额外列消失、角色授权消失
    truncate=true → 额外列保留、角色授权保留、新行取到列的默认值
                    （能取到默认值恰好证明写的是 INSERT 而不是重建后的全新表）

同一份文档也写明 truncate=true 的代价：「it will not work in some cases, such as
when the new data has a different schema」。实测确认：模型多一列时直接报
`Column extra not found in schema` 并中止，**表与授权原样保留**。
这个失败方式是可接受的 —— 报错、不破坏；比起「悄悄把授权清掉」要好得多。

所以本作业的选择是：truncate=true 保住元数据，代价是模型结构变了必须走显式迁移。
本作业在写入前先比对模型列与目标表列，不一致就带着「该补哪条迁移」的提示直接失败，
而不是把这个错留给 Spark 的原始报错。这与仓库红线「schema evolution 只走迁移，
禁止隐式加列」是同一条纪律。

导出后逐表回读行数，作为对"确实写进去了"的独立验证。

用法（Server 2，经 spark-submit 包装脚本执行，环境变量由包装脚本透传）：
    bash spark-submit-fr2052a.sh \
        /opt/fr2052a-app/python/exporters/export_gold_to_pg.py
"""

from __future__ import annotations

import os
import sys
from typing import Any

from pyspark.sql import DataFrame, SparkSession

GOLD_SCHEMA = "gold"
ADS_SCHEMA = "ads"

TABLES = (
    "ads_fr2052a_report",
    "ads_fr2052a_detail",
    "ads_gl_reconciliation",
)


def jdbc_url() -> str:
    """拼 PostgreSQL 的 JDBC 连接串。"""
    host = os.environ["SERVER1_HOST"]
    database = os.environ["POSTGRES_DB"]
    return f"jdbc:postgresql://{host}:5432/{database}"


def connection_properties() -> dict[str, str]:
    """JDBC 连接属性。凭据从环境变量取，不写进代码。"""
    return {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }


def target_columns(spark: SparkSession, url: str, table: str, properties: dict[str, Any]) -> list[str] | None:
    """取目标表现有列；**只有表确实不存在**时才返回 None（首次导出）。

    「表不存在」与「表存在但读不到」必须分开：后者（权限、瞬时故障、catalog 未刷新）如果也
    变成 None，下面那层防漂移比对会整体跳过、直接覆盖写 —— 那正是本函数要防的事。

    判存在用 `pg_catalog` 而不是 `information_schema`：后者只列出当前角色有权限的对象，
    「表在、但没权限读」会被它藏成「不存在」，于是退化回静默跳过比对的老路。实测过：
    无权限角色看 `information_schema.tables` 是 0 行，看 `pg_class` 是 1 行。
    """
    schema, _, name = table.partition(".")
    probe = (
        "(SELECT 1 FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        f"WHERE n.nspname = '{schema}' AND c.relname = '{name}' "
        "AND c.relkind IN ('r', 'p', 'v', 'm', 'f')) AS probe"
    )
    if not spark.read.jdbc(url, probe, properties=properties).take(1):
        return None
    return spark.read.jdbc(url, f"(SELECT * FROM {table} WHERE 1 = 0) AS probe", properties=properties).columns


def schema_diff(frame: DataFrame, existing: list[str]) -> tuple[list[str], list[str]]:
    """返回 (模型有而表没有的列, 表有而模型没有的列)。"""
    missing = [name for name in frame.columns if name not in existing]
    extra = [name for name in existing if name not in frame.columns]
    return missing, extra


def main() -> int:
    """把 gold 层的报送表导出到 PostgreSQL 的 ads 层，覆盖写以保住表上的授权与约束。"""
    url = jdbc_url()
    properties = connection_properties()

    spark = SparkSession.builder.appName("fr2052a-export-gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"导出 gold 层报表到 PostgreSQL（{url.split('@')[-1]}）")
    failures: list[str] = []
    for table in TABLES:
        target = f"{ADS_SCHEMA}.{table}"
        try:
            frame = spark.table(f"{GOLD_SCHEMA}.{table}")
            source_rows = frame.count()

            existing = target_columns(spark, url, target, properties)
            if existing is not None:
                missing, extra = schema_diff(frame, existing)
                if missing or extra:
                    print(
                        f"  [FAIL] {target:<28} 模型结构与目标表不一致，需要先写迁移（sql/postgres/ 下加 ALTER TABLE）"
                    )
                    if missing:
                        print(f"           模型独有列：{missing} → 目标表需 ADD COLUMN")
                    if extra:
                        print(f"           目标表独有列：{extra} → 确认是否 DROP COLUMN 或补进模型")
                    failures.append(target)
                    continue

            # truncate=true：保留表上的授权/触发器/索引与库侧列，只换数据。
            # 不加这个选项就是默认的 DROP + CREATE，会让上面那些东西全部消失。
            (frame.write.mode("overwrite").option("truncate", "true").jdbc(url, target, properties=properties))
            # 独立回读：写成功不等于写对了
            written_rows = spark.read.jdbc(url, target, properties=properties).count()
            ok = source_rows == written_rows
            if not ok:
                failures.append(target)
            print(f"  [{'OK' if ok else 'FAIL'}] {target:<28} gold {source_rows} 行，PG 回读 {written_rows} 行")
        except Exception as error:  # noqa: BLE001 - 逐表失败不中断整批，最后统一汇总
            failures.append(target)
            print(f"  [FAIL] {target:<28} {type(error).__name__}: {error}")

    print()
    if failures:
        print(f"导出失败：{failures}")
        return 1
    print(f"导出完成：{len(TABLES)} 张表全部回读一致")
    return 0


if __name__ == "__main__":
    sys.exit(main())
