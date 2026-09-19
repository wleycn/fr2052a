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

import argparse
import hashlib
import os
import re
import sys
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

GOLD_SCHEMA = "gold"
ADS_SCHEMA = "ads"

# 视为「已报送」的台账状态：这两个状态下库内数据与已报送文件必须保持一致。
SUBMITTED_STATES = ("SUBMITTED", "ACCEPTED")

TABLES = (
    "ads_fr2052a_report",
    "ads_fr2052a_detail",
    "ads_gl_reconciliation",
)


def parse_args() -> argparse.Namespace:
    """解析命令行参数。

    `--batch-id` 默认取环境变量 `BATCH_ID`：跑批脚本在宿主侧把它作为参数传进来，
    不依赖容器内的环境变量透传。批次号只允许字母数字与短横线 —— 它会被拼进 SQL 字面量，
    白名单校验比转义更不容易出错。
    """
    parser = argparse.ArgumentParser(description="把 gold 层报送表导出到 PostgreSQL 的 ads 层")
    parser.add_argument("--batch-id", default=os.environ.get("BATCH_ID", ""), help="本批批次号，用于核对运行上下文")
    parser.add_argument(
        "--allow-after-submission",
        action="store_true",
        default=os.environ.get("ALLOW_EXPORT_AFTER_SUBMISSION", "") not in ("", "0", "false", "False"),
        help="允许覆盖「已报送且内容已变」的报告期（重述流程用；默认禁止，见 preflight 第 5 条）",
    )
    args = parser.parse_args()
    if args.batch_id and not re.fullmatch(r"[A-Za-z0-9-]+", args.batch_id):
        parser.error(f"批次号只允许字母、数字与短横线，收到：{args.batch_id!r}")
    return args


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


def _submitted_periods(spark: SparkSession, url: str, properties: dict[str, Any]) -> set[str]:
    """已报送（SUBMITTED / ACCEPTED）的报告期。报送台账只落在 PG，走 JDBC 读。"""
    ledger = f"{ADS_SCHEMA}.ads_fr2052a_submission"
    if target_columns(spark, url, ledger, properties) is None:
        return set()
    frame = spark.read.jdbc(
        url,
        f"(SELECT DISTINCT report_date, submission_status FROM {ledger}) AS submitted",
        properties=properties,
    )
    return {str(row["report_date"]) for row in frame.collect() if row["submission_status"] in SUBMITTED_STATES}


def _content_fingerprint(frame: DataFrame, columns: list[str], period: str) -> str:
    """按报告期算内容指纹：行序无关（排序后拼接），列序固定（只取两侧共有的列）。

    比的是「库内现值」与「本次要写的值」，不是「有没有报送过」：重跑而数据未变时指纹相同、
    照常放行；只有内容真的变了才拦 —— 那才是会让库内数据与已报送文件分叉的形态。
    列取交集而不是 gold 的全部列：库里可能持有后加的迁移列，那不属于内容差异。
    """
    rows = frame.filter(F.col("report_date").cast("string") == period).select(*columns).toJSON().collect()
    return hashlib.sha256("\n".join(sorted(rows)).encode("utf-8")).hexdigest()


def _submitted_period_drift(spark: SparkSession, url: str, properties: dict[str, Any], periods: list[str]) -> list[str]:
    """返回「已报送期里，库内现值与本次要写的值不一致」的「表@报告期」清单（空 = 一致）。"""
    drifted: list[str] = []
    for table in TABLES:
        existing = target_columns(spark, url, f"{ADS_SCHEMA}.{table}", properties)
        if existing is None:
            continue  # 库里还没有这张表，谈不上「与已报送内容分叉」
        gold = spark.table(f"{GOLD_SCHEMA}.{table}")
        pg = spark.read.jdbc(url, f"(SELECT * FROM {ADS_SCHEMA}.{table}) AS existing", properties=properties)
        shared = [column for column in gold.columns if column in existing]
        for period in periods:
            if _content_fingerprint(gold, shared, period) != _content_fingerprint(pg, shared, period):
                drifted.append(f"{table}@{period}")
    return drifted


def _scalar(spark: SparkSession, url: str, properties: dict[str, Any], query: str) -> int:
    """跑一条只返回一个整数的查询；走 JDBC 子查询，不把整表拉到驱动端。"""
    frame = spark.read.jdbc(url, f"({query}) AS probe", properties=properties)
    return int(frame.collect()[0][0])


def preflight(
    spark: SparkSession,
    url: str,
    properties: dict[str, Any],
    batch_id: str,
    allow_after_submission: bool = False,
) -> list[str]:
    """覆盖写之前的**业务前提**检查；返回未通过的条目（空 = 可以写）。

    为什么必须在写之前检：三张表是 `truncate=true` 覆盖写，写错了没有回退路径。
    结构比对只能证明「列对得上」，证明不了「这批数据该不该写进去」。以下四种情况都会让一次
    「结构完全正确」的覆盖写把好数据换成坏数据：

      1. gold 里有空表 —— 上游没跑完，不是「本期确实没有数据」
      2. 本批的运行上下文缺失或已失败 —— 不知道自己在写哪一批
      3. 对账里有 FAIL 行 —— 报表与总账都没对上，不该进服务层
      4. 报表与对账的报告期集合不一致 —— 只导了一半期
      5. 已报送报告期的内容变了 —— 库内数据会与已报送文件分叉，且没有回退路径

    第 5 条的判据是「内容指纹变化」而不是「有没有报送过」：本演示的日批本身就是可重跑的，
    拿「已报送」直接拦会把正常重跑全部挡掉；真正需要拦的是「内容变了还悄悄覆盖」——
    例如上游修正、口径调整、模型改造之后重跑。真出现内容变化，正确路径是重述流程
    （restate-capture → rebuild → restate-register），而不是直接覆盖已报送期的数据。
    重述流程会显式带上 --allow-after-submission（或环境变量 ALLOW_EXPORT_AFTER_SUBMISSION=1）。

    检完再写：任一条不过就整批退出非零，**一张表都不碰**。
    """
    failures: list[str] = []

    for table in TABLES:
        rows = spark.table(f"{GOLD_SCHEMA}.{table}").count()
        if rows == 0:
            failures.append(f"gold.{table} 是空表，先确认上游跑完再导出")
        else:
            print(f"  [OK]   gold.{table:<26} {rows} 行")

    if not batch_id:
        failures.append("未提供批次号（--batch-id 或 BATCH_ID 环境变量），无法核对本批运行上下文")
    else:
        context_rows = _scalar(
            spark,
            url,
            properties,
            "SELECT count(*) FROM ads.ads_pipeline_run_context "
            f"WHERE batch_id = '{batch_id}' AND status IN ('OPEN', 'RUNNING', 'SUCCEEDED')",
        )
        failed_rows = _scalar(
            spark,
            url,
            properties,
            f"SELECT count(*) FROM ads.ads_pipeline_run_context WHERE batch_id = '{batch_id}' AND status = 'FAILED'",
        )
        if context_rows == 0 or failed_rows > 0:
            failures.append(
                f"本批运行上下文不成立（batch_id={batch_id}，成立 {context_rows} 行，失败 {failed_rows} 行）"
            )
        else:
            print(f"  [OK]   运行上下文 {batch_id} 成立")

    # 以下两项查的是 **gold（本次要写的那份数据）**，不是 PG 里上一批的存量 ——
    # 拿存量判新数据等于用旧结论放行新批次。
    recon_fails = spark.table(f"{GOLD_SCHEMA}.ads_gl_reconciliation").filter("status = 'FAIL'").count()
    if recon_fails > 0:
        failures.append(f"对账有 {recon_fails} 行 FAIL，先查清再导出")
    else:
        print("  [OK]   对账无 FAIL 行")

    report_periods = {
        str(row["report_date"])
        for row in spark.sql(f"SELECT DISTINCT report_date FROM {GOLD_SCHEMA}.ads_fr2052a_report").collect()
    }
    recon_periods = {
        str(row["report_date"])
        for row in spark.sql(f"SELECT DISTINCT report_date FROM {GOLD_SCHEMA}.ads_gl_reconciliation").collect()
    }
    if report_periods != recon_periods:
        failures.append(f"报表与对账的报告期集合不一致（报表 {sorted(report_periods)}，对账 {sorted(recon_periods)}）")
    else:
        print(f"  [OK]   报表与对账的报告期集合一致（{len(report_periods)} 期）")

    # 第 5 条前提：已报送报告期的内容不得变化（判据与理由见 docstring）。
    if allow_after_submission:
        print("  [WARN] 已按开关跳过「已报送期内容一致性」检查：确认本次是重述或口径变更")
    else:
        submitted = _submitted_periods(spark, url, properties)
        at_risk = sorted(period for period in report_periods if period in submitted)
        if not at_risk:
            print(f"  [OK]   本次报告期都不涉及已报送期（库内已报送 {len(submitted)} 期）")
        else:
            drifted = _submitted_period_drift(spark, url, properties, at_risk)
            if drifted:
                failures.append(
                    f"已报送期 {at_risk} 的内容与库内现值不一致（{drifted}）：覆盖写会让库内数据与已报送文件分叉。"
                    "正确路径是重述流程（restate-capture → rebuild → restate-register）；"
                    "确需直接覆盖时加 --allow-after-submission 并在报送说明里写明原因"
                )
            else:
                print(f"  [OK]   已报送期 {at_risk} 的内容与库内一致（重跑幂等）")

    return failures


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

    # 覆盖写是不可逆的：先检业务前提，再动任何一张表。
    # 检查与写入分两段，正是为了让「不合格」表现为「一张表都没改」，而不是「改了一半」。
    print("覆盖写前置检查：")
    args = parse_args()
    blockers = preflight(spark, url, properties, args.batch_id, args.allow_after_submission)
    if blockers:
        print()
        print("导出未开始，一张表都没写。未通过的前提：")
        for item in blockers:
            print(f"  [FAIL] {item}")
        return 1
    print()

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
