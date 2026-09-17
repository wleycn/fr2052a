# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""OWD 层 SCD2 版本化 —— 把「当前快照」变成「可回溯的版本序列」。

背景：silver.owd_* 是 dbt 全量重建的当前快照，每轮跑批覆盖，看不出某一行什么时候被改过。
重述要能回答「改之前长什么样」，靠快照本身答不了。本作业为每张 OWD 表维护一份
silver.owd_*_history，按 SCD2 语义记录版本：

    变更类型   处理方式
    新增键     插入新版本（record_version = 1，reason = 首次的 ORIGINAL）
    已变更键   先把当前版本置为失效（end_date = 生效日 - 1，且不早于该版本生效日），
               再插入新版本（record_version = 上一版 + 1，reason = 本轮传入的原因）
    已删除键   把当前版本置为失效（不回插新版本 —— 删除本身就是一种变化）
    未变更键   不动

版本区间用 begin_date / end_date 表示，**当前有效版本的 end_date 为空**。
为什么不用 9999-12-31 当哨兵值：它长得像一个真实日期，任何按日期范围过滤
或做区间聚合的查询都会把它当真，于是「当前有效」这个状态反而更难表达。
空值在 SQL 里语义明确（未知/未结束），并且 `end_date IS NULL` 是最直白的判据。
is_active 是 end_date 的冗余列，方便建索引与写查询；两者必须一致，
一旦不一致就说明有人绕过本作业改了历史表 —— 因此校验脚本会把不一致当错误报出来。

区间跑在**处理时间**上，不是报告日上：
同一个报告日的两个版本，报告日都是 2026-09-16，用报告日当前后边界会算出
「生效日晚于失效日」的反向区间。版本的有效区间描述的是「这份数据从哪一天起
被替换掉」，所以用生效日（effective date）：
    v1  begin 2026-09-16  end 2026-09-16
    v2  begin 2026-09-17  end 空（当前有效）
生效日由 --effective-date 显式传入，不读系统当前时间 ——
否则同一份数据在不同时刻跑会算出不同的区间，无法复现。
调用方应传**处理日**（日批取报告日次日），两个调用方必须用同一套约定：
一个用报告日、一个用处理日，后跑的那次会把先写下的版本压成零长度区间。

变更判定用 row_hash：业务列拼字符串取 MD5。ETL 元数据列（etl_load_timestamp 等）不参与，
因为它们每轮都变，纳入比较会让「全部行都变了」。

历史表由本作业按 OWD 表的实际结构生成（CTAS ... WHERE 1 = 0），不手写第二份列清单 ——
OWD 加列时若忘改历史表，SCD2 会静默漏掉新列的变化。

Iceberg 表属性（快照保留与元数据清理）在历史表创建时一并设好：
不设就是无限保留，元数据和快照会一直涨。

运行（Server 2，经 spark-submit 包装脚本执行）：
    bash spark-submit-fr2052a.sh /opt/fr2052a-app/python/lakehouse/owd_scd2.py \
        --report-date 2026-09-16 --reason ORIGINAL
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession

OWD_TABLES = (
    "owd_deposits",
    "owd_secured_financing",
    "owd_loans",
    "owd_securities",
    "owd_derivatives",
    "owd_off_bs",
    "owd_gl_entries",
)

# 自然键：源系统 + 源记录号 + 报告日。同一源记录在不同报告日是两条独立的记录，
# 因为本演示的 ODS 是「按报告日的头寸快照」，而不是事件流。
KEY_COLUMNS = ("source_system", "source_record_id", "report_date")

VERSION_COLUMNS = (
    "begin_date",
    "end_date",
    "is_active",
    "last_modified_reason",
    "record_version",
    "row_hash",
)

# 不参与变更比较的列：它们是入湖/加工元数据，每轮都会变。
EXCLUDED_FROM_HASH = ("etl_load_timestamp", "etl_batch_id")

TABLE_PROPERTIES = (
    "'format-version' = '2', "
    # 快照保留 7 天、至少留 10 个：够审计回溯，又不至于无限增长。
    "'history.expire.max-snapshot-age-ms' = '604800000', "
    "'history.expire.min-snapshots-to-keep' = '10', "
    # 元数据文件也清理，否则每次提交都留一份 manifest。
    "'write.metadata.delete-after-commit.enabled' = 'true', "
    "'write.metadata.previous-versions-max' = '20'"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OWD 层 SCD2 版本化")
    parser.add_argument("--report-date", required=True, help="报告日，作为版本生效日")
    parser.add_argument(
        "--effective-date",
        default=None,
        help="版本生效日（处理日），默认取报告日；改版本区间边界时用它，不要在定时任务里读系统时间",
    )
    parser.add_argument(
        "--reason",
        default="ORIGINAL",
        choices=("ORIGINAL", "CORRECTION", "RESTATEMENT"),
        help="本轮新增版本的变更原因",
    )
    parser.add_argument("--tables", nargs="*", default=list(OWD_TABLES), help="只处理指定表")
    return parser.parse_args()


def history_table(table: str) -> str:
    return f"silver.{table}_history"


def ensure_history_table(spark: SparkSession, table: str) -> None:
    """历史表不存在则按当前 OWD 结构生成（含版本列），已存在则原样保留。"""
    history = history_table(table)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {history}
        USING iceberg
        TBLPROPERTIES ({TABLE_PROPERTIES})
        AS
        SELECT
            t.*,
            CAST(NULL AS DATE) AS begin_date,
            CAST(NULL AS DATE) AS end_date,
            CAST(NULL AS BOOLEAN) AS is_active,
            CAST(NULL AS STRING) AS last_modified_reason,
            CAST(NULL AS INT) AS record_version,
            CAST(NULL AS STRING) AS row_hash
        FROM silver.{table} t
        WHERE 1 = 0
        """
    )
    migrate_legacy_columns(spark, history)


def migrate_legacy_columns(spark: SparkSession, history: str) -> None:
    """把老结构迁到新结构（幂等，只在检测到老列时动手）。

    老结构：valid_from_date / valid_to_date / is_current_flag，
            且当前有效版本用 9999-12-31 这个哨兵值占位。
    新结构：begin_date / end_date / is_active，当前有效版本的 end_date 为空。

    改列名而不是加新列：新旧语义一一对应，同时存在两套会让「哪一套才算数」变成
    每个使用方各自判断，早晚判错一次。
    """
    existing = set(spark.table(history).columns)
    renames = (
        ("valid_from_date", "begin_date"),
        ("valid_to_date", "end_date"),
        ("is_current_flag", "is_active"),
    )
    for old_name, new_name in renames:
        if old_name in existing:
            spark.sql(f"ALTER TABLE {history} RENAME COLUMN {old_name} TO {new_name}")
    if any(old in existing for old, _ in renames):
        # 哨兵值换成空值：只有当前有效版本带哨兵值，其余版本本来就有真实失效日
        spark.sql(
            f"UPDATE {history} SET end_date = NULL WHERE is_active AND end_date IS NOT NULL"
        )


def table_columns(spark: SparkSession, table: str) -> tuple[str, ...]:
    """OWD 表的全部列。写历史表时按这份清单写，一列都不能少。"""
    return tuple(spark.table(f"silver.{table}").columns)


def hash_columns(columns: tuple[str, ...]) -> tuple[str, ...]:
    """参与变更比较的列：去掉每轮都会变的 ETL 元数据列。

    注意区分「参与比较的列」与「要写入的列」：前者是后者的子集。
    把两者混用会让写入少一列，直接报 CANNOT_FIND_DATA。
    """
    return tuple(name for name in columns if name not in EXCLUDED_FROM_HASH)


def hash_expression(columns: tuple[str, ...]) -> str:
    """业务列拼起来取 MD5。用 coalesce 避免 NULL 让整行哈希变 NULL。"""
    parts = ", '|', ".join(f"coalesce(cast({name} as string), '~')" for name in columns)
    return f"md5(concat_ws('', {parts}))"


def version_table(spark: SparkSession, table: str, args: argparse.Namespace) -> dict[str, int]:
    """对单张 OWD 表做一轮 SCD2 归并，返回各变更类型的行数。

    实现方式：把「本轮跑完之后版本表应该长什么样」写成一个 SELECT，整表覆盖写回。

    为什么不用「先 MERGE 关闭旧版本、再 INSERT 新版本」的两段式：
    两段式要求判断集必须在改动历史表之前固化，因为判断集读的正是被改动的表。
    这依赖一条隐式约定，实测栽过两次 ——
      第一次：判断集是惰性视图，MERGE 之后再读被重新求值，所有键都被当成新键；
      第二次：把上一版版本号用 UNION ALL 拼进判断集，插入时取到的是字面量 0。
    两次的错误形态相同：数据行数对，但版本号与变更原因是错的。而变更原因就是审计本身。
    整表重算把读与写彻底分开：一个 SELECT 定义新状态，没有中间可变状态，没有时序依赖。
    代价是每轮重写整张版本表 —— 版本表的量级是「每键版本数很少」，这个代价可以接受。
    """
    ensure_history_table(spark, table)
    # 生效日：版本区间的边界基准，默认取报告日。不读系统当前时间 ——
    # 同一份数据在不同时刻跑必须算出同样的区间，否则无法复现。
    effective_date = args.effective_date or args.report_date
    columns = table_columns(spark, table)
    hashed = hash_columns(columns)
    history = history_table(table)

    business = ", ".join(columns)
    incoming = f"SELECT {business}, {hash_expression(hashed)} AS row_hash FROM silver.{table}"
    active = (
        f"SELECT {', '.join(list(columns) + list(VERSION_COLUMNS))} "
        f"FROM {history} WHERE is_active"
    )
    match_ai = " AND ".join(f"a.{name} = i.{name}" for name in KEY_COLUMNS)
    match_ia = " AND ".join(f"i.{name} = a.{name}" for name in KEY_COLUMNS)
    key0 = KEY_COLUMNS[0]

    counts = {
        "new": spark.sql(
            f"WITH incoming AS ({incoming}), active AS ({active}) "
            f"SELECT count(*) AS n FROM incoming i LEFT JOIN active a ON {match_ai} "
            f"WHERE a.{key0} IS NULL"
        ).first()["n"],
        "changed": spark.sql(
            f"WITH incoming AS ({incoming}), active AS ({active}) "
            f"SELECT count(*) AS n FROM incoming i JOIN active a ON {match_ai} "
            f"WHERE i.row_hash <> a.row_hash"
        ).first()["n"],
        "deleted": spark.sql(
            f"WITH incoming AS ({incoming}), active AS ({active}) "
            f"SELECT count(*) AS n FROM active a LEFT JOIN incoming i ON {match_ia} "
            f"WHERE i.{key0} IS NULL"
        ).first()["n"],
    }

    business_a = ", ".join(f"a.{name}" for name in columns)
    business_i = ", ".join(f"i.{name}" for name in columns)
    match_ga = " AND ".join(f"e.{name} = a.{name}" for name in KEY_COLUMNS)
    history_columns = list(columns) + list(VERSION_COLUMNS)

    spark.sql(
        f"""
        INSERT OVERWRITE {history} ({', '.join(history_columns)})
        WITH incoming AS ({incoming}),
        active AS ({active}),
        -- 需要关闭的当前版本：内容变了，或来源里已经没有了
        expiring AS (
            SELECT a.{key0} AS {key0},
                   a.{KEY_COLUMNS[1]} AS {KEY_COLUMNS[1]},
                   a.{KEY_COLUMNS[2]} AS {KEY_COLUMNS[2]}
            FROM active a
            LEFT JOIN incoming i ON {match_ai}
            WHERE i.{key0} IS NULL OR i.row_hash <> a.row_hash
        ),
        snapshot AS (
            -- 一、已失效的历史版本：原样保留，历史不可改
            SELECT {', '.join(history_columns)} FROM {history} WHERE NOT is_active
            UNION ALL
            -- 二、未受影响的当前版本：原样保留（键仍在、内容未变）
            SELECT {business_a}, a.begin_date, a.end_date, a.is_active,
                   a.last_modified_reason, a.record_version, a.row_hash
            FROM active a
            LEFT JOIN expiring e ON {match_ga}
            WHERE e.{key0} IS NULL
            UNION ALL
            -- 三、被替换掉的当前版本：写失效日，置为失效
            --     失效日 = 生效日 - 1（闭区间：这一版最后有效的日子）
            --     但不早于该版本自己的生效日：本次处理日若早于该版本的生效日
            --     （两个调用方的生效日约定不一致时就会发生），直接相减会写出
            --     「失效日早于生效日」的反向区间。取大值把它压成零长度区间 ——
            --     宁可让这一版长度为 0，也不写出自相矛盾的区间。
            SELECT {business_a}, a.begin_date,
                   greatest(date_sub(date '{effective_date}', 1), a.begin_date) AS end_date,
                   false AS is_active,
                   a.last_modified_reason, a.record_version, a.row_hash
            FROM active a
            JOIN expiring e ON {match_ga}
            UNION ALL
            -- 四、新版本：新键与已变更键
            SELECT {business_i}, date '{effective_date}' AS begin_date, CAST(NULL AS DATE) AS end_date, true AS is_active,
                   CASE WHEN a.record_version IS NULL THEN 'ORIGINAL' ELSE '{args.reason}' END AS last_modified_reason,
                   coalesce(a.record_version, 0) + 1 AS record_version, i.row_hash
            FROM incoming i
            LEFT JOIN active a ON {match_ia}
            WHERE a.{key0} IS NULL OR i.row_hash <> a.row_hash
        )
        SELECT * FROM snapshot
        """
    )

    active_after = spark.sql(f"SELECT count(*) AS n FROM {history} WHERE is_active").first()["n"]
    counts["unchanged"] = max(active_after - counts["new"] - counts["changed"], 0)

    # 写完立刻自检区间不变式：失效日不得早于生效日。
    # 写入侧的 greatest() 已经兜住不产生反向区间，这里再独立确认一次 ——
    # 兜底写错、有人绕过本作业改表、或将来改了写法，都会在这里被抓住，
    # 并让本环节退出非 0，而不是留下一张自相矛盾的版本表继续被下游读。
    reversed_rows = spark.sql(
        f"SELECT count(*) AS n FROM {history} WHERE end_date IS NOT NULL AND end_date < begin_date"
    ).first()["n"]
    if reversed_rows:
        raise ValueError(
            f"{history} 有 {reversed_rows} 行「失效日早于生效日」，版本区间被算反了，先修数据再继续"
        )
    return counts


def main() -> int:
    args = parse_args()
    spark = SparkSession.builder.appName("fr2052a-owd-scd2").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"OWD SCD2 版本化：报告日 {args.report_date}，变更原因 {args.reason}")
    failures: list[str] = []
    for table in args.tables:
        try:
            counts = version_table(spark, table, args)
            print(
                f"  [OK]   {table:<24} 新增 {counts['new']:>4}  "
                f"变更 {counts['changed']:>4}  删除 {counts['deleted']:>4}  "
                f"未变 {counts['unchanged']:>4}"
            )
        except Exception as error:  # noqa: BLE001 - 单表失败不中断整批，末尾统一汇总
            failures.append(table)
            print(f"  [FAIL] {table:<24} {type(error).__name__}: {error}")

    print()
    if failures:
        print(f"失败的表：{failures}")
        return 1
    print(f"完成：{len(args.tables)} 张 OWD 表已归并")
    return 0


if __name__ == "__main__":
    sys.exit(main())
