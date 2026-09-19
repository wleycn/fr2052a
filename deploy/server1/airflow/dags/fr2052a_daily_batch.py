# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""FR 2052a 每日跑批主链路 DAG。

链条（与 requirements/[99] §2.4 的设计一致，按当前已落地的环节展开）：

    run_context_open → check_source_arrival → load_ref → replay_ods → load_bronze → dbt_run
      → pii_vault → lineage → owd_scd2 → dq_validate → publish_access → export_pg
      → liquidity_monitor → verify_bronze → verify_silver → verify_scd2 → verify_ads → verify_rbac
      → pipeline_health → run_context_close

设计要点：
  1. 每个 Task 都是"SSH 到 Server 2 执行 run-daily-pipeline.sh 的某一个环节"，
     编排逻辑只有那一份脚本，DAG 不复制命令 —— 否则改了一处忘另一处必然漂移。
     这条纪律曾经失守过：跑批脚本在仓库里改成了分环节调度，但没同步到服务器，
     于是每个任务都跑了一整条链路、DAG 全绿而分环节调度根本没生效。
     现在服务器上的部署文件由 deploy/sync-deploy.sh 同步并支持漂移检查。
  2. 环节的退出码就是 Task 的成功/失败：核对脚本失败会让批次红，进而阻断下游，
     这是"数据不对就不许报送"的机器保障，不靠人看日志。
  3. **必须显式设置 cmd_timeout**：SSHOperator 的默认命令超时只有 10 秒，
     而单个环节要跑几十秒的 spark-submit，用默认值会以 "SSH command timed out" 失败。
     实测踩过这个坑，故在 ssh_task 工厂里统一设成 1 小时。
  4. **不在本 DAG 里的环节**：报送（gate / submission / verify-submission）在
     fr2052a_submission DAG；重述在 fr2052a_backfill_and_restate；
     实时扫描在 fr2052a_realtime_alert。分开的理由是触发条件不同 ——
     日批是定时的，报送要等人复核，重述是事件驱动的，实时是高频的。
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

SSH_CONN_ID = "ssh_default"
REMOTE_DIR = "/home/hermes/fr2052a-infra"
SSH_TIMEOUT_SECONDS = 3600


def pipeline_command(step: str) -> str:
    """把跑批脚本的某个环节包成一条远端命令。"""
    return f"cd {REMOTE_DIR} && bash run-daily-pipeline.sh {step}"


def ssh_task(task_id: str, command: str, doc_md: str) -> SSHOperator:
    """建一个远端执行任务。统一带上 1 小时的命令超时（默认 10 秒不够跑 spark-submit）。"""
    return SSHOperator(
        task_id=task_id,
        ssh_conn_id=SSH_CONN_ID,
        command=command,
        cmd_timeout=SSH_TIMEOUT_SECONDS,
        doc_md=doc_md,
    )


with DAG(
    dag_id="fr2052a_daily_batch",
    description="T+1 报送主链路：入湖 → 建模 → 质量校验 → 导出 → 三层核对",
    schedule="0 6 * * *",  # 每日 06:00 触发，为 T+1 08:00 的报送截止留出余量
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Shanghai"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "fr2052a",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=2),
        # SLA：超过 2 小时还没跑完就记一次 SLA Miss，在 UI 与元数据库里可见。
        # 06:00 起跑、通常 20 分钟内跑完，2 小时足够宽松 ——
        # 设得太紧会把正常波动也报成违规，反而没人看。
        "sla": pendulum.duration(hours=2),
    },
    tags=["fr2052a", "daily", "submission"],
) as dag:
    run_context_open = ssh_task(
        "run_context_open",
        f"cd {REMOTE_DIR} && RUN_TYPE=DAILY bash run-daily-pipeline.sh run-context-open",
        "运行上下文开口：把本次跑批的报告日、处理日、生效日写进库，所有环节与闸读它",
    )
    check_source_arrival = ssh_task(
        "check_source_arrival",
        pipeline_command("check-source"),
        "确认 7 张 ODS 源文件全部到位：数量不等于 7 即判失败，避免空跑一整轮",
    )
    load_ref = ssh_task(
        "load_ref",
        pipeline_command("ref-load"),
        "REF 字典表入 Iceberg（覆盖写，幂等）",
    )
    replay_ods = ssh_task(
        "replay_ods",
        pipeline_command("ods-replay"),
        "样本明细按主题重放进 Kafka，模拟源系统上报",
    )
    load_bronze = ssh_task(
        "load_bronze",
        pipeline_command("bronze-load"),
        "消费 Kafka 入 bronze 层，按主键 MERGE 去重（至少一次投递不写重）",
    )
    dbt_run = ssh_task(
        "dbt_run",
        pipeline_command("dbt-run"),
        "dbt 三层建模：OWD → OWS → ADS（层内依赖由 dbt 自己解析）",
    )
    pii_vault = ssh_task(
        "pii_vault",
        pipeline_command("pii-vault"),
        "建 PII 明文对照表；OWD 层只有 token，明文只在这一处且受角色限制",
    )
    lineage = ssh_task(
        "lineage",
        pipeline_command("lineage"),
        "渲染血缘与监管映射，并写审计血缘表",
    )
    owd_scd2 = ssh_task(
        "owd_scd2",
        pipeline_command("owd-scd2"),
        "OWD 版本历史 SCD2 归并（新增/变更/删除分别留痕）",
    )
    dq_validate = ssh_task(
        "dq_validate",
        pipeline_command("dq-rules"),
        "执行 ref 层声明的校验规则；ERROR 级违规会让本任务失败",
    )
    export_pg = ssh_task(
        "export_pg",
        pipeline_command("export-pg"),
        "gold 层报表导出到 PostgreSQL 报送服务层，并回读校验",
    )
    publish_access = ssh_task(
        "publish_access",
        pipeline_command("publish-access"),
        "在导出之前施加结构迁移与授权（导出用 truncate=true 覆盖写，保住授权与库侧列）",
    )
    liquidity_monitor = ssh_task(
        "liquidity_monitor",
        pipeline_command("liquidity-monitor"),
        "算 LCR 与各类上限占比，命中规则即写预警并翻转熔断闸",
    )
    verify_bronze = ssh_task(
        "verify_bronze",
        pipeline_command("verify-bronze"),
        "bronze 行数与样本 CSV 比对",
    )
    verify_silver = ssh_task(
        "verify_silver",
        pipeline_command("verify-silver"),
        "OWD 层行数、到期分桶、汇率折算逐行重算",
    )
    verify_scd2 = ssh_task(
        "verify_scd2",
        pipeline_command("verify-scd2"),
        "版本历史不变式：失效日为空 ⇔ 当前有效、版本号连续、无重复",
    )
    verify_ads = ssh_task(
        "verify_ads",
        pipeline_command("verify-ads"),
        "ADS 层合并口径、明细回溯、监管上限、GL 对账",
    )
    pipeline_health = ssh_task(
        "pipeline_health",
        pipeline_command("health"),
        "健康巡检：熔断状态、质量失败数、Kafka 滞后、连接与磁盘。恒成功，只报不改判定",
    )
    run_context_close = ssh_task(
        "run_context_close",
        pipeline_command("run-context-close"),
        "运行上下文收口：把本次跑批标为成功",
    )
    verify_rbac = ssh_task(
        "verify_rbac",
        pipeline_command("verify-rbac"),
        "权限自测：逐角色实读一次，与权限声明比对（授权是否真的生效）",
    )

    (
        run_context_open
        >> check_source_arrival
        >> load_ref
        >> replay_ods
        >> load_bronze
        >> dbt_run
        >> pii_vault
        >> lineage
        >> owd_scd2
        >> dq_validate
        >> publish_access
        >> export_pg
        >> liquidity_monitor
        >> verify_bronze
        >> verify_silver
        >> verify_scd2
        >> verify_ads
        >> verify_rbac
        >> pipeline_health
        >> run_context_close
    )
