"""FR 2052a 每日跑批主链路 DAG。

链条（与 requirements/[99] §2.4 的设计一致，按当前已落地的环节展开）：

    check_source_arrival → load_ref → replay_ods → load_bronze → dbt_run
      → dq_validate → export_pg → verify_bronze → verify_silver → verify_ads

设计要点：
  1. 每个 Task 都是"SSH 到 Server 2 执行 run-daily-pipeline.sh 的某一个环节"，
     编排逻辑只有那一份脚本，DAG 不复制命令 —— 否则改了一处忘另一处必然漂移。
  2. 环节的退出码就是 Task 的成功/失败：核对脚本失败会让批次红，进而阻断下游，
     这是"数据不对就不许报送"的机器保障，不靠人看日志。
  3. **必须显式设置 cmd_timeout**：SSHOperator 的默认命令超时只有 10 秒，
     而单个环节要跑几十秒的 spark-submit，用默认值会以 "SSH command timed out" 失败。
     实测踩过这个坑，故在 ssh_task 工厂里统一设成 1 小时。

尚未纳入本 DAG 的环节（属 E6/E7 范围，不做空壳任务占位）：
  报表生成 XBRL/XML/CSV、报送 Federal Reserve、DataHub 血缘摄取、实时告警。
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

SSH_CONN_ID = "ssh_default"
REMOTE_DIR = "/home/hermes/fr2052a-infra"
REMOTE_APP_DIR = "/opt/fr2052a-app"
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
    },
    tags=["fr2052a", "daily", "submission"],
) as dag:
    check_source_arrival = ssh_task(
        "check_source_arrival",
        f"ls -1 {REMOTE_APP_DIR}/sample_data/ods/*.csv | wc -l",
        "确认 7 张 ODS 源文件全部到位，避免空跑一整轮",
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
    verify_ads = ssh_task(
        "verify_ads",
        pipeline_command("verify-ads"),
        "ADS 层合并口径、明细回溯、监管上限、GL 对账",
    )

    (
        check_source_arrival
        >> load_ref
        >> replay_ods
        >> load_bronze
        >> dbt_run
        >> dq_validate
        >> export_pg
        >> verify_bronze
        >> verify_silver
        >> verify_ads
    )
