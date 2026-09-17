"""FR 2052a GL 对账 DAG。

职责：把总账与报送口径的对账结果取回来，按严重度判定是否放行。
与日批 DAG 的分工：日批负责"把数跑出来"，本 DAG 负责"盯着对账结论"，
并可独立于日批被触发（例如人工核查某个报告日）。

对账结论写在 PostgreSQL 的 `ads.ads_gl_reconciliation`（由 dbt 模型产出）。
只要有 FAIL 行，本 DAG 即失败 —— 对账不平不许报送。
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

SSH_CONN_ID = "ssh_default"
REMOTE_DIR = "/home/hermes/fr2052a-infra"
POSTGRES_CONN_ID = "postgres_default"


def check_reconciliation(**context) -> None:
    """读对账表，有 FAIL 行就抛异常让任务失败。"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = hook.get_records(
        """
        select section_code, gl_account_id, gl_amount, fr2052a_amount, variance, status
        from ads.ads_gl_reconciliation
        order by section_code
        """
    )
    if not rows:
        raise ValueError("对账表为空：日批可能尚未完成，或导出环节未执行")

    failures = [row for row in rows if row[5] != "PASS"]
    for section, accounts, gl_amount, report_amount, variance, status in rows:
        print(f"  [{status}] Section {section:<3} 科目 {accounts:<12} 总账 {gl_amount:>18,.2f} "
              f"报送 {report_amount:>18,.2f} 差异 {variance:>14,.2f}")

    print()
    if failures:
        raise ValueError(f"对账不平 {len(failures)} 项：{[row[0] for row in failures]}（报送已阻断）")
    print(f"对账 {len(rows)} 项全部通过")


with DAG(
    dag_id="fr2052a_gl_reconciliation",
    description="GL 对账：总账余额与 FR 2052a 报送口径逐 Section 比对，不平则阻断",
    schedule="0 7 * * *",  # 日批 06:00 之后一小时，确保报表已导出
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Shanghai"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "fr2052a", "retries": 0},
    tags=["fr2052a", "control", "reconciliation"],
) as dag:
    refresh_report = SSHOperator(
        task_id="refresh_report",
        ssh_conn_id=SSH_CONN_ID,
        command=f"cd {REMOTE_DIR} && bash run-daily-pipeline.sh export-pg",
        # SSHOperator 默认命令超时仅 10 秒，跑 spark-submit 必须显式放宽
        cmd_timeout=3600,
        doc_md="重新导出 gold 层报表，确保对账读的是当日最新口径",
    )

    check_reconciliation_task = PythonOperator(
        task_id="check_reconciliation",
        python_callable=check_reconciliation,
        doc_md="读 ads.ads_gl_reconciliation，任何 FAIL 行都会让任务失败",
    )

    refresh_report >> check_reconciliation_task
