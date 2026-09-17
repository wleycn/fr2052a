"""FR 2052a 报送 DAG。

链条：

    check_gate  →  generate_and_submit  →  verify_submission

三道任务对应三件不同的事：
    check_gate           放行判定。熔断中直接失败，后面的任务不会执行。
    generate_and_submit  生成 XBRL / XML / CSV 并登记回执。
                         它自己也会再过一次闸 —— 两个入口都要拦得住，
                         只靠 DAG 里那道任务，绕过 DAG 直接跑脚本就漏了。
    verify_submission    从磁盘重算文件哈希与台账比对。生成脚本返回 0 不等于文件是对的。

未纳入本 DAG 的环节（属 E7 治理范围）：
    真实监管网关提交。演示环境没有网关，回执为模拟回执，
    见 generate_submission.py 的 simulated_receipt。
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

SSH_CONN_ID = "ssh_default"
REMOTE_DIR = "/home/hermes/fr2052a-infra"
SSH_TIMEOUT_SECONDS = 3600


def pipeline_command(step: str) -> str:
    return f"cd {REMOTE_DIR} && REPORT_DATE={{{{ params.report_date }}}} bash run-daily-pipeline.sh {step}"


with DAG(
    dag_id="fr2052a_submission",
    description="报送：放行判定 → 生成 XBRL/XML/CSV → 文件完整性核对",
    # 日批 06:00 起跑、约 20 分钟跑完；08:00 前留给人工复核与重跑。
    schedule="30 7 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Shanghai"),
    catchup=False,
    max_active_runs=1,
    params={"report_date": "2026-09-16"},
    default_args={
        "owner": "fr2052a",
        "retries": 0,  # 报送环节不自动重试：状态不明时重跑要先看闸，而不是重发文件
    },
    tags=["fr2052a", "submission"],
) as dag:
    check_gate = SSHOperator(
        task_id="check_gate",
        ssh_conn_id=SSH_CONN_ID,
        cmd_timeout=SSH_TIMEOUT_SECONDS,
        command=pipeline_command("gate"),
        doc_md="读取熔断闸状态；熔断中退出码 2，本 DAG 就此停住",
    )

    generate_and_submit = SSHOperator(
        task_id="generate_and_submit",
        ssh_conn_id=SSH_CONN_ID,
        cmd_timeout=SSH_TIMEOUT_SECONDS,
        command=pipeline_command("submission"),
        doc_md="生成 XBRL/XML/CSV 三种格式并登记回执（内部再过一次闸）",
    )

    verify_submission = SSHOperator(
        task_id="verify_submission",
        ssh_conn_id=SSH_CONN_ID,
        cmd_timeout=SSH_TIMEOUT_SECONDS,
        command=pipeline_command("verify-submission"),
        doc_md="从磁盘重算哈希与台账比对，确认交付的文件就是登记的文件",
    )

    check_gate >> generate_and_submit >> verify_submission
