"""FR 2052a 实时敞口预警 DAG。

与日批的时间尺度不同：日批看的是「昨天结完账的报表」，实时扫描看的是「正在进来的源记录」。
一笔大额未保险存款当天进来、当天可能被提走，等到 T+1 跑批才看见就晚了。

链条：

    scan_large_exposures  →  summarize_alerts

    scan_large_exposures  消费 Kafka core_banking_txns，识别超过门槛的未保险存款敞口，
                          写入事件表并投递到 fr2052a_alerts 主题
    summarize_alerts      把本轮的预警数汇总出来，供人一眼看到有没有异常

调度说明：每 15 分钟一次。扫描用 trigger(availableNow=True) 把积压消息一口气消费完即退出，
不会长时间占用计算资源；消费位点由 checkpoint 记录，两次运行之间不会重复消费。

与熔断的关系：本 DAG 只报警、不阻断。单笔敞口不是报送阻断条件 ——
阻断由日批的 liquidity_monitor 按规则判定后翻闸决定（见 fr2052a_daily_batch）。
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

SSH_CONN_ID = "ssh_default"
REMOTE_DIR = "/home/hermes/fr2052a-infra"
SSH_TIMEOUT_SECONDS = 1800


with DAG(
    dag_id="fr2052a_realtime_alert",
    description="实时敞口扫描：消费核心存款主题，识别大额未保险敞口并告警",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Shanghai"),
    catchup=False,
    max_active_runs=1,
    params={"report_date": "2026-09-16"},
    default_args={
        "owner": "fr2052a",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=1),
    },
    tags=["fr2052a", "realtime", "alert"],
) as dag:
    scan_large_exposures = SSHOperator(
        task_id="scan_large_exposures",
        ssh_conn_id=SSH_CONN_ID,
        cmd_timeout=SSH_TIMEOUT_SECONDS,
        command=(
            f"cd {REMOTE_DIR} && REPORT_DATE={{{{ params.report_date }}}} bash run-daily-pipeline.sh realtime-scan"
        ),
        doc_md="扫描大额未保险存款敞口，写事件表并投递告警主题",
    )

    summarize_alerts = SSHOperator(
        task_id="summarize_alerts",
        ssh_conn_id=SSH_CONN_ID,
        cmd_timeout=SSH_TIMEOUT_SECONDS,
        command=(
            f"cd {REMOTE_DIR} && REPORT_DATE={{{{ params.report_date }}}} bash run-daily-pipeline.sh realtime-summary"
        ),
        doc_md="汇总事件表，供人快速判断本轮有没有异常",
    )

    scan_large_exposures >> summarize_alerts
