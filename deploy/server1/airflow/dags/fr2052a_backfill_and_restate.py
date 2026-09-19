"""FR 2052a 回刷与重述 DAG。

与日批的区别（这是本 DAG 存在的理由）：
  日批处理「该来的数据来了」，重述处理「已经报送过的数字要改」。
  后者必须留痕：改前长什么样、谁要求的、谁批的、改成了什么。

链条（顺序不能颠倒）：

    restate_capture  →  rebuild  →  restate_register  →  verify_ads
         │                │                │
         │                │                └─ 关闭旧版本、登记新版本、写重述登记表
         │                └─ 用 RESTATEMENT 原因重跑建模与导出（OWD 版本历史一并归并）
         └─ 重跑之前先存原报文的快照；重跑会覆盖 ads_fr2052a_report，
            覆盖之后再取就取不到原报表了

参数（触发时传入，也可用 DAG 默认值直接跑）：
    report_date     报告日
    entity_code     需要重述的法人实体
    reason          重述原因（写入重述登记表）
    requested_by    申请人
    effective_date  版本生效日（重述日），默认与报告日相同
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

SSH_CONN_ID = "ssh_default"
REMOTE_DIR = "/home/hermes/fr2052a-infra"
SSH_TIMEOUT_SECONDS = 3600

# 重跑环节串成一条命令：这七步之间不能插入任何人工操作，否则版本历史会不完整。
#
# 必须从前两步开始（ods-replay + bronze-load）：重述的前提是「源数据被修正了」，
# 如果只重建建模层，dbt 读到的还是 bronze 里的旧明细，重跑等于白跑 ——
# 实测踩过：漏了这两步，重跑后 OWD 显示「未变 500」，报文一字未动，
# 重述登记因此判定「不构成重述」而拒绝登记版本。
REBUILD_STEPS = "ods-replay bronze-load dbt-run owd-scd2 dq-rules export-pg publish-access liquidity-monitor"


def env_prefix(context_params: dict[str, str]) -> str:
    """把重述参数拼成远端命令的环境变量前缀。"""
    assignments = " ".join(f'{name}="{value}"' for name, value in context_params.items())
    return f"cd {REMOTE_DIR} && {assignments} bash run-daily-pipeline.sh"


with DAG(
    dag_id="fr2052a_backfill_and_restate",
    description="回刷与重述：留痕式重跑，记录原报表与新报表的版本对应关系",
    schedule=None,  # 只手动触发：重述是事件驱动的，不该有定时
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Shanghai"),
    catchup=False,
    max_active_runs=1,
    params={
        "report_date": "2026-09-16",
        "entity_code": "ENT001",
        "reason": "人工触发的重述演示",
        "requested_by": "treasury_analyst",
        "effective_date": "2026-09-17",
    },
    default_args={
        "owner": "fr2052a",
        "retries": 0,  # 重述重试会重复登记版本，宁可人工介入也不自动重试
    },
    tags=["fr2052a", "restatement", "backfill"],
) as dag:
    start = EmptyOperator(task_id="start")

    restate_capture = SSHOperator(
        task_id="restate_capture",
        ssh_conn_id=SSH_CONN_ID,
        cmd_timeout=SSH_TIMEOUT_SECONDS,
        command=(
            f"cd {REMOTE_DIR} && REPORT_DATE={{{{ params.report_date }}}} "
            "ENTITY_CODE={{ params.entity_code }} "
            "EFFECTIVE_DATE={{ params.effective_date }} "
            'RESTATE_REASON="{{ params.reason }}" '
            "REQUESTED_BY={{ params.requested_by }} "
            "bash run-daily-pipeline.sh restate-capture"
        ),
        doc_md="重跑前把当前生效的报文存进版本历史（保存原报表）",
    )

    rebuild = SSHOperator(
        task_id="rebuild",
        ssh_conn_id=SSH_CONN_ID,
        cmd_timeout=SSH_TIMEOUT_SECONDS,
        command=(
            f"cd {REMOTE_DIR} && REPORT_DATE={{{{ params.report_date }}}} "
            "SCD2_REASON=RESTATEMENT ENTITY_CODE={{ params.entity_code }} "
            "EFFECTIVE_DATE={{ params.effective_date }} "
            'RESTATE_REASON="{{ params.reason }}" '
            "REQUESTED_BY={{ params.requested_by }} "
            # 重述是「已报送期内容要变」的正当路径：显式打开导出的越过开关，
            # 否则前置闸会因为「已报送期内容变了」拦下重述本身（见 export_gold_to_pg.py 第 5 条）
            f"ALLOW_EXPORT_AFTER_SUBMISSION=1 bash run-daily-pipeline.sh {REBUILD_STEPS}"
        ),
        doc_md="按 RESTATEMENT 原因重跑：建模 → OWD 版本归并 → 质量 → 导出 → 流动性判定",
    )

    restate_register = SSHOperator(
        task_id="restate_register",
        ssh_conn_id=SSH_CONN_ID,
        cmd_timeout=SSH_TIMEOUT_SECONDS,
        command=(
            f"cd {REMOTE_DIR} && REPORT_DATE={{{{ params.report_date }}}} "
            "ENTITY_CODE={{ params.entity_code }} "
            "EFFECTIVE_DATE={{ params.effective_date }} "
            'RESTATE_REASON="{{ params.reason }}" '
            "REQUESTED_BY={{ params.requested_by }} "
            "bash run-daily-pipeline.sh restate-register"
        ),
        doc_md="关闭旧版本、登记新版本、写重述登记表",
    )

    verify_ads = SSHOperator(
        task_id="verify_ads",
        ssh_conn_id=SSH_CONN_ID,
        cmd_timeout=SSH_TIMEOUT_SECONDS,
        command=f"cd {REMOTE_DIR} && bash run-daily-pipeline.sh verify-ads",
        doc_md="重述后的 ADS 层核对（合并口径、明细回溯、监管上限、GL 对账）",
    )

    start >> restate_capture >> rebuild >> restate_register >> verify_ads
