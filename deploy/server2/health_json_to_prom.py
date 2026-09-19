# [AI-GENERATED] model=deepseek-flash date=2026-09-19 reviewed_by=pending
# ruff: noqa: D202
#
# 为什么在本文件关掉 D202：本仓的提交门禁有一条排版判据要求「docstring 后空一行」（阻断级），
# 而 ruff 的 D202 要求「docstring 后不得有空行」—— 两条对同一处给出相反要求，函数体紧接
# docstring 时无法同时满足。门禁是阻断级，按它的形态写，并把 ruff 这条在本文件关掉。
# 这是工具链的冲突，不是代码风格偏好，已写进 docs/changes/engineering.md 的 monitoring-stack 条目。
"""把 `pipeline_health.py --json` 的输出转成 Prometheus 文本格式。

上游契约：`python/governance/pipeline_health.py --json` 的输出（见该脚本的 report 字典）。
下游消费：node_exporter 的 textfile collector 读这个文件，Server 1 的 Prometheus 抓它。

这个脚本只做字段映射，不做判断 —— 指标值就是巡检脚本给出的值。阈值与告警条件写在
`deploy/server1/monitoring/prometheus/rules/fr2052a.yml`，不在两处各写一套。

用法（Server 2）：
    python health_json_to_prom.py <巡检 JSON 文件> <输出 .prom 文件>

缺失字段按 0 处理：指标缺值不该让整批采集失败。写文件走 `.tmp` 加原子替换，
避免 Prometheus 抓到写了一半的内容。
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any


def as_number(value: Any) -> float:
    """把 None 与不可解析的值折成 0.0，供指标使用。"""

    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def metric_lines(report: dict[str, Any]) -> list[str]:
    """按巡检报告生成 Prometheus 文本格式的指标行。"""

    breaker = report.get("breaker") or {}
    quality = report.get("quality") or {}
    submission = report.get("submission") or {}
    kafka = report.get("kafka") or {}
    disk = report.get("disk") or {}
    connections = report.get("connections") or {}

    metrics: list[tuple[str, float, str]] = [
        ("fr2052a_health_last_run_timestamp", time.time(), "采集脚本最后一次成功写完指标的时间"),
        ("fr2052a_breaker_halted", 1.0 if breaker.get("state") == "HALTED" else 0.0, "熔断闸是否处于 HALTED"),
        ("fr2052a_breaker_trip_count", as_number(breaker.get("trip_count")), "熔断闸累计触发次数"),
        ("fr2052a_dq_rules_total", as_number(quality.get("total")), "最近批次执行的校验条数"),
        ("fr2052a_dq_errors", as_number(quality.get("errors")), "最近批次的 ERROR 级校验失败条数"),
        ("fr2052a_dq_warnings", as_number(quality.get("warnings")), "最近批次的 WARNING 级校验条数"),
        ("fr2052a_alerts_open", as_number(report.get("open_alerts")), "未关闭的预警条数"),
        ("fr2052a_alerts_blocking", as_number(report.get("blocking_alerts")), "未关闭的阻断级预警条数"),
        ("fr2052a_submission_files", as_number(submission.get("files")), "最近报告期已登记的报送文件数"),
        ("fr2052a_submission_rejected", as_number(submission.get("rejected")), "最近报告期回执被拒的文件数"),
        ("fr2052a_restatements_total", as_number(report.get("restatements")), "累计重述次数"),
        ("fr2052a_kafka_max_lag", as_number(kafka.get("max_lag")), "Kafka 消费组最大滞后"),
        ("fr2052a_pg_connections", as_number(connections.get("active")), "PG 活跃连接数"),
        ("fr2052a_pg_connections_max", as_number(connections.get("max")), "PG 最大连接数"),
        ("fr2052a_disk_used_percent", as_number(disk.get("used_percent")), "巡检所看挂载点的磁盘占用百分比"),
    ]

    lines: list[str] = []
    for name, value, help_text in metrics:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} gauge")
        lines.append(f"{name} {value}")
    return lines


def main(argv: list[str]) -> int:
    """读巡检 JSON，写出 .prom 文件；参数不足时报错退出。"""

    if len(argv) != 3:
        print("用法：health_json_to_prom.py <巡检 JSON 文件> <输出 .prom 文件>", file=sys.stderr)
        return 2

    source, target = argv[1], argv[2]
    with open(source, encoding="utf-8") as handle:
        report = json.load(handle)

    lines = metric_lines(report)
    tmp_path = f"{target}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")
    os.replace(tmp_path, target)
    print(f"已写入 {target}（{len(lines) // 3} 个指标）")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
