# CRON-DESIGN.md — 定时任务说明

> 本文件是本项目全部定时任务的**唯一说明**。调度信息不在这里重复第二份：实现住在 DAG 文件与服务器 crontab 里，本文件只回答「谁在什么时候触发什么、失败会怎样、去哪查现状」。

## 1. Airflow DAG（Server 1）

DAG 只做编排：每个任务 ssh 到 Server 2 调 `run-daily-pipeline.sh` 的同一个环节，编排逻辑不写第二份。

| DAG | 调度（Cron） | 触发什么 | 失败语义 | 定义位置 |
|-----|--------------|----------|----------|----------|
| `fr2052a_daily_batch` | `0 6 * * *` 每天 06:00 | 按依赖顺序跑跑批的 19 个环节 | 任一环节非 0 即任务失败，`run-context-close` 不执行 | `deploy/server1/airflow/dags/fr2052a_daily_batch.py` |
| `fr2052a_gl_reconciliation` | `0 7 * * *` 每天 07:00 | 单独重跑 GL 对账 | 对账不为 8/8 PASS 即失败 | `deploy/server1/airflow/dags/fr2052a_gl_reconciliation.py` |
| `fr2052a_submission` | `30 7 * * *` 每天 07:30 | 先跑 `gate` 放行闸，再生成报送文件与回执 | 熔断中 `gate` 退 2，报送环节整体阻断 | `deploy/server1/airflow/dags/fr2052a_submission.py` |
| `fr2052a_realtime_alert` | `*/15 * * * *` 每 15 分钟 | 实时敞口扫描与汇总 | 本轮失败不影响上一轮结论 | `deploy/server1/airflow/dags/fr2052a_realtime_alert.py` |
| `fr2052a_backfill_and_restate` | 不排程，手动触发 | 回刷指定报告日并登记重述 | 手动运行，参数见 `INTERFACE-DESIGN.md` 的 DAG 接口一节 | `deploy/server1/airflow/dags/fr2052a_backfill_and_restate.py` |

**暂停状态不在本文件维护**：哪几个 DAG 现在是暂停的，属于 Airflow 侧的运行态事实，改了不用改文档。查现状：

```bash
ssh hermes@192.168.17.22 "docker exec fr2052a_postgres psql -U fr2052a -d airflow -t -A -F'|' -c 'SELECT dag_id, is_paused FROM dag ORDER BY 1;'"
```

## 2. 服务器 cron

| 主机 | 调度（Cron） | 跑什么 | 产出 | 声明文件 |
|------|--------------|--------|------|----------|
| Server 2 `192.168.17.24` | `*/5 * * * *` 每 5 分钟 | `~/fr2052a-infra/health_to_metrics.sh` | 巡检结论写成 node_exporter 的 textfile 指标，供 Server 1 的 Prometheus 抓 | `deploy/server2/crontab.example` |
| Server 1 `192.168.17.22` | 无 | 容器自带健康检查 | — | — |

这行 cron **只存在于服务器上**，所以仓库里留一份声明文件；服务器重建或换机后按它装回去：

```bash
scp deploy/server2/crontab.example hermes@192.168.17.24:/tmp/fr2052a-crontab
ssh hermes@192.168.17.24 "crontab /tmp/fr2052a-crontab && crontab -l"
```

## 3. 变更纪律

1. 改调度先改本文件与对应的声明文件，再改实现，三处必须一致
2. 新增定时任务时同时补本文件的一行与声明文件的一行
3. 下线定时任务时两处一起删，别只删服务器上的那行
