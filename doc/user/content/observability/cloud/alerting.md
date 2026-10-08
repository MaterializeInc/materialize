---
title: "Alerting"
description: "Alerting thresholds to use for monitoring."
menu:
  main:
    parent: "monitor-cloud"
    name: "Set alerts"
    weight: 15
aliases:
  - /manage/monitor/cloud/alerting/
---

{{< warning >}}
DO NOT MERGE. Early draft, shared for customer and internal feedback on the
[Materialize Cloud metrics](/observability/cloud/cloud-metrics-draft/) design.
Not official documentation; metric names will change.
{{</ warning >}}

After setting up a monitoring tool, it is important to configure alert rules. Alert rules send a notification when a metric surpasses a threshold. This will help you prevent operational incidents.

This page describes which metrics and thresholds to build as a starting point. For more details on how to set up alert rules in Datadog or Grafana, refer to:

 * [Datadog monitors](https://docs.datadoghq.com/monitors/)
 * [Grafana alerts](https://grafana.com/docs/grafana/latest/alerting/fundamentals/)

## Thresholds

Alert rules tend to have two threshold levels, and we are going to define them as follows:
 * Warning: represents a call to attention to a symptom with high chances to develop into an issue.
 * Alert: represents an active issue that requires immediate action.

For each threshold level, use the following table as a guide to set up your own alert rules:

Metric | Warning | Alert | Cloud metric
-- | -- | -- | --
Freshness | > 5s | > 1m | `mz_dataflow_wallclock_lag_seconds{quantile="0.95"}`, averaged over the last *15 minutes*.
Memory utilization | 80% | 90% | Average memory utilization for a cluster in the last *15 minutes*. (To be added.) In the meantime, `mz_metric_sink_curated_arrangement_size_bytes` shows which objects use memory.
Dataflow errors | - | > 0 | `mz_metric_sink_curated_dataflow_error_count` in the last *5 minutes*.
Source status | - | On Change | Source status change in the last *1 minute*. (To be added.) In the meantime, watch `mz_source_offset_known` minus `mz_source_offset_committed` for lag.
Cluster status | - | On Change | Cluster replica status change in the last *1 minute*. (To be added.)
Metrics freshness | - | Not advancing for 5m | `mz_compute_metric_sink_frontier_ms`. Means the metrics themselves are stale, not your data.

{{<note>}}
Customers on legacy cluster sizes should still monitor their Memory usage. Please [contact support](/support/) for questions.
{{</note>}}

### Custom Thresholds

For the following table, replace the two variables, _X_ and _Y_, by your organization and use case:

Metric | Warning | Alert | Cloud metric
-- | -- | -- | --
Latency | p95 > X | p95 > Y | `histogram_quantile(0.95, rate(mz_compute_peek_duration_seconds_bucket[15m]))`, where X and Y are the expected latencies in seconds.
Credits | Consumption rate increase by X% | Consumption rate increase by Y% | Average credit consumption in the last *60 minutes*. (To be added.)

## Maintenance window

Materialize has a release and a maintenance window almost every week at a defined [schedule](/releases/schedule/#cloud-upgrade-schedule).

During an upgrade, clients may experience brief connection interruptions, but the service otherwise remains fully available. Alerts may get triggered during this brief period of time. For this case, you can configure your monitoring tool to avoid unnecessary alerts as follows:

* [Datadog downtimes](https://docs.datadoghq.com/monitors/downtimes/)
* [Grafana mute timings](https://grafana.com/docs/grafana/latest/alerting/manage-notifications/mute-timings/)

## Status Page

All performance‑impacting incidents are communicated on our [status page](https://status.materialize.com/), which provides current and historical incident details and subscription options for notifications. Reviewing this page is the quickest way to confirm if a known issue is affecting your database.
