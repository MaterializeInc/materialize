---
title: "Alerting"
description: "Alerting thresholds to use for monitoring."
menu:
  main:
    parent: "monitor"
---

After setting up your monitoring tool, it is essential to configure alerting rules.
These rules are designed to trigger a warning or alert message
when a **metric** surpasses a **particular threshold**, helping to investigate
and prevent incidents.

This page describes which **thresholds** and **metrics** are crucial.
For more details on how to set up an alert in Datadog or Grafana, refer to:

 * [Datadog monitors](https://docs.datadoghq.com/monitors/)
 * [Grafana alerts](https://grafana.com/docs/grafana/latest/alerting/fundamentals/)

## Thresholds

Alerting rules can have two different severity level thresholds that we are going to define as follows:
 * **Warning:** represents a call to attention to a symptom with high chances to develop into an issue.
 * **Alert:** represents an active issue that requires immediate action.

Use the following table as a guide to set up your own alerting rules:

Metric | Warning | Alert | Description
-- | -- | -- | --
CPU | 85% | 100% | Average CPU usage for a cluster in the last *60 minutes*.
Memory | 80% | 90% | Average memory usage for a cluster in the last *30 minutes*.
Status | - | On Change | Source status change in the last *5 minutes*.
Distribution | Time spent by worker > Avg * 1.5 | Time spent by worker > Avg * 2 | Average [work distribution](https://materialize.com/docs/manage/troubleshooting/#is-work-distributed-equally-across-workers) in the last *60 minutes*.
Freshness | > 1s | > 1m | Average [lag behind an input](https://materialize.com/docs/sql/system-catalog/mz_internal/#mz_materialization_lag) in the last *30 minutes*.

### SLA Thresholds

The following metrics will change depending your business target SLAs:

Metric | Warning | Alert | Description
-- | -- | -- | --
Latency | p99 > X | p99 > Y | Latency percentile from the activity log in the last *60 minutes*. Where X and Y are the expected latencies.
Credits | Consumption rate increase by X% | Consumption rate increase by Y% | Average credit consumption in the last *60 minutes*.

## Maintenance window

Materialize has a [release and a maintenance window almost every week](https://materialize.com/docs/releases/) at a defined [schedule hours](https://materialize.com/docs/releases/#schedule). We announce every maintenance window on the [status page](https://status.materialize.com/), where you can subscribe to updates and receive alerts for this, or any other unexpected event.

After an upgrade, you’ll experience just a few minutes of downtime and the rehydration process. During this brieft period of time, alerts may get trigger. For these cases, you can configure your moring tool as follows to avoid unnecesary alerts:

* [Datadog dowtimes](https://docs.datadoghq.com/monitors/downtimes/)
* [Grafana mute timings](https://grafana.com/docs/grafana/latest/alerting/manage-notifications/mute-timings/)
