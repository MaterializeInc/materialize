---
title: "Alerting"
description: "Alerting thresholds to use for monitoring."
menu:
  main:
    parent: "monitor"
---

After setting up a monitoring tool, it is important to configure alert rules. Alert rules send a notification when a metric surpasses a threshold. This will help you prevent operational incidents.

This page describes which metrics and thresholds to build as a starting point. For more details on how to set up alert rules in Datadog or Grafana, refer to:

 * [Datadog monitors](https://docs.datadoghq.com/monitors/)
 * [Grafana alerts](https://grafana.com/docs/grafana/latest/alerting/fundamentals/)

## Thresholds

Alert rules tend to have two threshold levels, and we are going to define them as follows:
 * **Warning:** represents a call to attention to a symptom with high chances to develop into an issue.
 * **Alert:** represents an active issue that requires immediate action.

For each threshold level, use the following table as a guide to set up your own alert rules:

Metric | Warning | Alert | Description
-- | -- | -- | --
CPU | 85% | 100% | Average CPU usage for a cluster in the last *15 minutes*.
Memory | 80% | 90% | Average memory usage for a cluster in the last *15 minutes*.
Source status | - | On Change | Source status change in the last *1 minute*.
Cluster status | - | On Change | Cluster replica status change in the last *1 minute*.
Freshness | > 5s | > 1m | Average [lag behind an input](https://materialize.com/docs/sql/system-catalog/mz_internal/#mz_materialization_lag) in the last *15 minutes*.

### Custom Thresholds

For the following table, replace the two variables, _X_ and _Y_, by your organization and use case:

Metric | Warning | Alert | Description
-- | -- | -- | --
Latency | Avg > X | Avg > Y | Average latency in the last *15 minutes*. Where X and Y are the expected latencies in milliseconds.
Credits | Consumption rate increase by X% | Consumption rate increase by Y% | Average credit consumption in the last *60 minutes*.

## Maintenance window

Materialize has a [release and a maintenance window almost every week](https://materialize.com/docs/releases/) at a defined [schedule](https://materialize.com/docs/releases/#schedule). We announce every maintenance window on the [status page](https://status.materialize.com/), where you can subscribe to updates and receive alerts for this, or an unexpected incident at Materialize.

After an upgrade, you’ll experience a few minutes of downtime and the rehydration process. Alerts may get triggered during this brief period of time. For this case, you can configure your monitoring tool to avoid unnecessary alerts as follows:

* [Datadog downtimes](https://docs.datadoghq.com/monitors/downtimes/)
* [Grafana mute timings](https://grafana.com/docs/grafana/latest/alerting/manage-notifications/mute-timings/)
