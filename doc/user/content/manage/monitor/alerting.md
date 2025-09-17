---
title: "Alerting"
description: "Alerting thresholds to use for monitoring."
menu:
  main:
    parent: "monitor"
    weight: 15
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
Memory utilization | 80% | 90% | Average memory utilization for a cluster in the last *15 minutes*.
Source status | - | On Change | Source status change in the last *1 minute*.
Cluster status | - | On Change | Cluster replica status change in the last *1 minute*.
Freshness | > 5s | > 1m | Average [lag behind an input](/sql/system-catalog/mz_internal/#mz_materialization_lag) in the last *15 minutes*.

NOTE: Customers on legacy cluster sizes should still monitor their Memory usage. Please [contact support](/support/) for questions.

### Custom Thresholds

For the following table, replace the two variables, _X_ and _Y_, by your organization and use case:

Metric | Warning | Alert | Description
-- | -- | -- | --
Latency | Avg > X | Avg > Y | Average latency in the last *15 minutes*. Where X and Y are the expected latencies in milliseconds.
Credits | Consumption rate increase by X% | Consumption rate increase by Y% | Average credit consumption in the last *60 minutes*.

## Maintenance window

Materialize has a [release and a maintenance window almost every week](/releases/) at a defined [schedule](/releases/#schedule).

During an upgrade, clients may experience brief connection interruptions, but the service otherwise remains fully available. Alerts may get triggered during this brief period of time. For this case, you can configure your monitoring tool to avoid unnecessary alerts as follows:

* [Datadog downtimes](https://docs.datadoghq.com/monitors/downtimes/)
* [Grafana mute timings](https://grafana.com/docs/grafana/latest/alerting/manage-notifications/mute-timings/)

## Status Page

All performance‑impacting incidents are communicated on our [status page](https://status.materialize.com/), which provides current and historical incident details and subscription options for notifications. Reviewing this page is the quickest way to confirm if a known issue is affecting your database.
