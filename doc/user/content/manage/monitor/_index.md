---
title: "Monitoring and alerting"
description: "Monitor the performance of your Materialize region with Datadog and Grafana."
disable_toc: true
disable_list: true
menu:
  main:
    parent: "manage"
    identifier: "monitor"
    weight: 15
---

### Monitoring

You can monitor the performance and overall health of your Materialize region.
To help you get started, the following guides are available:
- [Prometheus Community Helm Chart](/manage/monitor/prometheus-community/)

- [Datadog using Prometheus SQL Exporter](/manage/monitor/datadog/)

- [Grafana using Prometheus SQL Exporter](/manage/monitor/grafana/)

### Alerting

After setting up a monitoring tool, you can configure alert rules. Alert rules
send a notification when a metric surpasses a threshold. This will help you
prevent operational incidents. For alert rules guidelines, see
[Alerting](/manage/monitor/alerting/).
