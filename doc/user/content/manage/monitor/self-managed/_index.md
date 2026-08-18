---
title: "Self-Managed"
description: "Monitor the performance of your Self-Managed Materialize region with Datadog and Grafana."
disable_toc: true
disable_list: true
aliases:
  - /self-managed/v25.2/manage/monitor/
  - /self-managed/v25.2/manage/monitor/prometheus/
  - /self-managed/v25.2/manage/monitor/datadog/
  - /self-managed/v25.2/manage/monitor/alerting/
menu:
  main:
    parent: "monitor"
    identifier: "monitor-sm"
    weight: 15
---

This section covers monitoring and alerting for Self-Managed Materialize.

### Monitoring

You can monitor the performance and overall health of your Self-Managed
Materialize.

To help you get started, the following guides are available:

- [Grafana](/manage/monitor/self-managed/grafana/), for the monitoring stack the
  Terraform modules install. Enabled by default starting in TF v12.0.0.

- [Datadog](/manage/monitor/self-managed/datadog/), for exporting the collected
  metrics to Datadog.

- [OpenTelemetry and remote
  write](/manage/monitor/self-managed/opentelemetry/), for exporting to an OTLP
  endpoint, a Prometheus remote-write store, or Google Cloud Monitoring.


### Alerting

After setting up a monitoring tool, you can configure alert rules. Alert rules
send a notification when a metric surpasses a threshold. This will help you
prevent operational incidents. For alert rules guidelines, see
[Alerting](/manage/monitor/self-managed/alerting/).
