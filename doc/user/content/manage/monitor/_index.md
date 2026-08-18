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

## Cloud

### Monitoring

You can monitor the performance and overall health of your Materialize region.
To help you get started, the following guides are available:

- [Datadog](/manage/monitor/cloud/datadog/)

- [Grafana](/manage/monitor/cloud/grafana/)

### Alerting

After setting up a monitoring tool, you can configure alert rules. Alert rules
send a notification when a metric surpasses a threshold. This will help you
prevent operational incidents. For alert rules guidelines, see
[Alerting](/manage/monitor/cloud/alerting/).

## Self-Managed

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
