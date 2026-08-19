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

The Terraform modules install a monitoring stack alongside your deployment,
enabled by default starting in TF v12.0.0. Start with what it stores and where
else that data can go:

- [Metric storage](/manage/monitor/self-managed/metric-store/)

- [Log storage](/manage/monitor/self-managed/log-store/)

- [Grafana](/manage/monitor/self-managed/grafana/), the dashboards and query
  interface that ship with the stack.

To send metrics and logs to a platform you already run, a guide is available for
each destination:

- [Datadog](/manage/monitor/self-managed/datadog/)

- [Honeycomb](/manage/monitor/self-managed/honeycomb/)

- [OpenTelemetry](/manage/monitor/self-managed/opentelemetry/), for any other OTLP
  endpoint, including your own collector.

- [Google Cloud Monitoring](/manage/monitor/self-managed/google-cloud-monitoring/)

- [Prometheus remote
  write](/manage/monitor/self-managed/prometheus-remote-write/), for Mimir,
  Amazon Managed Prometheus, Grafana Cloud, or a Thanos you run elsewhere.


### Alerting

After setting up a monitoring tool, you can configure alert rules. Alert rules
send a notification when a metric surpasses a threshold. This will help you
prevent operational incidents. For alert rules guidelines, see
[Alerting](/manage/monitor/self-managed/alerting/).
