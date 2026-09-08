---
title: "Monitoring and alerting"
description: "Monitor the performance of your Materialize region with Datadog and Grafana."
disable_toc: true
disable_list: true
menu:
  main:
    identifier: "monitor"
    name: "Observability"
    weight: 90
aliases:
  - /manage/monitor/
---

## Cloud

### Monitoring

You can monitor the performance and overall health of your Materialize region.
To help you get started, the following guides are available:

- [Datadog](/observability/cloud/datadog/)

- [Grafana](/observability/cloud/grafana/)

### Alerting

After setting up a monitoring tool, you can configure alert rules. Alert rules
send a notification when a metric surpasses a threshold. This will help you
prevent operational incidents. For alert rules guidelines, see
[Alerting](/observability/cloud/alerting/).

## Self-Managed

### Monitoring

You can monitor the performance and overall health of your Self-Managed
Materialize.

{{% include-headless "/headless/monitoring/self-managed-stack" %}}

{{% include-headless "/headless/monitoring/self-managed-destinations" %}}

### Alerting

{{% include-headless "/headless/monitoring/self-managed-alerting" %}}
