---
title: "Datadog"
description: "How to monitor the performance and overall health of your Materialize region using Datadog."
menu:
  main:
    parent: "monitor-cloud"
    weight: 5
aliases:
  - /manage/monitor/cloud/datadog/
---

{{< warning >}}
DO NOT MERGE. Early draft, shared for customer and internal feedback on the
[Materialize Cloud metrics](/observability/cloud/cloud-metrics-draft/) design.
Not official documentation; names and endpoints will change.
{{</ warning >}}

Collect Materialize Cloud metrics with the Datadog Agent you already run. The
Agent scrapes the Materialize scrape endpoint; no Prometheus SQL exporter
needed.

## Before you begin

- A metrics token. See [Create a metrics token](/observability/cloud/cloud-metrics-draft/#step-1-create-a-metrics-token).
- A Datadog Agent with network access to the internet.

## Step 1: Configure the OpenMetrics check

Add a `conf.d/openmetrics.d/conf.yaml` to your Agent:

```yaml
instances:
  - openmetrics_endpoint: 'https://<telemetry endpoint>/federate?match[]={__name__=~"mz_.*"}'
    namespace: materialize
    metrics:
      - mz_.*
    headers:
      Authorization: Bearer <token>
```

Restart the Agent. Metrics appear in Datadog under the `materialize.` namespace.

(Draft: the path, `match[]` requirement, and recommended collection interval are
not final.)

## Step 2: Build dashboards and monitors

Start from the metrics in
[Available metrics](/observability/cloud/cloud-metrics-draft/#available-metrics).
For recommended thresholds, see [Alerting](/observability/cloud/alerting/).

(Draft: a native Datadog integration that pushes metrics with only an API key is
planned for a later release.)
