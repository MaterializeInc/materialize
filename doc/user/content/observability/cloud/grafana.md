---
title: "Grafana"
description: "How to monitor the performance and overall health of your Materialize region using Grafana."
menu:
  main:
    parent: "monitor-cloud"
    weight: 10
aliases:
  - /manage/monitor/cloud/grafana/
---

{{< warning >}}
DO NOT MERGE. Early draft, shared for customer and internal feedback on the
[Materialize Cloud metrics](/observability/cloud/cloud-metrics-draft/) design.
Not official documentation; names and endpoints will change.
{{</ warning >}}

Connect Grafana directly to Materialize Cloud metrics. There is nothing to run:
no Prometheus SQL exporter and no scraper.

## Before you begin

- A metrics token. See [Create a metrics token](/observability/cloud/cloud-metrics-draft/#step-1-create-a-metrics-token).
- Grafana or Grafana Cloud.

## Step 1: Add the data source

In Grafana, add a **Prometheus** data source:

| Field | Value |
| --- | --- |
| URL | `https://<telemetry endpoint>/prometheus` |
| Custom HTTP header | `Authorization`: `Bearer <token>` |

Click **Save & test**. (Draft: hostname not yet decided.)

## Step 2: Import the dashboards

Download the dashboard JSON from **Settings > Monitoring > Grafana** in the
[Materialize Console](https://console.materialize.com/) and import it into
Grafana.

## Step 3: Set up alerts

Create alert rules on the data source above. For recommended metrics and
thresholds, see [Alerting](/observability/cloud/alerting/).

## Already running Prometheus?

You can scrape Materialize into your own Prometheus or Grafana Alloy instead,
and point Grafana at that. See
[Scrape with Prometheus or the Datadog Agent](/observability/cloud/cloud-metrics-draft/#optional-scrape-with-prometheus-or-the-datadog-agent).
