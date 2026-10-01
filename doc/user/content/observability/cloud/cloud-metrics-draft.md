---
title: "Exporting Metrics from Materialize Cloud"
description: "Query or scrape your Materialize Cloud metrics from Grafana, Prometheus, or Datadog."
menu:
  main:
    parent: "monitor-cloud"
    name: "Cloud metrics"
    identifier: "cloud-metrics"
    weight: 5
---

{{< warning >}}
Early draft, work in progress. Shared for customer and internal feedback on the
Materialize Cloud metrics design. Not official documentation; names, endpoints,
and limits will change.
{{</ warning >}}

Materialize Cloud collects metrics for every region you run in. Query them from
Grafana, or scrape them into your own Prometheus or Datadog.

Exporting metrics from Materialize Cloud is a good fit if you need:

- **Longer history**: weeks to months of history, not the roughly 1 day
  available through SQL today. (Draft: customer facing retention is not yet
  decided. The internal telemetry stack keeps 30 days raw and 1 year
  downsampled.)
- **Visibility during incidents**: metrics do not depend on your environment's
  SQL layer, so history stays readable when it is unreachable. You can also set
  alerts!
- **No load on your clusters**: reading metrics does not run SQL against your
  environment. (Draft: pricing for metrics reads is not yet decided.)
- **Bring Materialize metrics into your existing observability stack**: connect
  Grafana directly, or scrape with Prometheus or the Datadog Agent.

{{< note >}}
Intended to replace the [Prometheus SQL exporter setup](/observability/cloud/grafana/)
for most use cases. (Draft: whether the SQL exporter remains supported is still
open.)
{{</ note >}}

## How it works

Each cluster replica publishes a curated set of metrics directly. Materialize
stores them and serves them from a telemetry endpoint in each region. Every
request carries a token; the endpoint derives your organization from the token
and returns only your series. Infrastructure metrics (nodes, pods, the operator)
are managed by Materialize and are not exposed.

The endpoint offers two ways to read, with the same token:

- **Query API**: speaks PromQL, the Prometheus query language, and serves
  history from Materialize's store. Grafana connects with its built-in
  **Prometheus** data source type; you do not run Prometheus yourself.
- **Scrape endpoint**: returns the latest values in Prometheus text format, for
  your own Prometheus or Datadog Agent to collect and store.

## Available metrics (more to be added)

| Metric | What it tells you | Labels |
| --- | --- | --- |
| `mz_arrangement_size_bytes`, `mz_arrangement_records` | Memory held by each index, materialized view, and source | object, cluster, replica |
| `mz_dataflow_elapsed_seconds_total` | CPU time per dataflow | object, cluster, replica |
| `mz_dataflow_error_count` | Live error rows per object | object, cluster, replica |
| `mz_compute_metric_sink_frontier_ms` | Freshness of each metric set; stale if it stops advancing | sink |
| `mz_compute_metric_sink_errors` | Errors in a metric set; alert when above 0 | sink |

Labels include both IDs and names, so you do not need to join against the
catalog. (Draft: additional stable metrics, such as memory limits and active
sessions, are under consideration.)

Coming soon: the goal is to cover everything in
[Essential metrics](/observability/essential-metrics/). If you need a specific
metric, reach out to your Materialize representative.

## Prerequisites

- The **Organization Admin** role, to create metrics tokens. (Draft: which roles
  can create tokens is still open.)

## Step 1: Create a metrics token

In the [Materialize Console](https://console.materialize.com/), go to
**Settings > Monitoring > Tokens** and click **Create token**. Choose a name and
expiry. Copy the token; it is shown once.

A token never has more access than the person who created it. (Draft: whether a
token covers your whole organization or one region is still open.)

## Step 2: Add the data source

In Grafana, add a **Prometheus** data source:

| Field | Value |
| --- | --- |
| URL | `https://<telemetry endpoint>/prometheus` (Draft: hostname not yet decided) |
| Custom HTTP header | `Authorization`: `Bearer <token>` |

Click **Save & test**. (Draft: a Loki data source for logs, with a separate logs
scope on the token, is part of the design; logs are left out of this page for
now.)

## Step 3: Import the dashboards

Download the dashboard JSON from **Settings > Monitoring > Grafana** and import
it.

## Optional: Scrape with Prometheus or the Datadog Agent

Scrape `https://<telemetry endpoint>/federate` with the same bearer token. Your
Prometheus, Grafana Alloy, or Datadog Agent (OpenMetrics check) collects it like
any other target.

```yaml
scrape_configs:
  - job_name: materialize-cloud
    scheme: https
    metrics_path: /federate
    params:
      'match[]': ['{__name__=~"mz_.*"}']
    authorization:
      credentials: <token>
    static_configs:
      - targets: ['<telemetry endpoint>']
```

{{< note >}}
Draft: the path, recommended scrape interval, and `match[]` requirement are not
final.
{{</ note >}}

## Security model

- **Scoped tokens.** Your organization comes from the verified token, never from
  request headers, and each token reads only Materialize environment metrics.
- **No privilege escalation.** A token cannot be issued with more access than its
  creator holds.
- **Revocable.** Revoke a token at any time in **Settings > Monitoring > Tokens**.
  (Draft: the revocation window is still being finalized.)
- **Audited.** Reads, including refused ones, are logged with the token,
  endpoint, and query. (Draft: how customers access this audit trail is still
  open.)

## Limits

Queries are limited per organization by time range, step, concurrency, and
request rate. (Draft: values TBD.)

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `401 Unauthorized` | Token expired or revoked | Create a new token |
| `403` on a query | Query uses an infrastructure metric | Use environment metrics only |
| No data for a cluster | Introspection disabled on the replica | Enable introspection |

## Frequently asked questions

### Does reading metrics use my clusters?

No. Queries go to Materialize's telemetry store, not your clusters. (Draft:
pricing is not yet decided.)

### Should I use the query API or scraping?

Both use the same token and the same metrics.

- **Query API** if you want history with nothing to run. Query weeks back,
  including time before you connected anything.
- **Scraping** if you want Materialize next to everything else in your own
  Prometheus or Datadog. You store the history and set retention; history starts
  when you start scraping.

### Can Materialize push metrics to Datadog or an OpenTelemetry backend?

Not yet. Use the Datadog Agent or an OpenTelemetry Collector to scrape the
endpoint above. (Draft: push export and Console charts on these metrics are
planned for a later release.)

### Can I set up alerts?

Use Grafana alerting on the data source above.

## Need help?

Contact your Materialize representative, or reach the
[Materialize support team](https://materialize.com/contact).
