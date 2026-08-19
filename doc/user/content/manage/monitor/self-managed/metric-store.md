---
title: "Metric storage"
description: "Where Self-Managed Materialize stores the metrics it collects, and the backends you can send them to."
menu:
  main:
    parent: "monitor-sm"
    weight: 1
    identifier: "metric-store-sm"
---

The [Materialize Terraform
modules](/self-managed-deployments/installation/#install-using-terraform-modules)
install a monitoring stack alongside your deployment that collects metrics from
Materialize and from the cluster, stores them, and can forward them to backends
you already run.

This page covers where those metrics are stored and what your options are. The
guide for each backend covers its own requirements and full setup steps.

## How it works

{{< include-md file="content/headless/monitoring/how-it-works.md" >}}

## The bundled metric store

Metrics are stored in **Thanos**, running in the `monitoring` namespace and
persisting to object storage that the Terraform modules create. You do not
interact with it directly: the bundled Grafana queries it, and the
[Alertmanager](/manage/monitor/self-managed/alerting/) rules evaluate against it.

It is worth knowing three things about it.

**It presents one Prometheus-compatible endpoint.** Thanos Query federates recent
data and historical data behind a single PromQL API, so any tool that speaks the
Prometheus query API works against it unchanged. The examples publish the address
as a Terraform output:

```bash
terraform output -raw metrics_url
```

**Storage is object storage, not disks you size.** Thanos Receive accepts the
gateway's writes and uploads blocks to the bucket. A store gateway serves
historical blocks back out of it for queries, and a compactor compacts and
downsamples them. There is no volume to grow as retention increases, only cost.

**Retention is per resolution.** The compactor keeps three resolutions with
independent retention, so a year-wide query reads hourly blocks rather than raw
samples:

| Resolution | Default retention |
|------------|-------------------|
| raw | 30 days |
| 5 minute | 90 days |
| 1 hour | 365 days |

Tune these to trade storage cost against how far back high-resolution data stays
available. For the sizing profiles, the component layout, and the object-storage
configuration, see [Metrics > Storing
⧉](https://materializeinc.github.io/materialize-monitoring/metrics/storing/).

## Other metric storage backends

The gateway can send metrics to backends outside the cluster. There are two
shapes to this, and the difference matters more than the choice of vendor:

**Additive destinations** run alongside Thanos. Each one receives its own copy of
the metrics, with its own filter, so full-fidelity local storage and a smaller,
cheaper slice to a metered platform are the same install rather than a tradeoff.

**Prometheus remote write is a replacement.** It is the single sink the bundled
Thanos already occupies, so pointing it at an external store means Thanos stops
receiving metrics.

| Backend | Shape | Guide |
|---------|-------|-------|
| Thanos, in-cluster | the default sink | This page |
| Datadog | additive | [Datadog](/manage/monitor/self-managed/datadog/) |
| Honeycomb | additive, over OTLP | [Honeycomb](/manage/monitor/self-managed/honeycomb/) |
| Any OTLP endpoint, including your own OpenTelemetry Collector | additive | [OpenTelemetry](/manage/monitor/self-managed/opentelemetry/) |
| Google Cloud Monitoring | additive, GCP only | [Google Cloud Monitoring](/manage/monitor/self-managed/google-cloud-monitoring/) |
| Mimir, Amazon Managed Prometheus, Grafana Cloud, another Thanos | replaces Thanos | [Prometheus remote write](/manage/monitor/self-managed/prometheus-remote-write/) |

Several additive destinations can run at once. A platform that accepts both OTLP
and remote write, such as Grafana Cloud, can therefore be reached either way, and
the additive OTLP path is the one to prefer if you want to keep Thanos.

## Choosing what each destination stores

{{< include-md file="content/headless/monitoring/metric-tiers.md" >}}

## Connect existing tooling

Anything that speaks the Prometheus query API can read the bundled store
directly, without a destination being configured for it:

```bash
terraform output -raw metrics_url
```

That is a pull model: your tooling queries the stack on its own schedule. The
destinations above are push models, where the gateway delivers metrics to the
backend. A platform you already run for other services usually wants the push
model, so its alerting and dashboards work without reaching into this cluster.

## See also

- [Log storage](/manage/monitor/self-managed/log-store/), for the same picture on
  the logging side.

- [Grafana](/manage/monitor/self-managed/grafana/), for the bundled dashboards
  and how to reach them.

- [Alerting](/manage/monitor/self-managed/alerting/), for the metrics and
  thresholds to alert on.
