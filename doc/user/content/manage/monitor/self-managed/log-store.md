---
title: "Log storage"
description: "Where Self-Managed Materialize stores the logs it collects, and the backends you can send them to."
menu:
  main:
    parent: "monitor-sm"
    weight: 2
    identifier: "log-store-sm"
---

The monitoring stack the [Materialize Terraform
modules](/self-managed-deployments/installation/#install-using-terraform-modules)
install collects logs alongside metrics: container logs from every pod, and
Kubernetes events. It stores them in the cluster and can forward them to backends
you already run.

This page covers where those logs are stored and what your options are.

## How it works

{{< include-md file="content/headless/monitoring/how-it-works.md" >}}

Two details are specific to logs.

**Processing happens before storage.** The gateway normalizes log levels, reduces
label cardinality, and extracts structured metadata as the logs pass through. A
log that reaches an external destination arrives already normalized, the same as
one that lands in the bundled store.

**The agents' own logs take a second route.** Every other log reaches storage
because an agent tailed a file on its node. That would make the agent a single
point of failure for exactly the logs that explain why an agent failed, so the
gateway reads the agent pods' logs directly from the Kubernetes API instead. The
two paths share no process, host, or transport, and the direct one runs
constantly rather than activating during an incident.

## The bundled log store

Logs are stored in **Loki**, running in the `monitoring` namespace and persisting
to object storage that the Terraform modules create. As with metrics, you query
it through Grafana rather than directly, and the examples publish its read
endpoint as a Terraform output:

```bash
terraform output -raw logs_url
```

**Retention is enforced by Loki, not by bucket lifecycle rules.** The default is
30 days for everything. Loki also supports per-stream retention, which is the
main cost lever at volume: keep `ERROR` and audit-relevant streams far longer
than high-volume `INFO` chatter. A deletion API handles targeted deletes outside
the normal retention schedule.

{{< note >}}
Loki's ingesters run on node-local ephemeral storage rather than persistent
volumes, and durability comes from running at least three replicas. Scale them on
memory and stream cardinality rather than on bytes ingested.
{{< /note >}}

For the storage layout, the object-storage configuration, and disaster recovery,
see [Logs and events > Storing
⧉](https://materializeinc.github.io/materialize-monitoring/logs-and-events/storing/).

## Other log storage backends

The gateway can send logs outside the cluster, in the same two shapes as metrics:

**Additive destinations** run alongside Loki, each receiving its own copy. This is
the OpenTelemetry path, and it reuses whatever OTLP or Datadog destination you
have already configured for metrics.

**Repointing the bundled store is a replacement.** The Loki push destination is a
single endpoint, so aiming it at a Loki you run elsewhere means the bundled one no
longer receives logs. Running with the bundled Loki disabled entirely is a
supported topology: the agents and gateway still collect and process in-cluster,
and everything from storage onward lives somewhere else.

| Backend | Shape | Guide |
|---------|-------|-------|
| Loki, in-cluster | the default sink | This page |
| Datadog | additive, over OTLP | [Datadog](/manage/monitor/self-managed/datadog/) |
| Honeycomb | additive, over OTLP | [Honeycomb](/manage/monitor/self-managed/honeycomb/) |
| Any OTLP endpoint, including your own OpenTelemetry Collector | additive | [OpenTelemetry](/manage/monitor/self-managed/opentelemetry/) |
| A Loki you run elsewhere, or another cluster's gateway | replaces the bundled Loki | [Logs and events > Storing ⧉](https://materializeinc.github.io/materialize-monitoring/logs-and-events/storing/) |

{{< note >}}
Google Cloud Monitoring is the one destination that cannot receive logs. Its
exporter is metrics-only.
{{< /note >}}

## Forward logs to an OpenTelemetry destination

{{< include-md file="content/headless/monitoring/forward-logs.md" >}}

## Connect existing tooling

Anything that speaks Loki's query API can read the bundled store directly,
without a destination being configured for it:

```bash
terraform output -raw logs_url
```

## See also

- [Metric storage](/manage/monitor/self-managed/metric-store/), for the same
  picture on the metrics side.

- [Grafana](/manage/monitor/self-managed/grafana/), for querying logs alongside
  metrics in the bundled dashboards.
