---
title: "OpenTelemetry and remote write"
description: "How to push metrics from Self-Managed Materialize to an OTLP endpoint, a Prometheus remote-write store, or Google Cloud Monitoring."
menu:
  main:
    parent: "monitor-sm"
    weight: 10
    identifier: "otlp-sm"
---

The [monitoring stack](/manage/monitor/self-managed/grafana/) collects metrics
into a Grafana Alloy gateway and remote-writes them to the bundled Thanos. The
gateway can also push a copy to backends outside the cluster, so a central
observability platform sees Materialize without anyone querying the stack.

Each destination is independent, and each gets its own copy of the metrics with
its own filter. Full-fidelity local storage in Thanos and a smaller, cheaper
slice to a metered SaaS backend are the same install, not a tradeoff.

| Destination | Configured with | Notes |
|-------------|-----------------|-------|
| **OTLP** (Honeycomb, Grafana Cloud, your own OpenTelemetry Collector) | `otlp_metrics` | Additive. Thanos is unaffected. |
| **Datadog** | `datadog_metrics` | Additive. See [Datadog](/manage/monitor/self-managed/datadog/). |
| **Google Cloud Monitoring** | `enable_google_cloud_metrics` | Additive, GCP only. Authenticates with Workload Identity. |
| **Prometheus remote write** (Mimir, Amazon Managed Prometheus, Grafana Cloud, another Thanos) | `additional_values` | **Not** additive. This is the destination the bundled Thanos occupies. |

All of these need **TF v12.0.0** or later.

## Before you begin

Ensure you have:

- The monitoring stack installed, with `enable_observability = true`. See
  [Grafana](/manage/monitor/self-managed/grafana/).

- The endpoint and credential for your backend. Which credential depends on the
  backend: an API-key header, a bearer token, basic auth, or a cloud identity.

## Choose what each destination receives

Every metric the stack collects carries an *importance* tier, and each
destination keeps only the metrics at or above a floor you choose. The tiers
below run from most to least important, and the floor is cumulative: it keeps
that tier and every tier above it.

| Tier | What it covers |
|------|----------------|
| `essential` | The metrics that are critical and that you would always want available. These are the ones used in alerting. |
| `recommended` | The metrics used in dashboards, and generally desirable for troubleshooting. |
| `extended` | The metrics used by optional and experimental dashboards. |
| `diagnostic` | The metrics used for in-depth troubleshooting and analysis. |
| `all` | Absolutely everything scraped, including metrics no tier classifies. Suited to cheap storage such as the bundled Thanos, not to a metered backend. |

For OTLP the floor defaults to `recommended`, and the bundled Thanos keeps
`all`. The tiers are shared across the stack, so a tier selected in Terraform
means the same set of metrics as the same tier selected in Helm. For the
membership of each tier, see [List of metrics
⧉](https://materializeinc.github.io/materialize-monitoring/reference/stable-metrics/list-metrics/).
For the metrics Materialize recommends dashboarding and alerting on, see
[essential metrics](/manage/monitor/essential-metrics/), and for everything it
exposes, the [appendix of all metrics](/manage/monitor/appendix-metrics/).

Treat this as the cost control on a metered backend, and reach for it before you
reach for anything else.

{{< note >}}
The `extended` and `diagnostic` tiers are still being populated, so today they
resolve to the same set as `recommended`. To send everything that is scraped,
use `all`, not `diagnostic`.
{{< /note >}}

{{< warning >}}
The filter fails open. If the allowlist reaches the gateway empty, the gateway
sends everything to that destination rather than nothing. That is safe for
visibility and expensive on a metered backend, so check your backend's ingest
volume after a configuration change.
{{< /warning >}}

## Export to an OTLP endpoint

OTLP is configured on the `monitoring` module block, not through a root variable
of the examples. It provisions no cloud resources, so there is no `enable_otlp`
toggle: setting `otlp_metrics` is what turns it on. The examples ship the block
commented out, so you can uncomment it in place.

```hcl
module "monitoring" {
  # ...

  otlp_metrics = {
    url            = "api.honeycomb.io"
    min_importance = "recommended"
    auth_headers   = { "x-honeycomb-dataset" = "mzmon" }
  }
  otlp_auth_header_secrets = { "x-honeycomb-team" = var.honeycomb_api_key }
}
```

| Field | Default | Purpose |
|-------|---------|---------|
| `url` | required | The endpoint as `host[:port]`, with **no** scheme. |
| `protocol` | `grpc` | `grpc` for OTLP/gRPC, `http` for OTLP/HTTP. |
| `compression` | unset | `gzip` for compatibility, `snappy` for throughput. |
| `min_importance` | `recommended` | See [Choose what each destination receives](#choose-what-each-destination-receives). |
| `auth_headers` | `{}` | **Non-secret** request headers, such as a dataset or tenant name. |

{{< warning >}}
`url` takes no scheme. A `https://` prefix fails when the gateway starts, not at
plan time.
{{< /warning >}}

### Supply the credential

Two inputs carry credentials, and they are mutually exclusive because the gateway
has a single auth slot per OTLP destination. Setting both fails the plan rather
than silently dropping one.

| Input | Use when |
|-------|----------|
| `otlp_auth_header_secrets` | The backend authenticates with an API-key header. This is the common case: Honeycomb's `x-honeycomb-team`, and most OTLP vendors. |
| `otlp_auth_bearer_token` | The backend takes `Authorization: Bearer`. |

Declare the value as a sensitive variable and pass it the way you pass other
secrets:

```hcl
variable "honeycomb_api_key" {
  type      = string
  sensitive = true
}
```

```bash
export TF_VAR_honeycomb_api_key='<your-api-key>'
```

Credentials do not travel through the Helm values. The module puts them in a
Kubernetes Secret that the gateway mounts, so they are not recoverable with
`helm get values`. Rotating one rolls the gateway, because environment variables
are fixed at container start and a running pod would otherwise keep
authenticating with the credential it started with.

{{< note >}}
`auth_headers` renders into the gateway's config as literals, so anything secret
belongs in `otlp_auth_header_secrets` instead. Both compose into one header set,
so a non-secret dataset header and a secret key header work together.
{{< /note >}}

Apply the configuration:

```bash
terraform apply
```

### Forward logs to the same endpoint

The OTLP destination can also carry the logs the stack collects. Loki continues
to receive them either way. Enable it through `additional_values`:

```hcl
additional_values = [
  <<-EOT
    pipeline:
      logging:
        gateway:
          destination:
            otel:
              enabled: true
  EOT
]
```

The switch is not per-destination: it turns on the log path to every logs-capable
exporter the gateway has enabled, so a configured [Datadog
destination](/manage/monitor/self-managed/datadog/) receives the logs too. Google
Cloud Monitoring is metrics-only and cannot receive them, and enabling the switch
with no logs-capable exporter configured fails the install rather than silently
dropping the logs.

Logs are considerably higher volume than metrics. Turn this on deliberately.

## Export to a Prometheus remote-write store

Unlike the destinations above, remote write is not additive. It is the single
destination the bundled Thanos occupies, so pointing it at an external store
means Thanos stops receiving metrics, and the bundled Grafana dashboards go
empty unless you also repoint their data source.

{{< warning >}}
Repoint remote write only when the external store is replacing Thanos, not when
you want a second copy. For a second copy, use the OTLP destination above.
{{< /warning >}}

Set it through `additional_values` on the `monitoring` module block:

```hcl
additional_values = [
  <<-EOT
    pipeline:
      metrics:
        gateway:
          destination:
            prometheusRemoteWrite:
              url: https://<your-endpoint>/api/v1/write
              authType: basicAuth
              minMetricImportance: all
  EOT
]
```

`authType` is one of `none`, `basicAuth`, `bearer`, `oauth2`, or `sigv4`. Supply
the credential itself through the gateway's Secret rather than inline in the
values, which would bake it into the gateway's ConfigMap in plaintext. For
Amazon Managed Prometheus, `sigv4` needs no credential at all: it signs with the
gateway pod's IRSA identity.

Every sample carries a `cluster` label identifying the deployment it came from,
so set it per install to keep series from different deployments distinct once
they land in a shared store. See [Metrics > Storing
⧉](https://materializeinc.github.io/materialize-monitoring/metrics/storing/) for
the Secret's keys, the per-`authType` blocks, and the SigV4 setup.

## Export to Google Cloud Monitoring

On GCP, Cloud Monitoring is the one destination that needs cloud resources the
chart cannot create, so the modules surface it as flat variables rather than a
block. Setting `enable_google_cloud_metrics` creates a service account with
`roles/monitoring.metricWriter` and binds the gateway to it through Workload
Identity.

```hcl
module "monitoring" {
  # ...

  enable_google_cloud_metrics         = true
  google_cloud_metrics_min_importance = "recommended"
}
```

`google_cloud_metrics_prefix` sets the metric name prefix, defaulting to
`workload.googleapis.com/mzmon`.

{{< note >}}
Authentication is Application Default Credentials only, and there is no key-file
path. Without the Workload Identity binding the module creates, the exporter
falls back to the node's service account, which works only if that account
happens to hold `roles/monitoring.metricWriter`.
{{< /note >}}

## Confirm metrics are arriving

1. Check that the gateway restarted and is healthy:

   ```bash
   kubectl -n monitoring rollout status deployment/alloy-gateway
   ```

1. Query your backend for recent samples of a metric you expect, such as
   `mz_dataflow_wallclock_lag_seconds`.

{{< note >}}
A backend's schema or column browser is cumulative, so a metric listed there is
not proof it is arriving now. Query for recent samples instead.
{{< /note >}}

{{< warning >}}
The gateway shards scrape targets across its replicas. During a partial rollout
a metric can look missing simply because its target is being scraped by a pod
that has not picked up the new configuration yet. Let all gateway replicas roll
out before concluding that a metric is being filtered.
{{< /warning >}}

## Installing with Helm

If you install the `materialize-monitoring` chart directly rather than through
the Terraform modules, every destination on this page is a chart value, and
credentials go in a Secret you create. See [Metrics > Storing
⧉](https://materializeinc.github.io/materialize-monitoring/metrics/storing/) for
the values, the Secret's name and keys, and the environment variable each
credential becomes.

## Other destinations

- [Datadog](/manage/monitor/self-managed/datadog/), which follows the same
  additive model with its own exporter and cost profile.

- [Grafana](/manage/monitor/self-managed/grafana/), for the bundled stack and
  the query endpoints that existing tooling can read.
