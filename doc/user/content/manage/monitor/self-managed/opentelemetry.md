---
title: "OpenTelemetry"
description: "How to push metrics and logs from Self-Managed Materialize to any OpenTelemetry-compatible destination."
menu:
  main:
    parent: "monitor-sm"
    weight: 7
    identifier: "otlp-sm"
---

This guide walks you through the steps required to monitor the performance and
overall health of your Materialize region using any OpenTelemetry-compatible
destination, such as Honeycomb, Grafana Cloud, or an OpenTelemetry Collector you
run yourself. Self-Managed Materialize pushes metrics, and optionally logs, over
OTLP from the monitoring stack the Materialize Terraform modules install.

Some destinations have their own guide, because their endpoint and authentication
are specific enough to be worth spelling out:

| Destination | Guide |
|-------------|-------|
| Honeycomb | [Honeycomb](/manage/monitor/self-managed/honeycomb/) |
| Datadog | [Datadog](/manage/monitor/self-managed/datadog/) |
| Google Cloud Monitoring | [Google Cloud Monitoring](/manage/monitor/self-managed/google-cloud-monitoring/) |
| Grafana Cloud, an OpenTelemetry Collector, or any other OTLP endpoint | This page |

If your platform speaks Prometheus remote write rather than OTLP, see
[Prometheus remote
write](/manage/monitor/self-managed/prometheus-remote-write/) instead. Prefer OTLP
where a platform accepts both: the OTLP path runs alongside the bundled metric
store, and remote write replaces it.

## How it works

{{< include-md file="content/headless/monitoring/how-it-works.md" >}}

An OTLP destination is **additive**. It receives its own filtered copy of the
metrics, and the bundled [Thanos](/manage/monitor/self-managed/metric-store/),
Grafana, and Alertmanager keep working as before. Several additive destinations
can run at once, each with its own filter.

## Before you begin

{{< include-md file="content/headless/monitoring/before-you-begin.md" >}}

You also need:

- Your destination's OTLP endpoint, as a `host[:port]` with no scheme, and whether
  it accepts OTLP over gRPC or HTTP.

- The credential it expects. The gateway supports an API-key request header or a
  bearer token, and the two are mutually exclusive.

## Step 1. Enable observability

{{< include-md file="content/headless/monitoring/enable-observability.md" >}}

## Step 2. Choose which metrics to deliver

Most OTLP platforms are metered, so the volume you send is a cost decision. Decide
the floor before you configure the destination, because it is the input you are
most likely to want to change later.

{{< include-md file="content/headless/monitoring/metric-tiers.md" >}}

`otlp_metrics.min_importance` defaults to `recommended`, which covers the metrics
the dashboards and alerts use. The bundled Thanos keeps `all` regardless, so
lowering this floor does not cost you local fidelity.

## Step 3. Export to an OTLP endpoint

The destination is configured on the `monitoring` module block, not through a root
variable of the examples. It provisions no cloud resources, so there is no
`enable_otlp` toggle: setting `otlp_metrics` is what turns it on.

1. In the `monitoring` module block of your Terraform, add:

   ```hcl
   module "monitoring" {
     # ...

     otlp_metrics = {
       url            = "otlp.example.com:4317"
       protocol       = "grpc"
       min_importance = "recommended"
     }
     otlp_auth_bearer_token = var.otlp_token
   }
   ```

   The examples ship this block commented out, so you can uncomment it in place.

   | Field | Default | Purpose |
   |-------|---------|---------|
   | `url` | required | The endpoint as `host[:port]`, with **no** scheme. |
   | `protocol` | `grpc` | `grpc` for OTLP/gRPC, `http` for OTLP/HTTP. |
   | `compression` | unset | `gzip` for compatibility, `snappy` for throughput. |
   | `min_importance` | `recommended` | Which metrics to send. See [Step 2](#step-2-choose-which-metrics-to-deliver). |
   | `auth_headers` | `{}` | **Non-secret** request headers, such as a dataset or tenant name. |

   {{< warning >}}
   `url` takes no scheme. A `https://` prefix fails when the gateway starts, not
   at plan time.
   {{< /warning >}}

1. Supply the credential. Two inputs carry credentials, and they are mutually
   exclusive because the gateway has a single auth slot per OTLP destination.
   Setting both fails the plan rather than silently dropping one.

   | Input | Use when |
   |-------|----------|
   | `otlp_auth_header_secrets` | The destination authenticates with an API-key header. This is how most OTLP vendors work. See [Honeycomb](/manage/monitor/self-managed/honeycomb/) for a worked example. |
   | `otlp_auth_bearer_token` | The destination takes `Authorization: Bearer`. |

   Declare the value as a sensitive variable and pass it the way you pass other
   secrets:

   ```hcl
   variable "otlp_token" {
     type      = string
     sensitive = true
   }
   ```

   ```bash
   export TF_VAR_otlp_token='<your-token>'
   ```

   {{< note >}}
   `auth_headers` renders its values into the gateway's configuration as literals,
   so anything secret belongs in `otlp_auth_header_secrets` instead. Non-secret
   routing headers and secret credential headers compose into one header set.
   {{< /note >}}

1. Apply the configuration:

   ```bash
   terraform apply
   ```

{{< include-md file="content/headless/monitoring/gateway-credentials.md" >}}

## Step 4. Confirm metrics are being delivered

{{< include-md file="content/headless/monitoring/confirm-metrics.md" >}}

## Step 5. Configure alerts

Build alerts in your destination from the metrics and thresholds in
[Alerting](/manage/monitor/self-managed/alerting/).

The monitoring stack also ships Alertmanager rules that evaluate against the
bundled Thanos. Decide which system owns which alerts rather than running both
against the same thresholds and paging twice.

## How to forward logs

{{< include-md file="content/headless/monitoring/forward-logs.md" >}}

For the log storage options in full, see [Log
storage](/manage/monitor/self-managed/log-store/).

## Instructions when using Helm

If you install the `materialize-monitoring` chart directly rather than through the
Terraform modules, the destination is a chart value and the credential is a Secret
you create.

1. Enable the generic OTLP exporter and pick an auth type:

   ```yaml
   pipeline:
     metrics:
       gateway:
         destination:
           otel:
             enabled: true
             otlpExporter:
               enabled: true
               url: otlp.example.com:4317
               protocol: grpc
               compression: gzip
               minMetricImportance: recommended
             auth:
               authType: bearer
   ```

   `authType` is one of `none`, `basic`, `bearer`, `headers`, `awsSigv4`, or
   `custom`. Authentication is configured once under `otel.auth` and shared by the
   OTLP exporter.

1. Create the gateway Secret with the credential. The chart does not create it,
   and mounts it optionally, so a wrong name or namespace is ignored silently
   rather than failing:

   ```bash
   kubectl create secret generic mzmon-alloy-gateway-env \
     --namespace monitoring \
     --from-literal=GATEWAY_OTEL_DEST_BEARER_TOKEN='<your-token>'
   ```

   | Auth type | Secret keys |
   |-----------|-------------|
   | `basic` | `GATEWAY_OTEL_DEST_USERNAME`, `GATEWAY_OTEL_DEST_PASSWORD` |
   | `bearer` | `GATEWAY_OTEL_DEST_BEARER_TOKEN` |
   | `headers` | whatever each header's `valueEnv` names, which you choose |
   | `awsSigv4` | none. It signs with the gateway pod's IRSA identity |

   {{< warning >}}
   The Secret name must match the release, so with the default
   `fullnameOverride: mzmon` it is `mzmon-alloy-gateway-env`, in the namespace the
   gateway runs in. In production, source it from Sealed Secrets, External Secrets,
   or SOPS rather than committing a raw credential.
   {{< /warning >}}

For the full value reference, see [Metrics > Storing
⧉](https://materializeinc.github.io/materialize-monitoring/metrics/storing/).

## See also

- [Metric storage](/manage/monitor/self-managed/metric-store/), for the bundled
  store and the other backends you can send metrics to.

- [Log storage](/manage/monitor/self-managed/log-store/), for the same picture on
  the logging side.

- [Alerting](/manage/monitor/self-managed/alerting/), for the metrics and
  thresholds to alert on.
