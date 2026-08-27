---
title: "Honeycomb"
description: "How to monitor the performance and overall health of Self-Managed Materialize using Honeycomb."
menu:
  main:
    parent: "monitor-sm"
    weight: 6
    identifier: "honeycomb-sm"
---

This guide walks you through the steps required to monitor the performance and
overall health of your Materialize region using [Honeycomb
⧉](https://www.honeycomb.io/). Self-Managed Materialize pushes metrics, and
optionally logs, to Honeycomb over OTLP from the monitoring stack the Materialize
Terraform modules install.

Honeycomb is an OpenTelemetry destination, so the mechanism is the one described
in [OpenTelemetry](/manage/monitor/self-managed/opentelemetry/). This page covers
the Honeycomb-specific parts: the endpoint, the two request headers it expects,
and which of them is a secret.

## How it works

The stack collects metrics and logs before any destination is involved. For the
collection pipeline and where that data is stored by default, see [How logs and
metrics are stored](/manage/monitor/self-managed/storage/#how-it-works).

Honeycomb is an **additive** destination. It receives its own filtered copy of the
metrics, and the bundled [Thanos](/manage/monitor/self-managed/storage/),
Grafana, and Alertmanager keep working as before.

Honeycomb authenticates with an API-key **request header** rather than a bearer
token, and it takes the target dataset as a second header. That split matters
here, because the two headers are configured in different places: the dataset
renders into the gateway's configuration as a literal, and the API key is
delivered through a Secret.

## Instructions

### Before you begin

{{% include-headless "/headless/monitoring/before-you-begin" %}}

You also need:

- A Honeycomb [API key
  ⧉](https://docs.honeycomb.io/configure/environments/manage-api-keys/) with
  permission to send events, from the environment you want the metrics in.

- The name of the Honeycomb **dataset** the metrics should land in. Honeycomb
  creates it on first write, so this is a name you choose rather than one you look
  up.

- Your Honeycomb region's endpoint: `api.honeycomb.io`, or
  `api.eu1.honeycomb.io` for the EU instance.

### Step 1. Enable observability

{{% include-headless "/headless/monitoring/enable-observability" %}}

### Step 2. Configure the Honeycomb destination

Honeycomb is configured on the `monitoring` module block, not through a root
variable of the examples. It provisions no cloud resources, so there is no
`enable_honeycomb` toggle: setting `otlp_metrics` is what turns it on.

1. In the `monitoring` module block of your Terraform, add:

   ```hcl
   module "monitoring" {
     # ...

     otlp_metrics = {
       url            = "api.honeycomb.io"
       protocol       = "grpc"
       min_importance = "recommended"
       auth_headers   = { "x-honeycomb-dataset" = "mzmon" }
     }
     otlp_auth_header_secrets = { "x-honeycomb-team" = var.honeycomb_api_key }
   }
   ```

   The examples ship this block commented out, so you can uncomment it in place.

   | Field | Value | Why |
   |-------|-------|-----|
   | `url` | `api.honeycomb.io` | A `host[:port]` with **no** scheme. |
   | `protocol` | `grpc` | Honeycomb accepts OTLP over gRPC and HTTP. `grpc` is the default. |
   | `min_importance` | `recommended` | Honeycomb is metered. See [How to control which metrics Honeycomb receives](#how-to-control-which-metrics-honeycomb-receives). |
   | `auth_headers` | `x-honeycomb-dataset` | The target dataset. Not a secret, so it goes here and renders inline. |
   | `otlp_auth_header_secrets` | `x-honeycomb-team` | The API key. Delivered through a Secret. |

   {{< warning >}}
   `url` takes no scheme. A `https://` prefix fails when the gateway starts, not
   at plan time.
   {{< /warning >}}

   {{< note >}}
   Put the API key in `otlp_auth_header_secrets`, never in
   `otlp_metrics.auth_headers`. The latter renders its values into the gateway's
   configuration in plaintext. The two compose into one header set, so the
   non-secret dataset header and the secret key header work together.
   {{< /note >}}

1. Declare the API key as a sensitive variable and pass it in the way you pass
   other secrets, for example through an environment variable:

   ```hcl
   variable "honeycomb_api_key" {
     type      = string
     sensitive = true
   }
   ```

   ```bash
   export TF_VAR_honeycomb_api_key='<your-honeycomb-api-key>'
   ```

1. Apply the configuration:

   ```bash
   terraform apply
   ```

{{% include-headless "/headless/monitoring/gateway-credentials" %}}

### Step 3. Confirm metrics are arriving

{{% include-headless "/headless/monitoring/confirm-metrics" %}}

In Honeycomb, select the dataset you named and query for a recent metric.

{{< note >}}
Honeycomb's schema view is cumulative, so a metric shown there may predate a
configuration change. Confirm against a recent time window.
{{< /note >}}

### Step 4. Build alerts

Build Honeycomb [triggers
⧉](https://docs.honeycomb.io/investigate/alerts/triggers/) from the metrics and
thresholds in [Alerting](/manage/monitor/self-managed/alerting/).

The monitoring stack also ships Alertmanager rules that evaluate against the
bundled Thanos. Decide which system owns which alerts rather than running both
against the same thresholds.

## How to control which metrics Honeycomb receives

Honeycomb is metered, so the volume you send is a cost decision.

{{% include-headless "/headless/monitoring/metric-tiers" %}}

`otlp_metrics.min_importance` defaults to `recommended`, which covers the metrics
the dashboards and alerts use. `all` is a diagnostic setting, not a steady state.

## How to forward logs

{{% include-headless "/headless/monitoring/forward-logs" %}}

For the log storage options in full, see
[How logs and metrics are stored](/manage/monitor/self-managed/storage/).

## Instructions when using Helm

If you install the `materialize-monitoring` chart directly rather than through the
Terraform modules, the destination is a chart value and the API key is a Secret
you create.

1. Enable the generic OTLP exporter and set header auth:

   ```yaml
   pipeline:
     metrics:
       gateway:
         destination:
           otel:
             enabled: true
             otlpExporter:
               enabled: true
               url: api.honeycomb.io
               protocol: grpc
               minMetricImportance: recommended
             auth:
               authType: headers
               headers:
                 headers:
                   - key: x-honeycomb-team
                     valueEnv: GATEWAY_OTEL_DEST_HONEYCOMB_API_KEY
                   - key: x-honeycomb-dataset
                     value: mzmon
   ```

   Each header sets exactly one of `value` or `valueEnv`. `value` renders into the
   gateway's configuration in plaintext, so keep it for routing headers such as
   the dataset; `valueEnv` names an environment variable the gateway reads at
   startup, which is where the credential belongs. The variable name is yours to
   pick.

1. Create the gateway Secret with the API key. The chart does not create it, and
   mounts it optionally, so a wrong name or namespace is ignored silently rather
   than failing:

   ```bash
   kubectl create secret generic mzmon-alloy-gateway-env \
     --namespace monitoring \
     --from-literal=GATEWAY_OTEL_DEST_HONEYCOMB_API_KEY='<your-honeycomb-api-key>'
   ```

The chart validates the header shape at render time: an empty header list, a
header missing its `key`, a header setting both `value` and `valueEnv` or neither,
and a `valueEnv` that nothing could supply all fail the install rather than
authenticating with an empty header at run time.

A ready-made starting point for exactly this setup lives at
[`otlp-metrics-honeycomb.values.yaml`
⧉](https://github.com/MaterializeInc/materialize-monitoring/blob/main/charts/materialize-monitoring/profiles/otlp-metrics-honeycomb.values.yaml).

## See also

- [OpenTelemetry](/manage/monitor/self-managed/opentelemetry/), for OTLP
  destinations generally, including bearer-token authentication and your own
  collector.

- [How logs and metrics are stored](/manage/monitor/self-managed/storage/), for the
  bundled stores and the other backends you can send metrics and logs to.

- [Alerting](/manage/monitor/self-managed/alerting/), for the metrics and
  thresholds to alert on.
