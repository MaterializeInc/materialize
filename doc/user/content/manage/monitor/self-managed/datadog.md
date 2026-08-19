---
title: "Datadog"
description: "How to monitor the performance and overall health of Self-Managed Materialize using Datadog."
menu:
  main:
    parent: "monitor-sm"
    weight: 5
    identifier: "datadog-sm"
---

This guide walks you through the steps required to monitor the performance and
overall health of your Materialize region using [Datadog
⧉](https://www.datadoghq.com/). Self-Managed Materialize pushes metrics, and
optionally logs, to Datadog from the monitoring stack the Materialize Terraform
modules install.

## How it works

The stack collects metrics and logs before any destination is involved. For the
collection pipeline and where that data is stored by default, see [How logs and
metrics are stored](/manage/monitor/self-managed/storage/#how-it-works).

Datadog is an **additive** destination. It receives its own filtered copy of the
metrics, and the bundled [Thanos](/manage/monitor/self-managed/storage/),
Grafana, and Alertmanager keep working as before. You do not give anything up by
turning it on.

The exporter authenticates directly against the Datadog intake with an API key,
so the only decisions are which site to send to and how much to send.

## Instructions

### Before you begin

{{% include-headless "/headless/monitoring/before-you-begin" %}}

You also need:

- A [Datadog API key
  ⧉](https://docs.datadoghq.com/account_management/api-app-keys/). An application
  key is not needed and is not accepted: the metrics intake authenticates with
  the API key alone.

- Your [Datadog site ⧉](https://docs.datadoghq.com/getting_started/site/), such
  as `datadoghq.com`, `datadoghq.eu`, or `us3.datadoghq.com`. A wrong site is a
  403 from the intake rather than a routing error, so confirm it before you
  apply.

### Step 1. Enable observability

{{% include-headless "/headless/monitoring/enable-observability" %}}

### Step 2. Configure the Datadog destination

Datadog is configured on the `monitoring` module block, not through a root
variable of the examples. It provisions no cloud resources, so there is no
`enable_datadog` toggle: setting `datadog_metrics` is what turns it on.

1. In the `monitoring` module block of your Terraform, add:

   ```hcl
   module "monitoring" {
     # ...

     datadog_metrics = {
       site           = "datadoghq.com"
       min_importance = "essential"
     }
     datadog_api_key = var.datadog_api_key
   }
   ```

   The examples ship this block commented out, so you can uncomment it in place.

   | Field | Default | Purpose |
   |-------|---------|---------|
   | `site` | `datadoghq.com` | Your Datadog site. Determines the intake the exporter writes to. |
   | `min_importance` | `essential` | Which metrics to send. See [How to control which metrics Datadog receives](#how-to-control-which-metrics-datadog-receives). |
   | `metric_endpoint` | derived from `site` | Override the metrics intake URL. Only for a proxy or PrivateLink. |
   | `logs_endpoint` | derived from `site` | Override the logs intake URL. Only for a proxy or PrivateLink. |

   {{< warning >}}
   A hand-written `metric_endpoint` or `logs_endpoint` that disagrees with `site`
   fails at the intake, not at plan time. Leave both unset unless you are routing
   through a proxy.
   {{< /warning >}}

1. Declare the API key as a sensitive variable and pass it in the way you pass
   other secrets, for example through an environment variable:

   ```hcl
   variable "datadog_api_key" {
     type      = string
     sensitive = true
   }
   ```

   ```bash
   export TF_VAR_datadog_api_key='<your-datadog-api-key>'
   ```

1. Apply the configuration:

   ```bash
   terraform apply
   ```

{{% include-headless "/headless/monitoring/gateway-credentials" %}}

### Step 3. Confirm metrics are arriving

{{% include-headless "/headless/monitoring/confirm-metrics" %}}

In Datadog, **Metrics > Summary** filtered to `mz_` is the quickest place to look.

### Step 4. Build alerts

With metrics in Datadog, build [monitors ⧉](https://docs.datadoghq.com/monitors/)
from the metrics and thresholds in
[Alerting](/manage/monitor/self-managed/alerting/).

The monitoring stack also ships Alertmanager rules that evaluate against the
bundled Thanos. Decide which system owns which alerts rather than running both
against the same thresholds and paging twice.

## How to control which metrics Datadog receives

Datadog bills per custom metric, so the volume you send is a cost decision.

{{% include-headless "/headless/monitoring/metric-tiers" %}}

`datadog_metrics.min_importance` defaults to `essential`, a tighter floor than the
other destinations use, for exactly this reason. `all` is a diagnostic setting,
not a steady state.

## How to forward logs

{{% include-headless "/headless/monitoring/forward-logs" %}}

Datadog bills for logs separately from custom metrics. For the log storage
options in full, see [How logs and metrics are stored](/manage/monitor/self-managed/storage/).

## Instructions when using Helm

If you install the `materialize-monitoring` chart directly rather than through the
Terraform modules, the Datadog destination is a chart value and the API key is a
Secret you create.

1. Enable the exporter:

   ```yaml
   pipeline:
     metrics:
       gateway:
         destination:
           otel:
             enabled: true
             datadogExporter:
               enabled: true
               url: datadoghq.com
               minMetricImportance: essential
   ```

1. Create the gateway Secret with the API key. The chart does not create it, and
   mounts it optionally, so a wrong name or namespace is ignored silently rather
   than failing:

   ```bash
   kubectl create secret generic mzmon-alloy-gateway-env \
     --namespace monitoring \
     --from-literal=GATEWAY_OTEL_DEST_DATADOG_API_KEY='<your-datadog-api-key>'
   ```

   {{< warning >}}
   The Secret name must match the release, so with the default
   `fullnameOverride: mzmon` it is `mzmon-alloy-gateway-env`, in the namespace the
   gateway runs in. In production, source it from Sealed Secrets, External
   Secrets, or SOPS rather than committing a raw credential.
   {{< /warning >}}

For a ready-made starting point that fans metrics out to several backends at
once, see the [`otel-metrics-fanout.values.yaml`
⧉](https://github.com/MaterializeInc/materialize-monitoring/blob/main/charts/materialize-monitoring/profiles/otel-metrics-fanout.values.yaml)
profile, and for the full value reference, [Metrics > Storing
⧉](https://materializeinc.github.io/materialize-monitoring/metrics/storing/).

## See also

- [How logs and metrics are stored](/manage/monitor/self-managed/storage/), for the
  bundled stores and the other backends you can send metrics and logs to.

- [Honeycomb](/manage/monitor/self-managed/honeycomb/) and
  [OpenTelemetry](/manage/monitor/self-managed/opentelemetry/), which follow the
  same additive model over OTLP.

- [Alerting](/manage/monitor/self-managed/alerting/), for the metrics and
  thresholds to alert on.
