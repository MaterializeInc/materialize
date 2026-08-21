---
title: "Google Cloud Monitoring"
description: "How to monitor the performance and overall health of Self-Managed Materialize using Google Cloud Monitoring."
menu:
  main:
    parent: "monitor-sm"
    weight: 8
    identifier: "gcm-sm"
---

This guide walks you through the steps required to monitor the performance and
overall health of your Materialize region using [Google Cloud Monitoring
⧉](https://cloud.google.com/monitoring). Self-Managed Materialize pushes metrics
to Cloud Monitoring from the monitoring stack the Materialize Terraform modules
install.

This destination is **GCP only**, and it is the one destination that needs cloud
resources the monitoring chart cannot create for itself. That is why it is enabled
with a flat variable rather than a configuration block: Terraform has to know at
plan time whether to create the service account and the Workload Identity binding
that authenticate it.

## How it works

The stack collects metrics and logs before any destination is involved. For the
collection pipeline and where that data is stored by default, see [How logs and
metrics are stored](/manage/monitor/self-managed/storage/#how-it-works).

Cloud Monitoring is an **additive** destination. It receives its own filtered copy
of the metrics, and the bundled [Thanos](/manage/monitor/self-managed/storage/),
Grafana, and Alertmanager keep working as before.

Authentication is **ambient**, not a credential you supply. The module creates a
Google service account, grants it `roles/monitoring.metricWriter`, and binds the
gateway's in-cluster ServiceAccount to it through Workload Identity. The exporter
then authenticates with Application Default Credentials.

{{< note >}}
Cloud Monitoring is metrics-only. Its exporter cannot receive logs, and enabling
the log path with Cloud Monitoring as the only OpenTelemetry destination fails the
install rather than silently dropping them. For logs on GCP, use an [OTLP
destination](/manage/monitor/self-managed/opentelemetry/) or keep them in the
bundled [Loki](/manage/monitor/self-managed/storage/).
{{< /note >}}

## Instructions

### Before you begin

{{% include-headless "/headless/monitoring/before-you-begin" %}}

You also need:

- A deployment on **GCP**, created with the GCP Terraform modules. Workload
  Identity must be enabled on the cluster, which the `gke` module already sets.

- Permission to create a service account and IAM bindings in the project.

### Step 1. Enable observability

{{% include-headless "/headless/monitoring/enable-observability" %}}

### Step 2. Configure the Cloud Monitoring destination

1. On the `monitoring` module block of your Terraform, set:

   ```hcl
   module "monitoring" {
     # ...

     enable_google_cloud_metrics         = true
     google_cloud_metrics_min_importance = "recommended"
   }
   ```

   | Variable | Default | Purpose |
   |----------|---------|---------|
   | `enable_google_cloud_metrics` | `false` | Turns the destination on, and creates the service account and Workload Identity binding it authenticates with. |
   | `google_cloud_metrics_min_importance` | `recommended` | Which metrics to send. See [How to control which metrics Cloud Monitoring receives](#how-to-control-which-metrics-cloud-monitoring-receives). |
   | `google_cloud_metrics_prefix` | `workload.googleapis.com/mzmon` | The metric name prefix in Cloud Monitoring. |

1. Apply the configuration:

   ```bash
   terraform apply
   ```

{{< warning >}}
Authentication is Application Default Credentials only. There is no key-file path.
Without the Workload Identity binding the module creates, the exporter falls back
to the node's service account, which works only if that account happens to hold
`roles/monitoring.metricWriter`, and fails opaquely if it does not.
{{< /warning >}}

### Step 3. Confirm metrics are arriving

{{% include-headless "/headless/monitoring/confirm-metrics" %}}

In Cloud Monitoring, use **Metrics explorer** and filter to the prefix, which is
`workload.googleapis.com/mzmon` unless you changed it.

### Step 4. Build alerts

Build Cloud Monitoring [alerting policies
⧉](https://cloud.google.com/monitoring/alerts) from the metrics and thresholds in
[Alerting](/manage/monitor/self-managed/alerting/).

The monitoring stack also ships Alertmanager rules that evaluate against the
bundled Thanos. Decide which system owns which alerts rather than running both
against the same thresholds.

## How to control which metrics Cloud Monitoring receives

Cloud Monitoring bills per custom metric and per sample, so the tier you pick sets
the bill.

{{% include-headless "/headless/monitoring/metric-tiers" %}}

`google_cloud_metrics_min_importance` defaults to `recommended`, which covers the
metrics the dashboards and alerts use. `all` is a diagnostic setting, not a steady
state.

## Instructions when using Helm

If you install the `materialize-monitoring` chart directly rather than through the
Terraform modules, you create the identity and binding yourself, then point the
chart at it.

1. Create a Google service account, grant it `roles/monitoring.metricWriter` on
   the project, and bind the gateway's ServiceAccount to it with
   `roles/iam.workloadIdentityUser`.

1. Enable the exporter and annotate the gateway ServiceAccount so the binding
   applies:

   ```yaml
   alloy-gateway:
     serviceAccount:
       annotations:
         iam.gke.io/gcp-service-account: <gsa>@<project>.iam.gserviceaccount.com

   pipeline:
     metrics:
       gateway:
         destination:
           otel:
             enabled: true
             googleCloudExporter:
               enabled: true
               minMetricImportance: recommended
   ```

There is no Secret to create, because the exporter authenticates with the ambient
identity. Cloud Monitoring supports `gzip` compression only.

For the full value reference, see [Metrics > Storing
⧉](https://materializeinc.github.io/materialize-monitoring/metrics/storing/).

## See also

- [How logs and metrics are stored](/manage/monitor/self-managed/storage/), for the
  bundled stores and the other backends you can send metrics and logs to.

- [OpenTelemetry](/manage/monitor/self-managed/opentelemetry/), for OTLP
  destinations, which can also carry logs.

- [Alerting](/manage/monitor/self-managed/alerting/), for the metrics and
  thresholds to alert on.
