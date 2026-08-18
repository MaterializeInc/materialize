---
title: "Datadog"
description: "How to export metrics from Self-Managed Materialize to Datadog."
menu:
  main:
    parent: "monitor-sm"
    weight: 5
    identifier: "datadog-sm"
---

The [monitoring stack](/manage/monitor/self-managed/grafana/) collects metrics
from Materialize and from the cluster into a Grafana Alloy gateway, and that
gateway can export them to [Datadog ⧉](https://www.datadoghq.com/) in addition
to storing them in the bundled Thanos. Datadog receives a copy of the metrics;
Thanos, Grafana, and Alertmanager keep working as before.

Nothing runs alongside Materialize for this. There is no SQL exporter to
operate, no Datadog Agent to install next to Materialize, and no scrape config
to maintain: the gateway is already collecting these metrics, and Datadog becomes
one more place it writes them.

{{< note >}}
This page covers Self-Managed Materialize. For Materialize Cloud, where the
monitoring stack is not part of the deployment, see [Datadog for
Cloud](/manage/monitor/cloud/datadog/).
{{< /note >}}

## Before you begin

Ensure you have:

- The monitoring stack installed, with `enable_observability = true`. See
  [Grafana](/manage/monitor/self-managed/grafana/). Datadog export requires **TF
  v12.0.0** or later.

- A [Datadog API key
  ⧉](https://docs.datadoghq.com/account_management/api-app-keys/). An
  application key is not needed and is not accepted: the metrics intake
  authenticates with the API key alone.

- Your [Datadog site ⧉](https://docs.datadoghq.com/getting_started/site/), such
  as `datadoghq.com`, `datadoghq.eu`, or `us3.datadoghq.com`. A wrong site is a
  403 from the intake rather than a routing error, so confirm it before you
  apply.

## Step 1. Configure the Datadog destination

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
   | `min_importance` | `essential` | Which metrics to send. See [Controlling what Datadog receives](#controlling-what-datadog-receives). |
   | `metric_endpoint` | derived from `site` | Override the metrics intake URL. Only for a proxy or PrivateLink. |
   | `logs_endpoint` | derived from `site` | Override the logs intake URL. Only for a proxy or PrivateLink. |

   {{< warning >}}
   A hand-written `metric_endpoint` or `logs_endpoint` that disagrees with `site`
   fails at the intake, not at plan time. Leave both unset unless you are
   routing through a proxy.
   {{< /warning >}}

1. Supply the API key. Declare it as a sensitive variable and pass it in the way
   you pass other secrets, for example through an environment variable:

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

The API key does not travel through the Helm values. The module puts it in a
Kubernetes Secret that the gateway mounts, so it is not recoverable with `helm
get values`. Rotating the key rolls the gateway, because environment variables
are fixed at container start and a running pod would otherwise keep
authenticating with the key it started with.

## Step 2. Confirm metrics are arriving

1. Check that the gateway restarted and is healthy:

   ```bash
   kubectl -n monitoring rollout status deployment/alloy-gateway
   ```

1. In Datadog, open **Metrics > Summary** and search for `mz_`.

{{< note >}}
Datadog's metric summary is cumulative: a metric appearing there is not proof it
is arriving right now. Query for recent samples to confirm what is currently
flowing.
{{< /note >}}

## Controlling what Datadog receives

Datadog bills per custom metric, so the volume you send is a cost decision.
Every metric the stack collects carries an *importance* tier, and each
destination keeps only the metrics at or above a chosen floor. The tiers below
run from most to least important, and the floor is cumulative: it keeps that
tier and every tier above it.

| Tier | What it covers |
|------|----------------|
| `essential` | The metrics that are critical and that you would always want available. These are the ones used in alerting. |
| `recommended` | The metrics used in dashboards, and generally desirable for troubleshooting. |
| `extended` | The metrics used by optional and experimental dashboards. |
| `diagnostic` | The metrics used for in-depth troubleshooting and analysis. |
| `all` | Absolutely everything scraped, including metrics no tier classifies. Suited to cheap storage such as the bundled Thanos, not to a metered backend. |

`datadog_metrics.min_importance` defaults to `essential`, a tighter floor than
the other destinations use, for exactly this reason. `all` is a diagnostic
setting, not a steady state.

The tiers are shared with the rest of the stack, so a tier selected here means
the same set of metrics as the same tier selected in Helm. For the membership of
each tier, see [List of metrics
⧉](https://materializeinc.github.io/materialize-monitoring/reference/stable-metrics/list-metrics/).
For the metrics Materialize recommends dashboarding and alerting on, see
[essential metrics](/manage/monitor/essential-metrics/), and for everything it
exposes, the [appendix of all metrics](/manage/monitor/appendix-metrics/).

{{< note >}}
The `extended` and `diagnostic` tiers are still being populated, so today they
resolve to the same set as `recommended`. To send everything that is scraped,
use `all`, not `diagnostic`.
{{< /note >}}

{{< warning >}}
The filter fails open. If the allowlist reaches the gateway empty, the gateway
sends everything to that destination rather than nothing. That is safe for
visibility and expensive on a metered backend, so check your Datadog metric
volume after a configuration change.
{{< /warning >}}

## Forwarding logs as well

The same exporter can also carry the logs the stack collects, alongside the
metrics. Loki continues to receive them either way. Enable it through
`additional_values` on the `monitoring` module block:

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

This switch is not Datadog-specific: it turns on the log path to every
logs-capable exporter the gateway has enabled, so if you also configure an [OTLP
destination](/manage/monitor/self-managed/opentelemetry/), that one receives the
logs too.

Logs are considerably higher volume than metrics, and Datadog bills for them
separately from custom metrics. Turn this on deliberately.

## Building monitors and dashboards

With metrics in Datadog, build monitors from the thresholds in
[Alerting](/manage/monitor/self-managed/alerting/). Materialize also ships
Alertmanager rules with the monitoring stack, so decide which system owns which
alerts rather than running both against the same thresholds.

## Installing with Helm

If you install the `materialize-monitoring` chart directly rather than through
the Terraform modules, the Datadog destination is a chart value and the API key
is a Secret you create. See [Metrics > Storing
⧉](https://materializeinc.github.io/materialize-monitoring/metrics/storing/) for
the values, the Secret's name and keys, and the environment variable the API key
becomes.

## Other destinations

- [OpenTelemetry and remote
  write](/manage/monitor/self-managed/opentelemetry/), for OTLP endpoints,
  Prometheus remote-write stores, and Google Cloud Monitoring.

- [Grafana](/manage/monitor/self-managed/grafana/), for the bundled stack and
  the query endpoints that existing tooling can read.
