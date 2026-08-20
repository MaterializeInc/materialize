---
title: "Prometheus remote write"
description: "How to send metrics from Self-Managed Materialize to an external Prometheus remote-write store such as Mimir, Amazon Managed Prometheus, or Grafana Cloud."
menu:
  main:
    parent: "monitor-sm"
    weight: 9
    identifier: "remote-write-sm"
---

This guide walks you through the steps required to store the metrics from your
Materialize region in an external Prometheus remote-write store, such as Grafana
Mimir, Amazon Managed Prometheus, Grafana Cloud, or a Thanos you run elsewhere.

{{< warning >}}
Unlike other destinations, remote write is a **replacement**,
not an addition. It is the single sink the bundled Thanos already occupies, so
pointing it at an external store means Thanos stops receiving metrics, and the
bundled Grafana dashboards go empty unless you also repoint their data source.

If you want an external copy *and* the bundled store, use an [OTLP
destination](/manage/monitor/self-managed/opentelemetry/) instead, which runs
alongside Thanos. That only works if your platform exposes an OTLP ingest
endpoint. A remote-write-only backend cannot be reached additively, so with one of
those the choice really is external store or bundled store, not both.
{{< /warning >}}

## How it works

The stack collects metrics and logs before any destination is involved. For the
collection pipeline and where that data is stored by default, see [How logs and
metrics are stored](/manage/monitor/self-managed/storage/#how-it-works).

The gateway writes metrics with the Prometheus remote-write protocol, and the
bundled Thanos is simply the default endpoint for that write. Repointing it is a
change of address rather than a new code path, which is why it needs no separate
exporter and why there can only be one of them.

Because the external store replaces Thanos, consider turning the bundled one off
in the same change rather than paying to run a store nothing writes to.

## Instructions

### Before you begin

{{% include-headless "/headless/monitoring/before-you-begin" %}}

You also need:

- Your store's remote-write endpoint, as a full URL including the scheme and path,
  such as `https://<host>/api/v1/write`. Note that this differs from the OTLP
  destinations, which take a bare `host[:port]`.

- The credential it expects: basic auth, a bearer token, OAuth2 client
  credentials, or AWS SigV4 signing.

- A decision about the bundled Grafana. Its data source points at Thanos, so if
  you retire Thanos you either repoint that data source at the external store or
  use the external platform's own query interface.

### Step 1. Enable observability

{{% include-headless "/headless/monitoring/enable-observability" %}}

### Step 2. Repoint the remote-write destination

Remote write is not modelled as a Terraform input, because unlike the additive
destinations it changes where the stack's own storage lives. Set it through
`additional_values` on the `monitoring` module block, which is appended last and so
wins over anything the modules compute.

1. In the `monitoring` module block of your Terraform, add:

   ```hcl
   module "monitoring" {
     # ...

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
   }
   ```

   `authType` is one of `none`, `basicAuth`, `bearer`, `oauth2`, or `sigv4`.

1. Set the `cluster` label so series from different deployments stay distinct once
   they land in a shared store. Every sample carries it, and it defaults to
   `default`:

   ```hcl
   additional_values = [
     <<-EOT
       env:
         CLUSTER_NAME: prod-us-east-1
     EOT
   ]
   ```

1. Supply the credential through the gateway Secret rather than inline in the
   values. The remote-write block accepts a credential inline, but anything set
   there is baked into the gateway's ConfigMap in plaintext:

   ```bash
   kubectl create secret generic mzmon-alloy-gateway-env \
     --namespace monitoring \
     --from-literal=GATEWAY_PROMETHEUS_DEST_USERNAME='<user>' \
     --from-literal=GATEWAY_PROMETHEUS_DEST_PASSWORD='<password>'
   ```

   | Auth type | Secret keys |
   |-----------|-------------|
   | `basicAuth` | `GATEWAY_PROMETHEUS_DEST_USERNAME`, `GATEWAY_PROMETHEUS_DEST_PASSWORD` |
   | `bearer` | `GATEWAY_PROMETHEUS_DEST_BEARER_TOKEN` |
   | `oauth2` | `GATEWAY_PROMETHEUS_DEST_OAUTH2_CLIENT_ID`, `..._CLIENT_SECRET`, `..._TOKEN_URL` |
   | client TLS | `GATEWAY_PROMETHEUS_DEST_TLS_CA`, `..._TLS_CERT`, `..._TLS_KEY` |
   | `sigv4` | none. It signs with the gateway pod's IRSA identity |

   {{< warning >}}
   The Secret name must match the release, so with the default
   `fullnameOverride: mzmon` it is `mzmon-alloy-gateway-env`, in the namespace the
   gateway runs in. Because the mount is optional, a wrong name or namespace is
   ignored silently and the destination then authenticates with empty credentials
   rather than failing loudly.
   {{< /warning >}}

1. Apply the configuration:

   ```bash
   terraform apply
   ```

### Step 3. Amazon Managed Prometheus

Amazon Managed Prometheus is the one remote-write store that needs no credential
in the cluster at all. `sigv4` signs requests with the gateway pod's IRSA
identity.

1. Create an IAM role with remote-write permission on the workspace, and a trust
   policy scoped to the gateway's namespace and ServiceAccount.

1. Point remote write at the workspace and annotate the gateway ServiceAccount so
   IRSA applies:

   ```hcl
   additional_values = [
     <<-EOT
       pipeline:
         metrics:
           gateway:
             destination:
               prometheusRemoteWrite:
                 authType: sigv4
                 url: https://aps-workspaces.<region>.amazonaws.com/workspaces/<workspace-id>/api/v1/remote_write

       alloy-gateway:
         serviceAccount:
           annotations:
             eks.amazonaws.com/role-arn: arn:aws:iam::<account-id>:role/<amp-role>
     EOT
   ]
   ```

A ready-made starting point lives at [`aws-amp-example.values.yaml`
⧉](https://github.com/MaterializeInc/materialize-monitoring/blob/main/charts/materialize-monitoring/profiles/aws-amp-example.values.yaml).
Note that it also sets `thanos.enabled: false`, which is the next step.

### Step 4. Retire the bundled metric store

Once the external store is receiving metrics, the bundled Thanos is a component
nothing writes to. Turning it off frees its compute and stops new writes to its
object storage:

```hcl
additional_values = [
  <<-EOT
    thanos:
      enabled: false
  EOT
]
```

{{< warning >}}
Do this only after confirming the external store is receiving metrics. Disabling
Thanos does not delete its bucket, so historical blocks survive, but nothing serves
queries against them while it is off. Repoint the bundled Grafana's data source at
the external store in the same change, or its dashboards will show no data.
{{< /warning >}}

### Step 5. Confirm metrics are arriving

{{% include-headless "/headless/monitoring/confirm-metrics" %}}

### Step 6. Configure alerts

The Alertmanager rules the stack ships evaluate against the bundled Thanos, so
retiring it moves alerting to the external platform. Rebuild the alerts there from
the metrics and thresholds in [Alerting](/manage/monitor/self-managed/alerting/).

## How to control which metrics the store receives

{{% include-headless "/headless/monitoring/metric-tiers" %}}

The remote-write destination defaults to `all`, on the assumption that it is
backed by cheap storage. If you repoint it at a metered platform, lower the floor
in the same change:

```yaml
pipeline:
  metrics:
    gateway:
      destination:
        prometheusRemoteWrite:
          minMetricImportance: recommended
```

## Instructions when using Helm

The values above are chart values, so a Helm install uses them directly rather
than through `additional_values`. The gateway Secret and its keys are identical.

For the full value reference, including the per-`authType` blocks and the client
TLS settings, see [Metrics > Storing
⧉](https://materializeinc.github.io/materialize-monitoring/metrics/storing/).

## See also

- [How logs and metrics are stored](/manage/monitor/self-managed/storage/), for the
  bundled stores and the additive backends that keep them in place.

- [OpenTelemetry](/manage/monitor/self-managed/opentelemetry/), the additive
  alternative where your platform accepts OTLP.

- [Alerting](/manage/monitor/self-managed/alerting/), for the metrics and
  thresholds to alert on.
