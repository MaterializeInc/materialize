---
title: "Grafana"
description: "How to deploy Grafana and the Materialize monitoring stack using the Materialize Terraform modules."
aliases:
  - /manage/monitor/self-managed/prometheus/
menu:
  main:
    parent: "monitor-sm"
    weight: 1
    identifier: "grafana-sm"
---

The [Materialize Terraform modules](/self-managed-deployments/) can deploy a
monitoring stack alongside your Materialize deployment. Enabling it installs:

| Component | Purpose |
|-----------|---------|
| **Grafana** | Dashboards and query UI, with the Materialize dashboards pre-installed. |
| **Thanos** | Metrics storage backed by object storage, with a Prometheus-compatible query endpoint. |
| **Loki** | Log storage backed by object storage. |
| **Grafana Alloy** | Collection of metrics and logs from Materialize and from the cluster. |
| **Alertmanager** | Alert routing. |

The stack comes from the [`materialize-monitoring`
⧉](https://github.com/MaterializeInc/materialize-monitoring) charts. The
Terraform modules also create the object storage and the cloud identities the
stack needs, so you do not have to configure scrape targets, data sources, or
dashboards yourself.

## Before you begin

Ensure you have:

- A Materialize deployment created with the [Materialize Terraform
  modules](/self-managed-deployments/).

- [Terraform ⧉](https://developer.hashicorp.com/terraform/install) installed.

- [kubectl ⧉](https://kubernetes.io/docs/tasks/tools/) installed and configured
  to connect to your cluster.

## Step 1. Enable observability

The `simple` and `enterprise` examples for each cloud take an
`enable_observability` variable, which defaults to `false` in `simple` and
`true` in `enterprise`.

1. In your `terraform.tfvars`, set:

   ```hcl
   enable_observability = true
   ```

1. Apply the configuration:

   ```bash
   terraform apply
   ```

   The apply creates the object storage and cloud identities for metrics and
   logs, and installs the stack into the `monitoring` namespace.

{{< note >}}
The monitoring stack runs several components: Loki, Thanos, Grafana,
Alertmanager, kube-state-metrics, and two Alloy roles. Your generic node pool
may need to grow before the first apply can schedule all of them.
{{< /note >}}

If you instantiate the modules in your own Terraform rather than using an
example, add the `monitoring` module for your cloud (see the [Terraform
installation guide
⧉](https://materializeinc.github.io/materialize-monitoring/getting-started/terraform/)),
and turn on the operator's scrape annotations so its pods are collected:

```hcl
module "operator" {
  # ...
  helm_values = {
    observability = {
      enabled = true
      prometheus = {
        scrapeAnnotations = {
          enabled = true
        }
      }
    }
  }
}
```

## Step 2. Access Grafana

Grafana is deployed as a `ClusterIP` service in the `monitoring` namespace, so
reaching it means port forwarding.

1. Retrieve the `admin` password from the Terraform output:

   ```bash
   terraform output -raw grafana_admin_password
   ```

   {{< tip >}}
   Your shell may show an ending marker (such as `%`) because the output did not
   end with a newline. Do not include the marker when using the value.
   {{< /tip >}}

1. Forward a local port to the Grafana service:

   ```bash
   kubectl -n monitoring port-forward svc/grafana 3000:80
   ```

   {{< warning >}}
   Port forwarding is for testing purposes only. For production environments,
   expose Grafana through your own ingress and configure authentication for it.
   {{< /warning >}}

1. Open [http://localhost:3000](http://localhost:3000) in a browser and log in
   as `admin` with the password from the first step.

## Step 3. Open the Materialize dashboards

The dashboards and their data sources are installed by grafana-operator from the
released chart, so they track the chart version rather than a copy you maintain.

To confirm they landed:

```bash
kubectl -n monitoring get grafanamanifest,grafanadatasource
```

{{< note >}}
Helm returns once the operator's Deployment is ready. Pushing the dashboards
into Grafana happens afterwards and can fail on its own, so check these
resources rather than the Helm release status.
{{< /note >}}

![Image of Grafana](/images/self-managed/grafana-monitoring-success.png)

For what each dashboard covers, see the [dashboard documentation
⧉](https://materializeinc.github.io/materialize-monitoring/dashboards/).

## Connect existing tooling

If you already run Grafana, or want to point other tools at the collected data,
the examples output the query endpoints:

| Output | Endpoint |
|--------|----------|
| `metrics_url` | Thanos Query. Prometheus-API-compatible, so anything that speaks to a Prometheus server works against it. |
| `logs_url` | Loki read endpoint. |

```bash
terraform output -raw metrics_url
```

## Advanced configuration

The monitoring modules expose further options, including sizing profiles,
retention, node placement, and raw Helm value overrides. For these, and for
installing the stack without the Materialize Terraform modules, see:

- [Terraform installation guide
  ⧉](https://materializeinc.github.io/materialize-monitoring/getting-started/terraform/),
  for the full set of module variables.

- [Helm installation guide
  ⧉](https://materializeinc.github.io/materialize-monitoring/getting-started/helm/),
  for installing the stack with Helm rather than Terraform.

- [Production best practices
  ⧉](https://materializeinc.github.io/materialize-monitoring/operating/production-best-practices/),
  for the throughput envelope each sizing profile assumes and what to scale when
  metric/logging queries feel slow.

## Alerting

The stack includes Alertmanager. For the metrics and thresholds to start from,
see [Alerting](/manage/monitor/self-managed/alerting/).
