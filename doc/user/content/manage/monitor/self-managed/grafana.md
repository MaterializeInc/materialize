---
title: "Grafana"
description: "How to enable, reach, and use the Grafana that ships with the Self-Managed Materialize monitoring stack."
aliases:
  - /manage/monitor/self-managed/prometheus/
menu:
  main:
    parent: "monitor-sm"
    weight: 3
    identifier: "grafana-sm"
---

**Grafana** is the dashboarding and query interface for the monitoring stack the
[Materialize Terraform
modules](/self-managed-deployments/installation/#install-using-terraform-modules)
install. It is deployed with the stack, with the Materialize dashboards and their
data sources pre-installed, so there is nothing to import or wire up yourself.

It queries the two stores the stack runs:
[metrics](/manage/monitor/self-managed/metric-store/) and
[logs](/manage/monitor/self-managed/log-store/). Those pages cover what is stored
and how to send it elsewhere. This page covers Grafana itself: turning it on,
reaching it, keeping its state, and finding the dashboards.

If you are upgrading from a previous version of the Terraform modules, read
[Upgrading from the previous stack](#upgrading-from-the-previous-stack) first.

## Before you begin

Ensure you have:

- A Materialize deployment created with the [Materialize Terraform
  modules](/self-managed-deployments/).

- [Terraform ⧉](https://developer.hashicorp.com/terraform/install) installed.

- [kubectl ⧉](https://kubernetes.io/docs/tasks/tools/) installed and configured
  to connect to your cluster.

## Upgrading from the previous stack

Before TF v10.0.0, `enable_observability = true` installed a single Prometheus
and a Grafana from `kubernetes/modules/prometheus` and
`kubernetes/modules/grafana`. Those two modules were **removed** in v10.0.0 —
not deprecated in place — and replaced by a `monitoring` module per cloud.

{{< warning >}}
Upgrading to v10.0.0 or later **destroys** the `prometheus` and `grafana` Helm
releases and their PersistentVolumeClaims. Up to 15 days of local Prometheus
data goes with them: there is no backfill, and the new stack begins collecting
at install. Anything hand-created in the old Grafana — dashboards, users, saved
queries — does not carry over either.
{{< /warning >}}

Other things that change on that upgrade:

- If you referenced `kubernetes/modules/prometheus` or
  `kubernetes/modules/grafana` directly rather than through an example, that
  reference breaks. Pin the previous major until you have migrated to the
  `monitoring` module for your cloud.

- The `prometheus_url` output is gone, replaced by `metrics_url` (Thanos Query)
  and `logs_url` (Loki). Thanos Query is Prometheus-API-compatible, so consumers
  of the old URL work against the new one — only the host and port change.

- `grafana_url` and `grafana_admin_password` keep their names and meaning.

- New cloud resources are created: object storage for each backend (logs and
  metrics), plus a per-backend cloud identity bound to the in-cluster
  ServiceAccount.

- If you set `install_metrics_server = false` on the operator module, set
  `install_metrics_server = true` on the monitoring module in the same change.
  The Materialize Console depends on the metrics API for cluster metrics.

For the per-cloud module blocks and the full upgrade procedure, see the upgrade
guide for your cloud: [AWS](/self-managed-deployments/upgrading/upgrade-on-aws/),
[Azure](/self-managed-deployments/upgrading/upgrade-on-azure/), or
[GCP](/self-managed-deployments/upgrading/upgrade-on-gcp/).

## Step 1. Enable observability

{{< include-md file="content/headless/monitoring/enable-observability.md" >}}

Starting in **v10.1.0**, the examples also create two resources for Grafana
itself whenever `enable_observability` is on:

| Resource | Purpose |
|----------|---------|
| A dedicated PostgreSQL instance | Holds Grafana's own state — users, service accounts and API tokens, annotations, dashboard versions, preferences, and alert-rule state. |
| An L4 load balancer | Reaches Grafana without port forwarding. Internal by default. |

Both are billable, and both are sized as small as the cloud offers
(`db.t4g.micro` on AWS, `db-f1-micro` on GCP, `B_Standard_B1ms` on Azure).
See [Step 2](#step-2-access-grafana) for the load balancer and [Step
3](#step-3-persist-grafanas-own-state) for the database.

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

Retrieve the `admin` password from the Terraform output. You need it for either
access method below:

```bash
terraform output -raw grafana_admin_password
```

{{< tip >}}
Your shell may show an ending marker (such as `%`) because the output did not
end with a newline. Do not include the marker when using the value.
{{< /tip >}}

### Through the load balancer

Starting in v10.1.0, the examples put Grafana behind an L4 load balancer. It
follows the same `internal_load_balancer` and `ingress_cidr_blocks` variables as
the Materialize load balancer, so by default it is **internal** and allowlisted
to the same ranges.

1. Read the address:

   ```bash
   terraform output -raw grafana_url
   ```

   `grafana_url` is the hostname you supplied, else the load balancer's own
   address, else the in-cluster Service. `grafana_load_balancer_address` gives
   you just the load balancer.

   {{< note >}}
   On GCP and Azure the cloud assigns the address asynchronously, so a fresh
   apply can still report the in-cluster name. The next plan picks it up. Set
   `ip` on `grafana_load_balancer` to pre-allocate the address and have it known
   at plan time.
   {{< /note >}}

1. Open the address in a browser and log in as `admin`.

{{< warning >}}
The load balancer terminates no TLS, and Grafana has no identity provider until
you configure one — so the generated admin password is the whole of the access
control, sent over plain HTTP. Keep the load balancer internal until both are
addressed.

Every datasource behind Grafana reads every metric in Thanos and every log in
the tenant. A public load balancer whose allowlist is still `0.0.0.0/0` is
**refused at plan time** for Grafana specifically.
{{< /warning >}}

{{< note >}}
Do not set `security.cookie_secure` while Grafana is served over plain HTTP. It
marks the session cookie `Secure`, the browser then stops sending it over the
connection that works, and login breaks entirely.
{{< /note >}}

To make Grafana's own share links, alert notification links, and OAuth redirect
URIs correct, set `grafana_host` to a hostname you control. Nothing in the
modules publishes DNS for that name — that record is yours to create.

To skip the load balancer entirely and keep Grafana on a `ClusterIP` Service,
set `grafana_load_balancer = null` on the `monitoring` module block.

### Through port forwarding

Port forwarding stays the private path, and is the only option when the load
balancer is internal and you are outside the network.

1. Forward a local port to the Grafana service:

   ```bash
   kubectl -n monitoring port-forward svc/grafana 3000:80
   ```

1. Open [http://localhost:3000](http://localhost:3000) in a browser and log in
   as `admin` with the password from above.

## Step 3. Persist Grafana's own state

Grafana keeps users, service accounts and API tokens, annotations, dashboard
versions, preferences, and alert-rule state in its own database — separate from
the metrics in Thanos and the logs in Loki.

The chart default is SQLite on an `emptyDir`, so all of it is lost on every
restart, upgrade, and reschedule. Starting in v10.1.0 the examples provision a
dedicated PostgreSQL instance for it instead, whenever `enable_observability` is
on. Confirm it:

```bash
terraform output -raw grafana_database_endpoint
```

{{< warning >}}
Grafana has no SQLite-to-PostgreSQL migration. Switching to the database does
**not** carry existing state over — export anything you care about through
Grafana's HTTP API first.
{{< /warning >}}

To keep the previous SQLite behaviour, set `grafana_database = null` on the
`monitoring` module block. To point at a database you already run, leave
`grafana_database = null` and set the `grafana_database_host`,
`grafana_database_port`, `grafana_database_name`, `grafana_database_user`,
`grafana_database_password`, and `grafana_database_ssl_mode` variables instead.

## Step 4. Open the Materialize dashboards

The dashboards and their data sources are installed by Grafana Operator from the
released chart, so they track the chart version rather than a copy you maintain.

To confirm that they were installed:

```bash
kubectl -n monitoring get grafanamanifest,grafanadatasource
```

{{< note >}}
Helm returns once the operator's Deployment is ready. Pushing the dashboards
into Grafana happens afterwards and can fail on its own, so check these
resources rather than the Helm release status.
{{< /note >}}

![Image of Grafana](/images/self-managed/grafana-monitoring-success.png)

For the list of dashboards and what each one covers, see [Grafana dashboards
⧉](https://materializeinc.github.io/materialize-monitoring/dashboards/grafana/importing/).

## Connect existing tooling

If you already run Grafana, or another tool that should read the collected data,
the examples publish the query endpoints for both stores as Terraform outputs. See
[Metric storage](/manage/monitor/self-managed/metric-store/#connect-existing-tooling)
and [Log
storage](/manage/monitor/self-managed/log-store/#connect-existing-tooling).

To have the stack push its metrics or logs to a platform you already run rather
than being queried, see the destinations listed under [Metric
storage](/manage/monitor/self-managed/metric-store/#other-metric-storage-backends).

## Advanced configuration

The monitoring modules expose additional options, including sizing profiles,
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

The stack includes Alertmanager for recording and routing alerts.
For guidance on the initial set of metrics and suggested thresholds,
see [Alerting](/manage/monitor/self-managed/alerting/).
