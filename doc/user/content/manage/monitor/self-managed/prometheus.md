---
title: "Grafana using Prometheus"
description: "How to monitor the performance and overall health of your Self-Managed Materialize environment using Prometheus and Grafana."
menu:
  main:
    parent: "monitor-sm"
    weight: 1
    identifier: "grafana-prometheus-sm"
---

Self-Managed Materialize exposes a native Prometheus endpoint out of the box at
`/metrics`. This guide walks you through the steps to scrape these metrics
with Prometheus and visualize them in Grafana.

In this guide, you will:
1. Verify that the Prometheus metrics endpoint is working.
2. Configure Prometheus to scrape your Materialize environment.
3. Set up Grafana to visualize the scraped metrics.
4. Import pre-built Grafana dashboards for Materialize.

For a full list of available metrics, see the
[Metrics reference](/manage/monitor/metrics/).

## When to use Prometheus vs. `mz_catalog`

Materialize provides two complementary observability surfaces:

| | Prometheus metrics | `mz_catalog` / `mz_internal` |
|---|---|---|
| **Best for** | Monitoring health, alerting, and historical trends | Investigating issues and taking corrective action |
| **Access** | Scraped via HTTP (`/metrics`) | Queried via SQL |
| **Stability** | Stable, versioned API with deprecation policy | `mz_catalog` is stable; `mz_internal` is unstable |
| **Example use** | "Alert when CPU utilization exceeds 85%" | "Which dataflow is using the most memory?" |

**Our recommendation:** Use Prometheus metrics to detect *whether* there is a
problem and to build dashboards and alerts. Use `mz_catalog` to diagnose *what*
the problem is and to take action.

## Guide

### Before you begin

Ensure you have:

- A running [Self-Managed Materialize](/self-managed/) environment
- [kubectl](https://kubernetes.io/docs/tasks/tools/) installed and configured
- [Helm](https://helm.sh/docs/intro/install/) version 3.2.0+ installed

{{< important >}}
This guide assumes you have administrative access to your Kubernetes cluster and
the necessary permissions to install Prometheus.
{{< /important >}}

### Step 1. Verify the Prometheus endpoint

By default, each Materialize service exposes Prometheus metrics on port `9363`.
Verify that the endpoint is working:

```bash
curl http://<your-materialize-host>:9363/metrics
```

You should see Prometheus-formatted metrics output with lines like:

```
# HELP mz_environment_up Whether the environment is up (1) or down (0)
# TYPE mz_environment_up gauge
mz_environment_up 1
```

**TODO: Confirm port number, path, and whether each service (environmentd, clusterd) has its own endpoint**

{{< note >}}
All stable metrics use the `mz_` prefix and follow
[Prometheus naming conventions](https://prometheus.io/docs/practices/naming/).
For a full list of available metrics, see the
[Metrics reference](/manage/monitor/metrics/).
{{< /note >}}

### Step 2. Configure Prometheus to scrape Materialize

Follow the [Prometheus getting started guide](https://prometheus.io/docs/prometheus/latest/getting_started/)
to set up Prometheus if you don't already have it running.

The recommended approach is to use **Kubernetes service discovery** so
Prometheus can automatically discover and scrape all Materialize pods.

Edit your Prometheus configuration:

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'materialize'
    metrics_path: '/metrics'
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names:
            - materialize

    relabel_configs:
      # Only scrape pods with the Materialize app label
      - source_labels: [__meta_kubernetes_pod_label_app]
        regex: materialize
        action: keep

    # Optional:
    scrape_timeout: 10s
```

**TODO: Confirm the exact pod label selectors and whether separate jobs are needed for environmentd vs clusterd**

### Step 3. Verify that Prometheus is scraping Materialize

1. Open the Prometheus UI.
2. Navigate to **Status > Targets**.
3. Verify that the `materialize` job appears and shows targets in the **UP** state.

You can also run a quick query in the Prometheus expression browser to confirm
metrics are flowing:

```promql
mz_environment_up
```

### Step 4. Set up Grafana

1. Install the Grafana Helm chart following
   [this guide](https://grafana.com/docs/grafana/latest/setup-grafana/installation/helm/).

2. Set up port forwarding to access the Grafana UI:
   ```bash
   MZ_POD_GRAFANA=$(kubectl get pods -n monitoring -l app.kubernetes.io/name=grafana -o custom-columns="NAME:.metadata.name" --no-headers)
   kubectl port-forward pod/$MZ_POD_GRAFANA 3000:3000 -n monitoring
   ```

   {{< warning >}}
   The port forwarding method is for testing purposes only. For production
   environments, configure an ingress controller to securely expose the
   Grafana UI.
   {{< /warning >}}

3. Open the Grafana UI at [http://localhost:3000](http://localhost:3000).

4. Add a Prometheus data source. In the Grafana UI, under
   **Connection > Data sources**:
   - Click **Add data source** and select **Prometheus**.
   - In the Connection section, set **Prometheus server URL** to
     `http://<prometheus-server-name>.<namespace>.svc.cluster.local:<port>`
     (e.g., `http://prometheus-server.prometheus.svc.cluster.local:80`).

   ![Grafana Prometheus data source setup](/images/self-managed/grafana-prometheus-datasource-setup.png)

### Step 5. Import Grafana dashboards

Materialize provides pre-built Grafana dashboard templates to get you started:

**TODO: Update dashboard download URLs to the correct branch/release once finalized**

To import the dashboards, follow the
[Grafana import dashboard guide](https://grafana.com/docs/grafana/latest/dashboards/build-dashboards/import-dashboards/#importing-a-dashboard).

![Grafana monitoring dashboard](/images/self-managed/grafana-monitoring-success.png)

## Metrics deprecation policy

Materialize follows a deprecation policy for Prometheus metrics:

1. A metric scheduled for removal will have `deprecated` added to its `HELP`
   text.
2. Deprecated metrics are removed only with **major version upgrades**

This gives you time to update your dashboards and alert rules before a metric
is removed.

## Related pages

- [Metrics reference](/manage/monitor/metrics/)
- [Alerting](/manage/monitor/self-managed/alerting/)
- [Datadog using Prometheus SQL Exporter](/manage/monitor/self-managed/datadog/)
- [Prometheus naming conventions](https://prometheus.io/docs/practices/naming/)
