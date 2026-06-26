---
title: "Grafana using Prometheus"
description: "How to monitor the performance and overall health of your Materialize instance using Prometheus and Grafana."
menu:
  main:
    parent: "monitor-sm"
    weight: 1
    identifier: "grafana-prometheus-sm"
---

{{< warning >}}
The metrics scraped are unstable and may change across releases.
{{< /warning >}}

This guide walks you through the steps required to monitor the performance and
overall health of your Materialize instance using Prometheus and Grafana.

{{< important >}}
Every monitoring setup is unique, so be sure to adapt the steps in this guide to your specific environment and requirements.
{{< /important >}}

## Before you begin

Ensure you have:

- A self-managed instance of Materialize installed with [helm values](/self-managed-deployments/operator-configuration/) `observability.enabled=true`, `observability.podMetrics.enabled=true`, and `prometheus.scrapeAnnotations.enabled=true`
- [Helm](https://helm.sh/docs/intro/install/) version 3.2.0+ installed
- [kubectl](https://kubernetes.io/docs/tasks/tools/) installed and configured
- [metrics-server](https://github.com/kubernetes-sigs/metrics-server) installed in your cluster
- [kube-state-metrics](https://github.com/kubernetes/kube-state-metrics) installed in your cluster

{{< important >}}
This guide assumes you have administrative access to your Kubernetes cluster and the necessary permissions to install Prometheus.
{{< /important >}}

## 1. Download our Prometheus scrape configurations (`prometheus.yml`)
  Download the Prometheus scrape configurations that we'll use to configure Prometheus to collect metrics from Materialize:
  {{% self-managed/step-download-prometheus-scrape-configs %}}

{{< note >}}
If you are using prometheus-operator or do not know your prometheus distribution, visit [materialize-monitoring/Scraping](https://materializeinc.github.io/materialize-monitoring/metrics/scraping/) for additional scraping configurations.
{{< /note >}}

## 2. Install Prometheus to your Kubernetes cluster

{{< note >}}
This guide uses the [prometheus-community](https://github.com/prometheus-community/helm-charts) Helm chart to install Prometheus.

Many more production-ready Prometheus Helm charts are available
which have different ways to configure Prometheus and its scrape configurations.
Be sure to consult the documentation of the Prometheus Helm chart you are using to ensure that you are configuring Prometheus correctly.
{{< /note >}}

1. Download the prometheus-community default chart values (`values.yaml`):
   ```bash
   curl -O https://raw.githubusercontent.com/prometheus-community/helm-charts/refs/heads/main/charts/prometheus/values.yaml
   ```

2. Within `values.yaml`, replace `serverFiles > prometheus.yml > scrape_configs` with our scrape configurations (`prometheus_scrape_configs.yml`).

3. Install the helm chart with the updated `values.yaml`:
   ```bash
   kubectl create namespace prometheus
   helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
   helm repo update
   helm install --namespace prometheus prometheus prometheus-community/prometheus \
   --values values.yaml
   ```

## 3. Optional. Visualize through Grafana

1. Install the Grafana helm chart following [this guide](https://grafana.com/docs/grafana/latest/setup-grafana/installation/helm/).


2.  Set up port forwarding to access the Grafana UI:
    ```bash
    kubectl port-forward svc/grafana 3000:80 -n monitoring
    ```

    {{< warning >}}
  The port forwarding method is for testing purposes only. For production environments, configure an ingress controller to securely expose the Grafana UI.
    {{< /warning >}}

3. Open the Grafana UI on [http://localhost:3000](http://localhost:3000) in a browser.

4. Add a Prometheus data source. In the Grafana UI, under **Connection > Data sources**,
   - Click **Add data source** and select **prometheus**
   - In the Connection section, set **Prometheus server URL** to `http://<prometheus server name>.<namespace>.svc:<port>`(e.g. `http://prometheus-server.prometheus.svc:80`).

    ![Image of Grafana datasource setup](/images/self-managed/grafana-prometheus-datasource-setup.png)

5. Download dashboards from our [Grafana dashboards repository](https://materializeinc.github.io/materialize-monitoring/dashboards/grafana/)

6. [Import the dashboards using the Prometheus data source](https://grafana.com/docs/grafana/latest/dashboards/build-dashboards/import-dashboards/#importing-a-dashboard)

    ![Image of Grafana](/images/self-managed/grafana-monitoring-success.png)
