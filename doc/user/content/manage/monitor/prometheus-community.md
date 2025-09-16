---
title: "Grafana using Prometheus"
description: "How to monitor the performance and overall health of your Materialize instance using Prometheus and Grafana."
menu:
  main:
    parent: "monitor"
    weight: 1
---

This guide walks you through the steps required to monitor the performance and
overall health of your Materialize instance using Prometheus and Grafana.

## Before you begin

Ensure you have:

- A self-managed instance of Materialize installed with helm values `observability.enabled=true`, `observability.podMetrics.enabled=true`, and `prometheus.scrapeAnnotations.enabled=true`
- [Helm](https://helm.sh/docs/intro/install/) version 3.2.0+ installed
- [kubectl](https://kubernetes.io/docs/tasks/tools/) installed and configured

{{< important >}}
This guide assumes you have administrative access to your Kubernetes cluster and the necessary permissions to install Prometheus.
{{< /important >}}

## 1. Download our Prometheus scrape configurations (`prometheus.yml`)
  ```bash
  curl -O https://raw.githubusercontent.com/MaterializeInc/materialize/refs/heads/self-managed-docs/v25.2/doc/user/data/monitoring/prometheus.yml
  ```


## 2. Install Prometheus to your Kubernetes cluster using [`prometheus-community/prometheus`](https://github.com/prometheus-community/helm-charts/tree/main/charts/prometheus) (Optional)

1. Download the prometheus-community default chart values (`values.yaml`):
   ```bash
   curl -O https://raw.githubusercontent.com/prometheus-community/helm-charts/refs/heads/main/charts/prometheus/values.yaml
   ```

2. Within `values.yaml`, replace `serverFiles > prometheus.yml > scrape_configs` with our scrape configurations (`prometheus.yml`).

3. Install the operator with the updated `values.yaml`
   ```bash
   kubectl create namespace prometheus
   helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
   helm repo update
   helm install --namespace prometheus prometheus prometheus-community/prometheus \
   --values values.yaml
   ```

## 3. Visualize through Grafana (optional)

1. Install the Grafana helm chart following [this guide](https://grafana.com/docs/grafana/latest/setup-grafana/installation/helm/)


2.  Set up port forwarding to access the Grafana UI:
    ```bash
    MZ_POD_GRAFANA=$(kubectl get pods -n monitoring -l app.kubernetes.io/name=grafana -o custom-columns="NAME:.metadata.name" --no-headers)
    kubectl port-forward pod/$MZ_POD_GRAFANA 3000:3000 -n monitoring
    ```

    {{< note >}}
    The port forwarding method is for testing purposes only. For production environments, configure an ingress controller to securely expose the Grafana UI.
    {{< /note >}}

3. Within the UI, add a Prometheus data source where the URL is `http://<prometheus server name>.<namespace>.svc.cluster.local:<port>`(i.e. `http://prometheus-server.prometheus.svc.cluster.local:80`)

    ![Image of Materialize Console login screen with mz_system user](/images/grafana-prometheus-datasource-setup.png)

4. Download the following dashboards:
    ```bash
    # Environment overview: An overview of the state of different objects in your environment
    curl -O https://raw.githubusercontent.com/MaterializeInc/materialize/refs/heads/self-managed-docs/v25.2/doc/user/data/monitoring/grafana_dashboards/environment_overview_dashboard.json
    ```

     ```bash
     # Freshness overview: An overview of how out of date objects in your environment are
    curl -O https://raw.githubusercontent.com/MaterializeInc/materialize/refs/heads/self-managed-docs/v25.2/doc/user/data/monitoring/grafana_dashboards/freshness_overview_dashboard.json
    ```

5. [Import the dashboards using the Prometheus data source](https://grafana.com/docs/grafana/latest/dashboards/build-dashboards/import-dashboards/#importing-a-dashboard)

    ![Image of Materialize Console login screen with mz_system user](/images/grafana-monitoring-success.png)
