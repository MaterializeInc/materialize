---
title: "Prometheus Community Helm Chart"
description: "How to monitor the performance and overall health of your Materialize instance using the prometheus-community helm chart."
menu:
  main:
    parent: "monitor"
    weight: 1
---

This guide walks you through the steps required to monitor the performance and
overall health of your Materialize instance using the [`prometheus-community/prometheus`](https://github.com/prometheus-community/helm-charts/tree/main/charts/prometheus) helm chart.

## Before you begin

Ensure you have:

- A self-managed instance of Materialize installed with helm values `observability.enabled=true`, `observability.podMetrics.enabled=true`, and `prometheus.scrapeAnnotations.enabled=true`
- [Helm](https://helm.sh/docs/intro/install/) version 3.2.0+ installed
- [kubectl](https://kubernetes.io/docs/tasks/tools/) installed and configured

{{< important >}}
This guide assumes you have administrative access to your Kubernetes cluster and the necessary permissions to install Prometheus.
{{< /important >}}

## 1. Install Prometheus to your Kubernetes cluster using [`prometheus-community/prometheus`](https://github.com/prometheus-community/helm-charts/tree/main/charts/prometheus)

1. Download the Materialize Prometheus scrape configuration file:
   ```bash
   curl -O https://raw.githubusercontent.com/MaterializeInc/materialize/refs/heads/self-managed-docs/v25.2/doc/user/data/self_managed/prometheus.yml
   ```

2. Download the prometheus-community default chart values:
   ```bash
   curl -O https://raw.githubusercontent.com/prometheus-community/helm-charts/refs/heads/main/charts/prometheus/values.yaml
   ```

3. Replace `values.yaml`'s `serverFiles > prometheus.yml > scrape_configs` with `prometheus.yml`'s `scrape_configs`. It should look something like:
   ```yml
   serverFiles:
      prometheus.yml:
         scrape_configs:
            - job_name: kubernetes-pods
            ...
   ```
3. Create a `prometheus` namespace
   ```bash
   kubectl create namespace prometheus
   ```
4. Install the operator with the scrape configuration
   ```bash
   helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
   helm repo update
   helm install --namespace prometheus prometheus prometheus-community/prometheus \
   --values values.yaml
   ```


## 2. Validate through the Prometheus UI

{{< note >}}
The port forwarding method described below is for testing purposes only. For production environments, configure an ingress controller to securely expose the Prometheus UI.
{{< /note >}}

1. Set up port forwarding to access the Prometheus UI:

   ```bash
   MZ_POD_PROMETHEUS=$(kubectl get pods -n prometheus -l app.kubernetes.io/name=prometheus -o custom-columns="NAME:.metadata.name" --no-headers)
   kubectl port-forward pod/$MZ_POD_PROMETHEUS 9090:9090 -n prometheus
   ```

2. Access the Prometheus UI by navigating to `localhost:9090` in your web
   browser.
