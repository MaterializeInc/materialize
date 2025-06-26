---
title: "Prometheus"
description: "How to monitor the performance and overall health of your Materialize instance using Prometheus."
menu:
  main:
    parent: "monitor"
---

This guide walks you through the steps required to monitor the performance and
overall health of your Materialize instance using [Prometheus](https://prometheus.io/).

## Before you begin

Ensure you have: 

- A self-managed instance of Materialize installed with helm value `observability.podMetrics.enabled=true`
- [Helm](https://helm.sh/docs/intro/install/) version 3.2.0+ installed
- [kubectl](https://kubernetes.io/docs/tasks/tools/) installed and configured

{{< important >}}
This guide assumes you have administrative access to your Kubernetes cluster and the necessary permissions to install Prometheus.
{{< /important >}}

### Step 1. Install Prometheus

1. Download the starter Prometheus configuration file:

   ```bash
   curl -O https://raw.githubusercontent.com/MaterializeInc/materialize/refs/heads/self-managed-docs/v25.1/doc/user/data/self_managed/prometheus.yml
   ```

2. Install Prometheus using Helm:

   ```bash
   helm install --namespace kube-system -f prometheus.yaml prometheus-community/prometheus --generate-name
   ```

   {{< tip >}}
   The configuration file contains recommended settings for monitoring Materialize. You can customize these settings based on your requirements.
   {{< /tip >}}

## Step 2. Start Prometheus UI

1. Set up port forwarding to access the Prometheus UI:

   ```bash
   MZ_POD_PROMETHEUS=$(kubectl get pods -n kube-system -l app.kubernetes.io/name=prometheus -o custom-columns="NAME:.metadata.name" --no-headers)
   kubectl port-forward pod/$MZ_POD_PROMETHEUS 9090:9090 -n kube-system
   ```

2. Access the Prometheus UI by navigating to `localhost:9090` in your web browser.

<!-- TODO(cloud#10992): Need to document the following steps:
Step 3. Create alerts
Step 4. Visualize with Grafana
-->
