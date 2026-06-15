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

## Before you begin

Ensure you have one of:

- A self-managed instance of Materialize running with materialize-terraform-self-managed Terraform module with
  `enable_observability=true`.

You also need the following tools:
- [kubectl](https://kubernetes.io/docs/tasks/tools/) installed and configured
  - You will need administrative access to your Kubernetes cluster

{{< tip >}}
Exciting changes are coming in the near future to further enhance the monitoring experience for self-managed users.
Stay tuned for updates!
{{< /tip >}}

## Accessing Grafana in materialize-terraform-self-managed

If you are using materialize-terraform-self-managed, you already have Grafana available.

You can use the following to enable port-forwarding to access the Grafana UI:

```bash
kubectl port-forward service/grafana 80:3000 -n monitoring
```

You should then be able to open the Grafana UI on [http://localhost:3000](http://localhost:3000) in a browser.

{{< warning >}}
The port forwarding method is for testing purposes only.
For production environments, configure an ingress controller to securely expose the Grafana UI.
{{< /warning >}}

## Advanced Monitoring

{{< warning >}}
If you are using materialize-terraform-self-managed, you shouldn't need to do any manual changes to your Grafana or Prometheus configuration.
{{< /warning >}}

### Updating Grafana Dashboards

Additional dashboards are available from the materialize-monitoring repository.
You can download them from the [Grafana Dashboards](https://materializeinc.github.io/materialize-monitoring/dashboards/grafana/) page.

For historical dashboards and Changelogs, you can find them in the [materialize-monitoring releases](https://github.com/MaterializeInc/materialize-monitoring/releases?q=dashboards).

### Updating your Prometheus ScrapeConfigs

Prometheus-Operator resources (ServiceMonitors and PodMonitors) are
available in the materialize-monitoring repository.
You can find them in the [Metrics Scraping](https://materializeinc.github.io/materialize-monitoring/metrics/scraping/) page.

Classic ScrapeConfigs are also available in [Metrics Scraping](https://materializeinc.github.io/materialize-monitoring/metrics/scraping/).
