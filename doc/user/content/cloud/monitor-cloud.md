---
title: "Monitor Cloud"
description: "Monitor Materialize Cloud with the same tools you use for the rest of your pipeline."
menu:
  main:
    parent: "cloud"
    weight:
---

{{< cloud-notice >}}

We want to make it possible for you to monitor Materialize Cloud with the same tools you use for the rest of your pipeline. Currently Materialize Cloud only supports Prometheus, but we are working on a Datadog integration. If there are any other tools you're interested in, please [let us know](https://materialize.com/s/chat).

We also provide some internal tools that can be helpful for monitoring or debugging:

- The [system catalog SQL interface](#system-catalog-sql-interface)
- [Deployment metrics](../cloud-deployments/#metrics)

## Connect to Materialize Cloud

No matter which tool you use, you first need to download and unzip the TLS certificates in a location where they can be accessed by your monitoring tool, and then connect to Materialize Cloud.

{{% cloud-connection-details %}}

## Prometheus
### Set up Prometheus

Next, you need to add Materialize to the configuration file for your Prometheus installation.

Add the following configuration to `prometheus.yml`:

```
scrape_configs:
  - job_name: materialized
  - tls: config:
    - ca_file: ca.crt
    - cert_file: materialize.crt
    - key_file: materialize.key
  static_configs:
    -  targets:
      - <deployment_hostname>:6875
```
You can find the hostname listed next to the deployment name on your Deployments page. You can also copy the complete configuration update already customized for your deployment by clicking on the deployment, going to the Prometheus tab, and clicking the copy icon in the code box.

### View Prometheus metrics

{{% monitoring/prometheus-details %}}

{{% monitoring/grafana %}}

{{% monitoring/system-catalog-sql-interface %}}
