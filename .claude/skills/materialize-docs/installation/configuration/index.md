---
audience: developer
canonical_url: https://materialize.com/docs/installation/configuration/
complexity: intermediate
description: Configuration reference for the Materialize Operator Helm chart
doc_type: reference
keywords:
- Materialize Operator Configuration
product_area: Deployment
status: stable
title: Materialize Operator Configuration
---

# Materialize Operator Configuration

## Purpose
Configuration reference for the Materialize Operator Helm chart

If you need to understand the syntax and options for this command, you're in the right place.


Configuration reference for the Materialize Operator Helm chart


## Configure the Materialize operator

To configure the Materialize operator, you can:

- Use a configuration YAML file (e.g., `values.yaml`) that specifies the
  configuration values and then install the chart with the `-f` flag:

  ```shell
  # Assumes you have added the Materialize operator Helm chart repository
  helm install my-materialize-operator materialize/materialize-operator \
     -f /path/to/your/config/values.yaml
  ```text

- Specify each parameter using the `--set key=value[,key=value]` argument to
  `helm install`. For example:

  ```shell
  # Assumes you have added the Materialize operator Helm chart repository
  helm install my-materialize-operator materialize/materialize-operator  \
    --set observability.podMetrics.enabled=true
  ```

<!-- Unresolved shortcode: {{%  self-managed/materialize-operator-chart-param... -->

## Parameters

<!-- Unresolved shortcode: {{%  self-managed/materialize-operator-chart-param... -->

## See also

- [Installation](/installation/)
- [Troubleshooting](/installation/troubleshooting/)