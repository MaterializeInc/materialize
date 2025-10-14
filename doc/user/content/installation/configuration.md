---
title: "Materialize Operator Configuration"
description: "Configuration reference for the Materialize Operator Helm chart"
aliases:
  - /self-hosted/configuration/
menu:
  main:
    parent: "installation"
    weight: 70
---

## Configure the Materialize operator

To configure the Materialize operator, you can:

- Use a configuration YAML file (e.g., `values.yaml`) that specifies the
  configuration values and then install the chart with the `-f` flag:

  ```shell
  # Assumes you have added the Materialize operator Helm chart repository
  helm install my-materialize-operator materialize/materialize-operator-weekly \
     -f /path/to/your/config/values.yaml
  ```

- Specify each parameter using the `--set key=value[,key=value]` argument to
  `helm install`. For example:

  ```shell
  # Assumes you have added the Materialize operator Helm chart repository
  helm install my-materialize-operator materialize/materialize-operator-weekly \
    --set observability.podMetrics.enabled=true
  ```

{{%  self-managed/materialize-operator-chart-parameters-table %}}

## Parameters

{{%  self-managed/materialize-operator-chart-parameters %}}

## See also

- [Installation](/installation/)
- [Troubleshooting](/installation/troubleshooting/)
