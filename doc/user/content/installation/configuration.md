---
title: "Materialize Operator Configuration"
description: ""
aliases:
  - /self-hosted/configuration/
menu:
  main:
    parent: "installation"

---

You can configure the Materialize operator chart. For example:

- **RBAC**

  The chart creates a `ClusterRole` and `ClusterRoleBinding` by default.

- **Network Policies**

  Network policies can be enabled by setting
  [`networkPolicies.enabled=true`](#networkpoliciesenabled).
  By default, the chart uses native Kubernetes network policies. For additional network policy configuration options, see [`networkPolicies` parameters](#networkpolicies-parameters).

- **Observability**

  To enable observability features, set
  [`observability.enabled=true`](#observabilityenabled).
  This will create the necessary resources for monitoring the operator. For
  additional observability configuraiton options, see [`observability`
  parameters](#observability-parameters).

## Configure the Materialize operator chart

To configure the Materialize operator chart, you can:

- *Recommended:* Modify the provided  `values.yaml` file (or create your own
  YAML file) that specifies the configuration values and then install the
  chart with the `-f` flag:

  ```shell
  helm install my-materialize-operator -f /path/to/values.yaml /path/to/materialize/helm-charts/operator
  ```

- Specify each parameter using the `--set key=value[,key=value]` argument to
  `helm install`. For example:

  ```shell
  helm install my-materialize-operator \
    --set operator.image.tag=v0.130.1 \
    /path/to/materialize/helm-charts/operator
  ```

{{%  self-managed/materialize-operator-chart-parameters-table %}}

## Parameters

{{%  self-managed/materialize-operator-chart-parameters %}}

## See also

- [Troubleshooting](/installation/troubleshooting/)
- [Installation](/installation/)
- [Operational guidelines](/installation/operational-guidelines/)
- [Upgrading](/installation/upgrading/)
