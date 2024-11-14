---
title: "Materialize Operator Configuration"
description: ""
---

You can configure the Materialize operator chart. For example:

- **RBAC**

  The chart creates a `ClusterRole` and `ClusterRoleBinding` by default. To use
  an existing `ClusterRole`, set [`rbac.create=false`](/self-hosted/configuration/#rbaccreate) and specify the name of
  the existing `ClusterRole` using the
  [`rbac.clusterRole`](/self-hosted/configuration/#rbacclusterrole) parameter.

- **Network Policies**

  Network policies can be enabled by setting
  [`networkPolicies.enabled=true`](/self-hosted/configuration/#networkpoliciesenabled).
  By default, the chart uses native Kubernetes network policies. To use Cilium
  network policies instead, set
  `networkPolicies.useNativeKubernetesPolicy=false`.

- **Observability**

  To enable observability features, set
  [`observability.enabled=true`](/self-hosted/configuration/#observabilityenabled).
  This will create the necessary resources for monitoring the operator. If you
  want to use Prometheus, also set
  [`observability.prometheus.enabled=true`](/self-hosted/configuration/#observabilityprometheusenabled).


## Configure the Materialize operator chart

To configure the Materialize operator chart, you can:

- *Recommended:* Create a YAML file (e.g., `values.yaml`) that specifies the
  values for the parameters and then [install the chart](/self-hosted/#install-the-chart) with the `-f` flag:

  ```shell
  helm install my-materialize-operator -f values.yaml materialize/materialize-operator
  ```

- Specify each parameter using the `--set key=value[,key=value]` argument to
  [`helm install`]((/self-hosted/#install-the-chart). For example:

  ```shell
  helm install my-materialize-operator \
    --set operator.image.tag=v1.0.0 \
    materialize/materialize-operator
  ```

{{%  self-hosted/materialize-operator-chart-parameters-table %}}

## Parameters

{{%  self-hosted/materialize-operator-chart-parameters %}}

## See also

- [Materialize Kubernetes Operator Helm Chart](/self-hosted/)
- [Troubleshooting](/self-hosted/troubleshooting/)
