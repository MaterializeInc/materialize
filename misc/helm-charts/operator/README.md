# Materialize Kubernetes Operator Helm Chart

![Version: 0.1.0](https://img.shields.io/badge/Version-0.1.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.1.0](https://img.shields.io/badge/AppVersion-0.1.0-informational?style=flat-square)

Materialize Kubernetes Operator Helm Chart

This Helm chart deploys the Materialize operator on a Kubernetes cluster. The operator manages Materialize environments within your Kubernetes infrastructure.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.2.0+

## Installing the Chart

To install the chart with the release name `my-materialize-operator`:

```shell
helm install my-materialize-operator materialize/misc/helm-charts/operator
```

This command deploys the Materialize operator on the Kubernetes cluster with default configuration. The [Parameters](#parameters) section lists the parameters that can be configured during installation.

## Uninstalling the Chart

To uninstall/delete the `my-materialize-operator` deployment:

```shell
helm delete my-materialize-operator
```

This command removes all the Kubernetes components associated with the chart and deletes the release.

## Parameters

The following table lists the configurable parameters of the Materialize operator chart and their default values.

| Parameter | Description | Default |
|-----------|-------------|---------|
| `clusterd.nodeSelector` |  | ``{}`` |
| `environmentd.nodeSelector` |  | ``{}`` |
| `namespace.create` |  | ``false`` |
| `namespace.name` |  | ``"materialize"`` |
| `networkPolicies.enabled` | Whether to install any network policies. | ``true`` |
| `networkPolicies.internal.enabled` | Whether to install network policies allowing communication between Materialize pods. | ``true`` |
| `networkPolicies.ingress.enabled` | Whether to install network policies allowing communication from external locations to environmentd or balancerd SQL or HTTP ports. | ``true`` |
| `networkPolicies.ingress.cidrs` | List of CIDR blocks to allow to reach environmentd or balancerd. | ``["0.0.0.0/0"]`` |
| `networkPolicies.egress.enabled` | Whether to install network policies allowing communication from Materialize pods to external sources and sinks. | ``true`` |
| `networkPolicies.egress.cidrs` | List of CIDR blocks to allow Materialize pods to reach for sources and sinks. | ``["0.0.0.0/0"]`` |
| `observability.enabled` |  | ``false`` |
| `observability.prometheus.enabled` |  | ``false`` |
| `operator.args.cloudProvider` |  | ``"local"`` |
| `operator.args.localDevelopment` |  | ``true`` |
| `operator.args.region` |  | ``"kind"`` |
| `operator.args.startupLogFilter` |  | ``"INFO,mz_orchestratord=TRACE"`` |
| `operator.image.pullPolicy` |  | ``"IfNotPresent"`` |
| `operator.image.repository` |  | ``"materialize/orchestratord"`` |
| `operator.image.tag` |  | ``"v0.122.0-dev.0--pr.g8bb641fc00c77f98ba5556dcdca43670776eacfa"`` |
| `operator.nodeSelector` |  | ``{}`` |
| `operator.resources.limits.cpu` |  | ``"500m"`` |
| `operator.resources.limits.memory` |  | ``"512Mi"`` |
| `operator.resources.requests.cpu` |  | ``"100m"`` |
| `operator.resources.requests.memory` |  | ``"128Mi"`` |
| `rbac.create` |  | ``true`` |
| `serviceAccount.create` |  | ``true`` |
| `serviceAccount.name` |  | ``"orchestratord"`` |

Specify each parameter using the `--set key=value[,key=value]` argument to `helm install`. For example:

```shell
helm install my-materialize-operator \
  --set operator.image.tag=v1.0.0 \
  materialize/materialize-operator
```

Alternatively, a YAML file that specifies the values for the parameters can be provided while installing the chart. For example:

```shell
helm install my-materialize-operator -f values.yaml materialize/materialize-operator
```

## Configuration and Installation Details

### RBAC Configuration

The chart creates a `ClusterRole` and `ClusterRoleBinding` by default. To use an existing `ClusterRole`, set `rbac.create=false` and specify the name of the existing `ClusterRole` using the `rbac.clusterRole` parameter.

### Observability

To enable observability features, set `observability.enabled=true`. This will create the necessary resources for monitoring the operator. If you want to use Prometheus, also set `observability.prometheus.enabled=true`.

### Network Policies

Network policies can be enabled by setting `networkPolicies.enabled=true`.

To enable policies for internal communication between Materialize pods, set `networkPolicies.internal.enabled=true`.

To enable policies for ingress HTTP or SQL communication to Materialize environmentd or balancerd pods, set `networkPolicies.ingress.enabled=true`.

Ingress policies can configure the list of CIDR blocks to allow with `networkPolicies.ingress.cidrs`.

To enable policies for egress communication from Materialize pods to sources and sinks, set `networkPolicies.egress.enabled=true`.

Egress policies can configure the list of CIDR blocks to allow with `networkPolicies.ingress.cidrs`.

## Troubleshooting

If you encounter issues with the Materialize operator, check the operator logs:

```shell
kubectl logs -l app.kubernetes.io/name=materialize-operator
```

For more detailed information on using and troubleshooting the Materialize operator, refer to the [Materialize documentation](https://materialize.com/docs).

## Learn More

- [Materialize Documentation](https://materialize.com/docs)
- [Materialize GitHub Repository](https://github.com/MaterializeInc/materialize)

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.14.2](https://github.com/norwoodj/helm-docs/releases/v1.14.2)
