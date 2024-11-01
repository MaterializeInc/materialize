# Materialize Kubernetes Environmentd Helm Chart

![Version: 0.1.0](https://img.shields.io/badge/Version-0.1.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.1.0](https://img.shields.io/badge/AppVersion-0.1.0-informational?style=flat-square)

Materialize Kubernetes Environmentd Helm Chart

This Helm chart deploys a single Materialize environment on a Kubernetes cluster. It requires the Materialize operator to be installed first.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.2.0+
- Materialize operator installed in the cluster

## Installing the Chart

Make sure that the namespace configured in `values.yaml` in `namespace.name` (default: `materialize-environment`) exists or that `namespace.create` is set to `true`.

To install the chart for a production environment:

```shell
helm install prod-env materialize/misc/helm-charts/environmentd \
  --set environment.name=12345678-1234-1234-1234-123456789013
```

For a staging environment:

```shell
helm install staging-env materialize/misc/helm-charts/environmentd \
  --set environment.name=12345678-1234-1234-1234-123456789012
```

Each installation creates a separate environment with its own configuration. The [Parameters](#parameters) section lists the parameters that can be configured during installation.

## Uninstalling the Chart

To uninstall/delete an environment:

```shell
helm delete prod-env
```

This command removes all the Kubernetes components associated with the chart and deletes the release.

## Parameters

The following table lists the configurable parameters of the Materialize environmentd chart and their default values.

| Parameter | Description | Default |
|-----------|-------------|---------|
| `environment.balancerdResourceRequirements.limits.memory` |  | ``"256Mi"`` |
| `environment.balancerdResourceRequirements.requests.cpu` |  | ``"100m"`` |
| `environment.balancerdResourceRequirements.requests.memory` |  | ``"256Mi"`` |
| `environment.environmentdExtraArgs[0]` |  | ``"--orchestrator-kubernetes-ephemeral-volume-class=hostpath"`` |
| `environment.environmentdImageRef` |  | ``"materialize/environmentd:v0.122.0-dev.0--pr.g47923ddb1bb4f3fb38d152b8aa86a77514599b29"`` |
| `environment.environmentdResourceRequirements.limits.memory` |  | ``"512Mi"`` |
| `environment.environmentdResourceRequirements.requests.cpu` |  | ``"250m"`` |
| `environment.environmentdResourceRequirements.requests.memory` |  | ``"512Mi"`` |
| `environment.forceRollout` |  | ``"33333333-3333-3333-3333-333333333333"`` |
| `environment.inPlaceRollout` |  | ``false`` |
| `environment.name` |  | ``"12345678-1234-1234-1234-123456789012"`` |
| `environment.requestRollout` |  | ``"22222222-2222-2222-2222-222222222222"`` |
| `environment.secret.metadataBackendUrl` |  | ``"postgres://materialize_user:materialize_pass@postgres.materialize.svc.cluster.local:5432/materialize_db?sslmode=disable"`` |
| `environment.secret.persistBackendUrl` |  | ``"s3://minio:minio123@bucket/12345678-1234-1234-1234-123456789012?endpoint=http%3A%2F%2Fminio.materialize.svc.cluster.local%3A9000&region=minio"`` |
| `namespace.create` |  | ``false`` |
| `namespace.name` |  | ``"materialize-environment"`` |

Specify each parameter using the `--set key=value[,key=value]` argument to `helm install`. For example:

```shell
helm install prod-env materialize/materialize-environmentd \
  --set environment.name=prod \
  --set environment.environmentdImageRef=materialize/environmentd:v1.0.0
```

Alternatively, a YAML file that specifies the values for the parameters can be provided while installing the chart. For example:

```shell
helm install prod-env -f prod-values.yaml materialize/materialize-environmentd
```

## Configuration and Installation Details

### Namespace Configuration

By default, the chart creates a namespace called `materialize-environment`. You can customize this using the `namespace.name` parameter:

```shell
helm install prod-env . --set namespace.name=custom-namespace
```

### Resource Allocation

You can specify CPU and memory allocation for the environment using the `environment.environmentdCpuAllocation` and `environment.environmentdMemoryAllocation` parameters:

```shell
helm install prod-env . \
  --set environment.environmentdCpuAllocation=2 \
  --set environment.environmentdMemoryAllocation=2Gi
```

### Rollout Configuration

Control environment updates using the rollout parameters:
- `environment.requestRollout`: Request a rolling update
- `environment.forceRollout`: Force an immediate update
- `environment.inPlaceRollout`: Perform an in-place update

### Secret Configuration

The chart automatically creates a secret named `materialize-backend-{environment.name}` containing:
- `metadata_backend_url`: Database connection URL
- `persist_backend_url`: Storage backend URL

## Managing Multiple Environments

To manage multiple environments, create separate installations of the chart:

```shell
# Create production environment
helm install prod-env . \
  --set environment.name=prod \
  --set environment.environmentdCpuAllocation=4

# Create staging environment
helm install staging-env . \
  --set environment.name=staging \
  --set environment.environmentdCpuAllocation=2

# Create development environment
helm install dev-env . \
  --set environment.name=dev \
  --set environment.environmentdCpuAllocation=1
```

This approach allows independent lifecycle management for each environment.

## Troubleshooting

To check the status of an environment:

```shell
# Get environment status
kubectl get materialize -n materialize-environment

# Check environment logs
kubectl logs -l app.kubernetes.io/name=materialize-environmentd -n materialize-environment
```

For more detailed information on using and troubleshooting Materialize environments, refer to the [Materialize documentation](https://materialize.com/docs).

## Learn More

- [Materialize Documentation](https://materialize.com/docs)
- [Materialize GitHub Repository](https://github.com/MaterializeInc/materialize)

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.14.2](https://github.com/norwoodj/helm-docs/releases/v1.14.2)
