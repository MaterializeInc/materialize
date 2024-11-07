# Materialize Kubernetes Operator Helm Chart

![Version: 0.1.0](https://img.shields.io/badge/Version-0.1.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.1.0](https://img.shields.io/badge/AppVersion-0.1.0-informational?style=flat-square)

Materialize Kubernetes Operator Helm Chart

This Helm chart deploys the Materialize operator on a Kubernetes cluster. The operator manages Materialize environments within your Kubernetes infrastructure.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.2.0+

### Kubernetes Storage Configuration

Materialize requires fast storage for optimal performance. Network-attached storage (like EBS volumes) can significantly impact performance, so we strongly recommend using locally-attached NVMe drives.

We recommend using OpenEBS with LVM Local PV for managing local volumes. While other storage solutions may work, we have tested and recommend OpenEBS for optimal performance.

#### Installing OpenEBS

```bash
# Install OpenEBS operator
helm repo add openebs https://openebs.github.io/openebs
helm repo update

# Install only the Local PV Storage Engines
helm install openebs --namespace openebs openebs/openebs \
  --set engines.replicated.mayastor.enabled=false \
  --create-namespace
```

Verify the installation:
```bash
kubectl get pods -n openebs -l role=openebs-lvm
```

#### LVM Configuration

LVM setup varies by environment. Below is our tested and recommended configuration:

##### AWS EC2 with Bottlerocket AMI
Tested configurations:
- Instance types: m6g, m7g families
- AMI: AWS Bottlerocket
- Instance store volumes required

Setup process:
1. Use Bottlerocket bootstrap container for LVM configuration
2. Configure volume group name as `instance-store-vg`

**Note:** While LVM setup may work on other instance types with local storage (like i3.xlarge, i4i.xlarge, r5d.xlarge), we have not extensively tested these configurations.

#### Storage Configuration

Once LVM is configured, set up the storage class:

```yaml
storage:
  storageClass:
    create: true
    name: "openebs-lvm-instance-store-ext4"
    provisioner: "local.csi.openebs.io"
    parameters:
      storage: "lvm"
      fsType: "ext4"
      volgroup: "instance-store-vg"
```

While OpenEBS is our recommended solution, you can use any storage provisioner that meets your performance requirements by overriding the provisioner and parameters values.

For example, to use a different storage provider:

```yaml
storage:
  storageClass:
    create: true
    name: "your-storage-class"
    provisioner: "your.storage.provisioner"
    parameters:
      # Parameters specific to your chosen storage provisioner
```

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
| `networkPolicies.egress.cidrs[0]` |  | ``"0.0.0.0/0"`` |
| `networkPolicies.egress.enabled` |  | ``true`` |
| `networkPolicies.enabled` |  | ``true`` |
| `networkPolicies.ingress.cidrs[0]` |  | ``"0.0.0.0/0"`` |
| `networkPolicies.ingress.enabled` |  | ``true`` |
| `networkPolicies.internal.enabled` |  | ``true`` |
| `observability.enabled` |  | ``false`` |
| `observability.prometheus.enabled` |  | ``false`` |
| `operator.args.awsAccountID` |  | ``""`` |
| `operator.args.cloudProvider` |  | ``"local"`` |
| `operator.args.createBalancers` |  | ``true`` |
| `operator.args.environmentdConnectionRoleARN` |  | ``""`` |
| `operator.args.environmentdIAMRoleARN` |  | ``""`` |
| `operator.args.localDevelopment` |  | ``true`` |
| `operator.args.region` |  | ``"kind"`` |
| `operator.args.startupLogFilter` |  | ``"INFO,mz_orchestratord=TRACE"`` |
| `operator.clusters.defaultSizes.analytics` |  | ``"3xsmall"`` |
| `operator.clusters.defaultSizes.catalogServer` |  | ``"2xsmall"`` |
| `operator.clusters.defaultSizes.default` |  | ``"3xsmall"`` |
| `operator.clusters.defaultSizes.probe` |  | ``"probe"`` |
| `operator.clusters.defaultSizes.support` |  | ``"3xsmall"`` |
| `operator.clusters.defaultSizes.system` |  | ``"3xsmall"`` |
| `operator.clusters.sizes.2xsmall.credits_per_hour` |  | ``"0.5"`` |
| `operator.clusters.sizes.2xsmall.scale` |  | ``1`` |
| `operator.clusters.sizes.2xsmall.workers` |  | ``1`` |
| `operator.clusters.sizes.3xsmall.credits_per_hour` |  | ``"0.25"`` |
| `operator.clusters.sizes.3xsmall.scale` |  | ``1`` |
| `operator.clusters.sizes.3xsmall.workers` |  | ``1`` |
| `operator.clusters.sizes.probe.credits_per_hour` |  | ``"0"`` |
| `operator.clusters.sizes.probe.scale` |  | ``1`` |
| `operator.clusters.sizes.probe.workers` |  | ``1`` |
| `operator.image.pullPolicy` |  | ``"IfNotPresent"`` |
| `operator.image.repository` |  | ``"materialize/orchestratord"`` |
| `operator.image.tag` |  | ``"v0.124.0-dev.0--pr.gd5e227d7d07dc4878d2a065c7c10a48c2555385a"`` |
| `operator.nodeSelector` |  | ``{}`` |
| `operator.resources.limits.memory` |  | ``"512Mi"`` |
| `operator.resources.requests.cpu` |  | ``"100m"`` |
| `operator.resources.requests.memory` |  | ``"512Mi"`` |
| `rbac.create` |  | ``true`` |
| `serviceAccount.create` |  | ``true`` |
| `serviceAccount.name` |  | ``"orchestratord"`` |
| `storage.storageClass.allowVolumeExpansion` |  | ``false`` |
| `storage.storageClass.create` |  | ``false`` |
| `storage.storageClass.name` |  | ``""`` |
| `storage.storageClass.parameters.fsType` |  | ``"ext4"`` |
| `storage.storageClass.parameters.storage` |  | ``"lvm"`` |
| `storage.storageClass.parameters.volgroup` |  | ``"instance-store-vg"`` |
| `storage.storageClass.provisioner` |  | ``""`` |
| `storage.storageClass.reclaimPolicy` |  | ``"Delete"`` |
| `storage.storageClass.volumeBindingMode` |  | ``"WaitForFirstConsumer"`` |

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

## Deploying Materialize Environments

To deploy a Materialize environment, create a `Materialize` custom resource definition with the desired configuration.

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: materialize-environment
---
apiVersion: v1
kind: Secret
metadata:
  name: materialize-backend
  namespace: materialize-environment
stringData:
  metadata_backend_url: "postgres://materialize_user:materialize_pass@postgres.materialize.svc.cluster.local:5432/materialize_db?sslmode=disable"
  persist_backend_url: "s3://minio:minio123@bucket/12345678-1234-1234-1234-123456789012?endpoint=http%3A%2F%2Fminio.materialize.svc.cluster.local%3A9000&region=minio"
---
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:v0.124.0-dev.0--pr.gd5e227d7d07dc4878d2a065c7c10a48c2555385a
  environmentdResourceRequirements:
    limits:
      memory: 16Gi
    requests:
      cpu: 2
      memory: 16Gi
  balancerdResourceRequirements:
    limits:
      memory: 256Mi
    requests:
      cpu: 100m
      memory: 256Mi
  requestRollout: 22222222-2222-2222-2222-222222222222
  forceRollout: 33333333-3333-3333-3333-333333333333
  inPlaceRollout: false
  backendSecretName: materialize-backend
```

## Configuration and Installation Details

### RBAC Configuration

The chart creates a `ClusterRole` and `ClusterRoleBinding` by default. To use an existing `ClusterRole`, set `rbac.create=false` and specify the name of the existing `ClusterRole` using the `rbac.clusterRole` parameter.

### Observability

To enable observability features, set `observability.enabled=true`. This will create the necessary resources for monitoring the operator. If you want to use Prometheus, also set `observability.prometheus.enabled=true`.

### Network Policies

Network policies can be enabled by setting `networkPolicies.enabled=true`. By default, the chart uses native Kubernetes network policies. To use Cilium network policies instead, set `networkPolicies.useNativeKubernetesPolicy=false`.

## Troubleshooting

If you encounter issues with the Materialize operator, check the operator logs:

```shell
kubectl logs -l app.kubernetes.io/name=materialize-operator -n materialize
```

For more detailed information on using and troubleshooting the Materialize operator, refer to the [Materialize documentation](https://materialize.com/docs).

## Learn More

- [Materialize Documentation](https://materialize.com/docs)
- [Materialize GitHub Repository](https://github.com/MaterializeInc/materialize)

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.14.2](https://github.com/norwoodj/helm-docs/releases/v1.14.2)
