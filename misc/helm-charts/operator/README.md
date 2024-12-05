# Materialize Kubernetes Operator Helm Chart

![Version: v25.1.0-beta.1](https://img.shields.io/badge/Version-v25.1.0--beta.1-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: v0.126.5](https://img.shields.io/badge/AppVersion-v0.126.5-informational?style=flat-square)

Materialize Kubernetes Operator Helm Chart

This Helm chart deploys the Materialize operator on a Kubernetes cluster. The operator manages Materialize environments within your Kubernetes infrastructure.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.2.0+

### Kubernetes Storage Configuration

Materialize requires fast, locally-attached NVMe storage for optimal performance. Network-attached storage (like EBS volumes) is not supported.

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
- Instance types: r6g, r7g families
- AMI: AWS Bottlerocket
- Instance store volumes required

Setup process:
1. Use Bottlerocket bootstrap container for LVM configuration
2. Configure volume group name as `instance-store-vg`

**Note:** While LVM setup may work on other instance types with local storage (like i3.xlarge, i4i.xlarge, r5d.xlarge), we have not extensively tested these configurations.

#### Storage Configuration

Once LVM is configured, set up the storage class (for example in misc/helm-charts/operator/values.yaml):

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
kubectl create namespace materialize
helm install my-materialize-operator misc/helm-charts/operator
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
| `networkPolicies.egress.enabled` |  | ``false`` |
| `networkPolicies.enabled` |  | ``false`` |
| `networkPolicies.ingress.cidrs[0]` |  | ``"0.0.0.0/0"`` |
| `networkPolicies.ingress.enabled` |  | ``false`` |
| `networkPolicies.internal.enabled` |  | ``false`` |
| `observability.enabled` |  | ``false`` |
| `observability.podMetrics.enabled` |  | ``false`` |
| `observability.prometheus.enabled` |  | ``false`` |
| `operator.args.startupLogFilter` |  | ``"INFO,mz_orchestratord=TRACE"`` |
| `operator.cloudProvider.providers.aws.accountID` |  | ``""`` |
| `operator.cloudProvider.providers.aws.enabled` |  | ``false`` |
| `operator.cloudProvider.providers.aws.iam.roles.connection` |  | ``""`` |
| `operator.cloudProvider.providers.aws.iam.roles.environment` |  | ``""`` |
| `operator.cloudProvider.providers.gcp.enabled` |  | ``false`` |
| `operator.cloudProvider.region` |  | ``"kind"`` |
| `operator.cloudProvider.type` |  | ``"local"`` |
| `operator.clusters.defaultSizes.analytics` |  | ``"25cc"`` |
| `operator.clusters.defaultSizes.catalogServer` |  | ``"50cc"`` |
| `operator.clusters.defaultSizes.default` |  | ``"25cc"`` |
| `operator.clusters.defaultSizes.probe` |  | ``"mz_probe"`` |
| `operator.clusters.defaultSizes.support` |  | ``"25cc"`` |
| `operator.clusters.defaultSizes.system` |  | ``"25cc"`` |
| `operator.clusters.sizes.100cc.cpu_exclusive` |  | ``true`` |
| `operator.clusters.sizes.100cc.cpu_limit` |  | ``2`` |
| `operator.clusters.sizes.100cc.credits_per_hour` |  | ``"1"`` |
| `operator.clusters.sizes.100cc.disk_limit` |  | ``"31050MiB"`` |
| `operator.clusters.sizes.100cc.memory_limit` |  | ``"15525MiB"`` |
| `operator.clusters.sizes.100cc.scale` |  | ``1`` |
| `operator.clusters.sizes.100cc.workers` |  | ``2`` |
| `operator.clusters.sizes.1200cc.cpu_exclusive` |  | ``true`` |
| `operator.clusters.sizes.1200cc.cpu_limit` |  | ``24`` |
| `operator.clusters.sizes.1200cc.credits_per_hour` |  | ``"12"`` |
| `operator.clusters.sizes.1200cc.disk_limit` |  | ``"372603MiB"`` |
| `operator.clusters.sizes.1200cc.memory_limit` |  | ``"186301MiB"`` |
| `operator.clusters.sizes.1200cc.scale` |  | ``1`` |
| `operator.clusters.sizes.1200cc.workers` |  | ``24`` |
| `operator.clusters.sizes.128C.cpu_exclusive` |  | ``true`` |
| `operator.clusters.sizes.128C.cpu_limit` |  | ``62`` |
| `operator.clusters.sizes.128C.credits_per_hour` |  | ``"128"`` |
| `operator.clusters.sizes.128C.disk_limit` |  | ``"962560MiB"`` |
| `operator.clusters.sizes.128C.memory_limit` |  | ``"481280MiB"`` |
| `operator.clusters.sizes.128C.scale` |  | ``4`` |
| `operator.clusters.sizes.128C.workers` |  | ``62`` |
| `operator.clusters.sizes.1600cc.cpu_exclusive` |  | ``true`` |
| `operator.clusters.sizes.1600cc.cpu_limit` |  | ``31`` |
| `operator.clusters.sizes.1600cc.credits_per_hour` |  | ``"16"`` |
| `operator.clusters.sizes.1600cc.disk_limit` |  | ``"481280MiB"`` |
| `operator.clusters.sizes.1600cc.memory_limit` |  | ``"240640MiB"`` |
| `operator.clusters.sizes.1600cc.scale` |  | ``1`` |
| `operator.clusters.sizes.1600cc.workers` |  | ``31`` |
| `operator.clusters.sizes.200cc.cpu_exclusive` |  | ``true`` |
| `operator.clusters.sizes.200cc.cpu_limit` |  | ``4`` |
| `operator.clusters.sizes.200cc.credits_per_hour` |  | ``"2"`` |
| `operator.clusters.sizes.200cc.disk_limit` |  | ``"62100MiB"`` |
| `operator.clusters.sizes.200cc.memory_limit` |  | ``"31050MiB"`` |
| `operator.clusters.sizes.200cc.scale` |  | ``1`` |
| `operator.clusters.sizes.200cc.workers` |  | ``4`` |
| `operator.clusters.sizes.256C.cpu_exclusive` |  | ``true`` |
| `operator.clusters.sizes.256C.cpu_limit` |  | ``62`` |
| `operator.clusters.sizes.256C.credits_per_hour` |  | ``"256"`` |
| `operator.clusters.sizes.256C.disk_limit` |  | ``"962560MiB"`` |
| `operator.clusters.sizes.256C.memory_limit` |  | ``"481280MiB"`` |
| `operator.clusters.sizes.256C.scale` |  | ``8`` |
| `operator.clusters.sizes.256C.workers` |  | ``62`` |
| `operator.clusters.sizes.25cc.cpu_exclusive` |  | ``false`` |
| `operator.clusters.sizes.25cc.cpu_limit` |  | ``0.5`` |
| `operator.clusters.sizes.25cc.credits_per_hour` |  | ``"0.25"`` |
| `operator.clusters.sizes.25cc.disk_limit` |  | ``"7762MiB"`` |
| `operator.clusters.sizes.25cc.memory_limit` |  | ``"3881MiB"`` |
| `operator.clusters.sizes.25cc.scale` |  | ``1`` |
| `operator.clusters.sizes.25cc.workers` |  | ``1`` |
| `operator.clusters.sizes.300cc.cpu_exclusive` |  | ``true`` |
| `operator.clusters.sizes.300cc.cpu_limit` |  | ``6`` |
| `operator.clusters.sizes.300cc.credits_per_hour` |  | ``"3"`` |
| `operator.clusters.sizes.300cc.disk_limit` |  | ``"93150MiB"`` |
| `operator.clusters.sizes.300cc.memory_limit` |  | ``"46575MiB"`` |
| `operator.clusters.sizes.300cc.scale` |  | ``1`` |
| `operator.clusters.sizes.300cc.workers` |  | ``6`` |
| `operator.clusters.sizes.3200cc.cpu_exclusive` |  | ``true`` |
| `operator.clusters.sizes.3200cc.cpu_limit` |  | ``62`` |
| `operator.clusters.sizes.3200cc.credits_per_hour` |  | ``"32"`` |
| `operator.clusters.sizes.3200cc.disk_limit` |  | ``"962560MiB"`` |
| `operator.clusters.sizes.3200cc.memory_limit` |  | ``"481280MiB"`` |
| `operator.clusters.sizes.3200cc.scale` |  | ``1`` |
| `operator.clusters.sizes.3200cc.workers` |  | ``62`` |
| `operator.clusters.sizes.400cc.cpu_exclusive` |  | ``true`` |
| `operator.clusters.sizes.400cc.cpu_limit` |  | ``8`` |
| `operator.clusters.sizes.400cc.credits_per_hour` |  | ``"4"`` |
| `operator.clusters.sizes.400cc.disk_limit` |  | ``"124201MiB"`` |
| `operator.clusters.sizes.400cc.memory_limit` |  | ``"62100MiB"`` |
| `operator.clusters.sizes.400cc.scale` |  | ``1`` |
| `operator.clusters.sizes.400cc.workers` |  | ``8`` |
| `operator.clusters.sizes.50cc.cpu_exclusive` |  | ``true`` |
| `operator.clusters.sizes.50cc.cpu_limit` |  | ``1`` |
| `operator.clusters.sizes.50cc.credits_per_hour` |  | ``"0.5"`` |
| `operator.clusters.sizes.50cc.disk_limit` |  | ``"15525MiB"`` |
| `operator.clusters.sizes.50cc.memory_limit` |  | ``"7762MiB"`` |
| `operator.clusters.sizes.50cc.scale` |  | ``1`` |
| `operator.clusters.sizes.50cc.workers` |  | ``1`` |
| `operator.clusters.sizes.512C.cpu_exclusive` |  | ``true`` |
| `operator.clusters.sizes.512C.cpu_limit` |  | ``62`` |
| `operator.clusters.sizes.512C.credits_per_hour` |  | ``"512"`` |
| `operator.clusters.sizes.512C.disk_limit` |  | ``"962560MiB"`` |
| `operator.clusters.sizes.512C.memory_limit` |  | ``"481280MiB"`` |
| `operator.clusters.sizes.512C.scale` |  | ``16`` |
| `operator.clusters.sizes.512C.workers` |  | ``62`` |
| `operator.clusters.sizes.600cc.cpu_exclusive` |  | ``true`` |
| `operator.clusters.sizes.600cc.cpu_limit` |  | ``12`` |
| `operator.clusters.sizes.600cc.credits_per_hour` |  | ``"6"`` |
| `operator.clusters.sizes.600cc.disk_limit` |  | ``"186301MiB"`` |
| `operator.clusters.sizes.600cc.memory_limit` |  | ``"93150MiB"`` |
| `operator.clusters.sizes.600cc.scale` |  | ``1`` |
| `operator.clusters.sizes.600cc.workers` |  | ``12`` |
| `operator.clusters.sizes.6400cc.cpu_exclusive` |  | ``true`` |
| `operator.clusters.sizes.6400cc.cpu_limit` |  | ``62`` |
| `operator.clusters.sizes.6400cc.credits_per_hour` |  | ``"64"`` |
| `operator.clusters.sizes.6400cc.disk_limit` |  | ``"962560MiB"`` |
| `operator.clusters.sizes.6400cc.memory_limit` |  | ``"481280MiB"`` |
| `operator.clusters.sizes.6400cc.scale` |  | ``2`` |
| `operator.clusters.sizes.6400cc.workers` |  | ``62`` |
| `operator.clusters.sizes.800cc.cpu_exclusive` |  | ``true`` |
| `operator.clusters.sizes.800cc.cpu_limit` |  | ``16`` |
| `operator.clusters.sizes.800cc.credits_per_hour` |  | ``"8"`` |
| `operator.clusters.sizes.800cc.disk_limit` |  | ``"248402MiB"`` |
| `operator.clusters.sizes.800cc.memory_limit` |  | ``"124201MiB"`` |
| `operator.clusters.sizes.800cc.scale` |  | ``1`` |
| `operator.clusters.sizes.800cc.workers` |  | ``16`` |
| `operator.clusters.sizes.mz_probe.cpu_exclusive` |  | ``false`` |
| `operator.clusters.sizes.mz_probe.cpu_limit` |  | ``0.1`` |
| `operator.clusters.sizes.mz_probe.credits_per_hour` |  | ``"0.00"`` |
| `operator.clusters.sizes.mz_probe.disk_limit` |  | ``"1552MiB"`` |
| `operator.clusters.sizes.mz_probe.memory_limit` |  | ``"776MiB"`` |
| `operator.clusters.sizes.mz_probe.scale` |  | ``1`` |
| `operator.clusters.sizes.mz_probe.workers` |  | ``1`` |
| `operator.features.consoleImageTagMapOverride` |  | ``{}`` |
| `operator.features.createBalancers` |  | ``true`` |
| `operator.features.createConsole` |  | ``true`` |
| `operator.image.pullPolicy` |  | ``"IfNotPresent"`` |
| `operator.image.repository` |  | ``"materialize/orchestratord"`` |
| `operator.image.tag` |  | ``"v0.126.5"`` |
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
  --set operator.image.tag=v0.126.5 \
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
  environmentdImageRef: materialize/environmentd:v0.126.5
  backendSecretName: materialize-backend
  environmentdResourceRequirements:
    limits:
      memory: 16Gi
    requests:
      cpu: "2"
      memory: 16Gi
  balancerdResourceRequirements:
    limits:
      memory: 256Mi
    requests:
      cpu: 100m
      memory: 256Mi
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

## Upgrading

Once you have the Materialize operator installed and managing your Materialize instances, you can upgrade both components. While the operator and instances can be upgraded independently, you should ensure version compatibility between them. The operator can typically manage instances within a certain version range - upgrading the operator too far ahead of your instances may cause compatibility issues.

We recommend:

- Upgrade the operator first
- Always upgrade your Materialize instances after upgrading the operator to ensure compatibility

### Upgrading the Helm Chart

To upgrade the Materialize operator to a new version:

```shell
helm upgrade my-materialize-operator materialize/misc/helm-charts/operator
```

If you have custom values, make sure to include your values file:

```shell
helm upgrade my-materialize-operator materialize/misc/helm-charts/operator -f my-values.yaml
```

### Upgrading Materialize Instances

To upgrade your Materialize instances, you'll need to update the Materialize custom resource and trigger a rollout.

By default, the operator performs rolling upgrades (`inPlaceRollout: false`) which minimize downtime but require additional Kubernetes cluster resources during the transition. However, keep in mind that rolling upgrades typically take longer to complete due to the sequential rollout process. For environments where downtime is acceptable, you can opt for in-place upgrades (`inPlaceRollout: true`).

#### Determining the Version

The compatible version for your Materialize instances is specified in the Helm chart's `appVersion`. For the installed chart version, you can run:

```shell
helm list -n materialize
```

Or check the `Chart.yaml` file in the `misc/helm-charts/operator` directory:

```yaml
apiVersion: v2
name: materialize-operator
# ...
version: v25.1.0-beta.1
appVersion: v0.125.2  # Use this version for your Materialize instances
```

Use the `appVersion` (`v0.125.2` in this case) when updating your Materialize instances to ensure compatibility.

#### Using `kubectl` patch

For standard upgrades such as image updates:

```shell
# For version updates, first update the image reference
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"materialize/environmentd:v0.125.2\"}}"

# Then trigger the rollout with a new UUID
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\"}}"
```

You can combine both operations in a single command if preferred:

```shell
kubectl patch materialize 12345678-1234-1234-1234-123456789012 \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"materialize/environmentd:v0.125.2\", \"requestRollout\": \"$(uuidgen)\"}}"
```

#### Using YAML Definition

Alternatively, you can update your Materialize custom resource definition directly:

```yaml
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:v0.125.2 # Update version as needed
  requestRollout: 22222222-2222-2222-2222-222222222222    # Generate new UUID
  forceRollout: 33333333-3333-3333-3333-333333333333      # Optional: for forced rollouts
  inPlaceRollout: false                                   # When false, performs a rolling upgrade rather than in-place
  backendSecretName: materialize-backend
```

Apply the updated definition:

```shell
kubectl apply -f materialize.yaml
```

#### Forced Rollouts

If you need to force a rollout even when there are no changes to the instance:

```shell
kubectl patch materialize <instance-name> \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\", \"forceRollout\": \"$(uuidgen)\"}}"
```

The behavior of a forced rollout follows your `inPlaceRollout` setting:
- With `inPlaceRollout: false` (default): Creates new instances before terminating the old ones, temporarily requiring twice the resources during the transition
- With `inPlaceRollout: true`: Directly replaces the instances, causing downtime but without requiring additional resources

### Verifying the Upgrade

After initiating the rollout, you can monitor the status:

```shell
# Watch the status of your Materialize environment
kubectl get materialize -n materialize-environment -w

# Check the logs of the operator
kubectl logs -l app.kubernetes.io/name=materialize-operator -n materialize
```

### Notes on Rollouts

- `requestRollout` triggers a rollout only if there are actual changes to the instance (like image updates)
- `forceRollout` triggers a rollout regardless of whether there are changes, which can be useful for debugging or when you need to force a rollout for other reasons
- Both fields expect UUID values and each rollout requires a new, unique UUID value
- `inPlaceRollout`:
  - When `false` (default): Performs a rolling upgrade by spawning new instances before terminating old ones. While this minimizes downtime, there may still be a brief interruption during the transition.
  - When `true`: Directly replaces existing instances, which will cause downtime.

# Operational Guidelines

Beyond the Helm configuration, there are other important knobs to tune to get the best out of Materialize within a
Kubernetes environment.

## Instance Types

Materialize has been vetted to work on instances with the following properties:

- ARM-based CPU
- 1:8 ratio of vCPU to GiB memory
- 1:16 ratio of vCPU to GiB local instance storage (if enabling spill-to-disk)

When operating in AWS, we recommend using the `r7gd` and `r6gd` families of instances (and `r8gd` once available)
when running with local disk, and the `r8g`, `r7g`, and `r6g` families when running without local disk.

## CPU Affinity

It is strongly recommended to enable the Kubernetes `static` [CPU management policy](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/#static-policy).
This ensures that each worker thread of Materialize is given exclusively access to a vCPU. Our benchmarks have shown this
to substantially improve the performance of compute-bound workloads.

## Learn More

- [Materialize Documentation](https://materialize.com/docs)
- [Materialize GitHub Repository](https://github.com/MaterializeInc/materialize)

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.14.2](https://github.com/norwoodj/helm-docs/releases/v1.14.2)
