# Materialize Kubernetes Operator Helm Chart

![Version: v26.0.0-beta.1](https://img.shields.io/badge/Version-v26.0.0--beta.1-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: v26.0.0-dev.0](https://img.shields.io/badge/AppVersion-v26.0.0--dev.0-informational?style=flat-square)

Materialize Kubernetes Operator Helm Chart

This Helm chart deploys the Materialize operator on a Kubernetes cluster. The operator manages Materialize environments within your Kubernetes infrastructure.

## Prerequisites

- Kubernetes 1.29+
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
helm install my-materialize-operator misc/helm-charts/operator --namespace materialize --create-namespace
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
| `balancerd.affinity` | Affinity to use for balancerd pods spawned by the operator | ``nil`` |
| `balancerd.defaultResources` | Default resources if not set in the Materialize CR | ``{"limits":{"memory":"256Mi"},"requests":{"cpu":"500m","memory":"256Mi"}}`` |
| `balancerd.enabled` | Flag to indicate whether to create balancerd pods for the environments | ``true`` |
| `balancerd.nodeSelector` | Node selector to use for balancerd pods spawned by the operator | ``nil`` |
| `balancerd.tolerations` | Tolerations to use for balancerd pods spawned by the operator | ``nil`` |
| `clusterd.affinity` | Affinity to use for clusterd pods spawned by the operator | ``nil`` |
| `clusterd.nodeSelector` | Node selector to use for all clusterd pods spawned by the operator | ``{}`` |
| `clusterd.scratchfsNodeSelector` | Additional node selector to use for clusterd pods when using an LVM scratch disk. This will be merged with the values in `nodeSelector`. | ``{"materialize.cloud/scratch-fs":"true"}`` |
| `clusterd.swapNodeSelector` | Additional node selector to use for clusterd pods when using swap. This will be merged with the values in `nodeSelector`. | ``{"materialize.cloud/swap":"true"}`` |
| `clusterd.tolerations` | Tolerations to use for clusterd pods spawned by the operator | ``nil`` |
| `console.affinity` | Affinity to use for console pods spawned by the operator | ``nil`` |
| `console.defaultResources` | Default resources if not set in the Materialize CR | ``{"limits":{"memory":"256Mi"},"requests":{"cpu":"500m","memory":"256Mi"}}`` |
| `console.enabled` | Flag to indicate whether to create console pods for the environments | ``true`` |
| `console.imageTagMapOverride` | Override the mapping of environmentd versions to console versions | ``{}`` |
| `console.nodeSelector` | Node selector to use for console pods spawned by the operator | ``nil`` |
| `console.tolerations` | Tolerations to use for console pods spawned by the operator | ``nil`` |
| `environmentd.affinity` | Affinity to use for environmentd pods spawned by the operator | ``nil`` |
| `environmentd.defaultResources` | Default resources if not set in the Materialize CR | ``{"limits":{"memory":"4Gi"},"requests":{"cpu":"1","memory":"4095Mi"}}`` |
| `environmentd.nodeSelector` | Node selector to use for environmentd pods spawned by the operator | ``{}`` |
| `environmentd.tolerations` | Tolerations to use for environmentd pods spawned by the operator | ``nil`` |
| `networkPolicies.egress` | egress from Materialize pods to sources and sinks | ``{"cidrs":["0.0.0.0/0"],"enabled":false}`` |
| `networkPolicies.enabled` | Whether to enable network policies for securing communication between pods | ``false`` |
| `networkPolicies.ingress` | Whether to enable ingress to the SQL and HTTP interfaces on environmentd or balancerd | ``{"cidrs":["0.0.0.0/0"],"enabled":false}`` |
| `networkPolicies.internal` | Whether to enable internal communication between Materialize pods | ``{"enabled":false}`` |
| `observability.enabled` | Whether to enable observability features | ``true`` |
| `observability.podMetrics.enabled` | Whether to enable the pod metrics scraper which populates the Environment Overview Monitoring tab in the web console (requires metrics-server to be installed) | ``false`` |
| `observability.prometheus.scrapeAnnotations.enabled` | Whether to annotate pods with common keys used for prometheus scraping. | ``true`` |
| `operator.additionalMaterializeCRDColumns` | Additional columns to display when printing the Materialize CRD in table format. | ``nil`` |
| `operator.affinity` | Affinity to use for the operator pod | ``nil`` |
| `operator.args.enableInternalStatementLogging` |  | ``true`` |
| `operator.args.enableLicenseKeyChecks` |  | ``false`` |
| `operator.args.startupLogFilter` | Log filtering settings for startup logs | ``"INFO,mz_orchestratord=TRACE"`` |
| `operator.cloudProvider.providers.aws.accountID` | When using AWS, accountID is required | ``""`` |
| `operator.cloudProvider.providers.aws.enabled` |  | ``false`` |
| `operator.cloudProvider.providers.aws.iam.roles.connection` | ARN for CREATE CONNECTION feature | ``""`` |
| `operator.cloudProvider.providers.aws.iam.roles.environment` | ARN of the IAM role for environmentd | ``""`` |
| `operator.cloudProvider.providers.gcp` | GCP Configuration (placeholder for future use) | ``{"enabled":false}`` |
| `operator.cloudProvider.region` | Common cloud provider settings | ``"kind"`` |
| `operator.cloudProvider.type` | Specifies cloud provider. Valid values are 'aws', 'gcp', 'azure' , 'generic', or 'local' | ``"local"`` |
| `operator.clusters.defaultReplicationFactor.analytics` |  | ``0`` |
| `operator.clusters.defaultReplicationFactor.probe` |  | ``0`` |
| `operator.clusters.defaultReplicationFactor.support` |  | ``0`` |
| `operator.clusters.defaultReplicationFactor.system` |  | ``0`` |
| `operator.clusters.defaultSizes.analytics` |  | ``"25cc"`` |
| `operator.clusters.defaultSizes.catalogServer` |  | ``"25cc"`` |
| `operator.clusters.defaultSizes.default` |  | ``"25cc"`` |
| `operator.clusters.defaultSizes.probe` |  | ``"mz_probe"`` |
| `operator.clusters.defaultSizes.support` |  | ``"25cc"`` |
| `operator.clusters.defaultSizes.system` |  | ``"25cc"`` |
| `operator.clusters.swap_enabled` |  | ``true`` |
| `operator.image.pullPolicy` | Policy for pulling the image: "IfNotPresent" avoids unnecessary re-pulling of images | ``"IfNotPresent"`` |
| `operator.image.repository` | The Docker repository for the operator image | ``"materialize/orchestratord"`` |
| `operator.image.tag` | The tag/version of the operator image to be used | ``"v26.0.0-dev.0--pr.gfa8ba620176a7d0f9abcc64f89f5d21db80c6d94"`` |
| `operator.nodeSelector` | Node selector to use for the operator pod | ``nil`` |
| `operator.resources.limits` | Resource limits for the operator's CPU and memory | ``{"memory":"512Mi"}`` |
| `operator.resources.requests` | Resources requested by the operator for CPU and memory | ``{"cpu":"100m","memory":"512Mi"}`` |
| `operator.secretsController` | Which secrets controller to use for storing secrets. Valid values are 'kubernetes' and 'aws-secrets-manager'. Setting 'aws-secrets-manager' requires a configured AWS cloud provider and IAM role for the environment with Secrets Manager permissions. | ``"kubernetes"`` |
| `operator.tolerations` | Tolerations to use for the operator pod | ``nil`` |
| `rbac.create` | Whether to create necessary RBAC roles and bindings | ``true`` |
| `schedulerName` | Optionally use a non-default kubernetes scheduler. | ``nil`` |
| `serviceAccount.create` | Whether to create a new service account for the operator | ``true`` |
| `serviceAccount.name` | The name of the service account to be created | ``"orchestratord"`` |
| `storage.storageClass.allowVolumeExpansion` |  | ``false`` |
| `storage.storageClass.create` | Set to false to use an existing StorageClass instead. Refer to the [Kubernetes StorageClass documentation](https://kubernetes.io/docs/concepts/storage/storage-classes/) | ``false`` |
| `storage.storageClass.name` | Name of the StorageClass to create/use: eg "openebs-lvm-instance-store-ext4" | ``""`` |
| `storage.storageClass.parameters` | Parameters for the CSI driver | ``{"fsType":"ext4","storage":"lvm","volgroup":"instance-store-vg"}`` |
| `storage.storageClass.provisioner` | CSI driver to use, eg "local.csi.openebs.io" | ``""`` |
| `storage.storageClass.reclaimPolicy` |  | ``"Delete"`` |
| `storage.storageClass.volumeBindingMode` |  | ``"WaitForFirstConsumer"`` |
| `telemetry.enabled` |  | ``true`` |
| `telemetry.segmentApiKey` |  | ``"hMWi3sZ17KFMjn2sPWo9UJGpOQqiba4A"`` |
| `telemetry.segmentClientSide` |  | ``true`` |
| `tls.defaultCertificateSpecs` |  | ``{}`` |

Specify each parameter using the `--set key=value[,key=value]` argument to `helm install`. For example:

```shell
helm install my-materialize-operator \
  --set operator.image.tag=v26.0.0-dev.0 \
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
  environmentdImageRef: materialize/environmentd:v26.0.0-dev.0
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

By default, the operator performs rolling upgrades (`rolloutStrategy: WaitUntilReady`) which minimizes downtime but require additional Kubernetes cluster resources during the transition.

For environments without enough capacity to perform the `WaitUntilReady` strategy, and where downtime is acceptable, there is the `ImmediatelyPromoteCausingDowntime` strategy. This strategy will cause downtime and is not recommended. If you think you need this, please reach out to Materialize engineering to discuss your situation.

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
version: v26.0.0-dev.0
appVersion: v0.147.0  # Use this version for your Materialize instances
```

Use the `appVersion` (`v0.147.0` in this case) when updating your Materialize instances to ensure compatibility.

#### Using `kubectl` patch

For standard upgrades such as image updates:

```shell
# For version updates, first update the image reference
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"materialize/environmentd:v0.147.0\"}}"

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
  -p "{\"spec\": {\"environmentdImageRef\": \"materialize/environmentd:v0.147.0\", \"requestRollout\": \"$(uuidgen)\"}}"
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
  environmentdImageRef: materialize/environmentd:v0.147.0 # Update version as needed
  requestRollout: 22222222-2222-2222-2222-222222222222    # Generate new UUID
  forceRollout: 33333333-3333-3333-3333-333333333333      # Optional: for forced rollouts
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

## Learn More

- [Materialize Documentation](https://materialize.com/docs)
- [Materialize GitHub Repository](https://github.com/MaterializeInc/materialize)

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.14.2](https://github.com/norwoodj/helm-docs/releases/v1.14.2)
