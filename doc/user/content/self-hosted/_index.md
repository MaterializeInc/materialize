---
title: "Materialize Kubernetes Operator"
description: ""

---

## Materialize Kubernetes Operator

The Materialize Kubernetes Operator deploys Materialize into a Kubernetes
cluster.

This guide provides instructions to install the Materialize Operator using Helm.

## Prerequisites

### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, refer to the [Helm
documentation](https://helm.sh/docs/intro/install/).

### Kubernetes

Materialize supports [Kubernetes 1.19+](https://kubernetes.io/docs/setup/).

### `kubectl`

This tutorial uses `kubectl`. To install, refer to the [`kubectl` documentationq](https://kubernetes.io/docs/tasks/tools/).

### Kubernetes Storage Configuration

Materialize requires fast, locally-attached NVMe storage for optimal performance. Network-attached storage (like EBS volumes) can significantly degrade performance and is not supported.

We recommend using OpenEBS with LVM Local PV for managing local volumes. While other storage solutions may work, we have tested and recommend OpenEBS for optimal performance.

#### Install OpenEBS

Install OpenEBS to your running Kubernetes cluster.

```bash
# Add OpenEBS to Helm
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

Once LVM is configured, set up the storage class:

{{< tabs >}}
{{< tab "OpenEBS" >}}

If using OpenEBS, set up the storage class as follows:
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
{{< /tab >}}
{{< tab "Other Storage" >}}
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
{{< /tab >}}
{{< /tabs >}}

## Install the chart

1. If installing for the first time, create a namespace. The default
   configuration uses the `materialize` namespace.

   ```bash
   kubectl create namespace materialize
   ```

1. Set up the Materialize operator Helm repository.

   a. <red>TBD whether this is needed</red>. Add Helm to install charts that are
      hosted in the Materialize operator Helm repository:

      ```bash
      helm repo add materialize <MATERIALIZE_OPERATOR_HELM_LOCATION>
      helm repo update
      ```

   b. Install the Materialize operator with the release name
      `my-materialize-operator`

      - To use the [default configuration](/self-hosted/configuration/):

        ```shell
        helm install my-materialize-operator materialize/misc/helm-charts/operator
        ```

      * To include custom configuration, you can:

        - *Recommended:* Create a YAML file (e.g., `values.yaml`) that
          specifies the values for the parameters and then install the chart
          with the `-f` flag:

          ```shell
          helm install my-materialize-operator -f values.yaml materialize/materialize-operator
          ```

        - Specify each parameter using the `--set key=value[,key=value]`
            argument to `helm install`. For example:

            ```shell
            helm install my-materialize-operator \
              --set operator.image.tag=v1.0.0 \
              materialize/materialize-operator
            ```

1. Verify the installation:

    ```shell
    kubectl get all -n materialize
    ```

## Uninstalling the Chart

To uninstall/delete the `my-materialize-operator` deployment:

```shell
helm delete my-materialize-operator
```

This command removes all the Kubernetes components associated with the chart and
deletes the release.

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
  environmentdImageRef: materialize/environmentd:v0.125.0-dev.0--pr.g38d50d921720caaf15998f30988db1aed720b2fc
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


## Operational Guidelines

### Recommended Instance Types

Materialize has been vetted to work on instances with the following properties:

- ARM-based CPU
- 1:8 ratio of vCPU to GiB memory
- 1:16 ratio of vCPU to GiB local instance storage (if enabling spill-to-disk)

When operating in AWS, we recommend using the `r7gd` and `r6gd` families of instances (and `r8gd` once available)
when running with local disk, and the `r8g`, `r7g`, and `r6g` families when running without local disk.

### CPU Affinity

It is strongly recommended to enable the Kubernetes `static` [CPU management policy](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/#static-policy).
This ensures that each worker thread of Materialize is given exclusively access to a vCPU. Our benchmarks have shown this
to substantially improve the performance of compute-bound workloads.


## Related pages

<!-- Temporary:
Hugo will add links to the pages in the same folder.
Since we're hiding this section from the left-hand nav, adding the links here.
-->
