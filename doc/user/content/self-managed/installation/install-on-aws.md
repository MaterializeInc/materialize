---
title: "Install on AWS"
description: ""
robots: "noindex, nofollow"
---

The following tutorial deploys Materialize onto AWS.

{{< important >}}

For testing purposes only. For testing purposes only. For testing purposes only. ....

{{< /important >}}

## Prerequisites

### Required

#### AWS Kubernetes environment

Materialize provides a [Terraform
module](https://github.com/MaterializeInc/terraform-aws-materialize/blob/main/README.md)
to deploy a sample infrastructure on AWS with the following components:

- EKS component
- Networking component
- Storage component
- Database component for metadata storage

See the
[README](https://github.com/MaterializeInc/terraform-aws-materialize/blob/main/README.md)
for information on how to deploy the infrastructure.

#### `kubectl`

Install `kubectl` and configure cluster access. For details, see the [Amazon EKS
documentation](https://docs.aws.amazon.com/eks/latest/userguide/install-kubectl.html).

Configure `kubectl` to connect to your EKS cluster, replacing
`<your-region>` with the region of your EKS cluster:

```bash
aws eks update-kubeconfig --name materialize-cluster --region <your-region>
```

{{< note >}}

The exact authentication method may vary depending on your EKS configuration.

{{< /note >}}

To verify, run the following command:

```bash
kubectl get nodes
```

#### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, refer to the [Helm
documentation](https://helm.sh/docs/intro/install/).


### Recommended but optional

#### OpenEBS

For optimal performance, Materialize requires fast, *locally-attached* NVMe
storage. Having a locally-attached storage allows Materialize to spill to disk
when operating on datasets larger than main memory as well as allows for a more
graceful degradation rather than OOMing. *Network-attached* storage (like EBS
volumes) can significantly degrade performance and is not supported.

For locally-attached NVMe storage, we recommend using OpenEBS with LVM Local PV
for managing local volumes. While other storage solutions may work, we have
tested and recommend OpenEBS for optimal performance.

For locally-attached NVMe storage, install OpenEBS to your running Kubernetes
cluster.

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

#### Logical Volume Manager (LVM) configuration

Logical Volume Manager (LVM) setup varies by environment. Below is our tested
and recommended configuration:

##### AWS EC2 with Bottlerocket AMI

Tested configurations:

|                                              |      |
|----------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Instance types**                           | **r6g**, **r7g** families <br> **Note:** LVM setup may work on other instance types with local storage (like i3.xlarge, i4i.xlarge, r5d.xlarge), but we have not extensively tested these configurations. |
| **AMI**                                      | AWS Bottlerocket |
| **Instance store volumes**                   | Required |

To setup:

1. Use Bottlerocket bootstrap container for LVM configuration.
1. Configure volume group name as `instance-store-vg`

{{< tip >}}

If you are using the recommended Bottlerocket AMI with the Terraform module,
the LVM configuration is automatically handled by the EKS module using the
provided user data script.

{{< /tip >}}

To verify the LVM setup, run the following:

```bash
kubectl debug -it node/<node-name> --image=amazonlinux:2
chroot /host
lvs
```

You should see a volume group named `instance-store-vg`.

## 1. Install the Materialize Operator

1. If installing for the first time, create a namespace. The default
   configuration uses the `materialize` namespace.

   ```bash
   kubectl create namespace materialize
   ```

1. Create a `my-AWS-values.yaml` configuration file for the Materialize
   operator. Update with details from your AWS Kubernetes environment. For more
   information on cloud provider configuration, see the [Materialize Operator
   Configuration](/self-managed/configuration/#operator-parameters).

      ```yaml
      # my-AWS-values.yaml
      # Note: Updated with recent config changes in main branch and not v0.125.2 branch

      operator:
        args:
          startupLogFilter: INFO
        cloudProvider:
          providers:
            aws:
              accountID:  "<your-aws-account-id>"
              enabled: true
              iam:
                roles:
                  connection: null
                  environment: null
          region: "<your-aws-region>"
          type: "aws"

      namespace:
        create: false
        name: "materialize"

      # Adjust network policies as needed
      networkPolicies:
        enabled: true
        egress:
          enabled: true
          cidrs: ["0.0.0.0/0"]
        ingress:
          enabled: true
          cidrs: ["0.0.0.0/0"]
        internal:
          enabled: true
      ```

   If you have [opted for locally-attached storage](#openebs), include the
   storage configuration in your `my-AWS-values.yaml` file:

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
   While OpenEBS is our recommended solution, you can use any storage  provisioner that meets your performance requirements by overriding the  provisioner and parameters values.

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

1. Clone/download the [Materialize
   repo](https://github.com/MaterializeInc/materialize).

1. Go to the Materialize repo directory.

   ```bash
   cd materialize
   ```

1. Install the Materialize operator with the release name
   `my-materialize-operator`, specifying the path to your `my-AWS-values.yaml`
   file:

   ```shell
   helm install my-materialize-operator -f path/to/my-AWS-values.yaml materialize/misc/helm-charts/operator
   ```

1. Verify the installation:

    ```shell
    kubectl get all -n materialize
    ```

## 2. Install Materialize

To deploy Materialize:

1. Create a [Kubernetes
   Secret](https://kubernetes.io/docs/concepts/configuration/secret/) for your
   backend configuration information and save in a file (e.g.,
   `materialize-backend-secret.yaml`).

   Replace `${terraform_output.metadata_backend_url}` and
   `{terraform_output.persist_backend_url}` with the actual values from the
   Terraform output.

    ```yaml
    apiVersion: v1
    kind: Secret
    metadata:
      name: materialize-backend
      namespace: materialize-environment
    stringData:
      metadata_backend_url: "${terraform_output.metadata_backend_url}"
      persist_backend_url: "${terraform_output.persist_backend_url}"
    ```

1. Create a YAML file (e.g., `my-materialize.yaml`) for your Materialize
   configuration.

   Replace `${var.service_account_name}` with the the desired name for your
   Materialize. It should be a UUID (e.g.,
   `12345678-1234-1234-1234-123456789012`).

   ```yaml
   apiVersion: materialize.cloud/v1alpha1
   kind: Materialize
   metadata:
     name: "${var.service_account_name}"
     namespace: materialize-environment
   spec:
     environmentdImageRef: materialize/environmentd:latest
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
         cpu: "100m"
         memory: 256Mi
     backendSecretName: materialize-backend
   ```

1. Create the `materialize-environment` namespace and apply the files to install
   Materialize:

   ```shell
   kubectl create namespace materialize-environment
   kubectl apply -f materialize-backend-secret.yaml
   kubectl apply -f my-materialize.yaml
   ```

1. Verify the installation:

   ```bash
   kubectl get materializes -n materialize-environment
   kubectl get pods -n materialize-environment
   ```

## Troubleshooting

If you encounter issues:

1. Check operator logs:
```bash
kubectl logs -l app.kubernetes.io/name=materialize-operator -n materialize
```

2. Check environment logs:
```bash
kubectl logs -l app.kubernetes.io/name=environmentd -n materialize-environment
```

3. Verify the storage configuration:
```bash
kubectl get sc
kubectl get pv
kubectl get pvc -A
```

## Cleanup

Delete the Materialize environment:
```bash
kubectl delete -f materialize-environment.yaml
```

To uninstall the Materialize operator:
```bash
helm uninstall materialize-operator -n materialize
```

This will remove the operator but preserve any PVs and data. To completely clean
up:

```bash
kubectl delete namespace materialize
kubectl delete namespace materialize-environment
```

## See also

- [Materialize Kubernetes Operator Helm Chart](/self-managed/)
- [Materialize Operator Configuration](/self-managed/configuration/)
- [Troubleshooting](/self-managed/troubleshooting/)
- [Operational guidelines](/self-managed/operational-guidelines/)
- [Installation](/self-managed/installation/)
- [Upgrading](/self-managed/upgrading/)
