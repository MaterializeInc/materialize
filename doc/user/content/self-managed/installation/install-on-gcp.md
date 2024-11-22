---
title: "Install on GCP"
description: ""
robots: "noindex, nofollow"
---

The following tutorial deploys Materialize onto GCP.

{{< important >}}

For testing purposes only. For testing purposes only.  For testing purposes only. ....

{{< /important >}}

## Prerequisites

### Required

#### GCP Kubernetes environment

Materialize provides a [Terraform
module](https://github.com/MaterializeInc/terraform-google-materialize) to
deploy a sample infrastructure on GCP with the following:

- GKE component
- Storage component
- Database component for metadata storage

See the
[README](https://github.com/MaterializeInc/terraform-google-materialize/blob/main/README.md)
for information on how to deploy the infrastructure.

#### `kubectl`

Install `kubectl` and configure cluster access.
For details, see the [GCP documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl).


#### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, refer to the [Helm
documentation](https://helm.sh/docs/intro/install/).


## 1. Install the Materialize Operator

1. If installing for the first time, create a namespace. The default
   configuration uses the `materialize` namespace.

   ```bash
   kubectl create namespace materialize
   ```

1. Create a `my-GCP-values.yaml` configuration file for the Materialize
   operator. Update with details from your GCP Kubernetes environment. For more
   information on cloud provider configuration, see the [Materialize Operator
   Configuration](/self-managed/configuration/#operator-parameters).

      ```yaml
      # my-GCP-values.yaml
      # Note: Updated with recent config changes in main branch and not v0.125.2 branch


      operator:
      operator:
        args:
          startupLogFilter: INFO
        cloudProvider:
          providers:
            gcp:
              enabled: true
          region: "<your-gcp-region>"
          type: "gcp"

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


1. Clone/download the [Materialize
   repo](https://github.com/MaterializeInc/materialize).

1. Go to the Materialize repo directory.

   ```bash
   cd materialize
   ```

1. Install the Materialize operator with the release name
   `my-materialize-operator`, specifying the path to your
   `my-GCP-values.yaml` file:

   ```shell
   helm install my-materialize-operator -f path/to/my-GCP-values.yaml materialize/misc/helm-charts/operator
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
