---
title: "Install on Azure"
description: "Install Materialize on Azure Kubernetes Service (AKS) using Terraform"
aliases:
  - /self-hosted/install-on-gcp/
menu:
  main:
    parent: "installation"
---

{{% self-managed/materialize-components-sentence %}}

The tutorial deploys Materialize to Azure Kubernetes Service (AKS) with a
PostgreSQL database as the metadata database and Azure Blob Storage for blob
storage. The tutorial uses [Materialize on Azure Terraform
modules](https://github.com/MaterializeInc/terraform-azurerm-materialize) to:

- Set up the AWS Kubernetes environment
- Call
   [terraform-helm-materialize](https://github.com/MaterializeInc/terraform-helm-materialize)
   module to deploy Materialize Operator and Materialize instances to that EKS
   cluster

{{< warning >}}

The Terraform modules used in this tutorial are provided for
demonstration/evaluation purposes only and not intended for production use.
Materialize does not support nor recommend these modules for production use.

{{< /warning >}}

## Prerequisites

### Azure subscription

If you do not have an Azure subscription to use for this tutorial, create one.

### Azure CLI

If you don't have Azure CLI installed, [install Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli).

### Terraform

If you don't have Terraform installed, [install Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform).

### kubectl

If you do not have `kubectl`, install `kubectl`.

### Python (v3.12+) and pip

If you don't have Python (v3.12 or greater) installed, install it. See
[Python.org](https://www.python.org/downloads/). If `pip` is not included with
your version of Python, install it.

### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, refer to the [Helm
documentation](https://helm.sh/docs/intro/install/).

### jq (Optional)

*Optional*. `jq` is used to parse the EKS cluster name and region from the
Terraform outputs. Alternatively, you can manually specify the name and region.
If you want to use `jq` and do not have `jq` installed, install.

## A. Authenticate with Azure

1. Open a Terminal window.

1. Authenticate with Azure.

    ```bash
    az login
    ```

   The command opens a browser window to sign in to Azure. Sign in.

1. Select the subscription and tenant to use. After you have signed in, back in
   the terminal, your tenant and subscription information is displayed.

    ```none
    Retrieving tenants and subscriptions for the selection...

    [Tenant and subscription selection]

    No     Subscription name    Subscription ID                       Tenant
    -----  -------------------  ------------------------------------  ----------------
    [1]*   ...                  ...                                   ...

   The default is marked with an *; the default tenant is '<Tenant>' and
   subscription is '<Subscription Name>' (<Subscription ID>).
   ```

   Select the subscription and tenant.

1. Set `ARM_SUBSCRIPTION_ID` to the subscription ID.

    ```bash
    export ARM_SUBSCRIPTION_ID=<subscription-id>
    ```

## B. Set up Azure Kubernetes environment and install Materialize

{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< /warning >}}

Materialize provides the [Materialize on Azure Terraform
modules](https://github.com/MaterializeInc/terraform-azurerm-materialize) for
evaluation purposes only. The modules deploy a sample infrastructure on Azure
with the following components:

- AKS cluster for Materialize workloads
- Azure Database for PostgreSQL Flexible Server for metadata storage
- Azure Blob Storage for persistence
- Required networking and security configurations
- Managed identities with proper RBAC permissions
- Materialize Operator
- Materialize instances (during subsequent runs after the Operator is running)

{{< tip >}}
The tutorial uses the module found in the `examples/simple/`
directory, which requires minimal user input. For more configuration options,
you can run the modules at the [root of the
repository](https://github.com/MaterializeInc/terraform-azurerm-materialize/)
instead.

For details on the  `examples/simple/` infrastructure configuration (such as the
node instance type, etc.), see the
[examples/simple/main.tf](https://github.com/MaterializeInc/terraform-azurerm-materialize/blob/main/examples/simple/main.tf).
{{< /tip >}}

1. Open a Terminal window.

1. Clone the [Materialize's sample Terraform
   repo](https://github.com/MaterializeInc/terraform-azurerm-materialize) and
   checkout the `v0.1.2` tag.

   {{< tabs >}}
   {{< tab "Clone via SSH" >}}
   ```bash
   git clone --depth 1 -b v0.1.2 git@github.com:MaterializeInc/terraform-azurerm-materialize.git
   ```
   {{< /tab >}}
   {{< tab "Clone via HTTPS" >}}
   ```bash
   git clone --depth 1 -b v0.1.2 https://github.com/MaterializeInc/terraform-azurerm-materialize.git
   ```
   {{< /tab >}}
   {{< /tabs >}}

1. Go to the `examples/simple` folder in the Materialize Terraform repo
   directory.

   ```bash
   cd terraform-azurerm-materialize/examples/simple
   ```

   {{< tip >}}
   The tutorial uses the module found in the `examples/simple/` directory, which
   requires minimal user input. For more configuration options, you can run the
   modules at the [root of the
   repository](https://github.com/MaterializeInc/terraform-azurerm-materialize/)
   instead.

   For details on the  `examples/simple/` infrastructure configuration (such as
   the node instance type, etc.), see the [examples/simple/main.tf](https://github.com/MaterializeInc/terraform-azurerm-materialize/blob/main/examples/simple/main.tf).
   {{< /tip >}}


1. Optional. Create a virtual environment, specifying a path for the new virtual
   environment:

    ```bash
    python3 -m venv <path to the new virtual environment>

    ```

   Activate the virtual environment:
    ```bash
    source <path to the new virtual environment>/bin/activate
    ```

1. Install the required packages.

    ```bash
    pip install -r requirements.txt
    ```

1. Create a `terraform.tfvars` file (you can copy from the
   `terraform.tfvars.example` file) and specify:

   - The prefix for the resources. Prefix must be between 3-17 characters,
     containing only alphanumeric characters and dashes.

   -  The location for the AKS cluster.

   ```bash
   prefix="enter-prefix"  //  3-17 characters, containing only alphanumeric characters and dashes; e.g. my-demo-dev
   location="eastus2"
   ```

1. Initialize the terraform directory.

    ```bash
    terraform init
    ```

1. Create a terraform plan and review the changes.

    ```bash
    terraform plan -out my-plan.tfplan
    ```

1. If you are satisfied with the changes, apply the terraform plan.

    ```bash
    terraform apply my-plan.tfplan
    ```

   Upon successful completion, various fields and their values are output:

   ```bash
   Apply complete! Resources: 21 added, 0 changed, 0 destroyed.

   Outputs:

   aks_cluster = <sensitive>
   connection_strings = <sensitive>
   kube_config = <sensitive>
   resource_group_name = "my-demo-dev-rg"
   ```

1. Configure `kubectl` to connect to your cluster:

   ```bash
   az aks get-credentials --resource-group $(terraform output -raw resource_group_name) --name $(terraform output -json aks_cluster | jq -r '.name')
   ```

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl cluster-info
   ```

   For help with `kubectl` commands, see [kubectl Quick
   reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

1. By default, the example Terraform installs the Materialize Operator. Verify
   the installation and check the status:

   ```shell
   kubectl get all -n materialize
   ```

   Wait for the components to be in the `Running` state:

   ```none
   NAME                                                              READY       STATUS    RESTARTS   AGE
   pod/materialize-mz-simple-materialize-operator-74d8f549d6-lkjjf   1/1         Running   0          36m

   NAME                                                         READY       UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/materialize-mz-simple-materialize-operator   1/1         1            1           36m

   NAME                                                                        DESIRED   CURRENT   READY   AGE
   replicaset.apps/materialize-mz-simple-materialize-operator-74d8f549d6       1         1         1       36m
    ```

1. Once the Materialize operator is deployed and running, you can deploy the
   Materialize instances. To deploy Materialize instances, create a
   `mz_instances.tfvars` file with the Materialize instance configuration.

   For example, the following specifies the configuration for a `demo` instance.

   ```bash
   cat <<EOF > mz_instances.tfvars

   materialize_instances = [
       {
         name           = "demo"
         namespace      = "materialize-environment"
         database_name  = "demo_db"
         cpu_request    = "1"
         memory_request = "2Gi"
         memory_limit   = "2Gi"
       }
   ]
   EOF
   ```

1. Create a terraform plan with both `.tfvars` files and review the changes.

   ```bash
   terraform plan -var-file=terraform.tfvars -var-file=mz_instances.tfvars -out my-plan.tfplan
   ```

   The plan should show the changes to be made, with a summary similar to the
   following:

   ```
   Plan: 4 to add, 0 to change, 0 to destroy.

   Saved the plan to: my-plan.tfplan

   To perform exactly these actions, run the following command to apply:
   terraform apply "my-plan.tfplan"
   ```

1. If you are satisfied with the changes, apply the terraform plan.

    ```bash
    terraform apply my-plan.tfplan
    ```

   Upon successful completion, you should see output with a summary similar to the following:

   ```bash
   Apply complete! Resources: 4 added, 0 changed, 0 destroyed.

   Outputs:

   aks_cluster = <sensitive>
   connection_strings = <sensitive>
   kube_config = <sensitive>
   resource_group_name = "my-demo-dev-rg"
   ```

1. Verify the installation and check the status:

   ```bash
   kubectl get all -n materialize-environment
   ```

   Wait for the components to be ready and in the `Running` state.

   ```none
   NAME                                             READY   STATUS      RESTARTS      AGE
   pod/create-db-demo-db-wxq9k                      0/1     Completed   0             2m37s
   pod/mzypt46hq3ei-balancerd-6946cdcbf6-5lrjk      1/1     Running     0             2m10s
   pod/mzypt46hq3ei-cluster-s1-replica-s1-gen-1-0   1/1     Running     0             2m12s
   pod/mzypt46hq3ei-cluster-s2-replica-s2-gen-1-0   1/1     Running     0             2m12s
   pod/mzypt46hq3ei-cluster-s3-replica-s3-gen-1-0   1/1     Running     0             2m12s
   pod/mzypt46hq3ei-cluster-u1-replica-u1-gen-1-0   1/1     Running     0             2m12s
   pod/mzypt46hq3ei-console-86b57b7688-8tqqf        1/1     Running     0             2m
   pod/mzypt46hq3ei-console-86b57b7688-sqs47        1/1     Running     0             2m
   pod/mzypt46hq3ei-environmentd-1-0                1/1     Running     0             2m20s

   NAME                                               TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)                                        AGE
   service/mzypt46hq3ei-balancerd                     ClusterIP   None            <none>        6876/TCP,6875/TCP                              2m10s
   service/mzypt46hq3ei-cluster-s1-replica-s1-gen-1   ClusterIP   None            <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   2m12s
   service/mzypt46hq3ei-cluster-s2-replica-s2-gen-1   ClusterIP   None            <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   2m12s
   service/mzypt46hq3ei-cluster-s3-replica-s3-gen-1   ClusterIP   None            <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   2m12s
   service/mzypt46hq3ei-cluster-u1-replica-u1-gen-1   ClusterIP   None            <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   2m12s
   service/mzypt46hq3ei-console                       ClusterIP   None            <none>        8080/TCP                                       2m
   service/mzypt46hq3ei-environmentd                  ClusterIP   None            <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            2m10s
   service/mzypt46hq3ei-environmentd-1                ClusterIP   None            <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            2m20s
   service/mzypt46hq3ei-persist-pubsub-1              ClusterIP   None            <none>        6879/TCP                                       2m20s

   NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/mzypt46hq3ei-balancerd   1/1     1            1           2m10s
   deployment.apps/mzypt46hq3ei-console     2/2     2            2           2m

   NAME                                                DESIRED   CURRENT   READY      AGE
   replicaset.apps/mzypt46hq3ei-balancerd-6946cdcbf6   1         1         1          2m10s
   replicaset.apps/mzypt46hq3ei-console-86b57b7688     2         2         2          2m

   NAME                                                        READY   AGE
   statefulset.apps/mzypt46hq3ei-cluster-s1-replica-s1-gen-1   1/1     2m12s
   statefulset.apps/mzypt46hq3ei-cluster-s2-replica-s2-gen-1   1/1     2m12s
   statefulset.apps/mzypt46hq3ei-cluster-s3-replica-s3-gen-1   1/1     2m12s
   statefulset.apps/mzypt46hq3ei-cluster-u1-replica-u1-gen-1   1/1     2m12s
   statefulset.apps/mzypt46hq3ei-environmentd-1                1/1     2m20s

   NAME                          STATUS     COMPLETIONS   DURATION   AGE
   job.batch/create-db-demo-db   Complete   1/1           15s        2m37s
   ```

1. Open the Materialize Console in your browser:

   1. From the previous `kubectl` output, find the Materialize console service.

      ```none
      NAME                           TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)    AGE
      service/mzypt46hq3ei-console   ClusterIP   None         <none>        8080/TCP   2m
      ```

   1. Forward the Materialize Console service to your local machine (substitute
      your service name for `mzypt46hq3ei-console`):

      ```shell
      while true;
      do kubectl port-forward svc/mzypt46hq3ei-console 8080:8080 -n materialize-environment 2>&1 |
      grep -q "portforward.go" && echo "Restarting port forwarding due to an error." || break;
      done;
      ```
      {{< note >}}
      Due to a [known Kubernetes issue](https://github.com/kubernetes/kubernetes/issues/78446),
      interrupted long-running requests through a standard port-forward cause the port forward to hang. The command above
      automatically restarts the port forwarding if an error occurs, ensuring a more stable
      connection. It detects failures by monitoring for "portforward.go" error messages.
      {{< /note >}}

   1. Open a browser and navigate to
      [http://localhost:8080](http://localhost:8080). From the Console, you can get started with the Quickstart.

      {{< tip >}}

      {{% self-managed/troubleshoot-console-mz_catalog_server_blurb %}}

      {{< /tip >}}

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

In your Terraform directory, run:

```bash
terraform destroy
```

When prompted to proceed, type `yes` to confirm the deletion.

{{< tip>}}
If the `terraform destroy` command is unable to delete the subnet because it is
in use, you can try rerunning the `terraform destroy` command.
{{</ tip >}}

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Operational guidelines](/installation/operational-guidelines/)
- [Installation](/installation/)
