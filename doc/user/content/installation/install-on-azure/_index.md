---
title: "Install on Azure"
description: "Install Materialize on Azure Kubernetes Service (AKS) using Terraform"
disable_list: true
menu:
  main:
    parent: "installation"
    identifier: "install-on-azure"
---

{{% self-managed/materialize-components-sentence %}}

The tutorial deploys Materialize to Azure Kubernetes Service (AKS) with a
PostgreSQL database as the metadata database and Azure Blob Storage for blob
storage. The tutorial uses [Materialize on Azure Terraform
modules](https://github.com/MaterializeInc/terraform-azurerm-materialize) to:

- Set up the Azure Kubernetes environment
- Call
   [terraform-helm-materialize](https://github.com/MaterializeInc/terraform-helm-materialize)
   module to deploy Materialize Operator and Materialize instances to that AKS
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

If you don't have Helm version 3.2.0+ installed, install. For details, see to
the [Helm documentation](https://helm.sh/docs/intro/install/).

### jq (Optional)

*Optional*. `jq` is used to parse the AKS cluster name and region from the
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

The tutorial uses the `main.tf` found in the `examples/simple/` directory, which
requires minimal user input. For more configuration options, you can run the
`main.tf` file at the [root of the
repository](https://github.com/MaterializeInc/terraform-azurerm-materialize/)
instead. When running with the root `main.tf`, you must declare the required
providers. See [Providers
Configuration](/installation/install-on-azure/appendix-azure-provider-configuration/)
for details.

For details on the  `examples/simple/` infrastructure configuration (such as the
node instance type, etc.), see the
[examples/simple/main.tf](https://github.com/MaterializeInc/terraform-azurerm-materialize/blob/main/examples/simple/main.tf).
{{< /tip >}}

1. Open a Terminal window.

{{% self-managed/versions/step-clone-azure-terraform-repo %}}

1. Go to the `examples/simple` folder in the Materialize Terraform repo
   directory.

   ```bash
   cd terraform-azurerm-materialize/examples/simple
   ```

   {{< tip >}}
   The tutorial uses the `main.tf` found in the `examples/simple/` directory,
   which requires minimal user input. For more configuration options, you can
   run the `main.tf` file at the [root of the
   repository](https://github.com/MaterializeInc/terraform-azurerm-materialize/)
   instead. When running with the root `main.tf`, you must declare the required
   providers. See [Providers
   Configuration](/installation/install-on-azure/appendix-azure-provider-configuration/)
   for details.

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

   - The prefix for the resources. Prefix has a maximum of 12 characters and
     contains only alphanumeric characters and hyphens; e.g., `mydemo`.

   -  The location for the AKS cluster.

   ```bash
   prefix="enter-prefix"  //  maximum 12 characters, containing only alphanumeric characters and hyphens; e.g. mydemo
   location="eastus2"
   ```

1. Initialize the terraform directory.

    ```bash
    terraform init
    ```

1. Use terraform plan to review the changes to be made.

    ```bash
    terraform plan
    ```

1. If you are satisfied with the changes, apply.

    ```bash
    terraform apply
    ```

   To approve the changes and apply, enter `yes`.

   Upon successful completion, various fields and their values are output:

   ```bash
   Apply complete! Resources: 21 added, 0 changed, 0 destroyed.

   Outputs:

   aks_cluster = <sensitive>
   connection_strings = <sensitive>
   kube_config = <sensitive>
   resource_group_name = "mydemo-rg"
   ```

1. Configure `kubectl` to connect to your cluster:

   - `<cluster_name>`. Your cluster name has the form `<your prefix>-aks`; e.g.,
     `mz-simple-aks`.

   - `<resource_group_name>`, as specified in the output.

   ```bash
   az aks get-credentials --resource-group <resource_group_name> --name <cluster_name>
   ```

   Alternatively, you can use the following command to get the cluster name and
   resource group name from the Terraform output:

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
   pod/materialize-mydemo-materialize-operator-74d8f549d6-lkjjf   1/1         Running   0          36m

   NAME                                                         READY       UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/materialize-mydemo-materialize-operator   1/1         1            1           36m

   NAME                                                                        DESIRED   CURRENT   READY   AGE
   replicaset.apps/materialize-mydemo-materialize-operator-74d8f549d6       1         1         1       36m
    ```

   If you run into an error during deployment, refer to the
   [Troubleshooting](/installation/troubleshooting/).

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

1. Run `terraform plan` with both `.tfvars` files and review the changes to be
   made.

   ```bash
   terraform plan -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```

   The plan should show the changes to be made, with a summary similar to the
   following:

   ```
   Plan: 4 to add, 0 to change, 0 to destroy.
   ```

1. If you are satisfied with the changes, apply.

   ```bash
   terraform apply -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```

   To approve the changes and apply, enter `yes`.

   Upon successful completion, you should see output with a summary similar to the following:

   ```bash
   Apply complete! Resources: 4 added, 0 changed, 0 destroyed.

   Outputs:

   aks_cluster = <sensitive>
   connection_strings = <sensitive>
   kube_config = <sensitive>
   resource_group_name = "mydemo-rg"
   ```

1. Verify the installation and check the status:

   ```bash
   kubectl get all -n materialize-environment
   ```

   Wait for the components to be ready and in the `Running` state.

   ```none
   NAME                                             READY   STATUS      RESTARTS      AGE
   pod/create-db-demo-db-pw7mj                      0/1     Completed   0             39s
   pod/mzl88mc8f6if-balancerd-b66f4c485-rnvxj       1/1     Running     0             15s
   pod/mzl88mc8f6if-cluster-s2-replica-s1-gen-1-0   1/1     Running     0             18s
   pod/mzl88mc8f6if-cluster-u1-replica-u1-gen-1-0   1/1     Running     0             18s
   pod/mzl88mc8f6if-console-689565cfcc-4dkzf        1/1     Running     0             7s
   pod/mzl88mc8f6if-console-689565cfcc-g2bqv        1/1     Running     0             7s
   pod/mzl88mc8f6if-environmentd-1-0                1/1     Running     0             23s

   NAME                                               TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)                                        AGE
   service/mzl88mc8f6if-balancerd                     ClusterIP   None            <none>        6876/TCP,6875/TCP                              15s
   service/mzl88mc8f6if-cluster-s2-replica-s1-gen-1   ClusterIP   None            <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   18s
   service/mzl88mc8f6if-cluster-u1-replica-u1-gen-1   ClusterIP   None            <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   18s
   service/mzl88mc8f6if-console                       ClusterIP   None            <none>        8080/TCP                                       7s
   service/mzl88mc8f6if-environmentd                  ClusterIP   None            <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            15s
   service/mzl88mc8f6if-environmentd-1                ClusterIP   None            <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            23s
   service/mzl88mc8f6if-persist-pubsub-1              ClusterIP   None            <none>        6879/TCP                                       23s

   NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/mzl88mc8f6if-balancerd   1/1     1            1           15s
   deployment.apps/mzl88mc8f6if-console     2/2     2            2           7s

   NAME                                               DESIRED   CURRENT   READY      AGE
   replicaset.apps/mzl88mc8f6if-balancerd-b66f4c485   1         1         1          16s
   replicaset.apps/mzl88mc8f6if-console-689565cfcc    2         2         2          8s

   NAME                                                        READY   AGE
   statefulset.apps/mzl88mc8f6if-cluster-s2-replica-s1-gen-1   1/1     19s
   statefulset.apps/mzl88mc8f6if-cluster-u1-replica-u1-gen-1   1/1     19s
   statefulset.apps/mzl88mc8f6if-environmentd-1                1/1     24s

   NAME                          STATUS     COMPLETIONS   DURATION   AGE
   job.batch/create-db-demo-db   Complete   1/1           10s        40s
   ```

   If you run into an error during deployment, refer to the
   [Troubleshooting](/installation/troubleshooting/).

1. Open the Materialize Console in your browser:

   {{% self-managed/port-forwarding-handling %}}

      {{< tip >}}

      {{% self-managed/troubleshoot-console-mz_catalog_server_blurb %}}

      {{< /tip >}}

## Next steps

{{% self-managed/next-steps %}}

## Cleanup

{{% self-managed/cleanup-cloud %}}

  {{< tip>}}

  If the `terraform destroy` command is unable to delete the subnet because it
  is in use, you can rerun the `terraform destroy` command.

  {{</ tip >}}

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Operational guidelines](/installation/operational-guidelines/)
- [Installation](/installation/)
