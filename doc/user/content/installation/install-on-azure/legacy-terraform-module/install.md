---
title: "Install"
description: "Install Materialize on Azure Kubernetes Service (AKS) using Terraform"
menu:
  main:
    parent: "install-on-azure-legacy-terraform-module"
    identifier: "install-azure"
    weight: 5
---


{{% self-managed/materialize-components-sentence blobstorage="blob storage; specifically **block** blob storage on Azure" %}}

The tutorial deploys Materialize to Azure Kubernetes Service (AKS) with a
PostgreSQL database as the metadata database and Azure premium block blob
storage for blob storage. The tutorial uses [Materialize on Azure Terraform
modules](https://github.com/MaterializeInc/terraform-azurerm-materialize) to:

- Set up the Azure Kubernetes environment
- Call
   [terraform-helm-materialize](https://github.com/MaterializeInc/terraform-helm-materialize)
   module to deploy Materialize Operator and Materialize instances to that AKS
   cluster

{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< self-managed/tutorial-disclaimer >}}

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

### License key

{{< include-md file="shared-content/license-key-required.md" >}}

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

{{< tabs >}}

{{< tab "Deployed components" >}}

[Materialize on Azure Terraform
module](https://github.com/MaterializeInc/terraform-azurerm-materialize) for
deploys a sample infrastructure on Azure with the following components:

{{< yaml-table data="self_managed/azure_terraform_deployed_components" >}}

{{< tip >}}

{{% self-managed/azure-terraform-configs %}}

{{< /tip >}}

{{</ tab >}}
{{< tab "Releases" >}}

{{< yaml-table data="self_managed/azure_terraform_versions" >}}

{{</ tab >}}
{{</ tabs >}}

1. Open a Terminal window.

{{% self-managed/versions/step-clone-azure-terraform-repo %}}

1. Go to the `examples/simple` folder in the Materialize Terraform repo
   directory.

   ```bash
   cd terraform-azurerm-materialize/examples/simple
   ```

   {{< tip >}}

   {{% self-managed/azure-terraform-configs %}}

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

   {{< tip >}}

   {{% self-managed/azure-terraform-configs %}}

   {{< /tip >}}

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
   Apply complete! Resources: 33 added, 0 changed, 0 destroyed.

   Outputs:

   aks_cluster = <sensitive>
   connection_strings = <sensitive>
   kube_config = <sensitive>
   load_balancer_details = {}
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

1. By default, the example Terraform installs the Materialize Operator and,
   starting in v0.3.0, a `cert-manager`. Verify the
   installation and check the status:

   {{< tabs >}}
   {{< tab "Materialize Operator" >}}

   Verify the installation and check the status:

   ```shell
   kubectl get all -n materialize
   ```

   Wait for the components to be in the `Running` state:

   ```none
   NAME                                                              READY       STATUS    RESTARTS   AGE
   pod/materialize-mydemo-materialize-operator-74d8f549d6-lkjjf      1/1         Running   0          36m

   NAME                                                         READY       UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/materialize-mydemo-materialize-operator      1/1         1            1           36m

   NAME                                                                        DESIRED   CURRENT   READY   AGE
   replicaset.apps/materialize-mydemo-materialize-operator-74d8f549d6          1         1         1       36m
   ```

   {{</ tab >}}

   {{< tab "cert-manager (Starting in version 0.3.0)" >}}

   Verify the installation and check the status:

   ```shell
   kubectl get all -n cert-manager
   ```
   Wait for the components to be in the `Running` state:
   ```
   NAME                                           READY   STATUS    RESTARTS   AGE
   pod/cert-manager-8576d99cc8-xqxbc              1/1     Running   0          4m22s
   pod/cert-manager-cainjector-664b5878d6-wc4tz   1/1     Running   0          4m22s
   pod/cert-manager-webhook-6ddb7bd6c5-vrm2p      1/1     Running   0          4m22s

   NAME                              TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)            AGE
   service/cert-manager              ClusterIP   10.1.227.230   <none>        9402/TCP           4m22s
   service/cert-manager-cainjector   ClusterIP   10.1.222.156   <none>        9402/TCP           4m22s
   service/cert-manager-webhook      ClusterIP   10.1.84.207    <none>        443/TCP,9402/TCP   4m22s

   NAME                                      READY   UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/cert-manager              1/1     1            1           4m23s
   deployment.apps/cert-manager-cainjector   1/1     1            1           4m23s
   deployment.apps/cert-manager-webhook      1/1     1            1           4m23s

   NAME                                                 DESIRED   CURRENT   READY   AGE
   replicaset.apps/cert-manager-8576d99cc8              1         1         1       4m23s
   replicaset.apps/cert-manager-cainjector-664b5878d6   1         1         1       4m23s
   replicaset.apps/cert-manager-webhook-6ddb7bd6c5      1         1         1       4m23s
   ```

   {{</ tab >}}
   {{</ tabs >}}

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
         license_key    = ""
       }
   ]
   EOF
   ```

   - **Starting in v0.3.0**, the Materialize on Azure Terraform module also
     deploys, by default, a self-signed `ClusterIssuer`. The `ClusterIssuer` is
     deployed after the `cert-manager` is deployed and running.

   - **Starting in v0.3.1**, the Materialize on Azure Terraform module also
   deploys, by default, [Load
   balancers](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#input_materialize_instances)
   for Materialize instances (i.e., the
   [`create_load_balancer`](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#input_materialize_instances)
   flag defaults to `true`). The load balancers, by default, are configured to
   be internal (i.e., the
   [`internal_load_balancer`](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#input_materialize_instances)
   flag defaults to `true`).

   - **Starting in v0.4.3**, you can specify addition configuration options via
     `environmentd_extra_args`.

   {{< tip >}}
   {{% self-managed/azure-terraform-upgrade-notes %}}

   See [Materialize on Azure releases](/installation/appendix-terraforms/#materialize-on-azure-terraform-module) for notable changes.
   {{</ tip >}}

1. Run `terraform plan` with both `.tfvars` files and review the changes to be
   made.

   ```bash
   terraform plan -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```

   The plan should show the changes to be made, with a summary similar to the
   following:

   ```
   Plan: 9 to add, 1 to change, 0 to destroy.
   ```

1. If you are satisfied with the changes, apply.

   ```bash
   terraform apply -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```

   To approve the changes and apply, enter `yes`.

   <a name="azure-terraform-output"></a>

   Upon successful completion, you should see output with a summary similar to the following:

   ```bash
   Apply complete! Resources: 9 added, 1 changed, 0 destroyed.

   Outputs:

   aks_cluster = <sensitive>
   connection_strings = <sensitive>
   kube_config = <sensitive>
   load_balancer_details = {
      "demo" = {
         "balancerd_load_balancer_ip" = "192.0.2.10"
         "console_load_balancer_ip" = "192.0.2.254"
      }
   }
   resource_group_name = "mydemo-rg"
   ```

1. Verify the installation and check the status:

   ```bash
   kubectl get all -n materialize-environment
   ```

   Wait for the components to be ready and in the `Running` state.

   ```none
   NAME                                             READY   STATUS      RESTARTS   AGE
   pod/db-demo-db-l6ss8                             0/1     Completed   0          2m21s
   pod/mz62lr3yltj8-balancerd-6d5dd6d4cf-r9nf4      1/1     Running     0          111s
   pod/mz62lr3yltj8-cluster-s2-replica-s1-gen-1-0   1/1     Running     0          114s
   pod/mz62lr3yltj8-cluster-u1-replica-u1-gen-1-0   1/1     Running     0          114s
   pod/mz62lr3yltj8-console-bfc797745-6nlwv         1/1     Running     0          96s
   pod/mz62lr3yltj8-console-bfc797745-tk9vm         1/1     Running     0          96s
   pod/mz62lr3yltj8-environmentd-1-0                1/1     Running     0          2m4s

   NAME                                               TYPE           CLUSTER-IP     EXTERNAL-IP       PORT(S)                                        AGE
   service/mz62lr3yltj8-balancerd                     ClusterIP      None           <none>            6876/TCP,6875/TCP                              111s
   service/mz62lr3yltj8-balancerd-lb                  LoadBalancer   10.1.201.77    192.0.2.10        6875:30890/TCP,6876:31750/TCP                  2m4s
   service/mz62lr3yltj8-cluster-s2-replica-s1-gen-1   ClusterIP      None           <none>            2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   114s
   service/mz62lr3yltj8-cluster-u1-replica-u1-gen-1   ClusterIP      None           <none>            2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   114s
   service/mz62lr3yltj8-console                       ClusterIP      None           <none>            8080/TCP                                       96s
   service/mz62lr3yltj8-console-lb                    LoadBalancer   10.1.130.212   192.0.2.254       8080:30379/TCP                                 2m4s
   service/mz62lr3yltj8-environmentd                  ClusterIP      None           <none>            6875/TCP,6876/TCP,6877/TCP,6878/TCP            111s
   service/mz62lr3yltj8-environmentd-1                ClusterIP      None           <none>            6875/TCP,6876/TCP,6877/TCP,6878/TCP            2m5s
   service/mz62lr3yltj8-persist-pubsub-1              ClusterIP      None           <none>            6879/TCP                                       2m4s

   NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/mz62lr3yltj8-balancerd   1/1     1            1           111s
   deployment.apps/mz62lr3yltj8-console     2/2     2            2           96s

   NAME                                                DESIRED   CURRENT   READY   AGE
   replicaset.apps/mz62lr3yltj8-balancerd-6d5dd6d4cf   1         1         1       111s
   replicaset.apps/mz62lr3yltj8-console-bfc797745      2         2         2       96s

   NAME                                                        READY   AGE
   statefulset.apps/mz62lr3yltj8-cluster-s2-replica-s1-gen-1   1/1     114s
   statefulset.apps/mz62lr3yltj8-cluster-u1-replica-u1-gen-1   1/1     114s
   statefulset.apps/mz62lr3yltj8-environmentd-1                1/1     2m4s

   NAME                   STATUS     COMPLETIONS   DURATION   AGE
   job.batch/db-demo-db   Complete   1/1           10s        2m21s

   ```

   If you run into an error during deployment, refer to the
   [Troubleshooting](/installation/troubleshooting/).

1. Open the Materialize Console in your browser:


   {{< tabs >}}

   {{< tab  "Via Network Load Balancer" >}}

   Starting in v0.3.1, for each Materialize instance, Materialize on Azure
   Terraform module also deploys load balancers (by default, internal) with the
   following listeners, including a listener on port 8080 for the Materialize
   Console:

   | Port | Description |
   | ---- | ------------|
   | 6875 | For SQL connections to the database |
   | 6876 | For HTTP(S) connections to the database |
   | **8080** | **For HTTP(S) connections to Materialize Console** |

   The load balancer details are found in the `load_balancer_details`  in
   the [Terraform output](#azure-terraform-output).

   The example uses a self-signed ClusterIssuer. As such, you may encounter a
   warning with regards to the certificate. In production, run with certificates
   from an official Certificate Authority (CA) rather than self-signed
   certificates.

   {{</ tab >}}

   {{< tab "Via port forwarding" >}}

   {{% self-managed/port-forwarding-handling %}}

   {{</ tab>}}
   {{</ tabs >}}

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
- [Appendix: Azure deployment guidelines](/installation/install-on-azure/
  appendix-deployment-guidelines)
- [Installation](/installation/)
