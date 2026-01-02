---
audience: developer
canonical_url: https://materialize.com/docs/installation/install-on-azure/
complexity: advanced
description: Install Materialize on Azure Kubernetes Service (AKS) using Terraform
doc_type: reference
keywords:
- 'Warning:'
- Install on Azure (via Terraform)
- CREATE ONE
- CREATE A
- SELECT THE
- 'Note:'
- 'Tip:'
product_area: Deployment
status: stable
title: Install on Azure (via Terraform)
---

# Install on Azure (via Terraform)

## Purpose
Install Materialize on Azure Kubernetes Service (AKS) using Terraform

If you need to understand the syntax and options for this command, you're in the right place.


Install Materialize on Azure Kubernetes Service (AKS) using Terraform


<!-- Unresolved shortcode: {{% self-managed/materialize-components-sentence b... -->

The tutorial deploys Materialize to Azure Kubernetes Service (AKS) with a
PostgreSQL database as the metadata database and Azure premium block blob
storage for blob storage. The tutorial uses [Materialize on Azure Terraform
modules](https://github.com/MaterializeInc/terraform-azurerm-materialize) to:

- Set up the Azure Kubernetes environment
- Call
   [terraform-helm-materialize](https://github.com/MaterializeInc/terraform-helm-materialize)
   module to deploy Materialize Operator and Materialize instances to that AKS
   cluster

> **Warning:** 

> **Note:** Terraform configurations may vary based on your environment.


## Prerequisites

This section covers prerequisites.

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

Starting in v26.0, Self-Managed Materialize requires a license key.

<!-- Dynamic table: self_managed/license_key - see original docs -->

## A. Authenticate with Azure

1. Open a Terminal window.

1. Authenticate with Azure.

    ```bash
    az login
    ```text

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
   ```text

   Select the subscription and tenant.

1. Set `ARM_SUBSCRIPTION_ID` to the subscription ID.

    ```bash
    export ARM_SUBSCRIPTION_ID=<subscription-id>
    ```bash

## B. Set up Azure Kubernetes environment and install Materialize

> **Warning:** 

> **Note:** Terraform configurations may vary based on your environment.


#### Deployed components


[Materialize on Azure Terraform
module](https://github.com/MaterializeInc/terraform-azurerm-materialize) for
deploys a sample infrastructure on Azure with the following components:

<!-- Dynamic table: self_managed/azure_terraform_deployed_components - see original docs -->

> **Tip:** 

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See self-managed installation documentation --> --> -->


#### Releases


<!-- Dynamic table: self_managed/azure_terraform_versions - see original docs -->


1. Open a Terminal window.

<!-- Unresolved shortcode: {{% self-managed/versions/step-clone-azure-terrafo... -->

1. Go to the `examples/simple` folder in the Materialize Terraform repo
   directory.

   ```bash
   cd terraform-azurerm-materialize/examples/simple
   ```text

   > **Tip:** 

   <!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See self-managed installation documentation --> --> -->

   


1. Optional. Create a virtual environment, specifying a path for the new virtual
   environment:

    ```bash
    python3 -m venv <path to the new virtual environment>

    ```text

   Activate the virtual environment:
    ```bash
    source <path to the new virtual environment>/bin/activate
    ```text

1. Install the required packages.

    ```bash
    pip install -r requirements.txt
    ```text

1. Create a `terraform.tfvars` file (you can copy from the
   `terraform.tfvars.example` file) and specify:

   - The prefix for the resources. Prefix has a maximum of 12 characters and
     contains only alphanumeric characters and hyphens; e.g., `mydemo`.

   -  The location for the AKS cluster.

   ```bash
   prefix="enter-prefix"  //  maximum 12 characters, containing only alphanumeric characters and hyphens; e.g. mydemo
   location="eastus2"
   ```text

   > **Tip:** 

   <!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See self-managed installation documentation --> --> -->

   

1. Initialize the terraform directory.

    ```bash
    terraform init
    ```text

1. Use terraform plan to review the changes to be made.

    ```bash
    terraform plan
    ```text

1. If you are satisfied with the changes, apply.

    ```bash
    terraform apply
    ```text

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
   ```text

1. Configure `kubectl` to connect to your cluster:

   - `<cluster_name>`. Your cluster name has the form `<your prefix>-aks`; e.g.,
     `mz-simple-aks`.

   - `<resource_group_name>`, as specified in the output.

   ```bash
   az aks get-credentials --resource-group <resource_group_name> --name <cluster_name>
   ```text

   Alternatively, you can use the following command to get the cluster name and
   resource group name from the Terraform output:

   ```bash
   az aks get-credentials --resource-group $(terraform output -raw resource_group_name) --name $(terraform output -json aks_cluster | jq -r '.name')
   ```text

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl cluster-info
   ```text

   For help with `kubectl` commands, see [kubectl Quick
   reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

1. By default, the example Terraform installs the Materialize Operator and,
   starting in v0.3.0, a `cert-manager`. Verify the
   installation and check the status:

   
   #### Materialize Operator


   Verify the installation and check the status:

   ```shell
   kubectl get all -n materialize
   ```text

   Wait for the components to be in the `Running` state:

   ```none
   NAME                                                              READY       STATUS    RESTARTS   AGE
   pod/materialize-mydemo-materialize-operator-74d8f549d6-lkjjf      1/1         Running   0          36m

   NAME                                                         READY       UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/materialize-mydemo-materialize-operator      1/1         1            1           36m

   NAME                                                                        DESIRED   CURRENT   READY   AGE
   replicaset.apps/materialize-mydemo-materialize-operator-74d8f549d6          1         1         1       36m
   ```json

   

   #### cert-manager (Starting in version 0.3.0)


   Verify the installation and check the status:

   ```shell
   kubectl get all -n cert-manager
   ```text
   Wait for the components to be in the `Running` state:
   ```text
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
   ```json

   
   

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
         license_key    = "<ENTER YOUR LICENSE KEY HERE>"
       }
   ]
   EOF
   ```text

   - **Starting in v26.0**, Self-Managed Materialize requires a license key. To
     get your license key:
     <!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- Dynamic table - see original docs --> --> -->

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

   > **Tip:** 
   <!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See self-managed installation documentation --> --> -->

   See [Materialize on Azure releases](/installation/appendix-terraforms/#materialize-on-azure-terraform-module) for notable changes.
   

1. Run `terraform plan` with both `.tfvars` files and review the changes to be
   made.

   ```bash
   terraform plan -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```text

   The plan should show the changes to be made, with a summary similar to the
   following:

   ```text
   Plan: 9 to add, 1 to change, 0 to destroy.
   ```text

1. If you are satisfied with the changes, apply.

   ```bash
   terraform apply -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```text

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
   ```text

1. Verify the installation and check the status:

   ```bash
   kubectl get all -n materialize-environment
   ```text

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

   ```text

   If you run into an error during deployment, refer to the
   [Troubleshooting](/installation/troubleshooting/).

1. Open the Materialize Console in your browser:


   

   #### Via Network Load Balancer


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

   

   #### Via port forwarding


   <!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See self-managed installation documentation --> --> -->

   
   

   > **Tip:** 

   <!-- Unresolved shortcode: {{% self-managed/troubleshoot-console-mz_catalog_s... -->

   

## Next steps

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See self-managed installation documentation --> --> -->

## Cleanup

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See self-managed installation documentation --> --> -->

  > **Tip:** 

  If the `terraform destroy` command is unable to delete the subnet because it
  is in use, you can rerun the `terraform destroy` command.

  

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Appendix: Azure deployment guidelines](/installation/install-on-azure/
  appendix-deployment-guidelines)
- [Installation](/installation/)


---

## Appendix: Azure deployment guidelines


This section covers appendix: azure deployment guidelines.

## Recommended instance types

As a general guideline, we recommend:

- Processor Type: ARM-based CPU

- Sizing: 2:1 disk-to-RAM ratio with spill-to-disk enabled.

### Recommended Azure VM Types with Local NVMe Disks

When operating on Azure in production, we recommend [Epdsv6
sizes](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/memory-optimized/epdsv6-series?tabs=sizebasic#sizes-in-series)
Azure VM Types with Local NVMe Disk:

| VM Size            | vCPUs | Memory  | Ephemeral Disk | Disk-to-RAM Ratio |
| ------------------ | ----- | ------- | -------------- | ----------------- |
| Standard_E2pds_v6  | 2     | 16 GiB  | 75 GiB         | ~4.7:1           |
| Standard_E4pds_v6  | 4     | 32 GiB  | 150 GiB        | ~4.7:1           |
| Standard_E8pds_v6  | 8     | 64 GiB  | 300 GiB        | ~4.7:1           |
| Standard_E16pds_v6 | 16    | 128 GiB | 600 GiB        | ~4.7:1           |
| Standard_E32pds_v6 | 32    | 256 GiB | 1,200 GiB      | ~4.7:1           |

> **Important:** 

These VM types provide **ephemeral** local NVMe SSD disks. Data is lost
when the VM is stopped or deleted.


See also [Locally attached NVMe storage](#locally-attached-nvme-storage).

## Locally-attached NVMe storage

For optimal performance, Materialize requires fast, locally-attached NVMe
storage. Having a locally-attached storage allows Materialize to spill to disk
when operating on datasets larger than main memory as well as allows for a more
graceful degradation rather than OOMing. Network-attached storage (like EBS
volumes) can significantly degrade performance and is not supported.

### Swap support

Starting in v0.6.1 of Materialize on Azure Terraform,
disk support (using swap on NVMe instance storage) may be enabled for
Materialize. With this change, the Terraform:

  - Creates a node group for Materialize.
  - Configures NVMe instance store volumes as swap using a daemonset.
  - Enables swap at the Kubelet.

For swap support, the following configuration option is available:

- [`swap_enabled`](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#input_swap_enabled)

See [Upgrade Notes](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#v061).

## Recommended Azure Blob Storage

Materialize writes **block** blobs on Azure. As a general guideline, we
recommend **Premium block blob** storage accounts.

## CPU affinity

It is strongly recommended to enable the Kubernetes `static` [CPU management policy](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/#static-policy).
This ensures that each worker thread of Materialize is given exclusively access to a vCPU. Our benchmarks have shown this
to substantially improve the performance of compute-bound workloads.

## TLS

When running with TLS in production, run with certificates from an official
Certificate Authority (CA) rather than self-signed certificates.

## See also

- [Configuration](/installation/configuration/)
- [Installation](/installation/)
- [Troubleshooting](/installation/troubleshooting/)

[`enable_disk_support`]: https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#disk-support-for-materialize-on-azure

[`disk_support_config`]:
    https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#input_disk_support_config

[`disk_setup_image`]:
    https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#input_disk_setup_image


---

## Appendix: Required configuration


When using the root `main.tf` file from the [Materialize on Azure Terraform
module](https://github.com/MaterializeInc/terraform-azurerm-materialize), the
following configurations are required. [^1]

## Required variables

When using the root `main.tf` file from the [Materialize on Azure Terraform
module](https://github.com/MaterializeInc/terraform-azurerm-materialize), the
following variables must be set: [^1]

<!-- Dynamic table: self_managed/azure_required_variables - see original docs -->

For a list of all variables, see the
[README.md](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#inputs)
or the [`variables.tf` file](https://github.com/MaterializeInc/terraform-azurerm-materialize/blob/main/variables.tf).

## Resource group

When using the root `main.tf` file from the [Materialize on Azure Terraform
module](https://github.com/MaterializeInc/terraform-azurerm-materialize), an
Azure Resource Group `azurerm_resource_group` is required.[^1] You can either
create a new resource group or use an existing resource group:

- **To create a new resource group**, declare the resource group to create in
  your configuration:

  ```hcl
  resource "azurerm_resource_group" "materialize" {
    name     = var.resource_group_name
    location = var.location                    # Defaults to eastus2
    tags     = var.tags                        # Optional
  }
  ```text

- **To use an existing resource group**, set the [`resource_group_name`
  variable](https://github.com/MaterializeInc/terraform-azurerm-materialize/blob/main/variables.tf)
  to that group's name.

## Required providers

When using the root `main.tf` file from the [Materialize on Azure Terraform
module
v0.2.0+](https://github.com/MaterializeInc/terraform-azurerm-materialize), the
following provider declarations are required: [^1]

```hcl
provider "azurerm" {
  # Set the Azure subscription ID here or use the ARM_SUBSCRIPTION_ID environment variable
  # subscription_id = "XXXXXXXXXXXXXXXXXXX"

  # Specify addition Azure provider configuration as needed

  features { }
}


provider "kubernetes" {
  host                   = module.aks.cluster_endpoint
  client_certificate     = base64decode(module.aks.kube_config[0].client_certificate)
  client_key             = base64decode(module.aks.kube_config[0].client_key)
  cluster_ca_certificate = base64decode(module.aks.kube_config[0].cluster_ca_certificate)
}

provider "helm" {
  kubernetes {
    host                   = module.aks.cluster_endpoint
    client_certificate     = base64decode(module.aks.kube_config[0].client_certificate)
    client_key             = base64decode(module.aks.kube_config[0].client_key)
    cluster_ca_certificate = base64decode(module.aks.kube_config[0].cluster_ca_certificate)
  }
}
```json

[^1]: If using the `examples/simple/main.tf`, the example configuration handles
them for you.


---

## Upgrade on Azure (Terraform)


> **Disambiguation:** - To upgrade to `v26.0` using Materialize-provided Terraforms, upgrade your
Terraform version to `v0.6.1` or higher, [Azure Terraform v0.6.1 Upgrade
Notes](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#v061)
.

- To upgrade to `v26.0` if <red>**not**</red> using a Materialize-provided Terraforms, you must
prepare your nodes by adding the required labels. For detailed instructions, see
[Prepare for swap and upgrade to v26.0](/installation/upgrade-to-swap/).

To upgrade your Materialize instances, first choose a new operator version and upgrade the Materialize operator. Then, upgrade your Materialize instances to the same version. The following tutorial upgrades your
Materialize deployment running on Azure Kubernetes Service (AKS).

The tutorial assumes you have installed Materialize on Azure Kubernetes Service
(AKS) using the instructions on [Install on
Azure](/installation/install-on-azure/) (either from the examples/simple
directory or the root).

## Version compatibility

> **Important:** 

When performing major version upgrades, you can upgrade only one major version
at a time. For example, upgrades from **v26**.1.0 to **v27**.2.0 is permitted
but **v26**.1.0 to **v28**.0.0 is not. Skipping major versions or downgrading is
not supported. To upgrade from v25.2 to v26.0, you must [upgrade first to v25.2.16+](../self-managed/v25.2/release-notes/#v25216).


#### Materialize on Azure Terraform Releases


When upgrading, you may need or want to update your fork of the Terraform module
to upgrade.

<!-- Dynamic table: self_managed/azure_terraform_versions - see original docs -->


## Prerequisites

> **Important:** 

The following procedure performs a rolling upgrade, where both the old and new
Materialize instances are running before the the old instance are removed.
When performing a rolling upgrade, ensure you have enough resources to support
having both the old and new Materialize instances running.


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

Starting in v26.0, Materialize requires a license key. If your existing
deployment does not have a license key configured, contact [Materialize support](../support/).


## A. Authenticate with Azure

1. Open a Terminal window.

1. Authenticate with Azure.

    ```bash
    az login
    ```text

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
   ```text

   Select the subscription and tenant.

1. Set `ARM_SUBSCRIPTION_ID` to the subscription ID.

    ```bash
    export ARM_SUBSCRIPTION_ID=<subscription-id>
    ```bash

## B. Upgrade process

1. Go to the Terraform directory for your Materialize deployment. For example,
   if you deployed from the `examples/simple` directory:

   ```bash
   cd terraform-azurerm-materialize/examples/simple
   ```text

1. Optional. You may need to update your fork of the Terraform module to
   upgrade.

   <!-- Dynamic table: self_managed/azure_terraform_versions - see original docs -->

   > **Tip:** 

   <!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See self-managed installation documentation --> --> -->

   

1. Optional. Create a virtual environment, specifying a path for the new virtual
   environment:

    ```bash
    python3 -m venv <path to the new virtual environment>
    ```text

   Activate the virtual environment:
    ```bash
    source <path to the new virtual environment>/bin/activate
    ```text

1. Install the required packages.

    ```bash
    pip install -r requirements.txt
    ```text

1. Configure `kubectl` to connect to your cluster:

   - `<cluster_name>`. Your cluster name has the form `<your prefix>-aks`; e.g.,
     `mz-simple-aks`.

   - `<resource_group_name>`, as specified in the output. You resource group
     name has the form `<your prefix>-rg`; e.g., `mz-simple-rg`.

   ```bash
   az aks get-credentials --resource-group <resource_group_name> --name <cluster_name>
   ```text

   Alternatively, you can use the following command to get the cluster name and
   resource group name from the Terraform output (in your `terraform.tfstate`
   file):

   ```bash
   az aks get-credentials --resource-group $(terraform output -raw resource_group_name) --name $(terraform output -json aks_cluster | jq -r '.name')
   ```text

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl get nodes
   ```

   For help with `kubectl` commands, see [kubectl Quick
   reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

<!-- Unresolved shortcode: {{% self-managed/versions/upgrade/upgrade-steps-cl... -->