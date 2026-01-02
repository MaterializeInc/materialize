---
audience: developer
canonical_url: https://materialize.com/docs/installation/install-on-azure/upgrade-on-azure/
complexity: intermediate
description: Procedure to upgrade your Materialize operator and instances running
  on Azure
doc_type: reference
keywords:
- UPDATE YOUR
- v27
- v26
- 'Disambiguation:'
- CREATE ONE
- Upgrade on Azure (Terraform)
- SELECT THE
- not
- 'Important:'
product_area: Deployment
status: stable
title: Upgrade on Azure (Terraform)
---

# Upgrade on Azure (Terraform)

## Purpose
Procedure to upgrade your Materialize operator and instances running on Azure

If you need to understand the syntax and options for this command, you're in the right place.


Procedure to upgrade your Materialize operator and instances running on Azure


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