---
title: "Upgrade"
description: "Procedure to upgrade your Materialize operator and instances running on Azure"
menu:
  main:
    parent: "install-on-azure-legacy-terraform-module"
    identifier: "upgrade-on-azure"
    weight: 10
---

{{< annotation type="Disambiguation" >}}

This page is for upgrading from v25.2.13 or later using Terraform. For upgrading
from v25.2.12 or earlier, see:

- For upgrade via Terraform, see {{< include-md
file="shared-content/self-managed/azure-terraform-v0.6.1-upgrade-notes.md" >}}.

- For upgrade via Helm, see [Upgrade from v25.2.12 or earlier(Non-Terraform)](/installation/install-on-azure/upgrade-to-swap/).

{{< /annotation >}}

To upgrade your Materialize instances, first choose a new operator version and upgrade the Materialize operator. Then, upgrade your Materialize instances to the same version. The following tutorial upgrades your
Materialize deployment running on Azure Kubernetes Service (AKS).

The tutorial assumes you have installed Materialize on Azure Kubernetes Service
(AKS) using the instructions on [Install on
Azure](/installation/install-on-azure/) (either from the examples/simple
directory or the root).

## Version compatibility

{{< include-md file="shared-content/self-managed/version-compatibility-upgrade-banner.md" >}}



{{< tabs >}}

{{< tab "Materialize on Azure Terraform Releases" >}}

When upgrading, you may need or want to update your fork of the Terraform module
to upgrade.

{{< yaml-table data="self_managed/azure_terraform_versions" >}}

{{</ tab >}}
{{</ tabs >}}

## Prerequisites

{{< important >}}

The following procedure performs a rolling upgrade, where both the old and new
Materialize instances are running before the the old instance are removed.
When performing a rolling upgrade, ensure you have enough resources to support
having both the old and new Materialize instances running.

{{</ important >}}

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

## B. Upgrade process

1. Go to the Terraform directory for your Materialize deployment. For example,
   if you deployed from the `examples/simple` directory:

   ```bash
   cd terraform-azurerm-materialize/examples/simple
   ```

1. Optional. You may need to update your fork of the Terraform module to
   upgrade.

   {{< yaml-table data="self_managed/azure_terraform_versions" >}}

   {{< tip >}}

   {{% self-managed/azure-terraform-upgrade-notes %}}

   {{</ tip >}}

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

1. Configure `kubectl` to connect to your cluster:

   - `<cluster_name>`. Your cluster name has the form `<your prefix>-aks`; e.g.,
     `mz-simple-aks`.

   - `<resource_group_name>`, as specified in the output. You resource group
     name has the form `<your prefix>-rg`; e.g., `mz-simple-rg`.

   ```bash
   az aks get-credentials --resource-group <resource_group_name> --name <cluster_name>
   ```

   Alternatively, you can use the following command to get the cluster name and
   resource group name from the Terraform output (in your `terraform.tfstate`
   file):

   ```bash
   az aks get-credentials --resource-group $(terraform output -raw resource_group_name) --name $(terraform output -json aks_cluster | jq -r '.name')
   ```

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl get nodes
   ```

   For help with `kubectl` commands, see [kubectl Quick
   reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

{{% self-managed/versions/upgrade/upgrade-steps-cloud %}}
