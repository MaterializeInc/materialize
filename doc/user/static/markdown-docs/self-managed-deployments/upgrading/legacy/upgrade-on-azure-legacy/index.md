# Upgrade on Azure (Legacy Terraform)

Procedure to upgrade your Materialize operator and instances running on Azure



> **Disambiguation:** - To upgrade to `v26.0` using Materialize-provided Terraforms, upgrade your Terraform version to `v0.6.1` or higher, <a href="https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#v061" >Azure Terraform v0.6.1 Upgrade Notes</a>. - To upgrade to `v26.0` if <red>**not**</red> using a Materialize-provided Terraforms, you must prepare your nodes by adding the required labels. For detailed instructions, see [Prepare for swap and upgrade to v26.0](/installation/upgrade-to-swap/).


To upgrade your Materialize instances, first choose a new operator version and upgrade the Materialize operator. Then, upgrade your Materialize instances to the same version. The following tutorial upgrades your
Materialize deployment running on Azure Kubernetes Service (AKS).

The tutorial assumes you have installed Materialize on Azure Kubernetes Service
(AKS) using the instructions on [Install on
Azure](/self-managed-deployments/installation/legacy/install-on-azure-legacy/) (either from the examples/simple
directory or the root).

## Version compatibility

> **Important:** When performing major version upgrades, you can upgrade only one major version
> at a time. For example, upgrades from **v26**.1.0 to **v27**.2.0 is permitted
> but **v26**.1.0 to **v28**.0.0 is not. Skipping major versions or downgrading is
> not supported. To upgrade from v25.2 to v26.0, you must [upgrade first to v25.2.16+](https://materialize.com/docs/self-managed/v25.2/release-notes/#v25216).
>
>







**Materialize on Azure Terraform Releases:**

When upgrading, you may need or want to update your fork of the Terraform module
to upgrade.


| Terraform version | Notable changes |
| --- | --- |
| <a href="https://github.com/MaterializeInc/terraform-azurerm-materialize/releases/tag/v0.6.4" >v0.6.4</a> | <ul> <li>Released as part of v26.0.0.</li> <li>Uses <code>terraform-helm-materialize</code> version <code>v0.1.35</code>.</li> </ul>  |





## Prerequisites

> **Important:** The following procedure performs a rolling upgrade, where both the old and new
> Materialize instances are running before the the old instance are removed.
> When performing a rolling upgrade, ensure you have enough resources to support
> having both the old and new Materialize instances running.
>
>


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
deployment does not have a license key configured, contact <a href="https://materialize.com/docs/support/" >Materialize support</a>.

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


   | Terraform version | Notable changes |
   | --- | --- |
   | <a href="https://github.com/MaterializeInc/terraform-azurerm-materialize/releases/tag/v0.6.4" >v0.6.4</a> | <ul> <li>Released as part of v26.0.0.</li> <li>Uses <code>terraform-helm-materialize</code> version <code>v0.1.35</code>.</li> </ul>  |


   > **Tip:** If upgrading from a deployment that was set up using an earlier version of the
>    Terraform modules, additional considerations may apply when using an updated
>    Terraform modules to your existing deployments.
>
>
>


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





1. Back up your `terraform.tfvars` file.

   ```sh
   cp terraform.tfvars original_terraform.tfvars
   ```

1. Update the `terraform.tfvars` to set the Materialize Operator version:

   | Variable              | Description |
   |-----------------------|-------------|
   | `operator_version`    | New Materialize Operator version.<br> <ul><li>If the variable does not exist, add the variable and set to the new version.</li><li>If the variable exists, update the value to the new version.</li></ul> |

   ```sh
   ##... Existing content not shown for brevity
   ##... Leave the existing variables unchanged
   operator_version="v26.8.0"  # Set to the desired operator version
   ```

1. Initialize the terraform directory.

    ```bash
    terraform init
    ```

1. Run `terraform plan` with both the `terraform.tfvars` and your
   `mz_instances.tfvars` files and review the changes to be made.

   ```bash
   terraform plan -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```

   The plan should show the changes to be made for the `materialize_operator`.

1. If you are satisfied with the changes, apply.

   ```bash
   terraform apply -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```

   To approve the changes and apply, enter `yes`.

   Upon successful completion, you should see output with a summary of changes.

1. Verify that the operator is running:

   ```bash
   kubectl -n materialize get all
   ```

   Verify the operator upgrade by checking its events:

   ```bash
   kubectl -n materialize describe pod -l app.kubernetes.io/name=materialize-operator
   ```

   - The **Containers** section should show the ``--helm-chart-version``
     argument set to the new version.

   - The **Events** section should list that the new version of the
     orchestratord have been pulled.


1. Back up your ``mz_instances.tfvars`` file.

   ```sh
   cp mz_instances.tfvars original_mz_instances.tfvars
   ```

1. Update the `mz_instances.tfvars` to specify the upgrade variables for each
   instance:


   | Variable          | Description |
   |--------------------|-------------|
   | `create_database`  | Set to `false`. |
   | `environmentd_version`  | New Materialize instance version. This should be the same as the operator version: `v26.8.0`.|
   | `request_rollout`  or `force_rollout` | A new UUID string.  Can be generated with `uuidgen`.<br> <ul><li>`request_rollout` triggers a rollout only if changes exist. </li><li>`force_rollout`  triggers a rollout even if no changes exist.</li></ul> |
   | `inPlaceRollout` | Set to `false` to perform a rolling upgrade. For rolling upgrades, ensure you have enough resources to support having both the old and new Materialize instances running during the upgrade. |
   | `license_key` | Required. Set to the value of your license key. If your existing deployment does not have a license key, contact [Materialize support](https://materialize.com/docs/support/). If you have a license key, substitute your license key for `<ENTER YOUR LICENSE KEY HERE>`. |

   For example, the following instance specifies:

   - a `create_database` of `false`,
   - an `inPlaceRollout` of `false`,
   - an `environmentd_version` of `"v26.8.0"`,
   - a `request_rollout` of `"22222222-2222-2222-2222-222222222222"`, and
   - a `license_key` (substituting your license key for `<ENTER YOUR LICENSE KEY HERE>`).

   ```sh
   materialize_instances = [
       {
         name           = "demo"
         namespace      = "materialize-environment"
         database_name  = "demo_db"
         cpu_request    = "1"
         memory_request = "2Gi"
         memory_limit   = "2Gi"
         create_database = false
         license_key    = "<ENTER YOUR LICENSE KEY HERE>"        # Required.
         environmentd_version = "v26.8.0"
         inPlaceRollout: false                                   # When false, performs a rolling upgrade rather than in-place
         requestRollout: 22222222-2222-2222-2222-222222222222    # Enter a new UUID
       }
   ]
   ```

   <div class="warning">
      <strong class="gutter">WARNING!</strong> Please consult the Materialize team before setting inPlaceRollout to true and performing an in-place rollout. In almost all cases a rolling upgrade is preferred.
   </div>





1. Run `terraform plan` with both the `terraform.tfvars` and your
   `mz_instances.tfvars` files and review the changes to be made.

   ```bash
   terraform plan -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```

   The plan should show the changes to be made for the Materialize instance.

1. If you are satisfied with the changes, apply.

   ```bash
   terraform apply -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```

   To approve the changes and apply, enter `yes`.

   Upon successful completion, you should see output with a summary of changes.

1. Verify that the components are running after the upgrade:

   ```bash
   kubectl -n materialize-environment get all
   ```

   Verify upgrade by checking the `balancerd` events:

   ```bash
   kubectl -n materialize-environment describe pod -l app=balancerd
   ```

   The **Events** section should list that the new version of the `balancerd`
   have been pulled.

   Verify the upgrade by checking the `environmentd` events:

   ```bash
   kubectl -n materialize-environment describe pod -l app=environmentd
   ```

   The **Events** section should list that the new version of the `environmentd`
   have been pulled.

1. Open the Materialize Console. The Console should display the new version.
