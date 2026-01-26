# Upgrade on GCP (Legacy Terraform)



> **Disambiguation:** - To upgrade to `v26.0` using Materialize-provided Terraforms, upgrade your Terraform version to `v0.6.1` or higher, <a href="https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#v061" >GCP Terraform v0.6.1 Upgrade Notes</a>. - To upgrade to `v26.0` if <red>**not**</red> using a Materialize-provided Terraforms, you must prepare your nodes by adding the required labels. For detailed instructions, see [Prepare for swap and upgrade to v26.0](/self-managed-deployments/appendix/upgrade-to-swap/).


To upgrade your Materialize instances, first choose a new operator version and upgrade the Materialize operator. Then, upgrade your Materialize instances to the same version. The following tutorial upgrades your
Materialize deployment running on GCP Google Kubernetes Engine (GKE).

The tutorial assumes you have installed Materialize on GCP Google Kubernetes
Engine (GKE) using the instructions on [Install on
GCP](/self-managed-deployments/installation/legacy/install-on-gcp-legacy/) (either from the examples/simple directory
or the root).

## Version compatibility

> **Important:** When performing major version upgrades, you can upgrade only one major version
> at a time. For example, upgrades from **v26**.1.0 to **v27**.2.0 is permitted
> but **v26**.1.0 to **v28**.0.0 is not. Skipping major versions or downgrading is
> not supported. To upgrade from v25.2 to v26.0, you must [upgrade first to v25.2.16+](https://materialize.com/docs/self-managed/v25.2/release-notes/#v25216).
>
>





**Materialize on GCP Terraform Releases:**


| Terraform version | Notable changes |
| --- | --- |
| <a href="https://github.com/MaterializeInc/terraform-google-materialize/releases/tag/v0.6.4" >v0.6.4</a> | <ul> <li>Released as part of v26.0.0.</li> <li>Uses <code>terraform-helm-materialize</code> version <code>v0.1.35</code>.</li> </ul>  |





## Prerequisites

> **Important:** The following procedure performs a rolling upgrade, where both the old and new
> Materialize instances are running before the the old instance are removed.
> When performing a rolling upgrade, ensure you have enough resources to support
> having both the old and new Materialize instances running.
>
>


### Google cloud project

You need a GCP project for which you have a role (such as
`roles/resourcemanager.projectIamAdmin` or `roles/owner`) that includes [
permissions to manage access to the
project](https://cloud.google.com/iam/docs/granting-changing-revoking-access).

### gcloud CLI

If you do not have gcloud CLI, install. For details, see the [Install the gcloud
CLI documentation](https://cloud.google.com/sdk/docs/install).

### Google service account

The tutorial assumes the use of a service account. If you do not have a service
account to use for this tutorial, create a service account. For details, see
[Create service
accounts](https://cloud.google.com/iam/docs/service-accounts-create#creating).

### Terraform

If you do not have Terraform installed, [install
Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform).

### kubectl and plugins

> **Tip:** Using `gcloud` to install `kubectl` will also install the needed plugins.
> Otherwise, you will need to manually install the `gke-gcloud-auth-plugin` for
> `kubectl`.
>
>


- If you do not have `kubectl`, install `kubectl`.  To install, see [Install
  kubectl and configure cluster
  access](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl)
  for details. You will configure `kubectl` to interact with your GKE cluster
  later in the tutorial.

- If you do not have `gke-gcloud-auth-plugin` for `kubectl`, install the
  `gke-gcloud-auth-plugin`. For details, see [Install the
  gke-gcloud-auth-plugin](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#install_plugin).

### Helm 3.2.0+

If you do not have Helm version 3.2.0+ installed, install.  For details, see the
[Helm documentation](https://helm.sh/docs/intro/install/).

### jq (Optional)

*Optional*. `jq` is used to parse the EKS cluster name and region from the
Terraform outputs. Alternatively, you can manually specify the name and region.
If you want to use `jq` and do not have `jq` installed, install.

### License key

Starting in v26.0, Materialize requires a license key. If your existing
deployment does not have a license key configured, contact <a href="https://materialize.com/docs/support/" >Materialize support</a>.

## Procedure

### A. Setup GCP service account and authenticate

1. Open a Terminal window.

1. Initialize the gcloud CLI (`gcloud init`) to specify the GCP project you want
   to use. For details, see the [Initializing the gcloud CLI
   documentation](https://cloud.google.com/sdk/docs/initializing#initialize_the).

   > **Tip:** You do not need to configure a default Compute Region and Zone as you will
>    specify the region.
>


1. To the service account that will be used to perform the upgrade,
   grant the following IAM roles (if the account does not have them already):

   - `roles/editor`
   - `roles/iam.serviceAccountAdmin`
   - `roles/storage.admin`

   1. Enter your GCP project ID.

      ```bash
      read -s PROJECT_ID
      ```

   1. Find your service account email for your GCP project

      ```bash
      gcloud iam service-accounts list --project $PROJECT_ID
      ```

   1. Enter your service account email.

      ```bash
      read -s SERVICE_ACCOUNT
      ```

   1. Grant the service account the neccessary IAM roles.

      ```bash
      gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:$SERVICE_ACCOUNT" \
      --role="roles/editor"

      gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:$SERVICE_ACCOUNT" \
      --role="roles/iam.serviceAccountAdmin"

      gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:$SERVICE_ACCOUNT" \
      --role="roles/storage.admin"
      ```

1. For the service account, authenticate to allow Terraform to interact with
   your GCP project. For details, see [Terraform: Google Cloud Provider
   Configuration
   reference](https://registry.terraform.io/providers/hashicorp/google/latest/docs/guides/provider_reference#authentication).

   For example, if using [User Application Default
   Credentials](https://cloud.google.com/sdk/gcloud/reference/auth/application-default),
   you can run the following command:

   ```bash
   gcloud auth application-default login
   ```

   > **Tip:** If using `GOOGLE_APPLICATION_CREDENTIALS`, use absolute path to your key file.
>


### B. Upgrade Materialize operator and instances

1. Go to the `examples/simple` folder in the Materialize Terraform repo
   directory.

   ```bash
   cd terraform-google-materialize/examples/simple
   ```

1. Optional. You may need to update your fork of the Terraform module to
   upgrade.

   > **Tip:** If upgrading from a deployment that was set up using an earlier version of the
>    Terraform modules, additional considerations may apply when using an updated
>    Terraform modules to your existing deployments.
>
>
>    See [Materialize on GCP releases](/self-managed-deployments/appendix/legacy/appendix-legacy-terraform-releases/#materialize-on-gcp-terraform-module) for notable changes.
>
>


1. Configure `kubectl` to connect to your EKS cluster, specifying:

   - `<cluster name>`. Your cluster name has the form `<your prefix>-gke`; e.g.,
     `mz-simple-gke`.

   - `<region>`. By default, the example Terraform module uses the `us-central1`
     region.

   - `<project>`. Your GCP project ID.

   ```bash
   gcloud container clusters get-credentials <cluster-name>  \
    --region <region> \
    --project <project>
   ```

   Alternatively, you can use the following command to get the cluster name and
   region from the Terraform output and the project ID from the environment
   variable set earlier.

   ```bash
   gcloud container clusters get-credentials $(terraform output -json gke_cluster | jq -r .name) \
    --region $(terraform output -json gke_cluster | jq -r .location) --project $PROJECT_ID
   ```

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl cluster-info
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
