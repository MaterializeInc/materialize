<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [Install/Upgrade (Self-Managed)](/docs/installation/)
 /  [Install on GCP (via Terraform)](/docs/installation/install-on-gcp/)

</div>

# Upgrade on GCP (Terraform)

<div class="annotation">

<div class="annotation-title">

Disambiguation

</div>

<div>

- To upgrade to `v26.0` using Materialize-provided Terraforms, upgrade
  your Terraform version to `v0.6.1` or higher, [GCP Terraform v0.6.1
  Upgrade
  Notes](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#v061).

- To upgrade to `v26.0` if **not** using a Materialize-provided
  Terraforms, you must prepare your nodes by adding the required labels.
  For detailed instructions, see [Prepare for swap and upgrade to
  v26.0](/docs/installation/upgrade-to-swap/).

</div>

</div>

To upgrade your Materialize instances, first choose a new operator
version and upgrade the Materialize operator. Then, upgrade your
Materialize instances to the same version. The following tutorial
upgrades your Materialize deployment running on GCP Google Kubernetes
Engine (GKE).

The tutorial assumes you have installed Materialize on GCP Google
Kubernetes Engine (GKE) using the instructions on [Install on
GCP](/docs/installation/install-on-gcp/) (either from the
examples/simple directory or the root).

## Version compatibility

<div class="important">

**! Important:** When performing major version upgrades, you can upgrade
only one major version at a time. For example, upgrades from **v26**.1.0
to **v27**.2.0 is permitted but **v26**.1.0 to **v28**.0.0 is not.
Skipping major versions or downgrading is not supported. To upgrade from
v25.2 to v26.0, you must [upgrade first to
v25.2.16+](https://materialize.com/docs/self-managed/v25.2/release-notes/#v25216).

</div>

<div class="code-tabs">

<div class="tab-content">

<div id="tab-materialize-on-gcp-terraform-releases" class="tab-pane"
title="Materialize on GCP Terraform Releases">

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Terraform version</th>
<th>Notable changes</th>
</tr>
</thead>
<tbody>
<tr>
<td><a
href="https://github.com/MaterializeInc/terraform-google-materialize/releases/tag/v0.6.4">v0.6.4</a></td>
<td><ul>
<li>Released as part of v26.0.0.</li>
<li>Uses <code>terraform-helm-materialize</code> version
<code>v0.1.35</code>.</li>
</ul></td>
</tr>
</tbody>
</table>

</div>

</div>

</div>

## Prerequisites

<div class="important">

**! Important:** The following procedure performs a rolling upgrade,
where both the old and new Materialize instances are running before the
the old instance are removed. When performing a rolling upgrade, ensure
you have enough resources to support having both the old and new
Materialize instances running.

</div>

### Google cloud project

You need a GCP project for which you have a role (such as
`roles/resourcemanager.projectIamAdmin` or `roles/owner`) that includes
[permissions to manage access to the
project](https://cloud.google.com/iam/docs/granting-changing-revoking-access).

### gcloud CLI

If you do not have gcloud CLI, install. For details, see the [Install
the gcloud CLI
documentation](https://cloud.google.com/sdk/docs/install).

### Google service account

The tutorial assumes the use of a service account. If you do not have a
service account to use for this tutorial, create a service account. For
details, see [Create service
accounts](https://cloud.google.com/iam/docs/service-accounts-create#creating).

### Terraform

If you do not have Terraform installed, [install
Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform).

### kubectl and plugins

<div class="tip">

**💡 Tip:** Using `gcloud` to install `kubectl` will also install the
needed plugins. Otherwise, you will need to manually install the
`gke-gcloud-auth-plugin` for `kubectl`.

</div>

- If you do not have `kubectl`, install `kubectl`. To install, see
  [Install kubectl and configure cluster
  access](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl)
  for details. You will configure `kubectl` to interact with your GKE
  cluster later in the tutorial.

- If you do not have `gke-gcloud-auth-plugin` for `kubectl`, install the
  `gke-gcloud-auth-plugin`. For details, see [Install the
  gke-gcloud-auth-plugin](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#install_plugin).

### Helm 3.2.0+

If you do not have Helm version 3.2.0+ installed, install. For details,
see the [Helm documentation](https://helm.sh/docs/intro/install/).

### jq (Optional)

*Optional*. `jq` is used to parse the EKS cluster name and region from
the Terraform outputs. Alternatively, you can manually specify the name
and region. If you want to use `jq` and do not have `jq` installed,
install.

### License key

Starting in v26.0, Materialize requires a license key. If your existing
deployment does not have a license key configured, contact [Materialize
support](https://materialize.com/docs/support/).

## Procedure

### A. Setup GCP service account and authenticate

1.  Open a Terminal window.

2.  Initialize the gcloud CLI (`gcloud init`) to specify the GCP project
    you want to use. For details, see the [Initializing the gcloud CLI
    documentation](https://cloud.google.com/sdk/docs/initializing#initialize_the).

    <div class="tip">

    **💡 Tip:** You do not need to configure a default Compute Region
    and Zone as you will specify the region.

    </div>

3.  To the service account that will be used to perform the upgrade,
    grant the following IAM roles (if the account does not have them
    already):

    - `roles/editor`
    - `roles/iam.serviceAccountAdmin`
    - `roles/storage.admin`

    1.  Enter your GCP project ID.

        <div class="highlight">

        ``` chroma
        read -s PROJECT_ID
        ```

        </div>

    2.  Find your service account email for your GCP project

        <div class="highlight">

        ``` chroma
        gcloud iam service-accounts list --project $PROJECT_ID
        ```

        </div>

    3.  Enter your service account email.

        <div class="highlight">

        ``` chroma
        read -s SERVICE_ACCOUNT
        ```

        </div>

    4.  Grant the service account the neccessary IAM roles.

        <div class="highlight">

        ``` chroma
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

        </div>

4.  For the service account, authenticate to allow Terraform to interact
    with your GCP project. For details, see [Terraform: Google Cloud
    Provider Configuration
    reference](https://registry.terraform.io/providers/hashicorp/google/latest/docs/guides/provider_reference#authentication).

    For example, if using [User Application Default
    Credentials](https://cloud.google.com/sdk/gcloud/reference/auth/application-default),
    you can run the following command:

    <div class="highlight">

    ``` chroma
    gcloud auth application-default login
    ```

    </div>

    <div class="tip">

    **💡 Tip:** If using `GOOGLE_APPLICATION_CREDENTIALS`, use absolute
    path to your key file.

    </div>

### B. Upgrade Materialize operator and instances

1.  Go to the `examples/simple` folder in the Materialize Terraform repo
    directory.

    <div class="highlight">

    ``` chroma
    cd terraform-google-materialize/examples/simple
    ```

    </div>

2.  Optional. You may need to update your fork of the Terraform module
    to upgrade.

    <div class="tip">

    **💡 Tip:**
    If upgrading from a deployment that was set up using an earlier
    version of the Terraform modules, additional considerations may
    apply when using an updated Terraform modules to your existing
    deployments.

    See [Materialize on GCP
    releases](/docs/installation/appendix-terraforms/#materialize-on-gcp-terraform-module)
    for notable changes.

    </div>

3.  Configure `kubectl` to connect to your EKS cluster, specifying:

    - `<cluster name>`. Your cluster name has the form
      `<your prefix>-gke`; e.g., `mz-simple-gke`.

    - `<region>`. By default, the example Terraform module uses the
      `us-central1` region.

    - `<project>`. Your GCP project ID.

    <div class="highlight">

    ``` chroma
    gcloud container clusters get-credentials <cluster-name>  \
     --region <region> \
     --project <project>
    ```

    </div>

    Alternatively, you can use the following command to get the cluster
    name and region from the Terraform output and the project ID from
    the environment variable set earlier.

    <div class="highlight">

    ``` chroma
    gcloud container clusters get-credentials $(terraform output -json gke_cluster | jq -r .name) \
     --region $(terraform output -json gke_cluster | jq -r .location) --project $PROJECT_ID
    ```

    </div>

    To verify that you have configured correctly, run the following
    command:

    <div class="highlight">

    ``` chroma
    kubectl cluster-info
    ```

    </div>

    For help with `kubectl` commands, see [kubectl Quick
    reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

4.  Back up your `terraform.tfvars` file.

    <div class="highlight">

    ``` chroma
    cp terraform.tfvars original_terraform.tfvars
    ```

    </div>

5.  Update the `terraform.tfvars` to set the Materialize Operator
    version:

    <table>
    <colgroup>
    <col style="width: 50%" />
    <col style="width: 50%" />
    </colgroup>
    <thead>
    <tr>
    <th>Variable</th>
    <th>Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td><code>operator_version</code></td>
    <td>New Materialize Operator version.<br />
    &#10;<ul>
    <li>If the variable does not exist, add the variable and set to the new
    version.</li>
    <li>If the variable exists, update the value to the new version.</li>
    </ul></td>
    </tr>
    </tbody>
    </table>

    <div class="highlight">

    ``` chroma
    ##... Existing content not shown for brevity
    ##... Leave the existing variables unchanged
    operator_version="v26.0.0"  # Set to the desired operator version
    ```

    </div>

6.  Initialize the terraform directory.

    <div class="highlight">

    ``` chroma
    terraform init
    ```

    </div>

7.  Run `terraform plan` with both the `terraform.tfvars` and your
    `mz_instances.tfvars` files and review the changes to be made.

    <div class="highlight">

    ``` chroma
    terraform plan -var-file=terraform.tfvars -var-file=mz_instances.tfvars
    ```

    </div>

    The plan should show the changes to be made for the
    `materialize_operator`.

8.  If you are satisfied with the changes, apply.

    <div class="highlight">

    ``` chroma
    terraform apply -var-file=terraform.tfvars -var-file=mz_instances.tfvars
    ```

    </div>

    To approve the changes and apply, enter `yes`.

    Upon successful completion, you should see output with a summary of
    changes.

9.  Verify that the operator is running:

    <div class="highlight">

    ``` chroma
    kubectl -n materialize get all
    ```

    </div>

    Verify the operator upgrade by checking its events:

    <div class="highlight">

    ``` chroma
    kubectl -n materialize describe pod -l app.kubernetes.io/name=materialize-operator
    ```

    </div>

    - The **Containers** section should show the `--helm-chart-version`
      argument set to the new version.

    - The **Events** section should list that the new version of the
      orchestratord have been pulled.

10. Back up your `mz_instances.tfvars` file.

    <div class="highlight">

    ``` chroma
    cp mz_instances.tfvars original_mz_instances.tfvars
    ```

    </div>

11. Update the `mz_instances.tfvars` to specify the upgrade variables
    for each instance:

    <table>
    <colgroup>
    <col style="width: 50%" />
    <col style="width: 50%" />
    </colgroup>
    <thead>
    <tr>
    <th>Variable</th>
    <th>Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td><code>create_database</code></td>
    <td>Set to <code>false</code>.</td>
    </tr>
    <tr>
    <td><code>environmentd_version</code></td>
    <td>New Materialize instance version. This should be the same as the
    operator version: <code>v26.0.0</code>.</td>
    </tr>
    <tr>
    <td><code>request_rollout</code> or <code>force_rollout</code></td>
    <td>A new UUID string. Can be generated with <code>uuidgen</code>.<br />
    &#10;<ul>
    <li><code>request_rollout</code> triggers a rollout only if changes
    exist.</li>
    <li><code>force_rollout</code> triggers a rollout even if no changes
    exist.</li>
    </ul></td>
    </tr>
    <tr>
    <td><code>inPlaceRollout</code></td>
    <td>Set to <code>false</code> to perform a rolling upgrade. For rolling
    upgrades, ensure you have enough resources to support having both the
    old and new Materialize instances running during the upgrade.</td>
    </tr>
    <tr>
    <td><code>license_key</code></td>
    <td>Required. Set to the value of your license key. If your existing
    deployment does not have a license key, contact <a
    href="https://materialize.com/docs/support/">Materialize support</a>. If
    you have a license key, substitute your license key for
    <code>&lt;ENTER YOUR LICENSE KEY HERE&gt;</code>.</td>
    </tr>
    </tbody>
    </table>

    For example, the following instance specifies:

    - a `create_database` of `false`,
    - an `inPlaceRollout` of `false`,
    - an `environmentd_version` of `"v26.0.0"`,
    - a `request_rollout` of `"22222222-2222-2222-2222-222222222222"`,
      and
    - a `license_key` (substituting your license key for
      `<ENTER YOUR LICENSE KEY HERE>`).

    <div class="highlight">

    ``` chroma
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
          environmentd_version = "v26.0.0"
          inPlaceRollout: false                                   # When false, performs a rolling upgrade rather than in-place
          requestRollout: 22222222-2222-2222-2222-222222222222    # Enter a new UUID
        }
    ]
    ```

    </div>

    <div class="warning">

    **WARNING!** Please consult the Materialize team before setting
    inPlaceRollout to true and performing an in-place rollout. In almost
    all cases a rolling upgrade is preferred.

    </div>

12. Run `terraform plan` with both the `terraform.tfvars` and your
    `mz_instances.tfvars` files and review the changes to be made.

    <div class="highlight">

    ``` chroma
    terraform plan -var-file=terraform.tfvars -var-file=mz_instances.tfvars
    ```

    </div>

    The plan should show the changes to be made for the Materialize
    instance.

13. If you are satisfied with the changes, apply.

    <div class="highlight">

    ``` chroma
    terraform apply -var-file=terraform.tfvars -var-file=mz_instances.tfvars
    ```

    </div>

    To approve the changes and apply, enter `yes`.

    Upon successful completion, you should see output with a summary of
    changes.

14. Verify that the components are running after the upgrade:

    <div class="highlight">

    ``` chroma
    kubectl -n materialize-environment get all
    ```

    </div>

    Verify upgrade by checking the `balancerd` events:

    <div class="highlight">

    ``` chroma
    kubectl -n materialize-environment describe pod -l app=balancerd
    ```

    </div>

    The **Events** section should list that the new version of the
    `balancerd` have been pulled.

    Verify the upgrade by checking the `environmentd` events:

    <div class="highlight">

    ``` chroma
    kubectl -n materialize-environment describe pod -l app=environmentd
    ```

    </div>

    The **Events** section should list that the new version of the
    `environmentd` have been pulled.

15. Open the Materialize Console. The Console should display the new
    version.

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/installation/install-on-gcp/upgrade-on-gcp.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
