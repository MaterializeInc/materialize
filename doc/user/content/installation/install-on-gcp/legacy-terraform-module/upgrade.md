---
title: "Upgrade"
description: "Procedure to upgrade your Materialize operator and instances running on GCP"
menu:
  main:
    parent: "install-on-gcp-legacy-terraform-module"
    identifier: "legacy-terraform-module-upgrade"
    weight: 10
aliases:
  - /installation/install-on-gcp/upgrade-on-gcp
---

{{< annotation type="Disambiguation" >}}

This page is for upgrading from v25.2.13 or later using Terraform. For upgrading
from v25.2.12 or earlier, see:

- For upgrade via Terraform, see {{< include-md
file="shared-content/self-managed/gcp-terraform-v0.6.1-upgrade-notes.md" >}}.

- For upgrade via Helm, see [Upgrade from v25.2.12 or earlier(Non-Terraform)](/installation/install-on-gcp/upgrade-to-swap/).

{{< /annotation >}}

To upgrade your Materialize instances, first choose a new operator version and upgrade the Materialize operator. Then, upgrade your Materialize instances to the same version. The following tutorial upgrades your
Materialize deployment running on GCP Google Kubernetes Engine (GKE).

The tutorial assumes you have installed Materialize on GCP Google Kubernetes
Engine (GKE) using the instructions on [Install on
GCP](/installation/install-on-gcp/) (either from the examples/simple directory
or the root).

## Version compatibility

{{< include-md file="shared-content/self-managed/version-compatibility-upgrade-banner.md" >}}

{{< tabs >}}

{{< tab "Materialize on GCP Terraform Releases" >}}

{{< yaml-table data="self_managed/gcp_terraform_versions" >}}

{{</ tab >}}
{{</ tabs >}}

## Prerequisites

{{< important >}}

The following procedure performs a rolling upgrade, where both the old and new
Materialize instances are running before the the old instance are removed.
When performing a rolling upgrade, ensure you have enough resources to support
having both the old and new Materialize instances running.

{{</ important >}}

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

{{< tip >}}

Using `gcloud` to install `kubectl` will also install the needed plugins.
Otherwise, you will need to manually install the `gke-gcloud-auth-plugin` for
`kubectl`.

{{< /tip >}}

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

{{< include-md file="shared-content/license-key-required.md" >}}

## Procedure

### A. Setup GCP service account and authenticate

1. Open a Terminal window.

1. Initialize the gcloud CLI (`gcloud init`) to specify the GCP project you want
   to use. For details, see the [Initializing the gcloud CLI
   documentation](https://cloud.google.com/sdk/docs/initializing#initialize_the).

   {{< tip >}}
   You do not need to configure a default Compute Region and Zone as you will
   specify the region.
   {{</ tip >}}

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

   {{< tip >}}
   If using `GOOGLE_APPLICATION_CREDENTIALS`, use absolute path to your key file.
   {{</ tip >}}

### B. Upgrade Materialize operator and instances

1. Go to the `examples/simple` folder in the Materialize Terraform repo
   directory.

   ```bash
   cd terraform-google-materialize/examples/simple
   ```

1. Optional. You may need to update your fork of the Terraform module to
   upgrade.

   {{< tip >}}
   {{% self-managed/gcp-terraform-upgrade-notes %}}

   See [Materialize on GCP releases](/installation/appendix-terraforms/#materialize-on-gcp-terraform-module) for notable changes.

   {{</ tip >}}

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

{{% self-managed/versions/upgrade/upgrade-steps-cloud %}}
