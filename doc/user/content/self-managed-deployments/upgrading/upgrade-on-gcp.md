---
title: "Upgrade on GCP"
description: "Upgrade Materialize on GCP using the Unified Terraform module."
menu:
  main:
    parent: "upgrading"
    weight: 30
---

The following tutorial upgrades your Materialize deployment running on Google
Kubernetes Engine (GKE). The tutorial assumes you have installed the
example on [Install on
GCP](/self-managed-deployments/installation/install-on-gcp/).

## Upgrade guidelines

{{% include-from-yaml data="self_managed/upgrades"
name="upgrades-general-rules" %}}

{{< note >}}
{{< include-from-yaml data="self_managed/upgrades"
name="upgrade-major-version-restriction" >}}
{{< /note >}}

{{< note >}}
{{< include-from-yaml data="self_managed/upgrades"
name="downgrade-restriction" >}}
{{< /note >}}

## Prerequisites

### Required Tools

- [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform)
- [Google Cloud CLI](https://cloud.google.com/sdk/docs/install)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Helm 3.2.0+](https://helm.sh/docs/intro/install/)

## Upgrade process

{{< important >}}

The following procedure performs a rolling upgrade, where both the old and new Materialize instances are running before the old instances are removed. When performing a rolling upgrade, ensure you have enough resources to support having both the old and new Materialize instances running.

{{</ important >}}

### Step 1: Set up

1. Open a Terminal window.

1. Configure Google Cloud CLI with your GCP credentials. For details, see the [Google Cloud
   documentation](https://cloud.google.com/sdk/docs/initializing).

1. Go to the Terraform directory for your Materialize deployment. For example,
   if you deployed from the `gcp/examples/simple` directory:

   ```bash
   cd materialize-terraform-self-managed/gcp/examples/simple
   ```

1. Configure `kubectl` to connect to your GKE cluster, replacing:

   - `<your-gke-cluster-name>` with your cluster name; i.e., the
     `gke_cluster_name` in the Terraform output. For the sample example, your
     cluster name has the form `<name_prefix>-gke`; e.g., `simple-demo-gke`

   - `<your-region>` with your cluster location; i.e., the
     `gke_cluster_location` in the Terraform output. Your
     region can also be found in your `terraform.tfvars` file.

   - `<your-project-id>` with your GCP project ID.

   ```bash
   # gcloud container clusters get-credentials <your-gke-cluster-name> --region <your-region> --project <your-project-id>
   gcloud container clusters get-credentials $(terraform output -raw gke_cluster_name) \
    --region $(terraform output -raw gke_cluster_location) \
    --project <your-project-id>
   ```

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl get nodes
   ```

   For help with `kubectl` commands, see [kubectl Quick reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

### Step 2: Update the Helm Chart

{{< important >}}

{{% include-from-yaml data="self_managed/upgrades" name="upgrade-order-rule" %}}

{{</ important >}}

{{% include-from-yaml data="self_managed/upgrades"
name="upgrade-update-helm-chart" %}}

### Step 3: Upgrade the Materialize Operator

{{< important >}}

{{% include-from-yaml data="self_managed/upgrades" name="upgrade-order-rule" %}}

{{</ important >}}

{{% include-from-yaml data="self_managed/upgrades"
name="upgrade-materialize-operator" %}}

### Step 4: Upgrading Materialize Instances

{{< important >}}

{{% include-from-yaml data="self_managed/upgrades" name="upgrade-order-rule" %}}

{{</ important >}}

{{% include-from-yaml data="self_managed/upgrades"
name="upgrade-materialize-instance" %}}

## See also

- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)
- [Materialize CRD Field
  Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/)
- [Troubleshooting](/self-managed-deployments/troubleshooting/)
