---
title: "Upgrade on Azure"
description: "Upgrade Materialize on Azure using the new Terraform module."
menu:
  main:
    parent: "upgrading"
    weight: 25
---

The following tutorial upgrades your Materialize deployment running on Azure
Kubernetes Service (AKS). The tutorial assumes you have installed the
example on [Install on
Azure](/self-managed-deployments/installation/install-on-azure/).

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
- [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)

## Upgrade process

{{< important >}}

The following procedure performs a rolling upgrade, where both the old and new Materialize instances are running before the old instances are removed. When performing a rolling upgrade, ensure you have enough resources to support having both the old and new Materialize instances running.

{{</ important >}}

### Step 1: Update TF module source version

Update each module's `source` to point to the desired release tag, substituting
`<RELEASE_TAG>` in the code block below with your tag version:

{{< important >}}

The following code block is not comprehensive. Only the core modules and their
dependency chain are shown below.

If your configuration includes additional modules (networking, storage,
database, node pools, etc.) provided by Materialize, **update those to the same
release tag as well**.

{{< /important >}}

```hcl
module "aks" {
  source = "github.com/MaterializeInc/materialize-terraform-self-managed//azure/modules/aks?ref=<RELEASE_TAG>"
  # ... your existing configuration ...
}

module "cert_manager" {
  source = "github.com/MaterializeInc/materialize-terraform-self-managed//kubernetes/modules/cert-manager?ref=<RELEASE_TAG>"
  # ... your existing configuration ...

  # Your configuration may have additional dependencies here.
  depends_on = [module.aks]
}

module "operator" {
  source = "github.com/MaterializeInc/materialize-terraform-self-managed//azure/modules/operator?ref=<RELEASE_TAG>"
  # ... your existing configuration ...

  # Your configuration may have additional dependencies here.
  depends_on = [module.cert_manager]
}

module "materialize_instance" {
  source = "github.com/MaterializeInc/materialize-terraform-self-managed//kubernetes/modules/materialize-instance?ref=<RELEASE_TAG>"
  # ... your existing configuration ...

  # Your configuration may have additional dependencies here.
  depends_on = [module.operator]
}

# Update the source of any additional Materialize-provided modules to the same release tag
```

### Step 2: Explicitly request rollout if using v1alpha1

{{< self-managed/crd-version-note "v1alpha1" >}}

{{< include-from-yaml data="self_managed/crd_version_checks"
name="check-crd-version-tf" >}}

- If you are using `v1`, skip to the [Apply the updated TF
  step](#step-3-apply-the-updated-tf).
- {{< include-from-yaml data="self_managed/upgrades"
  name="upgrade-request_rollout" >}}

### Step 3: Apply the updated TF

{{% include-from-yaml data="self_managed/upgrades" name="upgrade-tf-apply" %}}

### Step 4: Verify the upgrade

Configure `kubectl` to connect to your AKS cluster:

```bash
# az aks get-credentials --resource-group <your-resource-group-name> --name <your-aks-cluster-name>
az aks get-credentials --resource-group $(terraform output -raw resource_group_name) --name $(terraform output -raw aks_cluster_name)
```

{{% include-from-yaml data="self_managed/upgrades" name="upgrade-verify-status" %}}

## See also

- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)
- [Materialize CRD Field
  Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/)
- [Troubleshooting](/self-managed-deployments/troubleshooting/)
