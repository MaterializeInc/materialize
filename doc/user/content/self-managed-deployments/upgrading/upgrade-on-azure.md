---
title: "Upgrade on Azure"
description: "Upgrade Materialize on Azure using the Unified Terraform module."
menu:
  main:
    parent: "upgrading"
    weight: 30
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

## Prerequisites

### Required Tools

- [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform)
- [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Helm 3.2.0+](https://helm.sh/docs/intro/install/)

## Upgrade process

{{< important >}}

The following procedure performs a rolling upgrade, where both the old and new Materialize instances are running before the old instances are removed. When performing a rolling upgrade, ensure you have enough resources to support having both the old and new Materialize instances running.

{{</ important >}}

### Step 1: Set up

1. Open a Terminal window.

1. Configure Azure CLI with your Azure credentials. For details, see the [Azure
   documentation](https://learn.microsoft.com/en-us/cli/azure/authenticate-azure-cli).

1. Go to the Terraform directory for your Materialize deployment. For example,
   if you deployed from the `azure/examples/simple` directory:

   ```bash
   cd materialize-terraform-self-managed/azure/examples/simple
   ```

1. Configure `kubectl` to connect to your AKS cluster, replacing:

   - `<your-resource-group>` with the name of your Azure resource group. Your resource group name can be found in the Terraform output or Azure portal.

   - `<your-aks-cluster-name>` with the name of your AKS cluster. Your cluster name can be found in the Terraform output or Azure portal.

   ```bash
   az aks get-credentials --resource-group <your-resource-group> --name <your-aks-cluster-name>
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
