---
title: "Appendix: Required configuration"
description: "Required configuration for Materialize on Azure Terraform."
menu:
  main:
    parent: "install-on-azure-legacy-terraform-module"
    identifier: "appendix-azure-config"
    weight: 50
aliases:
  - /installation/install-on-azure/appendix-azure-provider-configuration
---

When using the root `main.tf` file from the [Materialize on Azure Terraform
module](https://github.com/MaterializeInc/terraform-azurerm-materialize), the
following configurations are required. [^1]

## Required variables

When using the root `main.tf` file from the [Materialize on Azure Terraform
module](https://github.com/MaterializeInc/terraform-azurerm-materialize), the
following variables must be set: [^1]

{{< yaml-table data="self_managed/azure_required_variables" >}}

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
  ```

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
```

## Swap support

Starting in v0.6.1 of Materialize on Azure Terraform,
disk support (using swap on NVMe instance storage) may be enabled for
Materialize. With this change, the Terraform:

- Creates a node group for Materialize.
- Configures NVMe instance store volumes as swap using a daemonset.
- Enables swap at the Kubelet.

For swap support, the following configuration option is available:

- [`swap_enabled`](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#input_swap_enabled)

See [Upgrade Notes](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#v061).

[^1]: If using the `examples/simple/main.tf`, the example configuration handles
them for you.
