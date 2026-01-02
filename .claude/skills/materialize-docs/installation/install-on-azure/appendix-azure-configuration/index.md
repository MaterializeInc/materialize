---
audience: developer
canonical_url: https://materialize.com/docs/installation/install-on-azure/appendix-azure-configuration/
complexity: intermediate
description: Required configuration for Materialize on Azure Terraform.
doc_type: reference
keywords:
- CREATE A
- 'Appendix: Required configuration'
- CREATE IN
- To use an existing resource group
- To create a new resource group
product_area: Deployment
status: stable
title: 'Appendix: Required configuration'
---

# Appendix: Required configuration

## Purpose
Required configuration for Materialize on Azure Terraform.

If you need to understand the syntax and options for this command, you're in the right place.


Required configuration for Materialize on Azure Terraform.



When using the root `main.tf` file from the [Materialize on Azure Terraform
module](https://github.com/MaterializeInc/terraform-azurerm-materialize), the
following configurations are required. [^1]

## Required variables

When using the root `main.tf` file from the [Materialize on Azure Terraform
module](https://github.com/MaterializeInc/terraform-azurerm-materialize), the
following variables must be set: [^1]

<!-- Dynamic table: self_managed/azure_required_variables - see original docs -->

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
  ```text

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

[^1]: If using the `examples/simple/main.tf`, the example configuration handles
them for you.

