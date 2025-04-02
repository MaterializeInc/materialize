---
title: "Appendix: Required configuration"
description: "Required configuration for Materialize on Azure Terraform."
menu:
  main:
    parent: "install-on-azure"
    identifier: "appendix-azure-config"
    weight: 50
aliases:
  - /installation/install-on-azure/appendix-azure-provider-configuration
---

## Required variables

The following variables are required when using the [Materialize on Azure
Terraform
module](https://github.com/MaterializeInc/terraform-azurerm-materialize).

{{< yaml-table data="self_managed/azure_required_variables" >}}

For a list of all variables, see the
[README.md](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#inputs)
or the [`variables.tf` file](https://github.com/MaterializeInc/terraform-azurerm-materialize/blob/main/variables.tf).

## Required providers and data source declaration

To use [Materialize on Azure Terraform
module](https://github.com/MaterializeInc/terraform-azurerm-materialize)
v0.2.0+, you need to declare the following providers:

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
