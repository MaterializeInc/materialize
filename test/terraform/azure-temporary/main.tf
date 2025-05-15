# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

provider "azurerm" {
  subscription_id = "9bc1ad3f-3401-42a3-99cd-7faeeb51e059"

  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
    key_vault {
      purge_soft_delete_on_destroy    = true
      recover_soft_deleted_key_vaults = false
    }

  }
}

provider "kubernetes" {
  host                   = module.materialize.kube_config[0].host
  client_certificate     = base64decode(module.materialize.kube_config[0].client_certificate)
  client_key             = base64decode(module.materialize.kube_config[0].client_key)
  cluster_ca_certificate = base64decode(module.materialize.kube_config[0].cluster_ca_certificate)
}

provider "helm" {
  kubernetes {
    host                   = module.materialize.kube_config[0].host
    client_certificate     = base64decode(module.materialize.kube_config[0].client_certificate)
    client_key             = base64decode(module.materialize.kube_config[0].client_key)
    cluster_ca_certificate = base64decode(module.materialize.kube_config[0].cluster_ca_certificate)
  }
}

resource "random_password" "pass" {
  length  = 20
  special = false
}

variable "operator_version" {
  type    = string
  default = "v25.2.0-beta.1.tgz"
}

variable "orchestratord_version" {
  type    = string
  default = null
}

resource "azurerm_resource_group" "materialize" {
  name     = "mz-tf-test-rg"
  location = "eastus2"
  tags     = {}
}

module "materialize" {
  source = "git::https://github.com/MaterializeInc/terraform-azurerm-materialize.git?ref=v0.4.2"
  resource_group_name = azurerm_resource_group.materialize.name
  location            = "eastus2"
  prefix              = "mz-tf-test"

  install_materialize_operator = true
  use_local_chart = true
  helm_chart = "materialize-operator-v25.2.0-beta.1.tgz"
  operator_version = var.operator_version
  orchestratord_version = var.orchestratord_version

  install_cert_manager = false
  use_self_signed_cluster_issuer = false

  materialize_instances = var.materialize_instances

  database_config = {
    sku_name = "GP_Standard_D2s_v3"
    version  = "15"
    password = random_password.pass.result
  }

  network_config = {
    vnet_address_space   = "10.0.0.0/16"
    subnet_cidr          = "10.0.0.0/20"
    postgres_subnet_cidr = "10.0.16.0/24"
    service_cidr         = "10.1.0.0/16"
    docker_bridge_cidr   = "172.17.0.1/16"
  }

  tags = {
    environment = "dev"
    managed_by  = "terraform"
  }

  providers = {
    azurerm    = azurerm
    kubernetes = kubernetes
    helm       = helm
  }
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus2"
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}

variable "materialize_instances" {
  description = "Configuration for Materialize instances"
  type = list(object({
    name             = string
    namespace        = optional(string)
    database_name    = string
    cpu_request      = optional(string, "1")
    memory_request   = optional(string, "1Gi")
    memory_limit     = optional(string, "1Gi")
    create_database  = optional(bool, true)
    in_place_rollout = optional(bool, false)
    request_rollout  = optional(string)
    force_rollout    = optional(string)
  }))
  default = []
}

# Output the Materialize instance details
output "aks_cluster" {
  description = "AKS cluster details"
  value       = module.materialize.aks_cluster
  sensitive   = true
}

output "connection_strings" {
  description = "Connection strings for Materialize"
  value       = module.materialize.connection_strings
  sensitive   = true
}

output "kube_config" {
  description = "The kube_config for the AKS cluster"
  value       = module.materialize.kube_config
  sensitive   = true
}

output "resource_group_name" {
  value = azurerm_resource_group.materialize.name
}
