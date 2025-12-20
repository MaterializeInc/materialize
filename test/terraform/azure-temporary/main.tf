# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Locals
locals {
  materialize_instance_namespace = "materialize-environment"
  materialize_instance_name      = "main"

  vnet_config = {
    address_space                      = "20.0.0.0/16"
    aks_subnet_cidr                    = "20.0.0.0/20"
    postgres_subnet_cidr               = "20.0.16.0/24"
    enable_api_server_vnet_integration = true
    api_server_subnet_cidr             = "20.0.32.0/27"
  }

  aks_config = {
    kubernetes_version         = "1.32"
    service_cidr               = "20.1.0.0/16"
    enable_azure_monitor       = false
    log_analytics_workspace_id = null
  }

  node_pool_config = {
    vm_size              = "Standard_E4pds_v6"
    auto_scaling_enabled = true
    min_nodes            = 2
    max_nodes            = 5
    node_count           = null
    disk_size_gb         = 100
    swap_enabled         = true
  }

  database_config = {
    sku_name                      = "GP_Standard_D2s_v3"
    postgres_version              = "15"
    storage_mb                    = 32768
    backup_retention_days         = 7
    administrator_login           = "materialize"
    database_name                 = "materialize"
    public_network_access_enabled = false
  }

  storage_container_name = "materialize"

  database_statement_timeout = "15min"

  metadata_backend_url = format(
    "postgres://%s:%s@%s/%s?sslmode=require&options=-c%%20statement_timeout%%3D%s",
    module.database.administrator_login,
    urlencode(module.database.administrator_password),
    module.database.server_fqdn,
    local.database_config.database_name,
    local.database_statement_timeout
  )

  persist_backend_url = format(
    "%s%s",
    module.storage.primary_blob_endpoint,
    module.storage.container_name,
  )

  # Common node scheduling configuration
  generic_node_labels = {
    "workload" = "generic"
  }

  materialize_node_labels = {
    "workload" = "materialize-instance"
  }

  materialize_node_taints = [
    {
      key    = "materialize.cloud/workload"
      value  = "materialize-instance"
      effect = "NoSchedule"
    }
  ]

  materialize_tolerations = [
    {
      key      = "materialize.cloud/workload"
      value    = "materialize-instance"
      operator = "Equal"
      effect   = "NoSchedule"
    }
  ]
}

# 1. Create resource group
resource "azurerm_resource_group" "materialize" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

# 2. Create networking infrastructure
module "networking" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//azure/modules/networking?ref=main"

  resource_group_name                = azurerm_resource_group.materialize.name
  location                           = var.location
  prefix                             = var.name_prefix
  vnet_address_space                 = local.vnet_config.address_space
  aks_subnet_cidr                    = local.vnet_config.aks_subnet_cidr
  postgres_subnet_cidr               = local.vnet_config.postgres_subnet_cidr
  enable_api_server_vnet_integration = local.vnet_config.enable_api_server_vnet_integration
  api_server_subnet_cidr             = local.vnet_config.api_server_subnet_cidr

  tags = var.tags

  depends_on = [azurerm_resource_group.materialize]
}

# 3. Create AKS cluster with default node pool
module "aks" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//azure/modules/aks?ref=main"

  resource_group_name = azurerm_resource_group.materialize.name
  kubernetes_version  = local.aks_config.kubernetes_version
  service_cidr        = local.aks_config.service_cidr
  location            = var.location
  prefix              = var.name_prefix
  vnet_name           = module.networking.vnet_name
  subnet_name         = module.networking.aks_subnet_name
  subnet_id           = module.networking.aks_subnet_id

  enable_api_server_vnet_integration = local.vnet_config.enable_api_server_vnet_integration
  k8s_apiserver_authorized_networks  = concat(var.k8s_apiserver_authorized_networks, ["${module.networking.nat_gateway_public_ip}/32"])
  api_server_subnet_id               = module.networking.api_server_subnet_id

  # Default node pool with autoscaling (runs all workloads except Materialize)
  default_node_pool_vm_size             = "Standard_D4pds_v6"
  default_node_pool_enable_auto_scaling = true
  default_node_pool_min_count           = 2
  default_node_pool_max_count           = 5
  default_node_pool_node_labels         = local.generic_node_labels

  # Optional: Enable monitoring
  enable_azure_monitor       = local.aks_config.enable_azure_monitor
  log_analytics_workspace_id = local.aks_config.log_analytics_workspace_id

  tags = var.tags

  depends_on = [azurerm_resource_group.materialize]
}

# 3.1 Create Materialize-dedicated node pool with taints
module "materialize_nodepool" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//azure/modules/nodepool?ref=main"

  prefix     = var.name_prefix
  cluster_id = module.aks.cluster_id
  subnet_id  = module.networking.aks_subnet_id

  # Workload-specific configuration
  autoscaling_config = {
    enabled    = local.node_pool_config.auto_scaling_enabled
    min_nodes  = local.node_pool_config.min_nodes
    max_nodes  = local.node_pool_config.max_nodes
    node_count = local.node_pool_config.node_count
  }

  vm_size      = local.node_pool_config.vm_size
  disk_size_gb = local.node_pool_config.disk_size_gb
  swap_enabled = local.node_pool_config.swap_enabled

  labels = local.materialize_node_labels

  # Materialize-specific taint to isolate workloads
  node_taints = local.materialize_node_taints

  tags = var.tags

  depends_on = [azurerm_resource_group.materialize]
}

# 4. Create PostgreSQL database
module "database" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//azure/modules/database?ref=main"

  depends_on = [module.networking]

  # Database configuration
  databases = [
    {
      name      = local.database_config.database_name
      charset   = "UTF8"
      collation = "en_US.utf8"
    }
  ]

  # Administrator configuration
  administrator_login = local.database_config.administrator_login

  # Infrastructure configuration
  resource_group_name = azurerm_resource_group.materialize.name
  location            = var.location
  prefix              = var.name_prefix
  subnet_id           = module.networking.postgres_subnet_id
  private_dns_zone_id = module.networking.private_dns_zone_id

  # Database server configuration
  sku_name                      = local.database_config.sku_name
  postgres_version              = local.database_config.postgres_version
  storage_mb                    = local.database_config.storage_mb
  backup_retention_days         = local.database_config.backup_retention_days
  public_network_access_enabled = local.database_config.public_network_access_enabled

  tags = var.tags
}

# 5. Create Azure Blob Storage
module "storage" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//azure/modules/storage?ref=main"

  resource_group_name            = azurerm_resource_group.materialize.name
  location                       = var.location
  prefix                         = var.name_prefix
  workload_identity_principal_id = module.aks.workload_identity_principal_id
  subnets                        = [module.networking.aks_subnet_id]
  container_name                 = local.storage_container_name

  # Workload identity federation configuration
  workload_identity_id      = module.aks.workload_identity_id
  oidc_issuer_url           = module.aks.cluster_oidc_issuer_url
  service_account_namespace = local.materialize_instance_namespace
  service_account_name      = local.materialize_instance_name

  storage_account_tags = var.tags

  depends_on = [azurerm_resource_group.materialize]
}

# 6. Install cert-manager for TLS
module "cert_manager" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//kubernetes/modules/cert-manager?ref=main"

  node_selector = local.generic_node_labels

  depends_on = [
    module.aks,
  ]
}

module "self_signed_cluster_issuer" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//kubernetes/modules/self-signed-cluster-issuer?ref=main"

  name_prefix = var.name_prefix

  depends_on = [
    module.cert_manager,
  ]
}

# 7. Install Materialize Operator
module "operator" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//azure/modules/operator?ref=main"

  name_prefix = var.name_prefix
  location    = var.location

  # Use local chart
  use_local_chart       = true
  helm_chart            = "materialize-operator-${var.operator_version}.tgz"
  operator_version      = var.operator_version
  orchestratord_version = var.orchestratord_version

  # Helm values for operator configuration
  helm_values = {
    operator = {
      args = {
        enableLicenseKeyChecks = true
      }
    }
  }

  # tolerations and node selector for all mz instance workloads
  instance_pod_tolerations = local.materialize_tolerations
  instance_node_selector   = local.materialize_node_labels

  # node selector for operator and metrics-server workloads
  operator_node_selector = local.generic_node_labels

  depends_on = [
    module.aks,
    module.database,
    module.storage,
  ]
}

# 8. Deploy Materialize instance
module "materialize_instance" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//kubernetes/modules/materialize-instance?ref=main"

  instance_name        = local.materialize_instance_name
  instance_namespace   = local.materialize_instance_namespace
  metadata_backend_url = local.metadata_backend_url
  persist_backend_url  = local.persist_backend_url

  environmentd_version = var.environmentd_version

  # Rollout configuration
  force_rollout   = var.force_rollout
  request_rollout = var.request_rollout

  # No authentication for testing
  authenticator_kind = "None"

  # Azure workload identity annotations for service account
  service_account_annotations = {
    "azure.workload.identity/client-id" = module.aks.workload_identity_client_id
  }
  pod_labels = {
    "azure.workload.identity/use" = "true"
  }

  license_key = var.license_key

  issuer_ref = {
    name = module.self_signed_cluster_issuer.issuer_name
    kind = "ClusterIssuer"
  }

  depends_on = [
    module.aks,
    module.database,
    module.storage,
    module.networking,
    module.self_signed_cluster_issuer,
    module.operator,
    module.materialize_nodepool,
  ]
}
