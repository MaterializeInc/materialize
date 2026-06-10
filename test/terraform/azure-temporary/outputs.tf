# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Networking outputs
output "networking" {
  description = "Networking details"
  value = {
    vnet_id               = module.networking.vnet_id
    vnet_name             = module.networking.vnet_name
    aks_subnet_id         = module.networking.aks_subnet_id
    api_server_subnet_id  = module.networking.api_server_subnet_id
    postgres_subnet_id    = module.networking.postgres_subnet_id
    private_dns_zone_id   = module.networking.private_dns_zone_id
    nat_gateway_id        = module.networking.nat_gateway_id
    nat_gateway_public_ip = module.networking.nat_gateway_public_ip
    vnet_address_space    = module.networking.vnet_address_space
  }
}

# Cluster outputs
output "aks_cluster_name" {
  description = "The name of the AKS cluster"
  value       = module.aks.cluster_name
}

output "aks_cluster_id" {
  description = "The ID of the AKS cluster"
  value       = module.aks.cluster_id
}

output "aks_cluster_endpoint" {
  description = "The endpoint of the AKS cluster"
  value       = module.aks.cluster_endpoint
  sensitive   = true
}

output "aks_kube_config" {
  description = "The kube config of the AKS cluster"
  value       = module.aks.kube_config
  sensitive   = true
}

output "aks_oidc_issuer_url" {
  description = "The OIDC issuer URL of the AKS cluster"
  value       = module.aks.cluster_oidc_issuer_url
}

output "aks_workload_identity_client_id" {
  description = "The client ID of the workload identity"
  value       = module.aks.workload_identity_client_id
}

output "materialize_nodepool_name" {
  description = "The name of the Materialize node pool"
  value       = module.materialize_nodepool.nodepool_name
}

output "materialize_nodepool_id" {
  description = "The ID of the Materialize node pool"
  value       = module.materialize_nodepool.nodepool_id
}

# Database outputs
output "database_endpoint" {
  description = "PostgreSQL server endpoint"
  value       = module.database.server_fqdn
}

output "database_name" {
  description = "PostgreSQL server name"
  value       = module.database.server_name
}

output "database_username" {
  description = "PostgreSQL administrator username"
  value       = module.database.administrator_login
  sensitive   = true
}

# Storage outputs
output "storage_account_name" {
  description = "Name of the storage account"
  value       = module.storage.storage_account_name
}

output "storage_primary_blob_endpoint" {
  description = "Primary blob endpoint of the storage account"
  value       = module.storage.primary_blob_endpoint
}

output "storage_container_name" {
  description = "Name of the storage container"
  value       = module.storage.container_name
}

# Materialize component outputs
output "operator" {
  description = "Materialize operator details"
  value = {
    namespace      = module.operator.operator_namespace
    release_name   = module.operator.operator_release_name
    release_status = module.operator.operator_release_status
  }
}

output "materialize_instance_name" {
  description = "Materialize instance name"
  value       = module.materialize_instance.instance_name
}

output "materialize_instance_namespace" {
  description = "Materialize instance namespace"
  value       = module.materialize_instance.instance_namespace
}

# Azure-specific outputs
output "resource_group_name" {
  description = "The name of the resource group"
  value       = azurerm_resource_group.materialize.name
}

# Output for mzcompose compatibility (matches expected format)
output "aks_cluster" {
  description = "AKS cluster details for mzcompose"
  value = {
    name     = module.aks.cluster_name
    endpoint = module.aks.cluster_endpoint
  }
  sensitive = true
}

output "connection_strings" {
  description = "Connection strings for mzcompose"
  value = {
    metadata_backend_url = local.metadata_backend_url
    persist_backend_url  = local.persist_backend_url
  }
  sensitive = true
}

output "metadata_backend_url" {
  description = "PostgreSQL connection URL in the format required by Materialize"
  value       = local.metadata_backend_url
  sensitive   = true
}

output "persist_backend_url" {
  description = "Azure Blob Storage URL in the format required by Materialize"
  value       = local.persist_backend_url
}
