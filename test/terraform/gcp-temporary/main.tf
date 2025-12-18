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
      effect = "NO_SCHEDULE"
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

  subnets = [
    {
      name           = "${var.name_prefix}-subnet"
      cidr           = "192.168.0.0/20"
      region         = var.region
      private_access = true
      secondary_ranges = [
        {
          range_name    = "pods"
          ip_cidr_range = "192.168.64.0/18"
        },
        {
          range_name    = "services"
          ip_cidr_range = "192.168.128.0/20"
        }
      ]
    }
  ]

  gke_config = {
    machine_type = "n2-highmem-8"
    disk_size_gb = 100
    min_nodes    = 2
    max_nodes    = 5
  }

  database_config = {
    tier      = "db-custom-2-4096"
    database  = { name = "materialize", charset = "UTF8", collation = "en_US.UTF8" }
    user_name = "materialize"
  }

  local_ssd_count = 1
  swap_enabled    = true

  database_statement_timeout = "15min"

  metadata_backend_url = format(
    "postgres://%s:%s@%s/%s?sslmode=require&options=-c%%20statement_timeout%%3D%s",
    module.database.users[0].name,
    urlencode(module.database.users[0].password),
    module.database.private_ip,
    local.database_config.database.name,
    local.database_statement_timeout
  )

  encoded_endpoint = urlencode("https://storage.googleapis.com")
  encoded_secret   = urlencode(module.storage.hmac_secret)

  persist_backend_url = format(
    "s3://%s:%s@%s/materialize?endpoint=%s&region=%s",
    module.storage.hmac_access_id,
    local.encoded_secret,
    module.storage.bucket_name,
    local.encoded_endpoint,
    var.region
  )
}

# 1. Configure networking infrastructure including VPC, subnets, and CIDR blocks
module "networking" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//gcp/modules/networking?ref=main"

  project_id = var.project_id
  region     = var.region
  prefix     = var.name_prefix
  subnets    = local.subnets
  labels     = var.labels
}

# 2. Set up Google Kubernetes Engine (GKE) cluster
module "gke" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//gcp/modules/gke?ref=main"

  depends_on = [module.networking]

  project_id   = var.project_id
  region       = var.region
  prefix       = var.name_prefix
  network_name = module.networking.network_name
  # we only have one subnet, so we can use the first one
  subnet_name                       = module.networking.subnets_names[0]
  namespace                         = "materialize"
  k8s_apiserver_authorized_networks = var.k8s_apiserver_authorized_networks
  labels                            = var.labels
}

# 2.1 Create generic node pool for system workloads
module "generic_nodepool" {
  source     = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//gcp/modules/nodepool?ref=main"
  depends_on = [module.gke]

  prefix                = "${var.name_prefix}-generic"
  region                = var.region
  enable_private_nodes  = true
  cluster_name          = module.gke.cluster_name
  project_id            = var.project_id
  min_nodes             = 2
  max_nodes             = 5
  machine_type          = "e2-standard-8"
  disk_size_gb          = 50
  service_account_email = module.gke.service_account_email
  labels                = local.generic_node_labels
  swap_enabled          = false
  local_ssd_count       = 0
}

# 2.2 Create Materialize-dedicated node pool with taints
module "materialize_nodepool" {
  source     = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//gcp/modules/nodepool?ref=main"
  depends_on = [module.gke]

  prefix                = "${var.name_prefix}-mz"
  region                = var.region
  enable_private_nodes  = true
  cluster_name          = module.gke.cluster_name
  project_id            = var.project_id
  min_nodes             = local.gke_config.min_nodes
  max_nodes             = local.gke_config.max_nodes
  machine_type          = local.gke_config.machine_type
  disk_size_gb          = local.gke_config.disk_size_gb
  service_account_email = module.gke.service_account_email
  labels                = merge(var.labels, local.materialize_node_labels)
  # Materialize-specific taint to isolate workloads
  node_taints = local.materialize_node_taints

  swap_enabled    = local.swap_enabled
  local_ssd_count = local.local_ssd_count
}

# 3. Set up PostgreSQL database instance for Materialize metadata storage
module "database" {
  source     = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//gcp/modules/database?ref=main"
  depends_on = [module.networking]

  databases = [local.database_config.database]
  # We don't provide password, so random password is generated by the module
  users = [{ name = local.database_config.user_name }]

  project_id = var.project_id
  region     = var.region
  prefix     = var.name_prefix
  network_id = module.networking.network_id

  tier = local.database_config.tier

  labels = var.labels
}

# 4. Create Google Cloud Storage bucket for Materialize persistent data storage
module "storage" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//gcp/modules/storage?ref=main"

  project_id      = var.project_id
  region          = var.region
  prefix          = var.name_prefix
  service_account = module.gke.workload_identity_sa_email
  versioning      = false
  version_ttl     = 7

  labels = var.labels
}

# 5. Install cert-manager for SSL certificate management and create cluster issuer
module "cert_manager" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//kubernetes/modules/cert-manager?ref=main"

  node_selector = local.generic_node_labels

  depends_on = [
    module.gke,
    module.generic_nodepool,
  ]
}

module "self_signed_cluster_issuer" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//kubernetes/modules/self-signed-cluster-issuer?ref=main"

  name_prefix = var.name_prefix

  depends_on = [
    module.cert_manager,
  ]
}

# 6. Install Materialize Kubernetes operator for managing Materialize instances
module "operator" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//gcp/modules/operator?ref=main"

  name_prefix = var.name_prefix
  region      = var.region

  # Use local chart
  use_local_chart       = true
  helm_chart            = "materialize-operator-${var.operator_version}.tgz"
  operator_version      = var.operator_version
  orchestratord_version = var.orchestratord_version

  # tolerations and node selector for all mz instance workloads on GCP
  instance_pod_tolerations = local.materialize_tolerations
  instance_node_selector   = local.materialize_node_labels

  # node selector for operator and metrics-server workloads
  operator_node_selector = local.generic_node_labels

  depends_on = [
    module.gke,
    module.generic_nodepool,
    module.database,
    module.storage,
  ]
}

# 7. Deploy Materialize instance with configured backend connections
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

  authenticator_kind = "None"

  # GCP workload identity annotation for service account
  service_account_annotations = {
    "iam.gke.io/gcp-service-account" = module.gke.workload_identity_sa_email
  }

  license_key = var.license_key

  # TODO: when enabled portforwarding drops with:
  # portforward.go:406] an error occurred forwarding 6877 -> 6877: error forwarding port 6877 to pod
  # : failed to execute portforward in network namespace: writeto tcp4 127.0.0.1:48592->127.0.0.1:6877: read tcp4 127.0.0.1:48592->127.0.0.1:6877: read: connection reset by peer
  # portforward.go:234] lost connection to pod
  # issuer_ref = {
  #   name = module.self_signed_cluster_issuer.issuer_name
  #   kind = "ClusterIssuer"
  # }

  depends_on = [
    module.gke,
    module.database,
    module.storage,
    module.networking,
    module.self_signed_cluster_issuer,
    module.operator,
    module.materialize_nodepool,
  ]
}
