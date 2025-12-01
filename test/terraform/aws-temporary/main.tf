# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Random password for database
resource "random_password" "db_password" {
  length  = 32
  special = false
}

# 1. Create network infrastructure
module "networking" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//aws/modules/networking?ref=main"

  name_prefix          = var.name_prefix
  vpc_cidr             = "10.0.0.0/16"
  availability_zones   = ["us-east-1a", "us-east-1b"]
  private_subnet_cidrs = ["10.0.1.0/24", "10.0.2.0/24"]
  public_subnet_cidrs  = ["10.0.101.0/24", "10.0.102.0/24"]
  enable_vpc_endpoints = true
  tags                 = var.tags
}

# 2. Create EKS cluster
module "eks" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//aws/modules/eks?ref=main"

  name_prefix                              = var.name_prefix
  cluster_version                          = "1.32"
  vpc_id                                   = module.networking.vpc_id
  private_subnet_ids                       = module.networking.private_subnet_ids
  cluster_enabled_log_types                = ["api", "audit"]
  enable_cluster_creator_admin_permissions = true
  materialize_node_ingress_cidrs           = [module.networking.vpc_cidr_block]
  k8s_apiserver_authorized_networks        = var.k8s_apiserver_authorized_networks
  tags                                     = var.tags

  depends_on = [
    module.networking,
  ]
}

# 2.1 Create base node group for system workloads and Karpenter
module "base_node_group" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//aws/modules/eks-node-group?ref=main"

  cluster_name                      = module.eks.cluster_name
  subnet_ids                        = module.networking.private_subnet_ids
  node_group_name                   = "${var.name_prefix}-base"
  instance_types                    = local.instance_types_base
  swap_enabled                      = false
  min_size                          = 2
  max_size                          = 3
  desired_size                      = 2
  labels                            = local.base_node_labels
  cluster_service_cidr              = module.eks.cluster_service_cidr
  cluster_primary_security_group_id = module.eks.node_security_group_id
  tags                              = var.tags

  depends_on = [
    module.eks,
  ]
}

# 2.2 Install Karpenter to manage creation of additional nodes
module "karpenter" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//aws/modules/karpenter?ref=main"

  name_prefix             = var.name_prefix
  cluster_name            = module.eks.cluster_name
  cluster_endpoint        = module.eks.cluster_endpoint
  oidc_provider_arn       = module.eks.oidc_provider_arn
  cluster_oidc_issuer_url = module.eks.cluster_oidc_issuer_url
  node_selector           = local.base_node_labels

  helm_repo_username = data.aws_ecrpublic_authorization_token.token.user_name
  helm_repo_password = data.aws_ecrpublic_authorization_token.token.password

  depends_on = [
    module.eks,
    module.base_node_group,
    module.networking,
  ]
}

# Create a generic nodeclass and nodepool for system workloads
module "ec2nodeclass_generic" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//aws/modules/karpenter-ec2nodeclass?ref=main"

  name               = local.nodeclass_name_generic
  ami_selector_terms = local.ami_selector_terms
  instance_types     = local.instance_types_generic
  instance_profile   = module.karpenter.node_instance_profile
  security_group_ids = [module.eks.node_security_group_id]
  subnet_ids         = module.networking.private_subnet_ids
  swap_enabled       = false
  tags               = var.tags

  depends_on = [
    module.eks,
    module.karpenter,
  ]
}

module "nodepool_generic" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//aws/modules/karpenter-nodepool?ref=main"

  name            = local.nodeclass_name_generic
  nodeclass_name  = local.nodeclass_name_generic
  instance_types  = local.instance_types_generic
  node_labels     = local.generic_node_labels
  expire_after    = "168h"
  kubeconfig_data = local.kubeconfig_data

  depends_on = [
    module.karpenter,
    module.ec2nodeclass_generic,
  ]
}

# Create a dedicated nodeclass and nodepool for Materialize pods
module "ec2nodeclass_materialize" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//aws/modules/karpenter-ec2nodeclass?ref=main"

  name               = local.nodeclass_name_materialize
  ami_selector_terms = local.ami_selector_terms
  instance_types     = local.instance_types_materialize
  instance_profile   = module.karpenter.node_instance_profile
  security_group_ids = [module.eks.node_security_group_id]
  subnet_ids         = module.networking.private_subnet_ids
  swap_enabled       = true
  tags               = var.tags

  depends_on = [
    module.karpenter,
  ]
}

module "nodepool_materialize" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//aws/modules/karpenter-nodepool?ref=main"

  name            = local.nodeclass_name_materialize
  nodeclass_name  = local.nodeclass_name_materialize
  instance_types  = local.instance_types_materialize
  node_labels     = local.materialize_node_labels
  node_taints     = local.materialize_node_taints
  expire_after    = "Never"
  kubeconfig_data = local.kubeconfig_data

  depends_on = [
    module.karpenter,
    module.ec2nodeclass_materialize,
  ]
}

# 3. Install AWS Load Balancer Controller
module "aws_lbc" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//aws/modules/aws-lbc?ref=main"

  name_prefix       = var.name_prefix
  eks_cluster_name  = module.eks.cluster_name
  oidc_provider_arn = module.eks.oidc_provider_arn
  oidc_issuer_url   = module.eks.cluster_oidc_issuer_url
  vpc_id            = module.networking.vpc_id
  region            = var.aws_region
  node_selector     = local.generic_node_labels
  tags              = var.tags

  depends_on = [
    module.eks,
    module.nodepool_generic,
  ]
}

# 4. Install Certificate Manager for TLS
module "cert_manager" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//kubernetes/modules/cert-manager?ref=main"

  node_selector = local.generic_node_labels

  depends_on = [
    module.networking,
    module.eks,
    module.nodepool_generic,
    module.aws_lbc,
  ]
}

module "self_signed_cluster_issuer" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//kubernetes/modules/self-signed-cluster-issuer?ref=main"

  name_prefix = var.name_prefix

  depends_on = [
    module.cert_manager,
  ]
}

# 5. Setup dedicated database instance for Materialize
module "database" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//aws/modules/database?ref=main"

  name_prefix               = var.name_prefix
  postgres_version          = "15"
  instance_class            = "db.t3.micro"
  allocated_storage         = 20
  max_allocated_storage     = 100
  database_name             = "materialize"
  database_username         = "materialize"
  database_password         = random_password.db_password.result
  multi_az                  = false
  database_subnet_ids       = module.networking.private_subnet_ids
  vpc_id                    = module.networking.vpc_id
  cluster_name              = module.eks.cluster_name
  cluster_security_group_id = module.eks.cluster_security_group_id
  node_security_group_id    = module.eks.node_security_group_id
  tags                      = var.tags

  depends_on = [
    module.eks,
    module.networking,
  ]
}

# 6. Setup S3 bucket for Materialize
module "storage" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//aws/modules/storage?ref=main"

  name_prefix            = var.name_prefix
  bucket_lifecycle_rules = []
  bucket_force_destroy   = true

  # For testing purposes, we are disabling encryption and versioning to allow for easier cleanup
  # This should be enabled in production environments for security and data integrity
  enable_bucket_versioning = false
  enable_bucket_encryption = false

  # IRSA configuration
  oidc_provider_arn         = module.eks.oidc_provider_arn
  cluster_oidc_issuer_url   = module.eks.cluster_oidc_issuer_url
  service_account_namespace = local.materialize_instance_namespace
  service_account_name      = local.materialize_instance_name
  tags                      = var.tags

  depends_on = [
    module.eks,
  ]
}

# 7. Install Materialize Operator
module "operator" {
  source = "git::https://github.com/MaterializeInc/materialize-terraform-self-managed.git//aws/modules/operator?ref=main"

  name_prefix    = var.name_prefix
  aws_region     = var.aws_region
  aws_account_id = data.aws_caller_identity.current.account_id

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

  # tolerations and node selector for all mz instance workloads on AWS
  instance_pod_tolerations = local.materialize_tolerations
  instance_node_selector   = local.materialize_node_labels

  # node selector for operator and metrics-server workloads
  operator_node_selector = local.generic_node_labels

  depends_on = [
    module.eks,
    module.networking,
    module.nodepool_generic,
  ]
}

# 8. Setup Materialize instance
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

  # AWS IAM role annotation for service account
  service_account_annotations = {
    "eks.amazonaws.com/role-arn" = module.storage.materialize_s3_role_arn
  }

  license_key = var.license_key

  issuer_ref = {
    name = module.self_signed_cluster_issuer.issuer_name
    kind = "ClusterIssuer"
  }

  depends_on = [
    module.eks,
    module.database,
    module.storage,
    module.networking,
    module.self_signed_cluster_issuer,
    module.operator,
    module.aws_lbc,
    module.nodepool_materialize,
  ]
}

# Locals
locals {
  materialize_instance_namespace = "materialize-environment"
  materialize_instance_name      = "main"

  # Common node scheduling configuration
  base_node_labels = {
    "workload" = "base"
  }

  generic_node_labels = {
    "workload" = "generic"
  }

  materialize_node_labels = {
    "materialize.cloud/swap" = "true"
    "workload"               = "materialize-instance"
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

  database_statement_timeout = "15min"

  metadata_backend_url = format(
    "postgres://%s:%s@%s/%s?sslmode=require&options=-c%%20statement_timeout%%3D%s",
    module.database.db_instance_username,
    urlencode(random_password.db_password.result),
    module.database.db_instance_endpoint,
    module.database.db_instance_name,
    local.database_statement_timeout
  )

  persist_backend_url = format(
    "s3://%s/system:serviceaccount:%s:%s",
    module.storage.bucket_name,
    local.materialize_instance_namespace,
    local.materialize_instance_name
  )

  ami_selector_terms = [{ "alias" : "bottlerocket@latest" }]

  instance_types_base        = ["t4g.medium"]
  instance_types_generic     = ["t4g.xlarge"]
  instance_types_materialize = ["r7gd.2xlarge"]

  nodeclass_name_generic     = "generic"
  nodeclass_name_materialize = "materialize"

  kubeconfig_data = jsonencode({
    "apiVersion" : "v1",
    "kind" : "Config",
    "clusters" : [
      {
        "name" : module.eks.cluster_name,
        "cluster" : {
          "certificate-authority-data" : module.eks.cluster_certificate_authority_data,
          "server" : module.eks.cluster_endpoint,
        },
      },
    ],
    "contexts" : [
      {
        "name" : module.eks.cluster_name,
        "context" : {
          "cluster" : module.eks.cluster_name,
          "user" : module.eks.cluster_name,
        },
      },
    ],
    "current-context" : module.eks.cluster_name,
    "users" : [
      {
        "name" : module.eks.cluster_name,
        "user" : {
          "exec" : {
            "apiVersion" : "client.authentication.k8s.io/v1beta1",
            "command" : "aws",
            "args" : [
              "eks",
              "get-token",
              "--cluster-name",
              module.eks.cluster_name,
              "--region",
              var.aws_region,
            ]
          }
        },
      },
    ],
  })
}
