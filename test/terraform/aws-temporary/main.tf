# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

provider "aws" {
  region = "us-east-1"
}

provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)

  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
  }
}

provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)

    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
    }
  }
}

variable "name_prefix" {
  type    = string
  default = "aws-test-dev"
}

variable "operator_version" {
  type    = string
  default = "v25.2.0-beta.1.tgz"
}

variable "orchestratord_version" {
  type    = string
  default = null
}

resource "random_password" "db_password" {
  length  = 32
  special = false
}

data "aws_caller_identity" "current" {}

# 1. Create network infrastructure
module "networking" {
  source = "git::https://github.com/MaterializeInc/terraform-aws-materialize.git//modules/networking?ref=prod-ready-refactor"

  name_prefix = var.name_prefix

  vpc_cidr             = "10.0.0.0/16"
  availability_zones   = ["us-east-1a", "us-east-1b"]
  private_subnet_cidrs = ["10.0.1.0/24", "10.0.2.0/24"]
  public_subnet_cidrs  = ["10.0.101.0/24", "10.0.102.0/24"]
  single_nat_gateway   = true
}

# 2. Create EKS cluster
module "eks" {
  source = "git::https://github.com/MaterializeInc/terraform-aws-materialize.git//modules/eks?ref=prod-ready-refactor"

  name_prefix                              = var.name_prefix
  cluster_version                          = "1.32"
  vpc_id                                   = module.networking.vpc_id
  private_subnet_ids                       = module.networking.private_subnet_ids
  cluster_enabled_log_types                = ["api", "audit"]
  enable_cluster_creator_admin_permissions = true
  tags = {
    Environment = "dev"
    Project     = "aws-test"
    Terraform   = "true"
  }
}

# 2.1. Create EKS node group
module "eks_node_group" {
  source = "git::https://github.com/MaterializeInc/terraform-aws-materialize.git//modules/eks-node-group?ref=prod-ready-refactor"

  cluster_name                      = module.eks.cluster_name
  subnet_ids                        = module.networking.private_subnet_ids
  node_group_name                   = "${var.name_prefix}-mz"
  instance_types                    = ["r7gd.2xlarge"]
  desired_size                      = 2
  min_size                          = 1
  max_size                          = 3
  capacity_type                     = "ON_DEMAND"
  enable_disk_setup                 = true
  cluster_service_cidr              = module.eks.cluster_service_cidr
  cluster_primary_security_group_id = module.eks.node_security_group_id

  labels = {
    GithubRepo               = "materialize"
    "materialize.cloud/disk" = "true"
    "workload"               = "materialize-instance"
  }
}

# 3. Install AWS Load Balancer Controller
module "aws_lbc" {
  source = "git::https://github.com/MaterializeInc/terraform-aws-materialize.git//modules/aws-lbc?ref=prod-ready-refactor"

  name_prefix       = var.name_prefix
  eks_cluster_name  = module.eks.cluster_name
  oidc_provider_arn = module.eks.oidc_provider_arn
  oidc_issuer_url   = module.eks.cluster_oidc_issuer_url
  vpc_id            = module.networking.vpc_id
  region            = "us-east-1"

  depends_on = [
    module.eks,
    module.eks_node_group,
  ]
}

# 4. Install OpenEBS for storage
module "openebs" {
  source = "git::https://github.com/MaterializeInc/terraform-aws-materialize.git//modules/openebs?ref=prod-ready-refactor"

  openebs_namespace = "openebs"
  openebs_version   = "4.2.0"

  depends_on = [
    module.networking,
    module.eks,
    module.eks_node_group,
    module.aws_lbc,
  ]
}

# 5. Install Certificate Manager for TLS
module "certificates" {
  source = "git::https://github.com/MaterializeInc/terraform-aws-materialize.git//modules/certificates?ref=prod-ready-refactor"

  install_cert_manager           = false
  cert_manager_install_timeout   = 300
  cert_manager_chart_version     = "v1.13.3"
  use_self_signed_cluster_issuer = false
  cert_manager_namespace         = "cert-manager"
  name_prefix                    = var.name_prefix

  depends_on = [
    module.networking,
    module.eks,
    module.eks_node_group,
    module.aws_lbc,
  ]
}

# 6. Install Materialize Operator
module "operator" {
  source = "git::https://github.com/MaterializeInc/terraform-aws-materialize.git//modules/operator?ref=prod-ready-refactor"

  name_prefix                    = var.name_prefix
  aws_region                     = "us-east-1"
  aws_account_id                 = data.aws_caller_identity.current.account_id
  oidc_provider_arn              = module.eks.oidc_provider_arn
  cluster_oidc_issuer_url        = module.eks.cluster_oidc_issuer_url
  s3_bucket_arn                  = module.storage.bucket_arn
  use_self_signed_cluster_issuer = false

  # Use local chart
  use_local_chart       = true
  helm_chart            = "materialize-operator-v25.2.0-beta.1.tgz"
  operator_version      = var.operator_version
  orchestratord_version = var.orchestratord_version

  depends_on = [
    module.eks,
    module.networking,
    module.eks_node_group,
  ]
}

# 7. Setup dedicated database instance for Materialize
module "database" {
  source = "git::https://github.com/MaterializeInc/terraform-aws-materialize.git//modules/database?ref=prod-ready-refactor"

  name_prefix                = var.name_prefix
  postgres_version           = "15"
  instance_class             = "db.t3.micro"
  allocated_storage          = 20
  max_allocated_storage      = 50
  database_name              = "materialize"
  database_username          = "materialize"
  database_password          = random_password.db_password.result
  multi_az                   = false
  database_subnet_ids        = module.networking.private_subnet_ids
  vpc_id                     = module.networking.vpc_id
  eks_security_group_id      = module.eks.cluster_security_group_id
  eks_node_security_group_id = module.eks.node_security_group_id
  tags = {
    Environment = "dev"
    Project     = "aws-test"
    Terraform   = "true"
  }
}

# 8. Setup S3 bucket for Materialize
module "storage" {
  source = "git::https://github.com/MaterializeInc/terraform-aws-materialize.git//modules/storage?ref=prod-ready-refactor"

  name_prefix            = var.name_prefix
  bucket_lifecycle_rules = []
  bucket_force_destroy   = true

  # For testing purposes, we are disabling encryption and versioning to allow for easier cleanup
  # This should be enabled in production environments for security and data integrity
  enable_bucket_versioning = false
  enable_bucket_encryption = false

  tags = {
    Environment = "dev"
    Project     = "aws-test"
    Terraform   = "true"
  }
}

# Generate random suffix for unique S3 bucket name
resource "random_id" "suffix" {
  byte_length = 4
}

# Local values for connection details
locals {
  metadata_backend_url = format(
    "postgres://%s:%s@%s/%s?sslmode=require",
    module.database.db_instance_username,
    urlencode(random_password.db_password.result),
    module.database.db_instance_endpoint,
    module.database.db_instance_name
  )

  persist_backend_url = format(
    "s3://%s/%s:serviceaccount:%s:%s",
    module.storage.bucket_name,
    "aws-test",
    "materialize-environment",
    "main"
  )
}

output "eks_cluster_endpoint" {
  description = "EKS cluster endpoint"
  value       = module.eks.cluster_endpoint
}

output "database_endpoint" {
  description = "RDS instance endpoint"
  value       = module.database.db_instance_endpoint
}

output "s3_bucket_name" {
  description = "Name of the S3 bucket"
  value       = module.storage.bucket_name
}

output "materialize_s3_role_arn" {
  description = "The ARN of the IAM role for Materialize"
  value       = module.operator.materialize_s3_role_arn
}

output "metadata_backend_url" {
  description = "PostgreSQL connection URL in the format required by Materialize"
  value       = local.metadata_backend_url
  sensitive   = true
}

output "persist_backend_url" {
  description = "S3 connection URL in the format required by Materialize using IRSA"
  value       = local.persist_backend_url
}
