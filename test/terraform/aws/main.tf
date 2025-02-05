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

module "materialize_infrastructure" {
  source = "git::https://github.com/MaterializeInc/terraform-aws-materialize.git?ref=v0.2.0"

  # Basic settings
  # The namespace and environment variables are used to construct the names of the resources
  # e.g. ${namespace}-${environment}-eks and etc.
  namespace    = "terraform-aws-test"
  environment  = "dev"

  # VPC Configuration
  vpc_cidr             = "10.0.0.0/16"
  availability_zones   = ["us-east-1a", "us-east-1b"]
  private_subnet_cidrs = ["10.0.1.0/24", "10.0.2.0/24"]
  public_subnet_cidrs  = ["10.0.101.0/24", "10.0.102.0/24"]
  single_nat_gateway   = true

  # EKS Configuration
  cluster_version           = "1.31"
  node_group_instance_types = ["c7a.2xlarge"]
  node_group_desired_size   = 2
  node_group_min_size       = 1
  node_group_max_size       = 3
  node_group_capacity_type  = "ON_DEMAND"

  # Storage Configuration
  bucket_force_destroy = true

  # For testing purposes, we are disabling encryption and versioning to allow for easier cleanup
  # This should be enabled in production environments for security and data integrity
  enable_bucket_versioning = false
  enable_bucket_encryption = false

  # Database Configuration
  database_password    = "someRANDOMpasswordNOTsecure"
  postgres_version     = "15"
  db_instance_class    = "db.t3.micro"
  db_allocated_storage = 20
  database_name        = "materialize"
  database_username    = "materialize"
  db_multi_az          = false

  # Basic monitoring
  enable_monitoring      = true
  metrics_retention_days = 7

  # Tags
  tags = {
    Environment = "dev"
    Project     = "terraform-aws-test"
    Terraform   = "true"
  }
}

# Generate random suffix for unique S3 bucket name
resource "random_id" "suffix" {
  byte_length = 4
}

# outputs.tf
output "eks_cluster_endpoint" {
  description = "EKS cluster endpoint"
  value       = module.materialize_infrastructure.eks_cluster_endpoint
}

output "database_endpoint" {
  description = "RDS instance endpoint"
  value       = module.materialize_infrastructure.database_endpoint
}

output "s3_bucket_name" {
  description = "Name of the S3 bucket"
  value       = module.materialize_infrastructure.s3_bucket_name
}

output "materialize_s3_role_arn" {
  description = "The ARN of the IAM role for Materialize"
  value       = module.materialize_infrastructure.materialize_s3_role_arn
}

output "metadata_backend_url" {
  description = "PostgreSQL connection URL in the format required by Materialize"
  value       = module.materialize_infrastructure.metadata_backend_url
  sensitive   = true
}

output "persist_backend_url" {
  description = "S3 connection URL in the format required by Materialize using IRSA"
  value       = module.materialize_infrastructure.persist_backend_url
}
