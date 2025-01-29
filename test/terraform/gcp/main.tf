# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

terraform {
  required_version = ">= 1.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

module "materialize" {
  source = "github.com/MaterializeInc/terraform-google-materialize?ref=v0.1.1"

  project_id = var.project_id
  region     = var.region
  prefix     = "tf-gcp-test"

  database_config = {
    tier     = "db-custom-2-4096"
    version  = "POSTGRES_15"
    password = var.database_password
  }

  labels = {
    environment = "simple"
    example     = "true"
  }

  install_materialize_operator = false
}

variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "materialize-ci"
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-east1"
}

variable "database_password" {
  description = "Password for Cloud SQL database user"
  default     = "your-strong-password"
  type        = string
  sensitive   = true
}

output "gke_cluster" {
  description = "GKE cluster details"
  value       = module.materialize.gke_cluster
  sensitive   = true
}

output "service_accounts" {
  description = "Service account details"
  value       = module.materialize.service_accounts
}

output "connection_strings" {
  description = "Connection strings for metadata and persistence backends"
  value       = module.materialize.connection_strings
  sensitive   = true
}
