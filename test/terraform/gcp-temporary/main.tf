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
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

data "google_client_config" "default" {}

provider "kubernetes" {
  host                   = "https://${module.materialize.gke_cluster.endpoint}"
  token                  = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(module.materialize.gke_cluster.ca_certificate)
}

provider "helm" {
  kubernetes {
    host                   = "https://${module.materialize.gke_cluster.endpoint}"
    token                  = data.google_client_config.default.access_token
    cluster_ca_certificate = base64decode(module.materialize.gke_cluster.ca_certificate)
  }
}

module "materialize" {
  source = "github.com/MaterializeInc/terraform-google-materialize?ref=v0.4.2"

  project_id = var.project_id
  region     = var.region
  prefix     = "tf-gcp-test"

  database_config = {
    tier     = "db-custom-2-4096"
    version  = "POSTGRES_15"
    password = var.database_password
  }

  network_config = {
    subnet_cidr   = "10.0.0.0/20"
    pods_cidr     = "10.48.0.0/14"
    services_cidr = "10.52.0.0/20"
  }

  labels = {
    environment = "simple"
    example     = "true"
  }

  install_materialize_operator = true
  use_local_chart = true
  helm_chart = "materialize-operator-v25.2.0-beta.1.tgz"
  operator_version = var.operator_version
  orchestratord_version = var.orchestratord_version

  install_cert_manager = false
  use_self_signed_cluster_issuer = false

  helm_values = {
      clusters = {
        defaultReplicationFactor = {
            system = 1
            probe = 1
            support = 1
            analytics = 1
        }
      }
  }

  providers = {
    google     = google
    kubernetes = kubernetes
    helm       = helm
  }
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

variable "operator_version" {
  type    = string
  default = "v25.2.0-beta.1.tgz"
}

variable "orchestratord_version" {
  type    = string
  default = null
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
