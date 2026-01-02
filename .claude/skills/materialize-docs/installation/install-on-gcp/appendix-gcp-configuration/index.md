---
audience: developer
canonical_url: https://materialize.com/docs/installation/install-on-gcp/appendix-gcp-configuration/
complexity: intermediate
description: Required configuration for Materialize on GCP Terraform.
doc_type: reference
keywords:
- 'Appendix: Required configuration'
product_area: Deployment
status: stable
title: 'Appendix: Required configuration'
---

# Appendix: Required configuration

## Purpose
Required configuration for Materialize on GCP Terraform.

If you need to understand the syntax and options for this command, you're in the right place.


Required configuration for Materialize on GCP Terraform.



## Required variables

The following variables are required when using the [Materialize on Google Cloud
Provider Terraform
module](https://github.com/MaterializeInc/terraform-google-materialize).

<!-- Dynamic table: self_managed/gcp_required_variables - see original docs -->

For a list of all variables, see the
[README.md](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#inputs)
or the [`variables.tf` file](https://github.com/MaterializeInc/terraform-google-materialize/blob/main/variables.tf).

## Required providers and data source declaration

To use [Materialize on Google Cloud Terraform
module](https://github.com/MaterializeInc/terraform-google-materialize) v0.2.0+,
you need to declare:

- The following providers:

  ```hcl
  provider "google" {
    project = var.project_id
    region  = var.region
    # Specify additional Google provider configuration as needed
  }

  # Required for GKE authentication
  provider "kubernetes" {
    host                   = "https://${module.gke.cluster_endpoint}"
    token                  = data.google_client_config.current.access_token
    cluster_ca_certificate = base64decode(module.gke.cluster_ca_certificate)
  }

  provider "helm" {
    kubernetes {
      host                   = "https://${module.gke.cluster_endpoint}"
      token                  = data.google_client_config.current.access_token
      cluster_ca_certificate = base64decode(module.gke.cluster_ca_certificate)
    }
  }
  ```text

- The following data source:

  ```hcl
  data "google_client_config" "current" {}
  ```

