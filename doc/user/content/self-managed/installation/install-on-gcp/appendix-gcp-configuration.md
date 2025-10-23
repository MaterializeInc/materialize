---
title: "Appendix: Required configuration"
description: "Required configuration for Materialize on GCP Terraform."
menu:
  main:
    parent: "install-on-gcp"
    identifier: "appendix-gcp-config"
    weight: 50
---

## Required variables

The following variables are required when using the [Materialize on Google Cloud
Provider Terraform
module](https://github.com/MaterializeInc/terraform-google-materialize).

{{< yaml-table data="self_managed/gcp_required_variables" >}}

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
  ```

- The following data source:

  ```hcl
  data "google_client_config" "current" {}
  ```
