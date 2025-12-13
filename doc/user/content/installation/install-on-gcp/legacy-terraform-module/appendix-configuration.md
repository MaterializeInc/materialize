---
title: "Appendix: Configuration"
description: "Required configuration for Materialize on GCP Terraform (legacy)."
menu:
  main:
    parent: "install-on-gcp-legacy-terraform-module"
    identifier: "legacy-terraform-module-appendix-configuration"
    weight: 50
aliases:
  - /installation/install-on-gcp/appendix-gcp-configuration/
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

## Swap support

Starting in v0.6.1 of Materialize on Google Cloud Provider (GCP) Terraform,
disk support (using swap on NVMe instance storage) may be enabled for
Materialize. With this change, the Terraform:

- Creates a node group for Materialize.
- Configures NVMe instance store volumes as swap using a daemonset.
- Enables swap at the Kubelet.

For swap support, the following configuration options are available:

- [`swap_enabled`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_swap_enabled)

See [Upgrade Notes](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#v061).

## Storage bucket versioning

Starting in v0.3.1 of Materialize on GCP Terraform, storage bucket versioning is
disabled (i.e.,
[`storage_bucket_versioning`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_storage_bucket_versioning)
is set to `false` by default) to facilitate cleanup of resources during testing.
When running in production, versioning should be turned on with a sufficient TTL
([`storage_bucket_version_ttl`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_storage_bucket_version_ttl))
to meet any data-recovery requirements.
