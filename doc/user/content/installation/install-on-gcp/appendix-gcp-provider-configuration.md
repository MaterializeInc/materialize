---
title: "Appendix: GCP provider configuration"
description: ""
menu:
  main:
    parent: "install-on-gcp"
    identifier: "appendix-gcp-provider-config"
    weight: 50

---

To use [Materialize on Google Cloud Terraform
module](https://github.com/MaterializeInc/terraform-google-materialize) v0.2.0+,
you need to declare the following providers: [^1]

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

[^1]: If using the `examples/simple/main.tf`, the example configuration declares
    the providers for you.
