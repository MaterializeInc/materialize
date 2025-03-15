---
title: "Appendix: AWS provider configuration"
description: ""
menu:
  main:
    parent: "install-on-aws"
    identifier: "appendix-aws-provider-config"
    weight: 50

---

To use [Materialize on AWS Terraform
module](https://github.com/MaterializeInc/terraform-aws-materialize), you need
to declare the following providers:[^1]

```hcl
provider "aws" {
  region = "us-east-1"  # or some other region
  # Specify addition AWS provider configuration as needed
}

# Required for EKS authentication
provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)

  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
    command     = "aws"
  }
}

# Required for Materialize Operator installation
provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)

    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
      command     = "aws"
    }
  }
}
```

[^1]: If using the `examples/simple/main.tf`, the example configuration declares
    the providers for you.
