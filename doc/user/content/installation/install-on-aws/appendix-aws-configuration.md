---
title: "Appendix: Required configuration"
description: "Required configuration for Materialize on AWS Terraform."
menu:
  main:
    parent: "install-on-aws"
    identifier: "appendix-aws-provider-config"
    weight: 50
aliases:
  - /installation/install-on-aws/appendix-aws-provider-configuration
---

## Required variables

The following variables are required when using the [Materialize on AWS
Terraform modules](https://github.com/MaterializeInc/terraform-aws-materialize):

{{< yaml-table data="self_managed/aws_required_variables" >}}

For a list of all variables, see the
[README.md](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#inputs)
or the [`variables.tf`
file](https://github.com/MaterializeInc/terraform-aws-materialize/blob/main/variables.tf).

## Required providers

Starting in [Materialize on AWS Terraform module
v0.3.0](https://github.com/MaterializeInc/terraform-aws-materialize), you need
to declare the following providers:

```hcl
provider "aws" {
  region = "us-east-1"  # or some other region
  # Specify additional AWS provider configuration as needed
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
