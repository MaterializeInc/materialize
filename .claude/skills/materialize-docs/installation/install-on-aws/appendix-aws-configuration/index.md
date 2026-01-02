---
audience: developer
canonical_url: https://materialize.com/docs/installation/install-on-aws/appendix-aws-configuration/
complexity: intermediate
description: Required configuration for Materialize on AWS Terraform.
doc_type: reference
keywords:
- 'Appendix: Required configuration'
product_area: Deployment
status: beta
title: 'Appendix: Required configuration'
---

# Appendix: Required configuration

## Purpose
Required configuration for Materialize on AWS Terraform.

If you need to understand the syntax and options for this command, you're in the right place.


Required configuration for Materialize on AWS Terraform.



## Required variables

The following variables are required when using the [Materialize on AWS
Terraform modules](https://github.com/MaterializeInc/terraform-aws-materialize):

<!-- Dynamic table: self_managed/aws_required_variables - see original docs -->

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

