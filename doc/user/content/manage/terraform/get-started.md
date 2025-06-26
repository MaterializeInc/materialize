---
title: "Get started with the Materialize provider"
description: "Introductory setup with Materialize Terraform provider"
menu:
  main:
    parent: "manage-terraform"
    weight: 10
    name: "Get started"
---

The following guide provides an introduction to the Materialize Terraform
provider and setup.

## Terraform provider

The Materialize provider is hosted on the [Terraform provider registry](https://registry.terraform.io/providers/MaterializeInc/materialize/latest).

To use the Materialize provider, you create a new `main.tf` file and add the
required providers:

```hcl
terraform {
  required_providers {
    materialize = {
      source = "MaterializeInc/materialize"
    }
  }
}
```

## Authentication

To configure the provider to communicate with your Materialize region, you
need to authenticate with a Materialize username, app password, and other
specifics from your account.

We recommend saving sensitive input variables as environment variables to avoid
checking secrets into source control. In Terraform, you can export Materialize
app passwords as a [Terraform environment variable](https://developer.hashicorp.com/terraform/cli/config/environment-variables#tf_var_name)
with the `TF_VAR_<name>` format.

```shell
export TF_VAR_MZ_PASSWORD=<app_password>
```

In the `main.tf` file, add the provider configuration and any variable
references:

```hcl
variable "MZ_PASSWORD" {}

provider "materialize" {
  password       = var.MZ_PASSWORD
  default_region = <region>
  database       = <database>
}
```
