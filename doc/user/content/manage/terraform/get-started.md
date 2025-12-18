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

## Creating service accounts

**Minimum requirements:** `terraform-provider-materialize` v0.8.1+

As a best practice, we strongly recommend using [service accounts](/security/users-service-accounts/create-service-accounts/)
to connect external applications to Materialize. To create a
service account, create a new [`materialize_role`](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs/resources/role)
and associate it with a new [`materialize_app_password`](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs/resources/app_password)
of type `service`. More granular permissions for the service account can then
be configured using [role-based access control (RBAC)](/security/cloud/access-control/#role-based-access-control-rbac).

```hcl
# Create a service user in the aws/us-east-1 region.
resource "materialize_role" "production_dashboard" {
  name   = "svc_production_dashboard"
  region = "aws/us-east-1"
}

# Create an app password for the service user.
resource "materialize_app_password" "production_dashboard" {
  name = "production_dashboard_app_password"
  type = "service"
  user = materialize_role.production_dashboard.name
  roles = ["Member"]
}

# Allow the service user to use the "production_analytics" database.
resource "materialize_database_grant" "database_usage" {
  role_name     = materialize_role.production_dashboard.name
  privilege     = "USAGE"
  database_name = "production_analytics"
  region        = "aws/us-east-1"
}

# Export the user and password for use in the external tool.
output "production_dashboard_user" {
  value = materialize_role.production_dashboard.name
}
output "production_dashboard_password" {
  value = materialize_app_password.production_dashboard.password
}
```
