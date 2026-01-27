# Get started with the Materialize provider
Introductory setup with Materialize Terraform provider
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

## Authentication and Configuration

The provider auto-detects your deployment type based on the configuration you
provide:
- **Materialize Cloud**: Use `password` and `default_region`
- **Self-managed**: Use `host`, `username`, `password`, and other connection parameters

> **Warning:** Switching between Materialize Cloud and self-managed configuration **breaks your
> Terraform state file**. Ensure that your initial configuration
> matches your intended deployment type, and do not switch to a
> different deployment type afterward.



**Materialize Cloud:**
### Materialize Cloud

Configure the provider with your [app password](/security/cloud/users-service-accounts/create-service-accounts/)
and region. This provides access to all provider resources, including
organization-level resources (users, SSO, SCIM) and database resources.

To avoid checking secrets into source control, use environment variables for authentication. You have two options:

**Option 1: Using provider environment variables (recommended)**

The provider automatically reads the `MZ_PASSWORD` environment variable:

```shell
export MZ_PASSWORD=<app_password>
```

```hcl
provider "materialize" {
  default_region = <region>
  database       = <database>
}
```

**Option 2: Using Terraform input variables**

Use [Terraform environment variables](https://developer.hashicorp.com/terraform/cli/config/environment-variables#tf_var_name) with the `TF_VAR_` prefix:

```shell
export TF_VAR_materialize_password=<app_password>
```

```hcl
variable "materialize_password" {
  type      = string
  sensitive = true
}

provider "materialize" {
  password       = var.materialize_password
  default_region = <region>
  database       = <database>
}
```

#### Creating service accounts

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



**Self-managed Materialize:**
### Self-managed Materialize

Configure the provider with connection parameters similar to a standard
PostgreSQL connection. Only database resources are available (clusters, sources,
sinks, etc.). Organization-level resources like `materialize_app_password`,
`materialize_user`, and SSO/SCIM resources are not supported.

To avoid checking secrets into source control, use environment variables for authentication. You have two options:

**Option 1: Using provider environment variables (recommended)**

The provider automatically reads configuration from `MZ_*` environment variables:

```shell
export MZ_PASSWORD=<password>
export MZ_HOST=<host>
```

```hcl
provider "materialize" {
  # Configuration will be read from MZ_HOST, MZ_PORT, MZ_USER,
  # MZ_DATABASE, MZ_PASSWORD, MZ_SSLMODE environment variables
}
```

**Option 2: Using Terraform input variables**

Use [Terraform environment variables](https://developer.hashicorp.com/terraform/cli/config/environment-variables#tf_var_name) with the `TF_VAR_` prefix:

```shell
export TF_VAR_mz_password=<password>
```

```hcl
variable "mz_password" {
  type      = string
  sensitive = true
}

provider "materialize" {
  host     = "materialized"
  port     = 6875
  username = "materialize"
  database = "materialize"
  password = var.mz_password
  sslmode  = "disable"
}
```

#### Provider configuration parameters

| Parameter | Description | Environment Variable | Default |
|-----------|-------------|---------------------|---------|
| `host` | Materialize host address | `MZ_HOST` | - |
| `port` | Materialize port | `MZ_PORT` | `6875` |
| `username` | Database username | `MZ_USER` | `materialize` |
| `database` | Database name | `MZ_DATABASE` | `materialize` |
| `password` | Database password | `MZ_PASSWORD` | - |
| `sslmode` | SSL mode (`disable`, `require`, `verify-ca`, `verify-full`) | `MZ_SSLMODE` | `require` |
