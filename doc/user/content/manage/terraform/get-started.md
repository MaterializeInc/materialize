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

## Provider configuration

The Materialize provider supports two distinct configuration modes depending on
your deployment type:

{{< tabs >}}
{{< tab "Materialize Cloud" >}}
### Materialize Cloud

For Materialize Cloud environments, configure the provider with your app password
and region. This configuration provides access to **all provider resources**,
including:

- App passwords, users, SSO, and SCIM resources
- All database resources (clusters, sources, sinks, schemas, etc.)

We recommend saving sensitive input variables as environment variables to avoid
checking secrets into source control. In Terraform, you can export variables as
[Terraform environment variables](https://developer.hashicorp.com/terraform/cli/config/environment-variables#tf_var_name)
with the `TF_VAR_<name>` format.

```shell
export TF_VAR_materialize_password=<app_password>
```

In your `main.tf` file, add the provider configuration:

```hcl
variable "materialize_password" {
  sensitive = true
}

provider "materialize" {
  password       = var.materialize_password  # or use MZ_PASSWORD env var
  default_region = "aws/us-east-1"           # or use MZ_DEFAULT_REGION env var
}
```

{{</ tab >}}

{{< tab "Self-hosted" >}}
### Self-hosted Materialize

For self-hosted Materialize instances, configure the provider with connection
parameters similar to a standard PostgreSQL connection.

{{< warning >}}
**Important limitations for self-hosted mode:**

Self-hosted configurations do **not** support Frontegg-dependent resources:
- `materialize_app_password`
- `materialize_user`
- `materialize_sso_config` and related SSO resources
- `materialize_scim_config` and related SCIM resources

Only database resources are available (clusters, sources, sinks, schemas, etc.).
These organization and identity management resources require Materialize Cloud's
identity provider and will produce error messages if used in self-hosted mode.
{{< /warning >}}

Configure the provider for self-hosted deployments:

```shell
export TF_VAR_materialize_password=<database_password>
```

In your `main.tf` file:

```hcl
variable "materialize_password" {
  sensitive = true
}

provider "materialize" {
  host     = "materialized"        # or use MZ_HOST env var
  port     = 6875                  # or use MZ_PORT env var
  username = "materialize"         # or use MZ_USER env var
  database = "materialize"         # or use MZ_DATABASE env var
  password = var.materialize_password  # or use MZ_PASSWORD env var
  sslmode  = "disable"             # or use MZ_SSLMODE env var
}
```

#### Configuration parameters

The following parameters are available for self-hosted configurations:

| Parameter | Description | Environment Variable | Default |
|-----------|-------------|---------------------|---------|
| `host` | Materialize host address | `MZ_HOST` | - |
| `port` | Materialize port | `MZ_PORT` | `6875` |
| `username` | Database username | `MZ_USER` | `materialize` |
| `database` | Database name | `MZ_DATABASE` | `materialize` |
| `password` | Database password | `MZ_PASSWORD` | - |
| `sslmode` | SSL mode (`disable`, `require`, `verify-ca`, `verify-full`) | `MZ_SSLMODE` | `require` |

{{< /tab >}}
{{< /tabs >}}

{{< warning >}}
**Migration warning:**

Switching between SaaS and self-hosted modes requires careful state file
management as resource references and regional configurations differ between
modes. We strongly recommend using a consistent configuration mode from the
beginning to avoid complex state migrations.
{{< /warning >}}
