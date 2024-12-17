---
title: "Service accounts"
description: "Configure a new service account (i.e., non-human user) to connect external applications and services to Materialize."
---

It's a best practice to use service accounts (i.e., non-human users) to connect
external applications and services to Materialize. As an **administrator** of a
Materialize organization, you can create service accounts manually via the
[Materialize console](https://console.materialize.com/), or programatically
via [Terraform](/manage/terraform/).

More granular permissions for the service account can then be configured using
[role-based access control (RBAC)](/manage/access-control/#role-based-access-control-rbac).

## How to create a service account

### Materialize Console

1. [Log in to the Materialize Console](https://console.materialize.com/).

1. In the side navigation bar, click **+ Create New** > **App Password**.

1. In the **New app password** modal, select **Type** > **Service** and name the
new app password. Under **User**, specify the new service user you'd like to
create and associate with the new app password.

1. Optionally, associate the new service user with existing **Roles** to grant
it existing database privileges. You can manage these privileges at any time
using [RBAC](/manage/access-control/#role-based-access-control-rbac).

1. Click **Create Password** to create the new service account.

### Terraform

**Minimum requirements:** `terraform-provider-materialize` v0.8.1+

1. Create a new service user using the [`materialize_role`](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs/resources/role)
   resource:

  ```hcl
  resource "materialize_role" "production_dashboard" {
    name   = "svc_production_dashboard"
    region = "aws/us-east-1"
  }
  ```

1. Create a new `service` app password using the [`materialize_app_password`](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs/resources/app_password)
   resource, and associated it with the service user created in the previous
   step:

  ```hcl
  resource "materialize_app_password" "production_dashboard" {
    name = "production_dashboard_app_password"
    type = "service"
    user = materialize_role.production_dashboard.name
    roles = ["Member"]
  }
  ```

1. Optionally, associate the new service user with existing roles to grant it
   existing database privileges.

  ```hcl
  resource "materialize_database_grant" "database_usage" {
    role_name     = materialize_role.production_dashboard.name
    privilege     = "USAGE"
    database_name = "production_analytics"
    region        = "aws/us-east-1"
  }
  ```

1. Export the user and password for use in the external application or service.

  ```hcl
  output "production_dashboard_user" {
    value = materialize_role.production_dashboard.name
  }
  output "production_dashboard_password" {
    value = materialize_app_password.production_dashboard.password
  }
  ```
