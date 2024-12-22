---
title: "Create service accounts"
description: "How to create a new service account (i.e., non-human user) to connect external applications and services to Materialize."
---

It's a best practice to use service accounts (i.e., non-human users) to connect
external applications and services to Materialize. As an **administrator** of a
Materialize organization, you can create service accounts manually via the
[Materialize Console](#materialize-console), or programatically via
[Terraform](#terraform).

More granular permissions for the service account can then be configured using
[role-based access control (RBAC)](/manage/access-control/#role-based-access-control-rbac).

## Materialize Console

1. [Log in to the Materialize Console](https://console.materialize.com/).

1. In the side navigation bar, click **+ Create New** > **App Password**.

1. In the **New app password** modal, select **Type** > **Service** and name the
new app password. Under **User**, specify the new service user you'd like to
create and associate with the new app password.

1. Under **Roles**, select *Organization Admin* or *Organization Member*
   depending on the level of database access the service user needs:

    - `Organization Admin`: has _superuser_ privileges in the database.

    - `Organization Member`: has restricted access to the database, depending on
      the privileges defined via [role-based access control (RBAC)](/manage/access-control/#role-based-access-control-rbac).

1. Click **Create Password** to create the new service account.

For general guidance on using the Materialize Console for admin tasks, see the
[reference documentation](/console/admin/).

## Terraform

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
   resource, and associate it with the service user created in the previous
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

For general guidance on using the Materialize Terraform provider to manage
resources in your region, see the [reference documentation](/manage/terraform/).
