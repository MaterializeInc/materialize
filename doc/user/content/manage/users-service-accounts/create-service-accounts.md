---
title: "Create service accounts"
description: "Create a new service account (i.e., non-human user) to connect external applications and services to Materialize."
aliases:
  - /manage/access-control/create-service-accounts/
menu:
  main:
    parent: user-service-accounts
    weight: 10
---

It's a best practice to use service accounts (i.e., non-human users) to connect
external applications and services to Materialize. As an **administrator** of a
Materialize organization, you can create service accounts manually via the
[Materialize Console](#materialize-console) or programatically via
[Terraform](#terraform).

More granular permissions for the service account can then be configured using
[role-based access control (RBAC)](/manage/access-control/).

{{< note >}}

- The new account creation is not finished until the first time you connect with
the account.

- {{< include-md file="shared-content/rbac/service-account-creation.md" >}}

{{</ note >}}

## Materialize Console

1. [Log in to the Materialize Console](https://console.materialize.com/).

1. In the side navigation bar, click **+ Create New** > **App Password**.

1. In the **New app password** modal, specify the type and required field(s):

   {{< yaml-table data="console/service_account_fields" >}}

1. Click **Create Password** to generate a new password for your service
   account.

1. Store the new password securely.

   {{< note >}}

   Do not reload or navigate away from the screen before storing the
   password. This information is not displayed again.

   {{</ note >}}

1. Connect with the new service account to finish creating the new
   account.

   {{< note >}}

- The new account creation is not finished until the first time you connect with
  the account.

- {{< include-md file="shared-content/rbac/service-account-creation.md" >}}

   {{</ note >}}

   1. Find your new service account in the **App Passwords** table.

   1. Click on the **Connect** button to get details on connecting with the new
      account.

      {{< tabs >}}
      {{< tab "psql" >}}
If you have `psql` installed:

1. Click on the **Terminal** tab.
1. From a terminal, connect using the psql command displayed.
1. When prompted for the password, enter the app's password.

{{< include-md file="shared-content/rbac/service-account-creation.md" >}}

      {{</ tab >}}
      {{< tab "Other clients" >}}
To use a different client to connect,

1. Click on the **External tools** tab to get the connection details.

1. Update the client to use these details and connect.

{{< include-md file="shared-content/rbac/service-account-creation.md" >}}

      {{</ tab >}}
      {{</ tabs >}}

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

## Next steps

{{< include-md file="shared-content/rbac/account-creation-next-steps.md" >}}
