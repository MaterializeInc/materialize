# User and service accounts

Manage users and service accounts.



As an administrator of a Materialize organization, you can manage the users and
apps (via service accounts) that can access your Materialize organization and
resources.

## Organization roles

During creation of a user/service account in Materialize, the account is
assigned an organization role:

{{< include-md file="shared-content/rbac-cloud/organization-roles.md" >}}

## User accounts

As an **Organization admin**, you can [invite new
users](./invite-users/) via the Materialize Console. When you invite a new user,
Materialize will email the user with an invitation link.

{{< include-md file="shared-content/rbac-cloud/invite-user-note.md" >}}

For instructions on inviting users to your Materialize organization, see [Invite
users](./invite-users/).

## Service accounts

{{< tip >}}

As a best practice, we recommend you use service accounts to connect external
applications and services to Materialize.

{{</ tip >}}

As an **Organization admin**, you can create a new service account via
the [Materialize Console](/console/) or via
[Terraform](/manage/terraform/).

{{< note >}}

- The new account creation is not finished until the first time you connect with
the account.

- {{< include-md file="shared-content/rbac-cloud/service-account-creation.md" >}}

{{</ note >}}

For instructions on creating a new service account in your Materialize
organization, see [Create service accounts](./create-service-accounts/).

## Single sign-on (SSO)

As an **Organization admin**, you can configure single sign-on (SSO) as
an additional layer of account security using your existing
[SAML](https://auth0.com/blog/how-saml-authentication-works/)- or [OpenID
Connect](https://auth0.com/intro-to-iam/what-is-openid-connect-oidc)-based
identity provider. This ensures that all users can securely log in to the
Materialize Console using the same authentication scheme and credentials across
all systems in your organization.

To configure SSO for your Materialize organization, follow [this step-by-step
guide](./sso/).

## See also

- [Role-based access control](/security/cloud/access-control/)
- [Manage with dbt](/manage/dbt/)
- [Manage with Terraform](/manage/terraform/)




---

## Configure single sign-on (SSO)


As an **administrator** of a Materialize organization, you can configure single
sign-on (SSO) as an additional layer of account security using your existing
[SAML](https://auth0.com/blog/how-saml-authentication-works/)- or
[OpenID Connect](https://auth0.com/intro-to-iam/what-is-openid-connect-oidc)-based
identity provider. This ensures that all users can securely log in to the
Materialize console using the same authentication scheme and credentials across
all systems in your organization.

{{< note >}}
Single sign-on in Materialize only supports authentication into the Materialize
console. Permissions within the database are handled separately using
[role-based access control](/security/cloud/access-control/).
{{</ note >}}

## Before you begin

To make Materialize metadata available to Datadog, you must configure and run the following additional services:

* You must have an existing SAML- or OpenID Connect-based identity provider.
* Only users assigned the `OrganizationAdmin` role can view and modify SSO settings.

## Configure authentication

* [Log in to the Materialize console](/console/).

* Navigate to **Account** > **Account Settings** > **SSO**.

{{< tabs >}}
{{< tab "OpenID Connect" >}}

* Click **Add New** and choose the `OpenID Connect` connection type.

* Add the issuer URL, client ID, and secret key provided by your identity provider.

{{< /tab >}}
{{< tab "SAML" >}}

* Click **Add New** and choose the `SAML` connection type.

* Add the SSO endpoint and public certificate provided by your identity provider.

{{< /tab >}}
{{< /tabs >}}

* Optionally, add the SSO domain provided by your identity provider. Click **Proceed**.

* Select the organization role for the user:

  {{< include-md file="shared-content/rbac-cloud/organization-roles.md" >}}


## Next steps

{{< include-md file="shared-content/rbac-cloud/account-creation-next-steps.md" >}}




---

## Create service accounts


It's a best practice to use service accounts (i.e., non-human users) to connect
external applications and services to Materialize. As an **administrator** of a
Materialize organization, you can create service accounts manually via the
[Materialize Console](#materialize-console) or programatically via
[Terraform](#terraform).

More granular permissions for the service account can then be configured using
[role-based access control (RBAC)](/security/cloud/access-control/).

{{< note >}}

- The new account creation is not finished until the first time you connect with
the account.

- {{< include-md file="shared-content/rbac-cloud/service-account-creation.md" >}}

{{</ note >}}

## Materialize Console

1. [Log in to the Materialize Console](/console/).

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

- {{< include-md file="shared-content/rbac-cloud/service-account-creation.md" >}}

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

{{< include-md file="shared-content/rbac-cloud/service-account-creation.md" >}}

      {{</ tab >}}
      {{< tab "Other clients" >}}
To use a different client to connect,

1. Click on the **External tools** tab to get the connection details.

1. Update the client to use these details and connect.

{{< include-md file="shared-content/rbac-cloud/service-account-creation.md" >}}

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

{{< include-md file="shared-content/rbac-cloud/account-creation-next-steps.md" >}}




---

## Invite users


{{< include-md file="shared-content/rbac-cloud/invite-user-note.md" >}}

As an **Organization administrator**, you can invite new users via the
Materialize Console.

1. [Log in to the Materialize Console](/console/).

1. Navigate to **Account** > **Account Settings** > **Users**.

1. Click **Invite User** and fill in the user information.

1. In the **Select Role**, select the organization role for the user:

   {{< include-md file="shared-content/rbac-cloud/organization-roles.md" >}}

1. Click the **Invite** button at the bottom right section of the screen.

   Materialize will email the user with an invitation link.

   {{< include-md file="shared-content/rbac-cloud/invite-user-note.md" >}}

## Next steps

{{< include-md file="shared-content/rbac-cloud/account-creation-next-steps.md" >}}



