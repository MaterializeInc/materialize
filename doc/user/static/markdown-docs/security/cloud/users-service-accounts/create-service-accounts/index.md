# Create service accounts

Create a new service account (i.e., non-human user) to connect external applications and services to Materialize.



It's a best practice to use service accounts (i.e., non-human users) to connect
external applications and services to Materialize. As an **administrator** of a
Materialize organization, you can create service accounts manually via the
[Materialize Console](#materialize-console) or programatically via
[Terraform](#terraform).

More granular permissions for the service account can then be configured using
[role-based access control (RBAC)](/security/cloud/access-control/).

> **Note:** - The new account creation is not finished until the first time you connect with
> the account.
>
> - The first time the account connects, a database role with the same name as the
> specified service account <strong>User</strong> is created, and the service account creation is complete.
>
>


## Materialize Console

1. [Log in to the Materialize Console](/console/).

1. In the side navigation bar, click **+ Create New** > **App Password**.

1. In the **New app password** modal, specify the type and required field(s):


   | Field | Details |
   | --- | --- |
   | <strong>Type</strong> | Select <strong>Service</strong> |
   | <strong>Name</strong> | Specify a descriptive name. |
   | <strong>User</strong> | Specify a service account user name. If the specified account user does not exist, it will be automatically created the <strong>first time</strong> the application connects with the user name and password. |
   | <strong>Roles</strong> | <p>Select the organization role:</p>  \| Organization role \| Description \| \| --- \| --- \| \| <strong>Organization Admin</strong> \| <ul> <li> <p><strong>Console access</strong>: Has access to all Materialize console features, including administrative features (e.g., invite users, create service accounts, manage billing, and organization settings).</p> </li> <li> <p><strong>Database access</strong>: Has <red><strong>superuser</strong></red> privileges in the database.</p> </li> </ul>  \| \| <strong>Organization Member</strong> \| <ul> <li> <p><strong>Console access</strong>: Has no access to Materialize console administrative features.</p> </li> <li> <p><strong>Database access</strong>: Inherits role-level privileges defined by the <code>PUBLIC</code> role; may also have additional privileges via grants or default privileges. See <a href="/security/cloud/access-control/#roles-and-privileges" >Access control control</a>.</p> </li> </ul>  \|  > **Note:** - The first user for an organization is automatically assigned the >   **Organization Admin** role. >  > - An <a href="/security/cloud/users-service-accounts/#organization-roles" >Organization > Admin</a> has > <red><strong>superuser</strong></red> privileges in the database. Following the principle of > least privilege, only assign <strong>Organization Admin</strong> role to those users who > require superuser privileges. >  > - Users/service accounts can be granted additional database roles and privileges >   as needed. >  >     |


1. Click **Create Password** to generate a new password for your service
   account.

1. Store the new password securely.

   > **Note:** Do not reload or navigate away from the screen before storing the
>    password. This information is not displayed again.
>
>


1. Connect with the new service account to finish creating the new
   account.

   > **Note:** - The new account creation is not finished until the first time you connect with
>   the account.
>
> - The first time the account connects, a database role with the same name as the
> specified service account <strong>User</strong> is created, and the service account creation is complete.
>
>


   1. Find your new service account in the **App Passwords** table.

   1. Click on the **Connect** button to get details on connecting with the new
      account.


      **psql:**
If you have `psql` installed:

1. Click on the **Terminal** tab.
1. From a terminal, connect using the psql command displayed.
1. When prompted for the password, enter the app's password.

The first time the account connects, a database role with the same name as the
specified service account <strong>User</strong> is created, and the service account creation is complete.


      **Other clients:**
To use a different client to connect,

1. Click on the **External tools** tab to get the connection details.

1. Update the client to use these details and connect.

The first time the account connects, a database role with the same name as the
specified service account <strong>User</strong> is created, and the service account creation is complete.




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

The organization role for a user/service account determines the default level of
database access. Once the account creation is complete, you can use <a href="/security/cloud/access-control/#role-based-access-control-rbac" >role-based
access control
(RBAC)</a> to
control access for that account.
