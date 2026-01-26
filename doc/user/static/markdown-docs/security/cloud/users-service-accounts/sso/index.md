# Configure single sign-on (SSO)

Configure single sign-on (SSO) using SAML or Open ID Connect as an additional layer of account security.



As an **administrator** of a Materialize organization, you can configure single
sign-on (SSO) as an additional layer of account security using your existing
[SAML](https://auth0.com/blog/how-saml-authentication-works/)- or
[OpenID Connect](https://auth0.com/intro-to-iam/what-is-openid-connect-oidc)-based
identity provider. This ensures that all users can securely log in to the
Materialize console using the same authentication scheme and credentials across
all systems in your organization.

> **Note:** Single sign-on in Materialize only supports authentication into the Materialize
> console. Permissions within the database are handled separately using
> [role-based access control](/security/cloud/access-control/).
>


## Before you begin

To make Materialize metadata available to Datadog, you must configure and run the following additional services:

* You must have an existing SAML- or OpenID Connect-based identity provider.
* Only users assigned the `OrganizationAdmin` role can view and modify SSO settings.

## Configure authentication

* [Log in to the Materialize console](/console/).

* Navigate to **Account** > **Account Settings** > **SSO**.


**OpenID Connect:**

* Click **Add New** and choose the `OpenID Connect` connection type.

* Add the issuer URL, client ID, and secret key provided by your identity provider.


**SAML:**

* Click **Add New** and choose the `SAML` connection type.

* Add the SSO endpoint and public certificate provided by your identity provider.




* Optionally, add the SSO domain provided by your identity provider. Click **Proceed**.

* Select the organization role for the user:


  | Organization role | Description |
  | --- | --- |
  | <strong>Organization Admin</strong> | <ul> <li> <p><strong>Console access</strong>: Has access to all Materialize console features, including administrative features (e.g., invite users, create service accounts, manage billing, and organization settings).</p> </li> <li> <p><strong>Database access</strong>: Has <red><strong>superuser</strong></red> privileges in the database.</p> </li> </ul>  |
  | <strong>Organization Member</strong> | <ul> <li> <p><strong>Console access</strong>: Has no access to Materialize console administrative features.</p> </li> <li> <p><strong>Database access</strong>: Inherits role-level privileges defined by the <code>PUBLIC</code> role; may also have additional privileges via grants or default privileges. See <a href="/security/cloud/access-control/#roles-and-privileges" >Access control control</a>.</p> </li> </ul>  |

  > **Note:** - The first user for an organization is automatically assigned the
  >   **Organization Admin** role.
  >
  > - An <a href="/security/cloud/users-service-accounts/#organization-roles" >Organization
  > Admin</a> has
  > <red><strong>superuser</strong></red> privileges in the database. Following the principle of
  > least privilege, only assign <strong>Organization Admin</strong> role to those users who
  > require superuser privileges.
  >
  > - Users/service accounts can be granted additional database roles and privileges
  >   as needed.
  >
  >




## Next steps

The organization role for a user/service account determines the default level of
database access. Once the account creation is complete, you can use <a href="/security/cloud/access-control/#role-based-access-control-rbac" >role-based
access control
(RBAC)</a> to
control access for that account.
