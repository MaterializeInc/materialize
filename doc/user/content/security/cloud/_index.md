---
title: "Materialize Cloud"
description: "Manage users and service accounts for Materialize Cloud."
disable_list: true
menu:
  main:
    parent: "security"
    identifier: "security-cloud"
    weight: 10
---

As an administrator of a Materialize organization, you can manage the users and
apps (via service accounts) that can access your Materialize organization and
resources.

## Organization roles

During creation of a user/service account in Materialize, the account is
assigned an organization role:

{{< include-md file="shared-content/rbac/organization-roles.md" >}}

## User accounts

As an **Organization admin**, you can [invite new
users](./invite-users/) via the Materialize Console. When you invite a new user,
Materialize will email the user with an invitation link.

{{< include-md file="shared-content/rbac/invite-user-note.md" >}}

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

- {{< include-md file="shared-content/rbac/service-account-creation.md" >}}

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

{{< annotation type="Disambiguation" >}}
{{< include-md file="shared-content/rbac/rbac-intro-disambiguation.md" >}}

This section focuses on the database access control. For information on
organization roles, see [Users and service
accounts](/manage/users-service-accounts/).
{{</ annotation >}}

<a name="role-based-access-control-rbac" ></a>

## Role-based access control

In Materialize, role-based access control (RBAC) governs access to **database
objects** through privileges granted to [database
roles](/manage/access-control/manage-roles/).

## Roles and privileges

{{% include-md file="shared-content/rbac/db-roles.md" %}}

### Managing privileges

{{% include-md file="shared-content/rbac/db-roles-managing-privileges.md" %}}

{{< annotation type="Disambiguation" >}}
{{% include-md file="shared-content/rbac/grant-vs-alter-default-privilege.md"
%}}
{{</ annotation >}}

### Initial privileges

{{< include-md file="shared-content/rbac/db-roles-initial-privileges.md" >}}

You can modify the privileges of your organization's `PUBLIC` role as well as
the modify default privileges for `PUBLIC`.

## Privilege inheritance and modular access control

In Materialize, when you grant a role to another role (user role/service account
role/independent role), the target role inherits privileges through the granted
role.

In general, to grant a user or service account privileges, create roles with the
desired privileges and grant these roles to the database role associated with
the user/service account email/name. Although you can grant privileges directly
to the associated roles, using separate, reusable roles is recommended for
better access management.

With privilege inheritance, you can compose more complex roles by
combining existing roles, enabling modular access control. However:

- Inheritance only applies to role privileges; role attributes and parameters
  are not inherited.
- {{% include-md file="shared-content/rbac/revoke-roles-consideration.md" %}}

## Best practices

{{% yaml-sections data="rbac/recommendations" heading-field="recommendation" heading-level=3 %}}


## See also

- [Role-based access control](/manage/access-control/)
- [Manage with dbt](/manage/dbt/)
- [Manage with Terraform](/manage/terraform/)
