---
title: "User and service accounts"
description: "Manage users and service accounts."
disable_list: true
menu:
  main:
    parent: "manage"
    identifier: "user-service-accounts"
    weight: 10
---

As an administrator of a Materialize organization, you can manage the users and
apps (via service accounts) that can access your Materialize organization and
resources.

## Organization roles

Each user/service account in Materialize is associated with an organization
role:

{{< yaml-table data="access-controls/organization_roles" >}}

When creating a new account for your Materialize organization, you assign
an organization role to the account. As such, before creating a new account,
determine the appropriate access level needed for the new accounts.

## User accounts

As an **Organization administrator**, you can [invite new
users](./invite-users/) via the Materialize Console. When you invite a new user,
Materialize will email the user with an invitation link.

{{< include-md file="shared-content/invite-user-note.md" >}}

For instructions on inviting users to your Materialize organization, see [Invite
users](./invite-users/).

## Service accounts

{{< tip >}}

As a best practice, we recommend you use service accounts to connect external
applications and services to Materialize.

{{</ tip >}}

As an **Organization administrator**, you can create a new service account via
the [Materialize Console](https://console.materialize.com/) or via
[Terraform](/manage/terraform/).

{{< note >}}

The new account creation is not finished until you use it to connect for the
first time. That is, after you setup a new service account, you must use it to
connect to finish creating the new account.

{{</ note >}}

For instructions on creating a new service account in your Materialize
organization, see [Create service accounts](./create-service-accounts/).

## Single sign-on (SSO)

As an **Organization administrator**, you can configure single sign-on (SSO) as
an additional layer of account security using your existing
[SAML](https://auth0.com/blog/how-saml-authentication-works/)- or [OpenID
Connect](https://auth0.com/intro-to-iam/what-is-openid-connect-oidc)-based
identity provider. This ensures that all users can securely log in to the
Materialize Console using the same authentication scheme and credentials across
all systems in your organization.

To configure SSO for your Materialize organization, follow [this step-by-step
guide](./sso/).

## See also

- [Role-based access control](/manage/access-control/rbac/)
- [Manage with dbt](/manage/dbt/)
- [Manage with Terraform](/manage/terraform/)
