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
the [Materialize Console](https://console.materialize.com/) or via
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

## See also

- [Role-based access control](/manage/access-control/)
- [Manage with dbt](/manage/dbt/)
- [Manage with Terraform](/manage/terraform/)
