---
title: "Single sign-on"
description: "Configure single sign-on (SSO) using SAML or Open ID Connect as an additional layer of account security."
menu:
  main:
    parent: manage
    weight: 10
---

You can configure single sign-on (SSO) as an additional layer of account security using your existing [SAML](https://auth0.com/blog/how-saml-authentication-works/)- or [OpenID Connect](https://auth0.com/intro-to-iam/what-is-openid-connect-oidc)-based identity provider. This ensures that all users can securely log in to the Materialize console using the same authentication scheme and credentials across all systems in your organization.

{{< note >}}
Single sign-on in Materialize only supports authentication into the Materialize console. Permissions within the database are handled separately using [role-based access control](/manage/access-control/).
{{</ note >}}

## Prerequisites

* You must have an existing SAML- or OpenID Connect-based identity provider.
* Only users assigned the `OrganizationAdmin` role can view and modify SSO settings.

## Configure SAML authentication

1. [Login to the Materialize console](https://console.materialize.com/).

1. Navigate to **Account** > **Account Settings** > **SSO**.

1. Click **Add New** and choose the `SAML` connection type. 

1. Add the SSO endpoint and public certificate provided by your identity provider.

1. Optionally, add the SSO domain provided by your identity provider. Click **Proceed**.

1. Select *Organization Admin* or *Organization Member* depending on what level of console access the user needs:

    - **Organization Admin**: Can invite new users, edit account information,
    and edit account security information.
    - **Organization Member**: Can log in to the console.

    It's important to note that these roles determine which actions users can take
    in the Materialize console, and do not translate to RBAC roles and privileges
    in the database.

## Configure OpenID Connect authentication

1. [Log in to the Materialize console](https://console.materialize.com/).

1. Navigate to **Account** > **Account Settings** > **SSO**.

1. Click **Add New** and choose the `OpenID Connect` connection type. 

1. Add the issuer URL, client ID, and secret key provided by your identity provider.

1. Optionally, add the SSO domain provided by your identity provider. Click **Proceed**.

1. Select *Organization Admin* or *Organization Member* depending on what level of console access the user needs:

    - **Organization Admin**: Can invite new users, edit account information,
    and edit account security information.
    - **Organization Member**: Can login to the console.

    These roles do not refer to RBAC roles and privileges and refer to
    actions users can take in the Materialize Console.
