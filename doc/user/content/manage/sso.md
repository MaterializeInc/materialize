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

## Configure Authentication

* [Log in to the Materialize console](https://console.materialize.com/).

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

* Select *Organization Admin* or *Organization Member* depending on what level of console access the user needs:

    - **Organization Admin**: Can invite new users, edit account information, edit account security information, and are super users in the database.
    - **Organization Member**: Can login to the console and have database permissions defined via [role-based access control](/manage/access-control/).
