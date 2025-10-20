---
title: "Configure single sign-on (SSO)"
description: "Configure single sign-on (SSO) using SAML or Open ID Connect as an additional layer of account security."
aliases:
  - /manage/sso/
  - /manage/access-control/sso/
menu:
  main:
    parent: user-service-accounts
    weight: 15
---

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
[role-based access control](/manage/access-control/).
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

  {{< include-md file="shared-content/rbac/organization-roles.md" >}}


## Next steps

{{< include-md file="shared-content/rbac/account-creation-next-steps.md" >}}
