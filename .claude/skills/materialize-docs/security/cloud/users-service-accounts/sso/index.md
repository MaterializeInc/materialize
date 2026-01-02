---
audience: developer
canonical_url: https://materialize.com/docs/security/cloud/users-service-accounts/sso/
complexity: intermediate
description: Configure single sign-on (SSO) using SAML or Open ID Connect as an additional
  layer of account security.
doc_type: reference
keywords:
- Account Settings
- Account
- Configure single sign-on (SSO)
- SSO
- administrator
- 'Note:'
- SELECT THE
product_area: Security
status: stable
title: Configure single sign-on (SSO)
---

# Configure single sign-on (SSO)

## Purpose
Configure single sign-on (SSO) using SAML or Open ID Connect as an additional layer of account security.

If you need to understand the syntax and options for this command, you're in the right place.


Configure single sign-on (SSO) using SAML or Open ID Connect as an additional layer of account security.



As an **administrator** of a Materialize organization, you can configure single
sign-on (SSO) as an additional layer of account security using your existing
[SAML](https://auth0.com/blog/how-saml-authentication-works/)- or
[OpenID Connect](https://auth0.com/intro-to-iam/what-is-openid-connect-oidc)-based
identity provider. This ensures that all users can securely log in to the
Materialize console using the same authentication scheme and credentials across
all systems in your organization.

> **Note:** 
Single sign-on in Materialize only supports authentication into the Materialize
console. Permissions within the database are handled separately using
[role-based access control](/security/cloud/access-control/).


## Before you begin

To make Materialize metadata available to Datadog, you must configure and run the following additional services:

* You must have an existing SAML- or OpenID Connect-based identity provider.
* Only users assigned the `OrganizationAdmin` role can view and modify SSO settings.

## Configure authentication

* [Log in to the Materialize console](/console/).

* Navigate to **Account** > **Account Settings** > **SSO**.

#### OpenID Connect

* Click **Add New** and choose the `OpenID Connect` connection type.

* Add the issuer URL, client ID, and secret key provided by your identity provider.

#### SAML

* Click **Add New** and choose the `SAML` connection type.

* Add the SSO endpoint and public certificate provided by your identity provider.

* Optionally, add the SSO domain provided by your identity provider. Click **Proceed**.

* Select the organization role for the user:

  <!-- Dynamic table: rbac/organization_roles - see original docs -->

> **Note:** 
- The first user for an organization is automatically assigned the
  **Organization Admin** role.

- An [Organization
Admin](/security/cloud/users-service-accounts/#organization-roles) has
<red>**superuser**</red> privileges in the database. Following the principle of
least privilege, only assign **Organization Admin** role to those users who
require superuser privileges.


- Users/service accounts can be granted additional database roles and privileges
  as needed.





## Next steps

The organization role for a user/service account determines the default level of
database access. Once the account creation is complete, you can use [role-based
access control
(RBAC)](/security/cloud/access-control/#role-based-access-control-rbac) to
control access for that account.


