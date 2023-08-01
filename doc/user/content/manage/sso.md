---
title: "Single Sign-On"
description: "Configure single sign-on using SAML or Open Id Connect"
menu:
  main:
    parent: manage
    weight: 10
---

Materialize allows for single sign-on (SSO) using your existing [SAML](https://auth0.com/blog/how-saml-authentication-works/) or [Open Id Connect](https://auth0.com/intro-to-iam/what-is-openid-connect-oidc) based identity provider.
With SSO, users can log in to the Materialize console using the same ID and password as they do for other systems. 

{{< note >}}
Single sign-on in Materialize only supports authentication into the Materialize console. Permissions within the database are handled separately using [role-based access control](/manage/access-control/).
{{</ note >}}

## Prerequisites

* You must have an existing SAML or Open Id Connect based identity provider.
* Only users assigned the `OrganizationAdmin` role can view and modify SSO settings.

## SAML

1. [Login to the Materialize console](https://console.materialize.com/).

1. Navigate to Account > Account Settings > SSO.

1. Click **Add New** and choose the `SAML` connection type. 

1. Add the SSO endpoint and public certificate provided by your identify provider. Click proceed.

1. Optionally, add the SSO domain provided by your identify provider. Click proceed.

1. Select *Organization Admin* or *Organization Member* depending on what level of console access the user needs:

    - **Organization Admin**: Can invite new users, edit account information,
    and edit account security information.
    - **Organization Member**: Can login to the console.

    These roles do not refer to RBAC roles and privileges and refer to
    actions users can take in the Materialize Console.

## Open Id Connect

1. [Login to the Materialize console](https://console.materialize.com/).

1. Navigate to Account > Account Settings > SSO.

1. Click **Add New** and choose the `Open ID Connect` connection type. 

1. Add your issuer url, client id, and secret key provided by your identify provider. Click proceed.

1. Optionally, add the SSO domain provided by your identify provider. Click proceed.

1. Select *Organization Admin* or *Organization Member* depending on what level of console access the user needs:

    - **Organization Admin**: Can invite new users, edit account information,
    and edit account security information.
    - **Organization Member**: Can login to the console.

    These roles do not refer to RBAC roles and privileges and refer to
    actions users can take in the Materialize Console.
