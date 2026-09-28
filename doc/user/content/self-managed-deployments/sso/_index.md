---
title: "Configure single sign-on"
description: "Choose between OIDC and advanced single sign-on (SSO) for Self-Managed Materialize."
disable_list: true
menu:
  main:
    parent: "sm-deployments"
    identifier: "sm-sso"
    weight: 60
---

{{< public-preview />}}

Self-Managed Materialize supports two ways to set up single sign-on (SSO).
Both let users sign in through your identity provider (IdP) instead of
managing passwords in Materialize.

| Option | What it supports | What you deploy |
|---|---|---|
| [Configure OIDC](/self-managed-deployments/sso/oidc/) | OIDC sign-in against an OIDC-capable IdP, and group-to-role mapping if your IdP adds a groups claim | Nothing extra. You set Materialize's OIDC parameters to point at your IdP. |
| [Advanced SSO](/self-managed-deployments/sso/advanced/) | OIDC, SAML, SCIM provisioning, and group-to-role mapping | A Terraform-managed stack, powered by Ory, that acts as the OIDC issuer in front of Materialize. |

## When to use each option

| You need... | Use |
|---|---|
| OIDC against an OIDC-capable IdP (Okta OIDC, Google Workspace, Auth0 OIDC) | [Configure OIDC](/self-managed-deployments/sso/oidc/) |
| SAML against a SAML-only IdP (Entra SAML, ADFS, Auth0 SAML, Okta SAML) | [Advanced SSO](/self-managed-deployments/sso/advanced/) with Polis enabled |
| SCIM provisioning from your IdP | [Advanced SSO](/self-managed-deployments/sso/advanced/) with Polis enabled |
| One SSO endpoint so you can swap IdPs without changing Materialize's configuration | [Advanced SSO](/self-managed-deployments/sso/advanced/) |

Start with OIDC if your IdP supports it and you don't need SAML or SCIM.
Advanced SSO is a superset of the OIDC option, so you can move to it later
without rebuilding your Materialize deployment.
