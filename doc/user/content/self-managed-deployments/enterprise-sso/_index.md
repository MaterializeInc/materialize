---
title: "Enterprise SSO (Ory)"
description: "Deploy Materialize Self-Managed with the Ory stack for SAML, SCIM, and federated SSO."
disable_list: true
menu:
  main:
    parent: "sm-deployments"
    identifier: "enterprise-sso"
    weight: 60
---

{{< public-preview />}}

Self-Managed Materialize ships with a built-in OIDC authentication path
documented at [SSO](/security/self-managed/sso/). For customers who need
**SAML**, **SCIM provisioning**, or **federation through an IdP-agnostic
proxy**, Materialize provides an additional Terraform-managed Ory stack
that sits in front of the Materialize console and acts as the OIDC issuer.

This section walks through deploying that stack, configuring it against
your identity provider, and operating it day to day.

## When to use the Ory-based stack

| You need... | Use |
|---|---|
| OIDC against an OIDC-capable IdP (Okta OIDC, Google Workspace, Auth0 OIDC) | [Direct OIDC SSO](/security/self-managed/sso/) |
| SAML against a SAML-only IdP (Entra SAML, ADFS, Auth0 SAML, Okta SAML) | Ory stack with Polis enabled |
| SCIM provisioning from your IdP into Materialize | Ory stack with Polis enabled |
| One IdP-agnostic SSO endpoint so you can swap IdPs without changing Materialize config | Ory stack |
| API key management for service accounts beyond OAuth2 client credentials | Ory stack (future) |

The Ory-based stack is a superset of the direct-OIDC path. If you start
with direct OIDC and later need SAML or SCIM, you can migrate without
rebuilding your Materialize deployment.

## Architecture

```
Your IdP (Okta / Entra / Auth0 / ...)
       |
       |-- SAML auth + SCIM provisioning   (Polis path)
       v
   Polis (SAML-to-OIDC bridge, optional)
       |-- OIDC
       v
   Kratos (identity management)
       |
       v
   Selfservice UI  ----- consent flow -----> Hydra (OAuth2 / OIDC provider)
                                              |
                                              v
                                       Materialize console
                                       (validates Hydra JWTs)
```

| Component | Role |
|---|---|
| **Kratos** | Stores identities, runs the login flow, federates upstream OIDC providers |
| **Hydra** | OAuth2 + OIDC authorization server that Materialize talks to; issues JWTs |
| **Selfservice UI** | Renders login and consent pages, mediates between the browser and Kratos/Hydra |
| **Polis** | Optional SAML-to-OIDC translator and SCIM endpoint for customers whose IdPs don't speak OIDC natively |
| **Materialize** | The protected application; trusts Hydra's JWTs and JIT-creates SQL roles from the `email` claim |

Each component is deployed and managed by the Terraform modules in
[`materialize-terraform-self-managed`](https://github.com/MaterializeInc/materialize-terraform-self-managed).
The composite `ory-stack` module wires them together and exposes the
integration with your Materialize instance (OAuth2 client registration,
network policies, console TLS).

## What gets deployed

When you apply one of the enterprise examples, Terraform stands up:

- A Kubernetes cluster (AKS / GKE / EKS) sized for both Materialize and Ory
- A Materialize Postgres instance (Cloud SQL / Flexible Server / RDS)
- A separate Postgres instance (or set of databases on a shared instance,
  depending on cloud) for Kratos, Hydra, and Polis
- Object storage for Materialize's persistence backend
- The Materialize operator and a Materialize instance CR
- Kratos, Hydra, and the selfservice UI in the `ory` namespace
- Optional: Polis in the same namespace, with its own TLS termination
  proxy
- cert-manager, with either a self-signed or BYO ClusterIssuer for the
  browser-facing TLS certificates
- Optional: Prometheus and Grafana for observability

## Reading order

If you're planning a deployment for the first time, work through these
pages in order:

1. **[Prerequisites](/self-managed-deployments/enterprise-sso/prerequisites/)** -- license key, DNS, cert-manager, Polis-specific requirements
2. **Install on [Azure](/self-managed-deployments/enterprise-sso/install-on-azure/), [GCP](/self-managed-deployments/enterprise-sso/install-on-gcp/), or [AWS](/self-managed-deployments/enterprise-sso/install-on-aws/)** -- pick your cloud and walk through the example
3. **[Configure identity providers](/self-managed-deployments/enterprise-sso/identity-providers/)** -- direct OIDC, SAML via Polis, SCIM provisioning
4. **[Operations](/self-managed-deployments/enterprise-sso/operations/)** -- day-2 tasks: rotating credentials, adding OAuth2 clients, managing identities
5. **[Troubleshooting](/self-managed-deployments/enterprise-sso/troubleshooting/)** -- common errors and fixes

## See also

- [SSO (direct OIDC)](/security/self-managed/sso/) -- the simpler path
  when your IdP speaks OIDC
- [Ory documentation](https://www.ory.sh/docs/) -- upstream component
  reference
