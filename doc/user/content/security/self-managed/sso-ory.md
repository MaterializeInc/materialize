---
title: "Single sign-on via Ory"
description: "User-facing reference for SSO into Self-Managed Materialize via the Ory stack."
menu:
  main:
    parent: "authentication-sm"
    name: "SSO via Ory"
    identifier: "sso-ory-sm"
    weight: 2
---

{{< public-preview />}}

This page covers the SSO usage and configuration that's specific to the
Ory-based deployment. If you're using the direct-OIDC path (Kratos not
in the picture), see the [SSO](/security/self-managed/sso/) page
instead.

The Ory stack adds three capabilities on top of the direct-OIDC path:

- **SAML SSO** via Polis, for IdPs that don't speak OIDC natively
- **SCIM provisioning** via Polis, so users are pushed automatically
  from your IdP into Polis (and onward into Materialize on first login)
- **Federation across multiple IdPs** through a single proxy, so
  Materialize sees one issuer regardless of which IdP the user came
  from

## How sign-in works

When a user opens the Materialize console:

1. The console redirects to Hydra (the OAuth2 / OIDC provider deployed
   with the Ory stack).
2. Hydra delegates login to the Kratos selfservice UI.
3. The user sees the login screen with a button per configured upstream
   provider (e.g. "Sign in with Okta", "Sign in via SAML").
4. Clicking a button kicks off the federated flow:
   - For an OIDC provider: Kratos bounces the user to the IdP's
     authorization endpoint and back.
   - For SAML via Polis: Kratos bounces to Polis, Polis bounces to the
     SAML IdP, the IdP posts the SAML assertion back to Polis, Polis
     issues an OIDC token to Kratos.
5. Kratos creates a federated identity tied to the user's email on first
   login (a Kratos record, not yet a Materialize SQL role).
6. Kratos returns to Hydra, which issues an OAuth2 access token and
   ID token to the console.
7. The console attaches the token to its API calls into Materialize
   (the SQL HTTP endpoint via balancerd, the operator API, etc.).
8. Materialize validates the token against Hydra's JWKS, reads the
   `email` claim, and either uses the existing SQL role of that name or
   creates one if it doesn't exist (JIT role creation).

The user is now logged in. The role has no privileges by default; an
admin runs `GRANT` statements separately. See
[Auto-provisioning roles](/security/self-managed/sso/#auto-provisioning-roles)
for the JIT behaviour (it's identical to the direct-OIDC path).

## Configuring authentication on the Materialize side

The deployment example wires the OIDC system parameters automatically.
For reference, what gets set:

```hcl
system_parameters = {
  oidc_issuer               = "https://<your-hydra-hostname>"
  oidc_audience             = jsonencode([<oauth2-client-id>])
  oidc_authentication_claim = "email"
  console_oidc_client_id    = <oauth2-client-id>
  console_oidc_scopes       = "openid email"
}
```

Where `<oauth2-client-id>` is the `client_id` Hydra Maester generates
for the OAuth2Client CRD that the example creates.

You can override:

- `oidc_authentication_claim` if you want to use a different JWT claim
  than `email` for the SQL role name. Most customers stick with `email`.
- `console_oidc_scopes` if you want to request additional scopes from
  the user during sign-in. The defaults (`openid email`) are the
  minimum needed for the JIT role creation to work.

Anything beyond this is handled by the deployment example. You don't
need to manage these parameters by hand unless you're deviating from
the example's default setup.

## Configuring identity providers

The IdP-side setup (creating OIDC clients, registering SAML connections
in Polis, enabling SCIM) is covered in [Configure identity providers](/self-managed-deployments/enterprise-sso/identity-providers/).

## What the Ory stack does NOT change

This part is identical to the direct-OIDC path; see the
[SSO](/security/self-managed/sso/) page for the canonical docs on each:

- How SQL clients connect using JWT tokens (`psql` with a token
  password, the Materialize CLI, application connection strings)
- How service accounts authenticate (the SQL password authentication
  path and the OAuth2 Client Credentials flow)
- How role privileges are managed (RBAC via `GRANT` and `REVOKE`, plus
  pre-provisioning if needed)
- How to retrieve a token from the Materialize console for SQL access

## Feature support

| Feature | Status |
|---|---|
| SAML SSO | Supported via Polis |
| SCIM user provisioning | Supported via Polis |
| Multiple IdPs on a single Polis tenant | Supported |
| Group membership in the JWT | Supported via SAML attribute statement. Group names are attached to the token at login as a `groups` claim, refreshed each time the user re-authenticates. |
| Group to SQL role mapping (auto-`GRANT`) | Not supported. The `groups` JWT claim reaches Materialize but admins still run `GRANT` manually to translate group membership into SQL privileges. |
| Automatic role deprovisioning | Partial. SCIM-driven IdP deactivation marks the user inactive in Polis, but the Materialize SQL role isn't dropped automatically. |
| Long-lived API keys for service accounts | Not supported today. OAuth2 client credentials is the current path for machine-to-machine auth; broader API key management is tracked as future work via [Ory Talos](https://www.ory.sh/talos). |

## See also

- [Deploying the Ory stack](/self-managed-deployments/enterprise-sso/) --
  architecture and per-cloud install guides
- [Configure identity providers](/self-managed-deployments/enterprise-sso/identity-providers/) --
  OIDC, SAML via Polis, SCIM
- [Operations](/self-managed-deployments/enterprise-sso/operations/) --
  day-2 tasks for the Ory stack
- [SSO (direct OIDC)](/security/self-managed/sso/) -- the simpler path
  when you don't need SAML, SCIM, or federation
- [RBAC](/security/self-managed/access-control/) -- granting
  privileges to SSO-authenticated users
