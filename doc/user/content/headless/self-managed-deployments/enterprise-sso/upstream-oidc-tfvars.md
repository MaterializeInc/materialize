---
headless: true
---
To federate logins through one or more upstream OIDC providers (Okta, Google Workspace, Auth0, Entra), add an `upstream_oidc_providers` list. Each entry renders as a "Sign in with ..." button on the selfservice UI:

```hcl
upstream_oidc_providers = [
  {
    id            = "okta"
    provider      = "generic"
    client_id     = "<from your IdP>"
    client_secret = "<from your IdP>"
    issuer_url    = "https://your-org.okta.com/oauth2/default"
    scope         = ["openid", "email", "profile"]
    label         = "Sign in with Okta"
  },
]
```

Register the redirect URI `https://<ory_kratos_fqdn>/self-service/methods/oidc/callback/<id>` at the upstream IdP. See [Configure identity providers](/self-managed-deployments/enterprise-sso/identity-providers/) for SAML and SCIM setup once the stack is up.
