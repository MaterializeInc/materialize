---
headless: true
---
Smoke-test each browser-facing endpoint:

```bash
# Hydra OIDC discovery (issuer should match ory_hydra_fqdn)
curl -fsSL https://hydra.example.com/.well-known/openid-configuration | jq .issuer

# Kratos health
curl -fsSL https://kratos.example.com/health/ready

# Selfservice UI health
curl -fsSL https://auth.example.com/health/alive

# Polis health (only when enable_polis = true)
curl -fsSL https://polis.example.com/api/health

# Materialize console (expect HTTP 200)
curl -fsSL -o /dev/null -w "%{http_code}\n" https://console.example.com
```

Open `https://console.example.com` in a browser. You should land on the Kratos login screen. If you configured `upstream_oidc_providers`, a "Sign in with ..." button is rendered per provider. With Polis enabled and a SAML connection registered, a "Sign in via SAML" button is rendered as well.
