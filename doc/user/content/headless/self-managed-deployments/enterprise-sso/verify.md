---
headless: true
---
Smoke-test each browser-facing endpoint. These commands assume a publicly trusted issuer (`cert_issuer_ref` set). With the default self-signed issuer, fetch its CA first and pass `--cacert ca.crt` to each `curl`:

```bash
kubectl -n cert-manager get secret <name_prefix>-root-ca -o jsonpath='{.data.ca\.crt}' | base64 -d > ca.crt
```


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

Then sign in end to end, which is what proves SSO works:

1. Open `https://console.example.com`. You are redirected to the selfservice UI at `auth.example.com`, with one button per `upstream_identity_providers` entry and per `saml_providers` entry. Each button's text comes from that entry's `label`; see [Configure identity providers](/self-managed-deployments/enterprise-sso/identity-providers/).
2. Sign in through one of them. You should land back in the Console as that user.
3. Run `SELECT current_user;`. It should return the user's email.
