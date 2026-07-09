---
title: "Operations"
description: "Day-2 tasks for the Ory-based enterprise SSO stack."
menu:
  main:
    parent: "enterprise-sso"
    identifier: "enterprise-sso-operations"
    weight: 60
---

This page covers ongoing operations once the Ory stack is deployed and
your IdP is connected.

## Add additional OAuth2 clients

By default the example registers a single OAuth2Client in Hydra for the
Materialize console. If you have other internal applications that
should authenticate through the same Hydra instance, you can register
additional clients using Hydra Maester's `OAuth2Client` CRDs.

Apply a manifest like:

```yaml
apiVersion: hydra.ory.sh/v1alpha1
kind: OAuth2Client
metadata:
  name: my-internal-app
  namespace: ory
spec:
  clientName: My Internal App
  grantTypes: ["authorization_code", "refresh_token"]
  responseTypes: ["code", "id_token"]
  scope: "openid profile email offline"
  redirectUris:
    - "https://my-app.example.com/auth/callback"
  secretName: my-internal-app-credentials
  tokenEndpointAuthMethod: "client_secret_basic"
  # Set skipConsent: true for first-party apps to bypass the consent screen.
  skipConsent: true
```

Hydra Maester watches for these resources and registers the client with
Hydra. The generated client_id and client_secret are written to the
`secretName` Kubernetes Secret in the same namespace.

To read the credentials:

```bash
kubectl get secret my-internal-app-credentials -n ory \
  -o jsonpath='{.data.CLIENT_ID}' | base64 -d
kubectl get secret my-internal-app-credentials -n ory \
  -o jsonpath='{.data.CLIENT_SECRET}' | base64 -d
```

## Rotate the license key

When your Materialize license key approaches expiry or you receive a
new one with updated entitlements:

1. Update `license_key` in `terraform.tfvars`.
2. Run `terraform apply`.

The `imagePullSecret` in the `ory` namespace gets updated with the new
JWT. Pods don't roll automatically, but the next time they restart (or
the next image pull) they'll use the new credentials. To force an
immediate roll:

```bash
kubectl rollout restart deployment kratos hydra ory-selfservice-ui -n ory
kubectl rollout restart deployment polis -n ory   # only if enable_polis
```

The Materialize side also picks up the new key on the next operator
reconcile.

## Manage Kratos identities

Kratos stores user identities in its own Postgres database. You can
inspect and manage them via Kratos's admin API.

Get the in-cluster admin URL:

```bash
kubectl port-forward -n ory svc/kratos-admin 4434:4434
```

List identities:

```bash
curl -s http://localhost:4434/admin/identities | jq .
```

Get a specific identity:

```bash
curl -s http://localhost:4434/admin/identities/<id> | jq .
```

Lock a user out (disable login):

```bash
curl -X PATCH http://localhost:4434/admin/identities/<id> \
  -H "Content-Type: application/json-patch+json" \
  -d '[{"op": "replace", "path": "/state", "value": "inactive"}]'
```

See the [Kratos admin API
reference](https://www.ory.sh/docs/kratos/reference/api) for the full
set of operations.

## Manage Polis SAML connections and SCIM directories

Polis admin operations go through its admin API. The Polis admin web UI
is not exposed by default (the module sets `hosted = false`).

Get the admin API key:

```bash
POLIS_API_KEY=$(kubectl get secret -n ory polis-config \
  -o jsonpath='{.data.API_KEYS}' | base64 -d)
```

List SAML connections:

```bash
curl -s -H "Authorization: Api-Key $POLIS_API_KEY" \
  "https://<your-polis-hostname>/api/v1/sso?tenant=<customer-name>&product=materialize" | jq .
```

Update a SAML connection (for example, to refresh the IdP metadata XML
after a certificate rotation):

```bash
curl -X PATCH "https://<your-polis-hostname>/api/v1/sso?tenant=<customer-name>&product=materialize&name=okta-saml" \
  -H "Authorization: Api-Key $POLIS_API_KEY" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "rawMetadata=$(cat new-saml-metadata.xml)"
```

List SCIM directories:

```bash
curl -s -H "Authorization: Api-Key $POLIS_API_KEY" \
  "https://<your-polis-hostname>/api/v1/dsync?tenant=<customer-name>&product=materialize" | jq .
```

Delete a SCIM directory (use cautiously, this disconnects the IdP push
target):

```bash
curl -X DELETE -H "Authorization: Api-Key $POLIS_API_KEY" \
  "https://<your-polis-hostname>/api/v1/dsync?tenant=<customer-name>&product=materialize&directoryId=<id>"
```

If you want to enable the multi-tenant admin UI for hands-on
management instead of the API, set in tfvars:

```hcl
polis_helm_values = {
  polis = { hosted = true }
}
```

and re-apply. The UI is then available at
`https://<your-polis-hostname>/admin/auth/login`. You'll need to
configure a NextAuth provider for login (see
[Polis hosted-mode docs](https://www.ory.sh/docs/polis/deploy/env-variables)).

### Unlock the Polis admin UI without SMTP

The default admin login flow uses an email magic link, which requires
SMTP to be configured. If you don't want to run SMTP just to access the
admin plane, Polis exposes a built-in reserved tenant
(`_jackson_boxyhq` / `_jackson_admin_portal`) whose sole purpose is to
gate the admin UI login on a SAML IdP you already have. Registering
your customer's IdP there lets operators sign into the admin UI via
their own SSO with no mail server involved.

Reuse the same Okta (or other) SAML app you configured for user login,
then register a second connection under the reserved tenant:

```bash
curl -sS -H "Authorization: Api-Key $POLIS_API_KEY" \
  -X POST "https://<your-polis-hostname>/api/v1/connections" \
  --data-urlencode "tenant=_jackson_boxyhq" \
  --data-urlencode "product=_jackson_admin_portal" \
  --data-urlencode "name=admin-bootstrap" \
  --data-urlencode "encodedRawMetadata=$(base64 < okta-metadata.xml | tr -d '\n')" \
  --data-urlencode "defaultRedirectUrl=https://<your-polis-hostname>/admin/sso-connection" \
  --data-urlencode 'redirectUrl=["https://<your-polis-hostname>/api/auth/callback/boxyhq-saml"]'
```

Then browse to `https://<your-polis-hostname>/admin/auth/login` and
click "**Sign in with SAML**". The flow bounces through your IdP and
lands you at the Polis admin dashboard.

For a production deployment, register a **separate** SAML app on the
IdP side for admin access (rather than reusing the end-user app), so
you can control admin membership independently.

## Disable registration in Kratos

By default Kratos allows users to register new identities through the
selfservice UI. For production deployments where users come exclusively
from your IdP, disable registration via Helm values in tfvars:

```hcl
kratos_helm_values = {
  kratos = {
    config = {
      selfservice = {
        flows = {
          registration = {
            enabled = false
          }
        }
      }
    }
  }
}
```

Apply. The "Sign up" link disappears from the login screen.

## Back up the Ory Postgres database

The Ory components (Kratos, Hydra, Polis) store identities, OAuth2
clients, sessions, SAML connections, and SCIM directory state in a
Postgres database. Loss of this database means:

- All Kratos identities are gone (users will need to re-register / be
  re-provisioned via SCIM on next login)
- All issued Hydra tokens are invalidated
- All Polis SAML connections and SCIM directories have to be re-created

Each cloud's database module enables managed automated backups by
default (`backup_retained_backups = 35` on Cloud SQL,
`backup_retention_days = 35` on Flexible Server and RDS). For
production deployments, verify the retention period matches your DR
requirements and consider running periodic restore drills to confirm
the backups are usable.

## Monitor the stack

If you deployed with `enable_observability = true` (the default), the
example provisions Prometheus and Grafana scraping Materialize and the
Ory pods. The Ory Helm charts emit standard metrics including request
counts and latencies per endpoint, which you can wire into your
existing alerting.

Key signals to alert on:

- Hydra `/oauth2/token` 5xx rate (token issuance failing)
- Kratos `/sessions/whoami` 5xx rate (session validation failing)
- Polis OIDC callback errors (SAML assertions failing)
- Pod restart counts on any Ory component
- Postgres connection failures from any Ory component (suggests DB
  saturation or networking issues)

## Future work

Items tracked but not yet shipped:

- **API key management for service accounts** via Ory Talos
  is tracked as future work. When this lands, this page will gain a section
  on issuing and revoking API keys for non-interactive clients.
- **IdP group to SQL role mapping**. Today, group memberships push to
  Polis but don't translate to Materialize SQL role grants. The
  end-to-end automation is open product work.
- **Multi-IdP support on a single Polis tenant**. Polis supports
  multiple SAML connections per tenant, but the example doesn't
  document the multi-tenant Polis setup yet. Useful for customers with
  parallel IdPs (e.g., one for employees, one for contractors).
