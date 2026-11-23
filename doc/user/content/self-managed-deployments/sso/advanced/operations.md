---
title: "Operations"
description: "Day-2 tasks for the advanced SSO stack."
menu:
  main:
    parent: "enterprise-sso"
    identifier: "enterprise-sso-operations"
    weight: 60
---

This page covers ongoing operations once the Ory stack is deployed and
your IdP is connected.

## Add additional OAuth2 clients

By default the ory-stack module registers a single OAuth2Client in Hydra for the
Materialize Console. If you have other internal applications that
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
  # Marks the client as operator-managed. Without it, the client is treated
  # like one that registered itself: its audience is limited and it can't
  # use the client_credentials grant.
  metadata:
    materialize_managed: true
  # Trusted internal apps can skip the consent screen. The user's email and
  # groups are added to every token either way.
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

## Connect MCP clients

MCP clients such as Claude Code can sign in to Materialize's MCP
endpoint through the Ory stack. They register themselves with Hydra
through Dynamic Client Registration ([RFC
7591](https://datatracker.ietf.org/doc/html/rfc7591)), which is off by
default. To turn it on, set in tfvars:

```hcl
ory_enable_dynamic_client_registration = true
```

and bump `force_rollout` so Materialize picks up the new audience. This:

- turns on registration in Hydra and advertises its endpoint in Hydra's
  discovery document;
- gives self-registered clients `https://<materialize_console_fqdn>/api/mcp`
  as their token audience, and no other;
- adds that URL to Materialize's `oidc_audience`.

Then connect the client to `https://<materialize_console_fqdn>/api/mcp/developer`
(or `/api/mcp/agent`). On first use it registers, opens the browser for
sign-in and a consent screen, and stores its token.

{{< note >}}
Anyone who can reach Hydra's public endpoint can register a client.
Registered clients still need a user to sign in, and they can only get
the MCP audience. If Hydra is reachable from the internet, put a rate
limit in front of it.
{{</ note >}}

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

Kratos stores user identities in its own PostgreSQL database. You can
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

An inactive identity can't sign in, and its refresh tokens stop working
at the next refresh, because the selfservice UI checks the identity on
every token issuance.

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
`https://<your-polis-hostname>/admin/auth/login`. Out of the box, no one
can sign in: Polis offers email magic-link login only when `NEXTAUTH_ACL`
is set, and the module leaves it empty. To sign in, either
[use SAML](#unlock-the-polis-admin-ui-without-smtp) or configure SMTP and
set `nextAuthAcl` to the admin addresses or patterns, for example
`polis = { hosted = true, nextAuthAcl = "alice@example.com,*@ops.example.com" }`
(see
[Polis hosted-mode docs](https://www.ory.sh/docs/polis/deploy/env-variables)).

### Unlock the Polis admin UI without SMTP

Once the admin UI is up, prefer it over the direct API calls in
[Configure identity providers](/self-managed-deployments/sso/advanced/identity-providers/)
for registering SAML connections and SCIM directories going forward.

The default admin login flow uses an email magic link, which requires
SMTP to be configured. If you don't want to run SMTP just to access the
admin plane, Polis exposes a built-in reserved tenant
(`_jackson_boxyhq` / `_jackson_admin_portal`) whose sole purpose is to
gate the admin UI login on a SAML IdP you already have. Registering
your customer's IdP there lets operators sign into the admin UI via
their own SSO with no mail server involved.

{{< warning >}}
Everyone the admin SAML app lets sign in becomes a Polis admin, able to
change SAML connections and SCIM directories. `NEXTAUTH_ACL` doesn't
apply to SAML admin login, so the IdP's app assignment is the only
control. Don't reuse the end-user SAML app here: that would make every
Materialize user a Polis admin.
{{< /warning >}}

Create a **separate** SAML app on the IdP side for admin access, assign
only your Polis operators to it, and register it as a connection under
the reserved tenant:

```bash
curl -sS -H "Authorization: Api-Key $POLIS_API_KEY" \
  -X POST "https://<your-polis-hostname>/api/v1/connections" \
  --data-urlencode "tenant=_jackson_boxyhq" \
  --data-urlencode "product=_jackson_admin_portal" \
  --data-urlencode "name=admin-bootstrap" \
  --data-urlencode "encodedRawMetadata=$(base64 < okta-admin-metadata.xml | tr -d '\n')" \
  --data-urlencode "defaultRedirectUrl=https://<your-polis-hostname>/admin/sso-connection" \
  --data-urlencode 'redirectUrl=["https://<your-polis-hostname>/api/auth/callback/boxyhq-saml"]'
```

Then browse to `https://<your-polis-hostname>/admin/auth/login` and
click "**Sign in with SAML**". The flow bounces through your IdP and
lands you at the Polis admin dashboard.

## Control who can sign in

Users can't create a Kratos account with an email and password: the
module turns off password login and registration and leaves SSO as the
only way in. Kratos creates an identity the first time someone signs in
through a configured identity provider. That first sign-in goes through
Kratos's registration flow, so registration has to stay enabled.

{{< warning >}}
Don't set `selfservice.flows.registration.enabled = false` in
`kratos_helm_values`. Kratos would then reject the first sign-in of every
new user, from OIDC or SAML, while users who already signed in keep
working.
{{< /warning >}}

Each provider only signs in emails in its `allowed_email_domains` (see
[Sign-in checks](/self-managed-deployments/sso/advanced/identity-providers/#sign-in-checks)).
Within those domains, anyone your identity provider lets sign in to the
Materialize app gets an account. Control that in the IdP:

- **Okta**: Okta enforces app assignment. Assign the app to the users or
  groups who should have access, not to **Everyone**.
- **Microsoft Entra ID**: on the enterprise application, set
  **Assignment required?** to **Yes**. It defaults to **No**, which lets
  every user in the tenant sign in.
- **Public OIDC providers** such as Google: anyone with an account can
  sign in unless the provider restricts it, for example to a Google
  Workspace domain.

To remove a user's access, unassign them in the IdP and
[lock their Kratos identity](#manage-kratos-identities). Their
Materialize role isn't dropped automatically.

## Upgrade from kratos-selfservice-ui-node

Before v19.0.0 of the Terraform modules, the stack ran Ory's
`kratos-selfservice-ui-node` as its selfservice UI. From v19.0.0 it runs
[`materialize/ory-selfservice`](https://github.com/MaterializeInc/ory-selfservice),
which also enforces stricter sign-in rules. To upgrade an existing
deployment:

1. Before applying, set the new inputs:
   - `allowed_email_domains` on every entry in
     `upstream_identity_providers` and `saml_providers`.
   - `require_email_verified = false` on OIDC providers that don't send
     `email_verified`, such as Microsoft Entra ID.
   - `ory_kratos_cookie_domain`. It used to be derived from the Kratos
     hostname. Use the narrowest domain that covers the Kratos and
     selfservice UI hostnames.
2. Run `terraform apply`. On AWS, the Hydra and Polis database passwords
   rotate, and the pods restart with the new credentials.
3. Have everyone sign in again through the IdP. Tokens now take the
   user's email and groups from data the IdP writes at sign-in, so until
   a user signs in again, their tokens can't be refreshed. End every
   session so the next sign-in goes to the IdP:

   ```bash
   kubectl -n ory port-forward svc/kratos-admin 4434 &
   kubectl -n ory port-forward svc/hydra-admin 4445 &
   # Past 500 identities, repeat with the page_token from the Link header.
   for id in $(curl -s 'localhost:4434/admin/identities?page_size=500' | jq -r '.[].id'); do
     curl -s -X PATCH "localhost:4434/admin/identities/$id" -H 'content-type: application/json' \
       -d '[{"op":"remove","path":"/traits/groups"}]' >/dev/null
     curl -s -X DELETE "localhost:4434/admin/identities/$id/sessions"
     curl -s -X DELETE "localhost:4445/admin/oauth2/auth/sessions/login?subject=$id"
   done
   ```

   The `PATCH` removes the old `groups` trait, which the identity schema
   no longer allows. It fails harmlessly for identities without one.

Identities, OAuth2 clients, and Materialize roles carry over unchanged.
The full list of changes is in the module's
[UPGRADING.md](https://github.com/MaterializeInc/materialize-terraform-self-managed/blob/main/kubernetes/modules/ory-selfservice-ui/UPGRADING.md).

## Back up the Ory PostgreSQL database

The Ory components (Kratos, Hydra, Polis) store identities, OAuth2
clients, sessions, SAML connections, and SCIM directory state in a
PostgreSQL database. Loss of this database means:

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
- PostgreSQL connection failures from any Ory component (suggests DB
  saturation or networking issues)
