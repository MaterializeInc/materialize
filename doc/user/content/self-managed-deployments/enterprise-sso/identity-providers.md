---
title: "Configure identity providers"
description: "Wire your IdP into the Ory-based enterprise SSO stack via OIDC, SAML, or SCIM."
menu:
  main:
    parent: "enterprise-sso"
    identifier: "enterprise-sso-identity-providers"
    weight: 50
---

Once the Ory stack is deployed, you connect it to your identity
provider. There are three paths, depending on what your IdP supports
and what you need.

| Path | Use when |
|---|---|
| [Direct OIDC](#direct-oidc) | Your IdP speaks OIDC and you only need authentication |
| [SAML via Polis](#saml-via-polis) | Your IdP only speaks SAML, or you want a single proxy in front of multiple IdPs |
| [SCIM via Polis](#scim-via-polis) | You want users provisioned and deactivated automatically from your IdP |

The SAML and SCIM paths require `enable_polis = true` in your tfvars.
SCIM is layered on top of SAML; you need the SAML connection registered
in Polis first.

## Direct OIDC

The simplest path. Kratos federates upstream OIDC providers directly,
no Polis required. Use this when your IdP supports OIDC and you don't
need SCIM provisioning.

### Step 1. Create an OIDC application in your IdP

Create a new application with these settings:

- **Sign-in redirect URI**: `https://<your-kratos-hostname>/self-service/methods/oidc/callback/<id>`
  where `<id>` matches the entry you'll add to tfvars (e.g. `okta`,
  `entra`, `google`).
- **Grant types**: Authorization Code
- **Scopes**: `openid`, `email`, `profile`

For Okta specifically: Applications → Create App Integration → OIDC →
Web Application → set the redirect URI as above.

Grab the **Issuer URL**, **Client ID**, and **Client Secret** from the
app's settings.

### Step 2. Add the provider to tfvars

Add an entry to `upstream_oidc_providers` in your `terraform.tfvars`:

```hcl
upstream_oidc_providers = [
  {
    id            = "okta"
    provider      = "generic"
    client_id     = "<from Okta>"
    client_secret = "<from Okta>"
    issuer_url    = "https://your-org.okta.com/oauth2/default"
    scope         = ["openid", "email", "profile"]
    label         = "Sign in with Okta"
  },
]
```

Run `terraform apply`. Kratos will reload its config and pick up the new
provider. The label text becomes the button on the login screen.

### Step 3. Test the login

Open the Materialize console in an incognito window:
`https://<your-console-hostname>`. You should see the Kratos login
screen with a "Sign in with Okta" button. Click it; you'll bounce
through your IdP and land in the Materialize console signed in.

## SAML via Polis

Use this when your IdP only supports SAML (Entra SAML, ADFS, Okta SAML,
Auth0 SAML), or when you want a single SSO proxy in front of multiple
IdPs.

The flow at runtime: console → Hydra → Kratos UI → "Sign in via SAML"
button → Polis → your IdP SAML → back through Polis → Kratos issues a
federated identity → Hydra issues an OAuth2 token → console.

### Step 1. Create a SAML application in your IdP

In your IdP, create a SAML 2.0 application with:

- **ACS URL / Assertion consumer URL**:
  `https://<your-polis-hostname>/api/oauth/saml`
- **Audience URI / Entity ID**: `https://saml.boxyhq.com`
- **NameID format**: EmailAddress
- **Attribute statements**: at minimum, `firstName`, `lastName`, `email`
  mapped from the IdP user profile fields.

Save and grab the SAML metadata URL (or download the metadata XML).
Assign users (or groups) to the app so they can authenticate through it.

### Step 2. Get the Polis admin API key

```bash
POLIS_API_KEY=$(kubectl get secret -n ory polis-config \
  -o jsonpath='{.data.API_KEYS}' | base64 -d)
```

### Step 3. Register the SAML connection in Polis

If the IdP exposes a publicly-fetchable metadata URL:

```bash
curl -X POST https://<your-polis-hostname>/api/v1/sso \
  -H "Authorization: Api-Key $POLIS_API_KEY" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "tenant=<customer-name>" \
  --data-urlencode "product=materialize" \
  --data-urlencode "name=<idp-name>-saml" \
  --data-urlencode "redirectUrl=https://<your-kratos-hostname>/self-service/methods/oidc/callback/polis" \
  --data-urlencode "defaultRedirectUrl=https://<your-console-hostname>" \
  --data-urlencode "metadataUrl=https://<idp-metadata-url>"
```

If the metadata URL is gated by API auth (Okta integrator orgs, for
example), POST the raw XML instead:

```bash
curl -X POST https://<your-polis-hostname>/api/v1/sso \
  -H "Authorization: Api-Key $POLIS_API_KEY" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "tenant=<customer-name>" \
  --data-urlencode "product=materialize" \
  --data-urlencode "name=<idp-name>-saml" \
  --data-urlencode "redirectUrl=https://<your-kratos-hostname>/self-service/methods/oidc/callback/polis" \
  --data-urlencode "defaultRedirectUrl=https://<your-console-hostname>" \
  --data-urlencode "rawMetadata=$(cat saml-metadata.xml)"
```

The response contains a `clientID` and `clientSecret`. Save them for the
next step.

Verify the connection landed:

```bash
curl -s -H "Authorization: Api-Key $POLIS_API_KEY" \
  "https://<your-polis-hostname>/api/v1/sso?tenant=<customer-name>&product=materialize" | jq .
```

### Step 4. Wire Polis into Kratos as an upstream OIDC provider

Polis acts as an OIDC provider to Kratos. Add an entry to
`upstream_oidc_providers` in tfvars, alongside any direct OIDC entries:

```hcl
upstream_oidc_providers = [
  {
    id            = "polis"
    provider      = "generic"
    client_id     = "<clientID from Step 3>"
    client_secret = "<clientSecret from Step 3>"
    issuer_url    = "https://<your-polis-hostname>"
    scope         = ["openid", "email", "profile"]
    label         = "Sign in via SAML"
  },
]
```

Run `terraform apply`. The "Sign in via SAML" button will appear on the
Kratos login screen.

### Step 5. Test the SAML login

Open the Materialize console in an incognito window. Click **Sign in via
SAML**. You'll bounce through Polis to your IdP, authenticate, and land
in the Materialize console. The federated identity is created in Kratos
on first login, and Materialize JIT-creates the SQL role from your email
claim.

## SCIM via Polis

Adds automatic user provisioning and deactivation from your IdP to
Materialize. Build on top of the SAML setup above.

### Step 1. Create a SCIM directory in Polis

```bash
curl -X POST https://<your-polis-hostname>/api/v1/dsync \
  -H "Authorization: Api-Key $POLIS_API_KEY" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "tenant=<customer-name>" \
  --data-urlencode "product=materialize" \
  --data-urlencode "name=<idp-name>-scim" \
  --data-urlencode "type=okta-scim-v2"
```

For other IdPs, change `type` to `azure-scim-v2`, `onelogin-scim-v2`,
or `generic-scim-v2`.

Save the `scim.endpoint` URL and `scim.secret` bearer token from the
response. Both go into your IdP's SCIM configuration.

### Step 2. Enable SCIM provisioning in your IdP

Steps for Okta (other IdPs vary in wording but follow the same shape):

1. Open the SAML application you created above.
2. **General** tab → Provisioning → switch to "SCIM" → Save.
3. A **Provisioning** tab appears. Click into it → **Integration**:
   - SCIM connector base URL: paste `scim.endpoint`
   - Unique identifier for users: `email`
   - Supported provisioning actions: Push New Users, Push Profile
     Updates, Push Groups
   - Authentication Mode: HTTP Header
   - Authorization: paste `scim.secret`
   - **Test Connector Configuration** -- expect green checks
4. **Provisioning** → **To App** → Edit → enable Create Users, Update
   User Attributes, Deactivate Users. Save.

### Step 3. Verify the push

Currently-assigned users push immediately. New users push on assignment.

```bash
DIRECTORY_ID=<id from the Step 1 response>
curl -s -H "Authorization: Api-Key $POLIS_API_KEY" \
  "https://<your-polis-hostname>/api/v1/dsync/users?tenant=<customer-name>&product=materialize&directoryId=$DIRECTORY_ID" | jq .
```

You should see the assigned users with their email, name, and external
ID populated.

{{< note >}}
**Existing assignments don't backfill on enable.** If a user was
assigned to the SAML app before SCIM was turned on, Okta doesn't
re-push them. Either unassign and reassign the user (the cleanest
trigger), or push manually via Okta admin → Directory → People → the
user → Applications → "..." → Push profile updates.
{{</ note >}}

### Step 4. (Optional) Sync groups

Group memberships flow through the stack in two independent ways:

1. **SAML attribute statement**: on each login, the IdP attaches the
   user's group memberships to the SAML assertion. Polis passes them
   through as an OIDC claim, Kratos writes them onto the identity
   trait, and the Ory stack embeds them in the JWT as a `groups` claim
   that Materialize can read. **Refreshed on every login.**
2. **SCIM directory push**: the IdP synchronizes group entities and
   memberships into Polis's directory. **Refreshed continuously**, but
   doesn't itself change the contents of a JWT already in flight; the
   user has to log in again to see updates.

For the SAML attribute path (recommended):

1. In your SAML app configuration, add a `groups` attribute statement
   (Okta: Sign On tab → Attribute Statements → legacy Group Attribute
   Statements → Name: `groups`, Filter: `Matches regex .*`).
2. Log in via the console. The `groups` JWT claim will contain the
   user's group names.

For the SCIM directory push (audit + future integration):

1. Create a group in your IdP and add users to it.
2. Open the SAML app → **Push Groups** tab → "Push Groups" → "by name"
   → search and add the group → save.

Confirm the push landed in Polis:

```bash
curl -s -H "Authorization: Api-Key $POLIS_API_KEY" \
  "https://<your-polis-hostname>/api/v1/dsync/groups?tenant=<customer-name>&product=materialize&directoryId=$DIRECTORY_ID" | jq .
```

Neither path today translates group membership into automatic `GRANT`
statements on the Materialize side. Admins still run
`GRANT role_name TO "user@email"` manually; the `groups` JWT claim is
available for downstream tooling to consume once that automation lands.

## What happens when users sign in

The Ory stack issues OIDC tokens with the user's email as the `email`
claim. Materialize is configured with `oidc_authentication_claim =
"email"`, so:

- **First login**: Materialize creates a SQL role named after the user's
  email (JIT role creation). The role has no privileges by default.
- **Subsequent logins**: Same role is reused.
- **Deprovisioned in IdP**: SCIM deactivates the user in Polis, but the
  Materialize SQL role isn't dropped automatically. You'll need to
  `DROP ROLE "user@email"` separately or let it become a stale (but
  inactive) record.

To grant privileges, see the role / permission management docs at
[RBAC](/security/self-managed/access-control/).

## See also

- [SSO (direct OIDC)](/security/self-managed/sso/) -- the simpler path
  for OIDC-only deployments
- [Operations](/self-managed-deployments/enterprise-sso/operations/) -- day-2: rotating credentials, adding
  OAuth2 clients
- [Troubleshooting](/self-managed-deployments/enterprise-sso/troubleshooting/) -- common errors during the
  IdP-side setup
