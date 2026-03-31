---
title: "Migrate to SSO"
description: "Migrate from password authentication to OIDC-based single sign-on (SSO) in Self-Managed Materialize."
menu:
  main:
    parent: "security-sm"
    name: "Migrate to SSO"
    identifier: "sso-migration-sm"
    weight: 10
---

If you have an existing Materialize deployment using password authentication, you
can migrate to OIDC without losing access to existing roles and their owned
objects. The key is to configure `oidc_authentication_claim` so that the claim
value in the JWT matches the existing SQL username for each user.

## Step 1. Review existing roles

List the current roles in your Materialize deployment:

```mzsql
SELECT name FROM mz_roles WHERE name NOT LIKE 'mz_%';
```

Note these usernames — you will need to ensure they match the OIDC token claim
values.

## Step 2. Choose the right authentication claim

The `oidc_authentication_claim` parameter determines which JWT claim maps to the
Materialize role name. When a user authenticates via OIDC, Materialize looks up
the value of this claim and uses it as the username.

To preserve access to existing roles, set `oidc_authentication_claim` to a claim
whose value matches the existing SQL username for each user. Common options:

| Claim | Example value | When to use |
|-------|---------------|-------------|
| `email` | `alice@your-org.com` | If existing roles are named after email addresses |
| `preferred_username` | `alice` | If existing roles use short usernames |
| `sub` (default) | `auth0\|abc123` | If existing roles match the IdP's subject identifiers |

For example, if your existing roles are email addresses like
`alice@your-org.com`, set:

```mzsql
ALTER SYSTEM SET oidc_authentication_claim = 'email';
```

When `alice@your-org.com` signs in via OIDC, Materialize resolves the `email`
claim from the JWT and matches it to the existing `alice@your-org.com` role.
The user retains all privileges and object ownership from the original role.

## Step 3. Configure OIDC

Follow the steps in [Single sign-on (SSO)](/security/self-managed/sso/) to
configure your identity provider and enable OIDC authentication.

## Step 4. Verify the migration

After enabling OIDC, have each user sign in and verify their role username is the same as before.

## See also

- [Single sign-on (SSO)](/security/self-managed/sso/)
- [Authentication](/security/self-managed/authentication/)
- [Manage roles](/security/self-managed/access-control/manage-roles/)
