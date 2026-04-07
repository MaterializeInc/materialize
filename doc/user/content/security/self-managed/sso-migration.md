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

If you have an existing Materialize deployment using Password/SASL-SCRAM authentication, you
can migrate to OIDC without losing access to existing roles and their owned
objects. The key is to configure `oidc_authentication_claim` so that the value
in the JWT matches the existing Materialize user or service account's role name.

## Step 1. Identify existing roles and choose an authentication claim

Identify the login roles in your Materialize deployment:

```mzsql
SELECT name FROM mz_roles WHERE name NOT LIKE 'mz_%' AND rolcanlogin = true;
```

Users and service accounts authenticate using ID or access tokens issued by their IdP. As the admin, you need to choose the claim in these tokens whose value matches the existing role names in Materialize. The `oidc_authentication_claim` parameter tells Materialize which JWT claim to use as the role name during OIDC authentication. For more details, see [Mapping IdP Users to Materialize Roles](/security/self-managed/sso/#mapping-idp-users-to-materialize-roles).

In most cases, this will work if your existing role names are **email
addresses** (e.g., `alice@your-org.com`), since the `email` claim in the JWT
naturally matches.

If no JWT claim maps to an existing role name, you will need to recreate the
role.

## Step 2. Configure OIDC

Follow the steps in [Single sign-on (SSO)](/security/self-managed/sso/) to
configure your identity provider and enable OIDC authentication.

## Step 3. Verify the migration

After enabling OIDC, have each user sign in and verify their role name is the same as before.

## See also

- [Single sign-on (SSO)](/security/self-managed/sso/)
- [Authentication](/security/self-managed/authentication/)
- [Manage roles](/security/self-managed/access-control/manage-roles/)
