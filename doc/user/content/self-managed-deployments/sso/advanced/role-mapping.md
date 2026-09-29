---
title: "Enable role mapping"
description: "Grant Materialize roles from IdP group memberships with the advanced SSO stack."
menu:
  main:
    parent: "enterprise-sso"
    identifier: "enterprise-sso-role-mapping"
    weight: 55
---

Materialize can automatically grant and revoke SQL role memberships based
on the `groups` claim in the JWT Hydra issues, so you manage a user's
Materialize privileges by adjusting their IdP group memberships instead of
running manual `GRANT` statements.

## Before you begin

Your IdP must send group memberships in the `groups` claim. For SAML through
Polis, add a `groups` attribute statement to the SAML app, as described in
[Sync groups](/self-managed-deployments/sso/advanced/identity-providers/#step-4-optional-sync-groups).

## Enable the sync

The feature is off by default. To enable it, set the following system
parameter on the `materialize-instance` module:

```hcl
system_parameters = {
  # ... existing OIDC params ...
  oidc_group_role_sync_enabled = "true"
}
```

Bump `force_rollout` to a new UUID and re-apply so environmentd picks up the
change.

## How it works

On each OIDC login, environmentd reads the `groups` claim from the JWT
(default claim name `groups`, configurable via `oidc_group_claim`; supports
dot-separated paths like `customClaims.groups`). For each group name it
looks up a Materialize role with the exact same name (case-sensitive):

- Roles found are granted to the user.
- Roles previously granted by the sync that are no longer in the claim are
  revoked.
- Manual `GRANT`s are never touched; the sync only manages memberships it
  granted itself, marked by an internal sentinel grantor (`mz_jwt_sync`).
- Groups matching reserved role names (`mz_`, `pg_`, `PUBLIC`) or with no
  matching Materialize role are silently skipped with a client notice.

## Set up roles

Name IdP groups to match SQL role names one-to-one. Create the Materialize
roles once as `mz_system`, along with whatever privileges the group should
carry:

```mzsql
CREATE ROLE "mz-admins";
GRANT USAGE ON CLUSTER quickstart TO "mz-admins";
GRANT USAGE ON SCHEMA materialize.public TO "mz-admins";
GRANT SELECT ON ALL TABLES IN SCHEMA materialize.public TO "mz-admins";
```

Any user whose JWT `groups` claim contains `mz-admins` is now automatically
granted the role on their next login.

## Verify the sync

To see sync-managed memberships:

```mzsql
SELECT r.name AS role, m.name AS member, g.name AS grantor
FROM mz_role_members rm
JOIN mz_roles r ON r.id = rm.role_id
JOIN mz_roles m ON m.id = rm.member
JOIN mz_roles g ON g.id = rm.grantor
WHERE g.name = 'mz_jwt_sync';
```

## Strict vs. fail-open

By default (`oidc_group_role_sync_strict = false`), a sync failure during
login is logged and delivered to the client as a notice, but the login
still proceeds with existing memberships. Set the parameter to `"true"` to
reject logins on sync failure (fail-closed) if you need stricter guarantees.

## Deprovisioning caveat

Sync is a login-time snapshot. A currently-active session keeps its role
memberships until the user re-logs in and Materialize re-evaluates the
claim. For instant revocation, terminate the user's session at the IdP and
let their next login pick up the new state.
