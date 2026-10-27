# Group-to-Role Mapping for Self-Managed

## Associated:
1. [SSO For Self Managed](https://www.notion.so/materialize/SSO-for-Self-Managed-2a613f48d37b806f9cb2d06914454d15)
2. [SCIM](https://www.notion.so/materialize/SCIM-33513f48d37b8086be2ad9ec2a9ad554)

## The Problem

Self-managed Materialize customers using OIDC SSO have no way to manage database permissions from their identity provider. Today, an admin must:

1. Configure OIDC so users can log in (already supported)
2. Manually run `GRANT` statements in Materialize for every user

When a team member joins, leaves, or changes roles, the admin has to update permissions in both the IdP *and* Materialize separately. There's no connection between "Alice is in the analytics team in Okta" and "Alice can read the orders table in Materialize."

## Success Criteria

- Users' database role memberships are dynamically updated based on their IdP group memberships
- Self-managed OIDC is the primary target
- When a user's groups change in the IdP, their Materialize permissions update on next connection
- Permissions manually set for a user are left unchanged (independent of sync)
- Admins can configure whether sync failures block login (fail-open by default, fail-closed for strict compliance)
- The design must not lock into any single identity provider (e.g., Frontegg); SCIM for self-managed is a near-term priority

## Out of Scope

- Auto-creating database roles from IdP groups
- Syncing role *privileges* (GRANTs on objects) from the IdP, only role *membership*
- Real-time push-based sync (SCIM webhook to Materialize), we sync on connection. This is standard practice (CockroachDB, PostgreSQL LDAP) and aligns with customer expectations. A limitation is that the console and internal tables only reflect state as of the user's last connection.
- Revoking the user's role itself when removed from IdP (existing token-expiry behavior handles this)
- Cloud / Frontegg support (future work, but the design intentionally avoids Frontegg-specific coupling to support self-managed SCIM as a near-term follow-up)

### Future Goals

- Admin controls the mapping between external group names and database roles (name remapping via regex transforms, e.g., `materialize_(.*)` -> `$1`)
- Cloud / Frontegg group sync
- Mid-session sync on token refresh
- SCIM-based group provisioning for self-managed (complementary to JWT-based sync)

## Solution Proposal

Follow the CockroachDB pattern: extract groups from JWT claims, match group names directly to database role names (case-insensitive), sync memberships at session startup.

### User Experience

#### Admin Setup

**Step 1: Configure IdP to include groups in the JWT.**
In their IdP (Okta, Azure AD, etc.), the admin configures the OIDC application to include a `groups` claim in the access token.

Example JWT payload after configuration:
```json
{
  "sub": "alice@example.com",
  "iss": "https://dev-123456.okta.com/oauth2/default",
  "groups": ["analytics", "platform_eng"],
  "exp": 1234567890
}
```

**Step 2: Create matching roles in Materialize with privileges.**
```sql
CREATE ROLE analytics;
GRANT SELECT ON TABLE orders, customers TO analytics;
GRANT USAGE ON SCHEMA production TO analytics;

CREATE ROLE platform_eng;
GRANT ALL ON SCHEMA infrastructure TO platform_eng;
```

The role names must match the IdP group names (case-insensitive).

**Step 3: Enable group sync.**
```sql
ALTER SYSTEM SET oidc_group_role_sync_enabled = true;
-- Optional: change the claim name if your IdP uses something other than "groups"
-- ALTER SYSTEM SET oidc_group_claim = 'groups';
-- Optional: require group sync to succeed for login (fail-closed mode for strict compliance)
-- ALTER SYSTEM SET oidc_group_role_sync_strict = true;
```
#### End User Experience

Alice logs in via `psql` with her JWT. On connection:
- Her user role `alice` is auto-provisioned if it doesn't exist (existing behavior)
- The sync reads `groups: ["analytics", "platform_eng"]` from her JWT
- Materialize internally runs `GRANT analytics TO alice` and `GRANT platform_eng TO alice`
- Alice can now query tables she has access to through those roles

Later, Alice moves to a different team. Her IdP admin removes her from `analytics` and adds her to `data_eng`. On her next login:
- The sync sees her JWT now has `groups: ["data_eng", "platform_eng"]`
- Materialize revokes `analytics` from Alice and grants `data_eng`
- Her permissions update automatically

If an admin also manually ran `GRANT reporting TO alice`, that grant is unaffected by sync: manual grants are never touched.

### Where Groups Come From

Groups come from a configurable JWT claim (default: `"groups"`). The claim value can be a JSON array of strings or a single string.

### Configuration

New system variables (set via `ALTER SYSTEM SET`, stored in `SystemVars`):

```sql
ALTER SYSTEM SET oidc_group_role_sync_enabled = true;
ALTER SYSTEM SET oidc_group_claim = 'groups';
ALTER SYSTEM SET oidc_group_role_sync_strict = false;  -- default: fail-open
```

| Variable | Default | Description |
|---|---|---|
| `oidc_group_role_sync_enabled` | `false` | Feature gate for group-to-role sync |
| `oidc_group_claim` | `'groups'` | JWT claim name containing group memberships |
| `oidc_group_role_sync_strict` | `false` | When `true`, login is rejected if sync fails (fail-closed). When `false`, sync errors are logged but login proceeds (fail-open). |

Group names from the JWT are matched directly to Materialize role names (case-insensitive). Roles must be pre-created in Materialize; group sync does not auto-create roles (only auto-creates users). Pre-created roles serve as the allowlist for IdP groups. Roles prefixed with `mz_` or `pg_` are always excluded from sync to prevent privilege escalation to system roles.

### Sync Logic

The sync happens in `handle_startup_inner()`, after the user role is auto-provisioned but before the session is fully established.

```
1. Extract groups from JWT claims (via configured claim name)
2. Normalize: lowercase, deduplicate, sort
3. Look up each group name as a role name in the catalog (skip non-existent with warning). Reject any group that maps to a role prefixed with `mz_` or `pg_` (log warning).
4. Get user's current RoleMembership.map from catalog
5. Partition current memberships:
   - sync_granted: entries where grantor == MZ_JWT_SYNC_ROLE_ID (sentinel)
   - manual_granted: entries where grantor != sentinel (leave untouched)
6. Diff against sync_granted only:
   - To grant: target roles NOT in sync_granted AND NOT in manual_granted
   - To revoke: sync_granted roles NOT in target roles
7. Execute GRANTs (with sentinel grantor) and REVOKEs via catalog_transact
```

Note: if a role is already manually granted, we don't also sync-grant it. The manual grant takes precedence and won't be revoked by sync.

### How Manual Grants Are Preserved (Grantor Field)

We need to distinguish "roles granted by JWT sync" from "roles granted manually by SQL". We only revoke roles that were originally granted by the sync mechanism.

The **grantor** field already exists and is persisted in `RoleMembership`. Today it records who ran the `GRANT` statement. We use a **sentinel grantor** to mark sync-granted memberships:

- JWT group sync grants a role → grantor = `MZ_JWT_SYNC_ROLE_ID` (a new dedicated sentinel role)
- Human runs `GRANT role TO user` → grantor = their own role ID

**Note**: `RoleMembership.map` is `BTreeMap<RoleId, RoleId>` each role membership stores exactly one grantor. A role cannot simultaneously have both a manual grant and a sync grant; only one grantor is recorded. This means "manual wins" semantics must be enforced by the sync logic's ordering, not by the data model. Enforced by: sync checking the current grantor for each role membership before acting; if a role is already granted manually, the sync skips it entirely (to not overwrite the grantor); if a role is granted with the sentinel grantor (sync) and an admin later manually re-grants it, the manual `GRANT` overwrites the grantor to the admin's role ID.

If the admin later `REVOKE`s that manual grant, the role membership is removed entirely. On next login, the sync sees the role is absent and would re-grants it with the sentinel grantor.

### Security: Shadowed Permissions

Manually-granted permissions that coexist with sync-managed memberships create a risk of **shadowed permissions**: a user may retain access through a manual grant even after their IdP group membership is revoked. This is a known trade-off of the "manual grants are never touched" design.

To mitigate this:
- The `oidc_group_role_sync_strict` mode (when enabled) rejects login if sync fails, preventing stale permissions from persisting silently.
- Admins should audit manual grants periodically. The sentinel grantor (`MZ_JWT_SYNC_ROLE_ID`) makes it possible to distinguish sync-managed from manual memberships via `mz_role_members`.

### Edge Cases

The default behavior is fail-open: send a NOTICE to the client and skip on misconfiguration, allowing login to proceed. When `oidc_group_role_sync_strict = true`, sync failures reject the login instead.

**Group maps to non-existent role**: Send a NOTICE to the client (e.g., `NOTICE: group "foo" has no matching Materialize role, skipping`), emit a server log warning, and skip. No audit log entry for unmatched groups — the audit log only records actual GRANT/REVOKE operations.

**Missing groups claim vs empty groups claim**: These are different.
- `groups: []` (explicit empty) means revoke all sync-granted roles, keep manual grants. The user proceeds with whatever manual grants and default privileges they have (same as any user without synced roles).
- No `groups` claim at all means skip sync entirely, preserve current state. This prevents IdP misconfiguration from stripping all roles.

**Circular membership**: Pre-check for cycles before building ops. Skip with warning.

**Reserved/system roles**: Pre-filter target roles to skip any role prefixed with `mz_` or `pg_` with a warning. This prevents IdP groups from escalating to system-level privileges.

**All groups map to non-existent roles**: Login proceeds. User still has manual grants.

**Case sensitivity**: Normalize group names to lowercase for matching against catalog role names.

Note: Roles prefixed with `mz_` or `pg_` are reserved system roles and must not be synced from IdP groups (denied).

### Observability

For MVP, sync activity is surfaced through:

- **`mz_audit_log`**: All GRANTs and REVOKEs from sync are logged via `Op::GrantRole`/`Op::RevokeRole`. The audit log entries should indicate the source as JWT group sync (e.g., by recording the grantor as `MZ_JWT_SYNC_ROLE_ID` in the event details) so admins can distinguish sync-initiated changes from manual ones.
- **`mz_role_members`**: The `grantor` column distinguishes sync-managed memberships (grantor = `MZ_JWT_SYNC_ROLE_ID`) from manual grants, allowing admins to query for sync state.
- **Server logs**: Warnings for skipped groups, unmatched groups, cycles, reserved system role attempts (`mz_*`, `pg_*`).
- **Client NOTICEs**: Unmatched groups, reserved role attempts, and sync errors are sent as NOTICEs to the connecting client. Since users may belong to many IdP groups unrelated to Materialize, verbose notice output can be controlled by a session variable (deferred to post-MVP; MVP always sends notices).

A dedicated system table or `SHOW EXTERNAL_GROUPS` command is deferred.

## Minimal Viable Prototype

### Work Items

1. Add `MZ_JWT_SYNC_ROLE_ID` sentinel role (catalog migration)
2. Add `groups()` method to `OidcClaims`
3. Add `groups: Option<Vec<String>>` to `ExternalUserMetadata`
4. Add system variables: `oidc_group_role_sync_enabled`, `oidc_group_claim`, `oidc_group_role_sync_strict`
5. Implement sync logic in `handle_startup_inner` (including `mz_`/`pg_` prefix filtering)
6. Audit log entries for sync GRANTs/REVOKEs with source attribution (automatic via existing Op path, grantor recorded in event details)
7. Tests: unit tests for group extraction/normalization, reserved role filtering, strict/fail-open modes, end-to-end tests

## Alternatives

### CockroachDB (Prior Art)

CockroachDB (v25.4+) implements this via [JWT authorization with group claims](https://www.cockroachlabs.com/docs/stable/jwt-authorization):

1. **Groups extraction**: Reads `groups` claim from JWT (configurable claim name). Falls back to querying IdP's userinfo endpoint.
2. **Normalization**: Groups are lowercased, deduplicated, sorted.
3. **Role sync**: Each group name is matched to a database role name. User is GRANTed matching roles and REVOKEd roles that no longer match.
4. **Empty groups = login rejected**: If groups resolve to empty, login fails.
5. **Configuration**:
   - `server.jwt_authentication.group_claim`, which JWT field has groups (default: `"groups"`)
   - `server.jwt_authentication.authorization.enabled`, feature gate
6. **Roles must pre-exist**: No auto-creation of roles from group names.

Reference PR: [cockroachdb/cockroach#147318](https://github.com/cockroachdb/cockroach/pull/147318)

The design follows CockroachDB's design with small differences: we do not reject login on empty groups (we allow it if there are manual grants), and we use the grantor field to distinguish sync-managed from manually-managed role memberships.

### Kubernetes Custom Resource Definitions

Alternatively: model group-to-role mappings as Kubernetes CRDs, letting admins declare mappings declaratively in their cluster manifests. A controller would watch these resources and reconcile role memberships in Materialize.

This was rejected because it would require building and maintaining a new API server (or operator) alongside environmentd. The JWT already carries the group information at connection time, so it would simply add latency and complexity without clear benefit over reading the claims directly.

## Open Questions

1. **Sync on the connection hot path**: `handle_startup_inner` runs on every connection. The sync does a `catalog_transact`, which takes a write lock on the catalog. **Mitigation**: skip the catalog transaction entirely if the user's groups haven't changed since the last sync (compare the sorted group list from the JWT against the current sync-granted memberships). This should make the common case (reconnect with same groups) a no-op read.
2. **Unmatched group observability**: Unmatched groups are surfaced as client NOTICEs and server log warnings. The `mz_audit_log` only records actual GRANT/REVOKE operations (not skipped groups), since users may belong to many IdP groups unrelated to Materialize and logging each one would be noisy. A dedicated system table or session-variable-controlled verbosity for notices can be added post-MVP.
3. **Edge case behaviour**: Are these the right choices? (See Security: Shadowed Permissions section for the `strict` mode trade-off.)
4. **Frontegg group claims in JWT**: Can Frontegg be configured to include group membership as a claim in the JWT access token it issues? **Action**: Before implementation, someone from Cloud should verify that Frontegg JWTs include group membership for the authenticated user, and whether upstream IdP group memberships are also passed through.
5. **Two authentication paths for Cloud support**: The Frontegg authenticator uses app passwords. Users authenticate with a client ID and secret key, which are exchanged with Frontegg's API for a JWT (`exchange_app_password()`). This means group sync for Cloud would need its own implementation: extract groups from the JWT returned by the app password exchange, and push updated group memberships through another (or existing channel) on each token refresh. This is a separate code path from OIDC.
6. **SCIM for self-managed**: SCIM support for self-managed deployments is important and should be a near-term follow-up. The current JWT-based design is intentionally provider-agnostic (no Frontegg-specific coupling) to ensure we can layer SCIM on top without rearchitecting. Key consideration: SCIM would enable push-based provisioning (vs. pull-on-connect), complementing this design rather than replacing it.
7. **Group name transformation**: Enterprise IdPs often use prefixed group names (e.g., `materialize_platform_eng`). A regex-based transform (e.g., `oidc_group_name_transform = 'materialize_(.*)'` -> `$1`) would be valuable. Deferred to post-MVP but the sync logic should be structured to allow inserting a transform step easily.
