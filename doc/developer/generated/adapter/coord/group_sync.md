---
source: src/adapter/src/coord/group_sync.rs
revision: fda8736965
---

# adapter::coord::group_sync

Implements JWT group-to-role membership sync logic for the coordinator.

`compute_group_sync_diff` computes the set of `Op::GrantRole` and `Op::RevokeRole` catalog operations needed to bring a user's current role memberships into alignment with the role IDs resolved from their JWT group claims.
It partitions the user's existing memberships into two sets: those granted by the `MZ_JWT_SYNC_ROLE_ID` sentinel (sync-managed) and those granted by any other principal (manually-granted).
Manually-granted roles are never touched: the function neither revokes them nor overwrites their grantor, even if the same role appears in the JWT target set.
Roles to grant are those in the target set that are absent from both sync-managed and manual sets; roles to revoke are those in the sync-managed set that are absent from the target set.

`GroupSyncDiff` bundles the resulting `grants` and `revokes` operation vectors for the caller to apply as a single catalog transaction.

`Coordinator::maybe_sync_jwt_groups` is the top-level entry point called during connection startup: it checks whether sync is enabled (via `OIDC_GROUP_ROLE_SYNC_ENABLED`), short-circuits when no group claim is present (`groups == None`), and delegates to `sync_jwt_groups`. Errors are either propagated as `AdapterError::OidcGroupSyncFailed` (strict mode, `OIDC_GROUP_ROLE_SYNC_STRICT`) or suppressed with a warning notice (fail-open default). Notices about unmatched groups, reserved-name groups, and sync errors are sent to the client via `notice_tx`.

`Coordinator::sync_jwt_groups` resolves each group name to a catalog role via case-insensitive lookup (`roles_by_lowercase_name`), filtering out reserved names (`mz_`/`pg_` prefixes) and self-references (where the group resolves to the user's own role). It then calls `compute_group_sync_diff` and executes the resulting grant/revoke ops via `catalog_transact`.
