---
source: src/adapter/src/coord/group_sync.rs
revision: 0a20c581ea
---

# adapter::coord::group_sync

Implements JWT group-to-role membership sync logic for the coordinator.

`compute_group_sync_diff` computes the set of `Op::GrantRole` and `Op::RevokeRole` catalog operations needed to bring a user's current role memberships into alignment with the role IDs resolved from their JWT group claims.
It partitions the user's existing memberships into two sets: those granted by the `MZ_JWT_SYNC_ROLE_ID` sentinel (sync-managed) and those granted by any other principal (manually-granted).
Manually-granted roles are never touched: the function neither revokes them nor overwrites their grantor, even if the same role appears in the JWT target set.
Roles to grant are those in the target set that are absent from both sync-managed and manual sets; roles to revoke are those in the sync-managed set that are absent from the target set.

`GroupSyncDiff` bundles the resulting `grants` and `revokes` operation vectors for the caller to apply as a single catalog transaction.
