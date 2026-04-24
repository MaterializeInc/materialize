// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! JWT group-to-role membership sync logic.
//!
//! This module computes the diff between a user's current role memberships
//! and their JWT group claims, producing `Op::GrantRole` and `Op::RevokeRole`
//! operations. Only memberships granted by the `MZ_JWT_SYNC_ROLE_ID` sentinel
//! are managed; manually-granted memberships are never touched.

use std::collections::{BTreeMap, BTreeSet};

use mz_repr::role_id::RoleId;
use mz_sql::session::user::MZ_JWT_SYNC_ROLE_ID;

use crate::catalog::Op;

/// Result of computing the group-to-role membership sync diff.
#[derive(Debug, Clone)]
pub struct GroupSyncDiff {
    /// Roles to grant to the user (with sentinel grantor).
    pub grants: Vec<Op>,
    /// Roles to revoke from the user (with sentinel grantor).
    pub revokes: Vec<Op>,
}

/// Computes the grant/revoke operations needed to sync a user's role
/// memberships with their JWT group claims.
///
/// # Arguments
/// - `member_id`: The user's role ID.
/// - `current_membership`: The user's current `RoleMembership.map`
///   (role_id → grantor_id).
/// - `target_role_ids`: Role IDs resolved from the JWT group names.
///
/// # Semantics
/// - Only roles granted by the JWT sync sentinel (`MZ_JWT_SYNC_ROLE_ID`)
///   are managed by this function.
/// - Manually-granted roles (grantor != sentinel) are never revoked.
/// - If a target role is already manually granted, it is skipped — the
///   manual grant takes precedence and we don't overwrite the grantor.
pub fn compute_group_sync_diff(
    member_id: RoleId,
    current_membership: &BTreeMap<RoleId, RoleId>,
    target_role_ids: &BTreeSet<RoleId>,
) -> GroupSyncDiff {
    // Partition current memberships into sync-managed vs manually-granted.
    let mut sync_granted: BTreeSet<RoleId> = BTreeSet::new();
    let mut manual_granted: BTreeSet<RoleId> = BTreeSet::new();

    for (role_id, grantor_id) in current_membership {
        if *grantor_id == MZ_JWT_SYNC_ROLE_ID {
            sync_granted.insert(*role_id);
        } else {
            manual_granted.insert(*role_id);
        }
    }

    // Roles to grant: in target, not already sync-granted, not manually-granted.
    let grants: Vec<Op> = target_role_ids
        .iter()
        .filter(|r| !sync_granted.contains(r) && !manual_granted.contains(r))
        .map(|&role_id| Op::GrantRole {
            role_id,
            member_id,
            grantor_id: MZ_JWT_SYNC_ROLE_ID,
        })
        .collect();

    // Roles to revoke: sync-granted but no longer in target.
    let revokes: Vec<Op> = sync_granted
        .iter()
        .filter(|r| !target_role_ids.contains(r))
        .map(|&role_id| Op::RevokeRole {
            role_id,
            member_id,
            grantor_id: MZ_JWT_SYNC_ROLE_ID,
        })
        .collect();

    GroupSyncDiff { grants, revokes }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user_id() -> RoleId {
        RoleId::User(100)
    }
    fn role_a() -> RoleId {
        RoleId::User(1)
    }
    fn role_b() -> RoleId {
        RoleId::User(2)
    }
    fn role_c() -> RoleId {
        RoleId::User(3)
    }
    fn admin_id() -> RoleId {
        RoleId::User(99)
    }

    /// Extract the role IDs from grant ops for easy assertion.
    fn grant_role_ids(diff: &GroupSyncDiff) -> BTreeSet<RoleId> {
        diff.grants
            .iter()
            .map(|op| match op {
                Op::GrantRole { role_id, .. } => *role_id,
                _ => panic!("expected GrantRole op"),
            })
            .collect()
    }

    /// Extract the role IDs from revoke ops for easy assertion.
    fn revoke_role_ids(diff: &GroupSyncDiff) -> BTreeSet<RoleId> {
        diff.revokes
            .iter()
            .map(|op| match op {
                Op::RevokeRole { role_id, .. } => *role_id,
                _ => panic!("expected RevokeRole op"),
            })
            .collect()
    }

    /// Verify all grant ops use the sentinel grantor and correct member.
    fn assert_grants_well_formed(diff: &GroupSyncDiff, expected_member: RoleId) {
        for op in &diff.grants {
            match op {
                Op::GrantRole {
                    member_id,
                    grantor_id,
                    ..
                } => {
                    assert_eq!(*member_id, expected_member);
                    assert_eq!(*grantor_id, MZ_JWT_SYNC_ROLE_ID);
                }
                _ => panic!("expected GrantRole op"),
            }
        }
    }

    /// Verify all revoke ops use the sentinel grantor and correct member.
    fn assert_revokes_well_formed(diff: &GroupSyncDiff, expected_member: RoleId) {
        for op in &diff.revokes {
            match op {
                Op::RevokeRole {
                    member_id,
                    grantor_id,
                    ..
                } => {
                    assert_eq!(*member_id, expected_member);
                    assert_eq!(*grantor_id, MZ_JWT_SYNC_ROLE_ID);
                }
                _ => panic!("expected RevokeRole op"),
            }
        }
    }

    #[mz_ore::test]
    fn test_first_login_grants_all() {
        let current = BTreeMap::new();
        let target = BTreeSet::from([role_a(), role_b()]);
        let diff = compute_group_sync_diff(user_id(), &current, &target);

        assert_eq!(diff.grants.len(), 2);
        assert_eq!(diff.revokes.len(), 0);
        assert_eq!(grant_role_ids(&diff), BTreeSet::from([role_a(), role_b()]));
        assert_grants_well_formed(&diff, user_id());
    }

    #[mz_ore::test]
    fn test_no_change_is_noop() {
        let current = BTreeMap::from([
            (role_a(), MZ_JWT_SYNC_ROLE_ID),
            (role_b(), MZ_JWT_SYNC_ROLE_ID),
        ]);
        let target = BTreeSet::from([role_a(), role_b()]);
        let diff = compute_group_sync_diff(user_id(), &current, &target);

        assert_eq!(diff.grants.len(), 0);
        assert_eq!(diff.revokes.len(), 0);
    }

    #[mz_ore::test]
    fn test_revoke_removed_groups() {
        let current = BTreeMap::from([
            (role_a(), MZ_JWT_SYNC_ROLE_ID),
            (role_b(), MZ_JWT_SYNC_ROLE_ID),
            (role_c(), MZ_JWT_SYNC_ROLE_ID),
        ]);
        let target = BTreeSet::from([role_a()]);
        let diff = compute_group_sync_diff(user_id(), &current, &target);

        assert_eq!(diff.grants.len(), 0);
        assert_eq!(diff.revokes.len(), 2);
        assert_eq!(
            revoke_role_ids(&diff),
            BTreeSet::from([role_b(), role_c()])
        );
        assert_revokes_well_formed(&diff, user_id());
    }

    #[mz_ore::test]
    fn test_manual_grants_untouched() {
        // Manual grant for A (by admin), sync grant for B.
        // Target: A, C.
        // Expected: grant C (A is manual, skip), revoke B (not in target).
        let current = BTreeMap::from([
            (role_a(), admin_id()),
            (role_b(), MZ_JWT_SYNC_ROLE_ID),
        ]);
        let target = BTreeSet::from([role_a(), role_c()]);
        let diff = compute_group_sync_diff(user_id(), &current, &target);

        assert_eq!(diff.grants.len(), 1);
        assert_eq!(diff.revokes.len(), 1);
        assert_eq!(grant_role_ids(&diff), BTreeSet::from([role_c()]));
        assert_eq!(revoke_role_ids(&diff), BTreeSet::from([role_b()]));
    }

    #[mz_ore::test]
    fn test_empty_target_revokes_all_sync() {
        // Sync-granted A, B. Manual C. Target: empty.
        // Expected: revoke A and B, keep C.
        let current = BTreeMap::from([
            (role_a(), MZ_JWT_SYNC_ROLE_ID),
            (role_b(), MZ_JWT_SYNC_ROLE_ID),
            (role_c(), admin_id()),
        ]);
        let target = BTreeSet::new();
        let diff = compute_group_sync_diff(user_id(), &current, &target);

        assert_eq!(diff.grants.len(), 0);
        assert_eq!(diff.revokes.len(), 2);
        assert_eq!(
            revoke_role_ids(&diff),
            BTreeSet::from([role_a(), role_b()])
        );
    }

    #[mz_ore::test]
    fn test_mixed_grant_and_revoke() {
        // Sync-granted A. Target: B.
        // Expected: grant B, revoke A.
        let current = BTreeMap::from([(role_a(), MZ_JWT_SYNC_ROLE_ID)]);
        let target = BTreeSet::from([role_b()]);
        let diff = compute_group_sync_diff(user_id(), &current, &target);

        assert_eq!(diff.grants.len(), 1);
        assert_eq!(diff.revokes.len(), 1);
        assert_eq!(grant_role_ids(&diff), BTreeSet::from([role_b()]));
        assert_eq!(revoke_role_ids(&diff), BTreeSet::from([role_a()]));
    }

    #[mz_ore::test]
    fn test_empty_current_empty_target() {
        let current = BTreeMap::new();
        let target = BTreeSet::new();
        let diff = compute_group_sync_diff(user_id(), &current, &target);

        assert_eq!(diff.grants.len(), 0);
        assert_eq!(diff.revokes.len(), 0);
    }

    #[mz_ore::test]
    fn test_all_manual_grants_no_revokes() {
        // All current memberships are manual, none sync-granted.
        // Target has a new role. Manual ones should not be revoked.
        let current = BTreeMap::from([(role_a(), admin_id()), (role_b(), admin_id())]);
        let target = BTreeSet::from([role_c()]);
        let diff = compute_group_sync_diff(user_id(), &current, &target);

        assert_eq!(diff.grants.len(), 1);
        assert_eq!(diff.revokes.len(), 0);
        assert_eq!(grant_role_ids(&diff), BTreeSet::from([role_c()]));
    }

    #[mz_ore::test]
    fn test_target_overlaps_both_manual_and_sync() {
        // A is manual, B is sync, C is sync. Target: A, B, D.
        // Expected: grant D (A manual=skip, B sync=already there), revoke C.
        let role_d = RoleId::User(4);
        let current = BTreeMap::from([
            (role_a(), admin_id()),
            (role_b(), MZ_JWT_SYNC_ROLE_ID),
            (role_c(), MZ_JWT_SYNC_ROLE_ID),
        ]);
        let target = BTreeSet::from([role_a(), role_b(), role_d]);
        let diff = compute_group_sync_diff(user_id(), &current, &target);

        assert_eq!(diff.grants.len(), 1);
        assert_eq!(grant_role_ids(&diff), BTreeSet::from([role_d]));
        assert_eq!(diff.revokes.len(), 1);
        assert_eq!(revoke_role_ids(&diff), BTreeSet::from([role_c()]));
    }
}
