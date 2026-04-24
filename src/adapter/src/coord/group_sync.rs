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

use mz_adapter_types::dyncfgs::{OIDC_GROUP_ROLE_SYNC_ENABLED, OIDC_GROUP_ROLE_SYNC_STRICT};
use mz_repr::role_id::RoleId;
use mz_sql::session::user::MZ_JWT_SYNC_ROLE_ID;
use tokio::sync::mpsc;
use tracing::warn;

use crate::catalog::{self, Op};
use crate::coord::Coordinator;
use crate::notice::AdapterNotice;
use crate::AdapterError;

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

impl Coordinator {
    /// Top-level entry point for JWT group-to-role sync during connection startup.
    ///
    /// Checks whether sync is enabled and whether the user has group claims,
    /// then delegates to [`Self::sync_jwt_groups`]. Handles strict vs fail-open
    /// error semantics and delivers notices to the client via `notice_tx`.
    ///
    /// - `groups == None` (claim absent) → skip sync entirely, preserving current state.
    /// - `groups == Some([])` (empty claim) → revoke all sync-granted roles.
    /// - Strict mode (`oidc_group_role_sync_strict`) → reject login on sync failure.
    /// - Fail-open (default) → log warning, send notice, continue login.
    pub(crate) async fn maybe_sync_jwt_groups(
        &mut self,
        member_id: RoleId,
        groups: Option<&[String]>,
        notice_tx: &mpsc::UnboundedSender<AdapterNotice>,
    ) -> Result<(), AdapterError> {
        let groups = match groups {
            Some(g) => g,
            None => return Ok(()),
        };

        let dyncfgs = self.catalog().system_config().dyncfgs();
        let sync_enabled = OIDC_GROUP_ROLE_SYNC_ENABLED.get(dyncfgs);
        let strict = OIDC_GROUP_ROLE_SYNC_STRICT.get(dyncfgs);

        if !sync_enabled {
            return Ok(());
        }

        let mut notices = Vec::new();
        match self.sync_jwt_groups(member_id, groups, &mut notices).await {
            Ok(()) => {}
            Err(e) => {
                if strict {
                    return Err(AdapterError::OidcGroupSyncFailed(e.to_string()));
                } else {
                    warn!(
                        error = %e,
                        "OIDC group sync failed, proceeding with login (fail-open mode)"
                    );
                    notices.push(AdapterNotice::OidcGroupSyncError {
                        message: e.to_string(),
                    });
                }
            }
        }
        for notice in notices {
            let _ = notice_tx.send(notice);
        }
        Ok(())
    }

    /// Syncs the user's role memberships based on JWT group claims.
    ///
    /// Resolves group names to catalog role IDs (case-insensitive),
    /// computes the diff against current memberships, and executes
    /// grant/revoke operations via `catalog_transact`.
    ///
    /// Groups that map to reserved role names (`mz_`/`pg_` prefixes) are
    /// filtered out with a warning notice. Groups with no matching catalog
    /// role produce an informational notice.
    pub(crate) async fn sync_jwt_groups(
        &mut self,
        member_id: RoleId,
        groups: &[String],
        notices: &mut Vec<AdapterNotice>,
    ) -> Result<(), AdapterError> {
        // Resolve group names to role IDs (case-insensitive).
        let mut target_role_ids = BTreeSet::new();
        for group in groups {
            // Filter out reserved role names (mz_/pg_ prefixes, PUBLIC).
            if catalog::is_reserved_role_name(group) {
                warn!(
                    group = group.as_str(),
                    "OIDC group maps to reserved role name, skipping"
                );
                notices.push(AdapterNotice::OidcGroupSyncReservedRole {
                    group: group.clone(),
                });
                continue;
            }

            match self
                .catalog()
                .try_get_role_by_name_case_insensitive(group)
            {
                Some(role) => {
                    target_role_ids.insert(role.id);
                }
                None => {
                    warn!(
                        group = group.as_str(),
                        "OIDC group has no matching Materialize role, skipping"
                    );
                    notices.push(AdapterNotice::OidcGroupSyncUnmatchedGroup {
                        group: group.clone(),
                    });
                }
            }
        }

        // Get the user's current memberships. Clone is needed to release the
        // immutable catalog borrow before the mutable catalog_transact call below.
        // This is cheap — role membership maps are typically small.
        let current_membership = self
            .catalog()
            .get_role(&member_id)
            .membership
            .map
            .clone();

        // Compute diff.
        let diff = compute_group_sync_diff(member_id, &current_membership, &target_role_ids);

        // Skip catalog_transact if no changes (common for reconnect with same groups).
        if diff.grants.is_empty() && diff.revokes.is_empty() {
            return Ok(());
        }

        // Execute ops: revoke first, then grant.
        let mut ops = diff.revokes;
        ops.extend(diff.grants);

        self.catalog_transact(None, ops).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::is_reserved_role_name;

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

    // --- Reserved role name filtering tests ---
    // These verify the contract that `is_reserved_role_name` correctly
    // identifies names that sync_jwt_groups should filter out.

    #[mz_ore::test]
    fn test_reserved_role_mz_prefix() {
        assert!(is_reserved_role_name("mz_system"));
        assert!(is_reserved_role_name("mz_introspection"));
        assert!(is_reserved_role_name("mz_jwt_sync"));
        assert!(is_reserved_role_name("mz_anything"));
    }

    #[mz_ore::test]
    fn test_reserved_role_pg_prefix() {
        assert!(is_reserved_role_name("pg_monitor"));
        assert!(is_reserved_role_name("pg_read_all_data"));
    }

    #[mz_ore::test]
    fn test_reserved_role_public() {
        assert!(is_reserved_role_name("PUBLIC"));
    }

    #[mz_ore::test]
    fn test_normal_role_names_not_reserved() {
        assert!(!is_reserved_role_name("analytics"));
        assert!(!is_reserved_role_name("platform_eng"));
        assert!(!is_reserved_role_name("admin"));
        assert!(!is_reserved_role_name("data_eng"));
        // Prefix must be exact — "mzz_foo" or "pga_foo" are not reserved.
        assert!(!is_reserved_role_name("mzz_custom"));
        assert!(!is_reserved_role_name("pga_custom"));
    }

    // --- Diff tests for edge cases related to reserved role filtering ---
    // When reserved roles are filtered out before reaching compute_group_sync_diff,
    // the target set is effectively reduced. These tests verify the diff function
    // handles the resulting scenarios correctly.

    #[mz_ore::test]
    fn test_all_reserved_filtered_results_in_empty_target() {
        // If all groups are reserved and filtered, target is empty.
        // Existing sync-granted roles should be revoked.
        let current = BTreeMap::from([
            (role_a(), MZ_JWT_SYNC_ROLE_ID),
            (role_b(), MZ_JWT_SYNC_ROLE_ID),
        ]);
        let target = BTreeSet::new(); // empty after filtering
        let diff = compute_group_sync_diff(user_id(), &current, &target);

        assert_eq!(diff.grants.len(), 0);
        assert_eq!(diff.revokes.len(), 2);
        assert_eq!(
            revoke_role_ids(&diff),
            BTreeSet::from([role_a(), role_b()])
        );
    }

    #[mz_ore::test]
    fn test_mixed_reserved_and_valid_after_filtering() {
        // If some groups are reserved (filtered) and some are valid,
        // only valid ones appear in target. This is the same as a
        // partial target — only valid roles are granted.
        let current = BTreeMap::new();
        let target = BTreeSet::from([role_a()]); // role_b was reserved, filtered out
        let diff = compute_group_sync_diff(user_id(), &current, &target);

        assert_eq!(diff.grants.len(), 1);
        assert_eq!(diff.revokes.len(), 0);
        assert_eq!(grant_role_ids(&diff), BTreeSet::from([role_a()]));
    }
}
