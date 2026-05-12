// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Repair Role rows whose stored byte image diverged from the bytes written
//! by post-v80 catalog mutations.
//!
//! # Background
//!
//! The catalog persist shard requires that every `(key_bytes, ts)` tuple has
//! `Diff::ONE` after consolidation. `PersistPeek::do_peek` enforces this
//! per-row when serving `mz_internal.mz_catalog_raw`, and `run_versioned_upgrade`
//! checks it via `soft_assert_eq_or_log!("snapshot is consolidated", ...)` on
//! every catalog version bump. The catalog *writers*, however, retract by
//! re-serializing the in-memory parsed value through the current proto — sound
//! only if that round-trip is byte-exact (database-issues#7179). It isn't,
//! whenever a proto field has been added since a row was last written: the
//! stored row lacks the key entirely, and the re-serialized retraction writes
//! the key as explicit `null`. Different bytes, no cancellation under
//! consolidation, dangling `-1`.
//!
//! # The specific failure this migration targets
//!
//! `v80_to_v81::upgrade` was supposed to backfill `auto_provision_source` on
//! every existing Role row by retracting the v80 byte form and inserting a
//! v81 byte form with the new field set. That backfill was gated on an
//! `is_cloud` heuristic that, among other conditions, required the
//! `mz_system` cluster to be a `ClusterVariant::Managed`:
//!
//! ```text
//! if let ClusterVariant::Managed(ManagedCluster { replication_factor, .. })
//!     = cluster.value.config.variant
//! { replication_factor > 0 && !has_password_auth }
//! else { false }
//! ```
//!
//! On any env where `mz_system` was not in the `Managed` variant at upgrade
//! time, the heuristic returned `false` and the migration silently no-opped.
//! The version bump 80→81 still committed (it's unconditional in
//! `run_versioned_upgrade`), but every Role row kept its v80 byte form: a
//! JSON object with `attributes.{inherit, login, superuser}` and no
//! `auto_provision_source` key at all. After v26.18 onwards, *any* DDL that
//! touches one of these rows (`ALTER ROLE` of any kind, role membership
//! changes, `DROP ROLE`) reads the row, parses it (the missing field becomes
//! `None`), then writes a retract+insert pair through current protos — which
//! always include the `auto_provision_source` key. The retraction bytes don't
//! byte-match the stored row; the consolidation step doesn't merge them; the
//! shard ends up holding three rows per affected role:
//!
//!   * a stale `+1` in v80 byte form (no `auto_provision_source` key),
//!   * a dangling `-1` in v82 byte form (the retraction that missed),
//!   * a live `+1` in v82 byte form reflecting whatever change the DDL made.
//!
//! For a `DROP ROLE` the third row is absent — the role is gone, but the
//! first two persist forever, polluting the shard.
//!
//! `PersistPeek` fires on the dangling `-1`. `run_versioned_upgrade`'s soft
//! assert fires on the dangling `-1`. `apply()` in `persist.rs` does *not*
//! fire because it keys by parsed `(RoleKey, RoleValue)` and the dangling
//! `-1`'s parsed value equals the stale `+1`'s — both lack the field
//! semantically, only the byte image diverges. So affected envs run quietly
//! until either someone queries `mz_catalog_raw` or a new catalog version
//! migration runs.
//!
//! # The repair
//!
//! For every Role with the structural signature of this bug — a dangling `-1`
//! plus at least one `+1` whose parsed `RoleValue` equals the dangling row's
//! parsed value, plus at most one *other* `+1` with a different parsed value
//! (representing the live state, or absent if the role was dropped) — we
//! issue:
//!
//!   1. `+1` of the dangling row's exact bytes — cancels the dangling `-1`
//!      under byte-keyed consolidation.
//!   2. `-1` of every stale `+1` row's exact bytes — completes the retraction
//!      the original DDL was supposed to perform.
//!
//! After commit, each affected `RoleKey` has either one row in the shard (the
//! live `+1`) or zero (cleanly dropped). Both `PersistPeek`'s per-row
//! non-negativity check and `apply()`'s parsed-value invariant hold.
//!
//! The predicate is structural, not symptomatic. We don't care *what* field
//! the live `+1` differs from the dangling `-1` in — `login`, `superuser`,
//! `inherit`, `name`, anything — because the cause is byte-form drift on the
//! retraction side, not anything specific to which attribute got mutated.
//! Two `+1`s for the same `RoleKey` with parsed-equal values is the
//! fingerprint of the round-trip bug class; anything that fits the
//! fingerprint gets cleaned up.
//!
//! Anything that *doesn't* fit — a dangling `-1` with no parsed-equal `+1`
//! sibling, with multiple live candidates of distinct parsed values, with a
//! non-Role kind, or with `|diff| > 1` — is logged at WARN and left for human
//! review. Better to under-clean and surface unknown shapes for triage than
//! over-clean and accidentally retire live state.

use std::collections::BTreeMap;

use mz_repr::Diff;

use crate::durable::objects::state_update::{StateUpdate, StateUpdateKindJson};
use crate::durable::persist::{Mode, Timestamp, UnopenedPersistCatalogState};
use crate::durable::upgrade::objects_v83 as v83;
use crate::durable::{CatalogError, initialize::USER_VERSION_KEY};

const FROM_VERSION: u64 = 82;
const TO_VERSION: u64 = 83;

/// Outcome counters for the repair, returned for logging and assertable in
/// tests.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct RepairStats {
    /// Role retraction phantoms matching the v80-form-drift signature that
    /// were cancelled by writing a compensating `+1` of the same bytes.
    pub repaired: usize,
    /// Stale `+1` rows (alternate byte forms of the same parsed Role value as
    /// a repaired phantom) that were retracted as part of the repair.
    pub stale_retracted: usize,
    /// Dangling Role `-1`s that didn't fit the structural signature (no
    /// parsed-equal `+1` sibling, multiple distinct live candidates, etc.).
    pub skipped_role: usize,
    /// Dangling rows for kinds other than `Role`. The known corruption only
    /// affects Role rows; everything else is left for human inspection.
    pub skipped_non_role: usize,
}

pub async fn upgrade(
    unopened_catalog_state: &mut UnopenedPersistCatalogState,
    mut commit_ts: Timestamp,
) -> Result<(u64, Timestamp), CatalogError> {
    tracing::info!(
        from_version = FROM_VERSION,
        to_version = TO_VERSION,
        "running versioned Catalog upgrade (repair Role byte-form drift)",
    );

    let (repairs, stats) = compute_repairs(&unopened_catalog_state.snapshot);

    if !repairs.is_empty() {
        tracing::info!(
            repaired = stats.repaired,
            stale_retracted = stats.stale_retracted,
            "repairing Role rows left in inconsistent byte form by the v80->v81 migration's non-cloud no-op",
        );
    }
    if stats.skipped_role > 0 || stats.skipped_non_role > 0 {
        tracing::warn!(
            skipped_role = stats.skipped_role,
            skipped_non_role = stats.skipped_non_role,
            "left dangling diffs that did not fit the v80-form-drift signature; review the WARN events emitted above",
        );
    }

    let mut updates: Vec<(StateUpdateKindJson, Diff)> = repairs;
    updates.push((version_update_kind(FROM_VERSION), Diff::MINUS_ONE));
    updates.push((version_update_kind(TO_VERSION), Diff::ONE));

    if matches!(unopened_catalog_state.mode, Mode::Writable) {
        commit_ts = unopened_catalog_state
            .compare_and_append(updates, commit_ts)
            .await
            .map_err(|e| e.unwrap_fence_error())?;
    } else {
        let ts = commit_ts;
        let updates = updates
            .into_iter()
            .map(|(kind, diff)| StateUpdate { kind, ts, diff });
        commit_ts = commit_ts.step_forward();
        unopened_catalog_state.apply_updates_and_consolidate(updates)?;
    }

    unopened_catalog_state.consolidate();
    Ok((TO_VERSION, commit_ts))
}

/// Inspect a consolidated snapshot and return the byte-level updates needed
/// to converge every Role login-mismatch site onto "exactly one `+1` per
/// `RoleKey`, with the live login state."
///
/// The returned `Vec` is safe to feed straight into `compare_and_append`. For
/// each repaired site we emit:
///
///   * `+1` of the dangling row's exact bytes (cancels the existing `-1`);
///   * one `-1` per *stale* `+1` row whose parsed value equals the dangling
///     row's parsed value (retires the pre-evolution byte image the original
///     retraction failed to byte-match).
///
/// Separated from `upgrade` so it can be unit-tested without spinning up a
/// real catalog handle.
pub(crate) fn compute_repairs(
    snapshot: &[(StateUpdateKindJson, Timestamp, Diff)],
) -> (Vec<(StateUpdateKindJson, Diff)>, RepairStats) {
    // Group every Role `+1` row by its parsed `RoleKey`. We need the full set
    // (not just one representative) so we can identify the live row vs any
    // stale byte-form siblings — they all live under the same key.
    let mut role_plus_ones: BTreeMap<v83::RoleKey, Vec<RolePlusOne<'_>>> = BTreeMap::new();
    for (kind_json, _, diff) in snapshot {
        if *diff != Diff::ONE {
            continue;
        }
        let Some(role) = try_as_role(kind_json) else {
            continue;
        };
        role_plus_ones
            .entry(role.key.clone())
            .or_default()
            .push(RolePlusOne {
                bytes: kind_json,
                parsed: role,
            });
    }

    let mut repairs = Vec::new();
    let mut stats = RepairStats::default();
    for (kind_json, _, diff) in snapshot {
        if *diff == Diff::ONE {
            continue;
        }

        // Anything other than a Role row is outside the scope of this targeted
        // repair. We don't pretend to know how to fix Database, Schema, Item,
        // etc. dangling diffs — those need human triage.
        let Some(dangling) = try_as_role(kind_json) else {
            tracing::warn!(
                ?kind_json,
                %diff,
                "non-Role dangling diff; not repaired by the v80-form-drift migration",
            );
            stats.skipped_non_role += 1;
            continue;
        };

        // The known bug produces exactly `Diff::MINUS_ONE`. A magnitude > 1 is
        // a different kind of accounting error and shouldn't be auto-fixed.
        if *diff != Diff::MINUS_ONE {
            tracing::warn!(
                role_name = %dangling.value.name,
                %diff,
                "Role row with unexpected diff magnitude; not repaired",
            );
            stats.skipped_role += 1;
            continue;
        }

        // Structural classifier. For each `+1` sibling of this `-1`:
        //
        //   - `stale`: parsed value EQUALS the dangling row's parsed value.
        //     This is another byte form of "the state the original retraction
        //     was supposed to cancel" — the v80-form-drift fingerprint. We'll
        //     retract these.
        //   - `live`: parsed value DIFFERS from the dangling row's. This is
        //     the post-mutation state (e.g. after an `ALTER ROLE`). We leave
        //     it untouched. At most one such row is allowed; multiple distinct
        //     live values for the same key would be ambiguous.
        //
        // The signature requires at least one stale row — that's the
        // structural proof that the dangling `-1` is byte-form drift rather
        // than some other class of corruption. A dangling `-1` with no stale
        // sibling looks like a free-floating retraction and we won't act.
        // A dangling `-1` with stale siblings but multiple distinct live
        // candidates is ambiguous and we won't act.
        let siblings = role_plus_ones
            .get(&dangling.key)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let mut stale: Vec<&RolePlusOne<'_>> = Vec::new();
        let mut live: Option<&RolePlusOne<'_>> = None;
        let mut ambiguous_live = false;
        for sib in siblings {
            if sib.parsed.value == dangling.value {
                stale.push(sib);
            } else if live.replace(sib).is_some() {
                ambiguous_live = true;
            }
        }

        if stale.is_empty() {
            tracing::warn!(
                role_name = %dangling.value.name,
                num_siblings = siblings.len(),
                "dangling Role -1 has no parsed-equal +1 sibling; not the v80-form-drift signature",
            );
            stats.skipped_role += 1;
            continue;
        }
        if ambiguous_live {
            tracing::warn!(
                role_name = %dangling.value.name,
                num_siblings = siblings.len(),
                "Role key has multiple distinct live +1 rows; refusing to auto-repair",
            );
            stats.skipped_role += 1;
            continue;
        }

        tracing::info!(
            role_name = %dangling.value.name,
            stale_byte_forms = stale.len(),
            has_live = live.is_some(),
            "repairing v80-form-drift phantom retraction",
        );
        // 1. Cancel the dangling `-1` by writing `+1` of the same bytes.
        repairs.push((kind_json.clone(), Diff::ONE));
        // 2. Retract every stale `+1` row sharing the dangling row's parsed
        //    value. The original retraction was supposed to cancel one of
        //    these; we replay that intent against the actual stored bytes.
        for s in stale {
            // Don't retract the dangling row's own byte image — step 1
            // already cancels it.
            if s.bytes == kind_json {
                continue;
            }
            repairs.push((s.bytes.clone(), Diff::MINUS_ONE));
            stats.stale_retracted += 1;
        }
        stats.repaired += 1;
    }

    (repairs, stats)
}

/// A parsed `+1` Role row borrowed from the snapshot. Carries both the bytes
/// (so we can emit retractions against the exact byte image) and the parsed
/// value (so we can compare semantic equality across byte forms).
struct RolePlusOne<'a> {
    bytes: &'a StateUpdateKindJson,
    parsed: v83::Role,
}

/// Returns the parsed Role iff `kind_json` is one. Returns `None` for any
/// other kind, or for bytes we can't deserialize as the current Role shape
/// (which we treat as "leave it alone" — losing that row to the repair would
/// be worse than the soft_assert noise).
fn try_as_role(kind_json: &StateUpdateKindJson) -> Option<v83::Role> {
    let kind: v83::StateUpdateKind = kind_json.try_to_serde().ok()?;
    match kind {
        v83::StateUpdateKind::Role(role) => Some(role),
        _ => None,
    }
}

/// Produces the `Config` row encoding a user-version bump. Identical to the
/// helper in `upgrade.rs`, duplicated here so this module is self-contained
/// for testing.
fn version_update_kind(version: u64) -> StateUpdateKindJson {
    use crate::durable::objects::serialization::proto;
    use crate::durable::objects::state_update::StateUpdateKind;
    StateUpdateKind::Config(
        proto::ConfigKey {
            key: USER_VERSION_KEY.to_string(),
        },
        proto::ConfigValue { value: version },
    )
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable::upgrade::objects_v83 as v83;
    use mz_repr::Diff;

    fn role_kind(
        user_id: u64,
        name: &str,
        oid: u32,
        login: Option<bool>,
        superuser: Option<bool>,
        auto_provision_source: Option<v83::AutoProvisionSource>,
    ) -> StateUpdateKindJson {
        let role = v83::Role {
            key: v83::RoleKey {
                id: v83::RoleId::User(user_id),
            },
            value: v83::RoleValue {
                name: name.to_string(),
                oid,
                attributes: v83::RoleAttributes {
                    inherit: true,
                    superuser,
                    login,
                    auto_provision_source,
                },
                membership: v83::RoleMembership { map: vec![] },
                vars: v83::RoleVars { entries: vec![] },
            },
        };
        v83::StateUpdateKind::Role(role).into()
    }

    /// Build a `StateUpdateKindJson` for a Role whose JSON intentionally omits
    /// the `auto_provision_source` key — mirroring the v80-era byte form that
    /// survived a non-cloud-detecting `v80→v81` migration. Parses back to a
    /// Role with `auto_provision_source: None` (serde fills the missing
    /// `Option` field with `None`) but has different stored bytes than the
    /// post-v80 form that always writes the key explicitly.
    fn stale_role_kind_with_dropped_field(
        user_id: u64,
        name: &str,
        oid: u32,
    ) -> StateUpdateKindJson {
        use serde_json::json;
        let v = json!({
            "kind": "Role",
            "key": { "id": { "User": user_id } },
            "value": {
                "name": name,
                "oid": oid,
                "attributes": {
                    "inherit": true,
                    "superuser": null,
                    "login": null,
                    // NOTE: deliberately no "auto_provision_source" key.
                },
                "membership": { "map": [] },
                "vars": { "entries": [] },
            }
        });
        StateUpdateKindJson::from_serde(&v)
    }

    fn database_kind(id: u64, name: &str) -> StateUpdateKindJson {
        // Build a minimally-populated Database row to exercise the "non-Role"
        // skip path. We don't depend on the inner fields beyond the kind tag.
        let db = v83::Database {
            key: v83::DatabaseKey {
                id: v83::DatabaseId::User(id),
            },
            value: v83::DatabaseValue {
                name: name.to_string(),
                owner_id: v83::RoleId::System(1),
                privileges: vec![],
                oid: 0,
            },
        };
        v83::StateUpdateKind::Database(db).into()
    }

    fn snapshot(
        rows: Vec<(StateUpdateKindJson, Diff)>,
    ) -> Vec<(StateUpdateKindJson, Timestamp, Diff)> {
        rows.into_iter()
            .map(|(kind, diff)| (kind, Timestamp::new(0), diff))
            .collect()
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault` on OS `linux`
    fn healthy_snapshot_is_a_noop() {
        let snap = snapshot(vec![
            (
                role_kind(1, "alice@example.com", 100, Some(true), None, None),
                Diff::ONE,
            ),
            (role_kind(2, "bob", 101, None, None, None), Diff::ONE),
        ]);
        let (repairs, stats) = compute_repairs(&snap);
        assert!(repairs.is_empty());
        assert_eq!(stats, RepairStats::default());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault` on OS `linux`
    fn production_shape_alter_login_is_repaired() {
        // The shape we actually pulled out of the affected env for
        // jan@materialize.com (User:8): a stale v80-form `+1` lacking the
        // `auto_provision_source` key, a dangling v82-form `-1` with the key
        // explicit, and a live v82-form `+1` reflecting ALTER ROLE LOGIN.
        let live = role_kind(8, "jan@materialize.com", 20030, Some(true), None, None);
        let dangling = role_kind(8, "jan@materialize.com", 20030, None, None, None);
        let stale = stale_role_kind_with_dropped_field(8, "jan@materialize.com", 20030);

        assert_eq!(
            try_as_role(&stale).expect("parses as Role").value,
            try_as_role(&dangling).expect("parses as Role").value,
            "test fixture broken: stale and dangling must be parsed-equal",
        );
        assert_ne!(
            stale, dangling,
            "test fixture broken: stale and dangling must have different bytes",
        );

        let snap = snapshot(vec![
            (stale.clone(), Diff::ONE),
            (dangling.clone(), Diff::MINUS_ONE),
            (live, Diff::ONE),
        ]);
        let (repairs, stats) = compute_repairs(&snap);
        assert_eq!(
            repairs,
            vec![(dangling, Diff::ONE), (stale, Diff::MINUS_ONE)],
        );
        assert_eq!(
            stats,
            RepairStats {
                repaired: 1,
                stale_retracted: 1,
                ..Default::default()
            }
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault` on OS `linux`
    fn alter_changing_superuser_is_repaired() {
        // Same structural shape as the login case but the live row differs
        // in `superuser`. The predicate doesn't care which field changed —
        // the bug is byte-form drift, not anything attribute-specific.
        let live = role_kind(11, "ops@materialize.com", 20040, None, Some(true), None);
        let dangling = role_kind(11, "ops@materialize.com", 20040, None, None, None);
        let stale = stale_role_kind_with_dropped_field(11, "ops@materialize.com", 20040);

        let snap = snapshot(vec![
            (stale.clone(), Diff::ONE),
            (dangling.clone(), Diff::MINUS_ONE),
            (live, Diff::ONE),
        ]);
        let (repairs, stats) = compute_repairs(&snap);
        assert_eq!(
            repairs,
            vec![(dangling, Diff::ONE), (stale, Diff::MINUS_ONE)],
        );
        assert_eq!(stats.repaired, 1);
        assert_eq!(stats.stale_retracted, 1);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault` on OS `linux`
    fn alter_changing_name_is_repaired() {
        // Live row differs in `name` (RENAME-style mutation).
        let live = role_kind(12, "renamed@materialize.com", 20050, None, None, None);
        let dangling = role_kind(12, "original@materialize.com", 20050, None, None, None);
        let stale = stale_role_kind_with_dropped_field(12, "original@materialize.com", 20050);

        let snap = snapshot(vec![
            (stale.clone(), Diff::ONE),
            (dangling.clone(), Diff::MINUS_ONE),
            (live, Diff::ONE),
        ]);
        let (repairs, stats) = compute_repairs(&snap);
        assert_eq!(
            repairs,
            vec![(dangling, Diff::ONE), (stale, Diff::MINUS_ONE)],
        );
        assert_eq!(stats.repaired, 1);
        assert_eq!(stats.stale_retracted, 1);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault` on OS `linux`
    fn drop_role_shape_is_repaired() {
        // DROP ROLE on a v80-form-stuck role leaves a stale `+1` and a
        // dangling `-1` with no live `+1` sibling at all. The repair zeros
        // both rows so the key disappears from the shard entirely.
        let dangling = role_kind(13, "dropped@materialize.com", 20060, None, None, None);
        let stale = stale_role_kind_with_dropped_field(13, "dropped@materialize.com", 20060);

        let snap = snapshot(vec![
            (stale.clone(), Diff::ONE),
            (dangling.clone(), Diff::MINUS_ONE),
        ]);
        let (repairs, stats) = compute_repairs(&snap);
        assert_eq!(
            repairs,
            vec![(dangling, Diff::ONE), (stale, Diff::MINUS_ONE)],
        );
        assert_eq!(
            stats,
            RepairStats {
                repaired: 1,
                stale_retracted: 1,
                ..Default::default()
            }
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault` on OS `linux`
    fn dangling_minus_one_with_no_parsed_equal_plus_one_is_skipped() {
        // The dangling `-1` has no `+1` sibling at all; the bare retraction
        // is outside the structural signature.
        let dangling = role_kind(20, "ghost", 200, None, None, None);
        let snap = snapshot(vec![(dangling, Diff::MINUS_ONE)]);
        let (repairs, stats) = compute_repairs(&snap);
        assert!(repairs.is_empty());
        assert_eq!(stats.skipped_role, 1);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault` on OS `linux`
    fn dangling_minus_one_with_only_a_different_parsed_live_is_skipped() {
        // A `-1` sitting next to a single `+1` whose parsed value differs
        // looks like "free-floating retraction next to unrelated live state."
        // The bug requires a parsed-equal stale row; without one we won't
        // act, even though the soft_assert would still fire.
        let dangling = role_kind(21, "alice@materialize.com", 210, None, None, None);
        let live = role_kind(21, "alice@materialize.com", 210, Some(true), None, None);
        let snap = snapshot(vec![(dangling, Diff::MINUS_ONE), (live, Diff::ONE)]);
        let (repairs, stats) = compute_repairs(&snap);
        assert!(repairs.is_empty());
        assert_eq!(stats.skipped_role, 1);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault` on OS `linux`
    fn ambiguous_two_distinct_live_rows_is_skipped() {
        // Two `+1` rows for the same key with parsed values that differ
        // from each other AND from the dangling `-1`. We can't pick a live
        // truth, so we bail.
        let live_a = role_kind(22, "alice", 220, Some(true), None, None);
        let live_b = role_kind(22, "alice", 220, Some(false), Some(true), None);
        let dangling = role_kind(22, "alice", 220, None, None, None);
        let stale = stale_role_kind_with_dropped_field(22, "alice", 220);
        let snap = snapshot(vec![
            (stale, Diff::ONE),
            (live_a, Diff::ONE),
            (live_b, Diff::ONE),
            (dangling, Diff::MINUS_ONE),
        ]);
        let (repairs, stats) = compute_repairs(&snap);
        assert!(repairs.is_empty());
        assert_eq!(stats.skipped_role, 1);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault` on OS `linux`
    fn dangling_non_role_is_skipped() {
        let dangling = database_kind(1, "ghostdb");
        let snap = snapshot(vec![(dangling, Diff::MINUS_ONE)]);
        let (repairs, stats) = compute_repairs(&snap);
        assert!(repairs.is_empty());
        assert_eq!(stats.skipped_non_role, 1);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault` on OS `linux`
    fn dangling_diff_other_than_minus_one_is_skipped() {
        // Diff = -2 isn't the known bug — refuse to auto-fix.
        let dangling = role_kind(30, "arjun", 20032, None, None, None);
        let stale = stale_role_kind_with_dropped_field(30, "arjun", 20032);
        let live = role_kind(30, "arjun", 20032, Some(true), None, None);
        let snap = snapshot(vec![
            (dangling, Diff::MINUS_ONE + Diff::MINUS_ONE),
            (stale, Diff::ONE),
            (live, Diff::ONE),
        ]);
        let (repairs, stats) = compute_repairs(&snap);
        assert!(repairs.is_empty());
        assert_eq!(stats.skipped_role, 1);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault` on OS `linux`
    fn repair_with_multiple_stale_byte_forms_retracts_all() {
        // Pathological case: the same role has accreted *two* distinct stale
        // byte forms (e.g. the v80 form plus another byte-form drift from
        // some intermediate migration). The repair should retract both.
        let live = role_kind(40, "alice@materialize.com", 20070, Some(true), None, None);
        let dangling = role_kind(40, "alice@materialize.com", 20070, None, None, None);
        let stale_a = stale_role_kind_with_dropped_field(40, "alice@materialize.com", 20070);
        let stale_b = stale_role_kind_with_extra_whitespace(40, "alice@materialize.com", 20070);
        assert_ne!(stale_a, stale_b);

        let snap = snapshot(vec![
            (stale_a.clone(), Diff::ONE),
            (stale_b.clone(), Diff::ONE),
            (dangling.clone(), Diff::MINUS_ONE),
            (live, Diff::ONE),
        ]);
        let (repairs, stats) = compute_repairs(&snap);
        // The order of retractions follows snapshot iteration order, which
        // depends on the BTreeMap grouping. We assert by set rather than vec.
        let plus = repairs
            .iter()
            .filter(|(_, d)| *d == Diff::ONE)
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>();
        let minus = repairs
            .iter()
            .filter(|(_, d)| *d == Diff::MINUS_ONE)
            .map(|(k, _)| k.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(plus, vec![dangling]);
        let expected_minus: std::collections::BTreeSet<_> =
            [stale_a, stale_b].into_iter().collect();
        assert_eq!(minus, expected_minus);
        assert_eq!(stats.repaired, 1);
        assert_eq!(stats.stale_retracted, 2);
    }

    /// Like `stale_role_kind_with_dropped_field` but uses a different byte
    /// shape — explicit `auto_provision_source: null` plus a benign field-
    /// ordering tweak — to simulate yet another byte-form drift of the same
    /// parsed value. We're not asserting any specific other migration
    /// produces this exact shape; the test just exercises the repair's
    /// multi-stale-row handling.
    fn stale_role_kind_with_extra_whitespace(
        user_id: u64,
        name: &str,
        oid: u32,
    ) -> StateUpdateKindJson {
        use serde_json::json;
        // Differ from the canonical post-v82 form by including a stray
        // `password: null` key inside attributes — present in some
        // historical proto versions. Parses back the same; bytes differ.
        let v = json!({
            "kind": "Role",
            "key": { "id": { "User": user_id } },
            "value": {
                "name": name,
                "oid": oid,
                "attributes": {
                    "inherit": true,
                    "superuser": null,
                    "login": null,
                    "auto_provision_source": null,
                    "password": null,
                },
                "membership": { "map": [] },
                "vars": { "entries": [] },
            }
        });
        StateUpdateKindJson::from_serde(&v)
    }
}
