// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Repair Role rows left in an inconsistent state by the v80->v81 migration.
//!
//! # Background
//!
//! The catalog persist shard requires that every `(key, ts)` tuple
//! consolidate to `Diff::ONE`. Catalog writers retract by re-serializing the
//! in-memory parsed value through the current proto; this only consolidates
//! cleanly if the round-trip is byte-exact (database-issues#7179). Whenever
//! a proto adds a field, that invariant breaks for rows written before the
//! field existed: the stored row lacks the key entirely while the
//! re-serialized retraction writes it as explicit `null`, so the retraction
//! never cancels its target.
//!
//! # The specific failure this migration targets
//!
//! `v80_to_v81::upgrade` was supposed to backfill `auto_provision_source` on
//! every existing Role row. That backfill was gated on an `is_cloud`
//! heuristic that required the `mz_system` cluster to be
//! `ClusterVariant::Managed`; on envs where it wasn't, the heuristic returned
//! false and the migration silently no-opped. The version bump committed
//! anyway, but every Role row kept its v80 form.
//!
//! After v26.18 any DDL touching such a row (`ALTER ROLE`, role membership
//! changes, `DROP ROLE`) parses the row, then writes a retract+insert pair
//! through current protos that *do* include the new field. The retraction
//! doesn't cancel, and the shard ends up holding three rows per affected
//! role:
//!
//!   * a stale `+1` in the pre-v81 form,
//!   * a dangling `-1` in the current form (the retraction that missed),
//!   * a live `+1` in the current form reflecting whatever the DDL did.
//!
//! For `DROP ROLE` the third row is absent — the role is gone, but the first
//! two persist forever.
//!
//! # The repair
//!
//! Two passes over the snapshot.
//!
//! **Pass 1 — cancel already-dangling retractions.** For every Role with the
//! structural signature of the bug — a dangling `-1` plus at least one `+1`
//! whose parsed `RoleValue` equals it, plus at most one *other* `+1` with a
//! different parsed value — we emit:
//!
//!   1. `+1` of the dangling row, cancelling the dangling `-1`.
//!   2. `-1` of every parsed-equal stale `+1`, completing the retraction the
//!      original DDL intended.
//!
//! **Pass 2 — normalize untouched stale rows.** Every remaining `+1` Role
//! row whose stored form differs from what re-serializing its parsed value
//! through the current proto would produce is retracted and re-inserted in
//! canonical form. Without this, a Role still in pre-v81 form that hasn't
//! yet had any DDL run against it would survive the migration unchanged,
//! and the next `ALTER ROLE`/`DROP ROLE` after v83 would manufacture a
//! fresh dangling `-1` against bytes that no migration runs against
//! anymore. After pass 2, every Role row in the shard has the byte form
//! that future retractions will also produce, so consolidation cancels.
//!
//! After commit, each affected `RoleKey` has either one live canonical
//! `+1` or no rows at all (for the dropped case).
//!
//! Anything that doesn't fit the fingerprint — no parsed-equal sibling,
//! multiple distinct live candidates, non-Role kinds, `|diff| > 1` — is
//! logged at WARN and left for human review, and pass 2 also leaves every
//! `+1` for any such key alone (rewriting a subset of an unrepaired key
//! would just produce a new dangling diff). Better to under-clean and
//! surface unknown shapes for triage than over-clean and retire live state.

use std::collections::{BTreeMap, BTreeSet};

use mz_repr::Diff;

use crate::durable::objects::state_update::{StateUpdate, StateUpdateKindJson};
use crate::durable::persist::{Mode, Timestamp, UnopenedPersistCatalogState};
use crate::durable::upgrade::objects_v83 as v83;
use crate::durable::{CatalogError, initialize::USER_VERSION_KEY};

const FROM_VERSION: u64 = 83;
const TO_VERSION: u64 = 84;

/// Outcome counters for the repair, returned for logging and assertable in
/// tests.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct RepairStats {
    /// Role retraction phantoms matching the v80-form-drift signature that
    /// were cancelled by writing a compensating `+1`.
    pub repaired: usize,
    /// Stale `+1` rows (alternate forms of the same parsed Role value as a
    /// repaired phantom) that were retracted as part of the repair.
    pub stale_retracted: usize,
    /// Live `+1` Role rows whose stored form didn't match the canonical
    /// re-serialization of their parsed value, rewritten in place.
    pub normalized: usize,
    /// Dangling Role `-1`s that didn't fit the structural signature.
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
        "running versioned Catalog upgrade (repair Role row drift)",
    );

    let (repairs, stats) = compute_repairs(&unopened_catalog_state.snapshot);

    if !repairs.is_empty() {
        tracing::info!(
            repaired = stats.repaired,
            stale_retracted = stats.stale_retracted,
            normalized = stats.normalized,
            "repairing Role rows left inconsistent by the v80->v81 migration's non-cloud no-op",
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

    // Print the updates for inspection.
    tracing::info!(
        "catalog upgrade v{FROM_VERSION}_to_v{TO_VERSION}: about to apply updates: {updates:?}"
    );

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

/// Inspect a consolidated snapshot and return the updates needed to converge
/// every affected Role onto a single canonical-form `+1` (or zero rows, for
/// the dropped case), and every untouched Role onto canonical form so future
/// writers' retractions consolidate.
///
/// The returned `Vec` is safe to feed straight into `compare_and_append`.
///
/// Two passes:
///
/// 1. For each dangling `-1` Role row matching the v80-form-drift signature,
///    cancel it (`+1` of the same bytes) and retract every parsed-equal stale
///    `+1` sibling.
/// 2. For each remaining `+1` Role row whose stored bytes don't match the
///    canonical re-serialization of its parsed value, retract it and insert
///    the canonical form. Skipped for any `RoleKey` whose dangling `-1` we
///    declined to repair in pass 1 — partial rewriting there would manufacture
///    a fresh dangling diff against the canonical form.
///
/// Separated from `upgrade` so it can be unit-tested without spinning up a
/// real catalog handle.
pub(crate) fn compute_repairs(
    snapshot: &[(StateUpdateKindJson, Timestamp, Diff)],
) -> (Vec<(StateUpdateKindJson, Diff)>, RepairStats) {
    // Group every Role `+1` row by its parsed `RoleKey`. We need the full set
    // (not just one representative) so we can identify the live row vs any
    // stale siblings — they all live under the same key.
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
    // `+1` rows pass 1 already retracts: pass 2 must not double-retract them.
    let mut retracted_in_pass_1: BTreeSet<&StateUpdateKindJson> = BTreeSet::new();
    // Role keys with an unrepaired dangling diff: pass 2 leaves their `+1`s
    // alone, since normalizing only some bytes for a key that still has a
    // dangling `-1` would just shift the consolidation failure onto the new
    // canonical form.
    let mut unrepaired_keys: BTreeSet<v83::RoleKey> = BTreeSet::new();
    for (kind_json, _, diff) in snapshot {
        if *diff == Diff::ONE {
            continue;
        }

        // Non-Role dangling diffs are outside the scope of this targeted
        // repair and need human triage.
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
            unrepaired_keys.insert(dangling.key);
            continue;
        }

        // Classify each `+1` sibling of this `-1`:
        //
        //   - `stale`: parsed value equals the dangling row's. Another form
        //     of "the state the original retraction was supposed to cancel".
        //     We retract these.
        //   - `live`: parsed value differs. The post-mutation state. At most
        //     one such row is permitted; multiple is ambiguous and we bail.
        //
        // A dangling `-1` with no stale sibling looks like a free-floating
        // retraction; we won't act on it either.
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
            unrepaired_keys.insert(dangling.key);
            continue;
        }
        if ambiguous_live {
            tracing::warn!(
                role_name = %dangling.value.name,
                num_siblings = siblings.len(),
                "Role key has multiple distinct live +1 rows; refusing to auto-repair",
            );
            stats.skipped_role += 1;
            unrepaired_keys.insert(dangling.key);
            continue;
        }

        tracing::info!(
            role_name = %dangling.value.name,
            stale_byte_forms = stale.len(),
            has_live = live.is_some(),
            "repairing v80-form-drift phantom retraction",
        );
        // 1. Cancel the dangling `-1` by writing a matching `+1`.
        repairs.push((kind_json.clone(), Diff::ONE));
        // 2. Retract every stale `+1` row sharing the dangling row's parsed
        //    value. The original retraction was supposed to cancel one of
        //    these; we replay that intent against the actual stored row.
        for s in stale {
            // Don't retract the dangling row's own stored form — step 1
            // already cancels it.
            if s.bytes == kind_json {
                continue;
            }
            repairs.push((s.bytes.clone(), Diff::MINUS_ONE));
            retracted_in_pass_1.insert(s.bytes);
            stats.stale_retracted += 1;
        }
        stats.repaired += 1;
    }

    // Pass 2: normalize every remaining live `+1` Role row whose stored bytes
    // don't equal what re-serializing its parsed value through the current
    // proto would produce. Any future retraction will use canonical bytes;
    // unless what's stored matches, the retract+insert pair from that future
    // write will dangle a fresh `-1`.
    //
    // Dedupe canonical inserts per parsed value so two distinct stale byte
    // forms of the same role collapse to a single `+1` rather than `+2`.
    let mut inserted_canonical: BTreeSet<StateUpdateKindJson> = BTreeSet::new();
    for (kind_json, _, diff) in snapshot {
        if *diff != Diff::ONE {
            continue;
        }
        let Some(role) = try_as_role(kind_json) else {
            continue;
        };
        if retracted_in_pass_1.contains(kind_json) {
            continue;
        }
        if unrepaired_keys.contains(&role.key) {
            continue;
        }
        let canonical: StateUpdateKindJson = v83::StateUpdateKind::Role(role.clone()).into();
        if &canonical == kind_json {
            continue;
        }
        tracing::info!(
            role_name = %role.value.name,
            "normalizing pre-v81 Role row to canonical byte form",
        );
        repairs.push((kind_json.clone(), Diff::MINUS_ONE));
        if inserted_canonical.insert(canonical.clone()) {
            repairs.push((canonical, Diff::ONE));
        }
        stats.normalized += 1;
    }

    (repairs, stats)
}

/// A `+1` Role row borrowed from the snapshot. Carries both the stored form
/// (so retractions can target the exact row) and the parsed value (so we can
/// compare semantic equality across stored forms).
struct RolePlusOne<'a> {
    bytes: &'a StateUpdateKindJson,
    parsed: v83::Role,
}

/// Returns the parsed Role iff `kind_json` is one. Returns `None` for any
/// other kind, or for rows we can't deserialize as the current Role shape
/// (which we treat as "leave alone" — losing that row to the repair would be
/// worse than the soft_assert noise).
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
    /// the `auto_provision_source` key — mirroring the v80-era form that
    /// survived a non-cloud-detecting `v80→v81` migration. Parses back to a
    /// Role with `auto_provision_source: None` (serde fills the missing
    /// `Option` field with `None`) but its stored form differs from the
    /// post-v80 shape that always writes the key explicitly.
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
        // The shape pulled from the affected env: a stale v80-form `+1`
        // lacking `auto_provision_source`, a dangling v82-form `-1` with the
        // key explicit, and a live v82-form `+1` reflecting ALTER ROLE LOGIN.
        let live = role_kind(8, "alice@example.com", 20030, Some(true), None, None);
        let dangling = role_kind(8, "alice@example.com", 20030, None, None, None);
        let stale = stale_role_kind_with_dropped_field(8, "alice@example.com", 20030);

        assert_eq!(
            try_as_role(&stale).expect("parses as Role").value,
            try_as_role(&dangling).expect("parses as Role").value,
            "test fixture broken: stale and dangling must be parsed-equal",
        );
        assert_ne!(
            stale, dangling,
            "test fixture broken: stale and dangling must have distinct stored forms",
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
        // in a different attribute. The predicate is structural, not
        // attribute-specific.
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
        // stored forms (the v80 form plus another drift from some
        // intermediate migration). The repair should retract both.
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

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault` on OS `linux`
    fn untouched_pre_v81_role_is_normalized() {
        // A Role row stuck in pre-v81 byte form because no DDL has touched
        // it since the v80->v81 no-op. No dangling `-1` yet — but the next
        // ALTER ROLE under the current proto would manufacture one. Pass 2
        // rewrites the row in canonical form so that doesn't happen.
        let stale = stale_role_kind_with_dropped_field(50, "carol@materialize.com", 20100);
        let canonical = role_kind(50, "carol@materialize.com", 20100, None, None, None);
        assert_ne!(
            stale, canonical,
            "test fixture broken: stale and canonical must have distinct stored forms",
        );

        let snap = snapshot(vec![(stale.clone(), Diff::ONE)]);
        let (repairs, stats) = compute_repairs(&snap);
        assert_eq!(
            repairs,
            vec![(stale, Diff::MINUS_ONE), (canonical, Diff::ONE)],
        );
        assert_eq!(
            stats,
            RepairStats {
                normalized: 1,
                ..Default::default()
            },
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault` on OS `linux`
    fn canonical_form_role_is_not_normalized() {
        // Belt-and-braces companion to `healthy_snapshot_is_a_noop`:
        // explicitly verify that a `+1` whose stored form already matches
        // its canonical re-serialization triggers no normalize work.
        let canonical = role_kind(51, "dave@materialize.com", 20101, Some(true), None, None);
        let snap = snapshot(vec![(canonical, Diff::ONE)]);
        let (repairs, stats) = compute_repairs(&snap);
        assert!(repairs.is_empty());
        assert_eq!(stats.normalized, 0);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault` on OS `linux`
    fn multiple_stale_byte_forms_no_dangling_collapse_to_one_canonical() {
        // Pathological: two distinct stale stored forms of the same parsed
        // Role value, both `+1`, no dangling `-1`. Pass 2 must retract both
        // but only insert the canonical row once — emitting `(canonical, +1)`
        // per stale row would leave the shard with `(canonical, +2)`.
        let stale_a = stale_role_kind_with_dropped_field(60, "ed@materialize.com", 20110);
        let stale_b = stale_role_kind_with_extra_whitespace(60, "ed@materialize.com", 20110);
        let canonical = role_kind(60, "ed@materialize.com", 20110, None, None, None);
        assert_ne!(stale_a, stale_b);
        assert_ne!(stale_a, canonical);
        assert_ne!(stale_b, canonical);

        let snap = snapshot(vec![
            (stale_a.clone(), Diff::ONE),
            (stale_b.clone(), Diff::ONE),
        ]);
        let (repairs, stats) = compute_repairs(&snap);

        let plus: Vec<_> = repairs
            .iter()
            .filter(|(_, d)| *d == Diff::ONE)
            .cloned()
            .collect();
        let minus: std::collections::BTreeSet<_> = repairs
            .iter()
            .filter(|(_, d)| *d == Diff::MINUS_ONE)
            .map(|(k, _)| k.clone())
            .collect();
        assert_eq!(plus, vec![(canonical, Diff::ONE)]);
        let expected_minus: std::collections::BTreeSet<_> =
            [stale_a, stale_b].into_iter().collect();
        assert_eq!(minus, expected_minus);
        assert_eq!(stats.normalized, 2);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault` on OS `linux`
    fn unrepaired_dangling_skips_normalize_for_same_key() {
        // Pass 1 declines to repair this key (ambiguous live state). Pass 2
        // must also leave its stale `+1` alone — normalizing only the stale
        // row would just shift the dangling diff onto the canonical bytes,
        // turning a known-skip into a new dangling row.
        let live_a = role_kind(70, "frank", 20120, Some(true), None, None);
        let live_b = role_kind(70, "frank", 20120, Some(false), Some(true), None);
        let dangling = role_kind(70, "frank", 20120, None, None, None);
        let stale = stale_role_kind_with_dropped_field(70, "frank", 20120);

        let snap = snapshot(vec![
            (stale.clone(), Diff::ONE),
            (live_a, Diff::ONE),
            (live_b, Diff::ONE),
            (dangling, Diff::MINUS_ONE),
        ]);
        let (repairs, stats) = compute_repairs(&snap);
        assert!(repairs.is_empty());
        assert_eq!(stats.skipped_role, 1);
        assert_eq!(stats.normalized, 0);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault` on OS `linux`
    fn normalize_does_not_double_retract_pass_1_stale_rows() {
        // A repair-fingerprint role: pass 1 already retracts the stale `+1`.
        // Pass 2 must not retract it a second time, or the row goes to
        // `-2` and the shard is worse off than before.
        let live = role_kind(80, "grace@materialize.com", 20130, Some(true), None, None);
        let dangling = role_kind(80, "grace@materialize.com", 20130, None, None, None);
        let stale = stale_role_kind_with_dropped_field(80, "grace@materialize.com", 20130);

        let snap = snapshot(vec![
            (stale.clone(), Diff::ONE),
            (dangling.clone(), Diff::MINUS_ONE),
            (live, Diff::ONE),
        ]);
        let (repairs, stats) = compute_repairs(&snap);

        // The stale row appears exactly once with diff -1.
        let stale_retractions = repairs
            .iter()
            .filter(|(k, d)| k == &stale && *d == Diff::MINUS_ONE)
            .count();
        assert_eq!(stale_retractions, 1);
        assert_eq!(stats.repaired, 1);
        assert_eq!(stats.stale_retracted, 1);
        assert_eq!(stats.normalized, 0);
    }

    /// Like `stale_role_kind_with_dropped_field` but a different stored
    /// shape that still parses to the same value, used to exercise the
    /// repair's multi-stale-row handling.
    fn stale_role_kind_with_extra_whitespace(
        user_id: u64,
        name: &str,
        oid: u32,
    ) -> StateUpdateKindJson {
        use serde_json::json;
        // Differ from the canonical post-v82 form by including a stray
        // `password: null` key inside attributes. Parses back the same; the
        // stored form differs.
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
