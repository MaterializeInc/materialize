// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Repair migration for the v80→v81 `is_cloud` heuristic bug.
//!
//! The v80→v81 migration (`v80_to_v81.rs`) was guarded by an `is_cloud`
//! heuristic that returned false for any catalog where, at the moment the
//! migration committed in writable mode, `mz_system` was not a `Managed`
//! cluster with `replication_factor > 0`, or where `enable_password_auth`
//! was `"on"`. Affected catalogs ran the migration with the heuristic
//! returning false; the migration emitted `Vec::new()` and left every
//! existing `Role` record's on-disk JSON in v80 wire shape — that is,
//! without an `auto_provision_source` key.
//!
//! On v81+ the value is still readable (serde fills `None` for the missing
//! `Option<AutoProvisionSource>` field), so the env doesn't crash. But any
//! in-band write that retracts a Role record — `ALTER ROLE`, membership
//! changes, etc. — re-serializes the deserialized value through the v81
//! type's `Serialize`, which emits `"auto_provision_source": null`. Those
//! retraction bytes are not byte-equal to the v80-shape bytes on disk, so
//! `differential_dataflow` consolidation cannot cancel them; the original
//! `+1` survives as a ghost record alongside whatever else accumulates.
//!
//! This migration:
//!
//!   1. Iterates every Role record paired with its raw on-disk JSON.
//!   2. Inspects the raw JSON to determine whether `auto_provision_source`
//!      is present as a key in `value.attributes`.
//!   3. Decides the target `auto_provision_source`:
//!      - Field present and `Some(...)` on disk → preserve the value.
//!      - Field absent on disk (v80 wire shape) → apply the same email
//!        regex heuristic as the original v80→v81 migration:
//!        `Frontegg` if the role name matches `.+@.+\..+`, else `None`.
//!        The classification is **unconditional** — no `is_cloud` gate.
//!        That avoids reintroducing the same trap.
//!      - Field present and explicitly `null` on disk → `None`.
//!   4. Constructs the canonical v83 Role bytes (v83 uses
//!      `#[serde(default, skip_serializing_if = "Option::is_none")]` on
//!      `auto_provision_source`, so `None` round-trips to no-field and
//!      `Some(_)` round-trips to the value — byte-stable in both cases).
//!   5. If the canonical v83 bytes differ from the on-disk bytes, emits a
//!      raw retraction of the exact on-disk bytes and an insertion of the
//!      canonical v83 bytes. The byte-precise retraction is why this
//!      migration uses [`run_versioned_upgrade_raw`] rather than the
//!      standard `MigrationAction::Update` path.
//!
//! Records other than Role are untouched. v83 is structurally identical to
//! v82 for every other type, so existing v82-shape bytes remain valid v83
//! bytes without rewriting.

use mz_repr::Diff;
use mz_repr::adt::regex::Regex;

use crate::durable::objects::state_update::StateUpdateKindJson;
use crate::durable::upgrade::json_compatible::JsonCompatible;
use crate::durable::upgrade::objects_v82 as v82;
use crate::durable::upgrade::objects_v83 as v83;

pub fn upgrade(
    snapshot: Vec<(StateUpdateKindJson, v82::StateUpdateKind)>,
) -> Vec<(StateUpdateKindJson, Diff)> {
    let email_regex = Regex::new(r".+@.+\..+", true).expect("valid regex");
    let mut updates = Vec::new();

    for (raw, kind) in snapshot {
        let v82::StateUpdateKind::Role(role) = kind else {
            continue;
        };

        let field_present_on_disk = role_has_auto_provision_source_key(&raw);

        let target_aps: Option<v83::AutoProvisionSource> = match (
            &role.value.attributes.auto_provision_source,
            field_present_on_disk,
        ) {
            // Field already set to a concrete value — preserve it.
            (Some(aps), _) => Some(map_aps_v82_to_v83(aps)),
            // Field missing on disk: pre-v81 wire shape. Apply email
            // heuristic, unconditionally.
            (None, false) => {
                if email_regex.is_match(&role.value.name) {
                    Some(v83::AutoProvisionSource::Frontegg)
                } else {
                    None
                }
            }
            // Field present-and-null on disk: preserve None.
            (None, true) => None,
        };

        let new_role = v83::StateUpdateKind::Role(v83::Role {
            key: JsonCompatible::convert(&role.key),
            value: v83::RoleValue {
                name: role.value.name.clone(),
                attributes: v83::RoleAttributes {
                    inherit: role.value.attributes.inherit,
                    superuser: role.value.attributes.superuser,
                    login: role.value.attributes.login,
                    auto_provision_source: target_aps,
                },
                membership: JsonCompatible::convert(&role.value.membership),
                vars: JsonCompatible::convert(&role.value.vars),
                oid: role.value.oid,
            },
        });
        let new_raw: StateUpdateKindJson = new_role.into();

        if new_raw == raw {
            // Already in canonical v83 shape — no rewrite needed.
            continue;
        }

        // Retract by exact on-disk bytes; insert canonical v83.
        updates.push((raw, Diff::MINUS_ONE));
        updates.push((new_raw, Diff::ONE));
    }

    updates
}

/// Returns true iff the raw JSON for a `Role` `StateUpdateKind` contains an
/// `auto_provision_source` key inside `value.attributes`. The value (`null`
/// vs concrete) is not inspected here.
fn role_has_auto_provision_source_key(raw: &StateUpdateKindJson) -> bool {
    let Ok(value): Result<serde_json::Value, _> = raw.try_to_serde() else {
        return false;
    };
    value
        .get("Role")
        .and_then(|r| r.get("value"))
        .and_then(|v| v.get("attributes"))
        .and_then(|a| a.as_object())
        .map(|attrs| attrs.contains_key("auto_provision_source"))
        .unwrap_or(false)
}

fn map_aps_v82_to_v83(aps: &v82::AutoProvisionSource) -> v83::AutoProvisionSource {
    match aps {
        v82::AutoProvisionSource::Oidc => v83::AutoProvisionSource::Oidc,
        v82::AutoProvisionSource::Frontegg => v83::AutoProvisionSource::Frontegg,
        v82::AutoProvisionSource::None => v83::AutoProvisionSource::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable::upgrade::objects_v82 as v82;

    fn make_v82_role_raw(name: &str, include_aps: bool, aps: Option<&str>) -> StateUpdateKindJson {
        // Build the JSON shape directly so we can produce both v80-shape
        // (no auto_provision_source key) and v82-shape (key present) bytes.
        let attrs = if include_aps {
            match aps {
                Some(v) => serde_json::json!({
                    "inherit": true,
                    "superuser": null,
                    "login": null,
                    "auto_provision_source": v,
                }),
                None => serde_json::json!({
                    "inherit": true,
                    "superuser": null,
                    "login": null,
                    "auto_provision_source": null,
                }),
            }
        } else {
            serde_json::json!({
                "inherit": true,
                "superuser": null,
                "login": null,
            })
        };
        let full = serde_json::json!({
            "Role": {
                "key": { "id": { "User": 1 } },
                "value": {
                    "name": name,
                    "attributes": attrs,
                    "membership": { "map": [] },
                    "vars": { "entries": [] },
                    "oid": 1u32,
                },
            }
        });
        StateUpdateKindJson::from_serde(&full)
    }

    #[mz_ore::test]
    fn test_v80_shape_email_named_gets_frontegg() {
        let raw = make_v82_role_raw("user@example.com", false, None);
        let kind: v82::StateUpdateKind = raw.clone().into();
        let updates = upgrade(vec![(raw.clone(), kind)]);
        // 1 retraction, 1 insertion
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].1, Diff::MINUS_ONE);
        assert_eq!(updates[0].0, raw, "retraction must byte-match on-disk");
        assert_eq!(updates[1].1, Diff::ONE);
        // The insertion's value should now decode as v83 with Frontegg.
        let new_kind: v83::StateUpdateKind = updates[1].0.clone().into();
        let v83::StateUpdateKind::Role(new_role) = new_kind else {
            panic!("expected Role");
        };
        assert_eq!(
            new_role.value.attributes.auto_provision_source,
            Some(v83::AutoProvisionSource::Frontegg)
        );
    }

    #[mz_ore::test]
    fn test_v80_shape_non_email_stays_none_and_normalizes() {
        let raw = make_v82_role_raw("service_account", false, None);
        let kind: v82::StateUpdateKind = raw.clone().into();
        let updates = upgrade(vec![(raw.clone(), kind)]);
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].0, raw, "retraction must byte-match on-disk");
        // Insertion's value decodes with None. v83 serializes None as
        // skip-field, so re-deserializing yields None and re-serializing
        // back is byte-stable.
        let new_kind: v83::StateUpdateKind = updates[1].0.clone().into();
        let v83::StateUpdateKind::Role(new_role) = new_kind else {
            panic!("expected Role");
        };
        assert_eq!(new_role.value.attributes.auto_provision_source, None);
        // Round-trip: re-serializing should equal the canonical insertion.
        let reserialized: StateUpdateKindJson = v83::StateUpdateKind::Role(new_role).into();
        assert_eq!(reserialized, updates[1].0);
    }

    #[mz_ore::test]
    fn test_v82_shape_frontegg_preserves_value() {
        let raw = make_v82_role_raw("user@example.com", true, Some("Frontegg"));
        let kind: v82::StateUpdateKind = raw.clone().into();
        let updates = upgrade(vec![(raw.clone(), kind)]);
        // v82-shape bytes (with field present) differ from v83 canonical
        // (which would also have field present for Some) only if the v82
        // and v83 enum serializations differ — they don't. So no rewrite.
        assert!(
            updates.is_empty(),
            "v82-shape role with Frontegg should be canonical v83 already"
        );
    }

    #[mz_ore::test]
    fn test_v82_shape_explicit_null_becomes_no_field() {
        let raw = make_v82_role_raw("user@example.com", true, None);
        let kind: v82::StateUpdateKind = raw.clone().into();
        let updates = upgrade(vec![(raw.clone(), kind)]);
        // On-disk: `"auto_provision_source": null`. Canonical v83: no field.
        // Different bytes → rewrite expected. And: because the field was
        // *present* on disk, we DO NOT apply the email regex — we preserve
        // None.
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].0, raw, "retraction must byte-match on-disk");
        let new_kind: v83::StateUpdateKind = updates[1].0.clone().into();
        let v83::StateUpdateKind::Role(new_role) = new_kind else {
            panic!("expected Role");
        };
        assert_eq!(
            new_role.value.attributes.auto_provision_source, None,
            "field was explicitly null on disk; must not be reclassified",
        );
    }

    #[mz_ore::test]
    fn test_non_role_records_untouched() {
        // Build a Setting record and confirm it's not touched.
        let kind = v82::StateUpdateKind::Setting(v82::Setting {
            key: v82::SettingKey {
                name: "test".to_string(),
            },
            value: v82::SettingValue {
                value: "x".to_string(),
            },
        });
        let raw: StateUpdateKindJson = kind.clone().into();
        let updates = upgrade(vec![(raw, kind)]);
        assert!(updates.is_empty());
    }
}
