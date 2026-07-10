// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reshape proto `audit_log_event_v1::Details` JSON back into the shape
//! `mz_audit_log::EventDetails::as_json` produces.
//!
//! The `mz_audit_events` MV reads audit events from `mz_catalog_raw` as
//! their durable proto twin (`mz_catalog_protos::objects::audit_log_event_v1`)
//! and projects `details` through the SQL scalar
//! `parse_catalog_audit_log_details`. Its output must equal what the prior
//! `pack_audit_log_update` populator wrote, i.e.
//! `mz_audit_log::EventDetails::as_json`.
//!
//! The reshape is driven by five rule tables below plus two structural
//! rewrites (`StringWrapper` unwrap and the `ResetAllV1` null case). See the
//! individual table docstrings for their shapes. The reciprocal is
//! `EventDetails::as_json` combined with the proto `RustType` conversion in
//! `src/catalog-protos/src/audit_log.rs`. The round-trip property test at
//! `src/catalog/tests/audit_log_details.rs` samples every `Arbitrary`
//! variant and catches drift when either side gains a new variant, field,
//! or serde attribute.

use mz_expr_derive::sqlfunc;
use mz_repr::adt::jsonb::{Jsonb, JsonbRef};
use mz_repr::{Datum, Row, RowPacker};

use crate::EvalError;

/// Reshapes proto `audit_log_event_v1::Details` JSON from `mz_catalog_raw`
/// (e.g. `{"IdFullNameV1": {"id": "u1", "name": {...}}}`) into the shape
/// `mz_audit_log::EventDetails::as_json` produces: the format the prior
/// `pack_audit_log_update` populator wrote to `mz_audit_events`.
///
/// See the module docstring for the mechanism and the reciprocal side. Rule
/// changes must be paired with tests; the property test at
/// `src/catalog/tests/audit_log_details.rs` is the safety net.
#[sqlfunc]
fn parse_catalog_audit_log_details<'a>(a: JsonbRef<'a>) -> Result<Jsonb, EvalError> {
    /// `(variant, path, field, sub_variant)`: `#[serde(flatten)]` sites.
    /// `sub_variant` sets the context for rules on the hoisted content
    /// when the flattened struct itself has a `#[serde(flatten)]`.
    const FLATTENED_FIELDS: &[(&str, &str, &str, Option<&str>)] = &[
        ("IdFullNameV1", "", "name", None),
        ("CreateSourceSinkV1", "", "name", None),
        ("CreateSourceSinkV2", "", "name", None),
        ("CreateSourceSinkV3", "", "name", None),
        ("CreateSourceSinkV4", "", "name", None),
        ("CreateIndexV1", "", "name", None),
        ("CreateMetricSinkV1", "", "name", None),
        ("CreateMaterializedViewV1", "", "name", None),
        ("AlterSourceSinkV1", "", "name", None),
        ("AlterSetClusterV1", "", "name", None),
        ("UpdateItemV1", "", "name", None),
        // `AlterApplyReplacementV1.target: IdFullNameV1`, which itself
        // flattens `name: FullNameV1`.
        (
            "AlterApplyReplacementV1",
            "",
            "target",
            Some("IdFullNameV1"),
        ),
    ];

    /// `(variant, path, proto_key, audit_key)`: covers `#[serde(rename)]`
    /// on the audit-log side and Rust field-name diffs between the crates.
    const RENAMES: &[(&str, &str, &str, &str)] = &[
        ("CreateSourceSinkV2", "", "external_type", "type"),
        ("CreateSourceSinkV3", "", "external_type", "type"),
        ("CreateSourceSinkV4", "", "external_type", "type"),
        (
            "RefreshDecisionWithReasonV1",
            "",
            "rehydration_time_estimate",
            "hydration_time_estimate",
        ),
        (
            "RefreshDecisionWithReasonV2",
            "",
            "rehydration_time_estimate",
            "hydration_time_estimate",
        ),
    ];

    /// `(variant, path, field)`: drop when null. Keyed on the variant
    /// because the same field skips in V1 but is nullable in V2+
    /// (e.g. `replica_id`).
    const DROP_NULL: &[(&str, &str, &str)] = &[
        ("CreateClusterReplicaV1", "", "replica_id"),
        ("DropClusterReplicaV1", "", "replica_id"),
        ("CreateClusterReplicaV2", "", "scheduling_policies"),
        ("CreateClusterReplicaV3", "", "scheduling_policies"),
        ("CreateClusterReplicaV4", "", "scheduling_policies"),
        ("DropClusterReplicaV2", "", "scheduling_policies"),
        ("DropClusterReplicaV3", "", "scheduling_policies"),
        ("CreateMaterializedViewV1", "", "replacement_target_id"),
    ];

    /// Kebab map for `CreateOrDropClusterReplicaReasonV1`, shared across
    /// cluster-replica create and drop events.
    const REASON_MAP: &[(&str, &str)] = &[
        ("Manual", "manual"),
        ("Schedule", "schedule"),
        ("System", "system"),
        ("Reconfiguration", "reconfiguration"),
        ("HydrationBurst", "hydration-burst"),
        ("Retired", "retired"),
    ];

    /// `(variant, path, field, wrap, kebab_map)`: collapse a proto enum-
    /// with-`Empty`-payload into a kebab string. `Single` is `{"K": {}}`;
    /// `Double` is `{"<field>": {"K": {}}}`, where the outer key matches
    /// the field name in the current schemas (`reason`, `transition`).
    /// Unlisted variants error (fail-fast).
    #[allow(dead_code)]
    #[derive(Copy, Clone)]
    enum WrapKind {
        Single,
        Double,
    }
    const ENUM_COLLAPSE: &[(&str, &str, &str, WrapKind, &[(&str, &str)])] = &[
        (
            "RefreshDecisionWithReasonV1",
            "",
            "decision",
            WrapKind::Single,
            &[("On", "on"), ("Off", "off")],
        ),
        (
            "RefreshDecisionWithReasonV2",
            "",
            "decision",
            WrapKind::Single,
            &[("On", "on"), ("Off", "off")],
        ),
        (
            "CreateClusterReplicaV2",
            "",
            "reason",
            WrapKind::Double,
            REASON_MAP,
        ),
        (
            "CreateClusterReplicaV3",
            "",
            "reason",
            WrapKind::Double,
            REASON_MAP,
        ),
        (
            "CreateClusterReplicaV4",
            "",
            "reason",
            WrapKind::Double,
            REASON_MAP,
        ),
        (
            "DropClusterReplicaV2",
            "",
            "reason",
            WrapKind::Double,
            REASON_MAP,
        ),
        (
            "DropClusterReplicaV3",
            "",
            "reason",
            WrapKind::Double,
            REASON_MAP,
        ),
        (
            "AlterClusterReconfigurationV1",
            "",
            "transition",
            WrapKind::Double,
            &[
                ("Started", "started"),
                ("Finalized", "finalized"),
                ("TimedOut", "timed-out"),
                ("Cancelled", "cancelled"),
            ],
        ),
        (
            "ClusterHydrationBurstV1",
            "",
            "transition",
            WrapKind::Double,
            &[("Started", "started"), ("Finished", "finished")],
        ),
    ];

    /// `(variant, path, field, sub_variant)`: recurse with `sub_variant` as
    /// the new context so rules declared from its root fire. Used for
    /// versioned nested wrappers (`SchedulingDecisionsWithReasonsV1/V2`)
    /// and by-value `IdFullNameV1` fields whose inner `name: FullNameV1`
    /// needs flattening.
    const DESCEND: &[(&str, &str, &str, &str)] = &[
        ("AlterApplyReplacementV1", "", "replacement", "IdFullNameV1"),
        (
            "CreateClusterReplicaV2",
            "",
            "scheduling_policies",
            "SchedulingDecisionsWithReasonsV1",
        ),
        (
            "CreateClusterReplicaV3",
            "",
            "scheduling_policies",
            "SchedulingDecisionsWithReasonsV2",
        ),
        (
            "CreateClusterReplicaV4",
            "",
            "scheduling_policies",
            "SchedulingDecisionsWithReasonsV2",
        ),
        (
            "DropClusterReplicaV2",
            "",
            "scheduling_policies",
            "SchedulingDecisionsWithReasonsV1",
        ),
        (
            "DropClusterReplicaV3",
            "",
            "scheduling_policies",
            "SchedulingDecisionsWithReasonsV2",
        ),
        (
            "SchedulingDecisionsWithReasonsV1",
            "",
            "on_refresh",
            "RefreshDecisionWithReasonV1",
        ),
        (
            "SchedulingDecisionsWithReasonsV2",
            "",
            "on_refresh",
            "RefreshDecisionWithReasonV2",
        ),
    ];

    fn lookup_flatten(variant: &str, path: &str, field: &str) -> Option<Option<&'static str>> {
        FLATTENED_FIELDS
            .iter()
            .find(|(v, p, f, _)| *v == variant && *p == path && *f == field)
            .map(|(_, _, _, sv)| *sv)
    }

    fn lookup_rename(variant: &str, path: &str, field: &str) -> Option<&'static str> {
        RENAMES
            .iter()
            .find(|(v, p, k, _)| *v == variant && *p == path && *k == field)
            .map(|(_, _, _, out)| *out)
    }

    fn is_drop_null(variant: &str, path: &str, field: &str) -> bool {
        DROP_NULL
            .iter()
            .any(|(v, p, f)| *v == variant && *p == path && *f == field)
    }

    fn lookup_enum_collapse(
        variant: &str,
        path: &str,
        field: &str,
    ) -> Option<(WrapKind, &'static [(&'static str, &'static str)])> {
        ENUM_COLLAPSE
            .iter()
            .find(|(v, p, f, _, _)| *v == variant && *p == path && *f == field)
            .map(|(_, _, _, wrap, map)| (*wrap, *map))
    }

    fn lookup_descend(variant: &str, path: &str, field: &str) -> Option<&'static str> {
        DESCEND
            .iter()
            .find(|(v, p, f, _)| *v == variant && *p == path && *f == field)
            .map(|(_, _, _, sv)| *sv)
    }

    /// Unwrap a proto externally-tagged enum-with-`Empty`-payload and return
    /// its kebab form from `map`. Errors on unexpected shapes or unknown
    /// variants.
    fn collapse_enum(
        datum: Datum,
        wrap: WrapKind,
        map: &[(&'static str, &'static str)],
        context: &str,
    ) -> Result<&'static str, String> {
        let inner = match wrap {
            WrapKind::Single => datum,
            WrapKind::Double => {
                let Datum::Map(outer) = datum else {
                    return Err(format!(
                        "{context}: double-wrap enum expected outer object, got {datum:?}"
                    ));
                };
                let mut iter = outer.iter();
                let (Some((_, v)), None) = (iter.next(), iter.next()) else {
                    return Err(format!(
                        "{context}: double-wrap enum expected single-key outer object"
                    ));
                };
                v
            }
        };
        let Datum::Map(dict) = inner else {
            return Err(format!(
                "{context}: enum collapse expected object, got {inner:?}"
            ));
        };
        let mut iter = dict.iter();
        let (Some((variant_key, payload)), None) = (iter.next(), iter.next()) else {
            return Err(format!(
                "{context}: enum collapse expected single-key object"
            ));
        };
        // The payload must be `{}` (proto `Empty`); fail if a variant
        // grows a real payload in the future.
        if let Datum::Map(payload_dict) = payload {
            if payload_dict.iter().next().is_some() {
                return Err(format!(
                    "{context}: enum variant {variant_key} carries non-empty payload"
                ));
            }
        } else {
            return Err(format!(
                "{context}: enum variant {variant_key} payload is not an object"
            ));
        }
        map.iter()
            .find(|(k, _)| *k == variant_key)
            .map(|(_, kebab)| *kebab)
            .ok_or_else(|| format!("{context}: unknown enum variant {variant_key}"))
    }

    fn extend_path(path: &str, field: &str) -> String {
        if path.is_empty() {
            field.to_string()
        } else {
            format!("{path}.{field}")
        }
    }

    /// Recursively rewrite `datum` under `(variant, path)` into `out`.
    fn rewrite(datum: Datum, variant: &str, path: &str, out: &mut RowPacker) -> Result<(), String> {
        match datum {
            Datum::Map(dict) => {
                // Structural: `{"inner": v}` (proto `StringWrapper`) -> `v`.
                let mut iter = dict.iter();
                if let (Some((k, v)), None) = (iter.next(), iter.next()) {
                    if k == "inner" {
                        return rewrite(v, variant, path, out);
                    }
                }
                rewrite_map(dict, variant, path, out)
            }
            Datum::List(list) => {
                let mut result: Result<(), String> = Ok(());
                out.push_list_with(|out| {
                    for v in list.iter() {
                        if let Err(e) = rewrite(v, variant, path, out) {
                            result = Err(e);
                            break;
                        }
                    }
                });
                result
            }
            other => {
                out.push(other);
                Ok(())
            }
        }
    }

    fn rewrite_map(
        dict: mz_repr::DatumMap,
        variant: &str,
        path: &str,
        out: &mut RowPacker,
    ) -> Result<(), String> {
        let mut entries = collect_entries(dict, variant, path)?;
        // `Datum::Map` requires keys in ascending order.
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        out.push_dict_with(|out| {
            for (k, temp) in &entries {
                out.push(Datum::String(k));
                out.push(temp.unpack_first());
            }
        });
        Ok(())
    }

    /// Produce the transformed `(key, temp_row)` pairs for `dict` under
    /// `(variant, path)`. Split from `rewrite_map` so the flatten path can
    /// hoist a fully-processed sub-entry list into the parent.
    fn collect_entries(
        dict: mz_repr::DatumMap,
        variant: &str,
        path: &str,
    ) -> Result<Vec<(String, Row)>, String> {
        let mut entries: Vec<(String, Row)> = Vec::new();

        for (k, v) in dict.iter() {
            if is_drop_null(variant, path, k) && matches!(v, Datum::JsonNull) {
                continue;
            }

            // Flatten: hoist the sub-object's entries into the parent.
            if let Some(sub_variant_opt) = lookup_flatten(variant, path, k) {
                let sub_variant = sub_variant_opt.unwrap_or("");
                let Datum::Map(sub) = v else {
                    return Err(format!(
                        "expected flatten target {variant}.{k} to be an object"
                    ));
                };
                entries.extend(collect_entries(sub, sub_variant, "")?);
                continue;
            }

            let out_key = lookup_rename(variant, path, k).unwrap_or(k).to_string();

            // Enum collapse produces a computed string.
            if let Some((wrap, map)) = lookup_enum_collapse(variant, path, k) {
                let kebab = collapse_enum(v, wrap, map, &format!("{variant}.{k}"))?;
                let mut temp = Row::default();
                temp.packer().push(Datum::String(kebab));
                entries.push((out_key, temp));
                continue;
            }

            // Descend: recurse with a fresh variant context (path resets;
            // rules on the sub-variant are declared from its root).
            if let Some(sub_variant) = lookup_descend(variant, path, k) {
                let mut temp = Row::default();
                rewrite(v, sub_variant, "", &mut temp.packer())?;
                entries.push((out_key, temp));
                continue;
            }

            // Default: recurse with the same variant and an extended path.
            let child_path = extend_path(path, k);
            let mut temp = Row::default();
            rewrite(v, variant, &child_path, &mut temp.packer())?;
            entries.push((out_key, temp));
        }

        Ok(entries)
    }

    let parse = || -> Result<Jsonb, String> {
        let Datum::Map(dict) = a.into_datum() else {
            return Err("expected object".into());
        };
        let mut iter = dict.iter();
        let (variant, inner) = iter
            .next()
            .ok_or_else(|| "empty details enum".to_string())?;
        if iter.next().is_some() {
            return Err("details enum had multiple keys".into());
        }
        let mut row = Row::default();
        // `ResetAllV1` is the only variant `as_json` maps to null (the
        // proto `Empty` payload serializes to `{}`).
        if variant == "ResetAllV1" {
            row.packer().push(Datum::JsonNull);
            return Ok(Jsonb::from_row(row));
        }
        let Datum::Map(_) = inner else {
            return Err(format!("expected inner object for variant {variant}"));
        };
        rewrite(inner, variant, "", &mut row.packer())?;
        Ok(Jsonb::from_row(row))
    };

    parse().map_err(|e| EvalError::InvalidCatalogJson(e.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trip a JSON `input` through `parse_catalog_audit_log_details` and
    /// assert the resulting JSON parses equal to `expected`.
    fn check(input: &str, expected: &str) {
        let input: Jsonb = input.parse().expect("valid input JSONB");
        let actual = parse_catalog_audit_log_details(input.as_ref())
            .expect("helper succeeded")
            .to_string();
        let actual_value: serde_json::Value =
            serde_json::from_str(&actual).expect("valid output JSON");
        let expected_value: serde_json::Value =
            serde_json::from_str(expected).expect("valid expected JSON");
        assert_eq!(actual_value, expected_value);
    }

    /// Run `parse_catalog_audit_log_details` on `input` and assert it returns
    /// a `InvalidCatalogJson` error containing `expected_substr`.
    fn check_err(input: &str, expected_substr: &str) {
        let input: Jsonb = input.parse().expect("valid input JSONB");
        let err = parse_catalog_audit_log_details(input.as_ref()).expect_err("helper should error");
        let msg = format!("{err:?}");
        assert!(
            msg.contains(expected_substr),
            "error did not contain {expected_substr:?}: {msg}",
        );
    }

    /// Variant with no nested struct and no `#[serde(flatten)]`. The helper
    /// just strips the wrapper.
    #[mz_ore::test]
    fn variant_strip() {
        check(
            r#"{"IdNameV1": {"id": "u1", "name": "foo"}}"#,
            r#"{"id": "u1", "name": "foo"}"#,
        );
    }

    /// `IdFullNameV1` has `#[serde(flatten)] name: FullNameV1`. The helper
    /// must hoist `database`/`schema`/`item` to the top level.
    #[mz_ore::test]
    fn flatten_name() {
        check(
            r#"{"IdFullNameV1": {
                "id": "u1",
                "name": {"database": "materialize", "schema": "public", "item": "t"}
            }}"#,
            r#"{"id": "u1", "database": "materialize", "schema": "public", "item": "t"}"#,
        );
    }

    /// `AlterApplyReplacementV1` flattens `target: IdFullNameV1`, which itself
    /// flattens `name: FullNameV1`. Exercises the recursive sub-variant lookup.
    /// The non-flattened `replacement: IdFullNameV1` field has its inner
    /// `name` hoisted via a `DESCEND` entry pointing at `IdFullNameV1`.
    #[mz_ore::test]
    fn flatten_recursive_and_nested_full_name() {
        check(
            r#"{"AlterApplyReplacementV1": {
                "target": {
                    "id": "u1",
                    "name": {"database": "materialize", "schema": "public", "item": "mv"}
                },
                "replacement": {
                    "id": "u2",
                    "name": {"database": "materialize", "schema": "public", "item": "rp"}
                }
            }}"#,
            r#"{
                "id": "u1",
                "database": "materialize",
                "schema": "public",
                "item": "mv",
                "replacement": {
                    "id": "u2",
                    "database": "materialize",
                    "schema": "public",
                    "item": "rp"
                }
            }"#,
        );
    }

    /// `Option<StringWrapper>` fields serialize as `{"inner": "..."}` in the
    /// proto, where the audit-log crate uses a plain `String`. The helper must
    /// unwrap recursively (including on optional fields nested under flatten).
    #[mz_ore::test]
    fn string_wrapper_unwrap() {
        check(
            r#"{"AlterDefaultPrivilegeV1": {
                "role_id": "u1",
                "database_id": {"inner": "u2"},
                "schema_id": {"inner": "u3"},
                "grantee_id": "p",
                "privileges": "r"
            }}"#,
            r#"{
                "role_id": "u1",
                "database_id": "u2",
                "schema_id": "u3",
                "grantee_id": "p",
                "privileges": "r"
            }"#,
        );
    }

    /// Null `Option<StringWrapper>` fields are passed through as JSON null.
    #[mz_ore::test]
    fn null_option() {
        check(
            r#"{"AlterDefaultPrivilegeV1": {
                "role_id": "u1",
                "database_id": null,
                "schema_id": null,
                "grantee_id": "p",
                "privileges": "r"
            }}"#,
            r#"{
                "role_id": "u1",
                "database_id": null,
                "schema_id": null,
                "grantee_id": "p",
                "privileges": "r"
            }"#,
        );
    }

    /// Non-flattened nested objects pass through untouched. Guards against
    /// re-introducing a structural "hoist any `FullNameV1`-shaped object"
    /// heuristic: `RenameItemV1.old_name`/`new_name` are `FullNameV1` fields
    /// the audit-log side keeps nested, and any generic structural hoist
    /// would collide their `database`/`schema`/`item` keys.
    #[mz_ore::test]
    fn nested_full_name_stays_nested() {
        check(
            r#"{"RenameItemV1": {
                "id": "u1",
                "old_name": {"database": "d", "schema": "s", "item": "a"},
                "new_name": {"database": "d", "schema": "s", "item": "b"}
            }}"#,
            r#"{
                "id": "u1",
                "old_name": {"database": "d", "schema": "s", "item": "a"},
                "new_name": {"database": "d", "schema": "s", "item": "b"}
            }"#,
        );
    }

    /// Sources V2+ rename proto `external_type` to audit `type`. Value
    /// unchanged.
    #[mz_ore::test]
    fn rename_external_type() {
        check(
            r#"{"CreateSourceSinkV2": {
                "id": "u1",
                "name": {"database": "d", "schema": "s", "item": "src"},
                "size": {"inner": "small"},
                "external_type": "kafka"
            }}"#,
            r#"{
                "id": "u1",
                "database": "d",
                "schema": "s",
                "item": "src",
                "size": "small",
                "type": "kafka"
            }"#,
        );
    }

    /// The proto field `rehydration_time_estimate` becomes the audit field
    /// `hydration_time_estimate` — an invisible rename (no `#[serde(rename)]`
    /// on either side; just a genuine field-name diff between the crates).
    /// Nested under `scheduling_policies.on_refresh`, so the descent chain
    /// must land in the `RefreshDecisionWithReasonV1` variant context.
    #[mz_ore::test]
    fn rename_hydration_time_estimate_under_scheduling() {
        check(
            r#"{"CreateClusterReplicaV3": {
                "cluster_id": "u1",
                "cluster_name": "c",
                "replica_id": {"inner": "r1"},
                "replica_name": "n",
                "logical_size": "small",
                "disk": false,
                "billed_as": null,
                "internal": false,
                "reason": {"reason": {"Manual": {}}},
                "scheduling_policies": {
                    "on_refresh": {
                        "decision": {"On": {}},
                        "objects_needing_refresh": [],
                        "objects_needing_compaction": [],
                        "rehydration_time_estimate": "00:00:07"
                    }
                }
            }}"#,
            r#"{
                "cluster_id": "u1",
                "cluster_name": "c",
                "replica_id": "r1",
                "replica_name": "n",
                "logical_size": "small",
                "disk": false,
                "billed_as": null,
                "internal": false,
                "reason": "manual",
                "scheduling_policies": {
                    "on_refresh": {
                        "decision": "on",
                        "objects_needing_refresh": [],
                        "objects_needing_compaction": [],
                        "hydration_time_estimate": "00:00:07"
                    }
                }
            }"#,
        );
    }

    /// `CreateClusterReplicaV1.replica_id` uses
    /// `#[serde(skip_serializing_if = "Option::is_none")]` in the audit-log
    /// struct: a null proto value must be dropped from the output.
    #[mz_ore::test]
    fn drop_null_replica_id_v1() {
        check(
            r#"{"CreateClusterReplicaV1": {
                "cluster_id": "u1",
                "cluster_name": "c",
                "replica_id": null,
                "replica_name": "n",
                "logical_size": "small",
                "disk": false,
                "billed_as": null,
                "internal": false
            }}"#,
            r#"{
                "cluster_id": "u1",
                "cluster_name": "c",
                "replica_name": "n",
                "logical_size": "small",
                "disk": false,
                "billed_as": null,
                "internal": false
            }"#,
        );
    }

    /// V2+ struct keeps `replica_id` as a plain `Option<StringWrapper>` — no
    /// `skip_serializing_if`. A null value must round-trip as JSON null.
    /// This is the paired negative: the drop rule keys on (variant, field),
    /// not name alone.
    #[mz_ore::test]
    fn drop_null_replica_id_v2_kept() {
        check(
            r#"{"CreateClusterReplicaV2": {
                "cluster_id": "u1",
                "cluster_name": "c",
                "replica_id": null,
                "replica_name": "n",
                "logical_size": "small",
                "disk": false,
                "billed_as": null,
                "internal": false,
                "reason": {"reason": {"System": {}}},
                "scheduling_policies": null
            }}"#,
            r#"{
                "cluster_id": "u1",
                "cluster_name": "c",
                "replica_id": null,
                "replica_name": "n",
                "logical_size": "small",
                "disk": false,
                "billed_as": null,
                "internal": false,
                "reason": "system"
            }"#,
        );
    }

    /// Single-wrap enum collapse: `{"On":{}}` → `"on"`, applied under the
    /// `RefreshDecisionWithReasonV1` variant context.
    #[mz_ore::test]
    fn enum_single_wrap_decision() {
        check(
            r#"{"CreateClusterReplicaV2": {
                "cluster_id": "u1",
                "cluster_name": "c",
                "replica_id": {"inner": "r1"},
                "replica_name": "n",
                "logical_size": "small",
                "disk": false,
                "billed_as": null,
                "internal": false,
                "reason": {"reason": {"Schedule": {}}},
                "scheduling_policies": {
                    "on_refresh": {
                        "decision": {"Off": {}},
                        "objects_needing_refresh": [],
                        "rehydration_time_estimate": "00:00:00"
                    }
                }
            }}"#,
            r#"{
                "cluster_id": "u1",
                "cluster_name": "c",
                "replica_id": "r1",
                "replica_name": "n",
                "logical_size": "small",
                "disk": false,
                "billed_as": null,
                "internal": false,
                "reason": "schedule",
                "scheduling_policies": {
                    "on_refresh": {
                        "decision": "off",
                        "objects_needing_refresh": [],
                        "hydration_time_estimate": "00:00:00"
                    }
                }
            }"#,
        );
    }

    /// Double-wrap enum collapse: `{"reason":{"HydrationBurst":{}}}` →
    /// `"hydration-burst"`. Confirms the kebab-case mapping is used (the
    /// enum variant is `PascalCase` on the proto side).
    #[mz_ore::test]
    fn enum_double_wrap_reason_kebab() {
        check(
            r#"{"DropClusterReplicaV2": {
                "cluster_id": "u1",
                "cluster_name": "c",
                "replica_id": "r1",
                "replica_name": "n",
                "reason": {"reason": {"HydrationBurst": {}}},
                "scheduling_policies": null
            }}"#,
            r#"{
                "cluster_id": "u1",
                "cluster_name": "c",
                "replica_id": "r1",
                "replica_name": "n",
                "reason": "hydration-burst"
            }"#,
        );
    }

    /// `AlterClusterReconfigurationV1.transition` uses a distinct kebab map
    /// (`TimedOut` → `"timed-out"`, not shared with the reason map). Guards
    /// against accidentally reusing `REASON_MAP` for transitions.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
    fn enum_double_wrap_transition_timed_out() {
        check(
            r#"{"AlterClusterReconfigurationV1": {
                "cluster_id": "u1",
                "cluster_name": "c",
                "transition": {"transition": {"TimedOut": {}}},
                "target_size": "small",
                "target_replication_factor": 1,
                "target_availability_zones": [],
                "target_logging": {"log_logging": false, "interval": null},
                "deadline": null
            }}"#,
            r#"{
                "cluster_id": "u1",
                "cluster_name": "c",
                "transition": "timed-out",
                "target_size": "small",
                "target_replication_factor": 1,
                "target_availability_zones": [],
                "target_logging": {"log_logging": false, "interval": null},
                "deadline": null
            }"#,
        );
    }

    /// `ClusterHydrationBurstV1.transition` uses a two-value map
    /// (`Started`/`Finished`), separate from the reconfiguration lifecycle
    /// map. Same field name (`transition`), different rule, different map —
    /// dispatched by variant context.
    #[mz_ore::test]
    fn enum_double_wrap_hydration_burst_finished() {
        check(
            r#"{"ClusterHydrationBurstV1": {
                "cluster_id": "u1",
                "cluster_name": "c",
                "transition": {"transition": {"Finished": {}}},
                "burst_size": "small"
            }}"#,
            r#"{
                "cluster_id": "u1",
                "cluster_name": "c",
                "transition": "finished",
                "burst_size": "small"
            }"#,
        );
    }

    /// `ResetAllV1` is the only variant whose `as_json` returns JSON null.
    /// The proto side carries an `Empty` payload that serializes to `{}`,
    /// so a naive strip-the-wrapper would emit `{}`. Special-cased.
    #[mz_ore::test]
    fn reset_all_v1_is_null() {
        check(r#"{"ResetAllV1": {}}"#, r#"null"#);
    }

    /// An unlisted enum variant errors rather than being silently passed
    /// through — matches the fail-fast contract on this
    /// compliance-relevant table.
    #[mz_ore::test]
    fn enum_unknown_variant_errors() {
        check_err(
            r#"{"DropClusterReplicaV2": {
                "cluster_id": "u1",
                "cluster_name": "c",
                "replica_id": "r1",
                "replica_name": "n",
                "reason": {"reason": {"NoSuchVariant": {}}},
                "scheduling_policies": null
            }}"#,
            "unknown enum variant NoSuchVariant",
        );
    }

    /// The proto enum payload must be `Empty` (i.e. `{}`); a variant that
    /// grows a payload in the future would silently break the collapse, so
    /// we fail fast instead.
    #[mz_ore::test]
    fn enum_non_empty_payload_errors() {
        check_err(
            r#"{"DropClusterReplicaV2": {
                "cluster_id": "u1",
                "cluster_name": "c",
                "replica_id": "r1",
                "replica_name": "n",
                "reason": {"reason": {"Manual": {"unexpected": "x"}}},
                "scheduling_policies": null
            }}"#,
            "non-empty payload",
        );
    }

    /// Bad inputs: empty enum object, multiple keys, non-object.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
    fn error_cases() {
        check_err(r#"{}"#, "empty details enum");
        check_err(
            r#"{"A": {"x": 1}, "B": {"y": 2}}"#,
            "details enum had multiple keys",
        );
        check_err(r#"["IdNameV1", {"id": "u1"}]"#, "expected object");
        check_err(r#"{"IdNameV1": "not an object"}"#, "expected inner object");
    }
}
