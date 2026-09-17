// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Snapshot of the scalar function property registry per LIR version.
//!
//! [`FuncRegistry::build`] records the declared properties of every
//! `UnaryFunc`, `BinaryFunc` and `VariadicFunc` variant, and this test
//! compares them against `tests/snapshots/func_registry_v{LIR_VERSION}.json`.
//! It complements the LIR schema snapshot in `lir_schema.rs`, which pins the
//! serialized shape of these enums but not what the variants mean. The
//! registry lives in `mz-expr` behind its `func-registry` feature, which this
//! crate's dev-dependency enables.
//!
//! A property change (null propagation, error behavior, monotonicity, output
//! typing, an inverse or negation, and so on) changes what a stored plan
//! computes or what the optimizer assumed when producing it. Once a LIR
//! version has shipped, such a change must bump `LIR_VERSION` so pinned plans
//! of the old version are replanned. Changes confined to
//! [`INFORMATIONAL_FIELDS`] are reported separately, because they require
//! judgment---they may or may not be breaking.

use std::collections::BTreeMap;

use mz_compute_types::plan::LIR_VERSION;
use mz_expr::func::registry::FuncRegistry;

const SNAPSHOT_DIR: &str = "tests/snapshots";

/// The freshly built registry, rewritten on every run. Gitignored. Diff it
/// against the checked-in snapshot to see exactly what changed.
const CURRENT_PATH: &str = "tests/snapshots/func_registry_current.json";

/// Record fields whose change does not by itself alter what a stored plan
/// computes or how it was optimized.
///
/// `body_fingerprint` tracks the implementation---changes may or may not be
/// breaking. `sqlfunc_decl` is source text: the properties it declares are
/// recorded as their own fields (`sqlfunc_signature` carries the parameter and
/// return types), so what remains in it alone is parameter names, argument
/// order and the like. `display` only feeds EXPLAIN output, since LIR stores
/// variant names.
const INFORMATIONAL_FIELDS: &[&str] = &["body_fingerprint", "display", "sqlfunc_decl"];

fn snapshot_path() -> String {
    format!("{SNAPSHOT_DIR}/func_registry_v{LIR_VERSION}.json")
}

fn registry_json(registry: &FuncRegistry) -> String {
    let mut json = serde_json::to_string_pretty(registry).expect("registry serializes to JSON");
    // Lint requires text files to end with a newline.
    json.push('\n');
    json
}

type Records = BTreeMap<String, BTreeMap<String, serde_json::Map<String, serde_json::Value>>>;

/// Splits the differences between two registry documents into property
/// changes, which require a version bump once shipped, and informational
/// ones: added records and changes confined to [`INFORMATIONAL_FIELDS`].
fn classify_diff(expected: &str, actual: &str) -> (Vec<String>, Vec<String>) {
    let expected: Records = serde_json::from_str(expected).expect("snapshot is a registry");
    let actual: Records = serde_json::from_str(actual).expect("registry is JSON");
    let mut properties = Vec::new();
    let mut implementations = Vec::new();

    for enum_name in expected
        .keys()
        .chain(actual.keys())
        .collect::<std::collections::BTreeSet<_>>()
    {
        let empty = BTreeMap::new();
        let old = expected.get(enum_name).unwrap_or(&empty);
        let new = actual.get(enum_name).unwrap_or(&empty);
        // A stored plan cannot reference a variant that did not exist when it
        // was written, so an addition changes nothing about existing plans.
        for name in new.keys().filter(|name| !old.contains_key(*name)) {
            implementations.push(format!("  added {enum_name} `{name}`"));
        }
        for name in old.keys().filter(|name| !new.contains_key(*name)) {
            properties.push(format!("  removed {enum_name} `{name}`"));
        }
        for (name, new_record) in new {
            let Some(old_record) = old.get(name) else {
                continue;
            };
            let changed: Vec<&str> = old_record
                .keys()
                .chain(new_record.keys())
                .filter(|field| old_record.get(*field) != new_record.get(*field))
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect();
            if changed.is_empty() {
                continue;
            }
            if changed
                .iter()
                .all(|field| INFORMATIONAL_FIELDS.contains(field))
            {
                implementations.push(format!("  {enum_name} `{name}`: {changed:?}"));
            } else {
                properties.push(format!("  changed {enum_name} `{name}`: {changed:?}"));
            }
        }
    }
    (properties, implementations)
}

/// The registry must match the checked-in snapshot for [`LIR_VERSION`].
///
/// Run with `REWRITE=1` to regenerate the current version's snapshot. The
/// rewrite never touches other versions' snapshots.
#[mz_ore::test]
fn func_registry_snapshot() {
    let registry = FuncRegistry::build();
    let actual = registry_json(&registry);
    let path = snapshot_path();

    std::fs::create_dir_all(SNAPSHOT_DIR).expect("create snapshot dir");
    std::fs::write(CURRENT_PATH, &actual).expect("write current registry");

    if std::env::var_os("REWRITE").is_some() {
        std::fs::write(&path, actual).expect("write snapshot");
        return;
    }

    let expected = std::fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!(
            "missing function registry snapshot '{path}'.\n\n\
             Generate it with:\n\n    \
             REWRITE=1 cargo test -p mz-compute-types --test func_registry\n"
        )
    });
    if expected == actual {
        return;
    }

    let (properties, implementations) = classify_diff(&expected, &actual);
    let regenerate = format!(
        "Full detail: diff '{path}' against the freshly built registry at\n\
         '{CURRENT_PATH}'.\n\n\
         Then regenerate the snapshot and review the diff:\n\n    \
         REWRITE=1 cargo test -p mz-compute-types --test func_registry\n"
    );
    if !properties.is_empty() {
        let implementations = if implementations.is_empty() {
            String::new()
        } else {
            format!(
                "\nFunctions with informational changes only:\n{}\n",
                implementations.join("\n")
            )
        };
        panic!(
            "Scalar function properties changed!\n\n\
             The declared properties of the scalar functions no longer match\n\
             '{path}'. Any durably stored LIR plan using these functions may now\n\
             compute something else, or was optimized under assumptions that no\n\
             longer hold.\n\n\
             What changed:\n{}\n{implementations}\n\
             If LIR version {LIR_VERSION} has already shipped, bump LIR_VERSION in\n\
             src/compute-types/src/plan.rs so the change lands as a new version.\n\
             If version {LIR_VERSION} is unshipped, regenerating in place is fine.\n\n\
             {regenerate}",
            properties.join("\n"),
        );
    }
    panic!(
        "Scalar function registry changed without affecting stored plans!\n\n\
         These records were added, or differ from '{path}' only in fields\n\
         that do not by themselves change stored plans (a function body, its\n\
         declaration text, or its SQL display name):\n\
         {}\n\n\
         For a body_fingerprint change, review whether the new body alters the\n\
         result for any input. If it does and LIR version {LIR_VERSION} has\n\
         already shipped, bump LIR_VERSION in src/compute-types/src/plan.rs so\n\
         stored plans are replanned. Otherwise, or if version {LIR_VERSION} is\n\
         unshipped, regenerating in place is fine.\n\n\
         {regenerate}",
        implementations.join("\n"),
    );
}

/// The `#[sqlfunc]` macro must plumb its declaration and body fingerprint
/// through to the registry. A silently absent source would make the snapshot
/// blind to implementation changes without failing anything.
#[mz_ore::test]
fn func_registry_records_sqlfunc_sources() {
    let registry = FuncRegistry::build();
    // The exact rendering is pinned by the unit tests in mz-expr-derive-impl.
    let abs = &registry.unary["abs_int16"];
    let decl = abs.source.sqlfunc_decl.expect("abs_int16 is a #[sqlfunc]");
    assert!(decl.starts_with("#[sqlfunc("), "{decl}");
    assert!(decl.contains(" fn abs_int16("), "{decl}");
    assert!(abs.source.body_fingerprint.is_some());
    assert_eq!(
        abs.source.sqlfunc_signature,
        Some("fn(i16) -> Result<i16, EvalError>")
    );
    // The macro's natural input types drive the output type probe.
    assert_eq!(abs.input_types, ["Int16:NotNull"]);
    assert_eq!(abs.output_type.as_deref(), Some("Int16:NotNull"));

    // Hand-written functions have no source to record.
    let record_get = &registry.unary["record_get"];
    assert_eq!(record_get.source.sqlfunc_decl, None);
    assert_eq!(record_get.source.body_fingerprint, None);
}

#[mz_ore::test]
fn additions_are_informational_and_removals_are_not() {
    let old = r#"{"UnaryFunc": {"kept": {"could_error": false}, "gone": {"could_error": false}}}"#;
    let new = r#"{"UnaryFunc": {"kept": {"could_error": false}, "fresh": {"could_error": true}}}"#;
    let (properties, informational) = classify_diff(old, new);
    assert_eq!(properties, ["  removed UnaryFunc `gone`"]);
    assert_eq!(informational, ["  added UnaryFunc `fresh`"]);
}
