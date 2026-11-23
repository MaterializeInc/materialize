// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Snapshot of the builtins a durably stored LIR plan depends on.
//!
//! A pinned plan is replanned only when `LIR_VERSION` changes, so a change to
//! what a builtin computes, or to what the optimizer assumed about it, must
//! bump the version. The snapshot is keyed by kind of builtin, then by
//! canonical name, and splits each record into a properties half and a source
//! half.
//!
//! The functions come from [`FuncRegistry::build`], which records every
//! `UnaryFunc`, `BinaryFunc`, `VariadicFunc`, `AggregateFunc` and `TableFunc`
//! variant, see `Record` in the registry module. It lives in `mz-expr` behind
//! its `func-registry` feature, which this crate's dev-dependency enables. The
//! LIR schema snapshot in `mz-compute-types` complements it: that pins the
//! serialized shape of the function enums but not what the variants mean.
//!
//! SQL-defined builtin functions are recorded in the source half only.
//! Planning inlines them, so a change reaches only plans pinned afterwards,
//! and whether existing pins must be replanned is a judgment. Each pin lists
//! the SQL-defined builtins it may have inlined, so a migration can replan
//! just those.
//!
//! Builtin views are not recorded: pinning rejects a user item that depends on
//! any builtin relation, see `PinError::BuiltinRelation`. Builtins in unstable
//! schemas are left out too, and pinning rejects a user item that reaches one,
//! including through the body of a SQL-defined function, see
//! `PinError::UnstableDependency`.
//!
//! Three files in `tests/snapshots` hold the state, each with one compact
//! JSON object per record so a changed builtin is a one-line diff:
//!
//! * `builtin_registry.json`, the properties. A change here to a shipped LIR
//!   version alters what a stored plan computes or what the optimizer assumed
//!   when producing it, and must bump `LIR_VERSION` so pinned plans of the
//!   old version are replanned. Additions are the exception: a stored plan
//!   cannot reference a builtin that did not exist when it was written.
//!   Whether the current version has shipped is recorded in
//!   [`LIR_VERSION_POLICY`].
//! * `builtin_registry_source.json`, the sources. Changes here need judgment,
//!   a body may or may not have changed behavior, so they are reported
//!   without demanding a bump.
//! * `builtin_registry_digests.json`, a digest of the properties file per LIR
//!   version. `REWRITE=1` only ever writes the current version's entry, so
//!   after a bump the previous version's digest stays as it was, recording
//!   what that version's properties were. Old properties content is in git
//!   history, keyed by that digest. Unlike `lir_v{N}.json`, no tooling needs
//!   to read an old version's properties, so no per-version copy is kept.

use std::collections::{BTreeMap, BTreeSet};

use mz_catalog::builtin::BUILTINS;
use mz_compute_types::plan::reduce::reduction_type;
use mz_compute_types::plan::{LIR_VERSION, LIR_VERSION_POLICY};
use mz_expr::func::registry::{FuncRegistry, Record};
use mz_repr::namespaces::UNSTABLE_SCHEMAS;
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};

const SNAPSHOT_DIR: &str = "tests/snapshots";
const PROPERTIES_PATH: &str = "tests/snapshots/builtin_registry.json";
const SOURCE_PATH: &str = "tests/snapshots/builtin_registry_source.json";
const DIGESTS_PATH: &str = "tests/snapshots/builtin_registry_digests.json";

/// The freshly built halves, rewritten on every run. Gitignored. Diff them
/// against the checked-in files to see exactly what changed.
const PROPERTIES_CURRENT_PATH: &str = "tests/snapshots/builtin_registry_current.json";
const SOURCE_CURRENT_PATH: &str = "tests/snapshots/builtin_registry_source_current.json";

const REWRITE_COMMAND: &str = "REWRITE=1 cargo test -p mz-catalog --test builtin_registry";

/// Kind of builtin, then canonical name, then the record as a JSON object.
type Records = BTreeMap<String, BTreeMap<String, serde_json::Value>>;

/// Builds the properties and source halves of every builtin the snapshot
/// tracks.
fn build() -> (Records, Records) {
    fn split_into<F, P: Serialize>(
        properties: &mut Records,
        sources: &mut Records,
        kind: &str,
        map: &BTreeMap<String, Record<F, P>>,
    ) {
        for (name, record) in map {
            properties
                .entry(kind.to_string())
                .or_default()
                .insert(name.clone(), to_value(&record.properties));
            sources
                .entry(kind.to_string())
                .or_default()
                .insert(name.clone(), to_value(&record.source));
        }
    }
    let registry = FuncRegistry::build();
    let (mut properties, mut sources) = (Records::new(), Records::new());
    split_into(&mut properties, &mut sources, "UnaryFunc", &registry.unary);
    split_into(
        &mut properties,
        &mut sources,
        "BinaryFunc",
        &registry.binary,
    );
    split_into(
        &mut properties,
        &mut sources,
        "VariadicFunc",
        &registry.variadic,
    );
    split_into(
        &mut properties,
        &mut sources,
        "AggregateFunc",
        &registry.aggregate,
    );
    // Lowering stores each aggregate's reduction type in the plan, and
    // rendering dispatches on the function within that type, panicking on a
    // function it does not expect there (see `get_monoid` in `mz-compute`'s
    // `render/reduce.rs`). `mz-expr` cannot see the classification, so it is
    // recorded here.
    let aggregates = properties
        .get_mut("AggregateFunc")
        .expect("aggregates recorded");
    for (name, record) in &registry.aggregate {
        let serde_json::Value::Object(fields) = aggregates.get_mut(name).expect("recorded above")
        else {
            panic!("aggregate properties serialize as an object");
        };
        fields.insert(
            "reduction_type".to_string(),
            to_value(&reduction_type(&record.func)),
        );
    }
    split_into(&mut properties, &mut sources, "TableFunc", &registry.table);

    let sql_functions = sources.entry("SqlFunction".to_string()).or_default();
    for func in BUILTINS::funcs().filter(|func| !UNSTABLE_SCHEMAS.contains(&func.schema)) {
        for imp in func.inner.func_impls() {
            let Some(sql) = imp.sql else { continue };
            let mut params = imp.arg_typs.clone();
            if let Some(variadic) = imp.variadic_typ {
                params.push(variadic);
            }
            let key = format!("{}.{}({})", func.schema, func.name, params.join(", "));
            let previous = sql_functions.insert(
                key.clone(),
                json!({
                    "oid": imp.oid,
                    "variadic": imp.variadic_typ.is_some(),
                    "return_type": imp.return_typ,
                    "returns_set": imp.return_is_set,
                    "sql": sql,
                }),
            );
            assert!(previous.is_none(), "two overloads render as `{key}`");
        }
    }
    (properties, sources)
}

fn to_value<T: Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).expect("registry records serialize to JSON")
}

/// Renders records as a JSON object of objects with one compact line per
/// record. Valid JSON, and a changed record is a one-line diff.
fn render(records: &Records) -> String {
    let mut out = String::from("{\n");
    let mut kinds = records.iter().peekable();
    while let Some((kind, entries)) = kinds.next() {
        out.push_str(&format!("  {}: {{\n", serde_json::to_string(kind).unwrap()));
        let mut entries = entries.iter().peekable();
        while let Some((name, record)) = entries.next() {
            out.push_str(&format!(
                "    {}: {}{}\n",
                serde_json::to_string(name).unwrap(),
                serde_json::to_string(record).unwrap(),
                if entries.peek().is_some() { "," } else { "" }
            ));
        }
        out.push_str(&format!(
            "  }}{}\n",
            if kinds.peek().is_some() { "," } else { "" }
        ));
    }
    out.push_str("}\n");
    out
}

fn digest(contents: &str) -> String {
    format!(
        "sha256:{}",
        hex::encode(Sha256::digest(contents.as_bytes()))
    )
}

/// Per-record differences between two rendered record files.
struct Diff {
    added: Vec<String>,
    removed: Vec<String>,
    /// Record key, then the changed field names.
    changed: Vec<(String, Vec<String>)>,
}

fn diff(expected: &str, actual: &str) -> Diff {
    let expected: Records = serde_json::from_str(expected).expect("snapshot is a record file");
    let actual: Records = serde_json::from_str(actual).expect("rendered records are JSON");
    let mut diff = Diff {
        added: Vec::new(),
        removed: Vec::new(),
        changed: Vec::new(),
    };
    let kinds: BTreeSet<_> = expected.keys().chain(actual.keys()).collect();
    for kind in kinds {
        let empty = BTreeMap::new();
        let old = expected.get(kind).unwrap_or(&empty);
        let new = actual.get(kind).unwrap_or(&empty);
        for name in new.keys().filter(|name| !old.contains_key(*name)) {
            diff.added.push(format!("{kind} `{name}`"));
        }
        for name in old.keys().filter(|name| !new.contains_key(*name)) {
            diff.removed.push(format!("{kind} `{name}`"));
        }
        for (name, new_record) in new {
            let Some(old_record) = old.get(name) else {
                continue;
            };
            if old_record == new_record {
                continue;
            }
            let (Some(old_record), Some(new_record)) =
                (old_record.as_object(), new_record.as_object())
            else {
                diff.changed.push((format!("{kind} `{name}`"), vec![]));
                continue;
            };
            let fields: BTreeSet<_> = old_record
                .keys()
                .chain(new_record.keys())
                .filter(|field| old_record.get(*field) != new_record.get(*field))
                .cloned()
                .collect();
            diff.changed
                .push((format!("{kind} `{name}`"), fields.into_iter().collect()));
        }
    }
    diff
}

fn describe(diff: &Diff) -> String {
    let mut lines = Vec::new();
    lines.extend(diff.removed.iter().map(|key| format!("  removed {key}")));
    lines.extend(
        diff.changed
            .iter()
            .map(|(key, fields)| format!("  changed {key}: {fields:?}")),
    );
    lines.extend(diff.added.iter().map(|key| format!("  added {key}")));
    lines.join("\n")
}

/// The lines added to and removed from the `sql` field of each record present
/// in both files. Lines are compared as sets, so a change that only reorders
/// or duplicates lines names the record with no lines under it.
fn sql_changes(expected: &str, actual: &str) -> String {
    let expected: Records = serde_json::from_str(expected).expect("snapshot is a record file");
    let actual: Records = serde_json::from_str(actual).expect("rendered records are JSON");
    let mut out = Vec::new();
    for (kind, new) in &actual {
        let Some(old) = expected.get(kind) else {
            continue;
        };
        for (name, new_record) in new {
            let (Some(old_sql), Some(new_sql)) = (
                old.get(name)
                    .and_then(|r| r.get("sql"))
                    .and_then(|s| s.as_str()),
                new_record.get("sql").and_then(|s| s.as_str()),
            ) else {
                continue;
            };
            if old_sql == new_sql {
                continue;
            }
            let old_lines: BTreeSet<_> = old_sql.lines().collect();
            let new_lines: BTreeSet<_> = new_sql.lines().collect();
            out.push(format!("  {kind} `{name}`:"));
            out.extend(
                old_sql
                    .lines()
                    .filter(|line| !new_lines.contains(line))
                    .map(|line| format!("    - {line}")),
            );
            out.extend(
                new_sql
                    .lines()
                    .filter(|line| !old_lines.contains(line))
                    .map(|line| format!("    + {line}")),
            );
        }
    }
    out.join("\n")
}

fn read(path: &str) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|_| {
        panic!(
            "missing registry snapshot '{path}'.\n\nGenerate it with:\n\n    {REWRITE_COMMAND}\n"
        )
    })
}

/// The registry must match the checked-in snapshots, and the properties
/// digest recorded for [`LIR_VERSION`] must match the properties file.
///
/// Run with `REWRITE=1` to regenerate both halves and the current version's
/// digest. The rewrite never touches other versions' digests.
#[mz_ore::test]
fn builtin_registry_snapshot() {
    let (properties, source) = build();
    let (properties, source) = (render(&properties), render(&source));

    std::fs::create_dir_all(SNAPSHOT_DIR).expect("create snapshot dir");
    std::fs::write(PROPERTIES_CURRENT_PATH, &properties).expect("write current properties");
    std::fs::write(SOURCE_CURRENT_PATH, &source).expect("write current source");

    let version = LIR_VERSION.to_string();
    if std::env::var_os("REWRITE").is_some() {
        let mut digests: BTreeMap<String, String> = std::fs::read_to_string(DIGESTS_PATH)
            .ok()
            .map(|json| serde_json::from_str(&json).expect("digest table is JSON"))
            .unwrap_or_default();
        digests.insert(version, digest(&properties));
        let mut digests = serde_json::to_string_pretty(&digests).expect("digests serialize");
        digests.push('\n');
        std::fs::write(PROPERTIES_PATH, properties).expect("write properties");
        std::fs::write(SOURCE_PATH, source).expect("write source");
        std::fs::write(DIGESTS_PATH, digests).expect("write digests");
        return;
    }

    let expected_properties = read(PROPERTIES_PATH);
    let expected_source = read(SOURCE_PATH);
    let digests: BTreeMap<String, String> =
        serde_json::from_str(&read(DIGESTS_PATH)).expect("digest table is JSON");
    let regenerate = format!(
        "Full detail: diff '{PROPERTIES_PATH}' against '{PROPERTIES_CURRENT_PATH}' and\n\
         '{SOURCE_PATH}' against '{SOURCE_CURRENT_PATH}'.\n\n\
         Then regenerate the snapshots and review the diff:\n\n    {REWRITE_COMMAND}\n"
    );

    let properties_diff = diff(&expected_properties, &properties);
    if !properties_diff.removed.is_empty() || !properties_diff.changed.is_empty() {
        panic!(
            "Builtin properties changed!\n\n\
             The declared properties of the builtins no longer match\n\
             '{PROPERTIES_PATH}'. Any durably stored LIR plan using these builtins may\n\
             now compute something else, or was optimized under assumptions that no\n\
             longer hold.\n\n\
             What changed:\n{}\n\n\
             NOTE: if you only edited a Sample in src/expr/src/scalar/func/registry.rs,\n\
             this is not a function change. The input_types and output_type fields\n\
             record what the sample probed, and a removed `name[label]` record is a\n\
             deleted labeled sample. In that case regenerate without bumping.\n\n\
             {LIR_VERSION_POLICY}\n\n\
             {regenerate}",
            describe(&properties_diff),
        );
    }

    let mut informational = Vec::new();
    if !properties_diff.added.is_empty() {
        informational.push(format!("Added builtins:\n{}", describe(&properties_diff)));
    }
    if expected_source != source {
        informational.push(format!(
            "Builtins whose source changed (for a function: a body, its declaration\n\
             text, or its SQL display name), without a change to their properties:\n{}",
            describe(&diff(&expected_source, &source)),
        ));
        let sql = sql_changes(&expected_source, &source);
        if !sql.is_empty() {
            informational.push(format!(
                "Changed SQL of SQL-defined functions:\n{sql}\n\n\
                 Plans pinned before this change keep the old definition. If they must\n\
                 pick up the new one, bump LIR_VERSION and migrate the pins whose\n\
                 inlined_builtins list the builtin."
            ));
        }
    }
    if !informational.is_empty() {
        panic!(
            "Builtin registry changed without affecting stored plans!\n\n\
             {}\n\n\
             For a body_fingerprint change, review whether the new body alters the\n\
             result for any input. If it does, treat it as a change to the stable\n\
             format.\n\n\
             {LIR_VERSION_POLICY}\n\n\
             {regenerate}",
            informational.join("\n\n"),
        );
    }

    // Both files match, so the table must agree with the properties file.
    // A stale entry means someone edited the file or the table by hand.
    assert_eq!(
        digests.get(&version),
        Some(&digest(&expected_properties)),
        "'{DIGESTS_PATH}' has no matching digest for LIR version {LIR_VERSION}. \
         Regenerate it:\n\n    {REWRITE_COMMAND}\n"
    );
}

/// The `#[sqlfunc]` macro must plumb its source through to the registry. A
/// silently absent source would make the snapshot blind to implementation
/// changes without failing anything.
#[mz_ore::test]
fn builtin_registry_records_sqlfunc_sources() {
    let registry = FuncRegistry::build();
    // The exact rendering is pinned by the unit tests in mz-expr-derive-impl.
    let abs = &registry.unary["abs_int16"];
    let decl = abs.source.sqlfunc_decl.expect("abs_int16 is a #[sqlfunc]");
    assert!(decl.starts_with("#[sqlfunc("), "{decl}");
    assert!(decl.contains(" fn abs_int16("), "{decl}");
    assert!(abs.source.body_fingerprint.is_some());
    assert_eq!(
        abs.properties.sqlfunc_signature,
        Some("fn(i16) -> Result<i16, EvalError>")
    );
    // The macro's natural input types drive the output type probe.
    assert_eq!(abs.properties.input_types, ["Int16:NotNull"]);
    assert_eq!(abs.properties.output_type.as_deref(), Some("Int16:NotNull"));

    // Hand-written functions have no source to record. The compound-type casts are
    // generic over an `Eval` implementor, a shape the macro does not cover, so one of
    // them is a stable example of the absence.
    let cast_array_to_array = &registry.unary["cast_array_to_array"];
    assert_eq!(cast_array_to_array.source.sqlfunc_decl, None);
    assert_eq!(cast_array_to_array.source.body_fingerprint, None);
}

#[mz_ore::test]
fn diff_reports_each_kind_of_change() {
    let old = r#"{"UnaryFunc": {"kept": {"a": 1, "b": 1}, "gone": {"a": 1}}}"#;
    let new = r#"{"UnaryFunc": {"kept": {"a": 1, "b": 2}, "fresh": {"a": 1}}}"#;
    let diff = diff(old, new);
    assert_eq!(diff.added, ["UnaryFunc `fresh`"]);
    assert_eq!(diff.removed, ["UnaryFunc `gone`"]);
    assert_eq!(
        diff.changed,
        [("UnaryFunc `kept`".to_string(), vec!["b".to_string()])]
    );
}

#[mz_ore::test]
fn render_is_one_line_per_record_and_valid_json() {
    let mut records = Records::new();
    records.entry("UnaryFunc".into()).or_default().insert(
        "f".into(),
        serde_json::json!({"a": [1, 2], "b": {"c": null}}),
    );
    records
        .entry("BinaryFunc".into())
        .or_default()
        .insert("g".into(), serde_json::json!({"a": 1}));
    let rendered = render(&records);
    assert_eq!(
        rendered,
        "{\n  \"BinaryFunc\": {\n    \"g\": {\"a\":1}\n  },\n  \"UnaryFunc\": {\n    \"f\": {\"a\":[1,2],\"b\":{\"c\":null}}\n  }\n}\n"
    );
    let reparsed: Records = serde_json::from_str(&rendered).expect("valid JSON");
    assert_eq!(reparsed, records);
}

#[mz_ore::test]
fn sql_changes_names_changed_lines() {
    let render_func = |sql: &str| {
        render(&Records::from([(
            "SqlFunction".to_string(),
            BTreeMap::from([("pg_catalog.f(int4)".to_string(), json!({ "sql": sql }))]),
        )]))
    };
    let old = render_func("SELECT\n$1 + 1,\n$1");
    let new = render_func("SELECT\n$1 + 2,\n$1");
    assert_eq!(
        sql_changes(&old, &new),
        "  SqlFunction `pg_catalog.f(int4)`:\n    - $1 + 1,\n    + $1 + 2,"
    );
    assert_eq!(sql_changes(&old, &old), "");
}
