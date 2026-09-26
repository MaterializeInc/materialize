// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Snapshot of the scalar function property registry.
//!
//! [`FuncRegistry::build`] records every `UnaryFunc`, `BinaryFunc` and
//! `VariadicFunc` variant as a properties half and a source half, see
//! `Record` in the registry module. It complements the LIR schema snapshot in
//! `lir_schema.rs`, which pins the serialized shape of these enums but not
//! what the variants mean. The registry lives in `mz-expr` behind its
//! `func-registry` feature, which this crate's dev-dependency enables.
//!
//! Three files in `tests/snapshots` hold the state, each with one compact
//! JSON object per record so a changed function is a one-line diff:
//!
//! * `func_registry.json`, the properties. A change here to a shipped LIR
//!   version alters what a stored plan computes or what the optimizer assumed
//!   when producing it, and must bump `LIR_VERSION` so pinned plans of the
//!   old version are replanned. Additions are the exception: a stored plan
//!   cannot reference a variant that did not exist when it was written.
//!   Whether the current version has shipped is recorded in
//!   [`LIR_VERSION_POLICY`].
//! * `func_registry_source.json`, the sources. Changes here need judgment, a
//!   body may or may not have changed behavior, so they are reported without
//!   demanding a bump.
//! * `func_registry_digests.json`, a digest of the properties file per LIR
//!   version. `REWRITE=1` only ever writes the current version's entry, so
//!   after a bump the previous version's digest stays as it was, recording
//!   what that version's properties were. Old properties content is in git
//!   history, keyed by that digest. Unlike `lir_v{N}.json`, no tooling needs
//!   to read an old version's properties, so no per-version copy is kept.

use std::collections::{BTreeMap, BTreeSet};

use mz_compute_types::plan::{LIR_VERSION, LIR_VERSION_POLICY};
use mz_expr::func::registry::{FuncRegistry, Record};
use serde::Serialize;
use sha2::{Digest, Sha256};

const SNAPSHOT_DIR: &str = "tests/snapshots";
const PROPERTIES_PATH: &str = "tests/snapshots/func_registry.json";
const SOURCE_PATH: &str = "tests/snapshots/func_registry_source.json";
const DIGESTS_PATH: &str = "tests/snapshots/func_registry_digests.json";

/// The freshly built halves, rewritten on every run. Gitignored. Diff them
/// against the checked-in files to see exactly what changed.
const PROPERTIES_CURRENT_PATH: &str = "tests/snapshots/func_registry_current.json";
const SOURCE_CURRENT_PATH: &str = "tests/snapshots/func_registry_source_current.json";

const REWRITE_COMMAND: &str = "REWRITE=1 cargo test -p mz-compute-types --test func_registry";

/// Enum name, then canonical variant name, then the record as a JSON object.
type Records = BTreeMap<String, BTreeMap<String, serde_json::Value>>;

/// Splits the registry into its properties and source halves, each keyed by
/// enum name and canonical variant name.
fn split(registry: &FuncRegistry) -> (Records, Records) {
    fn split_into<P: Serialize>(
        properties: &mut Records,
        sources: &mut Records,
        enum_name: &str,
        map: &BTreeMap<String, Record<P>>,
    ) {
        for (name, record) in map {
            properties
                .entry(enum_name.to_string())
                .or_default()
                .insert(name.clone(), to_value(&record.properties));
            sources
                .entry(enum_name.to_string())
                .or_default()
                .insert(name.clone(), to_value(&record.source));
        }
    }
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
    (properties, sources)
}

fn to_value<T: Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).expect("registry records serialize to JSON")
}

/// Renders records as a JSON object of objects with one compact line per
/// record. Valid JSON, and a changed record is a one-line diff.
fn render(records: &Records) -> String {
    let mut out = String::from("{\n");
    let mut enums = records.iter().peekable();
    while let Some((enum_name, entries)) = enums.next() {
        out.push_str(&format!(
            "  {}: {{\n",
            serde_json::to_string(enum_name).unwrap()
        ));
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
            if enums.peek().is_some() { "," } else { "" }
        ));
    }
    out.push_str("}\n");
    out
}

fn digest(contents: &str) -> String {
    format!("sha256:{:x}", Sha256::digest(contents.as_bytes()))
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
    let enum_names: BTreeSet<_> = expected.keys().chain(actual.keys()).collect();
    for enum_name in enum_names {
        let empty = BTreeMap::new();
        let old = expected.get(enum_name).unwrap_or(&empty);
        let new = actual.get(enum_name).unwrap_or(&empty);
        for name in new.keys().filter(|name| !old.contains_key(*name)) {
            diff.added.push(format!("{enum_name} `{name}`"));
        }
        for name in old.keys().filter(|name| !new.contains_key(*name)) {
            diff.removed.push(format!("{enum_name} `{name}`"));
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
                diff.changed.push((format!("{enum_name} `{name}`"), vec![]));
                continue;
            };
            let fields: BTreeSet<_> = old_record
                .keys()
                .chain(new_record.keys())
                .filter(|field| old_record.get(*field) != new_record.get(*field))
                .cloned()
                .collect();
            diff.changed.push((
                format!("{enum_name} `{name}`"),
                fields.into_iter().collect(),
            ));
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
fn func_registry_snapshot() {
    let registry = FuncRegistry::build();
    let (properties, source) = split(&registry);
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
            "Scalar function properties changed!\n\n\
             The declared properties of the scalar functions no longer match\n\
             '{PROPERTIES_PATH}'. Any durably stored LIR plan using these functions may\n\
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
        informational.push(format!("Added functions:\n{}", describe(&properties_diff)));
    }
    if expected_source != source {
        informational.push(format!(
            "Functions whose source changed (a body, its declaration text, or its\n\
             SQL display name), without a change to their properties:\n{}",
            describe(&diff(&expected_source, &source)),
        ));
    }
    if !informational.is_empty() {
        panic!(
            "Scalar function registry changed without affecting stored plans!\n\n\
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
fn func_registry_records_sqlfunc_sources() {
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

    // Hand-written functions have no source to record.
    let extract_interval = &registry.unary["extract_interval"];
    assert_eq!(extract_interval.source.sqlfunc_decl, None);
    assert_eq!(extract_interval.source.body_fingerprint, None);
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
