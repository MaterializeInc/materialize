// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Schema registry for the stable LIR serialization format.
//!
//! These tests trace the serde serialization surface reachable from
//! [`LirRelationExpr`] into a [`serde_reflection::Registry`] and compare it
//! against a checked-in snapshot, `tests/snapshots/lir_v{LIR_VERSION}.json`.
//! Any change to the serialized format of LIR (including the `*Func` enums
//! and everything else in the graph) changes the traced schema and fails
//! `lir_schema_snapshot` until the snapshot is regenerated with `REWRITE=1`.
//! The snapshot diff is the reviewable record of the format change.
//!
//! Rows and eval errors are the two places where the schema's guarantee is
//! delegated rather than direct. `Row`'s serde impl emits the raw bytes of
//! the in-memory datum encoding, which is documented as free to change, and
//! `EvalError`'s serde impl mirrors a Rust enum that grows a variant
//! whenever a new error is added. Both are forbidden containers here. The
//! stable surface stores them as `StableRow` and `StableEvalError`, which
//! serialize as protobuf-encoded `ProtoRow` and `ProtoEvalError` bytes. The
//! registry sees those as opaque BYTES, and the contract behind them is
//! CI-guarded elsewhere: the proto files are covered by the buf breaking
//! lint, and persist requires both protos to stay backward compatible
//! because they form the storage codec for `SourceData` (`ProtoRow` on the
//! Ok side, `ProtoEvalError` inside `ProtoDataflowError` on the Err side).
//!
//! While a given LIR version is unshipped, regenerating its snapshot in place
//! is fine. Once pinned plans are durably stored, a schema change must instead
//! bump [`LIR_VERSION`], which targets a fresh snapshot file and leaves the
//! old schema in place for migration tooling.
//!
//! There are some subtleties to tracing. [`serde_reflection::Registry`] reuses
//! existing serde machinery, which means that we have to work carefully to ensure
//! our registry adequately explores all enums and has good default values.
//! Notes in error messages and implementations should help you diagnose any issues
//! that may arise as types change.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use mz_compute_types::plan::join::JoinPlan;
use mz_compute_types::plan::reduce::{BasicPlan, HierarchicalPlan, ReducePlan};
use mz_compute_types::plan::scalar::{LirScalarExpr, LiteralValue};
use mz_compute_types::plan::threshold::ThresholdPlan;
use mz_compute_types::plan::top_k::TopKPlan;
use mz_compute_types::plan::{
    ArrangementStrategy, ConstantRows, GetPlan, LIR_VERSION, LirRelationExpr, LirRelationNode,
};
use mz_expr::func::{TimezoneTime, ToCharTimestamp};
use mz_expr::like_pattern::Matcher;
use mz_expr::{
    AggregateFunc, BinaryFunc, EvalError, Id, LagLeadType, LetRecLimit, StableEvalError, TableFunc,
    UnaryFunc, VariadicFunc, WindowFrameBound, WindowFrameUnits,
};
use mz_pgtz::timezone::Timezone;
use mz_repr::adt::datetime::DateTimeUnits;
use mz_repr::{CatalogItemId, GlobalId, ReprScalarType, SqlScalarType};
use serde_reflection::{
    ContainerFormat, Format, FormatHolder, Registry, Samples, Tracer, TracerConfig,
};

const SNAPSHOT_DIR: &str = "tests/snapshots";

/// The freshly traced schema, rewritten on every run, complete or not.
/// Gitignored. Diff against the checked-in snapshot to see exactly what
/// changed, or grep it for what references an unexpected type, without
/// rerunning anything.
const CURRENT_PATH: &str = "tests/snapshots/lir_current.json";

fn snapshot_path() -> String {
    format!("{SNAPSHOT_DIR}/lir_v{LIR_VERSION}.json")
}

/// Rewrites ENUM variant maps and STRUCT field lists into name-keyed,
/// name-sorted maps.
///
/// The stable LIR format is self-describing JSON: enum values carry their
/// variant name, never their index, and struct fields deserialize by name in
/// any order. So variant indices, variant order, and field order have no
/// wire significance, and keying both by name keeps them out of the
/// snapshot. Adding a variant or field is a one-entry diff and reordering a
/// declaration is invisible, matching what actually can and cannot break
/// stored plans. Tuples stay positional, JSON arrays really are ordered.
///
/// NOTE: this bakes in the JSON assumption. Under a positional format such
/// as bincode or MessagePack, indices and declaration order are the wire
/// format, and this rewrite would hide breaking reorders. Changing the
/// storage format means removing this and bumping LIR_VERSION.
fn canonicalize_for_json(value: &mut serde_json::Value) {
    // Collect through BTreeMaps to sort by name. The workspace enables
    // serde_json's preserve_order feature, so Map keeps insertion order.
    fn named_entries<'a>(
        entries: impl IntoIterator<Item = &'a serde_json::Value>,
        what: &str,
    ) -> BTreeMap<String, serde_json::Value> {
        entries
            .into_iter()
            .flat_map(|entry| {
                entry
                    .as_object()
                    .unwrap_or_else(|| panic!("{what} are single-entry name-to-format objects"))
                    .iter()
                    .map(|(name, format)| (name.clone(), format.clone()))
            })
            .collect()
    }

    match value {
        serde_json::Value::Object(map) => {
            for (key, inner) in map.iter_mut() {
                match (key.as_str(), &*inner) {
                    ("ENUM", serde_json::Value::Object(variants)) => {
                        let by_name = named_entries(variants.values(), "ENUM variants");
                        *inner = serde_json::Value::Object(by_name.into_iter().collect());
                    }
                    ("STRUCT", serde_json::Value::Array(fields)) => {
                        let by_name = named_entries(fields.iter(), "STRUCT fields");
                        *inner = serde_json::Value::Object(by_name.into_iter().collect());
                    }
                    _ => {}
                }
                canonicalize_for_json(inner);
            }
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(canonicalize_for_json),
        _ => {}
    }
}

/// Serializes a registry exactly as the checked-in snapshot stores it.
fn registry_json(registry: &Registry) -> String {
    let mut value = serde_json::to_value(registry).expect("registry serializes to JSON");
    canonicalize_for_json(&mut value);
    let mut json = serde_json::to_string_pretty(&value).expect("value serializes to JSON");
    // Lint requires text files to end with a newline.
    json.push('\n');
    json
}

/// Like [`registry_json`], but tolerates a partially traced registry.
///
/// A container still mid-trace holds unresolved variables whose serde impl
/// errors, so serialize per container and fall back to the debug
/// representation, as a JSON string, for those. The output is always valid
/// JSON.
fn partial_registry_json(registry: &Registry) -> String {
    let map: BTreeMap<&String, serde_json::Value> = registry
        .iter()
        .map(|(name, container)| {
            let mut value = serde_json::to_value(container).unwrap_or_else(|_| {
                serde_json::Value::String(format!("<mid-trace: {container:?}>"))
            });
            canonicalize_for_json(&mut value);
            (name, value)
        })
        .collect();
    let mut json = serde_json::to_string_pretty(&map).expect("values are pre-validated JSON");
    json.push('\n');
    json
}

/// Writes the traced schema to [`CURRENT_PATH`].
///
/// The three tests trace concurrently and write identical bytes, so each
/// write goes to a writer-unique temp file first and then renames into place
/// to avoid interleaving. The temp name needs both the process id and a
/// counter: nextest runs the tests in separate processes, but cargo test
/// runs them as threads of one process.
fn write_current(contents: &str) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static WRITER: AtomicUsize = AtomicUsize::new(0);
    let tmp = format!(
        "{CURRENT_PATH}.tmp.{}.{}",
        std::process::id(),
        WRITER.fetch_add(1, Ordering::Relaxed)
    );
    std::fs::create_dir_all(SNAPSHOT_DIR).expect("create snapshot dir");
    std::fs::write(&tmp, contents).expect("write dump");
    std::fs::rename(&tmp, CURRENT_PATH).expect("move dump into place");
}

/// Panics with the tracing error and the remedy for its failure class.
///
/// serde_reflection's errors say what went wrong but not what to do about it
/// in this test, so we translate each class into its known fix here.
fn diagnose(context: &str, err: serde_reflection::Error) -> ! {
    use serde_reflection::Error;
    let remedy = match &err {
        Error::Custom(_) => {
            "A Deserialize impl rejected the value the tracer synthesized (for \
             example chrono types report 'premature end of input' when parsing \
             the synthesized \"\", chrono_tz reports 'not a valid timezone', \
             NonZero integers reject the synthesized 0, and StableEvalError \
             rejects the synthesized empty bytes because they decode to a \
             ProtoEvalError with no kind). Record a valid sample of the \
             smallest containing struct with tracer.trace_value in the \
             samples section of run_traces. Keep samples minimal. A recorded \
             sample is replayed wherever the type appears."
        }
        Error::Incompatible(_, _) => {
            "Two types registered different formats under the same serde \
             container name. Either two distinct types share a name (fix with \
             #[serde(rename)], as for CharMaxLength in mz-repr and \
             RegexpMatchStatic in mz-expr), or a named generic such as \
             Result is instantiated at two payload types (fix by serializing \
             the field through a named mirror enum with #[serde(with)], as for \
             ConstantRows in plan.rs and LiteralValue in plan/scalar.rs)."
        }
        Error::MissingVariants(_) => {
            "An enum reached only through struct fields is explored one \
             variant per pass and never completes. If the listed enum is \
             public, add it to the trace_enums! list in run_traces, at its \
             LIR instantiation if it is generic (as for \
             UnaryFunc<LirScalarExpr>). If it is a generic instantiated at \
             more than one payload type, such as Result, no single trace can \
             represent it: serialize the field through a named mirror enum \
             with #[serde(with)] instead, as for ConstantRows in plan.rs. If \
             it is private and cannot be imported, add its name to the \
             check_incomplete_enum loop at the end of run_traces, and trace a \
             public type that reaches it inside that loop. Beware same-named \
             public types in other modules (mz_repr::adt::datetime and \
             mz_sql_parser::ast both have a DateTimeField): the registry is \
             keyed by container name, so tracing the wrong type registers a \
             wrong format under the right name."
        }
        Error::UnknownFormat | Error::UnknownFormatInContainer(_) => {
            "Part of the traced structure stayed unknown. Usually a recorded \
             sample did not exercise a container's full structure (an empty \
             Vec or a None leaves its element type unknown). Enrich the sample \
             so every field carries a representative value, or drop the sample \
             if the type will deserialize synthesized defaults correctly."
        }
        Error::DeserializationError(_) | Error::UnexpectedDeserializationFormat(_, _, _) => {
            "A recorded sample was replayed against a format it no longer \
             matches. Check the samples section of run_traces for a sample \
             whose type changed shape, and regenerate or remove it."
        }
        Error::NotSupported(_) => {
            "serde_reflection cannot trace this construct (for example \
             #[serde(flatten)], deserialize_any, or untagged enums). The \
             error does not name the offending type: look for the last \
             mid-trace container in the partial dump. Change the type's serde \
             representation to something traceable."
        }
    };
    panic!(
        "failed to trace {context}: {err}\n\n{remedy}\n\n\
         The partially traced registry is at '{CURRENT_PATH}'.\n"
    );
}

/// Traces the full serde type graph reachable from [`LirRelationExpr`].
///
/// Whatever happens, the traced state lands at [`CURRENT_PATH`]: the complete
/// schema on success, the partial registry on failure. Failures panic with
/// the remedy for their error class, see [`diagnose`].
fn trace_lir_registry() -> Registry {
    let mut tracer = new_tracer();
    let mut samples = Samples::new();

    if let Err((context, err)) = run_traces(&mut tracer, &mut samples) {
        write_current(&partial_registry_json(&tracer.registry_unchecked()));
        diagnose(&context, err);
    }
    match tracer.registry() {
        Ok(registry) => {
            write_current(&registry_json(&registry));
            registry
        }
        Err(err) => {
            // The failed registry() call consumed the tracer, so retrace to
            // recover the partial registry for the dump. This path is about
            // to panic, the extra half second does not matter.
            let mut tracer = new_tracer();
            let mut samples = Samples::new();
            let _ = run_traces(&mut tracer, &mut samples);
            write_current(&partial_registry_json(&tracer.registry_unchecked()));
            diagnose("the completed registry", err);
        }
    }
}

fn new_tracer() -> Tracer {
    // The synthesized string is "UTC" instead of the default "" because
    // chrono_tz::Tz (inside Timezone) parses the string it deserializes and
    // rejects "". Sample replay cannot help enums like Timezone, because
    // completing an enum requires synthesized passes to assign variant
    // indices. "UTC" is a valid regex and LIKE pattern too, so the other
    // string-parsing types in the graph accept it.
    let config = TracerConfig::default()
        .record_samples_for_structs(true)
        .record_samples_for_tuple_structs(true)
        .default_borrowed_str_value("UTC")
        .default_string_value("UTC".to_string());
    Tracer::new(config)
}

/// Runs every trace step, stopping at the first failure.
///
/// serde_reflection drives `Deserialize` impls with synthesized default
/// values (0, "", etc.). Types whose `Deserialize` checks invariants reject
/// those defaults, so we record a valid sample for each such type up front
/// and configure the tracer to replay recorded samples.
fn run_traces(
    tracer: &mut Tracer,
    samples: &mut Samples,
) -> Result<(), (String, serde_reflection::Error)> {
    // NOTE: keep the recorded samples to a minimum. A recorded sample is
    // replayed wherever the container appears, which prevents synthesis from
    // exploring the container's full structure (and mismatched replays fail
    // with "premature end of input"). Types like Regex that recompile a
    // pattern on deserialization need no sample, because the synthesized
    // default "" is a valid pattern.

    // LetRecLimit's max_iters is a NonZeroU64, which rejects the synthesized 0.
    let limit = LetRecLimit {
        max_iters: NonZeroU64::new(1).expect("nonzero"),
        return_at_limit: false,
    };
    tracer
        .trace_value(samples, &limit)
        .map_err(|err| ("LetRecLimit sample".to_string(), err))?;

    // TimezoneTime stores a chrono NaiveDateTime, which deserializes by
    // parsing a string and rejects the synthesized default "". The sample
    // also covers Timezone's Tz variant, whose chrono_tz::Tz parses a string
    // as well. Timezone's FixedOffset variant deserializes from an i32 and
    // needs no sample.
    let timezone_time = TimezoneTime {
        tz: Timezone::Tz(chrono_tz::Tz::UTC),
        wall_time: chrono::NaiveDateTime::default(),
    };
    tracer
        .trace_value(samples, &timezone_time)
        .map_err(|err| ("TimezoneTime sample".to_string(), err))?;

    // StableEvalError deserializes by decoding protobuf bytes, and the
    // synthesized default "" decodes to a ProtoEvalError with no kind set,
    // which from_proto rejects. (StableRow needs no sample: an empty
    // ProtoRow is a valid empty row.)
    tracer
        .trace_value(samples, &StableEvalError(EvalError::DivisionByZero))
        .map_err(|err| ("StableEvalError sample".to_string(), err))?;

    tracer
        .trace_type::<LirRelationExpr>(samples)
        .map_err(|err| ("LirRelationExpr".to_string(), err))?;

    // Tracing a struct explores nested enums one variant per pass, so every
    // enum in the graph needs its own trace_type call to cover all variants.
    // The registry() call below fails with "missing variants" naming any enum
    // that still needs to be added here.
    macro_rules! trace_enums {
        ($($ty:ty),* $(,)?) => {
            $(
                tracer
                    .trace_type::<$ty>(samples)
                    .map_err(|err| (stringify!($ty).to_string(), err))?;
            )*
        };
    }
    trace_enums![
        LirRelationNode,
        LirScalarExpr,
        GetPlan,
        Id,
        JoinPlan,
        ReducePlan,
        TopKPlan,
        ThresholdPlan,
        // The LIR instantiation. The registry is keyed by container name, so
        // the schema records the cast funcs' payloads as LirScalarExpr. The
        // MIR instantiation must never be traced here, its cast payloads
        // would register incompatible formats under the same names.
        UnaryFunc<LirScalarExpr>,
        BinaryFunc,
        VariadicFunc,
        TableFunc,
        AggregateFunc,
        ReprScalarType,
        ConstantRows,
        LiteralValue,
        ArrangementStrategy,
        BasicPlan,
        HierarchicalPlan,
        GlobalId,
        SqlScalarType,
        DateTimeUnits,
        LagLeadType,
        WindowFrameBound,
        WindowFrameUnits,
        Timezone,
        CatalogItemId,
    ];

    // Some enums in the graph are private (MatcherImpl in like_pattern, and
    // DateTimeField, DateTimeFormatNode, and OrdinalMode in the to_char
    // formatter), so they cannot be listed above. Tracing their public
    // wrapper advances each incomplete enum on the path by one variant per
    // pass, but only if the enum is cleared from the tracer's incomplete set
    // before the pass (an enum marked incomplete is pinned to variant 0 to
    // avoid runaway recursion). NOTE: the clear must come at the top of the
    // pass, not the bottom. An enum that completes drops out of the
    // incomplete set on its own, so with the clear at the top a still
    // incomplete enum survives the final pass and the registry() call below
    // fails and names it if the pass count ever becomes too low. A clear at
    // the bottom would erase that evidence and let a truncated enum into the
    // registry silently.
    for _ in 0..128 {
        for private_enum in [
            "MatcherImpl",
            "DateTimeField",
            "DateTimeFormatNode",
            "OrdinalMode",
        ] {
            tracer.check_incomplete_enum(private_enum);
        }
        tracer
            .trace_type::<Matcher>(samples)
            .map_err(|err| ("Matcher".to_string(), err))?;
        tracer
            .trace_type::<ToCharTimestamp>(samples)
            .map_err(|err| ("ToCharTimestamp".to_string(), err))?;
    }

    Ok(())
}

/// Summarizes what changed between two schema JSON documents, per container.
///
/// For changed enums, names the added and removed variants. For other
/// changes, names the container. The full detail is always available by
/// diffing the snapshot against [`CURRENT_PATH`].
fn schema_diff(expected: &str, actual: &str) -> String {
    fn containers(json: &str) -> BTreeMap<String, serde_json::Value> {
        serde_json::from_str(json).expect("schema files are JSON objects")
    }
    fn variant_names(container: &serde_json::Value) -> Option<Vec<&str>> {
        let variants = container.get("ENUM")?.as_object()?;
        Some(variants.keys().map(String::as_str).collect())
    }

    let expected = containers(expected);
    let actual = containers(actual);
    let mut lines = Vec::new();
    for name in actual.keys() {
        if !expected.contains_key(name) {
            lines.push(format!("  added container: {name}"));
        }
    }
    for name in expected.keys() {
        if !actual.contains_key(name) {
            lines.push(format!("  removed container: {name}"));
        }
    }
    for (name, new) in &actual {
        let Some(old) = expected.get(name) else {
            continue;
        };
        if old == new {
            continue;
        }
        match (variant_names(old), variant_names(new)) {
            (Some(old_vs), Some(new_vs)) => {
                let added: Vec<_> = new_vs.iter().filter(|v| !old_vs.contains(v)).collect();
                let removed: Vec<_> = old_vs.iter().filter(|v| !new_vs.contains(v)).collect();
                if added.is_empty() && removed.is_empty() {
                    lines.push(format!("  changed container: {name} (variant contents)"));
                } else {
                    lines.push(format!(
                        "  changed container: {name} (variants added: {added:?}, removed: {removed:?})"
                    ));
                }
            }
            _ => lines.push(format!("  changed container: {name}")),
        }
    }
    lines.join("\n")
}

/// The traced schema must match the checked-in snapshot for [`LIR_VERSION`].
///
/// Run with `REWRITE=1` to regenerate the current version's snapshot. The
/// rewrite never touches other versions' snapshots.
#[mz_ore::test]
fn lir_schema_snapshot() {
    let registry = trace_lir_registry();
    let actual = registry_json(&registry);
    let path = snapshot_path();

    if std::env::var_os("REWRITE").is_some() {
        std::fs::create_dir_all(SNAPSHOT_DIR).expect("create snapshot dir");
        std::fs::write(&path, actual).expect("write snapshot");
        return;
    }

    let expected = std::fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!(
            "missing LIR schema snapshot '{path}'.\n\n\
             Generate it with:\n\n    \
             REWRITE=1 cargo test -p mz-compute-types --test lir_schema\n"
        )
    });
    if expected != actual {
        panic!(
            "The serialized stable LIR schema changed!\n\n\
             The serde type graph reachable from LirRelationExpr no longer matches\n\
             '{path}'. This affects any durably stored LIR plan.\n\n\
             What changed:\n{diff}\n\n\
             Full detail: diff '{path}' against the freshly traced schema at\n\
             '{CURRENT_PATH}'.\n\n\
             If LIR version {LIR_VERSION} has already shipped, bump LIR_VERSION in\n\
             src/compute-types/src/plan.rs so the change lands as a new version.\n\
             If version {LIR_VERSION} is unshipped, regenerating in place is fine.\n\n\
             Then regenerate the snapshot and review the diff:\n\n    \
             REWRITE=1 cargo test -p mz-compute-types --test lir_schema\n\n\
             See doc/developer/design/20260311_optimizer_customer_tradeoff.md.\n",
            diff = schema_diff(&expected, &actual),
        );
    }
}

/// The schema must contain the types that make up the stable LIR surface.
///
/// This guards against silently losing part of the traced graph, e.g. if a
/// field's type stops being a named container.
#[mz_ore::test]
fn lir_schema_contains_expected_types() {
    let registry = trace_lir_registry();
    const EXPECTED: &[&str] = &[
        // The LIR AST itself.
        "LirRelationExpr",
        "LirRelationNode",
        "LirScalarExpr",
        "LirAggregateExpr",
        // Function enums.
        "UnaryFunc",
        "BinaryFunc",
        "VariadicFunc",
        "TableFunc",
        "AggregateFunc",
        // Node-level plans.
        "GetPlan",
        "JoinPlan",
        "ReducePlan",
        "KeyValPlan",
        "TopKPlan",
        "ThresholdPlan",
        "AvailableCollections",
        "ArrangementStrategy",
        // MFPs.
        "MapFilterProject",
        "SafeMfpPlan",
        "MfpPlan",
        // Identifiers.
        "Id",
        "LocalId",
        "GlobalId",
        // Data and types.
        "StableRow",
        "ReprScalarType",
        "ReprColumnType",
        "StableEvalError",
        "LetRecLimit",
        "Regex",
    ];
    let missing: Vec<_> = EXPECTED
        .iter()
        .filter(|name| !registry.contains_key(**name))
        .collect();
    assert!(
        missing.is_empty(),
        "expected containers missing from the LIR schema: {missing:?}"
    );
}

/// The schema must only contain stable types. No MIR type may be reachable.
///
/// `LirScalarExpr` exists precisely to keep `MirScalarExpr` (and with it
/// `UnmaterializableFunc`) out of the stored format, `LirAggregateExpr` does
/// the same for aggregates, and the `UnaryFunc<E>` parameter does the same
/// for the cast funcs that store a nested cast expression (for example
/// CastArrayToArray's cast_expr, which is a LirScalarExpr in the traced
/// instantiation).
#[mz_ore::test]
fn lir_schema_contains_only_stable_types() {
    let registry = trace_lir_registry();

    let Some(ContainerFormat::Enum(variants)) = registry.get("LirScalarExpr") else {
        panic!("LirScalarExpr missing from registry or not an enum");
    };
    let variant_names: Vec<_> = variants.values().map(|v| v.name.as_str()).collect();
    assert_eq!(
        variant_names,
        [
            "Column",
            "Literal",
            "CallUnary",
            "CallBinary",
            "CallVariadic",
            "If"
        ],
        "unexpected LirScalarExpr variants"
    );

    // Row and EvalError are forbidden because their serde impls track
    // unstable Rust definitions (the in-memory datum encoding, the error
    // enum's variants). The stable surface must go through StableRow and
    // StableEvalError, which serialize as proto-encoded bytes.
    for forbidden in [
        "MirScalarExpr",
        "UnmaterializableFunc",
        "AggregateExpr",
        "MirRelationExpr",
        "Row",
        "EvalError",
    ] {
        assert!(
            !registry.contains_key(forbidden),
            "unstable type '{forbidden}' is reachable from LirRelationExpr"
        );
    }

    // Opaque bytes are a hole in the schema: the registry cannot see what is
    // inside them, so their contents are only as stable as whatever encodes
    // them. StableRow and StableEvalError are the two sanctioned holes, their
    // proto encodings are CI-guarded elsewhere (see the module docs). Any
    // other container carrying BYTES smuggles an unguarded format into the
    // stable surface.
    let bytes_bearing: Vec<_> = registry
        .iter()
        .filter(|(_, container)| {
            let mut has_bytes = false;
            container
                .visit(&mut |format| {
                    has_bytes |= matches!(format, Format::Bytes);
                    Ok(())
                })
                .expect("validated registry has no unresolved formats");
            has_bytes
        })
        .map(|(name, _)| name.as_str())
        .collect();
    assert_eq!(
        bytes_bearing,
        ["StableEvalError", "StableRow"],
        "only StableRow and StableEvalError may serialize as opaque bytes"
    );
}
