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
//! Any change to the serialized format of LIR (including the `*Func` enums,
//! `EvalError`, `Row`, and everything else in the graph) changes the traced
//! schema and fails `lir_schema_snapshot` until the snapshot is regenerated
//! with `REWRITE=1`. The snapshot diff is the reviewable record of the format
//! change.
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
    AggregateFunc, BinaryFunc, DomainLimit, EvalError, Id, LagLeadType, LetRecLimit, TableFunc,
    UnaryFunc, VariadicFunc, WindowFrameBound, WindowFrameUnits,
};
use mz_pgtz::timezone::Timezone;
use mz_repr::adt::array::InvalidArrayError;
use mz_repr::adt::datetime::DateTimeUnits;
use mz_repr::adt::range::InvalidRangeError;
use mz_repr::strconv::{ParseErrorKind, ParseHexError};
use mz_repr::{CatalogItemId, GlobalId, ReprScalarType, Row, SqlScalarType};
use serde_reflection::{ContainerFormat, Registry, Samples, Tracer, TracerConfig};

const SNAPSHOT_DIR: &str = "tests/snapshots";

fn snapshot_path() -> String {
    format!("{SNAPSHOT_DIR}/lir_v{LIR_VERSION}.json")
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
             and NonZero integers reject the synthesized 0). Record a valid \
             sample of the smallest containing struct with tracer.trace_value \
             in the samples section of trace_lir_registry. Keep samples \
             minimal. A recorded sample is replayed wherever the type \
             appears."
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
             variant per pass and never completes. Add each listed enum to the \
             trace_enums! list in trace_lir_registry."
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
             matches. Check the samples section of trace_lir_registry for a \
             sample whose type changed shape, and regenerate or remove it."
        }
        Error::NotSupported(_) => {
            "serde_reflection cannot trace this construct (for example \
             deserialize_any or untagged enums). Change the type's serde \
             representation to something traceable."
        }
    };
    panic!("failed to trace {context}: {err}\n\n{remedy}\n");
}

/// Traces the full serde type graph reachable from [`LirRelationExpr`].
///
/// serde_reflection drives `Deserialize` impls with synthesized default
/// values (0, "", etc.). Types whose `Deserialize` checks invariants reject
/// those defaults, so we record a valid sample for each such type up front
/// and configure the tracer to replay recorded samples.
fn trace_lir_registry() -> Registry {
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
    let mut tracer = Tracer::new(config);
    let mut samples = Samples::new();

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
        .trace_value(&mut samples, &limit)
        .unwrap_or_else(|err| diagnose("LetRecLimit sample", err));

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
        .trace_value(&mut samples, &timezone_time)
        .unwrap_or_else(|err| diagnose("TimezoneTime sample", err));

    tracer
        .trace_type::<LirRelationExpr>(&samples)
        .unwrap_or_else(|err| diagnose("LirRelationExpr", err));

    // Tracing a struct explores nested enums one variant per pass, so every
    // enum in the graph needs its own trace_type call to cover all variants.
    // The registry() call below fails with "missing variants" naming any enum
    // that still needs to be added here.
    macro_rules! trace_enums {
        ($($ty:ty),* $(,)?) => {
            $(
                tracer
                    .trace_type::<$ty>(&samples)
                    .unwrap_or_else(|err| diagnose(stringify!($ty), err));
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
        UnaryFunc,
        BinaryFunc,
        VariadicFunc,
        TableFunc,
        AggregateFunc,
        EvalError,
        ReprScalarType,
        ConstantRows,
        LiteralValue,
        ArrangementStrategy,
        BasicPlan,
        HierarchicalPlan,
        GlobalId,
        SqlScalarType,
        DateTimeUnits,
        DomainLimit,
        InvalidArrayError,
        InvalidRangeError,
        LagLeadType,
        ParseErrorKind,
        ParseHexError,
        WindowFrameBound,
        WindowFrameUnits,
        Timezone,
        CatalogItemId,
        // Reachable via MirScalarExpr::Literal through the cast func leak
        // noted below. There is exactly one Result instantiation in the
        // graph, so tracing it does not clash. (The LIR types themselves
        // avoid std Result, see ConstantRows and LiteralValue.)
        Result<Row, EvalError>,
        // TODO: MirScalarExpr and UnmaterializableFunc are reachable through
        // the cast funcs that store a nested cast expression (for example
        // CastArrayToArray's cast_expr). Remove these once those funcs hold a
        // stable expression type, per the Lir*Func plan in
        // doc/developer/design/20260311_optimizer_customer_tradeoff.md.
        mz_expr::MirScalarExpr,
        mz_expr::UnmaterializableFunc,
    ];

    // Some enums in the graph are private (MatcherImpl in like_pattern, and
    // DateTimeField, DateTimeFormatNode, and OrdinalMode in the to_char
    // formatter), so they cannot be listed above. Tracing their public
    // wrapper advances each incomplete enum on the path by one variant per
    // pass, but only if the enum is cleared from the tracer's incomplete set
    // between passes (an enum marked incomplete is pinned to variant 0 to
    // avoid runaway recursion). If the pass count ever becomes too low, the
    // registry() call below fails and names the incomplete enum.
    for _ in 0..128 {
        tracer
            .trace_type::<Matcher>(&samples)
            .unwrap_or_else(|err| diagnose("Matcher", err));
        tracer
            .trace_type::<ToCharTimestamp>(&samples)
            .unwrap_or_else(|err| diagnose("ToCharTimestamp", err));
        for private_enum in [
            "MatcherImpl",
            "DateTimeField",
            "DateTimeFormatNode",
            "OrdinalMode",
        ] {
            tracer.check_incomplete_enum(private_enum);
        }
    }

    // Debug aid: dump the partial registry to a file to see every traced
    // container, for example to find what references an unexpected type.
    if std::env::var_os("DUMP_UNCHECKED").is_some() {
        let registry = tracer.registry_unchecked();
        std::fs::write(
            std::env::var("DUMP_UNCHECKED").unwrap(),
            format!("{registry:#?}"),
        )
        .unwrap();
        panic!("dumped unchecked registry");
    }

    tracer
        .registry()
        .unwrap_or_else(|err| diagnose("the completed registry", err))
}

/// The traced schema must match the checked-in snapshot for [`LIR_VERSION`].
///
/// Run with `REWRITE=1` to regenerate the current version's snapshot. The
/// rewrite never touches other versions' snapshots.
///
/// Run with `DUMP_UNCHECKED=/path/to/dump.txt` to dump the current schema to file for inspection.
#[mz_ore::test]
fn lir_schema_snapshot() {
    let registry = trace_lir_registry();
    let mut actual = serde_json::to_string_pretty(&registry).expect("registry serializes to JSON");
    // Lint requires text files to end with a newline.
    actual.push('\n');
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
             If LIR version {LIR_VERSION} has already shipped, bump LIR_VERSION in\n\
             src/compute-types/src/plan.rs so the change lands as a new version.\n\
             If version {LIR_VERSION} is unshipped, regenerating in place is fine.\n\n\
             Then regenerate the snapshot and review the diff:\n\n    \
             REWRITE=1 cargo test -p mz-compute-types --test lir_schema\n\n\
             See doc/developer/design/20260311_optimizer_customer_tradeoff.md.\n"
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
        "Row",
        "ReprScalarType",
        "ReprColumnType",
        "EvalError",
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

/// The schema must only contain stable types, with one known exception.
///
/// `LirScalarExpr` exists precisely to keep `MirScalarExpr` (and with it
/// `UnmaterializableFunc`) out of the stored format, and `LirAggregateExpr`
/// does the same for aggregates.
///
/// The known exception: cast funcs that store a nested cast expression (for
/// example CastArrayToArray's cast_expr and CastRecord1ToRecord2's
/// cast_exprs) still hold `MirScalarExpr`, which pulls in
/// `UnmaterializableFunc` through its CallUnmaterializable variant. We pin
/// the leak as a positive assertion so this test flips loudly when it is
/// fixed.
///
/// TODO: once those funcs hold a stable expression type (per the Lir*Func
/// plan in doc/developer/design/20260311_optimizer_customer_tradeoff.md),
/// move MirScalarExpr and UnmaterializableFunc into the forbidden list.
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

    for forbidden in ["AggregateExpr", "MirRelationExpr"] {
        assert!(
            !registry.contains_key(forbidden),
            "unstable type '{forbidden}' is reachable from LirRelationExpr"
        );
    }

    // The known leak, pinned. See the doc comment above.
    for leaked in ["MirScalarExpr", "UnmaterializableFunc"] {
        assert!(
            registry.contains_key(leaked),
            "'{leaked}' no longer leaks into the LIR schema. Remove it from \
             trace_enums! and move it to the forbidden list in this test."
        );
    }
}
