// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Registry of the declared properties of every scalar function variant.
//!
//! The stable LIR schema (see `lir_schema.rs` in `mz-compute-types`) pins the
//! *shape* of [`UnaryFunc`], [`BinaryFunc`] and [`VariadicFunc`]: their
//! variants and payloads. It cannot see the properties the optimizer and the
//! renderer rely on, such as null propagation, error behavior, monotonicity
//! and output typing, nor the function bodies themselves. A change to any of
//! those changes the meaning of a stored LIR plan without changing its
//! serialized form.
//!
//! [`FuncRegistry::build`] records those properties for one representative
//! instance of every variant, and `tests/func_registry.rs` in
//! `mz-compute-types` compares the result against checked-in snapshots, one
//! per half of [`Record`]. The module exists only under the `func-registry` feature,
//! which that test enables. Production builds carry none of it. For `#[sqlfunc]`
//! functions the record also carries the declaration text, the types-only
//! signature and a fingerprint of the function body, see [`SqlFuncSource`],
//! and the output type is probed at the column types the parameter types
//! map to, see [`ColumnTypeProbe`].
//!
//! Variants whose payload cannot be constructed without data need a
//! representative [`Sample`] in this module. Building the registry panics
//! and names the variant otherwise. Samples may also be given for a
//! constructible variant, to probe its output type at chosen input types or
//! to record payloads whose properties differ, see [`Sample`].

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use mz_repr::adt::char::CharLength;
use mz_repr::adt::datetime::DateTimeUnits;
use mz_repr::adt::numeric::NumericMaxScale;
use mz_repr::adt::regex::Regex;
use mz_repr::adt::timestamp::TimestampPrecision;
use mz_repr::adt::varchar::VarCharMaxLength;
use mz_repr::{ColumnName, Datum, Row, SqlColumnType, SqlScalarType, StableRow};
use serde::Serialize;

use crate::func::format::DateTimeFormat;
use crate::func::variadic::{
    And, ArrayCreate, ArrayFill, ArrayIndex, ArrayToString, Coalesce, ErrorIfNull, Greatest, Least,
    ListCreate, MapBuild, Or, RangeCreate, RecordCreate,
};
use crate::func::*;
use crate::{BinaryFunc, MirScalarExpr, UnaryFunc, VariadicFunc, like_pattern};

/// A representative instance of a function variant.
///
/// `input_types` are the column types the output type is probed at. Leave
/// them empty to skip the probe, for functions whose output typing does not
/// depend on a specific input shape.
///
/// A variant's properties can depend on its payload, for example a numeric
/// cast errors only when it has a scale. One sample sees one point of each
/// such property function, so a variant may have several samples, told apart
/// by `label`. The unlabeled sample is the variant's primary record, keyed by
/// its canonical name. Labeled samples are keyed `name[label]`.
#[derive(Debug)]
pub struct Sample<F> {
    pub func: F,
    pub input_types: Vec<SqlColumnType>,
    pub label: &'static str,
}

impl<F> Sample<F> {
    /// Marks this as an additional sample of its variant, recorded alongside
    /// the primary one under `name[label]`.
    pub fn labeled(self, label: &'static str) -> Self {
        assert!(!label.is_empty(), "sample labels must be non-empty");
        Sample { label, ..self }
    }
}

/// The registry of every variant of the three scalar function enums, keyed
/// by canonical variant name (see [`FuncName`]) as described on [`Sample`].
#[derive(Debug)]
pub struct FuncRegistry {
    pub unary: BTreeMap<String, Record<UnaryFuncProperties>>,
    pub binary: BTreeMap<String, Record<BinaryFuncProperties>>,
    pub variadic: BTreeMap<String, Record<VariadicFuncProperties>>,
}

/// One registry entry: the properties that decide what a stored plan using
/// the function computes, and the source they were read from.
///
/// The two halves are snapshotted separately by the test in
/// `mz-compute-types`. A change to `properties` of a shipped LIR version
/// requires a version bump. A change to `source` alone is informational.
#[derive(Debug)]
pub struct Record<P> {
    pub properties: P,
    pub source: FuncSource,
}

/// Where a record's properties came from.
///
/// `display` only feeds EXPLAIN output, since LIR stores variant names.
/// `sqlfunc_decl` is the declaration text, whose semantic content the
/// properties already carry as their own fields. `body_fingerprint` tracks
/// the implementation, whose semantics only a reader can judge.
#[derive(Debug, Serialize)]
pub struct FuncSource {
    pub display: String,
    pub sqlfunc_decl: Option<&'static str>,
    pub body_fingerprint: Option<String>,
}

impl FuncSource {
    fn of(display: String, source: Option<SqlFuncSource>) -> Self {
        FuncSource {
            display,
            sqlfunc_decl: source.map(|s| s.decl),
            body_fingerprint: source.map(|s| format!("{:016x}", s.body_fingerprint)),
        }
    }
}

/// Resolves a Rust parameter type to its column type by autoref
/// specialization: `(&ColumnTypeProbe::<T>(PhantomData)).column_type()` picks
/// [`ProbeColumnType`] when `T: AsColumnType` and [`ProbeColumnTypeFallback`]
/// otherwise. `#[sqlfunc]` emits that expression for each parameter, which is
/// how it can ask for a column type without knowing whether one exists.
#[derive(Debug)]
pub struct ColumnTypeProbe<T>(pub std::marker::PhantomData<T>);

/// The specialized arm of [`ColumnTypeProbe`].
pub trait ProbeColumnType {
    fn column_type(&self) -> Option<SqlColumnType>;
}

impl<T: mz_repr::AsColumnType> ProbeColumnType for ColumnTypeProbe<T> {
    fn column_type(&self) -> Option<SqlColumnType> {
        Some(T::as_column_type())
    }
}

/// The fallback arm of [`ColumnTypeProbe`], reached through one more autoref
/// than [`ProbeColumnType`] so it only applies when that one does not.
pub trait ProbeColumnTypeFallback {
    fn column_type(&self) -> Option<SqlColumnType>;
}

impl<T> ProbeColumnTypeFallback for &ColumnTypeProbe<T> {
    fn column_type(&self) -> Option<SqlColumnType> {
        None
    }
}

#[derive(Debug, Serialize)]
pub struct UnaryFuncProperties {
    pub variant: String,
    pub propagates_nulls: bool,
    pub introduces_nulls: bool,
    pub could_error: bool,
    pub preserves_uniqueness: bool,
    pub is_monotone: bool,
    pub is_eliminable_cast: bool,
    /// Canonical name of the inverse function, if one is declared.
    pub inverse: Option<&'static str>,
    pub input_types: Vec<String>,
    pub output_type: Option<String>,
    /// The `#[sqlfunc]` parameter and return types as written, see
    /// [`SqlFuncSource::signature`].
    pub sqlfunc_signature: Option<&'static str>,
}

#[derive(Debug, Serialize)]
pub struct BinaryFuncProperties {
    pub variant: String,
    pub propagates_nulls: bool,
    pub introduces_nulls: bool,
    pub could_error: bool,
    pub is_monotone: (bool, bool),
    pub is_infinity_monotone: bool,
    pub is_infix_op: bool,
    /// Canonical name of the negated function, if one is declared.
    pub negate: Option<&'static str>,
    pub input_types: Vec<String>,
    pub output_type: Option<String>,
    /// The `#[sqlfunc]` parameter and return types as written, see
    /// [`SqlFuncSource::signature`].
    pub sqlfunc_signature: Option<&'static str>,
}

#[derive(Debug, Serialize)]
pub struct VariadicFuncProperties {
    pub variant: String,
    pub propagates_nulls: bool,
    pub introduces_nulls: bool,
    pub could_error: bool,
    pub is_monotone: bool,
    pub is_associative: bool,
    pub is_infix_op: bool,
    pub input_types: Vec<String>,
    pub output_type: Option<String>,
    /// The `#[sqlfunc]` parameter and return types as written, see
    /// [`SqlFuncSource::signature`].
    pub sqlfunc_signature: Option<&'static str>,
}

impl FuncRegistry {
    /// Records the properties of every variant of the three function enums.
    ///
    /// Panics if a variant can neither be constructed from its canonical name
    /// nor has a [`Sample`] in this module, or if a sample's input count does
    /// not fit its function.
    pub fn build() -> FuncRegistry {
        FuncRegistry {
            unary: collect(
                "UnaryFunc",
                UnaryFunc::variant_names(),
                UnaryFunc::from_variant_name,
                UnaryFunc::sqlfunc_input_types,
                unary_samples(),
                UnaryFunc::variant_name,
                UnaryFuncProperties::of,
            ),
            binary: collect(
                "BinaryFunc",
                BinaryFunc::variant_names(),
                BinaryFunc::from_variant_name,
                BinaryFunc::sqlfunc_input_types,
                binary_samples(),
                BinaryFunc::variant_name,
                BinaryFuncProperties::of,
            ),
            variadic: collect(
                "VariadicFunc",
                VariadicFunc::variant_names(),
                VariadicFunc::from_variant_name,
                VariadicFunc::sqlfunc_input_types,
                variadic_samples(),
                VariadicFunc::variant_name,
                VariadicFuncProperties::of,
            ),
        }
    }
}

/// Records the properties of every variant of one function enum, keyed as
/// described on [`Sample`].
///
/// A variant's primary record comes from its unlabeled hand-written sample if
/// there is one, otherwise from a payload-free instance built by `construct`
/// (see `from_variant_name` on the enums), probed at the column types
/// `natural_inputs` reports for it, if any. Hand-written primaries take
/// precedence so a constructible variant can still be probed at chosen input
/// types.
///
/// A variant whose payload has fields must have at least one hand-written
/// sample, even when the payload is constructible with every field defaulted,
/// because a defaulted payload sees only one branch of any property that
/// depends on it.
///
/// Panics if a variant has no primary or no required sample, if two samples
/// share a name and label, or if probing a sample panics, in which case the
/// panic names the sample.
fn collect<F: fmt::Debug, P>(
    enum_name: &str,
    names: impl Iterator<Item = &'static str>,
    construct: fn(&str) -> Option<F>,
    natural_inputs: fn(&F) -> Option<Vec<SqlColumnType>>,
    samples: Vec<Sample<F>>,
    variant_name: fn(&F) -> &'static str,
    properties: fn(&Sample<F>) -> P,
) -> BTreeMap<String, P> {
    let samples_fn = format!(
        "{}_samples() in src/expr/src/scalar/func/registry.rs",
        enum_name.trim_end_matches("Func").to_lowercase()
    );
    let mut by_name: BTreeMap<(&'static str, &'static str), Sample<F>> = BTreeMap::new();
    let mut hand_written = BTreeSet::new();
    for sample in samples {
        let name = variant_name(&sample.func);
        let label = sample.label;
        hand_written.insert(name);
        let duplicate = by_name.insert((name, label), sample).is_some();
        assert!(
            !duplicate,
            "duplicate {enum_name} sample for `{name}` with label `{label}`"
        );
    }

    let mut records = BTreeMap::new();
    for name in names {
        let primary = by_name.remove(&(name, "")).or_else(|| {
            construct(name).map(|func| Sample {
                input_types: natural_inputs(&func).unwrap_or_default(),
                func,
                label: "",
            })
        });
        let primary = primary.unwrap_or_else(|| {
            panic!(
                "{enum_name} variant `{name}` cannot be constructed from its name because \
                 its payload needs data. Add a representative Sample for it to {samples_fn}."
            )
        });
        assert!(
            hand_written.contains(name) || !payload_has_fields(&primary.func),
            "{enum_name} variant `{name}` has a payload with fields but only its defaulted \
             instance is recorded. Add a Sample with a non-default payload to {samples_fn}, \
             labeled if the defaulted instance should stay the primary record."
        );
        records.insert(
            name.to_string(),
            probe(enum_name, name, &primary, properties),
        );
    }
    for ((name, label), sample) in by_name {
        assert!(
            records.contains_key(name),
            "{enum_name} sample `{name}[{label}]` names an unknown variant"
        );
        records.insert(
            format!("{name}[{label}]"),
            probe(enum_name, &format!("{name}[{label}]"), &sample, properties),
        );
    }
    records
}

/// Whether a variant's payload struct has any fields.
///
/// Decided from the `Debug` rendering, which every payload derives: a unit
/// payload renders as `Variant(Payload)`, one with fields opens a second
/// parenthesis or a brace.
fn payload_has_fields<F: fmt::Debug>(func: &F) -> bool {
    let rendered = format!("{func:?}");
    rendered.matches('(').count() > 1 || rendered.contains('{')
}

/// Runs `properties` on a sample, attributing any panic (typically an
/// `output_sql_type` impl rejecting the sample's input types) to the sample.
fn probe<F, P>(
    enum_name: &str,
    key: &str,
    sample: &Sample<F>,
    properties: fn(&Sample<F>) -> P,
) -> P {
    mz_ore::panic::catch_unwind_str(std::panic::AssertUnwindSafe(|| properties(sample)))
        .unwrap_or_else(|message| {
            panic!(
                "probing {enum_name} sample `{key}` panicked: {message}\n\
                 Check the sample's input types in src/expr/src/scalar/func/registry.rs."
            )
        })
}

/// The serde variant name, which is what the stable LIR format stores.
fn variant_ident<F: Serialize>(func: &F) -> String {
    let value = serde_json::to_value(func).expect("function variants serialize");
    match value {
        serde_json::Value::Object(map) if map.len() == 1 => {
            map.into_iter().next().expect("one entry").0
        }
        serde_json::Value::String(name) => name,
        other => panic!("unexpected function variant encoding: {other}"),
    }
}

fn type_strings(types: &[SqlColumnType]) -> Vec<String> {
    types.iter().map(ToString::to_string).collect()
}

impl UnaryFuncProperties {
    fn of(sample: &Sample<UnaryFunc>) -> Record<Self> {
        let func = &sample.func;
        let output_type = match sample.input_types.as_slice() {
            [] => None,
            [input] => Some(func.output_sql_type(input.clone()).to_string()),
            _ => panic!("unary sample `{}` needs exactly one input type", func),
        };
        let source = func.sqlfunc_source();
        let properties = UnaryFuncProperties {
            variant: variant_ident(func),
            propagates_nulls: func.propagates_nulls(),
            introduces_nulls: func.introduces_nulls(),
            could_error: func.could_error(),
            preserves_uniqueness: func.preserves_uniqueness(),
            is_monotone: func.is_monotone(),
            is_eliminable_cast: func.is_eliminable_cast(),
            inverse: func.inverse().map(|f| f.variant_name()),
            input_types: type_strings(&sample.input_types),
            output_type,
            sqlfunc_signature: source.map(|s| s.signature),
        };
        Record {
            properties,
            source: FuncSource::of(func.to_string(), source),
        }
    }
}

impl BinaryFuncProperties {
    fn of(sample: &Sample<BinaryFunc>) -> Record<Self> {
        let func = &sample.func;
        let output_type = match sample.input_types.as_slice() {
            [] => None,
            [_, _] => Some(func.output_sql_type(&sample.input_types).to_string()),
            _ => panic!("binary sample `{}` needs exactly two input types", func),
        };
        let source = func.sqlfunc_source();
        let properties = BinaryFuncProperties {
            variant: variant_ident(func),
            propagates_nulls: func.propagates_nulls(),
            introduces_nulls: func.introduces_nulls(),
            could_error: func.could_error(),
            is_monotone: func.is_monotone(),
            is_infinity_monotone: func.is_infinity_monotone(),
            is_infix_op: func.is_infix_op(),
            negate: func.negate().map(|f| f.variant_name()),
            input_types: type_strings(&sample.input_types),
            output_type,
            sqlfunc_signature: source.map(|s| s.signature),
        };
        Record {
            properties,
            source: FuncSource::of(func.to_string(), source),
        }
    }
}

impl VariadicFuncProperties {
    fn of(sample: &Sample<VariadicFunc>) -> Record<Self> {
        let func = &sample.func;
        let output_type = (!sample.input_types.is_empty())
            .then(|| func.output_sql_type(sample.input_types.clone()).to_string());
        let source = func.sqlfunc_source();
        let properties = VariadicFuncProperties {
            variant: variant_ident(func),
            propagates_nulls: func.propagates_nulls(),
            introduces_nulls: func.introduces_nulls(),
            could_error: func.could_error(),
            is_monotone: func.is_monotone(),
            is_associative: func.is_associative(),
            is_infix_op: func.is_infix_op(),
            input_types: type_strings(&sample.input_types),
            output_type,
            sqlfunc_signature: source.map(|s| s.signature),
        };
        Record {
            properties,
            source: FuncSource::of(func.to_string(), source),
        }
    }
}

fn column(i: usize) -> Box<MirScalarExpr> {
    Box::new(MirScalarExpr::column(i))
}

fn record_type() -> SqlScalarType {
    SqlScalarType::Record {
        fields: [
            ("a".into(), SqlScalarType::Int32.nullable(false)),
            ("b".into(), SqlScalarType::String.nullable(true)),
        ]
        .into(),
        custom_id: None,
    }
}

fn list_type(element: SqlScalarType) -> SqlScalarType {
    SqlScalarType::List {
        element_type: Box::new(element),
        custom_id: None,
    }
}

fn map_type(value: SqlScalarType) -> SqlScalarType {
    SqlScalarType::Map {
        value_type: Box::new(value),
        custom_id: None,
    }
}

fn range_type(element: SqlScalarType) -> SqlScalarType {
    SqlScalarType::Range {
        element_type: Box::new(element),
    }
}

fn array_type(element: SqlScalarType) -> SqlScalarType {
    SqlScalarType::Array(Box::new(element))
}

fn regex() -> Regex {
    Regex::new("a+", false).expect("valid regex")
}

fn unary<F: Into<UnaryFunc>>(func: F, input: SqlScalarType) -> Sample<UnaryFunc> {
    Sample {
        func: func.into(),
        input_types: vec![input.nullable(false)],
        label: "",
    }
}

fn unary_samples() -> Vec<Sample<UnaryFunc>> {
    let timestamp_precision = Some(TimestampPrecision::try_from(3i64).expect("valid precision"));
    let numeric_scale = NumericMaxScale::try_from(2i64).expect("valid scale");
    let char_length = Some(CharLength::try_from(5i64).expect("valid length"));
    let varchar_length = Some(VarCharMaxLength::try_from(5i64).expect("valid length"));
    let tz = mz_pgtz::timezone::Timezone::Tz(chrono_tz::Tz::UTC);
    vec![
        // Casts between container types, which carry their target type.
        unary(
            CastArrayToString {
                ty: array_type(SqlScalarType::Int32),
            },
            array_type(SqlScalarType::Int32),
        ),
        unary(
            CastArrayToJsonb {
                cast_element: column(0),
            },
            array_type(SqlScalarType::Int32),
        ),
        unary(
            CastArrayToArray {
                return_ty: array_type(SqlScalarType::Int64),
                cast_expr: column(0),
            },
            array_type(SqlScalarType::Int32),
        ),
        unary(
            CastListToString {
                ty: list_type(SqlScalarType::Int32),
            },
            list_type(SqlScalarType::Int32),
        ),
        unary(
            CastListToJsonb {
                cast_element: column(0),
            },
            list_type(SqlScalarType::Int32),
        ),
        unary(
            CastList1ToList2 {
                return_ty: list_type(SqlScalarType::Int64),
                cast_expr: column(0),
            },
            list_type(SqlScalarType::Int32),
        ),
        unary(
            CastMapToString {
                ty: map_type(SqlScalarType::Int32),
            },
            map_type(SqlScalarType::Int32),
        ),
        unary(
            MapBuildFromRecordList {
                value_type: SqlScalarType::Int32,
            },
            list_type(record_type()),
        ),
        unary(
            CastRangeToString {
                ty: range_type(SqlScalarType::Int32),
            },
            range_type(SqlScalarType::Int32),
        ),
        unary(CastRecordToString { ty: record_type() }, record_type()),
        unary(
            CastRecord1ToRecord2 {
                return_ty: record_type(),
                cast_exprs: vec![*column(0), *column(1)].into(),
            },
            record_type(),
        ),
        unary(RecordGet(1), record_type()),
        // The null-fallback wrapper around a cast. Its properties derive from
        // the wrapped function, so the samples cover the branches that differ:
        // a fallible parse, a narrowing cast with an inverse (which the wrapper
        // must not inherit), a nullable output the wrapper only forces nullable,
        // a container cast whose element cast lives inside the wrapper, and a
        // wrapper around a function that cannot error, which the planner never
        // emits but the type allows.
        unary(
            TryCast {
                inner: Box::new(CastStringToInt32.into()),
            },
            SqlScalarType::String,
        ),
        unary(
            TryCast {
                inner: Box::new(CastInt64ToInt32.into()),
            },
            SqlScalarType::Int64,
        )
        .labeled("narrowing"),
        unary(
            TryCast {
                inner: Box::new(CastJsonbToNumeric(None).into()),
            },
            SqlScalarType::Jsonb,
        )
        .labeled("jsonb"),
        unary(
            TryCast {
                inner: Box::new(
                    CastList1ToList2 {
                        return_ty: list_type(SqlScalarType::Int32),
                        cast_expr: column(0),
                    }
                    .into(),
                ),
            },
            list_type(SqlScalarType::Int64),
        )
        .labeled("list"),
        unary(
            TryCast {
                inner: Box::new(CastInt32ToInt64.into()),
            },
            SqlScalarType::Int32,
        )
        .labeled("infallible"),
        unary(
            CastStringToArray {
                return_ty: array_type(SqlScalarType::Int32),
                cast_expr: column(0),
            },
            SqlScalarType::String,
        ),
        unary(
            CastStringToList {
                return_ty: list_type(SqlScalarType::Int32),
                cast_expr: column(0),
            },
            SqlScalarType::String,
        ),
        unary(
            CastStringToMap {
                return_ty: map_type(SqlScalarType::Int32),
                cast_expr: column(0),
            },
            SqlScalarType::String,
        ),
        unary(
            CastStringToRange {
                return_ty: range_type(SqlScalarType::Int32),
                cast_expr: column(0),
            },
            SqlScalarType::String,
        ),
        // Length-parameterized string casts.
        unary(
            CastStringToChar {
                length: char_length,
                fail_on_len: true,
            },
            SqlScalarType::String,
        ),
        unary(
            PadChar {
                length: char_length,
            },
            SqlScalarType::String,
        ),
        unary(
            CastStringToVarChar {
                length: varchar_length,
                fail_on_len: true,
            },
            SqlScalarType::String,
        ),
        // Precision and scale parameterized casts.
        unary(
            CastTimestampToTimestampTz {
                from: None,
                to: timestamp_precision,
            },
            SqlScalarType::Timestamp { precision: None },
        ),
        unary(
            CastTimestampTzToTimestamp {
                from: None,
                to: timestamp_precision,
            },
            SqlScalarType::TimestampTz { precision: None },
        ),
        unary(
            AdjustTimestampPrecision {
                from: None,
                to: timestamp_precision,
            },
            SqlScalarType::Timestamp { precision: None },
        ),
        unary(
            AdjustTimestampTzPrecision {
                from: None,
                to: timestamp_precision,
            },
            SqlScalarType::TimestampTz { precision: None },
        ),
        unary(
            AdjustNumericScale(numeric_scale),
            SqlScalarType::Numeric { max_scale: None },
        ),
        // Pattern matching.
        unary(
            IsLikeMatch(like_pattern::compile("%a%", false).expect("valid pattern")),
            SqlScalarType::String,
        ),
        unary(IsRegexpMatch(regex()), SqlScalarType::String),
        unary(RegexpMatch(regex()), SqlScalarType::String),
        unary(RegexpSplitToArray(regex()), SqlScalarType::String),
        // Date and time functions carrying units, time zones and formats.
        unary(
            ExtractInterval(DateTimeUnits::Epoch),
            SqlScalarType::Interval,
        ),
        unary(ExtractTime(DateTimeUnits::Epoch), SqlScalarType::Time),
        unary(
            ExtractTimestamp(DateTimeUnits::Epoch),
            SqlScalarType::Timestamp { precision: None },
        ),
        unary(
            ExtractTimestampTz(DateTimeUnits::Epoch),
            SqlScalarType::TimestampTz { precision: None },
        ),
        unary(ExtractDate(DateTimeUnits::Epoch), SqlScalarType::Date),
        unary(
            DatePartInterval(DateTimeUnits::Epoch),
            SqlScalarType::Interval,
        ),
        unary(DatePartTime(DateTimeUnits::Epoch), SqlScalarType::Time),
        unary(
            DatePartTimestamp(DateTimeUnits::Epoch),
            SqlScalarType::Timestamp { precision: None },
        ),
        unary(
            DatePartTimestampTz(DateTimeUnits::Epoch),
            SqlScalarType::TimestampTz { precision: None },
        ),
        unary(
            DateTruncTimestamp(DateTimeUnits::Day),
            SqlScalarType::Timestamp { precision: None },
        ),
        unary(
            DateTruncTimestampTz(DateTimeUnits::Day),
            SqlScalarType::TimestampTz { precision: None },
        ),
        unary(
            TimezoneTimestamp(tz.clone()),
            SqlScalarType::Timestamp { precision: None },
        ),
        unary(
            TimezoneTimestampTz(tz.clone()),
            SqlScalarType::TimestampTz { precision: None },
        ),
        unary(
            TimezoneTime {
                tz,
                wall_time: chrono::NaiveDateTime::default(),
            },
            SqlScalarType::Time,
        ),
        unary(
            ToCharTimestamp {
                format_string: "YYYY".into(),
                format: DateTimeFormat::compile("YYYY"),
            },
            SqlScalarType::Timestamp { precision: None },
        ),
        unary(
            ToCharTimestampTz {
                format_string: "YYYY".into(),
                format: DateTimeFormat::compile("YYYY"),
            },
            SqlScalarType::TimestampTz { precision: None },
        ),
        // Payload-free hand-written casts, probed at their natural input.
        unary(CastStringToInt2Vector, SqlScalarType::String),
        unary(CastDateToTimestamp(None), SqlScalarType::Date),
        unary(CastDateToTimestampTz(None), SqlScalarType::Date),
        unary(CastStringToTimestamp(None), SqlScalarType::String),
        unary(CastStringToTimestampTz(None), SqlScalarType::String),
        unary(
            CastDateToTimestamp(timestamp_precision),
            SqlScalarType::Date,
        )
        .labeled("precision"),
        unary(
            CastDateToTimestampTz(timestamp_precision),
            SqlScalarType::Date,
        )
        .labeled("precision"),
        unary(
            CastStringToTimestamp(timestamp_precision),
            SqlScalarType::String,
        )
        .labeled("precision"),
        unary(
            CastStringToTimestampTz(timestamp_precision),
            SqlScalarType::String,
        )
        .labeled("precision"),
        // Without a length to enforce, these casts cannot error.
        unary(
            CastStringToChar {
                length: None,
                fail_on_len: false,
            },
            SqlScalarType::String,
        )
        .labeled("unbounded"),
        unary(PadChar { length: None }, SqlScalarType::String).labeled("unbounded"),
        unary(
            CastStringToVarChar {
                length: None,
                fail_on_len: false,
            },
            SqlScalarType::String,
        )
        .labeled("unbounded"),
        // Widening a precision preserves uniqueness, narrowing does not.
        unary(
            CastTimestampToTimestampTz {
                from: timestamp_precision,
                to: None,
            },
            SqlScalarType::Timestamp {
                precision: timestamp_precision,
            },
        )
        .labeled("widening"),
        unary(
            CastTimestampTzToTimestamp {
                from: timestamp_precision,
                to: None,
            },
            SqlScalarType::TimestampTz {
                precision: timestamp_precision,
            },
        )
        .labeled("widening"),
        unary(
            AdjustTimestampPrecision {
                from: timestamp_precision,
                to: None,
            },
            SqlScalarType::Timestamp {
                precision: timestamp_precision,
            },
        )
        .labeled("widening"),
        unary(
            AdjustTimestampTzPrecision {
                from: timestamp_precision,
                to: None,
            },
            SqlScalarType::TimestampTz {
                precision: timestamp_precision,
            },
        )
        .labeled("widening"),
        // Units below the most significant ones are not monotone.
        unary(
            ExtractInterval(DateTimeUnits::Month),
            SqlScalarType::Interval,
        )
        .labeled("month"),
        unary(ExtractTime(DateTimeUnits::Minute), SqlScalarType::Time).labeled("minute"),
        unary(
            ExtractTimestamp(DateTimeUnits::Month),
            SqlScalarType::Timestamp { precision: None },
        )
        .labeled("month"),
        unary(
            ExtractTimestampTz(DateTimeUnits::Month),
            SqlScalarType::TimestampTz { precision: None },
        )
        .labeled("month"),
        unary(ExtractDate(DateTimeUnits::Month), SqlScalarType::Date).labeled("month"),
        unary(
            DatePartInterval(DateTimeUnits::Month),
            SqlScalarType::Interval,
        )
        .labeled("month"),
        unary(DatePartTime(DateTimeUnits::Minute), SqlScalarType::Time).labeled("minute"),
        unary(
            DatePartTimestamp(DateTimeUnits::Month),
            SqlScalarType::Timestamp { precision: None },
        )
        .labeled("month"),
        unary(
            DatePartTimestampTz(DateTimeUnits::Month),
            SqlScalarType::TimestampTz { precision: None },
        )
        .labeled("month"),
    ]
    .into_iter()
    .chain(numeric_cast_samples(numeric_scale))
    .collect()
}

/// The numeric casts, each without a scale (the primary record) and with
/// one. Only the scaled cast can error, because it rounds.
fn numeric_cast_samples(scale: NumericMaxScale) -> Vec<Sample<UnaryFunc>> {
    let casts: [(fn(Option<NumericMaxScale>) -> UnaryFunc, SqlScalarType); 10] = [
        (|s| CastInt16ToNumeric(s).into(), SqlScalarType::Int16),
        (|s| CastInt32ToNumeric(s).into(), SqlScalarType::Int32),
        (|s| CastInt64ToNumeric(s).into(), SqlScalarType::Int64),
        (|s| CastUint16ToNumeric(s).into(), SqlScalarType::UInt16),
        (|s| CastUint32ToNumeric(s).into(), SqlScalarType::UInt32),
        (|s| CastUint64ToNumeric(s).into(), SqlScalarType::UInt64),
        (|s| CastFloat32ToNumeric(s).into(), SqlScalarType::Float32),
        (|s| CastFloat64ToNumeric(s).into(), SqlScalarType::Float64),
        (|s| CastStringToNumeric(s).into(), SqlScalarType::String),
        (|s| CastJsonbToNumeric(s).into(), SqlScalarType::Jsonb),
    ];
    casts
        .into_iter()
        .flat_map(|(cast, input)| {
            [
                unary(cast(None), input.clone()),
                unary(cast(Some(scale)), input).labeled("scale"),
            ]
        })
        .collect()
}

fn binary<F: Into<BinaryFunc>>(
    func: F,
    left: SqlScalarType,
    right: SqlScalarType,
) -> Sample<BinaryFunc> {
    Sample {
        func: func.into(),
        input_types: vec![left.nullable(false), right.nullable(false)],
        label: "",
    }
}

fn binary_samples() -> Vec<Sample<BinaryFunc>> {
    vec![
        binary(
            ListLengthMax { max_layer: 1 },
            list_type(SqlScalarType::Int32),
            SqlScalarType::Int64,
        ),
        binary(
            RegexpReplace {
                regex: regex(),
                limit: 1,
            },
            SqlScalarType::String,
            SqlScalarType::String,
        ),
    ]
}

fn variadic<F: Into<VariadicFunc>>(func: F, inputs: Vec<SqlScalarType>) -> Sample<VariadicFunc> {
    Sample {
        func: func.into(),
        input_types: inputs.into_iter().map(|ty| ty.nullable(false)).collect(),
        label: "",
    }
}

fn variadic_samples() -> Vec<Sample<VariadicFunc>> {
    vec![
        variadic(
            ArrayCreate {
                elem_type: SqlScalarType::Int32,
            },
            vec![SqlScalarType::Int32, SqlScalarType::Int32],
        ),
        variadic(
            ArrayFill {
                elem_type: SqlScalarType::Int32,
            },
            vec![SqlScalarType::Int32, array_type(SqlScalarType::Int32)],
        ),
        variadic(
            ArrayIndex { offset: 1 },
            vec![array_type(SqlScalarType::Int32), SqlScalarType::Int64],
        ),
        variadic(
            ArrayToString {
                elem_type: SqlScalarType::Int32,
            },
            vec![array_type(SqlScalarType::Int32), SqlScalarType::String],
        ),
        variadic(
            ListCreate {
                elem_type: SqlScalarType::Int32,
            },
            vec![SqlScalarType::Int32, SqlScalarType::Int32],
        ),
        variadic(
            RangeCreate {
                elem_type: SqlScalarType::Int32,
            },
            vec![
                SqlScalarType::Int32,
                SqlScalarType::Int32,
                SqlScalarType::String,
            ],
        ),
        variadic(
            RecordCreate {
                field_names: vec![ColumnName::from("a".to_string())],
            },
            vec![SqlScalarType::Int32],
        ),
        variadic(
            MapBuild {
                value_type: SqlScalarType::Int32,
            },
            vec![SqlScalarType::String, SqlScalarType::Int32],
        ),
        variadic(
            CaseLiteral {
                lookup: vec![CaseLiteralEntry {
                    literal: StableRow::from(Row::pack_slice(&[Datum::Int32(1)])),
                    expr_index: 0,
                }],
                return_type: SqlScalarType::String.nullable(true),
            },
            vec![SqlScalarType::Int32, SqlScalarType::String],
        ),
        // Payload-free hand-written functions, probed at their natural inputs.
        variadic(And, vec![SqlScalarType::Bool, SqlScalarType::Bool]),
        variadic(Or, vec![SqlScalarType::Bool, SqlScalarType::Bool]),
        variadic(Coalesce, vec![SqlScalarType::Int32, SqlScalarType::Int32]),
        variadic(Greatest, vec![SqlScalarType::Int32, SqlScalarType::Int32]),
        variadic(Least, vec![SqlScalarType::Int32, SqlScalarType::Int32]),
        variadic(
            ErrorIfNull,
            vec![SqlScalarType::Int32, SqlScalarType::String],
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn payload_field_detection() {
        fn unary(func: impl Into<UnaryFunc>) -> UnaryFunc {
            func.into()
        }
        assert!(!payload_has_fields(&unary(Not)));
        assert!(payload_has_fields(&unary(CastInt32ToNumeric(None))));
        assert!(payload_has_fields(&unary(PadChar { length: None })));
        assert!(!payload_has_fields(&VariadicFunc::from(And)));
        assert!(payload_has_fields(&VariadicFunc::from(ArrayIndex {
            offset: 0
        })));
    }
}
