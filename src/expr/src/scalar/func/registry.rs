// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Registry of the declared properties of every scalar, aggregate and table
//! function variant.
//!
//! The stable LIR schema (see `lir_schema.rs` in `mz-compute-types`) pins the
//! *shape* of [`UnaryFunc`], [`BinaryFunc`], [`VariadicFunc`],
//! [`AggregateFunc`] and [`TableFunc`]: their variants and payloads. It
//! cannot see the properties the optimizer and the renderer rely on, such as
//! null propagation, error behavior, monotonicity and output typing, nor the
//! function bodies themselves. A change to any of
//! those changes the meaning of a stored LIR plan without changing its
//! serialized form.
//!
//! [`FuncRegistry::build`] records those properties for one representative
//! instance of every variant, and `tests/builtin_registry.rs` in
//! `mz-catalog` compares the result against checked-in snapshots, one
//! per half of [`Record`]. The module exists only under the `func-registry` feature,
//! which that test enables. Production builds carry none of it. For `#[sqlfunc]`
//! functions the record also carries the declaration text, the types-only
//! signature and a fingerprint of the function body, see [`SqlFuncSource`],
//! and the output type is probed at the column types the parameter types
//! map to, see [`ColumnTypeProbe`].
//!
//! Aggregate and table functions have no canonical names of their own, so
//! their variants are listed from their `Deserialize` impls and keyed by serde
//! variant name, which is what LIR stores. All of them are hand-written, so
//! their records carry no source beyond the display name.
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
use mz_repr::{ColumnName, Datum, Row, SqlColumnType, SqlRelationType, SqlScalarType, StableRow};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::func::format::DateTimeFormat;
use crate::func::variadic::{
    And, ArrayCreate, ArrayFill, ArrayIndex, ArrayToString, Coalesce, ErrorIfNull, Greatest, Least,
    ListCreate, MapBuild, Or, RangeCreate, RecordCreate,
};
use crate::func::*;
use crate::{
    AggregateFunc, AnalyzedRegex, AnalyzedRegexOpts, BinaryFunc, ColumnOrder, LagLeadType,
    MirScalarExpr, TableFunc, UnaryFunc, VariadicFunc, WindowFrame, WindowFrameBound,
    WindowFrameUnits, like_pattern,
};

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
/// name as described on [`FuncRegistry`]. Labeled samples are keyed
/// `name[label]`.
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

/// The registry of every variant of the function enums, keyed as described on
/// [`Sample`] by canonical variant name (see [`FuncName`]) for the scalar
/// functions and by serde variant name for the others.
#[derive(Debug)]
pub struct FuncRegistry {
    pub unary: BTreeMap<String, Record<UnaryFunc, UnaryFuncProperties>>,
    pub binary: BTreeMap<String, Record<BinaryFunc, BinaryFuncProperties>>,
    pub variadic: BTreeMap<String, Record<VariadicFunc, VariadicFuncProperties>>,
    pub aggregate: BTreeMap<String, Record<AggregateFunc, AggregateFuncProperties>>,
    pub table: BTreeMap<String, Record<TableFunc, TableFuncProperties>>,
}

/// One registry entry: the properties that decide what a stored plan using
/// the function computes, the source they were read from, and the instance
/// they were probed from. The instance lets a crate that `mz-expr` cannot
/// depend on record properties it owns under the same key.
///
/// The two halves are snapshotted separately by the test in
/// `mz-catalog`. A change to `properties` of a shipped LIR version
/// requires a version bump. A change to `source` alone is informational.
#[derive(Debug)]
pub struct Record<F, P> {
    pub func: F,
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

#[derive(Debug, Serialize)]
pub struct AggregateFuncProperties {
    pub ignores_multiplicity: bool,
    pub propagates_nonnull_constraint: bool,
    pub can_fuse_with_unnest_list: bool,
    /// The result on empty input, as a `Datum` debug rendering.
    pub default: String,
    /// The datum whose inclusion leaves the result unchanged, as a `Datum`
    /// debug rendering.
    pub identity_datum: String,
    pub input_types: Vec<String>,
    pub output_type: Option<String>,
    /// The output type at the same input made nullable. Most aggregates pass
    /// input nullability through, so this and `output_type` together pin the
    /// nullability rule.
    pub output_type_on_nullable_input: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TableFuncProperties {
    pub output_types: Vec<String>,
    pub output_keys: Vec<Vec<usize>>,
    pub output_arity: usize,
    pub empty_on_null_input: bool,
    pub preserves_monotonicity: bool,
    pub supports_with_ordinality: bool,
}

impl FuncRegistry {
    /// Records the properties of every variant of the function enums.
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
            aggregate: collect(
                "AggregateFunc",
                serde_variants::<AggregateFunc>().iter().copied(),
                from_serde_variant,
                aggregate_natural_inputs,
                aggregate_samples(),
                serde_variant_name,
                AggregateFuncProperties::of,
            ),
            table: collect(
                "TableFunc",
                serde_variants::<TableFunc>().iter().copied(),
                from_serde_variant,
                |_| None,
                table_samples(),
                serde_variant_name,
                TableFuncProperties::of,
            ),
        }
    }
}

/// The variant names an enum's `Deserialize` impl declares.
///
/// Derived impls pass them to `Deserializer::deserialize_enum`, which is the
/// only place serde exposes them. This deserializer records them there and
/// then fails.
fn serde_variants<F: DeserializeOwned>() -> &'static [&'static str] {
    use serde::de::{Error, Visitor};

    struct Probe<'a>(&'a std::cell::Cell<&'static [&'static str]>);

    impl<'de> serde::Deserializer<'de> for Probe<'_> {
        type Error = serde::de::value::Error;

        fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Self::Error> {
            Err(Error::custom("not an enum"))
        }

        fn deserialize_enum<V: Visitor<'de>>(
            self,
            _name: &'static str,
            variants: &'static [&'static str],
            _: V,
        ) -> Result<V::Value, Self::Error> {
            self.0.set(variants);
            Err(Error::custom("variants recorded"))
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes
            byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct map
            struct identifier ignored_any
        }
    }

    let variants = std::cell::Cell::new(&[][..]);
    let _ = F::deserialize(Probe(&variants));
    let variants = variants.get();
    assert!(
        !variants.is_empty(),
        "`{}` does not deserialize as an enum",
        std::any::type_name::<F>()
    );
    variants
}

/// Constructs a unit variant from its serde name. `None` for variants with a
/// payload.
fn from_serde_variant<F: DeserializeOwned>(name: &str) -> Option<F> {
    serde_json::from_value(serde_json::Value::String(name.to_string())).ok()
}

fn serde_variant_name<F: Serialize + DeserializeOwned>(func: &F) -> &'static str {
    let ident = variant_ident(func);
    serde_variants::<F>()
        .iter()
        .find(|name| **name == ident)
        .unwrap_or_else(|| panic!("serialized variant `{ident}` is not a declared variant"))
}

/// Records the properties of every variant of one function enum, keyed as
/// described on [`Sample`].
///
/// A variant's primary record comes from its unlabeled hand-written sample if
/// there is one, otherwise from a payload-free instance built by `construct`
/// (`from_variant_name` on the scalar enums, `from_serde_variant` for the
/// others), probed at the column types
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
    fn of(sample: &Sample<UnaryFunc>) -> Record<UnaryFunc, Self> {
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
            func: func.clone(),
            properties,
            source: FuncSource::of(func.to_string(), source),
        }
    }
}

impl BinaryFuncProperties {
    fn of(sample: &Sample<BinaryFunc>) -> Record<BinaryFunc, Self> {
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
            func: func.clone(),
            properties,
            source: FuncSource::of(func.to_string(), source),
        }
    }
}

impl VariadicFuncProperties {
    fn of(sample: &Sample<VariadicFunc>) -> Record<VariadicFunc, Self> {
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
            func: func.clone(),
            properties,
            source: FuncSource::of(func.to_string(), source),
        }
    }
}

impl AggregateFuncProperties {
    fn of(sample: &Sample<AggregateFunc>) -> Record<AggregateFunc, Self> {
        let func = &sample.func;
        let (output_type, output_type_on_nullable_input) = match sample.input_types.as_slice() {
            [] => (None, None),
            [input] => (
                Some(func.output_sql_type(input.clone()).to_string()),
                Some(
                    func.output_sql_type(input.scalar_type.clone().nullable(true))
                        .to_string(),
                ),
            ),
            _ => panic!(
                "aggregate sample `{}` needs exactly one input type",
                func.name()
            ),
        };
        let properties = AggregateFuncProperties {
            ignores_multiplicity: func.ignores_multiplicity(),
            propagates_nonnull_constraint: func.propagates_nonnull_constraint(),
            can_fuse_with_unnest_list: func.can_fuse_with_unnest_list(),
            default: format!("{:?}", func.default()),
            identity_datum: format!("{:?}", func.identity_datum()),
            input_types: type_strings(&sample.input_types),
            output_type,
            output_type_on_nullable_input,
        };
        Record {
            func: func.clone(),
            properties,
            source: FuncSource::of(func.name().to_string(), None),
        }
    }
}

impl TableFuncProperties {
    fn of(sample: &Sample<TableFunc>) -> Record<TableFunc, Self> {
        let func = &sample.func;
        assert!(
            sample.input_types.is_empty(),
            "table function output types do not depend on input types, \
             so sample `{func}` must not give any"
        );
        let output = func.output_sql_type();
        let properties = TableFuncProperties {
            output_types: type_strings(&output.column_types),
            output_keys: output.keys,
            output_arity: func.output_arity(),
            empty_on_null_input: func.empty_on_null_input(),
            preserves_monotonicity: func.preserves_monotonicity(),
            supports_with_ordinality: TableFunc::with_ordinality(func.clone()).is_some(),
        };
        Record {
            func: func.clone(),
            properties,
            source: FuncSource::of(func.to_string(), None),
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

fn time_record_type() -> SqlScalarType {
    SqlScalarType::Record {
        fields: [("a".into(), SqlScalarType::Time.nullable(false))].into(),
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
        // and a container cast whose element cast lives inside the wrapper.
        unary(
            UnaryFunc::try_cast_if_fallible(CastStringToInt32),
            SqlScalarType::String,
        ),
        unary(
            UnaryFunc::try_cast_if_fallible(CastInt64ToInt32),
            SqlScalarType::Int64,
        )
        .labeled("narrowing"),
        unary(
            UnaryFunc::try_cast_if_fallible(CastJsonbToNumeric(None)),
            SqlScalarType::Jsonb,
        )
        .labeled("jsonb"),
        unary(
            UnaryFunc::try_cast_if_fallible(CastList1ToList2 {
                return_ty: list_type(SqlScalarType::Int32),
                cast_expr: column(0),
            }),
            list_type(SqlScalarType::Int64),
        )
        .labeled("list"),
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
        unary(
            Jq(JqProgram::new(".a").expect("valid program")),
            SqlScalarType::Jsonb,
        ),
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
        // Text rounds `time` elements to microseconds, so these casts do not
        // preserve uniqueness.
        unary(
            CastArrayToString {
                ty: array_type(SqlScalarType::Time),
            },
            array_type(SqlScalarType::Time),
        )
        .labeled("time"),
        unary(
            CastListToString {
                ty: list_type(SqlScalarType::Time),
            },
            list_type(SqlScalarType::Time),
        )
        .labeled("time"),
        unary(
            CastMapToString {
                ty: map_type(SqlScalarType::Time),
            },
            map_type(SqlScalarType::Time),
        )
        .labeled("time"),
        unary(
            CastRangeToString {
                ty: range_type(SqlScalarType::Time),
            },
            range_type(SqlScalarType::Time),
        )
        .labeled("time"),
        unary(
            CastRecordToString {
                ty: time_record_type(),
            },
            time_record_type(),
        )
        .labeled("time"),
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

/// The input column type a payload-free aggregate is probed at. `None` for
/// variants with a payload, which have samples in [`aggregate_samples`].
fn aggregate_natural_inputs(func: &AggregateFunc) -> Option<Vec<SqlColumnType>> {
    use AggregateFunc::*;
    // No wildcard arm, so a new variant does not compile until it is placed.
    let input = match func {
        MaxNumeric | MinNumeric | SumNumeric => SqlScalarType::Numeric { max_scale: None },
        MaxInt16 | MinInt16 | SumInt16 => SqlScalarType::Int16,
        MaxInt32 | MinInt32 | SumInt32 => SqlScalarType::Int32,
        MaxInt64 | MinInt64 | SumInt64 => SqlScalarType::Int64,
        MaxUInt16 | MinUInt16 | SumUInt16 => SqlScalarType::UInt16,
        MaxUInt32 | MinUInt32 | SumUInt32 => SqlScalarType::UInt32,
        MaxUInt64 | MinUInt64 | SumUInt64 => SqlScalarType::UInt64,
        MaxMzTimestamp | MinMzTimestamp => SqlScalarType::MzTimestamp,
        MaxFloat32 | MinFloat32 | SumFloat32 => SqlScalarType::Float32,
        MaxFloat64 | MinFloat64 | SumFloat64 => SqlScalarType::Float64,
        MaxBool | MinBool | Any | All => SqlScalarType::Bool,
        MaxString | MinString => SqlScalarType::String,
        MaxDate | MinDate => SqlScalarType::Date,
        MaxTimestamp | MinTimestamp => SqlScalarType::Timestamp { precision: None },
        MaxTimestampTz | MinTimestampTz => SqlScalarType::TimestampTz { precision: None },
        MaxInterval | MinInterval | SumInterval => SqlScalarType::Interval,
        MaxTime | MinTime => SqlScalarType::Time,
        MaxArray | MinArray => array_type(SqlScalarType::Int32),
        MaxList | MinList => list_type(SqlScalarType::Int32),
        Count | Dummy => SqlScalarType::Int32,
        JsonbAgg { .. }
        | JsonbObjectAgg { .. }
        | MapAgg { .. }
        | ArrayConcat { .. }
        | ListConcat { .. }
        | StringAgg { .. }
        | RowNumber { .. }
        | Rank { .. }
        | DenseRank { .. }
        | LagLead { .. }
        | FirstValue { .. }
        | LastValue { .. }
        | FusedValueWindowFunc { .. }
        | WindowAggregate { .. }
        | FusedWindowAggregate { .. } => return None,
    };
    Some(vec![input.nullable(false)])
}

fn record_of(fields: Vec<SqlScalarType>) -> SqlScalarType {
    SqlScalarType::Record {
        fields: fields
            .into_iter()
            .enumerate()
            .map(|(i, ty)| (ColumnName::from(format!("f{i}")), ty.nullable(false)))
            .collect(),
        custom_id: None,
    }
}

fn aggregate(func: AggregateFunc, input: SqlScalarType) -> Sample<AggregateFunc> {
    Sample {
        func,
        input_types: vec![input.nullable(false)],
        label: "",
    }
}

fn aggregate_samples() -> Vec<Sample<AggregateFunc>> {
    let order_by = || {
        vec![ColumnOrder {
            column: 1,
            desc: true,
            nulls_last: true,
        }]
    };
    let window_frame = || WindowFrame {
        units: WindowFrameUnits::Rows,
        start_bound: WindowFrameBound::OffsetPreceding(2),
        end_bound: WindowFrameBound::CurrentRow,
    };
    let lag_lead = |lag_lead| AggregateFunc::LagLead {
        order_by: order_by(),
        lag_lead,
        ignore_nulls: false,
    };
    // Order-sensitive aggregates take a record of the aggregated value and
    // the ORDER BY columns. The ORDER BY column's type differs from every
    // value type below, so reading the wrong field changes the output type.
    let ordered = |value| record_of(vec![value, SqlScalarType::Date]);
    let original_row = record_of(vec![SqlScalarType::Int32]);
    // Ranking functions take the partition's original rows as a list.
    let ranking_input = ordered(list_type(original_row.clone()));
    // Value window functions and window aggregates take a record of the
    // original row and the encoded arguments.
    let window_input = |args| ordered(record_of(vec![original_row.clone(), args]));
    // lag and lead encode (value, offset, default). The value's type differs
    // from the offset's, so reading the wrong field changes the output type.
    let lag_lead_args = record_of(vec![
        SqlScalarType::String,
        SqlScalarType::Int32,
        SqlScalarType::String,
    ]);
    let numeric_with_scale = || SqlScalarType::Numeric {
        max_scale: Some(NumericMaxScale::try_from(2i64).expect("valid scale")),
    };
    let precision = || Some(TimestampPrecision::try_from(3i64).expect("valid precision"));
    let timestamp_with_precision = || SqlScalarType::Timestamp {
        precision: precision(),
    };
    let timestamptz_with_precision = || SqlScalarType::TimestampTz {
        precision: precision(),
    };
    vec![
        aggregate(
            AggregateFunc::JsonbAgg {
                order_by: order_by(),
            },
            ordered(SqlScalarType::Jsonb),
        ),
        aggregate(
            AggregateFunc::JsonbObjectAgg {
                order_by: order_by(),
            },
            ordered(record_of(vec![SqlScalarType::String, SqlScalarType::Jsonb])),
        ),
        aggregate(
            AggregateFunc::MapAgg {
                order_by: order_by(),
                value_type: SqlScalarType::Int32,
            },
            ordered(record_of(vec![SqlScalarType::String, SqlScalarType::Int32])),
        ),
        aggregate(
            AggregateFunc::ArrayConcat {
                order_by: order_by(),
            },
            ordered(array_type(SqlScalarType::Int32)),
        ),
        aggregate(
            AggregateFunc::ListConcat {
                order_by: order_by(),
            },
            ordered(list_type(SqlScalarType::Int32)),
        ),
        aggregate(
            AggregateFunc::StringAgg {
                order_by: order_by(),
            },
            ordered(record_of(vec![
                SqlScalarType::String,
                SqlScalarType::String,
            ])),
        ),
        aggregate(
            AggregateFunc::StringAgg {
                order_by: order_by(),
            },
            ordered(SqlScalarType::Record {
                fields: vec![
                    (ColumnName::from("f0"), SqlScalarType::String.nullable(true)),
                    (
                        ColumnName::from("f1"),
                        SqlScalarType::String.nullable(false),
                    ),
                ]
                .into(),
                custom_id: None,
            }),
        )
        .labeled("nullable_value"),
        aggregate(
            AggregateFunc::RowNumber {
                order_by: order_by(),
            },
            ranking_input.clone(),
        ),
        aggregate(
            AggregateFunc::Rank {
                order_by: order_by(),
            },
            ranking_input.clone(),
        ),
        aggregate(
            AggregateFunc::DenseRank {
                order_by: order_by(),
            },
            ranking_input,
        ),
        aggregate(
            lag_lead(LagLeadType::Lag),
            window_input(lag_lead_args.clone()),
        ),
        aggregate(
            lag_lead(LagLeadType::Lead),
            window_input(lag_lead_args.clone()),
        )
        .labeled("lead"),
        aggregate(
            AggregateFunc::FirstValue {
                order_by: order_by(),
                window_frame: window_frame(),
            },
            window_input(SqlScalarType::Int32),
        ),
        aggregate(
            AggregateFunc::LastValue {
                order_by: order_by(),
                window_frame: window_frame(),
            },
            window_input(SqlScalarType::Int32),
        ),
        aggregate(
            AggregateFunc::FusedValueWindowFunc {
                funcs: vec![
                    lag_lead(LagLeadType::Lag),
                    AggregateFunc::FirstValue {
                        order_by: order_by(),
                        window_frame: window_frame(),
                    },
                ],
                order_by: order_by(),
            },
            window_input(record_of(vec![lag_lead_args.clone(), SqlScalarType::Int32])),
        ),
        aggregate(
            AggregateFunc::FusedValueWindowFunc {
                funcs: vec![
                    lag_lead(LagLeadType::Lead),
                    AggregateFunc::LastValue {
                        order_by: order_by(),
                        window_frame: window_frame(),
                    },
                ],
                order_by: order_by(),
            },
            window_input(record_of(vec![lag_lead_args, SqlScalarType::Int32])),
        )
        .labeled("lead_last"),
        aggregate(
            AggregateFunc::WindowAggregate {
                wrapped_aggregate: Box::new(AggregateFunc::SumInt32),
                order_by: order_by(),
                window_frame: window_frame(),
            },
            window_input(SqlScalarType::Int32),
        ),
        aggregate(
            AggregateFunc::FusedWindowAggregate {
                wrapped_aggregates: vec![AggregateFunc::SumInt32, AggregateFunc::MaxString],
                order_by: order_by(),
                window_frame: window_frame(),
            },
            window_input(record_of(vec![SqlScalarType::Int32, SqlScalarType::String])),
        ),
        // These return their input type, which the natural `String`,
        // unscaled and unprecise inputs cannot tell from a fixed type.
        aggregate(
            AggregateFunc::MaxString,
            SqlScalarType::VarChar { max_length: None },
        )
        .labeled("varchar"),
        aggregate(
            AggregateFunc::MinString,
            SqlScalarType::VarChar { max_length: None },
        )
        .labeled("varchar"),
        aggregate(AggregateFunc::MaxNumeric, numeric_with_scale()).labeled("scale"),
        aggregate(AggregateFunc::MinNumeric, numeric_with_scale()).labeled("scale"),
        aggregate(AggregateFunc::SumNumeric, numeric_with_scale()).labeled("scale"),
        aggregate(AggregateFunc::MaxTimestamp, timestamp_with_precision()).labeled("precision"),
        aggregate(AggregateFunc::MinTimestamp, timestamp_with_precision()).labeled("precision"),
        aggregate(AggregateFunc::MaxTimestampTz, timestamptz_with_precision()).labeled("precision"),
        aggregate(AggregateFunc::MinTimestampTz, timestamptz_with_precision()).labeled("precision"),
    ]
}

fn table(func: TableFunc) -> Sample<TableFunc> {
    Sample {
        func,
        input_types: vec![],
        label: "",
    }
}

fn table_samples() -> Vec<Sample<TableFunc>> {
    let regex =
        AnalyzedRegex::new("(?<a>a+)(b)", AnalyzedRegexOpts::default()).expect("valid regex");
    vec![
        table(TableFunc::RegexpExtract(regex)),
        table(TableFunc::CsvExtract(2)),
        table(TableFunc::GuardSubquerySize {
            column_type: SqlScalarType::Int32,
        }),
        table(TableFunc::UnnestArray {
            el_typ: SqlScalarType::Int32,
        }),
        table(TableFunc::UnnestList {
            el_typ: SqlScalarType::Int32,
        }),
        table(TableFunc::UnnestMap {
            value_type: SqlScalarType::Int32,
        }),
        table(TableFunc::Wrap {
            types: vec![
                SqlScalarType::Int32.nullable(false),
                SqlScalarType::String.nullable(true),
            ],
            width: 2,
        }),
        table(TableFunc::TabletizedScalar {
            name: "tabletized".to_string(),
            relation: SqlRelationType::new(vec![SqlScalarType::Int32.nullable(true)]),
        }),
        table(
            TableFunc::with_ordinality(TableFunc::UnnestList {
                el_typ: SqlScalarType::Int32,
            })
            .expect("unnest_list supports WITH ORDINALITY"),
        ),
        table(
            TableFunc::with_ordinality(TableFunc::TabletizedScalar {
                name: "tabletized".to_string(),
                relation: SqlRelationType::new(vec![SqlScalarType::Int32.nullable(true)]),
            })
            .expect("tabletized scalars support WITH ORDINALITY"),
        )
        .labeled("tabletized"),
        // Neither inner function above has keys or breaks monotonicity.
        table(
            TableFunc::with_ordinality(TableFunc::GuardSubquerySize {
                column_type: SqlScalarType::Int32,
            })
            .expect("guard_subquery_size supports WITH ORDINALITY"),
        )
        .labeled("guard"),
        table(
            TableFunc::with_ordinality(TableFunc::UnnestMap {
                value_type: SqlScalarType::Int32,
            })
            .expect("unnest_map supports WITH ORDINALITY"),
        )
        .labeled("keyed"),
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

    /// `payload_has_fields` reads the scalar enums' `Variant(Payload)` shape.
    /// The aggregate and table enums hold their data directly, so a variant
    /// such as `CsvExtract(2)` would read as payload-free. The gate is still
    /// sound because `from_serde_variant` constructs only unit variants.
    #[mz_ore::test]
    fn variants_constructed_by_serde_name_are_units() {
        fn check<F: DeserializeOwned + fmt::Debug>() {
            for name in serde_variants::<F>() {
                if let Some(func) = from_serde_variant::<F>(name) {
                    let rendered = format!("{func:?}");
                    assert!(
                        !rendered.contains('(') && !rendered.contains('{'),
                        "`{name}` was constructed with a payload: {rendered}"
                    );
                }
            }
        }
        check::<AggregateFunc>();
        check::<TableFunc>();
    }
}
