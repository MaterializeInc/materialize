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
//! instance of every variant, and `tests/func_registry.rs` compares the
//! result against a checked-in snapshot per LIR version. For `#[sqlfunc]`
//! functions the record also carries the declaration text and a fingerprint
//! of the function body, see [`SqlFuncSource`].
//!
//! Variants whose payload cannot be constructed without data need a
//! representative [`Sample`] in this module. Building the registry panics
//! and names the variant otherwise. A sample may also be given for a
//! constructible variant, to probe its output type at chosen input types.

use std::collections::BTreeMap;

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
    ArrayCreate, ArrayFill, ArrayIndex, ArrayToString, ListCreate, MapBuild, RangeCreate,
    RecordCreate,
};
use crate::func::*;
use crate::{BinaryFunc, MirScalarExpr, UnaryFunc, VariadicFunc, like_pattern};

/// A representative instance of a function variant.
///
/// `input_types` are the column types the output type is probed at. Leave
/// them empty to skip the probe, for functions whose output typing does not
/// depend on a specific input shape.
#[derive(Debug)]
pub struct Sample<F> {
    pub func: F,
    pub input_types: Vec<SqlColumnType>,
}

/// The declared properties of every variant of the three scalar function
/// enums, keyed by canonical variant name (see
/// [`FuncName`]).
#[derive(Debug, Serialize)]
pub struct FuncRegistry {
    #[serde(rename = "UnaryFunc")]
    pub unary: BTreeMap<String, UnaryFuncProperties>,
    #[serde(rename = "BinaryFunc")]
    pub binary: BTreeMap<String, BinaryFuncProperties>,
    #[serde(rename = "VariadicFunc")]
    pub variadic: BTreeMap<String, VariadicFuncProperties>,
}

/// The `#[sqlfunc]` source of a function, in the registry's serialized form.
///
/// `body_fingerprint` is the one field of a registry record that describes
/// the implementation rather than a declared property. The snapshot test
/// treats a change to it alone as informational.
#[derive(Debug, Serialize)]
pub struct SourceProperties {
    pub sqlfunc_decl: Option<&'static str>,
    pub body_fingerprint: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct UnaryFuncProperties {
    pub variant: String,
    pub display: String,
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
    #[serde(flatten)]
    pub source: SourceProperties,
}

#[derive(Debug, Serialize)]
pub struct BinaryFuncProperties {
    pub variant: String,
    pub display: String,
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
    #[serde(flatten)]
    pub source: SourceProperties,
}

#[derive(Debug, Serialize)]
pub struct VariadicFuncProperties {
    pub variant: String,
    pub display: String,
    pub propagates_nulls: bool,
    pub introduces_nulls: bool,
    pub could_error: bool,
    pub is_monotone: bool,
    pub is_associative: bool,
    pub is_infix_op: bool,
    pub input_types: Vec<String>,
    pub output_type: Option<String>,
    #[serde(flatten)]
    pub source: SourceProperties,
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
                unary_samples(),
                UnaryFunc::variant_name,
                UnaryFuncProperties::of,
            ),
            binary: collect(
                "BinaryFunc",
                BinaryFunc::variant_names(),
                BinaryFunc::from_variant_name,
                binary_samples(),
                BinaryFunc::variant_name,
                BinaryFuncProperties::of,
            ),
            variadic: collect(
                "VariadicFunc",
                VariadicFunc::variant_names(),
                VariadicFunc::from_variant_name,
                variadic_samples(),
                VariadicFunc::variant_name,
                VariadicFuncProperties::of,
            ),
        }
    }
}

/// Records the properties of every variant of one function enum, keyed by
/// canonical name.
///
/// Each name in `names` resolves to a [`Sample`]: the hand-written one from
/// `samples` if present, otherwise a payload-free instance from `construct`
/// (see `from_variant_name` on the enums). Hand-written samples take
/// precedence so a constructible variant can still be probed at chosen input
/// types.
///
/// Panics if a name has neither, or if two samples name the same variant. A
/// sample for a name outside `names` is unreachable in practice because it
/// is keyed by `variant_name`, and it is silently dropped.
fn collect<F, P>(
    enum_name: &str,
    names: impl Iterator<Item = &'static str>,
    construct: fn(&str) -> Option<F>,
    samples: Vec<Sample<F>>,
    variant_name: fn(&F) -> &'static str,
    properties: fn(&Sample<F>) -> P,
) -> BTreeMap<String, P> {
    let mut by_name: BTreeMap<&'static str, Sample<F>> = BTreeMap::new();
    for sample in samples {
        let name = variant_name(&sample.func);
        let duplicate = by_name.insert(name, sample).is_some();
        assert!(!duplicate, "duplicate {enum_name} sample for `{name}`");
    }

    names
        .map(|name| {
            let sample = by_name.remove(name).or_else(|| {
                construct(name).map(|func| Sample {
                    func,
                    input_types: vec![],
                })
            });
            let sample = sample.unwrap_or_else(|| {
                panic!(
                    "{enum_name} variant `{name}` cannot be constructed from its name because \
                     its payload needs data. Add a representative Sample for it to \
                     {}_samples() in src/expr/src/scalar/func/registry.rs.",
                    enum_name.trim_end_matches("Func").to_lowercase()
                )
            });
            (name.to_string(), properties(&sample))
        })
        .collect()
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

impl From<Option<SqlFuncSource>> for SourceProperties {
    fn from(source: Option<SqlFuncSource>) -> Self {
        SourceProperties {
            sqlfunc_decl: source.map(|s| s.decl),
            body_fingerprint: source.map(|s| format!("{:016x}", s.body_fingerprint)),
        }
    }
}

fn type_strings(types: &[SqlColumnType]) -> Vec<String> {
    types.iter().map(ToString::to_string).collect()
}

impl UnaryFuncProperties {
    fn of(sample: &Sample<UnaryFunc>) -> Self {
        let func = &sample.func;
        let output_type = match sample.input_types.as_slice() {
            [] => None,
            [input] => Some(func.output_sql_type(input.clone()).to_string()),
            _ => panic!("unary sample `{}` needs exactly one input type", func),
        };
        UnaryFuncProperties {
            variant: variant_ident(func),
            display: func.to_string(),
            propagates_nulls: func.propagates_nulls(),
            introduces_nulls: func.introduces_nulls(),
            could_error: func.could_error(),
            preserves_uniqueness: func.preserves_uniqueness(),
            is_monotone: func.is_monotone(),
            is_eliminable_cast: func.is_eliminable_cast(),
            inverse: func.inverse().map(|f| f.variant_name()),
            input_types: type_strings(&sample.input_types),
            output_type,
            source: func.sqlfunc_source().into(),
        }
    }
}

impl BinaryFuncProperties {
    fn of(sample: &Sample<BinaryFunc>) -> Self {
        let func = &sample.func;
        let output_type = match sample.input_types.as_slice() {
            [] => None,
            [_, _] => Some(func.output_sql_type(&sample.input_types).to_string()),
            _ => panic!("binary sample `{}` needs exactly two input types", func),
        };
        BinaryFuncProperties {
            variant: variant_ident(func),
            display: func.to_string(),
            propagates_nulls: func.propagates_nulls(),
            introduces_nulls: func.introduces_nulls(),
            could_error: func.could_error(),
            is_monotone: func.is_monotone(),
            is_infinity_monotone: func.is_infinity_monotone(),
            is_infix_op: func.is_infix_op(),
            negate: func.negate().map(|f| f.variant_name()),
            input_types: type_strings(&sample.input_types),
            output_type,
            source: func.sqlfunc_source().into(),
        }
    }
}

impl VariadicFuncProperties {
    fn of(sample: &Sample<VariadicFunc>) -> Self {
        let func = &sample.func;
        let output_type = (!sample.input_types.is_empty())
            .then(|| func.output_sql_type(sample.input_types.clone()).to_string());
        VariadicFuncProperties {
            variant: variant_ident(func),
            display: func.to_string(),
            propagates_nulls: func.propagates_nulls(),
            introduces_nulls: func.introduces_nulls(),
            could_error: func.could_error(),
            is_monotone: func.is_monotone(),
            is_associative: func.is_associative(),
            is_infix_op: func.is_infix_op(),
            input_types: type_strings(&sample.input_types),
            output_type,
            source: func.sqlfunc_source().into(),
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
    ]
}

fn binary<F: Into<BinaryFunc>>(
    func: F,
    left: SqlScalarType,
    right: SqlScalarType,
) -> Sample<BinaryFunc> {
    Sample {
        func: func.into(),
        input_types: vec![left.nullable(false), right.nullable(false)],
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
    ]
}
