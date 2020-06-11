// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use serde::{Deserialize, Serialize};

use ore::collections::CollectionExt;
use repr::adt::datetime::DateTimeUnits;
use repr::adt::regex::Regex;
use repr::{ColumnName, ColumnType, Datum, RowArena, ScalarType};

use crate::{EvalError, ScalarExpr};

pub(crate) mod bool;
pub(crate) mod cmp;
pub(crate) mod datetime;
pub(crate) mod decimal;
pub(crate) mod float;
pub(crate) mod format;
pub(crate) mod integer;
pub(crate) mod interval;
pub(crate) mod jsonb;
pub(crate) mod list;
pub(crate) mod null;
pub(crate) mod record;
pub(crate) mod string;

/// Specifies the null behavior of a function.
#[derive(Debug)]
pub enum Nulls {
    /// The function never returns null, not even if the inputs to the function
    /// are null.
    Never,
    /// The function sometimes returns null.
    Sometimes {
        /// The function returns null if any of its inputs are null.
        propagates_nulls: bool,
        /// The function might return null even if its inputs are non-null.
        introduces_nulls: bool,
    },
}

/// The output type of a function.
pub enum OutputType {
    /// The output type is the same as the input type.
    ///
    /// If the function takes multiple arguments, this variant indicates that
    /// those arguments must have identical types. The function must take at
    /// least one argument.
    MatchesInput,
    /// The output type is fixed to the contained type.
    Fixed(ScalarType),
    /// The output type is computed based on the input types.
    Computed(fn(Vec<ColumnType>) -> ScalarType),
    Computed2(Box<dyn Fn(Vec<ColumnType>) -> ScalarType>),
}

impl fmt::Debug for OutputType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OutputType::MatchesInput => f.write_str("OutputType::MatchesInput"),
            OutputType::Fixed(ty) => write!(f, "OutputType::Fixed({:?})", ty),
            OutputType::Computed(_) => f.write_str("OutputType::Computed(<omitted>)"),
            OutputType::Computed2(_) => f.write_str("OutputType::Computed2(<omitted>)"),
        }
    }
}

/// Describes properties a function.
#[derive(Debug)]
pub struct FuncProps {
    /// Whether the function can produce an error.
    pub can_error: bool,
    /// True iff for x != y, we are assured f(x) != f(y).
    ///
    /// This is most often the case for methods that promote to types that
    /// can contain all the precision of the input type.
    pub preserves_uniqueness: bool,
    /// Specifies the null behavior of the function function.
    pub nulls: Nulls,
    /// Specifies the output type of the function.
    pub output_type: OutputType,
}

impl FuncProps {
    /// Returns whether this function returns null if any of its inputs are
    /// null.
    ///
    /// This is a convenience accessor for the field of the same name in
    /// [`Nulls`].
    pub fn propagates_nulls(&self) -> bool {
        match self.nulls {
            Nulls::Never => false,
            Nulls::Sometimes {
                propagates_nulls, ..
            } => propagates_nulls,
        }
    }

    fn output_type(&self, input_types: Vec<ColumnType>) -> ColumnType {
        let nullable = match self.nulls {
            Nulls::Never => false,
            Nulls::Sometimes {
                introduces_nulls: true,
                ..
            } => true,
            Nulls::Sometimes {
                introduces_nulls: false,
                ..
            } => input_types.iter().any(|t| t.nullable),
        };
        match &self.output_type {
            OutputType::Fixed(ty) => ColumnType::new(ty.clone()).nullable(nullable),
            OutputType::MatchesInput => {
                debug_assert!(
                    input_types
                        .windows(2)
                        .all(|w| w[0].scalar_type == w[1].scalar_type),
                    "inputs did not have uniform type: {:?}",
                    input_types
                );
                input_types.into_first().nullable(nullable)
            }
            OutputType::Computed(f) => ColumnType::new(f(input_types)).nullable(nullable),
            OutputType::Computed2(f) => ColumnType::new(f(input_types)).nullable(nullable),
        }
    }

    /// Checks whether the properties permit the supplied result for the
    /// supplied input.
    fn check(&self, input: &[Datum], result: &Result<Datum, EvalError>) -> Result<(), String> {
        let result = match result {
            Ok(result) => result,
            Err(_) if self.can_error => return Ok(()),
            Err(e) => return Err(format!("!can_error function returned error ({})", e)),
        };

        if result.is_null() {
            match self.nulls {
                Nulls::Never => Err("never-null function returned null".into()),
                Nulls::Sometimes {
                    introduces_nulls: false,
                    ..
                } if !input.iter().any(|d| d.is_null()) => {
                    Err("!introduces_null function introduced null".into())
                }
                _ => Ok(()),
            }
        } else {
            if let OutputType::Fixed(ty) = &self.output_type {
                if !result.is_instance_of_scalar(ty) {
                    return Err(format!(
                        "return value {:?} does not have expected type {:?}",
                        result, ty
                    ));
                }
            }
            Ok(())
        }
    }
}

/// Specifies the affixment of the string representation of an operator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Affixment {
    Prefix,
    Infix,
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum NullaryFunc {
    MzLogicalTimestamp,
}

impl NullaryFunc {
    pub fn output_type(&self) -> ColumnType {
        match self {
            NullaryFunc::MzLogicalTimestamp => ColumnType::new(ScalarType::Decimal(38, 0)),
        }
    }
}

impl fmt::Display for NullaryFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NullaryFunc::MzLogicalTimestamp => f.write_str("mz_logical_timestamp"),
        }
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum UnaryFunc {
    /// Boolean functions.
    CastBoolToStringExplicit,
    CastBoolToStringImplicit,
    CastStringToBool,
    Not,

    // Date and time functions.
    CastDateToString,
    CastTimeToString,
    CastTimestampToString,
    CastTimestampTzToString,
    CastStringToDate,
    CastStringToTime,
    CastStringToTimestamp,
    CastStringToTimestampTz,
    CastDateToTimestamp,
    CastDateToTimestampTz,
    CastTimestampToDate,
    CastTimestampTzToDate,
    CastTimestampToTimestampTz,
    CastTimestampTzToTimestamp,
    ToTimestamp,
    DatePartTimestamp(DateTimeUnits),
    DatePartTimestampTz(DateTimeUnits),
    DateTruncTimestamp(DateTimeUnits),
    DateTruncTimestampTz(DateTimeUnits),

    // Decimal functions.
    CastDecimalToString(u8),
    CastStringToDecimal(u8),
    CastDecimalToInt32,
    CastDecimalToInt64,
    CastDecimalToFloat32,
    CastDecimalToFloat64,
    AbsDecimal,
    NegDecimal,
    CeilDecimal(u8),
    FloorDecimal(u8),
    RoundDecimal(u8),
    SqrtDecimal(u8),

    // Float functions.
    CastFloat32ToString,
    CastFloat64ToString,
    CastStringToFloat32,
    CastStringToFloat64,
    CastFloat32ToInt64,
    CastFloat32ToFloat64,
    CastFloat64ToInt32,
    CastFloat64ToInt64,
    CastFloat32ToDecimal(u8),
    CastFloat64ToDecimal(u8),
    AbsFloat32,
    AbsFloat64,
    NegFloat32,
    NegFloat64,
    SqrtFloat32,
    SqrtFloat64,
    CeilFloat32,
    CeilFloat64,
    FloorFloat32,
    FloorFloat64,
    RoundFloat32,
    RoundFloat64,

    // Integer functions.
    CastStringToInt32,
    CastStringToInt64,
    CastInt32ToBool,
    CastInt32ToFloat32,
    CastInt32ToFloat64,
    CastInt32ToInt64,
    CastInt32ToString,
    CastInt32ToDecimal,
    CastInt64ToBool,
    CastInt64ToDecimal,
    CastInt64ToFloat32,
    CastInt64ToFloat64,
    CastInt64ToInt32,
    CastInt64ToString,
    AbsInt32,
    AbsInt64,
    NegInt32,
    NegInt64,

    // Interval functions.
    CastIntervalToString,
    CastStringToInterval,
    CastTimeToInterval,
    CastIntervalToTime,
    NegInterval,
    DatePartInterval(DateTimeUnits),

    // JSONB functions.
    CastStringToJsonb,
    CastJsonbToString,
    CastJsonbOrNullToJsonb,
    CastJsonbToFloat64,
    CastJsonbToBool,
    JsonbPretty,
    JsonbTypeof,
    JsonbArrayLength,
    JsonbStripNulls,

    // Null-handling functions.
    IsNull,

    // Record functions.
    RecordGet(usize),

    // String functions.
    CastBytesToString,
    CastStringToBytes,
    BitLengthBytes,
    BitLengthString,
    ByteLengthBytes,
    ByteLengthString,
    CharLength,
    Ascii,
    TrimWhitespace,
    TrimLeadingWhitespace,
    TrimTrailingWhitespace,
    MatchRegex(Regex),
}

impl UnaryFunc {
    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a ScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        use UnaryFunc::*;

        let a = a.eval(datums, temp_storage)?;
        if self.props().propagates_nulls() && a.is_null() {
            return Ok(Datum::Null);
        }

        let out = match self {
            // Boolean functions.
            CastBoolToStringExplicit => bool::cast_bool_to_string_explicit(a),
            CastBoolToStringImplicit => bool::cast_bool_to_string_implicit(a),
            CastStringToBool => bool::cast_string_to_bool(a),
            Not => bool::not(a),

            // Date and time functions.
            CastDateToString => datetime::cast_date_to_string(a, temp_storage),
            CastTimeToString => datetime::cast_time_to_string(a, temp_storage),
            CastTimestampToString => datetime::cast_timestamp_to_string(a, temp_storage),
            CastTimestampTzToString => datetime::cast_timestamptz_to_string(a, temp_storage),
            CastStringToDate => datetime::cast_string_to_date(a),
            CastStringToTime => datetime::cast_string_to_time(a),
            CastStringToTimestamp => datetime::cast_string_to_timestamp(a),
            CastStringToTimestampTz => datetime::cast_string_to_timestamptz(a),
            CastDateToTimestamp => datetime::cast_date_to_timestamp(a),
            CastDateToTimestampTz => datetime::cast_date_to_timestamptz(a),
            CastTimestampToDate => datetime::cast_timestamp_to_date(a),
            CastTimestampTzToDate => datetime::cast_timestamptz_to_date(a),
            CastTimestampToTimestampTz => datetime::cast_timestamp_to_timestamptz(a),
            CastTimestampTzToTimestamp => datetime::cast_timestamptz_to_timestamp(a),
            ToTimestamp => datetime::to_timestamp(a),
            DatePartTimestamp(units) => datetime::date_part_inner(*units, a.unwrap_timestamp()),
            DatePartTimestampTz(units) => datetime::date_part_inner(*units, a.unwrap_timestamptz()),
            DateTruncTimestamp(units) => datetime::date_trunc_inner(*units, a.unwrap_timestamp()),
            DateTruncTimestampTz(units) => {
                datetime::date_trunc_inner(*units, a.unwrap_timestamptz())
            }

            // Decimal functions.
            CastDecimalToString(scale) => decimal::cast_decimal_to_string(a, *scale, temp_storage),
            CastStringToDecimal(scale) => decimal::cast_string_to_decimal(a, *scale),
            CastDecimalToInt32 => decimal::cast_decimal_to_int32(a),
            CastDecimalToInt64 => decimal::cast_decimal_to_int64(a),
            CastDecimalToFloat32 => decimal::cast_decimal_to_float32(a),
            CastDecimalToFloat64 => decimal::cast_decimal_to_float64(a),
            AbsDecimal => decimal::abs_decimal(a),
            NegDecimal => decimal::neg_decimal(a),
            SqrtDecimal(scale) => decimal::sqrt_decimal(a, *scale),
            CeilDecimal(scale) => decimal::ceil_decimal(a, *scale),
            FloorDecimal(scale) => decimal::floor_decimal(a, *scale),
            RoundDecimal(scale) => decimal::round_decimal_unary(a, *scale),

            // Float functions.
            CastStringToFloat32 => float::cast_string_to_float32(a),
            CastStringToFloat64 => float::cast_string_to_float64(a),
            CastFloat32ToInt64 => float::cast_float32_to_int64(a),
            CastFloat32ToFloat64 => float::cast_float32_to_float64(a),
            CastFloat32ToString => float::cast_float32_to_string(a, temp_storage),
            CastFloat64ToInt32 => float::cast_float64_to_int32(a),
            CastFloat64ToInt64 => float::cast_float64_to_int64(a),
            CastFloat64ToString => float::cast_float64_to_string(a, temp_storage),
            CastFloat32ToDecimal(s) => float::cast_float32_to_decimal(a, *s),
            CastFloat64ToDecimal(s) => float::cast_float64_to_decimal(a, *s),
            AbsFloat32 => float::abs_float32(a),
            AbsFloat64 => float::abs_float64(a),
            NegFloat32 => float::neg_float32(a),
            NegFloat64 => float::neg_float64(a),
            CeilFloat32 => float::ceil_float32(a),
            CeilFloat64 => float::ceil_float64(a),
            FloorFloat32 => float::floor_float32(a),
            FloorFloat64 => float::floor_float64(a),
            SqrtFloat32 => float::sqrt_float32(a),
            SqrtFloat64 => float::sqrt_float64(a),
            RoundFloat32 => float::round_float32(a),
            RoundFloat64 => float::round_float64(a),

            // Integer functions.
            CastInt32ToBool => integer::cast_int32_to_bool(a),
            CastInt32ToFloat32 => integer::cast_int32_to_float32(a),
            CastInt32ToFloat64 => integer::cast_int32_to_float64(a),
            CastInt32ToInt64 => integer::cast_int32_to_int64(a),
            CastInt32ToDecimal => integer::cast_int32_to_decimal(a),
            CastInt32ToString => integer::cast_int32_to_string(a, temp_storage),
            CastInt64ToInt32 => integer::cast_int64_to_int32(a),
            CastInt64ToBool => integer::cast_int64_to_bool(a),
            CastInt64ToDecimal => integer::cast_int64_to_decimal(a),
            CastInt64ToFloat32 => integer::cast_int64_to_float32(a),
            CastInt64ToFloat64 => integer::cast_int64_to_float64(a),
            CastInt64ToString => integer::cast_int64_to_string(a, temp_storage),
            CastStringToInt32 => integer::cast_string_to_int32(a),
            CastStringToInt64 => integer::cast_string_to_int64(a),
            AbsInt32 => integer::abs_int32(a),
            AbsInt64 => integer::abs_int64(a),
            NegInt32 => integer::neg_int32(a),
            NegInt64 => integer::neg_int64(a),

            // Interval functions.
            CastStringToInterval => interval::cast_string_to_interval(a),
            CastTimeToInterval => interval::cast_time_to_interval(a),
            CastIntervalToString => interval::cast_interval_to_string(a, temp_storage),
            CastIntervalToTime => interval::cast_interval_to_time(a),
            NegInterval => interval::neg_interval(a),
            DatePartInterval(units) => interval::date_part_inner(*units, a.unwrap_interval()),

            // JSONB functions.
            CastJsonbToString => jsonb::cast_jsonb_to_string(a, temp_storage),
            CastStringToJsonb => jsonb::cast_string_to_jsonb(a, temp_storage),
            CastJsonbOrNullToJsonb => jsonb::cast_jsonb_or_null_to_jsonb(a),
            CastJsonbToFloat64 => jsonb::cast_jsonb_to_float64(a),
            CastJsonbToBool => jsonb::cast_jsonb_to_bool(a),
            JsonbPretty => jsonb::jsonb_pretty(a, temp_storage),
            JsonbArrayLength => jsonb::jsonb_array_length(a),
            JsonbStripNulls => jsonb::jsonb_strip_nulls(a, temp_storage),
            JsonbTypeof => jsonb::jsonb_typeof(a),

            // Null-handling functions.
            IsNull => null::is_null(a),

            // Record functions.
            RecordGet(i) => record::record_get(a, *i),

            // String functions.
            CastBytesToString => string::cast_bytes_to_string(a, temp_storage),
            CastStringToBytes => string::cast_string_to_bytes(a, temp_storage),
            BitLengthString => string::bit_length(a.unwrap_str()),
            BitLengthBytes => string::bit_length(a.unwrap_bytes()),
            ByteLengthString => string::byte_length(a.unwrap_str()),
            ByteLengthBytes => string::byte_length(a.unwrap_bytes()),
            CharLength => string::char_length(a),
            Ascii => string::ascii(a),
            TrimWhitespace => string::trim_whitespace(a),
            TrimLeadingWhitespace => string::trim_leading_whitespace(a),
            TrimTrailingWhitespace => string::trim_trailing_whitespace(a),
            MatchRegex(regex) => string::match_regex(a, &regex),
        };

        #[cfg(debug_assertions)]
        self.props()
            .check(&[a], &out)
            .unwrap_or_else(|e| panic!("{} failed property validation: {}", self, e));

        out
    }

    pub fn props(&self) -> FuncProps {
        use UnaryFunc::*;

        match self {
            // Boolean functions.
            CastBoolToStringImplicit | CastBoolToStringExplicit => bool::CAST_BOOL_TO_STRING_PROPS,
            CastStringToBool => bool::CAST_STRING_TO_BOOL_PROPS,
            Not => bool::NOT_PROPS,

            // Date and time functions.
            CastDateToString
            | CastTimeToString
            | CastTimestampToString
            | CastTimestampTzToString => datetime::CAST_TO_STRING_PROPS,
            CastStringToDate => datetime::CAST_STRING_TO_DATE_PROPS,
            CastStringToTime => datetime::CAST_STRING_TO_TIME_PROPS,
            CastStringToTimestamp => datetime::CAST_STRING_TO_TIMESTAMP_PROPS,
            CastStringToTimestampTz => datetime::CAST_STRING_TO_TIMESTAMPTZ_PROPS,
            CastDateToTimestamp => datetime::CAST_DATE_TO_TIMESTAMP_PROPS,
            CastDateToTimestampTz => datetime::CAST_DATE_TO_TIMESTAMPTZ_PROPS,
            CastTimestampToDate | CastTimestampTzToDate => datetime::CAST_TIMESTAMP_TO_DATE_PROPS,
            CastTimestampToTimestampTz => datetime::CAST_TIMESTAMP_TO_TIMESTAMPTZ_PROPS,
            CastTimestampTzToTimestamp => datetime::CAST_TIMESTAMPTZ_TO_TIMESTAMP_PROPS,
            ToTimestamp => datetime::TO_TIMESTAMP_PROPS,
            DatePartInterval(_) | DatePartTimestamp(_) | DatePartTimestampTz(_) => {
                datetime::DATE_PART_PROPS
            }
            DateTruncTimestamp(_) => datetime::DATE_TRUNC_TIMESTAMP_PROPS,
            DateTruncTimestampTz(_) => datetime::DATE_TRUNC_TIMESTAMPTZ_PROPS,

            // Decimal functions.
            CastDecimalToString(_) => decimal::CAST_DECIMAL_TO_STRING_PROPS,
            CastStringToDecimal(s) => decimal::cast_string_to_decimal_props(*s),
            CastDecimalToInt32 => decimal::CAST_DECIMAL_TO_INT32_PROPS,
            CastDecimalToInt64 => decimal::CAST_DECIMAL_TO_INT64_PROPS,
            CastDecimalToFloat32 => decimal::CAST_DECIMAL_TO_FLOAT32_PROPS,
            CastDecimalToFloat64 => decimal::CAST_DECIMAL_TO_FLOAT64_PROPS,
            AbsDecimal | NegDecimal | CeilDecimal(_) | FloorDecimal(_) => {
                decimal::DECIMAL_MATH_PROPS
            }
            RoundDecimal(_) => decimal::ROUND_DECIMAL_PROPS,
            SqrtDecimal(_) => decimal::SQRT_DECIMAL_PROPS,

            // Float functions.
            CastStringToFloat32 => float::CAST_STRING_TO_FLOAT32_PROPS,
            CastStringToFloat64 => float::CAST_STRING_TO_FLOAT64_PROPS,
            CastFloat32ToInt64 | CastFloat64ToInt64 => float::CAST_FLOAT_TO_INT64_PROPS,
            CastFloat32ToFloat64 => float::CAST_FLOAT32_TO_FLOAT64_PROPS,
            CastFloat32ToString | CastFloat64ToString => float::CAST_FLOAT_TO_STRING_PROPS,
            CastFloat64ToInt32 => float::CAST_FLOAT_TO_INT32_PROPS,
            CastFloat32ToDecimal(s) | CastFloat64ToDecimal(s) => {
                float::cast_float_to_decimal_props(*s)
            }
            AbsFloat32 | NegFloat32 | CeilFloat32 | FloorFloat32 | RoundFloat32 | SqrtFloat32 => {
                float::FLOAT32_MATH_PROPS
            }
            AbsFloat64 | NegFloat64 | CeilFloat64 | FloorFloat64 | RoundFloat64 | SqrtFloat64 => {
                float::FLOAT64_MATH_PROPS
            }

            // Integer functions.
            CastStringToInt32 => integer::CAST_STRING_TO_INT32_PROPS,
            CastStringToInt64 => integer::CAST_STRING_TO_INT64_PROPS,
            CastInt32ToBool | CastInt64ToBool => integer::CAST_INT_TO_BOOL_PROPS,
            CastInt32ToFloat32 | CastInt64ToFloat32 => integer::CAST_INT_TO_FLOAT32_PROPS,
            CastInt32ToFloat64 | CastInt64ToFloat64 => integer::CAST_INT_TO_FLOAT64_PROPS,
            CastInt32ToString | CastInt64ToString => integer::CAST_INT_TO_STRING_PROPS,
            CastInt32ToInt64 => integer::CAST_INT32_TO_INT64_PROPS,
            CastInt32ToDecimal => integer::CAST_INT32_TO_DECIMAL_PROPS,
            CastInt64ToInt32 => integer::CAST_INT64_TO_INT32_PROPS,
            CastInt64ToDecimal => integer::CAST_INT64_TO_DECIMAL_PROPS,
            AbsInt32 | NegInt32 => integer::INT32_MATH_PROPS,
            AbsInt64 | NegInt64 => integer::INT64_MATH_PROPS,

            // Interval functions.
            CastIntervalToString => interval::CAST_INTERVAL_TO_STRING_PROPS,
            CastStringToInterval => interval::CAST_STRING_TO_INTERVAL_PROPS,
            CastTimeToInterval => interval::CAST_TIME_TO_INTERVAL_PROPS,
            CastIntervalToTime => interval::CAST_INTERVAL_TO_TIME_PROPS,
            NegInterval => interval::INTERVAL_MATH_PROPS,

            // JSONB functions.
            CastJsonbToString => jsonb::CAST_JSONB_TO_STRING_PROPS,
            CastStringToJsonb => jsonb::CAST_STRING_TO_JSONB_PROPS,
            CastJsonbOrNullToJsonb => jsonb::CAST_JSONB_OR_NULL_TO_JSONB_PROPS,
            CastJsonbToFloat64 => jsonb::CAST_JSONB_TO_FLOAT64_PROPS,
            CastJsonbToBool => jsonb::CAST_JSONB_TO_BOOL_PROPS,
            JsonbPretty => jsonb::JSONB_PRETTY_PROPS,
            JsonbArrayLength => jsonb::JSONB_ARRAY_LENGTH_PROPS,
            JsonbStripNulls => jsonb::JSONB_STRIP_NULLS_PROPS,
            JsonbTypeof => jsonb::JSONB_TYPEOF_PROPS,

            // Null-handling functions.
            IsNull => null::IS_NULL_PROPS,

            // Record functions.
            RecordGet(i) => record::record_get_props(*i),

            // String functions.
            CastBytesToString => string::CAST_BYTES_TO_STRING_PROPS,
            CastStringToBytes => string::CAST_STRING_TO_BYTES_PROPS,
            TrimWhitespace | TrimLeadingWhitespace | TrimTrailingWhitespace => string::TRIM_PROPS,
            Ascii => string::ASCII_PROPS,
            CharLength | BitLengthBytes | BitLengthString | ByteLengthBytes | ByteLengthString => {
                string::LENGTH_PROPS
            }
            MatchRegex(_) => string::MATCH_REGEX_PROPS,
        }
    }

    pub fn output_type(&self, input_type: ColumnType) -> ColumnType {
        self.props().output_type(vec![input_type])
    }
}

impl fmt::Display for UnaryFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use UnaryFunc::*;

        match self {
            // Boolean functions.
            CastBoolToStringExplicit => f.write_str("booltostrex"),
            CastBoolToStringImplicit => f.write_str("booltostrim"),
            CastStringToBool => f.write_str("strtobool"),
            Not => f.write_str("!"),

            // Date and time functions.
            CastDateToString => f.write_str("datetostr"),
            CastTimeToString => f.write_str("timetostr"),
            CastTimestampToString => f.write_str("tstostr"),
            CastTimestampTzToString => f.write_str("tstztostr"),
            CastStringToDate => f.write_str("strtodate"),
            CastStringToTime => f.write_str("strtotime"),
            CastStringToTimestamp => f.write_str("strtots"),
            CastStringToTimestampTz => f.write_str("strtotstz"),
            CastDateToTimestamp => f.write_str("datetots"),
            CastDateToTimestampTz => f.write_str("datetotstz"),
            CastTimestampToDate => f.write_str("tstodate"),
            CastTimestampToTimestampTz => f.write_str("tstotstz"),
            CastTimestampTzToDate => f.write_str("tstodate"),
            CastTimestampTzToTimestamp => f.write_str("tstztots"),
            ToTimestamp => f.write_str("tots"),
            DatePartInterval(units) => write!(f, "date_part_{}_iv", units),
            DatePartTimestamp(units) => write!(f, "date_part_{}_ts", units),
            DatePartTimestampTz(units) => write!(f, "date_part_{}_tstz", units),
            DateTruncTimestamp(units) => write!(f, "date_trunc_{}_ts", units),
            DateTruncTimestampTz(units) => write!(f, "date_trunc_{}_tstz", units),

            // Decimal functions.
            CastStringToDecimal(_) => f.write_str("strtodec"),
            CastDecimalToString(_) => f.write_str("dectostr"),
            CastDecimalToInt32 => f.write_str("dectoi32"),
            CastDecimalToInt64 => f.write_str("dectoi64"),
            CastDecimalToFloat32 => f.write_str("dectof32"),
            CastDecimalToFloat64 => f.write_str("dectof64"),
            AbsDecimal => f.write_str("abs"),
            NegDecimal => f.write_str("-"),
            SqrtDecimal(_) => f.write_str("sqrtdec"),
            CeilDecimal(_) => f.write_str("ceildec"),
            FloorDecimal(_) => f.write_str("floordec"),
            RoundDecimal(_) => f.write_str("roundunary"),

            // Float functions.
            CastStringToFloat32 => f.write_str("strtof32"),
            CastStringToFloat64 => f.write_str("strtof64"),
            CastFloat32ToInt64 => f.write_str("f32toi64"),
            CastFloat32ToFloat64 => f.write_str("f32tof64"),
            CastFloat32ToString => f.write_str("f32tostr"),
            CastFloat64ToInt32 => f.write_str("f64toi32"),
            CastFloat64ToInt64 => f.write_str("f64toi64"),
            CastFloat64ToString => f.write_str("f64tostr"),
            CastFloat32ToDecimal(_) => f.write_str("f32todec"),
            CastFloat64ToDecimal(_) => f.write_str("f64todec"),
            AbsFloat32 => f.write_str("abs"),
            AbsFloat64 => f.write_str("abs"),
            NegFloat32 => f.write_str("-"),
            NegFloat64 => f.write_str("-"),
            SqrtFloat32 => f.write_str("sqrtf32"),
            SqrtFloat64 => f.write_str("sqrtf64"),
            CeilFloat32 => f.write_str("ceilf32"),
            CeilFloat64 => f.write_str("ceilf64"),
            FloorFloat32 => f.write_str("floorf32"),
            FloorFloat64 => f.write_str("floorf64"),
            RoundFloat32 => f.write_str("roundf32"),
            RoundFloat64 => f.write_str("roundf64"),

            // Integer functions.
            CastStringToInt32 => f.write_str("strtoi32"),
            CastStringToInt64 => f.write_str("strtoi64"),
            CastInt32ToBool => f.write_str("i32tobool"),
            CastInt32ToFloat32 => f.write_str("i32tof32"),
            CastInt32ToFloat64 => f.write_str("i32tof64"),
            CastInt32ToInt64 => f.write_str("i32toi64"),
            CastInt32ToString => f.write_str("i32tostr"),
            CastInt32ToDecimal => f.write_str("i32todec"),
            CastInt64ToInt32 => f.write_str("i64toi32"),
            CastInt64ToBool => f.write_str("i64tobool"),
            CastInt64ToDecimal => f.write_str("i64todec"),
            CastInt64ToFloat32 => f.write_str("i64tof32"),
            CastInt64ToFloat64 => f.write_str("i64tof64"),
            CastInt64ToString => f.write_str("i64tostr"),
            AbsInt32 => f.write_str("abs"),
            AbsInt64 => f.write_str("abs"),
            NegInt32 => f.write_str("-"),
            NegInt64 => f.write_str("-"),

            // Interval functions.
            CastIntervalToString => f.write_str("ivtostr"),
            CastStringToInterval => f.write_str("strtoiv"),
            CastTimeToInterval => f.write_str("timetoiv"),
            CastIntervalToTime => f.write_str("ivtotime"),
            NegInterval => f.write_str("-"),

            // JSONB functions.
            CastJsonbToString => f.write_str("jsonbtostr"),
            CastStringToJsonb => f.write_str("strtojsonb"),
            CastJsonbOrNullToJsonb => f.write_str("jsonb?tojsonb"),
            CastJsonbToFloat64 => f.write_str("jsonbtof64"),
            CastJsonbToBool => f.write_str("jsonbtobool"),
            JsonbPretty => f.write_str("jsonb_pretty"),
            JsonbTypeof => f.write_str("jsonb_typeof"),
            JsonbArrayLength => f.write_str("jsonb_array_length"),
            JsonbStripNulls => f.write_str("jsonb_strip_nulls"),

            // Null-handling functions.
            IsNull => f.write_str("isnull"),

            // Record functions.
            RecordGet(i) => write!(f, "record_get_{}", i),

            // String functions.
            CastBytesToString => f.write_str("bytestostr"),
            CastStringToBytes => f.write_str("strtobytes"),
            TrimWhitespace => f.write_str("btrim"),
            TrimLeadingWhitespace => f.write_str("ltrim"),
            TrimTrailingWhitespace => f.write_str("rtrim"),
            Ascii => f.write_str("ascii"),
            CharLength => f.write_str("char_length"),
            BitLengthBytes => f.write_str("bit_length"),
            BitLengthString => f.write_str("bit_length"),
            ByteLengthBytes => f.write_str("byte_length"),
            ByteLengthString => f.write_str("byte_length"),
            MatchRegex(regex) => write!(f, "\"{}\" ~", regex.as_str()),
        }
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum BinaryFunc {
    // Boolean functions.
    And,
    Or,

    // Comparison functions.
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,

    // Date and time functions.
    DatePartTimestamp,
    DatePartTimestampTz,
    DateTruncTimestamp,
    DateTruncTimestampTz,
    AddDateTime,

    // Decimal functions.
    AddDecimal,
    SubDecimal,
    ModDecimal,
    MulDecimal,
    DivDecimal,
    RoundDecimal(u8),

    // Float functions.
    AddFloat32,
    AddFloat64,
    SubFloat32,
    SubFloat64,
    MulFloat32,
    MulFloat64,
    DivFloat32,
    DivFloat64,
    ModFloat32,
    ModFloat64,

    // Formatting functions.
    ToCharTimestamp,
    ToCharTimestampTz,

    // Integer functions.
    AddInt32,
    AddInt64,
    SubInt32,
    SubInt64,
    MulInt32,
    MulInt64,
    DivInt32,
    DivInt64,
    ModInt32,
    ModInt64,

    // Interval functions.
    AddInterval,
    AddDateInterval,
    AddTimeInterval,
    AddTimestampInterval,
    AddTimestampTzInterval,
    SubInterval,
    SubDateInterval,
    SubTimeInterval,
    SubTimestampTzInterval,
    SubTimestampInterval,
    SubDate,
    SubTime,
    SubTimestamp,
    SubTimestampTz,
    DatePartInterval,

    // JSONB functions.
    JsonbGetInt64 { stringify: bool },
    JsonbGetString { stringify: bool },
    JsonbDeleteInt64,
    JsonbDeleteString,
    JsonbContainsString,
    JsonbContainsJsonb,
    JsonbConcat,

    // String functions.
    ConvertFrom,
    EncodedBytesCharLength,
    StringConcat,
    Trim,
    TrimLeading,
    TrimTrailing,
    MatchLikePattern,
}

impl BinaryFunc {
    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a_expr: &'a ScalarExpr,
        b_expr: &'a ScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        use BinaryFunc::*;

        macro_rules! eager {
            ($func:expr $(, $args:expr)*) => {{
                let a = a_expr.eval(datums, temp_storage)?;
                let b = b_expr.eval(datums, temp_storage)?;
                if self.props().propagates_nulls() && (a.is_null() || b.is_null()) {
                    return Ok(Datum::Null);
                }
                let out = $func(a, b $(, $args)*);
                #[cfg(debug_assertions)]
                self.props()
                    .check(&[a, b], &out)
                    .unwrap_or_else(|e| panic!("{} failed property validation: {}", self, e));
                out
            }}
        }

        match self {
            // Boolean functions.
            And => bool::and(datums, temp_storage, a_expr, b_expr),
            Or => bool::or(datums, temp_storage, a_expr, b_expr),

            // Comparison functions.
            Eq => eager!(cmp::eq),
            NotEq => eager!(cmp::not_eq),
            Lt => eager!(cmp::lt),
            Lte => eager!(cmp::lte),
            Gt => eager!(cmp::gt),
            Gte => eager!(cmp::gte),

            // Date and time functions.
            DatePartTimestamp => eager!(|a, b: Datum| datetime::date_part(a, b.unwrap_timestamp())),
            DatePartTimestampTz => {
                eager!(|a, b: Datum| datetime::date_part(a, b.unwrap_timestamptz()))
            }
            DateTruncTimestamp => {
                eager!(|a, b: Datum| datetime::date_trunc(a, b.unwrap_timestamp()))
            }
            DateTruncTimestampTz => {
                eager!(|a, b: Datum| datetime::date_trunc(a, b.unwrap_timestamptz()))
            }
            AddDateTime => eager!(datetime::add_date_time),

            // Decimal functions.
            AddDecimal => eager!(decimal::add_decimal),
            SubDecimal => eager!(decimal::sub_decimal),
            MulDecimal => eager!(decimal::mul_decimal),
            DivDecimal => eager!(decimal::div_decimal),
            ModDecimal => eager!(decimal::mod_decimal),
            RoundDecimal(scale) => eager!(decimal::round_decimal_binary, *scale),

            // Float functions.
            AddFloat32 => eager!(float::add_float32),
            AddFloat64 => eager!(float::add_float64),
            SubFloat32 => eager!(float::sub_float32),
            SubFloat64 => eager!(float::sub_float64),
            MulFloat32 => eager!(float::mul_float32),
            MulFloat64 => eager!(float::mul_float64),
            DivFloat32 => eager!(float::div_float32),
            DivFloat64 => eager!(float::div_float64),
            ModFloat32 => eager!(float::mod_float32),
            ModFloat64 => eager!(float::mod_float64),

            // Formatting functions.
            ToCharTimestamp => eager!(format::to_char_timestamp, temp_storage),
            ToCharTimestampTz => eager!(format::to_char_timestamptz, temp_storage),

            // Integer functions.
            AddInt32 => eager!(integer::add_int32),
            AddInt64 => eager!(integer::add_int64),
            SubInt32 => eager!(integer::sub_int32),
            SubInt64 => eager!(integer::sub_int64),
            MulInt32 => eager!(integer::mul_int32),
            MulInt64 => eager!(integer::mul_int64),
            DivInt32 => eager!(integer::div_int32),
            DivInt64 => eager!(integer::div_int64),
            ModInt32 => eager!(integer::mod_int32),
            ModInt64 => eager!(integer::mod_int64),

            // Interval functions.
            AddInterval => eager!(interval::add_interval),
            AddDateInterval => eager!(interval::add_date_interval),
            AddTimeInterval => eager!(interval::add_time_interval),
            AddTimestampInterval => eager!(interval::add_timestamp_interval),
            AddTimestampTzInterval => eager!(interval::add_timestamptz_interval),
            SubInterval => eager!(interval::sub_interval),
            SubDateInterval => eager!(interval::sub_date_interval),
            SubTimeInterval => eager!(interval::sub_time_interval),
            SubTimestampInterval => eager!(interval::sub_timestamp_interval),
            SubTimestampTzInterval => eager!(interval::sub_timestamptz_interval),
            SubDate => eager!(interval::sub_date),
            SubTime => eager!(interval::sub_time),
            SubTimestamp => eager!(interval::sub_timestamp),
            SubTimestampTz => eager!(interval::sub_timestamptz),
            DatePartInterval => eager!(|a, b: Datum| interval::date_part(a, b.unwrap_interval())),

            // JSONB functions.
            JsonbGetInt64 { stringify } => eager!(jsonb::jsonb_get_int64, temp_storage, *stringify),
            JsonbGetString { stringify } => {
                eager!(jsonb::jsonb_get_string, temp_storage, *stringify)
            }
            JsonbDeleteInt64 => eager!(jsonb::jsonb_delete_int64, temp_storage),
            JsonbDeleteString => eager!(jsonb::jsonb_delete_string, temp_storage),
            JsonbContainsString => eager!(jsonb::jsonb_contains_string),
            JsonbContainsJsonb => eager!(jsonb::jsonb_contains_jsonb),
            JsonbConcat => eager!(jsonb::jsonb_concat, temp_storage),

            // String functions.
            ConvertFrom => eager!(string::convert_from),
            EncodedBytesCharLength => eager!(string::encoded_bytes_char_length),
            StringConcat => eager!(string::concat_binary, temp_storage),
            Trim => eager!(string::trim),
            TrimLeading => eager!(string::trim_leading),
            TrimTrailing => eager!(string::trim_trailing),
            MatchLikePattern => eager!(string::match_like_pattern),
        }
    }

    pub fn props(&self) -> FuncProps {
        use BinaryFunc::*;

        match self {
            // Boolean functions.
            And => bool::AND_PROPS,
            Or => bool::OR_PROPS,

            // Comparison functions.
            Eq | NotEq | Lt | Lte | Gt | Gte => cmp::CMP_PROPS,

            // Date and time functions.
            DatePartTimestamp | DatePartTimestampTz => datetime::DATE_PART_PROPS,
            DateTruncTimestamp => datetime::DATE_TRUNC_TIMESTAMP_PROPS,
            DateTruncTimestampTz => datetime::DATE_TRUNC_TIMESTAMPTZ_PROPS,
            AddDateTime => datetime::ADD_DATE_TIME_PROPS,

            // Decimal functions.
            AddDecimal | SubDecimal | ModDecimal => decimal::DECIMAL_MATH_PROPS,
            MulDecimal => decimal::MUL_DECIMAL_PROPS,
            DivDecimal => decimal::DIV_DECIMAL_PROPS,
            RoundDecimal(_) => decimal::ROUND_DECIMAL_PROPS,

            // Float functions.
            AddFloat32 | SubFloat32 | MulFloat32 | DivFloat32 | ModFloat32 => {
                float::FLOAT32_MATH_PROPS
            }
            AddFloat64 | SubFloat64 | MulFloat64 | DivFloat64 | ModFloat64 => {
                float::FLOAT64_MATH_PROPS
            }

            // Formatting functions.
            ToCharTimestamp | ToCharTimestampTz => format::TO_CHAR_PROPS,

            // Integer functions.
            AddInt32 | SubInt32 | MulInt32 | DivInt32 | ModInt32 => integer::INT32_MATH_PROPS,
            AddInt64 | SubInt64 | MulInt64 | DivInt64 | ModInt64 => integer::INT64_MATH_PROPS,

            // Interval functions.
            AddInterval => interval::INTERVAL_MATH_PROPS,
            AddDateInterval | SubDateInterval => interval::INTERVAL_DATE_MATH_PROPS,
            AddTimeInterval | SubTimeInterval => interval::INTERVAL_TIME_MATH_PROPS,
            AddTimestampInterval | SubTimestampInterval => interval::INTERVAL_TIMESTAMP_MATH_PROPS,
            AddTimestampTzInterval | SubTimestampTzInterval => {
                interval::INTERVAL_TIMESTAMPTZ_MATH_PROPS
            }
            SubInterval => interval::INTERVAL_MATH_PROPS,
            SubDate => interval::INTERVAL_MATH_PROPS,
            SubTime => interval::INTERVAL_MATH_PROPS,
            SubTimestamp => interval::INTERVAL_MATH_PROPS,
            SubTimestampTz => interval::INTERVAL_MATH_PROPS,
            DatePartInterval => interval::DATE_PART_PROPS,

            // JSONB functions.
            JsonbGetInt64 { stringify } | JsonbGetString { stringify } => {
                jsonb::jsonb_get_props(*stringify)
            }
            JsonbDeleteInt64 | JsonbDeleteString => jsonb::JSONB_DELETE_PROPS,
            JsonbContainsString => jsonb::JSONB_CONTAINS_STRING_PROPS,
            JsonbContainsJsonb => jsonb::JSONB_CONTAINS_JSONB_PROPS,
            JsonbConcat => jsonb::JSONB_CONCAT_PROPS,

            // String functions.
            ConvertFrom => string::CONVERT_FROM_PROPS,
            StringConcat => string::CONCAT_BINARY_PROPS,
            EncodedBytesCharLength => string::LENGTH_PROPS,
            Trim | TrimLeading | TrimTrailing => string::TRIM_PROPS,
            MatchLikePattern => string::MATCH_LIKE_PATTERN_PROPS,
        }
    }

    pub fn output_type(&self, input1_type: ColumnType, input2_type: ColumnType) -> ColumnType {
        self.props().output_type(vec![input1_type, input2_type])
    }

    pub fn is_infix_op(&self) -> bool {
        let (_symbol, affixment) = self.display();
        affixment == Affixment::Infix
    }

    fn display(&self) -> (&'static str, Affixment) {
        use Affixment::*;
        use BinaryFunc::*;

        match self {
            // Boolean functions.
            And => ("&&", Infix),
            Or => ("||", Infix),

            // Comparison functions.
            Eq => ("=", Infix),
            NotEq => ("!=", Infix),
            Lt => ("<", Infix),
            Lte => ("<=", Infix),
            Gt => (">", Infix),
            Gte => (">=", Infix),

            // Date and time functions.
            DatePartTimestamp => ("date_partts", Prefix),
            DatePartTimestampTz => ("date_parttstz", Prefix),
            DateTruncTimestamp => ("date_truncts", Prefix),
            DateTruncTimestampTz => ("date_trunctstz", Prefix),
            AddDateTime => ("+", Infix),

            // Decimal functions.
            AddDecimal => ("+", Infix),
            SubDecimal => ("-", Infix),
            ModDecimal => ("%", Infix),
            MulDecimal => ("*", Infix),
            DivDecimal => ("/", Infix),
            RoundDecimal(_) => ("round", Prefix),

            // Float functions.
            AddFloat32 => ("+", Infix),
            AddFloat64 => ("+", Infix),
            SubFloat32 => ("-", Infix),
            SubFloat64 => ("-", Infix),
            MulFloat32 => ("*", Infix),
            MulFloat64 => ("*", Infix),
            DivFloat32 => ("/", Infix),
            DivFloat64 => ("/", Infix),
            ModFloat32 => ("%", Infix),
            ModFloat64 => ("%", Infix),

            // Formatting functions.
            ToCharTimestamp => ("tocharts", Prefix),
            ToCharTimestampTz => ("tochartstz", Prefix),

            // Integer functions.
            AddInt32 => ("+", Infix),
            AddInt64 => ("+", Infix),
            SubInt32 => ("-", Infix),
            SubInt64 => ("-", Infix),
            MulInt32 => ("*", Infix),
            MulInt64 => ("*", Infix),
            DivInt32 => ("/", Infix),
            DivInt64 => ("/", Infix),
            ModInt32 => ("%", Infix),
            ModInt64 => ("%", Infix),

            // Interval functions.
            AddInterval => ("+", Infix),
            AddDateInterval => ("+", Infix),
            AddTimeInterval => ("+", Infix),
            AddTimestampInterval => ("+", Infix),
            AddTimestampTzInterval => ("+", Infix),
            SubInterval => ("-", Infix),
            SubDateInterval => ("-", Infix),
            SubTimeInterval => ("-", Infix),
            SubTimestampInterval => ("-", Infix),
            SubTimestampTzInterval => ("-", Infix),
            SubDate => ("-", Infix),
            SubTime => ("-", Infix),
            SubTimestamp => ("-", Infix),
            SubTimestampTz => ("-", Infix),
            DatePartInterval => ("date_partiv", Prefix),

            // JSONB functions.
            JsonbGetInt64 { .. } => ("->", Infix),
            JsonbGetString { .. } => ("->", Infix),
            JsonbDeleteInt64 => ("-", Infix),
            JsonbDeleteString => ("-", Infix),
            JsonbContainsString => ("?", Infix),
            JsonbContainsJsonb => ("@>", Infix),
            JsonbConcat => ("||", Infix),

            // String functions.
            StringConcat => ("||", Infix),
            ConvertFrom => ("convert_from", Prefix),
            EncodedBytesCharLength => ("length", Prefix),
            Trim => ("btrim", Prefix),
            TrimLeading => ("ltrim", Prefix),
            TrimTrailing => ("rtrim", Prefix),
            MatchLikePattern => ("like", Prefix),
        }
    }
}

impl fmt::Display for BinaryFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (symbol, _affixment) = self.display();
        f.write_str(symbol)
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum VariadicFunc {
    // Date and time functions.
    MakeTimestamp,

    // List functions.
    ListCreate {
        elem_type: ScalarType,
    },

    // JSONB functions.
    JsonbBuildArray,
    JsonbBuildObject,

    // Null-handling functions.
    Coalesce,

    // Record functions.
    RecordCreate {
        field_names: Vec<ColumnName>,
        oid: Option<u32>,
    },

    // String functions.
    Concat,
    Substr,
    Replace,
}

impl VariadicFunc {
    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [ScalarExpr],
    ) -> Result<Datum<'a>, EvalError> {
        use VariadicFunc::*;

        macro_rules! eager {
            ($func:expr $(, $args:expr)*) => {{
                let ds = exprs.iter()
                    .map(|e| e.eval(datums, temp_storage))
                    .collect::<Result<Vec<_>, _>>()?;
                if self.props().propagates_nulls() && ds.iter().any(|d| d.is_null()) {
                    return Ok(Datum::Null);
                }
                let out = $func(&ds $(, $args)*);
                self.props()
                    .check(&ds, &out)
                    .unwrap_or_else(|e| panic!("{} failed property validation: {}", self, e));
                out
            }}
        }

        match self {
            // Date and time functions.
            MakeTimestamp => eager!(datetime::make_timestamp),

            // List functions.
            ListCreate { .. } => eager!(list::list_create, temp_storage),

            // JSONB functions.
            JsonbBuildArray => eager!(jsonb::jsonb_build_array, temp_storage),
            JsonbBuildObject => eager!(jsonb::jsonb_build_object, temp_storage),

            // Null-handling functions.
            Coalesce => null::coalesce(datums, temp_storage, exprs),

            // Record functions.
            RecordCreate { .. } => eager!(record::record_create, temp_storage),

            // String functions.
            Concat => eager!(string::concat_variadic, temp_storage),
            Substr => eager!(string::substr),
            Replace => eager!(string::replace, temp_storage),
        }
    }

    pub fn props(&self) -> FuncProps {
        use VariadicFunc::*;

        match self {
            // Date and time functions.
            MakeTimestamp => datetime::MAKE_TIMESTAMP_PROPS,

            // JSONB functions.
            JsonbBuildArray => jsonb::JSONB_BUILD_ARRAY_PROPS,
            JsonbBuildObject => jsonb::JSONB_BUILD_OBJECT_PROPS,

            // List functions.
            ListCreate { elem_type } => list::list_create_props(elem_type.clone()),

            // Null-handling functions.
            Coalesce => null::COALESCE_PROPS,

            // Record functions.
            RecordCreate { field_names, oid } => {
                record::record_create_props(field_names.clone(), *oid)
            }

            // String functions.
            Concat => string::CONCAT_VARIADIC_PROPS,
            Substr => string::SUBSTR_PROPS,
            Replace => string::REPLACE_PROPS,
        }
    }

    pub fn output_type(&self, input_types: Vec<ColumnType>) -> ColumnType {
        self.props().output_type(input_types)
    }
}

impl fmt::Display for VariadicFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use VariadicFunc::*;

        match self {
            // Date and time functions.
            MakeTimestamp => f.write_str("makets"),

            // List functions.
            ListCreate { .. } => f.write_str("list_create"),

            // JSONB functions.
            JsonbBuildArray => f.write_str("jsonb_build_array"),
            JsonbBuildObject => f.write_str("jsonb_build_object"),

            // Null-handling functions.
            Coalesce => f.write_str("coalesce"),

            // Record functions.
            RecordCreate { .. } => f.write_str("record_create"),

            // String functions.
            Concat => f.write_str("concat"),
            Substr => f.write_str("substr"),
            Replace => f.write_str("replace"),
        }
    }
}
