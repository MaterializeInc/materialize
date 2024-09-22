// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::fmt;
use std::sync::LazyLock;

use chrono::{DateTime, NaiveDateTime, NaiveTime, Utc};
use mz_lowertest::MzReflect;
use mz_ore::cast::CastFrom;
use mz_ore::result::ResultExt;
use mz_ore::str::StrExt;
use mz_repr::adt::char::{format_str_trim, Char};
use mz_repr::adt::date::Date;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::adt::numeric::{self, Numeric, NumericMaxScale};
use mz_repr::adt::pg_legacy_name::PgLegacyName;
use mz_repr::adt::regex::Regex;
use mz_repr::adt::system::{Oid, PgLegacyChar};
use mz_repr::adt::timestamp::{CheckedTimestamp, TimestampPrecision};
use mz_repr::adt::varchar::{VarChar, VarCharMaxLength};
use mz_repr::{strconv, ColumnType, Datum, RowArena, ScalarType};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::func::regexp_match_static;
use crate::scalar::func::{
    array_create_scalar, regexp_split_to_array_re, EagerUnaryFunc, LazyUnaryFunc,
};
use crate::{like_pattern, EvalError, MirScalarExpr, UnaryFunc};

sqlfunc!(
    #[sqlname = "text_to_boolean"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastBoolToString)]
    fn cast_string_to_bool<'a>(a: &'a str) -> Result<bool, EvalError> {
        strconv::parse_bool(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "text_to_\"char\""]
    #[preserves_uniqueness = true]
    #[inverse = to_unary!(super::CastPgLegacyCharToString)]
    fn cast_string_to_pg_legacy_char<'a>(a: &'a str) -> PgLegacyChar {
        PgLegacyChar(a.as_bytes().get(0).copied().unwrap_or(0))
    }
);

sqlfunc!(
    #[sqlname = "text_to_name"]
    #[preserves_uniqueness = true]
    fn cast_string_to_pg_legacy_name<'a>(a: &'a str) -> PgLegacyName<String> {
        PgLegacyName(strconv::parse_pg_legacy_name(a))
    }
);

sqlfunc!(
    #[sqlname = "text_to_bytea"]
    #[preserves_uniqueness = true]
    #[inverse = to_unary!(super::CastBytesToString)]
    fn cast_string_to_bytes<'a>(a: &'a str) -> Result<Vec<u8>, EvalError> {
        strconv::parse_bytes(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "text_to_smallint"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastInt16ToString)]
    fn cast_string_to_int16<'a>(a: &'a str) -> Result<i16, EvalError> {
        strconv::parse_int16(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "text_to_integer"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastInt32ToString)]
    fn cast_string_to_int32<'a>(a: &'a str) -> Result<i32, EvalError> {
        strconv::parse_int32(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "text_to_bigint"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastInt64ToString)]
    fn cast_string_to_int64<'a>(a: &'a str) -> Result<i64, EvalError> {
        strconv::parse_int64(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "text_to_real"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastFloat32ToString)]
    fn cast_string_to_float32<'a>(a: &'a str) -> Result<f32, EvalError> {
        strconv::parse_float32(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "text_to_double"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastFloat64ToString)]
    fn cast_string_to_float64<'a>(a: &'a str) -> Result<f64, EvalError> {
        strconv::parse_float64(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "text_to_oid"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastOidToString)]
    fn cast_string_to_oid<'a>(a: &'a str) -> Result<Oid, EvalError> {
        Ok(Oid(strconv::parse_oid(a)?))
    }
);

sqlfunc!(
    #[sqlname = "text_to_uint2"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastUint16ToString)]
    fn cast_string_to_uint16(a: &'a str) -> Result<u16, EvalError> {
        strconv::parse_uint16(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "text_to_uint4"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastUint32ToString)]
    fn cast_string_to_uint32(a: &'a str) -> Result<u32, EvalError> {
        strconv::parse_uint32(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "text_to_uint8"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastUint64ToString)]
    fn cast_string_to_uint64(a: &'a str) -> Result<u64, EvalError> {
        strconv::parse_uint64(a).err_into()
    }
);

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct CastStringToNumeric(pub Option<NumericMaxScale>);

impl<'a> EagerUnaryFunc<'a> for CastStringToNumeric {
    type Input = &'a str;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, a: &'a str) -> Result<Numeric, EvalError> {
        let mut d = strconv::parse_numeric(a)?;
        if let Some(scale) = self.0 {
            if numeric::rescale(&mut d.0, scale.into_u8()).is_err() {
                return Err(EvalError::NumericFieldOverflow);
            }
        }
        Ok(d.into_inner())
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Numeric { max_scale: self.0 }.nullable(input.nullable)
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        to_unary!(super::CastNumericToString)
    }
}

impl fmt::Display for CastStringToNumeric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("text_to_numeric")
    }
}

sqlfunc!(
    #[sqlname = "text_to_date"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastDateToString)]
    fn cast_string_to_date<'a>(a: &'a str) -> Result<Date, EvalError> {
        strconv::parse_date(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "text_to_time"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastTimeToString)]
    fn cast_string_to_time<'a>(a: &'a str) -> Result<NaiveTime, EvalError> {
        strconv::parse_time(a).err_into()
    }
);

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct CastStringToTimestamp(pub Option<TimestampPrecision>);

impl<'a> EagerUnaryFunc<'a> for CastStringToTimestamp {
    type Input = &'a str;
    type Output = Result<CheckedTimestamp<NaiveDateTime>, EvalError>;

    fn call(&self, a: &'a str) -> Result<CheckedTimestamp<NaiveDateTime>, EvalError> {
        let out = strconv::parse_timestamp(a)?;
        let updated = out.round_to_precision(self.0)?;
        Ok(updated)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Timestamp { precision: self.0 }.nullable(input.nullable)
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        to_unary!(super::CastTimestampToString)
    }
}

impl fmt::Display for CastStringToTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("text_to_timestamp")
    }
}

sqlfunc!(
    #[sqlname = "try_parse_monotonic_iso8601_timestamp"]
    // TODO: Pretty sure this preserves uniqueness, but not 100%.
    //
    // Ironically, even though this has "monotonic" in the name, it's not quite
    // eligible for `#[is_monotone = true]` because any input could also be
    // mapped to null. So, handle it via SpecialUnary in the interpreter.
    fn try_parse_monotonic_iso8601_timestamp<'a>(
        a: &'a str,
    ) -> Option<CheckedTimestamp<NaiveDateTime>> {
        let ts = mz_persist_types::timestamp::try_parse_monotonic_iso8601_timestamp(a)?;
        let ts = CheckedTimestamp::from_timestamplike(ts)
            .expect("monotonic_iso8601 range is a subset of CheckedTimestamp domain");
        Some(ts)
    }
);

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct CastStringToTimestampTz(pub Option<TimestampPrecision>);

impl<'a> EagerUnaryFunc<'a> for CastStringToTimestampTz {
    type Input = &'a str;
    type Output = Result<CheckedTimestamp<DateTime<Utc>>, EvalError>;

    fn call(&self, a: &'a str) -> Result<CheckedTimestamp<DateTime<Utc>>, EvalError> {
        let out = strconv::parse_timestamptz(a)?;
        let updated = out.round_to_precision(self.0)?;
        Ok(updated)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::TimestampTz { precision: self.0 }.nullable(input.nullable)
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        to_unary!(super::CastTimestampTzToString)
    }
}

impl fmt::Display for CastStringToTimestampTz {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("text_to_timestamp_with_time_zone")
    }
}

sqlfunc!(
    #[sqlname = "text_to_interval"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastIntervalToString)]
    fn cast_string_to_interval<'a>(a: &'a str) -> Result<Interval, EvalError> {
        strconv::parse_interval(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "text_to_uuid"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastUuidToString)]
    fn cast_string_to_uuid<'a>(a: &'a str) -> Result<Uuid, EvalError> {
        strconv::parse_uuid(a).err_into()
    }
);

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastStringToArray {
    // Target array's type.
    pub return_ty: ScalarType,
    // The expression to cast the discovered array elements to the array's
    // element type.
    pub cast_expr: Box<MirScalarExpr>,
}

impl LazyUnaryFunc for CastStringToArray {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        let a = a.eval(datums, temp_storage)?;
        if a.is_null() {
            return Ok(Datum::Null);
        }
        let (datums, dims) = strconv::parse_array(
            a.unwrap_str(),
            || Datum::Null,
            |elem_text| {
                let elem_text = match elem_text {
                    Cow::Owned(s) => temp_storage.push_string(s),
                    Cow::Borrowed(s) => s,
                };
                self.cast_expr
                    .eval(&[Datum::String(elem_text)], temp_storage)
            },
        )?;

        Ok(temp_storage.try_make_datum(|packer| packer.push_array(&dims, datums))?)
    }

    /// The output ColumnType of this function
    fn output_type(&self, input_type: ColumnType) -> ColumnType {
        self.return_ty.clone().nullable(input_type.nullable)
    }

    /// Whether this function will produce NULL on NULL input
    fn propagates_nulls(&self) -> bool {
        true
    }

    /// Whether this function will produce NULL on non-NULL input
    fn introduces_nulls(&self) -> bool {
        false
    }

    /// Whether this function preserves uniqueness
    fn preserves_uniqueness(&self) -> bool {
        false
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        to_unary!(super::CastArrayToString {
            ty: self.return_ty.clone(),
        })
    }

    fn is_monotone(&self) -> bool {
        false
    }
}

impl fmt::Display for CastStringToArray {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("strtoarray")
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastStringToList {
    // Target list's type
    pub return_ty: ScalarType,
    // The expression to cast the discovered list elements to the list's
    // element type.
    pub cast_expr: Box<MirScalarExpr>,
}

impl LazyUnaryFunc for CastStringToList {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        let a = a.eval(datums, temp_storage)?;
        if a.is_null() {
            return Ok(Datum::Null);
        }
        let parsed_datums = strconv::parse_list(
            a.unwrap_str(),
            matches!(
                self.return_ty.unwrap_list_element_type(),
                ScalarType::List { .. }
            ),
            || Datum::Null,
            |elem_text| {
                let elem_text = match elem_text {
                    Cow::Owned(s) => temp_storage.push_string(s),
                    Cow::Borrowed(s) => s,
                };
                self.cast_expr
                    .eval(&[Datum::String(elem_text)], temp_storage)
            },
        )?;

        Ok(temp_storage.make_datum(|packer| packer.push_list(parsed_datums)))
    }

    /// The output ColumnType of this function
    fn output_type(&self, input_type: ColumnType) -> ColumnType {
        self.return_ty
            .without_modifiers()
            .nullable(input_type.nullable)
    }

    /// Whether this function will produce NULL on NULL input
    fn propagates_nulls(&self) -> bool {
        true
    }

    /// Whether this function will produce NULL on non-NULL input
    fn introduces_nulls(&self) -> bool {
        false
    }

    /// Whether this function preserves uniqueness
    fn preserves_uniqueness(&self) -> bool {
        false
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        to_unary!(super::CastListToString {
            ty: self.return_ty.clone(),
        })
    }

    fn is_monotone(&self) -> bool {
        false
    }
}

impl fmt::Display for CastStringToList {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("strtolist")
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastStringToMap {
    // Target map's value type
    pub return_ty: ScalarType,
    // The expression used to cast the discovered values to the map's value
    // type.
    pub cast_expr: Box<MirScalarExpr>,
}

impl LazyUnaryFunc for CastStringToMap {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        let a = a.eval(datums, temp_storage)?;
        if a.is_null() {
            return Ok(Datum::Null);
        }
        let parsed_map = strconv::parse_map(
            a.unwrap_str(),
            matches!(
                self.return_ty.unwrap_map_value_type(),
                ScalarType::Map { .. }
            ),
            |value_text| -> Result<Datum, EvalError> {
                let value_text = match value_text {
                    Some(Cow::Owned(s)) => Datum::String(temp_storage.push_string(s)),
                    Some(Cow::Borrowed(s)) => Datum::String(s),
                    None => Datum::Null,
                };
                self.cast_expr.eval(&[value_text], temp_storage)
            },
        )?;
        let mut pairs: Vec<(String, Datum)> = parsed_map.into_iter().map(|(k, v)| (k, v)).collect();
        pairs.sort_by(|(k1, _v1), (k2, _v2)| k1.cmp(k2));
        pairs.dedup_by(|(k1, _v1), (k2, _v2)| k1 == k2);
        Ok(temp_storage.make_datum(|packer| {
            packer.push_dict_with(|packer| {
                for (k, v) in pairs {
                    packer.push(Datum::String(&k));
                    packer.push(v);
                }
            })
        }))
    }

    /// The output ColumnType of this function
    fn output_type(&self, input_type: ColumnType) -> ColumnType {
        self.return_ty.clone().nullable(input_type.nullable)
    }

    /// Whether this function will produce NULL on NULL input
    fn propagates_nulls(&self) -> bool {
        true
    }

    /// Whether this function will produce NULL on non-NULL input
    fn introduces_nulls(&self) -> bool {
        false
    }

    /// Whether this function preserves uniqueness
    fn preserves_uniqueness(&self) -> bool {
        false
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        to_unary!(super::CastMapToString {
            ty: self.return_ty.clone(),
        })
    }

    fn is_monotone(&self) -> bool {
        false
    }
}

impl fmt::Display for CastStringToMap {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("strtomap")
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct CastStringToChar {
    pub length: Option<mz_repr::adt::char::CharLength>,
    pub fail_on_len: bool,
}

impl<'a> EagerUnaryFunc<'a> for CastStringToChar {
    type Input = &'a str;
    type Output = Result<Char<String>, EvalError>;

    fn call(&self, a: &'a str) -> Result<Char<String>, EvalError> {
        let s = format_str_trim(a, self.length, self.fail_on_len).map_err(|_| {
            assert!(self.fail_on_len);
            EvalError::StringValueTooLong {
                target_type: "character".to_string(),
                length: usize::cast_from(self.length.unwrap().into_u32()),
            }
        })?;

        Ok(Char(s))
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Char {
            length: self.length,
        }
        .nullable(input.nullable)
    }

    fn could_error(&self) -> bool {
        self.fail_on_len && self.length.is_some()
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        to_unary!(super::CastCharToString)
    }
}

impl fmt::Display for CastStringToChar {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("text_to_char")
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastStringToRange {
    // Target range's type
    pub return_ty: ScalarType,
    // The expression to cast the discovered range elements to the range's
    // element type.
    pub cast_expr: Box<MirScalarExpr>,
}

impl LazyUnaryFunc for CastStringToRange {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        let a = a.eval(datums, temp_storage)?;
        if a.is_null() {
            return Ok(Datum::Null);
        }
        let mut range = strconv::parse_range(a.unwrap_str(), |elem_text| {
            let elem_text = match elem_text {
                Cow::Owned(s) => temp_storage.push_string(s),
                Cow::Borrowed(s) => s,
            };
            self.cast_expr
                .eval(&[Datum::String(elem_text)], temp_storage)
        })?;

        range.canonicalize()?;

        Ok(temp_storage.make_datum(|packer| {
            packer
                .push_range(range)
                .expect("must have already handled errors")
        }))
    }

    /// The output ColumnType of this function
    fn output_type(&self, input_type: ColumnType) -> ColumnType {
        self.return_ty
            .without_modifiers()
            .nullable(input_type.nullable)
    }

    /// Whether this function will produce NULL on NULL input
    fn propagates_nulls(&self) -> bool {
        true
    }

    /// Whether this function will produce NULL on non-NULL input
    fn introduces_nulls(&self) -> bool {
        false
    }

    /// Whether this function preserves uniqueness
    fn preserves_uniqueness(&self) -> bool {
        false
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        to_unary!(super::CastRangeToString {
            ty: self.return_ty.clone(),
        })
    }

    fn is_monotone(&self) -> bool {
        false
    }
}

impl fmt::Display for CastStringToRange {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("strtorange")
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct CastStringToVarChar {
    pub length: Option<VarCharMaxLength>,
    pub fail_on_len: bool,
}

impl<'a> EagerUnaryFunc<'a> for CastStringToVarChar {
    type Input = &'a str;
    type Output = Result<VarChar<&'a str>, EvalError>;

    fn call(&self, a: &'a str) -> Result<VarChar<&'a str>, EvalError> {
        let s =
            mz_repr::adt::varchar::format_str(a, self.length, self.fail_on_len).map_err(|_| {
                assert!(self.fail_on_len);
                EvalError::StringValueTooLong {
                    target_type: "character varying".to_string(),
                    length: usize::cast_from(self.length.unwrap().into_u32()),
                }
            })?;

        Ok(VarChar(s))
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::VarChar {
            max_length: self.length,
        }
        .nullable(input.nullable)
    }

    fn could_error(&self) -> bool {
        self.fail_on_len && self.length.is_some()
    }

    fn preserves_uniqueness(&self) -> bool {
        !self.fail_on_len || self.length.is_none()
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        to_unary!(super::CastVarCharToString)
    }
}

impl fmt::Display for CastStringToVarChar {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("text_to_varchar")
    }
}

// If we support another vector type, this should likely get hoisted into a
// position akin to array parsing.
static INT2VECTOR_CAST_EXPR: LazyLock<MirScalarExpr> = LazyLock::new(|| MirScalarExpr::CallUnary {
    func: UnaryFunc::CastStringToInt16(CastStringToInt16),
    expr: Box::new(MirScalarExpr::Column(0)),
});

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct CastStringToInt2Vector;

impl LazyUnaryFunc for CastStringToInt2Vector {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        let a = a.eval(datums, temp_storage)?;
        if a.is_null() {
            return Ok(Datum::Null);
        }

        let datums = strconv::parse_legacy_vector(a.unwrap_str(), |elem_text| {
            let elem_text = match elem_text {
                Cow::Owned(s) => temp_storage.push_string(s),
                Cow::Borrowed(s) => s,
            };
            INT2VECTOR_CAST_EXPR.eval(&[Datum::String(elem_text)], temp_storage)
        })?;
        array_create_scalar(&datums, temp_storage)
    }

    /// The output ColumnType of this function
    fn output_type(&self, input_type: ColumnType) -> ColumnType {
        ScalarType::Int2Vector.nullable(input_type.nullable)
    }

    /// Whether this function will produce NULL on NULL input
    fn propagates_nulls(&self) -> bool {
        true
    }

    /// Whether this function will produce NULL on non-NULL input
    fn introduces_nulls(&self) -> bool {
        false
    }

    /// Whether this function preserves uniqueness
    fn preserves_uniqueness(&self) -> bool {
        false
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        to_unary!(super::CastInt2VectorToString)
    }

    fn is_monotone(&self) -> bool {
        false
    }
}

impl fmt::Display for CastStringToInt2Vector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("strtoint2vector")
    }
}

sqlfunc!(
    #[sqlname = "text_to_jsonb"]
    #[preserves_uniqueness = false]
    #[inverse = to_unary!(super::CastJsonbToString)]
    // TODO(jamii): it would be much more efficient to skip the intermediate repr::jsonb::Jsonb.
    fn cast_string_to_jsonb<'a>(a: &'a str) -> Result<Jsonb, EvalError> {
        Ok(strconv::parse_jsonb(a)?)
    }
);

sqlfunc!(
    #[sqlname = "btrim"]
    fn trim_whitespace<'a>(a: &'a str) -> &'a str {
        a.trim_matches(' ')
    }
);

sqlfunc!(
    #[sqlname = "ltrim"]
    fn trim_leading_whitespace<'a>(a: &'a str) -> &'a str {
        a.trim_start_matches(' ')
    }
);

sqlfunc!(
    #[sqlname = "rtrim"]
    fn trim_trailing_whitespace<'a>(a: &'a str) -> &'a str {
        a.trim_end_matches(' ')
    }
);

sqlfunc!(
    #[sqlname = "initcap"]
    fn initcap<'a>(a: &'a str) -> String {
        let mut out = String::new();
        let mut capitalize_next = true;
        for ch in a.chars() {
            if capitalize_next {
                out.extend(ch.to_uppercase())
            } else {
                out.extend(ch.to_lowercase())
            };
            capitalize_next = !ch.is_alphanumeric();
        }
        out
    }
);

sqlfunc!(
    #[sqlname = "ascii"]
    fn ascii<'a>(a: &'a str) -> i32 {
        a.chars()
            .next()
            .and_then(|c| i32::try_from(u32::from(c)).ok())
            .unwrap_or(0)
    }
);

sqlfunc!(
    #[sqlname = "char_length"]
    fn char_length<'a>(a: &'a str) -> Result<i32, EvalError> {
        let length = a.chars().count();
        i32::try_from(length).or(Err(EvalError::Int32OutOfRange(length.to_string())))
    }
);

sqlfunc!(
    #[sqlname = "bit_length"]
    fn bit_length_string<'a>(a: &'a str) -> Result<i32, EvalError> {
        let length = a.as_bytes().len() * 8;
        i32::try_from(length).or(Err(EvalError::Int32OutOfRange(length.to_string())))
    }
);

sqlfunc!(
    #[sqlname = "octet_length"]
    fn byte_length_string<'a>(a: &'a str) -> Result<i32, EvalError> {
        let length = a.as_bytes().len();
        i32::try_from(length).or(Err(EvalError::Int32OutOfRange(length.to_string())))
    }
);

sqlfunc!(
    fn upper<'a>(a: &'a str) -> String {
        a.to_uppercase()
    }
);

sqlfunc!(
    fn lower<'a>(a: &'a str) -> String {
        a.to_lowercase()
    }
);

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct IsLikeMatch(pub like_pattern::Matcher);

impl<'a> EagerUnaryFunc<'a> for IsLikeMatch {
    type Input = &'a str;
    type Output = bool;

    fn call(&self, haystack: &'a str) -> bool {
        self.0.is_match(haystack)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Bool.nullable(input.nullable)
    }
}

impl fmt::Display for IsLikeMatch {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}like[{}]",
            if self.0.case_insensitive { "i" } else { "" },
            self.0.pattern.escaped()
        )
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct IsRegexpMatch(pub Regex);

impl<'a> EagerUnaryFunc<'a> for IsRegexpMatch {
    type Input = &'a str;
    type Output = bool;

    fn call(&self, haystack: &'a str) -> bool {
        self.0.is_match(haystack)
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Bool.nullable(input.nullable)
    }
}

impl fmt::Display for IsRegexpMatch {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "is_regexp_match[{}, case_insensitive={}]",
            self.0.pattern.escaped(),
            self.0.case_insensitive
        )
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct RegexpMatch(pub Regex);

impl LazyUnaryFunc for RegexpMatch {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        let haystack = a.eval(datums, temp_storage)?;
        if haystack.is_null() {
            return Ok(Datum::Null);
        }
        regexp_match_static(haystack, temp_storage, &self.0)
    }

    /// The output ColumnType of this function
    fn output_type(&self, _input_type: ColumnType) -> ColumnType {
        ScalarType::Array(Box::new(ScalarType::String)).nullable(true)
    }

    /// Whether this function will produce NULL on NULL input
    fn propagates_nulls(&self) -> bool {
        true
    }

    /// Whether this function will produce NULL on non-NULL input
    fn introduces_nulls(&self) -> bool {
        // Returns null if the regex did not match
        true
    }

    /// Whether this function preserves uniqueness
    fn preserves_uniqueness(&self) -> bool {
        false
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        None
    }

    fn is_monotone(&self) -> bool {
        false
    }
}

impl fmt::Display for RegexpMatch {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "regexp_match[{}, case_insensitive={}]",
            self.0.pattern.escaped(),
            self.0.case_insensitive
        )
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct RegexpSplitToArray(pub Regex);

impl LazyUnaryFunc for RegexpSplitToArray {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        let haystack = a.eval(datums, temp_storage)?;
        if haystack.is_null() {
            return Ok(Datum::Null);
        }
        regexp_split_to_array_re(haystack.unwrap_str(), &self.0, temp_storage)
    }

    /// The output ColumnType of this function
    fn output_type(&self, input_type: ColumnType) -> ColumnType {
        ScalarType::Array(Box::new(ScalarType::String)).nullable(input_type.nullable)
    }

    /// Whether this function will produce NULL on NULL input
    fn propagates_nulls(&self) -> bool {
        true
    }

    /// Whether this function will produce NULL on non-NULL input
    fn introduces_nulls(&self) -> bool {
        false
    }

    /// Whether this function preserves uniqueness
    fn preserves_uniqueness(&self) -> bool {
        false
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        None
    }

    fn is_monotone(&self) -> bool {
        false
    }
}

impl fmt::Display for RegexpSplitToArray {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "regexp_split_to_array[{}, case_insensitive={}]",
            self.0.pattern.escaped(),
            self.0.case_insensitive
        )
    }
}

sqlfunc!(
    #[sqlname = "mz_panic"]
    fn panic<'a>(a: &'a str) -> String {
        print!("{}", a);
        panic!("{}", a)
    }
);

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect, Arbitrary,
)]
pub struct QuoteIdent;

impl LazyUnaryFunc for QuoteIdent {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        let d = a.eval(datums, temp_storage)?;
        if d.is_null() {
            return Ok(Datum::Null);
        }
        let v = d.unwrap_str();
        let i = mz_sql_parser::ast::Ident::new(v).map_err(|err| EvalError::InvalidIdentifier {
            ident: v.to_string(),
            detail: Some(err.to_string()),
        })?;
        let r = temp_storage.push_string(i.to_string());

        Ok(Datum::String(r))
    }

    /// The output ColumnType of this function
    fn output_type(&self, input_type: ColumnType) -> ColumnType {
        ScalarType::String.nullable(input_type.nullable)
    }

    /// Whether this function will produce NULL on NULL input
    fn propagates_nulls(&self) -> bool {
        true
    }

    /// Whether this function will produce NULL on non-NULL input
    fn introduces_nulls(&self) -> bool {
        false
    }

    /// Whether this function preserves uniqueness
    fn preserves_uniqueness(&self) -> bool {
        true
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        None
    }

    fn is_monotone(&self) -> bool {
        false
    }
}

impl fmt::Display for QuoteIdent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "quote_ident")
    }
}
