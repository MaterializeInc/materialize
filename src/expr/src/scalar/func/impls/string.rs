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

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use once_cell::sync::Lazy;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use mz_lowertest::MzReflect;
use mz_ore::cast::CastFrom;
use mz_ore::result::ResultExt;
use mz_ore::str::StrExt;
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::char::{format_str_trim, Char};
use mz_repr::adt::interval::Interval;
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::adt::numeric::{self, Numeric, NumericMaxScale};
use mz_repr::adt::regex::Regex;
use mz_repr::adt::system::{Oid, PgLegacyChar};
use mz_repr::adt::varchar::{VarChar, VarCharMaxLength};
use mz_repr::{strconv, ColumnType, Datum, Row, RowArena, ScalarType};

use crate::scalar::func::{array_create_scalar, EagerUnaryFunc, LazyUnaryFunc};
use crate::{like_pattern, EvalError, MirScalarExpr, UnaryFunc};

sqlfunc!(
    #[sqlname = "strtobool"]
    fn cast_string_to_bool<'a>(a: &'a str) -> Result<bool, EvalError> {
        strconv::parse_bool(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtopglegacychar"]
    #[preserves_uniqueness = true]
    fn cast_string_to_pg_legacy_char<'a>(a: &'a str) -> PgLegacyChar {
        PgLegacyChar(a.as_bytes().get(0).copied().unwrap_or(0))
    }
);

sqlfunc!(
    #[sqlname = "strtobytes"]
    #[preserves_uniqueness = true]
    fn cast_string_to_bytes<'a>(a: &'a str) -> Result<Vec<u8>, EvalError> {
        strconv::parse_bytes(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtoi16"]
    fn cast_string_to_int16<'a>(a: &'a str) -> Result<i16, EvalError> {
        strconv::parse_int16(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtoi32"]
    fn cast_string_to_int32<'a>(a: &'a str) -> Result<i32, EvalError> {
        strconv::parse_int32(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtoi64"]
    fn cast_string_to_int64<'a>(a: &'a str) -> Result<i64, EvalError> {
        strconv::parse_int64(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtof32"]
    fn cast_string_to_float32<'a>(a: &'a str) -> Result<f32, EvalError> {
        strconv::parse_float32(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtof64"]
    fn cast_string_to_float64<'a>(a: &'a str) -> Result<f64, EvalError> {
        strconv::parse_float64(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtooid"]
    fn cast_string_to_oid<'a>(a: &'a str) -> Result<Oid, EvalError> {
        Ok(Oid(strconv::parse_oid(a)?))
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
}

impl fmt::Display for CastStringToNumeric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("strtonumeric")
    }
}

sqlfunc!(
    #[sqlname = "strtodate"]
    fn cast_string_to_date<'a>(a: &'a str) -> Result<NaiveDate, EvalError> {
        strconv::parse_date(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtotime"]
    fn cast_string_to_time<'a>(a: &'a str) -> Result<NaiveTime, EvalError> {
        strconv::parse_time(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtots"]
    fn cast_string_to_timestamp<'a>(a: &'a str) -> Result<NaiveDateTime, EvalError> {
        strconv::parse_timestamp(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtotstz"]
    fn cast_string_to_timestamp_tz<'a>(a: &'a str) -> Result<DateTime<Utc>, EvalError> {
        strconv::parse_timestamptz(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtoiv"]
    fn cast_string_to_interval<'a>(a: &'a str) -> Result<Interval, EvalError> {
        strconv::parse_interval(a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "strtouuid"]
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
        let datums = strconv::parse_array(
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
        array_create_scalar(&datums, temp_storage)
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
    fn output_type(&self, _input_type: ColumnType) -> ColumnType {
        self.return_ty.without_modifiers().nullable(false)
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
                    Cow::Owned(s) => temp_storage.push_string(s),
                    Cow::Borrowed(s) => s,
                };
                self.cast_expr
                    .eval(&[Datum::String(value_text)], temp_storage)
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
}

impl fmt::Display for CastStringToChar {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("strtochar")
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
    type Output = Result<VarChar<String>, EvalError>;

    fn call(&self, a: &'a str) -> Result<VarChar<String>, EvalError> {
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
}

impl fmt::Display for CastStringToVarChar {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("strtovarchar")
    }
}

// If we support another vector type, this should likely get hoisted into a
// position akin to array parsing.
static INT2VECTOR_CAST_EXPR: Lazy<MirScalarExpr> = Lazy::new(|| MirScalarExpr::CallUnary {
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
    fn output_type(&self, _input_type: ColumnType) -> ColumnType {
        ScalarType::Int2Vector.nullable(false)
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
}

impl fmt::Display for CastStringToInt2Vector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("strtoint2vector")
    }
}

sqlfunc!(
    #[sqlname = "strtojsonb"]
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
    #[sqlname = "ascii"]
    fn ascii<'a>(a: &'a str) -> i32 {
        match a.chars().next() {
            None => 0,
            Some(v) => v as i32,
        }
    }
);

sqlfunc!(
    #[sqlname = "char_length"]
    fn char_length<'a>(a: &'a str) -> Result<i32, EvalError> {
        i32::try_from(a.chars().count()).or(Err(EvalError::Int32OutOfRange))
    }
);

sqlfunc!(
    #[sqlname = "bit_length"]
    fn bit_length_string<'a>(a: &'a str) -> Result<i32, EvalError> {
        i32::try_from(a.as_bytes().len() * 8).or(Err(EvalError::Int32OutOfRange))
    }
);

sqlfunc!(
    #[sqlname = "octet_length"]
    fn byte_length_string<'a>(a: &'a str) -> Result<i32, EvalError> {
        i32::try_from(a.as_bytes().len()).or(Err(EvalError::Int32OutOfRange))
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
        write!(f, "{} ~~", self.0.pattern.quoted())
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
        write!(f, "{} ~", self.0.as_str().quoted())
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
        let mut row = Row::default();
        let mut packer = row.packer();
        if self.0.captures_len() > 1 {
            // The regex contains capture groups, so return an array containing the
            // matched text in each capture group, unless the entire match fails.
            // Individual capture groups may also be null if that group did not
            // participate in the match.
            match self.0.captures(haystack.unwrap_str()) {
                None => packer.push(Datum::Null),
                Some(captures) => packer.push_array(
                    &[ArrayDimension {
                        lower_bound: 1,
                        length: captures.len() - 1,
                    }],
                    // Skip the 0th capture group, which is the whole match.
                    captures.iter().skip(1).map(|mtch| match mtch {
                        None => Datum::Null,
                        Some(mtch) => Datum::String(mtch.as_str()),
                    }),
                )?,
            }
        } else {
            // The regex contains no capture groups, so return a one-element array
            // containing the match, or null if there is no match.
            match self.0.find(haystack.unwrap_str()) {
                None => packer.push(Datum::Null),
                Some(mtch) => packer.push_array(
                    &[ArrayDimension {
                        lower_bound: 1,
                        length: 1,
                    }],
                    std::iter::once(Datum::String(mtch.as_str())),
                )?,
            };
        };
        Ok(temp_storage.push_unary_row(row))
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
}

impl fmt::Display for RegexpMatch {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "regexp_match[{}]", self.0.as_str())
    }
}

sqlfunc!(
    #[sqlname = "mz_panic"]
    fn panic<'a>(a: &'a str) -> String {
        print!("{}", a);
        panic!("{}", a)
    }
);
