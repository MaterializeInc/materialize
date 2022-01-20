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
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use lowertest::MzStructReflect;
use ore::result::ResultExt;
use repr::adt::char::{format_str_trim, Char};
use repr::adt::interval::Interval;
use repr::adt::numeric::{self, Numeric};
use repr::adt::system::Int2Vector;
use repr::adt::varchar::VarChar;
use repr::{strconv, ColumnType, Datum, RowArena, ScalarType};

use crate::scalar::func::{array_create_scalar, EagerUnaryFunc, LazyUnaryFunc};
use crate::{EvalError, MirScalarExpr};

sqlfunc!(
    #[sqlname = "strtobool"]
    fn cast_string_to_bool<'a>(a: &'a str) -> Result<bool, EvalError> {
        strconv::parse_bool(a).err_into()
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
    #[sqlname = "strtoint2vector"]
    #[preserves_uniqueness = true]
    fn cast_string_to_int2_vector<'a>(a: &'a str) -> Result<Int2Vector, EvalError> {
        Ok(Int2Vector(String::from(a)))
    }
);

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzStructReflect,
)]
pub struct CastStringToNumeric(pub Option<u8>);

impl<'a> EagerUnaryFunc<'a> for CastStringToNumeric {
    type Input = &'a str;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, a: &'a str) -> Result<Numeric, EvalError> {
        let mut d = strconv::parse_numeric(a)?;
        if let Some(scale) = self.0 {
            if numeric::rescale(&mut d.0, scale).is_err() {
                return Err(EvalError::NumericFieldOverflow);
            }
        }
        Ok(d.into_inner())
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Numeric { scale: self.0 }.nullable(input.nullable)
    }
}

impl fmt::Display for CastStringToNumeric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("i64tonumeric")
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

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzStructReflect,
)]
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

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzStructReflect,
)]
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
        self.return_ty.default_embedded_value().nullable(false)
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

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzStructReflect,
)]
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
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzStructReflect,
)]
pub struct CastStringToChar {
    pub length: Option<usize>,
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
                length: self.length.unwrap(),
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
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzStructReflect,
)]
pub struct CastStringToVarChar {
    pub length: Option<usize>,
    pub fail_on_len: bool,
}

impl<'a> EagerUnaryFunc<'a> for CastStringToVarChar {
    type Input = &'a str;
    type Output = Result<VarChar<String>, EvalError>;

    fn call(&self, a: &'a str) -> Result<VarChar<String>, EvalError> {
        let s = repr::adt::varchar::format_str(a, self.length, self.fail_on_len).map_err(|_| {
            assert!(self.fail_on_len);
            EvalError::StringValueTooLong {
                target_type: "character varying".to_string(),
                length: self.length.unwrap(),
            }
        })?;

        Ok(VarChar(s))
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::VarChar {
            length: self.length,
        }
        .nullable(input.nullable)
    }
}

impl fmt::Display for CastStringToVarChar {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("strtovarchar")
    }
}
