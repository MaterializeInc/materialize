// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::cmp::{self, Ordering};
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::iter;
use std::str;
use std::thread;

use ::encoding::label::encoding_from_whatwg_label;
use ::encoding::DecoderTrap;
use chrono::{
    DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone, Timelike,
    Utc,
};
use dec::{OrderedDecimal, Rounding};
use hmac::{Hmac, Mac, NewMac};
use itertools::Itertools;
use md5::{Digest, Md5};
use ordered_float::OrderedFloat;
use regex::RegexBuilder;
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Sha224, Sha256, Sha384, Sha512};

use lowertest::MzEnumReflect;
use ore::collections::CollectionExt;
use ore::fmt::FormatBuffer;
use ore::result::ResultExt;
use ore::str::StrExt;
use pgrepr::Type;
use repr::adt::apd::{self, Apd};
use repr::adt::array::ArrayDimension;
use repr::adt::datetime::{DateTimeUnits, Timezone};
use repr::adt::decimal::MAX_DECIMAL_PRECISION;
use repr::adt::interval::Interval;
use repr::adt::jsonb::JsonbRef;
use repr::adt::regex::Regex;
use repr::{strconv, ColumnName, ColumnType, Datum, Row, RowArena, ScalarType};

use crate::scalar::func::format::DateTimeFormat;
use crate::{like_pattern, EvalError, MirScalarExpr};

mod encoding;
mod format;

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzEnumReflect,
)]
pub enum NullaryFunc {
    MzLogicalTimestamp,
}

impl NullaryFunc {
    pub fn output_type(&self) -> ColumnType {
        match self {
            NullaryFunc::MzLogicalTimestamp => ScalarType::Decimal(38, 0).nullable(false),
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

pub fn and<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
    a_expr: &'a MirScalarExpr,
    b_expr: &'a MirScalarExpr,
) -> Result<Datum<'a>, EvalError> {
    match a_expr.eval(datums, temp_storage)? {
        Datum::False => Ok(Datum::False),
        a => match (a, b_expr.eval(datums, temp_storage)?) {
            (_, Datum::False) => Ok(Datum::False),
            (Datum::Null, _) | (_, Datum::Null) => Ok(Datum::Null),
            (Datum::True, Datum::True) => Ok(Datum::True),
            _ => unreachable!(),
        },
    }
}

pub fn or<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
    a_expr: &'a MirScalarExpr,
    b_expr: &'a MirScalarExpr,
) -> Result<Datum<'a>, EvalError> {
    match a_expr.eval(datums, temp_storage)? {
        Datum::True => Ok(Datum::True),
        a => match (a, b_expr.eval(datums, temp_storage)?) {
            (_, Datum::True) => Ok(Datum::True),
            (Datum::Null, _) | (_, Datum::Null) => Ok(Datum::Null),
            (Datum::False, Datum::False) => Ok(Datum::False),
            _ => unreachable!(),
        },
    }
}

fn not<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(!a.unwrap_bool())
}

fn abs_int32<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int32().abs())
}

fn abs_int64<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int64().abs())
}

fn abs_decimal<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_decimal().abs())
}

fn abs_apd<'a>(a: Datum<'a>) -> Datum<'a> {
    let mut a = a.unwrap_apd();
    apd::cx_datum().abs(&mut a.0);
    Datum::APD(a)
}

fn abs_float32<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float32().abs())
}

fn abs_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float64().abs())
}

fn cast_bool_to_string<'a>(a: Datum<'a>) -> Datum<'a> {
    match a.unwrap_bool() {
        true => Datum::from("true"),
        false => Datum::from("false"),
    }
}

fn cast_bool_to_string_nonstandard<'a>(a: Datum<'a>) -> Datum<'a> {
    // N.B. this function differs from `cast_bool_to_string_implicit` because
    // the SQL specification requires `true` and `false` to be spelled out in
    // explicit casts, while PostgreSQL prefers its more concise `t` and `f`
    // representation in some contexts, for historical reasons.
    Datum::String(strconv::format_bool_static(a.unwrap_bool()))
}

fn cast_bool_to_int32<'a>(a: Datum<'a>) -> Datum<'a> {
    match a.unwrap_bool() {
        true => Datum::Int32(1),
        false => Datum::Int32(0),
    }
}

fn cast_int32_to_bool<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int32() != 0)
}

fn cast_int32_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_int32(&mut buf, a.unwrap_int32());
    Datum::String(temp_storage.push_string(buf))
}

fn cast_int32_to_float32<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int32() as f32)
}

fn cast_int32_to_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_int32()))
}

fn cast_int32_to_int64<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(i64::from(a.unwrap_int32()))
}

fn cast_int32_to_decimal<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(i128::from(a.unwrap_int32()))
}

fn cast_int32_to_apd<'a>(a: Datum<'a>, scale: Option<u8>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_int32();
    let mut a = Apd::from(a);
    if let Some(scale) = scale {
        if apd::rescale(&mut a, scale).is_err() {
            return Err(EvalError::NumericFieldOverflow);
        }
    }
    // Besides `rescale`, cast is infallible.
    Ok(Datum::APD(OrderedDecimal(a)))
}

fn cast_int64_to_bool<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int64() != 0)
}

fn cast_int64_to_int32<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    match i32::try_from(a.unwrap_int64()) {
        Ok(n) => Ok(Datum::from(n)),
        Err(_) => Err(EvalError::Int32OutOfRange),
    }
}

fn cast_int64_to_decimal<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(i128::from(a.unwrap_int64()))
}

fn cast_int64_to_apd<'a>(a: Datum<'a>, scale: Option<u8>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_int64();
    let mut a = Apd::from(a);
    if let Some(scale) = scale {
        if apd::rescale(&mut a, scale).is_err() {
            return Err(EvalError::NumericFieldOverflow);
        }
    }
    // Besides `rescale`, cast is infallible.
    Ok(Datum::APD(OrderedDecimal(a)))
}

fn cast_int64_to_float32<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int64() as f32)
}

fn cast_int64_to_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int64() as f64)
}

fn cast_int64_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_int64(&mut buf, a.unwrap_int64());
    Datum::String(temp_storage.push_string(buf))
}

fn cast_float32_to_int32<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let f = round_float32(a).unwrap_float32();
    // This condition is delicate because i32::MIN can be represented exactly by
    // an f32 but not i32::MAX. We follow PostgreSQL's approach here.
    //
    // See: https://github.com/postgres/postgres/blob/ca3b37487/src/include/c.h#L1074-L1096
    if (f >= (i32::MIN as f32)) && (f < -(i32::MIN as f32)) {
        Ok(Datum::from(f as i32))
    } else {
        Err(EvalError::Int32OutOfRange)
    }
}

fn cast_float32_to_int64<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let f = round_float32(a).unwrap_float32();
    // This condition is delicate because i64::MIN can be represented exactly by
    // an f32 but not i64::MAX. We follow PostgreSQL's approach here.
    //
    // See: https://github.com/postgres/postgres/blob/ca3b37487/src/include/c.h#L1074-L1096
    if (f >= (i64::MIN as f32)) && (f < -(i64::MIN as f32)) {
        Ok(Datum::from(f as i64))
    } else {
        Err(EvalError::Int64OutOfRange)
    }
}

fn cast_float32_to_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_float32()))
}

fn cast_float32_to_decimal<'a>(a: Datum<'a>, scale: u8) -> Result<Datum<'a>, EvalError> {
    let f = a.unwrap_float32();
    if f > 10_f32.powi(i32::from(MAX_DECIMAL_PRECISION - scale)) {
        // When we can return error detail:
        // format!("A field with precision {}, \
        //         scale {} must round to an absolute value less than 10^{}.",
        //         MAX_DECIMAL_PRECISION, scale, MAX_DECIMAL_PRECISION - scale)
        return Err(EvalError::NumericFieldOverflow);
    }
    Ok(Datum::from((f * 10_f32.powi(i32::from(scale))) as i128))
}

fn cast_float32_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_float32(&mut buf, a.unwrap_float32());
    Datum::String(temp_storage.push_string(buf))
}

fn cast_float64_to_int32<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let f = round_float64(a).unwrap_float64();
    // This condition is delicate because i32::MIN can be represented exactly by
    // an f64 but not i32::MAX. We follow PostgreSQL's approach here.
    //
    // See: https://github.com/postgres/postgres/blob/ca3b37487/src/include/c.h#L1074-L1096
    if (f >= (i32::MIN as f64)) && (f < -(i32::MIN as f64)) {
        Ok(Datum::from(f as i32))
    } else {
        Err(EvalError::Int32OutOfRange)
    }
}

fn cast_float64_to_int64<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let f = round_float64(a).unwrap_float64();
    // This condition is delicate because i64::MIN can be represented exactly by
    // an f64 but not i64::MAX. We follow PostgreSQL's approach here.
    //
    // See: https://github.com/postgres/postgres/blob/ca3b37487/src/include/c.h#L1074-L1096
    if (f >= (i64::MIN as f64)) && (f < -(i64::MIN as f64)) {
        Ok(Datum::from(f as i64))
    } else {
        Err(EvalError::Int64OutOfRange)
    }
}

fn cast_float64_to_float32<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_float64();
    let result = a as f32;
    if result.is_infinite() && !a.is_infinite() {
        Err(EvalError::FloatOverflow)
    } else if result == 0.0 && a != 0.0 {
        Err(EvalError::FloatUnderflow)
    } else {
        Ok(Datum::from(result))
    }
}

fn cast_float64_to_decimal<'a>(a: Datum<'a>, scale: u8) -> Result<Datum<'a>, EvalError> {
    let f = a.unwrap_float64();
    if f > 10_f64.powi(i32::from(MAX_DECIMAL_PRECISION - scale)) {
        // When we can return error detail:
        // format!("A field with precision {}, \
        //         scale {} must round to an absolute value less than 10^{}.",
        //         MAX_DECIMAL_PRECISION, scale, MAX_DECIMAL_PRECISION - scale)
        return Err(EvalError::NumericFieldOverflow);
    }
    Ok(Datum::from((f * 10_f64.powi(i32::from(scale))) as i128))
}

fn cast_float32_to_apd<'a>(a: Datum<'a>, scale: Option<u8>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_float32();
    if a.is_infinite() {
        return Err(EvalError::InfinityOutOfDomain(
            "casting real to apd".to_owned(),
        ));
    }
    let mut a = Apd::from(a);
    if let Some(scale) = scale {
        if apd::rescale(&mut a, scale).is_err() {
            return Err(EvalError::NumericFieldOverflow);
        }
    }
    apd::munge_apd(&mut a).unwrap();
    Ok(Datum::APD(OrderedDecimal(a)))
}

fn cast_float64_to_apd<'a>(a: Datum<'a>, scale: Option<u8>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_float64();
    if a.is_infinite() {
        return Err(EvalError::InfinityOutOfDomain(
            "casting double precision to apd".to_owned(),
        ));
    }
    let mut a = Apd::from(a);
    if let Some(scale) = scale {
        if apd::rescale(&mut a, scale).is_err() {
            return Err(EvalError::NumericFieldOverflow);
        }
    }
    match apd::munge_apd(&mut a) {
        Ok(_) => Ok(Datum::APD(OrderedDecimal(a))),
        Err(_) => Err(EvalError::NumericFieldOverflow),
    }
}

fn cast_float64_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_float64(&mut buf, a.unwrap_float64());
    Datum::String(temp_storage.push_string(buf))
}

fn cast_decimal_to_int32<'a>(a: Datum<'a>, scale: u8) -> Datum<'a> {
    let decimal = a.unwrap_decimal().with_scale(scale);
    let factor = 10_i128.pow(u32::from(scale));
    Datum::from((decimal.round(0).significand() / factor) as i32)
}

fn cast_decimal_to_int64<'a>(a: Datum<'a>, scale: u8) -> Datum<'a> {
    let decimal = a.unwrap_decimal().with_scale(scale);
    let factor = 10_i128.pow(u32::from(scale));
    Datum::from((decimal.round(0).significand() / factor) as i64)
}

fn cast_apd_to_int32<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut a = a.unwrap_apd().0;
    let mut cx = apd::cx_datum();
    cx.round(&mut a);
    match cx.try_into_i32(a) {
        Ok(i) => Ok(Datum::from(i)),
        Err(_) => Err(EvalError::Int32OutOfRange),
    }
}

fn cast_apd_to_int64<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut a = a.unwrap_apd().0;
    let mut cx = apd::cx_datum();
    cx.round(&mut a);
    match cx.try_into_i64(a) {
        Ok(i) => Ok(Datum::from(i)),
        Err(_) => Err(EvalError::Int64OutOfRange),
    }
}

fn cast_significand_to_float32<'a>(a: Datum<'a>) -> Datum<'a> {
    // The second half of this function is defined in plan_cast_internal
    Datum::from(a.unwrap_decimal().as_i128() as f32)
}

fn cast_significand_to_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    // The second half of this function is defined in plan_cast_internal
    Datum::from(a.unwrap_decimal().as_i128() as f64)
}

fn cast_apd_to_float32<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_apd().0;
    let mut cx = apd::cx_datum();
    match cx.try_into_f32(a) {
        Ok(i) => Ok(Datum::from(i)),
        Err(_) => Err(EvalError::Float32OutOfRange),
    }
}

fn cast_apd_to_float64<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_apd().0;
    let mut cx = apd::cx_datum();
    let i = cx.try_into_f64(a).unwrap();
    Ok(Datum::from(i))
}

fn cast_decimal_to_string<'a>(a: Datum<'a>, scale: u8, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_decimal(&mut buf, &a.unwrap_decimal().with_scale(scale));
    Datum::String(temp_storage.push_string(buf))
}

fn cast_apd_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_apd(&mut buf, &a.unwrap_apd());
    Datum::String(temp_storage.push_string(buf))
}

fn cast_string_to_bool<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    match strconv::parse_bool(a.unwrap_str())? {
        true => Ok(Datum::True),
        false => Ok(Datum::False),
    }
}

fn cast_string_to_bytes<'a>(
    a: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let bytes = strconv::parse_bytes(a.unwrap_str())?;
    Ok(Datum::Bytes(temp_storage.push_bytes(bytes)))
}

fn cast_string_to_int32<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    strconv::parse_int32(a.unwrap_str())
        .map(Datum::Int32)
        .err_into()
}

fn cast_string_to_int64<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    strconv::parse_int64(a.unwrap_str())
        .map(Datum::Int64)
        .err_into()
}

fn cast_string_to_float32<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    strconv::parse_float32(a.unwrap_str())
        .map(|n| Datum::Float32(n.into()))
        .err_into()
}

fn cast_string_to_float64<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    strconv::parse_float64(a.unwrap_str())
        .map(|n| Datum::Float64(n.into()))
        .err_into()
}

fn cast_string_to_decimal<'a>(a: Datum<'a>, scale: u8) -> Result<Datum<'a>, EvalError> {
    strconv::parse_decimal(a.unwrap_str())
        .map(|d| {
            Datum::from(match d.scale().cmp(&scale) {
                Ordering::Less => d.significand() * 10_i128.pow(u32::from(scale - d.scale())),
                Ordering::Equal => d.significand(),
                Ordering::Greater => d.significand() / 10_i128.pow(u32::from(d.scale() - scale)),
            })
        })
        .err_into()
}

fn cast_string_to_apd<'a>(a: Datum<'a>, scale: Option<u8>) -> Result<Datum<'a>, EvalError> {
    let mut d = strconv::parse_apd(a.unwrap_str())?;
    if let Some(scale) = scale {
        if apd::rescale(&mut d.0, scale).is_err() {
            return Err(EvalError::NumericFieldOverflow);
        }
    }
    Ok(Datum::APD(d))
}

fn cast_string_to_date<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    strconv::parse_date(a.unwrap_str())
        .map(Datum::Date)
        .err_into()
}

fn cast_string_to_array<'a>(
    a: Datum<'a>,
    cast_expr: &'a MirScalarExpr,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let datums = strconv::parse_array(
        a.unwrap_str(),
        || Datum::Null,
        |elem_text| {
            let elem_text = match elem_text {
                Cow::Owned(s) => temp_storage.push_string(s),
                Cow::Borrowed(s) => s,
            };
            cast_expr.eval(&[Datum::String(elem_text)], temp_storage)
        },
    )?;
    array_create_scalar(&datums, temp_storage)
}

fn cast_string_to_list<'a>(
    a: Datum<'a>,
    list_typ: &ScalarType,
    cast_expr: &'a MirScalarExpr,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let parsed_datums = strconv::parse_list(
        a.unwrap_str(),
        matches!(list_typ.unwrap_list_element_type(), ScalarType::List { .. }),
        || Datum::Null,
        |elem_text| {
            let elem_text = match elem_text {
                Cow::Owned(s) => temp_storage.push_string(s),
                Cow::Borrowed(s) => s,
            };
            cast_expr.eval(&[Datum::String(elem_text)], temp_storage)
        },
    )?;

    Ok(temp_storage.make_datum(|packer| packer.push_list(parsed_datums)))
}

fn cast_string_to_map<'a>(
    a: Datum<'a>,
    map_typ: &ScalarType,
    cast_expr: &'a MirScalarExpr,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let parsed_map = strconv::parse_map(
        a.unwrap_str(),
        matches!(map_typ.unwrap_map_value_type(), ScalarType::Map { .. }),
        |value_text| -> Result<Datum, EvalError> {
            let value_text = match value_text {
                Cow::Owned(s) => temp_storage.push_string(s),
                Cow::Borrowed(s) => s,
            };
            cast_expr.eval(&[Datum::String(value_text)], temp_storage)
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

fn cast_string_to_time<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    strconv::parse_time(a.unwrap_str())
        .map(Datum::Time)
        .err_into()
}

fn cast_string_to_timestamp<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    strconv::parse_timestamp(a.unwrap_str())
        .map(Datum::Timestamp)
        .err_into()
}

fn cast_string_to_timestamptz<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    strconv::parse_timestamptz(a.unwrap_str())
        .map(Datum::TimestampTz)
        .err_into()
}

fn cast_string_to_interval<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    strconv::parse_interval(a.unwrap_str())
        .map(Datum::Interval)
        .err_into()
}

fn cast_string_to_uuid<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    strconv::parse_uuid(a.unwrap_str())
        .map(Datum::Uuid)
        .err_into()
}

fn cast_date_to_timestamp<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::Timestamp(a.unwrap_date().and_hms(0, 0, 0))
}

fn cast_date_to_timestamptz<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::TimestampTz(DateTime::<Utc>::from_utc(
        a.unwrap_date().and_hms(0, 0, 0),
        Utc,
    ))
}

fn cast_date_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_date(&mut buf, a.unwrap_date());
    Datum::String(temp_storage.push_string(buf))
}

fn cast_time_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_time(&mut buf, a.unwrap_time());
    Datum::String(temp_storage.push_string(buf))
}

fn cast_time_to_interval<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let t = a.unwrap_time();
    match Interval::new(
        0,
        t.num_seconds_from_midnight() as i64,
        t.nanosecond() as i64,
    ) {
        Ok(i) => Ok(Datum::Interval(i)),
        Err(_) => Err(EvalError::IntervalOutOfRange),
    }
}

fn cast_timestamp_to_date<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::Date(a.unwrap_timestamp().date())
}

fn cast_timestamp_to_timestamptz<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::TimestampTz(DateTime::<Utc>::from_utc(a.unwrap_timestamp(), Utc))
}

fn cast_timestamp_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_timestamp(&mut buf, a.unwrap_timestamp());
    Datum::String(temp_storage.push_string(buf))
}

fn cast_timestamptz_to_date<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::Date(a.unwrap_timestamptz().naive_utc().date())
}

fn cast_timestamptz_to_timestamp<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::Timestamp(a.unwrap_timestamptz().naive_utc())
}

fn cast_timestamptz_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_timestamptz(&mut buf, a.unwrap_timestamptz());
    Datum::String(temp_storage.push_string(buf))
}

fn cast_interval_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_interval(&mut buf, a.unwrap_interval());
    Datum::String(temp_storage.push_string(buf))
}

fn cast_interval_to_time<'a>(a: Datum<'a>) -> Datum<'a> {
    let mut i = a.unwrap_interval();

    // Negative durations have their HH::MM::SS.NS values subtracted from 1 day.
    if i.duration < 0 {
        i = Interval::new(0, 86400, 0)
            .unwrap()
            .checked_add(
                &Interval::new(0, i.dur_as_secs() % (24 * 60 * 60), i.nanoseconds() as i64)
                    .unwrap(),
            )
            .unwrap();
    }

    Datum::Time(NaiveTime::from_hms_nano(
        i.hours() as u32,
        i.minutes() as u32,
        i.seconds() as u32,
        i.nanoseconds() as u32,
    ))
}

fn cast_bytes_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_bytes(&mut buf, a.unwrap_bytes());
    Datum::String(temp_storage.push_string(buf))
}

// TODO(jamii): it would be much more efficient to skip the intermediate
// repr::jsonb::Jsonb.
fn cast_string_to_jsonb<'a>(
    a: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let jsonb = strconv::parse_jsonb(a.unwrap_str())?;
    Ok(temp_storage.push_unary_row(jsonb.into_row()))
}

fn cast_jsonb_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_jsonb(&mut buf, JsonbRef::from_datum(a));
    Datum::String(temp_storage.push_string(buf))
}

pub fn jsonb_stringify<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    match a {
        Datum::JsonNull => Datum::Null,
        Datum::String(_) => a,
        _ => cast_jsonb_to_string(a, temp_storage),
    }
}

fn cast_jsonb_or_null_to_jsonb<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Null => Datum::JsonNull,
        Datum::Float64(f) => {
            if f.is_finite() {
                a
            } else if f.is_nan() {
                Datum::String("NaN")
            } else if f.is_sign_positive() {
                Datum::String("Infinity")
            } else {
                Datum::String("-Infinity")
            }
        }
        _ => a,
    }
}

fn cast_jsonb_to_int32<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    match a {
        Datum::Int64(_) => cast_int64_to_int32(a),
        Datum::Float64(_) => cast_float64_to_int32(a),
        _ => Err(EvalError::InvalidJsonbCast {
            from: jsonb_type(a).into(),
            to: "integer".into(),
        }),
    }
}

fn cast_jsonb_to_int64<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    match a {
        Datum::Int64(_) => Ok(a),
        Datum::Float64(_) => cast_float64_to_int64(a),
        _ => Err(EvalError::InvalidJsonbCast {
            from: jsonb_type(a).into(),
            to: "bigint".into(),
        }),
    }
}

fn cast_jsonb_to_float32<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    match a {
        Datum::Int64(_) => Ok(cast_int64_to_float32(a)),
        Datum::Float64(_) => cast_float64_to_float32(a),
        _ => Err(EvalError::InvalidJsonbCast {
            from: jsonb_type(a).into(),
            to: "real".into(),
        }),
    }
}

fn cast_jsonb_to_float64<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    match a {
        Datum::Int64(_) => Ok(cast_int64_to_float64(a)),
        Datum::Float64(_) => Ok(a),
        _ => Err(EvalError::InvalidJsonbCast {
            from: jsonb_type(a).into(),
            to: "double precision".into(),
        }),
    }
}

fn cast_jsonb_to_decimal<'a>(a: Datum<'a>, scale: u8) -> Result<Datum<'a>, EvalError> {
    match a {
        Datum::Int64(_) => {
            let a = cast_int64_to_decimal(a);
            if scale > 0 {
                Ok(mul_decimal(a, Datum::from(10_i128.pow(u32::from(scale)))))
            } else {
                Ok(a)
            }
        }
        Datum::Float64(_) => cast_float64_to_decimal(a, scale),
        _ => Err(EvalError::InvalidJsonbCast {
            from: jsonb_type(a).into(),
            to: "numeric".into(),
        }),
    }
}

fn cast_jsonb_to_apd<'a>(a: Datum<'a>, scale: Option<u8>) -> Result<Datum<'a>, EvalError> {
    match a {
        Datum::Int64(_) => cast_int64_to_apd(a, scale),
        Datum::Float64(_) => cast_float64_to_apd(a, scale),
        _ => Err(EvalError::InvalidJsonbCast {
            from: jsonb_type(a).into(),
            to: "apd".into(),
        }),
    }
}

fn cast_jsonb_to_bool<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    match a {
        Datum::True | Datum::False => Ok(a),
        _ => Err(EvalError::InvalidJsonbCast {
            from: jsonb_type(a).into(),
            to: "boolean".into(),
        }),
    }
}

fn jsonb_type(d: Datum<'_>) -> &'static str {
    match d {
        Datum::JsonNull => "null",
        Datum::False | Datum::True => "boolean",
        Datum::String(_) => "string",
        Datum::Int64(_) | Datum::Float64(_) => "numeric",
        Datum::List(_) => "array",
        Datum::Map(_) => "object",
        _ => unreachable!("jsonb_type called on invalid datum {:?}", d),
    }
}

fn cast_uuid_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_uuid(&mut buf, a.unwrap_uuid());
    Datum::String(temp_storage.push_string(buf))
}

/// Casts between two list types by casting each element of `a` ("list1") using
/// `cast_expr` and collecting the results into a new list ("list2").
fn cast_list1_to_list2<'a>(
    a: Datum,
    cast_expr: &'a MirScalarExpr,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut cast_datums = Vec::new();
    for el in a.unwrap_list().iter() {
        // `cast_expr` is evaluated as an expression that casts the
        // first column in `datums` (i.e. `datums[0]`) from the list elements'
        // current type to a target type.
        cast_datums.push(cast_expr.eval(&[el], temp_storage)?);
    }

    Ok(temp_storage.make_datum(|packer| packer.push_list(cast_datums)))
}

fn add_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int32()
        .checked_add(b.unwrap_int32())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
}

fn add_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int64()
        .checked_add(b.unwrap_int64())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
}

fn add_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float32() + b.unwrap_float32())
}

fn add_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float64() + b.unwrap_float64())
}

fn add_timestamp_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let dt = a.unwrap_timestamp();
    Datum::Timestamp(match b {
        Datum::Interval(i) => {
            let dt = add_timestamp_months(dt, i.months);
            dt + i.duration_as_chrono()
        }
        _ => panic!("Tried to do timestamp addition with non-interval: {:?}", b),
    })
}

fn add_timestamptz_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let dt = a.unwrap_timestamptz().naive_utc();

    let new_ndt = match b {
        Datum::Interval(i) => {
            let dt = add_timestamp_months(dt, i.months);
            dt + i.duration_as_chrono()
        }
        _ => panic!("Tried to do timestamp addition with non-interval: {:?}", b),
    };

    Datum::TimestampTz(DateTime::<Utc>::from_utc(new_ndt, Utc))
}

fn add_date_time<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let date = a.unwrap_date();
    let time = b.unwrap_time();

    Datum::Timestamp(
        NaiveDate::from_ymd(date.year(), date.month(), date.day()).and_hms_nano(
            time.hour(),
            time.minute(),
            time.second(),
            time.nanosecond(),
        ),
    )
}

fn add_date_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let date = a.unwrap_date();
    let interval = b.unwrap_interval();

    let dt = NaiveDate::from_ymd(date.year(), date.month(), date.day()).and_hms(0, 0, 0);
    let dt = add_timestamp_months(dt, interval.months);
    Datum::Timestamp(dt + interval.duration_as_chrono())
}

fn add_time_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let time = a.unwrap_time();
    let interval = b.unwrap_interval();
    let (t, _) = time.overflowing_add_signed(interval.duration_as_chrono());
    Datum::Time(t)
}

fn ceil_float32<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float32().ceil())
}

fn ceil_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float64().ceil())
}

fn ceil_decimal<'a>(a: Datum<'a>, scale: u8) -> Datum<'a> {
    let decimal = a.unwrap_decimal();
    Datum::from(decimal.with_scale(scale).ceil().significand())
}

fn ceil_apd<'a>(a: Datum<'a>) -> Datum<'a> {
    let mut a = a.unwrap_apd();
    let mut cx = apd::cx_datum();
    cx.set_rounding(Rounding::Ceiling);
    cx.round(&mut a.0);
    // Avoids negative 0.
    apd::munge_apd(&mut a.0).unwrap();
    Datum::APD(a)
}

fn floor_float32<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float32().floor())
}

fn floor_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float64().floor())
}

fn floor_decimal<'a>(a: Datum<'a>, scale: u8) -> Datum<'a> {
    let decimal = a.unwrap_decimal();
    Datum::from(decimal.with_scale(scale).floor().significand())
}

fn floor_apd<'a>(a: Datum<'a>) -> Datum<'a> {
    let mut a = a.unwrap_apd();
    let mut cx = apd::cx_datum();
    cx.set_rounding(Rounding::Floor);
    cx.round(&mut a.0);
    apd::munge_apd(&mut a.0).unwrap();
    Datum::APD(a)
}

fn round_float32<'a>(a: Datum<'a>) -> Datum<'a> {
    // f32::round violates IEEE 754 by rounding ties away from zero rather than
    // to nearest even. There appears to be no way to round ties to nearest even
    // in Rust natively, so bail out to C.
    extern "C" {
        fn rintf(f: f32) -> f32;
    }
    let a = a.unwrap_float32();
    Datum::from(unsafe { rintf(a) })
}

fn round_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    // f64::round violates IEEE 754 by rounding ties away from zero rather than
    // to nearest even. There appears to be no way to round ties to nearest even
    // in Rust natively, so bail out to C.
    extern "C" {
        fn rint(f: f64) -> f64;
    }
    let a = a.unwrap_float64();
    Datum::from(unsafe { rint(a) })
}

fn round_decimal_unary<'a>(a: Datum<'a>, a_scale: u8) -> Datum<'a> {
    round_decimal_binary(a, Datum::Int64(0), a_scale)
}

fn round_apd_unary<'a>(a: Datum<'a>) -> Datum<'a> {
    let mut a = a.unwrap_apd();
    apd::cx_datum().round(&mut a.0);
    Datum::APD(a)
}

fn round_decimal_binary<'a>(a: Datum<'a>, b: Datum<'a>, a_scale: u8) -> Datum<'a> {
    let round_to = b.unwrap_int64();
    let decimal = a.unwrap_decimal().with_scale(a_scale);
    Datum::from(decimal.round(round_to).significand())
}

fn round_apd_binary<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut a = a.unwrap_apd().0;
    let mut b = b.unwrap_int32();
    let mut cx = apd::cx_datum();
    let a_exp = a.exponent();
    if a_exp > 0 && b > 0 || a_exp < 0 && -a_exp < b {
        // This condition indicates:
        // - a is a value without a decimal point, b is a positive number
        // - a has a decimal point, but b is larger than its scale
        // In both of these situations, right-pad the number with zeroes, which // is most easily done with rescale.

        // Ensure rescale doesn't exceed max precision by putting a ceiling on
        // b equal to the maximum remaining scale the value can support.
        b = std::cmp::min(
            b,
            (apd::APD_DATUM_MAX_PRECISION as u32
                - (apd::get_precision(&a) - u32::from(apd::get_scale(&a)))) as i32,
        );
        cx.rescale(&mut a, &apd::Apd::from(-b));
    } else {
        // To avoid invalid operations, clamp b to be within 1 more than the
        // precision limit.
        const MAX_P_LIMIT: i32 = 1 + apd::APD_DATUM_MAX_PRECISION as i32;
        b = std::cmp::min(MAX_P_LIMIT, b);
        b = std::cmp::max(-MAX_P_LIMIT, b);
        let mut b = apd::Apd::from(b);
        // Shift by 10^b; this put digit to round to in the one's place.
        cx.scaleb(&mut a, &b);
        cx.round(&mut a);
        // Negate exponent for shift back
        cx.neg(&mut b);
        cx.scaleb(&mut a, &b);
    }

    if cx.status().overflow() {
        Err(EvalError::FloatOverflow)
    } else if a.is_zero() {
        // simpler than handling cases where exponent has gotten set to some
        // value greater than the max precision, but all significant digits
        // were rounded away.
        Ok(Datum::APD(OrderedDecimal(apd::Apd::zero())))
    } else {
        apd::munge_apd(&mut a).unwrap();
        Ok(Datum::APD(OrderedDecimal(a)))
    }
}

fn convert_from<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    // Convert PostgreSQL-style encoding names[1] to WHATWG-style encoding names[2],
    // which the encoding library uses[3].
    // [1]: https://www.postgresql.org/docs/9.5/multibyte.html
    // [2]: https://encoding.spec.whatwg.org/
    // [3]: https://github.com/lifthrasiir/rust-encoding/blob/4e79c35ab6a351881a86dbff565c4db0085cc113/src/label.rs
    let encoding_name = b.unwrap_str().to_lowercase().replace("_", "-");

    // Supporting other encodings is tracked by #2282.
    if encoding_from_whatwg_label(&encoding_name).map(|e| e.name()) != Some("utf-8") {
        return Err(EvalError::InvalidEncodingName(encoding_name));
    }

    match str::from_utf8(a.unwrap_bytes()) {
        Ok(from) => Ok(Datum::String(from)),
        Err(e) => Err(EvalError::InvalidByteSequence {
            byte_sequence: e.to_string(),
            encoding_name,
        }),
    }
}

fn encode<'a>(
    bytes: Datum<'a>,
    format: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let format = encoding::lookup_format(format.unwrap_str())?;
    let out = format.encode(bytes.unwrap_bytes());
    Ok(Datum::from(temp_storage.push_string(out)))
}

fn decode<'a>(
    string: Datum<'a>,
    format: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let format = encoding::lookup_format(format.unwrap_str())?;
    let out = format.decode(string.unwrap_str())?;
    Ok(Datum::from(temp_storage.push_bytes(out)))
}

fn bit_length<'a, B>(bytes: B) -> Result<Datum<'a>, EvalError>
where
    B: AsRef<[u8]>,
{
    match i32::try_from(bytes.as_ref().len() * 8) {
        Ok(l) => Ok(Datum::from(l)),
        Err(_) => Err(EvalError::Int32OutOfRange),
    }
}

fn byte_length<'a, B>(bytes: B) -> Result<Datum<'a>, EvalError>
where
    B: AsRef<[u8]>,
{
    match i32::try_from(bytes.as_ref().len()) {
        Ok(l) => Ok(Datum::from(l)),
        Err(_) => Err(EvalError::Int32OutOfRange),
    }
}

fn char_length<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    match i32::try_from(a.unwrap_str().chars().count()) {
        Ok(l) => Ok(Datum::from(l)),
        Err(_) => Err(EvalError::Int32OutOfRange),
    }
}

fn encoded_bytes_char_length<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    // Convert PostgreSQL-style encoding names[1] to WHATWG-style encoding names[2],
    // which the encoding library uses[3].
    // [1]: https://www.postgresql.org/docs/9.5/multibyte.html
    // [2]: https://encoding.spec.whatwg.org/
    // [3]: https://github.com/lifthrasiir/rust-encoding/blob/4e79c35ab6a351881a86dbff565c4db0085cc113/src/label.rs
    let encoding_name = b.unwrap_str().to_lowercase().replace("_", "-");

    let enc = match encoding_from_whatwg_label(&encoding_name) {
        Some(enc) => enc,
        None => return Err(EvalError::InvalidEncodingName(encoding_name)),
    };

    let decoded_string = match enc.decode(a.unwrap_bytes(), DecoderTrap::Strict) {
        Ok(s) => s,
        Err(e) => {
            return Err(EvalError::InvalidByteSequence {
                byte_sequence: e.to_string(),
                encoding_name,
            })
        }
    };

    match i32::try_from(decoded_string.chars().count()) {
        Ok(l) => Ok(Datum::from(l)),
        Err(_) => Err(EvalError::Int32OutOfRange),
    }
}

fn sub_timestamp_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    add_timestamp_interval(a, Datum::Interval(-b.unwrap_interval()))
}

fn sub_timestamptz_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    add_timestamptz_interval(a, Datum::Interval(-b.unwrap_interval()))
}

fn add_timestamp_months(dt: NaiveDateTime, months: i32) -> NaiveDateTime {
    if months == 0 {
        return dt;
    }

    let mut months = months;

    let (mut year, mut month, mut day) = (dt.year(), dt.month0() as i32, dt.day());
    let years = months / 12;
    year += years;
    months %= 12;
    // positive modulus is easier to reason about
    if months < 0 {
        year -= 1;
        months += 12;
    }
    year += (month + months) / 12;
    month = (month + months) % 12;
    // account for dt.month0
    month += 1;

    // handle going from January 31st to February by saturation
    let mut new_d = chrono::NaiveDate::from_ymd_opt(year, month as u32, day);
    while new_d.is_none() {
        debug_assert!(day > 28, "there are no months with fewer than 28 days");
        day -= 1;
        new_d = chrono::NaiveDate::from_ymd_opt(year, month as u32, day);
    }
    let new_d = new_d.unwrap();

    // Neither postgres nor mysql support leap seconds, so this should be safe.
    //
    // Both my testing and https://dba.stackexchange.com/a/105829 support the
    // idea that we should ignore leap seconds
    new_d.and_hms_nano(dt.hour(), dt.minute(), dt.second(), dt.nanosecond())
}

fn add_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_decimal() + b.unwrap_decimal())
}

fn add_apd<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut cx = apd::cx_datum();
    let mut a = a.unwrap_apd().0;
    cx.add(&mut a, &b.unwrap_apd().0);
    if cx.status().overflow() {
        Err(EvalError::FloatOverflow)
    } else {
        Ok(Datum::APD(OrderedDecimal(a)))
    }
}

fn add_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_interval()
        .checked_add(&b.unwrap_interval())
        .ok_or(EvalError::IntervalOutOfRange)
        .map(Datum::from)
}

fn sub_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int32()
        .checked_sub(b.unwrap_int32())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
}

fn sub_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int64()
        .checked_sub(b.unwrap_int64())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
}

fn sub_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float32() - b.unwrap_float32())
}

fn sub_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float64() - b.unwrap_float64())
}

fn sub_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_decimal() - b.unwrap_decimal())
}

fn sub_apd<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut cx = apd::cx_datum();
    let mut a = a.unwrap_apd().0;
    cx.sub(&mut a, &b.unwrap_apd().0);
    if cx.status().overflow() {
        Err(EvalError::FloatOverflow)
    } else {
        Ok(Datum::APD(OrderedDecimal(a)))
    }
}

fn sub_timestamp<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_timestamp() - b.unwrap_timestamp())
}

fn sub_timestamptz<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_timestamptz() - b.unwrap_timestamptz())
}

fn sub_date<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from((a.unwrap_date() - b.unwrap_date()).num_days() as i32)
}

fn sub_time<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_time() - b.unwrap_time())
}

fn sub_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_interval()
        .checked_add(&-b.unwrap_interval())
        .ok_or(EvalError::IntervalOutOfRange)
        .map(Datum::from)
}

fn sub_date_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let date = a.unwrap_date();
    let interval = b.unwrap_interval();

    let dt = NaiveDate::from_ymd(date.year(), date.month(), date.day()).and_hms(0, 0, 0);
    let dt = add_timestamp_months(dt, -interval.months);
    Datum::Timestamp(dt - interval.duration_as_chrono())
}

fn sub_time_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let time = a.unwrap_time();
    let interval = b.unwrap_interval();
    let (t, _) = time.overflowing_sub_signed(interval.duration_as_chrono());
    Datum::Time(t)
}

fn mul_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int32()
        .checked_mul(b.unwrap_int32())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
}

fn mul_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int64()
        .checked_mul(b.unwrap_int64())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
}

fn mul_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float32() * b.unwrap_float32())
}

fn mul_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float64() * b.unwrap_float64())
}

fn mul_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_decimal() * b.unwrap_decimal())
}

fn mul_apd<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut cx = apd::cx_datum();
    let mut a = a.unwrap_apd().0;
    cx.mul(&mut a, &b.unwrap_apd().0);
    let cx_status = cx.status();
    if cx_status.overflow() {
        Err(EvalError::FloatOverflow)
    } else if cx_status.subnormal() {
        Err(EvalError::FloatUnderflow)
    } else {
        apd::munge_apd(&mut a).unwrap();
        Ok(Datum::APD(OrderedDecimal(a)))
    }
}

fn mul_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_interval()
        .checked_mul(b.unwrap_float64())
        .ok_or(EvalError::IntervalOutOfRange)
        .map(Datum::from)
}

fn div_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_int32();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_int32() / b))
    }
}

fn div_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_int64();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_int64() / b))
    }
}

fn div_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_float32();
    let b = b.unwrap_float32();
    if b == 0.0 && !a.is_nan() {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a / b))
    }
}

fn div_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_float64();
    let b = b.unwrap_float64();
    if b == 0.0 && !a.is_nan() {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a / b))
    }
}

fn div_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_decimal();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_decimal() / b))
    }
}

fn div_apd<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut cx = apd::cx_datum();
    let mut a = a.unwrap_apd().0;
    let b = b.unwrap_apd().0;

    cx.div(&mut a, &b);
    let cx_status = cx.status();

    // checking the status for division by zero errors is insufficient because
    // the underlying library treats 0/0 as undefined and not division by zero.
    if b.is_zero() {
        Err(EvalError::DivisionByZero)
    } else if cx_status.overflow() {
        Err(EvalError::FloatOverflow)
    } else if cx_status.subnormal() {
        Err(EvalError::FloatUnderflow)
    } else {
        apd::munge_apd(&mut a).unwrap();
        Ok(Datum::APD(OrderedDecimal(a)))
    }
}

fn div_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_float64();
    if b == 0.0 {
        Err(EvalError::DivisionByZero)
    } else {
        a.unwrap_interval()
            .checked_div(b)
            .ok_or(EvalError::IntervalOutOfRange)
            .map(Datum::from)
    }
}

fn mod_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_int32();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_int32() % b))
    }
}

fn mod_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_int64();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_int64() % b))
    }
}

fn mod_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_float32();
    if b == 0.0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_float32() % b))
    }
}

fn mod_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_float64();
    if b == 0.0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_float64() % b))
    }
}

fn mod_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_decimal();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_decimal() % b))
    }
}

fn mod_apd<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut a = a.unwrap_apd();
    let b = b.unwrap_apd();
    if b.0.is_zero() {
        return Err(EvalError::DivisionByZero);
    }
    let mut cx = apd::cx_datum();
    // Postgres does _not_ use IEEE 754-style remainder
    cx.rem(&mut a.0, &b.0);
    apd::munge_apd(&mut a.0).unwrap();
    Ok(Datum::APD(a))
}

fn neg_int32<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(-a.unwrap_int32())
}

fn neg_int64<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(-a.unwrap_int64())
}

fn neg_float32<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(-a.unwrap_float32())
}

fn neg_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(-a.unwrap_float64())
}

fn neg_decimal<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(-a.unwrap_decimal())
}

fn neg_apd<'a>(a: Datum<'a>) -> Datum<'a> {
    let mut a = a.unwrap_apd();
    apd::cx_datum().neg(&mut a.0);
    apd::munge_apd(&mut a.0).unwrap();
    Datum::APD(a)
}

pub fn neg_interval<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(-a.unwrap_interval())
}

fn sqrt_float64<'a>(a: Datum<'a>) -> Result<Datum, EvalError> {
    let x = a.unwrap_float64();
    if x < 0.0 {
        return Err(EvalError::NegSqrt);
    }

    Ok(Datum::from(x.sqrt()))
}

fn sqrt_dec<'a>(a: Datum<'a>, scale: u8) -> Result<Datum, EvalError> {
    let d = a.unwrap_decimal();
    if d.as_i128() < 0 {
        return Err(EvalError::NegSqrt);
    }
    let d_f64 = cast_significand_to_float64(a);
    let d_scaled = d_f64.unwrap_float64() / 10_f64.powi(i32::from(scale));
    cast_float64_to_decimal(Datum::from(d_scaled.sqrt()), scale)
}

fn sqrt_apd<'a>(a: Datum<'a>) -> Result<Datum, EvalError> {
    let mut a = a.unwrap_apd();
    if a.0.is_negative() {
        return Err(EvalError::NegSqrt);
    }
    let mut cx = apd::cx_datum();
    cx.sqrt(&mut a.0);
    apd::munge_apd(&mut a.0).unwrap();
    Ok(Datum::APD(a))
}

fn cbrt_float64<'a>(a: Datum<'a>) -> Datum {
    Datum::from(a.unwrap_float64().cbrt())
}

fn cos<'a>(a: Datum<'a>) -> Result<Datum, EvalError> {
    let f = a.unwrap_float64();
    if f.is_infinite() {
        return Err(EvalError::InfinityOutOfDomain("cos".to_owned()));
    }
    Ok(Datum::from(f.cos()))
}

fn cosh<'a>(a: Datum<'a>) -> Datum {
    Datum::from(a.unwrap_float64().cosh())
}

fn sin<'a>(a: Datum<'a>) -> Result<Datum, EvalError> {
    let f = a.unwrap_float64();
    if f.is_infinite() {
        return Err(EvalError::InfinityOutOfDomain("sin".to_owned()));
    }
    Ok(Datum::from(f.sin()))
}

fn sinh<'a>(a: Datum<'a>) -> Datum {
    Datum::from(a.unwrap_float64().sinh())
}

fn tan<'a>(a: Datum<'a>) -> Result<Datum, EvalError> {
    let f = a.unwrap_float64();
    if f.is_infinite() {
        return Err(EvalError::InfinityOutOfDomain("tan".to_owned()));
    }
    Ok(Datum::from(f.tan()))
}

fn tanh<'a>(a: Datum<'a>) -> Datum {
    Datum::from(a.unwrap_float64().tanh())
}

fn cot<'a>(a: Datum<'a>) -> Result<Datum, EvalError> {
    let f = a.unwrap_float64();
    if f.is_infinite() {
        return Err(EvalError::InfinityOutOfDomain("cot".to_owned()));
    }
    Ok(Datum::from(1.0 / f.tan()))
}

fn log_guard(val: f64, function_name: &str) -> Result<f64, EvalError> {
    if val.is_sign_negative() {
        return Err(EvalError::NegativeOutOfDomain(function_name.to_owned()));
    }
    if val == 0.0 {
        return Err(EvalError::ZeroOutOfDomain(function_name.to_owned()));
    }
    Ok(val)
}

fn log_base<'a>(a: Datum<'a>, b: Datum<'a>, scale: u8) -> Result<Datum<'a>, EvalError> {
    let b = cast_significand_to_float64(b).unwrap_float64() / 10_f64.powi(i32::from(scale));
    let base = log_guard(b, "log")?;
    log_dec(a, |f| f.log(base), "log", scale)
}

fn log_dec<'a, 'b, F: Fn(f64) -> f64>(
    a: Datum<'a>,
    logic: F,
    name: &'b str,
    scale: u8,
) -> Result<Datum<'a>, EvalError> {
    let significand = cast_significand_to_float64(a);
    let a = log_guard(
        significand.unwrap_float64() / 10_f64.powi(i32::from(scale)),
        name,
    )?;
    Ok(cast_float64_to_decimal(Datum::from(logic(a)), scale)?)
}

fn log<'a, 'b, F: Fn(f64) -> f64>(
    a: Datum<'a>,
    logic: F,
    name: &'b str,
) -> Result<Datum<'a>, EvalError> {
    let f = log_guard(a.unwrap_float64(), name)?;
    Ok(Datum::from(logic(f)))
}

fn log_guard_apd(val: &Apd, function_name: &str) -> Result<(), EvalError> {
    if val.is_negative() {
        return Err(EvalError::NegativeOutOfDomain(function_name.to_owned()));
    }
    if val.is_zero() {
        return Err(EvalError::ZeroOutOfDomain(function_name.to_owned()));
    }
    Ok(())
}

// From the `decNumber` library's documentation:
// > Inexact results will almost always be correctly rounded, but may be up to 1
// > ulp (unit in last place) in error in rare cases.
//
// See decNumberLn documentation at http://speleotrove.com/decimal/dnnumb.html
fn log_base_apd<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut a = a.unwrap_apd().0;
    log_guard_apd(&a, "log")?;
    let mut b = b.unwrap_apd().0;
    log_guard_apd(&b, "log")?;
    let mut cx = apd::cx_datum();
    cx.ln(&mut a);
    cx.ln(&mut b);
    cx.div(&mut b, &a);
    if a.is_zero() {
        Err(EvalError::DivisionByZero)
    } else {
        apd::munge_apd(&mut b).unwrap();
        Ok(Datum::APD(OrderedDecimal(b)))
    }
}

// From the `decNumber` library's documentation:
// > Inexact results will almost always be correctly rounded, but may be up to 1
// > ulp (unit in last place) in error in rare cases.
//
// See decNumberLog10 documentation at http://speleotrove.com/decimal/dnnumb.html
fn log_apd<'a, 'b, F: Fn(&mut dec::Context<Apd>, &mut Apd)>(
    a: Datum<'a>,
    logic: F,
    name: &'b str,
) -> Result<Datum<'a>, EvalError> {
    let mut a = a.unwrap_apd();
    log_guard_apd(&a.0, name)?;
    let mut cx = apd::cx_datum();
    logic(&mut cx, &mut a.0);
    apd::munge_apd(&mut a.0).unwrap();
    Ok(Datum::APD(a))
}

fn exp<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    Ok(Datum::from(a.unwrap_float64().exp()))
}

fn exp_dec<'a>(a: Datum<'a>, scale: u8) -> Result<Datum<'a>, EvalError> {
    let significand = cast_significand_to_float64(a);
    let a = significand.unwrap_float64() / 10_f64.powi(i32::from(scale));
    Ok(cast_float64_to_decimal(Datum::from(a.exp()), scale)?)
}

fn exp_apd<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut a = a.unwrap_apd();
    let mut cx = apd::cx_datum();
    cx.exp(&mut a.0);
    let cx_status = cx.status();
    if cx_status.overflow() {
        Err(EvalError::FloatOverflow)
    } else if cx_status.subnormal() {
        Err(EvalError::FloatUnderflow)
    } else {
        apd::munge_apd(&mut a.0).unwrap();
        Ok(Datum::APD(a))
    }
}

fn power<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_float64();
    let b = b.unwrap_float64();
    if a == 0.0 && b.is_sign_negative() {
        return Err(EvalError::Undefined(
            "zero raised to a negative power".to_owned(),
        ));
    }
    if a.is_sign_negative() && b.fract() != 0.0 {
        // Equivalent to PG error:
        // > a negative number raised to a non-integer power yields a complex result
        return Err(EvalError::ComplexOutOfRange("pow".to_owned()));
    }
    Ok(Datum::from(a.powf(b)))
}

fn power_dec<'a>(a: Datum<'a>, b: Datum<'a>, scale: u8) -> Result<Datum<'a>, EvalError> {
    let a = cast_significand_to_float64(a).unwrap_float64() / 10_f64.powi(i32::from(scale));
    let b = cast_significand_to_float64(b).unwrap_float64() / 10_f64.powi(i32::from(scale));
    cast_float64_to_decimal(Datum::from(a.powf(b)), scale)
}

fn power_apd<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut a = a.unwrap_apd().0;
    let b = b.unwrap_apd().0;
    if a.is_zero() {
        if b.is_zero() {
            return Ok(Datum::APD(OrderedDecimal(Apd::from(1))));
        }
        if b.is_negative() {
            return Err(EvalError::Undefined(
                "zero raised to a negative power".to_owned(),
            ));
        }
    }
    if a.is_negative() && b.exponent() < 0 {
        // Equivalent to PG error:
        // > a negative number raised to a non-integer power yields a complex result
        return Err(EvalError::ComplexOutOfRange("pow".to_owned()));
    }
    let mut cx = apd::cx_datum();
    cx.pow(&mut a, &b);
    let cx_status = cx.status();
    if cx_status.overflow() {
        Err(EvalError::FloatOverflow)
    } else if cx_status.subnormal() {
        Err(EvalError::FloatUnderflow)
    } else {
        apd::munge_apd(&mut a).unwrap();
        Ok(Datum::APD(OrderedDecimal(a)))
    }
}

fn sleep<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let duration = std::time::Duration::from_secs_f64(a.unwrap_float64());
    thread::sleep(duration);
    Ok(Datum::Null)
}

fn rescale_apd<'a>(a: Datum<'a>, scale: u8) -> Result<Datum<'a>, EvalError> {
    let mut d = a.unwrap_apd();
    if apd::rescale(&mut d.0, scale).is_err() {
        return Err(EvalError::NumericFieldOverflow);
    };
    apd::munge_apd(&mut d.0).unwrap();
    Ok(Datum::APD(d))
}

fn pg_column_size<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let sz = repr::datum_size(&a);
    let sz = match i32::try_from(sz) {
        Ok(sz) => sz,
        Err(_) => return Err(EvalError::Int32OutOfRange),
    };
    Ok(Datum::Int32(sz))
}

/// Return the number of bytes this Record (List) datum would use if packed as
/// a Row.
fn mz_row_size<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let sz = repr::row_size(a.unwrap_list().iter());
    let sz = match i32::try_from(sz) {
        Ok(sz) => sz,
        Err(_) => return Err(EvalError::Int32OutOfRange),
    };
    Ok(Datum::Int32(sz))
}

fn eq<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a == b)
}

fn not_eq<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a != b)
}

fn lt<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a < b)
}

fn lte<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a <= b)
}

fn gt<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a > b)
}

fn gte<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a >= b)
}

fn to_char_timestamp<'a>(a: Datum<'a>, b: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let fmt = DateTimeFormat::compile(b.unwrap_str());
    Datum::String(temp_storage.push_string(fmt.render(a.unwrap_timestamp())))
}

fn to_char_timestamptz<'a>(a: Datum<'a>, b: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let fmt = DateTimeFormat::compile(b.unwrap_str());
    Datum::String(temp_storage.push_string(fmt.render(a.unwrap_timestamptz())))
}

fn jsonb_get_int64<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
    stringify: bool,
) -> Datum<'a> {
    let i = b.unwrap_int64();
    match a {
        Datum::List(list) => {
            let i = if i >= 0 {
                i
            } else {
                // index backwards from the end
                (list.iter().count() as i64) + i
            };
            match list.iter().nth(i as usize) {
                Some(d) if stringify => jsonb_stringify(d, temp_storage),
                Some(d) => d,
                None => Datum::Null,
            }
        }
        Datum::Map(_) => Datum::Null,
        _ => {
            if i == 0 || i == -1 {
                // I have no idea why postgres does this, but we're stuck with it
                if stringify {
                    jsonb_stringify(a, temp_storage)
                } else {
                    a
                }
            } else {
                Datum::Null
            }
        }
    }
}

fn jsonb_get_string<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
    stringify: bool,
) -> Datum<'a> {
    let k = b.unwrap_str();
    match a {
        Datum::Map(dict) => match dict.iter().find(|(k2, _v)| k == *k2) {
            Some((_k, v)) if stringify => jsonb_stringify(v, temp_storage),
            Some((_k, v)) => v,
            None => Datum::Null,
        },
        _ => Datum::Null,
    }
}

fn jsonb_get_path<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
    stringify: bool,
) -> Datum<'a> {
    let mut json = a;
    let path = b.unwrap_array().elements();
    for key in path.iter() {
        let key = match key {
            Datum::String(s) => s,
            Datum::Null => return Datum::Null,
            _ => unreachable!("keys in jsonb_get_path known to be strings"),
        };
        json = match json {
            Datum::Map(map) => match map.iter().find(|(k, _)| key == *k) {
                Some((_k, v)) => v,
                None => return Datum::Null,
            },
            Datum::List(list) => match strconv::parse_int64(key) {
                Ok(i) => {
                    let i = if i >= 0 {
                        i
                    } else {
                        // index backwards from the end
                        (list.iter().count() as i64) + i
                    };
                    list.iter().nth(i as usize).unwrap_or(Datum::Null)
                }
                Err(_) => return Datum::Null,
            },
            _ => return Datum::Null,
        }
    }
    if stringify {
        jsonb_stringify(json, temp_storage)
    } else {
        json
    }
}

fn jsonb_contains_string<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let k = b.unwrap_str();
    // https://www.postgresql.org/docs/current/datatype-json.html#JSON-CONTAINMENT
    match a {
        Datum::List(list) => list.iter().any(|k2| b == k2).into(),
        Datum::Map(dict) => dict.iter().any(|(k2, _v)| k == k2).into(),
        Datum::String(string) => (string == k).into(),
        _ => false.into(),
    }
}

fn map_contains_key<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let map = a.unwrap_map();
    let k = b.unwrap_str(); // Map keys are always text.
    map.iter().any(|(k2, _v)| k == k2).into()
}

fn map_contains_all_keys<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let map = a.unwrap_map();
    let keys = b.unwrap_array();

    keys.elements()
        .iter()
        .all(|key| !key.is_null() && map.iter().any(|(k, _v)| k == key.unwrap_str()))
        .into()
}

fn map_contains_any_keys<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let map = a.unwrap_map();
    let keys = b.unwrap_array();

    keys.elements()
        .iter()
        .any(|key| !key.is_null() && map.iter().any(|(k, _v)| k == key.unwrap_str()))
        .into()
}

fn map_contains_map<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let map_a = a.unwrap_map();
    b.unwrap_map()
        .iter()
        .all(|(b_key, b_val)| {
            map_a
                .iter()
                .any(|(a_key, a_val)| (a_key == b_key) && (a_val == b_val))
        })
        .into()
}

fn map_get_value<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let target_key = b.unwrap_str();
    match a.unwrap_map().iter().find(|(key, _v)| target_key == *key) {
        Some((_k, v)) => v,
        None => Datum::Null,
    }
}

fn map_get_values<'a>(a: Datum<'a>, b: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let map = a.unwrap_map();
    let values: Vec<Datum> = b
        .unwrap_array()
        .elements()
        .iter()
        .map(
            |target_key| match map.iter().find(|(key, _v)| target_key.unwrap_str() == *key) {
                Some((_k, v)) => v,
                None => Datum::Null,
            },
        )
        .collect();

    temp_storage.make_datum(|packer| {
        packer
            .push_array(
                &[ArrayDimension {
                    lower_bound: 1,
                    length: values.len(),
                }],
                values,
            )
            .unwrap()
    })
}

// TODO(jamii) nested loops are possibly not the fastest way to do this
fn jsonb_contains_jsonb<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    // https://www.postgresql.org/docs/current/datatype-json.html#JSON-CONTAINMENT
    fn contains(a: Datum, b: Datum, at_top_level: bool) -> bool {
        match (a, b) {
            (Datum::JsonNull, Datum::JsonNull) => true,
            (Datum::False, Datum::False) => true,
            (Datum::True, Datum::True) => true,
            (Datum::Int64(a), Datum::Int64(b)) => (a == b),
            (Datum::Int64(a), Datum::Float64(b)) => (OrderedFloat(a as f64) == b),
            (Datum::Float64(a), Datum::Int64(b)) => (a == OrderedFloat(b as f64)),
            (Datum::Float64(a), Datum::Float64(b)) => (a == b),
            (Datum::String(a), Datum::String(b)) => (a == b),
            (Datum::List(a), Datum::List(b)) => b
                .iter()
                .all(|b_elem| a.iter().any(|a_elem| contains(a_elem, b_elem, false))),
            (Datum::Map(a), Datum::Map(b)) => b.iter().all(|(b_key, b_val)| {
                a.iter()
                    .any(|(a_key, a_val)| (a_key == b_key) && contains(a_val, b_val, false))
            }),

            // fun special case
            (Datum::List(a), b) => {
                at_top_level && a.iter().any(|a_elem| contains(a_elem, b, false))
            }

            _ => false,
        }
    }
    contains(a, b, true).into()
}

fn jsonb_concat<'a>(a: Datum<'a>, b: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    match (a, b) {
        (Datum::Map(dict_a), Datum::Map(dict_b)) => {
            let mut pairs = dict_b.iter().chain(dict_a.iter()).collect::<Vec<_>>();
            // stable sort, so if keys collide dedup prefers dict_b
            pairs.sort_by(|(k1, _v1), (k2, _v2)| k1.cmp(k2));
            pairs.dedup_by(|(k1, _v1), (k2, _v2)| k1 == k2);
            temp_storage.make_datum(|packer| packer.push_dict(pairs))
        }
        (Datum::List(list_a), Datum::List(list_b)) => {
            let elems = list_a.iter().chain(list_b.iter());
            temp_storage.make_datum(|packer| packer.push_list(elems))
        }
        (Datum::List(list_a), b) => {
            let elems = list_a.iter().chain(Some(b).into_iter());
            temp_storage.make_datum(|packer| packer.push_list(elems))
        }
        (a, Datum::List(list_b)) => {
            let elems = Some(a).into_iter().chain(list_b.iter());
            temp_storage.make_datum(|packer| packer.push_list(elems))
        }
        _ => Datum::Null,
    }
}

fn jsonb_delete_int64<'a>(a: Datum<'a>, b: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let i = b.unwrap_int64();
    match a {
        Datum::List(list) => {
            let i = if i >= 0 {
                i
            } else {
                // index backwards from the end
                (list.iter().count() as i64) + i
            } as usize;
            let elems = list
                .iter()
                .enumerate()
                .filter(|(i2, _e)| i != *i2)
                .map(|(_, e)| e);
            temp_storage.make_datum(|packer| packer.push_list(elems))
        }
        _ => Datum::Null,
    }
}

fn jsonb_delete_string<'a>(a: Datum<'a>, b: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    match a {
        Datum::List(list) => {
            let elems = list.iter().filter(|e| b != *e);
            temp_storage.make_datum(|packer| packer.push_list(elems))
        }
        Datum::Map(dict) => {
            let k = b.unwrap_str();
            let pairs = dict.iter().filter(|(k2, _v)| k != *k2);
            temp_storage.make_datum(|packer| packer.push_dict(pairs))
        }
        _ => Datum::Null,
    }
}

fn ascii<'a>(a: Datum<'a>) -> Datum<'a> {
    match a.unwrap_str().chars().next() {
        None => Datum::Int32(0),
        Some(v) => Datum::Int32(v as i32),
    }
}

/// A timestamp with both a date and a time component, but not necessarily a
/// timezone component.
pub trait TimestampLike: chrono::Datelike + chrono::Timelike + for<'a> Into<Datum<'a>> {
    fn new(date: NaiveDate, time: NaiveTime) -> Self;

    /// Returns the weekday as a `usize` between 0 and 6, where 0 represents
    /// Sunday and 6 represents Saturday.
    fn weekday0(&self) -> usize {
        self.weekday().num_days_from_sunday() as usize
    }

    /// Like [`chrono::Datelike::year_ce`], but works on the ISO week system.
    fn iso_year_ce(&self) -> u32 {
        let year = self.iso_week().year();
        if year < 1 {
            (1 - year) as u32
        } else {
            year as u32
        }
    }

    fn timestamp(&self) -> i64;

    fn timestamp_subsec_micros(&self) -> u32;

    fn extract_epoch(&self) -> f64 {
        self.timestamp() as f64 + (self.timestamp_subsec_micros() as f64) / 1e6
    }

    fn extract_year(&self) -> f64 {
        f64::from(self.year())
    }

    fn extract_quarter(&self) -> f64 {
        (f64::from(self.month()) / 3.0).ceil()
    }

    fn extract_month(&self) -> f64 {
        f64::from(self.month())
    }

    fn extract_day(&self) -> f64 {
        f64::from(self.day())
    }

    fn extract_hour(&self) -> f64 {
        f64::from(self.hour())
    }

    fn extract_minute(&self) -> f64 {
        f64::from(self.minute())
    }

    fn extract_second(&self) -> f64 {
        let s = f64::from(self.second());
        let ns = f64::from(self.nanosecond()) / 1e9;
        s + ns
    }

    fn extract_millisecond(&self) -> f64 {
        let s = f64::from(self.second() * 1_000);
        let ns = f64::from(self.nanosecond()) / 1e6;
        s + ns
    }

    fn extract_microsecond(&self) -> f64 {
        let s = f64::from(self.second() * 1_000_000);
        let ns = f64::from(self.nanosecond()) / 1e3;
        s + ns
    }

    fn extract_millennium(&self) -> f64 {
        f64::from((self.year() + if self.year() > 0 { 999 } else { -1_000 }) / 1_000)
    }

    fn extract_century(&self) -> f64 {
        f64::from((self.year() + if self.year() > 0 { 99 } else { -100 }) / 100)
    }

    fn extract_decade(&self) -> f64 {
        f64::from(self.year().div_euclid(10))
    }

    /// Extract the iso week of the year
    ///
    /// Note that because isoweeks are defined in terms of January 4th, Jan 1 is only in week
    /// 1 about half of the time
    fn extract_week(&self) -> f64 {
        f64::from(self.iso_week().week())
    }

    fn extract_dayofyear(&self) -> f64 {
        f64::from(self.ordinal())
    }

    fn extract_dayofweek(&self) -> f64 {
        f64::from(self.weekday().num_days_from_sunday())
    }

    fn extract_isodayofweek(&self) -> f64 {
        f64::from(self.weekday().number_from_monday())
    }

    fn truncate_microseconds(&self) -> Self {
        let time = NaiveTime::from_hms_micro(
            self.hour(),
            self.minute(),
            self.second(),
            self.nanosecond() / 1_000,
        );

        Self::new(self.date(), time)
    }

    fn truncate_milliseconds(&self) -> Self {
        let time = NaiveTime::from_hms_milli(
            self.hour(),
            self.minute(),
            self.second(),
            self.nanosecond() / 1_000_000,
        );

        Self::new(self.date(), time)
    }

    fn truncate_second(&self) -> Self {
        let time = NaiveTime::from_hms(self.hour(), self.minute(), self.second());

        Self::new(self.date(), time)
    }

    fn truncate_minute(&self) -> Self {
        Self::new(
            self.date(),
            NaiveTime::from_hms(self.hour(), self.minute(), 0),
        )
    }

    fn truncate_hour(&self) -> Self {
        Self::new(self.date(), NaiveTime::from_hms(self.hour(), 0, 0))
    }

    fn truncate_day(&self) -> Self {
        Self::new(self.date(), NaiveTime::from_hms(0, 0, 0))
    }

    fn truncate_week(&self) -> Result<Self, EvalError> {
        let num_days_from_monday = self.date().weekday().num_days_from_monday() as i64;
        let new_date = NaiveDate::from_ymd(self.year(), self.month(), self.day())
            .checked_sub_signed(Duration::days(num_days_from_monday))
            .ok_or(EvalError::TimestampOutOfRange)?;
        Ok(Self::new(new_date, NaiveTime::from_hms(0, 0, 0)))
    }

    fn truncate_month(&self) -> Self {
        Self::new(
            NaiveDate::from_ymd(self.year(), self.month(), 1),
            NaiveTime::from_hms(0, 0, 0),
        )
    }

    fn truncate_quarter(&self) -> Self {
        let month = self.month();
        let quarter = if month <= 3 {
            1
        } else if month <= 6 {
            4
        } else if month <= 9 {
            7
        } else {
            10
        };

        Self::new(
            NaiveDate::from_ymd(self.year(), quarter, 1),
            NaiveTime::from_hms(0, 0, 0),
        )
    }

    fn truncate_year(&self) -> Self {
        Self::new(
            NaiveDate::from_ymd(self.year(), 1, 1),
            NaiveTime::from_hms(0, 0, 0),
        )
    }
    fn truncate_decade(&self) -> Self {
        Self::new(
            NaiveDate::from_ymd(self.year() - self.year().rem_euclid(10), 1, 1),
            NaiveTime::from_hms(0, 0, 0),
        )
    }
    fn truncate_century(&self) -> Self {
        // Expects the first year of the century, meaning 2001 instead of 2000.
        Self::new(
            NaiveDate::from_ymd(
                if self.year() > 0 {
                    self.year() - (self.year() - 1) % 100
                } else {
                    self.year() - self.year() % 100 - 99
                },
                1,
                1,
            ),
            NaiveTime::from_hms(0, 0, 0),
        )
    }
    fn truncate_millennium(&self) -> Self {
        // Expects the first year of the millennium, meaning 2001 instead of 2000.
        Self::new(
            NaiveDate::from_ymd(
                if self.year() > 0 {
                    self.year() - (self.year() - 1) % 1000
                } else {
                    self.year() - self.year() % 1000 - 999
                },
                1,
                1,
            ),
            NaiveTime::from_hms(0, 0, 0),
        )
    }

    /// Return the date component of the timestamp
    fn date(&self) -> NaiveDate;

    /// Returns a string representing the timezone's offset from UTC.
    fn timezone_offset(&self) -> &'static str;

    /// Returns a string representing the hour portion of the timezone's offset
    /// from UTC.
    fn timezone_hours(&self) -> &'static str;

    /// Returns a string representing the minute portion of the timezone's
    /// offset from UTC.
    fn timezone_minutes(&self) -> &'static str;

    /// Returns the abbreviated name of the timezone with the specified
    /// capitalization.
    fn timezone_name(&self, caps: bool) -> &'static str;
}

impl TimestampLike for chrono::NaiveDateTime {
    fn new(date: NaiveDate, time: NaiveTime) -> Self {
        NaiveDateTime::new(date, time)
    }

    fn date(&self) -> NaiveDate {
        self.date()
    }

    fn timestamp(&self) -> i64 {
        self.timestamp()
    }

    fn timestamp_subsec_micros(&self) -> u32 {
        self.timestamp_subsec_micros()
    }

    fn timezone_offset(&self) -> &'static str {
        "+00"
    }

    fn timezone_hours(&self) -> &'static str {
        "+00"
    }

    fn timezone_minutes(&self) -> &'static str {
        "00"
    }

    fn timezone_name(&self, _caps: bool) -> &'static str {
        ""
    }
}

impl TimestampLike for chrono::DateTime<chrono::Utc> {
    fn new(date: NaiveDate, time: NaiveTime) -> Self {
        DateTime::<Utc>::from_utc(NaiveDateTime::new(date, time), Utc)
    }

    fn date(&self) -> NaiveDate {
        self.naive_utc().date()
    }

    fn timestamp(&self) -> i64 {
        self.timestamp()
    }

    fn timestamp_subsec_micros(&self) -> u32 {
        self.timestamp_subsec_micros()
    }

    fn timezone_offset(&self) -> &'static str {
        "+00"
    }

    fn timezone_hours(&self) -> &'static str {
        "+00"
    }

    fn timezone_minutes(&self) -> &'static str {
        "00"
    }

    fn timezone_name(&self, caps: bool) -> &'static str {
        if caps {
            "UTC"
        } else {
            "utc"
        }
    }
}

fn date_part_interval<'a>(a: Datum<'a>, interval: Interval) -> Result<Datum<'a>, EvalError> {
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => date_part_interval_inner(units, interval),
        Err(_) => Err(EvalError::UnknownUnits(units.to_owned())),
    }
}

fn date_part_interval_inner(
    units: DateTimeUnits,
    interval: Interval,
) -> Result<Datum<'static>, EvalError> {
    match units {
        DateTimeUnits::Epoch => Ok(interval.as_seconds().into()),
        DateTimeUnits::Year => Ok(interval.years().into()),
        DateTimeUnits::Day => Ok(interval.days().into()),
        DateTimeUnits::Hour => Ok(interval.hours().into()),
        DateTimeUnits::Minute => Ok(interval.minutes().into()),
        DateTimeUnits::Second => Ok(interval.seconds().into()),
        DateTimeUnits::Millennium => Ok(interval.millennia().into()),
        DateTimeUnits::Century => Ok(interval.centuries().into()),
        DateTimeUnits::Decade => Ok(interval.decades().into()),
        DateTimeUnits::Quarter => Ok(interval.quarters().into()),
        DateTimeUnits::Month => Ok(interval.months().into()),
        DateTimeUnits::Milliseconds => Ok(interval.milliseconds().into()),
        DateTimeUnits::Microseconds => Ok(interval.microseconds().into()),
        DateTimeUnits::Week
        | DateTimeUnits::Timezone
        | DateTimeUnits::TimezoneHour
        | DateTimeUnits::TimezoneMinute
        | DateTimeUnits::DayOfWeek
        | DateTimeUnits::DayOfYear
        | DateTimeUnits::IsoDayOfWeek
        | DateTimeUnits::IsoDayOfYear => Err(EvalError::UnsupportedDateTimeUnits(units)),
    }
}

fn date_part_timestamp<'a, T>(a: Datum<'a>, ts: T) -> Result<Datum<'a>, EvalError>
where
    T: TimestampLike,
{
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => date_part_timestamp_inner(units, ts),
        Err(_) => Err(EvalError::UnknownUnits(units.to_owned())),
    }
}

fn date_part_timestamp_inner<'a, T>(units: DateTimeUnits, ts: T) -> Result<Datum<'a>, EvalError>
where
    T: TimestampLike,
{
    match units {
        DateTimeUnits::Epoch => Ok(ts.extract_epoch().into()),
        DateTimeUnits::Year => Ok(ts.extract_year().into()),
        DateTimeUnits::Quarter => Ok(ts.extract_quarter().into()),
        DateTimeUnits::Week => Ok(ts.extract_week().into()),
        DateTimeUnits::Day => Ok(ts.extract_day().into()),
        DateTimeUnits::DayOfWeek => Ok(ts.extract_dayofweek().into()),
        DateTimeUnits::DayOfYear => Ok(ts.extract_dayofyear().into()),
        DateTimeUnits::IsoDayOfWeek => Ok(ts.extract_isodayofweek().into()),
        DateTimeUnits::Hour => Ok(ts.extract_hour().into()),
        DateTimeUnits::Minute => Ok(ts.extract_minute().into()),
        DateTimeUnits::Second => Ok(ts.extract_second().into()),
        DateTimeUnits::Month => Ok(ts.extract_month().into()),
        DateTimeUnits::Milliseconds => Ok(ts.extract_millisecond().into()),
        DateTimeUnits::Microseconds => Ok(ts.extract_microsecond().into()),
        DateTimeUnits::Millennium => Ok(ts.extract_millennium().into()),
        DateTimeUnits::Century => Ok(ts.extract_century().into()),
        DateTimeUnits::Decade => Ok(ts.extract_decade().into()),
        DateTimeUnits::Timezone
        | DateTimeUnits::TimezoneHour
        | DateTimeUnits::TimezoneMinute
        | DateTimeUnits::IsoDayOfYear => Err(EvalError::UnsupportedDateTimeUnits(units)),
    }
}

fn date_trunc<'a, T>(a: Datum<'a>, ts: T) -> Result<Datum<'a>, EvalError>
where
    T: TimestampLike,
{
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => date_trunc_inner(units, ts),
        Err(_) => Err(EvalError::UnknownUnits(units.to_owned())),
    }
}

fn date_trunc_inner<'a, T>(units: DateTimeUnits, ts: T) -> Result<Datum<'a>, EvalError>
where
    T: TimestampLike,
{
    match units {
        DateTimeUnits::Millennium => Ok(ts.truncate_millennium().into()),
        DateTimeUnits::Century => Ok(ts.truncate_century().into()),
        DateTimeUnits::Decade => Ok(ts.truncate_decade().into()),
        DateTimeUnits::Year => Ok(ts.truncate_year().into()),
        DateTimeUnits::Quarter => Ok(ts.truncate_quarter().into()),
        DateTimeUnits::Week => Ok(ts.truncate_week()?.into()),
        DateTimeUnits::Day => Ok(ts.truncate_day().into()),
        DateTimeUnits::Hour => Ok(ts.truncate_hour().into()),
        DateTimeUnits::Minute => Ok(ts.truncate_minute().into()),
        DateTimeUnits::Second => Ok(ts.truncate_second().into()),
        DateTimeUnits::Month => Ok(ts.truncate_month().into()),
        DateTimeUnits::Milliseconds => Ok(ts.truncate_milliseconds().into()),
        DateTimeUnits::Microseconds => Ok(ts.truncate_microseconds().into()),
        DateTimeUnits::Epoch
        | DateTimeUnits::Timezone
        | DateTimeUnits::TimezoneHour
        | DateTimeUnits::TimezoneMinute
        | DateTimeUnits::DayOfWeek
        | DateTimeUnits::DayOfYear
        | DateTimeUnits::IsoDayOfWeek
        | DateTimeUnits::IsoDayOfYear => Err(EvalError::UnsupportedDateTimeUnits(units)),
    }
}

/// Parses a named timezone like `EST` or `America/New_York`, or a fixed-offset timezone like `-05:00`.
pub(crate) fn parse_timezone(tz: &str) -> Result<Timezone, EvalError> {
    tz.parse()
        .map_err(|_| EvalError::InvalidTimezone(tz.to_owned()))
}

/// Converts the time `t`, which is assumed to be in UTC, to the timezone `tz`.
/// For example, `EST` and `17:39:14` would return `12:39:14`.
fn timezone_time(tz: Timezone, t: NaiveTime, wall_time: &NaiveDateTime) -> Datum<'static> {
    let offset = match tz {
        Timezone::FixedOffset(offset) => offset,
        Timezone::Tz(tz) => tz.offset_from_utc_datetime(&wall_time).fix(),
    };
    (t + offset).into()
}

/// Converts the timestamp `dt`, which is assumed to be in the time of the timezone `tz` to a timestamptz in UTC.
/// This operation is fallible because certain timestamps at timezones that observe DST are simply impossible or
/// ambiguous. In case of ambiguity (when a hour repeats) we will prefer the latest variant, and when an hour is
/// impossible, we will attempt to fix it by advancing it. For example, `EST` and `2020-11-11T12:39:14` would return
/// `2020-11-11T17:39:14Z`. A DST observing timezone like `America/New_York` would cause the following DST anomalies:
/// `2020-11-01T00:59:59` -> `2020-11-01T04:59:59Z` and `2020-11-01T01:00:00` -> `2020-11-01T06:00:00Z`
/// `2020-03-08T02:59:59` -> `2020-03-08T07:59:59Z` and `2020-03-08T03:00:00` -> `2020-03-08T07:00:00Z`
fn timezone_timestamp(tz: Timezone, mut dt: NaiveDateTime) -> Result<Datum<'static>, EvalError> {
    let offset = match tz {
        Timezone::FixedOffset(offset) => offset,
        Timezone::Tz(tz) => match tz.offset_from_local_datetime(&dt).latest() {
            Some(offset) => offset.fix(),
            None => {
                dt += Duration::hours(1);
                tz.offset_from_local_datetime(&dt)
                    .latest()
                    .ok_or(EvalError::InvalidTimezoneConversion)?
                    .fix()
            }
        },
    };
    Ok(DateTime::from_utc(dt - offset, Utc).into())
}

/// Converts the UTC timestamptz `utc` to the local timestamp of the timezone `tz`.
/// For example, `EST` and `2020-11-11T17:39:14Z` would return `2020-11-11T12:39:14`.
fn timezone_timestamptz(tz: Timezone, utc: DateTime<Utc>) -> Datum<'static> {
    let offset = match tz {
        Timezone::FixedOffset(offset) => offset,
        Timezone::Tz(tz) => tz.offset_from_utc_datetime(&utc.naive_utc()).fix(),
    };
    (utc.naive_utc() + offset).into()
}

/// Converts the time datum `b`, which is assumed to be in UTC, to the timezone that the interval datum `a` is assumed
/// to represent. The interval is not allowed to hold months, but there are no limits on the amount of seconds.
/// The interval acts like a `chrono::FixedOffset`, without the `-86,400 < x < 86,400` limitation.
fn timezone_interval_time(a: Datum<'_>, b: Datum<'_>) -> Result<Datum<'static>, EvalError> {
    let interval = a.unwrap_interval();
    if interval.months != 0 {
        Err(EvalError::InvalidTimezoneInterval)
    } else {
        Ok(b.unwrap_time()
            .overflowing_add_signed(interval.duration_as_chrono())
            .0
            .into())
    }
}

/// Converts the timestamp datum `b`, which is assumed to be in the time of the timezone datum `a` to a timestamptz
/// in UTC. The interval is not allowed to hold months, but there are no limits on the amount of seconds.
/// The interval acts like a `chrono::FixedOffset`, without the `-86,400 < x < 86,400` limitation.
fn timezone_interval_timestamp(a: Datum<'_>, b: Datum<'_>) -> Result<Datum<'static>, EvalError> {
    let interval = a.unwrap_interval();
    if interval.months != 0 {
        Err(EvalError::InvalidTimezoneInterval)
    } else {
        Ok(DateTime::from_utc(b.unwrap_timestamp() - interval.duration_as_chrono(), Utc).into())
    }
}

/// Converts the UTC timestamptz datum `b`, to the local timestamp of the timezone datum `a`.
/// The interval is not allowed to hold months, but there are no limits on the amount of seconds.
/// The interval acts like a `chrono::FixedOffset`, without the `-86,400 < x < 86,400` limitation.
fn timezone_interval_timestamptz(a: Datum<'_>, b: Datum<'_>) -> Result<Datum<'static>, EvalError> {
    let interval = a.unwrap_interval();
    if interval.months != 0 {
        Err(EvalError::InvalidTimezoneInterval)
    } else {
        Ok((b.unwrap_timestamptz().naive_utc() + interval.duration_as_chrono()).into())
    }
}

fn to_timestamp<'a>(a: Datum<'a>) -> Datum<'a> {
    let f = a.unwrap_float64();
    if !f.is_finite() {
        return Datum::Null;
    }
    let secs = f.trunc() as i64;
    // NOTE(benesch): PostgreSQL has microsecond precision in its timestamps,
    // while chrono has nanosecond precision. While we normally accept
    // nanosecond precision, here we round to the nearest microsecond because
    // f64s lose quite a bit of accuracy in the nanosecond digits when dealing
    // with common Unix timestamp values (> 1 billion).
    let nanosecs = ((f.fract() * 1_000_000.0).round() as u32) * 1_000;
    match NaiveDateTime::from_timestamp_opt(secs as i64, nanosecs as u32) {
        None => Datum::Null,
        Some(ts) => Datum::TimestampTz(DateTime::<Utc>::from_utc(ts, Utc)),
    }
}

fn jsonb_array_length<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::List(list) => Datum::Int64(list.iter().count() as i64),
        _ => Datum::Null,
    }
}

fn jsonb_typeof<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Map(_) => Datum::String("object"),
        Datum::List(_) => Datum::String("array"),
        Datum::String(_) => Datum::String("string"),
        Datum::Float64(_) => Datum::String("number"),
        Datum::True | Datum::False => Datum::String("boolean"),
        Datum::JsonNull => Datum::String("null"),
        Datum::Null => Datum::Null,
        _ => panic!("Not jsonb: {:?}", a),
    }
}

fn jsonb_strip_nulls<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    fn strip_nulls(a: Datum, row: &mut Row) {
        match a {
            Datum::Map(dict) => row.push_dict_with(|row| {
                for (k, v) in dict.iter() {
                    match v {
                        Datum::JsonNull => (),
                        _ => {
                            row.push(Datum::String(k));
                            strip_nulls(v, row);
                        }
                    }
                }
            }),
            Datum::List(list) => row.push_list_with(|row| {
                for elem in list.iter() {
                    strip_nulls(elem, row);
                }
            }),
            _ => row.push(a),
        }
    }
    temp_storage.make_datum(|row| strip_nulls(a, row))
}

fn jsonb_pretty<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_jsonb_pretty(&mut buf, JsonbRef::from_datum(a));
    Datum::String(temp_storage.push_string(buf))
}

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzEnumReflect,
)]
pub enum BinaryFunc {
    And,
    Or,
    AddInt32,
    AddInt64,
    AddFloat32,
    AddFloat64,
    AddInterval,
    AddTimestampInterval,
    AddTimestampTzInterval,
    AddDateInterval,
    AddDateTime,
    AddTimeInterval,
    AddDecimal,
    AddAPD,
    SubInt32,
    SubInt64,
    SubFloat32,
    SubFloat64,
    SubInterval,
    SubTimestamp,
    SubTimestampTz,
    SubTimestampInterval,
    SubTimestampTzInterval,
    SubDate,
    SubDateInterval,
    SubTime,
    SubTimeInterval,
    SubDecimal,
    SubAPD,
    MulInt32,
    MulInt64,
    MulFloat32,
    MulFloat64,
    MulDecimal,
    MulAPD,
    MulInterval,
    DivInt32,
    DivInt64,
    DivFloat32,
    DivFloat64,
    DivDecimal,
    DivAPD,
    DivInterval,
    ModInt32,
    ModInt64,
    ModFloat32,
    ModFloat64,
    ModDecimal,
    ModAPD,
    RoundDecimal(u8),
    RoundAPD,
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    IsLikePatternMatch { case_insensitive: bool },
    IsRegexpMatch { case_insensitive: bool },
    ToCharTimestamp,
    ToCharTimestampTz,
    DatePartInterval,
    DatePartTimestamp,
    DatePartTimestampTz,
    DateTruncTimestamp,
    DateTruncTimestampTz,
    TimezoneTimestamp,
    TimezoneTimestampTz,
    TimezoneTime { wall_time: NaiveDateTime },
    TimezoneIntervalTimestamp,
    TimezoneIntervalTimestampTz,
    TimezoneIntervalTime,
    TextConcat,
    JsonbGetInt64 { stringify: bool },
    JsonbGetString { stringify: bool },
    JsonbGetPath { stringify: bool },
    JsonbContainsString,
    JsonbConcat,
    JsonbContainsJsonb,
    JsonbDeleteInt64,
    JsonbDeleteString,
    MapContainsKey,
    MapGetValue,
    MapGetValues,
    MapContainsAllKeys,
    MapContainsAnyKeys,
    MapContainsMap,
    ConvertFrom,
    Position,
    Right,
    RepeatString,
    Trim,
    TrimLeading,
    TrimTrailing,
    EncodedBytesCharLength,
    ListIndex,
    ListLengthMax { max_dim: usize },
    ArrayContains,
    ArrayIndex,
    ArrayLength,
    ArrayLower,
    ArrayUpper,
    ListListConcat,
    ListElementConcat,
    ElementListConcat,
    DigestString,
    DigestBytes,
    MzRenderTypemod,
    Encode,
    Decode,
    LogDecimal(u8),
    LogAPD,
    Power,
    PowerDecimal(u8),
    PowerAPD,
}

impl BinaryFunc {
    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a_expr: &'a MirScalarExpr,
        b_expr: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        macro_rules! eager {
            ($func:expr $(, $args:expr)*) => {{
                let a = a_expr.eval(datums, temp_storage)?;
                let b = b_expr.eval(datums, temp_storage)?;
                if self.propagates_nulls() && (a.is_null() || b.is_null()) {
                    return Ok(Datum::Null);
                }
                $func(a, b $(, $args)*)
            }}
        }

        match self {
            BinaryFunc::And => and(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::Or => or(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::AddInt32 => eager!(add_int32),
            BinaryFunc::AddInt64 => eager!(add_int64),
            BinaryFunc::AddFloat32 => Ok(eager!(add_float32)),
            BinaryFunc::AddFloat64 => Ok(eager!(add_float64)),
            BinaryFunc::AddTimestampInterval => Ok(eager!(add_timestamp_interval)),
            BinaryFunc::AddTimestampTzInterval => Ok(eager!(add_timestamptz_interval)),
            BinaryFunc::AddDateTime => Ok(eager!(add_date_time)),
            BinaryFunc::AddDateInterval => Ok(eager!(add_date_interval)),
            BinaryFunc::AddTimeInterval => Ok(eager!(add_time_interval)),
            BinaryFunc::AddDecimal => Ok(eager!(add_decimal)),
            BinaryFunc::AddAPD => eager!(add_apd),
            BinaryFunc::AddInterval => eager!(add_interval),
            BinaryFunc::SubInt32 => eager!(sub_int32),
            BinaryFunc::SubInt64 => eager!(sub_int64),
            BinaryFunc::SubFloat32 => Ok(eager!(sub_float32)),
            BinaryFunc::SubFloat64 => Ok(eager!(sub_float64)),
            BinaryFunc::SubTimestamp => Ok(eager!(sub_timestamp)),
            BinaryFunc::SubTimestampTz => Ok(eager!(sub_timestamptz)),
            BinaryFunc::SubTimestampInterval => Ok(eager!(sub_timestamp_interval)),
            BinaryFunc::SubTimestampTzInterval => Ok(eager!(sub_timestamptz_interval)),
            BinaryFunc::SubInterval => eager!(sub_interval),
            BinaryFunc::SubDate => Ok(eager!(sub_date)),
            BinaryFunc::SubDateInterval => Ok(eager!(sub_date_interval)),
            BinaryFunc::SubTime => Ok(eager!(sub_time)),
            BinaryFunc::SubTimeInterval => Ok(eager!(sub_time_interval)),
            BinaryFunc::SubDecimal => Ok(eager!(sub_decimal)),
            BinaryFunc::SubAPD => eager!(sub_apd),
            BinaryFunc::MulInt32 => eager!(mul_int32),
            BinaryFunc::MulInt64 => eager!(mul_int64),
            BinaryFunc::MulFloat32 => Ok(eager!(mul_float32)),
            BinaryFunc::MulFloat64 => Ok(eager!(mul_float64)),
            BinaryFunc::MulDecimal => Ok(eager!(mul_decimal)),
            BinaryFunc::MulAPD => eager!(mul_apd),
            BinaryFunc::MulInterval => eager!(mul_interval),
            BinaryFunc::DivInt32 => eager!(div_int32),
            BinaryFunc::DivInt64 => eager!(div_int64),
            BinaryFunc::DivFloat32 => eager!(div_float32),
            BinaryFunc::DivFloat64 => eager!(div_float64),
            BinaryFunc::DivDecimal => eager!(div_decimal),
            BinaryFunc::DivAPD => eager!(div_apd),
            BinaryFunc::DivInterval => eager!(div_interval),
            BinaryFunc::ModInt32 => eager!(mod_int32),
            BinaryFunc::ModInt64 => eager!(mod_int64),
            BinaryFunc::ModFloat32 => eager!(mod_float32),
            BinaryFunc::ModFloat64 => eager!(mod_float64),
            BinaryFunc::ModDecimal => eager!(mod_decimal),
            BinaryFunc::ModAPD => eager!(mod_apd),
            BinaryFunc::Eq => Ok(eager!(eq)),
            BinaryFunc::NotEq => Ok(eager!(not_eq)),
            BinaryFunc::Lt => Ok(eager!(lt)),
            BinaryFunc::Lte => Ok(eager!(lte)),
            BinaryFunc::Gt => Ok(eager!(gt)),
            BinaryFunc::Gte => Ok(eager!(gte)),
            BinaryFunc::IsLikePatternMatch { case_insensitive } => {
                eager!(is_like_pattern_match_dynamic, *case_insensitive)
            }
            BinaryFunc::IsRegexpMatch { case_insensitive } => {
                eager!(is_regexp_match_dynamic, *case_insensitive)
            }
            BinaryFunc::ToCharTimestamp => Ok(eager!(to_char_timestamp, temp_storage)),
            BinaryFunc::ToCharTimestampTz => Ok(eager!(to_char_timestamptz, temp_storage)),
            BinaryFunc::DatePartInterval => {
                eager!(|a, b: Datum| date_part_interval(a, b.unwrap_interval()))
            }
            BinaryFunc::DatePartTimestamp => {
                eager!(|a, b: Datum| date_part_timestamp(a, b.unwrap_timestamp()))
            }
            BinaryFunc::DatePartTimestampTz => {
                eager!(|a, b: Datum| date_part_timestamp(a, b.unwrap_timestamptz()))
            }
            BinaryFunc::DateTruncTimestamp => {
                eager!(|a, b: Datum| date_trunc(a, b.unwrap_timestamp()))
            }
            BinaryFunc::DateTruncTimestampTz => {
                eager!(|a, b: Datum| date_trunc(a, b.unwrap_timestamptz()))
            }
            BinaryFunc::TimezoneTimestamp => {
                eager!(|a: Datum, b: Datum| parse_timezone(a.unwrap_str())
                    .and_then(|tz| timezone_timestamp(tz, b.unwrap_timestamp())))
            }
            BinaryFunc::TimezoneTimestampTz => {
                eager!(|a: Datum, b: Datum| parse_timezone(a.unwrap_str())
                    .map(|tz| timezone_timestamptz(tz, b.unwrap_timestamptz())))
            }
            BinaryFunc::TimezoneTime { wall_time } => {
                eager!(
                    |a: Datum, b: Datum| parse_timezone(a.unwrap_str()).map(|tz| timezone_time(
                        tz,
                        b.unwrap_time(),
                        wall_time
                    ))
                )
            }
            BinaryFunc::TimezoneIntervalTimestamp => eager!(timezone_interval_timestamp),
            BinaryFunc::TimezoneIntervalTimestampTz => eager!(timezone_interval_timestamptz),
            BinaryFunc::TimezoneIntervalTime => eager!(timezone_interval_time),
            BinaryFunc::TextConcat => Ok(eager!(text_concat_binary, temp_storage)),
            BinaryFunc::JsonbGetInt64 { stringify } => {
                Ok(eager!(jsonb_get_int64, temp_storage, *stringify))
            }
            BinaryFunc::JsonbGetString { stringify } => {
                Ok(eager!(jsonb_get_string, temp_storage, *stringify))
            }
            BinaryFunc::JsonbGetPath { stringify } => {
                Ok(eager!(jsonb_get_path, temp_storage, *stringify))
            }
            BinaryFunc::JsonbContainsString => Ok(eager!(jsonb_contains_string)),
            BinaryFunc::JsonbConcat => Ok(eager!(jsonb_concat, temp_storage)),
            BinaryFunc::JsonbContainsJsonb => Ok(eager!(jsonb_contains_jsonb)),
            BinaryFunc::JsonbDeleteInt64 => Ok(eager!(jsonb_delete_int64, temp_storage)),
            BinaryFunc::JsonbDeleteString => Ok(eager!(jsonb_delete_string, temp_storage)),
            BinaryFunc::MapContainsKey => Ok(eager!(map_contains_key)),
            BinaryFunc::MapGetValue => Ok(eager!(map_get_value)),
            BinaryFunc::MapGetValues => Ok(eager!(map_get_values, temp_storage)),
            BinaryFunc::MapContainsAllKeys => Ok(eager!(map_contains_all_keys)),
            BinaryFunc::MapContainsAnyKeys => Ok(eager!(map_contains_any_keys)),
            BinaryFunc::MapContainsMap => Ok(eager!(map_contains_map)),
            BinaryFunc::RoundDecimal(scale) => Ok(eager!(round_decimal_binary, *scale)),
            BinaryFunc::RoundAPD => eager!(round_apd_binary),
            BinaryFunc::ConvertFrom => eager!(convert_from),
            BinaryFunc::Encode => eager!(encode, temp_storage),
            BinaryFunc::Decode => eager!(decode, temp_storage),
            BinaryFunc::Position => eager!(position),
            BinaryFunc::Right => eager!(right),
            BinaryFunc::Trim => Ok(eager!(trim)),
            BinaryFunc::TrimLeading => Ok(eager!(trim_leading)),
            BinaryFunc::TrimTrailing => Ok(eager!(trim_trailing)),
            BinaryFunc::EncodedBytesCharLength => eager!(encoded_bytes_char_length),
            BinaryFunc::ListIndex => Ok(eager!(list_index)),
            BinaryFunc::ListLengthMax { max_dim } => eager!(list_length_max, *max_dim),
            BinaryFunc::ArrayLength => Ok(eager!(array_length)),
            BinaryFunc::ArrayContains => Ok(eager!(array_contains)),
            BinaryFunc::ArrayIndex => Ok(eager!(array_index)),
            BinaryFunc::ArrayLower => Ok(eager!(array_lower)),
            BinaryFunc::ArrayUpper => Ok(eager!(array_upper)),
            BinaryFunc::ListListConcat => Ok(eager!(list_list_concat, temp_storage)),
            BinaryFunc::ListElementConcat => Ok(eager!(list_element_concat, temp_storage)),
            BinaryFunc::ElementListConcat => Ok(eager!(element_list_concat, temp_storage)),
            BinaryFunc::DigestString => eager!(digest_string, temp_storage),
            BinaryFunc::DigestBytes => eager!(digest_bytes, temp_storage),
            BinaryFunc::MzRenderTypemod => Ok(eager!(mz_render_typemod, temp_storage)),
            BinaryFunc::LogDecimal(scale) => eager!(log_base, *scale),
            BinaryFunc::LogAPD => eager!(log_base_apd),
            BinaryFunc::Power => eager!(power),
            BinaryFunc::PowerDecimal(scale) => eager!(power_dec, *scale),
            BinaryFunc::PowerAPD => eager!(power_apd),
            BinaryFunc::RepeatString => eager!(repeat_string, temp_storage),
        }
    }

    pub fn output_type(&self, input1_type: ColumnType, input2_type: ColumnType) -> ColumnType {
        use BinaryFunc::*;
        let in_nullable = input1_type.nullable || input2_type.nullable;
        let is_div_mod = matches!(
            self,
            DivInt32
                | ModInt32
                | DivInt64
                | ModInt64
                | DivFloat32
                | ModFloat32
                | DivFloat64
                | ModFloat64
                | DivDecimal
                | DivAPD
                | ModDecimal
                | ModAPD
        );
        match self {
            And | Or | Eq | NotEq | Lt | Lte | Gt | Gte | ArrayContains => {
                ScalarType::Bool.nullable(in_nullable)
            }

            IsLikePatternMatch { .. } | IsRegexpMatch { .. } => {
                // The output can be null if the pattern is invalid.
                ScalarType::Bool.nullable(true)
            }

            ToCharTimestamp | ToCharTimestampTz | ConvertFrom | Right | Trim | TrimLeading
            | TrimTrailing => ScalarType::String.nullable(in_nullable),

            AddInt32
            | SubInt32
            | MulInt32
            | DivInt32
            | ModInt32
            | EncodedBytesCharLength
            | SubDate => ScalarType::Int32.nullable(in_nullable || is_div_mod),

            AddInt64 | SubInt64 | MulInt64 | DivInt64 | ModInt64 => {
                ScalarType::Int64.nullable(in_nullable || is_div_mod)
            }

            AddFloat32 | SubFloat32 | MulFloat32 | DivFloat32 | ModFloat32 => {
                ScalarType::Float32.nullable(in_nullable || is_div_mod)
            }

            AddFloat64 | SubFloat64 | MulFloat64 | DivFloat64 | ModFloat64 => {
                ScalarType::Float64.nullable(in_nullable || is_div_mod)
            }

            AddInterval | SubInterval | SubTimestamp | SubTimestampTz | MulInterval
            | DivInterval => ScalarType::Interval.nullable(in_nullable),

            // TODO(benesch): we correctly compute types for decimal scale, but
            // not decimal precision... because nothing actually cares about
            // decimal precision. Should either remove or fix.
            AddDecimal | SubDecimal | ModDecimal => {
                let (s1, s2) = match (&input1_type.scalar_type, &input2_type.scalar_type) {
                    (ScalarType::Decimal(_, s1), ScalarType::Decimal(_, s2)) => (s1, s2),
                    _ => unreachable!(),
                };
                assert_eq!(s1, s2);
                ScalarType::Decimal(MAX_DECIMAL_PRECISION, *s1).nullable(in_nullable || is_div_mod)
            }
            MulDecimal => {
                let (s1, s2) = match (&input1_type.scalar_type, &input2_type.scalar_type) {
                    (ScalarType::Decimal(_, s1), ScalarType::Decimal(_, s2)) => (s1, s2),
                    _ => unreachable!(),
                };
                let s = s1 + s2;
                ScalarType::Decimal(MAX_DECIMAL_PRECISION, s).nullable(in_nullable)
            }
            DivDecimal => {
                let (s1, s2) = match (&input1_type.scalar_type, &input2_type.scalar_type) {
                    (ScalarType::Decimal(_, s1), ScalarType::Decimal(_, s2)) => (s1, s2),
                    _ => unreachable!(),
                };
                let s = s1 - s2;
                ScalarType::Decimal(MAX_DECIMAL_PRECISION, s).nullable(true)
            }

            RoundDecimal(scale) => {
                match input1_type.scalar_type {
                    ScalarType::Decimal(_, s) => assert_eq!(*scale, s),
                    _ => unreachable!(),
                }
                input1_type.scalar_type.nullable(in_nullable)
            }

            AddTimestampInterval
            | SubTimestampInterval
            | AddTimestampTzInterval
            | SubTimestampTzInterval
            | AddTimeInterval
            | SubTimeInterval => input1_type,

            AddDateInterval | SubDateInterval | AddDateTime | DateTruncTimestamp => {
                ScalarType::Timestamp.nullable(true)
            }

            TimezoneTimestampTz | TimezoneIntervalTimestampTz => {
                ScalarType::Timestamp.nullable(in_nullable)
            }

            DatePartInterval | DatePartTimestamp | DatePartTimestampTz => {
                ScalarType::Float64.nullable(true)
            }

            DateTruncTimestampTz => ScalarType::TimestampTz.nullable(true),

            TimezoneTimestamp | TimezoneIntervalTimestamp => {
                ScalarType::TimestampTz.nullable(in_nullable)
            }

            TimezoneTime { .. } | TimezoneIntervalTime => ScalarType::Time.nullable(in_nullable),

            SubTime => ScalarType::Interval.nullable(true),

            MzRenderTypemod | TextConcat => ScalarType::String.nullable(in_nullable),

            JsonbGetInt64 { stringify: true }
            | JsonbGetString { stringify: true }
            | JsonbGetPath { stringify: true } => ScalarType::String.nullable(true),

            JsonbGetInt64 { stringify: false }
            | JsonbGetString { stringify: false }
            | JsonbGetPath { stringify: false }
            | JsonbConcat
            | JsonbDeleteInt64
            | JsonbDeleteString => ScalarType::Jsonb.nullable(true),

            JsonbContainsString | JsonbContainsJsonb | MapContainsKey | MapContainsAllKeys
            | MapContainsAnyKeys | MapContainsMap => ScalarType::Bool.nullable(in_nullable),

            MapGetValue => input1_type
                .scalar_type
                .unwrap_map_value_type()
                .clone()
                .nullable(true),

            MapGetValues => ScalarType::Array(Box::new(
                input1_type.scalar_type.unwrap_map_value_type().clone(),
            ))
            .nullable(true),

            ListIndex => input1_type
                .scalar_type
                .unwrap_list_element_type()
                .clone()
                .nullable(true),

            ArrayIndex => input1_type
                .scalar_type
                .unwrap_array_element_type()
                .clone()
                .nullable(true),

            ListLengthMax { .. } | ArrayLength | ArrayLower | ArrayUpper => {
                ScalarType::Int64.nullable(true)
            }
            ListListConcat | ListElementConcat => input1_type.scalar_type.nullable(true),
            ElementListConcat => input2_type.scalar_type.nullable(true),
            DigestString | DigestBytes => ScalarType::Bytes.nullable(true),
            Position => ScalarType::Int32.nullable(in_nullable),
            Encode => ScalarType::String.nullable(in_nullable),
            Decode => ScalarType::Bytes.nullable(in_nullable),
            LogDecimal(_) => input1_type.scalar_type.nullable(in_nullable),
            Power => ScalarType::Float64.nullable(in_nullable),
            PowerDecimal(_) => input1_type.scalar_type.nullable(in_nullable),
            RepeatString => input1_type.scalar_type.nullable(in_nullable),

            AddAPD | DivAPD | LogAPD | ModAPD | MulAPD | PowerAPD | RoundAPD | SubAPD => {
                ScalarType::APD { scale: None }.nullable(in_nullable)
            }
        }
    }

    /// Whether the function output is NULL if any of its inputs are NULL.
    pub fn propagates_nulls(&self) -> bool {
        !matches!(
            self,
            BinaryFunc::And
                | BinaryFunc::Or
                | BinaryFunc::ListListConcat
                | BinaryFunc::ListElementConcat
                | BinaryFunc::ElementListConcat
        )
    }

    /// Whether the function might return NULL even if none of its inputs are
    /// NULL.
    ///
    /// This is presently conservative, and may indicate that a function
    /// introduces nulls even when it does not.
    pub fn introduces_nulls(&self) -> bool {
        use BinaryFunc::*;
        !matches!(
            self,
            And | Or
                | Eq
                | NotEq
                | Lt
                | Lte
                | Gt
                | Gte
                | AddInt32
                | AddInt64
                | AddFloat32
                | AddFloat64
                | AddTimestampInterval
                | AddTimestampTzInterval
                | AddDateTime
                | AddDateInterval
                | AddTimeInterval
                | AddInterval
                | SubInterval
                | MulInterval
                | DivInterval
                | AddDecimal
                | AddAPD
                | SubInt32
                | SubInt64
                | SubFloat32
                | SubFloat64
                | SubTimestamp
                | SubTimestampTz
                | SubTimestampInterval
                | SubTimestampTzInterval
                | SubDate
                | SubDateInterval
                | SubTime
                | SubTimeInterval
                | SubDecimal
                | SubAPD
                | MulInt32
                | MulInt64
                | MulFloat32
                | MulFloat64
                | MulDecimal
                | MulAPD
                | DivInt32
                | DivInt64
                | DivFloat32
                | DivFloat64
                | DivDecimal
                | ModInt32
                | ModInt64
                | ModFloat32
                | ModFloat64
                | ModDecimal
                | ModAPD
        )
    }

    pub fn is_infix_op(&self) -> bool {
        use BinaryFunc::*;
        match self {
            And
            | Or
            | AddInt32
            | AddInt64
            | AddFloat32
            | AddFloat64
            | AddTimestampInterval
            | AddTimestampTzInterval
            | AddDateTime
            | AddDateInterval
            | AddTimeInterval
            | AddInterval
            | SubInterval
            | MulInterval
            | DivInterval
            | AddDecimal
            | AddAPD
            | SubInt32
            | SubInt64
            | SubFloat32
            | SubFloat64
            | SubTimestamp
            | SubTimestampTz
            | SubTimestampInterval
            | SubTimestampTzInterval
            | SubDate
            | SubDateInterval
            | SubTime
            | SubTimeInterval
            | SubDecimal
            | SubAPD
            | MulInt32
            | MulInt64
            | MulFloat32
            | MulFloat64
            | MulDecimal
            | MulAPD
            | DivInt32
            | DivInt64
            | DivFloat32
            | DivFloat64
            | DivDecimal
            | DivAPD
            | ModInt32
            | ModInt64
            | ModFloat32
            | ModFloat64
            | ModDecimal
            | ModAPD
            | Eq
            | NotEq
            | Lt
            | Lte
            | Gt
            | Gte
            | JsonbConcat
            | JsonbContainsJsonb
            | JsonbGetInt64 { .. }
            | JsonbGetString { .. }
            | JsonbGetPath { .. }
            | JsonbContainsString
            | JsonbDeleteInt64
            | JsonbDeleteString
            | MapContainsKey
            | MapGetValue
            | MapGetValues
            | MapContainsAllKeys
            | MapContainsAnyKeys
            | MapContainsMap
            | TextConcat
            | ListIndex
            | IsRegexpMatch { .. }
            | ArrayContains
            | ArrayIndex
            | ArrayLength
            | ArrayLower
            | ArrayUpper
            | ListListConcat
            | ListElementConcat
            | ElementListConcat => true,
            IsLikePatternMatch { .. }
            | ToCharTimestamp
            | ToCharTimestampTz
            | DatePartInterval
            | DatePartTimestamp
            | DatePartTimestampTz
            | DateTruncTimestamp
            | DateTruncTimestampTz
            | TimezoneTimestamp
            | TimezoneTimestampTz
            | TimezoneTime { .. }
            | TimezoneIntervalTimestamp
            | TimezoneIntervalTimestampTz
            | TimezoneIntervalTime
            | RoundDecimal(_)
            | RoundAPD
            | ConvertFrom
            | Position
            | Right
            | Trim
            | TrimLeading
            | TrimTrailing
            | EncodedBytesCharLength
            | ListLengthMax { .. }
            | DigestString
            | DigestBytes
            | MzRenderTypemod
            | Encode
            | Decode
            | LogDecimal(_)
            | LogAPD
            | Power
            | PowerDecimal(_)
            | PowerAPD
            | RepeatString => false,
        }
    }

    /// Returns the negation of the given binary function, if it exists.
    pub fn negate(&self) -> Option<Self> {
        match self {
            BinaryFunc::Eq => Some(BinaryFunc::NotEq),
            BinaryFunc::NotEq => Some(BinaryFunc::Eq),
            BinaryFunc::Lt => Some(BinaryFunc::Gte),
            BinaryFunc::Gte => Some(BinaryFunc::Lt),
            BinaryFunc::Gt => Some(BinaryFunc::Lte),
            BinaryFunc::Lte => Some(BinaryFunc::Gt),
            _ => None,
        }
    }
}

impl fmt::Display for BinaryFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BinaryFunc::And => f.write_str("&&"),
            BinaryFunc::Or => f.write_str("||"),
            BinaryFunc::AddInt32 => f.write_str("+"),
            BinaryFunc::AddInt64 => f.write_str("+"),
            BinaryFunc::AddFloat32 => f.write_str("+"),
            BinaryFunc::AddFloat64 => f.write_str("+"),
            BinaryFunc::AddDecimal => f.write_str("+"),
            BinaryFunc::AddAPD => f.write_str("+"),
            BinaryFunc::AddInterval => f.write_str("+"),
            BinaryFunc::AddTimestampInterval => f.write_str("+"),
            BinaryFunc::AddTimestampTzInterval => f.write_str("+"),
            BinaryFunc::AddDateTime => f.write_str("+"),
            BinaryFunc::AddDateInterval => f.write_str("+"),
            BinaryFunc::AddTimeInterval => f.write_str("+"),
            BinaryFunc::SubInt32 => f.write_str("-"),
            BinaryFunc::SubInt64 => f.write_str("-"),
            BinaryFunc::SubFloat32 => f.write_str("-"),
            BinaryFunc::SubFloat64 => f.write_str("-"),
            BinaryFunc::SubDecimal => f.write_str("-"),
            BinaryFunc::SubAPD => f.write_str("-"),
            BinaryFunc::SubInterval => f.write_str("-"),
            BinaryFunc::SubTimestamp => f.write_str("-"),
            BinaryFunc::SubTimestampTz => f.write_str("-"),
            BinaryFunc::SubTimestampInterval => f.write_str("-"),
            BinaryFunc::SubTimestampTzInterval => f.write_str("-"),
            BinaryFunc::SubDate => f.write_str("-"),
            BinaryFunc::SubDateInterval => f.write_str("-"),
            BinaryFunc::SubTime => f.write_str("-"),
            BinaryFunc::SubTimeInterval => f.write_str("-"),
            BinaryFunc::MulInt32 => f.write_str("*"),
            BinaryFunc::MulInt64 => f.write_str("*"),
            BinaryFunc::MulFloat32 => f.write_str("*"),
            BinaryFunc::MulFloat64 => f.write_str("*"),
            BinaryFunc::MulDecimal => f.write_str("*"),
            BinaryFunc::MulAPD => f.write_str("*"),
            BinaryFunc::MulInterval => f.write_str("*"),
            BinaryFunc::DivInt32 => f.write_str("/"),
            BinaryFunc::DivInt64 => f.write_str("/"),
            BinaryFunc::DivFloat32 => f.write_str("/"),
            BinaryFunc::DivFloat64 => f.write_str("/"),
            BinaryFunc::DivDecimal => f.write_str("/"),
            BinaryFunc::DivAPD => f.write_str("/"),
            BinaryFunc::DivInterval => f.write_str("/"),
            BinaryFunc::ModInt32 => f.write_str("%"),
            BinaryFunc::ModInt64 => f.write_str("%"),
            BinaryFunc::ModFloat32 => f.write_str("%"),
            BinaryFunc::ModFloat64 => f.write_str("%"),
            BinaryFunc::ModDecimal => f.write_str("%"),
            BinaryFunc::ModAPD => f.write_str("%"),
            BinaryFunc::Eq => f.write_str("="),
            BinaryFunc::NotEq => f.write_str("!="),
            BinaryFunc::Lt => f.write_str("<"),
            BinaryFunc::Lte => f.write_str("<="),
            BinaryFunc::Gt => f.write_str(">"),
            BinaryFunc::Gte => f.write_str(">="),
            BinaryFunc::IsLikePatternMatch {
                case_insensitive: false,
            } => f.write_str("like"),
            BinaryFunc::IsLikePatternMatch {
                case_insensitive: true,
            } => f.write_str("ilike"),
            BinaryFunc::IsRegexpMatch {
                case_insensitive: false,
            } => f.write_str("~"),
            BinaryFunc::IsRegexpMatch {
                case_insensitive: true,
            } => f.write_str("~*"),
            BinaryFunc::ToCharTimestamp => f.write_str("tocharts"),
            BinaryFunc::ToCharTimestampTz => f.write_str("tochartstz"),
            BinaryFunc::DatePartInterval => f.write_str("date_partiv"),
            BinaryFunc::DatePartTimestamp => f.write_str("date_partts"),
            BinaryFunc::DatePartTimestampTz => f.write_str("date_parttstz"),
            BinaryFunc::DateTruncTimestamp => f.write_str("date_truncts"),
            BinaryFunc::DateTruncTimestampTz => f.write_str("date_trunctstz"),
            BinaryFunc::TimezoneTimestamp => f.write_str("timezonets"),
            BinaryFunc::TimezoneTimestampTz => f.write_str("timezonetstz"),
            BinaryFunc::TimezoneTime { .. } => f.write_str("timezonet"),
            BinaryFunc::TimezoneIntervalTimestamp => f.write_str("timezoneits"),
            BinaryFunc::TimezoneIntervalTimestampTz => f.write_str("timezoneitstz"),
            BinaryFunc::TimezoneIntervalTime => f.write_str("timezoneit"),
            BinaryFunc::TextConcat => f.write_str("||"),
            BinaryFunc::JsonbGetInt64 { stringify: false } => f.write_str("->"),
            BinaryFunc::JsonbGetInt64 { stringify: true } => f.write_str("->>"),
            BinaryFunc::JsonbGetString { stringify: false } => f.write_str("->"),
            BinaryFunc::JsonbGetString { stringify: true } => f.write_str("->>"),
            BinaryFunc::JsonbGetPath { stringify: false } => f.write_str("#>"),
            BinaryFunc::JsonbGetPath { stringify: true } => f.write_str("#>>"),
            BinaryFunc::JsonbContainsString | BinaryFunc::MapContainsKey => f.write_str("?"),
            BinaryFunc::JsonbConcat => f.write_str("||"),
            BinaryFunc::JsonbContainsJsonb | BinaryFunc::MapContainsMap => f.write_str("@>"),
            BinaryFunc::JsonbDeleteInt64 => f.write_str("-"),
            BinaryFunc::JsonbDeleteString => f.write_str("-"),
            BinaryFunc::MapGetValue | BinaryFunc::MapGetValues => f.write_str("->"),
            BinaryFunc::MapContainsAllKeys => f.write_str("?&"),
            BinaryFunc::MapContainsAnyKeys => f.write_str("?|"),
            BinaryFunc::RoundDecimal(_) => f.write_str("round"),
            BinaryFunc::RoundAPD => f.write_str("round"),
            BinaryFunc::ConvertFrom => f.write_str("convert_from"),
            BinaryFunc::Position => f.write_str("position"),
            BinaryFunc::Right => f.write_str("right"),
            BinaryFunc::Trim => f.write_str("btrim"),
            BinaryFunc::TrimLeading => f.write_str("ltrim"),
            BinaryFunc::TrimTrailing => f.write_str("rtrim"),
            BinaryFunc::EncodedBytesCharLength => f.write_str("length"),
            BinaryFunc::ListIndex => f.write_str("list_index"),
            BinaryFunc::ListLengthMax { .. } => f.write_str("list_length_max"),
            BinaryFunc::ArrayContains => f.write_str("array_contains"),
            BinaryFunc::ArrayIndex => f.write_str("array_index"),
            BinaryFunc::ArrayLength => f.write_str("array_length"),
            BinaryFunc::ArrayLower => f.write_str("array_lower"),
            BinaryFunc::ArrayUpper => f.write_str("array_upper"),
            BinaryFunc::ListListConcat => f.write_str("||"),
            BinaryFunc::ListElementConcat => f.write_str("||"),
            BinaryFunc::ElementListConcat => f.write_str("||"),
            BinaryFunc::DigestString | BinaryFunc::DigestBytes => f.write_str("digest"),
            BinaryFunc::MzRenderTypemod => f.write_str("mz_render_typemod"),
            BinaryFunc::Encode => f.write_str("encode"),
            BinaryFunc::Decode => f.write_str("decode"),
            BinaryFunc::LogDecimal(_) => f.write_str("log"),
            BinaryFunc::LogAPD => f.write_str("log"),
            BinaryFunc::Power => f.write_str("power"),
            BinaryFunc::PowerDecimal(_) => f.write_str("power_decimal"),
            BinaryFunc::PowerAPD => f.write_str("power_apd"),
            BinaryFunc::RepeatString => f.write_str("repeat"),
        }
    }
}

fn is_null<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a == Datum::Null)
}

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzEnumReflect,
)]
pub enum UnaryFunc {
    Not,
    IsNull,
    NegInt32,
    NegInt64,
    NegFloat32,
    NegFloat64,
    NegDecimal,
    NegAPD,
    NegInterval,
    SqrtFloat64,
    SqrtDec(u8),
    SqrtAPD,
    CbrtFloat64,
    AbsInt32,
    AbsInt64,
    AbsFloat32,
    AbsFloat64,
    AbsDecimal,
    AbsAPD,
    CastBoolToString,
    CastBoolToStringNonstandard,
    CastBoolToInt32,
    CastInt32ToBool,
    CastInt32ToFloat32,
    CastInt32ToFloat64,
    CastInt32ToOid,
    CastInt32ToInt64,
    CastInt32ToString,
    CastOidToInt32,
    CastInt64ToInt32,
    CastInt32ToDecimal,
    CastInt32ToAPD(Option<u8>),
    CastInt64ToBool,
    CastInt64ToDecimal,
    CastInt64ToAPD(Option<u8>),
    CastInt64ToFloat32,
    CastInt64ToFloat64,
    CastInt64ToString,
    CastFloat32ToInt32,
    CastFloat32ToInt64,
    CastFloat32ToFloat64,
    CastFloat32ToString,
    CastFloat32ToDecimal(u8),
    CastFloat64ToDecimal(u8),
    CastFloat32ToAPD(Option<u8>),
    CastFloat64ToAPD(Option<u8>),
    CastFloat64ToInt32,
    CastFloat64ToInt64,
    CastFloat64ToFloat32,
    CastFloat64ToString,
    CastDecimalToInt32(u8),
    CastDecimalToInt64(u8),
    CastDecimalToString(u8),
    CastAPDToFloat32,
    CastAPDToFloat64,
    CastAPDToInt32,
    CastAPDToInt64,
    CastAPDToString,
    CastSignificandToFloat32,
    CastSignificandToFloat64,
    CastStringToBool,
    CastStringToBytes,
    CastStringToInt32,
    CastStringToInt64,
    CastStringToFloat32,
    CastStringToFloat64,
    CastStringToDate,
    CastStringToArray {
        // Target array's type.
        return_ty: ScalarType,
        // The expression to cast the discovered array elements to the array's
        // element type.
        cast_expr: Box<MirScalarExpr>,
    },
    CastStringToList {
        // Target list's type
        return_ty: ScalarType,
        // The expression to cast the discovered list elements to the list's
        // element type.
        cast_expr: Box<MirScalarExpr>,
    },
    CastStringToMap {
        // Target map's value type
        return_ty: ScalarType,
        // The expression used to cast the discovered values to the map's value
        // type.
        cast_expr: Box<MirScalarExpr>,
    },
    CastStringToTime,
    CastStringToTimestamp,
    CastStringToTimestampTz,
    CastStringToInterval,
    CastStringToDecimal(u8),
    CastStringToAPD(Option<u8>),
    CastStringToUuid,
    CastDateToTimestamp,
    CastDateToTimestampTz,
    CastDateToString,
    CastTimeToInterval,
    CastTimeToString,
    CastTimestampToDate,
    CastTimestampToTimestampTz,
    CastTimestampToString,
    CastTimestampTzToDate,
    CastTimestampTzToTimestamp,
    CastTimestampTzToString,
    CastIntervalToString,
    CastIntervalToTime,
    CastBytesToString,
    CastStringToJsonb,
    CastJsonbToString,
    CastJsonbOrNullToJsonb,
    CastJsonbToInt32,
    CastJsonbToInt64,
    CastJsonbToFloat32,
    CastJsonbToFloat64,
    CastJsonbToDecimal(u8),
    CastJsonbToAPD(Option<u8>),
    CastJsonbToBool,
    CastUuidToString,
    CastRecordToString {
        ty: ScalarType,
    },
    CastArrayToString {
        ty: ScalarType,
    },
    CastListToString {
        ty: ScalarType,
    },
    CastList1ToList2 {
        // List2's type
        return_ty: ScalarType,
        // The expression to cast List1's elements to List2's elements' type
        cast_expr: Box<MirScalarExpr>,
    },
    CastMapToString {
        ty: ScalarType,
    },
    CastInPlace {
        return_ty: ScalarType,
    },
    CeilFloat32,
    CeilFloat64,
    CeilDecimal(u8),
    CeilAPD,
    FloorFloat32,
    FloorFloat64,
    FloorDecimal(u8),
    FloorAPD,
    Ascii,
    BitLengthBytes,
    BitLengthString,
    ByteLengthBytes,
    ByteLengthString,
    CharLength,
    IsRegexpMatch(Regex),
    RegexpMatch(Regex),
    DatePartInterval(DateTimeUnits),
    DatePartTimestamp(DateTimeUnits),
    DatePartTimestampTz(DateTimeUnits),
    DateTruncTimestamp(DateTimeUnits),
    DateTruncTimestampTz(DateTimeUnits),
    TimezoneTimestamp(Timezone),
    TimezoneTimestampTz(Timezone),
    TimezoneTime {
        tz: Timezone,
        wall_time: NaiveDateTime,
    },
    ToTimestamp,
    JsonbArrayLength,
    JsonbTypeof,
    JsonbStripNulls,
    JsonbPretty,
    RoundFloat32,
    RoundFloat64,
    RoundDecimal(u8),
    RoundAPD,
    TrimWhitespace,
    TrimLeadingWhitespace,
    TrimTrailingWhitespace,
    RecordGet(usize),
    ListLength,
    Upper,
    Lower,
    Cos,
    Cosh,
    Sin,
    Sinh,
    Tan,
    Tanh,
    Cot,
    Log10,
    Log10Decimal(u8),
    Log10APD,
    Ln,
    LnDecimal(u8),
    LnAPD,
    Exp,
    ExpDecimal(u8),
    ExpAPD,
    Sleep,
    RescaleAPD(u8),
    PgColumnSize,
    MzRowSize,
}

impl UnaryFunc {
    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        let a = a.eval(datums, temp_storage)?;
        if self.propagates_nulls() && a.is_null() {
            return Ok(Datum::Null);
        }

        match self {
            UnaryFunc::Not => Ok(not(a)),
            UnaryFunc::IsNull => Ok(is_null(a)),
            UnaryFunc::NegInt32 => Ok(neg_int32(a)),
            UnaryFunc::NegInt64 => Ok(neg_int64(a)),
            UnaryFunc::NegFloat32 => Ok(neg_float32(a)),
            UnaryFunc::NegFloat64 => Ok(neg_float64(a)),
            UnaryFunc::NegDecimal => Ok(neg_decimal(a)),
            UnaryFunc::NegAPD => Ok(neg_apd(a)),
            UnaryFunc::NegInterval => Ok(neg_interval(a)),
            UnaryFunc::AbsInt32 => Ok(abs_int32(a)),
            UnaryFunc::AbsInt64 => Ok(abs_int64(a)),
            UnaryFunc::AbsFloat32 => Ok(abs_float32(a)),
            UnaryFunc::AbsFloat64 => Ok(abs_float64(a)),
            UnaryFunc::AbsDecimal => Ok(abs_decimal(a)),
            UnaryFunc::AbsAPD => Ok(abs_apd(a)),
            UnaryFunc::CastBoolToString => Ok(cast_bool_to_string(a)),
            UnaryFunc::CastBoolToStringNonstandard => Ok(cast_bool_to_string_nonstandard(a)),
            UnaryFunc::CastBoolToInt32 => Ok(cast_bool_to_int32(a)),
            UnaryFunc::CastFloat32ToDecimal(scale) => cast_float32_to_decimal(a, *scale),
            UnaryFunc::CastFloat64ToDecimal(scale) => cast_float64_to_decimal(a, *scale),
            UnaryFunc::CastFloat32ToAPD(scale) => cast_float32_to_apd(a, *scale),
            UnaryFunc::CastFloat64ToAPD(scale) => cast_float64_to_apd(a, *scale),
            UnaryFunc::CastInt32ToBool => Ok(cast_int32_to_bool(a)),
            UnaryFunc::CastInt32ToFloat32 => Ok(cast_int32_to_float32(a)),
            UnaryFunc::CastInt32ToFloat64 => Ok(cast_int32_to_float64(a)),
            UnaryFunc::CastInt32ToInt64 => Ok(cast_int32_to_int64(a)),
            UnaryFunc::CastInt32ToOid => Ok(a),
            UnaryFunc::CastInt32ToDecimal => Ok(cast_int32_to_decimal(a)),
            UnaryFunc::CastInt32ToAPD(scale) => cast_int32_to_apd(a, *scale),
            UnaryFunc::CastInt32ToString => Ok(cast_int32_to_string(a, temp_storage)),
            UnaryFunc::CastOidToInt32 => Ok(a),
            UnaryFunc::CastInt64ToInt32 => cast_int64_to_int32(a),
            UnaryFunc::CastInt64ToBool => Ok(cast_int64_to_bool(a)),
            UnaryFunc::CastInt64ToDecimal => Ok(cast_int64_to_decimal(a)),
            UnaryFunc::CastInt64ToAPD(scale) => cast_int64_to_apd(a, *scale),
            UnaryFunc::CastInt64ToFloat32 => Ok(cast_int64_to_float32(a)),
            UnaryFunc::CastInt64ToFloat64 => Ok(cast_int64_to_float64(a)),
            UnaryFunc::CastInt64ToString => Ok(cast_int64_to_string(a, temp_storage)),
            UnaryFunc::CastFloat32ToInt32 => cast_float32_to_int32(a),
            UnaryFunc::CastFloat32ToInt64 => cast_float32_to_int64(a),
            UnaryFunc::CastFloat32ToFloat64 => Ok(cast_float32_to_float64(a)),
            UnaryFunc::CastFloat32ToString => Ok(cast_float32_to_string(a, temp_storage)),
            UnaryFunc::CastFloat64ToInt32 => cast_float64_to_int32(a),
            UnaryFunc::CastFloat64ToInt64 => cast_float64_to_int64(a),
            UnaryFunc::CastFloat64ToFloat32 => cast_float64_to_float32(a),
            UnaryFunc::CastFloat64ToString => Ok(cast_float64_to_string(a, temp_storage)),
            UnaryFunc::CastDecimalToInt32(scale) => Ok(cast_decimal_to_int32(a, *scale)),
            UnaryFunc::CastDecimalToInt64(scale) => Ok(cast_decimal_to_int64(a, *scale)),
            UnaryFunc::CastSignificandToFloat32 => Ok(cast_significand_to_float32(a)),
            UnaryFunc::CastSignificandToFloat64 => Ok(cast_significand_to_float64(a)),
            UnaryFunc::CastStringToBool => cast_string_to_bool(a),
            UnaryFunc::CastStringToBytes => cast_string_to_bytes(a, temp_storage),
            UnaryFunc::CastStringToInt32 => cast_string_to_int32(a),
            UnaryFunc::CastStringToInt64 => cast_string_to_int64(a),
            UnaryFunc::CastStringToFloat32 => cast_string_to_float32(a),
            UnaryFunc::CastStringToFloat64 => cast_string_to_float64(a),
            UnaryFunc::CastStringToDecimal(scale) => cast_string_to_decimal(a, *scale),
            UnaryFunc::CastStringToAPD(scale) => cast_string_to_apd(a, *scale),
            UnaryFunc::CastStringToDate => cast_string_to_date(a),
            UnaryFunc::CastStringToArray { cast_expr, .. } => {
                cast_string_to_array(a, cast_expr, temp_storage)
            }
            UnaryFunc::CastStringToList {
                cast_expr,
                return_ty,
            } => cast_string_to_list(a, return_ty, cast_expr, temp_storage),
            UnaryFunc::CastStringToMap {
                cast_expr,
                return_ty,
            } => cast_string_to_map(a, return_ty, cast_expr, temp_storage),
            UnaryFunc::CastStringToTime => cast_string_to_time(a),
            UnaryFunc::CastStringToTimestamp => cast_string_to_timestamp(a),
            UnaryFunc::CastStringToTimestampTz => cast_string_to_timestamptz(a),
            UnaryFunc::CastStringToInterval => cast_string_to_interval(a),
            UnaryFunc::CastStringToUuid => cast_string_to_uuid(a),
            UnaryFunc::CastStringToJsonb => cast_string_to_jsonb(a, temp_storage),
            UnaryFunc::CastDateToTimestamp => Ok(cast_date_to_timestamp(a)),
            UnaryFunc::CastDateToTimestampTz => Ok(cast_date_to_timestamptz(a)),
            UnaryFunc::CastDateToString => Ok(cast_date_to_string(a, temp_storage)),
            UnaryFunc::CastDecimalToString(scale) => {
                Ok(cast_decimal_to_string(a, *scale, temp_storage))
            }
            UnaryFunc::CastAPDToFloat32 => cast_apd_to_float32(a),
            UnaryFunc::CastAPDToFloat64 => cast_apd_to_float64(a),
            UnaryFunc::CastAPDToInt32 => cast_apd_to_int32(a),
            UnaryFunc::CastAPDToInt64 => cast_apd_to_int64(a),
            UnaryFunc::CastAPDToString => Ok(cast_apd_to_string(a, temp_storage)),
            UnaryFunc::CastTimeToInterval => cast_time_to_interval(a),
            UnaryFunc::CastTimeToString => Ok(cast_time_to_string(a, temp_storage)),
            UnaryFunc::CastTimestampToDate => Ok(cast_timestamp_to_date(a)),
            UnaryFunc::CastTimestampToTimestampTz => Ok(cast_timestamp_to_timestamptz(a)),
            UnaryFunc::CastTimestampToString => Ok(cast_timestamp_to_string(a, temp_storage)),
            UnaryFunc::CastTimestampTzToDate => Ok(cast_timestamptz_to_date(a)),
            UnaryFunc::CastTimestampTzToTimestamp => Ok(cast_timestamptz_to_timestamp(a)),
            UnaryFunc::CastTimestampTzToString => Ok(cast_timestamptz_to_string(a, temp_storage)),
            UnaryFunc::CastIntervalToString => Ok(cast_interval_to_string(a, temp_storage)),
            UnaryFunc::CastIntervalToTime => Ok(cast_interval_to_time(a)),
            UnaryFunc::CastBytesToString => Ok(cast_bytes_to_string(a, temp_storage)),
            UnaryFunc::CastJsonbOrNullToJsonb => Ok(cast_jsonb_or_null_to_jsonb(a)),
            UnaryFunc::CastJsonbToString => Ok(cast_jsonb_to_string(a, temp_storage)),
            UnaryFunc::CastJsonbToInt32 => cast_jsonb_to_int32(a),
            UnaryFunc::CastJsonbToInt64 => cast_jsonb_to_int64(a),
            UnaryFunc::CastJsonbToFloat32 => cast_jsonb_to_float32(a),
            UnaryFunc::CastJsonbToFloat64 => cast_jsonb_to_float64(a),
            UnaryFunc::CastJsonbToDecimal(scale) => cast_jsonb_to_decimal(a, *scale),
            UnaryFunc::CastJsonbToAPD(scale) => cast_jsonb_to_apd(a, *scale),
            UnaryFunc::CastJsonbToBool => cast_jsonb_to_bool(a),
            UnaryFunc::CastUuidToString => Ok(cast_uuid_to_string(a, temp_storage)),
            UnaryFunc::CastRecordToString { ty }
            | UnaryFunc::CastArrayToString { ty }
            | UnaryFunc::CastListToString { ty }
            | UnaryFunc::CastMapToString { ty } => {
                Ok(cast_collection_to_string(a, ty, temp_storage))
            }
            UnaryFunc::CastList1ToList2 { cast_expr, .. } => {
                cast_list1_to_list2(a, &*cast_expr, temp_storage)
            }
            UnaryFunc::CastInPlace { .. } => Ok(a),
            UnaryFunc::CeilFloat32 => Ok(ceil_float32(a)),
            UnaryFunc::CeilFloat64 => Ok(ceil_float64(a)),
            UnaryFunc::CeilDecimal(scale) => Ok(ceil_decimal(a, *scale)),
            UnaryFunc::CeilAPD => Ok(ceil_apd(a)),
            UnaryFunc::FloorFloat32 => Ok(floor_float32(a)),
            UnaryFunc::FloorFloat64 => Ok(floor_float64(a)),
            UnaryFunc::FloorDecimal(scale) => Ok(floor_decimal(a, *scale)),
            UnaryFunc::FloorAPD => Ok(floor_apd(a)),
            UnaryFunc::SqrtFloat64 => sqrt_float64(a),
            UnaryFunc::SqrtDec(scale) => sqrt_dec(a, *scale),
            UnaryFunc::SqrtAPD => sqrt_apd(a),
            UnaryFunc::CbrtFloat64 => Ok(cbrt_float64(a)),
            UnaryFunc::Ascii => Ok(ascii(a)),
            UnaryFunc::BitLengthString => bit_length(a.unwrap_str()),
            UnaryFunc::BitLengthBytes => bit_length(a.unwrap_bytes()),
            UnaryFunc::ByteLengthString => byte_length(a.unwrap_str()),
            UnaryFunc::ByteLengthBytes => byte_length(a.unwrap_bytes()),
            UnaryFunc::CharLength => char_length(a),
            UnaryFunc::IsRegexpMatch(regex) => Ok(is_regexp_match_static(a, &regex)),
            UnaryFunc::RegexpMatch(regex) => regexp_match_static(a, temp_storage, &regex),
            UnaryFunc::DatePartInterval(units) => {
                date_part_interval_inner(*units, a.unwrap_interval())
            }
            UnaryFunc::DatePartTimestamp(units) => {
                date_part_timestamp_inner(*units, a.unwrap_timestamp())
            }
            UnaryFunc::DatePartTimestampTz(units) => {
                date_part_timestamp_inner(*units, a.unwrap_timestamptz())
            }
            UnaryFunc::DateTruncTimestamp(units) => date_trunc_inner(*units, a.unwrap_timestamp()),
            UnaryFunc::DateTruncTimestampTz(units) => {
                date_trunc_inner(*units, a.unwrap_timestamptz())
            }
            UnaryFunc::TimezoneTimestamp(tz) => timezone_timestamp(*tz, a.unwrap_timestamp()),
            UnaryFunc::TimezoneTimestampTz(tz) => {
                Ok(timezone_timestamptz(*tz, a.unwrap_timestamptz()))
            }
            UnaryFunc::TimezoneTime { tz, wall_time } => {
                Ok(timezone_time(*tz, a.unwrap_time(), wall_time))
            }
            UnaryFunc::ToTimestamp => Ok(to_timestamp(a)),
            UnaryFunc::JsonbArrayLength => Ok(jsonb_array_length(a)),
            UnaryFunc::JsonbTypeof => Ok(jsonb_typeof(a)),
            UnaryFunc::JsonbStripNulls => Ok(jsonb_strip_nulls(a, temp_storage)),
            UnaryFunc::JsonbPretty => Ok(jsonb_pretty(a, temp_storage)),
            UnaryFunc::RoundFloat32 => Ok(round_float32(a)),
            UnaryFunc::RoundFloat64 => Ok(round_float64(a)),
            UnaryFunc::RoundDecimal(scale) => Ok(round_decimal_unary(a, *scale)),
            UnaryFunc::RoundAPD => Ok(round_apd_unary(a)),
            UnaryFunc::TrimWhitespace => Ok(trim_whitespace(a)),
            UnaryFunc::TrimLeadingWhitespace => Ok(trim_leading_whitespace(a)),
            UnaryFunc::TrimTrailingWhitespace => Ok(trim_trailing_whitespace(a)),
            UnaryFunc::RecordGet(i) => Ok(record_get(a, *i)),
            UnaryFunc::ListLength => Ok(list_length(a)),
            UnaryFunc::Upper => Ok(upper(a, temp_storage)),
            UnaryFunc::Lower => Ok(lower(a, temp_storage)),
            UnaryFunc::Cos => cos(a),
            UnaryFunc::Cosh => Ok(cosh(a)),
            UnaryFunc::Sin => sin(a),
            UnaryFunc::Sinh => Ok(sinh(a)),
            UnaryFunc::Tan => tan(a),
            UnaryFunc::Tanh => Ok(tanh(a)),
            UnaryFunc::Cot => cot(a),
            UnaryFunc::Log10 => log(a, f64::log10, "log10"),
            UnaryFunc::Log10Decimal(scale) => log_dec(a, f64::log10, "log10", *scale),
            UnaryFunc::Log10APD => log_apd(a, dec::Context::log10, "log10"),
            UnaryFunc::Ln => log(a, f64::ln, "ln"),
            UnaryFunc::LnDecimal(scale) => log_dec(a, f64::ln, "ln", *scale),
            UnaryFunc::LnAPD => log_apd(a, dec::Context::ln, "ln"),
            UnaryFunc::Exp => exp(a),
            UnaryFunc::ExpDecimal(scale) => exp_dec(a, *scale),
            UnaryFunc::ExpAPD => exp_apd(a),
            UnaryFunc::Sleep => sleep(a),
            UnaryFunc::RescaleAPD(scale) => rescale_apd(a, *scale),
            UnaryFunc::PgColumnSize => pg_column_size(a),
            UnaryFunc::MzRowSize => mz_row_size(a),
        }
    }

    pub fn output_type(&self, input_type: ColumnType) -> ColumnType {
        use UnaryFunc::*;
        let in_nullable = input_type.nullable;
        match self {
            IsNull | CastInt32ToBool | CastInt64ToBool => ScalarType::Bool.nullable(false),

            Ascii | CharLength | BitLengthBytes | BitLengthString | ByteLengthBytes
            | ByteLengthString => ScalarType::Int32.nullable(in_nullable),

            IsRegexpMatch(_) => ScalarType::Bool.nullable(in_nullable),

            CastStringToBool => ScalarType::Bool.nullable(true),
            CastStringToBytes => ScalarType::Bytes.nullable(true),
            CastStringToInt32 => ScalarType::Int32.nullable(true),
            CastStringToInt64 => ScalarType::Int64.nullable(true),
            CastStringToFloat32 => ScalarType::Float32.nullable(true),
            CastStringToFloat64 => ScalarType::Float64.nullable(true),
            CastStringToDecimal(scale) => {
                ScalarType::Decimal(MAX_DECIMAL_PRECISION, *scale).nullable(true)
            }
            CastStringToAPD(scale) => ScalarType::APD { scale: *scale }.nullable(true),
            CastStringToDate => ScalarType::Date.nullable(true),
            CastStringToTime => ScalarType::Time.nullable(true),
            CastStringToTimestamp => ScalarType::Timestamp.nullable(true),
            CastStringToTimestampTz => ScalarType::TimestampTz.nullable(true),
            CastStringToInterval | CastTimeToInterval => ScalarType::Interval.nullable(true),
            CastStringToUuid => ScalarType::Uuid.nullable(true),

            CastBoolToInt32 => ScalarType::Int32.nullable(in_nullable),

            CastBoolToString
            | CastBoolToStringNonstandard
            | CastInt32ToString
            | CastInt64ToString
            | CastFloat32ToString
            | CastFloat64ToString
            | CastDecimalToString(_)
            | CastAPDToString
            | CastDateToString
            | CastTimeToString
            | CastTimestampToString
            | CastTimestampTzToString
            | CastIntervalToString
            | CastBytesToString
            | CastRecordToString { .. }
            | CastArrayToString { .. }
            | CastListToString { .. }
            | CastMapToString { .. }
            | TrimWhitespace
            | TrimLeadingWhitespace
            | TrimTrailingWhitespace
            | Upper
            | Lower => ScalarType::String.nullable(in_nullable),

            CastFloat64ToFloat32
            | CastInt32ToFloat32
            | CastInt64ToFloat32
            | CastSignificandToFloat32
            | CastAPDToFloat32 => ScalarType::Float32.nullable(in_nullable),

            CastInt32ToFloat64
            | CastInt64ToFloat64
            | CastFloat32ToFloat64
            | CastSignificandToFloat64
            | CastAPDToFloat64 => ScalarType::Float64.nullable(in_nullable),

            CastFloat32ToDecimal(s) | CastFloat64ToDecimal(s) => {
                ScalarType::Decimal(MAX_DECIMAL_PRECISION, *s).nullable(in_nullable)
            }

            CastInt64ToInt32 | CastDecimalToInt32(_) | CastAPDToInt32 => {
                ScalarType::Int32.nullable(in_nullable)
            }

            CastFloat32ToInt32 | CastFloat64ToInt32 => ScalarType::Int32.nullable(true),

            CastInt32ToInt64
            | CastDecimalToInt64(_)
            | CastFloat32ToInt64
            | CastFloat64ToInt64
            | CastAPDToInt64 => ScalarType::Int64.nullable(in_nullable),

            CastInt32ToDecimal => ScalarType::Decimal(0, 0).nullable(in_nullable),
            CastInt64ToDecimal => ScalarType::Decimal(0, 0).nullable(in_nullable),
            CastInt32ToAPD(scale)
            | CastInt64ToAPD(scale)
            | CastFloat32ToAPD(scale)
            | CastFloat64ToAPD(scale)
            | CastJsonbToAPD(scale) => ScalarType::APD { scale: *scale }.nullable(in_nullable),

            CastInt32ToOid => ScalarType::Oid.nullable(in_nullable),
            CastOidToInt32 => ScalarType::Oid.nullable(in_nullable),

            CastTimestampToDate | CastTimestampTzToDate => ScalarType::Date.nullable(in_nullable),

            CastIntervalToTime | TimezoneTime { .. } => ScalarType::Time.nullable(in_nullable),

            CastDateToTimestamp | CastTimestampTzToTimestamp | TimezoneTimestampTz(_) => {
                ScalarType::Timestamp.nullable(in_nullable)
            }

            CastDateToTimestampTz | CastTimestampToTimestampTz | TimezoneTimestamp(_) => {
                ScalarType::TimestampTz.nullable(in_nullable)
            }

            // can return null for invalid json
            CastStringToJsonb => ScalarType::Jsonb.nullable(true),

            // converts null to jsonnull
            CastJsonbOrNullToJsonb => ScalarType::Jsonb.nullable(false),

            // These return null when their input is SQL null.
            CastJsonbToString => ScalarType::String.nullable(true),
            CastJsonbToInt32 => ScalarType::Int32.nullable(true),
            CastJsonbToInt64 => ScalarType::Int64.nullable(true),
            CastJsonbToFloat32 => ScalarType::Float32.nullable(true),
            CastJsonbToFloat64 => ScalarType::Float64.nullable(true),
            CastJsonbToDecimal(scale) => {
                ScalarType::Decimal(MAX_DECIMAL_PRECISION, *scale).nullable(true)
            }
            CastJsonbToBool => ScalarType::Bool.nullable(true),

            CastUuidToString => ScalarType::String.nullable(true),

            CastList1ToList2 { return_ty, .. }
            | CastStringToArray { return_ty, .. }
            | CastStringToList { return_ty, .. }
            | CastStringToMap { return_ty, .. }
            | CastInPlace { return_ty } => (return_ty.clone()).nullable(false),

            CeilFloat32 | FloorFloat32 | RoundFloat32 => ScalarType::Float32.nullable(in_nullable),
            CeilFloat64 | FloorFloat64 | RoundFloat64 => ScalarType::Float64.nullable(in_nullable),
            CeilDecimal(scale) | FloorDecimal(scale) | RoundDecimal(scale) | SqrtDec(scale) => {
                match input_type.scalar_type {
                    ScalarType::Decimal(_, s) => assert_eq!(*scale, s),
                    _ => unreachable!(),
                }
                input_type.scalar_type.nullable(in_nullable)
            }

            SqrtFloat64 => ScalarType::Float64.nullable(true),

            CbrtFloat64 => ScalarType::Float64.nullable(true),

            Not | NegInt32 | NegInt64 | NegFloat32 | NegFloat64 | NegDecimal | NegInterval
            | AbsInt32 | AbsInt64 | AbsFloat32 | AbsFloat64 | AbsDecimal => input_type,

            DatePartInterval(_) | DatePartTimestamp(_) | DatePartTimestampTz(_) => {
                ScalarType::Float64.nullable(in_nullable)
            }

            DateTruncTimestamp(_) => ScalarType::Timestamp.nullable(true),
            DateTruncTimestampTz(_) => ScalarType::TimestampTz.nullable(true),

            ToTimestamp => ScalarType::TimestampTz.nullable(true),

            JsonbArrayLength => ScalarType::Int64.nullable(true),
            JsonbTypeof => ScalarType::String.nullable(in_nullable),
            JsonbStripNulls => ScalarType::Jsonb.nullable(true),
            JsonbPretty => ScalarType::String.nullable(in_nullable),

            RecordGet(i) => match input_type.scalar_type {
                ScalarType::Record { mut fields, .. } => {
                    let (_name, mut ty) = fields.swap_remove(*i);
                    ty.nullable = ty.nullable || input_type.nullable;
                    ty
                }
                _ => unreachable!("RecordGet specified nonexistent field"),
            },

            ListLength => ScalarType::Int64.nullable(true),

            RegexpMatch(_) => ScalarType::Array(Box::new(ScalarType::String)).nullable(true),

            Cos => ScalarType::Float64.nullable(in_nullable),
            Cosh => ScalarType::Float64.nullable(in_nullable),
            Sin => ScalarType::Float64.nullable(in_nullable),
            Sinh => ScalarType::Float64.nullable(in_nullable),
            Tan => ScalarType::Float64.nullable(in_nullable),
            Tanh => ScalarType::Float64.nullable(in_nullable),
            Cot => ScalarType::Float64.nullable(in_nullable),
            Log10 | Ln | Exp => ScalarType::Float64.nullable(in_nullable),
            Log10Decimal(_) | LnDecimal(_) | ExpDecimal(_) => input_type,
            Sleep => ScalarType::TimestampTz.nullable(true),
            RescaleAPD(scale) => ScalarType::APD {
                scale: Some(*scale),
            }
            .nullable(true),
            PgColumnSize => ScalarType::Int32.nullable(in_nullable),
            MzRowSize => ScalarType::Int32.nullable(in_nullable),

            AbsAPD | CeilAPD | ExpAPD | FloorAPD | LnAPD | Log10APD | NegAPD | RoundAPD
            | SqrtAPD => ScalarType::APD { scale: None }.nullable(in_nullable),
        }
    }

    /// Whether the function output is NULL if any of its inputs are NULL.
    pub fn propagates_nulls(&self) -> bool {
        !matches!(self, UnaryFunc::IsNull | UnaryFunc::CastJsonbOrNullToJsonb)
    }

    /// True iff for x != y, we are assured f(x) != f(y).
    ///
    /// This is most often the case for methods that promote to types that
    /// can contain all the precision of the input type.
    pub fn preserves_uniqueness(&self) -> bool {
        matches!(
            self,
            UnaryFunc::Not
                | UnaryFunc::NegInt32
                | UnaryFunc::NegInt64
                | UnaryFunc::NegFloat32
                | UnaryFunc::NegFloat64
                | UnaryFunc::NegDecimal
                | UnaryFunc::NegAPD
                | UnaryFunc::CastBoolToString
                | UnaryFunc::CastBoolToStringNonstandard
                | UnaryFunc::CastInt32ToInt64
                | UnaryFunc::CastInt32ToString
                | UnaryFunc::CastInt64ToString
                | UnaryFunc::CastFloat32ToFloat64
                | UnaryFunc::CastFloat32ToString
                | UnaryFunc::CastFloat64ToString
                | UnaryFunc::CastStringToBytes
                | UnaryFunc::CastDateToTimestamp
                | UnaryFunc::CastDateToTimestampTz
                | UnaryFunc::CastDateToString
                | UnaryFunc::CastTimeToInterval
                | UnaryFunc::CastTimeToString
        )
    }
}

impl fmt::Display for UnaryFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UnaryFunc::Not => f.write_str("!"),
            UnaryFunc::IsNull => f.write_str("isnull"),
            UnaryFunc::NegInt32 => f.write_str("-"),
            UnaryFunc::NegInt64 => f.write_str("-"),
            UnaryFunc::NegFloat32 => f.write_str("-"),
            UnaryFunc::NegFloat64 => f.write_str("-"),
            UnaryFunc::NegDecimal => f.write_str("-"),
            UnaryFunc::NegAPD => f.write_str("-"),
            UnaryFunc::NegInterval => f.write_str("-"),
            UnaryFunc::AbsInt32 => f.write_str("abs"),
            UnaryFunc::AbsInt64 => f.write_str("abs"),
            UnaryFunc::AbsDecimal => f.write_str("abs"),
            UnaryFunc::AbsAPD => f.write_str("abs"),
            UnaryFunc::AbsFloat32 => f.write_str("abs"),
            UnaryFunc::AbsFloat64 => f.write_str("abs"),
            UnaryFunc::CastBoolToString => f.write_str("booltostr"),
            UnaryFunc::CastBoolToStringNonstandard => f.write_str("booltostrns"),
            UnaryFunc::CastBoolToInt32 => f.write_str("booltoi32"),
            UnaryFunc::CastInt32ToBool => f.write_str("i32tobool"),
            UnaryFunc::CastInt32ToFloat32 => f.write_str("i32tof32"),
            UnaryFunc::CastInt32ToFloat64 => f.write_str("i32tof64"),
            UnaryFunc::CastInt32ToInt64 => f.write_str("i32toi64"),
            UnaryFunc::CastInt32ToOid => f.write_str("i32tooid"),
            UnaryFunc::CastInt32ToString => f.write_str("i32tostr"),
            UnaryFunc::CastInt32ToDecimal => f.write_str("i32todec"),
            UnaryFunc::CastInt32ToAPD(..) => f.write_str("i32toapd"),
            UnaryFunc::CastOidToInt32 => f.write_str("oidtoi32"),
            UnaryFunc::CastInt64ToInt32 => f.write_str("i64toi32"),
            UnaryFunc::CastInt64ToBool => f.write_str("i64tobool"),
            UnaryFunc::CastInt64ToDecimal => f.write_str("i64todec"),
            UnaryFunc::CastInt64ToAPD(..) => f.write_str("i64toapd"),
            UnaryFunc::CastInt64ToFloat32 => f.write_str("i64tof32"),
            UnaryFunc::CastInt64ToFloat64 => f.write_str("i64tof64"),
            UnaryFunc::CastInt64ToString => f.write_str("i64tostr"),
            UnaryFunc::CastFloat32ToInt64 => f.write_str("f32toi64"),
            UnaryFunc::CastFloat32ToFloat64 => f.write_str("f32tof64"),
            UnaryFunc::CastFloat32ToString => f.write_str("f32tostr"),
            UnaryFunc::CastFloat32ToInt32 => f.write_str("f32toi32"),
            UnaryFunc::CastFloat32ToDecimal(_) => f.write_str("f32todec"),
            UnaryFunc::CastFloat32ToAPD(_) => f.write_str("f32toapd"),
            UnaryFunc::CastFloat64ToInt32 => f.write_str("f64toi32"),
            UnaryFunc::CastFloat64ToInt64 => f.write_str("f64toi64"),
            UnaryFunc::CastFloat64ToFloat32 => f.write_str("f64tof32"),
            UnaryFunc::CastFloat64ToString => f.write_str("f64tostr"),
            UnaryFunc::CastFloat64ToDecimal(_) => f.write_str("f64todec"),
            UnaryFunc::CastFloat64ToAPD(_) => f.write_str("f64toapd"),
            UnaryFunc::CastDecimalToInt32(_) => f.write_str("dectoi32"),
            UnaryFunc::CastDecimalToInt64(_) => f.write_str("dectoi64"),
            UnaryFunc::CastAPDToInt32 => f.write_str("apdtoi32"),
            UnaryFunc::CastAPDToInt64 => f.write_str("apdtoi64"),
            UnaryFunc::CastDecimalToString(_) => f.write_str("dectostr"),
            UnaryFunc::CastAPDToString => f.write_str("numerictostr"),
            UnaryFunc::CastAPDToFloat32 => f.write_str("apdtof32"),
            UnaryFunc::CastAPDToFloat64 => f.write_str("apdtof64"),
            UnaryFunc::CastSignificandToFloat32 => f.write_str("dectof32"),
            UnaryFunc::CastSignificandToFloat64 => f.write_str("dectof64"),
            UnaryFunc::CastStringToBool => f.write_str("strtobool"),
            UnaryFunc::CastStringToBytes => f.write_str("strtobytes"),
            UnaryFunc::CastStringToInt32 => f.write_str("strtoi32"),
            UnaryFunc::CastStringToInt64 => f.write_str("strtoi64"),
            UnaryFunc::CastStringToFloat32 => f.write_str("strtof32"),
            UnaryFunc::CastStringToFloat64 => f.write_str("strtof64"),
            UnaryFunc::CastStringToDecimal(_) => f.write_str("strtodec"),
            UnaryFunc::CastStringToAPD(_) => f.write_str("strtoapd"),
            UnaryFunc::CastStringToDate => f.write_str("strtodate"),
            UnaryFunc::CastStringToArray { .. } => f.write_str("strtoarray"),
            UnaryFunc::CastStringToList { .. } => f.write_str("strtolist"),
            UnaryFunc::CastStringToMap { .. } => f.write_str("strtomap"),
            UnaryFunc::CastStringToTime => f.write_str("strtotime"),
            UnaryFunc::CastStringToTimestamp => f.write_str("strtots"),
            UnaryFunc::CastStringToTimestampTz => f.write_str("strtotstz"),
            UnaryFunc::CastStringToInterval => f.write_str("strtoiv"),
            UnaryFunc::CastStringToUuid => f.write_str("strtouuid"),
            UnaryFunc::CastDateToTimestamp => f.write_str("datetots"),
            UnaryFunc::CastDateToTimestampTz => f.write_str("datetotstz"),
            UnaryFunc::CastDateToString => f.write_str("datetostr"),
            UnaryFunc::CastTimeToInterval => f.write_str("timetoiv"),
            UnaryFunc::CastTimeToString => f.write_str("timetostr"),
            UnaryFunc::CastTimestampToDate => f.write_str("tstodate"),
            UnaryFunc::CastTimestampToTimestampTz => f.write_str("tstotstz"),
            UnaryFunc::CastTimestampToString => f.write_str("tstostr"),
            UnaryFunc::CastTimestampTzToDate => f.write_str("tstodate"),
            UnaryFunc::CastTimestampTzToTimestamp => f.write_str("tstztots"),
            UnaryFunc::CastTimestampTzToString => f.write_str("tstztostr"),
            UnaryFunc::CastIntervalToString => f.write_str("ivtostr"),
            UnaryFunc::CastIntervalToTime => f.write_str("ivtotime"),
            UnaryFunc::CastBytesToString => f.write_str("bytestostr"),
            UnaryFunc::CastStringToJsonb => f.write_str("strtojsonb"),
            UnaryFunc::CastJsonbOrNullToJsonb => f.write_str("jsonb?tojsonb"),
            UnaryFunc::CastJsonbToString => f.write_str("jsonbtostr"),
            UnaryFunc::CastJsonbToInt32 => f.write_str("jsonbtoi32"),
            UnaryFunc::CastJsonbToInt64 => f.write_str("jsonbtoi64"),
            UnaryFunc::CastJsonbToFloat32 => f.write_str("jsonbtof32"),
            UnaryFunc::CastJsonbToFloat64 => f.write_str("jsonbtof64"),
            UnaryFunc::CastJsonbToBool => f.write_str("jsonbtobool"),
            UnaryFunc::CastJsonbToDecimal(_) => f.write_str("jsonbtodec"),
            UnaryFunc::CastJsonbToAPD(_) => f.write_str("jsonbtoapd"),
            UnaryFunc::CastUuidToString => f.write_str("uuidtostr"),
            UnaryFunc::CastRecordToString { .. } => f.write_str("recordtostr"),
            UnaryFunc::CastArrayToString { .. } => f.write_str("arraytostr"),
            UnaryFunc::CastListToString { .. } => f.write_str("listtostr"),
            UnaryFunc::CastList1ToList2 { .. } => f.write_str("list1tolist2"),
            UnaryFunc::CastMapToString { .. } => f.write_str("maptostr"),
            UnaryFunc::CastInPlace { .. } => f.write_str("castinplace"),
            UnaryFunc::CeilFloat32 => f.write_str("ceilf32"),
            UnaryFunc::CeilFloat64 => f.write_str("ceilf64"),
            UnaryFunc::CeilDecimal(_) => f.write_str("ceildec"),
            UnaryFunc::CeilAPD => f.write_str("ceilapd"),
            UnaryFunc::FloorFloat32 => f.write_str("floorf32"),
            UnaryFunc::FloorFloat64 => f.write_str("floorf64"),
            UnaryFunc::FloorDecimal(_) => f.write_str("floordec"),
            UnaryFunc::FloorAPD => f.write_str("floorapd"),
            UnaryFunc::SqrtFloat64 => f.write_str("sqrtf64"),
            UnaryFunc::SqrtDec(_) => f.write_str("sqrtdec"),
            UnaryFunc::SqrtAPD => f.write_str("sqrtapd"),
            UnaryFunc::CbrtFloat64 => f.write_str("cbrtf64"),
            UnaryFunc::Ascii => f.write_str("ascii"),
            UnaryFunc::CharLength => f.write_str("char_length"),
            UnaryFunc::BitLengthBytes => f.write_str("bit_length"),
            UnaryFunc::BitLengthString => f.write_str("bit_length"),
            UnaryFunc::ByteLengthBytes => f.write_str("byte_length"),
            UnaryFunc::ByteLengthString => f.write_str("byte_length"),
            UnaryFunc::IsRegexpMatch(regex) => write!(f, "{} ~", regex.as_str().quoted()),
            UnaryFunc::RegexpMatch(regex) => write!(f, "regexp_match[{}]", regex.as_str()),
            UnaryFunc::DatePartInterval(units) => write!(f, "date_part_{}_iv", units),
            UnaryFunc::DatePartTimestamp(units) => write!(f, "date_part_{}_ts", units),
            UnaryFunc::DatePartTimestampTz(units) => write!(f, "date_part_{}_tstz", units),
            UnaryFunc::DateTruncTimestamp(units) => write!(f, "date_trunc_{}_ts", units),
            UnaryFunc::DateTruncTimestampTz(units) => write!(f, "date_trunc_{}_tstz", units),
            UnaryFunc::TimezoneTimestamp(tz) => write!(f, "timezone_{}_ts", tz),
            UnaryFunc::TimezoneTimestampTz(tz) => write!(f, "timezone_{}_tstz", tz),
            UnaryFunc::TimezoneTime { tz, .. } => write!(f, "timezone_{}_t", tz),
            UnaryFunc::ToTimestamp => f.write_str("tots"),
            UnaryFunc::JsonbArrayLength => f.write_str("jsonb_array_length"),
            UnaryFunc::JsonbTypeof => f.write_str("jsonb_typeof"),
            UnaryFunc::JsonbStripNulls => f.write_str("jsonb_strip_nulls"),
            UnaryFunc::JsonbPretty => f.write_str("jsonb_pretty"),
            UnaryFunc::RoundFloat32 => f.write_str("roundf32"),
            UnaryFunc::RoundFloat64 => f.write_str("roundf64"),
            UnaryFunc::RoundDecimal(_) => f.write_str("roundunary"),
            UnaryFunc::RoundAPD => f.write_str("roundapd"),
            UnaryFunc::TrimWhitespace => f.write_str("btrim"),
            UnaryFunc::TrimLeadingWhitespace => f.write_str("ltrim"),
            UnaryFunc::TrimTrailingWhitespace => f.write_str("rtrim"),
            UnaryFunc::RecordGet(i) => write!(f, "record_get[{}]", i),
            UnaryFunc::ListLength => f.write_str("list_length"),
            UnaryFunc::Upper => f.write_str("upper"),
            UnaryFunc::Lower => f.write_str("lower"),
            UnaryFunc::Cos => f.write_str("cos"),
            UnaryFunc::Cosh => f.write_str("cosh"),
            UnaryFunc::Sin => f.write_str("sin"),
            UnaryFunc::Sinh => f.write_str("sinh"),
            UnaryFunc::Tan => f.write_str("tan"),
            UnaryFunc::Tanh => f.write_str("tanh"),
            UnaryFunc::Cot => f.write_str("cot"),
            UnaryFunc::Log10 => f.write_str("log10f64"),
            UnaryFunc::Log10Decimal(_) => f.write_str("log10dec"),
            UnaryFunc::Log10APD => f.write_str("log10apd"),
            UnaryFunc::Ln => f.write_str("lnf64"),
            UnaryFunc::LnDecimal(_) => f.write_str("lndec"),
            UnaryFunc::LnAPD => f.write_str("lnapd"),
            UnaryFunc::ExpDecimal(_) => f.write_str("expdec"),
            UnaryFunc::ExpAPD => f.write_str("expapd"),
            UnaryFunc::Exp => f.write_str("expf64"),
            UnaryFunc::Sleep => f.write_str("mz_sleep"),
            UnaryFunc::RescaleAPD(..) => f.write_str("rescale_apd"),
            UnaryFunc::PgColumnSize => f.write_str("pg_column_size"),
            UnaryFunc::MzRowSize => f.write_str("mz_row_size"),
        }
    }
}

fn coalesce<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
    exprs: &'a [MirScalarExpr],
) -> Result<Datum<'a>, EvalError> {
    for e in exprs {
        let d = e.eval(datums, temp_storage)?;
        if !d.is_null() {
            return Ok(d);
        }
    }
    Ok(Datum::Null)
}

fn text_concat_binary<'a>(a: Datum<'a>, b: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    buf.push_str(a.unwrap_str());
    buf.push_str(b.unwrap_str());
    Datum::String(temp_storage.push_string(buf))
}

fn text_concat_variadic<'a>(datums: &[Datum<'a>], temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    for d in datums {
        if !d.is_null() {
            buf.push_str(d.unwrap_str());
        }
    }
    Datum::String(temp_storage.push_string(buf))
}

fn pad_leading<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let string = datums[0].unwrap_str();

    let len = match usize::try_from(datums[1].unwrap_int64()) {
        Ok(len) => len,
        Err(_) => {
            return Err(EvalError::InvalidParameterValue(
                "length must be nonnegative".to_owned(),
            ))
        }
    };

    let pad_string = if datums.len() == 3 {
        datums[2].unwrap_str()
    } else {
        " "
    };

    let (end_char, end_char_byte_offset) = string
        .chars()
        .take(len)
        .fold((0, 0), |acc, char| (acc.0 + 1, acc.1 + char.len_utf8()));

    let mut buf = String::with_capacity(len);
    if len == end_char {
        buf.push_str(&string[0..end_char_byte_offset]);
    } else {
        buf.extend(pad_string.chars().cycle().take(len - end_char));
        buf.push_str(string);
    }

    Ok(Datum::String(temp_storage.push_string(buf)))
}

fn substr<'a>(datums: &[Datum<'a>]) -> Result<Datum<'a>, EvalError> {
    let s: &'a str = datums[0].unwrap_str();

    let raw_start_idx = datums[1].unwrap_int64() - 1;
    let start_idx = match usize::try_from(cmp::max(raw_start_idx, 0)) {
        Ok(i) => i,
        Err(_) => {
            return Err(EvalError::InvalidParameterValue(format!(
                "substring starting index ({}) exceeds min/max position",
                raw_start_idx
            )))
        }
    } as usize;

    let mut char_indices = s.char_indices();
    let get_str_index = |(index, _char)| index;

    let str_len = s.len();
    let start_char_idx = char_indices
        .nth(start_idx as usize)
        .map_or(str_len, &get_str_index);

    if datums.len() == 3 {
        let end_idx = match datums[2].unwrap_int64() {
            e if e < 0 => {
                return Err(EvalError::InvalidParameterValue(
                    "negative substring length not allowed".to_owned(),
                ))
            }
            e if e == 0 || e + raw_start_idx < 1 => return Ok(Datum::String("")),
            e => {
                let e = cmp::min(raw_start_idx + e - 1, e - 1);
                match usize::try_from(e) {
                    Ok(i) => i,
                    Err(_) => {
                        return Err(EvalError::InvalidParameterValue(format!(
                            "substring length ({}) exceeds max position",
                            e
                        )))
                    }
                }
            }
        };

        let end_char_idx = char_indices.nth(end_idx).map_or(str_len, &get_str_index);

        Ok(Datum::String(&s[start_char_idx..end_char_idx]))
    } else {
        Ok(Datum::String(&s[start_char_idx..]))
    }
}

fn split_part<'a>(datums: &[Datum<'a>]) -> Result<Datum<'a>, EvalError> {
    let string = datums[0].unwrap_str();
    let delimiter = datums[1].unwrap_str();

    // Provided index value begins at 1, not 0.
    let index = match usize::try_from(datums[2].unwrap_int64() - 1) {
        Ok(index) => index,
        Err(_) => {
            return Err(EvalError::InvalidParameterValue(
                "field position must be greater than zero".to_owned(),
            ))
        }
    };

    // If the provided delimiter is the empty string,
    // PostgreSQL does not break the string into individual
    // characters. Instead, it generates the following parts: [string].
    if delimiter.is_empty() {
        if index == 0 {
            return Ok(datums[0]);
        } else {
            return Ok(Datum::String(""));
        }
    }

    // If provided index is greater than the number of split parts,
    // return an empty string.
    Ok(Datum::String(
        string.split(delimiter).nth(index).unwrap_or(""),
    ))
}

fn is_like_pattern_match_dynamic<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    case_insensitive: bool,
) -> Result<Datum<'a>, EvalError> {
    let haystack = a.unwrap_str();
    let flags = if case_insensitive { "i" } else { "" };
    let needle = like_pattern::build_regex(b.unwrap_str(), flags)?;
    Ok(Datum::from(needle.is_match(haystack.as_ref())))
}

fn is_regexp_match_static<'a>(a: Datum<'a>, needle: &regex::Regex) -> Datum<'a> {
    let haystack = a.unwrap_str();
    Datum::from(needle.is_match(haystack))
}

fn is_regexp_match_dynamic<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    case_insensitive: bool,
) -> Result<Datum<'a>, EvalError> {
    let haystack = a.unwrap_str();
    let needle = build_regex(b.unwrap_str(), if case_insensitive { "i" } else { "" })?;
    Ok(Datum::from(needle.is_match(haystack)))
}

fn regexp_match_dynamic<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let haystack = datums[0];
    let needle = datums[1].unwrap_str();
    let flags = match datums.get(2) {
        Some(d) => d.unwrap_str(),
        None => "",
    };
    let needle = build_regex(needle, flags)?;
    regexp_match_static(haystack, temp_storage, &needle)
}

fn regexp_match_static<'a>(
    haystack: Datum<'a>,
    temp_storage: &'a RowArena,
    needle: &regex::Regex,
) -> Result<Datum<'a>, EvalError> {
    let mut row = Row::default();
    if needle.captures_len() > 1 {
        // The regex contains capture groups, so return an array containing the
        // matched text in each capture group, unless the entire match fails.
        // Individual capture groups may also be null if that group did not
        // participate in the match.
        match needle.captures(haystack.unwrap_str()) {
            None => row.push(Datum::Null),
            Some(captures) => row.push_array(
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
        match needle.find(haystack.unwrap_str()) {
            None => row.push(Datum::Null),
            Some(mtch) => row.push_array(
                &[ArrayDimension {
                    lower_bound: 1,
                    length: 1,
                }],
                iter::once(Datum::String(mtch.as_str())),
            )?,
        };
    };
    Ok(temp_storage.push_unary_row(row))
}

pub fn build_regex(needle: &str, flags: &str) -> Result<regex::Regex, EvalError> {
    let mut regex = RegexBuilder::new(needle);
    for f in flags.chars() {
        match f {
            'i' => {
                regex.case_insensitive(true);
            }
            'c' => {
                regex.case_insensitive(false);
            }
            _ => return Err(EvalError::InvalidRegexFlag(f)),
        }
    }
    Ok(regex.build()?)
}

pub fn hmac_string<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let to_digest = datums[0].unwrap_str().as_bytes();
    let key = datums[1].unwrap_str().as_bytes();
    let typ = datums[2].unwrap_str();
    hmac_inner(to_digest, key, typ, temp_storage)
}

pub fn hmac_bytes<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let to_digest = datums[0].unwrap_bytes();
    let key = datums[1].unwrap_bytes();
    let typ = datums[2].unwrap_str();
    hmac_inner(to_digest, key, typ, temp_storage)
}

pub fn hmac_inner<'a>(
    to_digest: &[u8],
    key: &[u8],
    typ: &str,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let bytes = match typ {
        "md5" => {
            let mut mac = Hmac::<Md5>::new_varkey(key).expect("HMAC can take key of any size");
            mac.update(to_digest);
            mac.finalize().into_bytes().to_vec()
        }
        "sha1" => {
            let mut mac = Hmac::<Sha1>::new_varkey(key).expect("HMAC can take key of any size");
            mac.update(to_digest);
            mac.finalize().into_bytes().to_vec()
        }
        "sha224" => {
            let mut mac = Hmac::<Sha224>::new_varkey(key).expect("HMAC can take key of any size");
            mac.update(to_digest);
            mac.finalize().into_bytes().to_vec()
        }
        "sha256" => {
            let mut mac = Hmac::<Sha256>::new_varkey(key).expect("HMAC can take key of any size");
            mac.update(to_digest);
            mac.finalize().into_bytes().to_vec()
        }
        "sha384" => {
            let mut mac = Hmac::<Sha384>::new_varkey(key).expect("HMAC can take key of any size");
            mac.update(to_digest);
            mac.finalize().into_bytes().to_vec()
        }
        "sha512" => {
            let mut mac = Hmac::<Sha512>::new_varkey(key).expect("HMAC can take key of any size");
            mac.update(to_digest);
            mac.finalize().into_bytes().to_vec()
        }
        other => return Err(EvalError::InvalidHashAlgorithm(other.to_owned())),
    };
    Ok(Datum::Bytes(temp_storage.push_bytes(bytes)))
}

fn repeat_string<'a>(
    string: Datum<'a>,
    count: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    Ok(Datum::String(
        temp_storage.push_string(
            string
                .unwrap_str()
                .repeat(usize::try_from(count.unwrap_int32()).unwrap_or(0)),
        ),
    ))
}

fn replace<'a>(datums: &[Datum<'a>], temp_storage: &'a RowArena) -> Datum<'a> {
    Datum::String(
        temp_storage.push_string(
            datums[0]
                .unwrap_str()
                .replace(datums[1].unwrap_str(), datums[2].unwrap_str()),
        ),
    )
}

fn jsonb_build_array<'a>(datums: &[Datum<'a>], temp_storage: &'a RowArena) -> Datum<'a> {
    if datums.iter().any(|datum| datum.is_null()) {
        // the inputs should all be valid jsonb types, but a casting error might produce a Datum::Null that needs to be propagated
        Datum::Null
    } else {
        temp_storage.make_datum(|packer| packer.push_list(datums))
    }
}

fn jsonb_build_object<'a>(datums: &[Datum<'a>], temp_storage: &'a RowArena) -> Datum<'a> {
    if datums.iter().any(|datum| datum.is_null()) {
        // the inputs should all be valid jsonb types, but a casting error might produce a Datum::Null that needs to be propagated
        Datum::Null
    } else {
        let mut kvs = datums.chunks(2).collect::<Vec<_>>();
        kvs.sort_by(|kv1, kv2| kv1[0].cmp(&kv2[0]));
        kvs.dedup_by(|kv1, kv2| kv1[0] == kv2[0]);
        temp_storage.make_datum(|packer| {
            packer.push_dict(kvs.into_iter().map(|kv| (kv[0].unwrap_str(), kv[1])))
        })
    }
}

/// Constructs a new multidimensional array out of an arbitrary number of
/// lower-dimensional arrays.
///
/// For example, if given three 1D arrays of length 2, this function will
/// construct a 2D array with dimensions 3x2.
///
/// The input datums in `datums` must all be arrays of the same dimensions.
/// (The arrays must also be of the same element type, but that is checked by
/// the SQL type system, rather than checked here at runtime.)
///
/// If all input arrays are zero-dimensional arrays, then the output is a zero-
/// dimensional array. Otherwise the lower bound of the additional dimension is
/// one and the length of the new dimension is equal to `datums.len()`.
fn array_create_multidim<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    // Per PostgreSQL, if all input arrays are zero dimensional, so is the
    // output.
    if datums.iter().all(|d| d.unwrap_array().dims().is_empty()) {
        let dims = &[];
        let datums = &[];
        let datum = temp_storage.try_make_datum(|packer| packer.push_array(dims, datums))?;
        return Ok(datum);
    }

    let mut dims = vec![ArrayDimension {
        lower_bound: 1,
        length: datums.len(),
    }];
    if let Some(d) = datums.first() {
        dims.extend(d.unwrap_array().dims());
    };
    let elements = datums
        .iter()
        .flat_map(|d| d.unwrap_array().elements().iter());
    let datum = temp_storage.try_make_datum(move |packer| packer.push_array(&dims, elements))?;
    Ok(datum)
}

/// Constructs a new zero or one dimensional array out of an arbitrary number of
/// scalars.
///
/// If `datums` is empty, constructs a zero-dimensional array. Otherwise,
/// constructs a one dimensional array whose lower bound is one and whose length
/// is equal to `datums.len()`.
fn array_create_scalar<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut dims = &[ArrayDimension {
        lower_bound: 1,
        length: datums.len(),
    }][..];
    if datums.is_empty() {
        // Per PostgreSQL, empty arrays are represented with zero dimensions,
        // not one dimension of zero length. We write this condition a little
        // strangely to satisfy the borrow checker while avoiding an allocation.
        dims = &[];
    }
    let datum = temp_storage.try_make_datum(|packer| packer.push_array(dims, datums))?;
    Ok(datum)
}

fn array_to_string<'a>(
    datums: &[Datum<'a>],
    elem_type: &ScalarType,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    if datums[0].is_null() || datums[1].is_null() {
        return Ok(Datum::Null);
    }
    let array = datums[0].unwrap_array();
    let delimiter = datums[1].unwrap_str();
    let null_str = match datums.get(2) {
        None | Some(Datum::Null) => None,
        Some(d) => Some(d.unwrap_str()),
    };

    let mut out = String::new();
    for elem in array.elements().iter() {
        if elem.is_null() {
            if let Some(null_str) = null_str {
                out.push_str(null_str);
                out.push_str(delimiter);
            }
        } else {
            stringify_datum(&mut out, elem, elem_type);
            out.push_str(delimiter);
        }
    }
    out.truncate(out.len() - delimiter.len()); // lop off last delimiter
    Ok(Datum::String(temp_storage.push_string(out)))
}

fn list_create<'a>(datums: &[Datum<'a>], temp_storage: &'a RowArena) -> Datum<'a> {
    temp_storage.make_datum(|packer| packer.push_list(datums))
}

fn cast_collection_to_string<'a>(
    a: Datum,
    ty: &ScalarType,
    temp_storage: &'a RowArena,
) -> Datum<'a> {
    let mut buf = String::new();
    stringify_datum(&mut buf, a, ty);
    Datum::String(temp_storage.push_string(buf))
}

fn stringify_datum<'a, B>(buf: &mut B, d: Datum<'a>, ty: &ScalarType) -> strconv::Nestable
where
    B: FormatBuffer,
{
    use ScalarType::*;
    match &ty {
        Bool => strconv::format_bool(buf, d.unwrap_bool()),
        Int32 | Oid => strconv::format_int32(buf, d.unwrap_int32()),
        Int64 => strconv::format_int64(buf, d.unwrap_int64()),
        Float32 => strconv::format_float32(buf, d.unwrap_float32()),
        Float64 => strconv::format_float64(buf, d.unwrap_float64()),
        Decimal(_, s) => strconv::format_decimal(buf, &d.unwrap_decimal().with_scale(*s)),
        APD { scale } => {
            let mut d = d.unwrap_apd();
            if let Some(scale) = scale {
                apd::rescale(&mut d.0, *scale).unwrap();
            }

            strconv::format_apd(buf, &d)
        }
        Date => strconv::format_date(buf, d.unwrap_date()),
        Time => strconv::format_time(buf, d.unwrap_time()),
        Timestamp => strconv::format_timestamp(buf, d.unwrap_timestamp()),
        TimestampTz => strconv::format_timestamptz(buf, d.unwrap_timestamptz()),
        Interval => strconv::format_interval(buf, d.unwrap_interval()),
        Bytes => strconv::format_bytes(buf, d.unwrap_bytes()),
        String => strconv::format_string(buf, d.unwrap_str()),
        Jsonb => strconv::format_jsonb(buf, JsonbRef::from_datum(d)),
        Uuid => strconv::format_uuid(buf, d.unwrap_uuid()),
        Record { fields, .. } => {
            let mut fields = fields.iter();
            strconv::format_record(buf, &d.unwrap_list(), |buf, d| {
                let (_name, ty) = fields.next().unwrap();
                if d.is_null() {
                    buf.write_null()
                } else {
                    stringify_datum(buf.nonnull_buffer(), d, &ty.scalar_type)
                }
            })
        }
        Array(elem_type) => strconv::format_array(
            buf,
            &d.unwrap_array().dims().into_iter().collect::<Vec<_>>(),
            &d.unwrap_array().elements(),
            |buf, d| {
                if d.is_null() {
                    buf.write_null()
                } else {
                    stringify_datum(buf.nonnull_buffer(), d, elem_type)
                }
            },
        ),
        List { element_type, .. } => strconv::format_list(buf, &d.unwrap_list(), |buf, d| {
            if d.is_null() {
                buf.write_null()
            } else {
                stringify_datum(buf.nonnull_buffer(), d, element_type)
            }
        }),
        Map { value_type, .. } => strconv::format_map(buf, &d.unwrap_map(), |buf, d| {
            if d.is_null() {
                buf.write_null()
            } else {
                stringify_datum(buf.nonnull_buffer(), d, value_type)
            }
        }),
    }
}

fn list_slice<'a>(datums: &[Datum<'a>], temp_storage: &'a RowArena) -> Datum<'a> {
    // Return value indicates whether this level's slices are empty results.
    fn slice_and_descend(d: Datum, ranges: &[(usize, usize)], row: &mut Row) -> bool {
        match ranges {
            [(start, n), ranges @ ..] if !d.is_null() => {
                let mut iter = d.unwrap_list().iter().skip(*start).take(*n).peekable();
                if iter.peek().is_none() {
                    row.push(Datum::Null);
                    true
                } else {
                    let mut empty_results = true;
                    let start = row.data().len();
                    row.push_list_with(|row| {
                        for d in iter {
                            // Determine if all higher-dimension slices produced empty results.
                            empty_results = slice_and_descend(d, ranges, row) && empty_results;
                        }
                    });

                    if empty_results {
                        // If all results were empty, delete the list, insert a
                        // NULL, and notify lower-order slices that your results
                        // were empty.

                        // SAFETY: `start` points to a datum boundary because a)
                        // it comes from a call to `row.data().len()` above,
                        // and b) recursive calls to `slice_and_descend` will
                        // not shrink the row. (The recursive calls may write
                        // data and then erase that data, but a recursive call
                        // will never erase data that it did not write itself.)
                        unsafe { row.truncate(start) }
                        row.push(Datum::Null);
                    }
                    empty_results
                }
            }
            _ => {
                row.push(d);
                // Slicing a NULL produces an empty result.
                d.is_null() && ranges.len() > 0
            }
        }
    }

    assert_eq!(
        datums.len() % 2,
        1,
        "expr::scalar::func::list_slice expects an odd number of arguments; 1 for list + 2 \
        for each start-end pair"
    );
    assert!(
        datums.len() > 2,
        "expr::scalar::func::list_slice expects at least 3 arguments; 1 for list + at least \
        one start-end pair"
    );

    let mut ranges = Vec::new();
    for (start, end) in datums[1..].iter().tuples::<(_, _)>() {
        let start = std::cmp::max(start.unwrap_int64(), 1);
        let end = end.unwrap_int64();

        if start > end {
            return Datum::Null;
        }

        ranges.push((start as usize - 1, (end - start) as usize + 1));
    }

    temp_storage.make_datum(|row| {
        slice_and_descend(datums[0], &ranges, row);
    })
}

fn record_get(a: Datum, i: usize) -> Datum {
    a.unwrap_list().iter().nth(i).unwrap()
}

fn list_length(a: Datum) -> Datum {
    Datum::Int64(a.unwrap_list().iter().count() as i64)
}

fn upper<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    Datum::String(temp_storage.push_string(a.unwrap_str().to_owned().to_uppercase()))
}

fn lower<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    Datum::String(temp_storage.push_string(a.unwrap_str().to_owned().to_lowercase()))
}

fn make_timestamp<'a>(datums: &[Datum<'a>]) -> Datum<'a> {
    let year: i32 = match datums[0].unwrap_int64().try_into() {
        Ok(year) => year,
        Err(_) => return Datum::Null,
    };
    let month: u32 = match datums[1].unwrap_int64().try_into() {
        Ok(month) => month,
        Err(_) => return Datum::Null,
    };
    let day: u32 = match datums[2].unwrap_int64().try_into() {
        Ok(day) => day,
        Err(_) => return Datum::Null,
    };
    let hour: u32 = match datums[3].unwrap_int64().try_into() {
        Ok(day) => day,
        Err(_) => return Datum::Null,
    };
    let minute: u32 = match datums[4].unwrap_int64().try_into() {
        Ok(day) => day,
        Err(_) => return Datum::Null,
    };
    let second_float = datums[5].unwrap_float64();
    let second = second_float as u32;
    let micros = ((second_float - second as f64) * 1_000_000.0) as u32;
    let date = match NaiveDate::from_ymd_opt(year, month, day) {
        Some(date) => date,
        None => return Datum::Null,
    };
    let timestamp = match date.and_hms_micro_opt(hour, minute, second, micros) {
        Some(timestamp) => timestamp,
        None => return Datum::Null,
    };
    Datum::Timestamp(timestamp)
}

fn trim_whitespace<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_str().trim_matches(' '))
}

fn position<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let substring: &'a str = a.unwrap_str();
    let string = b.unwrap_str();
    let char_index = string.find(substring);

    if let Some(char_index) = char_index {
        // find the index in char space
        let string_prefix = &string[0..char_index];

        let num_prefix_chars = string_prefix.chars().count();
        let num_prefix_chars =
            i32::try_from(num_prefix_chars).map_err(|_| EvalError::Int32OutOfRange)?;

        Ok(Datum::Int32(num_prefix_chars + 1))
    } else {
        Ok(Datum::Int32(0))
    }
}

fn right<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let string: &'a str = a.unwrap_str();
    let n = b.unwrap_int32();

    let mut byte_indices = string.char_indices().map(|(i, _)| i);

    let start_in_bytes = if n == 0 {
        string.len()
    } else if n > 0 {
        let n = usize::try_from(n - 1).map_err(|_| {
            EvalError::InvalidParameterValue(format!("invalid parameter n: {:?}", n))
        })?;
        // nth from the back
        byte_indices.rev().nth(n).unwrap_or(0)
    } else if n == i32::MIN {
        // this seems strange but Postgres behaves like this
        0
    } else {
        let n = n.abs();
        let n = usize::try_from(n).map_err(|_| {
            EvalError::InvalidParameterValue(format!("invalid parameter n: {:?}", n))
        })?;
        byte_indices.nth(n).unwrap_or_else(|| string.len())
    };

    Ok(Datum::String(&string[start_in_bytes..]))
}

fn trim<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let trim_chars = b.unwrap_str();

    Datum::from(a.unwrap_str().trim_matches(|c| trim_chars.contains(c)))
}

fn trim_leading_whitespace<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_str().trim_start_matches(' '))
}

fn trim_leading<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let trim_chars = b.unwrap_str();

    Datum::from(
        a.unwrap_str()
            .trim_start_matches(|c| trim_chars.contains(c)),
    )
}

fn trim_trailing_whitespace<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_str().trim_end_matches(' '))
}

fn trim_trailing<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let trim_chars = b.unwrap_str();

    Datum::from(a.unwrap_str().trim_end_matches(|c| trim_chars.contains(c)))
}

fn list_index<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let i = b.unwrap_int64();
    if i < 1 {
        return Datum::Null;
    }
    a.unwrap_list()
        .iter()
        .nth(i as usize - 1)
        .unwrap_or(Datum::Null)
}

fn array_length<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let i = match usize::try_from(b.unwrap_int64()) {
        Ok(0) | Err(_) => return Datum::Null,
        Ok(n) => n - 1,
    };
    match a.unwrap_array().dims().into_iter().nth(i) {
        None => Datum::Null,
        Some(dim) => Datum::Int64(dim.length as i64),
    }
}

fn array_index<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let i = b.unwrap_int64();
    if i < 1 {
        return Datum::Null;
    }
    a.unwrap_array()
        .elements()
        .iter()
        .nth(i as usize - 1)
        .unwrap_or(Datum::Null)
}

fn array_lower<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let i = b.unwrap_int64();
    if i < 1 {
        return Datum::Null;
    }
    match a.unwrap_array().dims().into_iter().nth(i as usize - 1) {
        Some(_) => Datum::Int64(1),
        None => Datum::Null,
    }
}

fn array_upper<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let i = b.unwrap_int64();
    if i < 1 {
        return Datum::Null;
    }
    match a.unwrap_array().dims().into_iter().nth(i as usize - 1) {
        Some(dim) => Datum::Int64(dim.length as i64),
        None => Datum::Null,
    }
}

fn list_length_max<'a>(a: Datum<'a>, b: Datum<'a>, max_dim: usize) -> Result<Datum<'a>, EvalError> {
    fn max_len_on_dim<'a>(d: Datum<'a>, on_dim: i64) -> Option<i64> {
        match d {
            Datum::List(i) => {
                let mut i = i.iter();
                if on_dim > 1 {
                    let mut max_len = None;
                    while let Some(Datum::List(i)) = i.next() {
                        max_len =
                            std::cmp::max(max_len_on_dim(Datum::List(i), on_dim - 1), max_len);
                    }
                    max_len
                } else {
                    Some(i.count() as i64)
                }
            }
            Datum::Null => None,
            _ => unreachable!(),
        }
    }

    let b = b.unwrap_int64();

    if b as usize > max_dim || b < 1 {
        Err(EvalError::InvalidDimension { max_dim, val: b })
    } else {
        Ok(match max_len_on_dim(a, b) {
            Some(l) => Datum::from(l),
            None => Datum::Null,
        })
    }
}

fn array_contains<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let array = Datum::unwrap_array(&b);
    Datum::from(array.elements().iter().any(|e| e == a))
}

fn list_list_concat<'a>(a: Datum<'a>, b: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    if a.is_null() {
        return b;
    } else if b.is_null() {
        return a;
    }

    let a = a.unwrap_list().iter();
    let b = b.unwrap_list().iter();

    temp_storage.make_datum(|packer| packer.push_list(a.chain(b)))
}

fn list_element_concat<'a>(a: Datum<'a>, b: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    temp_storage.make_datum(|packer| {
        packer.push_list_with(|packer| {
            if !a.is_null() {
                for elem in a.unwrap_list().iter() {
                    packer.push(elem);
                }
            }
            packer.push(b);
        })
    })
}

fn element_list_concat<'a>(a: Datum<'a>, b: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    temp_storage.make_datum(|packer| {
        packer.push_list_with(|packer| {
            packer.push(a);
            if !b.is_null() {
                for elem in b.unwrap_list().iter() {
                    packer.push(elem);
                }
            }
        })
    })
}

fn digest_string<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let to_digest = a.unwrap_str().as_bytes();
    digest_inner(to_digest, b, temp_storage)
}

fn digest_bytes<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let to_digest = a.unwrap_bytes();
    digest_inner(to_digest, b, temp_storage)
}

fn digest_inner<'a>(
    bytes: &[u8],
    digest_fn: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let bytes = match digest_fn.unwrap_str() {
        "md5" => Md5::digest(bytes).to_vec(),
        "sha1" => Sha1::digest(bytes).to_vec(),
        "sha224" => Sha224::digest(bytes).to_vec(),
        "sha256" => Sha256::digest(bytes).to_vec(),
        "sha384" => Sha384::digest(bytes).to_vec(),
        "sha512" => Sha512::digest(bytes).to_vec(),
        other => return Err(EvalError::InvalidHashAlgorithm(other.to_owned())),
    };
    Ok(Datum::Bytes(temp_storage.push_bytes(bytes)))
}

fn mz_render_typemod<'a>(
    oid: Datum<'a>,
    typmod: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Datum<'a> {
    let oid = oid.unwrap_int32();
    let mut typmod = typmod.unwrap_int32();
    let typmod_base = 65_536;

    let inner = if matches!(Type::from_oid(oid as u32), Some(Type::Numeric)) && typmod >= 0 {
        typmod -= 4;
        if typmod < 0 {
            temp_storage.push_string(format!("({},{})", 65_535, typmod_base + typmod))
        } else {
            temp_storage.push_string(format!(
                "({},{})",
                typmod / typmod_base,
                typmod % typmod_base
            ))
        }
    } else {
        ""
    };

    Datum::String(inner)
}

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzEnumReflect,
)]
pub enum VariadicFunc {
    Coalesce,
    Concat,
    MakeTimestamp,
    PadLeading,
    Substr,
    Replace,
    JsonbBuildArray,
    JsonbBuildObject,
    ArrayCreate {
        // We need to know the element type to type empty arrays.
        elem_type: ScalarType,
    },
    ArrayToString {
        elem_type: ScalarType,
    },
    ListCreate {
        // We need to know the element type to type empty lists.
        elem_type: ScalarType,
    },
    RecordCreate {
        field_names: Vec<ColumnName>,
    },
    ListSlice,
    SplitPart,
    RegexpMatch,
    HmacString,
    HmacBytes,
}

impl VariadicFunc {
    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [MirScalarExpr],
    ) -> Result<Datum<'a>, EvalError> {
        macro_rules! eager {
            ($func:ident $(, $args:expr)*) => {{
                let ds = exprs.iter()
                    .map(|e| e.eval(datums, temp_storage))
                    .collect::<Result<Vec<_>, _>>()?;
                if self.propagates_nulls() && ds.iter().any(|d| d.is_null()) {
                    return Ok(Datum::Null);
                }
                $func(&ds $(, $args)*)
            }}
        }

        match self {
            VariadicFunc::Coalesce => coalesce(datums, temp_storage, exprs),
            VariadicFunc::Concat => Ok(eager!(text_concat_variadic, temp_storage)),
            VariadicFunc::MakeTimestamp => Ok(eager!(make_timestamp)),
            VariadicFunc::PadLeading => eager!(pad_leading, temp_storage),
            VariadicFunc::Substr => eager!(substr),
            VariadicFunc::Replace => Ok(eager!(replace, temp_storage)),
            VariadicFunc::JsonbBuildArray => Ok(eager!(jsonb_build_array, temp_storage)),
            VariadicFunc::JsonbBuildObject => Ok(eager!(jsonb_build_object, temp_storage)),
            VariadicFunc::ArrayCreate {
                elem_type: ScalarType::Array(_),
            } => eager!(array_create_multidim, temp_storage),
            VariadicFunc::ArrayCreate { .. } => eager!(array_create_scalar, temp_storage),
            VariadicFunc::ArrayToString { elem_type } => {
                eager!(array_to_string, elem_type, temp_storage)
            }
            VariadicFunc::ListCreate { .. } | VariadicFunc::RecordCreate { .. } => {
                Ok(eager!(list_create, temp_storage))
            }
            VariadicFunc::ListSlice => Ok(eager!(list_slice, temp_storage)),
            VariadicFunc::SplitPart => eager!(split_part),
            VariadicFunc::RegexpMatch => eager!(regexp_match_dynamic, temp_storage),
            VariadicFunc::HmacString => eager!(hmac_string, temp_storage),
            VariadicFunc::HmacBytes => eager!(hmac_bytes, temp_storage),
        }
    }

    pub fn output_type(&self, input_types: Vec<ColumnType>) -> ColumnType {
        use VariadicFunc::*;
        match self {
            Coalesce => {
                assert!(input_types.len() > 0);
                debug_assert!(
                    input_types
                        .windows(2)
                        .all(|w| w[0].scalar_type == w[1].scalar_type),
                    "coalesce inputs did not have uniform type: {:?}",
                    input_types
                );
                input_types.into_first().nullable(true)
            }
            Concat => ScalarType::String.nullable(true),
            MakeTimestamp => ScalarType::Timestamp.nullable(true),
            PadLeading => ScalarType::String.nullable(true),
            Substr => ScalarType::String.nullable(true),
            Replace => ScalarType::String.nullable(true),
            JsonbBuildArray | JsonbBuildObject => ScalarType::Jsonb.nullable(true),
            ArrayCreate { elem_type } => {
                debug_assert!(
                    input_types.iter().all(|t| t.scalar_type == *elem_type),
                    "Args to ArrayCreate should have types that are compatible with the elem_type"
                );
                match elem_type {
                    ScalarType::Array(_) => elem_type.clone().nullable(false),
                    _ => ScalarType::Array(Box::new(elem_type.clone())).nullable(false),
                }
            }
            ArrayToString { .. } => ScalarType::String.nullable(true),
            ListCreate { elem_type } => {
                debug_assert!(
                    input_types.iter().all(|t| t.scalar_type == *elem_type),
                    "Args to ListCreate should have types that are compatible with the elem_type"
                );
                ScalarType::List {
                    element_type: Box::new(elem_type.clone()),
                    custom_oid: None,
                }
                .nullable(false)
            }
            ListSlice { .. } => input_types[0].scalar_type.clone().nullable(true),
            RecordCreate { field_names } => ScalarType::Record {
                fields: field_names
                    .clone()
                    .into_iter()
                    .zip(input_types.into_iter())
                    .collect(),
                custom_oid: None,
                custom_name: None,
            }
            .nullable(true),
            SplitPart => ScalarType::String.nullable(true),
            RegexpMatch => ScalarType::Array(Box::new(ScalarType::String)).nullable(true),
            HmacString | HmacBytes => ScalarType::Bytes.nullable(true),
        }
    }

    /// Whether the function output is NULL if any of its inputs are NULL.
    pub fn propagates_nulls(&self) -> bool {
        !matches!(
            self,
            VariadicFunc::Coalesce
                | VariadicFunc::Concat
                | VariadicFunc::JsonbBuildArray
                | VariadicFunc::JsonbBuildObject
                | VariadicFunc::ListCreate { .. }
                | VariadicFunc::RecordCreate { .. }
                | VariadicFunc::ArrayCreate { .. }
                | VariadicFunc::ArrayToString { .. }
        )
    }
}

impl fmt::Display for VariadicFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VariadicFunc::Coalesce => f.write_str("coalesce"),
            VariadicFunc::Concat => f.write_str("concat"),
            VariadicFunc::MakeTimestamp => f.write_str("makets"),
            VariadicFunc::PadLeading => f.write_str("lpad"),
            VariadicFunc::Substr => f.write_str("substr"),
            VariadicFunc::Replace => f.write_str("replace"),
            VariadicFunc::JsonbBuildArray => f.write_str("jsonb_build_array"),
            VariadicFunc::JsonbBuildObject => f.write_str("jsonb_build_object"),
            VariadicFunc::ArrayCreate { .. } => f.write_str("array_create"),
            VariadicFunc::ArrayToString { .. } => f.write_str("array_to_string"),
            VariadicFunc::ListCreate { .. } => f.write_str("list_create"),
            VariadicFunc::RecordCreate { .. } => f.write_str("record_create"),
            VariadicFunc::ListSlice => f.write_str("list_slice"),
            VariadicFunc::SplitPart => f.write_str("split_string"),
            VariadicFunc::RegexpMatch => f.write_str("regexp_match"),
            VariadicFunc::HmacString | VariadicFunc::HmacBytes => f.write_str("hmac"),
        }
    }
}

#[cfg(test)]
mod test {
    use chrono::prelude::*;

    use super::*;

    #[test]
    fn add_interval_months() {
        let dt = ym(2000, 1);

        assert_eq!(add_timestamp_months(dt, 0), dt);
        assert_eq!(add_timestamp_months(dt, 1), ym(2000, 2));
        assert_eq!(add_timestamp_months(dt, 12), ym(2001, 1));
        assert_eq!(add_timestamp_months(dt, 13), ym(2001, 2));
        assert_eq!(add_timestamp_months(dt, 24), ym(2002, 1));
        assert_eq!(add_timestamp_months(dt, 30), ym(2002, 7));

        // and negatives
        assert_eq!(add_timestamp_months(dt, -1), ym(1999, 12));
        assert_eq!(add_timestamp_months(dt, -12), ym(1999, 1));
        assert_eq!(add_timestamp_months(dt, -13), ym(1998, 12));
        assert_eq!(add_timestamp_months(dt, -24), ym(1998, 1));
        assert_eq!(add_timestamp_months(dt, -30), ym(1997, 7));

        // and going over a year boundary by less than a year
        let dt = ym(1999, 12);
        assert_eq!(add_timestamp_months(dt, 1), ym(2000, 1));
        let end_of_month_dt = NaiveDate::from_ymd(1999, 12, 31).and_hms(9, 9, 9);
        assert_eq!(
            // leap year
            add_timestamp_months(end_of_month_dt, 2),
            NaiveDate::from_ymd(2000, 2, 29).and_hms(9, 9, 9),
        );
        assert_eq!(
            // not leap year
            add_timestamp_months(end_of_month_dt, 14),
            NaiveDate::from_ymd(2001, 2, 28).and_hms(9, 9, 9),
        );
    }

    fn ym(year: i32, month: u32) -> NaiveDateTime {
        NaiveDate::from_ymd(year, month, 1).and_hms(9, 9, 9)
    }
}
