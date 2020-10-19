// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::{self, Ordering};
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::str;

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use encoding::label::encoding_from_whatwg_label;
use encoding::DecoderTrap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use ore::collections::CollectionExt;
use ore::fmt::FormatBuffer;
use ore::result::ResultExt;
use repr::adt::array::ArrayDimension;
use repr::adt::datetime::DateTimeUnits;
use repr::adt::decimal::MAX_DECIMAL_PRECISION;
use repr::adt::interval::Interval;
use repr::adt::jsonb::JsonbRef;
use repr::adt::regex::Regex;
use repr::{strconv, ColumnName, ColumnType, Datum, RowArena, RowPacker, ScalarType};

use crate::scalar::func::format::DateTimeFormat;
use crate::{like_pattern, EvalError, ScalarExpr};

mod format;

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
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
    a_expr: &'a ScalarExpr,
    b_expr: &'a ScalarExpr,
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
    a_expr: &'a ScalarExpr,
    b_expr: &'a ScalarExpr,
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

fn abs_float32<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float32().abs())
}

fn abs_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float64().abs())
}

fn cast_bool_to_string_explicit<'a>(a: Datum<'a>) -> Datum<'a> {
    // N.B. this function differs from `cast_bool_to_string_implicit` because
    // the SQL specification requires `true` and `false` to be spelled out
    // in explicit casts, while PostgreSQL prefers its more concise `t` and `f`
    // representation in implicit casts.
    match a.unwrap_bool() {
        true => Datum::from("true"),
        false => Datum::from("false"),
    }
}

fn cast_bool_to_string_implicit<'a>(a: Datum<'a>) -> Datum<'a> {
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
    // TODO(benesch): is this cast valid?
    Datum::from(a.unwrap_int32() as f32)
}

fn cast_int32_to_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    // TODO(benesch): is this cast valid?
    Datum::from(f64::from(a.unwrap_int32()))
}

fn cast_int32_to_int64<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(i64::from(a.unwrap_int32()))
}

fn cast_int32_to_decimal<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(i128::from(a.unwrap_int32()))
}

fn cast_int64_to_bool<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int64() != 0)
}

fn cast_int64_to_int32<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    match i32::try_from(a.unwrap_int64()) {
        Ok(n) => Ok(Datum::from(n)),
        Err(_) => Err(EvalError::IntegerOutOfRange),
    }
}

fn cast_int64_to_decimal<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(i128::from(a.unwrap_int64()))
}

fn cast_int64_to_float32<'a>(a: Datum<'a>) -> Datum<'a> {
    // TODO(benesch): is this cast valid?
    Datum::from(a.unwrap_int64() as f32)
}

fn cast_int64_to_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    // TODO(benesch): is this cast valid?
    Datum::from(a.unwrap_int64() as f64)
}

fn cast_int64_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_int64(&mut buf, a.unwrap_int64());
    Datum::String(temp_storage.push_string(buf))
}

fn cast_float32_to_int64<'a>(a: Datum<'a>) -> Datum<'a> {
    // TODO(benesch): this is undefined behavior if the f32 doesn't fit in an
    // i64 (https://github.com/rust-lang/rust/issues/10184).
    Datum::from(a.unwrap_float32().round() as i64)
}

fn cast_float32_to_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    // TODO(benesch): is this cast valid?
    Datum::from(f64::from(a.unwrap_float32()))
}

fn cast_float32_to_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let f = a.unwrap_float32();
    let scale = b.unwrap_int32();

    if f > 10_f32.powi(MAX_DECIMAL_PRECISION as i32 - scale) {
        // When we can return error detail:
        // format!("A field with precision {}, \
        //         scale {} must round to an absolute value less than 10^{}.",
        //         MAX_DECIMAL_PRECISION, scale, MAX_DECIMAL_PRECISION - scale)
        return Err(EvalError::NumericFieldOverflow);
    }

    Ok(Datum::from((f * 10_f32.powi(scale)) as i128))
}

fn cast_float32_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_float32(&mut buf, a.unwrap_float32());
    Datum::String(temp_storage.push_string(buf))
}

fn cast_float64_to_int32<'a>(a: Datum<'a>) -> Datum<'a> {
    let f = a.unwrap_float64();
    if f > (i32::max_value() as f64) || f < (i32::min_value() as f64) {
        Datum::Null
    } else {
        Datum::from(f.round() as i32)
    }
}

fn cast_float64_to_int64<'a>(a: Datum<'a>) -> Datum<'a> {
    // TODO(benesch): this is undefined behavior if the f32 doesn't fit in an
    // i64 (https://github.com/rust-lang/rust/issues/10184).
    Datum::from(a.unwrap_float64().round() as i64)
}

fn cast_float64_to_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let f = a.unwrap_float64();
    let scale = b.unwrap_int32();

    if f > 10_f64.powi(MAX_DECIMAL_PRECISION as i32 - scale) {
        // When we can return error detail:
        // format!("A field with precision {}, \
        //         scale {} must round to an absolute value less than 10^{}.",
        //         MAX_DECIMAL_PRECISION, scale, MAX_DECIMAL_PRECISION - scale)
        return Err(EvalError::NumericFieldOverflow);
    }

    Ok(Datum::from((f * 10_f64.powi(scale)) as i128))
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

fn cast_significand_to_float32<'a>(a: Datum<'a>) -> Datum<'a> {
    // The second half of this function is defined in plan_cast_internal
    Datum::from(a.unwrap_decimal().as_i128() as f32)
}

fn cast_significand_to_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    // The second half of this function is defined in plan_cast_internal
    Datum::from(a.unwrap_decimal().as_i128() as f64)
}

fn cast_decimal_to_string<'a>(a: Datum<'a>, scale: u8, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_decimal(&mut buf, &a.unwrap_decimal().with_scale(scale));
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

fn cast_string_to_date<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    strconv::parse_date(a.unwrap_str())
        .map(Datum::Date)
        .err_into()
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
fn cast_string_to_jsonb<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    match strconv::parse_jsonb(a.unwrap_str()) {
        Err(_) => Datum::Null,
        Ok(jsonb) => temp_storage.push_row(jsonb.into_row()).unpack_first(),
    }
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

fn cast_jsonb_to_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Float64(_) => a,
        Datum::String(s) => match s {
            "NaN" => std::f64::NAN.into(),
            "Infinity" => std::f64::INFINITY.into(),
            "-Infinity" => std::f64::NEG_INFINITY.into(),
            _ => Datum::Null,
        },
        _ => Datum::Null,
    }
}

fn cast_jsonb_to_bool<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::True | Datum::False => a,
        _ => Datum::Null,
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
    cast_expr: &'a ScalarExpr,
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

fn round_float32<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float32().round())
}

fn round_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float64().round())
}

fn round_decimal_unary<'a>(a: Datum<'a>, a_scale: u8) -> Datum<'a> {
    round_decimal_binary(a, Datum::Int64(0), a_scale)
}

fn round_decimal_binary<'a>(a: Datum<'a>, b: Datum<'a>, a_scale: u8) -> Datum<'a> {
    let round_to = b.unwrap_int64();
    let decimal = a.unwrap_decimal().with_scale(a_scale);
    Datum::from(decimal.round(round_to).significand())
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

fn bit_length<'a, B>(bytes: B) -> Result<Datum<'a>, EvalError>
where
    B: AsRef<[u8]>,
{
    match i32::try_from(bytes.as_ref().len() * 8) {
        Ok(l) => Ok(Datum::from(l)),
        Err(_) => Err(EvalError::IntegerOutOfRange),
    }
}

fn byte_length<'a, B>(bytes: B) -> Result<Datum<'a>, EvalError>
where
    B: AsRef<[u8]>,
{
    match i32::try_from(bytes.as_ref().len()) {
        Ok(l) => Ok(Datum::from(l)),
        Err(_) => Err(EvalError::IntegerOutOfRange),
    }
}

fn char_length<'a>(a: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    match i32::try_from(a.unwrap_str().chars().count()) {
        Ok(l) => Ok(Datum::from(l)),
        Err(_) => Err(EvalError::IntegerOutOfRange),
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
        Err(_) => Err(EvalError::IntegerOutOfRange),
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

fn sub_timestamp<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_timestamp() - b.unwrap_timestamp())
}

fn sub_timestamptz<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_timestamptz() - b.unwrap_timestamptz())
}

fn sub_date<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_date() - b.unwrap_date())
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
    let b = b.unwrap_float32();
    if b == 0.0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_float32() / b))
    }
}

fn div_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_float64();
    if b == 0.0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_float64() / b))
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

pub fn neg_interval<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(-a.unwrap_interval())
}

fn sqrt_float32<'a>(a: Datum<'a>) -> Result<Datum, EvalError> {
    let x = a.unwrap_float32();
    if x < 0.0 {
        return Err(EvalError::NegSqrt);
    }
    Ok(Datum::from(x.sqrt()))
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
    cast_float64_to_decimal(Datum::from(d_scaled.sqrt()), Datum::from(i32::from(scale)))
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
        Datum::Dict(_) => Datum::Null,
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
        Datum::Dict(dict) => match dict.iter().find(|(k2, _v)| k == *k2) {
            Some((_k, v)) if stringify => jsonb_stringify(v, temp_storage),
            Some((_k, v)) => v,
            None => Datum::Null,
        },
        _ => Datum::Null,
    }
}

fn jsonb_contains_string<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let k = b.unwrap_str();
    // https://www.postgresql.org/docs/current/datatype-json.html#JSON-CONTAINMENT
    match a {
        Datum::List(list) => list.iter().any(|k2| b == k2).into(),
        Datum::Dict(dict) => dict.iter().any(|(k2, _v)| k == k2).into(),
        Datum::String(string) => (string == k).into(),
        _ => false.into(),
    }
}

// TODO(jamii) nested loops are possibly not the fastest way to do this
fn jsonb_contains_jsonb<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    // https://www.postgresql.org/docs/current/datatype-json.html#JSON-CONTAINMENT
    fn contains(a: Datum, b: Datum, at_top_level: bool) -> bool {
        match (a, b) {
            (Datum::JsonNull, Datum::JsonNull) => true,
            (Datum::False, Datum::False) => true,
            (Datum::True, Datum::True) => true,
            (Datum::Float64(a), Datum::Float64(b)) => (a == b),
            (Datum::String(a), Datum::String(b)) => (a == b),
            (Datum::List(a), Datum::List(b)) => b
                .iter()
                .all(|b_elem| a.iter().any(|a_elem| contains(a_elem, b_elem, false))),
            (Datum::Dict(a), Datum::Dict(b)) => b.iter().all(|(b_key, b_val)| {
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
        (Datum::Dict(dict_a), Datum::Dict(dict_b)) => {
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
        Datum::Dict(dict) => {
            let k = b.unwrap_str();
            let pairs = dict.iter().filter(|(k2, _v)| k != *k2);
            temp_storage.make_datum(|packer| packer.push_dict(pairs))
        }
        _ => Datum::Null,
    }
}

fn match_like_pattern<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let haystack = a.unwrap_str();
    let needle = like_pattern::build_regex(b.unwrap_str())?;
    Ok(Datum::from(needle.is_match(haystack)))
}

fn match_cached_regex<'a>(a: Datum<'a>, needle: &regex::Regex) -> Datum<'a> {
    let haystack = a.unwrap_str();
    Datum::from(needle.is_match(haystack))
}

fn match_regex<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    case_insensitive: bool,
) -> Result<Datum<'a>, EvalError> {
    let haystack = a.unwrap_str();
    let needle = build_regex(b, case_insensitive)?;
    Ok(Datum::from(needle.is_match(haystack)))
}

pub fn build_regex(d: Datum, case_insensitive: bool) -> Result<regex::Regex, EvalError> {
    regex::RegexBuilder::new(d.unwrap_str())
        .case_insensitive(case_insensitive)
        .build()
        .map_err(|e| EvalError::InvalidRegex(e.to_string()))
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
            NaiveDate::from_ymd(self.year() - (self.year() % 10), 1, 1),
            NaiveTime::from_hms(0, 0, 0),
        )
    }
    fn truncate_century(&self) -> Self {
        // Expects the first year of the century, meaning 2001 instead of 2000.
        Self::new(
            NaiveDate::from_ymd(self.year() - (self.year() % 100) + 1, 1, 1),
            NaiveTime::from_hms(0, 0, 0),
        )
    }
    fn truncate_millennium(&self) -> Self {
        // Expects the first year of the millennium, meaning 2001 instead of 2000.
        Self::new(
            NaiveDate::from_ymd(self.year() - (self.year() % 1_000) + 1, 1, 1),
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
        DateTimeUnits::Millennium
        | DateTimeUnits::Century
        | DateTimeUnits::Decade
        | DateTimeUnits::Quarter
        | DateTimeUnits::Week
        | DateTimeUnits::Month
        | DateTimeUnits::Milliseconds
        | DateTimeUnits::Microseconds
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
        DateTimeUnits::Millennium
        | DateTimeUnits::Century
        | DateTimeUnits::Decade
        | DateTimeUnits::Milliseconds
        | DateTimeUnits::Microseconds
        | DateTimeUnits::Timezone
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
        Datum::Dict(_) => Datum::String("object"),
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
    fn strip_nulls(a: Datum, packer: &mut RowPacker) {
        match a {
            Datum::Dict(dict) => packer.push_dict_with(|packer| {
                for (k, v) in dict.iter() {
                    match v {
                        Datum::JsonNull => (),
                        _ => {
                            packer.push(Datum::String(k));
                            strip_nulls(v, packer);
                        }
                    }
                }
            }),
            Datum::List(list) => packer.push_list_with(|packer| {
                for elem in list.iter() {
                    strip_nulls(elem, packer);
                }
            }),
            _ => packer.push(a),
        }
    }
    temp_storage.make_datum(|packer| strip_nulls(a, packer))
}

fn jsonb_pretty<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_jsonb_pretty(&mut buf, JsonbRef::from_datum(a));
    Datum::String(temp_storage.push_string(buf))
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
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
    MulInt32,
    MulInt64,
    MulFloat32,
    MulFloat64,
    MulDecimal,
    DivInt32,
    DivInt64,
    DivFloat32,
    DivFloat64,
    DivDecimal,
    ModInt32,
    ModInt64,
    ModFloat32,
    ModFloat64,
    ModDecimal,
    RoundDecimal(u8),
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    MatchLikePattern,
    MatchRegex { case_insensitive: bool },
    ToCharTimestamp,
    ToCharTimestampTz,
    DatePartInterval,
    DatePartTimestamp,
    DatePartTimestampTz,
    DateTruncTimestamp,
    DateTruncTimestampTz,
    CastFloat32ToDecimal,
    CastFloat64ToDecimal,
    TextConcat,
    JsonbGetInt64 { stringify: bool },
    JsonbGetString { stringify: bool },
    JsonbContainsString,
    JsonbConcat,
    JsonbContainsJsonb,
    JsonbDeleteInt64,
    JsonbDeleteString,
    ConvertFrom,
    Trim,
    TrimLeading,
    TrimTrailing,
    EncodedBytesCharLength,
    ListIndex,
    ListLengthMax { max_dim: usize },
    ArrayContains,
    ArrayIndex,
    ArrayLower,
}

impl BinaryFunc {
    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a_expr: &'a ScalarExpr,
        b_expr: &'a ScalarExpr,
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
            BinaryFunc::MulInt32 => eager!(mul_int32),
            BinaryFunc::MulInt64 => eager!(mul_int64),
            BinaryFunc::MulFloat32 => Ok(eager!(mul_float32)),
            BinaryFunc::MulFloat64 => Ok(eager!(mul_float64)),
            BinaryFunc::MulDecimal => Ok(eager!(mul_decimal)),
            BinaryFunc::DivInt32 => eager!(div_int32),
            BinaryFunc::DivInt64 => eager!(div_int64),
            BinaryFunc::DivFloat32 => eager!(div_float32),
            BinaryFunc::DivFloat64 => eager!(div_float64),
            BinaryFunc::DivDecimal => eager!(div_decimal),
            BinaryFunc::ModInt32 => eager!(mod_int32),
            BinaryFunc::ModInt64 => eager!(mod_int64),
            BinaryFunc::ModFloat32 => eager!(mod_float32),
            BinaryFunc::ModFloat64 => eager!(mod_float64),
            BinaryFunc::ModDecimal => eager!(mod_decimal),
            BinaryFunc::Eq => Ok(eager!(eq)),
            BinaryFunc::NotEq => Ok(eager!(not_eq)),
            BinaryFunc::Lt => Ok(eager!(lt)),
            BinaryFunc::Lte => Ok(eager!(lte)),
            BinaryFunc::Gt => Ok(eager!(gt)),
            BinaryFunc::Gte => Ok(eager!(gte)),
            BinaryFunc::MatchLikePattern => eager!(match_like_pattern),
            BinaryFunc::MatchRegex { case_insensitive } => eager!(match_regex, *case_insensitive),
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
            BinaryFunc::CastFloat32ToDecimal => eager!(cast_float32_to_decimal),
            BinaryFunc::CastFloat64ToDecimal => eager!(cast_float64_to_decimal),
            BinaryFunc::TextConcat => Ok(eager!(text_concat_binary, temp_storage)),
            BinaryFunc::JsonbGetInt64 { stringify } => {
                Ok(eager!(jsonb_get_int64, temp_storage, *stringify))
            }
            BinaryFunc::JsonbGetString { stringify } => {
                Ok(eager!(jsonb_get_string, temp_storage, *stringify))
            }
            BinaryFunc::JsonbContainsString => Ok(eager!(jsonb_contains_string)),
            BinaryFunc::JsonbConcat => Ok(eager!(jsonb_concat, temp_storage)),
            BinaryFunc::JsonbContainsJsonb => Ok(eager!(jsonb_contains_jsonb)),
            BinaryFunc::JsonbDeleteInt64 => Ok(eager!(jsonb_delete_int64, temp_storage)),
            BinaryFunc::JsonbDeleteString => Ok(eager!(jsonb_delete_string, temp_storage)),
            BinaryFunc::RoundDecimal(scale) => Ok(eager!(round_decimal_binary, *scale)),
            BinaryFunc::ConvertFrom => eager!(convert_from),
            BinaryFunc::Trim => Ok(eager!(trim)),
            BinaryFunc::TrimLeading => Ok(eager!(trim_leading)),
            BinaryFunc::TrimTrailing => Ok(eager!(trim_trailing)),
            BinaryFunc::EncodedBytesCharLength => eager!(encoded_bytes_char_length),
            BinaryFunc::ListIndex => Ok(eager!(list_index)),
            BinaryFunc::ListLengthMax { max_dim } => eager!(list_length_max, *max_dim),
            BinaryFunc::ArrayContains => Ok(eager!(array_contains)),
            BinaryFunc::ArrayIndex => Ok(eager!(array_index)),
            BinaryFunc::ArrayLower => Ok(eager!(array_lower)),
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
                | ModDecimal
        );
        match self {
            And | Or | Eq | NotEq | Lt | Lte | Gt | Gte | ArrayContains => {
                ScalarType::Bool.nullable(in_nullable)
            }

            MatchLikePattern | MatchRegex { .. } => {
                // The output can be null if the pattern is invalid.
                ScalarType::Bool.nullable(true)
            }

            ToCharTimestamp | ToCharTimestampTz | ConvertFrom | Trim | TrimLeading
            | TrimTrailing => ScalarType::String.nullable(in_nullable),

            AddInt32 | SubInt32 | MulInt32 | DivInt32 | ModInt32 | EncodedBytesCharLength => {
                ScalarType::Int32.nullable(in_nullable || is_div_mod)
            }

            AddInt64 | SubInt64 | MulInt64 | DivInt64 | ModInt64 => {
                ScalarType::Int64.nullable(in_nullable || is_div_mod)
            }

            AddFloat32 | SubFloat32 | MulFloat32 | DivFloat32 | ModFloat32 => {
                ScalarType::Float32.nullable(in_nullable || is_div_mod)
            }

            AddFloat64 | SubFloat64 | MulFloat64 | DivFloat64 | ModFloat64 => {
                ScalarType::Float64.nullable(in_nullable || is_div_mod)
            }

            AddInterval | SubInterval | SubTimestamp | SubTimestampTz | SubDate => {
                ScalarType::Interval.nullable(in_nullable)
            }

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

            CastFloat32ToDecimal | CastFloat64ToDecimal => match input2_type.scalar_type {
                ScalarType::Decimal(_, s) => {
                    ScalarType::Decimal(MAX_DECIMAL_PRECISION, s).nullable(true)
                }
                _ => unreachable!(),
            },

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

            DatePartInterval | DatePartTimestamp | DatePartTimestampTz => {
                ScalarType::Float64.nullable(true)
            }

            DateTruncTimestampTz => ScalarType::TimestampTz.nullable(true),

            SubTime => ScalarType::Interval.nullable(true),

            TextConcat => ScalarType::String.nullable(in_nullable),

            JsonbGetInt64 { stringify: true } | JsonbGetString { stringify: true } => {
                ScalarType::String.nullable(true)
            }

            JsonbGetInt64 { stringify: false }
            | JsonbGetString { stringify: false }
            | JsonbConcat
            | JsonbDeleteInt64
            | JsonbDeleteString => ScalarType::Jsonb.nullable(true),

            JsonbContainsString | JsonbContainsJsonb => ScalarType::Bool.nullable(in_nullable),

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

            ListLengthMax { .. } | ArrayLower => ScalarType::Int64.nullable(true),
        }
    }

    /// Whether the function output is NULL if any of its inputs are NULL.
    pub fn propagates_nulls(&self) -> bool {
        !matches!(self, BinaryFunc::And | BinaryFunc::Or)
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
                | AddDecimal
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
                | MulInt32
                | MulInt64
                | MulFloat32
                | MulFloat64
                | MulDecimal
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
            | AddDecimal
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
            | MulInt32
            | MulInt64
            | MulFloat32
            | MulFloat64
            | MulDecimal
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
            | JsonbContainsString
            | JsonbDeleteInt64
            | JsonbDeleteString
            | TextConcat
            | ListIndex
            | MatchRegex { .. }
            | ArrayContains
            | ArrayIndex
            | ArrayLower => true,
            MatchLikePattern
            | ToCharTimestamp
            | ToCharTimestampTz
            | DatePartInterval
            | DatePartTimestamp
            | DatePartTimestampTz
            | DateTruncTimestamp
            | DateTruncTimestampTz
            | CastFloat32ToDecimal
            | CastFloat64ToDecimal
            | RoundDecimal(_)
            | ConvertFrom
            | Trim
            | TrimLeading
            | TrimTrailing
            | EncodedBytesCharLength
            | ListLengthMax { .. } => false,
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
            BinaryFunc::DivInt32 => f.write_str("/"),
            BinaryFunc::DivInt64 => f.write_str("/"),
            BinaryFunc::DivFloat32 => f.write_str("/"),
            BinaryFunc::DivFloat64 => f.write_str("/"),
            BinaryFunc::DivDecimal => f.write_str("/"),
            BinaryFunc::ModInt32 => f.write_str("%"),
            BinaryFunc::ModInt64 => f.write_str("%"),
            BinaryFunc::ModFloat32 => f.write_str("%"),
            BinaryFunc::ModFloat64 => f.write_str("%"),
            BinaryFunc::ModDecimal => f.write_str("%"),
            BinaryFunc::Eq => f.write_str("="),
            BinaryFunc::NotEq => f.write_str("!="),
            BinaryFunc::Lt => f.write_str("<"),
            BinaryFunc::Lte => f.write_str("<="),
            BinaryFunc::Gt => f.write_str(">"),
            BinaryFunc::Gte => f.write_str(">="),
            BinaryFunc::MatchLikePattern => f.write_str("like"),
            BinaryFunc::MatchRegex {
                case_insensitive: false,
            } => f.write_str("~"),
            BinaryFunc::MatchRegex {
                case_insensitive: true,
            } => f.write_str("~*"),
            BinaryFunc::ToCharTimestamp => f.write_str("tocharts"),
            BinaryFunc::ToCharTimestampTz => f.write_str("tochartstz"),
            BinaryFunc::DatePartInterval => f.write_str("date_partiv"),
            BinaryFunc::DatePartTimestamp => f.write_str("date_partts"),
            BinaryFunc::DatePartTimestampTz => f.write_str("date_parttstz"),
            BinaryFunc::DateTruncTimestamp => f.write_str("date_truncts"),
            BinaryFunc::DateTruncTimestampTz => f.write_str("date_trunctstz"),
            BinaryFunc::CastFloat32ToDecimal => f.write_str("f32todec"),
            BinaryFunc::CastFloat64ToDecimal => f.write_str("f64todec"),
            BinaryFunc::TextConcat => f.write_str("||"),
            BinaryFunc::JsonbGetInt64 { .. } => f.write_str("->"),
            BinaryFunc::JsonbGetString { .. } => f.write_str("->"),
            BinaryFunc::JsonbContainsString => f.write_str("?"),
            BinaryFunc::JsonbConcat => f.write_str("||"),
            BinaryFunc::JsonbContainsJsonb => f.write_str("@>"),
            BinaryFunc::JsonbDeleteInt64 => f.write_str("-"),
            BinaryFunc::JsonbDeleteString => f.write_str("-"),
            BinaryFunc::RoundDecimal(_) => f.write_str("round"),
            BinaryFunc::ConvertFrom => f.write_str("convert_from"),
            BinaryFunc::Trim => f.write_str("btrim"),
            BinaryFunc::TrimLeading => f.write_str("ltrim"),
            BinaryFunc::TrimTrailing => f.write_str("rtrim"),
            BinaryFunc::EncodedBytesCharLength => f.write_str("length"),
            BinaryFunc::ListIndex => f.write_str("list_index"),
            BinaryFunc::ListLengthMax { .. } => f.write_str("list_length_max"),
            BinaryFunc::ArrayContains => f.write_str("array_contains"),
            BinaryFunc::ArrayIndex => f.write_str("array_index"),
            BinaryFunc::ArrayLower => f.write_str("array_lower"),
        }
    }
}

fn is_null<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a == Datum::Null)
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum UnaryFunc {
    Not,
    IsNull,
    NegInt32,
    NegInt64,
    NegFloat32,
    NegFloat64,
    NegDecimal,
    NegInterval,
    SqrtFloat32,
    SqrtFloat64,
    SqrtDec(u8),
    AbsInt32,
    AbsInt64,
    AbsFloat32,
    AbsFloat64,
    AbsDecimal,
    CastBoolToStringExplicit,
    CastBoolToStringImplicit,
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
    CastInt64ToBool,
    CastInt64ToDecimal,
    CastInt64ToFloat32,
    CastInt64ToFloat64,
    CastInt64ToString,
    CastFloat32ToInt64,
    CastFloat32ToFloat64,
    CastFloat32ToString,
    CastFloat64ToInt32,
    CastFloat64ToInt64,
    CastFloat64ToString,
    CastDecimalToInt32(u8),
    CastDecimalToInt64(u8),
    CastDecimalToString(u8),
    CastSignificandToFloat32,
    CastSignificandToFloat64,
    CastStringToBool,
    CastStringToBytes,
    CastStringToInt32,
    CastStringToInt64,
    CastStringToFloat32,
    CastStringToFloat64,
    CastStringToDate,
    CastStringToTime,
    CastStringToTimestamp,
    CastStringToTimestampTz,
    CastStringToInterval,
    CastStringToDecimal(u8),
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
    CastJsonbToFloat64,
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
        cast_expr: Box<ScalarExpr>,
    },
    CeilFloat32,
    CeilFloat64,
    CeilDecimal(u8),
    FloorFloat32,
    FloorFloat64,
    FloorDecimal(u8),
    Ascii,
    BitLengthBytes,
    BitLengthString,
    ByteLengthBytes,
    ByteLengthString,
    CharLength,
    MatchRegex(Regex),
    DatePartInterval(DateTimeUnits),
    DatePartTimestamp(DateTimeUnits),
    DatePartTimestampTz(DateTimeUnits),
    DateTruncTimestamp(DateTimeUnits),
    DateTruncTimestampTz(DateTimeUnits),
    ToTimestamp,
    JsonbArrayLength,
    JsonbTypeof,
    JsonbStripNulls,
    JsonbPretty,
    RoundFloat32,
    RoundFloat64,
    RoundDecimal(u8),
    TrimWhitespace,
    TrimLeadingWhitespace,
    TrimTrailingWhitespace,
    RecordGet(usize),
    ListLength,
}

impl UnaryFunc {
    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a ScalarExpr,
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
            UnaryFunc::NegInterval => Ok(neg_interval(a)),
            UnaryFunc::AbsInt32 => Ok(abs_int32(a)),
            UnaryFunc::AbsInt64 => Ok(abs_int64(a)),
            UnaryFunc::AbsFloat32 => Ok(abs_float32(a)),
            UnaryFunc::AbsFloat64 => Ok(abs_float64(a)),
            UnaryFunc::AbsDecimal => Ok(abs_decimal(a)),
            UnaryFunc::CastBoolToStringExplicit => Ok(cast_bool_to_string_explicit(a)),
            UnaryFunc::CastBoolToStringImplicit => Ok(cast_bool_to_string_implicit(a)),
            UnaryFunc::CastBoolToInt32 => Ok(cast_bool_to_int32(a)),
            UnaryFunc::CastInt32ToBool => Ok(cast_int32_to_bool(a)),
            UnaryFunc::CastInt32ToFloat32 => Ok(cast_int32_to_float32(a)),
            UnaryFunc::CastInt32ToFloat64 => Ok(cast_int32_to_float64(a)),
            UnaryFunc::CastInt32ToInt64 => Ok(cast_int32_to_int64(a)),
            UnaryFunc::CastInt32ToOid => Ok(a),
            UnaryFunc::CastInt32ToDecimal => Ok(cast_int32_to_decimal(a)),
            UnaryFunc::CastInt32ToString => Ok(cast_int32_to_string(a, temp_storage)),
            UnaryFunc::CastOidToInt32 => Ok(a),
            UnaryFunc::CastInt64ToInt32 => cast_int64_to_int32(a),
            UnaryFunc::CastInt64ToBool => Ok(cast_int64_to_bool(a)),
            UnaryFunc::CastInt64ToDecimal => Ok(cast_int64_to_decimal(a)),
            UnaryFunc::CastInt64ToFloat32 => Ok(cast_int64_to_float32(a)),
            UnaryFunc::CastInt64ToFloat64 => Ok(cast_int64_to_float64(a)),
            UnaryFunc::CastInt64ToString => Ok(cast_int64_to_string(a, temp_storage)),
            UnaryFunc::CastFloat32ToInt64 => Ok(cast_float32_to_int64(a)),
            UnaryFunc::CastFloat32ToFloat64 => Ok(cast_float32_to_float64(a)),
            UnaryFunc::CastFloat32ToString => Ok(cast_float32_to_string(a, temp_storage)),
            UnaryFunc::CastFloat64ToInt32 => Ok(cast_float64_to_int32(a)),
            UnaryFunc::CastFloat64ToInt64 => Ok(cast_float64_to_int64(a)),
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
            UnaryFunc::CastStringToDate => cast_string_to_date(a),
            UnaryFunc::CastStringToTime => cast_string_to_time(a),
            UnaryFunc::CastStringToTimestamp => cast_string_to_timestamp(a),
            UnaryFunc::CastStringToTimestampTz => cast_string_to_timestamptz(a),
            UnaryFunc::CastStringToInterval => cast_string_to_interval(a),
            UnaryFunc::CastStringToUuid => cast_string_to_uuid(a),
            UnaryFunc::CastDateToTimestamp => Ok(cast_date_to_timestamp(a)),
            UnaryFunc::CastDateToTimestampTz => Ok(cast_date_to_timestamptz(a)),
            UnaryFunc::CastDateToString => Ok(cast_date_to_string(a, temp_storage)),
            UnaryFunc::CastDecimalToString(scale) => {
                Ok(cast_decimal_to_string(a, *scale, temp_storage))
            }
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
            UnaryFunc::CastStringToJsonb => Ok(cast_string_to_jsonb(a, temp_storage)),
            UnaryFunc::CastJsonbOrNullToJsonb => Ok(cast_jsonb_or_null_to_jsonb(a)),
            UnaryFunc::CastJsonbToString => Ok(cast_jsonb_to_string(a, temp_storage)),
            UnaryFunc::CastJsonbToFloat64 => Ok(cast_jsonb_to_float64(a)),
            UnaryFunc::CastJsonbToBool => Ok(cast_jsonb_to_bool(a)),
            UnaryFunc::CastUuidToString => Ok(cast_uuid_to_string(a, temp_storage)),
            UnaryFunc::CastRecordToString { ty }
            | UnaryFunc::CastArrayToString { ty }
            | UnaryFunc::CastListToString { ty } => {
                Ok(cast_collection_to_string(a, ty, temp_storage))
            }
            UnaryFunc::CastList1ToList2 { cast_expr, .. } => {
                cast_list1_to_list2(a, &*cast_expr, temp_storage)
            }
            UnaryFunc::CeilFloat32 => Ok(ceil_float32(a)),
            UnaryFunc::CeilFloat64 => Ok(ceil_float64(a)),
            UnaryFunc::CeilDecimal(scale) => Ok(ceil_decimal(a, *scale)),
            UnaryFunc::FloorFloat32 => Ok(floor_float32(a)),
            UnaryFunc::FloorFloat64 => Ok(floor_float64(a)),
            UnaryFunc::FloorDecimal(scale) => Ok(floor_decimal(a, *scale)),
            UnaryFunc::SqrtFloat32 => sqrt_float32(a),
            UnaryFunc::SqrtFloat64 => sqrt_float64(a),
            UnaryFunc::SqrtDec(scale) => sqrt_dec(a, *scale),
            UnaryFunc::Ascii => Ok(ascii(a)),
            UnaryFunc::BitLengthString => bit_length(a.unwrap_str()),
            UnaryFunc::BitLengthBytes => bit_length(a.unwrap_bytes()),
            UnaryFunc::ByteLengthString => byte_length(a.unwrap_str()),
            UnaryFunc::ByteLengthBytes => byte_length(a.unwrap_bytes()),
            UnaryFunc::CharLength => char_length(a),
            UnaryFunc::MatchRegex(regex) => Ok(match_cached_regex(a, &regex)),
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
            UnaryFunc::ToTimestamp => Ok(to_timestamp(a)),
            UnaryFunc::JsonbArrayLength => Ok(jsonb_array_length(a)),
            UnaryFunc::JsonbTypeof => Ok(jsonb_typeof(a)),
            UnaryFunc::JsonbStripNulls => Ok(jsonb_strip_nulls(a, temp_storage)),
            UnaryFunc::JsonbPretty => Ok(jsonb_pretty(a, temp_storage)),
            UnaryFunc::RoundFloat32 => Ok(round_float32(a)),
            UnaryFunc::RoundFloat64 => Ok(round_float64(a)),
            UnaryFunc::RoundDecimal(scale) => Ok(round_decimal_unary(a, *scale)),
            UnaryFunc::TrimWhitespace => Ok(trim_whitespace(a)),
            UnaryFunc::TrimLeadingWhitespace => Ok(trim_leading_whitespace(a)),
            UnaryFunc::TrimTrailingWhitespace => Ok(trim_trailing_whitespace(a)),
            UnaryFunc::RecordGet(i) => Ok(record_get(a, *i)),
            UnaryFunc::ListLength => Ok(list_length(a)),
        }
    }

    pub fn output_type(&self, input_type: ColumnType) -> ColumnType {
        use UnaryFunc::*;
        let in_nullable = input_type.nullable;
        match self {
            IsNull | CastInt32ToBool | CastInt64ToBool => ScalarType::Bool.nullable(false),

            Ascii | CharLength | BitLengthBytes | BitLengthString | ByteLengthBytes
            | ByteLengthString => ScalarType::Int32.nullable(in_nullable),

            MatchRegex(_) => ScalarType::Bool.nullable(in_nullable),

            CastStringToBool => ScalarType::Bool.nullable(true),
            CastStringToBytes => ScalarType::Bytes.nullable(true),
            CastStringToInt32 => ScalarType::Int32.nullable(true),
            CastStringToInt64 => ScalarType::Int64.nullable(true),
            CastStringToFloat32 => ScalarType::Float32.nullable(true),
            CastStringToFloat64 => ScalarType::Float64.nullable(true),
            CastStringToDecimal(scale) => {
                ScalarType::Decimal(MAX_DECIMAL_PRECISION, *scale).nullable(true)
            }
            CastStringToDate => ScalarType::Date.nullable(true),
            CastStringToTime => ScalarType::Time.nullable(true),
            CastStringToTimestamp => ScalarType::Timestamp.nullable(true),
            CastStringToTimestampTz => ScalarType::TimestampTz.nullable(true),
            CastStringToInterval | CastTimeToInterval => ScalarType::Interval.nullable(true),
            CastStringToUuid => ScalarType::Uuid.nullable(true),

            CastBoolToInt32 => ScalarType::Int32.nullable(in_nullable),

            CastBoolToStringExplicit
            | CastBoolToStringImplicit
            | CastInt32ToString
            | CastInt64ToString
            | CastFloat32ToString
            | CastFloat64ToString
            | CastDecimalToString(_)
            | CastDateToString
            | CastTimeToString
            | CastTimestampToString
            | CastTimestampTzToString
            | CastIntervalToString
            | CastBytesToString
            | CastRecordToString { .. }
            | CastArrayToString { .. }
            | CastListToString { .. }
            | TrimWhitespace
            | TrimLeadingWhitespace
            | TrimTrailingWhitespace => ScalarType::String.nullable(in_nullable),

            CastInt32ToFloat32 | CastInt64ToFloat32 | CastSignificandToFloat32 => {
                ScalarType::Float32.nullable(in_nullable)
            }

            CastInt32ToFloat64
            | CastInt64ToFloat64
            | CastFloat32ToFloat64
            | CastSignificandToFloat64 => ScalarType::Float64.nullable(in_nullable),

            CastInt64ToInt32 | CastDecimalToInt32(_) => ScalarType::Int32.nullable(in_nullable),

            CastFloat64ToInt32 => ScalarType::Int32.nullable(true),

            CastInt32ToInt64 | CastDecimalToInt64(_) | CastFloat32ToInt64 | CastFloat64ToInt64 => {
                ScalarType::Int64.nullable(in_nullable)
            }

            CastInt32ToDecimal => ScalarType::Decimal(0, 0).nullable(in_nullable),
            CastInt64ToDecimal => ScalarType::Decimal(0, 0).nullable(in_nullable),

            CastInt32ToOid => ScalarType::Oid.nullable(in_nullable),
            CastOidToInt32 => ScalarType::Oid.nullable(in_nullable),

            CastTimestampToDate | CastTimestampTzToDate => ScalarType::Date.nullable(in_nullable),

            CastIntervalToTime => ScalarType::Time.nullable(in_nullable),

            CastDateToTimestamp | CastTimestampTzToTimestamp => {
                ScalarType::Timestamp.nullable(in_nullable)
            }

            CastDateToTimestampTz | CastTimestampToTimestampTz => {
                ScalarType::TimestampTz.nullable(in_nullable)
            }

            // can return null for invalid json
            CastStringToJsonb => ScalarType::Jsonb.nullable(true),

            // converts null to jsonnull
            CastJsonbOrNullToJsonb => ScalarType::Jsonb.nullable(false),

            // can return null for other jsonb types
            CastJsonbToString => ScalarType::String.nullable(true),
            CastJsonbToFloat64 => ScalarType::Float64.nullable(true),
            CastJsonbToBool => ScalarType::Bool.nullable(true),

            CastUuidToString => ScalarType::String.nullable(true),

            CastList1ToList2 { return_ty, .. } => (return_ty.clone()).nullable(false),

            CeilFloat32 | FloorFloat32 | RoundFloat32 => ScalarType::Float32.nullable(in_nullable),
            CeilFloat64 | FloorFloat64 | RoundFloat64 => ScalarType::Float64.nullable(in_nullable),
            CeilDecimal(scale) | FloorDecimal(scale) | RoundDecimal(scale) | SqrtDec(scale) => {
                match input_type.scalar_type {
                    ScalarType::Decimal(_, s) => assert_eq!(*scale, s),
                    _ => unreachable!(),
                }
                input_type.scalar_type.nullable(in_nullable)
            }

            SqrtFloat32 => ScalarType::Float32.nullable(true),
            SqrtFloat64 => ScalarType::Float64.nullable(true),

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
                ScalarType::Record { mut fields } => fields.swap_remove(*i).1.nullable(true),
                _ => unreachable!("RecordGet specified nonexistent field"),
            },

            ListLength => ScalarType::Int64.nullable(true),
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
                | UnaryFunc::CastBoolToStringExplicit
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
            UnaryFunc::NegInterval => f.write_str("-"),
            UnaryFunc::AbsInt32 => f.write_str("abs"),
            UnaryFunc::AbsInt64 => f.write_str("abs"),
            UnaryFunc::AbsDecimal => f.write_str("abs"),
            UnaryFunc::AbsFloat32 => f.write_str("abs"),
            UnaryFunc::AbsFloat64 => f.write_str("abs"),
            UnaryFunc::CastBoolToStringExplicit => f.write_str("booltostrex"),
            UnaryFunc::CastBoolToStringImplicit => f.write_str("booltostrim"),
            UnaryFunc::CastBoolToInt32 => f.write_str("booltoi32"),
            UnaryFunc::CastInt32ToBool => f.write_str("i32tobool"),
            UnaryFunc::CastInt32ToFloat32 => f.write_str("i32tof32"),
            UnaryFunc::CastInt32ToFloat64 => f.write_str("i32tof64"),
            UnaryFunc::CastInt32ToInt64 => f.write_str("i32toi64"),
            UnaryFunc::CastInt32ToOid => f.write_str("i32tooid"),
            UnaryFunc::CastInt32ToString => f.write_str("i32tostr"),
            UnaryFunc::CastInt32ToDecimal => f.write_str("i32todec"),
            UnaryFunc::CastOidToInt32 => f.write_str("oidtoi32"),
            UnaryFunc::CastInt64ToInt32 => f.write_str("i64toi32"),
            UnaryFunc::CastInt64ToBool => f.write_str("i64tobool"),
            UnaryFunc::CastInt64ToDecimal => f.write_str("i64todec"),
            UnaryFunc::CastInt64ToFloat32 => f.write_str("i64tof32"),
            UnaryFunc::CastInt64ToFloat64 => f.write_str("i64tof64"),
            UnaryFunc::CastInt64ToString => f.write_str("i64tostr"),
            UnaryFunc::CastFloat32ToInt64 => f.write_str("f32toi64"),
            UnaryFunc::CastFloat32ToFloat64 => f.write_str("f32tof64"),
            UnaryFunc::CastFloat32ToString => f.write_str("f32tostr"),
            UnaryFunc::CastFloat64ToInt32 => f.write_str("f64toi32"),
            UnaryFunc::CastFloat64ToInt64 => f.write_str("f64toi64"),
            UnaryFunc::CastFloat64ToString => f.write_str("f64tostr"),
            UnaryFunc::CastDecimalToInt32(_) => f.write_str("dectoi32"),
            UnaryFunc::CastDecimalToInt64(_) => f.write_str("dectoi64"),
            UnaryFunc::CastDecimalToString(_) => f.write_str("dectostr"),
            UnaryFunc::CastSignificandToFloat32 => f.write_str("dectof32"),
            UnaryFunc::CastSignificandToFloat64 => f.write_str("dectof64"),
            UnaryFunc::CastStringToBool => f.write_str("strtobool"),
            UnaryFunc::CastStringToBytes => f.write_str("strtobytes"),
            UnaryFunc::CastStringToInt32 => f.write_str("strtoi32"),
            UnaryFunc::CastStringToInt64 => f.write_str("strtoi64"),
            UnaryFunc::CastStringToFloat32 => f.write_str("strtof32"),
            UnaryFunc::CastStringToFloat64 => f.write_str("strtof64"),
            UnaryFunc::CastStringToDecimal(_) => f.write_str("strtodec"),
            UnaryFunc::CastStringToDate => f.write_str("strtodate"),
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
            UnaryFunc::CastJsonbToFloat64 => f.write_str("jsonbtof64"),
            UnaryFunc::CastJsonbToBool => f.write_str("jsonbtobool"),
            UnaryFunc::CastUuidToString => f.write_str("uuidtostr"),
            UnaryFunc::CastRecordToString { .. } => f.write_str("recordtostr"),
            UnaryFunc::CastArrayToString { .. } => f.write_str("arraytostr"),
            UnaryFunc::CastListToString { .. } => f.write_str("listtostr"),
            UnaryFunc::CastList1ToList2 { .. } => f.write_str("list1tolist2"),
            UnaryFunc::CeilFloat32 => f.write_str("ceilf32"),
            UnaryFunc::CeilFloat64 => f.write_str("ceilf64"),
            UnaryFunc::CeilDecimal(_) => f.write_str("ceildec"),
            UnaryFunc::FloorFloat32 => f.write_str("floorf32"),
            UnaryFunc::FloorFloat64 => f.write_str("floorf64"),
            UnaryFunc::FloorDecimal(_) => f.write_str("floordec"),
            UnaryFunc::SqrtFloat32 => f.write_str("sqrtf32"),
            UnaryFunc::SqrtFloat64 => f.write_str("sqrtf64"),
            UnaryFunc::SqrtDec(_) => f.write_str("sqrtdec"),
            UnaryFunc::Ascii => f.write_str("ascii"),
            UnaryFunc::CharLength => f.write_str("char_length"),
            UnaryFunc::BitLengthBytes => f.write_str("bit_length"),
            UnaryFunc::BitLengthString => f.write_str("bit_length"),
            UnaryFunc::ByteLengthBytes => f.write_str("byte_length"),
            UnaryFunc::ByteLengthString => f.write_str("byte_length"),
            UnaryFunc::MatchRegex(regex) => write!(f, "\"{}\" ~", regex.as_str()),
            UnaryFunc::DatePartInterval(units) => write!(f, "date_part_{}_iv", units),
            UnaryFunc::DatePartTimestamp(units) => write!(f, "date_part_{}_ts", units),
            UnaryFunc::DatePartTimestampTz(units) => write!(f, "date_part_{}_tstz", units),
            UnaryFunc::DateTruncTimestamp(units) => write!(f, "date_trunc_{}_ts", units),
            UnaryFunc::DateTruncTimestampTz(units) => write!(f, "date_trunc_{}_tstz", units),
            UnaryFunc::ToTimestamp => f.write_str("tots"),
            UnaryFunc::JsonbArrayLength => f.write_str("jsonb_array_length"),
            UnaryFunc::JsonbTypeof => f.write_str("jsonb_typeof"),
            UnaryFunc::JsonbStripNulls => f.write_str("jsonb_strip_nulls"),
            UnaryFunc::JsonbPretty => f.write_str("jsonb_pretty"),
            UnaryFunc::RoundFloat32 => f.write_str("roundf32"),
            UnaryFunc::RoundFloat64 => f.write_str("roundf64"),
            UnaryFunc::RoundDecimal(_) => f.write_str("roundunary"),
            UnaryFunc::TrimWhitespace => f.write_str("btrim"),
            UnaryFunc::TrimLeadingWhitespace => f.write_str("ltrim"),
            UnaryFunc::TrimTrailingWhitespace => f.write_str("rtrim"),
            UnaryFunc::RecordGet(_) => f.write_str("record_get"),
            UnaryFunc::ListLength => f.write_str("list_length"),
        }
    }
}

fn coalesce<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
    exprs: &'a [ScalarExpr],
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

fn substr<'a>(datums: &[Datum<'a>]) -> Datum<'a> {
    let string: &'a str = datums[0].unwrap_str();
    let mut chars = string.chars();

    let start_in_chars = datums[1].unwrap_int64() - 1;
    let mut start_in_bytes = 0;
    for _ in 0..cmp::max(start_in_chars, 0) {
        start_in_bytes += chars.next().map(|char| char.len_utf8()).unwrap_or(0);
    }

    if datums.len() == 3 {
        let mut length_in_chars = datums[2].unwrap_int64();
        if length_in_chars < 0 {
            return Datum::Null;
        }
        if start_in_chars < 0 {
            length_in_chars += start_in_chars;
        }
        let mut length_in_bytes = 0;
        for _ in 0..cmp::max(length_in_chars, 0) {
            length_in_bytes += chars.next().map(|char| char.len_utf8()).unwrap_or(0);
        }
        Datum::String(&string[start_in_bytes..start_in_bytes + length_in_bytes])
    } else {
        Datum::String(&string[start_in_bytes..])
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
    if delimiter == "" {
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
/// The lower bound of the additional dimension is always one. The length of
/// the new dimension is equal to `datums.len()`.
fn array_create_multidim<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
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

/// Constructs a new 1D array out of an arbitrary number of scalars.
///
/// The lower bound of the array is always one. The length of the array is equal
/// to `datums.len()`.
fn array_create_scalar<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let dims = &[ArrayDimension {
        lower_bound: 1,
        length: datums.len(),
    }];
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
        Date => strconv::format_date(buf, d.unwrap_date()),
        Time => strconv::format_time(buf, d.unwrap_time()),
        Timestamp => strconv::format_timestamp(buf, d.unwrap_timestamp()),
        TimestampTz => strconv::format_timestamptz(buf, d.unwrap_timestamptz()),
        Interval => strconv::format_interval(buf, d.unwrap_interval()),
        Bytes => strconv::format_bytes(buf, d.unwrap_bytes()),
        String => strconv::format_string(buf, d.unwrap_str()),
        Jsonb => strconv::format_jsonb(buf, JsonbRef::from_datum(d)),
        Uuid => strconv::format_uuid(buf, d.unwrap_uuid()),
        Record { fields } => {
            let mut fields = fields.iter();
            strconv::format_record(buf, &d.unwrap_list(), |buf, d| {
                let (_name, ty) = fields.next().unwrap();
                if d.is_null() {
                    buf.write_null()
                } else {
                    stringify_datum(buf.nonnull_buffer(), d, ty)
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
        List(elem_type) => strconv::format_list(buf, &d.unwrap_list(), |buf, d| {
            if d.is_null() {
                buf.write_null()
            } else {
                stringify_datum(buf.nonnull_buffer(), d, elem_type)
            }
        }),
    }
}

fn list_slice<'a>(datums: &[Datum<'a>], temp_storage: &'a RowArena) -> Datum<'a> {
    // Return value indicates whether this level's slices are empty results.
    fn slice_and_descend(d: Datum, ranges: &[(usize, usize)], packer: &mut RowPacker) -> bool {
        match ranges {
            [(start, n), ranges @ ..] if !d.is_null() => {
                let mut iter = d.unwrap_list().iter().skip(*start).take(*n).peekable();
                if iter.peek().is_none() {
                    packer.push(Datum::Null);
                    true
                } else {
                    let mut empty_results = true;
                    let start = packer.data().len();
                    packer.push_list_with(|packer| {
                        for d in iter {
                            // Determine if all higher-dimension slices produced empty results.
                            empty_results = slice_and_descend(d, ranges, packer) && empty_results;
                        }
                    });

                    if empty_results {
                        // If all results were empty, delete the list, insert a
                        // NULL, and notify lower-order slices that your results
                        // were empty.

                        // SAFETY: `start` points to a datum boundary because a)
                        // it comes from a call to `packer.data().len()` above,
                        // and b) recursive calls to `slice_and_descend` will
                        // not shrink the packer. (The recursive calls may write
                        // data and then erase that data, but a recursive call
                        // will never erase data that it did not write itself.)
                        unsafe { packer.truncate(start) }
                        packer.push(Datum::Null);
                    }
                    empty_results
                }
            }
            _ => {
                packer.push(d);
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

    temp_storage.make_datum(|packer| {
        slice_and_descend(datums[0], &ranges, packer);
    })
}

fn record_get(a: Datum, i: usize) -> Datum {
    a.unwrap_list().iter().nth(i).unwrap()
}

fn list_length(a: Datum) -> Datum {
    Datum::Int64(a.unwrap_list().iter().count() as i64)
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

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
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
}

impl VariadicFunc {
    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [ScalarExpr],
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
            VariadicFunc::PadLeading => Ok(eager!(pad_leading, temp_storage)?),
            VariadicFunc::Substr => Ok(eager!(substr)),
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
            VariadicFunc::SplitPart => Ok(eager!(split_part)?),
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
                ScalarType::List(Box::new(elem_type.clone())).nullable(false)
            }
            ListSlice { .. } => input_types[0].scalar_type.clone().nullable(true),
            RecordCreate { field_names } => ScalarType::Record {
                fields: field_names
                    .clone()
                    .into_iter()
                    .zip(input_types.into_iter().map(|ty| ty.scalar_type))
                    .collect(),
            }
            .nullable(true),
            SplitPart => ScalarType::String.nullable(true),
        }
    }

    /// Whether the function output is NULL if any of its inputs are NULL.
    pub fn propagates_nulls(&self) -> bool {
        !matches!(self, VariadicFunc::Coalesce
            | VariadicFunc::Concat
            | VariadicFunc::JsonbBuildArray
            | VariadicFunc::JsonbBuildObject
            | VariadicFunc::ListCreate { .. }
            | VariadicFunc::RecordCreate { .. }
            | VariadicFunc::ArrayCreate { .. }
            | VariadicFunc::ArrayToString { .. })
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
