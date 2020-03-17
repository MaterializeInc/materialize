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
use std::str::{self, FromStr};

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use encoding::label::encoding_from_whatwg_label;
use encoding::DecoderTrap;
use serde::{Deserialize, Serialize};

use ore::collections::CollectionExt;
use repr::decimal::MAX_DECIMAL_PRECISION;
use repr::jsonb::Jsonb;
use repr::regex::Regex;
use repr::{strconv, ColumnType, Datum, RowArena, RowPacker, ScalarType};

use crate::scalar::func::format::DateTimeFormat;
use crate::{like_pattern, EvalEnv, EvalError, ScalarExpr};

mod format;

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum NullaryFunc {
    MzLogicalTimestamp,
    Now,
}

impl NullaryFunc {
    pub fn eval<'a>(
        &'a self,
        _datums: &[Datum<'a>],
        env: &'a EvalEnv,
        _temp_storage: &'a RowArena,
    ) -> Result<Datum<'a>, EvalError> {
        match self {
            NullaryFunc::MzLogicalTimestamp => Ok(mz_logical_time(env)),
            NullaryFunc::Now => Ok(now(env)),
        }
    }

    pub fn output_type(&self) -> ColumnType {
        match self {
            NullaryFunc::MzLogicalTimestamp => ColumnType::new(ScalarType::Decimal(38, 0)),
            NullaryFunc::Now => ColumnType::new(ScalarType::TimestampTz),
        }
    }
}

impl fmt::Display for NullaryFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NullaryFunc::MzLogicalTimestamp => f.write_str("mz_logical_timestamp"),
            NullaryFunc::Now => f.write_str("now"),
        }
    }
}

fn mz_logical_time(env: &EvalEnv) -> Datum<'static> {
    Datum::from(
        env.logical_time
            .expect("mz_logical_time missing logical time") as i128,
    )
}

fn now(env: &EvalEnv) -> Datum<'static> {
    Datum::from(env.wall_time.expect("now missing wall time in env"))
}

pub fn and<'a>(
    datums: &[Datum<'a>],
    env: &'a EvalEnv,
    temp_storage: &'a RowArena,
    a_expr: &'a ScalarExpr,
    b_expr: &'a ScalarExpr,
) -> Result<Datum<'a>, EvalError> {
    match a_expr.eval(datums, env, temp_storage)? {
        Datum::False => Ok(Datum::False),
        a => match (a, b_expr.eval(datums, env, temp_storage)?) {
            (_, Datum::False) => Ok(Datum::False),
            (Datum::Null, _) | (_, Datum::Null) => Ok(Datum::Null),
            (Datum::True, Datum::True) => Ok(Datum::True),
            _ => unreachable!(),
        },
    }
}

pub fn or<'a>(
    datums: &[Datum<'a>],
    env: &'a EvalEnv,
    temp_storage: &'a RowArena,
    a_expr: &'a ScalarExpr,
    b_expr: &'a ScalarExpr,
) -> Result<Datum<'a>, EvalError> {
    match a_expr.eval(datums, env, temp_storage)? {
        Datum::True => Ok(Datum::True),
        a => match (a, b_expr.eval(datums, env, temp_storage)?) {
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
    Datum::from(a.unwrap_float32() as i64)
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
        Datum::from(f as i32)
    }
}

fn cast_float64_to_int64<'a>(a: Datum<'a>) -> Datum<'a> {
    // TODO(benesch): this is undefined behavior if the f32 doesn't fit in an
    // i64 (https://github.com/rust-lang/rust/issues/10184).
    Datum::from(a.unwrap_float64() as i64)
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

fn cast_decimal_to_int32<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_decimal().as_i128() as i32)
}

fn cast_decimal_to_int64<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_decimal().as_i128() as i64)
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

fn cast_string_to_bool<'a>(a: Datum<'a>) -> Datum<'a> {
    match strconv::parse_bool(a.unwrap_str()) {
        Ok(true) => Datum::True,
        Ok(false) => Datum::False,
        Err(_) => Datum::Null,
    }
}

fn cast_string_to_bytes<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    match strconv::parse_bytes(a.unwrap_str()) {
        Ok(bytes) => Datum::Bytes(temp_storage.push_bytes(bytes)),
        Err(_) => Datum::Null,
    }
}

fn cast_string_to_int32<'a>(a: Datum<'a>) -> Datum<'a> {
    match strconv::parse_int32(a.unwrap_str()) {
        Ok(n) => Datum::Int32(n),
        Err(_) => Datum::Null,
    }
}

fn cast_string_to_int64<'a>(a: Datum<'a>) -> Datum<'a> {
    match strconv::parse_int64(a.unwrap_str()) {
        Ok(n) => Datum::Int64(n),
        Err(_) => Datum::Null,
    }
}

fn cast_string_to_float32<'a>(a: Datum<'a>) -> Datum<'a> {
    match strconv::parse_float32(a.unwrap_str()) {
        Ok(n) => Datum::Float32(n.into()),
        Err(_) => Datum::Null,
    }
}

fn cast_string_to_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    match strconv::parse_float64(a.unwrap_str()) {
        Ok(n) => Datum::Float64(n.into()),
        Err(_) => Datum::Null,
    }
}

fn cast_string_to_decimal<'a>(a: Datum<'a>, scale: u8) -> Datum<'a> {
    match strconv::parse_decimal(a.unwrap_str()) {
        Ok(d) => Datum::from(match d.scale().cmp(&scale) {
            Ordering::Less => d.significand() * 10_i128.pow(u32::from(scale - d.scale())),
            Ordering::Equal => d.significand(),
            Ordering::Greater => d.significand() / 10_i128.pow(u32::from(d.scale() - scale)),
        }),
        Err(_) => Datum::Null,
    }
}

fn cast_string_to_date<'a>(a: Datum<'a>) -> Datum<'a> {
    match strconv::parse_date(a.unwrap_str()) {
        Ok(d) => Datum::Date(d),
        Err(_) => Datum::Null,
    }
}

fn cast_string_to_time<'a>(a: Datum<'a>) -> Datum<'a> {
    match strconv::parse_time(a.unwrap_str()) {
        Ok(t) => Datum::Time(t),
        Err(_) => Datum::Null,
    }
}

fn cast_string_to_timestamp<'a>(a: Datum<'a>) -> Datum<'a> {
    match strconv::parse_timestamp(a.unwrap_str()) {
        Ok(ts) => Datum::Timestamp(ts),
        Err(_) => Datum::Null,
    }
}

fn cast_string_to_timestamptz<'a>(a: Datum<'a>) -> Datum<'a> {
    match strconv::parse_timestamptz(a.unwrap_str()) {
        Ok(ts) => Datum::TimestampTz(ts),
        Err(_) => Datum::Null,
    }
}

fn cast_string_to_interval<'a>(a: Datum<'a>) -> Datum<'a> {
    match strconv::parse_interval(a.unwrap_str()) {
        Ok(iv) => Datum::Interval(iv),
        Err(_) => Datum::Null,
    }
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

// TODO(jamii): it would be much more efficient to skip the intermediate
// repr::jsonb::Jsonb.
fn jsonb_stringify<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    strconv::format_jsonb(&mut buf, &Jsonb::from_datum(a));
    Datum::String(temp_storage.push_string(buf))
}

fn jsonb_stringify_unless_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    match a {
        Datum::JsonNull => Datum::Null,
        Datum::String(_) => a,
        _ => jsonb_stringify(a, temp_storage),
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

fn cast_jsonb_to_string<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::String(_) => a,
        _ => Datum::Null,
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

fn add_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int32() + b.unwrap_int32())
}

fn add_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int64() + b.unwrap_int64())
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
            add_timestamp_duration(dt, i.is_positive_dur, i.duration)
        }
        _ => panic!("Tried to do timestamp addition with non-interval: {:?}", b),
    })
}

fn add_timestamptz_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let dt = a.unwrap_timestamptz().naive_utc();

    let new_ndt = match b {
        Datum::Interval(i) => {
            let dt = add_timestamp_months(dt, i.months);
            add_timestamp_duration(dt, i.is_positive_dur, i.duration)
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
    Datum::Timestamp(add_timestamp_duration(
        dt,
        interval.is_positive_dur,
        interval.duration,
    ))
}

fn add_time_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let time = a.unwrap_time();
    let interval = b.unwrap_interval();
    let date = match chrono::Duration::from_std(interval.duration) {
        Ok(d) => d,
        Err(_) => return Datum::Null,
    };

    let time = if interval.is_positive_dur {
        let (t, _) = time.overflowing_add_signed(date);
        t
    } else {
        let (t, _) = time.overflowing_sub_signed(date);
        t
    };

    Datum::Time(time)
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

fn round_decimal<'a>(a: Datum<'a>, b: Datum<'a>, a_scale: u8) -> Datum<'a> {
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

    match encoding_from_whatwg_label(&encoding_name) {
        Some(enc) => {
            // todo@jldlaughlin: #2282
            if enc.name() != "utf-8" {
                return Err(EvalError::InvalidEncodingName(encoding_name));
            }
        }
        None => return Err(EvalError::InvalidEncodingName(encoding_name)),
    }

    let bytes = a.unwrap_bytes();
    match str::from_utf8(bytes) {
        Ok(from) => Ok(Datum::String(from)),
        Err(e) => {
            let mut byte_sequence = String::new();
            strconv::format_bytes(&mut byte_sequence, &bytes);
            Err(EvalError::InvalidByteSequence {
                byte_sequence: e.to_string(),
                encoding_name,
            })
        }
    }
}

fn sub_timestamp_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let inverse = match b {
        Datum::Interval(i) => {
            let mut res = i;
            res.months = -res.months;
            res.is_positive_dur = !res.is_positive_dur;
            Datum::Interval(res)
        }
        _ => panic!(
            "Tried to do timestamptz subtraction with non-interval: {:?}",
            b
        ),
    };
    add_timestamp_interval(a, inverse)
}

fn sub_timestamptz_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let inverse = match b {
        Datum::Interval(i) => {
            let mut res = i;
            res.months = -res.months;
            res.is_positive_dur = !res.is_positive_dur;
            Datum::Interval(res)
        }
        _ => panic!(
            "Tried to do timestamptz subtraction with non-interval: {:?}",
            b
        ),
    };
    add_timestamptz_interval(a, inverse)
}

fn add_timestamp_months(dt: NaiveDateTime, months: i64) -> NaiveDateTime {
    if months == 0 {
        return dt;
    }

    let mut months: i32 = months.try_into().expect("fewer than i64 months");
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

fn add_timestamp_duration(
    dt: NaiveDateTime,
    is_positive: bool,
    duration: std::time::Duration,
) -> NaiveDateTime {
    let d = chrono::Duration::from_std(duration).expect("a duration that fits into i64 seconds");
    if is_positive {
        dt + d
    } else {
        dt - d
    }
}

fn add_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_decimal() + b.unwrap_decimal())
}

fn add_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_interval() + b.unwrap_interval())
}

fn sub_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int32() - b.unwrap_int32())
}

fn sub_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int64() - b.unwrap_int64())
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

fn sub_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_interval() - b.unwrap_interval())
}

fn sub_date_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let date = a.unwrap_date();
    let interval = b.unwrap_interval();

    let dt = NaiveDate::from_ymd(date.year(), date.month(), date.day()).and_hms(0, 0, 0);
    let dt = add_timestamp_months(dt, -interval.months);
    Datum::Timestamp(add_timestamp_duration(
        dt,
        !interval.is_positive_dur,
        interval.duration,
    ))
}

fn sub_time_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let time = a.unwrap_time();
    let interval = b.unwrap_interval();
    let date = match chrono::Duration::from_std(interval.duration) {
        Ok(d) => d,
        Err(_) => return Datum::Null,
    };

    let time = if interval.is_positive_dur {
        let (t, _) = time.overflowing_sub_signed(date);
        t
    } else {
        let (t, _) = time.overflowing_add_signed(date);
        t
    };

    Datum::Time(time)
}

fn mul_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int32() * b.unwrap_int32())
}

fn mul_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int64() * b.unwrap_int64())
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

fn sqrt_float32<'a>(a: Datum<'a>) -> Datum<'a> {
    let x = a.unwrap_float32();
    if x < 0.0 {
        return Datum::Null;
    }
    Datum::from(x.sqrt())
}

fn sqrt_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    let x = a.unwrap_float64();
    if x < 0.0 {
        return Datum::Null;
    }
    Datum::from(x.sqrt())
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

fn jsonb_get_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let i = b.unwrap_int64();
    match a {
        Datum::List(list) => {
            let i = if i >= 0 {
                i
            } else {
                // index backwards from the end
                (list.iter().count() as i64) + i
            };
            list.iter().nth(i as usize).unwrap_or(Datum::Null)
        }
        Datum::Dict(_) => Datum::Null,
        _ => {
            if i == 0 || i == -1 {
                // I have no idea why postgres does this, but we're stuck with it
                a
            } else {
                Datum::Null
            }
        }
    }
}

fn jsonb_get_string<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let k = b.unwrap_str();
    match a {
        Datum::Dict(dict) => match dict.iter().find(|(k2, _v)| k == *k2) {
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

fn match_regex<'a>(a: Datum<'a>, needle: &regex::Regex) -> Datum<'a> {
    let haystack = a.unwrap_str();
    Datum::from(needle.is_match(haystack))
}

fn ascii<'a>(a: Datum<'a>) -> Datum<'a> {
    match a.unwrap_str().chars().next() {
        None => Datum::Int32(0),
        Some(v) => Datum::Int32(v as i32),
    }
}

/// A timestamp with both a date and a time component, but not necessarily a
/// timezone component.
pub trait TimestampLike: chrono::Datelike + chrono::Timelike {
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
        (s + ns)
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
    
    fn truncate_microseconds(&self) -> NaiveDateTime {
        let time = NaiveTime::from_hms_micro(
            self.hour(),
            self.minute(),
            self.second(),
            self.nanosecond() / 1_000,
        );

        NaiveDateTime::new(self.date(), time)
    }

    fn truncate_milliseconds(&self) -> NaiveDateTime {
        let time = NaiveTime::from_hms_milli(
            self.hour(),
            self.minute(),
            self.second(),
            self.nanosecond() / 1_000_000,
        );

        NaiveDateTime::new(self.date(), time)
    }
    
    fn truncate_second(&self) -> NaiveDateTime {
        let time = NaiveTime::from_hms(
            self.hour(),
            self.minute(),
            self.second(),
        );

        NaiveDateTime::new(self.date(), time)
    }

    fn truncate_minute(&self) -> NaiveDateTime {
        NaiveDateTime::new(
            self.date(),
            NaiveTime::from_hms(self.hour(), self.minute(), 0))
    }
    
    fn truncate_hour(&self) -> NaiveDateTime {
        NaiveDateTime::new(
            self.date(),
            NaiveTime::from_hms(self.hour(), 0, 0))
    }
    
    fn truncate_day(&self) -> NaiveDateTime {
        NaiveDateTime::new(
            self.date(),
            NaiveTime::from_hms(0, 0, 0))
    }

    fn truncate_week(&self) -> NaiveDateTime {
        let num_days_from_monday = self.date().weekday().num_days_from_monday();
        NaiveDateTime::new(
            NaiveDate::from_ymd(
                self.year(),
                self.month(),
                self.day() - num_days_from_monday,
            ),
            NaiveTime::from_hms(0, 0, 0),
        )
    }
    
    fn truncate_month(&self) -> NaiveDateTime {
        NaiveDateTime::new(
            NaiveDate::from_ymd(
                self.year(),
                self.month(),
                1,
            ),
            NaiveTime::from_hms(0, 0, 0),
        )
    }

    fn truncate_quarter(&self) -> NaiveDateTime {
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

        NaiveDateTime::new(
            NaiveDate::from_ymd(self.year(), quarter, 1),
            NaiveTime::from_hms(0, 0, 0),
        )
    }

    fn truncate_year(&self) -> NaiveDateTime {
        NaiveDateTime::new(
            NaiveDate::from_ymd(
                self.year(),
                1,
                1,
            ),
            NaiveTime::from_hms(0, 0, 0),
        )
    }
    fn truncate_decade(&self) -> NaiveDateTime {
        NaiveDateTime::new(
            NaiveDate::from_ymd(
                self.year() - (self.year() % 10),
                1,
                1,
            ),
            NaiveTime::from_hms(0, 0, 0),
        )
    }
    fn truncate_century(&self) -> NaiveDateTime {
        // Expects the first year of the century, meaning 2001 instead of 2000.
        NaiveDateTime::new(
            NaiveDate::from_ymd(
                self.year() - (self.year() % 100) + 1,
                1,
                1,
            ),
            NaiveTime::from_hms(0, 0, 0),
        )
    }
    fn truncate_millennium(&self) -> NaiveDateTime {
        // Expects the first year of the millennium, meaning 2001 instead of 2000.
        NaiveDateTime::new(
            NaiveDate::from_ymd(
                self.year() - (self.year() % 1_000) + 1,
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
    fn date(&self) -> NaiveDate {
        self.date()
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
    fn date(&self) -> NaiveDate {
        self.naive_utc().date()
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

fn extract_interval_year<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_interval().years())
}

fn extract_interval_month<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_interval().months())
}

fn extract_interval_day<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_interval().days())
}

fn extract_interval_hour<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_interval().hours())
}

fn extract_interval_minute<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_interval().minutes())
}

fn extract_interval_second<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_interval().seconds())
}

fn extract_timelike_year<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::extract_year(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => Datum::from(TimestampLike::extract_year(&a.unwrap_timestamptz())),
        _ => panic!("scalar::func::extract_timelike_year called on {:?}", a),
    }
}

fn extract_timelike_quarter<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::extract_quarter(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => {
            Datum::from(TimestampLike::extract_quarter(&a.unwrap_timestamptz()))
        }
        _ => panic!("scalar::func::extract_timelike_quarter called on {:?}", a),
    }
}

fn extract_timelike_month<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::extract_month(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => Datum::from(TimestampLike::extract_month(&a.unwrap_timestamptz())),
        _ => panic!("scalar::func::extract_timelike_month called on {:?}", a),
    }
}

fn extract_timelike_day<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::extract_day(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => Datum::from(TimestampLike::extract_day(&a.unwrap_timestamptz())),
        _ => panic!("scalar::func::extract_timelike_day called on {:?}", a),
    }
}

fn extract_timelike_hour<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::extract_hour(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => Datum::from(TimestampLike::extract_hour(&a.unwrap_timestamptz())),
        _ => panic!("scalar::func::extract_timelike_hour called on {:?}", a),
    }
}

fn extract_timelike_minute<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::extract_minute(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => {
            Datum::from(TimestampLike::extract_minute(&a.unwrap_timestamptz()))
        }
        _ => panic!("scalar::func::extract_timelike_minute called on {:?}", a),
    }
}

fn extract_timelike_second<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::extract_second(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => {
            Datum::from(TimestampLike::extract_second(&a.unwrap_timestamptz()))
        }
        _ => panic!("scalar::func::extract_timelike_second called on {:?}", a),
    }
}

fn extract_timelike_week<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::extract_week(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => Datum::from(TimestampLike::extract_week(&a.unwrap_timestamptz())),
        _ => panic!("scalar::func::extract_timelike_week called on {:?}", a),
    }
}

fn extract_timelike_dayofyear<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::extract_dayofyear(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => {
            Datum::from(TimestampLike::extract_dayofyear(&a.unwrap_timestamptz()))
        }
        _ => panic!("scalar::func::extract_timelike_dayofyear called on {:?}", a),
    }
}

fn extract_timelike_dayofweek<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::extract_dayofweek(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => {
            Datum::from(TimestampLike::extract_dayofweek(&a.unwrap_timestamptz()))
        }
        _ => panic!("scalar::func::extract_timelike_dayofweek called on {:?}", a),
    }
}

fn extract_timelike_isodayofweek<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => {
            Datum::from(TimestampLike::extract_isodayofweek(&a.unwrap_timestamp()))
        }
        Datum::TimestampTz(_) => {
            Datum::from(TimestampLike::extract_isodayofweek(&a.unwrap_timestamptz()))
        }
        _ => panic!(
            "scalar::func::extract_timelike_isodayofweek called on {:?}",
            a
        ),
    }
}

fn date_trunc<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let units = a.unwrap_str();
    match units.parse::<DateTruncTo>() {
        Ok(DateTruncTo::Micros) => Ok(date_trunc_microseconds(b)),
        Ok(DateTruncTo::Millis) => Ok(date_trunc_milliseconds(b)),
        Ok(DateTruncTo::Second) => Ok(date_trunc_second(b)),
        Ok(DateTruncTo::Minute) => Ok(date_trunc_minute(b)),
        Ok(DateTruncTo::Hour) => Ok(date_trunc_hour(b)),
        Ok(DateTruncTo::Day) => Ok(date_trunc_day(b)),
        Ok(DateTruncTo::Week) => Ok(date_trunc_week(b)),
        Ok(DateTruncTo::Month) => Ok(date_trunc_month(b)),
        Ok(DateTruncTo::Quarter) => Ok(date_trunc_quarter(b)),
        Ok(DateTruncTo::Year) => Ok(date_trunc_year(b)),
        Ok(DateTruncTo::Decade) => Ok(date_trunc_decade(b)),
        Ok(DateTruncTo::Century) => Ok(date_trunc_century(b)),
        Ok(DateTruncTo::Millennium) => Ok(date_trunc_millennium(b)),
        Err(_) => Err(EvalError::UnknownUnits(units.to_owned())),
    }
}

fn date_trunc_microseconds<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::truncate_microseconds(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => {
            Datum::timestamptz(TimestampLike::truncate_microseconds(&a.unwrap_timestamptz()))
        }
        _ => panic!("scalar::func::date_trunc_microseconds called on {:?}", a),
    }
}

fn date_trunc_milliseconds<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::truncate_milliseconds(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => {
            Datum::timestamptz(TimestampLike::truncate_milliseconds(&a.unwrap_timestamptz()))
        }
        _ => panic!("scalar::func::date_trunc_millisecondss called on {:?}", a),
    }
}

fn date_trunc_second<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::truncate_second(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => {
            Datum::timestamptz(TimestampLike::truncate_second(&a.unwrap_timestamptz()))
        }
        _ => panic!("scalar::func::date_trunc_second called on {:?}", a),
    }
}

fn date_trunc_minute<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::truncate_minute(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => {
            Datum::timestamptz(TimestampLike::truncate_minute(&a.unwrap_timestamptz()))
        }
        _ => panic!("scalar::func::date_trunc_minute called on {:?}", a),
    }
}

fn date_trunc_hour<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::truncate_hour(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => {
            Datum::timestamptz(TimestampLike::truncate_hour(&a.unwrap_timestamptz()))
        }
        _ => panic!("scalar::func::date_trunc_hour called on {:?}", a),
    }
}

fn date_trunc_day<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::truncate_day(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => {
            Datum::timestamptz(TimestampLike::truncate_day(&a.unwrap_timestamptz()))
        }
        _ => panic!("scalar::func::date_trunc_day called on {:?}", a),
    }
}

fn date_trunc_week<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::truncate_week(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => {
            Datum::timestamptz(TimestampLike::truncate_week(&a.unwrap_timestamptz()))
        }
        _ => panic!("scalar::func::date_trunc_week called on {:?}", a),
    }
}

fn date_trunc_month<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::truncate_month(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => {
            Datum::timestamptz(TimestampLike::truncate_month(&a.unwrap_timestamptz()))
        }
        _ => panic!("scalar::func::date_trunc_month called on {:?}", a),
    }
}

fn date_trunc_quarter<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::truncate_quarter(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => {
            Datum::timestamptz(TimestampLike::truncate_quarter(&a.unwrap_timestamptz()))
        }
        _ => panic!("scalar::func::date_trunc_quarter called on {:?}", a),
    }
}

fn date_trunc_year<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::truncate_year(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => {
            Datum::timestamptz(TimestampLike::truncate_year(&a.unwrap_timestamptz()))
        }
        _ => panic!("scalar::func::date_trunc_year called on {:?}", a),
    }
}

fn date_trunc_decade<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::truncate_decade(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => {
            Datum::timestamptz(TimestampLike::truncate_decade(&a.unwrap_timestamptz()))
        }
        _ => panic!("scalar::func::date_trunc_decade called on {:?}", a),
    }
}

fn date_trunc_century<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::truncate_century(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => {
            Datum::timestamptz(TimestampLike::truncate_century(&a.unwrap_timestamptz()))
        }
        _ => panic!("scalar::func::date_trunc_century called on {:?}", a),
    }
}

fn date_trunc_millennium<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Timestamp(_) => Datum::from(TimestampLike::truncate_millennium(&a.unwrap_timestamp())),
        Datum::TimestampTz(_) => {
            Datum::timestamptz(TimestampLike::truncate_millennium(&a.unwrap_timestamptz()))
        }
        _ => panic!("scalar::func::date_trunc_millennium called on {:?}", a),
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
    strconv::format_jsonb_pretty(&mut buf, &Jsonb::from_datum(a));
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
    ToCharTimestamp,
    ToCharTimestampTz,
    DateTruncTimestamp,
    DateTruncTimestampTz,
    CastFloat32ToDecimal,
    CastFloat64ToDecimal,
    TextConcat,
    JsonbGetInt64,
    JsonbGetString,
    JsonbContainsString,
    JsonbConcat,
    JsonbContainsJsonb,
    JsonbDeleteInt64,
    JsonbDeleteString,
    ConvertFrom,
}

#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DateTruncTo {
    Micros,
    Millis,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
    Decade,
    Century,
    Millennium,
}

impl FromStr for DateTruncTo {
    type Err = EvalError;

    fn from_str(s: &str) -> Result<DateTruncTo, Self::Err> {
        let s = unicase::Ascii::new(s);
        if s == "microseconds" {
            Ok(DateTruncTo::Micros)
        } else if s == "milliseconds" {
            Ok(DateTruncTo::Millis)
        } else if s == "second" {
            Ok(DateTruncTo::Second)
        } else if s == "minute" {
            Ok(DateTruncTo::Minute)
        } else if s == "hour" {
            Ok(DateTruncTo::Hour)
        } else if s == "day" {
            Ok(DateTruncTo::Day)
        } else if s == "week" {
            Ok(DateTruncTo::Week)
        } else if s == "month" {
            Ok(DateTruncTo::Month)
        } else if s == "quarter" {
            Ok(DateTruncTo::Quarter)
        } else if s == "year" {
            Ok(DateTruncTo::Year)
        } else if s == "decade" {
            Ok(DateTruncTo::Decade)
        } else if s == "century" {
            Ok(DateTruncTo::Century)
        } else if s == "millennium" {
            Ok(DateTruncTo::Millennium)
        } else {
            Err(EvalError::UnknownUnits(s.into_inner().to_owned()))
        }
    }
}

impl BinaryFunc {
    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        env: &'a EvalEnv,
        temp_storage: &'a RowArena,
        a_expr: &'a ScalarExpr,
        b_expr: &'a ScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        macro_rules! eager {
            ($func:ident $(, $args:expr)*) => {{
                let a = a_expr.eval(datums, env, temp_storage)?;
                let b = b_expr.eval(datums, env, temp_storage)?;
                if self.propagates_nulls() && (a.is_null() || b.is_null()) {
                    return Ok(Datum::Null);
                }
                $func(a, b $(, $args)*)
            }}
        }

        match self {
            BinaryFunc::And => and(datums, env, temp_storage, a_expr, b_expr),
            BinaryFunc::Or => or(datums, env, temp_storage, a_expr, b_expr),
            BinaryFunc::AddInt32 => Ok(eager!(add_int32)),
            BinaryFunc::AddInt64 => Ok(eager!(add_int64)),
            BinaryFunc::AddFloat32 => Ok(eager!(add_float32)),
            BinaryFunc::AddFloat64 => Ok(eager!(add_float64)),
            BinaryFunc::AddTimestampInterval => Ok(eager!(add_timestamp_interval)),
            BinaryFunc::AddTimestampTzInterval => Ok(eager!(add_timestamptz_interval)),
            BinaryFunc::AddDateTime => Ok(eager!(add_date_time)),
            BinaryFunc::AddDateInterval => Ok(eager!(add_date_interval)),
            BinaryFunc::AddTimeInterval => Ok(eager!(add_time_interval)),
            BinaryFunc::AddDecimal => Ok(eager!(add_decimal)),
            BinaryFunc::AddInterval => Ok(eager!(add_interval)),
            BinaryFunc::SubInt32 => Ok(eager!(sub_int32)),
            BinaryFunc::SubInt64 => Ok(eager!(sub_int64)),
            BinaryFunc::SubFloat32 => Ok(eager!(sub_float32)),
            BinaryFunc::SubFloat64 => Ok(eager!(sub_float64)),
            BinaryFunc::SubTimestamp => Ok(eager!(sub_timestamp)),
            BinaryFunc::SubTimestampTz => Ok(eager!(sub_timestamptz)),
            BinaryFunc::SubTimestampInterval => Ok(eager!(sub_timestamp_interval)),
            BinaryFunc::SubTimestampTzInterval => Ok(eager!(sub_timestamptz_interval)),
            BinaryFunc::SubInterval => Ok(eager!(sub_interval)),
            BinaryFunc::SubDate => Ok(eager!(sub_date)),
            BinaryFunc::SubDateInterval => Ok(eager!(sub_date_interval)),
            BinaryFunc::SubTime => Ok(eager!(sub_time)),
            BinaryFunc::SubTimeInterval => Ok(eager!(sub_time_interval)),
            BinaryFunc::SubDecimal => Ok(eager!(sub_decimal)),
            BinaryFunc::MulInt32 => Ok(eager!(mul_int32)),
            BinaryFunc::MulInt64 => Ok(eager!(mul_int64)),
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
            BinaryFunc::ToCharTimestamp => Ok(eager!(to_char_timestamp, temp_storage)),
            BinaryFunc::ToCharTimestampTz => Ok(eager!(to_char_timestamptz, temp_storage)),
            BinaryFunc::DateTruncTimestamp => eager!(date_trunc),
            BinaryFunc::DateTruncTimestampTz => eager!(date_trunc),
            BinaryFunc::CastFloat32ToDecimal => eager!(cast_float32_to_decimal),
            BinaryFunc::CastFloat64ToDecimal => eager!(cast_float64_to_decimal),
            BinaryFunc::TextConcat => Ok(eager!(text_concat_binary, temp_storage)),
            BinaryFunc::JsonbGetInt64 => Ok(eager!(jsonb_get_int64)),
            BinaryFunc::JsonbGetString => Ok(eager!(jsonb_get_string)),
            BinaryFunc::JsonbContainsString => Ok(eager!(jsonb_contains_string)),
            BinaryFunc::JsonbConcat => Ok(eager!(jsonb_concat, temp_storage)),
            BinaryFunc::JsonbContainsJsonb => Ok(eager!(jsonb_contains_jsonb)),
            BinaryFunc::JsonbDeleteInt64 => Ok(eager!(jsonb_delete_int64, temp_storage)),
            BinaryFunc::JsonbDeleteString => Ok(eager!(jsonb_delete_string, temp_storage)),
            BinaryFunc::RoundDecimal(scale) => Ok(eager!(round_decimal, *scale)),
            BinaryFunc::ConvertFrom => eager!(convert_from),
        }
    }

    pub fn output_type(&self, input1_type: ColumnType, input2_type: ColumnType) -> ColumnType {
        use BinaryFunc::*;
        let in_nullable = input1_type.nullable || input2_type.nullable;
        let is_div_mod = match self {
            DivInt32 | ModInt32 | DivInt64 | ModInt64 | DivFloat32 | ModFloat32 | DivFloat64
            | ModFloat64 | DivDecimal | ModDecimal => true,
            _ => false,
        };
        match self {
            And | Or | Eq | NotEq | Lt | Lte | Gt | Gte => {
                ColumnType::new(ScalarType::Bool).nullable(in_nullable)
            }

            MatchLikePattern => {
                // The output can be null if the pattern is invalid.
                ColumnType::new(ScalarType::Bool).nullable(true)
            }

            ToCharTimestamp | ToCharTimestampTz | ConvertFrom => {
                ColumnType::new(ScalarType::String).nullable(false)
            }

            AddInt32 | SubInt32 | MulInt32 | DivInt32 | ModInt32 => {
                ColumnType::new(ScalarType::Int32).nullable(in_nullable || is_div_mod)
            }

            AddInt64 | SubInt64 | MulInt64 | DivInt64 | ModInt64 => {
                ColumnType::new(ScalarType::Int64).nullable(in_nullable || is_div_mod)
            }

            AddFloat32 | SubFloat32 | MulFloat32 | DivFloat32 | ModFloat32 => {
                ColumnType::new(ScalarType::Float32).nullable(in_nullable || is_div_mod)
            }

            AddFloat64 | SubFloat64 | MulFloat64 | DivFloat64 | ModFloat64 => {
                ColumnType::new(ScalarType::Float64).nullable(in_nullable || is_div_mod)
            }

            AddInterval | SubInterval | SubTimestamp | SubTimestampTz | SubDate => {
                ColumnType::new(ScalarType::Interval).nullable(in_nullable)
            }

            // TODO(benesch): we correctly compute types for decimal scale, but
            // not decimal precision... because nothing actually cares about
            // decimal precision. Should either remove or fix.
            AddDecimal | SubDecimal | ModDecimal => {
                let (s1, s2) = match (&input1_type.scalar_type, &input2_type.scalar_type) {
                    (ScalarType::Unknown, _) | (_, ScalarType::Unknown) => {
                        return ColumnType::new(ScalarType::Unknown);
                    }
                    (ScalarType::Decimal(_, s1), ScalarType::Decimal(_, s2)) => (s1, s2),
                    _ => unreachable!(),
                };
                assert_eq!(s1, s2);
                ColumnType::new(ScalarType::Decimal(MAX_DECIMAL_PRECISION, *s1))
                    .nullable(in_nullable || is_div_mod)
            }
            MulDecimal => {
                let (s1, s2) = match (&input1_type.scalar_type, &input2_type.scalar_type) {
                    (ScalarType::Unknown, _) | (_, ScalarType::Unknown) => {
                        return ColumnType::new(ScalarType::Unknown);
                    }
                    (ScalarType::Decimal(_, s1), ScalarType::Decimal(_, s2)) => (s1, s2),
                    _ => unreachable!(),
                };
                let s = s1 + s2;
                ColumnType::new(ScalarType::Decimal(MAX_DECIMAL_PRECISION, s)).nullable(in_nullable)
            }
            DivDecimal => {
                let (s1, s2) = match (&input1_type.scalar_type, &input2_type.scalar_type) {
                    (ScalarType::Unknown, _) | (_, ScalarType::Unknown) => {
                        return ColumnType::new(ScalarType::Unknown);
                    }
                    (ScalarType::Decimal(_, s1), ScalarType::Decimal(_, s2)) => (s1, s2),
                    _ => unreachable!(),
                };
                let s = s1 - s2;
                ColumnType::new(ScalarType::Decimal(MAX_DECIMAL_PRECISION, s)).nullable(true)
            }

            CastFloat32ToDecimal | CastFloat64ToDecimal => match input2_type.scalar_type {
                ScalarType::Unknown => ColumnType::new(ScalarType::Unknown),
                ScalarType::Decimal(_, s) => {
                    ColumnType::new(ScalarType::Decimal(MAX_DECIMAL_PRECISION, s)).nullable(true)
                }
                _ => unreachable!(),
            },

            RoundDecimal(scale) => {
                match input1_type.scalar_type {
                    ScalarType::Decimal(_, s) => assert_eq!(*scale, s),
                    _ => unreachable!(),
                }
                ColumnType::new(input1_type.scalar_type).nullable(in_nullable)
            }

            AddTimestampInterval
            | SubTimestampInterval
            | AddTimestampTzInterval
            | SubTimestampTzInterval
            | AddTimeInterval
            | SubTimeInterval => input1_type,

            AddDateInterval | SubDateInterval | AddDateTime | DateTruncTimestamp => {
                ColumnType::new(ScalarType::Timestamp).nullable(true)
            }

            DateTruncTimestampTz => ColumnType::new(ScalarType::TimestampTz).nullable(true),

            SubTime => ColumnType::new(ScalarType::Interval).nullable(true),

            TextConcat => ColumnType::new(ScalarType::String).nullable(in_nullable),

            JsonbGetInt64 | JsonbGetString | JsonbConcat | JsonbDeleteInt64 | JsonbDeleteString => {
                ColumnType::new(ScalarType::Jsonb).nullable(true)
            }

            JsonbContainsString | JsonbContainsJsonb => {
                ColumnType::new(ScalarType::Bool).nullable(in_nullable)
            }
        }
    }

    /// Whether the function output is NULL if any of its inputs are NULL.
    pub fn propagates_nulls(&self) -> bool {
        match self {
            BinaryFunc::And | BinaryFunc::Or => false,
            _ => true,
        }
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
            | JsonbGetInt64
            | JsonbGetString
            | JsonbContainsString
            | JsonbDeleteInt64
            | JsonbDeleteString
            | TextConcat => true,
            MatchLikePattern | ToCharTimestamp | ToCharTimestampTz | DateTruncTimestamp
            | DateTruncTimestampTz | CastFloat32ToDecimal | CastFloat64ToDecimal
            | RoundDecimal(_) | ConvertFrom => false,
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
            BinaryFunc::ToCharTimestamp => f.write_str("tocharts"),
            BinaryFunc::ToCharTimestampTz => f.write_str("tochartstz"),
            BinaryFunc::DateTruncTimestamp => f.write_str("date_truncts"),
            BinaryFunc::DateTruncTimestampTz => f.write_str("date_trunctstz"),
            BinaryFunc::CastFloat32ToDecimal => f.write_str("f32todec"),
            BinaryFunc::CastFloat64ToDecimal => f.write_str("f64todec"),
            BinaryFunc::TextConcat => f.write_str("||"),
            BinaryFunc::JsonbGetInt64 => f.write_str("->"),
            BinaryFunc::JsonbGetString => f.write_str("->"),
            BinaryFunc::JsonbContainsString => f.write_str("?"),
            BinaryFunc::JsonbConcat => f.write_str("||"),
            BinaryFunc::JsonbContainsJsonb => f.write_str("<@"),
            BinaryFunc::JsonbDeleteInt64 => f.write_str("-"),
            BinaryFunc::JsonbDeleteString => f.write_str("-"),
            BinaryFunc::RoundDecimal(_) => f.write_str("round"),
            BinaryFunc::ConvertFrom => f.write_str("convert_from"),
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
    AbsInt32,
    AbsInt64,
    AbsFloat32,
    AbsFloat64,
    CastBoolToStringExplicit,
    CastBoolToStringImplicit,
    CastInt32ToBool,
    CastInt32ToFloat32,
    CastInt32ToFloat64,
    CastInt32ToInt64,
    CastInt32ToString,
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
    CastDecimalToInt32,
    CastDecimalToInt64,
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
    CastDateToTimestamp,
    CastDateToTimestampTz,
    CastDateToString,
    CastTimestampToDate,
    CastTimestampToTimestampTz,
    CastTimestampToString,
    CastTimestampTzToDate,
    CastTimestampTzToTimestamp,
    CastTimestampTzToString,
    CastIntervalToString,
    CastBytesToString,
    CastStringToJsonb,
    JsonbStringify,
    JsonbStringifyUnlessString,
    CastJsonbOrNullToJsonb,
    CastJsonbToString,
    CastJsonbToFloat64,
    CastJsonbToBool,
    CeilFloat32,
    CeilFloat64,
    CeilDecimal(u8),
    FloorFloat32,
    FloorFloat64,
    FloorDecimal(u8),
    Ascii,
    LengthBytes,
    MatchRegex(Regex),
    ExtractIntervalYear,
    ExtractIntervalMonth,
    ExtractIntervalDay,
    ExtractIntervalHour,
    ExtractIntervalMinute,
    ExtractIntervalSecond,
    ExtractTimestampYear,
    ExtractTimestampQuarter,
    ExtractTimestampMonth,
    ExtractTimestampDay,
    ExtractTimestampHour,
    ExtractTimestampMinute,
    ExtractTimestampSecond,
    ExtractTimestampWeek,
    ExtractTimestampDayOfYear,
    ExtractTimestampDayOfWeek,
    ExtractTimestampIsoDayOfWeek,
    ExtractTimestampTzYear,
    ExtractTimestampTzQuarter,
    ExtractTimestampTzMonth,
    ExtractTimestampTzDay,
    ExtractTimestampTzHour,
    ExtractTimestampTzMinute,
    ExtractTimestampTzSecond,
    ExtractTimestampTzWeek,
    ExtractTimestampTzDayOfYear,
    ExtractTimestampTzDayOfWeek,
    ExtractTimestampTzIsoDayOfWeek,
    DateTrunc(DateTruncTo),
    ToTimestamp,
    JsonbArrayLength,
    JsonbTypeof,
    JsonbStripNulls,
    JsonbPretty,
    RoundFloat32,
    RoundFloat64,
}

impl UnaryFunc {
    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        env: &'a EvalEnv,
        temp_storage: &'a RowArena,
        a: &'a ScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        let a = a.eval(datums, env, temp_storage)?;
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
            UnaryFunc::CastBoolToStringExplicit => Ok(cast_bool_to_string_explicit(a)),
            UnaryFunc::CastBoolToStringImplicit => Ok(cast_bool_to_string_implicit(a)),
            UnaryFunc::CastInt32ToBool => Ok(cast_int32_to_bool(a)),
            UnaryFunc::CastInt32ToFloat32 => Ok(cast_int32_to_float32(a)),
            UnaryFunc::CastInt32ToFloat64 => Ok(cast_int32_to_float64(a)),
            UnaryFunc::CastInt32ToInt64 => Ok(cast_int32_to_int64(a)),
            UnaryFunc::CastInt32ToDecimal => Ok(cast_int32_to_decimal(a)),
            UnaryFunc::CastInt32ToString => Ok(cast_int32_to_string(a, temp_storage)),
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
            UnaryFunc::CastDecimalToInt32 => Ok(cast_decimal_to_int32(a)),
            UnaryFunc::CastDecimalToInt64 => Ok(cast_decimal_to_int64(a)),
            UnaryFunc::CastSignificandToFloat32 => Ok(cast_significand_to_float32(a)),
            UnaryFunc::CastSignificandToFloat64 => Ok(cast_significand_to_float64(a)),
            UnaryFunc::CastStringToBool => Ok(cast_string_to_bool(a)),
            UnaryFunc::CastStringToBytes => Ok(cast_string_to_bytes(a, temp_storage)),
            UnaryFunc::CastStringToInt32 => Ok(cast_string_to_int32(a)),
            UnaryFunc::CastStringToInt64 => Ok(cast_string_to_int64(a)),
            UnaryFunc::CastStringToFloat32 => Ok(cast_string_to_float32(a)),
            UnaryFunc::CastStringToFloat64 => Ok(cast_string_to_float64(a)),
            UnaryFunc::CastStringToDecimal(scale) => Ok(cast_string_to_decimal(a, *scale)),
            UnaryFunc::CastStringToDate => Ok(cast_string_to_date(a)),
            UnaryFunc::CastStringToTime => Ok(cast_string_to_time(a)),
            UnaryFunc::CastStringToTimestamp => Ok(cast_string_to_timestamp(a)),
            UnaryFunc::CastStringToTimestampTz => Ok(cast_string_to_timestamptz(a)),
            UnaryFunc::CastStringToInterval => Ok(cast_string_to_interval(a)),
            UnaryFunc::CastDateToTimestamp => Ok(cast_date_to_timestamp(a)),
            UnaryFunc::CastDateToTimestampTz => Ok(cast_date_to_timestamptz(a)),
            UnaryFunc::CastDateToString => Ok(cast_date_to_string(a, temp_storage)),
            UnaryFunc::CastDecimalToString(scale) => {
                Ok(cast_decimal_to_string(a, *scale, temp_storage))
            }
            UnaryFunc::CastTimestampToDate => Ok(cast_timestamp_to_date(a)),
            UnaryFunc::CastTimestampToTimestampTz => Ok(cast_timestamp_to_timestamptz(a)),
            UnaryFunc::CastTimestampToString => Ok(cast_timestamp_to_string(a, temp_storage)),
            UnaryFunc::CastTimestampTzToDate => Ok(cast_timestamptz_to_date(a)),
            UnaryFunc::CastTimestampTzToTimestamp => Ok(cast_timestamptz_to_timestamp(a)),
            UnaryFunc::CastTimestampTzToString => Ok(cast_timestamptz_to_string(a, temp_storage)),
            UnaryFunc::CastIntervalToString => Ok(cast_interval_to_string(a, temp_storage)),
            UnaryFunc::CastBytesToString => Ok(cast_bytes_to_string(a, temp_storage)),
            UnaryFunc::CastStringToJsonb => Ok(cast_string_to_jsonb(a, temp_storage)),
            UnaryFunc::JsonbStringify => Ok(jsonb_stringify(a, temp_storage)),
            UnaryFunc::JsonbStringifyUnlessString => {
                Ok(jsonb_stringify_unless_string(a, temp_storage))
            }
            UnaryFunc::CastJsonbOrNullToJsonb => Ok(cast_jsonb_or_null_to_jsonb(a)),
            UnaryFunc::CastJsonbToString => Ok(cast_jsonb_to_string(a)),
            UnaryFunc::CastJsonbToFloat64 => Ok(cast_jsonb_to_float64(a)),
            UnaryFunc::CastJsonbToBool => Ok(cast_jsonb_to_bool(a)),
            UnaryFunc::CeilFloat32 => Ok(ceil_float32(a)),
            UnaryFunc::CeilFloat64 => Ok(ceil_float64(a)),
            UnaryFunc::CeilDecimal(scale) => Ok(ceil_decimal(a, *scale)),
            UnaryFunc::FloorFloat32 => Ok(floor_float32(a)),
            UnaryFunc::FloorFloat64 => Ok(floor_float64(a)),
            UnaryFunc::FloorDecimal(scale) => Ok(floor_decimal(a, *scale)),
            UnaryFunc::SqrtFloat32 => Ok(sqrt_float32(a)),
            UnaryFunc::SqrtFloat64 => Ok(sqrt_float64(a)),
            UnaryFunc::Ascii => Ok(ascii(a)),
            UnaryFunc::LengthBytes => Ok(length_bytes(a)),
            UnaryFunc::MatchRegex(regex) => Ok(match_regex(a, &regex)),
            UnaryFunc::ExtractIntervalYear => Ok(extract_interval_year(a)),
            UnaryFunc::ExtractIntervalMonth => Ok(extract_interval_month(a)),
            UnaryFunc::ExtractIntervalDay => Ok(extract_interval_day(a)),
            UnaryFunc::ExtractIntervalHour => Ok(extract_interval_hour(a)),
            UnaryFunc::ExtractIntervalMinute => Ok(extract_interval_minute(a)),
            UnaryFunc::ExtractIntervalSecond => Ok(extract_interval_second(a)),
            UnaryFunc::ExtractTimestampYear | UnaryFunc::ExtractTimestampTzYear => {
                Ok(extract_timelike_year(a))
            }
            UnaryFunc::ExtractTimestampQuarter | UnaryFunc::ExtractTimestampTzQuarter => {
                Ok(extract_timelike_quarter(a))
            }
            UnaryFunc::ExtractTimestampMonth | UnaryFunc::ExtractTimestampTzMonth => {
                Ok(extract_timelike_month(a))
            }
            UnaryFunc::ExtractTimestampDay | UnaryFunc::ExtractTimestampTzDay => {
                Ok(extract_timelike_day(a))
            }
            UnaryFunc::ExtractTimestampHour | UnaryFunc::ExtractTimestampTzHour => {
                Ok(extract_timelike_hour(a))
            }
            UnaryFunc::ExtractTimestampMinute | UnaryFunc::ExtractTimestampTzMinute => {
                Ok(extract_timelike_minute(a))
            }
            UnaryFunc::ExtractTimestampSecond | UnaryFunc::ExtractTimestampTzSecond => {
                Ok(extract_timelike_second(a))
            }
            UnaryFunc::ExtractTimestampWeek | UnaryFunc::ExtractTimestampTzWeek => {
                Ok(extract_timelike_week(a))
            }
            UnaryFunc::ExtractTimestampDayOfYear | UnaryFunc::ExtractTimestampTzDayOfYear => {
                Ok(extract_timelike_dayofyear(a))
            }
            UnaryFunc::ExtractTimestampDayOfWeek | UnaryFunc::ExtractTimestampTzDayOfWeek => {
                Ok(extract_timelike_dayofweek(a))
            }
            UnaryFunc::ExtractTimestampIsoDayOfWeek | UnaryFunc::ExtractTimestampTzIsoDayOfWeek => {
                Ok(extract_timelike_isodayofweek(a))
            }
            UnaryFunc::DateTrunc(to) => Ok(match to {
                DateTruncTo::Micros => date_trunc_microseconds(a),
                DateTruncTo::Millis => date_trunc_milliseconds(a),
                DateTruncTo::Second => date_trunc_second(a),
                DateTruncTo::Minute => date_trunc_minute(a),
                DateTruncTo::Hour => date_trunc_hour(a),
                DateTruncTo::Day => date_trunc_day(a),
                DateTruncTo::Week => date_trunc_week(a),
                DateTruncTo::Month => date_trunc_month(a),
                DateTruncTo::Quarter => date_trunc_quarter(a),
                DateTruncTo::Year => date_trunc_year(a),
                DateTruncTo::Decade => date_trunc_decade(a),
                DateTruncTo::Century => date_trunc_century(a),
                DateTruncTo::Millennium => date_trunc_millennium(a),
            }),
            UnaryFunc::ToTimestamp => Ok(to_timestamp(a)),
            UnaryFunc::JsonbArrayLength => Ok(jsonb_array_length(a)),
            UnaryFunc::JsonbTypeof => Ok(jsonb_typeof(a)),
            UnaryFunc::JsonbStripNulls => Ok(jsonb_strip_nulls(a, temp_storage)),
            UnaryFunc::JsonbPretty => Ok(jsonb_pretty(a, temp_storage)),
            UnaryFunc::RoundFloat32 => Ok(round_float32(a)),
            UnaryFunc::RoundFloat64 => Ok(round_float64(a)),
        }
    }

    pub fn output_type(&self, input_type: ColumnType) -> ColumnType {
        use UnaryFunc::*;
        let in_nullable = input_type.nullable;
        match self {
            IsNull | CastInt32ToBool | CastInt64ToBool => ColumnType::new(ScalarType::Bool),

            Ascii | LengthBytes => ColumnType::new(ScalarType::Int32).nullable(in_nullable),

            MatchRegex(_) => ColumnType::new(ScalarType::Bool).nullable(in_nullable),

            CastStringToBool => ColumnType::new(ScalarType::Bool).nullable(true),
            CastStringToBytes => ColumnType::new(ScalarType::Bytes).nullable(true),
            CastStringToInt32 => ColumnType::new(ScalarType::Int32).nullable(true),
            CastStringToInt64 => ColumnType::new(ScalarType::Int64).nullable(true),
            CastStringToFloat32 => ColumnType::new(ScalarType::Float32).nullable(true),
            CastStringToFloat64 => ColumnType::new(ScalarType::Float64).nullable(true),
            CastStringToDecimal(scale) => {
                ColumnType::new(ScalarType::Decimal(MAX_DECIMAL_PRECISION, *scale)).nullable(true)
            }
            CastStringToDate => ColumnType::new(ScalarType::Date).nullable(true),
            CastStringToTime => ColumnType::new(ScalarType::Time).nullable(true),
            CastStringToTimestamp => ColumnType::new(ScalarType::Timestamp).nullable(true),
            CastStringToTimestampTz => ColumnType::new(ScalarType::TimestampTz).nullable(true),
            CastStringToInterval => ColumnType::new(ScalarType::Interval).nullable(true),

            CastBoolToStringExplicit
            | CastBoolToStringImplicit
            | CastInt32ToString
            | CastInt64ToString
            | CastFloat32ToString
            | CastFloat64ToString
            | CastDecimalToString(_)
            | CastDateToString
            | CastTimestampToString
            | CastTimestampTzToString
            | CastIntervalToString
            | CastBytesToString => ColumnType::new(ScalarType::String).nullable(in_nullable),

            CastInt32ToFloat32 | CastInt64ToFloat32 | CastSignificandToFloat32 => {
                ColumnType::new(ScalarType::Float32).nullable(in_nullable)
            }

            CastInt32ToFloat64
            | CastInt64ToFloat64
            | CastFloat32ToFloat64
            | CastSignificandToFloat64 => {
                ColumnType::new(ScalarType::Float64).nullable(in_nullable)
            }

            CastInt64ToInt32 | CastDecimalToInt32 => {
                ColumnType::new(ScalarType::Int32).nullable(in_nullable)
            }

            CastFloat64ToInt32 => ColumnType::new(ScalarType::Int32).nullable(true),

            CastInt32ToInt64 | CastDecimalToInt64 | CastFloat32ToInt64 | CastFloat64ToInt64 => {
                ColumnType::new(ScalarType::Int64).nullable(in_nullable)
            }

            CastInt32ToDecimal => ColumnType::new(ScalarType::Decimal(10, 0)).nullable(in_nullable),
            CastInt64ToDecimal => ColumnType::new(ScalarType::Decimal(20, 0)).nullable(in_nullable),

            CastTimestampToDate | CastTimestampTzToDate => {
                ColumnType::new(ScalarType::Date).nullable(in_nullable)
            }

            CastDateToTimestamp | CastTimestampTzToTimestamp => {
                ColumnType::new(ScalarType::Timestamp).nullable(in_nullable)
            }

            CastDateToTimestampTz | CastTimestampToTimestampTz => {
                ColumnType::new(ScalarType::TimestampTz).nullable(in_nullable)
            }

            // can return null for invalid json
            CastStringToJsonb => ColumnType::new(ScalarType::Jsonb).nullable(true),

            JsonbStringify => ColumnType::new(ScalarType::String).nullable(in_nullable),

            // converts jsonnull to null
            JsonbStringifyUnlessString => ColumnType::new(ScalarType::String).nullable(true),

            // converts null to jsonnull
            CastJsonbOrNullToJsonb => ColumnType::new(ScalarType::Jsonb).nullable(false),

            // can return null for other jsonb types
            CastJsonbToString => ColumnType::new(ScalarType::String).nullable(true),
            CastJsonbToFloat64 => ColumnType::new(ScalarType::Float64).nullable(true),
            CastJsonbToBool => ColumnType::new(ScalarType::Bool).nullable(true),

            CeilFloat32 | FloorFloat32 | RoundFloat32 => {
                ColumnType::new(ScalarType::Float32).nullable(in_nullable)
            }
            CeilFloat64 | FloorFloat64 | RoundFloat64 => {
                ColumnType::new(ScalarType::Float64).nullable(in_nullable)
            }
            CeilDecimal(scale) | FloorDecimal(scale) => {
                match input_type.scalar_type {
                    ScalarType::Decimal(_, s) => assert_eq!(*scale, s),
                    _ => unreachable!(),
                }
                ColumnType::new(input_type.scalar_type).nullable(in_nullable)
            }

            SqrtFloat32 => ColumnType::new(ScalarType::Float32).nullable(true),
            SqrtFloat64 => ColumnType::new(ScalarType::Float64).nullable(true),

            Not | NegInt32 | NegInt64 | NegFloat32 | NegFloat64 | NegDecimal | NegInterval
            | AbsInt32 | AbsInt64 | AbsFloat32 | AbsFloat64 => input_type,

            ExtractIntervalYear
            | ExtractIntervalMonth
            | ExtractIntervalDay
            | ExtractIntervalHour
            | ExtractIntervalMinute
            | ExtractIntervalSecond
            | ExtractTimestampYear
            | ExtractTimestampQuarter
            | ExtractTimestampMonth
            | ExtractTimestampDay
            | ExtractTimestampHour
            | ExtractTimestampMinute
            | ExtractTimestampSecond
            | ExtractTimestampWeek
            | ExtractTimestampDayOfYear
            | ExtractTimestampDayOfWeek
            | ExtractTimestampIsoDayOfWeek
            | ExtractTimestampTzYear
            | ExtractTimestampTzQuarter
            | ExtractTimestampTzMonth
            | ExtractTimestampTzDay
            | ExtractTimestampTzHour
            | ExtractTimestampTzMinute
            | ExtractTimestampTzSecond
            | ExtractTimestampTzWeek
            | ExtractTimestampTzDayOfYear
            | ExtractTimestampTzDayOfWeek
            | ExtractTimestampTzIsoDayOfWeek => {
                ColumnType::new(ScalarType::Float64).nullable(in_nullable)
            }

            DateTrunc(_) => ColumnType::new(ScalarType::Timestamp).nullable(false),

            ToTimestamp => ColumnType::new(ScalarType::TimestampTz).nullable(true),

            JsonbArrayLength => ColumnType::new(ScalarType::Int64).nullable(true),
            JsonbTypeof => ColumnType::new(ScalarType::String).nullable(in_nullable),
            JsonbStripNulls => ColumnType::new(ScalarType::Jsonb).nullable(true),
            JsonbPretty => ColumnType::new(ScalarType::String).nullable(in_nullable),
        }
    }

    /// Whether the function output is NULL if any of its inputs are NULL.
    pub fn propagates_nulls(&self) -> bool {
        match self {
            UnaryFunc::IsNull | UnaryFunc::CastJsonbOrNullToJsonb => false,
            _ => true,
        }
    }

    /// True iff for x != y, we are assured f(x) != f(y).
    ///
    /// This is most often the case for methods that promote to types that
    /// can contain all the precision of the input type.
    pub fn preserves_uniqueness(&self) -> bool {
        match self {
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
            | UnaryFunc::CastDateToString => true,
            _ => false,
        }
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
            UnaryFunc::AbsFloat32 => f.write_str("abs"),
            UnaryFunc::AbsFloat64 => f.write_str("abs"),
            UnaryFunc::CastBoolToStringExplicit => f.write_str("booltostrex"),
            UnaryFunc::CastBoolToStringImplicit => f.write_str("booltostrim"),
            UnaryFunc::CastInt32ToBool => f.write_str("i32tobool"),
            UnaryFunc::CastInt32ToFloat32 => f.write_str("i32tof32"),
            UnaryFunc::CastInt32ToFloat64 => f.write_str("i32tof64"),
            UnaryFunc::CastInt32ToInt64 => f.write_str("i32toi64"),
            UnaryFunc::CastInt32ToString => f.write_str("i32tostr"),
            UnaryFunc::CastInt32ToDecimal => f.write_str("i32todec"),
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
            UnaryFunc::CastDecimalToInt32 => f.write_str("dectoi32"),
            UnaryFunc::CastDecimalToInt64 => f.write_str("dectoi64"),
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
            UnaryFunc::CastDateToTimestamp => f.write_str("datetots"),
            UnaryFunc::CastDateToTimestampTz => f.write_str("datetotstz"),
            UnaryFunc::CastDateToString => f.write_str("datetostr"),
            UnaryFunc::CastTimestampToDate => f.write_str("tstodate"),
            UnaryFunc::CastTimestampToTimestampTz => f.write_str("tstotstz"),
            UnaryFunc::CastTimestampToString => f.write_str("tstostr"),
            UnaryFunc::CastTimestampTzToDate => f.write_str("tstodate"),
            UnaryFunc::CastTimestampTzToTimestamp => f.write_str("tstztots"),
            UnaryFunc::CastTimestampTzToString => f.write_str("tstztostr"),
            UnaryFunc::CastIntervalToString => f.write_str("ivtostr"),
            UnaryFunc::CastBytesToString => f.write_str("bytestostr"),
            UnaryFunc::CastStringToJsonb => f.write_str("strtojsonb"),
            UnaryFunc::JsonbStringify => f.write_str("jsonbtostr"),
            UnaryFunc::JsonbStringifyUnlessString => f.write_str("jsonbtostr?"),
            UnaryFunc::CastJsonbOrNullToJsonb => f.write_str("jsonb?tojsonb"),
            UnaryFunc::CastJsonbToString => f.write_str("jsonbtostr"),
            UnaryFunc::CastJsonbToFloat64 => f.write_str("jsonbtof64"),
            UnaryFunc::CastJsonbToBool => f.write_str("jsonbtobool"),
            UnaryFunc::CeilFloat32 => f.write_str("ceilf32"),
            UnaryFunc::CeilFloat64 => f.write_str("ceilf64"),
            UnaryFunc::CeilDecimal(_) => f.write_str("ceildec"),
            UnaryFunc::FloorFloat32 => f.write_str("floorf32"),
            UnaryFunc::FloorFloat64 => f.write_str("floorf64"),
            UnaryFunc::FloorDecimal(_) => f.write_str("floordec"),
            UnaryFunc::SqrtFloat32 => f.write_str("sqrtf32"),
            UnaryFunc::SqrtFloat64 => f.write_str("sqrtf64"),
            UnaryFunc::Ascii => f.write_str("ascii"),
            UnaryFunc::LengthBytes => f.write_str("lengthbytes"),
            UnaryFunc::MatchRegex(regex) => write!(f, "\"{}\" ~", regex.as_str()),
            UnaryFunc::ExtractIntervalYear => f.write_str("ivextractyear"),
            UnaryFunc::ExtractIntervalMonth => f.write_str("ivextractmonth"),
            UnaryFunc::ExtractIntervalDay => f.write_str("ivextractday"),
            UnaryFunc::ExtractIntervalHour => f.write_str("ivextracthour"),
            UnaryFunc::ExtractIntervalMinute => f.write_str("ivextractminute"),
            UnaryFunc::ExtractIntervalSecond => f.write_str("ivextractsecond"),
            UnaryFunc::ExtractTimestampYear => f.write_str("tsextractyear"),
            UnaryFunc::ExtractTimestampQuarter => f.write_str("tsextractquarter"),
            UnaryFunc::ExtractTimestampMonth => f.write_str("tsextractmonth"),
            UnaryFunc::ExtractTimestampDay => f.write_str("tsextractday"),
            UnaryFunc::ExtractTimestampHour => f.write_str("tsextracthour"),
            UnaryFunc::ExtractTimestampMinute => f.write_str("tsextractminute"),
            UnaryFunc::ExtractTimestampSecond => f.write_str("tsextractsecond"),
            UnaryFunc::ExtractTimestampWeek => f.write_str("tsextractweek"),
            UnaryFunc::ExtractTimestampDayOfYear => f.write_str("tsextractdayofyear"),
            UnaryFunc::ExtractTimestampDayOfWeek => f.write_str("tsextractdayofweek"),
            UnaryFunc::ExtractTimestampIsoDayOfWeek => f.write_str("tsextractisodayofweek"),
            UnaryFunc::ExtractTimestampTzYear => f.write_str("tstzextractyear"),
            UnaryFunc::ExtractTimestampTzQuarter => f.write_str("tsextracttzquarter"),
            UnaryFunc::ExtractTimestampTzMonth => f.write_str("tstzextractmonth"),
            UnaryFunc::ExtractTimestampTzDay => f.write_str("tstzextractday"),
            UnaryFunc::ExtractTimestampTzHour => f.write_str("tstzextracthour"),
            UnaryFunc::ExtractTimestampTzMinute => f.write_str("tstzextractminute"),
            UnaryFunc::ExtractTimestampTzSecond => f.write_str("tstzextractsecond"),
            UnaryFunc::ExtractTimestampTzWeek => f.write_str("tstzextractweek"),
            UnaryFunc::ExtractTimestampTzDayOfYear => f.write_str("tstzextractdayofyear"),
            UnaryFunc::ExtractTimestampTzDayOfWeek => f.write_str("tstzextractdayofweek"),
            UnaryFunc::ExtractTimestampTzIsoDayOfWeek => f.write_str("tstzextractisodayofweek"),
            UnaryFunc::DateTrunc(to) => {
                f.write_str("date_trunc_")?;
                f.write_str(&format!("{:?}", to).to_lowercase())
            }
            UnaryFunc::ToTimestamp => f.write_str("tots"),
            UnaryFunc::JsonbArrayLength => f.write_str("jsonb_array_length"),
            UnaryFunc::JsonbTypeof => f.write_str("jsonb_typeof"),
            UnaryFunc::JsonbStripNulls => f.write_str("jsonb_strip_nulls"),
            UnaryFunc::JsonbPretty => f.write_str("jsonb_pretty"),
            UnaryFunc::RoundFloat32 => f.write_str("roundf32"),
            UnaryFunc::RoundFloat64 => f.write_str("roundf64"),
        }
    }
}

fn coalesce<'a>(
    datums: &[Datum<'a>],
    env: &'a EvalEnv,
    temp_storage: &'a RowArena,
    exprs: &'a [ScalarExpr],
) -> Result<Datum<'a>, EvalError> {
    for e in exprs {
        let d = e.eval(datums, env, temp_storage)?;
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

fn length_string<'a>(datums: &[Datum<'a>]) -> Result<Datum<'a>, EvalError> {
    let string = datums[0].unwrap_str();

    if datums.len() == 2 {
        // Convert PostgreSQL-style encoding names[1] to WHATWG-style encoding names[2],
        // which the encoding library uses[3].
        // [1]: https://www.postgresql.org/docs/9.5/multibyte.html
        // [2]: https://encoding.spec.whatwg.org/
        // [3]: https://github.com/lifthrasiir/rust-encoding/blob/4e79c35ab6a351881a86dbff565c4db0085cc113/src/label.rs
        let encoding_name = datums[1].unwrap_str().to_lowercase().replace("_", "-");

        let enc = match encoding_from_whatwg_label(&encoding_name) {
            Some(enc) => enc,
            None => return Err(EvalError::InvalidEncodingName(encoding_name)),
        };

        let decoded_string = match enc.decode(string.as_bytes(), DecoderTrap::Strict) {
            Ok(s) => s,
            Err(e) => {
                return Err(EvalError::InvalidByteSequence {
                    byte_sequence: e.to_string(),
                    encoding_name,
                })
            }
        };

        Ok(Datum::from(decoded_string.chars().count() as i32))
    } else {
        Ok(Datum::from(string.chars().count() as i32))
    }
}

fn length_bytes<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::Int32(a.unwrap_bytes().len() as i32)
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

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum VariadicFunc {
    Coalesce,
    Concat,
    MakeTimestamp,
    Substr,
    LengthString,
    Replace,
    JsonbBuildArray,
    JsonbBuildObject,
}

impl VariadicFunc {
    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        env: &'a EvalEnv,
        temp_storage: &'a RowArena,
        exprs: &'a [ScalarExpr],
    ) -> Result<Datum<'a>, EvalError> {
        macro_rules! eager {
            ($func:ident $(, $args:expr)*) => {{
                let ds = exprs.iter()
                    .map(|e| e.eval(datums, env, temp_storage))
                    .collect::<Result<Vec<_>, _>>()?;
                if self.propagates_nulls() && ds.iter().any(|d| d.is_null()) {
                    return Ok(Datum::Null);
                }
                $func(&ds $(, $args)*)
            }}
        }

        match self {
            VariadicFunc::Coalesce => coalesce(datums, env, temp_storage, exprs),
            VariadicFunc::Concat => Ok(eager!(text_concat_variadic, temp_storage)),
            VariadicFunc::MakeTimestamp => Ok(eager!(make_timestamp)),
            VariadicFunc::Substr => Ok(eager!(substr)),
            VariadicFunc::LengthString => eager!(length_string),
            VariadicFunc::Replace => Ok(eager!(replace, temp_storage)),
            VariadicFunc::JsonbBuildArray => Ok(eager!(jsonb_build_array, temp_storage)),
            VariadicFunc::JsonbBuildObject => Ok(eager!(jsonb_build_object, temp_storage)),
        }
    }

    pub fn output_type(&self, input_types: Vec<ColumnType>) -> ColumnType {
        use VariadicFunc::*;
        match self {
            Coalesce => {
                debug_assert!(
                    input_types
                        .windows(2)
                        .all(|w| w[0].scalar_type == w[1].scalar_type),
                    "coalesce inputs did not have uniform type: {:?}",
                    input_types
                );
                if input_types.is_empty() {
                    ColumnType::new(ScalarType::Unknown)
                } else {
                    input_types.into_first().nullable(true)
                }
            }
            Concat => ColumnType::new(ScalarType::String).nullable(true),
            MakeTimestamp => ColumnType::new(ScalarType::Timestamp).nullable(true),
            Substr => ColumnType::new(ScalarType::String).nullable(true),
            LengthString => ColumnType::new(ScalarType::Int32).nullable(true),
            Replace => ColumnType::new(ScalarType::String).nullable(true),
            JsonbBuildArray | JsonbBuildObject => ColumnType::new(ScalarType::Jsonb).nullable(true),
        }
    }

    /// Whether the function output is NULL if any of its inputs are NULL.
    pub fn propagates_nulls(&self) -> bool {
        match self {
            VariadicFunc::Coalesce | VariadicFunc::Concat => false,
            VariadicFunc::JsonbBuildArray | VariadicFunc::JsonbBuildObject => false,
            _ => true,
        }
    }
}

impl fmt::Display for VariadicFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VariadicFunc::Coalesce => f.write_str("coalesce"),
            VariadicFunc::Concat => f.write_str("concat"),
            VariadicFunc::MakeTimestamp => f.write_str("makets"),
            VariadicFunc::Substr => f.write_str("substr"),
            VariadicFunc::LengthString => f.write_str("lengthstr"),
            VariadicFunc::Replace => f.write_str("replace"),
            VariadicFunc::JsonbBuildArray => f.write_str("jsonb_build_array"),
            VariadicFunc::JsonbBuildObject => f.write_str("jsonb_build_object"),
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
