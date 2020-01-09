// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::cmp;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::fmt::Write;
use std::str::FromStr;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use encoding::label::encoding_from_whatwg_label;
use encoding::DecoderTrap;
use failure::bail;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use repr::decimal::MAX_DECIMAL_PRECISION;
use repr::regex::Regex;
use repr::{ColumnType, Datum, Interval, RowArena, RowPacker, ScalarType};

use self::format::DateTimeFormat;
pub use crate::like::build_like_regex_from_string;
use crate::EvalEnv;

mod format;

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum NullaryFunc {
    MzLogicalTimestamp,
    Now,
}

impl NullaryFunc {
    pub fn eval<'a>(&'a self, env: &'a EvalEnv, _temp_storage: &'a RowArena) -> Datum<'a> {
        match self {
            NullaryFunc::MzLogicalTimestamp => mz_logical_time(env),
            NullaryFunc::Now => now(env),
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

pub fn and<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    match (a, b) {
        (Datum::False, _) => Datum::False,
        (_, Datum::False) => Datum::False,
        (Datum::Null, _) => Datum::Null,
        (_, Datum::Null) => Datum::Null,
        (Datum::True, Datum::True) => Datum::True,
        _ => unreachable!(),
    }
}

pub fn or<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    match (a, b) {
        (Datum::True, _) => Datum::True,
        (_, Datum::True) => Datum::True,
        (Datum::Null, _) => Datum::Null,
        (_, Datum::Null) => Datum::Null,
        (Datum::False, Datum::False) => Datum::False,
        _ => unreachable!(),
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

fn cast_bool_to_string<'a>(a: Datum<'a>) -> Datum<'a> {
    match a.unwrap_bool() {
        true => Datum::from("true"),
        false => Datum::from("false"),
    }
}

fn cast_int32_to_bool<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int32() != 0)
}

fn cast_int32_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    Datum::String(temp_storage.push_string(a.unwrap_int32().to_string()))
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

fn cast_int64_to_int32<'a>(a: Datum<'a>) -> Datum<'a> {
    // TODO(benesch): we need to do something better than panicking if the
    // datum doesn't fit in an int32, but what? Poison the whole dataflow?
    // The SQL standard says this an error, but runtime errors are complicated
    // in a streaming setting.
    Datum::from(i32::try_from(a.unwrap_int64()).unwrap())
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
    Datum::String(temp_storage.push_string(a.unwrap_int64().to_string()))
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

fn cast_float32_to_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let f = a.unwrap_float32();
    let scale = b.unwrap_int32();

    // If errors were returnable, this would be:
    // "ERROR:  numeric field overflow
    // DETAIL:  A field with precision {},
    //   scale {} must round to an absolute value less than 10^{}.",
    //  MAX_DECIMAL_PRECISION, scale, MAX_DECIMAL_PRECISION - scale
    if f > 10_f32.powi(MAX_DECIMAL_PRECISION as i32 - scale) {
        return Datum::Null;
    }

    Datum::from((f * 10_f32.powi(scale)) as i128)
}

fn cast_float32_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    Datum::String(temp_storage.push_string(a.unwrap_float32().to_string()))
}

fn cast_float64_to_int64<'a>(a: Datum<'a>) -> Datum<'a> {
    // TODO(benesch): this is undefined behavior if the f32 doesn't fit in an
    // i64 (https://github.com/rust-lang/rust/issues/10184).
    Datum::from(a.unwrap_float64() as i64)
}

fn cast_float64_to_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let f = a.unwrap_float64();
    let scale = b.unwrap_int32();

    // If errors were returnable, this would be:
    // "ERROR:  numeric field overflow
    // DETAIL:  A field with precision {},
    //   scale {} must round to an absolute value less than 10^{}.",
    //  MAX_DECIMAL_PRECISION, scale, MAX_DECIMAL_PRECISION - scale
    if f > 10_f64.powi(MAX_DECIMAL_PRECISION as i32 - scale) {
        return Datum::Null;
    }

    Datum::from((f * 10_f64.powi(scale)) as i128)
}

fn cast_float64_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    Datum::String(temp_storage.push_string(a.unwrap_float64().to_string()))
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
    Datum::String(temp_storage.push_string(a.unwrap_decimal().with_scale(scale).to_string()))
}

fn cast_string_to_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    let val: Result<f64, _> = a.unwrap_str().to_lowercase().parse();
    Datum::from(val.ok())
}

fn cast_string_to_bytes<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    Datum::Bytes(&temp_storage.push_bytes(a.unwrap_str().as_bytes().to_vec()))
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
    Datum::String(temp_storage.push_string(a.unwrap_date().to_string()))
}

fn cast_timestamp_to_date<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::Date(a.unwrap_timestamp().date())
}

fn cast_timestamp_to_timestamptz<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::TimestampTz(DateTime::<Utc>::from_utc(a.unwrap_timestamp(), Utc))
}

fn cast_timestamp_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    Datum::String(temp_storage.push_string(a.unwrap_timestamp().to_string()))
}

fn cast_timestamptz_to_date<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::Date(a.unwrap_timestamptz().naive_utc().date())
}

fn cast_timestamptz_to_timestamp<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::Timestamp(a.unwrap_timestamptz().naive_utc())
}

fn cast_timestamptz_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    Datum::String(temp_storage.push_string(a.unwrap_timestamptz().to_string()))
}

fn cast_interval_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    Datum::String(temp_storage.push_string(a.unwrap_interval().to_string()))
}

fn cast_bytes_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let bytes = a.unwrap_bytes();
    let mut out = String::from("\\x");
    out.reserve(bytes.len() * 2);
    for byte in bytes {
        write!(&mut out, "{:x}", byte).expect("writing to string cannot fail");
    }
    Datum::String(temp_storage.push_string(out))
}

pub fn serde_into_row(
    mut packer: RowPacker,
    serde: serde_json::Value,
) -> Result<RowPacker, failure::Error> {
    use serde_json::Value;
    match serde {
        Value::Null => packer.push(Datum::JsonNull),
        Value::Bool(b) => {
            if b {
                packer.push(Datum::True)
            } else {
                packer.push(Datum::False)
            }
        }
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                packer.push(Datum::Float64(OrderedFloat(f)))
            } else {
                bail!("{} is out of range for json number", n)
            }
        }
        Value::String(s) => packer.push(Datum::String(&s)),
        Value::Array(array) => {
            let start = unsafe { packer.start_list() };
            for elem in array {
                // if we bail here the packer might be in an invalid state, but we throw the packer away so it's safe
                packer = serde_into_row(packer, elem)?;
            }
            unsafe { packer.finish_list(start) };
        }
        Value::Object(object) => {
            let start = unsafe { packer.start_dict() };
            let mut pairs = object.into_iter().collect::<Vec<_>>();
            // dict keys must be in ascending order
            pairs.sort_by(|(k1, _), (k2, _)| k1.cmp(&k2));
            for (key, val) in pairs {
                packer.push(Datum::String(&key));
                // if we bail here the packer might be in an invalid state, but we throw the packer away so it's safe
                packer = serde_into_row(packer, val)?;
            }
            unsafe { packer.finish_dict(start) };
        }
    }
    Ok(packer)
}

#[allow(clippy::float_cmp)]
pub fn datum_to_serde(datum: Datum) -> serde_json::Value {
    use serde_json::Value;
    match datum {
        Datum::JsonNull => Value::Null,
        Datum::True => Value::Bool(true),
        Datum::False => Value::Bool(false),
        Datum::Float64(f) => {
            let f: f64 = f.into();
            if let Some(n) = serde_json::Number::from_f64(f) {
                Value::Number(n)
            } else {
                // This should only be reachable for NaN/Infinity, which aren't allowed to be cast to Jsonb
                panic!("Not a valid json number: {}", f)
            }
        }
        Datum::String(s) => Value::String(s.to_owned()),
        Datum::List(list) => Value::Array(list.iter().map(|e| datum_to_serde(e)).collect()),
        Datum::Dict(dict) => Value::Object(
            dict.iter()
                .map(|(k, v)| (k.to_owned(), datum_to_serde(v)))
                .collect(),
        ),
        _ => panic!("Not a json-compatible datum: {:?}", datum),
    }
}

// TODO(jamii) it would be much more efficient to skip the intermediate serde_json::Value
fn cast_string_to_jsonb<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    match serde_json::from_str(a.unwrap_str()) {
        Err(_) => Datum::Null,
        Ok(json) => match serde_into_row(RowPacker::new(), json) {
            Err(_) => Datum::Null,
            Ok(packer) => temp_storage.push_row(packer.finish()).unpack_first(),
        },
    }
}

// TODO(jamii) it would be much more efficient to skip the intermediate serde_json::Value
fn cast_jsonb_to_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    Datum::String(temp_storage.push_string(datum_to_serde(a).to_string()))
}

fn cast_jsonb_to_string_unless_string<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
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
            } else {
                // invalid json float
                Datum::Null
            }
        }
        _ => a,
    }
}

fn cast_jsonb_to_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    match a {
        Datum::Float64(_) => a,
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
        Datum::Interval(Interval::Months(months)) => add_timestamp_months(dt, months),
        Datum::Interval(Interval::Duration {
            is_positive,
            duration,
        }) => add_timestamp_duration(dt, is_positive, duration),
        _ => panic!("Tried to do timestamp addition with non-interval: {:?}", b),
    })
}

fn add_timestamptz_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let dt = a.unwrap_timestamptz().naive_utc();

    let new_ndt = match b {
        Datum::Interval(Interval::Months(months)) => add_timestamp_months(dt, months),
        Datum::Interval(Interval::Duration {
            is_positive,
            duration,
        }) => add_timestamp_duration(dt, is_positive, duration),
        _ => panic!("Tried to do timestamp addition with non-interval: {:?}", b),
    };

    Datum::TimestampTz(DateTime::<Utc>::from_utc(new_ndt, Utc))
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

fn sub_timestamp_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let inverse = match b {
        Datum::Interval(Interval::Months(months)) => Datum::Interval(Interval::Months(-months)),
        Datum::Interval(Interval::Duration {
            is_positive,
            duration,
        }) => Datum::Interval(Interval::Duration {
            is_positive: !is_positive,
            duration,
        }),
        _ => panic!(
            "Tried to do timestamptz subtraction with non-interval: {:?}",
            b
        ),
    };
    add_timestamp_interval(a, inverse)
}

fn sub_timestamptz_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let inverse = match b {
        Datum::Interval(Interval::Months(months)) => Datum::Interval(Interval::Months(-months)),
        Datum::Interval(Interval::Duration {
            is_positive,
            duration,
        }) => Datum::Interval(Interval::Duration {
            is_positive: !is_positive,
            duration,
        }),
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

// TODO(jamii) we don't currently have any way of reporting errors from functions, so for now we just adopt sqlite's approach 1/0 = null

fn div_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let b = b.unwrap_int32();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_int32() / b)
    }
}

fn div_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let b = b.unwrap_int64();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_int64() / b)
    }
}

fn div_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let b = b.unwrap_float32();
    if b == 0.0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_float32() / b)
    }
}

fn div_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let b = b.unwrap_float64();
    if b == 0.0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_float64() / b)
    }
}

fn div_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let b = b.unwrap_decimal();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_decimal() / b)
    }
}

fn mod_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let b = b.unwrap_int32();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_int32() % b)
    }
}

fn mod_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let b = b.unwrap_int64();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_int64() % b)
    }
}

fn mod_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let b = b.unwrap_float32();
    if b == 0.0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_float32() % b)
    }
}

fn mod_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let b = b.unwrap_float64();
    if b == 0.0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_float64() % b)
    }
}

fn mod_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let b = b.unwrap_decimal();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_decimal() % b)
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

fn match_regex<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let haystack = a.unwrap_str();
    match build_like_regex_from_string(b.unwrap_str()) {
        Ok(needle) => Datum::from(needle.is_match(haystack)),
        Err(_) => {
            // TODO(benesch): this should cause a runtime error, but we don't
            // support those yet, so just return NULL for now.
            Datum::Null
        }
    }
}

fn match_cached_regex<'a>(a: Datum<'a>, needle: &regex::Regex) -> Datum<'a> {
    let haystack = a.unwrap_str();
    Datum::from(needle.is_match(haystack))
}

fn ascii<'a>(a: Datum<'a>) -> Datum<'a> {
    match a.unwrap_str().chars().next() {
        None => Datum::Int32(0),
        Some(v) => Datum::Int32(v as i32),
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

fn extract_timestamp_year<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamp().year()))
}

fn extract_timestamptz_year<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamptz().year()))
}

fn extract_timestamp_quarter<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from((f64::from(a.unwrap_timestamp().month()) / 3.0).ceil())
}

fn extract_timestamptz_quarter<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from((f64::from(a.unwrap_timestamptz().month()) / 3.0).ceil())
}

fn extract_timestamp_month<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamp().month()))
}

fn extract_timestamptz_month<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamptz().month()))
}

fn extract_timestamp_day<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamp().day()))
}

fn extract_timestamptz_day<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamptz().day()))
}

fn extract_timestamp_hour<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamp().hour()))
}

fn extract_timestamptz_hour<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamptz().hour()))
}

fn extract_timestamp_minute<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamp().minute()))
}

fn extract_timestamptz_minute<'a>(a: Datum<'a>) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamptz().minute()))
}

fn extract_timestamp_second<'a>(a: Datum<'a>) -> Datum<'a> {
    let a = a.unwrap_timestamp();
    let s = f64::from(a.second());
    let ns = f64::from(a.nanosecond()) / 1e9;
    Datum::from(s + ns)
}

fn extract_timestamptz_second<'a>(a: Datum<'a>) -> Datum<'a> {
    let a = a.unwrap_timestamptz();
    let s = f64::from(a.second());
    let ns = f64::from(a.nanosecond()) / 1e9;
    Datum::from(s + ns)
}

/// Extract the iso week of the year
///
/// Note that because isoweeks are defined in terms of January 4th, Jan 1 is only in week
/// 1 about half of the time
fn extract_timestamp_week<'a>(a: Datum<'a>) -> Datum<'a> {
    let a = a.unwrap_timestamp();
    Datum::from(f64::from(a.iso_week().week()))
}

/// Extract the iso week of the year
///
/// Note that because isoweeks are defined in terms of January 4th, Jan 1 is only in week
/// 1 about half of the time
fn extract_timestamptz_week<'a>(a: Datum<'a>) -> Datum<'a> {
    let a = a.unwrap_timestamptz();
    Datum::from(f64::from(a.iso_week().week()))
}

fn extract_timestamp_dayofyear<'a>(a: Datum<'a>) -> Datum<'a> {
    let a = a.unwrap_timestamp();
    Datum::from(f64::from(a.ordinal()))
}

fn extract_timestamptz_dayofyear<'a>(a: Datum<'a>) -> Datum<'a> {
    let a = a.unwrap_timestamptz();
    Datum::from(f64::from(a.ordinal()))
}

/// extract day of week with monday = 1 sunday = 0
fn extract_timestamp_dayofweek<'a>(a: Datum<'a>) -> Datum<'a> {
    let a = a.unwrap_timestamp();
    Datum::from(a.weekday().num_days_from_sunday() as f64)
}

/// extract day of week with monday = 1 sunday = 0
fn extract_timestamptz_dayofweek<'a>(a: Datum<'a>) -> Datum<'a> {
    let a = a.unwrap_timestamptz();
    Datum::from(a.weekday().num_days_from_sunday() as f64)
}

/// extract day of week with monday = 1 sunday = 7
fn extract_timestamp_isodayofweek<'a>(a: Datum<'a>) -> Datum<'a> {
    let a = a.unwrap_timestamp();
    Datum::from(a.weekday().number_from_monday() as f64)
}

/// extract day of week with monday = 1 sunday = 7
fn extract_timestamptz_isodayofweek<'a>(a: Datum<'a>) -> Datum<'a> {
    let a = a.unwrap_timestamptz();
    Datum::from(a.weekday().number_from_monday() as f64)
}

fn date_trunc<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    match a.unwrap_str().parse::<DateTruncTo>() {
        Ok(DateTruncTo::Micros) => date_trunc_microseconds(b),
        Ok(DateTruncTo::Millis) => date_trunc_milliseconds(b),
        Ok(DateTruncTo::Second) => date_trunc_second(b),
        Ok(DateTruncTo::Minute) => date_trunc_minute(b),
        Ok(DateTruncTo::Hour) => date_trunc_hour(b),
        Ok(DateTruncTo::Day) => date_trunc_day(b),
        Ok(DateTruncTo::Week) => date_trunc_week(b),
        Ok(DateTruncTo::Month) => date_trunc_month(b),
        Ok(DateTruncTo::Quarter) => date_trunc_quarter(b),
        Ok(DateTruncTo::Year) => date_trunc_year(b),
        Ok(DateTruncTo::Decade) => date_trunc_decade(b),
        Ok(DateTruncTo::Century) => date_trunc_century(b),
        Ok(DateTruncTo::Millennium) => date_trunc_millennium(b),
        // TODO: return an error when we support that.
        _ => Datum::Null,
    }
}

fn date_trunc_microseconds<'a>(a: Datum<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    let time = NaiveTime::from_hms_micro(
        source_timestamp.hour(),
        source_timestamp.minute(),
        source_timestamp.second(),
        source_timestamp.nanosecond() / 1_000,
    );
    Datum::Timestamp(NaiveDateTime::new(source_timestamp.date(), time))
}

fn date_trunc_milliseconds<'a>(a: Datum<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    let time = NaiveTime::from_hms_milli(
        source_timestamp.hour(),
        source_timestamp.minute(),
        source_timestamp.second(),
        source_timestamp.nanosecond() / 1_000_000,
    );
    Datum::Timestamp(NaiveDateTime::new(source_timestamp.date(), time))
}

fn date_trunc_second<'a>(a: Datum<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    Datum::Timestamp(NaiveDateTime::new(
        source_timestamp.date(),
        NaiveTime::from_hms(
            source_timestamp.hour(),
            source_timestamp.minute(),
            source_timestamp.second(),
        ),
    ))
}

fn date_trunc_minute<'a>(a: Datum<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    Datum::Timestamp(NaiveDateTime::new(
        source_timestamp.date(),
        NaiveTime::from_hms(source_timestamp.hour(), source_timestamp.minute(), 0),
    ))
}

fn date_trunc_hour<'a>(a: Datum<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    Datum::Timestamp(NaiveDateTime::new(
        source_timestamp.date(),
        NaiveTime::from_hms(source_timestamp.hour(), 0, 0),
    ))
}

fn date_trunc_day<'a>(a: Datum<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    Datum::Timestamp(NaiveDateTime::new(
        source_timestamp.date(),
        NaiveTime::from_hms(0, 0, 0),
    ))
}

fn date_trunc_week<'a>(a: Datum<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    let num_days_from_monday = source_timestamp.date().weekday().num_days_from_monday();
    Datum::Timestamp(NaiveDateTime::new(
        NaiveDate::from_ymd(
            source_timestamp.year(),
            source_timestamp.month(),
            source_timestamp.day() - num_days_from_monday,
        ),
        NaiveTime::from_hms(0, 0, 0),
    ))
}

fn date_trunc_month<'a>(a: Datum<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    Datum::Timestamp(NaiveDateTime::new(
        NaiveDate::from_ymd(source_timestamp.year(), source_timestamp.month(), 1),
        NaiveTime::from_hms(0, 0, 0),
    ))
}

fn date_trunc_quarter<'a>(a: Datum<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    let month = source_timestamp.month();
    let quarter = if month <= 3 {
        1
    } else if month <= 6 {
        4
    } else if month <= 9 {
        7
    } else {
        10
    };

    Datum::Timestamp(NaiveDateTime::new(
        NaiveDate::from_ymd(source_timestamp.year(), quarter, 1),
        NaiveTime::from_hms(0, 0, 0),
    ))
}

fn date_trunc_year<'a>(a: Datum<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    Datum::Timestamp(NaiveDateTime::new(
        NaiveDate::from_ymd(source_timestamp.year(), 1, 1),
        NaiveTime::from_hms(0, 0, 0),
    ))
}

fn date_trunc_decade<'a>(a: Datum<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    Datum::Timestamp(NaiveDateTime::new(
        NaiveDate::from_ymd(
            source_timestamp.year() - (source_timestamp.year() % 10),
            1,
            1,
        ),
        NaiveTime::from_hms(0, 0, 0),
    ))
}

fn date_trunc_century<'a>(a: Datum<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    // Expects the first year of the century, meaning 2001 instead of 2000.
    let century = source_timestamp.year() - ((source_timestamp.year() % 100) - 1);
    Datum::Timestamp(NaiveDateTime::new(
        NaiveDate::from_ymd(century, 1, 1),
        NaiveTime::from_hms(0, 0, 0),
    ))
}

fn date_trunc_millennium<'a>(a: Datum<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    // Expects the first year of the millennium, meaning 2001 instead of 2000.
    let millennium = source_timestamp.year() - ((source_timestamp.year() % 1000) - 1);
    Datum::Timestamp(NaiveDateTime::new(
        NaiveDate::from_ymd(millennium, 1, 1),
        NaiveTime::from_hms(0, 0, 0),
    ))
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
        Datum::Float64(_) => Datum::String("number"),
        Datum::True | Datum::False => Datum::String("boolean"),
        Datum::JsonNull => Datum::String("null"),
        Datum::Null => Datum::Null,
        _ => panic!("Not jsonb: {:?}", a),
    }
}

fn jsonb_strip_nulls<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    match a {
        Datum::Dict(dict) => temp_storage.make_datum(|packer| {
            packer.push_dict(dict.iter().filter(|(_, v)| match v {
                Datum::JsonNull => false,
                _ => true,
            }))
        }),
        _ => Datum::Null,
    }
}

fn jsonb_pretty<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    Datum::String(temp_storage.push_string(
        // to_string_pretty shouldn't be able to fail on the output of datum_to_serde - see https://docs.serde.rs/serde_json/fn.to_string_pretty.html
        serde_json::to_string_pretty(&datum_to_serde(a)).unwrap(),
    ))
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum BinaryFunc {
    And,
    Or,
    AddInt32,
    AddInt64,
    AddFloat32,
    AddFloat64,
    AddTimestampInterval,
    AddTimestampTzInterval,
    AddDecimal,
    SubInt32,
    SubInt64,
    SubFloat32,
    SubFloat64,
    SubTimestamp,
    SubTimestampTz,
    SubTimestampInterval,
    SubTimestampTzInterval,
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
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    MatchRegex,
    ToCharTimestamp,
    ToCharTimestampTz,
    DateTrunc,
    CastFloat32ToDecimal,
    CastFloat64ToDecimal,
    JsonbGetInt64,
    JsonbGetString,
    JsonbContainsString,
    JsonbConcat,
    JsonbContainsJsonb,
    JsonbDeleteInt64,
    JsonbDeleteString,
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
    type Err = failure::Error;
    fn from_str(s: &str) -> Result<DateTruncTo, Self::Err> {
        let s = unicase::Ascii::new(s);
        Ok(if s == "microseconds" {
            DateTruncTo::Micros
        } else if s == "milliseconds" {
            DateTruncTo::Millis
        } else if s == "second" {
            DateTruncTo::Second
        } else if s == "minute" {
            DateTruncTo::Minute
        } else if s == "hour" {
            DateTruncTo::Hour
        } else if s == "day" {
            DateTruncTo::Day
        } else if s == "week" {
            DateTruncTo::Week
        } else if s == "month" {
            DateTruncTo::Month
        } else if s == "quarter" {
            DateTruncTo::Quarter
        } else if s == "year" {
            DateTruncTo::Year
        } else if s == "decade" {
            DateTruncTo::Decade
        } else if s == "century" {
            DateTruncTo::Century
        } else if s == "millennium" {
            DateTruncTo::Millennium
        } else {
            failure::bail!("invalid date_trunc precision: {}", s.into_inner())
        })
    }
}

impl BinaryFunc {
    pub fn eval<'a>(
        &'a self,
        a: Datum<'a>,
        b: Datum<'a>,
        _env: &'a EvalEnv,
        temp_storage: &'a RowArena,
    ) -> Datum<'a> {
        match self {
            BinaryFunc::And => and(a, b),
            BinaryFunc::Or => or(a, b),
            BinaryFunc::AddInt32 => add_int32(a, b),
            BinaryFunc::AddInt64 => add_int64(a, b),
            BinaryFunc::AddFloat32 => add_float32(a, b),
            BinaryFunc::AddFloat64 => add_float64(a, b),
            BinaryFunc::AddTimestampInterval => add_timestamp_interval(a, b),
            BinaryFunc::AddTimestampTzInterval => add_timestamptz_interval(a, b),
            BinaryFunc::AddDecimal => add_decimal(a, b),
            BinaryFunc::SubInt32 => sub_int32(a, b),
            BinaryFunc::SubInt64 => sub_int64(a, b),
            BinaryFunc::SubFloat32 => sub_float32(a, b),
            BinaryFunc::SubFloat64 => sub_float64(a, b),
            BinaryFunc::SubTimestamp => sub_timestamp(a, b),
            BinaryFunc::SubTimestampTz => sub_timestamptz(a, b),
            BinaryFunc::SubTimestampInterval => sub_timestamp_interval(a, b),
            BinaryFunc::SubTimestampTzInterval => sub_timestamptz_interval(a, b),
            BinaryFunc::SubDecimal => sub_decimal(a, b),
            BinaryFunc::MulInt32 => mul_int32(a, b),
            BinaryFunc::MulInt64 => mul_int64(a, b),
            BinaryFunc::MulFloat32 => mul_float32(a, b),
            BinaryFunc::MulFloat64 => mul_float64(a, b),
            BinaryFunc::MulDecimal => mul_decimal(a, b),
            BinaryFunc::DivInt32 => div_int32(a, b),
            BinaryFunc::DivInt64 => div_int64(a, b),
            BinaryFunc::DivFloat32 => div_float32(a, b),
            BinaryFunc::DivFloat64 => div_float64(a, b),
            BinaryFunc::DivDecimal => div_decimal(a, b),
            BinaryFunc::ModInt32 => mod_int32(a, b),
            BinaryFunc::ModInt64 => mod_int64(a, b),
            BinaryFunc::ModFloat32 => mod_float32(a, b),
            BinaryFunc::ModFloat64 => mod_float64(a, b),
            BinaryFunc::ModDecimal => mod_decimal(a, b),
            BinaryFunc::Eq => eq(a, b),
            BinaryFunc::NotEq => not_eq(a, b),
            BinaryFunc::Lt => lt(a, b),
            BinaryFunc::Lte => lte(a, b),
            BinaryFunc::Gt => gt(a, b),
            BinaryFunc::Gte => gte(a, b),
            BinaryFunc::MatchRegex => match_regex(a, b),
            BinaryFunc::ToCharTimestamp => to_char_timestamp(a, b, temp_storage),
            BinaryFunc::ToCharTimestampTz => to_char_timestamptz(a, b, temp_storage),
            BinaryFunc::DateTrunc => date_trunc(a, b),
            BinaryFunc::CastFloat32ToDecimal => cast_float32_to_decimal(a, b),
            BinaryFunc::CastFloat64ToDecimal => cast_float64_to_decimal(a, b),
            BinaryFunc::JsonbGetInt64 => jsonb_get_int64(a, b),
            BinaryFunc::JsonbGetString => jsonb_get_string(a, b),
            BinaryFunc::JsonbContainsString => jsonb_contains_string(a, b),
            BinaryFunc::JsonbConcat => jsonb_concat(a, b, temp_storage),
            BinaryFunc::JsonbContainsJsonb => jsonb_contains_jsonb(a, b),
            BinaryFunc::JsonbDeleteInt64 => jsonb_delete_int64(a, b, temp_storage),
            BinaryFunc::JsonbDeleteString => jsonb_delete_string(a, b, temp_storage),
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

            MatchRegex => {
                // output can be null if the regex is invalid
                ColumnType::new(ScalarType::Bool).nullable(true)
            }

            ToCharTimestamp | ToCharTimestampTz => {
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

            SubTimestamp | SubTimestampTz => {
                ColumnType::new(ScalarType::Interval).nullable(in_nullable)
            }

            // TODO(benesch): we correctly compute types for decimal scale, but
            // not decimal precision... because nothing actually cares about
            // decimal precision. Should either remove or fix.
            AddDecimal | SubDecimal | ModDecimal => {
                let (s1, s2) = match (&input1_type.scalar_type, &input2_type.scalar_type) {
                    (ScalarType::Null, _) | (_, ScalarType::Null) => {
                        return ColumnType::new(ScalarType::Null);
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
                    (ScalarType::Null, _) | (_, ScalarType::Null) => {
                        return ColumnType::new(ScalarType::Null);
                    }
                    (ScalarType::Decimal(_, s1), ScalarType::Decimal(_, s2)) => (s1, s2),
                    _ => unreachable!(),
                };
                let s = s1 + s2;
                ColumnType::new(ScalarType::Decimal(MAX_DECIMAL_PRECISION, s)).nullable(in_nullable)
            }
            DivDecimal => {
                let (s1, s2) = match (&input1_type.scalar_type, &input2_type.scalar_type) {
                    (ScalarType::Null, _) | (_, ScalarType::Null) => {
                        return ColumnType::new(ScalarType::Null);
                    }
                    (ScalarType::Decimal(_, s1), ScalarType::Decimal(_, s2)) => (s1, s2),
                    _ => unreachable!(),
                };
                let s = s1 - s2;
                ColumnType::new(ScalarType::Decimal(MAX_DECIMAL_PRECISION, s)).nullable(true)
            }

            CastFloat32ToDecimal | CastFloat64ToDecimal => match input2_type.scalar_type {
                ScalarType::Null => ColumnType::new(ScalarType::Null),
                ScalarType::Decimal(_, s) => {
                    ColumnType::new(ScalarType::Decimal(MAX_DECIMAL_PRECISION, s)).nullable(true)
                }
                _ => unreachable!(),
            },

            AddTimestampInterval
            | SubTimestampInterval
            | AddTimestampTzInterval
            | SubTimestampTzInterval => input1_type,

            DateTrunc => ColumnType::new(ScalarType::Timestamp).nullable(true),

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
            BinaryFunc::AddTimestampInterval => f.write_str("+"),
            BinaryFunc::AddTimestampTzInterval => f.write_str("+"),
            BinaryFunc::AddDecimal => f.write_str("+"),
            BinaryFunc::SubInt32 => f.write_str("-"),
            BinaryFunc::SubInt64 => f.write_str("-"),
            BinaryFunc::SubFloat32 => f.write_str("-"),
            BinaryFunc::SubFloat64 => f.write_str("-"),
            BinaryFunc::SubTimestamp => f.write_str("-"),
            BinaryFunc::SubTimestampTz => f.write_str("-"),
            BinaryFunc::SubTimestampInterval => f.write_str("-"),
            BinaryFunc::SubTimestampTzInterval => f.write_str("-"),
            BinaryFunc::SubDecimal => f.write_str("-"),
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
            BinaryFunc::MatchRegex => f.write_str("~"),
            BinaryFunc::ToCharTimestamp => f.write_str("tocharts"),
            BinaryFunc::ToCharTimestampTz => f.write_str("tochartstz"),
            BinaryFunc::DateTrunc => f.write_str("date_trunc"),
            BinaryFunc::CastFloat32ToDecimal => f.write_str("f32todec"),
            BinaryFunc::CastFloat64ToDecimal => f.write_str("f64todec"),
            BinaryFunc::JsonbGetInt64 => f.write_str("b->i64"),
            BinaryFunc::JsonbGetString => f.write_str("b->str"),
            BinaryFunc::JsonbContainsString => f.write_str("b?"),
            BinaryFunc::JsonbConcat => f.write_str("b||"),
            BinaryFunc::JsonbContainsJsonb => f.write_str("b<@"),
            BinaryFunc::JsonbDeleteInt64 => f.write_str("b-int64"),
            BinaryFunc::JsonbDeleteString => f.write_str("b-string"),
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
    SqrtFloat32,
    SqrtFloat64,
    AbsInt32,
    AbsInt64,
    AbsFloat32,
    AbsFloat64,
    CastBoolToString,
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
    CastFloat64ToInt64,
    CastFloat64ToString,
    CastDecimalToInt32,
    CastDecimalToInt64,
    CastDecimalToString(u8),
    CastSignificandToFloat32,
    CastSignificandToFloat64,
    CastStringToBytes,
    CastStringToFloat64,
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
    CastJsonbToString,
    CastJsonbToStringUnlessString,
    CastJsonbOrNullToJsonb,
    CastJsonbToFloat64,
    CastJsonbToBool,
    CeilFloat32,
    CeilFloat64,
    CeilDecimal(u8),
    FloorFloat32,
    FloorFloat64,
    FloorDecimal(u8),
    Ascii,
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
    JsonbArrayLength,
    JsonbTypeof,
    JsonbStripNulls,
    JsonbPretty,
}

impl UnaryFunc {
    pub fn eval<'a>(
        &'a self,
        a: Datum<'a>,
        _env: &'a EvalEnv,
        temp_storage: &'a RowArena,
    ) -> Datum<'a> {
        match self {
            UnaryFunc::Not => not(a),
            UnaryFunc::IsNull => is_null(a),
            UnaryFunc::NegInt32 => neg_int32(a),
            UnaryFunc::NegInt64 => neg_int64(a),
            UnaryFunc::NegFloat32 => neg_float32(a),
            UnaryFunc::NegFloat64 => neg_float64(a),
            UnaryFunc::NegDecimal => neg_decimal(a),
            UnaryFunc::AbsInt32 => abs_int32(a),
            UnaryFunc::AbsInt64 => abs_int64(a),
            UnaryFunc::AbsFloat32 => abs_float32(a),
            UnaryFunc::AbsFloat64 => abs_float64(a),
            UnaryFunc::CastBoolToString => cast_bool_to_string(a),
            UnaryFunc::CastInt32ToBool => cast_int32_to_bool(a),
            UnaryFunc::CastInt32ToFloat32 => cast_int32_to_float32(a),
            UnaryFunc::CastInt32ToFloat64 => cast_int32_to_float64(a),
            UnaryFunc::CastInt32ToInt64 => cast_int32_to_int64(a),
            UnaryFunc::CastInt32ToDecimal => cast_int32_to_decimal(a),
            UnaryFunc::CastInt32ToString => cast_int32_to_string(a, temp_storage),
            UnaryFunc::CastInt64ToInt32 => cast_int64_to_int32(a),
            UnaryFunc::CastInt64ToBool => cast_int64_to_bool(a),
            UnaryFunc::CastInt64ToDecimal => cast_int64_to_decimal(a),
            UnaryFunc::CastInt64ToFloat32 => cast_int64_to_float32(a),
            UnaryFunc::CastInt64ToFloat64 => cast_int64_to_float64(a),
            UnaryFunc::CastInt64ToString => cast_int64_to_string(a, temp_storage),
            UnaryFunc::CastFloat32ToInt64 => cast_float32_to_int64(a),
            UnaryFunc::CastFloat32ToFloat64 => cast_float32_to_float64(a),
            UnaryFunc::CastFloat32ToString => cast_float32_to_string(a, temp_storage),
            UnaryFunc::CastFloat64ToInt64 => cast_float64_to_int64(a),
            UnaryFunc::CastFloat64ToString => cast_float64_to_string(a, temp_storage),
            UnaryFunc::CastDecimalToInt32 => cast_decimal_to_int32(a),
            UnaryFunc::CastDecimalToInt64 => cast_decimal_to_int64(a),
            UnaryFunc::CastSignificandToFloat32 => cast_significand_to_float32(a),
            UnaryFunc::CastSignificandToFloat64 => cast_significand_to_float64(a),
            UnaryFunc::CastStringToFloat64 => cast_string_to_float64(a),
            UnaryFunc::CastStringToBytes => cast_string_to_bytes(a, temp_storage),
            UnaryFunc::CastDateToTimestamp => cast_date_to_timestamp(a),
            UnaryFunc::CastDateToTimestampTz => cast_date_to_timestamptz(a),
            UnaryFunc::CastDateToString => cast_date_to_string(a, temp_storage),
            UnaryFunc::CastDecimalToString(scale) => {
                cast_decimal_to_string(a, *scale, temp_storage)
            }
            UnaryFunc::CastTimestampToDate => cast_timestamp_to_date(a),
            UnaryFunc::CastTimestampToTimestampTz => cast_timestamp_to_timestamptz(a),
            UnaryFunc::CastTimestampToString => cast_timestamp_to_string(a, temp_storage),
            UnaryFunc::CastTimestampTzToDate => cast_timestamptz_to_date(a),
            UnaryFunc::CastTimestampTzToTimestamp => cast_timestamptz_to_timestamp(a),
            UnaryFunc::CastTimestampTzToString => cast_timestamptz_to_string(a, temp_storage),
            UnaryFunc::CastIntervalToString => cast_interval_to_string(a, temp_storage),
            UnaryFunc::CastBytesToString => cast_bytes_to_string(a, temp_storage),
            UnaryFunc::CastStringToJsonb => cast_string_to_jsonb(a, temp_storage),
            UnaryFunc::CastJsonbToString => cast_jsonb_to_string(a, temp_storage),
            UnaryFunc::CastJsonbToStringUnlessString => {
                cast_jsonb_to_string_unless_string(a, temp_storage)
            }
            UnaryFunc::CastJsonbOrNullToJsonb => cast_jsonb_or_null_to_jsonb(a),
            UnaryFunc::CastJsonbToFloat64 => cast_jsonb_to_float64(a),
            UnaryFunc::CastJsonbToBool => cast_jsonb_to_bool(a),
            UnaryFunc::CeilFloat32 => ceil_float32(a),
            UnaryFunc::CeilFloat64 => ceil_float64(a),
            UnaryFunc::CeilDecimal(scale) => ceil_decimal(a, *scale),
            UnaryFunc::FloorFloat32 => floor_float32(a),
            UnaryFunc::FloorFloat64 => floor_float64(a),
            UnaryFunc::FloorDecimal(scale) => floor_decimal(a, *scale),
            UnaryFunc::SqrtFloat32 => sqrt_float32(a),
            UnaryFunc::SqrtFloat64 => sqrt_float64(a),
            UnaryFunc::Ascii => ascii(a),
            UnaryFunc::MatchRegex(regex) => match_cached_regex(a, &regex),
            UnaryFunc::ExtractIntervalYear => extract_interval_year(a),
            UnaryFunc::ExtractIntervalMonth => extract_interval_month(a),
            UnaryFunc::ExtractIntervalDay => extract_interval_day(a),
            UnaryFunc::ExtractIntervalHour => extract_interval_hour(a),
            UnaryFunc::ExtractIntervalMinute => extract_interval_minute(a),
            UnaryFunc::ExtractIntervalSecond => extract_interval_second(a),
            UnaryFunc::ExtractTimestampYear => extract_timestamp_year(a),
            UnaryFunc::ExtractTimestampQuarter => extract_timestamp_quarter(a),
            UnaryFunc::ExtractTimestampMonth => extract_timestamp_month(a),
            UnaryFunc::ExtractTimestampDay => extract_timestamp_day(a),
            UnaryFunc::ExtractTimestampHour => extract_timestamp_hour(a),
            UnaryFunc::ExtractTimestampMinute => extract_timestamp_minute(a),
            UnaryFunc::ExtractTimestampSecond => extract_timestamp_second(a),
            UnaryFunc::ExtractTimestampWeek => extract_timestamp_week(a),
            UnaryFunc::ExtractTimestampDayOfYear => extract_timestamp_dayofyear(a),
            UnaryFunc::ExtractTimestampDayOfWeek => extract_timestamp_dayofweek(a),
            UnaryFunc::ExtractTimestampIsoDayOfWeek => extract_timestamp_isodayofweek(a),
            UnaryFunc::ExtractTimestampTzYear => extract_timestamptz_year(a),
            UnaryFunc::ExtractTimestampTzQuarter => extract_timestamptz_quarter(a),
            UnaryFunc::ExtractTimestampTzMonth => extract_timestamptz_month(a),
            UnaryFunc::ExtractTimestampTzDay => extract_timestamptz_day(a),
            UnaryFunc::ExtractTimestampTzHour => extract_timestamptz_hour(a),
            UnaryFunc::ExtractTimestampTzMinute => extract_timestamptz_minute(a),
            UnaryFunc::ExtractTimestampTzSecond => extract_timestamptz_second(a),
            UnaryFunc::ExtractTimestampTzWeek => extract_timestamptz_week(a),
            UnaryFunc::ExtractTimestampTzDayOfYear => extract_timestamptz_dayofyear(a),
            UnaryFunc::ExtractTimestampTzDayOfWeek => extract_timestamptz_dayofweek(a),
            UnaryFunc::ExtractTimestampTzIsoDayOfWeek => extract_timestamptz_isodayofweek(a),
            UnaryFunc::DateTrunc(to) => match to {
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
            },
            UnaryFunc::JsonbArrayLength => jsonb_array_length(a),
            UnaryFunc::JsonbTypeof => jsonb_typeof(a),
            UnaryFunc::JsonbStripNulls => jsonb_strip_nulls(a, temp_storage),
            UnaryFunc::JsonbPretty => jsonb_pretty(a, temp_storage),
        }
    }

    /// Reports whether this function has a symbolic string representation.
    pub fn display_is_symbolic(&self) -> bool {
        let out = match self {
            UnaryFunc::Not
            | UnaryFunc::NegInt32
            | UnaryFunc::NegInt64
            | UnaryFunc::NegFloat32
            | UnaryFunc::NegFloat64
            | UnaryFunc::NegDecimal => true,
            _ => false,
        };
        // This debug assertion is an attempt to ensure that this function
        // stays in sync when new `UnaryFunc` variants are added.
        debug_assert_eq!(out, self.to_string().len() < 3);
        out
    }

    pub fn output_type(&self, input_type: ColumnType) -> ColumnType {
        use UnaryFunc::*;
        let in_nullable = input_type.nullable;
        match self {
            IsNull | CastInt32ToBool | CastInt64ToBool => ColumnType::new(ScalarType::Bool),

            Ascii => ColumnType::new(ScalarType::Int32).nullable(in_nullable),

            MatchRegex(_) => ColumnType::new(ScalarType::Bool).nullable(in_nullable),

            CastStringToBytes => ColumnType::new(ScalarType::Bytes).nullable(in_nullable),

            CastBoolToString
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
            | CastSignificandToFloat64
            | CastStringToFloat64 => ColumnType::new(ScalarType::Float64).nullable(in_nullable),

            CastInt64ToInt32 | CastDecimalToInt32 => {
                ColumnType::new(ScalarType::Int32).nullable(in_nullable)
            }

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
            // can return null for nan/infinity
            CastJsonbToString | CastJsonbToStringUnlessString => {
                ColumnType::new(ScalarType::String).nullable(true)
            }

            // converts null to jsonnull but converts nan to null
            CastJsonbOrNullToJsonb => ColumnType::new(ScalarType::Jsonb).nullable(true),

            // can return null for other jsonb types
            CastJsonbToFloat64 => ColumnType::new(ScalarType::Float64).nullable(true),
            CastJsonbToBool => ColumnType::new(ScalarType::Bool).nullable(true),

            CeilFloat32 | FloorFloat32 => {
                ColumnType::new(ScalarType::Float32).nullable(in_nullable)
            }
            CeilFloat64 | FloorFloat64 => {
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

            Not | NegInt32 | NegInt64 | NegFloat32 | NegFloat64 | NegDecimal | AbsInt32
            | AbsInt64 | AbsFloat32 | AbsFloat64 => input_type,

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
            | UnaryFunc::CastBoolToString
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
            UnaryFunc::AbsInt32 => f.write_str("abs"),
            UnaryFunc::AbsInt64 => f.write_str("abs"),
            UnaryFunc::AbsFloat32 => f.write_str("abs"),
            UnaryFunc::AbsFloat64 => f.write_str("abs"),
            UnaryFunc::CastBoolToString => f.write_str("booltostr"),
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
            UnaryFunc::CastFloat64ToInt64 => f.write_str("f64toi64"),
            UnaryFunc::CastFloat64ToString => f.write_str("f64tostr"),
            UnaryFunc::CastDecimalToInt32 => f.write_str("dectoi32"),
            UnaryFunc::CastDecimalToInt64 => f.write_str("dectoi64"),
            UnaryFunc::CastDecimalToString(_) => f.write_str("dectostr"),
            UnaryFunc::CastSignificandToFloat32 => f.write_str("dectof32"),
            UnaryFunc::CastSignificandToFloat64 => f.write_str("dectof64"),
            UnaryFunc::CastStringToBytes => f.write_str("strtobytes"),
            UnaryFunc::CastStringToFloat64 => f.write_str("strtof64"),
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
            UnaryFunc::CastJsonbToString => f.write_str("jsonbtostr"),
            UnaryFunc::CastJsonbToStringUnlessString => f.write_str("jsonbtostr?"),
            UnaryFunc::CastJsonbOrNullToJsonb => f.write_str("jsonb?tojsonb"),
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
            UnaryFunc::MatchRegex(regex) => write!(f, "{} ~", regex.as_str()),
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
            UnaryFunc::JsonbArrayLength => f.write_str("jsonb_array_length"),
            UnaryFunc::JsonbTypeof => f.write_str("jsonb_typeof"),
            UnaryFunc::JsonbStripNulls => f.write_str("jsonb_strip_nulls"),
            UnaryFunc::JsonbPretty => f.write_str("jsonb_pretty"),
        }
    }
}

fn coalesce<'a>(datums: &[Datum<'a>]) -> Datum<'a> {
    datums
        .iter()
        .find(|d| !d.is_null())
        .cloned()
        .unwrap_or(Datum::Null)
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

fn length<'a>(datums: &[Datum<'a>]) -> Datum<'a> {
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
            None => {
                // TODO: Once we can return errors from functions, this should return:
                // "ERROR:  invalid encoding name \"{}\"", encoding_name
                return Datum::Null;
            }
        };

        let decoded_string = match enc.decode(string.as_bytes(), DecoderTrap::Strict) {
            Ok(s) => s,
            Err(_) => {
                // TODO: Once we can return errors from functions, this should return:
                // "ERROR:  invalid byte sequence for encoding \"{}\": \"{}\"", encoding_name, errored_bytes
                return Datum::Null;
            }
        };

        Datum::from(decoded_string.chars().count() as i32)
    } else {
        Datum::from(string.chars().count() as i32)
    }
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
        temp_storage.make_datum(|packer| {
            packer.push_dict(datums.chunks(2).map(|kv| (kv[0].unwrap_str(), kv[1])))
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
    MakeTimestamp,
    Substr,
    Length,
    Replace,
    JsonbBuildArray,
    JsonbBuildObject,
}

impl VariadicFunc {
    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        _env: &'a EvalEnv,
        temp_storage: &'a RowArena,
    ) -> Datum<'a> {
        match self {
            VariadicFunc::Coalesce => coalesce(datums),
            VariadicFunc::MakeTimestamp => make_timestamp(datums),
            VariadicFunc::Substr => substr(datums),
            VariadicFunc::Length => length(datums),
            VariadicFunc::Replace => replace(datums, temp_storage),
            VariadicFunc::JsonbBuildArray => jsonb_build_array(datums, temp_storage),
            VariadicFunc::JsonbBuildObject => jsonb_build_object(datums, temp_storage),
        }
    }

    pub fn output_type(&self, input_types: Vec<ColumnType>) -> ColumnType {
        use VariadicFunc::*;
        match self {
            Coalesce => {
                let any_nullable = input_types.iter().any(|typ| typ.nullable);
                for typ in input_types {
                    if typ.scalar_type != ScalarType::Null {
                        return typ.nullable(any_nullable);
                    }
                }
                ColumnType::new(ScalarType::Null)
            }
            MakeTimestamp => ColumnType::new(ScalarType::Timestamp).nullable(true),
            Substr => ColumnType::new(ScalarType::String).nullable(true),
            Length => ColumnType::new(ScalarType::Int32).nullable(true),
            Replace => ColumnType::new(ScalarType::String).nullable(true),
            JsonbBuildArray | JsonbBuildObject => {
                ColumnType::new(ScalarType::Jsonb).nullable(false)
            }
        }
    }

    /// Whether the function output is NULL if any of its inputs are NULL.
    pub fn propagates_nulls(&self) -> bool {
        match self {
            VariadicFunc::Coalesce
            | VariadicFunc::JsonbBuildArray
            | VariadicFunc::JsonbBuildObject => false,
            _ => true,
        }
    }
}

impl fmt::Display for VariadicFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VariadicFunc::Coalesce => f.write_str("coalesce"),
            VariadicFunc::MakeTimestamp => f.write_str("makets"),
            VariadicFunc::Substr => f.write_str("substr"),
            VariadicFunc::Length => f.write_str("length"),
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
