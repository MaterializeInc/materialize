// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::cmp;
use std::convert::TryFrom;
use std::fmt;
use std::fmt::Write;
use std::str::FromStr;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use encoding::label::encoding_from_whatwg_label;
use encoding::DecoderTrap;
use failure::bail;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

pub use crate::like::build_like_regex_from_string;
use crate::EvalEnv;
use repr::decimal::MAX_DECIMAL_PRECISION;
use repr::regex::Regex;
use repr::{ColumnType, Datum, Interval, PackableRow, ScalarType};

#[derive(Ord, PartialOrd, Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum NullaryFunc {
    MzLogicalTimestamp,
    Now,
}

impl NullaryFunc {
    pub fn func(self) -> for<'a> fn(&EvalEnv, &mut PackableRow<'a>) -> Datum<'a> {
        match self {
            NullaryFunc::MzLogicalTimestamp => mz_logical_time,
            NullaryFunc::Now => now,
        }
    }

    pub fn output_type(self) -> ColumnType {
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

pub fn mz_logical_time<'a>(env: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(
        env.logical_time
            .expect("mz_logical_time missing logical time") as i128,
    )
}

pub fn now<'a>(env: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(env.wall_time.expect("now missing wall time in env"))
}

pub fn and<'a>(a: Datum<'a>, b: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    match (a, b) {
        (Datum::False, _) => Datum::False,
        (_, Datum::False) => Datum::False,
        (Datum::Null, _) => Datum::Null,
        (_, Datum::Null) => Datum::Null,
        (Datum::True, Datum::True) => Datum::True,
        _ => unreachable!(),
    }
}

pub fn or<'a>(a: Datum<'a>, b: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    match (a, b) {
        (Datum::True, _) => Datum::True,
        (_, Datum::True) => Datum::True,
        (Datum::Null, _) => Datum::Null,
        (_, Datum::Null) => Datum::Null,
        (Datum::False, Datum::False) => Datum::False,
        _ => unreachable!(),
    }
}

pub fn not<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(!a.unwrap_bool())
}

pub fn abs_int32<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int32().abs())
}

pub fn abs_int64<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int64().abs())
}

pub fn abs_float32<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float32().abs())
}

pub fn abs_float64<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float64().abs())
}

pub fn cast_bool_to_string<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    match a.unwrap_bool() {
        true => Datum::from("true"),
        false => Datum::from("false"),
    }
}

pub fn cast_int32_to_bool<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int32() != 0)
}

pub fn cast_int32_to_string<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    temp_storage: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::String(temp_storage.push_string(&a.unwrap_int32().to_string()))
}

pub fn cast_int32_to_float32<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    // TODO(benesch): is this cast valid?
    Datum::from(a.unwrap_int32() as f32)
}

pub fn cast_int32_to_float64<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    // TODO(benesch): is this cast valid?
    Datum::from(f64::from(a.unwrap_int32()))
}

pub fn cast_int32_to_int64<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(i64::from(a.unwrap_int32()))
}

pub fn cast_int32_to_decimal<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(i128::from(a.unwrap_int32()))
}

pub fn cast_int64_to_bool<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int64() != 0)
}

pub fn cast_int64_to_int32<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    // TODO(benesch): we need to do something better than panicking if the
    // datum doesn't fit in an int32, but what? Poison the whole dataflow?
    // The SQL standard says this an error, but runtime errors are complicated
    // in a streaming setting.
    Datum::from(i32::try_from(a.unwrap_int64()).unwrap())
}

pub fn cast_int64_to_decimal<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(i128::from(a.unwrap_int64()))
}

pub fn cast_int64_to_float32<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    // TODO(benesch): is this cast valid?
    Datum::from(a.unwrap_int64() as f32)
}

pub fn cast_int64_to_float64<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    // TODO(benesch): is this cast valid?
    Datum::from(a.unwrap_int64() as f64)
}

pub fn cast_int64_to_string<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    temp_storage: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::String(temp_storage.push_string(&a.unwrap_int64().to_string()))
}

pub fn cast_float32_to_int64<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    // TODO(benesch): this is undefined behavior if the f32 doesn't fit in an
    // i64 (https://github.com/rust-lang/rust/issues/10184).
    Datum::from(a.unwrap_float32() as i64)
}

pub fn cast_float32_to_float64<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    // TODO(benesch): is this cast valid?
    Datum::from(f64::from(a.unwrap_float32()))
}

pub fn cast_float32_to_decimal<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
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

pub fn cast_float32_to_string<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    temp_storage: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::String(temp_storage.push_string(&a.unwrap_float32().to_string()))
}

pub fn cast_float64_to_int64<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    // TODO(benesch): this is undefined behavior if the f32 doesn't fit in an
    // i64 (https://github.com/rust-lang/rust/issues/10184).
    Datum::from(a.unwrap_float64() as i64)
}

pub fn cast_float64_to_decimal<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
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

pub fn cast_float64_to_string<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    temp_storage: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::String(temp_storage.push_string(&a.unwrap_float64().to_string()))
}

pub fn cast_decimal_to_int32<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_decimal().as_i128() as i32)
}

pub fn cast_decimal_to_int64<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_decimal().as_i128() as i64)
}

pub fn cast_significand_to_float32<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    // The second half of this function is defined in plan_cast_internal
    Datum::from(a.unwrap_decimal().as_i128() as f32)
}

pub fn cast_significand_to_float64<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    // The second half of this function is defined in plan_cast_internal
    Datum::from(a.unwrap_decimal().as_i128() as f64)
}

pub fn cast_decimal_to_string<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    temp_storage: &mut PackableRow<'a>,
) -> Datum<'a> {
    // TODO(benesch): a better way to pass the scale into the dataflow layer.
    let scale = b.unwrap_int32() as u8;
    Datum::String(temp_storage.push_string(&a.unwrap_decimal().with_scale(scale).to_string()))
}

pub fn cast_string_to_float64<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    let val: Result<f64, _> = a.unwrap_str().to_lowercase().parse();
    Datum::from(val.ok())
}

pub fn cast_string_to_bytes<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    temp_storage: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::Bytes(&temp_storage.push_bytes(&a.unwrap_str().as_bytes().to_vec()))
}

pub fn cast_date_to_timestamp<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::Timestamp(a.unwrap_date().and_hms(0, 0, 0))
}

pub fn cast_date_to_timestamptz<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::TimestampTz(DateTime::<Utc>::from_utc(
        a.unwrap_date().and_hms(0, 0, 0),
        Utc,
    ))
}

pub fn cast_date_to_string<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    temp_storage: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::String(temp_storage.push_string(&a.unwrap_date().to_string()))
}

pub fn cast_timestamp_to_date<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::Date(a.unwrap_timestamp().date())
}

pub fn cast_timestamp_to_timestamptz<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::TimestampTz(DateTime::<Utc>::from_utc(a.unwrap_timestamp(), Utc))
}

pub fn cast_timestamp_to_string<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    temp_storage: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::String(temp_storage.push_string(&a.unwrap_timestamp().to_string()))
}

pub fn cast_timestamptz_to_date<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::Date(a.unwrap_timestamptz().naive_utc().date())
}

pub fn cast_timestamptz_to_timestamp<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::Timestamp(a.unwrap_timestamptz().naive_utc())
}

pub fn cast_timestamptz_to_string<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    temp_storage: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::String(temp_storage.push_string(&a.unwrap_timestamptz().to_string()))
}

pub fn cast_interval_to_string<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    temp_storage: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::String(temp_storage.push_string(&a.unwrap_interval().to_string()))
}

pub fn cast_bytes_to_string<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    temp_storage: &mut PackableRow<'a>,
) -> Datum<'a> {
    let bytes = a.unwrap_bytes();
    let mut out = String::from("\\x");
    out.reserve(bytes.len() * 2);
    for byte in bytes {
        write!(&mut out, "{:x}", byte).expect("writing to string cannot fail");
    }
    Datum::String(temp_storage.push_string(&out))
}

pub fn serde_to_datum<'a>(
    temp_storage: &mut PackableRow<'a>,
    serde: serde_json::Value,
) -> Result<Datum<'a>, failure::Error> {
    use serde_json::Value;
    Ok(match serde {
        Value::Null => Datum::JsonNull,
        Value::Bool(b) => {
            if b {
                Datum::True
            } else {
                Datum::False
            }
        }
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                Datum::Float64(OrderedFloat(f))
            } else {
                bail!("{} is out of range for json number", n)
            }
        }
        Value::String(s) => Datum::String(temp_storage.push_string(&s)),
        Value::Array(array) => {
            let elems = array
                .into_iter()
                .map(|elem| serde_to_datum(temp_storage, elem))
                .collect::<Result<Vec<_>, _>>()?;
            Datum::List(temp_storage.push_list(elems))
        }
        Value::Object(object) => {
            let mut pairs = object
                .into_iter()
                .map(|(key, val)| Ok((key, serde_to_datum(temp_storage, val)?)))
                .collect::<Result<Vec<_>, failure::Error>>()?;
            pairs.sort();
            Datum::Dict(temp_storage.push_dict(pairs))
        }
    })
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
            Value::Number(
                // Internally we want all json numbers to be floats so we have a consistent binary representation for joins.
                // But we want serde to print integer-like things as integers, for consistency with postgres.
                if f == f.trunc() {
                    (f.trunc() as i64).into()
                } else if let Some(n) = serde_json::Number::from_f64(f) {
                    n
                } else {
                    // This should only be reachable for NaN/Infinity, which aren't allowed to be cast to Jsonb
                    panic!("Not a valid json number: {}", f)
                },
            )
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
pub fn cast_string_to_jsonb<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    temp_storage: &mut PackableRow<'a>,
) -> Datum<'a> {
    match serde_json::from_str(a.unwrap_str()) {
        Err(_) => Datum::Null,
        Ok(json) => match serde_to_datum(temp_storage, json) {
            Err(_) => Datum::Null,
            Ok(datum) => datum,
        },
    }
}

// TODO(jamii) it would be much more efficient to skip the intermediate serde_json::Value
pub fn cast_jsonb_to_string<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    temp_storage: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::String(temp_storage.push_string(&datum_to_serde(a).to_string()))
}

pub fn add_int32<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(a.unwrap_int32() + b.unwrap_int32())
}

pub fn add_int64<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(a.unwrap_int64() + b.unwrap_int64())
}

pub fn add_float32<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(a.unwrap_float32() + b.unwrap_float32())
}

pub fn add_float64<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(a.unwrap_float64() + b.unwrap_float64())
}

pub fn add_timestamp_interval<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
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

pub fn add_timestamptz_interval<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
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

pub fn ceil_float32<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float32().ceil())
}

pub fn ceil_float64<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float64().ceil())
}

pub fn floor_float32<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float32().floor())
}

pub fn floor_float64<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_float64().floor())
}

pub fn sub_timestamp_interval<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    env: &EvalEnv,
    temp_storage: &mut PackableRow<'a>,
) -> Datum<'a> {
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
    add_timestamp_interval(a, inverse, env, temp_storage)
}

pub fn sub_timestamptz_interval<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    env: &EvalEnv,
    temp_storage: &mut PackableRow<'a>,
) -> Datum<'a> {
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
    add_timestamptz_interval(a, inverse, env, temp_storage)
}

fn add_timestamp_months(dt: NaiveDateTime, months: i64) -> NaiveDateTime {
    use std::convert::TryInto;

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

pub fn add_decimal<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(a.unwrap_decimal() + b.unwrap_decimal())
}

pub fn sub_int32<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(a.unwrap_int32() - b.unwrap_int32())
}

pub fn sub_int64<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(a.unwrap_int64() - b.unwrap_int64())
}

pub fn sub_float32<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(a.unwrap_float32() - b.unwrap_float32())
}

pub fn sub_float64<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(a.unwrap_float64() - b.unwrap_float64())
}

pub fn sub_decimal<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(a.unwrap_decimal() - b.unwrap_decimal())
}

pub fn mul_int32<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(a.unwrap_int32() * b.unwrap_int32())
}

pub fn mul_int64<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(a.unwrap_int64() * b.unwrap_int64())
}

pub fn mul_float32<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(a.unwrap_float32() * b.unwrap_float32())
}

pub fn mul_float64<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(a.unwrap_float64() * b.unwrap_float64())
}

pub fn mul_decimal<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(a.unwrap_decimal() * b.unwrap_decimal())
}

// TODO(jamii) we don't currently have any way of reporting errors from functions, so for now we just adopt sqlite's approach 1/0 = null

pub fn div_int32<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let b = b.unwrap_int32();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_int32() / b)
    }
}

pub fn div_int64<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let b = b.unwrap_int64();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_int64() / b)
    }
}

pub fn div_float32<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let b = b.unwrap_float32();
    if b == 0.0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_float32() / b)
    }
}

pub fn div_float64<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let b = b.unwrap_float64();
    if b == 0.0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_float64() / b)
    }
}

pub fn div_decimal<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let b = b.unwrap_decimal();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_decimal() / b)
    }
}

pub fn ceil_decimal<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let decimal = a.unwrap_decimal();
    let scale = b.unwrap_int32() as u8;
    Datum::from(decimal.with_scale(scale).ceil().significand())
}

pub fn floor_decimal<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let decimal = a.unwrap_decimal();
    let scale = b.unwrap_int32() as u8;
    Datum::from(decimal.with_scale(scale).floor().significand())
}

pub fn mod_int32<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let b = b.unwrap_int32();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_int32() % b)
    }
}

pub fn mod_int64<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let b = b.unwrap_int64();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_int64() % b)
    }
}

pub fn mod_float32<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let b = b.unwrap_float32();
    if b == 0.0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_float32() % b)
    }
}

pub fn mod_float64<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let b = b.unwrap_float64();
    if b == 0.0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_float64() % b)
    }
}

pub fn mod_decimal<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let b = b.unwrap_decimal();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_decimal() % b)
    }
}

pub fn neg_int32<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(-a.unwrap_int32())
}

pub fn neg_int64<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(-a.unwrap_int64())
}

pub fn neg_float32<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(-a.unwrap_float32())
}

pub fn neg_float64<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(-a.unwrap_float64())
}

pub fn neg_decimal<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(-a.unwrap_decimal())
}

pub fn sqrt_float32<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    let x = a.unwrap_float32();
    if x < 0.0 {
        return Datum::Null;
    }
    Datum::from(x.sqrt())
}

pub fn sqrt_float64<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    let x = a.unwrap_float64();
    if x < 0.0 {
        return Datum::Null;
    }
    Datum::from(x.sqrt())
}

pub fn eq<'a>(a: Datum<'a>, b: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a == b)
}

pub fn not_eq<'a>(a: Datum<'a>, b: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a != b)
}

pub fn lt<'a>(a: Datum<'a>, b: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a < b)
}

pub fn lte<'a>(a: Datum<'a>, b: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a <= b)
}

pub fn gt<'a>(a: Datum<'a>, b: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a > b)
}

pub fn gte<'a>(a: Datum<'a>, b: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a >= b)
}

pub fn jsonb_get_int64<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
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
            list.iter().nth(i as usize).unwrap_or(Datum::Null)
        }
        Datum::Dict(_) => Datum::Null,
        Datum::Null
        | Datum::JsonNull
        | Datum::True
        | Datum::False
        | Datum::Float64(_)
        | Datum::String(_) => {
            if i == 0 || i == -1 {
                // I have no idea why postgres does this, but we're stuck with it
                a
            } else {
                Datum::Null
            }
        }
        _ => unreachable!(),
    }
}

pub fn jsonb_get_string<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let k = b.unwrap_str();
    match a {
        Datum::Dict(dict) => match dict.iter().find(|(k2, _v)| k == *k2) {
            Some((_k, v)) => v,
            None => Datum::Null,
        },
        Datum::Null
        | Datum::JsonNull
        | Datum::True
        | Datum::False
        | Datum::Float64(_)
        | Datum::String(_)
        | Datum::List(_) => Datum::Null,
        _ => unreachable!(),
    }
}

pub fn to_char<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    temp_storage: &mut PackableRow<'a>,
) -> Datum<'a> {
    let datetime = a.unwrap_timestamptz();
    let format_string = b.unwrap_str();
    // PostgreSQL parses this weird format string, hand-interpret for now
    // to unblock Metabase progress. Will have to revisit formatting strings and
    // other versions of to_char() in the future.
    if format_string == "YYYY-MM-DD HH24:MI:SS.MS TZ" {
        let interpreted_format_string = "%Y-%m-%d %H:%M:%S.%f";
        Datum::String(
            temp_storage.push_string(&datetime.format(interpreted_format_string).to_string()),
        )
    } else {
        Datum::Null
    }
}

pub fn match_regex<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
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

pub fn match_cached_regex<'a>(a: Datum<'a>, needle: &Regex) -> Datum<'a> {
    let haystack = a.unwrap_str();
    Datum::from(needle.is_match(haystack))
}

pub fn ascii<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    match a.unwrap_str().chars().next() {
        None => Datum::Int32(0),
        Some(v) => Datum::Int32(v as i32),
    }
}

pub fn extract_interval_year<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_interval().years())
}

pub fn extract_interval_month<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_interval().months())
}

pub fn extract_interval_day<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_interval().days())
}

pub fn extract_interval_hour<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_interval().hours())
}

pub fn extract_interval_minute<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(a.unwrap_interval().minutes())
}

pub fn extract_interval_second<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(a.unwrap_interval().seconds())
}

pub fn extract_timestamp_year<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamp().year()))
}

pub fn extract_timestamptz_year<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamptz().year()))
}

pub fn extract_timestamp_quarter<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from((f64::from(a.unwrap_timestamp().month()) / 3.0).ceil())
}

pub fn extract_timestamptz_quarter<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from((f64::from(a.unwrap_timestamptz().month()) / 3.0).ceil())
}

pub fn extract_timestamp_month<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamp().month()))
}

pub fn extract_timestamptz_month<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamptz().month()))
}

pub fn extract_timestamp_day<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamp().day()))
}

pub fn extract_timestamptz_day<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamptz().day()))
}

pub fn extract_timestamp_hour<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamp().hour()))
}

pub fn extract_timestamptz_hour<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamptz().hour()))
}

pub fn extract_timestamp_minute<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamp().minute()))
}

pub fn extract_timestamptz_minute<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::from(f64::from(a.unwrap_timestamptz().minute()))
}

pub fn extract_timestamp_second<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let a = a.unwrap_timestamp();
    let s = f64::from(a.second());
    let ns = f64::from(a.nanosecond()) / 1e9;
    Datum::from(s + ns)
}

pub fn extract_timestamptz_second<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let a = a.unwrap_timestamptz();
    let s = f64::from(a.second());
    let ns = f64::from(a.nanosecond()) / 1e9;
    Datum::from(s + ns)
}

/// Extract the iso week of the year
///
/// Note that because isoweeks are defined in terms of January 4th, Jan 1 is only in week
/// 1 about half of the time
pub fn extract_timestamp_week<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    let a = a.unwrap_timestamp();
    Datum::from(f64::from(a.iso_week().week()))
}

/// Extract the iso week of the year
///
/// Note that because isoweeks are defined in terms of January 4th, Jan 1 is only in week
/// 1 about half of the time
pub fn extract_timestamptz_week<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let a = a.unwrap_timestamptz();
    Datum::from(f64::from(a.iso_week().week()))
}

pub fn extract_timestamp_dayofyear<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let a = a.unwrap_timestamp();
    Datum::from(f64::from(a.ordinal()))
}

pub fn extract_timestamptz_dayofyear<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let a = a.unwrap_timestamptz();
    Datum::from(f64::from(a.ordinal()))
}

/// extract day of week with monday = 1 sunday = 0
pub fn extract_timestamp_dayofweek<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let a = a.unwrap_timestamp();
    Datum::from(a.weekday().num_days_from_sunday() as f64)
}

/// extract day of week with monday = 1 sunday = 0
pub fn extract_timestamptz_dayofweek<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let a = a.unwrap_timestamptz();
    Datum::from(a.weekday().num_days_from_sunday() as f64)
}

/// extract day of week with monday = 1 sunday = 7
pub fn extract_timestamp_isodayofweek<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let a = a.unwrap_timestamp();
    Datum::from(a.weekday().number_from_monday() as f64)
}

/// extract day of week with monday = 1 sunday = 7
pub fn extract_timestamptz_isodayofweek<'a>(
    a: Datum<'a>,
    _: &EvalEnv,
    _: &mut PackableRow<'a>,
) -> Datum<'a> {
    let a = a.unwrap_timestamptz();
    Datum::from(a.weekday().number_from_monday() as f64)
}

pub fn date_trunc<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    env: &EvalEnv,
    temp_storage: &mut PackableRow<'a>,
) -> Datum<'a> {
    match a.unwrap_str().parse::<DateTruncTo>() {
        Ok(DateTruncTo::Micros) => date_trunc_microseconds(b, env, temp_storage),
        Ok(DateTruncTo::Millis) => date_trunc_milliseconds(b, env, temp_storage),
        Ok(DateTruncTo::Second) => date_trunc_second(b, env, temp_storage),
        Ok(DateTruncTo::Minute) => date_trunc_minute(b, env, temp_storage),
        Ok(DateTruncTo::Hour) => date_trunc_hour(b, env, temp_storage),
        Ok(DateTruncTo::Day) => date_trunc_day(b, env, temp_storage),
        Ok(DateTruncTo::Week) => date_trunc_week(b, env, temp_storage),
        Ok(DateTruncTo::Month) => date_trunc_month(b, env, temp_storage),
        Ok(DateTruncTo::Quarter) => date_trunc_quarter(b, env, temp_storage),
        Ok(DateTruncTo::Year) => date_trunc_year(b, env, temp_storage),
        Ok(DateTruncTo::Decade) => date_trunc_decade(b, env, temp_storage),
        Ok(DateTruncTo::Century) => date_trunc_century(b, env, temp_storage),
        Ok(DateTruncTo::Millennium) => date_trunc_millennium(b, env, temp_storage),
        // TODO: return an error when we support that.
        _ => Datum::Null,
    }
}

fn date_trunc_microseconds<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    let time = NaiveTime::from_hms_micro(
        source_timestamp.hour(),
        source_timestamp.minute(),
        source_timestamp.second(),
        source_timestamp.nanosecond() / 1_000,
    );
    Datum::Timestamp(NaiveDateTime::new(source_timestamp.date(), time))
}

fn date_trunc_milliseconds<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    let time = NaiveTime::from_hms_milli(
        source_timestamp.hour(),
        source_timestamp.minute(),
        source_timestamp.second(),
        source_timestamp.nanosecond() / 1_000_000,
    );
    Datum::Timestamp(NaiveDateTime::new(source_timestamp.date(), time))
}

fn date_trunc_second<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
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

fn date_trunc_minute<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    Datum::Timestamp(NaiveDateTime::new(
        source_timestamp.date(),
        NaiveTime::from_hms(source_timestamp.hour(), source_timestamp.minute(), 0),
    ))
}

fn date_trunc_hour<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    Datum::Timestamp(NaiveDateTime::new(
        source_timestamp.date(),
        NaiveTime::from_hms(source_timestamp.hour(), 0, 0),
    ))
}

fn date_trunc_day<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    Datum::Timestamp(NaiveDateTime::new(
        source_timestamp.date(),
        NaiveTime::from_hms(0, 0, 0),
    ))
}

fn date_trunc_week<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
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

fn date_trunc_month<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    Datum::Timestamp(NaiveDateTime::new(
        NaiveDate::from_ymd(source_timestamp.year(), source_timestamp.month(), 1),
        NaiveTime::from_hms(0, 0, 0),
    ))
}

fn date_trunc_quarter<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
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

fn date_trunc_year<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    Datum::Timestamp(NaiveDateTime::new(
        NaiveDate::from_ymd(source_timestamp.year(), 1, 1),
        NaiveTime::from_hms(0, 0, 0),
    ))
}

fn date_trunc_decade<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
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

fn date_trunc_century<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    // Expects the first year of the century, meaning 2001 instead of 2000.
    let century = source_timestamp.year() - ((source_timestamp.year() % 100) - 1);
    Datum::Timestamp(NaiveDateTime::new(
        NaiveDate::from_ymd(century, 1, 1),
        NaiveTime::from_hms(0, 0, 0),
    ))
}

fn date_trunc_millennium<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    let source_timestamp = a.unwrap_timestamp();
    // Expects the first year of the millennium, meaning 2001 instead of 2000.
    let millennium = source_timestamp.year() - ((source_timestamp.year() % 1000) - 1);
    Datum::Timestamp(NaiveDateTime::new(
        NaiveDate::from_ymd(millennium, 1, 1),
        NaiveTime::from_hms(0, 0, 0),
    ))
}

#[derive(Ord, PartialOrd, Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
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
    CeilDecimal,
    FloorDecimal,
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
    ToChar,
    DateTrunc,
    CastFloat32ToDecimal,
    CastFloat64ToDecimal,
    CastDecimalToString,
    JsonbGetInt64,
    JsonbGetString,
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
    pub fn func(
        self,
    ) -> for<'a> fn(Datum<'a>, Datum<'a>, &EvalEnv, &mut PackableRow<'a>) -> Datum<'a> {
        match self {
            BinaryFunc::And => and,
            BinaryFunc::Or => or,
            BinaryFunc::AddInt32 => add_int32,
            BinaryFunc::AddInt64 => add_int64,
            BinaryFunc::AddFloat32 => add_float32,
            BinaryFunc::AddFloat64 => add_float64,
            BinaryFunc::AddTimestampInterval => add_timestamp_interval,
            BinaryFunc::AddTimestampTzInterval => add_timestamptz_interval,
            BinaryFunc::AddDecimal => add_decimal,
            BinaryFunc::SubInt32 => sub_int32,
            BinaryFunc::SubInt64 => sub_int64,
            BinaryFunc::SubFloat32 => sub_float32,
            BinaryFunc::SubFloat64 => sub_float64,
            BinaryFunc::SubTimestampInterval => sub_timestamp_interval,
            BinaryFunc::SubTimestampTzInterval => sub_timestamptz_interval,
            BinaryFunc::SubDecimal => sub_decimal,
            BinaryFunc::MulInt32 => mul_int32,
            BinaryFunc::MulInt64 => mul_int64,
            BinaryFunc::MulFloat32 => mul_float32,
            BinaryFunc::MulFloat64 => mul_float64,
            BinaryFunc::MulDecimal => mul_decimal,
            BinaryFunc::DivInt32 => div_int32,
            BinaryFunc::DivInt64 => div_int64,
            BinaryFunc::DivFloat32 => div_float32,
            BinaryFunc::DivFloat64 => div_float64,
            BinaryFunc::DivDecimal => div_decimal,
            BinaryFunc::CeilDecimal => ceil_decimal,
            BinaryFunc::FloorDecimal => floor_decimal,
            BinaryFunc::ModInt32 => mod_int32,
            BinaryFunc::ModInt64 => mod_int64,
            BinaryFunc::ModFloat32 => mod_float32,
            BinaryFunc::ModFloat64 => mod_float64,
            BinaryFunc::ModDecimal => mod_decimal,
            BinaryFunc::Eq => eq,
            BinaryFunc::NotEq => not_eq,
            BinaryFunc::Lt => lt,
            BinaryFunc::Lte => lte,
            BinaryFunc::Gt => gt,
            BinaryFunc::Gte => gte,
            BinaryFunc::MatchRegex => match_regex,
            BinaryFunc::ToChar => to_char,
            BinaryFunc::DateTrunc => date_trunc,
            BinaryFunc::CastFloat32ToDecimal => cast_float32_to_decimal,
            BinaryFunc::CastFloat64ToDecimal => cast_float64_to_decimal,
            BinaryFunc::CastDecimalToString => cast_decimal_to_string,
            BinaryFunc::JsonbGetInt64 => jsonb_get_int64,
            BinaryFunc::JsonbGetString => jsonb_get_string,
        }
    }

    pub fn output_type(self, input1_type: ColumnType, input2_type: ColumnType) -> ColumnType {
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

            ToChar => ColumnType::new(ScalarType::String).nullable(false),

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
            FloorDecimal | CeilDecimal => match input1_type.scalar_type {
                ScalarType::Null => ColumnType::new(ScalarType::Null),
                ScalarType::Decimal(prec, scale) => {
                    ColumnType::new(ScalarType::Decimal(prec, scale)).nullable(false)
                }
                _ => unreachable!("Got invalid type as first argument of floor/ceil decimal"),
            },
            CastFloat32ToDecimal | CastFloat64ToDecimal => match input2_type.scalar_type {
                ScalarType::Null => ColumnType::new(ScalarType::Null),
                ScalarType::Decimal(_, s) => {
                    ColumnType::new(ScalarType::Decimal(MAX_DECIMAL_PRECISION, s)).nullable(true)
                }
                _ => unreachable!(),
            },
            CastDecimalToString => ColumnType::new(ScalarType::String).nullable(in_nullable),

            AddTimestampInterval
            | SubTimestampInterval
            | AddTimestampTzInterval
            | SubTimestampTzInterval => input1_type,

            DateTrunc => ColumnType::new(ScalarType::Timestamp).nullable(true),

            JsonbGetInt64 | JsonbGetString => ColumnType::new(ScalarType::Jsonb).nullable(true),
        }
    }

    /// Whether the function output is NULL if any of its inputs are NULL.
    pub fn propagates_nulls(self) -> bool {
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
            BinaryFunc::CeilDecimal => f.write_str("ceildec"),
            BinaryFunc::FloorDecimal => f.write_str("floordec"),
            BinaryFunc::Eq => f.write_str("="),
            BinaryFunc::NotEq => f.write_str("!="),
            BinaryFunc::Lt => f.write_str("<"),
            BinaryFunc::Lte => f.write_str("<="),
            BinaryFunc::Gt => f.write_str(">"),
            BinaryFunc::Gte => f.write_str(">="),
            BinaryFunc::MatchRegex => f.write_str("~"),
            BinaryFunc::ToChar => f.write_str("to_char"),
            BinaryFunc::DateTrunc => f.write_str("date_trunc"),
            BinaryFunc::CastFloat32ToDecimal => f.write_str("f32todec"),
            BinaryFunc::CastFloat64ToDecimal => f.write_str("f64todec"),
            BinaryFunc::CastDecimalToString => f.write_str("dectostr"),
            BinaryFunc::JsonbGetInt64 => f.write_str("->i64"),
            BinaryFunc::JsonbGetString => f.write_str("->str"),
        }
    }
}

pub fn is_null<'a>(a: Datum<'a>, _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    Datum::from(a == Datum::Null)
}

#[derive(Ord, PartialOrd, Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
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
    CeilFloat32,
    CeilFloat64,
    FloorFloat32,
    FloorFloat64,
    Ascii,
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
}

impl UnaryFunc {
    pub fn func(self) -> for<'a> fn(Datum<'a>, &EvalEnv, &mut PackableRow<'a>) -> Datum<'a> {
        match self {
            UnaryFunc::Not => not,
            UnaryFunc::IsNull => is_null,
            UnaryFunc::NegInt32 => neg_int32,
            UnaryFunc::NegInt64 => neg_int64,
            UnaryFunc::NegFloat32 => neg_float32,
            UnaryFunc::NegFloat64 => neg_float64,
            UnaryFunc::NegDecimal => neg_decimal,
            UnaryFunc::AbsInt32 => abs_int32,
            UnaryFunc::AbsInt64 => abs_int64,
            UnaryFunc::AbsFloat32 => abs_float32,
            UnaryFunc::AbsFloat64 => abs_float64,
            UnaryFunc::CastBoolToString => cast_bool_to_string,
            UnaryFunc::CastInt32ToBool => cast_int32_to_bool,
            UnaryFunc::CastInt32ToFloat32 => cast_int32_to_float32,
            UnaryFunc::CastInt32ToFloat64 => cast_int32_to_float64,
            UnaryFunc::CastInt32ToInt64 => cast_int32_to_int64,
            UnaryFunc::CastInt32ToDecimal => cast_int32_to_decimal,
            UnaryFunc::CastInt32ToString => cast_int32_to_string,
            UnaryFunc::CastInt64ToInt32 => cast_int64_to_int32,
            UnaryFunc::CastInt64ToBool => cast_int64_to_bool,
            UnaryFunc::CastInt64ToDecimal => cast_int64_to_decimal,
            UnaryFunc::CastInt64ToFloat32 => cast_int64_to_float32,
            UnaryFunc::CastInt64ToFloat64 => cast_int64_to_float64,
            UnaryFunc::CastInt64ToString => cast_int64_to_string,
            UnaryFunc::CastFloat32ToInt64 => cast_float32_to_int64,
            UnaryFunc::CastFloat32ToFloat64 => cast_float32_to_float64,
            UnaryFunc::CastFloat32ToString => cast_float32_to_string,
            UnaryFunc::CastFloat64ToInt64 => cast_float64_to_int64,
            UnaryFunc::CastFloat64ToString => cast_float64_to_string,
            UnaryFunc::CastDecimalToInt32 => cast_decimal_to_int32,
            UnaryFunc::CastDecimalToInt64 => cast_decimal_to_int64,
            UnaryFunc::CastSignificandToFloat32 => cast_significand_to_float32,
            UnaryFunc::CastSignificandToFloat64 => cast_significand_to_float64,
            UnaryFunc::CastStringToFloat64 => cast_string_to_float64,
            UnaryFunc::CastStringToBytes => cast_string_to_bytes,
            UnaryFunc::CastDateToTimestamp => cast_date_to_timestamp,
            UnaryFunc::CastDateToTimestampTz => cast_date_to_timestamptz,
            UnaryFunc::CastDateToString => cast_date_to_string,
            UnaryFunc::CastTimestampToDate => cast_timestamp_to_date,
            UnaryFunc::CastTimestampToTimestampTz => cast_timestamp_to_timestamptz,
            UnaryFunc::CastTimestampToString => cast_timestamp_to_string,
            UnaryFunc::CastTimestampTzToDate => cast_timestamptz_to_date,
            UnaryFunc::CastTimestampTzToTimestamp => cast_timestamptz_to_timestamp,
            UnaryFunc::CastTimestampTzToString => cast_timestamptz_to_string,
            UnaryFunc::CastIntervalToString => cast_interval_to_string,
            UnaryFunc::CastBytesToString => cast_bytes_to_string,
            UnaryFunc::CastStringToJsonb => cast_string_to_jsonb,
            UnaryFunc::CastJsonbToString => cast_jsonb_to_string,
            UnaryFunc::CeilFloat32 => ceil_float32,
            UnaryFunc::CeilFloat64 => ceil_float64,
            UnaryFunc::FloorFloat32 => floor_float32,
            UnaryFunc::FloorFloat64 => floor_float64,
            UnaryFunc::SqrtFloat32 => sqrt_float32,
            UnaryFunc::SqrtFloat64 => sqrt_float64,
            UnaryFunc::Ascii => ascii,
            UnaryFunc::ExtractIntervalYear => extract_interval_year,
            UnaryFunc::ExtractIntervalMonth => extract_interval_month,
            UnaryFunc::ExtractIntervalDay => extract_interval_day,
            UnaryFunc::ExtractIntervalHour => extract_interval_hour,
            UnaryFunc::ExtractIntervalMinute => extract_interval_minute,
            UnaryFunc::ExtractIntervalSecond => extract_interval_second,
            UnaryFunc::ExtractTimestampYear => extract_timestamp_year,
            UnaryFunc::ExtractTimestampQuarter => extract_timestamp_quarter,
            UnaryFunc::ExtractTimestampMonth => extract_timestamp_month,
            UnaryFunc::ExtractTimestampDay => extract_timestamp_day,
            UnaryFunc::ExtractTimestampHour => extract_timestamp_hour,
            UnaryFunc::ExtractTimestampMinute => extract_timestamp_minute,
            UnaryFunc::ExtractTimestampSecond => extract_timestamp_second,
            UnaryFunc::ExtractTimestampWeek => extract_timestamp_week,
            UnaryFunc::ExtractTimestampDayOfYear => extract_timestamp_dayofyear,
            UnaryFunc::ExtractTimestampDayOfWeek => extract_timestamp_dayofweek,
            UnaryFunc::ExtractTimestampIsoDayOfWeek => extract_timestamp_isodayofweek,
            UnaryFunc::ExtractTimestampTzYear => extract_timestamptz_year,
            UnaryFunc::ExtractTimestampTzQuarter => extract_timestamptz_quarter,
            UnaryFunc::ExtractTimestampTzMonth => extract_timestamptz_month,
            UnaryFunc::ExtractTimestampTzDay => extract_timestamptz_day,
            UnaryFunc::ExtractTimestampTzHour => extract_timestamptz_hour,
            UnaryFunc::ExtractTimestampTzMinute => extract_timestamptz_minute,
            UnaryFunc::ExtractTimestampTzSecond => extract_timestamptz_second,
            UnaryFunc::ExtractTimestampTzWeek => extract_timestamptz_week,
            UnaryFunc::ExtractTimestampTzDayOfYear => extract_timestamptz_dayofyear,
            UnaryFunc::ExtractTimestampTzDayOfWeek => extract_timestamptz_dayofweek,
            UnaryFunc::ExtractTimestampTzIsoDayOfWeek => extract_timestamptz_isodayofweek,
            UnaryFunc::DateTrunc(to) => match to {
                DateTruncTo::Micros => date_trunc_microseconds,
                DateTruncTo::Millis => date_trunc_milliseconds,
                DateTruncTo::Second => date_trunc_second,
                DateTruncTo::Minute => date_trunc_minute,
                DateTruncTo::Hour => date_trunc_hour,
                DateTruncTo::Day => date_trunc_day,
                DateTruncTo::Week => date_trunc_week,
                DateTruncTo::Month => date_trunc_month,
                DateTruncTo::Quarter => date_trunc_quarter,
                DateTruncTo::Year => date_trunc_year,
                DateTruncTo::Decade => date_trunc_decade,
                DateTruncTo::Century => date_trunc_century,
                DateTruncTo::Millennium => date_trunc_millennium,
            },
        }
    }

    /// Reports whether this function has a symbolic string representation.
    pub fn display_is_symbolic(self) -> bool {
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

    pub fn output_type(self, input_type: ColumnType) -> ColumnType {
        use UnaryFunc::*;
        let in_nullable = input_type.nullable;
        match self {
            IsNull | CastInt32ToBool | CastInt64ToBool => ColumnType::new(ScalarType::Bool),

            Ascii => ColumnType::new(ScalarType::Int32).nullable(in_nullable),

            CastStringToBytes => ColumnType::new(ScalarType::Bytes).nullable(in_nullable),

            CastBoolToString
            | CastInt32ToString
            | CastInt64ToString
            | CastFloat32ToString
            | CastFloat64ToString
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
            CastJsonbToString => ColumnType::new(ScalarType::String).nullable(true),

            CeilFloat32 | FloorFloat32 => {
                ColumnType::new(ScalarType::Float32).nullable(in_nullable)
            }
            CeilFloat64 | FloorFloat64 => {
                ColumnType::new(ScalarType::Float64).nullable(in_nullable)
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
        }
    }

    /// Whether the function output is NULL if any of its inputs are NULL.
    pub fn propagates_nulls(self) -> bool {
        match self {
            UnaryFunc::IsNull => false,
            _ => true,
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
            UnaryFunc::CeilFloat32 => f.write_str("ceilf32"),
            UnaryFunc::CeilFloat64 => f.write_str("ceilf64"),
            UnaryFunc::FloorFloat32 => f.write_str("floorf32"),
            UnaryFunc::FloorFloat64 => f.write_str("floorf64"),
            UnaryFunc::SqrtFloat32 => f.write_str("sqrtf32"),
            UnaryFunc::SqrtFloat64 => f.write_str("sqrtf64"),
            UnaryFunc::Ascii => f.write_str("ascii"),
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
        }
    }
}

pub fn coalesce<'a>(datums: &[Datum<'a>], _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
    datums
        .iter()
        .find(|d| !d.is_null())
        .cloned()
        .unwrap_or(Datum::Null)
}

pub fn substr<'a>(datums: &[Datum<'a>], _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
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

pub fn length<'a>(datums: &[Datum<'a>], _: &EvalEnv, _: &mut PackableRow<'a>) -> Datum<'a> {
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

pub fn replace<'a>(
    datums: &[Datum<'a>],
    _: &EvalEnv,
    temp_storage: &mut PackableRow<'a>,
) -> Datum<'a> {
    Datum::String(
        temp_storage.push_string(
            &datums[0]
                .unwrap_str()
                .replace(datums[1].unwrap_str(), datums[2].unwrap_str()),
        ),
    )
}

#[derive(Ord, PartialOrd, Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum VariadicFunc {
    Coalesce,
    Substr,
    Length,
    Replace,
}

impl VariadicFunc {
    pub fn func(self) -> for<'a> fn(&[Datum<'a>], &EvalEnv, &mut PackableRow<'a>) -> Datum<'a> {
        match self {
            VariadicFunc::Coalesce => coalesce,
            VariadicFunc::Substr => substr,
            VariadicFunc::Length => length,
            VariadicFunc::Replace => replace,
        }
    }

    pub fn output_type(self, input_types: Vec<ColumnType>) -> ColumnType {
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
            Substr => ColumnType::new(ScalarType::String).nullable(true),
            Length => ColumnType::new(ScalarType::Int32).nullable(true),
            Replace => ColumnType::new(ScalarType::String).nullable(true),
        }
    }

    /// Whether the function output is NULL if any of its inputs are NULL.
    pub fn propagates_nulls(self) -> bool {
        match self {
            VariadicFunc::Coalesce => false,
            _ => true,
        }
    }
}

impl fmt::Display for VariadicFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VariadicFunc::Coalesce => f.write_str("coalesce"),
            VariadicFunc::Substr => f.write_str("substr"),
            VariadicFunc::Length => f.write_str("length"),
            VariadicFunc::Replace => f.write_str("replace"),
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
