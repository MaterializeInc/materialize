// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::cmp;
use std::convert::TryFrom;
use std::fmt;

use chrono::{DateTime, Datelike, NaiveDateTime, Timelike, Utc};
use pretty::{BoxDoc, Doc};
use serde::{Deserialize, Serialize};

pub use crate::like::build_like_regex_from_string;
use repr::decimal::MAX_DECIMAL_PRECISION;
use repr::regex::Regex;
use repr::{ColumnType, Datum, Interval, ScalarType};

pub fn and<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    match (&a, &b) {
        (Datum::False, _) => Datum::False,
        (_, Datum::False) => Datum::False,
        (Datum::Null, _) => Datum::Null,
        (_, Datum::Null) => Datum::Null,
        (Datum::True, Datum::True) => Datum::True,
        _ => panic!("Cannot compute {:?} AND {:?}", a, b),
    }
}

pub fn or<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    match (&a, &b) {
        (Datum::True, _) => Datum::True,
        (_, Datum::True) => Datum::True,
        (Datum::Null, _) => Datum::Null,
        (_, Datum::Null) => Datum::Null,
        (Datum::False, Datum::False) => Datum::False,
        _ => panic!("Cannot compute {:?} OR {:?}", a, b),
    }
}

pub fn not<'a>(a: Datum<'a>) -> Datum<'a> {
    match &a {
        Datum::False => Datum::True,
        Datum::True => Datum::False,
        Datum::Null => Datum::Null,
        _ => panic!("Cannot compute NOT {:?}", a),
    }
}

pub fn abs_int32<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int32().abs())
}

pub fn abs_int64<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int64().abs())
}

pub fn abs_float32<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float32().abs())
}

pub fn abs_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float64().abs())
}

pub fn cast_int32_to_float32<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): is this cast valid?
    Datum::from(a.unwrap_int32() as f32)
}

pub fn cast_int32_to_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): is this cast valid?
    Datum::from(f64::from(a.unwrap_int32()))
}

pub fn cast_int32_to_int64<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(i64::from(a.unwrap_int32()))
}

pub fn cast_int64_to_int32<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): we need to do something better than panicking if the
    // datum doesn't fit in an int32, but what? Poison the whole dataflow?
    // The SQL standard says this an error, but runtime errors are complicated
    // in a streaming setting.
    Datum::from(i32::try_from(a.unwrap_int64()).unwrap())
}

pub fn cast_int32_to_decimal<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(i128::from(a.unwrap_int32()))
}

pub fn cast_int64_to_decimal<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(i128::from(a.unwrap_int64()))
}

pub fn cast_int64_to_float32<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): is this cast valid?
    Datum::from(a.unwrap_int64() as f32)
}

pub fn cast_int64_to_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): is this cast valid?
    Datum::from(a.unwrap_int64() as f64)
}

pub fn cast_float32_to_int64<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): this is undefined behavior if the f32 doesn't fit in an
    // i64 (https://github.com/rust-lang/rust/issues/10184).
    Datum::from(a.unwrap_float32() as i64)
}

pub fn cast_float32_to_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): is this cast valid?
    Datum::from(f64::from(a.unwrap_float32()))
}

pub fn cast_float64_to_int64<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): this is undefined behavior if the f32 doesn't fit in an
    // i64 (https://github.com/rust-lang/rust/issues/10184).
    Datum::from(a.unwrap_float64() as i64)
}

pub fn cast_decimal_to_int32<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_decimal().as_i128() as i32)
}

pub fn cast_decimal_to_int64<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_decimal().as_i128() as i64)
}

pub fn cast_decimal_to_float32<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_decimal().as_i128() as f32)
}

pub fn cast_decimal_to_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_decimal().as_i128() as f64)
}

pub fn cast_date_to_timestamp<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::Timestamp(a.unwrap_date().and_hms(0, 0, 0))
}

pub fn cast_date_to_timestamptz<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::TimestampTz(DateTime::<Utc>::from_utc(
        a.unwrap_date().and_hms(0, 0, 0),
        Utc,
    ))
}

pub fn cast_timestamp_to_timestamptz<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::TimestampTz(DateTime::<Utc>::from_utc(a.unwrap_timestamp(), Utc))
}

pub fn add_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int32() + b.unwrap_int32())
}

pub fn add_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int64() + b.unwrap_int64())
}

pub fn add_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float32() + b.unwrap_float32())
}

pub fn add_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float64() + b.unwrap_float64())
}

pub fn add_timestamp_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }

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

pub fn add_timestamptz_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }

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

pub fn sub_timestamp_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
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

pub fn sub_timestamptz_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
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

pub fn add_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_decimal() + b.unwrap_decimal())
}

pub fn sub_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int32() - b.unwrap_int32())
}

pub fn sub_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int64() - b.unwrap_int64())
}

pub fn sub_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float32() - b.unwrap_float32())
}

pub fn sub_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float64() - b.unwrap_float64())
}

pub fn sub_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_decimal() - b.unwrap_decimal())
}

pub fn mul_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int32() * b.unwrap_int32())
}

pub fn mul_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int64() * b.unwrap_int64())
}

pub fn mul_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float32() * b.unwrap_float32())
}

pub fn mul_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float64() * b.unwrap_float64())
}

pub fn mul_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_decimal() * b.unwrap_decimal())
}

// TODO(jamii) we don't currently have any way of reporting errors from functions, so for now we just adopt sqlite's approach 1/0 = null

pub fn div_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    let b = b.unwrap_int32();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_int32() / b)
    }
}

pub fn div_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    let b = b.unwrap_int64();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_int64() / b)
    }
}

pub fn div_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    let b = b.unwrap_float32();
    if b == 0.0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_float32() / b)
    }
}

pub fn div_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    let b = b.unwrap_float64();
    if b == 0.0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_float64() / b)
    }
}

pub fn div_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    let b = b.unwrap_decimal();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_decimal() / b)
    }
}

pub fn mod_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    let b = b.unwrap_int32();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_int32() % b)
    }
}

pub fn mod_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    let b = b.unwrap_int64();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_int64() % b)
    }
}

pub fn mod_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    let b = b.unwrap_float32();
    if b == 0.0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_float32() % b)
    }
}

pub fn mod_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    let b = b.unwrap_float64();
    if b == 0.0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_float64() % b)
    }
}

pub fn mod_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    let b = b.unwrap_decimal();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_decimal() % b)
    }
}

pub fn neg_int32<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(-a.unwrap_int32())
}

pub fn neg_int64<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(-a.unwrap_int64())
}

pub fn neg_float32<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(-a.unwrap_float32())
}

pub fn neg_float64<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(-a.unwrap_float64())
}

pub fn neg_decimal<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(-a.unwrap_decimal())
}

pub fn eq<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a == b)
}

pub fn not_eq<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a != b)
}

pub fn lt<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a < b)
}

pub fn lte<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a <= b)
}

pub fn gt<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a > b)
}

pub fn gte<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a >= b)
}

pub fn match_regex<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
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
    if a.is_null() {
        return Datum::Null;
    }
    let haystack = a.unwrap_str();
    Datum::from(needle.is_match(haystack))
}

pub fn ascii<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    match a.unwrap_str().chars().next() {
        None => Datum::Int32(0),
        Some(v) => Datum::Int32(v as i32),
    }
}

pub fn extract_interval_year<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_interval().years())
}

pub fn extract_interval_month<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_interval().months())
}

pub fn extract_interval_day<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_interval().days())
}

pub fn extract_interval_hour<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_interval().hours())
}

pub fn extract_interval_minute<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_interval().minutes())
}

pub fn extract_interval_second<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_interval().seconds())
}

pub fn extract_timestamp_year<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(f64::from(a.unwrap_timestamp().year()))
}

pub fn extract_timestamptz_year<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(f64::from(a.unwrap_timestamptz().year()))
}

pub fn extract_timestamp_month<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(f64::from(a.unwrap_timestamp().month()))
}

pub fn extract_timestamptz_month<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(f64::from(a.unwrap_timestamptz().month()))
}

pub fn extract_timestamp_day<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(f64::from(a.unwrap_timestamp().day()))
}

pub fn extract_timestamptz_day<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(f64::from(a.unwrap_timestamptz().day()))
}

pub fn extract_timestamp_hour<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(f64::from(a.unwrap_timestamp().hour()))
}

pub fn extract_timestamptz_hour<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(f64::from(a.unwrap_timestamptz().hour()))
}

pub fn extract_timestamp_minute<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(f64::from(a.unwrap_timestamp().minute()))
}

pub fn extract_timestamptz_minute<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(f64::from(a.unwrap_timestamptz().minute()))
}

pub fn extract_timestamp_second<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    let a = a.unwrap_timestamp();
    let s = f64::from(a.second());
    let ns = f64::from(a.nanosecond()) / 1e9;
    Datum::from(s + ns)
}

pub fn extract_timestamptz_second<'a>(a: Datum<'a>) -> Datum<'a> {
    if a.is_null() {
        return Datum::Null;
    }
    let a = a.unwrap_timestamptz();
    let s = f64::from(a.second());
    let ns = f64::from(a.nanosecond()) / 1e9;
    Datum::from(s + ns)
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
}

impl BinaryFunc {
    pub fn func(self) -> for<'a> fn(Datum<'a>, Datum<'a>) -> Datum<'a> {
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

            AddTimestampInterval
            | SubTimestampInterval
            | AddTimestampTzInterval
            | SubTimestampTzInterval => input1_type,
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
            BinaryFunc::Eq => f.write_str("="),
            BinaryFunc::NotEq => f.write_str("!="),
            BinaryFunc::Lt => f.write_str("<"),
            BinaryFunc::Lte => f.write_str("<="),
            BinaryFunc::Gt => f.write_str(">"),
            BinaryFunc::Gte => f.write_str(">="),
            BinaryFunc::MatchRegex => f.write_str("~"),
        }
    }
}

impl<'a> From<&'a BinaryFunc> for Doc<'a, BoxDoc<'a, ()>, ()> {
    fn from(f: &'a BinaryFunc) -> Doc<'a, BoxDoc<'a, ()>, ()> {
        Doc::text(f.to_string())
    }
}

pub fn is_null<'a>(a: Datum<'a>) -> Datum<'a> {
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
    AbsInt32,
    AbsInt64,
    AbsFloat32,
    AbsFloat64,
    CastInt32ToFloat32,
    CastInt32ToFloat64,
    CastInt32ToInt64,
    CastInt64ToInt32,
    CastInt32ToDecimal,
    CastInt64ToDecimal,
    CastInt64ToFloat32,
    CastInt64ToFloat64,
    CastFloat32ToInt64,
    CastFloat32ToFloat64,
    CastFloat64ToInt64,
    CastDecimalToInt32,
    CastDecimalToInt64,
    CastDecimalToFloat32,
    CastDecimalToFloat64,
    CastDateToTimestamp,
    CastDateToTimestampTz,
    CastTimestampToTimestampTz,
    Ascii,
    ExtractIntervalYear,
    ExtractIntervalMonth,
    ExtractIntervalDay,
    ExtractIntervalHour,
    ExtractIntervalMinute,
    ExtractIntervalSecond,
    ExtractTimestampYear,
    ExtractTimestampMonth,
    ExtractTimestampDay,
    ExtractTimestampHour,
    ExtractTimestampMinute,
    ExtractTimestampSecond,
    ExtractTimestampTzYear,
    ExtractTimestampTzMonth,
    ExtractTimestampTzDay,
    ExtractTimestampTzHour,
    ExtractTimestampTzMinute,
    ExtractTimestampTzSecond,
}

impl UnaryFunc {
    pub fn func(self) -> for<'a> fn(Datum<'a>) -> Datum<'a> {
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
            UnaryFunc::CastInt32ToFloat32 => cast_int32_to_float32,
            UnaryFunc::CastInt32ToFloat64 => cast_int32_to_float64,
            UnaryFunc::CastInt32ToInt64 => cast_int32_to_int64,
            UnaryFunc::CastInt64ToInt32 => cast_int64_to_int32,
            UnaryFunc::CastInt32ToDecimal => cast_int32_to_decimal,
            UnaryFunc::CastInt64ToDecimal => cast_int64_to_decimal,
            UnaryFunc::CastInt64ToFloat32 => cast_int64_to_float32,
            UnaryFunc::CastInt64ToFloat64 => cast_int64_to_float64,
            UnaryFunc::CastFloat32ToInt64 => cast_float32_to_int64,
            UnaryFunc::CastFloat32ToFloat64 => cast_float32_to_float64,
            UnaryFunc::CastFloat64ToInt64 => cast_float64_to_int64,
            UnaryFunc::CastDecimalToInt32 => cast_decimal_to_int32,
            UnaryFunc::CastDecimalToInt64 => cast_decimal_to_int64,
            UnaryFunc::CastDecimalToFloat32 => cast_decimal_to_float32,
            UnaryFunc::CastDecimalToFloat64 => cast_decimal_to_float64,
            UnaryFunc::CastDateToTimestamp => cast_date_to_timestamp,
            UnaryFunc::CastDateToTimestampTz => cast_date_to_timestamptz,
            UnaryFunc::CastTimestampToTimestampTz => cast_timestamp_to_timestamptz,
            UnaryFunc::Ascii => ascii,
            UnaryFunc::ExtractIntervalYear => extract_interval_year,
            UnaryFunc::ExtractIntervalMonth => extract_interval_month,
            UnaryFunc::ExtractIntervalDay => extract_interval_day,
            UnaryFunc::ExtractIntervalHour => extract_interval_hour,
            UnaryFunc::ExtractIntervalMinute => extract_interval_minute,
            UnaryFunc::ExtractIntervalSecond => extract_interval_second,
            UnaryFunc::ExtractTimestampYear => extract_timestamp_year,
            UnaryFunc::ExtractTimestampMonth => extract_timestamp_month,
            UnaryFunc::ExtractTimestampDay => extract_timestamp_day,
            UnaryFunc::ExtractTimestampHour => extract_timestamp_hour,
            UnaryFunc::ExtractTimestampMinute => extract_timestamp_minute,
            UnaryFunc::ExtractTimestampSecond => extract_timestamp_second,
            UnaryFunc::ExtractTimestampTzYear => extract_timestamptz_year,
            UnaryFunc::ExtractTimestampTzMonth => extract_timestamptz_month,
            UnaryFunc::ExtractTimestampTzDay => extract_timestamptz_day,
            UnaryFunc::ExtractTimestampTzHour => extract_timestamptz_hour,
            UnaryFunc::ExtractTimestampTzMinute => extract_timestamptz_minute,
            UnaryFunc::ExtractTimestampTzSecond => extract_timestamptz_second,
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
            IsNull => ColumnType::new(ScalarType::Bool),

            Ascii => ColumnType::new(ScalarType::Int32).nullable(in_nullable),

            CastInt32ToFloat32 => ColumnType::new(ScalarType::Float32).nullable(in_nullable),
            CastInt32ToFloat64 => ColumnType::new(ScalarType::Float64).nullable(in_nullable),
            CastInt64ToInt32 => ColumnType::new(ScalarType::Int32).nullable(in_nullable),
            CastInt32ToInt64 => ColumnType::new(ScalarType::Int64).nullable(in_nullable),
            CastInt32ToDecimal => ColumnType::new(ScalarType::Decimal(20, 0)).nullable(in_nullable),
            CastInt64ToDecimal => ColumnType::new(ScalarType::Decimal(10, 0)).nullable(in_nullable),
            CastInt64ToFloat32 => ColumnType::new(ScalarType::Float32).nullable(in_nullable),
            CastInt64ToFloat64 => ColumnType::new(ScalarType::Float64).nullable(in_nullable),
            CastFloat32ToInt64 => ColumnType::new(ScalarType::Int64).nullable(in_nullable),
            CastFloat32ToFloat64 => ColumnType::new(ScalarType::Float64).nullable(in_nullable),
            CastFloat64ToInt64 => ColumnType::new(ScalarType::Int64).nullable(in_nullable),
            CastDecimalToInt32 => ColumnType::new(ScalarType::Int32).nullable(in_nullable),
            CastDecimalToInt64 => ColumnType::new(ScalarType::Int64).nullable(in_nullable),
            CastDecimalToFloat32 => ColumnType::new(ScalarType::Float32).nullable(in_nullable),
            CastDecimalToFloat64 => ColumnType::new(ScalarType::Float64).nullable(in_nullable),
            CastDateToTimestamp => ColumnType::new(ScalarType::Timestamp).nullable(in_nullable),
            CastDateToTimestampTz => ColumnType::new(ScalarType::TimestampTz).nullable(in_nullable),
            CastTimestampToTimestampTz => {
                ColumnType::new(ScalarType::TimestampTz).nullable(in_nullable)
            }

            Not | NegInt32 | NegInt64 | NegFloat32 | NegFloat64 | NegDecimal | AbsInt32
            | AbsInt64 | AbsFloat32 | AbsFloat64 => input_type,

            ExtractIntervalYear
            | ExtractIntervalMonth
            | ExtractIntervalDay
            | ExtractIntervalHour
            | ExtractIntervalMinute
            | ExtractIntervalSecond
            | ExtractTimestampYear
            | ExtractTimestampMonth
            | ExtractTimestampDay
            | ExtractTimestampHour
            | ExtractTimestampMinute
            | ExtractTimestampSecond
            | ExtractTimestampTzYear
            | ExtractTimestampTzMonth
            | ExtractTimestampTzDay
            | ExtractTimestampTzHour
            | ExtractTimestampTzMinute
            | ExtractTimestampTzSecond => {
                ColumnType::new(ScalarType::Float64).nullable(in_nullable)
            }
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
            UnaryFunc::CastInt32ToFloat32 => f.write_str("i32tof32"),
            UnaryFunc::CastInt32ToFloat64 => f.write_str("i32tof64"),
            UnaryFunc::CastInt32ToInt64 => f.write_str("i32toi64"),
            UnaryFunc::CastInt64ToInt32 => f.write_str("i64toi32"),
            UnaryFunc::CastInt32ToDecimal => f.write_str("i32todec"),
            UnaryFunc::CastInt64ToDecimal => f.write_str("i64todec"),
            UnaryFunc::CastInt64ToFloat32 => f.write_str("i64tof32"),
            UnaryFunc::CastInt64ToFloat64 => f.write_str("i64tof64"),
            UnaryFunc::CastFloat32ToInt64 => f.write_str("f32toi64"),
            UnaryFunc::CastFloat32ToFloat64 => f.write_str("f32tof64"),
            UnaryFunc::CastFloat64ToInt64 => f.write_str("f64toi64"),
            UnaryFunc::CastDecimalToInt32 => f.write_str("dectoi32"),
            UnaryFunc::CastDecimalToInt64 => f.write_str("dectoi64"),
            UnaryFunc::CastDecimalToFloat32 => f.write_str("dectof32"),
            UnaryFunc::CastDecimalToFloat64 => f.write_str("dectof64"),
            UnaryFunc::CastDateToTimestamp => f.write_str("datetots"),
            UnaryFunc::CastDateToTimestampTz => f.write_str("datetotstz"),
            UnaryFunc::CastTimestampToTimestampTz => f.write_str("tstotstz"),
            UnaryFunc::Ascii => f.write_str("ascii"),
            UnaryFunc::ExtractIntervalYear => f.write_str("ivextractyear"),
            UnaryFunc::ExtractIntervalMonth => f.write_str("ivextractmonth"),
            UnaryFunc::ExtractIntervalDay => f.write_str("ivextractday"),
            UnaryFunc::ExtractIntervalHour => f.write_str("ivextracthour"),
            UnaryFunc::ExtractIntervalMinute => f.write_str("ivextractminute"),
            UnaryFunc::ExtractIntervalSecond => f.write_str("ivextractsecond"),
            UnaryFunc::ExtractTimestampYear => f.write_str("tsextractyear"),
            UnaryFunc::ExtractTimestampMonth => f.write_str("tsextractmonth"),
            UnaryFunc::ExtractTimestampDay => f.write_str("tsextractday"),
            UnaryFunc::ExtractTimestampHour => f.write_str("tsextracthour"),
            UnaryFunc::ExtractTimestampMinute => f.write_str("tsextractminute"),
            UnaryFunc::ExtractTimestampSecond => f.write_str("tsextractsecond"),
            UnaryFunc::ExtractTimestampTzYear => f.write_str("tstzextractyear"),
            UnaryFunc::ExtractTimestampTzMonth => f.write_str("tstzextractmonth"),
            UnaryFunc::ExtractTimestampTzDay => f.write_str("tstzextractday"),
            UnaryFunc::ExtractTimestampTzHour => f.write_str("tstzextracthour"),
            UnaryFunc::ExtractTimestampTzMinute => f.write_str("tstzextractminute"),
            UnaryFunc::ExtractTimestampTzSecond => f.write_str("tstzextractsecond"),
        }
    }
}

impl<'a> From<&'a UnaryFunc> for Doc<'a, BoxDoc<'a, ()>, ()> {
    fn from(f: &'a UnaryFunc) -> Doc<'a, BoxDoc<'a, ()>, ()> {
        Doc::text(f.to_string())
    }
}

pub fn coalesce<'a>(datums: &[Datum<'a>]) -> Datum<'a> {
    datums
        .iter()
        .find(|d| !d.is_null())
        .copied()
        .unwrap_or(Datum::Null)
}

pub fn substr<'a>(datums: &[Datum<'a>]) -> Datum<'a> {
    if datums.iter().any(|d| d.is_null()) {
        return Datum::Null;
    }
    let string = datums[0].unwrap_str();
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
        Datum::from(&string[start_in_bytes..start_in_bytes + length_in_bytes])
    } else {
        Datum::from(&string[start_in_bytes..])
    }
}

#[derive(Ord, PartialOrd, Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum VariadicFunc {
    Coalesce,
    Substr,
}

impl VariadicFunc {
    pub fn func(self) -> for<'a> fn(&[Datum<'a>]) -> Datum<'a> {
        match self {
            VariadicFunc::Coalesce => coalesce,
            VariadicFunc::Substr => substr,
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
        }
    }
}

impl fmt::Display for VariadicFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VariadicFunc::Coalesce => f.write_str("coalesce"),
            VariadicFunc::Substr => f.write_str("substr"),
        }
    }
}

impl<'a> From<&'a VariadicFunc> for Doc<'a, BoxDoc<'a, ()>, ()> {
    fn from(f: &'a VariadicFunc) -> Doc<'a, BoxDoc<'a, ()>, ()> {
        Doc::text(f.to_string())
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
