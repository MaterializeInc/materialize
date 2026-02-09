// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Portions of this file are derived from the PostgreSQL project. The original
// source code is subject to the terms of the PostgreSQL license, a copy of
// which can be found in the LICENSE file at the root of this repository.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::convert::{TryFrom, TryInto};
use std::str::FromStr;
use std::{iter, str};

use ::encoding::DecoderTrap;
use ::encoding::label::encoding_from_whatwg_label;
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc};
use chrono_tz::{OffsetComponents, OffsetName, Tz};
use dec::OrderedDecimal;
use itertools::Itertools;
use md5::{Digest, Md5};
use mz_expr_derive::sqlfunc;
use mz_ore::cast::{self, CastFrom};
use mz_ore::fmt::FormatBuffer;
use mz_ore::lex::LexBuf;
use mz_ore::option::OptionExt;
use mz_pgrepr::Type;
use mz_pgtz::timezone::{Timezone, TimezoneSpec};
use mz_repr::adt::array::{Array, ArrayDimension};
use mz_repr::adt::date::Date;
use mz_repr::adt::interval::{Interval, RoundBehavior};
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::adt::numeric::{self, Numeric};
use mz_repr::adt::range::Range;
use mz_repr::adt::regex::Regex;
use mz_repr::adt::timestamp::{CheckedTimestamp, TimestampLike};
use mz_repr::{
    ArrayRustType, Datum, DatumList, DatumMap, DatumType, ExcludeNull, Row, RowArena,
    SqlScalarType, strconv,
};
use mz_sql_parser::ast::display::FormatMode;
use mz_sql_pretty::{PrettyConfig, pretty_str};
use num::traits::CheckedNeg;
use sha1::Sha1;
use sha2::{Sha224, Sha256, Sha384, Sha512};
use subtle::ConstantTimeEq;

use crate::scalar::func::format::DateTimeFormat;
use crate::{EvalError, like_pattern};

#[macro_use]
mod macros;
mod binary;
mod encoding;
pub(crate) mod format;
pub(crate) mod impls;
mod unary;
mod unmaterializable;
mod variadic;

pub use binary::BinaryFunc;
pub use impls::*;
pub use unary::{EagerUnaryFunc, LazyUnaryFunc, UnaryFunc};
pub use unmaterializable::UnmaterializableFunc;
pub use variadic::VariadicFunc;

/// The maximum size of the result strings of certain string functions, such as `repeat` and `lpad`.
/// Chosen to be the smallest number to keep our tests passing without changing. 100MiB is probably
/// higher than what we want, but it's better than no limit.
///
/// Note: This number appears in our user-facing documentation in the function reference for every
/// function where it applies.
pub const MAX_STRING_FUNC_RESULT_BYTES: usize = 1024 * 1024 * 100;

pub fn jsonb_stringify<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Option<&'a str> {
    match a {
        Datum::JsonNull => None,
        Datum::String(s) => Some(s),
        _ => {
            let s = cast_jsonb_to_string(JsonbRef::from_datum(a));
            Some(temp_storage.push_string(s))
        }
    }
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "+",
    propagates_nulls = true
)]
fn add_int16(a: i16, b: i16) -> Result<i16, EvalError> {
    a.checked_add(b).ok_or(EvalError::NumericFieldOverflow)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "+",
    propagates_nulls = true
)]
fn add_int32(a: i32, b: i32) -> Result<i32, EvalError> {
    a.checked_add(b).ok_or(EvalError::NumericFieldOverflow)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "+",
    propagates_nulls = true
)]
fn add_int64(a: i64, b: i64) -> Result<i64, EvalError> {
    a.checked_add(b).ok_or(EvalError::NumericFieldOverflow)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "+",
    propagates_nulls = true
)]
fn add_uint16(a: u16, b: u16) -> Result<u16, EvalError> {
    a.checked_add(b)
        .ok_or_else(|| EvalError::UInt16OutOfRange(format!("{a} + {b}").into()))
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "+",
    propagates_nulls = true
)]
fn add_uint32(a: u32, b: u32) -> Result<u32, EvalError> {
    a.checked_add(b)
        .ok_or_else(|| EvalError::UInt32OutOfRange(format!("{a} + {b}").into()))
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "+",
    propagates_nulls = true
)]
fn add_uint64(a: u64, b: u64) -> Result<u64, EvalError> {
    a.checked_add(b)
        .ok_or_else(|| EvalError::UInt64OutOfRange(format!("{a} + {b}").into()))
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "+",
    propagates_nulls = true
)]
fn add_float32(a: f32, b: f32) -> Result<f32, EvalError> {
    let sum = a + b;
    if sum.is_infinite() && !a.is_infinite() && !b.is_infinite() {
        Err(EvalError::FloatOverflow)
    } else {
        Ok(sum)
    }
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "+",
    propagates_nulls = true
)]
fn add_float64(a: f64, b: f64) -> Result<f64, EvalError> {
    let sum = a + b;
    if sum.is_infinite() && !a.is_infinite() && !b.is_infinite() {
        Err(EvalError::FloatOverflow)
    } else {
        Ok(sum)
    }
}

#[sqlfunc(is_monotone = "(true, true)", is_infix_op = true, sqlname = "+")]
fn add_timestamp_interval(
    a: CheckedTimestamp<NaiveDateTime>,
    b: Interval,
) -> Result<CheckedTimestamp<NaiveDateTime>, EvalError> {
    add_timestamplike_interval(a, b)
}

#[sqlfunc(is_monotone = "(true, true)", is_infix_op = true, sqlname = "+")]
fn add_timestamp_tz_interval(
    a: CheckedTimestamp<DateTime<Utc>>,
    b: Interval,
) -> Result<CheckedTimestamp<DateTime<Utc>>, EvalError> {
    add_timestamplike_interval(a, b)
}

fn add_timestamplike_interval<T>(
    a: CheckedTimestamp<T>,
    b: Interval,
) -> Result<CheckedTimestamp<T>, EvalError>
where
    T: TimestampLike,
{
    let dt = a.date_time();
    let dt = add_timestamp_months(&dt, b.months)?;
    let dt = dt
        .checked_add_signed(b.duration_as_chrono())
        .ok_or(EvalError::TimestampOutOfRange)?;
    Ok(CheckedTimestamp::from_timestamplike(T::from_date_time(dt))?)
}

#[sqlfunc(is_monotone = "(true, true)", is_infix_op = true, sqlname = "-")]
fn sub_timestamp_interval(
    a: CheckedTimestamp<NaiveDateTime>,
    b: Interval,
) -> Result<CheckedTimestamp<NaiveDateTime>, EvalError> {
    sub_timestamplike_interval(a, b)
}

#[sqlfunc(is_monotone = "(true, true)", is_infix_op = true, sqlname = "-")]
fn sub_timestamp_tz_interval(
    a: CheckedTimestamp<DateTime<Utc>>,
    b: Interval,
) -> Result<CheckedTimestamp<DateTime<Utc>>, EvalError> {
    sub_timestamplike_interval(a, b)
}

fn sub_timestamplike_interval<T>(
    a: CheckedTimestamp<T>,
    b: Interval,
) -> Result<CheckedTimestamp<T>, EvalError>
where
    T: TimestampLike,
{
    neg_interval_inner(b).and_then(|i| add_timestamplike_interval(a, i))
}

#[sqlfunc(is_monotone = "(true, true)", is_infix_op = true, sqlname = "+")]
fn add_date_time(
    date: Date,
    time: chrono::NaiveTime,
) -> Result<CheckedTimestamp<NaiveDateTime>, EvalError> {
    let dt = NaiveDate::from(date)
        .and_hms_nano_opt(time.hour(), time.minute(), time.second(), time.nanosecond())
        .unwrap();
    Ok(CheckedTimestamp::from_timestamplike(dt)?)
}

#[sqlfunc(is_monotone = "(true, true)", is_infix_op = true, sqlname = "+")]
fn add_date_interval(
    date: Date,
    interval: Interval,
) -> Result<CheckedTimestamp<NaiveDateTime>, EvalError> {
    let dt = NaiveDate::from(date).and_hms_opt(0, 0, 0).unwrap();
    let dt = add_timestamp_months(&dt, interval.months)?;
    let dt = dt
        .checked_add_signed(interval.duration_as_chrono())
        .ok_or(EvalError::TimestampOutOfRange)?;
    Ok(CheckedTimestamp::from_timestamplike(dt)?)
}

#[sqlfunc(
    // <time> + <interval> wraps!
    is_monotone = "(false, false)",
    is_infix_op = true,
    sqlname = "+",
    propagates_nulls = true
)]
fn add_time_interval(time: chrono::NaiveTime, interval: Interval) -> chrono::NaiveTime {
    let (t, _) = time.overflowing_add_signed(interval.duration_as_chrono());
    t
}

#[sqlfunc(
    is_monotone = "(true, false)",
    output_type = "Numeric",
    sqlname = "round",
    propagates_nulls = true
)]
fn round_numeric_binary(a: OrderedDecimal<Numeric>, mut b: i32) -> Result<Numeric, EvalError> {
    let mut a = a.0;
    let mut cx = numeric::cx_datum();
    let a_exp = a.exponent();
    if a_exp > 0 && b > 0 || a_exp < 0 && -a_exp < b {
        // This condition indicates:
        // - a is a value without a decimal point, b is a positive number
        // - a has a decimal point, but b is larger than its scale
        // In both of these situations, right-pad the number with zeroes, which // is most easily done with rescale.

        // Ensure rescale doesn't exceed max precision by putting a ceiling on
        // b equal to the maximum remaining scale the value can support.
        let max_remaining_scale = u32::from(numeric::NUMERIC_DATUM_MAX_PRECISION)
            - (numeric::get_precision(&a) - numeric::get_scale(&a));
        b = match i32::try_from(max_remaining_scale) {
            Ok(max_remaining_scale) => std::cmp::min(b, max_remaining_scale),
            Err(_) => b,
        };
        cx.rescale(&mut a, &numeric::Numeric::from(-b));
    } else {
        // To avoid invalid operations, clamp b to be within 1 more than the
        // precision limit.
        const MAX_P_LIMIT: i32 = 1 + cast::u8_to_i32(numeric::NUMERIC_DATUM_MAX_PRECISION);
        b = std::cmp::min(MAX_P_LIMIT, b);
        b = std::cmp::max(-MAX_P_LIMIT, b);
        let mut b = numeric::Numeric::from(b);
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
        Ok(numeric::Numeric::zero())
    } else {
        numeric::munge_numeric(&mut a).unwrap();
        Ok(a)
    }
}

#[sqlfunc(sqlname = "convert_from", propagates_nulls = true)]
fn convert_from<'a>(a: &'a [u8], b: &str) -> Result<&'a str, EvalError> {
    // Convert PostgreSQL-style encoding names[1] to WHATWG-style encoding names[2],
    // which the encoding library uses[3].
    // [1]: https://www.postgresql.org/docs/9.5/multibyte.html
    // [2]: https://encoding.spec.whatwg.org/
    // [3]: https://github.com/lifthrasiir/rust-encoding/blob/4e79c35ab6a351881a86dbff565c4db0085cc113/src/label.rs
    let encoding_name = b.to_lowercase().replace('_', "-").into_boxed_str();

    // Supporting other encodings is tracked by database-issues#797.
    if encoding_from_whatwg_label(&encoding_name).map(|e| e.name()) != Some("utf-8") {
        return Err(EvalError::InvalidEncodingName(encoding_name));
    }

    match str::from_utf8(a) {
        Ok(from) => Ok(from),
        Err(e) => Err(EvalError::InvalidByteSequence {
            byte_sequence: e.to_string().into(),
            encoding_name,
        }),
    }
}

#[sqlfunc]
fn encode(bytes: &[u8], format: &str) -> Result<String, EvalError> {
    let format = encoding::lookup_format(format)?;
    Ok(format.encode(bytes))
}

#[sqlfunc]
fn decode(string: &str, format: &str) -> Result<Vec<u8>, EvalError> {
    let format = encoding::lookup_format(format)?;
    let out = format.decode(string)?;
    if out.len() > MAX_STRING_FUNC_RESULT_BYTES {
        Err(EvalError::LengthTooLarge)
    } else {
        Ok(out)
    }
}

#[sqlfunc(sqlname = "length", propagates_nulls = true)]
fn encoded_bytes_char_length(a: &[u8], b: &str) -> Result<i32, EvalError> {
    // Convert PostgreSQL-style encoding names[1] to WHATWG-style encoding names[2],
    // which the encoding library uses[3].
    // [1]: https://www.postgresql.org/docs/9.5/multibyte.html
    // [2]: https://encoding.spec.whatwg.org/
    // [3]: https://github.com/lifthrasiir/rust-encoding/blob/4e79c35ab6a351881a86dbff565c4db0085cc113/src/label.rs
    let encoding_name = b.to_lowercase().replace('_', "-").into_boxed_str();

    let enc = match encoding_from_whatwg_label(&encoding_name) {
        Some(enc) => enc,
        None => return Err(EvalError::InvalidEncodingName(encoding_name)),
    };

    let decoded_string = match enc.decode(a, DecoderTrap::Strict) {
        Ok(s) => s,
        Err(e) => {
            return Err(EvalError::InvalidByteSequence {
                byte_sequence: e.into(),
                encoding_name,
            });
        }
    };

    let count = decoded_string.chars().count();
    i32::try_from(count).map_err(|_| EvalError::Int32OutOfRange(count.to_string().into()))
}

// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
pub fn add_timestamp_months<T: TimestampLike>(
    dt: &T,
    mut months: i32,
) -> Result<CheckedTimestamp<T>, EvalError> {
    if months == 0 {
        return Ok(CheckedTimestamp::from_timestamplike(dt.clone())?);
    }

    let (mut year, mut month, mut day) = (dt.year(), dt.month0() as i32, dt.day());
    let years = months / 12;
    year = year
        .checked_add(years)
        .ok_or(EvalError::TimestampOutOfRange)?;

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
        // If we have decremented day past 28 and are still receiving `None`,
        // then we have generally overflowed `NaiveDate`.
        if day < 28 {
            return Err(EvalError::TimestampOutOfRange);
        }
        day -= 1;
        new_d = chrono::NaiveDate::from_ymd_opt(year, month as u32, day);
    }
    let new_d = new_d.unwrap();

    // Neither postgres nor mysql support leap seconds, so this should be safe.
    //
    // Both my testing and https://dba.stackexchange.com/a/105829 support the
    // idea that we should ignore leap seconds
    let new_dt = new_d
        .and_hms_nano_opt(dt.hour(), dt.minute(), dt.second(), dt.nanosecond())
        .unwrap();
    let new_dt = T::from_date_time(new_dt);
    Ok(CheckedTimestamp::from_timestamplike(new_dt)?)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "+",
    propagates_nulls = true
)]
fn add_numeric(
    a: OrderedDecimal<Numeric>,
    b: OrderedDecimal<Numeric>,
) -> Result<Numeric, EvalError> {
    let mut cx = numeric::cx_datum();
    let mut a = a.0;
    cx.add(&mut a, &b.0);
    if cx.status().overflow() {
        Err(EvalError::FloatOverflow)
    } else {
        Ok(a)
    }
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "+",
    propagates_nulls = true
)]
fn add_interval(a: Interval, b: Interval) -> Result<Interval, EvalError> {
    a.checked_add(&b)
        .ok_or_else(|| EvalError::IntervalOutOfRange(format!("{a} + {b}").into()))
}

#[sqlfunc(is_infix_op = true, sqlname = "&", propagates_nulls = true)]
fn bit_and_int16(a: i16, b: i16) -> i16 {
    a & b
}

#[sqlfunc(is_infix_op = true, sqlname = "&", propagates_nulls = true)]
fn bit_and_int32(a: i32, b: i32) -> i32 {
    a & b
}

#[sqlfunc(is_infix_op = true, sqlname = "&", propagates_nulls = true)]
fn bit_and_int64(a: i64, b: i64) -> i64 {
    a & b
}

#[sqlfunc(is_infix_op = true, sqlname = "&", propagates_nulls = true)]
fn bit_and_uint16(a: u16, b: u16) -> u16 {
    a & b
}

#[sqlfunc(is_infix_op = true, sqlname = "&", propagates_nulls = true)]
fn bit_and_uint32(a: u32, b: u32) -> u32 {
    a & b
}

#[sqlfunc(is_infix_op = true, sqlname = "&", propagates_nulls = true)]
fn bit_and_uint64(a: u64, b: u64) -> u64 {
    a & b
}

#[sqlfunc(is_infix_op = true, sqlname = "|", propagates_nulls = true)]
fn bit_or_int16(a: i16, b: i16) -> i16 {
    a | b
}

#[sqlfunc(is_infix_op = true, sqlname = "|", propagates_nulls = true)]
fn bit_or_int32(a: i32, b: i32) -> i32 {
    a | b
}

#[sqlfunc(is_infix_op = true, sqlname = "|", propagates_nulls = true)]
fn bit_or_int64(a: i64, b: i64) -> i64 {
    a | b
}

#[sqlfunc(is_infix_op = true, sqlname = "|", propagates_nulls = true)]
fn bit_or_uint16(a: u16, b: u16) -> u16 {
    a | b
}

#[sqlfunc(is_infix_op = true, sqlname = "|", propagates_nulls = true)]
fn bit_or_uint32(a: u32, b: u32) -> u32 {
    a | b
}

#[sqlfunc(is_infix_op = true, sqlname = "|", propagates_nulls = true)]
fn bit_or_uint64(a: u64, b: u64) -> u64 {
    a | b
}

#[sqlfunc(is_infix_op = true, sqlname = "#", propagates_nulls = true)]
fn bit_xor_int16(a: i16, b: i16) -> i16 {
    a ^ b
}

#[sqlfunc(is_infix_op = true, sqlname = "#", propagates_nulls = true)]
fn bit_xor_int32(a: i32, b: i32) -> i32 {
    a ^ b
}

#[sqlfunc(is_infix_op = true, sqlname = "#", propagates_nulls = true)]
fn bit_xor_int64(a: i64, b: i64) -> i64 {
    a ^ b
}

#[sqlfunc(is_infix_op = true, sqlname = "#", propagates_nulls = true)]
fn bit_xor_uint16(a: u16, b: u16) -> u16 {
    a ^ b
}

#[sqlfunc(is_infix_op = true, sqlname = "#", propagates_nulls = true)]
fn bit_xor_uint32(a: u32, b: u32) -> u32 {
    a ^ b
}

#[sqlfunc(is_infix_op = true, sqlname = "#", propagates_nulls = true)]
fn bit_xor_uint64(a: u64, b: u64) -> u64 {
    a ^ b
}

#[sqlfunc(is_infix_op = true, sqlname = "<<", propagates_nulls = true)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn bit_shift_left_int16(a: i16, b: i32) -> i16 {
    // widen to i32 and then cast back to i16 in order emulate the C promotion rules used in by Postgres
    // when the rhs in the 16-31 range, e.g. (1 << 17 should evaluate to 0)
    // see https://github.com/postgres/postgres/blob/REL_14_STABLE/src/backend/utils/adt/int.c#L1460-L1476
    let lhs: i32 = a as i32;
    let rhs: u32 = b as u32;
    lhs.wrapping_shl(rhs) as i16
}

#[sqlfunc(is_infix_op = true, sqlname = "<<", propagates_nulls = true)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn bit_shift_left_int32(lhs: i32, rhs: i32) -> i32 {
    let rhs = rhs as u32;
    lhs.wrapping_shl(rhs)
}

#[sqlfunc(is_infix_op = true, sqlname = "<<", propagates_nulls = true)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn bit_shift_left_int64(lhs: i64, rhs: i32) -> i64 {
    let rhs = rhs as u32;
    lhs.wrapping_shl(rhs)
}

#[sqlfunc(is_infix_op = true, sqlname = "<<", propagates_nulls = true)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn bit_shift_left_uint16(a: u16, b: u32) -> u16 {
    // widen to u32 and then cast back to u16 in order emulate the C promotion rules used in by Postgres
    // when the rhs in the 16-31 range, e.g. (1 << 17 should evaluate to 0)
    // see https://github.com/postgres/postgres/blob/REL_14_STABLE/src/backend/utils/adt/int.c#L1460-L1476
    let lhs: u32 = a as u32;
    let rhs: u32 = b;
    lhs.wrapping_shl(rhs) as u16
}

#[sqlfunc(is_infix_op = true, sqlname = "<<", propagates_nulls = true)]
fn bit_shift_left_uint32(a: u32, b: u32) -> u32 {
    let lhs = a;
    let rhs = b;
    lhs.wrapping_shl(rhs)
}

#[sqlfunc(
    output_type = "u64",
    is_infix_op = true,
    sqlname = "<<",
    propagates_nulls = true
)]
fn bit_shift_left_uint64(lhs: u64, rhs: u32) -> u64 {
    lhs.wrapping_shl(rhs)
}

#[sqlfunc(is_infix_op = true, sqlname = ">>", propagates_nulls = true)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn bit_shift_right_int16(lhs: i16, rhs: i32) -> i16 {
    // widen to i32 and then cast back to i16 in order emulate the C promotion rules used in by Postgres
    // when the rhs in the 16-31 range, e.g. (-32767 >> 17 should evaluate to -1)
    // see https://github.com/postgres/postgres/blob/REL_14_STABLE/src/backend/utils/adt/int.c#L1460-L1476
    let lhs = lhs as i32;
    let rhs = rhs as u32;
    lhs.wrapping_shr(rhs) as i16
}

#[sqlfunc(is_infix_op = true, sqlname = ">>", propagates_nulls = true)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn bit_shift_right_int32(lhs: i32, rhs: i32) -> i32 {
    lhs.wrapping_shr(rhs as u32)
}

#[sqlfunc(is_infix_op = true, sqlname = ">>", propagates_nulls = true)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn bit_shift_right_int64(lhs: i64, rhs: i32) -> i64 {
    lhs.wrapping_shr(rhs as u32)
}

#[sqlfunc(is_infix_op = true, sqlname = ">>", propagates_nulls = true)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn bit_shift_right_uint16(lhs: u16, rhs: u32) -> u16 {
    // widen to u32 and then cast back to u16 in order emulate the C promotion rules used in by Postgres
    // when the rhs in the 16-31 range, e.g. (-32767 >> 17 should evaluate to -1)
    // see https://github.com/postgres/postgres/blob/REL_14_STABLE/src/backend/utils/adt/int.c#L1460-L1476
    let lhs = lhs as u32;
    lhs.wrapping_shr(rhs) as u16
}

#[sqlfunc(is_infix_op = true, sqlname = ">>", propagates_nulls = true)]
fn bit_shift_right_uint32(lhs: u32, rhs: u32) -> u32 {
    lhs.wrapping_shr(rhs)
}

#[sqlfunc(is_infix_op = true, sqlname = ">>", propagates_nulls = true)]
fn bit_shift_right_uint64(lhs: u64, rhs: u32) -> u64 {
    lhs.wrapping_shr(rhs)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_int16(a: i16, b: i16) -> Result<i16, EvalError> {
    a.checked_sub(b).ok_or(EvalError::NumericFieldOverflow)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_int32(a: i32, b: i32) -> Result<i32, EvalError> {
    a.checked_sub(b).ok_or(EvalError::NumericFieldOverflow)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_int64(a: i64, b: i64) -> Result<i64, EvalError> {
    a.checked_sub(b).ok_or(EvalError::NumericFieldOverflow)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_uint16(a: u16, b: u16) -> Result<u16, EvalError> {
    a.checked_sub(b)
        .ok_or_else(|| EvalError::UInt16OutOfRange(format!("{a} - {b}").into()))
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_uint32(a: u32, b: u32) -> Result<u32, EvalError> {
    a.checked_sub(b)
        .ok_or_else(|| EvalError::UInt32OutOfRange(format!("{a} - {b}").into()))
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_uint64(a: u64, b: u64) -> Result<u64, EvalError> {
    a.checked_sub(b)
        .ok_or_else(|| EvalError::UInt64OutOfRange(format!("{a} - {b}").into()))
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_float32(a: f32, b: f32) -> Result<f32, EvalError> {
    let difference = a - b;
    if difference.is_infinite() && !a.is_infinite() && !b.is_infinite() {
        Err(EvalError::FloatOverflow)
    } else {
        Ok(difference)
    }
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_float64(a: f64, b: f64) -> Result<f64, EvalError> {
    let difference = a - b;
    if difference.is_infinite() && !a.is_infinite() && !b.is_infinite() {
        Err(EvalError::FloatOverflow)
    } else {
        Ok(difference)
    }
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_numeric(
    a: OrderedDecimal<Numeric>,
    b: OrderedDecimal<Numeric>,
) -> Result<Numeric, EvalError> {
    let mut cx = numeric::cx_datum();
    let mut a = a.0;
    cx.sub(&mut a, &b.0);
    if cx.status().overflow() {
        Err(EvalError::FloatOverflow)
    } else {
        Ok(a)
    }
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "Interval",
    sqlname = "age",
    propagates_nulls = true
)]
fn age_timestamp(
    a: CheckedTimestamp<chrono::NaiveDateTime>,
    b: CheckedTimestamp<chrono::NaiveDateTime>,
) -> Result<Interval, EvalError> {
    Ok(a.age(&b)?)
}

#[sqlfunc(is_monotone = "(true, true)", sqlname = "age", propagates_nulls = true)]
fn age_timestamp_tz(
    a: CheckedTimestamp<chrono::DateTime<Utc>>,
    b: CheckedTimestamp<chrono::DateTime<Utc>>,
) -> Result<Interval, EvalError> {
    Ok(a.age(&b)?)
}

#[sqlfunc(is_monotone = "(true, true)", is_infix_op = true, sqlname = "-")]
fn sub_timestamp(
    a: CheckedTimestamp<NaiveDateTime>,
    b: CheckedTimestamp<NaiveDateTime>,
) -> Result<Interval, EvalError> {
    Interval::from_chrono_duration(a - b)
        .map_err(|e| EvalError::IntervalOutOfRange(e.to_string().into()))
}

#[sqlfunc(is_monotone = "(true, true)", is_infix_op = true, sqlname = "-")]
fn sub_timestamp_tz(
    a: CheckedTimestamp<chrono::DateTime<Utc>>,
    b: CheckedTimestamp<chrono::DateTime<Utc>>,
) -> Result<Interval, EvalError> {
    Interval::from_chrono_duration(a - b)
        .map_err(|e| EvalError::IntervalOutOfRange(e.to_string().into()))
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_date(a: Date, b: Date) -> i32 {
    a - b
}

#[sqlfunc(is_monotone = "(true, true)", is_infix_op = true, sqlname = "-")]
fn sub_time(a: chrono::NaiveTime, b: chrono::NaiveTime) -> Result<Interval, EvalError> {
    Interval::from_chrono_duration(a - b)
        .map_err(|e| EvalError::IntervalOutOfRange(e.to_string().into()))
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "Interval",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_interval(a: Interval, b: Interval) -> Result<Interval, EvalError> {
    b.checked_neg()
        .and_then(|b| b.checked_add(&a))
        .ok_or_else(|| EvalError::IntervalOutOfRange(format!("{a} - {b}").into()))
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_date_interval(
    date: Date,
    interval: Interval,
) -> Result<CheckedTimestamp<NaiveDateTime>, EvalError> {
    let dt = NaiveDate::from(date).and_hms_opt(0, 0, 0).unwrap();
    let dt = interval
        .months
        .checked_neg()
        .ok_or_else(|| EvalError::IntervalOutOfRange(interval.months.to_string().into()))
        .and_then(|months| add_timestamp_months(&dt, months))?;
    let dt = dt
        .checked_sub_signed(interval.duration_as_chrono())
        .ok_or(EvalError::TimestampOutOfRange)?;
    Ok(dt.try_into()?)
}

#[sqlfunc(
    is_monotone = "(false, false)",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_time_interval(time: chrono::NaiveTime, interval: Interval) -> chrono::NaiveTime {
    let (t, _) = time.overflowing_sub_signed(interval.duration_as_chrono());
    t
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_int16(a: i16, b: i16) -> Result<i16, EvalError> {
    a.checked_mul(b).ok_or(EvalError::NumericFieldOverflow)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_int32(a: i32, b: i32) -> Result<i32, EvalError> {
    a.checked_mul(b).ok_or(EvalError::NumericFieldOverflow)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_int64(a: i64, b: i64) -> Result<i64, EvalError> {
    a.checked_mul(b).ok_or(EvalError::NumericFieldOverflow)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_uint16(a: u16, b: u16) -> Result<u16, EvalError> {
    a.checked_mul(b)
        .ok_or_else(|| EvalError::UInt16OutOfRange(format!("{a} * {b}").into()))
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_uint32(a: u32, b: u32) -> Result<u32, EvalError> {
    a.checked_mul(b)
        .ok_or_else(|| EvalError::UInt32OutOfRange(format!("{a} * {b}").into()))
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_uint64(a: u64, b: u64) -> Result<u64, EvalError> {
    a.checked_mul(b)
        .ok_or_else(|| EvalError::UInt64OutOfRange(format!("{a} * {b}").into()))
}

#[sqlfunc(
    is_monotone = (true, true),
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_float32(a: f32, b: f32) -> Result<f32, EvalError> {
    let product = a * b;
    if product.is_infinite() && !a.is_infinite() && !b.is_infinite() {
        Err(EvalError::FloatOverflow)
    } else if product == 0.0f32 && a != 0.0f32 && b != 0.0f32 {
        Err(EvalError::FloatUnderflow)
    } else {
        Ok(product)
    }
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_float64(a: f64, b: f64) -> Result<f64, EvalError> {
    let product = a * b;
    if product.is_infinite() && !a.is_infinite() && !b.is_infinite() {
        Err(EvalError::FloatOverflow)
    } else if product == 0.0f64 && a != 0.0f64 && b != 0.0f64 {
        Err(EvalError::FloatUnderflow)
    } else {
        Ok(product)
    }
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_numeric(mut a: Numeric, b: Numeric) -> Result<Numeric, EvalError> {
    let mut cx = numeric::cx_datum();
    cx.mul(&mut a, &b);
    let cx_status = cx.status();
    if cx_status.overflow() {
        Err(EvalError::FloatOverflow)
    } else if cx_status.subnormal() {
        Err(EvalError::FloatUnderflow)
    } else {
        numeric::munge_numeric(&mut a).unwrap();
        Ok(a)
    }
}

#[sqlfunc(
    is_monotone = "(false, false)",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_interval(a: Interval, b: f64) -> Result<Interval, EvalError> {
    a.checked_mul(b)
        .ok_or_else(|| EvalError::IntervalOutOfRange(format!("{a} * {b}").into()))
}

#[sqlfunc(
    is_monotone = "(true, false)",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_int16(a: i16, b: i16) -> Result<i16, EvalError> {
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        a.checked_div(b)
            .ok_or_else(|| EvalError::Int16OutOfRange(format!("{a} / {b}").into()))
    }
}

#[sqlfunc(
    is_monotone = "(true, false)",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_int32(a: i32, b: i32) -> Result<i32, EvalError> {
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        a.checked_div(b)
            .ok_or_else(|| EvalError::Int32OutOfRange(format!("{a} / {b}").into()))
    }
}

#[sqlfunc(
    is_monotone = "(true, false)",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_int64(a: i64, b: i64) -> Result<i64, EvalError> {
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        a.checked_div(b)
            .ok_or_else(|| EvalError::Int64OutOfRange(format!("{a} / {b}").into()))
    }
}

#[sqlfunc(
    is_monotone = "(true, false)",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_uint16(a: u16, b: u16) -> Result<u16, EvalError> {
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(a / b)
    }
}

#[sqlfunc(
    is_monotone = "(true, false)",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_uint32(a: u32, b: u32) -> Result<u32, EvalError> {
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(a / b)
    }
}

#[sqlfunc(
    is_monotone = "(true, false)",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_uint64(a: u64, b: u64) -> Result<u64, EvalError> {
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(a / b)
    }
}

#[sqlfunc(
    is_monotone = "(true, false)",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_float32(a: f32, b: f32) -> Result<f32, EvalError> {
    if b == 0.0f32 && !a.is_nan() {
        Err(EvalError::DivisionByZero)
    } else {
        let quotient = a / b;
        if quotient.is_infinite() && !a.is_infinite() {
            Err(EvalError::FloatOverflow)
        } else if quotient == 0.0f32 && a != 0.0f32 && !b.is_infinite() {
            Err(EvalError::FloatUnderflow)
        } else {
            Ok(quotient)
        }
    }
}

#[sqlfunc(
    is_monotone = "(true, false)",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_float64(a: f64, b: f64) -> Result<f64, EvalError> {
    if b == 0.0f64 && !a.is_nan() {
        Err(EvalError::DivisionByZero)
    } else {
        let quotient = a / b;
        if quotient.is_infinite() && !a.is_infinite() {
            Err(EvalError::FloatOverflow)
        } else if quotient == 0.0f64 && a != 0.0f64 && !b.is_infinite() {
            Err(EvalError::FloatUnderflow)
        } else {
            Ok(quotient)
        }
    }
}

#[sqlfunc(
    is_monotone = "(true, false)",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_numeric(mut a: Numeric, b: Numeric) -> Result<Numeric, EvalError> {
    let mut cx = numeric::cx_datum();

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
        numeric::munge_numeric(&mut a).unwrap();
        Ok(a)
    }
}

#[sqlfunc(
    is_monotone = "(false, false)",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_interval(a: Interval, b: f64) -> Result<Interval, EvalError> {
    if b == 0.0 {
        Err(EvalError::DivisionByZero)
    } else {
        a.checked_div(b)
            .ok_or_else(|| EvalError::IntervalOutOfRange(format!("{a} / {b}").into()))
    }
}

#[sqlfunc(is_infix_op = true, sqlname = "%", propagates_nulls = true)]
fn mod_int16(a: i16, b: i16) -> Result<i16, EvalError> {
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(a.checked_rem(b).unwrap_or(0))
    }
}

#[sqlfunc(is_infix_op = true, sqlname = "%", propagates_nulls = true)]
fn mod_int32(a: i32, b: i32) -> Result<i32, EvalError> {
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(a.checked_rem(b).unwrap_or(0))
    }
}

#[sqlfunc(is_infix_op = true, sqlname = "%", propagates_nulls = true)]
fn mod_int64(a: i64, b: i64) -> Result<i64, EvalError> {
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(a.checked_rem(b).unwrap_or(0))
    }
}

#[sqlfunc(is_infix_op = true, sqlname = "%", propagates_nulls = true)]
fn mod_uint16(a: u16, b: u16) -> Result<u16, EvalError> {
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(a % b)
    }
}

#[sqlfunc(is_infix_op = true, sqlname = "%", propagates_nulls = true)]
fn mod_uint32(a: u32, b: u32) -> Result<u32, EvalError> {
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(a % b)
    }
}

#[sqlfunc(is_infix_op = true, sqlname = "%", propagates_nulls = true)]
fn mod_uint64(a: u64, b: u64) -> Result<u64, EvalError> {
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(a % b)
    }
}

#[sqlfunc(is_infix_op = true, sqlname = "%", propagates_nulls = true)]
fn mod_float32(a: f32, b: f32) -> Result<f32, EvalError> {
    if b == 0.0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(a % b)
    }
}

#[sqlfunc(is_infix_op = true, sqlname = "%", propagates_nulls = true)]
fn mod_float64(a: f64, b: f64) -> Result<f64, EvalError> {
    if b == 0.0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(a % b)
    }
}

#[sqlfunc(is_infix_op = true, sqlname = "%", propagates_nulls = true)]
fn mod_numeric(mut a: Numeric, b: Numeric) -> Result<Numeric, EvalError> {
    if b.is_zero() {
        return Err(EvalError::DivisionByZero);
    }
    let mut cx = numeric::cx_datum();
    // Postgres does _not_ use IEEE 754-style remainder
    cx.rem(&mut a, &b);
    numeric::munge_numeric(&mut a).unwrap();
    Ok(a)
}

fn neg_interval_inner(a: Interval) -> Result<Interval, EvalError> {
    a.checked_neg()
        .ok_or_else(|| EvalError::IntervalOutOfRange(a.to_string().into()))
}

fn log_guard_numeric(val: &Numeric, function_name: &str) -> Result<(), EvalError> {
    if val.is_negative() {
        return Err(EvalError::NegativeOutOfDomain(function_name.into()));
    }
    if val.is_zero() {
        return Err(EvalError::ZeroOutOfDomain(function_name.into()));
    }
    Ok(())
}

#[sqlfunc(sqlname = "log", propagates_nulls = true)]
fn log_base_numeric(mut a: Numeric, mut b: Numeric) -> Result<Numeric, EvalError> {
    log_guard_numeric(&a, "log")?;
    log_guard_numeric(&b, "log")?;
    let mut cx = numeric::cx_datum();
    cx.ln(&mut a);
    cx.ln(&mut b);
    cx.div(&mut b, &a);
    if a.is_zero() {
        Err(EvalError::DivisionByZero)
    } else {
        // This division can result in slightly wrong answers due to the
        // limitation of dividing irrational numbers. To correct that, see if
        // rounding off the value from its `numeric::NUMERIC_DATUM_MAX_PRECISION
        // - 1`th position results in an integral value.
        cx.set_precision(usize::from(numeric::NUMERIC_DATUM_MAX_PRECISION - 1))
            .expect("reducing precision below max always succeeds");
        let mut integral_check = b.clone();

        // `reduce` rounds to the context's final digit when the number of
        // digits in its argument exceeds its precision. We've contrived that to
        // happen by shrinking the context's precision by 1.
        cx.reduce(&mut integral_check);

        // Reduced integral values always have a non-negative exponent.
        let mut b = if integral_check.exponent() >= 0 {
            // We believe our result should have been an integral
            integral_check
        } else {
            b
        };

        numeric::munge_numeric(&mut b).unwrap();
        Ok(b)
    }
}

#[sqlfunc(propagates_nulls = true)]
fn power(a: f64, b: f64) -> Result<f64, EvalError> {
    if a == 0.0 && b.is_sign_negative() {
        return Err(EvalError::Undefined(
            "zero raised to a negative power".into(),
        ));
    }
    if a.is_sign_negative() && b.fract() != 0.0 {
        // Equivalent to PG error:
        // > a negative number raised to a non-integer power yields a complex result
        return Err(EvalError::ComplexOutOfRange("pow".into()));
    }
    let res = a.powf(b);
    if res.is_infinite() {
        return Err(EvalError::FloatOverflow);
    }
    if res == 0.0 && a != 0.0 {
        return Err(EvalError::FloatUnderflow);
    }
    Ok(res)
}

#[sqlfunc(propagates_nulls = true)]
fn uuid_generate_v5(a: uuid::Uuid, b: &str) -> uuid::Uuid {
    uuid::Uuid::new_v5(&a, b.as_bytes())
}

#[sqlfunc(output_type = "Numeric", propagates_nulls = true)]
fn power_numeric(mut a: Numeric, b: Numeric) -> Result<Numeric, EvalError> {
    if a.is_zero() {
        if b.is_zero() {
            return Ok(Numeric::from(1));
        }
        if b.is_negative() {
            return Err(EvalError::Undefined(
                "zero raised to a negative power".into(),
            ));
        }
    }
    if a.is_negative() && b.exponent() < 0 {
        // Equivalent to PG error:
        // > a negative number raised to a non-integer power yields a complex result
        return Err(EvalError::ComplexOutOfRange("pow".into()));
    }
    let mut cx = numeric::cx_datum();
    cx.pow(&mut a, &b);
    let cx_status = cx.status();
    if cx_status.overflow() || (cx_status.invalid_operation() && !b.is_negative()) {
        Err(EvalError::FloatOverflow)
    } else if cx_status.subnormal() || cx_status.invalid_operation() {
        Err(EvalError::FloatUnderflow)
    } else {
        numeric::munge_numeric(&mut a).unwrap();
        Ok(a)
    }
}

#[sqlfunc(propagates_nulls = true)]
fn get_bit(bytes: &[u8], index: i32) -> Result<i32, EvalError> {
    let err = EvalError::IndexOutOfRange {
        provided: index,
        valid_end: i32::try_from(bytes.len().saturating_mul(8)).unwrap() - 1,
    };

    let index = usize::try_from(index).map_err(|_| err.clone())?;

    let byte_index = index / 8;
    let bit_index = index % 8;

    let i = bytes
        .get(byte_index)
        .map(|b| (*b >> bit_index) & 1)
        .ok_or(err)?;
    assert!(i == 0 || i == 1);
    Ok(i32::from(i))
}

#[sqlfunc(propagates_nulls = true)]
fn get_byte(bytes: &[u8], index: i32) -> Result<i32, EvalError> {
    let err = EvalError::IndexOutOfRange {
        provided: index,
        valid_end: i32::try_from(bytes.len()).unwrap() - 1,
    };
    let i: &u8 = bytes
        .get(usize::try_from(index).map_err(|_| err.clone())?)
        .ok_or(err)?;
    Ok(i32::from(*i))
}

#[sqlfunc(sqlname = "constant_time_compare_bytes", propagates_nulls = true)]
pub fn constant_time_eq_bytes(a: &[u8], b: &[u8]) -> bool {
    bool::from(a.ct_eq(b))
}

#[sqlfunc(sqlname = "constant_time_compare_strings", propagates_nulls = true)]
pub fn constant_time_eq_string(a: &str, b: &str) -> bool {
    bool::from(a.as_bytes().ct_eq(b.as_bytes()))
}

#[sqlfunc(is_infix_op = true, sqlname = "@>", propagates_nulls = true)]
fn range_contains_i32<'a>(a: Range<Datum<'a>>, b: i32) -> bool {
    a.contains_elem(&b)
}

#[sqlfunc(is_infix_op = true, sqlname = "@>", propagates_nulls = true)]
fn range_contains_i64<'a>(a: Range<Datum<'a>>, elem: i64) -> bool {
    a.contains_elem(&elem)
}

#[sqlfunc(is_infix_op = true, sqlname = "@>", propagates_nulls = true)]
fn range_contains_date<'a>(a: Range<Datum<'a>>, elem: Date) -> bool {
    a.contains_elem(&elem)
}

#[sqlfunc(is_infix_op = true, sqlname = "@>", propagates_nulls = true)]
fn range_contains_numeric<'a>(a: Range<Datum<'a>>, elem: OrderedDecimal<Numeric>) -> bool {
    a.contains_elem(&elem)
}

#[sqlfunc(is_infix_op = true, sqlname = "@>", propagates_nulls = true)]
fn range_contains_timestamp<'a>(
    a: Range<Datum<'a>>,
    elem: CheckedTimestamp<NaiveDateTime>,
) -> bool {
    a.contains_elem(&elem)
}

#[sqlfunc(is_infix_op = true, sqlname = "@>", propagates_nulls = true)]
fn range_contains_timestamp_tz<'a>(
    a: Range<Datum<'a>>,
    elem: CheckedTimestamp<DateTime<Utc>>,
) -> bool {
    a.contains_elem(&elem)
}

#[sqlfunc(is_infix_op = true, sqlname = "<@", propagates_nulls = true)]
fn range_contains_i32_rev<'a>(a: Range<Datum<'a>>, b: i32) -> bool {
    a.contains_elem(&b)
}

#[sqlfunc(is_infix_op = true, sqlname = "<@", propagates_nulls = true)]
fn range_contains_i64_rev<'a>(a: Range<Datum<'a>>, elem: i64) -> bool {
    a.contains_elem(&elem)
}

#[sqlfunc(is_infix_op = true, sqlname = "<@", propagates_nulls = true)]
fn range_contains_date_rev<'a>(a: Range<Datum<'a>>, elem: Date) -> bool {
    a.contains_elem(&elem)
}

#[sqlfunc(is_infix_op = true, sqlname = "<@", propagates_nulls = true)]
fn range_contains_numeric_rev<'a>(a: Range<Datum<'a>>, elem: OrderedDecimal<Numeric>) -> bool {
    a.contains_elem(&elem)
}

#[sqlfunc(is_infix_op = true, sqlname = "<@", propagates_nulls = true)]
fn range_contains_timestamp_rev<'a>(
    a: Range<Datum<'a>>,
    elem: CheckedTimestamp<NaiveDateTime>,
) -> bool {
    a.contains_elem(&elem)
}

#[sqlfunc(is_infix_op = true, sqlname = "<@", propagates_nulls = true)]
fn range_contains_timestamp_tz_rev<'a>(
    a: Range<Datum<'a>>,
    elem: CheckedTimestamp<DateTime<Utc>>,
) -> bool {
    a.contains_elem(&elem)
}

/// Macro to define binary function for various range operations.
/// Parameters:
/// 1. Unique binary function symbol.
/// 2. Range function symbol.
/// 3. SQL name for the function.
macro_rules! range_fn {
    ($fn:expr, $range_fn:expr, $sqlname:expr) => {
        paste::paste! {

            #[sqlfunc(
                output_type = "bool",
                is_infix_op = true,
                sqlname = $sqlname,
                propagates_nulls = true
            )]
            fn [< range_ $fn >]<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a>
            {
                if a.is_null() || b.is_null() { return Datum::Null }
                let l = a.unwrap_range();
                let r = b.unwrap_range();
                Datum::from(Range::<Datum<'a>>::$range_fn(&l, &r))
            }
        }
    };
}

// RangeContainsRange is either @> or <@ depending on the order of the arguments.
// It doesn't influence the result, but it does influence the display string.
range_fn!(contains_range, contains_range, "@>");
range_fn!(contains_range_rev, contains_range, "<@");
range_fn!(overlaps, overlaps, "&&");
range_fn!(after, after, ">>");
range_fn!(before, before, "<<");
range_fn!(overleft, overleft, "&<");
range_fn!(overright, overright, "&>");
range_fn!(adjacent, adjacent, "-|-");

#[sqlfunc(
    output_type_expr = "input_type_a.scalar_type.without_modifiers().nullable(true)",
    is_infix_op = true,
    sqlname = "+",
    propagates_nulls = true,
    introduces_nulls = false
)]
fn range_union<'a>(
    l: Range<Datum<'a>>,
    r: Range<Datum<'a>>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    l.union(&r)?.into_result(temp_storage)
}

#[sqlfunc(
    output_type_expr = "input_type_a.scalar_type.without_modifiers().nullable(true)",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true,
    introduces_nulls = false
)]
fn range_intersection<'a>(
    l: Range<Datum<'a>>,
    r: Range<Datum<'a>>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    l.intersection(&r).into_result(temp_storage)
}

#[sqlfunc(
    output_type_expr = "input_type_a.scalar_type.without_modifiers().nullable(true)",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true,
    introduces_nulls = false
)]
fn range_difference<'a>(
    l: Range<Datum<'a>>,
    r: Range<Datum<'a>>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    l.difference(&r)?.into_result(temp_storage)
}

#[sqlfunc(is_infix_op = true, sqlname = "=", negate = "Some(NotEq.into())")]
fn eq<'a>(a: ExcludeNull<Datum<'a>>, b: ExcludeNull<Datum<'a>>) -> bool {
    // SQL equality demands that if either input is null, then the result should be null. However,
    // we don't need to handle this case here; it is handled when `BinaryFunc::eval` checks
    // `propagates_nulls`.
    a == b
}

#[sqlfunc(is_infix_op = true, sqlname = "!=", negate = "Some(Eq.into())")]
fn not_eq<'a>(a: ExcludeNull<Datum<'a>>, b: ExcludeNull<Datum<'a>>) -> bool {
    a != b
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "<",
    negate = "Some(Gte.into())"
)]
fn lt<'a>(a: ExcludeNull<Datum<'a>>, b: ExcludeNull<Datum<'a>>) -> bool {
    a < b
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = "<=",
    negate = "Some(Gt.into())"
)]
fn lte<'a>(a: ExcludeNull<Datum<'a>>, b: ExcludeNull<Datum<'a>>) -> bool {
    a <= b
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = ">",
    negate = "Some(Lte.into())"
)]
fn gt<'a>(a: ExcludeNull<Datum<'a>>, b: ExcludeNull<Datum<'a>>) -> bool {
    a > b
}

#[sqlfunc(
    is_monotone = "(true, true)",
    is_infix_op = true,
    sqlname = ">=",
    negate = "Some(Lt.into())"
)]
fn gte<'a>(a: ExcludeNull<Datum<'a>>, b: ExcludeNull<Datum<'a>>) -> bool {
    a >= b
}

#[sqlfunc(sqlname = "tocharts", propagates_nulls = true)]
fn to_char_timestamp_format(ts: CheckedTimestamp<chrono::NaiveDateTime>, format: &str) -> String {
    let fmt = DateTimeFormat::compile(format);
    fmt.render(&*ts)
}

#[sqlfunc(sqlname = "tochartstz", propagates_nulls = true)]
fn to_char_timestamp_tz_format(
    ts: CheckedTimestamp<chrono::DateTime<Utc>>,
    format: &str,
) -> String {
    let fmt = DateTimeFormat::compile(format);
    fmt.render(&*ts)
}

#[sqlfunc(sqlname = "->", is_infix_op = true)]
fn jsonb_get_int64<'a>(a: JsonbRef<'a>, i: i64) -> Option<JsonbRef<'a>> {
    match a.into_datum() {
        Datum::List(list) => {
            let i = if i >= 0 {
                usize::cast_from(i.unsigned_abs())
            } else {
                // index backwards from the end
                let i = usize::cast_from(i.unsigned_abs());
                (list.iter().count()).wrapping_sub(i)
            };
            let v = list.iter().nth(i)?;
            // `v` should be valid jsonb because it came from a jsonb list, but we don't
            // panic on mismatch to avoid bringing down the whole system on corrupt data.
            // Instead, we'll return None.
            JsonbRef::try_from_result(Ok::<_, ()>(v)).ok()
        }
        Datum::Map(_) => None,
        _ => {
            // I have no idea why postgres does this, but we're stuck with it
            (i == 0 || i == -1).then_some(a)
        }
    }
}

#[sqlfunc(sqlname = "->>", is_infix_op = true)]
fn jsonb_get_int64_stringify<'a>(
    a: JsonbRef<'a>,
    i: i64,
    temp_storage: &'a RowArena,
) -> Option<&'a str> {
    let json = jsonb_get_int64(a, i)?;
    jsonb_stringify(json.into_datum(), temp_storage)
}

#[sqlfunc(sqlname = "->", is_infix_op = true)]
fn jsonb_get_string<'a>(a: JsonbRef<'a>, k: &str) -> Option<JsonbRef<'a>> {
    let dict = DatumMap::try_from_result(Ok::<_, ()>(a.into_datum())).ok()?;
    let v = dict.iter().find(|(k2, _v)| k == *k2).map(|(_k, v)| v)?;
    JsonbRef::try_from_result(Ok::<_, ()>(v)).ok()
}

#[sqlfunc(sqlname = "->>", is_infix_op = true)]
fn jsonb_get_string_stringify<'a>(
    a: JsonbRef<'a>,
    k: &str,
    temp_storage: &'a RowArena,
) -> Option<&'a str> {
    let v = jsonb_get_string(a, k)?;
    jsonb_stringify(v.into_datum(), temp_storage)
}

#[sqlfunc(sqlname = "#>", is_infix_op = true)]
fn jsonb_get_path<'a>(mut json: JsonbRef<'a>, b: Array<'a>) -> Option<JsonbRef<'a>> {
    let path = b.elements();
    for key in path.iter() {
        let key = match key {
            Datum::String(s) => s,
            Datum::Null => return None,
            _ => unreachable!("keys in jsonb_get_path known to be strings"),
        };
        let v = match json.into_datum() {
            Datum::Map(map) => map.iter().find(|(k, _)| key == *k).map(|(_k, v)| v),
            Datum::List(list) => {
                let i = strconv::parse_int64(key).ok()?;
                let i = if i >= 0 {
                    usize::cast_from(i.unsigned_abs())
                } else {
                    // index backwards from the end
                    let i = usize::cast_from(i.unsigned_abs());
                    (list.iter().count()).wrapping_sub(i)
                };
                list.iter().nth(i)
            }
            _ => return None,
        }?;
        json = JsonbRef::try_from_result(Ok::<_, ()>(v)).ok()?;
    }
    Some(json)
}

#[sqlfunc(sqlname = "#>>", is_infix_op = true)]
fn jsonb_get_path_stringify<'a>(
    a: JsonbRef<'a>,
    b: Array<'a>,
    temp_storage: &'a RowArena,
) -> Option<&'a str> {
    let json = jsonb_get_path(a, b)?;
    jsonb_stringify(json.into_datum(), temp_storage)
}

#[sqlfunc(is_infix_op = true, sqlname = "?", propagates_nulls = true)]
fn jsonb_contains_string<'a>(a: Datum<'a>, k: &str) -> bool {
    // https://www.postgresql.org/docs/current/datatype-json.html#JSON-CONTAINMENT
    match a {
        Datum::List(list) => list.iter().any(|k2| Datum::from(k) == k2),
        Datum::Map(dict) => dict.iter().any(|(k2, _v)| k == k2),
        Datum::String(string) => string == k,
        _ => false,
    }
}

#[sqlfunc(is_infix_op = true, sqlname = "?", propagates_nulls = true)]
// Map keys are always text.
fn map_contains_key<'a>(map: DatumMap<'a>, k: &str) -> bool {
    map.iter().any(|(k2, _v)| k == k2)
}

#[sqlfunc(is_infix_op = true, sqlname = "?&")]
fn map_contains_all_keys<'a>(map: DatumMap<'a>, keys: Array<'a>) -> bool {
    keys.elements()
        .iter()
        .all(|key| !key.is_null() && map.iter().any(|(k, _v)| k == key.unwrap_str()))
}

#[sqlfunc(is_infix_op = true, sqlname = "?|", propagates_nulls = true)]
fn map_contains_any_keys<'a>(map: DatumMap<'a>, keys: Array<'a>) -> bool {
    keys.elements()
        .iter()
        .any(|key| !key.is_null() && map.iter().any(|(k, _v)| k == key.unwrap_str()))
}

#[sqlfunc(is_infix_op = true, sqlname = "@>", propagates_nulls = true)]
fn map_contains_map<'a>(map_a: DatumMap<'a>, b: DatumMap<'a>) -> bool {
    b.iter().all(|(b_key, b_val)| {
        map_a
            .iter()
            .any(|(a_key, a_val)| (a_key == b_key) && (a_val == b_val))
    })
}

#[sqlfunc(
    output_type_expr = "input_type_a.scalar_type.unwrap_map_value_type().clone().nullable(true)",
    is_infix_op = true,
    sqlname = "->",
    propagates_nulls = true,
    introduces_nulls = true
)]
fn map_get_value<'a>(a: DatumMap<'a>, target_key: &str) -> Datum<'a> {
    match a.iter().find(|(key, _v)| target_key == *key) {
        Some((_k, v)) => v,
        None => Datum::Null,
    }
}

#[sqlfunc(is_infix_op = true, sqlname = "@>")]
fn list_contains_list<'a>(a: ExcludeNull<DatumList<'a>>, b: ExcludeNull<DatumList<'a>>) -> bool {
    // NULL is never equal to NULL. If NULL is an element of b, b cannot be contained in a, even if a contains NULL.
    if b.iter().contains(&Datum::Null) {
        false
    } else {
        b.iter()
            .all(|item_b| a.iter().any(|item_a| item_a == item_b))
    }
}

#[sqlfunc(is_infix_op = true, sqlname = "<@")]
fn list_contains_list_rev<'a>(
    a: ExcludeNull<DatumList<'a>>,
    b: ExcludeNull<DatumList<'a>>,
) -> bool {
    list_contains_list(b, a)
}

// TODO(jamii) nested loops are possibly not the fastest way to do this
#[sqlfunc(is_infix_op = true, sqlname = "@>")]
fn jsonb_contains_jsonb<'a>(a: JsonbRef<'a>, b: JsonbRef<'a>) -> bool {
    // https://www.postgresql.org/docs/current/datatype-json.html#JSON-CONTAINMENT
    fn contains(a: Datum, b: Datum, at_top_level: bool) -> bool {
        match (a, b) {
            (Datum::JsonNull, Datum::JsonNull) => true,
            (Datum::False, Datum::False) => true,
            (Datum::True, Datum::True) => true,
            (Datum::Numeric(a), Datum::Numeric(b)) => a == b,
            (Datum::String(a), Datum::String(b)) => a == b,
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
    contains(a.into_datum(), b.into_datum(), true)
}

#[sqlfunc(is_infix_op = true, sqlname = "||")]
fn jsonb_concat<'a>(
    a: JsonbRef<'a>,
    b: JsonbRef<'a>,
    temp_storage: &'a RowArena,
) -> Option<JsonbRef<'a>> {
    let res = match (a.into_datum(), b.into_datum()) {
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
            let elems = list_a.iter().chain(Some(b));
            temp_storage.make_datum(|packer| packer.push_list(elems))
        }
        (a, Datum::List(list_b)) => {
            let elems = Some(a).into_iter().chain(list_b.iter());
            temp_storage.make_datum(|packer| packer.push_list(elems))
        }
        _ => return None,
    };
    Some(JsonbRef::from_datum(res))
}

#[sqlfunc(
    output_type_expr = "SqlScalarType::Jsonb.nullable(true)",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true,
    introduces_nulls = true
)]
fn jsonb_delete_int64<'a>(a: Datum<'a>, i: i64, temp_storage: &'a RowArena) -> Datum<'a> {
    match a {
        Datum::List(list) => {
            let i = if i >= 0 {
                usize::cast_from(i.unsigned_abs())
            } else {
                // index backwards from the end
                let i = usize::cast_from(i.unsigned_abs());
                (list.iter().count()).wrapping_sub(i)
            };
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

#[sqlfunc(
    output_type_expr = "SqlScalarType::Jsonb.nullable(true)",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true,
    introduces_nulls = true
)]
fn jsonb_delete_string<'a>(a: Datum<'a>, k: &str, temp_storage: &'a RowArena) -> Datum<'a> {
    match a {
        Datum::List(list) => {
            let elems = list.iter().filter(|e| Datum::from(k) != *e);
            temp_storage.make_datum(|packer| packer.push_list(elems))
        }
        Datum::Map(dict) => {
            let pairs = dict.iter().filter(|(k2, _v)| k != *k2);
            temp_storage.make_datum(|packer| packer.push_dict(pairs))
        }
        _ => Datum::Null,
    }
}

#[sqlfunc(
    sqlname = "extractiv",
    propagates_nulls = true,
    introduces_nulls = false
)]
fn date_part_interval_numeric(units: &str, b: Interval) -> Result<Numeric, EvalError> {
    match units.parse() {
        Ok(units) => Ok(date_part_interval_inner::<Numeric>(units, b)?),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(
    sqlname = "date_partiv",
    propagates_nulls = true,
    introduces_nulls = false
)]
fn date_part_interval_f64(units: &str, b: Interval) -> Result<f64, EvalError> {
    match units.parse() {
        Ok(units) => Ok(date_part_interval_inner::<f64>(units, b)?),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(
    sqlname = "extractt",
    propagates_nulls = true,
    introduces_nulls = false
)]
fn date_part_time_numeric(units: &str, b: chrono::NaiveTime) -> Result<Numeric, EvalError> {
    match units.parse() {
        Ok(units) => Ok(date_part_time_inner::<Numeric>(units, b)?),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(
    sqlname = "date_partt",
    propagates_nulls = true,
    introduces_nulls = false
)]
fn date_part_time_f64(units: &str, b: chrono::NaiveTime) -> Result<f64, EvalError> {
    match units.parse() {
        Ok(units) => Ok(date_part_time_inner::<f64>(units, b)?),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(sqlname = "extractts", propagates_nulls = true)]
fn date_part_timestamp_timestamp_numeric(
    units: &str,
    ts: CheckedTimestamp<NaiveDateTime>,
) -> Result<Numeric, EvalError> {
    match units.parse() {
        Ok(units) => Ok(date_part_timestamp_inner::<_, Numeric>(units, &*ts)?),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(sqlname = "extracttstz", propagates_nulls = true)]
fn date_part_timestamp_timestamp_tz_numeric(
    units: &str,
    ts: CheckedTimestamp<DateTime<Utc>>,
) -> Result<Numeric, EvalError> {
    match units.parse() {
        Ok(units) => Ok(date_part_timestamp_inner::<_, Numeric>(units, &*ts)?),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(sqlname = "date_partts", propagates_nulls = true)]
fn date_part_timestamp_timestamp_f64(
    units: &str,
    ts: CheckedTimestamp<NaiveDateTime>,
) -> Result<f64, EvalError> {
    match units.parse() {
        Ok(units) => date_part_timestamp_inner(units, &*ts),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(sqlname = "date_parttstz", propagates_nulls = true)]
fn date_part_timestamp_timestamp_tz_f64(
    units: &str,
    ts: CheckedTimestamp<DateTime<Utc>>,
) -> Result<f64, EvalError> {
    match units.parse() {
        Ok(units) => date_part_timestamp_inner(units, &*ts),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(sqlname = "extractd", propagates_nulls = true)]
fn extract_date_units(units: &str, b: Date) -> Result<Numeric, EvalError> {
    match units.parse() {
        Ok(units) => Ok(extract_date_inner(units, b.into())?),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

pub fn date_bin<T>(
    stride: Interval,
    source: CheckedTimestamp<T>,
    origin: CheckedTimestamp<T>,
) -> Result<CheckedTimestamp<T>, EvalError>
where
    T: TimestampLike,
{
    if stride.months != 0 {
        return Err(EvalError::DateBinOutOfRange(
            "timestamps cannot be binned into intervals containing months or years".into(),
        ));
    }

    let stride_ns = match stride.duration_as_chrono().num_nanoseconds() {
        Some(ns) if ns <= 0 => Err(EvalError::DateBinOutOfRange(
            "stride must be greater than zero".into(),
        )),
        Some(ns) => Ok(ns),
        None => Err(EvalError::DateBinOutOfRange(
            format!("stride cannot exceed {}/{} nanoseconds", i64::MAX, i64::MIN,).into(),
        )),
    }?;

    // Make sure the returned timestamp is at the start of the bin, even if the
    // origin is in the future. We do this here because `T` is not `Copy` and
    // gets moved by its subtraction operation.
    let sub_stride = origin > source;

    let tm_diff = (source - origin.clone()).num_nanoseconds().ok_or_else(|| {
        EvalError::DateBinOutOfRange(
            "source and origin must not differ more than 2^63 nanoseconds".into(),
        )
    })?;

    let mut tm_delta = tm_diff - tm_diff % stride_ns;

    if sub_stride {
        tm_delta -= stride_ns;
    }

    let res = origin
        .checked_add_signed(Duration::nanoseconds(tm_delta))
        .ok_or(EvalError::TimestampOutOfRange)?;
    Ok(CheckedTimestamp::from_timestamplike(res)?)
}

#[sqlfunc(is_monotone = "(true, true)", sqlname = "bin_unix_epoch_timestamp")]
fn date_bin_timestamp(
    stride: Interval,
    source: CheckedTimestamp<NaiveDateTime>,
) -> Result<CheckedTimestamp<NaiveDateTime>, EvalError> {
    let origin =
        CheckedTimestamp::from_timestamplike(DateTime::from_timestamp(0, 0).unwrap().naive_utc())
            .expect("must fit");
    date_bin(stride, source, origin)
}

#[sqlfunc(is_monotone = "(true, true)", sqlname = "bin_unix_epoch_timestamptz")]
fn date_bin_timestamp_tz(
    stride: Interval,
    source: CheckedTimestamp<DateTime<Utc>>,
) -> Result<CheckedTimestamp<DateTime<Utc>>, EvalError> {
    let origin = CheckedTimestamp::from_timestamplike(DateTime::from_timestamp(0, 0).unwrap())
        .expect("must fit");
    date_bin(stride, source, origin)
}

#[sqlfunc(sqlname = "date_truncts", propagates_nulls = true)]
fn date_trunc_units_timestamp(
    units: &str,
    ts: CheckedTimestamp<NaiveDateTime>,
) -> Result<CheckedTimestamp<NaiveDateTime>, EvalError> {
    match units.parse() {
        Ok(units) => Ok(date_trunc_inner(units, &*ts)?.try_into()?),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(sqlname = "date_trunctstz", propagates_nulls = true)]
fn date_trunc_units_timestamp_tz(
    units: &str,
    ts: CheckedTimestamp<DateTime<Utc>>,
) -> Result<CheckedTimestamp<DateTime<Utc>>, EvalError> {
    match units.parse() {
        Ok(units) => Ok(date_trunc_inner(units, &*ts)?.try_into()?),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(sqlname = "date_trunciv", propagates_nulls = true)]
fn date_trunc_interval(units: &str, mut interval: Interval) -> Result<Interval, EvalError> {
    let dtf = units
        .parse()
        .map_err(|_| EvalError::UnknownUnits(units.into()))?;

    interval
        .truncate_low_fields(dtf, Some(0), RoundBehavior::Truncate)
        .expect(
            "truncate_low_fields should not fail with max_precision 0 and RoundBehavior::Truncate",
        );
    Ok(interval)
}

/// Parses a named timezone like `EST` or `America/New_York`, or a fixed-offset timezone like `-05:00`.
///
/// The interpretation of fixed offsets depend on whether the POSIX or ISO 8601 standard is being
/// used.
pub(crate) fn parse_timezone(tz: &str, spec: TimezoneSpec) -> Result<Timezone, EvalError> {
    Timezone::parse(tz, spec).map_err(|_| EvalError::InvalidTimezone(tz.into()))
}

/// Converts the time datum `b`, which is assumed to be in UTC, to the timezone that the interval datum `a` is assumed
/// to represent. The interval is not allowed to hold months, but there are no limits on the amount of seconds.
/// The interval acts like a `chrono::FixedOffset`, without the `-86,400 < x < 86,400` limitation.
#[sqlfunc(sqlname = "timezoneit")]
fn timezone_interval_time_binary(
    interval: Interval,
    time: chrono::NaiveTime,
) -> Result<chrono::NaiveTime, EvalError> {
    if interval.months != 0 {
        Err(EvalError::InvalidTimezoneInterval)
    } else {
        Ok(time.overflowing_add_signed(interval.duration_as_chrono()).0)
    }
}

/// Converts the timestamp datum `b`, which is assumed to be in the time of the timezone datum `a` to a timestamptz
/// in UTC. The interval is not allowed to hold months, but there are no limits on the amount of seconds.
/// The interval acts like a `chrono::FixedOffset`, without the `-86,400 < x < 86,400` limitation.
#[sqlfunc(sqlname = "timezoneits")]
fn timezone_interval_timestamp_binary(
    interval: Interval,
    ts: CheckedTimestamp<NaiveDateTime>,
) -> Result<CheckedTimestamp<DateTime<Utc>>, EvalError> {
    if interval.months != 0 {
        Err(EvalError::InvalidTimezoneInterval)
    } else {
        match ts.checked_sub_signed(interval.duration_as_chrono()) {
            Some(sub) => Ok(DateTime::from_naive_utc_and_offset(sub, Utc).try_into()?),
            None => Err(EvalError::TimestampOutOfRange),
        }
    }
}

/// Converts the UTC timestamptz datum `b`, to the local timestamp of the timezone datum `a`.
/// The interval is not allowed to hold months, but there are no limits on the amount of seconds.
/// The interval acts like a `chrono::FixedOffset`, without the `-86,400 < x < 86,400` limitation.
#[sqlfunc(sqlname = "timezoneitstz")]
fn timezone_interval_timestamp_tz_binary(
    interval: Interval,
    tstz: CheckedTimestamp<DateTime<Utc>>,
) -> Result<CheckedTimestamp<NaiveDateTime>, EvalError> {
    if interval.months != 0 {
        return Err(EvalError::InvalidTimezoneInterval);
    }
    match tstz
        .naive_utc()
        .checked_add_signed(interval.duration_as_chrono())
    {
        Some(dt) => Ok(dt.try_into()?),
        None => Err(EvalError::TimestampOutOfRange),
    }
}

#[sqlfunc(
    output_type_expr = r#"SqlScalarType::Record {
                fields: [
                    ("abbrev".into(), SqlScalarType::String.nullable(false)),
                    ("base_utc_offset".into(), SqlScalarType::Interval.nullable(false)),
                    ("dst_offset".into(), SqlScalarType::Interval.nullable(false)),
                ].into(),
                custom_id: None,
            }.nullable(true)"#,
    propagates_nulls = true,
    introduces_nulls = false
)]
fn timezone_offset<'a>(
    tz_str: &str,
    b: CheckedTimestamp<chrono::DateTime<Utc>>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let tz = match Tz::from_str_insensitive(tz_str) {
        Ok(tz) => tz,
        Err(_) => return Err(EvalError::InvalidIanaTimezoneId(tz_str.into())),
    };
    let offset = tz.offset_from_utc_datetime(&b.naive_utc());
    Ok(temp_storage.make_datum(|packer| {
        packer.push_list_with(|packer| {
            packer.push(Datum::from(offset.abbreviation()));
            packer.push(Datum::from(offset.base_utc_offset()));
            packer.push(Datum::from(offset.dst_offset()));
        });
    }))
}

/// Determines if an mz_aclitem contains one of the specified privileges. This will return true if
/// any of the listed privileges are contained in the mz_aclitem.
#[sqlfunc(
    sqlname = "mz_aclitem_contains_privilege",
    output_type = "bool",
    propagates_nulls = true
)]
fn mz_acl_item_contains_privilege(
    mz_acl_item: MzAclItem,
    privileges: &str,
) -> Result<bool, EvalError> {
    let acl_mode = AclMode::parse_multiple_privileges(privileges)
        .map_err(|e: anyhow::Error| EvalError::InvalidPrivileges(e.to_string().into()))?;
    let contains = !mz_acl_item.acl_mode.intersection(acl_mode).is_empty();
    Ok(contains)
}

#[sqlfunc]
// transliterated from postgres/src/backend/utils/adt/misc.c
fn parse_ident<'a>(ident: &'a str, strict: bool) -> Result<ArrayRustType<Cow<'a, str>>, EvalError> {
    fn is_ident_start(c: char) -> bool {
        matches!(c, 'A'..='Z' | 'a'..='z' | '_' | '\u{80}'..=char::MAX)
    }

    fn is_ident_cont(c: char) -> bool {
        matches!(c, '0'..='9' | '$') || is_ident_start(c)
    }

    let mut elems = vec![];
    let buf = &mut LexBuf::new(ident);

    let mut after_dot = false;

    buf.take_while(|ch| ch.is_ascii_whitespace());

    loop {
        let mut missing_ident = true;

        let c = buf.next();

        if c == Some('"') {
            let s = buf.take_while(|ch| !matches!(ch, '"'));

            if buf.next() != Some('"') {
                return Err(EvalError::InvalidIdentifier {
                    ident: ident.into(),
                    detail: Some("String has unclosed double quotes.".into()),
                });
            }
            elems.push(Cow::Borrowed(s));
            missing_ident = false;
        } else if c.map(is_ident_start).unwrap_or(false) {
            buf.prev();
            let s = buf.take_while(is_ident_cont);
            elems.push(Cow::Owned(s.to_ascii_lowercase()));
            missing_ident = false;
        }

        if missing_ident {
            if c == Some('.') {
                return Err(EvalError::InvalidIdentifier {
                    ident: ident.into(),
                    detail: Some("No valid identifier before \".\".".into()),
                });
            } else if after_dot {
                return Err(EvalError::InvalidIdentifier {
                    ident: ident.into(),
                    detail: Some("No valid identifier after \".\".".into()),
                });
            } else {
                return Err(EvalError::InvalidIdentifier {
                    ident: ident.into(),
                    detail: None,
                });
            }
        }

        buf.take_while(|ch| ch.is_ascii_whitespace());

        match buf.next() {
            Some('.') => {
                after_dot = true;

                buf.take_while(|ch| ch.is_ascii_whitespace());
            }
            Some(_) if strict => {
                return Err(EvalError::InvalidIdentifier {
                    ident: ident.into(),
                    detail: None,
                });
            }
            _ => break,
        }
    }

    Ok(elems.into())
}

fn regexp_split_to_array_re<'a>(
    text: &str,
    regexp: &Regex,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let found = mz_regexp::regexp_split_to_array(text, regexp);
    let mut row = Row::default();
    let mut packer = row.packer();
    packer.try_push_array(
        &[ArrayDimension {
            lower_bound: 1,
            length: found.len(),
        }],
        found.into_iter().map(Datum::String),
    )?;
    Ok(temp_storage.push_unary_row(row))
}

#[sqlfunc(propagates_nulls = true)]
fn pretty_sql<'a>(sql: &str, width: i32, temp_storage: &'a RowArena) -> Result<&'a str, EvalError> {
    let width =
        usize::try_from(width).map_err(|_| EvalError::PrettyError("invalid width".into()))?;
    let pretty = pretty_str(
        sql,
        PrettyConfig {
            width,
            format_mode: FormatMode::Simple,
        },
    )
    .map_err(|e| EvalError::PrettyError(e.to_string().into()))?;
    let pretty = temp_storage.push_string(pretty);
    Ok(pretty)
}

#[sqlfunc(propagates_nulls = true)]
fn starts_with(a: &str, b: &str) -> bool {
    a.starts_with(b)
}

#[sqlfunc(
    sqlname = "||",
    is_infix_op = true,
    propagates_nulls = true,
    // Text concatenation is monotonic in its second argument, because if I change the
    // second argument but don't change the first argument, then we won't find a difference
    // in that part of the concatenation result that came from the first argument, so we'll
    // find the difference that comes from changing the second argument.
    // (It's not monotonic in its first argument, because e.g.,
    // 'A' < 'AA' but 'AZ' > 'AAZ'.)
    is_monotone = (false, true),
)]
fn text_concat_binary(a: &str, b: &str) -> Result<String, EvalError> {
    if a.len() + b.len() > MAX_STRING_FUNC_RESULT_BYTES {
        return Err(EvalError::LengthTooLarge);
    }
    let mut buf = String::with_capacity(a.len() + b.len());
    buf.push_str(a);
    buf.push_str(b);
    Ok(buf)
}

#[sqlfunc(propagates_nulls = true, introduces_nulls = false)]
fn like_escape<'a>(
    pattern: &str,
    b: &str,
    temp_storage: &'a RowArena,
) -> Result<&'a str, EvalError> {
    let escape = like_pattern::EscapeBehavior::from_str(b)?;
    let normalized = like_pattern::normalize_pattern(pattern, escape)?;
    Ok(temp_storage.push_string(normalized))
}

#[sqlfunc(is_infix_op = true, sqlname = "like")]
fn is_like_match_case_sensitive(haystack: &str, pattern: &str) -> Result<bool, EvalError> {
    like_pattern::compile(pattern, false).map(|needle| needle.is_match(haystack))
}

#[sqlfunc(is_infix_op = true, sqlname = "ilike")]
fn is_like_match_case_insensitive(haystack: &str, pattern: &str) -> Result<bool, EvalError> {
    like_pattern::compile(pattern, true).map(|needle| needle.is_match(haystack))
}

#[sqlfunc(is_infix_op = true, sqlname = "~")]
fn is_regexp_match_case_sensitive(haystack: &str, needle: &str) -> Result<bool, EvalError> {
    let regex = build_regex(needle, "")?;
    Ok(regex.is_match(haystack))
}

#[sqlfunc(is_infix_op = true, sqlname = "~*")]
fn is_regexp_match_case_insensitive(haystack: &str, needle: &str) -> Result<bool, EvalError> {
    let regex = build_regex(needle, "i")?;
    Ok(regex.is_match(haystack))
}

fn regexp_match_static<'a>(
    haystack: Datum<'a>,
    temp_storage: &'a RowArena,
    needle: &regex::Regex,
) -> Result<Datum<'a>, EvalError> {
    let mut row = Row::default();
    let mut packer = row.packer();
    if needle.captures_len() > 1 {
        // The regex contains capture groups, so return an array containing the
        // matched text in each capture group, unless the entire match fails.
        // Individual capture groups may also be null if that group did not
        // participate in the match.
        match needle.captures(haystack.unwrap_str()) {
            None => packer.push(Datum::Null),
            Some(captures) => packer.try_push_array(
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
            None => packer.push(Datum::Null),
            Some(mtch) => packer.try_push_array(
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

/// Sets `limit` based on the presence of 'g' in `flags` for use in `Regex::replacen`,
/// and removes 'g' from `flags` if present.
pub(crate) fn regexp_replace_parse_flags(flags: &str) -> (usize, Cow<'_, str>) {
    // 'g' means to replace all instead of the first. Use a Cow to avoid allocating in the fast
    // path. We could switch build_regex to take an iter which would also achieve that.
    let (limit, flags) = if flags.contains('g') {
        let flags = flags.replace('g', "");
        (0, Cow::Owned(flags))
    } else {
        (1, Cow::Borrowed(flags))
    };
    (limit, flags)
}

pub fn build_regex(needle: &str, flags: &str) -> Result<Regex, EvalError> {
    let mut case_insensitive = false;
    // Note: Postgres accepts it when both flags are present, taking the last one. We do the same.
    for f in flags.chars() {
        match f {
            'i' => {
                case_insensitive = true;
            }
            'c' => {
                case_insensitive = false;
            }
            _ => return Err(EvalError::InvalidRegexFlag(f)),
        }
    }
    Ok(Regex::new(needle, case_insensitive)?)
}

#[sqlfunc(sqlname = "repeat")]
fn repeat_string(string: &str, count: i32) -> Result<String, EvalError> {
    let len = usize::try_from(count).unwrap_or(0);
    if (len * string.len()) > MAX_STRING_FUNC_RESULT_BYTES {
        return Err(EvalError::LengthTooLarge);
    }
    Ok(string.repeat(len))
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
    let datum = temp_storage.try_make_datum(|packer| packer.try_push_array(dims, datums))?;
    Ok(datum)
}

fn stringify_datum<'a, B>(
    buf: &mut B,
    d: Datum<'a>,
    ty: &SqlScalarType,
) -> Result<strconv::Nestable, EvalError>
where
    B: FormatBuffer,
{
    use SqlScalarType::*;
    match &ty {
        AclItem => Ok(strconv::format_acl_item(buf, d.unwrap_acl_item())),
        Bool => Ok(strconv::format_bool(buf, d.unwrap_bool())),
        Int16 => Ok(strconv::format_int16(buf, d.unwrap_int16())),
        Int32 => Ok(strconv::format_int32(buf, d.unwrap_int32())),
        Int64 => Ok(strconv::format_int64(buf, d.unwrap_int64())),
        UInt16 => Ok(strconv::format_uint16(buf, d.unwrap_uint16())),
        UInt32 | Oid | RegClass | RegProc | RegType => {
            Ok(strconv::format_uint32(buf, d.unwrap_uint32()))
        }
        UInt64 => Ok(strconv::format_uint64(buf, d.unwrap_uint64())),
        Float32 => Ok(strconv::format_float32(buf, d.unwrap_float32())),
        Float64 => Ok(strconv::format_float64(buf, d.unwrap_float64())),
        Numeric { .. } => Ok(strconv::format_numeric(buf, &d.unwrap_numeric())),
        Date => Ok(strconv::format_date(buf, d.unwrap_date())),
        Time => Ok(strconv::format_time(buf, d.unwrap_time())),
        Timestamp { .. } => Ok(strconv::format_timestamp(buf, &d.unwrap_timestamp())),
        TimestampTz { .. } => Ok(strconv::format_timestamptz(buf, &d.unwrap_timestamptz())),
        Interval => Ok(strconv::format_interval(buf, d.unwrap_interval())),
        Bytes => Ok(strconv::format_bytes(buf, d.unwrap_bytes())),
        String | VarChar { .. } | PgLegacyName => Ok(strconv::format_string(buf, d.unwrap_str())),
        Char { length } => Ok(strconv::format_string(
            buf,
            &mz_repr::adt::char::format_str_pad(d.unwrap_str(), *length),
        )),
        PgLegacyChar => {
            format_pg_legacy_char(buf, d.unwrap_uint8())?;
            Ok(strconv::Nestable::MayNeedEscaping)
        }
        Jsonb => Ok(strconv::format_jsonb(buf, JsonbRef::from_datum(d))),
        Uuid => Ok(strconv::format_uuid(buf, d.unwrap_uuid())),
        Record { fields, .. } => {
            let mut fields = fields.iter();
            strconv::format_record(buf, d.unwrap_list(), |buf, d| {
                let (_name, ty) = fields.next().unwrap();
                if d.is_null() {
                    Ok(buf.write_null())
                } else {
                    stringify_datum(buf.nonnull_buffer(), d, &ty.scalar_type)
                }
            })
        }
        Array(elem_type) => strconv::format_array(
            buf,
            &d.unwrap_array().dims().into_iter().collect::<Vec<_>>(),
            d.unwrap_array().elements(),
            |buf, d| {
                if d.is_null() {
                    Ok(buf.write_null())
                } else {
                    stringify_datum(buf.nonnull_buffer(), d, elem_type)
                }
            },
        ),
        List { element_type, .. } => strconv::format_list(buf, d.unwrap_list(), |buf, d| {
            if d.is_null() {
                Ok(buf.write_null())
            } else {
                stringify_datum(buf.nonnull_buffer(), d, element_type)
            }
        }),
        Map { value_type, .. } => strconv::format_map(buf, &d.unwrap_map(), |buf, d| {
            if d.is_null() {
                Ok(buf.write_null())
            } else {
                stringify_datum(buf.nonnull_buffer(), d, value_type)
            }
        }),
        Int2Vector => strconv::format_legacy_vector(buf, d.unwrap_array().elements(), |buf, d| {
            stringify_datum(buf.nonnull_buffer(), d, &SqlScalarType::Int16)
        }),
        MzTimestamp { .. } => Ok(strconv::format_mz_timestamp(buf, d.unwrap_mz_timestamp())),
        Range { element_type } => strconv::format_range(buf, &d.unwrap_range(), |buf, d| match d {
            Some(d) => stringify_datum(buf.nonnull_buffer(), *d, element_type),
            None => Ok::<_, EvalError>(buf.write_null()),
        }),
        MzAclItem => Ok(strconv::format_mz_acl_item(buf, d.unwrap_mz_acl_item())),
    }
}

#[sqlfunc(propagates_nulls = true)]
fn position(substring: &str, string: &str) -> Result<i32, EvalError> {
    let char_index = string.find(substring);

    if let Some(char_index) = char_index {
        // find the index in char space
        let string_prefix = &string[0..char_index];

        let num_prefix_chars = string_prefix.chars().count();
        let num_prefix_chars = i32::try_from(num_prefix_chars)
            .map_err(|_| EvalError::Int32OutOfRange(num_prefix_chars.to_string().into()))?;

        Ok(num_prefix_chars + 1)
    } else {
        Ok(0)
    }
}

#[sqlfunc(
    propagates_nulls = true,
    // `left` is unfortunately not monotonic (at least for negative second arguments),
    // because 'aa' < 'z', but `left(_, -1)` makes 'a' > ''.
    is_monotone = (false, false)
)]
fn left<'a>(string: &'a str, b: i32) -> Result<&'a str, EvalError> {
    let n = i64::from(b);

    let mut byte_indices = string.char_indices().map(|(i, _)| i);

    let end_in_bytes = match n.cmp(&0) {
        Ordering::Equal => 0,
        Ordering::Greater => {
            let n = usize::try_from(n).map_err(|_| {
                EvalError::InvalidParameterValue(format!("invalid parameter n: {:?}", n).into())
            })?;
            // nth from the back
            byte_indices.nth(n).unwrap_or(string.len())
        }
        Ordering::Less => {
            let n = usize::try_from(n.abs() - 1).map_err(|_| {
                EvalError::InvalidParameterValue(format!("invalid parameter n: {:?}", n).into())
            })?;
            byte_indices.rev().nth(n).unwrap_or(0)
        }
    };

    Ok(&string[..end_in_bytes])
}

#[sqlfunc(propagates_nulls = true)]
fn right<'a>(string: &'a str, n: i32) -> Result<&'a str, EvalError> {
    let mut byte_indices = string.char_indices().map(|(i, _)| i);

    let start_in_bytes = if n == 0 {
        string.len()
    } else if n > 0 {
        let n = usize::try_from(n - 1).map_err(|_| {
            EvalError::InvalidParameterValue(format!("invalid parameter n: {:?}", n).into())
        })?;
        // nth from the back
        byte_indices.rev().nth(n).unwrap_or(0)
    } else if n == i32::MIN {
        // this seems strange but Postgres behaves like this
        0
    } else {
        let n = n.abs();
        let n = usize::try_from(n).map_err(|_| {
            EvalError::InvalidParameterValue(format!("invalid parameter n: {:?}", n).into())
        })?;
        byte_indices.nth(n).unwrap_or(string.len())
    };

    Ok(&string[start_in_bytes..])
}

#[sqlfunc(sqlname = "btrim", propagates_nulls = true)]
fn trim<'a>(a: &'a str, trim_chars: &str) -> &'a str {
    a.trim_matches(|c| trim_chars.contains(c))
}

#[sqlfunc(sqlname = "ltrim", propagates_nulls = true)]
fn trim_leading<'a>(a: &'a str, trim_chars: &str) -> &'a str {
    a.trim_start_matches(|c| trim_chars.contains(c))
}

#[sqlfunc(sqlname = "rtrim", propagates_nulls = true)]
fn trim_trailing<'a>(a: &'a str, trim_chars: &str) -> &'a str {
    a.trim_end_matches(|c| trim_chars.contains(c))
}

#[sqlfunc(
    sqlname = "array_length",
    propagates_nulls = true,
    introduces_nulls = true
)]
fn array_length<'a>(a: Array<'a>, b: i64) -> Result<Option<i32>, EvalError> {
    let i = match usize::try_from(b) {
        Ok(0) | Err(_) => return Ok(None),
        Ok(n) => n - 1,
    };
    Ok(match a.dims().into_iter().nth(i) {
        None => None,
        Some(dim) => Some(
            dim.length
                .try_into()
                .map_err(|_| EvalError::Int32OutOfRange(dim.length.to_string().into()))?,
        ),
    })
}

#[sqlfunc(
    output_type = "Option<i32>",
    is_infix_op = true,
    sqlname = "array_lower",
    propagates_nulls = true,
    introduces_nulls = true
)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn array_lower<'a>(a: Array<'a>, i: i64) -> Option<i32> {
    if i < 1 {
        return None;
    }
    match a.dims().into_iter().nth(i as usize - 1) {
        Some(_) => Some(1),
        None => None,
    }
}

#[sqlfunc(
    output_type_expr = "input_type_a.scalar_type.without_modifiers().nullable(true)",
    sqlname = "array_remove",
    propagates_nulls = false,
    introduces_nulls = false
)]
fn array_remove<'a>(
    arr: Array<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    // Zero-dimensional arrays are empty by definition
    if arr.dims().len() == 0 {
        return Ok(Datum::Array(arr));
    }

    // array_remove only supports one-dimensional arrays
    if arr.dims().len() > 1 {
        return Err(EvalError::MultidimensionalArrayRemovalNotSupported);
    }

    let elems: Vec<_> = arr.elements().iter().filter(|v| v != &b).collect();
    let mut dims = arr.dims().into_iter().collect::<Vec<_>>();
    // This access is safe because `dims` is guaranteed to be non-empty
    dims[0] = ArrayDimension {
        lower_bound: 1,
        length: elems.len(),
    };

    Ok(temp_storage.try_make_datum(|packer| packer.try_push_array(&dims, elems))?)
}

#[sqlfunc(
    output_type = "Option<i32>",
    is_infix_op = true,
    sqlname = "array_upper",
    propagates_nulls = true,
    introduces_nulls = true
)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn array_upper<'a>(a: Array<'a>, i: i64) -> Result<Option<i32>, EvalError> {
    if i < 1 {
        return Ok(None);
    }
    a.dims()
        .into_iter()
        .nth(i as usize - 1)
        .map(|dim| {
            dim.length
                .try_into()
                .map_err(|_| EvalError::Int32OutOfRange(dim.length.to_string().into()))
        })
        .transpose()
}

#[sqlfunc(
    is_infix_op = true,
    sqlname = "array_contains",
    propagates_nulls = true,
    introduces_nulls = false
)]
fn array_contains<'a>(a: Datum<'a>, array: Array<'a>) -> bool {
    array.elements().iter().any(|e| e == a)
}

#[sqlfunc(is_infix_op = true, sqlname = "@>")]
fn array_contains_array<'a>(a: Array<'a>, b: Array<'a>) -> bool {
    let a = a.elements();
    let b = b.elements();

    // NULL is never equal to NULL. If NULL is an element of b, b cannot be contained in a, even if a contains NULL.
    if b.iter().contains(&Datum::Null) {
        false
    } else {
        b.iter()
            .all(|item_b| a.iter().any(|item_a| item_a == item_b))
    }
}

#[sqlfunc(is_infix_op = true, sqlname = "<@")]
fn array_contains_array_rev<'a>(a: Array<'a>, b: Array<'a>) -> bool {
    array_contains_array(b, a)
}

#[sqlfunc(
    output_type_expr = "input_type_a.scalar_type.without_modifiers().nullable(true)",
    is_infix_op = true,
    sqlname = "||",
    propagates_nulls = false,
    introduces_nulls = false
)]
fn array_array_concat<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    if a.is_null() {
        return Ok(b);
    } else if b.is_null() {
        return Ok(a);
    }

    let a_array = a.unwrap_array();
    let b_array = b.unwrap_array();

    let a_dims: Vec<ArrayDimension> = a_array.dims().into_iter().collect();
    let b_dims: Vec<ArrayDimension> = b_array.dims().into_iter().collect();

    let a_ndims = a_dims.len();
    let b_ndims = b_dims.len();

    // Per PostgreSQL, if either of the input arrays is zero dimensional,
    // the output is the other array, no matter their dimensions.
    if a_ndims == 0 {
        return Ok(b);
    } else if b_ndims == 0 {
        return Ok(a);
    }

    // Postgres supports concatenating arrays of different dimensions,
    // as long as one of the arrays has the same type as an element of
    // the other array, i.e. `int[2][4] || int[4]` (or `int[4] || int[2][4]`)
    // works, because each element of `int[2][4]` is an `int[4]`.
    // This check is separate from the one below because Postgres gives a
    // specific error message if the number of dimensions differs by more
    // than one.
    // This cast is safe since MAX_ARRAY_DIMENSIONS is 6
    // Can be replaced by .abs_diff once it is stabilized
    // TODO(benesch): remove potentially dangerous usage of `as`.
    #[allow(clippy::as_conversions)]
    if (a_ndims as isize - b_ndims as isize).abs() > 1 {
        return Err(EvalError::IncompatibleArrayDimensions {
            dims: Some((a_ndims, b_ndims)),
        });
    }

    let mut dims;

    // After the checks above, we are certain that:
    // - neither array is zero dimensional nor empty
    // - both arrays have the same number of dimensions, or differ
    //   at most by one.
    match a_ndims.cmp(&b_ndims) {
        // If both arrays have the same number of dimensions, validate
        // that their inner dimensions are the same and concatenate the
        // arrays.
        Ordering::Equal => {
            if &a_dims[1..] != &b_dims[1..] {
                return Err(EvalError::IncompatibleArrayDimensions { dims: None });
            }
            dims = vec![ArrayDimension {
                lower_bound: a_dims[0].lower_bound,
                length: a_dims[0].length + b_dims[0].length,
            }];
            dims.extend(&a_dims[1..]);
        }
        // If `a` has less dimensions than `b`, this is an element-array
        // concatenation, which requires that `a` has the same dimensions
        // as an element of `b`.
        Ordering::Less => {
            if &a_dims[..] != &b_dims[1..] {
                return Err(EvalError::IncompatibleArrayDimensions { dims: None });
            }
            dims = vec![ArrayDimension {
                lower_bound: b_dims[0].lower_bound,
                // Since `a` is treated as an element of `b`, the length of
                // the first dimension of `b` is incremented by one, as `a` is
                // non-empty.
                length: b_dims[0].length + 1,
            }];
            dims.extend(a_dims);
        }
        // If `a` has more dimensions than `b`, this is an array-element
        // concatenation, which requires that `b` has the same dimensions
        // as an element of `a`.
        Ordering::Greater => {
            if &a_dims[1..] != &b_dims[..] {
                return Err(EvalError::IncompatibleArrayDimensions { dims: None });
            }
            dims = vec![ArrayDimension {
                lower_bound: a_dims[0].lower_bound,
                // Since `b` is treated as an element of `a`, the length of
                // the first dimension of `a` is incremented by one, as `b`
                // is non-empty.
                length: a_dims[0].length + 1,
            }];
            dims.extend(b_dims);
        }
    }

    let elems = a_array.elements().iter().chain(b_array.elements().iter());

    Ok(temp_storage.try_make_datum(|packer| packer.try_push_array(&dims, elems))?)
}

#[sqlfunc(
    output_type_expr = "input_type_a.scalar_type.without_modifiers().nullable(true)",
    is_infix_op = true,
    sqlname = "||",
    propagates_nulls = false,
    introduces_nulls = false
)]
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

#[sqlfunc(
    output_type_expr = "input_type_a.scalar_type.without_modifiers().nullable(true)",
    is_infix_op = true,
    sqlname = "||",
    propagates_nulls = false,
    introduces_nulls = false
)]
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

#[sqlfunc(
    output_type_expr = "input_type_b.scalar_type.without_modifiers().nullable(true)",
    is_infix_op = true,
    sqlname = "||",
    propagates_nulls = false,
    introduces_nulls = false
)]
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

#[sqlfunc(
    output_type_expr = "input_type_a.scalar_type.without_modifiers().nullable(true)",
    sqlname = "list_remove",
    propagates_nulls = false,
    introduces_nulls = false
)]
fn list_remove<'a>(a: DatumList<'a>, b: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    temp_storage.make_datum(|packer| {
        packer.push_list_with(|packer| {
            for elem in a.iter() {
                if elem != b {
                    packer.push(elem);
                }
            }
        })
    })
}

#[sqlfunc(
    output_type = "Vec<u8>",
    sqlname = "digest",
    propagates_nulls = true,
    introduces_nulls = false
)]
fn digest_string<'a>(a: &str, b: &str, temp_storage: &'a RowArena) -> Result<Datum<'a>, EvalError> {
    let to_digest = a.as_bytes();
    digest_inner(to_digest, b, temp_storage)
}

#[sqlfunc(
    output_type = "Vec<u8>",
    sqlname = "digest",
    propagates_nulls = true,
    introduces_nulls = false
)]
fn digest_bytes<'a>(a: &[u8], b: &str, temp_storage: &'a RowArena) -> Result<Datum<'a>, EvalError> {
    let to_digest = a;
    digest_inner(to_digest, b, temp_storage)
}

fn digest_inner<'a>(
    bytes: &[u8],
    digest_fn: &str,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let bytes = match digest_fn {
        "md5" => Md5::digest(bytes).to_vec(),
        "sha1" => Sha1::digest(bytes).to_vec(),
        "sha224" => Sha224::digest(bytes).to_vec(),
        "sha256" => Sha256::digest(bytes).to_vec(),
        "sha384" => Sha384::digest(bytes).to_vec(),
        "sha512" => Sha512::digest(bytes).to_vec(),
        other => return Err(EvalError::InvalidHashAlgorithm(other.into())),
    };
    Ok(Datum::Bytes(temp_storage.push_bytes(bytes)))
}

#[sqlfunc(
    output_type = "String",
    sqlname = "mz_render_typmod",
    propagates_nulls = true,
    introduces_nulls = false
)]
fn mz_render_typmod<'a>(
    oid: u32,
    typmod: i32,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let s = match Type::from_oid_and_typmod(oid, typmod) {
        Ok(typ) => typ.constraint().display_or("").to_string(),
        // Match dubious PostgreSQL behavior of outputting the unmodified
        // `typmod` when positive if the type OID/typmod is invalid.
        Err(_) if typmod >= 0 => format!("({typmod})"),
        Err(_) => "".into(),
    };
    Ok(Datum::String(temp_storage.push_string(s)))
}

#[cfg(test)]
mod test {
    use chrono::prelude::*;
    use mz_repr::PropDatum;
    use proptest::prelude::*;

    use super::*;
    use crate::MirScalarExpr;

    #[mz_ore::test]
    fn add_interval_months() {
        let dt = ym(2000, 1);

        assert_eq!(add_timestamp_months(&*dt, 0).unwrap(), dt);
        assert_eq!(add_timestamp_months(&*dt, 1).unwrap(), ym(2000, 2));
        assert_eq!(add_timestamp_months(&*dt, 12).unwrap(), ym(2001, 1));
        assert_eq!(add_timestamp_months(&*dt, 13).unwrap(), ym(2001, 2));
        assert_eq!(add_timestamp_months(&*dt, 24).unwrap(), ym(2002, 1));
        assert_eq!(add_timestamp_months(&*dt, 30).unwrap(), ym(2002, 7));

        // and negatives
        assert_eq!(add_timestamp_months(&*dt, -1).unwrap(), ym(1999, 12));
        assert_eq!(add_timestamp_months(&*dt, -12).unwrap(), ym(1999, 1));
        assert_eq!(add_timestamp_months(&*dt, -13).unwrap(), ym(1998, 12));
        assert_eq!(add_timestamp_months(&*dt, -24).unwrap(), ym(1998, 1));
        assert_eq!(add_timestamp_months(&*dt, -30).unwrap(), ym(1997, 7));

        // and going over a year boundary by less than a year
        let dt = ym(1999, 12);
        assert_eq!(add_timestamp_months(&*dt, 1).unwrap(), ym(2000, 1));
        let end_of_month_dt = NaiveDate::from_ymd_opt(1999, 12, 31)
            .unwrap()
            .and_hms_opt(9, 9, 9)
            .unwrap();
        assert_eq!(
            // leap year
            add_timestamp_months(&end_of_month_dt, 2).unwrap(),
            NaiveDate::from_ymd_opt(2000, 2, 29)
                .unwrap()
                .and_hms_opt(9, 9, 9)
                .unwrap()
                .try_into()
                .unwrap(),
        );
        assert_eq!(
            // not leap year
            add_timestamp_months(&end_of_month_dt, 14).unwrap(),
            NaiveDate::from_ymd_opt(2001, 2, 28)
                .unwrap()
                .and_hms_opt(9, 9, 9)
                .unwrap()
                .try_into()
                .unwrap(),
        );
    }

    fn ym(year: i32, month: u32) -> CheckedTimestamp<NaiveDateTime> {
        NaiveDate::from_ymd_opt(year, month, 1)
            .unwrap()
            .and_hms_opt(9, 9, 9)
            .unwrap()
            .try_into()
            .unwrap()
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_is_monotone() {
        use proptest::prelude::*;

        /// Asserts that the function is either monotonically increasing or decreasing over
        /// the given sets of arguments.
        fn assert_monotone<'a, const N: usize>(
            expr: &MirScalarExpr,
            arena: &'a RowArena,
            datums: &[[Datum<'a>; N]],
        ) {
            // TODO: assertions for nulls, errors
            let Ok(results) = datums
                .iter()
                .map(|args| expr.eval(args.as_slice(), arena))
                .collect::<Result<Vec<_>, _>>()
            else {
                return;
            };

            let forward = results.iter().tuple_windows().all(|(a, b)| a <= b);
            let reverse = results.iter().tuple_windows().all(|(a, b)| a >= b);
            assert!(
                forward || reverse,
                "expected {expr} to be monotone, but passing {datums:?} returned {results:?}"
            );
        }

        fn proptest_binary<'a>(
            func: BinaryFunc,
            arena: &'a RowArena,
            left: impl Strategy<Value = PropDatum>,
            right: impl Strategy<Value = PropDatum>,
        ) {
            let (left_monotone, right_monotone) = func.is_monotone();
            let expr = MirScalarExpr::CallBinary {
                func,
                expr1: Box::new(MirScalarExpr::column(0)),
                expr2: Box::new(MirScalarExpr::column(1)),
            };
            proptest!(|(
                mut left in proptest::array::uniform3(left),
                mut right in proptest::array::uniform3(right),
            )| {
                left.sort();
                right.sort();
                if left_monotone {
                    for r in &right {
                        let args: Vec<[_; 2]> = left
                            .iter()
                            .map(|l| [Datum::from(l), Datum::from(r)])
                            .collect();
                        assert_monotone(&expr, arena, &args);
                    }
                }
                if right_monotone {
                    for l in &left {
                        let args: Vec<[_; 2]> = right
                            .iter()
                            .map(|r| [Datum::from(l), Datum::from(r)])
                            .collect();
                        assert_monotone(&expr, arena, &args);
                    }
                }
            });
        }

        let interesting_strs: Vec<_> = SqlScalarType::String.interesting_datums().collect();
        let str_datums = proptest::strategy::Union::new([
            proptest::string::string_regex("[A-Z]{0,10}")
                .expect("valid regex")
                .prop_map(|s| PropDatum::String(s.to_string()))
                .boxed(),
            (0..interesting_strs.len())
                .prop_map(move |i| {
                    let Datum::String(val) = interesting_strs[i] else {
                        unreachable!("interesting strings has non-strings")
                    };
                    PropDatum::String(val.to_string())
                })
                .boxed(),
        ]);

        let interesting_i32s: Vec<Datum<'static>> =
            SqlScalarType::Int32.interesting_datums().collect();
        let i32_datums = proptest::strategy::Union::new([
            any::<i32>().prop_map(PropDatum::Int32).boxed(),
            (0..interesting_i32s.len())
                .prop_map(move |i| {
                    let Datum::Int32(val) = interesting_i32s[i] else {
                        unreachable!("interesting int32 has non-i32s")
                    };
                    PropDatum::Int32(val)
                })
                .boxed(),
            (-10i32..10).prop_map(PropDatum::Int32).boxed(),
        ]);

        let arena = RowArena::new();

        // It would be interesting to test all funcs here, but we currently need to hardcode
        // the generators for the argument types, which makes this tedious. Choose an interesting
        // subset for now.
        proptest_binary(
            BinaryFunc::AddInt32(AddInt32),
            &arena,
            &i32_datums,
            &i32_datums,
        );
        proptest_binary(SubInt32.into(), &arena, &i32_datums, &i32_datums);
        proptest_binary(MulInt32.into(), &arena, &i32_datums, &i32_datums);
        proptest_binary(DivInt32.into(), &arena, &i32_datums, &i32_datums);
        proptest_binary(TextConcatBinary.into(), &arena, &str_datums, &str_datums);
        proptest_binary(Left.into(), &arena, &str_datums, &i32_datums);
    }
}
