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
use std::{fmt, iter, str};

use ::encoding::DecoderTrap;
use ::encoding::label::encoding_from_whatwg_label;
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc};
use chrono_tz::{OffsetComponents, OffsetName, Tz};
use dec::OrderedDecimal;
use itertools::Itertools;
use md5::{Digest, Md5};
use mz_expr_derive::sqlfunc;
use mz_lowertest::MzReflect;
use mz_ore::cast::{self, CastFrom};
use mz_ore::fmt::FormatBuffer;
use mz_ore::lex::LexBuf;
use mz_ore::option::OptionExt;
use mz_ore::result::ResultExt;
use mz_ore::str::StrExt;
use mz_pgrepr::Type;
use mz_pgtz::timezone::{Timezone, TimezoneSpec};
use mz_repr::adt::array::{Array, ArrayDimension};
use mz_repr::adt::date::Date;
use mz_repr::adt::interval::{Interval, RoundBehavior};
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::adt::numeric::{self, Numeric};
use mz_repr::adt::range::{Range, RangeOps};
use mz_repr::adt::regex::Regex;
use mz_repr::adt::timestamp::{CheckedTimestamp, TimestampLike};
use mz_repr::{
    Datum, DatumList, DatumMap, DatumType, ExcludeNull, Row, RowArena, SqlColumnType,
    SqlScalarType, strconv,
};
use mz_sql_parser::ast::display::FormatMode;
use mz_sql_pretty::{PrettyConfig, pretty_str};
use num::traits::CheckedNeg;
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Sha224, Sha256, Sha384, Sha512};
use subtle::ConstantTimeEq;

use crate::func::binary::LazyBinaryFunc;
use crate::scalar::func::format::DateTimeFormat;
use crate::{EvalError, MirScalarExpr, like_pattern};

#[macro_use]
mod macros;
mod binary;
mod encoding;
pub(crate) mod format;
pub(crate) mod impls;
mod unary;
mod unmaterializable;
mod variadic;

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

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "CheckedTimestamp<NaiveDateTime>",
    is_infix_op = true,
    sqlname = "+"
)]
fn add_timestamp_interval<'a>(
    a: CheckedTimestamp<NaiveDateTime>,
    b: Interval,
) -> Result<Datum<'a>, EvalError> {
    add_timestamplike_interval(a, b)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "CheckedTimestamp<DateTime<Utc>>",
    is_infix_op = true,
    sqlname = "+"
)]
fn add_timestamp_tz_interval<'a>(
    a: CheckedTimestamp<DateTime<Utc>>,
    b: Interval,
) -> Result<Datum<'a>, EvalError> {
    add_timestamplike_interval(a, b)
}

fn add_timestamplike_interval<'a, T>(
    a: CheckedTimestamp<T>,
    b: Interval,
) -> Result<Datum<'a>, EvalError>
where
    T: TimestampLike,
{
    let dt = a.date_time();
    let dt = add_timestamp_months(&dt, b.months)?;
    let dt = dt
        .checked_add_signed(b.duration_as_chrono())
        .ok_or(EvalError::TimestampOutOfRange)?;
    T::from_date_time(dt).try_into().err_into()
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "CheckedTimestamp<NaiveDateTime>",
    is_infix_op = true,
    sqlname = "-"
)]
fn sub_timestamp_interval<'a>(
    a: CheckedTimestamp<NaiveDateTime>,
    b: Interval,
) -> Result<Datum<'a>, EvalError> {
    sub_timestamplike_interval(a, b)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "CheckedTimestamp<DateTime<Utc>>",
    is_infix_op = true,
    sqlname = "-"
)]
fn sub_timestamp_tz_interval<'a>(
    a: CheckedTimestamp<DateTime<Utc>>,
    b: Interval,
) -> Result<Datum<'a>, EvalError> {
    sub_timestamplike_interval(a, b)
}

fn sub_timestamplike_interval<'a, T>(
    a: CheckedTimestamp<T>,
    b: Interval,
) -> Result<Datum<'a>, EvalError>
where
    T: TimestampLike,
{
    neg_interval_inner(b).and_then(|i| add_timestamplike_interval(a, i))
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "CheckedTimestamp<NaiveDateTime>",
    is_infix_op = true,
    sqlname = "+",
    propagates_nulls = true
)]
fn add_date_time<'a>(date: Date, time: chrono::NaiveTime) -> Result<Datum<'a>, EvalError> {
    let dt = NaiveDate::from(date)
        .and_hms_nano_opt(time.hour(), time.minute(), time.second(), time.nanosecond())
        .unwrap();
    Ok(dt.try_into()?)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "CheckedTimestamp<NaiveDateTime>",
    is_infix_op = true,
    sqlname = "+",
    propagates_nulls = true
)]
fn add_date_interval<'a>(date: Date, interval: Interval) -> Result<Datum<'a>, EvalError> {
    let dt = NaiveDate::from(date).and_hms_opt(0, 0, 0).unwrap();
    let dt = add_timestamp_months(&dt, interval.months)?;
    let dt = dt
        .checked_add_signed(interval.duration_as_chrono())
        .ok_or(EvalError::TimestampOutOfRange)?;
    Ok(dt.try_into()?)
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

#[sqlfunc(propagates_nulls = true)]
fn encode<'a>(
    bytes: &[u8],
    format: &str,
    temp_storage: &'a RowArena,
) -> Result<&'a str, EvalError> {
    let format = encoding::lookup_format(format)?;
    let out = format.encode(bytes);
    Ok(temp_storage.push_string(out))
}

#[sqlfunc(propagates_nulls = true)]
fn decode<'a>(
    string: &str,
    format: &str,
    temp_storage: &'a RowArena,
) -> Result<&'a [u8], EvalError> {
    let format = encoding::lookup_format(format)?;
    let out = format.decode(string)?;
    if out.len() > MAX_STRING_FUNC_RESULT_BYTES {
        return Err(EvalError::LengthTooLarge);
    }
    Ok(temp_storage.push_bytes(out))
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

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "Interval",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_timestamp<'a>(
    a: CheckedTimestamp<chrono::NaiveDateTime>,
    b: CheckedTimestamp<chrono::NaiveDateTime>,
) -> Datum<'a> {
    Datum::from(a - b)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "Interval",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_timestamp_tz<'a>(
    a: CheckedTimestamp<chrono::DateTime<Utc>>,
    b: CheckedTimestamp<chrono::DateTime<Utc>>,
) -> Datum<'a> {
    Datum::from(a - b)
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

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "Interval",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_time<'a>(a: chrono::NaiveTime, b: chrono::NaiveTime) -> Datum<'a> {
    Datum::from(a - b)
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

fn contains_range_elem<'a, R: RangeOps<'a>>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a>
where
    <R as TryFrom<Datum<'a>>>::Error: std::fmt::Debug,
{
    let range = a.unwrap_range();
    let elem = R::try_from(b).expect("type checking must produce correct R");
    Datum::from(range.contains_elem(&elem))
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

pub fn date_bin<'a, T>(
    stride: Interval,
    source: CheckedTimestamp<T>,
    origin: CheckedTimestamp<T>,
) -> Result<Datum<'a>, EvalError>
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
    Ok(res.try_into()?)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "CheckedTimestamp<NaiveDateTime>",
    sqlname = "bin_unix_epoch_timestamp",
    propagates_nulls = true
)]
fn date_bin_timestamp<'a>(
    stride: Interval,
    source: CheckedTimestamp<NaiveDateTime>,
) -> Result<Datum<'a>, EvalError> {
    let origin =
        CheckedTimestamp::from_timestamplike(DateTime::from_timestamp(0, 0).unwrap().naive_utc())
            .expect("must fit");
    date_bin(stride, source, origin)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "CheckedTimestamp<DateTime<Utc>>",
    sqlname = "bin_unix_epoch_timestamptz",
    propagates_nulls = true
)]
fn date_bin_timestamp_tz<'a>(
    stride: Interval,
    source: CheckedTimestamp<DateTime<Utc>>,
) -> Result<Datum<'a>, EvalError> {
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
        match b
            .unwrap_timestamp()
            .checked_sub_signed(interval.duration_as_chrono())
        {
            Some(sub) => Ok(DateTime::from_naive_utc_and_offset(sub, Utc).try_into()?),
            None => Err(EvalError::TimestampOutOfRange),
        }
    }
}

/// Converts the UTC timestamptz datum `b`, to the local timestamp of the timezone datum `a`.
/// The interval is not allowed to hold months, but there are no limits on the amount of seconds.
/// The interval acts like a `chrono::FixedOffset`, without the `-86,400 < x < 86,400` limitation.
fn timezone_interval_timestamptz(a: Datum<'_>, b: Datum<'_>) -> Result<Datum<'static>, EvalError> {
    let interval = a.unwrap_interval();
    if interval.months != 0 {
        return Err(EvalError::InvalidTimezoneInterval);
    }
    match b
        .unwrap_timestamptz()
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

#[sqlfunc(
    output_type = "mz_repr::ArrayRustType<String>",
    propagates_nulls = true
)]
// transliterated from postgres/src/backend/utils/adt/misc.c
fn parse_ident<'a>(
    ident: &str,
    strict: bool,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
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
            elems.push(Datum::String(s));
            missing_ident = false;
        } else if c.map(is_ident_start).unwrap_or(false) {
            buf.prev();
            let s = buf.take_while(is_ident_cont);
            let s = temp_storage.push_string(s.to_ascii_lowercase());
            elems.push(Datum::String(s));
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

    Ok(temp_storage.try_make_datum(|packer| {
        packer.try_push_array(
            &[ArrayDimension {
                lower_bound: 1,
                length: elems.len(),
            }],
            elems,
        )
    })?)
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

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub enum BinaryFunc {
    AddInt16(AddInt16),
    AddInt32(AddInt32),
    AddInt64(AddInt64),
    AddUint16(AddUint16),
    AddUint32(AddUint32),
    AddUint64(AddUint64),
    AddFloat32(AddFloat32),
    AddFloat64(AddFloat64),
    AddInterval(AddInterval),
    AddTimestampInterval(AddTimestampInterval),
    AddTimestampTzInterval(AddTimestampTzInterval),
    AddDateInterval(AddDateInterval),
    AddDateTime(AddDateTime),
    AddTimeInterval(AddTimeInterval),
    AddNumeric(AddNumeric),
    AgeTimestamp(AgeTimestamp),
    AgeTimestampTz(AgeTimestampTz),
    BitAndInt16(BitAndInt16),
    BitAndInt32(BitAndInt32),
    BitAndInt64(BitAndInt64),
    BitAndUint16(BitAndUint16),
    BitAndUint32(BitAndUint32),
    BitAndUint64(BitAndUint64),
    BitOrInt16(BitOrInt16),
    BitOrInt32(BitOrInt32),
    BitOrInt64(BitOrInt64),
    BitOrUint16(BitOrUint16),
    BitOrUint32(BitOrUint32),
    BitOrUint64(BitOrUint64),
    BitXorInt16(BitXorInt16),
    BitXorInt32(BitXorInt32),
    BitXorInt64(BitXorInt64),
    BitXorUint16(BitXorUint16),
    BitXorUint32(BitXorUint32),
    BitXorUint64(BitXorUint64),
    BitShiftLeftInt16(BitShiftLeftInt16),
    BitShiftLeftInt32(BitShiftLeftInt32),
    BitShiftLeftInt64(BitShiftLeftInt64),
    BitShiftLeftUint16(BitShiftLeftUint16),
    BitShiftLeftUint32(BitShiftLeftUint32),
    BitShiftLeftUint64(BitShiftLeftUint64),
    BitShiftRightInt16(BitShiftRightInt16),
    BitShiftRightInt32(BitShiftRightInt32),
    BitShiftRightInt64(BitShiftRightInt64),
    BitShiftRightUint16(BitShiftRightUint16),
    BitShiftRightUint32(BitShiftRightUint32),
    BitShiftRightUint64(BitShiftRightUint64),
    SubInt16(SubInt16),
    SubInt32(SubInt32),
    SubInt64(SubInt64),
    SubUint16(SubUint16),
    SubUint32(SubUint32),
    SubUint64(SubUint64),
    SubFloat32(SubFloat32),
    SubFloat64(SubFloat64),
    SubInterval(SubInterval),
    SubTimestamp(SubTimestamp),
    SubTimestampTz(SubTimestampTz),
    SubTimestampInterval(SubTimestampInterval),
    SubTimestampTzInterval(SubTimestampTzInterval),
    SubDate(SubDate),
    SubDateInterval(SubDateInterval),
    SubTime(SubTime),
    SubTimeInterval(SubTimeInterval),
    SubNumeric(SubNumeric),
    MulInt16(MulInt16),
    MulInt32(MulInt32),
    MulInt64(MulInt64),
    MulUint16(MulUint16),
    MulUint32(MulUint32),
    MulUint64(MulUint64),
    MulFloat32(MulFloat32),
    MulFloat64(MulFloat64),
    MulNumeric(MulNumeric),
    MulInterval(MulInterval),
    DivInt16(DivInt16),
    DivInt32(DivInt32),
    DivInt64(DivInt64),
    DivUint16(DivUint16),
    DivUint32(DivUint32),
    DivUint64(DivUint64),
    DivFloat32(DivFloat32),
    DivFloat64(DivFloat64),
    DivNumeric(DivNumeric),
    DivInterval(DivInterval),
    ModInt16(ModInt16),
    ModInt32(ModInt32),
    ModInt64(ModInt64),
    ModUint16(ModUint16),
    ModUint32(ModUint32),
    ModUint64(ModUint64),
    ModFloat32(ModFloat32),
    ModFloat64(ModFloat64),
    ModNumeric(ModNumeric),
    RoundNumeric(RoundNumericBinary),
    Eq(Eq),
    NotEq(NotEq),
    Lt(Lt),
    Lte(Lte),
    Gt(Gt),
    Gte(Gte),
    LikeEscape(LikeEscape),
    IsLikeMatchCaseInsensitive(IsLikeMatchCaseInsensitive),
    IsLikeMatchCaseSensitive(IsLikeMatchCaseSensitive),
    IsRegexpMatch { case_insensitive: bool },
    ToCharTimestamp(ToCharTimestampFormat),
    ToCharTimestampTz(ToCharTimestampTzFormat),
    DateBinTimestamp(DateBinTimestamp),
    DateBinTimestampTz(DateBinTimestampTz),
    ExtractInterval(DatePartIntervalNumeric),
    ExtractTime(DatePartTimeNumeric),
    ExtractTimestamp(DatePartTimestampTimestampNumeric),
    ExtractTimestampTz(DatePartTimestampTimestampTzNumeric),
    ExtractDate(ExtractDateUnits),
    DatePartInterval(DatePartIntervalF64),
    DatePartTime(DatePartTimeF64),
    DatePartTimestamp(DatePartTimestampTimestampF64),
    DatePartTimestampTz(DatePartTimestampTimestampTzF64),
    DateTruncTimestamp(DateTruncUnitsTimestamp),
    DateTruncTimestampTz(DateTruncUnitsTimestampTz),
    DateTruncInterval(DateTruncInterval),
    TimezoneTimestamp,
    TimezoneTimestampTz,
    TimezoneIntervalTimestamp,
    TimezoneIntervalTimestampTz,
    TimezoneIntervalTime,
    TimezoneOffset(TimezoneOffset),
    TextConcat(TextConcatBinary),
    JsonbGetInt64(JsonbGetInt64),
    JsonbGetInt64Stringify(JsonbGetInt64Stringify),
    JsonbGetString(JsonbGetString),
    JsonbGetStringStringify(JsonbGetStringStringify),
    JsonbGetPath(JsonbGetPath),
    JsonbGetPathStringify(JsonbGetPathStringify),
    JsonbContainsString(JsonbContainsString),
    JsonbConcat(JsonbConcat),
    JsonbContainsJsonb(JsonbContainsJsonb),
    JsonbDeleteInt64(JsonbDeleteInt64),
    JsonbDeleteString(JsonbDeleteString),
    MapContainsKey(MapContainsKey),
    MapGetValue(MapGetValue),
    MapContainsAllKeys(MapContainsAllKeys),
    MapContainsAnyKeys(MapContainsAnyKeys),
    MapContainsMap(MapContainsMap),
    ConvertFrom(ConvertFrom),
    Left(Left),
    Position(Position),
    Right(Right),
    RepeatString,
    Normalize,
    Trim(Trim),
    TrimLeading(TrimLeading),
    TrimTrailing(TrimTrailing),
    EncodedBytesCharLength(EncodedBytesCharLength),
    ListLengthMax(ListLengthMax),
    ArrayContains(ArrayContains),
    ArrayContainsArray(ArrayContainsArray),
    ArrayContainsArrayRev(ArrayContainsArrayRev),
    ArrayLength(ArrayLength),
    ArrayLower(ArrayLower),
    ArrayRemove(ArrayRemove),
    ArrayUpper(ArrayUpper),
    ArrayArrayConcat(ArrayArrayConcat),
    ListListConcat(ListListConcat),
    ListElementConcat(ListElementConcat),
    ElementListConcat(ElementListConcat),
    ListRemove(ListRemove),
    ListContainsList(ListContainsList),
    ListContainsListRev(ListContainsListRev),
    DigestString(DigestString),
    DigestBytes(DigestBytes),
    MzRenderTypmod(MzRenderTypmod),
    Encode(Encode),
    Decode(Decode),
    LogNumeric(LogBaseNumeric),
    Power(Power),
    PowerNumeric(PowerNumeric),
    GetBit(GetBit),
    GetByte(GetByte),
    ConstantTimeEqBytes(ConstantTimeEqBytes),
    ConstantTimeEqString(ConstantTimeEqString),
    RangeContainsElem { elem_type: SqlScalarType, rev: bool },
    RangeContainsRange { rev: bool },
    RangeOverlaps(RangeOverlaps),
    RangeAfter(RangeAfter),
    RangeBefore(RangeBefore),
    RangeOverleft(RangeOverleft),
    RangeOverright(RangeOverright),
    RangeAdjacent(RangeAdjacent),
    RangeUnion(RangeUnion),
    RangeIntersection(RangeIntersection),
    RangeDifference(RangeDifference),
    UuidGenerateV5(UuidGenerateV5),
    MzAclItemContainsPrivilege(MzAclItemContainsPrivilege),
    ParseIdent(ParseIdent),
    PrettySql(PrettySql),
    RegexpReplace { regex: Regex, limit: usize },
    StartsWith(StartsWith),
}

impl BinaryFunc {
    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a_expr: &'a MirScalarExpr,
        b_expr: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        match self {
            BinaryFunc::AddInt16(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::AddInt32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::AddInt64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::AddUint16(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::AddUint32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::AddUint64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::AddFloat32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::AddFloat64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::AddInterval(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::AddTimestampInterval(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::AddTimestampTzInterval(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::AddDateTime(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::AddDateInterval(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::AddTimeInterval(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::AddNumeric(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::AgeTimestamp(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::AgeTimestampTz(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::BitAndInt16(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::BitAndInt32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::BitAndInt64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::BitAndUint16(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::BitAndUint32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::BitAndUint64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::BitOrInt16(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::BitOrInt32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::BitOrInt64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::BitOrUint16(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::BitOrUint32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::BitOrUint64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::BitXorInt16(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::BitXorInt32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::BitXorInt64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::BitXorUint16(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::BitXorUint32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::BitXorUint64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::BitShiftLeftInt16(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::BitShiftLeftInt32(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::BitShiftLeftInt64(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::BitShiftLeftUint16(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::BitShiftLeftUint32(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::BitShiftLeftUint64(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::BitShiftRightInt16(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::BitShiftRightInt32(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::BitShiftRightInt64(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::BitShiftRightUint16(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::BitShiftRightUint32(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::BitShiftRightUint64(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::SubInt16(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::SubInt32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::SubInt64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::SubUint16(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::SubUint32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::SubUint64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::SubFloat32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::SubFloat64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::SubTimestamp(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::SubTimestampTz(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::SubTimestampInterval(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::SubTimestampTzInterval(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::SubInterval(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::SubDate(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::SubDateInterval(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::SubTime(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::SubTimeInterval(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::SubNumeric(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::MulInt16(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::MulInt32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::MulInt64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::MulUint16(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::MulUint32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::MulUint64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::MulFloat32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::MulFloat64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::MulNumeric(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::MulInterval(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::DivInt16(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::DivInt32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::DivInt64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::DivUint16(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::DivUint32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::DivUint64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::DivFloat32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::DivFloat64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::DivNumeric(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::DivInterval(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ModInt16(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ModInt32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ModInt64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ModUint16(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ModUint32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ModUint64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ModFloat32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ModFloat64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ModNumeric(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::Eq(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::NotEq(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::Lt(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::Lte(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::Gt(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::Gte(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::LikeEscape(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::IsLikeMatchCaseInsensitive(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::IsLikeMatchCaseSensitive(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            // BinaryFunc::IsRegexpMatch { case_insensitive } => {
            //     is_regexp_match_dynamic(a, b, *case_insensitive)
            // }
            BinaryFunc::ToCharTimestamp(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ToCharTimestampTz(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::DateBinTimestamp(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::DateBinTimestampTz(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::ExtractInterval(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ExtractTime(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ExtractTimestamp(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ExtractTimestampTz(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::ExtractDate(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::DatePartInterval(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::DatePartTime(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::DatePartTimestamp(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::DatePartTimestampTz(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::DateTruncTimestamp(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::DateTruncInterval(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::DateTruncTimestampTz(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            // BinaryFunc::TimezoneTimestamp(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            // BinaryFunc::TimezoneTimestampTz(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            // BinaryFunc::TimezoneIntervalTimestamp(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            // BinaryFunc::TimezoneIntervalTimestampTz(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            // BinaryFunc::TimezoneIntervalTime(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::TimezoneOffset(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::TextConcat(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::JsonbGetInt64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::JsonbGetInt64Stringify(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::JsonbGetString(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::JsonbGetStringStringify(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::JsonbGetPath(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::JsonbGetPathStringify(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::JsonbContainsString(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::JsonbConcat(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::JsonbContainsJsonb(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::JsonbDeleteInt64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::JsonbDeleteString(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::MapContainsKey(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::MapGetValue(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::MapContainsAllKeys(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::MapContainsAnyKeys(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::MapContainsMap(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::RoundNumeric(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ConvertFrom(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::Encode(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::Decode(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::Left(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::Position(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::Right(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::Trim(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::TrimLeading(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::TrimTrailing(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::EncodedBytesCharLength(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::ListLengthMax(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ArrayLength(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ArrayContains(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ArrayContainsArray(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::ArrayContainsArrayRev(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::ArrayLower(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ArrayRemove(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ArrayUpper(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ArrayArrayConcat(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ListListConcat(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ListElementConcat(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::ElementListConcat(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::ListRemove(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ListContainsList(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ListContainsListRev(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::DigestString(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::DigestBytes(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::MzRenderTypmod(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::LogNumeric(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::Power(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::PowerNumeric(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            // BinaryFunc::RepeatString(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            // BinaryFunc::Normalize(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::GetBit(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::GetByte(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::ConstantTimeEqBytes(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::ConstantTimeEqString(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            // BinaryFunc::RangeContainsElem { elem_type, rev: _ } => Ok(match elem_type {
            //     SqlScalarType::Int32 => contains_range_elem::<i32>(a, b),
            //     SqlScalarType::Int64 => contains_range_elem::<i64>(a, b),
            //     SqlScalarType::Date => contains_range_elem::<Date>(a, b),
            //     SqlScalarType::Numeric { .. } => {
            //         contains_range_elem::<OrderedDecimal<Numeric>>(a, b)
            //     }
            //     SqlScalarType::Timestamp { .. } => {
            //         contains_range_elem::<CheckedTimestamp<NaiveDateTime>>(a, b)
            //     }
            //     SqlScalarType::TimestampTz { .. } => {
            //         contains_range_elem::<CheckedTimestamp<DateTime<Utc>>>(a, b)
            //     }
            //     _ => unreachable!(),
            // }),
            // BinaryFunc::RangeContainsRange { rev: false } => Ok(range_contains_range(a, b)),
            // BinaryFunc::RangeContainsRange { rev: true } => Ok(range_contains_range_rev(a, b)),
            BinaryFunc::RangeOverlaps(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::RangeAfter(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::RangeBefore(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::RangeOverleft(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::RangeOverright(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::RangeAdjacent(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::RangeUnion(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::RangeIntersection(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::RangeDifference(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::UuidGenerateV5(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::MzAclItemContainsPrivilege(s) => {
                return s.eval(datums, temp_storage, a_expr, b_expr);
            }
            BinaryFunc::ParseIdent(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::PrettySql(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            // BinaryFunc::RegexpReplace { regex, limit } => {
            //     regexp_replace_static(a, b, regex, *limit, temp_storage)
            // }
            BinaryFunc::StartsWith(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            _ => { /* fall through */ }
        }

        let a = a_expr.eval(datums, temp_storage)?;
        let b = b_expr.eval(datums, temp_storage)?;
        if self.propagates_nulls() && (a.is_null() || b.is_null()) {
            return Ok(Datum::Null);
        }
        match self {
            BinaryFunc::IsRegexpMatch { case_insensitive } => {
                is_regexp_match_dynamic(a, b, *case_insensitive)
            }
            BinaryFunc::TimezoneTimestamp => parse_timezone(a.unwrap_str(), TimezoneSpec::Posix)
                .and_then(|tz| timezone_timestamp(tz, b.unwrap_timestamp().into()).map(Into::into)),
            BinaryFunc::TimezoneTimestampTz => parse_timezone(a.unwrap_str(), TimezoneSpec::Posix)
                .and_then(|tz| {
                    Ok(timezone_timestamptz(tz, b.unwrap_timestamptz().into())?.try_into()?)
                }),
            BinaryFunc::TimezoneIntervalTimestamp => timezone_interval_timestamp(a, b),
            BinaryFunc::TimezoneIntervalTimestampTz => timezone_interval_timestamptz(a, b),
            BinaryFunc::TimezoneIntervalTime => timezone_interval_time(a, b),
            BinaryFunc::RepeatString => repeat_string(a, b, temp_storage),
            BinaryFunc::Normalize => normalize_with_form(a, b, temp_storage),
            BinaryFunc::RangeContainsElem { elem_type, rev: _ } => Ok(match elem_type {
                SqlScalarType::Int32 => contains_range_elem::<i32>(a, b),
                SqlScalarType::Int64 => contains_range_elem::<i64>(a, b),
                SqlScalarType::Date => contains_range_elem::<Date>(a, b),
                SqlScalarType::Numeric { .. } => {
                    contains_range_elem::<OrderedDecimal<Numeric>>(a, b)
                }
                SqlScalarType::Timestamp { .. } => {
                    contains_range_elem::<CheckedTimestamp<NaiveDateTime>>(a, b)
                }
                SqlScalarType::TimestampTz { .. } => {
                    contains_range_elem::<CheckedTimestamp<DateTime<Utc>>>(a, b)
                }
                _ => unreachable!(),
            }),
            BinaryFunc::RangeContainsRange { rev: false } => Ok(range_contains_range(a, b)),
            BinaryFunc::RangeContainsRange { rev: true } => Ok(range_contains_range_rev(a, b)),
            BinaryFunc::RegexpReplace { regex, limit } => {
                regexp_replace_static(a, b, regex, *limit, temp_storage)
            }
            _ => unreachable!(),
        }
    }

    pub fn output_type(
        &self,
        input1_type: SqlColumnType,
        input2_type: SqlColumnType,
    ) -> SqlColumnType {
        use BinaryFunc::*;
        let in_nullable = input1_type.nullable || input2_type.nullable;
        match self {
            AddInt16(s) => s.output_type(input1_type, input2_type),
            AddInt32(s) => s.output_type(input1_type, input2_type),
            AddInt64(s) => s.output_type(input1_type, input2_type),
            AddUint16(s) => s.output_type(input1_type, input2_type),
            AddUint32(s) => s.output_type(input1_type, input2_type),
            AddUint64(s) => s.output_type(input1_type, input2_type),
            AddFloat32(s) => s.output_type(input1_type, input2_type),
            AddFloat64(s) => s.output_type(input1_type, input2_type),
            AddInterval(s) => s.output_type(input1_type, input2_type),

            Eq(s) => s.output_type(input1_type, input2_type),
            NotEq(s) => s.output_type(input1_type, input2_type),
            Lt(s) => s.output_type(input1_type, input2_type),
            Lte(s) => s.output_type(input1_type, input2_type),
            Gt(s) => s.output_type(input1_type, input2_type),
            Gte(s) => s.output_type(input1_type, input2_type),
            ArrayContains(s) => s.output_type(input1_type, input2_type),
            ArrayContainsArray(s) => s.output_type(input1_type, input2_type),
            ArrayContainsArrayRev(s) => s.output_type(input1_type, input2_type),
            // like and regexp produce errors on invalid like-strings or regexes
            IsLikeMatchCaseInsensitive(s) => s.output_type(input1_type, input2_type),
            IsLikeMatchCaseSensitive(s) => s.output_type(input1_type, input2_type),
            IsRegexpMatch { .. } => SqlScalarType::Bool.nullable(in_nullable),

            ToCharTimestamp(s) => s.output_type(input1_type, input2_type),
            ToCharTimestampTz(s) => s.output_type(input1_type, input2_type),
            ConvertFrom(s) => s.output_type(input1_type, input2_type),
            Left(s) => s.output_type(input1_type, input2_type),
            Right(s) => s.output_type(input1_type, input2_type),
            Trim(s) => s.output_type(input1_type, input2_type),
            TrimLeading(s) => s.output_type(input1_type, input2_type),
            TrimTrailing(s) => s.output_type(input1_type, input2_type),
            LikeEscape(s) => s.output_type(input1_type, input2_type),

            SubInt16(s) => s.output_type(input1_type, input2_type),
            MulInt16(s) => s.output_type(input1_type, input2_type),
            DivInt16(s) => s.output_type(input1_type, input2_type),
            ModInt16(s) => s.output_type(input1_type, input2_type),
            BitAndInt16(s) => s.output_type(input1_type, input2_type),
            BitOrInt16(s) => s.output_type(input1_type, input2_type),
            BitXorInt16(s) => s.output_type(input1_type, input2_type),
            BitShiftLeftInt16(s) => s.output_type(input1_type, input2_type),
            BitShiftRightInt16(s) => s.output_type(input1_type, input2_type),

            SubInt32(s) => s.output_type(input1_type, input2_type),
            MulInt32(s) => s.output_type(input1_type, input2_type),
            DivInt32(s) => s.output_type(input1_type, input2_type),
            ModInt32(s) => s.output_type(input1_type, input2_type),
            BitAndInt32(s) => s.output_type(input1_type, input2_type),
            BitOrInt32(s) => s.output_type(input1_type, input2_type),
            BitXorInt32(s) => s.output_type(input1_type, input2_type),
            BitShiftLeftInt32(s) => s.output_type(input1_type, input2_type),
            BitShiftRightInt32(s) => s.output_type(input1_type, input2_type),
            EncodedBytesCharLength(s) => s.output_type(input1_type, input2_type),
            SubDate(s) => s.output_type(input1_type, input2_type),

            SubInt64(s) => s.output_type(input1_type, input2_type),
            MulInt64(s) => s.output_type(input1_type, input2_type),
            DivInt64(s) => s.output_type(input1_type, input2_type),
            ModInt64(s) => s.output_type(input1_type, input2_type),
            BitAndInt64(s) => s.output_type(input1_type, input2_type),
            BitOrInt64(s) => s.output_type(input1_type, input2_type),
            BitXorInt64(s) => s.output_type(input1_type, input2_type),
            BitShiftLeftInt64(s) => s.output_type(input1_type, input2_type),
            BitShiftRightInt64(s) => s.output_type(input1_type, input2_type),

            SubUint16(s) => s.output_type(input1_type, input2_type),
            MulUint16(s) => s.output_type(input1_type, input2_type),
            DivUint16(s) => s.output_type(input1_type, input2_type),
            ModUint16(s) => s.output_type(input1_type, input2_type),
            BitAndUint16(s) => s.output_type(input1_type, input2_type),
            BitOrUint16(s) => s.output_type(input1_type, input2_type),
            BitXorUint16(s) => s.output_type(input1_type, input2_type),
            BitShiftLeftUint16(s) => s.output_type(input1_type, input2_type),
            BitShiftRightUint16(s) => s.output_type(input1_type, input2_type),

            SubUint32(s) => s.output_type(input1_type, input2_type),
            MulUint32(s) => s.output_type(input1_type, input2_type),
            DivUint32(s) => s.output_type(input1_type, input2_type),
            ModUint32(s) => s.output_type(input1_type, input2_type),
            BitAndUint32(s) => s.output_type(input1_type, input2_type),
            BitOrUint32(s) => s.output_type(input1_type, input2_type),
            BitXorUint32(s) => s.output_type(input1_type, input2_type),
            BitShiftLeftUint32(s) => s.output_type(input1_type, input2_type),
            BitShiftRightUint32(s) => s.output_type(input1_type, input2_type),

            SubUint64(s) => s.output_type(input1_type, input2_type),
            MulUint64(s) => s.output_type(input1_type, input2_type),
            DivUint64(s) => s.output_type(input1_type, input2_type),
            ModUint64(s) => s.output_type(input1_type, input2_type),
            BitAndUint64(s) => s.output_type(input1_type, input2_type),
            BitOrUint64(s) => s.output_type(input1_type, input2_type),
            BitXorUint64(s) => s.output_type(input1_type, input2_type),
            BitShiftLeftUint64(s) => s.output_type(input1_type, input2_type),
            BitShiftRightUint64(s) => s.output_type(input1_type, input2_type),

            SubFloat32(s) => s.output_type(input1_type, input2_type),
            MulFloat32(s) => s.output_type(input1_type, input2_type),
            DivFloat32(s) => s.output_type(input1_type, input2_type),
            ModFloat32(s) => s.output_type(input1_type, input2_type),

            SubFloat64(s) => s.output_type(input1_type, input2_type),
            MulFloat64(s) => s.output_type(input1_type, input2_type),
            DivFloat64(s) => s.output_type(input1_type, input2_type),
            ModFloat64(s) => s.output_type(input1_type, input2_type),

            SubInterval(s) => s.output_type(input1_type, input2_type),
            SubTimestamp(s) => s.output_type(input1_type, input2_type),
            SubTimestampTz(s) => s.output_type(input1_type, input2_type),
            MulInterval(s) => s.output_type(input1_type, input2_type),
            DivInterval(s) => s.output_type(input1_type, input2_type),

            AgeTimestamp(s) => s.output_type(input1_type, input2_type),
            AgeTimestampTz(s) => s.output_type(input1_type, input2_type),

            AddTimestampInterval(s) => s.output_type(input1_type, input2_type),
            SubTimestampInterval(s) => s.output_type(input1_type, input2_type),
            AddTimestampTzInterval(s) => s.output_type(input1_type, input2_type),
            SubTimestampTzInterval(s) => s.output_type(input1_type, input2_type),
            AddTimeInterval(s) => s.output_type(input1_type, input2_type),
            SubTimeInterval(s) => s.output_type(input1_type, input2_type),

            AddDateInterval(s) => s.output_type(input1_type, input2_type),
            SubDateInterval(s) => s.output_type(input1_type, input2_type),
            AddDateTime(s) => s.output_type(input1_type, input2_type),
            DateBinTimestamp(s) => s.output_type(input1_type, input2_type),
            DateTruncTimestamp(s) => s.output_type(input1_type, input2_type),

            DateTruncInterval(s) => s.output_type(input1_type, input2_type),

            TimezoneTimestampTz | TimezoneIntervalTimestampTz => {
                SqlScalarType::Timestamp { precision: None }.nullable(in_nullable)
            }

            ExtractInterval(s) => s.output_type(input1_type, input2_type),
            ExtractTime(s) => s.output_type(input1_type, input2_type),
            ExtractTimestamp(s) => s.output_type(input1_type, input2_type),
            ExtractTimestampTz(s) => s.output_type(input1_type, input2_type),
            ExtractDate(s) => s.output_type(input1_type, input2_type),

            DatePartInterval(s) => s.output_type(input1_type, input2_type),
            DatePartTime(s) => s.output_type(input1_type, input2_type),
            DatePartTimestamp(s) => s.output_type(input1_type, input2_type),
            DatePartTimestampTz(s) => s.output_type(input1_type, input2_type),

            DateBinTimestampTz(s) => s.output_type(input1_type, input2_type),
            DateTruncTimestampTz(s) => s.output_type(input1_type, input2_type),

            TimezoneTimestamp | TimezoneIntervalTimestamp => {
                SqlScalarType::TimestampTz { precision: None }.nullable(in_nullable)
            }

            TimezoneIntervalTime => SqlScalarType::Time.nullable(in_nullable),

            TimezoneOffset(s) => s.output_type(input1_type, input2_type),

            SubTime(s) => s.output_type(input1_type, input2_type),

            MzRenderTypmod(s) => s.output_type(input1_type, input2_type),
            TextConcat(s) => s.output_type(input1_type, input2_type),

            JsonbGetInt64Stringify(s) => s.output_type(input1_type, input2_type),
            JsonbGetStringStringify(s) => s.output_type(input1_type, input2_type),
            JsonbGetPathStringify(s) => s.output_type(input1_type, input2_type),

            JsonbGetInt64(s) => s.output_type(input1_type, input2_type),
            JsonbGetString(s) => s.output_type(input1_type, input2_type),
            JsonbGetPath(s) => s.output_type(input1_type, input2_type),
            JsonbConcat(s) => s.output_type(input1_type, input2_type),
            JsonbDeleteInt64(s) => s.output_type(input1_type, input2_type),
            JsonbDeleteString(s) => s.output_type(input1_type, input2_type),

            JsonbContainsString(s) => s.output_type(input1_type, input2_type),
            JsonbContainsJsonb(s) => s.output_type(input1_type, input2_type),
            MapContainsKey(s) => s.output_type(input1_type, input2_type),
            MapContainsAllKeys(s) => s.output_type(input1_type, input2_type),
            MapContainsAnyKeys(s) => s.output_type(input1_type, input2_type),
            MapContainsMap(s) => s.output_type(input1_type, input2_type),

            MapGetValue(s) => s.output_type(input1_type, input2_type),

            ArrayLength(s) => s.output_type(input1_type, input2_type),
            ArrayLower(s) => s.output_type(input1_type, input2_type),
            ArrayUpper(s) => s.output_type(input1_type, input2_type),

            ListLengthMax(s) => s.output_type(input1_type, input2_type),

            ArrayArrayConcat(s) => s.output_type(input1_type, input2_type),
            ArrayRemove(s) => s.output_type(input1_type, input2_type),
            ListListConcat(s) => s.output_type(input1_type, input2_type),
            ListElementConcat(s) => s.output_type(input1_type, input2_type),
            ListRemove(s) => s.output_type(input1_type, input2_type),

            ElementListConcat(s) => s.output_type(input1_type, input2_type),

            ListContainsList(s) => s.output_type(input1_type, input2_type),
            ListContainsListRev(s) => s.output_type(input1_type, input2_type),

            DigestString(s) => s.output_type(input1_type, input2_type),
            DigestBytes(s) => s.output_type(input1_type, input2_type),
            Position(s) => s.output_type(input1_type, input2_type),
            Encode(s) => s.output_type(input1_type, input2_type),
            Decode(s) => s.output_type(input1_type, input2_type),
            Power(s) => s.output_type(input1_type, input2_type),
            RepeatString | Normalize => input1_type.scalar_type.nullable(in_nullable),

            AddNumeric(s) => s.output_type(input1_type, input2_type),
            DivNumeric(s) => s.output_type(input1_type, input2_type),
            LogNumeric(s) => s.output_type(input1_type, input2_type),
            ModNumeric(s) => s.output_type(input1_type, input2_type),
            MulNumeric(s) => s.output_type(input1_type, input2_type),
            PowerNumeric(s) => s.output_type(input1_type, input2_type),
            RoundNumeric(s) => s.output_type(input1_type, input2_type),
            SubNumeric(s) => s.output_type(input1_type, input2_type),

            GetBit(s) => s.output_type(input1_type, input2_type),
            GetByte(s) => s.output_type(input1_type, input2_type),

            ConstantTimeEqBytes(s) => s.output_type(input1_type, input2_type),
            ConstantTimeEqString(s) => s.output_type(input1_type, input2_type),

            UuidGenerateV5(s) => s.output_type(input1_type, input2_type),

            RangeContainsElem { .. } | RangeContainsRange { .. } => {
                SqlScalarType::Bool.nullable(in_nullable)
            }
            RangeOverlaps(s) => s.output_type(input1_type, input2_type),
            RangeAfter(s) => s.output_type(input1_type, input2_type),
            RangeBefore(s) => s.output_type(input1_type, input2_type),
            RangeOverleft(s) => s.output_type(input1_type, input2_type),
            RangeOverright(s) => s.output_type(input1_type, input2_type),
            RangeAdjacent(s) => s.output_type(input1_type, input2_type),

            RangeUnion(s) => s.output_type(input1_type, input2_type),
            RangeIntersection(s) => s.output_type(input1_type, input2_type),
            RangeDifference(s) => s.output_type(input1_type, input2_type),

            MzAclItemContainsPrivilege(s) => s.output_type(input1_type, input2_type),

            ParseIdent(s) => s.output_type(input1_type, input2_type),
            PrettySql(s) => s.output_type(input1_type, input2_type),
            RegexpReplace { .. } => SqlScalarType::String.nullable(in_nullable),

            StartsWith(s) => s.output_type(input1_type, input2_type),
        }
    }

    /// Whether the function output is NULL if any of its inputs are NULL.
    pub fn propagates_nulls(&self) -> bool {
        match self {
            BinaryFunc::AddDateInterval(s) => s.propagates_nulls(),
            BinaryFunc::AddDateTime(s) => s.propagates_nulls(),
            BinaryFunc::AddFloat32(s) => s.propagates_nulls(),
            BinaryFunc::AddFloat64(s) => s.propagates_nulls(),
            BinaryFunc::AddInt16(s) => s.propagates_nulls(),
            BinaryFunc::AddInt32(s) => s.propagates_nulls(),
            BinaryFunc::AddInt64(s) => s.propagates_nulls(),
            BinaryFunc::AddInterval(s) => s.propagates_nulls(),
            BinaryFunc::AddNumeric(s) => s.propagates_nulls(),
            BinaryFunc::AddTimeInterval(s) => s.propagates_nulls(),
            BinaryFunc::AddTimestampInterval(s) => s.propagates_nulls(),
            BinaryFunc::AddTimestampTzInterval(s) => s.propagates_nulls(),
            BinaryFunc::AddUint16(s) => s.propagates_nulls(),
            BinaryFunc::AddUint32(s) => s.propagates_nulls(),
            BinaryFunc::AddUint64(s) => s.propagates_nulls(),
            BinaryFunc::AgeTimestamp(s) => s.propagates_nulls(),
            BinaryFunc::AgeTimestampTz(s) => s.propagates_nulls(),
            BinaryFunc::ArrayArrayConcat(s) => s.propagates_nulls(),
            BinaryFunc::ArrayContains(s) => s.propagates_nulls(),
            BinaryFunc::ArrayContainsArray(s) => s.propagates_nulls(),
            BinaryFunc::ArrayContainsArrayRev(s) => s.propagates_nulls(),
            BinaryFunc::ArrayLength(s) => s.propagates_nulls(),
            BinaryFunc::ArrayLower(s) => s.propagates_nulls(),
            BinaryFunc::ArrayRemove(s) => s.propagates_nulls(),
            BinaryFunc::ArrayUpper(s) => s.propagates_nulls(),
            BinaryFunc::BitAndInt16(s) => s.propagates_nulls(),
            BinaryFunc::BitAndInt32(s) => s.propagates_nulls(),
            BinaryFunc::BitAndInt64(s) => s.propagates_nulls(),
            BinaryFunc::BitAndUint16(s) => s.propagates_nulls(),
            BinaryFunc::BitAndUint32(s) => s.propagates_nulls(),
            BinaryFunc::BitAndUint64(s) => s.propagates_nulls(),
            BinaryFunc::BitOrInt16(s) => s.propagates_nulls(),
            BinaryFunc::BitOrInt32(s) => s.propagates_nulls(),
            BinaryFunc::BitOrInt64(s) => s.propagates_nulls(),
            BinaryFunc::BitOrUint16(s) => s.propagates_nulls(),
            BinaryFunc::BitOrUint32(s) => s.propagates_nulls(),
            BinaryFunc::BitOrUint64(s) => s.propagates_nulls(),
            BinaryFunc::BitShiftLeftInt16(s) => s.propagates_nulls(),
            BinaryFunc::BitShiftLeftInt32(s) => s.propagates_nulls(),
            BinaryFunc::BitShiftLeftInt64(s) => s.propagates_nulls(),
            BinaryFunc::BitShiftLeftUint16(s) => s.propagates_nulls(),
            BinaryFunc::BitShiftLeftUint32(s) => s.propagates_nulls(),
            BinaryFunc::BitShiftLeftUint64(s) => s.propagates_nulls(),
            BinaryFunc::BitShiftRightInt16(s) => s.propagates_nulls(),
            BinaryFunc::BitShiftRightInt32(s) => s.propagates_nulls(),
            BinaryFunc::BitShiftRightInt64(s) => s.propagates_nulls(),
            BinaryFunc::BitShiftRightUint16(s) => s.propagates_nulls(),
            BinaryFunc::BitShiftRightUint32(s) => s.propagates_nulls(),
            BinaryFunc::BitShiftRightUint64(s) => s.propagates_nulls(),
            BinaryFunc::BitXorInt16(s) => s.propagates_nulls(),
            BinaryFunc::BitXorInt32(s) => s.propagates_nulls(),
            BinaryFunc::BitXorInt64(s) => s.propagates_nulls(),
            BinaryFunc::BitXorUint16(s) => s.propagates_nulls(),
            BinaryFunc::BitXorUint32(s) => s.propagates_nulls(),
            BinaryFunc::BitXorUint64(s) => s.propagates_nulls(),
            BinaryFunc::ConstantTimeEqBytes(s) => s.propagates_nulls(),
            BinaryFunc::ConstantTimeEqString(s) => s.propagates_nulls(),
            BinaryFunc::ConvertFrom(s) => s.propagates_nulls(),
            BinaryFunc::DateBinTimestamp(s) => s.propagates_nulls(),
            BinaryFunc::DateBinTimestampTz(s) => s.propagates_nulls(),
            BinaryFunc::DatePartInterval(s) => s.propagates_nulls(),
            BinaryFunc::DatePartTime(s) => s.propagates_nulls(),
            BinaryFunc::DatePartTimestamp(s) => s.propagates_nulls(),
            BinaryFunc::DatePartTimestampTz(s) => s.propagates_nulls(),
            BinaryFunc::DateTruncInterval(s) => s.propagates_nulls(),
            BinaryFunc::DateTruncTimestamp(s) => s.propagates_nulls(),
            BinaryFunc::DateTruncTimestampTz(s) => s.propagates_nulls(),
            BinaryFunc::Decode(s) => s.propagates_nulls(),
            BinaryFunc::DigestBytes(s) => s.propagates_nulls(),
            BinaryFunc::DigestString(s) => s.propagates_nulls(),
            BinaryFunc::DivFloat32(s) => s.propagates_nulls(),
            BinaryFunc::DivFloat64(s) => s.propagates_nulls(),
            BinaryFunc::DivInt16(s) => s.propagates_nulls(),
            BinaryFunc::DivInt32(s) => s.propagates_nulls(),
            BinaryFunc::DivInt64(s) => s.propagates_nulls(),
            BinaryFunc::DivInterval(s) => s.propagates_nulls(),
            BinaryFunc::DivNumeric(s) => s.propagates_nulls(),
            BinaryFunc::DivUint16(s) => s.propagates_nulls(),
            BinaryFunc::DivUint32(s) => s.propagates_nulls(),
            BinaryFunc::DivUint64(s) => s.propagates_nulls(),
            BinaryFunc::ElementListConcat(s) => s.propagates_nulls(),
            BinaryFunc::Encode(s) => s.propagates_nulls(),
            BinaryFunc::EncodedBytesCharLength(s) => s.propagates_nulls(),
            BinaryFunc::Eq(s) => s.propagates_nulls(),
            BinaryFunc::ExtractDate(s) => s.propagates_nulls(),
            BinaryFunc::ExtractInterval(s) => s.propagates_nulls(),
            BinaryFunc::ExtractTime(s) => s.propagates_nulls(),
            BinaryFunc::ExtractTimestamp(s) => s.propagates_nulls(),
            BinaryFunc::ExtractTimestampTz(s) => s.propagates_nulls(),
            BinaryFunc::GetBit(s) => s.propagates_nulls(),
            BinaryFunc::GetByte(s) => s.propagates_nulls(),
            BinaryFunc::Gt(s) => s.propagates_nulls(),
            BinaryFunc::Gte(s) => s.propagates_nulls(),
            BinaryFunc::IsLikeMatchCaseInsensitive(s) => s.propagates_nulls(),
            BinaryFunc::IsLikeMatchCaseSensitive(s) => s.propagates_nulls(),
            BinaryFunc::IsRegexpMatch { .. } => true,
            BinaryFunc::JsonbConcat(s) => s.propagates_nulls(),
            BinaryFunc::JsonbContainsJsonb(s) => s.propagates_nulls(),
            BinaryFunc::JsonbContainsString(s) => s.propagates_nulls(),
            BinaryFunc::JsonbDeleteInt64(s) => s.propagates_nulls(),
            BinaryFunc::JsonbDeleteString(s) => s.propagates_nulls(),
            BinaryFunc::JsonbGetInt64(s) => s.propagates_nulls(),
            BinaryFunc::JsonbGetInt64Stringify(s) => s.propagates_nulls(),
            BinaryFunc::JsonbGetPath(s) => s.propagates_nulls(),
            BinaryFunc::JsonbGetPathStringify(s) => s.propagates_nulls(),
            BinaryFunc::JsonbGetString(s) => s.propagates_nulls(),
            BinaryFunc::JsonbGetStringStringify(s) => s.propagates_nulls(),
            BinaryFunc::Left(s) => s.propagates_nulls(),
            BinaryFunc::LikeEscape(s) => s.propagates_nulls(),
            BinaryFunc::ListContainsList(s) => s.propagates_nulls(),
            BinaryFunc::ListContainsListRev(s) => s.propagates_nulls(),
            BinaryFunc::ListElementConcat(s) => s.propagates_nulls(),
            BinaryFunc::ListLengthMax(s) => s.propagates_nulls(),
            BinaryFunc::ListListConcat(s) => s.propagates_nulls(),
            BinaryFunc::ListRemove(s) => s.propagates_nulls(),
            BinaryFunc::LogNumeric(s) => s.propagates_nulls(),
            BinaryFunc::Lt(s) => s.propagates_nulls(),
            BinaryFunc::Lte(s) => s.propagates_nulls(),
            BinaryFunc::MapContainsAllKeys(s) => s.propagates_nulls(),
            BinaryFunc::MapContainsAnyKeys(s) => s.propagates_nulls(),
            BinaryFunc::MapContainsKey(s) => s.propagates_nulls(),
            BinaryFunc::MapContainsMap(s) => s.propagates_nulls(),
            BinaryFunc::MapGetValue(s) => s.propagates_nulls(),
            BinaryFunc::ModFloat32(s) => s.propagates_nulls(),
            BinaryFunc::ModFloat64(s) => s.propagates_nulls(),
            BinaryFunc::ModInt16(s) => s.propagates_nulls(),
            BinaryFunc::ModInt32(s) => s.propagates_nulls(),
            BinaryFunc::ModInt64(s) => s.propagates_nulls(),
            BinaryFunc::ModNumeric(s) => s.propagates_nulls(),
            BinaryFunc::ModUint16(s) => s.propagates_nulls(),
            BinaryFunc::ModUint32(s) => s.propagates_nulls(),
            BinaryFunc::ModUint64(s) => s.propagates_nulls(),
            BinaryFunc::MulFloat32(s) => s.propagates_nulls(),
            BinaryFunc::MulFloat64(s) => s.propagates_nulls(),
            BinaryFunc::MulInt16(s) => s.propagates_nulls(),
            BinaryFunc::MulInt32(s) => s.propagates_nulls(),
            BinaryFunc::MulInt64(s) => s.propagates_nulls(),
            BinaryFunc::MulInterval(s) => s.propagates_nulls(),
            BinaryFunc::MulNumeric(s) => s.propagates_nulls(),
            BinaryFunc::MulUint16(s) => s.propagates_nulls(),
            BinaryFunc::MulUint32(s) => s.propagates_nulls(),
            BinaryFunc::MulUint64(s) => s.propagates_nulls(),
            BinaryFunc::MzAclItemContainsPrivilege(s) => s.propagates_nulls(),
            BinaryFunc::MzRenderTypmod(s) => s.propagates_nulls(),
            BinaryFunc::Normalize => true,
            BinaryFunc::NotEq(s) => s.propagates_nulls(),
            BinaryFunc::ParseIdent(s) => s.propagates_nulls(),
            BinaryFunc::Position(s) => s.propagates_nulls(),
            BinaryFunc::Power(s) => s.propagates_nulls(),
            BinaryFunc::PowerNumeric(s) => s.propagates_nulls(),
            BinaryFunc::PrettySql(s) => s.propagates_nulls(),
            BinaryFunc::RangeAdjacent(s) => s.propagates_nulls(),
            BinaryFunc::RangeAfter(s) => s.propagates_nulls(),
            BinaryFunc::RangeBefore(s) => s.propagates_nulls(),
            BinaryFunc::RangeContainsElem { .. } => true,
            BinaryFunc::RangeContainsRange { .. } => true,
            BinaryFunc::RangeDifference(s) => s.propagates_nulls(),
            BinaryFunc::RangeIntersection(s) => s.propagates_nulls(),
            BinaryFunc::RangeOverlaps(s) => s.propagates_nulls(),
            BinaryFunc::RangeOverleft(s) => s.propagates_nulls(),
            BinaryFunc::RangeOverright(s) => s.propagates_nulls(),
            BinaryFunc::RangeUnion(s) => s.propagates_nulls(),
            BinaryFunc::RegexpReplace { .. } => true,
            BinaryFunc::RepeatString => true,
            BinaryFunc::Right(s) => s.propagates_nulls(),
            BinaryFunc::RoundNumeric(s) => s.propagates_nulls(),
            BinaryFunc::StartsWith(s) => s.propagates_nulls(),
            BinaryFunc::SubDate(s) => s.propagates_nulls(),
            BinaryFunc::SubDateInterval(s) => s.propagates_nulls(),
            BinaryFunc::SubFloat32(s) => s.propagates_nulls(),
            BinaryFunc::SubFloat64(s) => s.propagates_nulls(),
            BinaryFunc::SubInt16(s) => s.propagates_nulls(),
            BinaryFunc::SubInt32(s) => s.propagates_nulls(),
            BinaryFunc::SubInt64(s) => s.propagates_nulls(),
            BinaryFunc::SubInterval(s) => s.propagates_nulls(),
            BinaryFunc::SubNumeric(s) => s.propagates_nulls(),
            BinaryFunc::SubTime(s) => s.propagates_nulls(),
            BinaryFunc::SubTimeInterval(s) => s.propagates_nulls(),
            BinaryFunc::SubTimestamp(s) => s.propagates_nulls(),
            BinaryFunc::SubTimestampInterval(s) => s.propagates_nulls(),
            BinaryFunc::SubTimestampTz(s) => s.propagates_nulls(),
            BinaryFunc::SubTimestampTzInterval(s) => s.propagates_nulls(),
            BinaryFunc::SubUint16(s) => s.propagates_nulls(),
            BinaryFunc::SubUint32(s) => s.propagates_nulls(),
            BinaryFunc::SubUint64(s) => s.propagates_nulls(),
            BinaryFunc::TextConcat(s) => s.propagates_nulls(),
            BinaryFunc::TimezoneIntervalTime => true,
            BinaryFunc::TimezoneIntervalTimestamp => true,
            BinaryFunc::TimezoneIntervalTimestampTz => true,
            BinaryFunc::TimezoneOffset(s) => s.propagates_nulls(),
            BinaryFunc::TimezoneTimestamp => true,
            BinaryFunc::TimezoneTimestampTz => true,
            BinaryFunc::ToCharTimestamp(s) => s.propagates_nulls(),
            BinaryFunc::ToCharTimestampTz(s) => s.propagates_nulls(),
            BinaryFunc::Trim(s) => s.propagates_nulls(),
            BinaryFunc::TrimLeading(s) => s.propagates_nulls(),
            BinaryFunc::TrimTrailing(s) => s.propagates_nulls(),
            BinaryFunc::UuidGenerateV5(s) => s.propagates_nulls(),
        }
    }

    /// Whether the function might return NULL even if none of its inputs are
    /// NULL.
    ///
    /// This is presently conservative, and may indicate that a function
    /// introduces nulls even when it does not.
    pub fn introduces_nulls(&self) -> bool {
        use BinaryFunc::*;
        match self {
            AddDateInterval(s) => s.introduces_nulls(),
            AddDateTime(s) => s.introduces_nulls(),
            AddFloat32(s) => s.introduces_nulls(),
            AddFloat64(s) => s.introduces_nulls(),
            AddInt16(s) => s.introduces_nulls(),
            AddInt32(s) => s.introduces_nulls(),
            AddInt64(s) => s.introduces_nulls(),
            AddInterval(s) => s.introduces_nulls(),
            AddNumeric(s) => s.introduces_nulls(),
            AddTimeInterval(s) => s.introduces_nulls(),
            AddTimestampInterval(s) => s.introduces_nulls(),
            AddTimestampTzInterval(s) => s.introduces_nulls(),
            AddUint16(s) => s.introduces_nulls(),
            AddUint32(s) => s.introduces_nulls(),
            AddUint64(s) => s.introduces_nulls(),
            AgeTimestamp(s) => s.introduces_nulls(),
            AgeTimestampTz(s) => s.introduces_nulls(),
            ArrayArrayConcat(s) => s.introduces_nulls(),
            ArrayContains(s) => s.introduces_nulls(),
            ArrayContainsArray(s) => s.introduces_nulls(),
            ArrayContainsArrayRev(s) => s.introduces_nulls(),
            ArrayRemove(s) => s.introduces_nulls(),
            BitAndInt16(s) => s.introduces_nulls(),
            BitAndInt32(s) => s.introduces_nulls(),
            BitAndInt64(s) => s.introduces_nulls(),
            BitAndUint16(s) => s.introduces_nulls(),
            BitAndUint32(s) => s.introduces_nulls(),
            BitAndUint64(s) => s.introduces_nulls(),
            BitOrInt16(s) => s.introduces_nulls(),
            BitOrInt32(s) => s.introduces_nulls(),
            BitOrInt64(s) => s.introduces_nulls(),
            BitOrUint16(s) => s.introduces_nulls(),
            BitOrUint32(s) => s.introduces_nulls(),
            BitOrUint64(s) => s.introduces_nulls(),
            BitShiftLeftInt16(s) => s.introduces_nulls(),
            BitShiftLeftInt32(s) => s.introduces_nulls(),
            BitShiftLeftInt64(s) => s.introduces_nulls(),
            BitShiftLeftUint16(s) => s.introduces_nulls(),
            BitShiftLeftUint32(s) => s.introduces_nulls(),
            BitShiftLeftUint64(s) => s.introduces_nulls(),
            BitShiftRightInt16(s) => s.introduces_nulls(),
            BitShiftRightInt32(s) => s.introduces_nulls(),
            BitShiftRightInt64(s) => s.introduces_nulls(),
            BitShiftRightUint16(s) => s.introduces_nulls(),
            BitShiftRightUint32(s) => s.introduces_nulls(),
            BitShiftRightUint64(s) => s.introduces_nulls(),
            BitXorInt16(s) => s.introduces_nulls(),
            BitXorInt32(s) => s.introduces_nulls(),
            BitXorInt64(s) => s.introduces_nulls(),
            BitXorUint16(s) => s.introduces_nulls(),
            BitXorUint32(s) => s.introduces_nulls(),
            BitXorUint64(s) => s.introduces_nulls(),
            ConstantTimeEqBytes(s) => s.introduces_nulls(),
            ConstantTimeEqString(s) => s.introduces_nulls(),
            ConvertFrom(s) => s.introduces_nulls(),
            DateBinTimestamp(s) => s.introduces_nulls(),
            DateBinTimestampTz(s) => s.introduces_nulls(),
            DatePartInterval(s) => s.introduces_nulls(),
            DatePartTime(s) => s.introduces_nulls(),
            DatePartTimestamp(s) => s.introduces_nulls(),
            DatePartTimestampTz(s) => s.introduces_nulls(),
            DateTruncInterval(s) => s.introduces_nulls(),
            DateTruncTimestamp(s) => s.introduces_nulls(),
            DateTruncTimestampTz(s) => s.introduces_nulls(),
            Decode(s) => s.introduces_nulls(),
            DigestBytes(s) => s.introduces_nulls(),
            DigestString(s) => s.introduces_nulls(),
            DivFloat32(s) => s.introduces_nulls(),
            DivFloat64(s) => s.introduces_nulls(),
            DivInt16(s) => s.introduces_nulls(),
            DivInt32(s) => s.introduces_nulls(),
            DivInt64(s) => s.introduces_nulls(),
            DivInterval(s) => s.introduces_nulls(),
            DivNumeric(s) => s.introduces_nulls(),
            DivUint16(s) => s.introduces_nulls(),
            DivUint32(s) => s.introduces_nulls(),
            DivUint64(s) => s.introduces_nulls(),
            ElementListConcat(s) => s.introduces_nulls(),
            Encode(s) => s.introduces_nulls(),
            EncodedBytesCharLength(s) => s.introduces_nulls(),
            Eq(s) => s.introduces_nulls(),
            ExtractDate(s) => s.introduces_nulls(),
            ExtractInterval(s) => s.introduces_nulls(),
            ExtractTime(s) => s.introduces_nulls(),
            ExtractTimestamp(s) => s.introduces_nulls(),
            ExtractTimestampTz(s) => s.introduces_nulls(),
            GetBit(s) => s.introduces_nulls(),
            GetByte(s) => s.introduces_nulls(),
            Gt(s) => s.introduces_nulls(),
            Gte(s) => s.introduces_nulls(),
            IsLikeMatchCaseInsensitive(s) => s.introduces_nulls(),
            IsLikeMatchCaseSensitive(s) => s.introduces_nulls(),
            IsRegexpMatch { .. } => false,
            JsonbContainsJsonb(s) => s.introduces_nulls(),
            JsonbContainsString(s) => s.introduces_nulls(),
            Left(s) => s.introduces_nulls(),
            LikeEscape(s) => s.introduces_nulls(),
            ListContainsList(s) => s.introduces_nulls(),
            ListContainsListRev(s) => s.introduces_nulls(),
            ListElementConcat(s) => s.introduces_nulls(),
            ListListConcat(s) => s.introduces_nulls(),
            ListRemove(s) => s.introduces_nulls(),
            LogNumeric(s) => s.introduces_nulls(),
            Lt(s) => s.introduces_nulls(),
            Lte(s) => s.introduces_nulls(),
            MapContainsAllKeys(s) => s.introduces_nulls(),
            MapContainsAnyKeys(s) => s.introduces_nulls(),
            MapContainsKey(s) => s.introduces_nulls(),
            MapContainsMap(s) => s.introduces_nulls(),
            ModFloat32(s) => s.introduces_nulls(),
            ModFloat64(s) => s.introduces_nulls(),
            ModInt16(s) => s.introduces_nulls(),
            ModInt32(s) => s.introduces_nulls(),
            ModInt64(s) => s.introduces_nulls(),
            ModNumeric(s) => s.introduces_nulls(),
            ModUint16(s) => s.introduces_nulls(),
            ModUint32(s) => s.introduces_nulls(),
            ModUint64(s) => s.introduces_nulls(),
            MulFloat32(s) => s.introduces_nulls(),
            MulFloat64(s) => s.introduces_nulls(),
            MulInt16(s) => s.introduces_nulls(),
            MulInt32(s) => s.introduces_nulls(),
            MulInt64(s) => s.introduces_nulls(),
            MulInterval(s) => s.introduces_nulls(),
            MulNumeric(s) => s.introduces_nulls(),
            MulUint16(s) => s.introduces_nulls(),
            MulUint32(s) => s.introduces_nulls(),
            MulUint64(s) => s.introduces_nulls(),
            MzAclItemContainsPrivilege(s) => s.introduces_nulls(),
            MzRenderTypmod(s) => s.introduces_nulls(),
            Normalize => false,
            NotEq(s) => s.introduces_nulls(),
            ParseIdent(s) => s.introduces_nulls(),
            Position(s) => s.introduces_nulls(),
            Power(s) => s.introduces_nulls(),
            PowerNumeric(s) => s.introduces_nulls(),
            PrettySql(s) => s.introduces_nulls(),
            RangeAdjacent(s) => s.introduces_nulls(),
            RangeAfter(s) => s.introduces_nulls(),
            RangeBefore(s) => s.introduces_nulls(),
            RangeContainsElem { .. } => false,
            RangeContainsRange { .. } => false,
            RangeDifference(s) => s.introduces_nulls(),
            RangeIntersection(s) => s.introduces_nulls(),
            RangeOverlaps(s) => s.introduces_nulls(),
            RangeOverleft(s) => s.introduces_nulls(),
            RangeOverright(s) => s.introduces_nulls(),
            RangeUnion(s) => s.introduces_nulls(),
            RegexpReplace { .. } => false,
            RepeatString => false,
            Right(s) => s.introduces_nulls(),
            RoundNumeric(s) => s.introduces_nulls(),
            StartsWith(s) => s.introduces_nulls(),
            SubDate(s) => s.introduces_nulls(),
            SubDateInterval(s) => s.introduces_nulls(),
            SubFloat32(s) => s.introduces_nulls(),
            SubFloat64(s) => s.introduces_nulls(),
            SubInt16(s) => s.introduces_nulls(),
            SubInt32(s) => s.introduces_nulls(),
            SubInt64(s) => s.introduces_nulls(),
            SubInterval(s) => s.introduces_nulls(),
            SubNumeric(s) => s.introduces_nulls(),
            SubTime(s) => s.introduces_nulls(),
            SubTimeInterval(s) => s.introduces_nulls(),
            SubTimestamp(s) => s.introduces_nulls(),
            SubTimestampInterval(s) => s.introduces_nulls(),
            SubTimestampTz(s) => s.introduces_nulls(),
            SubTimestampTzInterval(s) => s.introduces_nulls(),
            SubUint16(s) => s.introduces_nulls(),
            SubUint32(s) => s.introduces_nulls(),
            SubUint64(s) => s.introduces_nulls(),
            TextConcat(s) => s.introduces_nulls(),
            TimezoneIntervalTime => false,
            TimezoneIntervalTimestamp => false,
            TimezoneIntervalTimestampTz => false,
            TimezoneOffset(s) => s.introduces_nulls(),
            TimezoneTimestamp => false,
            TimezoneTimestampTz => false,
            ToCharTimestamp(s) => s.introduces_nulls(),
            ToCharTimestampTz(s) => s.introduces_nulls(),
            Trim(s) => s.introduces_nulls(),
            TrimLeading(s) => s.introduces_nulls(),
            TrimTrailing(s) => s.introduces_nulls(),
            UuidGenerateV5(s) => s.introduces_nulls(),

            ArrayLength(s) => s.introduces_nulls(),
            ArrayLower(s) => s.introduces_nulls(),
            ArrayUpper(s) => s.introduces_nulls(),
            JsonbConcat(s) => s.introduces_nulls(),
            JsonbDeleteInt64(s) => s.introduces_nulls(),
            JsonbDeleteString(s) => s.introduces_nulls(),
            JsonbGetInt64(s) => s.introduces_nulls(),
            JsonbGetInt64Stringify(s) => s.introduces_nulls(),
            JsonbGetPath(s) => s.introduces_nulls(),
            JsonbGetPathStringify(s) => s.introduces_nulls(),
            JsonbGetString(s) => s.introduces_nulls(),
            JsonbGetStringStringify(s) => s.introduces_nulls(),
            ListLengthMax(s) => s.introduces_nulls(),
            MapGetValue(s) => s.introduces_nulls(),
        }
    }

    pub fn is_infix_op(&self) -> bool {
        use BinaryFunc::*;
        match self {
            AddDateInterval(s) => s.is_infix_op(),
            AddDateTime(s) => s.is_infix_op(),
            AddFloat32(s) => s.is_infix_op(),
            AddFloat64(s) => s.is_infix_op(),
            AddInt16(s) => s.is_infix_op(),
            AddInt32(s) => s.is_infix_op(),
            AddInt64(s) => s.is_infix_op(),
            AddInterval(s) => s.is_infix_op(),
            AddNumeric(s) => s.is_infix_op(),
            AddTimeInterval(s) => s.is_infix_op(),
            AddTimestampInterval(s) => s.is_infix_op(),
            AddTimestampTzInterval(s) => s.is_infix_op(),
            AddUint16(s) => s.is_infix_op(),
            AddUint32(s) => s.is_infix_op(),
            AddUint64(s) => s.is_infix_op(),
            ArrayArrayConcat(s) => s.is_infix_op(),
            ArrayContains(s) => s.is_infix_op(),
            ArrayContainsArray(s) => s.is_infix_op(),
            ArrayContainsArrayRev(s) => s.is_infix_op(),
            ArrayLength(s) => s.is_infix_op(),
            ArrayLower(s) => s.is_infix_op(),
            ArrayUpper(s) => s.is_infix_op(),
            BitAndInt16(s) => s.is_infix_op(),
            BitAndInt32(s) => s.is_infix_op(),
            BitAndInt64(s) => s.is_infix_op(),
            BitAndUint16(s) => s.is_infix_op(),
            BitAndUint32(s) => s.is_infix_op(),
            BitAndUint64(s) => s.is_infix_op(),
            BitOrInt16(s) => s.is_infix_op(),
            BitOrInt32(s) => s.is_infix_op(),
            BitOrInt64(s) => s.is_infix_op(),
            BitOrUint16(s) => s.is_infix_op(),
            BitOrUint32(s) => s.is_infix_op(),
            BitOrUint64(s) => s.is_infix_op(),
            BitShiftLeftInt16(s) => s.is_infix_op(),
            BitShiftLeftInt32(s) => s.is_infix_op(),
            BitShiftLeftInt64(s) => s.is_infix_op(),
            BitShiftLeftUint16(s) => s.is_infix_op(),
            BitShiftLeftUint32(s) => s.is_infix_op(),
            BitShiftLeftUint64(s) => s.is_infix_op(),
            BitShiftRightInt16(s) => s.is_infix_op(),
            BitShiftRightInt32(s) => s.is_infix_op(),
            BitShiftRightInt64(s) => s.is_infix_op(),
            BitShiftRightUint16(s) => s.is_infix_op(),
            BitShiftRightUint32(s) => s.is_infix_op(),
            BitShiftRightUint64(s) => s.is_infix_op(),
            BitXorInt16(s) => s.is_infix_op(),
            BitXorInt32(s) => s.is_infix_op(),
            BitXorInt64(s) => s.is_infix_op(),
            BitXorUint16(s) => s.is_infix_op(),
            BitXorUint32(s) => s.is_infix_op(),
            BitXorUint64(s) => s.is_infix_op(),
            DivFloat32(s) => s.is_infix_op(),
            DivFloat64(s) => s.is_infix_op(),
            DivInt16(s) => s.is_infix_op(),
            DivInt32(s) => s.is_infix_op(),
            DivInt64(s) => s.is_infix_op(),
            DivInterval(s) => s.is_infix_op(),
            DivNumeric(s) => s.is_infix_op(),
            DivUint16(s) => s.is_infix_op(),
            DivUint32(s) => s.is_infix_op(),
            DivUint64(s) => s.is_infix_op(),
            ElementListConcat(s) => s.is_infix_op(),
            Eq(s) => s.is_infix_op(),
            Gt(s) => s.is_infix_op(),
            Gte(s) => s.is_infix_op(),
            IsLikeMatchCaseInsensitive(s) => s.is_infix_op(),
            IsLikeMatchCaseSensitive(s) => s.is_infix_op(),
            IsRegexpMatch { .. } => true,
            JsonbConcat(s) => s.is_infix_op(),
            JsonbContainsJsonb(s) => s.is_infix_op(),
            JsonbContainsString(s) => s.is_infix_op(),
            JsonbDeleteInt64(s) => s.is_infix_op(),
            JsonbDeleteString(s) => s.is_infix_op(),
            JsonbGetInt64(s) => s.is_infix_op(),
            JsonbGetInt64Stringify(s) => s.is_infix_op(),
            JsonbGetPath(s) => s.is_infix_op(),
            JsonbGetPathStringify(s) => s.is_infix_op(),
            JsonbGetString(s) => s.is_infix_op(),
            JsonbGetStringStringify(s) => s.is_infix_op(),
            ListContainsList(s) => s.is_infix_op(),
            ListContainsListRev(s) => s.is_infix_op(),
            ListElementConcat(s) => s.is_infix_op(),
            ListListConcat(s) => s.is_infix_op(),
            Lt(s) => s.is_infix_op(),
            Lte(s) => s.is_infix_op(),
            MapContainsAllKeys(s) => s.is_infix_op(),
            MapContainsAnyKeys(s) => s.is_infix_op(),
            MapContainsKey(s) => s.is_infix_op(),
            MapContainsMap(s) => s.is_infix_op(),
            MapGetValue(s) => s.is_infix_op(),
            ModFloat32(s) => s.is_infix_op(),
            ModFloat64(s) => s.is_infix_op(),
            ModInt16(s) => s.is_infix_op(),
            ModInt32(s) => s.is_infix_op(),
            ModInt64(s) => s.is_infix_op(),
            ModNumeric(s) => s.is_infix_op(),
            ModUint16(s) => s.is_infix_op(),
            ModUint32(s) => s.is_infix_op(),
            ModUint64(s) => s.is_infix_op(),
            MulFloat32(s) => s.is_infix_op(),
            MulFloat64(s) => s.is_infix_op(),
            MulInt16(s) => s.is_infix_op(),
            MulInt32(s) => s.is_infix_op(),
            MulInt64(s) => s.is_infix_op(),
            MulInterval(s) => s.is_infix_op(),
            MulNumeric(s) => s.is_infix_op(),
            MulUint16(s) => s.is_infix_op(),
            MulUint32(s) => s.is_infix_op(),
            MulUint64(s) => s.is_infix_op(),
            NotEq(s) => s.is_infix_op(),
            RangeAdjacent(s) => s.is_infix_op(),
            RangeAfter(s) => s.is_infix_op(),
            RangeBefore(s) => s.is_infix_op(),
            RangeContainsElem { .. } => true,
            RangeContainsRange { .. } => true,
            RangeDifference(s) => s.is_infix_op(),
            RangeIntersection(s) => s.is_infix_op(),
            RangeOverlaps(s) => s.is_infix_op(),
            RangeOverleft(s) => s.is_infix_op(),
            RangeOverright(s) => s.is_infix_op(),
            RangeUnion(s) => s.is_infix_op(),
            SubDate(s) => s.is_infix_op(),
            SubDateInterval(s) => s.is_infix_op(),
            SubFloat32(s) => s.is_infix_op(),
            SubFloat64(s) => s.is_infix_op(),
            SubInt16(s) => s.is_infix_op(),
            SubInt32(s) => s.is_infix_op(),
            SubInt64(s) => s.is_infix_op(),
            SubInterval(s) => s.is_infix_op(),
            SubNumeric(s) => s.is_infix_op(),
            SubTime(s) => s.is_infix_op(),
            SubTimeInterval(s) => s.is_infix_op(),
            SubTimestamp(s) => s.is_infix_op(),
            SubTimestampInterval(s) => s.is_infix_op(),
            SubTimestampTz(s) => s.is_infix_op(),
            SubTimestampTzInterval(s) => s.is_infix_op(),
            SubUint16(s) => s.is_infix_op(),
            SubUint32(s) => s.is_infix_op(),
            SubUint64(s) => s.is_infix_op(),
            TextConcat(s) => s.is_infix_op(),

            AgeTimestamp(s) => s.is_infix_op(),
            AgeTimestampTz(s) => s.is_infix_op(),
            ArrayRemove(s) => s.is_infix_op(),
            ConstantTimeEqBytes(s) => s.is_infix_op(),
            ConstantTimeEqString(s) => s.is_infix_op(),
            ConvertFrom(s) => s.is_infix_op(),
            DateBinTimestamp(s) => s.is_infix_op(),
            DateBinTimestampTz(s) => s.is_infix_op(),
            DatePartInterval(s) => s.is_infix_op(),
            DatePartTime(s) => s.is_infix_op(),
            DatePartTimestamp(s) => s.is_infix_op(),
            DatePartTimestampTz(s) => s.is_infix_op(),
            DateTruncInterval(s) => s.is_infix_op(),
            DateTruncTimestamp(s) => s.is_infix_op(),
            DateTruncTimestampTz(s) => s.is_infix_op(),
            Decode(s) => s.is_infix_op(),
            DigestBytes(s) => s.is_infix_op(),
            DigestString(s) => s.is_infix_op(),
            Encode(s) => s.is_infix_op(),
            EncodedBytesCharLength(s) => s.is_infix_op(),
            ExtractDate(s) => s.is_infix_op(),
            ExtractInterval(s) => s.is_infix_op(),
            ExtractTime(s) => s.is_infix_op(),
            ExtractTimestamp(s) => s.is_infix_op(),
            ExtractTimestampTz(s) => s.is_infix_op(),
            GetBit(s) => s.is_infix_op(),
            GetByte(s) => s.is_infix_op(),
            Left(s) => s.is_infix_op(),
            LikeEscape(s) => s.is_infix_op(),
            ListLengthMax(s) => s.is_infix_op(),
            ListRemove(s) => s.is_infix_op(),
            LogNumeric(s) => s.is_infix_op(),
            MzAclItemContainsPrivilege(s) => s.is_infix_op(),
            MzRenderTypmod(s) => s.is_infix_op(),
            Normalize => false,
            ParseIdent(s) => s.is_infix_op(),
            Position(s) => s.is_infix_op(),
            Power(s) => s.is_infix_op(),
            PowerNumeric(s) => s.is_infix_op(),
            PrettySql(s) => s.is_infix_op(),
            RegexpReplace { .. } => false,
            RepeatString => false,
            Right(s) => s.is_infix_op(),
            RoundNumeric(s) => s.is_infix_op(),
            StartsWith(s) => s.is_infix_op(),
            TimezoneIntervalTime => false,
            TimezoneIntervalTimestamp => false,
            TimezoneIntervalTimestampTz => false,
            TimezoneOffset(s) => s.is_infix_op(),
            TimezoneTimestamp => false,
            TimezoneTimestampTz => false,
            ToCharTimestamp(s) => s.is_infix_op(),
            ToCharTimestampTz(s) => s.is_infix_op(),
            Trim(s) => s.is_infix_op(),
            TrimLeading(s) => s.is_infix_op(),
            TrimTrailing(s) => s.is_infix_op(),
            UuidGenerateV5(s) => s.is_infix_op(),
        }
    }

    /// Returns the negation of the given binary function, if it exists.
    pub fn negate(&self) -> Option<Self> {
        match self {
            BinaryFunc::AddDateInterval(s) => s.negate(),
            BinaryFunc::AddDateTime(s) => s.negate(),
            BinaryFunc::AddFloat32(s) => s.negate(),
            BinaryFunc::AddFloat64(s) => s.negate(),
            BinaryFunc::AddInt16(s) => s.negate(),
            BinaryFunc::AddInt32(s) => s.negate(),
            BinaryFunc::AddInt64(s) => s.negate(),
            BinaryFunc::AddInterval(s) => s.negate(),
            BinaryFunc::AddNumeric(s) => s.negate(),
            BinaryFunc::AddTimeInterval(s) => s.negate(),
            BinaryFunc::AddTimestampInterval(s) => s.negate(),
            BinaryFunc::AddTimestampTzInterval(s) => s.negate(),
            BinaryFunc::AddUint16(s) => s.negate(),
            BinaryFunc::AddUint32(s) => s.negate(),
            BinaryFunc::AddUint64(s) => s.negate(),
            BinaryFunc::AgeTimestamp(s) => s.negate(),
            BinaryFunc::AgeTimestampTz(s) => s.negate(),
            BinaryFunc::ArrayArrayConcat(s) => s.negate(),
            BinaryFunc::ArrayContains(s) => s.negate(),
            BinaryFunc::ArrayContainsArray(s) => s.negate(),
            BinaryFunc::ArrayContainsArrayRev(s) => s.negate(),
            BinaryFunc::ArrayLength(s) => s.negate(),
            BinaryFunc::ArrayLower(s) => s.negate(),
            BinaryFunc::ArrayRemove(s) => s.negate(),
            BinaryFunc::ArrayUpper(s) => s.negate(),
            BinaryFunc::BitAndInt16(s) => s.negate(),
            BinaryFunc::BitAndInt32(s) => s.negate(),
            BinaryFunc::BitAndInt64(s) => s.negate(),
            BinaryFunc::BitAndUint16(s) => s.negate(),
            BinaryFunc::BitAndUint32(s) => s.negate(),
            BinaryFunc::BitAndUint64(s) => s.negate(),
            BinaryFunc::BitOrInt16(s) => s.negate(),
            BinaryFunc::BitOrInt32(s) => s.negate(),
            BinaryFunc::BitOrInt64(s) => s.negate(),
            BinaryFunc::BitOrUint16(s) => s.negate(),
            BinaryFunc::BitOrUint32(s) => s.negate(),
            BinaryFunc::BitOrUint64(s) => s.negate(),
            BinaryFunc::BitShiftLeftInt16(s) => s.negate(),
            BinaryFunc::BitShiftLeftInt32(s) => s.negate(),
            BinaryFunc::BitShiftLeftInt64(s) => s.negate(),
            BinaryFunc::BitShiftLeftUint16(s) => s.negate(),
            BinaryFunc::BitShiftLeftUint32(s) => s.negate(),
            BinaryFunc::BitShiftLeftUint64(s) => s.negate(),
            BinaryFunc::BitShiftRightInt16(s) => s.negate(),
            BinaryFunc::BitShiftRightInt32(s) => s.negate(),
            BinaryFunc::BitShiftRightInt64(s) => s.negate(),
            BinaryFunc::BitShiftRightUint16(s) => s.negate(),
            BinaryFunc::BitShiftRightUint32(s) => s.negate(),
            BinaryFunc::BitShiftRightUint64(s) => s.negate(),
            BinaryFunc::BitXorInt16(s) => s.negate(),
            BinaryFunc::BitXorInt32(s) => s.negate(),
            BinaryFunc::BitXorInt64(s) => s.negate(),
            BinaryFunc::BitXorUint16(s) => s.negate(),
            BinaryFunc::BitXorUint32(s) => s.negate(),
            BinaryFunc::BitXorUint64(s) => s.negate(),
            BinaryFunc::ConstantTimeEqBytes(s) => s.negate(),
            BinaryFunc::ConstantTimeEqString(s) => s.negate(),
            BinaryFunc::ConvertFrom(s) => s.negate(),
            BinaryFunc::DateBinTimestamp(s) => s.negate(),
            BinaryFunc::DateBinTimestampTz(s) => s.negate(),
            BinaryFunc::DatePartInterval(s) => s.negate(),
            BinaryFunc::DatePartTime(s) => s.negate(),
            BinaryFunc::DatePartTimestamp(s) => s.negate(),
            BinaryFunc::DatePartTimestampTz(s) => s.negate(),
            BinaryFunc::DateTruncInterval(s) => s.negate(),
            BinaryFunc::DateTruncTimestamp(s) => s.negate(),
            BinaryFunc::DateTruncTimestampTz(s) => s.negate(),
            BinaryFunc::Decode(s) => s.negate(),
            BinaryFunc::DigestBytes(s) => s.negate(),
            BinaryFunc::DigestString(s) => s.negate(),
            BinaryFunc::DivFloat32(s) => s.negate(),
            BinaryFunc::DivFloat64(s) => s.negate(),
            BinaryFunc::DivInt16(s) => s.negate(),
            BinaryFunc::DivInt32(s) => s.negate(),
            BinaryFunc::DivInt64(s) => s.negate(),
            BinaryFunc::DivInterval(s) => s.negate(),
            BinaryFunc::DivNumeric(s) => s.negate(),
            BinaryFunc::DivUint16(s) => s.negate(),
            BinaryFunc::DivUint32(s) => s.negate(),
            BinaryFunc::DivUint64(s) => s.negate(),
            BinaryFunc::ElementListConcat(s) => s.negate(),
            BinaryFunc::Encode(s) => s.negate(),
            BinaryFunc::EncodedBytesCharLength(s) => s.negate(),
            BinaryFunc::Eq(s) => s.negate(),
            BinaryFunc::ExtractDate(s) => s.negate(),
            BinaryFunc::ExtractInterval(s) => s.negate(),
            BinaryFunc::ExtractTime(s) => s.negate(),
            BinaryFunc::ExtractTimestamp(s) => s.negate(),
            BinaryFunc::ExtractTimestampTz(s) => s.negate(),
            BinaryFunc::GetBit(s) => s.negate(),
            BinaryFunc::GetByte(s) => s.negate(),
            BinaryFunc::Gt(s) => s.negate(),
            BinaryFunc::Gte(s) => s.negate(),
            BinaryFunc::IsLikeMatchCaseInsensitive(s) => s.negate(),
            BinaryFunc::IsLikeMatchCaseSensitive(s) => s.negate(),
            BinaryFunc::IsRegexpMatch { .. } => None,
            BinaryFunc::JsonbConcat(s) => s.negate(),
            BinaryFunc::JsonbContainsJsonb(s) => s.negate(),
            BinaryFunc::JsonbContainsString(s) => s.negate(),
            BinaryFunc::JsonbDeleteInt64(s) => s.negate(),
            BinaryFunc::JsonbDeleteString(s) => s.negate(),
            BinaryFunc::JsonbGetInt64(s) => s.negate(),
            BinaryFunc::JsonbGetInt64Stringify(s) => s.negate(),
            BinaryFunc::JsonbGetPath(s) => s.negate(),
            BinaryFunc::JsonbGetPathStringify(s) => s.negate(),
            BinaryFunc::JsonbGetString(s) => s.negate(),
            BinaryFunc::JsonbGetStringStringify(s) => s.negate(),
            BinaryFunc::Left(s) => s.negate(),
            BinaryFunc::LikeEscape(s) => s.negate(),
            BinaryFunc::ListContainsList(s) => s.negate(),
            BinaryFunc::ListContainsListRev(s) => s.negate(),
            BinaryFunc::ListElementConcat(s) => s.negate(),
            BinaryFunc::ListLengthMax(s) => s.negate(),
            BinaryFunc::ListListConcat(s) => s.negate(),
            BinaryFunc::ListRemove(s) => s.negate(),
            BinaryFunc::LogNumeric(s) => s.negate(),
            BinaryFunc::Lt(s) => s.negate(),
            BinaryFunc::Lte(s) => s.negate(),
            BinaryFunc::MapContainsAllKeys(s) => s.negate(),
            BinaryFunc::MapContainsAnyKeys(s) => s.negate(),
            BinaryFunc::MapContainsKey(s) => s.negate(),
            BinaryFunc::MapContainsMap(s) => s.negate(),
            BinaryFunc::MapGetValue(s) => s.negate(),
            BinaryFunc::ModFloat32(s) => s.negate(),
            BinaryFunc::ModFloat64(s) => s.negate(),
            BinaryFunc::ModInt16(s) => s.negate(),
            BinaryFunc::ModInt32(s) => s.negate(),
            BinaryFunc::ModInt64(s) => s.negate(),
            BinaryFunc::ModNumeric(s) => s.negate(),
            BinaryFunc::ModUint16(s) => s.negate(),
            BinaryFunc::ModUint32(s) => s.negate(),
            BinaryFunc::ModUint64(s) => s.negate(),
            BinaryFunc::MulFloat32(s) => s.negate(),
            BinaryFunc::MulFloat64(s) => s.negate(),
            BinaryFunc::MulInt16(s) => s.negate(),
            BinaryFunc::MulInt32(s) => s.negate(),
            BinaryFunc::MulInt64(s) => s.negate(),
            BinaryFunc::MulInterval(s) => s.negate(),
            BinaryFunc::MulNumeric(s) => s.negate(),
            BinaryFunc::MulUint16(s) => s.negate(),
            BinaryFunc::MulUint32(s) => s.negate(),
            BinaryFunc::MulUint64(s) => s.negate(),
            BinaryFunc::MzAclItemContainsPrivilege(s) => s.negate(),
            BinaryFunc::MzRenderTypmod(s) => s.negate(),
            BinaryFunc::Normalize => None,
            BinaryFunc::NotEq(s) => s.negate(),
            BinaryFunc::ParseIdent(s) => s.negate(),
            BinaryFunc::Position(s) => s.negate(),
            BinaryFunc::Power(s) => s.negate(),
            BinaryFunc::PowerNumeric(s) => s.negate(),
            BinaryFunc::PrettySql(s) => s.negate(),
            BinaryFunc::RangeAdjacent(s) => s.negate(),
            BinaryFunc::RangeAfter(s) => s.negate(),
            BinaryFunc::RangeBefore(s) => s.negate(),
            BinaryFunc::RangeContainsElem { .. } => None,
            BinaryFunc::RangeContainsRange { .. } => None,
            BinaryFunc::RangeDifference(s) => s.negate(),
            BinaryFunc::RangeIntersection(s) => s.negate(),
            BinaryFunc::RangeOverlaps(s) => s.negate(),
            BinaryFunc::RangeOverleft(s) => s.negate(),
            BinaryFunc::RangeOverright(s) => s.negate(),
            BinaryFunc::RangeUnion(s) => s.negate(),
            BinaryFunc::RegexpReplace { .. } => None,
            BinaryFunc::RepeatString => None,
            BinaryFunc::Right(s) => s.negate(),
            BinaryFunc::RoundNumeric(s) => s.negate(),
            BinaryFunc::StartsWith(s) => s.negate(),
            BinaryFunc::SubDate(s) => s.negate(),
            BinaryFunc::SubDateInterval(s) => s.negate(),
            BinaryFunc::SubFloat32(s) => s.negate(),
            BinaryFunc::SubFloat64(s) => s.negate(),
            BinaryFunc::SubInt16(s) => s.negate(),
            BinaryFunc::SubInt32(s) => s.negate(),
            BinaryFunc::SubInt64(s) => s.negate(),
            BinaryFunc::SubInterval(s) => s.negate(),
            BinaryFunc::SubNumeric(s) => s.negate(),
            BinaryFunc::SubTime(s) => s.negate(),
            BinaryFunc::SubTimeInterval(s) => s.negate(),
            BinaryFunc::SubTimestamp(s) => s.negate(),
            BinaryFunc::SubTimestampInterval(s) => s.negate(),
            BinaryFunc::SubTimestampTz(s) => s.negate(),
            BinaryFunc::SubTimestampTzInterval(s) => s.negate(),
            BinaryFunc::SubUint16(s) => s.negate(),
            BinaryFunc::SubUint32(s) => s.negate(),
            BinaryFunc::SubUint64(s) => s.negate(),
            BinaryFunc::TextConcat(s) => s.negate(),
            BinaryFunc::TimezoneIntervalTime => None,
            BinaryFunc::TimezoneIntervalTimestamp => None,
            BinaryFunc::TimezoneIntervalTimestampTz => None,
            BinaryFunc::TimezoneOffset(s) => s.negate(),
            BinaryFunc::TimezoneTimestamp => None,
            BinaryFunc::TimezoneTimestampTz => None,
            BinaryFunc::ToCharTimestamp(s) => s.negate(),
            BinaryFunc::ToCharTimestampTz(s) => s.negate(),
            BinaryFunc::Trim(s) => s.negate(),
            BinaryFunc::TrimLeading(s) => s.negate(),
            BinaryFunc::TrimTrailing(s) => s.negate(),
            BinaryFunc::UuidGenerateV5(s) => s.negate(),
        }
    }

    /// Returns true if the function could introduce an error on non-error inputs.
    pub fn could_error(&self) -> bool {
        match self {
            BinaryFunc::AddFloat32(s) => s.could_error(),
            BinaryFunc::AddFloat64(s) => s.could_error(),
            BinaryFunc::AddInt16(s) => s.could_error(),
            BinaryFunc::AddInt32(s) => s.could_error(),
            BinaryFunc::AddInt64(s) => s.could_error(),
            BinaryFunc::AddUint16(s) => s.could_error(),
            BinaryFunc::AddUint32(s) => s.could_error(),
            BinaryFunc::AddUint64(s) => s.could_error(),
            BinaryFunc::ArrayContains(s) => s.could_error(),
            BinaryFunc::ArrayContainsArray(s) => s.could_error(),
            BinaryFunc::ArrayContainsArrayRev(s) => s.could_error(),
            BinaryFunc::ArrayLower(s) => s.could_error(),
            BinaryFunc::BitAndInt16(s) => s.could_error(),
            BinaryFunc::BitAndInt32(s) => s.could_error(),
            BinaryFunc::BitAndInt64(s) => s.could_error(),
            BinaryFunc::BitAndUint16(s) => s.could_error(),
            BinaryFunc::BitAndUint32(s) => s.could_error(),
            BinaryFunc::BitAndUint64(s) => s.could_error(),
            BinaryFunc::BitOrInt16(s) => s.could_error(),
            BinaryFunc::BitOrInt32(s) => s.could_error(),
            BinaryFunc::BitOrInt64(s) => s.could_error(),
            BinaryFunc::BitOrUint16(s) => s.could_error(),
            BinaryFunc::BitOrUint32(s) => s.could_error(),
            BinaryFunc::BitOrUint64(s) => s.could_error(),
            BinaryFunc::BitShiftLeftInt16(s) => s.could_error(),
            BinaryFunc::BitShiftLeftInt32(s) => s.could_error(),
            BinaryFunc::BitShiftLeftInt64(s) => s.could_error(),
            BinaryFunc::BitShiftLeftUint16(s) => s.could_error(),
            BinaryFunc::BitShiftLeftUint32(s) => s.could_error(),
            BinaryFunc::BitShiftLeftUint64(s) => s.could_error(),
            BinaryFunc::BitShiftRightInt16(s) => s.could_error(),
            BinaryFunc::BitShiftRightInt32(s) => s.could_error(),
            BinaryFunc::BitShiftRightInt64(s) => s.could_error(),
            BinaryFunc::BitShiftRightUint16(s) => s.could_error(),
            BinaryFunc::BitShiftRightUint32(s) => s.could_error(),
            BinaryFunc::BitShiftRightUint64(s) => s.could_error(),
            BinaryFunc::BitXorInt16(s) => s.could_error(),
            BinaryFunc::BitXorInt32(s) => s.could_error(),
            BinaryFunc::BitXorInt64(s) => s.could_error(),
            BinaryFunc::BitXorUint16(s) => s.could_error(),
            BinaryFunc::BitXorUint32(s) => s.could_error(),
            BinaryFunc::BitXorUint64(s) => s.could_error(),
            BinaryFunc::ElementListConcat(s) => s.could_error(),
            BinaryFunc::Eq(s) => s.could_error(),
            BinaryFunc::Gt(s) => s.could_error(),
            BinaryFunc::Gte(s) => s.could_error(),
            BinaryFunc::JsonbGetInt64(s) => s.could_error(),
            BinaryFunc::JsonbGetInt64Stringify(s) => s.could_error(),
            BinaryFunc::JsonbGetPath(s) => s.could_error(),
            BinaryFunc::JsonbGetPathStringify(s) => s.could_error(),
            BinaryFunc::JsonbGetString(s) => s.could_error(),
            BinaryFunc::JsonbGetStringStringify(s) => s.could_error(),
            BinaryFunc::ListContainsList(s) => s.could_error(),
            BinaryFunc::ListContainsListRev(s) => s.could_error(),
            BinaryFunc::ListElementConcat(s) => s.could_error(),
            BinaryFunc::ListListConcat(s) => s.could_error(),
            BinaryFunc::ListRemove(s) => s.could_error(),
            BinaryFunc::Lt(s) => s.could_error(),
            BinaryFunc::Lte(s) => s.could_error(),
            BinaryFunc::NotEq(s) => s.could_error(),
            BinaryFunc::RangeAdjacent(s) => s.could_error(),
            BinaryFunc::RangeAfter(s) => s.could_error(),
            BinaryFunc::RangeBefore(s) => s.could_error(),
            BinaryFunc::RangeContainsElem { .. } => false,
            BinaryFunc::RangeContainsRange { .. } => false,
            BinaryFunc::RangeOverlaps(s) => s.could_error(),
            BinaryFunc::RangeOverleft(s) => s.could_error(),
            BinaryFunc::RangeOverright(s) => s.could_error(),
            BinaryFunc::StartsWith(s) => s.could_error(),
            BinaryFunc::TextConcat(s) => s.could_error(),
            BinaryFunc::ToCharTimestamp(s) => s.could_error(),
            BinaryFunc::ToCharTimestampTz(s) => s.could_error(),
            BinaryFunc::Trim(s) => s.could_error(),
            BinaryFunc::TrimLeading(s) => s.could_error(),
            BinaryFunc::TrimTrailing(s) => s.could_error(),

            // All that formally returned true.
            BinaryFunc::AddInterval(s) => s.could_error(),
            BinaryFunc::AddTimestampInterval(s) => s.could_error(),
            BinaryFunc::AddTimestampTzInterval(s) => s.could_error(),
            BinaryFunc::AddDateInterval(s) => s.could_error(),
            BinaryFunc::AddDateTime(s) => s.could_error(),
            BinaryFunc::AddTimeInterval(s) => s.could_error(),
            BinaryFunc::AddNumeric(s) => s.could_error(),
            BinaryFunc::AgeTimestamp(s) => s.could_error(),
            BinaryFunc::AgeTimestampTz(s) => s.could_error(),
            BinaryFunc::SubInt16(s) => s.could_error(),
            BinaryFunc::SubInt32(s) => s.could_error(),
            BinaryFunc::SubInt64(s) => s.could_error(),
            BinaryFunc::SubUint16(s) => s.could_error(),
            BinaryFunc::SubUint32(s) => s.could_error(),
            BinaryFunc::SubUint64(s) => s.could_error(),
            BinaryFunc::SubFloat32(s) => s.could_error(),
            BinaryFunc::SubFloat64(s) => s.could_error(),
            BinaryFunc::SubInterval(s) => s.could_error(),
            BinaryFunc::SubTimestamp(s) => s.could_error(),
            BinaryFunc::SubTimestampTz(s) => s.could_error(),
            BinaryFunc::SubTimestampInterval(s) => s.could_error(),
            BinaryFunc::SubTimestampTzInterval(s) => s.could_error(),
            BinaryFunc::SubDate(s) => s.could_error(),
            BinaryFunc::SubDateInterval(s) => s.could_error(),
            BinaryFunc::SubTime(s) => s.could_error(),
            BinaryFunc::SubTimeInterval(s) => s.could_error(),
            BinaryFunc::SubNumeric(s) => s.could_error(),
            BinaryFunc::MulInt16(s) => s.could_error(),
            BinaryFunc::MulInt32(s) => s.could_error(),
            BinaryFunc::MulInt64(s) => s.could_error(),
            BinaryFunc::MulUint16(s) => s.could_error(),
            BinaryFunc::MulUint32(s) => s.could_error(),
            BinaryFunc::MulUint64(s) => s.could_error(),
            BinaryFunc::MulFloat32(s) => s.could_error(),
            BinaryFunc::MulFloat64(s) => s.could_error(),
            BinaryFunc::MulNumeric(s) => s.could_error(),
            BinaryFunc::MulInterval(s) => s.could_error(),
            BinaryFunc::DivInt16(s) => s.could_error(),
            BinaryFunc::DivInt32(s) => s.could_error(),
            BinaryFunc::DivInt64(s) => s.could_error(),
            BinaryFunc::DivUint16(s) => s.could_error(),
            BinaryFunc::DivUint32(s) => s.could_error(),
            BinaryFunc::DivUint64(s) => s.could_error(),
            BinaryFunc::DivFloat32(s) => s.could_error(),
            BinaryFunc::DivFloat64(s) => s.could_error(),
            BinaryFunc::DivNumeric(s) => s.could_error(),
            BinaryFunc::DivInterval(s) => s.could_error(),
            BinaryFunc::ModInt16(s) => s.could_error(),
            BinaryFunc::ModInt32(s) => s.could_error(),
            BinaryFunc::ModInt64(s) => s.could_error(),
            BinaryFunc::ModUint16(s) => s.could_error(),
            BinaryFunc::ModUint32(s) => s.could_error(),
            BinaryFunc::ModUint64(s) => s.could_error(),
            BinaryFunc::ModFloat32(s) => s.could_error(),
            BinaryFunc::ModFloat64(s) => s.could_error(),
            BinaryFunc::ModNumeric(s) => s.could_error(),
            BinaryFunc::RoundNumeric(s) => s.could_error(),
            BinaryFunc::LikeEscape(s) => s.could_error(),
            BinaryFunc::IsLikeMatchCaseInsensitive(s) => s.could_error(),
            BinaryFunc::IsLikeMatchCaseSensitive(s) => s.could_error(),
            BinaryFunc::IsRegexpMatch { .. } => true,
            BinaryFunc::DateBinTimestamp(s) => s.could_error(),
            BinaryFunc::DateBinTimestampTz(s) => s.could_error(),
            BinaryFunc::ExtractInterval(s) => s.could_error(),
            BinaryFunc::ExtractTime(s) => s.could_error(),
            BinaryFunc::ExtractTimestamp(s) => s.could_error(),
            BinaryFunc::ExtractTimestampTz(s) => s.could_error(),
            BinaryFunc::ExtractDate(s) => s.could_error(),
            BinaryFunc::DatePartInterval(s) => s.could_error(),
            BinaryFunc::DatePartTime(s) => s.could_error(),
            BinaryFunc::DatePartTimestamp(s) => s.could_error(),
            BinaryFunc::DatePartTimestampTz(s) => s.could_error(),
            BinaryFunc::DateTruncTimestamp(s) => s.could_error(),
            BinaryFunc::DateTruncTimestampTz(s) => s.could_error(),
            BinaryFunc::DateTruncInterval(s) => s.could_error(),
            BinaryFunc::TimezoneTimestamp => true,
            BinaryFunc::TimezoneTimestampTz => true,
            BinaryFunc::TimezoneIntervalTimestamp => true,
            BinaryFunc::TimezoneIntervalTimestampTz => true,
            BinaryFunc::TimezoneIntervalTime => true,
            BinaryFunc::TimezoneOffset(s) => s.could_error(),
            BinaryFunc::JsonbContainsString(s) => s.could_error(),
            BinaryFunc::JsonbConcat(s) => s.could_error(),
            BinaryFunc::JsonbContainsJsonb(s) => s.could_error(),
            BinaryFunc::JsonbDeleteInt64(s) => s.could_error(),
            BinaryFunc::JsonbDeleteString(s) => s.could_error(),
            BinaryFunc::MapContainsKey(s) => s.could_error(),
            BinaryFunc::MapGetValue(s) => s.could_error(),
            BinaryFunc::MapContainsAllKeys(s) => s.could_error(),
            BinaryFunc::MapContainsAnyKeys(s) => s.could_error(),
            BinaryFunc::MapContainsMap(s) => s.could_error(),
            BinaryFunc::ConvertFrom(s) => s.could_error(),
            BinaryFunc::Left(s) => s.could_error(),
            BinaryFunc::Position(s) => s.could_error(),
            BinaryFunc::Right(s) => s.could_error(),
            BinaryFunc::RepeatString => true,
            BinaryFunc::Normalize => true,
            BinaryFunc::EncodedBytesCharLength(s) => s.could_error(),
            BinaryFunc::ListLengthMax(s) => s.could_error(),
            BinaryFunc::ArrayLength(s) => s.could_error(),
            BinaryFunc::ArrayRemove(s) => s.could_error(),
            BinaryFunc::ArrayUpper(s) => s.could_error(),
            BinaryFunc::ArrayArrayConcat(s) => s.could_error(),
            BinaryFunc::DigestString(s) => s.could_error(),
            BinaryFunc::DigestBytes(s) => s.could_error(),
            BinaryFunc::MzRenderTypmod(s) => s.could_error(),
            BinaryFunc::Encode(s) => s.could_error(),
            BinaryFunc::Decode(s) => s.could_error(),
            BinaryFunc::LogNumeric(s) => s.could_error(),
            BinaryFunc::Power(s) => s.could_error(),
            BinaryFunc::PowerNumeric(s) => s.could_error(),
            BinaryFunc::GetBit(s) => s.could_error(),
            BinaryFunc::GetByte(s) => s.could_error(),
            BinaryFunc::ConstantTimeEqBytes(s) => s.could_error(),
            BinaryFunc::ConstantTimeEqString(s) => s.could_error(),
            BinaryFunc::RangeUnion(s) => s.could_error(),
            BinaryFunc::RangeIntersection(s) => s.could_error(),
            BinaryFunc::RangeDifference(s) => s.could_error(),
            BinaryFunc::UuidGenerateV5(s) => s.could_error(),
            BinaryFunc::MzAclItemContainsPrivilege(s) => s.could_error(),
            BinaryFunc::ParseIdent(s) => s.could_error(),
            BinaryFunc::PrettySql(s) => s.could_error(),
            BinaryFunc::RegexpReplace { .. } => true,
        }
    }

    /// Returns true if the function is monotone. (Non-strict; either increasing or decreasing.)
    /// Monotone functions map ranges to ranges: ie. given a range of possible inputs, we can
    /// determine the range of possible outputs just by mapping the endpoints.
    ///
    /// This describes the *pointwise* behaviour of the function:
    /// ie. the behaviour of any specific argument as the others are held constant. (For example, `a - b` is
    /// monotone in the first argument because for any particular value of `b`, increasing `a` will
    /// always cause the result to increase... and in the second argument because for any specific `a`,
    /// increasing `b` will always cause the result to _decrease_.)
    ///
    /// This property describes the behaviour of the function over ranges where the function is defined:
    /// ie. the arguments and the result are non-error datums.
    pub fn is_monotone(&self) -> (bool, bool) {
        match self {
            BinaryFunc::AddInt16(s) => s.is_monotone(),
            BinaryFunc::AddInt32(s) => s.is_monotone(),
            BinaryFunc::AddInt64(s) => s.is_monotone(),
            BinaryFunc::AddUint16(s) => s.is_monotone(),
            BinaryFunc::AddUint32(s) => s.is_monotone(),
            BinaryFunc::AddUint64(s) => s.is_monotone(),
            BinaryFunc::AddFloat32(s) => s.is_monotone(),
            BinaryFunc::AddFloat64(s) => s.is_monotone(),
            BinaryFunc::AddInterval(s) => s.is_monotone(),
            BinaryFunc::AddTimestampInterval(s) => s.is_monotone(),
            BinaryFunc::AddTimestampTzInterval(s) => s.is_monotone(),
            BinaryFunc::AddDateInterval(s) => s.is_monotone(),
            BinaryFunc::AddDateTime(s) => s.is_monotone(),
            BinaryFunc::AddNumeric(s) => s.is_monotone(),
            // <time> + <interval> wraps!
            BinaryFunc::AddTimeInterval(s) => s.is_monotone(),
            BinaryFunc::BitAndInt16(s) => s.is_monotone(),
            BinaryFunc::BitAndInt32(s) => s.is_monotone(),
            BinaryFunc::BitAndInt64(s) => s.is_monotone(),
            BinaryFunc::BitAndUint16(s) => s.is_monotone(),
            BinaryFunc::BitAndUint32(s) => s.is_monotone(),
            BinaryFunc::BitAndUint64(s) => s.is_monotone(),
            BinaryFunc::BitOrInt16(s) => s.is_monotone(),
            BinaryFunc::BitOrInt32(s) => s.is_monotone(),
            BinaryFunc::BitOrInt64(s) => s.is_monotone(),
            BinaryFunc::BitOrUint16(s) => s.is_monotone(),
            BinaryFunc::BitOrUint32(s) => s.is_monotone(),
            BinaryFunc::BitOrUint64(s) => s.is_monotone(),
            BinaryFunc::BitXorInt16(s) => s.is_monotone(),
            BinaryFunc::BitXorInt32(s) => s.is_monotone(),
            BinaryFunc::BitXorInt64(s) => s.is_monotone(),
            BinaryFunc::BitXorUint16(s) => s.is_monotone(),
            BinaryFunc::BitXorUint32(s) => s.is_monotone(),
            BinaryFunc::BitXorUint64(s) => s.is_monotone(),
            // The shift functions wrap, which means they are monotonic in neither argument.
            BinaryFunc::BitShiftLeftInt16(s) => s.is_monotone(),
            BinaryFunc::BitShiftLeftInt32(s) => s.is_monotone(),
            BinaryFunc::BitShiftLeftInt64(s) => s.is_monotone(),
            BinaryFunc::BitShiftLeftUint16(s) => s.is_monotone(),
            BinaryFunc::BitShiftLeftUint32(s) => s.is_monotone(),
            BinaryFunc::BitShiftLeftUint64(s) => s.is_monotone(),
            BinaryFunc::BitShiftRightInt16(s) => s.is_monotone(),
            BinaryFunc::BitShiftRightInt32(s) => s.is_monotone(),
            BinaryFunc::BitShiftRightInt64(s) => s.is_monotone(),
            BinaryFunc::BitShiftRightUint16(s) => s.is_monotone(),
            BinaryFunc::BitShiftRightUint32(s) => s.is_monotone(),
            BinaryFunc::BitShiftRightUint64(s) => s.is_monotone(),
            BinaryFunc::SubInt16(s) => s.is_monotone(),
            BinaryFunc::SubInt32(s) => s.is_monotone(),
            BinaryFunc::SubInt64(s) => s.is_monotone(),
            BinaryFunc::SubUint16(s) => s.is_monotone(),
            BinaryFunc::SubUint32(s) => s.is_monotone(),
            BinaryFunc::SubUint64(s) => s.is_monotone(),
            BinaryFunc::SubFloat32(s) => s.is_monotone(),
            BinaryFunc::SubFloat64(s) => s.is_monotone(),
            BinaryFunc::SubInterval(s) => s.is_monotone(),
            BinaryFunc::SubTimestamp(s) => s.is_monotone(),
            BinaryFunc::SubTimestampTz(s) => s.is_monotone(),
            BinaryFunc::SubTimestampInterval(s) => s.is_monotone(),
            BinaryFunc::SubTimestampTzInterval(s) => s.is_monotone(),
            BinaryFunc::SubDate(s) => s.is_monotone(),
            BinaryFunc::SubDateInterval(s) => s.is_monotone(),
            BinaryFunc::SubTime(s) => s.is_monotone(),
            BinaryFunc::SubNumeric(s) => s.is_monotone(),
            // <time> - <interval> wraps!
            BinaryFunc::SubTimeInterval(s) => s.is_monotone(),
            BinaryFunc::MulInt16(s) => s.is_monotone(),
            BinaryFunc::MulInt32(s) => s.is_monotone(),
            BinaryFunc::MulInt64(s) => s.is_monotone(),
            BinaryFunc::MulUint16(s) => s.is_monotone(),
            BinaryFunc::MulUint32(s) => s.is_monotone(),
            BinaryFunc::MulUint64(s) => s.is_monotone(),
            BinaryFunc::MulFloat32(s) => s.is_monotone(),
            BinaryFunc::MulFloat64(s) => s.is_monotone(),
            BinaryFunc::MulNumeric(s) => s.is_monotone(),
            BinaryFunc::MulInterval(s) => s.is_monotone(),
            BinaryFunc::DivInt16(s) => s.is_monotone(),
            BinaryFunc::DivInt32(s) => s.is_monotone(),
            BinaryFunc::DivInt64(s) => s.is_monotone(),
            BinaryFunc::DivUint16(s) => s.is_monotone(),
            BinaryFunc::DivUint32(s) => s.is_monotone(),
            BinaryFunc::DivUint64(s) => s.is_monotone(),
            BinaryFunc::DivFloat32(s) => s.is_monotone(),
            BinaryFunc::DivFloat64(s) => s.is_monotone(),
            BinaryFunc::DivNumeric(s) => s.is_monotone(),
            BinaryFunc::DivInterval(s) => s.is_monotone(),
            BinaryFunc::ModInt16(s) => s.is_monotone(),
            BinaryFunc::ModInt32(s) => s.is_monotone(),
            BinaryFunc::ModInt64(s) => s.is_monotone(),
            BinaryFunc::ModUint16(s) => s.is_monotone(),
            BinaryFunc::ModUint32(s) => s.is_monotone(),
            BinaryFunc::ModUint64(s) => s.is_monotone(),
            BinaryFunc::ModFloat32(s) => s.is_monotone(),
            BinaryFunc::ModFloat64(s) => s.is_monotone(),
            BinaryFunc::ModNumeric(s) => s.is_monotone(),
            BinaryFunc::RoundNumeric(s) => s.is_monotone(),
            BinaryFunc::Eq(s) => s.is_monotone(),
            BinaryFunc::NotEq(s) => s.is_monotone(),
            BinaryFunc::Lt(s) => s.is_monotone(),
            BinaryFunc::Lte(s) => s.is_monotone(),
            BinaryFunc::Gt(s) => s.is_monotone(),
            BinaryFunc::Gte(s) => s.is_monotone(),
            BinaryFunc::LikeEscape(s) => s.is_monotone(),
            BinaryFunc::IsLikeMatchCaseInsensitive(s) => s.is_monotone(),
            BinaryFunc::IsLikeMatchCaseSensitive(s) => s.is_monotone(),
            BinaryFunc::IsRegexpMatch { .. } => (false, false),
            BinaryFunc::ToCharTimestamp(s) => s.is_monotone(),
            BinaryFunc::ToCharTimestampTz(s) => s.is_monotone(),
            BinaryFunc::DateBinTimestamp(s) => s.is_monotone(),
            BinaryFunc::DateBinTimestampTz(s) => s.is_monotone(),
            BinaryFunc::AgeTimestamp(s) => s.is_monotone(),
            BinaryFunc::AgeTimestampTz(s) => s.is_monotone(),
            BinaryFunc::TextConcat(s) => s.is_monotone(),
            BinaryFunc::Left(s) => s.is_monotone(),
            // TODO: can these ever be treated as monotone? It's safe to treat the unary versions
            // as monotone in some cases, but only when extracting specific parts.
            BinaryFunc::ExtractInterval(s) => s.is_monotone(),
            BinaryFunc::ExtractTime(s) => s.is_monotone(),
            BinaryFunc::ExtractTimestamp(s) => s.is_monotone(),
            BinaryFunc::ExtractTimestampTz(s) => s.is_monotone(),
            BinaryFunc::ExtractDate(s) => s.is_monotone(),
            BinaryFunc::DatePartInterval(s) => s.is_monotone(),
            BinaryFunc::DatePartTime(s) => s.is_monotone(),
            BinaryFunc::DatePartTimestamp(s) => s.is_monotone(),
            BinaryFunc::DatePartTimestampTz(s) => s.is_monotone(),
            BinaryFunc::DateTruncTimestamp(s) => s.is_monotone(),
            BinaryFunc::DateTruncTimestampTz(s) => s.is_monotone(),
            BinaryFunc::DateTruncInterval(s) => s.is_monotone(),
            BinaryFunc::TimezoneTimestamp
            | BinaryFunc::TimezoneTimestampTz
            | BinaryFunc::TimezoneIntervalTimestamp
            | BinaryFunc::TimezoneIntervalTimestampTz
            | BinaryFunc::TimezoneIntervalTime => (false, false),
            BinaryFunc::TimezoneOffset(s) => s.is_monotone(),
            BinaryFunc::JsonbGetInt64(s) => s.is_monotone(),
            BinaryFunc::JsonbGetInt64Stringify(s) => s.is_monotone(),
            BinaryFunc::JsonbGetString(s) => s.is_monotone(),
            BinaryFunc::JsonbGetStringStringify(s) => s.is_monotone(),
            BinaryFunc::JsonbGetPath(s) => s.is_monotone(),
            BinaryFunc::JsonbGetPathStringify(s) => s.is_monotone(),
            BinaryFunc::JsonbContainsString(s) => s.is_monotone(),
            BinaryFunc::JsonbConcat(s) => s.is_monotone(),
            BinaryFunc::JsonbContainsJsonb(s) => s.is_monotone(),
            BinaryFunc::JsonbDeleteInt64(s) => s.is_monotone(),
            BinaryFunc::JsonbDeleteString(s) => s.is_monotone(),
            BinaryFunc::MapContainsKey(s) => s.is_monotone(),
            BinaryFunc::MapGetValue(s) => s.is_monotone(),
            BinaryFunc::MapContainsAllKeys(s) => s.is_monotone(),
            BinaryFunc::MapContainsAnyKeys(s) => s.is_monotone(),
            BinaryFunc::MapContainsMap(s) => s.is_monotone(),
            BinaryFunc::ConvertFrom(s) => s.is_monotone(),
            BinaryFunc::Position(s) => s.is_monotone(),
            BinaryFunc::Right(s) => s.is_monotone(),
            BinaryFunc::RepeatString => (false, false),
            BinaryFunc::Trim(s) => s.is_monotone(),
            BinaryFunc::TrimLeading(s) => s.is_monotone(),
            BinaryFunc::TrimTrailing(s) => s.is_monotone(),
            BinaryFunc::EncodedBytesCharLength(s) => s.is_monotone(),
            BinaryFunc::ListLengthMax(s) => s.is_monotone(),
            BinaryFunc::ArrayContains(s) => s.is_monotone(),
            BinaryFunc::ArrayContainsArray(s) => s.is_monotone(),
            BinaryFunc::ArrayContainsArrayRev(s) => s.is_monotone(),
            BinaryFunc::ArrayLength(s) => s.is_monotone(),
            BinaryFunc::ArrayLower(s) => s.is_monotone(),
            BinaryFunc::ArrayRemove(s) => s.is_monotone(),
            BinaryFunc::ArrayUpper(s) => s.is_monotone(),
            BinaryFunc::ArrayArrayConcat(s) => s.is_monotone(),
            BinaryFunc::ListListConcat(s) => s.is_monotone(),
            BinaryFunc::ListElementConcat(s) => s.is_monotone(),
            BinaryFunc::ElementListConcat(s) => s.is_monotone(),
            BinaryFunc::ListContainsList(s) => s.is_monotone(),
            BinaryFunc::ListContainsListRev(s) => s.is_monotone(),
            BinaryFunc::ListRemove(s) => s.is_monotone(),
            BinaryFunc::DigestString(s) => s.is_monotone(),
            BinaryFunc::DigestBytes(s) => s.is_monotone(),
            BinaryFunc::MzRenderTypmod(s) => s.is_monotone(),
            BinaryFunc::Encode(s) => s.is_monotone(),
            BinaryFunc::Decode(s) => s.is_monotone(),
            // TODO: it may be safe to treat these as monotone.
            BinaryFunc::LogNumeric(s) => s.is_monotone(),
            BinaryFunc::Power(s) => s.is_monotone(),
            BinaryFunc::PowerNumeric(s) => s.is_monotone(),
            BinaryFunc::GetBit(s) => s.is_monotone(),
            BinaryFunc::GetByte(s) => s.is_monotone(),
            BinaryFunc::RangeContainsElem { .. } => (false, false),
            BinaryFunc::RangeContainsRange { .. } => (false, false),
            BinaryFunc::RangeOverlaps(s) => s.is_monotone(),
            BinaryFunc::RangeAfter(s) => s.is_monotone(),
            BinaryFunc::RangeBefore(s) => s.is_monotone(),
            BinaryFunc::RangeOverleft(s) => s.is_monotone(),
            BinaryFunc::RangeOverright(s) => s.is_monotone(),
            BinaryFunc::RangeAdjacent(s) => s.is_monotone(),
            BinaryFunc::RangeUnion(s) => s.is_monotone(),
            BinaryFunc::RangeIntersection(s) => s.is_monotone(),
            BinaryFunc::RangeDifference(s) => s.is_monotone(),
            BinaryFunc::UuidGenerateV5(s) => s.is_monotone(),
            BinaryFunc::MzAclItemContainsPrivilege(s) => s.is_monotone(),
            BinaryFunc::ParseIdent(s) => s.is_monotone(),
            BinaryFunc::ConstantTimeEqBytes(s) => s.is_monotone(),
            BinaryFunc::ConstantTimeEqString(s) => s.is_monotone(),
            BinaryFunc::PrettySql(s) => s.is_monotone(),
            BinaryFunc::RegexpReplace { .. } => (false, false),
            BinaryFunc::StartsWith(s) => s.is_monotone(),
            BinaryFunc::Normalize => (false, false),
        }
    }
}

impl fmt::Display for BinaryFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BinaryFunc::AddInt16(s) => s.fmt(f),
            BinaryFunc::AddInt32(s) => s.fmt(f),
            BinaryFunc::AddInt64(s) => s.fmt(f),
            BinaryFunc::AddUint16(s) => s.fmt(f),
            BinaryFunc::AddUint32(s) => s.fmt(f),
            BinaryFunc::AddUint64(s) => s.fmt(f),
            BinaryFunc::AddFloat32(s) => s.fmt(f),
            BinaryFunc::AddFloat64(s) => s.fmt(f),
            BinaryFunc::AddNumeric(s) => s.fmt(f),
            BinaryFunc::AddInterval(s) => s.fmt(f),
            BinaryFunc::AddTimestampInterval(s) => s.fmt(f),
            BinaryFunc::AddTimestampTzInterval(s) => s.fmt(f),
            BinaryFunc::AddDateTime(s) => s.fmt(f),
            BinaryFunc::AddDateInterval(s) => s.fmt(f),
            BinaryFunc::AddTimeInterval(s) => s.fmt(f),
            BinaryFunc::AgeTimestamp(s) => s.fmt(f),
            BinaryFunc::AgeTimestampTz(s) => s.fmt(f),
            BinaryFunc::BitAndInt16(s) => s.fmt(f),
            BinaryFunc::BitAndInt32(s) => s.fmt(f),
            BinaryFunc::BitAndInt64(s) => s.fmt(f),
            BinaryFunc::BitAndUint16(s) => s.fmt(f),
            BinaryFunc::BitAndUint32(s) => s.fmt(f),
            BinaryFunc::BitAndUint64(s) => s.fmt(f),
            BinaryFunc::BitOrInt16(s) => s.fmt(f),
            BinaryFunc::BitOrInt32(s) => s.fmt(f),
            BinaryFunc::BitOrInt64(s) => s.fmt(f),
            BinaryFunc::BitOrUint16(s) => s.fmt(f),
            BinaryFunc::BitOrUint32(s) => s.fmt(f),
            BinaryFunc::BitOrUint64(s) => s.fmt(f),
            BinaryFunc::BitXorInt16(s) => s.fmt(f),
            BinaryFunc::BitXorInt32(s) => s.fmt(f),
            BinaryFunc::BitXorInt64(s) => s.fmt(f),
            BinaryFunc::BitXorUint16(s) => s.fmt(f),
            BinaryFunc::BitXorUint32(s) => s.fmt(f),
            BinaryFunc::BitXorUint64(s) => s.fmt(f),
            BinaryFunc::BitShiftLeftInt16(s) => s.fmt(f),
            BinaryFunc::BitShiftLeftInt32(s) => s.fmt(f),
            BinaryFunc::BitShiftLeftInt64(s) => s.fmt(f),
            BinaryFunc::BitShiftLeftUint16(s) => s.fmt(f),
            BinaryFunc::BitShiftLeftUint32(s) => s.fmt(f),
            BinaryFunc::BitShiftLeftUint64(s) => s.fmt(f),
            BinaryFunc::BitShiftRightInt16(s) => s.fmt(f),
            BinaryFunc::BitShiftRightInt32(s) => s.fmt(f),
            BinaryFunc::BitShiftRightInt64(s) => s.fmt(f),
            BinaryFunc::BitShiftRightUint16(s) => s.fmt(f),
            BinaryFunc::BitShiftRightUint32(s) => s.fmt(f),
            BinaryFunc::BitShiftRightUint64(s) => s.fmt(f),
            BinaryFunc::SubInt16(s) => s.fmt(f),
            BinaryFunc::SubInt32(s) => s.fmt(f),
            BinaryFunc::SubInt64(s) => s.fmt(f),
            BinaryFunc::SubUint16(s) => s.fmt(f),
            BinaryFunc::SubUint32(s) => s.fmt(f),
            BinaryFunc::SubUint64(s) => s.fmt(f),
            BinaryFunc::SubFloat32(s) => s.fmt(f),
            BinaryFunc::SubFloat64(s) => s.fmt(f),
            BinaryFunc::SubNumeric(s) => s.fmt(f),
            BinaryFunc::SubInterval(s) => s.fmt(f),
            BinaryFunc::SubTimestamp(s) => s.fmt(f),
            BinaryFunc::SubTimestampTz(s) => s.fmt(f),
            BinaryFunc::SubTimestampInterval(s) => s.fmt(f),
            BinaryFunc::SubTimestampTzInterval(s) => s.fmt(f),
            BinaryFunc::SubDate(s) => s.fmt(f),
            BinaryFunc::SubDateInterval(s) => s.fmt(f),
            BinaryFunc::SubTime(s) => s.fmt(f),
            BinaryFunc::SubTimeInterval(s) => s.fmt(f),
            BinaryFunc::MulInt16(s) => s.fmt(f),
            BinaryFunc::MulInt32(s) => s.fmt(f),
            BinaryFunc::MulInt64(s) => s.fmt(f),
            BinaryFunc::MulUint16(s) => s.fmt(f),
            BinaryFunc::MulUint32(s) => s.fmt(f),
            BinaryFunc::MulUint64(s) => s.fmt(f),
            BinaryFunc::MulFloat32(s) => s.fmt(f),
            BinaryFunc::MulFloat64(s) => s.fmt(f),
            BinaryFunc::MulNumeric(s) => s.fmt(f),
            BinaryFunc::MulInterval(s) => s.fmt(f),
            BinaryFunc::DivInt16(s) => s.fmt(f),
            BinaryFunc::DivInt32(s) => s.fmt(f),
            BinaryFunc::DivInt64(s) => s.fmt(f),
            BinaryFunc::DivUint16(s) => s.fmt(f),
            BinaryFunc::DivUint32(s) => s.fmt(f),
            BinaryFunc::DivUint64(s) => s.fmt(f),
            BinaryFunc::DivFloat32(s) => s.fmt(f),
            BinaryFunc::DivFloat64(s) => s.fmt(f),
            BinaryFunc::DivNumeric(s) => s.fmt(f),
            BinaryFunc::DivInterval(s) => s.fmt(f),
            BinaryFunc::ModInt16(s) => s.fmt(f),
            BinaryFunc::ModInt32(s) => s.fmt(f),
            BinaryFunc::ModInt64(s) => s.fmt(f),
            BinaryFunc::ModUint16(s) => s.fmt(f),
            BinaryFunc::ModUint32(s) => s.fmt(f),
            BinaryFunc::ModUint64(s) => s.fmt(f),
            BinaryFunc::ModFloat32(s) => s.fmt(f),
            BinaryFunc::ModFloat64(s) => s.fmt(f),
            BinaryFunc::ModNumeric(s) => s.fmt(f),
            BinaryFunc::Eq(s) => s.fmt(f),
            BinaryFunc::NotEq(s) => s.fmt(f),
            BinaryFunc::Lt(s) => s.fmt(f),
            BinaryFunc::Lte(s) => s.fmt(f),
            BinaryFunc::Gt(s) => s.fmt(f),
            BinaryFunc::Gte(s) => s.fmt(f),
            BinaryFunc::LikeEscape(s) => s.fmt(f),
            BinaryFunc::IsLikeMatchCaseSensitive(s) => s.fmt(f),
            BinaryFunc::IsLikeMatchCaseInsensitive(s) => s.fmt(f),
            BinaryFunc::IsRegexpMatch {
                case_insensitive: false,
            } => f.write_str("~"),
            BinaryFunc::IsRegexpMatch {
                case_insensitive: true,
            } => f.write_str("~*"),
            BinaryFunc::ToCharTimestamp(s) => s.fmt(f),
            BinaryFunc::ToCharTimestampTz(s) => s.fmt(f),
            BinaryFunc::DateBinTimestamp(s) => s.fmt(f),
            BinaryFunc::DateBinTimestampTz(s) => s.fmt(f),
            BinaryFunc::ExtractInterval(s) => s.fmt(f),
            BinaryFunc::ExtractTime(s) => s.fmt(f),
            BinaryFunc::ExtractTimestamp(s) => s.fmt(f),
            BinaryFunc::ExtractTimestampTz(s) => s.fmt(f),
            BinaryFunc::ExtractDate(s) => s.fmt(f),
            BinaryFunc::DatePartInterval(s) => s.fmt(f),
            BinaryFunc::DatePartTime(s) => s.fmt(f),
            BinaryFunc::DatePartTimestamp(s) => s.fmt(f),
            BinaryFunc::DatePartTimestampTz(s) => s.fmt(f),
            BinaryFunc::DateTruncTimestamp(s) => s.fmt(f),
            BinaryFunc::DateTruncInterval(s) => s.fmt(f),
            BinaryFunc::DateTruncTimestampTz(s) => s.fmt(f),
            BinaryFunc::TimezoneTimestamp => f.write_str("timezonets"),
            BinaryFunc::TimezoneTimestampTz => f.write_str("timezonetstz"),
            BinaryFunc::TimezoneIntervalTimestamp => f.write_str("timezoneits"),
            BinaryFunc::TimezoneIntervalTimestampTz => f.write_str("timezoneitstz"),
            BinaryFunc::TimezoneIntervalTime => f.write_str("timezoneit"),
            BinaryFunc::TimezoneOffset(s) => s.fmt(f),
            BinaryFunc::TextConcat(s) => s.fmt(f),
            BinaryFunc::JsonbGetInt64(s) => s.fmt(f),
            BinaryFunc::JsonbGetInt64Stringify(s) => s.fmt(f),
            BinaryFunc::JsonbGetString(s) => s.fmt(f),
            BinaryFunc::JsonbGetStringStringify(s) => s.fmt(f),
            BinaryFunc::JsonbGetPath(s) => s.fmt(f),
            BinaryFunc::JsonbGetPathStringify(s) => s.fmt(f),
            BinaryFunc::JsonbContainsString(s) => s.fmt(f),
            BinaryFunc::MapContainsKey(s) => s.fmt(f),
            BinaryFunc::JsonbConcat(s) => s.fmt(f),
            BinaryFunc::JsonbContainsJsonb(s) => s.fmt(f),
            BinaryFunc::MapContainsMap(s) => s.fmt(f),
            BinaryFunc::JsonbDeleteInt64(s) => s.fmt(f),
            BinaryFunc::JsonbDeleteString(s) => s.fmt(f),
            BinaryFunc::MapGetValue(s) => s.fmt(f),
            BinaryFunc::MapContainsAllKeys(s) => s.fmt(f),
            BinaryFunc::MapContainsAnyKeys(s) => s.fmt(f),
            BinaryFunc::RoundNumeric(s) => s.fmt(f),
            BinaryFunc::ConvertFrom(s) => s.fmt(f),
            BinaryFunc::Left(s) => s.fmt(f),
            BinaryFunc::Position(s) => s.fmt(f),
            BinaryFunc::Right(s) => s.fmt(f),
            BinaryFunc::Trim(s) => s.fmt(f),
            BinaryFunc::TrimLeading(s) => s.fmt(f),
            BinaryFunc::TrimTrailing(s) => s.fmt(f),
            BinaryFunc::EncodedBytesCharLength(s) => s.fmt(f),
            BinaryFunc::ListLengthMax(s) => s.fmt(f),
            BinaryFunc::ArrayContains(s) => s.fmt(f),
            BinaryFunc::ArrayContainsArray(s) => s.fmt(f),
            BinaryFunc::ArrayContainsArrayRev(s) => s.fmt(f),
            BinaryFunc::ArrayLength(s) => s.fmt(f),
            BinaryFunc::ArrayLower(s) => s.fmt(f),
            BinaryFunc::ArrayRemove(s) => s.fmt(f),
            BinaryFunc::ArrayUpper(s) => s.fmt(f),
            BinaryFunc::ArrayArrayConcat(s) => s.fmt(f),
            BinaryFunc::ListListConcat(s) => s.fmt(f),
            BinaryFunc::ListElementConcat(s) => s.fmt(f),
            BinaryFunc::ElementListConcat(s) => s.fmt(f),
            BinaryFunc::ListRemove(s) => s.fmt(f),
            BinaryFunc::ListContainsList(s) => s.fmt(f),
            BinaryFunc::ListContainsListRev(s) => s.fmt(f),
            BinaryFunc::DigestString(s) => s.fmt(f),
            BinaryFunc::DigestBytes(s) => s.fmt(f),
            BinaryFunc::MzRenderTypmod(s) => s.fmt(f),
            BinaryFunc::Encode(s) => s.fmt(f),
            BinaryFunc::Decode(s) => s.fmt(f),
            BinaryFunc::LogNumeric(s) => s.fmt(f),
            BinaryFunc::Power(s) => s.fmt(f),
            BinaryFunc::PowerNumeric(s) => s.fmt(f),
            BinaryFunc::RepeatString => f.write_str("repeat"),
            BinaryFunc::Normalize => f.write_str("normalize"),
            BinaryFunc::GetBit(s) => s.fmt(f),
            BinaryFunc::GetByte(s) => s.fmt(f),
            BinaryFunc::ConstantTimeEqBytes(s) => s.fmt(f),
            BinaryFunc::ConstantTimeEqString(s) => s.fmt(f),
            BinaryFunc::RangeContainsElem { rev, .. } => {
                f.write_str(if *rev { "<@" } else { "@>" })
            }
            BinaryFunc::RangeContainsRange { rev, .. } => {
                f.write_str(if *rev { "<@" } else { "@>" })
            }
            BinaryFunc::RangeOverlaps(s) => s.fmt(f),
            BinaryFunc::RangeAfter(s) => s.fmt(f),
            BinaryFunc::RangeBefore(s) => s.fmt(f),
            BinaryFunc::RangeOverleft(s) => s.fmt(f),
            BinaryFunc::RangeOverright(s) => s.fmt(f),
            BinaryFunc::RangeAdjacent(s) => s.fmt(f),
            BinaryFunc::RangeUnion(s) => s.fmt(f),
            BinaryFunc::RangeIntersection(s) => s.fmt(f),
            BinaryFunc::RangeDifference(s) => s.fmt(f),
            BinaryFunc::UuidGenerateV5(s) => s.fmt(f),
            BinaryFunc::MzAclItemContainsPrivilege(s) => s.fmt(f),
            BinaryFunc::ParseIdent(s) => s.fmt(f),
            BinaryFunc::PrettySql(s) => s.fmt(f),
            BinaryFunc::RegexpReplace { regex, limit } => write!(
                f,
                "regexp_replace[{}, case_insensitive={}, limit={}]",
                regex.pattern().escaped(),
                regex.case_insensitive,
                limit
            ),
            BinaryFunc::StartsWith(s) => s.fmt(f),
        }
    }
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
fn text_concat_binary(
    a: &str,
    b: &str,
) -> Result<String, EvalError> {
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

fn is_regexp_match_dynamic<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    case_insensitive: bool,
) -> Result<Datum<'a>, EvalError> {
    let haystack = a.unwrap_str();
    let needle = build_regex(b.unwrap_str(), if case_insensitive { "i" } else { "" })?;
    Ok(Datum::from(needle.is_match(haystack)))
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

// WARNING: This function has potential OOM risk if used with an inflationary
// replacement pattern. It is very difficult to calculate the output size ahead
// of time because the replacement pattern may depend on capture groups.
fn regexp_replace_static<'a>(
    source: Datum<'a>,
    replacement: Datum<'a>,
    regexp: &regex::Regex,
    limit: usize,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let replaced = match regexp.replacen(source.unwrap_str(), limit, replacement.unwrap_str()) {
        Cow::Borrowed(s) => s,
        Cow::Owned(s) => temp_storage.push_string(s),
    };
    Ok(Datum::String(replaced))
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

fn repeat_string<'a>(
    string: Datum<'a>,
    count: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let len = usize::try_from(count.unwrap_int32()).unwrap_or(0);
    let string = string.unwrap_str();
    if (len * string.len()) > MAX_STRING_FUNC_RESULT_BYTES {
        return Err(EvalError::LengthTooLarge);
    }
    Ok(Datum::String(temp_storage.push_string(string.repeat(len))))
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
            strconv::format_record(buf, &d.unwrap_list(), |buf, d| {
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
            &d.unwrap_array().elements(),
            |buf, d| {
                if d.is_null() {
                    Ok(buf.write_null())
                } else {
                    stringify_datum(buf.nonnull_buffer(), d, elem_type)
                }
            },
        ),
        List { element_type, .. } => strconv::format_list(buf, &d.unwrap_list(), |buf, d| {
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
        Int2Vector => strconv::format_legacy_vector(buf, &d.unwrap_array().elements(), |buf, d| {
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
    is_infix_op = true,
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
