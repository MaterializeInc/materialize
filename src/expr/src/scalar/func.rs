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
use std::ops::Deref;
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
use mz_ore::soft_assert_eq_or_log;
use mz_ore::str::StrExt;
use mz_pgrepr::Type;
use mz_pgtz::timezone::{Timezone, TimezoneSpec};
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::date::Date;
use mz_repr::adt::interval::{Interval, RoundBehavior};
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::adt::mz_acl_item::AclMode;
use mz_repr::adt::numeric::{self, DecimalLike, Numeric};
use mz_repr::adt::range::{Range, RangeOps};
use mz_repr::adt::regex::Regex;
use mz_repr::adt::timestamp::{CheckedTimestamp, TimestampLike};
use mz_repr::{Datum, DatumType, Row, RowArena, SqlColumnType, SqlScalarType, strconv};
use mz_sql_parser::ast::display::FormatMode;
use mz_sql_pretty::{PrettyConfig, pretty_str};
use num::traits::CheckedNeg;
use proptest::prelude::*;
use proptest::strategy::*;
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

/// The maximum size of a newly allocated string. Chosen to be the smallest number to keep our tests
/// passing without changing. 100MiB is probably higher than what we want, but it's better than no
/// limit.
const MAX_STRING_BYTES: usize = 1024 * 1024 * 100;

pub fn jsonb_stringify<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    match a {
        Datum::JsonNull => Datum::Null,
        Datum::String(_) => a,
        _ => {
            let s = cast_jsonb_to_string(JsonbRef::from_datum(a));
            Datum::String(temp_storage.push_string(s))
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

fn sub_timestamplike_interval<'a, T>(
    a: CheckedTimestamp<T>,
    b: Datum,
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
fn add_date_time<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let date = a.unwrap_date();
    let time = b.unwrap_time();

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
fn add_date_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let date = a.unwrap_date();
    let interval = b.unwrap_interval();

    let dt = NaiveDate::from(date).and_hms_opt(0, 0, 0).unwrap();
    let dt = add_timestamp_months(&dt, interval.months)?;
    let dt = dt
        .checked_add_signed(interval.duration_as_chrono())
        .ok_or(EvalError::TimestampOutOfRange)?;
    Ok(dt.try_into()?)
}

#[sqlfunc(
    is_monotone = "(false, false)",
    output_type = "CheckedTimestamp<chrono::DateTime<Utc>>",
    is_infix_op = true,
    sqlname = "+",
    propagates_nulls = true
)]
fn add_time_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let time = a.unwrap_time();
    let interval = b.unwrap_interval();
    let (t, _) = time.overflowing_add_signed(interval.duration_as_chrono());
    Datum::Time(t)
}

#[sqlfunc(
    is_monotone = "(true, false)",
    output_type = "Numeric",
    sqlname = "round",
    propagates_nulls = true
)]
fn round_numeric_binary<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut a = a.unwrap_numeric().0;
    let mut b = b.unwrap_int32();
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
        Ok(Datum::from(numeric::Numeric::zero()))
    } else {
        numeric::munge_numeric(&mut a).unwrap();
        Ok(Datum::from(a))
    }
}

#[sqlfunc(
    output_type = "String",
    sqlname = "convert_from",
    propagates_nulls = true
)]
fn convert_from<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    // Convert PostgreSQL-style encoding names[1] to WHATWG-style encoding names[2],
    // which the encoding library uses[3].
    // [1]: https://www.postgresql.org/docs/9.5/multibyte.html
    // [2]: https://encoding.spec.whatwg.org/
    // [3]: https://github.com/lifthrasiir/rust-encoding/blob/4e79c35ab6a351881a86dbff565c4db0085cc113/src/label.rs
    let encoding_name = b
        .unwrap_str()
        .to_lowercase()
        .replace('_', "-")
        .into_boxed_str();

    // Supporting other encodings is tracked by database-issues#797.
    if encoding_from_whatwg_label(&encoding_name).map(|e| e.name()) != Some("utf-8") {
        return Err(EvalError::InvalidEncodingName(encoding_name));
    }

    match str::from_utf8(a.unwrap_bytes()) {
        Ok(from) => Ok(Datum::String(from)),
        Err(e) => Err(EvalError::InvalidByteSequence {
            byte_sequence: e.to_string().into(),
            encoding_name,
        }),
    }
}

#[sqlfunc(output_type = "String", propagates_nulls = true)]
fn encode<'a>(
    bytes: Datum<'a>,
    format: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let format = encoding::lookup_format(format.unwrap_str())?;
    let out = format.encode(bytes.unwrap_bytes());
    Ok(Datum::from(temp_storage.push_string(out)))
}

#[sqlfunc(output_type = "Vec<u8>", propagates_nulls = true)]
fn decode<'a>(
    string: Datum<'a>,
    format: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let format = encoding::lookup_format(format.unwrap_str())?;
    let out = format.decode(string.unwrap_str())?;
    Ok(Datum::from(temp_storage.push_bytes(out)))
}

#[sqlfunc(output_type = "i32", sqlname = "length", propagates_nulls = true)]
fn encoded_bytes_char_length<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    // Convert PostgreSQL-style encoding names[1] to WHATWG-style encoding names[2],
    // which the encoding library uses[3].
    // [1]: https://www.postgresql.org/docs/9.5/multibyte.html
    // [2]: https://encoding.spec.whatwg.org/
    // [3]: https://github.com/lifthrasiir/rust-encoding/blob/4e79c35ab6a351881a86dbff565c4db0085cc113/src/label.rs
    let encoding_name = b
        .unwrap_str()
        .to_lowercase()
        .replace('_', "-")
        .into_boxed_str();

    let enc = match encoding_from_whatwg_label(&encoding_name) {
        Some(enc) => enc,
        None => return Err(EvalError::InvalidEncodingName(encoding_name)),
    };

    let decoded_string = match enc.decode(a.unwrap_bytes(), DecoderTrap::Strict) {
        Ok(s) => s,
        Err(e) => {
            return Err(EvalError::InvalidByteSequence {
                byte_sequence: e.into(),
                encoding_name,
            });
        }
    };

    let count = decoded_string.chars().count();
    match i32::try_from(count) {
        Ok(l) => Ok(Datum::from(l)),
        Err(_) => Err(EvalError::Int32OutOfRange(count.to_string().into())),
    }
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
    output_type = "Numeric",
    is_infix_op = true,
    sqlname = "+",
    propagates_nulls = true
)]
fn add_numeric<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut cx = numeric::cx_datum();
    let mut a = a.unwrap_numeric().0;
    cx.add(&mut a, &b.unwrap_numeric().0);
    if cx.status().overflow() {
        Err(EvalError::FloatOverflow)
    } else {
        Ok(Datum::from(a))
    }
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "Interval",
    is_infix_op = true,
    sqlname = "+",
    propagates_nulls = true
)]
fn add_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_interval()
        .checked_add(&b.unwrap_interval())
        .ok_or_else(|| EvalError::IntervalOutOfRange(format!("{a} + {b}").into()))
        .map(Datum::from)
}

#[sqlfunc(
    output_type = "i16",
    is_infix_op = true,
    sqlname = "&",
    propagates_nulls = true
)]
fn bit_and_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int16() & b.unwrap_int16())
}

#[sqlfunc(
    output_type = "i32",
    is_infix_op = true,
    sqlname = "&",
    propagates_nulls = true
)]
fn bit_and_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int32() & b.unwrap_int32())
}

#[sqlfunc(
    output_type = "i64",
    is_infix_op = true,
    sqlname = "&",
    propagates_nulls = true
)]
fn bit_and_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int64() & b.unwrap_int64())
}

#[sqlfunc(
    output_type = "u16",
    is_infix_op = true,
    sqlname = "&",
    propagates_nulls = true
)]
fn bit_and_uint16<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_uint16() & b.unwrap_uint16())
}

#[sqlfunc(
    output_type = "u32",
    is_infix_op = true,
    sqlname = "&",
    propagates_nulls = true
)]
fn bit_and_uint32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_uint32() & b.unwrap_uint32())
}

#[sqlfunc(
    output_type = "u64",
    is_infix_op = true,
    sqlname = "&",
    propagates_nulls = true
)]
fn bit_and_uint64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_uint64() & b.unwrap_uint64())
}

#[sqlfunc(
    output_type = "i16",
    is_infix_op = true,
    sqlname = "|",
    propagates_nulls = true
)]
fn bit_or_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int16() | b.unwrap_int16())
}

#[sqlfunc(
    output_type = "i32",
    is_infix_op = true,
    sqlname = "|",
    propagates_nulls = true
)]
fn bit_or_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int32() | b.unwrap_int32())
}

#[sqlfunc(
    output_type = "i64",
    is_infix_op = true,
    sqlname = "|",
    propagates_nulls = true
)]
fn bit_or_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int64() | b.unwrap_int64())
}

#[sqlfunc(
    output_type = "u16",
    is_infix_op = true,
    sqlname = "|",
    propagates_nulls = true
)]
fn bit_or_uint16<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_uint16() | b.unwrap_uint16())
}

#[sqlfunc(
    output_type = "u32",
    is_infix_op = true,
    sqlname = "|",
    propagates_nulls = true
)]
fn bit_or_uint32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_uint32() | b.unwrap_uint32())
}

#[sqlfunc(
    output_type = "u64",
    is_infix_op = true,
    sqlname = "|",
    propagates_nulls = true
)]
fn bit_or_uint64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_uint64() | b.unwrap_uint64())
}

#[sqlfunc(
    output_type = "i16",
    is_infix_op = true,
    sqlname = "#",
    propagates_nulls = true
)]
fn bit_xor_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int16() ^ b.unwrap_int16())
}

#[sqlfunc(
    output_type = "i32",
    is_infix_op = true,
    sqlname = "#",
    propagates_nulls = true
)]
fn bit_xor_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int32() ^ b.unwrap_int32())
}

#[sqlfunc(
    output_type = "i64",
    is_infix_op = true,
    sqlname = "#",
    propagates_nulls = true
)]
fn bit_xor_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int64() ^ b.unwrap_int64())
}

#[sqlfunc(
    output_type = "u16",
    is_infix_op = true,
    sqlname = "#",
    propagates_nulls = true
)]
fn bit_xor_uint16<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_uint16() ^ b.unwrap_uint16())
}

#[sqlfunc(
    output_type = "u32",
    is_infix_op = true,
    sqlname = "#",
    propagates_nulls = true
)]
fn bit_xor_uint32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_uint32() ^ b.unwrap_uint32())
}

#[sqlfunc(
    output_type = "u64",
    is_infix_op = true,
    sqlname = "#",
    propagates_nulls = true
)]
fn bit_xor_uint64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_uint64() ^ b.unwrap_uint64())
}

#[sqlfunc(
    output_type = "i16",
    is_infix_op = true,
    sqlname = "<<",
    propagates_nulls = true
)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn bit_shift_left_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    // widen to i32 and then cast back to i16 in order emulate the C promotion rules used in by Postgres
    // when the rhs in the 16-31 range, e.g. (1 << 17 should evaluate to 0)
    // see https://github.com/postgres/postgres/blob/REL_14_STABLE/src/backend/utils/adt/int.c#L1460-L1476
    let lhs: i32 = a.unwrap_int16() as i32;
    let rhs: u32 = b.unwrap_int32() as u32;
    Datum::from(lhs.wrapping_shl(rhs) as i16)
}

#[sqlfunc(
    output_type = "i32",
    is_infix_op = true,
    sqlname = "<<",
    propagates_nulls = true
)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn bit_shift_left_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let lhs = a.unwrap_int32();
    let rhs = b.unwrap_int32() as u32;
    Datum::from(lhs.wrapping_shl(rhs))
}

#[sqlfunc(
    output_type = "i64",
    is_infix_op = true,
    sqlname = "<<",
    propagates_nulls = true
)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn bit_shift_left_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let lhs = a.unwrap_int64();
    let rhs = b.unwrap_int32() as u32;
    Datum::from(lhs.wrapping_shl(rhs))
}

#[sqlfunc(
    output_type = "u16",
    is_infix_op = true,
    sqlname = "<<",
    propagates_nulls = true
)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn bit_shift_left_uint16<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    // widen to u32 and then cast back to u16 in order emulate the C promotion rules used in by Postgres
    // when the rhs in the 16-31 range, e.g. (1 << 17 should evaluate to 0)
    // see https://github.com/postgres/postgres/blob/REL_14_STABLE/src/backend/utils/adt/int.c#L1460-L1476
    let lhs: u32 = a.unwrap_uint16() as u32;
    let rhs: u32 = b.unwrap_uint32();
    Datum::from(lhs.wrapping_shl(rhs) as u16)
}

#[sqlfunc(
    output_type = "u32",
    is_infix_op = true,
    sqlname = "<<",
    propagates_nulls = true
)]
fn bit_shift_left_uint32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let lhs = a.unwrap_uint32();
    let rhs = b.unwrap_uint32();
    Datum::from(lhs.wrapping_shl(rhs))
}

#[sqlfunc(
    output_type = "u64",
    is_infix_op = true,
    sqlname = "<<",
    propagates_nulls = true
)]
fn bit_shift_left_uint64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let lhs = a.unwrap_uint64();
    let rhs = b.unwrap_uint32();
    Datum::from(lhs.wrapping_shl(rhs))
}

#[sqlfunc(
    output_type = "i16",
    is_infix_op = true,
    sqlname = ">>",
    propagates_nulls = true
)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn bit_shift_right_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    // widen to i32 and then cast back to i16 in order emulate the C promotion rules used in by Postgres
    // when the rhs in the 16-31 range, e.g. (-32767 >> 17 should evaluate to -1)
    // see https://github.com/postgres/postgres/blob/REL_14_STABLE/src/backend/utils/adt/int.c#L1460-L1476
    let lhs = a.unwrap_int16() as i32;
    let rhs = b.unwrap_int32() as u32;
    Datum::from(lhs.wrapping_shr(rhs) as i16)
}

#[sqlfunc(
    output_type = "i32",
    is_infix_op = true,
    sqlname = ">>",
    propagates_nulls = true
)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn bit_shift_right_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let lhs = a.unwrap_int32();
    let rhs = b.unwrap_int32() as u32;
    Datum::from(lhs.wrapping_shr(rhs))
}

#[sqlfunc(
    output_type = "i64",
    is_infix_op = true,
    sqlname = ">>",
    propagates_nulls = true
)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn bit_shift_right_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let lhs = a.unwrap_int64();
    let rhs = b.unwrap_int32() as u32;
    Datum::from(lhs.wrapping_shr(rhs))
}

#[sqlfunc(
    output_type = "u16",
    is_infix_op = true,
    sqlname = ">>",
    propagates_nulls = true
)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn bit_shift_right_uint16<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    // widen to u32 and then cast back to u16 in order emulate the C promotion rules used in by Postgres
    // when the rhs in the 16-31 range, e.g. (-32767 >> 17 should evaluate to -1)
    // see https://github.com/postgres/postgres/blob/REL_14_STABLE/src/backend/utils/adt/int.c#L1460-L1476
    let lhs = a.unwrap_uint16() as u32;
    let rhs = b.unwrap_uint32();
    Datum::from(lhs.wrapping_shr(rhs) as u16)
}

#[sqlfunc(
    output_type = "u32",
    is_infix_op = true,
    sqlname = ">>",
    propagates_nulls = true
)]
fn bit_shift_right_uint32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let lhs = a.unwrap_uint32();
    let rhs = b.unwrap_uint32();
    Datum::from(lhs.wrapping_shr(rhs))
}

#[sqlfunc(
    output_type = "u64",
    is_infix_op = true,
    sqlname = ">>",
    propagates_nulls = true
)]
fn bit_shift_right_uint64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let lhs = a.unwrap_uint64();
    let rhs = b.unwrap_uint32();
    Datum::from(lhs.wrapping_shr(rhs))
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "i16",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int16()
        .checked_sub(b.unwrap_int16())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "i32",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int32()
        .checked_sub(b.unwrap_int32())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "i64",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int64()
        .checked_sub(b.unwrap_int64())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "u16",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_uint16<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_uint16()
        .checked_sub(b.unwrap_uint16())
        .ok_or_else(|| EvalError::UInt16OutOfRange(format!("{a} - {b}").into()))
        .map(Datum::from)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "u32",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_uint32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_uint32()
        .checked_sub(b.unwrap_uint32())
        .ok_or_else(|| EvalError::UInt32OutOfRange(format!("{a} - {b}").into()))
        .map(Datum::from)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "u64",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_uint64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_uint64()
        .checked_sub(b.unwrap_uint64())
        .ok_or_else(|| EvalError::UInt64OutOfRange(format!("{a} - {b}").into()))
        .map(Datum::from)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "f32",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_float32();
    let b = b.unwrap_float32();
    let difference = a - b;
    if difference.is_infinite() && !a.is_infinite() && !b.is_infinite() {
        Err(EvalError::FloatOverflow)
    } else {
        Ok(Datum::from(difference))
    }
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "f64",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_float64();
    let b = b.unwrap_float64();
    let difference = a - b;
    if difference.is_infinite() && !a.is_infinite() && !b.is_infinite() {
        Err(EvalError::FloatOverflow)
    } else {
        Ok(Datum::from(difference))
    }
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "Numeric",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_numeric<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut cx = numeric::cx_datum();
    let mut a = a.unwrap_numeric().0;
    cx.sub(&mut a, &b.unwrap_numeric().0);
    if cx.status().overflow() {
        Err(EvalError::FloatOverflow)
    } else {
        Ok(Datum::from(a))
    }
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "Interval",
    sqlname = "age",
    propagates_nulls = true
)]
fn age_timestamp<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let a_ts = a.unwrap_timestamp();
    let b_ts = b.unwrap_timestamp();
    let age = a_ts.age(&b_ts)?;

    Ok(Datum::from(age))
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "Interval",
    sqlname = "age",
    propagates_nulls = true
)]
fn age_timestamptz<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let a_ts = a.unwrap_timestamptz();
    let b_ts = b.unwrap_timestamptz();
    let age = a_ts.age(&b_ts)?;

    Ok(Datum::from(age))
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "Interval",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_timestamp<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_timestamp() - b.unwrap_timestamp())
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "Interval",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_timestamptz<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_timestamptz() - b.unwrap_timestamptz())
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "i32",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_date<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_date() - b.unwrap_date())
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "Interval",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_time<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_time() - b.unwrap_time())
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "Interval",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    b.unwrap_interval()
        .checked_neg()
        .and_then(|b| b.checked_add(&a.unwrap_interval()))
        .ok_or_else(|| EvalError::IntervalOutOfRange(format!("{a} - {b}").into()))
        .map(Datum::from)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "CheckedTimestamp<NaiveDateTime>",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_date_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let date = a.unwrap_date();
    let interval = b.unwrap_interval();

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
    output_type = "chrono::NaiveTime",
    is_infix_op = true,
    sqlname = "-",
    propagates_nulls = true
)]
fn sub_time_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let time = a.unwrap_time();
    let interval = b.unwrap_interval();
    let (t, _) = time.overflowing_sub_signed(interval.duration_as_chrono());
    Datum::Time(t)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "i16",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int16()
        .checked_mul(b.unwrap_int16())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "i32",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int32()
        .checked_mul(b.unwrap_int32())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "i64",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int64()
        .checked_mul(b.unwrap_int64())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "u16",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_uint16<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_uint16()
        .checked_mul(b.unwrap_uint16())
        .ok_or_else(|| EvalError::UInt16OutOfRange(format!("{a} * {b}").into()))
        .map(Datum::from)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "u32",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_uint32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_uint32()
        .checked_mul(b.unwrap_uint32())
        .ok_or_else(|| EvalError::UInt32OutOfRange(format!("{a} * {b}").into()))
        .map(Datum::from)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "u64",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_uint64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_uint64()
        .checked_mul(b.unwrap_uint64())
        .ok_or_else(|| EvalError::UInt64OutOfRange(format!("{a} * {b}").into()))
        .map(Datum::from)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "f32",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_float32();
    let b = b.unwrap_float32();
    let product = a * b;
    if product.is_infinite() && !a.is_infinite() && !b.is_infinite() {
        Err(EvalError::FloatOverflow)
    } else if product == 0.0f32 && a != 0.0f32 && b != 0.0f32 {
        Err(EvalError::FloatUnderflow)
    } else {
        Ok(Datum::from(product))
    }
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "f64",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_float64();
    let b = b.unwrap_float64();
    let product = a * b;
    if product.is_infinite() && !a.is_infinite() && !b.is_infinite() {
        Err(EvalError::FloatOverflow)
    } else if product == 0.0f64 && a != 0.0f64 && b != 0.0f64 {
        Err(EvalError::FloatUnderflow)
    } else {
        Ok(Datum::from(product))
    }
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "Numeric",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_numeric<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut cx = numeric::cx_datum();
    let mut a = a.unwrap_numeric().0;
    cx.mul(&mut a, &b.unwrap_numeric().0);
    let cx_status = cx.status();
    if cx_status.overflow() {
        Err(EvalError::FloatOverflow)
    } else if cx_status.subnormal() {
        Err(EvalError::FloatUnderflow)
    } else {
        numeric::munge_numeric(&mut a).unwrap();
        Ok(Datum::from(a))
    }
}

#[sqlfunc(
    is_monotone = "(false, false)",
    output_type = "Interval",
    is_infix_op = true,
    sqlname = "*",
    propagates_nulls = true
)]
fn mul_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_interval()
        .checked_mul(b.unwrap_float64())
        .ok_or_else(|| EvalError::IntervalOutOfRange(format!("{a} * {b}").into()))
        .map(Datum::from)
}

#[sqlfunc(
    is_monotone = "(true, false)",
    output_type = "i16",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_int16();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        a.unwrap_int16()
            .checked_div(b)
            .map(Datum::from)
            .ok_or_else(|| EvalError::Int16OutOfRange(format!("{a} / {b}").into()))
    }
}

#[sqlfunc(
    is_monotone = "(true, false)",
    output_type = "i32",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_int32();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        a.unwrap_int32()
            .checked_div(b)
            .map(Datum::from)
            .ok_or_else(|| EvalError::Int32OutOfRange(format!("{a} / {b}").into()))
    }
}

#[sqlfunc(
    is_monotone = "(true, false)",
    output_type = "i64",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_int64();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        a.unwrap_int64()
            .checked_div(b)
            .map(Datum::from)
            .ok_or_else(|| EvalError::Int64OutOfRange(format!("{a} / {b}").into()))
    }
}

#[sqlfunc(
    is_monotone = "(true, false)",
    output_type = "u16",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_uint16<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_uint16();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_uint16() / b))
    }
}

#[sqlfunc(
    is_monotone = "(true, false)",
    output_type = "u32",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_uint32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_uint32();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_uint32() / b))
    }
}

#[sqlfunc(
    is_monotone = "(true, false)",
    output_type = "u64",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_uint64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_uint64();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_uint64() / b))
    }
}

#[sqlfunc(
    is_monotone = "(true, false)",
    output_type = "f32",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_float32();
    let b = b.unwrap_float32();
    if b == 0.0f32 && !a.is_nan() {
        Err(EvalError::DivisionByZero)
    } else {
        let quotient = a / b;
        if quotient.is_infinite() && !a.is_infinite() {
            Err(EvalError::FloatOverflow)
        } else if quotient == 0.0f32 && a != 0.0f32 && !b.is_infinite() {
            Err(EvalError::FloatUnderflow)
        } else {
            Ok(Datum::from(quotient))
        }
    }
}

#[sqlfunc(
    is_monotone = "(true, false)",
    output_type = "f64",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_float64();
    let b = b.unwrap_float64();
    if b == 0.0f64 && !a.is_nan() {
        Err(EvalError::DivisionByZero)
    } else {
        let quotient = a / b;
        if quotient.is_infinite() && !a.is_infinite() {
            Err(EvalError::FloatOverflow)
        } else if quotient == 0.0f64 && a != 0.0f64 && !b.is_infinite() {
            Err(EvalError::FloatUnderflow)
        } else {
            Ok(Datum::from(quotient))
        }
    }
}

#[sqlfunc(
    is_monotone = "(true, false)",
    output_type = "Numeric",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_numeric<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut cx = numeric::cx_datum();
    let mut a = a.unwrap_numeric().0;
    let b = b.unwrap_numeric().0;

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
        Ok(Datum::from(a))
    }
}

#[sqlfunc(
    is_monotone = "(false, false)",
    output_type = "Interval",
    is_infix_op = true,
    sqlname = "/",
    propagates_nulls = true
)]
fn div_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_float64();
    if b == 0.0 {
        Err(EvalError::DivisionByZero)
    } else {
        a.unwrap_interval()
            .checked_div(b)
            .ok_or_else(|| EvalError::IntervalOutOfRange(format!("{a} / {b}").into()))
            .map(Datum::from)
    }
}

#[sqlfunc(
    output_type = "i16",
    is_infix_op = true,
    sqlname = "%",
    propagates_nulls = true
)]
fn mod_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_int16();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_int16().checked_rem(b).unwrap_or(0)))
    }
}

#[sqlfunc(
    output_type = "i32",
    is_infix_op = true,
    sqlname = "%",
    propagates_nulls = true
)]
fn mod_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_int32();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_int32().checked_rem(b).unwrap_or(0)))
    }
}

#[sqlfunc(
    output_type = "i64",
    is_infix_op = true,
    sqlname = "%",
    propagates_nulls = true
)]
fn mod_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_int64();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_int64().checked_rem(b).unwrap_or(0)))
    }
}

#[sqlfunc(
    output_type = "u16",
    is_infix_op = true,
    sqlname = "%",
    propagates_nulls = true
)]
fn mod_uint16<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_uint16();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_uint16() % b))
    }
}

#[sqlfunc(
    output_type = "u32",
    is_infix_op = true,
    sqlname = "%",
    propagates_nulls = true
)]
fn mod_uint32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_uint32();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_uint32() % b))
    }
}

#[sqlfunc(
    output_type = "u64",
    is_infix_op = true,
    sqlname = "%",
    propagates_nulls = true
)]
fn mod_uint64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_uint64();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_uint64() % b))
    }
}

#[sqlfunc(
    output_type = "f32",
    is_infix_op = true,
    sqlname = "%",
    propagates_nulls = true
)]
fn mod_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_float32();
    if b == 0.0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_float32() % b))
    }
}

#[sqlfunc(
    output_type = "f64",
    is_infix_op = true,
    sqlname = "%",
    propagates_nulls = true
)]
fn mod_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_float64();
    if b == 0.0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_float64() % b))
    }
}

#[sqlfunc(
    output_type = "Numeric",
    is_infix_op = true,
    sqlname = "%",
    propagates_nulls = true
)]
fn mod_numeric<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut a = a.unwrap_numeric();
    let b = b.unwrap_numeric();
    if b.0.is_zero() {
        return Err(EvalError::DivisionByZero);
    }
    let mut cx = numeric::cx_datum();
    // Postgres does _not_ use IEEE 754-style remainder
    cx.rem(&mut a.0, &b.0);
    numeric::munge_numeric(&mut a.0).unwrap();
    Ok(Datum::Numeric(a))
}

fn neg_interval_inner(a: Datum) -> Result<Interval, EvalError> {
    a.unwrap_interval()
        .checked_neg()
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

#[sqlfunc(output_type = "Numeric", sqlname = "log", propagates_nulls = true)]
fn log_base_numeric<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut a = a.unwrap_numeric().0;
    log_guard_numeric(&a, "log")?;
    let mut b = b.unwrap_numeric().0;
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
        Ok(Datum::from(b))
    }
}

#[sqlfunc(output_type = "f64", propagates_nulls = true)]
fn power<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_float64();
    let b = b.unwrap_float64();
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
    Ok(Datum::from(res))
}

#[sqlfunc(output_type = "uuid::Uuid", propagates_nulls = true)]
fn uuid_generate_v5<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let a = a.unwrap_uuid();
    let b = b.unwrap_str();
    let res = uuid::Uuid::new_v5(&a, b.as_bytes());
    Datum::Uuid(res)
}

#[sqlfunc(output_type = "Numeric", propagates_nulls = true)]
fn power_numeric<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut a = a.unwrap_numeric().0;
    let b = b.unwrap_numeric().0;
    if a.is_zero() {
        if b.is_zero() {
            return Ok(Datum::from(Numeric::from(1)));
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
        Ok(Datum::from(a))
    }
}

#[sqlfunc(output_type = "i32", propagates_nulls = true)]
fn get_bit<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let bytes = a.unwrap_bytes();
    let index = b.unwrap_int32();
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
    Ok(Datum::from(i32::from(i)))
}

#[sqlfunc(output_type = "i32", propagates_nulls = true)]
fn get_byte<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let bytes = a.unwrap_bytes();
    let index = b.unwrap_int32();
    let err = EvalError::IndexOutOfRange {
        provided: index,
        valid_end: i32::try_from(bytes.len()).unwrap() - 1,
    };
    let i: &u8 = bytes
        .get(usize::try_from(index).map_err(|_| err.clone())?)
        .ok_or(err)?;
    Ok(Datum::from(i32::from(*i)))
}

#[sqlfunc(
    output_type = "bool",
    sqlname = "constant_time_compare_bytes",
    propagates_nulls = true
)]
pub fn constant_time_eq_bytes<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let a_bytes = a.unwrap_bytes();
    let b_bytes = b.unwrap_bytes();
    Ok(Datum::from(bool::from(a_bytes.ct_eq(b_bytes))))
}

#[sqlfunc(
    output_type = "bool",
    sqlname = "constant_time_compare_strings",
    propagates_nulls = true
)]
pub fn constant_time_eq_string<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_str();
    let b = b.unwrap_str();
    Ok(Datum::from(bool::from(a.as_bytes().ct_eq(b.as_bytes()))))
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
#[allow(dead_code)]
fn range_contains_i32<'a>(a: Range<Datum<'a>>, b: i32) -> bool {
    a.contains_elem(&b)
}

#[sqlfunc(is_infix_op = true, sqlname = "@>", propagates_nulls = true)]
#[allow(dead_code)]
fn range_contains_i64<'a>(a: Range<Datum<'a>>, elem: i64) -> bool {
    a.contains_elem(&elem)
}

#[sqlfunc(is_infix_op = true, sqlname = "@>", propagates_nulls = true)]
#[allow(dead_code)]
fn range_contains_date<'a>(a: Range<Datum<'a>>, elem: Date) -> bool {
    a.contains_elem(&elem)
}

#[sqlfunc(is_infix_op = true, sqlname = "@>", propagates_nulls = true)]
#[allow(dead_code)]
fn range_contains_numeric<'a>(a: Range<Datum<'a>>, elem: OrderedDecimal<Numeric>) -> bool {
    a.contains_elem(&elem)
}

#[sqlfunc(is_infix_op = true, sqlname = "@>", propagates_nulls = true)]
#[allow(dead_code)]
fn range_contains_timestamp<'a>(
    a: Range<Datum<'a>>,
    elem: CheckedTimestamp<NaiveDateTime>,
) -> bool {
    a.contains_elem(&elem)
}

#[sqlfunc(is_infix_op = true, sqlname = "@>", propagates_nulls = true)]
#[allow(dead_code)]
fn range_contains_timestamp_tz<'a>(
    a: Range<Datum<'a>>,
    elem: CheckedTimestamp<DateTime<Utc>>,
) -> bool {
    a.contains_elem(&elem)
}

#[sqlfunc(is_infix_op = true, sqlname = "<@", propagates_nulls = true)]
#[allow(dead_code)]
fn range_contains_i32_rev<'a>(a: Range<Datum<'a>>, b: i32) -> bool {
    a.contains_elem(&b)
}

#[sqlfunc(is_infix_op = true, sqlname = "<@", propagates_nulls = true)]
#[allow(dead_code)]
fn range_contains_i64_rev<'a>(a: Range<Datum<'a>>, elem: i64) -> bool {
    a.contains_elem(&elem)
}

#[sqlfunc(is_infix_op = true, sqlname = "<@", propagates_nulls = true)]
#[allow(dead_code)]
fn range_contains_date_rev<'a>(a: Range<Datum<'a>>, elem: Date) -> bool {
    a.contains_elem(&elem)
}

#[sqlfunc(is_infix_op = true, sqlname = "<@", propagates_nulls = true)]
#[allow(dead_code)]
fn range_contains_numeric_rev<'a>(a: Range<Datum<'a>>, elem: OrderedDecimal<Numeric>) -> bool {
    a.contains_elem(&elem)
}

#[sqlfunc(is_infix_op = true, sqlname = "<@", propagates_nulls = true)]
#[allow(dead_code)]
fn range_contains_timestamp_rev<'a>(
    a: Range<Datum<'a>>,
    elem: CheckedTimestamp<NaiveDateTime>,
) -> bool {
    a.contains_elem(&elem)
}

#[sqlfunc(is_infix_op = true, sqlname = "<@", propagates_nulls = true)]
#[allow(dead_code)]
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
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let l = a.unwrap_range();
    let r = b.unwrap_range();
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
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let l = a.unwrap_range();
    let r = b.unwrap_range();
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
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let l = a.unwrap_range();
    let r = b.unwrap_range();
    l.difference(&r)?.into_result(temp_storage)
}

#[sqlfunc(
    output_type = "bool",
    is_infix_op = true,
    sqlname = "=",
    propagates_nulls = true,
    negate = "Some(BinaryFunc::NotEq)"
)]
fn eq<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    // SQL equality demands that if either input is null, then the result should be null. However,
    // we don't need to handle this case here; it is handled when `BinaryFunc::eval` checks
    // `propagates_nulls`.
    Datum::from(a == b)
}

#[sqlfunc(
    output_type = "bool",
    is_infix_op = true,
    sqlname = "!=",
    propagates_nulls = true,
    negate = "Some(BinaryFunc::Eq)"
)]
fn not_eq<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a != b)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "bool",
    is_infix_op = true,
    sqlname = "<",
    propagates_nulls = true,
    negate = "Some(BinaryFunc::Gte)"
)]
fn lt<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a < b)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "bool",
    is_infix_op = true,
    sqlname = "<=",
    propagates_nulls = true,
    negate = "Some(BinaryFunc::Gt)"
)]
fn lte<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a <= b)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "bool",
    is_infix_op = true,
    sqlname = ">",
    propagates_nulls = true,
    negate = "Some(BinaryFunc::Lte)"
)]
fn gt<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a > b)
}

#[sqlfunc(
    is_monotone = "(true, true)",
    output_type = "bool",
    is_infix_op = true,
    sqlname = ">=",
    propagates_nulls = true,
    negate = "Some(BinaryFunc::Lt)"
)]
fn gte<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a >= b)
}

fn to_char_timestamplike<'a, T>(ts: &T, format: &str, temp_storage: &'a RowArena) -> Datum<'a>
where
    T: TimestampLike,
{
    let fmt = DateTimeFormat::compile(format);
    Datum::String(temp_storage.push_string(fmt.render(ts)))
}

#[sqlfunc(output_type = "String", sqlname = "tocharts", propagates_nulls = true)]
#[allow(dead_code)]
fn to_char_timestamp_format<'a>(a: Datum<'a>, format: &str) -> String {
    let ts = a.unwrap_timestamp();
    let fmt = DateTimeFormat::compile(format);
    fmt.render(&*ts)
}

#[sqlfunc(
    output_type = "String",
    sqlname = "tochartstz",
    propagates_nulls = true
)]
#[allow(dead_code)]
fn to_char_timestamp_tz_format<'a>(a: Datum<'a>, format: &str) -> String {
    let ts = a.unwrap_timestamptz();
    let fmt = DateTimeFormat::compile(format);
    fmt.render(&*ts)
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
                usize::cast_from(i.unsigned_abs())
            } else {
                // index backwards from the end
                let i = usize::cast_from(i.unsigned_abs());
                (list.iter().count()).wrapping_sub(i)
            };
            match list.iter().nth(i) {
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
                        usize::cast_from(i.unsigned_abs())
                    } else {
                        // index backwards from the end
                        let i = usize::cast_from(i.unsigned_abs());
                        (list.iter().count()).wrapping_sub(i)
                    };
                    match list.iter().nth(i) {
                        Some(e) => e,
                        None => return Datum::Null,
                    }
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

#[sqlfunc(
    output_type = "bool",
    is_infix_op = true,
    sqlname = "?",
    propagates_nulls = true
)]
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

#[sqlfunc(
    output_type = "bool",
    is_infix_op = true,
    sqlname = "?",
    propagates_nulls = true
)]
fn map_contains_key<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let map = a.unwrap_map();
    let k = b.unwrap_str(); // Map keys are always text.
    map.iter().any(|(k2, _v)| k == k2).into()
}

#[sqlfunc(
    output_type = "bool",
    is_infix_op = true,
    sqlname = "?&",
    propagates_nulls = true
)]
fn map_contains_all_keys<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let map = a.unwrap_map();
    let keys = b.unwrap_array();

    keys.elements()
        .iter()
        .all(|key| !key.is_null() && map.iter().any(|(k, _v)| k == key.unwrap_str()))
        .into()
}

#[sqlfunc(
    output_type = "bool",
    is_infix_op = true,
    sqlname = "?|",
    propagates_nulls = true
)]
fn map_contains_any_keys<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let map = a.unwrap_map();
    let keys = b.unwrap_array();

    keys.elements()
        .iter()
        .any(|key| !key.is_null() && map.iter().any(|(k, _v)| k == key.unwrap_str()))
        .into()
}

#[sqlfunc(
    output_type = "bool",
    is_infix_op = true,
    sqlname = "@>",
    propagates_nulls = true
)]
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

#[sqlfunc(
    output_type_expr = "input_type_a.scalar_type.unwrap_map_value_type().clone().nullable(true)",
    is_infix_op = true,
    sqlname = "->",
    propagates_nulls = true,
    introduces_nulls = true
)]
fn map_get_value<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let target_key = b.unwrap_str();
    match a.unwrap_map().iter().find(|(key, _v)| target_key == *key) {
        Some((_k, v)) => v,
        None => Datum::Null,
    }
}

#[sqlfunc(
    output_type = "bool",
    is_infix_op = true,
    sqlname = "@>",
    propagates_nulls = true,
    introduces_nulls = false
)]
fn list_contains_list<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let a = a.unwrap_list();
    let b = b.unwrap_list();

    // NULL is never equal to NULL. If NULL is an element of b, b cannot be contained in a, even if a contains NULL.
    if b.iter().contains(&Datum::Null) {
        Datum::False
    } else {
        b.iter()
            .all(|item_b| a.iter().any(|item_a| item_a == item_b))
            .into()
    }
}

#[sqlfunc(
    output_type = "bool",
    is_infix_op = true,
    sqlname = "<@",
    propagates_nulls = true,
    introduces_nulls = false
)]
#[allow(dead_code)]
fn list_contains_list_rev<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    list_contains_list(b, a)
}

// TODO(jamii) nested loops are possibly not the fastest way to do this
#[sqlfunc(
    output_type = "bool",
    is_infix_op = true,
    sqlname = "@>",
    propagates_nulls = true
)]
fn jsonb_contains_jsonb<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
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
    contains(a, b, true).into()
}

#[sqlfunc(
    output_type_expr = "SqlScalarType::Jsonb.nullable(true)",
    is_infix_op = true,
    sqlname = "||",
    propagates_nulls = true,
    introduces_nulls = true
)]
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
            let elems = list_a.iter().chain(Some(b));
            temp_storage.make_datum(|packer| packer.push_list(elems))
        }
        (a, Datum::List(list_b)) => {
            let elems = Some(a).into_iter().chain(list_b.iter());
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
fn jsonb_delete_int64<'a>(a: Datum<'a>, b: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let i = b.unwrap_int64();
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

fn date_part_interval<'a, D>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError>
where
    D: DecimalLike + Into<Datum<'static>>,
{
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => Ok(date_part_interval_inner::<D>(units, b.unwrap_interval())?.into()),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(
    output_type = "Numeric",
    sqlname = "extractiv",
    propagates_nulls = true,
    introduces_nulls = false
)]
#[allow(dead_code)]
fn date_part_interval_numeric<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => Ok(date_part_interval_inner::<Numeric>(units, b.unwrap_interval())?.into()),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(
    output_type = "f64",
    sqlname = "date_partiv",
    propagates_nulls = true,
    introduces_nulls = false
)]
#[allow(dead_code)]
fn date_part_interval_f64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => Ok(date_part_interval_inner::<f64>(units, b.unwrap_interval())?.into()),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

fn date_part_time<'a, D>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError>
where
    D: DecimalLike + Into<Datum<'a>>,
{
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => Ok(date_part_time_inner::<D>(units, b.unwrap_time())?.into()),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(
    output_type = "Numeric",
    sqlname = "extractt",
    propagates_nulls = true,
    introduces_nulls = false
)]
#[allow(dead_code)]
fn date_part_time_numeric<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => Ok(date_part_time_inner::<Numeric>(units, b.unwrap_time())?.into()),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(
    output_type = "f64",
    sqlname = "date_partt",
    propagates_nulls = true,
    introduces_nulls = false
)]
#[allow(dead_code)]
fn date_part_time_f64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => Ok(date_part_time_inner::<f64>(units, b.unwrap_time())?.into()),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

fn date_part_timestamp<'a, T, D>(a: Datum<'a>, ts: &T) -> Result<Datum<'a>, EvalError>
where
    T: TimestampLike,
    D: DecimalLike + Into<Datum<'a>>,
{
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => Ok(date_part_timestamp_inner::<_, D>(units, ts)?.into()),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(
    output_type = "Numeric",
    sqlname = "extractts",
    propagates_nulls = true
)]
#[allow(dead_code)]
fn date_part_timestamp_timestamp_numeric<'a>(
    units: &str,
    ts: CheckedTimestamp<NaiveDateTime>,
) -> Result<Datum<'a>, EvalError> {
    match units.parse() {
        Ok(units) => Ok(date_part_timestamp_inner::<_, Numeric>(units, &*ts)?.into()),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(
    output_type = "Numeric",
    sqlname = "extracttstz",
    propagates_nulls = true
)]
#[allow(dead_code)]
fn date_part_timestamp_timestamp_tz_numeric<'a>(
    units: &str,
    ts: CheckedTimestamp<DateTime<Utc>>,
) -> Result<Datum<'a>, EvalError> {
    match units.parse() {
        Ok(units) => Ok(date_part_timestamp_inner::<_, Numeric>(units, &*ts)?.into()),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(sqlname = "date_partts", propagates_nulls = true)]
#[allow(dead_code)]
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
#[allow(dead_code)]
fn date_part_timestamp_timestamp_tz_f64(
    units: &str,
    ts: CheckedTimestamp<DateTime<Utc>>,
) -> Result<f64, EvalError> {
    match units.parse() {
        Ok(units) => date_part_timestamp_inner(units, &*ts),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(output_type = "Numeric", sqlname = "extractd", propagates_nulls = true)]
fn extract_date_units<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => Ok(extract_date_inner(units, b.unwrap_date().into())?.into()),
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
#[allow(dead_code)]
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
#[allow(dead_code)]
fn date_bin_timestamp_tz<'a>(
    stride: Interval,
    source: CheckedTimestamp<DateTime<Utc>>,
) -> Result<Datum<'a>, EvalError> {
    let origin = CheckedTimestamp::from_timestamplike(DateTime::from_timestamp(0, 0).unwrap())
        .expect("must fit");
    date_bin(stride, source, origin)
}

fn date_trunc<'a, T>(a: Datum<'a>, ts: &T) -> Result<Datum<'a>, EvalError>
where
    T: TimestampLike,
{
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => Ok(date_trunc_inner(units, ts)?.try_into()?),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(sqlname = "date_truncts", propagates_nulls = true)]
#[allow(dead_code)]
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
#[allow(dead_code)]
fn date_trunc_units_timestamp_tz(
    units: &str,
    ts: CheckedTimestamp<DateTime<Utc>>,
) -> Result<CheckedTimestamp<DateTime<Utc>>, EvalError> {
    match units.parse() {
        Ok(units) => Ok(date_trunc_inner(units, &*ts)?.try_into()?),
        Err(_) => Err(EvalError::UnknownUnits(units.into())),
    }
}

#[sqlfunc(
    output_type = "Interval",
    sqlname = "date_trunciv",
    propagates_nulls = true
)]
fn date_trunc_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut interval = b.unwrap_interval();
    let units = a.unwrap_str();
    let dtf = units
        .parse()
        .map_err(|_| EvalError::UnknownUnits(units.into()))?;

    interval
        .truncate_low_fields(dtf, Some(0), RoundBehavior::Truncate)
        .expect(
            "truncate_low_fields should not fail with max_precision 0 and RoundBehavior::Truncate",
        );
    Ok(interval.into())
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
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let tz_str = a.unwrap_str();
    let tz = match Tz::from_str_insensitive(tz_str) {
        Ok(tz) => tz,
        Err(_) => return Err(EvalError::InvalidIanaTimezoneId(tz_str.into())),
    };
    let offset = tz.offset_from_utc_datetime(&b.unwrap_timestamptz().naive_utc());
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
fn mz_acl_item_contains_privilege<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mz_acl_item = a.unwrap_mz_acl_item();
    let privileges = b.unwrap_str();
    let acl_mode = AclMode::parse_multiple_privileges(privileges)
        .map_err(|e: anyhow::Error| EvalError::InvalidPrivileges(e.to_string().into()))?;
    let contains = !mz_acl_item.acl_mode.intersection(acl_mode).is_empty();
    Ok(contains.into())
}

#[sqlfunc(
    output_type = "mz_repr::ArrayRustType<String>",
    propagates_nulls = true
)]
// transliterated from postgres/src/backend/utils/adt/misc.c
fn parse_ident<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    fn is_ident_start(c: char) -> bool {
        matches!(c, 'A'..='Z' | 'a'..='z' | '_' | '\u{80}'..=char::MAX)
    }

    fn is_ident_cont(c: char) -> bool {
        matches!(c, '0'..='9' | '$') || is_ident_start(c)
    }

    let ident = a.unwrap_str();
    let strict = b.unwrap_bool();

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

#[sqlfunc(output_type = "String", propagates_nulls = true)]
fn pretty_sql<'a>(
    sql: Datum<'a>,
    width: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let sql = sql.unwrap_str();
    let width = width.unwrap_int32();
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
    Ok(Datum::String(pretty))
}

#[sqlfunc(output_type = "bool", propagates_nulls = true)]
fn starts_with<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let a = a.unwrap_str();
    let b = b.unwrap_str();
    Datum::from(a.starts_with(b))
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub enum BinaryFunc {
    AddInt16(AddInt16),
    AddInt32(AddInt32),
    AddInt64(AddInt64),
    AddUInt16(AddUint16),
    AddUInt32(AddUint32),
    AddUInt64(AddUint64),
    AddFloat32(AddFloat32),
    AddFloat64(AddFloat64),
    AddInterval,
    AddTimestampInterval,
    AddTimestampTzInterval,
    AddDateInterval,
    AddDateTime,
    AddTimeInterval,
    AddNumeric,
    AgeTimestamp,
    AgeTimestampTz,
    BitAndInt16,
    BitAndInt32,
    BitAndInt64,
    BitAndUInt16,
    BitAndUInt32,
    BitAndUInt64,
    BitOrInt16,
    BitOrInt32,
    BitOrInt64,
    BitOrUInt16,
    BitOrUInt32,
    BitOrUInt64,
    BitXorInt16,
    BitXorInt32,
    BitXorInt64,
    BitXorUInt16,
    BitXorUInt32,
    BitXorUInt64,
    BitShiftLeftInt16,
    BitShiftLeftInt32,
    BitShiftLeftInt64,
    BitShiftLeftUInt16,
    BitShiftLeftUInt32,
    BitShiftLeftUInt64,
    BitShiftRightInt16,
    BitShiftRightInt32,
    BitShiftRightInt64,
    BitShiftRightUInt16,
    BitShiftRightUInt32,
    BitShiftRightUInt64,
    SubInt16,
    SubInt32,
    SubInt64,
    SubUInt16,
    SubUInt32,
    SubUInt64,
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
    SubNumeric,
    MulInt16,
    MulInt32,
    MulInt64,
    MulUInt16,
    MulUInt32,
    MulUInt64,
    MulFloat32,
    MulFloat64,
    MulNumeric,
    MulInterval,
    DivInt16,
    DivInt32,
    DivInt64,
    DivUInt16,
    DivUInt32,
    DivUInt64,
    DivFloat32,
    DivFloat64,
    DivNumeric,
    DivInterval,
    ModInt16,
    ModInt32,
    ModInt64,
    ModUInt16,
    ModUInt32,
    ModUInt64,
    ModFloat32,
    ModFloat64,
    ModNumeric,
    RoundNumeric,
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    LikeEscape,
    IsLikeMatch { case_insensitive: bool },
    IsRegexpMatch { case_insensitive: bool },
    ToCharTimestamp,
    ToCharTimestampTz,
    DateBinTimestamp,
    DateBinTimestampTz,
    ExtractInterval,
    ExtractTime,
    ExtractTimestamp,
    ExtractTimestampTz,
    ExtractDate,
    DatePartInterval,
    DatePartTime,
    DatePartTimestamp,
    DatePartTimestampTz,
    DateTruncTimestamp,
    DateTruncTimestampTz,
    DateTruncInterval,
    TimezoneTimestamp,
    TimezoneTimestampTz,
    TimezoneIntervalTimestamp,
    TimezoneIntervalTimestampTz,
    TimezoneIntervalTime,
    TimezoneOffset,
    TextConcat,
    JsonbGetInt64,
    JsonbGetInt64Stringify,
    JsonbGetString,
    JsonbGetStringStringify,
    JsonbGetPath,
    JsonbGetPathStringify,
    JsonbContainsString,
    JsonbConcat,
    JsonbContainsJsonb,
    JsonbDeleteInt64,
    JsonbDeleteString,
    MapContainsKey,
    MapGetValue,
    MapContainsAllKeys,
    MapContainsAnyKeys,
    MapContainsMap,
    ConvertFrom,
    Left,
    Position,
    Right,
    RepeatString,
    Normalize,
    Trim,
    TrimLeading,
    TrimTrailing,
    EncodedBytesCharLength,
    ListLengthMax { max_layer: usize },
    ArrayContains,
    ArrayContainsArray { rev: bool },
    ArrayLength,
    ArrayLower,
    ArrayRemove,
    ArrayUpper,
    ArrayArrayConcat,
    ListListConcat,
    ListElementConcat,
    ElementListConcat,
    ListRemove,
    ListContainsList { rev: bool },
    DigestString,
    DigestBytes,
    MzRenderTypmod,
    Encode,
    Decode,
    LogNumeric,
    Power,
    PowerNumeric,
    GetBit,
    GetByte,
    ConstantTimeEqBytes,
    ConstantTimeEqString,
    RangeContainsElem { elem_type: SqlScalarType, rev: bool },
    RangeContainsRange { rev: bool },
    RangeOverlaps,
    RangeAfter,
    RangeBefore,
    RangeOverleft,
    RangeOverright,
    RangeAdjacent,
    RangeUnion,
    RangeIntersection,
    RangeDifference,
    UuidGenerateV5,
    MzAclItemContainsPrivilege,
    ParseIdent,
    PrettySql,
    RegexpReplace { regex: Regex, limit: usize },
    StartsWith,
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
            BinaryFunc::AddUInt16(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::AddUInt32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::AddUInt64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::AddFloat32(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            BinaryFunc::AddFloat64(s) => return s.eval(datums, temp_storage, a_expr, b_expr),
            _ => { /* fall through */ }
        }

        let a = a_expr.eval(datums, temp_storage)?;
        let b = b_expr.eval(datums, temp_storage)?;
        if self.propagates_nulls() && (a.is_null() || b.is_null()) {
            return Ok(Datum::Null);
        }
        match self {
            BinaryFunc::AddInt16(_)
            | BinaryFunc::AddInt32(_)
            | BinaryFunc::AddInt64(_)
            | BinaryFunc::AddUInt16(_)
            | BinaryFunc::AddUInt32(_)
            | BinaryFunc::AddUInt64(_)
            | BinaryFunc::AddFloat32(_)
            | BinaryFunc::AddFloat64(_) => unreachable!(),
            BinaryFunc::AddTimestampInterval => {
                add_timestamplike_interval(a.unwrap_timestamp(), b.unwrap_interval())
            }
            BinaryFunc::AddTimestampTzInterval => {
                add_timestamplike_interval(a.unwrap_timestamptz(), b.unwrap_interval())
            }
            BinaryFunc::AddDateTime => add_date_time(a, b),
            BinaryFunc::AddDateInterval => add_date_interval(a, b),
            BinaryFunc::AddTimeInterval => Ok(add_time_interval(a, b)),
            BinaryFunc::AddNumeric => add_numeric(a, b),
            BinaryFunc::AddInterval => add_interval(a, b),
            BinaryFunc::AgeTimestamp => age_timestamp(a, b),
            BinaryFunc::AgeTimestampTz => age_timestamptz(a, b),
            BinaryFunc::BitAndInt16 => Ok(bit_and_int16(a, b)),
            BinaryFunc::BitAndInt32 => Ok(bit_and_int32(a, b)),
            BinaryFunc::BitAndInt64 => Ok(bit_and_int64(a, b)),
            BinaryFunc::BitAndUInt16 => Ok(bit_and_uint16(a, b)),
            BinaryFunc::BitAndUInt32 => Ok(bit_and_uint32(a, b)),
            BinaryFunc::BitAndUInt64 => Ok(bit_and_uint64(a, b)),
            BinaryFunc::BitOrInt16 => Ok(bit_or_int16(a, b)),
            BinaryFunc::BitOrInt32 => Ok(bit_or_int32(a, b)),
            BinaryFunc::BitOrInt64 => Ok(bit_or_int64(a, b)),
            BinaryFunc::BitOrUInt16 => Ok(bit_or_uint16(a, b)),
            BinaryFunc::BitOrUInt32 => Ok(bit_or_uint32(a, b)),
            BinaryFunc::BitOrUInt64 => Ok(bit_or_uint64(a, b)),
            BinaryFunc::BitXorInt16 => Ok(bit_xor_int16(a, b)),
            BinaryFunc::BitXorInt32 => Ok(bit_xor_int32(a, b)),
            BinaryFunc::BitXorInt64 => Ok(bit_xor_int64(a, b)),
            BinaryFunc::BitXorUInt16 => Ok(bit_xor_uint16(a, b)),
            BinaryFunc::BitXorUInt32 => Ok(bit_xor_uint32(a, b)),
            BinaryFunc::BitXorUInt64 => Ok(bit_xor_uint64(a, b)),
            BinaryFunc::BitShiftLeftInt16 => Ok(bit_shift_left_int16(a, b)),
            BinaryFunc::BitShiftLeftInt32 => Ok(bit_shift_left_int32(a, b)),
            BinaryFunc::BitShiftLeftInt64 => Ok(bit_shift_left_int64(a, b)),
            BinaryFunc::BitShiftLeftUInt16 => Ok(bit_shift_left_uint16(a, b)),
            BinaryFunc::BitShiftLeftUInt32 => Ok(bit_shift_left_uint32(a, b)),
            BinaryFunc::BitShiftLeftUInt64 => Ok(bit_shift_left_uint64(a, b)),
            BinaryFunc::BitShiftRightInt16 => Ok(bit_shift_right_int16(a, b)),
            BinaryFunc::BitShiftRightInt32 => Ok(bit_shift_right_int32(a, b)),
            BinaryFunc::BitShiftRightInt64 => Ok(bit_shift_right_int64(a, b)),
            BinaryFunc::BitShiftRightUInt16 => Ok(bit_shift_right_uint16(a, b)),
            BinaryFunc::BitShiftRightUInt32 => Ok(bit_shift_right_uint32(a, b)),
            BinaryFunc::BitShiftRightUInt64 => Ok(bit_shift_right_uint64(a, b)),
            BinaryFunc::SubInt16 => sub_int16(a, b),
            BinaryFunc::SubInt32 => sub_int32(a, b),
            BinaryFunc::SubInt64 => sub_int64(a, b),
            BinaryFunc::SubUInt16 => sub_uint16(a, b),
            BinaryFunc::SubUInt32 => sub_uint32(a, b),
            BinaryFunc::SubUInt64 => sub_uint64(a, b),
            BinaryFunc::SubFloat32 => sub_float32(a, b),
            BinaryFunc::SubFloat64 => sub_float64(a, b),
            BinaryFunc::SubTimestamp => Ok(sub_timestamp(a, b)),
            BinaryFunc::SubTimestampTz => Ok(sub_timestamptz(a, b)),
            BinaryFunc::SubTimestampInterval => sub_timestamplike_interval(a.unwrap_timestamp(), b),
            BinaryFunc::SubTimestampTzInterval => {
                sub_timestamplike_interval(a.unwrap_timestamptz(), b)
            }
            BinaryFunc::SubInterval => sub_interval(a, b),
            BinaryFunc::SubDate => Ok(sub_date(a, b)),
            BinaryFunc::SubDateInterval => sub_date_interval(a, b),
            BinaryFunc::SubTime => Ok(sub_time(a, b)),
            BinaryFunc::SubTimeInterval => Ok(sub_time_interval(a, b)),
            BinaryFunc::SubNumeric => sub_numeric(a, b),
            BinaryFunc::MulInt16 => mul_int16(a, b),
            BinaryFunc::MulInt32 => mul_int32(a, b),
            BinaryFunc::MulInt64 => mul_int64(a, b),
            BinaryFunc::MulUInt16 => mul_uint16(a, b),
            BinaryFunc::MulUInt32 => mul_uint32(a, b),
            BinaryFunc::MulUInt64 => mul_uint64(a, b),
            BinaryFunc::MulFloat32 => mul_float32(a, b),
            BinaryFunc::MulFloat64 => mul_float64(a, b),
            BinaryFunc::MulNumeric => mul_numeric(a, b),
            BinaryFunc::MulInterval => mul_interval(a, b),
            BinaryFunc::DivInt16 => div_int16(a, b),
            BinaryFunc::DivInt32 => div_int32(a, b),
            BinaryFunc::DivInt64 => div_int64(a, b),
            BinaryFunc::DivUInt16 => div_uint16(a, b),
            BinaryFunc::DivUInt32 => div_uint32(a, b),
            BinaryFunc::DivUInt64 => div_uint64(a, b),
            BinaryFunc::DivFloat32 => div_float32(a, b),
            BinaryFunc::DivFloat64 => div_float64(a, b),
            BinaryFunc::DivNumeric => div_numeric(a, b),
            BinaryFunc::DivInterval => div_interval(a, b),
            BinaryFunc::ModInt16 => mod_int16(a, b),
            BinaryFunc::ModInt32 => mod_int32(a, b),
            BinaryFunc::ModInt64 => mod_int64(a, b),
            BinaryFunc::ModUInt16 => mod_uint16(a, b),
            BinaryFunc::ModUInt32 => mod_uint32(a, b),
            BinaryFunc::ModUInt64 => mod_uint64(a, b),
            BinaryFunc::ModFloat32 => mod_float32(a, b),
            BinaryFunc::ModFloat64 => mod_float64(a, b),
            BinaryFunc::ModNumeric => mod_numeric(a, b),
            BinaryFunc::Eq => Ok(eq(a, b)),
            BinaryFunc::NotEq => Ok(not_eq(a, b)),
            BinaryFunc::Lt => Ok(lt(a, b)),
            BinaryFunc::Lte => Ok(lte(a, b)),
            BinaryFunc::Gt => Ok(gt(a, b)),
            BinaryFunc::Gte => Ok(gte(a, b)),
            BinaryFunc::LikeEscape => like_escape(a, b, temp_storage),
            BinaryFunc::IsLikeMatch { case_insensitive } => {
                is_like_match_dynamic(a, b, *case_insensitive)
            }
            BinaryFunc::IsRegexpMatch { case_insensitive } => {
                is_regexp_match_dynamic(a, b, *case_insensitive)
            }
            BinaryFunc::ToCharTimestamp => Ok(to_char_timestamplike(
                a.unwrap_timestamp().deref(),
                b.unwrap_str(),
                temp_storage,
            )),
            BinaryFunc::ToCharTimestampTz => Ok(to_char_timestamplike(
                a.unwrap_timestamptz().deref(),
                b.unwrap_str(),
                temp_storage,
            )),
            BinaryFunc::DateBinTimestamp => date_bin(
                a.unwrap_interval(),
                b.unwrap_timestamp(),
                CheckedTimestamp::from_timestamplike(
                    DateTime::from_timestamp(0, 0).unwrap().naive_utc(),
                )
                .expect("must fit"),
            ),
            BinaryFunc::DateBinTimestampTz => date_bin(
                a.unwrap_interval(),
                b.unwrap_timestamptz(),
                CheckedTimestamp::from_timestamplike(DateTime::from_timestamp(0, 0).unwrap())
                    .expect("must fit"),
            ),
            BinaryFunc::ExtractInterval => date_part_interval::<Numeric>(a, b),
            BinaryFunc::ExtractTime => date_part_time::<Numeric>(a, b),
            BinaryFunc::ExtractTimestamp => {
                date_part_timestamp::<_, Numeric>(a, b.unwrap_timestamp().deref())
            }
            BinaryFunc::ExtractTimestampTz => {
                date_part_timestamp::<_, Numeric>(a, b.unwrap_timestamptz().deref())
            }
            BinaryFunc::ExtractDate => extract_date_units(a, b),
            BinaryFunc::DatePartInterval => date_part_interval::<f64>(a, b),
            BinaryFunc::DatePartTime => date_part_time::<f64>(a, b),
            BinaryFunc::DatePartTimestamp => {
                date_part_timestamp::<_, f64>(a, b.unwrap_timestamp().deref())
            }
            BinaryFunc::DatePartTimestampTz => {
                date_part_timestamp::<_, f64>(a, b.unwrap_timestamptz().deref())
            }
            BinaryFunc::DateTruncTimestamp => date_trunc(a, b.unwrap_timestamp().deref()),
            BinaryFunc::DateTruncInterval => date_trunc_interval(a, b),
            BinaryFunc::DateTruncTimestampTz => date_trunc(a, b.unwrap_timestamptz().deref()),
            BinaryFunc::TimezoneTimestamp => parse_timezone(a.unwrap_str(), TimezoneSpec::Posix)
                .and_then(|tz| timezone_timestamp(tz, b.unwrap_timestamp().into()).map(Into::into)),
            BinaryFunc::TimezoneTimestampTz => parse_timezone(a.unwrap_str(), TimezoneSpec::Posix)
                .and_then(|tz| {
                    Ok(timezone_timestamptz(tz, b.unwrap_timestamptz().into())?.try_into()?)
                }),
            BinaryFunc::TimezoneIntervalTimestamp => timezone_interval_timestamp(a, b),
            BinaryFunc::TimezoneIntervalTimestampTz => timezone_interval_timestamptz(a, b),
            BinaryFunc::TimezoneIntervalTime => timezone_interval_time(a, b),
            BinaryFunc::TimezoneOffset => timezone_offset(a, b, temp_storage),
            BinaryFunc::TextConcat => Ok(text_concat_binary(a, b, temp_storage)),
            BinaryFunc::JsonbGetInt64 => Ok(jsonb_get_int64(a, b, temp_storage, false)),
            BinaryFunc::JsonbGetInt64Stringify => Ok(jsonb_get_int64(a, b, temp_storage, true)),
            BinaryFunc::JsonbGetString => Ok(jsonb_get_string(a, b, temp_storage, false)),
            BinaryFunc::JsonbGetStringStringify => Ok(jsonb_get_string(a, b, temp_storage, true)),
            BinaryFunc::JsonbGetPath => Ok(jsonb_get_path(a, b, temp_storage, false)),
            BinaryFunc::JsonbGetPathStringify => Ok(jsonb_get_path(a, b, temp_storage, true)),
            BinaryFunc::JsonbContainsString => Ok(jsonb_contains_string(a, b)),
            BinaryFunc::JsonbConcat => Ok(jsonb_concat(a, b, temp_storage)),
            BinaryFunc::JsonbContainsJsonb => Ok(jsonb_contains_jsonb(a, b)),
            BinaryFunc::JsonbDeleteInt64 => Ok(jsonb_delete_int64(a, b, temp_storage)),
            BinaryFunc::JsonbDeleteString => Ok(jsonb_delete_string(a, b, temp_storage)),
            BinaryFunc::MapContainsKey => Ok(map_contains_key(a, b)),
            BinaryFunc::MapGetValue => Ok(map_get_value(a, b)),
            BinaryFunc::MapContainsAllKeys => Ok(map_contains_all_keys(a, b)),
            BinaryFunc::MapContainsAnyKeys => Ok(map_contains_any_keys(a, b)),
            BinaryFunc::MapContainsMap => Ok(map_contains_map(a, b)),
            BinaryFunc::RoundNumeric => round_numeric_binary(a, b),
            BinaryFunc::ConvertFrom => convert_from(a, b),
            BinaryFunc::Encode => encode(a, b, temp_storage),
            BinaryFunc::Decode => decode(a, b, temp_storage),
            BinaryFunc::Left => left(a, b),
            BinaryFunc::Position => position(a, b),
            BinaryFunc::Right => right(a, b),
            BinaryFunc::Trim => Ok(trim(a, b)),
            BinaryFunc::TrimLeading => Ok(trim_leading(a, b)),
            BinaryFunc::TrimTrailing => Ok(trim_trailing(a, b)),
            BinaryFunc::EncodedBytesCharLength => encoded_bytes_char_length(a, b),
            BinaryFunc::ListLengthMax { max_layer } => list_length_max(a, b, *max_layer),
            BinaryFunc::ArrayLength => array_length(a, b),
            BinaryFunc::ArrayContains => Ok(array_contains(a, b)),
            BinaryFunc::ArrayContainsArray { rev: false } => Ok(array_contains_array(a, b)),
            BinaryFunc::ArrayContainsArray { rev: true } => Ok(array_contains_array(b, a)),
            BinaryFunc::ArrayLower => Ok(array_lower(a, b)),
            BinaryFunc::ArrayRemove => array_remove(a, b, temp_storage),
            BinaryFunc::ArrayUpper => array_upper(a, b),
            BinaryFunc::ArrayArrayConcat => array_array_concat(a, b, temp_storage),
            BinaryFunc::ListListConcat => Ok(list_list_concat(a, b, temp_storage)),
            BinaryFunc::ListElementConcat => Ok(list_element_concat(a, b, temp_storage)),
            BinaryFunc::ElementListConcat => Ok(element_list_concat(a, b, temp_storage)),
            BinaryFunc::ListRemove => Ok(list_remove(a, b, temp_storage)),
            BinaryFunc::ListContainsList { rev: false } => Ok(list_contains_list(a, b)),
            BinaryFunc::ListContainsList { rev: true } => Ok(list_contains_list(b, a)),
            BinaryFunc::DigestString => digest_string(a, b, temp_storage),
            BinaryFunc::DigestBytes => digest_bytes(a, b, temp_storage),
            BinaryFunc::MzRenderTypmod => mz_render_typmod(a, b, temp_storage),
            BinaryFunc::LogNumeric => log_base_numeric(a, b),
            BinaryFunc::Power => power(a, b),
            BinaryFunc::PowerNumeric => power_numeric(a, b),
            BinaryFunc::RepeatString => repeat_string(a, b, temp_storage),
            BinaryFunc::Normalize => normalize_with_form(a, b, temp_storage),
            BinaryFunc::GetBit => get_bit(a, b),
            BinaryFunc::GetByte => get_byte(a, b),
            BinaryFunc::ConstantTimeEqBytes => constant_time_eq_bytes(a, b),
            BinaryFunc::ConstantTimeEqString => constant_time_eq_string(a, b),
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
            BinaryFunc::RangeOverlaps => Ok(range_overlaps(a, b)),
            BinaryFunc::RangeAfter => Ok(range_after(a, b)),
            BinaryFunc::RangeBefore => Ok(range_before(a, b)),
            BinaryFunc::RangeOverleft => Ok(range_overleft(a, b)),
            BinaryFunc::RangeOverright => Ok(range_overright(a, b)),
            BinaryFunc::RangeAdjacent => Ok(range_adjacent(a, b)),
            BinaryFunc::RangeUnion => range_union(a, b, temp_storage),
            BinaryFunc::RangeIntersection => range_intersection(a, b, temp_storage),
            BinaryFunc::RangeDifference => range_difference(a, b, temp_storage),
            BinaryFunc::UuidGenerateV5 => Ok(uuid_generate_v5(a, b)),
            BinaryFunc::MzAclItemContainsPrivilege => mz_acl_item_contains_privilege(a, b),
            BinaryFunc::ParseIdent => parse_ident(a, b, temp_storage),
            BinaryFunc::PrettySql => pretty_sql(a, b, temp_storage),
            BinaryFunc::RegexpReplace { regex, limit } => {
                regexp_replace_static(a, b, regex, *limit, temp_storage)
            }
            BinaryFunc::StartsWith => Ok(starts_with(a, b)),
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
            AddUInt16(s) => s.output_type(input1_type, input2_type),
            AddUInt32(s) => s.output_type(input1_type, input2_type),
            AddUInt64(s) => s.output_type(input1_type, input2_type),
            AddFloat32(s) => s.output_type(input1_type, input2_type),
            AddFloat64(s) => s.output_type(input1_type, input2_type),

            Eq
            | NotEq
            | Lt
            | Lte
            | Gt
            | Gte
            | ArrayContains
            | ArrayContainsArray { .. }
            // like and regexp produce errors on invalid like-strings or regexes
            | IsLikeMatch { .. }
            | IsRegexpMatch { .. } => SqlScalarType::Bool.nullable(in_nullable),

            ToCharTimestamp | ToCharTimestampTz | ConvertFrom | Left | Right | Trim
            | TrimLeading | TrimTrailing | LikeEscape => SqlScalarType::String.nullable(in_nullable),

             SubInt16 | MulInt16 | DivInt16 | ModInt16 | BitAndInt16 | BitOrInt16
            | BitXorInt16 | BitShiftLeftInt16 | BitShiftRightInt16 => {
                SqlScalarType::Int16.nullable(in_nullable)
            }

            SubInt32
            | MulInt32
            | DivInt32
            | ModInt32
            | BitAndInt32
            | BitOrInt32
            | BitXorInt32
            | BitShiftLeftInt32
            | BitShiftRightInt32
            | EncodedBytesCharLength
            | SubDate => SqlScalarType::Int32.nullable(in_nullable),

            SubInt64 | MulInt64 | DivInt64 | ModInt64 | BitAndInt64 | BitOrInt64
            | BitXorInt64 | BitShiftLeftInt64 | BitShiftRightInt64 => {
                SqlScalarType::Int64.nullable(in_nullable)
            }

            SubUInt16 | MulUInt16 | DivUInt16 | ModUInt16 | BitAndUInt16
            | BitOrUInt16 | BitXorUInt16 | BitShiftLeftUInt16 | BitShiftRightUInt16 => {
                SqlScalarType::UInt16.nullable(in_nullable)
            }

            SubUInt32 | MulUInt32 | DivUInt32 | ModUInt32 | BitAndUInt32
            | BitOrUInt32 | BitXorUInt32 | BitShiftLeftUInt32 | BitShiftRightUInt32 => {
                SqlScalarType::UInt32.nullable(in_nullable)
            }

             SubUInt64 | MulUInt64 | DivUInt64 | ModUInt64 | BitAndUInt64
            | BitOrUInt64 | BitXorUInt64 | BitShiftLeftUInt64 | BitShiftRightUInt64 => {
                SqlScalarType::UInt64.nullable(in_nullable)
            }

             SubFloat32 | MulFloat32 | DivFloat32 | ModFloat32 => {
                SqlScalarType::Float32.nullable(in_nullable)
            }

             SubFloat64 | MulFloat64 | DivFloat64 | ModFloat64 => {
                SqlScalarType::Float64.nullable(in_nullable)
            }

            AddInterval | SubInterval | SubTimestamp | SubTimestampTz | MulInterval
            | DivInterval => SqlScalarType::Interval.nullable(in_nullable),

            AgeTimestamp | AgeTimestampTz => SqlScalarType::Interval.nullable(in_nullable),

            AddTimestampInterval
            | SubTimestampInterval
            | AddTimestampTzInterval
            | SubTimestampTzInterval
            | AddTimeInterval
            | SubTimeInterval => input1_type.nullable(in_nullable),

            AddDateInterval | SubDateInterval | AddDateTime | DateBinTimestamp
            | DateTruncTimestamp => SqlScalarType::Timestamp { precision: None }.nullable(in_nullable),

            DateTruncInterval => SqlScalarType::Interval.nullable(in_nullable),

            TimezoneTimestampTz | TimezoneIntervalTimestampTz => {
                SqlScalarType::Timestamp { precision: None }.nullable(in_nullable)
            }

            ExtractInterval | ExtractTime | ExtractTimestamp | ExtractTimestampTz | ExtractDate => {
                SqlScalarType::Numeric { max_scale: None }.nullable(in_nullable)
            }

            DatePartInterval | DatePartTime | DatePartTimestamp | DatePartTimestampTz => {
                SqlScalarType::Float64.nullable(in_nullable)
            }

            DateBinTimestampTz | DateTruncTimestampTz => SqlScalarType::TimestampTz { precision: None }.nullable(in_nullable),

            TimezoneTimestamp | TimezoneIntervalTimestamp => {
                SqlScalarType::TimestampTz { precision: None }.nullable(in_nullable)
            }

            TimezoneIntervalTime => SqlScalarType::Time.nullable(in_nullable),

            TimezoneOffset => SqlScalarType::Record {
                fields: [
                    ("abbrev".into(), SqlScalarType::String.nullable(false)),
                    ("base_utc_offset".into(), SqlScalarType::Interval.nullable(false)),
                    ("dst_offset".into(), SqlScalarType::Interval.nullable(false)),
                ].into(),
                custom_id: None,
            }.nullable(true),

            SubTime => SqlScalarType::Interval.nullable(in_nullable),

            MzRenderTypmod | TextConcat => SqlScalarType::String.nullable(in_nullable),

            JsonbGetInt64Stringify
            | JsonbGetStringStringify
            | JsonbGetPathStringify => SqlScalarType::String.nullable(true),

            JsonbGetInt64
            | JsonbGetString
            | JsonbGetPath
            | JsonbConcat
            | JsonbDeleteInt64
            | JsonbDeleteString => SqlScalarType::Jsonb.nullable(true),

            JsonbContainsString | JsonbContainsJsonb | MapContainsKey | MapContainsAllKeys
            | MapContainsAnyKeys | MapContainsMap => SqlScalarType::Bool.nullable(in_nullable),

            MapGetValue => input1_type
                .scalar_type
                .unwrap_map_value_type()
                .clone()
                .nullable(true),

            ArrayLength | ArrayLower | ArrayUpper => SqlScalarType::Int32.nullable(true),

            ListLengthMax { .. } => SqlScalarType::Int32.nullable(true),

            ArrayArrayConcat | ArrayRemove | ListListConcat | ListElementConcat | ListRemove => {
                input1_type.scalar_type.without_modifiers().nullable(true)
            }

            ElementListConcat => input2_type.scalar_type.without_modifiers().nullable(true),

            ListContainsList { .. } =>  SqlScalarType::Bool.nullable(in_nullable),

            DigestString | DigestBytes => SqlScalarType::Bytes.nullable(in_nullable),
            Position => SqlScalarType::Int32.nullable(in_nullable),
            Encode => SqlScalarType::String.nullable(in_nullable),
            Decode => SqlScalarType::Bytes.nullable(in_nullable),
            Power => SqlScalarType::Float64.nullable(in_nullable),
            RepeatString | Normalize => input1_type.scalar_type.nullable(in_nullable),

            AddNumeric | DivNumeric | LogNumeric | ModNumeric | MulNumeric | PowerNumeric
            | RoundNumeric | SubNumeric => {
                SqlScalarType::Numeric { max_scale: None }.nullable(in_nullable)
            }

            GetBit => SqlScalarType::Int32.nullable(in_nullable),
            GetByte => SqlScalarType::Int32.nullable(in_nullable),

            ConstantTimeEqBytes | ConstantTimeEqString => {
                SqlScalarType::Bool.nullable(in_nullable)
            },

            UuidGenerateV5 => SqlScalarType::Uuid.nullable(in_nullable),

            RangeContainsElem { .. }
            | RangeContainsRange { .. }
            | RangeOverlaps
            | RangeAfter
            | RangeBefore
            | RangeOverleft
            | RangeOverright
            | RangeAdjacent => SqlScalarType::Bool.nullable(in_nullable),

            RangeUnion | RangeIntersection | RangeDifference => {
                soft_assert_eq_or_log!(
                    input1_type.scalar_type.without_modifiers(),
                    input2_type.scalar_type.without_modifiers()
                );
                input1_type.scalar_type.without_modifiers().nullable(true)
            }

            MzAclItemContainsPrivilege => SqlScalarType::Bool.nullable(in_nullable),

            ParseIdent => SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(in_nullable),
            PrettySql => SqlScalarType::String.nullable(in_nullable),
            RegexpReplace { .. } => SqlScalarType::String.nullable(in_nullable),

            StartsWith => SqlScalarType::Bool.nullable(in_nullable),
        }
    }

    /// Whether the function output is NULL if any of its inputs are NULL.
    pub fn propagates_nulls(&self) -> bool {
        match self {
            BinaryFunc::AddInt16(s) => return s.propagates_nulls(),
            BinaryFunc::AddInt32(s) => return s.propagates_nulls(),
            BinaryFunc::AddInt64(s) => return s.propagates_nulls(),
            BinaryFunc::AddUInt16(s) => return s.propagates_nulls(),
            BinaryFunc::AddUInt32(s) => return s.propagates_nulls(),
            BinaryFunc::AddUInt64(s) => return s.propagates_nulls(),
            BinaryFunc::AddFloat32(s) => return s.propagates_nulls(),
            BinaryFunc::AddFloat64(s) => return s.propagates_nulls(),
            _ => { /* fall through */ }
        }
        // NOTE: The following is a list of the binary functions
        // that **DO NOT** propagate nulls.
        !matches!(
            self,
            BinaryFunc::ArrayArrayConcat
                | BinaryFunc::ListListConcat
                | BinaryFunc::ListElementConcat
                | BinaryFunc::ElementListConcat
                | BinaryFunc::ArrayRemove
                | BinaryFunc::ListRemove
        )
    }

    /// Whether the function might return NULL even if none of its inputs are
    /// NULL.
    ///
    /// This is presently conservative, and may indicate that a function
    /// introduces nulls even when it does not.
    pub fn introduces_nulls(&self) -> bool {
        use BinaryFunc::*;
        match self {
            AddInt16(s) => s.introduces_nulls(),
            AddInt32(s) => s.introduces_nulls(),
            AddInt64(s) => s.introduces_nulls(),
            AddUInt16(s) => s.introduces_nulls(),
            AddUInt32(s) => s.introduces_nulls(),
            AddUInt64(s) => s.introduces_nulls(),
            AddFloat32(s) => s.introduces_nulls(),
            AddFloat64(s) => s.introduces_nulls(),
            AddInterval
            | AddTimestampInterval
            | AddTimestampTzInterval
            | AddDateInterval
            | AddDateTime
            | AddTimeInterval
            | AddNumeric
            | AgeTimestamp
            | AgeTimestampTz
            | BitAndInt16
            | BitAndInt32
            | BitAndInt64
            | BitAndUInt16
            | BitAndUInt32
            | BitAndUInt64
            | BitOrInt16
            | BitOrInt32
            | BitOrInt64
            | BitOrUInt16
            | BitOrUInt32
            | BitOrUInt64
            | BitXorInt16
            | BitXorInt32
            | BitXorInt64
            | BitXorUInt16
            | BitXorUInt32
            | BitXorUInt64
            | BitShiftLeftInt16
            | BitShiftLeftInt32
            | BitShiftLeftInt64
            | BitShiftLeftUInt16
            | BitShiftLeftUInt32
            | BitShiftLeftUInt64
            | BitShiftRightInt16
            | BitShiftRightInt32
            | BitShiftRightInt64
            | BitShiftRightUInt16
            | BitShiftRightUInt32
            | BitShiftRightUInt64
            | SubInt16
            | SubInt32
            | SubInt64
            | SubUInt16
            | SubUInt32
            | SubUInt64
            | SubFloat32
            | SubFloat64
            | SubInterval
            | SubTimestamp
            | SubTimestampTz
            | SubTimestampInterval
            | SubTimestampTzInterval
            | SubDate
            | SubDateInterval
            | SubTime
            | SubTimeInterval
            | SubNumeric
            | MulInt16
            | MulInt32
            | MulInt64
            | MulUInt16
            | MulUInt32
            | MulUInt64
            | MulFloat32
            | MulFloat64
            | MulNumeric
            | MulInterval
            | DivInt16
            | DivInt32
            | DivInt64
            | DivUInt16
            | DivUInt32
            | DivUInt64
            | DivFloat32
            | DivFloat64
            | DivNumeric
            | DivInterval
            | ModInt16
            | ModInt32
            | ModInt64
            | ModUInt16
            | ModUInt32
            | ModUInt64
            | ModFloat32
            | ModFloat64
            | ModNumeric
            | RoundNumeric
            | Eq
            | NotEq
            | Lt
            | Lte
            | Gt
            | Gte
            | LikeEscape
            | IsLikeMatch { .. }
            | IsRegexpMatch { .. }
            | ToCharTimestamp
            | ToCharTimestampTz
            | ConstantTimeEqBytes
            | ConstantTimeEqString
            | DateBinTimestamp
            | DateBinTimestampTz
            | ExtractInterval
            | ExtractTime
            | ExtractTimestamp
            | ExtractTimestampTz
            | ExtractDate
            | DatePartInterval
            | DatePartTime
            | DatePartTimestamp
            | DatePartTimestampTz
            | DateTruncTimestamp
            | DateTruncTimestampTz
            | DateTruncInterval
            | TimezoneTimestamp
            | TimezoneTimestampTz
            | TimezoneIntervalTimestamp
            | TimezoneIntervalTimestampTz
            | TimezoneIntervalTime
            | TimezoneOffset
            | TextConcat
            | JsonbContainsString
            | JsonbContainsJsonb
            | MapContainsKey
            | MapContainsAllKeys
            | MapContainsAnyKeys
            | MapContainsMap
            | ConvertFrom
            | Left
            | Position
            | Right
            | RepeatString
            | Normalize
            | Trim
            | TrimLeading
            | TrimTrailing
            | EncodedBytesCharLength
            | ArrayContains
            | ArrayRemove
            | ArrayContainsArray { .. }
            | ArrayArrayConcat
            | ListListConcat
            | ListElementConcat
            | ElementListConcat
            | ListContainsList { .. }
            | ListRemove
            | DigestString
            | DigestBytes
            | MzRenderTypmod
            | Encode
            | Decode
            | LogNumeric
            | Power
            | PowerNumeric
            | GetBit
            | GetByte
            | RangeContainsElem { .. }
            | RangeContainsRange { .. }
            | RangeOverlaps
            | RangeAfter
            | RangeBefore
            | RangeOverleft
            | RangeOverright
            | RangeAdjacent
            | RangeUnion
            | RangeIntersection
            | RangeDifference
            | UuidGenerateV5
            | MzAclItemContainsPrivilege
            | ParseIdent
            | PrettySql
            | RegexpReplace { .. }
            | StartsWith => false,

            JsonbGetInt64
            | JsonbGetInt64Stringify
            | JsonbGetString
            | JsonbGetStringStringify
            | JsonbGetPath
            | JsonbGetPathStringify
            | JsonbConcat
            | JsonbDeleteInt64
            | JsonbDeleteString
            | MapGetValue
            | ListLengthMax { .. }
            | ArrayLength
            | ArrayLower
            | ArrayUpper => true,
        }
    }

    pub fn is_infix_op(&self) -> bool {
        use BinaryFunc::*;
        match self {
            AddInt16(s) => s.is_infix_op(),
            AddInt32(s) => s.is_infix_op(),
            AddInt64(s) => s.is_infix_op(),
            AddUInt16(s) => s.is_infix_op(),
            AddUInt32(s) => s.is_infix_op(),
            AddUInt64(s) => s.is_infix_op(),
            AddFloat32(s) => s.is_infix_op(),
            AddFloat64(s) => s.is_infix_op(),
            AddTimestampInterval
            | AddTimestampTzInterval
            | AddDateTime
            | AddDateInterval
            | AddTimeInterval
            | AddInterval
            | BitAndInt16
            | BitAndInt32
            | BitAndInt64
            | BitAndUInt16
            | BitAndUInt32
            | BitAndUInt64
            | BitOrInt16
            | BitOrInt32
            | BitOrInt64
            | BitOrUInt16
            | BitOrUInt32
            | BitOrUInt64
            | BitXorInt16
            | BitXorInt32
            | BitXorInt64
            | BitXorUInt16
            | BitXorUInt32
            | BitXorUInt64
            | BitShiftLeftInt16
            | BitShiftLeftInt32
            | BitShiftLeftInt64
            | BitShiftLeftUInt16
            | BitShiftLeftUInt32
            | BitShiftLeftUInt64
            | BitShiftRightInt16
            | BitShiftRightInt32
            | BitShiftRightInt64
            | BitShiftRightUInt16
            | BitShiftRightUInt32
            | BitShiftRightUInt64
            | SubInterval
            | MulInterval
            | DivInterval
            | AddNumeric
            | SubInt16
            | SubInt32
            | SubInt64
            | SubUInt16
            | SubUInt32
            | SubUInt64
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
            | SubNumeric
            | MulInt16
            | MulInt32
            | MulInt64
            | MulUInt16
            | MulUInt32
            | MulUInt64
            | MulFloat32
            | MulFloat64
            | MulNumeric
            | DivInt16
            | DivInt32
            | DivInt64
            | DivUInt16
            | DivUInt32
            | DivUInt64
            | DivFloat32
            | DivFloat64
            | DivNumeric
            | ModInt16
            | ModInt32
            | ModInt64
            | ModUInt16
            | ModUInt32
            | ModUInt64
            | ModFloat32
            | ModFloat64
            | ModNumeric
            | Eq
            | NotEq
            | Lt
            | Lte
            | Gt
            | Gte
            | JsonbConcat
            | JsonbContainsJsonb
            | JsonbGetInt64
            | JsonbGetInt64Stringify
            | JsonbGetString
            | JsonbGetStringStringify
            | JsonbGetPath
            | JsonbGetPathStringify
            | JsonbContainsString
            | JsonbDeleteInt64
            | JsonbDeleteString
            | MapContainsKey
            | MapGetValue
            | MapContainsAllKeys
            | MapContainsAnyKeys
            | MapContainsMap
            | TextConcat
            | IsLikeMatch { .. }
            | IsRegexpMatch { .. }
            | ArrayContains
            | ArrayContainsArray { .. }
            | ArrayLength
            | ArrayLower
            | ArrayUpper
            | ArrayArrayConcat
            | ListListConcat
            | ListElementConcat
            | ElementListConcat
            | ListContainsList { .. }
            | RangeContainsElem { .. }
            | RangeContainsRange { .. }
            | RangeOverlaps
            | RangeAfter
            | RangeBefore
            | RangeOverleft
            | RangeOverright
            | RangeAdjacent
            | RangeUnion
            | RangeIntersection
            | RangeDifference => true,
            ToCharTimestamp
            | ToCharTimestampTz
            | AgeTimestamp
            | AgeTimestampTz
            | DateBinTimestamp
            | DateBinTimestampTz
            | ExtractInterval
            | ExtractTime
            | ExtractTimestamp
            | ExtractTimestampTz
            | ExtractDate
            | DatePartInterval
            | DatePartTime
            | DatePartTimestamp
            | DatePartTimestampTz
            | DateTruncInterval
            | DateTruncTimestamp
            | DateTruncTimestampTz
            | TimezoneTimestamp
            | TimezoneTimestampTz
            | TimezoneIntervalTimestamp
            | TimezoneIntervalTimestampTz
            | TimezoneIntervalTime
            | TimezoneOffset
            | RoundNumeric
            | ConvertFrom
            | Left
            | Position
            | Right
            | Trim
            | TrimLeading
            | TrimTrailing
            | EncodedBytesCharLength
            | ListLengthMax { .. }
            | DigestString
            | DigestBytes
            | MzRenderTypmod
            | Encode
            | Decode
            | LogNumeric
            | Power
            | PowerNumeric
            | RepeatString
            | Normalize
            | ArrayRemove
            | ListRemove
            | LikeEscape
            | UuidGenerateV5
            | GetBit
            | GetByte
            | MzAclItemContainsPrivilege
            | ConstantTimeEqBytes
            | ConstantTimeEqString
            | ParseIdent
            | PrettySql
            | RegexpReplace { .. }
            | StartsWith => false,
        }
    }

    /// Returns the negation of the given binary function, if it exists.
    pub fn negate(&self) -> Option<Self> {
        match self {
            BinaryFunc::AddInt16(s) => s.negate(),
            BinaryFunc::AddInt32(s) => s.negate(),
            BinaryFunc::AddInt64(s) => s.negate(),
            BinaryFunc::AddUInt16(s) => s.negate(),
            BinaryFunc::AddUInt32(s) => s.negate(),
            BinaryFunc::AddUInt64(s) => s.negate(),
            BinaryFunc::AddFloat32(s) => s.negate(),
            BinaryFunc::AddFloat64(s) => s.negate(),

            // The following functions are specifically declared for legacy reasons.
            // TODO: Pending conversion to `LazyBinaryFunc`, these can be removed.
            BinaryFunc::Eq => Some(BinaryFunc::NotEq),
            BinaryFunc::NotEq => Some(BinaryFunc::Eq),
            BinaryFunc::Lt => Some(BinaryFunc::Gte),
            BinaryFunc::Gte => Some(BinaryFunc::Lt),
            BinaryFunc::Gt => Some(BinaryFunc::Lte),
            BinaryFunc::Lte => Some(BinaryFunc::Gt),
            _ => None,
        }
    }

    /// Returns true if the function could introduce an error on non-error inputs.
    pub fn could_error(&self) -> bool {
        match self {
            BinaryFunc::AddInt16(s) => s.could_error(),
            BinaryFunc::AddInt32(s) => s.could_error(),
            BinaryFunc::AddInt64(s) => s.could_error(),
            BinaryFunc::AddUInt16(s) => s.could_error(),
            BinaryFunc::AddUInt32(s) => s.could_error(),
            BinaryFunc::AddUInt64(s) => s.could_error(),
            BinaryFunc::AddFloat32(s) => s.could_error(),
            BinaryFunc::AddFloat64(s) => s.could_error(),
            BinaryFunc::Eq
            | BinaryFunc::NotEq
            | BinaryFunc::Lt
            | BinaryFunc::Gte
            | BinaryFunc::Gt
            | BinaryFunc::Lte => false,
            BinaryFunc::BitAndInt16
            | BinaryFunc::BitAndInt32
            | BinaryFunc::BitAndInt64
            | BinaryFunc::BitAndUInt16
            | BinaryFunc::BitAndUInt32
            | BinaryFunc::BitAndUInt64
            | BinaryFunc::BitOrInt16
            | BinaryFunc::BitOrInt32
            | BinaryFunc::BitOrInt64
            | BinaryFunc::BitOrUInt16
            | BinaryFunc::BitOrUInt32
            | BinaryFunc::BitOrUInt64
            | BinaryFunc::BitXorInt16
            | BinaryFunc::BitXorInt32
            | BinaryFunc::BitXorInt64
            | BinaryFunc::BitXorUInt16
            | BinaryFunc::BitXorUInt32
            | BinaryFunc::BitXorUInt64
            | BinaryFunc::BitShiftLeftInt16
            | BinaryFunc::BitShiftLeftInt32
            | BinaryFunc::BitShiftLeftInt64
            | BinaryFunc::BitShiftLeftUInt16
            | BinaryFunc::BitShiftLeftUInt32
            | BinaryFunc::BitShiftLeftUInt64
            | BinaryFunc::BitShiftRightInt16
            | BinaryFunc::BitShiftRightInt32
            | BinaryFunc::BitShiftRightInt64
            | BinaryFunc::BitShiftRightUInt16
            | BinaryFunc::BitShiftRightUInt32
            | BinaryFunc::BitShiftRightUInt64 => false,
            BinaryFunc::JsonbGetInt64
            | BinaryFunc::JsonbGetInt64Stringify
            | BinaryFunc::JsonbGetString
            | BinaryFunc::JsonbGetStringStringify
            | BinaryFunc::JsonbGetPath
            | BinaryFunc::JsonbGetPathStringify
            | BinaryFunc::JsonbContainsString
            | BinaryFunc::JsonbConcat
            | BinaryFunc::JsonbContainsJsonb
            | BinaryFunc::JsonbDeleteInt64
            | BinaryFunc::JsonbDeleteString => false,
            BinaryFunc::MapContainsKey
            | BinaryFunc::MapGetValue
            | BinaryFunc::MapContainsAllKeys
            | BinaryFunc::MapContainsAnyKeys
            | BinaryFunc::MapContainsMap => false,
            BinaryFunc::AddTimeInterval
            | BinaryFunc::SubTimestamp
            | BinaryFunc::SubTimestampTz
            | BinaryFunc::SubDate
            | BinaryFunc::SubTime
            | BinaryFunc::SubTimeInterval
            | BinaryFunc::UuidGenerateV5
            | BinaryFunc::RangeContainsRange { .. }
            | BinaryFunc::RangeContainsElem { .. }
            | BinaryFunc::RangeOverlaps
            | BinaryFunc::RangeAfter
            | BinaryFunc::RangeBefore
            | BinaryFunc::RangeOverleft
            | BinaryFunc::RangeOverright
            | BinaryFunc::RangeAdjacent
            | BinaryFunc::ArrayLower
            | BinaryFunc::ArrayContains
            | BinaryFunc::ArrayContainsArray { rev: _ }
            | BinaryFunc::ListListConcat
            | BinaryFunc::ListElementConcat
            | BinaryFunc::ElementListConcat
            | BinaryFunc::ListRemove
            | BinaryFunc::ToCharTimestamp
            | BinaryFunc::ToCharTimestampTz
            | BinaryFunc::ListContainsList { rev: _ }
            | BinaryFunc::Trim
            | BinaryFunc::TrimLeading
            | BinaryFunc::TrimTrailing
            | BinaryFunc::TextConcat
            | BinaryFunc::StartsWith => false,

            _ => true,
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
            BinaryFunc::AddUInt16(s) => s.is_monotone(),
            BinaryFunc::AddUInt32(s) => s.is_monotone(),
            BinaryFunc::AddUInt64(s) => s.is_monotone(),
            BinaryFunc::AddFloat32(s) => s.is_monotone(),
            BinaryFunc::AddFloat64(s) => s.is_monotone(),
            BinaryFunc::AddInterval
            | BinaryFunc::AddTimestampInterval
            | BinaryFunc::AddTimestampTzInterval
            | BinaryFunc::AddDateInterval
            | BinaryFunc::AddDateTime
            | BinaryFunc::AddNumeric => (true, true),
            // <time> + <interval> wraps!
            BinaryFunc::AddTimeInterval => (false, false),
            BinaryFunc::BitAndInt16
            | BinaryFunc::BitAndInt32
            | BinaryFunc::BitAndInt64
            | BinaryFunc::BitAndUInt16
            | BinaryFunc::BitAndUInt32
            | BinaryFunc::BitAndUInt64
            | BinaryFunc::BitOrInt16
            | BinaryFunc::BitOrInt32
            | BinaryFunc::BitOrInt64
            | BinaryFunc::BitOrUInt16
            | BinaryFunc::BitOrUInt32
            | BinaryFunc::BitOrUInt64
            | BinaryFunc::BitXorInt16
            | BinaryFunc::BitXorInt32
            | BinaryFunc::BitXorInt64
            | BinaryFunc::BitXorUInt16
            | BinaryFunc::BitXorUInt32
            | BinaryFunc::BitXorUInt64 => (false, false),
            // The shift functions wrap, which means they are monotonic in neither argument.
            BinaryFunc::BitShiftLeftInt16
            | BinaryFunc::BitShiftLeftInt32
            | BinaryFunc::BitShiftLeftInt64
            | BinaryFunc::BitShiftLeftUInt16
            | BinaryFunc::BitShiftLeftUInt32
            | BinaryFunc::BitShiftLeftUInt64
            | BinaryFunc::BitShiftRightInt16
            | BinaryFunc::BitShiftRightInt32
            | BinaryFunc::BitShiftRightInt64
            | BinaryFunc::BitShiftRightUInt16
            | BinaryFunc::BitShiftRightUInt32
            | BinaryFunc::BitShiftRightUInt64 => (false, false),
            BinaryFunc::SubInt16
            | BinaryFunc::SubInt32
            | BinaryFunc::SubInt64
            | BinaryFunc::SubUInt16
            | BinaryFunc::SubUInt32
            | BinaryFunc::SubUInt64
            | BinaryFunc::SubFloat32
            | BinaryFunc::SubFloat64
            | BinaryFunc::SubInterval
            | BinaryFunc::SubTimestamp
            | BinaryFunc::SubTimestampTz
            | BinaryFunc::SubTimestampInterval
            | BinaryFunc::SubTimestampTzInterval
            | BinaryFunc::SubDate
            | BinaryFunc::SubDateInterval
            | BinaryFunc::SubTime
            | BinaryFunc::SubNumeric => (true, true),
            // <time> - <interval> wraps!
            BinaryFunc::SubTimeInterval => (false, false),
            BinaryFunc::MulInt16
            | BinaryFunc::MulInt32
            | BinaryFunc::MulInt64
            | BinaryFunc::MulUInt16
            | BinaryFunc::MulUInt32
            | BinaryFunc::MulUInt64
            | BinaryFunc::MulFloat32
            | BinaryFunc::MulFloat64
            | BinaryFunc::MulNumeric => (true, true),
            BinaryFunc::MulInterval => (false, false),
            BinaryFunc::DivInt16
            | BinaryFunc::DivInt32
            | BinaryFunc::DivInt64
            | BinaryFunc::DivUInt16
            | BinaryFunc::DivUInt32
            | BinaryFunc::DivUInt64
            | BinaryFunc::DivFloat32
            | BinaryFunc::DivFloat64
            | BinaryFunc::DivNumeric => (true, false),
            BinaryFunc::DivInterval => (false, false),
            BinaryFunc::ModInt16
            | BinaryFunc::ModInt32
            | BinaryFunc::ModInt64
            | BinaryFunc::ModUInt16
            | BinaryFunc::ModUInt32
            | BinaryFunc::ModUInt64
            | BinaryFunc::ModFloat32
            | BinaryFunc::ModFloat64
            | BinaryFunc::ModNumeric => (false, false),
            BinaryFunc::RoundNumeric => (true, false),
            BinaryFunc::Eq | BinaryFunc::NotEq => (false, false),
            BinaryFunc::Lt | BinaryFunc::Lte | BinaryFunc::Gt | BinaryFunc::Gte => (true, true),
            BinaryFunc::LikeEscape
            | BinaryFunc::IsLikeMatch { .. }
            | BinaryFunc::IsRegexpMatch { .. } => (false, false),
            BinaryFunc::ToCharTimestamp | BinaryFunc::ToCharTimestampTz => (false, false),
            BinaryFunc::DateBinTimestamp | BinaryFunc::DateBinTimestampTz => (true, true),
            BinaryFunc::AgeTimestamp | BinaryFunc::AgeTimestampTz => (true, true),
            // Text concatenation is monotonic in its second argument, because if I change the
            // second argument but don't change the first argument, then we won't find a difference
            // in that part of the concatenation result that came from the first argument, so we'll
            // find the difference that comes from changing the second argument.
            // (It's not monotonic in its first argument, because e.g.,
            // 'A' < 'AA' but 'AZ' > 'AAZ'.)
            BinaryFunc::TextConcat => (false, true),
            // `left` is unfortunately not monotonic (at least for negative second arguments),
            // because 'aa' < 'z', but `left(_, -1)` makes 'a' > ''.
            BinaryFunc::Left => (false, false),
            // TODO: can these ever be treated as monotone? It's safe to treat the unary versions
            // as monotone in some cases, but only when extracting specific parts.
            BinaryFunc::ExtractInterval
            | BinaryFunc::ExtractTime
            | BinaryFunc::ExtractTimestamp
            | BinaryFunc::ExtractTimestampTz
            | BinaryFunc::ExtractDate => (false, false),
            BinaryFunc::DatePartInterval
            | BinaryFunc::DatePartTime
            | BinaryFunc::DatePartTimestamp
            | BinaryFunc::DatePartTimestampTz => (false, false),
            BinaryFunc::DateTruncTimestamp
            | BinaryFunc::DateTruncTimestampTz
            | BinaryFunc::DateTruncInterval => (false, false),
            BinaryFunc::TimezoneTimestamp
            | BinaryFunc::TimezoneTimestampTz
            | BinaryFunc::TimezoneIntervalTimestamp
            | BinaryFunc::TimezoneIntervalTimestampTz
            | BinaryFunc::TimezoneIntervalTime
            | BinaryFunc::TimezoneOffset => (false, false),
            BinaryFunc::JsonbGetInt64
            | BinaryFunc::JsonbGetInt64Stringify
            | BinaryFunc::JsonbGetString
            | BinaryFunc::JsonbGetStringStringify
            | BinaryFunc::JsonbGetPath
            | BinaryFunc::JsonbGetPathStringify
            | BinaryFunc::JsonbContainsString
            | BinaryFunc::JsonbConcat
            | BinaryFunc::JsonbContainsJsonb
            | BinaryFunc::JsonbDeleteInt64
            | BinaryFunc::JsonbDeleteString
            | BinaryFunc::MapContainsKey
            | BinaryFunc::MapGetValue
            | BinaryFunc::MapContainsAllKeys
            | BinaryFunc::MapContainsAnyKeys
            | BinaryFunc::MapContainsMap => (false, false),
            BinaryFunc::ConvertFrom
            | BinaryFunc::Position
            | BinaryFunc::Right
            | BinaryFunc::RepeatString
            | BinaryFunc::Trim
            | BinaryFunc::TrimLeading
            | BinaryFunc::TrimTrailing
            | BinaryFunc::EncodedBytesCharLength
            | BinaryFunc::ListLengthMax { .. }
            | BinaryFunc::ArrayContains
            | BinaryFunc::ArrayContainsArray { .. }
            | BinaryFunc::ArrayLength
            | BinaryFunc::ArrayLower
            | BinaryFunc::ArrayRemove
            | BinaryFunc::ArrayUpper
            | BinaryFunc::ArrayArrayConcat
            | BinaryFunc::ListListConcat
            | BinaryFunc::ListElementConcat
            | BinaryFunc::ElementListConcat
            | BinaryFunc::ListContainsList { .. }
            | BinaryFunc::ListRemove
            | BinaryFunc::DigestString
            | BinaryFunc::DigestBytes
            | BinaryFunc::MzRenderTypmod
            | BinaryFunc::Encode
            | BinaryFunc::Decode => (false, false),
            // TODO: it may be safe to treat these as monotone.
            BinaryFunc::LogNumeric | BinaryFunc::Power | BinaryFunc::PowerNumeric => (false, false),
            BinaryFunc::GetBit
            | BinaryFunc::GetByte
            | BinaryFunc::RangeContainsElem { .. }
            | BinaryFunc::RangeContainsRange { .. }
            | BinaryFunc::RangeOverlaps
            | BinaryFunc::RangeAfter
            | BinaryFunc::RangeBefore
            | BinaryFunc::RangeOverleft
            | BinaryFunc::RangeOverright
            | BinaryFunc::RangeAdjacent
            | BinaryFunc::RangeUnion
            | BinaryFunc::RangeIntersection
            | BinaryFunc::RangeDifference => (false, false),
            BinaryFunc::UuidGenerateV5 => (false, false),
            BinaryFunc::MzAclItemContainsPrivilege => (false, false),
            BinaryFunc::ParseIdent => (false, false),
            BinaryFunc::ConstantTimeEqBytes | BinaryFunc::ConstantTimeEqString => (false, false),
            BinaryFunc::PrettySql => (false, false),
            BinaryFunc::RegexpReplace { .. } => (false, false),
            BinaryFunc::StartsWith => (false, false),
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
            BinaryFunc::AddUInt16(s) => s.fmt(f),
            BinaryFunc::AddUInt32(s) => s.fmt(f),
            BinaryFunc::AddUInt64(s) => s.fmt(f),
            BinaryFunc::AddFloat32(s) => s.fmt(f),
            BinaryFunc::AddFloat64(s) => s.fmt(f),
            BinaryFunc::AddNumeric => f.write_str("+"),
            BinaryFunc::AddInterval => f.write_str("+"),
            BinaryFunc::AddTimestampInterval => f.write_str("+"),
            BinaryFunc::AddTimestampTzInterval => f.write_str("+"),
            BinaryFunc::AddDateTime => f.write_str("+"),
            BinaryFunc::AddDateInterval => f.write_str("+"),
            BinaryFunc::AddTimeInterval => f.write_str("+"),
            BinaryFunc::AgeTimestamp => f.write_str("age"),
            BinaryFunc::AgeTimestampTz => f.write_str("age"),
            BinaryFunc::BitAndInt16 => f.write_str("&"),
            BinaryFunc::BitAndInt32 => f.write_str("&"),
            BinaryFunc::BitAndInt64 => f.write_str("&"),
            BinaryFunc::BitAndUInt16 => f.write_str("&"),
            BinaryFunc::BitAndUInt32 => f.write_str("&"),
            BinaryFunc::BitAndUInt64 => f.write_str("&"),
            BinaryFunc::BitOrInt16 => f.write_str("|"),
            BinaryFunc::BitOrInt32 => f.write_str("|"),
            BinaryFunc::BitOrInt64 => f.write_str("|"),
            BinaryFunc::BitOrUInt16 => f.write_str("|"),
            BinaryFunc::BitOrUInt32 => f.write_str("|"),
            BinaryFunc::BitOrUInt64 => f.write_str("|"),
            BinaryFunc::BitXorInt16 => f.write_str("#"),
            BinaryFunc::BitXorInt32 => f.write_str("#"),
            BinaryFunc::BitXorInt64 => f.write_str("#"),
            BinaryFunc::BitXorUInt16 => f.write_str("#"),
            BinaryFunc::BitXorUInt32 => f.write_str("#"),
            BinaryFunc::BitXorUInt64 => f.write_str("#"),
            BinaryFunc::BitShiftLeftInt16 => f.write_str("<<"),
            BinaryFunc::BitShiftLeftInt32 => f.write_str("<<"),
            BinaryFunc::BitShiftLeftInt64 => f.write_str("<<"),
            BinaryFunc::BitShiftLeftUInt16 => f.write_str("<<"),
            BinaryFunc::BitShiftLeftUInt32 => f.write_str("<<"),
            BinaryFunc::BitShiftLeftUInt64 => f.write_str("<<"),
            BinaryFunc::BitShiftRightInt16 => f.write_str(">>"),
            BinaryFunc::BitShiftRightInt32 => f.write_str(">>"),
            BinaryFunc::BitShiftRightInt64 => f.write_str(">>"),
            BinaryFunc::BitShiftRightUInt16 => f.write_str(">>"),
            BinaryFunc::BitShiftRightUInt32 => f.write_str(">>"),
            BinaryFunc::BitShiftRightUInt64 => f.write_str(">>"),
            BinaryFunc::SubInt16 => f.write_str("-"),
            BinaryFunc::SubInt32 => f.write_str("-"),
            BinaryFunc::SubInt64 => f.write_str("-"),
            BinaryFunc::SubUInt16 => f.write_str("-"),
            BinaryFunc::SubUInt32 => f.write_str("-"),
            BinaryFunc::SubUInt64 => f.write_str("-"),
            BinaryFunc::SubFloat32 => f.write_str("-"),
            BinaryFunc::SubFloat64 => f.write_str("-"),
            BinaryFunc::SubNumeric => f.write_str("-"),
            BinaryFunc::SubInterval => f.write_str("-"),
            BinaryFunc::SubTimestamp => f.write_str("-"),
            BinaryFunc::SubTimestampTz => f.write_str("-"),
            BinaryFunc::SubTimestampInterval => f.write_str("-"),
            BinaryFunc::SubTimestampTzInterval => f.write_str("-"),
            BinaryFunc::SubDate => f.write_str("-"),
            BinaryFunc::SubDateInterval => f.write_str("-"),
            BinaryFunc::SubTime => f.write_str("-"),
            BinaryFunc::SubTimeInterval => f.write_str("-"),
            BinaryFunc::MulInt16 => f.write_str("*"),
            BinaryFunc::MulInt32 => f.write_str("*"),
            BinaryFunc::MulInt64 => f.write_str("*"),
            BinaryFunc::MulUInt16 => f.write_str("*"),
            BinaryFunc::MulUInt32 => f.write_str("*"),
            BinaryFunc::MulUInt64 => f.write_str("*"),
            BinaryFunc::MulFloat32 => f.write_str("*"),
            BinaryFunc::MulFloat64 => f.write_str("*"),
            BinaryFunc::MulNumeric => f.write_str("*"),
            BinaryFunc::MulInterval => f.write_str("*"),
            BinaryFunc::DivInt16 => f.write_str("/"),
            BinaryFunc::DivInt32 => f.write_str("/"),
            BinaryFunc::DivInt64 => f.write_str("/"),
            BinaryFunc::DivUInt16 => f.write_str("/"),
            BinaryFunc::DivUInt32 => f.write_str("/"),
            BinaryFunc::DivUInt64 => f.write_str("/"),
            BinaryFunc::DivFloat32 => f.write_str("/"),
            BinaryFunc::DivFloat64 => f.write_str("/"),
            BinaryFunc::DivNumeric => f.write_str("/"),
            BinaryFunc::DivInterval => f.write_str("/"),
            BinaryFunc::ModInt16 => f.write_str("%"),
            BinaryFunc::ModInt32 => f.write_str("%"),
            BinaryFunc::ModInt64 => f.write_str("%"),
            BinaryFunc::ModUInt16 => f.write_str("%"),
            BinaryFunc::ModUInt32 => f.write_str("%"),
            BinaryFunc::ModUInt64 => f.write_str("%"),
            BinaryFunc::ModFloat32 => f.write_str("%"),
            BinaryFunc::ModFloat64 => f.write_str("%"),
            BinaryFunc::ModNumeric => f.write_str("%"),
            BinaryFunc::Eq => f.write_str("="),
            BinaryFunc::NotEq => f.write_str("!="),
            BinaryFunc::Lt => f.write_str("<"),
            BinaryFunc::Lte => f.write_str("<="),
            BinaryFunc::Gt => f.write_str(">"),
            BinaryFunc::Gte => f.write_str(">="),
            BinaryFunc::LikeEscape => f.write_str("like_escape"),
            BinaryFunc::IsLikeMatch {
                case_insensitive: false,
            } => f.write_str("like"),
            BinaryFunc::IsLikeMatch {
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
            BinaryFunc::DateBinTimestamp => f.write_str("bin_unix_epoch_timestamp"),
            BinaryFunc::DateBinTimestampTz => f.write_str("bin_unix_epoch_timestamptz"),
            BinaryFunc::ExtractInterval => f.write_str("extractiv"),
            BinaryFunc::ExtractTime => f.write_str("extractt"),
            BinaryFunc::ExtractTimestamp => f.write_str("extractts"),
            BinaryFunc::ExtractTimestampTz => f.write_str("extracttstz"),
            BinaryFunc::ExtractDate => f.write_str("extractd"),
            BinaryFunc::DatePartInterval => f.write_str("date_partiv"),
            BinaryFunc::DatePartTime => f.write_str("date_partt"),
            BinaryFunc::DatePartTimestamp => f.write_str("date_partts"),
            BinaryFunc::DatePartTimestampTz => f.write_str("date_parttstz"),
            BinaryFunc::DateTruncTimestamp => f.write_str("date_truncts"),
            BinaryFunc::DateTruncInterval => f.write_str("date_trunciv"),
            BinaryFunc::DateTruncTimestampTz => f.write_str("date_trunctstz"),
            BinaryFunc::TimezoneTimestamp => f.write_str("timezonets"),
            BinaryFunc::TimezoneTimestampTz => f.write_str("timezonetstz"),
            BinaryFunc::TimezoneIntervalTimestamp => f.write_str("timezoneits"),
            BinaryFunc::TimezoneIntervalTimestampTz => f.write_str("timezoneitstz"),
            BinaryFunc::TimezoneIntervalTime => f.write_str("timezoneit"),
            BinaryFunc::TimezoneOffset => f.write_str("timezone_offset"),
            BinaryFunc::TextConcat => f.write_str("||"),
            BinaryFunc::JsonbGetInt64 => f.write_str("->"),
            BinaryFunc::JsonbGetInt64Stringify => f.write_str("->>"),
            BinaryFunc::JsonbGetString => f.write_str("->"),
            BinaryFunc::JsonbGetStringStringify => f.write_str("->>"),
            BinaryFunc::JsonbGetPath => f.write_str("#>"),
            BinaryFunc::JsonbGetPathStringify => f.write_str("#>>"),
            BinaryFunc::JsonbContainsString | BinaryFunc::MapContainsKey => f.write_str("?"),
            BinaryFunc::JsonbConcat => f.write_str("||"),
            BinaryFunc::JsonbContainsJsonb | BinaryFunc::MapContainsMap => f.write_str("@>"),
            BinaryFunc::JsonbDeleteInt64 => f.write_str("-"),
            BinaryFunc::JsonbDeleteString => f.write_str("-"),
            BinaryFunc::MapGetValue => f.write_str("->"),
            BinaryFunc::MapContainsAllKeys => f.write_str("?&"),
            BinaryFunc::MapContainsAnyKeys => f.write_str("?|"),
            BinaryFunc::RoundNumeric => f.write_str("round"),
            BinaryFunc::ConvertFrom => f.write_str("convert_from"),
            BinaryFunc::Left => f.write_str("left"),
            BinaryFunc::Position => f.write_str("position"),
            BinaryFunc::Right => f.write_str("right"),
            BinaryFunc::Trim => f.write_str("btrim"),
            BinaryFunc::TrimLeading => f.write_str("ltrim"),
            BinaryFunc::TrimTrailing => f.write_str("rtrim"),
            BinaryFunc::EncodedBytesCharLength => f.write_str("length"),
            BinaryFunc::ListLengthMax { .. } => f.write_str("list_length_max"),
            BinaryFunc::ArrayContains => f.write_str("array_contains"),
            BinaryFunc::ArrayContainsArray { rev } => f.write_str(if *rev { "<@" } else { "@>" }),
            BinaryFunc::ArrayLength => f.write_str("array_length"),
            BinaryFunc::ArrayLower => f.write_str("array_lower"),
            BinaryFunc::ArrayRemove => f.write_str("array_remove"),
            BinaryFunc::ArrayUpper => f.write_str("array_upper"),
            BinaryFunc::ArrayArrayConcat => f.write_str("||"),
            BinaryFunc::ListListConcat => f.write_str("||"),
            BinaryFunc::ListElementConcat => f.write_str("||"),
            BinaryFunc::ElementListConcat => f.write_str("||"),
            BinaryFunc::ListRemove => f.write_str("list_remove"),
            BinaryFunc::ListContainsList { rev } => f.write_str(if *rev { "<@" } else { "@>" }),
            BinaryFunc::DigestString | BinaryFunc::DigestBytes => f.write_str("digest"),
            BinaryFunc::MzRenderTypmod => f.write_str("mz_render_typmod"),
            BinaryFunc::Encode => f.write_str("encode"),
            BinaryFunc::Decode => f.write_str("decode"),
            BinaryFunc::LogNumeric => f.write_str("log"),
            BinaryFunc::Power => f.write_str("power"),
            BinaryFunc::PowerNumeric => f.write_str("power_numeric"),
            BinaryFunc::RepeatString => f.write_str("repeat"),
            BinaryFunc::Normalize => f.write_str("normalize"),
            BinaryFunc::GetBit => f.write_str("get_bit"),
            BinaryFunc::GetByte => f.write_str("get_byte"),
            BinaryFunc::ConstantTimeEqBytes => f.write_str("constant_time_compare_bytes"),
            BinaryFunc::ConstantTimeEqString => f.write_str("constant_time_compare_strings"),
            BinaryFunc::RangeContainsElem { rev, .. } => {
                f.write_str(if *rev { "<@" } else { "@>" })
            }
            BinaryFunc::RangeContainsRange { rev, .. } => {
                f.write_str(if *rev { "<@" } else { "@>" })
            }
            BinaryFunc::RangeOverlaps => f.write_str("&&"),
            BinaryFunc::RangeAfter => f.write_str(">>"),
            BinaryFunc::RangeBefore => f.write_str("<<"),
            BinaryFunc::RangeOverleft => f.write_str("&<"),
            BinaryFunc::RangeOverright => f.write_str("&>"),
            BinaryFunc::RangeAdjacent => f.write_str("-|-"),
            BinaryFunc::RangeUnion => f.write_str("+"),
            BinaryFunc::RangeIntersection => f.write_str("*"),
            BinaryFunc::RangeDifference => f.write_str("-"),
            BinaryFunc::UuidGenerateV5 => f.write_str("uuid_generate_v5"),
            BinaryFunc::MzAclItemContainsPrivilege => f.write_str("mz_aclitem_contains_privilege"),
            BinaryFunc::ParseIdent => f.write_str("parse_ident"),
            BinaryFunc::PrettySql => f.write_str("pretty_sql"),
            BinaryFunc::RegexpReplace { regex, limit } => write!(
                f,
                "regexp_replace[{}, case_insensitive={}, limit={}]",
                regex.pattern().escaped(),
                regex.case_insensitive,
                limit
            ),
            BinaryFunc::StartsWith => f.write_str("starts_with"),
        }
    }
}

/// An explicit [`Arbitrary`] implementation needed here because of a known
/// `proptest` issue.
///
/// Revert to the derive-macro impementation once the issue[^1] is fixed.
///
/// [^1]: <https://github.com/AltSysrq/proptest/issues/152>
impl Arbitrary for BinaryFunc {
    type Parameters = ();

    type Strategy = Union<BoxedStrategy<Self>>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        Union::new(vec![
            Just(BinaryFunc::AddInt16(AddInt16)).boxed(),
            Just(BinaryFunc::AddInt32(AddInt32)).boxed(),
            Just(BinaryFunc::AddInt64(AddInt64)).boxed(),
            Just(BinaryFunc::AddUInt16(AddUint16)).boxed(),
            Just(BinaryFunc::AddUInt32(AddUint32)).boxed(),
            Just(BinaryFunc::AddUInt64(AddUint64)).boxed(),
            Just(BinaryFunc::AddFloat32(AddFloat32)).boxed(),
            Just(BinaryFunc::AddFloat64(AddFloat64)).boxed(),
            Just(BinaryFunc::AddInterval).boxed(),
            Just(BinaryFunc::AddTimestampInterval).boxed(),
            Just(BinaryFunc::AddTimestampTzInterval).boxed(),
            Just(BinaryFunc::AddDateInterval).boxed(),
            Just(BinaryFunc::AddDateTime).boxed(),
            Just(BinaryFunc::AddTimeInterval).boxed(),
            Just(BinaryFunc::AddNumeric).boxed(),
            Just(BinaryFunc::AgeTimestamp).boxed(),
            Just(BinaryFunc::AgeTimestampTz).boxed(),
            Just(BinaryFunc::BitAndInt16).boxed(),
            Just(BinaryFunc::BitAndInt32).boxed(),
            Just(BinaryFunc::BitAndInt64).boxed(),
            Just(BinaryFunc::BitAndUInt16).boxed(),
            Just(BinaryFunc::BitAndUInt32).boxed(),
            Just(BinaryFunc::BitAndUInt64).boxed(),
            Just(BinaryFunc::BitOrInt16).boxed(),
            Just(BinaryFunc::BitOrInt32).boxed(),
            Just(BinaryFunc::BitOrInt64).boxed(),
            Just(BinaryFunc::BitOrUInt16).boxed(),
            Just(BinaryFunc::BitOrUInt32).boxed(),
            Just(BinaryFunc::BitOrUInt64).boxed(),
            Just(BinaryFunc::BitXorInt16).boxed(),
            Just(BinaryFunc::BitXorInt32).boxed(),
            Just(BinaryFunc::BitXorInt64).boxed(),
            Just(BinaryFunc::BitXorUInt16).boxed(),
            Just(BinaryFunc::BitXorUInt32).boxed(),
            Just(BinaryFunc::BitXorUInt64).boxed(),
            Just(BinaryFunc::BitShiftLeftInt16).boxed(),
            Just(BinaryFunc::BitShiftLeftInt32).boxed(),
            Just(BinaryFunc::BitShiftLeftInt64).boxed(),
            Just(BinaryFunc::BitShiftLeftUInt16).boxed(),
            Just(BinaryFunc::BitShiftLeftUInt32).boxed(),
            Just(BinaryFunc::BitShiftLeftUInt64).boxed(),
            Just(BinaryFunc::BitShiftRightInt16).boxed(),
            Just(BinaryFunc::BitShiftRightInt32).boxed(),
            Just(BinaryFunc::BitShiftRightInt64).boxed(),
            Just(BinaryFunc::BitShiftRightUInt16).boxed(),
            Just(BinaryFunc::BitShiftRightUInt32).boxed(),
            Just(BinaryFunc::BitShiftRightUInt64).boxed(),
            Just(BinaryFunc::SubInt16).boxed(),
            Just(BinaryFunc::SubInt32).boxed(),
            Just(BinaryFunc::SubInt64).boxed(),
            Just(BinaryFunc::SubUInt16).boxed(),
            Just(BinaryFunc::SubUInt32).boxed(),
            Just(BinaryFunc::SubUInt64).boxed(),
            Just(BinaryFunc::SubFloat32).boxed(),
            Just(BinaryFunc::SubFloat64).boxed(),
            Just(BinaryFunc::SubInterval).boxed(),
            Just(BinaryFunc::SubTimestamp).boxed(),
            Just(BinaryFunc::SubTimestampTz).boxed(),
            Just(BinaryFunc::SubTimestampInterval).boxed(),
            Just(BinaryFunc::SubTimestampTzInterval).boxed(),
            Just(BinaryFunc::SubDate).boxed(),
            Just(BinaryFunc::SubDateInterval).boxed(),
            Just(BinaryFunc::SubTime).boxed(),
            Just(BinaryFunc::SubTimeInterval).boxed(),
            Just(BinaryFunc::SubNumeric).boxed(),
            Just(BinaryFunc::MulInt16).boxed(),
            Just(BinaryFunc::MulInt32).boxed(),
            Just(BinaryFunc::MulInt64).boxed(),
            Just(BinaryFunc::MulUInt16).boxed(),
            Just(BinaryFunc::MulUInt32).boxed(),
            Just(BinaryFunc::MulUInt64).boxed(),
            Just(BinaryFunc::MulFloat32).boxed(),
            Just(BinaryFunc::MulFloat64).boxed(),
            Just(BinaryFunc::MulNumeric).boxed(),
            Just(BinaryFunc::MulInterval).boxed(),
            Just(BinaryFunc::DivInt16).boxed(),
            Just(BinaryFunc::DivInt32).boxed(),
            Just(BinaryFunc::DivInt64).boxed(),
            Just(BinaryFunc::DivUInt16).boxed(),
            Just(BinaryFunc::DivUInt32).boxed(),
            Just(BinaryFunc::DivUInt64).boxed(),
            Just(BinaryFunc::DivFloat32).boxed(),
            Just(BinaryFunc::DivFloat64).boxed(),
            Just(BinaryFunc::DivNumeric).boxed(),
            Just(BinaryFunc::DivInterval).boxed(),
            Just(BinaryFunc::ModInt16).boxed(),
            Just(BinaryFunc::ModInt32).boxed(),
            Just(BinaryFunc::ModInt64).boxed(),
            Just(BinaryFunc::ModUInt16).boxed(),
            Just(BinaryFunc::ModUInt32).boxed(),
            Just(BinaryFunc::ModUInt64).boxed(),
            Just(BinaryFunc::ModFloat32).boxed(),
            Just(BinaryFunc::ModFloat64).boxed(),
            Just(BinaryFunc::ModNumeric).boxed(),
            Just(BinaryFunc::RoundNumeric).boxed(),
            Just(BinaryFunc::Eq).boxed(),
            Just(BinaryFunc::NotEq).boxed(),
            Just(BinaryFunc::Lt).boxed(),
            Just(BinaryFunc::Lte).boxed(),
            Just(BinaryFunc::Gt).boxed(),
            Just(BinaryFunc::Gte).boxed(),
            Just(BinaryFunc::LikeEscape).boxed(),
            bool::arbitrary()
                .prop_map(|case_insensitive| BinaryFunc::IsLikeMatch { case_insensitive })
                .boxed(),
            bool::arbitrary()
                .prop_map(|case_insensitive| BinaryFunc::IsRegexpMatch { case_insensitive })
                .boxed(),
            Just(BinaryFunc::ToCharTimestamp).boxed(),
            Just(BinaryFunc::ToCharTimestampTz).boxed(),
            Just(BinaryFunc::DateBinTimestamp).boxed(),
            Just(BinaryFunc::DateBinTimestampTz).boxed(),
            Just(BinaryFunc::ExtractInterval).boxed(),
            Just(BinaryFunc::ExtractTime).boxed(),
            Just(BinaryFunc::ExtractTimestamp).boxed(),
            Just(BinaryFunc::ExtractTimestampTz).boxed(),
            Just(BinaryFunc::ExtractDate).boxed(),
            Just(BinaryFunc::DatePartInterval).boxed(),
            Just(BinaryFunc::DatePartTime).boxed(),
            Just(BinaryFunc::DatePartTimestamp).boxed(),
            Just(BinaryFunc::DatePartTimestampTz).boxed(),
            Just(BinaryFunc::DateTruncTimestamp).boxed(),
            Just(BinaryFunc::DateTruncTimestampTz).boxed(),
            Just(BinaryFunc::DateTruncInterval).boxed(),
            Just(BinaryFunc::TimezoneTimestamp).boxed(),
            Just(BinaryFunc::TimezoneTimestampTz).boxed(),
            Just(BinaryFunc::TimezoneIntervalTimestamp).boxed(),
            Just(BinaryFunc::TimezoneIntervalTimestampTz).boxed(),
            Just(BinaryFunc::TimezoneIntervalTime).boxed(),
            Just(BinaryFunc::TimezoneOffset).boxed(),
            Just(BinaryFunc::TextConcat).boxed(),
            Just(BinaryFunc::JsonbGetInt64).boxed(),
            Just(BinaryFunc::JsonbGetInt64Stringify).boxed(),
            Just(BinaryFunc::JsonbGetString).boxed(),
            Just(BinaryFunc::JsonbGetStringStringify).boxed(),
            Just(BinaryFunc::JsonbGetPath).boxed(),
            Just(BinaryFunc::JsonbGetPathStringify).boxed(),
            Just(BinaryFunc::JsonbContainsString).boxed(),
            Just(BinaryFunc::JsonbConcat).boxed(),
            Just(BinaryFunc::JsonbContainsJsonb).boxed(),
            Just(BinaryFunc::JsonbDeleteInt64).boxed(),
            Just(BinaryFunc::JsonbDeleteString).boxed(),
            Just(BinaryFunc::MapContainsKey).boxed(),
            Just(BinaryFunc::MapGetValue).boxed(),
            Just(BinaryFunc::MapContainsAllKeys).boxed(),
            Just(BinaryFunc::MapContainsAnyKeys).boxed(),
            Just(BinaryFunc::MapContainsMap).boxed(),
            Just(BinaryFunc::ConvertFrom).boxed(),
            Just(BinaryFunc::Left).boxed(),
            Just(BinaryFunc::Position).boxed(),
            Just(BinaryFunc::Right).boxed(),
            Just(BinaryFunc::RepeatString).boxed(),
            Just(BinaryFunc::Normalize).boxed(),
            Just(BinaryFunc::Trim).boxed(),
            Just(BinaryFunc::TrimLeading).boxed(),
            Just(BinaryFunc::TrimTrailing).boxed(),
            Just(BinaryFunc::EncodedBytesCharLength).boxed(),
            usize::arbitrary()
                .prop_map(|max_layer| BinaryFunc::ListLengthMax { max_layer })
                .boxed(),
            Just(BinaryFunc::ArrayContains).boxed(),
            Just(BinaryFunc::ArrayLength).boxed(),
            Just(BinaryFunc::ArrayLower).boxed(),
            Just(BinaryFunc::ArrayRemove).boxed(),
            Just(BinaryFunc::ArrayUpper).boxed(),
            Just(BinaryFunc::ArrayArrayConcat).boxed(),
            Just(BinaryFunc::ListListConcat).boxed(),
            Just(BinaryFunc::ListElementConcat).boxed(),
            Just(BinaryFunc::ElementListConcat).boxed(),
            Just(BinaryFunc::ListRemove).boxed(),
            Just(BinaryFunc::DigestString).boxed(),
            Just(BinaryFunc::DigestBytes).boxed(),
            Just(BinaryFunc::MzRenderTypmod).boxed(),
            Just(BinaryFunc::Encode).boxed(),
            Just(BinaryFunc::Decode).boxed(),
            Just(BinaryFunc::LogNumeric).boxed(),
            Just(BinaryFunc::Power).boxed(),
            Just(BinaryFunc::PowerNumeric).boxed(),
            (bool::arbitrary(), mz_repr::arb_range_type())
                .prop_map(|(rev, elem_type)| BinaryFunc::RangeContainsElem { elem_type, rev })
                .boxed(),
            bool::arbitrary()
                .prop_map(|rev| BinaryFunc::RangeContainsRange { rev })
                .boxed(),
            Just(BinaryFunc::RangeOverlaps).boxed(),
            Just(BinaryFunc::RangeAfter).boxed(),
            Just(BinaryFunc::RangeBefore).boxed(),
            Just(BinaryFunc::RangeOverleft).boxed(),
            Just(BinaryFunc::RangeOverright).boxed(),
            Just(BinaryFunc::RangeAdjacent).boxed(),
            Just(BinaryFunc::RangeUnion).boxed(),
            Just(BinaryFunc::RangeIntersection).boxed(),
            Just(BinaryFunc::RangeDifference).boxed(),
            Just(BinaryFunc::ParseIdent).boxed(),
        ])
    }
}

#[sqlfunc(
    sqlname = "||",
    is_infix_op = true,
    output_type = "String",
    propagates_nulls = true,
    is_monotone = (false, true),
)]
fn text_concat_binary<'a>(a: Datum<'a>, b: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    let mut buf = String::new();
    buf.push_str(a.unwrap_str());
    buf.push_str(b.unwrap_str());
    Datum::String(temp_storage.push_string(buf))
}

#[sqlfunc(
    output_type = "String",
    propagates_nulls = true,
    introduces_nulls = false
)]
fn like_escape<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let pattern = a.unwrap_str();
    let escape = like_pattern::EscapeBehavior::from_str(b.unwrap_str())?;
    let normalized = like_pattern::normalize_pattern(pattern, escape)?;
    Ok(Datum::String(temp_storage.push_string(normalized)))
}

fn is_like_match_dynamic<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    case_insensitive: bool,
) -> Result<Datum<'a>, EvalError> {
    let haystack = a.unwrap_str();
    let needle = like_pattern::compile(b.unwrap_str(), case_insensitive)?;
    Ok(Datum::from(needle.is_match(haystack.as_ref())))
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
    if (len * string.len()) > MAX_STRING_BYTES {
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

#[sqlfunc(output_type = "i32", propagates_nulls = true)]
fn position<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let substring: &'a str = a.unwrap_str();
    let string = b.unwrap_str();
    let char_index = string.find(substring);

    if let Some(char_index) = char_index {
        // find the index in char space
        let string_prefix = &string[0..char_index];

        let num_prefix_chars = string_prefix.chars().count();
        let num_prefix_chars = i32::try_from(num_prefix_chars)
            .map_err(|_| EvalError::Int32OutOfRange(num_prefix_chars.to_string().into()))?;

        Ok(Datum::Int32(num_prefix_chars + 1))
    } else {
        Ok(Datum::Int32(0))
    }
}

#[sqlfunc(output_type = "String", propagates_nulls = true)]
fn left<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let string: &'a str = a.unwrap_str();
    let n = i64::from(b.unwrap_int32());

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

    Ok(Datum::String(&string[..end_in_bytes]))
}

#[sqlfunc(output_type = "String", propagates_nulls = true)]
fn right<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let string: &'a str = a.unwrap_str();
    let n = b.unwrap_int32();

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

    Ok(Datum::String(&string[start_in_bytes..]))
}

#[sqlfunc(sqlname = "btrim", output_type = "String", propagates_nulls = true)]
fn trim<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let trim_chars = b.unwrap_str();

    Datum::from(a.unwrap_str().trim_matches(|c| trim_chars.contains(c)))
}

#[sqlfunc(sqlname = "ltrim", output_type = "String", propagates_nulls = true)]
fn trim_leading<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let trim_chars = b.unwrap_str();

    Datum::from(
        a.unwrap_str()
            .trim_start_matches(|c| trim_chars.contains(c)),
    )
}

#[sqlfunc(sqlname = "rtrim", output_type = "String", propagates_nulls = true)]
fn trim_trailing<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let trim_chars = b.unwrap_str();

    Datum::from(a.unwrap_str().trim_end_matches(|c| trim_chars.contains(c)))
}

#[sqlfunc(
    output_type = "Option<i32>",
    is_infix_op = true,
    sqlname = "array_length",
    propagates_nulls = true,
    introduces_nulls = true
)]
fn array_length<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let i = match usize::try_from(b.unwrap_int64()) {
        Ok(0) | Err(_) => return Ok(Datum::Null),
        Ok(n) => n - 1,
    };
    Ok(match a.unwrap_array().dims().into_iter().nth(i) {
        None => Datum::Null,
        Some(dim) => Datum::Int32(
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
fn array_lower<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let i = b.unwrap_int64();
    if i < 1 {
        return Datum::Null;
    }
    match a.unwrap_array().dims().into_iter().nth(i as usize - 1) {
        Some(_) => Datum::Int32(1),
        None => Datum::Null,
    }
}

#[sqlfunc(
    output_type_expr = "input_type_a.scalar_type.without_modifiers().nullable(true)",
    sqlname = "array_remove",
    propagates_nulls = false,
    introduces_nulls = false
)]
fn array_remove<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    if a.is_null() {
        return Ok(a);
    }

    let arr = a.unwrap_array();

    // Zero-dimensional arrays are empty by definition
    if arr.dims().len() == 0 {
        return Ok(a);
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
fn array_upper<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let i = b.unwrap_int64();
    if i < 1 {
        return Ok(Datum::Null);
    }
    Ok(
        match a.unwrap_array().dims().into_iter().nth(i as usize - 1) {
            Some(dim) => Datum::Int32(
                dim.length
                    .try_into()
                    .map_err(|_| EvalError::Int32OutOfRange(dim.length.to_string().into()))?,
            ),
            None => Datum::Null,
        },
    )
}

// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn list_length_max<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    max_layer: usize,
) -> Result<Datum<'a>, EvalError> {
    fn max_len_on_layer<'a>(d: Datum<'a>, on_layer: i64) -> Option<usize> {
        match d {
            Datum::List(i) => {
                let mut i = i.iter();
                if on_layer > 1 {
                    let mut max_len = None;
                    while let Some(Datum::List(i)) = i.next() {
                        max_len =
                            std::cmp::max(max_len_on_layer(Datum::List(i), on_layer - 1), max_len);
                    }
                    max_len
                } else {
                    Some(i.count())
                }
            }
            Datum::Null => None,
            _ => unreachable!(),
        }
    }

    let b = b.unwrap_int64();

    if b as usize > max_layer || b < 1 {
        Err(EvalError::InvalidLayer { max_layer, val: b })
    } else {
        match max_len_on_layer(a, b) {
            Some(l) => match l.try_into() {
                Ok(c) => Ok(Datum::Int32(c)),
                Err(_) => Err(EvalError::Int32OutOfRange(l.to_string().into())),
            },
            None => Ok(Datum::Null),
        }
    }
}

#[sqlfunc(
    output_type = "bool",
    is_infix_op = true,
    sqlname = "array_contains",
    propagates_nulls = true,
    introduces_nulls = false
)]
fn array_contains<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let array = Datum::unwrap_array(&b);
    Datum::from(array.elements().iter().any(|e| e == a))
}

#[sqlfunc(
    output_type = "bool",
    is_infix_op = true,
    sqlname = "@>",
    propagates_nulls = true,
    introduces_nulls = false
)]
fn array_contains_array<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let a = a.unwrap_array().elements();
    let b = b.unwrap_array().elements();

    // NULL is never equal to NULL. If NULL is an element of b, b cannot be contained in a, even if a contains NULL.
    if b.iter().contains(&Datum::Null) {
        Datum::False
    } else {
        b.iter()
            .all(|item_b| a.iter().any(|item_a| item_a == item_b))
            .into()
    }
}

#[sqlfunc(
    output_type = "bool",
    is_infix_op = true,
    sqlname = "<@",
    propagates_nulls = true,
    introduces_nulls = false
)]
#[allow(dead_code)]
fn array_contains_array_rev<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    array_contains_array(a, b)
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
    output_type_expr = "input_type_a.scalar_type.without_modifiers().nullable(true)",
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
fn list_remove<'a>(a: Datum<'a>, b: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    if a.is_null() {
        return a;
    }

    temp_storage.make_datum(|packer| {
        packer.push_list_with(|packer| {
            for elem in a.unwrap_list().iter() {
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
fn digest_string<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let to_digest = a.unwrap_str().as_bytes();
    digest_inner(to_digest, b, temp_storage)
}

#[sqlfunc(
    output_type = "Vec<u8>",
    sqlname = "digest",
    propagates_nulls = true,
    introduces_nulls = false
)]
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
    oid: Datum<'a>,
    typmod: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let oid = oid.unwrap_uint32();
    let typmod = typmod.unwrap_int32();
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
        proptest_binary(BinaryFunc::SubInt32, &arena, &i32_datums, &i32_datums);
        proptest_binary(BinaryFunc::MulInt32, &arena, &i32_datums, &i32_datums);
        proptest_binary(BinaryFunc::DivInt32, &arena, &i32_datums, &i32_datums);
        proptest_binary(BinaryFunc::TextConcat, &arena, &str_datums, &str_datums);
        proptest_binary(BinaryFunc::Left, &arena, &str_datums, &i32_datums);
    }
}
