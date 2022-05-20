// Copyright Materialize, Inc. and contributors. All rights reserved.
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
use std::iter;
use std::str;
use std::str::FromStr;

use ::encoding::label::encoding_from_whatwg_label;
use ::encoding::DecoderTrap;
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use fallible_iterator::FallibleIterator;
use hmac::{Hmac, Mac};
use itertools::Itertools;
use md5::{Digest, Md5};
use num::traits::CheckedNeg;
use proptest_derive::Arbitrary;
use regex::RegexBuilder;
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Sha224, Sha256, Sha384, Sha512};

use mz_lowertest::MzReflect;
use mz_ore::cast;
use mz_ore::collections::CollectionExt;
use mz_ore::fmt::FormatBuffer;
use mz_ore::option::OptionExt;
use mz_pgrepr::Type;
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::datetime::Timezone;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::adt::numeric::{self, DecimalLike, Numeric, NumericMaxScale};
use mz_repr::adt::regex::any_regex;
use mz_repr::chrono::any_naive_datetime;
use mz_repr::proto::newapi::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::proto::TryIntoIfSome;
use mz_repr::{strconv, ColumnName, ColumnType, Datum, DatumType, Row, RowArena, ScalarType};

use crate::scalar::func::format::DateTimeFormat;
use crate::scalar::{
    ProtoBinaryFunc, ProtoUnaryFunc, ProtoUnmaterializableFunc, ProtoVariadicFunc,
};
use crate::{like_pattern, EvalError, MirScalarExpr};

#[macro_use]
mod macros;
mod encoding;
mod format;
pub(crate) mod impls;

pub use impls::*;

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub enum UnmaterializableFunc {
    CurrentDatabase,
    CurrentSchemasWithSystem,
    CurrentSchemasWithoutSystem,
    CurrentTimestamp,
    CurrentUser,
    MzClusterId,
    MzLogicalTimestamp,
    MzSessionId,
    MzUptime,
    MzVersion,
    PgBackendPid,
    PgPostmasterStartTime,
    Version,
}

impl UnmaterializableFunc {
    pub fn output_type(&self) -> ColumnType {
        match self {
            UnmaterializableFunc::CurrentDatabase => ScalarType::String.nullable(false),
            // TODO: The `CurrentSchemas` functions should should return name[].
            UnmaterializableFunc::CurrentSchemasWithSystem => {
                ScalarType::Array(Box::new(ScalarType::String)).nullable(false)
            }
            UnmaterializableFunc::CurrentSchemasWithoutSystem => {
                ScalarType::Array(Box::new(ScalarType::String)).nullable(false)
            }
            UnmaterializableFunc::CurrentTimestamp => ScalarType::TimestampTz.nullable(false),
            UnmaterializableFunc::CurrentUser => ScalarType::String.nullable(false),
            UnmaterializableFunc::MzClusterId => ScalarType::Uuid.nullable(false),
            UnmaterializableFunc::MzLogicalTimestamp => ScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            }
            .nullable(false),
            UnmaterializableFunc::MzSessionId => ScalarType::Uuid.nullable(false),
            UnmaterializableFunc::MzUptime => ScalarType::Interval.nullable(true),
            UnmaterializableFunc::MzVersion => ScalarType::String.nullable(false),
            UnmaterializableFunc::PgBackendPid => ScalarType::Int32.nullable(false),
            UnmaterializableFunc::PgPostmasterStartTime => ScalarType::TimestampTz.nullable(false),
            UnmaterializableFunc::Version => ScalarType::String.nullable(false),
        }
    }
}

impl fmt::Display for UnmaterializableFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UnmaterializableFunc::CurrentDatabase => f.write_str("current_database"),
            UnmaterializableFunc::CurrentSchemasWithSystem => f.write_str("current_schemas(true)"),
            UnmaterializableFunc::CurrentSchemasWithoutSystem => {
                f.write_str("current_schemas(false)")
            }
            UnmaterializableFunc::CurrentTimestamp => f.write_str("current_timestamp"),
            UnmaterializableFunc::CurrentUser => f.write_str("current_user"),
            UnmaterializableFunc::MzClusterId => f.write_str("mz_cluster_id"),
            UnmaterializableFunc::MzLogicalTimestamp => f.write_str("mz_logical_timestamp"),
            UnmaterializableFunc::MzSessionId => f.write_str("mz_session_id"),
            UnmaterializableFunc::MzUptime => f.write_str("mz_uptime"),
            UnmaterializableFunc::MzVersion => f.write_str("mz_version"),
            UnmaterializableFunc::PgBackendPid => f.write_str("pg_backend_pid"),
            UnmaterializableFunc::PgPostmasterStartTime => f.write_str("pg_postmaster_start_time"),
            UnmaterializableFunc::Version => f.write_str("version"),
        }
    }
}

impl From<&UnmaterializableFunc> for ProtoUnmaterializableFunc {
    fn from(func: &UnmaterializableFunc) -> Self {
        use crate::scalar::proto_unmaterializable_func::Kind::*;
        let kind = match func {
            UnmaterializableFunc::CurrentDatabase => CurrentDatabase(()),
            UnmaterializableFunc::CurrentSchemasWithSystem => CurrentSchemasWithSystem(()),
            UnmaterializableFunc::CurrentSchemasWithoutSystem => CurrentSchemasWithoutSystem(()),
            UnmaterializableFunc::CurrentTimestamp => CurrentTimestamp(()),
            UnmaterializableFunc::CurrentUser => CurrentUser(()),
            UnmaterializableFunc::MzClusterId => MzClusterId(()),
            UnmaterializableFunc::MzLogicalTimestamp => MzLogicalTimestamp(()),
            UnmaterializableFunc::MzSessionId => MzSessionId(()),
            UnmaterializableFunc::MzUptime => MzUptime(()),
            UnmaterializableFunc::MzVersion => MzVersion(()),
            UnmaterializableFunc::PgBackendPid => PgBackendPid(()),
            UnmaterializableFunc::PgPostmasterStartTime => PgPostmasterStartTime(()),
            UnmaterializableFunc::Version => Version(()),
        };
        ProtoUnmaterializableFunc { kind: Some(kind) }
    }
}

impl TryFrom<ProtoUnmaterializableFunc> for UnmaterializableFunc {
    type Error = TryFromProtoError;

    fn try_from(func: ProtoUnmaterializableFunc) -> Result<Self, Self::Error> {
        use crate::scalar::proto_unmaterializable_func::Kind::*;
        if let Some(kind) = func.kind {
            match kind {
                CurrentDatabase(()) => Ok(UnmaterializableFunc::CurrentDatabase),
                CurrentSchemasWithSystem(()) => Ok(UnmaterializableFunc::CurrentSchemasWithSystem),
                CurrentSchemasWithoutSystem(()) => {
                    Ok(UnmaterializableFunc::CurrentSchemasWithoutSystem)
                }
                CurrentTimestamp(()) => Ok(UnmaterializableFunc::CurrentTimestamp),
                CurrentUser(()) => Ok(UnmaterializableFunc::CurrentUser),
                MzClusterId(()) => Ok(UnmaterializableFunc::MzClusterId),
                MzLogicalTimestamp(()) => Ok(UnmaterializableFunc::MzLogicalTimestamp),
                MzSessionId(()) => Ok(UnmaterializableFunc::MzSessionId),
                MzUptime(()) => Ok(UnmaterializableFunc::MzUptime),
                MzVersion(()) => Ok(UnmaterializableFunc::MzVersion),
                PgBackendPid(()) => Ok(UnmaterializableFunc::PgBackendPid),
                PgPostmasterStartTime(()) => Ok(UnmaterializableFunc::PgPostmasterStartTime),
                Version(()) => Ok(UnmaterializableFunc::Version),
            }
        } else {
            Err(TryFromProtoError::missing_field(
                "`ProtoUnmaterializableFunc::kind`",
            ))
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

fn add_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int16()
        .checked_add(b.unwrap_int16())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
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

fn add_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_float32();
    let b = b.unwrap_float32();
    let sum = a + b;
    if sum.is_infinite() && !a.is_infinite() && !b.is_infinite() {
        Err(EvalError::FloatOverflow)
    } else {
        Ok(Datum::from(sum))
    }
}

fn add_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let a = a.unwrap_float64();
    let b = b.unwrap_float64();
    let sum = a + b;
    if sum.is_infinite() && !a.is_infinite() && !b.is_infinite() {
        Err(EvalError::FloatOverflow)
    } else {
        Ok(Datum::from(sum))
    }
}

fn add_timestamplike_interval<'a, T>(a: T, b: Interval) -> Result<Datum<'a>, EvalError>
where
    T: TimestampLike,
{
    let mut dt = a.date_time();
    dt = add_timestamp_months(dt, b.months)?;
    dt = dt
        .checked_add_signed(b.duration_as_chrono())
        .ok_or(EvalError::TimestampOutOfRange)?;
    Ok(T::from_date_time(dt).into())
}

fn sub_timestamplike_interval<'a, T>(a: T, b: Datum) -> Result<Datum<'a>, EvalError>
where
    T: TimestampLike,
{
    neg_interval_inner(b).and_then(|i| add_timestamplike_interval(a, i))
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

fn add_date_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let date = a.unwrap_date();
    let interval = b.unwrap_interval();

    let dt = NaiveDate::from_ymd(date.year(), date.month(), date.day()).and_hms(0, 0, 0);
    let dt = add_timestamp_months(dt, interval.months)?;
    Ok(Datum::Timestamp(
        dt.checked_add_signed(interval.duration_as_chrono())
            .ok_or(EvalError::TimestampOutOfRange)?,
    ))
}

fn add_time_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let time = a.unwrap_time();
    let interval = b.unwrap_interval();
    let (t, _) = time.overflowing_add_signed(interval.duration_as_chrono());
    Datum::Time(t)
}

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
        b = std::cmp::min(
            b,
            (u32::from(numeric::NUMERIC_DATUM_MAX_PRECISION)
                - (numeric::get_precision(&a) - u32::from(numeric::get_scale(&a))))
                as i32,
        );
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

fn convert_from<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    // Convert PostgreSQL-style encoding names[1] to WHATWG-style encoding names[2],
    // which the encoding library uses[3].
    // [1]: https://www.postgresql.org/docs/9.5/multibyte.html
    // [2]: https://encoding.spec.whatwg.org/
    // [3]: https://github.com/lifthrasiir/rust-encoding/blob/4e79c35ab6a351881a86dbff565c4db0085cc113/src/label.rs
    let encoding_name = b.unwrap_str().to_lowercase().replace('_', "-");

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

fn encoded_bytes_char_length<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    // Convert PostgreSQL-style encoding names[1] to WHATWG-style encoding names[2],
    // which the encoding library uses[3].
    // [1]: https://www.postgresql.org/docs/9.5/multibyte.html
    // [2]: https://encoding.spec.whatwg.org/
    // [3]: https://github.com/lifthrasiir/rust-encoding/blob/4e79c35ab6a351881a86dbff565c4db0085cc113/src/label.rs
    let encoding_name = b.unwrap_str().to_lowercase().replace('_', "-");

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

pub fn add_timestamp_months(
    dt: NaiveDateTime,
    mut months: i32,
) -> Result<NaiveDateTime, EvalError> {
    if months == 0 {
        return Ok(dt);
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
    Ok(new_d.and_hms_nano(dt.hour(), dt.minute(), dt.second(), dt.nanosecond()))
}

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

fn add_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_interval()
        .checked_add(&b.unwrap_interval())
        .ok_or(EvalError::IntervalOutOfRange)
        .map(Datum::from)
}

fn bit_and_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int16() & b.unwrap_int16())
}

fn bit_and_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int32() & b.unwrap_int32())
}

fn bit_and_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int64() & b.unwrap_int64())
}

fn bit_or_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int16() | b.unwrap_int16())
}

fn bit_or_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int32() | b.unwrap_int32())
}

fn bit_or_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int64() | b.unwrap_int64())
}

fn bit_xor_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int16() ^ b.unwrap_int16())
}

fn bit_xor_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int32() ^ b.unwrap_int32())
}

fn bit_xor_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    Datum::from(a.unwrap_int64() ^ b.unwrap_int64())
}

fn bit_shift_left_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    // widen to i32 and then cast back to i16 in order emulate the C promotion rules used in by Postgres
    // when the rhs in the 16-31 range, e.g. (1 << 17 should evaluate to 0)
    // see https://github.com/postgres/postgres/blob/REL_14_STABLE/src/backend/utils/adt/int.c#L1460-L1476
    let lhs: i32 = a.unwrap_int16() as i32;
    let rhs: u32 = b.unwrap_int32() as u32;
    Datum::from(lhs.wrapping_shl(rhs) as i16)
}

fn bit_shift_left_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let lhs = a.unwrap_int32();
    let rhs = b.unwrap_int32() as u32;
    Datum::from(lhs.wrapping_shl(rhs))
}

fn bit_shift_left_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let lhs = a.unwrap_int64();
    let rhs = b.unwrap_int32() as u32;
    Datum::from(lhs.wrapping_shl(rhs))
}

fn bit_shift_right_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    // widen to i32 and then cast back to i16 in order emulate the C promotion rules used in by Postgres
    // when the rhs in the 16-31 range, e.g. (-32767 >> 17 should evaluate to -1)
    // see https://github.com/postgres/postgres/blob/REL_14_STABLE/src/backend/utils/adt/int.c#L1460-L1476
    let lhs = a.unwrap_int16() as i32;
    let rhs = b.unwrap_int32() as u32;
    Datum::from(lhs.wrapping_shr(rhs) as i16)
}

fn bit_shift_right_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let lhs = a.unwrap_int32();
    let rhs = b.unwrap_int32() as u32;
    Datum::from(lhs.wrapping_shr(rhs))
}

fn bit_shift_right_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let lhs = a.unwrap_int64();
    let rhs = b.unwrap_int32() as u32;
    Datum::from(lhs.wrapping_shr(rhs))
}

fn sub_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int16()
        .checked_sub(b.unwrap_int16())
        .ok_or(EvalError::NumericFieldOverflow)
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
    b.unwrap_interval()
        .checked_neg()
        .and_then(|b| b.checked_add(&a.unwrap_interval()))
        .ok_or(EvalError::IntervalOutOfRange)
        .map(Datum::from)
}

fn sub_date_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let date = a.unwrap_date();
    let interval = b.unwrap_interval();

    let dt = NaiveDate::from_ymd(date.year(), date.month(), date.day()).and_hms(0, 0, 0);
    let dt = interval
        .months
        .checked_neg()
        .ok_or(EvalError::IntervalOutOfRange)
        .and_then(|months| add_timestamp_months(dt, months))?;

    Ok(Datum::Timestamp(
        dt.checked_sub_signed(interval.duration_as_chrono())
            .ok_or(EvalError::TimestampOutOfRange)?,
    ))
}

fn sub_time_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let time = a.unwrap_time();
    let interval = b.unwrap_interval();
    let (t, _) = time.overflowing_sub_signed(interval.duration_as_chrono());
    Datum::Time(t)
}

fn mul_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int16()
        .checked_mul(b.unwrap_int16())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
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

fn mul_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_interval()
        .checked_mul(b.unwrap_float64())
        .ok_or(EvalError::IntervalOutOfRange)
        .map(Datum::from)
}

fn div_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_int16();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_int16() / b))
    }
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

fn mod_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_int16();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_int16() % b))
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

pub fn neg_interval(a: Datum) -> Result<Datum, EvalError> {
    neg_interval_inner(a).map(Datum::from)
}

fn neg_interval_inner(a: Datum) -> Result<Interval, EvalError> {
    a.unwrap_interval()
        .checked_neg()
        .ok_or(EvalError::IntervalOutOfRange)
}

fn log_guard_numeric(val: &Numeric, function_name: &str) -> Result<(), EvalError> {
    if val.is_negative() {
        return Err(EvalError::NegativeOutOfDomain(function_name.to_owned()));
    }
    if val.is_zero() {
        return Err(EvalError::ZeroOutOfDomain(function_name.to_owned()));
    }
    Ok(())
}

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

        // `reduce` rounds to the the context's final digit when the number of
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

fn power_numeric<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let mut a = a.unwrap_numeric().0;
    let b = b.unwrap_numeric().0;
    if a.is_zero() {
        if b.is_zero() {
            return Ok(Datum::from(Numeric::from(1)));
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

fn to_char_timestamplike<'a, T>(ts: T, format: &str, temp_storage: &'a RowArena) -> Datum<'a>
where
    T: TimestampLike,
{
    let fmt = DateTimeFormat::compile(format);
    Datum::String(temp_storage.push_string(fmt.render(ts)))
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
            (Datum::Numeric(a), Datum::Numeric(b)) => (a == b),
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

/// A timestamp with both a date and a time component, but not necessarily a
/// timezone component.
pub trait TimestampLike:
    Clone
    + PartialOrd
    + std::ops::Add<Duration, Output = Self>
    + std::ops::Sub<Duration, Output = Self>
    + std::ops::Sub<Output = Duration>
    + for<'a> Into<Datum<'a>>
    + for<'a> TryFrom<Datum<'a>, Error = ()>
    + TimeLike
    + DateLike
{
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

    fn extract_epoch<T>(&self) -> T
    where
        T: DecimalLike,
    {
        T::lossy_from(self.timestamp()) + T::from(self.timestamp_subsec_micros()) / T::from(1e6)
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

    /// Return the date and time of the timestamp
    fn date_time(&self) -> NaiveDateTime;

    /// Return the date and time of the timestamp
    fn from_date_time(dt: NaiveDateTime) -> Self;

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

    fn date_time(&self) -> NaiveDateTime {
        self.clone()
    }

    fn from_date_time(dt: NaiveDateTime) -> NaiveDateTime {
        dt
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
        Self::from_date_time(NaiveDateTime::new(date, time))
    }

    fn date(&self) -> NaiveDate {
        self.naive_utc().date()
    }

    fn date_time(&self) -> NaiveDateTime {
        self.naive_utc()
    }

    fn from_date_time(dt: NaiveDateTime) -> Self {
        DateTime::<Utc>::from_utc(dt, Utc)
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

fn date_part_interval<'a, D>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError>
where
    D: DecimalLike + Into<Datum<'static>>,
{
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => Ok(date_part_interval_inner::<D>(units, b.unwrap_interval())?.into()),
        Err(_) => Err(EvalError::UnknownUnits(units.to_owned())),
    }
}

fn date_part_time<'a, D>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError>
where
    D: DecimalLike + Into<Datum<'a>>,
{
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => Ok(date_part_time_inner::<D>(units, b.unwrap_time())?.into()),
        Err(_) => Err(EvalError::UnknownUnits(units.to_owned())),
    }
}

fn date_part_timestamp<'a, T, D>(a: Datum<'a>, ts: T) -> Result<Datum<'a>, EvalError>
where
    T: TimestampLike,
    D: DecimalLike + Into<Datum<'a>>,
{
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => Ok(date_part_timestamp_inner::<_, D>(units, ts)?.into()),
        Err(_) => Err(EvalError::UnknownUnits(units.to_owned())),
    }
}

fn extract_date<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => Ok(extract_date_inner(units, b.unwrap_date())?.into()),
        Err(_) => Err(EvalError::UnknownUnits(units.to_owned())),
    }
}

pub fn date_bin<'a, T>(stride: Interval, source: T, origin: T) -> Result<Datum<'a>, EvalError>
where
    T: TimestampLike,
{
    if stride.months != 0 {
        return Err(EvalError::DateBinOutOfRange(
            "timestamps cannot be binned into intervals containing months or years".to_string(),
        ));
    }

    let stride_ns = match stride.duration_as_chrono().num_nanoseconds() {
        Some(ns) if ns <= 0 => Err(EvalError::DateBinOutOfRange(
            "stride must be greater than zero".to_string(),
        )),
        Some(ns) => Ok(ns),
        None => Err(EvalError::DateBinOutOfRange(format!(
            "stride cannot exceed {}/{} nanoseconds",
            i64::MAX,
            i64::MIN,
        ))),
    }?;

    // Make sure the returned timestamp is at the start of the bin, even if the
    // origin is in the future. We do this here because `T` is not `Copy` and
    // gets moved by its subtraction operation.
    let sub_stride = origin > source;

    let tm_diff = (source - origin.clone()).num_nanoseconds().ok_or_else(|| {
        EvalError::DateBinOutOfRange(
            "source and origin must not differ more than 2^63 nanoseconds".to_string(),
        )
    })?;

    let mut tm_delta = tm_diff - tm_diff % stride_ns;

    if sub_stride {
        tm_delta -= stride_ns;
    }

    let res = origin + Duration::nanoseconds(tm_delta);
    Ok(res.into())
}

fn date_trunc<'a, T>(a: Datum<'a>, ts: T) -> Result<Datum<'a>, EvalError>
where
    T: TimestampLike,
{
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => Ok(date_trunc_inner(units, ts)?.into()),
        Err(_) => Err(EvalError::UnknownUnits(units.to_owned())),
    }
}

fn date_trunc_interval<'a>(a: Datum, b: Datum) -> Result<Datum<'a>, EvalError> {
    let mut interval = b.unwrap_interval();
    let units = a.unwrap_str();
    let dtf = units
        .parse()
        .map_err(|_| EvalError::UnknownUnits(units.to_owned()))?;

    interval
        .truncate_low_fields(dtf, Some(0))
        .expect("truncate_low_fields should not fail with max_precision 0");
    Ok(interval.into())
}

/// Parses a named timezone like `EST` or `America/New_York`, or a fixed-offset timezone like `-05:00`.
pub(crate) fn parse_timezone(tz: &str) -> Result<Timezone, EvalError> {
    tz.parse()
        .map_err(|_| EvalError::InvalidTimezone(tz.to_owned()))
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

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub enum BinaryFunc {
    And,
    Or,
    AddInt16,
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
    AddNumeric,
    BitAndInt16,
    BitAndInt32,
    BitAndInt64,
    BitOrInt16,
    BitOrInt32,
    BitOrInt64,
    BitXorInt16,
    BitXorInt32,
    BitXorInt64,
    BitShiftLeftInt16,
    BitShiftLeftInt32,
    BitShiftLeftInt64,
    BitShiftRightInt16,
    BitShiftRightInt32,
    BitShiftRightInt64,
    SubInt16,
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
    SubNumeric,
    MulInt16,
    MulInt32,
    MulInt64,
    MulFloat32,
    MulFloat64,
    MulNumeric,
    MulInterval,
    DivInt16,
    DivInt32,
    DivInt64,
    DivFloat32,
    DivFloat64,
    DivNumeric,
    DivInterval,
    ModInt16,
    ModInt32,
    ModInt64,
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
    Left,
    Position,
    Right,
    RepeatString,
    Trim,
    TrimLeading,
    TrimTrailing,
    EncodedBytesCharLength,
    ListLengthMax { max_layer: usize },
    ArrayContains,
    ArrayLength,
    ArrayLower,
    ArrayRemove,
    ArrayUpper,
    ArrayArrayConcat,
    ListListConcat,
    ListElementConcat,
    ElementListConcat,
    ListRemove,
    DigestString,
    DigestBytes,
    MzRenderTypmod,
    Encode,
    Decode,
    LogNumeric,
    Power,
    PowerNumeric,
    GetByte,
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
            BinaryFunc::AddInt16 => eager!(add_int16),
            BinaryFunc::AddInt32 => eager!(add_int32),
            BinaryFunc::AddInt64 => eager!(add_int64),
            BinaryFunc::AddFloat32 => eager!(add_float32),
            BinaryFunc::AddFloat64 => eager!(add_float64),
            BinaryFunc::AddTimestampInterval => {
                eager!(|a: Datum, b: Datum| add_timestamplike_interval(
                    a.unwrap_timestamp(),
                    b.unwrap_interval(),
                ))
            }
            BinaryFunc::AddTimestampTzInterval => {
                eager!(|a: Datum, b: Datum| add_timestamplike_interval(
                    a.unwrap_timestamptz(),
                    b.unwrap_interval(),
                ))
            }
            BinaryFunc::AddDateTime => Ok(eager!(add_date_time)),
            BinaryFunc::AddDateInterval => eager!(add_date_interval),
            BinaryFunc::AddTimeInterval => Ok(eager!(add_time_interval)),
            BinaryFunc::AddNumeric => eager!(add_numeric),
            BinaryFunc::AddInterval => eager!(add_interval),
            BinaryFunc::BitAndInt16 => Ok(eager!(bit_and_int16)),
            BinaryFunc::BitAndInt32 => Ok(eager!(bit_and_int32)),
            BinaryFunc::BitAndInt64 => Ok(eager!(bit_and_int64)),
            BinaryFunc::BitOrInt16 => Ok(eager!(bit_or_int16)),
            BinaryFunc::BitOrInt32 => Ok(eager!(bit_or_int32)),
            BinaryFunc::BitOrInt64 => Ok(eager!(bit_or_int64)),
            BinaryFunc::BitXorInt16 => Ok(eager!(bit_xor_int16)),
            BinaryFunc::BitXorInt32 => Ok(eager!(bit_xor_int32)),
            BinaryFunc::BitXorInt64 => Ok(eager!(bit_xor_int64)),
            BinaryFunc::BitShiftLeftInt16 => Ok(eager!(bit_shift_left_int16)),
            BinaryFunc::BitShiftLeftInt32 => Ok(eager!(bit_shift_left_int32)),
            BinaryFunc::BitShiftLeftInt64 => Ok(eager!(bit_shift_left_int64)),
            BinaryFunc::BitShiftRightInt16 => Ok(eager!(bit_shift_right_int16)),
            BinaryFunc::BitShiftRightInt32 => Ok(eager!(bit_shift_right_int32)),
            BinaryFunc::BitShiftRightInt64 => Ok(eager!(bit_shift_right_int64)),
            BinaryFunc::SubInt16 => eager!(sub_int16),
            BinaryFunc::SubInt32 => eager!(sub_int32),
            BinaryFunc::SubInt64 => eager!(sub_int64),
            BinaryFunc::SubFloat32 => eager!(sub_float32),
            BinaryFunc::SubFloat64 => eager!(sub_float64),
            BinaryFunc::SubTimestamp => Ok(eager!(sub_timestamp)),
            BinaryFunc::SubTimestampTz => Ok(eager!(sub_timestamptz)),
            BinaryFunc::SubTimestampInterval => {
                eager!(|a: Datum, b: Datum| sub_timestamplike_interval(a.unwrap_timestamp(), b))
            }
            BinaryFunc::SubTimestampTzInterval => {
                eager!(|a: Datum, b: Datum| sub_timestamplike_interval(a.unwrap_timestamptz(), b))
            }
            BinaryFunc::SubInterval => eager!(sub_interval),
            BinaryFunc::SubDate => Ok(eager!(sub_date)),
            BinaryFunc::SubDateInterval => eager!(sub_date_interval),
            BinaryFunc::SubTime => Ok(eager!(sub_time)),
            BinaryFunc::SubTimeInterval => Ok(eager!(sub_time_interval)),
            BinaryFunc::SubNumeric => eager!(sub_numeric),
            BinaryFunc::MulInt16 => eager!(mul_int16),
            BinaryFunc::MulInt32 => eager!(mul_int32),
            BinaryFunc::MulInt64 => eager!(mul_int64),
            BinaryFunc::MulFloat32 => eager!(mul_float32),
            BinaryFunc::MulFloat64 => eager!(mul_float64),
            BinaryFunc::MulNumeric => eager!(mul_numeric),
            BinaryFunc::MulInterval => eager!(mul_interval),
            BinaryFunc::DivInt16 => eager!(div_int16),
            BinaryFunc::DivInt32 => eager!(div_int32),
            BinaryFunc::DivInt64 => eager!(div_int64),
            BinaryFunc::DivFloat32 => eager!(div_float32),
            BinaryFunc::DivFloat64 => eager!(div_float64),
            BinaryFunc::DivNumeric => eager!(div_numeric),
            BinaryFunc::DivInterval => eager!(div_interval),
            BinaryFunc::ModInt16 => eager!(mod_int16),
            BinaryFunc::ModInt32 => eager!(mod_int32),
            BinaryFunc::ModInt64 => eager!(mod_int64),
            BinaryFunc::ModFloat32 => eager!(mod_float32),
            BinaryFunc::ModFloat64 => eager!(mod_float64),
            BinaryFunc::ModNumeric => eager!(mod_numeric),
            BinaryFunc::Eq => Ok(eager!(eq)),
            BinaryFunc::NotEq => Ok(eager!(not_eq)),
            BinaryFunc::Lt => Ok(eager!(lt)),
            BinaryFunc::Lte => Ok(eager!(lte)),
            BinaryFunc::Gt => Ok(eager!(gt)),
            BinaryFunc::Gte => Ok(eager!(gte)),
            BinaryFunc::LikeEscape => eager!(like_escape, temp_storage),
            BinaryFunc::IsLikeMatch { case_insensitive } => {
                eager!(is_like_match_dynamic, *case_insensitive)
            }
            BinaryFunc::IsRegexpMatch { case_insensitive } => {
                eager!(is_regexp_match_dynamic, *case_insensitive)
            }
            BinaryFunc::ToCharTimestamp => Ok(eager!(|a: Datum, b: Datum| to_char_timestamplike(
                a.unwrap_timestamp(),
                b.unwrap_str(),
                temp_storage
            ))),
            BinaryFunc::ToCharTimestampTz => {
                Ok(eager!(|a: Datum, b: Datum| to_char_timestamplike(
                    a.unwrap_timestamptz(),
                    b.unwrap_str(),
                    temp_storage
                )))
            }
            BinaryFunc::DateBinTimestamp => {
                eager!(|a: Datum, b: Datum| date_bin(
                    a.unwrap_interval(),
                    b.unwrap_timestamp(),
                    NaiveDateTime::from_timestamp(0, 0)
                ))
            }
            BinaryFunc::DateBinTimestampTz => {
                eager!(|a: Datum, b: Datum| date_bin(
                    a.unwrap_interval(),
                    b.unwrap_timestamptz(),
                    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc)
                ))
            }
            BinaryFunc::ExtractInterval => {
                eager!(date_part_interval::<Numeric>)
            }
            BinaryFunc::ExtractTime => {
                eager!(date_part_time::<Numeric>)
            }
            BinaryFunc::ExtractTimestamp => {
                eager!(|a, b: Datum| date_part_timestamp::<_, Numeric>(a, b.unwrap_timestamp()))
            }
            BinaryFunc::ExtractTimestampTz => {
                eager!(|a, b: Datum| date_part_timestamp::<_, Numeric>(a, b.unwrap_timestamptz()))
            }
            BinaryFunc::ExtractDate => {
                eager!(extract_date)
            }
            BinaryFunc::DatePartInterval => {
                eager!(date_part_interval::<f64>)
            }
            BinaryFunc::DatePartTime => {
                eager!(date_part_time::<f64>)
            }
            BinaryFunc::DatePartTimestamp => {
                eager!(|a, b: Datum| date_part_timestamp::<_, f64>(a, b.unwrap_timestamp()))
            }
            BinaryFunc::DatePartTimestampTz => {
                eager!(|a, b: Datum| date_part_timestamp::<_, f64>(a, b.unwrap_timestamptz()))
            }
            BinaryFunc::DateTruncTimestamp => {
                eager!(|a, b: Datum| date_trunc(a, b.unwrap_timestamp()))
            }
            BinaryFunc::DateTruncInterval => {
                eager!(date_trunc_interval)
            }
            BinaryFunc::DateTruncTimestampTz => {
                eager!(|a, b: Datum| date_trunc(a, b.unwrap_timestamptz()))
            }
            BinaryFunc::TimezoneTimestamp => {
                eager!(|a: Datum, b: Datum| parse_timezone(a.unwrap_str())
                    .and_then(|tz| Ok(timezone_timestamp(tz, b.unwrap_timestamp())?.into())))
            }
            BinaryFunc::TimezoneTimestampTz => {
                eager!(|a: Datum, b: Datum| parse_timezone(a.unwrap_str())
                    .map(|tz| timezone_timestamptz(tz, b.unwrap_timestamptz()).into()))
            }
            BinaryFunc::TimezoneTime { wall_time } => {
                eager!(
                    |a: Datum, b: Datum| parse_timezone(a.unwrap_str()).map(|tz| timezone_time(
                        tz,
                        b.unwrap_time(),
                        wall_time
                    )
                    .into())
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
            BinaryFunc::RoundNumeric => eager!(round_numeric_binary),
            BinaryFunc::ConvertFrom => eager!(convert_from),
            BinaryFunc::Encode => eager!(encode, temp_storage),
            BinaryFunc::Decode => eager!(decode, temp_storage),
            BinaryFunc::Left => eager!(left),
            BinaryFunc::Position => eager!(position),
            BinaryFunc::Right => eager!(right),
            BinaryFunc::Trim => Ok(eager!(trim)),
            BinaryFunc::TrimLeading => Ok(eager!(trim_leading)),
            BinaryFunc::TrimTrailing => Ok(eager!(trim_trailing)),
            BinaryFunc::EncodedBytesCharLength => eager!(encoded_bytes_char_length),
            BinaryFunc::ListLengthMax { max_layer } => eager!(list_length_max, *max_layer),
            BinaryFunc::ArrayLength => eager!(array_length),
            BinaryFunc::ArrayContains => Ok(eager!(array_contains)),
            BinaryFunc::ArrayLower => Ok(eager!(array_lower)),
            BinaryFunc::ArrayRemove => eager!(array_remove, temp_storage),
            BinaryFunc::ArrayUpper => eager!(array_upper),
            BinaryFunc::ArrayArrayConcat => eager!(array_array_concat, temp_storage),
            BinaryFunc::ListListConcat => Ok(eager!(list_list_concat, temp_storage)),
            BinaryFunc::ListElementConcat => Ok(eager!(list_element_concat, temp_storage)),
            BinaryFunc::ElementListConcat => Ok(eager!(element_list_concat, temp_storage)),
            BinaryFunc::ListRemove => Ok(eager!(list_remove, temp_storage)),
            BinaryFunc::DigestString => eager!(digest_string, temp_storage),
            BinaryFunc::DigestBytes => eager!(digest_bytes, temp_storage),
            BinaryFunc::MzRenderTypmod => eager!(mz_render_typmod, temp_storage),
            BinaryFunc::LogNumeric => eager!(log_base_numeric),
            BinaryFunc::Power => eager!(power),
            BinaryFunc::PowerNumeric => eager!(power_numeric),
            BinaryFunc::RepeatString => eager!(repeat_string, temp_storage),
            BinaryFunc::GetByte => eager!(get_byte),
        }
    }

    pub fn output_type(&self, input1_type: ColumnType, input2_type: ColumnType) -> ColumnType {
        use BinaryFunc::*;
        let in_nullable = input1_type.nullable || input2_type.nullable;
        match self {
            And | Or | Eq | NotEq | Lt | Lte | Gt | Gte | ArrayContains => {
                ScalarType::Bool.nullable(in_nullable)
            }

            IsLikeMatch { .. } | IsRegexpMatch { .. } => {
                // The output can be null if the pattern is invalid.
                ScalarType::Bool.nullable(true)
            }

            ToCharTimestamp | ToCharTimestampTz | ConvertFrom | Left | Right | Trim
            | TrimLeading | TrimTrailing | LikeEscape => ScalarType::String.nullable(in_nullable),

            AddInt16 | SubInt16 | MulInt16 | DivInt16 | ModInt16 | BitAndInt16 | BitOrInt16
            | BitXorInt16 | BitShiftLeftInt16 | BitShiftRightInt16 => {
                ScalarType::Int16.nullable(in_nullable)
            }

            AddInt32
            | SubInt32
            | MulInt32
            | DivInt32
            | ModInt32
            | BitAndInt32
            | BitOrInt32
            | BitXorInt32
            | BitShiftLeftInt32
            | BitShiftRightInt32
            | EncodedBytesCharLength
            | SubDate => ScalarType::Int32.nullable(in_nullable),

            AddInt64 | SubInt64 | MulInt64 | DivInt64 | ModInt64 | BitAndInt64 | BitOrInt64
            | BitXorInt64 | BitShiftLeftInt64 | BitShiftRightInt64 => {
                ScalarType::Int64.nullable(in_nullable)
            }

            AddFloat32 | SubFloat32 | MulFloat32 | DivFloat32 | ModFloat32 => {
                ScalarType::Float32.nullable(in_nullable)
            }

            AddFloat64 | SubFloat64 | MulFloat64 | DivFloat64 | ModFloat64 => {
                ScalarType::Float64.nullable(in_nullable)
            }

            AddInterval | SubInterval | SubTimestamp | SubTimestampTz | MulInterval
            | DivInterval => ScalarType::Interval.nullable(in_nullable),

            AddTimestampInterval
            | SubTimestampInterval
            | AddTimestampTzInterval
            | SubTimestampTzInterval
            | AddTimeInterval
            | SubTimeInterval => input1_type,

            AddDateInterval | SubDateInterval | AddDateTime | DateBinTimestamp
            | DateTruncTimestamp => ScalarType::Timestamp.nullable(true),

            DateTruncInterval => ScalarType::Interval.nullable(true),

            TimezoneTimestampTz | TimezoneIntervalTimestampTz => {
                ScalarType::Timestamp.nullable(in_nullable)
            }

            ExtractInterval | ExtractTime | ExtractTimestamp | ExtractTimestampTz | ExtractDate => {
                ScalarType::Numeric { max_scale: None }.nullable(true)
            }

            DatePartInterval | DatePartTime | DatePartTimestamp | DatePartTimestampTz => {
                ScalarType::Float64.nullable(true)
            }

            DateBinTimestampTz | DateTruncTimestampTz => ScalarType::TimestampTz.nullable(true),

            TimezoneTimestamp | TimezoneIntervalTimestamp => {
                ScalarType::TimestampTz.nullable(in_nullable)
            }

            TimezoneTime { .. } | TimezoneIntervalTime => ScalarType::Time.nullable(in_nullable),

            SubTime => ScalarType::Interval.nullable(true),

            MzRenderTypmod | TextConcat => ScalarType::String.nullable(in_nullable),

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

            ArrayLength | ArrayLower | ArrayUpper => ScalarType::Int32.nullable(true),

            ListLengthMax { .. } => ScalarType::Int32.nullable(true),

            ArrayArrayConcat | ArrayRemove | ListListConcat | ListElementConcat | ListRemove => {
                input1_type.scalar_type.without_modifiers().nullable(true)
            }

            ElementListConcat => input2_type.scalar_type.without_modifiers().nullable(true),

            DigestString | DigestBytes => ScalarType::Bytes.nullable(true),
            Position => ScalarType::Int32.nullable(in_nullable),
            Encode => ScalarType::String.nullable(in_nullable),
            Decode => ScalarType::Bytes.nullable(in_nullable),
            Power => ScalarType::Float64.nullable(in_nullable),
            RepeatString => input1_type.scalar_type.nullable(in_nullable),

            AddNumeric | DivNumeric | LogNumeric | ModNumeric | MulNumeric | PowerNumeric
            | RoundNumeric | SubNumeric => {
                ScalarType::Numeric { max_scale: None }.nullable(in_nullable)
            }

            GetByte => ScalarType::Int32.nullable(in_nullable),
        }
    }

    /// Whether the function output is NULL if any of its inputs are NULL.
    pub fn propagates_nulls(&self) -> bool {
        !matches!(
            self,
            BinaryFunc::And
                | BinaryFunc::Or
                | BinaryFunc::ArrayArrayConcat
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
        !matches!(
            self,
            And | Or
                | Eq
                | NotEq
                | Lt
                | Lte
                | Gt
                | Gte
                | AddInt16
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
                | BitAndInt16
                | BitAndInt32
                | BitAndInt64
                | BitOrInt16
                | BitOrInt32
                | BitOrInt64
                | BitXorInt16
                | BitXorInt32
                | BitXorInt64
                | BitShiftLeftInt16
                | BitShiftLeftInt32
                | BitShiftLeftInt64
                | BitShiftRightInt16
                | BitShiftRightInt32
                | BitShiftRightInt64
                | SubInterval
                | MulInterval
                | DivInterval
                | AddNumeric
                | SubInt16
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
                | SubNumeric
                | MulInt16
                | MulInt32
                | MulInt64
                | MulFloat32
                | MulFloat64
                | MulNumeric
                | DivInt16
                | DivInt32
                | DivInt64
                | DivFloat32
                | DivFloat64
                | ModInt16
                | ModInt32
                | ModInt64
                | ModFloat32
                | ModFloat64
                | ModNumeric
        )
    }

    pub fn is_infix_op(&self) -> bool {
        use BinaryFunc::*;
        match self {
            And
            | Or
            | AddInt16
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
            | BitAndInt16
            | BitAndInt32
            | BitAndInt64
            | BitOrInt16
            | BitOrInt32
            | BitOrInt64
            | BitXorInt16
            | BitXorInt32
            | BitXorInt64
            | BitShiftLeftInt16
            | BitShiftLeftInt32
            | BitShiftLeftInt64
            | BitShiftRightInt16
            | BitShiftRightInt32
            | BitShiftRightInt64
            | SubInterval
            | MulInterval
            | DivInterval
            | AddNumeric
            | SubInt16
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
            | SubNumeric
            | MulInt16
            | MulInt32
            | MulInt64
            | MulFloat32
            | MulFloat64
            | MulNumeric
            | DivInt16
            | DivInt32
            | DivInt64
            | DivFloat32
            | DivFloat64
            | DivNumeric
            | ModInt16
            | ModInt32
            | ModInt64
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
            | IsLikeMatch { .. }
            | IsRegexpMatch { .. }
            | ArrayContains
            | ArrayLength
            | ArrayLower
            | ArrayUpper
            | ArrayArrayConcat
            | ListListConcat
            | ListElementConcat
            | ElementListConcat => true,
            ToCharTimestamp
            | ToCharTimestampTz
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
            | TimezoneTime { .. }
            | TimezoneIntervalTimestamp
            | TimezoneIntervalTimestampTz
            | TimezoneIntervalTime
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
            | ArrayRemove
            | ListRemove
            | LikeEscape
            | GetByte => false,
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
            BinaryFunc::AddInt16 => f.write_str("+"),
            BinaryFunc::AddInt32 => f.write_str("+"),
            BinaryFunc::AddInt64 => f.write_str("+"),
            BinaryFunc::AddFloat32 => f.write_str("+"),
            BinaryFunc::AddFloat64 => f.write_str("+"),
            BinaryFunc::AddNumeric => f.write_str("+"),
            BinaryFunc::AddInterval => f.write_str("+"),
            BinaryFunc::AddTimestampInterval => f.write_str("+"),
            BinaryFunc::AddTimestampTzInterval => f.write_str("+"),
            BinaryFunc::AddDateTime => f.write_str("+"),
            BinaryFunc::AddDateInterval => f.write_str("+"),
            BinaryFunc::AddTimeInterval => f.write_str("+"),
            BinaryFunc::BitAndInt16 => f.write_str("&"),
            BinaryFunc::BitAndInt32 => f.write_str("&"),
            BinaryFunc::BitAndInt64 => f.write_str("&"),
            BinaryFunc::BitOrInt16 => f.write_str("|"),
            BinaryFunc::BitOrInt32 => f.write_str("|"),
            BinaryFunc::BitOrInt64 => f.write_str("|"),
            BinaryFunc::BitXorInt16 => f.write_str("#"),
            BinaryFunc::BitXorInt32 => f.write_str("#"),
            BinaryFunc::BitXorInt64 => f.write_str("#"),
            BinaryFunc::BitShiftLeftInt16 => f.write_str("<<"),
            BinaryFunc::BitShiftLeftInt32 => f.write_str("<<"),
            BinaryFunc::BitShiftLeftInt64 => f.write_str("<<"),
            BinaryFunc::BitShiftRightInt16 => f.write_str(">>"),
            BinaryFunc::BitShiftRightInt32 => f.write_str(">>"),
            BinaryFunc::BitShiftRightInt64 => f.write_str(">>"),
            BinaryFunc::SubInt16 => f.write_str("-"),
            BinaryFunc::SubInt32 => f.write_str("-"),
            BinaryFunc::SubInt64 => f.write_str("-"),
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
            BinaryFunc::MulFloat32 => f.write_str("*"),
            BinaryFunc::MulFloat64 => f.write_str("*"),
            BinaryFunc::MulNumeric => f.write_str("*"),
            BinaryFunc::MulInterval => f.write_str("*"),
            BinaryFunc::DivInt16 => f.write_str("/"),
            BinaryFunc::DivInt32 => f.write_str("/"),
            BinaryFunc::DivInt64 => f.write_str("/"),
            BinaryFunc::DivFloat32 => f.write_str("/"),
            BinaryFunc::DivFloat64 => f.write_str("/"),
            BinaryFunc::DivNumeric => f.write_str("/"),
            BinaryFunc::DivInterval => f.write_str("/"),
            BinaryFunc::ModInt16 => f.write_str("%"),
            BinaryFunc::ModInt32 => f.write_str("%"),
            BinaryFunc::ModInt64 => f.write_str("%"),
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
            BinaryFunc::ArrayLength => f.write_str("array_length"),
            BinaryFunc::ArrayLower => f.write_str("array_lower"),
            BinaryFunc::ArrayRemove => f.write_str("array_remove"),
            BinaryFunc::ArrayUpper => f.write_str("array_upper"),
            BinaryFunc::ArrayArrayConcat => f.write_str("||"),
            BinaryFunc::ListListConcat => f.write_str("||"),
            BinaryFunc::ListElementConcat => f.write_str("||"),
            BinaryFunc::ElementListConcat => f.write_str("||"),
            BinaryFunc::ListRemove => f.write_str("list_remove"),
            BinaryFunc::DigestString | BinaryFunc::DigestBytes => f.write_str("digest"),
            BinaryFunc::MzRenderTypmod => f.write_str("mz_render_typmod"),
            BinaryFunc::Encode => f.write_str("encode"),
            BinaryFunc::Decode => f.write_str("decode"),
            BinaryFunc::LogNumeric => f.write_str("log"),
            BinaryFunc::Power => f.write_str("power"),
            BinaryFunc::PowerNumeric => f.write_str("power_numeric"),
            BinaryFunc::RepeatString => f.write_str("repeat"),
            BinaryFunc::GetByte => f.write_str("get_byte"),
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
        prop_oneof![
            Just(BinaryFunc::And),
            Just(BinaryFunc::Or),
            Just(BinaryFunc::AddInt16),
            Just(BinaryFunc::AddInt32),
            Just(BinaryFunc::AddInt64),
            Just(BinaryFunc::AddFloat32),
            Just(BinaryFunc::AddFloat64),
            Just(BinaryFunc::AddInterval),
            Just(BinaryFunc::AddTimestampInterval),
            Just(BinaryFunc::AddTimestampTzInterval),
            Just(BinaryFunc::AddDateInterval),
            Just(BinaryFunc::AddDateTime),
            Just(BinaryFunc::AddTimeInterval),
            Just(BinaryFunc::AddNumeric),
            Just(BinaryFunc::BitAndInt16),
            Just(BinaryFunc::BitAndInt32),
            Just(BinaryFunc::BitAndInt64),
            Just(BinaryFunc::BitOrInt16),
            Just(BinaryFunc::BitOrInt32),
            Just(BinaryFunc::BitOrInt64),
            Just(BinaryFunc::BitXorInt16),
            Just(BinaryFunc::BitXorInt32),
            Just(BinaryFunc::BitXorInt64),
            Just(BinaryFunc::BitShiftLeftInt16),
            Just(BinaryFunc::BitShiftLeftInt32),
            Just(BinaryFunc::BitShiftLeftInt64),
            Just(BinaryFunc::BitShiftRightInt16),
            Just(BinaryFunc::BitShiftRightInt32),
            Just(BinaryFunc::BitShiftRightInt64),
            Just(BinaryFunc::SubInt16),
            Just(BinaryFunc::SubInt32),
            Just(BinaryFunc::SubInt64),
            Just(BinaryFunc::SubFloat32),
            Just(BinaryFunc::SubFloat64),
            Just(BinaryFunc::SubInterval),
            Just(BinaryFunc::SubTimestamp),
            Just(BinaryFunc::SubTimestampTz),
            Just(BinaryFunc::SubTimestampInterval),
            Just(BinaryFunc::SubTimestampTzInterval),
            Just(BinaryFunc::SubDate),
            Just(BinaryFunc::SubDateInterval),
            Just(BinaryFunc::SubTime),
            Just(BinaryFunc::SubTimeInterval),
            Just(BinaryFunc::SubNumeric),
            Just(BinaryFunc::MulInt16),
            Just(BinaryFunc::MulInt32),
            Just(BinaryFunc::MulInt64),
            Just(BinaryFunc::MulFloat32),
            Just(BinaryFunc::MulFloat64),
            Just(BinaryFunc::MulNumeric),
            Just(BinaryFunc::MulInterval),
            Just(BinaryFunc::DivInt16),
            Just(BinaryFunc::DivInt32),
            Just(BinaryFunc::DivInt64),
            Just(BinaryFunc::DivFloat32),
            Just(BinaryFunc::DivFloat64),
            Just(BinaryFunc::DivNumeric),
            Just(BinaryFunc::DivInterval),
            Just(BinaryFunc::ModInt16),
            Just(BinaryFunc::ModInt32),
            Just(BinaryFunc::ModInt64),
            Just(BinaryFunc::ModFloat32),
            Just(BinaryFunc::ModFloat64),
            Just(BinaryFunc::ModNumeric),
            Just(BinaryFunc::RoundNumeric),
            Just(BinaryFunc::Eq),
            Just(BinaryFunc::NotEq),
            Just(BinaryFunc::Lt),
            Just(BinaryFunc::Lte),
            Just(BinaryFunc::Gt),
            Just(BinaryFunc::Gte),
            Just(BinaryFunc::LikeEscape),
            bool::arbitrary()
                .prop_map(|case_insensitive| BinaryFunc::IsLikeMatch { case_insensitive }),
            bool::arbitrary()
                .prop_map(|case_insensitive| BinaryFunc::IsRegexpMatch { case_insensitive }),
            Just(BinaryFunc::ToCharTimestamp),
            Just(BinaryFunc::ToCharTimestampTz),
            Just(BinaryFunc::DateBinTimestamp),
            Just(BinaryFunc::DateBinTimestampTz),
            Just(BinaryFunc::ExtractInterval),
            Just(BinaryFunc::ExtractTime),
            Just(BinaryFunc::ExtractTimestamp),
            Just(BinaryFunc::ExtractTimestampTz),
            Just(BinaryFunc::ExtractDate),
            Just(BinaryFunc::DatePartInterval),
            Just(BinaryFunc::DatePartTime),
            Just(BinaryFunc::DatePartTimestamp),
            Just(BinaryFunc::DatePartTimestampTz),
            Just(BinaryFunc::DateTruncTimestamp),
            Just(BinaryFunc::DateTruncTimestampTz),
            Just(BinaryFunc::DateTruncInterval),
            Just(BinaryFunc::TimezoneTimestamp),
            Just(BinaryFunc::TimezoneTimestampTz),
            any_naive_datetime().prop_map(|wall_time| BinaryFunc::TimezoneTime { wall_time }),
            Just(BinaryFunc::TimezoneIntervalTimestamp),
            Just(BinaryFunc::TimezoneIntervalTimestampTz),
            Just(BinaryFunc::TimezoneIntervalTime),
            Just(BinaryFunc::TextConcat),
            bool::arbitrary().prop_map(|stringify| BinaryFunc::JsonbGetInt64 { stringify }),
            bool::arbitrary().prop_map(|stringify| BinaryFunc::JsonbGetString { stringify }),
            bool::arbitrary().prop_map(|stringify| BinaryFunc::JsonbGetPath { stringify }),
            Just(BinaryFunc::JsonbContainsString),
            Just(BinaryFunc::JsonbConcat),
            Just(BinaryFunc::JsonbContainsJsonb),
            Just(BinaryFunc::JsonbDeleteInt64),
            Just(BinaryFunc::JsonbDeleteString),
            Just(BinaryFunc::MapContainsKey),
            Just(BinaryFunc::MapGetValue),
            Just(BinaryFunc::MapGetValues),
            Just(BinaryFunc::MapContainsAllKeys),
            Just(BinaryFunc::MapContainsAnyKeys),
            Just(BinaryFunc::MapContainsMap),
            Just(BinaryFunc::ConvertFrom),
            Just(BinaryFunc::Left),
            Just(BinaryFunc::Position),
            Just(BinaryFunc::Right),
            Just(BinaryFunc::RepeatString),
            Just(BinaryFunc::Trim),
            Just(BinaryFunc::TrimLeading),
            Just(BinaryFunc::TrimTrailing),
            Just(BinaryFunc::EncodedBytesCharLength),
            usize::arbitrary().prop_map(|max_layer| BinaryFunc::ListLengthMax { max_layer }),
            Just(BinaryFunc::ArrayContains),
            Just(BinaryFunc::ArrayLength),
            Just(BinaryFunc::ArrayLower),
            Just(BinaryFunc::ArrayRemove),
            Just(BinaryFunc::ArrayUpper),
            Just(BinaryFunc::ArrayArrayConcat),
            Just(BinaryFunc::ListListConcat),
            Just(BinaryFunc::ListElementConcat),
            Just(BinaryFunc::ElementListConcat),
            Just(BinaryFunc::ListRemove),
            Just(BinaryFunc::DigestString),
            Just(BinaryFunc::DigestBytes),
            Just(BinaryFunc::MzRenderTypmod),
            Just(BinaryFunc::Encode),
            Just(BinaryFunc::Decode),
            Just(BinaryFunc::LogNumeric),
            Just(BinaryFunc::Power),
            Just(BinaryFunc::PowerNumeric),
        ]
    }
}

impl From<&BinaryFunc> for ProtoBinaryFunc {
    fn from(func: &BinaryFunc) -> Self {
        use crate::scalar::proto_binary_func::Kind::*;
        let kind = match func {
            BinaryFunc::And => And(()),
            BinaryFunc::Or => Or(()),
            BinaryFunc::AddInt16 => AddInt16(()),
            BinaryFunc::AddInt32 => AddInt32(()),
            BinaryFunc::AddInt64 => AddInt64(()),
            BinaryFunc::AddFloat32 => AddFloat32(()),
            BinaryFunc::AddFloat64 => AddFloat64(()),
            BinaryFunc::AddInterval => AddInterval(()),
            BinaryFunc::AddTimestampInterval => AddTimestampInterval(()),
            BinaryFunc::AddTimestampTzInterval => AddTimestampTzInterval(()),
            BinaryFunc::AddDateInterval => AddDateInterval(()),
            BinaryFunc::AddDateTime => AddDateTime(()),
            BinaryFunc::AddTimeInterval => AddTimeInterval(()),
            BinaryFunc::AddNumeric => AddNumeric(()),
            BinaryFunc::BitAndInt16 => BitAndInt16(()),
            BinaryFunc::BitAndInt32 => BitAndInt32(()),
            BinaryFunc::BitAndInt64 => BitAndInt64(()),
            BinaryFunc::BitOrInt16 => BitOrInt16(()),
            BinaryFunc::BitOrInt32 => BitOrInt32(()),
            BinaryFunc::BitOrInt64 => BitOrInt64(()),
            BinaryFunc::BitXorInt16 => BitXorInt16(()),
            BinaryFunc::BitXorInt32 => BitXorInt32(()),
            BinaryFunc::BitXorInt64 => BitXorInt64(()),
            BinaryFunc::BitShiftLeftInt16 => BitShiftLeftInt16(()),
            BinaryFunc::BitShiftLeftInt32 => BitShiftLeftInt32(()),
            BinaryFunc::BitShiftLeftInt64 => BitShiftLeftInt64(()),
            BinaryFunc::BitShiftRightInt16 => BitShiftRightInt16(()),
            BinaryFunc::BitShiftRightInt32 => BitShiftRightInt32(()),
            BinaryFunc::BitShiftRightInt64 => BitShiftRightInt64(()),
            BinaryFunc::SubInt16 => SubInt16(()),
            BinaryFunc::SubInt32 => SubInt32(()),
            BinaryFunc::SubInt64 => SubInt64(()),
            BinaryFunc::SubFloat32 => SubFloat32(()),
            BinaryFunc::SubFloat64 => SubFloat64(()),
            BinaryFunc::SubInterval => SubInterval(()),
            BinaryFunc::SubTimestamp => SubTimestamp(()),
            BinaryFunc::SubTimestampTz => SubTimestampTz(()),
            BinaryFunc::SubTimestampInterval => SubTimestampInterval(()),
            BinaryFunc::SubTimestampTzInterval => SubTimestampTzInterval(()),
            BinaryFunc::SubDate => SubDate(()),
            BinaryFunc::SubDateInterval => SubDateInterval(()),
            BinaryFunc::SubTime => SubTime(()),
            BinaryFunc::SubTimeInterval => SubTimeInterval(()),
            BinaryFunc::SubNumeric => SubNumeric(()),
            BinaryFunc::MulInt16 => MulInt16(()),
            BinaryFunc::MulInt32 => MulInt32(()),
            BinaryFunc::MulInt64 => MulInt64(()),
            BinaryFunc::MulFloat32 => MulFloat32(()),
            BinaryFunc::MulFloat64 => MulFloat64(()),
            BinaryFunc::MulNumeric => MulNumeric(()),
            BinaryFunc::MulInterval => MulInterval(()),
            BinaryFunc::DivInt16 => DivInt16(()),
            BinaryFunc::DivInt32 => DivInt32(()),
            BinaryFunc::DivInt64 => DivInt64(()),
            BinaryFunc::DivFloat32 => DivFloat32(()),
            BinaryFunc::DivFloat64 => DivFloat64(()),
            BinaryFunc::DivNumeric => DivNumeric(()),
            BinaryFunc::DivInterval => DivInterval(()),
            BinaryFunc::ModInt16 => ModInt16(()),
            BinaryFunc::ModInt32 => ModInt32(()),
            BinaryFunc::ModInt64 => ModInt64(()),
            BinaryFunc::ModFloat32 => ModFloat32(()),
            BinaryFunc::ModFloat64 => ModFloat64(()),
            BinaryFunc::ModNumeric => ModNumeric(()),
            BinaryFunc::RoundNumeric => RoundNumeric(()),
            BinaryFunc::Eq => Eq(()),
            BinaryFunc::NotEq => NotEq(()),
            BinaryFunc::Lt => Lt(()),
            BinaryFunc::Lte => Lte(()),
            BinaryFunc::Gt => Gt(()),
            BinaryFunc::Gte => Gte(()),
            BinaryFunc::LikeEscape => LikeEscape(()),
            BinaryFunc::IsLikeMatch { case_insensitive } => IsLikeMatch(*case_insensitive),
            BinaryFunc::IsRegexpMatch { case_insensitive } => IsRegexpMatch(*case_insensitive),
            BinaryFunc::ToCharTimestamp => ToCharTimestamp(()),
            BinaryFunc::ToCharTimestampTz => ToCharTimestampTz(()),
            BinaryFunc::DateBinTimestamp => DateBinTimestamp(()),
            BinaryFunc::DateBinTimestampTz => DateBinTimestampTz(()),
            BinaryFunc::ExtractInterval => ExtractInterval(()),
            BinaryFunc::ExtractTime => ExtractTime(()),
            BinaryFunc::ExtractTimestamp => ExtractTimestamp(()),
            BinaryFunc::ExtractTimestampTz => ExtractTimestampTz(()),
            BinaryFunc::ExtractDate => ExtractDate(()),
            BinaryFunc::DatePartInterval => DatePartInterval(()),
            BinaryFunc::DatePartTime => DatePartTime(()),
            BinaryFunc::DatePartTimestamp => DatePartTimestamp(()),
            BinaryFunc::DatePartTimestampTz => DatePartTimestampTz(()),
            BinaryFunc::DateTruncTimestamp => DateTruncTimestamp(()),
            BinaryFunc::DateTruncTimestampTz => DateTruncTimestampTz(()),
            BinaryFunc::DateTruncInterval => DateTruncInterval(()),
            BinaryFunc::TimezoneTimestamp => TimezoneTimestamp(()),
            BinaryFunc::TimezoneTimestampTz => TimezoneTimestampTz(()),
            BinaryFunc::TimezoneTime { wall_time } => TimezoneTime(wall_time.into_proto()),
            BinaryFunc::TimezoneIntervalTimestamp => TimezoneIntervalTimestamp(()),
            BinaryFunc::TimezoneIntervalTimestampTz => TimezoneIntervalTimestampTz(()),
            BinaryFunc::TimezoneIntervalTime => TimezoneIntervalTime(()),
            BinaryFunc::TextConcat => TextConcat(()),
            BinaryFunc::JsonbGetInt64 { stringify } => JsonbGetInt64(*stringify),
            BinaryFunc::JsonbGetString { stringify } => JsonbGetString(*stringify),
            BinaryFunc::JsonbGetPath { stringify } => JsonbGetPath(*stringify),
            BinaryFunc::JsonbContainsString => JsonbContainsString(()),
            BinaryFunc::JsonbConcat => JsonbConcat(()),
            BinaryFunc::JsonbContainsJsonb => JsonbContainsJsonb(()),
            BinaryFunc::JsonbDeleteInt64 => JsonbDeleteInt64(()),
            BinaryFunc::JsonbDeleteString => JsonbDeleteString(()),
            BinaryFunc::MapContainsKey => MapContainsKey(()),
            BinaryFunc::MapGetValue => MapGetValue(()),
            BinaryFunc::MapGetValues => MapGetValues(()),
            BinaryFunc::MapContainsAllKeys => MapContainsAllKeys(()),
            BinaryFunc::MapContainsAnyKeys => MapContainsAnyKeys(()),
            BinaryFunc::MapContainsMap => MapContainsMap(()),
            BinaryFunc::ConvertFrom => ConvertFrom(()),
            BinaryFunc::Left => Left(()),
            BinaryFunc::Position => Position(()),
            BinaryFunc::Right => Right(()),
            BinaryFunc::RepeatString => RepeatString(()),
            BinaryFunc::Trim => Trim(()),
            BinaryFunc::TrimLeading => TrimLeading(()),
            BinaryFunc::TrimTrailing => TrimTrailing(()),
            BinaryFunc::EncodedBytesCharLength => EncodedBytesCharLength(()),
            BinaryFunc::ListLengthMax { max_layer } => ListLengthMax(max_layer.into_proto()),
            BinaryFunc::ArrayContains => ArrayContains(()),
            BinaryFunc::ArrayLength => ArrayLength(()),
            BinaryFunc::ArrayLower => ArrayLower(()),
            BinaryFunc::ArrayRemove => ArrayRemove(()),
            BinaryFunc::ArrayUpper => ArrayUpper(()),
            BinaryFunc::ArrayArrayConcat => ArrayArrayConcat(()),
            BinaryFunc::ListListConcat => ListListConcat(()),
            BinaryFunc::ListElementConcat => ListElementConcat(()),
            BinaryFunc::ElementListConcat => ElementListConcat(()),
            BinaryFunc::ListRemove => ListRemove(()),
            BinaryFunc::DigestString => DigestString(()),
            BinaryFunc::DigestBytes => DigestBytes(()),
            BinaryFunc::MzRenderTypmod => MzRenderTypmod(()),
            BinaryFunc::Encode => Encode(()),
            BinaryFunc::Decode => Decode(()),
            BinaryFunc::LogNumeric => LogNumeric(()),
            BinaryFunc::Power => Power(()),
            BinaryFunc::PowerNumeric => PowerNumeric(()),
            BinaryFunc::GetByte => GetByte(()),
        };
        ProtoBinaryFunc { kind: Some(kind) }
    }
}

impl TryFrom<ProtoBinaryFunc> for BinaryFunc {
    type Error = TryFromProtoError;

    fn try_from(func: ProtoBinaryFunc) -> Result<Self, Self::Error> {
        use crate::scalar::proto_binary_func::Kind::*;
        if let Some(kind) = func.kind {
            match kind {
                And(()) => Ok(BinaryFunc::And),
                Or(()) => Ok(BinaryFunc::Or),
                AddInt16(()) => Ok(BinaryFunc::AddInt16),
                AddInt32(()) => Ok(BinaryFunc::AddInt32),
                AddInt64(()) => Ok(BinaryFunc::AddInt64),
                AddFloat32(()) => Ok(BinaryFunc::AddFloat32),
                AddFloat64(()) => Ok(BinaryFunc::AddFloat64),
                AddInterval(()) => Ok(BinaryFunc::AddInterval),
                AddTimestampInterval(()) => Ok(BinaryFunc::AddTimestampInterval),
                AddTimestampTzInterval(()) => Ok(BinaryFunc::AddTimestampTzInterval),
                AddDateInterval(()) => Ok(BinaryFunc::AddDateInterval),
                AddDateTime(()) => Ok(BinaryFunc::AddDateTime),
                AddTimeInterval(()) => Ok(BinaryFunc::AddTimeInterval),
                AddNumeric(()) => Ok(BinaryFunc::AddNumeric),
                BitAndInt16(()) => Ok(BinaryFunc::BitAndInt16),
                BitAndInt32(()) => Ok(BinaryFunc::BitAndInt32),
                BitAndInt64(()) => Ok(BinaryFunc::BitAndInt64),
                BitOrInt16(()) => Ok(BinaryFunc::BitOrInt16),
                BitOrInt32(()) => Ok(BinaryFunc::BitOrInt32),
                BitOrInt64(()) => Ok(BinaryFunc::BitOrInt64),
                BitXorInt16(()) => Ok(BinaryFunc::BitXorInt16),
                BitXorInt32(()) => Ok(BinaryFunc::BitXorInt32),
                BitXorInt64(()) => Ok(BinaryFunc::BitXorInt64),
                BitShiftLeftInt16(()) => Ok(BinaryFunc::BitShiftLeftInt16),
                BitShiftLeftInt32(()) => Ok(BinaryFunc::BitShiftLeftInt32),
                BitShiftLeftInt64(()) => Ok(BinaryFunc::BitShiftLeftInt64),
                BitShiftRightInt16(()) => Ok(BinaryFunc::BitShiftRightInt16),
                BitShiftRightInt32(()) => Ok(BinaryFunc::BitShiftRightInt32),
                BitShiftRightInt64(()) => Ok(BinaryFunc::BitShiftRightInt64),
                SubInt16(()) => Ok(BinaryFunc::SubInt16),
                SubInt32(()) => Ok(BinaryFunc::SubInt32),
                SubInt64(()) => Ok(BinaryFunc::SubInt64),
                SubFloat32(()) => Ok(BinaryFunc::SubFloat32),
                SubFloat64(()) => Ok(BinaryFunc::SubFloat64),
                SubInterval(()) => Ok(BinaryFunc::SubInterval),
                SubTimestamp(()) => Ok(BinaryFunc::SubTimestamp),
                SubTimestampTz(()) => Ok(BinaryFunc::SubTimestampTz),
                SubTimestampInterval(()) => Ok(BinaryFunc::SubTimestampInterval),
                SubTimestampTzInterval(()) => Ok(BinaryFunc::SubTimestampTzInterval),
                SubDate(()) => Ok(BinaryFunc::SubDate),
                SubDateInterval(()) => Ok(BinaryFunc::SubDateInterval),
                SubTime(()) => Ok(BinaryFunc::SubTime),
                SubTimeInterval(()) => Ok(BinaryFunc::SubTimeInterval),
                SubNumeric(()) => Ok(BinaryFunc::SubNumeric),
                MulInt16(()) => Ok(BinaryFunc::MulInt16),
                MulInt32(()) => Ok(BinaryFunc::MulInt32),
                MulInt64(()) => Ok(BinaryFunc::MulInt64),
                MulFloat32(()) => Ok(BinaryFunc::MulFloat32),
                MulFloat64(()) => Ok(BinaryFunc::MulFloat64),
                MulNumeric(()) => Ok(BinaryFunc::MulNumeric),
                MulInterval(()) => Ok(BinaryFunc::MulInterval),
                DivInt16(()) => Ok(BinaryFunc::DivInt16),
                DivInt32(()) => Ok(BinaryFunc::DivInt32),
                DivInt64(()) => Ok(BinaryFunc::DivInt64),
                DivFloat32(()) => Ok(BinaryFunc::DivFloat32),
                DivFloat64(()) => Ok(BinaryFunc::DivFloat64),
                DivNumeric(()) => Ok(BinaryFunc::DivNumeric),
                DivInterval(()) => Ok(BinaryFunc::DivInterval),
                ModInt16(()) => Ok(BinaryFunc::ModInt16),
                ModInt32(()) => Ok(BinaryFunc::ModInt32),
                ModInt64(()) => Ok(BinaryFunc::ModInt64),
                ModFloat32(()) => Ok(BinaryFunc::ModFloat32),
                ModFloat64(()) => Ok(BinaryFunc::ModFloat64),
                ModNumeric(()) => Ok(BinaryFunc::ModNumeric),
                RoundNumeric(()) => Ok(BinaryFunc::RoundNumeric),
                Eq(()) => Ok(BinaryFunc::Eq),
                NotEq(()) => Ok(BinaryFunc::NotEq),
                Lt(()) => Ok(BinaryFunc::Lt),
                Lte(()) => Ok(BinaryFunc::Lte),
                Gt(()) => Ok(BinaryFunc::Gt),
                Gte(()) => Ok(BinaryFunc::Gte),
                LikeEscape(()) => Ok(BinaryFunc::LikeEscape),
                IsLikeMatch(case_insensitive) => Ok(BinaryFunc::IsLikeMatch { case_insensitive }),
                IsRegexpMatch(case_insensitive) => {
                    Ok(BinaryFunc::IsRegexpMatch { case_insensitive })
                }
                ToCharTimestamp(()) => Ok(BinaryFunc::ToCharTimestamp),
                ToCharTimestampTz(()) => Ok(BinaryFunc::ToCharTimestampTz),
                DateBinTimestamp(()) => Ok(BinaryFunc::DateBinTimestamp),
                DateBinTimestampTz(()) => Ok(BinaryFunc::DateBinTimestampTz),
                ExtractInterval(()) => Ok(BinaryFunc::ExtractInterval),
                ExtractTime(()) => Ok(BinaryFunc::ExtractTime),
                ExtractTimestamp(()) => Ok(BinaryFunc::ExtractTimestamp),
                ExtractTimestampTz(()) => Ok(BinaryFunc::ExtractTimestampTz),
                ExtractDate(()) => Ok(BinaryFunc::ExtractDate),
                DatePartInterval(()) => Ok(BinaryFunc::DatePartInterval),
                DatePartTime(()) => Ok(BinaryFunc::DatePartTime),
                DatePartTimestamp(()) => Ok(BinaryFunc::DatePartTimestamp),
                DatePartTimestampTz(()) => Ok(BinaryFunc::DatePartTimestampTz),
                DateTruncTimestamp(()) => Ok(BinaryFunc::DateTruncTimestamp),
                DateTruncTimestampTz(()) => Ok(BinaryFunc::DateTruncTimestampTz),
                DateTruncInterval(()) => Ok(BinaryFunc::DateTruncInterval),
                TimezoneTimestamp(()) => Ok(BinaryFunc::TimezoneTimestamp),
                TimezoneTimestampTz(()) => Ok(BinaryFunc::TimezoneTimestampTz),
                TimezoneTime(wall_time) => Ok(BinaryFunc::TimezoneTime {
                    wall_time: wall_time.into_rust()?,
                }),
                TimezoneIntervalTimestamp(()) => Ok(BinaryFunc::TimezoneIntervalTimestamp),
                TimezoneIntervalTimestampTz(()) => Ok(BinaryFunc::TimezoneIntervalTimestampTz),
                TimezoneIntervalTime(()) => Ok(BinaryFunc::TimezoneIntervalTime),
                TextConcat(()) => Ok(BinaryFunc::TextConcat),
                JsonbGetInt64(stringify) => Ok(BinaryFunc::JsonbGetInt64 { stringify }),
                JsonbGetString(stringify) => Ok(BinaryFunc::JsonbGetString { stringify }),
                JsonbGetPath(stringify) => Ok(BinaryFunc::JsonbGetPath { stringify }),
                JsonbContainsString(()) => Ok(BinaryFunc::JsonbContainsString),
                JsonbConcat(()) => Ok(BinaryFunc::JsonbConcat),
                JsonbContainsJsonb(()) => Ok(BinaryFunc::JsonbContainsJsonb),
                JsonbDeleteInt64(()) => Ok(BinaryFunc::JsonbDeleteInt64),
                JsonbDeleteString(()) => Ok(BinaryFunc::JsonbDeleteString),
                MapContainsKey(()) => Ok(BinaryFunc::MapContainsKey),
                MapGetValue(()) => Ok(BinaryFunc::MapGetValue),
                MapGetValues(()) => Ok(BinaryFunc::MapGetValues),
                MapContainsAllKeys(()) => Ok(BinaryFunc::MapContainsAllKeys),
                MapContainsAnyKeys(()) => Ok(BinaryFunc::MapContainsAnyKeys),
                MapContainsMap(()) => Ok(BinaryFunc::MapContainsMap),
                ConvertFrom(()) => Ok(BinaryFunc::ConvertFrom),
                Left(()) => Ok(BinaryFunc::Left),
                Position(()) => Ok(BinaryFunc::Position),
                Right(()) => Ok(BinaryFunc::Right),
                RepeatString(()) => Ok(BinaryFunc::RepeatString),
                Trim(()) => Ok(BinaryFunc::Trim),
                TrimLeading(()) => Ok(BinaryFunc::TrimLeading),
                TrimTrailing(()) => Ok(BinaryFunc::TrimTrailing),
                EncodedBytesCharLength(()) => Ok(BinaryFunc::EncodedBytesCharLength),
                ListLengthMax(max_layer) => Ok(BinaryFunc::ListLengthMax {
                    max_layer: usize::from_proto(max_layer)?,
                }),
                ArrayContains(()) => Ok(BinaryFunc::ArrayContains),
                ArrayLength(()) => Ok(BinaryFunc::ArrayLength),
                ArrayLower(()) => Ok(BinaryFunc::ArrayLower),
                ArrayRemove(()) => Ok(BinaryFunc::ArrayRemove),
                ArrayUpper(()) => Ok(BinaryFunc::ArrayUpper),
                ArrayArrayConcat(()) => Ok(BinaryFunc::ArrayArrayConcat),
                ListListConcat(()) => Ok(BinaryFunc::ListListConcat),
                ListElementConcat(()) => Ok(BinaryFunc::ListElementConcat),
                ElementListConcat(()) => Ok(BinaryFunc::ElementListConcat),
                ListRemove(()) => Ok(BinaryFunc::ListRemove),
                DigestString(()) => Ok(BinaryFunc::DigestString),
                DigestBytes(()) => Ok(BinaryFunc::DigestBytes),
                MzRenderTypmod(()) => Ok(BinaryFunc::MzRenderTypmod),
                Encode(()) => Ok(BinaryFunc::Encode),
                Decode(()) => Ok(BinaryFunc::Decode),
                LogNumeric(()) => Ok(BinaryFunc::LogNumeric),
                Power(()) => Ok(BinaryFunc::Power),
                PowerNumeric(()) => Ok(BinaryFunc::PowerNumeric),
                GetByte(()) => Ok(BinaryFunc::GetByte),
            }
        } else {
            Err(TryFromProtoError::missing_field("ProtoBinaryFunc::kind"))
        }
    }
}

/// A description of an SQL unary function that has the ability to lazy evaluate its arguments
// This trait will eventualy be annotated with #[enum_dispatch] to autogenerate the UnaryFunc enum
trait LazyUnaryFunc {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError>;

    /// The output ColumnType of this function
    fn output_type(&self, input_type: ColumnType) -> ColumnType;

    /// Whether this function will produce NULL on NULL input
    fn propagates_nulls(&self) -> bool;

    /// Whether this function will produce NULL on non-NULL input
    fn introduces_nulls(&self) -> bool;

    /// Whether this function preserves uniqueness
    fn preserves_uniqueness(&self) -> bool;
}

/// A description of an SQL unary function that operates on eagerly evaluated expressions
trait EagerUnaryFunc<'a> {
    type Input: DatumType<'a, EvalError>;
    type Output: DatumType<'a, EvalError>;

    fn call(&self, input: Self::Input) -> Self::Output;

    /// The output ColumnType of this function
    fn output_type(&self, input_type: ColumnType) -> ColumnType;

    /// Whether this function will produce NULL on NULL input
    fn propagates_nulls(&self) -> bool {
        // If the input is not nullable then nulls are propagated
        !Self::Input::nullable()
    }

    /// Whether this function will produce NULL on non-NULL input
    fn introduces_nulls(&self) -> bool {
        // If the output is nullable then nulls can be introduced
        Self::Output::nullable()
    }

    /// Whether this function preserves uniqueness
    fn preserves_uniqueness(&self) -> bool {
        false
    }
}

impl<T: for<'a> EagerUnaryFunc<'a>> LazyUnaryFunc for T {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        match T::Input::try_from_result(a.eval(datums, temp_storage)) {
            // If we can convert to the input type then we call the function
            Ok(input) => self.call(input).into_result(temp_storage),
            // If we can't and we got a non-null datum something went wrong in the planner
            Err(Ok(datum)) if !datum.is_null() => panic!("invalid input type"),
            // Otherwise we just propagate NULLs and errors
            Err(res) => res,
        }
    }

    fn output_type(&self, input_type: ColumnType) -> ColumnType {
        self.output_type(input_type)
    }

    fn propagates_nulls(&self) -> bool {
        self.propagates_nulls()
    }

    fn introduces_nulls(&self) -> bool {
        self.introduces_nulls()
    }

    fn preserves_uniqueness(&self) -> bool {
        self.preserves_uniqueness()
    }
}

use proptest::{prelude::*, strategy::*};

derive_unary!(
    Not,
    IsNull,
    IsTrue,
    IsFalse,
    BitNotInt16,
    BitNotInt32,
    BitNotInt64,
    NegInt16,
    NegInt32,
    NegInt64,
    NegFloat32,
    NegFloat64,
    NegNumeric,
    NegInterval,
    SqrtFloat64,
    SqrtNumeric,
    CbrtFloat64,
    AbsInt16,
    AbsInt32,
    AbsInt64,
    AbsFloat32,
    AbsFloat64,
    AbsNumeric,
    CastBoolToString,
    CastBoolToStringNonstandard,
    CastBoolToInt32,
    CastInt16ToFloat32,
    CastInt16ToFloat64,
    CastInt16ToInt32,
    CastInt16ToInt64,
    CastInt16ToString,
    CastInt2VectorToArray,
    CastInt32ToBool,
    CastInt32ToFloat32,
    CastInt32ToFloat64,
    CastInt32ToOid,
    CastInt32ToPgLegacyChar,
    CastInt32ToInt16,
    CastInt32ToInt64,
    CastInt32ToString,
    CastOidToInt32,
    CastOidToInt64,
    CastOidToString,
    CastOidToRegClass,
    CastRegClassToOid,
    CastOidToRegProc,
    CastRegProcToOid,
    CastOidToRegType,
    CastRegTypeToOid,
    CastInt64ToInt16,
    CastInt64ToInt32,
    CastInt16ToNumeric,
    CastInt32ToNumeric,
    CastInt64ToBool,
    CastInt64ToNumeric,
    CastInt64ToFloat32,
    CastInt64ToFloat64,
    CastInt64ToOid,
    CastInt64ToString,
    CastFloat32ToInt16,
    CastFloat32ToInt32,
    CastFloat32ToInt64,
    CastFloat32ToFloat64,
    CastFloat32ToString,
    CastFloat32ToNumeric,
    CastFloat64ToNumeric,
    CastFloat64ToInt16,
    CastFloat64ToInt32,
    CastFloat64ToInt64,
    CastFloat64ToFloat32,
    CastFloat64ToString,
    CastNumericToFloat32,
    CastNumericToFloat64,
    CastNumericToInt16,
    CastNumericToInt32,
    CastNumericToInt64,
    CastNumericToString,
    CastStringToBool,
    CastStringToPgLegacyChar,
    CastStringToBytes,
    CastStringToInt16,
    CastStringToInt32,
    CastStringToInt64,
    CastStringToInt2Vector,
    CastStringToOid,
    CastStringToFloat32,
    CastStringToFloat64,
    CastStringToDate,
    CastStringToArray,
    CastStringToList,
    CastStringToMap,
    CastStringToTime,
    CastStringToTimestamp,
    CastStringToTimestampTz,
    CastStringToInterval,
    CastStringToNumeric,
    CastStringToUuid,
    CastStringToChar,
    PadChar,
    CastStringToVarChar,
    CastCharToString,
    CastVarCharToString,
    CastDateToTimestamp,
    CastDateToTimestampTz,
    CastDateToString,
    CastTimeToInterval,
    CastTimeToString,
    CastIntervalToString,
    CastIntervalToTime,
    CastTimestampToDate,
    CastTimestampToTimestampTz,
    CastTimestampToString,
    CastTimestampToTime,
    CastTimestampTzToDate,
    CastTimestampTzToTimestamp,
    CastTimestampTzToString,
    CastTimestampTzToTime,
    CastPgLegacyCharToString,
    CastPgLegacyCharToInt32,
    CastBytesToString,
    CastStringToJsonb,
    CastJsonbToString,
    CastJsonbOrNullToJsonb,
    CastJsonbToInt16,
    CastJsonbToInt32,
    CastJsonbToInt64,
    CastJsonbToFloat32,
    CastJsonbToFloat64,
    CastJsonbToNumeric,
    CastJsonbToBool,
    CastUuidToString,
    CastRecordToString,
    CastRecord1ToRecord2,
    CastArrayToString,
    CastListToString,
    CastList1ToList2,
    CastArrayToListOneDim,
    CastMapToString,
    CastInt2VectorToString,
    CeilFloat32,
    CeilFloat64,
    CeilNumeric,
    FloorFloat32,
    FloorFloat64,
    FloorNumeric,
    Ascii,
    BitLengthBytes,
    BitLengthString,
    ByteLengthBytes,
    ByteLengthString,
    CharLength,
    Chr,
    IsLikeMatch,
    IsRegexpMatch,
    RegexpMatch,
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
    TimezoneTimestamp,
    TimezoneTimestampTz,
    TimezoneTime,
    ToTimestamp,
    JustifyDays,
    JustifyHours,
    JustifyInterval,
    JsonbArrayLength,
    JsonbTypeof,
    JsonbStripNulls,
    JsonbPretty,
    RoundFloat32,
    RoundFloat64,
    RoundNumeric,
    TrimWhitespace,
    TrimLeadingWhitespace,
    TrimTrailingWhitespace,
    RecordGet,
    ListLength,
    MapLength,
    Upper,
    Lower,
    Cos,
    Acos,
    Cosh,
    Acosh,
    Sin,
    Asin,
    Sinh,
    Asinh,
    Tan,
    Atan,
    Tanh,
    Atanh,
    Cot,
    Degrees,
    Radians,
    Log10,
    Log10Numeric,
    Ln,
    LnNumeric,
    Exp,
    ExpNumeric,
    Sleep,
    Panic,
    RescaleNumeric,
    PgColumnSize,
    MzRowSize,
    MzTypeName
);

/// An explicit [`Arbitrary`] implementation needed here because of a known
/// `proptest` issue.
///
/// Revert to the derive-macro impementation once the issue[^1] is fixed.
///
/// [^1]: <https://github.com/AltSysrq/proptest/issues/152>
impl Arbitrary for UnaryFunc {
    type Parameters = ();

    type Strategy = Union<BoxedStrategy<Self>>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            Not::arbitrary().prop_map_into(),
            IsNull::arbitrary().prop_map_into(),
            IsTrue::arbitrary().prop_map_into(),
            IsFalse::arbitrary().prop_map_into(),
            BitNotInt16::arbitrary().prop_map_into(),
            BitNotInt32::arbitrary().prop_map_into(),
            BitNotInt64::arbitrary().prop_map_into(),
            NegInt16::arbitrary().prop_map_into(),
            NegInt32::arbitrary().prop_map_into(),
            NegInt64::arbitrary().prop_map_into(),
            NegFloat32::arbitrary().prop_map_into(),
            NegFloat64::arbitrary().prop_map_into(),
            NegNumeric::arbitrary().prop_map_into(),
            NegInterval::arbitrary().prop_map_into(),
            SqrtFloat64::arbitrary().prop_map_into(),
            SqrtNumeric::arbitrary().prop_map_into(),
            CbrtFloat64::arbitrary().prop_map_into(),
            AbsInt16::arbitrary().prop_map_into(),
            AbsInt32::arbitrary().prop_map_into(),
            AbsInt64::arbitrary().prop_map_into(),
            AbsFloat32::arbitrary().prop_map_into(),
            AbsFloat64::arbitrary().prop_map_into(),
            AbsNumeric::arbitrary().prop_map_into(),
            CastBoolToString::arbitrary().prop_map_into(),
            CastBoolToStringNonstandard::arbitrary().prop_map_into(),
            CastBoolToInt32::arbitrary().prop_map_into(),
            CastInt16ToFloat32::arbitrary().prop_map_into(),
            CastInt16ToFloat64::arbitrary().prop_map_into(),
            CastInt16ToInt32::arbitrary().prop_map_into(),
            CastInt16ToInt64::arbitrary().prop_map_into(),
            CastInt16ToString::arbitrary().prop_map_into(),
            CastInt2VectorToArray::arbitrary().prop_map_into(),
            CastInt32ToBool::arbitrary().prop_map_into(),
            CastInt32ToFloat32::arbitrary().prop_map_into(),
            CastInt32ToFloat64::arbitrary().prop_map_into(),
            CastInt32ToOid::arbitrary().prop_map_into(),
            CastInt32ToPgLegacyChar::arbitrary().prop_map_into(),
            CastInt32ToInt16::arbitrary().prop_map_into(),
            CastInt32ToInt64::arbitrary().prop_map_into(),
            CastInt32ToString::arbitrary().prop_map_into(),
            CastOidToInt32::arbitrary().prop_map_into(),
            CastOidToInt64::arbitrary().prop_map_into(),
            CastOidToString::arbitrary().prop_map_into(),
            CastOidToRegClass::arbitrary().prop_map_into(),
            CastRegClassToOid::arbitrary().prop_map_into(),
            CastOidToRegProc::arbitrary().prop_map_into(),
            CastRegProcToOid::arbitrary().prop_map_into(),
            CastOidToRegType::arbitrary().prop_map_into(),
            CastRegTypeToOid::arbitrary().prop_map_into(),
            CastInt64ToInt16::arbitrary().prop_map_into(),
            CastInt64ToInt32::arbitrary().prop_map_into(),
            any::<Option<NumericMaxScale>>()
                .prop_map(|i| UnaryFunc::CastInt16ToNumeric(CastInt16ToNumeric(i))),
            any::<Option<NumericMaxScale>>()
                .prop_map(|i| UnaryFunc::CastInt32ToNumeric(CastInt32ToNumeric(i))),
            CastInt64ToBool::arbitrary().prop_map_into(),
            any::<Option<NumericMaxScale>>()
                .prop_map(|i| UnaryFunc::CastInt64ToNumeric(CastInt64ToNumeric(i))),
            CastInt64ToFloat32::arbitrary().prop_map_into(),
            CastInt64ToFloat64::arbitrary().prop_map_into(),
            CastInt64ToOid::arbitrary().prop_map_into(),
            CastInt64ToString::arbitrary().prop_map_into(),
            CastFloat32ToInt16::arbitrary().prop_map_into(),
            CastFloat32ToInt32::arbitrary().prop_map_into(),
            CastFloat32ToInt64::arbitrary().prop_map_into(),
            CastFloat32ToFloat64::arbitrary().prop_map_into(),
            CastFloat32ToString::arbitrary().prop_map_into(),
            any::<Option<NumericMaxScale>>()
                .prop_map(|i| UnaryFunc::CastFloat32ToNumeric(CastFloat32ToNumeric(i))),
            any::<Option<NumericMaxScale>>()
                .prop_map(|i| UnaryFunc::CastFloat64ToNumeric(CastFloat64ToNumeric(i))),
            CastFloat64ToInt16::arbitrary().prop_map_into(),
            CastFloat64ToInt32::arbitrary().prop_map_into(),
            CastFloat64ToInt64::arbitrary().prop_map_into(),
            CastFloat64ToFloat32::arbitrary().prop_map_into(),
            CastFloat64ToString::arbitrary().prop_map_into(),
            CastNumericToFloat32::arbitrary().prop_map_into(),
            CastNumericToFloat64::arbitrary().prop_map_into(),
            CastNumericToInt16::arbitrary().prop_map_into(),
            CastNumericToInt32::arbitrary().prop_map_into(),
            CastNumericToInt64::arbitrary().prop_map_into(),
            CastNumericToString::arbitrary().prop_map_into(),
            CastStringToBool::arbitrary().prop_map_into(),
            CastStringToPgLegacyChar::arbitrary().prop_map_into(),
            CastStringToBytes::arbitrary().prop_map_into(),
            CastStringToInt16::arbitrary().prop_map_into(),
            CastStringToInt32::arbitrary().prop_map_into(),
            CastStringToInt64::arbitrary().prop_map_into(),
            CastStringToInt2Vector::arbitrary().prop_map_into(),
            CastStringToOid::arbitrary().prop_map_into(),
            CastStringToFloat32::arbitrary().prop_map_into(),
            CastStringToFloat64::arbitrary().prop_map_into(),
            CastStringToDate::arbitrary().prop_map_into(),
            (any::<ScalarType>(), any::<MirScalarExpr>()).prop_map(|(return_ty, expr)| {
                UnaryFunc::CastStringToArray(CastStringToArray {
                    return_ty,
                    cast_expr: Box::new(expr),
                })
            }),
            (any::<ScalarType>(), any::<MirScalarExpr>()).prop_map(|(return_ty, expr)| {
                UnaryFunc::CastStringToList(CastStringToList {
                    return_ty,
                    cast_expr: Box::new(expr),
                })
            }),
            (any::<ScalarType>(), any::<MirScalarExpr>()).prop_map(|(return_ty, expr)| {
                UnaryFunc::CastStringToMap(CastStringToMap {
                    return_ty,
                    cast_expr: Box::new(expr),
                })
            }),
            CastStringToTime::arbitrary().prop_map_into(),
            CastStringToTimestamp::arbitrary().prop_map_into(),
            CastStringToTimestampTz::arbitrary().prop_map_into(),
            CastStringToInterval::arbitrary().prop_map_into(),
            CastStringToNumeric::arbitrary().prop_map_into(),
            CastStringToUuid::arbitrary().prop_map_into(),
            CastStringToChar::arbitrary().prop_map_into(),
            PadChar::arbitrary().prop_map_into(),
            CastStringToVarChar::arbitrary().prop_map_into(),
            CastCharToString::arbitrary().prop_map_into(),
            CastVarCharToString::arbitrary().prop_map_into(),
            CastDateToTimestamp::arbitrary().prop_map_into(),
            CastDateToTimestampTz::arbitrary().prop_map_into(),
            CastDateToString::arbitrary().prop_map_into(),
            CastTimeToInterval::arbitrary().prop_map_into(),
            CastTimeToString::arbitrary().prop_map_into(),
            CastIntervalToString::arbitrary().prop_map_into(),
            CastIntervalToTime::arbitrary().prop_map_into(),
            CastTimestampToDate::arbitrary().prop_map_into(),
            CastTimestampToTimestampTz::arbitrary().prop_map_into(),
            CastTimestampToString::arbitrary().prop_map_into(),
            CastTimestampToTime::arbitrary().prop_map_into(),
            CastTimestampTzToDate::arbitrary().prop_map_into(),
            CastTimestampTzToTimestamp::arbitrary().prop_map_into(),
            CastTimestampTzToString::arbitrary().prop_map_into(),
            CastTimestampTzToTime::arbitrary().prop_map_into(),
            CastPgLegacyCharToString::arbitrary().prop_map_into(),
            CastPgLegacyCharToInt32::arbitrary().prop_map_into(),
            CastBytesToString::arbitrary().prop_map_into(),
            CastStringToJsonb::arbitrary().prop_map_into(),
            CastJsonbToString::arbitrary().prop_map_into(),
            CastJsonbOrNullToJsonb::arbitrary().prop_map_into(),
            CastJsonbToInt16::arbitrary().prop_map_into(),
            CastJsonbToInt32::arbitrary().prop_map_into(),
            CastJsonbToInt64::arbitrary().prop_map_into(),
            CastJsonbToFloat32::arbitrary().prop_map_into(),
            CastJsonbToFloat64::arbitrary().prop_map_into(),
            CastJsonbToNumeric::arbitrary().prop_map_into(),
            CastJsonbToBool::arbitrary().prop_map_into(),
            CastUuidToString::arbitrary().prop_map_into(),
            CastRecordToString::arbitrary().prop_map_into(),
            (
                any::<ScalarType>(),
                proptest::collection::vec(any::<MirScalarExpr>(), 1..5)
            )
                .prop_map(|(return_ty, cast_exprs)| {
                    UnaryFunc::CastRecord1ToRecord2(CastRecord1ToRecord2 {
                        return_ty,
                        cast_exprs,
                    })
                }),
            CastArrayToString::arbitrary().prop_map_into(),
            CastListToString::arbitrary().prop_map_into(),
            (any::<ScalarType>(), any::<MirScalarExpr>()).prop_map(|(return_ty, expr)| {
                UnaryFunc::CastList1ToList2(CastList1ToList2 {
                    return_ty,
                    cast_expr: Box::new(expr),
                })
            }),
            CastArrayToListOneDim::arbitrary().prop_map_into(),
            CastMapToString::arbitrary().prop_map_into(),
            CastInt2VectorToString::arbitrary().prop_map_into(),
            CeilFloat32::arbitrary().prop_map_into(),
            CeilFloat64::arbitrary().prop_map_into(),
            CeilNumeric::arbitrary().prop_map_into(),
            FloorFloat32::arbitrary().prop_map_into(),
            FloorFloat64::arbitrary().prop_map_into(),
            FloorNumeric::arbitrary().prop_map_into(),
            Ascii::arbitrary().prop_map_into(),
            BitLengthBytes::arbitrary().prop_map_into(),
            BitLengthString::arbitrary().prop_map_into(),
            ByteLengthBytes::arbitrary().prop_map_into(),
            ByteLengthString::arbitrary().prop_map_into(),
            CharLength::arbitrary().prop_map_into(),
            Chr::arbitrary().prop_map_into(),
            like_pattern::any_matcher()
                .prop_map(|matcher| UnaryFunc::IsLikeMatch(IsLikeMatch(matcher))),
            any_regex().prop_map(|regex| UnaryFunc::IsRegexpMatch(IsRegexpMatch(regex))),
            any_regex().prop_map(|regex| UnaryFunc::RegexpMatch(RegexpMatch(regex))),
            ExtractInterval::arbitrary().prop_map_into(),
            ExtractTime::arbitrary().prop_map_into(),
            ExtractTimestamp::arbitrary().prop_map_into(),
            ExtractTimestampTz::arbitrary().prop_map_into(),
            ExtractDate::arbitrary().prop_map_into(),
            DatePartInterval::arbitrary().prop_map_into(),
            DatePartTime::arbitrary().prop_map_into(),
            DatePartTimestamp::arbitrary().prop_map_into(),
            DatePartTimestampTz::arbitrary().prop_map_into(),
            DateTruncTimestamp::arbitrary().prop_map_into(),
            DateTruncTimestampTz::arbitrary().prop_map_into(),
            TimezoneTimestamp::arbitrary().prop_map_into(),
            TimezoneTimestampTz::arbitrary().prop_map_into(),
            TimezoneTime::arbitrary().prop_map_into(),
            ToTimestamp::arbitrary().prop_map_into(),
            JustifyDays::arbitrary().prop_map_into(),
            JustifyHours::arbitrary().prop_map_into(),
            JustifyInterval::arbitrary().prop_map_into(),
            JsonbArrayLength::arbitrary().prop_map_into(),
            JsonbTypeof::arbitrary().prop_map_into(),
            JsonbStripNulls::arbitrary().prop_map_into(),
            JsonbPretty::arbitrary().prop_map_into(),
            RoundFloat32::arbitrary().prop_map_into(),
            RoundFloat64::arbitrary().prop_map_into(),
            RoundNumeric::arbitrary().prop_map_into(),
            TrimWhitespace::arbitrary().prop_map_into(),
            TrimLeadingWhitespace::arbitrary().prop_map_into(),
            TrimTrailingWhitespace::arbitrary().prop_map_into(),
            RecordGet::arbitrary().prop_map_into(),
            ListLength::arbitrary().prop_map_into(),
            MapLength::arbitrary().prop_map_into(),
            Upper::arbitrary().prop_map_into(),
            Lower::arbitrary().prop_map_into(),
            Cos::arbitrary().prop_map_into(),
            Acos::arbitrary().prop_map_into(),
            Cosh::arbitrary().prop_map_into(),
            Acosh::arbitrary().prop_map_into(),
            Sin::arbitrary().prop_map_into(),
            Asin::arbitrary().prop_map_into(),
            Sinh::arbitrary().prop_map_into(),
            Asinh::arbitrary().prop_map_into(),
            Tan::arbitrary().prop_map_into(),
            Atan::arbitrary().prop_map_into(),
            Tanh::arbitrary().prop_map_into(),
            Atanh::arbitrary().prop_map_into(),
            Cot::arbitrary().prop_map_into(),
            Degrees::arbitrary().prop_map_into(),
            Radians::arbitrary().prop_map_into(),
            Log10::arbitrary().prop_map_into(),
            Log10Numeric::arbitrary().prop_map_into(),
            Ln::arbitrary().prop_map_into(),
            LnNumeric::arbitrary().prop_map_into(),
            Exp::arbitrary().prop_map_into(),
            ExpNumeric::arbitrary().prop_map_into(),
            Sleep::arbitrary().prop_map_into(),
            Panic::arbitrary().prop_map_into(),
            RescaleNumeric::arbitrary().prop_map_into(),
            PgColumnSize::arbitrary().prop_map_into(),
            MzRowSize::arbitrary().prop_map_into(),
            MzTypeName::arbitrary().prop_map_into(),
        ]
    }
}

impl From<&UnaryFunc> for ProtoUnaryFunc {
    fn from(func: &UnaryFunc) -> Self {
        use crate::scalar::proto_unary_func::Kind::*;
        use crate::scalar::proto_unary_func::*;
        let kind = match func {
            UnaryFunc::Not(_) => Not(()),
            UnaryFunc::IsNull(_) => IsNull(()),
            UnaryFunc::IsTrue(_) => IsTrue(()),
            UnaryFunc::IsFalse(_) => IsFalse(()),
            UnaryFunc::BitNotInt16(_) => BitNotInt16(()),
            UnaryFunc::BitNotInt32(_) => BitNotInt32(()),
            UnaryFunc::BitNotInt64(_) => BitNotInt64(()),
            UnaryFunc::NegInt16(_) => NegInt16(()),
            UnaryFunc::NegInt32(_) => NegInt32(()),
            UnaryFunc::NegInt64(_) => NegInt64(()),
            UnaryFunc::NegFloat32(_) => NegFloat32(()),
            UnaryFunc::NegFloat64(_) => NegFloat64(()),
            UnaryFunc::NegNumeric(_) => NegNumeric(()),
            UnaryFunc::NegInterval(_) => NegInterval(()),
            UnaryFunc::SqrtFloat64(_) => SqrtFloat64(()),
            UnaryFunc::SqrtNumeric(_) => SqrtNumeric(()),
            UnaryFunc::CbrtFloat64(_) => CbrtFloat64(()),
            UnaryFunc::AbsInt16(_) => AbsInt16(()),
            UnaryFunc::AbsInt32(_) => AbsInt32(()),
            UnaryFunc::AbsInt64(_) => AbsInt64(()),
            UnaryFunc::AbsFloat32(_) => AbsFloat32(()),
            UnaryFunc::AbsFloat64(_) => AbsFloat64(()),
            UnaryFunc::AbsNumeric(_) => AbsNumeric(()),
            UnaryFunc::CastBoolToString(_) => CastBoolToString(()),
            UnaryFunc::CastBoolToStringNonstandard(_) => CastBoolToStringNonstandard(()),
            UnaryFunc::CastBoolToInt32(_) => CastBoolToInt32(()),
            UnaryFunc::CastInt16ToFloat32(_) => CastInt16ToFloat32(()),
            UnaryFunc::CastInt16ToFloat64(_) => CastInt16ToFloat64(()),
            UnaryFunc::CastInt16ToInt32(_) => CastInt16ToInt32(()),
            UnaryFunc::CastInt16ToInt64(_) => CastInt16ToInt64(()),
            UnaryFunc::CastInt16ToString(_) => CastInt16ToString(()),
            UnaryFunc::CastInt2VectorToArray(_) => CastInt2VectorToArray(()),
            UnaryFunc::CastInt32ToBool(_) => CastInt32ToBool(()),
            UnaryFunc::CastInt32ToFloat32(_) => CastInt32ToFloat32(()),
            UnaryFunc::CastInt32ToFloat64(_) => CastInt32ToFloat64(()),
            UnaryFunc::CastInt32ToOid(_) => CastInt32ToOid(()),
            UnaryFunc::CastInt32ToPgLegacyChar(_) => CastInt32ToPgLegacyChar(()),
            UnaryFunc::CastInt32ToInt16(_) => CastInt32ToInt16(()),
            UnaryFunc::CastInt32ToInt64(_) => CastInt32ToInt64(()),
            UnaryFunc::CastInt32ToString(_) => CastInt32ToString(()),
            UnaryFunc::CastOidToInt32(_) => CastOidToInt32(()),
            UnaryFunc::CastOidToInt64(_) => CastOidToInt64(()),
            UnaryFunc::CastOidToString(_) => CastOidToString(()),
            UnaryFunc::CastOidToRegClass(_) => CastOidToRegClass(()),
            UnaryFunc::CastRegClassToOid(_) => CastRegClassToOid(()),
            UnaryFunc::CastOidToRegProc(_) => CastOidToRegProc(()),
            UnaryFunc::CastRegProcToOid(_) => CastRegProcToOid(()),
            UnaryFunc::CastOidToRegType(_) => CastOidToRegType(()),
            UnaryFunc::CastRegTypeToOid(_) => CastRegTypeToOid(()),
            UnaryFunc::CastInt64ToInt16(_) => CastInt64ToInt16(()),
            UnaryFunc::CastInt64ToInt32(_) => CastInt64ToInt32(()),
            UnaryFunc::CastInt16ToNumeric(func) => CastInt16ToNumeric(func.0.into_proto()),
            UnaryFunc::CastInt32ToNumeric(func) => CastInt32ToNumeric(func.0.into_proto()),
            UnaryFunc::CastInt64ToBool(_) => CastInt64ToBool(()),
            UnaryFunc::CastInt64ToNumeric(func) => CastInt64ToNumeric(func.0.into_proto()),
            UnaryFunc::CastInt64ToFloat32(_) => CastInt64ToFloat32(()),
            UnaryFunc::CastInt64ToFloat64(_) => CastInt64ToFloat64(()),
            UnaryFunc::CastInt64ToOid(_) => CastInt64ToOid(()),
            UnaryFunc::CastInt64ToString(_) => CastInt64ToString(()),
            UnaryFunc::CastFloat32ToInt16(_) => CastFloat32ToInt16(()),
            UnaryFunc::CastFloat32ToInt32(_) => CastFloat32ToInt32(()),
            UnaryFunc::CastFloat32ToInt64(_) => CastFloat32ToInt64(()),
            UnaryFunc::CastFloat32ToFloat64(_) => CastFloat32ToFloat64(()),
            UnaryFunc::CastFloat32ToString(_) => CastFloat32ToString(()),
            UnaryFunc::CastFloat32ToNumeric(func) => CastFloat32ToNumeric(func.0.into_proto()),
            UnaryFunc::CastFloat64ToNumeric(func) => CastFloat64ToNumeric(func.0.into_proto()),
            UnaryFunc::CastFloat64ToInt16(_) => CastFloat64ToInt16(()),
            UnaryFunc::CastFloat64ToInt32(_) => CastFloat64ToInt32(()),
            UnaryFunc::CastFloat64ToInt64(_) => CastFloat64ToInt64(()),
            UnaryFunc::CastFloat64ToFloat32(_) => CastFloat64ToFloat32(()),
            UnaryFunc::CastFloat64ToString(_) => CastFloat64ToString(()),
            UnaryFunc::CastNumericToFloat32(_) => CastNumericToFloat32(()),
            UnaryFunc::CastNumericToFloat64(_) => CastNumericToFloat64(()),
            UnaryFunc::CastNumericToInt16(_) => CastNumericToInt16(()),
            UnaryFunc::CastNumericToInt32(_) => CastNumericToInt32(()),
            UnaryFunc::CastNumericToInt64(_) => CastNumericToInt64(()),
            UnaryFunc::CastNumericToString(_) => CastNumericToString(()),
            UnaryFunc::CastStringToBool(_) => CastStringToBool(()),
            UnaryFunc::CastStringToPgLegacyChar(_) => CastStringToPgLegacyChar(()),
            UnaryFunc::CastStringToBytes(_) => CastStringToBytes(()),
            UnaryFunc::CastStringToInt16(_) => CastStringToInt16(()),
            UnaryFunc::CastStringToInt32(_) => CastStringToInt32(()),
            UnaryFunc::CastStringToInt64(_) => CastStringToInt64(()),
            UnaryFunc::CastStringToInt2Vector(_) => CastStringToInt2Vector(()),
            UnaryFunc::CastStringToOid(_) => CastStringToOid(()),
            UnaryFunc::CastStringToFloat32(_) => CastStringToFloat32(()),
            UnaryFunc::CastStringToFloat64(_) => CastStringToFloat64(()),
            UnaryFunc::CastStringToDate(_) => CastStringToDate(()),
            UnaryFunc::CastStringToArray(inner) => {
                CastStringToArray(Box::new(ProtoCastToVariableType {
                    return_ty: Some((&inner.return_ty).into()),
                    cast_expr: Some(Box::new((&*inner.cast_expr).into())),
                }))
            }
            UnaryFunc::CastStringToList(inner) => {
                CastStringToList(Box::new(ProtoCastToVariableType {
                    return_ty: Some((&inner.return_ty).into()),
                    cast_expr: Some(Box::new((&*inner.cast_expr).into())),
                }))
            }
            UnaryFunc::CastStringToMap(inner) => {
                CastStringToMap(Box::new(ProtoCastToVariableType {
                    return_ty: Some((&inner.return_ty).into()),
                    cast_expr: Some(Box::new((&*inner.cast_expr).into())),
                }))
            }
            UnaryFunc::CastStringToTime(_) => CastStringToTime(()),
            UnaryFunc::CastStringToTimestamp(_) => CastStringToTimestamp(()),
            UnaryFunc::CastStringToTimestampTz(_) => CastStringToTimestampTz(()),
            UnaryFunc::CastStringToInterval(_) => CastStringToInterval(()),
            UnaryFunc::CastStringToNumeric(func) => CastStringToNumeric(func.0.into_proto()),
            UnaryFunc::CastStringToUuid(_) => CastStringToUuid(()),
            UnaryFunc::CastStringToChar(func) => CastStringToChar(ProtoCastStringToChar {
                length: func.length.as_ref().map(Into::into),
                fail_on_len: func.fail_on_len,
            }),
            UnaryFunc::PadChar(func) => PadChar(ProtoPadChar {
                length: func.length.into_proto(),
            }),
            UnaryFunc::CastStringToVarChar(func) => CastStringToVarChar(ProtoCastStringToVarChar {
                length: func.length.into_proto(),
                fail_on_len: func.fail_on_len,
            }),
            UnaryFunc::CastCharToString(_) => CastCharToString(()),
            UnaryFunc::CastVarCharToString(_) => CastVarCharToString(()),
            UnaryFunc::CastDateToTimestamp(_) => CastDateToTimestamp(()),
            UnaryFunc::CastDateToTimestampTz(_) => CastDateToTimestampTz(()),
            UnaryFunc::CastDateToString(_) => CastDateToString(()),
            UnaryFunc::CastTimeToInterval(_) => CastTimeToInterval(()),
            UnaryFunc::CastTimeToString(_) => CastTimeToString(()),
            UnaryFunc::CastIntervalToString(_) => CastIntervalToString(()),
            UnaryFunc::CastIntervalToTime(_) => CastIntervalToTime(()),
            UnaryFunc::CastTimestampToDate(_) => CastTimestampToDate(()),
            UnaryFunc::CastTimestampToTimestampTz(_) => CastTimestampToTimestampTz(()),
            UnaryFunc::CastTimestampToString(_) => CastTimestampToString(()),
            UnaryFunc::CastTimestampToTime(_) => CastTimestampToTime(()),
            UnaryFunc::CastTimestampTzToDate(_) => CastTimestampTzToDate(()),
            UnaryFunc::CastTimestampTzToTimestamp(_) => CastTimestampTzToTimestamp(()),
            UnaryFunc::CastTimestampTzToString(_) => CastTimestampTzToString(()),
            UnaryFunc::CastTimestampTzToTime(_) => CastTimestampTzToTime(()),
            UnaryFunc::CastPgLegacyCharToString(_) => CastPgLegacyCharToString(()),
            UnaryFunc::CastPgLegacyCharToInt32(_) => CastPgLegacyCharToInt32(()),
            UnaryFunc::CastBytesToString(_) => CastBytesToString(()),
            UnaryFunc::CastStringToJsonb(_) => CastStringToJsonb(()),
            UnaryFunc::CastJsonbToString(_) => CastJsonbToString(()),
            UnaryFunc::CastJsonbOrNullToJsonb(_) => CastJsonbOrNullToJsonb(()),
            UnaryFunc::CastJsonbToInt16(_) => CastJsonbToInt16(()),
            UnaryFunc::CastJsonbToInt32(_) => CastJsonbToInt32(()),
            UnaryFunc::CastJsonbToInt64(_) => CastJsonbToInt64(()),
            UnaryFunc::CastJsonbToFloat32(_) => CastJsonbToFloat32(()),
            UnaryFunc::CastJsonbToFloat64(_) => CastJsonbToFloat64(()),
            UnaryFunc::CastJsonbToNumeric(func) => CastJsonbToNumeric(func.0.into_proto()),
            UnaryFunc::CastJsonbToBool(_) => CastJsonbToBool(()),
            UnaryFunc::CastUuidToString(_) => CastUuidToString(()),
            UnaryFunc::CastRecordToString(func) => CastRecordToString((&func.ty).into()),
            UnaryFunc::CastRecord1ToRecord2(inner) => {
                CastRecord1ToRecord2(ProtoCastRecord1ToRecord2 {
                    return_ty: Some((&inner.return_ty).into()),
                    cast_exprs: inner.cast_exprs.iter().map(Into::into).collect(),
                })
            }
            UnaryFunc::CastArrayToString(func) => CastArrayToString((&func.ty).into()),
            UnaryFunc::CastListToString(func) => CastListToString((&func.ty).into()),
            UnaryFunc::CastList1ToList2(inner) => {
                CastList1ToList2(Box::new(ProtoCastToVariableType {
                    return_ty: Some((&inner.return_ty).into()),
                    cast_expr: Some(Box::new((&*inner.cast_expr).into())),
                }))
            }
            UnaryFunc::CastArrayToListOneDim(_) => CastArrayToListOneDim(()),
            UnaryFunc::CastMapToString(func) => CastMapToString((&func.ty).into()),
            UnaryFunc::CastInt2VectorToString(_) => CastInt2VectorToString(()),
            UnaryFunc::CeilFloat32(_) => CeilFloat32(()),
            UnaryFunc::CeilFloat64(_) => CeilFloat64(()),
            UnaryFunc::CeilNumeric(_) => CeilNumeric(()),
            UnaryFunc::FloorFloat32(_) => FloorFloat32(()),
            UnaryFunc::FloorFloat64(_) => FloorFloat64(()),
            UnaryFunc::FloorNumeric(_) => FloorNumeric(()),
            UnaryFunc::Ascii(_) => Ascii(()),
            UnaryFunc::BitLengthBytes(_) => BitLengthBytes(()),
            UnaryFunc::BitLengthString(_) => BitLengthString(()),
            UnaryFunc::ByteLengthBytes(_) => ByteLengthBytes(()),
            UnaryFunc::ByteLengthString(_) => ByteLengthString(()),
            UnaryFunc::CharLength(_) => CharLength(()),
            UnaryFunc::Chr(_) => Chr(()),
            UnaryFunc::IsLikeMatch(pattern) => IsLikeMatch((&pattern.0).into()),
            UnaryFunc::IsRegexpMatch(regex) => IsRegexpMatch(regex.0.into_proto()),
            UnaryFunc::RegexpMatch(regex) => RegexpMatch(regex.0.into_proto()),
            UnaryFunc::ExtractInterval(func) => ExtractInterval(func.0.into_proto()),
            UnaryFunc::ExtractTime(func) => ExtractTime(func.0.into_proto()),
            UnaryFunc::ExtractTimestamp(func) => ExtractTimestamp(func.0.into_proto()),
            UnaryFunc::ExtractTimestampTz(func) => ExtractTimestampTz(func.0.into_proto()),
            UnaryFunc::ExtractDate(func) => ExtractDate(func.0.into_proto()),
            UnaryFunc::DatePartInterval(func) => DatePartInterval(func.0.into_proto()),
            UnaryFunc::DatePartTime(func) => DatePartTime(func.0.into_proto()),
            UnaryFunc::DatePartTimestamp(func) => DatePartTimestamp(func.0.into_proto()),
            UnaryFunc::DatePartTimestampTz(func) => DatePartTimestampTz(func.0.into_proto()),
            UnaryFunc::DateTruncTimestamp(func) => DateTruncTimestamp(func.0.into_proto()),
            UnaryFunc::DateTruncTimestampTz(func) => DateTruncTimestampTz(func.0.into_proto()),
            UnaryFunc::TimezoneTimestamp(func) => TimezoneTimestamp(func.0.into_proto()),
            UnaryFunc::TimezoneTimestampTz(func) => TimezoneTimestampTz(func.0.into_proto()),
            UnaryFunc::TimezoneTime(func) => TimezoneTime(ProtoTimezoneTime {
                tz: Some(func.tz.into_proto()),
                wall_time: Some(func.wall_time.into_proto()),
            }),
            UnaryFunc::ToTimestamp(_) => ToTimestamp(()),
            UnaryFunc::JustifyDays(_) => JustifyDays(()),
            UnaryFunc::JustifyHours(_) => JustifyHours(()),
            UnaryFunc::JustifyInterval(_) => JustifyInterval(()),
            UnaryFunc::JsonbArrayLength(_) => JsonbArrayLength(()),
            UnaryFunc::JsonbTypeof(_) => JsonbTypeof(()),
            UnaryFunc::JsonbStripNulls(_) => JsonbStripNulls(()),
            UnaryFunc::JsonbPretty(_) => JsonbPretty(()),
            UnaryFunc::RoundFloat32(_) => RoundFloat32(()),
            UnaryFunc::RoundFloat64(_) => RoundFloat64(()),
            UnaryFunc::RoundNumeric(_) => RoundNumeric(()),
            UnaryFunc::TrimWhitespace(_) => TrimWhitespace(()),
            UnaryFunc::TrimLeadingWhitespace(_) => TrimLeadingWhitespace(()),
            UnaryFunc::TrimTrailingWhitespace(_) => TrimTrailingWhitespace(()),
            UnaryFunc::RecordGet(func) => RecordGet(func.0.into_proto()),
            UnaryFunc::ListLength(_) => ListLength(()),
            UnaryFunc::MapLength(_) => MapLength(()),
            UnaryFunc::Upper(_) => Upper(()),
            UnaryFunc::Lower(_) => Lower(()),
            UnaryFunc::Cos(_) => Cos(()),
            UnaryFunc::Acos(_) => Acos(()),
            UnaryFunc::Cosh(_) => Cosh(()),
            UnaryFunc::Acosh(_) => Acosh(()),
            UnaryFunc::Sin(_) => Sin(()),
            UnaryFunc::Asin(_) => Asin(()),
            UnaryFunc::Sinh(_) => Sinh(()),
            UnaryFunc::Asinh(_) => Asinh(()),
            UnaryFunc::Tan(_) => Tan(()),
            UnaryFunc::Atan(_) => Atan(()),
            UnaryFunc::Tanh(_) => Tanh(()),
            UnaryFunc::Atanh(_) => Atanh(()),
            UnaryFunc::Cot(_) => Cot(()),
            UnaryFunc::Degrees(_) => Degrees(()),
            UnaryFunc::Radians(_) => Radians(()),
            UnaryFunc::Log10(_) => Log10(()),
            UnaryFunc::Log10Numeric(_) => Log10Numeric(()),
            UnaryFunc::Ln(_) => Ln(()),
            UnaryFunc::LnNumeric(_) => LnNumeric(()),
            UnaryFunc::Exp(_) => Exp(()),
            UnaryFunc::ExpNumeric(_) => ExpNumeric(()),
            UnaryFunc::Sleep(_) => Sleep(()),
            UnaryFunc::Panic(_) => Panic(()),
            UnaryFunc::RescaleNumeric(func) => RescaleNumeric(func.0.into_proto()),
            UnaryFunc::PgColumnSize(_) => PgColumnSize(()),
            UnaryFunc::MzRowSize(_) => MzRowSize(()),
            UnaryFunc::MzTypeName(_) => MzTypeName(()),
        };
        ProtoUnaryFunc { kind: Some(kind) }
    }
}

impl TryFrom<ProtoUnaryFunc> for UnaryFunc {
    type Error = TryFromProtoError;

    fn try_from(func: ProtoUnaryFunc) -> Result<Self, Self::Error> {
        use crate::scalar::proto_unary_func::Kind::*;
        if let Some(kind) = func.kind {
            match kind {
                Not(()) => Ok(impls::Not.into()),
                IsNull(()) => Ok(impls::IsNull.into()),
                IsTrue(()) => Ok(impls::IsTrue.into()),
                IsFalse(()) => Ok(impls::IsFalse.into()),
                BitNotInt16(()) => Ok(impls::BitNotInt16.into()),
                BitNotInt32(()) => Ok(impls::BitNotInt32.into()),
                BitNotInt64(()) => Ok(impls::BitNotInt64.into()),
                NegInt16(()) => Ok(impls::NegInt16.into()),
                NegInt32(()) => Ok(impls::NegInt32.into()),
                NegInt64(()) => Ok(impls::NegInt64.into()),
                NegFloat32(()) => Ok(impls::NegFloat32.into()),
                NegFloat64(()) => Ok(impls::NegFloat64.into()),
                NegNumeric(()) => Ok(impls::NegNumeric.into()),
                NegInterval(()) => Ok(impls::NegInterval.into()),
                SqrtFloat64(()) => Ok(impls::SqrtFloat64.into()),
                SqrtNumeric(()) => Ok(impls::SqrtNumeric.into()),
                CbrtFloat64(()) => Ok(impls::CbrtFloat64.into()),
                AbsInt16(()) => Ok(impls::AbsInt16.into()),
                AbsInt32(()) => Ok(impls::AbsInt32.into()),
                AbsInt64(()) => Ok(impls::AbsInt64.into()),
                AbsFloat32(()) => Ok(impls::AbsFloat32.into()),
                AbsFloat64(()) => Ok(impls::AbsFloat64.into()),
                AbsNumeric(()) => Ok(impls::AbsNumeric.into()),
                CastBoolToString(()) => Ok(impls::CastBoolToString.into()),
                CastBoolToStringNonstandard(()) => Ok(impls::CastBoolToStringNonstandard.into()),
                CastBoolToInt32(()) => Ok(impls::CastBoolToInt32.into()),
                CastInt16ToFloat32(()) => Ok(impls::CastInt16ToFloat32.into()),
                CastInt16ToFloat64(()) => Ok(impls::CastInt16ToFloat64.into()),
                CastInt16ToInt32(()) => Ok(impls::CastInt16ToInt32.into()),
                CastInt16ToInt64(()) => Ok(impls::CastInt16ToInt64.into()),
                CastInt16ToString(()) => Ok(impls::CastInt16ToString.into()),
                CastInt2VectorToArray(()) => Ok(impls::CastInt2VectorToArray.into()),
                CastInt32ToBool(()) => Ok(impls::CastInt32ToBool.into()),
                CastInt32ToFloat32(()) => Ok(impls::CastInt32ToFloat32.into()),
                CastInt32ToFloat64(()) => Ok(impls::CastInt32ToFloat64.into()),
                CastInt32ToOid(()) => Ok(impls::CastInt32ToOid.into()),
                CastInt32ToPgLegacyChar(()) => Ok(impls::CastInt32ToPgLegacyChar.into()),
                CastInt32ToInt16(()) => Ok(impls::CastInt32ToInt16.into()),
                CastInt32ToInt64(()) => Ok(impls::CastInt32ToInt64.into()),
                CastInt32ToString(()) => Ok(impls::CastInt32ToString.into()),
                CastOidToInt32(()) => Ok(impls::CastOidToInt32.into()),
                CastOidToInt64(()) => Ok(impls::CastOidToInt64.into()),
                CastOidToString(()) => Ok(impls::CastOidToString.into()),
                CastOidToRegClass(()) => Ok(impls::CastOidToRegClass.into()),
                CastRegClassToOid(()) => Ok(impls::CastRegClassToOid.into()),
                CastOidToRegProc(()) => Ok(impls::CastOidToRegProc.into()),
                CastRegProcToOid(()) => Ok(impls::CastRegProcToOid.into()),
                CastOidToRegType(()) => Ok(impls::CastOidToRegType.into()),
                CastRegTypeToOid(()) => Ok(impls::CastRegTypeToOid.into()),
                CastInt64ToInt16(()) => Ok(impls::CastInt64ToInt16.into()),
                CastInt64ToInt32(()) => Ok(impls::CastInt64ToInt32.into()),
                CastInt16ToNumeric(max_scale) => {
                    Ok(impls::CastInt16ToNumeric(max_scale.into_rust()?).into())
                }
                CastInt32ToNumeric(max_scale) => {
                    Ok(impls::CastInt32ToNumeric(max_scale.into_rust()?).into())
                }
                CastInt64ToBool(()) => Ok(impls::CastInt64ToBool.into()),
                CastInt64ToNumeric(max_scale) => {
                    Ok(impls::CastInt64ToNumeric(max_scale.into_rust()?).into())
                }
                CastInt64ToFloat32(()) => Ok(impls::CastInt64ToFloat32.into()),
                CastInt64ToFloat64(()) => Ok(impls::CastInt64ToFloat64.into()),
                CastInt64ToOid(()) => Ok(impls::CastInt64ToOid.into()),
                CastInt64ToString(()) => Ok(impls::CastInt64ToString.into()),
                CastFloat32ToInt16(()) => Ok(impls::CastFloat32ToInt16.into()),
                CastFloat32ToInt32(()) => Ok(impls::CastFloat32ToInt32.into()),
                CastFloat32ToInt64(()) => Ok(impls::CastFloat32ToInt64.into()),
                CastFloat32ToFloat64(()) => Ok(impls::CastFloat32ToFloat64.into()),
                CastFloat32ToString(()) => Ok(impls::CastFloat32ToString.into()),
                CastFloat32ToNumeric(max_scale) => {
                    Ok(impls::CastFloat32ToNumeric(max_scale.into_rust()?).into())
                }
                CastFloat64ToNumeric(max_scale) => {
                    Ok(impls::CastFloat64ToNumeric(max_scale.into_rust()?).into())
                }
                CastFloat64ToInt16(()) => Ok(impls::CastFloat64ToInt16.into()),
                CastFloat64ToInt32(()) => Ok(impls::CastFloat64ToInt32.into()),
                CastFloat64ToInt64(()) => Ok(impls::CastFloat64ToInt64.into()),
                CastFloat64ToFloat32(()) => Ok(impls::CastFloat64ToFloat32.into()),
                CastFloat64ToString(()) => Ok(impls::CastFloat64ToString.into()),
                CastNumericToFloat32(()) => Ok(impls::CastNumericToFloat32.into()),
                CastNumericToFloat64(()) => Ok(impls::CastNumericToFloat64.into()),
                CastNumericToInt16(()) => Ok(impls::CastNumericToInt16.into()),
                CastNumericToInt32(()) => Ok(impls::CastNumericToInt32.into()),
                CastNumericToInt64(()) => Ok(impls::CastNumericToInt64.into()),
                CastNumericToString(()) => Ok(impls::CastNumericToString.into()),
                CastStringToBool(()) => Ok(impls::CastStringToBool.into()),
                CastStringToPgLegacyChar(()) => Ok(impls::CastStringToPgLegacyChar.into()),
                CastStringToBytes(()) => Ok(impls::CastStringToBytes.into()),
                CastStringToInt16(()) => Ok(impls::CastStringToInt16.into()),
                CastStringToInt32(()) => Ok(impls::CastStringToInt32.into()),
                CastStringToInt64(()) => Ok(impls::CastStringToInt64.into()),
                CastStringToInt2Vector(()) => Ok(impls::CastStringToInt2Vector.into()),
                CastStringToOid(()) => Ok(impls::CastStringToOid.into()),
                CastStringToFloat32(()) => Ok(impls::CastStringToFloat32.into()),
                CastStringToFloat64(()) => Ok(impls::CastStringToFloat64.into()),
                CastStringToDate(()) => Ok(impls::CastStringToDate.into()),
                CastStringToArray(inner) => Ok(impls::CastStringToArray {
                    return_ty: inner
                        .return_ty
                        .try_into_if_some("ProtoCastStringToArray::return_ty")?,
                    cast_expr: inner
                        .cast_expr
                        .try_into_if_some("ProtoCastStringToArray::cast_expr")?,
                }
                .into()),
                CastStringToList(inner) => Ok(impls::CastStringToList {
                    return_ty: inner
                        .return_ty
                        .try_into_if_some("ProtoCastStringToList::return_ty")?,
                    cast_expr: inner
                        .cast_expr
                        .try_into_if_some("ProtoCastStringToList::cast_expr")?,
                }
                .into()),
                CastStringToMap(inner) => Ok(impls::CastStringToMap {
                    return_ty: inner
                        .return_ty
                        .try_into_if_some("ProtoCastStringToMap::return_ty")?,
                    cast_expr: inner
                        .cast_expr
                        .try_into_if_some("ProtoCastStringToMap::cast_expr")?,
                }
                .into()),
                CastStringToTime(()) => Ok(impls::CastStringToTime.into()),
                CastStringToTimestamp(()) => Ok(impls::CastStringToTimestamp.into()),
                CastStringToTimestampTz(()) => Ok(impls::CastStringToTimestampTz.into()),
                CastStringToInterval(()) => Ok(impls::CastStringToInterval.into()),
                CastStringToNumeric(max_scale) => {
                    Ok(impls::CastStringToNumeric(max_scale.into_rust()?).into())
                }
                CastStringToUuid(()) => Ok(impls::CastStringToUuid.into()),
                CastStringToChar(func) => Ok(impls::CastStringToChar {
                    length: func.length.map(TryInto::try_into).transpose()?,
                    fail_on_len: func.fail_on_len,
                }
                .into()),
                PadChar(func) => Ok(impls::PadChar {
                    length: func.length.map(TryInto::try_into).transpose()?,
                }
                .into()),
                CastStringToVarChar(func) => Ok(impls::CastStringToVarChar {
                    length: func.length.into_rust()?,
                    fail_on_len: func.fail_on_len,
                }
                .into()),
                CastCharToString(()) => Ok(impls::CastCharToString.into()),
                CastVarCharToString(()) => Ok(impls::CastVarCharToString.into()),
                CastDateToTimestamp(()) => Ok(impls::CastDateToTimestamp.into()),
                CastDateToTimestampTz(()) => Ok(impls::CastDateToTimestampTz.into()),
                CastDateToString(()) => Ok(impls::CastDateToString.into()),
                CastTimeToInterval(()) => Ok(impls::CastTimeToInterval.into()),
                CastTimeToString(()) => Ok(impls::CastTimeToString.into()),
                CastIntervalToString(()) => Ok(impls::CastIntervalToString.into()),
                CastIntervalToTime(()) => Ok(impls::CastIntervalToTime.into()),
                CastTimestampToDate(()) => Ok(impls::CastTimestampToDate.into()),
                CastTimestampToTimestampTz(()) => Ok(impls::CastTimestampToTimestampTz.into()),
                CastTimestampToString(()) => Ok(impls::CastTimestampToString.into()),
                CastTimestampToTime(()) => Ok(impls::CastTimestampToTime.into()),
                CastTimestampTzToDate(()) => Ok(impls::CastTimestampTzToDate.into()),
                CastTimestampTzToTimestamp(()) => Ok(impls::CastTimestampTzToTimestamp.into()),
                CastTimestampTzToString(()) => Ok(impls::CastTimestampTzToString.into()),
                CastTimestampTzToTime(()) => Ok(impls::CastTimestampTzToTime.into()),
                CastPgLegacyCharToString(()) => Ok(impls::CastPgLegacyCharToString.into()),
                CastPgLegacyCharToInt32(()) => Ok(impls::CastPgLegacyCharToInt32.into()),
                CastBytesToString(()) => Ok(impls::CastBytesToString.into()),
                CastStringToJsonb(()) => Ok(impls::CastStringToJsonb.into()),
                CastJsonbToString(()) => Ok(impls::CastJsonbToString.into()),
                CastJsonbOrNullToJsonb(()) => Ok(impls::CastJsonbOrNullToJsonb.into()),
                CastJsonbToInt16(()) => Ok(impls::CastJsonbToInt16.into()),
                CastJsonbToInt32(()) => Ok(impls::CastJsonbToInt32.into()),
                CastJsonbToInt64(()) => Ok(impls::CastJsonbToInt64.into()),
                CastJsonbToFloat32(()) => Ok(impls::CastJsonbToFloat32.into()),
                CastJsonbToFloat64(()) => Ok(impls::CastJsonbToFloat64.into()),
                CastJsonbToNumeric(max_scale) => {
                    Ok(impls::CastJsonbToNumeric(max_scale.into_rust()?).into())
                }
                CastJsonbToBool(()) => Ok(impls::CastJsonbToBool.into()),
                CastUuidToString(()) => Ok(impls::CastUuidToString.into()),
                CastRecordToString(ty) => {
                    Ok(impls::CastRecordToString { ty: ty.try_into()? }.into())
                }
                CastRecord1ToRecord2(inner) => Ok(impls::CastRecord1ToRecord2 {
                    return_ty: inner
                        .return_ty
                        .try_into_if_some("ProtoCastRecord1ToRecord2::return_ty")?,
                    cast_exprs: inner
                        .cast_exprs
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<Vec<_>, _>>()?,
                }
                .into()),
                CastArrayToString(ty) => Ok(impls::CastArrayToString { ty: ty.try_into()? }.into()),
                CastListToString(ty) => Ok(impls::CastListToString { ty: ty.try_into()? }.into()),
                CastList1ToList2(inner) => Ok(impls::CastList1ToList2 {
                    return_ty: inner
                        .return_ty
                        .try_into_if_some("ProtoCastList1ToList2::return_ty")?,
                    cast_expr: inner
                        .cast_expr
                        .try_into_if_some("ProtoCastList1ToList2::cast_expr")?,
                }
                .into()),
                CastArrayToListOneDim(()) => Ok(impls::CastArrayToListOneDim.into()),
                CastMapToString(ty) => Ok(impls::CastMapToString { ty: ty.try_into()? }.into()),
                CastInt2VectorToString(_) => Ok(impls::CastInt2VectorToString.into()),
                CeilFloat32(_) => Ok(impls::CeilFloat32.into()),
                CeilFloat64(_) => Ok(impls::CeilFloat64.into()),
                CeilNumeric(_) => Ok(impls::CeilNumeric.into()),
                FloorFloat32(_) => Ok(impls::FloorFloat32.into()),
                FloorFloat64(_) => Ok(impls::FloorFloat64.into()),
                FloorNumeric(_) => Ok(impls::FloorNumeric.into()),
                Ascii(_) => Ok(impls::Ascii.into()),
                BitLengthBytes(_) => Ok(impls::BitLengthBytes.into()),
                BitLengthString(_) => Ok(impls::BitLengthString.into()),
                ByteLengthBytes(_) => Ok(impls::ByteLengthBytes.into()),
                ByteLengthString(_) => Ok(impls::ByteLengthString.into()),
                CharLength(_) => Ok(impls::CharLength.into()),
                Chr(_) => Ok(impls::Chr.into()),
                IsLikeMatch(pattern) => Ok(impls::IsLikeMatch(pattern.try_into()?).into()),
                IsRegexpMatch(regex) => Ok(impls::IsRegexpMatch(regex.into_rust()?).into()),
                RegexpMatch(regex) => Ok(impls::RegexpMatch(regex.into_rust()?).into()),
                ExtractInterval(units) => Ok(impls::ExtractInterval(units.into_rust()?).into()),
                ExtractTime(units) => Ok(impls::ExtractTime(units.into_rust()?).into()),
                ExtractTimestamp(units) => Ok(impls::ExtractTimestamp(units.into_rust()?).into()),
                ExtractTimestampTz(units) => {
                    Ok(impls::ExtractTimestampTz(units.into_rust()?).into())
                }
                ExtractDate(units) => Ok(impls::ExtractDate(units.into_rust()?).into()),
                DatePartInterval(units) => Ok(impls::DatePartInterval(units.into_rust()?).into()),
                DatePartTime(units) => Ok(impls::DatePartTime(units.into_rust()?).into()),
                DatePartTimestamp(units) => Ok(impls::DatePartTimestamp(units.into_rust()?).into()),
                DatePartTimestampTz(units) => {
                    Ok(impls::DatePartTimestampTz(units.into_rust()?).into())
                }
                DateTruncTimestamp(units) => {
                    Ok(impls::DateTruncTimestamp(units.into_rust()?).into())
                }
                DateTruncTimestampTz(units) => {
                    Ok(impls::DateTruncTimestampTz(units.into_rust()?).into())
                }
                TimezoneTimestamp(tz) => Ok(impls::TimezoneTimestamp(tz.into_rust()?).into()),
                TimezoneTimestampTz(tz) => Ok(impls::TimezoneTimestampTz(tz.into_rust()?).into()),
                TimezoneTime(func) => Ok(impls::TimezoneTime {
                    tz: func.tz.into_rust_if_some("ProtoTimezoneTime::tz")?,
                    wall_time: func
                        .wall_time
                        .into_rust_if_some("ProtoTimezoneTime::wall_time")?,
                }
                .into()),
                ToTimestamp(()) => Ok(impls::ToTimestamp.into()),
                JustifyDays(()) => Ok(impls::JustifyDays.into()),
                JustifyHours(()) => Ok(impls::JustifyHours.into()),
                JustifyInterval(()) => Ok(impls::JustifyInterval.into()),
                JsonbArrayLength(()) => Ok(impls::JsonbArrayLength.into()),
                JsonbTypeof(()) => Ok(impls::JsonbTypeof.into()),
                JsonbStripNulls(()) => Ok(impls::JsonbStripNulls.into()),
                JsonbPretty(()) => Ok(impls::JsonbPretty.into()),
                RoundFloat32(()) => Ok(impls::RoundFloat32.into()),
                RoundFloat64(()) => Ok(impls::RoundFloat64.into()),
                RoundNumeric(()) => Ok(impls::RoundNumeric.into()),
                TrimWhitespace(()) => Ok(impls::TrimWhitespace.into()),
                TrimLeadingWhitespace(()) => Ok(impls::TrimLeadingWhitespace.into()),
                TrimTrailingWhitespace(()) => Ok(impls::TrimTrailingWhitespace.into()),
                RecordGet(field) => Ok(impls::RecordGet(usize::from_proto(field)?).into()),
                ListLength(()) => Ok(impls::ListLength.into()),
                MapLength(()) => Ok(impls::MapLength.into()),
                Upper(()) => Ok(impls::Upper.into()),
                Lower(()) => Ok(impls::Lower.into()),
                Cos(()) => Ok(impls::Cos.into()),
                Acos(()) => Ok(impls::Acos.into()),
                Cosh(()) => Ok(impls::Cosh.into()),
                Acosh(()) => Ok(impls::Acosh.into()),
                Sin(()) => Ok(impls::Sin.into()),
                Asin(()) => Ok(impls::Asin.into()),
                Sinh(()) => Ok(impls::Sinh.into()),
                Asinh(()) => Ok(impls::Asinh.into()),
                Tan(()) => Ok(impls::Tan.into()),
                Atan(()) => Ok(impls::Atan.into()),
                Tanh(()) => Ok(impls::Tanh.into()),
                Atanh(()) => Ok(impls::Atanh.into()),
                Cot(()) => Ok(impls::Cot.into()),
                Degrees(()) => Ok(impls::Degrees.into()),
                Radians(()) => Ok(impls::Radians.into()),
                Log10(()) => Ok(impls::Log10.into()),
                Log10Numeric(()) => Ok(impls::Log10Numeric.into()),
                Ln(()) => Ok(impls::Ln.into()),
                LnNumeric(()) => Ok(impls::LnNumeric.into()),
                Exp(()) => Ok(impls::Exp.into()),
                ExpNumeric(()) => Ok(impls::ExpNumeric.into()),
                Sleep(()) => Ok(impls::Sleep.into()),
                Panic(()) => Ok(impls::Panic.into()),
                RescaleNumeric(max_scale) => {
                    Ok(impls::RescaleNumeric(max_scale.into_rust()?).into())
                }
                PgColumnSize(()) => Ok(impls::PgColumnSize.into()),
                MzRowSize(()) => Ok(impls::MzRowSize.into()),
                MzTypeName(()) => Ok(impls::MzTypeName.into()),
            }
        } else {
            Err(TryFromProtoError::missing_field("ProtoUnaryFunc::kind"))
        }
    }
}

impl TryFrom<Box<ProtoUnaryFunc>> for UnaryFunc {
    type Error = TryFromProtoError;

    fn try_from(value: Box<ProtoUnaryFunc>) -> Result<Self, Self::Error> {
        Ok((*value).try_into()?)
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

fn greatest<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
    exprs: &'a [MirScalarExpr],
) -> Result<Datum<'a>, EvalError> {
    let datums = fallible_iterator::convert(exprs.iter().map(|e| e.eval(datums, temp_storage)));
    Ok(datums
        .filter(|d| Ok(!d.is_null()))
        .max()?
        .unwrap_or(Datum::Null))
}

fn least<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
    exprs: &'a [MirScalarExpr],
) -> Result<Datum<'a>, EvalError> {
    let datums = fallible_iterator::convert(exprs.iter().map(|e| e.eval(datums, temp_storage)));
    Ok(datums
        .filter(|d| Ok(!d.is_null()))
        .min()?
        .unwrap_or(Datum::Null))
}

fn error_if_null<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
    exprs: &'a [MirScalarExpr],
) -> Result<Datum<'a>, EvalError> {
    let datums = exprs
        .iter()
        .map(|e| e.eval(datums, temp_storage))
        .collect::<Result<Vec<_>, _>>()?;
    match datums[0] {
        Datum::Null => Err(EvalError::Internal(datums[1].unwrap_str().to_string())),
        _ => Ok(datums[0]),
    }
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
    let mut packer = row.packer();
    if needle.captures_len() > 1 {
        // The regex contains capture groups, so return an array containing the
        // matched text in each capture group, unless the entire match fails.
        // Individual capture groups may also be null if that group did not
        // participate in the match.
        match needle.captures(haystack.unwrap_str()) {
            None => packer.push(Datum::Null),
            Some(captures) => packer.push_array(
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
            Some(mtch) => packer.push_array(
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
            let mut mac = Hmac::<Md5>::new_from_slice(key).expect("HMAC accepts any key size");
            mac.update(to_digest);
            mac.finalize().into_bytes().to_vec()
        }
        "sha1" => {
            let mut mac = Hmac::<Sha1>::new_from_slice(key).expect("HMAC accepts any key size");
            mac.update(to_digest);
            mac.finalize().into_bytes().to_vec()
        }
        "sha224" => {
            let mut mac = Hmac::<Sha224>::new_from_slice(key).expect("HMAC accepts any key size");
            mac.update(to_digest);
            mac.finalize().into_bytes().to_vec()
        }
        "sha256" => {
            let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key size");
            mac.update(to_digest);
            mac.finalize().into_bytes().to_vec()
        }
        "sha384" => {
            let mut mac = Hmac::<Sha384>::new_from_slice(key).expect("HMAC accepts any key size");
            mac.update(to_digest);
            mac.finalize().into_bytes().to_vec()
        }
        "sha512" => {
            let mut mac = Hmac::<Sha512>::new_from_slice(key).expect("HMAC accepts any key size");
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
            stringify_datum(&mut out, elem, elem_type)?;
            out.push_str(delimiter);
        }
    }
    if out.len() > 0 {
        // Lop off last delimiter only if string is not empty
        out.truncate(out.len() - delimiter.len());
    }
    Ok(Datum::String(temp_storage.push_string(out)))
}

fn list_create<'a>(datums: &[Datum<'a>], temp_storage: &'a RowArena) -> Datum<'a> {
    temp_storage.make_datum(|packer| packer.push_list(datums))
}

fn stringify_datum<'a, B>(
    buf: &mut B,
    d: Datum<'a>,
    ty: &ScalarType,
) -> Result<strconv::Nestable, EvalError>
where
    B: FormatBuffer,
{
    use ScalarType::*;
    match &ty {
        Bool => Ok(strconv::format_bool(buf, d.unwrap_bool())),
        Int16 => Ok(strconv::format_int16(buf, d.unwrap_int16())),
        Int32 => Ok(strconv::format_int32(buf, d.unwrap_int32())),
        Int64 => Ok(strconv::format_int64(buf, d.unwrap_int64())),
        Oid | RegClass | RegProc | RegType => Ok(strconv::format_oid(buf, d.unwrap_uint32())),
        Float32 => Ok(strconv::format_float32(buf, d.unwrap_float32())),
        Float64 => Ok(strconv::format_float64(buf, d.unwrap_float64())),
        Numeric { .. } => Ok(strconv::format_numeric(buf, &d.unwrap_numeric())),
        Date => Ok(strconv::format_date(buf, d.unwrap_date())),
        Time => Ok(strconv::format_time(buf, d.unwrap_time())),
        Timestamp => Ok(strconv::format_timestamp(buf, d.unwrap_timestamp())),
        TimestampTz => Ok(strconv::format_timestamptz(buf, d.unwrap_timestamptz())),
        Interval => Ok(strconv::format_interval(buf, d.unwrap_interval())),
        Bytes => Ok(strconv::format_bytes(buf, d.unwrap_bytes())),
        String | VarChar { .. } => Ok(strconv::format_string(buf, d.unwrap_str())),
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
            stringify_datum(buf.nonnull_buffer(), d, &ScalarType::Int16)
        }),
    }
}

fn array_index<'a>(datums: &[Datum<'a>], offset: usize) -> Datum<'a> {
    let array = datums[0].unwrap_array();
    let dims = array.dims();
    if dims.len() != datums.len() - 1 {
        // You missed the datums "layer"
        return Datum::Null;
    }

    let mut final_idx = 0;

    for (
        ArrayDimension {
            lower_bound,
            length,
        },
        idx,
    ) in dims.into_iter().zip_eq(datums[1..].iter())
    {
        let idx = idx.unwrap_int64();
        if idx < lower_bound as i64 {
            // TODO: How does/should this affect the offset? If this isn't 1,
            // what is the physical representation of the array?
            return Datum::Null;
        }

        final_idx = final_idx * length + (idx as usize - offset);
    }

    array
        .elements()
        .iter()
        .nth(final_idx)
        .unwrap_or(Datum::Null)
}

fn list_index<'a>(datums: &[Datum<'a>]) -> Datum<'a> {
    let mut buf = datums[0];

    for i in datums[1..].iter() {
        if buf.is_null() {
            break;
        }

        let i = i.unwrap_int64();
        if i < 1 {
            return Datum::Null;
        }

        buf = buf
            .unwrap_list()
            .iter()
            .nth(i as usize - 1)
            .unwrap_or(Datum::Null);
    }
    buf
}

fn list_slice_linear<'a>(datums: &[Datum<'a>], temp_storage: &'a RowArena) -> Datum<'a> {
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

    let mut start_idx = 0;
    let mut total_length = usize::MAX;

    for (start, end) in datums[1..].iter().tuples::<(_, _)>() {
        let start = std::cmp::max(start.unwrap_int64(), 1);
        let end = end.unwrap_int64();

        // Result should be empty list.
        if start > end {
            start_idx = 0;
            total_length = 0;
            break;
        }

        let start_inner = start as usize - 1;
        // Start index only moves to geq positions.
        start_idx += start_inner;

        // Length index only moves to leq positions
        let length_inner = (end - start) as usize + 1;
        total_length = std::cmp::min(length_inner, total_length - start_inner);
    }

    let iter = datums[0]
        .unwrap_list()
        .iter()
        .skip(start_idx)
        .take(total_length);

    temp_storage.make_datum(|row| {
        row.push_list_with(|row| {
            // if iter is empty, will get the appropriate empty list.
            for d in iter {
                row.push(d);
            }
        });
    })
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

fn left<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let string: &'a str = a.unwrap_str();
    let n = i64::from(b.unwrap_int32());

    let mut byte_indices = string.char_indices().map(|(i, _)| i);

    let end_in_bytes = match n.cmp(&0) {
        Ordering::Equal => 0,
        Ordering::Greater => {
            let n = usize::try_from(n).map_err(|_| {
                EvalError::InvalidParameterValue(format!("invalid parameter n: {:?}", n))
            })?;
            // nth from the back
            byte_indices.nth(n).unwrap_or(string.len())
        }
        Ordering::Less => {
            let n = usize::try_from(n.abs() - 1).map_err(|_| {
                EvalError::InvalidParameterValue(format!("invalid parameter n: {:?}", n))
            })?;
            byte_indices.rev().nth(n).unwrap_or(0)
        }
    };

    Ok(Datum::String(&string[..end_in_bytes]))
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
        byte_indices.nth(n).unwrap_or(string.len())
    };

    Ok(Datum::String(&string[start_in_bytes..]))
}

fn trim<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let trim_chars = b.unwrap_str();

    Datum::from(a.unwrap_str().trim_matches(|c| trim_chars.contains(c)))
}

fn trim_leading<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let trim_chars = b.unwrap_str();

    Datum::from(
        a.unwrap_str()
            .trim_start_matches(|c| trim_chars.contains(c)),
    )
}

fn trim_trailing<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let trim_chars = b.unwrap_str();

    Datum::from(a.unwrap_str().trim_end_matches(|c| trim_chars.contains(c)))
}

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
                .map_err(|_| EvalError::Int32OutOfRange)?,
        ),
    })
}

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

    Ok(temp_storage.try_make_datum(|packer| packer.push_array(&dims, elems))?)
}

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
                    .map_err(|_| EvalError::Int32OutOfRange)?,
            ),
            None => Datum::Null,
        },
    )
}

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
                Err(_) => Err(EvalError::Int32OutOfRange),
            },
            None => Ok(Datum::Null),
        }
    }
}

fn array_contains<'a>(a: Datum<'a>, b: Datum<'a>) -> Datum<'a> {
    let array = Datum::unwrap_array(&b);
    Datum::from(array.elements().iter().any(|e| e == a))
}

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
                lower_bound: 1,
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
                lower_bound: 1,
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
                lower_bound: 1,
                // Since `b` is treated as an element of `a`, the length of
                // the first dimension of `a` is incremented by one, as `b`
                // is non-empty.
                length: a_dims[0].length + 1,
            }];
            dims.extend(b_dims);
        }
    }

    let elems = a_array.elements().iter().chain(b_array.elements().iter());

    Ok(temp_storage.try_make_datum(|packer| packer.push_array(&dims, elems))?)
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

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub enum VariadicFunc {
    Coalesce,
    Greatest,
    Least,
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
    ArrayIndex {
        // Subtract `offset` from users' input to use 0-indexed arrays, i.e. is
        // `1` in the case of `ScalarType::Array`.
        offset: usize,
    },
    ListCreate {
        // We need to know the element type to type empty lists.
        elem_type: ScalarType,
    },
    RecordCreate {
        field_names: Vec<ColumnName>,
    },
    ListIndex,
    ListSliceLinear,
    SplitPart,
    RegexpMatch,
    HmacString,
    HmacBytes,
    ErrorIfNull,
    DateBinTimestamp,
    DateBinTimestampTz,
}

impl VariadicFunc {
    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [MirScalarExpr],
    ) -> Result<Datum<'a>, EvalError> {
        macro_rules! eager {
            ($func:expr $(, $args:expr)*) => {{
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
            VariadicFunc::Greatest => greatest(datums, temp_storage, exprs),
            VariadicFunc::Least => least(datums, temp_storage, exprs),
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
            VariadicFunc::ArrayIndex { offset } => Ok(eager!(array_index, *offset)),

            VariadicFunc::ListCreate { .. } | VariadicFunc::RecordCreate { .. } => {
                Ok(eager!(list_create, temp_storage))
            }
            VariadicFunc::ListIndex => Ok(eager!(list_index)),
            VariadicFunc::ListSliceLinear => Ok(eager!(list_slice_linear, temp_storage)),
            VariadicFunc::SplitPart => eager!(split_part),
            VariadicFunc::RegexpMatch => eager!(regexp_match_dynamic, temp_storage),
            VariadicFunc::HmacString => eager!(hmac_string, temp_storage),
            VariadicFunc::HmacBytes => eager!(hmac_bytes, temp_storage),
            VariadicFunc::ErrorIfNull => error_if_null(datums, temp_storage, exprs),
            VariadicFunc::DateBinTimestamp => eager!(|d: &[Datum]| date_bin(
                d[0].unwrap_interval(),
                d[1].unwrap_timestamp(),
                d[2].unwrap_timestamp(),
            )),
            VariadicFunc::DateBinTimestampTz => eager!(|d: &[Datum]| date_bin(
                d[0].unwrap_interval(),
                d[1].unwrap_timestamptz(),
                d[2].unwrap_timestamptz(),
            )),
        }
    }

    pub fn output_type(&self, input_types: Vec<ColumnType>) -> ColumnType {
        use VariadicFunc::*;
        let in_nullable = input_types.iter().any(|t| t.nullable);
        match self {
            Coalesce | Greatest | Least => {
                assert!(input_types.len() > 0);
                debug_assert!(
                    input_types
                        .windows(2)
                        .all(|w| w[0].scalar_type.base_eq(&w[1].scalar_type)),
                    "coalesce/greatest/least inputs did not have uniform type: {:?}",
                    input_types
                );
                let nullable = input_types.iter().all(|ty| ty.nullable);
                input_types.into_first().nullable(nullable)
            }
            Concat => ScalarType::String.nullable(true),
            MakeTimestamp => ScalarType::Timestamp.nullable(true),
            PadLeading => ScalarType::String.nullable(true),
            Substr => ScalarType::String.nullable(true),
            Replace => ScalarType::String.nullable(true),
            JsonbBuildArray | JsonbBuildObject => ScalarType::Jsonb.nullable(true),
            ArrayCreate { elem_type } => {
                debug_assert!(
                    input_types.iter().all(|t| t.scalar_type.base_eq(elem_type)),
                    "Args to ArrayCreate should have types that are compatible with the elem_type"
                );
                match elem_type {
                    ScalarType::Array(_) => elem_type.clone().nullable(false),
                    _ => ScalarType::Array(Box::new(elem_type.clone())).nullable(false),
                }
            }
            ArrayToString { .. } => ScalarType::String.nullable(true),
            ArrayIndex { .. } => input_types[0]
                .scalar_type
                .unwrap_array_element_type()
                .clone()
                .nullable(true),
            ListCreate { elem_type } => {
                // commented out to work around
                // https://github.com/MaterializeInc/materialize/issues/8963
                // soft_assert!(
                //     input_types.iter().all(|t| t.scalar_type.base_eq(elem_type)),
                //     "{}", format!("Args to ListCreate should have types that are compatible with the elem_type.\nArgs:{:#?}\nelem_type:{:#?}", input_types, elem_type)
                // );
                ScalarType::List {
                    element_type: Box::new(elem_type.clone()),
                    custom_id: None,
                }
                .nullable(false)
            }
            ListIndex => input_types[0]
                .scalar_type
                .unwrap_list_nth_layer_type(input_types.len() - 1)
                .clone()
                .nullable(true),
            ListSliceLinear { .. } => input_types[0].scalar_type.clone().nullable(true),
            RecordCreate { field_names } => ScalarType::Record {
                fields: field_names
                    .clone()
                    .into_iter()
                    .zip(input_types.into_iter())
                    .collect(),
                custom_id: None,
            }
            .nullable(false),
            SplitPart => ScalarType::String.nullable(in_nullable),
            RegexpMatch => ScalarType::Array(Box::new(ScalarType::String)).nullable(true),
            HmacString | HmacBytes => ScalarType::Bytes.nullable(true),
            ErrorIfNull => input_types[0].scalar_type.clone().nullable(false),
            DateBinTimestamp => ScalarType::Timestamp.nullable(true),
            DateBinTimestampTz => ScalarType::TimestampTz.nullable(true),
        }
    }

    /// Whether the function output is NULL if any of its inputs are NULL.
    pub fn propagates_nulls(&self) -> bool {
        // NOTE: The following is a list of the variadic functions
        // that **DO NOT** propagate nulls.
        !matches!(
            self,
            VariadicFunc::Coalesce
                | VariadicFunc::Greatest
                | VariadicFunc::Least
                | VariadicFunc::Concat
                | VariadicFunc::JsonbBuildArray
                | VariadicFunc::JsonbBuildObject
                | VariadicFunc::ListCreate { .. }
                | VariadicFunc::RecordCreate { .. }
                | VariadicFunc::ArrayCreate { .. }
                | VariadicFunc::ArrayToString { .. }
                | VariadicFunc::ErrorIfNull
        )
    }
}

impl fmt::Display for VariadicFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VariadicFunc::Coalesce => f.write_str("coalesce"),
            VariadicFunc::Greatest => f.write_str("greatest"),
            VariadicFunc::Least => f.write_str("least"),
            VariadicFunc::Concat => f.write_str("concat"),
            VariadicFunc::MakeTimestamp => f.write_str("makets"),
            VariadicFunc::PadLeading => f.write_str("lpad"),
            VariadicFunc::Substr => f.write_str("substr"),
            VariadicFunc::Replace => f.write_str("replace"),
            VariadicFunc::JsonbBuildArray => f.write_str("jsonb_build_array"),
            VariadicFunc::JsonbBuildObject => f.write_str("jsonb_build_object"),
            VariadicFunc::ArrayCreate { .. } => f.write_str("array_create"),
            VariadicFunc::ArrayToString { .. } => f.write_str("array_to_string"),
            VariadicFunc::ArrayIndex { .. } => f.write_str("array_index"),
            VariadicFunc::ListCreate { .. } => f.write_str("list_create"),
            VariadicFunc::RecordCreate { .. } => f.write_str("record_create"),
            VariadicFunc::ListIndex => f.write_str("list_index"),
            VariadicFunc::ListSliceLinear => f.write_str("list_slice_linear"),
            VariadicFunc::SplitPart => f.write_str("split_string"),
            VariadicFunc::RegexpMatch => f.write_str("regexp_match"),
            VariadicFunc::HmacString | VariadicFunc::HmacBytes => f.write_str("hmac"),
            VariadicFunc::ErrorIfNull => f.write_str("error_if_null"),
            VariadicFunc::DateBinTimestamp => f.write_str("timestamp_bin"),
            VariadicFunc::DateBinTimestampTz => f.write_str("timestamptz_bin"),
        }
    }
}

/// An explicit [`Arbitrary`] implementation needed here because of a known
/// `proptest` issue.
///
/// Revert to the derive-macro impementation once the issue[^1] is fixed.
///
/// [^1]: <https://github.com/AltSysrq/proptest/issues/152>
impl Arbitrary for VariadicFunc {
    type Parameters = ();

    type Strategy = Union<BoxedStrategy<Self>>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            Just(VariadicFunc::Coalesce),
            Just(VariadicFunc::Greatest),
            Just(VariadicFunc::Least),
            Just(VariadicFunc::Concat),
            Just(VariadicFunc::MakeTimestamp),
            Just(VariadicFunc::PadLeading),
            Just(VariadicFunc::Substr),
            Just(VariadicFunc::Replace),
            Just(VariadicFunc::JsonbBuildArray),
            Just(VariadicFunc::JsonbBuildObject),
            ScalarType::arbitrary().prop_map(|elem_type| VariadicFunc::ArrayCreate { elem_type }),
            ScalarType::arbitrary().prop_map(|elem_type| VariadicFunc::ArrayToString { elem_type }),
            usize::arbitrary().prop_map(|offset| VariadicFunc::ArrayIndex { offset }),
            ScalarType::arbitrary().prop_map(|elem_type| VariadicFunc::ListCreate { elem_type }),
            Vec::<ColumnName>::arbitrary()
                .prop_map(|field_names| VariadicFunc::RecordCreate { field_names }),
            Just(VariadicFunc::ListIndex),
            Just(VariadicFunc::ListSliceLinear),
            Just(VariadicFunc::SplitPart),
            Just(VariadicFunc::RegexpMatch),
            Just(VariadicFunc::HmacString),
            Just(VariadicFunc::HmacBytes),
            Just(VariadicFunc::ErrorIfNull),
            Just(VariadicFunc::DateBinTimestamp),
            Just(VariadicFunc::DateBinTimestampTz),
        ]
    }
}

impl From<&VariadicFunc> for ProtoVariadicFunc {
    fn from(func: &VariadicFunc) -> Self {
        use crate::scalar::proto_variadic_func::Kind::*;
        use crate::scalar::proto_variadic_func::ProtoRecordCreate;
        let kind = match func {
            VariadicFunc::Coalesce => Coalesce(()),
            VariadicFunc::Greatest => Greatest(()),
            VariadicFunc::Least => Least(()),
            VariadicFunc::Concat => Concat(()),
            VariadicFunc::MakeTimestamp => MakeTimestamp(()),
            VariadicFunc::PadLeading => PadLeading(()),
            VariadicFunc::Substr => Substr(()),
            VariadicFunc::Replace => Replace(()),
            VariadicFunc::JsonbBuildArray => JsonbBuildArray(()),
            VariadicFunc::JsonbBuildObject => JsonbBuildObject(()),
            VariadicFunc::ArrayCreate { elem_type } => ArrayCreate(elem_type.into()),
            VariadicFunc::ArrayToString { elem_type } => ArrayToString(elem_type.into()),
            VariadicFunc::ArrayIndex { offset } => ArrayIndex(offset.into_proto()),
            VariadicFunc::ListCreate { elem_type } => ListCreate(elem_type.into()),
            VariadicFunc::RecordCreate { field_names } => RecordCreate(ProtoRecordCreate {
                field_names: field_names.iter().map(Into::into).collect(),
            }),
            VariadicFunc::ListIndex => ListIndex(()),
            VariadicFunc::ListSliceLinear => ListSliceLinear(()),
            VariadicFunc::SplitPart => SplitPart(()),
            VariadicFunc::RegexpMatch => RegexpMatch(()),
            VariadicFunc::HmacString => HmacString(()),
            VariadicFunc::HmacBytes => HmacBytes(()),
            VariadicFunc::ErrorIfNull => ErrorIfNull(()),
            VariadicFunc::DateBinTimestamp => DateBinTimestamp(()),
            VariadicFunc::DateBinTimestampTz => DateBinTimestampTz(()),
        };
        ProtoVariadicFunc { kind: Some(kind) }
    }
}

impl TryFrom<ProtoVariadicFunc> for VariadicFunc {
    type Error = TryFromProtoError;

    fn try_from(func: ProtoVariadicFunc) -> Result<Self, Self::Error> {
        use crate::scalar::proto_variadic_func::Kind::*;
        use crate::scalar::proto_variadic_func::ProtoRecordCreate;
        if let Some(kind) = func.kind {
            match kind {
                Coalesce(()) => Ok(VariadicFunc::Coalesce),
                Greatest(()) => Ok(VariadicFunc::Greatest),
                Least(()) => Ok(VariadicFunc::Least),
                Concat(()) => Ok(VariadicFunc::Concat),
                MakeTimestamp(()) => Ok(VariadicFunc::MakeTimestamp),
                PadLeading(()) => Ok(VariadicFunc::PadLeading),
                Substr(()) => Ok(VariadicFunc::Substr),
                Replace(()) => Ok(VariadicFunc::Replace),
                JsonbBuildArray(()) => Ok(VariadicFunc::JsonbBuildArray),
                JsonbBuildObject(()) => Ok(VariadicFunc::JsonbBuildObject),
                ArrayCreate(elem_type) => Ok(VariadicFunc::ArrayCreate {
                    elem_type: elem_type.try_into()?,
                }),
                ArrayToString(elem_type) => Ok(VariadicFunc::ArrayToString {
                    elem_type: elem_type.try_into()?,
                }),
                ArrayIndex(offset) => Ok(VariadicFunc::ArrayIndex {
                    offset: usize::from_proto(offset)?,
                }),
                ListCreate(elem_type) => Ok(VariadicFunc::ListCreate {
                    elem_type: elem_type.try_into()?,
                }),
                RecordCreate(ProtoRecordCreate { field_names }) => Ok(VariadicFunc::RecordCreate {
                    field_names: field_names
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<_, TryFromProtoError>>()?,
                }),
                ListIndex(()) => Ok(VariadicFunc::ListIndex),
                ListSliceLinear(()) => Ok(VariadicFunc::ListSliceLinear),
                SplitPart(()) => Ok(VariadicFunc::SplitPart),
                RegexpMatch(()) => Ok(VariadicFunc::RegexpMatch),
                HmacString(()) => Ok(VariadicFunc::HmacString),
                HmacBytes(()) => Ok(VariadicFunc::HmacBytes),
                ErrorIfNull(()) => Ok(VariadicFunc::ErrorIfNull),
                DateBinTimestamp(()) => Ok(VariadicFunc::DateBinTimestamp),
                DateBinTimestampTz(()) => Ok(VariadicFunc::DateBinTimestampTz),
            }
        } else {
            Err(TryFromProtoError::missing_field(
                "`ProtoVariadicFunc::kind`",
            ))
        }
    }
}

#[cfg(test)]
mod test {
    use chrono::prelude::*;
    use proptest::prelude::*;

    use mz_repr::proto::protobuf_roundtrip;

    use super::*;

    #[test]
    fn add_interval_months() {
        let dt = ym(2000, 1);

        assert_eq!(add_timestamp_months(dt, 0).unwrap(), dt);
        assert_eq!(add_timestamp_months(dt, 1).unwrap(), ym(2000, 2));
        assert_eq!(add_timestamp_months(dt, 12).unwrap(), ym(2001, 1));
        assert_eq!(add_timestamp_months(dt, 13).unwrap(), ym(2001, 2));
        assert_eq!(add_timestamp_months(dt, 24).unwrap(), ym(2002, 1));
        assert_eq!(add_timestamp_months(dt, 30).unwrap(), ym(2002, 7));

        // and negatives
        assert_eq!(add_timestamp_months(dt, -1).unwrap(), ym(1999, 12));
        assert_eq!(add_timestamp_months(dt, -12).unwrap(), ym(1999, 1));
        assert_eq!(add_timestamp_months(dt, -13).unwrap(), ym(1998, 12));
        assert_eq!(add_timestamp_months(dt, -24).unwrap(), ym(1998, 1));
        assert_eq!(add_timestamp_months(dt, -30).unwrap(), ym(1997, 7));

        // and going over a year boundary by less than a year
        let dt = ym(1999, 12);
        assert_eq!(add_timestamp_months(dt, 1).unwrap(), ym(2000, 1));
        let end_of_month_dt = NaiveDate::from_ymd(1999, 12, 31).and_hms(9, 9, 9);
        assert_eq!(
            // leap year
            add_timestamp_months(end_of_month_dt, 2).unwrap(),
            NaiveDate::from_ymd(2000, 2, 29).and_hms(9, 9, 9),
        );
        assert_eq!(
            // not leap year
            add_timestamp_months(end_of_month_dt, 14).unwrap(),
            NaiveDate::from_ymd(2001, 2, 28).and_hms(9, 9, 9),
        );
    }

    fn ym(year: i32, month: u32) -> NaiveDateTime {
        NaiveDate::from_ymd(year, month, 1).and_hms(9, 9, 9)
    }

    // Tests that `UnaryFunc::output_type` are consistent with
    // `UnaryFunc::introduces_nulls` and `UnaryFunc::propagates_nulls`.
    // Currently, only unit variants of UnaryFunc are tested because those are
    // the easiest to construct in bulk.
    #[test]
    fn unary_func_introduces_nulls() {
        // Dummy columns to test the nullability of `UnaryFunc::output_type`.
        // It is ok that we're feeding these dummy columns into functions that
        // may not even support this `ScalarType` as an input because we only
        // care about input and output nullabilities.
        let dummy_col_nullable_type = ScalarType::Bool.nullable(true);
        let dummy_col_nonnullable_type = ScalarType::Bool.nullable(false);
        let mut rti = mz_lowertest::ReflectedTypeInfo::default();
        UnaryFunc::add_to_reflected_type_info(&mut rti);
        for (variant, (_, f_types)) in rti.enum_dict["UnaryFunc"].iter() {
            println!("f_types {:?}", f_types);
            if f_types.is_empty() {
                let unary_unit_variant: UnaryFunc =
                    serde_json::from_str(&format!("\"{}\"", variant)).unwrap();
                let output_on_nullable_input = unary_unit_variant
                    .output_type(dummy_col_nullable_type.clone())
                    .nullable;
                let output_on_nonnullable_input = unary_unit_variant
                    .output_type(dummy_col_nonnullable_type.clone())
                    .nullable;
                if unary_unit_variant.introduces_nulls() {
                    // The output type should always be nullable no matter the
                    // input type.
                    assert!(output_on_nullable_input, "failure on {}", variant);
                    assert!(output_on_nonnullable_input, "failure on {}", variant)
                } else {
                    // The output type will be nonnullable if the input type is
                    // nonnullable. If the input type is nullable, the output
                    // type is equal to whether the func propagates nulls.
                    assert!(!output_on_nonnullable_input, "failure on {}", variant);
                    assert_eq!(
                        output_on_nullable_input,
                        unary_unit_variant.propagates_nulls()
                    );
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(4096))]

        #[test]
        fn unmaterializable_func_protobuf_roundtrip(expect in any::<UnmaterializableFunc>()) {
            let actual = protobuf_roundtrip::<_, ProtoUnmaterializableFunc>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

        #[test]
        fn unary_func_protobuf_roundtrip(expect in any::<UnaryFunc>()) {
            let actual = protobuf_roundtrip::<_, ProtoUnaryFunc>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

        #[test]
        fn binary_func_protobuf_roundtrip(expect in any::<BinaryFunc>()) {
            let actual = protobuf_roundtrip::<_, ProtoBinaryFunc>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

        #[test]
        fn variadic_func_protobuf_roundtrip(expect in any::<VariadicFunc>()) {
            let actual = protobuf_roundtrip::<_, ProtoVariadicFunc>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
