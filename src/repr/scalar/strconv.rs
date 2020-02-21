// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Routines for converting datum values to and from their string
//! representation.
//!
//! The functions in this module are tightly related to the variants of
//! [`ScalarType`](crate::ScalarType). Each variant has a pair of functions in
//! this module named `parse_VARIANT` and `format_VARIANT`. The type returned
//! by `parse` functions, and the type accepted by `format` functions, will
//! be a type that is easily converted into the [`Datum`](crate::Datum) variant
//! for that type. The functions do not directly convert from `Datum`s to
//! `String`s so that the logic can be reused when `Datum`s are not available or
//! desired, as in the pgrepr crate.
//!
//! The string representations used are exactly the same as the PostgreSQL
//! string representations for the corresponding PostgreSQL type. Deviations
//! should be considered a bug.

use chrono::offset::TimeZone;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use failure::bail;

use ore::fmt::FormatBuffer;

use crate::datetime::{DateTimeField, ParsedDateTime};
use crate::decimal::Decimal;
use crate::jsonb::Jsonb;
use crate::Interval;

/// Parses a [`bool`] from `s`.
///
/// The accepted values are "true", "false", "yes", "no", "on", "off", "1", and
/// "0", or any unambiguous prefix of one of those values. Leading or trailing
/// whitespace is permissible.
pub fn parse_bool(s: &str) -> Result<bool, failure::Error> {
    match s.trim().to_lowercase().as_str() {
        "t" | "tr" | "tru" | "true" | "y" | "ye" | "yes" | "on" | "1" => Ok(true),
        "f" | "fa" | "fal" | "fals" | "false" | "n" | "no" | "of" | "off" | "0" => Ok(false),
        _ => bail!("unable to parse bool"),
    }
}

/// Writes a boolean value into `buf`.
///
/// `true` is encoded as the char `'t'` and `false` is encoded as the char
/// `'f'`.
pub fn format_bool<F>(buf: &mut F, b: bool)
where
    F: FormatBuffer,
{
    buf.write_char(match b {
        true => 't',
        false => 'f',
    })
}

/// Parses an [`i32`] from `s`.
///
/// Valid values are whatever the [`FromStr`] implementation on `i32` accepts,
/// plus leading and trailing whitespace.
pub fn parse_int32(s: &str) -> Result<i32, failure::Error> {
    Ok(s.trim().parse()?)
}

/// Writes an [`i32`] to `buf`.
pub fn format_int32<F>(buf: &mut F, i: i32)
where
    F: FormatBuffer,
{
    write!(buf, "{}", i)
}

/// Parses an `i64` from `s`.
pub fn parse_int64(s: &str) -> Result<i64, failure::Error> {
    Ok(s.trim().parse()?)
}

/// Writes an `i64` to `buf`.
pub fn format_int64<F>(buf: &mut F, i: i64)
where
    F: FormatBuffer,
{
    write!(buf, "{}", i)
}

/// Parses an `f32` from `s`.
pub fn parse_float32(s: &str) -> Result<f32, failure::Error> {
    Ok(s.trim().to_lowercase().parse()?)
}

/// Writes an `f32` to `buf`.
pub fn format_float32<F>(buf: &mut F, f: f32)
where
    F: FormatBuffer,
{
    write!(buf, "{}", f)
}

/// Parses an `f64` from `s`.
pub fn parse_float64(s: &str) -> Result<f64, failure::Error> {
    Ok(s.trim().to_lowercase().parse()?)
}

/// Writes an `f64` to `buf`.
pub fn format_float64<F>(buf: &mut F, f: f64)
where
    F: FormatBuffer,
{
    write!(buf, "{}", f)
}

/// Use the following grammar to parse `s` into:
///
/// - `NaiveDate`
/// - `NaiveTime`
/// - Timezone string
///
/// `NaiveDate` and `NaiveTime` are appropriate to compute a `NaiveDateTime`,
/// which can be used in conjunction with a timezone string to generate a
/// `DateTime<Utc>`.
///
/// ```text
/// <unquoted timestamp string> ::=
///     <date value> <space> <time value> [ <time zone interval> ]
/// <date value> ::=
///     <years value> <minus sign> <months value> <minus sign> <days value>
/// <time zone interval> ::=
///     <sign> <hours value> <colon> <minutes value>
/// ```
fn parse_timestamp_string(s: &str) -> Result<(NaiveDate, NaiveTime, &str), failure::Error> {
    use std::convert::TryInto;

    if s.is_empty() {
        bail!("Timestamp string is empty!")
    }
    let (ts_string, tz_string) = crate::datetime::split_timestamp_string(s);

    // Split timestamp into date and time components.
    let ts_value_split = ts_string.trim().split_whitespace().collect::<Vec<&str>>();

    if ts_value_split.len() > 2 {
        bail!("unknown format")
    }

    let date_pdt = ParsedDateTime::build_parsed_datetime_date(ts_value_split[0])?;
    date_pdt.check_datelike_bounds()?;

    // pdt.hour, pdt.minute, pdt.second are all dropped, which is allowed by
    // PostgreSQL.
    let date = match (date_pdt.year, date_pdt.month, date_pdt.day) {
        (Some(year), Some(month), Some(day)) => {
            let p_err = {
                |e: std::num::TryFromIntError, field: &str| {
                    failure::format_err!("{} in date is invalid: {}", field, e)
                }
            };
            let year = year.unit.try_into().map_err(|e| p_err(e, "Year"))?;
            let month = month.unit.try_into().map_err(|e| p_err(e, "Month"))?;
            let day = day.unit.try_into().map_err(|e| p_err(e, "Day"))?;
            NaiveDate::from_ymd(year, month, day)
        }
        (_, _, _) => bail!("YEAR, MONTH, DAY are all required"),
    };

    let time;

    // Only parse time-component of TIMESTAMP if present.
    if ts_value_split.len() == 2 {
        match parse_time_string(ts_value_split[1]) {
            Ok(t) => time = t,
            Err(e) => bail!("{}", e),
        }
    } else {
        time = NaiveTime::from_hms(0, 0, 0);
    }

    Ok((date, time, tz_string))
}

/// Use the following grammar to parse `s` into a `NaiveTime`.
///
/// ```text
/// <time value> ::=
///     <hours value> <colon> <minutes value> <colon> <seconds integer value>
///     [ <period> [ <seconds fraction> ] ]
/// ```
fn parse_time_string(s: &str) -> Result<NaiveTime, failure::Error> {
    use std::convert::TryInto;

    let pdt = match ParsedDateTime::build_parsed_datetime_time(&s) {
        Ok(pdt) => pdt,
        Err(e) => bail!("{}", e),
    };

    pdt.check_datelike_bounds()?;

    let p_err = {
        |e: std::num::TryFromIntError, field: &str| -> failure::Error {
            failure::format_err!("invalid {} : {}", field, e)
        }
    };

    let hour = match pdt.hour {
        Some(hour) => hour.unit.try_into().map_err(|e| p_err(e, "HOUR"))?,
        None => 0,
    };

    let minute = match pdt.minute {
        Some(minute) => minute.unit.try_into().map_err(|e| p_err(e, "MINUTE"))?,
        None => 0,
    };

    let (second, nano) = match pdt.second {
        Some(second) => {
            let nano: u32 = second
                .fraction
                .try_into()
                .map_err({ |e| p_err(e, "NANOSECOND") })?;
            let second: u32 = second.unit.try_into().map_err(|e| p_err(e, "MINUTE"))?;
            (second, nano)
        }
        None => (0, 0),
    };

    Ok(NaiveTime::from_hms_nano(hour, minute, second, nano))
}

/// Parses a [`NaiveDate`] from `s`.
pub fn parse_date(s: &str) -> Result<NaiveDate, failure::Error> {
    match parse_timestamp_string(s) {
        Ok((date, _, _)) => Ok(date),
        Err(e) => bail!("Invalid DATE '{}': {}", s, e),
    }
}

/// Writes a [`NaiveDate`] to `buf`.
pub fn format_date<F>(buf: &mut F, d: NaiveDate)
where
    F: FormatBuffer,
{
    write!(buf, "{}", d)
}

/// Parses a `NaiveTime` from `s`.
pub fn parse_time(s: &str) -> Result<NaiveTime, failure::Error> {
    match parse_time_string(s) {
        Ok(t) => Ok(t),
        Err(e) => bail!("Invalid TIME '{}': {}", s, e),
    }
}

/// Writes a [`NaiveDateTime`] timestamp to `buf`.
pub fn format_time<F>(buf: &mut F, t: NaiveTime)
where
    F: FormatBuffer,
{
    write!(buf, "{}", t.format("%H:%M:%S"));
    format_nanos(buf, t.nanosecond());
}

/// Parses a `NaiveDateTime` from `s`.
pub fn parse_timestamp(s: &str) -> Result<NaiveDateTime, failure::Error> {
    match parse_timestamp_string(s) {
        Ok((date, time, _)) => Ok(date.and_time(time)),
        Err(e) => bail!("Invalid TIMESTAMP '{}': {}", s, e),
    }
}

/// Writes a [`NaiveDateTime`] timestamp to `buf`.
pub fn format_timestamp<F>(buf: &mut F, ts: NaiveDateTime)
where
    F: FormatBuffer,
{
    write!(buf, "{}", ts.format("%Y-%m-%d %H:%M:%S"));
    format_nanos(buf, ts.timestamp_subsec_nanos());
}

pub fn parse_timestamptz(s: &str) -> Result<DateTime<Utc>, failure::Error> {
    let (date, time, tz_string) = match parse_timestamp_string(s) {
        Ok((date, time, tz_string)) => (date, time, tz_string),
        Err(e) => bail!("Invalid TIMESTAMPTZ '{}': {}", s, e),
    };

    let ts = date.and_time(time);

    let offset = if tz_string.is_empty() {
        FixedOffset::east(0)
    } else {
        match crate::datetime::parse_timezone_offset_second(tz_string) {
            Ok(timezone_offset_second) => FixedOffset::east(timezone_offset_second as i32),
            Err(e) => bail!("Invalid TIMESTAMPTZ '{}': {}", s, e),
        }
    };

    let dt_fixed_offset = offset.from_local_datetime(&ts).earliest().unwrap();
    // .ok_or_else(|| return bail!("Invalid tz conversion"))?;
    Ok(DateTime::<Utc>::from_utc(dt_fixed_offset.naive_utc(), Utc))
}

pub fn format_timestamptz<F>(buf: &mut F, ts: DateTime<Utc>)
where
    F: FormatBuffer,
{
    write!(buf, "{}", ts.format("%Y-%m-%d %H:%M:%S+00"));
    format_nanos(buf, ts.timestamp_subsec_nanos());
}

/// parse
///
/// ```text
/// <unquoted interval string> ::=
///   [ <sign> ] { <year-month literal> | <day-time literal> }
/// <year-month literal> ::=
///     <years value> [ <minus sign> <months value> ]
///   | <months value>
/// <day-time literal> ::=
///     <day-time interval>
///   | <time interval>
/// <day-time interval> ::=
///   <days value> [ <space> <hours value> [ <colon> <minutes value>
///       [ <colon> <seconds value> ] ] ]
/// <time interval> ::=
///     <hours value> [ <colon> <minutes value> [ <colon> <seconds value> ] ]
///   | <minutes value> [ <colon> <seconds value> ]
///   | <seconds value>
/// ```
pub fn parse_interval(s: &str) -> Result<Interval, failure::Error> {
    parse_interval_w_disambiguator(s, DateTimeField::Second)
}

/// Parse an interval string, using a specific sql_parser::ast::DateTimeField
/// to identify ambiguous elements. For more information about this operation,
/// see the doucmentation on ParsedDateTime::build_parsed_datetime_interval.
pub fn parse_interval_w_disambiguator(
    s: &str,
    d: DateTimeField,
) -> Result<Interval, failure::Error> {
    let pdt = match ParsedDateTime::build_parsed_datetime_interval(&s, d) {
        Ok(pdt) => pdt,
        Err(e) => bail!("Invalid INTERVAL '{}': {}", s, e),
    };

    match pdt.compute_interval() {
        Ok(i) => Ok(i),
        Err(e) => bail!("Invalid INTERVAL '{}': {}", s, e),
    }
}

pub fn format_interval<F>(buf: &mut F, iv: Interval)
where
    F: FormatBuffer,
{
    write!(buf, "{}", iv)
}

pub fn parse_decimal(s: &str) -> Result<Decimal, failure::Error> {
    s.trim().parse()
}

pub fn format_decimal<F>(buf: &mut F, d: &Decimal)
where
    F: FormatBuffer,
{
    write!(buf, "{}", d)
}

pub fn parse_bytes(s: &str) -> Result<Vec<u8>, failure::Error> {
    // If the input starts with "\x", then the remaining bytes are hex encoded
    // [0]. Otherwise the bytes use the traditional "escape" format. [1]
    //
    // [0]: https://www.postgresql.org/docs/current/datatype-binary.html#id-1.5.7.12.9
    // [1]: https://www.postgresql.org/docs/current/datatype-binary.html#id-1.5.7.12.10
    if s.starts_with("\\x") {
        Ok(hex::decode(&s[2..])?)
    } else {
        parse_bytes_traditional(s.as_bytes())
    }
}

fn parse_bytes_traditional(buf: &[u8]) -> Result<Vec<u8>, failure::Error> {
    // Bytes are interpreted literally, save for the special escape sequences
    // "\\", which represents a single backslash, and "\NNN", where each N
    // is an octal digit, which represents the byte whose octal value is NNN.
    let mut out = Vec::new();
    let mut bytes = buf.iter().fuse();
    while let Some(&b) = bytes.next() {
        if b != b'\\' {
            out.push(b);
            continue;
        }
        match bytes.next() {
            None => bail!("bytea input ends with escape character"),
            Some(b'\\') => out.push(b'\\'),
            b => match (b, bytes.next(), bytes.next()) {
                (Some(d2 @ b'0'..=b'3'), Some(d1 @ b'0'..=b'7'), Some(d0 @ b'0'..=b'7')) => {
                    out.push(((d2 - b'0') << 6) + ((d1 - b'0') << 3) + (d0 - b'0'));
                }
                _ => bail!("invalid bytea escape sequence"),
            },
        }
    }
    Ok(out)
}

pub fn format_bytes<F>(buf: &mut F, bytes: &[u8])
where
    F: FormatBuffer,
{
    write!(buf, "\\x{}", hex::encode(bytes))
}

pub fn parse_jsonb(s: &str) -> Result<Jsonb, failure::Error> {
    s.trim().parse()
}

pub fn format_jsonb<F>(buf: &mut F, jsonb: &Jsonb)
where
    F: FormatBuffer,
{
    write!(buf, "{}", jsonb)
}

pub fn format_jsonb_pretty<F>(buf: &mut F, jsonb: &Jsonb)
where
    F: FormatBuffer,
{
    write!(buf, "{:#}", jsonb)
}

fn format_nanos<F>(buf: &mut F, mut nanos: u32)
where
    F: FormatBuffer,
{
    if nanos > 0 {
        let mut width = 9;
        while nanos % 10 == 0 {
            width -= 1;
            nanos /= 10;
        }
        write!(buf, ".{:0width$}", nanos, width = width);
    }
}
