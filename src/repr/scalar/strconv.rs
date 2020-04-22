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

use std::{f32, f64};

use chrono::offset::TimeZone;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use failure::{bail, format_err};

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

/// Like `format_bool`, but returns a string with a static lifetime.
///
/// This function should be preferred to `format_bool` when applicable, as it
/// avoids an allocation.
pub fn format_bool_static(b: bool) -> &'static str {
    match b {
        true => "t",
        false => "f",
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
    buf.write_str(format_bool_static(b))
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
    Ok(match s.trim().to_lowercase().as_str() {
        "inf" | "infinity" | "+inf" | "+infinity" => f32::INFINITY,
        "-inf" | "-infinity" => f32::NEG_INFINITY,
        "nan" => f32::NAN,
        s => s.parse()?,
    })
}

/// Writes an `f32` to `buf`.
pub fn format_float32<F>(buf: &mut F, f: f32)
where
    F: FormatBuffer,
{
    if f.is_infinite() {
        if f.is_sign_negative() {
            buf.write_str("-Infinity")
        } else {
            buf.write_str("Infinity")
        }
    } else {
        write!(buf, "{}", f)
    }
}

/// Parses an `f64` from `s`.
pub fn parse_float64(s: &str) -> Result<f64, failure::Error> {
    Ok(match s.trim().to_lowercase().as_str() {
        "inf" | "infinity" | "+inf" | "+infinity" => f64::INFINITY,
        "-inf" | "-infinity" => f64::NEG_INFINITY,
        "nan" => f64::NAN,
        s => s.parse()?,
    })
}

/// Writes an `f64` to `buf`.
pub fn format_float64<F>(buf: &mut F, f: f64)
where
    F: FormatBuffer,
{
    if f.is_infinite() {
        if f.is_sign_negative() {
            buf.write_str("-Infinity")
        } else {
            buf.write_str("Infinity")
        }
    } else {
        write!(buf, "{}", f)
    }
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
fn parse_timestamp_string(s: &str) -> Result<(NaiveDate, NaiveTime, i64), failure::Error> {
    if s.is_empty() {
        bail!("Timestamp string is empty!")
    }

    // PostgreSQL special date-time inputs
    // https://www.postgresql.org/docs/12/datatype-datetime.html#id-1.5.7.13.18.8
    // We should add support for other values here, e.g. infinity
    // which @quodlibetor is willing to add to the chrono package.
    if s == "epoch" {
        return Ok((
            NaiveDate::from_ymd(1970, 1, 1),
            NaiveTime::from_hms(0, 0, 0),
            0,
        ));
    }

    let (ts_string, tz_string) = crate::datetime::split_timestamp_string(s);

    let pdt = ParsedDateTime::build_parsed_datetime_timestamp(&ts_string)?;
    let d: NaiveDate = pdt.compute_date()?;
    let t: NaiveTime = pdt.compute_time()?;

    let offset = if tz_string.is_empty() {
        0
    } else {
        crate::datetime::parse_timezone_offset_second(tz_string)?
    };

    Ok((d, t, offset))
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

/// Parses a `NaiveTime` from `s`, using the following grammar.
///
/// ```text
/// <time value> ::=
///     <hours value> <colon> <minutes value> <colon> <seconds integer value>
///     [ <period> [ <seconds fraction> ] ]
/// ```
pub fn parse_time(s: &str) -> Result<NaiveTime, failure::Error> {
    match ParsedDateTime::build_parsed_datetime_time(&s) {
        Ok(pdt) => pdt.compute_time(),
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

/// Parses a `DateTime<Utc>` from `s`.
pub fn parse_timestamptz(s: &str) -> Result<DateTime<Utc>, failure::Error> {
    let (date, time, offset) = match parse_timestamp_string(s) {
        Ok((date, time, tz_string)) => (date, time, tz_string),
        Err(e) => bail!("Invalid TIMESTAMPTZ '{}': {}", s, e),
    };

    let ts = date.and_time(time);

    let dt_fixed_offset = FixedOffset::east(offset as i32)
        .from_local_datetime(&ts)
        .earliest()
        .ok_or_else(|| format_err!("Invalid tz conversion"))?;

    Ok(DateTime::<Utc>::from_utc(dt_fixed_offset.naive_utc(), Utc))
}

/// Writes a [`DateTime<Utc>`] timestamp to `buf`.
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

pub fn parse_list<T>(
    s: &str,
    mut make_null: impl FnMut() -> T,
    mut parse_elem: impl FnMut(&str) -> Result<T, failure::Error>,
) -> Result<Vec<T>, failure::Error> {
    let mut elems = vec![];
    let mut chars = s.chars().peekable();
    match chars.next() {
        // start of list
        Some('{') => (),
        Some(other) => {
            bail!("expected '{{', found {}", other);
        }
        None => bail!("unexpected end of input"),
    }
    loop {
        match chars.peek().copied() {
            // end of list
            Some('}') => {
                // consume
                chars.next();
                match chars.next() {
                    Some(other) => bail!("unexpected leftover input {}", other),
                    None => break,
                }
            }
            // whitespace, ignore
            Some(' ') => {
                // consume
                chars.next();
                continue;
            }
            // an escaped elem
            Some('"') => {
                chars.next();
                let mut elem_text = String::new();
                loop {
                    match chars.next() {
                        // end of escaped elem
                        Some('"') => break,
                        // a backslash-escaped character
                        Some('\\') => match chars.next() {
                            Some('\\') => elem_text.push('\\'),
                            Some('"') => elem_text.push('"'),
                            Some(other) => bail!("bad escape \\{}", other),
                            None => bail!("unexpected end of input"),
                        },
                        // a normal character
                        Some(other) => elem_text.push(other),
                        None => bail!("unexpected end of input"),
                    }
                }
                elems.push(parse_elem(&elem_text)?);
            }
            // an unescaped elem
            Some(_) => {
                let mut elem_text = String::new();
                loop {
                    match chars.peek().copied() {
                        // end of unescaped elem
                        Some('}') | Some(',') | Some(' ') => break,
                        // a normal character
                        Some(other) => {
                            // consume
                            chars.next();
                            elem_text.push(other);
                        }
                        None => bail!("unexpected end of input"),
                    }
                }
                elems.push(if elem_text.trim() == "NULL" {
                    make_null()
                } else {
                    parse_elem(&elem_text)?
                });
            }
            None => bail!("unexpected end of input"),
        }
        // consume whitespace
        while let Some(' ') = chars.peek() {
            chars.next();
        }
        // look for delimiter
        match chars.next() {
            // another elem
            Some(',') => continue,
            // end of list
            Some('}') => break,
            Some(other) => bail!("expected ',' or '}}', found '{}'", other),
            None => bail!("unexpected end of input"),
        }
    }
    Ok(elems)
}

// can't use a FormatBuffer here because we need to pass a temp buffer to format_elem
pub fn format_list<T>(
    buf: &mut String,
    elems: &[T],
    mut is_null: impl FnMut(&T) -> bool,
    mut format_elem: impl FnMut(&mut String, &T),
) {
    buf.write_str("{");
    let mut elems = elems.iter().peekable();
    while let Some(elem) = elems.next() {
        if is_null(elem) {
            buf.write_str("NULL");
        } else {
            let mut tmp = String::new();
            format_elem(&mut tmp, elem);
            // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-IO
            // > The array output routine will put double quotes around element values if they are empty strings, contain curly braces, delimiter characters, double quotes, backslashes, or white space, or match the word NULL. Double quotes and backslashes embedded in element values will be backslash-escaped.
            let mut needs_escaping = tmp.is_empty() || tmp.trim() == "NULL";
            for chr in tmp.chars() {
                match chr {
                    '{' | '}' | ',' | '"' | '\\' | ' ' => needs_escaping = true,
                    _ => (),
                }
            }
            if !needs_escaping {
                buf.write_str(&tmp);
            } else {
                for chr in tmp.chars() {
                    match chr {
                        '\\' => buf.write_str(r#"\\"#),
                        '"' => buf.write_str(r#"\""#),
                        _ => write!(buf, "{}", chr),
                    }
                }
            }
        }
        if elems.peek().is_some() {
            buf.write_str(",")
        }
    }
    buf.write_str("}");
}
