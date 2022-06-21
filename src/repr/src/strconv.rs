// Copyright Materialize, Inc. and contributors. All rights reserved.
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

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::num::FpCategory;

use chrono::offset::{Offset, TimeZone};
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use dec::OrderedDecimal;
use fast_float::FastFloat;
use mz_lowertest::MzReflect;
use mz_ore::display::DisplayExt;
use mz_ore::result::ResultExt;
use num_traits::Float as NumFloat;
use once_cell::sync::Lazy;
use proptest_derive::Arbitrary;
use regex::bytes::Regex;
use ryu::Float as RyuFloat;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use mz_ore::fmt::FormatBuffer;
use mz_ore::lex::LexBuf;
use mz_ore::str::StrExt;

use crate::adt::array::ArrayDimension;
use crate::adt::datetime::{self, DateTimeField, ParsedDateTime};
use crate::adt::interval::Interval;
use crate::adt::jsonb::{Jsonb, JsonbRef};
use crate::adt::numeric::{self, Numeric, NUMERIC_DATUM_MAX_PRECISION};
use crate::proto::{RustType, TryFromProtoError};

include!(concat!(env!("OUT_DIR"), "/mz_repr.strconv.rs"));

macro_rules! bail {
    ($($arg:tt)*) => { return Err(format!($($arg)*)) };
}

/// Yes should be provided for types that will *never* return true for [`ElementEscaper::needs_escaping`]
#[derive(Debug)]
pub enum Nestable {
    Yes,
    MayNeedEscaping,
}

/// Parses a [`bool`] from `s`.
///
/// The accepted values are "true", "false", "yes", "no", "on", "off", "1", and
/// "0", or any unambiguous prefix of one of those values. Leading or trailing
/// whitespace is permissible.
pub fn parse_bool(s: &str) -> Result<bool, ParseError> {
    match s.trim().to_lowercase().as_str() {
        "t" | "tr" | "tru" | "true" | "y" | "ye" | "yes" | "on" | "1" => Ok(true),
        "f" | "fa" | "fal" | "fals" | "false" | "n" | "no" | "of" | "off" | "0" => Ok(false),
        _ => Err(ParseError::invalid_input_syntax("boolean", s)),
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
pub fn format_bool<F>(buf: &mut F, b: bool) -> Nestable
where
    F: FormatBuffer,
{
    buf.write_str(format_bool_static(b));
    Nestable::Yes
}

/// Parses an [`i16`] from `s`.
///
/// Valid values are whatever the [`std::str::FromStr`] implementation on `i16` accepts,
/// plus leading and trailing whitespace.
pub fn parse_int16(s: &str) -> Result<i16, ParseError> {
    s.trim()
        .parse()
        .map_err(|e| ParseError::invalid_input_syntax("smallint", s).with_details(e))
}

/// Writes an [`i16`] to `buf`.
pub fn format_int16<F>(buf: &mut F, i: i16) -> Nestable
where
    F: FormatBuffer,
{
    write!(buf, "{}", i);
    Nestable::Yes
}

/// Parses an [`i32`] from `s`.
///
/// Valid values are whatever the [`std::str::FromStr`] implementation on `i32` accepts,
/// plus leading and trailing whitespace.
pub fn parse_int32(s: &str) -> Result<i32, ParseError> {
    s.trim()
        .parse()
        .map_err(|e| ParseError::invalid_input_syntax("integer", s).with_details(e))
}

/// Writes an [`i32`] to `buf`.
pub fn format_int32<F>(buf: &mut F, i: i32) -> Nestable
where
    F: FormatBuffer,
{
    write!(buf, "{}", i);
    Nestable::Yes
}

/// Parses an `i64` from `s`.
pub fn parse_int64(s: &str) -> Result<i64, ParseError> {
    s.trim()
        .parse()
        .map_err(|e| ParseError::invalid_input_syntax("bigint", s).with_details(e))
}

/// Writes an `i64` to `buf`.
pub fn format_int64<F>(buf: &mut F, i: i64) -> Nestable
where
    F: FormatBuffer,
{
    write!(buf, "{}", i);
    Nestable::Yes
}

/// Writes an OID to `buf`.
pub fn format_oid<F>(buf: &mut F, oid: u32) -> Nestable
where
    F: FormatBuffer,
{
    write!(buf, "{}", oid);
    Nestable::Yes
}

/// Parses an OID from `s`.
pub fn parse_oid(s: &str) -> Result<u32, ParseError> {
    // For historical reasons in PostgreSQL, OIDs are parsed as `i32`s and then
    // reinterpreted as `u32`s.
    //
    // Do not use this as a model for behavior in other contexts. OIDs should
    // not in general be thought of as freely convertible from `i32`s.
    let oid: i32 = s
        .trim()
        .parse()
        .map_err(|e| ParseError::invalid_input_syntax("oid", s).with_details(e))?;
    Ok(u32::from_ne_bytes(oid.to_ne_bytes()))
}

fn parse_float<Fl>(type_name: &'static str, s: &str) -> Result<Fl, ParseError>
where
    Fl: NumFloat + FastFloat,
{
    // Matching PostgreSQL's float parsing behavior is tricky. PostgreSQL's
    // implementation delegates almost entirely to strtof(3)/strtod(3), which
    // will report an out-of-range error if a number was rounded to zero or
    // infinity. For example, parsing "1e70" as a 32-bit float will yield an
    // out-of-range error because it is rounded to infinity, but parsing an
    // explicitly-specified "inf" will yield infinity without an error.
    //
    // To @benesch's knowledge, there is no Rust implementation of float parsing
    // that reports whether underflow or overflow occurred. So we figure it out
    // ourselves after the fact. If fast_float returns infinity and the input
    // was not an explicitly-specified infinity, then we know overflow occurred.
    // If fast_float returns zero and the input was not an explicitly-specified
    // zero, then we know underflow occurred.

    // Matches `0`, `-0`, `+0`, `000000.00000`, `0.0e10`, et al.
    static ZERO_RE: Lazy<Regex> = Lazy::new(|| Regex::new("(?i-u)^[-+]?0+(\\.0+)?(e|$)").unwrap());
    // Matches `inf`, `-inf`, `+inf`, `infinity`, et al.
    static INF_RE: Lazy<Regex> = Lazy::new(|| Regex::new("(?i-u)^[-+]?inf").unwrap());

    let buf = s.trim().as_bytes();
    let f: Fl =
        fast_float::parse(buf).map_err(|_| ParseError::invalid_input_syntax(type_name, s))?;
    match f.classify() {
        FpCategory::Infinite if !INF_RE.is_match(buf) => {
            Err(ParseError::out_of_range(type_name, s))
        }
        FpCategory::Zero if !ZERO_RE.is_match(buf) => Err(ParseError::out_of_range(type_name, s)),
        _ => Ok(f),
    }
}

fn format_float<F, Fl>(buf: &mut F, f: Fl) -> Nestable
where
    F: FormatBuffer,
    Fl: NumFloat + RyuFloat,
{
    // Use ryu rather than the standard library. ryu uses scientific notation
    // when possible, which better matches PostgreSQL. The standard library's
    // `ToString` implementations print all available digits, which is rather
    // verbose.
    //
    // Note that we have to fix up ryu's formatting in a few cases to match
    // PostgreSQL. PostgreSQL spells out "Infinity" in full, never emits a
    // trailing ".0", formats positive exponents as e.g. "1e+10" rather than
    // "1e10", and emits a negative sign for negative zero. If we need to speed
    // up float formatting, we can look into forking ryu and making these edits
    // directly, but for now it doesn't seem worth it.

    match f.classify() {
        FpCategory::Infinite if f.is_sign_negative() => buf.write_str("-Infinity"),
        FpCategory::Infinite => buf.write_str("Infinity"),
        FpCategory::Nan => buf.write_str("NaN"),
        FpCategory::Zero if f.is_sign_negative() => buf.write_str("-0"),
        _ => {
            debug_assert!(f.is_finite());
            let mut ryu_buf = ryu::Buffer::new();
            let mut s = ryu_buf.format_finite(f);
            if let Some(trimmed) = s.strip_suffix(".0") {
                s = trimmed;
            }
            let mut chars = s.chars().peekable();
            while let Some(ch) = chars.next() {
                buf.write_char(ch);
                if ch == 'e' && chars.peek() != Some(&'-') {
                    buf.write_char('+');
                }
            }
        }
    }

    Nestable::Yes
}

/// Parses an `f32` from `s`.
pub fn parse_float32(s: &str) -> Result<f32, ParseError> {
    parse_float("real", s)
}

/// Writes an `f32` to `buf`.
pub fn format_float32<F>(buf: &mut F, f: f32) -> Nestable
where
    F: FormatBuffer,
{
    format_float(buf, f)
}

/// Parses an `f64` from `s`.
pub fn parse_float64(s: &str) -> Result<f64, ParseError> {
    parse_float("double precision", s)
}

/// Writes an `f64` to `buf`.
pub fn format_float64<F>(buf: &mut F, f: f64) -> Nestable
where
    F: FormatBuffer,
{
    format_float(buf, f)
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
fn parse_timestamp_string(s: &str) -> Result<(NaiveDate, NaiveTime, datetime::Timezone), String> {
    if s.is_empty() {
        return Err("timestamp string is empty".into());
    }

    // PostgreSQL special date-time inputs
    // https://www.postgresql.org/docs/12/datatype-datetime.html#id-1.5.7.13.18.8
    // We should add support for other values here, e.g. infinity
    // which @quodlibetor is willing to add to the chrono package.
    if s == "epoch" {
        return Ok((
            NaiveDate::from_ymd(1970, 1, 1),
            NaiveTime::from_hms(0, 0, 0),
            Default::default(),
        ));
    }

    let (ts_string, tz_string) = datetime::split_timestamp_string(s);

    let pdt = ParsedDateTime::build_parsed_datetime_timestamp(&ts_string)?;
    let d: NaiveDate = pdt.compute_date()?;
    let t: NaiveTime = pdt.compute_time()?;

    let offset = if tz_string.is_empty() {
        Default::default()
    } else {
        tz_string.parse()?
    };

    Ok((d, t, offset))
}

/// Parses a [`NaiveDate`] from `s`.
pub fn parse_date(s: &str) -> Result<NaiveDate, ParseError> {
    match parse_timestamp_string(s) {
        Ok((date, _, _)) => Ok(date),
        Err(e) => Err(ParseError::invalid_input_syntax("date", s).with_details(e)),
    }
}

/// Writes a [`NaiveDate`] to `buf`.
pub fn format_date<F>(buf: &mut F, d: NaiveDate) -> Nestable
where
    F: FormatBuffer,
{
    let (year_ad, year) = d.year_ce();
    write!(buf, "{:04}-{}", year, d.format("%m-%d"));
    if !year_ad {
        write!(buf, " BC");
    }
    Nestable::Yes
}

/// Parses a `NaiveTime` from `s`, using the following grammar.
///
/// ```text
/// <time value> ::=
///     <hours value> <colon> <minutes value> <colon> <seconds integer value>
///     [ <period> [ <seconds fraction> ] ]
/// ```
pub fn parse_time(s: &str) -> Result<NaiveTime, ParseError> {
    ParsedDateTime::build_parsed_datetime_time(&s)
        .and_then(|pdt| pdt.compute_time())
        .map_err(|e| ParseError::invalid_input_syntax("time", s).with_details(e))
}

/// Writes a [`NaiveDateTime`] timestamp to `buf`.
pub fn format_time<F>(buf: &mut F, t: NaiveTime) -> Nestable
where
    F: FormatBuffer,
{
    write!(buf, "{}", t.format("%H:%M:%S"));
    format_nanos_to_micros(buf, t.nanosecond());
    Nestable::Yes
}

/// Parses a `NaiveDateTime` from `s`.
pub fn parse_timestamp(s: &str) -> Result<NaiveDateTime, ParseError> {
    match parse_timestamp_string(s) {
        Ok((date, time, _)) => Ok(date.and_time(time)),
        Err(e) => Err(ParseError::invalid_input_syntax("timestamp", s).with_details(e)),
    }
}

/// Writes a [`NaiveDateTime`] timestamp to `buf`.
pub fn format_timestamp<F>(buf: &mut F, ts: NaiveDateTime) -> Nestable
where
    F: FormatBuffer,
{
    let (year_ad, year) = ts.year_ce();
    write!(buf, "{:04}-{}", year, ts.format("%m-%d %H:%M:%S"));
    format_nanos_to_micros(buf, ts.timestamp_subsec_nanos());
    if !year_ad {
        write!(buf, " BC");
    }
    // This always needs escaping because of the whitespace
    Nestable::MayNeedEscaping
}

/// Parses a `DateTime<Utc>` from `s`. See `mz_expr::scalar::func::timezone_timestamp` for timezone anomaly considerations.
pub fn parse_timestamptz(s: &str) -> Result<DateTime<Utc>, ParseError> {
    parse_timestamp_string(s)
        .and_then(|(date, time, timezone)| {
            use datetime::Timezone::*;
            let mut dt = date.and_time(time);
            let offset = match timezone {
                FixedOffset(offset) => offset,
                Tz(tz) => match tz.offset_from_local_datetime(&dt).latest() {
                    Some(offset) => offset.fix(),
                    None => {
                        dt += Duration::hours(1);
                        tz.offset_from_local_datetime(&dt)
                            .latest()
                            .ok_or_else(|| "invalid timezone conversion".to_owned())?
                            .fix()
                    }
                },
            };
            Ok(DateTime::from_utc(dt - offset, Utc))
        })
        .map_err(|e| {
            ParseError::invalid_input_syntax("timestamp with time zone", s).with_details(e)
        })
}

/// Writes a [`DateTime<Utc>`] timestamp to `buf`.
pub fn format_timestamptz<F>(buf: &mut F, ts: DateTime<Utc>) -> Nestable
where
    F: FormatBuffer,
{
    let (year_ad, year) = ts.year_ce();
    write!(buf, "{:04}-{}", year, ts.format("%m-%d %H:%M:%S"));
    format_nanos_to_micros(buf, ts.timestamp_subsec_nanos());
    write!(buf, "+00");
    if !year_ad {
        write!(buf, " BC");
    }
    // This always needs escaping because of the whitespace
    Nestable::MayNeedEscaping
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
pub fn parse_interval(s: &str) -> Result<Interval, ParseError> {
    parse_interval_w_disambiguator(s, None, DateTimeField::Second)
}

/// Parse an interval string, using an optional leading precision for time (H:M:S)
/// and a specific mz_sql_parser::ast::DateTimeField to identify ambiguous elements.
/// For more information about this operation, see the documentation on
/// ParsedDateTime::build_parsed_datetime_interval.
pub fn parse_interval_w_disambiguator(
    s: &str,
    leading_time_precision: Option<DateTimeField>,
    d: DateTimeField,
) -> Result<Interval, ParseError> {
    ParsedDateTime::build_parsed_datetime_interval(&s, leading_time_precision, d)
        .and_then(|pdt| pdt.compute_interval())
        .map_err(|e| ParseError::invalid_input_syntax("interval", s).with_details(e))
}

pub fn format_interval<F>(buf: &mut F, iv: Interval) -> Nestable
where
    F: FormatBuffer,
{
    write!(buf, "{}", iv);
    Nestable::MayNeedEscaping
}

pub fn parse_numeric(s: &str) -> Result<OrderedDecimal<Numeric>, ParseError> {
    let mut cx = numeric::cx_datum();
    let mut n = match cx.parse(s.trim()) {
        Ok(n) => n,
        Err(..) => {
            return Err(ParseError::invalid_input_syntax("numeric", s));
        }
    };

    let cx_status = cx.status();

    // Check for values that can only be generated by invalid syntax.
    if (n.is_infinite() && !cx_status.overflow())
        || (n.is_nan() && n.is_negative())
        || n.is_signaling_nan()
    {
        return Err(ParseError::invalid_input_syntax("numeric", s));
    }

    // Process value; only errors if value is out of range of numeric's max precision.
    let out_of_range = numeric::munge_numeric(&mut n).is_err();

    if cx_status.overflow() || cx_status.subnormal() || out_of_range {
        Err(ParseError::out_of_range("numeric", s).with_details(format!(
            "exceeds maximum precision {}",
            NUMERIC_DATUM_MAX_PRECISION
        )))
    } else {
        Ok(OrderedDecimal(n))
    }
}

pub fn format_numeric<F>(buf: &mut F, n: &OrderedDecimal<Numeric>) -> Nestable
where
    F: FormatBuffer,
{
    write!(buf, "{}", n.0.to_standard_notation_string());
    Nestable::Yes
}

pub fn format_string<F>(buf: &mut F, s: &str) -> Nestable
where
    F: FormatBuffer,
{
    buf.write_str(s);
    Nestable::MayNeedEscaping
}

pub fn parse_bytes(s: &str) -> Result<Vec<u8>, ParseError> {
    // If the input starts with "\x", then the remaining bytes are hex encoded
    // [0]. Otherwise the bytes use the traditional "escape" format. [1]
    //
    // [0]: https://www.postgresql.org/docs/current/datatype-binary.html#id-1.5.7.12.9
    // [1]: https://www.postgresql.org/docs/current/datatype-binary.html#id-1.5.7.12.10
    if let Some(remainder) = s.strip_prefix(r"\x") {
        parse_bytes_hex(remainder).map_err(|e| {
            ParseError::invalid_input_syntax("bytea", s).with_details(e.to_string_alt())
        })
    } else {
        parse_bytes_traditional(s)
    }
}

pub fn parse_bytes_hex(s: &str) -> Result<Vec<u8>, ParseHexError> {
    // Can't use `hex::decode` here, as it doesn't tolerate whitespace
    // between encoded bytes.

    let decode_nibble = |b| match b {
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        b'0'..=b'9' => Ok(b - b'0'),
        _ => Err(ParseHexError::InvalidHexDigit(char::from(b))),
    };

    let mut buf = vec![];
    let mut nibbles = s.as_bytes().iter().copied();
    while let Some(n) = nibbles.next() {
        if let b' ' | b'\n' | b'\t' | b'\r' = n {
            continue;
        }
        let n = decode_nibble(n)?;
        let n2 = match nibbles.next() {
            None => return Err(ParseHexError::OddLength),
            Some(n2) => decode_nibble(n2)?,
        };
        buf.push((n << 4) | n2);
    }
    Ok(buf)
}

pub fn parse_bytes_traditional(s: &str) -> Result<Vec<u8>, ParseError> {
    // Bytes are interpreted literally, save for the special escape sequences
    // "\\", which represents a single backslash, and "\NNN", where each N
    // is an octal digit, which represents the byte whose octal value is NNN.
    let mut out = Vec::new();
    let mut bytes = s.as_bytes().iter().fuse();
    while let Some(&b) = bytes.next() {
        if b != b'\\' {
            out.push(b);
            continue;
        }
        match bytes.next() {
            None => {
                return Err(ParseError::invalid_input_syntax("bytea", s)
                    .with_details("ends with escape character"))
            }
            Some(b'\\') => out.push(b'\\'),
            b => match (b, bytes.next(), bytes.next()) {
                (Some(d2 @ b'0'..=b'3'), Some(d1 @ b'0'..=b'7'), Some(d0 @ b'0'..=b'7')) => {
                    out.push(((d2 - b'0') << 6) + ((d1 - b'0') << 3) + (d0 - b'0'));
                }
                _ => {
                    return Err(ParseError::invalid_input_syntax("bytea", s)
                        .with_details("invalid escape sequence"))
                }
            },
        }
    }
    Ok(out)
}

pub fn format_bytes<F>(buf: &mut F, bytes: &[u8]) -> Nestable
where
    F: FormatBuffer,
{
    write!(buf, "\\x{}", hex::encode(bytes));
    Nestable::Yes
}

pub fn parse_jsonb(s: &str) -> Result<Jsonb, ParseError> {
    s.trim()
        .parse()
        .map_err(|e| ParseError::invalid_input_syntax("jsonb", s).with_details(e))
}

pub fn format_jsonb<F>(buf: &mut F, jsonb: JsonbRef) -> Nestable
where
    F: FormatBuffer,
{
    write!(buf, "{}", jsonb);
    Nestable::MayNeedEscaping
}

pub fn format_jsonb_pretty<F>(buf: &mut F, jsonb: JsonbRef)
where
    F: FormatBuffer,
{
    write!(buf, "{:#}", jsonb)
}

pub fn parse_uuid(s: &str) -> Result<Uuid, ParseError> {
    s.trim()
        .parse()
        .map_err(|e| ParseError::invalid_input_syntax("uuid", s).with_details(e))
}

pub fn format_uuid<F>(buf: &mut F, uuid: Uuid) -> Nestable
where
    F: FormatBuffer,
{
    write!(buf, "{}", uuid);
    Nestable::Yes
}

fn format_nanos_to_micros<F>(buf: &mut F, nanos: u32)
where
    F: FormatBuffer,
{
    if nanos >= 500 {
        let mut micros = nanos / 1000;
        let rem = nanos % 1000;
        if rem >= 500 {
            micros += 1;
        }
        // strip trailing zeros
        let mut width = 6;
        while micros % 10 == 0 {
            width -= 1;
            micros /= 10;
        }
        write!(buf, ".{:0width$}", micros, width = width);
    }
}

pub fn parse_array<'a, T, E>(
    s: &'a str,
    make_null: impl FnMut() -> T,
    gen_elem: impl FnMut(Cow<'a, str>) -> Result<T, E>,
) -> Result<Vec<T>, ParseError>
where
    E: fmt::Display,
{
    parse_array_inner(s, make_null, gen_elem)
        .map_err(|details| ParseError::invalid_input_syntax("array", s).with_details(details))
}

fn parse_array_inner<'a, T, E>(
    s: &'a str,
    mut make_null: impl FnMut() -> T,
    mut gen_elem: impl FnMut(Cow<'a, str>) -> Result<T, E>,
) -> Result<Vec<T>, String>
where
    E: fmt::Display,
{
    let mut elems = vec![];
    let buf = &mut LexBuf::new(s);

    if !buf.consume('{') {
        bail!("malformed array literal: missing opening left brace");
    }

    let mut gen = |elem| gen_elem(elem).map_err_to_string();
    let is_special_char = |c| matches!(c, '{' | '}' | ',' | '\\' | '"');
    let is_end_of_literal = |c| matches!(c, ',' | '}');

    loop {
        buf.take_while(|ch| ch.is_ascii_whitespace());
        match buf.next() {
            Some('}') => break,
            _ if elems.len() == 0 => {
                buf.prev();
            }
            Some(',') => {}
            Some(c) => bail!("expected ',' or '}}', got '{}'", c),
            None => bail!("unexpected end of input"),
        }
        buf.take_while(|ch| ch.is_ascii_whitespace());

        let elem = match buf.peek() {
            Some('"') => gen(lex_quoted_element(buf)?)?,
            Some('{') => bail!("parsing multi-dimensional arrays is not supported"),
            Some(_) => match lex_unquoted_element(buf, is_special_char, is_end_of_literal)? {
                Some(elem) => gen(elem)?,
                None => make_null(),
            },
            None => bail!("unexpected end of input"),
        };
        elems.push(elem);
    }

    if buf.next().is_some() {
        bail!("malformed array literal: junk after closing right brace");
    }

    Ok(elems)
}

pub fn parse_list<'a, T, E>(
    s: &'a str,
    is_element_type_list: bool,
    make_null: impl FnMut() -> T,
    gen_elem: impl FnMut(Cow<'a, str>) -> Result<T, E>,
) -> Result<Vec<T>, ParseError>
where
    E: fmt::Display,
{
    parse_list_inner(s, is_element_type_list, make_null, gen_elem)
        .map_err(|details| ParseError::invalid_input_syntax("list", s).with_details(details))
}

// `parse_list_inner`'s separation from `parse_list` simplifies error handling
// by allowing subprocedures to return `String` errors.
fn parse_list_inner<'a, T, E>(
    s: &'a str,
    is_element_type_list: bool,
    mut make_null: impl FnMut() -> T,
    mut gen_elem: impl FnMut(Cow<'a, str>) -> Result<T, E>,
) -> Result<Vec<T>, String>
where
    E: fmt::Display,
{
    let mut elems = vec![];
    let buf = &mut LexBuf::new(s);

    // Consume opening paren.
    if !buf.consume('{') {
        bail!(
            "expected '{{', found {}",
            match buf.next() {
                Some(c) => format!("{}", c),
                None => "empty string".to_string(),
            }
        )
    }

    // Simplifies calls to `gen_elem` by handling errors
    let mut gen = |elem| gen_elem(elem).map_err_to_string();
    let is_special_char = |c| matches!(c, '{' | '}' | ',' | '\\' | '"');
    let is_end_of_literal = |c| matches!(c, ',' | '}');

    // Consume elements.
    loop {
        buf.take_while(|ch| ch.is_ascii_whitespace());
        // Check for terminals.
        match buf.next() {
            Some('}') => {
                break;
            }
            _ if elems.len() == 0 => {
                buf.prev();
            }
            Some(',') => {}
            Some(c) => bail!("expected ',' or '}}', got '{}'", c),
            None => bail!("unexpected end of input"),
        }

        buf.take_while(|ch| ch.is_ascii_whitespace());
        // Get elements.
        let elem = match buf.peek() {
            Some('"') => gen(lex_quoted_element(buf)?)?,
            Some('{') => {
                if !is_element_type_list {
                    bail!(
                        "unescaped '{{' at beginning of element; perhaps you \
                        want a nested list, e.g. '{{a}}'::text list list"
                    )
                }
                gen(lex_embedded_element(buf)?)?
            }
            Some(_) => match lex_unquoted_element(buf, is_special_char, is_end_of_literal)? {
                Some(elem) => gen(elem)?,
                None => make_null(),
            },
            None => bail!("unexpected end of input"),
        };
        elems.push(elem);
    }

    buf.take_while(|ch| ch.is_ascii_whitespace());
    if let Some(c) = buf.next() {
        bail!(
            "malformed array literal; contains '{}' after terminal '}}'",
            c
        )
    }

    Ok(elems)
}

pub fn parse_legacy_vector<'a, T, E>(
    s: &'a str,
    gen_elem: impl FnMut(Cow<'a, str>) -> Result<T, E>,
) -> Result<Vec<T>, ParseError>
where
    E: fmt::Display,
{
    parse_legacy_vector_inner(s, gen_elem)
        .map_err(|details| ParseError::invalid_input_syntax("int2vector", s).with_details(details))
}

pub fn parse_legacy_vector_inner<'a, T, E>(
    s: &'a str,
    mut gen_elem: impl FnMut(Cow<'a, str>) -> Result<T, E>,
) -> Result<Vec<T>, String>
where
    E: fmt::Display,
{
    let mut elems = vec![];
    let buf = &mut LexBuf::new(s);

    let mut gen = |elem| gen_elem(elem).map_err_to_string();

    loop {
        buf.take_while(|ch| ch.is_ascii_whitespace());
        match buf.peek() {
            Some(_) => {
                let elem = buf.take_while(|ch| !ch.is_ascii_whitespace());
                elems.push(gen(elem.into())?);
            }
            None => break,
        }
    }

    Ok(elems)
}

fn lex_quoted_element<'a>(buf: &mut LexBuf<'a>) -> Result<Cow<'a, str>, String> {
    assert!(buf.consume('"'));
    let s = buf.take_while(|ch| !matches!(ch, '"' | '\\'));

    // `Cow::Borrowed` optimization for quoted strings without escapes
    if let Some('"') = buf.peek() {
        buf.next();
        return Ok(s.into());
    }

    let mut s = s.to_string();
    loop {
        match buf.next() {
            Some('\\') => match buf.next() {
                Some(c) => s.push(c),
                None => bail!("unterminated quoted string"),
            },
            Some('"') => break,
            Some(c) => s.push(c),
            None => bail!("unterminated quoted string"),
        }
    }
    Ok(s.into())
}

fn lex_embedded_element<'a>(buf: &mut LexBuf<'a>) -> Result<Cow<'a, str>, String> {
    let pos = buf.pos();
    assert!(matches!(buf.next(), Some('{')));
    let mut depth = 1;
    let mut in_escape = false;
    while depth > 0 {
        match buf.next() {
            Some('\\') => {
                buf.next(); // Next character is escaped, so ignore it
            }
            Some('"') => in_escape = !in_escape, // Begin or end escape
            Some('{') if !in_escape => depth += 1,
            Some('}') if !in_escape => depth -= 1,
            Some(_) => (),
            None => bail!("unterminated embedded element"),
        }
    }
    let s = &buf.inner()[pos..buf.pos()];
    Ok(Cow::Borrowed(s))
}

// Result of `None` indicates element is NULL.
fn lex_unquoted_element<'a>(
    buf: &mut LexBuf<'a>,
    is_special_char: impl Fn(char) -> bool,
    is_end_of_literal: impl Fn(char) -> bool,
) -> Result<Option<Cow<'a, str>>, String> {
    // first char is guaranteed to be non-whitespace
    assert!(!buf.peek().unwrap().is_ascii_whitespace());

    let s = buf.take_while(|ch| !is_special_char(ch) && !ch.is_ascii_whitespace());

    // `Cow::Borrowed` optimization for elements without special characters.
    match buf.peek() {
        Some(',') | Some('}') if !s.is_empty() => {
            return Ok(if s.to_uppercase() == "NULL" {
                None
            } else {
                Some(s.into())
            });
        }
        _ => {}
    }

    // Track whether there are any escaped characters to determine if the string
    // "NULL" should be treated as a NULL, or if it had any escaped characters
    // and should be treated as the string "NULL".
    let mut escaped_char = false;

    let mut s = s.to_string();
    // As we go, we keep track of where to truncate to in order to remove any
    // trailing whitespace.
    let mut trimmed_len = s.len();
    loop {
        match buf.next() {
            Some('\\') => match buf.next() {
                Some(c) => {
                    escaped_char = true;
                    s.push(c);
                    trimmed_len = s.len();
                }
                None => return Err("unterminated element".into()),
            },
            Some(c) if is_end_of_literal(c) => {
                // End of literal characters as the first character indicates
                // a missing element definition.
                if s.is_empty() {
                    bail!("malformed literal; missing element")
                }
                buf.prev();
                break;
            }
            Some(c) if is_special_char(c) => {
                bail!("malformed literal; must escape special character '{}'", c)
            }
            Some(c) => {
                s.push(c);
                if !c.is_ascii_whitespace() {
                    trimmed_len = s.len();
                }
            }
            None => bail!("unterminated element"),
        }
    }
    s.truncate(trimmed_len);
    Ok(if s.to_uppercase() == "NULL" && !escaped_char {
        None
    } else {
        Some(Cow::Owned(s))
    })
}

pub fn parse_map<'a, V, E>(
    s: &'a str,
    is_value_type_map: bool,
    gen_elem: impl FnMut(Cow<'a, str>) -> Result<V, E>,
) -> Result<BTreeMap<String, V>, ParseError>
where
    E: fmt::Display,
{
    parse_map_inner(s, is_value_type_map, gen_elem)
        .map_err(|details| ParseError::invalid_input_syntax("map", s).with_details(details))
}

fn parse_map_inner<'a, V, E>(
    s: &'a str,
    is_value_type_map: bool,
    mut gen_elem: impl FnMut(Cow<'a, str>) -> Result<V, E>,
) -> Result<BTreeMap<String, V>, String>
where
    E: fmt::Display,
{
    let mut map = BTreeMap::new();
    let buf = &mut LexBuf::new(s);

    // Consume opening paren.
    if !buf.consume('{') {
        bail!(
            "expected '{{', found {}",
            match buf.next() {
                Some(c) => format!("{}", c),
                None => "empty string".to_string(),
            }
        )
    }

    // Simplifies calls to generators by handling errors
    let gen_key = |key: Option<Cow<'a, str>>| -> Result<String, String> {
        match key {
            Some(Cow::Owned(s)) => Ok(s),
            Some(Cow::Borrowed(s)) => Ok(s.to_owned()),
            None => Err("expected key".to_owned()),
        }
    };
    let mut gen_value = |elem| gen_elem(elem).map_err_to_string();
    let is_special_char = |c| matches!(c, '{' | '}' | ',' | '"' | '=' | '>' | '\\');
    let is_end_of_literal = |c| matches!(c, ',' | '}' | '=');

    loop {
        // Check for terminals.
        buf.take_while(|ch| ch.is_ascii_whitespace());
        match buf.next() {
            Some('}') => break,
            _ if map.len() == 0 => {
                buf.prev();
            }
            Some(',') => {}
            Some(c) => bail!("expected ',' or end of input, got '{}'", c),
            None => bail!("unexpected end of input"),
        }

        // Get key.
        buf.take_while(|ch| ch.is_ascii_whitespace());
        let key = match buf.peek() {
            Some('"') => Some(lex_quoted_element(buf)?),
            Some(_) => lex_unquoted_element(buf, is_special_char, is_end_of_literal)?,
            None => bail!("unexpected end of input"),
        };
        let key = gen_key(key)?;

        // Assert mapping arrow (=>) is present.
        buf.take_while(|ch| ch.is_ascii_whitespace());
        if !buf.consume('=') || !buf.consume('>') {
            bail!("expected =>")
        }

        // Get value.
        buf.take_while(|ch| ch.is_ascii_whitespace());
        let value = match buf.peek() {
            Some('"') => Some(lex_quoted_element(buf)?),
            Some('{') => {
                if !is_value_type_map {
                    bail!(
                        "unescaped '{{' at beginning of value; perhaps you \
                           want a nested map, e.g. '{{a=>{{a=>1}}}}'::map[text=>map[text=>int]]"
                    )
                }
                Some(lex_embedded_element(buf)?)
            }
            Some(_) => lex_unquoted_element(buf, is_special_char, is_end_of_literal)?,
            None => bail!("unexpected end of input"),
        };
        let value = gen_value(value.unwrap())?;

        // Insert elements.
        map.insert(key, value);
    }
    Ok(map)
}

pub fn format_map<F, T, E>(
    buf: &mut F,
    elems: impl IntoIterator<Item = (impl AsRef<str>, T)>,
    mut format_elem: impl FnMut(MapValueWriter<F>, T) -> Result<Nestable, E>,
) -> Result<Nestable, E>
where
    F: FormatBuffer,
{
    buf.write_char('{');
    let mut elems = elems.into_iter().peekable();
    while let Some((key, value)) = elems.next() {
        // Map key values are always Strings, which always evaluate to
        // Nestable::MayNeedEscaping.
        let key_start = buf.len();
        buf.write_str(key.as_ref());
        escape_elem::<_, MapElementEscaper>(buf, key_start);

        buf.write_str("=>");

        let value_start = buf.len();
        if let Nestable::MayNeedEscaping = format_elem(MapValueWriter(buf), value)? {
            escape_elem::<_, MapElementEscaper>(buf, value_start);
        }

        if elems.peek().is_some() {
            buf.write_char(',');
        }
    }
    buf.write_char('}');
    Ok(Nestable::Yes)
}

pub fn format_array<F, T, E>(
    buf: &mut F,
    dims: &[ArrayDimension],
    elems: impl IntoIterator<Item = T>,
    mut format_elem: impl FnMut(ListElementWriter<F>, T) -> Result<Nestable, E>,
) -> Result<Nestable, E>
where
    F: FormatBuffer,
{
    format_array_inner(buf, dims, &mut elems.into_iter(), &mut format_elem)?;
    Ok(Nestable::Yes)
}

pub fn format_array_inner<F, T, E>(
    buf: &mut F,
    dims: &[ArrayDimension],
    elems: &mut impl Iterator<Item = T>,
    format_elem: &mut impl FnMut(ListElementWriter<F>, T) -> Result<Nestable, E>,
) -> Result<(), E>
where
    F: FormatBuffer,
{
    if dims.is_empty() {
        buf.write_str("{}");
        return Ok(());
    }

    buf.write_char('{');
    for j in 0..dims[0].length {
        if j > 0 {
            buf.write_char(',');
        }
        if dims.len() == 1 {
            let start = buf.len();
            let elem = elems.next().unwrap();
            if let Nestable::MayNeedEscaping = format_elem(ListElementWriter(buf), elem)? {
                escape_elem::<_, ListElementEscaper>(buf, start);
            }
        } else {
            format_array_inner(buf, &dims[1..], elems, format_elem)?;
        }
    }
    buf.write_char('}');

    Ok(())
}

pub fn format_legacy_vector<F, T, E>(
    buf: &mut F,
    elems: impl IntoIterator<Item = T>,
    format_elem: impl FnMut(ListElementWriter<F>, T) -> Result<Nestable, E>,
) -> Result<Nestable, E>
where
    F: FormatBuffer,
{
    format_elems(buf, elems, format_elem, ' ')?;
    Ok(Nestable::MayNeedEscaping)
}

pub fn format_list<F, T, E>(
    buf: &mut F,
    elems: impl IntoIterator<Item = T>,
    format_elem: impl FnMut(ListElementWriter<F>, T) -> Result<Nestable, E>,
) -> Result<Nestable, E>
where
    F: FormatBuffer,
{
    buf.write_char('{');
    format_elems(buf, elems, format_elem, ',')?;
    buf.write_char('}');
    Ok(Nestable::Yes)
}

/// Writes each `elem` into `buf`, separating the elems with `sep`.
pub fn format_elems<F, T, E>(
    buf: &mut F,
    elems: impl IntoIterator<Item = T>,
    mut format_elem: impl FnMut(ListElementWriter<F>, T) -> Result<Nestable, E>,
    sep: char,
) -> Result<(), E>
where
    F: FormatBuffer,
{
    let mut elems = elems.into_iter().peekable();
    while let Some(elem) = elems.next() {
        let start = buf.len();
        if let Nestable::MayNeedEscaping = format_elem(ListElementWriter(buf), elem)? {
            escape_elem::<_, ListElementEscaper>(buf, start);
        }
        if elems.peek().is_some() {
            buf.write_char(sep)
        }
    }
    Ok(())
}

pub trait ElementEscaper {
    fn needs_escaping(elem: &[u8]) -> bool;
    fn escape_char(c: u8) -> u8;
}

struct ListElementEscaper;

impl ElementEscaper for ListElementEscaper {
    fn needs_escaping(elem: &[u8]) -> bool {
        elem.is_empty()
            || elem == b"NULL"
            || elem
                .iter()
                .any(|c| matches!(c, b'{' | b'}' | b',' | b'"' | b'\\') || c.is_ascii_whitespace())
    }

    fn escape_char(_: u8) -> u8 {
        b'\\'
    }
}

struct MapElementEscaper;

impl ElementEscaper for MapElementEscaper {
    fn needs_escaping(elem: &[u8]) -> bool {
        elem.is_empty()
            || elem == b"NULL"
            || elem.iter().any(|c| {
                matches!(c, b'{' | b'}' | b',' | b'"' | b'=' | b'>' | b'\\')
                    || c.is_ascii_whitespace()
            })
    }

    fn escape_char(_: u8) -> u8 {
        b'\\'
    }
}

struct RecordElementEscaper;

impl ElementEscaper for RecordElementEscaper {
    fn needs_escaping(elem: &[u8]) -> bool {
        elem.is_empty()
            || elem
                .iter()
                .any(|c| matches!(c, b'(' | b')' | b',' | b'"' | b'\\') || c.is_ascii_whitespace())
    }

    fn escape_char(c: u8) -> u8 {
        if c == b'"' {
            b'"'
        } else {
            b'\\'
        }
    }
}

/// Escapes a list, record, or map element in place.
///
/// The element must start at `start` and extend to the end of the buffer. The
/// buffer will be resized if escaping is necessary to account for the
/// additional escape characters.
///
/// The `needs_escaping` function is used to determine whether an element needs
/// to be escaped. It is provided with the bytes of each element and should
/// return whether the element needs to be escaped.
fn escape_elem<F, E>(buf: &mut F, start: usize)
where
    F: FormatBuffer,
    E: ElementEscaper,
{
    let elem = &buf.as_ref()[start..];
    if !E::needs_escaping(elem) {
        return;
    }

    // We'll need two extra bytes for the quotes at the start and end of the
    // element, plus an extra byte for each quote and backslash.
    let extras = 2 + elem.iter().filter(|b| matches!(b, b'"' | b'\\')).count();
    let orig_end = buf.len();
    let new_end = buf.len() + extras;

    // Pad the buffer to the new length. These characters will all be
    // overwritten.
    //
    // NOTE(benesch): we never read these characters, so we could instead use
    // uninitialized memory, but that's a level of unsafety I'm currently
    // uncomfortable with. The performance gain is negligible anyway.
    for _ in 0..extras {
        buf.write_char('\0');
    }

    // SAFETY: inserting ASCII characters before other ASCII characters
    // preserves UTF-8 encoding.
    let elem = unsafe { buf.as_bytes_mut() };

    // Walk the string backwards, writing characters at the new end index while
    // reading from the old end index, adding quotes at the beginning and end,
    // and adding a backslash before every backslash or quote.
    let mut wi = new_end - 1;
    elem[wi] = b'"';
    wi -= 1;
    for ri in (start..orig_end).rev() {
        elem[wi] = elem[ri];
        wi -= 1;
        if let b'\\' | b'"' = elem[ri] {
            elem[wi] = E::escape_char(elem[ri]);
            wi -= 1;
        }
    }
    elem[wi] = b'"';

    assert!(wi == start);
}

/// A helper for `format_list` that formats a single list element.
#[derive(Debug)]
pub struct ListElementWriter<'a, F>(&'a mut F);

impl<'a, F> ListElementWriter<'a, F>
where
    F: FormatBuffer,
{
    /// Marks this list element as null.
    pub fn write_null(self) -> Nestable {
        self.0.write_str("NULL");
        Nestable::Yes
    }

    /// Returns a [`FormatBuffer`] into which a non-null element can be
    /// written.
    pub fn nonnull_buffer(self) -> &'a mut F {
        self.0
    }
}

/// A helper for `format_map` that formats a single map value.
#[derive(Debug)]
pub struct MapValueWriter<'a, F>(&'a mut F);

impl<'a, F> MapValueWriter<'a, F>
where
    F: FormatBuffer,
{
    /// Marks this value element as null.
    pub fn write_null(self) -> Nestable {
        self.0.write_str("NULL");
        Nestable::Yes
    }

    /// Returns a [`FormatBuffer`] into which a non-null element can be
    /// written.
    pub fn nonnull_buffer(self) -> &'a mut F {
        self.0
    }
}

pub fn format_record<F, T, E>(
    buf: &mut F,
    elems: impl IntoIterator<Item = T>,
    mut format_elem: impl FnMut(RecordElementWriter<F>, T) -> Result<Nestable, E>,
) -> Result<Nestable, E>
where
    F: FormatBuffer,
{
    buf.write_char('(');
    let mut elems = elems.into_iter().peekable();
    while let Some(elem) = elems.next() {
        let start = buf.len();
        if let Nestable::MayNeedEscaping = format_elem(RecordElementWriter(buf), elem)? {
            escape_elem::<_, RecordElementEscaper>(buf, start);
        }
        if elems.peek().is_some() {
            buf.write_char(',')
        }
    }
    buf.write_char(')');
    Ok(Nestable::MayNeedEscaping)
}

/// A helper for `format_record` that formats a single record element.
#[derive(Debug)]
pub struct RecordElementWriter<'a, F>(&'a mut F);

impl<'a, F> RecordElementWriter<'a, F>
where
    F: FormatBuffer,
{
    /// Marks this record element as null.
    pub fn write_null(self) -> Nestable {
        Nestable::Yes
    }

    /// Returns a [`FormatBuffer`] into which a non-null element can be
    /// written.
    pub fn nonnull_buffer(self) -> &'a mut F {
        self.0
    }
}

/// An error while parsing an input as a type.
#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct ParseError {
    pub(crate) kind: ParseErrorKind,
    pub(crate) type_name: String,
    pub(crate) input: String,
    pub(crate) details: Option<String>,
}

#[derive(
    Arbitrary,
    Ord,
    PartialOrd,
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect,
)]
pub enum ParseErrorKind {
    OutOfRange,
    InvalidInputSyntax,
}

impl ParseError {
    // To ensure that reversing the parameters causes a compile-time error, we
    // require that `type_name` be a string literal, even though `ParseError`
    // itself stores the type name as a `String`.
    fn new<S>(kind: ParseErrorKind, type_name: &'static str, input: S) -> ParseError
    where
        S: Into<String>,
    {
        ParseError {
            kind,
            type_name: type_name.into(),
            input: input.into(),
            details: None,
        }
    }

    fn out_of_range<S>(type_name: &'static str, input: S) -> ParseError
    where
        S: Into<String>,
    {
        ParseError::new(ParseErrorKind::OutOfRange, type_name, input)
    }

    fn invalid_input_syntax<S>(type_name: &'static str, input: S) -> ParseError
    where
        S: Into<String>,
    {
        ParseError::new(ParseErrorKind::InvalidInputSyntax, type_name, input)
    }

    fn with_details<D>(mut self, details: D) -> ParseError
    where
        D: fmt::Display,
    {
        self.details = Some(details.to_string());
        self
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.kind {
            ParseErrorKind::OutOfRange => {
                write!(
                    f,
                    "{} is out of range for type {}",
                    self.input.quoted(),
                    self.type_name
                )?;
                if let Some(details) = &self.details {
                    write!(f, ": {}", details)?;
                }
                Ok(())
            }
            ParseErrorKind::InvalidInputSyntax => {
                write!(f, "invalid input syntax for type {}: ", self.type_name)?;
                if let Some(details) = &self.details {
                    write!(f, "{}: ", details)?;
                }
                write!(f, "{}", self.input.quoted())
            }
        }
    }
}

impl Error for ParseError {}

impl RustType<ProtoParseError> for ParseError {
    fn into_proto(&self) -> ProtoParseError {
        use proto_parse_error::*;
        use Kind::*;
        let kind = match self.kind {
            ParseErrorKind::OutOfRange => OutOfRange(()),
            ParseErrorKind::InvalidInputSyntax => InvalidInputSyntax(()),
        };
        ProtoParseError {
            kind: Some(kind),
            type_name: self.type_name.clone(),
            input: self.input.clone(),
            details: self.details.clone(),
        }
    }

    fn from_proto(proto: ProtoParseError) -> Result<Self, TryFromProtoError> {
        use proto_parse_error::Kind::*;

        if let Some(kind) = proto.kind {
            Ok(ParseError {
                kind: match kind {
                    OutOfRange(()) => ParseErrorKind::OutOfRange,
                    InvalidInputSyntax(()) => ParseErrorKind::InvalidInputSyntax,
                },
                type_name: proto.type_name,
                input: proto.input,
                details: proto.details,
            })
        } else {
            Err(TryFromProtoError::missing_field("ProtoParseError::kind"))
        }
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub enum ParseHexError {
    InvalidHexDigit(char),
    OddLength,
}

impl fmt::Display for ParseHexError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ParseHexError::InvalidHexDigit(c) => {
                write!(f, "invalid hexadecimal digit: \"{}\"", c.escape_default())
            }
            ParseHexError::OddLength => {
                f.write_str("invalid hexadecimal data: odd number of digits")
            }
        }
    }
}

impl RustType<ProtoParseHexError> for ParseHexError {
    fn into_proto(&self) -> ProtoParseHexError {
        use proto_parse_hex_error::*;
        use Kind::*;
        let kind = match self {
            ParseHexError::InvalidHexDigit(v) => InvalidHexDigit(v.into_proto()),
            ParseHexError::OddLength => OddLength(()),
        };
        ProtoParseHexError { kind: Some(kind) }
    }

    fn from_proto(error: ProtoParseHexError) -> Result<Self, TryFromProtoError> {
        use proto_parse_hex_error::Kind::*;
        match error.kind {
            Some(kind) => match kind {
                InvalidHexDigit(v) => Ok(ParseHexError::InvalidHexDigit(char::from_proto(v)?)),
                OddLength(()) => Ok(ParseHexError::OddLength),
            },
            None => Err(TryFromProtoError::missing_field(
                "`ProtoParseHexError::kind`",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::protobuf_roundtrip;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn parse_error_protobuf_roundtrip(expect in any::<ParseError>()) {
            let actual = protobuf_roundtrip::<_, ProtoParseError>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }

    proptest! {
        #[test]
        fn parse_hex_error_protobuf_roundtrip(expect in any::<ParseHexError>()) {
            let actual = protobuf_roundtrip::<_, ProtoParseHexError>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }

    #[test]
    fn test_format_nanos_to_micros() {
        let cases: Vec<(u32, &str)> = vec![
            (0, ""),
            (1, ""),
            (499, ""),
            (500, ".000001"),
            (500_000, ".0005"),
            (5_000_000, ".005"),
            // Leap second. This is possibly wrong and should maybe be reduced (nanosecond
            // % 1_000_000_000), but we are at least now aware it does this.
            (1_999_999_999, ".2"),
        ];
        for (nanos, expect) in cases {
            let mut buf = String::new();
            format_nanos_to_micros(&mut buf, nanos);
            assert_eq!(&buf, expect);
        }
    }
}
