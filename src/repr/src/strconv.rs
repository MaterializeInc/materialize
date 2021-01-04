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

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use chrono::offset::{Offset, TimeZone};
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use ore::lex::LexBuf;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use ore::fmt::FormatBuffer;

use crate::adt::array::ArrayDimension;
use crate::adt::datetime::{self, DateTimeField, ParsedDateTime};
use crate::adt::decimal::Decimal;
use crate::adt::interval::Interval;
use crate::adt::jsonb::{Jsonb, JsonbRef};

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
        _ => Err(ParseError::new("bool", s)),
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

/// Parses an [`i32`] from `s`.
///
/// Valid values are whatever the [`FromStr`] implementation on `i32` accepts,
/// plus leading and trailing whitespace.
pub fn parse_int32(s: &str) -> Result<i32, ParseError> {
    s.trim()
        .parse()
        .map_err(|e| ParseError::new("int4", s).with_details(e))
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
        .map_err(|e| ParseError::new("int8", s).with_details(e))
}

/// Writes an `i64` to `buf`.
pub fn format_int64<F>(buf: &mut F, i: i64) -> Nestable
where
    F: FormatBuffer,
{
    write!(buf, "{}", i);
    Nestable::Yes
}

/// Parses an `f32` from `s`.
pub fn parse_float32(s: &str) -> Result<f32, ParseError> {
    match s.trim().to_lowercase().as_str() {
        "inf" | "infinity" | "+inf" | "+infinity" => Ok(f32::INFINITY),
        "-inf" | "-infinity" => Ok(f32::NEG_INFINITY),
        "nan" => Ok(f32::NAN),
        s => s
            .parse()
            .map_err(|e| ParseError::new("float4", s).with_details(e)),
    }
}

/// Writes an `f32` to `buf`.
pub fn format_float32<F>(buf: &mut F, f: f32) -> Nestable
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
    Nestable::Yes
}

/// Parses an `f64` from `s`.
pub fn parse_float64(s: &str) -> Result<f64, ParseError> {
    match s.trim().to_lowercase().as_str() {
        "inf" | "infinity" | "+inf" | "+infinity" => Ok(f64::INFINITY),
        "-inf" | "-infinity" => Ok(f64::NEG_INFINITY),
        "nan" => Ok(f64::NAN),
        s => s
            .parse()
            .map_err(|e| ParseError::new("float8", s).with_details(e)),
    }
}

/// Writes an `f64` to `buf`.
pub fn format_float64<F>(buf: &mut F, f: f64) -> Nestable
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
    Nestable::Yes
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
        Err(e) => Err(ParseError::new("date", s).with_details(e)),
    }
}

/// Writes a [`NaiveDate`] to `buf`.
pub fn format_date<F>(buf: &mut F, d: NaiveDate) -> Nestable
where
    F: FormatBuffer,
{
    write!(buf, "{}", d);
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
        .map_err(|e| ParseError::new("time", s).with_details(e))
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
        Err(e) => Err(ParseError::new("timestamp", s).with_details(e)),
    }
}

/// Writes a [`NaiveDateTime`] timestamp to `buf`.
pub fn format_timestamp<F>(buf: &mut F, ts: NaiveDateTime) -> Nestable
where
    F: FormatBuffer,
{
    write!(buf, "{}", ts.format("%Y-%m-%d %H:%M:%S"));
    format_nanos_to_micros(buf, ts.timestamp_subsec_nanos());
    // This always needs escaping because of the whitespace
    Nestable::MayNeedEscaping
}

/// Parses a `DateTime<Utc>` from `s`. See [expr::scalar::func::timezone_timestamp] for timezone anomaly considerations.
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
        .map_err(|e| ParseError::new("timestamptz", s).with_details(e))
}

/// Writes a [`DateTime<Utc>`] timestamp to `buf`.
pub fn format_timestamptz<F>(buf: &mut F, ts: DateTime<Utc>) -> Nestable
where
    F: FormatBuffer,
{
    write!(buf, "{}", ts.format("%Y-%m-%d %H:%M:%S"));
    format_nanos_to_micros(buf, ts.timestamp_subsec_nanos());
    write!(buf, "+00");
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
    parse_interval_w_disambiguator(s, DateTimeField::Second)
}

/// Parse an interval string, using a specific sql_parser::ast::DateTimeField
/// to identify ambiguous elements. For more information about this operation,
/// see the doucmentation on ParsedDateTime::build_parsed_datetime_interval.
pub fn parse_interval_w_disambiguator(s: &str, d: DateTimeField) -> Result<Interval, ParseError> {
    ParsedDateTime::build_parsed_datetime_interval(&s, d)
        .and_then(|pdt| pdt.compute_interval())
        .map_err(|e| ParseError::new("interval", s).with_details(e))
}

pub fn format_interval<F>(buf: &mut F, iv: Interval) -> Nestable
where
    F: FormatBuffer,
{
    write!(buf, "{}", iv);
    Nestable::MayNeedEscaping
}

pub fn parse_decimal(s: &str) -> Result<Decimal, ParseError> {
    s.trim()
        .parse()
        .map_err(|e| ParseError::new("decimal", s).with_details(e))
}

pub fn format_decimal<F>(buf: &mut F, d: &Decimal) -> Nestable
where
    F: FormatBuffer,
{
    write!(buf, "{}", d);
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
        hex::decode(remainder).map_err(|e| ParseError::new("bytea", s).with_details(e))
    } else {
        parse_bytes_traditional(s)
    }
}

fn parse_bytes_traditional(s: &str) -> Result<Vec<u8>, ParseError> {
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
                return Err(ParseError::new("bytea", s).with_details("ends with escape character"))
            }
            Some(b'\\') => out.push(b'\\'),
            b => match (b, bytes.next(), bytes.next()) {
                (Some(d2 @ b'0'..=b'3'), Some(d1 @ b'0'..=b'7'), Some(d0 @ b'0'..=b'7')) => {
                    out.push(((d2 - b'0') << 6) + ((d1 - b'0') << 3) + (d0 - b'0'));
                }
                _ => {
                    return Err(ParseError::new("bytea", s).with_details("invalid escape sequence"))
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
        .map_err(|e| ParseError::new("jsonb", s).with_details(e))
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
        .map_err(|e| ParseError::new("uuid", s).with_details(e))
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
    if nanos > 0 {
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
        .map_err(|details| ParseError::new("list", s).with_details(details))
}

// `parse_list_inner`'s separation from `parse_list` simplifies error handling
// by allowing subprocesses to return `String` errors.
pub fn parse_list_inner<'a, T, E>(
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
    let mut gen = |elem| gen_elem(elem).map_err(|e| e.to_string());
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
        .map_err(|details| ParseError::new("map", s).with_details(details))
}

pub fn parse_map_inner<'a, V, E>(
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
    let mut gen_value = |elem| gen_elem(elem).map_err(|e| e.to_string());
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

pub fn format_map<F, T>(
    buf: &mut F,
    elems: impl IntoIterator<Item = (impl AsRef<str>, T)>,
    mut format_elem: impl FnMut(MapValueWriter<F>, T) -> Nestable,
) -> Nestable
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
        if let Nestable::MayNeedEscaping = format_elem(MapValueWriter(buf), value) {
            escape_elem::<_, MapElementEscaper>(buf, value_start);
        }

        if elems.peek().is_some() {
            buf.write_char(',');
        }
    }
    buf.write_char('}');
    Nestable::Yes
}

pub fn format_array<F, T>(
    buf: &mut F,
    dims: &[ArrayDimension],
    elems: impl IntoIterator<Item = T>,
    mut format_elem: impl FnMut(ListElementWriter<F>, T) -> Nestable,
) -> Nestable
where
    F: FormatBuffer,
{
    format_array_inner(buf, dims, &mut elems.into_iter(), &mut format_elem);
    Nestable::Yes
}

pub fn format_array_inner<F, T>(
    buf: &mut F,
    dims: &[ArrayDimension],
    elems: &mut impl Iterator<Item = T>,
    format_elem: &mut impl FnMut(ListElementWriter<F>, T) -> Nestable,
) where
    F: FormatBuffer,
{
    buf.write_char('{');
    for j in 0..dims[0].length {
        if j > 0 {
            buf.write_char(',');
        }
        if dims.len() == 1 {
            let start = buf.len();
            let elem = elems.next().unwrap();
            if let Nestable::MayNeedEscaping = format_elem(ListElementWriter(buf), elem) {
                escape_elem::<_, ListElementEscaper>(buf, start);
            }
        } else {
            format_array_inner(buf, &dims[1..], elems, format_elem);
        }
    }
    buf.write_char('}');
}

pub fn format_list<F, T>(
    buf: &mut F,
    elems: impl IntoIterator<Item = T>,
    mut format_elem: impl FnMut(ListElementWriter<F>, T) -> Nestable,
) -> Nestable
where
    F: FormatBuffer,
{
    buf.write_char('{');
    let mut elems = elems.into_iter().peekable();
    while let Some(elem) = elems.next() {
        let start = buf.len();
        if let Nestable::MayNeedEscaping = format_elem(ListElementWriter(buf), elem) {
            escape_elem::<_, ListElementEscaper>(buf, start);
        }
        if elems.peek().is_some() {
            buf.write_char(',')
        }
    }
    buf.write_char('}');
    Nestable::Yes
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

pub fn format_record<F, T>(
    buf: &mut F,
    elems: impl IntoIterator<Item = T>,
    mut format_elem: impl FnMut(RecordElementWriter<F>, T) -> Nestable,
) -> Nestable
where
    F: FormatBuffer,
{
    buf.write_char('(');
    let mut elems = elems.into_iter().peekable();
    while let Some(elem) = elems.next() {
        let start = buf.len();
        if let Nestable::MayNeedEscaping = format_elem(RecordElementWriter(buf), elem) {
            escape_elem::<_, RecordElementEscaper>(buf, start);
        }
        if elems.peek().is_some() {
            buf.write_char(',')
        }
    }
    buf.write_char(')');
    Nestable::MayNeedEscaping
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

/// An error while parsing input as a type.
#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct ParseError {
    type_name: String,
    input: String,
    details: Option<String>,
}

impl ParseError {
    // To ensure that reversing the parameters causes a compile-time error, we
    // require that `type_name` be a string literal, even though `ParseError`
    // itself stores the type name as a `String`.
    fn new<S>(type_name: &'static str, input: S) -> ParseError
    where
        S: Into<String>,
    {
        ParseError {
            type_name: type_name.into(),
            input: input.into(),
            details: None,
        }
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
        write!(f, "invalid input syntax for {}: ", self.type_name)?;
        if let Some(details) = &self.details {
            write!(f, "{}: ", details)?;
        }
        write!(f, "\"{}\"", self.input)
    }
}

impl Error for ParseError {}
