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

use std::error::Error;
use std::fmt;

use chrono::offset::TimeZone;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use serde::{Deserialize, Serialize};

use ore::fmt::FormatBuffer;

use crate::adt::datetime::{self, DateTimeField, ParsedDateTime};
use crate::adt::decimal::Decimal;
use crate::adt::interval::Interval;
use crate::adt::jsonb::{Jsonb, JsonbRef};

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
fn parse_timestamp_string(s: &str) -> Result<(NaiveDate, NaiveTime, i64), String> {
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
            0,
        ));
    }

    let (ts_string, tz_string) = datetime::split_timestamp_string(s);

    let pdt = ParsedDateTime::build_parsed_datetime_timestamp(&ts_string)?;
    let d: NaiveDate = pdt.compute_date()?;
    let t: NaiveTime = pdt.compute_time()?;

    let offset = if tz_string.is_empty() {
        0
    } else {
        datetime::parse_timezone_offset_second(tz_string)?
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
    // NOTE(benesch): this may be overly conservative. Perhaps dates never
    // have special characters.
    Nestable::MayNeedEscaping
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
    format_nanos(buf, t.nanosecond());
    // NOTE(benesch): this may be overly conservative. Perhaps times never
    // have special characters.
    Nestable::MayNeedEscaping
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
    format_nanos(buf, ts.timestamp_subsec_nanos());
    // NOTE(benesch): this may be overly conservative. Perhaps timestamps never
    // have special characters.
    Nestable::MayNeedEscaping
}

/// Parses a `DateTime<Utc>` from `s`.
pub fn parse_timestamptz(s: &str) -> Result<DateTime<Utc>, ParseError> {
    parse_timestamp_string(s)
        .and_then(|(date, time, offset)| {
            let offset = FixedOffset::east(offset as i32)
                .from_local_datetime(&date.and_time(time))
                .earliest()
                .ok_or_else(|| "invalid timezone conversion".to_owned())?;
            Ok(DateTime::<Utc>::from_utc(offset.naive_utc(), Utc))
        })
        .map_err(|e| ParseError::new("timestamptz", s).with_details(e))
}

/// Writes a [`DateTime<Utc>`] timestamp to `buf`.
pub fn format_timestamptz<F>(buf: &mut F, ts: DateTime<Utc>) -> Nestable
where
    F: FormatBuffer,
{
    write!(buf, "{}", ts.format("%Y-%m-%d %H:%M:%S+00"));
    format_nanos(buf, ts.timestamp_subsec_nanos());
    // NOTE(benesch): this may be overly conservative. Perhaps timestamptzs
    // never have special characters.
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
    if s.starts_with("\\x") {
        hex::decode(&s[2..]).map_err(|e| ParseError::new("bytea", s).with_details(e))
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

pub fn parse_list<T, E>(
    s: &str,
    mut make_null: impl FnMut() -> T,
    mut parse_elem: impl FnMut(&str) -> Result<T, E>,
) -> Result<Vec<T>, ParseError>
where
    E: fmt::Display,
{
    let err = |details| ParseError::new("list", s).with_details(details);

    macro_rules! bail {
        ($($arg:tt)*) => { return Err(err(format!($($arg)*))) };
    }

    let mut elems = vec![];
    let mut chars = s.chars().peekable();
    match chars.next() {
        // start of list
        Some('{') => (),
        Some(other) => bail!("expected '{{', found {}", other),
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
                let elem = parse_elem(&elem_text).map_err(|e| err(e.to_string()))?;
                elems.push(elem);
            }
            // a nested list
            Some('{') => {
                let mut elem_text = String::new();
                loop {
                    match chars.next() {
                        Some(c) => {
                            elem_text.push(c);
                            if c == '}' {
                                break;
                            }
                        }
                        None => bail!("unexpected end of input"),
                    }
                }
                let elem = parse_elem(&elem_text).map_err(|e| err(e.to_string()))?;
                elems.push(elem);
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
                    parse_elem(&elem_text).map_err(|e| err(e.to_string()))?
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

/// Escapes a list or record element in place.
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
