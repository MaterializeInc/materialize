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
use sql_parser::ast::DateTimeField;

use crate::datetime::ParsedDateTime;
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

#[test]
fn test_parse_date() {
    run_test_parse_date("2001-02-03", NaiveDate::from_ymd(2001, 2, 3));
    run_test_parse_date("2001-02-03 04:05:06.789", NaiveDate::from_ymd(2001, 2, 3));
    fn run_test_parse_date(s: &str, n: NaiveDate) {
        assert_eq!(parse_date(s).unwrap(), n);
    }
}

#[test]
fn test_parse_date_errors() {
    run_test_parse_date_errors(
        "2001-01",
        "Invalid DATE \'2001-01\': YEAR, MONTH, DAY are all required",
    );
    run_test_parse_date_errors(
        "2001",
        "Invalid DATE \'2001\': YEAR, MONTH, DAY are all required",
    );
    run_test_parse_date_errors(
        "2001-13-01",
        "Invalid DATE \'2001-13-01\': MONTH must be (1, 12), got 13",
    );
    run_test_parse_date_errors(
        "2001-12-32",
        "Invalid DATE \'2001-12-32\': DAY must be (1, 31), got 32",
    );
    run_test_parse_date_errors(
        "2001-01-02 04",
        "Invalid DATE '2001-01-02 04': Unknown format",
    );
    fn run_test_parse_date_errors(s: &str, e: &str) {
        assert_eq!(e.to_string(), format!("{}", parse_date(s).unwrap_err()));
    }
}

#[test]
fn test_parse_time() {
    run_test_parse_time(
        "01:02:03.456",
        NaiveTime::from_hms_nano(1, 2, 3, 456_000_000),
    );
    run_test_parse_time("01:02:03", NaiveTime::from_hms(1, 2, 3));
    run_test_parse_time("02:03.456", NaiveTime::from_hms_nano(0, 2, 3, 456_000_000));
    run_test_parse_time("01:02", NaiveTime::from_hms(1, 2, 0));
    fn run_test_parse_time(s: &str, t: NaiveTime) {
        assert_eq!(parse_time(s).unwrap(), t);
    }
}

#[test]
fn test_parse_time_errors() {
    run_test_parse_time_errors(
        "26:01:02.345",
        "Invalid TIME \'26:01:02.345\': HOUR must be (0, 23), got 26",
    );
    run_test_parse_time_errors(
        "01:60:02.345",
        "Invalid TIME \'01:60:02.345\': MINUTE must be (0, 59), got 60",
    );
    run_test_parse_time_errors(
        "01:02:61.345",
        "Invalid TIME \'01:02:61.345\': SECOND must be (0, 60), got 61",
    );
    run_test_parse_time_errors("03.456", "Invalid TIME \'03.456\': Unknown format");

    fn run_test_parse_time_errors(s: &str, e: &str) {
        assert_eq!(e.to_string(), format!("{}", parse_time(s).unwrap_err()));
    }
}

#[test]
fn test_parse_timestamp() {
    run_test_parse_timestamp(
        "2001-02-03 04:05:06.789",
        NaiveDate::from_ymd(2001, 2, 3).and_hms_nano(4, 5, 6, 789_000_000),
    );
    run_test_parse_timestamp(
        "2001-02-03",
        NaiveDate::from_ymd(2001, 2, 3).and_hms(0, 0, 0),
    );
    run_test_parse_timestamp(
        "2001-02-03 01:02:03",
        NaiveDate::from_ymd(2001, 2, 3).and_hms(1, 2, 3),
    );
    run_test_parse_timestamp(
        "2001-02-03 02:03.456",
        NaiveDate::from_ymd(2001, 2, 3).and_hms_nano(0, 2, 3, 456_000_000),
    );
    run_test_parse_timestamp(
        "2001-02-03 01:02",
        NaiveDate::from_ymd(2001, 2, 3).and_hms(1, 2, 0),
    );

    fn run_test_parse_timestamp(s: &str, ts: NaiveDateTime) {
        assert_eq!(parse_timestamp(s).unwrap(), ts);
    }
}

#[test]
fn test_parse_timestamp_errors() {
    run_test_parse_timestamp_errors(
        "2001-01",
        "Invalid TIMESTAMP \'2001-01\': YEAR, MONTH, DAY are all required",
    );
    run_test_parse_timestamp_errors(
        "2001",
        "Invalid TIMESTAMP \'2001\': YEAR, MONTH, DAY are all required",
    );
    run_test_parse_timestamp_errors(
        "2001-13-01",
        "Invalid TIMESTAMP \'2001-13-01\': MONTH must be (1, 12), got 13",
    );
    run_test_parse_timestamp_errors(
        "2001-12-32",
        "Invalid TIMESTAMP \'2001-12-32\': DAY must be (1, 31), got 32",
    );
    run_test_parse_timestamp_errors(
        "2001-01-02 04",
        "Invalid TIMESTAMP \'2001-01-02 04\': Unknown format",
    );

    run_test_parse_timestamp_errors(
        "2001-01-02 26:01:02.345",
        "Invalid TIMESTAMP \'2001-01-02 26:01:02.345\': HOUR must be (0, 23), got 26",
    );
    run_test_parse_timestamp_errors(
        "2001-01-02 01:60:02.345",
        "Invalid TIMESTAMP \'2001-01-02 01:60:02.345\': MINUTE must be (0, 59), got 60",
    );
    run_test_parse_timestamp_errors(
        "2001-01-02 01:02:61.345",
        "Invalid TIMESTAMP \'2001-01-02 01:02:61.345\': SECOND must be (0, 60), got 61",
    );

    fn run_test_parse_timestamp_errors(s: &str, e: &str) {
        assert_eq!(
            e.to_string(),
            format!("{}", parse_timestamp(s).unwrap_err())
        );
    }
}

#[test]
fn test_parse_timestamptz() {
    #[rustfmt::skip]
    let test_cases = [("1999-01-01 01:23:34.555", 1999, 1, 1, 1, 23, 34, 555_000_000, 0),
        ("1999-01-01 01:23:34.555+0:00", 1999, 1, 1, 1, 23, 34, 555_000_000, 0),
        ("1999-01-01 01:23:34.555+0", 1999, 1, 1, 1, 23, 34, 555_000_000, 0),
        ("1999-01-01 01:23:34.555z", 1999, 1, 1, 1, 23, 34, 555_000_000, 0),
        ("1999-01-01 01:23:34.555Z", 1999, 1, 1, 1, 23, 34, 555_000_000, 0),
        ("1999-01-01 01:23:34.555 z", 1999, 1, 1, 1, 23, 34, 555_000_000, 0),
        ("1999-01-01 01:23:34.555 Z", 1999, 1, 1, 1, 23, 34, 555_000_000, 0),
        ("1999-01-01 01:23:34.555+4:00", 1999, 1, 1, 1, 23, 34, 555_000_000, 14400),
        ("1999-01-01 01:23:34.555-4:00", 1999, 1, 1, 1, 23, 34, 555_000_000, -14400),
        ("1999-01-01 01:23:34.555+400", 1999, 1, 1, 1, 23, 34, 555_000_000, 14400),
        ("1999-01-01 01:23:34.555+4", 1999, 1, 1, 1, 23, 34, 555_000_000, 14400),
        ("1999-01-01 01:23:34.555+4:30", 1999, 1, 1, 1, 23, 34, 555_000_000, 16200),
        ("1999-01-01 01:23:34.555+430", 1999, 1, 1, 1, 23, 34, 555_000_000, 16200),
        ("1999-01-01 01:23:34.555+4:45", 1999, 1, 1, 1, 23, 34, 555_000_000, 17100),
        ("1999-01-01 01:23:34.555+445", 1999, 1, 1, 1, 23, 34, 555_000_000, 17100),
        ("1999-01-01 01:23:34.555+14:45", 1999, 1, 1, 1, 23, 34, 555_000_000, 53100),
        ("1999-01-01 01:23:34.555-14:45", 1999, 1, 1, 1, 23, 34, 555_000_000, -53100),
        ("1999-01-01 01:23:34.555+1445", 1999, 1, 1, 1, 23, 34, 555_000_000, 53100),
        ("1999-01-01 01:23:34.555-1445", 1999, 1, 1, 1, 23, 34, 555_000_000, -53100),
        ("1999-01-01 01:23:34.555 +14:45", 1999, 1, 1, 1, 23, 34, 555_000_000, 53100),
        ("1999-01-01 01:23:34.555 -14:45", 1999, 1, 1, 1, 23, 34, 555_000_000, -53100),
        ("1999-01-01 01:23:34.555 +1445", 1999, 1, 1, 1, 23, 34, 555_000_000, 53100),
        ("1999-01-01 01:23:34.555 -1445", 1999, 1, 1, 1, 23, 34, 555_000_000, -53100),
    ];

    for test in test_cases.iter() {
        let actual = parse_timestamptz(test.0).unwrap();

        let expected = NaiveDate::from_ymd(test.1, test.2, test.3)
            .and_hms_nano(test.4, test.5, test.6, test.7);
        let offset = FixedOffset::east(test.8);
        let dt_fixed_offset = offset.from_local_datetime(&expected).earliest().unwrap();
        let expected = DateTime::<Utc>::from_utc(dt_fixed_offset.naive_utc(), Utc);

        assert_eq!(actual, expected);
    }
}

#[test]
fn test_parse_timestamptz_errors() {
    run_test_parse_timestamptz_errors(
        "1999-01-01 01:23:34.555 +25:45",
        "Invalid TIMESTAMPTZ \'1999-01-01 01:23:34.555 +25:45\': Invalid timezone string \
         (+25:45): timezone hour invalid 25",
    );
    run_test_parse_timestamptz_errors(
        "1999-01-01 01:23:34.555 +21:61",
        "Invalid TIMESTAMPTZ \'1999-01-01 01:23:34.555 +21:61\': Invalid timezone string \
         (+21:61): timezone minute invalid 61",
    );
    run_test_parse_timestamptz_errors(
        "1999-01-01 01:23:34.555 4",
        "Invalid TIMESTAMPTZ \'1999-01-01 01:23:34.555 4\': Cannot parse timezone offset 4",
    );

    fn run_test_parse_timestamptz_errors(s: &str, e: &str) {
        assert_eq!(
            e.to_string(),
            format!("{}", parse_timestamptz(s).unwrap_err())
        );
    }
}

#[test]
fn test_parse_interval_monthlike() {
    run_test_parse_interval_monthlike(
        "2 year",
        Interval {
            months: 24,
            ..Default::default()
        },
    );
    run_test_parse_interval_monthlike(
        "3-",
        Interval {
            months: 36,
            ..Default::default()
        },
    );
    run_test_parse_interval_monthlike(
        "2 year 2 months",
        Interval {
            months: 26,
            ..Default::default()
        },
    );
    run_test_parse_interval_monthlike(
        "3-3",
        Interval {
            months: 39,
            ..Default::default()
        },
    );

    fn run_test_parse_interval_monthlike(s: &str, expected: Interval) {
        let actual = parse_interval(s).unwrap();
        assert_eq!(actual, expected);
    }
}

#[test]
fn test_parse_interval_durationlike() {
    use std::time::Duration;
    use DateTimeField::*;

    run_test_parse_interval_durationlike(
        "10",
        Interval {
            duration: Duration::new(10, 0),
            ..Default::default()
        },
    );

    run_test_parse_interval_durationlike_from_sql(
        "10",
        Day,
        Interval {
            duration: Duration::new(10 * 24 * 60 * 60, 0),
            ..Default::default()
        },
    );

    run_test_parse_interval_durationlike_from_sql(
        "10",
        Hour,
        Interval {
            duration: Duration::new(10 * 60 * 60, 0),
            ..Default::default()
        },
    );

    run_test_parse_interval_durationlike_from_sql(
        "10",
        Minute,
        Interval {
            duration: Duration::new(10 * 60, 0),
            ..Default::default()
        },
    );

    run_test_parse_interval_durationlike_from_sql(
        "10",
        Second,
        Interval {
            duration: Duration::new(10, 0),
            ..Default::default()
        },
    );

    run_test_parse_interval_durationlike(
        "0.01",
        Interval {
            duration: Duration::new(0, 10_000_000),
            ..Default::default()
        },
    );

    run_test_parse_interval_durationlike(
        "1 2:3:4.5",
        Interval {
            duration: Duration::new(93_784, 500_000_000),
            ..Default::default()
        },
    );

    run_test_parse_interval_durationlike(
        "-1 2:3:4.5",
        Interval {
            duration: Duration::new(79_015, 500_000_000),
            is_positive_dur: false,
            ..Default::default()
        },
    );

    run_test_parse_interval_durationlike(
        "1 -2:3:4.5",
        Interval {
            duration: Duration::new(79_015, 500_000_000),
            ..Default::default()
        },
    );

    run_test_parse_interval_durationlike(
        "1 2:3",
        Interval {
            duration: Duration::new(93_780, 0),
            ..Default::default()
        },
    );
    fn run_test_parse_interval_durationlike(s: &str, expected: Interval) {
        let actual = parse_interval(s).unwrap();
        assert_eq!(actual, expected);
    }
    fn run_test_parse_interval_durationlike_from_sql(
        s: &str,
        d: DateTimeField,
        expected: Interval,
    ) {
        let actual = parse_interval_w_disambiguator(s, d).unwrap();
        assert_eq!(actual, expected);
    }
}

#[test]
fn test_parse_interval_full() {
    use std::time::Duration;
    use DateTimeField::*;

    run_test_parse_interval_full(
        "6-7 1 2:3:4.5",
        Interval {
            months: 79,
            duration: Duration::new(93_784, 500_000_000),
            is_positive_dur: true,
        },
    );

    run_test_parse_interval_full(
        "-6-7 1 2:3:4.5",
        Interval {
            months: -79,
            duration: Duration::new(93_784, 500_000_000),
            is_positive_dur: true,
        },
    );

    run_test_parse_interval_full(
        "6-7 -1 -2:3:4.5",
        Interval {
            months: 79,
            duration: Duration::new(93_784, 500_000_000),
            is_positive_dur: false,
        },
    );

    run_test_parse_interval_full(
        "-6-7 -1 -2:3:4.5",
        Interval {
            months: -79,
            duration: Duration::new(93_784, 500_000_000),
            is_positive_dur: false,
        },
    );

    run_test_parse_interval_full(
        "-6-7 1 -2:3:4.5",
        Interval {
            months: -79,
            duration: Duration::new(79_015, 500_000_000),
            is_positive_dur: true,
        },
    );

    run_test_parse_interval_full(
        "-6-7 -1 2:3:4.5",
        Interval {
            months: -79,
            duration: Duration::new(79_015, 500_000_000),
            is_positive_dur: false,
        },
    );

    run_test_parse_interval_full_from_sql(
        "-6-7 1",
        Minute,
        Interval {
            months: -79,
            duration: Duration::new(60, 0),
            is_positive_dur: true,
        },
    );

    fn run_test_parse_interval_full(s: &str, expected: Interval) {
        let actual = parse_interval(s).unwrap();
        assert_eq!(actual, expected);
    }
    fn run_test_parse_interval_full_from_sql(s: &str, d: DateTimeField, expected: Interval) {
        let actual = parse_interval_w_disambiguator(s, d).unwrap();
        assert_eq!(actual, expected);
    }
}

#[test]
fn parse_interval_error() {
    fn run_test_parse_interval_errors(s: &str, e: &str) {
        assert_eq!(e.to_string(), format!("{}", parse_interval(s).unwrap_err()));
    }

    run_test_parse_interval_errors(
        "1 1-1",
        "Invalid INTERVAL '1 1-1': Cannot determine format of all parts. Add explicit time \
         components, e.g. INTERVAL '1 day' or INTERVAL '1' DAY",
    );
}
