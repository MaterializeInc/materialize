// Copyright Materialize, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#![deny(missing_docs)]

use crate::ast::{DateTimeFieldValue, ParsedDateTime};
use crate::parser::DateTimeField;
use log::warn;
use std::str::FromStr;

/// Builds a ParsedDateTime from a DATE string (`value`).
///
/// # Arguments
///
/// * `value` is a SQL-formatted DATE string.
pub(crate) fn build_parsed_datetime_date(value: &str) -> Result<ParsedDateTime, String> {
    use TimeStrToken::*;

    let mut pdt = ParsedDateTime::default();

    let date_actual = tokenize_time_str(value)?;

    let mut date_actual = date_actual.iter().peekable();
    // PostgreSQL inexplicably trims all leading colons from all timestamp parts.
    while let Some(Colon) = date_actual.peek() {
        date_actual.next();
    }

    let date_expected = [
        Num(0), // year
        Dash,
        Num(0), // month
        Dash,
        Num(0), // day
    ];
    let mut date_expected = date_expected.iter().peekable();

    if let Err(e) = fill_pdt_from_tokens(
        &mut pdt,
        &mut date_actual,
        &mut date_expected,
        DateTimeField::Year,
        1,
    ) {
        return Err(format!("Invalid DATE/TIME '{}'; {}", value, e));
    }

    Ok(pdt)
}

/// Builds a ParsedDateTime from a TIME string (`value`).
///
/// # Arguments
///
/// * `value` is a SQL-formatted TIME string.
pub(crate) fn build_parsed_datetime_time(value: &str) -> Result<ParsedDateTime, String> {
    let mut pdt = ParsedDateTime::default();

    let time_actual = tokenize_time_str(value)?;

    match determine_format_w_datetimefield(&time_actual.clone(), value)? {
        Some(TimePartFormat::SQLStandard(leading_field)) => {
            fill_pdt_sql_standard(&time_actual, leading_field, value, &mut pdt)?
        }
        _ => return Err(format!("Invalid DATE/TIME '{}'; unknown format", value)),
    }

    Ok(pdt)
}

/// Builds a ParsedDateTime from an interval string (`value`).
///
/// # Arguments
///
/// * `value` is a PostgreSQL-compatible interval string, e.g `INTERVAL 'value'`.
/// * `ambiguous_resolver` identifies the DateTimeField of the final part
///   if it's ambiguous, e.g. in `INTERVAL '1' MONTH` '1' is ambiguous as its
///   DateTimeField, but MONTH resolves the ambiguity.
pub(crate) fn build_parsed_datetime_interval(
    value: &str,
    ambiguous_resolver: DateTimeField,
) -> Result<ParsedDateTime, String> {
    use DateTimeField::*;

    let mut pdt = ParsedDateTime::default();

    let mut value_parts = Vec::new();

    let value_split = value.trim().split_whitespace().collect::<Vec<&str>>();
    for s in value_split {
        value_parts.push(tokenize_time_str(s)?);
    }

    let mut value_parts = value_parts.iter().peekable();

    let mut annotated_parts = Vec::new();

    while let Some(part) = value_parts.next() {
        let mut fmt = determine_format_w_datetimefield(&part, value)?;
        // If you cannot determine the format of this part, try to infer its
        // format.
        if fmt.is_none() {
            fmt = match value_parts.next() {
                Some(next_part) => {
                    match determine_format_w_datetimefield(&next_part, value)? {
                        Some(TimePartFormat::SQLStandard(f)) => {
                            match f {
                                // Do not capture this token because expression
                                // is going to fail.
                                Year | Month | Day => None,
                                // If following part is H:M:S, infer that this
                                // part is Day. Because this part can use a
                                // fraction, it should be parsed as PostgreSQL.
                                _ => {
                                    // We can capture these annotated tokens
                                    // because expressions are commutative.
                                    annotated_parts.push(AnnotatedIntervalPart {
                                        fmt: TimePartFormat::SQLStandard(f),
                                        tokens: next_part.clone(),
                                    });
                                    Some(TimePartFormat::PostgreSQL(Day))
                                }
                            }
                        }
                        // None | Some(TimePartFormat::PostgreSQL(f))
                        // If next_fmt is TimePartFormat::PostgreSQL, that
                        // indicates that the following string was a TimeUnit,
                        // e.g. `day`, and this is where those tokens get
                        // consumed and propagated to their preceding numerical
                        // value.
                        next_fmt => next_fmt,
                    }
                }
                // Allow resolution of final part using ambiguous_resolver.
                None => Some(TimePartFormat::PostgreSQL(ambiguous_resolver)),
            }
        }
        match fmt {
            Some(fmt) => annotated_parts.push(AnnotatedIntervalPart {
                fmt,
                tokens: part.clone(),
            }),
            None => {
                return Err(format!(
                    "Invalid INTERVAL '{}': cannot determine format of all parts. Add \
                     explicit time components, e.g. INTERVAL '1 day' or INTERVAL '1' DAY",
                    value
                ))
            }
        }
    }

    for ap in annotated_parts {
        match ap.fmt {
            TimePartFormat::SQLStandard(f) => {
                fill_pdt_sql_standard(&ap.tokens, f, value, &mut pdt)?
            }
            TimePartFormat::PostgreSQL(f) => fill_pdt_pg(&ap.tokens, f, value, &mut pdt)?,
        }
    }

    Ok(pdt)
}

/// Fills a ParsedDateTime's fields when encountering SQL standard-style interval
/// parts, e.g. `1-2` for Y-M `4:5:6.7` for H:M:S.NS.
///
/// Note that:
/// - SQL-standard style groups ({Y-M}{D}{H:M:S.NS}) require that no fields in
///   the group have been modified, and do not allow any fields to be modified
///   afterward.
/// - Single digits, e.g. `3` in `3 4:5:6.7` could be parsed as SQL standard
///   tokens, but end up being parsed as PostgreSQL-style tokens because of their
///   greater expressivity, in that they allow fractions, and otherwise-equivalence.
fn fill_pdt_sql_standard(
    v: &[TimeStrToken],
    leading_field: DateTimeField,
    value: &str,
    mut pdt: &mut ParsedDateTime,
) -> Result<(), String> {
    use DateTimeField::*;

    // Ensure that no fields have been previously modified.
    match leading_field {
        Year | Month => {
            if pdt.year.is_some() || pdt.month.is_some() {
                return Err(format!(
                    "Invalid INTERVAL '{}': YEAR or MONTH field set twice",
                    value
                ));
            }
        }
        Day => {
            if pdt.day.is_some() {
                return Err(format!("Invalid INTERVAL '{}': DAY field set twice", value));
            }
        }
        Hour | Minute | Second => {
            if pdt.hour.is_some() || pdt.minute.is_some() || pdt.second.is_some() {
                return Err(format!(
                    "Invalid INTERVAL '{}': HOUR, MINUTE, SECOND field set twice",
                    value
                ));
            }
        }
    }

    let mut actual = v.iter().peekable();
    let expected = expected_sql_standard_interval_tokens(leading_field);
    let mut expected = expected.iter().peekable();

    let sign = trim_interval_chars_return_sign(&mut actual);

    if let Err(e) = fill_pdt_from_tokens(&mut pdt, &mut actual, &mut expected, leading_field, sign)
    {
        return Err(format!("Invalid INTERVAL '{}': {}", value, e));
    }

    // Do not allow any fields in the group to be modified afterward, and check
    // that values are valid. SQL standard-style interval parts do not allow
    // non-leading group components to "overflow" into the next-greatest
    // component, e.g. months cannot overflow into years.
    match leading_field {
        Year | Month => {
            if pdt.year.is_none() {
                pdt.year = Some(DateTimeFieldValue::default());
            }
            match pdt.month {
                None => pdt.month = Some(DateTimeFieldValue::default()),
                Some(m) => {
                    if m.unit >= 12 {
                        return Err(format!(
                            "Invalid INTERVAL '{}': MONTH field out range; \
                             must be < 12, have {}",
                            value, m.unit
                        ));
                    }
                }
            }
        }
        Day => {
            if pdt.day.is_none() {
                pdt.day = Some(DateTimeFieldValue::default());
            }
        }
        Hour | Minute | Second => {
            if pdt.hour.is_none() {
                pdt.hour = Some(DateTimeFieldValue::default());
            }

            match pdt.minute {
                None => pdt.minute = Some(DateTimeFieldValue::default()),
                Some(m) => {
                    if m.unit >= 60 {
                        return Err(format!(
                            "Invalid INTERVAL '{}': MINUTE field out range; \
                             must be < 60, have {}",
                            value, m.unit
                        ));
                    }
                }
            }

            match pdt.second {
                None => pdt.second = Some(DateTimeFieldValue::default()),
                Some(s) => {
                    if s.unit >= 60 {
                        return Err(format!(
                            "Invalid INTERVAL '{}': SECOND field out range; \
                             must be < 60, have {}",
                            value, s.unit
                        ));
                    }
                }
            }
        }
    }

    Ok(())
}

/// Fills a ParsedDateTime's fields for a single PostgreSQL-style interval
/// parts, e.g. `1 month`. Invoke this function once for each PostgreSQL-style
/// interval part.
///
/// Note that:
/// - This function only meaningfully parses the numerical component of the
///   string, and relies on determining the DateTimeField component from
///   AnnotatedIntervalPart, passed in as `time_unit`.
/// - Only PostgreSQL-style parts can use fractional components in positions
///   other than seconds, e.g. `1.5 months`.
fn fill_pdt_pg(
    tokens: &[TimeStrToken],
    time_unit: DateTimeField,
    value: &str,
    mut pdt: &mut ParsedDateTime,
) -> Result<(), String> {
    use TimeStrToken::*;

    let mut actual = tokens.iter().peekable();
    // We remove all spaces during tokenization, so TimeUnit only shows up if
    // there is no space between the number and the TimeUnit, e.g. `1y 2d 3h`, which
    // PostgreSQL allows.
    let expected = vec![Num(0), Dot, Nanos(0), TimeUnit(DateTimeField::Year)];
    let mut expected = expected.iter().peekable();

    let sign = trim_interval_chars_return_sign(&mut actual);

    if let Err(e) = fill_pdt_from_tokens(&mut pdt, &mut actual, &mut expected, time_unit, sign) {
        return Err(format!("Invalid INTERVAL '{}': {}", value, e));
    }

    Ok(())
}

/// Fills the fields of `pdt` using the `actual` tokens, starting at `leading_field`
/// and descending to less significant DateTimeFields.
///
/// # Errors
/// - If `actual` doesn't match `expected`, modulo skippable TimeStrTokens.
/// - Setting a field in ParsedDateTime that has already been set.
///
/// # Panics
/// - Trying to advance to the next smallest DateTimeField if you're currently
///     at DateTimeField::Second.
fn fill_pdt_from_tokens(
    pdt: &mut ParsedDateTime,
    actual: &mut std::iter::Peekable<std::slice::Iter<'_, TimeStrToken>>,
    expected: &mut std::iter::Peekable<std::slice::Iter<'_, TimeStrToken>>,
    leading_field: DateTimeField,
    sign: i64,
) -> Result<(), failure::Error> {
    use TimeStrToken::*;
    let mut current_field = leading_field;

    let mut i = 0u8;

    let mut unit_buf: Option<DateTimeFieldValue> = None;

    while let Some(atok) = actual.peek() {
        if let Some(etok) = expected.next() {
            match (atok, etok) {
                // The following forms of puncutation signal the end of a field and can
                // trigger a write.
                (Dash, Dash) | (Colon, Colon) => {
                    pdt.write_field_iff_none(current_field, unit_buf)?;
                    unit_buf = None;
                    current_field = current_field.next_smallest();
                    actual.next();
                }
                (Space, Space) => {
                    pdt.write_field_iff_none(current_field, unit_buf)?;
                    unit_buf = None;
                    current_field = current_field.next_smallest();
                    actual.next();
                    // PostgreSQL inexplicably trims all leading colons from all timestamp parts.
                    while let Some(Colon) = actual.peek() {
                        actual.next();
                    }
                }
                // Dots do not denote terminating a field, so should not trigger a write.
                (Dot, Dot) => {
                    actual.next();
                }
                (Num(val), Num(_)) => {
                    match unit_buf {
                        Some(_) => failure::bail!(
                            "Invalid syntax; parts must be separated by '-', ':', or ' '"
                        ),
                        None => {
                            unit_buf = Some(DateTimeFieldValue {
                                unit: *val * sign,
                                fraction: 0,
                            });
                        }
                    }
                    actual.next();
                }
                (Nanos(val), Nanos(_)) => {
                    match unit_buf {
                        Some(ref mut u) => {
                            u.fraction = *val * sign;
                        }
                        None => {
                            unit_buf = Some(DateTimeFieldValue {
                                unit: 0,
                                fraction: *val * sign,
                            });
                        }
                    }
                    actual.next();
                }
                (Num(n), Nanos(_)) => {
                    // Create disposable copy of n.
                    let mut nc = *n;

                    let mut width = 0;
                    // Destructively count the number of digits in n.
                    while nc != 0 {
                        nc /= 10;
                        width += 1;
                    }

                    let mut n = *n;

                    // Nanoseconds have 9 digits of precision.
                    let precision = 9;

                    if width > precision {
                        // Trim n to its 9 most significant digits.
                        n /= 10_i64.pow(width - precision);
                        warn!(
                            "Trimming {} to 9 digits, the max precision permitted by decimal numerics \
                             in timestamp, timestamptz, date, time, and interval strings.",
                            n
                        );
                    } else {
                        // Right-pad n with 0s.
                        n *= 10_i64.pow(precision - width);
                    }

                    match unit_buf {
                        Some(ref mut u) => {
                            u.fraction = n * sign;
                        }
                        None => {
                            unit_buf = Some(DateTimeFieldValue {
                                unit: 0,
                                fraction: n * sign,
                            });
                        }
                    }
                    actual.next();
                }
                // Prevents PostgreSQL shorthand-style intervals (`'1h'`) from
                // having the 'h' token parse if not preceded by a number or
                // nano position, while still allowing those fields to be
                // skipped over.
                (TimeUnit(f), TimeUnit(_)) if unit_buf.is_none() => {
                    failure::bail!("{:?} must be preceeded by a number, e.g. '1{:?}'", f, f)
                }
                (TimeUnit(f), TimeUnit(_)) => {
                    if *f != current_field {
                        failure::bail!(
                            "Invalid syntax at offset {}: provided {:?} but expected {:?}'",
                            i,
                            current_field,
                            f
                        )
                    }
                    actual.next();
                }
                // Allow skipping expected numbers, dots, and nanoseconds.
                (_, Num(_)) | (_, Dot) | (_, Nanos(_)) => {}
                (provided, expected) => failure::bail!(
                    "Invalid syntax at offset {}: provided {:?} but expected {:?}",
                    i,
                    provided,
                    expected
                ),
            }
        } else {
            // actual has more tokens than expected.
            failure::bail!(
                "Invalid syntax at offset {}: provided {:?} but expected None",
                i,
                atok,
            )
        };

        i += 1;
    }

    pdt.write_field_iff_none(current_field, unit_buf)?;

    Ok(())
}

/// Interval strings can be presented in one of two formats:
/// - SQL Standard, e.g. `1-2 3 4:5:6.7`
/// - PostgreSQL, e.g. `1 year 2 months 3 days`
/// TimePartFormat indicates which type of parsing to use and encodes a
/// DateTimeField, which indicates "where" you should begin parsing the
/// associated tokens w/r/t their respective syntax.
#[derive(Debug, Eq, PartialEq, Clone)]
enum TimePartFormat {
    SQLStandard(DateTimeField),
    PostgreSQL(DateTimeField),
}

/// AnnotatedIntervalPart contains the tokens to be parsed, as well as the format
/// to parse them.
struct AnnotatedIntervalPart {
    pub tokens: std::vec::Vec<TimeStrToken>,
    pub fmt: TimePartFormat,
}

/// Determines the format of the interval part (uses None to identify an
/// indeterminant/ambiguous format). This is necessary because the interval
/// string format is not LL(1); we instead parse as few tokens as possible to
/// generate the string's semantics.
///
/// Note that `toks` should _not_ contain space
fn determine_format_w_datetimefield(
    toks: &[TimeStrToken],
    interval_str: &str,
) -> Result<Option<TimePartFormat>, String> {
    use DateTimeField::*;
    use TimePartFormat::*;
    use TimeStrToken::*;

    let mut toks = toks.iter().peekable();

    trim_interval_chars_return_sign(&mut toks);

    if let Some(Num(_)) = toks.peek() {
        toks.next();
    }

    match toks.next() {
        // Implies {?}{?}{?}, ambiguous case.
        None | Some(Space) => Ok(None),
        Some(Dot) => {
            if let Some(Num(_)) = toks.peek() {
                toks.next();
            }
            match toks.peek() {
                // Implies {Num.NumTimeUnit}
                Some(TimeUnit(f)) => Ok(Some(PostgreSQL(*f))),
                // Implies {?}{?}{?}, ambiguous case.
                _ => Ok(None),
            }
        }
        // Implies {Y-...}{}{}
        Some(Dash) => Ok(Some(SQLStandard(Year))),
        // Implies {}{}{?:...}
        Some(Colon) => {
            if let Some(Num(_)) = toks.peek() {
                toks.next();
            }
            match toks.peek() {
                // Implies {H:M:?...}
                Some(Colon) | Some(Space) | None => Ok(Some(SQLStandard(Hour))),
                // Implies {M:S.NS}
                Some(Dot) => Ok(Some(SQLStandard(Minute))),
                _ => Err(format!(
                    "Invalid date-time type string '{}': cannot determine format",
                    interval_str,
                )),
            }
        }
        // Implies {Num}?{TimeUnit}
        Some(TimeUnit(f)) => Ok(Some(PostgreSQL(*f))),
        _ => Err(format!(
            "Invalid date-time type string '{}': cannot determine format",
            interval_str,
        )),
    }
}

/// Get the expected TimeStrTokens to parse SQL Standard time-like
/// DateTimeFields, i.e. HOUR, MINUTE, SECOND. This is used for INTERVAL,
/// TIMESTAMP, and will eventually be used for TIME.
fn expected_dur_like_tokens(from: DateTimeField) -> Vec<TimeStrToken> {
    use DateTimeField::*;
    use TimeStrToken::*;

    let all_toks = [
        Num(0), // hour
        Colon,
        Num(0), // minute
        Colon,
        Num(0), // second
        Dot,
        Nanos(0), // Nanos
    ];
    let start = match from {
        Hour => 0,
        Minute => 2,
        Second => 4,
        _ => panic!(
            "expected_dur_like_tokens can only be called with HOUR, MINUTE, SECOND; got {}",
            from
        ),
    };
    all_toks[start..all_toks.len()].to_vec()
}

/// Get the expected TimeStrTokens to parse TimePartFormat::SqlStandard parts,
/// starting from some `DateTimeField`. Space tokens are never actually included
/// in the output, but are illustrative of what the expected input of SQL
/// Standard interval values looks like.
fn expected_sql_standard_interval_tokens(from: DateTimeField) -> Vec<TimeStrToken> {
    use DateTimeField::*;
    use TimeStrToken::*;

    let all_toks = [
        Num(0), // year
        Dash,
        Num(0), // month
        Space,
        Num(0), // day
        Space,
    ];
    match from {
        Year => all_toks[0..4].to_vec(),
        Month => all_toks[2..4].to_vec(),
        Day => all_toks[4..6].to_vec(),
        hms => expected_dur_like_tokens(hms),
    }
}

/// Trims tokens equivalent to regex `(:*(+|-)?)` and returns a value reflecting
/// the expressed sign: 1 for positive, -1 for negative.
fn trim_interval_chars_return_sign(
    z: &mut std::iter::Peekable<std::slice::Iter<'_, TimeStrToken>>,
) -> i64 {
    use TimeStrToken::*;

    // PostgreSQL inexplicably trims all leading colons from interval parts.
    while let Some(Colon) = z.peek() {
        z.next();
    }

    match z.peek() {
        Some(Dash) => {
            z.next();
            -1
        }
        Some(Plus) => {
            z.next();
            1
        }
        _ => 1,
    }
}

/// TimeStrToken represents valid tokens in time-like strings,
/// i.e those used in INTERVAL, TIMESTAMP/TZ, DATE, and TIME.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TimeStrToken {
    Dash,
    Space,
    Colon,
    Dot,
    Plus,
    Zulu,
    Num(i64),
    Nanos(i64),
    // String representation of a named timezone e.g. 'EST'
    TzName(String),
    // Tokenized version of a DateTimeField string e.g. 'YEAR'
    TimeUnit(DateTimeField),
}

/// Convert a string from a time-like datatype (INTERVAL, TIMESTAMP/TZ, DATE, and TIME)
/// into Vec<TimeStrToken>.
///
/// # Warning
/// - Any sequence of numeric characters following a decimal that exceeds 9 charactrers
///   gets truncated to 9 characters, e.g. `0.1234567899` is truncated to `0.123456789`.
///
/// # Errors
/// - Any sequence of alphabetic characters cannot be cast into a DateTimeField.
/// - Any sequence of numeric characters cannot be cast into an i64.
/// - Any non-alpha numeric character cannot be cast into a TimeStrToken, e.g. `%`.
pub(crate) fn tokenize_time_str(value: &str) -> Result<Vec<TimeStrToken>, String> {
    let mut toks = vec![];
    let mut num_buf = String::with_capacity(4);
    let mut char_buf = String::with_capacity(7);
    fn parse_num(n: &str, idx: usize) -> Result<TimeStrToken, String> {
        Ok(TimeStrToken::Num(n.parse().map_err(|e| {
            format!("Unable to parse value as a number at index {}: {}", idx, e)
        })?))
    };
    fn maybe_tokenize_num_buf(
        n: &mut String,
        i: usize,
        t: &mut Vec<TimeStrToken>,
    ) -> Result<(), String> {
        if !n.is_empty() {
            t.push(parse_num(&n, i)?);
            n.clear();
        }
        Ok(())
    }
    fn maybe_tokenize_char_buf(c: &mut String, t: &mut Vec<TimeStrToken>) -> Result<(), String> {
        if !c.is_empty() {
            t.push(TimeStrToken::TimeUnit(DateTimeField::from_str(
                &c.to_uppercase(),
            )?));
            c.clear();
        }
        Ok(())
    }
    let mut last_field_is_frac = false;
    for (i, chr) in value.chars().enumerate() {
        if !num_buf.is_empty() && !char_buf.is_empty() {
            return Err(format!(
                "Invalid string in time-like type '{}': could not tokenize",
                value
            ));
        }
        match chr {
            '+' => {
                maybe_tokenize_num_buf(&mut num_buf, i, &mut toks)?;
                maybe_tokenize_char_buf(&mut char_buf, &mut toks)?;
                toks.push(TimeStrToken::Plus);
            }
            '-' => {
                maybe_tokenize_num_buf(&mut num_buf, i, &mut toks)?;
                maybe_tokenize_char_buf(&mut char_buf, &mut toks)?;
                toks.push(TimeStrToken::Dash);
            }
            ' ' => {
                maybe_tokenize_num_buf(&mut num_buf, i, &mut toks)?;
                maybe_tokenize_char_buf(&mut char_buf, &mut toks)?;
                toks.push(TimeStrToken::Space);
            }
            ':' => {
                maybe_tokenize_num_buf(&mut num_buf, i, &mut toks)?;
                maybe_tokenize_char_buf(&mut char_buf, &mut toks)?;
                toks.push(TimeStrToken::Colon);
            }
            '.' => {
                maybe_tokenize_num_buf(&mut num_buf, i, &mut toks)?;
                maybe_tokenize_char_buf(&mut char_buf, &mut toks)?;
                toks.push(TimeStrToken::Dot);
                last_field_is_frac = true;
            }
            chr if chr.is_digit(10) => {
                maybe_tokenize_char_buf(&mut char_buf, &mut toks)?;
                num_buf.push(chr)
            }
            chr if chr.is_ascii_alphabetic() => {
                maybe_tokenize_num_buf(&mut num_buf, i, &mut toks)?;
                char_buf.push(chr)
            }
            chr => {
                return Err(format!(
                    "Invalid character at offset {} in {}: {:?}",
                    i, value, chr
                ))
            }
        }
    }
    if !num_buf.is_empty() {
        if !last_field_is_frac {
            toks.push(parse_num(&num_buf, 0)?);
        } else {
            // this is guaranteed to be ascii, so len is fine
            let mut chars = num_buf.len();
            // Fractions only support 9 places of precision.
            let default_precision = 9;
            if chars > default_precision {
                warn!(
                    "Trimming {} to 9 digits, the max precision permitted by decimal numerics \
                     in timestamp, timestamptz, date, time, and interval strings.",
                    num_buf
                );
                num_buf = num_buf[..default_precision].to_string();
                chars = default_precision;
            }
            let raw: i64 = num_buf
                .parse()
                .map_err(|e| format!("couldn't parse fraction {}: {}", num_buf, e))?;
            let multiplicand = 1_000_000_000 / 10_i64.pow(chars as u32);

            toks.push(TimeStrToken::Nanos(raw * multiplicand));
        }
    } else {
        maybe_tokenize_char_buf(&mut char_buf, &mut toks)?
    }
    Ok(toks)
}

fn tokenize_timezone(value: &str) -> Result<Vec<TimeStrToken>, String> {
    let mut toks: Vec<TimeStrToken> = vec![];
    let mut num_buf = String::with_capacity(4);
    // If the timezone string has a colon, we need to parse all numbers naively.
    // Otherwise we need to parse long sequences of digits as [..hhhhmm]
    let split_nums: bool = !value.contains(':');

    // Takes a string and tries to parse it as a number token and insert it into
    // the token list
    fn parse_num(
        toks: &mut Vec<TimeStrToken>,
        n: &str,
        split_nums: bool,
        idx: usize,
    ) -> Result<(), String> {
        if n.is_empty() {
            return Ok(());
        }

        let (first, second) = if n.len() > 2 && split_nums {
            let (first, second) = n.split_at(n.len() - 2);
            (first, Some(second))
        } else {
            (n, None)
        };

        toks.push(TimeStrToken::Num(first.parse().map_err(|e| {
                format!(
                    "Error tokenizing timezone string: unable to parse value {} as a number at index {}: {}",
                    first, idx, e
                )
            })?));

        if let Some(second) = second {
            toks.push(TimeStrToken::Num(second.parse().map_err(|e| {
                format!(
                    "Error tokenizing timezone string: unable to parse value {} as a number at index {}: {}",
                    second, idx, e
                )
            })?));
        }

        Ok(())
    };
    for (i, chr) in value.chars().enumerate() {
        match chr {
            '-' => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(TimeStrToken::Dash);
            }
            ' ' => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(TimeStrToken::Space);
            }
            ':' => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(TimeStrToken::Colon);
            }
            '+' => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(TimeStrToken::Plus);
            }
            chr if (chr == 'z' || chr == 'Z') && (i == value.len() - 1) => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(TimeStrToken::Zulu);
            }
            chr if chr.is_digit(10) => num_buf.push(chr),
            chr if chr.is_ascii_alphabetic() => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                let substring = &value[i..];
                toks.push(TimeStrToken::TzName(substring.to_string()));
                return Ok(toks);
            }
            chr => {
                return Err(format!(
                    "Error tokenizing timezone string ({}): invalid character {:?} at offset {}",
                    value, chr, i
                ))
            }
        }
    }
    parse_num(&mut toks, &num_buf, split_nums, 0)?;
    Ok(toks)
}

fn build_timezone_offset_second(tokens: &[TimeStrToken], value: &str) -> Result<i64, String> {
    use TimeStrToken::*;
    let all_formats = [
        vec![Plus, Num(0), Colon, Num(0)],
        vec![Dash, Num(0), Colon, Num(0)],
        vec![Plus, Num(0), Num(0)],
        vec![Dash, Num(0), Num(0)],
        vec![Plus, Num(0)],
        vec![Dash, Num(0)],
        vec![TzName("".to_string())],
        vec![Zulu],
    ];

    let mut is_positive = true;
    let mut hour_offset: Option<i64> = None;
    let mut minute_offset: Option<i64> = None;

    for format in all_formats.iter() {
        let actual = tokens.iter();

        if actual.len() != format.len() {
            continue;
        }

        for (i, (atok, etok)) in actual.zip(format).enumerate() {
            match (atok, etok) {
                (Colon, Colon) | (Plus, Plus) => { /* Matching punctuation */ }
                (Dash, Dash) => {
                    is_positive = false;
                }
                (Num(val), Num(_)) => {
                    let val = *val;
                    match (hour_offset, minute_offset) {
                        (None, None) => if val <= 24 {
                            hour_offset = Some(val as i64);
                        } else {
                            // We can return an error here because in all the
                            // formats with numbers we require the first number
                            // to be an hour and we require it to be <= 24
                            return Err(format!(
                                "Error parsing timezone string ({}): timezone hour invalid {}",
                                value, val
                            ));
                        }
                        (Some(_), None) => if val <= 60 {
                            minute_offset = Some(val as i64);
                        } else {
                            return Err(format!(
                                "Error parsing timezone string ({}): timezone minute invalid {}",
                                value, val
                            ));
                        },
                        // We've already seen an hour and a minute so we should
                        // never see another number
                        (Some(_), Some(_)) => return Err(format!(
                            "Error parsing timezone string ({}): invalid value {} at token index {}", value,
                            val, i
                        )),
                        (None, Some(_)) => unreachable!("parsed a minute before an hour!"),
                    }
                }
                (Zulu, Zulu) => return Ok(0 as i64),
                (TzName(val), TzName(_)) => {
                    // For now, we don't support named timezones
                    return Err(format!(
                        "Error parsing timezone string ({}): named timezones are not supported. \
                         Failed to parse {} at token index {}",
                        value, val, i
                    ));
                }
                (_, _) => {
                    // Theres a mismatch between this format and the actual
                    // token stream Stop trying to parse in this format and go
                    // to the next one
                    is_positive = true;
                    hour_offset = None;
                    minute_offset = None;
                    break;
                }
            }
        }

        // Return the first valid parsed result
        if let Some(hour_offset) = hour_offset {
            let mut tz_offset_second: i64 = hour_offset * 60 * 60;

            if let Some(minute_offset) = minute_offset {
                tz_offset_second += minute_offset * 60;
            }

            if !is_positive {
                tz_offset_second *= -1
            }
            return Ok(tz_offset_second);
        }
    }

    Err("It didnt work".into())
}

/// Takes a 'date timezone' 'date time timezone' string and splits it into 'date
/// {time}' and 'timezone' components
pub(crate) fn split_timestamp_string(value: &str) -> (&str, &str) {
    // First we need to see if the string contains " +" or " -" because
    // timestamps can come in a format YYYY-MM-DD {+|-}<tz> (where the timezone
    // string can have colons)
    let cut = value.find(" +").or_else(|| value.find(" -"));

    if let Some(cut) = cut {
        let (first, second) = value.split_at(cut);
        return (first.trim(), second.trim());
    }

    // If we have a hh:mm:dd component, we need to go past that to see if we can
    // find a tz
    let colon = value.find(':');

    if let Some(colon) = colon {
        let substring = value.get(colon..);
        if let Some(substring) = substring {
            let tz = substring
                .find(|c: char| (c == '-') || (c == '+') || (c == ' ') || c.is_ascii_alphabetic());

            if let Some(tz) = tz {
                let (first, second) = value.split_at(colon + tz);
                return (first.trim(), second.trim());
            }
        }

        (value.trim(), "")
    } else {
        // We don't have a time, so the only formats available are YYY-mm-dd<tz>
        // or YYYY-MM-dd <tz> Numeric offset timezones need to be separated from
        // the ymd by a space
        let cut = value.find(|c: char| (c == ' ') || c.is_ascii_alphabetic());

        if let Some(cut) = cut {
            let (first, second) = value.split_at(cut);
            return (first.trim(), second.trim());
        }

        (value.trim(), "")
    }
}

pub(crate) fn parse_timezone_offset_second(value: &str) -> Result<i64, String> {
    let toks = tokenize_timezone(value)?;
    Ok(build_timezone_offset_second(&toks, value)?)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::parser::*;

    #[test]
    fn test_expected_dur_like_tokens() {
        use DateTimeField::*;
        use TimeStrToken::*;
        assert_eq!(
            expected_sql_standard_interval_tokens(Hour),
            vec![Num(0), Colon, Num(0), Colon, Num(0), Dot, Nanos(0)]
        );
        assert_eq!(
            expected_sql_standard_interval_tokens(Minute),
            vec![Num(0), Colon, Num(0), Dot, Nanos(0)]
        );
        assert_eq!(
            expected_sql_standard_interval_tokens(Second),
            vec![Num(0), Dot, Nanos(0)]
        );
    }

    #[test]
    fn test_expected_sql_standard_interval_tokens() {
        use DateTimeField::*;
        use TimeStrToken::*;
        assert_eq!(
            expected_sql_standard_interval_tokens(Year),
            vec![Num(0), Dash, Num(0), Space]
        );

        assert_eq!(
            expected_sql_standard_interval_tokens(Day),
            vec![Num(0), Space,]
        );
        assert_eq!(
            expected_sql_standard_interval_tokens(Hour),
            vec![Num(0), Colon, Num(0), Colon, Num(0), Dot, Nanos(0)]
        );
    }
    #[test]
    fn test_trim_interval_chars_return_sign() {
        let test_cases = [
            ("::::-2", -1, "2"),
            ("-3", -1, "3"),
            ("::::+4", 1, "4"),
            ("+5", 1, "5"),
            ("-::::", -1, ":"),
            ("-YEAR", -1, "YEAR"),
            ("YEAR", 1, "YEAR"),
        ];

        for test in test_cases.iter() {
            let s = tokenize_time_str(test.0).unwrap();
            let mut s = s.iter().peekable();

            assert_eq!(trim_interval_chars_return_sign(&mut s), test.1);
            assert_eq!(**s.peek().unwrap(), tokenize_time_str(test.2).unwrap()[0]);
        }
    }
    #[test]
    fn test_determine_format_w_datetimefield() {
        use DateTimeField::*;
        use TimePartFormat::*;

        let test_cases = [
            ("1-2 3", Some(SQLStandard(Year))),
            ("4:5", Some(SQLStandard(Hour))),
            ("4:5.6", Some(SQLStandard(Minute))),
            ("-4:5.6", Some(SQLStandard(Minute))),
            ("+4:5.6", Some(SQLStandard(Minute))),
            ("year", Some(PostgreSQL(Year))),
            ("4year", Some(PostgreSQL(Year))),
            ("-4year", Some(PostgreSQL(Year))),
            ("5", None),
            ("5.6", None),
            ("3 4:5:6.7", None),
        ];

        for test in test_cases.iter() {
            let s = tokenize_time_str(test.0).unwrap();

            match (
                determine_format_w_datetimefield(&s, test.0).unwrap(),
                test.1.as_ref(),
            ) {
                (Some(a), Some(b)) => {
                    if a != *b {
                        panic!(
                            "determine_format_w_datetimefield returned {:?}, expected {:?}",
                            a, b,
                        )
                    }
                }
                (None, None) => {}
                (x, y) => panic!(
                    "determine_format_w_datetimefield returned {:?}, expected {:?}",
                    x, y,
                ),
            }
        }
    }
    #[test]
    fn test_determine_format_w_datetimefield_error() {
        let test_cases = [
            (
                "1+2",
                "Invalid date-time type string '1+2': cannot determine format",
            ),
            (
                "1:2+3",
                "Invalid date-time type string '1:2+3': cannot determine format",
            ),
            (
                "1:1YEAR2",
                "Invalid date-time type string '1:1YEAR2': cannot determine format",
            ),
        ];

        for test in test_cases.iter() {
            let s = tokenize_time_str(test.0).unwrap();
            match determine_format_w_datetimefield(&s, test.0) {
                Err(e) => assert_eq!(e.to_string(), test.1),
                Ok(f) => panic!(
                    "Test passed when expected to fail: {}, generated {:?}",
                    test.0, f
                ),
            };
        }
    }

    #[test]
    fn test_fill_pdt_from_tokens() {
        use DateTimeField::*;
        let test_cases = [
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(6, 0)),
                    ..Default::default()
                },
                "1 2 3 4 5 6",
                "0 0 0 0 0 0",
                Year,
                1,
            ),
            (
                ParsedDateTime {
                    day: Some(DateTimeFieldValue::new(4, 0)),
                    hour: Some(DateTimeFieldValue::new(5, 0)),
                    minute: Some(DateTimeFieldValue::new(6, 0)),
                    ..Default::default()
                },
                "4 5 6",
                "0 0 0",
                Day,
                1,
            ),
            (
                ParsedDateTime {
                    day: Some(DateTimeFieldValue::new(-4, 0)),
                    hour: Some(DateTimeFieldValue::new(-5, 0)),
                    minute: Some(DateTimeFieldValue::new(-6, 0)),
                    ..Default::default()
                },
                "4 5 6",
                "0 0 0",
                Day,
                -1,
            ),
            // Mixed delimeter parsing
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(6, 0)),
                    ..Default::default()
                },
                "1-2:3-4 5 6",
                "0-0:0-0 0 0",
                Year,
                1,
            ),
            // Skip an element at the end
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(5, 0)),
                    minute: Some(DateTimeFieldValue::new(6, 0)),
                    ..Default::default()
                },
                "1 2 3 5 6",
                "0 0 0 0 0 0",
                Year,
                1,
            ),
            // Skip an element w/ non-space parsing
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(6, 0)),
                    ..Default::default()
                },
                "1-2:3- 5 6",
                "0-0:0-0 0 0",
                Year,
                1,
            ),
            // Get Nanos from tokenizer
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(6, 700_000_000)),
                    ..Default::default()
                },
                "1-2:3-4 5 6.7",
                "0-0:0-0 0 0.0",
                Year,
                1,
            ),
            // Proper fraction/nano conversion anywhere
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 200_000_000)),
                    ..Default::default()
                },
                "1.2",
                "0.0",
                Year,
                1,
            ),
            (
                ParsedDateTime {
                    minute: Some(DateTimeFieldValue::new(1, 200_000_000)),
                    ..Default::default()
                },
                "1.2",
                "0.0",
                Minute,
                1,
            ),
            // Parse TimeUnit
            (
                ParsedDateTime {
                    month: Some(DateTimeFieldValue::new(3, 0)),
                    ..Default::default()
                },
                "3MONTHS",
                "0YEAR",
                Month,
                1,
            ),
            (
                ParsedDateTime {
                    month: Some(DateTimeFieldValue::new(1, 0)),
                    day: Some(DateTimeFieldValue::new(2, 0)),
                    hour: Some(DateTimeFieldValue::new(3, 0)),
                    ..Default::default()
                },
                "1MONTHS 2DAYS 3HOURS",
                "0YEAR 0YEAR 0YEAR",
                Month,
                1,
            ),
            (
                ParsedDateTime {
                    month: Some(DateTimeFieldValue::new(1, 0)),
                    day: Some(DateTimeFieldValue::new(2, 0)),
                    ..Default::default()
                },
                "1MONTHS-2",
                "0YEAR-0",
                Month,
                1,
            ),
        ];
        for test in test_cases.iter() {
            let mut pdt = ParsedDateTime::default();
            let actual = tokenize_time_str(test.1).unwrap();
            let mut actual = actual.iter().peekable();
            let expected = tokenize_time_str(test.2).unwrap();
            let mut expected = expected.iter().peekable();

            fill_pdt_from_tokens(&mut pdt, &mut actual, &mut expected, test.3, test.4).unwrap();

            assert_eq!(pdt, test.0);
        }
    }

    #[test]
    fn test_fill_pdt_from_tokens_errors() {
        use DateTimeField::*;
        let test_cases = [
            // Mismatched syntax
            (
                "1 2 3",
                "0-0 0",
                Year,
                1,
                "Invalid syntax at offset 1: provided Space but expected Dash",
            ),
            // If you have Nanos, you cannot convert to Num.
            (
                "1 2.3",
                "0 0.0YEAR",
                Year,
                1,
                "Invalid syntax at offset 5: provided Nanos(300000000) but expected TimeUnit(Year)",
            ),
        ];
        for test in test_cases.iter() {
            let mut pdt = ParsedDateTime::default();
            let actual = tokenize_time_str(test.0).unwrap();
            let mut actual = actual.iter().peekable();
            let expected = tokenize_time_str(test.1).unwrap();
            let mut expected = expected.iter().peekable();

            match fill_pdt_from_tokens(&mut pdt, &mut actual, &mut expected, test.2, test.3) {
                Err(e) => assert_eq!(e.to_string(), test.4),
                Ok(_) => panic!(
                    "Test passed when expected to fail, generated ParsedDateTime {:?}",
                    pdt
                ),
            };
        }
    }
    #[test]
    #[should_panic(expected = "Cannot get smaller DateTimeField than SECOND")]
    fn test_fill_pdt_from_tokens_panic() {
        use DateTimeField::*;
        let test_cases = [
            // Mismatched syntax
            ("1 2", "0 0", Second, 1),
        ];
        for test in test_cases.iter() {
            let mut pdt = ParsedDateTime::default();
            let actual = tokenize_time_str(test.0).unwrap();
            let mut actual = actual.iter().peekable();
            let expected = tokenize_time_str(test.1).unwrap();
            let mut expected = expected.iter().peekable();

            if fill_pdt_from_tokens(&mut pdt, &mut actual, &mut expected, test.2, test.3).is_ok() {
                panic!(
                    "test_fill_pdt_from_tokens_panic should have paniced. input {}\nformat {}\
                     \nDateTimeField {}\nGenerated ParsedDateTime {:?}",
                    test.0, test.1, test.2, pdt
                );
            };
        }
    }

    #[test]
    fn test_fill_pdt_pg() {
        use DateTimeField::*;
        let test_cases = [
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(2, 0)),
                    ..Default::default()
                },
                "2",
                Year,
            ),
            (
                ParsedDateTime {
                    month: Some(DateTimeFieldValue::new(2, 300_000_000)),
                    ..Default::default()
                },
                "2.3",
                Month,
            ),
            (
                ParsedDateTime {
                    day: Some(DateTimeFieldValue::new(-2, -300_000_000)),
                    ..Default::default()
                },
                "-2.3",
                Day,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(2, 0)),
                    ..Default::default()
                },
                "2",
                Hour,
            ),
            (
                ParsedDateTime {
                    minute: Some(DateTimeFieldValue::new(2, 300_000_000)),
                    ..Default::default()
                },
                "2.3",
                Minute,
            ),
            (
                ParsedDateTime {
                    second: Some(DateTimeFieldValue::new(-2, -300_000_000)),
                    ..Default::default()
                },
                "-2.3",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(2, 0)),
                    ..Default::default()
                },
                "2year",
                Year,
            ),
            (
                ParsedDateTime {
                    month: Some(DateTimeFieldValue::new(2, 300_000_000)),
                    ..Default::default()
                },
                "2.3month",
                Month,
            ),
            (
                ParsedDateTime {
                    day: Some(DateTimeFieldValue::new(-2, -300_000_000)),
                    ..Default::default()
                },
                "-2.3day",
                Day,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(2, 0)),
                    ..Default::default()
                },
                "2hour",
                Hour,
            ),
            (
                ParsedDateTime {
                    minute: Some(DateTimeFieldValue::new(2, 300_000_000)),
                    ..Default::default()
                },
                "2.3minute",
                Minute,
            ),
            (
                ParsedDateTime {
                    second: Some(DateTimeFieldValue::new(-2, -300_000_000)),
                    ..Default::default()
                },
                "-2.3second",
                Second,
            ),
            (
                ParsedDateTime {
                    second: Some(DateTimeFieldValue::new(-2, -300_000_000)),
                    ..Default::default()
                },
                ":::::::::-2.3second",
                Second,
            ),
            (
                ParsedDateTime {
                    second: Some(DateTimeFieldValue::new(2, 300_000_000)),
                    ..Default::default()
                },
                ":::::::::+2.3second",
                Second,
            ),
        ];
        for test in test_cases.iter() {
            let mut pdt = ParsedDateTime::default();
            let actual = tokenize_time_str(test.1).unwrap();
            fill_pdt_pg(&actual, test.2, test.1, &mut pdt).unwrap();

            assert_eq!(pdt, test.0);
        }
    }

    #[test]
    fn fill_pdt_pg_errors() {
        use DateTimeField::*;
        let test_cases = [
            // Invalid syntax
            (
                "1.2.",
                Month,
                "Invalid INTERVAL \'1.2.\': Invalid syntax at offset 3: \
                 provided Dot but expected TimeUnit(Year)",
            ),
            (
                "YEAR",
                Year,
                "Invalid INTERVAL \'YEAR\': Year must be preceeded \
                 by a number, e.g. \'1Year\'",
            ),
            // Running into this error means that determine_format_w_datetimefield
            // failed.
            (
                "1YEAR",
                Month,
                "Invalid INTERVAL \'1YEAR\': Invalid syntax at offset \
                 3: provided Month but expected Year\'",
            ),
        ];
        for test in test_cases.iter() {
            let mut pdt = ParsedDateTime::default();
            let actual = tokenize_time_str(test.0).unwrap();
            match fill_pdt_pg(&actual, test.1, test.0, &mut pdt) {
                Err(e) => assert_eq!(e.to_string(), test.2),
                Ok(_) => panic!(
                    "Test passed when expected to fail, generated ParsedDateTime {:?}",
                    pdt
                ),
            };
        }
    }

    #[test]
    fn test_fill_pdt_sql_standard() {
        use DateTimeField::*;
        let test_cases = [
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    ..Default::default()
                },
                "1-2",
                Year,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(1, 0)),
                    minute: Some(DateTimeFieldValue::new(2, 0)),
                    second: Some(DateTimeFieldValue::new(3, 400_000_000)),
                    ..Default::default()
                },
                "1:2:3.4",
                Hour,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(1, 0)),
                    minute: Some(DateTimeFieldValue::new(0, 0)),
                    second: Some(DateTimeFieldValue::new(3, 400_000_000)),
                    ..Default::default()
                },
                "1::3.4",
                Hour,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(1, 0)),
                    minute: Some(DateTimeFieldValue::new(0, 0)),
                    second: Some(DateTimeFieldValue::new(0, 400_000_000)),
                    ..Default::default()
                },
                "1::.4",
                Hour,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(0, 0)),
                    minute: Some(DateTimeFieldValue::new(0, 0)),
                    second: Some(DateTimeFieldValue::new(0, 400_000_000)),
                    ..Default::default()
                },
                ".4",
                Second,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(0, 0)),
                    minute: Some(DateTimeFieldValue::new(1, 0)),
                    second: Some(DateTimeFieldValue::new(2, 300_000_000)),
                    ..Default::default()
                },
                "1:2.3",
                Minute,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(-1, 0)),
                    month: Some(DateTimeFieldValue::new(-2, 0)),
                    ..Default::default()
                },
                "-1-2",
                Year,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(-1, 0)),
                    minute: Some(DateTimeFieldValue::new(-2, 0)),
                    second: Some(DateTimeFieldValue::new(-3, -400_000_000)),
                    ..Default::default()
                },
                "-1:2:3.4",
                Hour,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    ..Default::default()
                },
                "+1-2",
                Year,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(1, 0)),
                    minute: Some(DateTimeFieldValue::new(2, 0)),
                    second: Some(DateTimeFieldValue::new(3, 400_000_000)),
                    ..Default::default()
                },
                "+1:2:3.4",
                Hour,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(-1, 0)),
                    month: Some(DateTimeFieldValue::new(-2, 0)),
                    ..Default::default()
                },
                "::::::-1-2",
                Year,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(-1, 0)),
                    minute: Some(DateTimeFieldValue::new(-2, 0)),
                    second: Some(DateTimeFieldValue::new(-3, -400_000_000)),
                    ..Default::default()
                },
                ":::::::-1:2:3.4",
                Hour,
            ),
        ];
        for test in test_cases.iter() {
            let mut pdt = ParsedDateTime::default();
            let actual = tokenize_time_str(test.1).unwrap();
            fill_pdt_sql_standard(&actual, test.2, test.1, &mut pdt).unwrap();

            assert_eq!(pdt, test.0);
        }
    }

    #[test]
    fn test_fill_pdt_sql_standard_errors() {
        use DateTimeField::*;
        let test_cases = [
            // Invalid syntax
            (
                "1.2",
                Year,
                "Invalid INTERVAL \'1.2\': Invalid syntax at \
                 offset 1: provided Dot but expected Dash",
            ),
            (
                "1-2:3.4",
                Minute,
                "Invalid INTERVAL \'1-2:3.4\': Invalid syntax at \
                 offset 1: provided Dash but expected Colon",
            ),
            (
                "1:2.2.",
                Minute,
                "Invalid INTERVAL \'1:2.2.\': Invalid syntax at \
                 offset 5: provided Dot but expected None",
            ),
            (
                "1YEAR",
                Year,
                "Invalid INTERVAL \'1YEAR\': Invalid syntax at \
                 offset 1: provided TimeUnit(Year) but expected Dash",
            ),
        ];
        for test in test_cases.iter() {
            let mut pdt = ParsedDateTime::default();
            let actual = tokenize_time_str(test.0).unwrap();
            match fill_pdt_sql_standard(&actual, test.1, test.0, &mut pdt) {
                Err(e) => assert_eq!(e.to_string(), test.2),
                Ok(_) => panic!(
                    "Test passed when expected to fail, generated ParsedDateTime {:?}",
                    pdt
                ),
            };
        }
    }

    #[test]
    fn test_build_parsed_datetime_date() {
        run_test_build_parsed_datetime_date(
            "2000-01-02",
            ParsedDateTime {
                year: Some(DateTimeFieldValue::new(2000, 0)),
                month: Some(DateTimeFieldValue::new(1, 0)),
                day: Some(DateTimeFieldValue::new(2, 0)),
                ..Default::default()
            },
        );
        run_test_build_parsed_datetime_date(
            "2000",
            ParsedDateTime {
                year: Some(DateTimeFieldValue::new(2000, 0)),
                ..Default::default()
            },
        );
        run_test_build_parsed_datetime_date(
            "2000-1-",
            ParsedDateTime {
                year: Some(DateTimeFieldValue::new(2000, 0)),
                month: Some(DateTimeFieldValue::new(1, 0)),
                ..Default::default()
            },
        );

        fn run_test_build_parsed_datetime_date(test: &str, res: ParsedDateTime) {
            assert_eq!(build_parsed_datetime_date(test), Ok(res));
        }
    }

    #[test]
    fn test_build_parsed_datetime_time() {
        run_test_build_parsed_datetime_time(
            "3:4:5.6",
            ParsedDateTime {
                hour: Some(DateTimeFieldValue::new(3, 0)),
                minute: Some(DateTimeFieldValue::new(4, 0)),
                second: Some(DateTimeFieldValue::new(5, 600_000_000)),
                ..Default::default()
            },
        );
        run_test_build_parsed_datetime_time(
            "3:4",
            ParsedDateTime {
                hour: Some(DateTimeFieldValue::new(3, 0)),
                minute: Some(DateTimeFieldValue::new(4, 0)),
                second: Some(DateTimeFieldValue::new(0, 0)),
                ..Default::default()
            },
        );
        run_test_build_parsed_datetime_time(
            "3:4.5",
            ParsedDateTime {
                hour: Some(DateTimeFieldValue::new(0, 0)),
                minute: Some(DateTimeFieldValue::new(3, 0)),
                second: Some(DateTimeFieldValue::new(4, 500_000_000)),
                ..Default::default()
            },
        );
        run_test_build_parsed_datetime_time(
            "0::4.5",
            ParsedDateTime {
                hour: Some(DateTimeFieldValue::new(0, 0)),
                minute: Some(DateTimeFieldValue::new(0, 0)),
                second: Some(DateTimeFieldValue::new(4, 500_000_000)),
                ..Default::default()
            },
        );
        run_test_build_parsed_datetime_time(
            "0::.5",
            ParsedDateTime {
                hour: Some(DateTimeFieldValue::new(0, 0)),
                minute: Some(DateTimeFieldValue::new(0, 0)),
                second: Some(DateTimeFieldValue::new(0, 500_000_000)),
                ..Default::default()
            },
        );

        fn run_test_build_parsed_datetime_time(test: &str, res: ParsedDateTime) {
            assert_eq!(build_parsed_datetime_time(test), Ok(res));
        }
    }

    #[test]
    fn test_build_parsed_datetime_interval() {
        use DateTimeField::*;
        let test_cases = [
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(6, 700_000_000)),
                    ..Default::default()
                },
                "1-2 3 4:5:6.7",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(6, 700_000_000)),
                    ..Default::default()
                },
                "1 year 2 months 3 days 4 hours 5 minutes 6.7 seconds",
                Second,
            ),
            (
                ParsedDateTime {
                    second: Some(DateTimeFieldValue::new(1, 0)),
                    ..Default::default()
                },
                "1",
                Second,
            ),
            (
                ParsedDateTime {
                    day: Some(DateTimeFieldValue::new(1, 0)),
                    ..Default::default()
                },
                "1",
                Day,
            ),
            (
                ParsedDateTime {
                    month: Some(DateTimeFieldValue::new(1, 0)),
                    ..Default::default()
                },
                "1",
                Month,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(1, 0)),
                    minute: Some(DateTimeFieldValue::new(0, 0)),
                    second: Some(DateTimeFieldValue::new(0, 0)),
                    ..Default::default()
                },
                "1:",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(0, 0)),
                    ..Default::default()
                },
                "1-",
                Second,
            ),
            (
                ParsedDateTime {
                    day: Some(DateTimeFieldValue::new(1, 0)),
                    hour: Some(DateTimeFieldValue::new(2, 0)),
                    minute: Some(DateTimeFieldValue::new(0, 0)),
                    second: Some(DateTimeFieldValue::new(0, 0)),
                    ..Default::default()
                },
                "1 2:",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    hour: Some(DateTimeFieldValue::new(3, 0)),
                    minute: Some(DateTimeFieldValue::new(4, 0)),
                    second: Some(DateTimeFieldValue::new(0, 0)),
                    ..Default::default()
                },
                "1-2 3:4",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(0, 0)),
                    day: Some(DateTimeFieldValue::new(2, 0)),
                    hour: Some(DateTimeFieldValue::new(3, 0)),
                    minute: Some(DateTimeFieldValue::new(0, 0)),
                    second: Some(DateTimeFieldValue::new(0, 0)),
                    ..Default::default()
                },
                "1- 2 3:",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(5, 0)),
                    month: Some(DateTimeFieldValue::new(6, 0)),
                    hour: Some(DateTimeFieldValue::new(1, 0)),
                    minute: Some(DateTimeFieldValue::new(2, 0)),
                    second: Some(DateTimeFieldValue::new(3, 400_000_000)),
                    ..Default::default()
                },
                "1:2:3.4 5-6",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    hour: Some(DateTimeFieldValue::new(3, 0)),
                    ..Default::default()
                },
                "1-2 3",
                Hour,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(-1, 0)),
                    month: Some(DateTimeFieldValue::new(-2, 0)),
                    day: Some(DateTimeFieldValue::new(-3, 0)),
                    hour: Some(DateTimeFieldValue::new(-4, 0)),
                    minute: Some(DateTimeFieldValue::new(-5, 0)),
                    second: Some(DateTimeFieldValue::new(-6, -700_000_000)),
                    ..Default::default()
                },
                "-1-2 -3 -4:5:6.7",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(-1, 0)),
                    month: Some(DateTimeFieldValue::new(-2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(-4, 0)),
                    minute: Some(DateTimeFieldValue::new(-5, 0)),
                    second: Some(DateTimeFieldValue::new(-6, -700_000_000)),
                    ..Default::default()
                },
                "-1-2 3 -4:5:6.7",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(-1, 0)),
                    month: Some(DateTimeFieldValue::new(-2, 0)),
                    day: Some(DateTimeFieldValue::new(-3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(0, 0)),
                    second: Some(DateTimeFieldValue::new(0, 500_000_000)),
                    ..Default::default()
                },
                "-1-2 -3 4::.5",
                Second,
            ),
            (
                ParsedDateTime {
                    hour: Some(DateTimeFieldValue::new(0, 0)),
                    minute: Some(DateTimeFieldValue::new(0, 0)),
                    second: Some(DateTimeFieldValue::new(-1, -270_000_000)),
                    ..Default::default()
                },
                "-::1.27",
                Second,
            ),
            (
                ParsedDateTime {
                    second: Some(DateTimeFieldValue::new(1, 270_000_000)),
                    ..Default::default()
                },
                ":::::1.27",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(0, 0)),
                    day: Some(DateTimeFieldValue::new(2, 0)),
                    hour: Some(DateTimeFieldValue::new(3, 0)),
                    minute: Some(DateTimeFieldValue::new(0, 0)),
                    second: Some(DateTimeFieldValue::new(0, 0)),
                    ..Default::default()
                },
                ":::1- ::2 ::::3:",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(6, 700_000_000)),
                    ..Default::default()
                },
                "1 years 2 months 3 days 4 hours 5 minutes 6.7 seconds",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(6, 700_000_000)),
                    ..Default::default()
                },
                "1y 2mon 3d 4h 5m 6.7s",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(6, 700_000_000)),
                    ..Default::default()
                },
                "6.7 seconds 5 minutes 3 days 4 hours 1 year 2 month",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(-1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(-3, 0)),
                    hour: Some(DateTimeFieldValue::new(4, 0)),
                    minute: Some(DateTimeFieldValue::new(5, 0)),
                    second: Some(DateTimeFieldValue::new(-6, -700_000_000)),
                    ..Default::default()
                },
                "-6.7 seconds 5 minutes -3 days 4 hours -1 year 2 month",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 300_000_000)),
                    day: Some(DateTimeFieldValue::new(4, 500_000_000)),
                    ..Default::default()
                },
                "1y 2.3mon 4.5d",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(-1, -200_000_000)),
                    month: Some(DateTimeFieldValue::new(2, 300_000_000)),
                    day: Some(DateTimeFieldValue::new(-3, -400_000_000)),
                    hour: Some(DateTimeFieldValue::new(4, 500_000_000)),
                    minute: Some(DateTimeFieldValue::new(5, 600_000_000)),
                    second: Some(DateTimeFieldValue::new(-6, -700_000_000)),
                    ..Default::default()
                },
                "-6.7 seconds 5.6 minutes -3.4 days 4.5 hours -1.2 year 2.3 month",
                Second,
            ),
            (
                ParsedDateTime {
                    day: Some(DateTimeFieldValue::new(1, 0)),
                    second: Some(DateTimeFieldValue::new(0, -270_000_000)),
                    ..Default::default()
                },
                "1 day -0.27 seconds",
                Second,
            ),
            (
                ParsedDateTime {
                    day: Some(DateTimeFieldValue::new(-1, 0)),
                    second: Some(DateTimeFieldValue::new(0, 270_000_000)),
                    ..Default::default()
                },
                "-1 day 0.27 seconds",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(10, 333_000_000)),
                    ..Default::default()
                },
                "10.333 years",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(10, 333_000_000)),
                    ..Default::default()
                },
                "10.333",
                Year,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(5, 0)),
                    hour: Some(DateTimeFieldValue::new(3, 0)),
                    minute: Some(DateTimeFieldValue::new(4, 0)),
                    second: Some(DateTimeFieldValue::new(0, 0)),
                    ..Default::default()
                },
                "1-2 3:4 5 day",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(5, 0)),
                    hour: Some(DateTimeFieldValue::new(3, 0)),
                    minute: Some(DateTimeFieldValue::new(4, 0)),
                    second: Some(DateTimeFieldValue::new(0, 0)),
                    ..Default::default()
                },
                "5 day 3:4 1-2",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    day: Some(DateTimeFieldValue::new(5, 0)),
                    hour: Some(DateTimeFieldValue::new(3, 0)),
                    minute: Some(DateTimeFieldValue::new(4, 0)),
                    second: Some(DateTimeFieldValue::new(0, 0)),
                    ..Default::default()
                },
                "1-2 5 day 3:4",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    day: Some(DateTimeFieldValue::new(2, 0)),
                    hour: Some(DateTimeFieldValue::new(3, 0)),
                    minute: Some(DateTimeFieldValue::new(4, 0)),
                    second: Some(DateTimeFieldValue::new(5, 600_000_000)),
                    ..Default::default()
                },
                "+1 year +2 days +3:4:5.6",
                Second,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    ..Default::default()
                },
                "1-2",
                Month,
            ),
            (
                ParsedDateTime {
                    year: Some(DateTimeFieldValue::new(1, 0)),
                    month: Some(DateTimeFieldValue::new(2, 0)),
                    ..Default::default()
                },
                "1-2",
                Minute,
            ),
            (
                ParsedDateTime {
                    day: Some(DateTimeFieldValue::new(1, 999_999_999)),
                    ..Default::default()
                },
                "1.999999999999999999 days",
                Second,
            ),
        ];

        for test in test_cases.iter() {
            let actual = build_parsed_datetime_interval(test.1, test.2);
            if actual != Ok(test.0.clone()) {
                panic!(
                    "In test INTERVAL '{}' {}\n actual: {:?} \n expected: {:?}",
                    test.1, test.2, actual, test.0
                );
            }
        }
    }

    #[test]
    fn test_build_parsed_datetime_interval_errors() {
        use DateTimeField::*;
        let test_cases = [
            (
                "1 year 2 years",
                Second,
                "Invalid INTERVAL '1 year 2 years': YEAR field \
                set twice",
            ),
            (
                "1-2 3-4",
                Second,
                "Invalid INTERVAL '1-2 3-4': YEAR or MONTH field \
                set twice",
            ),
            (
                "1-2 3 year",
                Second,
                "Invalid INTERVAL '1-2 3 year': YEAR field set twice",
            ),
            (
                "1-2 3",
                Month,
                "Invalid INTERVAL '1-2 3': MONTH field set twice",
            ),
            (
                "1-2 3:4 5",
                Second,
                "Invalid INTERVAL '1-2 3:4 5': SECOND field set twice",
            ),
            (
                "1:2:3.4 5-6 7",
                Year,
                "Invalid INTERVAL '1:2:3.4 5-6 7': YEAR field set twice",
            ),
            (
                "-:::::1.27",
                Second,
                "Invalid INTERVAL '-:::::1.27': Invalid syntax at \
                offset 7: provided Colon but expected None",
            ),
            (
                "-1 ::.27",
                Second,
                "Invalid INTERVAL '-1 ::.27': cannot determine format of all parts. \
                Add explicit time components, e.g. INTERVAL '1 day' or INTERVAL '1' DAY",
            ),
            (
                "100-13",
                Second,
                "Invalid INTERVAL '100-13': MONTH field out range; must be < 12, have 13",
            ),
            (
                "100-11 366 250:61",
                Second,
                "Invalid INTERVAL '100-11 366 250:61': MINUTE field out range; \
                must be < 60, have 61",
            ),
            (
                "100-11 366 250:59:61",
                Second,
                "Invalid INTERVAL '100-11 366 250:59:61': SECOND field out range; \
                must be < 60, have 61",
            ),
            (
                "1:2:3.4.5",
                Second,
                "Invalid INTERVAL '1:2:3.4.5': Invalid syntax at offset 7: provided \
                Dot but expected None",
            ),
            (
                "1+2:3.4",
                Second,
                "Invalid date-time type string '1+2:3.4': cannot determine format",
            ),
            (
                "1x2:3.4",
                Second,
                "invalid DateTimeField: X",
            ),
            (
                "0 foo",
                Second,
                "invalid DateTimeField: FOO",
            ),
            (
                "1-2 hour",
                Second,
                "Invalid INTERVAL '1-2 hour': Hour must be preceeded by a number, e.g. '1Hour'",
            ),
            (
                "1-2hour",
                Second,
                "Invalid INTERVAL '1-2hour': Invalid syntax at offset 3: provided TimeUnit(Hour) \
                but expected Space",
            ),
            (
                "1-2 3:4 5 second",
                Second,
                "Invalid INTERVAL '1-2 3:4 5 second': SECOND field set twice",
            ),
            (
                "1-2 5 second 3:4",
                Second,
                "Invalid INTERVAL '1-2 5 second 3:4': HOUR, MINUTE, SECOND field set twice",
            ),
            (
                "1 2-3 4:5",
                Day,
                "Invalid INTERVAL '1 2-3 4:5': cannot determine format of all parts. Add \
                explicit time components, e.g. INTERVAL '1 day' or INTERVAL '1' DAY",
            ),
            (
                "9223372036854775808 months",
                Day,
                "Unable to parse value as a number at index 0: number too large to fit in target type",
            ),
            (
                "-9223372036854775808 months",
                Day,
                "Unable to parse value as a number at index 0: number too large to fit in target type",
            ),
            (
                "9223372036854775808 seconds",
                Day,
                "Unable to parse value as a number at index 0: number too large to fit in target type",
            ),
            (
                "-9223372036854775808 seconds",
                Day,
                "Unable to parse value as a number at index 0: number too large to fit in target type",
            ),
        ];
        for test in test_cases.iter() {
            match build_parsed_datetime_interval(test.0, test.1) {
                Err(e) => assert_eq!(e.to_string(), test.2),
                Ok(pdt) => panic!(
                    "Test INTERVAL '{}' {} passed when expected to fail with {}, generated ParsedDateTime {:?}",
                    test.0,
                    test.1,
                    test.2,
                    pdt,
                ),
            }
        }
    }

    #[test]
    fn test_split_timestamp_string() {
        let test_cases = [
            (
                "1969-06-01 10:10:10.410 UTC",
                "1969-06-01 10:10:10.410",
                "UTC",
            ),
            (
                "1969-06-01 10:10:10.410+4:00",
                "1969-06-01 10:10:10.410",
                "+4:00",
            ),
            (
                "1969-06-01 10:10:10.410-4:00",
                "1969-06-01 10:10:10.410",
                "-4:00",
            ),
            ("1969-06-01 10:10:10.410", "1969-06-01 10:10:10.410", ""),
            ("1969-06-01 10:10:10.410+4", "1969-06-01 10:10:10.410", "+4"),
            ("1969-06-01 10:10:10.410-4", "1969-06-01 10:10:10.410", "-4"),
            ("1969-06-01 10:10:10+4:00", "1969-06-01 10:10:10", "+4:00"),
            ("1969-06-01 10:10:10-4:00", "1969-06-01 10:10:10", "-4:00"),
            ("1969-06-01 10:10:10 UTC", "1969-06-01 10:10:10", "UTC"),
            ("1969-06-01 10:10:10", "1969-06-01 10:10:10", ""),
            ("1969-06-01 10:10+4:00", "1969-06-01 10:10", "+4:00"),
            ("1969-06-01 10:10-4:00", "1969-06-01 10:10", "-4:00"),
            ("1969-06-01 10:10 UTC", "1969-06-01 10:10", "UTC"),
            ("1969-06-01 10:10", "1969-06-01 10:10", ""),
            ("1969-06-01 UTC", "1969-06-01", "UTC"),
            ("1969-06-01 +4:00", "1969-06-01", "+4:00"),
            ("1969-06-01 -4:00", "1969-06-01", "-4:00"),
            ("1969-06-01 +4", "1969-06-01", "+4"),
            ("1969-06-01 -4", "1969-06-01", "-4"),
            ("1969-06-01", "1969-06-01", ""),
            ("1969-06-01 10:10:10.410Z", "1969-06-01 10:10:10.410", "Z"),
            ("1969-06-01 10:10:10.410z", "1969-06-01 10:10:10.410", "z"),
            ("1969-06-01Z", "1969-06-01", "Z"),
            ("1969-06-01z", "1969-06-01", "z"),
            ("1969-06-01 10:10:10.410   ", "1969-06-01 10:10:10.410", ""),
            (
                "1969-06-01     10:10:10.410   ",
                "1969-06-01     10:10:10.410",
                "",
            ),
            ("   1969-06-01 10:10:10.412", "1969-06-01 10:10:10.412", ""),
            (
                "   1969-06-01 10:10:10.413   ",
                "1969-06-01 10:10:10.413",
                "",
            ),
            (
                "1969-06-01 10:10:10.410 +4:00",
                "1969-06-01 10:10:10.410",
                "+4:00",
            ),
            (
                "1969-06-01 10:10:10.410+4 :00",
                "1969-06-01 10:10:10.410",
                "+4 :00",
            ),
            (
                "1969-06-01 10:10:10.410      +4:00",
                "1969-06-01 10:10:10.410",
                "+4:00",
            ),
            (
                "1969-06-01 10:10:10.410+4:00     ",
                "1969-06-01 10:10:10.410",
                "+4:00",
            ),
            (
                "1969-06-01 10:10:10.410  Z  ",
                "1969-06-01 10:10:10.410",
                "Z",
            ),
            ("1969-06-01    +4  ", "1969-06-01", "+4"),
            ("1969-06-01   Z   ", "1969-06-01", "Z"),
        ];

        for test in test_cases.iter() {
            let (ts, tz) = split_timestamp_string(test.0);

            assert_eq!(ts, test.1);
            assert_eq!(tz, test.2);
        }
    }

    #[test]
    fn test_parse_timezone_offset_second() {
        let test_cases = [
            ("+0:00", 0),
            ("-0:00", 0),
            ("+0:000000", 0),
            ("+000000:00", 0),
            ("+000000:000000", 0),
            ("+0", 0),
            ("+00", 0),
            ("+000", 0),
            ("+0000", 0),
            ("+00000000", 0),
            ("+0000001:000000", 3600),
            ("+0000000:000001", 60),
            ("+0000001:000001", 3660),
            ("+4:00", 14400),
            ("-4:00", -14400),
            ("+2:30", 9000),
            ("-5:15", -18900),
            ("+0:20", 1200),
            ("-0:20", -1200),
            ("+5", 18000),
            ("-5", -18000),
            ("+05", 18000),
            ("-05", -18000),
            ("+500", 18000),
            ("-500", -18000),
            ("+530", 19800),
            ("-530", -19800),
            ("+050", 3000),
            ("-050", -3000),
            ("+15", 54000),
            ("-15", -54000),
            ("+1515", 54900),
            ("+015", 900),
            ("-015", -900),
            ("+0015", 900),
            ("-0015", -900),
            ("+00015", 900),
            ("-00015", -900),
            ("+005", 300),
            ("-005", -300),
            ("+0000005", 300),
            ("+00000100", 3600),
            ("Z", 0),
            ("z", 0),
        ];

        for test in test_cases.iter() {
            match parse_timezone_offset_second(test.0) {
                Ok(tz_offset) => {
                    let expected: i64 = test.1 as i64;

                    println!("{} {}", expected, tz_offset);
                    assert_eq!(tz_offset, expected);
                }
                Err(e) => panic!(
                    "Test failed when expected to pass test case: {} error: {}",
                    test.0, e
                ),
            }
        }

        let failure_test_cases = [
            "+25:00", "+120:00", "+0:61", "+0:500", " 12:30", "+-12:30", "+2525", "+2561",
            "+255900", "+25", "+5::30", "+5:30:", "+5:30:16", "+5:", "++5:00", "--5:00", "UTC",
            " UTC", "a", "zzz", "ZZZ", "ZZ Top", " +", " -", " ", "1", "12", "1234",
        ];

        for test in failure_test_cases.iter() {
            match parse_timezone_offset_second(test) {
                Ok(t) => panic!("Test passed when expected to fail test case: {} parsed tz offset (seconds): {}", test, t),
                Err(e) => println!("{}", e),
            }
        }
    }
}
