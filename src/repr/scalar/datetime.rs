// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use failure::{bail, ensure, format_err};

use super::Interval;
use sql_parser::ast::DateTimeField;

type Result<T> = std::result::Result<T, failure::Error>;

/// Tracks a unit and a fraction from a parsed time-like string, e.g. INTERVAL
/// '1.2' DAYS.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct DateTimeFieldValue {
    /// Integer part of the value.
    pub unit: i64,
    /// Fractional part of value, padded to billions/has 9 digits of precision,
    /// e.g. `.5` is represented as `500000000`.
    pub fraction: i64,
}

impl Default for DateTimeFieldValue {
    fn default() -> Self {
        DateTimeFieldValue {
            unit: 0,
            fraction: 0,
        }
    }
}

impl DateTimeFieldValue {
    /// Construct `DateTimeFieldValue { unit, fraction }`.
    pub fn new(unit: i64, fraction: i64) -> Self {
        DateTimeFieldValue { unit, fraction }
    }
}

/// All of the fields that can appear in a literal `DATE`, `TIME`, `TIMESTAMP`
/// or `INTERVAL` string.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ParsedDateTime {
    pub year: Option<DateTimeFieldValue>,
    pub month: Option<DateTimeFieldValue>,
    pub day: Option<DateTimeFieldValue>,
    pub hour: Option<DateTimeFieldValue>,
    pub minute: Option<DateTimeFieldValue>,
    // second.fraction is equivalent to nanoseconds.
    pub second: Option<DateTimeFieldValue>,
    pub timezone_offset_second: Option<i64>,
}

impl Default for ParsedDateTime {
    fn default() -> Self {
        ParsedDateTime {
            year: None,
            month: None,
            day: None,
            hour: None,
            minute: None,
            second: None,
            timezone_offset_second: None,
        }
    }
}

impl ParsedDateTime {
    /// Compute an Interval from an IntervalValue. This could be adapted
    /// to `impl TryFrom<IntervalValue> for Interval` but is slightly more
    /// ergononmic because it doesn't require exposing all of the private
    /// functions this method calls.
    ///
    /// # Errors
    /// - If any component overflows a parameter (i.e. i64).
    pub fn compute_interval(&self) -> Result<Interval> {
        use DateTimeField::*;
        let mut months = 0i64;
        let mut seconds = 0i64;
        let mut nanos = 0i64;

        // Add all DateTimeFields, from Year to Seconds.
        self.add_field(Year, &mut months, &mut seconds, &mut nanos)?;

        for field in Year.into_iter().take_while(|f| *f <= Second) {
            self.add_field(field, &mut months, &mut seconds, &mut nanos)?;
        }

        // Handle negative seconds with positive nanos or vice versa.
        if nanos < 0 && seconds > 0 {
            nanos += 1_000_000_000_i64;
            seconds -= 1;
        } else if nanos > 0 && seconds < 0 {
            nanos -= 1_000_000_000_i64;
            seconds += 1;
        }

        Ok(Interval {
            months,
            duration: Duration::new(seconds.abs() as u64, nanos.abs() as u32),
            is_positive_dur: seconds >= 0 && nanos >= 0,
        })
    }
    /// Adds the appropriate values from self's ParsedDateTime to `months`,
    /// `seconds`, and `nanos`. These fields are then appropriate to construct
    /// std::time::Duration, once accounting for their sign.
    ///
    /// # Errors
    /// - If any component overflows a parameter (i.e. i64).
    fn add_field(
        &self,
        d: DateTimeField,
        months: &mut i64,
        seconds: &mut i64,
        nanos: &mut i64,
    ) -> Result<()> {
        use DateTimeField::*;
        match d {
            Year => {
                let (y, y_f) = match self.units_of(Year) {
                    Some(y) => (y.unit, y.fraction),
                    None => return Ok(()),
                };
                // months += y * 12
                *months = y
                    .checked_mul(12)
                    .and_then(|y_m| months.checked_add(y_m))
                    .ok_or_else(|| {
                        format_err!(
                            "Overflows maximum months; \
                             cannot exceed {} months",
                            std::i64::MAX
                        )
                    })?;

                // months += y_f * 12 / 1_000_000_000
                *months = y_f
                    .checked_mul(12)
                    .and_then(|y_f_m| months.checked_add(y_f_m / 1_000_000_000))
                    .ok_or_else(|| {
                        format_err!(
                            "Overflows maximum months; \
                             cannot exceed {} months",
                            std::i64::MAX
                        )
                    })?;
                Ok(())
            }
            Month => {
                let (m, m_f) = match self.units_of(Month) {
                    Some(m) => (m.unit, m.fraction),
                    None => return Ok(()),
                };

                *months = m.checked_add(*months).ok_or_else(|| {
                    format_err!(
                        "Overflows maximum months; \
                         cannot exceed {} months",
                        std::i64::MAX
                    )
                })?;

                let m_f_ns = m_f
                    .checked_mul(30 * seconds_multiplier(Day))
                    .ok_or_else(|| format_err!("Intermediate overflow in MONTH fraction"))?;

                // seconds += m_f * 30 * seconds_multiplier(Day) / 1_000_000_000
                *seconds = seconds.checked_add(m_f_ns / 1_000_000_000).ok_or_else(|| {
                    format_err!(
                        "Overflows maximum seconds; \
                         cannot exceed {} seconds",
                        std::i64::MAX
                    )
                })?;

                *nanos += m_f_ns % 1_000_000_000;
                Ok(())
            }
            dhms => {
                let (t, t_f) = match self.units_of(dhms) {
                    Some(t) => (t.unit, t.fraction),
                    None => return Ok(()),
                };

                *seconds = t
                    .checked_mul(seconds_multiplier(d))
                    .and_then(|t_s| seconds.checked_add(t_s))
                    .ok_or_else(|| {
                        format_err!(
                            "Overflows maximum seconds; \
                             cannot exceed {} seconds",
                            std::i64::MAX
                        )
                    })?;

                let t_f_ns = t_f
                    .checked_mul(seconds_multiplier(dhms))
                    .ok_or_else(|| format_err!("Intermediate overflow in {} fraction", dhms))?;

                // seconds += t_f * seconds_multiplier(dhms) / 1_000_000_000
                *seconds = seconds.checked_add(t_f_ns / 1_000_000_000).ok_or_else(|| {
                    format_err!(
                        "Overflows maximum seconds; \
                         cannot exceed {} seconds",
                        std::i64::MAX
                    )
                })?;

                *nanos += t_f_ns % 1_000_000_000;
                Ok(())
            }
        }
    }

    /// Builds a ParsedDateTime from an interval string (`value`).
    ///
    /// # Arguments
    ///
    /// * `value` is a PostgreSQL-compatible interval string, e.g `INTERVAL 'value'`.
    /// * `ambiguous_resolver` identifies the DateTimeField of the final part
    ///   if it's ambiguous, e.g. in `INTERVAL '1' MONTH` '1' is ambiguous as its
    ///   DateTimeField, but MONTH resolves the ambiguity.
    pub fn build_parsed_datetime_interval(
        value: &str,
        ambiguous_resolver: DateTimeField,
    ) -> Result<ParsedDateTime> {
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
            let mut fmt = determine_format_w_datetimefield(&part)?;
            // If you cannot determine the format of this part, try to infer its
            // format.
            if fmt.is_none() {
                fmt = match value_parts.next() {
                    Some(next_part) => {
                        match determine_format_w_datetimefield(&next_part)? {
                            Some(TimePartFormat::SqlStandard(f)) => {
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
                                            fmt: TimePartFormat::SqlStandard(f),
                                            tokens: next_part.clone(),
                                        });
                                        Some(TimePartFormat::PostgreSql(Day))
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
                    None => Some(TimePartFormat::PostgreSql(ambiguous_resolver)),
                }
            }
            match fmt {
                Some(fmt) => annotated_parts.push(AnnotatedIntervalPart {
                    fmt,
                    tokens: part.clone(),
                }),
                None => bail!(
                    "Cannot determine format of all parts. Add \
                     explicit time components, e.g. INTERVAL '1 day' or INTERVAL '1' DAY"
                ),
            }
        }

        for ap in annotated_parts {
            match ap.fmt {
                TimePartFormat::SqlStandard(f) => {
                    fill_pdt_sql_standard(&ap.tokens, f, &mut pdt)?;
                    pdt.check_interval_bounds(f)?;
                }
                TimePartFormat::PostgreSql(f) => fill_pdt_pg(&ap.tokens, f, &mut pdt)?,
            }
        }

        Ok(pdt)
    }
    /// Builds a ParsedDateTime from a DATE string (`value`).
    ///
    /// # Arguments
    ///
    /// * `value` is a SQL-formatted DATE string.
    pub fn build_parsed_datetime_date(value: &str) -> Result<ParsedDateTime> {
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

        fill_pdt_from_tokens(
            &mut pdt,
            &mut date_actual,
            &mut date_expected,
            DateTimeField::Year,
            1,
        )?;

        Ok(pdt)
    }
    /// Builds a ParsedDateTime from a TIME string (`value`).
    ///
    /// # Arguments
    ///
    /// * `value` is a SQL-formatted TIME string.
    pub fn build_parsed_datetime_time(value: &str) -> Result<ParsedDateTime> {
        let mut pdt = ParsedDateTime::default();

        let time_actual = tokenize_time_str(value)?;

        match determine_format_w_datetimefield(&time_actual.clone())? {
            Some(TimePartFormat::SqlStandard(leading_field)) => {
                fill_pdt_sql_standard(&time_actual, leading_field, &mut pdt)?
            }
            _ => bail!("Unknown format"),
        }

        Ok(pdt)
    }
    /// Write to the specified field of a ParsedDateTime iff it is currently set
    /// to None; otherwise generate an error to propagate to the user.
    pub fn write_field_iff_none(
        &mut self,
        f: DateTimeField,
        u: Option<DateTimeFieldValue>,
    ) -> Result<()> {
        use DateTimeField::*;

        match f {
            Year if self.year.is_none() => {
                self.year = u;
            }
            Month if self.month.is_none() => {
                self.month = u;
            }
            Day if self.day.is_none() => {
                self.day = u;
            }
            Hour if self.hour.is_none() => {
                self.hour = u;
            }
            Minute if self.minute.is_none() => {
                self.minute = u;
            }
            Second if self.second.is_none() => {
                self.second = u;
            }
            _ => failure::bail!("{} field set twice", f),
        }
        Ok(())
    }
    pub fn check_datelike_bounds(&self) -> Result<()> {
        if let Some(month) = self.month {
            ensure!(
                month.unit < 13 && month.unit > 0,
                "MONTH must be (1, 12), got {}",
                month.unit
            );
        }
        if let Some(day) = self.day {
            ensure!(
                day.unit < 32 && day.unit > 0,
                "DAY must be (1, 31), got {}",
                day.unit
            );
        }
        if let Some(hour) = self.hour {
            ensure!(
                hour.unit < 24 && hour.unit >= 0,
                "HOUR must be (0, 23), got {}",
                hour.unit
            );
        }
        if let Some(minute) = self.minute {
            ensure!(
                minute.unit < 60 && minute.unit >= 0,
                "MINUTE must be (0, 59), got {}",
                minute.unit
            );
        }

        if let Some(second) = self.second {
            ensure!(
                second.unit < 61 && second.unit >= 0,
                "SECOND must be (0, 60), got {}",
                second.unit
            );
            ensure!(
                second.fraction < 1_000_000_001 && second.fraction >= 0,
                "NANOSECOND must be (0, 1_000_000_000), got {}",
                second.fraction
            );
        }

        Ok(())
    }
    pub fn check_interval_bounds(&self, d: DateTimeField) -> Result<()> {
        use DateTimeField::*;

        match d {
            Year | Month => {
                if let Some(month) = self.month {
                    ensure!(
                        month.unit < 13 && month.unit > -13,
                        "MONTH must be (-12, 12), got {}",
                        month.unit
                    );
                }
            }
            Hour | Minute | Second => {
                if let Some(minute) = self.minute {
                    ensure!(
                        minute.unit < 60 && minute.unit > -60,
                        "MINUTE must be (-59, 59), got {}",
                        minute.unit
                    );
                }
                if let Some(second) = self.second {
                    ensure!(
                        second.unit < 61 && second.unit > -61,
                        "SECOND must be (-60, 60), got {}",
                        second.unit
                    );
                    ensure!(
                        second.fraction < 1_000_000_001 && second.fraction > -1_000_000_001,
                        "NANOSECOND must be (-1_000_000_000, 1_000_000_000), got {}",
                        second.fraction
                    );
                }
            }
            Day => {}
        }

        Ok(())
    }
    /// Retrieve any value that we parsed out of the literal string for the
    /// `field`.
    fn units_of(&self, field: DateTimeField) -> Option<DateTimeFieldValue> {
        match field {
            DateTimeField::Year => self.year,
            DateTimeField::Month => self.month,
            DateTimeField::Day => self.day,
            DateTimeField::Hour => self.hour,
            DateTimeField::Minute => self.minute,
            DateTimeField::Second => self.second,
        }
    }
}

/// Returns the number of seconds in a single unit of `field`.
fn seconds_multiplier(field: DateTimeField) -> i64 {
    match field {
        DateTimeField::Day => 60 * 60 * 24,
        DateTimeField::Hour => 60 * 60,
        DateTimeField::Minute => 60,
        DateTimeField::Second => 1,
        _other => unreachable!("Do not call with a non-duration field"),
    }
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
    mut pdt: &mut ParsedDateTime,
) -> Result<()> {
    use DateTimeField::*;

    // Ensure that no fields have been previously modified.
    match leading_field {
        Year | Month => {
            if pdt.year.is_some() || pdt.month.is_some() {
                bail!("YEAR or MONTH field set twice")
            }
        }
        Day => {
            if pdt.day.is_some() {
                bail!("DAY field set twice",)
            }
        }
        Hour | Minute | Second => {
            if pdt.hour.is_some() || pdt.minute.is_some() || pdt.second.is_some() {
                bail!("HOUR, MINUTE, SECOND field set twice")
            }
        }
    }

    let mut actual = v.iter().peekable();
    let expected = expected_sql_standard_interval_tokens(leading_field);
    let mut expected = expected.iter().peekable();

    let sign = trim_interval_chars_return_sign(&mut actual);

    fill_pdt_from_tokens(&mut pdt, &mut actual, &mut expected, leading_field, sign)?;

    // Do not allow any fields in the group to be modified afterward, and check
    // that values are valid. SQL standard-style interval parts do not allow
    // non-leading group components to "overflow" into the next-greatest
    // component, e.g. months cannot overflow into years.
    match leading_field {
        Year | Month => {
            if pdt.year.is_none() {
                pdt.year = Some(DateTimeFieldValue::default());
            }
            if pdt.month.is_none() {
                pdt.month = Some(DateTimeFieldValue::default());
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
            if pdt.minute.is_none() {
                pdt.minute = Some(DateTimeFieldValue::default());
            }
            if pdt.second.is_none() {
                pdt.second = Some(DateTimeFieldValue::default());
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
    mut pdt: &mut ParsedDateTime,
) -> Result<()> {
    use TimeStrToken::*;

    let mut actual = tokens.iter().peekable();
    // We remove all spaces during tokenization, so TimeUnit only shows up if
    // there is no space between the number and the TimeUnit, e.g. `1y 2d 3h`, which
    // PostgreSQL allows.
    let expected = vec![Num(0), Dot, Nanos(0), TimeUnit(DateTimeField::Year)];
    let mut expected = expected.iter().peekable();

    let sign = trim_interval_chars_return_sign(&mut actual);

    fill_pdt_from_tokens(&mut pdt, &mut actual, &mut expected, time_unit, sign)?;

    Ok(())
}

/// Fills a ParsedDateTime's fields using the `actual` tokens, starting at `leading_field`
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
) -> Result<()> {
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
    SqlStandard(DateTimeField),
    PostgreSql(DateTimeField),
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
fn determine_format_w_datetimefield(toks: &[TimeStrToken]) -> Result<Option<TimePartFormat>> {
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
                Some(TimeUnit(f)) => Ok(Some(PostgreSql(*f))),
                // Implies {?}{?}{?}, ambiguous case.
                _ => Ok(None),
            }
        }
        // Implies {Y-...}{}{}
        Some(Dash) => Ok(Some(SqlStandard(Year))),
        // Implies {}{}{?:...}
        Some(Colon) => {
            if let Some(Num(_)) = toks.peek() {
                toks.next();
            }
            match toks.peek() {
                // Implies {H:M:?...}
                Some(Colon) | Some(Space) | None => Ok(Some(SqlStandard(Hour))),
                // Implies {M:S.NS}
                Some(Dot) => Ok(Some(SqlStandard(Minute))),
                _ => bail!("Cannot determine format of all parts"),
            }
        }
        // Implies {Num}?{TimeUnit}
        Some(TimeUnit(f)) => Ok(Some(PostgreSql(*f))),
        _ => bail!("Cannot determine format of all parts"),
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
pub(crate) fn tokenize_time_str(value: &str) -> Result<Vec<TimeStrToken>> {
    use std::str::FromStr;

    let mut toks = vec![];
    let mut num_buf = String::with_capacity(4);
    let mut char_buf = String::with_capacity(7);
    fn parse_num(n: &str, idx: usize) -> Result<TimeStrToken> {
        Ok(TimeStrToken::Num(n.parse().map_err(|e| {
            format_err!("Unable to parse value as a number at index {}: {}", idx, e)
        })?))
    };
    fn maybe_tokenize_num_buf(n: &mut String, i: usize, t: &mut Vec<TimeStrToken>) -> Result<()> {
        if !n.is_empty() {
            t.push(parse_num(&n, i)?);
            n.clear();
        }
        Ok(())
    }
    fn maybe_tokenize_char_buf(c: &mut String, t: &mut Vec<TimeStrToken>) -> Result<()> {
        if !c.is_empty() {
            let tu = match DateTimeField::from_str(&c.to_uppercase()) {
                Ok(tu) => tu,
                // DateTimeField::from_str's errors are String, but we
                // need failure::Error, so cannot rely on ? for conversion.
                Err(e) => bail!("{}", e),
            };
            t.push(TimeStrToken::TimeUnit(tu));
            c.clear();
        }
        Ok(())
    }
    let mut last_field_is_frac = false;
    for (i, chr) in value.chars().enumerate() {
        if !num_buf.is_empty() && !char_buf.is_empty() {
            bail!("Could not tokenize")
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
            chr => bail!("Invalid character at offset {} in {}: {:?}", i, value, chr),
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
                num_buf = num_buf[..default_precision].to_string();
                chars = default_precision;
            }
            let raw: i64 = num_buf
                .parse()
                .map_err(|e| format_err!("couldn't parse fraction {}: {}", num_buf, e))?;
            let multiplicand = 1_000_000_000 / 10_i64.pow(chars as u32);

            toks.push(TimeStrToken::Nanos(raw * multiplicand));
        }
    } else {
        maybe_tokenize_char_buf(&mut char_buf, &mut toks)?
    }
    Ok(toks)
}

fn tokenize_timezone(value: &str) -> Result<Vec<TimeStrToken>> {
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
    ) -> Result<()> {
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
            format_err!(
                "Unable to tokenize value {} as a number at index {}: {}",
                first,
                idx,
                e
            )
        })?));

        if let Some(second) = second {
            toks.push(TimeStrToken::Num(second.parse().map_err(|e| {
                format_err!(
                    "Unable to tokenize value {} as a number at index {}: {}",
                    second,
                    idx,
                    e
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
            chr => bail!(
                "Error tokenizing timezone string ('{}'): invalid character {:?} at offset {}",
                value,
                chr,
                i
            ),
        }
    }
    parse_num(&mut toks, &num_buf, split_nums, 0)?;
    Ok(toks)
}

fn build_timezone_offset_second(tokens: &[TimeStrToken], value: &str) -> Result<i64> {
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
                        (None, None) => {
                            if val <= 24 {
                                hour_offset = Some(val as i64);
                            } else {
                                // We can return an error here because in all the
                                // formats with numbers we require the first number
                                // to be an hour and we require it to be <= 24
                                bail!(
                                    "Invalid timezone string ({}): timezone hour invalid {}",
                                    value,
                                    val
                                )
                            }
                        }
                        (Some(_), None) => {
                            if val <= 60 {
                                minute_offset = Some(val as i64);
                            } else {
                                bail!(
                                    "Invalid timezone string ({}): timezone minute invalid {}",
                                    value,
                                    val
                                )
                            }
                        }
                        // We've already seen an hour and a minute so we should
                        // never see another number
                        (Some(_), Some(_)) => bail!(
                            "Invalid timezone string ({}): invalid value {} at token index {}",
                            value,
                            val,
                            i
                        ),
                        (None, Some(_)) => unreachable!("parsed a minute before an hour!"),
                    }
                }
                (Zulu, Zulu) => return Ok(0 as i64),
                (TzName(val), TzName(_)) => {
                    // For now, we don't support named timezones
                    bail!(
                        "Invalid timezone string ({}): named timezones are not supported. \
                         Failed to parse {} at token index {}",
                        value,
                        val,
                        i
                    )
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

    bail!("Cannot parse timezone offset {}", value)
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
        let cut = value.find(|c: char| c.is_ascii_alphabetic());

        if let Some(cut) = cut {
            let (first, second) = value.split_at(cut);
            return (first.trim(), second.trim());
        }

        (value.trim(), "")
    }
}

pub(crate) fn parse_timezone_offset_second(value: &str) -> Result<i64> {
    let toks = tokenize_timezone(value)?;
    Ok(build_timezone_offset_second(&toks, value)?)
}

#[cfg(test)]
mod test {
    use super::*;

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
            ("1-2 3", Some(SqlStandard(Year))),
            ("4:5", Some(SqlStandard(Hour))),
            ("4:5.6", Some(SqlStandard(Minute))),
            ("-4:5.6", Some(SqlStandard(Minute))),
            ("+4:5.6", Some(SqlStandard(Minute))),
            ("year", Some(PostgreSql(Year))),
            ("4year", Some(PostgreSql(Year))),
            ("-4year", Some(PostgreSql(Year))),
            ("5", None),
            ("5.6", None),
            ("3 4:5:6.7", None),
        ];

        for test in test_cases.iter() {
            let s = tokenize_time_str(test.0).unwrap();

            match (
                determine_format_w_datetimefield(&s).unwrap(),
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
            ("1+2", "Cannot determine format of all parts"),
            ("1:2+3", "Cannot determine format of all parts"),
            ("1:1YEAR2", "Cannot determine format of all parts"),
        ];

        for test in test_cases.iter() {
            let s = tokenize_time_str(test.0).unwrap();
            match determine_format_w_datetimefield(&s) {
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
            fill_pdt_pg(&actual, test.2, &mut pdt).unwrap();

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
                "Invalid syntax at offset 3: provided Dot but expected TimeUnit(Year)",
            ),
            (
                "YEAR",
                Year,
                "Year must be preceeded by a number, e.g. \'1Year\'",
            ),
            // Running into this error means that determine_format_w_datetimefield
            // failed.
            (
                "1YEAR",
                Month,
                "Invalid syntax at offset 3: provided Month but expected Year\'",
            ),
        ];
        for test in test_cases.iter() {
            let mut pdt = ParsedDateTime::default();
            let actual = tokenize_time_str(test.0).unwrap();
            match fill_pdt_pg(&actual, test.1, &mut pdt) {
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
            fill_pdt_sql_standard(&actual, test.2, &mut pdt).unwrap();

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
                "Invalid syntax at offset 1: provided Dot but expected Dash",
            ),
            (
                "1-2:3.4",
                Minute,
                "Invalid syntax at offset 1: provided Dash but expected Colon",
            ),
            (
                "1:2.2.",
                Minute,
                "Invalid syntax at offset 5: provided Dot but expected None",
            ),
            (
                "1YEAR",
                Year,
                "Invalid syntax at offset 1: provided TimeUnit(Year) but expected Dash",
            ),
        ];
        for test in test_cases.iter() {
            let mut pdt = ParsedDateTime::default();
            let actual = tokenize_time_str(test.0).unwrap();
            match fill_pdt_sql_standard(&actual, test.1, &mut pdt) {
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
            assert_eq!(
                ParsedDateTime::build_parsed_datetime_date(test).unwrap(),
                res
            );
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
            assert_eq!(
                ParsedDateTime::build_parsed_datetime_time(test).unwrap(),
                res
            );
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
            let actual = ParsedDateTime::build_parsed_datetime_interval(test.1, test.2).unwrap();
            if actual != test.0 {
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
                "YEAR field set twice",
            ),
            (
                "1-2 3-4",
                Second,
                "YEAR or MONTH field set twice",
            ),
            (
                "1-2 3 year",
                Second,
                "YEAR field set twice",
            ),
            (
                "1-2 3",
                Month,
                "MONTH field set twice",
            ),
            (
                "1-2 3:4 5",
                Second,
                "SECOND field set twice",
            ),
            (
                "1:2:3.4 5-6 7",
                Year,
                "YEAR field set twice",
            ),
            (
                "-:::::1.27",
                Second,
                "Invalid syntax at \
                offset 7: provided Colon but expected None",
            ),
            (
                "-1 ::.27",
                Second,
                "Cannot determine format of all parts. Add explicit time components, e.g. \
                INTERVAL '1 day' or INTERVAL '1' DAY",
            ),
            (
                "1:2:3.4.5",
                Second,
                "Invalid syntax at offset 7: provided Dot but expected None",
            ),
            (
                "1+2:3.4",
                Second,
                "Cannot determine format of all parts",
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
                "Hour must be preceeded by a number, e.g. '1Hour'",
            ),
            (
                "1-2hour",
                Second,
                "Invalid syntax at offset 3: provided TimeUnit(Hour) but expected Space",
            ),
            (
                "1-2 3:4 5 second",
                Second,
                "SECOND field set twice",
            ),
            (
                "1-2 5 second 3:4",
                Second,
                "HOUR, MINUTE, SECOND field set twice",
            ),
            (
                "1 2-3 4:5",
                Day,
                "Cannot determine format of all parts. Add explicit time components, e.g. \
                INTERVAL '1 day' or INTERVAL '1' DAY",
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
            match ParsedDateTime::build_parsed_datetime_interval(test.0, test.1) {
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

#[test]
fn test_parseddatetime_add_field() {
    use DateTimeField::*;
    let pdt_unit = ParsedDateTime {
        year: Some(DateTimeFieldValue::new(1, 0)),
        month: Some(DateTimeFieldValue::new(2, 0)),
        day: Some(DateTimeFieldValue::new(2, 0)),
        hour: Some(DateTimeFieldValue::new(3, 0)),
        minute: Some(DateTimeFieldValue::new(4, 0)),
        second: Some(DateTimeFieldValue::new(5, 0)),
        ..Default::default()
    };

    let pdt_frac = ParsedDateTime {
        year: Some(DateTimeFieldValue::new(1, 555_555_555)),
        month: Some(DateTimeFieldValue::new(2, 555_555_555)),
        day: Some(DateTimeFieldValue::new(2, 555_555_555)),
        hour: Some(DateTimeFieldValue::new(3, 555_555_555)),
        minute: Some(DateTimeFieldValue::new(4, 555_555_555)),
        second: Some(DateTimeFieldValue::new(5, 555_555_555)),
        ..Default::default()
    };

    let pdt_frac_neg = ParsedDateTime {
        year: Some(DateTimeFieldValue::new(-1, -555_555_555)),
        month: Some(DateTimeFieldValue::new(-2, -555_555_555)),
        day: Some(DateTimeFieldValue::new(-2, -555_555_555)),
        hour: Some(DateTimeFieldValue::new(-3, -555_555_555)),
        minute: Some(DateTimeFieldValue::new(-4, -555_555_555)),
        second: Some(DateTimeFieldValue::new(-5, -555_555_555)),
        ..Default::default()
    };

    run_test_parseddatetime_add_field(pdt_unit.clone(), Year, (12, 0, 0));
    run_test_parseddatetime_add_field(pdt_unit.clone(), Month, (2, 0, 0));
    run_test_parseddatetime_add_field(pdt_unit.clone(), Day, (0, 2 * 60 * 60 * 24, 0));
    run_test_parseddatetime_add_field(pdt_unit.clone(), Hour, (0, 3 * 60 * 60, 0));
    run_test_parseddatetime_add_field(pdt_unit.clone(), Minute, (0, 4 * 60, 0));
    run_test_parseddatetime_add_field(pdt_unit, Second, (0, 5, 0));
    run_test_parseddatetime_add_field(pdt_frac.clone(), Year, (18, 0, 0));
    run_test_parseddatetime_add_field(
        pdt_frac.clone(),
        Month,
        (
            2,
            // 16 days 15:59:59.99856
            16 * 60 * 60 * 24 + 15 * 60 * 60 + 59 * 60 + 59,
            998_560_000,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac.clone(),
        Day,
        (
            0,
            // 2 days 13:19:59.999952
            2 * 60 * 60 * 24 + 13 * 60 * 60 + 19 * 60 + 59,
            999_952_000,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac.clone(),
        Hour,
        (
            0,
            // 03:33:19.999998
            3 * 60 * 60 + 33 * 60 + 19,
            999_998_000,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac.clone(),
        Minute,
        (
            0,
            // 00:04:33.333333
            4 * 60 + 33,
            333_333_300,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac,
        Second,
        (
            0,
            // 00:00:05.555556
            5,
            555_555_555,
        ),
    );
    run_test_parseddatetime_add_field(pdt_frac_neg.clone(), Year, (-18, 0, 0));
    run_test_parseddatetime_add_field(
        pdt_frac_neg.clone(),
        Month,
        (
            -2,
            // -16 days -15:59:59.99856
            -(16 * 60 * 60 * 24 + 15 * 60 * 60 + 59 * 60 + 59),
            -998_560_000,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac_neg.clone(),
        Day,
        (
            0,
            // -2 days 13:19:59.999952
            -(2 * 60 * 60 * 24 + 13 * 60 * 60 + 19 * 60 + 59),
            -999_952_000,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac_neg.clone(),
        Hour,
        (
            0,
            // -03:33:19.999998
            -(3 * 60 * 60 + 33 * 60 + 19),
            -999_998_000,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac_neg.clone(),
        Minute,
        (
            0,
            // -00:04:33.333333
            -(4 * 60 + 33),
            -333_333_300,
        ),
    );
    run_test_parseddatetime_add_field(
        pdt_frac_neg,
        Second,
        (
            0,
            // -00:00:05.555556
            -5,
            -555_555_555,
        ),
    );

    fn run_test_parseddatetime_add_field(
        pdt: ParsedDateTime,
        f: DateTimeField,
        expected: (i64, i64, i64),
    ) {
        let mut res = (0, 0, 0);

        pdt.add_field(f, &mut res.0, &mut res.1, &mut res.2)
            .unwrap();

        if res.0 != expected.0 || res.1 != expected.1 || res.2 != expected.2 {
            panic!(
                "test_parseddatetime_add_field failed \n actual: {:?} \n expected: {:?}",
                res, expected
            );
        }
    }
}

#[test]
fn test_parseddatetime_compute_interval() {
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            year: Some(DateTimeFieldValue::new(1, 0)),
            month: Some(DateTimeFieldValue::new(1, 0)),
            ..Default::default()
        },
        Interval {
            months: 13,
            ..Default::default()
        },
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            year: Some(DateTimeFieldValue::new(1, 0)),
            month: Some(DateTimeFieldValue::new(-1, 0)),
            ..Default::default()
        },
        Interval {
            months: 11,
            ..Default::default()
        },
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            year: Some(DateTimeFieldValue::new(-1, 0)),
            month: Some(DateTimeFieldValue::new(1, 0)),
            ..Default::default()
        },
        Interval {
            months: -11,
            ..Default::default()
        },
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            day: Some(DateTimeFieldValue::new(1, 0)),
            hour: Some(DateTimeFieldValue::new(-2, 0)),
            minute: Some(DateTimeFieldValue::new(-3, 0)),
            second: Some(DateTimeFieldValue::new(-4, -500_000_000)),
            ..Default::default()
        },
        // 21:56:55.5
        Interval {
            duration: Duration::new(21 * 60 * 60 + 56 * 60 + 55, 500_000_000),
            ..Default::default()
        },
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            day: Some(DateTimeFieldValue::new(-1, 0)),
            hour: Some(DateTimeFieldValue::new(2, 0)),
            minute: Some(DateTimeFieldValue::new(3, 0)),
            second: Some(DateTimeFieldValue::new(4, 500_000_000)),
            ..Default::default()
        },
        // -21:56:55.5
        Interval {
            is_positive_dur: false,
            duration: Duration::new(21 * 60 * 60 + 56 * 60 + 55, 500_000_000),
            ..Default::default()
        },
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            day: Some(DateTimeFieldValue::new(1, 0)),
            second: Some(DateTimeFieldValue::new(0, -270_000_000)),
            ..Default::default()
        },
        // 23:59:59.73
        Interval {
            duration: Duration::new(23 * 60 * 60 + 59 * 60 + 59, 730_000_000),
            ..Default::default()
        },
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            day: Some(DateTimeFieldValue::new(-1, 0)),
            second: Some(DateTimeFieldValue::new(0, 270_000_000)),
            ..Default::default()
        },
        // -23:59:59.73
        Interval {
            months: 0,
            is_positive_dur: false,
            duration: Duration::new(23 * 60 * 60 + 59 * 60 + 59, 730_000_000),
        },
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            year: Some(DateTimeFieldValue::new(-1, -555_555_555)),
            month: Some(DateTimeFieldValue::new(2, 555_555_555)),
            day: Some(DateTimeFieldValue::new(-3, -555_555_555)),
            hour: Some(DateTimeFieldValue::new(4, 555_555_555)),
            minute: Some(DateTimeFieldValue::new(-5, -555_555_555)),
            second: Some(DateTimeFieldValue::new(6, 555_555_555)),
            ..Default::default()
        },
        // -1 year -4 months +13 days +07:07:53.220828
        Interval {
            months: -16,
            is_positive_dur: true,
            duration: Duration::new(13 * 60 * 60 * 24 + 7 * 60 * 60 + 7 * 60 + 53, 220_828_255),
        },
    );
    run_test_parseddatetime_compute_interval(
        ParsedDateTime {
            year: Some(DateTimeFieldValue::new(-1, -555_555_555)),
            month: Some(DateTimeFieldValue::new(2, 555_555_555)),
            day: Some(DateTimeFieldValue::new(-3, -555_555_555)),
            hour: Some(DateTimeFieldValue::new(4, 555_555_555)),
            minute: Some(DateTimeFieldValue::new(-5, -555_555_555)),
            second: Some(DateTimeFieldValue::new(6, 555_555_555)),
            ..Default::default()
        },
        // -1 year -4 months +13 days +07:07:53.220828255
        Interval {
            months: -16,
            is_positive_dur: true,
            duration: Duration::new(13 * 60 * 60 * 24 + 7 * 60 * 60 + 7 * 60 + 53, 220_828_255),
        },
    );

    fn run_test_parseddatetime_compute_interval(pdt: ParsedDateTime, expected: Interval) {
        let actual = pdt.compute_interval().unwrap();

        if actual != expected {
            panic!(
                "test_interval_compute_interval failed\n input {:?}\nactual {:?}\nexpected {:?}",
                pdt, actual, expected
            )
        }
    }
}
