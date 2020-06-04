// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A time interval abstract data type.

use std::fmt::{self, Write};

use failure::bail;
use serde::{Deserialize, Serialize};

use crate::adt::datetime::DateTimeField;

/// An interval of time meant to express SQL intervals.
///
/// Obtained by parsing an `INTERVAL '<value>' <unit> [TO <precision>]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Hash, Deserialize)]
pub struct Interval {
    /// A possibly negative number of months for field types like `YEAR`
    pub months: i32,
    /// A timespan represented in nanoseconds.
    ///
    /// Irrespective of values, `duration` will not be carried over into
    /// `months`.
    pub duration: i128,
}

impl Default for Interval {
    fn default() -> Self {
        Self {
            months: 0,
            duration: 0,
        }
    }
}

impl std::ops::Neg for Interval {
    type Output = Self;
    fn neg(self) -> Self {
        Self {
            months: -self.months,
            duration: -self.duration,
        }
    }
}

impl Interval {
    /// Constructs a new `Interval` with the specified units of time.
    ///
    /// `nanos` in excess of `999_999_999` are carried over into seconds.
    pub fn new(months: i32, seconds: i64, nanos: i64) -> Result<Interval, failure::Error> {
        let i = Interval {
            months,
            duration: i128::from(seconds) * 1_000_000_000 + i128::from(nanos),
        };
        // Don't let our duration exceed Postgres' min/max for those same fields,
        // equivalent to:
        // ```
        // SELECT INTERVAL '2147483647 days 2147483647 hours 59 minutes 59.999999 seconds';
        // SELECT INTERVAL '-2147483647 days -2147483647 hours -59 minutes -59.999999 seconds';
        // ```
        if i.duration > 193_273_528_233_599_999_999_000
            || i.duration < -193_273_528_233_599_999_999_000
        {
            bail!(
                "exceeds min/max interval duration +/-(2147483647 days 2147483647 hours \
                59 minutes 59.999999 seconds)"
            )
        } else {
            Ok(i)
        }
    }

    pub fn checked_add(&self, other: &Self) -> Option<Self> {
        let months = match self.months.checked_add(other.months) {
            Some(m) => m,
            None => return None,
        };
        let seconds = match self.dur_as_secs().checked_add(other.dur_as_secs()) {
            Some(s) => s,
            None => return None,
        };

        match Self::new(
            months,
            seconds,
            i64::from(self.nanoseconds() + other.nanoseconds()),
        ) {
            Ok(i) => Some(i),
            Err(_) => None,
        }
    }

    /// Returns the total number of whole seconds in the `Interval`'s duration.
    pub fn dur_as_secs(&self) -> i64 {
        (self.duration / 1_000_000_000) as i64
    }

    /// Computes the year part of the interval.
    ///
    /// The year part is the number of whole years in the interval. For example,
    /// this function returns `3.0` for the interval `3 years 4 months`.
    pub fn years(&self) -> f64 {
        (self.months / 12) as f64
    }

    /// Computes the month part of the interval.
    ///
    /// The whole part is the number of whole months in the interval, modulo 12.
    /// For example, this function returns `4.0` for the interval `3 years 4
    /// months`.
    pub fn months(&self) -> f64 {
        (self.months % 12) as f64
    }

    /// Computes the day part of the interval.
    ///
    /// The day part is the number of whole days in the interval. For example,
    /// this function returns `5.0` for the interval `5 days 4 hours 3 minutes
    /// 2.1 seconds`.
    pub fn days(&self) -> f64 {
        (self.dur_as_secs() / (60 * 60 * 24)) as f64
    }

    /// Computes the hour part of the interval.
    ///
    /// The hour part is the number of whole hours in the interval, modulo 24.
    /// For example, this function returns `4.0` for the interval `5 days 4
    /// hours 3 minutes 2.1 seconds`.
    pub fn hours(&self) -> f64 {
        ((self.dur_as_secs() / (60 * 60)) % 24) as f64
    }

    /// Computes the minute part of the interval.
    ///
    /// The minute part is the number of whole minutes in the interval, modulo
    /// 60. For example, this function returns `3.0` for the interval `5 days 4
    /// hours 3 minutes 2.1 seconds`.
    pub fn minutes(&self) -> f64 {
        ((self.dur_as_secs() / 60) % 60) as f64
    }

    /// Computes the second part of the interval.
    ///
    /// The second part is the number of fractional seconds in the interval,
    /// modulo 60.0.
    pub fn seconds(&self) -> f64 {
        let s = (self.dur_as_secs() % 60) as f64;
        let ns = f64::from(self.nanoseconds()) / 1e9;
        s + ns
    }

    /// Computes the nanosecond part of the interval.
    pub fn nanoseconds(&self) -> i32 {
        (self.duration % 1_000_000_000) as i32
    }

    /// Computes the total number of seconds in the interval.
    pub fn as_seconds(&self) -> f64 {
        (self.months as f64) * 60.0 * 60.0 * 24.0 * 30.0
            + (self.dur_as_secs() as f64)
            + f64::from(self.nanoseconds()) / 1e9
    }

    /// Truncate the "head" of the interval, removing all time units greater than `f`.
    pub fn truncate_high_fields(&mut self, f: DateTimeField) {
        match f {
            DateTimeField::Year => {}
            DateTimeField::Month => self.months %= 12,
            DateTimeField::Day => self.months = 0,
            hms => {
                self.months = 0;
                self.duration %= hms.next_largest().nanos_multiplier() as i128
            }
        }
    }

    /// Converts this `Interval`'s duration into `chrono::Duration`.
    pub fn duration_as_chrono(&self) -> chrono::Duration {
        use chrono::Duration;
        // This can be converted into a single call with
        // https://github.com/chronotope/chrono/pull/426
        Duration::seconds(self.dur_as_secs() as i64)
            + Duration::nanoseconds(self.nanoseconds() as i64)
    }

    /// Truncate the "tail" of the interval, removing all time units less than `f`.
    /// # Arguments
    /// - `f`: Round the interval down to the specified time unit.
    /// - `fsec_max_precision`: If `Some(x)`, keep only `x` places of nanosecond precision.
    ///    Must be `(0,6)`.
    ///
    /// # Errors
    /// - If `fsec_max_precision` is not None or within (0,6).
    pub fn truncate_low_fields(
        &mut self,
        f: DateTimeField,
        fsec_max_precision: Option<u64>,
    ) -> Result<(), failure::Error> {
        use DateTimeField::*;
        match f {
            Year => {
                self.months -= self.months % 12;
                self.duration = 0;
            }
            Month => {
                self.duration = 0;
            }
            // Round nanoseconds.
            Second => {
                let default_precision = 6;
                let precision = match fsec_max_precision {
                    Some(p) => p,
                    None => default_precision,
                };

                if precision > default_precision {
                    bail!(
                        "SECOND precision must be (0, 6), have SECOND({})",
                        precision
                    )
                }

                // Check if value should round up to nearest fractional place.
                let remainder = self.duration % 10_i128.pow(9 - precision as u32);
                if remainder / 10_i128.pow(8 - precision as u32) > 4 {
                    self.duration += 10_i128.pow(9 - precision as u32);
                }

                self.duration -= remainder;
            }
            dhm => {
                self.duration -= self.duration % dhm.nanos_multiplier() as i128;
            }
        }
        Ok(())
    }
}

/// Format an interval in a human form
///
/// Example outputs:
///
/// * 1 year 2 months 5 days 03:04:00
/// * -1 year +5 days +18:59:29.3
/// * 00:00:00
impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut months = self.months;
        let neg_mos = months < 0;
        months = months.abs();
        let years = months / 12;
        months %= 12;
        let mut secs = self.dur_as_secs().abs();
        let mut nanos = self.nanoseconds().abs();
        let days = secs / (24 * 60 * 60);
        secs %= 24 * 60 * 60;
        let hours = secs / (60 * 60);
        secs %= 60 * 60;
        let minutes = secs / 60;
        secs %= 60;

        if years > 0 {
            if neg_mos {
                f.write_char('-')?;
            }
            write!(f, "{} year", years)?;
            if years > 1 {
                f.write_char('s')?;
            }
        }

        if months > 0 {
            if years != 0 {
                f.write_char(' ')?;
            }
            if neg_mos {
                f.write_char('-')?;
            }
            write!(f, "{} month", months)?;
            if months > 1 {
                f.write_char('s')?;
            }
        }

        if days > 0 {
            if years > 0 || months > 0 {
                f.write_char(' ')?;
            }
            if self.duration < 0 {
                f.write_char('-')?;
            } else if neg_mos {
                f.write_char('+')?;
            }
            write!(f, "{} day", days)?;
            if days != 1 {
                f.write_char('s')?;
            }
        }

        let non_zero_hmsn = hours > 0 || minutes > 0 || secs > 0 || nanos > 0;

        if (years == 0 && months == 0 && days == 0) || non_zero_hmsn {
            if years > 0 || months > 0 || days > 0 {
                f.write_char(' ')?;
            }
            if self.duration < 0 && non_zero_hmsn {
                f.write_char('-')?;
            } else if neg_mos {
                f.write_char('+')?;
            }
            write!(f, "{:02}:{:02}:{:02}", hours, minutes, secs)?;
            if nanos > 0 {
                let mut width = 9;
                while nanos % 10 == 0 {
                    width -= 1;
                    nanos /= 10;
                }
                write!(f, ".{:0width$}", nanos, width = width)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn interval_fmt() {
        fn mon(mon: i32) -> String {
            Interval {
                months: mon,
                ..Default::default()
            }
            .to_string()
        }

        assert_eq!(mon(1), "1 month");
        assert_eq!(mon(12), "1 year");
        assert_eq!(mon(13), "1 year 1 month");
        assert_eq!(mon(24), "2 years");
        assert_eq!(mon(25), "2 years 1 month");
        assert_eq!(mon(26), "2 years 2 months");

        fn dur(d: i64) -> String {
            Interval::new(0, d, 0).unwrap().to_string()
        }
        assert_eq!(&dur(86_400 * 2), "2 days");
        assert_eq!(&dur(86_400 * 2 + 3_600 * 3), "2 days 03:00:00");
        assert_eq!(
            &dur(86_400 * 2 + 3_600 * 3 + 60 * 45 + 6),
            "2 days 03:45:06"
        );
        assert_eq!(&dur(86_400 * 2 + 3_600 * 3 + 60 * 45), "2 days 03:45:00");
        assert_eq!(&dur(86_400 * 2 + 6), "2 days 00:00:06");
        assert_eq!(&dur(86_400 * 2 + 60 * 45 + 6), "2 days 00:45:06");
        assert_eq!(&dur(86_400 * 2 + 3_600 * 3 + 6), "2 days 03:00:06");
        assert_eq!(&dur(3_600 * 3 + 60 * 45 + 6), "03:45:06");
        assert_eq!(&dur(3_600 * 3 + 6), "03:00:06");
        assert_eq!(&dur(3_600 * 3), "03:00:00");
        assert_eq!(&dur(60 * 45 + 6), "00:45:06");
        assert_eq!(&dur(60 * 45), "00:45:00");
        assert_eq!(&dur(6), "00:00:06");

        assert_eq!(&dur(-(86_400 * 2 + 6)), "-2 days -00:00:06");
        assert_eq!(&dur(-(86_400 * 2 + 60 * 45 + 6)), "-2 days -00:45:06");
        assert_eq!(&dur(-(86_400 * 2 + 3_600 * 3 + 6)), "-2 days -03:00:06");
        assert_eq!(&dur(-(3_600 * 3 + 60 * 45 + 6)), "-03:45:06");
        assert_eq!(&dur(-(3_600 * 3 + 6)), "-03:00:06");
        assert_eq!(&dur(-(3_600 * 3)), "-03:00:00");
        assert_eq!(&dur(-(60 * 45 + 6)), "-00:45:06");
        assert_eq!(&dur(-(60 * 45)), "-00:45:00");
        assert_eq!(&dur(-6), "-00:00:06");

        fn mon_dur(mon: i32, d: i64) -> String {
            Interval::new(mon, d, 0).unwrap().to_string()
        }
        assert_eq!(&mon_dur(1, 86_400 * 2 + 6), "1 month 2 days 00:00:06");
        assert_eq!(
            &mon_dur(1, 86_400 * 2 + 60 * 45 + 6),
            "1 month 2 days 00:45:06"
        );
        assert_eq!(
            &mon_dur(1, 86_400 * 2 + 3_600 * 3 + 6),
            "1 month 2 days 03:00:06"
        );
        assert_eq!(
            &mon_dur(26, 3_600 * 3 + 60 * 45 + 6),
            "2 years 2 months 03:45:06"
        );
        assert_eq!(&mon_dur(26, 3_600 * 3 + 6), "2 years 2 months 03:00:06");
        assert_eq!(&mon_dur(26, 3_600 * 3), "2 years 2 months 03:00:00");
        assert_eq!(&mon_dur(26, 60 * 45 + 6), "2 years 2 months 00:45:06");
        assert_eq!(&mon_dur(26, 60 * 45), "2 years 2 months 00:45:00");
        assert_eq!(&mon_dur(26, 6), "2 years 2 months 00:00:06");

        assert_eq!(
            &mon_dur(26, -(86_400 * 2 + 6)),
            "2 years 2 months -2 days -00:00:06"
        );
        assert_eq!(
            &mon_dur(26, -(86_400 * 2 + 60 * 45 + 6)),
            "2 years 2 months -2 days -00:45:06"
        );
        assert_eq!(
            &mon_dur(26, -(86_400 * 2 + 3_600 * 3 + 6)),
            "2 years 2 months -2 days -03:00:06"
        );
        assert_eq!(
            &mon_dur(26, -(3_600 * 3 + 60 * 45 + 6)),
            "2 years 2 months -03:45:06"
        );
        assert_eq!(&mon_dur(26, -(3_600 * 3 + 6)), "2 years 2 months -03:00:06");
        assert_eq!(&mon_dur(26, -(3_600 * 3)), "2 years 2 months -03:00:00");
        assert_eq!(&mon_dur(26, -(60 * 45 + 6)), "2 years 2 months -00:45:06");
        assert_eq!(&mon_dur(26, -(60 * 45)), "2 years 2 months -00:45:00");
        assert_eq!(&mon_dur(26, -6), "2 years 2 months -00:00:06");

        assert_eq!(&mon_dur(-1, 86_400 * 2 + 6), "-1 month +2 days +00:00:06");
        assert_eq!(
            &mon_dur(-1, 86_400 * 2 + 60 * 45 + 6),
            "-1 month +2 days +00:45:06"
        );
        assert_eq!(
            &mon_dur(-1, 86_400 * 2 + 3_600 * 3 + 6),
            "-1 month +2 days +03:00:06"
        );
        assert_eq!(
            &mon_dur(-26, 3_600 * 3 + 60 * 45 + 6),
            "-2 years -2 months +03:45:06"
        );
        assert_eq!(&mon_dur(-26, 3_600 * 3 + 6), "-2 years -2 months +03:00:06");
        assert_eq!(&mon_dur(-26, 3_600 * 3), "-2 years -2 months +03:00:00");
        assert_eq!(&mon_dur(-26, 60 * 45 + 6), "-2 years -2 months +00:45:06");
        assert_eq!(&mon_dur(-26, 60 * 45), "-2 years -2 months +00:45:00");
        assert_eq!(&mon_dur(-26, 6), "-2 years -2 months +00:00:06");

        assert_eq!(
            &mon_dur(-26, -(86_400 * 2 + 6)),
            "-2 years -2 months -2 days -00:00:06"
        );
        assert_eq!(
            &mon_dur(-26, -(86_400 * 2 + 60 * 45 + 6)),
            "-2 years -2 months -2 days -00:45:06"
        );
        assert_eq!(
            &mon_dur(-26, -(86_400 * 2 + 3_600 * 3 + 6)),
            "-2 years -2 months -2 days -03:00:06"
        );
        assert_eq!(
            &mon_dur(-26, -(3_600 * 3 + 60 * 45 + 6)),
            "-2 years -2 months -03:45:06"
        );
        assert_eq!(
            &mon_dur(-26, -(3_600 * 3 + 6)),
            "-2 years -2 months -03:00:06"
        );
        assert_eq!(&mon_dur(-26, -(3_600 * 3)), "-2 years -2 months -03:00:00");
        assert_eq!(
            &mon_dur(-26, -(60 * 45 + 6)),
            "-2 years -2 months -00:45:06"
        );
        assert_eq!(&mon_dur(-26, -(60 * 45)), "-2 years -2 months -00:45:00");
        assert_eq!(&mon_dur(-26, -6), "-2 years -2 months -00:00:06");
    }

    #[test]
    fn test_interval_value_truncate_low_fields() {
        use DateTimeField::*;

        let mut test_cases = [
            (Year, None, (321, 654_321, 321_000_000), (26 * 12, 0, 0)),
            (Month, None, (321, 654_321, 321_000_000), (321, 0, 0)),
            (
                Day,
                None,
                (321, 654_321, 321_000_000),
                (321, 7 * 60 * 60 * 24, 0), // months: 321, duration: 604800s, is_positive_dur: true
            ),
            (
                Hour,
                None,
                (321, 654_321, 321_000_000),
                (321, 181 * 60 * 60, 0),
            ),
            (
                Minute,
                None,
                (321, 654_321, 321_000_000),
                (321, 10905 * 60, 0),
            ),
            (
                Second,
                None,
                (321, 654_321, 321_000_000),
                (321, 654_321, 321_000_000),
            ),
            (
                Second,
                Some(1),
                (321, 654_321, 321_000_000),
                (321, 654_321, 300_000_000),
            ),
            (
                Second,
                Some(0),
                (321, 654_321, 321_000_000),
                (321, 654_321, 0),
            ),
        ];

        for test in test_cases.iter_mut() {
            let mut i = Interval::new((test.2).0, (test.2).1, (test.2).2).unwrap();
            let j = Interval::new((test.3).0, (test.3).1, (test.3).2).unwrap();

            i.truncate_low_fields(test.0, test.1).unwrap();

            if i != j {
                panic!(
                "test_interval_value_truncate_low_fields failed on {} \n actual: {:?} \n expected: {:?}",
                test.0, i, j
            );
            }
        }
    }

    #[test]
    fn test_interval_value_truncate_high_fields() {
        use DateTimeField::*;

        let mut test_cases = [
            (Year, (321, 654_321), (321, 654_321)),
            (Month, (321, 654_321), (9, 654_321)),
            (Day, (321, 654_321), (0, 654_321)),
            (Hour, (321, 654_321), (0, 654_321 % (60 * 60 * 24))),
            (Minute, (321, 654_321), (0, 654_321 % (60 * 60))),
            (Second, (321, 654_321), (0, 654_321 % 60)),
        ];

        for test in test_cases.iter_mut() {
            let mut i = Interval::new((test.1).0, (test.1).1, 123).unwrap();
            let j = Interval::new((test.2).0, (test.2).1, 123).unwrap();

            i.truncate_high_fields(test.0);

            if i != j {
                panic!(
                    "test_interval_value_truncate_high_fields failed on {} \n actual: {:?} \n expected: {:?}",
                    test.0, i, j
                );
            }
        }
    }
}
