// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A time interval abstract data type.

use std::fmt::{self, Write};
use std::time::Duration;

use anyhow::bail;
use serde::{Deserialize, Serialize};

use crate::adt::datetime::DateTimeField;
use crate::adt::numeric::DecimalLike;

/// An interval of time meant to express SQL intervals.
///
/// Obtained by parsing an `INTERVAL '<value>' <unit> [TO <precision>]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Hash, Deserialize)]
pub struct Interval {
    /// A possibly negative number of months for field types like `YEAR`
    pub months: i32,
    /// A possibly negative number of days.
    ///
    /// Irrespective of values, `days` will not be carried over into `months`.
    pub days: i32,
    /// A timespan represented in microseconds.
    ///
    /// Irrespective of values, `micros` will not be carried over into `days` or
    /// `months`.
    pub micros: i64,
}

impl Default for Interval {
    fn default() -> Self {
        Self {
            months: 0,
            days: 0,
            micros: 0,
        }
    }
}

impl std::ops::Neg for Interval {
    type Output = Self;
    fn neg(self) -> Self {
        Self {
            months: -self.months,
            days: -self.days,
            micros: -self.micros,
        }
    }
}

impl Interval {
    // Don't let our duration exceed Postgres' min/max for those same fields,
    // equivalent to:
    // ```
    // SELECT INTERVAL '2147483647 hours 59 minutes 59.999999 seconds';
    // SELECT INTERVAL '-2147483648 hours -59 minutes -59.999999 seconds';
    // ```
    const MAX_MICROSECONDS: i64 = (((i32::MAX as i64 * 60) + 59) * 60) * 1_000_000 + 59_999_999;
    const MIN_MICROSECONDS: i64 = (((i32::MIN as i64 * 60) - 59) * 60) * 1_000_000 - 59_999_999;

    /// Constructs a new `Interval` with the specified units of time.
    pub fn new(months: i32, days: i32, micros: i64) -> Result<Interval, anyhow::Error> {
        let i = Interval {
            months,
            days,
            micros,
        };
        if i.micros > Self::MAX_MICROSECONDS || i.micros < Self::MIN_MICROSECONDS {
            bail!(format!(
                "exceeds min/max interval duration ({} hours 59 minutes 59.999999 seconds/\
                {} hours -59 minutes -59.999999 seconds)",
                i32::MAX,
                i32::MIN,
            ))
        } else {
            Ok(i)
        }
    }

    pub fn checked_add(&self, other: &Self) -> Option<Self> {
        let months = match self.months.checked_add(other.months) {
            Some(m) => m,
            None => return None,
        };
        let days = match self.days.checked_add(other.days) {
            Some(d) => d,
            None => return None,
        };
        let micros = match self.micros.checked_add(other.micros) {
            Some(us) => us,
            None => return None,
        };

        match Self::new(months, days, micros) {
            Ok(i) => Some(i),
            Err(_) => None,
        }
    }

    pub fn checked_mul(&self, other: f64) -> Option<Self> {
        let months = self.months as f64 * other;
        if months.is_nan() || months < i32::MIN as f64 || months > i32::MAX as f64 {
            return None;
        }

        let days = self.days as f64 * other + months.fract() * 30.0;
        if days.is_nan() || days < i32::MIN as f64 || days > i32::MAX as f64 {
            return None;
        }

        let micros = self.micros as f64 * other + days.fract() * 1e6 * 60.0 * 60.0 * 24.0;
        if micros.is_nan() || micros < i64::MIN as f64 || days > i64::MAX as f64 {
            return None;
        }

        Self::new(months as i32, days as i32, micros as i64).ok()
    }

    pub fn checked_div(&self, other: f64) -> Option<Self> {
        let months = self.months as f64 / other;
        if months.is_nan() || months < i32::MIN as f64 || months > i32::MAX as f64 {
            return None;
        }

        let days = self.days as f64 / other + months.fract() * 30.0;
        if days.is_nan() || days < i32::MIN as f64 || days > i32::MAX as f64 {
            return None;
        }

        let micros = self.micros as f64 / other + days.fract() * 1e6 * 60.0 * 60.0 * 24.0;
        if micros.is_nan() || micros < i64::MIN as f64 || days > i64::MAX as f64 {
            return None;
        }

        Self::new(months as i32, days as i32, micros as i64).ok()
    }

    /// Returns the total number of whole seconds in the `Interval`'s duration.
    pub fn dur_as_secs(&self) -> i64 {
        self.micros / 1_000_000
    }

    /// Returns the total number of microseconds in the `Interval`'s duration.
    pub fn dur_as_microsecs(&self) -> i64 {
        self.micros
    }

    /// Computes the millennium part of the interval.
    ///
    /// The millennium part is the number of whole millennia in the interval. For example,
    /// this function returns `3` for the interval `3400 years`.
    pub fn millennia(&self) -> i32 {
        self.months / 12_000
    }

    /// Computes the century part of the interval.
    ///
    /// The century part is the number of whole centuries in the interval. For example,
    /// this function returns `3` for the interval `340 years`.
    pub fn centuries(&self) -> i32 {
        self.months / 1_200
    }

    /// Computes the decade part of the interval.
    ///
    /// The decade part is the number of whole decades in the interval. For example,
    /// this function returns `3` for the interval `34 years`.
    pub fn decades(&self) -> i32 {
        self.months / 120
    }

    /// Computes the year part of the interval.
    ///
    /// The year part is the number of whole years in the interval. For example,
    /// this function returns `3` for the interval `3 years 4 months`.
    pub fn years(&self) -> i32 {
        self.months / 12
    }

    /// Computes the quarter part of the interval.
    ///
    /// The quarter part is obtained from taking the number of whole months modulo 12,
    /// and assigning quarter #1 for months 0-2, #2 for 3-5, #3 for 6-8 and #4 for 9-11.
    /// For example, this function returns `4` for the interval `11 months`.
    pub fn quarters(&self) -> i32 {
        (self.months % 12) / 3 + 1
    }

    /// Computes the month part of the interval.
    ///
    /// The month part is the number of whole months in the interval, modulo 12.
    /// For example, this function returns `4` for the interval `3 years 4
    /// months`.
    pub fn months(&self) -> i32 {
        self.months % 12
    }

    /// Computes the day part of the interval.
    ///
    /// The day part is the number of whole days in the interval. For example,
    /// this function returns `5` for the interval `5 days 4 hours 3 minutes
    /// 2.1 seconds`.
    pub fn days(&self) -> i64 {
        self.days.into()
    }

    /// Computes the hour part of the interval.
    ///
    /// The hour part is the number of whole hours in the interval, modulo 24.
    /// For example, this function returns `4` for the interval `5 days 4
    /// hours 3 minutes 2.1 seconds`.
    pub fn hours(&self) -> i64 {
        (self.dur_as_secs() / (60 * 60)) % 24
    }

    /// Computes the minute part of the interval.
    ///
    /// The minute part is the number of whole minutes in the interval, modulo
    /// 60. For example, this function returns `3` for the interval `5 days 4
    /// hours 3 minutes 2.1 seconds`.
    pub fn minutes(&self) -> i64 {
        (self.dur_as_secs() / 60) % 60
    }

    /// Computes the second part of the interval.
    ///
    /// The second part is the number of fractional seconds in the interval,
    /// modulo 60.0.
    pub fn seconds<T>(&self) -> T
    where
        T: DecimalLike,
    {
        T::lossy_from(self.micros % 60_000_000) / T::from(1e6)
    }

    /// Computes the second part of the interval displayed in milliseconds.
    ///
    /// The second part is the number of fractional seconds in the interval,
    /// modulo 60.0.
    pub fn milliseconds<T>(&self) -> T
    where
        T: DecimalLike,
    {
        T::lossy_from(self.micros % 60_000_000) / T::from(1e3)
    }

    /// Computes the second part of the interval displayed in microseconds.
    ///
    /// The second part is the number of fractional seconds in the interval,
    /// modulo 60.0.
    pub fn microseconds<T>(&self) -> T
    where
        T: DecimalLike,
    {
        T::lossy_from(self.micros % 60_000_000)
    }

    /// Computes the nanosecond part of the interval.
    pub fn nanoseconds(&self) -> i32 {
        (self.micros % 1_000_000 * 1_000).try_into().unwrap()
    }

    /// Computes the total number of seconds in the interval.
    pub fn as_seconds<T>(&self) -> T
    where
        T: DecimalLike,
    {
        T::from(self.years() * 60 * 60 * 24) * T::from(365.25)
            + T::from(self.months() * 60 * 60 * 24 * 30)
            + T::from(self.days * 60 * 60 * 24)
            + T::lossy_from(self.dur_as_secs())
            + T::from(self.nanoseconds()) / T::from(1e9)
    }

    /// Truncate the "head" of the interval, removing all time units greater than `f`.
    pub fn truncate_high_fields(&mut self, f: DateTimeField) {
        match f {
            DateTimeField::Year => {}
            DateTimeField::Month => self.months %= 12,
            DateTimeField::Day => self.months = 0,
            DateTimeField::Hour | DateTimeField::Minute | DateTimeField::Second => {
                self.months = 0;
                self.days = 0;
                self.micros %= f.next_largest().micros_multiplier()
            }
            DateTimeField::Millennium
            | DateTimeField::Century
            | DateTimeField::Decade
            | DateTimeField::Milliseconds
            | DateTimeField::Microseconds => {
                unreachable!(format!("Cannot truncate interval by {}", f))
            }
        }
    }

    /// Converts this `Interval`'s duration into `chrono::Duration`.
    pub fn duration_as_chrono(&self) -> chrono::Duration {
        use chrono::Duration;
        Duration::days(self.days.into()) + Duration::microseconds(self.micros)
    }

    pub fn duration(&self) -> Result<Duration, anyhow::Error> {
        if self.months != 0 {
            bail!("cannot convert interval with months to duration");
        }
        if self.duration_as_chrono() < chrono::Duration::zero() {
            bail!("cannot convert negative interval to duration");
        }
        Ok(self.duration_as_chrono().to_std()?)
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
    ) -> Result<(), anyhow::Error> {
        use DateTimeField::*;
        match f {
            Year => {
                self.months -= self.months % 12;
                self.days = 0;
                self.micros = 0;
            }
            Month => {
                self.days = 0;
                self.micros = 0;
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
                let precision = match u32::try_from(precision) {
                    Ok(p) => p,
                    Err(_) => bail!(
                        "SECOND precision must be (0, 6), have SECOND({})",
                        precision
                    ),
                };
                let remainder = self.micros % 10_i64.pow(6 - precision);
                if u64::from(precision) != default_precision
                    && remainder / 10_i64.pow(5 - precision) > 4
                {
                    self.micros += 10_i64.pow(6 - precision);
                }
                self.micros -= remainder;
            }
            Day => {
                self.micros = 0;
            }
            Hour | Minute => {
                self.micros -= self.micros % f.micros_multiplier();
            }
            Millennium | Century | Decade | Milliseconds | Microseconds => {
                unreachable!(format!("Cannot truncate interval by {}", f))
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
        let neg_months = self.months < 0;
        let years = (self.months / 12).abs();
        let months = (self.months % 12).abs();

        let neg_days = self.days < 0;
        let days = i64::from(self.days).abs();

        let mut nanos = self.nanoseconds().abs();
        let mut secs = (self.micros / 1_000_000).abs();

        let sec_per_hr = 60 * 60;
        let hours = secs / sec_per_hr;
        secs %= sec_per_hr;

        let sec_per_min = 60;
        let minutes = secs / sec_per_min;
        secs %= sec_per_min;

        if years > 0 {
            if neg_months {
                f.write_char('-')?;
            }
            write!(f, "{} year", years)?;
            if years > 1 || neg_months {
                f.write_char('s')?;
            }
        }

        if months > 0 {
            if years != 0 {
                f.write_char(' ')?;
            }
            if neg_months {
                f.write_char('-')?;
            }
            write!(f, "{} month", months)?;
            if months > 1 || neg_months {
                f.write_char('s')?;
            }
        }

        if days != 0 {
            if years > 0 || months > 0 {
                f.write_char(' ')?;
            }
            if neg_months && !neg_days {
                f.write_char('+')?;
            }
            write!(f, "{} day", self.days)?;
            if self.days != 1 {
                f.write_char('s')?;
            }
        }

        let non_zero_hmsn = hours > 0 || minutes > 0 || secs > 0 || nanos > 0;

        if (years == 0 && months == 0 && days == 0) || non_zero_hmsn {
            if years > 0 || months > 0 || days > 0 {
                f.write_char(' ')?;
            }
            if self.micros < 0 && non_zero_hmsn {
                f.write_char('-')?;
            } else if neg_days || (days == 0 && neg_months) {
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

        fn dur(days: i32, micros: i64) -> String {
            Interval::new(0, days, micros).unwrap().to_string()
        }
        assert_eq!(&dur(2, 0), "2 days");
        assert_eq!(&dur(2, 3 * 60 * 60 * 1_000_000), "2 days 03:00:00");
        assert_eq!(
            &dur(
                2,
                (3 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (6 * 1_000_000)
            ),
            "2 days 03:45:06"
        );
        assert_eq!(
            &dur(2, (3 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000)),
            "2 days 03:45:00"
        );
        assert_eq!(&dur(2, 6 * 1_000_000), "2 days 00:00:06");
        assert_eq!(
            &dur(2, (45 * 60 * 1_000_000) + (6 * 1_000_000)),
            "2 days 00:45:06"
        );
        assert_eq!(
            &dur(2, (3 * 60 * 60 * 1_000_000) + (6 * 1_000_000)),
            "2 days 03:00:06"
        );
        assert_eq!(
            &dur(
                0,
                (3 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (6 * 1_000_000)
            ),
            "03:45:06"
        );
        assert_eq!(
            &dur(0, (3 * 60 * 60 * 1_000_000) + (6 * 1_000_000)),
            "03:00:06"
        );
        assert_eq!(&dur(0, 3 * 60 * 60 * 1_000_000), "03:00:00");
        assert_eq!(&dur(0, (45 * 60 * 1_000_000) + (6 * 1_000_000)), "00:45:06");
        assert_eq!(&dur(0, 45 * 60 * 1_000_000), "00:45:00");
        assert_eq!(&dur(0, 6 * 1_000_000), "00:00:06");

        assert_eq!(&dur(-2, -6 * 1_000_000), "-2 days -00:00:06");
        assert_eq!(
            &dur(-2, (-45 * 60 * 1_000_000) + (-6 * 1_000_000)),
            "-2 days -00:45:06"
        );
        assert_eq!(
            &dur(-2, (-3 * 60 * 60 * 1_000_000) + (-6 * 1_000_000)),
            "-2 days -03:00:06"
        );
        assert_eq!(
            &dur(
                0,
                (-3 * 60 * 60 * 1_000_000) + (-45 * 60 * 1_000_000) + (-6 * 1_000_000)
            ),
            "-03:45:06"
        );
        assert_eq!(
            &dur(0, (-3 * 60 * 60 * 1_000_000) + (-6 * 1_000_000)),
            "-03:00:06"
        );
        assert_eq!(&dur(0, -3 * 60 * 60 * 1_000_000), "-03:00:00");
        assert_eq!(
            &dur(0, (-45 * 60 * 1_000_000) + (-6 * 1_000_000)),
            "-00:45:06"
        );
        assert_eq!(&dur(0, -45 * 60 * 1_000_000), "-00:45:00");
        assert_eq!(&dur(0, -6 * 1_000_000), "-00:00:06");

        fn mon_dur(mon: i32, days: i32, micros: i64) -> String {
            Interval::new(mon, days, micros).unwrap().to_string()
        }
        assert_eq!(&mon_dur(1, 2, 6 * 1_000_000), "1 month 2 days 00:00:06");
        assert_eq!(
            &mon_dur(1, 2, (45 * 60 * 1_000_000) + (6 * 1_000_000)),
            "1 month 2 days 00:45:06"
        );
        assert_eq!(
            &mon_dur(1, 2, (3 * 60 * 60 * 1_000_000) + (6 * 1_000_000)),
            "1 month 2 days 03:00:06"
        );
        assert_eq!(
            &mon_dur(
                26,
                0,
                (3 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (6 * 1_000_000)
            ),
            "2 years 2 months 03:45:06"
        );
        assert_eq!(
            &mon_dur(26, 0, (3 * 60 * 60 * 1_000_000) + (6 * 1_000_000)),
            "2 years 2 months 03:00:06"
        );
        assert_eq!(
            &mon_dur(26, 0, 3 * 60 * 60 * 1_000_000),
            "2 years 2 months 03:00:00"
        );
        assert_eq!(
            &mon_dur(26, 0, (45 * 60 * 1_000_000) + (6 * 1_000_000)),
            "2 years 2 months 00:45:06"
        );
        assert_eq!(
            &mon_dur(26, 0, 45 * 60 * 1_000_000),
            "2 years 2 months 00:45:00"
        );
        assert_eq!(&mon_dur(26, 0, 6 * 1_000_000), "2 years 2 months 00:00:06");

        assert_eq!(
            &mon_dur(26, -2, -6 * 1_000_000),
            "2 years 2 months -2 days -00:00:06"
        );
        assert_eq!(
            &mon_dur(26, -2, (-45 * 60 * 1_000_000) + (-6 * 1_000_000)),
            "2 years 2 months -2 days -00:45:06"
        );
        assert_eq!(
            &mon_dur(26, -2, (-3 * 60 * 60 * 1_000_000) + (-6 * 1_000_000)),
            "2 years 2 months -2 days -03:00:06"
        );
        assert_eq!(
            &mon_dur(
                26,
                0,
                (-3 * 60 * 60 * 1_000_000) + (-45 * 60 * 1_000_000) + (-6 * 1_000_000)
            ),
            "2 years 2 months -03:45:06"
        );
        assert_eq!(
            &mon_dur(26, 0, (-3 * 60 * 60 * 1_000_000) + (-6 * 1_000_000)),
            "2 years 2 months -03:00:06"
        );
        assert_eq!(
            &mon_dur(26, 0, -3 * 60 * 60 * 1_000_000),
            "2 years 2 months -03:00:00"
        );
        assert_eq!(
            &mon_dur(26, 0, (-45 * 60 * 1_000_000) + (-6 * 1_000_000)),
            "2 years 2 months -00:45:06"
        );
        assert_eq!(
            &mon_dur(26, 0, -45 * 60 * 1_000_000),
            "2 years 2 months -00:45:00"
        );
        assert_eq!(
            &mon_dur(26, 0, -6 * 1_000_000),
            "2 years 2 months -00:00:06"
        );

        assert_eq!(&mon_dur(-1, 2, 6 * 1_000_000), "-1 months +2 days 00:00:06");
        assert_eq!(
            &mon_dur(-1, 2, (45 * 60 * 1_000_000) + (6 * 1_000_000)),
            "-1 months +2 days 00:45:06"
        );
        assert_eq!(
            &mon_dur(-1, 2, (3 * 60 * 60 * 1_000_000) + (6 * 1_000_000)),
            "-1 months +2 days 03:00:06"
        );
        assert_eq!(
            &mon_dur(
                -26,
                0,
                (3 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (6 * 1_000_000)
            ),
            "-2 years -2 months +03:45:06"
        );
        assert_eq!(
            &mon_dur(-26, 0, (3 * 60 * 60 * 1_000_000) + (6 * 1_000_000)),
            "-2 years -2 months +03:00:06"
        );
        assert_eq!(
            &mon_dur(-26, 0, 3 * 60 * 60 * 1_000_000),
            "-2 years -2 months +03:00:00"
        );
        assert_eq!(
            &mon_dur(-26, 0, (45 * 60 * 1_000_000) + (6 * 1_000_000)),
            "-2 years -2 months +00:45:06"
        );
        assert_eq!(
            &mon_dur(-26, 0, 45 * 60 * 1_000_000),
            "-2 years -2 months +00:45:00"
        );
        assert_eq!(
            &mon_dur(-26, 0, 6 * 1_000_000),
            "-2 years -2 months +00:00:06"
        );

        assert_eq!(
            &mon_dur(-26, -2, -6 * 1_000_000),
            "-2 years -2 months -2 days -00:00:06"
        );
        assert_eq!(
            &mon_dur(-26, -2, (-45 * 60 * 1_000_000) + (-6 * 1_000_000)),
            "-2 years -2 months -2 days -00:45:06"
        );
        assert_eq!(
            &mon_dur(-26, -2, (-3 * 60 * 60 * 1_000_000) + (-6 * 1_000_000)),
            "-2 years -2 months -2 days -03:00:06"
        );
        assert_eq!(
            &mon_dur(
                -26,
                0,
                (-3 * 60 * 60 * 1_000_000) + (-45 * 60 * 1_000_000) + (-6 * 1_000_000)
            ),
            "-2 years -2 months -03:45:06"
        );
        assert_eq!(
            &mon_dur(-26, 0, (-3 * 60 * 60 * 1_000_000) + (-6 * 1_000_000)),
            "-2 years -2 months -03:00:06"
        );
        assert_eq!(
            &mon_dur(-26, 0, -3 * 60 * 60 * 1_000_000),
            "-2 years -2 months -03:00:00"
        );
        assert_eq!(
            &mon_dur(-26, 0, (-45 * 60 * 1_000_000) + (-6 * 1_000_000)),
            "-2 years -2 months -00:45:06"
        );
        assert_eq!(
            &mon_dur(-26, 0, -45 * 60 * 1_000_000),
            "-2 years -2 months -00:45:00"
        );
        assert_eq!(
            &mon_dur(-26, 0, -6 * 1_000_000),
            "-2 years -2 months -00:00:06"
        );
    }

    #[test]
    fn test_interval_value_truncate_low_fields() {
        use DateTimeField::*;

        let mut test_cases = [
            (
                Year,
                None,
                (
                    321,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000) + 321_000,
                ),
                (26 * 12, 0, 0),
            ),
            (
                Month,
                None,
                (
                    321,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000) + 321_000,
                ),
                (321, 0, 0),
            ),
            (
                Day,
                None,
                (
                    321,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000) + 321_000,
                ),
                (321, 7, 0),
            ),
            (
                Hour,
                None,
                (
                    321,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000) + 321_000,
                ),
                (321, 7, 13 * 60 * 60 * 1_000_000),
            ),
            (
                Minute,
                None,
                (
                    321,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000) + 321_000,
                ),
                (321, 7, (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000)),
            ),
            (
                Second,
                None,
                (
                    321,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000) + 321_000,
                ),
                (
                    321,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000) + 321_000,
                ),
            ),
            (
                Second,
                Some(1),
                (
                    321,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000) + 321_000,
                ),
                (
                    321,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000) + 300_000,
                ),
            ),
            (
                Second,
                Some(0),
                (
                    321,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000) + 321_000,
                ),
                (
                    321,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000),
                ),
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

        // (month, day, microsecond) tuples
        let mut test_cases = [
            (
                Year,
                (
                    321,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000),
                ),
                (
                    321,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000),
                ),
            ),
            (
                Month,
                (
                    321,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000),
                ),
                (
                    9,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000),
                ),
            ),
            (
                Day,
                (
                    321,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000),
                ),
                (
                    0,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000),
                ),
            ),
            (
                Hour,
                (
                    321,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000),
                ),
                (
                    0,
                    0,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000),
                ),
            ),
            (
                Minute,
                (
                    321,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000),
                ),
                (0, 0, (45 * 60 * 1_000_000) + (21 * 1_000_000)),
            ),
            (
                Second,
                (
                    321,
                    7,
                    (13 * 60 * 60 * 1_000_000) + (45 * 60 * 1_000_000) + (21 * 1_000_000),
                ),
                (0, 0, 21 * 1_000_000),
            ),
        ];

        for test in test_cases.iter_mut() {
            let mut i = Interval::new((test.1).0, (test.1).1, (test.1).2).unwrap();
            let j = Interval::new((test.2).0, (test.2).1, (test.2).2).unwrap();

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
