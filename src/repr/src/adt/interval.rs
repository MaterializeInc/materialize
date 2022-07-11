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

use anyhow::{anyhow, bail};
use mz_proto::{RustType, TryFromProtoError};
use num_traits::CheckedMul;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::adt::datetime::DateTimeField;
use crate::adt::numeric::{DecimalLike, Numeric};

include!(concat!(env!("OUT_DIR"), "/mz_repr.adt.interval.rs"));

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

impl RustType<ProtoInterval> for Interval {
    fn into_proto(&self) -> ProtoInterval {
        ProtoInterval {
            months: self.months,
            days: self.days,
            micros: self.micros,
        }
    }

    fn from_proto(proto: ProtoInterval) -> Result<Self, TryFromProtoError> {
        Ok(Interval {
            months: proto.months,
            days: proto.days,
            micros: proto.micros,
        })
    }
}

impl num_traits::ops::checked::CheckedNeg for Interval {
    fn checked_neg(&self) -> Option<Self> {
        if let (Some(months), Some(days), Some(micros)) = (
            self.months.checked_neg(),
            self.days.checked_neg(),
            self.micros.checked_neg(),
        ) {
            Some(Self::new(months, days, micros))
        } else {
            None
        }
    }
}

impl std::str::FromStr for Interval {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        crate::strconv::parse_interval(s).map_err(|e| anyhow!(e))
    }
}

static MONTH_OVERFLOW_ERROR: Lazy<String> = Lazy::new(|| {
    format!(
        "Overflows maximum months; cannot exceed {}/{} microseconds",
        i32::MAX,
        i32::MIN,
    )
});
static DAY_OVERFLOW_ERROR: Lazy<String> = Lazy::new(|| {
    format!(
        "Overflows maximum days; cannot exceed {}/{} microseconds",
        i32::MAX,
        i32::MIN,
    )
});
static USECS_PER_DAY: Lazy<i64> = Lazy::new(|| {
    Interval::convert_date_time_unit(DateTimeField::Day, DateTimeField::Microseconds, 1i64).unwrap()
});

impl Interval {
    pub const CENTURY_PER_MILLENNIUM: u16 = 10;
    pub const DECADE_PER_CENTURY: u16 = 10;
    pub const YEAR_PER_DECADE: u16 = 10;
    pub const MONTH_PER_YEAR: u16 = 12;
    // Interval type considers 30 days == 1 month
    pub const DAY_PER_MONTH: u16 = 30;
    // Interval type considers 24 hours == 1 day
    pub const HOUR_PER_DAY: u16 = 24;
    pub const MINUTE_PER_HOUR: u16 = 60;
    pub const SECOND_PER_MINUTE: u16 = 60;
    pub const MILLISECOND_PER_SECOND: u16 = 1_000;
    pub const MICROSECOND_PER_MILLISECOND: u16 = 1_000;
    pub const NANOSECOND_PER_MICROSECOND: u16 = 1_000;
    // PostgreSQL actually has a bug where when using EXTRACT it truncates this value to 365, but
    // when using date_part it does not truncate this value. Therefore our EXTRACT function may differ
    // from PostgreSQL.
    // EXTRACT: https://github.com/postgres/postgres/blob/c2e8bd27519f47ff56987b30eb34a01969b9a9e8/src/backend/utils/adt/timestamp.c#L5270-L5273
    // date_part: https://github.com/postgres/postgres/blob/c2e8bd27519f47ff56987b30eb34a01969b9a9e8/src/backend/utils/adt/timestamp.c#L5301
    pub const EPOCH_DAYS_PER_YEAR: f64 = 365.25;

    /// Constructs a new `Interval` with the specified units of time.
    pub fn new(months: i32, days: i32, micros: i64) -> Interval {
        Interval {
            months,
            days,
            micros,
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

        Some(Self::new(months, days, micros))
    }

    pub fn checked_mul(&self, other: f64) -> Option<Self> {
        self.checked_op(other, |f1, f2| f1 * f2)
    }

    pub fn checked_div(&self, other: f64) -> Option<Self> {
        self.checked_op(other, |f1, f2| f1 / f2)
    }

    fn checked_op<F1>(&self, other: f64, op: F1) -> Option<Self>
    where
        F1: Fn(f64, f64) -> f64,
    {
        let months = op(f64::from(self.months), other);
        if months.is_nan()
            || months.is_infinite()
            || months < i32::MIN.into()
            || months > i32::MAX.into()
        {
            return None;
        }

        let days =
            op(f64::from(self.days), other) + months.fract() * f64::from(Self::DAY_PER_MONTH);
        if days.is_nan() || days.is_infinite() || days < i32::MIN.into() || days > i32::MAX.into() {
            return None;
        }

        let micros = op(self.micros as f64, other)
            + days.fract()
                * f64::from(Self::HOUR_PER_DAY)
                * f64::from(Self::MINUTE_PER_HOUR)
                * f64::from(Self::SECOND_PER_MINUTE)
                * f64::from(Self::MILLISECOND_PER_SECOND)
                * f64::from(Self::MICROSECOND_PER_MILLISECOND);

        if micros.is_nan()
            || micros.is_infinite()
            || Numeric::from(micros) < Numeric::from(i64::MIN)
            || Numeric::from(micros) > Numeric::from(i64::MAX)
        {
            return None;
        }

        Some(Self::new(months as i32, days as i32, micros as i64))
    }

    /// Computes the millennium part of the interval.
    ///
    /// The millennium part is the number of whole millennia in the interval. For example,
    /// this function returns `3` for the interval `3400 years`.
    pub fn millennia(&self) -> i32 {
        Self::convert_date_time_unit(DateTimeField::Month, DateTimeField::Millennium, self.months)
            .unwrap()
    }

    /// Computes the century part of the interval.
    ///
    /// The century part is the number of whole centuries in the interval. For example,
    /// this function returns `3` for the interval `340 years`.
    pub fn centuries(&self) -> i32 {
        Self::convert_date_time_unit(DateTimeField::Month, DateTimeField::Century, self.months)
            .unwrap()
    }

    /// Computes the decade part of the interval.
    ///
    /// The decade part is the number of whole decades in the interval. For example,
    /// this function returns `3` for the interval `34 years`.
    pub fn decades(&self) -> i32 {
        Self::convert_date_time_unit(DateTimeField::Month, DateTimeField::Decade, self.months)
            .unwrap()
    }

    /// Computes the year part of the interval.
    ///
    /// The year part is the number of whole years in the interval. For example,
    /// this function returns `3` for the interval `3 years 4 months`.
    pub fn years(&self) -> i32 {
        Self::convert_date_time_unit(DateTimeField::Month, DateTimeField::Year, self.months)
            .unwrap()
    }

    /// Computes the quarter part of the interval.
    ///
    /// The quarter part is obtained from taking the number of whole months modulo 12,
    /// and assigning quarter #1 for months 0-2, #2 for 3-5, #3 for 6-8 and #4 for 9-11.
    /// For example, this function returns `4` for the interval `11 months`.
    pub fn quarters(&self) -> i32 {
        self.months() / 3 + 1
    }

    /// Computes the month part of the interval.
    ///
    /// The month part is the number of whole months in the interval, modulo 12.
    /// For example, this function returns `4` for the interval `3 years 4
    /// months`.
    pub fn months(&self) -> i32 {
        self.months % i32::from(Self::MONTH_PER_YEAR)
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
        Self::convert_date_time_unit(
            DateTimeField::Microseconds,
            DateTimeField::Hour,
            self.micros,
        )
        .unwrap()
            % i64::from(Self::HOUR_PER_DAY)
    }

    /// Computes the minute part of the interval.
    ///
    /// The minute part is the number of whole minutes in the interval, modulo
    /// 60. For example, this function returns `3` for the interval `5 days 4
    /// hours 3 minutes 2.1 seconds`.
    pub fn minutes(&self) -> i64 {
        Self::convert_date_time_unit(
            DateTimeField::Microseconds,
            DateTimeField::Minute,
            self.micros,
        )
        .unwrap()
            % i64::from(Self::MINUTE_PER_HOUR)
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

    /// Computes the total number of epoch seconds in the interval.
    /// When extracting an epoch, PostgreSQL considers a year
    /// 365.25 days.
    pub fn as_epoch_seconds<T>(&self) -> T
    where
        T: DecimalLike,
    {
        let days = T::from(self.years()) * T::from(Self::EPOCH_DAYS_PER_YEAR)
            + T::from(self.months()) * T::from(Self::DAY_PER_MONTH)
            + T::from(self.days);
        let seconds = days
            * T::from(Self::HOUR_PER_DAY)
            * T::from(Self::MINUTE_PER_HOUR)
            * T::from(Self::SECOND_PER_MINUTE);

        seconds
            + T::lossy_from(self.micros)
                / (T::from(Self::MICROSECOND_PER_MILLISECOND)
                    * T::from(Self::MILLISECOND_PER_SECOND))
    }

    /// Computes the total number of microseconds in the interval.
    pub fn as_microseconds(&self) -> i128 {
        // unwrap is safe because i32::MAX/i32::MIN number of months will not overflow an i128 when
        // converted to microseconds.
        Self::convert_date_time_unit(
            DateTimeField::Month,
            DateTimeField::Microseconds,
            i128::from(self.months),
        ).unwrap() +
        // unwrap is safe because i32::MAX/i32::MIN number of days will not overflow an i128 when
        // converted to microseconds.
        Self::convert_date_time_unit(
            DateTimeField::Day,
            DateTimeField::Microseconds,
            i128::from(self.days),
        ).unwrap() +
        i128::from(self.micros)
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
        if self.is_negative() {
            bail!("cannot convert negative interval to duration");
        }
        let micros: u64 = u64::try_from(self.as_microseconds())?;
        Ok(Duration::from_micros(micros))
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
                unreachable!("Cannot truncate interval by {f}");
            }
        }
    }

    /// Truncate the "tail" of the interval, removing all time units less than `f`.
    /// # Arguments
    /// - `f`: Round the interval down to the specified time unit.
    /// - `fsec_max_precision`: If `Some(x)`, keep only `x` places of microsecond precision.
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
            Millennium => {
                self.months -= self.months % (12 * 1000);
                self.days = 0;
                self.micros = 0;
            }
            Century => {
                self.months -= self.months % (12 * 100);
                self.days = 0;
                self.micros = 0;
            }
            Decade => {
                self.months -= self.months % (12 * 10);
                self.days = 0;
                self.micros = 0;
            }
            Year => {
                self.months -= self.months % 12;
                self.days = 0;
                self.micros = 0;
            }
            Month => {
                self.days = 0;
                self.micros = 0;
            }
            // Round microseconds.
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
            Hour | Minute | Milliseconds | Microseconds => {
                self.micros -= self.micros % f.micros_multiplier();
            }
        }
        Ok(())
    }

    /// Returns a new Interval with only the time component
    pub fn as_time_interval(&self) -> Self {
        Self::new(0, 0, self.micros)
    }

    /// Returns true if combining all fields results in a negative number, false otherwise
    pub fn is_negative(&self) -> bool {
        self.as_microseconds() < 0
    }

    /// Convert val from source unit to dest unit. Does not maintain fractional values.
    /// Returns None if the result overflows/underflows.
    ///
    /// WARNING: Due to the fact that Intervals consider months to have 30 days, you may get
    /// unexpected and incorrect results when trying to convert from a non-year type to a year type
    /// and vice versa. For example from years to days.
    pub fn convert_date_time_unit<T>(
        source: DateTimeField,
        dest: DateTimeField,
        val: T,
    ) -> Option<T>
    where
        T: From<u16> + CheckedMul + std::ops::DivAssign,
    {
        if source < dest {
            Self::convert_date_time_unit_increasing(source, dest, val)
        } else if source > dest {
            Self::convert_date_time_unit_decreasing(source, dest, val)
        } else {
            Some(val)
        }
    }

    fn convert_date_time_unit_increasing<T>(
        source: DateTimeField,
        dest: DateTimeField,
        val: T,
    ) -> Option<T>
    where
        T: From<u16> + std::ops::DivAssign,
    {
        let mut cur_unit = source;
        let mut res = val;
        while cur_unit < dest {
            let divisor: T = match cur_unit {
                DateTimeField::Millennium => 1.into(),
                DateTimeField::Century => Self::CENTURY_PER_MILLENNIUM.into(),
                DateTimeField::Decade => Self::DECADE_PER_CENTURY.into(),
                DateTimeField::Year => Self::YEAR_PER_DECADE.into(),
                DateTimeField::Month => Self::MONTH_PER_YEAR.into(),
                DateTimeField::Day => Self::DAY_PER_MONTH.into(),
                DateTimeField::Hour => Self::HOUR_PER_DAY.into(),
                DateTimeField::Minute => Self::MINUTE_PER_HOUR.into(),
                DateTimeField::Second => Self::SECOND_PER_MINUTE.into(),
                DateTimeField::Milliseconds => Self::MILLISECOND_PER_SECOND.into(),
                DateTimeField::Microseconds => Self::MICROSECOND_PER_MILLISECOND.into(),
            };
            res /= divisor;
            cur_unit = cur_unit.next_largest();
        }

        Some(res)
    }

    fn convert_date_time_unit_decreasing<T>(
        source: DateTimeField,
        dest: DateTimeField,
        val: T,
    ) -> Option<T>
    where
        T: From<u16> + CheckedMul,
    {
        let mut cur_unit = source;
        let mut res = val;
        while cur_unit > dest {
            let multiplier: T = match cur_unit {
                DateTimeField::Millennium => Self::CENTURY_PER_MILLENNIUM.into(),
                DateTimeField::Century => Self::DECADE_PER_CENTURY.into(),
                DateTimeField::Decade => Self::YEAR_PER_DECADE.into(),
                DateTimeField::Year => Self::MONTH_PER_YEAR.into(),
                DateTimeField::Month => Self::DAY_PER_MONTH.into(),
                DateTimeField::Day => Self::HOUR_PER_DAY.into(),
                DateTimeField::Hour => Self::MINUTE_PER_HOUR.into(),
                DateTimeField::Minute => Self::SECOND_PER_MINUTE.into(),
                DateTimeField::Second => Self::MILLISECOND_PER_SECOND.into(),
                DateTimeField::Milliseconds => Self::MICROSECOND_PER_MILLISECOND.into(),
                DateTimeField::Microseconds => 1.into(),
            };
            res = match res.checked_mul(&multiplier) {
                Some(r) => r,
                None => return None,
            };
            cur_unit = cur_unit.next_smallest();
        }

        Some(res)
    }

    /// Adjust interval so 'days' contains less than 30 days, adding the excess to 'months'.
    pub fn justify_days(&self) -> Result<Self, anyhow::Error> {
        let days_per_month = i32::from(Self::DAY_PER_MONTH);
        let (mut months, mut days) = Self::justify_days_inner(self.months, self.days)?;
        if months > 0 && days < 0 {
            days += days_per_month;
            months -= 1;
        } else if months < 0 && days > 0 {
            days -= days_per_month;
            months += 1;
        }

        Ok(Self::new(months, days, self.micros))
    }

    fn justify_days_inner(months: i32, days: i32) -> Result<(i32, i32), anyhow::Error> {
        let days_per_month = i32::from(Self::DAY_PER_MONTH);
        let whole_month = days / days_per_month;
        let days = days - whole_month * days_per_month;

        let months = months
            .checked_add(whole_month)
            .ok_or_else(|| anyhow!(&*MONTH_OVERFLOW_ERROR))?;

        Ok((months, days))
    }

    /// Adjust interval so 'micros' contains less than a whole day, adding the excess to 'days'.
    pub fn justify_hours(&self) -> Result<Self, anyhow::Error> {
        let (mut days, mut micros) = Self::justify_hours_inner(self.days, self.micros)?;
        if days > 0 && micros < 0 {
            micros += &*USECS_PER_DAY;
            days -= 1;
        } else if days < 0 && micros > 0 {
            micros -= &*USECS_PER_DAY;
            days += 1;
        }

        Ok(Self::new(self.months, days, micros))
    }

    fn justify_hours_inner(days: i32, micros: i64) -> Result<(i32, i64), anyhow::Error> {
        let days = i32::try_from(micros / &*USECS_PER_DAY)
            .ok()
            .and_then(|d| days.checked_add(d))
            .ok_or_else(|| anyhow!(&*DAY_OVERFLOW_ERROR))?;
        let micros = micros % &*USECS_PER_DAY;

        Ok((days, micros))
    }

    /// Adjust interval so 'days' contains less than 30 days, adding the excess to 'months'.
    /// Adjust interval so 'micros' contains less than a whole day, adding the excess to 'days'.
    /// Also, the sign bit on all three fields is made equal, so either all three fields are negative or all are positive.
    pub fn justify_interval(&self) -> Result<Self, anyhow::Error> {
        let days_per_month = i32::from(Self::DAY_PER_MONTH);
        let mut months = self.months;
        let mut days = self.days;
        let micros = self.micros;
        // We justify days twice to try to avoid an intermediate overflow of days if it would be
        // able to fit in months.
        if (days > 0 && micros > 0) || (days < 0 && micros < 0) {
            let (m, d) = Self::justify_days_inner(self.months, self.days)?;
            months = m;
            days = d;
        }
        let (days, mut micros) = Self::justify_hours_inner(days, micros)?;
        let (mut months, mut days) = Self::justify_days_inner(months, days)?;

        if months > 0 && (days < 0 || (days == 0 && micros < 0)) {
            days += days_per_month;
            months -= 1;
        } else if months < 0 && (days > 0 || (days == 0 && micros > 0)) {
            days -= days_per_month;
            months += 1;
        }

        if days > 0 && micros < 0 {
            micros += &*USECS_PER_DAY;
            days -= 1;
        } else if days < 0 && micros > 0 {
            micros -= &*USECS_PER_DAY;
            days += 1;
        }

        Ok(Self::new(months, days, micros))
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
            Interval::new(0, days, micros).to_string()
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
            Interval::new(mon, days, micros).to_string()
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
            let mut i = Interval::new((test.2).0, (test.2).1, (test.2).2);
            let j = Interval::new((test.3).0, (test.3).1, (test.3).2);

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
            let mut i = Interval::new((test.1).0, (test.1).1, (test.1).2);
            let j = Interval::new((test.2).0, (test.2).1, (test.2).2);

            i.truncate_high_fields(test.0);

            if i != j {
                panic!(
                    "test_interval_value_truncate_high_fields failed on {} \n actual: {:?} \n expected: {:?}",
                    test.0, i, j
                );
            }
        }
    }

    #[test]
    fn test_convert_date_time_unit() {
        assert_eq!(
            Some(1_123_200_000_000),
            Interval::convert_date_time_unit(
                DateTimeField::Day,
                DateTimeField::Microseconds,
                13i64
            )
        );

        assert_eq!(
            Some(3_558_399_705),
            Interval::convert_date_time_unit(
                DateTimeField::Milliseconds,
                DateTimeField::Month,
                i64::MAX
            )
        );

        assert_eq!(
            None,
            Interval::convert_date_time_unit(
                DateTimeField::Minute,
                DateTimeField::Second,
                i32::MAX
            )
        );

        assert_eq!(
            Some(1),
            Interval::convert_date_time_unit(DateTimeField::Day, DateTimeField::Year, 365)
        );

        // Strange behavior due to months having 30 days
        assert_eq!(
            Some(360),
            Interval::convert_date_time_unit(DateTimeField::Year, DateTimeField::Day, 1)
        );
    }
}
