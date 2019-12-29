// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::fmt::{self, Write};

use serde::{Deserialize, Serialize};

/// Either a number of months, or a number of seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Hash, Deserialize)]
pub enum Interval {
    /// A possibly negative number of months for field types like `YEAR`
    Months(i64),
    /// An actual timespan, possibly negative, because why not
    Duration {
        is_positive: bool,
        duration: std::time::Duration,
    },
}

impl Interval {
    /// Computes the year part of the interval.
    ///
    /// The year part is the number of whole years in the interval. For example,
    /// this function returns `3.0` for the interval `3 years 4 months`.
    pub fn years(&self) -> f64 {
        match self {
            Interval::Months(n) => (n / 12) as f64,
            Interval::Duration { .. } => 0.0,
        }
    }

    /// Computes the month part of the interval.
    ///
    /// The whole part is the number of whole months in the interval, modulo 12.
    /// For example, this function returns `4.0` for the interval `3 years 4
    /// months`.
    pub fn months(&self) -> f64 {
        match self {
            Interval::Months(n) => (n % 12) as f64,
            Interval::Duration { .. } => 0.0,
        }
    }

    /// Computes the day part of the interval.
    ///
    /// The day part is the number of whole days in the interval. For example,
    /// this function returns `5.0` for the interval `5 days 4 hours 3 minutes
    /// 2.1 seconds`.
    pub fn days(&self) -> f64 {
        match self {
            Interval::Months(_) => 0.0,
            Interval::Duration { duration, .. } => (duration.as_secs() / (60 * 60 * 24)) as f64,
        }
    }

    /// Computes the hour part of the interval.
    ///
    /// The hour part is the number of whole hours in the interval, modulo 24.
    /// For example, this function returns `4.0` for the interval `5 days 4
    /// hours 3 minutes 2.1 seconds`.
    pub fn hours(&self) -> f64 {
        match self {
            Interval::Months(_) => 0.0,
            Interval::Duration { duration, .. } => ((duration.as_secs() / (60 * 60)) % 24) as f64,
        }
    }

    /// Computes the minute part of the interval.
    ///
    /// The minute part is the number of whole minutes in the interval, modulo
    /// 60. For example, this function returns `3.0` for the interval `5 days 4
    /// hours 3 minutes 2.1 seconds`.
    pub fn minutes(&self) -> f64 {
        match self {
            Interval::Months(_) => 0.0,
            Interval::Duration { duration, .. } => ((duration.as_secs() / 60) % 60) as f64,
        }
    }

    /// Computes the second part of the interval.
    ///
    /// The second part is the number of fractional seconds in the interval,
    /// modulo 60.0.
    pub fn seconds(&self) -> f64 {
        match self {
            Interval::Months(_) => 0.0,
            Interval::Duration { duration, .. } => {
                let s = (duration.as_secs() % 60) as f64;
                let ns = f64::from(duration.subsec_nanos()) / 1e9;
                s + ns
            }
        }
    }
}

/// Format an interval in a human form
///
/// Example outputs:
///
/// * 1 year
/// * 2 years
/// * 00
/// * 01:00.01
impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Interval::Months(c) => {
                let mut c = *c;
                if c == 0 {
                    f.write_str("0 months")?;
                    return Ok(());
                }
                if c < 0 {
                    f.write_char('-')?;
                }
                c = c.abs();
                if c >= 12 {
                    let years = c / 12;
                    c %= 12;
                    write!(f, "{} year", years)?;
                    if years > 1 {
                        f.write_char('s')?;
                    }
                    if c > 0 {
                        f.write_char(' ')?;
                    }
                }
                if c > 0 {
                    write!(f, "{} month", c)?;
                    if c > 1 {
                        f.write_char('s')?;
                    }
                }
            }
            Interval::Duration {
                is_positive,
                duration,
            } => {
                let mut secs = duration.as_secs();
                let mut nanos = duration.subsec_nanos();
                let days = secs / (24 * 60 * 60);
                secs %= 24 * 60 * 60;
                let hours = secs / (60 * 60);
                secs %= 60 * 60;
                let minutes = secs / 60;
                secs %= 60;

                if days > 0 {
                    if !*is_positive {
                        f.write_char('-')?;
                    }
                    write!(f, "{} day", days)?;
                    if days != 1 || !*is_positive {
                        f.write_char('s')?;
                    }
                }

                if days == 0 || hours > 0 || minutes > 0 || secs > 0 || nanos > 0 {
                    if days > 0 {
                        f.write_char(' ')?;
                    }
                    if !*is_positive {
                        f.write_char('-')?;
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
        assert_eq!(&Interval::Months(1).to_string(), "1 month");
        assert_eq!(&Interval::Months(0).to_string(), "0 months");
        assert_eq!(&Interval::Months(12).to_string(), "1 year");
        assert_eq!(&Interval::Months(13).to_string(), "1 year 1 month");
        assert_eq!(&Interval::Months(24).to_string(), "2 years");
        assert_eq!(&Interval::Months(25).to_string(), "2 years 1 month");
        assert_eq!(&Interval::Months(26).to_string(), "2 years 2 months");

        fn dur(is_positive: bool, d: u64) -> String {
            Interval::Duration {
                is_positive,
                duration: std::time::Duration::from_secs(d),
            }
            .to_string()
        }
        assert_eq!(&dur(true, 86_400 * 2), "2 days");
        assert_eq!(&dur(true, 86_400 * 2 + 3_600 * 3), "2 days 03:00:00");
        assert_eq!(
            &dur(true, 86_400 * 2 + 3_600 * 3 + 60 * 45 + 6),
            "2 days 03:45:06"
        );
        assert_eq!(
            &dur(true, 86_400 * 2 + 3_600 * 3 + 60 * 45),
            "2 days 03:45:00"
        );
        assert_eq!(&dur(true, 86_400 * 2 + 6), "2 days 00:00:06");
        assert_eq!(&dur(true, 86_400 * 2 + 60 * 45 + 6), "2 days 00:45:06");
        assert_eq!(&dur(true, 86_400 * 2 + 3_600 * 3 + 6), "2 days 03:00:06");
        assert_eq!(&dur(true, 3_600 * 3 + 60 * 45 + 6), "03:45:06");
        assert_eq!(&dur(true, 3_600 * 3 + 6), "03:00:06");
        assert_eq!(&dur(true, 3_600 * 3), "03:00:00");
        assert_eq!(&dur(true, 60 * 45 + 6), "00:45:06");
        assert_eq!(&dur(true, 60 * 45), "00:45:00");
        assert_eq!(&dur(true, 6), "00:00:06");

        assert_eq!(&dur(false, 86_400 * 2 + 6), "-2 days -00:00:06");
        assert_eq!(&dur(false, 86_400 * 2 + 60 * 45 + 6), "-2 days -00:45:06");
        assert_eq!(&dur(false, 86_400 * 2 + 3_600 * 3 + 6), "-2 days -03:00:06");
        assert_eq!(&dur(false, 3_600 * 3 + 60 * 45 + 6), "-03:45:06");
        assert_eq!(&dur(false, 3_600 * 3 + 6), "-03:00:06");
        assert_eq!(&dur(false, 3_600 * 3), "-03:00:00");
        assert_eq!(&dur(false, 60 * 45 + 6), "-00:45:06");
        assert_eq!(&dur(false, 60 * 45), "-00:45:00");
        assert_eq!(&dur(false, 6), "-00:00:06");

        assert_eq!(&dur(true, 0), "00:00:00");
    }
}
