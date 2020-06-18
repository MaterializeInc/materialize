// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};

use ore::result::ResultExt;
use repr::adt::datetime::DateTimeUnits;
use repr::adt::interval::Interval;
use repr::{strconv, Datum, RowArena, ScalarType};

use crate::scalar::func::{FuncProps, Nulls, OutputType};
use crate::scalar::EvalError;

pub const CAST_INTERVAL_TO_STRING_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::String),
};

pub fn cast_interval_to_string<'a>(
    a: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut buf = String::new();
    strconv::format_interval(&mut buf, a.unwrap_interval());
    Ok(Datum::String(temp_storage.push_string(buf)))
}

pub const CAST_STRING_TO_INTERVAL_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Interval),
};

pub fn cast_string_to_interval(a: Datum) -> Result<Datum, EvalError> {
    strconv::parse_interval(a.unwrap_str())
        .map(Datum::Interval)
        .err_into()
}

pub const CAST_TIME_TO_INTERVAL_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Interval),
};

pub fn cast_time_to_interval(a: Datum) -> Result<Datum, EvalError> {
    let t = a.unwrap_time();
    match Interval::new(
        0,
        t.num_seconds_from_midnight() as i64,
        t.nanosecond() as i64,
    ) {
        Ok(i) => Ok(Datum::Interval(i)),
        Err(_) => Err(EvalError::IntervalOutOfRange),
    }
}

pub const CAST_INTERVAL_TO_TIME_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Time),
};

pub fn cast_interval_to_time(a: Datum) -> Result<Datum, EvalError> {
    let mut i = a.unwrap_interval();

    // Negative durations have their HH::MM::SS.NS values subtracted from 1 day.
    if i.duration < 0 {
        i = Interval::new(0, 86400, 0)
            .unwrap()
            .checked_add(
                &Interval::new(0, i.dur_as_secs() % (24 * 60 * 60), i.nanoseconds() as i64)
                    .unwrap(),
            )
            .unwrap();
    }

    Ok(Datum::Time(NaiveTime::from_hms_nano(
        i.hours() as u32,
        i.minutes() as u32,
        i.seconds() as u32,
        i.nanoseconds() as u32,
    )))
}

pub const INTERVAL_MATH_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Interval),
};

pub fn neg_interval(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(-a.unwrap_interval()))
}

pub fn add_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_interval()
        .checked_add(&b.unwrap_interval())
        .ok_or(EvalError::IntervalOutOfRange)
        .map(Datum::from)
}

pub fn sub_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_interval()
        .checked_add(&-b.unwrap_interval())
        .ok_or(EvalError::IntervalOutOfRange)
        .map(Datum::from)
}

pub const INTERVAL_DATE_MATH_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Timestamp),
};

pub const INTERVAL_TIME_MATH_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Time),
};

pub const INTERVAL_TIMESTAMP_MATH_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Timestamp),
};

pub const INTERVAL_TIMESTAMPTZ_MATH_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::TimestampTz),
};

pub fn add_timestamp_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let dt = a.unwrap_timestamp();
    Ok(Datum::Timestamp(match b {
        Datum::Interval(i) => {
            let dt = add_timestamp_months(dt, i.months);
            dt + i.duration_as_chrono()
        }
        _ => panic!("Tried to do timestamp addition with non-interval: {:?}", b),
    }))
}

pub fn add_timestamptz_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let dt = a.unwrap_timestamptz().naive_utc();

    let new_ndt = match b {
        Datum::Interval(i) => {
            let dt = add_timestamp_months(dt, i.months);
            dt + i.duration_as_chrono()
        }
        _ => panic!("Tried to do timestamp addition with non-interval: {:?}", b),
    };

    Ok(Datum::TimestampTz(DateTime::<Utc>::from_utc(new_ndt, Utc)))
}

pub fn sub_timestamp_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    add_timestamp_interval(a, Datum::Interval(-b.unwrap_interval()))
}

pub fn sub_timestamptz_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    add_timestamptz_interval(a, Datum::Interval(-b.unwrap_interval()))
}

pub fn add_date_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let date = a.unwrap_date();
    let interval = b.unwrap_interval();

    let dt = NaiveDate::from_ymd(date.year(), date.month(), date.day()).and_hms(0, 0, 0);
    let dt = add_timestamp_months(dt, interval.months);
    Ok(Datum::Timestamp(dt + interval.duration_as_chrono()))
}

pub fn add_time_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let time = a.unwrap_time();
    let interval = b.unwrap_interval();
    let (t, _) = time.overflowing_add_signed(interval.duration_as_chrono());
    Ok(Datum::Time(t))
}

pub fn sub_timestamp<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    Ok(Datum::from(a.unwrap_timestamp() - b.unwrap_timestamp()))
}

pub fn sub_timestamptz<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    Ok(Datum::from(a.unwrap_timestamptz() - b.unwrap_timestamptz()))
}

pub fn sub_date<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    Ok(Datum::from(a.unwrap_date() - b.unwrap_date()))
}

pub fn sub_time<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    Ok(Datum::from(a.unwrap_time() - b.unwrap_time()))
}

pub fn sub_date_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let date = a.unwrap_date();
    let interval = b.unwrap_interval();

    let dt = NaiveDate::from_ymd(date.year(), date.month(), date.day()).and_hms(0, 0, 0);
    let dt = add_timestamp_months(dt, -interval.months);
    Ok(Datum::Timestamp(dt - interval.duration_as_chrono()))
}

pub fn sub_time_interval<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let time = a.unwrap_time();
    let interval = b.unwrap_interval();
    let (t, _) = time.overflowing_sub_signed(interval.duration_as_chrono());
    Ok(Datum::Time(t))
}

pub const DATE_PART_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Float64),
};

pub fn date_part(a: Datum, interval: Interval) -> Result<Datum, EvalError> {
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => date_part_inner(units, interval),
        Err(_) => Err(EvalError::UnknownUnits(units.to_owned())),
    }
}

pub fn date_part_inner(
    units: DateTimeUnits,
    interval: Interval,
) -> Result<Datum<'static>, EvalError> {
    match units {
        DateTimeUnits::Epoch => Ok(interval.as_seconds().into()),
        DateTimeUnits::Year => Ok(interval.years().into()),
        DateTimeUnits::Day => Ok(interval.days().into()),
        DateTimeUnits::Hour => Ok(interval.hours().into()),
        DateTimeUnits::Minute => Ok(interval.minutes().into()),
        DateTimeUnits::Second => Ok(interval.seconds().into()),
        DateTimeUnits::Millennium
        | DateTimeUnits::Century
        | DateTimeUnits::Decade
        | DateTimeUnits::Quarter
        | DateTimeUnits::Week
        | DateTimeUnits::Month
        | DateTimeUnits::Milliseconds
        | DateTimeUnits::Microseconds
        | DateTimeUnits::Timezone
        | DateTimeUnits::TimezoneHour
        | DateTimeUnits::TimezoneMinute
        | DateTimeUnits::DayOfWeek
        | DateTimeUnits::DayOfYear
        | DateTimeUnits::IsoDayOfWeek
        | DateTimeUnits::IsoDayOfYear => Err(EvalError::UnsupportedDateTimeUnits(units)),
    }
}

pub fn add_timestamp_months(dt: NaiveDateTime, months: i32) -> NaiveDateTime {
    if months == 0 {
        return dt;
    }

    let mut months = months;

    let (mut year, mut month, mut day) = (dt.year(), dt.month0() as i32, dt.day());
    let years = months / 12;
    year += years;
    months %= 12;
    // positive modulus is easier to reason about
    if months < 0 {
        year -= 1;
        months += 12;
    }
    year += (month + months) / 12;
    month = (month + months) % 12;
    // account for dt.month0
    month += 1;

    // handle going from January 31st to February by saturation
    let mut new_d = chrono::NaiveDate::from_ymd_opt(year, month as u32, day);
    while new_d.is_none() {
        debug_assert!(day > 28, "there are no months with fewer than 28 days");
        day -= 1;
        new_d = chrono::NaiveDate::from_ymd_opt(year, month as u32, day);
    }
    let new_d = new_d.unwrap();

    // Neither postgres nor mysql support leap seconds, so this should be safe.
    //
    // Both my testing and https://dba.stackexchange.com/a/105829 support the
    // idea that we should ignore leap seconds
    new_d.and_hms_nano(dt.hour(), dt.minute(), dt.second(), dt.nanosecond())
}

#[cfg(test)]
mod test {
    use chrono::prelude::*;

    use super::*;

    #[test]
    fn add_interval_months() {
        let dt = ym(2000, 1);

        assert_eq!(add_timestamp_months(dt, 0), dt);
        assert_eq!(add_timestamp_months(dt, 1), ym(2000, 2));
        assert_eq!(add_timestamp_months(dt, 12), ym(2001, 1));
        assert_eq!(add_timestamp_months(dt, 13), ym(2001, 2));
        assert_eq!(add_timestamp_months(dt, 24), ym(2002, 1));
        assert_eq!(add_timestamp_months(dt, 30), ym(2002, 7));

        // and negatives
        assert_eq!(add_timestamp_months(dt, -1), ym(1999, 12));
        assert_eq!(add_timestamp_months(dt, -12), ym(1999, 1));
        assert_eq!(add_timestamp_months(dt, -13), ym(1998, 12));
        assert_eq!(add_timestamp_months(dt, -24), ym(1998, 1));
        assert_eq!(add_timestamp_months(dt, -30), ym(1997, 7));

        // and going over a year boundary by less than a year
        let dt = ym(1999, 12);
        assert_eq!(add_timestamp_months(dt, 1), ym(2000, 1));
        let end_of_month_dt = NaiveDate::from_ymd(1999, 12, 31).and_hms(9, 9, 9);
        assert_eq!(
            // leap year
            add_timestamp_months(end_of_month_dt, 2),
            NaiveDate::from_ymd(2000, 2, 29).and_hms(9, 9, 9),
        );
        assert_eq!(
            // not leap year
            add_timestamp_months(end_of_month_dt, 14),
            NaiveDate::from_ymd(2001, 2, 28).and_hms(9, 9, 9),
        );
    }

    fn ym(year: i32, month: u32) -> NaiveDateTime {
        NaiveDate::from_ymd(year, month, 1).and_hms(9, 9, 9)
    }
}
