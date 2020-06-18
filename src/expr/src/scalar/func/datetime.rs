// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Date and time functions.

use std::convert::TryInto;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};

use ore::result::ResultExt;
use repr::adt::datetime::DateTimeUnits;
use repr::strconv;
use repr::{Datum, RowArena, ScalarType};

use crate::scalar::func::{FuncProps, Nulls, OutputType};
use crate::scalar::EvalError;

pub const CAST_TO_STRING_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::String),
};

pub fn cast_date_to_string<'a>(
    a: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut buf = String::new();
    strconv::format_date(&mut buf, a.unwrap_date());
    Ok(Datum::String(temp_storage.push_string(buf)))
}

pub fn cast_time_to_string<'a>(
    a: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut buf = String::new();
    strconv::format_time(&mut buf, a.unwrap_time());
    Ok(Datum::String(temp_storage.push_string(buf)))
}

pub fn cast_timestamp_to_string<'a>(
    a: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut buf = String::new();
    strconv::format_timestamp(&mut buf, a.unwrap_timestamp());
    Ok(Datum::String(temp_storage.push_string(buf)))
}

pub fn cast_timestamptz_to_string<'a>(
    a: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut buf = String::new();
    strconv::format_timestamptz(&mut buf, a.unwrap_timestamptz());
    Ok(Datum::String(temp_storage.push_string(buf)))
}

pub const CAST_STRING_TO_DATE_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Date),
};

pub fn cast_string_to_date(a: Datum) -> Result<Datum, EvalError> {
    strconv::parse_date(a.unwrap_str())
        .map(Datum::Date)
        .err_into()
}

pub const CAST_STRING_TO_TIME_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Time),
};

pub fn cast_string_to_time(a: Datum) -> Result<Datum, EvalError> {
    strconv::parse_time(a.unwrap_str())
        .map(Datum::Time)
        .err_into()
}

pub const CAST_STRING_TO_TIMESTAMP_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Timestamp),
};

pub fn cast_string_to_timestamp(a: Datum) -> Result<Datum, EvalError> {
    strconv::parse_timestamp(a.unwrap_str())
        .map(Datum::Timestamp)
        .err_into()
}

pub const CAST_STRING_TO_TIMESTAMPTZ_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::TimestampTz),
};

pub fn cast_string_to_timestamptz(a: Datum) -> Result<Datum, EvalError> {
    strconv::parse_timestamptz(a.unwrap_str())
        .map(Datum::TimestampTz)
        .err_into()
}

pub const CAST_DATE_TO_TIMESTAMP_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: true,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Timestamp),
};

pub fn cast_date_to_timestamp(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::Timestamp(a.unwrap_date().and_hms(0, 0, 0)))
}

pub const CAST_DATE_TO_TIMESTAMPTZ_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: true,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::TimestampTz),
};

pub fn cast_date_to_timestamptz(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::TimestampTz(DateTime::<Utc>::from_utc(
        a.unwrap_date().and_hms(0, 0, 0),
        Utc,
    )))
}

pub const CAST_TIMESTAMP_TO_DATE_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Date),
};

pub fn cast_timestamp_to_date(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::Date(a.unwrap_timestamp().date()))
}

pub fn cast_timestamptz_to_date(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::Date(a.unwrap_timestamptz().naive_utc().date()))
}

pub const CAST_TIMESTAMP_TO_TIMESTAMPTZ_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::TimestampTz),
};

pub fn cast_timestamp_to_timestamptz(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::TimestampTz(DateTime::<Utc>::from_utc(
        a.unwrap_timestamp(),
        Utc,
    )))
}

pub const CAST_TIMESTAMPTZ_TO_TIMESTAMP_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Timestamp),
};

pub fn cast_timestamptz_to_timestamp(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::Timestamp(a.unwrap_timestamptz().naive_utc()))
}

pub const MAKE_TIMESTAMP_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: true, // TODO(benesch): should error instead
    },
    output_type: OutputType::Fixed(ScalarType::Timestamp),
};

pub fn make_timestamp<'a>(datums: &[Datum<'a>]) -> Result<Datum<'a>, EvalError> {
    let year: i32 = match datums[0].unwrap_int64().try_into() {
        Ok(year) => year,
        Err(_) => return Ok(Datum::Null),
    };
    let month: u32 = match datums[1].unwrap_int64().try_into() {
        Ok(month) => month,
        Err(_) => return Ok(Datum::Null),
    };
    let day: u32 = match datums[2].unwrap_int64().try_into() {
        Ok(day) => day,
        Err(_) => return Ok(Datum::Null),
    };
    let hour: u32 = match datums[3].unwrap_int64().try_into() {
        Ok(day) => day,
        Err(_) => return Ok(Datum::Null),
    };
    let minute: u32 = match datums[4].unwrap_int64().try_into() {
        Ok(day) => day,
        Err(_) => return Ok(Datum::Null),
    };
    let second_float = datums[5].unwrap_float64();
    let second = second_float as u32;
    let micros = ((second_float - second as f64) * 1_000_000.0) as u32;
    let date = match NaiveDate::from_ymd_opt(year, month, day) {
        Some(date) => date,
        None => return Ok(Datum::Null),
    };
    let timestamp = match date.and_hms_micro_opt(hour, minute, second, micros) {
        Some(timestamp) => timestamp,
        None => return Ok(Datum::Null),
    };
    Ok(Datum::Timestamp(timestamp))
}

pub const TO_TIMESTAMP_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: true, // TODO(benesch): should error instead
    },
    output_type: OutputType::Fixed(ScalarType::TimestampTz),
};

pub fn to_timestamp(a: Datum) -> Result<Datum, EvalError> {
    let f = a.unwrap_float64();
    if !f.is_finite() {
        return Ok(Datum::Null);
    }
    let secs = f.trunc() as i64;
    // NOTE(benesch): PostgreSQL has microsecond precision in its timestamps,
    // while chrono has nanosecond precision. While we normally accept
    // nanosecond precision, here we round to the nearest microsecond because
    // f64s lose quite a bit of accuracy in the nanosecond digits when dealing
    // with common Unix timestamp values (> 1 billion).
    let nanosecs = ((f.fract() * 1_000_000.0).round() as u32) * 1_000;
    match NaiveDateTime::from_timestamp_opt(secs as i64, nanosecs as u32) {
        None => Ok(Datum::Null),
        Some(ts) => Ok(Datum::TimestampTz(DateTime::<Utc>::from_utc(ts, Utc))),
    }
}

/// A timestamp with both a date and a time component, but not necessarily a
/// timezone component.
pub trait TimestampLike: chrono::Datelike + chrono::Timelike + for<'a> Into<Datum<'a>> {
    fn new(date: NaiveDate, time: NaiveTime) -> Self;

    /// Returns the weekday as a `usize` between 0 and 6, where 0 represents
    /// Sunday and 6 represents Saturday.
    fn weekday0(&self) -> usize {
        self.weekday().num_days_from_sunday() as usize
    }

    /// Like [`chrono::Datelike::year_ce`], but works on the ISO week system.
    fn iso_year_ce(&self) -> u32 {
        let year = self.iso_week().year();
        if year < 1 {
            (1 - year) as u32
        } else {
            year as u32
        }
    }

    fn timestamp(&self) -> i64;

    fn timestamp_subsec_micros(&self) -> u32;

    fn extract_epoch(&self) -> f64 {
        self.timestamp() as f64 + (self.timestamp_subsec_micros() as f64) / 1e6
    }

    fn extract_year(&self) -> f64 {
        f64::from(self.year())
    }

    fn extract_quarter(&self) -> f64 {
        (f64::from(self.month()) / 3.0).ceil()
    }

    fn extract_month(&self) -> f64 {
        f64::from(self.month())
    }

    fn extract_day(&self) -> f64 {
        f64::from(self.day())
    }

    fn extract_hour(&self) -> f64 {
        f64::from(self.hour())
    }

    fn extract_minute(&self) -> f64 {
        f64::from(self.minute())
    }

    fn extract_second(&self) -> f64 {
        let s = f64::from(self.second());
        let ns = f64::from(self.nanosecond()) / 1e9;
        s + ns
    }

    /// Extract the iso week of the year
    ///
    /// Note that because isoweeks are defined in terms of January 4th, Jan 1 is only in week
    /// 1 about half of the time
    fn extract_week(&self) -> f64 {
        f64::from(self.iso_week().week())
    }

    fn extract_dayofyear(&self) -> f64 {
        f64::from(self.ordinal())
    }

    fn extract_dayofweek(&self) -> f64 {
        f64::from(self.weekday().num_days_from_sunday())
    }

    fn extract_isodayofweek(&self) -> f64 {
        f64::from(self.weekday().number_from_monday())
    }

    fn truncate_microseconds(&self) -> Self {
        let time = NaiveTime::from_hms_micro(
            self.hour(),
            self.minute(),
            self.second(),
            self.nanosecond() / 1_000,
        );

        Self::new(self.date(), time)
    }

    fn truncate_milliseconds(&self) -> Self {
        let time = NaiveTime::from_hms_milli(
            self.hour(),
            self.minute(),
            self.second(),
            self.nanosecond() / 1_000_000,
        );

        Self::new(self.date(), time)
    }

    fn truncate_second(&self) -> Self {
        let time = NaiveTime::from_hms(self.hour(), self.minute(), self.second());

        Self::new(self.date(), time)
    }

    fn truncate_minute(&self) -> Self {
        Self::new(
            self.date(),
            NaiveTime::from_hms(self.hour(), self.minute(), 0),
        )
    }

    fn truncate_hour(&self) -> Self {
        Self::new(self.date(), NaiveTime::from_hms(self.hour(), 0, 0))
    }

    fn truncate_day(&self) -> Self {
        Self::new(self.date(), NaiveTime::from_hms(0, 0, 0))
    }

    fn truncate_week(&self) -> Self {
        let num_days_from_monday = self.date().weekday().num_days_from_monday();
        Self::new(
            NaiveDate::from_ymd(self.year(), self.month(), self.day() - num_days_from_monday),
            NaiveTime::from_hms(0, 0, 0),
        )
    }

    fn truncate_month(&self) -> Self {
        Self::new(
            NaiveDate::from_ymd(self.year(), self.month(), 1),
            NaiveTime::from_hms(0, 0, 0),
        )
    }

    fn truncate_quarter(&self) -> Self {
        let month = self.month();
        let quarter = if month <= 3 {
            1
        } else if month <= 6 {
            4
        } else if month <= 9 {
            7
        } else {
            10
        };

        Self::new(
            NaiveDate::from_ymd(self.year(), quarter, 1),
            NaiveTime::from_hms(0, 0, 0),
        )
    }

    fn truncate_year(&self) -> Self {
        Self::new(
            NaiveDate::from_ymd(self.year(), 1, 1),
            NaiveTime::from_hms(0, 0, 0),
        )
    }
    fn truncate_decade(&self) -> Self {
        Self::new(
            NaiveDate::from_ymd(self.year() - (self.year() % 10), 1, 1),
            NaiveTime::from_hms(0, 0, 0),
        )
    }
    fn truncate_century(&self) -> Self {
        // Expects the first year of the century, meaning 2001 instead of 2000.
        Self::new(
            NaiveDate::from_ymd(self.year() - (self.year() % 100) + 1, 1, 1),
            NaiveTime::from_hms(0, 0, 0),
        )
    }
    fn truncate_millennium(&self) -> Self {
        // Expects the first year of the millennium, meaning 2001 instead of 2000.
        Self::new(
            NaiveDate::from_ymd(self.year() - (self.year() % 1_000) + 1, 1, 1),
            NaiveTime::from_hms(0, 0, 0),
        )
    }

    /// Return the date component of the timestamp
    fn date(&self) -> NaiveDate;

    /// Returns a string representing the timezone's offset from UTC.
    fn timezone_offset(&self) -> &'static str;

    /// Returns a string representing the hour portion of the timezone's offset
    /// from UTC.
    fn timezone_hours(&self) -> &'static str;

    /// Returns a string representing the minute portion of the timezone's
    /// offset from UTC.
    fn timezone_minutes(&self) -> &'static str;

    /// Returns the abbreviated name of the timezone with the specified
    /// capitalization.
    fn timezone_name(&self, caps: bool) -> &'static str;
}

impl TimestampLike for chrono::NaiveDateTime {
    fn new(date: NaiveDate, time: NaiveTime) -> Self {
        NaiveDateTime::new(date, time)
    }

    fn date(&self) -> NaiveDate {
        self.date()
    }

    fn timestamp(&self) -> i64 {
        self.timestamp()
    }

    fn timestamp_subsec_micros(&self) -> u32 {
        self.timestamp_subsec_micros()
    }

    fn timezone_offset(&self) -> &'static str {
        "+00"
    }

    fn timezone_hours(&self) -> &'static str {
        "+00"
    }

    fn timezone_minutes(&self) -> &'static str {
        "00"
    }

    fn timezone_name(&self, _caps: bool) -> &'static str {
        ""
    }
}

impl TimestampLike for chrono::DateTime<chrono::Utc> {
    fn new(date: NaiveDate, time: NaiveTime) -> Self {
        DateTime::<Utc>::from_utc(NaiveDateTime::new(date, time), Utc)
    }

    fn date(&self) -> NaiveDate {
        self.naive_utc().date()
    }

    fn timestamp(&self) -> i64 {
        self.timestamp()
    }

    fn timestamp_subsec_micros(&self) -> u32 {
        self.timestamp_subsec_micros()
    }

    fn timezone_offset(&self) -> &'static str {
        "+00"
    }

    fn timezone_hours(&self) -> &'static str {
        "+00"
    }

    fn timezone_minutes(&self) -> &'static str {
        "00"
    }

    fn timezone_name(&self, caps: bool) -> &'static str {
        if caps {
            "UTC"
        } else {
            "utc"
        }
    }
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

pub fn date_part<T>(a: Datum, ts: T) -> Result<Datum, EvalError>
where
    T: TimestampLike,
{
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => date_part_inner(units, ts),
        Err(_) => Err(EvalError::UnknownUnits(units.to_owned())),
    }
}

pub fn date_part_inner<T>(units: DateTimeUnits, ts: T) -> Result<Datum<'static>, EvalError>
where
    T: TimestampLike,
{
    match units {
        DateTimeUnits::Epoch => Ok(ts.extract_epoch().into()),
        DateTimeUnits::Year => Ok(ts.extract_year().into()),
        DateTimeUnits::Quarter => Ok(ts.extract_quarter().into()),
        DateTimeUnits::Week => Ok(ts.extract_week().into()),
        DateTimeUnits::Day => Ok(ts.extract_day().into()),
        DateTimeUnits::DayOfWeek => Ok(ts.extract_dayofweek().into()),
        DateTimeUnits::DayOfYear => Ok(ts.extract_dayofyear().into()),
        DateTimeUnits::IsoDayOfWeek => Ok(ts.extract_isodayofweek().into()),
        DateTimeUnits::Hour => Ok(ts.extract_hour().into()),
        DateTimeUnits::Minute => Ok(ts.extract_minute().into()),
        DateTimeUnits::Second => Ok(ts.extract_second().into()),
        DateTimeUnits::Month => Ok(ts.extract_month().into()),
        DateTimeUnits::Millennium
        | DateTimeUnits::Century
        | DateTimeUnits::Decade
        | DateTimeUnits::Milliseconds
        | DateTimeUnits::Microseconds
        | DateTimeUnits::Timezone
        | DateTimeUnits::TimezoneHour
        | DateTimeUnits::TimezoneMinute
        | DateTimeUnits::IsoDayOfYear => Err(EvalError::UnsupportedDateTimeUnits(units)),
    }
}

pub const DATE_TRUNC_TIMESTAMP_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Timestamp),
};

pub const DATE_TRUNC_TIMESTAMPTZ_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::TimestampTz),
};

pub fn date_trunc<T>(a: Datum, ts: T) -> Result<Datum, EvalError>
where
    T: TimestampLike,
{
    let units = a.unwrap_str();
    match units.parse() {
        Ok(units) => date_trunc_inner(units, ts),
        Err(_) => Err(EvalError::UnknownUnits(units.to_owned())),
    }
}

pub fn date_trunc_inner<T>(units: DateTimeUnits, ts: T) -> Result<Datum<'static>, EvalError>
where
    T: TimestampLike,
{
    match units {
        DateTimeUnits::Millennium => Ok(ts.truncate_millennium().into()),
        DateTimeUnits::Century => Ok(ts.truncate_century().into()),
        DateTimeUnits::Decade => Ok(ts.truncate_decade().into()),
        DateTimeUnits::Year => Ok(ts.truncate_year().into()),
        DateTimeUnits::Quarter => Ok(ts.truncate_quarter().into()),
        DateTimeUnits::Week => Ok(ts.truncate_week().into()),
        DateTimeUnits::Day => Ok(ts.truncate_day().into()),
        DateTimeUnits::Hour => Ok(ts.truncate_hour().into()),
        DateTimeUnits::Minute => Ok(ts.truncate_minute().into()),
        DateTimeUnits::Second => Ok(ts.truncate_second().into()),
        DateTimeUnits::Month => Ok(ts.truncate_month().into()),
        DateTimeUnits::Milliseconds => Ok(ts.truncate_milliseconds().into()),
        DateTimeUnits::Microseconds => Ok(ts.truncate_microseconds().into()),
        DateTimeUnits::Epoch
        | DateTimeUnits::Timezone
        | DateTimeUnits::TimezoneHour
        | DateTimeUnits::TimezoneMinute
        | DateTimeUnits::DayOfWeek
        | DateTimeUnits::DayOfYear
        | DateTimeUnits::IsoDayOfWeek
        | DateTimeUnits::IsoDayOfYear => Err(EvalError::UnsupportedDateTimeUnits(units)),
    }
}

pub const ADD_DATE_TIME_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: true,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Timestamp),
};

pub fn add_date_time<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let date = a.unwrap_date();
    let time = b.unwrap_time();

    Ok(Datum::Timestamp(
        NaiveDate::from_ymd(date.year(), date.month(), date.day()).and_hms_nano(
            time.hour(),
            time.minute(),
            time.second(),
            time.nanosecond(),
        ),
    ))
}
