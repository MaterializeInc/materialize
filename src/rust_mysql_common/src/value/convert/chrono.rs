// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

//! This module implements conversion from/to `Value` for `chrono` types.

#![cfg(feature = "chrono")]

use std::convert::TryFrom;

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

use crate::value::Value;

use super::{
    parse_mysql_datetime_string, parse_mysql_time_string, FromValue, FromValueError, ParseIr,
};

#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
impl FromValue for NaiveDate {
    type Intermediate = ParseIr<NaiveDate>;
}

#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
impl FromValue for NaiveTime {
    type Intermediate = ParseIr<NaiveTime>;
}

#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
impl FromValue for NaiveDateTime {
    type Intermediate = ParseIr<NaiveDateTime>;
}

#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
impl TryFrom<Value> for ParseIr<NaiveDateTime> {
    type Error = FromValueError;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v {
            Value::Date(year, month, day, hour, minute, second, micros) => {
                let date = NaiveDate::from_ymd_opt(year.into(), month.into(), day.into());
                let time = NaiveTime::from_hms_micro_opt(
                    hour.into(),
                    minute.into(),
                    second.into(),
                    micros,
                );
                if let Some((date, time)) = date.zip(time) {
                    Ok(ParseIr(NaiveDateTime::new(date, time), v))
                } else {
                    Err(FromValueError(v))
                }
            }
            Value::Bytes(ref bytes) => {
                if let Some((year, month, day, hour, minute, second, micros)) =
                    parse_mysql_datetime_string(bytes)
                {
                    let date = NaiveDate::from_ymd_opt(year as i32, month, day);
                    let time = NaiveTime::from_hms_micro_opt(hour, minute, second, micros);
                    if let Some((date, time)) = date.zip(time) {
                        Ok(ParseIr(NaiveDateTime::new(date, time), v))
                    } else {
                        Err(FromValueError(v))
                    }
                } else {
                    Err(FromValueError(v))
                }
            }
            _ => Err(FromValueError(v)),
        }
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
impl TryFrom<Value> for ParseIr<NaiveDate> {
    type Error = FromValueError;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        let result = match v {
            Value::Date(year, month, day, hour, minute, second, micros) => {
                let date = NaiveDate::from_ymd_opt(year.into(), month.into(), day.into());
                Ok((
                    date,
                    Value::Date(year, month, day, hour, minute, second, micros),
                ))
            }
            Value::Bytes(bytes) => {
                if let Some((y, m, d, _, _, _, _)) = parse_mysql_datetime_string(&*bytes) {
                    let date = NaiveDate::from_ymd_opt(y as i32, m, d);
                    Ok((date, Value::Bytes(bytes)))
                } else {
                    Err(FromValueError(Value::Bytes(bytes)))
                }
            }
            v => Err(FromValueError(v)),
        };

        let (date, value) = result?;

        if let Some(output) = date {
            Ok(ParseIr(output, value))
        } else {
            Err(FromValueError(value))
        }
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
impl TryFrom<Value> for ParseIr<NaiveTime> {
    type Error = FromValueError;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        let result = match v {
            Value::Time(false, 0, h, m, s, u) => {
                let time = NaiveTime::from_hms_micro_opt(h.into(), m.into(), s.into(), u);
                Ok((time, Value::Time(false, 0, h, m, s, u)))
            }
            Value::Bytes(bytes) => {
                if let Some((false, h, m, s, u)) = parse_mysql_time_string(&*bytes) {
                    let time = NaiveTime::from_hms_micro_opt(h, m as u32, s as u32, u);
                    Ok((time, Value::Bytes(bytes)))
                } else {
                    Err(FromValueError(Value::Bytes(bytes)))
                }
            }
            v => Err(FromValueError(v)),
        };

        let (time, value) = result?;

        if let Some(output) = time {
            Ok(ParseIr(output, value))
        } else {
            Err(FromValueError(value))
        }
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
impl From<ParseIr<NaiveDateTime>> for NaiveDateTime {
    fn from(value: ParseIr<NaiveDateTime>) -> Self {
        value.commit()
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
impl From<ParseIr<NaiveDate>> for NaiveDate {
    fn from(value: ParseIr<NaiveDate>) -> Self {
        value.commit()
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
impl From<ParseIr<NaiveTime>> for NaiveTime {
    fn from(value: ParseIr<NaiveTime>) -> Self {
        value.commit()
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
impl From<ParseIr<NaiveDateTime>> for Value {
    fn from(value: ParseIr<NaiveDateTime>) -> Self {
        value.rollback()
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
impl From<ParseIr<NaiveDate>> for Value {
    fn from(value: ParseIr<NaiveDate>) -> Self {
        value.rollback()
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
impl From<ParseIr<NaiveTime>> for Value {
    fn from(value: ParseIr<NaiveTime>) -> Self {
        value.rollback()
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
impl From<NaiveDateTime> for Value {
    fn from(x: NaiveDateTime) -> Value {
        if 1000 > x.year() || x.year() > 9999 {
            panic!("Year `{}` not in supported range [1000, 9999]", x.year())
        }
        Value::Date(
            x.year() as u16,
            x.month() as u8,
            x.day() as u8,
            x.hour() as u8,
            x.minute() as u8,
            x.second() as u8,
            x.nanosecond() / 1000,
        )
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
impl From<NaiveDate> for Value {
    fn from(x: NaiveDate) -> Value {
        if 1000 > x.year() || x.year() > 9999 {
            panic!("Year `{}` not in supported range [1000, 9999]", x.year())
        }
        Value::Date(x.year() as u16, x.month() as u8, x.day() as u8, 0, 0, 0, 0)
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
impl From<NaiveTime> for Value {
    fn from(x: NaiveTime) -> Value {
        Value::Time(
            false,
            0,
            x.hour() as u8,
            x.minute() as u8,
            x.second() as u8,
            x.nanosecond() / 1000,
        )
    }
}
