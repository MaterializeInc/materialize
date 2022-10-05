// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};
use mz_repr::adt::date::Date;
use mz_repr::adt::timestamp::{CheckedTimestamp, DateLike};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_lowertest::MzReflect;
use mz_repr::adt::datetime::DateTimeUnits;
use mz_repr::adt::numeric::Numeric;
use mz_repr::{strconv, ColumnType, ScalarType};

use crate::scalar::func::EagerUnaryFunc;
use crate::EvalError;

sqlfunc!(
    #[sqlname = "date_to_text"]
    #[preserves_uniqueness = true]
    fn cast_date_to_string(a: Date) -> String {
        let mut buf = String::new();
        strconv::format_date(&mut buf, a);
        buf
    }
);

sqlfunc!(
    #[sqlname = "date_to_timestamp"]
    #[preserves_uniqueness = true]
    fn cast_date_to_timestamp(a: Date) -> Result<CheckedTimestamp<NaiveDateTime>, EvalError> {
        Ok(CheckedTimestamp::from_timestamplike(
            NaiveDate::from(a).and_hms(0, 0, 0),
        )?)
    }
);

sqlfunc!(
    #[sqlname = "date_to_timestamp_with_timezone"]
    #[preserves_uniqueness = true]
    fn cast_date_to_timestamp_tz(a: Date) -> Result<CheckedTimestamp<DateTime<Utc>>, EvalError> {
        Ok(CheckedTimestamp::from_timestamplike(
            DateTime::<Utc>::from_utc(NaiveDate::from(a).and_hms(0, 0, 0), Utc),
        )?)
    }
);

pub fn extract_date_inner(units: DateTimeUnits, date: NaiveDate) -> Result<Numeric, EvalError> {
    match units {
        DateTimeUnits::Epoch => Ok(Numeric::from(date.extract_epoch())),
        DateTimeUnits::Millennium => Ok(Numeric::from(date.millennium())),
        DateTimeUnits::Century => Ok(Numeric::from(date.century())),
        DateTimeUnits::Decade => Ok(Numeric::from(date.decade())),
        DateTimeUnits::Year => Ok(Numeric::from(date.year())),
        DateTimeUnits::Quarter => Ok(Numeric::from(date.quarter())),
        DateTimeUnits::Week => Ok(Numeric::from(date.iso_week_number())),
        DateTimeUnits::Month => Ok(Numeric::from(date.month())),
        DateTimeUnits::Day => Ok(Numeric::from(date.day())),
        DateTimeUnits::DayOfWeek => Ok(Numeric::from(date.day_of_week())),
        DateTimeUnits::DayOfYear => Ok(Numeric::from(date.ordinal())),
        DateTimeUnits::IsoDayOfWeek => Ok(Numeric::from(date.iso_day_of_week())),
        DateTimeUnits::Hour
        | DateTimeUnits::Minute
        | DateTimeUnits::Second
        | DateTimeUnits::Milliseconds
        | DateTimeUnits::Microseconds => Err(EvalError::UnsupportedUnits(
            format!("{}", units),
            "date".to_string(),
        )),
        DateTimeUnits::Timezone
        | DateTimeUnits::TimezoneHour
        | DateTimeUnits::TimezoneMinute
        | DateTimeUnits::IsoDayOfYear => Err(EvalError::Unsupported {
            feature: format!("'{}' timestamp units", units),
            issue_no: None,
        }),
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct ExtractDate(pub DateTimeUnits);

impl<'a> EagerUnaryFunc<'a> for ExtractDate {
    type Input = Date;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, a: Date) -> Result<Numeric, EvalError> {
        extract_date_inner(self.0, a.into())
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Numeric { max_scale: None }.nullable(input.nullable)
    }
}

impl fmt::Display for ExtractDate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "extract_{}_d", self.0)
    }
}
