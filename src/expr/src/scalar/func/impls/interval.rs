// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::NaiveTime;
use mz_expr_derive::sqlfunc;
use mz_ore::cast::CastLossy;
use mz_repr::adt::interval::{Interval, USECS_PER_DAY};
use mz_repr::strconv;
use num::traits::CheckedNeg;

use crate::EvalError;

#[sqlfunc(
    sqlname = "interval_to_text",
    preserves_uniqueness = true,
    inverse = to_unary!(super::CastStringToInterval),
    category = "Cast",
    doc = "Converts interval to text."
)]
fn cast_interval_to_string(a: Interval) -> String {
    let mut buf = String::new();
    strconv::format_interval(&mut buf, a);
    buf
}

#[sqlfunc(
    sqlname = "interval_to_time",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastTimeToInterval),
    category = "Cast",
    doc = "Converts interval to time."
)]
fn cast_interval_to_time(i: Interval) -> NaiveTime {
    // Modeled after the PostgreSQL implementation:
    // https://github.com/postgres/postgres/blob/6a1ea02c491d16474a6214603dce40b5b122d4d1/src/backend/utils/adt/date.c#L2003-L2027
    let mut result = i.micros % *USECS_PER_DAY;
    if result < 0 {
        result += *USECS_PER_DAY;
    }

    let i = Interval::new(0, 0, result);

    let hours: u32 = i
        .hours()
        .try_into()
        .expect("interval is positive and hours() returns a value in the range [-24, 24]");
    let minutes: u32 = i
        .minutes()
        .try_into()
        .expect("interval is positive and minutes() returns a value in the range [-60, 60]");
    let seconds: u32 = i64::cast_lossy(i.seconds::<f64>())
        .try_into()
        .expect("interval is positive and seconds() returns a value in the range [-60.0, 60.0]");
    let nanoseconds: u32 =
            i.nanoseconds().try_into().expect(
                "interval is positive and nanoseconds() returns a value in the range [-1_000_000_000, 1_000_000_000]",
            );

    NaiveTime::from_hms_nano_opt(hours, minutes, seconds, nanoseconds).unwrap()
}

#[sqlfunc(
    sqlname = "-",
    preserves_uniqueness = true,
    inverse = to_unary!(super::NegInterval),
    category = "Date and time",
    doc = "Negates the interval."
)]
fn neg_interval(i: Interval) -> Result<Interval, EvalError> {
    i.checked_neg()
        .ok_or_else(|| EvalError::IntervalOutOfRange(i.to_string().into()))
}

#[sqlfunc(
    sqlname = "justify_days",
    category = "Date and time",
    doc = "Adjusts the interval so 30-day time periods are represented as months.",
    url = "/sql/functions/justify-days"
)]
fn justify_days(i: Interval) -> Result<Interval, EvalError> {
    i.justify_days()
        .map_err(|_| EvalError::IntervalOutOfRange(i.to_string().into()))
}

#[sqlfunc(
    sqlname = "justify_hours",
    category = "Date and time",
    doc = "Adjusts the interval so 24-hour time periods are represented as days.",
    url = "/sql/functions/justify-hours"
)]
fn justify_hours(i: Interval) -> Result<Interval, EvalError> {
    i.justify_hours()
        .map_err(|_| EvalError::IntervalOutOfRange(i.to_string().into()))
}

#[sqlfunc(
    sqlname = "justify_interval",
    category = "Date and time",
    doc = "Adjusts the interval using justify_days and justify_hours, with additional sign adjustments.",
    url = "/sql/functions/justify-interval"
)]
fn justify_interval(i: Interval) -> Result<Interval, EvalError> {
    i.justify_interval()
        .map_err(|_| EvalError::IntervalOutOfRange(i.to_string().into()))
}
