// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::NaiveTime;
use num::traits::CheckedNeg;

use crate::EvalError;
use mz_repr::adt::interval::Interval;
use mz_repr::strconv;

sqlfunc!(
    #[sqlname = "interval_to_text"]
    #[preserves_uniqueness = true]
    fn cast_interval_to_string(a: Interval) -> String {
        let mut buf = String::new();
        strconv::format_interval(&mut buf, a);
        buf
    }
);

sqlfunc!(
    #[sqlname = "interval_to_time"]
    fn cast_interval_to_time(mut i: Interval) -> NaiveTime {
        // Negative durations have their HH::MM::SS.NS values subtracted from 1 day.
        if i.is_negative() {
            i = Interval::new(0, 0, 86_400_000_000)
                .checked_add(&i.as_time_interval())
                .unwrap();
        }

        NaiveTime::from_hms_nano(
            i.hours() as u32,
            i.minutes() as u32,
            i.seconds::<f64>() as u32,
            i.nanoseconds() as u32,
        )
    }
);

sqlfunc!(
    #[sqlname = "-"]
    #[preserves_uniqueness = true]
    fn neg_interval(i: Interval) -> Result<Interval, EvalError> {
        i.checked_neg().ok_or(EvalError::IntervalOutOfRange)
    }
);

sqlfunc!(
    #[sqlname = "justify_days"]
    fn justify_days(i: Interval) -> Result<Interval, EvalError> {
        i.justify_days().map_err(|_| EvalError::IntervalOutOfRange)
    }
);

sqlfunc!(
    #[sqlname = "justify_hours"]
    fn justify_hours(i: Interval) -> Result<Interval, EvalError> {
        i.justify_hours().map_err(|_| EvalError::IntervalOutOfRange)
    }
);

sqlfunc!(
    #[sqlname = "justify_interval"]
    fn justify_interval(i: Interval) -> Result<Interval, EvalError> {
        i.justify_interval()
            .map_err(|_| EvalError::IntervalOutOfRange)
    }
);
