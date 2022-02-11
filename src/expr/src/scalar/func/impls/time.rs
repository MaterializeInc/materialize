// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{NaiveTime, Timelike};

use mz_repr::adt::interval::Interval;
use mz_repr::strconv;

use crate::EvalError;

sqlfunc!(
    #[sqlname = "timetostr"]
    #[preserves_uniqueness = true]
    fn cast_time_to_string(a: NaiveTime) -> String {
        let mut buf = String::new();
        strconv::format_time(&mut buf, a);
        buf
    }
);

sqlfunc!(
    #[sqlname = "timetoiv"]
    #[preserves_uniqueness = true]
    fn cast_time_to_interval<'a>(t: NaiveTime) -> Result<Interval, EvalError> {
        Interval::new(
            0,
            t.num_seconds_from_midnight() as i64,
            t.nanosecond().into(),
        )
        .map_err(|_| EvalError::IntervalOutOfRange)
    }
);
