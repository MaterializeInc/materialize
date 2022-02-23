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
        // wont overflow because value can't exceed 24 hrs + 1_000_000 ns = 86_400 seconds + 1_000_000 ns = 86_400_001_000 us
        let micros: i64 = i64::from(t.num_seconds_from_midnight())
            * i64::from(Interval::MILLISECOND_PER_SECOND)
            * i64::from(Interval::MICROSECOND_PER_MILLISECOND)
            + i64::from(t.nanosecond()) / i64::from(Interval::NANOSECOND_PER_MICROSECOND);

        Interval::new(0, 0, micros).map_err(|_| EvalError::IntervalOutOfRange)
    }
);
