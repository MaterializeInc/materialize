// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::NaiveTime;

use mz_repr::adt::interval::Interval;
use mz_repr::strconv;

sqlfunc!(
    #[sqlname = "ivtostr"]
    #[preserves_uniqueness = true]
    fn cast_interval_to_string(a: Interval) -> String {
        let mut buf = String::new();
        strconv::format_interval(&mut buf, a);
        buf
    }
);

sqlfunc!(
    #[sqlname = "ivtotime"]
    fn cast_interval_to_time(mut i: Interval) -> NaiveTime {
        // Negative durations have their HH::MM::SS.NS values subtracted from 1 day.
        if i.duration_as_chrono() < chrono::Duration::zero() {
            i = Interval::new(0, 0, 86_400_000_000)
                .unwrap()
                .checked_add(&Interval::new(0, 0, i.dur_as_microsecs()).unwrap())
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
    fn neg_interval(i: Interval) -> Interval {
        -i
    }
);
