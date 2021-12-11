// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

use repr::strconv;

sqlfunc!(
    #[sqlname = "tstostr"]
    #[preserves_uniqueness = true]
    fn cast_timestamp_to_string(a: NaiveDateTime) -> String {
        let mut buf = String::new();
        strconv::format_timestamp(&mut buf, a);
        buf
    }
);

sqlfunc!(
    #[sqlname = "tstztostr"]
    #[preserves_uniqueness = true]
    fn cast_timestamp_tz_to_string(a: DateTime<Utc>) -> String {
        let mut buf = String::new();
        strconv::format_timestamptz(&mut buf, a);
        buf
    }
);

sqlfunc!(
    #[sqlname = "tstodate"]
    fn cast_timestamp_to_date(a: NaiveDateTime) -> NaiveDate {
        a.date()
    }
);

sqlfunc!(
    #[sqlname = "tstztodate"]
    fn cast_timestamp_tz_to_date(a: DateTime<Utc>) -> NaiveDate {
        a.naive_utc().date()
    }
);

sqlfunc!(
    #[sqlname = "tstotstz"]
    #[preserves_uniqueness = true]
    fn cast_timestamp_to_timestamp_tz(a: NaiveDateTime) -> DateTime<Utc> {
        DateTime::<Utc>::from_utc(a, Utc)
    }
);

sqlfunc!(
    #[sqlname = "tstztots"]
    fn cast_timestamp_tz_to_timestamp(a: DateTime<Utc>) -> NaiveDateTime {
        a.naive_utc()
    }
);
