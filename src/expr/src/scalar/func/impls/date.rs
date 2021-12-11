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
    #[sqlname = "datetostr"]
    #[preserves_uniqueness = true]
    fn cast_date_to_string(a: NaiveDate) -> String {
        let mut buf = String::new();
        strconv::format_date(&mut buf, a);
        buf
    }
);

sqlfunc!(
    #[sqlname = "datetots"]
    #[preserves_uniqueness = true]
    fn cast_date_to_timestamp(a: NaiveDate) -> NaiveDateTime {
        a.and_hms(0, 0, 0)
    }
);

sqlfunc!(
    #[sqlname = "datetotstz"]
    #[preserves_uniqueness = true]
    fn cast_date_to_timestamp_tz(a: NaiveDate) -> DateTime<Utc> {
        DateTime::<Utc>::from_utc(a.and_hms(0, 0, 0), Utc)
    }
);
