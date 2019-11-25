// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

mod util;

#[test]
fn test_current_timestamp_and_now() -> util::TestResult {
    ore::log::init();

    let data_dir = None;
    let (_server, conn) = util::start_server(data_dir)?;

    let rows = &conn.query("SELECT now()", &[])?;
    assert_eq!(1, rows.len());
    let first_row = rows.get(0);
    // Confirm that `now()` returns a DateTime<Utc>, don't assert a specific time.
    let _now: DateTime<Utc> = first_row.get(0);

    let rows = &conn.query("SELECT now(), (SELECT now())", &[])?;
    assert_eq!(1, rows.len());
    let first_row = rows.get(0);
    // Confirm calls to now() will use the same DateTime<Utc> from the QueryContext
    let first_now: DateTime<Utc> = first_row.get(0);
    let second_now: DateTime<Utc> = first_row.get(1);
    assert_eq!(first_now, second_now);

    let rows = &conn.query("SELECT current_timestamp()", &[])?;
    assert_eq!(1, rows.len());
    let first_row = rows.get(0);
    // Confirm that `current_timestamp()` returns a DateTime<Utc>, don't assert a specific time.
    let _current_timestamp: DateTime<Utc> = first_row.get(0);

    Ok(())
}
