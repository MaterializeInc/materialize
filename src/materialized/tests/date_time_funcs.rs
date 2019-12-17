// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use chrono::{DateTime, Utc};

pub mod util;

#[test]
fn test_current_timestamp_and_now() -> util::TestResult {
    ore::log::init();

    let (_server, mut client) = util::start_server(util::Config::default())?;

    let rows = &client.query("SELECT now()", &[])?;
    assert_eq!(1, rows.len());
    let first_row = rows.get(0).expect("first row");
    // Confirm that `now()` returns a DateTime<Utc>, don't assert a specific time.
    let _now: DateTime<Utc> = first_row.get(0);

    let rows = &client.query("SELECT now(), (SELECT now())", &[])?;
    assert_eq!(1, rows.len());
    let first_row = rows.get(0).expect("first row");
    // Confirm calls to now() will use the same DateTime<Utc> from the QueryContext
    let first_now: DateTime<Utc> = first_row.get(0);
    let second_now: DateTime<Utc> = first_row.get(1);
    assert_eq!(first_now, second_now);

    let rows = &client.query("SELECT current_timestamp()", &[])?;
    assert_eq!(1, rows.len());
    let first_row = rows.get(0).expect("first row");
    // Confirm that `current_timestamp()` returns a DateTime<Utc>, don't assert a specific time.
    let _current_timestamp: DateTime<Utc> = first_row.get(0);

    Ok(())
}

#[test]
fn test_to_char() -> util::TestResult {
    ore::log::init();

    let (_server, mut client) = util::start_server(util::Config::default())?;

    // Only works for these two specific examples:
    //     1. select to_char(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.MS TZ')
    //     2. select to_char(now(), 'YYYY-MM-DD HH24:MI:SS.MS TZ')
    // Anything else will require more work to implement.
    let rows = &client.query(
        "SELECT to_char(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.MS TZ')",
        &[],
    )?;
    assert_eq!(1, rows.len());
    // Confirm that `to_char(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.MS TZ')` returns a String, don't assert a specific time.
    let _current_time_string: String = rows.get(0).expect("a row").get(0);

    let rows = &client.query("SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS.MS TZ')", &[])?;
    assert_eq!(1, rows.len());
    // Confirm that `to_char(now(), 'YYYY-MM-DD HH24:MI:SS.MS TZ')` returns a String, don't assert a specific time.
    let _current_time_string: String = rows.get(0).expect("a row").get(0);

    Ok(())
}
