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
    // both inside and outside of subqueries.
    let first_now: DateTime<Utc> = first_row.get(0);
    let second_now: DateTime<Utc> = first_row.get(1);
    assert_eq!(first_now, second_now);

    let rows = &client.query("SELECT current_timestamp()", &[])?;
    assert_eq!(1, rows.len());
    let first_row = rows.get(0).expect("first row");
    // Confirm that `current_timestamp()` returns a DateTime<Utc>, don't assert a specific time.
    let _current_timestamp: DateTime<Utc> = first_row.get(0);

    // Ensure that EXPLAIN selects a timestamp for `current_timestamp()`, though
    // we don't care what the timestamp is.
    let rows = &client.query("EXPLAIN PLAN FOR SELECT current_timestamp()", &[])?;
    assert_eq!(1, rows.len());

    // Ditto for `now()`.
    let rows = &client.query("EXPLAIN PLAN FOR SELECT now()", &[])?;
    assert_eq!(1, rows.len());

    Ok(())
}
