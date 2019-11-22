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

#[test]
fn test_date_trunc() -> util::TestResult {
    ore::log::init();

    let data_dir = None;
    let (_server, conn) = util::start_server(data_dir)?;

    let rows = &conn.query(
        "SELECT date_trunc('hour', TIMESTAMP '2001-02-16 20:38:40')",
        &[],
    )?;
    assert_eq!(1, rows.len());
    let d = NaiveDate::from_ymd(2001, 02, 16);
    let t = NaiveTime::from_hms(20, 0, 0);
    let dt = NaiveDateTime::new(d, t);
    assert_eq!(dt, rows.get(0).get(0));

    // Output from psql:
    //    jessicalaughlin=# SELECT date_trunc('week', TIMESTAMP '2001-02-16 20:38:40');
    //    date_trunc
    //        ---------------------
    //        2001-02-12 00:00:00
    let rows = &conn.query(
        "SELECT date_trunc('week', TIMESTAMP '2001-02-16 20:38:40')",
        &[],
    )?;
    assert_eq!(1, rows.len());
    let d = NaiveDate::from_ymd(2001, 02, 12);
    let t = NaiveTime::from_hms(0, 0, 0);
    let dt = NaiveDateTime::new(d, t);
    assert_eq!(dt, rows.get(0).get(0));

    // Output from psql:
    //    jessicalaughlin=# SELECT date_trunc('quarter', TIMESTAMP '2001-02-16 20:38:40');
    //    date_trunc
    //        ---------------------
    //        2001-01-01 00:00:00
    let rows = &conn.query(
        "SELECT date_trunc('quarter', TIMESTAMP '2001-02-16 20:38:40')",
        &[],
    )?;
    assert_eq!(1, rows.len());
    let d = NaiveDate::from_ymd(2001, 01, 01);
    let t = NaiveTime::from_hms(0, 0, 0);
    let dt = NaiveDateTime::new(d, t);
    assert_eq!(dt, rows.get(0).get(0));

    // Output from psql:
    //    jessicalaughlin=# SELECT date_trunc('century', TIMESTAMP '2019-02-16 20:38:40');
    //    date_trunc
    //        ---------------------
    //        2001-01-01 00:00:00
    let rows = &conn.query(
        "SELECT date_trunc('century', TIMESTAMP '2001-02-16 20:38:40')",
        &[],
    )?;
    assert_eq!(1, rows.len());
    let d = NaiveDate::from_ymd(2001, 01, 01);
    let t = NaiveTime::from_hms(0, 0, 0);
    let dt = NaiveDateTime::new(d, t);
    assert_eq!(dt, rows.get(0).get(0));

    // Output from psql:
    //    jessicalaughlin=# SELECT date_trunc('millennium', TIMESTAMP '2991-02-16 20:38:40');
    //    date_trunc
    //        ---------------------
    //        2001-01-01 00:00:00
    let rows = &conn.query(
        "SELECT date_trunc('millennium', TIMESTAMP '2991-02-16 20:38:40')",
        &[],
    )?;
    assert_eq!(1, rows.len());
    let d = NaiveDate::from_ymd(2001, 01, 01);
    let t = NaiveTime::from_hms(0, 0, 0);
    let dt = NaiveDateTime::new(d, t);
    assert_eq!(dt, rows.get(0).get(0));

    Ok(())
}
