// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::error::Error;

mod util;

#[test]
fn test_to_char() -> Result<(), Box<dyn Error>> {
    ore::log::init();

    let data_dir = None;
    let (_server, conn) = util::start_server(data_dir)?;

    // Only works for these two specific examples:
    //     1. select to_char(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.MS TZ')
    //     2. select to_char(now(), 'YYYY-MM-DD HH24:MI:SS.MS TZ')
    // Anything else will require more work to implement.
    let rows = &conn.query(
        "SELECT to_char(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.MS TZ')",
        &[],
    )?;
    assert_eq!(1, rows.len());
    // Confirm that `to_char(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.MS TZ')` returns a String, don't assert a specific time.
    let _current_time_string: String = rows.get(0).get(0);

    let rows = &conn.query("SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS.MS TZ')", &[])?;
    assert_eq!(1, rows.len());
    // Confirm that `to_char(now(), 'YYYY-MM-DD HH24:MI:SS.MS TZ')` returns a String, don't assert a specific time.
    let _current_time_string: String = rows.get(0).get(0);

    Ok(())
}
