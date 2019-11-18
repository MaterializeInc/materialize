// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use chrono::NaiveDateTime;
use std::error::Error;

mod util;

#[test]
fn test_now() -> Result<(), Box<dyn Error>> {
    ore::log::init();

    let (_server, conn) = util::start_server()?;

    let rows = &conn.query("SELECT now()", &[])?;
    assert_eq!(1, rows.len());
    // Confirm that `now()` returns a NaiveDateTime, don't assert a specific time.
    let _now: NaiveDateTime = rows.get(0).get(0);

    Ok(())
}
