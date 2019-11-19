// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

mod util;

#[test]
fn test_prepared_statements() -> util::TestResult {
    ore::log::init();

    let data_dir = None;
    let (_server, conn) = util::start_server(data_dir)?;

    let rows: Vec<String> = conn
        .query("SELECT $1", &[&String::from("42")])?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(rows, &["42"]);

    Ok(())
}
