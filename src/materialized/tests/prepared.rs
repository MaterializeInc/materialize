// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use fallible_iterator::FallibleIterator;
use itertools::Itertools;

pub mod util;

#[test]
fn test_prepared_statements() -> util::TestResult {
    ore::log::init();

    let (_server, conn) = util::start_server(util::Config::default())?;

    let rows: Vec<String> = conn
        .query("SELECT $1", &[&String::from("42")])?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(rows, &["42"]);

    let rows: Vec<i64> = conn
        .query("SELECT $1 + 1", &[&42_i64])?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(rows, &[43]);

    let rows: Vec<i64> = conn
        .query("SELECT $1 - 1", &[&42_i64])?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(rows, &[41]);

    let rows: Vec<i64> = conn
        .query("SELECT 1 - $1", &[&42_i64])?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(rows, &[-41]);

    Ok(())
}

#[test]
fn test_partial_read() -> util::TestResult {
    ore::log::init();

    let (_server, conn) = util::start_server(util::Config::default())?;
    let query = "VALUES ('1'), ('2'), ('3'), ('4'), ('5'), ('6'), ('7')";

    let simpler = conn.query(query, &[])?;

    // TODO: when we migrate to rust-postgres 0.17 we can verify that we are
    // only getting one row back at a time.
    let max_rows = 1;
    let stmt = conn.prepare(query)?;
    let trans = conn.transaction()?;
    let rows_lazily: Vec<_> = stmt.lazy_query(&trans, &[], max_rows)?.collect()?;

    for (eagerly_row, lazily_row) in simpler.iter().zip_eq(rows_lazily) {
        let eagerly: String = eagerly_row.get(0);
        let lazily: String = lazily_row.get(0);
        assert_eq!(eagerly, lazily);
    }

    Ok(())
}
