// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

pub mod util;

#[test]
fn test_bind_params() -> util::TestResult {
    ore::log::init();

    let (_server, mut client) = util::start_server(util::Config::default())?;

    let rows: Vec<String> = client
        .query("SELECT $1", &[&String::from("42")])?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(rows, &["42"]);

    let rows: Vec<i64> = client
        .query("SELECT $1 + 1", &[&42_i64])?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(rows, &[43]);

    let rows: Vec<i64> = client
        .query("SELECT $1 - 1", &[&42_i64])?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(rows, &[41]);

    let rows: Vec<i64> = client
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

    let (_server, mut client) = util::start_server(util::Config::default())?;
    let query = "VALUES ('1'), ('2'), ('3'), ('4'), ('5'), ('6'), ('7')";

    let simpler = client.query(query, &[])?;

    let mut simpler_iter = simpler.iter();

    let max_rows = 1;
    let mut trans = client.transaction()?;
    let portal = trans.bind(query, &[])?;
    for _ in 0..7 {
        let rows = trans.query_portal(&portal, max_rows)?;
        assert_eq!(
            rows.len(),
            max_rows as usize,
            "should get max rows each time"
        );
        let eagerly = simpler_iter.next().unwrap().get::<_, String>(0);
        let prepared: &str = rows.get(0).unwrap().get(0);
        assert_eq!(prepared, eagerly);
    }

    Ok(())
}

#[test]
fn test_read_many_rows() -> util::TestResult {
    ore::log::init();

    let (_server, mut client) = util::start_server(util::Config::default())?;
    let query = "VALUES (1), (2), (3)";

    let max_rows = 10_000;
    let mut trans = client.transaction()?;
    let portal = trans.bind(query, &[])?;
    let rows = trans.query_portal(&portal, max_rows)?;

    assert_eq!(rows.len(), 3, "row len should be all values");

    Ok(())
}
