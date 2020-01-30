// Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Integration tests for pgwire functionality.

use std::error::Error;
use std::thread;
use std::time::Duration;

use postgres::error::SqlState;

pub mod util;

#[test]
fn test_bind_params() -> Result<(), Box<dyn Error>> {
    ore::log::init();

    let (_server, mut client) = util::start_server(util::Config::default())?;

    // Simple queries with parameters should be rejected.
    match client.simple_query("SELECT $1") {
        Ok(_) => panic!("query with invalid parameters executed successfully"),
        Err(err) => assert_eq!(err.code(), Some(&SqlState::UNDEFINED_PARAMETER)),
    }

    let rows: Vec<String> = client
        .query("SELECT $1", &[&String::from("42")])?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(rows, &["42"]);

    let rows: Vec<i32> = client
        .query("SELECT $1 + 1", &[&42_i32])?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(rows, &[43]);

    let rows: Vec<i32> = client
        .query("SELECT $1 - 1", &[&42_i32])?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(rows, &[41]);

    let rows: Vec<i32> = client
        .query("SELECT 1 - $1", &[&42_i32])?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(rows, &[-41]);

    Ok(())
}

#[test]
fn test_partial_read() -> Result<(), Box<dyn Error>> {
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
fn test_read_many_rows() -> Result<(), Box<dyn Error>> {
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

#[test]
fn test_conn_params() -> Result<(), Box<dyn Error>> {
    ore::log::init();

    let (server, mut client) = util::start_server(util::Config::default())?;

    // The default database should be `materialize`.
    assert_eq!(
        client.query_one("SHOW database", &[])?.get::<_, String>(0),
        "materialize",
    );

    // Connecting to a nonexistent database should work, and creating that
    // database should work.
    {
        let mut client = server
            .pg_config()
            .dbname("newdb")
            .connect(postgres::NoTls)?;
        assert_eq!(
            client.query_one("SHOW database", &[])?.get::<_, String>(0),
            "newdb",
        );
        client.batch_execute("CREATE DATABASE newdb")?;
        client.batch_execute("CREATE MATERIALIZED VIEW v AS SELECT 1")?;
    }

    // Connecting to an existing database should work.
    {
        let mut client = server
            .pg_config()
            .dbname("newdb")
            .connect(postgres::NoTls)?;

        // Sleep a little bit so the view catches up.
        // TODO(benesch): seriously? It's a view over a static query.
        thread::sleep(Duration::from_millis(100));

        assert_eq!(
            // `v` here should refer to the `v` in `newdb.public` that we
            // created above.
            client.query_one("SELECT * FROM v", &[])?.get::<_, i32>(0),
            1,
        );
    }

    // Setting the application name at connection time should be respected.
    {
        let mut client = server
            .pg_config()
            .application_name("hello")
            .connect(postgres::NoTls)?;
        assert_eq!(
            client
                .query_one("SHOW application_name", &[])?
                .get::<_, String>(0),
            "hello",
        );
    }

    Ok(())
}

#[test]
fn test_persistence() -> Result<(), Box<dyn Error>> {
    ore::log::init();

    let data_directory = tempfile::tempdir()?;
    let config = util::Config::default()
        .data_directory(data_directory.path().to_owned())
        .bootstrap_sql(
            "CREATE VIEW bootstrap1 AS SELECT 1;
             CREATE VIEW bootstrap2 AS SELECT * FROM bootstrap1;",
        );

    {
        let (_server, mut client) = util::start_server(config.clone())?;
        // TODO(benesch): when file sources land, use them here. Creating a
        // populated Kafka source here is too annoying.
        client.batch_execute("CREATE VIEW constant AS SELECT 1")?;
        client.batch_execute(
            "CREATE VIEW logging_derived AS SELECT * FROM mz_catalog.mz_arrangement_sizes",
        )?;
        client.batch_execute("CREATE DATABASE d")?;
        client.batch_execute("CREATE SCHEMA d.s")?;
        client.batch_execute("CREATE VIEW d.s.v AS SELECT 1")?;
    }

    {
        let (_server, mut client) = util::start_server(config)?;
        assert_eq!(
            client
                .query("SHOW VIEWS", &[])?
                .into_iter()
                .map(|row| row.get(0))
                .collect::<Vec<String>>(),
            &["bootstrap1", "bootstrap2", "constant", "logging_derived"]
        );
        assert_eq!(
            client
                .query("SHOW VIEWS FROM d.s", &[])?
                .into_iter()
                .map(|row| row.get(0))
                .collect::<Vec<String>>(),
            &["v"]
        );
    }

    Ok(())
}
