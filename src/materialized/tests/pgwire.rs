// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for pgwire functionality.

use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::thread;
use std::time::Duration;

use futures::stream::{self, StreamExt, TryStreamExt};
use postgres::error::SqlState;
use tokio::runtime::Runtime;

pub mod util;

#[test]
fn test_bind_params() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

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

    // Just ensure it does not panic (see #2498).
    client.query("EXPLAIN PLAN FOR SELECT $1::int", &[&42_i32])?;

    Ok(())
}

#[test]
fn test_partial_read() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

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
    ore::test::init_logging();

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
    ore::test::init_logging();

    let (server, mut client) = util::start_server(util::Config::default())?;

    // The default database should be `materialize`.
    assert_eq!(
        client.query_one("SHOW database", &[])?.get::<_, String>(0),
        "materialize",
    );

    // Connecting to a nonexistent database should work, and creating that
    // database should work.
    //
    // TODO(benesch): we can use the sync client when this issue is fixed:
    // https://github.com/sfackler/rust-postgres/issues/404.
    Runtime::new()?.block_on(async {
        let (client, mut conn) = server
            .pg_config_async()
            .dbname("newdb")
            .connect(postgres::NoTls)
            .await?;
        let (notice_tx, mut notice_rx) = futures::channel::mpsc::unbounded();
        tokio::spawn(
            stream::poll_fn(move |cx| conn.poll_message(cx))
                .map_err(|e| panic!(e))
                .forward(notice_tx),
        );

        assert_eq!(
            client
                .query_one("SHOW database", &[])
                .await?
                .get::<_, String>(0),
            "newdb",
        );
        client
            .batch_execute(
                "CREATE DATABASE newdb; \
                 CREATE MATERIALIZED VIEW v AS SELECT 1;",
            )
            .await?;

        match notice_rx.next().await {
            Some(tokio_postgres::AsyncMessage::Notice(n)) => {
                assert_eq!(*n.code(), SqlState::SUCCESSFUL_COMPLETION);
                assert_eq!(n.message(), "session database \'newdb\' does not exist");
            }
            _ => panic!("missing database notice not generated"),
        }

        Ok::<_, Box<dyn Error>>(())
    })?;

    // Connecting to an existing database should work.
    {
        let mut client = server
            .pg_config()
            .dbname("newdb")
            .connect(postgres::NoTls)?;

        // Sleep a little bit so the view catches up.
        // TODO(benesch): seriously? It's a view over a static query.
        // HISTORICAL NOTE(brennan): The above comment was left when this was only 500 millis.
        // WTF is going on here?
        thread::sleep(Duration::from_millis(2000));

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
fn test_multiple_statements() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();
    let (_server, mut client) = util::start_server(util::Config::default())?;
    let result = client.batch_execute(
        "CREATE VIEW v1 AS SELECT * FROM (VALUES (1)); \
         CREATE VIEW v2 AS SELECT * FROM (VALUES (2)); \
         CREATE VIEW v3 AS SELECT sum(column1) FROM (SELECT * FROM v1 UNION SELECT * FROM v2); \
         CREATE VIEW v4 AS SELECT * FROM nonexistent; \
         CREATE VIEW v5 AS SELECT 5;",
    );

    // v4 creation fails, so the whole query should be an error.
    assert!(result.is_err());

    assert_eq!(
        client.query_one("SELECT * FROM v1", &[])?.get::<_, i32>(0),
        1,
    );

    assert_eq!(
        client.query_one("SELECT * FROM v2", &[])?.get::<_, i32>(0),
        2,
    );

    assert_eq!(
        client.query_one("SELECT * FROM v3", &[])?.get::<_, i32>(0),
        3,
    );

    assert!(client.query_one("SELECT * FROM v4", &[]).is_err());

    // the statement to create v5 is correct, but it should not have been executed, since v4 failed to create.
    assert!(client.query_one("SELECT * from v5", &[]).is_err());

    Ok(())
}

#[test]
fn test_simple_query_no_hang() -> Result<(), Box<dyn Error>> {
    let (_server, mut client) = util::start_server(util::Config::default())?;
    assert!(client.simple_query("asdfjkl;").is_err());
    // This will hang if #2880 is not fixed.
    assert!(client.simple_query("SELECT 1").is_ok());

    Ok(())
}

#[test]
fn test_persistence() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let data_dir = tempfile::tempdir()?;
    let config = util::Config::default().data_directory(data_dir.path().to_owned());

    let temp_dir = tempfile::tempdir()?;
    let temp_file = Path::join(temp_dir.path(), "source.txt");
    File::create(&temp_file)?;

    {
        let (_server, mut client) = util::start_server(config.clone())?;
        client.batch_execute(&format!(
            "CREATE SOURCE src FROM FILE '{}' FORMAT BYTES; \
             CREATE VIEW constant AS SELECT 1; \
             CREATE VIEW logging_derived AS SELECT * FROM mz_catalog.mz_arrangement_sizes; \
             CREATE MATERIALIZED VIEW mat AS SELECT 'a', data, 'c' AS c, data FROM src; \
             CREATE DATABASE d; \
             CREATE SCHEMA d.s; \
             CREATE VIEW d.s.v AS SELECT 1;",
            temp_file.display(),
        ))?;
    }

    {
        let (_server, mut client) = util::start_server(config.clone())?;
        assert_eq!(
            client
                .query("SHOW VIEWS", &[])?
                .into_iter()
                .map(|row| row.get(0))
                .collect::<Vec<String>>(),
            &["constant", "logging_derived", "mat"]
        );
        assert_eq!(
            client
                .query("SHOW INDEXES FROM mat", &[])?
                .into_iter()
                .map(|row| (row.get("Column_name"), row.get("Seq_in_index")))
                .collect::<Vec<(String, i64)>>(),
            &[
                ("@1".into(), 1),
                ("@2".into(), 2),
                ("@4".into(), 4),
                ("c".into(), 3),
            ],
        );
        assert_eq!(
            client
                .query("SHOW VIEWS FROM d.s", &[])?
                .into_iter()
                .map(|row| row.get(0))
                .collect::<Vec<String>>(),
            &["v"]
        );

        // Test that catalog recovery correctly populates `mz_catalog_names`
        // This test is racy, because the effects are not necessarily immediately visible.
        std::thread::sleep(std::time::Duration::from_secs(1));
        assert_eq!(
            client
                .query("SELECT * FROM mz_catalog_names", &[])?
                .into_iter()
                .map(|row| row.get(0))
                .collect::<Vec<String>>(),
            vec![
                "u6", "u1", "u4", "s27", "s27", "s23", "s23", "s55", "s55", "u2", "s31", "s31",
                "s25", "s25", "s35", "s57", "s57", "s3", "s3", "s11", "s11", "s17", "s17", "s1",
                "s1", "s5", "s5", "s13", "s13", "s29", "s29", "s7", "s7", "u3", "u5", "s15", "s15",
                "s41", "s28", "s24", "s56", "s47", "s49", "s21", "s21", "s32", "s45", "s9", "s9",
                "s26", "s33", "s36", "s51", "s58", "s37", "s43", "s4", "s12", "s18", "s19", "s19",
                "s2", "s6", "s14", "s30", "s39", "s53", "s8", "s16", "s42", "s48", "s50", "s22",
                "s46", "s34", "s52", "s10", "s38", "s44", "s20", "s40", "s54"
            ]
        );
    }

    {
        let config = config.logging_granularity(None);
        match util::start_server(config) {
            Ok(_) => panic!("server unexpectedly booted with corrupted catalog"),
            Err(e) => assert_eq!(
                e.to_string(),
                "catalog item 'materialize.public.logging_derived' depends on system logging, \
                 but logging is disabled"
            ),
        }
    }

    Ok(())
}
