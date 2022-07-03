// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for Materialize server.

use std::error::Error;
use std::thread;
use std::time::Duration;

use mz_ore::retry::Retry;
use reqwest::{blocking::Client, StatusCode, Url};
use serde_json::json;

use crate::util::KAFKA_ADDRS;

pub mod util;

#[test]
fn test_persistence() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let data_dir = tempfile::tempdir()?;
    let config = util::Config::default().data_directory(data_dir.path());

    {
        let server = util::start_server(config.clone())?;
        let mut client = server.connect(postgres::NoTls)?;
        client.batch_execute(&format!(
            "CREATE SOURCE src FROM KAFKA BROKER '{}' TOPIC 'ignored' FORMAT BYTES",
            &*KAFKA_ADDRS,
        ))?;
        client.batch_execute("CREATE VIEW constant AS SELECT 1")?;
        client.batch_execute(
            "CREATE VIEW logging_derived AS SELECT * FROM mz_catalog.mz_arrangement_sizes",
        )?;
        client.batch_execute(
            "CREATE MATERIALIZED VIEW mat (a, a_data, c, c_data) AS SELECT 'a', data, 'c' AS c, data FROM src",
        )?;
        client.batch_execute("CREATE DATABASE d")?;
        client.batch_execute("CREATE SCHEMA d.s")?;
        client.batch_execute("CREATE VIEW d.s.v AS SELECT 1")?;
    }

    for config in [config.clone(), config.logging_granularity(None)] {
        let server = util::start_server(config)?;
        let mut client = server.connect(postgres::NoTls)?;
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
                ("a".into(), 1),
                ("a_data".into(), 2),
                ("c".into(), 3),
                ("c_data".into(), 4),
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

        // Test that catalog recovery correctly populates `mz_catalog_names`.
        assert_eq!(
            client
                .query(
                    "SELECT global_id FROM mz_catalog_names WHERE global_id LIKE 'u%' ORDER BY 1",
                    &[]
                )?
                .into_iter()
                .map(|row| row.get(0))
                .collect::<Vec<String>>(),
            vec!["u1", "u2", "u3", "u4", "u5", "u6"]
        );
    }

    Ok(())
}

// Test the /sql POST endpoint of the HTTP server.
#[test]
fn test_http_sql() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();
    let server = util::start_server(util::Config::default())?;
    let url = Url::parse(&format!(
        "http://{}/api/sql",
        server.inner.http_local_addr()
    ))?;

    struct TestCase {
        query: &'static str,
        status: StatusCode,
        body: &'static str,
    }

    let tests = vec![
        // Regular query works.
        TestCase {
            query: "select 1+2 as col",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[3]],"col_names":["col"]}]}"#,
        },
        // Multiple queries are ok.
        TestCase {
            query: "select 1; select 2",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["?column?"]},{"rows":[[2]],"col_names":["?column?"]}]}"#,
        },
        // Succeeding and failing queries can mix and match.
        TestCase {
            query: "select 1; select * from noexist;",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["?column?"]},{"error":"unknown catalog item 'noexist'"}]}"#,
        },
        // CREATEs should work when provided alone.
        TestCase {
            query: "create view v as select 1",
            status: StatusCode::OK,
            body: r#"{"results":[null]}"#,
        },
        // Multiple CREATEs do not work.
        TestCase {
            query: "create view v1 as select 1; create view v2 as select 1",
            status: StatusCode::OK,
            body: r#"{"results":[{"error":"CREATE VIEW v1 AS SELECT 1 cannot be run inside a transaction block"},{"error":"CREATE VIEW v2 AS SELECT 1 cannot be run inside a transaction block"}]}"#,
        },
        // Syntax errors fail the request.
        TestCase {
            query: "'",
            status: StatusCode::BAD_REQUEST,
            body: r#"unterminated quoted string"#,
        },
    ];

    for tc in tests {
        let res = Client::new()
            .post(url.clone())
            .json(&json!({"sql": tc.query}))
            .send()?;
        assert_eq!(res.status(), tc.status);
        assert_eq!(res.text()?, tc.body);
    }

    Ok(())
}

// Test that the server properly handles cancellation requests.
#[test]
fn test_cancel_long_running_query() -> Result<(), Box<dyn Error>> {
    let config = util::Config::default();
    let server = util::start_server(config)?;

    let mut client = server.connect(postgres::NoTls)?;
    let cancel_token = client.cancel_token();

    thread::spawn(move || {
        // Abort the query after 2s.
        thread::sleep(Duration::from_secs(2));
        let _ = cancel_token.cancel_query(postgres::NoTls);
    });

    client.batch_execute("CREATE TABLE t (i INT)")?;

    match client.simple_query("SELECT * FROM t AS OF 18446744073709551615") {
        Err(e) if e.code() == Some(&postgres::error::SqlState::QUERY_CANCELED) => {}
        Err(e) => panic!("expected error SqlState::QUERY_CANCELED, but got {:?}", e),
        Ok(_) => panic!("expected error SqlState::QUERY_CANCELED, but query succeeded"),
    }

    client
        .simple_query("SELECT 1")
        .expect("simple query succeeds after cancellation");

    Ok(())
}

// Test that dataflow uninstalls cancelled peeks.
#[test]
fn test_cancel_dataflow_removal() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let config = util::Config::default();
    let server = util::start_server(config)?;

    let mut client1 = server.connect(postgres::NoTls)?;
    let mut client2 = server.connect(postgres::NoTls)?;
    let cancel_token = client1.cancel_token();

    client1.batch_execute("CREATE TABLE t (i INT)")?;
    // No dataflows expected at startup.
    assert_eq!(
        client1
            .query_one("SELECT count(*) FROM mz_dataflow_operators", &[])?
            .get::<_, i64>(0),
        0
    );

    thread::spawn(move || {
        // Wait until we see the expected dataflow.
        Retry::default()
            .retry(|_state| {
                let count: i64 = client2
                    .query_one("SELECT count(*) FROM mz_dataflow_operators", &[])
                    .map_err(|_| ())?
                    .get(0);
                if count == 0 {
                    Err(())
                } else {
                    Ok(())
                }
            })
            .unwrap();
        cancel_token.cancel_query(postgres::NoTls).unwrap();
    });

    match client1.simple_query("SELECT * FROM t AS OF 18446744073709551615") {
        Err(e) if e.code() == Some(&postgres::error::SqlState::QUERY_CANCELED) => {}
        Err(e) => panic!("expected error SqlState::QUERY_CANCELED, but got {:?}", e),
        Ok(_) => panic!("expected error SqlState::QUERY_CANCELED, but query succeeded"),
    }
    // Expect the dataflows to shut down.
    Retry::default()
        .retry(|_state| {
            let count: i64 = client1
                .query_one("SELECT count(*) FROM mz_dataflow_operators", &[])
                .map_err(|_| ())?
                .get(0);
            if count == 0 {
                Ok(())
            } else {
                Err(())
            }
        })
        .unwrap();

    Ok(())
}
