// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for Materialize server.

use std::collections::HashMap;
use std::error::Error;
use std::thread;
use std::time::Duration;

use reqwest::{blocking::Client, StatusCode, Url};
use tempfile::NamedTempFile;

use crate::util::{PostgresErrorExt, KAFKA_ADDRS};

pub mod util;

#[test]
fn test_persistence() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let data_dir = tempfile::tempdir()?;
    let config = util::Config::default().data_directory(data_dir.path());

    let source_file = NamedTempFile::new()?;

    {
        let server = util::start_server(config.clone())?;
        let mut client = server.connect(postgres::NoTls)?;
        client.batch_execute(&format!(
            "CREATE SOURCE src FROM FILE '{}' FORMAT BYTES",
            source_file.path().display()
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

// Ensures that once a node is started with `--experimental`, it requires
// `--experimental` on reboot.
#[test]
fn test_experimental_mode_reboot() -> Result<(), Box<dyn Error>> {
    let data_dir = tempfile::tempdir()?;
    let config = util::Config::default().data_directory(data_dir.path());

    {
        let _ = util::start_server(config.clone().experimental_mode())?;
    }

    {
        match util::start_server(config.clone()) {
            Ok(_) => panic!("unexpected success"),
            Err(e) => {
                if !e
                    .to_string()
                    .contains("Materialize previously started with --experimental")
                {
                    return Err(e.into());
                }
            }
        }
    }

    {
        let _ = util::start_server(config.experimental_mode())?;
    }

    Ok(())
}

// Ensures that only new nodes can start in experimental mode.
#[test]
fn test_experimental_mode_on_init_or_never() -> Result<(), Box<dyn Error>> {
    let data_dir = tempfile::tempdir()?;
    let config = util::Config::default().data_directory(data_dir.path());

    {
        let _ = util::start_server(config.clone())?;
    }

    {
        match util::start_server(config.experimental_mode()) {
            Ok(_) => panic!("unexpected success"),
            Err(e) => {
                if !e
                    .to_string()
                    .contains("Experimental mode is only available on new nodes")
                {
                    return Err(e.into());
                }
            }
        }
    }

    Ok(())
}

#[test]
fn test_pid_file() -> Result<(), Box<dyn Error>> {
    let data_dir = tempfile::tempdir()?;
    let config = util::Config::default().data_directory(data_dir.path());

    // While `server1` is running, it should not be possible to start another
    // server against the same data directory.
    let server1 = util::start_server(config.clone())?;
    match util::start_server(config.clone()) {
        Ok(_) => panic!("unexpected success"),
        Err(e) => {
            if !e
                .to_string()
                .contains("running with the same data directory")
            {
                return Err(e.into());
            }
        }
    }

    // But it should be possible to start a server against a *different*
    // data directory.
    let _server2 = util::start_server(util::Config::default())?;

    // Stopping `server1` should allow starting another server against the
    // `server1`'s old data directory.
    drop(server1);
    util::start_server(config)?;

    Ok(())
}

#[test]
fn test_safe_mode() -> Result<(), Box<dyn Error>> {
    let server = util::start_server(util::Config::default().safe_mode())?;
    let mut client = server.connect(postgres::NoTls)?;

    // No file sources or sinks.
    let err = client
        .batch_execute("CREATE SOURCE src FROM FILE '/ignored' FORMAT BYTES")
        .unwrap_db_error();
    assert_eq!(err.message(), "cannot create file source in safe mode");
    let err = client
        .batch_execute("CREATE SINK snk FROM mz_sources INTO FILE '/ignored' FORMAT BYTES")
        .unwrap_db_error();
    assert_eq!(err.message(), "Expected one of KAFKA or AVRO, found FILE");

    // No Avro OCF sources or sinks.
    let err = client
        .batch_execute("CREATE SOURCE src FROM AVRO OCF '/ignored'")
        .unwrap_db_error();
    assert_eq!(err.message(), "cannot create Avro OCF source in safe mode");
    let err = client
        .batch_execute("CREATE SINK snk FROM mz_sources INTO AVRO OCF '/ignored'")
        .unwrap_db_error();
    assert_eq!(err.message(), "cannot create Avro OCF sink in safe mode");

    // No Kerberos-authenticated Kafka sources or sinks.
    let err = client
        .batch_execute(
            "CREATE SOURCE src
            FROM KAFKA BROKER 'ignored' TOPIC 'ignored'
            WITH (security_protocol = 'sasl_plaintext')",
        )
        .unwrap_db_error();
    assert_eq!(
        err.message(),
        "cannot create Kerberos-authenticated Kafka source in safe mode"
    );
    let err = client
        .batch_execute(
            "CREATE SINK src FROM mz_sources
            INTO KAFKA BROKER 'ignored' TOPIC 'ignored'
            WITH (security_protocol = 'sasl_plaintext')",
        )
        .unwrap_db_error();
    assert_eq!(
        err.message(),
        "cannot create Kerberos-authenticated Kafka sink in safe mode"
    );

    // Non-Kerberos Kafka sources are okay though.
    client.batch_execute(&*format!(
        "CREATE SOURCE src
        FROM KAFKA BROKER '{}' TOPIC 'foo'
        FORMAT BYTES",
        &*KAFKA_ADDRS,
    ))?;

    Ok(())
}

// Test the /sql POST endpoint of the HTTP server.
#[test]
fn test_http_sql() -> Result<(), Box<dyn Error>> {
    let server = util::start_server(util::Config::default())?;
    let url = Url::parse(&format!("http://{}/sql", server.inner.local_addr()))?;
    let mut params = HashMap::new();

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
        // CREATEs should not work.
        TestCase {
            query: "create view v as select 1",
            status: StatusCode::BAD_REQUEST,
            body: r#"CREATE VIEW v AS SELECT 1 cannot be run inside a transaction block"#,
        },
    ];

    for tc in tests {
        params.insert("sql", tc.query);
        let res = Client::new().post(url.clone()).form(&params).send()?;
        assert_eq!(res.status(), tc.status);
        assert_eq!(res.text()?, tc.body);
    }

    Ok(())
}

#[test]
fn test_metrics_registry_hygiene() -> Result<(), Box<dyn Error>> {
    // Minor setup chores to ensure the server has done at least a little work:
    let server = util::start_server(util::Config::default())?;
    let source_file = NamedTempFile::new()?;
    let mut client = server.connect(postgres::NoTls)?;
    client.batch_execute(&format!(
        "CREATE SOURCE src FROM FILE '{}' FORMAT BYTES",
        source_file.path().display()
    ))?;
    client.batch_execute(
        "CREATE MATERIALIZED VIEW mat (a, a_data, c, c_data) AS SELECT 'a', data, 'c' AS c, data FROM src",
    )?;

    // Check that metrics are where we expect them:
    let default_metrics = prometheus::default_registry().gather();
    assert_eq!(0, default_metrics.len());
    assert_ne!(0, server.metrics_registry.gather().len());
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

    match client.simple_query("SELECT * FROM t AS OF now()+'1h'") {
        Err(e) if e.code() == Some(&postgres::error::SqlState::QUERY_CANCELED) => {}
        Err(e) => panic!("expected error SqlState::QUERY_CANCELED, but got {:?}", e),
        Ok(_) => panic!("expected error SqlState::QUERY_CANCELED, but query succeeded"),
    }

    client
        .simple_query("SELECT 1")
        .expect("simple query succeeds after cancellation");

    Ok(())
}
