// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for Materialize server.

use bytes::Buf;
use mz_environmentd::WebSocketResponse;
use std::error::Error;
use std::fmt::Write;
use std::thread;
use std::time::Duration;
use tungstenite::Message;

use mz_ore::retry::Retry;
use reqwest::{blocking::Client, Url};
use tokio_postgres::types::{FromSql, Type};

use crate::util::{PostgresErrorExt, KAFKA_ADDRS};

pub mod util;

#[derive(Debug)]
struct UInt8(u64);

impl<'a> FromSql<'a> for UInt8 {
    fn from_sql(_: &Type, mut raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let v = raw.get_u64();
        if !raw.is_empty() {
            return Err("invalid buffer size".into());
        }
        Ok(Self(v))
    }

    fn accepts(ty: &Type) -> bool {
        ty.oid() == mz_pgrepr::oid::TYPE_UINT8_OID
    }
}

#[test]
fn test_persistence() {
    let data_dir = tempfile::tempdir().unwrap();
    let config = util::Config::default()
        .data_directory(data_dir.path())
        .unsafe_mode();

    {
        let server = util::start_server(config.clone()).unwrap();
        let mut client = server.connect(postgres::NoTls).unwrap();
        client
            .batch_execute(&format!(
                "CREATE CONNECTION kafka_conn TO KAFKA (BROKER '{}')",
                &*KAFKA_ADDRS,
            ))
            .unwrap();
        client
            .batch_execute(
                "CREATE SOURCE src FROM KAFKA CONNECTION kafka_conn (TOPIC 'ignored') FORMAT BYTES",
            )
            .unwrap();
        client
            .batch_execute("CREATE VIEW constant AS SELECT 1")
            .unwrap();
        client.batch_execute(
            "CREATE VIEW mat (a, a_data, c, c_data) AS SELECT 'a', data, 'c' AS c, data FROM src",
        ).unwrap();
        client.batch_execute("CREATE DEFAULT INDEX ON mat").unwrap();
        client.batch_execute("CREATE DATABASE d").unwrap();
        client.batch_execute("CREATE SCHEMA d.s").unwrap();
        client
            .batch_execute("CREATE VIEW d.s.v AS SELECT 1")
            .unwrap();
    }

    let server = util::start_server(config).unwrap();
    let mut client = server.connect(postgres::NoTls).unwrap();
    assert_eq!(
        client
            .query("SHOW VIEWS", &[])
            .unwrap()
            .into_iter()
            .map(|row| row.get(0))
            .collect::<Vec<String>>(),
        &["constant", "mat"]
    );
    assert_eq!(
        client
            .query_one("SHOW INDEXES ON mat", &[])
            .unwrap()
            .get::<_, Vec<String>>("key"),
        &["a", "a_data", "c", "c_data"],
    );
    assert_eq!(
        client
            .query("SHOW VIEWS FROM d.s", &[])
            .unwrap()
            .into_iter()
            .map(|row| row.get(0))
            .collect::<Vec<String>>(),
        &["v"]
    );

    // Test that catalog recovery correctly populates `mz_objects`.
    assert_eq!(
        client
            .query(
                "SELECT id FROM mz_objects WHERE id LIKE 'u%' ORDER BY 1",
                &[]
            )
            .unwrap()
            .into_iter()
            .map(|row| row.get(0))
            .collect::<Vec<String>>(),
        vec!["u1", "u2", "u3", "u4", "u5", "u6"]
    );
}

// Test that sources and sinks require an explicit `SIZE` parameter outside of
// unsafe mode.
#[test]
fn test_source_sink_size_required() {
    let server = util::start_server(util::Config::default()).unwrap();
    let mut client = server.connect(postgres::NoTls).unwrap();

    // Sources bail without an explicit size.
    let result = client.batch_execute("CREATE SOURCE lg FROM LOAD GENERATOR COUNTER");
    assert_eq!(
        result.unwrap_err().unwrap_db_error().message(),
        "size option is required"
    );

    // Sources work with an explicit size.
    client
        .batch_execute("CREATE SOURCE lg FROM LOAD GENERATOR COUNTER WITH (SIZE '1')")
        .unwrap();

    // `ALTER SOURCE ... RESET SIZE` is banned.
    let result = client.batch_execute("ALTER SOURCE lg RESET (SIZE)");
    assert_eq!(
        result.unwrap_err().unwrap_db_error().message(),
        "size option is required"
    );

    client
        .batch_execute(&format!(
            "CREATE CONNECTION conn TO KAFKA (BROKER '{}')",
            &*KAFKA_ADDRS,
        ))
        .unwrap();

    // Sinks bail without an explicit size.
    let result = client.batch_execute("CREATE SINK snk FROM mz_sources INTO KAFKA CONNECTION conn (TOPIC 'foo') FORMAT JSON ENVELOPE DEBEZIUM");
    assert_eq!(
        result.unwrap_err().unwrap_db_error().message(),
        "size option is required"
    );

    // Sinks work with an explicit size.
    client.batch_execute("CREATE SINK snk FROM mz_sources INTO KAFKA CONNECTION conn (TOPIC 'foo') FORMAT JSON ENVELOPE DEBEZIUM WITH (SIZE '1')").unwrap();

    // `ALTER SINK ... RESET SIZE` is banned.
    let result = client.batch_execute("ALTER SINK snk RESET (SIZE)");
    assert_eq!(
        result.unwrap_err().unwrap_db_error().message(),
        "size option is required"
    );
}

// Test the POST and WS server endpoints.
#[test]
fn test_http_sql() {
    // Datadriven directives for WebSocket are "ws-text" and "ws-binary" to send
    // text or binary websocket messages that are the input. Output is
    // everything until and including the next ReadyForQuery message. An
    // optional "rows=N" argument can be given in the directive to produce
    // datadriven output after N rows. Any directive with rows=N should be the
    // final directive in a file, since it leaves the websocket in a
    // mid-statement state. A "fixtimestamp=true" argument can be given to
    // replace timestamps with "<TIMESTAMP>".
    //
    // Datadriven directive for HTTP POST is "http". Input and output are the
    // documented JSON formats.

    let fixtimestamp_re = regex::Regex::new("\\d{13}(\\.0)?").unwrap();
    let fixtimestamp_replace = "\"<TIMESTAMP>\"";

    datadriven::walk("tests/testdata/http", |f| {
        let server = util::start_server(util::Config::default()).unwrap();
        let ws_url = Url::parse(&format!(
            "ws://{}/api/experimental/sql",
            server.inner.http_local_addr()
        ))
        .unwrap();
        let http_url = Url::parse(&format!(
            "http://{}/api/sql",
            server.inner.http_local_addr()
        ))
        .unwrap();
        let (mut ws, _resp) = tungstenite::connect(&ws_url).unwrap();

        f.run(|tc| {
            let msg = match tc.directive.as_str() {
                "ws-text" => Message::Text(tc.input.clone()),
                "ws-binary" => Message::Binary(tc.input.as_bytes().to_vec()),
                "http" => {
                    let json: serde_json::Value = serde_json::from_str(&tc.input).unwrap();
                    let res = Client::new()
                        .post(http_url.clone())
                        .json(&json)
                        .send()
                        .unwrap();
                    return format!("{}\n{}\n", res.status(), res.text().unwrap());
                }
                _ => panic!("unknown directive {}", tc.directive),
            };
            let mut rows = tc
                .args
                .get("rows")
                .map(|rows| rows.get(0).map(|row| row.parse::<usize>().unwrap()))
                .flatten();
            let fixtimestamp = tc.args.get("fixtimestamp").is_some();
            ws.write_message(msg).unwrap();
            let mut responses = String::new();
            loop {
                let resp = ws.read_message().unwrap();
                match resp {
                    Message::Text(mut msg) => {
                        if fixtimestamp {
                            msg = fixtimestamp_re
                                .replace_all(&msg, fixtimestamp_replace)
                                .into();
                        }
                        let msg: WebSocketResponse = serde_json::from_str(&msg).unwrap();
                        write!(&mut responses, "{}\n", serde_json::to_string(&msg).unwrap())
                            .unwrap();
                        match msg {
                            WebSocketResponse::ReadyForQuery(_) => return responses,
                            WebSocketResponse::Row(_) => {
                                if let Some(rows) = rows.as_mut() {
                                    *rows -= 1;
                                    if *rows == 0 {
                                        return responses;
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    Message::Ping(_) => continue,
                    _ => panic!("unexpected response: {:?}", resp),
                }
            }
        });
    });
}

// Test that the server properly handles cancellation requests.
#[test]
fn test_cancel_long_running_query() {
    let config = util::Config::default().unsafe_mode();
    let server = util::start_server(config).unwrap();

    let mut client = server.connect(postgres::NoTls).unwrap();
    let cancel_token = client.cancel_token();

    thread::spawn(move || {
        // Abort the query after 2s.
        thread::sleep(Duration::from_secs(2));
        let _ = cancel_token.cancel_query(postgres::NoTls);
    });

    client.batch_execute("CREATE TABLE t (i INT)").unwrap();

    match client.simple_query("SELECT * FROM t AS OF 18446744073709551615") {
        Err(e) if e.code() == Some(&postgres::error::SqlState::QUERY_CANCELED) => {}
        Err(e) => panic!("expected error SqlState::QUERY_CANCELED, but got {:?}", e),
        Ok(_) => panic!("expected error SqlState::QUERY_CANCELED, but query succeeded"),
    }

    client
        .simple_query("SELECT 1")
        .expect("simple query succeeds after cancellation");
}

// Test that dataflow uninstalls cancelled peeks.
#[test]
fn test_cancel_dataflow_removal() {
    let config = util::Config::default().unsafe_mode();
    let server = util::start_server(config).unwrap();

    let mut client1 = server.connect(postgres::NoTls).unwrap();
    let mut client2 = server.connect(postgres::NoTls).unwrap();
    let cancel_token = client1.cancel_token();

    client1.batch_execute("CREATE TABLE t (i INT)").unwrap();
    // No dataflows expected at startup.
    assert_eq!(
        client1
            .query_one(
                "SELECT count(*) FROM mz_internal.mz_dataflow_operators",
                &[]
            )
            .unwrap()
            .get::<_, i64>(0),
        0
    );

    thread::spawn(move || {
        // Wait until we see the expected dataflow.
        Retry::default()
            .retry(|_state| {
                let count: i64 = client2
                    .query_one(
                        "SELECT count(*) FROM mz_internal.mz_dataflow_operators",
                        &[],
                    )
                    .map_err(|_| ())
                    .unwrap()
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

    match client1.simple_query("SELECT * FROM t AS OF 9223372036854775807") {
        Err(e) if e.code() == Some(&postgres::error::SqlState::QUERY_CANCELED) => {}
        Err(e) => panic!("expected error SqlState::QUERY_CANCELED, but got {:?}", e),
        Ok(_) => panic!("expected error SqlState::QUERY_CANCELED, but query succeeded"),
    }
    // Expect the dataflows to shut down.
    Retry::default()
        .retry(|_state| {
            let count: i64 = client1
                .query_one(
                    "SELECT count(*) FROM mz_internal.mz_dataflow_operators",
                    &[],
                )
                .map_err(|_| ())
                .unwrap()
                .get(0);
            if count == 0 {
                Ok(())
            } else {
                Err(())
            }
        })
        .unwrap();
}

#[test]
fn test_storage_usage_collection_interval() {
    let config =
        util::Config::default().with_storage_usage_collection_interval(Duration::from_secs(1));
    let server = util::start_server(config).unwrap();
    let mut client = server.connect(postgres::NoTls).unwrap();

    // Retry because it may take some time for the initial snapshot to be taken.
    let initial_storage: i64 = Retry::default()
        .retry(|_| {
            client
                .query_one(
                    "SELECT SUM(size_bytes)::int8 FROM mz_catalog.mz_storage_usage;",
                    &[],
                )
                .map_err(|e| e.to_string())
                .unwrap()
                .try_get::<_, i64>(0)
                .map_err(|e| e.to_string())
        })
        .unwrap();

    client.batch_execute("CREATE TABLE t (a INT)").unwrap();
    client
        .batch_execute("INSERT INTO t VALUES (1), (2)")
        .unwrap();

    // Retry until storage usage is updated.
    Retry::default().max_duration(Duration::from_secs(5)).retry(|_| {
        let updated_storage = client
            .query_one("SELECT SUM(size_bytes)::int8 FROM mz_catalog.mz_storage_usage;", &[])
            .map_err(|e| e.to_string()).unwrap()
            .try_get::<_, i64>(0)
            .map_err(|e| e.to_string()).unwrap();

        if updated_storage > initial_storage {
            Ok(())
        } else {
            Err(format!("updated storage count {updated_storage} is not greater than initial storage {initial_storage}"))
        }
    }).unwrap();
}

#[test]
fn test_storage_usage_updates_between_restarts() {
    let data_dir = tempfile::tempdir().unwrap();
    let storage_usage_collection_interval = Duration::from_secs(3);
    let config = util::Config::default()
        .with_storage_usage_collection_interval(storage_usage_collection_interval)
        .data_directory(data_dir.path());

    // Wait for initial storage usage collection.
    let initial_timestamp: f64 = {
        let server = util::start_server(config.clone()).unwrap();
        let mut client = server.connect(postgres::NoTls).unwrap();
        // Retry because it may take some time for the initial snapshot to be taken.
        Retry::default().max_duration(Duration::from_secs(60)).retry(|_| {
            client
                    .query_one(
                        "SELECT EXTRACT(EPOCH FROM MAX(collection_timestamp))::float8 FROM mz_catalog.mz_storage_usage;",
                        &[],
                    )
                    .map_err(|e| e.to_string()).unwrap()
                    .try_get::<_, f64>(0)
                    .map_err(|e| e.to_string())
        }).unwrap()
    };

    std::thread::sleep(storage_usage_collection_interval);

    // Another storage usage collection should be scheduled immediately.
    {
        let server = util::start_server(config).unwrap();
        let mut client = server.connect(postgres::NoTls).unwrap();

        // Retry until storage usage is updated.
        Retry::default().max_duration(Duration::from_secs(60)).retry(|_| {
            let updated_timestamp = client
                .query_one("SELECT EXTRACT(EPOCH FROM MAX(collection_timestamp))::float8 FROM mz_catalog.mz_storage_usage;", &[])
                .map_err(|e| e.to_string()).unwrap()
                .try_get::<_, f64>(0)
                .map_err(|e| e.to_string()).unwrap();

            if updated_timestamp > initial_timestamp {
                Ok(())
            } else {
                Err(format!("updated storage collection timestamp {updated_timestamp} is not greater than initial timestamp {initial_timestamp}"))
            }
        }).unwrap();
    }
}

#[test]
fn test_storage_usage_doesnt_update_between_restarts() {
    let data_dir = tempfile::tempdir().unwrap();
    let storage_usage_collection_interval = Duration::from_secs(60);
    let config = util::Config::default()
        .with_storage_usage_collection_interval(storage_usage_collection_interval)
        .data_directory(data_dir.path());

    // Wait for initial storage usage collection.
    let initial_timestamp: f64 = {
        let server = util::start_server(config.clone()).unwrap();
        let mut client = server.connect(postgres::NoTls).unwrap();
        // Retry because it may take some time for the initial snapshot to be taken.
        Retry::default().max_duration(Duration::from_secs(60)).retry(|_| {
            client
                    .query_one(
                        "SELECT EXTRACT(EPOCH FROM MAX(collection_timestamp))::float8 FROM mz_catalog.mz_storage_usage;",
                        &[],
                    )
                    .map_err(|e| e.to_string()).unwrap()
                    .try_get::<_, f64>(0)
                    .map_err(|e| e.to_string())
        }).unwrap()
    };

    std::thread::sleep(Duration::from_secs(2));

    // Another storage usage collection should not be scheduled immediately.
    {
        let server = util::start_server(config).unwrap();
        let mut client = server.connect(postgres::NoTls).unwrap();

        let updated_timestamp = client
            .query_one(
                "SELECT EXTRACT(EPOCH FROM MAX(collection_timestamp))::float8 FROM mz_catalog.mz_storage_usage;",
                &[],
            ).unwrap().get::<_, f64>(0);

        assert_eq!(initial_timestamp, updated_timestamp);
    }
}

#[test]
fn test_storage_usage_collection_interval_timestamps() {
    let config =
        util::Config::default().with_storage_usage_collection_interval(Duration::from_secs(30));
    let server = util::start_server(config).unwrap();
    let mut client = server.connect(postgres::NoTls).unwrap();

    // Retry because it may take some time for the initial snapshot to be taken.
    Retry::default().max_duration(Duration::from_secs(10)).retry(|_| {
        let rows = client
            .query(
                "SELECT collection_timestamp, SUM(size_bytes)::int8 FROM mz_catalog.mz_storage_usage GROUP BY collection_timestamp ORDER BY collection_timestamp;",
                &[],
            )
            .map_err(|e| e.to_string()).unwrap();
        if rows.len() == 1 {
            Ok(())
        } else {
            Err(format!("expected a single timestamp, instead found {}", rows.len()))
        }
    }).unwrap();
}

#[test]
fn test_default_cluster_sizes() {
    let config = util::Config::default()
        .with_builtin_cluster_replica_size("1".to_string())
        .with_default_cluster_replica_size("2".to_string());
    let server = util::start_server(config).unwrap();
    let mut client = server.connect(postgres::NoTls).unwrap();

    let builtin_size: String = client
        .query(
            "SELECT size FROM (SHOW CLUSTER REPLICAS WHERE cluster LIKE 'mz_%')",
            &[],
        )
        .unwrap()
        .get(0)
        .unwrap()
        .get(0);
    assert_eq!(builtin_size, "1");

    let builtin_size: String = client
        .query(
            "SELECT size FROM (SHOW CLUSTER REPLICAS WHERE cluster = 'default')",
            &[],
        )
        .unwrap()
        .get(0)
        .unwrap()
        .get(0);
    assert_eq!(builtin_size, "2");
}
