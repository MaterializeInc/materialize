// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for Materialize server.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Write;
use std::io::Write as _;
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{iter, thread};

use anyhow::bail;
use chrono::{DateTime, Utc};
use flate2::Compression;
use flate2::write::GzEncoder;
use futures::FutureExt;
use http::Request;
use itertools::Itertools;
use jsonwebtoken::{DecodingKey, EncodingKey};
use mz_environmentd::test_util::{self, Ca, KAFKA_ADDRS, PostgresErrorExt, make_pg_tls};
use mz_environmentd::{WebSocketAuth, WebSocketResponse};
use mz_frontegg_auth::{
    Authenticator as FronteggAuthentication, AuthenticatorConfig as FronteggConfig,
    DEFAULT_REFRESH_DROP_FACTOR, DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
};
use mz_frontegg_mock::{FronteggMockServer, models::ApiToken, models::UserConfig};
use mz_ore::cast::CastFrom;
use mz_ore::cast::CastLossy;
use mz_ore::cast::TryCastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{NowFn, SYSTEM_TIME, to_datetime};
use mz_ore::retry::Retry;
use mz_ore::{assert_contains, task::RuntimeExt};
use mz_ore::{assert_err, assert_none, assert_ok, task};
use mz_pgrepr::UInt8;
use mz_repr::UNKNOWN_COLUMN_NAME;
use mz_sql::session::user::{ANALYTICS_USER, HTTP_DEFAULT_USER, SYSTEM_USER};
use mz_sql_parser::ast::display::AstDisplay;
use openssl::ssl::{SslConnectorBuilder, SslVerifyMode};
use openssl::x509::X509;
use postgres::config::SslMode;
use postgres_array::Array;
use rand::RngCore;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka_sys::RDKafkaErrorCode;
use reqwest::blocking::Client;
use reqwest::header::{CONTENT_ENCODING, CONTENT_TYPE};
use reqwest::{StatusCode, Url};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio_postgres::error::SqlState;
use tracing::info;
use tungstenite::error::ProtocolError;
use tungstenite::{Error, Message, Utf8Bytes};
use uuid::Uuid;

// Allow the use of banned rdkafka methods, because we are just in tests.
#[allow(clippy::disallowed_methods)]
#[mz_ore::test]
fn test_persistence() {
    let data_dir = tempfile::tempdir().unwrap();
    let harness = test_util::TestHarness::default()
        .data_directory(data_dir.path())
        .unsafe_mode();

    {
        let server = harness.clone().start_blocking();
        let mut client = server.connect(postgres::NoTls).unwrap();

        server.runtime().block_on(async {
            let admin: AdminClient<_> = ClientConfig::new()
                .set("bootstrap.servers", &*KAFKA_ADDRS)
                .create()
                .expect("Admin client creation failed");
            let new_topic = NewTopic::new("foo", 1, TopicReplication::Fixed(1));
            let topic_results = admin
                .create_topics([&new_topic], &AdminOptions::new())
                .await
                .expect("topic creation failed");
            match topic_results[0] {
                Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
                Err((ref err, _)) => panic!("failed to ensure topic: {err}"),
            }
        });

        client
            .batch_execute(&format!(
                "CREATE CONNECTION kafka_conn TO KAFKA (BROKER '{}', SECURITY PROTOCOL PLAINTEXT)",
                &*KAFKA_ADDRS,
            ))
            .unwrap();
        client
            .batch_execute(
                "CREATE SOURCE src FROM KAFKA CONNECTION kafka_conn (TOPIC 'foo') FORMAT BYTES",
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

    let server = harness.start_blocking();
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
        vec!["u1", "u2", "u3", "u4", "u5", "u6", "u7"]
    );
}

fn setup_statement_logging_core(
    max_sample_rate: f64,
    sample_rate: f64,
    target_data_rate: &str,
    test_harness: test_util::TestHarness,
) -> (test_util::TestServerWithRuntime, postgres::Client) {
    let server = test_harness
        .with_system_parameter_default(
            "statement_logging_max_sample_rate".to_string(),
            max_sample_rate.to_string(),
        )
        .with_system_parameter_default(
            "statement_logging_default_sample_rate".to_string(),
            sample_rate.to_string(),
        )
        .with_system_parameter_default(
            "statement_logging_max_data_credit".to_string(),
            "".to_string(),
        )
        .with_system_parameter_default(
            "statement_logging_target_data_rate".to_string(),
            target_data_rate.to_string(),
        )
        .with_system_parameter_default(
            "statement_logging_use_reproducible_rng".to_string(),
            "true".to_string(),
        )
        .with_system_parameter_default(
            "enable_frontend_peek_sequencing".to_string(),
            "false".to_string(),
        )
        .start_blocking();
    let client = server.connect(postgres::NoTls).unwrap();
    (server, client)
}

fn setup_statement_logging(
    max_sample_rate: f64,
    sample_rate: f64,
    target_data_rate: &str,
) -> (test_util::TestServerWithRuntime, postgres::Client) {
    setup_statement_logging_core(
        max_sample_rate,
        sample_rate,
        target_data_rate,
        test_util::TestHarness::default(),
    )
}

// Test that we log various kinds of statement whose execution terminates in the coordinator.
#[mz_ore::test]
fn test_statement_logging_immediate() {
    let (server, mut client) = setup_statement_logging(1.0, 1.0, "");

    let mut mz_client = server
        .pg_config_internal()
        .user(&SYSTEM_USER.name)
        .connect(postgres::NoTls)
        .unwrap();
    mz_client
        .batch_execute("ALTER SYSTEM SET enable_statement_lifecycle_logging = false")
        .unwrap();
    mz_client
        .batch_execute("ALTER SYSTEM SET statement_logging_max_sample_rate = 1")
        .unwrap();
    mz_client
        .batch_execute("ALTER SYSTEM SET statement_logging_default_sample_rate = 1")
        .unwrap();
    mz_client
        .batch_execute("ALTER SYSTEM SET enable_load_generator_counter = true")
        .unwrap();

    let successful_immediates: &[&str] = &[
        "CREATE VIEW v AS SELECT 1;",
        "CREATE DEFAULT INDEX i ON v;",
        "CREATE TABLE t (x bigint);",
        "INSERT INTO t VALUES (1), (2), (3)",
        "UPDATE t SET x=x+1",
        "DELETE FROM t;",
        "CREATE SECRET s AS 'hunter2';",
        "DROP SECRET s;",
        "",
        "CREATE SOURCE s FROM LOAD GENERATOR COUNTER",
        "PREPARE foo AS SELECT * FROM t",
        "EXECUTE foo",
        "BEGIN",
        "DECLARE c CURSOR FOR SELECT * FROM t",
        "FETCH FORWARD ALL FROM c",
        "COMMIT",
        "BEGIN",
        "ROLLBACK",
        "SET application_name='my_application'",
        "SHOW ALL",
        "SHOW application_name",
    ];
    let constants: &[&str] = &["1", "2", "3", "hunter2", "my_application"];

    for &statement in successful_immediates {
        client.execute(statement, &[]).unwrap();

        // Enforce a small delay to avoid duplicate `began_at` times, which would make the ordering
        // of logged statements non-deterministic when we retrieve them below.
        thread::sleep(Duration::from_millis(10));
    }

    let mut client = server.connect_internal(postgres::NoTls).unwrap();
    let seh_query = "
        SELECT
            mseh.sample_rate,
            mseh.began_at,
            mseh.finished_at,
            mseh.finished_status,
            mst.sql,
            mpsh.prepared_at,
            mst.redacted_sql
        FROM mz_internal.mz_statement_execution_history AS mseh
        LEFT JOIN
            mz_internal.mz_prepared_statement_history AS mpsh
            ON mseh.prepared_statement_id = mpsh.id
        JOIN
            (SELECT DISTINCT sql, sql_hash, redacted_sql FROM mz_internal.mz_sql_text) mst
            ON mpsh.sql_hash = mst.sql_hash
        WHERE mst.sql !~~ '%mz_statement_execution_history%'
        ORDER BY mseh.began_at";

    // Statement logging happens async, retry until we get the expected number of logged
    // statements.
    let mut sl = Vec::new();
    for _ in 0..10 {
        thread::sleep(Duration::from_secs(1));
        sl = client.query(seh_query, &[]).unwrap();
        if sl.len() >= successful_immediates.len() {
            break;
        }
    }

    assert_eq!(sl.len(), successful_immediates.len());

    #[derive(Debug)]
    struct Record {
        sample_rate: f64,
        began_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        finished_status: String,
        sql: String,
        prepared_at: DateTime<Utc>,
        redacted_sql: String,
    }
    for (r, stmt) in std::iter::zip(sl.iter(), successful_immediates) {
        let r = Record {
            sample_rate: r.get(0),
            began_at: r.get(1),
            finished_at: r.get(2),
            finished_status: r.get(3),
            sql: r.get(4),
            prepared_at: r.get(5),
            redacted_sql: r.get(6),
        };
        assert_eq!(r.sample_rate, 1.0);

        let expected_sql = if r.sql.contains("SECRET")
            || r.sql.contains("INSERT")
            || r.sql.contains("UPDATE")
            || r.sql.contains("EXECUTE")
        {
            mz_sql::parse::parse(&r.sql)
                .unwrap()
                .into_element()
                .ast
                .to_ast_string_redacted()
        } else {
            stmt.chars().filter(|&ch| ch != ';').collect::<String>()
        };
        assert_eq!(r.sql, expected_sql);
        assert_eq!(r.finished_status, "success");
        assert!(r.prepared_at <= r.began_at);
        assert!(r.began_at <= r.finished_at);
        // NB[btv] -- It would be a bit nicer if we could separately mock
        // both the start and end time, but the `NowFn` mechanism doesn't
        // appear to give us any way to do that. Instead, let's just check
        // that none of these statements took longer than 5s wall-clock time.
        assert!(r.finished_at - r.began_at <= chrono::Duration::try_seconds(5).unwrap());
        if !r.sql.is_empty() {
            let expected_redacted = mz_sql::parse::parse(&r.sql)
                .unwrap()
                .into_element()
                .ast
                .to_ast_string_redacted();
            assert_eq!(r.redacted_sql, expected_redacted);
            for constant in constants {
                assert!(!r.redacted_sql.contains(constant));
            }
        }
    }
}

#[mz_ore::test]
fn test_statement_logging_basic() {
    let (server, mut client) = setup_statement_logging(1.0, 1.0, "");
    client.execute("SELECT 1", &[]).unwrap();
    // We test that queries of this view execute on a cluster.
    // If we ever change the threshold for constant folding such that
    // this gets to run on environmentd, change this query.
    client
        .execute(
            "CREATE VIEW v AS SELECT * FROM generate_series(1, 10001)",
            &[],
        )
        .unwrap();
    client.execute("SELECT * FROM v", &[]).unwrap();
    client.execute("CREATE DEFAULT INDEX i ON v", &[]).unwrap();
    client.execute("SELECT * FROM v", &[]).unwrap();
    let _ = client.execute("SELECT 1/0", &[]);
    client.execute("CREATE TABLE t (x int)", &[]).unwrap();
    client.execute("SELECT * FROM t", &[]).unwrap();

    #[derive(Debug)]
    struct Record {
        sample_rate: f64,
        began_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        finished_status: String,
        error_message: Option<String>,
        prepared_at: DateTime<Utc>,
        execution_strategy: Option<String>,
        result_size: Option<i64>,
        rows_returned: Option<i64>,
        execution_timestamp: Option<u64>,
    }

    let mut client = server.connect_internal(postgres::NoTls).unwrap();

    let result = Retry::default()
        .max_duration(Duration::from_secs(30))
        .retry(|_| {
            let sl_results = client
                .query(
                    "SELECT
    mseh.sample_rate,
    mseh.began_at,
    mseh.finished_at,
    mseh.finished_status,
    mseh.error_message,
    mpsh.prepared_at,
    mseh.execution_strategy,
    mseh.result_size,
    mseh.rows_returned,
    mseh.execution_timestamp
FROM
    mz_internal.mz_statement_execution_history AS mseh
        LEFT JOIN
            mz_internal.mz_prepared_statement_history AS mpsh
            ON mseh.prepared_statement_id = mpsh.id
        JOIN
            (SELECT DISTINCT sql, sql_hash, redacted_sql FROM mz_internal.mz_sql_text) AS mst
            ON mpsh.sql_hash = mst.sql_hash
WHERE mst.sql ~~ 'SELECT%'
AND mst.sql !~~ '%unique string to prevent this query showing up in results after retries%'
AND mst.sql !~~ '%pg_catalog.pg_type%' --this gets executed behind the scenes by tokio-postgres
OR mst.sql ~~ 'CREATE TABLE%'
ORDER BY mseh.began_at",
                    &[],
                )
                .unwrap();

            if sl_results.len() == 6 {
                Ok(sl_results)
            } else {
                Err(sl_results.len())
            }
        });
    let sl_results = match result {
        Ok(rows) => rows
            .into_iter()
            .map(|r| Record {
                sample_rate: r.get(0),
                began_at: r.get(1),
                finished_at: r.get(2),
                finished_status: r.get(3),
                error_message: r.get(4),
                prepared_at: r.get(5),
                execution_strategy: r.get(6),
                result_size: r.get(7),
                rows_returned: r.get(8),
                execution_timestamp: r.get::<_, Option<UInt8>>(9).map(|UInt8(val)| val),
            })
            .collect::<Vec<_>>(),
        Err(rows) => {
            panic!("number of results never became correct: {rows}");
        }
    };
    // The two queries on generate_series(1,10001) execute at the maximum timestamp
    assert_eq!(
        sl_results
            .iter()
            .filter(|r| r.execution_timestamp == Some(u64::MAX))
            .count(),
        2
    );
    // The two queries that can be satisfied by envd (SELECT 1 and SELECT 1/0) have no execution timestamp
    assert_eq!(
        sl_results
            .iter()
            .filter(|r| r.execution_timestamp.is_none())
            .count(),
        2
    );
    // All other queries have an execution timestamp, in particular, including `CREATE TABLE`.
    assert_eq!(sl_results.len(), 6);
    for r in &sl_results {
        assert_eq!(r.sample_rate, 1.0);
        assert!(r.prepared_at <= r.began_at);
        assert!(r.began_at <= r.finished_at);
        // It would be nice to be able to control
        // execution timestamp via a `NowFn`, but
        // that is hard to get right and interferes with our logic
        // about when to flush to persist. So instead, just check that they're sane.
        if let Some(ts) = r.execution_timestamp {
            if ts != u64::MAX {
                let ts = to_datetime(ts);
                assert!((ts - r.prepared_at).abs() < chrono::Duration::try_seconds(5).unwrap())
            }
        }
    }
    assert!(sl_results[0].result_size.unwrap_or(0) > 0);
    assert_eq!(sl_results[0].rows_returned, Some(1));
    assert_eq!(sl_results[0].finished_status, "success");
    assert_eq!(
        sl_results[0].execution_strategy.as_ref().unwrap(),
        "constant"
    );
    assert!(sl_results[1].result_size.unwrap_or(0) > 0);
    assert_eq!(sl_results[1].rows_returned, Some(10001));
    assert_eq!(sl_results[1].finished_status, "success");
    assert_eq!(
        sl_results[1].execution_strategy.as_ref().unwrap(),
        "standard"
    );
    assert!(sl_results[2].result_size.unwrap_or(0) > 0);
    assert_eq!(sl_results[2].rows_returned, Some(10001));
    assert_eq!(sl_results[2].finished_status, "success");
    assert_eq!(
        sl_results[2].execution_strategy.as_ref().unwrap(),
        "fast-path"
    );
    assert_eq!(sl_results[3].finished_status, "error");
    assert!(
        sl_results[3]
            .error_message
            .as_ref()
            .unwrap()
            .contains("division by zero")
    );
    assert_none!(sl_results[3].result_size);
    assert_none!(sl_results[3].rows_returned);
}

fn run_throttling_test(use_prepared_statement: bool) {
    let (server, mut client) = setup_statement_logging(1.0, 1.0, "1000");
    thread::sleep(Duration::from_secs(2));

    if use_prepared_statement {
        let statement = client.prepare("SELECT 1").unwrap();
        for _ in 0..100 {
            client.execute(&statement, &[]).unwrap();
        }
    } else {
        for _ in 0..100 {
            client.execute("SELECT 1", &[]).unwrap();
        }
    }

    thread::sleep(Duration::from_secs(4));
    client.execute("SELECT 2", &[]).unwrap();
    let mut client = server.connect_internal(postgres::NoTls).unwrap();
    let logs = Retry::default()
        .max_duration(Duration::from_secs(60))
        .retry(|_| {
            let sl_results = client
                .query(
                    "SELECT
    sql,
    throttled_count
FROM mz_internal.mz_statement_execution_history mseh
JOIN mz_internal.mz_prepared_statement_history mpsh
ON mseh.prepared_statement_id = mpsh.id
JOIN (SELECT DISTINCT sql, sql_hash, redacted_sql FROM mz_internal.mz_sql_text) mst
ON mpsh.sql_hash = mst.sql_hash
WHERE sql IN ('SELECT 1', 'SELECT 2')",
                    &[],
                )
                .unwrap();

            if sl_results.iter().any(|stmt| {
                let sql: String = stmt.get(0);
                sql == "SELECT 2"
            }) {
                Ok(sl_results)
            } else {
                Err(())
            }
        })
        .expect("Never saw last statement (`SELECT 2`)");
    let throttled_count = logs
        .iter()
        .map(|log| {
            let UInt8(throttled_count) = log.get(1);
            throttled_count
        })
        .sum::<u64>();
    assert!(
        throttled_count > 0,
        "at least some statements should have been throttled"
    );

    assert_eq!(logs.len() + usize::cast_from(throttled_count), 101);
}

#[mz_ore::test]
fn test_statement_logging_throttling() {
    run_throttling_test(false);
}

#[mz_ore::test]
fn test_statement_logging_prepared_statement_throttling() {
    run_throttling_test(true);
}

#[mz_ore::test]
fn test_statement_logging_subscribes() {
    let (server, mut client) = setup_statement_logging(1.0, 1.0, "");
    let cancel_token = client.cancel_token();

    // This should finish
    client
        .execute(
            "SUBSCRIBE TO (SELECT * FROM generate_series(1, 10001))",
            &[],
        )
        .unwrap();

    let handle = thread::spawn(move || {
        client.execute("CREATE TABLE t (x int)", &[]).unwrap();
        // This should not finish until it's canceled.
        let _ = client.execute("SUBSCRIBE TO (SELECT * FROM t)", &[]);
    });

    while !handle.is_finished() {
        thread::sleep(Duration::from_secs(1));
        cancel_token.cancel_query(postgres::NoTls).unwrap();
    }
    handle.join().unwrap();

    let mut client = server.connect_internal(postgres::NoTls).unwrap();
    let seh_query = "
        SELECT
            mseh.sample_rate,
            mseh.began_at,
            mseh.finished_at,
            mseh.finished_status,
            mpsh.prepared_at,
            mseh.execution_strategy
        FROM mz_internal.mz_statement_execution_history AS mseh
        LEFT JOIN
            mz_internal.mz_prepared_statement_history AS mpsh
            ON mseh.prepared_statement_id = mpsh.id
        JOIN
            mz_internal.mz_sql_text AS mst
            ON mpsh.sql_hash = mst.sql_hash
        WHERE mst.sql ~~ 'SUBSCRIBE%'
        ORDER BY mseh.began_at";

    // Statement logging happens async, retry until we get the expected number of logged
    // statements.
    let mut sl = Vec::new();
    for _ in 0..10 {
        thread::sleep(Duration::from_secs(1));
        sl = client.query(seh_query, &[]).unwrap();
        if sl.len() >= 2 {
            break;
        }
    }

    assert_eq!(sl.len(), 2);

    struct Record {
        sample_rate: f64,
        began_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        finished_status: String,
        prepared_at: DateTime<Utc>,
        execution_strategy: Option<String>,
    }

    let sl_subscribes = sl
        .into_iter()
        .map(|r| Record {
            sample_rate: r.get(0),
            began_at: r.get(1),
            finished_at: r.get(2),
            finished_status: r.get(3),
            prepared_at: r.get(4),
            execution_strategy: r.get(5),
        })
        .collect::<Vec<_>>();
    for r in &sl_subscribes {
        assert_eq!(r.sample_rate, 1.0);
        assert!(r.prepared_at <= r.began_at);
        assert!(r.began_at <= r.finished_at);
        assert_none!(r.execution_strategy);
    }
    assert_eq!(sl_subscribes[0].finished_status, "success");
    assert_eq!(sl_subscribes[1].finished_status, "canceled");
}

/// Test that we are sampling approximately 50% of statements.
/// Relies on two assumptions:
/// (1) that the effective sampling rate for the session is 50%,
/// (2) that we are using the deterministic testing RNG.
fn test_statement_logging_sampling_inner(
    server: test_util::TestServerWithRuntime,
    mut client: postgres::Client,
) {
    for i in 0..50 {
        client.execute(&format!("SELECT {i}"), &[]).unwrap();

        // Enforce a small delay to avoid duplicate `began_at` times, which would make the ordering
        // of logged statements non-deterministic when we retrieve them below.
        thread::sleep(Duration::from_millis(10));
    }

    // 23 randomly sampled out of 50 with 50% sampling. Seems legit!
    let expected_sqls = [
        2, 4, 5, 6, 9, 15, 17, 18, 19, 20, 21, 23, 24, 25, 31, 32, 33, 36, 37, 42, 46,
    ]
    .into_iter()
    .map(|i| format!("SELECT {i}"))
    .collect::<Vec<_>>();

    let mut internal_client = server.connect_internal(postgres::NoTls).unwrap();
    let seh_query = "
        SELECT mst.sql
        FROM mz_internal.mz_statement_execution_history AS mseh
        JOIN
            mz_internal.mz_prepared_statement_history AS mpsh
            ON mseh.prepared_statement_id = mpsh.id
        JOIN
            mz_internal.mz_sql_text AS mst
            ON mpsh.sql_hash = mst.sql_hash
        WHERE mst.sql ~~ 'SELECT%' AND mst.sql !~~ '%mz_statement_execution_history%'
        ORDER BY mseh.began_at ASC";

    // Statement logging happens async, retry until we get the expected number of logged
    // statements.
    let mut sl = Vec::new();
    for _ in 0..10 {
        thread::sleep(Duration::from_secs(1));
        sl = internal_client.query(seh_query, &[]).unwrap();
        if sl.len() >= expected_sqls.len() {
            break;
        }
    }

    let sqls: Vec<String> = sl.into_iter().map(|r| r.get(0)).collect();
    assert_eq!(sqls, expected_sqls);
}

#[mz_ore::test]
fn test_statement_logging_sampling() {
    let (server, client) = setup_statement_logging(1.0, 0.5, "");
    test_statement_logging_sampling_inner(server, client);
}

/// Test that we are not allowed to set `statement_logging_sample_rate`
/// arbitrarily high, but that it is constrained by `statement_logging_max_sample_rate`.
#[mz_ore::test]
fn test_statement_logging_sampling_constrained() {
    let (server, client) = setup_statement_logging(0.5, 1.0, "");
    test_statement_logging_sampling_inner(server, client);
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
async fn test_statement_logging_unsampled_metrics() {
    let server = test_util::TestHarness::default().start().await;
    server
        .disable_feature_flags(&["enable_frontend_peek_sequencing"])
        .await;
    let client = server.connect().await.unwrap();

    // TODO[btv]
    //
    // The point of these metrics is to show how much SQL text we
    // would have logged had statement logging been turned on.
    // Since there is no way (yet) to turn statement logging off or on,
    // this test is valid as-is currently. However, once we turn statement logging on,
    // we should make sure to turn it _off_ in this test.
    let batch_queries = [
        "SELECT 'Hello, world!';SELECT 1;;",
        "SELECT 'Hello, world again!'",
    ];
    let batch_total: usize = batch_queries
        .iter()
        .map(|s| s.as_bytes().iter().filter(|&&ch| ch != b';').count())
        .sum();
    let single_queries = ["SELECT 'foo'", "SELECT 'bar';;;"];
    let single_total: usize = single_queries
        .iter()
        .map(|s| s.as_bytes().iter().filter(|&&ch| ch != b';').count())
        .sum();
    let prepared_queries = ["SELECT 'baz';;;", "SELECT 'quux';"];
    let prepared_total: usize = prepared_queries
        .iter()
        .map(|s| s.as_bytes().iter().filter(|&&ch| ch != b';').count())
        .sum();

    let named_prepared_inner = "SELECT 42";
    let named_prepared_outer = format!("PREPARE p AS {named_prepared_inner};EXECUTE p;");
    let named_prepared_outer_len = named_prepared_outer
        .as_bytes()
        .iter()
        .filter(|&&ch| ch != b';')
        .count();

    for q in batch_queries {
        client.batch_execute(q).await.unwrap();
    }

    for q in single_queries {
        client.execute(q, &[]).await.unwrap();
    }

    for q in prepared_queries {
        let s = client.prepare(q).await.unwrap();
        client.execute(&s, &[]).await.unwrap();
    }

    client.batch_execute(&named_prepared_outer).await.unwrap();

    // This should NOT be logged, since we never actually execute it.
    client
        .prepare("SELECT 'Hello, not counted!'")
        .await
        .unwrap();

    let expected_total = batch_total + single_total + prepared_total + named_prepared_outer_len;
    let metric_value = server
        .metrics_registry
        .gather()
        .into_iter()
        .find(|m| m.name() == "mz_statement_logging_unsampled_bytes")
        .unwrap()
        .take_metric()[0]
        .get_counter()
        .get_value();
    let metric_value = usize::cast_from(u64::try_cast_from(metric_value).unwrap());
    assert_eq!(expected_total, metric_value);
}

#[mz_ore::test]
fn test_enable_internal_statement_logging() {
    let (server, mut client) = setup_statement_logging_core(
        1.0,
        1.0,
        "",
        test_util::TestHarness::default().with_system_parameter_default(
            "enable_internal_statement_logging".to_string(),
            "true".to_string(),
        ),
    );

    client.execute("SELECT 1", &[]).unwrap();

    let mut client = server.connect_internal(postgres::NoTls).unwrap();
    let num_mz_system_statements = Retry::default()
        .max_duration(Duration::from_secs(30))
        .retry(|_| {
            let sl_results = client
                .query(
                    "SELECT
    count(*)
FROM mz_internal.mz_prepared_statement_history mpsh
JOIN mz_internal.mz_session_history USING (session_id)
WHERE authenticated_user='mz_system'",
                    &[],
                )
                .unwrap();

            let count: i64 = sl_results[0].get(0);

            if count > 0 { Ok(count) } else { Err(()) }
        })
        .expect("at least some statements from mz_system should have been logged");

    assert!(
        num_mz_system_statements > 0,
        "statements executed by mz_system should have been logged"
    );
}

// Test the POST and WS server endpoints.
#[mz_ore::test]
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
    let fixtimestamp_replace = "<TIMESTAMP>";

    datadriven::walk("tests/testdata/http", |f| {
        let server = test_util::TestHarness::default().start_blocking();

        // Grant all privileges to default http user.
        // TODO(jkosh44) The HTTP endpoint has a special default user while the WS endpoint just
        //  uses the "materialize" role. This is probably wrong. They should either both user
        //  materialize, both use the same special default user, each have their own special
        //  default user.
        {
            let mut super_user = server
                .pg_config_internal()
                .user(&SYSTEM_USER.name)
                .connect(postgres::NoTls)
                .unwrap();
            super_user
                .batch_execute(&format!("CREATE ROLE {}", &HTTP_DEFAULT_USER.name))
                .unwrap();
            super_user
                .batch_execute(&format!(
                    "GRANT ALL PRIVILEGES ON SYSTEM TO {}",
                    &HTTP_DEFAULT_USER.name
                ))
                .unwrap();
            super_user
                .batch_execute(&format!(
                    "GRANT ALL PRIVILEGES ON CLUSTER quickstart TO {}",
                    &HTTP_DEFAULT_USER.name
                ))
                .unwrap();
            super_user
                .batch_execute(&format!(
                    "GRANT ALL PRIVILEGES ON DATABASE materialize TO {}",
                    &HTTP_DEFAULT_USER.name
                ))
                .unwrap();
            super_user
                .batch_execute(&format!(
                    "GRANT ALL PRIVILEGES ON SCHEMA materialize.public TO {}",
                    &HTTP_DEFAULT_USER.name
                ))
                .unwrap();
        }

        let ws_url = server.ws_addr();
        let http_url = Url::parse(&format!("http://{}/api/sql", server.http_local_addr())).unwrap();
        let (mut ws, _resp) = tungstenite::connect(ws_url).unwrap();
        let ws_init = test_util::auth_with_ws(&mut ws, BTreeMap::default()).unwrap();

        // Verify ws_init contains roughly what we expect. This varies (rng secret and version
        // numbers), so easier to test here instead of in the ws file.
        assert!(
            ws_init
                .iter()
                .filter(|m| matches!(m, WebSocketResponse::ParameterStatus(_)))
                .count()
                > 1
        );
        assert!(matches!(
            ws_init.last(),
            Some(WebSocketResponse::BackendKeyData(_))
        ));

        f.run(|tc| {
            let msg = match tc.directive.as_str() {
                "ws-text" => Message::Text(tc.input.clone().into()),
                "ws-binary" => Message::Binary(tc.input.as_bytes().to_vec().into()),
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
            let fixtimestamp = tc.args.contains_key("fixtimestamp");
            ws.send(msg).unwrap();
            let mut responses = String::new();
            loop {
                let resp = ws.read().unwrap();
                match resp {
                    Message::Text(mut msg) => {
                        if fixtimestamp {
                            msg = Utf8Bytes::from(
                                fixtimestamp_re
                                    .replace_all(&msg, fixtimestamp_replace)
                                    .into_owned(),
                            );
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
#[mz_ore::test]
fn test_cancel_long_running_query() {
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();

    let mut client = server.connect(postgres::NoTls).unwrap();
    let cancel_token = client.cancel_token();

    client.batch_execute("CREATE TABLE t (i INT)").unwrap();
    let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();

    let handle = thread::spawn(move || {
        // Repeatedly attempt to abort the query because we're not sure exactly
        // when the SELECT will arrive.
        loop {
            thread::sleep(Duration::from_secs(1));
            match shutdown_rx.try_recv() {
                Ok(()) => return,
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    let _ = cancel_token.cancel_query(postgres::NoTls);
                }
                _ => panic!("unexpected"),
            }
        }
    });

    match client.simple_query("SELECT * FROM t AS OF 18446744073709551615") {
        Err(e) if e.code() == Some(&postgres::error::SqlState::QUERY_CANCELED) => {}
        Err(e) => panic!("expected error SqlState::QUERY_CANCELED, but got {:?}", e),
        Ok(_) => panic!("expected error SqlState::QUERY_CANCELED, but query succeeded"),
    }

    // Wait for the cancellation thread to stop.
    shutdown_tx.send(()).unwrap();
    handle.join().unwrap();

    client
        .simple_query("SELECT 1")
        .expect("simple query succeeds after cancellation");
}

fn test_cancellation_cancels_dataflows(query: &str) {
    // Query that returns how many dataflows are currently installed.
    // Accounts for the presence of introspection subscribe dataflows by ignoring those.
    const DATAFLOW_QUERY: &str = " \
        SELECT count(*) \
        FROM mz_introspection.mz_dataflows \
        WHERE name NOT LIKE '%introspection-subscribe%'";

    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();

    let mut client1 = server.connect(postgres::NoTls).unwrap();
    let mut client2 = server.connect(postgres::NoTls).unwrap();
    let cancel_token = client1.cancel_token();

    client1.batch_execute("CREATE TABLE t (i INT)").unwrap();
    // No dataflows expected at startup.
    assert_eq!(
        client1
            .query_one(DATAFLOW_QUERY, &[])
            .unwrap()
            .get::<_, i64>(0),
        0
    );

    thread::spawn(move || {
        // Wait until we see the expected dataflow.
        Retry::default()
            .retry(|_state| {
                let count: i64 = client2
                    .query_one(DATAFLOW_QUERY, &[])
                    .map_err(|_| ())
                    .unwrap()
                    .get(0);
                if count == 0 { Err(()) } else { Ok(()) }
            })
            .unwrap();
        cancel_token.cancel_query(postgres::NoTls).unwrap();
    });

    match client1.simple_query(query) {
        Err(e) if e.code() == Some(&postgres::error::SqlState::QUERY_CANCELED) => {}
        Err(e) => panic!("expected error SqlState::QUERY_CANCELED, but got {:?}", e),
        Ok(_) => panic!("expected error SqlState::QUERY_CANCELED, but query succeeded"),
    }
    // Expect the dataflows to shut down.
    Retry::default()
        .retry(|_state| {
            let count: i64 = client1
                .query_one(DATAFLOW_QUERY, &[])
                .map_err(|_| ())
                .unwrap()
                .get(0);
            if count == 0 { Ok(()) } else { Err(()) }
        })
        .unwrap();
}

// Test that dataflow uninstalls cancelled peeks.
#[mz_ore::test]
fn test_cancel_dataflow_removal() {
    test_cancellation_cancels_dataflows("SELECT * FROM t AS OF 9223372036854775807");
}

#[mz_ore::test]
fn test_cancel_long_select() {
    test_cancellation_cancels_dataflows(
        "WITH MUTUALLY RECURSIVE flip(x INTEGER) AS (VALUES(1) EXCEPT ALL SELECT * FROM flip) SELECT * FROM flip;",
    );
}

#[mz_ore::test]
fn test_cancel_insert_select() {
    test_cancellation_cancels_dataflows(
        "INSERT INTO t WITH MUTUALLY RECURSIVE flip(x INTEGER) AS (VALUES(1) EXCEPT ALL SELECT * FROM flip) SELECT * FROM flip;",
    );
}

fn test_closing_connection_cancels_dataflows(query: String) {
    // Query that returns how many dataflows are currently installed.
    // Accounts for the presence of introspection subscribe dataflows by ignoring those.
    const DATAFLOW_QUERY: &str = " \
        SELECT count(*) \
        FROM mz_introspection.mz_dataflows \
        WHERE name NOT LIKE '%introspection-subscribe%'";

    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();

    let mut cmd = Command::new("psql");
    let cmd = cmd
        .args([
            // Ignore .psqlrc so that local execution of testdrive isn't
            // affected by it.
            "--no-psqlrc",
            &format!(
                "postgres://{}:{}/materialize",
                Ipv4Addr::LOCALHOST,
                server.sql_local_addr().port()
            ),
        ])
        .stdin(Stdio::piped());
    tracing::info!("spawning: {cmd:#?}");
    let mut child = cmd.spawn().expect("failed to spawn psql");
    let mut stdin = child.stdin.take().expect("failed to open stdin");
    thread::spawn(move || {
        use std::io::Write;
        stdin
            .write_all("SET STATEMENT_TIMEOUT = \"120s\";".as_bytes())
            .unwrap();
        stdin.write_all(query.as_bytes()).unwrap();
    });

    let spawned_psql = Instant::now();

    let mut client = server.connect(postgres::NoTls).unwrap();

    // Wait until we see the expected dataflow.
    Retry::default()
        .retry(|_state| {
            if spawned_psql.elapsed() > Duration::from_secs(30) {
                panic!("waited too long for psql to send the query");
            }
            let count: i64 = client
                .query_one(DATAFLOW_QUERY, &[])
                .map_err(|_| ())
                .unwrap()
                .get(0);
            if count == 0 { Err(()) } else { Ok(()) }
        })
        .unwrap();

    let started = Instant::now();
    if let Some(wait_status) = child.try_wait().expect("wait shouldn't error") {
        panic!("child should still be running, it exitted with {wait_status}");
    }
    child.kill().expect("killing psql child");

    // Expect the dataflows to shut down.
    Retry::default()
        .retry(|_state| {
            if started.elapsed() > Duration::from_secs(30) {
                // this has to be less than statement timeout
                panic!("waited too long for dataflow cancellation");
            }
            let count: i64 = client
                .query_one(DATAFLOW_QUERY, &[])
                .map_err(|_| ())
                .unwrap()
                .get(0);
            if count == 0 { Ok(()) } else { Err(()) }
        })
        .unwrap();
    info!(
        "Took {:#?} until dataflows were cancelled",
        started.elapsed()
    );
}

#[mz_ore::test]
fn test_closing_connection_for_long_select() {
    test_closing_connection_cancels_dataflows("WITH MUTUALLY RECURSIVE flip(x INTEGER) AS (VALUES(1) EXCEPT ALL SELECT * FROM flip) SELECT * FROM flip;".to_string())
}

#[mz_ore::test]
fn test_closing_connection_for_insert_select() {
    test_closing_connection_cancels_dataflows("CREATE TABLE t1 (a int); INSERT INTO t1 WITH MUTUALLY RECURSIVE flip(x INTEGER) AS (VALUES(1) EXCEPT ALL SELECT * FROM flip) SELECT * FROM flip;".to_string())
}

#[mz_ore::test]
fn test_storage_usage_collection_interval() {
    /// Waits for the next storage collection to occur, then returns the
    /// timestamp at which the collection occurred. The timestamp of the last
    /// collection must be provided
    fn wait_for_next_collection(
        client: &mut postgres::Client,
        last_timestamp: DateTime<Utc>,
    ) -> DateTime<Utc> {
        info!("waiting for next storage usage collection");
        let ts = Retry::default()
            .max_duration(Duration::from_secs(10))
            .retry(|_| {
                let row = client.query_one(
                    "SELECT max(collection_timestamp)
                    FROM mz_internal.mz_storage_usage_by_shard",
                    &[],
                )?;
                // mz_storage_usage_by_shard may not be populated yet, which would result in a NULL ts.
                let ts = row.try_get::<_, DateTime<Utc>>("max")?;
                if ts <= last_timestamp {
                    bail!("next collection has not yet occurred")
                }
                Ok(ts)
            })
            .unwrap();
        info!(%ts, "detected storage usage collection");
        ts
    }

    fn get_shard_id(client: &mut postgres::Client, name: &str) -> String {
        let row = Retry::default()
            .max_duration(Duration::from_secs(10))
            .retry(|_| {
                client.query_one(
                    "SELECT shard_id
                     FROM mz_internal.mz_storage_shards s
                     JOIN mz_objects o ON o.id = s.object_id
                     WHERE o.name = $1",
                    &[&name],
                )
            })
            .unwrap();
        row.get("shard_id")
    }

    fn get_storage_usage(
        client: &mut postgres::Client,
        shard_id: &str,
        collection_timestamp: DateTime<Utc>,
    ) -> u64 {
        let row = Retry::default()
            .max_duration(Duration::from_secs(10))
            .retry(|_| {
                client.query_one(
                    "SELECT coalesce(sum(size_bytes), 0)::uint8 AS size
                     FROM mz_internal.mz_storage_usage_by_shard
                     WHERE shard_id = $1 AND collection_timestamp = $2",
                    &[&shard_id, &collection_timestamp],
                )
            })
            .unwrap();
        row.get::<_, UInt8>("size").0
    }

    let server = test_util::TestHarness::default()
        .with_storage_usage_collection_interval(Duration::from_secs(1))
        .start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();

    // Wait for the initial storage usage collection to occur.
    let mut timestamp = wait_for_next_collection(&mut client, DateTime::<Utc>::MIN_UTC);

    // Create a table with no data.
    client
        .batch_execute("CREATE TABLE usage_test (a int)")
        .unwrap();
    let shard_id = get_shard_id(&mut client, "usage_test");
    info!(%shard_id, "created table");

    // Test that the storage usage for the table was zero before it was
    // created.
    let pre_create_storage_usage = get_storage_usage(&mut client, &shard_id, timestamp);
    info!(%pre_create_storage_usage);
    assert_eq!(pre_create_storage_usage, 0);

    // Test that the storage usage for the table is nonzero after it is created
    // (there is some overhead even for empty tables). We wait multiple storage
    // collection intervals (here and below) because the next storage collection
    // may have been concurrent with the previous operation.
    let post_create_storage_usage = Retry::default().max_tries(10).retry(|_| {
        timestamp = wait_for_next_collection(&mut client, timestamp);
        let post_create_storage_usage = get_storage_usage(&mut client, &shard_id, timestamp);
        info!(%post_create_storage_usage);
        if post_create_storage_usage > 0 {
            Ok(post_create_storage_usage)
        } else {
            Err(format!("expected non-zero post create storage usage, found: {post_create_storage_usage}"))
        }
    }).unwrap();

    // Insert some data into the table.
    for _ in 0..3 {
        client
            .batch_execute("INSERT INTO usage_test VALUES (1)")
            .unwrap();
    }

    // Test that the storage usage for the table is larger than it was before.
    Retry::default().max_tries(10).retry(|_| {
        timestamp = wait_for_next_collection(&mut client, timestamp);
        let after_insert_storage_usage = get_storage_usage(&mut client, &shard_id, timestamp);
        info!(%after_insert_storage_usage);
        if after_insert_storage_usage > post_create_storage_usage {
            Ok(())
        } else {
            Err(format!("expected insert storage usage, {after_insert_storage_usage}, to be higher than pre insert storage usage of {post_create_storage_usage}"))
        }
    }).unwrap();

    // Drop the table.
    client.batch_execute("DROP TABLE usage_test").unwrap();

    // Test that the storage usage is reported as zero.
    Retry::default()
        .max_tries(10)
        .retry(|_| {
            timestamp = wait_for_next_collection(&mut client, timestamp);
            let after_drop_storage_usage = get_storage_usage(&mut client, &shard_id, timestamp);
            info!(%after_drop_storage_usage);
            if after_drop_storage_usage == 0 {
                Ok(())
            } else {
                Err(format!(
                    "expected zero storage usage after drop, found {after_drop_storage_usage}"
                ))
            }
        })
        .unwrap();
}

#[mz_ore::test]
fn test_storage_usage_updates_between_restarts() {
    let data_dir = tempfile::tempdir().unwrap();
    let storage_usage_collection_interval = Duration::from_secs(3);
    let harness = test_util::TestHarness::default()
        .with_storage_usage_collection_interval(storage_usage_collection_interval)
        .data_directory(data_dir.path());

    // Wait for initial storage usage collection.
    let initial_timestamp: f64 = {
        let server = harness.clone().start_blocking();
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
        let server = harness.start_blocking();
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

#[mz_ore::test]
#[cfg_attr(coverage, ignore)] // https://github.com/MaterializeInc/database-issues/issues/5584
fn test_storage_usage_doesnt_update_between_restarts() {
    let data_dir = tempfile::tempdir().unwrap();
    let storage_usage_collection_interval = Duration::from_secs(10);
    let harness = test_util::TestHarness::default()
        .with_storage_usage_collection_interval(storage_usage_collection_interval)
        .data_directory(data_dir.path());

    // Wait for initial storage usage collection.
    let initial_timestamp = {
        let server = harness.clone().start_blocking();
        let mut client = server.connect(postgres::NoTls).unwrap();
        // Retry because it may take some time for the initial snapshot to be taken.
        Retry::default().max_duration(Duration::from_secs(60)).retry(|_| {
                client
                    .query_one(
                        "SELECT DISTINCT(EXTRACT(EPOCH FROM MAX(collection_timestamp))::float8) FROM mz_catalog.mz_storage_usage;",
                        &[],
                    )
                    .map_err(|e| e.to_string()).unwrap()
                    .try_get::<_, f64>(0)
                    .map_err(|e| e.to_string())
            }).unwrap()
    };

    // Another storage usage collection should not be scheduled immediately.
    {
        // Give plenty of time, so we don't accidentally do another collection if this test is slow.
        let server = harness
            .with_storage_usage_collection_interval(Duration::from_secs(60 * 1000))
            .start_blocking();
        let mut client = server.connect(postgres::NoTls).unwrap();

        let collection_timestamps = client
            .query(
                "SELECT DISTINCT(EXTRACT(EPOCH FROM collection_timestamp)::float8) as epoch FROM mz_catalog.mz_storage_usage ORDER BY epoch DESC LIMIT 2;",
                &[],
            ).unwrap();
        match collection_timestamps.len() {
            0 => panic!("storage usage disappeared"),
            1 => assert_eq!(initial_timestamp, collection_timestamps[0].get::<_, f64>(0)),
            // It's possible that after collecting the first usage timestamp but before shutting the
            // server down, we collect another usage timestamp.
            2 => {
                let most_recent_timestamp = collection_timestamps[0].get::<_, f64>(0);
                let second_most_recent_timestamp = collection_timestamps[1].get::<_, f64>(0);

                let actual_collection_interval =
                    most_recent_timestamp - second_most_recent_timestamp;
                let expected_collection_interval: f64 =
                    f64::cast_lossy(storage_usage_collection_interval.as_secs());

                assert!(
                    // Add 1 second grace period to avoid flaky tests.
                    actual_collection_interval >= expected_collection_interval - 1.0,
                    "actual_collection_interval={actual_collection_interval}, expected_collection_interval={expected_collection_interval}"
                );
            }
            _ => unreachable!("query is limited to 2"),
        }
    }
}

// Test that all rows for a single collection use the same timestamp.
#[mz_ore::test]
fn test_storage_usage_collection_interval_timestamps() {
    let storage_interval_s = 2;
    let server = test_util::TestHarness::default()
        .with_storage_usage_collection_interval(Duration::from_secs(storage_interval_s))
        .start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();

    // Retry because it may take some time for the initial snapshot to be taken.
    let rows = Retry::default()
        .max_duration(Duration::from_secs(10))
        .retry(|_| {
            let rows = client
                .query(
                    "SELECT EXTRACT(EPOCH FROM collection_timestamp), ARRAY_AGG(object_id) \
                FROM mz_catalog.mz_storage_usage \
                GROUP BY collection_timestamp \
                ORDER BY collection_timestamp ASC;",
                    &[],
                )
                .map_err(|e| e.to_string())
                .unwrap();

            if rows.is_empty() {
                Err("expected some timestamp, instead found None".to_string())
            } else {
                Ok(rows)
            }
        })
        .unwrap();

    // The timestamp is selected after the storage usage is collected, so depending on how long
    // each collection takes, the timestamps can be arbitrarily close together or far apart. If
    // there are multiple timestamps, we ensure that they each contain the full set of object
    // IDs.
    let object_ids_by_timestamp: Vec<BTreeSet<_>> = rows
        .into_iter()
        .map(|row| row.get::<_, Array<String>>(1))
        .map(|array| array.into_iter().collect())
        .collect();
    let mut prev = None;
    for object_ids in object_ids_by_timestamp {
        match prev {
            None => {
                prev = Some(object_ids);
            }
            Some(ref prev_object_ids) => {
                assert_eq!(
                    prev_object_ids, &object_ids,
                    "found different storage collection timestamps, that contained non-matching \
                        object IDs. {prev_object_ids:?} and {object_ids:?}"
                );
            }
        }
    }
}

#[mz_ore::test]
fn test_old_storage_usage_records_are_reaped_on_restart() {
    let now = Arc::new(Mutex::new(0));
    let now_fn = {
        let timestamp = Arc::clone(&now);
        NowFn::from(move || *timestamp.lock().expect("lock poisoned"))
    };
    let data_dir = tempfile::tempdir().unwrap();
    let collection_interval = Duration::from_millis(100);
    let retention_period = Duration::from_millis(200);
    let harness = test_util::TestHarness::default()
        .with_now(now_fn)
        .with_storage_usage_collection_interval(collection_interval)
        .with_storage_usage_retention_period(retention_period)
        .data_directory(data_dir.path());

    let initial_timestamp = {
        let server = harness.clone().start_blocking();
        let mut client = server.connect(postgres::NoTls).unwrap();

        // Create a table with no data, which should have some overhead and therefore some storage usage
        client
            .batch_execute("CREATE TABLE usage_test (a int)")
            .unwrap();

        *now.lock().expect("lock poisoned") +=
            u64::try_from(collection_interval.as_millis()).expect("known to fit") + 1;

        // Wait for initial storage usage collection, to be sure records are present.
        let initial_timestamp = Retry::default().max_duration(Duration::from_secs(5)).retry(|_| {
                client
                    .query_one(
                        "SELECT (EXTRACT(EPOCH FROM MAX(collection_timestamp)) * 1000)::integer FROM mz_internal.mz_storage_usage_by_shard;",
                        &[],
                    )
                    .map_err(|e| e.to_string())?
                    .try_get::<_, i32>(0)
                    .map_err(|e| e.to_string())
            }).expect("Could not fetch initial timestamp");

        let initial_storage_usage_records = client
            .query_one(
                "SELECT COUNT(*)::integer AS number
                     FROM mz_internal.mz_storage_usage_by_shard",
                &[],
            )
            .unwrap()
            .try_get::<_, i32>(0)
            .expect("Could not get initial count of records");

        info!(%initial_timestamp, %initial_storage_usage_records);
        assert!(
            initial_storage_usage_records >= 1,
            "No initial server usage records!"
        );

        initial_timestamp
    };

    // Push time forward, start a new server, and assert that the previous storage records have been reaped
    *now.lock().expect("lock poisoned") = u64::try_from(initial_timestamp)
        .expect("negative timestamps are impossible")
        + u64::try_from(retention_period.as_millis()).expect("known to fit")
        // Add a second to account for any rounding errors.
        + 1000;

    {
        let server = harness.start_blocking();
        let mut client = server.connect(postgres::NoTls).unwrap();

        *now.lock().expect("lock poisoned") +=
            u64::try_from(collection_interval.as_millis()).expect("known to fit") + 1000;

        let subsequent_initial_timestamp = Retry::default().max_duration(Duration::from_secs(30)).retry(|_| {
                client
                    .query_one(
                        &format!("SELECT ts FROM (SELECT (EXTRACT(EPOCH FROM MIN(collection_timestamp)) * 1000)::integer as ts FROM mz_internal.mz_storage_usage_by_shard) WHERE ts > {initial_timestamp};"),
                        &[],
                    )
                    .map_err(|e| e.to_string())?
                    .try_get::<_, i32>(0)
                    .map_err(|e| e.to_string())
            }).expect("Could not fetch initial timestamp");

        info!(%subsequent_initial_timestamp);
        assert!(
            subsequent_initial_timestamp > initial_timestamp,
            "Records were not reaped!"
        );
    };
}

#[mz_ore::test]
fn test_storage_usage_records_are_not_cleared_on_restart() {
    let data_dir = tempfile::tempdir().unwrap();
    let collection_interval = Duration::from_secs(1);
    // Intentionally set retention period extremely high so that records aren't reaped.
    let retention_period = Duration::from_secs(u64::MAX);
    let harness = test_util::TestHarness::default()
        .with_storage_usage_collection_interval(collection_interval)
        .with_storage_usage_retention_period(retention_period)
        .data_directory(data_dir.path());

    let (initial_timestamp, initial_storage_usage_records) = {
        let server = harness.clone().start_blocking();
        let mut client = server.connect(postgres::NoTls).unwrap();
        // Create a table with no data, which should have some overhead and therefore some storage usage.
        client
            .batch_execute("CREATE TABLE usage_test (a int)")
            .unwrap();

        // Wait for initial storage usage collection, to be sure records are present.
        let initial_timestamp = Retry::default().max_duration(Duration::from_secs(5)).retry(|_| {
            client
                .query_one(
                    "SELECT (EXTRACT(EPOCH FROM MIN(collection_timestamp)) * 1000)::int8 FROM mz_internal.mz_storage_usage_by_shard;",
                    &[],
                )
                .map_err(|e| e.to_string()).unwrap()
                .try_get::<_, i64>(0)
                .map_err(|e| e.to_string())
        }).expect("Could not fetch initial timestamp");

        let initial_storage_usage_records = client
            .query_one(
                "SELECT COUNT(*)::integer AS number
                     FROM mz_internal.mz_storage_usage_by_shard",
                &[],
            )
            .unwrap()
            .try_get::<_, i32>(0)
            .expect("Could not get initial count of records");

        info!(%initial_timestamp, %initial_storage_usage_records);
        assert!(
            initial_storage_usage_records >= 1,
            "No initial server usage records!"
        );

        (initial_timestamp, initial_storage_usage_records)
    };

    {
        let server = harness.start_blocking();
        let mut client = server.connect(postgres::NoTls).unwrap();

        let subsequent_initial_timestamp =client
            .query_one(
                "SELECT (EXTRACT(EPOCH FROM MIN(collection_timestamp)) * 1000)::int8 FROM mz_internal.mz_storage_usage_by_shard;",
                &[],
            )
            .map_err(|e| e.to_string()).unwrap()
            .try_get::<_, i64>(0)
            .expect("Could not fetch subsequent initial timestamp");

        let subsequent_storage_usage_records = client
            .query_one(
                "SELECT COUNT(*)::integer AS number
                     FROM mz_internal.mz_storage_usage_by_shard",
                &[],
            )
            .unwrap()
            .try_get::<_, i32>(0)
            .expect("Could not get initial count of records");

        info!(%subsequent_initial_timestamp, %subsequent_storage_usage_records);
        assert_eq!(
            subsequent_initial_timestamp, initial_timestamp,
            "storage usage records were cleared"
        );
        assert!(
            subsequent_storage_usage_records >= initial_storage_usage_records,
            "storage usage was cleared!"
        );
    };
}

#[mz_ore::test]
fn test_default_cluster_sizes() {
    let server = test_util::TestHarness::default()
        .with_builtin_system_cluster_replica_size("scale=1,workers=1".to_string())
        .with_builtin_catalog_server_cluster_replica_size("scale=1,workers=1".to_string())
        .with_default_cluster_replica_size("scale=1,workers=2".to_string())
        .start_blocking();
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
    assert_eq!(builtin_size, "scale=1,workers=1");

    let builtin_size: String = client
        .query(
            "SELECT size FROM (SHOW CLUSTER REPLICAS WHERE cluster = 'quickstart')",
            &[],
        )
        .unwrap()
        .get(0)
        .unwrap()
        .get(0);
    assert_eq!(builtin_size, "scale=1,workers=2");
}

#[mz_ore::test]
#[ignore] // TODO: Reenable when https://github.com/MaterializeInc/database-issues/issues/6931 is fixed
fn test_max_request_size() {
    let statement = "SELECT $1::text";
    let statement_size = statement.bytes().count();
    let server = test_util::TestHarness::default().start_blocking();

    // pgwire
    {
        let param_size = mz_pgwire_common::MAX_REQUEST_SIZE - statement_size + 1;
        let param = std::iter::repeat("1").take(param_size).join("");
        let mut client = server.connect(postgres::NoTls).unwrap();

        let err = client.query(statement, &[&param]).unwrap_db_error();
        assert_contains!(err.message(), "request larger than");
        assert_err!(client.is_valid(Duration::from_secs(2)));
    }

    // http
    {
        let param_size = mz_environmentd::http::MAX_REQUEST_SIZE - statement_size + 1;
        let param = std::iter::repeat("1").take(param_size).join("");
        let http_url = Url::parse(&format!("http://{}/api/sql", server.http_local_addr())).unwrap();
        let json = format!("{{\"queries\":[{{\"query\":\"{statement}\",\"params\":[{param}]}}]}}");
        let json: serde_json::Value = serde_json::from_str(&json).unwrap();
        let res = Client::new().post(http_url).json(&json).send().unwrap();
        assert_eq!(res.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    // ws
    {
        let param_size = mz_environmentd::http::MAX_REQUEST_SIZE - statement_size + 1;
        let param = std::iter::repeat("1").take(param_size).join("");
        let ws_url = server.ws_addr();
        let (mut ws, _resp) = tungstenite::connect(ws_url).unwrap();
        test_util::auth_with_ws(&mut ws, BTreeMap::default()).unwrap();
        let json =
            format!("{{\"queries\":[{{\"query\":\"{statement}\",\"params\":[\"{param}\"]}}]}}");
        let json: serde_json::Value = serde_json::from_str(&json).unwrap();
        ws.send(Message::Text(json.to_string().into())).unwrap();

        // The specific error isn't forwarded to the client, the connection is just closed.
        let err = ws.read().unwrap_err();
        assert!(matches!(
            err,
            Error::Protocol(ProtocolError::ResetWithoutClosingHandshake)
        ));
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_max_statement_batch_size() {
    let statement = "SELECT 1;";
    let statement_size = statement.bytes().count();
    let max_statement_size = mz_sql_parser::parser::MAX_STATEMENT_BATCH_SIZE;
    let max_statement_count = max_statement_size / statement_size + 1;
    let statements = iter::repeat(statement).take(max_statement_count).join("");
    let server = test_util::TestHarness::default().start_blocking();

    // pgwire
    {
        let mut client = server.connect(postgres::NoTls).unwrap();

        let err = client
            .batch_execute(&statements)
            .expect_err("statement should be too large")
            .unwrap_db_error();
        assert_eq!(&SqlState::PROGRAM_LIMIT_EXCEEDED, err.code());
        assert!(
            err.message().contains("statement batch size cannot exceed"),
            "error should indicate that the statement was too large: {}",
            err.message()
        );
    }

    // http
    {
        let http_url = Url::parse(&format!("http://{}/api/sql", server.http_local_addr())).unwrap();
        let json = format!("{{\"query\":\"{statements}\"}}");
        let json: serde_json::Value = serde_json::from_str(&json).unwrap();

        let res = Client::new().post(http_url).json(&json).send().unwrap();
        let body: serde_json::Value = res.json().unwrap();
        let is_error = body
            .get("results")
            .unwrap()
            .get(0)
            .unwrap()
            .get("error")
            .is_some();
        assert!(is_error, "statement should result in an error: {body}");
        assert!(
            body.get("results")
                .unwrap()
                .get(0)
                .unwrap()
                .get("error")
                .unwrap()
                .get("message")
                .unwrap()
                .as_str()
                .unwrap()
                .contains("statement batch size cannot exceed"),
            "error should indicate that the statement was too large: {}",
            body
        );
    }

    // ws
    {
        let ws_url = server.ws_addr();
        let (mut ws, _resp) = tungstenite::connect(ws_url).unwrap();
        test_util::auth_with_ws(&mut ws, BTreeMap::default()).unwrap();
        let json = format!("{{\"query\":\"{statements}\"}}");
        let json: serde_json::Value = serde_json::from_str(&json).unwrap();
        ws.send(Message::Text(json.to_string().into())).unwrap();

        // Discard the CommandStarting message
        let _ = ws.read().unwrap();

        let msg = ws.read().unwrap();
        let msg = msg.into_text().expect("response should be text");
        let msg: WebSocketResponse = serde_json::from_str(&msg).unwrap();
        match msg {
            WebSocketResponse::Error(err) => assert!(
                err.message.contains("statement batch size cannot exceed"),
                "error should indicate that the statement was too large: {}",
                err.message,
            ),
            msg => {
                panic!("response should be error: {msg:?}")
            }
        }
    }
}

#[mz_ore::test]
fn test_mz_system_user_admin() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client = server
        .pg_config_internal()
        .user(&SYSTEM_USER.name)
        .connect(postgres::NoTls)
        .unwrap();
    assert_eq!(
        "on".to_string(),
        client
            .query_one("SHOW is_superuser;", &[])
            .unwrap()
            .get::<_, String>(0)
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_ws_passes_options() {
    let server = test_util::TestHarness::default().start_blocking();

    // Create our WebSocket.
    let ws_url = server.ws_addr();
    let (mut ws, _resp) = tungstenite::connect(ws_url).unwrap();
    let options = BTreeMap::from([(
        "application_name".to_string(),
        "billion_dollar_idea".to_string(),
    )]);
    test_util::auth_with_ws(&mut ws, options).unwrap();

    // Query to make sure we get back the correct session var, which should be
    // set from the options map we passed with the auth.
    let json = "{\"query\":\"SHOW application_name;\"}";
    let json: serde_json::Value = serde_json::from_str(json).unwrap();
    ws.send(Message::Text(json.to_string().into())).unwrap();

    let mut read_msg = || -> WebSocketResponse {
        let msg = ws.read().unwrap();
        let msg = msg.into_text().expect("response should be text");
        serde_json::from_str(&msg).unwrap()
    };
    let starting = read_msg();
    let columns = read_msg();
    let row_val = read_msg();

    if !matches!(starting, WebSocketResponse::CommandStarting(_)) {
        panic!("wrong message!, {starting:?}");
    };

    if let WebSocketResponse::Rows(rows) = columns {
        let names: Vec<&str> = rows.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["application_name"]);
    } else {
        panic!("wrong message!, {columns:?}");
    };

    if let WebSocketResponse::Row(row) = row_val {
        let expected = serde_json::Value::String("billion_dollar_idea".to_string());
        assert_eq!(row, [expected]);
    } else {
        panic!("wrong message!, {row_val:?}");
    }
}

#[mz_ore::test]
// Test that statement logging
// doesn't cause a crash with subscribes over web sockets,
// which was previously happening (in staging) due to us
// dropping the `ExecuteContext` on the floor in that case.
fn test_ws_subscribe_no_crash() {
    let server = test_util::TestHarness::default()
        .with_system_parameter_default(
            "statement_logging_max_sample_rate".to_string(),
            "1.0".to_string(),
        )
        .with_system_parameter_default(
            "statement_logging_default_sample_rate".to_string(),
            "1.0".to_string(),
        )
        .start_blocking();

    // Create our WebSocket.
    let ws_url = server.ws_addr();
    let (mut ws, _resp) = tungstenite::connect(ws_url).unwrap();
    test_util::auth_with_ws(&mut ws, Default::default()).unwrap();

    let query = "SUBSCRIBE (SELECT 1)";
    let json = format!("{{\"query\":\"{query}\"}}");
    let json: serde_json::Value = serde_json::from_str(&json).unwrap();
    ws.send(Message::Text(json.to_string().into())).unwrap();

    // Give the server time to crash, if it's going to.
    std::thread::sleep(Duration::from_secs(1))
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_ws_notifies_for_bad_options() {
    let server = test_util::TestHarness::default().start_blocking();

    // Create our WebSocket.
    let ws_url = server.ws_addr();
    let (mut ws, _resp) = tungstenite::connect(ws_url).unwrap();
    let options = BTreeMap::from([("bad_var_name".to_string(), "i_do_not_exist".to_string())]);
    test_util::auth_with_ws(&mut ws, options).unwrap();

    let mut read_msg = || -> WebSocketResponse {
        let msg = ws.read().unwrap();
        let msg = msg.into_text().expect("response should be text");
        serde_json::from_str(&msg).unwrap()
    };
    let notice = read_msg();

    // After startup, we should get a notice that our var name was not set.
    if let WebSocketResponse::Notice(notice) = notice {
        let msg = notice.message();
        assert!(msg.starts_with("startup setting bad_var_name not set"));
    } else {
        panic!("wrong message!, {notice:?}");
    };
}

#[derive(Debug, Deserialize)]
struct HttpResponse<R> {
    results: Vec<R>,
}

#[derive(Debug, Deserialize)]
struct HttpRows {
    rows: Vec<Vec<serde_json::Value>>,
    desc: HttpResponseDesc,
    notices: Vec<Notice>,
}

#[derive(Debug, Deserialize)]
struct HttpResponseDesc {
    columns: Vec<HttpResponseColumn>,
}

#[derive(Debug, Deserialize)]
struct HttpResponseColumn {
    name: String,
}

#[derive(Debug, Deserialize)]
struct Notice {
    message: String,
    #[serde(rename = "severity")]
    _severity: String,
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_http_options_param() {
    let server = test_util::TestHarness::default().start_blocking();

    #[derive(Debug, Serialize)]
    struct Params {
        options: String,
    }

    let make_request = |params| {
        let http_url = Url::parse(&format!(
            "http://{}/api/sql?{}",
            server.http_local_addr(),
            params
        ))
        .unwrap();

        let json = r#"{ "query": "SHOW application_name;" }"#;
        let json: serde_json::Value = serde_json::from_str(json).unwrap();
        Client::new().post(http_url).json(&json).send().unwrap()
    };

    //
    // Happy path, valid and correctly formatted options dictionary.
    //
    let options = BTreeMap::from([("application_name", "yet_another_client")]);
    let options = serde_json::to_string(&options).unwrap();
    let params = serde_urlencoded::to_string(Params { options }).unwrap();

    let resp = make_request(params);
    assert!(resp.status().is_success());

    let mut result: HttpResponse<HttpRows> = resp.json().unwrap();
    assert_eq!(result.results.len(), 1);

    let mut rows = result.results.pop().unwrap();
    assert!(rows.notices.is_empty());

    let row = rows.rows.pop().unwrap().pop().unwrap();
    let col = rows.desc.columns.pop().unwrap().name;

    assert_eq!(col, "application_name");
    assert_eq!(row, "yet_another_client");

    //
    // Malformed options dictionary.
    //
    let params = "options=not_a_urlencoded_json_object".to_string();
    let resp = make_request(params);
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    //
    // Correctly formed options dictionary, but invalid param.
    //
    let options = BTreeMap::from([("not_a_session_var", "hmmmm")]);
    let options = serde_json::to_string(&options).unwrap();
    let params = serde_urlencoded::to_string(Params { options }).unwrap();

    let resp = make_request(params);
    assert!(resp.status().is_success());

    let result: HttpResponse<HttpRows> = resp.json().unwrap();
    assert_eq!(result.results.len(), 1);
    assert_eq!(result.results[0].notices.len(), 1);

    let notice = &result.results[0].notices[0];
    assert!(
        notice
            .message
            .contains(r#"startup setting not_a_session_var not set"#)
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_max_connections_on_all_interfaces() {
    let query = "SELECT 1";
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();

    let mut mz_client = server
        .pg_config_internal()
        .user(&SYSTEM_USER.name)
        .connect(postgres::NoTls)
        .unwrap();
    mz_client
        .batch_execute("ALTER SYSTEM SET superuser_reserved_connections = 1")
        .unwrap();
    mz_client
        .batch_execute("ALTER SYSTEM SET max_connections = 2")
        .unwrap();

    let client = server.connect(postgres::NoTls).unwrap();

    let ws_url = server.ws_addr();
    let http_url = Url::parse(&format!("http://{}/api/sql", server.http_local_addr())).unwrap();
    let json = format!("{{\"query\":\"{query}\"}}");
    let json: serde_json::Value = serde_json::from_str(&json).unwrap();

    {
        // while postgres client is connected, http connections error out
        let res = Client::new()
            .post(http_url.clone())
            .json(&json)
            .send()
            .unwrap();
        let status = res.status();
        let text = res.text().expect("no body?");
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_contains!(
            text,
            "creating connection would violate max_connections limit (desired: 2, limit: 2, current: 1)"
        );
    }

    {
        // while postgres client is connected, websockets can't auth
        let (mut ws, _resp) = tungstenite::connect(ws_url.clone()).unwrap();
        let err = test_util::auth_with_ws(&mut ws, BTreeMap::default()).unwrap_err();
        assert_contains!(
            err.to_string_with_causes(),
            "creating connection would violate max_connections limit (desired: 2, limit: 2, current: 1)"
        );
    }

    tracing::info!("closing postgres client");
    client.close().unwrap();
    tracing::info!("closed postgres client");

    tracing::info!("waiting for postgres client to close so that the query goes through");
    Retry::default().max_tries(10).retry(|_state| {
            let res = Client::new().post(http_url.clone()).json(&json).send().unwrap();
            let status = res.status();
            if status == StatusCode::INTERNAL_SERVER_ERROR {
                assert_contains!(res.text().expect("expect body"), "creating connection would violate max_connections limit (desired: 2, limit: 2, current: 1)");
                return Err(());
            }
            assert_eq!(status, StatusCode::OK);
            let result: HttpResponse<HttpRows> = res.json().unwrap();
            assert_eq!(result.results.len(), 1);
            assert_eq!(result.results[0].rows, vec![vec!["1"]]);
            Ok(())
        }).unwrap();

    tracing::info!("http query succeeded");
    let (mut ws, _resp) = tungstenite::connect(ws_url).unwrap();
    test_util::auth_with_ws(&mut ws, BTreeMap::default()).unwrap();
    let json = format!("{{\"query\":\"{query}\"}}");
    let json: serde_json::Value = serde_json::from_str(&json).unwrap();
    ws.send(Message::Text(json.to_string().into())).unwrap();

    // The specific error isn't forwarded to the client, the connection is just closed.
    match ws.read() {
        Ok(Message::Text(msg)) => {
            assert_eq!(
                msg,
                r#"{"type":"CommandStarting","payload":{"has_rows":true,"is_streaming":false}}"#
            );
            assert_eq!(
                ws.read().unwrap(),
                Message::Text(format!(
                    r#"{{"type":"Rows","payload":{{"columns":[{{"name":"{UNKNOWN_COLUMN_NAME}","type_oid":23,"type_len":4,"type_mod":-1}}]}}}}"#
                ).into())
            );
            assert_eq!(
                ws.read().unwrap(),
                Message::Text("{\"type\":\"Row\",\"payload\":[\"1\"]}".to_string().into())
            );
            tracing::info!("data: {:?}", ws.read().unwrap());
        }
        Ok(msg) => panic!("unexpected msg: {msg:?}"),
        Err(e) => panic!("{e}"),
    }

    // While the websocket connection is still open, http requests fail
    let res = Client::new().post(http_url).json(&json).send().unwrap();
    tracing::info!("res: {:#?}", res);
    let status = res.status();
    let text = res.text().expect("no body?");
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_contains!(
        text,
        "creating connection would violate max_connections limit (desired: 2, limit: 2, current: 1)"
    );

    // Make sure lowering our max connections below the current limit does not
    // cause a panic.
    mz_client
        .batch_execute("ALTER SYSTEM SET max_connections = 10")
        .unwrap();

    // Open a few connections.
    let mut clients = Vec::new();
    for _ in 0..5 {
        let client = server
            .pg_config()
            .connect(postgres::NoTls)
            .expect("success opening client");
        clients.push(client);
    }

    // Lower the connection below the number of open clients.
    mz_client
        .batch_execute("ALTER SYSTEM SET max_connections = 2")
        .unwrap();

    // Opening a new connection should fail.
    let result = server.pg_config().connect(postgres::NoTls);
    let Err(failure) = result else {
        panic!("unexpected success connecting to server");
    };
    assert_contains!(
        failure.to_string_with_causes(),
        "creating connection would violate max_connections limit (desired: 7, limit: 2, current: 6)"
    );
}

// Test max_connections and superuser_reserved_connections.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
async fn test_max_connections_limits() {
    let ca = Ca::new_root("test ca").unwrap();
    let (server_cert, server_key) = ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let metrics_registry = MetricsRegistry::new();

    let tenant_id = Uuid::new_v4();
    let regular_user = UserConfig::generate(tenant_id.clone(), "user@_.com", Vec::new());
    let admin_user = UserConfig::generate(tenant_id.clone(), "admin@_.com", vec!["mzadmin".into()]);
    let users = BTreeMap::from([
        (regular_user.email.clone(), regular_user.clone()),
        (admin_user.email.clone(), admin_user.clone()),
    ]);

    let issuer = "frontegg-mock".to_owned();
    let encoding_key =
        EncodingKey::from_rsa_pem(&ca.pkey.private_key_to_pem_pkcs8().unwrap()).unwrap();
    let decoding_key = DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap();

    let frontegg_server = FronteggMockServer::start(
        None,
        issuer,
        encoding_key,
        decoding_key,
        users,
        BTreeMap::new(),
        None,
        SYSTEM_TIME.clone(),
        500,
        None,
        None,
    )
    .await
    .unwrap();

    let frontegg_auth = FronteggAuthentication::new(
        FronteggConfig {
            admin_api_token_url: frontegg_server.auth_api_token_url(),
            decoding_key: DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap(),
            tenant_id: Some(tenant_id),
            now: SYSTEM_TIME.clone(),
            admin_role: "mzadmin".to_string(),
            refresh_drop_lru_size: DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
            refresh_drop_factor: DEFAULT_REFRESH_DROP_FACTOR,
        },
        mz_frontegg_auth::Client::default(),
        &metrics_registry,
    );

    let server = test_util::TestHarness::default()
        .with_tls(server_cert, server_key)
        .with_frontegg_auth(&frontegg_auth)
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    let tls = make_pg_tls(|b| Ok(b.set_verify(SslVerifyMode::NONE)));

    let connect_regular_user = || async {
        server
            .connect()
            .ssl_mode(SslMode::Require)
            .with_tls(tls.clone())
            .user(&regular_user.email)
            .password(&regular_user.frontegg_password())
            .await
    };
    let connect_external_admin = || async {
        server
            .connect()
            .ssl_mode(SslMode::Require)
            .with_tls(tls.clone())
            .user(&admin_user.email)
            .password(&admin_user.frontegg_password())
            .await
    };
    let connect_system_user = || async { server.connect().internal().await };

    let mz_client = connect_system_user().await.unwrap();
    mz_client
        .batch_execute("ALTER SYSTEM SET max_connections TO 1")
        .await
        .unwrap();
    mz_client
        .batch_execute("ALTER SYSTEM SET superuser_reserved_connections = 1")
        .await
        .unwrap();

    // No regular user connections when max_connections = superuser_reserved_connections.
    assert_contains!(
        connect_regular_user()
            .await
            .expect_err("connect should fail")
            .to_string_with_causes(),
        "creating connection would violate max_connections limit"
    );

    // Set this real high to make sure nothing bad happens.
    mz_client
        .batch_execute("ALTER SYSTEM SET superuser_reserved_connections = 100")
        .await
        .unwrap();
    assert_contains!(
        connect_regular_user()
            .await
            .expect_err("connect should fail")
            .to_string_with_causes(),
        "creating connection would violate max_connections limit"
    );

    mz_client
        .batch_execute("ALTER SYSTEM SET superuser_reserved_connections = 0")
        .await
        .unwrap();

    {
        let client1 = connect_regular_user().await.unwrap();
        let _ = client1.batch_execute("SELECT 1").await.unwrap();

        assert_contains!(
            connect_regular_user()
                .await
                .expect_err("connect should fail")
                .to_string_with_causes(),
            "creating connection would violate max_connections limit"
        );

        assert_contains!(
            connect_regular_user()
                .await
                .expect_err("connect should fail")
                .to_string_with_causes(),
            "creating connection would violate max_connections limit"
        );

        let mz_client2 = connect_system_user().await.unwrap();
        mz_client2
            .batch_execute("SELECT 1")
            .await
            .expect("super users are still allowed to do queries");

        // External admins can't get around the connection limit.
        assert_contains!(
            connect_external_admin()
                .await
                .expect_err("connect should fail")
                .to_string_with_causes(),
            "creating connection would violate max_connections limit"
        );

        // Bump the limits.
        mz_client
            .batch_execute("ALTER SYSTEM SET max_connections TO 2")
            .await
            .unwrap();
        mz_client
            .batch_execute("ALTER SYSTEM SET superuser_reserved_connections = 1")
            .await
            .unwrap();

        // Regular users still should not connect.
        assert_contains!(
            connect_regular_user()
                .await
                .expect_err("connect should fail")
                .to_string_with_causes(),
            "creating connection would violate max_connections limit"
        );

        // But external admin should succeed.
        connect_external_admin().await.unwrap();
    }
    // after a client disconnects we can connect once the server notices the close
    Retry::default()
        .max_tries(10)
        .retry_async(|_state| async {
            let client = match connect_regular_user().await {
                Err(e) => {
                    assert_contains!(
                        e.to_string_with_causes(),
                        "creating connection would violate max_connections limit"
                    );
                    return Err(());
                }
                Ok(client) => client,
            };
            let _ = client.batch_execute("SELECT 1").await.unwrap();
            Ok(())
        })
        .await
        .unwrap();

    mz_client
        .batch_execute("ALTER SYSTEM RESET max_connections")
        .await
        .unwrap();

    // We can create lots of clients now
    let mut clients = Vec::new();
    for _ in 0..10 {
        let client = connect_regular_user().await.unwrap();
        client.batch_execute("SELECT 1").await.unwrap();
        clients.push(client);
    }
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_concurrent_id_reuse() {
    let server = test_util::TestHarness::default().start().await;

    {
        let client = server.connect().await.unwrap();
        client
            .batch_execute("CREATE TABLE t (a INT);")
            .await
            .unwrap();
    }

    let http_url = Url::parse(&format!("http://{}/api/sql", server.http_local_addr())).unwrap();
    let select_json = "{\"queries\":[{\"query\":\"SELECT * FROM t;\",\"params\":[]}]}";
    let select_json: serde_json::Value = serde_json::from_str(select_json).unwrap();

    let insert_json = "{\"queries\":[{\"query\":\"INSERT INTO t VALUES (1);\",\"params\":[]}]}";
    let insert_json: serde_json::Value = serde_json::from_str(insert_json).unwrap();

    // The goal here is to start some connection `B`, after another connection `A` has
    // terminated, but while connection `A` still has some asynchronous work in flight. Then
    // connection `A` will terminate it's session after connection `B` has started it's own
    // session. If they use the same connection ID, then `A` will accidentally tear down `B`'s
    // state and `B` will panic at any point it tries to access it's state. If they don't use
    // the same connection ID, then everything will be fine.
    fail::cfg("async_prepare", "return(true)").unwrap();
    for i in 0..50 {
        let http_url = http_url.clone();
        if i % 2 == 0 {
            let fut = reqwest::Client::new()
                .post(http_url)
                .json(&select_json)
                .send();
            let time = tokio::time::sleep(Duration::from_millis(500));
            futures::select! {
                _ = fut.fuse() => {},
                _ = time.fuse() => {},
            }
        } else {
            reqwest::Client::new()
                .post(http_url)
                .json(&insert_json)
                .send()
                .await
                .unwrap();
        }
    }

    let client = server.connect().await.unwrap();
    client.batch_execute("SELECT 1").await.unwrap();
}

#[mz_ore::test]
fn test_internal_console_proxy() {
    let server = test_util::TestHarness::default().start_blocking();

    let res = Client::builder()
        .build()
        .unwrap()
        .get(
            Url::parse(&format!(
                "http://{}/internal-console/",
                server.internal_http_local_addr()
            ))
            .unwrap(),
        )
        .send()
        .unwrap();

    assert_eq!(res.status().is_success(), true);
    assert_contains!(
        res.headers().get(CONTENT_TYPE).unwrap().to_str().unwrap(),
        "text/html"
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_internal_http_auth() {
    let server = test_util::TestHarness::default().start_blocking();
    let json = serde_json::json!({"query": "SELECT current_user;"});
    let url = Url::parse(&format!(
        "http://{}/api/sql",
        server.internal_http_local_addr()
    ))
    .unwrap();

    let res = Client::new()
        .post(url.clone())
        .header("x-materialize-user", "mz_system")
        .json(&json)
        .send()
        .unwrap();

    tracing::info!("response: {res:?}");
    assert_eq!(
        res.status(),
        StatusCode::OK,
        "{:?}",
        res.json::<serde_json::Value>()
    );
    // can be explicitly set to mz_system
    assert!(res.text().unwrap().to_string().contains("mz_system"));

    let res = Client::new()
        .post(url.clone())
        .header("x-materialize-user", "mz_support")
        .json(&json)
        .send()
        .unwrap();

    tracing::info!("response: {res:?}");
    assert_eq!(
        res.status(),
        StatusCode::OK,
        "{:?}",
        res.json::<serde_json::Value>()
    );
    // can be explicitly set to mz_support
    assert!(res.text().unwrap().to_string().contains("mz_support"));

    let res = Client::new()
        .post(url.clone())
        .header("x-materialize-user", "invalid value")
        .json(&json)
        .send()
        .unwrap();

    tracing::info!("response: {res:?}");
    // invalid header returns an error
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED, "{:?}", res.text());
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_internal_ws_auth() {
    let server = test_util::TestHarness::default().start_blocking();

    // Create our WebSocket.
    let ws_url = server.internal_ws_addr();
    let make_req = || {
        Request::builder()
            .uri(ws_url.clone())
            .method("GET")
            .header("Host", ws_url.host().unwrap())
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", "foobar")
            // Set our user to the mz_support user
            .header("x-materialize-user", "mz_support")
            .body(())
            .unwrap()
    };

    let (mut ws, _resp) = tungstenite::connect(make_req()).unwrap();
    let options = BTreeMap::from([
        (
            "application_name".to_string(),
            "billion_dollar_idea".to_string(),
        ),
        ("welcome_message".to_string(), "off".to_string()),
    ]);
    // We should receive error if sending the standard bearer auth, since that is unexpected
    // for the Internal HTTP API
    assert_err!(test_util::auth_with_ws(&mut ws, options.clone()));

    // Recreate the websocket
    let (mut ws, _resp) = tungstenite::connect(make_req()).unwrap();
    // Auth with OptionsOnly
    test_util::auth_with_ws_impl(
        &mut ws,
        Message::Text(
            serde_json::to_string(&WebSocketAuth::OptionsOnly { options })
                .unwrap()
                .into(),
        ),
    )
    .unwrap();

    // Query to make sure we get back the correct user, which should be
    // set from the headers passed with the websocket request.
    let json = "{\"query\":\"SELECT current_user;\"}";
    let json: serde_json::Value = serde_json::from_str(json).unwrap();
    ws.send(Message::Text(json.to_string().into())).unwrap();

    let mut read_msg = || -> WebSocketResponse {
        let msg = ws.read().unwrap();
        let msg = msg.into_text().expect("response should be text");
        serde_json::from_str(&msg).unwrap()
    };
    let starting = read_msg();
    let columns = read_msg();
    let row_val = read_msg();

    if !matches!(starting, WebSocketResponse::CommandStarting(_)) {
        panic!("wrong message!, {starting:?}");
    };

    if let WebSocketResponse::Rows(rows) = columns {
        let names: Vec<&str> = rows.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["current_user"]);
    } else {
        panic!("wrong message!, {columns:?}");
    };

    if let WebSocketResponse::Row(row) = row_val {
        let expected = serde_json::Value::String("mz_support".to_string());
        assert_eq!(row, [expected]);
    } else {
        panic!("wrong message!, {row_val:?}");
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_leader_promotion_always_using_deploy_generation() {
    let tmpdir = TempDir::new().unwrap();
    let harness = test_util::TestHarness::default()
        .unsafe_mode()
        .data_directory(tmpdir.path())
        .with_deploy_generation(2);
    {
        // propose a deploy generation for the first time
        let server = harness.clone().start_blocking();
        let mut client = server.connect(postgres::NoTls).unwrap();
        client.simple_query("SELECT 1").unwrap();
    }
    {
        // keep it the same, no need to promote the leader
        let server = harness.start_blocking();
        let mut client = server.connect(postgres::NoTls).unwrap();
        client.simple_query("SELECT 1").unwrap();

        let http_client = reqwest::blocking::Client::new();

        // check that we're the leader and promotion doesn't do anything
        let status_http_url = Url::parse(&format!(
            "http://{}/api/leader/status",
            server.internal_http_local_addr()
        ))
        .unwrap();
        let res = http_client.get(status_http_url).send().unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let response = res.text().unwrap();
        assert_eq!(response, r#"{"status":"IsLeader"}"#);

        let promote_http_url = Url::parse(&format!(
            "http://{}/api/leader/promote",
            server.internal_http_local_addr()
        ))
        .unwrap();
        let res = http_client.post(promote_http_url).send().unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let response = res.text().unwrap();
        assert_eq!(response, r#"{"result":"Success"}"#);
    }
}

#[mz_ore::test(tokio::test(flavor = "multi_thread"))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_leader_promotion_mixed_code_version() {
    let tmpdir = TempDir::new().unwrap();
    let this_version = mz_environmentd::BUILD_INFO.semver_version();
    let next_version = semver::Version::new(this_version.major, this_version.minor + 1, 0);
    let harness = test_util::TestHarness::default()
        .unsafe_mode()
        .data_directory(tmpdir.path())
        .with_deploy_generation(1)
        .with_code_version(this_version);

    // Query a server at the current version before we start a second server.
    let server_this = harness.clone().start().await;
    let client_this = server_this.connect().await.unwrap();
    client_this.simple_query("SELECT 1").await.unwrap();

    // Simulate a rolling upgrade and wait for the preflight checks.
    let listeners_next = test_util::Listeners::new(&harness).await.unwrap();
    let internal_http_addr_next = listeners_next.inner.http["internal"].handle.local_addr;
    let config_next = harness
        .clone()
        .with_deploy_generation(2)
        .with_code_version(next_version.clone());
    let _server_next = mz_ore::task::spawn(|| "next version", async move {
        listeners_next.serve(config_next).await.unwrap()
    })
    .abort_on_drop();

    let status_http_url_next = Url::parse(&format!(
        "http://{}/api/leader/status",
        internal_http_addr_next
    ))
    .unwrap();
    // Wait for the new version to be ready to promote.
    Retry::default()
        .retry_async(|_state| async {
            let res = reqwest::Client::new()
                .get(status_http_url_next.clone())
                .send()
                .await
                .unwrap();
            tracing::info!("{} response: {res:?}", next_version);
            assert_eq!(res.status(), StatusCode::OK);
            let response = res.text().await.unwrap();
            tracing::info!("{} response body: {response:?}", next_version);
            assert_ne!(response, r#"{"status":"IsLeader"}"#);
            if response == r#"{"status":"ReadyToPromote"}"# {
                Ok(())
            } else {
                Err(())
            }
        })
        .await
        .unwrap();
    // The next_version preflight checks shouldn't fence out the this_version
    // server.
    client_this.simple_query("SELECT 1").await.unwrap();
}

// Test that websockets observe cancellation.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `epoll_wait` on OS `linux`
fn test_cancel_ws() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();
    client.batch_execute("CREATE TABLE t (i INT);").unwrap();

    // Start a thread to perform the cancel while the SUBSCRIBE is running in this thread.
    let handle = thread::spawn(move || {
        // Wait for the subscription to start.
        let conn_id = Retry::default()
            .retry(|_| {
                let conn_id: String = client
                    .query_one(
                        "SELECT s.connection_id::text FROM mz_internal.mz_subscriptions b JOIN mz_internal.mz_sessions s ON s.id = b.session_id",
                        &[],
                    )?
                    .get(0);
                Ok::<_, postgres::Error>(conn_id)
            })
            .unwrap();

        client
            .query_one(&format!("SELECT pg_cancel_backend({conn_id})"), &[])
            .unwrap();
    });

    let (mut ws, _resp) = tungstenite::connect(server.ws_addr()).unwrap();
    test_util::auth_with_ws(&mut ws, BTreeMap::default()).unwrap();
    let json = r#"{"queries":[{"query":"SUBSCRIBE t"}]}"#;
    let json: serde_json::Value = serde_json::from_str(json).unwrap();
    ws.send(Message::Text(json.to_string().into())).unwrap();

    loop {
        let msg = ws.read().unwrap();
        if let Ok(msg) = msg.into_text() {
            if msg.contains("canceling statement") {
                break;
            }
        }
    }
    handle.join().unwrap();
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn smoketest_webhook_source() {
    let server = test_util::TestHarness::default().start().await;
    let client = server.connect().await.unwrap();

    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Clone)]
    struct WebhookEvent {
        ts: u128,
        name: String,
        attrs: Vec<String>,
    }

    // Create a webhook source.
    client
        .execute(
            "CREATE CLUSTER webhook_cluster REPLICAS (r1 (SIZE 'scale=1,workers=1'));",
            &[],
        )
        .await
        .expect("failed to create cluster");
    client
        .execute(
            "CREATE SOURCE webhook_json IN CLUSTER webhook_cluster FROM WEBHOOK BODY FORMAT JSON",
            &[],
        )
        .await
        .expect("failed to create source");

    let now = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time went backwards")
            .as_nanos()
    };

    // Generate some events.
    const NUM_EVENTS: i64 = 100;
    let events: BTreeSet<_> = (0..NUM_EVENTS)
        .map(|i| WebhookEvent {
            ts: now(),
            name: format!("event_{i}"),
            attrs: (0..i).map(|j| format!("attr_{j}")).collect(),
        })
        .collect();

    let http_client = reqwest::Client::new();
    let webhook_url = Arc::new(format!(
        "http://{}/api/webhook/materialize/public/webhook_json",
        server.http_local_addr()
    ));
    // Send all of our events to our webhook source.
    let mut handles = Vec::with_capacity(events.len());
    for event in &events {
        let webhook_url = Arc::clone(&webhook_url);
        let event = event.clone();
        let http_client_ = http_client.clone();
        let handle = mz_ore::task::spawn(|| "smoketest_webhook_source_event", async move {
            let resp = http_client_
                .post(&*webhook_url)
                .json(&event)
                .send()
                .await
                .expect("failed to POST event");
            assert!(resp.status().is_success());
        });
        handles.push(handle);
    }
    futures::future::join_all(handles).await;

    let total_requests_metric = server
        .metrics_registry
        .gather()
        .into_iter()
        .find(|metric| metric.name() == "mz_http_requests_total")
        .unwrap();
    let total_requests_metric = &total_requests_metric.get_metric()[0];
    assert_eq!(total_requests_metric.get_counter().get_value(), 100.0);

    let path_label = &total_requests_metric.get_label()[0];
    assert_eq!(path_label.value(), "/api/webhook/:database/:schema/:id");

    let status_label = &total_requests_metric.get_label()[2];
    assert_eq!(status_label.value(), "200");

    // Wait for the events to be persisted.
    let (client, result) = mz_ore::retry::Retry::default()
        .max_tries(10)
        .retry_async_with_state(client, |_, client| async move {
            let cnt: i64 = client
                .query_one("SELECT COUNT(*) FROM webhook_json", &[])
                .await
                .expect("failed to get count")
                .get(0);

            if cnt != 100 {
                (client, Err(anyhow::anyhow!("not all rows present")))
            } else {
                (client, Ok(()))
            }
        })
        .await;
    result.expect("failed to read events");

    // Read all of our events back.
    let events_roundtrip: BTreeSet<WebhookEvent> = client
        .query("SELECT * FROM webhook_json", &[])
        .await
        .expect("failed to query source")
        .into_iter()
        .map(|row| serde_json::from_value(row.get("body")).expect("failed to deserialize"))
        .collect();

    similar_asserts::assert_eq!(events, events_roundtrip);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_invalid_webhook_body() {
    let server = test_util::TestHarness::default().start_blocking();

    let mut client = server.connect(postgres::NoTls).unwrap();
    let http_client = Client::new();

    // Create a cluster we can install webhook sources on.
    client
        .execute(
            "CREATE CLUSTER webhook_cluster REPLICAS (r1 (SIZE 'scale=1,workers=1'));",
            &[],
        )
        .expect("failed to create cluster");

    // Create a webhook source with a body format of text.
    client
        .execute(
            "CREATE SOURCE webhook_text IN CLUSTER webhook_cluster FROM WEBHOOK BODY FORMAT TEXT",
            &[],
        )
        .expect("failed to create source");
    let webhook_url = format!(
        "http://{}/api/webhook/materialize/public/webhook_text",
        server.http_local_addr()
    );

    // Send non-UTF8 text which will fail to get deserialized.
    let non_utf8 = vec![255, 255, 255, 255];
    assert_err!(std::str::from_utf8(&non_utf8));

    let resp = http_client
        .post(webhook_url)
        .body(non_utf8)
        .send()
        .expect("failed to POST event");
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // Create a webhook source with a body format of JSON.
    client
        .execute(
            "CREATE SOURCE webhook_json IN CLUSTER webhook_cluster FROM WEBHOOK BODY FORMAT JSON",
            &[],
        )
        .expect("failed to create source");
    let webhook_url = format!(
        "http://{}/api/webhook/materialize/public/webhook_json",
        server.http_local_addr()
    );

    // Send invalid JSON which will fail to get deserialized.
    let resp = http_client
        .post(webhook_url)
        .body("a")
        .send()
        .expect("failed to POST event");
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // Create a webhook source with a body format of bytes.
    client
        .execute(
            "CREATE SOURCE webhook_bytes IN CLUSTER webhook_cluster FROM WEBHOOK BODY FORMAT BYTES",
            &[],
        )
        .expect("failed to create source");
    let webhook_url = format!(
        "http://{}/api/webhook/materialize/public/webhook_bytes",
        server.http_local_addr()
    );

    // No matter what is in the body, we should always succeed.
    let mut data = [0u8; 128];
    rand::rng().fill_bytes(&mut data);
    println!("Random bytes: {data:?}");
    let resp = http_client
        .post(webhook_url)
        .body(data.to_vec())
        .send()
        .expect("failed to POST event");
    assert!(resp.status().is_success());
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_webhook_duplicate_headers() {
    let server = test_util::TestHarness::default().start_blocking();

    let mut client = server.connect(postgres::NoTls).unwrap();
    let http_client = Client::new();

    // Create a webhook source that includes headers.
    client
        .execute(
            "CREATE CLUSTER webhook_cluster REPLICAS (r1 (SIZE 'scale=1,workers=1'));",
            &[],
        )
        .expect("failed to create cluster");
    client
        .execute(
            "CREATE SOURCE webhook_text IN CLUSTER webhook_cluster FROM WEBHOOK BODY FORMAT TEXT INCLUDE HEADERS",
            &[],
        )
        .expect("failed to create source");
    let webhook_url = format!(
        "http://{}/api/webhook/materialize/public/webhook_text",
        server.http_local_addr()
    );

    // Send a request with duplicate headers.
    let resp = http_client
        .post(webhook_url)
        .body("test")
        .header("dupe", "first")
        .header("DUPE", "second")
        .header("dUpE", "third")
        .header("dUpE", "final")
        .send()
        .expect("failed to POST event");
    assert_eq!(resp.status().as_u16(), 401);
}

// Test that websockets observe cancellation and leave the transaction in an idle state.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `epoll_wait` on OS `linux`
fn test_github_20262() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();
    client.batch_execute("CREATE TABLE t (i INT);").unwrap();

    let mut cancel = || {
        // Wait for the subscription to start.
        let conn_id = Retry::default()
            .retry(|_| {
                let conn_id: String = client
                    .query_one(
                        "SELECT s.connection_id::text FROM mz_internal.mz_subscriptions b JOIN mz_internal.mz_sessions s ON s.id = b.session_id",
                        &[],
                    )?
                    .get(0);
                Ok::<_, postgres::Error>(conn_id)
            })
            .unwrap();
        client
            .query_one(&format!("SELECT pg_cancel_backend({conn_id})"), &[])
            .unwrap();
    };

    let subscribe: serde_json::Value =
        serde_json::from_str(r#"{"queries":[{"query":"SUBSCRIBE t"}]}"#).unwrap();
    let subscribe = subscribe.to_string();
    let commit: serde_json::Value =
        serde_json::from_str(r#"{"queries":[{"query":"COMMIT"}]}"#).unwrap();
    let commit = commit.to_string();
    let select: serde_json::Value =
        serde_json::from_str(r#"{"queries":[{"query":"SELECT 1"}]}"#).unwrap();
    let select = select.to_string();

    let (mut ws, _resp) = tungstenite::connect(server.ws_addr()).unwrap();
    test_util::auth_with_ws(&mut ws, BTreeMap::default()).unwrap();
    ws.send(Message::Text(subscribe.into())).unwrap();
    cancel();
    ws.send(Message::Text(commit.into())).unwrap();
    ws.send(Message::Text(select.into())).unwrap();

    let mut expect = VecDeque::from([
        r#"{"type":"CommandStarting","payload":{"has_rows":true,"is_streaming":true}}"#.to_string(),
        r#"{"type":"Rows","payload":{"columns":[{"name":"mz_timestamp","type_oid":1700,"type_len":-1,"type_mod":2555908},{"name":"mz_diff","type_oid":20,"type_len":8,"type_mod":-1},{"name":"i","type_oid":23,"type_len":4,"type_mod":-1}]}}"#.to_string(),
        r#"{"type":"Error","payload":{"message":"canceling statement due to user request","code":"57014"}}"#.to_string(),
        r#"{"type":"ReadyForQuery","payload":"I"}"#.to_string(),
        r#"{"type":"Notice","payload":{"message":"there is no transaction in progress","code":"25P01","severity":"warning"}}"#.to_string(),
        r#"{"type":"CommandStarting","payload":{"has_rows":false,"is_streaming":false}}"#.to_string(),
        r#"{"type":"CommandComplete","payload":"COMMIT"}"#.to_string(),
        r#"{"type":"ReadyForQuery","payload":"I"}"#.to_string(),
        r#"{"type":"CommandStarting","payload":{"has_rows":true,"is_streaming":false}}"#.to_string(),
        format!(r#"{{"type":"Rows","payload":{{"columns":[{{"name":"{UNKNOWN_COLUMN_NAME}","type_oid":23,"type_len":4,"type_mod":-1}}]}}}}"#),
        r#"{"type":"Row","payload":["1"]}"#.to_string(),
        r#"{"type":"CommandComplete","payload":"SELECT 1"}"#.to_string(),
        r#"{"type":"ReadyForQuery","payload":"I"}"#.to_string(),
    ]);
    while !expect.is_empty() {
        if let Message::Text(text) = ws.read().unwrap() {
            let next = expect.pop_front().unwrap();
            assert_eq!(text, next);
        }
    }
}

// Test that the server properly handles cancellation requests of read-then-write queries.
// See database-issues#6134.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `epoll_wait` on OS `linux`
fn test_cancel_read_then_write() {
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();
    server.enable_feature_flags(&["unsafe_enable_unsafe_functions"]);

    let mut client = server.connect(postgres::NoTls).unwrap();
    client
        .batch_execute("CREATE TABLE foo (a TEXT, ts INT)")
        .unwrap();

    // Lots of races here, so try this whole thing in a loop.
    Retry::default()
        .clamp_backoff(Duration::ZERO)
        .retry(|_state| {
            let mut client1 = server.connect(postgres::NoTls).unwrap();
            let mut client2 = server.connect(postgres::NoTls).unwrap();
            let cancel_token = client2.cancel_token();

            client1.batch_execute("DELETE FROM foo").unwrap();
            client1.batch_execute("SET statement_timeout = '5s'").unwrap();
            client1
                .batch_execute("INSERT INTO foo VALUES ('hello', 10)")
                .unwrap();

            let handle1 = thread::spawn(move || {
                let err =  client1
                    .batch_execute("insert into foo select a, case when mz_unsafe.mz_sleep(ts) > 0 then 0 end as ts from foo")
                    .unwrap_err();
                assert_contains!(
                    err.to_string_with_causes(),
                    "statement timeout"
                );
                client1
            });
            std::thread::sleep(Duration::from_millis(100));
            let handle2 = thread::spawn(move || {
                let err = client2
                .batch_execute("insert into foo values ('blah', 1);")
                .unwrap_err();
                assert_contains!(
                    err.to_string_with_causes(),
                    "canceling statement"
                );
            });
            std::thread::sleep(Duration::from_millis(100));
            cancel_token.cancel_query(postgres::NoTls)?;
            let mut client1 = handle1.join().unwrap();
            handle2.join().unwrap();
            let rows:i64 = client1.query_one ("SELECT count(*) FROM foo", &[]).unwrap().get(0);
            // We ran 3 inserts. First succeeded. Second timedout. Third cancelled.
            if rows !=1 {
                anyhow::bail!("unexpected row count: {rows}");
            }
            Ok::<_, anyhow::Error>(())
        })
        .unwrap();
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_http_metrics() {
    let server = test_util::TestHarness::default().start().await;

    let http_url = Url::parse(&format!("http://{}/api/sql", server.http_local_addr(),)).unwrap();

    // Handled query (successful)
    let json = r#"{ "query": "SHOW application_name;" }"#;
    let json: serde_json::Value = serde_json::from_str(json).unwrap();
    let response = reqwest::Client::new()
        .post(http_url.clone())
        .json(&json)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());

    // Handled query (error)
    let json = r#"{ "query": "invalid sql 123;" }"#;
    let json: serde_json::Value = serde_json::from_str(json).unwrap();
    let response = reqwest::Client::new()
        .post(http_url.clone())
        .json(&json)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = response.json().await.unwrap();
    let is_error = body
        .get("results")
        .unwrap()
        .get(0)
        .unwrap()
        .get("error")
        .is_some();
    assert!(is_error);

    // Invalid request
    let json = r#"{ "q": "invalid sql 123;" }"#;
    let json: serde_json::Value = serde_json::from_str(json).unwrap();
    let response = reqwest::Client::new()
        .post(http_url)
        .json(&json)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_client_error());

    let metrics = server.metrics_registry.gather();
    let http_metrics: Vec<_> = metrics
        .into_iter()
        .filter(|metric| metric.name().starts_with("mz_http"))
        .collect();

    // Make sure the duration metric exists.
    let duration_count = http_metrics
        .iter()
        .filter(|metric| metric.name() == "mz_http_request_duration_seconds")
        .count();
    assert_eq!(duration_count, 1);
    // Make sure the active count metric exists.
    let active_count = http_metrics
        .iter()
        .filter(|metric| metric.name() == "mz_http_requests_active")
        .count();
    assert_eq!(active_count, 1);

    // Make sure our metrics capture the one successful query and the one failure.
    let mut request_metrics: Vec<_> = http_metrics
        .into_iter()
        .filter(|metric| metric.name() == "mz_http_requests_total")
        .collect();
    assert_eq!(request_metrics.len(), 1);

    let request_metric = request_metrics.pop().unwrap();
    let success_metric = &request_metric.get_metric()[0];
    assert_eq!(success_metric.get_counter().get_value(), 2.0);
    assert_eq!(success_metric.get_label()[0].value(), "/api/sql");
    assert_eq!(success_metric.get_label()[2].value(), "200");

    let failure_metric = &request_metric.get_metric()[1];
    assert_eq!(failure_metric.get_counter().get_value(), 1.0);
    assert_eq!(failure_metric.get_label()[0].value(), "/api/sql");
    assert_eq!(failure_metric.get_label()[2].value(), "422");
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
#[cfg_attr(miri, ignore)] // too slow
async fn webhook_concurrent_actions() {
    let server = test_util::TestHarness::default().start().await;
    let client = server.connect().await.unwrap();

    #[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
    struct WebhookEvent {
        ts: u128,
        name: String,
        thread: usize,
    }

    // Create a webhook source.
    let src_name = "webhook_json";
    client
        .execute(
            "CREATE CLUSTER webhook_cluster_concurrent REPLICAS (r1 (SIZE 'scale=1,workers=1'));",
            &[],
        )
        .await
        .expect("failed to create cluster");
    client
        .execute(
            &format!("CREATE SOURCE {src_name} IN CLUSTER webhook_cluster_concurrent FROM WEBHOOK BODY FORMAT JSON"),
            &[],
        )
        .await
        .expect("failed to create source");

    fn now() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time went backwards")
            .as_nanos()
    }

    // Flag that lets us shutdown our threads.
    let keep_sending = Arc::new(AtomicBool::new(true));
    // Track how many requests were resolved before we dropped the collection.
    let num_requests_before_drop = Arc::new(AtomicUsize::new(0));

    // Spin up tasks that will contiously push data to the webhook.
    let keep_sending_ = Arc::clone(&keep_sending);
    let num_requests_before_drop_ = Arc::clone(&num_requests_before_drop);
    let addr = server.http_local_addr();

    let poster = mz_ore::task::spawn(|| "webhook_concurrent_actions-poster", async move {
        let mut i = 0;

        let http_client = reqwest::Client::new();
        let mut tasks = Vec::with_capacity(500);

        // Keep sending events until we're told to stop.
        while keep_sending_.load(std::sync::atomic::Ordering::Relaxed) {
            let webhook_url = format!(
                "http://{}/api/webhook/materialize/public/webhook_json",
                addr,
            );

            let http_client_ = http_client.clone();
            let num_requests_before_drop__ = Arc::clone(&num_requests_before_drop_);
            let handle =
                mz_ore::task::spawn(|| "webhook_concurrent_actions-appender", async move {
                    // Create an event.
                    let event = WebhookEvent {
                        ts: now(),
                        name: format!("event_{i}"),
                        thread: 1,
                    };

                    // Send all of our events to our webhook source.
                    let resp = http_client_
                        .post(&webhook_url)
                        .json(&event)
                        .send()
                        .await
                        .expect("failed to POST event");
                    num_requests_before_drop__.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    resp
                });
            tasks.push(handle);
            i += 1;

            // Wait before sending another request.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Wait on all of the tasks to finish, then return the results.
        futures::future::join_all(tasks).await
    });

    // Let the posting threads run for a bit.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // We should see at least this many successes.
    let num_requests_before_drop =
        num_requests_before_drop.load(std::sync::atomic::Ordering::Relaxed);
    // Drop the source
    client
        .execute(&format!("DROP SOURCE {src_name};"), &[])
        .await
        .expect("failed to drop source");

    // Keep sending for a bit.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    // Stop the threads.
    keep_sending.store(false, std::sync::atomic::Ordering::Relaxed);
    let results = poster.await;

    // Inspect the results.
    let mut results = results.into_iter().collect::<Vec<_>>().into_iter();

    for _ in 0..num_requests_before_drop {
        let response = results.next().expect("element");
        let status = response.status();

        // We expect the following response codes:
        //
        // 1. 200 - Successfully append data.
        // 2. 429 - Rate limited.
        // 3. 404 - Collection not found. Due to the artificial latency we add to appending, we
        //    could send a request, it gets queued, we drop the source, pull all requests from the
        //    queue, find our source is missing. This should cause a 404 Not Found, not a 500.
        //
        assert!(
            status.is_success()
                || status == StatusCode::TOO_MANY_REQUESTS
                || status == StatusCode::NOT_FOUND,
            "response: {response:?}"
        )
    }
    // We should have seen at least 100 requests before dropping the source.
    assert!(num_requests_before_drop > 100);

    // Make sure the source got dropped.
    //
    // Note: this doubles as a check to make sure nothing internally panicked.
    let sources: Vec<String> = client
        .query("SELECT name FROM mz_sources", &[])
        .await
        .expect("success")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert!(!sources.iter().any(|name| name == src_name));

    // Best effort cleanup.
    let _ = client
        .execute("DROP CLUSTER webhook_cluster_concurrent CASCADE", &[])
        .await;
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn webhook_concurrency_limit() {
    let concurrency_limit = 15;
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();

    // Note: we need enable_unstable_dependencies to use mz_sleep.
    server.enable_feature_flags(&[
        "unsafe_enable_unstable_dependencies",
        "unsafe_enable_unsafe_functions",
    ]);

    // Reduce the webhook concurrency limit;
    let mut mz_client = server
        .pg_config_internal()
        .user(&SYSTEM_USER.name)
        .connect(postgres::NoTls)
        .unwrap();
    mz_client
        .batch_execute(&format!(
            "ALTER SYSTEM SET webhook_concurrent_request_limit = {concurrency_limit}"
        ))
        .unwrap();

    let mut client = server.connect(postgres::NoTls).unwrap();

    // Create a webhook source.
    client
        .execute(
            "CREATE CLUSTER webhook_cluster REPLICAS (r1 (SIZE 'scale=1,workers=1'));",
            &[],
        )
        .expect("failed to create cluster");
    client
        .execute(
            "CREATE SOURCE webhook_text IN CLUSTER webhook_cluster FROM WEBHOOK \
             BODY FORMAT TEXT \
             CHECK ( WITH(BODY) body IS NOT NULL AND mz_unsafe.mz_sleep(5) IS NULL )",
            &[],
        )
        .expect("failed to create source");

    let http_client = reqwest::Client::new();
    let webhook_url = format!(
        "http://{}/api/webhook/materialize/public/webhook_text",
        server.http_local_addr().clone(),
    );
    let mut handles = Vec::with_capacity(concurrency_limit + 5);

    for _ in 0..concurrency_limit + 5 {
        let http_client_ = http_client.clone();
        let webhook_url_ = webhook_url.clone();

        let handle = server.runtime().spawn_named(
            || "webhook-concurrency-limit-test".to_string(),
            async move {
                let resp = http_client_
                    .post(webhook_url_)
                    .body("a")
                    .send()
                    .await
                    .expect("response to succeed");
                resp.status()
            },
        );
        handles.push(handle);
    }
    let results = server
        .runtime()
        .block_on(futures::future::join_all(handles));

    let successes = results
        .iter()
        .filter(|status_code| status_code.is_success())
        .count();
    // Note: if this status code changes, we need to update our docs.
    let rate_limited = results
        .iter()
        .filter(|status_code| **status_code == StatusCode::TOO_MANY_REQUESTS)
        .count();

    // We expect at least the number of requests we allow concurrently to succeed.
    assert!(successes >= concurrency_limit);
    // We send 5 more requests than our limit, but we assert only at least 3 get
    // rate limited to reduce test flakiness.
    assert!(rate_limited >= 3);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn webhook_too_large_request() {
    let metrics_registry = MetricsRegistry::new();
    let server = test_util::TestHarness::default()
        .with_metrics_registry(metrics_registry.clone())
        .start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();

    // Create a webhook source.
    client
        .execute(
            "CREATE CLUSTER webhook_cluster REPLICAS (r1 (SIZE 'scale=1,workers=1'));",
            &[],
        )
        .expect("failed to create cluster");
    client
        .execute(
            "CREATE SOURCE webhook_bytes IN CLUSTER webhook_cluster FROM WEBHOOK BODY FORMAT BYTES",
            &[],
        )
        .expect("failed to create source");

    let http_client = Client::new();
    let webhook_url = format!(
        "http://{}/api/webhook/materialize/public/webhook_bytes",
        server.http_local_addr(),
    );

    // Send an event with a body larger that is exactly our max size.
    let five_mib = usize::cast_from(bytesize::mib(5u64));
    let body = vec![42u8; five_mib];
    let resp = http_client
        .post(&webhook_url)
        .body(body)
        .send()
        .expect("failed to POST event");
    assert!(resp.status().is_success());

    // Send an event that is one larger than our max size.
    let body = vec![42u8; five_mib + 1];
    let resp = http_client
        .post(&webhook_url)
        .body(body)
        .send()
        .expect("failed to POST event");

    // Note: If this changes then we need to update our docs.
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);

    // Ensure we logged a prometheus metric that we responded with a 413.
    let metrics: Vec<_> = metrics_registry.gather().into_iter().collect();
    let payload_too_large = metrics
        .iter()
        .find(|metric_family| metric_family.name() == "mz_http_requests_total")
        .unwrap()
        .get_metric()
        .iter()
        .find(|metric| {
            metric
                .get_label()
                .iter()
                .find(|label_pair| label_pair.value() == "413")
                .is_some()
        })
        .unwrap()
        .get_counter()
        .get_value();
    assert_eq!(payload_too_large, 1.0);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_webhook_url_notice() {
    let server = test_util::TestHarness::default().start_blocking();
    let (tx, mut rx) = futures::channel::mpsc::unbounded();

    let mut client = server
        .pg_config()
        .notice_callback(move |notice| tx.unbounded_send(notice).expect("send notice"))
        .connect(postgres::NoTls)
        .unwrap();
    let http_client = Client::new();

    // Create a webhook source that includes headers.
    client
        .execute(
            "CREATE CLUSTER webhook_cluster REPLICAS (r1 (SIZE 'scale=1,workers=1'));",
            &[],
        )
        .expect("failed to create cluster");
    client
        .execute(
            "CREATE SOURCE \"webhook-source(with odd/name\" IN CLUSTER webhook_cluster FROM WEBHOOK BODY FORMAT TEXT INCLUDE HEADERS",
            &[],
        )
        .expect("failed to create source");

    let url_notice = rx
        .try_next()
        .expect("contains notice")
        .expect("contains message");
    // We should only get the one notice.
    assert_err!(rx.try_next());

    // Print the notice to stderr for future debug-ability.
    eprintln!("notice: {}", url_notice.message());
    let url_in_quotes = url_notice
        .message()
        .strip_prefix("URL to POST data is ")
        .expect("got wrong message");
    // Remove the single quotes.
    let https_url = url_in_quotes.replace('\'', "");
    // Our local test server does not have the certs for https.
    let http_url = https_url.replace("https", "http");

    // Send a request with duplicate headers.
    let resp = http_client
        .post(http_url)
        .body("request_foo")
        .send()
        .expect("failed to POST event");
    assert_eq!(resp.status().as_u16(), 200);

    // Wait for the source to process our request.
    std::thread::sleep(std::time::Duration::from_secs(3));

    let row = client
        .query_one("SELECT body FROM \"webhook-source(with odd/name\"", &[])
        .expect("success");
    let body: String = row.get("body");
    assert_eq!(body, "request_foo");
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
#[cfg_attr(miri, ignore)] // too slow
async fn webhook_concurrent_swap() {
    let server = test_util::TestHarness::default().start().await;
    let mut client = server.connect().await.unwrap();

    // Create our webhook sources.
    let webhook_cluster = "webhook_cluster_concurrent_swap";
    client
        .execute(
            &format!("CREATE CLUSTER {webhook_cluster} REPLICAS (r1 (SIZE 'scale=1,workers=1'));"),
            &[],
        )
        .await
        .expect("failed to create cluster");

    let src_foo = "webhook_foo";
    client
        .execute(
            &format!(
                "CREATE SOURCE {src_foo} IN CLUSTER {webhook_cluster} FROM WEBHOOK BODY FORMAT TEXT"
            ),
            &[],
        )
        .await
        .expect("failed to create source");
    let src_bar = "webhook_bar";
    client
        .execute(
            &format!(
                "CREATE SOURCE {src_bar} IN CLUSTER {webhook_cluster} FROM WEBHOOK BODY FORMAT TEXT"
            ),
            &[],
        )
        .await
        .expect("failed to create source");

    // Spin up tasks that will contiously push data to both webhooks.
    let addr = server.http_local_addr();
    let http_client = reqwest::Client::new();
    let keep_sending = Arc::new(AtomicBool::new(true));

    let create_poster = |name: &'static str,
                         client: reqwest::Client,
                         addr,
                         keep_sending: Arc<AtomicBool>,
                         count: Arc<AtomicUsize>| {
        let url = format!("http://{addr}/api/webhook/materialize/public/{name}");
        mz_ore::task::spawn(|| format!("POST-{name}"), async move {
            let mut tasks = Vec::new();

            while keep_sending.load(std::sync::atomic::Ordering::Relaxed) {
                let http_client = client.clone();
                let webhook_url = url.clone();
                let handle =
                    mz_ore::task::spawn(|| "webhook_concurrent_actions-appender", async move {
                        // Send all of our events to our webhook source.
                        http_client
                            .post(&webhook_url)
                            .body(name)
                            .send()
                            .await
                            .expect("failed to POST event");
                    });
                tasks.push(handle);
                count.fetch_add(1, Ordering::SeqCst);

                // Wait before sending another request.
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            // Wait for all of the inner sends to finish.
            futures::future::join_all(tasks).await;
        })
    };

    // Spawn tasks that will continuously POST events to our webhook sources.
    let foo_count = Arc::new(AtomicUsize::new(0));
    let foo_poster = create_poster(
        src_foo,
        http_client.clone(),
        addr.clone(),
        Arc::clone(&keep_sending),
        Arc::clone(&foo_count),
    );
    let bar_count = Arc::new(AtomicUsize::new(0));
    let bar_poster = create_poster(
        src_bar,
        http_client.clone(),
        addr.clone(),
        Arc::clone(&keep_sending),
        Arc::clone(&bar_count),
    );

    // Let the posting tasks run for a bit.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Swap the two sources.
    let txn = client
        .transaction()
        .await
        .expect("failed to start a transaction");

    txn.execute(&format!("ALTER SOURCE {src_foo} RENAME TO temp"), &[])
        .await
        .expect("failed to rename foo");
    txn.execute(&format!("ALTER SOURCE {src_bar} RENAME TO {src_foo}"), &[])
        .await
        .expect("failed to swap bar");
    txn.execute(&format!("ALTER SOURCE temp RENAME TO {src_bar}"), &[])
        .await
        .expect("failed to swap foo");

    txn.commit().await.expect("transaction failed");

    // Now that we've swapped the source, this is the maximum number of original events we should
    // see in each source.
    let max_foo = foo_count.load(Ordering::SeqCst);
    let max_bar = bar_count.load(Ordering::SeqCst);

    // Let the posting tasks run for a bit longer.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Stop the tasks.
    keep_sending.store(false, Ordering::SeqCst);
    let _ = foo_poster.await;
    let _ = bar_poster.await;

    let foo_row = client
        .query_one(
            &format!("SELECT COUNT(*) FROM {src_bar} WHERE body = '{src_foo}'"),
            &[],
        )
        .await
        .expect("failed to count foos");
    let foo_count: usize = foo_row.get::<_, i64>(0).try_into().expect("invalid count");

    let bar_row = client
        .query_one(
            &format!("SELECT COUNT(*) FROM {src_foo} WHERE body = '{src_bar}'"),
            &[],
        )
        .await
        .expect("failed to count foos");
    let bar_count: usize = bar_row.get::<_, i64>(0).try_into().expect("invalid count");

    assert!(foo_count <= max_foo);
    assert!(bar_count <= max_bar);

    let appender_count = server
        .metrics_registry
        .gather()
        .into_iter()
        .find(|m| m.name() == "mz_webhook_get_appender_count")
        .unwrap()
        .take_metric()[0]
        .get_counter()
        .get_value();

    // We should only get a webhook appender from the Coordinator 4 times, once for each source
    // when we start posting, and then once again for each source after they are renamed.
    assert_eq!(appender_count, 4.0);

    // Best effort cleanup.
    let _ = client
        .execute(&format!("DROP CLUSTER {webhook_cluster} CASCADE"), &[])
        .await;
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn copy_from() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();

    let mut system_client = server
        .pg_config_internal()
        .user(&SYSTEM_USER.name)
        .connect(postgres::NoTls)
        .unwrap();
    system_client
        .batch_execute("ALTER SYSTEM SET max_copy_from_size = 50")
        .unwrap();
    drop(system_client);

    client
        .execute("CREATE TABLE copy_from_test ( x text )", &[])
        .expect("success");

    let mut writer = client
        .copy_in("COPY copy_from_test FROM STDIN (FORMAT TEXT)")
        .expect("success");
    writer
        .write_all(b"hello\nworld\n")
        .expect("write all to succeed");
    writer.finish().expect("success");

    let rows = client
        .query("SELECT * FROM copy_from_test", &[])
        .expect("success");
    assert_eq!(rows.len(), 2);

    // This copy from is 53 bytes long, which is greater than our limit of 50.
    let mut writer = client
        .copy_in("COPY copy_from_test FROM STDIN (FORMAT TEXT)")
        .expect("success");
    writer
        .write_all(b"this\ncopy\nis\nlarger\nthan\nour\ngreatest\nsupported\nsize\n")
        .expect("write all to succeed");
    let result = writer.finish().unwrap_db_error();
    assert_eq!(result.code(), &SqlState::INSUFFICIENT_RESOURCES);

    let rows = client
        .query("SELECT * FROM copy_from_test", &[])
        .expect("success");
    assert_eq!(rows.len(), 2);
}

// Test that a cluster dropped mid transaction results in an error.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn concurrent_cluster_drop() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut txn_client = server.connect(postgres::NoTls).unwrap();
    let mut drop_client = server.connect(postgres::NoTls).unwrap();

    txn_client
        .execute(
            "CREATE CLUSTER c REPLICAS (r1 (SIZE 'scale=1,workers=1'));",
            &[],
        )
        .expect("failed to create cluster");
    txn_client
        .execute("CREATE TABLE t (a INT);", &[])
        .expect("failed to create cluster");

    txn_client
        .execute("SET CLUSTER TO c", &[])
        .expect("success");
    txn_client.execute("BEGIN", &[]).expect("success");
    let _ = txn_client.query("SELECT * FROM t", &[]).expect("success");

    drop_client.execute("DROP CLUSTER c", &[]).expect("success");

    let err = txn_client
        .execute("SELECT * FROM t", &[])
        .expect_err("error");

    assert_eq!(
        err.as_db_error().unwrap().message(),
        "the transaction's active cluster has been dropped"
    );
}

// Test connection ID properties.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_connection_id() {
    let harness = test_util::TestHarness::default();
    let envid = harness.environment_id.organization_id().as_u128();
    let server = harness.start().await;
    let client = server.connect().await.unwrap();

    let pid: i32 = client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    assert!(pid > 0);
    let envid_lower = u32::try_from(envid & 0xFFF).unwrap();
    let pid = u32::from_le_bytes(pid.to_le_bytes());
    let pid_envid = pid >> 19;
    assert_eq!(envid_lower, pid_envid);
}

// Test connection ID properties.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_github_25388() {
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start()
        .await;
    server
        .enable_feature_flags(&["unsafe_enable_unsafe_functions"])
        .await;

    // TODO(peek-seq) The second part of this test no longer works with the new peek sequencing,
    // because we no longer check the catalog after optimization whether the original dependencies
    // still exist. This might be fine, because nothing bad happens: timestamp determination already
    // puts a a read hold on the index, so the index doesn't actually gets dropped in the
    // Controller, and therefore the peek actually succeeds. In other words, the old peek
    // sequencing's dependency check was overly cautious. I'm planning to revisit this later, and
    // probably delete the second part of the test.
    server
        .disable_feature_flags(&["enable_frontend_peek_sequencing"])
        .await;

    let client1 = server.connect().await.unwrap();

    client1
        .batch_execute("CREATE TABLE t (a int)")
        .await
        .unwrap();

    // May be flakey/racey due to various sleeps.
    Retry::default()
        .retry_async(|_| async {
            client1
                .batch_execute("DROP INDEX IF EXISTS idx")
                .await
                .unwrap();
            client1
                .batch_execute("CREATE INDEX idx ON t(a)")
                .await
                .unwrap();

            let client2 = server.connect().await.unwrap();
            mz_ore::task::spawn(|| "test", async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                client2.batch_execute("DROP INDEX idx").await.unwrap();
            });

            match client1
                .query("SUBSCRIBE (SELECT *, mz_unsafe.mz_sleep(2) FROM t)", &[])
                .await
            {
                Ok(_) => Err("unexpected query success".to_string()),
                Err(err)
                    if err
                        .to_string_with_causes()
                        .contains("dependency was removed") =>
                {
                    Ok(())
                }
                Err(err) => Err(err.to_string_with_causes()),
            }
        })
        .await
        .unwrap();

    Retry::default()
        .retry_async(|_| async {
            client1
                .batch_execute("DROP INDEX IF EXISTS idx")
                .await
                .unwrap();
            client1
                .batch_execute("CREATE INDEX idx ON t(a)")
                .await
                .unwrap();

            let client2 = server.connect().await.unwrap();
            mz_ore::task::spawn(|| "test", async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                client2.batch_execute("DROP INDEX idx").await.unwrap();
            });

            match client1
                .query("SELECT *, mz_unsafe.mz_sleep(2) FROM t", &[])
                .await
            {
                Ok(_) => Err("unexpected query success".to_string()),
                Err(err)
                    if err
                        .to_string_with_causes()
                        .contains("dependency was removed") =>
                {
                    Ok(())
                }
                Err(err) => Err(err.to_string_with_causes()),
            }
        })
        .await
        .unwrap();
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_webhook_source_batch_interval() {
    let server = test_util::TestHarness::default().start().await;
    let client = server.connect().await.unwrap();

    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Clone)]
    struct WebhookEvent {
        name: &'static str,
    }

    // Create a webhook source.
    client
        .execute(
            "CREATE SOURCE webhook_batch_interval_test FROM WEBHOOK BODY FORMAT JSON",
            &[],
        )
        .await
        .expect("failed to create source");

    let http_client = reqwest::Client::new();
    let webhook_url = format!(
        "http://{}/api/webhook/materialize/public/webhook_batch_interval_test",
        server.http_local_addr()
    );

    // Send one event to our webhook source.
    let start = Instant::now();
    let event = WebhookEvent { name: "first" };
    let resp = http_client
        .post(&webhook_url)
        .json(&event)
        .send()
        .await
        .expect("failed to POST event");
    let first_duration = start.elapsed();
    assert!(resp.status().is_success());
    // All is normal, the request should be fast.
    assert!(first_duration < Duration::from_secs(4));

    // Change our batch interval to be very high.
    let sys_client = server
        .connect()
        .internal()
        .user(&SYSTEM_USER.name)
        .await
        .unwrap();
    sys_client
        .execute(
            "ALTER SYSTEM SET user_storage_managed_collections_batch_duration TO '10s'",
            &[],
        )
        .await
        .expect("failed to set batch duration");

    // Send a second event...
    let start = Instant::now();
    let event = WebhookEvent { name: "second" };
    let resp = http_client
        .post(&webhook_url)
        .json(&event)
        .send()
        .await
        .expect("failed to POST event");
    let second_duration = start.elapsed();
    assert!(resp.status().is_success());

    // ... and then quickly follow up with a third.
    let start = Instant::now();
    let event = WebhookEvent { name: "third" };
    let resp = http_client
        .post(&webhook_url)
        .json(&event)
        .send()
        .await
        .expect("failed to POST event");
    let third_duration = start.elapsed();
    assert!(resp.status().is_success());

    // The second event should finish quickly like the first!
    assert!(second_duration < Duration::from_secs(4));
    // But the third will now be stuck behind the batch interval so it will take longer!
    assert!(third_duration > Duration::from_secs(7));
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_startup_cluster_notice_with_http_options() {
    let server = test_util::TestHarness::default().start().await;

    let http_client = reqwest::Client::new();

    let http_url = Url::parse(&format!("http://{}/api/sql", server.http_local_addr(),)).unwrap();

    let query = serde_json::json!({
        "query": "SHOW cluster"
    });

    // Specify a non-existant cluster should not show a notice.
    let options = serde_json::json!({
        "cluster": "i_do_not_exist"
    });
    let options = options.to_string();
    let mut non_existant_cluster_url = http_url.clone();
    non_existant_cluster_url
        .query_pairs_mut()
        .append_pair("options", &options);
    let result: serde_json::Value = http_client
        .post(non_existant_cluster_url)
        .json(&query)
        .send()
        .await
        .expect("failed to send SQL")
        .json()
        .await
        .expect("invalid JSON");
    let result = result
        .get("results")
        .expect("success")
        .as_array()
        .expect("results should be an array")
        .get(0)
        .expect("should have 1 element");

    let query_result = result.get("rows").expect("rows should exist");
    insta::assert_json_snapshot!(query_result, @r###"
    [
      [
        "i_do_not_exist"
      ]
    ]
    "###);

    // We have a single notice because we ran a query, not because we connected.
    let notices = result.get("notices").expect("notices should exist");
    insta::assert_json_snapshot!(notices, @r###"
    [
      {
        "message": "session default cluster \"i_do_not_exist\" does not exist",
        "code": "MZ005",
        "severity": "notice",
        "hint": "Pick an extant cluster with SET CLUSTER = name. Run SHOW CLUSTERS to see available clusters."
      },
      {
        "message": "cluster \"i_do_not_exist\" does not exist",
        "code": "MZ007",
        "severity": "notice",
        "hint": "Create the cluster with CREATE CLUSTER or pick an extant cluster with SET CLUSTER = name. List available clusters with SHOW CLUSTERS."
      }
    ]
    "###);
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_startup_cluster_notice() {
    let server = test_util::TestHarness::default().start().await;

    let notices = Arc::new(Mutex::new(Vec::new()));
    let notices_ = Arc::clone(&notices);
    let client = server
        .connect()
        .notice_callback(move |notice| notices_.lock().expect("not poisoned").push(notice))
        .await
        .expect("success");
    let notices = { notices.lock().expect("not poisoned").clone() };
    assert!(notices.is_empty());
    drop(client);

    let sys_client = server
        .connect()
        .internal()
        .user(&SYSTEM_USER.name)
        .await
        .unwrap();
    sys_client
        .execute("DROP CLUSTER quickstart", &[])
        .await
        .expect("able to drop cluster");

    // Reconnecting without a system cluster should get us a notice.
    let notices = Arc::new(Mutex::new(Vec::new()));
    let notices_ = Arc::clone(&notices);
    let client = server
        .connect()
        .notice_callback(move |notice| notices_.lock().expect("not poisoned").push(notice))
        .await
        .expect("success");
    // Give the server a moment to flush all notices.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let notices = { notices.lock().expect("not poisoned").clone() };
    insta::assert_debug_snapshot!(notices, @r###"
    [
        DbError {
            severity: "NOTICE",
            parsed_severity: None,
            code: SqlState(
                Other(
                    "MZ005",
                ),
            ),
            message: "system default cluster \"quickstart\" does not exist",
            detail: None,
            hint: Some(
                "Set a default cluster for the current role with `ALTER ROLE <role> SET cluster TO <cluster>;`.",
            ),
            position: None,
            where_: None,
            schema: None,
            table: None,
            column: None,
            datatype: None,
            constraint: None,
            file: None,
            line: None,
            routine: None,
        },
    ]
    "###);

    client
        .execute("ALTER ROLE materialize SET CLUSTER to non_existant", &[])
        .await
        .expect("able to set cluster");
    drop(client);

    // If we have a role default cluster, but it doesn't exist, we should still get a notice.
    let notices = Arc::new(Mutex::new(Vec::new()));
    let notices_ = Arc::clone(&notices);
    let _client = server
        .connect()
        .notice_callback(move |notice| notices_.lock().expect("not poisoned").push(notice))
        .await
        .expect("success");
    // Give the server a moment to flush all notices.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let notices = { notices.lock().expect("not poisoned").clone() };
    insta::assert_debug_snapshot!(notices, @r###"
    [
        DbError {
            severity: "NOTICE",
            parsed_severity: None,
            code: SqlState(
                Other(
                    "MZ005",
                ),
            ),
            message: "role default cluster \"non_existant\" does not exist",
            detail: None,
            hint: Some(
                "Change the default cluster for the current role with `ALTER ROLE <role> SET cluster TO <cluster>;`.",
            ),
            position: None,
            where_: None,
            schema: None,
            table: None,
            column: None,
            datatype: None,
            constraint: None,
            file: None,
            line: None,
            routine: None,
        },
    ]
    "###);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_durable_oids() {
    let data_dir = tempfile::tempdir().unwrap();
    let harness = test_util::TestHarness::default().data_directory(data_dir.path());

    let table_oid: u32 = {
        let server = harness.clone().start_blocking();
        let mut client = server.connect(postgres::NoTls).unwrap();
        client
            .execute("CREATE TABLE t (a INT);", &[])
            .expect("failed to create table");
        client
            .query_one("SELECT oid FROM mz_tables WHERE name = 't'", &[])
            .expect("failed to select")
            .get(0)
    };

    {
        let server = harness.clone().start_blocking();
        let mut client = server.connect(postgres::NoTls).unwrap();

        let restarted_table_oid: u32 = client
            .query_one("SELECT oid FROM mz_tables WHERE name = 't'", &[])
            .expect("failed to select")
            .get(0);
        assert_eq!(table_oid, restarted_table_oid);

        client
            .execute("CREATE VIEW v AS SELECT 1;", &[])
            .expect("failed to create table");
        let view_oid: u32 = client
            .query_one("SELECT oid FROM mz_views WHERE name = 'v'", &[])
            .expect("failed to select")
            .get(0);
        assert_ne!(table_oid, view_oid);
    }
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_double_encoded_json() {
    let server = test_util::TestHarness::default().start().await;
    let client = server.connect().await.expect("success");

    client
        .execute("CREATE TABLE t1 (a jsonb)", &[])
        .await
        .unwrap();
    client
        .execute("GRANT ALL PRIVILEGES ON TABLE t1 TO PUBLIC", &[])
        .await
        .unwrap();

    client
        .execute(r#"INSERT INTO t1 VALUES ('{ "type": "foo" }')"#, &[])
        .await
        .unwrap();
    client
        .execute(
            r#"INSERT INTO t1 VALUES ('" { \"type\": \"foo\" } "')"#,
            &[],
        )
        .await
        .unwrap();

    let http_client = reqwest::Client::new();
    let http_url = Url::parse(&format!("http://{}/api/sql", server.http_local_addr(),)).unwrap();
    let query = serde_json::json!({
        "query": "SELECT a FROM t1 ORDER BY a"
    });

    let result: serde_json::Value = http_client
        .post(http_url)
        .json(&query)
        .send()
        .await
        .expect("failed to send SQL")
        .json()
        .await
        .expect("invalid JSON");
    let rows = result
        .as_object()
        .unwrap()
        .get("results")
        .unwrap()
        .as_array()
        .unwrap()
        .get(0)
        .unwrap()
        .as_object()
        .unwrap()
        .get("rows")
        .unwrap();

    insta::assert_debug_snapshot!(rows, @r###"
    Array [
        Array [
            String(" { \"type\": \"foo\" } "),
        ],
        Array [
            Object {
                "type": String("foo"),
            },
        ],
    ]
    "###);

    let ws_url = server.ws_addr();
    let (mut ws, _resp) = tungstenite::connect(ws_url).unwrap();
    let _ws_init = test_util::auth_with_ws(&mut ws, BTreeMap::default()).unwrap();

    let json = "{\"query\":\"SELECT a FROM t1 ORDER BY a;\"}";
    let json: serde_json::Value = serde_json::from_str(json).unwrap();
    ws.send(Message::Text(json.to_string().into())).unwrap();

    let mut read_msg = || -> WebSocketResponse {
        let msg = ws.read().unwrap();
        let msg = msg.into_text().expect("response should be text");
        serde_json::from_str(&msg).unwrap()
    };
    let _starting = read_msg();
    let _columns = read_msg();

    let row_1 = read_msg();
    insta::assert_debug_snapshot!(row_1, @r###"
    Row(
        [
            String(" { \"type\": \"foo\" } "),
        ],
    )
    "###);
    let row_2 = read_msg();
    insta::assert_debug_snapshot!(row_2, @r###"
    Row(
        [
            Object {
                "type": String("foo"),
            },
        ],
    )
    "###);
}

// Tests cert reloading of environmentd.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_cert_reloading() {
    let ca = Ca::new_root("test ca").unwrap();
    let (server_cert, server_key) = ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let metrics_registry = MetricsRegistry::new();

    let tenant_id = Uuid::new_v4();
    let email = "user@_.com".to_string();
    let password = Uuid::new_v4().to_string();
    let client_id = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let initial_api_tokens = vec![ApiToken {
        client_id: client_id.clone(),
        secret: secret.clone(),
        description: None,
        created_at: Utc::now(),
    }];
    let roles = Vec::new();
    let users = BTreeMap::from([(
        email.clone(),
        UserConfig {
            id: Uuid::new_v4(),
            email,
            password,
            tenant_id,
            initial_api_tokens,
            roles,
            auth_provider: None,
            verified: None,
            metadata: None,
        },
    )]);

    let issuer = "frontegg-mock".to_owned();
    let encoding_key =
        EncodingKey::from_rsa_pem(&ca.pkey.private_key_to_pem_pkcs8().unwrap()).unwrap();
    let decoding_key = DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap();

    const EXPIRES_IN_SECS: i64 = 50;
    let frontegg_server = FronteggMockServer::start(
        None,
        issuer,
        encoding_key,
        decoding_key,
        users,
        BTreeMap::new(),
        None,
        SYSTEM_TIME.clone(),
        EXPIRES_IN_SECS,
        // Add a bit of delay so we can test connection de-duplication.
        Some(Duration::from_millis(100)),
        None,
    )
    .await
    .unwrap();

    let frontegg_auth = FronteggAuthentication::new(
        FronteggConfig {
            admin_api_token_url: frontegg_server.auth_api_token_url(),
            decoding_key: DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap(),
            tenant_id: Some(tenant_id),
            now: SYSTEM_TIME.clone(),
            admin_role: "mzadmin".to_string(),
            refresh_drop_lru_size: DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
            refresh_drop_factor: DEFAULT_REFRESH_DROP_FACTOR,
        },
        mz_frontegg_auth::Client::default(),
        &metrics_registry,
    );
    let frontegg_user = "user@_.com";
    let frontegg_password = format!("mzp_{client_id}{secret}");

    let (mut reload_tx, reload_rx) = futures::channel::mpsc::channel(1);
    let reload_certs = Box::pin(reload_rx);

    let config = test_util::TestHarness::default()
        // Enable SSL on the main port. There should be a balancerd port with no SSL.
        .with_tls(server_cert.clone(), server_key.clone())
        .with_frontegg_auth(&frontegg_auth)
        .with_metrics_registry(metrics_registry);
    let envd_server = config.start_with_trigger(reload_certs).await;

    let body = r#"{"query": "select 12234"}"#;
    let ca_cert = reqwest::Certificate::from_pem(&ca.cert.to_pem().unwrap()).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(ca_cert)
        // No pool so that connections are never re-used which can use old ssl certs.
        .pool_max_idle_per_host(0)
        .tls_info(true)
        .build()
        .unwrap();

    let conn_str = Arc::new(format!(
        "user={frontegg_user} password={frontegg_password} host={} port={} sslmode=require",
        envd_server.sql_local_addr().ip(),
        envd_server.sql_local_addr().port()
    ));

    /// Asserts that the postgres connection provides the expected server-side certificate.
    async fn check_pgwire(conn_str: &str, ca_cert_path: &PathBuf, expected_cert: X509) {
        let tls = make_pg_tls(Box::new(move |b: &mut SslConnectorBuilder| {
            b.set_ca_file(ca_cert_path).unwrap();
            b.set_verify_callback(SslVerifyMode::all(), move |verify_success, x509store| {
                assert!(verify_success);
                for cert in x509store.chain().unwrap() {
                    // Expect exactly one cert to be the expected one.
                    if *cert == expected_cert {
                        return true;
                    }
                }
                false
            });
            Ok(())
        }));

        let (pg_client, conn) = tokio_postgres::connect(conn_str, tls.clone())
            .await
            .unwrap();
        task::spawn(|| "pg_client", async move {
            let _ = conn.await;
        });

        assert_eq!(
            pg_client
                .query_one("SELECT 2", &[])
                .await
                .unwrap()
                .get::<_, i32>(0),
            2
        );
    }

    // Various tests about reloading of certs.

    // Assert the current certificate is as expected.
    let https_url = format!(
        "https://{addr}/api/sql",
        addr = envd_server.http_local_addr(),
    );
    let resp = client
        .post(&https_url)
        .header("Content-Type", "application/json")
        .basic_auth(frontegg_user, Some(&frontegg_password))
        .body(body)
        .send()
        .await
        .unwrap();
    let tlsinfo = resp.extensions().get::<reqwest::tls::TlsInfo>().unwrap();
    let resp_x509 = X509::from_der(tlsinfo.peer_certificate().unwrap()).unwrap();
    let server_x509 = X509::from_pem(&std::fs::read(&server_cert).unwrap()).unwrap();
    assert_eq!(resp_x509, server_x509);
    assert_contains!(resp.text().await.unwrap(), "12234");
    check_pgwire(&conn_str, &ca.ca_cert_path(), server_x509.clone()).await;

    // Generate new certs. Install only the key, reload, and make sure the old cert is still in
    // use.
    let (next_cert, next_key) = ca
        .request_cert("next", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let next_x509 = X509::from_pem(&std::fs::read(&next_cert).unwrap()).unwrap();
    assert_ne!(next_x509, server_x509);
    std::fs::copy(next_key, &server_key).unwrap();
    let (tx, rx) = oneshot::channel();
    reload_tx.try_send(Some(tx)).unwrap();
    let res = rx.await.unwrap();
    assert_err!(res);

    // We should still be on the old cert because now the cert and key mismatch.
    let resp = client
        .post(&https_url)
        .header("Content-Type", "application/json")
        .basic_auth(frontegg_user, Some(&frontegg_password))
        .body(body)
        .send()
        .await
        .unwrap();
    let tlsinfo = resp.extensions().get::<reqwest::tls::TlsInfo>().unwrap();
    let resp_x509 = X509::from_der(tlsinfo.peer_certificate().unwrap()).unwrap();
    assert_eq!(resp_x509, server_x509);
    check_pgwire(&conn_str, &ca.ca_cert_path(), server_x509.clone()).await;

    // Now move the cert too. Reloading should succeed and the response should have the new
    // cert.
    std::fs::copy(next_cert, &server_cert).unwrap();
    let (tx, rx) = oneshot::channel();
    reload_tx.try_send(Some(tx)).unwrap();
    let res = rx.await.unwrap();
    assert_ok!(res);
    let resp = client
        .post(&https_url)
        .header("Content-Type", "application/json")
        .basic_auth(frontegg_user, Some(&frontegg_password))
        .body(body)
        .send()
        .await
        .unwrap();
    let tlsinfo = resp.extensions().get::<reqwest::tls::TlsInfo>().unwrap();
    let resp_x509 = X509::from_der(tlsinfo.peer_certificate().unwrap()).unwrap();
    assert_eq!(resp_x509, next_x509);
    check_pgwire(&conn_str, &ca.ca_cert_path(), next_x509.clone()).await;
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_builtin_connection_alterations_are_preserved_across_restarts() {
    let data_dir = tempfile::tempdir().unwrap();
    let harness = test_util::TestHarness::default()
        .data_directory(data_dir.path())
        .unsafe_mode();

    {
        let server = harness.clone().start_blocking();
        let mut client = server
            .pg_config_internal()
            .user(&ANALYTICS_USER.name)
            .connect(postgres::NoTls)
            .unwrap();

        let row = client
            .query_one(
                "SELECT create_sql
            FROM mz_catalog.mz_connections
            WHERE name = 'mz_analytics'",
                &[],
            )
            .expect("success");
        let sql: String = row.get("create_sql");
        assert_eq!(
            sql,
            "CREATE CONNECTION \"mz_internal\".\"mz_analytics\" TO AWS (ASSUME ROLE ARN = '')"
        );
    }

    {
        let server = harness.clone().start_blocking();
        let mut client = server
            .pg_config_internal()
            .user(&ANALYTICS_USER.name)
            .connect(postgres::NoTls)
            .unwrap();

        let row = client
            .query_one(
                "SELECT create_sql
            FROM mz_catalog.mz_connections
            WHERE name = 'mz_analytics'",
                &[],
            )
            .expect("success");
        let sql: String = row.get("create_sql");
        assert_eq!(
            sql,
            "CREATE CONNECTION \"mz_internal\".\"mz_analytics\" TO AWS (ASSUME ROLE ARN = '')"
        );

        client
            .execute(
                "ALTER CONNECTION mz_internal.mz_analytics SET (ASSUME ROLE ARN = 'foo')",
                &[],
            )
            .expect("success");

        let row = client
            .query_one(
                "SELECT create_sql
            FROM mz_catalog.mz_connections
            WHERE name = 'mz_analytics'",
                &[],
            )
            .expect("success");
        let sql: String = row.get("create_sql");
        assert_eq!(
            sql,
            "CREATE CONNECTION \"mz_internal\".\"mz_analytics\" TO AWS (ASSUME ROLE ARN = 'foo')"
        );
    }

    {
        let server = harness.clone().start_blocking();
        let mut client = server
            .pg_config_internal()
            .user(&ANALYTICS_USER.name)
            .connect(postgres::NoTls)
            .unwrap();

        let row = client
            .query_one(
                "SELECT create_sql
            FROM mz_catalog.mz_connections
            WHERE name = 'mz_analytics'",
                &[],
            )
            .expect("success");
        let sql: String = row.get("create_sql");
        assert_eq!(
            sql,
            "CREATE CONNECTION \"mz_internal\".\"mz_analytics\" TO AWS (ASSUME ROLE ARN = 'foo')"
        );
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_webhook_request_compression() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();

    // Create a webhook source.
    client
        .execute(
            "CREATE CLUSTER webhook_cluster REPLICAS (r1 (SIZE 'scale=1,workers=1'));",
            &[],
        )
        .expect("failed to create cluster");
    client
        .execute(
            "CREATE SOURCE webhook_text IN CLUSTER webhook_cluster FROM WEBHOOK BODY FORMAT TEXT",
            &[],
        )
        .expect("failed to create source");

    let http_client = Client::new();
    let webhook_url = format!(
        "http://{}/api/webhook/materialize/public/webhook_text",
        server.http_local_addr(),
    );

    let og_body = "hello world!";

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(og_body.as_bytes()).unwrap();
    let compressed_body = encoder.finish().unwrap();

    assert!(og_body.as_bytes().len() < compressed_body.len());

    let resp = http_client
        .post(&webhook_url)
        .header(CONTENT_ENCODING, "gzip")
        .body(compressed_body)
        .send()
        .expect("failed to POST event");
    assert!(resp.status().is_success(), "{resp:?}");

    // Wait for up to 10s for the webhook to get appended.
    let mut row = None;
    for _ in 0..10 {
        std::thread::sleep(std::time::Duration::from_secs(1));

        row = client
            .query_opt("SELECT body FROM webhook_text", &[])
            .unwrap();
        if row.is_some() {
            break;
        }
    }

    let rnd_body = row.as_ref().map(|r| r.get("body"));
    assert_eq!(rnd_body, Some(og_body));
}
