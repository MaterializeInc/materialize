// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for SQL functionality.
//!
//! Nearly all tests for SQL behavior should be sqllogictest or testdrive
//! scripts. The tests here are simply too complicated to be easily expressed
//! in testdrive, e.g., because they depend on the current time.

use std::collections::BTreeMap;
use std::io::Read;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use axum::response::{IntoResponse, Response};
use axum::{Json, Router, routing};
use chrono::{DateTime, Utc};
use http::StatusCode;
use mz_adapter::{TimestampContext, TimestampExplanation};
use mz_catalog::builtin::BUILTINS;
use mz_environmentd::test_util::{
    self, KAFKA_ADDRS, MzTimestamp, PostgresErrorExt, TestServerWithRuntime, get_explain_timestamp,
    get_explain_timestamp_determination, try_get_explain_timestamp,
};
use mz_ore::collections::CollectionExt;
use mz_ore::now::{EpochMillis, NOW_ZERO, NowFn};
use mz_ore::result::ResultExt;
use mz_ore::retry::Retry;
use mz_ore::task::{self, AbortOnDropHandle};
use mz_ore::{assert_contains, assert_err, assert_ok};
use mz_pgrepr::UInt4;
use mz_repr::Timestamp;
use mz_sql::catalog::BuiltinsConfig;
use mz_sql::session::user::{INTERNAL_USER_NAME_TO_DEFAULT_CLUSTER, SUPPORT_USER, SYSTEM_USER};
use mz_storage_types::sources::Timeline;
use postgres::Row;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka_sys::RDKafkaErrorCode;
use regex::Regex;
use serde_json::json;
use timely::order::PartialOrder;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_postgres::error::SqlState;
use tracing::{debug, info};

/// An HTTP server whose responses can be controlled from another thread.
struct MockHttpServer {
    _task: AbortOnDropHandle<()>,
    addr: SocketAddr,
    conn_rx: mpsc::UnboundedReceiver<oneshot::Sender<Response>>,
}

impl MockHttpServer {
    /// Constructs a new mock HTTP server.
    async fn new() -> MockHttpServer {
        let (conn_tx, conn_rx) = mpsc::unbounded_channel();
        let router = Router::new().route(
            "/*path",
            routing::get(|| async move {
                let (response_tx, response_rx) = oneshot::channel();
                conn_tx
                    .send(response_tx)
                    .expect("handle unexpectedly closed channel");
                response_rx
                    .await
                    .expect("response channel unexpectedly closed")
            }),
        );
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let listener = TcpListener::bind(addr).await.expect("can bind");
        let addr = listener.local_addr().unwrap();
        let server = axum::serve(listener, router.into_make_service());
        let task = task::spawn(|| "mock_http_server", async {
            server
                .await
                .unwrap_or_else(|e| panic!("mock http server failed: {}", e))
        });
        MockHttpServer {
            _task: task.abort_on_drop(),
            addr,
            conn_rx,
        }
    }

    /// Accepts a new connection.
    ///
    /// The future resolves once a new connection has arrived at the server and
    /// is awaiting a response. The provided oneshot channel should be used to
    /// deliver the response.
    async fn accept(&mut self) -> oneshot::Sender<Response> {
        self.conn_rx
            .recv()
            .await
            .expect("server unexpectedly closed channel")
    }
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
async fn test_no_block() {
    // We manually time out the test because it's better than relying on CI to time out, because
    // an actual failure (as opposed to a CI timeout) causes `services.log` to be uploaded.

    // Allow the use of banned rdkafka methods, because we are just in tests.
    #[allow(clippy::disallowed_methods)]
    let test_case = async move {
        println!("test_no_block: starting server");
        let server = test_util::TestHarness::default().start().await;
        server
            .enable_feature_flags(&["enable_connection_validation_syntax"])
            .await;

        println!("test_no_block: starting mock HTTP server");
        let mut schema_registry_server = MockHttpServer::new().await;

        println!("test_no_block: connecting to server");
        let client = server.connect().await.unwrap();

        let slow_task = task::spawn(|| "slow_client", async move {
            println!("test_no_block: in thread; executing create source");
            let result = client
                .batch_execute(&format!(
                    "CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (URL 'http://{}') WITH (VALIDATE = false);",
                    schema_registry_server.addr,
                ))
                .await;
            println!("test_no_block: in thread; create CSR conn done");
            let _ = result.unwrap();

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

            let result = client
                    .batch_execute(&format!(
                        "CREATE CONNECTION kafka_conn TO KAFKA (BROKER '{}', SECURITY PROTOCOL PLAINTEXT) WITH (VALIDATE = false)",
                        &*KAFKA_ADDRS,
                    ))
                    .await;
            println!("test_no_block: in thread; create Kafka conn done");
            let _ = result.unwrap();

            let result = client
                .batch_execute(
                    "CREATE SOURCE foo \
                        FROM KAFKA CONNECTION kafka_conn (TOPIC 'foo') \
                        FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn",
                )
                .await;
            println!("test_no_block: in thread; create source done");
            // Verify that the schema registry error was returned to the client, for
            // good measure.
            assert_contains!(result.unwrap_err().to_string(), "server error 503");
        });

        // Wait for Materialize to contact the schema registry, which
        // indicates the adapter is processing the CREATE SOURCE command. It
        // will be unable to complete the query until we respond.
        println!("test_no_block: accepting fake schema registry connection");
        let response_tx = schema_registry_server.accept().await;

        // Verify that the adapter can still process other requests from
        // other sessions.
        println!("test_no_block: connecting to server again");
        let client = server.connect().await.unwrap();
        println!("test_no_block: executing query");
        let answer: i32 = client.query_one("SELECT 1 + 1", &[]).await.unwrap().get(0);
        assert_eq!(answer, 2);

        // Return an error to the adapter, so that we can shutdown cleanly.
        println!("test_no_block: writing fake schema registry error");
        response_tx
            .send(StatusCode::SERVICE_UNAVAILABLE.into_response())
            .expect("server unexpectedly closed channel");

        println!("test_no_block: joining task");
        slow_task.await;
    };

    tokio::time::timeout(Duration::from_secs(120), test_case)
        .await
        .expect("Test timed out");
}

/// Test that dropping a connection while a source is undergoing purification
/// does not crash the server.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
async fn test_drop_connection_race() {
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start()
        .await;
    server
        .enable_feature_flags(&["enable_connection_validation_syntax"])
        .await;
    info!("test_drop_connection_race: server started");

    info!("test_drop_connection_race: starting mock HTTP server");
    let mut schema_registry_server = MockHttpServer::new().await;

    // Construct a source that depends on a schema registry connection.
    let client = server.connect().await.unwrap();
    client
        .batch_execute(&format!(
            "CREATE CONNECTION conn TO CONFLUENT SCHEMA REGISTRY (URL 'http://{}') WITH (VALIDATE = false)",
            schema_registry_server.addr,
        ))
        .await
        .unwrap();
    client
        .batch_execute(&format!(
            "CREATE CONNECTION kafka_conn TO KAFKA (BROKER '{}', SECURITY PROTOCOL PLAINTEXT) WITH (VALIDATE = false)",
            &*KAFKA_ADDRS,
        ))
        .await
        .unwrap();

    // Allow the use of banned rdkafka methods, because we are just in tests.
    #[allow(clippy::disallowed_methods)]
    let source_task = task::spawn(|| "source_client", async move {
        info!("test_drop_connection_race: in task; creating connection and source");
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

        let result = client
            .batch_execute(
                "CREATE SOURCE foo \
                     FROM KAFKA CONNECTION kafka_conn (TOPIC 'foo') \
                     FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION conn",
            )
            .await;
        info!(
            "test_drop_connection_race: in task; create source done: {:?}",
            result
        );
        result
    });

    // Wait for Materialize to contact the schema registry, which indicates
    // the adapter is processing the CREATE SOURCE command. It will be
    // unable to complete the query until we respond.
    info!("test_drop_connection_race: accepting fake schema registry connection");
    let response_tx = schema_registry_server.accept().await;

    // Drop the connection on which the source depends.
    info!("test_drop_connection_race: dropping connection");
    let client = server.connect().await.unwrap();
    client.batch_execute("DROP CONNECTION conn").await.unwrap();

    let schema = Json(json!({
        "id": 1_i64,
        "subject": "foo-value",
        "version": 1_i64,
        "schema": r#"{"type": "long"}"#,
    }));

    info!("test_drop_connection_race: sending fake schema registry response");
    response_tx
        .send(schema.clone().into_response())
        .expect("server unexpectedly closed channel");
    info!("test_drop_connection_race: sending fake schema registry response again");
    let response_tx = schema_registry_server.accept().await;
    response_tx
        .send(schema.into_response())
        .expect("server unexpectedly closed channel");

    info!("test_drop_connection_race: asserting response");
    let source_res = source_task.into_tokio_handle().await.unwrap();
    assert_contains!(
        source_res.unwrap_err().to_string(),
        "unknown catalog item 'conn'"
    );
}

#[mz_ore::test]
fn test_time() {
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();
    server.enable_feature_flags(&["unsafe_enable_unsafe_functions"]);
    let mut client = server.connect(postgres::NoTls).unwrap();

    // Confirm that `now()` and `current_timestamp()` both return a
    // DateTime<Utc>, but don't assert specific times.
    let row = client
        .query_one("SELECT now(), current_timestamp()", &[])
        .unwrap();
    let _ = row.get::<_, DateTime<Utc>>(0);
    let _ = row.get::<_, DateTime<Utc>>(1);

    // Confirm calls to now() return the same DateTime<Utc> both inside and
    // outside of subqueries.
    let row = client
        .query_one("SELECT now(), (SELECT now())", &[])
        .unwrap();
    assert_eq!(
        row.get::<_, DateTime<Utc>>(0),
        row.get::<_, DateTime<Utc>>(1)
    );

    // Ensure that EXPLAIN selects a timestamp for `now()` and
    // `current_timestamp()`, though we don't care what the timestamp is.
    let rows = client
        .query(
            "EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT now(), current_timestamp()",
            &[],
        )
        .unwrap();
    assert_eq!(1, rows.len());

    // Test that `mz_sleep` causes a delay of at least the appropriate time.
    let start = Instant::now();
    client
        .batch_execute("SELECT mz_unsafe.mz_sleep(0.3)")
        .unwrap();
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(300),
        "start.elapsed() = {:?}",
        elapsed
    );
}

#[mz_ore::test]
fn test_subscribe_consolidation() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client_writes = server.connect(postgres::NoTls).unwrap();
    let mut client_reads = server.connect(postgres::NoTls).unwrap();

    client_writes
        .batch_execute("CREATE TABLE t (data text)")
        .unwrap();
    client_reads
        .batch_execute(
            "BEGIN;
         DECLARE c CURSOR FOR SUBSCRIBE t;",
        )
        .unwrap();

    let data = format!("line {}", 42);
    client_writes
        .execute(
            "INSERT INTO t VALUES ($1), ($2), ($3)",
            &[&data, &data, &data],
        )
        .unwrap();
    let row = client_reads.query_one("FETCH ALL c", &[]).unwrap();

    assert_eq!(row.get::<_, i64>("mz_diff"), 3);
    assert_eq!(row.get::<_, String>("data"), data);
}

#[mz_ore::test]
fn test_subscribe_negative_diffs() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client_writes = server.connect(postgres::NoTls).unwrap();
    let mut client_reads = server.connect(postgres::NoTls).unwrap();

    client_writes
        .batch_execute("CREATE TABLE t (data text)")
        .unwrap();
    client_writes.batch_execute(
        "CREATE MATERIALIZED VIEW counts AS SELECT data AS key, COUNT(data) AS count FROM t GROUP BY data",
    ).unwrap();
    client_reads
        .batch_execute(
            "BEGIN;
         DECLARE c CURSOR FOR SUBSCRIBE counts;",
        )
        .unwrap();

    let data = format!("line {}", 42);
    client_writes
        .execute("INSERT INTO t VALUES ($1)", &[&data])
        .unwrap();
    let row = client_reads.query_one("FETCH ALL c", &[]).unwrap();

    assert_eq!(row.get::<_, i64>("mz_diff"), 1);
    assert_eq!(row.get::<_, String>("key"), data);
    assert_eq!(row.get::<_, i64>("count"), 1);

    // send another row with the same key, this will retract the previous
    // count and emit an updated count

    let data = format!("line {}", 42);
    client_writes
        .execute("INSERT INTO t VALUES ($1)", &[&data])
        .unwrap();

    let rows = client_reads.query("FETCH ALL c", &[]).unwrap();
    let mut rows = rows.iter();

    let row = rows.next().expect("missing result");
    assert_eq!(row.get::<_, i64>("mz_diff"), -1);
    assert_eq!(row.get::<_, String>("key"), data);
    assert_eq!(row.get::<_, i64>("count"), 1);

    let row = rows.next().expect("missing result");
    assert_eq!(row.get::<_, i64>("mz_diff"), 1);
    assert_eq!(row.get::<_, String>("key"), data);
    assert_eq!(row.get::<_, i64>("count"), 2);
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
async fn test_empty_subscribe_notice() {
    let server = test_util::TestHarness::default()
        .with_now(NOW_ZERO.clone())
        .start()
        .await;

    let (tx, mut rx) = futures::channel::mpsc::unbounded();
    let client = server
        .connect()
        .notice_callback(move |notice| tx.unbounded_send(notice).unwrap())
        .await
        .unwrap();

    client
        .batch_execute("CREATE TABLE t (a int)")
        .await
        .unwrap();
    let now = test_util::get_explain_timestamp("t", &client).await;
    client
        .batch_execute(&format!("SUBSCRIBE TO t AS OF {now} UP TO {now}"))
        .await
        .unwrap();

    Retry::default()
        .max_duration(Duration::from_secs(10))
        .retry(|_| {
            let Some(e) = rx.try_next().unwrap() else {
                return Err("No notice received".to_string());
            };
            if e.message().contains("guaranteed to be empty") {
                Ok(())
            } else {
                Err(format!("wrong notice received: {e:?}"))
            }
        })
        .unwrap();
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
async fn test_empty_subscribe_error() {
    let server = test_util::TestHarness::default()
        .with_now(NOW_ZERO.clone())
        .start()
        .await;
    let client = server.connect().await.unwrap();

    client
        .batch_execute("CREATE TABLE t (a int)")
        .await
        .unwrap();
    let now = test_util::get_explain_timestamp("t", &client).await;
    let e = client
        .batch_execute(&format!("SUBSCRIBE TO t AS OF {now} UP TO {}", now - 1))
        .await
        .expect_err("expected DB error");
    let e = e.as_db_error().expect("expected DB error");
    assert!(e.code().code() == "22000")
}

#[mz_ore::test]
fn test_subscribe_basic() {
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();
    let mut client_writes = server.connect(postgres::NoTls).unwrap();
    let mut client_reads = server.connect(postgres::NoTls).unwrap();

    server.enable_feature_flags(&["enable_index_options", "enable_logical_compaction_window"]);

    let mut sys_client = server.connect_internal(postgres::NoTls).unwrap();

    sys_client
        .execute(
            "ALTER SYSTEM SET constraint_based_timestamp_selection = 'verify'",
            &[],
        )
        .unwrap();

    // Create a table with disabled compaction.
    client_writes
        .batch_execute("CREATE TABLE t (data text) WITH (RETAIN HISTORY FOR '1000 hours')")
        .unwrap();
    client_writes.batch_execute("SELECT * FROM t").unwrap();
    client_reads
        .batch_execute(
            "BEGIN;
         DECLARE c CURSOR FOR SUBSCRIBE t;",
        )
        .unwrap();
    // Locks the timestamp of the SUBSCRIBE to before any of the following INSERTs, which is required
    // for mz_timestamp column to be accurate
    let _ = client_reads.query_one("FETCH 0 c", &[]);

    let mut events = vec![];

    for i in 1..=3 {
        let data = format!("line {}", i);
        client_writes
            .execute("INSERT INTO t VALUES ($1)", &[&data])
            .unwrap();
        let row = client_reads.query_one("FETCH ALL c", &[]).unwrap();
        assert_eq!(row.get::<_, i64>("mz_diff"), 1);
        assert_eq!(row.get::<_, String>("data"), data);
        events.push((row.get::<_, MzTimestamp>("mz_timestamp").0, data));

        if i > 1 {
            // write timestamps should all increase
            assert!(events[i - 1].0 > events[i - 2].0);
        }
    }

    // Now subscribe without a snapshot as of each timestamp, verifying that when we do
    // so we only see events that occur as of or later than that timestamp.
    for (ts, _) in &events {
        client_reads
            .batch_execute(&*format!(
                "COMMIT; BEGIN;
            DECLARE c CURSOR FOR SUBSCRIBE t WITH (SNAPSHOT = false) AS OF {}",
                ts - 1
            ))
            .unwrap();

        // Skip by the things we won't be able to see.
        for (_, expected) in events.iter().skip_while(|(inner_ts, _)| inner_ts < ts) {
            let actual = client_reads.query_one("FETCH c", &[]).unwrap();
            assert_eq!(actual.get::<_, String>("data"), *expected);
        }
    }

    // Now subscribe with a snapshot as of each timestamp. We should see a batch of
    // updates all at the subscribed timestamp, and then updates afterward.
    for (ts, _) in &events {
        client_reads
            .batch_execute(&*format!(
                "COMMIT; BEGIN;
            DECLARE c CURSOR FOR SUBSCRIBE t AS OF {}",
                ts - 1
            ))
            .unwrap();

        for &(mut expected_ts, ref expected_data) in events.iter() {
            if expected_ts < ts - 1 {
                // If the thing we initially got was before the timestamp, it should have gotten
                // fast-forwarded up to the timestamp.
                expected_ts = ts - 1;
            }

            let actual = client_reads.query_one("FETCH c", &[]).unwrap();
            assert_eq!(actual.get::<_, String>("data"), *expected_data);
            assert_eq!(actual.get::<_, MzTimestamp>("mz_timestamp").0, expected_ts);
        }
    }

    // Check that a subscription with `UP TO` is cut off at the proper point
    let begin = events[0].0;
    for (ts, _) in &events {
        client_reads
            .batch_execute(&*format!(
                "COMMIT; BEGIN; DECLARE c CURSOR FOR SUBSCRIBE t AS OF {begin} UP TO {ts}"
            ))
            .unwrap();
        for (expected_ts, expected_data) in events.iter() {
            if expected_ts >= ts {
                // We hit the `UP TO`; we should be done.
                break;
            }

            let actual = client_reads.query_one("FETCH c", &[]).unwrap();
            assert_eq!(actual.get::<_, String>("data"), *expected_data);
            assert_eq!(actual.get::<_, MzTimestamp>("mz_timestamp").0, *expected_ts);
        }
        // Make sure no rows from after or equal to the `UP TO` show up.
        // This also checks that the subscribe has been dropped, since
        // we don't specify a timeout.
        let should_be_empty = client_reads.query("FETCH c", &[]).unwrap();
        assert!(should_be_empty.is_empty())
    }

    // Aggressively compact the data in the index, then subscribe an unmaterialized
    // view derived from the index. This previously selected an invalid
    // `AS OF` timestamp (database-issues#1666).
    client_writes
        .batch_execute("ALTER TABLE t SET (RETAIN HISTORY = FOR '1s')")
        .unwrap();
    client_writes
        .batch_execute("CREATE VIEW v AS SELECT * FROM t")
        .unwrap();
    client_reads
        .batch_execute(
            "COMMIT; BEGIN;
         DECLARE c CURSOR FOR SUBSCRIBE v;",
        )
        .unwrap();
    let rows = client_reads.query("FETCH ALL c", &[]).unwrap();
    assert_eq!(rows.len(), 3);
    for i in 0..3 {
        assert_eq!(rows[i].get::<_, i64>("mz_diff"), 1);
        assert_eq!(rows[i].get::<_, String>("data"), format!("line {}", i + 1));
    }

    // Wait until compaction kicks in and we get an error on trying to read from the cursor.
    let err = loop {
        client_reads
            .batch_execute("COMMIT; BEGIN; DECLARE c CURSOR FOR SUBSCRIBE v AS OF 1")
            .unwrap();

        if let Err(err) = client_reads.query("FETCH ALL c", &[]) {
            break err;
        }
    };

    assert!(
        err.unwrap_db_error()
            .message()
            .starts_with("Timestamp (1) is not valid for all inputs")
    );
}

/// Test the done messages by sending inserting a single row and waiting to
/// observe it. Since SUBSCRIBE always sends a progressed message at the end of its
/// batches and we won't yet insert a second row, we know that if we've seen a
/// data row we will also see one progressed message.
#[mz_ore::test]
fn test_subscribe_progress() {
    let server = test_util::TestHarness::default().start_blocking();

    for has_initial_data in [false, true] {
        for has_index in [false, true] {
            for has_snapshot in [false, true] {
                let mut client_writes = server.connect(postgres::NoTls).unwrap();
                let mut client_reads = server.connect(postgres::NoTls).unwrap();

                info!(
                    msg = "Running test",
                    has_initial_data, has_index, has_snapshot
                );

                client_writes
                    .batch_execute("CREATE TABLE t1 (data text)")
                    .unwrap();
                if has_index {
                    client_writes
                        .batch_execute("CREATE INDEX i1 on t1(data)")
                        .unwrap();
                }
                if has_initial_data {
                    client_writes
                        .batch_execute("INSERT INTO t1 VALUES ('snapdata')")
                        .unwrap();
                }
                client_reads
                    .batch_execute(&format!(
                        "COMMIT; BEGIN;
                DECLARE c1 CURSOR FOR SUBSCRIBE t1 WITH (PROGRESS, SNAPSHOT = {})",
                        has_snapshot
                    ))
                    .unwrap();

                // Asserts that the next data message is `data`. Ignores any progress
                // messages that occur first.
                //
                // Returns the timestamp at which that data arrived.
                fn await_data(
                    client: &mut postgres::Client,
                    last_seen_ts: &mut u64,
                    data: &str,
                ) -> u64 {
                    // We have to try several times. It might be that the FETCH gets a
                    // progress statements rather than data. We retry until we get the batch
                    // that has the data.
                    debug!("awaiting data: {data}");
                    loop {
                        let rows = client.query("FETCH 1 c1", &[]).unwrap();
                        debug!(row = ?rows.first());
                        let data_row = match rows.first() {
                            Some(row) if row.try_get::<_, String>("data").is_ok() => row,
                            _ => continue, // retry
                        };
                        assert_eq!(data_row.get::<_, bool>("mz_progressed"), false);
                        assert_eq!(data_row.get::<_, i64>("mz_diff"), 1);
                        assert_eq!(data_row.get::<_, String>("data"), data);
                        let ts = data_row.get::<_, MzTimestamp>("mz_timestamp").0;
                        assert!(ts >= *last_seen_ts);
                        *last_seen_ts = ts;
                        return ts;
                    }
                }

                // Asserts that the next message has a timestamp of at least `ts` and is a progress message.
                //
                // Returns the timestamp of the progress message.
                fn await_progress(
                    client: &mut postgres::Client,
                    last_seen_ts: &mut u64,
                    ts: u64,
                ) -> u64 {
                    debug!("awaiting progress");
                    let row = client.query_one("FETCH 1 c1", &[]).unwrap();
                    debug!(?row);
                    assert_eq!(row.get::<_, bool>("mz_progressed"), true);
                    assert_eq!(row.get::<_, Option<i64>>("mz_diff"), None);
                    assert_eq!(row.get::<_, Option<String>>("data"), None);
                    let progress_ts = row.get::<_, MzTimestamp>("mz_timestamp").0;
                    assert!(progress_ts >= ts);
                    assert!(progress_ts >= *last_seen_ts);
                    *last_seen_ts = progress_ts;
                    progress_ts
                }

                let mut last_seen_ts = 0;

                // The first message should always be a progress message indicating the
                // snapshot time, followed by the data in the snapshot at that time,
                // followed by an progress message indicating the end of the snapshot.
                let snapshot_ts = await_progress(&mut client_reads, &mut last_seen_ts, 0);
                if has_initial_data && has_snapshot {
                    await_data(&mut client_reads, &mut last_seen_ts, "snapdata");
                }
                await_progress(&mut client_reads, &mut last_seen_ts, snapshot_ts + 1);

                for i in 1..=3 {
                    let data = format!("line {}", i);
                    client_writes
                        .execute("INSERT INTO t1 VALUES ($1)", &[&data])
                        .unwrap();
                    let data_ts = await_data(&mut client_reads, &mut last_seen_ts, &data);
                    await_progress(&mut client_reads, &mut last_seen_ts, data_ts + 1);
                }

                client_writes
                    .batch_execute("DROP TABLE t1 CASCADE")
                    .unwrap();
            }
        }
    }
}

// Verifies that subscribing to non-nullable columns with progress information
// turns them into nullable columns. See database-issues#1946.
#[mz_ore::test]
fn test_subscribe_progress_non_nullable_columns() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client_writes = server.connect(postgres::NoTls).unwrap();
    let mut client_reads = server.connect(postgres::NoTls).unwrap();

    client_writes
        .batch_execute("CREATE TABLE t2 (data text NOT NULL)")
        .unwrap();
    client_writes
        .batch_execute("INSERT INTO t2 VALUES ('data')")
        .unwrap();
    client_reads
        .batch_execute(
            "COMMIT; BEGIN;
            DECLARE c2 CURSOR FOR SUBSCRIBE t2 WITH (PROGRESS);",
        )
        .unwrap();

    #[derive(PartialEq)]
    enum State {
        WaitingForData,
        WaitingForProgress,
        Done,
    }

    let mut state = State::WaitingForData;

    // Wait for one progress statement after seeing the data update.
    // Alternatively, we could just check any progress statement to make sure
    // that columns are in fact `Options`

    while state != State::Done {
        let row = client_reads.query_one("FETCH 1 c2", &[]).unwrap();

        if !row.get::<_, bool>("mz_progressed") {
            assert_eq!(row.get::<_, i64>("mz_diff"), 1);
            assert_eq!(row.get::<_, String>("data"), "data");
            state = State::WaitingForProgress;
        } else if state == State::WaitingForProgress {
            assert_eq!(row.get::<_, bool>("mz_progressed"), true);
            assert_eq!(row.get::<_, Option<i64>>("mz_diff"), None);
            assert_eq!(row.get::<_, Option<String>>("data"), None);
            state = State::Done;
        }
    }
}

/// Verifies that we get continuous progress messages, regardless of if we
/// receive data or not.
#[mz_ore::test]
fn test_subcribe_continuous_progress() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client_writes = server.connect(postgres::NoTls).unwrap();
    let mut client_reads = server.connect(postgres::NoTls).unwrap();

    client_writes
        .batch_execute("CREATE TABLE t1 (data text)")
        .unwrap();
    client_reads
        .batch_execute(
            "COMMIT; BEGIN;
         DECLARE c1 CURSOR FOR SUBSCRIBE t1 WITH (PROGRESS);",
        )
        .unwrap();

    let mut last_ts = MzTimestamp(u64::MIN);
    let mut verify_rows = move |rows: Vec<Row>| -> (usize, usize) {
        let mut num_data_rows = 0;
        let mut num_progress_rows = 0;

        for row in rows {
            let diff = row.get::<_, Option<i64>>("mz_diff");
            match diff {
                Some(diff) => {
                    num_data_rows += 1;

                    assert_eq!(diff, 1);
                    assert_eq!(row.get::<_, bool>("mz_progressed"), false);
                    let data = row.get::<_, Option<String>>("data");
                    assert!(data.is_some());
                }
                None => {
                    num_progress_rows += 1;

                    assert_eq!(row.get::<_, bool>("mz_progressed"), true);
                    assert_eq!(row.get::<_, Option<String>>("data"), None);
                }
            }

            let ts: MzTimestamp = row.get("mz_timestamp");
            assert!(last_ts <= ts);
            last_ts = ts;
        }

        (num_data_rows, num_progress_rows)
    };

    // make sure we see progress without any data ever being produced
    loop {
        let rows = client_reads.query("FETCH ALL c1", &[]).unwrap();
        let (num_data_rows, num_progress_rows) = verify_rows(rows);
        assert_eq!(num_data_rows, 0);
        if num_progress_rows > 0 {
            break;
        }
    }

    client_writes
        .execute("INSERT INTO t1 VALUES ($1)", &[&"hello".to_owned()])
        .unwrap();

    // fetch away the data message, plus maybe some progress messages
    let mut num_data_rows = 0;
    let mut num_progress_rows = 0;

    while num_data_rows == 0 || num_progress_rows == 0 {
        let rows = client_reads.query("FETCH ALL c1", &[]).unwrap();
        let (current_num_data_rows, current_num_progress_rows) = verify_rows(rows);
        num_data_rows += current_num_data_rows;
        num_progress_rows += current_num_progress_rows;
    }

    // Try and read some progress messages. The normal update interval is
    // 1s, so only wait for two updates. Otherwise this would run for too long.
    for _i in 1..=2 {
        let rows = client_reads.query("FETCH ALL c1", &[]).unwrap();

        let (num_data_rows, num_progress_rows) = verify_rows(rows);
        assert_eq!(num_data_rows, 0);
        assert!(num_progress_rows > 0);
    }
}

#[mz_ore::test]
fn test_subscribe_fetch_timeout() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();

    client.batch_execute("CREATE TABLE t (i INT8)").unwrap();
    client
        .batch_execute("INSERT INTO t VALUES (1), (2), (3);")
        .unwrap();
    client
        .batch_execute(
            "BEGIN;
         DECLARE c CURSOR FOR SUBSCRIBE t;",
        )
        .unwrap();

    let expected: Vec<i64> = vec![1, 2, 3];
    let mut expected_iter = expected.iter();
    let mut next = expected_iter.next();

    // Test 0s timeouts.
    while let Some(expect) = next {
        let rows = client.query("FETCH c WITH (TIMEOUT = '0s')", &[]).unwrap();
        // It is fine for there to be no rows ready yet. Immediately try again because
        // they should be ready soon.
        if rows.len() != 1 {
            continue;
        }
        assert_eq!(rows[0].get::<_, i64>(2), *expect);

        next = expected_iter.next();
    }

    // Test a 1s timeout and make sure we waited for at least that long.
    let before = Instant::now();
    let rows = client.query("FETCH c WITH (TIMEOUT = '1s')", &[]).unwrap();
    let duration = before.elapsed();
    assert_eq!(rows.len(), 0);
    // Make sure we waited at least 1s but also not too long.
    assert!(duration >= Duration::from_secs(1));
    assert!(duration < Duration::from_secs(10));

    // Make a new cursor. Try to fetch more rows from it than exist. Verify that
    // we got all the rows we expect and also waited for at least the timeout
    // duration. Cursor may take a moment to be ready, so do it in a loop.
    client
        .batch_execute(
            "COMMIT; BEGIN;
        DECLARE c CURSOR FOR SUBSCRIBE t",
        )
        .unwrap();
    loop {
        let before = Instant::now();
        let rows = client
            .query("FETCH 4 c WITH (TIMEOUT = '1s')", &[])
            .unwrap();
        let duration = before.elapsed();
        if rows.len() != 0 {
            assert_eq!(rows.len(), expected.len());
            assert!(duration >= Duration::from_secs(1));
            assert!(duration < Duration::from_secs(10));
            for i in 0..expected.len() {
                assert_eq!(rows[i].get::<_, i64>(2), expected[i])
            }
            break;
        }
    }

    // Another fetch should return nothing.
    let rows = client.query("FETCH c WITH (TIMEOUT = '0s')", &[]).unwrap();
    assert_eq!(rows.len(), 0);

    // Make a third cursor. Fetch should return immediately if there are enough
    // rows, even with a really long timeout.
    //
    // Regression test for database-issues#1949
    client
        .batch_execute(
            "COMMIT; BEGIN;
        DECLARE c CURSOR FOR SUBSCRIBE t",
        )
        .unwrap();
    let before = Instant::now();
    // NB: This timeout is chosen such that the test will timeout if the bad
    // behavior occurs.
    let rows = client
        .query("FETCH 3 c WITH (TIMEOUT = '1h')", &[])
        .unwrap();
    let duration = before.elapsed();
    assert_eq!(rows.len(), expected.len());
    assert!(duration < Duration::from_secs(10));
    for i in 0..expected.len() {
        assert_eq!(rows[i].get::<_, i64>(2), expected[i])
    }
}

#[mz_ore::test]
fn test_subscribe_fetch_wait() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();

    client.batch_execute("CREATE TABLE t (i INT8)").unwrap();
    client
        .batch_execute("INSERT INTO t VALUES (1), (2), (3)")
        .unwrap();
    client
        .batch_execute(
            "BEGIN;
         DECLARE c CURSOR FOR SUBSCRIBE t;",
        )
        .unwrap();

    let expected: Vec<i64> = vec![1, 2, 3];
    let mut expected_iter = expected.iter();
    let mut next = expected_iter.next();

    while let Some(expect) = next {
        // FETCH with no timeout will wait for at least 1 result.
        let rows = client.query("FETCH c", &[]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, i64>(2), *expect);
        next = expected_iter.next();
    }

    // Try again with FETCH ALL. ALL only guarantees that all available rows will
    // be returned, but it's up to the system to decide what is available. This
    // means that we could still get only one row per request, and we won't know
    // how many rows will come back otherwise.
    client
        .batch_execute(
            "COMMIT; BEGIN;
        DECLARE c CURSOR FOR SUBSCRIBE t;",
        )
        .unwrap();
    let mut expected_iter = expected.iter().peekable();
    while expected_iter.peek().is_some() {
        let rows = client.query("FETCH ALL c", &[]).unwrap();
        assert!(rows.len() > 0);
        for row in rows {
            let next = expected_iter.next().unwrap();
            assert_eq!(*next, row.get::<_, i64>(2));
        }
    }

    // Verify that the wait only happens for SUBSCRIBE. A SELECT with 0 rows should not
    // block.
    client.batch_execute("COMMIT").unwrap();
    client.batch_execute("CREATE TABLE empty ()").unwrap();
    client
        .batch_execute(
            "BEGIN;
         DECLARE c CURSOR FOR SELECT * FROM empty;",
        )
        .unwrap();
    let rows = client.query("FETCH c", &[]).unwrap();
    assert_eq!(rows.len(), 0);
}

#[mz_ore::test]
fn test_subscribe_empty_upper_frontier() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();

    client
        .batch_execute("CREATE MATERIALIZED VIEW foo AS VALUES (1), (2), (3);")
        .unwrap();

    let subscribe = client
        .query("SUBSCRIBE foo WITH (SNAPSHOT = false)", &[])
        .unwrap();
    assert_eq!(0, subscribe.len());

    let subscribe = client.query("SUBSCRIBE foo WITH (SNAPSHOT)", &[]).unwrap();
    assert_eq!(3, subscribe.len());
}

// Tests that a client that launches a non-terminating SUBSCRIBE and disconnects
// does not keep the server alive forever.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn test_subscribe_shutdown() {
    let server = test_util::TestHarness::default().start().await;

    let (client, conn_task) = server.connect().with_handle().await.unwrap();

    // Create a table with no data that we can SUBSCRIBE. This is the simplest
    // way to cause a SUBSCRIBE to never terminate.
    client.batch_execute("CREATE TABLE t ()").await.unwrap();

    // Launch the ill-fated subscribe.
    client
        .copy_out("COPY (SUBSCRIBE t) TO STDOUT")
        .await
        .unwrap();

    // Un-gracefully abort the connection.
    // We need to await `conn_task` to actually deliver the `abort`. We don't
    // care about the result though (it's probably `JoinError` with `is_cancelled` being true).
    conn_task.abort_and_wait().await;

    // Dropping the server will initiate a graceful shutdown. We previously had
    // a bug where the server would fail to notice that the client running
    // `SUBSCRIBE v` had disconnected, and would hang forever waiting for data
    // to be written to `path`, which in this test never comes. So if this
    // function exits, things are working correctly.
}

#[mz_ore::test]
fn test_subscribe_table_rw_timestamps() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client_interactive = server.connect(postgres::NoTls).unwrap();
    let mut client_subscribe = server.connect(postgres::NoTls).unwrap();

    client_interactive
        .batch_execute("CREATE TABLE t1 (data text)")
        .unwrap();

    client_subscribe
        .batch_execute(
            "COMMIT; BEGIN;
         DECLARE c1 CURSOR FOR SUBSCRIBE t1 WITH (PROGRESS);",
        )
        .unwrap();

    client_interactive.execute("BEGIN", &[]).unwrap();
    client_interactive
        .execute("INSERT INTO t1 VALUES ($1)", &[&"first".to_owned()])
        .unwrap();
    client_interactive
        .execute("INSERT INTO t1 VALUES ($1)", &[&"first".to_owned()])
        .unwrap();
    client_interactive.execute("COMMIT", &[]).unwrap();
    let _ = client_interactive.query("SELECT * FROM T1", &[]).unwrap();
    client_interactive.execute("BEGIN", &[]).unwrap();
    client_interactive
        .execute("INSERT INTO t1 VALUES ($1)", &[&"second".to_owned()])
        .unwrap();
    client_interactive
        .execute("INSERT INTO t1 VALUES ($1)", &[&"second".to_owned()])
        .unwrap();
    client_interactive.execute("COMMIT", &[]).unwrap();
    let _ = client_interactive.query("SELECT * FROM T1", &[]).unwrap();

    let mut first_write_ts = None;
    let mut second_write_ts = None;
    let mut seen_first = false;
    let mut seen_second = false;

    // TODO(aljoscha): We can wrap this with timeout logic, if we want/need to?
    // We need to do multiple FETCH ALL calls. ALL only guarantees that all
    // available rows will be returned, but it's up to the system to decide what
    // is available. This means that we could still get only one row per
    // request, and we won't know how many rows will come back otherwise.
    while !seen_second {
        let rows = client_subscribe.query("FETCH ALL c1", &[]).unwrap();
        for row in rows.iter() {
            let mz_timestamp = row.get::<_, MzTimestamp>("mz_timestamp");
            let mz_progressed = row.get::<_, Option<bool>>("mz_progressed").unwrap();
            let mz_diff = row.get::<_, Option<i64>>("mz_diff");
            let data = row.get::<_, Option<String>>("data");

            if !mz_progressed {
                // Actual data
                let mz_diff = mz_diff.unwrap();
                let data = data.unwrap();
                if !seen_first {
                    assert_eq!(data, "first");
                    seen_first = true;
                    first_write_ts = Some(mz_timestamp);
                } else {
                    assert_eq!(data, "second");
                    seen_second = true;
                    second_write_ts = Some(mz_timestamp);
                }
                assert_eq!(mz_diff, 2);
            }
        }
    }

    client_subscribe.batch_execute("COMMIT;").unwrap();

    assert!(seen_first);
    assert!(seen_second);

    let first_write_ts = first_write_ts.unwrap();
    let second_write_ts = second_write_ts.unwrap();
    assert!(first_write_ts <= second_write_ts);
}

// Tests that temporary views created by one connection cannot be viewed
// by another connection.
#[mz_ore::test]
fn test_temporary_views() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client_a = server.connect(postgres::NoTls).unwrap();
    let mut client_b = server.connect(postgres::NoTls).unwrap();
    client_a
        .batch_execute("CREATE VIEW v AS VALUES (1, 'foo'), (2, 'bar'), (3, 'foo'), (1, 'bar')")
        .unwrap();
    client_a
        .batch_execute("CREATE TEMPORARY VIEW temp_v AS SELECT * FROM v")
        .unwrap();

    let query_v = "SELECT count(*) FROM v;";
    let query_temp_v = "SELECT count(*) FROM temp_v;";

    // Ensure that client_a can query v and temp_v.
    let count: i64 = client_b.query_one(query_v, &[]).unwrap().get("count");
    assert_eq!(4, count);
    let count: i64 = client_a.query_one(query_temp_v, &[]).unwrap().get("count");
    assert_eq!(4, count);

    // Ensure that client_b can query v, but not temp_v.
    let count: i64 = client_b.query_one(query_v, &[]).unwrap().get("count");
    assert_eq!(4, count);

    let err = client_b.query_one(query_temp_v, &[]).unwrap_db_error();
    assert_eq!(err.message(), "unknown catalog item \'temp_v\'");
}

// Test EXPLAIN TIMESTAMP with tables.
#[mz_ore::test]
fn test_explain_timestamp_table() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();
    let timestamp_re = Regex::new(r"\s*(\d+) \(\d+-\d\d-\d\d \d\d:\d\d:\d\d.\d\d\d\)").unwrap();

    client.batch_execute("CREATE TABLE t1 (i1 int)").unwrap();

    let expect = "                query timestamp:<TIMESTAMP>
          oracle read timestamp:<TIMESTAMP>
largest not in advance of upper:<TIMESTAMP>
                          upper:[<TIMESTAMP>]
                          since:[<TIMESTAMP>]
        can respond immediately: true
                       timeline: Some(EpochMilliseconds)
              session wall time:<TIMESTAMP>

source materialize.public.t1 (u2, storage):
                  read frontier:[<TIMESTAMP>]
                 write frontier:[<TIMESTAMP>]

binding constraints:
lower:
  (StorageInput([User(2)])): [<TIMESTAMP>]
  (IsolationLevel(StrictSerializable)): [<TIMESTAMP>]\n";

    let row = client
        .query_one("EXPLAIN TIMESTAMP FOR SELECT * FROM t1;", &[])
        .unwrap();
    let explain: String = row.get(0);
    let explain = timestamp_re.replace_all(&explain, "<TIMESTAMP>");
    assert_eq!(explain, expect, "{explain}\n\n{expect}");
}

// Test `EXPLAIN TIMESTAMP AS JSON`
#[mz_ore::test]
fn test_explain_timestamp_json() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();
    client.batch_execute("CREATE TABLE t1 (i1 int)").unwrap();

    let row = client
        .query_one("EXPLAIN TIMESTAMP AS JSON FOR SELECT * FROM t1;", &[])
        .unwrap();
    let explain: String = row.get(0);
    // Just check that we can round-trip to the original type
    let _explain: TimestampExplanation<Timestamp> = serde_json::from_str(&explain).unwrap();
}

// Verify that `EXPLAIN TIMESTAMP ...` within acts like a peek within a transaction.
// That is, ensure the following:
// 1. Consistently returns its transaction timestamp as the "query timestamp"
// 2. Acquires read holds for all objects within the same time domain
// 3. Errors during a write-only transaction
// 4. Errors when an object outside the chosen time domain is referenced
//
// GitHub issue # 18950
#[mz_ore::test]
fn test_transactional_explain_timestamps() {
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();

    let mut client_writes = server.connect(postgres::NoTls).unwrap();
    let mut client_reads = server.connect(postgres::NoTls).unwrap();

    client_writes
        .batch_execute("CREATE TABLE t1 (i1 int)")
        .unwrap();

    client_writes.batch_execute("CREATE SCHEMA s").unwrap();
    client_writes
        .batch_execute("CREATE TABLE s.t2 (i2 int)")
        .unwrap();

    // Verify execution during a write-only txn fails
    client_writes.batch_execute("BEGIN").unwrap();
    client_writes
        .batch_execute("INSERT INTO t1 VALUES (1)")
        .unwrap();
    let error = client_writes
        .query_one("EXPLAIN TIMESTAMP FOR SELECT * FROM t1;", &[])
        .unwrap_err();

    assert!(format!("{}", error).contains("transaction in write-only mode"));

    client_writes.batch_execute("ROLLBACK").unwrap();

    // Verify the transaction timestamp is returned for each select and
    // the read frontier does not advance.
    client_reads.batch_execute("BEGIN").unwrap();
    let mut query_timestamp = None;

    for _ in 1..5 {
        let row = client_reads
            .query_one("EXPLAIN TIMESTAMP AS JSON FOR SELECT * FROM t1;", &[])
            .unwrap();

        let explain: String = row.get(0);
        let explain: TimestampExplanation<Timestamp> = serde_json::from_str(&explain).unwrap();
        let explain_timestamp = explain.determination.timestamp_context.timestamp().unwrap();

        if let Some(timestamp) = query_timestamp {
            assert_eq!(timestamp, *explain_timestamp);
        } else {
            query_timestamp = Some(*explain_timestamp);
        }

        let explain_t1_read_frontier = explain
            .sources
            .first()
            .unwrap()
            .read_frontier
            .first()
            .unwrap();

        // Ensure `t1`'s read frontier remains <= the query timestamp
        assert!(*explain_t1_read_frontier <= query_timestamp.unwrap());

        // Inserting tends to cause sources to compact, so this should ideally
        // strengthen the assertion above that `t1`'s read frontier should
        // not advance during the txn
        client_writes
            .batch_execute("INSERT INTO t1 VALUES (1)")
            .unwrap();
    }

    // Errors when an object outside the chosen time domain is referenced
    let error = client_reads
        .query_one("EXPLAIN TIMESTAMP FOR SELECT * FROM s.t2;", &[])
        .unwrap_err();

    assert!(
        format!("{}", error)
            .contains("Transactions can only reference objects in the same timedomain")
    );

    client_reads.batch_execute("COMMIT").unwrap();

    // Since mz_now() is a custom function, the postgres client will look it up in the catalog on
    // first use. If the first use happens to be in a transaction, then we can get unexpected time
    // domain errors. This is an annoying hack to load the information in the postgres client before
    // we start any transactions.
    client_reads.query_one("SELECT mz_now();", &[]).unwrap();

    // Ensure behavior is same when starting txn with `SELECT`
    client_reads.batch_execute("BEGIN").unwrap();

    client_reads.query("SELECT * FROM t1;", &[]).unwrap();

    let row = client_reads
        .query_one("SELECT mz_now()::text;", &[])
        .unwrap();

    let mz_now_ts_raw: String = row.get(0);
    let mz_now_timestamp = Timestamp::new(mz_now_ts_raw.parse().unwrap());

    let row = client_reads
        .query_one("EXPLAIN TIMESTAMP AS JSON FOR SELECT * FROM t1;", &[])
        .unwrap();

    let explain: String = row.get(0);
    let explain: TimestampExplanation<Timestamp> = serde_json::from_str(&explain).unwrap();
    let explain_timestamp = explain.determination.timestamp_context.timestamp().unwrap();

    assert_eq!(*explain_timestamp, mz_now_timestamp);
}

// Test that the since for `mz_cluster_replica_utilization` is held back by at least
// 30 days, which is required for the frontend observability work.
//
// Feel free to modify this test if that product requirement changes,
// but please at least keep _something_ that tests that custom compaction windows are working.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(coverage, ignore)] // https://github.com/MaterializeInc/database-issues/issues/5600
#[ignore] // TODO: Reenable when https://github.com/MaterializeInc/database-issues/issues/8491 is fixed
async fn test_utilization_hold() {
    const THIRTY_DAYS_MS: u64 = 30 * 24 * 60 * 60 * 1000;
    // `mz_catalog_server` tests indexes, `quickstart` tests tables.
    // The bool determines whether we are testing indexes.
    const CLUSTERS_TO_TRY: &[&str] = &["mz_catalog_server", "quickstart"];
    const QUERIES_TO_TRY: &[&str] = &["SELECT * FROM mz_internal.mz_cluster_replica_statuses"];

    let now_millis = 619388520000;
    let past_millis = now_millis - THIRTY_DAYS_MS;
    let past_since = Timestamp::from(past_millis);

    let now = Arc::new(Mutex::new(past_millis));
    let now_fn = {
        let timestamp = Arc::clone(&now);
        NowFn::from(move || *timestamp.lock().unwrap())
    };
    let data_dir = tempfile::tempdir().unwrap();

    // Create the server with the past time, to make sure the table is created.
    let server = test_util::TestHarness::default()
        .with_now(now_fn)
        .data_directory(data_dir.path())
        .start()
        .await;

    // Fast-forward time to make sure the table is still readable at the old time.
    *now.lock().unwrap() = now_millis;

    let client = server.connect().await.unwrap();

    for q in QUERIES_TO_TRY {
        let explain_q = &format!("EXPLAIN TIMESTAMP AS JSON FOR {q}");
        for cluster in CLUSTERS_TO_TRY {
            client
                .execute(&format!("SET cluster={cluster}"), &[])
                .await
                .unwrap();

            let row = client.query_one(explain_q, &[]).await.unwrap();
            let explain: String = row.get(0);
            let explain: TimestampExplanation<Timestamp> = serde_json::from_str(&explain).unwrap();

            // Assert that we actually used the indexes/tables, as required
            for s in &explain.sources {
                assert!(s.name.ends_with("compute)"));
            }

            // If we're not in EpochMilliseconds, the timestamp math below is invalid, so assert that here.
            assert!(matches!(
                explain.determination.timestamp_context,
                TimestampContext::TimelineTimestamp {
                    timeline: Timeline::EpochMilliseconds,
                    ..
                }
            ));
            let since = explain
                .determination
                .since
                .into_option()
                .expect("The since must be finite");
            // Plus 10 to allow for a small number of write timestamps to be
            // consumed.
            assert!(since.less_equal(&past_since.checked_add(10).unwrap()));
            // Assert we aren't lagging by more than 30 days + 1 second.
            // If we ever make the since granularity configurable, this line will
            // need to be changed.
            assert!(past_since.less_equal(&since.checked_add(1000).unwrap()));
        }
    }

    // Check that we can turn off retention
    let sys_client = server
        .connect()
        .internal()
        .user(&SYSTEM_USER.name)
        .await
        .unwrap();

    sys_client
        .execute("ALTER SYSTEM SET metrics_retention='1s'", &[])
        .await
        .unwrap();

    // This is a bit clunky, but setting the metrics_retention will not
    // propagate to all tables instantly. We have to retry the queries below,
    // but only up to a point.
    let deadline = Instant::now() + Duration::from_secs(60);

    'retry: loop {
        let now = Instant::now();
        if now > deadline {
            panic!("sinces did not advance as expected in time");
        }

        let mut all_sinces_correct = true;

        for q in QUERIES_TO_TRY {
            let explain_q = &format!("EXPLAIN TIMESTAMP AS JSON FOR {q}");
            for cluster in CLUSTERS_TO_TRY {
                client
                    .execute(&format!("SET cluster={cluster}"), &[])
                    .await
                    .unwrap();
                let row = client.query_one(explain_q, &[]).await.unwrap();
                let explain: String = row.get(0);
                let explain: TimestampExplanation<Timestamp> =
                    serde_json::from_str(&explain).unwrap();
                let since = explain
                    .determination
                    .since
                    .into_option()
                    .expect("The since must be finite");
                // Check that since is not more than 2 seconds in the past
                let since_is_correct = Timestamp::new(now_millis)
                    .less_equal(&since.step_forward_by(&Timestamp::new(2000)));

                if !since_is_correct {
                    info!(now_millis = ?now_millis, since = ?since, query = ?q,
                          "since has not yet advanced to expected time, retrying");
                }

                all_sinces_correct &= since_is_correct;
            }
        }

        if all_sinces_correct {
            break 'retry;
        }
    }
}

// Test that a query that causes a compute instance to panic will resolve
// the panic and allow the compute instance to restart (instead of crash loop
// forever) when a client is terminated (disconnects from the server) instead
// of cancelled (sends a pgwire cancel request on a new connection).
#[ignore] // TODO(necaris): Re-enable this as soon as possible
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn test_github_12546() {
    let server = test_util::TestHarness::default()
        .with_propagate_crashes(false)
        .unsafe_mode()
        .start()
        .await;
    server
        .enable_feature_flags(&["unsafe_enable_unsafe_functions"])
        .await;

    let (client, conn_task) = server.connect().with_handle().await.unwrap();

    client
        .batch_execute("CREATE TABLE test(a text);")
        .await
        .unwrap();
    client
        .batch_execute("INSERT INTO test VALUES ('a');")
        .await
        .unwrap();

    let query = client.query("SELECT mz_unsafe.mz_panic(a) FROM test", &[]);
    let timeout = tokio::time::timeout(Duration::from_secs(2), query);
    // We expect the timeout to trigger because the query should be crashing the
    // compute instance.
    assert_eq!(
        timeout.await.unwrap_err().to_string(),
        "deadline has elapsed"
    );

    // Aborting the connection should cause its pending queries to be cancelled,
    // allowing the compute instances to stop crashing while trying to execute
    // them.
    //
    // We need to await `conn_task` to actually deliver the `abort`.
    conn_task.abort_and_wait().await;

    // Make a new connection to verify the compute instance can now start.
    let client = server.connect().await.unwrap();
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM test", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1,
    );
}

/// Regression test for database-issues#3721.
#[mz_ore::test]
fn test_github_3721() {
    let server = test_util::TestHarness::default().start_blocking();

    // Verify sinks (SUBSCRIBE) are correctly handled for a dropped cluster.
    {
        let mut client1 = server.connect(postgres::NoTls).unwrap();
        let mut client2 = server.connect(postgres::NoTls).unwrap();

        client1
            .batch_execute("CREATE CLUSTER foo REPLICAS (r1 (size 'scale=1,workers=1'))")
            .unwrap();
        client1.batch_execute("CREATE TABLE t1(f1 int)").unwrap();
        client2.batch_execute("SET CLUSTER = foo").unwrap();
        client2
            .batch_execute(
                "BEGIN; DECLARE c CURSOR FOR SUBSCRIBE (SELECT count(*) FROM t1); FETCH 1 c",
            )
            .unwrap();
        client1.batch_execute("DROP CLUSTER foo CASCADE").unwrap();
        client2
            .batch_execute("ROLLBACK; SET CLUSTER = default")
            .unwrap();
        assert_eq!(
            client2
                .query_one("SELECT count(*) FROM t1", &[])
                .unwrap()
                .get::<_, i64>(0),
            0
        );
    }

    // Verify read holds (transactions) are correctly handled for a dropped cluster.
    {
        let mut client1 = server.connect(postgres::NoTls).unwrap();
        let mut client2 = server.connect(postgres::NoTls).unwrap();

        client1
            .batch_execute("CREATE CLUSTER foo REPLICAS (r1 (size 'scale=1,workers=1'))")
            .unwrap();
        client2.batch_execute("SET CLUSTER = foo").unwrap();
        client2.batch_execute("BEGIN; SELECT * FROM t1").unwrap();
        client1.batch_execute("DROP CLUSTER foo CASCADE").unwrap();
        client2
            .batch_execute("COMMIT; SET CLUSTER = default")
            .unwrap();
        assert_eq!(
            client2
                .query_one("SELECT count(*) FROM t1", &[])
                .unwrap()
                .get::<_, i64>(0),
            0
        );
    }
}

#[mz_ore::test]
// Tests github issue database-issues#3761
fn test_subscribe_outlive_cluster() {
    let server = test_util::TestHarness::default().start_blocking();

    // Verify sinks (SUBSCRIBE) are correctly handled for a dropped cluster, when a new cluster is created.
    let mut client1 = server.connect(postgres::NoTls).unwrap();
    let mut client2 = server.connect(postgres::NoTls).unwrap();
    let client2_cancel = client2.cancel_token();

    client1
        .batch_execute("CREATE CLUSTER foo REPLICAS (r1 (size 'scale=1,workers=1'))")
        .unwrap();
    client1.batch_execute("CREATE TABLE t1(f1 int)").unwrap();
    client2.batch_execute("SET CLUSTER = foo").unwrap();
    client2
        .batch_execute("BEGIN; DECLARE c CURSOR FOR SUBSCRIBE (SELECT count(*) FROM t1); FETCH 1 c")
        .unwrap();
    client1.batch_execute("DROP CLUSTER foo CASCADE").unwrap();
    client1
        .batch_execute("CREATE CLUSTER newcluster REPLICAS (r1 (size 'scale=1,workers=1'))")
        .unwrap();
    client2_cancel.cancel_query(postgres::NoTls).unwrap();
    client2
        .batch_execute("ROLLBACK; SET CLUSTER = default")
        .unwrap();
    assert_eq!(
        client2
            .query_one("SELECT count(*) FROM t1", &[])
            .unwrap()
            .get::<_, i64>(0),
        0
    );
}

#[mz_ore::test]
fn test_read_then_write_serializability() {
    let server = test_util::TestHarness::default().start_blocking();

    // Create table with initial value
    {
        let mut client = server.connect(postgres::NoTls).unwrap();
        client.batch_execute("CREATE TABLE t(f bigint)").unwrap();
        client.batch_execute("INSERT INTO t VALUES (1)").unwrap();
    }

    let num_threads = 3;
    let num_loops = 3;

    // Start threads to run `INSERT INTO t SELECT * FROM t`. Each statement should double the
    // number of rows in the table if they're serializable.
    let mut clients = Vec::new();
    for _ in 0..num_threads {
        clients.push(server.connect(postgres::NoTls).unwrap());
    }

    let handles: Vec<_> = clients
        .into_iter()
        .map(|mut client| {
            std::thread::spawn(move || {
                for _ in 0..num_loops {
                    client
                        .batch_execute("INSERT INTO t SELECT * FROM t")
                        .unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }

    {
        let mut client = server.connect(postgres::NoTls).unwrap();
        let count = client
            .query_one("SELECT count(*) FROM t", &[])
            .unwrap()
            .get::<_, i64>(0);
        assert_eq!(
            u128::try_from(count).unwrap(),
            2u128.pow(num_loops * num_threads)
        );
    }
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
async fn test_timestamp_recovery() {
    let now = Arc::new(Mutex::new(1));
    let now_fn = {
        let timestamp = Arc::clone(&now);
        NowFn::from(move || *timestamp.lock().unwrap())
    };
    let data_dir = tempfile::tempdir().unwrap();
    let harness = test_util::TestHarness::default()
        .with_now(now_fn)
        .data_directory(data_dir.path());

    // Start a server and get the current global timestamp
    let global_timestamp = {
        let server = harness.clone().start().await;
        let client = server.connect().await.unwrap();

        client
            .batch_execute("CREATE TABLE t1 (i1 INT)")
            .await
            .unwrap();
        test_util::get_explain_timestamp("t1", &client).await
    };

    // Rollback the current time and ensure that a value larger than the old global timestamp is
    // recovered
    {
        *now.lock().expect("lock poisoned") = 0;
        let server = harness.clone().start().await;
        let client = server.connect().await.unwrap();
        let recovered_timestamp = test_util::get_explain_timestamp("t1", &client).await;
        assert!(recovered_timestamp > global_timestamp);
    }
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn test_linearizability() {
    // TODO(jkosh44) This doesn't actually test linearizability across sessions which would be nice.
    test_session_linearizability("strict serializable").await;
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn test_strong_session_serializability() {
    test_session_linearizability("strong session serializable").await;
}

async fn test_session_linearizability(isolation_level: &str) {
    // Set the timestamp to zero for deterministic initial timestamps.
    let now = Arc::new(Mutex::new(0));
    let now_fn = {
        let now = Arc::clone(&now);
        NowFn::from(move || *now.lock().unwrap())
    };
    let server = test_util::TestHarness::default()
        .with_now(now_fn)
        .unsafe_mode()
        .start()
        .await;
    server
        .enable_feature_flags(&["enable_session_timelines"])
        .await;
    let mz_client = server.connect().await.unwrap();

    let pg_table_name = "v_lin";
    let pg_source_name = "source_lin";
    let (pg_client, cleanup_fn) = test_util::create_postgres_source_with_table(
        &server,
        &mz_client,
        pg_table_name,
        "(a INT)",
        pg_source_name,
    )
    .await;
    // Insert value into postgres table.
    let _ = pg_client
        .execute(&format!("INSERT INTO {pg_table_name} VALUES (42);"), &[])
        .await
        .unwrap();

    test_util::wait_for_pg_table_population(&mz_client, pg_table_name, 1).await;

    // The user table's write frontier will be close to zero because we use a
    // deterministic now function in this test. It may be slightly higher than
    // zero because bootstrapping and background tasks push the global timestamp
    // forward.
    //
    // The source's write frontier will be close to the system time because it
    // uses the system clock to close timestamps.
    //
    // Therefore queries that only involve the Postgres table (from the source)
    // will normally happen at a higher timestamp than queries that involve the
    // Materialze user table. However, we prevent this when in strict
    // serializable mode.

    mz_client
        .batch_execute("SET transaction_isolation = serializable")
        .await
        .unwrap();

    // Wait until the Coordinator has learned about at least one upper
    // advancement in the source, which results in picking a higher timestamp
    // for queries.
    let mut source_ts;
    loop {
        source_ts = test_util::get_explain_timestamp(pg_table_name, &mz_client).await;
        if source_ts > 0 {
            break;
        }
    }

    // Create user table in Materialize.
    mz_client
        .batch_execute("DROP TABLE IF EXISTS t;")
        .await
        .unwrap();
    mz_client
        .batch_execute("CREATE TABLE t (a INT);")
        .await
        .unwrap();
    let join_ts =
        test_util::get_explain_timestamp(&format!("{pg_table_name}, t"), &mz_client).await;

    // In serializable transaction isolation, read timestamps can go backwards.
    assert!(join_ts < source_ts, "{join_ts} < {source_ts}");

    mz_client
        .batch_execute(&format!("SET transaction_isolation = '{isolation_level}'"))
        .await
        .unwrap();

    let source_ts = test_util::get_explain_timestamp(pg_table_name, &mz_client).await;
    let join_ts =
        test_util::get_explain_timestamp(&format!("{pg_table_name}, t"), &mz_client).await;

    // Since the query on the join was done after the query on the view, it should have a higher or
    // equal timestamp in strict serializable mode.
    assert!(join_ts >= source_ts);

    mz_client
        .batch_execute("SET transaction_isolation = serializable")
        .await
        .unwrap();

    let source_ts = test_util::get_explain_timestamp(pg_table_name, &mz_client).await;
    let join_ts =
        test_util::get_explain_timestamp(&format!("{pg_table_name}, t"), &mz_client).await;

    // If we go back to serializable, then timestamps can revert again.
    assert!(join_ts < source_ts);

    cleanup_fn(&mz_client, &pg_client).await;
}

#[mz_ore::test]
fn test_internal_users() {
    let server = test_util::TestHarness::default().start_blocking();

    assert!(
        server
            .pg_config()
            .user(&SYSTEM_USER.name)
            .connect(postgres::NoTls)
            .is_err()
    );
    assert!(
        server
            .pg_config_internal()
            .user(&SYSTEM_USER.name)
            .connect(postgres::NoTls)
            .is_ok()
    );
    assert!(
        server
            .pg_config_internal()
            .user(&SUPPORT_USER.name)
            .connect(postgres::NoTls)
            .is_ok()
    );
    assert!(
        server
            .pg_config_internal()
            .user("mz_something_else")
            .connect(postgres::NoTls)
            .is_err()
    );
}

#[mz_ore::test]
fn test_internal_users_cluster() {
    let server = test_util::TestHarness::default().start_blocking();

    for (user, expected_cluster) in INTERNAL_USER_NAME_TO_DEFAULT_CLUSTER.iter() {
        let mut internal_client = server
            .pg_config_internal()
            .user(user)
            .connect(postgres::NoTls)
            .unwrap();

        let cluster = internal_client
            .query_one("SHOW CLUSTER", &[])
            .unwrap()
            .get::<_, String>(0);
        assert_eq!(expected_cluster, &cluster);
    }
}

// Tests that you can have simultaneous connections on the internal and external ports without
// crashing
#[mz_ore::test]
fn test_internal_ports() {
    let server = test_util::TestHarness::default().start_blocking();

    {
        let mut external_client = server.connect(postgres::NoTls).unwrap();
        let mut internal_client = server
            .pg_config_internal()
            .user(&SYSTEM_USER.name)
            .connect(postgres::NoTls)
            .unwrap();

        assert_eq!(
            1,
            external_client
                .query_one("SELECT 1;", &[])
                .unwrap()
                .get::<_, i32>(0)
        );
        assert_eq!(
            1,
            internal_client
                .query_one("SELECT 1;", &[])
                .unwrap()
                .get::<_, i32>(0)
        );
    }

    {
        let mut external_client = server.connect(postgres::NoTls).unwrap();
        let mut internal_client = server
            .pg_config_internal()
            .user(&SYSTEM_USER.name)
            .connect(postgres::NoTls)
            .unwrap();

        assert_eq!(
            1,
            external_client
                .query_one("SELECT 1;", &[])
                .unwrap()
                .get::<_, i32>(0)
        );
        assert_eq!(
            1,
            internal_client
                .query_one("SELECT 1;", &[])
                .unwrap()
                .get::<_, i32>(0)
        );
    }
}

// Test that trying to alter an invalid system param returns an error.
// This really belongs in the resource-limits.td testdrive, but testdrive
// doesn't allow you to specify a connection and expect a failure which is
// needed for this test.
#[mz_ore::test]
fn test_alter_system_invalid_param() {
    let server = test_util::TestHarness::default().start_blocking();

    let mut mz_client = server
        .pg_config_internal()
        .user(&SYSTEM_USER.name)
        .connect(postgres::NoTls)
        .unwrap();

    mz_client
        .batch_execute("ALTER SYSTEM SET max_tables TO 2")
        .unwrap();
    let res = mz_client
        .batch_execute("ALTER SYSTEM SET invalid_param TO 42")
        .unwrap_err();
    assert!(
        res.to_string()
            .contains("unrecognized configuration parameter \"invalid_param\"")
    );
    let res = mz_client
        .batch_execute("ALTER SYSTEM RESET invalid_param")
        .unwrap_err();
    assert!(
        res.to_string()
            .contains("unrecognized configuration parameter \"invalid_param\"")
    );
}

#[mz_ore::test]
fn test_concurrent_writes() {
    let server = test_util::TestHarness::default().start_blocking();

    let num_tables = 10;

    {
        let mut client = server.connect(postgres::NoTls).unwrap();
        for i in 0..num_tables {
            client
                .batch_execute(&format!("CREATE TABLE t_{i} (a INT, b text, c text)"))
                .unwrap();
        }
    }

    let num_threads = 3;
    let num_loops = 10;

    let mut clients = Vec::new();
    for _ in 0..num_threads {
        clients.push(server.connect(postgres::NoTls).unwrap());
    }

    let handles: Vec<_> = clients
        .into_iter()
        .map(|mut client| {
            std::thread::spawn(move || {
                for j in 0..num_loops {
                    for i in 0..num_tables {
                        let string_a = "A";
                        let string_b = "B";
                        client
                            .batch_execute(&format!(
                                "INSERT INTO t_{i} VALUES ({j}, '{string_a}', '{string_b}')"
                            ))
                            .unwrap();
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let mut client = server.connect(postgres::NoTls).unwrap();

    for i in 0..num_tables {
        let count = client
            .query_one(&format!("SELECT count(*) FROM t_{i}"), &[])
            .unwrap()
            .get::<_, i64>(0);
        assert_eq!(num_loops * num_threads, count);
    }
}

#[mz_ore::test]
fn test_load_generator() {
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();
    server.enable_feature_flags(&[
        "enable_create_table_from_source",
        "enable_load_generator_counter",
    ]);
    let mut client = server.connect(postgres::NoTls).unwrap();

    client
        .batch_execute("CREATE SOURCE counter FROM LOAD GENERATOR COUNTER (TICK INTERVAL '1ms')")
        .unwrap();

    client
        .batch_execute("CREATE TABLE counter_tbl FROM SOURCE counter")
        .unwrap();

    let row = client
        .query_one("SELECT count(*), mz_now()::text FROM counter_tbl", &[])
        .unwrap();
    let initial_count: i64 = row.get(0);
    let timestamp_millis: String = row.get(1);
    let timestamp_millis: i64 = timestamp_millis.parse().unwrap();
    const WAIT: i64 = 100;
    let next = timestamp_millis + WAIT;
    let expect = initial_count + WAIT;
    Retry::default()
        .retry(|_| {
            let row = client
                .query_one(
                    &format!("SELECT count(*) FROM counter_tbl AS OF AT LEAST {next}"),
                    &[],
                )
                .unwrap();
            let count: i64 = row.get(0);
            if count < expect {
                Err(format!("expected {expect}, got {count}"))
            } else {
                Ok(())
            }
        })
        .unwrap();
}

#[mz_ore::test]
fn test_support_user_permissions() {
    let server = test_util::TestHarness::default().start_blocking();

    let mut external_client = server.connect(postgres::NoTls).unwrap();
    let mut internal_client = server
        .pg_config_internal()
        .user(&SUPPORT_USER.name)
        .connect(postgres::NoTls)
        .unwrap();

    external_client
        .batch_execute("CREATE TABLE materialize.public.t1 (a INT)")
        .unwrap();

    internal_client
        .batch_execute("SET CLUSTER TO 'mz_catalog_server'")
        .unwrap();

    assert_err!(internal_client.query("SELECT * FROM materialize.public.t1", &[]));
    assert_err!(internal_client.batch_execute("INSERT INTO materialize.public.t1 VALUES (1)"));
    assert_err!(internal_client.batch_execute("CREATE TABLE t2 (a INT)"));

    assert_ok!(internal_client.query("SELECT * FROM mz_internal.mz_comments", &[]));
    assert_ok!(internal_client.query("SELECT * FROM mz_catalog.mz_tables", &[]));
    assert_ok!(internal_client.query("SELECT * FROM pg_catalog.pg_namespace", &[]));

    internal_client
        .batch_execute("SET CLUSTER TO 'mz_system'")
        .unwrap();
    assert_ok!(internal_client.query("SELECT * FROM mz_internal.mz_comments", &[]));
    assert_ok!(internal_client.query("SELECT * FROM mz_catalog.mz_tables", &[]));
    assert_ok!(internal_client.query("SELECT * FROM pg_catalog.pg_namespace", &[]));

    internal_client
        .batch_execute("SET CLUSTER TO 'default'")
        .unwrap();
    assert_err!(internal_client.query("SELECT * FROM t1", &[]));
    assert_ok!(internal_client.query("SELECT * FROM mz_internal.mz_comments", &[]));
    assert_ok!(internal_client.query("SELECT * FROM mz_catalog.mz_tables", &[]));
    assert_ok!(internal_client.query("SELECT * FROM pg_catalog.pg_namespace", &[]));
}

#[mz_ore::test]
fn test_idle_in_transaction_session_timeout() {
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();
    server.enable_feature_flags(&["unsafe_enable_unsafe_functions"]);

    let mut client = server.connect(postgres::NoTls).unwrap();
    client
        .batch_execute("SET idle_in_transaction_session_timeout TO '4ms'")
        .unwrap();
    client.batch_execute("BEGIN").unwrap();
    std::thread::sleep(Duration::from_millis(5));
    // Retry because sleep might be woken up early.
    Retry::default()
        .max_duration(Duration::from_secs(1))
        .retry(|_| {
            let res = client.query("SELECT 1", &[]);
            if let Err(error) = res {
                if error.is_closed() {
                    Ok(())
                } else {
                    Err(format!(
                        "error should indicates that the connection is closed: {error:?}"
                    ))
                }
            } else {
                Err(format!("query should return error: {res:?}"))
            }
        })
        .unwrap();

    // session should be timed out even if transaction has failed.
    let mut client = server.connect(postgres::NoTls).unwrap();
    client
        .batch_execute("SET idle_in_transaction_session_timeout TO '50ms'")
        .unwrap();
    client.batch_execute("BEGIN").unwrap();
    let error = client.batch_execute("SELECT 1/0").unwrap_err();
    assert!(
        !error.is_closed(),
        "failing a transaction should not close the connection: {error:?}"
    );
    std::thread::sleep(Duration::from_millis(51));
    // Retry because sleep might be woken up early.
    Retry::default()
        .max_duration(Duration::from_secs(1))
        .retry(|_| {
            let res = client.query("SELECT 1", &[]);
            if let Err(error) = res {
                if error.is_closed() {
                    Ok(())
                } else {
                    Err(format!(
                        "error should indicates that the connection is closed: {error:?}"
                    ))
                }
            } else {
                Err(format!("query should return error: {res:?}"))
            }
        })
        .unwrap();

    // session should not be timed out if it's not idle.
    let mut client = server.connect(postgres::NoTls).unwrap();
    client
        .batch_execute("SET idle_in_transaction_session_timeout TO '50ms'")
        .unwrap();
    client.batch_execute("BEGIN").unwrap();
    client.query("SELECT mz_unsafe.mz_sleep(1)", &[]).unwrap();
    client.query("SELECT 1", &[]).unwrap();
    client.batch_execute("COMMIT").unwrap();

    // 0 timeout indicated no timeout.
    let mut client = server.connect(postgres::NoTls).unwrap();
    client
        .batch_execute("SET idle_in_transaction_session_timeout TO 0")
        .unwrap();
    client.batch_execute("BEGIN").unwrap();
    std::thread::sleep(Duration::from_millis(5));
    client.query("SELECT 1", &[]).unwrap();
    client.batch_execute("COMMIT").unwrap();
}

#[mz_ore::test]
fn test_peek_on_dropped_cluster() {
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();

    let mut read_client = server.connect(postgres::NoTls).unwrap();
    read_client.batch_execute("CREATE TABLE t ()").unwrap();

    let handle = thread::spawn(move || {
        // Run query asynchronously that will hang forever.
        let res = read_client.query_one("SELECT * FROM t AS OF 18446744073709551615", &[]);
        assert_err!(res, "cancelled query should return error");
        let err = res.unwrap_err().unwrap_db_error();
        //TODO(jkosh44) This isn't actually an internal error but we often incorrectly give back internal errors.
        assert_eq!(
            err.code(),
            &SqlState::INTERNAL_ERROR,
            "error code should match INTERNAL_ERROR"
        );
        assert!(
            err.message()
                .contains("query could not complete because cluster \"quickstart\" was dropped"),
            "error message should contain 'query could not complete because cluster \"quickstart\" was dropped'"
        );
    });

    // Wait for asynchronous query to start.
    let mut client = server.connect(postgres::NoTls).unwrap();
    Retry::default()
        .max_duration(Duration::from_secs(10))
        .retry(|_| {
            let count: i64 = client
                .query_one("SELECT COUNT(*) FROM mz_introspection.mz_active_peeks", &[])
                .unwrap()
                .get(0);
            if count == 1 {
                Ok(())
            } else {
                Err(format!("query should return 1 rows, instead saw {count}"))
            }
        })
        .unwrap();

    // Drop cluster that query is running on, and table that query is selecting from.
    let mut drop_client = server
        .pg_config_internal()
        .user(&SYSTEM_USER.name)
        .connect(postgres::NoTls)
        .unwrap();
    drop_client
        .batch_execute("DROP CLUSTER quickstart;")
        .unwrap();
    drop_client.batch_execute("DROP TABLE t;").unwrap();
    handle.join().unwrap();
}

#[mz_ore::test]
fn test_emit_timestamp_notice() {
    let server = test_util::TestHarness::default().start_blocking();

    let (tx, mut rx) = futures::channel::mpsc::unbounded();

    let mut client = server
        .pg_config()
        .notice_callback(move |notice| {
            tx.unbounded_send(notice).unwrap();
        })
        .connect(postgres::NoTls)
        .unwrap();

    client.batch_execute("CREATE TABLE t (i INT)").unwrap();
    client
        .batch_execute("SET emit_timestamp_notice = true")
        .unwrap();
    client.batch_execute("SELECT * FROM t").unwrap();

    let timestamp_re = Regex::new("query timestamp: *([0-9]+)").unwrap();

    // Wait until there's a query timestamp notice.
    let first_timestamp = Retry::default()
        .retry(|_| {
            loop {
                match rx.try_next() {
                    Ok(Some(msg)) => {
                        if let Some(caps) = timestamp_re.captures(msg.detail().unwrap_or_default())
                        {
                            let ts: u64 = caps.get(1).unwrap().as_str().parse().unwrap();
                            return Ok(mz_repr::Timestamp::from(ts));
                        }
                    }
                    Ok(None) => panic!("unexpected channel close"),
                    Err(e) => return Err(e),
                }
            }
        })
        .unwrap();

    // Wait until the query timestamp notice goes up.
    Retry::default()
        .retry(|_| {
            client.batch_execute("SELECT * FROM t").unwrap();
            loop {
                match rx.try_next() {
                    Ok(Some(msg)) => {
                        if let Some(caps) = timestamp_re.captures(msg.detail().unwrap_or_default())
                        {
                            let ts: u64 = caps.get(1).unwrap().as_str().parse().unwrap();
                            let ts = mz_repr::Timestamp::from(ts);
                            if ts > first_timestamp {
                                return Ok(());
                            }
                            return Err("not yet advanced");
                        }
                    }
                    Ok(None) => panic!("unexpected channel close"),
                    Err(_) => return Err("no messages available"),
                }
            }
        })
        .unwrap();
}

#[mz_ore::test]
fn test_isolation_level_notice() {
    let server = test_util::TestHarness::default().start_blocking();

    let (tx, mut rx) = futures::channel::mpsc::unbounded();
    let mut client = server
        .pg_config()
        .notice_callback(move |notice| {
            tx.unbounded_send(notice).unwrap();
        })
        .connect(postgres::NoTls)
        .unwrap();

    client
        .batch_execute("SET TRANSACTION_ISOLATION TO 'READ UNCOMMITTED'")
        .unwrap();

    let notice_re = Regex::new(
        "transaction isolation level .* is unimplemented, the session will be upgraded to .*",
    )
    .unwrap();

    Retry::default()
        .max_duration(Duration::from_secs(10))
        .retry(|_| match rx.try_next() {
            Ok(Some(msg)) => notice_re
                .captures(msg.message())
                .ok_or("wrong message")
                .map(|_| ()),
            Ok(None) => panic!("unexpected channel close"),
            Err(_) => Err("no messages available"),
        })
        .unwrap();
}

#[test] // allow(test-attribute)
fn test_emit_tracing_notice() {
    let server = test_util::TestHarness::default()
        .with_enable_tracing(true)
        // NB: Make sure to keep opentelemetry_filter as "info" to match the
        // configuration in staging (and that we hope to run in prod).
        .with_system_parameter_default("opentelemetry_filter".to_string(), "info".to_string())
        .with_system_parameter_default("log_filter".to_string(), "info".to_string())
        .start_blocking();

    server.disable_feature_flags(&["enable_frontend_peek_sequencing"]);

    let (tx, mut rx) = futures::channel::mpsc::unbounded();

    let mut client = server
        .pg_config()
        .notice_callback(move |notice| {
            tx.unbounded_send(notice).unwrap();
        })
        .connect(postgres::NoTls)
        .unwrap();

    client
        .execute("SET emit_trace_id_notice = true", &[])
        .unwrap();
    let _row = client.query_one("SELECT 1;", &[]).unwrap();

    let tracing_re = Regex::new("trace id: (.*)").unwrap();
    match rx.try_next() {
        Ok(Some(msg)) => {
            // assert the NOTICE we recieved contained a trace_id
            let captures = tracing_re.captures(msg.message()).expect("no matches?");
            let trace_id = captures.get(1).expect("trace_id not captured?").as_str();

            assert!(trace_id.is_ascii());
            assert_eq!(trace_id.len(), 32);
        }
        x => panic!("failed to read message from channel, {:?}", x),
    }
}

#[mz_ore::test]
fn test_subscribe_on_dropped_source() {
    fn test_subscribe_on_dropped_source_inner(
        server: &TestServerWithRuntime,
        tables: Vec<&'static str>,
        subscribe: &'static str,
        assertion: impl FnOnce(Result<(), tokio_postgres::error::Error>) + Send + 'static,
    ) {
        assert!(
            subscribe.to_lowercase().contains("subscribe"),
            "test only works with subscribe queries, query: {subscribe}"
        );

        let mut client = server.connect(postgres::NoTls).unwrap();
        for table in &tables {
            client
                .batch_execute(&format!("CREATE TABLE {table} (a INT);"))
                .unwrap();
            client
                .batch_execute(&format!("INSERT INTO {table} VALUES (1);"))
                .unwrap();
        }

        let mut query_client = server.pg_config().connect(postgres::NoTls).unwrap();
        let query_thread = std::thread::spawn(move || {
            let res = query_client.batch_execute(subscribe);
            assertion(res);
        });

        Retry::default()
            .max_duration(Duration::from_secs(10))
            .retry(|_| {
                let row = client
                    .query_one("SELECT count(*) FROM mz_internal.mz_subscriptions;", &[])
                    .unwrap();
                let count: i64 = row.get(0);
                if count < 1 {
                    Err("no active subscribe")
                } else {
                    Ok(())
                }
            })
            .unwrap();

        for table in &tables {
            client
                .batch_execute(&format!("DROP TABLE {table};"))
                .unwrap();
        }

        query_thread.join().unwrap();

        Retry::default()
            .max_duration(Duration::from_secs(100))
            .retry(|_| {
                let row = client
                    .query_one("SELECT count(*) FROM mz_internal.mz_subscriptions;", &[])
                    .unwrap();
                let count: i64 = row.get(0);
                if count > 0 {
                    Err("subscribe still active")
                } else {
                    Ok(())
                }
            })
            .unwrap();
    }

    fn assert_subscribe_error(res: Result<(), tokio_postgres::error::Error>) {
        assert_err!(res);
        assert!(
            res.unwrap_db_error()
                .message()
                .contains("subscribe has been terminated because underlying relation")
        );
    }

    let server = test_util::TestHarness::default().start_blocking();

    test_subscribe_on_dropped_source_inner(
        &server,
        vec!["t"],
        "SUBSCRIBE t",
        assert_subscribe_error,
    );
    test_subscribe_on_dropped_source_inner(
        &server,
        vec!["t1", "t2"],
        "SUBSCRIBE (SELECT * FROM t1, t2)",
        assert_subscribe_error,
    );
}

#[mz_ore::test]
fn test_dont_drop_sinks_twice() {
    let server = test_util::TestHarness::default().start_blocking();

    let mut client_a = server.pg_config().connect(postgres::NoTls).unwrap();

    client_a.batch_execute("CREATE TABLE t1 (a INT)").unwrap();
    client_a.batch_execute("CREATE TABLE t2 (a INT)").unwrap();
    let client_a_token = client_a.cancel_token();

    let mut out = client_a
        .copy_out("COPY (SUBSCRIBE (SELECT * FROM t1,t2)) TO STDOUT")
        .unwrap();

    // Drop the tables that the subscribe depend on in a second session.
    let mut client_b = server.connect(postgres::NoTls).unwrap();

    // By inserting 10_000 rows into t1 and t2, it's very likely that the response from
    // compute indicating whether or not we successfully dropped the sink, will get
    // queued behind the responses for the row insertions, which should trigger the race
    // condition we're trying to exercise here.
    client_b
        .batch_execute("INSERT INTO t1 SELECT generate_series(0, 10000)")
        .unwrap();
    client_b
        .batch_execute("INSERT INTO t2 SELECT generate_series(0, 10000)")
        .unwrap();
    client_b.batch_execute("DROP TABLE t1").unwrap();
    client_b.batch_execute("DROP TABLE t2").unwrap();

    client_a_token
        .cancel_query(postgres::NoTls)
        .expect("failed to cancel subscribe");
    let err = out.read_to_end(&mut vec![]).unwrap_err();
    assert!(err.to_string().contains("subscribe has been terminated"));

    drop(out);
    client_a.close().expect("failed to drop client");
}

// This can almost be tested with SLT using the simple directive, but
// we have no way to disconnect sessions using SLT.
#[mz_ore::test]
fn test_mz_sessions() {
    let server = test_util::TestHarness::default().start_blocking();
    // TODO(peek-seq): This would currently fail with the flag on, indicating a bug in the frontend
    // peek sequencing. Re-enable this once we never fall back from the frontend peek sequencing to
    // the old peek sequencing. See comment on the call to `waiting_on_startup_appends` in
    // `frontend_peek.rs`.
    server.disable_feature_flags(&["enable_frontend_peek_sequencing"]);

    let mut foo_client = server
        .pg_config()
        .user("foo")
        .connect(postgres::NoTls)
        .unwrap();

    // Active session appears in mz_sessions.
    assert_eq!(
        foo_client
            .query_one("SELECT count(*) FROM mz_internal.mz_sessions", &[])
            .unwrap()
            .get::<_, i64>(0),
        1,
    );
    let foo_session_row = foo_client
        .query_one(
            "SELECT connection_id::int8, role_id FROM mz_internal.mz_sessions",
            &[],
        )
        .unwrap();
    let foo_conn_id = foo_session_row.get::<_, i64>("connection_id");
    let foo_role_id = foo_session_row.get::<_, String>("role_id");
    assert_eq!(
        foo_client
            .query_one("SELECT name FROM mz_roles WHERE id = $1", &[&foo_role_id])
            .unwrap()
            .get::<_, String>(0),
        "foo",
    );

    // Concurrent session appears in mz_sessions and is removed from mz_sessions.
    {
        let mut bar_client = server
            .pg_config()
            .user("bar")
            .connect(postgres::NoTls)
            .unwrap();
        assert_eq!(
            bar_client
                .query_one("SELECT count(*) FROM mz_internal.mz_sessions", &[])
                .unwrap()
                .get::<_, i64>(0),
            2,
        );
        let bar_session_row = bar_client
            .query_one(
                &format!("SELECT role_id FROM mz_internal.mz_sessions WHERE connection_id <> {foo_conn_id}"),
                &[],
            )
            .unwrap();
        let bar_role_id = bar_session_row.get::<_, String>("role_id");
        assert_eq!(
            bar_client
                .query_one("SELECT name FROM mz_roles WHERE id = $1", &[&bar_role_id])
                .unwrap()
                .get::<_, String>(0),
            "bar",
        );
    }

    // Wait for the previous session to get cleaned up.
    std::thread::sleep(Duration::from_secs(3));

    assert_eq!(
        foo_client
            .query_one("SELECT count(*) FROM mz_internal.mz_sessions", &[])
            .unwrap()
            .get::<_, i64>(0),
        1,
    );
    assert_eq!(
        foo_client
            .query_one(
                "SELECT connection_id::int8 FROM mz_internal.mz_sessions",
                &[]
            )
            .unwrap()
            .get::<_, i64>("connection_id"),
        foo_conn_id,
    );

    // Concurrent session, with the same name as active session,
    // appears in mz_sessions and is removed from mz_sessions.
    {
        let mut other_foo_client = server
            .pg_config()
            .user("foo")
            .connect(postgres::NoTls)
            .unwrap();
        assert_eq!(
            other_foo_client
                .query_one("SELECT count(*) FROM mz_internal.mz_sessions", &[])
                .unwrap()
                .get::<_, i64>(0),
            2,
        );
        let other_foo_session_row = foo_client
            .query_one(
                &format!("SELECT connection_id::int8, role_id FROM mz_internal.mz_sessions WHERE connection_id <> {foo_conn_id}"),
                &[],
            )
            .unwrap();
        let other_foo_conn_id = other_foo_session_row.get::<_, i64>("connection_id");
        let other_foo_role_id = other_foo_session_row.get::<_, String>("role_id");
        assert_ne!(foo_conn_id, other_foo_conn_id);
        assert_eq!(foo_role_id, other_foo_role_id);
    }

    // Wait for the previous session to get cleaned up.
    std::thread::sleep(Duration::from_secs(3));

    assert_eq!(
        foo_client
            .query_one("SELECT count(*) FROM mz_internal.mz_sessions", &[])
            .unwrap()
            .get::<_, i64>(0),
        1,
    );
    assert_eq!(
        foo_client
            .query_one(
                "SELECT connection_id::int8 FROM mz_internal.mz_sessions",
                &[]
            )
            .unwrap()
            .get::<_, i64>("connection_id"),
        foo_conn_id,
    );
}

#[mz_ore::test]
fn test_auto_run_on_introspection_feature_enabled() {
    // unsafe_mode enables the feature as a whole
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();

    let (tx, mut rx) = futures::channel::mpsc::unbounded();
    let mut client = server
        .pg_config()
        .notice_callback(move |notice| {
            tx.unbounded_send(notice).unwrap();
        })
        .connect(postgres::NoTls)
        .unwrap();

    let mut assert_catalog_server_notice = |expected| {
        match (rx.try_next(), expected) {
            (Ok(Some(notice)), true) => {
                let msg = notice.message();
                let expected = "query was automatically run on the \"mz_catalog_server\" cluster";
                assert_eq!(msg, expected);
            }
            (Err(_), false) => (),
            (_, true) => panic!("Didn't get the expected notice!"),
            (res, false) => panic!("Got a notice, but it wasn't expected! {:?}", res),
        }
        // Drain the channel of any other notices
        while let Ok(Some(_)) = rx.try_next() {}
    };

    // The notice we assert on only gets emitted at the DEBUG level
    client
        .execute("SET client_min_messages = debug", &[])
        .unwrap();

    // Simple queries with no dependencies should get run on the catalog server cluster
    let _row = client.query_one("SELECT 1;", &[]).unwrap();
    assert_catalog_server_notice(true);

    // Not "simple" queries with no dependencies shouldn't get run on the catalog server cluster
    let _row = client
        .query_one("SELECT generate_series(1, 1);", &[])
        .unwrap();
    assert_catalog_server_notice(false);

    // Queries that only depend on system tables, __should__ get run on the catalog server cluster
    let _row = client
        .query_one("SELECT * FROM mz_functions LIMIT 1", &[])
        .unwrap();
    assert_catalog_server_notice(true);

    let _row = client
        .query_one("SELECT * FROM pg_attribute LIMIT 1", &[])
        .unwrap();
    assert_catalog_server_notice(true);

    // Start our subscribe.
    client
        .batch_execute("BEGIN; DECLARE c CURSOR FOR SUBSCRIBE (SELECT * FROM mz_functions);")
        .unwrap();
    assert_catalog_server_notice(false);

    // Fetch all of the rows.
    client.batch_execute("FETCH ALL c").unwrap();
    assert_catalog_server_notice(true);

    // End the subscribe.
    client.batch_execute("COMMIT").unwrap();
    assert_catalog_server_notice(false);

    // ... even more complex queries that depend on multiple system tables
    let _rows = client
        .query("SELECT mz_types.name, mz_types.id FROM mz_types JOIN mz_base_types ON mz_types.id = mz_base_types.id", &[])
        .unwrap();
    assert_catalog_server_notice(true);

    client
        .execute(
            "CREATE VIEW user_made AS SELECT name FROM mz_functions;",
            &[],
        )
        .unwrap();
    assert_catalog_server_notice(false);

    // But querying user made objects should not result in queries being run on mz_catalog_server.
    let _row = client
        .query_one("SELECT * FROM user_made LIMIT 1", &[])
        .unwrap();
    assert_catalog_server_notice(false);
}

const INTROSPECTION_NOTICE: &str = "results from querying these objects depend on the current values of the `cluster` and `cluster_replica` session variables";

#[mz_ore::test]
fn test_auto_run_on_introspection_feature_disabled() {
    // unsafe_mode enables the feature as a whole
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();

    let (tx, mut rx) = futures::channel::mpsc::unbounded();
    let mut client = server
        .pg_config()
        .notice_callback(move |notice| {
            tx.unbounded_send(notice).unwrap();
        })
        .connect(postgres::NoTls)
        .unwrap();

    let mut assert_notice = |expected: Option<&str>| {
        match (rx.try_next(), expected) {
            (Ok(Some(notice)), Some(expected)) => {
                let msg = notice.message();
                assert!(msg.contains(expected));
            }
            (Err(_), None) => (),
            (_, Some(_)) => panic!("Didn't get the expected notice!"),
            (res, None) => panic!("Got a notice, but it wasn't expected! {:?}", res),
        }
        // Drain the channel of any other notices
        while let Ok(Some(_)) = rx.try_next() {}
    };

    // The notice we assert on only gets emitted at the DEBUG level
    client
        .execute("SET client_min_messages = debug", &[])
        .unwrap();
    // Disable the feature, as a user would
    client
        .execute("SET auto_route_catalog_queries = false", &[])
        .unwrap();

    // Nothing should emit the notice

    let _row = client
        .query_one("SELECT * FROM mz_functions LIMIT 1", &[])
        .unwrap();
    assert_notice(None);

    let _row = client
        .query_one("SELECT * FROM pg_attribute LIMIT 1", &[])
        .unwrap();
    assert_notice(None);

    let _rows = client
        .query("SELECT * FROM mz_introspection.mz_active_peeks", &[])
        .unwrap();
    assert_notice(Some(INTROSPECTION_NOTICE));

    let _rows = client
        .query(
            "SELECT * FROM mz_introspection.mz_dataflow_operator_parents",
            &[],
        )
        .unwrap();
    assert_notice(Some(INTROSPECTION_NOTICE));

    client
        .batch_execute("BEGIN; DECLARE c CURSOR FOR SUBSCRIBE (SELECT * FROM mz_functions);")
        .unwrap();
    assert_notice(None);

    client.batch_execute("FETCH ALL c").unwrap();
    assert_notice(None);

    client.batch_execute("COMMIT").unwrap();
    assert_notice(None);
}

#[mz_ore::test]
fn test_auto_run_on_introspection_per_replica_relations() {
    // unsafe_mode enables the feature as a whole
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();

    let (tx, mut rx) = futures::channel::mpsc::unbounded();
    let mut client = server
        .pg_config()
        .notice_callback(move |notice| {
            tx.unbounded_send(notice).unwrap();
        })
        .connect(postgres::NoTls)
        .unwrap();

    let mut assert_notice = |expected: Option<&str>| {
        match (rx.try_next(), expected) {
            (Ok(Some(notice)), Some(expected)) => {
                let msg = notice.message();
                assert!(msg.contains(expected));
            }
            (Err(_), None) => (),
            (_, Some(_)) => panic!("Didn't get the expected notice!"),
            (res, None) => panic!("Got a notice, but it wasn't expected! {:?}", res),
        }
        // Drain the channel of any other notices
        while let Ok(Some(_)) = rx.try_next() {}
    };

    // The notice we assert on only gets emitted at the DEBUG level
    client
        .execute("SET client_min_messages = debug", &[])
        .unwrap();

    // Disable the feature, as a user would
    client
        .execute("SET auto_route_catalog_queries = false", &[])
        .unwrap();

    // If a system object is a per-replica relation we do not want to automatically run
    // it on the mz_catalog_server cluster

    // `mz_active_peeks` is a per-replica relation
    let _rows = client
        .query("SELECT * FROM mz_introspection.mz_active_peeks", &[])
        .unwrap();
    assert_notice(Some(INTROSPECTION_NOTICE));

    // `mz_dataflow_operator_parents` is a VIEW that depends on per-replica relations
    let _rows = client
        .query(
            "SELECT * FROM mz_introspection.mz_dataflow_operator_parents",
            &[],
        )
        .unwrap();
    assert_notice(Some(INTROSPECTION_NOTICE));

    // Enable the feature
    client
        .execute("SET auto_route_catalog_queries = true", &[])
        .unwrap();

    // Even after enabling the feature we still shouldn't emit the auto-routing notice

    let _rows = client
        .query(
            "SELECT * FROM mz_introspection.mz_dataflow_operator_parents",
            &[],
        )
        .unwrap();
    assert_notice(Some(INTROSPECTION_NOTICE));

    let _rows = client
        .query("SELECT * FROM mz_introspection.mz_active_peeks", &[])
        .unwrap();
    assert_notice(Some(INTROSPECTION_NOTICE));
}

#[mz_ore::test]
fn test_pg_cancel_backend() {
    mz_ore::test::init_logging();
    let server = test_util::TestHarness::default().start_blocking();

    let mut mz_client = server
        .pg_config_internal()
        .user(&SYSTEM_USER.name)
        .connect(postgres::NoTls)
        .unwrap();
    mz_client
        .batch_execute("ALTER SYSTEM SET enable_rbac_checks TO true")
        .unwrap();

    let mut client1 = server.connect(postgres::NoTls).unwrap();
    let mut client2 = server.connect(postgres::NoTls).unwrap();

    // Connect as another user who should not be able to cancel the query.
    let mut client_user = server
        .pg_config()
        .user("other")
        .connect(postgres::NoTls)
        .unwrap();

    let _ = client1.batch_execute("CREATE TABLE t (i INT)").unwrap();

    // Start a thread to perform the cancel while the SUBSCRIBE is running in this thread.
    let handle = thread::spawn(move || {
        // Wait for the subscription to start.
        let conn_id = Retry::default()
            .retry(|_| {
                let conn_id: String = client2
                    .query_one(
                        "SELECT s.connection_id::text FROM mz_internal.mz_subscriptions b JOIN mz_internal.mz_sessions s ON s.id = b.session_id",
                        &[],
                    )?
                    .get(0);
                Ok::<_, postgres::Error>(conn_id)
            })
            .unwrap();

        // The other user doesn't have permission to cancel.
        assert_contains!(
            client_user
                .query_one(&format!("SELECT pg_cancel_backend({conn_id})"), &[])
                .unwrap_err()
                .to_string(),
            r#"must be a member of "materialize""#
        );

        let found_conn: bool = client2
            .query_one(&format!("SELECT pg_cancel_backend({conn_id})"), &[])
            .unwrap()
            .get(0);
        assert!(found_conn);
    });

    let err = client1.query("SUBSCRIBE t", &[]).unwrap_err();
    assert_contains!(err.to_string(), "canceling statement due to user request");

    handle.join().unwrap();

    assert_contains!(
        client1
            .query_one("SELECT * FROM (SELECT pg_cancel_backend(1))", &[])
            .unwrap_err()
            .to_string(),
        "pg_cancel_backend in this position",
    );

    // Ensure cancelling oneself does anything besides panic. Currently it cancels itself.
    let conn_id: i32 = client1
        .query_one("SELECT pg_backend_pid()", &[])
        .unwrap()
        .get(0);
    assert_contains!(
        client1
            .query_one(&format!("SELECT pg_cancel_backend({conn_id})"), &[])
            .unwrap_err()
            .to_string(),
        "canceling statement due to user request",
    );

    // Test that canceling with a parameter is accepted by the planner. 99999 is
    // an arbitrary connection ID that will not exist.
    let found_conn: bool = client1
        .query_one("SELECT pg_cancel_backend($1)", &[&99999_i32])
        .unwrap()
        .get(0);
    assert!(!found_conn);
    let found_conn: bool = client1
        .query_one("SELECT pg_cancel_backend($1::int4)", &[&99999_i32])
        .unwrap()
        .get(0);
    assert!(!found_conn);
    let found_conn: bool = client1
        .query_one("SELECT pg_cancel_backend($1 + 42)", &[&99999_i32])
        .unwrap()
        .get(0);
    assert!(!found_conn);

    // Cancelling in a transaction.
    client1.batch_execute("BEGIN").unwrap();
    assert_contains!(
        client1
            .query_one(&format!("SELECT pg_cancel_backend({conn_id})"), &[])
            .unwrap_err()
            .to_string(),
        "canceling statement due to user request",
    );
    assert_contains!(
        client1.batch_execute("SELECT 1").unwrap_err().to_string(),
        "current transaction is aborted"
    );
}

// Test params in interesting places.
#[mz_ore::test]
fn test_params() {
    mz_ore::test::init_logging();
    let server = test_util::TestHarness::default().start_blocking();

    let mut client = server.connect(postgres::NoTls).unwrap();

    assert_eq!(
        client
            .query_one("SELECT $1", &[&"test"])
            .unwrap()
            .get::<_, String>(0),
        "test"
    );
    assert_eq!(
        client
            .query_one("SUBSCRIBE (SELECT $1 as c)", &[&"test"])
            .unwrap()
            .get::<_, String>("c"),
        "test"
    );

    client.batch_execute("BEGIN").unwrap();
    client
        .execute("DECLARE c CURSOR FOR SELECT $1", &[&"test"])
        .map_err_to_string_with_causes()
        .unwrap();
    assert_eq!(
        client
            .query_one("FETCH c", &[])
            .map_err_to_string_with_causes()
            .unwrap()
            .get::<_, String>(0),
        "test"
    );
    client.batch_execute("COMMIT").unwrap();

    client.batch_execute("BEGIN").unwrap();
    client
        .execute("DECLARE c CURSOR FOR SUBSCRIBE(SELECT $1 as c)", &[&"test"])
        .map_err_to_string_with_causes()
        .unwrap();
    assert_eq!(
        client
            .query_one("FETCH c", &[])
            .map_err_to_string_with_causes()
            .unwrap()
            .get::<_, String>("c"),
        "test"
    );
    client.batch_execute("COMMIT").unwrap();
}

// Test pg_cancel_backend after the authenticated role is dropped.
#[mz_ore::test]
fn test_pg_cancel_dropped_role() {
    let server = test_util::TestHarness::default().start_blocking();

    // TODO(peek-seq): This would currently fail with the flag on, indicating a bug in the frontend
    // peek sequencing. Re-enable this once we never fall back from the frontend peek sequencing to
    // the old peek sequencing. See comment on the call to `waiting_on_startup_appends` in
    // `frontend_peek.rs`.
    server.disable_feature_flags(&["enable_frontend_peek_sequencing"]);

    let dropped_role = "r1";

    let mut query_client = server.connect(postgres::NoTls).unwrap();

    // Create role.
    query_client
        .execute(&format!("CREATE ROLE {dropped_role}"), &[])
        .unwrap();

    // Start session using role.
    let mut dropped_client = server
        .pg_config()
        .user(dropped_role)
        .connect(postgres::NoTls)
        .unwrap();

    // Get the connection ID of the new session.
    let connection_id = dropped_client
        .query_one(
            &format!(
                "SELECT s.connection_id
         FROM mz_internal.mz_sessions s
         JOIN mz_roles r ON s.role_id = r.id
         WHERE r.name = '{dropped_role}'"
            ),
            &[],
        )
        .unwrap()
        .get::<_, UInt4>(0)
        .0;

    // Drop role.
    query_client
        .execute(&format!("DROP ROLE {dropped_role}"), &[])
        .unwrap();

    // Test that pg_cancel_backend doesn't panic. It's expected to fail with a privilege error. To
    // execute pg_cancel_backend you must be a member of the authenticated role of the connection.
    // Since the authenticated role doesn't exist, no one can be a member, and no one will have the
    // required privileges.
    let err = query_client
        .query_one(&format!("SELECT pg_cancel_backend({connection_id})"), &[])
        .unwrap_err();

    assert_eq!(err.code().unwrap(), &SqlState::INSUFFICIENT_PRIVILEGE);
}

#[mz_ore::test]
fn test_peek_on_dropped_indexed_view() {
    let server = test_util::TestHarness::default().start_blocking();

    // TODO(peek-seq): This needs peek cancellation to work, which is not yet implemented in the
    // new peek sequencing.
    server.disable_feature_flags(&["enable_frontend_peek_sequencing"]);

    let mut ddl_client = server.connect(postgres::NoTls).unwrap();
    let mut peek_client = server.connect(postgres::NoTls).unwrap();

    ddl_client.batch_execute("CREATE TABLE t (a INT)").unwrap();
    ddl_client
        .batch_execute("CREATE VIEW v AS SELECT (a + 1) AS a FROM t")
        .unwrap();
    ddl_client.batch_execute("CREATE INDEX i ON v (a)").unwrap();

    // Asynchronously query an indexed view.
    let handle = thread::spawn(move || {
        peek_client.query(
            &format!("SELECT * FROM v AS OF {}", mz_repr::Timestamp::MAX),
            &[],
        )
    });

    let index_id: String = ddl_client
        .query_one("SELECT id FROM mz_indexes WHERE name = 'i'", &[])
        .unwrap()
        .get(0);

    // Wait for SELECT to start.
    Retry::default()
        .max_duration(Duration::from_secs(10))
        .retry(|_| {
            let count: i64 = ddl_client
                .query_one(
                    &format!(
                "SELECT COUNT(*) FROM mz_introspection.mz_active_peeks WHERE object_id = '{index_id}'"
            ),
                    &[],
                )
                .unwrap()
                .get(0);
            if count <= 0 {
                Err("Peek not active")
            } else {
                Ok(())
            }
        })
        .unwrap();

    // Drop the table, view, and index.
    ddl_client.batch_execute("DROP TABLE t CASCADE").unwrap();

    // Check that the peek is cancelled an all resources are cleaned up.
    let select_res = handle.join().unwrap();
    let select_err = select_res.unwrap_err().to_string();
    assert!(
        select_err.contains(
            "query could not complete because relation \"materialize.public.v\" was dropped"
        ),
        "unexpected error: {select_err}"
    );
    Retry::default()
        .max_duration(Duration::from_secs(10))
        .retry(|_| {
            let count: i64 = ddl_client
                .query_one(
                    &format!(
                        "SELECT COUNT(*) FROM mz_introspection.mz_active_peeks WHERE object_id = '{index_id}'"
                    ),
                    &[],
                )
                .unwrap()
                .get(0);
            if count > 0 {
                Err("Peek still active")
            } else {
                Ok(())
            }
        })
        .unwrap();
}

/// Test AS OF in EXPLAIN. This output will only differ from the non-ASOF versions with RETAIN
/// HISTORY, where the object and its indexes have differing compaction policies.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
async fn test_explain_as_of() {
    // TODO: This would be better in testdrive, but we'd need to support negative intervals in AS
    // OF first.
    let server = test_util::TestHarness::default().start().await;
    let client = server.connect().await.unwrap();

    // Retry until we are able to explain plan and timestamp for a few seconds ago.
    // mz_object_dependencies is a retained metrics table which is why a historical AS OF
    // works.
    Retry::default()
        .clamp_backoff(Duration::from_secs(1))
        .retry_async(|_| async {
            let now: String = client
                .query_one("SELECT mz_now()::text", &[])
                .await
                .unwrap()
                .get(0);
            let now: u64 = now.parse().unwrap();
            let ts = now - 3000;
            let query = format!("mz_internal.mz_object_dependencies AS OF {ts}");
            let query_ts = try_get_explain_timestamp(&query, &client).await?;
            assert_eq!(ts, query_ts);
            client
                .query_one(
                    &format!("EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM {query}"),
                    &[],
                )
                .await?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .unwrap();
}

// Test that RETAIN HISTORY results in the since and upper being separated by the specified amount.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[ignore] // TODO: Reenable when database-issues#7450 is fixed
async fn test_retain_history() {
    let server = test_util::TestHarness::default().start().await;
    let client = server.connect().await.unwrap();
    let sys_client = server
        .connect()
        .internal()
        .user(&SYSTEM_USER.name)
        .await
        .unwrap();

    // Must fail before flag set.
    assert!(
        client
            .batch_execute(
                "CREATE MATERIALIZED VIEW v WITH (RETAIN HISTORY = FOR '2s') AS SELECT * FROM t",
            )
            .await
            .is_err()
    );
    assert!(
        client
            .batch_execute(
                "CREATE SOURCE s FROM LOAD GENERATOR COUNTER WITH (RETAIN HISTORY = FOR '2s')",
            )
            .await
            .is_err()
    );

    sys_client
        .batch_execute("ALTER SYSTEM SET enable_logical_compaction_window = true")
        .await
        .unwrap();

    client
        .batch_execute("CREATE TABLE t (a INT4)")
        .await
        .unwrap();
    client
        .batch_execute("INSERT INTO t VALUES (1)")
        .await
        .unwrap();

    assert_contains!(
        client
            .batch_execute(
                "CREATE MATERIALIZED VIEW v WITH (RETAIN HISTORY = FOR '-2s') AS SELECT * FROM t",
            )
            .await
            .unwrap_err()
            .to_string(),
        "invalid RETAIN HISTORY"
    );

    client
        .batch_execute(
            "CREATE MATERIALIZED VIEW v WITH (RETAIN HISTORY = FOR '5s') AS SELECT * FROM t",
        )
        .await
        .unwrap();
    client
        .batch_execute(
            "CREATE SOURCE s FROM LOAD GENERATOR COUNTER WITH (RETAIN HISTORY = FOR '5s')",
        )
        .await
        .unwrap();
    client
        .batch_execute(
            "CREATE SOURCE s_auction FROM LOAD GENERATOR AUCTION FOR ALL TABLES WITH (RETAIN HISTORY = FOR '2s')",
        )
        .await
        .unwrap();

    // users is a subsource on the auction source.
    for name in ["v", "s", "users"] {
        // Test compaction and querying without an index present.
        Retry::default()
            .retry_async(|_| async {
                let ts = get_explain_timestamp_determination(name, &client).await?;
                let source = ts.sources.into_element();
                let upper = source.write_frontier.into_element();
                let since = source.read_frontier.into_element();
                if upper.saturating_sub(since) < Timestamp::from(2000u64) {
                    anyhow::bail!("{upper} - {since} should be at least 2s apart")
                }
                client
                    .query(
                        &format!(
                            "SELECT 1 FROM {name} LIMIT 1 AS OF {}-1000",
                            ts.determination.timestamp_context.timestamp_or_default()
                        ),
                        &[],
                    )
                    .await?;
                Ok(())
            })
            .await
            .unwrap();

        // We should be able to query the views now and in the past.
        let ts = get_explain_timestamp(name, &client).await;
        let now_vals = client
            .query(&format!("SELECT * FROM {name} AS OF {ts}"), &[])
            .await
            .expect("this query should succeed")
            .into_iter()
            .count();

        let earlier_values = client
            .query(&format!("SELECT * FROM {name} AS OF {ts}-1000"), &[])
            .await
            .expect("this query should succeed")
            .into_iter()
            .count();

        // Out of bounds AS OF should fail.
        assert_contains!(
            client
                .query(&format!("SELECT * FROM {name} AS OF {ts}-10000"), &[])
                .await
                .unwrap_err()
                .to_string(),
            "not valid for all inputs"
        );

        // We sanity check that we have _some_
        assert!(now_vals > 0);
        assert!(earlier_values > 0);
        // The `s` view is of the counter source, so it should have more values now than earlier.
        if name == "s" {
            assert!(now_vals > earlier_values);
        }
    }
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
async fn test_temporal_static_queries() {
    let server = test_util::TestHarness::default().start().await;
    let client = server.connect().await.unwrap();

    client
        .batch_execute("CREATE SCHEMA dev_fy2023;")
        .await
        .unwrap();
    client
        .batch_execute("CREATE SCHEMA dev_warm;")
        .await
        .unwrap();
    client.batch_execute("CREATE SCHEMA dev;").await.unwrap();
    client
        .batch_execute(
            "CREATE VIEW dev.mock_data_days AS
                WITH
                    days AS (
                        SELECT generate_series(
                            CAST('2022-12-01 11:00:00' AS timestamp),
                            CAST('2024-02-01' AS timestamp),
                            CAST('1 hour' AS interval)
                        ) AS \"day\"
                    )
                SELECT
                    \"day\" AS ts,
                    datediff('hour', CAST('2020-01-01' AS timestamp), \"day\") AS id
                FROM days;",
        )
        .await
        .unwrap();
    client
        .batch_execute(
            "CREATE VIEW dev_warm.stg_data_days AS
                SELECT *
                FROM dev.mock_data_days
                WHERE
                    mz_now() <= date_trunc('year', ts + CAST('1 year' AS interval)) AND
                    ts + CAST('7 days' AS interval) - CAST('1 month' AS interval) < mz_now();",
        )
        .await
        .unwrap();
    client
        .batch_execute(
            "CREATE VIEW dev_warm.count_by_day AS
                SELECT
                    date_trunc('day', ts) AS \"day\",
                    count(*) AS cnt
                FROM dev_warm.stg_data_days
                GROUP BY 1
                HAVING
                    NOT (mz_now() <= date_trunc('day', ts) + CAST('7 days' AS interval)) AND
                    mz_now() <= date_trunc('year', date_trunc('day', ts) + CAST('1 year' AS interval));",
        )
        .await
        .unwrap();
    client
        .batch_execute(
            "CREATE VIEW dev_fy2023.stg_data_days AS
                SELECT *
                FROM dev.mock_data_days
                WHERE
                    CAST('2023-01-01' AS timestamp) <= ts AND
                    ts - CAST('1 month' AS interval) < CAST('2024-01-01' AS timestamp) AND
                    ts - CAST('0 month' AS interval) < CAST('2025-01-01' AS timestamp);",
        )
        .await
        .unwrap();
    client
        .batch_execute(
            "CREATE VIEW dev_fy2023.count_by_day AS
                SELECT
                    date_trunc('day', ts) AS \"day\",
                    count(*) AS cnt
                FROM dev_fy2023.stg_data_days
                GROUP BY 1
                HAVING
                    NOT (CAST('2024-01-01' AS timestamp) <= date_trunc('day', ts)) AND
                    CAST('2023-01-01' AS timestamp) <= date_trunc('day', ts);",
        )
        .await
        .unwrap();

    let timestamp = get_explain_timestamp(
        "(SELECT *, 'fy2023' AS origin FROM dev_fy2023.count_by_day
        UNION ALL
        SELECT *, 'warm' AS origin FROM dev_warm.count_by_day)",
        &client,
    )
    .await;
    assert!(
        timestamp < EpochMillis::MAX,
        "expected temporal query to have timestamps less than max timestamp of {}, instead found {}",
        EpochMillis::MAX,
        timestamp
    )
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_constant_materialized_view() {
    let server = test_util::TestHarness::default().start().await;
    let client = server.connect().await.unwrap();
    client
        .batch_execute("CREATE CLUSTER cold (SIZE 'scale=1,workers=1', REPLICATION FACTOR = 0);")
        .await
        .unwrap();
    // The materialized view is created in a cluster with 0 replicas, so it's upper will be stuck
    // at 0.
    client
        .batch_execute(
            "CREATE MATERIALIZED VIEW mv IN CLUSTER cold AS SELECT generate_series(1, 100);",
        )
        .await
        .unwrap();

    let timestamp_determination = get_explain_timestamp_determination("mv", &client)
        .await
        .unwrap();

    assert!(
        !timestamp_determination.respond_immediately,
        "upper is stuck at 0 so the query cannot respond immediately"
    );
    match timestamp_determination.determination.timestamp_context {
        TimestampContext::TimelineTimestamp { chosen_ts, .. } => {
            assert_ne!(Timestamp::MAX, chosen_ts)
        }
        TimestampContext::NoTimestamp => {
            panic!("queries against materialized views always require a timestamp")
        }
    }
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_explain_timestamp_blocking() {
    let server = test_util::TestHarness::default().start().await;
    server
        .enable_feature_flags(&[
            "enable_refresh_every_mvs",
            "enable_frontend_peek_sequencing",
        ])
        .await;
    let client = server.connect().await.unwrap();
    // This test will break in the year 30,000 after Jan 1st. When that happens, increase the year
    // to fix the test.
    client
        .batch_execute(
            "CREATE MATERIALIZED VIEW const_mv WITH (REFRESH AT '30000-01-01 23:59') AS SELECT 2;",
        )
        .await
        .unwrap();

    let mv_timestamp = get_explain_timestamp("const_mv", &client).await;

    let row = client
        .query_one("SELECT mz_now()::text;", &[])
        .await
        .unwrap();
    let mz_now_ts_raw: String = row.get(0);
    let mz_now_timestamp: EpochMillis = mz_now_ts_raw.parse().unwrap();

    assert!(
        mv_timestamp > mz_now_timestamp,
        "read against mv at timestamp {mv_timestamp} should be in the future compared to now, {mz_now_timestamp}"
    );
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_explain_timestamp_on_const_with_temporal() {
    // Regression test for https://github.com/MaterializeInc/database-issues/issues/7705
    let server = test_util::TestHarness::default().start().await;
    let client = server.connect().await.unwrap();

    // EXPLAIN TIMESTAMP a query that is a constant, but has a temporal filter.
    // Before fixing the bug, this was returning the timestamp u64:MAX.
    let timestamp = get_explain_timestamp(
        "(
            WITH dt_series AS (
                SELECT generate_series(
                    date_trunc('day', date '2024-03-02'),
                    date_trunc('day', date '2024-03-10'),
                    '1 day'::interval
                ) AS dt
            )
            SELECT dt FROM dt_series WHERE mz_now() <= dt + '7 days'::interval AND dt < mz_now())",
        &client,
    )
    .await;
    assert!(timestamp < 32503716000000); // year 3000
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_cancel_linearize_reads() {
    let server = test_util::TestHarness::default().start().await;
    server
        .enable_feature_flags(&["enable_refresh_every_mvs"])
        .await;
    let client = server.connect().await.unwrap();
    let client_cancel = client.cancel_token();

    // This test will break in the year 30,000 after Jan 1st. When that happens, increase the year
    // to fix the test.
    client
        .batch_execute(
            "CREATE MATERIALIZED VIEW const_mv WITH (REFRESH AT '30000-01-01 23:59') AS SELECT 2;",
        )
        .await
        .unwrap();

    let (tx, rx) = oneshot::channel();

    task::spawn(|| "read task", async move {
        // The since of const_mv will be in the year 30,000, requiring us to select a timestamp in
        // the year 30,000, requiring us to linearize the read waiting for the timestamp oracle to
        // catch up to the year 30,000.
        let res = client.query_one("SELECT * FROM const_mv;", &[]).await;
        let err = res.unwrap_err();
        assert_eq!(err.unwrap_db_error().code(), &SqlState::QUERY_CANCELED);
        tx.send(()).unwrap();
    });

    // There's a race for when we issue the cancel, so we need to retry. We want to issue the
    // cancel after the async SELECT.
    let (_, res) = Retry::default()
        .max_duration(Duration::from_secs(30))
        .retry_async_with_state(
            (client_cancel, rx),
            |_, (client_cancel, mut rx)| async move {
                client_cancel.cancel_query(postgres::NoTls).await.unwrap();
                let res = rx.try_recv();
                ((client_cancel, rx), res)
            },
        )
        .await;
    res.unwrap();
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_cancel_linearize_read_then_writes() {
    let server = test_util::TestHarness::default().start().await;
    server
        .enable_feature_flags(&["enable_refresh_every_mvs"])
        .await;
    let client = server.connect().await.unwrap();
    let client_cancel = client.cancel_token();

    client
        .batch_execute("CREATE TABLE t (a INT);")
        .await
        .unwrap();

    // This test will break in the year 30,000 after Jan 1st. When that happens, increase the year
    // to fix the test.
    client
        .batch_execute(
            "CREATE MATERIALIZED VIEW const_mv WITH (REFRESH AT '30000-01-01 23:59') AS SELECT 2;",
        )
        .await
        .unwrap();

    let handle = task::spawn(|| "read-then-write task", async move {
        // The since of const_mv will be in the year 30,000, requiring us to select a timestamp in
        // the year 30,000, requiring us to linearize the read waiting for the timestamp oracle to
        // catch up to the year 30,000.
        let res = client
            .execute("INSERT INTO t SELECT * FROM const_mv;", &[])
            .await;
        let err = res.unwrap_err();
        assert_eq!(err.unwrap_db_error().code(), &SqlState::QUERY_CANCELED);
    });

    // Sleep for a bit to try and get the `INSERT INTO SELECT` to run. We may get some false
    // positives, but we won't get false negatives.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !handle.is_finished(),
        "INSERT INTO SELECT should be linearized and wait for the year 30,000"
    );

    // There's a race for when we issue the cancel, so we need to retry. We want to issue the
    // cancel after the async SELECT.
    let ((_, handle), res) = Retry::default()
        .max_duration(Duration::from_secs(30))
        .retry_async_with_state(
            (client_cancel, handle),
            |_, (client_cancel, handle)| async move {
                client_cancel.cancel_query(postgres::NoTls).await.unwrap();
                let res = if handle.is_finished() {
                    Ok(())
                } else {
                    Err("thread unfinished".to_string())
                };
                ((client_cancel, handle), res)
            },
        )
        .await;
    res.unwrap();
    handle.await;
}

// Test that builtin objects are created in the schemas they advertise in builtin.rs.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_builtin_schemas() {
    let server = test_util::TestHarness::default().start().await;
    let client = server.connect().await.unwrap();

    let builtins_cfg = BuiltinsConfig {
        include_continual_tasks: true,
    };
    let mut builtins = BTreeMap::new();
    for builtin in BUILTINS::iter(&builtins_cfg) {
        builtins.insert(builtin.name(), builtin.schema());
    }

    let rows = client
        .query(
            "
            SELECT o.name name, s.name schema
            FROM mz_objects o
            JOIN mz_schemas s ON o.schema_id = s.id
            WHERE o.id LIKE 's%'",
            &[],
        )
        .await
        .unwrap();

    // Remove suffixes like _s1_primary_idx from some items.
    let name_re = Regex::new("_[su][0-9]+_.*").unwrap();

    for row in rows {
        let name: String = row.get("name");
        let name: String = name_re.replace(&name, "").into();
        let schema: String = row.get("schema");
        let builtin_schema = builtins.get(name.as_str());
        assert_eq!(
            builtin_schema,
            Some(&schema.as_str()),
            "wrong schema for {name}, builtin.rs has {builtin_schema:?}, database has {schema}"
        );
    }
}

// Test that DDLs are appear serial. This does not test that they are actually executed serially,
// only that they appear so. Do this by generating DDLs where the success of one will cause all
// others to fail.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_serialized_ddl_serial() {
    let server = test_util::TestHarness::default().start().await;
    let mut handles = Vec::new();
    let n = 10;
    for i in 0..n {
        let client = server.connect().await.unwrap();
        let handle = task::spawn(|| "test", async move {
            let stmt = format!("CREATE VIEW v AS SELECT {i}");
            let res = client
                .batch_execute(&stmt)
                .await
                .map_err(|err| err.to_string());
            res
        });
        handles.push(handle);
    }

    let mut successes = 0;
    let mut errors = 0;
    for handle in handles {
        let result = handle.await;
        match result {
            Ok(_) => {
                successes += 1;
            }
            Err(_) => {
                errors += 1;
            }
        }
    }
    // Exactly one must succeed.
    assert_eq!(successes, 1);
    assert_eq!(errors, n - 1);
}

// Test that serial DDLs are cancellable.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_serialized_ddl_cancel() {
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start()
        .await;
    server
        .enable_feature_flags(&["unsafe_enable_unsafe_functions"])
        .await;

    let client1 = server.connect().await.unwrap();
    let client2 = server.connect().await.unwrap();
    let cancel1 = client1.cancel_token();
    let cancel2 = client2.cancel_token();

    let handle1 = task::spawn(|| "test", async move {
        let err = client1
            .batch_execute("create view y as select mz_unsafe.mz_sleep(3)")
            .await
            .unwrap_err();
        err
    });
    let handle2 = task::spawn(|| "test", async move {
        // Encourage client1 to always execute first.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let err = client2
            .batch_execute("create view z as select mz_unsafe.mz_sleep(3)")
            .await
            .unwrap_err();
        err
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    // Cancel the pending statement (this uses different cancellation logic and is the actual thing
    // we are trying to test here).
    cancel2.cancel_query(tokio_postgres::NoTls).await.unwrap();
    let err = handle2.await;
    assert_contains!(err.to_string(), "canceling statement due to user request");
    // Cancel the in-progress statement.
    cancel1.cancel_query(tokio_postgres::NoTls).await.unwrap();
    let err = handle1.await;
    assert_contains!(err.to_string(), "canceling statement due to user request");

    // The mz_sleep calls above cause this test to not exit until the optimization tasks have fully
    // run, because spawn_blocking (used by optimization) are waited upon during Drop. Thus, don't
    // pass very high durations to mz_sleep so that we aren't waiting for long.
}
