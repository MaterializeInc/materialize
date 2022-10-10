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

use std::error::Error;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use std::{env, thread};

use anyhow::anyhow;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::{routing, Json, Router};
use chrono::{DateTime, Utc};
use http::StatusCode;
use postgres::Row;
use regex::Regex;
use serde_json::json;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot};
use tokio_postgres::config::Host;
use tokio_postgres::Client;
use tracing::info;

use mz_adapter::catalog::SYSTEM_USER;
use mz_ore::assert_contains;
use mz_ore::now::{EpochMillis, NowFn, NOW_ZERO, SYSTEM_TIME};
use mz_ore::retry::Retry;
use mz_ore::task::{self, AbortOnDropHandle, JoinHandleExt};

use crate::util::{MzTimestamp, PostgresErrorExt, KAFKA_ADDRS};

pub mod util;

/// An HTTP server whose responses can be controlled from another thread.
struct MockHttpServer {
    _task: AbortOnDropHandle<()>,
    addr: SocketAddr,
    conn_rx: mpsc::UnboundedReceiver<oneshot::Sender<Response>>,
}

impl MockHttpServer {
    /// Constructs a new mock HTTP server.
    fn new() -> MockHttpServer {
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
        let server = axum::Server::bind(&SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .serve(router.into_make_service());
        let addr = server.local_addr();
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

#[test]
fn test_no_block() -> Result<(), anyhow::Error> {
    mz_ore::test::init_logging();

    // This is better than relying on CI to time out, because an actual failure
    // (as opposed to a CI timeout) causes `services.log` to be uploaded.
    mz_ore::test::timeout(Duration::from_secs(30), || {
        info!("test_no_block: starting server");
        let server = util::start_server(util::Config::default())?;

        server.runtime.block_on(async {
            info!("test_no_block: starting mock HTTP server");
            let mut schema_registry_server = MockHttpServer::new();

            info!("test_no_block: connecting to server");
            let (client, _conn) = server.connect_async(postgres::NoTls).await?;

            let slow_task = task::spawn(|| "slow_client", async move {
                info!("test_no_block: in thread; executing create source");
                let result = client
                .batch_execute(&format!(
                    "CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL 'http://{}';",
                    schema_registry_server.addr,
                ))
                .await;
                info!("test_no_block: in thread; create CSR conn done");
                let _ = result?;

                let result = client
                    .batch_execute(&format!(
                        "CREATE CONNECTION kafka_conn FOR KAFKA BROKER '{}'",
                        &*KAFKA_ADDRS,
                    ))
                    .await;
                info!("test_no_block: in thread; create Kafka conn done");
                let _ = result?;

                let result = client
                    .batch_execute(
                        "CREATE SOURCE foo \
                        FROM KAFKA CONNECTION kafka_conn (TOPIC 'foo') \
                        FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn",
                    )
                    .await;
                info!("test_no_block: in thread; create source done");
                result
            });

            // Wait for Materialize to contact the schema registry, which
            // indicates the adapter is processing the CREATE SOURCE command. It
            // will be unable to complete the query until we respond.
            info!("test_no_block: accepting fake schema registry connection");
            let response_tx = schema_registry_server.accept().await;

            // Verify that the adapter can still process other requests from
            // other sessions.
            info!("test_no_block: connecting to server again");
            let (client, _conn) = server.connect_async(postgres::NoTls).await?;
            info!("test_no_block: executing query");
            let answer: i32 = client.query_one("SELECT 1 + 1", &[]).await?.get(0);
            assert_eq!(answer, 2);

            // Return an error to the adapter, so that we can shutdown cleanly.
            info!("test_no_block: writing fake schema registry error");
            response_tx
                .send(StatusCode::SERVICE_UNAVAILABLE.into_response())
                .expect("server unexpectedly closed channel");

            // Verify that the schema registry error was returned to the client, for
            // good measure.
            info!("test_no_block: joining task");
            let slow_res = slow_task.await.unwrap();
            assert_contains!(slow_res.unwrap_err().to_string(), "server error 503");

            Ok(())
        })
    })
}

/// Test that dropping a connection while a source is undergoing purification
/// does not crash the server.
#[test]
fn test_drop_connection_race() -> Result<(), anyhow::Error> {
    mz_ore::test::init_logging();

    info!("test_drop_connection_race: starting server");
    let server = util::start_server(util::Config::default().unsafe_mode())?;

    server.runtime.block_on(async {
        info!("test_drop_connection_race: starting mock HTTP server");
        let mut schema_registry_server = MockHttpServer::new();

        // Construct a source that depends on a schema registry connection.
        let (client, _conn) = server.connect_async(postgres::NoTls).await?;
        client
            .batch_execute(&format!(
                "CREATE CONNECTION conn FOR CONFLUENT SCHEMA REGISTRY URL 'http://{}'",
                schema_registry_server.addr,
            ))
            .await?;
        client
            .batch_execute(&format!(
                "CREATE CONNECTION kafka_conn FOR KAFKA BROKER '{}'",
                &*KAFKA_ADDRS,
            ))
            .await?;
        let source_task = task::spawn(|| "source_client", async move {
            info!("test_drop_connection_race: in task; creating connection and source");
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
        let (client, _conn) = server.connect_async(postgres::NoTls).await?;
        client.batch_execute("DROP CONNECTION conn").await?;

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
        let source_res = source_task.await.unwrap();
        assert_contains!(
            source_res.unwrap_err().to_string(),
            "unknown catalog item 'conn'"
        );

        Ok(())
    })
}

#[test]
fn test_time() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let server = util::start_server(util::Config::default())?;
    let mut client = server.connect(postgres::NoTls)?;

    // Confirm that `now()` and `current_timestamp()` both return a
    // DateTime<Utc>, but don't assert specific times.
    let row = client.query_one("SELECT now(), current_timestamp()", &[])?;
    let _ = row.get::<_, DateTime<Utc>>(0);
    let _ = row.get::<_, DateTime<Utc>>(1);

    // Confirm calls to now() return the same DateTime<Utc> both inside and
    // outside of subqueries.
    let row = client.query_one("SELECT now(), (SELECT now())", &[])?;
    assert_eq!(
        row.get::<_, DateTime<Utc>>(0),
        row.get::<_, DateTime<Utc>>(1)
    );

    // Ensure that EXPLAIN selects a timestamp for `now()` and
    // `current_timestamp()`, though we don't care what the timestamp is.
    let rows = client.query("EXPLAIN PLAN FOR SELECT now(), current_timestamp()", &[])?;
    assert_eq!(1, rows.len());

    // Test that `mz_sleep` causes a delay of at least the appropriate time.
    let start = Instant::now();
    client.batch_execute("SELECT mz_internal.mz_sleep(0.3)")?;
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(300),
        "start.elapsed() = {:?}",
        elapsed
    );

    Ok(())
}

#[test]
fn test_subscribe_consolidation() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let config = util::Config::default().workers(2);
    let server = util::start_server(config)?;
    let mut client_writes = server.connect(postgres::NoTls)?;
    let mut client_reads = server.connect(postgres::NoTls)?;

    client_writes.batch_execute("CREATE TABLE t (data text)")?;
    client_reads.batch_execute(
        "BEGIN;
         DECLARE c CURSOR FOR SUBSCRIBE t;",
    )?;

    let data = format!("line {}", 42);
    client_writes.execute(
        "INSERT INTO t VALUES ($1), ($2), ($3)",
        &[&data, &data, &data],
    )?;
    let row = client_reads.query_one("FETCH ALL c", &[])?;

    assert_eq!(row.get::<_, i64>("mz_diff"), 3);
    assert_eq!(row.get::<_, String>("data"), data);

    Ok(())
}

#[test]
fn test_subscribe_negative_diffs() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let config = util::Config::default().workers(2);
    let server = util::start_server(config)?;
    let mut client_writes = server.connect(postgres::NoTls)?;
    let mut client_reads = server.connect(postgres::NoTls)?;

    client_writes.batch_execute("CREATE TABLE t (data text)")?;
    client_writes.batch_execute(
        "CREATE MATERIALIZED VIEW counts AS SELECT data AS key, COUNT(data) AS count FROM t GROUP BY data",
    )?;
    client_reads.batch_execute(
        "BEGIN;
         DECLARE c CURSOR FOR SUBSCRIBE counts;",
    )?;

    let data = format!("line {}", 42);
    client_writes.execute("INSERT INTO t VALUES ($1)", &[&data])?;
    let row = client_reads.query_one("FETCH ALL c", &[])?;

    assert_eq!(row.get::<_, i64>("mz_diff"), 1);
    assert_eq!(row.get::<_, String>("key"), data);
    assert_eq!(row.get::<_, i64>("count"), 1);

    // send another row with the same key, this will retract the previous
    // count and emit an updated count

    let data = format!("line {}", 42);
    client_writes.execute("INSERT INTO t VALUES ($1)", &[&data])?;

    let rows = client_reads.query("FETCH ALL c", &[])?;
    let mut rows = rows.iter();

    let row = rows.next().expect("missing result");
    assert_eq!(row.get::<_, i64>("mz_diff"), -1);
    assert_eq!(row.get::<_, String>("key"), data);
    assert_eq!(row.get::<_, i64>("count"), 1);

    let row = rows.next().expect("missing result");
    assert_eq!(row.get::<_, i64>("mz_diff"), 1);
    assert_eq!(row.get::<_, String>("key"), data);
    assert_eq!(row.get::<_, i64>("count"), 2);

    Ok(())
}

#[test]
fn test_subscribe_basic() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    // Set the timestamp to zero for deterministic initial timestamps.
    let nowfn = Arc::new(Mutex::new(NOW_ZERO.clone()));
    let now = {
        let nowfn = Arc::clone(&nowfn);
        NowFn::from(move || (nowfn.lock().unwrap())())
    };
    let config = util::Config::default()
        .workers(2)
        .with_now(now)
        .unsafe_mode();
    let server = util::start_server(config)?;
    let mut client_writes = server.connect(postgres::NoTls)?;
    let mut client_reads = server.connect(postgres::NoTls)?;

    client_writes.batch_execute("CREATE TABLE t (data text)")?;
    client_writes.batch_execute(
        "CREATE DEFAULT INDEX t_primary_idx ON t WITH (LOGICAL COMPACTION WINDOW 0)",
    )?;
    // Now that the index (and its since) are initialized to 0, we can resume using
    // system time. Do a read to bump the oracle's state so it will read from the
    // system clock during inserts below.
    *nowfn.lock().unwrap() = SYSTEM_TIME.clone();
    client_writes.batch_execute("SELECT * FROM t")?;
    client_reads.batch_execute(
        "BEGIN;
         DECLARE c CURSOR FOR SUBSCRIBE t;",
    )?;
    // Locks the timestamp of the SUBSCRIBE to before any of the following INSERTs, which is required
    // for mz_timestamp column to be accurate
    let _ = client_reads.query_one("FETCH 0 c", &[]);

    let mut events = vec![];

    for i in 1..=3 {
        let data = format!("line {}", i);
        client_writes.execute("INSERT INTO t VALUES ($1)", &[&data])?;
        let row = client_reads.query_one("FETCH ALL c", &[])?;
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
        client_reads.batch_execute(&*format!(
            "COMMIT; BEGIN;
            DECLARE c CURSOR FOR SUBSCRIBE t WITH (SNAPSHOT = false) AS OF {}",
            ts - 1
        ))?;

        // Skip by the things we won't be able to see.
        for (_, expected) in events.iter().skip_while(|(inner_ts, _)| inner_ts < ts) {
            let actual = client_reads.query_one("FETCH c", &[])?;
            assert_eq!(actual.get::<_, String>("data"), *expected);
        }
    }

    // Now subscribe with a snapshot as of each timestamp. We should see a batch of
    // updates all at the subscribed timestamp, and then updates afterward.
    for (ts, _) in &events {
        client_reads.batch_execute(&*format!(
            "COMMIT; BEGIN;
            DECLARE c CURSOR FOR SUBSCRIBE t AS OF {}",
            ts - 1
        ))?;

        for (mut expected_ts, expected_data) in events.iter() {
            if expected_ts < ts - 1 {
                // If the thing we initially got was before the timestamp, it should have gotten
                // fast-forwarded up to the timestamp.
                expected_ts = ts - 1;
            }

            let actual = client_reads.query_one("FETCH c", &[])?;
            assert_eq!(actual.get::<_, String>("data"), *expected_data);
            assert_eq!(actual.get::<_, MzTimestamp>("mz_timestamp").0, expected_ts);
        }
    }

    // Aggressively compact the data in the index, then subscribe an unmaterialized
    // view derived from the index. This previously selected an invalid
    // `AS OF` timestamp (#5391).
    client_writes
        .batch_execute("ALTER INDEX t_primary_idx SET (LOGICAL COMPACTION WINDOW = '1ms')")?;
    client_writes.batch_execute("CREATE VIEW v AS SELECT * FROM t")?;
    client_reads.batch_execute(
        "COMMIT; BEGIN;
         DECLARE c CURSOR FOR SUBSCRIBE v;",
    )?;
    let rows = client_reads.query("FETCH ALL c", &[])?;
    assert_eq!(rows.len(), 3);
    for i in 0..3 {
        assert_eq!(rows[i].get::<_, i64>("mz_diff"), 1);
        assert_eq!(rows[i].get::<_, String>("data"), format!("line {}", i + 1));
    }

    // Wait until compaction kicks in and we get an error on trying to read from the cursor.
    let err = loop {
        client_reads.batch_execute("COMMIT; BEGIN; DECLARE c CURSOR FOR SUBSCRIBE v AS OF 1")?;

        if let Err(err) = client_reads.query("FETCH ALL c", &[]) {
            break err;
        }
    };

    assert!(err
        .unwrap_db_error()
        .message()
        .starts_with("Timestamp (1) is not valid for all inputs"));

    Ok(())
}

/// Test the done messages by sending inserting a single row and waiting to
/// observe it. Since SUBSCRIBE always sends a progressed message at the end of its
/// batches and we won't yet insert a second row, we know that if we've seen a
/// data row we will also see one progressed message.
#[test]
fn test_subscribe_progress() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let config = util::Config::default().workers(2);
    let server = util::start_server(config)?;
    let mut client_writes = server.connect(postgres::NoTls)?;
    let mut client_reads = server.connect(postgres::NoTls)?;

    client_writes.batch_execute("CREATE TABLE t1 (data text)")?;
    client_reads.batch_execute(
        "COMMIT; BEGIN;
         DECLARE c1 CURSOR FOR SUBSCRIBE t1 WITH (PROGRESS);",
    )?;

    #[derive(PartialEq)]
    enum State {
        WaitingForData,
        WaitingForProgress(MzTimestamp),
        Done,
    }

    for i in 1..=3 {
        let data = format!("line {}", i);
        client_writes.execute("INSERT INTO t1 VALUES ($1)", &[&data])?;

        // We have to try several times. It might be that the FETCH gets
        // a batch that only contains continuous progress statements, without
        // any data. We retry until we get the batch that has the data, and
        // then verify that it also has a progress statement.
        let mut state = State::WaitingForData;
        while state != State::Done {
            let rows = client_reads.query("FETCH ALL c1", &[])?;

            let rows = rows.iter();

            // find the data row in the sea of progress rows

            // remove progress statements that occurred before our data
            let skip_progress = state == State::WaitingForData;
            let mut rows = rows
                .skip_while(move |row| skip_progress && row.try_get::<_, String>("data").is_err());

            if state == State::WaitingForData {
                // this must be the data row
                let data_row = rows.next();

                let data_row = match data_row {
                    Some(data_row) => data_row,
                    None => continue, //retry
                };

                assert_eq!(data_row.get::<_, bool>("mz_progressed"), false);
                assert_eq!(data_row.get::<_, i64>("mz_diff"), 1);
                assert_eq!(data_row.get::<_, String>("data"), data);
                let data_ts: MzTimestamp = data_row.get("mz_timestamp");
                state = State::WaitingForProgress(data_ts);
            }
            if let State::WaitingForProgress(data_ts) = &state {
                let mut num_progress_rows = 0;
                for progress_row in rows {
                    assert_eq!(progress_row.get::<_, bool>("mz_progressed"), true);
                    assert_eq!(progress_row.get::<_, Option<i64>>("mz_diff"), None);
                    assert_eq!(progress_row.get::<_, Option<String>>("data"), None);

                    let progress_ts: MzTimestamp = progress_row.get("mz_timestamp");
                    assert!(data_ts < &progress_ts);

                    num_progress_rows += 1;
                }
                if num_progress_rows > 0 {
                    state = State::Done;
                }
            }
        }
    }

    Ok(())
}

// Verifies that subscribing to non-nullable columns with progress information
// turns them into nullable columns. See #6304.
#[test]
fn test_subscribe_progress_non_nullable_columns() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let config = util::Config::default().workers(2);
    let server = util::start_server(config)?;
    let mut client_writes = server.connect(postgres::NoTls)?;
    let mut client_reads = server.connect(postgres::NoTls)?;

    client_writes.batch_execute("CREATE TABLE t2 (data text NOT NULL)")?;
    client_writes.batch_execute("INSERT INTO t2 VALUES ('data')")?;
    client_reads.batch_execute(
        "COMMIT; BEGIN;
            DECLARE c2 CURSOR FOR SUBSCRIBE t2 WITH (PROGRESS);",
    )?;

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
        let row = client_reads.query_one("FETCH 1 c2", &[])?;

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

    Ok(())
}

/// Verifies that we get continuous progress messages, regardless of if we
/// receive data or not.
#[test]
fn test_subcribe_continuous_progress() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let config = util::Config::default().workers(2);
    let server = util::start_server(config)?;
    let mut client_writes = server.connect(postgres::NoTls)?;
    let mut client_reads = server.connect(postgres::NoTls)?;

    client_writes.batch_execute("CREATE TABLE t1 (data text)")?;
    client_reads.batch_execute(
        "COMMIT; BEGIN;
         DECLARE c1 CURSOR FOR SUBSCRIBE t1 WITH (PROGRESS);",
    )?;

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
        let rows = client_reads.query("FETCH ALL c1", &[])?;
        let (num_data_rows, num_progress_rows) = verify_rows(rows);
        assert_eq!(num_data_rows, 0);
        if num_progress_rows > 0 {
            break;
        }
    }

    client_writes.execute("INSERT INTO t1 VALUES ($1)", &[&"hello".to_owned()])?;

    // fetch away the data message, plus maybe some progress messages
    let mut num_data_rows = 0;
    let mut num_progress_rows = 0;

    while num_data_rows == 0 || num_progress_rows == 0 {
        let rows = client_reads.query("FETCH ALL c1", &[])?;
        let (current_num_data_rows, current_num_progress_rows) = verify_rows(rows);
        num_data_rows += current_num_data_rows;
        num_progress_rows += current_num_progress_rows;
    }

    // Try and read some progress messages. The normal update interval is
    // 1s, so only wait for two updates. Otherwise this would run for too long.
    for _i in 1..=2 {
        let rows = client_reads.query("FETCH ALL c1", &[])?;

        let (num_data_rows, num_progress_rows) = verify_rows(rows);
        assert_eq!(num_data_rows, 0);
        assert!(num_progress_rows > 0);
    }

    Ok(())
}

#[test]
fn test_subscribe_fetch_timeout() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let config = util::Config::default().workers(2);
    let server = util::start_server(config)?;
    let mut client = server.connect(postgres::NoTls)?;

    client.batch_execute("CREATE TABLE t (i INT8)")?;
    client.batch_execute("INSERT INTO t VALUES (1), (2), (3);")?;
    client.batch_execute(
        "BEGIN;
         DECLARE c CURSOR FOR SUBSCRIBE t;",
    )?;

    let expected: Vec<i64> = vec![1, 2, 3];
    let mut expected_iter = expected.iter();
    let mut next = expected_iter.next();

    // Test 0s timeouts.
    while let Some(expect) = next {
        let rows = client.query("FETCH c WITH (TIMEOUT = '0s')", &[])?;
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
    let rows = client.query("FETCH c WITH (TIMEOUT = '1s')", &[])?;
    let duration = before.elapsed();
    assert_eq!(rows.len(), 0);
    // Make sure we waited at least 1s but also not too long.
    assert!(duration >= Duration::from_secs(1));
    assert!(duration < Duration::from_secs(10));

    // Make a new cursor. Try to fetch more rows from it than exist. Verify that
    // we got all the rows we expect and also waited for at least the timeout
    // duration. Cursor may take a moment to be ready, so do it in a loop.
    client.batch_execute(
        "COMMIT; BEGIN;
        DECLARE c CURSOR FOR SUBSCRIBE t",
    )?;
    loop {
        let before = Instant::now();
        let rows = client.query("FETCH 4 c WITH (TIMEOUT = '1s')", &[])?;
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
    let rows = client.query("FETCH c WITH (TIMEOUT = '0s')", &[])?;
    assert_eq!(rows.len(), 0);

    // Make a third cursor. Fetch should return immediately if there are enough
    // rows, even with a really long timeout.
    //
    // Regression test for #6307
    client.batch_execute(
        "COMMIT; BEGIN;
        DECLARE c CURSOR FOR SUBSCRIBE t",
    )?;
    let before = Instant::now();
    // NB: This timeout is chosen such that the test will timeout if the bad
    // behavior occurs.
    let rows = client.query("FETCH 3 c WITH (TIMEOUT = '1h')", &[])?;
    let duration = before.elapsed();
    assert_eq!(rows.len(), expected.len());
    assert!(duration < Duration::from_secs(10));
    for i in 0..expected.len() {
        assert_eq!(rows[i].get::<_, i64>(2), expected[i])
    }

    Ok(())
}

#[test]
fn test_subscribe_fetch_wait() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let config = util::Config::default().workers(2);
    let server = util::start_server(config)?;
    let mut client = server.connect(postgres::NoTls)?;

    client.batch_execute("CREATE TABLE t (i INT8)")?;
    client.batch_execute("INSERT INTO t VALUES (1), (2), (3)")?;
    client.batch_execute(
        "BEGIN;
         DECLARE c CURSOR FOR SUBSCRIBE t;",
    )?;

    let expected: Vec<i64> = vec![1, 2, 3];
    let mut expected_iter = expected.iter();
    let mut next = expected_iter.next();

    while let Some(expect) = next {
        // FETCH with no timeout will wait for at least 1 result.
        let rows = client.query("FETCH c", &[])?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, i64>(2), *expect);
        next = expected_iter.next();
    }

    // Try again with FETCH ALL. ALL only guarantees that all available rows will
    // be returned, but it's up to the system to decide what is available. This
    // means that we could still get only one row per request, and we won't know
    // how many rows will come back otherwise.
    client.batch_execute(
        "COMMIT; BEGIN;
        DECLARE c CURSOR FOR SUBSCRIBE t;",
    )?;
    let mut expected_iter = expected.iter().peekable();
    while expected_iter.peek().is_some() {
        let rows = client.query("FETCH ALL c", &[])?;
        assert!(rows.len() > 0);
        for row in rows {
            let next = expected_iter.next().unwrap();
            assert_eq!(*next, row.get::<_, i64>(2));
        }
    }

    // Verify that the wait only happens for SUBSCRIBE. A SELECT with 0 rows should not
    // block.
    client.batch_execute("COMMIT")?;
    client.batch_execute("CREATE TABLE empty ()")?;
    client.batch_execute(
        "BEGIN;
         DECLARE c CURSOR FOR SELECT * FROM empty;",
    )?;
    let rows = client.query("FETCH c", &[])?;
    assert_eq!(rows.len(), 0);

    Ok(())
}

#[test]
fn test_subscribe_empty_upper_frontier() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let config = util::Config::default();
    let server = util::start_server(config)?;
    let mut client = server.connect(postgres::NoTls)?;

    client.batch_execute("CREATE MATERIALIZED VIEW foo AS VALUES (1), (2), (3);")?;

    let subscribe = client.query("SUBSCRIBE foo WITH (SNAPSHOT = false)", &[])?;
    assert_eq!(0, subscribe.len());

    let subscribe = client.query("SUBSCRIBE foo WITH (SNAPSHOT)", &[])?;
    assert_eq!(3, subscribe.len());

    Ok(())
}

// Tests that a client that launches a non-terminating SUBSCRIBE and disconnects
// does not keep the server alive forever.
#[test]
fn test_subscribe_shutdown() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let server = util::start_server(util::Config::default())?;

    // We have to use the async PostgreSQL client so that we can ungracefully
    // abort the connection task.
    // See: https://github.com/sfackler/rust-postgres/issues/725
    server.runtime.block_on(async {
        let (client, conn_task) = server.connect_async(tokio_postgres::NoTls).await?;

        // Create a table with no data that we can SUBSCRIBE. This is the simplest
        // way to cause a SUBSCRIBE to never terminate.
        client.batch_execute("CREATE TABLE t ()").await?;

        // Launch the ill-fated subscribe.
        client.copy_out("COPY (SUBSCRIBE t) TO STDOUT").await?;

        // Un-gracefully abort the connection.
        conn_task.abort();

        // Need to await `conn_task` to actually deliver the `abort`. We don't
        // care about the result though (it's probably `JoinError` with `is_cancelled` being true).
        let _ = conn_task.await;

        Ok::<_, Box<dyn Error>>(())
    })?;

    // Dropping the server will initiate a graceful shutdown. We previously had
    // a bug where the server would fail to notice that the client running
    // `SUBSCRIBE v` had disconnected, and would hang forever waiting for data
    // to be written to `path`, which in this test never comes. So if this
    // function exits, things are working correctly.

    Ok(())
}

#[test]
fn test_subscribe_table_rw_timestamps() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let config = util::Config::default().workers(3);
    let server = util::start_server(config)?;
    let mut client_interactive = server.connect(postgres::NoTls)?;
    let mut client_subscribe = server.connect(postgres::NoTls)?;

    client_interactive.batch_execute("CREATE TABLE t1 (data text)")?;

    client_subscribe.batch_execute(
        "COMMIT; BEGIN;
         DECLARE c1 CURSOR FOR SUBSCRIBE t1 WITH (PROGRESS);",
    )?;

    client_interactive.execute("BEGIN", &[]).unwrap();
    client_interactive.execute("INSERT INTO t1 VALUES ($1)", &[&"first".to_owned()])?;
    client_interactive.execute("INSERT INTO t1 VALUES ($1)", &[&"first".to_owned()])?;
    client_interactive.execute("COMMIT", &[])?;
    let _ = client_interactive.query("SELECT * FROM T1", &[])?;
    client_interactive.execute("BEGIN", &[])?;
    client_interactive.execute("INSERT INTO t1 VALUES ($1)", &[&"second".to_owned()])?;
    client_interactive.execute("INSERT INTO t1 VALUES ($1)", &[&"second".to_owned()])?;
    client_interactive.execute("COMMIT", &[])?;
    let _ = client_interactive.query("SELECT * FROM T1", &[])?;

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
        let rows = client_subscribe.query("FETCH ALL c1", &[])?;
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

    client_subscribe.batch_execute("COMMIT;")?;

    assert!(seen_first);
    assert!(seen_second);

    let first_write_ts = first_write_ts.unwrap();
    let second_write_ts = second_write_ts.unwrap();
    assert!(first_write_ts <= second_write_ts);

    Ok(())
}

// Tests that temporary views created by one connection cannot be viewed
// by another connection.
#[test]
fn test_temporary_views() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let server = util::start_server(util::Config::default())?;
    let mut client_a = server.connect(postgres::NoTls)?;
    let mut client_b = server.connect(postgres::NoTls)?;
    client_a
        .batch_execute("CREATE VIEW v AS VALUES (1, 'foo'), (2, 'bar'), (3, 'foo'), (1, 'bar')")?;
    client_a.batch_execute("CREATE TEMPORARY VIEW temp_v AS SELECT * FROM v")?;

    let query_v = "SELECT count(*) FROM v;";
    let query_temp_v = "SELECT count(*) FROM temp_v;";

    // Ensure that client_a can query v and temp_v.
    let count: i64 = client_b.query_one(query_v, &[])?.get("count");
    assert_eq!(4, count);
    let count: i64 = client_a.query_one(query_temp_v, &[])?.get("count");
    assert_eq!(4, count);

    // Ensure that client_b can query v, but not temp_v.
    let count: i64 = client_b.query_one(query_v, &[])?.get("count");
    assert_eq!(4, count);

    let err = client_b.query_one(query_temp_v, &[]).unwrap_db_error();
    assert_eq!(err.message(), "unknown catalog item \'temp_v\'");

    Ok(())
}

// Test EXPLAIN TIMESTAMP with tables. Mock time to verify initial table since
// is now(), not 0.
#[test]
fn test_explain_timestamp_table() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();
    let timestamp = Arc::new(Mutex::new(1_000));
    let now = {
        let timestamp = Arc::clone(&timestamp);
        NowFn::from(move || *timestamp.lock().unwrap())
    };
    let config = util::Config::default().with_now(now);
    let server = util::start_server(config)?;
    let mut client = server.connect(postgres::NoTls)?;
    let timestamp_re = Regex::new(r"\s*(\d{4}|0) \(\d+-\d\d-\d\d \d\d:\d\d:\d\d.\d\d\d\)").unwrap();
    let bool_re = Regex::new(r"true|false").unwrap();

    client.batch_execute("CREATE TABLE t1 (i1 INT)")?;

    let expect = "          query timestamp:<TIMESTAMP>
                    since:[<TIMESTAMP>]
                    upper:[<TIMESTAMP>]
         global timestamp:<TIMESTAMP>
  can respond immediately: <BOOL>

source materialize.public.t1 (u1, storage):
            read frontier:[<TIMESTAMP>]
           write frontier:[<TIMESTAMP>]\n";

    let row = client
        .query_one("EXPLAIN TIMESTAMP FOR SELECT * FROM t1;", &[])
        .unwrap();
    let explain: String = row.get(0);
    let explain = timestamp_re.replace_all(&explain, "<TIMESTAMP>");
    let explain = bool_re.replace_all(&explain, "<BOOL>");
    assert_eq!(explain, expect);

    Ok(())
}

// Test that a query that causes a compute instance to panic will resolve
// the panic and allow the compute instance to restart (instead of crash loop
// forever) when a client is terminated (disconnects from the server) instead
// of cancelled (sends a pgwire cancel request on a new connection).
#[test]
fn test_github_12546() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();
    let config = util::Config::default();
    let server = util::start_server(config)?;

    server.runtime.block_on(async {
        let (client, conn_task) = server.connect_async(tokio_postgres::NoTls).await?;

        client.batch_execute("CREATE TABLE test(a text);").await?;
        client
            .batch_execute("INSERT INTO test VALUES ('a');")
            .await?;

        let query = client.query("SELECT mz_internal.mz_panic(a) FROM test", &[]);
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
        conn_task.abort();

        // Need to await `conn_task` to actually deliver the `abort`.
        let _ = conn_task.await;

        // Make a new connection to verify the compute instance can now start.
        let (client, _conn_task) = server.connect_async(tokio_postgres::NoTls).await?;
        assert_eq!(
            client
                .query_one("SELECT count(*) FROM test", &[])
                .await?
                .get::<_, i64>(0),
            1,
        );

        Ok::<_, Box<dyn Error>>(())
    })?;

    Ok(())
}

#[test]
fn test_github_12951() {
    mz_ore::test::init_logging();
    let config = util::Config::default();
    let server = util::start_server(config).unwrap();

    // Verify sinks (SUBSCRIBE) are correctly handled for a dropped cluster.
    {
        let mut client1 = server.connect(postgres::NoTls).unwrap();
        let mut client2 = server.connect(postgres::NoTls).unwrap();
        let client2_cancel = client2.cancel_token();

        client1
            .batch_execute("CREATE CLUSTER foo REPLICAS (r1 (size '1'))")
            .unwrap();
        client1.batch_execute("CREATE TABLE t1(f1 int)").unwrap();
        client2.batch_execute("SET CLUSTER = foo").unwrap();
        client2
            .batch_execute(
                "BEGIN; DECLARE c CURSOR FOR SUBSCRIBE (SELECT count(*) FROM t1); FETCH 1 c",
            )
            .unwrap();
        client1.batch_execute("DROP CLUSTER foo CASCADE").unwrap();
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

    // Verify read holds (transactions) are correctly handled for a dropped cluster.
    {
        let mut client1 = server.connect(postgres::NoTls).unwrap();
        let mut client2 = server.connect(postgres::NoTls).unwrap();

        client1
            .batch_execute("CREATE CLUSTER foo REPLICAS (r1 (size '1'))")
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

#[test]
// Tests github issue #13100
fn test_subscribe_outlive_cluster() {
    mz_ore::test::init_logging();
    let config = util::Config::default();
    let server = util::start_server(config).unwrap();

    // Verify sinks (SUBSCRIBE) are correctly handled for a dropped cluster, when a new cluster is created.
    let mut client1 = server.connect(postgres::NoTls).unwrap();
    let mut client2 = server.connect(postgres::NoTls).unwrap();
    let client2_cancel = client2.cancel_token();

    client1
        .batch_execute("CREATE CLUSTER foo REPLICAS (r1 (size '1'))")
        .unwrap();
    client1.batch_execute("CREATE TABLE t1(f1 int)").unwrap();
    client2.batch_execute("SET CLUSTER = foo").unwrap();
    client2
        .batch_execute("BEGIN; DECLARE c CURSOR FOR SUBSCRIBE (SELECT count(*) FROM t1); FETCH 1 c")
        .unwrap();
    client1.batch_execute("DROP CLUSTER foo CASCADE").unwrap();
    client1
        .batch_execute("CREATE CLUSTER newcluster REPLICAS (r1 (size '1'))")
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

#[test]
fn test_read_then_write_serializability() {
    mz_ore::test::init_logging();
    let config = util::Config::default();
    let server = util::start_server(config).unwrap();

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

#[test]
fn test_timestamp_recovery() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();
    let now = Arc::new(Mutex::new(1_000_000_000));
    let now_fn = {
        let timestamp = Arc::clone(&now);
        NowFn::from(move || *timestamp.lock().unwrap())
    };
    let data_dir = tempfile::tempdir()?;
    let config = util::Config::default()
        .with_now(now_fn)
        .data_directory(data_dir.path());

    // Start a server and insert some data to establish the current global timestamp
    let global_timestamp = {
        let server = util::start_server(config.clone())?;
        let mut client = server.connect(postgres::NoTls)?;

        client.batch_execute("CREATE TABLE t1 (i1 INT)")?;
        insert_with_deterministic_timestamps("t1", "(42)", &server, Arc::clone(&now))?;
        get_explain_timestamp("t1", &mut client)
    };

    // Rollback the current time and ensure that a value larger than the old global timestamp is
    // recovered
    {
        *now.lock().expect("lock poisoned") = 0;
        let server = util::start_server(config)?;
        let mut client = server.connect(postgres::NoTls)?;
        let recovered_timestamp = get_explain_timestamp("t1", &mut client);
        assert!(recovered_timestamp > global_timestamp);
    }

    Ok(())
}

#[test]
fn test_timeline_read_holds() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();
    // Set the timestamp to zero for deterministic initial timestamps.
    let now = Arc::new(Mutex::new(0));
    let now_fn = {
        let now = Arc::clone(&now);
        NowFn::from(move || *now.lock().unwrap())
    };
    let config = util::Config::default().with_now(now_fn).unsafe_mode();
    let server = util::start_server(config).unwrap();
    let mut mz_client = server.connect(postgres::NoTls)?;

    let view_name = "v_hold";
    let source_name = "source_hold";
    let (mut pg_client, cleanup_fn) = create_postgres_source_with_table(
        &server.runtime,
        &mut mz_client,
        view_name,
        "(a INT)",
        source_name,
    )?;

    // Create user table in Materialize.
    mz_client.batch_execute("DROP TABLE IF EXISTS t;")?;
    mz_client.batch_execute("CREATE TABLE t (a INT);")?;
    insert_with_deterministic_timestamps("t", "(42)", &server, Arc::clone(&now))?;

    // Insert data into source.
    let source_rows: i64 = 10;
    for _ in 0..source_rows {
        let _ = server
            .runtime
            .block_on(pg_client.execute(&format!("INSERT INTO {view_name} VALUES (42);"), &[]))?;
    }

    wait_for_view_population(&mut mz_client, view_name, source_rows)?;

    // Make sure that the table and view are joinable immediately at some timestamp.
    let mut mz_join_client = server.connect(postgres::NoTls)?;
    let _ = mz_ore::test::timeout(Duration::from_millis(1_000), move || {
        Ok(mz_join_client
            .query_one(&format!("SELECT COUNT(t.a) FROM t, {view_name};"), &[])?
            .get::<_, i64>(0))
    })?;

    cleanup_fn(&mut mz_client, &mut pg_client, &server.runtime)?;

    Ok(())
}

#[test]
fn test_linearizability() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();
    // Set the timestamp to zero for deterministic initial timestamps.
    let now = Arc::new(Mutex::new(0));
    let now_fn = {
        let now = Arc::clone(&now);
        NowFn::from(move || *now.lock().unwrap())
    };
    let config = util::Config::default().with_now(now_fn).unsafe_mode();
    let server = util::start_server(config)?;
    let mut mz_client = server.connect(postgres::NoTls)?;

    let view_name = "v_lin";
    let source_name = "source_lin";
    let (mut pg_client, cleanup_fn) = create_postgres_source_with_table(
        &server.runtime,
        &mut mz_client,
        view_name,
        "(a INT)",
        source_name,
    )?;
    // Insert value into postgres table.
    let _ = server
        .runtime
        .block_on(pg_client.execute(&format!("INSERT INTO {view_name} VALUES (42);"), &[]))?;

    wait_for_view_population(&mut mz_client, view_name, 1)?;

    // The user table's write frontier will be close to zero because we use a deterministic
    // now function in this test. It may be slightly higher than zero because bootstrapping
    // and background tasks push the global timestamp forward.
    // The materialized view's write frontier will be close to the system time because it uses
    // the system clock to close timestamps.
    // Therefore queries that only involve the view will normally happen at a higher timestamp
    // than queries that involve the user table. However, we prevent this when in strict
    // serializable mode.

    mz_client.batch_execute("SET transaction_isolation = serializable")?;
    let view_ts = get_explain_timestamp(view_name, &mut mz_client);
    // Create user table in Materialize.
    mz_client.batch_execute("DROP TABLE IF EXISTS t;")?;
    mz_client.batch_execute("CREATE TABLE t (a INT);")?;
    let join_ts = get_explain_timestamp(&format!("{view_name}, t"), &mut mz_client);
    // In serializable transaction isolation, read timestamps can go backwards.
    assert!(join_ts < view_ts);

    mz_client.batch_execute("SET transaction_isolation = 'strict serializable'")?;
    let view_ts = get_explain_timestamp(view_name, &mut mz_client);
    let join_ts = get_explain_timestamp(&format!("{view_name}, t"), &mut mz_client);
    // Since the query on the join was done after the query on the view, it should have a higher or
    // equal timestamp in strict serializable mode.
    assert!(join_ts >= view_ts);

    cleanup_fn(&mut mz_client, &mut pg_client, &server.runtime)?;

    Ok(())
}

#[test]
fn test_system_user() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let config = util::Config::default();
    let server = util::start_server(config)?;

    assert!(server
        .pg_config()
        .user(&SYSTEM_USER.name)
        .connect(postgres::NoTls)
        .is_err());
    assert!(server
        .pg_config_internal()
        .user(&SYSTEM_USER.name)
        .connect(postgres::NoTls)
        .is_ok());
    assert!(server
        .pg_config_internal()
        .user("mz_something_else")
        .connect(postgres::NoTls)
        .is_err());

    Ok(())
}

// Tests that you can have simultaneous connections on the internal and external ports without
// crashing
#[test]
fn test_internal_ports() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let config = util::Config::default();
    let server = util::start_server(config)?;

    {
        let mut external_client = server.connect(postgres::NoTls)?;
        let mut internal_client = server
            .pg_config_internal()
            .user(&SYSTEM_USER.name)
            .connect(postgres::NoTls)?;

        assert_eq!(
            1,
            external_client
                .query_one("SELECT 1;", &[])?
                .get::<_, i32>(0)
        );
        assert_eq!(
            1,
            internal_client
                .query_one("SELECT 1;", &[])?
                .get::<_, i32>(0)
        );
    }

    {
        let mut external_client = server.connect(postgres::NoTls)?;
        let mut internal_client = server
            .pg_config_internal()
            .user(&SYSTEM_USER.name)
            .connect(postgres::NoTls)?;

        assert_eq!(
            1,
            external_client
                .query_one("SELECT 1;", &[])?
                .get::<_, i32>(0)
        );
        assert_eq!(
            1,
            internal_client
                .query_one("SELECT 1;", &[])?
                .get::<_, i32>(0)
        );
    }

    Ok(())
}

// Test that trying to alter an invalid system param returns an error.
// This really belongs in the resource-limits.td testdrive, but testdrive
// doesn't allow you to specify a connection and expect a failure which is
// needed for this test.
#[test]
fn test_alter_system_invalid_param() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let config = util::Config::default();
    let server = util::start_server(config)?;

    let mut mz_client = server
        .pg_config_internal()
        .user(&SYSTEM_USER.name)
        .connect(postgres::NoTls)?;

    mz_client.batch_execute("ALTER SYSTEM SET max_tables TO 2")?;
    let res = mz_client
        .batch_execute("ALTER SYSTEM SET invalid_param TO 42")
        .unwrap_err();
    assert!(res
        .to_string()
        .contains("unrecognized configuration parameter \"invalid_param\""));
    let res = mz_client
        .batch_execute("ALTER SYSTEM RESET invalid_param")
        .unwrap_err();
    assert!(res
        .to_string()
        .contains("unrecognized configuration parameter \"invalid_param\""));

    Ok(())
}

#[test]
fn test_concurrent_writes() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let config = util::Config::default();
    let server = util::start_server(config)?;

    let num_tables = 10;

    {
        let mut client = server.connect(postgres::NoTls)?;
        for i in 0..num_tables {
            client.batch_execute(&format!("CREATE TABLE t_{i} (a INT, b text, c text)"))?;
        }
    }

    let num_threads = 3;
    let num_loops = 10;

    let mut clients = Vec::new();
    for _ in 0..num_threads {
        clients.push(server.connect(postgres::NoTls)?);
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

    let mut client = server.connect(postgres::NoTls)?;

    for i in 0..num_tables {
        let count = client
            .query_one(&format!("SELECT count(*) FROM t_{i}"), &[])?
            .get::<_, i64>(0);
        assert_eq!(num_loops * num_threads, count);
    }

    Ok(())
}

/// Group commit will block writes until the current time has advanced. This can make
/// performing inserts while using deterministic time difficult. This is a helper
/// method to perform writes and advance the current time.
fn insert_with_deterministic_timestamps(
    table: &'static str,
    values: &'static str,
    server: &util::Server,
    now: Arc<Mutex<EpochMillis>>,
) -> Result<(), Box<dyn Error>> {
    let mut client_write = server.connect(postgres::NoTls)?;
    let mut client_read = server.connect(postgres::NoTls)?;

    let mut current_ts = get_explain_timestamp(table, &mut client_read);
    let write_thread = thread::spawn(move || {
        client_write
            .execute(&format!("INSERT INTO {table} VALUES {values}"), &[])
            .unwrap();
    });
    while !write_thread.is_finished() {
        // Keep increasing `now` until the write has executed succeed. Table advancements may
        // have increased the global timestamp by an unknown amount.
        current_ts += 1;
        *now.lock().expect("lock poisoned") = current_ts;
        thread::sleep(Duration::from_millis(1));
    }
    write_thread.join().unwrap();
    Ok(())
}

fn get_explain_timestamp(table: &str, client: &mut postgres::Client) -> EpochMillis {
    let row = client
        .query_one(&format!("EXPLAIN TIMESTAMP FOR SELECT * FROM {table}"), &[])
        .unwrap();
    let explain: String = row.get(0);
    let timestamp_re =
        Regex::new(r"^\s+query timestamp:\s+(\d+) \(\d+-\d\d-\d\d \d\d:\d\d:\d\d\.\d\d\d\)\n")
            .unwrap();
    let timestamp_caps = timestamp_re.captures(&explain).unwrap();
    timestamp_caps.get(1).unwrap().as_str().parse().unwrap()
}

/// Helper function to create a Postgres source.
///
/// IMPORTANT: Make sure to call closure that is returned at
/// the end of the test to clean up Postgres state.
///
/// WARNING: If multiple tests use this, and the tests are run
/// in parallel, then make sure the test use different postgres
/// tables.
fn create_postgres_source_with_table(
    runtime: &Arc<Runtime>,
    mz_client: &mut postgres::Client,
    table_name: &str,
    table_schema: &str,
    source_name: &str,
) -> Result<
    (
        Client,
        impl FnOnce(&mut postgres::Client, &mut Client, &Arc<Runtime>) -> Result<(), Box<dyn Error>>,
    ),
    Box<dyn Error>,
> {
    let postgres_url = env::var("POSTGRES_URL")
        .map_err(|_| anyhow!("POSTGRES_URL environment variable is not set"))?;

    let (pg_client, connection) =
        runtime.block_on(tokio_postgres::connect(&postgres_url, postgres::NoTls))?;

    let pg_config: tokio_postgres::Config = postgres_url.parse().unwrap();
    let user = pg_config.get_user().unwrap_or("postgres");
    let db_name = pg_config.get_dbname().unwrap_or(user);
    let ports = pg_config.get_ports();
    let port = if ports.is_empty() { 5432 } else { ports[0] };
    let hosts = pg_config.get_hosts();
    let host = if hosts.is_empty() {
        "localhost".to_string()
    } else {
        match &hosts[0] {
            Host::Tcp(host) => host.to_string(),
            Host::Unix(host) => host.to_str().unwrap().to_string(),
        }
    };
    let password = pg_config.get_password();

    let pg_runtime = Arc::<tokio::runtime::Runtime>::clone(runtime);
    thread::spawn(move || {
        if let Err(e) = pg_runtime.block_on(connection) {
            panic!("connection error: {}", e);
        }
    });

    // Create table in Postgres with publication.
    let _ =
        runtime.block_on(pg_client.execute(&format!("DROP TABLE IF EXISTS {table_name};"), &[]))?;
    let _ = runtime
        .block_on(pg_client.execute(&format!("DROP PUBLICATION IF EXISTS {source_name};"), &[]))?;
    let _ = runtime
        .block_on(pg_client.execute(&format!("CREATE TABLE {table_name} {table_schema};"), &[]))?;
    let _ = runtime.block_on(pg_client.execute(
        &format!("ALTER TABLE {table_name} REPLICA IDENTITY FULL;"),
        &[],
    ))?;
    let _ = runtime.block_on(pg_client.execute(
        &format!("CREATE PUBLICATION {source_name} FOR TABLE {table_name};"),
        &[],
    ))?;

    // Create postgres source in Materialize.
    let mut connection_str = format!("HOST '{host}', PORT {port}, USER {user}, DATABASE {db_name}");
    if let Some(password) = password {
        let password = std::str::from_utf8(password).unwrap();
        mz_client.batch_execute(&format!("CREATE SECRET s AS '{password}'"))?;
        connection_str = format!("{connection_str}, PASSWORD SECRET s");
    }
    mz_client.batch_execute(&format!(
        "CREATE CONNECTION pgconn FOR POSTGRES {connection_str}"
    ))?;
    mz_client.batch_execute(&format!(
        "CREATE SOURCE {source_name}
            FROM POSTGRES
            CONNECTION pgconn
            (PUBLICATION '{source_name}')
            FOR TABLES ({table_name});"
    ))?;

    let table_name = table_name.to_string();
    let source_name = source_name.to_string();
    Ok((
        pg_client,
        move |mz_client: &mut postgres::Client, pg_client: &mut Client, runtime: &Arc<Runtime>| {
            mz_client.batch_execute(&format!("DROP SOURCE {source_name};"))?;
            mz_client.batch_execute("DROP CONNECTION pgconn;")?;

            let _ = runtime
                .block_on(pg_client.execute(&format!("DROP PUBLICATION {source_name};"), &[]))?;
            let _ =
                runtime.block_on(pg_client.execute(&format!("DROP TABLE {table_name};"), &[]))?;
            Ok(())
        },
    ))
}

fn wait_for_view_population(
    mz_client: &mut postgres::Client,
    view_name: &str,
    source_rows: i64,
) -> Result<(), Box<dyn Error>> {
    let mut rows = 0;
    while rows != source_rows {
        thread::sleep(Duration::from_millis(1));
        // This is a bit hacky. We have no way of getting the freshest data in the view, without
        // also advancing every other object in the time domain, which we usually want to avoid in
        // these tests. Instead we query the view using AS OF a value close to the current system
        // clock and hope it gives us fresh enough data.
        let now = ((SYSTEM_TIME.as_secs() as EpochMillis) * 1_000) - 100;
        rows = mz_client
            .query_one(
                &format!("SELECT COUNT(*) FROM {view_name} AS OF {now};"),
                &[],
            )
            .map(|row| row.get::<_, i64>(0))
            .unwrap_or(0);
    }
    Ok(())
}

#[test]
fn test_load_generator() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let server = util::start_server(util::Config::default().unsafe_mode()).unwrap();
    let mut client = server.connect(postgres::NoTls).unwrap();

    client
        .batch_execute("CREATE SOURCE counter FROM LOAD GENERATOR COUNTER (TICK INTERVAL '1ms')")
        .unwrap();

    let row = client
        .query_one("SELECT count(*), mz_now()::text FROM counter", &[])
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
                    &format!("SELECT count(*) FROM counter AS OF AT LEAST {next}"),
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

    Ok(())
}
