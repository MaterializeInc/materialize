// Copyright Materialize, Inc. All rights reserved.
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
use std::fs::File;
use std::io::Write;
use std::net::TcpListener;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::thread::sleep;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use log::info;
use tempfile::NamedTempFile;

use util::{MzTimestamp, PostgresErrorExt, KAFKA_ADDRS};

pub mod util;

#[test]
fn test_no_block() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    ore::panic::set_abort_on_panic();
    // This is better than relying on CI to time out,
    // because an actual abort (as opposed to a CI timeout) causes `services.log` to be uploaded.
    let finished = Arc::new(AtomicBool::new(false));
    thread::spawn({
        let finished = finished.clone();
        move || {
            sleep(Duration::from_secs(30));
            if !finished.load(Ordering::SeqCst) {
                panic!("test_no_block timed out")
            }
        }
    });
    // Create a listener that will simulate a slow Confluent Schema Registry.
    info!("test_no_block: creating listener");
    let listener = TcpListener::bind("localhost:0")?;
    let listener_port = listener.local_addr()?.port();

    info!("test_no_block: starting server");
    let server = util::start_server(util::Config::default())?;
    info!("test_no_block: connecting to server");
    let mut client = server.connect(postgres::NoTls)?;

    info!("test_no_block: spawning thread");
    let slow_thread = thread::spawn(move || {
        info!("test_no_block: in thread; executing create source");
        let result = client.batch_execute(&format!(
            "CREATE SOURCE foo \
             FROM KAFKA BROKER '{}' TOPIC 'foo' \
             FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://localhost:{}'",
            &*KAFKA_ADDRS, listener_port,
        ));
        info!("test_no_block: in thread; create source done");
        result
    });

    // Wait for materialized to contact the schema registry, which indicates
    // the coordinator is processing the CREATE SOURCE command. It will be
    // unable to complete the query until we respond.
    info!("test_no_block: accepting fake schema registry connection");
    let (mut stream, _) = listener.accept()?;

    // Verify that the coordinator can still process other requests from other
    // sessions.
    info!("test_no_block: connecting to server again");
    let mut client = server.connect(postgres::NoTls)?;
    info!("test_no_block: executing query");
    let answer: i32 = client.query_one("SELECT 1 + 1", &[])?.get(0);
    assert_eq!(answer, 2);

    // Return an error to the coordinator, so that we can shutdown cleanly.
    info!("test_no_block: writing fake schema registry error");
    write!(stream, "HTTP/1.1 503 Service Unavailable\r\n\r\n")?;
    info!("test_no_block: dropping fake schema registry connection");
    drop(stream);

    // Verify that the schema registry error was returned to the client, for
    // good measure.
    info!("test_no_block: joining thread");
    let slow_res = slow_thread.join().unwrap();
    assert!(slow_res
        .unwrap_err()
        .to_string()
        .contains("server error 503"));

    info!("test_no_block: returning");
    finished.store(true, Ordering::SeqCst);
    Ok(())
}

#[test]
fn test_time() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

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
fn test_tail_basic() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let config = util::Config::default().workers(2);
    let server = util::start_server(config)?;
    let mut client_writes = server.connect(postgres::NoTls)?;
    let mut client_reads = server.connect(postgres::NoTls)?;

    client_writes.batch_execute("CREATE TABLE t (data text)")?;
    client_reads.batch_execute(
        "BEGIN;
         DECLARE c CURSOR FOR TAIL t;",
    )?;

    let mut events = vec![];

    for i in 1..=3 {
        let data = format!("line {}", i);
        client_writes.execute("INSERT INTO t VALUES ($1)", &[&data])?;
        let row = client_reads.query_one("FETCH ALL c", &[])?;
        assert_eq!(row.get::<_, i64>("diff"), 1);
        assert_eq!(row.get::<_, String>("data"), data);
        events.push((row.get::<_, MzTimestamp>("timestamp").0, data));
    }

    // Now tail without a snapshot as of each timestamp, verifying that when we do
    // so we only see events that occur as of or later than that timestamp.
    for (ts, _) in &events {
        client_reads.batch_execute(&*format!(
            "CLOSE c;
            DECLARE c CURSOR FOR TAIL t WITH (SNAPSHOT = false) AS OF {}",
            ts - 1
        ))?;

        // Skip by the things we won't be able to see.
        for (_, expected) in events.iter().skip_while(|(inner_ts, _)| inner_ts < ts) {
            let actual = client_reads.query_one("FETCH c", &[])?;
            assert_eq!(actual.get::<_, String>("data"), *expected);
        }
    }

    // Now tail with a snapshot as of each timestamp. We should see a batch of
    // updates all at the tailed timestamp, and then updates afterward.
    for (ts, _) in &events {
        client_reads.batch_execute(&*format!(
            "CLOSE c;
            DECLARE c CURSOR FOR TAIL t AS OF {}",
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
            assert_eq!(actual.get::<_, MzTimestamp>("timestamp").0, expected_ts);
        }
    }

    // Aggressively compact the data in the index, then tail an unmaterialized
    // view derived from the index. This previously selected an invalid
    // `AS OF` timestamp (#5391).
    client_writes
        .batch_execute("ALTER INDEX t_primary_idx SET (logical_compaction_window = '1ms')")?;
    client_writes.batch_execute("CREATE VIEW v AS SELECT * FROM t")?;
    client_reads.batch_execute(
        "CLOSE c;
         DECLARE c CURSOR FOR TAIL v;",
    )?;
    let rows = client_reads.query("FETCH ALL c", &[])?;
    assert_eq!(rows.len(), 3);
    for i in 0..3 {
        assert_eq!(rows[i].get::<_, i64>("diff"), 1);
        assert_eq!(rows[i].get::<_, String>("data"), format!("line {}", i + 1));
    }

    let err = client_reads
        .batch_execute("TAIL v AS OF 1")
        .unwrap_db_error();
    assert!(err
        .message()
        .starts_with("Timestamp (1) is not valid for all inputs"));

    Ok(())
}

/// Test the done messages by sending inserting a single row and waiting to
/// observe it. Since TAIL always sends a progressed message at the end of its
/// batches and we won't yet insert a second row, we know that if we've seen a
/// data row we will also see one progressed message.
#[test]
fn test_tail_progress() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let config = util::Config::default().workers(2);
    let server = util::start_server(config)?;
    let mut client_writes = server.connect(postgres::NoTls)?;
    let mut client_reads = server.connect(postgres::NoTls)?;

    client_writes.batch_execute("CREATE TABLE t (data text)")?;
    client_reads.batch_execute(
        "BEGIN;
         DECLARE c CURSOR FOR TAIL t WITH (PROGRESS);",
    )?;

    for i in 1..=3 {
        let data = format!("line {}", i);
        client_writes.execute("INSERT INTO t VALUES ($1)", &[&data])?;
        match client_reads.query("FETCH ALL c", &[])?.as_slice() {
            [data_row, progress_row] => {
                assert_eq!(data_row.get::<_, bool>("progressed"), false);
                assert_eq!(data_row.get::<_, i64>("diff"), 1);
                assert_eq!(data_row.get::<_, String>("data"), data);

                assert_eq!(progress_row.get::<_, bool>("progressed"), true);
                assert_eq!(progress_row.get::<_, Option<i64>>("diff"), None);
                assert_eq!(progress_row.get::<_, Option<String>>("data"), None);

                let data_ts: MzTimestamp = data_row.get("timestamp");
                let progress_ts: MzTimestamp = progress_row.get("timestamp");
                assert!(data_ts < progress_ts);
            }
            _ => panic!("wrong number of rows returned"),
        }
    }

    Ok(())
}

#[test]
fn test_tail_fetch_timeout() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let config = util::Config::default().workers(2);
    let server = util::start_server(config)?;
    let mut client = server.connect(postgres::NoTls)?;

    client.batch_execute("CREATE TABLE t (i INT8)")?;
    client.batch_execute("INSERT INTO t VALUES (1), (2), (3);")?;
    client.batch_execute(
        "BEGIN;
         DECLARE c CURSOR FOR TAIL t;",
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
        "CLOSE c;
        DECLARE c CURSOR FOR TAIL t",
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
        "CLOSE c;
        DECLARE c CURSOR FOR TAIL t",
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
fn test_tail_fetch_wait() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let config = util::Config::default().workers(2);
    let server = util::start_server(config)?;
    let mut client = server.connect(postgres::NoTls)?;

    client.batch_execute("CREATE TABLE t (i INT8)")?;
    client.batch_execute("INSERT INTO t VALUES (1), (2), (3)")?;
    client.batch_execute(
        "BEGIN;
         DECLARE c CURSOR FOR TAIL t;",
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
        "CLOSE c;
        DECLARE c CURSOR FOR TAIL t;",
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

    // Verify that the wait only happens for TAIL. A SELECT with 0 rows should not
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
fn test_tail_empty_upper_frontier() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let config = util::Config::default();
    let server = util::start_server(config)?;
    let mut client = server.connect(postgres::NoTls)?;

    client.batch_execute("CREATE MATERIALIZED VIEW foo AS VALUES (1), (2), (3);")?;

    let tail = client.query("TAIL foo WITH (SNAPSHOT = false)", &[])?;
    assert_eq!(0, tail.len());

    let tail = client.query("TAIL foo WITH (SNAPSHOT)", &[])?;
    assert_eq!(3, tail.len());

    Ok(())
}

/// Test the TAIL SQL command on an unmaterialized, tailed file source. This is
/// end-to-end tailing: changes to the file will propagate through Materialize
/// and into the user's SQL console.
#[test]
fn test_tail_unmaterialized_file() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let config = util::Config::default();
    let server = util::start_server(config)?;
    let mut client = server.connect(postgres::NoTls)?;

    let mut file = NamedTempFile::new()?;
    client.batch_execute(&*format!(
        "CREATE SOURCE f FROM FILE '{}' WITH (tail = true) FORMAT TEXT",
        file.path().display()
    ))?;
    client.batch_execute(
        "BEGIN;
         DECLARE c CURSOR FOR TAIL f;",
    )?;

    let mut append = |data| -> Result<_, Box<dyn Error>> {
        file.write_all(data)?;
        file.as_file_mut().sync_all()?;
        Ok(())
    };

    append(b"line 1\n")?;
    let row = client.query_one("FETCH ALL c", &[])?;
    assert_eq!(row.get::<_, i64>("diff"), 1);
    assert_eq!(row.get::<_, String>("text"), "line 1");

    append(b"line 2\n")?;
    let row = client.query_one("FETCH ALL c", &[])?;
    assert_eq!(row.get::<_, i64>("diff"), 1);
    assert_eq!(row.get::<_, String>("text"), "line 2");

    // Wait a little bit to make sure no more new rows arrive.
    let rows = client.query("FETCH ALL c WITH (timeout = '1s')", &[])?;
    assert_eq!(rows.len(), 0);

    // Check that writing to the tailed file after the source is dropped doesn't
    // cause a crash (#1361).
    client.batch_execute("COMMIT")?;
    client.batch_execute("DROP SOURCE f")?;
    thread::sleep(Duration::from_millis(100));
    append(b"line 3\n")?;
    thread::sleep(Duration::from_millis(100));

    Ok(())
}

// Tests that a client that launches a non-terminating TAIL and disconnects
// does not keep the server alive forever.
#[test]
fn test_tail_shutdown() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let server = util::start_server(util::Config::default())?;

    // We have to use the async PostgreSQL client so that we can ungracefully
    // abort the connection task.
    // See: https://github.com/sfackler/rust-postgres/issues/725
    server.runtime.block_on(async {
        let (client, conn_task) = server.connect_async(tokio_postgres::NoTls).await?;

        // Create a table with no data that we can TAIL. This is the simplest
        // way to cause a TAIL to never terminate.
        client.batch_execute("CREATE TABLE t ()").await?;

        // Launch the ill-fated tail.
        client.copy_out("COPY (TAIL t) TO STDOUT").await?;

        // Un-gracefully abort the connection.
        conn_task.abort();

        Ok::<_, Box<dyn Error>>(())
    })?;

    // Dropping the server will initiate a graceful shutdown. We previously had
    // a bug where the server would fail to notice that the client running `TAIL
    // v` had disconnected, and would hang forever waiting for data to be
    // written to `path`, which in this test never comes. So if this function
    // exits, things are working correctly.

    Ok(())
}

// Tests that temporary views created by one connection cannot be viewed
// by another connection.
#[test]
fn test_temporary_views() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

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

// This test attempts to observe a linearizability violation by creating a set of
// sources which are constantly being appended to, then creating a materialized
// view of each of their sizes, then repeatedly reading from some subset of
// those materialized views. If any of the sizes ever decrease from one read to
// the next, linearizability has been violated.
//
// The sources are based off of n CSVs, each named in<i>.csv, a source for each
// one named s<i> which tails the CSV, and a materialized view for each named
// v<i>.
//
// N.B. this test currently fails and is ignored. TODO(justin): fix it.
// N.B. this test also fails more reliably in release mode.
#[test]
#[ignore]
fn test_linearizable() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();
    let config = util::Config::default();
    config.logical_compaction_window(Duration::from_secs(60));
    let server = util::start_server(util::Config::default())?;
    let mut client = server.connect(postgres::NoTls)?;

    const NUM_FILES: usize = 5;
    const NUM_READS: usize = 1000;
    const NUM_WRITES: usize = 100000;

    let temp_dir = tempfile::tempdir()?;

    // For each source we want to create, we spawn a thread that is constantly appending to a CSV,
    // which we tail for the source.
    for i in 0..NUM_FILES {
        let path = Path::join(temp_dir.path(), format!("in{}.csv", i));
        thread::spawn({
            let mut file = File::create(&path)?;
            move || {
                for _ in 0..NUM_WRITES {
                    file.write_all(b"a\n").unwrap();
                    file.sync_all().unwrap();
                }
            }
        });

        sleep(Duration::from_secs(3));

        client.batch_execute(&*format!(
            "CREATE MATERIALIZED SOURCE s{} FROM FILE '{}' WITH (tail = true)
         FORMAT CSV WITH 1 COLUMNS",
            i,
            path.display()
        ))?;
        client.batch_execute(&*format!(
            "CREATE MATERIALIZED VIEW v{} AS SELECT count(*) AS c FROM s{}",
            i, i
        ))?;
    }

    // TODO(justin): kind of hacky.
    sleep(Duration::from_secs(1));

    // largest[i] tracks the highest value seen for v<i>.
    let mut largest = Vec::new();
    for _ in 0..NUM_FILES {
        largest.push(-1);
    }

    for _ in 0..NUM_READS {
        // Construct a query that reads from a random subset of the views.
        let mut query = String::from("SELECT ");
        for i in 0..NUM_FILES {
            if i > 0 {
                query.push_str(", ");
            }
            if rand::random() {
                query.push_str(format!("(SELECT c FROM v{})", i).as_str());
            } else {
                // If we opt not to read from the view, just to keep things
                // lined up, we put a dummy -1 value which we know to ignore.
                query.push_str(format!("-1::int8").as_str());
            }
        }
        let result = client.query_one(query.as_str(), &[])?;
        for i in 0..NUM_FILES {
            let size: i64 = result.get(i);
            if size != -1 {
                if largest[i] > size {
                    // If we hit this, then a value we saw went backwards, which
                    // should be impossible.
                    panic!("linearizability violation: {} {} {}", i, largest[i], size);
                }
                largest[i] = size;
            }
        }
    }

    Ok(())
}
