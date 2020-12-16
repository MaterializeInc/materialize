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
use std::fs::{self, File};
use std::io::{BufRead, Write};
use std::net::TcpListener;
use std::path::Path;
use std::str;
use std::thread;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};

pub mod util;

#[test]
fn test_no_block() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    // Create a listener that will simulate a slow Confluent Schema Registry.
    let listener = TcpListener::bind("localhost:0")?;
    let listener_port = listener.local_addr()?.port();

    let (server, mut client) = util::start_server(util::Config::default())?;

    let slow_thread = thread::spawn(move || {
        client.batch_execute(&format!(
            "CREATE SOURCE foo \
             FROM KAFKA BROKER 'localhost:9092' TOPIC 'foo' \
             FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://localhost:{}'",
            listener_port,
        ))
    });

    // Wait for materialized to contact the schema registry, which indicates
    // the coordinator is processing the CREATE SOURCE command. It will be
    // unable to complete the query until we respond.
    let (mut stream, _) = listener.accept()?;

    // Verify that the coordinator can still process other requests from other
    // sessions.
    let mut client = server.connect()?;
    let answer: i32 = client.query_one("SELECT 1 + 1", &[])?.get(0);
    assert_eq!(answer, 2);

    // Return an error to the coordinator, so that we can shutdown cleanly.
    write!(stream, "HTTP/1.1 503 Service Unavailable\r\n\r\n")?;
    drop(stream);

    // Verify that the schema registry error was returned to the client, for
    // good measure.
    let slow_res = slow_thread.join().unwrap();
    assert!(slow_res
        .unwrap_err()
        .to_string()
        .contains("server error 503"));

    Ok(())
}

#[test]
fn test_current_timestamp_and_now() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let (_server, mut client) = util::start_server(util::Config::default())?;

    // Confirm that `now()` and `current_timestamp()` both return a
    // DateTime<Utc>, but don't assert specific times.
    let row = &client.query_one("SELECT now(), current_timestamp()", &[])?;
    let _ = row.get::<_, DateTime<Utc>>(0);
    let _ = row.get::<_, DateTime<Utc>>(1);

    // Confirm calls to now() return the same DateTime<Utc> both inside and
    // outside of subqueries.
    let row = &client.query_one("SELECT now(), (SELECT now())", &[])?;
    assert_eq!(
        row.get::<_, DateTime<Utc>>(0),
        row.get::<_, DateTime<Utc>>(1)
    );

    // Ensure that EXPLAIN selects a timestamp for `now()` and
    // `current_timestamp()`, though we don't care what the timestamp is.
    let rows = &client.query("EXPLAIN PLAN FOR SELECT now(), current_timestamp()", &[])?;
    assert_eq!(1, rows.len());

    Ok(())
}

fn extract_ts(data: &[u8]) -> Result<u64, Box<dyn Error>> {
    Ok(str::from_utf8(data)?
        .split_whitespace()
        .next()
        .unwrap()
        .parse()?)
}

#[test]
fn test_tail_basic() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let config = util::Config::default().threads(2);
    let (_server, mut client) = util::start_server(config)?;

    let temp_dir = tempfile::tempdir()?;
    let path = Path::join(temp_dir.path(), "dynamic.csv");
    let mut file = File::create(&path)?;
    let mut append = |data| -> Result<_, Box<dyn Error>> {
        file.write_all(data)?;
        file.sync_all()?;
        Ok(())
    };

    client.batch_execute(&*format!(
        "CREATE MATERIALIZED SOURCE dynamic_csv FROM FILE '{}' WITH (tail = true)
         FORMAT CSV WITH 3 COLUMNS",
        path.display()
    ))?;

    // Test the TAIL SQL command on the tailed file source. This is end-to-end
    // tailing: changes to the file will propagate through Materialize and
    // into the user's SQL console.
    let cancel_token = client.cancel_token();
    let mut tail_reader = client
        .copy_out("COPY (TAIL dynamic_csv) TO STDOUT")?
        .split(b'\n');

    let mut events = Vec::new();

    append(b"City 1,ST,00001\n")?;
    let next = tail_reader.next().unwrap()?;
    assert!(next.ends_with(&b"1\tCity 1\tST\t00001\t1"[..]));
    events.push((extract_ts(&next).unwrap(), next.clone()));

    append(b"City 2,ST,00002\n")?;
    let next = tail_reader.next().unwrap()?;
    assert!(next.ends_with(&b"1\tCity 2\tST\t00002\t2"[..]));
    events.push((extract_ts(&next).unwrap(), next.clone()));

    append(b"City 3,ST,00003\n")?;
    let next = tail_reader.next().unwrap()?;
    assert!(next.ends_with(&b"1\tCity 3\tST\t00003\t3"[..]));
    events.push((extract_ts(&next).unwrap(), next.clone()));

    // The tail won't end until a cancellation request is sent.
    cancel_token.cancel_query(postgres::NoTls)?;

    assert!(tail_reader.next().is_none());
    drop(tail_reader);

    // Now tail WITH (SNAPSHOT = false) AS OF each timestamp, verifying that when we do so we only see events
    // that occur as of or later than that timestamp.
    for (ts, _) in &events {
        let cancel_token = client.cancel_token();
        let q = format!(
            "COPY (TAIL dynamic_csv WITH (SNAPSHOT = false) AS OF {}) TO STDOUT",
            ts - 1
        );
        let mut tail_reader = client.copy_out(q.as_str())?.split(b'\n');

        // Skip by the things we won't be able to see.
        for (_, expected) in events.iter().skip_while(|(inner_ts, _)| inner_ts < ts) {
            let actual = tail_reader.next().unwrap()?;
            assert_eq!(actual, *expected);
        }

        cancel_token.cancel_query(postgres::NoTls)?;
        assert!(tail_reader.next().is_none());
        drop(tail_reader);
    }

    // Now tail AS OF each timestamp. We should see a batch of updates all at the
    // tailed timestamp, and then updates afterward.
    for (ts, _) in &events {
        let cancel_token = client.cancel_token();
        let q = format!("COPY (TAIL dynamic_csv AS OF {}) TO STDOUT", ts - 1);
        let mut tail_reader = client.copy_out(q.as_str())?.split(b'\n');

        for (_, expected) in events.iter() {
            let mut expected_ts = extract_ts(expected).unwrap();
            if expected_ts < ts - 1 {
                // If the thing we initially got was before the timestamp, it should have gotten
                // fast-forwarded up to the timestamp.
                expected_ts = ts - 1;
            }

            let actual = tail_reader.next().unwrap()?;
            let actual_ts = extract_ts(&actual).unwrap();

            assert_eq!(expected_ts, actual_ts);
        }

        cancel_token.cancel_query(postgres::NoTls)?;
        assert!(tail_reader.next().is_none());
        drop(tail_reader);
    }

    // Check that writing to the tailed file after the view and source are
    // dropped doesn't cause a crash (#1361).
    client.execute("DROP SOURCE dynamic_csv", &[])?;
    thread::sleep(Duration::from_millis(100));
    append(b"Glendale,AZ,85310\n")?;
    thread::sleep(Duration::from_millis(100));
    Ok(())
}

#[test]
fn test_tail_progress() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let config = util::Config::default().threads(2);
    let (_server, mut client) = util::start_server(config)?;

    let temp_dir = tempfile::tempdir()?;
    let path = Path::join(temp_dir.path(), "dynamic.csv");
    let mut file = File::create(&path)?;
    let mut append = |data| -> Result<_, Box<dyn Error>> {
        file.write_all(data)?;
        file.sync_all()?;
        Ok(())
    };

    client.batch_execute(&*format!(
        "CREATE MATERIALIZED SOURCE dynamic_csv FROM FILE '{}' WITH (tail = true)
         FORMAT CSV WITH 3 COLUMNS",
        path.display()
    ))?;

    // Test the TAIL SQL command on the tailed file source. This is end-to-end
    // tailing: changes to the file will propagate through Materialize and
    // into the user's SQL console.
    let cancel_token = client.cancel_token();
    let mut tail_reader = client
        .copy_out("COPY (TAIL dynamic_csv WITH (PROGRESS)) TO STDOUT")?
        .split(b'\n');

    // Test the done messages by sending inserting a single row and waiting to
    // observe it. Since TAIL always sends a done message at the end of its batches
    // and we won't yet insert a second row, we know that if we've seen a data row
    // we will also see one done message.

    append(b"City 1,ST,00001\n")?;
    let next = tail_reader.next().unwrap()?;
    println!("{}", String::from_utf8_lossy(&next));
    assert!(next.ends_with(&b"f\t1\tCity 1\tST\t00001\t1"[..]));
    let ts = extract_ts(&next)?;
    let next = tail_reader.next().unwrap()?;
    assert!(next.ends_with(&b"t\t\\N\t\\N\t\\N\t\\N\t\\N"[..]));
    assert!(ts < extract_ts(&next)?);

    append(b"City 2,ST,00002\n")?;
    let next = tail_reader.next().unwrap()?;
    println!("{}", String::from_utf8_lossy(&next));
    assert!(next.ends_with(&b"f\t1\tCity 2\tST\t00002\t2"[..]));
    let ts = extract_ts(&next)?;
    let next = tail_reader.next().unwrap()?;
    assert!(next.ends_with(&b"t\t\\N\t\\N\t\\N\t\\N\t\\N"[..]));
    assert!(ts < extract_ts(&next)?);

    append(b"City 3,ST,00003\n")?;
    let next = tail_reader.next().unwrap()?;
    println!("{}", String::from_utf8_lossy(&next));
    assert!(next.ends_with(&b"f\t1\tCity 3\tST\t00003\t3"[..]));
    let ts = extract_ts(&next)?;
    let next = tail_reader.next().unwrap()?;
    assert!(next.ends_with(&b"t\t\\N\t\\N\t\\N\t\\N\t\\N"[..]));
    assert!(ts < extract_ts(&next)?);

    // The tail won't end until a cancellation request is sent.
    cancel_token.cancel_query(postgres::NoTls)?;

    assert!(tail_reader.next().is_none());
    drop(tail_reader);

    Ok(())
}

#[test]
fn test_tail_fetch_timeout() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let config = util::Config::default().threads(2);
    let (_server, mut client) = util::start_server(config)?;

    client.batch_execute(
        "BEGIN;
         CREATE TABLE t (i INT8);
         INSERT INTO t VALUES (1), (2), (3);
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

    // Test a 1s timeout and make sure we waited for atleast that long.
    let before = Instant::now();
    let rows = client.query("FETCH c WITH (TIMEOUT = '1s')", &[])?;
    let duration = before.elapsed();
    assert_eq!(rows.len(), 0);
    // Make sure we waited at least 1s but also not too long.
    assert!(duration >= Duration::from_secs(1));
    assert!(duration < Duration::from_secs(10));

    // Make a new cursor. Try to fetch more rows from it than exist. Verify that
    // we got all the rows we expect and also waited for at least the timeout
    // duration.
    client.batch_execute("DECLARE c CURSOR FOR TAIL t")?;
    let before = Instant::now();
    let rows = client.query("FETCH 4 c WITH (TIMEOUT = '1s')", &[])?;
    let duration = before.elapsed();
    assert_eq!(rows.len(), expected.len());
    assert!(duration >= Duration::from_secs(1));
    assert!(duration < Duration::from_secs(10));
    for i in 0..expected.len() {
        assert_eq!(rows[i].get::<_, i64>(2), expected[i])
    }

    // Another fetch should return nothing.
    let rows = client.query("FETCH c WITH (TIMEOUT = '0s')", &[])?;
    assert_eq!(rows.len(), 0);

    Ok(())
}

#[test]
fn test_tail_fetch_wait() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let config = util::Config::default().threads(2);
    let (_server, mut client) = util::start_server(config)?;

    client.batch_execute(
        "BEGIN;
         CREATE TABLE t (i INT8);
         INSERT INTO t VALUES (1), (2), (3);
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
    client.batch_execute("DECLARE c CURSOR FOR TAIL t;")?;
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
    client.batch_execute(
        "CREATE TABLE empty ();
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
    let (_server, mut client) = util::start_server(config)?;

    client.batch_execute("CREATE MATERIALIZED VIEW foo AS VALUES (1), (2), (3);")?;
    // All records should be read into view before we start tailing.
    thread::sleep(Duration::from_millis(100));

    let mut tail_reader = client
        .copy_out("COPY (TAIL foo WITH (SNAPSHOT = false)) TO STDOUT")?
        .split(b'\n');
    let mut without_snapshot_count = 0;
    while let Some(_value) = tail_reader.next().transpose()? {
        without_snapshot_count += 1;
    }
    assert_eq!(0, without_snapshot_count);
    drop(tail_reader);

    let mut tail_reader = client
        .copy_out("COPY (TAIL foo WITH (SNAPSHOT)) TO STDOUT")?
        .split(b'\n');
    let mut with_snapshot_count = 0;
    while let Some(_value) = tail_reader.next().transpose()? {
        with_snapshot_count += 1;
    }
    assert_eq!(3, with_snapshot_count);

    Ok(())
}

#[test]
fn test_tail_unmaterialized() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let config = util::Config::default();
    let (_server, mut client) = util::start_server(config)?;

    let temp_dir = tempfile::tempdir()?;
    let path = Path::join(temp_dir.path(), "dynamic.csv");
    let mut file = File::create(&path)?;
    let mut append = |data| -> Result<_, Box<dyn Error>> {
        file.write_all(data)?;
        file.sync_all()?;
        Ok(())
    };

    client.batch_execute(&*format!(
        "CREATE SOURCE dynamic_csv FROM FILE '{}' WITH (tail = true)
         FORMAT CSV WITH 3 COLUMNS",
        path.display()
    ))?;

    // Test the TAIL SQL command on the tailed file source. This is end-to-end
    // tailing: changes to the file will propagate through Materialize and
    // into the user's SQL console.
    let cancel_token = client.cancel_token();
    let mut tail_reader = client
        .copy_out("COPY (TAIL dynamic_csv) TO STDOUT")?
        .split(b'\n');

    append(b"City 1,ST,00001\n")?;
    assert!(tail_reader
        .next()
        .unwrap()?
        .ends_with(&b"1\tCity 1\tST\t00001\t1"[..]));

    append(b"City 2,ST,00002\n")?;
    assert!(tail_reader
        .next()
        .unwrap()?
        .ends_with(&b"1\tCity 2\tST\t00002\t2"[..]));

    // The tail won't end until a cancellation request is sent.
    cancel_token.cancel_query(postgres::NoTls)?;

    assert!(tail_reader.next().is_none());
    drop(tail_reader);

    // Check that writing to the tailed file after the view and source are
    // dropped doesn't cause a crash (#1361).
    client.execute("DROP SOURCE dynamic_csv", &[])?;
    thread::sleep(Duration::from_millis(100));
    append(b"Glendale,AZ,85310\n")?;
    thread::sleep(Duration::from_millis(100));
    Ok(())
}

// Tests that a client that launches a non-terminating TAIL and disconnects
// does not keep the server alive forever.
#[test]
fn test_tail_shutdown() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let temp_dir = tempfile::tempdir()?;
    let (_server, mut client) = util::start_server(util::Config::default())?;

    // Create a tailing file source that never produces any data. This is the
    // simplest way to cause a TAIL to never terminate.
    let path = Path::join(temp_dir.path(), "file");
    fs::write(&path, "")?;
    client.batch_execute(&*format!(
        "CREATE MATERIALIZED SOURCE s FROM FILE '{}' WITH (tail = true) FORMAT BYTES",
        path.display()
    ))?;

    // Launch the ill-fated tail.
    client.copy_out("COPY (TAIL s) TO STDOUT")?;

    // Drop order will first disconnect clients and then gracefully shut down
    // the server. We previously had a bug where the server would fail to notice
    // that the client running `TAIL v` had disconnected, and would hang forever
    // waiting for data to be written to `path`, which in this test never comes.
    // So if this function exits, things are working correctly.
    Ok(())
}

// Tests that temporary views created by one connection cannot be viewed
// by another connection.
#[test]
fn test_temporary_views() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let (server, mut client_a) = util::start_server(util::Config::default())?;
    let mut client_b = server.connect()?;
    client_a.batch_execute(
        &*"CREATE VIEW v AS VALUES (1, 'foo'), (2, 'bar'), (3, 'foo'), (1, 'bar');".to_owned(),
    )?;
    client_a.batch_execute(&*"CREATE TEMPORARY VIEW temp_v AS SELECT * FROM v;".to_owned())?;

    let query_v = "SELECT COUNT(*) as count FROM v;";
    let query_temp_v = "SELECT COUNT(*) as count FROM temp_v;";

    // Ensure that client_a can query v and temp_v.
    let count: i64 = client_b.query_one(&*query_v, &[])?.get("count");
    assert_eq!(4, count);
    let count: i64 = client_a.query_one(&*query_temp_v, &[])?.get("count");
    assert_eq!(4, count);

    // Ensure that client_b can query v, but not temp_v.
    let count: i64 = client_b.query_one(&*query_v, &[])?.get("count");
    assert_eq!(4, count);

    let result = client_b.query_one(&*query_temp_v, &[]);
    result.expect_err("unknown catalog item \'temp_v\'");

    Ok(())
}
