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
use std::thread;
use std::time::Duration;

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

#[test]
fn test_tail() -> Result<(), Box<dyn Error>> {
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
        "CREATE MATERIALIZED SOURCE dynamic_csv FROM FILE '{}' WITH (tail = true)
         FORMAT CSV WITH 3 COLUMNS",
        path.display()
    ))?;

    // Test the TAIL SQL command on the tailed file source. This is end-to-end
    // tailing: changes to the file will propagate through Materialize and
    // into the user's SQL console.
    let cancel_token = client.cancel_token();
    let mut tail_reader = client.copy_out("TAIL dynamic_csv")?.split(b'\n');

    append(b"City 1,ST,00001\n")?;
    assert!(tail_reader
        .next()
        .unwrap()?
        .starts_with(&b"City 1\tST\t00001\t1\tDiff: 1 at "[..]));

    append(b"City 2,ST,00002\n")?;
    assert!(tail_reader
        .next()
        .unwrap()?
        .starts_with(&b"City 2\tST\t00002\t2\tDiff: 1 at "[..]));

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
    let mut tail_reader = client.copy_out("TAIL dynamic_csv")?.split(b'\n');

    append(b"City 1,ST,00001\n")?;
    assert!(tail_reader
        .next()
        .unwrap()?
        .starts_with(&b"City 1\tST\t00001\t1\tDiff: 1 at "[..]));

    append(b"City 2,ST,00002\n")?;
    assert!(tail_reader
        .next()
        .unwrap()?
        .starts_with(&b"City 2\tST\t00002\t2\tDiff: 1 at "[..]));

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
    client.copy_out("TAIL s")?;

    // Drop order will first disconnect clients and then gracefully shut down
    // the server. We previously had a bug where the server would fail to notice
    // that the client running `TAIL v` had disconnected, and would hang forever
    // waiting for data to be written to `path`, which in this test never comes.
    // So if this function exits, things are working correctly.
    Ok(())
}
