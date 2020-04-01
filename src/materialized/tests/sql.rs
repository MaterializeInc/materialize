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

use avro::types::Value;
use avro::Schema;
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
fn test_file_sources() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let temp_dir = tempfile::tempdir()?;
    let config = util::Config::default();

    let (_server, mut client) = util::start_server(config)?;

    let fetch_rows = |client: &mut postgres::Client, source| -> Result<_, Box<dyn Error>> {
        // TODO(benesch): use a blocking SELECT when that exists.
        thread::sleep(Duration::from_secs(1));
        Ok(client
            .query(
                &*format!("SELECT * FROM {} ORDER BY mz_line_no", source),
                &[],
            )?
            .into_iter()
            .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
            .collect::<Vec<(String, String, String, i64)>>())
    };

    let fetch_cols = |client: &mut postgres::Client, source| -> Result<_, Box<dyn Error>> {
        // TODO(benesch): use a blocking SELECT when that exists.
        thread::sleep(Duration::from_secs(1));
        let rows = client.query(&*format!("SELECT * FROM {} LIMIT 1", source), &[])?;
        Ok(rows[0]
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect::<Vec<String>>())
    };

    let fetch_avro_rows = |client: &mut postgres::Client, source| -> Result<_, Box<dyn Error>> {
        // TODO(brennan): use a blocking SELECT when that exists.
        thread::sleep(Duration::from_secs(1));
        Ok(client
            .query(
                &*format!("SELECT * FROM {} ORDER BY mz_obj_no", source),
                &[],
            )?
            .into_iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect::<Vec<(f64, f64, i64)>>())
    };

    let append = |file: &mut File, data| -> Result<_, Box<dyn Error>> {
        file.write_all(data)?;
        file.sync_all()?;
        // We currently have to poll for changes on macOS every 100ms, so sleep
        // for 200ms to be sure that the new data has been noticed and accepted
        // by materialize.
        thread::sleep(Duration::from_millis(200));
        Ok(())
    };

    let static_path = Path::join(temp_dir.path(), "static.csv");
    let dynamic_path = Path::join(temp_dir.path(), "dynamic.csv");
    let avro_path = Path::join(temp_dir.path(), "dynamic.avro");
    let avro_schema = serde_json::from_str(
        r#"
    {
     "type": "record",
     "name": "cpx",
     "fields" : [
         {"name": "im", "type" : "double"},
         {"name": "re", "type": "double"}
     ]
    }
    "#,
    )?;
    let avro_schema = Schema::parse(&avro_schema)?;
    let mut dynamic_file = File::create(&dynamic_path)?;
    let mut avro_writer = avro::Writer::new(avro_schema, File::create(&avro_path)?);

    // CSV files
    fs::write(
        &static_path,
        "city,state,zip
Rochester,NY,14618
New York,NY,10004
\"bad,place\"\"\",CA,92679
",
    )?;

    let line1 = ("city".into(), "state".into(), "zip".into(), 1);
    let line2 = ("Rochester".into(), "NY".into(), "14618".into(), 2);
    let line3 = ("New York".into(), "NY".into(), "10004".into(), 3);
    let line4 = ("bad,place\"".into(), "CA".into(), "92679".into(), 4);

    // Static CSV without headers
    client.batch_execute(&*format!(
        "CREATE SOURCE static_csv_source FROM FILE '{}' FORMAT CSV WITH 3 COLUMNS",
        static_path.display(),
    ))?;
    client
        .batch_execute("CREATE MATERIALIZED VIEW static_csv AS SELECT * FROM static_csv_source")?;

    assert_eq!(
        fetch_rows(&mut client, "static_csv")?,
        &[line1.clone(), line2.clone(), line3.clone(), line4.clone()],
    );

    assert_eq!(
        fetch_cols(&mut client, "static_csv")?,
        &[
            "column1".to_string(),
            "column2".to_string(),
            "column3".to_string(),
            "mz_line_no".to_string()
        ],
    );

    // Static CSV with automatic headers
    client.batch_execute(&*format!(
        "CREATE SOURCE static_csv_header_source FROM FILE '{}' FORMAT CSV WITH HEADER",
        static_path.display(),
    ))?;
    client.batch_execute(
        "CREATE MATERIALIZED VIEW static_csv_header AS SELECT * FROM static_csv_header_source",
    )?;

    assert_eq!(
        fetch_rows(&mut client, "static_csv_header")?,
        &[line2.clone(), line3.clone(), line4.clone()],
    );

    assert_eq!(
        fetch_cols(&mut client, "static_csv_header")?,
        &[
            "city".to_string(),
            "state".to_string(),
            "zip".to_string(),
            "mz_line_no".to_string()
        ],
    );

    // Static CSV with automatic headers overwritten
    client.batch_execute(&*format!(
        "CREATE SOURCE static_csv_header_man_source FROM FILE '{}' WITH (col_names='city_man,state_man,zip_man') FORMAT CSV WITH HEADER;",
        static_path.display(),
    ))?;
    client.batch_execute(
        "CREATE MATERIALIZED VIEW static_csv_header_man AS SELECT * FROM static_csv_header_man_source",
    )?;

    assert_eq!(
        fetch_rows(&mut client, "static_csv_header_man")?,
        &[line2.clone(), line3.clone(), line4.clone()],
    );

    assert_eq!(
        fetch_cols(&mut client, "static_csv_header_man")?,
        &[
            "city_man".to_string(),
            "state_man".to_string(),
            "zip_man".to_string(),
            "mz_line_no".to_string()
        ],
    );

    // Dynamic CSV without headers
    append(&mut dynamic_file, b"")?;

    client.batch_execute(&*format!(
        "CREATE SOURCE dynamic_csv_source FROM FILE '{}' WITH (tail = true) FORMAT CSV WITH 3 COLUMNS",
        dynamic_path.display()
    ))?;
    client.batch_execute(
        "CREATE MATERIALIZED VIEW dynamic_csv AS SELECT * FROM dynamic_csv_source",
    )?;

    append(&mut dynamic_file, b"city,state,zip\n")?;
    append(&mut dynamic_file, b"Rochester,NY,14618\n")?;
    assert_eq!(
        fetch_rows(&mut client, "dynamic_csv")?,
        &[line1.clone(), line2.clone()]
    );

    append(&mut dynamic_file, b"New York,NY,10004\n")?;
    assert_eq!(
        fetch_rows(&mut client, "dynamic_csv")?,
        &[line1.clone(), line2.clone(), line3.clone()]
    );

    append(&mut dynamic_file, b"\"bad,place\"\"\",CA,92679\n")?;
    assert_eq!(
        fetch_rows(&mut client, "dynamic_csv")?,
        &[line1, line2, line3, line4]
    );

    fn get_record(i: i32) -> Value {
        Value::Record(vec![
            ("im".to_owned(), Value::Double(i as f64)),
            (
                "re".to_owned(),
                Value::Double((i as f64) * std::f64::consts::PI),
            ),
        ])
    }

    for i in 0..100 {
        let val = get_record(i);
        avro_writer.append(val)?;
    }
    avro_writer.flush()?;

    client.batch_execute(&*format!(
        "CREATE SOURCE dynamic_ocf_source FROM AVRO OCF '{}' WITH (tail = true)",
        avro_path.display()
    ))?;
    client.batch_execute(
        "CREATE MATERIALIZED VIEW dynamic_ocf AS SELECT * FROM dynamic_ocf_source",
    )?;
    assert_eq!(
        fetch_avro_rows(&mut client, "dynamic_ocf")?,
        (0..100)
            .map(|i| (i as f64, (i as f64) * std::f64::consts::PI, i + 1))
            .collect::<Vec<_>>()
    );

    for i in 100..150 {
        let val = get_record(i);
        avro_writer.append(val)?;
    }
    avro_writer.flush()?;

    assert_eq!(
        fetch_avro_rows(&mut client, "dynamic_ocf")?,
        (0..150)
            .map(|i| (i as f64, (i as f64) * std::f64::consts::PI, i + 1))
            .collect::<Vec<_>>()
    );

    // Test the TAIL SQL command on the tailed file source. This is end-to-end
    // tailing: changes to the file will propagate through Materialized and
    // into the user's SQL console.
    let cancel_token = client.cancel_token();
    let mut tail_reader = client.copy_out("TAIL dynamic_csv")?.split(b'\n');

    append(&mut dynamic_file, b"City 1,ST,00001\n")?;
    assert!(tail_reader
        .next()
        .unwrap()?
        .starts_with(&b"City 1\tST\t00001\t5\tDiff: 1 at "[..]));

    append(&mut dynamic_file, b"City 2,ST,00002\n")?;
    assert!(tail_reader
        .next()
        .unwrap()?
        .starts_with(&b"City 2\tST\t00002\t6\tDiff: 1 at "[..]));

    // The tail won't end until a cancellation request is sent.
    cancel_token.cancel_query(postgres::NoTls)?;

    assert!(tail_reader.next().is_none());
    drop(tail_reader);

    // Check that writing to the tailed file after the view and source are
    // dropped doesn't cause a crash (#1361).
    client.execute("DROP VIEW dynamic_csv", &[])?;
    client.execute("DROP SOURCE dynamic_csv_source", &[])?;
    thread::sleep(Duration::from_millis(100));
    append(&mut dynamic_file, b"Glendale,AZ,85310\n")?;
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
        "CREATE SOURCE s FROM FILE '{}' WITH (tail = true) FORMAT BYTES",
        path.display()
    ))?;
    client.batch_execute("CREATE MATERIALIZED VIEW v AS SELECT * FROM s")?;

    // Launch the ill-fated tail.
    client.copy_out("TAIL v")?;

    // Drop order will first disconnect clients and then gracefully shut down
    // the server. We previously had a bug where the server would fail to notice
    // that the client running `TAIL v` had disconnected, and would hang forever
    // waiting for data to be written to `path`, which in this test never comes.
    // So if this function exits, things are working correctly.
    Ok(())
}
