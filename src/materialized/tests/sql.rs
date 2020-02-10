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
use std::fs;
use std::io::{BufRead, Write};
use std::path::Path;
use std::thread;
use std::time::Duration;

use chrono::{DateTime, Utc};

pub mod util;

#[test]
fn test_current_timestamp_and_now() -> Result<(), Box<dyn Error>> {
    ore::log::init();

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
fn test_regex_sources() -> Result<(), Box<dyn Error>> {
    ore::log::init();

    let temp_dir = tempfile::tempdir()?;
    let config = util::Config::default();

    let (_server, mut client) = util::start_server(config)?;

    let regex_path = Path::join(temp_dir.path(), "request.log");
    fs::write(
        &regex_path,
        r#"123.17.127.5 - - [22/Jan/2020 18:59:52] "GET / HTTP/1.1" 200 -
8.15.119.56 - - [22/Jan/2020 18:59:52] "GET /detail/nNZpqxzR HTTP/1.1" 200 -
96.12.83.72 - - [22/Jan/2020 18:59:52] "GET /search/?kw=helper+ins+hennaed HTTP/1.1" 200 -
"#,
    )?;
    // ip, ts, path, search_kw, product_detail_id, code
    let home_page_row = (
        Some("123.17.127.5".into()),
        Some("22/Jan/2020 18:59:52".into()),
        Some("GET / HTTP/1.1".into()),
        None,
        None,
        Some("200".into()),
    );
    let detail_page_row = (
        Some("8.15.119.56".into()),
        Some("22/Jan/2020 18:59:52".into()),
        Some("GET /detail/nNZpqxzR HTTP/1.1".into()),
        None,
        Some("nNZpqxzR".into()),
        Some("200".into()),
    );
    let search_page_row = (
        Some("96.12.83.72".into()),
        Some("22/Jan/2020 18:59:52".into()),
        Some("GET /search/?kw=helper+ins+hennaed HTTP/1.1".into()),
        Some("helper+ins+hennaed".into()),
        None,
        Some("200".into()),
    );

    client.batch_execute(&*format!(
        "CREATE SOURCE regex_source FROM FILE '{}' FORMAT REGEX '{}'",
        regex_path.display(),
        // Regex explained here: https://www.debuggex.com/r/k48kBEt-lTMUZbaw
        r#"(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<ts>[^]]+)\] "(?P<path>(?:GET /search/\?kw=(?P<search_kw>[^ ]*) HTTP/\d\.\d)|(?:GET /detail/(?P<product_detail_id>[a-zA-Z0-9]+) HTTP/\d\.\d)|(?:[^"]+))" (?P<code>\d{3}) -"#
    ))?;
    client.batch_execute("CREATE MATERIALIZED VIEW regex AS SELECT * FROM regex_source")?;

    // TODO(brennan): use blocking SELECT when that exists.
    thread::sleep(Duration::from_secs(1));

    let all_results: Vec<_> = client
        .query("SELECT * FROM regex ORDER BY mz_line_no", &[])?
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5)))
        .collect::<Vec<(
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        )>>();
    assert_eq!(
        all_results,
        vec![home_page_row, detail_page_row, search_page_row.clone()]
    );

    let search_only: Vec<_> = client
        .query(
            "SELECT search_kw FROM regex WHERE search_kw IS NOT NULL",
            &[],
        )?
        .into_iter()
        .map(|r| r.get(0))
        .collect::<Vec<Option<String>>>();
    assert_eq!(search_only, vec![search_page_row.3]);
    Ok(())
}

#[test]
fn test_file_sources() -> Result<(), Box<dyn Error>> {
    ore::log::init();

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

    let append = |path, data| -> Result<_, Box<dyn Error>> {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
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

    fs::write(
        &static_path,
        "Rochester,NY,14618
New York,NY,10004
\"bad,place\"\"\",CA,92679
",
    )?;

    let line1 = ("Rochester".into(), "NY".into(), "14618".into(), 1);
    let line2 = ("New York".into(), "NY".into(), "10004".into(), 2);
    let line3 = ("bad,place\"".into(), "CA".into(), "92679".into(), 3);

    client.batch_execute(&*format!(
        "CREATE SOURCE static_csv_source FROM FILE '{}' FORMAT CSV WITH 3 COLUMNS",
        static_path.display(),
    ))?;
    client
        .batch_execute("CREATE MATERIALIZED VIEW static_csv AS SELECT * FROM static_csv_source")?;

    assert_eq!(
        fetch_rows(&mut client, "static_csv")?,
        &[line1.clone(), line2.clone(), line3.clone()],
    );

    append(&dynamic_path, b"")?;

    client.batch_execute(&*format!(
        "CREATE SOURCE dynamic_csv_source FROM FILE '{}' WITH (tail = true) FORMAT CSV WITH 3 COLUMNS",
        dynamic_path.display()
    ))?;
    client.batch_execute(
        "CREATE MATERIALIZED VIEW dynamic_csv AS SELECT * FROM dynamic_csv_source",
    )?;

    append(&dynamic_path, b"Rochester,NY,14618\n")?;
    assert_eq!(fetch_rows(&mut client, "dynamic_csv")?, &[line1.clone()]);

    append(&dynamic_path, b"New York,NY,10004\n")?;
    assert_eq!(
        fetch_rows(&mut client, "dynamic_csv")?,
        &[line1.clone(), line2.clone()]
    );

    append(&dynamic_path, b"\"bad,place\"\"\",CA,92679\n")?;
    assert_eq!(
        fetch_rows(&mut client, "dynamic_csv")?,
        &[line1, line2, line3]
    );

    // Test the TAIL SQL command on the tailed file source. This is end-to-end
    // tailing: changes to the file will propagate through Materialized and
    // into the user's SQL console.
    let cancel_token = client.cancel_token();
    let mut tail_reader = client.copy_out("TAIL dynamic_csv")?.split(b'\n');

    append(&dynamic_path, b"City 1,ST,00001\n")?;
    assert!(tail_reader
        .next()
        .unwrap()?
        .starts_with(&b"City 1\tST\t00001\t4\tDiff: 1 at "[..]));

    append(&dynamic_path, b"City 2,ST,00002\n")?;
    assert!(tail_reader
        .next()
        .unwrap()?
        .starts_with(&b"City 2\tST\t00002\t5\tDiff: 1 at "[..]));

    // The tail won't end until a cancellation request is sent.
    cancel_token.cancel_query(postgres::NoTls)?;

    assert!(tail_reader.next().is_none());
    drop(tail_reader);

    // Check that writing to the tailed file after the view and source are
    // dropped doesn't cause a crash (#1361).
    client.execute("DROP VIEW dynamic_csv", &[])?;
    client.execute("DROP SOURCE dynamic_csv_source", &[])?;
    thread::sleep(Duration::from_millis(100));
    append(&dynamic_path, b"Glendale,AZ,85310\n")?;
    thread::sleep(Duration::from_millis(100));
    Ok(())
}

// Tests that a client that launches a non-terminating TAIL and disconnects
// does not keep the server alive forever.
#[test]
fn test_tail_shutdown() -> Result<(), Box<dyn Error>> {
    ore::log::init();

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
