// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

mod util;

#[test]
fn test_prepared_statements() -> util::TestResult {
    ore::log::init();

    let data_dir = None;
    let (_server, conn) = util::start_server(data_dir)?;

    let rows: Vec<String> = conn
        .query("SELECT $1", &[&String::from("42")])?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(rows, &["42"]);

    Ok(())
}

#[test]
fn test_partial_read() -> util::TestResult {
    use fallible_iterator::FallibleIterator;

    ore::log::init();
    let (_server, conn) = util::start_server(None)?;

    // TODO(#1039): we can clean this up using symbiosis mode
    let expected_rows = 3;
    conn.query("CREATE VIEW a AS SELECT * FROM logs_histogram", &[])?;
    conn.query("CREATE VIEW b AS SELECT * FROM logs_channels", &[])?;
    conn.query("CREATE VIEW c AS SELECT * FROM logs_peek_durations", &[])?;

    let query = "SELECT ldd.dataflow \
                 FROM logs_dataflow_dependency ldd \
                 ORDER BY ldd.dataflow";
    let mut simpler = conn.query(query, &[])?;
    // sometimes the logs don't show up for a few millies
    for i in 0..10 {
        if simpler.len() >= expected_rows {
            break;
        }
        if i > 8 {
            println!("WAT: got zero dataflows after 8 tries");
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
        simpler = conn.query(query, &[])?;
    }
    assert!(
        simpler.len() >= expected_rows,
        "we should have found {} rows, but found {}",
        expected_rows,
        simpler.len()
    );

    let stmt = conn.prepare(query)?;
    let trans = conn.transaction()?;

    // TODO: when we migrate to rust-postgres 0.17 we can verify that we are only getting
    // one back at a time.
    let max_rows = 1;
    let rows_lazily: Vec<_> = stmt.lazy_query(&trans, &[], max_rows)?.collect()?;

    let (simple_count, lazy_count) = (simpler.len(), rows_lazily.len());
    assert_eq!(
        simple_count, lazy_count,
        "simple should have same total number of rows as lazy. simple={} lazy={}",
        simple_count, lazy_count
    );
    for (eagerly_row, lazily_row) in simpler.iter().zip(rows_lazily) {
        let eagerly: String = eagerly_row.get(0);
        let lazily: String = lazily_row.get(0);
        assert_eq!(eagerly, lazily);
    }

    Ok(())
}
