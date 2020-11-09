// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for Materialize server.

use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::path::Path;

use reqwest::{blocking::Client, StatusCode, Url};

pub mod util;

#[test]
fn test_persistence() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let data_dir = tempfile::tempdir()?;
    let config = util::Config::default().data_directory(data_dir.path().to_owned());

    let temp_dir = tempfile::tempdir()?;
    let temp_file = Path::join(temp_dir.path(), "source.txt");
    File::create(&temp_file)?;

    {
        let (_server, mut client) = util::start_server(config.clone())?;
        client.batch_execute(&format!(
            "CREATE SOURCE src FROM FILE '{}' FORMAT BYTES; \
             CREATE VIEW constant AS SELECT 1; \
             CREATE VIEW logging_derived AS SELECT * FROM mz_catalog.mz_arrangement_sizes; \
             CREATE MATERIALIZED VIEW mat AS SELECT 'a', data, 'c' AS c, data FROM src; \
             CREATE DATABASE d; \
             CREATE SCHEMA d.s; \
             CREATE VIEW d.s.v AS SELECT 1;",
            temp_file.display(),
        ))?;
    }

    {
        let (_server, mut client) = util::start_server(config.clone())?;
        assert_eq!(
            client
                .query("SHOW VIEWS", &[])?
                .into_iter()
                .map(|row| row.get(0))
                .collect::<Vec<String>>(),
            &["constant", "logging_derived", "mat"]
        );
        assert_eq!(
            client
                .query("SHOW INDEXES FROM mat", &[])?
                .into_iter()
                .map(|row| (row.get("Column_name"), row.get("Seq_in_index")))
                .collect::<Vec<(String, i64)>>(),
            &[
                ("?column?".into(), 1),
                ("data".into(), 2),
                ("c".into(), 3),
                ("data".into(), 4),
            ],
        );
        assert_eq!(
            client
                .query("SHOW VIEWS FROM d.s", &[])?
                .into_iter()
                .map(|row| row.get(0))
                .collect::<Vec<String>>(),
            &["v"]
        );

        // Test that catalog recovery correctly populates `mz_catalog_names`.
        assert_eq!(
            client
                .query("SELECT global_id FROM mz_catalog_names ORDER BY 1", &[])?
                .into_iter()
                .map(|row| row.get(0))
                .collect::<Vec<String>>(),
            vec![
                "s1000", "s1001", "s1002", "s1003", "s1004", "s1005", "s1006", "s1007", "s1008",
                "s1009", "s1010", "s1011", "s1012", "s1013", "s1014", "s1015", "s1016", "s1017",
                "s1018", "s1019", "s1020", "s1021", "s1022", "s1023", "s1024", "s1025", "s1026",
                "s1027", "s2001", "s2002", "s2003", "s2004", "s2005", "s2006", "s2007", "s2008",
                "s2009", "s2010", "s2011", "s2012", "s2013", "s2014", "s2015", "s2016", "s2017",
                "s2018", "s2019", "s2020", "s2021", "s2022", "s2023", "s2024", "s2025", "s2026",
                "s2027", "s2028", "s2029", "s2030", "s3000", "s3001", "s3002", "s3003", "s3004",
                "s3005", "s3006", "s3007", "s3008", "s3009", "s3010", "s3011", "s3012", "s3013",
                "s3014", "s3015", "s3016", "s3017", "s3018", "s3019", "s3020", "s3021", "s3022",
                "s3023", "s3024", "u1", "u2", "u3", "u4", "u5", "u6"
            ]
        );
    }

    {
        let config = config.logging_granularity(None);
        match util::start_server(config) {
            Ok(_) => panic!("server unexpectedly booted with corrupted catalog"),
            Err(e) => assert_eq!(
                e.to_string(),
                "catalog item 'materialize.public.logging_derived' depends on system logging, \
                 but logging is disabled"
            ),
        }
    }

    Ok(())
}

// Ensures that once a node is started with `--experimental`, it requires
// `--experimental` on reboot.
#[test]
fn test_experimental_mode_reboot() -> Result<(), Box<dyn Error>> {
    let data_dir = tempfile::tempdir()?;
    let config = util::Config::default().data_directory(data_dir.path().to_owned());

    {
        let (_server, _) = util::start_server(config.clone().experimental_mode())?;
    }

    {
        match util::start_server(config.clone()) {
            Ok((_server, _)) => panic!("unexpected success"),
            Err(e) => {
                if !e
                    .to_string()
                    .contains("Materialize previously started with --experimental")
                {
                    return Err(e);
                }
            }
        }
    }

    {
        let (_server, _) = util::start_server(config.experimental_mode())?;
    }

    Ok(())
}

// Ensures that only new nodes can start in experimental mode.
#[test]
fn test_experimental_mode_on_init_or_never() -> Result<(), Box<dyn Error>> {
    let data_dir = tempfile::tempdir()?;
    let config = util::Config::default().data_directory(data_dir.path().to_owned());

    {
        let (_server, _) = util::start_server(config.clone())?;
    }

    {
        match util::start_server(config.experimental_mode()) {
            Ok((_server, _)) => panic!("unexpected success"),
            Err(e) => {
                if !e
                    .to_string()
                    .contains("Experimental mode is only available on new nodes")
                {
                    return Err(e);
                }
            }
        }
    }

    Ok(())
}

// Test the /sql POST endpoint of the HTTP server.
#[test]
fn test_http_sql() -> Result<(), Box<dyn Error>> {
    let (server, _client) = util::start_server(util::Config::default())?;
    let url = Url::parse(&format!("http://{}/sql", server.inner.local_addr()))?;
    let mut params = HashMap::new();

    struct TestCase {
        query: &'static str,
        status: StatusCode,
        body: &'static str,
    }

    let tests = vec![
        // Regular query works.
        TestCase {
            query: "select 1+2 as col",
            status: StatusCode::OK,
            body: r#"{"rows":[[3]],"col_names":["col"]}"#,
        },
        // Only one query at a time.
        TestCase {
            query: "select 1; select 2",
            status: StatusCode::BAD_REQUEST,
            body: r#"expected exactly 1 statement"#,
        },
        // CREATEs should not work.
        TestCase {
            query: "create view v as select 1",
            status: StatusCode::BAD_REQUEST,
            body: r#"unsupported plan"#,
        },
    ];

    for tc in tests {
        params.insert("sql", tc.query);
        let res = Client::new().post(url.clone()).form(&params).send()?;
        assert_eq!(res.status(), tc.status);
        assert_eq!(res.text()?, tc.body);
    }

    Ok(())
}
