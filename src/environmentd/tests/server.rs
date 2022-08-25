// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for Materialize server.

use bytes::Buf;
use std::error::Error;
use std::thread;
use std::time::Duration;

use mz_ore::retry::Retry;
use reqwest::{blocking::Client, StatusCode, Url};
use serde_json::json;
use tokio_postgres::types::{FromSql, Type};

use crate::util::KAFKA_ADDRS;

pub mod util;

#[derive(Debug)]
struct UInt8(u64);

impl<'a> FromSql<'a> for UInt8 {
    fn from_sql(_: &Type, mut raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let v = raw.get_u64();
        if !raw.is_empty() {
            return Err("invalid buffer size".into());
        }
        Ok(Self(v))
    }

    fn accepts(ty: &Type) -> bool {
        ty.oid() == mz_pgrepr::oid::TYPE_UINT8_OID
    }
}

#[test]
fn test_persistence() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let data_dir = tempfile::tempdir()?;
    let config = util::Config::default()
        .data_directory(data_dir.path())
        .unsafe_mode();

    {
        let server = util::start_server(config.clone())?;
        let mut client = server.connect(postgres::NoTls)?;
        client.batch_execute(&format!(
            "CREATE SOURCE src FROM KAFKA BROKER '{}' TOPIC 'ignored' FORMAT BYTES",
            &*KAFKA_ADDRS,
        ))?;
        client.batch_execute("CREATE VIEW constant AS SELECT 1")?;
        client.batch_execute(
            "CREATE VIEW logging_derived AS SELECT * FROM mz_catalog.mz_arrangement_sizes",
        )?;
        client.batch_execute(
            "CREATE VIEW mat (a, a_data, c, c_data) AS SELECT 'a', data, 'c' AS c, data FROM src",
        )?;
        client.batch_execute("CREATE DEFAULT INDEX ON mat")?;
        client.batch_execute("CREATE DATABASE d")?;
        client.batch_execute("CREATE SCHEMA d.s")?;
        client.batch_execute("CREATE VIEW d.s.v AS SELECT 1")?;
    }

    let server = util::start_server(config)?;
    let mut client = server.connect(postgres::NoTls)?;
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
            .map(|row| (
                row.get("Column_name"),
                row.get::<_, UInt8>("Seq_in_index").0
            ))
            .collect::<Vec<(String, u64)>>(),
        &[
            ("a".into(), 1),
            ("a_data".into(), 2),
            ("c".into(), 3),
            ("c_data".into(), 4),
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
            .query(
                "SELECT global_id FROM mz_catalog_names WHERE global_id LIKE 'u%' ORDER BY 1",
                &[]
            )?
            .into_iter()
            .map(|row| row.get(0))
            .collect::<Vec<String>>(),
        vec!["u1", "u2", "u3", "u4", "u5", "u6"]
    );

    Ok(())
}

// Test the /sql POST endpoint of the HTTP server.
#[test]
fn test_http_sql() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();
    let server = util::start_server(util::Config::default())?;
    let url = Url::parse(&format!(
        "http://{}/api/sql",
        server.inner.http_local_addr()
    ))?;

    struct TestCase {
        query: &'static str,
        params: Option<Vec<(&'static str, &'static str)>>,
        status: StatusCode,
        body: &'static str,
    }

    let tests = [
        // Regular query works.
        TestCase {
            query: "select 1+2 as col",
            params: None,
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[3]],"col_names":["col"]}]}"#,
        },
        // Multiple queries are ok.
        TestCase {
            query: "select 1; select 2",
            params: None,
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["?column?"]},{"rows":[[2]],"col_names":["?column?"]}]}"#,
        },
        // Arrays + lists work
        TestCase {
            query: "select array[1], list[2]",
            params: None,
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[[1],[2]]],"col_names":["array","list"]}]}"#,
        },
        // Succeeding and failing queries can mix and match.
        TestCase {
            query: "select 1; select * from noexist;",
            params: None,
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["?column?"]},{"error":"unknown catalog item 'noexist'"}]}"#,
        },
        // CREATEs should work when provided alone.
        TestCase {
            query: "create view v as select 1",
            params: None,
            status: StatusCode::OK,
            body: r#"{"results":[null]}"#,
        },
        // Multiple CREATEs do not work.
        TestCase {
            query: "create view v1 as select 1; create view v2 as select 1",
            params: None,
            status: StatusCode::OK,
            body: r#"{"results":[{"error":"CREATE VIEW v1 AS SELECT 1 cannot be run inside a transaction block"},{"error":"CREATE VIEW v2 AS SELECT 1 cannot be run inside a transaction block"}]}"#,
        },
        // Syntax errors fail the request.
        TestCase {
            query: "'",
            params: None,
            status: StatusCode::BAD_REQUEST,
            body: r#"unterminated quoted string"#,
        },
        // Parameterized queries work
        TestCase {
            query: "select $1+$2 as col",
            params: Some(vec![("1", "int"), ("2", "int8")]),
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[3]],"col_names":["col"]}]}"#,
        },
        TestCase {
            query: "select $1+$2 as col",
            params: Some(vec![
                ("1", "pg_catalog.int4"),
                ("2", "materialize.pg_catalog.int8"),
            ]),
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[3]],"col_names":["col"]}]}"#,
        },
        // Parameters can be present and empty
        TestCase {
            query: "select 3 as col",
            params: Some(vec![]),
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[3]],"col_names":["col"]}]}"#,
        },
        // Ininity, NaN
        TestCase {
            query: "select $1+$2 as col",
            params: Some(vec![("infinity", "numeric"), ("2", "int8")]),
            status: StatusCode::OK,
            body: r#"{"results":[{"error":"invalid input syntax for type numeric: \"infinity\""}]}"#,
        },
        TestCase {
            query: "select $1+$2 as col",
            params: Some(vec![("nan", "numeric"), ("2", "int8")]),
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[["NaN"]],"col_names":["col"]}]}"#,
        },
        // Quotes escaped
        TestCase {
            query: "select length($1), length($2)",
            params: Some(vec![("abc", "text"), ("'abc'", "text")]),
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[3,5]],"col_names":["length","length"]}]}"#,
        },
        // Various forms of SQL injection fail
        TestCase {
            query: "select length($1)",
            params: Some(vec![("'abc'); DELETE FROM users; SELECT version(", "text")]),
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[42]],"col_names":["length"]}]}"#,
        },
        TestCase {
            query: "select length($1)",
            params: Some(vec![(
                "abc' AS text); DELETE FROM users; SELECT CAST('abc",
                "text",
            )]),
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[50]],"col_names":["length"]}]}"#,
        },
        TestCase {
            query: "select length($1)",
            params: Some(vec![("1", "text); DELETE * FROM t; SELECT version(")]),
            status: StatusCode::BAD_REQUEST,
            body: r#"invalid parameter: CAST ('<val>' as <type>) must be valid expression"#,
        },
        // All parameters values treated as strings
        TestCase {
            query: "select length($1), length($2)",
            params: Some(vec![("sum(a)", "text"), ("SELECT * FROM t;", "text")]),
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[6,16]],"col_names":["length","length"]}]}"#,
        },
        // Too many parameters (OK)
        TestCase {
            query: "select $1 as col",
            params: Some(vec![("1", "int"), ("2", "int8")]),
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["col"]}]}"#,
        },
        // Too few parameters (FAIL)
        TestCase {
            query: "select $1+$2 as col",
            params: Some(vec![("1", "int")]),
            status: StatusCode::BAD_REQUEST,
            body: r#"there is no parameter $2"#,
        },
        // Parameters of wrong type
        TestCase {
            query: "select $1+$2 as col",
            params: Some(vec![("1", "int"), ("1s", "interval")]),
            status: StatusCode::OK,
            body: r#"{"results":[{"error":"no overload for integer + interval: arguments cannot be implicitly cast to any implementation's parameters; try providing explicit casts"}]}"#,
        },
        // Parameters with invalid input for type
        TestCase {
            query: "select $1+$2 as col",
            params: Some(vec![("1", "int"), ("abc", "int8")]),
            status: StatusCode::OK,
            body: r#"{"results":[{"error":"invalid input syntax for type bigint: invalid digit found in string: \"abc\""}]}"#,
        },
        // Null string value parameters
        TestCase {
            query: "select $1+$2 as col",
            params: Some(vec![("null", "int"), ("2", "int")]),
            status: StatusCode::OK,
            body: r#"{"results":[{"error":"invalid input syntax for type integer: invalid digit found in string: \"null\""}]}"#,
        },
        // Parameters with invalid type name
        TestCase {
            query: "select $1+$2 as col",
            params: Some(vec![("1", "int"), ("2", "abc")]),
            status: StatusCode::OK,
            body: r#"{"results":[{"error":"unknown catalog item 'abc'"}]}"#,
        },
        // Null type parameters
        TestCase {
            query: "select $1+$2 as col",
            params: Some(vec![("1", "null"), ("2", "int")]),
            status: StatusCode::OK,
            body: r#"{"results":[{"error":"unknown catalog item 'null'"}]}"#,
        },
    ];

    for tc in tests {
        let res = Client::new()
            .post(url.clone())
            .json(&json!({"sql": tc.query, "parameters": tc.params}))
            .send()?;
        assert_eq!(res.status(), tc.status);
        assert_eq!(res.text()?, tc.body);
    }

    struct TestCaseJSON {
        json: serde_json::Value,
        status: StatusCode,
        body_contains: &'static str,
    }

    let json_tests = [
        // null values invalid in params
        TestCaseJSON {
            json: json!({"sql": "select 1 as col", "parameters": vec![(None::<Option<&str>>, "int")]}),
            status: StatusCode::UNPROCESSABLE_ENTITY,
            body_contains: "Failed to deserialize the JSON body into the target type",
        },
        // JSON numbers invalid in params
        TestCaseJSON {
            json: json!({"sql": "select 1 as col", "parameters": vec![(1, "int")]}),
            status: StatusCode::UNPROCESSABLE_ENTITY,
            body_contains: "Failed to deserialize the JSON body into the target type",
        },
        // 0 param elements invalid in params
        TestCaseJSON {
            json: json!({"sql": "select 1 as col", "parameters": vec![()]}),
            status: StatusCode::UNPROCESSABLE_ENTITY,
            body_contains: "Failed to deserialize the JSON body into the target type",
        },
        // 1 element invalid in params
        TestCaseJSON {
            json: json!({"sql": "select $1 as col", "parameters": vec![("1")]}),
            status: StatusCode::UNPROCESSABLE_ENTITY,
            body_contains: "Failed to deserialize the JSON body into the target type",
        },
        // > 2 elements invalid in params
        TestCaseJSON {
            json: json!({"sql": "select $1 as col", "parameters": vec![("1", "int", "int")]}),
            status: StatusCode::BAD_REQUEST,
            body_contains: "Failed to parse the request body as JSON",
        },
    ];

    for tc in json_tests {
        let res = Client::new().post(url.clone()).json(&tc.json).send()?;
        assert_eq!(res.status(), tc.status);
        assert!(res.text()?.contains(tc.body_contains));
    }

    Ok(())
}

// Test that the server properly handles cancellation requests.
#[test]
fn test_cancel_long_running_query() -> Result<(), Box<dyn Error>> {
    let config = util::Config::default().unsafe_mode();
    let server = util::start_server(config)?;

    let mut client = server.connect(postgres::NoTls)?;
    let cancel_token = client.cancel_token();

    thread::spawn(move || {
        // Abort the query after 2s.
        thread::sleep(Duration::from_secs(2));
        let _ = cancel_token.cancel_query(postgres::NoTls);
    });

    client.batch_execute("CREATE TABLE t (i INT)")?;

    match client.simple_query("SELECT * FROM t AS OF 18446744073709551615") {
        Err(e) if e.code() == Some(&postgres::error::SqlState::QUERY_CANCELED) => {}
        Err(e) => panic!("expected error SqlState::QUERY_CANCELED, but got {:?}", e),
        Ok(_) => panic!("expected error SqlState::QUERY_CANCELED, but query succeeded"),
    }

    client
        .simple_query("SELECT 1")
        .expect("simple query succeeds after cancellation");

    Ok(())
}

// Test that dataflow uninstalls cancelled peeks.
#[test]
fn test_cancel_dataflow_removal() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let config = util::Config::default().unsafe_mode();
    let server = util::start_server(config)?;

    let mut client1 = server.connect(postgres::NoTls)?;
    let mut client2 = server.connect(postgres::NoTls)?;
    let cancel_token = client1.cancel_token();

    client1.batch_execute("CREATE TABLE t (i INT)")?;
    // No dataflows expected at startup.
    assert_eq!(
        client1
            .query_one("SELECT count(*) FROM mz_dataflow_operators", &[])?
            .get::<_, i64>(0),
        0
    );

    thread::spawn(move || {
        // Wait until we see the expected dataflow.
        Retry::default()
            .retry(|_state| {
                let count: i64 = client2
                    .query_one("SELECT count(*) FROM mz_dataflow_operators", &[])
                    .map_err(|_| ())?
                    .get(0);
                if count == 0 {
                    Err(())
                } else {
                    Ok(())
                }
            })
            .unwrap();
        cancel_token.cancel_query(postgres::NoTls).unwrap();
    });

    match client1.simple_query("SELECT * FROM t AS OF 18446744073709551615") {
        Err(e) if e.code() == Some(&postgres::error::SqlState::QUERY_CANCELED) => {}
        Err(e) => panic!("expected error SqlState::QUERY_CANCELED, but got {:?}", e),
        Ok(_) => panic!("expected error SqlState::QUERY_CANCELED, but query succeeded"),
    }
    // Expect the dataflows to shut down.
    Retry::default()
        .retry(|_state| {
            let count: i64 = client1
                .query_one("SELECT count(*) FROM mz_dataflow_operators", &[])
                .map_err(|_| ())?
                .get(0);
            if count == 0 {
                Ok(())
            } else {
                Err(())
            }
        })
        .unwrap();

    Ok(())
}
