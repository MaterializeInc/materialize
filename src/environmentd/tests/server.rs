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

    // Test that catalog recovery correctly populates `mz_objects`.
    assert_eq!(
        client
            .query(
                "SELECT id FROM mz_objects WHERE id LIKE 'u%' ORDER BY 1",
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
        status: StatusCode,
        body: &'static str,
    }

    let tests = vec![
        // Regular query works.
        TestCase {
            query: "select 1+2 as col",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[3]],"col_names":["col"]}]}"#,
        },
        // Multiple queries are ok.
        TestCase {
            query: "select 1; select 2",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["?column?"]},{"rows":[[2]],"col_names":["?column?"]}]}"#,
        },
        // Arrays + lists work
        TestCase {
            query: "select array[1], list[2]",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[[1],[2]]],"col_names":["array","list"]}]}"#,
        },
        // Succeeding and failing queries can mix and match.
        TestCase {
            query: "select 1; select * from noexist;",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["?column?"]},{"error":"unknown catalog item 'noexist'"}]}"#,
        },
        // CREATEs should work when provided alone.
        TestCase {
            query: "create view v as select 1",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"CREATE VIEW"}]}"#,
        },
        // Partial errors make it to the client.
        TestCase {
            query: "create view if not exists v as select 1",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"CREATE VIEW","partial_err":{"severity":"notice","message":"view already exists, skipping"}}]}"#,
        },
        // Multiple CREATEs do not work.
        TestCase {
            query: "create view v1 as select 1; create view v2 as select 1",
            status: StatusCode::OK,
            body: r#"{"results":[{"error":"CREATE VIEW v1 AS SELECT 1 cannot be run inside a transaction block"}]}"#,
        },
        // Syntax errors fail the request.
        TestCase {
            query: "'",
            status: StatusCode::BAD_REQUEST,
            body: r#"unterminated quoted string"#,
        },
        // Tables
        TestCase {
            query: "create table t (a int);",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"CREATE TABLE"}]}"#,
        },
        TestCase {
            query: "insert into t values (1)",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"INSERT 0 1"}]}"#,
        },
        // n.b. this used to fail because the insert was treated as an
        // uncommitted explicit transaction
        TestCase {
            query: "select * from t;",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["a"]}]}"#,
        },
        TestCase {
            query: "delete from t",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"DELETE 1"}]}"#,
        },
        TestCase {
            query: "delete from t",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"DELETE 0"}]}"#,
        },
        // # Txns
        // ## Txns, read only
        TestCase {
            query: "begin; select 1; commit",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"BEGIN"},{"rows":[[1]],"col_names":["?column?"]},{"ok":"COMMIT"}]}"#,
        },
        TestCase {
            query: "begin; select 1; commit; select 2;",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"BEGIN"},{"rows":[[1]],"col_names":["?column?"]},{"ok":"COMMIT"},{"rows":[[2]],"col_names":["?column?"]}]}"#,
        },
        TestCase {
            query: "select 1; begin; select 2; commit;",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["?column?"]},{"ok":"BEGIN"},{"rows":[[2]],"col_names":["?column?"]},{"ok":"COMMIT"}]}"#,
        },
        TestCase {
            query: "begin; select 1/0; commit; select 2;",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"BEGIN"},{"error":"division by zero"}]}"#,
        },
        TestCase {
            query: "begin; select 1; commit; select 1/0;",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"BEGIN"},{"rows":[[1]],"col_names":["?column?"]},{"ok":"COMMIT"},{"error":"division by zero"}]}"#,
        },
        TestCase {
            query: "select 1/0; begin; select 2; commit;",
            status: StatusCode::OK,
            body: r#"{"results":[{"error":"division by zero"}]}"#,
        },
        TestCase {
            query: "select 1; begin; select 1/0; commit;",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["?column?"]},{"ok":"BEGIN"},{"error":"division by zero"}]}"#,
        },
        // ## Txns w/ writes
        // Implicit txn aborted on first error
        TestCase {
            query: "insert into t values (1); select 1/0; insert into t values (2)",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"INSERT 0 1"},{"error":"division by zero"}]}"#,
        },
        // Values not successfully written due to aborted txn
        TestCase {
            query: "select * from t;",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[],"col_names":["a"]}]}"#,
        },
        // Explicit txn invocation commits values w/in txn, irrespective of results outside txn
        TestCase {
            query: "begin; insert into t values (1); commit; insert into t values (2); select 1/0;",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"BEGIN"},{"ok":"INSERT 0 1"},{"ok":"COMMIT"},{"ok":"INSERT 0 1"},{"error":"division by zero"}]}"#,
        },
        TestCase {
            query: "select * from t;",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["a"]}]}"#,
        },
        TestCase {
            query: "delete from t;",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"DELETE 1"}]}"#,
        },
        TestCase {
            query: "delete from t;",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"DELETE 0"}]}"#,
        },
        TestCase {
            query: "insert into t values (1); begin; insert into t values (2); insert into t values (3); commit;",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"INSERT 0 1"},{"ok":"BEGIN"},{"ok":"INSERT 0 1"},{"ok":"INSERT 0 1"},{"ok":"COMMIT"}]}"#,
        },
        TestCase {
            query: "select * from t;",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1],[2],[3]],"col_names":["a"]}]}"#,
        },
        TestCase {
            query: "delete from t;",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"DELETE 3"}]}"#,
        },
        // Explicit txn must be terminated to commit
        TestCase {
            query: "begin; insert into t values (1)",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"BEGIN"},{"ok":"INSERT 0 1"}]}"#,
        },
        TestCase {
            query: "select * from t;",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[],"col_names":["a"]}]}"#,
        },
        // Syntax errors fail the request.
        TestCase {
            query: "",
            status: StatusCode::OK,
            body: r#"{"results":[]}"#,
        },
        // Syntax errors fail the request.
        TestCase {
            query: "select $1",
            status: StatusCode::OK,
            body: r#"{"results":[{"error":"request supplied 0 parameters, but SELECT $1 requires 1"}]}"#,
        },
    ];

    for tc in tests {
        let res = Client::new()
            .post(url.clone())
            .json(&json!({"query": tc.query}))
            .send()?;
        assert_eq!(res.status(), tc.status);
        assert_eq!(res.text()?, tc.body);
    }

    // Parameter-based queries

    struct TestCaseParams {
        requests: Vec<(&'static str, Vec<Option<&'static str>>)>,
        status: StatusCode,
        body: &'static str,
    }

    let param_test_cases = vec![
        // Parameterized queries work
        TestCaseParams {
            requests: vec![("select $1+$2::int as col", vec![Some("1"), Some("2")])],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[3]],"col_names":["col"]}]}"#,
        },
        // Parameters can be present and empty
        TestCaseParams {
            requests: vec![("select 3 as col", vec![])],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[3]],"col_names":["col"]}]}"#,
        },
        // Multiple statements
        TestCaseParams {
            requests: vec![
                ("select 1 as col", vec![]),
                ("select $1+$2::int as col", vec![Some("1"), Some("2")]),
            ],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["col"]},{"rows":[[3]],"col_names":["col"]}]}"#,
        },
        TestCaseParams {
            requests: vec![
                ("select $1+$2::int as col", vec![Some("1"), Some("2")]),
                ("select 1 as col", vec![]),
            ],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[3]],"col_names":["col"]},{"rows":[[1]],"col_names":["col"]}]}"#,
        },
        TestCaseParams {
            requests: vec![
                ("select $1+$2::int as col", vec![Some("1"), Some("2")]),
                ("select $1*$2::int as col", vec![Some("2"), Some("3")]),
            ],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[3]],"col_names":["col"]},{"rows":[[6]],"col_names":["col"]}]}"#,
        },
        // Quotes escaped
        TestCaseParams {
            requests: vec![(
                "select length($1), length($2)",
                vec![Some("abc"), Some("'abc'")],
            )],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[3,5]],"col_names":["length","length"]}]}"#,
        },
        // All parameters values treated as strings
        TestCaseParams {
            requests: vec![(
                "select length($1), length($2)",
                vec![Some("sum(a)"), Some("SELECT * FROM t;")],
            )],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[6,16]],"col_names":["length","length"]}]}"#,
        },
        // Too many parameters
        TestCaseParams {
            requests: vec![("select $1 as col", vec![Some("1"), Some("2")])],
            status: StatusCode::OK,
            body: r#"{"results":[{"error":"request supplied 2 parameters, but SELECT $1 AS col requires 1"}]}"#,
        },
        // Too few parameters
        TestCaseParams {
            requests: vec![("select $1+$2::int as col", vec![Some("1")])],
            status: StatusCode::OK,
            body: r#"{"results":[{"error":"request supplied 1 parameters, but SELECT $1 + ($2)::int4 AS col requires 2"}]}"#,
        },
        // NaN
        TestCaseParams {
            requests: vec![("select $1::decimal+2 as col", vec![Some("nan")])],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[["NaN"]],"col_names":["col"]}]}"#,
        },
        // Null string value parameters
        TestCaseParams {
            requests: vec![("select $1+$2::int as col", vec![Some("1"), None])],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[null]],"col_names":["col"]}]}"#,
        },
        // Empty query
        TestCaseParams {
            requests: vec![("", vec![])],
            status: StatusCode::OK,
            body: r#"{"results":[]}"#,
        },
        TestCaseParams {
            requests: vec![("", vec![])],
            status: StatusCode::OK,
            body: r#"{"results":[]}"#,
        },
        // Empty query w/ param
        TestCaseParams {
            requests: vec![("", vec![Some("1")])],
            status: StatusCode::OK,
            body: r#"{"results":[{"error":"cannot provide parameters to an empty query"}]}"#,
        },
        TestCaseParams {
            requests: vec![("select 1 as col", vec![]), ("", vec![None])],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["col"]},{"error":"cannot provide parameters to an empty query"}]}"#,
        },
        // Multiple statements
        TestCaseParams {
            requests: vec![
                ("select 1 as col", vec![]),
                ("select 1; select 2;", vec![None]),
            ],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["col"]},{"error":"each query can contain at most 1 statement"}]}"#,
        },
        // Txns
        // - Rolledback
        TestCaseParams {
            requests: vec![
                ("begin;", vec![]),
                ("insert into t values (1);", vec![]),
                ("rollback", vec![]),
            ],
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"BEGIN"},{"ok":"INSERT 0 1"},{"ok":"ROLLBACK"}]}"#,
        },
        // - Implicit txn
        TestCaseParams {
            requests: vec![
                ("insert into t values (1);", vec![]),
                ("select 1/0;", vec![]),
            ],
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"INSERT 0 1"},{"error":"division by zero"}]}"#,
        },
        // - Errors prevent commit + further execution
        TestCaseParams {
            requests: vec![
                ("begin;", vec![]),
                ("insert into t values (1);", vec![]),
                ("select 1/0;", vec![]),
                ("select * from t", vec![]),
                ("commit", vec![]),
            ],
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"BEGIN"},{"ok":"INSERT 0 1"},{"error":"division by zero"}]}"#,
        },
        // - Requires explicit commit in explicit txn
        TestCaseParams {
            requests: vec![("begin;", vec![]), ("insert into t values (1);", vec![])],
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"BEGIN"},{"ok":"INSERT 0 1"}]}"#,
        },
        TestCaseParams {
            requests: vec![("select * from t", vec![])],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[],"col_names":["a"]}]}"#,
        },
        // Writes
        TestCaseParams {
            requests: vec![
                ("insert into t values ($1);", vec![Some("1")]),
                ("begin;", vec![]),
                ("insert into t values ($1);", vec![Some("2")]),
                ("insert into t values ($1);", vec![Some("3")]),
                ("commit;", vec![]),
                ("select 1/0", vec![]),
            ],
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"INSERT 0 1"},{"ok":"BEGIN"},{"ok":"INSERT 0 1"},{"ok":"INSERT 0 1"},{"ok":"COMMIT"},{"error":"division by zero"}]}"#,
        },
        TestCaseParams {
            requests: vec![("select * from t", vec![])],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1],[2],[3]],"col_names":["a"]}]}"#,
        },
        TestCaseParams {
            requests: vec![
                ("insert into t values ($1);", vec![Some("4")]),
                ("begin;", vec![]),
                ("select 1/0;", vec![]),
                ("commit;", vec![]),
            ],
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"INSERT 0 1"},{"ok":"BEGIN"},{"error":"division by zero"}]}"#,
        },
        TestCaseParams {
            requests: vec![("select * from t", vec![])],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1],[2],[3]],"col_names":["a"]}]}"#,
        },
    ];

    for tc in param_test_cases {
        let mut queries = vec![];
        for (query, params) in tc.requests.into_iter() {
            queries.push(mz_environmentd::http::ExtendedRequest {
                query: query.to_string(),
                params: params
                    .iter()
                    .map(|p| p.map(str::to_string))
                    .collect::<Vec<_>>(),
            });
        }
        let req = mz_environmentd::http::HttpSqlRequest::Extended { queries };
        let res = Client::new().post(url.clone()).json(&req).send()?;
        assert_eq!(res.status(), tc.status, "{:?}: {:?}", req, res.text());
        assert_eq!(res.text()?, tc.body, "{:?}", req);
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
