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
            "CREATE CONNECTION kafka_conn FOR KAFKA BROKER '{}'",
            &*KAFKA_ADDRS,
        ))?;
        client.batch_execute(
            "CREATE SOURCE src FROM KAFKA CONNECTION kafka_conn (TOPIC 'ignored') FORMAT BYTES",
        )?;
        client.batch_execute("CREATE VIEW constant AS SELECT 1")?;
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
        &["constant", "mat"]
    );
    assert_eq!(
        client
            .query_one("SHOW INDEXES FROM mat", &[])?
            .get::<_, Vec<String>>("key"),
        &["a", "a_data", "c", "c_data"],
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

    struct TestCaseSimple {
        query: &'static str,
        status: StatusCode,
        body: &'static str,
    }

    let simple_test_cases = vec![
        // Regular query works.
        TestCaseSimple {
            query: "select 1+2 as col",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[3]],"col_names":["col"],"notices":[]}]}"#,
        },
        // Multiple queries are ok.
        TestCaseSimple {
            query: "select 1; select 2",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["?column?"],"notices":[]},{"rows":[[2]],"col_names":["?column?"],"notices":[]}]}"#,
        },
        // Arrays + lists work
        TestCaseSimple {
            query: "select array[1], list[2]",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[[1],[2]]],"col_names":["array","list"],"notices":[]}]}"#,
        },
        // Succeeding and failing queries can mix and match.
        TestCaseSimple {
            query: "select 1; select * from noexist;",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["?column?"],"notices":[]},{"error":"unknown catalog item 'noexist'","notices":[]}]}"#,
        },
        // CREATEs should work when provided alone.
        TestCaseSimple {
            query: "create view v as select 1",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"CREATE VIEW","notices":[]}]}"#,
        },
        // Partial errors make it to the client.
        TestCaseSimple {
            query: "create view if not exists v as select 1",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"CREATE VIEW","notices":[{"message":"view \"v\" already exists, skipping","severity":"notice"}]}]}"#,
        },
        // Multiple CREATEs do not work.
        TestCaseSimple {
            query: "create view v1 as select 1; create view v2 as select 1",
            status: StatusCode::OK,
            body: r#"{"results":[{"error":"CREATE VIEW v1 AS SELECT 1 cannot be run inside a transaction block","notices":[]}]}"#,
        },
        // Syntax errors fail the request.
        TestCaseSimple {
            query: "'",
            status: StatusCode::BAD_REQUEST,
            body: r#"unterminated quoted string"#,
        },
        // Tables
        TestCaseSimple {
            query: "create table t (a int);",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"CREATE TABLE","notices":[]}]}"#,
        },
        TestCaseSimple {
            query: "insert into t values (1)",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"INSERT 0 1","notices":[]}]}"#,
        },
        // n.b. this used to fail because the insert was treated as an
        // uncommitted explicit transaction
        TestCaseSimple {
            query: "select * from t;",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["a"],"notices":[]}]}"#,
        },
        TestCaseSimple {
            query: "delete from t",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"DELETE 1","notices":[]}]}"#,
        },
        TestCaseSimple {
            query: "delete from t",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"DELETE 0","notices":[]}]}"#,
        },
        // # Txns
        // ## Txns, read only
        TestCaseSimple {
            query: "begin; select 1; commit",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"BEGIN","notices":[]},{"rows":[[1]],"col_names":["?column?"],"notices":[]},{"ok":"COMMIT","notices":[]}]}"#,
        },
        TestCaseSimple {
            query: "begin; select 1; commit; select 2;",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"BEGIN","notices":[]},{"rows":[[1]],"col_names":["?column?"],"notices":[]},{"ok":"COMMIT","notices":[]},{"rows":[[2]],"col_names":["?column?"],"notices":[]}]}"#,
        },
        TestCaseSimple {
            query: "select 1; begin; select 2; commit;",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["?column?"],"notices":[]},{"ok":"BEGIN","notices":[]},{"rows":[[2]],"col_names":["?column?"],"notices":[]},{"ok":"COMMIT","notices":[]}]}"#,
        },
        TestCaseSimple {
            query: "begin; select 1/0; commit; select 2;",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"BEGIN","notices":[]},{"error":"division by zero","notices":[]}]}"#,
        },
        TestCaseSimple {
            query: "begin; select 1; commit; select 1/0;",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"BEGIN","notices":[]},{"rows":[[1]],"col_names":["?column?"],"notices":[]},{"ok":"COMMIT","notices":[]},{"error":"division by zero","notices":[]}]}"#,
        },
        TestCaseSimple {
            query: "select 1/0; begin; select 2; commit;",
            status: StatusCode::OK,
            body: r#"{"results":[{"error":"division by zero","notices":[]}]}"#,
        },
        TestCaseSimple {
            query: "select 1; begin; select 1/0; commit;",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["?column?"],"notices":[]},{"ok":"BEGIN","notices":[]},{"error":"division by zero","notices":[]}]}"#,
        },
        // ## Txns w/ writes
        // Implicit txn aborted on first error
        TestCaseSimple {
            query: "insert into t values (1); select 1/0; insert into t values (2)",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"INSERT 0 1","notices":[]},{"error":"division by zero","notices":[]}]}"#,
        },
        // Values not successfully written due to aborted txn
        TestCaseSimple {
            query: "select * from t;",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[],"col_names":["a"],"notices":[]}]}"#,
        },
        // Explicit txn invocation commits values w/in txn, irrespective of results outside txn
        TestCaseSimple {
            query: "begin; insert into t values (1); commit; insert into t values (2); select 1/0;",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"BEGIN","notices":[]},{"ok":"INSERT 0 1","notices":[]},{"ok":"COMMIT","notices":[]},{"ok":"INSERT 0 1","notices":[]},{"error":"division by zero","notices":[]}]}"#,
        },
        TestCaseSimple {
            query: "select * from t;",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["a"],"notices":[]}]}"#,
        },
        TestCaseSimple {
            query: "delete from t;",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"DELETE 1","notices":[]}]}"#,
        },
        TestCaseSimple {
            query: "delete from t;",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"DELETE 0","notices":[]}]}"#,
        },
        TestCaseSimple {
            query: "insert into t values (1); begin; insert into t values (2); insert into t values (3); commit;",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"INSERT 0 1","notices":[]},{"ok":"BEGIN","notices":[]},{"ok":"INSERT 0 1","notices":[]},{"ok":"INSERT 0 1","notices":[]},{"ok":"COMMIT","notices":[]}]}"#,
        },
        TestCaseSimple {
            query: "select * from t;",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1],[2],[3]],"col_names":["a"],"notices":[]}]}"#,
        },
        TestCaseSimple {
            query: "delete from t;",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"DELETE 3","notices":[]}]}"#,
        },
        // Explicit txn must be terminated to commit
        TestCaseSimple {
            query: "begin; insert into t values (1)",
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"BEGIN","notices":[]},{"ok":"INSERT 0 1","notices":[]}]}"#,
        },
        TestCaseSimple {
            query: "select * from t;",
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[],"col_names":["a"],"notices":[]}]}"#,
        },
        // Empty query OK.
        TestCaseSimple {
            query: "",
            status: StatusCode::OK,
            body: r#"{"results":[]}"#,
        },
        // Does not support parameters
        TestCaseSimple {
            query: "select $1",
            status: StatusCode::OK,
            body: r#"{"results":[{"error":"request supplied 0 parameters, but SELECT $1 requires 1","notices":[]}]}"#,
        },
        TestCaseSimple {
            query: "subscribe (select * from t)",
            status: StatusCode::BAD_REQUEST,
            body: r#"unsupported via this API: SUBSCRIBE (SELECT * FROM t)"#,
        },
    ];

    for tc in simple_test_cases {
        let res = Client::new()
            .post(url.clone())
            .json(&json!({"query": tc.query}))
            .send()?;
        assert_eq!(res.status(), tc.status);
        assert_eq!(res.text()?, tc.body);
    }

    // Parameter-based queries

    struct TestCaseExtended {
        requests: Vec<(&'static str, Vec<Option<&'static str>>)>,
        status: StatusCode,
        body: &'static str,
    }

    let extended_test_cases = vec![
        // Parameterized queries work
        TestCaseExtended {
            requests: vec![("select $1+$2::int as col", vec![Some("1"), Some("2")])],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[3]],"col_names":["col"],"notices":[]}]}"#,
        },
        // Parameters can be present and empty
        TestCaseExtended {
            requests: vec![("select 3 as col", vec![])],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[3]],"col_names":["col"],"notices":[]}]}"#,
        },
        // Multiple statements
        TestCaseExtended {
            requests: vec![
                ("select 1 as col", vec![]),
                ("select $1+$2::int as col", vec![Some("1"), Some("2")]),
            ],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1]],"col_names":["col"],"notices":[]},{"rows":[[3]],"col_names":["col"],"notices":[]}]}"#,
        },
        TestCaseExtended {
            requests: vec![
                ("select $1+$2::int as col", vec![Some("1"), Some("2")]),
                ("select 1 as col", vec![]),
            ],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[3]],"col_names":["col"],"notices":[]},{"rows":[[1]],"col_names":["col"],"notices":[]}]}"#,
        },
        TestCaseExtended {
            requests: vec![
                ("select $1+$2::int as col", vec![Some("1"), Some("2")]),
                ("select $1*$2::int as col", vec![Some("2"), Some("3")]),
            ],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[3]],"col_names":["col"],"notices":[]},{"rows":[[6]],"col_names":["col"],"notices":[]}]}"#,
        },
        // Quotes escaped
        TestCaseExtended {
            requests: vec![(
                "select length($1), length($2)",
                vec![Some("abc"), Some("'abc'")],
            )],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[3,5]],"col_names":["length","length"],"notices":[]}]}"#,
        },
        // All parameters values treated as strings
        TestCaseExtended {
            requests: vec![(
                "select length($1), length($2)",
                vec![Some("sum(a)"), Some("SELECT * FROM t;")],
            )],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[6,16]],"col_names":["length","length"],"notices":[]}]}"#,
        },
        // Too many parameters
        TestCaseExtended {
            requests: vec![("select $1 as col", vec![Some("1"), Some("2")])],
            status: StatusCode::OK,
            body: r#"{"results":[{"error":"request supplied 2 parameters, but SELECT $1 AS col requires 1","notices":[]}]}"#,
        },
        // Too few parameters
        TestCaseExtended {
            requests: vec![("select $1+$2::int as col", vec![Some("1")])],
            status: StatusCode::OK,
            body: r#"{"results":[{"error":"request supplied 1 parameters, but SELECT $1 + ($2)::int4 AS col requires 2","notices":[]}]}"#,
        },
        // NaN
        TestCaseExtended {
            requests: vec![("select $1::decimal+2 as col", vec![Some("nan")])],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[["NaN"]],"col_names":["col"],"notices":[]}]}"#,
        },
        // Null string value parameters
        TestCaseExtended {
            requests: vec![("select $1+$2::int as col", vec![Some("1"), None])],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[null]],"col_names":["col"],"notices":[]}]}"#,
        },
        // Empty query
        TestCaseExtended {
            requests: vec![("", vec![])],
            status: StatusCode::BAD_REQUEST,
            body: r#"each query must contain exactly 1 statement, but "" contains 0"#,
        },
        // Empty query w/ param
        TestCaseExtended {
            requests: vec![("", vec![Some("1")])],
            status: StatusCode::BAD_REQUEST,
            body: r#"each query must contain exactly 1 statement, but "" contains 0"#,
        },
        TestCaseExtended {
            requests: vec![("select 1 as col", vec![]), ("", vec![None])],
            status: StatusCode::BAD_REQUEST,
            body: r#"each query must contain exactly 1 statement, but "" contains 0"#,
        },
        // Multiple statements
        TestCaseExtended {
            requests: vec![
                ("select 1 as col", vec![]),
                ("select 1; select 2;", vec![None]),
            ],
            status: StatusCode::BAD_REQUEST,
            body: r#"each query must contain exactly 1 statement, but "select 1; select 2;" contains 2"#,
        },
        // Txns
        // - Rolledback
        TestCaseExtended {
            requests: vec![
                ("begin;", vec![]),
                ("insert into t values (1);", vec![]),
                ("rollback", vec![]),
            ],
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"BEGIN","notices":[]},{"ok":"INSERT 0 1","notices":[]},{"ok":"ROLLBACK","notices":[]}]}"#,
        },
        // - Implicit txn
        TestCaseExtended {
            requests: vec![
                ("insert into t values (1);", vec![]),
                ("select 1/0;", vec![]),
            ],
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"INSERT 0 1","notices":[]},{"error":"division by zero","notices":[]}]}"#,
        },
        // - Errors prevent commit + further execution
        TestCaseExtended {
            requests: vec![
                ("begin;", vec![]),
                ("insert into t values (1);", vec![]),
                ("select 1/0;", vec![]),
                ("select * from t", vec![]),
                ("commit", vec![]),
            ],
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"BEGIN","notices":[]},{"ok":"INSERT 0 1","notices":[]},{"error":"division by zero","notices":[]}]}"#,
        },
        // - Requires explicit commit in explicit txn
        TestCaseExtended {
            requests: vec![("begin;", vec![]), ("insert into t values (1);", vec![])],
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"BEGIN","notices":[]},{"ok":"INSERT 0 1","notices":[]}]}"#,
        },
        TestCaseExtended {
            requests: vec![("select * from t", vec![])],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[],"col_names":["a"],"notices":[]}]}"#,
        },
        // Writes
        TestCaseExtended {
            requests: vec![
                ("insert into t values ($1);", vec![Some("1")]),
                ("begin;", vec![]),
                ("insert into t values ($1);", vec![Some("2")]),
                ("insert into t values ($1);", vec![Some("3")]),
                ("commit;", vec![]),
                ("select 1/0", vec![]),
            ],
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"INSERT 0 1","notices":[]},{"ok":"BEGIN","notices":[]},{"ok":"INSERT 0 1","notices":[]},{"ok":"INSERT 0 1","notices":[]},{"ok":"COMMIT","notices":[]},{"error":"division by zero","notices":[]}]}"#,
        },
        TestCaseExtended {
            requests: vec![("select * from t", vec![])],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1],[2],[3]],"col_names":["a"],"notices":[]}]}"#,
        },
        TestCaseExtended {
            requests: vec![
                ("insert into t values ($1);", vec![Some("4")]),
                ("begin;", vec![]),
                ("select 1/0;", vec![]),
                ("commit;", vec![]),
            ],
            status: StatusCode::OK,
            body: r#"{"results":[{"ok":"INSERT 0 1","notices":[]},{"ok":"BEGIN","notices":[]},{"error":"division by zero","notices":[]}]}"#,
        },
        TestCaseExtended {
            requests: vec![("select * from t", vec![])],
            status: StatusCode::OK,
            body: r#"{"results":[{"rows":[[1],[2],[3]],"col_names":["a"],"notices":[]}]}"#,
        },
        TestCaseExtended {
            requests: vec![("subscribe (select * from t)", vec![])],
            status: StatusCode::BAD_REQUEST,
            body: r#"unsupported via this API: SUBSCRIBE (SELECT * FROM t)"#,
        },
    ];

    for tc in extended_test_cases {
        let mut queries = vec![];
        for (query, params) in tc.requests.into_iter() {
            queries.push(json!({
                "query": query.to_string(),
                "params": params
                    .iter()
                    .map(|p| p.map(str::to_string))
                    .collect::<Vec<_>>(),
            }));
        }
        let req = json!({ "queries": queries });
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
            .query_one(
                "SELECT count(*) FROM mz_internal.mz_dataflow_operators",
                &[]
            )?
            .get::<_, i64>(0),
        0
    );

    thread::spawn(move || {
        // Wait until we see the expected dataflow.
        Retry::default()
            .retry(|_state| {
                let count: i64 = client2
                    .query_one(
                        "SELECT count(*) FROM mz_internal.mz_dataflow_operators",
                        &[],
                    )
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

    match client1.simple_query("SELECT * FROM t AS OF 9223372036854775807") {
        Err(e) if e.code() == Some(&postgres::error::SqlState::QUERY_CANCELED) => {}
        Err(e) => panic!("expected error SqlState::QUERY_CANCELED, but got {:?}", e),
        Ok(_) => panic!("expected error SqlState::QUERY_CANCELED, but query succeeded"),
    }
    // Expect the dataflows to shut down.
    Retry::default()
        .retry(|_state| {
            let count: i64 = client1
                .query_one(
                    "SELECT count(*) FROM mz_internal.mz_dataflow_operators",
                    &[],
                )
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

#[test]
fn test_storage_usage_collection_interval() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let config =
        util::Config::default().with_storage_usage_collection_interval(Duration::from_secs(1));
    let server = util::start_server(config)?;
    let mut client = server.connect(postgres::NoTls)?;

    // Retry because it may take some time for the initial snapshot to be taken.
    let initial_storage: i64 = Retry::default().retry(|_| {
        client
            .query_one(
                "SELECT SUM(size_bytes)::int8 FROM mz_catalog.mz_storage_usage;",
                &[],
            )
            .map_err(|e| e.to_string())?
            .try_get::<_, i64>(0)
            .map_err(|e| e.to_string())
    })?;

    client.batch_execute("CREATE TABLE t (a INT)")?;
    client.batch_execute("INSERT INTO t VALUES (1), (2)")?;

    // Retry until storage usage is updated.
    Retry::default().max_duration(Duration::from_secs(5)).retry(|_| {
        let updated_storage = client
            .query_one("SELECT SUM(size_bytes)::int8 FROM mz_catalog.mz_storage_usage;", &[])
            .map_err(|e| e.to_string())?
            .try_get::<_, i64>(0)
            .map_err(|e| e.to_string())?;

        if updated_storage > initial_storage {
            Ok(())
        } else {
            Err(format!("updated storage count {updated_storage} is not greater than initial storage {initial_storage}"))
        }
    })?;

    Ok(())
}

#[test]
fn test_storage_usage_updates_between_restarts() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let data_dir = tempfile::tempdir()?;
    let storage_usage_collection_interval = Duration::from_secs(3);
    let config = util::Config::default()
        .with_storage_usage_collection_interval(storage_usage_collection_interval)
        .data_directory(data_dir.path());

    // Wait for initial storage usage collection.
    let initial_timestamp: f64 = {
        let server = util::start_server(config.clone())?;
        let mut client = server.connect(postgres::NoTls)?;
        // Retry because it may take some time for the initial snapshot to be taken.
        Retry::default().max_duration(Duration::from_secs(60)).retry(|_| {
            client
                    .query_one(
                        "SELECT EXTRACT(EPOCH FROM MAX(collection_timestamp))::float8 FROM mz_catalog.mz_storage_usage;",
                        &[],
                    )
                    .map_err(|e| e.to_string())?
                    .try_get::<_, f64>(0)
                    .map_err(|e| e.to_string())
        })?
    };

    std::thread::sleep(storage_usage_collection_interval);

    // Another storage usage collection should be scheduled immediately.
    {
        let server = util::start_server(config)?;
        let mut client = server.connect(postgres::NoTls)?;

        // Retry until storage usage is updated.
        Retry::default().max_duration(Duration::from_secs(60)).retry(|_| {
            let updated_timestamp = client
                .query_one("SELECT EXTRACT(EPOCH FROM MAX(collection_timestamp))::float8 FROM mz_catalog.mz_storage_usage;", &[])
                .map_err(|e| e.to_string())?
                .try_get::<_, f64>(0)
                .map_err(|e| e.to_string())?;

            if updated_timestamp > initial_timestamp {
                Ok(())
            } else {
                Err(format!("updated storage collection timestamp {updated_timestamp} is not greater than initial timestamp {initial_timestamp}"))
            }
        })?;
    }

    Ok(())
}

#[test]
fn test_storage_usage_doesnt_update_between_restarts() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let data_dir = tempfile::tempdir()?;
    let storage_usage_collection_interval = Duration::from_secs(60);
    let config = util::Config::default()
        .with_storage_usage_collection_interval(storage_usage_collection_interval)
        .data_directory(data_dir.path());

    // Wait for initial storage usage collection.
    let initial_timestamp: f64 = {
        let server = util::start_server(config.clone())?;
        let mut client = server.connect(postgres::NoTls)?;
        // Retry because it may take some time for the initial snapshot to be taken.
        Retry::default().max_duration(Duration::from_secs(60)).retry(|_| {
            client
                    .query_one(
                        "SELECT EXTRACT(EPOCH FROM MAX(collection_timestamp))::float8 FROM mz_catalog.mz_storage_usage;",
                        &[],
                    )
                    .map_err(|e| e.to_string())?
                    .try_get::<_, f64>(0)
                    .map_err(|e| e.to_string())
        })?
    };

    std::thread::sleep(Duration::from_secs(2));

    // Another storage usage collection should not be scheduled immediately.
    {
        let server = util::start_server(config)?;
        let mut client = server.connect(postgres::NoTls)?;

        let updated_timestamp = client
            .query_one(
                "SELECT EXTRACT(EPOCH FROM MAX(collection_timestamp))::float8 FROM mz_catalog.mz_storage_usage;",
                &[],
            )?.get::<_, f64>(0);

        assert_eq!(initial_timestamp, updated_timestamp);
    }

    Ok(())
}
