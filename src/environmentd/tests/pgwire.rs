// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for pgwire functionality.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use mz_adapter::session::DEFAULT_DATABASE_NAME;
use mz_environmentd::test_util::{self, PostgresErrorExt};
use mz_ore::collections::CollectionExt;
use mz_ore::error::ErrorExt;
use mz_ore::retry::Retry;
use mz_ore::{assert_err, assert_ok};
use mz_pgrepr::{Numeric, Record};
use postgres::SimpleQueryMessage;
use postgres::binary_copy::BinaryCopyOutIter;
use postgres::error::SqlState;
use postgres::types::Type;
use postgres_array::{Array, Dimension};
use tokio::sync::mpsc;

#[mz_ore::test]
fn test_bind_params() {
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();
    server.enable_feature_flags(&["enable_expressions_in_limit_syntax"]);
    let mut client = server.connect(postgres::NoTls).unwrap();

    match client.query("SELECT ROW(1, 2) = $1", &[&"(1,2)"]) {
        Ok(_) => panic!("query with invalid parameters executed successfully"),
        Err(err) => assert!(
            err.to_string_with_causes()
                .contains("operator does not exist"),
            "unexpected error: {err}"
        ),
    }

    assert!(
        client
            .query_one("SELECT ROW(1, 2) = ROW(1, $1)", &[&2_i32])
            .unwrap()
            .get::<_, bool>(0)
    );

    // Just ensure it does not panic (see database-issues#871).
    client
        .query(
            "EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT $1::int",
            &[&42_i32],
        )
        .unwrap();

    // Ensure that a type hint provided by the client is respected.
    {
        let stmt = client.prepare_typed("SELECT $1", &[Type::INT4]).unwrap();
        let val: i32 = client.query_one(&stmt, &[&42_i32]).unwrap().get(0);
        assert_eq!(val, 42);
    }

    // Ensure that unspecified type hints are inferred.
    {
        let stmt = client
            .prepare_typed("SELECT $1 + $2", &[Type::INT4])
            .unwrap();
        let val: i32 = client.query_one(&stmt, &[&1, &2]).unwrap().get(0);
        assert_eq!(val, 3);
    }

    // Ensure that the fractional component of a decimal is not lost.
    {
        let mut num = Numeric::from(mz_repr::adt::numeric::Numeric::from(123));
        num.0.0.set_exponent(-2);
        let stmt = client
            .prepare_typed("SELECT $1 + 2.34", &[Type::NUMERIC])
            .unwrap();
        let val: Numeric = client.query_one(&stmt, &[&num]).unwrap().get(0);
        assert_eq!(val.to_string(), "3.57");
    }

    // Ensure that parameters in a `SELECT .. LIMIT` clause are supported.
    {
        let stmt = client
            .prepare("SELECT generate_series(1, 3) LIMIT $1")
            .unwrap();
        let vals = client
            .query(&stmt, &[&2_i64])
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect::<Vec<i32>>();
        assert_eq!(vals, &[1, 2]);
    }

    // Ensure that parameters in a `SELECT .. OFFSET` clause are supported.
    // See also in `order_by.slt`.
    {
        let stmt = client
            .prepare("SELECT generate_series(1, 5) OFFSET $1")
            .unwrap();
        let vals = client
            .query(&stmt, &[&2_i64])
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect::<Vec<i32>>();
        assert_eq!(vals, &[3, 4, 5]);
    }

    // Ensure that parameters in a `VALUES .. LIMIT` clause are supported.
    {
        let stmt = client.prepare("VALUES (1), (2), (3) LIMIT $1").unwrap();
        let vals = client
            .query(&stmt, &[&2_i64])
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect::<Vec<i32>>();
        assert_eq!(vals, &[1, 2]);
    }

    // Some statement types can't have parameters at all.
    //
    // TODO: We should harmonize whether `describe` returns parameters for statement types that
    // can't have parameters. For example, currently it does return parameters for
    // EXPLAIN TIMESTAMP,
    // but it doesn't return parameters for
    // CREATE VIEW, CREATE MATERIALIZED VIEW.
    // (It also returns parameters for EXPLAIN SELECT and EXPLAIN FILTER PUSHDOWN, but these are
    // weird special cases currently, see below.)
    {
        let err = client
            .query_one("CREATE VIEW v AS SELECT $3", &[])
            .unwrap_db_error();
        assert_eq!(err.message(), "views cannot have parameters");
        assert_eq!(err.code(), &SqlState::UNDEFINED_PARAMETER);
    }
    {
        let err = client
            .query_one("CREATE MATERIALIZED VIEW mv AS SELECT $3", &[])
            .unwrap_db_error();
        assert_eq!(err.message(), "materialized views cannot have parameters");
        assert_eq!(err.code(), &SqlState::UNDEFINED_PARAMETER);
    }
    {
        let err = client
            // We need to actually supply a parameter here, or we fail inside tokio-postgres,
            // because then the parameter list returned by `describe` doesn't match the supplied
            // params.
            .query_one("EXPLAIN TIMESTAMP FOR SELECT $1::int", &[&42_i32])
            .unwrap_db_error();
        assert_eq!(err.message(), "EXPLAIN TIMESTAMP cannot have parameters");
        assert_eq!(err.code(), &SqlState::UNDEFINED_PARAMETER);
    }
    {
        let err = client
            .query_one("EXPLAIN CREATE MATERIALIZED VIEW mv AS SELECT $3", &[])
            .unwrap_db_error();
        assert_eq!(err.message(), "materialized views cannot have parameters");
        assert_eq!(err.code(), &SqlState::UNDEFINED_PARAMETER);
    }

    // Surprisingly, the following are allowed. The weirdness is that it is only allowed through a
    // pgwire "Extended Query", but not through PREPARE, e.g., `PREPARE EXPLAIN SELECT $1::int`.
    {
        client
            .query_one("EXPLAIN SELECT $1::int", &[&42_i32])
            .expect_element(|| "expected plan");
    }
    {
        let result = client
            .query("EXPLAIN FILTER PUSHDOWN FOR SELECT $1::int", &[&42_i32])
            .unwrap();
        assert!(result.is_empty());
    }

    // Test that `INSERT` statements support prepared statements.
    {
        client.batch_execute("CREATE TABLE t (a int)").unwrap();
        client
            .query("INSERT INTO t VALUES ($1)", &[&42_i32])
            .unwrap();
        let val: i32 = client.query_one("SELECT * FROM t", &[]).unwrap().get(0);
        assert_eq!(val, 42);
    }
}

#[mz_ore::test]
fn test_partial_read() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();
    let query = "VALUES ('1'), ('2'), ('3'), ('4'), ('5'), ('6'), ('7')";

    let simpler = client.query(query, &[]).unwrap();

    let mut simpler_iter = simpler.iter();

    let max_rows = 1;
    let mut trans = client.transaction().unwrap();
    let portal = trans.bind(query, &[]).unwrap();
    for _ in 0..7 {
        let rows = trans.query_portal(&portal, max_rows).unwrap();
        assert_eq!(
            rows.len(),
            usize::try_from(max_rows).unwrap(),
            "should get max rows each time"
        );
        let eagerly = simpler_iter.next().unwrap().get::<_, String>(0);
        let prepared: &str = rows.get(0).unwrap().get(0);
        assert_eq!(prepared, eagerly);
    }
}

#[mz_ore::test]
fn test_read_many_rows() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();
    let query = "VALUES (1), (2), (3)";

    let max_rows = 10_000;
    let mut trans = client.transaction().unwrap();
    let portal = trans.bind(query, &[]).unwrap();
    let rows = trans.query_portal(&portal, max_rows).unwrap();

    assert_eq!(rows.len(), 3, "row len should be all values");
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
async fn test_conn_startup() {
    let server = test_util::TestHarness::default().start().await;
    let client = server.connect().await.unwrap();

    // The default database should be `materialize`.
    assert_eq!(
        client
            .query_one("SHOW database", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        DEFAULT_DATABASE_NAME,
    );

    // Connecting to a nonexistent database should work, and creating that
    // database should work.
    {
        let (notice_tx, mut notice_rx) = mpsc::unbounded_channel();
        let client = server
            .connect()
            .notice_callback(move |notice| notice_tx.send(notice).unwrap())
            .dbname("newdb")
            .await
            .unwrap();

        assert_eq!(
            client
                .query_one("SHOW database", &[])
                .await
                .unwrap()
                .get::<_, String>(0),
            "newdb",
        );
        client.batch_execute("CREATE DATABASE newdb").await.unwrap();
        client
            .batch_execute("CREATE TABLE v (i INT)")
            .await
            .unwrap();
        client
            .batch_execute("INSERT INTO v VALUES (1)")
            .await
            .unwrap();

        match notice_rx.recv().await {
            Some(n) => {
                assert_eq!(*n.code(), SqlState::from_code("MZ004"));
                assert_eq!(n.message(), "session database \"newdb\" does not exist");
            }
            _ => panic!("missing database notice not generated"),
        }
    }

    // Connecting to a nonexistent database should work, and creating that
    // database should work.
    {
        let (notice_tx, mut notice_rx) = mpsc::unbounded_channel();
        server
            .connect()
            .options("--current_object_missing_warnings=off --welcome_message=off")
            .notice_callback(move |notice| notice_tx.send(notice).unwrap())
            .dbname("newdb2")
            .await
            .unwrap();

        // Execute a query to ensure startup notices are flushed.
        client.batch_execute("SELECT 1").await.unwrap();

        drop(client);
        if let Some(n) = notice_rx.recv().await {
            panic!("unexpected notice generated: {n:#?}");
        }
    }

    // Connecting to an existing database should work.
    {
        let client = server.connect().dbname("newdb").await.unwrap();
        assert_eq!(
            // `v` here should refer to the `v` in `newdb.public` that we
            // created above.
            client
                .query_one("SELECT * FROM v", &[])
                .await
                .unwrap()
                .get::<_, i32>(0),
            1,
        );
    }

    // Setting the application name at connection time should be respected.
    {
        let client = server.connect().application_name("hello").await.unwrap();
        assert_eq!(
            client
                .query_one("SHOW application_name", &[])
                .await
                .unwrap()
                .get::<_, String>(0),
            "hello",
        );
    }

    // A welcome notice should be sent.
    {
        let (notice_tx, mut notice_rx) = mpsc::unbounded_channel();
        let _client = server
            .connect()
            .options("") // Override the test harness's default of `--welcome_message=off`.
            .notice_callback(move |notice| notice_tx.send(notice).unwrap())
            .await
            .unwrap();
        match notice_rx.recv().await {
            Some(n) => {
                assert_eq!(*n.code(), SqlState::SUCCESSFUL_COMPLETION);
                assert!(n.message().starts_with("connected to Materialize"));
            }
            _ => panic!("welcome notice not generated"),
        }
    }

    // Test that connecting with an old protocol version is gracefully rejected.
    // This used to crash the adapter.
    {
        use postgres_protocol::message::backend::Message;

        let mut stream = TcpStream::connect(server.sql_local_addr()).unwrap();

        // Send a startup packet for protocol version two, which Materialize
        // does not support.
        let mut buf = vec![];
        buf.extend(0_i32.to_be_bytes()); // frame length, corrected below
        buf.extend(0x20000_i32.to_be_bytes()); // protocol version two
        buf.extend(b"user\0ignored\0\0"); // dummy user parameter
        let len: i32 = buf.len().try_into().unwrap();
        buf[0..4].copy_from_slice(&len.to_be_bytes());
        stream.write_all(&buf).unwrap();

        // Verify the server sends back an error and closes the connection.
        buf.clear();
        stream.read_to_end(&mut buf).unwrap();
        let message = Message::parse(&mut BytesMut::from(&*buf)).unwrap();
        let error = match message {
            Some(Message::ErrorResponse(error)) => error,
            _ => panic!("did not receive expected error response"),
        };
        let mut fields: Vec<_> = error
            .fields()
            .map(|f| {
                Ok((
                    f.type_(),
                    String::from_utf8_lossy(f.value_bytes()).into_owned(),
                ))
            })
            .collect()
            .unwrap();
        fields.sort_by_key(|(ty, _value)| *ty);
        assert_eq!(
            fields,
            &[
                (b'C', "08004".into()),
                (
                    b'M',
                    "server does not support the client's requested protocol version".into()
                ),
                (b'S', "FATAL".into()),
            ]
        );
    }
}

#[mz_ore::test]
fn test_conn_user() {
    let server = test_util::TestHarness::default().start_blocking();

    // This sometimes returns a network error, so retry until we get a db error.
    let err = Retry::default()
        .retry(|_| {
            // Attempting to connect as a nonexistent user via the internal port should fail.
            server
                .pg_config_internal()
                .user("mz_rj")
                .connect(postgres::NoTls)
                .err()
                .unwrap()
                .as_db_error()
                .cloned()
                .ok_or("unexpected error")
        })
        .unwrap();

    assert_eq!(err.severity(), "FATAL");
    assert_eq!(*err.code(), SqlState::INSUFFICIENT_PRIVILEGE);
    assert_eq!(err.message(), "unauthorized login to user 'mz_rj'");

    // But should succeed via the external port.
    let mut client = server
        .pg_config()
        .user("rj")
        .connect(postgres::NoTls)
        .unwrap();
    let row = client.query_one("SELECT current_user", &[]).unwrap();
    assert_eq!(row.get::<_, String>(0), "rj");
}

#[mz_ore::test]
fn test_simple_query_no_hang() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();
    assert_err!(client.simple_query("asdfjkl;"));
    // This will hang if database-issues#972 is not fixed.
    assert_ok!(client.simple_query("SELECT 1"));
}

#[mz_ore::test]
fn test_copy() {
    let server = test_util::TestHarness::default().start_blocking();
    server.enable_feature_flags(&["enable_frontend_peek_sequencing"]);
    let mut client = server.connect(postgres::NoTls).unwrap();

    // Ensure empty COPY result sets work. We used to mishandle this with binary
    // COPY.
    {
        let tail = BinaryCopyOutIter::new(
            client
                .copy_out("COPY (SELECT 1 WHERE FALSE) TO STDOUT (FORMAT BINARY)")
                .unwrap(),
            &[Type::INT4],
        );
        assert_eq!(tail.count().unwrap(), 0);

        let mut buf = String::new();
        client
            .copy_out("COPY (SELECT 1 WHERE FALSE) TO STDOUT")
            .unwrap()
            .read_to_string(&mut buf)
            .unwrap();
        assert_eq!(buf, "");

        let mut buf = String::new();
        client
            .copy_out("COPY (SELECT 1 WHERE FALSE) TO STDOUT (FORMAT CSV)")
            .unwrap()
            .read_to_string(&mut buf)
            .unwrap();
        assert_eq!(buf, "");
    }

    // Test basic, non-empty COPY.
    {
        let tail = BinaryCopyOutIter::new(
            client
                .copy_out("COPY (VALUES (NULL, 2), (E'\t', 4)) TO STDOUT (FORMAT BINARY)")
                .unwrap(),
            &[Type::TEXT, Type::INT4],
        );
        let rows: Vec<(Option<String>, Option<i32>)> = tail
            .map(|row| Ok((row.get(0), row.get(1))))
            .collect()
            .unwrap();
        assert_eq!(rows, &[(None, Some(2)), (Some("\t".into()), Some(4))]);

        let mut buf = String::new();
        client
            .copy_out("COPY (VALUES (NULL, 2), (E'\t', 4)) TO STDOUT")
            .unwrap()
            .read_to_string(&mut buf)
            .unwrap();
        assert_eq!(buf, "\\N\t2\n\\t\t4\n");

        let mut buf = String::new();
        client
            .copy_out("COPY (VALUES (NULL, '21', 2), (E'\t', 'my,str', 4)) TO STDOUT (FORMAT CSV)")
            .unwrap()
            .read_to_string(&mut buf)
            .unwrap();
        assert_eq!(buf, ",21,2\n\t,\"my,str\",4\n");
    }
}

#[mz_ore::test]
fn test_arrays() {
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();

    let row = client
        .query_one("SELECT ARRAY[ARRAY[1], ARRAY[NULL::int], ARRAY[2]]", &[])
        .unwrap();
    let array: Array<Option<i32>> = row.get(0);
    assert_eq!(
        array.dimensions(),
        &[
            Dimension {
                len: 3,
                lower_bound: 1,
            },
            Dimension {
                len: 1,
                lower_bound: 1,
            }
        ]
    );
    assert_eq!(array.into_inner(), &[Some(1), None, Some(2)]);

    let message = client
        .simple_query("SELECT ARRAY[ARRAY[1], ARRAY[NULL::int], ARRAY[2]]")
        .unwrap()
        .into_iter()
        .find(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .unwrap();
    match message {
        SimpleQueryMessage::Row(row) => {
            assert_eq!(row.get(0).unwrap(), "{{1},{NULL},{2}}");
        }
        _ => panic!("unexpected simple query message"),
    }

    let message = client
        .simple_query("SELECT ARRAY[ROW(1,2), ROW(3,4), ROW(5,6)]")
        .unwrap()
        .into_iter()
        .find(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .unwrap();
    match message {
        SimpleQueryMessage::Row(row) => {
            assert_eq!(row.get(0).unwrap(), r#"{"(1,2)","(3,4)","(5,6)"}"#);
        }
        _ => panic!("unexpected simple query message"),
    }
}

#[mz_ore::test]
fn test_record_types() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();

    let row = client.query_one("SELECT ROW()", &[]).unwrap();
    let _: Record<()> = row.get(0);

    let row = client.query_one("SELECT ROW(1)", &[]).unwrap();
    let record: Record<(i32,)> = row.get(0);
    assert_eq!(record, Record((1,)));

    let row = client.query_one("SELECT (1, (2, 3))", &[]).unwrap();
    let record: Record<(i32, Record<(i32, i32)>)> = row.get(0);
    assert_eq!(record, Record((1, Record((2, 3)))));

    let row = client.query_one("SELECT (1, 'a')", &[]).unwrap();
    let record: Record<(i32, String)> = row.get(0);
    assert_eq!(record, Record((1, "a".into())));

    client
        .batch_execute("CREATE TYPE named_composite AS (a int, b text)")
        .unwrap();
    let row = client
        .query_one("SELECT ROW(321, '123')::named_composite", &[])
        .unwrap();
    let record: Record<(i32, String)> = row.get(0);
    assert_eq!(record, Record((321, "123".into())));

    client
        .batch_execute("CREATE TABLE has_named_composites (f named_composite)")
        .unwrap();
    client.batch_execute(
        "INSERT INTO has_named_composites (f) VALUES ((10, '10')), ((20, '20')::named_composite)",
    ).unwrap();
    let rows = client
        .query(
            "SELECT f FROM has_named_composites ORDER BY (f).a DESC",
            &[],
        )
        .unwrap();
    let record: Record<(i32, String)> = rows[0].get(0);
    assert_eq!(record, Record((20, "20".into())));
    let record: Record<(i32, String)> = rows[1].get(0);
    assert_eq!(record, Record((10, "10".into())));
    assert_eq!(rows.len(), 2);
}

fn pg_test_inner(path: &Path, mz_flags: bool) {
    datadriven::walk(path.to_str().unwrap(), |tf| {
        let server = test_util::TestHarness::default()
            .unsafe_mode()
            .start_blocking();
        if mz_flags {
            server.enable_feature_flags(&[
                "enable_copy_to_expr",
                "enable_create_table_from_source",
                "enable_load_generator_datums",
                "enable_raise_statement",
                "unsafe_enable_unorchestrated_cluster_replicas",
                "unsafe_enable_unsafe_functions",
            ]);
        }
        let config = server.pg_config();
        let addr = match &config.get_hosts()[0] {
            tokio_postgres::config::Host::Tcp(host) => {
                format!("{}:{}", host, config.get_ports()[0])
            }
            _ => panic!("only tcp connections supported"),
        };
        let user = config.get_user().unwrap();
        let timeout = Duration::from_secs(120);

        mz_pgtest::run_test(tf, addr, user.to_string(), timeout);
    });
}

#[mz_ore::test]
fn test_pgtest_binary() {
    pg_test_inner(Path::new("../../test/pgtest/binary.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_chr() {
    pg_test_inner(Path::new("../../test/pgtest/chr.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_client_min_messages() {
    pg_test_inner(Path::new("../../test/pgtest/client_min_messages.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_copy_from_2() {
    pg_test_inner(Path::new("../../test/pgtest/copy-from-2.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_copy_from_fail() {
    pg_test_inner(Path::new("../../test/pgtest/copy-from-fail.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_copy_from_null() {
    pg_test_inner(Path::new("../../test/pgtest/copy-from-null.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_copy_from() {
    pg_test_inner(Path::new("../../test/pgtest/copy-from.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_copy() {
    pg_test_inner(Path::new("../../test/pgtest/copy.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_cursors() {
    pg_test_inner(Path::new("../../test/pgtest/cursors.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_ddl_extended() {
    pg_test_inner(Path::new("../../test/pgtest/ddl-extended.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_desc() {
    pg_test_inner(Path::new("../../test/pgtest/desc.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_empty() {
    pg_test_inner(Path::new("../../test/pgtest/empty.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_notice() {
    pg_test_inner(Path::new("../../test/pgtest/notice.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_params() {
    pg_test_inner(Path::new("../../test/pgtest/params.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_portals() {
    pg_test_inner(Path::new("../../test/pgtest/portals.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_prepare() {
    pg_test_inner(Path::new("../../test/pgtest/prepare.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_range() {
    pg_test_inner(Path::new("../../test/pgtest/range.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_transactions() {
    pg_test_inner(Path::new("../../test/pgtest/transactions.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_vars() {
    pg_test_inner(Path::new("../../test/pgtest/vars.pt"), false);
}

// Materialize's differences from Postgres' responses.
#[mz_ore::test]
fn test_pgtest_mz_copy_from_csv() {
    pg_test_inner(Path::new("../../test/pgtest-mz/copy-from-csv.pt"), true);
}

#[mz_ore::test]
fn test_pgtest_mz_copy_to() {
    pg_test_inner(Path::new("../../test/pgtest-mz/copy-to.pt"), true);
}

#[mz_ore::test]
fn test_pgtest_mz_datums() {
    pg_test_inner(Path::new("../../test/pgtest-mz/datums.pt"), true);
}

#[mz_ore::test]
fn test_pgtest_mz_ddl_extended() {
    pg_test_inner(Path::new("../../test/pgtest-mz/ddl-extended.pt"), true);
}

#[mz_ore::test]
fn test_pgtest_mz_desc() {
    pg_test_inner(Path::new("../../test/pgtest-mz/desc.pt"), true);
}

#[mz_ore::test]
fn test_pgtest_mz_notice() {
    pg_test_inner(Path::new("../../test/pgtest-mz/notice.pt"), true);
}

#[mz_ore::test]
fn test_pgtest_mz_parse_started() {
    pg_test_inner(Path::new("../../test/pgtest-mz/parse-started.pt"), true);
}

#[mz_ore::test]
fn test_pgtest_mz_portals() {
    pg_test_inner(Path::new("../../test/pgtest-mz/portals.pt"), true);
}

#[mz_ore::test]
fn test_pgtest_mz_raise() {
    pg_test_inner(Path::new("../../test/pgtest-mz/raise.pt"), true);
}

#[mz_ore::test]
fn test_pgtest_mz_startup() {
    pg_test_inner(Path::new("../../test/pgtest-mz/startup.pt"), true);
}

#[mz_ore::test]
fn test_pgtest_mz_transactions() {
    pg_test_inner(Path::new("../../test/pgtest-mz/transactions.pt"), true);
}

#[mz_ore::test]
fn test_pgtest_mz_vars() {
    pg_test_inner(Path::new("../../test/pgtest-mz/vars.pt"), true);
}
