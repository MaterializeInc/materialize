// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for pgwire functionality.

use std::error::Error;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::time::Duration;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use futures::future;
use mz_adapter::session::DEFAULT_DATABASE_NAME;
use postgres::binary_copy::BinaryCopyOutIter;
use postgres::error::SqlState;
use postgres::types::Type;
use postgres::SimpleQueryMessage;
use postgres_array::{Array, Dimension};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use mz_ore::collections::CollectionExt;
use mz_ore::{assert_contains, task};
use mz_pgrepr::{Numeric, Record};

use crate::util::PostgresErrorExt;

pub mod util;

#[test]
fn test_bind_params() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let config = util::Config::default().unsafe_mode();
    let server = util::start_server(config)?;
    let mut client = server.connect(postgres::NoTls)?;

    match client.query("SELECT ROW(1, 2) = $1", &[&42_i32]) {
        Ok(_) => panic!("query with invalid parameters executed successfully"),
        Err(err) => assert_contains!(format!("{:?}", err.source()), "WrongType"),
    }

    match client.query("SELECT ROW(1, 2) = $1", &[&"(1,2)"]) {
        Ok(_) => panic!("query with invalid parameters executed successfully"),
        Err(err) => assert!(err.to_string().contains("no overload")),
    }

    assert!(client
        .query_one("SELECT ROW(1, 2) = ROW(1, $1)", &[&2_i32])?
        .get::<_, bool>(0));

    // Just ensure it does not panic (see #2498).
    client.query("EXPLAIN PLAN FOR SELECT $1::int", &[&42_i32])?;

    // Ensure that a type hint provided by the client is respected.
    {
        let stmt = client.prepare_typed("SELECT $1", &[Type::INT4])?;
        let val: i32 = client.query_one(&stmt, &[&42_i32])?.get(0);
        assert_eq!(val, 42);
    }

    // Ensure that unspecified type hints are inferred.
    {
        let stmt = client.prepare_typed("SELECT $1 + $2", &[Type::INT4])?;
        let val: i32 = client.query_one(&stmt, &[&1, &2])?.get(0);
        assert_eq!(val, 3);
    }

    // Ensure that the fractional component of a decimal is not lost.
    {
        let mut num = Numeric::from(mz_repr::adt::numeric::Numeric::from(123));
        num.0 .0.set_exponent(-2);
        let stmt = client.prepare_typed("SELECT $1 + 2.34", &[Type::NUMERIC])?;
        let val: Numeric = client.query_one(&stmt, &[&num])?.get(0);
        assert_eq!(val.to_string(), "3.57");
    }

    // A `CREATE` statement with parameters should be rejected.
    let err = client
        .query_one("CREATE VIEW v AS SELECT $3", &[])
        .unwrap_db_error();
    // TODO(benesch): this should be `UNDEFINED_PARAMETER`, but blocked
    // on #3147.
    assert_eq!(err.message(), "there is no parameter $3");
    assert_eq!(err.code(), &SqlState::INTERNAL_ERROR);

    // Test that `INSERT` statements support prepared statements.
    {
        client.batch_execute("CREATE TABLE t (a int)")?;
        client.query("INSERT INTO t VALUES ($1)", &[&42_i32])?;
        let val: i32 = client.query_one("SELECT * FROM t", &[])?.get(0);
        assert_eq!(val, 42);
    }

    Ok(())
}

#[test]
fn test_partial_read() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let server = util::start_server(util::Config::default())?;
    let mut client = server.connect(postgres::NoTls)?;
    let query = "VALUES ('1'), ('2'), ('3'), ('4'), ('5'), ('6'), ('7')";

    let simpler = client.query(query, &[])?;

    let mut simpler_iter = simpler.iter();

    let max_rows = 1;
    let mut trans = client.transaction()?;
    let portal = trans.bind(query, &[])?;
    for _ in 0..7 {
        let rows = trans.query_portal(&portal, max_rows)?;
        assert_eq!(
            rows.len(),
            max_rows as usize,
            "should get max rows each time"
        );
        let eagerly = simpler_iter.next().unwrap().get::<_, String>(0);
        let prepared: &str = rows.get(0).unwrap().get(0);
        assert_eq!(prepared, eagerly);
    }

    Ok(())
}

#[test]
fn test_read_many_rows() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let server = util::start_server(util::Config::default())?;
    let mut client = server.connect(postgres::NoTls)?;
    let query = "VALUES (1), (2), (3)";

    let max_rows = 10_000;
    let mut trans = client.transaction()?;
    let portal = trans.bind(query, &[])?;
    let rows = trans.query_portal(&portal, max_rows)?;

    assert_eq!(rows.len(), 3, "row len should be all values");

    Ok(())
}

#[test]
fn test_conn_startup() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let server = util::start_server(util::Config::default())?;
    let mut client = server.connect(postgres::NoTls)?;

    // The default database should be `materialize`.
    assert_eq!(
        client.query_one("SHOW database", &[])?.get::<_, String>(0),
        DEFAULT_DATABASE_NAME,
    );

    // Connecting to a nonexistent database should work, and creating that
    // database should work.
    //
    // TODO(benesch): we can use the sync client when this issue is fixed:
    // https://github.com/sfackler/rust-postgres/issues/404.
    Runtime::new()?.block_on(async {
        let (client, mut conn) = server
            .pg_config_async()
            .dbname("newdb")
            .connect(postgres::NoTls)
            .await?;
        let (notice_tx, mut notice_rx) = mpsc::unbounded_channel();
        task::spawn(|| "test_conn_startup", async move {
            while let Some(msg) = future::poll_fn(|cx| conn.poll_message(cx)).await {
                match msg {
                    Ok(msg) => notice_tx.send(msg).unwrap(),
                    Err(e) => panic!("{}", e),
                }
            }
        });

        assert_eq!(
            client
                .query_one("SHOW database", &[])
                .await?
                .get::<_, String>(0),
            "newdb",
        );
        client.batch_execute("CREATE DATABASE newdb").await?;
        client.batch_execute("CREATE TABLE v (i INT)").await?;
        client.batch_execute("INSERT INTO v VALUES (1)").await?;

        match notice_rx.recv().await {
            Some(tokio_postgres::AsyncMessage::Notice(n)) => {
                assert_eq!(*n.code(), SqlState::SUCCESSFUL_COMPLETION);
                assert_eq!(n.message(), "session database \"newdb\" does not exist");
            }
            _ => panic!("missing database notice not generated"),
        }

        Ok::<_, Box<dyn Error>>(())
    })?;

    // Connecting to an existing database should work.
    {
        let mut client = server
            .pg_config()
            .dbname("newdb")
            .connect(postgres::NoTls)?;

        assert_eq!(
            // `v` here should refer to the `v` in `newdb.public` that we
            // created above.
            client.query_one("SELECT * FROM v", &[])?.get::<_, i32>(0),
            1,
        );
    }

    // Setting the application name at connection time should be respected.
    {
        let mut client = server
            .pg_config()
            .application_name("hello")
            .connect(postgres::NoTls)?;
        assert_eq!(
            client
                .query_one("SHOW application_name", &[])?
                .get::<_, String>(0),
            "hello",
        );
    }

    // Test that connecting with an old protocol version is gracefully rejected.
    // This used to crash the adapter.
    {
        use postgres_protocol::message::backend::Message;

        let mut stream = TcpStream::connect(server.inner.sql_local_addr())?;

        // Send a startup packet for protocol version two, which Materialize
        // does not support.
        let mut buf = vec![];
        buf.extend(&0_i32.to_be_bytes()); // frame length, corrected below
        buf.extend(&0x20000_i32.to_be_bytes()); // protocol version two
        buf.extend(b"user\0ignored\0\0"); // dummy user parameter
        let len: i32 = buf.len().try_into()?;
        buf[0..4].copy_from_slice(&len.to_be_bytes());
        stream.write_all(&buf)?;

        // Verify the server sends back an error and closes the connection.
        buf.clear();
        stream.read_to_end(&mut buf)?;
        let message = Message::parse(&mut BytesMut::from(&*buf))?;
        let error = match message {
            Some(Message::ErrorResponse(error)) => error,
            _ => panic!("did not receive expected error response"),
        };
        let mut fields: Vec<_> = error
            .fields()
            .map(|f| Ok((f.type_(), f.value().to_owned())))
            .collect()?;
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

    Ok(())
}

#[test]
fn test_conn_user() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let server = util::start_server(util::Config::default())?;
    let mut client = server.connect(postgres::NoTls)?;

    // Attempting to connect as a nonexistent user should fail.
    let err = server
        .pg_config()
        .user("rj")
        .connect(postgres::NoTls)
        .unwrap_db_error();
    assert_eq!(err.severity(), "FATAL");
    assert_eq!(*err.code(), SqlState::INVALID_AUTHORIZATION_SPECIFICATION);
    assert_eq!(err.message(), "role \"rj\" does not exist");
    assert_eq!(
        err.hint(),
        Some("Try connecting as the \"materialize\" user.")
    );

    // But should succeed after that user comes into existence.
    client.batch_execute("CREATE ROLE rj LOGIN SUPERUSER")?;
    let mut client = server.pg_config().user("rj").connect(postgres::NoTls)?;
    let row = client.query_one("SELECT current_user", &[])?;
    assert_eq!(row.get::<_, String>(0), "rj");

    Ok(())
}

#[test]
fn test_simple_query_no_hang() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let server = util::start_server(util::Config::default())?;
    let mut client = server.connect(postgres::NoTls)?;
    assert!(client.simple_query("asdfjkl;").is_err());
    // This will hang if #2880 is not fixed.
    assert!(client.simple_query("SELECT 1").is_ok());

    Ok(())
}

#[test]
fn test_copy() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let server = util::start_server(util::Config::default())?;
    let mut client = server.connect(postgres::NoTls)?;

    // Ensure empty COPY result sets work. We used to mishandle this with binary
    // COPY.
    {
        let tail = BinaryCopyOutIter::new(
            client.copy_out("COPY (SELECT 1 WHERE FALSE) TO STDOUT (FORMAT BINARY)")?,
            &[Type::INT4],
        );
        assert_eq!(tail.count()?, 0);

        let mut buf = String::new();
        client
            .copy_out("COPY (SELECT 1 WHERE FALSE) TO STDOUT")?
            .read_to_string(&mut buf)?;
        assert_eq!(buf, "");
    }

    // Test basic, non-empty COPY.
    {
        let tail = BinaryCopyOutIter::new(
            client.copy_out("COPY (VALUES (NULL, 2), (E'\t', 4)) TO STDOUT (FORMAT BINARY)")?,
            &[Type::TEXT, Type::INT4],
        );
        let rows: Vec<(Option<String>, Option<i32>)> =
            tail.map(|row| Ok((row.get(0), row.get(1)))).collect()?;
        assert_eq!(rows, &[(None, Some(2)), (Some("\t".into()), Some(4))]);

        let mut buf = String::new();
        client
            .copy_out("COPY (VALUES (NULL, 2), (E'\t', 4)) TO STDOUT")?
            .read_to_string(&mut buf)?;
        assert_eq!(buf, "\\N\t2\n\\t\t4\n");
    }

    Ok(())
}

#[test]
fn test_arrays() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let server = util::start_server(util::Config::default().unsafe_mode())?;
    let mut client = server.connect(postgres::NoTls)?;

    let row = client.query_one("SELECT ARRAY[ARRAY[1], ARRAY[NULL::int], ARRAY[2]]", &[])?;
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
        .simple_query("SELECT ARRAY[ARRAY[1], ARRAY[NULL::int], ARRAY[2]]")?
        .into_first();
    match message {
        SimpleQueryMessage::Row(row) => {
            assert_eq!(row.get(0).unwrap(), "{{1},{NULL},{2}}");
        }
        _ => panic!("unexpected simple query message"),
    }

    let message = client
        .simple_query("SELECT ARRAY[ROW(1,2), ROW(3,4), ROW(5,6)]")?
        .into_first();
    match message {
        SimpleQueryMessage::Row(row) => {
            assert_eq!(row.get(0).unwrap(), r#"{"(1,2)","(3,4)","(5,6)"}"#);
        }
        _ => panic!("unexpected simple query message"),
    }

    Ok(())
}

#[test]
fn test_record_types() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let server = util::start_server(util::Config::default())?;
    let mut client = server.connect(postgres::NoTls)?;

    let row = client.query_one("SELECT ROW()", &[])?;
    let _: Record<()> = row.get(0);

    let row = client.query_one("SELECT ROW(1)", &[])?;
    let record: Record<(i32,)> = row.get(0);
    assert_eq!(record, Record((1,)));

    let row = client.query_one("SELECT (1, (2, 3))", &[])?;
    let record: Record<(i32, Record<(i32, i32)>)> = row.get(0);
    assert_eq!(record, Record((1, Record((2, 3)))));

    let row = client.query_one("SELECT (1, 'a')", &[])?;
    let record: Record<(i32, String)> = row.get(0);
    assert_eq!(record, Record((1, "a".into())));

    client.batch_execute("CREATE TYPE named_composite AS (a int, b text)")?;
    let row = client.query_one("SELECT ROW(321, '123')::named_composite", &[])?;
    let record: Record<(i32, String)> = row.get(0);
    assert_eq!(record, Record((321, "123".into())));

    client.batch_execute("CREATE TABLE has_named_composites (f named_composite)")?;
    client.batch_execute(
        "INSERT INTO has_named_composites (f) VALUES ((10, '10')), ((20, '20')::named_composite)",
    )?;
    let rows = client.query(
        "SELECT f FROM has_named_composites ORDER BY (f).a DESC",
        &[],
    )?;
    let record: Record<(i32, String)> = rows[0].get(0);
    assert_eq!(record, Record((20, "20".into())));
    let record: Record<(i32, String)> = rows[1].get(0);
    assert_eq!(record, Record((10, "10".into())));
    assert_eq!(rows.len(), 2);

    Ok(())
}

fn pg_test_inner(dir: PathBuf) -> Result<(), Box<dyn Error>> {
    // We want a new server per file, so we can't use pgtest::walk.
    datadriven::walk(dir.to_str().unwrap(), |tf| {
        let server = util::start_server(util::Config::default().unsafe_mode()).unwrap();
        let config = server.pg_config();
        let addr = match &config.get_hosts()[0] {
            tokio_postgres::config::Host::Tcp(host) => {
                format!("{}:{}", host, config.get_ports()[0])
            }
            _ => panic!("only tcp connections supported"),
        };
        let user = config.get_user().unwrap();
        let timeout = Duration::from_secs(5);

        mz_pgtest::run_test(tf, addr, user.to_string(), timeout);
    });

    Ok(())
}

#[test]
fn test_pgtest() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let dir: PathBuf = ["..", "..", "test", "pgtest"].iter().collect();
    pg_test_inner(dir)
}

#[test]
// Materialize's differences from Postgres' responses.
fn test_pgtest_mz() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let dir: PathBuf = ["..", "..", "test", "pgtest-mz"].iter().collect();
    pg_test_inner(dir)
}
