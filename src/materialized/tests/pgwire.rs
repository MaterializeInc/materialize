// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for pgwire functionality.

use std::error::Error;
use std::path::Path;
use std::thread;
use std::time::Duration;

use pgrepr::{Numeric, Record};
use repr::adt::decimal::Significand;

use futures::stream::{self, StreamExt, TryStreamExt};
use openssl::ssl::{SslConnector, SslConnectorBuilder, SslMethod, SslVerifyMode};
use postgres::config::SslMode;
use postgres::error::SqlState;
use postgres::types::Type;
use postgres_openssl::MakeTlsConnector;
use tokio::runtime::Runtime;

pub mod util;

#[test]
fn test_bind_params() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let (_server, mut client) = util::start_server(util::Config::default())?;

    // Simple queries with parameters should be rejected.
    match client.simple_query("SELECT $1") {
        Ok(_) => panic!("query with invalid parameters executed successfully"),
        Err(err) => assert_eq!(err.code(), Some(&SqlState::UNDEFINED_PARAMETER)),
    }

    let val: String = client
        .query_one("SELECT $1", &[&String::from("42")])?
        .get(0);
    assert_eq!(val, "42");

    let val: i32 = client.query_one("SELECT $1 + 1", &[&42_i32])?.get(0);
    assert_eq!(val, 43);

    let val: i32 = client.query_one("SELECT $1 - 1", &[&42_i32])?.get(0);
    assert_eq!(val, 41);

    let val: i32 = client.query_one("SELECT 1 - $1", &[&42_i32])?.get(0);
    assert_eq!(val, -41);

    match client.query("SELECT ROW(1, 2) = $1", &[&42_i32]) {
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
        let num = Numeric(Significand::new(123).with_scale(2));
        let stmt = client.prepare_typed("SELECT $1 + 1.23", &[Type::NUMERIC])?;
        let val: Numeric = client.query_one(&stmt, &[&num])?.get(0);
        assert_eq!(val.to_string(), "2.46");
    }

    // A `CREATE` statement with parameters should be rejected.
    match client.query_one("CREATE VIEW v AS SELECT $3", &[]) {
        Ok(_) => panic!("query with invalid parameters executed successfully"),
        Err(err) => {
            assert!(err.to_string().contains("there is no parameter $3"));
            // TODO(benesch): this should be `UNDEFINED_PARAMETER`, but blocked
            // on #3147.
            assert_eq!(err.code(), Some(&SqlState::INTERNAL_ERROR));
        }
    }

    Ok(())
}

#[test]
fn test_partial_read() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let (_server, mut client) = util::start_server(util::Config::default())?;
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
    ore::test::init_logging();

    let (_server, mut client) = util::start_server(util::Config::default())?;
    let query = "VALUES (1), (2), (3)";

    let max_rows = 10_000;
    let mut trans = client.transaction()?;
    let portal = trans.bind(query, &[])?;
    let rows = trans.query_portal(&portal, max_rows)?;

    assert_eq!(rows.len(), 3, "row len should be all values");

    Ok(())
}

#[test]
fn test_conn_params() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let (server, mut client) = util::start_server(util::Config::default())?;

    // The default database should be `materialize`.
    assert_eq!(
        client.query_one("SHOW database", &[])?.get::<_, String>(0),
        "materialize",
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
        let (notice_tx, mut notice_rx) = futures::channel::mpsc::unbounded();
        tokio::spawn(
            stream::poll_fn(move |cx| conn.poll_message(cx))
                .map_err(|e| panic!(e))
                .forward(notice_tx),
        );

        assert_eq!(
            client
                .query_one("SHOW database", &[])
                .await?
                .get::<_, String>(0),
            "newdb",
        );
        client
            .batch_execute(
                "CREATE DATABASE newdb; \
                 CREATE MATERIALIZED VIEW v AS SELECT 1;",
            )
            .await?;

        match notice_rx.next().await {
            Some(tokio_postgres::AsyncMessage::Notice(n)) => {
                assert_eq!(*n.code(), SqlState::SUCCESSFUL_COMPLETION);
                assert_eq!(n.message(), "session database \'newdb\' does not exist");
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

        // Sleep a little bit so the view catches up.
        // TODO(benesch): seriously? It's a view over a static query.
        // HISTORICAL NOTE(brennan): The above comment was left when this was only 500 millis.
        // WTF is going on here?
        thread::sleep(Duration::from_millis(2000));

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

    Ok(())
}

#[test]
fn test_multiple_statements() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();
    let (_server, mut client) = util::start_server(util::Config::default())?;
    let result = client.batch_execute(
        "CREATE VIEW v1 AS SELECT * FROM (VALUES (1)); \
         CREATE VIEW v2 AS SELECT * FROM (VALUES (2)); \
         CREATE VIEW v3 AS SELECT sum(column1) FROM (SELECT * FROM v1 UNION SELECT * FROM v2); \
         CREATE VIEW v4 AS SELECT * FROM nonexistent; \
         CREATE VIEW v5 AS SELECT 5;",
    );

    // v4 creation fails, so the whole query should be an error.
    assert!(result.is_err());

    assert_eq!(
        client.query_one("SELECT * FROM v1", &[])?.get::<_, i32>(0),
        1,
    );

    assert_eq!(
        client.query_one("SELECT * FROM v2", &[])?.get::<_, i32>(0),
        2,
    );

    assert_eq!(
        client.query_one("SELECT * FROM v3", &[])?.get::<_, i64>(0),
        3,
    );

    assert!(client.query_one("SELECT * FROM v4", &[]).is_err());

    // the statement to create v5 is correct, but it should not have been executed, since v4 failed to create.
    assert!(client.query_one("SELECT * from v5", &[]).is_err());

    Ok(())
}

#[test]
fn test_simple_query_no_hang() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let (_server, mut client) = util::start_server(util::Config::default())?;
    assert!(client.simple_query("asdfjkl;").is_err());
    // This will hang if #2880 is not fixed.
    assert!(client.simple_query("SELECT 1").is_ok());

    Ok(())
}

#[test]
fn test_tls() -> Result<(), Box<dyn Error>> {
    fn make_tls<F>(configure: F) -> Result<MakeTlsConnector, Box<dyn Error>>
    where
        F: FnOnce(&mut SslConnectorBuilder) -> Result<(), openssl::error::ErrorStack>,
    {
        let mut connector_builder = SslConnector::builder(SslMethod::tls())?;
        configure(&mut connector_builder)?;
        Ok(MakeTlsConnector::new(connector_builder.build()))
    }

    let make_nonverifying_tls = || {
        make_tls(|b| {
            b.set_verify(SslVerifyMode::NONE);
            Ok(())
        })
    };

    let smoke_test = |mut client: postgres::Client| -> Result<(), Box<dyn Error>> {
        assert_eq!(client.query_one("SELECT 1", &[])?.get::<_, i32>(0), 1);
        Ok(())
    };

    // Test TLS modes with a server that does not support TLS.
    {
        let config = util::Config::default();
        let (server, _client) = util::start_server(config)?;

        // Explicitly disabling TLS should succeed.
        smoke_test(
            server
                .pg_config()
                .ssl_mode(SslMode::Disable)
                .connect(make_nonverifying_tls()?)?,
        )?;

        // Preferring TLS should fall back to no TLS.
        smoke_test(
            server
                .pg_config()
                .ssl_mode(SslMode::Prefer)
                .connect(make_nonverifying_tls()?)?,
        )?;

        // Requiring TLS should fail.
        match server
            .pg_config()
            .ssl_mode(SslMode::Require)
            .connect(make_nonverifying_tls()?)
        {
            Ok(_) => panic!("expected connection to fail"),
            Err(e) => assert_eq!(
                e.to_string(),
                "error performing TLS handshake: server does not support TLS"
            ),
        }
    }

    // Test TLS modes with a server that does support TLS.
    {
        let temp_dir = tempfile::tempdir()?;
        let cert_path = Path::join(temp_dir.path(), "server.crt");
        let key_path = Path::join(temp_dir.path(), "server.key");
        util::generate_certs(&cert_path, &key_path)?;

        let config = util::Config::default().enable_tls(cert_path.clone(), key_path);
        let (server, _client) = util::start_server(config)?;

        // Disabling TLS should succeed.
        smoke_test(
            server
                .pg_config()
                .ssl_mode(SslMode::Disable)
                .connect(make_nonverifying_tls()?)?,
        )?;

        // Preferring TLS should succeed.
        smoke_test(
            server
                .pg_config()
                .ssl_mode(SslMode::Prefer)
                .connect(make_nonverifying_tls()?)?,
        )?;

        // Requiring TLS should succeed.
        smoke_test(
            server
                .pg_config()
                .ssl_mode(SslMode::Require)
                .connect(make_nonverifying_tls()?)?,
        )?;

        // Requiring TLS with server identity verification should succeed when
        // the server's certificate is set as trusted.
        smoke_test(
            server
                .pg_config()
                .ssl_mode(SslMode::Require)
                .connect(make_tls(|b| b.set_ca_file(&cert_path))?)?,
        )?;

        // Requiring TLS with server identity verification should fail when the
        // server's certificate is not trusted.
        match server
            .pg_config()
            .ssl_mode(SslMode::Require)
            .connect(make_tls(|_| Ok(()))?)
        {
            Ok(_) => panic!("expected connection to fail"),
            Err(e) => assert!(e.to_string().contains("certificate verify failed")),
        }
    }

    Ok(())
}

#[test]
fn test_record_types() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let (_server, mut client) = util::start_server(util::Config::default())?;

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

    Ok(())
}

#[test]
fn test_pgtest() -> Result<(), Box<dyn Error>> {
    ore::test::init_logging();

    let dir: std::path::PathBuf = ["..", "..", "test", "pgtest"].iter().collect();

    // We want a new server per file, so we can't use pgtest::walk.
    datadriven::walk(dir.to_str().unwrap(), |tf| {
        let (server, _client) = util::start_server(util::Config::default()).unwrap();
        let config = server.pg_config();
        let addr = match &config.get_hosts()[0] {
            tokio_postgres::config::Host::Tcp(host) => {
                format!("{}:{}", host, config.get_ports()[0])
            }
            _ => panic!("only tcp connections supported"),
        };
        let user = config.get_user().unwrap();
        let timeout = std::time::Duration::from_secs(5);

        pgtest::run_test(tf, &addr, user, timeout);
    });

    Ok(())
}
