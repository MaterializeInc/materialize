// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for pgwire functionality.

#![recursion_limit = "256"]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use itertools::Itertools;
use mz_adapter::session::DEFAULT_DATABASE_NAME;
use mz_environmentd::test_util::{self, PostgresErrorExt};
use mz_ore::cast::CastLossy;
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
#[allow(clippy::disallowed_methods)] // Compare SQL preparation with pgwire Parse directly.
fn test_sql_prepare_inferred_parameter_error_timing() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut system = server.connect_internal(postgres::NoTls).unwrap();
    for reuse in [false, true] {
        system
            .batch_execute(&format!(
                "ALTER SYSTEM SET enable_prepared_query_reuse = {reuse}"
            ))
            .unwrap();
        let mut client = server.connect(postgres::NoTls).unwrap();
        for (name, query, error) in [
            (
                "param_left",
                "SELECT $1 = ROW(1, 2)",
                "operator does not exist: text = record(f1: integer,f2: integer)",
            ),
            (
                "param_right",
                "SELECT ROW(1, 2) = $1",
                "operator does not exist: record(f1: integer,f2: integer) = text",
            ),
        ] {
            client
                .batch_execute(&format!("PREPARE {name} AS {query}"))
                .unwrap();
            let stmt = client.prepare(query).unwrap();
            assert_eq!(stmt.params(), &[Type::TEXT]);
            for value in ["(1,2)", "(1,2,3)"] {
                assert_eq!(
                    client
                        .simple_query(&format!("EXECUTE {name} ('{value}')"))
                        .unwrap_db_error()
                        .message(),
                    error,
                    "SQL PREPARE with reuse={reuse}"
                );
                assert_eq!(
                    client.query(&stmt, &[&value]).unwrap_db_error().message(),
                    error,
                    "pgwire Parse with reuse={reuse}"
                );
            }
        }
    }
}

#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
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

    // Test that `varchar_to_text` is properly handled (whether eliminated or no).
    {
        client.batch_execute("CREATE TABLE v (w varchar)").unwrap();
        client.query("INSERT INTO v VALUES ($1)", &[&"aa"]).unwrap();
        let stmt = client
            .prepare("SELECT w, w::text, w::text::varchar FROM v WHERE w = $1")
            .unwrap();
        assert_eq!(stmt.columns().len(), 3);
        assert_eq!(stmt.columns()[0].type_(), &Type::VARCHAR);
        assert_eq!(stmt.columns()[1].type_(), &Type::TEXT);
        assert_eq!(stmt.columns()[2].type_(), &Type::VARCHAR);
        let row = client.query_one(&stmt, &[&"aa"]).unwrap();
        assert_eq!(row.get::<_, String>(0), "aa");
        assert_eq!(row.get::<_, String>(1), "aa");
        assert_eq!(row.get::<_, String>(2), "aa");

        let stmt = client
            .prepare_typed("SELECT 'aa'::varchar::text = $1", &[Type::TEXT])
            .unwrap();
        let val: bool = client.query_one(&stmt, &[&"aa"]).unwrap().get(0);
        assert_eq!(val, true);
    }
}

#[mz_ore::test]
#[allow(clippy::disallowed_methods)] // Exercise the client's SQL protocol directly.
fn test_reused_prepared_select_context() {
    for frontend in [true, false] {
        let server = test_util::TestHarness::default()
            .with_system_parameter_default(
                "enable_frontend_peek_sequencing".into(),
                frontend.to_string(),
            )
            .start_blocking();
        let mut client = server.connect(postgres::NoTls).unwrap();
        for sql in [
            "CREATE TABLE prepared_rows (id int, value text)",
            "INSERT INTO prepared_rows VALUES (1, 'one'), (2, 'two'), (3, 'three')",
            "CREATE INDEX prepared_rows_id ON prepared_rows (id)",
        ] {
            client.batch_execute(sql).unwrap();
        }
        let stmt = client
            .prepare("SELECT id, value || $2 FROM prepared_rows WHERE id = $1 ORDER BY id LIMIT $3")
            .unwrap();
        client.query_one(&stmt, &[&1_i32, &"", &1_i64]).unwrap();
        let optimizer_count = || -> u64 {
            server
                .metrics_registry()
                .gather()
                .iter()
                .filter(|family| family.name() == "mz_optimizer_e2e_optimization_time_seconds")
                .flat_map(|family| family.get_metric())
                .filter(|metric| {
                    metric.get_label().iter().any(|label| {
                        label.name() == "object_type" && label.value().starts_with("peek:")
                    })
                })
                .map(|metric| metric.get_histogram().get_sample_count())
                .sum()
        };
        let prepared_count = |event: &str| -> u64 {
            server
                .metrics_registry()
                .gather()
                .iter()
                .filter(|family| family.name() == "mz_prepared_query_events_total")
                .flat_map(|family| family.get_metric())
                .filter(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .any(|label| label.name() == "event" && label.value() == event)
                })
                .map(|metric| u64::cast_lossy(metric.get_counter().value()))
                .sum()
        };
        let compilation_counts =
            || ["analysis", "custom_bind", "template_compile"].map(&prepared_count);
        let sql_compiler_counts = || {
            ["resolve", "describe", "plan", "root_query", "parse"].map(|phase| -> u64 {
                server
                    .metrics_registry()
                    .gather()
                    .iter()
                    .filter(|family| family.name() == "mz_sql_compiler_calls_total")
                    .flat_map(|family| family.get_metric())
                    .filter(|metric| {
                        metric
                            .get_label()
                            .iter()
                            .any(|label| label.name() == "phase" && label.value() == phase)
                    })
                    .map(|metric| u64::cast_lossy(metric.get_counter().value()))
                    .sum()
            })
        };
        let before_inferred = sql_compiler_counts();
        let inferred = client.prepare("SELECT $1::int4").unwrap();
        let after_inferred = sql_compiler_counts();
        assert_eq!(after_inferred[1], before_inferred[1] + 1);
        assert_eq!(after_inferred[3], before_inferred[3] + 2);
        let typed = client
            .prepare_typed("SELECT $1::int4", &[postgres::types::Type::INT4])
            .unwrap();
        let after_typed = sql_compiler_counts();
        assert_eq!(after_typed[1], after_inferred[1] + 1);
        assert_eq!(after_typed[3], after_inferred[3] + 1);
        drop((inferred, typed));
        let before = optimizer_count();
        let before_compilation = compilation_counts();
        let before_sql_compiler = sql_compiler_counts();
        assert!(before_sql_compiler.iter().all(|count| *count > 0));
        let before_hits = prepared_count("template_hit");
        let before_generic = prepared_count("generic_execute");
        for (key, value) in [(1_i32, "one"), (3, "three"), (2, "two"), (1, "one")] {
            let suffix = format!("-{key}");
            let row = client.query_one(&stmt, &[&key, &suffix, &1_i64]).unwrap();
            assert_eq!(row.get::<_, i32>(0), key);
            assert_eq!(row.get::<_, String>(1), format!("{value}{suffix}"));
        }
        assert!(
            client
                .query(&stmt, &[&None::<i32>, &"", &1_i64])
                .unwrap()
                .is_empty()
        );
        assert!(
            client
                .query(&stmt, &[&1_i32, &"", &0_i64])
                .unwrap()
                .is_empty()
        );
        let after = optimizer_count();
        assert_eq!(sql_compiler_counts(), before_sql_compiler);
        if frontend {
            assert!(
                before_compilation[2] > 0,
                "the template compiler was exercised"
            );
            assert_eq!(compilation_counts(), before_compilation);
            assert_eq!(prepared_count("template_hit"), before_hits + 6);
            assert_eq!(prepared_count("generic_execute"), before_generic + 6);
            assert_eq!(
                after, before,
                "warm changing-parameter executions must skip the optimizer"
            );
        } else {
            assert!(
                after >= before + 6,
                "coordinator custom path must exercise the optimizer metric"
            );
        }
        client
            .batch_execute("PREPARE prepared_sql AS SELECT value FROM prepared_rows WHERE id = $1")
            .unwrap();
        client.query_one("EXECUTE prepared_sql(1)", &[]).unwrap();
        let before = optimizer_count();
        let before_compilation = compilation_counts();
        let before_sql_compiler = sql_compiler_counts();
        let before_generic = prepared_count("generic_execute");
        for (key, value) in [(2, "two"), (1, "one"), (3, "three")] {
            let row = client
                .query_one(&format!("EXECUTE prepared_sql({key})"), &[])
                .unwrap();
            assert_eq!(row.get::<_, String>(0), value);
        }
        let after_sql_compiler = sql_compiler_counts();
        assert_eq!(after_sql_compiler[3], before_sql_compiler[3]);
        assert!(after_sql_compiler[4] > before_sql_compiler[4]);
        assert!(
            after_sql_compiler[..3]
                .iter()
                .zip_eq(&before_sql_compiler[..3])
                .all(|(after, before)| after > before),
            "SQL EXECUTE compiles its wrapper, but not the prepared SELECT body"
        );
        if frontend {
            assert_eq!(compilation_counts(), before_compilation);
            assert_eq!(prepared_count("generic_execute"), before_generic + 3);
            assert_eq!(
                optimizer_count(),
                before,
                "SQL EXECUTE must share the reusable program path"
            );
        }

        for (ddl, has_index) in [
            ("DROP INDEX prepared_rows_id", false),
            ("CREATE INDEX prepared_rows_id ON prepared_rows (id)", true),
        ] {
            client.batch_execute(ddl).unwrap();
            let before_generic = prepared_count("generic_execute");
            let row = client.query_one(&stmt, &[&2_i32, &"-new", &1_i64]).unwrap();
            assert_eq!(row.get::<_, i32>(0), 2);
            assert_eq!(row.get::<_, String>(1), "two-new");
            assert_eq!(
                prepared_count("generic_execute") - before_generic,
                u64::from(frontend && has_index),
                "index removal must fall back, and its replacement must be usable"
            );
        }

        for sql in [
            "CREATE SCHEMA first_path",
            "CREATE SCHEMA second_path",
            "CREATE VIEW first_path.prepared_view AS SELECT 11 AS value",
            "CREATE VIEW second_path.prepared_view AS SELECT 22 AS value",
            "SET search_path = first_path",
        ] {
            client.batch_execute(sql).unwrap();
        }
        let path_stmt = client.prepare("SELECT value FROM prepared_view").unwrap();
        assert_eq!(
            client.query_one(&path_stmt, &[]).unwrap().get::<_, i32>(0),
            11
        );
        client
            .batch_execute("BEGIN; SET LOCAL search_path = second_path")
            .unwrap();
        assert_eq!(
            client.query_one(&path_stmt, &[]).unwrap().get::<_, i32>(0),
            22
        );
        client.batch_execute("ROLLBACK").unwrap();
        assert_eq!(
            client.query_one(&path_stmt, &[]).unwrap().get::<_, i32>(0),
            11
        );
        client
            .batch_execute("DROP VIEW first_path.prepared_view")
            .unwrap();
        client
            .batch_execute("CREATE VIEW first_path.prepared_view AS SELECT 33 AS value")
            .unwrap();
        assert_eq!(
            client.query_one(&path_stmt, &[]).unwrap().get::<_, i32>(0),
            33
        );
        client
            .batch_execute("DROP VIEW first_path.prepared_view")
            .unwrap();
        client
            .batch_execute("CREATE VIEW first_path.prepared_view AS SELECT true AS value")
            .unwrap();
        let err = client.query_one(&path_stmt, &[]).unwrap_db_error();
        assert_eq!(err.message(), "cached plan must not change result type");

        let mut system = server.connect_internal(postgres::NoTls).unwrap();
        system
            .batch_execute("ALTER SYSTEM SET enable_prepared_query_templates = false")
            .unwrap();
        let custom = client
            .prepare("SELECT value FROM public.prepared_rows WHERE id = $1")
            .unwrap();
        client.query_one(&custom, &[&1_i32]).unwrap();
        let before_custom = sql_compiler_counts();
        let before_optimizer = optimizer_count();
        let before_generic = prepared_count("generic_execute");
        let before_custom_bind = prepared_count("custom_bind");
        for (key, value) in [(2_i32, "two"), (3, "three")] {
            assert_eq!(
                client
                    .query_one(&custom, &[&key])
                    .unwrap()
                    .get::<_, String>(0),
                value
            );
        }
        assert_eq!(sql_compiler_counts(), before_custom);
        assert_eq!(prepared_count("generic_execute"), before_generic);
        assert_eq!(prepared_count("custom_bind"), before_custom_bind + 2);
        assert!(optimizer_count() >= before_optimizer + 2);
        system
            .batch_execute("ALTER SYSTEM SET enable_prepared_query_reuse = false")
            .unwrap();
        let uncached = client
            .prepare("SELECT value FROM public.prepared_rows WHERE id = $1")
            .unwrap();
        client.query_one(&uncached, &[&1_i32]).unwrap();
        let before_uncached = sql_compiler_counts();
        let before_optimizer = optimizer_count();
        for (key, value) in [(2_i32, "two"), (3, "three")] {
            assert_eq!(
                client
                    .query_one(&uncached, &[&key])
                    .unwrap()
                    .get::<_, String>(0),
                value
            );
        }
        let after_uncached = sql_compiler_counts();
        assert_eq!(after_uncached[4], before_uncached[4]);
        assert!(
            after_uncached[..4]
                .iter()
                .zip_eq(&before_uncached[..4])
                .all(|(after, before)| after > before),
            "the disabled control must exercise resolution, description and both planners"
        );
        assert!(optimizer_count() >= before_optimizer + 2);
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
#[allow(clippy::disallowed_methods)] // Exercise the client's SQL protocol directly.
fn test_prepared_indexed_portal_lifetimes() {
    use postgres_protocol::IsNull;
    use postgres_protocol::message::{backend::Message, frontend};

    fn exchange(stream: &mut TcpStream, buf: &mut BytesMut) -> (Vec<i32>, usize) {
        stream.write_all(buf).unwrap();
        buf.clear();
        let mut rows = Vec::new();
        let mut suspended = 0;
        for message in read_until_ready(stream) {
            match message {
                Message::ErrorResponse(body) => {
                    let fields: Vec<_> = body
                        .fields()
                        .map(|field| Ok(String::from_utf8_lossy(field.value_bytes()).into_owned()))
                        .collect()
                        .unwrap();
                    panic!("unexpected error response: {fields:?}");
                }
                Message::DataRow(body) => {
                    let field = body.ranges().next().unwrap().unwrap().unwrap();
                    rows.push(
                        std::str::from_utf8(&body.buffer()[field])
                            .unwrap()
                            .parse()
                            .unwrap(),
                    );
                }
                Message::PortalSuspended => suspended += 1,
                _ => (),
            }
        }
        (rows, suspended)
    }

    fn bind(
        buf: &mut BytesMut,
        portal: &str,
        statement: &str,
        key: i32,
        addend: i32,
        binary: bool,
    ) {
        frontend::bind(
            portal,
            statement,
            [i16::from(binary)],
            [key, addend],
            |value, buf| {
                if binary {
                    buf.put_i32(value);
                } else {
                    buf.put_slice(value.to_string().as_bytes());
                }
                Ok(IsNull::No)
            },
            [],
            buf,
        )
        .unwrap_or_else(|_| panic!("failed to encode Bind"));
    }

    for reuse in [true, false] {
        let server = test_util::TestHarness::default()
            .with_system_parameter_default("enable_prepared_query_reuse".into(), reuse.to_string())
            .start_blocking();
        let mut client = server.connect(postgres::NoTls).unwrap();
        for sql in [
            "CREATE TABLE portal_rows (lookup int, value int)",
            "INSERT INTO portal_rows VALUES (1, 11), (1, 12), (1, 13), (2, 21), (2, 22)",
            "CREATE INDEX portal_rows_lookup ON portal_rows (lookup)",
        ] {
            client.batch_execute(sql).unwrap();
        }
        let compiled_entries = || -> u64 {
            server
                .metrics_registry()
                .gather()
                .iter()
                .filter(|family| family.name() == "mz_prepared_query_cache_compiled_entries")
                .flat_map(|family| family.get_metric())
                .map(|metric| u64::cast_lossy(metric.get_gauge().value()))
                .sum()
        };
        let query = "SELECT value + $2 FROM portal_rows WHERE lookup = $1 ORDER BY value";
        let mut stream = TcpStream::connect(server.sql_local_addr()).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(120)))
            .unwrap();
        let mut buf = BytesMut::new();
        frontend::startup_message(
            [
                ("user", "materialize"),
                ("database", DEFAULT_DATABASE_NAME),
                ("options", "--welcome_message=off"),
            ],
            &mut buf,
        )
        .unwrap();
        exchange(&mut stream, &mut buf);
        frontend::query("BEGIN", &mut buf).unwrap();
        exchange(&mut stream, &mut buf);
        frontend::parse("q", query, [23, 23], &mut buf).unwrap();
        bind(&mut buf, "first", "q", 1, 0, true);
        bind(&mut buf, "second", "q", 2, 100, false);
        bind(&mut buf, "third", "q", 1, 1000, true);
        frontend::execute("first", 1, &mut buf).unwrap();
        frontend::sync(&mut buf);
        assert_eq!(exchange(&mut stream, &mut buf), (vec![11], 1));
        assert_eq!(compiled_entries(), u64::from(reuse));
        frontend::close(b'S', "q", &mut buf).unwrap();
        frontend::sync(&mut buf);
        exchange(&mut stream, &mut buf);
        assert_eq!(
            compiled_entries(),
            u64::from(reuse),
            "bound portals retain their analysis"
        );
        frontend::execute("second", 1, &mut buf).unwrap();
        frontend::sync(&mut buf);
        assert_eq!(exchange(&mut stream, &mut buf), (vec![121], 1));
        frontend::query("DEALLOCATE ALL", &mut buf).unwrap();
        exchange(&mut stream, &mut buf);
        assert_eq!(compiled_entries(), 0);
        frontend::execute("third", 1, &mut buf).unwrap();
        frontend::sync(&mut buf);
        assert_eq!(exchange(&mut stream, &mut buf), (vec![1011], 1));
        assert_eq!(
            compiled_entries(),
            u64::from(reuse),
            "an unexecuted portal may recompile after eviction"
        );
        for (portal, expected) in [
            ("first", vec![12, 13]),
            ("second", vec![122]),
            ("third", vec![1012, 1013]),
        ] {
            frontend::execute(portal, 0, &mut buf).unwrap();
            frontend::sync(&mut buf);
            assert_eq!(exchange(&mut stream, &mut buf), (expected, 0));
        }
        frontend::query("ROLLBACK", &mut buf).unwrap();
        exchange(&mut stream, &mut buf);
        assert_eq!(
            compiled_entries(),
            0,
            "rollback destroys orphaned portal programs"
        );

        frontend::parse("q", query, [23, 23], &mut buf).unwrap();
        bind(&mut buf, "", "q", 2, 0, true);
        frontend::execute("", 0, &mut buf).unwrap();
        frontend::sync(&mut buf);
        assert_eq!(exchange(&mut stream, &mut buf), (vec![21, 22], 0));
        assert_eq!(compiled_entries(), u64::from(reuse));
        frontend::query("DISCARD ALL", &mut buf).unwrap();
        exchange(&mut stream, &mut buf);
        assert_eq!(compiled_entries(), 0);
        bind(&mut buf, "", "q", 1, 0, true);
        frontend::sync(&mut buf);
        stream.write_all(&buf).unwrap();
        let errors: Vec<_> = read_until_ready(&mut stream)
            .into_iter()
            .filter_map(|message| {
                let Message::ErrorResponse(body) = message else {
                    return None;
                };
                let code = body
                    .fields()
                    .find(|field| Ok(field.type_() == b'C'))
                    .unwrap()
                    .unwrap();
                Some(std::str::from_utf8(code.value_bytes()).unwrap().to_owned())
            })
            .collect();
        assert_eq!(errors, [SqlState::INVALID_SQL_STATEMENT_NAME.code()]);
    }
}

#[mz_ore::test]
#[allow(clippy::disallowed_methods)] // Exercise the client's SQL protocol directly.
fn test_prepared_indexed_execution_context() {
    let server = test_util::TestHarness::default()
        .with_system_parameter_default("enable_rbac_checks".into(), "true".into())
        .start_blocking();
    let mut admin = server.connect(postgres::NoTls).unwrap();
    for sql in [
        "CREATE ROLE prepared_reader",
        "CREATE TABLE prepared_context (id int, value int)",
        "INSERT INTO prepared_context VALUES (1, 11)",
        "CREATE INDEX prepared_context_id ON prepared_context (id)",
        "GRANT SELECT ON prepared_context TO prepared_reader",
    ] {
        admin.batch_execute(sql).unwrap();
    }
    let mut system = server.connect_internal(postgres::NoTls).unwrap();
    for sql in [
        "GRANT USAGE ON DATABASE materialize TO prepared_reader",
        "GRANT USAGE ON SCHEMA public TO prepared_reader",
    ] {
        system.batch_execute(sql).unwrap();
    }
    let cluster: String = admin.query_one("SHOW cluster", &[]).unwrap().get(0);
    system
        .batch_execute(&format!(
            "GRANT USAGE ON CLUSTER \"{}\" TO prepared_reader",
            cluster.replace('"', "\"\""),
        ))
        .unwrap();
    let mut reader = server
        .pg_config()
        .user("prepared_reader")
        .connect(postgres::NoTls)
        .unwrap();
    let query = "SELECT value, current_user::text, current_timestamp::text, mz_now()::text FROM prepared_context WHERE id = $1";
    let stmt = reader.prepare(query).unwrap();
    let event_count = |event: &str| -> u64 {
        server
            .metrics_registry()
            .gather()
            .iter()
            .filter(|family| family.name() == "mz_prepared_query_events_total")
            .flat_map(|family| family.get_metric())
            .filter(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|label| label.name() == "event" && label.value() == event)
            })
            .map(|metric| u64::cast_lossy(metric.get_counter().value()))
            .sum()
    };
    let first = reader.query_one(&stmt, &[&1_i32]).unwrap();
    assert_eq!(first.get::<_, i32>(0), 11);
    assert_eq!(first.get::<_, String>(1), "prepared_reader");
    let first_time: u64 = first.get::<_, String>(3).parse().unwrap();
    let before_compile = event_count("template_compile");
    let before_generic = event_count("generic_execute");
    assert!(
        before_generic > 0,
        "dynamic expressions must use the template path"
    );
    admin
        .batch_execute("UPDATE prepared_context SET value = 12 WHERE id = 1")
        .unwrap();
    let second = reader.query_one(&stmt, &[&1_i32]).unwrap();
    assert_eq!(second.get::<_, i32>(0), 12);
    assert!(second.get::<_, String>(3).parse::<u64>().unwrap() > first_time);

    reader.batch_execute("BEGIN").unwrap();
    let transaction_first = reader.query_one(&stmt, &[&1_i32]).unwrap();
    admin
        .batch_execute("UPDATE prepared_context SET value = 13 WHERE id = 1")
        .unwrap();
    let transaction_second = reader.query_one(&stmt, &[&1_i32]).unwrap();
    assert_eq!(transaction_second.get::<_, i32>(0), 12);
    assert_eq!(
        transaction_first.get::<_, String>(2),
        transaction_second.get::<_, String>(2)
    );
    assert_eq!(
        transaction_first.get::<_, String>(3),
        transaction_second.get::<_, String>(3)
    );
    reader.batch_execute("ROLLBACK").unwrap();
    assert_eq!(
        reader.query_one(&stmt, &[&1_i32]).unwrap().get::<_, i32>(0),
        13
    );
    assert_eq!(event_count("template_compile"), before_compile);
    assert_eq!(event_count("generic_execute"), before_generic + 4);

    admin
        .batch_execute("REVOKE SELECT ON prepared_context FROM prepared_reader")
        .unwrap();
    let error = reader.query_one(&stmt, &[&1_i32]).unwrap_db_error();
    assert_eq!(error.code(), &SqlState::INSUFFICIENT_PRIVILEGE);
    assert_eq!(event_count("generic_execute"), before_generic + 4);
    admin
        .batch_execute("GRANT SELECT ON prepared_context TO prepared_reader")
        .unwrap();
    assert_eq!(
        reader.query_one(&stmt, &[&1_i32]).unwrap().get::<_, i32>(0),
        13
    );

    let admin_stmt = admin.prepare(query).unwrap();
    assert_eq!(
        admin
            .query_one(&admin_stmt, &[&1_i32])
            .unwrap()
            .get::<_, String>(1),
        "materialize"
    );
}

#[mz_ore::test]
#[allow(clippy::disallowed_methods)] // Retain the same pgwire handle across execution contexts.
fn test_prepared_indexed_cluster_and_transaction_changes() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut system = server.connect_internal(postgres::NoTls).unwrap();
    let mut admin = server.connect(postgres::NoTls).unwrap();
    let original_cluster: String = admin.query_one("SHOW cluster", &[]).unwrap().get(0);
    for sql in [
        "CREATE TABLE prepared_targets (id int, value int)",
        "INSERT INTO prepared_targets VALUES (1, 11), (2, 22)",
        "CREATE INDEX prepared_targets_id ON prepared_targets (id)",
        "CREATE CLUSTER prepared_target SIZE 'scale=1,workers=1'",
    ] {
        admin.batch_execute(sql).unwrap();
    }
    let generic_count = || -> u64 {
        server
            .metrics_registry()
            .gather()
            .iter()
            .filter(|family| family.name() == "mz_prepared_query_events_total")
            .flat_map(|family| family.get_metric())
            .filter(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|label| label.name() == "event" && label.value() == "generic_execute")
            })
            .map(|metric| u64::cast_lossy(metric.get_counter().value()))
            .sum()
    };
    for templates in [false, true] {
        system
            .batch_execute(&format!(
                "ALTER SYSTEM SET enable_prepared_query_templates = {templates}"
            ))
            .unwrap();
        let mut client = server.connect(postgres::NoTls).unwrap();
        let stmt = client
            .prepare("SELECT value / $2 FROM prepared_targets WHERE id = $1")
            .unwrap();
        let before = generic_count();
        assert_eq!(
            client
                .query_one(&stmt, &[&1_i32, &1_i32])
                .unwrap()
                .get::<_, i32>(0),
            11
        );
        assert_eq!(generic_count() - before, u64::from(templates));

        client
            .batch_execute("BEGIN; SET LOCAL cluster = prepared_target")
            .unwrap();
        let before = generic_count();
        assert_eq!(
            client
                .query_one(&stmt, &[&2_i32, &1_i32])
                .unwrap()
                .get::<_, i32>(0),
            22
        );
        assert_eq!(generic_count(), before, "the selected cluster has no index");
        client.batch_execute("ROLLBACK").unwrap();
        assert_eq!(
            client
                .query_one("SHOW cluster", &[])
                .unwrap()
                .get::<_, String>(0),
            original_cluster
        );
        let before = generic_count();
        assert_eq!(
            client
                .query_one(&stmt, &[&1_i32, &1_i32])
                .unwrap()
                .get::<_, i32>(0),
            11
        );
        assert_eq!(generic_count() - before, u64::from(templates));

        admin.batch_execute("CREATE INDEX prepared_targets_other IN CLUSTER prepared_target ON prepared_targets (id)").unwrap();
        client
            .batch_execute("SET cluster = prepared_target")
            .unwrap();
        let before = generic_count();
        assert_eq!(
            client
                .query_one(&stmt, &[&2_i32, &1_i32])
                .unwrap()
                .get::<_, i32>(0),
            22
        );
        assert_eq!(generic_count() - before, u64::from(templates));

        let old_config: String = system
            .query_one("SHOW enable_eager_delta_joins", &[])
            .unwrap()
            .get(0);
        system
            .batch_execute(&format!(
                "ALTER SYSTEM SET enable_eager_delta_joins = {}",
                old_config != "on"
            ))
            .unwrap();
        let before = generic_count();
        assert_eq!(
            client
                .query_one(&stmt, &[&1_i32, &1_i32])
                .unwrap()
                .get::<_, i32>(0),
            11
        );
        assert_eq!(generic_count() - before, u64::from(templates));
        system
            .batch_execute(&format!(
                "ALTER SYSTEM SET enable_eager_delta_joins = '{old_config}'"
            ))
            .unwrap();

        client.batch_execute("BEGIN").unwrap();
        assert_eq!(
            client
                .query_one(&stmt, &[&2_i32, &1_i32])
                .unwrap()
                .get::<_, i32>(0),
            22
        );
        assert_eq!(
            client
                .query_one(&stmt, &[&1_i32, &0_i32])
                .unwrap_db_error()
                .code(),
            &SqlState::DIVISION_BY_ZERO
        );
        let before = generic_count();
        assert_eq!(
            client
                .query_one(&stmt, &[&2_i32, &1_i32])
                .unwrap_db_error()
                .code(),
            &SqlState::IN_FAILED_SQL_TRANSACTION
        );
        assert_eq!(
            generic_count(),
            before,
            "aborted transactions must not execute a cached program"
        );
        client.batch_execute("ROLLBACK").unwrap();
        assert_eq!(
            client
                .query_one(&stmt, &[&1_i32, &1_i32])
                .unwrap()
                .get::<_, i32>(0),
            11
        );
        admin
            .batch_execute("DROP INDEX prepared_targets_other")
            .unwrap();
    }
}

#[mz_ore::test]
#[allow(clippy::disallowed_methods)] // Reuse historical reads and preserve AS OF errors.
fn test_prepared_indexed_as_of_changes() {
    let server = test_util::TestHarness::default()
        .with_system_parameter_default("enable_index_options".into(), "true".into())
        .with_system_parameter_default("enable_logical_compaction_window".into(), "true".into())
        .start_blocking();
    let mut admin = server.connect(postgres::NoTls).unwrap();
    let mut system = server.connect_internal(postgres::NoTls).unwrap();
    for sql in [
        "CREATE TABLE prepared_history (id int, value int)",
        "CREATE INDEX prepared_history_id ON prepared_history (id) WITH (RETAIN HISTORY FOR '1h')",
        "INSERT INTO prepared_history VALUES (1, 11), (2, 101)",
    ] {
        admin.batch_execute(sql).unwrap();
    }
    let timestamp = |client: &mut postgres::Client| -> String {
        client
            .query_one(
                "SELECT mz_now()::text FROM prepared_history WHERE id = 1",
                &[],
            )
            .unwrap()
            .get(0)
    };
    let first = timestamp(&mut admin);
    admin
        .batch_execute("UPDATE prepared_history SET value = 22 WHERE id = 1")
        .unwrap();
    let second = timestamp(&mut admin);
    assert!(second.parse::<u64>().unwrap() > first.parse::<u64>().unwrap());
    let generic_count = || -> u64 {
        server
            .metrics_registry()
            .gather()
            .iter()
            .filter(|family| family.name() == "mz_prepared_query_events_total")
            .flat_map(|family| family.get_metric())
            .filter(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|label| label.name() == "event" && label.value() == "generic_execute")
            })
            .map(|metric| u64::cast_lossy(metric.get_counter().value()))
            .sum()
    };
    for reuse in [false, true] {
        system
            .batch_execute(&format!(
                "ALTER SYSTEM SET enable_prepared_query_reuse = {reuse}"
            ))
            .unwrap();
        let mut client = server.connect(postgres::NoTls).unwrap();
        let stmt = client.prepare_typed(
            "SELECT value, mz_now()::text FROM prepared_history WHERE id = $1 AS OF $2::text::mz_timestamp",
            &[Type::INT4, Type::TEXT],
        ).unwrap();
        assert_eq!(stmt.params(), &[Type::INT4, Type::TEXT]);
        // AS OF does not support parameter binding on the ordinary path either.
        let error = client.query_one(&stmt, &[&1_i32, &first]).unwrap_db_error();
        assert_eq!(error.code(), &SqlState::UNDEFINED_PARAMETER);
        assert_eq!(error.message(), "there is no parameter $2");
        let old = client
            .prepare(&format!(
                "SELECT value, mz_now()::text FROM prepared_history WHERE id = $1 AS OF {first}"
            ))
            .unwrap();
        let new = client
            .prepare(&format!(
                "SELECT value, mz_now()::text FROM prepared_history WHERE id = $1 AS OF {second}"
            ))
            .unwrap();
        let before = generic_count();
        for (stmt, time, expected) in [(&old, &first, 11), (&new, &second, 22), (&old, &first, 11)]
        {
            let row = client.query_one(stmt, &[&1_i32]).unwrap();
            assert_eq!(row.get::<_, i32>(0), expected);
            assert_eq!(&row.get::<_, String>(1), time);
            assert_eq!(
                client.query_one(stmt, &[&2_i32]).unwrap().get::<_, i32>(0),
                101
            );
            assert!(client.query(stmt, &[&None::<i32>]).unwrap().is_empty());
        }
        let invalid = client
            .prepare("SELECT value FROM prepared_history WHERE id = $1 AS OF NULL::mz_timestamp")
            .unwrap();
        let error = client.query_one(&invalid, &[&1_i32]).unwrap_db_error();
        assert!(error.message().contains("non-null"), "{error}");
        assert_eq!(
            client.query_one(&new, &[&1_i32]).unwrap().get::<_, i32>(0),
            22
        );
        assert_eq!(
            generic_count(),
            before,
            "AS OF must use the explicit custom fallback"
        );
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
#[allow(clippy::disallowed_methods)]
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

// Startup-packet parameters must become the session defaults, like in
// PostgreSQL, so that RESET and DISCARD ALL restore them rather than the
// server defaults. Connection poolers rely on this. For example, pgbouncer's
// default server_reset_query is DISCARD ALL, which must not rebind a pooled
// connection to the default database.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[allow(clippy::disallowed_methods)]
async fn test_startup_params_survive_reset() {
    let server = test_util::TestHarness::default().start().await;

    async fn show(client: &tokio_postgres::Client, name: &str) -> String {
        client
            .query_one(&format!("SHOW {name}"), &[])
            .await
            .unwrap()
            .get(0)
    }

    // Parameters passed directly in the startup packet.
    {
        let client = server
            .connect()
            .dbname("startup_db")
            .application_name("startup_app")
            .await
            .unwrap();

        client
            .batch_execute("SET application_name = 'changed_app'")
            .await
            .unwrap();
        assert_eq!(show(&client, "application_name").await, "changed_app");
        client
            .batch_execute("RESET application_name")
            .await
            .unwrap();
        assert_eq!(show(&client, "application_name").await, "startup_app");

        client
            .batch_execute("SET database = other_db")
            .await
            .unwrap();
        client.batch_execute("DISCARD ALL").await.unwrap();
        assert_eq!(show(&client, "database").await, "startup_db");
        assert_eq!(show(&client, "application_name").await, "startup_app");

        client.batch_execute("RESET database").await.unwrap();
        assert_eq!(show(&client, "database").await, "startup_db");
    }

    // Parameters passed via the `options` startup parameter.
    {
        let client = server
            .connect()
            .options("--search_path=custom_schema")
            .await
            .unwrap();

        client.batch_execute("DISCARD ALL").await.unwrap();
        assert_eq!(show(&client, "search_path").await, "custom_schema");
    }

    // Client-supplied startup parameters take precedence over role defaults,
    // also as the reset value.
    {
        let client = server.connect().await.unwrap();
        client
            .batch_execute("ALTER ROLE materialize SET database = role_db")
            .await
            .unwrap();

        let client = server.connect().dbname("client_db").await.unwrap();
        assert_eq!(show(&client, "database").await, "client_db");
        client.batch_execute("DISCARD ALL").await.unwrap();
        assert_eq!(show(&client, "database").await, "client_db");

        // Without a client-supplied database, the role default applies.
        let client = server.connect().await.unwrap();
        assert_eq!(show(&client, "database").await, "role_db");
    }
}

// SQL-529: DISCARD ALL has to reset session variables over the extended query
// protocol as well as the simple one. tokio-postgres `execute`/`query` run over
// the extended protocol, while `batch_execute` runs over the simple one, so the
// existing simple-protocol tests never caught this bug.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[allow(clippy::disallowed_methods)]
async fn test_discard_all_resets_over_extended_protocol() {
    let server = test_util::TestHarness::default().start().await;

    async fn show(client: &tokio_postgres::Client, name: &str) -> String {
        client
            .query_one(&format!("SHOW {name}"), &[])
            .await
            .unwrap()
            .get(0)
    }

    // A plain SET is reset back to the compiled-in default.
    {
        let client = server.connect().await.unwrap();
        client
            .execute("SET extra_float_digits = 2", &[])
            .await
            .unwrap();
        assert_eq!(show(&client, "extra_float_digits").await, "2");
        client.execute("DISCARD ALL", &[]).await.unwrap();
        assert_eq!(show(&client, "extra_float_digits").await, "1");
    }

    // A startup-supplied default survives DISCARD ALL rather than reverting to
    // the compiled-in default.
    {
        let client = server
            .connect()
            .application_name("startup_app")
            .await
            .unwrap();
        client
            .execute("SET application_name = 'changed_app'", &[])
            .await
            .unwrap();
        assert_eq!(show(&client, "application_name").await, "changed_app");
        client.execute("DISCARD ALL", &[]).await.unwrap();
        assert_eq!(show(&client, "application_name").await, "startup_app");
    }

    // A role-supplied default (ALTER ROLE ... SET) survives DISCARD ALL rather
    // than reverting to the compiled-in default.
    {
        let client = server.connect().await.unwrap();
        client
            .execute("ALTER ROLE materialize SET database = role_db", &[])
            .await
            .unwrap();

        let client = server.connect().await.unwrap();
        assert_eq!(show(&client, "database").await, "role_db");
        client
            .execute("SET database = other_db", &[])
            .await
            .unwrap();
        assert_eq!(show(&client, "database").await, "other_db");
        client.execute("DISCARD ALL", &[]).await.unwrap();
        assert_eq!(show(&client, "database").await, "role_db");
    }
}

#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
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
#[allow(clippy::disallowed_methods)]
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
#[allow(clippy::disallowed_methods)]
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
#[allow(clippy::disallowed_methods)]
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
    pg_test_harness(path, mz_flags, test_util::TestHarness::default)
}

fn pg_test_harness(path: &Path, mz_flags: bool, harness: fn() -> test_util::TestHarness) {
    datadriven::walk(path.to_str().unwrap(), |tf| {
        let server = harness().unsafe_mode().start_blocking();
        if mz_flags {
            server.enable_feature_flags(&[
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
            tokio_postgres::config::Host::Unix(_) => panic!("only tcp connections supported"),
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
fn test_pgtest_copy_from_range() {
    pg_test_inner(Path::new("../../test/pgtest/copy-from-range.pt"), false);
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
fn test_pgtest_extra_float_digits() {
    pg_test_inner(Path::new("../../test/pgtest/extra-float-digits.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_notice() {
    pg_test_inner(Path::new("../../test/pgtest/notice.pt"), false);
}

#[mz_ore::test]
fn test_pgtest_nul() {
    pg_test_inner(Path::new("../../test/pgtest/nul.pt"), false);
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
fn test_pgtest_mz_affected() {
    pg_test_inner(Path::new("../../test/pgtest-mz/affected.pt"), true);
}

#[mz_ore::test]
fn test_pgtest_mz_copy_binary_unsupported() {
    pg_test_inner(
        Path::new("../../test/pgtest-mz/copy-binary-unsupported.pt"),
        true,
    );
}

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
fn test_pgtest_mz_numeric_binary_infinity() {
    pg_test_inner(
        Path::new("../../test/pgtest-mz/numeric-binary-infinity.pt"),
        true,
    );
}

#[mz_ore::test]
fn test_pgtest_mz_numeric_binary_overflow() {
    pg_test_inner(
        Path::new("../../test/pgtest-mz/numeric-binary-overflow.pt"),
        true,
    );
}

#[mz_ore::test]
fn test_pgtest_mz_time_binary_out_of_range() {
    pg_test_inner(
        Path::new("../../test/pgtest-mz/time-binary-out-of-range.pt"),
        true,
    );
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
fn test_pgtest_mz_subscribe_dependency_dropped() {
    pg_test_inner(
        Path::new("../../test/pgtest-mz/subscribe-dependency-dropped.pt"),
        true,
    );
}

#[mz_ore::test]
fn test_pgtest_mz_startup() {
    pg_test_inner(Path::new("../../test/pgtest-mz/startup.pt"), true);
}

#[mz_ore::test]
fn test_pgtest_mz_stray_copy() {
    pg_test_inner(Path::new("../../test/pgtest-mz/stray-copy.pt"), true);
}

#[mz_ore::test]
fn test_pgtest_mz_set_local() {
    pg_test_inner(Path::new("../../test/pgtest-mz/set-local.pt"), true);
}

#[mz_ore::test]
fn test_pgtest_mz_transactions() {
    pg_test_inner(Path::new("../../test/pgtest-mz/transactions.pt"), true);
}

#[mz_ore::test]
fn test_pgtest_mz_vars() {
    pg_test_inner(Path::new("../../test/pgtest-mz/vars.pt"), true);
}

// Guard against .pt files silently losing test coverage when new files are
// added to test/pgtest/ or test/pgtest-mz/ without a corresponding test wrapper.
#[mz_ore::test]
fn test_all_pt_files_have_test_wrappers() {
    let source = include_str!("pgwire.rs");
    for (dir, prefix) in [
        ("../../test/pgtest", "test_pgtest_"),
        ("../../test/pgtest-mz", "test_pgtest_mz_"),
    ] {
        let dir_path = Path::new(dir);
        for entry in
            std::fs::read_dir(dir_path).unwrap_or_else(|e| panic!("failed to read {dir}: {e}"))
        {
            let entry = entry.unwrap();
            let file_name = entry.file_name();
            let name = file_name.to_str().unwrap();
            if !name.ends_with(".pt") {
                continue;
            }
            let stem = name.trim_end_matches(".pt").replace('-', "_");
            let expected_fn = format!("fn {prefix}{stem}(");
            assert!(
                source.contains(&expected_fn),
                "no test wrapper found for {dir}/{name} — expected `{expected_fn})`",
            );
        }
    }
}

// Test that encoding a message with too many columns (> i16::MAX) doesn't
// corrupt the connection. The codec truncates the buffer when encoding fails,
// ensuring no partial message is left in the buffer. See
// https://github.com/MaterializeInc/database-issues/issues/9496
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_many_columns() {
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();

    let cols = (1..=32769)
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let query = format!("SELECT {cols}");

    // The query must fail because the server can't encode a RowDescription
    // with more than i16::MAX columns. When encoding fails, `machine.run()`
    // returns an error, and the catch-all handler in `protocol.rs` sends a
    // FATAL ErrorResponse with "fields in row description, which exceeds ..."
    // before closing the connection.
    //
    // However, the client non-deterministically sees either the FATAL
    // ErrorResponse or just "connection closed". This is because the
    // rust-postgres client's `poll_block_on` drains connection-level events
    // before polling the query future: if the ErrorResponse and the TCP
    // close (FIN) arrive in the same read, `poll_read` processes the
    // ErrorResponse but then immediately hits EOF and returns
    // `Err(Error::closed)`, which short-circuits `poll_block_on` before the
    // query future can read the ErrorResponse from the channel.
    //
    // Both outcomes indicate the server handled the overflow gracefully (no
    // partial/corrupt message in the buffer).
    match client.simple_query(&query) {
        Ok(_) => panic!("query with too many columns should have failed"),
        Err(err) => {
            let err_str = err.to_string_with_causes();
            assert!(
                err_str.contains("fields in row description, which exceeds")
                    || err_str.contains("connection closed"),
                "unexpected error: {err}"
            );
        }
    }
}

/// Writes a frontend message with type byte `typ`, filling in its length prefix.
///
/// `postgres_protocol`'s builders cap repeated groups at `i16::MAX` entries, so
/// a message with more parameters than that must be assembled by hand.
fn put_frontend_message<F: FnOnce(&mut BytesMut)>(buf: &mut BytesMut, typ: u8, body: F) {
    buf.put_u8(typ);
    let base = buf.len();
    buf.put_u32(0);
    body(buf);
    let len = u32::try_from(buf.len() - base).unwrap();
    buf[base..base + 4].copy_from_slice(&len.to_be_bytes());
}

/// Reads backend messages up to and including the `ReadyForQuery` that answers
/// `Sync`.
fn read_until_ready(stream: &mut TcpStream) -> Vec<postgres_protocol::message::backend::Message> {
    use postgres_protocol::message::backend::Message;

    let mut buf = BytesMut::new();
    let mut chunk = [0; 1 << 13];
    let mut msgs = Vec::new();
    loop {
        if let Some(msg) = Message::parse(&mut buf).unwrap() {
            let ready = matches!(msg, Message::ReadyForQuery(_));
            msgs.push(msg);
            if ready {
                return msgs;
            }
        } else {
            let n = stream.read(&mut chunk).unwrap();
            assert!(n > 0, "connection closed after {} messages", msgs.len());
            buf.extend_from_slice(&chunk[..n]);
        }
    }
}

// The counts that precede the repeated groups of Parse and Bind (parameter
// types, format codes, parameter values) are decoded as unsigned, like
// PostgreSQL does, so a client may send more than `i16::MAX` parameters.
// Reading such a count as signed made it negative, which dropped the group and
// left the decoder misaligned for the rest of the message. See SQL-491.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_many_bind_params() {
    use postgres_protocol::message::backend::Message;
    use postgres_protocol::message::frontend;

    // One past `i16::MAX`. The wire format allows 65535, but a bind that large
    // runs into the unrelated `MAX_REQUEST_SIZE` limit.
    const PARAMS: usize = 32768;
    const INT4_OID: u32 = 23;
    // Bound to the last parameter, so the inserted row matches only if the
    // decoder stayed aligned to the end of the parameter array.
    const LAST_VALUE: i32 = 4242;

    let server = test_util::TestHarness::default().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();
    client
        .batch_execute("CREATE TABLE many_params (a int)")
        .unwrap();

    let mut stream = TcpStream::connect(server.sql_local_addr()).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(120)))
        .unwrap();
    let mut buf = BytesMut::new();
    frontend::startup_message(
        vec![
            ("user", "materialize"),
            ("database", DEFAULT_DATABASE_NAME),
            ("options", "--welcome_message=off"),
        ],
        &mut buf,
    )
    .unwrap();
    stream.write_all(&buf).unwrap();
    read_until_ready(&mut stream);

    // The shape from the report: pgjdbc's reWriteBatchedInserts rewrites a batch
    // into one insert whose placeholder count exceeds `i16::MAX`.
    let placeholders = (1..=PARAMS)
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!("INSERT INTO many_params VALUES (coalesce({placeholders}))");

    // The statement and portal are unnamed throughout, hence the bare `0`s.
    buf.clear();
    put_frontend_message(&mut buf, b'P', |buf| {
        buf.put_u8(0);
        buf.put_slice(sql.as_bytes());
        buf.put_u8(0);
        buf.put_u16(u16::try_from(PARAMS).unwrap());
        for _ in 0..PARAMS {
            buf.put_u32(INT4_OID);
        }
    });
    put_frontend_message(&mut buf, b'D', |buf| {
        buf.put_u8(b'S'); // the statement, to get a ParameterDescription back
        buf.put_u8(0);
    });
    put_frontend_message(&mut buf, b'B', |buf| {
        buf.put_u8(0);
        buf.put_u8(0);
        buf.put_u16(u16::try_from(PARAMS).unwrap());
        for _ in 0..PARAMS {
            buf.put_u16(1); // binary
        }
        buf.put_u16(u16::try_from(PARAMS).unwrap());
        for _ in 1..PARAMS {
            buf.put_i32(-1); // NULL
        }
        buf.put_i32(4);
        buf.put_i32(LAST_VALUE);
        buf.put_u16(0); // results in text
    });
    put_frontend_message(&mut buf, b'E', |buf| {
        buf.put_u8(0);
        buf.put_i32(0); // no row limit
    });
    frontend::sync(&mut buf);
    stream.write_all(&buf).unwrap();

    let mut described_params = None;
    let mut tags = Vec::new();
    for msg in read_until_ready(&mut stream) {
        match msg {
            Message::ErrorResponse(body) => {
                let fields: Vec<_> = body
                    .fields()
                    .map(|f| Ok(String::from_utf8_lossy(f.value_bytes()).into_owned()))
                    .collect()
                    .unwrap();
                panic!("unexpected error response: {fields:?}");
            }
            Message::ParameterDescription(body) => {
                described_params = Some(body.parameters().count().unwrap());
            }
            Message::CommandComplete(body) => tags.push(body.tag().unwrap().to_string()),
            _ => (),
        }
    }
    assert_eq!(described_params, Some(PARAMS));
    assert_eq!(tags, ["INSERT 0 1"]);

    let row = client.query_one("SELECT a FROM many_params", &[]).unwrap();
    assert_eq!(row.get::<_, i32>(0), LAST_VALUE);
}

/// How the frontend OCC read-then-write path behaves inside an extended-protocol
/// pipeline: a write that reads nothing stages its rows and rolls back with the
/// pipeline, one that reads persisted state is either refused or commits as its
/// own transaction.
#[mz_ore::test]
fn test_pgtest_mz_frontend_occ_pipelined_dml() {
    pg_test_harness(
        Path::new("../../test/pgtest-mz/frontend-occ-pipelined-dml.pt"),
        true,
        || {
            test_util::TestHarness::default().with_system_parameter_default(
                "enable_adapter_frontend_occ_read_then_write".to_string(),
                "true".to_string(),
            )
        },
    );
}
