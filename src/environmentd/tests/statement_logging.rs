// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for statement logging.

use std::thread;
use std::time::Duration;

use chrono::{DateTime, Utc};
use mz_environmentd::test_util;
use mz_ore::assert_contains;
use mz_ore::assert_none;
use mz_ore::cast::{CastFrom, CastLossy, TryCastFrom};
use mz_ore::collections::CollectionExt;
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::to_datetime;
use mz_ore::retry::Retry;
use mz_pgrepr::UInt8;
use mz_sql_parser::ast::display::AstDisplay;
use tokio_postgres::error::SqlState;
use tungstenite::Message;

/// A wrapper around `TestServerWithRuntime` that runs statement logging checks when dropped.
///
/// This guard ensures that all statements have finished executing (have non-NULL `finished_at`
/// and `finished_status` in `mz_internal.mz_recent_activity_log`) before the test completes.
struct TestServerWithStatementLoggingChecks {
    server: test_util::TestServerWithRuntime,
}

impl TestServerWithStatementLoggingChecks {
    /// Connect to the __internal__ SQL port of the running `environmentd` server.
    pub fn connect_internal<T>(&self, tls: T) -> Result<postgres::Client, anyhow::Error>
    where
        T: postgres::tls::MakeTlsConnect<postgres::Socket> + Send + 'static,
        T::TlsConnect: Send,
        T::Stream: Send,
        <T::TlsConnect as postgres::tls::TlsConnect<postgres::Socket>>::Future: Send,
    {
        self.server.connect_internal(tls)
    }

    /// Returns the metrics registry for the test server.
    pub fn metrics_registry(&self) -> &MetricsRegistry {
        self.server.metrics_registry()
    }

    /// Returns a config for connecting to the __public__ SQL port, so a test
    /// can connect as a role other than the default one.
    pub fn pg_config(&self) -> postgres::Config {
        self.server.pg_config()
    }
}

/// Helper to get statement logging record counts from the metrics registry.
/// Returns (sampled_true_count, sampled_false_count).
#[allow(clippy::disallowed_methods)]
fn get_statement_logging_record_counts(
    server: &TestServerWithStatementLoggingChecks,
) -> (u64, u64) {
    let metrics = server.metrics_registry().gather();
    let record_count_metric = metrics
        .into_iter()
        .find(|m| m.name() == "mz_statement_logging_record_count")
        .expect("mz_statement_logging_record_count metric should exist");

    let metric_entries = record_count_metric.get_metric();
    let sampled_true = metric_entries
        .iter()
        .find(|m| {
            m.get_label()
                .iter()
                .any(|l| l.name() == "sample" && l.value() == "true")
        })
        .map(|m| u64::cast_lossy(m.get_counter().value()))
        .unwrap_or(0);
    let sampled_false = metric_entries
        .iter()
        .find(|m| {
            m.get_label()
                .iter()
                .any(|l| l.name() == "sample" && l.value() == "false")
        })
        .map(|m| u64::cast_lossy(m.get_counter().value()))
        .unwrap_or(0);

    (sampled_true, sampled_false)
}

impl Drop for TestServerWithStatementLoggingChecks {
    #[allow(clippy::disallowed_methods)]
    fn drop(&mut self) {
        // Don't run checks if we're already panicking, as this could mask the original error.
        if std::thread::panicking() {
            return;
        }

        let mut mz_client = self
            .server
            .connect_internal(postgres::NoTls)
            .expect("Failed to connect to internal SQL port for statement logging check");

        // Disable RBAC checks so we can query mz_internal tables.
        // (We don't need to restore this afterwards, since no more tests run in the same system.)
        mz_client
            .batch_execute("ALTER SYSTEM SET enable_rbac_checks = false")
            .expect("Failed to disable RBAC checks");

        // The statement log has a 5-second buffer flush interval, so allow sufficient time.
        Retry::default()
            .max_duration(Duration::from_secs(30))
            .retry(|_| {
                let result = mz_client.query_one(
                    "SELECT count(*)
                     FROM mz_internal.mz_recent_activity_log
                     WHERE
                       (finished_at IS NULL OR finished_status IS NULL)
                       AND sql NOT LIKE '%__FILTER-OUT-THIS-QUERY__%'
                       AND finished_status IS DISTINCT FROM 'aborted'",
                    &[],
                );

                match result {
                    Ok(row) => {
                        let count: i64 = row.get(0);
                        if count == 0 {
                            Ok(())
                        } else {
                            Err(format!("{} statements have not finished", count))
                        }
                    }
                    Err(e) => Err(format!("Query failed: {}", e)),
                }
            })
            .expect("All statements should have finished executing");
    }
}

fn setup_statement_logging_core(
    max_sample_rate: f64,
    sample_rate: f64,
    target_data_rate: &str,
    test_harness: test_util::TestHarness,
) -> (TestServerWithStatementLoggingChecks, postgres::Client) {
    let server = test_harness
        .with_system_parameter_default(
            "statement_logging_max_sample_rate".to_string(),
            max_sample_rate.to_string(),
        )
        .with_system_parameter_default(
            "statement_logging_default_sample_rate".to_string(),
            sample_rate.to_string(),
        )
        .with_system_parameter_default(
            "statement_logging_max_data_credit".to_string(),
            "".to_string(),
        )
        .with_system_parameter_default(
            "statement_logging_target_data_rate".to_string(),
            target_data_rate.to_string(),
        )
        .with_system_parameter_default(
            "statement_logging_use_reproducible_rng".to_string(),
            "true".to_string(),
        )
        .start_blocking();
    let client = server.connect(postgres::NoTls).unwrap();
    let server = TestServerWithStatementLoggingChecks { server };
    (server, client)
}

fn setup_statement_logging(
    max_sample_rate: f64,
    sample_rate: f64,
    target_data_rate: &str,
) -> (TestServerWithStatementLoggingChecks, postgres::Client) {
    setup_statement_logging_core(
        max_sample_rate,
        sample_rate,
        target_data_rate,
        test_util::TestHarness::default(),
    )
}

// Test that we log various kinds of statement whose execution terminates in the coordinator.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_statement_logging_immediate() {
    let (server, mut client) = setup_statement_logging(1.0, 1.0, "");

    let mut mz_client = server.connect_internal(postgres::NoTls).unwrap();
    mz_client
        .batch_execute("ALTER SYSTEM SET enable_statement_lifecycle_logging = false")
        .unwrap();
    mz_client
        .batch_execute("ALTER SYSTEM SET statement_logging_max_sample_rate = 1")
        .unwrap();
    mz_client
        .batch_execute("ALTER SYSTEM SET statement_logging_default_sample_rate = 1")
        .unwrap();
    mz_client
        .batch_execute("ALTER SYSTEM SET enable_load_generator_counter = true")
        .unwrap();

    let successful_immediates: &[&str] = &[
        "CREATE VIEW v AS SELECT 1;",
        "CREATE DEFAULT INDEX i ON v;",
        "CREATE TABLE t (x bigint);",
        "INSERT INTO t VALUES (1), (2), (3)",
        "UPDATE t SET x=x+1",
        "DELETE FROM t;",
        "CREATE SECRET s AS 'hunter2';",
        "DROP SECRET s;",
        "",
        "CREATE SOURCE s FROM LOAD GENERATOR COUNTER",
        "PREPARE foo AS SELECT * FROM t",
        "EXECUTE foo",
        "BEGIN",
        "DECLARE c CURSOR FOR SELECT * FROM t",
        "FETCH FORWARD ALL FROM c",
        "COMMIT",
        "BEGIN",
        "ROLLBACK",
        "SET application_name='my_application'",
        "SHOW ALL",
        "SHOW application_name",
    ];
    let constants: &[&str] = &["1", "2", "3", "hunter2", "my_application"];

    for &statement in successful_immediates {
        client.execute(statement, &[]).unwrap();

        // Enforce a small delay to avoid duplicate `began_at` times, which would make the ordering
        // of logged statements non-deterministic when we retrieve them below.
        thread::sleep(Duration::from_millis(10));
    }

    let mut client = server.connect_internal(postgres::NoTls).unwrap();
    let seh_query = "
        SELECT
            mseh.sample_rate,
            mseh.began_at,
            mseh.finished_at,
            mseh.finished_status,
            mst.sql,
            mpsh.prepared_at,
            mst.redacted_sql
        FROM mz_internal.mz_statement_execution_history AS mseh
        LEFT JOIN
            mz_internal.mz_prepared_statement_history AS mpsh
            ON mseh.prepared_statement_id = mpsh.id
        JOIN
            (SELECT DISTINCT sql, sql_hash, redacted_sql FROM mz_internal.mz_sql_text) mst
            ON mpsh.sql_hash = mst.sql_hash
        WHERE
            mst.sql !~~ '%mz_statement_execution_history%' AND
            mseh.finished_at IS NOT NULL
        ORDER BY mseh.began_at";

    // Statement logging happens async, retry until we get the expected number of logged
    // statements.
    let mut sl = Vec::new();
    for _ in 0..10 {
        thread::sleep(Duration::from_secs(1));
        sl = client.query(seh_query, &[]).unwrap();
        if sl.len() >= successful_immediates.len() {
            break;
        }
    }

    assert_eq!(sl.len(), successful_immediates.len());

    #[derive(Debug)]
    struct Record {
        sample_rate: f64,
        began_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        finished_status: String,
        sql: String,
        prepared_at: DateTime<Utc>,
        redacted_sql: String,
    }
    for (r, stmt) in std::iter::zip(sl.iter(), successful_immediates) {
        let r = Record {
            sample_rate: r.get(0),
            began_at: r.get(1),
            finished_at: r.get(2),
            finished_status: r.get(3),
            sql: r.get(4),
            prepared_at: r.get(5),
            redacted_sql: r.get(6),
        };
        assert_eq!(r.sample_rate, 1.0);

        let expected_sql = if r.sql.contains("SECRET")
            || r.sql.contains("INSERT")
            || r.sql.contains("UPDATE")
            || r.sql.contains("EXECUTE")
        {
            mz_sql::parse::parse(&r.sql)
                .unwrap()
                .into_element()
                .ast
                .to_ast_string_redacted()
        } else {
            stmt.chars().filter(|&ch| ch != ';').collect::<String>()
        };
        assert_eq!(r.sql, expected_sql);
        assert_eq!(r.finished_status, "success");
        assert!(r.prepared_at <= r.began_at);
        assert!(r.began_at <= r.finished_at);
        // NB[btv] -- It would be a bit nicer if we could separately mock
        // both the start and end time, but the `NowFn` mechanism doesn't
        // appear to give us any way to do that. Instead, let's just check
        // that none of these statements took longer than 5s wall-clock time.
        assert!(r.finished_at - r.began_at <= chrono::Duration::try_seconds(5).unwrap());
        if !r.sql.is_empty() {
            let expected_redacted = mz_sql::parse::parse(&r.sql)
                .unwrap()
                .into_element()
                .ast
                .to_ast_string_redacted();
            assert_eq!(r.redacted_sql, expected_redacted);
            for constant in constants {
                assert!(!r.redacted_sql.contains(constant));
            }
        }
    }
}

#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_statement_logging_basic() {
    let (server, mut client) = setup_statement_logging(1.0, 1.0, "");
    client.execute("SELECT 1", &[]).unwrap();
    // We test that queries of this view execute on a cluster.
    // If we ever change the threshold for constant folding such that
    // this gets to run on environmentd, change this query.
    client
        .execute(
            "CREATE VIEW v AS SELECT * FROM generate_series(1, 10001)",
            &[],
        )
        .unwrap();
    client.execute("SELECT * FROM v", &[]).unwrap();
    client.execute("CREATE DEFAULT INDEX i ON v", &[]).unwrap();
    client.execute("SELECT * FROM v", &[]).unwrap();
    let _ = client.execute("SELECT 1/0", &[]);
    client.execute("CREATE TABLE t (x int)", &[]).unwrap();
    client.execute("SELECT * FROM t", &[]).unwrap();

    #[derive(Debug)]
    struct Record {
        sample_rate: f64,
        began_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        finished_status: String,
        error_message: Option<String>,
        prepared_at: DateTime<Utc>,
        execution_strategy: Option<String>,
        result_size: Option<i64>,
        rows_returned: Option<i64>,
        execution_timestamp: Option<u64>,
    }

    let mut client = server.connect_internal(postgres::NoTls).unwrap();

    let result = Retry::default()
        .max_duration(Duration::from_secs(30))
        .retry(|_| {
            let sl_results = client
                .query(
                    "SELECT
    mseh.sample_rate,
    mseh.began_at,
    mseh.finished_at,
    mseh.finished_status,
    mseh.error_message,
    mpsh.prepared_at,
    mseh.execution_strategy,
    mseh.result_size,
    mseh.rows_returned,
    mseh.execution_timestamp
FROM
    mz_internal.mz_statement_execution_history AS mseh
        LEFT JOIN
            mz_internal.mz_prepared_statement_history AS mpsh
            ON mseh.prepared_statement_id = mpsh.id
        JOIN
            (SELECT DISTINCT sql, sql_hash, redacted_sql FROM mz_internal.mz_sql_text) AS mst
            ON mpsh.sql_hash = mst.sql_hash
WHERE (mst.sql ~~ 'SELECT%'
AND mst.sql !~~ '%unique string to prevent this query showing up in results after retries%'
AND mst.sql !~~ '%pg_catalog.pg_type%' --this gets executed behind the scenes by tokio-postgres
OR mst.sql ~~ 'CREATE TABLE%')
AND mseh.finished_at IS NOT NULL
ORDER BY mseh.began_at",
                    &[],
                )
                .unwrap();

            if sl_results.len() == 6 {
                Ok(sl_results)
            } else {
                Err(sl_results.len())
            }
        });
    let sl_results = match result {
        Ok(rows) => rows
            .into_iter()
            .map(|r| Record {
                sample_rate: r.get(0),
                began_at: r.get(1),
                finished_at: r.get(2),
                finished_status: r.get(3),
                error_message: r.get(4),
                prepared_at: r.get(5),
                execution_strategy: r.get(6),
                result_size: r.get(7),
                rows_returned: r.get(8),
                execution_timestamp: r.get::<_, Option<UInt8>>(9).map(|UInt8(val)| val),
            })
            .collect::<Vec<_>>(),
        Err(rows) => {
            panic!("number of results never became correct: {rows}");
        }
    };
    // The two queries on generate_series(1,10001) execute at the maximum timestamp
    assert_eq!(
        sl_results
            .iter()
            .filter(|r| r.execution_timestamp == Some(u64::MAX))
            .count(),
        2
    );
    // The two queries that can be satisfied by envd (SELECT 1 and SELECT 1/0) have no execution timestamp
    assert_eq!(
        sl_results
            .iter()
            .filter(|r| r.execution_timestamp.is_none())
            .count(),
        2
    );
    // All other queries have an execution timestamp, in particular, including `CREATE TABLE`.
    assert_eq!(sl_results.len(), 6);
    for r in &sl_results {
        assert_eq!(r.sample_rate, 1.0);
        assert!(r.prepared_at <= r.began_at);
        assert!(r.began_at <= r.finished_at);
        // It would be nice to be able to control
        // execution timestamp via a `NowFn`, but
        // that is hard to get right and interferes with our logic
        // about when to flush to persist. So instead, just check that they're sane.
        if let Some(ts) = r.execution_timestamp {
            if ts != u64::MAX {
                let ts = to_datetime(ts);
                assert!((ts - r.prepared_at).abs() < chrono::Duration::try_seconds(5).unwrap())
            }
        }
    }
    assert!(sl_results[0].result_size.unwrap_or(0) > 0);
    assert_eq!(sl_results[0].rows_returned, Some(1));
    assert_eq!(sl_results[0].finished_status, "success");
    assert_eq!(
        sl_results[0].execution_strategy.as_ref().unwrap(),
        "constant"
    );
    assert!(sl_results[1].result_size.unwrap_or(0) > 0);
    assert_eq!(sl_results[1].rows_returned, Some(10001));
    assert_eq!(sl_results[1].finished_status, "success");
    assert_eq!(
        sl_results[1].execution_strategy.as_ref().unwrap(),
        "standard"
    );
    assert!(sl_results[2].result_size.unwrap_or(0) > 0);
    assert_eq!(sl_results[2].rows_returned, Some(10001));
    assert_eq!(sl_results[2].finished_status, "success");
    assert_eq!(
        sl_results[2].execution_strategy.as_ref().unwrap(),
        "fast-path"
    );
    assert_eq!(sl_results[3].finished_status, "error");
    assert!(
        sl_results[3]
            .error_message
            .as_ref()
            .unwrap()
            .contains("division by zero")
    );
    assert_none!(sl_results[3].result_size);
    assert_none!(sl_results[3].rows_returned);

    // Verify metrics show all statements were sampled (100% sample rate means no unsampled).
    let (sampled_true, sampled_false) = get_statement_logging_record_counts(&server);
    assert!(
        sampled_true > 0,
        "some statements should be sampled with 100% rate"
    );
    assert_eq!(
        sampled_false, 0,
        "no statements should be unsampled with 100% rate"
    );

    // Verify statement_logging_actual_bytes metric is being tracked.
    // With 100% sample rate, actual_bytes should equal unsampled_bytes.
    let metrics = server.metrics_registry().gather();
    let actual_bytes = metrics
        .iter()
        .find(|m| m.name() == "mz_statement_logging_actual_bytes")
        .expect("mz_statement_logging_actual_bytes metric should exist")
        .get_metric()[0]
        .get_counter()
        .value();
    let unsampled_bytes = metrics
        .iter()
        .find(|m| m.name() == "mz_statement_logging_unsampled_bytes")
        .expect("mz_statement_logging_unsampled_bytes metric should exist")
        .get_metric()[0]
        .get_counter()
        .value();
    assert!(
        actual_bytes > 0.0,
        "actual_bytes should be > 0 with 100% sample rate"
    );
    assert_eq!(
        actual_bytes, unsampled_bytes,
        "with 100% sample rate, actual_bytes should equal unsampled_bytes"
    );
}

#[allow(clippy::disallowed_methods)]
fn run_throttling_test(use_prepared_statement: bool) {
    // The `target_data_rate` should be
    // - high enough so that the `SELECT 1` queries get throttled (even with high CPU load due to
    //   other tests running in parallel),
    // - but low enough that the `SELECT 2` query after the sleep doesn't get throttled.
    let (server, mut client) = setup_statement_logging(1.0, 1.0, "200");
    thread::sleep(Duration::from_secs(2));

    if use_prepared_statement {
        let statement = client.prepare("SELECT 1").unwrap();
        for _ in 0..100 {
            client.execute(&statement, &[]).unwrap();
        }
    } else {
        for _ in 0..100 {
            client.execute("SELECT 1", &[]).unwrap();
        }
    }

    thread::sleep(Duration::from_secs(4));
    client.execute("SELECT 2", &[]).unwrap();
    let mut client = server.connect_internal(postgres::NoTls).unwrap();
    let logs = Retry::default()
        .max_duration(Duration::from_secs(60))
        .retry(|_| {
            let sl_results = client
                .query(
                    "SELECT
    sql,
    throttled_count
FROM mz_internal.mz_statement_execution_history mseh
JOIN mz_internal.mz_prepared_statement_history mpsh
ON mseh.prepared_statement_id = mpsh.id
JOIN (SELECT DISTINCT sql, sql_hash, redacted_sql FROM mz_internal.mz_sql_text) mst
ON mpsh.sql_hash = mst.sql_hash
WHERE sql IN ('SELECT 1', 'SELECT 2')",
                    &[],
                )
                .unwrap();

            if sl_results.iter().any(|stmt| {
                let sql: String = stmt.get(0);
                sql == "SELECT 2"
            }) {
                Ok(sl_results)
            } else {
                Err(())
            }
        })
        .expect("Never saw last statement (`SELECT 2`)");
    let throttled_count = logs
        .iter()
        .map(|log| {
            let UInt8(throttled_count) = log.get(1);
            throttled_count
        })
        .sum::<u64>();
    assert!(
        throttled_count > 0,
        "at least some statements should have been throttled"
    );

    assert_eq!(logs.len() + usize::cast_from(throttled_count), 101);
}

#[mz_ore::test]
fn test_statement_logging_throttling() {
    run_throttling_test(false);
}

#[mz_ore::test]
fn test_statement_logging_prepared_statement_throttling() {
    run_throttling_test(true);
}

/// throttling the first execution of a prepared statement must not make
//  every subsequent execution of that statement invisible in
//  `mz_recent_activity_log`.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_statement_logging_throttled_prepared_statement_stays_visible() {
    const APPLICATION_NAME: &str = "throttled_prepared_repro";
    const NUM_SUBSEQUENT_STATEMENT_EXECUTIONS: i64 = 5;

    let (server, mut client) = setup_statement_logging(1.0, 1.0, "1");
    let mut mz_client = server.connect_internal(postgres::NoTls).unwrap();

    // cap the credit at a single byte such that the first execution of our
    // prepared statement is guaranteed to be throttled.
    mz_client
        .batch_execute("ALTER SYSTEM SET statement_logging_target_data_rate = 1")
        .unwrap();
    mz_client
        .batch_execute("ALTER SYSTEM SET statement_logging_max_data_credit = 1")
        .unwrap();

    client
        .batch_execute(&format!("SET application_name = '{APPLICATION_NAME}'"))
        .unwrap();

    // First execution of the prepared statement. This one gets throttled, and so
    // is never logged.
    let stmt = client.prepare("SELECT $1::text").unwrap();
    assert_eq!(
        client
            .query_one(&stmt, &[&"first"])
            .unwrap()
            .get::<_, String>(0),
        "first"
    );

    // Lift the throttle so subsequent executions are logged. Sleep afterwards so
    // the token bucket has time to refill before the next execution
    mz_client
        .batch_execute("ALTER SYSTEM SET statement_logging_target_data_rate = 1000000000")
        .unwrap();
    mz_client
        .batch_execute("ALTER SYSTEM SET statement_logging_max_data_credit = 1000000000")
        .unwrap();
    thread::sleep(Duration::from_secs(2));

    // Subsequent executions of the same prepared statement.
    for _ in 0..NUM_SUBSEQUENT_STATEMENT_EXECUTIONS {
        assert_eq!(
            client
                .query_one(&stmt, &[&"again"])
                .unwrap()
                .get::<_, String>(0),
            "again"
        );
    }

    // Statement logging flushes asynchronously; wait until the
    // subsequent executions have landed in `mz_statement_execution_history`.
    Retry::default()
        .max_duration(Duration::from_secs(30))
        .retry(|_| {
            let mseh_count: i64 = mz_client
                .query_one(
                    &format!(
                        "SELECT count(*)
                         FROM mz_internal.mz_statement_execution_history
                         WHERE application_name = '{APPLICATION_NAME}'"
                    ),
                    &[],
                )
                .unwrap()
                .get(0);
            if mseh_count >= NUM_SUBSEQUENT_STATEMENT_EXECUTIONS {
                Ok(())
            } else {
                Err(format!(
                    "only {mseh_count} of {NUM_SUBSEQUENT_STATEMENT_EXECUTIONS} executions logged so far"
                ))
            }
        })
        .expect("subsequent executions never appeared in mz_statement_execution_history");

    // The subsequent executions in mz_statement_execution_history should
    // have a matching entry in mz_prepared_statement_history
    let orphan_count: i64 = mz_client
        .query_one(
            &format!(
                "SELECT count(*)
                 FROM mz_internal.mz_statement_execution_history mseh
                 LEFT JOIN mz_internal.mz_prepared_statement_history mpsh
                   ON mseh.prepared_statement_id = mpsh.id
                 WHERE mseh.application_name = '{APPLICATION_NAME}'
                   AND mpsh.id IS NULL"
            ),
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(
        orphan_count, 0,
        "{orphan_count} mz_statement_execution_history rows reference a \
         prepared_statement_id with no matching mz_prepared_statement_history row"
    );
}

#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_statement_logging_subscribes() {
    let (server, mut client) = setup_statement_logging(1.0, 1.0, "");
    let cancel_token = client.cancel_token();

    // This should finish
    client
        .execute(
            "SUBSCRIBE TO (SELECT * FROM generate_series(1, 10001))",
            &[],
        )
        .unwrap();

    let handle = thread::spawn(move || {
        client.execute("CREATE TABLE t (x int)", &[]).unwrap();
        // This should not finish until it's canceled.
        let _ = client.execute("SUBSCRIBE TO (SELECT * FROM t)", &[]);
    });

    while !handle.is_finished() {
        thread::sleep(Duration::from_secs(1));
        cancel_token.cancel_query(postgres::NoTls).unwrap();
    }
    handle.join().unwrap();

    let mut client = server.connect_internal(postgres::NoTls).unwrap();
    let seh_query = "
        SELECT
            mseh.sample_rate,
            mseh.began_at,
            mseh.finished_at,
            mseh.finished_status,
            mpsh.prepared_at,
            mseh.execution_strategy
        FROM mz_internal.mz_statement_execution_history AS mseh
        LEFT JOIN
            mz_internal.mz_prepared_statement_history AS mpsh
            ON mseh.prepared_statement_id = mpsh.id
        JOIN
            mz_internal.mz_sql_text AS mst
            ON mpsh.sql_hash = mst.sql_hash
        WHERE
            mst.sql ~~ 'SUBSCRIBE%' AND
            mseh.finished_at IS NOT NULL
        ORDER BY mseh.began_at";

    // Statement logging happens async, retry until we get the expected number of logged
    // statements.
    let mut sl = Vec::new();
    for _ in 0..10 {
        thread::sleep(Duration::from_secs(1));
        sl = client.query(seh_query, &[]).unwrap();
        if sl.len() >= 2 {
            break;
        }
    }

    assert_eq!(sl.len(), 2);

    struct Record {
        sample_rate: f64,
        began_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        finished_status: String,
        prepared_at: DateTime<Utc>,
        execution_strategy: Option<String>,
    }

    let sl_subscribes = sl
        .into_iter()
        .map(|r| Record {
            sample_rate: r.get(0),
            began_at: r.get(1),
            finished_at: r.get(2),
            finished_status: r.get(3),
            prepared_at: r.get(4),
            execution_strategy: r.get(5),
        })
        .collect::<Vec<_>>();
    for r in &sl_subscribes {
        assert_eq!(r.sample_rate, 1.0);
        assert!(r.prepared_at <= r.began_at);
        assert!(r.began_at <= r.finished_at);
        assert_none!(r.execution_strategy);
    }
    assert_eq!(sl_subscribes[0].finished_status, "success");
    assert_eq!(sl_subscribes[1].finished_status, "canceled");
}

/// Test that we are sampling approximately 50% of statements.
/// Relies on two assumptions:
/// (1) that the effective sampling rate for the session is 50%,
/// (2) that we are using the deterministic testing RNG.
#[allow(clippy::disallowed_methods)]
fn test_statement_logging_sampling_inner(
    server: TestServerWithStatementLoggingChecks,
    mut client: postgres::Client,
) {
    for i in 0..50 {
        client.execute(&format!("SELECT {i}"), &[]).unwrap();

        // Enforce a small delay to avoid duplicate `began_at` times, which would make the ordering
        // of logged statements non-deterministic when we retrieve them below.
        thread::sleep(Duration::from_millis(10));
    }

    // 23 randomly sampled out of 50 with 50% sampling. Seems legit!
    let expected_sqls = [
        2, 4, 5, 6, 9, 15, 17, 18, 19, 20, 21, 23, 24, 25, 31, 32, 33, 36, 37, 42, 46,
    ]
    .into_iter()
    .map(|i| format!("SELECT {i}"))
    .collect::<Vec<_>>();

    let mut internal_client = server.connect_internal(postgres::NoTls).unwrap();
    let seh_query = "
        SELECT mst.sql
        FROM mz_internal.mz_statement_execution_history AS mseh
        JOIN
            mz_internal.mz_prepared_statement_history AS mpsh
            ON mseh.prepared_statement_id = mpsh.id
        JOIN
            mz_internal.mz_sql_text AS mst
            ON mpsh.sql_hash = mst.sql_hash
        WHERE mst.sql ~~ 'SELECT%' AND mst.sql !~~ '%mz_statement_execution_history%'
        ORDER BY mseh.began_at ASC";

    // Statement logging happens async, retry until we get the expected number of logged
    // statements.
    let mut sl = Vec::new();
    for _ in 0..10 {
        thread::sleep(Duration::from_secs(1));
        sl = internal_client.query(seh_query, &[]).unwrap();
        if sl.len() >= expected_sqls.len() {
            break;
        }
    }

    let sqls: Vec<String> = sl.into_iter().map(|r| r.get(0)).collect();
    assert_eq!(sqls, expected_sqls);

    // Verify the statement_logging_record_count metric correctly tracks sampled vs unsampled.
    // With 50% sampling and deterministic RNG, exactly 21 of 50 statements should be sampled.
    let (sampled_true, sampled_false) = get_statement_logging_record_counts(&server);
    assert_eq!(
        sampled_true, 21,
        "expected 21 statements to be sampled with 50% rate and deterministic RNG"
    );
    assert_eq!(
        sampled_false, 29,
        "expected 29 statements to not be sampled with 50% rate and deterministic RNG"
    );
}

#[mz_ore::test]
fn test_statement_logging_sampling() {
    let (server, client) = setup_statement_logging(1.0, 0.5, "");
    test_statement_logging_sampling_inner(server, client);
}

/// Test that we are not allowed to set `statement_logging_sample_rate`
/// arbitrarily high, but that it is constrained by `statement_logging_max_sample_rate`.
#[mz_ore::test]
fn test_statement_logging_sampling_constrained() {
    let (server, client) = setup_statement_logging(0.5, 1.0, "");
    test_statement_logging_sampling_inner(server, client);
}

/// Test that the `mz_statement_logging_unsampled_bytes` metric tracks the total bytes
/// of SQL text that would have been logged if statement logging were fully enabled.
/// We set `sample_rate=0.0` so no statements are actually sampled/logged, but the
/// unsampled_bytes metric still gets incremented for every executed statement.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_statement_logging_unsampled_metrics() {
    // Use sample_rate=0.0 so statements are not sampled, but unsampled_bytes metric is still tracked.
    let (server, mut client) = setup_statement_logging(1.0, 0.0, "");

    let batch_queries = [
        "SELECT 'Hello, world!';SELECT 1;;",
        "SELECT 'Hello, world again!'",
    ];
    let batch_total: usize = batch_queries
        .iter()
        .map(|s| s.as_bytes().iter().filter(|&&ch| ch != b';').count())
        .sum();
    let single_queries = ["SELECT 'foo'", "SELECT 'bar';;;"];
    let single_total: usize = single_queries
        .iter()
        .map(|s| s.as_bytes().iter().filter(|&&ch| ch != b';').count())
        .sum();
    let prepared_queries = ["SELECT 'baz';;;", "SELECT 'quux';"];
    let prepared_total: usize = prepared_queries
        .iter()
        .map(|s| s.as_bytes().iter().filter(|&&ch| ch != b';').count())
        .sum();

    let named_prepared_inner = "SELECT 42";
    let named_prepared_outer = format!("PREPARE p AS {named_prepared_inner};EXECUTE p;");
    let named_prepared_outer_len = named_prepared_outer
        .as_bytes()
        .iter()
        .filter(|&&ch| ch != b';')
        .count();

    for q in batch_queries {
        client.batch_execute(q).unwrap();
    }

    for q in single_queries {
        client.execute(q, &[]).unwrap();
    }

    for q in prepared_queries {
        let s = client.prepare(q).unwrap();
        client.execute(&s, &[]).unwrap();
    }

    client.batch_execute(&named_prepared_outer).unwrap();

    // This should NOT be logged, since we never actually execute it.
    client.prepare("SELECT 'Hello, not counted!'").unwrap();

    let expected_total = batch_total + single_total + prepared_total + named_prepared_outer_len;
    let metric_value = server
        .metrics_registry()
        .gather()
        .into_iter()
        .find(|m| m.name() == "mz_statement_logging_unsampled_bytes")
        .unwrap()
        .take_metric()[0]
        .get_counter()
        .value();
    let metric_value = usize::cast_from(u64::try_cast_from(metric_value).unwrap());
    assert_eq!(expected_total, metric_value);

    // Also verify that statement_logging_record_count shows all statements as not sampled
    // (since we're using 0% sample rate).
    let (sampled_true, _sampled_false) = get_statement_logging_record_counts(&server);
    assert_eq!(
        sampled_true, 0,
        "no statements should be sampled with 0% sample rate"
    );
}

#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_enable_internal_statement_logging() {
    let (server, mut client) = setup_statement_logging_core(
        1.0,
        1.0,
        "",
        test_util::TestHarness::default().with_system_parameter_default(
            "enable_internal_statement_logging".to_string(),
            "true".to_string(),
        ),
    );

    client.execute("SELECT 1", &[]).unwrap();

    let mut client = server.connect_internal(postgres::NoTls).unwrap();
    let num_mz_system_statements = Retry::default()
        .max_duration(Duration::from_secs(30))
        .retry(|_| {
            let sl_results = client
                .query(
                    "SELECT
    count(*)
FROM mz_internal.mz_prepared_statement_history mpsh
JOIN mz_internal.mz_session_history USING (session_id)
WHERE authenticated_user='mz_system'",
                    &[],
                )
                .unwrap();

            let count: i64 = sl_results[0].get(0);

            if count > 0 { Ok(count) } else { Err(()) }
        })
        .expect("at least some statements from mz_system should have been logged");

    assert!(
        num_mz_system_statements > 0,
        "statements executed by mz_system should have been logged"
    );
}

#[mz_ore::test]
// Test that statement logging
// doesn't cause a crash with subscribes over web sockets,
// which was previously happening (in staging) due to us
// dropping the `ExecuteContext` on the floor in that case.
fn test_statement_logging_ws_subscribe_no_crash() {
    let (server, _client) = setup_statement_logging(1.0, 1.0, "");

    // Create our WebSocket.
    let ws_url = server.server.ws_addr();
    let (mut ws, _resp) = tungstenite::connect(ws_url).unwrap();
    test_util::auth_with_ws(&mut ws, Default::default()).unwrap();

    let query = "SUBSCRIBE (SELECT 1)";
    let json = format!("{{\"query\":\"{query}\"}}");
    let json: serde_json::Value = serde_json::from_str(&json).unwrap();
    ws.send(Message::Text(json.to_string().into())).unwrap();

    // Give the server time to crash, if it's going to.
    std::thread::sleep(Duration::from_secs(1))
}
/// `finished_at` must record when execution finished, not when the coordinator
/// got around to the end event. A statement the session task retires itself
/// reports its own end timestamp, so a busy coordinator cannot inflate it.
///
/// The coordinator is stalled inside `Catalog::transact` while the measured
/// statement runs. That failpoint sits ahead of every catalog mutation, so the
/// catalog revision does not move while it sleeps and the session's cached
/// snapshot stays valid, which is what keeps the measured statement from
/// needing the coordinator at all.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_statement_logging_finished_at_excludes_coordinator_queue() {
    let (server, mut client) = setup_statement_logging(1.0, 1.0, "");

    // Populate this session's catalog snapshot cache, so the measured statement
    // below needs nothing from the stalled coordinator.
    client.execute("SELECT 1", &[]).unwrap();

    let mut ddl = server.connect_internal(postgres::NoTls).unwrap();
    fail::cfg("catalog_transact", "sleep(3000)").unwrap();
    let stall = thread::spawn(move || {
        let _ = ddl.batch_execute("CREATE TABLE stalls_the_coordinator (x int)");
    });
    // Give the DDL time to reach the failpoint before we measure.
    thread::sleep(Duration::from_millis(500));

    // Logged timestamps are epoch milliseconds and truncate, so floor the lower
    // bound and ceil the upper one rather than comparing against the
    // sub-millisecond instants `Utc::now` reports.
    let began_bound = Utc::now().timestamp_millis();
    client
        .execute("SELECT 1 AS finished_at_probe", &[])
        .unwrap();
    let finished_bound = Utc::now().timestamp_millis() + 1;

    stall.join().unwrap();
    fail::remove("catalog_transact");

    let mut internal = server.connect_internal(postgres::NoTls).unwrap();
    let query = "
        SELECT mseh.began_at, mseh.finished_at
        FROM mz_internal.mz_statement_execution_history AS mseh
        JOIN mz_internal.mz_prepared_statement_history AS mpsh
            ON mseh.prepared_statement_id = mpsh.id
        JOIN mz_internal.mz_sql_text AS mst ON mpsh.sql_hash = mst.sql_hash
        WHERE mst.sql ~~ '%finished_at_probe%' AND mseh.finished_at IS NOT NULL";

    let mut rows = Vec::new();
    for _ in 0..30 {
        rows = internal.query(query, &[]).unwrap();
        if !rows.is_empty() {
            break;
        }
        thread::sleep(Duration::from_secs(1));
    }
    assert_eq!(rows.len(), 1, "expected exactly one logged probe statement");

    let began_at: DateTime<Utc> = rows[0].get(0);
    let finished_at: DateTime<Utc> = rows[0].get(1);
    assert!(
        began_at.timestamp_millis() >= began_bound,
        "began_at precedes the client-observed start of the statement by {} ms",
        began_bound - began_at.timestamp_millis()
    );
    assert!(
        finished_at.timestamp_millis() <= finished_bound,
        "finished_at is {} ms past the client-observed end of the statement, so it \
         charges the statement for time its end event spent queued for the coordinator",
        finished_at.timestamp_millis() - finished_bound
    );
}

#[mz_ore::test]
fn test_statement_logging_frontend_constant_insert_sets_cluster() {
    let harness = test_util::TestHarness::default().with_system_parameter_default(
        "enable_adapter_frontend_occ_read_then_write".to_string(),
        "true".to_string(),
    );
    let (server, mut client) = setup_statement_logging_core(1.0, 1.0, "", harness);

    client.execute("SET CLUSTER TO quickstart", &[]).unwrap();
    client
        .execute(
            "CREATE TABLE statement_logging_constant_insert_t (x INT)",
            &[],
        )
        .unwrap();
    client
        .execute(
            "INSERT INTO statement_logging_constant_insert_t VALUES (1)",
            &[],
        )
        .unwrap();

    let mut client = server.connect_internal(postgres::NoTls).unwrap();
    let row = Retry::default()
        .max_duration(Duration::from_secs(30))
        .retry(|_| {
            let rows = client
                .query(
                    "SELECT mseh.cluster_name, mseh.finished_status
FROM mz_internal.mz_statement_execution_history AS mseh
LEFT JOIN mz_internal.mz_prepared_statement_history AS mpsh
    ON mseh.prepared_statement_id = mpsh.id
JOIN (SELECT DISTINCT sql, sql_hash FROM mz_internal.mz_sql_text) AS mst
    ON mpsh.sql_hash = mst.sql_hash
WHERE mst.sql ~~ 'INSERT INTO statement_logging_constant_insert_t%'
    AND mseh.finished_at IS NOT NULL
ORDER BY mseh.began_at DESC",
                    &[],
                )
                .unwrap();

            if let Some(row) = rows.into_iter().next() {
                Ok(row)
            } else {
                Err(())
            }
        })
        .expect("constant INSERT statement log entry should be recorded");

    let cluster_name: Option<String> = row.get(0);
    let finished_status: String = row.get(1);
    assert_eq!(cluster_name.as_deref(), Some("quickstart"));
    assert_eq!(finished_status, "success");
}

// Regression test: the frontend OCC read-then-write path must set
// `execution_timestamp` on the statement's log entry. The coordinator path does
// this through `set_statement_execution_timestamp` during group commit, so the
// frontend path has to emit the equivalent signal once its write commits.
#[mz_ore::test]
fn test_statement_logging_frontend_read_then_write_sets_execution_timestamp() {
    let harness = test_util::TestHarness::default().with_system_parameter_default(
        "enable_adapter_frontend_occ_read_then_write".to_string(),
        "true".to_string(),
    );
    let (server, mut client) = setup_statement_logging_core(1.0, 1.0, "", harness);

    client.execute("SET CLUSTER TO quickstart", &[]).unwrap();
    client
        .execute("CREATE TABLE statement_logging_rtw_t (x INT)", &[])
        .unwrap();
    client
        .execute("INSERT INTO statement_logging_rtw_t VALUES (1), (2)", &[])
        .unwrap();
    // DELETE goes through the frontend OCC read-then-write path.
    client
        .execute("DELETE FROM statement_logging_rtw_t WHERE x = 1", &[])
        .unwrap();

    let mut client = server.connect_internal(postgres::NoTls).unwrap();
    let row = Retry::default()
        .max_duration(Duration::from_secs(30))
        .retry(|_| {
            let rows = client
                .query(
                    "SELECT mseh.execution_timestamp, mseh.finished_status
FROM mz_internal.mz_statement_execution_history AS mseh
LEFT JOIN mz_internal.mz_prepared_statement_history AS mpsh
    ON mseh.prepared_statement_id = mpsh.id
JOIN (SELECT DISTINCT sql, sql_hash FROM mz_internal.mz_sql_text) AS mst
    ON mpsh.sql_hash = mst.sql_hash
WHERE mst.sql ~~ 'DELETE FROM statement_logging_rtw_t%'
    AND mseh.finished_at IS NOT NULL
ORDER BY mseh.began_at DESC",
                    &[],
                )
                .unwrap();

            if let Some(row) = rows.into_iter().next() {
                Ok(row)
            } else {
                Err(())
            }
        })
        .expect("DELETE statement log entry should be recorded");

    let execution_timestamp: Option<UInt8> = row.get(0);
    let finished_status: String = row.get(1);
    assert_eq!(finished_status, "success");
    assert!(
        execution_timestamp.is_some(),
        "frontend OCC read-then-write DELETE must set execution_timestamp, got NULL"
    );
}

/// One case in the DML statement-logging parity table.
struct DmlLoggingCase {
    /// Statements run before the one under test, to set up transaction state.
    /// These must succeed.
    before: &'static [&'static str],
    /// The statement under test. Its log row is found by the redacted form of
    /// this text, so no two cases may share it.
    sql: &'static str,
    /// Statements run after the one under test, to make the session usable
    /// again. These must succeed.
    after: &'static [&'static str],
    /// Whether only the frontend path records an `execution_timestamp` for this
    /// statement. It holds for every read-then-write that commits: the frontend
    /// emits the write timestamp when its write lands, while the coordinator
    /// retires the log entry in `sequence_read_then_write` and only reaches the
    /// group commit that would set the timestamp afterwards, by which time the
    /// entry is closed. The difference is pinned per case rather than excluded
    /// from the comparison, so a change on either path fails this test.
    frontend_only_execution_timestamp: bool,
}

/// DML whose statement-logging record must not depend on which path sequenced
/// it. Failures matter as much as successes: the two paths reject a statement
/// at different points, so their error exits are where they drift apart.
const DML_LOGGING_PARITY_CASES: &[DmlLoggingCase] = &[
    DmlLoggingCase {
        before: &[],
        sql: "UPDATE parity_t SET x = x + 1",
        after: &[],
        frontend_only_execution_timestamp: true,
    },
    DmlLoggingCase {
        before: &[],
        sql: "DELETE FROM parity_t WHERE x = 2",
        after: &[],
        frontend_only_execution_timestamp: true,
    },
    DmlLoggingCase {
        before: &[],
        sql: "INSERT INTO parity_t SELECT x + 10 FROM parity_t",
        after: &[],
        frontend_only_execution_timestamp: true,
    },
    // Reads nothing, so both paths stage the rows and the write timestamp is
    // chosen when the transaction commits rather than by the statement. Neither
    // path records an execution timestamp for it.
    DmlLoggingCase {
        before: &[],
        sql: "INSERT INTO parity_t VALUES (100) RETURNING x",
        after: &[],
        frontend_only_execution_timestamp: false,
    },
    // A RETURNING insert that matches no rows. Both paths report a row count
    // rather than an empty result set, so this pins the response kind (and with
    // it `rows_returned` and `result_size`) of the zero-row case.
    DmlLoggingCase {
        before: &[],
        sql: "INSERT INTO parity_t SELECT 101 WHERE false RETURNING x",
        after: &[],
        frontend_only_execution_timestamp: false,
    },
    // Rejected while describing the portal, before either path begins an
    // execution, so neither logs one.
    DmlLoggingCase {
        before: &[],
        sql: "UPDATE parity_t SET x = nonexistent_col",
        after: &[],
        frontend_only_execution_timestamp: false,
    },
    // Bounded staleness forbids writes. Both paths reject the statement after
    // they have planned it and recorded its cluster, so the error row carries a
    // cluster on both.
    DmlLoggingCase {
        before: &["SET transaction_isolation = 'bounded staleness 5s'"],
        sql: "DELETE FROM parity_t WHERE x > 1000",
        after: &["SET transaction_isolation = 'strict serializable'"],
        frontend_only_execution_timestamp: false,
    },
    DmlLoggingCase {
        before: &["BEGIN"],
        sql: "DELETE FROM parity_t",
        after: &["ROLLBACK"],
        frontend_only_execution_timestamp: false,
    },
];

/// The part of a statement's log row that both sequencing paths must agree on.
#[derive(Debug, PartialEq, Eq)]
struct DmlLoggingRecord {
    finished_status: String,
    error_message: Option<String>,
    /// Compared as "is it recorded at all", since the byte count itself is not
    /// a property of the path.
    result_size_is_null: bool,
    rows_returned: Option<i64>,
    execution_strategy: Option<String>,
    has_cluster: bool,
    has_execution_timestamp: bool,
}

/// SQL of the statement whose log row marks the end of the parity run. Once it
/// is visible, every earlier statement's row is too: the log's end-execution
/// events are recorded in the order the statements finished, and a flush
/// appends everything pending at once.
const DML_LOGGING_PARITY_SENTINEL: &str = "SELECT count(*) FROM parity_t";

/// The form of `sql` that statement logging records, which is what identifies a
/// statement's rows. DML is stored redacted.
fn redacted_sql(sql: &str) -> String {
    mz_sql::parse::parse(sql)
        .unwrap()
        .into_element()
        .ast
        .to_ast_string_redacted()
}

/// Reads the log row of the statement whose redacted SQL is `redacted_sql`.
///
/// `None` means the execution was not logged at all, which is a comparable
/// outcome: a statement rejected before execution begins has no row on either
/// path. Only meaningful once the sentinel row is visible.
fn read_dml_logging_record(
    mz_client: &mut postgres::Client,
    redacted_sql: &str,
) -> Option<DmlLoggingRecord> {
    let rows = mz_client
        .query(
            "SELECT
    mseh.finished_status,
    mseh.error_message,
    mseh.result_size IS NULL,
    mseh.rows_returned,
    mseh.execution_strategy,
    mseh.cluster_name IS NOT NULL,
    mseh.execution_timestamp IS NOT NULL
FROM mz_internal.mz_statement_execution_history AS mseh
JOIN mz_internal.mz_prepared_statement_history AS mpsh
    ON mseh.prepared_statement_id = mpsh.id
JOIN (SELECT DISTINCT sql_hash, redacted_sql FROM mz_internal.mz_sql_text) AS mst
    ON mpsh.sql_hash = mst.sql_hash
WHERE mst.redacted_sql = $1 AND mseh.finished_at IS NOT NULL",
            &[&redacted_sql],
        )
        .unwrap();

    assert!(
        rows.len() <= 1,
        "expected at most one log row for {redacted_sql}, got {}",
        rows.len()
    );
    rows.first().map(|row| DmlLoggingRecord {
        finished_status: row.get(0),
        error_message: row.get(1),
        result_size_is_null: row.get(2),
        rows_returned: row.get(3),
        execution_strategy: row.get(4),
        has_cluster: row.get(5),
        has_execution_timestamp: row.get(6),
    })
}

/// Runs [`DML_LOGGING_PARITY_CASES`] on a server configured with the given
/// value of the frontend OCC read-then-write flag, and returns each case's log
/// row in table order.
#[allow(clippy::disallowed_methods)]
fn collect_dml_logging_records(
    frontend_occ: bool,
) -> Vec<(&'static str, String, Option<DmlLoggingRecord>)> {
    let harness = test_util::TestHarness::default().with_system_parameter_default(
        "enable_adapter_frontend_occ_read_then_write".to_string(),
        frontend_occ.to_string(),
    );
    let (server, mut client) = setup_statement_logging_core(1.0, 1.0, "", harness);

    client.batch_execute("SET CLUSTER TO quickstart").unwrap();
    client
        .batch_execute("CREATE TABLE parity_t (x INT)")
        .unwrap();
    client
        .batch_execute("INSERT INTO parity_t VALUES (1), (2)")
        .unwrap();

    for case in DML_LOGGING_PARITY_CASES {
        for sql in case.before {
            client.batch_execute(sql).unwrap();
        }
        // Several cases fail on purpose. The log row is what the test reads, so
        // the client-visible outcome is deliberately ignored here.
        let _ = client.batch_execute(case.sql);
        for sql in case.after {
            client.batch_execute(sql).unwrap();
        }
    }
    client.batch_execute(DML_LOGGING_PARITY_SENTINEL).unwrap();

    let mut mz_client = server.connect_internal(postgres::NoTls).unwrap();
    let sentinel = redacted_sql(DML_LOGGING_PARITY_SENTINEL);
    Retry::default()
        .max_duration(Duration::from_secs(60))
        .retry(
            |_| match read_dml_logging_record(&mut mz_client, &sentinel) {
                Some(_) => Ok(()),
                None => Err(()),
            },
        )
        .expect("statement log should flush the sentinel row");

    DML_LOGGING_PARITY_CASES
        .iter()
        .map(|case| {
            let redacted_sql = redacted_sql(case.sql);
            let record = read_dml_logging_record(&mut mz_client, &redacted_sql);
            (case.sql, redacted_sql, record)
        })
        .collect()
}

// DELETE/UPDATE/INSERT..SELECT are sequenced either by the coordinator or by
// the session task, depending on a flag fixed at process startup. What they
// record in `mz_statement_execution_history` must not depend on that: the log
// is a user-visible product surface, and a customer querying it cannot tell
// which path ran.
#[mz_ore::test]
fn test_statement_logging_dml_path_parity() {
    let coordinator = collect_dml_logging_records(false);
    let frontend = collect_dml_logging_records(true);

    let mut mismatches = Vec::new();
    for (case, ((sql, redacted, mut coordinator), (_, _, frontend))) in std::iter::zip(
        DML_LOGGING_PARITY_CASES,
        std::iter::zip(coordinator, frontend),
    ) {
        if case.frontend_only_execution_timestamp {
            let timestamps = (
                coordinator
                    .as_ref()
                    .map(|record| record.has_execution_timestamp),
                frontend
                    .as_ref()
                    .map(|record| record.has_execution_timestamp),
            );
            assert_eq!(
                timestamps,
                (Some(false), Some(true)),
                "{sql} is marked as recording an execution_timestamp on the frontend path only, \
                 but the paths report {timestamps:?}"
            );
            coordinator
                .as_mut()
                .expect("checked above")
                .has_execution_timestamp = true;
        }
        if coordinator != frontend {
            mismatches.push(format!(
                "{sql} (logged as {redacted}):\n  coordinator: {coordinator:?}\n  frontend:    {frontend:?}"
            ));
        }
    }
    assert!(
        mismatches.is_empty(),
        "statement log rows differ between the coordinator and the frontend OCC path:\n{}",
        mismatches.join("\n")
    );
}

/// Statement-logging outcome of one execution, as recorded once the log
/// flushes.
#[derive(Debug)]
struct StatementOutcome {
    finished_status: String,
    error_message: Option<String>,
}

/// Reads the outcomes of every finished execution whose logged SQL matches the
/// `LIKE` pattern `sql_pattern`, waiting for at least one to show up.
fn read_statement_outcomes(
    mz_client: &mut postgres::Client,
    sql_pattern: &str,
) -> Vec<StatementOutcome> {
    Retry::default()
        .max_duration(Duration::from_secs(60))
        .retry(|_| {
            let rows = mz_client
                .query(
                    "SELECT mseh.finished_status, mseh.error_message
FROM mz_internal.mz_statement_execution_history AS mseh
JOIN mz_internal.mz_prepared_statement_history AS mpsh
    ON mseh.prepared_statement_id = mpsh.id
JOIN (SELECT DISTINCT sql_hash, sql FROM mz_internal.mz_sql_text) AS mst
    ON mpsh.sql_hash = mst.sql_hash
WHERE mst.sql LIKE $1 AND mseh.finished_at IS NOT NULL",
                    &[&sql_pattern],
                )
                .unwrap();
            if rows.is_empty() {
                return Err(());
            }
            Ok(rows
                .into_iter()
                .map(|row| StatementOutcome {
                    finished_status: row.get(0),
                    error_message: row.get(1),
                })
                .collect::<Vec<_>>())
        })
        .unwrap_or_else(|_| panic!("no finished log row matching {sql_pattern}"))
}

// A cancelled frontend-sequenced write must be logged as the error the user
// received. Recording `aborted` instead loses the reason: `aborted` is the
// status for an execution whose outcome we never learned, and it carries no
// error message.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_statement_logging_cancel_frontend_read_then_write() {
    let harness = test_util::TestHarness::default()
        .unsafe_mode()
        .with_system_parameter_default(
            "enable_adapter_frontend_occ_read_then_write".to_string(),
            "true".to_string(),
        )
        .with_system_parameter_default(
            "unsafe_enable_unsafe_functions".to_string(),
            "true".to_string(),
        );
    let (server, mut client) = setup_statement_logging_core(1.0, 1.0, "", harness);

    client
        .batch_execute("CREATE TABLE cancel_logging_t (a TEXT, ts INT)")
        .unwrap();
    client
        .batch_execute("INSERT INTO cancel_logging_t VALUES ('hello', 10)")
        .unwrap();

    let cancel_token = client.cancel_token();
    let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
    let cancel_thread = thread::spawn(move || {
        // The write below sleeps for ten seconds, so the first cancel lands
        // well after it registered its cancellation watch. Cancelling before
        // that would exercise a different exit.
        thread::sleep(Duration::from_secs(1));
        loop {
            match shutdown_rx.try_recv() {
                Ok(()) | Err(std::sync::mpsc::TryRecvError::Disconnected) => return,
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    let _ = cancel_token.cancel_query(postgres::NoTls);
                }
            }
            thread::sleep(Duration::from_millis(500));
        }
    });

    let err = client
        .batch_execute(
            "INSERT INTO cancel_logging_t SELECT a, CASE WHEN mz_unsafe.mz_sleep(ts) > 0 THEN 0 END AS ts FROM cancel_logging_t",
        )
        .unwrap_err();
    assert_eq!(err.code(), Some(&SqlState::QUERY_CANCELED));

    shutdown_tx.send(()).unwrap();
    cancel_thread.join().unwrap();

    let mut mz_client = server.connect_internal(postgres::NoTls).unwrap();
    let outcomes = read_statement_outcomes(&mut mz_client, "INSERT INTO cancel_logging_t SELECT%");
    assert_eq!(outcomes.len(), 1, "unexpected log rows: {outcomes:?}");
    let outcome = &outcomes[0];
    assert_ne!(
        outcome.finished_status, "aborted",
        "cancellation must not be recorded as an unknown outcome: {outcome:?}"
    );
    assert_eq!(outcome.finished_status, "error", "{outcome:?}");
    assert_eq!(
        outcome.error_message.as_deref(),
        Some("canceling statement due to user request"),
        "{outcome:?}"
    );
}

// Same as cancellation, for the statement timeout: the log must carry the
// error the user received, not `aborted` with no message.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_statement_logging_timeout_frontend_read_then_write() {
    let harness = test_util::TestHarness::default()
        .unsafe_mode()
        .with_system_parameter_default(
            "enable_adapter_frontend_occ_read_then_write".to_string(),
            "true".to_string(),
        )
        .with_system_parameter_default(
            "unsafe_enable_unsafe_functions".to_string(),
            "true".to_string(),
        );
    let (server, mut client) = setup_statement_logging_core(1.0, 1.0, "", harness);

    client
        .batch_execute("CREATE TABLE timeout_logging_t (a TEXT, ts INT)")
        .unwrap();
    client
        .batch_execute("INSERT INTO timeout_logging_t VALUES ('hello', 10)")
        .unwrap();
    client
        .batch_execute("SET statement_timeout = '5s'")
        .unwrap();

    let err = client
        .batch_execute(
            "INSERT INTO timeout_logging_t SELECT a, CASE WHEN mz_unsafe.mz_sleep(ts) > 0 THEN 0 END AS ts FROM timeout_logging_t",
        )
        .unwrap_err();
    assert_contains!(err.to_string_with_causes(), "statement timeout");

    let mut mz_client = server.connect_internal(postgres::NoTls).unwrap();
    let outcomes = read_statement_outcomes(&mut mz_client, "INSERT INTO timeout_logging_t SELECT%");
    assert_eq!(outcomes.len(), 1, "unexpected log rows: {outcomes:?}");
    let outcome = &outcomes[0];
    assert_ne!(
        outcome.finished_status, "aborted",
        "a statement timeout must not be recorded as an unknown outcome: {outcome:?}"
    );
    assert_eq!(outcome.finished_status, "error", "{outcome:?}");
    assert_eq!(
        outcome.error_message.as_deref(),
        Some("canceling statement due to statement timeout"),
        "{outcome:?}"
    );
}

// A statement that fails before the frontend commits to executing it is still
// the frontend's to record: the coordinator never sees it, so nothing else
// would. DML in an explicit transaction block is such a statement, rejected
// right after the frontend takes it over.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_statement_logging_frontend_read_then_write_transaction_error() {
    let harness = test_util::TestHarness::default().with_system_parameter_default(
        "enable_adapter_frontend_occ_read_then_write".to_string(),
        "true".to_string(),
    );
    let (server, mut client) = setup_statement_logging_core(1.0, 1.0, "", harness);

    client
        .batch_execute("CREATE TABLE txn_error_t (x INT)")
        .unwrap();
    client.batch_execute("BEGIN").unwrap();
    let err = client.batch_execute("DELETE FROM txn_error_t").unwrap_err();
    assert_contains!(
        err.to_string_with_causes(),
        "cannot be run inside a transaction block"
    );
    client.batch_execute("ROLLBACK").unwrap();

    let mut mz_client = server.connect_internal(postgres::NoTls).unwrap();
    let outcomes = read_statement_outcomes(&mut mz_client, "DELETE FROM txn_error_t");
    assert_eq!(outcomes.len(), 1, "unexpected log rows: {outcomes:?}");
    let outcome = &outcomes[0];
    assert_eq!(outcome.finished_status, "error", "{outcome:?}");
    assert_contains!(
        outcome.error_message.as_deref().unwrap_or_default(),
        "DELETE FROM txn_error_t cannot be run inside a transaction block"
    );
}

// An RBAC denial is another exit that happens after the frontend takes the
// statement over.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_statement_logging_frontend_read_then_write_rbac_error() {
    let harness = test_util::TestHarness::default().with_system_parameter_default(
        "enable_adapter_frontend_occ_read_then_write".to_string(),
        "true".to_string(),
    );
    let (server, mut client) = setup_statement_logging_core(1.0, 1.0, "", harness);

    client.batch_execute("CREATE TABLE rbac_t (x INT)").unwrap();

    // The grants go through the system user: the session that owns the table
    // does not own the schema, database and cluster the role also needs.
    let mut mz_client = server.connect_internal(postgres::NoTls).unwrap();
    mz_client
        .batch_execute("CREATE ROLE rbac_role INHERIT")
        .unwrap();
    // Everything the UPDATE needs except UPDATE on the table itself, so that
    // the privilege check on the table is what rejects it.
    for grant in [
        "GRANT SELECT ON TABLE rbac_t TO rbac_role",
        "GRANT USAGE ON SCHEMA public TO rbac_role",
        "GRANT USAGE ON DATABASE materialize TO rbac_role",
        "GRANT USAGE ON CLUSTER quickstart TO rbac_role",
    ] {
        mz_client.batch_execute(grant).unwrap();
    }

    let mut rbac_client = server
        .pg_config()
        .user("rbac_role")
        .connect(postgres::NoTls)
        .unwrap();
    let err = rbac_client
        .batch_execute("UPDATE rbac_t SET x = 1")
        .unwrap_err();
    assert_contains!(err.to_string_with_causes(), "permission denied");

    let outcomes = read_statement_outcomes(&mut mz_client, "UPDATE rbac_t%");
    assert_eq!(outcomes.len(), 1, "unexpected log rows: {outcomes:?}");
    let outcome = &outcomes[0];
    assert_eq!(outcome.finished_status, "error", "{outcome:?}");
    assert_contains!(
        outcome.error_message.as_deref().unwrap_or_default(),
        "permission denied"
    );
}

// A prepared DML statement that no frontend path handles. `EXECUTE` is
// unrolled in the session task, which takes over the EXECUTE's log entry, and
// the inner statement then falls back to the coordinator. The coordinator has
// to receive that entry and finish it, and the two statements have to be
// counted once each: the EXECUTE by the session task, the inner statement by
// the coordinator.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_statement_logging_prepared_dml_coordinator_fallback() {
    let (server, mut client) = setup_statement_logging(1.0, 1.0, "");

    client
        .batch_execute("CREATE TABLE prepared_dml_t (x INT)")
        .unwrap();
    client
        .batch_execute("PREPARE p AS INSERT INTO prepared_dml_t VALUES (1)")
        .unwrap();

    let insert_labels = [("session_type", "user"), ("statement_type", "insert")];
    let execute_labels = [("session_type", "user"), ("statement_type", "execute")];
    let inserts_before =
        test_util::get_counter_value(server.metrics_registry(), "mz_query_total", &insert_labels);
    let executes_before =
        test_util::get_counter_value(server.metrics_registry(), "mz_query_total", &execute_labels);

    client.batch_execute("EXECUTE p").unwrap();

    let rows: i64 = client
        .query_one("SELECT count(*) FROM prepared_dml_t", &[])
        .unwrap()
        .get(0);
    assert_eq!(rows, 1);
    assert_eq!(
        test_util::get_counter_value(server.metrics_registry(), "mz_query_total", &insert_labels),
        inserts_before + 1
    );
    assert_eq!(
        test_util::get_counter_value(server.metrics_registry(), "mz_query_total", &execute_labels),
        executes_before + 1
    );

    let mut mz_client = server.connect_internal(postgres::NoTls).unwrap();
    let outcomes = read_statement_outcomes(&mut mz_client, "EXECUTE p");
    assert_eq!(outcomes.len(), 1, "unexpected log rows: {outcomes:?}");
    assert_eq!(outcomes[0].finished_status, "success", "{:?}", outcomes[0]);
}
