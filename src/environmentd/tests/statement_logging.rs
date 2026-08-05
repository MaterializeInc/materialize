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
use mz_ore::assert_none;
use mz_ore::cast::{CastFrom, CastLossy, TryCastFrom};
use mz_ore::collections::CollectionExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::to_datetime;
use mz_ore::retry::Retry;
use mz_pgrepr::UInt8;
use mz_sql_parser::ast::display::AstDisplay;
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
