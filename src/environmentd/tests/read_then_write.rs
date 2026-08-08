// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for read-then-write statements: `DELETE`, `UPDATE` and
//! `INSERT ... SELECT`, plus the constant `INSERT` that shares their planning
//! path.
//!
//! Most tests here enable `enable_adapter_frontend_occ_read_then_write` and so
//! cover the frontend OCC path. `test_counts_query_total` runs with the flag
//! both off and on, because the property it checks must hold whichever path
//! sequenced the statement. `test_cancel_read_then_write` covers the
//! coordinator path only, and is the other half of the cancellation behavior
//! its OCC counterpart pins.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use mz_environmentd::test_util;
use mz_ore::assert_contains;
use mz_ore::error::ErrorExt;
use mz_ore::retry::Retry;
use tokio_postgres::error::SqlState;

/// A harness with frontend OCC read-then-write enabled and nothing else.
///
/// Callers add what they need on top, notably `unsafe_mode` for the tests that
/// hold a statement open with `mz_unsafe` functions.
fn frontend_occ_harness() -> test_util::TestHarness {
    test_util::TestHarness::default().with_system_parameter_default(
        "enable_adapter_frontend_occ_read_then_write".to_string(),
        "true".to_string(),
    )
}

/// The server's message for a client error, or the client-side rendering when
/// the error never reached the server. `postgres::Error::to_string` is only "db
/// error" for a server error, so matching on it tells us nothing.
fn server_error_message(err: &postgres::Error) -> String {
    match err.as_db_error() {
        Some(db_error) => db_error.message().to_string(),
        None => err.to_string(),
    }
}

// `mz_query_total` feeds product telemetry, and DML the session task sequences
// itself must be counted there exactly once, like DML the coordinator
// sequences.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_counts_query_total() {
    for frontend_occ in [false, true] {
        let server = test_util::TestHarness::default()
            .with_system_parameter_default(
                "enable_adapter_frontend_occ_read_then_write".to_string(),
                frontend_occ.to_string(),
            )
            .start_blocking();
        let mut client = server.connect(postgres::NoTls).unwrap();
        client
            .batch_execute("CREATE TABLE query_total_t (x INT)")
            .unwrap();

        for (statement_type, sql) in [
            ("insert", "INSERT INTO query_total_t SELECT 1"),
            ("update", "UPDATE query_total_t SET x = 2"),
            ("delete", "DELETE FROM query_total_t"),
        ] {
            let labels = [("session_type", "user"), ("statement_type", statement_type)];
            let before =
                test_util::get_counter_value(server.metrics_registry(), "mz_query_total", &labels);
            client.batch_execute(sql).unwrap();
            let after =
                test_util::get_counter_value(server.metrics_registry(), "mz_query_total", &labels);
            assert_eq!(
                after,
                before + 1,
                "mz_query_total{{statement_type={statement_type}}} moved from {before} to {after} \
                 across `{sql}`, with frontend OCC read-then-write {frontend_occ}"
            );
        }
    }
}

// Test that frontend-sequenced read-then-write statements honor pgwire cancel
// requests and do not run to completion after cancellation.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_cancel_long_running_write() {
    let server = frontend_occ_harness().unsafe_mode().start_blocking();
    server.enable_feature_flags(&["unsafe_enable_unsafe_functions"]);

    let mut client = server.connect(postgres::NoTls).unwrap();
    let cancel_token = client.cancel_token();

    client
        .batch_execute("CREATE TABLE t (a TEXT, ts INT)")
        .unwrap();
    client
        .batch_execute("INSERT INTO t VALUES ('hello', 10)")
        .unwrap();

    let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
    let cancel_thread = thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_millis(200));
            match shutdown_rx.try_recv() {
                Ok(()) => return,
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    let _ = cancel_token.cancel_query(postgres::NoTls);
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => return,
            }
        }
    });

    match client.batch_execute(
        "INSERT INTO t SELECT a, CASE WHEN mz_unsafe.mz_sleep(ts) > 0 THEN 0 END AS ts FROM t",
    ) {
        Err(e) if e.code() == Some(&SqlState::QUERY_CANCELED) => {}
        Err(e) => panic!("expected error SqlState::QUERY_CANCELED, but got {e:?}"),
        Ok(_) => panic!("expected error SqlState::QUERY_CANCELED, but query succeeded"),
    }

    shutdown_tx.send(()).unwrap();
    cancel_thread.join().unwrap();

    // The last cancel request the thread sent is processed asynchronously, so
    // it can still land on this read-back. Retry it in that case.
    let rows = Retry::default()
        .max_tries(5)
        .clamp_backoff(Duration::from_millis(100))
        .retry(|_| client.query_one("SELECT count(*) FROM t", &[]))
        .unwrap()
        .get::<_, i64>(0);
    assert_eq!(
        rows, 1,
        "cancelled statement should not have committed writes"
    );

    // NOTE: mz_sleep with a constant ts gets evaluated differently. This gives
    // us additional coverage for cancelling at different moments in the
    // processing pipeline.
    let cancel_token = client.cancel_token();
    let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
    let cancel_thread = thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_millis(200));
            match shutdown_rx.try_recv() {
                Ok(()) => return,
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    let _ = cancel_token.cancel_query(postgres::NoTls);
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => return,
            }
        }
    });

    match client.batch_execute(
        "INSERT INTO t SELECT a, CASE WHEN mz_unsafe.mz_sleep(10) > 0 THEN 0 END AS ts FROM t",
    ) {
        Err(e) if e.code() == Some(&SqlState::QUERY_CANCELED) => {}
        Err(e) => panic!("expected error SqlState::QUERY_CANCELED, but got {e:?}"),
        Ok(_) => panic!("expected error SqlState::QUERY_CANCELED, but query succeeded"),
    }

    shutdown_tx.send(()).unwrap();
    cancel_thread.join().unwrap();

    let rows = Retry::default()
        .max_tries(5)
        .clamp_backoff(Duration::from_millis(100))
        .retry(|_| client.query_one("SELECT count(*) FROM t", &[]))
        .unwrap()
        .get::<_, i64>(0);
    assert_eq!(
        rows, 1,
        "cancelled statement should not have committed writes"
    );

    // The read-then-write ran its selection through an internal subscribe, so a
    // `SubscribeHandle` whose drop never reached the coordinator would leave
    // that dataflow installed.
    wait_for_no_dataflows(&mut client, "after cancelling a read-then-write");
}

#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_constant_insert_prepares_unmaterializable_functions() {
    let server = frontend_occ_harness().unsafe_mode().start_blocking();

    let mut client = server.connect(postgres::NoTls).unwrap();

    client.batch_execute("CREATE TABLE t (u text)").unwrap();
    client.batch_execute("BEGIN").unwrap();
    client
        .execute("INSERT INTO t VALUES (current_user())", &[])
        .unwrap();
    client.batch_execute("COMMIT").unwrap();

    let inserted_matches_current_user = client
        .query_one("SELECT u = current_user() FROM t", &[])
        .unwrap()
        .get::<_, bool>(0);
    assert!(inserted_matches_current_user);
}

#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_rejected_in_multi_statement_batch() {
    let server = frontend_occ_harness().unsafe_mode().start_blocking();

    let mut client = server.connect(postgres::NoTls).unwrap();

    client.batch_execute("CREATE TABLE t (a int)").unwrap();
    client.batch_execute("INSERT INTO t VALUES (1)").unwrap();

    // Non-constant DML in a multi-statement implicit transaction (a simple
    // query batch) is prohibited, matching the coordinator's transaction
    // gate. Allowing it into the OCC path would commit the write durably
    // mid-batch, breaking the batch's atomicity.
    let err = client
        .batch_execute("INSERT INTO t SELECT * FROM t; SELECT 1")
        .unwrap_err();
    let db_err = err.as_db_error().expect("expected db error");
    assert!(
        db_err
            .message()
            .contains("cannot be run inside a transaction block"),
        "unexpected error: {err:?}"
    );

    // Constant INSERTs join the implicit transaction's write ops, so a later
    // error in the batch rolls them back.
    let err = client
        .batch_execute("INSERT INTO t VALUES (2); SELECT 1/0")
        .unwrap_err();
    let db_err = err.as_db_error().expect("expected db error");
    assert!(
        db_err.message().contains("division by zero"),
        "unexpected error: {err:?}"
    );

    let count = client
        .query_one("SELECT count(*)::int4 FROM t", &[])
        .unwrap()
        .get::<_, i32>(0);
    assert_eq!(count, 1, "no batch write may have committed");
}

#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_constant_insert_respects_max_result_size() {
    let server = frontend_occ_harness()
        .unsafe_mode()
        .with_system_parameter_default("max_result_size".to_string(), "1MB".to_string())
        .start_blocking();

    let mut client = server.connect(postgres::NoTls).unwrap();

    client
        .batch_execute("CREATE TABLE t2 (a int4, b text)")
        .unwrap();

    let err = client
        .execute(
            "INSERT INTO t2 SELECT * FROM generate_series(1, 10001), repeat('a', 100)",
            &[],
        )
        .unwrap_err();
    let db_err = err.as_db_error().expect("expected db error");
    assert!(
        db_err
            .message()
            .contains("result exceeds max size of 1.0 MiB"),
        "unexpected error: {err:?}"
    );
}

#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_constant_insert_rejects_mz_now() {
    let server = frontend_occ_harness().unsafe_mode().start_blocking();

    let mut client = server.connect(postgres::NoTls).unwrap();

    client
        .batch_execute("CREATE TABLE dec (d mz_timestamp)")
        .unwrap();

    let err = client
        .execute("INSERT INTO dec VALUES (mz_now())", &[])
        .unwrap_err();
    let db_err = err.as_db_error().expect("expected db error");
    assert!(
        db_err
            .message()
            .contains("calls to mz_now in write statements"),
        "unexpected error: {err:?}"
    );
}

#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_returning_error_does_not_commit_write() {
    let server = frontend_occ_harness().unsafe_mode().start_blocking();

    let mut client = server.connect(postgres::NoTls).unwrap();

    client
        .batch_execute("CREATE TABLE t (a INT, b INT)")
        .unwrap();

    let err = client
        .query("INSERT INTO t VALUES (7, 8) RETURNING 1/0", &[])
        .unwrap_err();
    let db_err = err.as_db_error().expect("expected db error");
    assert!(
        db_err.message().contains("division by zero"),
        "unexpected error message: {:?}",
        db_err.message()
    );

    let rows = client
        .query_one("SELECT count(*) FROM t", &[])
        .unwrap()
        .get::<_, i64>(0);
    assert_eq!(rows, 0, "failing RETURNING must not commit the write");
}

// Regression test for the empty-snapshot branch of the OCC loop.
//
// `ActiveSubscribe::initialize` emits a progress message at `as_of` before
// any data batch is processed, so the OCC loop must not conclude
// `NoRowsMatched` on that first progress, the snapshot hasn't been
// delivered yet. The check that distinguishes "initial progress" from
// "snapshot complete and empty" is `ts > as_of`. This test exercises both
// empty-match cases and asserts the operations return zero without
// hanging or writing.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_empty_snapshot_returns_zero() {
    let server = frontend_occ_harness().start_blocking();

    let mut client = server.connect(postgres::NoTls).unwrap();

    client.batch_execute("CREATE TABLE t (x INT)").unwrap();

    // DELETE on a completely empty table.
    let deleted = client
        .execute("DELETE FROM t", &[])
        .expect("DELETE on empty table should return 0 rows");
    assert_eq!(deleted, 0, "DELETE on empty table must report 0 rows");

    // DELETE with a WHERE clause that matches no rows against a non-empty
    // table. The snapshot is non-empty (contains row (1)) but the selection
    // is empty after filtering.
    client.batch_execute("INSERT INTO t VALUES (1)").unwrap();
    let deleted = client
        .execute("DELETE FROM t WHERE x = 999", &[])
        .expect("DELETE with no matches should return 0 rows");
    assert_eq!(deleted, 0, "DELETE with no matches must report 0 rows");

    // UPDATE with a WHERE clause that matches no rows.
    let updated = client
        .execute("UPDATE t SET x = 2 WHERE x = 999", &[])
        .expect("UPDATE with no matches should return 0 rows");
    assert_eq!(updated, 0, "UPDATE with no matches must report 0 rows");

    // The original row is still there.
    let rows = client
        .query_one("SELECT count(*) FROM t", &[])
        .unwrap()
        .get::<_, i64>(0);
    assert_eq!(rows, 1);
}

// End-to-end coverage of the OCC retry path:
//
// N concurrent connections each issue M `UPDATE counter SET v = v + 1`
// statements against the same single-row table. Without a working
// `TimestampPassed` retry loop this would lose updates (two writers reading
// `v = k` and both committing `v = k + 1`). The final value pinning down at
// `N * M` proves retries actually re-read fresh state and re-apply the diff.
//
// Also asserts the `mz_occ_read_then_write_retry_count` histogram observes
// every UPDATE and that at least one observation reports a retry, so the
// retry-count metric stays wired up to the OCC loop.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_concurrent_updates_retry() {
    const NUM_WORKERS: usize = 4;
    const UPDATES_PER_WORKER: usize = 25;

    let server = frontend_occ_harness().start_blocking();

    let mut setup = server.connect(postgres::NoTls).unwrap();
    setup
        .batch_execute("CREATE TABLE counter (id INT, v INT)")
        .unwrap();
    setup
        .batch_execute("INSERT INTO counter VALUES (1, 0)")
        .unwrap();

    let mut handles = Vec::with_capacity(NUM_WORKERS);
    for _ in 0..NUM_WORKERS {
        let mut client = server.connect(postgres::NoTls).unwrap();
        handles.push(thread::spawn(move || {
            for _ in 0..UPDATES_PER_WORKER {
                client
                    .execute("UPDATE counter SET v = v + 1 WHERE id = 1", &[])
                    .expect("UPDATE under contention should succeed via OCC retry");
            }
        }));
    }
    for handle in handles {
        handle.join().expect("worker thread panicked");
    }

    let final_v: i32 = setup
        .query_one("SELECT v FROM counter WHERE id = 1", &[])
        .unwrap()
        .get(0);
    let expected = i32::try_from(NUM_WORKERS * UPDATES_PER_WORKER).unwrap();
    assert_eq!(
        final_v, expected,
        "concurrent OCC UPDATEs lost updates: expected {expected}, got {final_v}",
    );

    // Inspect the OCC retry-count histogram. Every UPDATE that took the
    // frontend OCC path should produce exactly one observation, so
    // sample_count must be >= NUM_WORKERS * UPDATES_PER_WORKER. Same-row
    // contention essentially guarantees at least one observation lands above
    // the 0-retry bucket, so we assert that too.
    let metrics = server.metrics_registry().gather();
    let retry_metric = metrics
        .iter()
        .find(|m| m.name() == "mz_occ_read_then_write_retry_count")
        .expect("mz_occ_read_then_write_retry_count metric should be registered");
    let metric = retry_metric.get_metric();
    assert_eq!(metric.len(), 1, "expected a single histogram series");
    let histogram = metric[0].get_histogram();

    let total_updates = u64::try_from(NUM_WORKERS * UPDATES_PER_WORKER).unwrap();
    assert!(
        histogram.get_sample_count() >= total_updates,
        "expected at least {} OCC observations, got {}",
        total_updates,
        histogram.get_sample_count(),
    );

    let zero_retry_bucket = histogram
        .get_bucket()
        .iter()
        .find(|b| b.upper_bound() == 0.0)
        .expect("histogram should have a 0-retry bucket");
    assert!(
        zero_retry_bucket.cumulative_count() < histogram.get_sample_count(),
        "expected at least one UPDATE to retry under contention. \
         all {} observations landed in the 0-retry bucket",
        histogram.get_sample_count(),
    );
}

// A frontend OCC read-then-write that times out must not commit its writes.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_statement_timeout_does_not_commit_write() {
    let server = frontend_occ_harness().unsafe_mode().start_blocking();
    server.enable_feature_flags(&["unsafe_enable_unsafe_functions"]);

    let mut client = server.connect(postgres::NoTls).unwrap();
    client
        .batch_execute("CREATE TABLE frontend_timeout (a TEXT, ts INT);")
        .unwrap();
    client
        .batch_execute("INSERT INTO frontend_timeout VALUES ('hello', 10)")
        .unwrap();
    client
        .batch_execute("SET statement_timeout = '5s'")
        .unwrap();

    let err = client
        .batch_execute(
            "INSERT INTO frontend_timeout SELECT a, CASE WHEN mz_unsafe.mz_sleep(ts) > 0 THEN 0 END AS ts FROM frontend_timeout",
        )
        .unwrap_err();
    assert_contains!(err.to_string_with_causes(), "statement timeout");

    let rows: i64 = client
        .query_one("SELECT count(*) FROM frontend_timeout", &[])
        .unwrap()
        .get(0);
    assert_eq!(rows, 1, "timed-out statement committed writes");
}

/// Concurrent DELETEs of the same multiset rows must not over-delete.
/// With a row of multiplicity M and N concurrent deleters, exactly the
/// committed deletes should sum to M, the table must end empty, and the
/// stored multiplicity must never go negative.
///
/// The sum oracle alone cannot distinguish "no over-deletion under concurrency"
/// from "there was no concurrency": workers running one after another satisfy it
/// too. So the clients are connected before any worker starts, released
/// together by a barrier, and the OCC retry histogram must show at least one
/// attempt that saw its read timestamp move.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_concurrent_delete_does_not_over_delete() {
    const MULTIPLICITY: i32 = 7;
    const NUM_WORKERS: usize = 6;
    const ROUNDS: usize = 10;

    let server = frontend_occ_harness().unsafe_mode().start_blocking();
    let mut setup = server.connect(postgres::NoTls).unwrap();
    setup.batch_execute("CREATE TABLE t (id INT)").unwrap();

    for _round in 0..ROUNDS {
        setup.batch_execute("DELETE FROM t").unwrap();
        setup
            .execute(
                "INSERT INTO t SELECT 1 FROM generate_series(1, $1)",
                &[&MULTIPLICITY],
            )
            .unwrap();

        // Connecting inside the spawn loop would let the first worker finish
        // its DELETE before the last client even exists.
        let clients: Vec<_> = (0..NUM_WORKERS)
            .map(|_| server.connect(postgres::NoTls).unwrap())
            .collect();

        let barrier = Arc::new(Barrier::new(NUM_WORKERS));
        let total_deleted = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for mut client in clients {
            let barrier = Arc::clone(&barrier);
            let total_deleted = Arc::clone(&total_deleted);
            handles.push(thread::spawn(move || {
                barrier.wait();
                let n = client
                    .execute("DELETE FROM t WHERE id = 1", &[])
                    .expect("DELETE under contention should succeed via OCC");
                total_deleted.fetch_add(usize::try_from(n).unwrap(), Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.join().expect("worker panicked");
        }

        let remaining: i64 = setup
            .query_one("SELECT count(*) FROM t", &[])
            .unwrap()
            .get(0);
        assert_eq!(
            remaining, 0,
            "table should be empty after concurrent deletes"
        );
        assert_eq!(
            total_deleted.load(Ordering::SeqCst),
            usize::try_from(MULTIPLICITY).unwrap(),
            "sum of reported deletes must equal initial multiplicity (no over/under-delete)",
        );
    }

    // Proof that the deletes actually raced: with all workers deleting the same
    // rows at once, at least one attempt must have found its read timestamp
    // passed and retried against fresh state.
    let metrics = server.metrics_registry().gather();
    let retry_metric = metrics
        .iter()
        .find(|m| m.name() == "mz_occ_read_then_write_retry_count")
        .expect("mz_occ_read_then_write_retry_count metric should be registered");
    let metric = retry_metric.get_metric();
    assert_eq!(metric.len(), 1, "expected a single histogram series");
    let histogram = metric[0].get_histogram();
    let zero_retry_bucket = histogram
        .get_bucket()
        .iter()
        .find(|b| b.upper_bound() == 0.0)
        .expect("histogram should have a 0-retry bucket");
    assert!(
        zero_retry_bucket.cumulative_count() < histogram.get_sample_count(),
        "expected at least one DELETE to retry under contention. \
         all {} observations landed in the 0-retry bucket, so the workers \
         did not actually run concurrently",
        histogram.get_sample_count(),
    );
}

/// Multiset multiplicity must be reflected in affected-row counts for
/// DELETE / UPDATE / INSERT...SELECT.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_duplicate_row_multiplicity_counts() {
    let server = frontend_occ_harness().unsafe_mode().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();
    client
        .batch_execute("CREATE TABLE t (a INT, b INT)")
        .unwrap();

    // 3 identical rows.
    client
        .execute("INSERT INTO t SELECT 1, 10 FROM generate_series(1, 3)", &[])
        .unwrap();

    // UPDATE all 3 -> Updated(3).
    let n = client
        .execute("UPDATE t SET b = 20 WHERE a = 1", &[])
        .unwrap();
    assert_eq!(
        n, 3,
        "UPDATE should report 3 affected rows for multiplicity 3"
    );

    // INSERT INTO t SELECT * FROM t -> doubles, returns 3.
    let n = client
        .execute("INSERT INTO t SELECT a, b FROM t", &[])
        .unwrap();
    assert_eq!(n, 3, "INSERT...SELECT should report 3 inserted rows");
    let cnt: i64 = client
        .query_one("SELECT count(*) FROM t", &[])
        .unwrap()
        .get(0);
    assert_eq!(cnt, 6);

    // DELETE all 6 -> Deleted(6).
    let n = client.execute("DELETE FROM t WHERE a = 1", &[]).unwrap();
    assert_eq!(n, 6, "DELETE should report 6 affected rows");
    let cnt: i64 = client
        .query_one("SELECT count(*) FROM t", &[])
        .unwrap()
        .get(0);
    assert_eq!(cnt, 0);
}

/// NOT NULL constraint violations via UPDATE and INSERT...SELECT must error
/// and leave the table unchanged (no partial commit).
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_not_null_constraint_enforced() {
    let server = frontend_occ_harness().unsafe_mode().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();
    client
        .batch_execute("CREATE TABLE t (a INT NOT NULL, b INT)")
        .unwrap();
    client.execute("INSERT INTO t VALUES (1, 10)", &[]).unwrap();

    // UPDATE setting NOT NULL column to NULL must fail.
    let err = client
        .execute("UPDATE t SET a = NULL WHERE b = 10", &[])
        .unwrap_err();
    let msg = err
        .as_db_error()
        .expect("expected db error")
        .message()
        .to_lowercase();
    assert!(
        msg.contains("null"),
        "expected null-constraint error, got: {msg}"
    );

    // INSERT...SELECT producing a NULL into a NOT NULL column must fail.
    let err = client
        .execute("INSERT INTO t SELECT NULL::INT, 99", &[])
        .unwrap_err();
    let msg = err
        .as_db_error()
        .expect("expected db error")
        .message()
        .to_lowercase();
    assert!(
        msg.contains("null"),
        "expected null-constraint error, got: {msg}"
    );

    // Table must be unchanged: still exactly (1, 10).
    let rows = client.query("SELECT a, b FROM t", &[]).unwrap();
    assert_eq!(rows.len(), 1, "no rows should have been added/removed");
    let a: i32 = rows[0].get(0);
    let b: i32 = rows[0].get(1);
    assert_eq!(
        (a, b),
        (1, 10),
        "row must be unchanged after failed mutations"
    );
}

/// `RETURNING` must report the inserted rows, in both the constant and the
/// read-dependent `INSERT ... SELECT` shape. Only `INSERT` accepts it, so
/// `UPDATE` and `DELETE` have nothing to check here.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_insert_returning_values() {
    let server = frontend_occ_harness().unsafe_mode().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();
    client
        .batch_execute("CREATE TABLE t (id INT, v INT)")
        .unwrap();
    client
        .batch_execute("INSERT INTO t VALUES (1, 100), (2, 200)")
        .unwrap();

    // Materialize only supports RETURNING on INSERT (the parser rejects it for
    // UPDATE/DELETE), so we exercise INSERT...RETURNING in both the constant
    // and the read-dependent (INSERT...SELECT) shapes.

    // Constant INSERT RETURNING with an expression over the inserted row.
    let rows = client
        .query("INSERT INTO t VALUES (3, 300) RETURNING id, v, v * 2", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 3);
    assert_eq!(rows[0].get::<_, i32>(1), 300);
    assert_eq!(
        rows[0].get::<_, i32>(2),
        600,
        "RETURNING expression mis-evaluated"
    );

    // INSERT...SELECT RETURNING reading existing rows (goes through OCC).
    let mut rows: Vec<(i32, i32)> = client
        .query(
            "INSERT INTO t SELECT id + 100, v + 1 FROM t WHERE id <= 2 RETURNING id, v",
            &[],
        )
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![(101, 101), (102, 201)],
        "INSERT...SELECT RETURNING returned wrong inserted rows"
    );

    // Final state: original (1,100),(2,200),(3,300) plus (101,101),(102,201).
    let mut got: Vec<(i32, i32)> = client
        .query("SELECT id, v FROM t", &[])
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec![(1, 100), (2, 200), (3, 300), (101, 101), (102, 201)]
    );
}

/// UPDATEs that move rows into a range that overlaps existing rows must
/// produce the correct final set (exercises the Let/Negate/map MIR
/// transform with consolidation overlap).
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_update_moves_overlapping_rows() {
    let server = frontend_occ_harness().unsafe_mode().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();
    client.batch_execute("CREATE TABLE t (id INT)").unwrap();
    client
        .execute("INSERT INTO t SELECT generate_series(1, 5)", &[])
        .unwrap();

    // Overlapping shift: {1,2,3,4,5} -> {2,3,4,5,6}.
    client.execute("UPDATE t SET id = id + 1", &[]).unwrap();
    let mut got: Vec<i32> = client
        .query("SELECT id FROM t ORDER BY id", &[])
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec![2, 3, 4, 5, 6],
        "overlapping +1 shift produced wrong set"
    );

    // Non-overlapping shift: {2..6} -> {12..16}.
    client.execute("UPDATE t SET id = id + 10", &[]).unwrap();
    let mut got: Vec<i32> = client
        .query("SELECT id FROM t ORDER BY id", &[])
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    got.sort();
    assert_eq!(got, vec![12, 13, 14, 15, 16]);
}

/// INSERT INTO t SELECT FROM a materialized view must read the MV's content
/// correctly. Exercises the TimestampDependent timeline + linearization
/// defaulting to EpochMilliseconds.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_insert_select_from_materialized_view() {
    let server = frontend_occ_harness().unsafe_mode().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();
    client.batch_execute("CREATE TABLE src (a INT)").unwrap();
    client
        .batch_execute("INSERT INTO src VALUES (1), (2), (3)")
        .unwrap();
    client
        .batch_execute("CREATE MATERIALIZED VIEW mv AS SELECT a * 10 AS a FROM src")
        .unwrap();
    client.batch_execute("CREATE TABLE dst (a INT)").unwrap();

    let n = client
        .execute("INSERT INTO dst SELECT a FROM mv", &[])
        .unwrap();
    assert_eq!(n, 3);
    let mut got: Vec<i32> = client
        .query("SELECT a FROM dst ORDER BY a", &[])
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    got.sort();
    assert_eq!(got, vec![10, 20, 30]);
}

/// Concurrent mixed DML (UPDATE / DELETE / INSERT...SELECT) on one table must
/// conserve exactly the writes it reported, and may only fail with errors a
/// correct implementation is allowed to return.
///
/// Only one of the four statement arms mutates anything, which is what makes an
/// exact oracle available: `v` never goes negative so `WHERE v < 0` matches
/// nothing, no row is ever deleted so the `NOT EXISTS` guard never fires, and
/// `SET v = v` consolidates to no diffs. So `sum(v)` must equal the affected-row
/// counts the `v = v + 1` arm reported, and `count(*)` must not move. A write
/// that committed while reporting an error, reported an affected row without
/// committing, or applied its diffs twice across a retry all break that
/// equality.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_concurrent_mixed_dml_conserves_writes() {
    const NUM_WORKERS: usize = 8;
    const ITERS: usize = 30;
    const NUM_ROWS: usize = 20;
    // The only failure a correct implementation may return here. An internal
    // error, a concurrently modified write target, or an indeterminate write
    // are all bugs, so the error set is checked against this list instead of
    // against a handful of bad-news substrings.
    const ALLOWED_ERRORS: &[&str] =
        &["read-then-write exceeded maximum retry attempts under contention"];

    let server = frontend_occ_harness().unsafe_mode().start_blocking();
    let mut setup = server.connect(postgres::NoTls).unwrap();
    setup
        .batch_execute("CREATE TABLE t (id INT, v INT)")
        .unwrap();
    setup
        .execute(
            &format!("INSERT INTO t SELECT generate_series(1, {NUM_ROWS}), 0"),
            &[],
        )
        .unwrap();

    let reported_increments = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut handles = Vec::new();
    for w in 0..NUM_WORKERS {
        let mut client = server.connect(postgres::NoTls).unwrap();
        let reported_increments = Arc::clone(&reported_increments);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            for i in 0..ITERS {
                let id = (w * 7 + i) % NUM_ROWS + 1;
                let stmt = match i % 4 {
                    0 => format!("UPDATE t SET v = v + 1 WHERE id = {id}"),
                    1 => format!("DELETE FROM t WHERE id = {id} AND v < 0"),
                    2 => format!("INSERT INTO t SELECT {id}, 0 WHERE NOT EXISTS (SELECT 1 FROM t WHERE id = {id})"),
                    _ => format!("UPDATE t SET v = v WHERE id = {id}"),
                };
                match client.execute(stmt.as_str(), &[]) {
                    Ok(affected) if i % 4 == 0 => {
                        reported_increments
                            .fetch_add(usize::try_from(affected).unwrap(), Ordering::SeqCst);
                    }
                    Ok(_) => {}
                    Err(e) => errors
                        .lock()
                        .unwrap()
                        .push(format!("`{stmt}`: {}", server_error_message(&e))),
                }
            }
        }));
    }
    for h in handles {
        h.join().expect("worker panicked");
    }

    let errors = errors.lock().unwrap();
    let unexpected: Vec<&String> = errors
        .iter()
        .filter(|error| !ALLOWED_ERRORS.iter().any(|allowed| error.contains(allowed)))
        .collect();
    assert!(
        unexpected.is_empty(),
        "concurrent mixed DML returned errors outside the allow-list: {unexpected:#?}"
    );

    let sum: i64 = setup
        .query_one("SELECT coalesce(sum(v), 0)::bigint FROM t", &[])
        .unwrap()
        .get(0);
    let expected = i64::try_from(reported_increments.load(Ordering::SeqCst)).unwrap();
    assert_eq!(
        sum, expected,
        "sum(v) must equal the number of rows the incrementing UPDATEs reported"
    );

    let count: i64 = setup
        .query_one("SELECT count(*) FROM t", &[])
        .unwrap()
        .get(0);
    assert_eq!(
        count,
        i64::try_from(NUM_ROWS).unwrap(),
        "no arm of this workload may add or remove a row"
    );
}

/// A read-then-write whose read resolves to a far-future timestamp (here, a
/// `REFRESH AT <far future>` materialized view) parks in
/// `ensure_read_linearized`'s sleep loop. `statement_timeout` has to end that
/// park, which it does because
/// `SessionClient::try_frontend_read_then_write_with_cancel` bounds the
/// *entire* operation, not just the OCC loop.
///
/// NOTE: This holds for the OCC path only. The coordinator path also bounds the
/// scenario, but through the `statement_timeout` it arms around the row stream
/// it reads the selection from, and it blocks while holding the target table's
/// write lock rather than an OCC permit.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_far_future_refresh_mv_respects_statement_timeout() {
    let server = frontend_occ_harness()
        .unsafe_mode()
        .with_system_parameter_default("enable_refresh_every_mvs".to_string(), "true".to_string())
        .start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();
    client.batch_execute("CREATE TABLE src (a INT)").unwrap();
    client
        .batch_execute("INSERT INTO src VALUES (1), (2), (3)")
        .unwrap();
    // Refresh only at a far-future instant: the MV holds no readable content
    // until then, so a freshest-table-write read must pick a far-future as_of.
    client
        .batch_execute(
            "CREATE MATERIALIZED VIEW mv \
             WITH (REFRESH AT '3000-01-01 00:00:00') AS SELECT a FROM src",
        )
        .unwrap();
    client.batch_execute("CREATE TABLE dst (a INT)").unwrap();

    let mut worker = server.connect(postgres::NoTls).unwrap();
    // Grab a cancel token so we can free the parked statement for a clean
    // teardown if it hangs.
    let cancel = worker.cancel_token();
    let (tx, rx) = std::sync::mpsc::channel();
    let handle = thread::spawn(move || {
        worker
            .batch_execute("SET statement_timeout = '3s'")
            .unwrap();
        let started = Instant::now();
        let res = worker.batch_execute("INSERT INTO dst SELECT a FROM mv");
        // `tokio_postgres::Error::to_string()` is just "db error", so preserve
        // the SqlState code and server message for the assertion to inspect.
        let res = res.map_err(|e| {
            (
                e.code().cloned(),
                e.as_db_error().map(|d| d.message().to_string()),
            )
        });
        let _ = tx.send((started.elapsed(), res));
    });

    let outcome = rx.recv_timeout(Duration::from_secs(45));
    if outcome.is_err() {
        // Free the parked statement so the server can shut down cleanly.
        let _ = cancel.cancel_query(postgres::NoTls);
    }
    let _ = handle.join();

    match outcome {
        Ok((elapsed, res)) => {
            // `statement_timeout` bounds the whole operation, so the far-future
            // op must error out rather than silently succeed, and do so well
            // within the 45s recv budget.
            let (code, message) = res.expect_err(
                "far-future RTW should have failed with a statement-timeout error, \
                 but it returned successfully",
            );
            // `StatementTimeout` surfaces as QUERY_CANCELED with the standard
            // "canceling statement due to statement timeout" message.
            assert_eq!(
                code.as_ref(),
                Some(&SqlState::QUERY_CANCELED),
                "far-future RTW failed with unexpected SqlState {code:?} (message: {message:?})"
            );
            assert!(
                message
                    .as_deref()
                    .is_some_and(|m| m.to_lowercase().contains("timeout")),
                "far-future RTW error did not mention a timeout: {message:?}"
            );
            assert!(
                elapsed < Duration::from_secs(15),
                "statement_timeout was 3s but the op took {elapsed:?} to return",
            );
        }
        Err(recv_err) => {
            panic!(
                "INSERT...SELECT from a far-future REFRESH AT MV did not return \
                 within 45s despite statement_timeout = '3s'; the central \
                 statement_timeout enforcement in \
                 try_frontend_read_then_write_with_cancel did not fire ({recv_err})."
            );
        }
    }
}

/// A far-future RTW parked in `ensure_read_linearized` holds its OCC semaphore
/// permit for as long as it is parked. With a bounded permit pool, one such op
/// therefore starves every other read-then-write in the process, including ones
/// on unrelated tables, because a victim blocks on permit acquisition *before*
/// the OCC loop.
///
/// The victim must still honor its own `statement_timeout`, which it does
/// because `try_frontend_read_then_write_with_cancel`'s `select!` bounds the
/// *whole* operation, permit-acquisition wait included.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_far_future_read_then_write_starves_permit_pool() {
    let server = frontend_occ_harness()
        .unsafe_mode()
        .with_system_parameter_default("enable_refresh_every_mvs".to_string(), "true".to_string())
        // One permit: a single hung op exhausts the pool.
        .with_system_parameter_default("max_concurrent_occ_writes".to_string(), "1".to_string())
        .start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();
    client.batch_execute("CREATE TABLE src (a INT)").unwrap();
    client.batch_execute("INSERT INTO src VALUES (1)").unwrap();
    client
        .batch_execute(
            "CREATE MATERIALIZED VIEW mv \
             WITH (REFRESH AT '3000-01-01 00:00:00') AS SELECT a FROM src",
        )
        .unwrap();
    client.batch_execute("CREATE TABLE dst (a INT)").unwrap();
    client
        .batch_execute("CREATE TABLE other (id INT, v INT)")
        .unwrap();
    client
        .batch_execute("INSERT INTO other VALUES (1, 0)")
        .unwrap();

    // Launch the hung far-future RTW. It grabs the single OCC permit and parks
    // in ensure_read_linearized.
    //
    // `mz_query_total` is bumped when the session task takes the statement
    // over, which is the last observable point before it acquires the permit.
    // Polling for that instead of sleeping for a fixed span keeps a loaded
    // machine from starting the victim while the parked op is still connecting
    // or planning, which would fail the test without the permit pool ever being
    // starved.
    let insert_labels = [("session_type", "user"), ("statement_type", "insert")];
    let inserts_before =
        test_util::get_counter_value(server.metrics_registry(), "mz_query_total", &insert_labels);
    let mut hung = server.connect(postgres::NoTls).unwrap();
    let hung_cancel = hung.cancel_token();
    let hung_handle = thread::spawn(move || {
        let _ = hung.batch_execute("INSERT INTO dst SELECT a FROM mv");
    });
    Retry::default()
        .max_duration(Duration::from_secs(60))
        .clamp_backoff(Duration::from_millis(100))
        .retry(|_| {
            let inserts_now = test_util::get_counter_value(
                server.metrics_registry(),
                "mz_query_total",
                &insert_labels,
            );
            if inserts_now > inserts_before {
                Ok(())
            } else {
                Err("far-future INSERT has not started executing")
            }
        })
        .expect("far-future INSERT never started executing");
    // The counter moves a few planning steps before the permit is taken.
    thread::sleep(Duration::from_secs(1));

    // A completely unrelated UPDATE, with a short statement_timeout, should be
    // able to make progress. Run it on a worker thread with a wall-clock guard.
    let mut victim = server.connect(postgres::NoTls).unwrap();
    let victim_cancel = victim.cancel_token();
    let (tx, rx) = std::sync::mpsc::channel();
    let victim_handle = thread::spawn(move || {
        victim
            .batch_execute("SET statement_timeout = '3s'")
            .unwrap();
        let started = Instant::now();
        let res = victim.execute("UPDATE other SET v = v + 1 WHERE id = 1", &[]);
        // Preserve the SqlState code and server message (`to_string()` is just
        // "db error").
        let res = res.map_err(|e| {
            (
                e.code().cloned(),
                e.as_db_error().map(|d| d.message().to_string()),
            )
        });
        let _ = tx.send((started.elapsed(), res));
    });

    let outcome = rx.recv_timeout(Duration::from_secs(25));
    // Free both parked statements for a clean teardown.
    let _ = victim_cancel.cancel_query(postgres::NoTls);
    let _ = hung_cancel.cancel_query(postgres::NoTls);
    let _ = victim_handle.join();
    let _ = hung_handle.join();

    match outcome {
        Ok((elapsed, res)) => {
            // The far-future op holds the sole permit for its (default 60s)
            // lifetime, so the victim cannot acquire a permit. Its own
            // `statement_timeout = '3s'` bounds the permit-acquisition wait, so
            // it returns a timeout error rather than hanging.
            let (code, message) = res.expect_err(
                "victim UPDATE should have timed out waiting on the starved permit pool, \
                 but it returned successfully",
            );
            assert_eq!(
                code.as_ref(),
                Some(&SqlState::QUERY_CANCELED),
                "victim UPDATE failed with unexpected SqlState {code:?} (message: {message:?})"
            );
            assert!(
                message
                    .as_deref()
                    .is_some_and(|m| m.to_lowercase().contains("timeout")),
                "victim UPDATE error did not mention a timeout: {message:?}"
            );
            // It should time out on its own 3s budget, well within the 25s
            // recv guard.
            assert!(
                elapsed < Duration::from_secs(15),
                "victim statement_timeout was 3s but it took {elapsed:?} to return",
            );
        }
        Err(recv_err) => {
            panic!(
                "an unrelated UPDATE on a different table did not return within 25s \
                 (statement_timeout = '3s'). A single far-future read-then-write holds the \
                 sole OCC permit while parked in ensure_read_linearized. Because \
                 statement_timeout bounds the permit wait, the victim should time out on \
                 permit acquisition within ~3s instead of hanging ({recv_err})."
            );
        }
    }
}

/// A cancelled or timed-out read-then-write must give back its OCC permit.
///
/// The permit pool is sized to one here, so a permit that leaks wedges every
/// later read-then-write in the process, and the follow-up UPDATE is the oracle:
/// it needs the permit the abandoned statement held, and its own
/// `statement_timeout` turns a wedge into a failure instead of a hang. The cycle
/// repeats so that leaking one permit per iteration is fatal rather than
/// tolerable.
///
/// The abandoned statement parks in `ensure_read_linearized`, waiting on a
/// far-future REFRESH materialized view. That holds a permit while occupying no
/// cluster worker, which the oracle depends on: a statement that blocks by
/// sleeping inside its dataflow keeps the worker busy after it is cancelled, so
/// the follow-up would be measuring cluster occupancy rather than permit
/// availability.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_cancel_and_timeout_release_permit() {
    const ITERATIONS: i32 = 5;
    // Bounds the follow-up write, so a leaked permit surfaces as this error
    // rather than as a hang.
    const FOLLOW_UP_TIMEOUT: &str = "30s";

    let server = frontend_occ_harness()
        .unsafe_mode()
        .with_system_parameter_default("enable_refresh_every_mvs".to_string(), "true".to_string())
        // One permit: a single leak starves every read-then-write.
        .with_system_parameter_default("max_concurrent_occ_writes".to_string(), "1".to_string())
        .start_blocking();

    let mut client = server.connect(postgres::NoTls).unwrap();
    client.batch_execute("CREATE TABLE src (a INT)").unwrap();
    client.batch_execute("INSERT INTO src VALUES (1)").unwrap();
    client
        .batch_execute(
            "CREATE MATERIALIZED VIEW mv \
             WITH (REFRESH AT '3000-01-01 00:00:00') AS SELECT a FROM src",
        )
        .unwrap();
    client.batch_execute("CREATE TABLE dst (a INT)").unwrap();
    client.batch_execute("CREATE TABLE t (n INT)").unwrap();
    client.batch_execute("INSERT INTO t VALUES (0)").unwrap();
    client
        .batch_execute(&format!("SET statement_timeout = '{FOLLOW_UP_TIMEOUT}'"))
        .unwrap();

    // The read cannot be linearized until the year 3000, so the statement parks
    // holding the sole permit.
    let parking_insert = "INSERT INTO dst SELECT a FROM mv";

    for iteration in 1..=ITERATIONS {
        let mut parked = server.connect(postgres::NoTls).unwrap();
        let cancel_token = parked.cancel_token();
        let baseline = user_insert_count(&server);
        let parked_handle = thread::spawn(move || parked.batch_execute(parking_insert));
        wait_until_parked_holding_a_permit(&server, baseline, iteration, "the cancel half");
        cancel_token.cancel_query(postgres::NoTls).unwrap();
        let err = parked_handle
            .join()
            .unwrap()
            .expect_err("the parked INSERT should have been cancelled");
        let message = server_error_message(&err);
        assert_eq!(
            err.code(),
            Some(&SqlState::QUERY_CANCELED),
            "iteration {iteration}: cancelled INSERT reported {message}"
        );
        // A statement timeout reports this same SQLSTATE, so without the message
        // check a cancel that never arrived would look like a pass: the victim
        // would sit on the default 60s timeout and fail with QUERY_CANCELED too.
        assert!(
            message.contains("user request"),
            "iteration {iteration}: expected a cancellation, got {message}"
        );

        follow_up_write_gets_a_permit(&mut client, iteration, "a cancelled");

        // Long enough that the deadline cannot fire before the permit is taken,
        // short enough to keep the test quick.
        let mut parked = server.connect(postgres::NoTls).unwrap();
        parked
            .batch_execute("SET statement_timeout = '5s'")
            .unwrap();
        let baseline = user_insert_count(&server);
        let parked_handle = thread::spawn(move || parked.batch_execute(parking_insert));
        wait_until_parked_holding_a_permit(&server, baseline, iteration, "the timeout half");
        let err = parked_handle
            .join()
            .unwrap()
            .expect_err("the parked INSERT should have timed out");
        let message = server_error_message(&err);
        assert_eq!(
            err.code(),
            Some(&SqlState::QUERY_CANCELED),
            "iteration {iteration}: timed-out INSERT reported {message}"
        );
        assert!(
            message.contains("statement timeout"),
            "iteration {iteration}: expected a statement-timeout error, got {message}"
        );

        follow_up_write_gets_a_permit(&mut client, iteration, "a timed-out");
    }

    // Two follow-up writes per iteration, each of them a single-row UPDATE.
    let n: i32 = client.query_one("SELECT n FROM t", &[]).unwrap().get(0);
    assert_eq!(n, 2 * ITERATIONS, "a follow-up write did not commit");
    let count: i64 = client
        .query_one("SELECT count(*) FROM dst", &[])
        .unwrap()
        .get(0);
    assert_eq!(
        count, 0,
        "a cancelled or timed-out INSERT committed its rows"
    );
}

/// Counts user INSERTs the process has executed, the observable that
/// [`wait_until_parked_holding_a_permit`] watches.
fn user_insert_count(server: &test_util::TestServerWithRuntime) -> u64 {
    const LABELS: [(&str, &str); 2] = [("session_type", "user"), ("statement_type", "insert")];
    test_util::get_counter_value(server.metrics_registry(), "mz_query_total", &LABELS)
}

/// Waits until the INSERT under test holds an OCC permit.
///
/// Without this, a loaded machine can let the cancel or the deadline land while
/// the statement is still connecting or planning. That exercises an exit which
/// never held a permit, so it proves nothing about releasing one, and it passes
/// anyway.
///
/// There is no metric for permit acquisition, so this polls the closest
/// observable, the `mz_query_total` bump in `ExecutionLogging::take_over`, and
/// then settles. The bump happens a few steps before `acquire_owned`, hence the
/// settle.
///
/// `baseline` must be read with [`user_insert_count`] before the statement is
/// started. Every user INSERT in the process shares that counter, so a baseline
/// read afterwards can already include the statement we are waiting for, and
/// then no bump ever arrives.
fn wait_until_parked_holding_a_permit(
    server: &test_util::TestServerWithRuntime,
    baseline: u64,
    iteration: i32,
    half: &str,
) {
    const SETTLE: Duration = Duration::from_secs(1);

    Retry::default()
        .max_duration(Duration::from_secs(60))
        .clamp_backoff(Duration::from_millis(50))
        .retry(|_| {
            let inserts = user_insert_count(server);
            if inserts > baseline {
                Ok(())
            } else {
                Err(inserts)
            }
        })
        .unwrap_or_else(|inserts| {
            panic!(
                "iteration {iteration}, {half}: the parked INSERT never reached its session task, \
                 mz_query_total stuck at {inserts}"
            )
        });
    thread::sleep(SETTLE);
}

/// For `test_cancel_and_timeout_release_permit`: runs a read-then-write
/// that can only make progress if the abandoned statement released its permit.
fn follow_up_write_gets_a_permit(client: &mut postgres::Client, iteration: i32, predecessor: &str) {
    let affected = client
        .execute("UPDATE t SET n = n + 1", &[])
        .unwrap_or_else(|err| {
            panic!(
                "iteration {iteration}: the read-then-write following {predecessor} one failed \
                 with `{}`, which is what a leaked OCC permit looks like",
                server_error_message(&err)
            )
        });
    assert_eq!(
        affected, 1,
        "iteration {iteration}: follow-up UPDATE after {predecessor} one affected {affected} rows"
    );
}

/// Waits until the cluster runs no dataflows other than the ones introspection
/// installs for itself. An OCC read-then-write's internal subscribe is a
/// dataflow, so this catches a `SubscribeHandle` whose drop never tore it down.
fn wait_for_no_dataflows(client: &mut postgres::Client, context: &str) {
    // Storage operators have their IDs offset by STORAGE_ID_OFFSET (1 << 48),
    // so they are excluded by id.
    const DATAFLOW_QUERY: &str = "SELECT count(*) \
        FROM mz_introspection.mz_dataflows \
        WHERE name NOT LIKE '%introspection-subscribe%' \
        AND id < 281474976710656";

    Retry::default()
        .max_duration(Duration::from_secs(60))
        .clamp_backoff(Duration::from_millis(500))
        .retry(|_| {
            let count: i64 = client.query_one(DATAFLOW_QUERY, &[]).unwrap().get(0);
            if count == 0 { Ok(()) } else { Err(count) }
        })
        .unwrap_or_else(|count| panic!("{count} dataflows still installed {context}"));
}

/// A read-then-write computed against one generation of its write target must
/// not commit once a concurrent `ALTER TABLE ... ADD COLUMN` has given the
/// target a new one. Either the write wins the race and commits in full, or the
/// group committer rejects it as a concurrent dependency mutation, which is a
/// retryable serialization failure. Both outcomes leave the table consistent
/// and neither may panic the coordinator.
///
/// Which of the two happens depends on where the ALTER lands relative to the
/// generation the write captured, so the test accepts either. The sleep in the
/// selection makes the write slow enough that the ALTER usually lands inside
/// the window.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_write_racing_alter_table_add_column() {
    const SLEEP_SECS: i32 = 3;
    const ROUNDS: usize = 3;

    let server = frontend_occ_harness()
        .unsafe_mode()
        .with_system_parameter_default(
            "unsafe_enable_unsafe_functions".to_string(),
            "true".to_string(),
        )
        .with_system_parameter_default(
            "enable_alter_table_add_column".to_string(),
            "true".to_string(),
        )
        .start_blocking();

    let mut client = server.connect(postgres::NoTls).unwrap();
    client
        .batch_execute("CREATE TABLE t (n INT, ts INT)")
        .unwrap();
    client
        .batch_execute(&format!("INSERT INTO t VALUES (0, {SLEEP_SECS})"))
        .unwrap();

    let update_labels = [("session_type", "user"), ("statement_type", "update")];
    let mut committed = 0;
    for round in 0..ROUNDS {
        let updates_before = test_util::get_counter_value(
            server.metrics_registry(),
            "mz_query_total",
            &update_labels,
        );
        let mut writer = server.connect(postgres::NoTls).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = thread::spawn(move || {
            // The CASE always takes its ELSE branch (`mz_sleep` returns NULL),
            // so every row matches and the sleep runs in the subscribe
            // dataflow, which is what keeps the write in flight.
            let result = writer.execute(
                "UPDATE t SET n = n + 1 \
                 WHERE ts >= CASE WHEN mz_unsafe.mz_sleep(ts) > 0 THEN 1 ELSE 0 END",
                &[],
            );
            // Preserve the SqlState and server message: `to_string()` on a
            // server error is only "db error".
            let result = result.map_err(|err| {
                (
                    err.code().cloned(),
                    err.as_db_error().map(|db| db.message().to_string()),
                )
            });
            let _ = tx.send(result);
        });

        // Land the ALTER after the UPDATE started executing, so it has a chance
        // to invalidate the generation the UPDATE is writing against.
        Retry::default()
            .max_duration(Duration::from_secs(60))
            .clamp_backoff(Duration::from_millis(100))
            .retry(|_| {
                let updates_now = test_util::get_counter_value(
                    server.metrics_registry(),
                    "mz_query_total",
                    &update_labels,
                );
                if updates_now > updates_before {
                    Ok(())
                } else {
                    Err("racing UPDATE has not started executing")
                }
            })
            .expect("racing UPDATE never started executing");
        client
            .batch_execute(&format!("ALTER TABLE t ADD COLUMN c{round} INT"))
            .unwrap();

        let outcome = rx
            .recv_timeout(Duration::from_secs(120))
            .expect("UPDATE racing ALTER TABLE never returned");
        handle.join().expect("writer thread panicked");
        match outcome {
            Ok(affected) => {
                assert_eq!(
                    affected, 1,
                    "round {round}: the UPDATE committed but reported {affected} rows"
                );
                committed += 1;
            }
            Err((code, message)) => {
                assert_eq!(
                    code.as_ref(),
                    Some(&SqlState::T_R_SERIALIZATION_FAILURE),
                    "round {round}: the UPDATE failed with SqlState {code:?} \
                     (message: {message:?}) rather than as a retryable conflict"
                );
                assert!(
                    message
                        .as_deref()
                        .is_some_and(|m| m.contains("was concurrently modified")),
                    "round {round}: unexpected serialization-failure message {message:?}"
                );
            }
        }

        // Whichever way the race went, the table holds one row whose counter
        // matches the number of UPDATEs that committed.
        let rows = client.query("SELECT n FROM t", &[]).unwrap();
        assert_eq!(rows.len(), 1, "round {round}: unexpected row count");
        assert_eq!(
            rows[0].get::<_, i32>(0),
            committed,
            "round {round}: a rejected UPDATE left its write behind, or a \
             committed one applied twice"
        );
    }
}

// A read-then-write the frontend OCC path committed must be visible to a
// strict serializable read that starts afterwards, on any session. The write
// commits at a timestamp it chose itself, so if it reported success before that
// timestamp was reflected in the timeline's oracle, a later linearized read
// could pick an earlier timestamp and miss the write.
//
// The reader connects fresh every iteration, so it cannot inherit the writer
// session's timestamp bookkeeping: the only thing that can carry the write
// forward is the global oracle. Iterating is what gives the race a chance to
// show up. A single pass could pass by luck.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_frontend_occ_write_visible_to_linearizable_read() {
    const ITERATIONS: i32 = 50;

    let server = frontend_occ_harness().start_blocking();

    let mut writer = server.connect(postgres::NoTls).unwrap();
    writer
        .batch_execute("CREATE TABLE t (id INT, v INT)")
        .unwrap();
    writer.batch_execute("INSERT INTO t VALUES (1, 0)").unwrap();

    for iteration in 1..=ITERATIONS {
        let affected = writer
            .execute("UPDATE t SET v = v + 1 WHERE id = 1", &[])
            .unwrap();
        assert_eq!(affected, 1, "iteration {iteration}: UPDATE lost its row");

        let mut reader = server.connect(postgres::NoTls).unwrap();
        reader
            .batch_execute("SET transaction_isolation = 'strict serializable'")
            .unwrap();
        let v: i32 = reader
            .query_one("SELECT v FROM t WHERE id = 1", &[])
            .unwrap()
            .get(0);
        assert_eq!(
            v, iteration,
            "a strict serializable read on a fresh session did not observe the \
             committed UPDATE"
        );
    }
}

// Test that the server properly handles cancellation requests of read-then-write queries.
// See database-issues#6134.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `epoll_wait` on OS `linux`
#[allow(clippy::disallowed_methods)]
fn test_cancel_read_then_write() {
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();
    server.enable_feature_flags(&["unsafe_enable_unsafe_functions"]);

    let mut client = server.connect(postgres::NoTls).unwrap();
    client
        .batch_execute("CREATE TABLE foo (a TEXT, ts INT)")
        .unwrap();

    // Lots of races here, so try this whole thing in a loop.
    Retry::default()
        .clamp_backoff(Duration::ZERO)
        .retry(|_state| {
            let mut client1 = server.connect(postgres::NoTls).unwrap();
            let mut client2 = server.connect(postgres::NoTls).unwrap();
            let cancel_token = client2.cancel_token();

            client1.batch_execute("DELETE FROM foo").unwrap();
            client1.batch_execute("SET statement_timeout = '5s'").unwrap();
            client1
                .batch_execute("INSERT INTO foo VALUES ('hello', 10)")
                .unwrap();

            let handle1 = thread::spawn(move || {
                let err =  client1
                    .batch_execute("insert into foo select a, case when mz_unsafe.mz_sleep(ts) > 0 then 0 end as ts from foo")
                    .unwrap_err();
                assert_contains!(
                    err.to_string_with_causes(),
                    "statement timeout"
                );
                client1
            });
            std::thread::sleep(Duration::from_millis(100));
            let handle2 = thread::spawn(move || {
                let err = client2
                .batch_execute("insert into foo values ('blah', 1);")
                .unwrap_err();
                assert_contains!(
                    err.to_string_with_causes(),
                    "canceling statement"
                );
            });
            std::thread::sleep(Duration::from_millis(100));
            cancel_token.cancel_query(postgres::NoTls)?;
            let mut client1 = handle1.join().unwrap();
            handle2.join().unwrap();
            let rows:i64 = client1.query_one ("SELECT count(*) FROM foo", &[]).unwrap().get(0);
            // We ran 3 inserts. First succeeded. Second timedout. Third cancelled.
            if rows !=1 {
                anyhow::bail!("unexpected row count: {rows}");
            }
            Ok::<_, anyhow::Error>(())
        })
        .unwrap();
}

/// An INSERT whose values read no persisted state may run in a transaction,
/// even when the values are too large to fold into a literal. The rows are
/// buffered as session write ops, so they commit with the transaction and
/// disappear if it does not commit.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_nonconstant_insert_in_transaction() {
    // Above `FOLD_CONSTANTS_LIMIT`, so the planned values stay a dataflow
    // instead of folding into a constant.
    const BIG_INSERT: &str = "INSERT INTO t SELECT generate_series(1, 20000)";

    let server = frontend_occ_harness().unsafe_mode().start_blocking();

    let mut client = server.connect(postgres::NoTls).unwrap();
    client.batch_execute("CREATE TABLE t (a int)").unwrap();

    let count = |client: &mut postgres::Client| {
        client
            .query_one("SELECT count(*)::int4 FROM t", &[])
            .unwrap()
            .get::<_, i32>(0)
    };

    client
        .batch_execute(&format!("BEGIN; {BIG_INSERT}; COMMIT;"))
        .unwrap();
    assert_eq!(count(&mut client), 20000);

    client
        .batch_execute(&format!("BEGIN; {BIG_INSERT}; ROLLBACK;"))
        .unwrap();
    assert_eq!(count(&mut client), 20000, "rolled back write must not land");

    // A later error in the same implicit batch aborts the transaction, so the
    // write must not be visible.
    let err = client
        .batch_execute(&format!("{BIG_INSERT}; SELECT 1/0;"))
        .unwrap_err();
    let db_err = err.as_db_error().expect("expected db error");
    assert!(
        db_err.message().contains("division by zero"),
        "unexpected error: {err:?}"
    );
    let _ = client.batch_execute("ROLLBACK");
    assert_eq!(count(&mut client), 20000, "aborted write must not land");

    // Outside a transaction the same statement commits on its own.
    client.batch_execute(BIG_INSERT).unwrap();
    assert_eq!(count(&mut client), 40000);

    // The command tag counts diffs, not distinct rows: 20000 copies of the
    // same row consolidate into one row with diff 20000.
    client.batch_execute("BEGIN").unwrap();
    let inserted = client
        .execute("INSERT INTO t SELECT 1 FROM generate_series(1, 20000)", &[])
        .unwrap();
    assert_eq!(inserted, 20000);
    client.batch_execute("COMMIT").unwrap();
    assert_eq!(count(&mut client), 60000);

    // Deferred and constant writes mix freely in one transaction and all land
    // at COMMIT.
    client.batch_execute("BEGIN").unwrap();
    assert_eq!(client.execute(BIG_INSERT, &[]).unwrap(), 20000);
    assert_eq!(client.execute(BIG_INSERT, &[]).unwrap(), 20000);
    assert_eq!(client.execute("INSERT INTO t VALUES (1)", &[]).unwrap(), 1);
    client.batch_execute("COMMIT").unwrap();
    assert_eq!(count(&mut client), 100001);

    // Constraint violations are caught while the diffs are being collected, so
    // the statement fails and the transaction buffers nothing.
    client
        .batch_execute("CREATE TABLE nn (a int NOT NULL)")
        .unwrap();
    client.batch_execute("BEGIN").unwrap();
    let err = client
        .execute(
            "INSERT INTO nn SELECT CASE WHEN g = 5 THEN NULL ELSE g END \
             FROM generate_series(1, 20000) g",
            &[],
        )
        .unwrap_err();
    let db_err = err.as_db_error().expect("expected db error");
    assert!(
        db_err.message().contains("violates not-null constraint"),
        "unexpected error: {err:?}"
    );
    let _ = client.batch_execute("ROLLBACK");
    let nn_count = client
        .query_one("SELECT count(*)::int4 FROM nn", &[])
        .unwrap()
        .get::<_, i32>(0);
    assert_eq!(nn_count, 0, "failed write must not land");

    // RETURNING needs the rows to be visible now, so it cannot wait for
    // COMMIT. The transaction gate rejects it whether or not the values fold.
    client.batch_execute("BEGIN").unwrap();
    let err = client
        .query(&format!("{BIG_INSERT} RETURNING a"), &[])
        .unwrap_err();
    let db_err = err.as_db_error().expect("expected db error");
    assert!(
        db_err
            .message()
            .contains("cannot be run inside a transaction block"),
        "unexpected error: {err:?}"
    );
    let _ = client.batch_execute("ROLLBACK");
}

/// A transaction that has committed itself to DDL or to a subscribe cannot
/// take a write. The reported error must be the one the coordinator reports,
/// not whatever the write-op merge in the session state machine happens to
/// produce.
#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn test_rejected_in_non_writable_transaction() {
    const BIG_INSERT: &str = "INSERT INTO t SELECT generate_series(1, 20000)";

    let server = frontend_occ_harness().unsafe_mode().start_blocking();

    let mut client = server.connect(postgres::NoTls).unwrap();
    client.batch_execute("CREATE TABLE t (a int)").unwrap();
    client.batch_execute("CREATE TABLE z (a int)").unwrap();
    client.batch_execute("CREATE TABLE u (a int)").unwrap();
    client.batch_execute("INSERT INTO u VALUES (1)").unwrap();

    let assert_read_only = |client: &mut postgres::Client, setup: &[&str]| {
        client.batch_execute("BEGIN").unwrap();
        for stmt in setup {
            client.batch_execute(stmt).unwrap();
        }
        let err = client.execute(BIG_INSERT, &[]).unwrap_err();
        let db_err = err.as_db_error().expect("expected db error");
        assert!(
            db_err.message().contains("transaction in read-only mode"),
            "after {setup:?}, unexpected error: {err:?}"
        );
        // The failed statement aborts the transaction, so the rollback is what
        // hands the session back in a usable state for the next case.
        let _ = client.batch_execute("ROLLBACK");
    };

    // Each case rolls back, so the catalog changes never apply and the next
    // case starts from the same state.
    assert_read_only(&mut client, &["ALTER TABLE z RENAME TO z2"]);
    assert_read_only(&mut client, &["CREATE TABLE zz (i int)"]);
    // The FETCH is what pins the transaction to the subscribe, the DECLARE
    // alone leaves it undecided.
    assert_read_only(
        &mut client,
        &["DECLARE c CURSOR FOR SUBSCRIBE u", "FETCH 1 c"],
    );

    let count = client
        .query_one("SELECT count(*)::int4 FROM t", &[])
        .unwrap()
        .get::<_, i32>(0);
    assert_eq!(count, 0, "no rejected write may have landed");
}
