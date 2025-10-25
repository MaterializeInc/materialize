// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_environmentd::test_util;
use mz_ore::collections::CollectionExt;
use tracing_capture::SharedStorage;

// Test that expected spans are generated for various queries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)] // allow(test-attribute)
#[cfg_attr(miri, ignore)] // too slow
async fn test_expected_spans() {
    let storage = SharedStorage::default();
    // Sets of expected span names and SQL statements that should produce that span.
    let tests = &[
        // TODO: Tests that call create_collections have a race condition.
        // ("sequence_create_table", "CREATE TABLE t (i INT)"),
        // ("sequence_insert", "INSERT INTO t VALUES (1)"),
        // (
        //     "create_materialized_view_finish",
        //     "CREATE MATERIALIZED VIEW MV AS SELECT 1",
        // ),
        ("create_view_finish", "CREATE VIEW V AS SELECT 1"),
        ("create_index_finish", "CREATE DEFAULT INDEX i ON v"),
        ("subscribe_finish", "SUBSCRIBE (SELECT 1)"),
        ("peek_finish", "SELECT 1"),
        ("peek_explain_plan", "EXPLAIN SELECT 1"),
    ];

    let server = test_util::TestHarness::default()
        .with_enable_tracing(true)
        .with_capture(storage.clone())
        .with_system_parameter_default("opentelemetry_filter".to_string(), "info".to_string())
        .start()
        .await;

    // This test checks for specific functions of the old peek sequencing, so we disable the new
    // peek sequencing for now.
    // TODO(peek-seq): Modify the test to check for the new peek sequencing instead of the old one.
    server
        .disable_feature_flags(&["enable_frontend_peek_sequencing"])
        .await;

    let client = server.connect().await.unwrap();

    // Assert that there are no expected spans.
    {
        let storage = storage.lock();
        for (name, _) in tests {
            let stats = storage
                .all_spans()
                .filter_map(|span| (span.metadata().name() == *name).then(|| span.stats()))
                .collect::<Vec<_>>();
            assert!(stats.is_empty(), "{stats:?}");
        }
    }

    for (_, sql) in tests {
        client.batch_execute(sql).await.unwrap();
    }

    {
        let storage = storage.lock();
        for (name, _) in tests {
            let stats = storage
                .all_spans()
                .filter_map(|span| (span.metadata().name() == *name).then(|| span.stats()))
                .collect::<Vec<_>>();
            let stat = stats.into_element();
            // TODO: entered and exited can sometimes be > 1 (and so we can't assert == 1). Why does
            // this happen? It's not bootstrapping, which we know because of the empty span assert
            // above.
            assert!(stat.entered > 0, "{name}: {stat:?}");
            assert!(stat.exited > 0, "{name}: {stat:?}");
            assert_eq!(stat.is_closed, true, "{name}: {stat:?}");
        }
    }
}

// Test that secrets are not leaked in tracing. Send many kinds of queries that should execute many
// of the most common code paths. This is not guaranteed to exhaustively test all of our tracing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)] // allow(test-attribute)
#[cfg_attr(miri, ignore)] // too slow
async fn test_secrets() {
    let storage = SharedStorage::default();

    let server = test_util::TestHarness::default()
        .with_enable_tracing(true)
        .with_capture(storage.clone())
        .with_system_parameter_default("opentelemetry_filter".to_string(), "trace".to_string())
        .start()
        .await;
    let client = server.connect().await.unwrap();

    const SENTINEL: &str = "MUST_NOT_APPEAR";

    // pgwire simple.
    client
        .batch_execute(&format!("CREATE SECRET s1 AS '{SENTINEL}'"))
        .await
        .unwrap();
    // pgwire extended.
    client
        .execute(&format!("CREATE SECRET s2 AS '{SENTINEL}'"), &[])
        .await
        .unwrap();
    // Lex error.
    client
        .batch_execute(&format!("CREATE SECRET _er AS '{SENTINEL}"))
        .await
        .unwrap_err();
    // Parse error.
    client
        .batch_execute(&format!("CREATE SECRET _er AS 1 '{SENTINEL}'"))
        .await
        .unwrap_err();
    // Plan error.
    client
        .batch_execute(&format!("CREATE SECRET _er AS 1 + '{SENTINEL}'"))
        .await
        .unwrap_err();
    client
        .batch_execute(&format!("ALTER SECRET s1 AS 'new_{SENTINEL}'"))
        .await
        .unwrap();

    // TODO: Query via HTTP.

    // TODO: Check event values.

    // TODO: Search through span fields. Span metadata contains the names of fields, but I've been
    // unable to find the values of those fields.

    let storage = storage.lock();
    for ev in storage.all_events() {
        let assert_no_sentinel = |msg: Option<&str>| {
            if let Some(msg) = msg {
                if !msg.contains(SENTINEL) {
                    return;
                }
                let meta = ev.metadata();
                if let Some(mp) = meta.module_path() {
                    if !mp.starts_with("mz_") {
                        // TODO: Remove this.
                        return;
                    }
                } else {
                    return;
                }
                assert!(
                    !msg.contains(SENTINEL),
                    "event contains sentinel: {msg} at {}",
                    ev.metadata().name()
                );
            }
        };

        assert_no_sentinel(ev.message());
        for (_, v) in ev.values() {
            assert_no_sentinel(v.as_str());
        }
    }
    for span in storage.all_spans() {
        for (name, v) in span.values() {
            let Some(msg) = v.as_str() else {
                continue;
            };
            if !msg.contains(SENTINEL) {
                continue;
            }
            assert!(
                !msg.contains(SENTINEL),
                "span value {name} contains sentinel: {msg} at {}",
                span.metadata().name()
            );
        }
    }
}
