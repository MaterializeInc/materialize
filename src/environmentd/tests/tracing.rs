// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_environmentd::test_util;
use tracing_fluent_assertions::AssertionRegistry;

// Test that expected spans are generated for various queries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)] // allow(test-attribute)
#[cfg_attr(miri, ignore)] // too slow
async fn test_expected_spans() {
    let registry = AssertionRegistry::default();
    // Sets of expected span names and SQL statements that should produce that span.
    let tests = vec![
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
        ("peek_stage_finish", "SELECT 1"),
        ("peek_stage_explain_plan", "EXPLAIN SELECT 1"),
    ];
    let asserts = tests
        .iter()
        .map(|(name, _)| {
            (
                *name,
                registry
                    .build()
                    .with_name(*name)
                    .was_closed_exactly(1)
                    .finalize(),
            )
        })
        .collect::<Vec<_>>();

    let server = test_util::TestHarness::default()
        .with_enable_tracing(true)
        .with_fluent_assertions(registry)
        .with_system_parameter_default("opentelemetry_filter".to_string(), "info".to_string())
        .start()
        .await;
    let client = server.connect().await.unwrap();

    for (_, sql) in tests {
        client.batch_execute(sql).await.unwrap();
    }
    for (name, assert) in asserts {
        if !assert.try_assert() {
            // Try one more time and print the name. Use .assert here because it prints a deeper
            // cause than try_assert.
            println!("assert failed for {name}");
            assert.assert();
        }
    }
}
