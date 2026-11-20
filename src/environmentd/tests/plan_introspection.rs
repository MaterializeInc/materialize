// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Execute the selected-plan views against deterministic input rows. These tests
//! cover the SQL contract, not publication by real producers or adapter convergence.

#![recursion_limit = "256"]

use mz_catalog::builtin::{MZ_MAINTAINED_COMPUTE_DEPENDENCIES, MZ_MATERIALIZED_VIEW_REFRESHES};
use mz_catalog::expr_cache::PROTOTYPE_SHARED_PLAN_NAMESPACE;
use mz_environmentd::test_util::TestHarness;
use mz_postgres_util::{Sql, batch_execute, query, sql};
use serde_json::{Value, json};
use tokio_postgres::Client;

async fn fixture(client: &Client) {
    for statement in [
        sql!("CREATE TABLE plans (data jsonb)"),
        sql!("CREATE TABLE placement (object_id text, replica_owned bool, compute_global bool)"),
        sql!(
            "CREATE TABLE frontiers (object_id text, write_known bool, write_frontier mz_timestamp)"
        ),
    ] {
        batch_execute(client, statement).await.unwrap();
    }
    for (name, production_sql) in [
        ("refreshes", MZ_MATERIALIZED_VIEW_REFRESHES.sql),
        ("dependencies", MZ_MAINTAINED_COMPUTE_DEPENDENCIES.sql),
    ] {
        // Keep all production expressions, including parse_catalog_id and filters.
        let bound = production_sql
            .replace("mz_internal.mz_catalog_shared", "public.plans")
            .replace(
                "mz_internal.mz_frontier_object_placement",
                "public.placement",
            )
            .replace(
                "mz_internal.mz_current_storage_frontiers",
                "public.frontiers",
            );
        batch_execute(
            client,
            sql!(
                "CREATE VIEW {} AS {}",
                Sql::ident(name),
                Sql::raw_unchecked(bound)
            ),
        )
        .await
        .unwrap();
    }
}

fn plan(id: u64, physical_imports: &[Value], schedule: Value, initial: Option<u64>) -> Value {
    json!({
        "kind": "WrittenPlan",
        "key": {"id": {"User": id}, "build_version": PROTOTYPE_SHARED_PLAN_NAMESPACE},
        "value": {
            "imports": [{"User": 900}],
            "introspection": {
                "physical_imports": physical_imports,
                "refresh_schedule": schedule,
                "initial_storage_as_of": initial
            }
        }
    })
}

async fn insert_plan(client: &Client, plan: Value) {
    batch_execute(
        client,
        sql!(
            "INSERT INTO plans VALUES ({}::jsonb)",
            Sql::literal(&plan.to_string())
        ),
    )
    .await
    .unwrap();
}

async fn assert_rows(
    client: &Client,
    statement: Sql,
    expected: &[Vec<Option<String>>],
    context: &str,
) {
    let actual: Vec<Vec<Option<String>>> = query(client, statement, &[])
        .await
        .unwrap()
        .iter()
        .map(|row| (0..row.len()).map(|i| row.get(i)).collect())
        .collect();
    assert_eq!(actual, expected, "{context}");
}

fn refresh_query() -> Sql {
    sql!(
        "SELECT materialized_view_id, last_completed_refresh::text, next_refresh::text
          FROM refreshes ORDER BY materialized_view_id"
    )
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn selected_plan_refreshes() {
    // These user views bind builtin SQL, including its internal ID decoder.
    let server = TestHarness::default()
        .unsafe_mode()
        .with_system_parameter_default("unsafe_enable_unstable_dependencies".into(), "true".into())
        .start()
        .await;
    let client = server.connect().await.unwrap();
    fixture(&client).await;
    // No replica inventory or per-replica observations are supplied, including
    // for live objects on zero-replica clusters.
    batch_execute(
        &client,
        sql!("INSERT INTO placement VALUES ('u1', true, false)"),
    )
    .await
    .unwrap();
    let ats = json!({"ats": [5, 20, 40], "everies": []});
    let every = json!({"ats": [], "everies": [
        {"interval": {"secs": 0, "nanos": 10000000}, "aligned_to": 3}
    ]});
    let mixed = json!({"ats": [5, 22, 40], "everies": [
        {"interval": {"secs": 0, "nanos": 10000000}, "aligned_to": 3},
        {"interval": {"secs": 0, "nanos": 20000000}, "aligned_to": 7}
    ]});
    let fractional = json!({"ats": [], "everies": [
        {"interval": {"secs": 1, "nanos": 1500000}, "aligned_to": 3}
    ]});
    // Frontier: None = absent, Some((false, _)) = unknown,
    // Some((true, None)) = known complete. Expected NULLs are not absent rows.
    for (name, schedule, initial, frontier, expected) in [
        ("missing", ats.clone(), Some(20), None, None),
        (
            "unknown null",
            ats.clone(),
            Some(20),
            Some((false, None)),
            None,
        ),
        (
            "unknown finite",
            ats.clone(),
            Some(20),
            Some((false, Some(40))),
            None,
        ),
        (
            "before first refresh",
            ats.clone(),
            Some(20),
            Some((true, Some(10))),
            Some((None, Some(20))),
        ),
        (
            "at first refresh",
            ats.clone(),
            Some(20),
            Some((true, Some(20))),
            Some((None, Some(20))),
        ),
        (
            "strict predecessor",
            ats.clone(),
            Some(20),
            Some((true, Some(40))),
            Some((Some(20), Some(40))),
        ),
        (
            "between refreshes",
            ats.clone(),
            Some(20),
            Some((true, Some(30))),
            Some((Some(20), Some(30))),
        ),
        (
            "completed AT",
            ats.clone(),
            Some(20),
            Some((true, None)),
            Some((Some(40), None)),
        ),
        (
            "aligned EVERY",
            every.clone(),
            Some(3),
            Some((true, Some(23))),
            Some((Some(13), Some(23))),
        ),
        (
            "before alignment",
            json!({"ats": [], "everies": [
                {"interval": {"secs": 0, "nanos": 10000000}, "aligned_to": 103}
            ]}),
            Some(3),
            Some((true, Some(4))),
            Some((Some(3), Some(4))),
        ),
        (
            "mixed AT wins",
            mixed.clone(),
            Some(5),
            Some((true, Some(23))),
            Some((Some(22), Some(23))),
        ),
        (
            "mixed EVERY wins",
            mixed,
            Some(5),
            Some((true, Some(28))),
            Some((Some(27), Some(28))),
        ),
        // Duration::as_millis in repr::RefreshSchedule truncates 1001.5ms.
        (
            "fractional milliseconds",
            fractional,
            Some(3),
            Some((true, Some(2005))),
            Some((Some(1004), Some(2005))),
        ),
        (
            "finite maximum EVERY",
            every.clone(),
            Some(3),
            Some((true, Some(u64::MAX))),
            Some((Some(18446744073709551613), Some(u64::MAX))),
        ),
        (
            "completed EVERY",
            every,
            Some(3),
            Some((true, None)),
            Some((Some(u64::MAX), None)),
        ),
        (
            "finite maximum AT",
            json!({"ats": [20, u64::MAX], "everies": []}),
            Some(20),
            Some((true, Some(u64::MAX))),
            Some((Some(20), Some(u64::MAX))),
        ),
        (
            "completed maximum AT",
            json!({"ats": [20, u64::MAX], "everies": []}),
            Some(20),
            Some((true, None)),
            Some((Some(u64::MAX), None)),
        ),
        (
            "no schedule",
            Value::Null,
            Some(20),
            Some((true, None)),
            None,
        ),
        (
            "no initial as of",
            ats.clone(),
            None,
            Some((true, None)),
            None,
        ),
    ] {
        for statement in [sql!("DELETE FROM plans"), sql!("DELETE FROM frontiers")] {
            batch_execute(&client, statement).await.unwrap();
        }
        insert_plan(&client, plan(1, &[], schedule, initial)).await;
        if let Some((known, upper)) = frontier {
            let upper = upper
                .map(|t| Sql::literal(&t.to_string()))
                .unwrap_or_else(|| sql!("NULL"));
            batch_execute(
                &client,
                sql!(
                    "INSERT INTO frontiers VALUES ('u1', {}, {}::mz_timestamp)",
                    if known { sql!("true") } else { sql!("false") },
                    upper
                ),
            )
            .await
            .unwrap();
        }
        let expected: Vec<_> = expected
            .into_iter()
            .map(|(last, next)| {
                vec![
                    Some("u1".into()),
                    last.map(|t| t.to_string()),
                    next.map(|t| t.to_string()),
                ]
            })
            .collect();
        assert_rows(&client, refresh_query(), &expected, name).await;
    }

    for (name, placement, namespace, kind) in [
        (
            "dropped or old writer",
            "",
            PROTOTYPE_SHARED_PLAN_NAMESPACE,
            "WrittenPlan",
        ),
        (
            "not replica owned",
            "('u1', false, false)",
            PROTOTYPE_SHARED_PLAN_NAMESPACE,
            "WrittenPlan",
        ),
        (
            "compute global",
            "('u1', true, true)",
            PROTOTYPE_SHARED_PLAN_NAMESPACE,
            "WrittenPlan",
        ),
        (
            "foreign namespace",
            "('u1', true, false)",
            "foreign",
            "WrittenPlan",
        ),
        (
            "other metadata",
            "('u1', true, false)",
            PROTOTYPE_SHARED_PLAN_NAMESPACE,
            "Other",
        ),
    ] {
        for statement in [sql!("DELETE FROM plans"), sql!("DELETE FROM placement")] {
            batch_execute(&client, statement).await.unwrap();
        }
        if !placement.is_empty() {
            batch_execute(
                &client,
                sql!("INSERT INTO placement VALUES {}", Sql::new(placement)),
            )
            .await
            .unwrap();
        }
        let mut record = plan(1, &[], ats.clone(), Some(20));
        record["key"]["build_version"] = json!(namespace);
        record["kind"] = json!(kind);
        insert_plan(&client, record).await;
        assert_rows(&client, refresh_query(), &[], name).await;
    }
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn selected_plan_dependencies() {
    let server = TestHarness::default()
        .unsafe_mode()
        .with_system_parameter_default("unsafe_enable_unstable_dependencies".into(), "true".into())
        .start()
        .await;
    let client = server.connect().await.unwrap();
    fixture(&client).await;
    batch_execute(
        &client,
        sql!(
            "INSERT INTO placement VALUES
        ('u1', true, false), ('u2', true, true), ('u3', true, false), ('u4', false, false)"
        ),
    )
    .await
    .unwrap();
    // Physical alias IDs must survive unchanged, even without placement for the
    // imported object. The broader validation import u900 must never be an edge.
    insert_plan(
        &client,
        plan(
            1,
            &[json!({"User": 101}), json!({"System": 7})],
            Value::Null,
            None,
        ),
    )
    .await;
    insert_plan(&client, plan(2, &[], Value::Null, None)).await;
    let mut foreign = plan(3, &[json!({"User": 303})], Value::Null, None);
    foreign["key"]["build_version"] = json!("foreign");
    insert_plan(&client, foreign).await;
    for id in [4, 5] {
        // u5 is an observer selection with no current placement.
        insert_plan(
            &client,
            plan(id, &[json!({"User": 404})], Value::Null, None),
        )
        .await;
    }
    let dependencies = || {
        sql!("SELECT object_id, dependency_id FROM dependencies ORDER BY object_id, dependency_id")
    };
    let edges = |ids: &[&str]| {
        ids.iter()
            .map(|id| vec![Some("u1".into()), Some((*id).into())])
            .collect::<Vec<_>>()
    };
    assert_rows(
        &client,
        dependencies(),
        &edges(&["s7", "u101"]),
        "physical imports only",
    )
    .await;

    batch_execute(
        &client,
        sql!("DELETE FROM plans WHERE data->'key'->'id' = '{\"User\":1}'::jsonb"),
    )
    .await
    .unwrap();
    insert_plan(&client, plan(1, &[json!({"User": 202})], Value::Null, None)).await;
    assert_rows(
        &client,
        dependencies(),
        &edges(&["u202"]),
        "replacement removes old edges",
    )
    .await;

    batch_execute(
        &client,
        sql!("DELETE FROM placement WHERE object_id = 'u1'"),
    )
    .await
    .unwrap();
    assert_rows(
        &client,
        dependencies(),
        &[],
        "placement removal hides retained plan",
    )
    .await;
}
