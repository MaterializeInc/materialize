// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Real selected-plan producers and shared public projections across adapters.
//! `start_in` restarts adapter state, not replicas or the process-wide Persist cache.

#![recursion_limit = "256"]

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use mz_catalog::expr_cache::PROTOTYPE_SHARED_PLAN_NAMESPACE;
use mz_environmentd::test_util::TestHarness;
use mz_postgres_util::{Sql, batch_execute, query, query_one, sql};
use mz_repr::AdapterId;
use tokio_postgres::Client;

const POLL: Duration = Duration::from_millis(100);

#[derive(Debug, PartialEq)]
struct Selection {
    revision: String,
    birth: String,
    schedule: String,
}

async fn selection(client: &Client, writer: &str) -> Selection {
    // Unrelated catalog kinds have different key shapes. WHERE conjunct order
    // cannot protect the fallible catalog-ID decoder.
    let rows = query(
        client,
        sql!(
            "SELECT data->'value'->>'revision',
                    data->'value'->'introspection'->>'initial_storage_as_of',
                    data->'value'->'introspection'->>'refresh_schedule'
             FROM mz_internal.mz_catalog_raw
             WHERE CASE WHEN data->>'kind' = 'WrittenPlan'
                        THEN mz_internal.parse_catalog_id(data->'key'->'id') = $1
                        ELSE false END
               AND data->'key'->>'build_version' = $2"
        ),
        &[&writer, &PROTOTYPE_SHARED_PLAN_NAMESPACE],
    )
    .await
    .expect("read selected WrittenPlan");
    assert_eq!(rows.len(), 1, "one shared selection for writer {writer}");
    let selected = Selection {
        revision: rows[0]
            .get::<_, Option<String>>(0)
            .expect("selected revision"),
        birth: rows[0]
            .get::<_, Option<String>>(1)
            .expect("original storage birth"),
        schedule: rows[0]
            .get::<_, Option<String>>(2)
            .expect("selected refresh schedule"),
    };
    let schedule: serde_json::Value =
        serde_json::from_str(&selected.schedule).expect("selected schedule JSON");
    let birth: u64 = selected.birth.parse().expect("numeric storage birth");
    assert!(
        schedule["ats"]
            .as_array()
            .expect("AT schedule")
            .contains(&serde_json::json!(birth)),
        "AT CREATION must retain the original birth in the selected schedule"
    );
    let everies = schedule["everies"].as_array().expect("EVERY schedule");
    assert_eq!(everies.len(), 1, "one periodic refresh schedule");
    assert_eq!(
        everies[0]["interval"],
        serde_json::json!({"secs": 36000, "nanos": 0})
    );
    selected
}

async fn global_id(client: &Client, name: &str) -> String {
    query_one(
        client,
        sql!(
            "SELECT g.global_id FROM mz_internal.mz_object_global_ids g
             JOIN mz_catalog.mz_objects o ON o.id = g.id
             WHERE o.name = $1"
        ),
        &[&name],
    )
    .await
    .expect("resolve exactly one public GlobalId")
    .get(0)
}

// UNION ALL and no DISTINCT preserve multiplicity. All three projections are
// read at one SQL timestamp, including old IDs after DROP/replacement.
// Progress is checked separately so legitimate advancement cannot fail a
// lifecycle observation of row identity and multiplicity.
fn public_rows(ids: &[&str]) -> Sql {
    let ids = ids.iter().map(|id| Sql::literal(id)).collect::<Vec<_>>();
    sql!(
        "SELECT 'dependency', object_id, dependency_id
           FROM mz_internal.mz_compute_dependencies WHERE object_id IN ({})
         UNION ALL
         SELECT 'refresh', materialized_view_id, 'present'
           FROM mz_internal.mz_materialized_view_refreshes
           WHERE materialized_view_id IN ({})
         UNION ALL
         SELECT 'shard', object_id, shard_id FROM mz_internal.mz_storage_shards
           WHERE object_id IN ({})
         ORDER BY 1, 2, 3",
        Sql::join(ids.clone(), ", "),
        Sql::join(ids.clone(), ", "),
        Sql::join(ids, ", ")
    )
}

async fn rows(client: &Client, statement: Sql) -> Vec<(String, String, String)> {
    query(client, statement, &[])
        .await
        .expect("read public plan projections")
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect()
}

async fn assert_rows(
    client: &Client,
    statement: &Sql,
    expected: &[(String, String, String)],
    context: &str,
) {
    assert_eq!(rows(client, statement.clone()).await, expected, "{context}");
}

async fn observe_transition<T>(
    observer: &Client,
    statement: &Sql,
    expected: &[(String, String, String)],
    transition: impl Future<Output = T>,
    context: &str,
) -> T {
    tokio::pin!(transition);
    loop {
        tokio::select! {
            result = &mut transition => {
                assert_rows(observer, statement, expected, context).await;
                return result;
            }
            _ = tokio::time::sleep(POLL) => {
                assert_rows(observer, statement, expected, context).await;
            }
        }
    }
}

async fn wait_for_refresh(client: &Client, writer: &str, birth: &str) {
    loop {
        let ready: bool = query_one(
            client,
            sql!(
                "SELECT count(*) = 1 AND coalesce(bool_and(
                    last_completed_refresh::text::numeric >= $2::text::numeric
                    AND next_refresh > last_completed_refresh), false)
                 FROM mz_internal.mz_materialized_view_refreshes
                 WHERE materialized_view_id = $1"
            ),
            &[&writer, &birth],
        )
        .await
        .expect("wait for refresh progress through original birth")
        .get(0);
        if ready {
            return;
        }
        tokio::time::sleep(POLL).await;
    }
}

async fn shard(client: &Client, writer: &str) -> String {
    query_one(
        client,
        sql!("SELECT shard_id FROM mz_internal.mz_storage_shards WHERE object_id = $1"),
        &[&writer],
    )
    .await
    .expect("exactly one public storage shard")
    .get(0)
}

async fn assert_contents(client: &Client, expected: i64) {
    let actual: i64 = query_one(client, sql!("SELECT total FROM plan_mv"), &[])
        .await
        .expect("read AT CREATION contents without waiting for the periodic refresh")
        .get(0);
    assert_eq!(actual, expected, "actual scheduled MV contents");
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn real_plan_introspection() {
    tokio::time::timeout(Duration::from_secs(180), run())
        .await
        .expect("real plan introspection exceeded whole-test 180s timeout");
}

async fn run() {
    let harness = TestHarness::default()
        .with_system_parameter_default("enable_refresh_every_mvs".into(), "true".into());
    let server = harness.clone().start().await;
    let deployment = server.deployment();
    let client = server
        .connect()
        .internal()
        .await
        .expect("creator connection");
    for statement in [
        sql!("CREATE CLUSTER plan_cluster SIZE 'scale=1,workers=1'"),
        sql!("SET cluster = plan_cluster"),
        sql!("CREATE TABLE plan_input (a int)"),
        sql!("INSERT INTO plan_input VALUES (1), (2), (3)"),
        sql!("CREATE INDEX plan_idx ON plan_input (a)"),
    ] {
        batch_execute(&client, statement)
            .await
            .expect("create real table and index producers");
    }
    loop {
        let ready: bool = query_one(
            &client,
            sql!(
                "SELECT EXISTS (
                   SELECT 1 FROM mz_internal.mz_frontiers f
                   JOIN mz_internal.mz_object_global_ids g ON g.global_id = f.object_id
                   JOIN mz_catalog.mz_indexes i ON i.id = g.id
                   WHERE i.name = 'plan_idx' AND f.read_frontier IS NOT NULL
                     AND f.write_frontier > f.read_frontier)"
            ),
            &[],
        )
        .await
        .expect("index readable interval")
        .get(0);
        if ready {
            break;
        }
        tokio::time::sleep(POLL).await;
    }
    batch_execute(
        &client,
        sql!(
            "CREATE MATERIALIZED VIEW plan_mv
              WITH (REFRESH AT CREATION, REFRESH EVERY '10 hours')
              AS SELECT sum(a) AS total FROM plan_input"
        ),
    )
    .await
    .expect("create scheduled MV in the index's session cluster");
    let table = global_id(&client, "plan_input").await;
    let index = global_id(&client, "plan_idx").await;
    let writer = global_id(&client, "plan_mv").await;
    let original = selection(&client, &writer).await;
    wait_for_refresh(&client, &writer, &original.birth).await;
    assert_contents(&client, 6).await;
    let table_shard = shard(&client, &table).await;
    let mv_shard = shard(&client, &writer).await;
    let statement = public_rows(&[&table, &index, &writer]);
    let initial = rows(&client, statement.clone()).await;
    let refresh = initial
        .iter()
        .find(|r| r.0 == "refresh")
        .expect("completed refresh row")
        .clone();
    let mut expected = vec![
        ("dependency".into(), index.clone(), table.clone()),
        ("dependency".into(), writer.clone(), index.clone()),
        refresh,
        ("shard".into(), table.clone(), table_shard.clone()),
        ("shard".into(), writer.clone(), mv_shard.clone()),
    ];
    expected.sort();
    assert_eq!(
        initial, expected,
        "physical imports, one refresh, table/MV shards, no index shard"
    );

    batch_execute(&client, sql!("CREATE ADAPTER plan_peer (SIZE '1')"))
        .await
        .expect("declare distinct peer adapter");
    let peer_id: String = query_one(
        &client,
        sql!("SELECT id FROM mz_catalog.mz_adapters WHERE name = 'plan_peer'"),
        &[],
    )
    .await
    .expect("declared peer ID")
    .get(0);
    let peer_harness = harness
        .clone()
        .with_adapter_id(peer_id.parse::<AdapterId>().expect("AdapterId"));
    let peer = observe_transition(
        &client,
        &statement,
        &expected,
        peer_harness.clone().start_in(Arc::clone(&deployment)),
        "peer join",
    )
    .await;
    let peer_client = peer.connect().internal().await.expect("peer connection");
    assert_rows(&peer_client, &statement, &expected, "peer sees same rows").await;
    assert_eq!(selection(&peer_client, &writer).await, original);
    wait_for_refresh(&peer_client, &writer, &original.birth).await;
    assert_contents(&peer_client, 6).await;

    drop(client);
    drop(server);
    let server = observe_transition(
        &peer_client,
        &statement,
        &expected,
        harness.start_in(Arc::clone(&deployment)),
        "creator restart",
    )
    .await;
    let client = server
        .connect()
        .internal()
        .await
        .expect("restarted creator connection");
    batch_execute(&client, sql!("SET cluster = plan_cluster"))
        .await
        .expect("restore creator session cluster");
    assert_rows(
        &client,
        &statement,
        &expected,
        "creator recovered projections",
    )
    .await;
    assert_eq!(selection(&client, &writer).await, original);
    wait_for_refresh(&client, &writer, &original.birth).await;

    // An index DROP must select a new physical plan without replacing storage.
    batch_execute(&client, sql!("DROP INDEX plan_idx"))
        .await
        .expect("force real MV replan");
    let replanned = selection(&client, &writer).await;
    assert_eq!(
        global_id(&client, "plan_mv").await,
        writer,
        "replan retains writer"
    );
    assert_ne!(
        replanned.revision, original.revision,
        "replan changes selected revision"
    );
    assert_eq!(
        replanned.birth, original.birth,
        "recovery must not replace birth"
    );
    assert_eq!(
        replanned.schedule, original.schedule,
        "replan retains schedule"
    );
    expected.retain(|r| r.0 != "dependency");
    expected.push(("dependency".into(), writer.clone(), table.clone()));
    expected.sort();
    for c in [&client, &peer_client] {
        assert_rows(
            c,
            &statement,
            &expected,
            "direct MV-to-table import after DROP INDEX",
        )
        .await;
        wait_for_refresh(c, &writer, &original.birth).await;
        assert_contents(c, 6).await;
    }

    batch_execute(
        &client,
        sql!(
            "CREATE OR REPLACE MATERIALIZED VIEW plan_mv
              WITH (REFRESH AT CREATION, REFRESH EVERY '10 hours')
              AS SELECT sum(a) + 1 AS total FROM plan_input"
        ),
    )
    .await
    .expect("replace scheduled MV");
    let replacement = global_id(&client, "plan_mv").await;
    assert_ne!(replacement, writer, "replacement has a new writer");
    let replaced = selection(&client, &replacement).await;
    assert_ne!(
        replaced.birth, original.birth,
        "replacement has a new birth"
    );
    wait_for_refresh(&client, &replacement, &replaced.birth).await;
    let new_shard = shard(&client, &replacement).await;
    assert_ne!(new_shard, mv_shard, "replacement has a new shard");
    let statement = public_rows(&[&table, &index, &writer, &replacement]);
    let current = rows(&client, statement.clone()).await;
    let refresh = current
        .iter()
        .find(|r| r.0 == "refresh" && r.1 == replacement)
        .expect("replacement refresh row")
        .clone();
    let mut expected = vec![
        ("dependency".into(), replacement.clone(), table.clone()),
        refresh,
        ("shard".into(), table.clone(), table_shard),
        ("shard".into(), replacement.clone(), new_shard),
    ];
    expected.sort();
    for c in [&client, &peer_client] {
        assert_rows(
            c,
            &statement,
            &expected,
            "replacement removes all old public rows",
        )
        .await;
        wait_for_refresh(c, &replacement, &replaced.birth).await;
        assert_contents(c, 7).await;
    }

    drop(peer_client);
    drop(peer);
    // Sample after disconnect as well as throughout the subsequent peer restart.
    for _ in 0..10 {
        assert_rows(&client, &statement, &expected, "peer disconnect").await;
        tokio::time::sleep(POLL).await;
    }
    let peer = observe_transition(
        &client,
        &statement,
        &expected,
        peer_harness.start_in(Arc::clone(&deployment)),
        "peer restart after replacement",
    )
    .await;
    let peer_client = peer
        .connect()
        .internal()
        .await
        .expect("restarted peer connection");
    assert_rows(
        &peer_client,
        &statement,
        &expected,
        "restarted peer sees replacement only",
    )
    .await;
    assert_eq!(
        selection(&peer_client, &global_id(&client, "plan_mv").await).await,
        replaced
    );
    assert_contents(&peer_client, 7).await;
}
