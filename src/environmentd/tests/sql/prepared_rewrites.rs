// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mz_adapter::test_util::observe_prepared_rewrites;
use mz_catalog::durable::{CatalogError, DurableCatalogError, persist_backed_catalog_join_active};
use mz_catalog::expr_cache::ExpressionCacheHandle;
use mz_environmentd::test_util::TestHarness;
use mz_postgres_util::{PostgresError, batch_execute, query_one, sql};
use mz_repr::Timestamp;
use tokio_postgres::error::SqlState;

#[derive(Clone, Copy)]
enum Conflict {
    Heartbeat,
    Structural,
    Reclamation,
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
async fn metadata_conflict_preserves_prepared_mv_rewrite() {
    run(Conflict::Heartbeat).await;
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
async fn structural_conflict_rejects_prepared_mv_rewrite() {
    run(Conflict::Structural).await;
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
async fn reclaimed_client_rejects_prepared_mv_rewrite() {
    run(Conflict::Reclamation).await;
}

async fn run(conflict: Conflict) {
    tokio::time::timeout(Duration::from_secs(180), run_inner(conflict))
        .await
        .expect("prepared MV rewrite test timed out");
}

async fn run_inner(conflict: Conflict) {
    // Pin maintained requirements from birth, including in native followers.
    // Heartbeats and inline grant acquisition keep their normal behavior.
    let server = TestHarness::default()
        .with_system_parameter_default(
            "catalog_read_protection_publish_interval".into(),
            "1h".into(),
        )
        .start()
        .await;
    let client = server.connect().await.unwrap();
    for statement in [
        sql!("CREATE DATABASE rewrite_unrelated"),
        sql!("CREATE TABLE rewrite_input (a int)"),
        sql!("INSERT INTO rewrite_input VALUES (1), (2), (3)"),
        sql!("CREATE INDEX rewrite_idx ON rewrite_input (a)"),
    ] {
        batch_execute(&client, statement).await.unwrap();
    }

    // Planning eligibility is not readability. Wait for an actual readable
    // interval before asking the MV optimizer to select the index.
    loop {
        let ready: bool = query_one(
            &client,
            sql!(
                "SELECT EXISTS (
                   SELECT 1 FROM mz_internal.mz_frontiers f
                   JOIN mz_internal.mz_object_global_ids g ON g.global_id = f.object_id
                   JOIN mz_catalog.mz_indexes i ON i.id = g.id
                   WHERE i.name = 'rewrite_idx'
                     AND f.read_frontier IS NOT NULL
                     AND f.write_frontier > f.read_frontier)"
            ),
            &[],
        )
        .await
        .unwrap()
        .get(0);
        if ready {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    batch_execute(
        &client,
        sql!("CREATE MATERIALIZED VIEW rewrite_mv AS SELECT sum(a) AS total FROM rewrite_input"),
    )
    .await
    .unwrap();

    let persist = server
        .persist_clients
        .open(server.persist_location.clone())
        .await
        .unwrap();
    let mut peer = persist_backed_catalog_join_active(
        persist.clone(),
        server.environment_id.organization_id(),
        mz_environmentd::BUILD_INFO.semver_version(),
        Arc::new(mz_catalog::durable::Metrics::new(
            &mz_ore::metrics::MetricsRegistry::new(),
        )),
    )
    .await
    .unwrap();

    // Create the peer before installing the hook so the metadata case races
    // only a heartbeat publication.
    let (input, index, mv, selection, shard, peer_incarnation) = loop {
        peer.sync_to_current_updates().await.unwrap();
        let mut txn = match peer.transaction().await {
            Ok(txn) => txn,
            Err(CatalogError::Durable(DurableCatalogError::CatalogOutOfSync { .. })) => continue,
            Err(error) => panic!("fixture snapshot: {error}"),
        };
        let item = |name| txn.get_items().find(|item| item.name == name).unwrap();
        let input = item("rewrite_input").global_id;
        let index = item("rewrite_idx").global_id;
        let mv = item("rewrite_mv").global_id;
        let selection = txn.get_written_plans().find(|plan| plan.id == mv).unwrap();
        let shard = txn.get_expression_cache_shard().unwrap();
        let incarnation = txn.create_client_incarnation(None).unwrap();
        let ts = txn.upper();
        let _ = txn.get_and_commit_op_updates();
        match txn.commit(ts).await {
            Ok(()) => break (input, index, mv, selection, shard, incarnation),
            Err(CatalogError::Durable(DurableCatalogError::CatalogOutOfSync { .. })) => continue,
            Err(error) => panic!("create peer incarnation: {error}"),
        }
    };
    loop {
        match batch_execute(
            &client,
            sql!("COMMENT ON TABLE rewrite_input IS 'follow peer'"),
        )
        .await
        {
            Ok(()) => break,
            Err(PostgresError::Postgres(error))
                if error.code() == Some(&SqlState::T_R_SERIALIZATION_FAILURE) => {}
            Err(error) => panic!("follow peer: {error}"),
        }
    }
    let store = ExpressionCacheHandle::open_plan_store(
        selection.build_version.parse().unwrap(),
        &persist,
        shard,
    )
    .await;
    let plans = store
        .read_plans(vec![(mv, selection.revision)])
        .await
        .unwrap();
    assert!(
        plans[&(mv, selection.revision)]
            .physical_plan
            .index_imports
            .contains_key(&index)
    );
    // This group commit moves the timeline window beyond the pinned MV
    // requirement. Its token cannot mask missing preparation protection.
    batch_execute(&client, sql!("INSERT INTO rewrite_input VALUES (4)"))
        .await
        .expect("advance the timeline window before preparation");

    let observations = Arc::new(Mutex::new(Vec::new()));
    let (prepared_tx, prepared_rx) = tokio::sync::oneshot::channel();
    let prepared_tx = Mutex::new(Some(prepared_tx));
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let rendezvous_failed = Arc::new(AtomicBool::new(false));
    let observer = observe_prepared_rewrites(&server.environment_id, {
        let observations = Arc::clone(&observations);
        let rendezvous_failed = Arc::clone(&rendezvous_failed);
        move |observation| {
            observations.lock().unwrap().push(observation.clone());
            let prepared_tx = prepared_tx.lock().unwrap().take();
            if let Some(tx) = prepared_tx {
                if tx.send(observation.clone()).is_ok() {
                    // Never leave the coordinator parked if the peer task fails.
                    if release_rx
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(60))
                        .is_err()
                    {
                        rendezvous_failed.store(true, Ordering::SeqCst);
                    }
                }
            }
        }
    });

    // Only DROP runs SQL during this rendezvous. In particular, no SELECT or
    // open read transaction on the input can mask a missing preparation hold.
    let drop_index = batch_execute(&client, sql!("DROP INDEX rewrite_idx"));
    tokio::pin!(drop_index);
    let prepared = tokio::select! {
        prepared = prepared_rx => prepared.expect("DROP must prepare a nonempty rewrite"),
        result = &mut drop_index => panic!("DROP completed without observing its required rewrite: {result:?}"),
    };
    let revision = prepared.selections[&mv];
    assert_ne!(revision, selection.revision);
    let plans = store.read_plans(vec![(mv, revision)]).await.unwrap();
    let replacement = &plans[&(mv, revision)].physical_plan;
    assert!(replacement.index_imports.is_empty());
    assert!(replacement.source_imports.contains_key(&input));

    let required = loop {
        peer.sync_to_current_updates().await.unwrap();
        let mut txn = match peer.transaction().await {
            Ok(txn) => txn,
            Err(CatalogError::Durable(DurableCatalogError::CatalogOutOfSync { .. })) => {
                continue;
            }
            Err(error) => panic!("peer conflict snapshot: {error}"),
        };
        let snapshot = txn.current_snapshot();
        let mv_proto = &snapshot
            .items
            .values()
            .find(|item| item.name == "rewrite_mv")
            .unwrap()
            .global_id;
        let required = snapshot
            .maintained_read_requirements
            .iter()
            .find(|(key, _)| &key.id == mv_proto)
            .and_then(|(_, value)| value.frontier)
            .map(Timestamp::from)
            .expect("MV must have a live source requirement");
        let incarnation = prepared
            .incarnation
            .expect("actual query client incarnation");
        let heartbeat = snapshot
            .client_incarnations
            .iter()
            .find(|(key, _)| key.id == incarnation)
            .expect("prepared client must still be live")
            .1
            .heartbeat;
        assert!(
            prepared
                .active_holds
                .get(&input)
                .is_some_and(|held| *held <= required),
            "active input hold must cover the MV requirement {required}: {prepared:?}"
        );
        assert!(
            prepared
                .active_holds_before_preparation
                .get(&input)
                .is_none_or(|held| *held > required),
            "unrelated holds must not mask preparation protection at {required}: {prepared:?}"
        );
        match conflict {
            Conflict::Heartbeat => {
                txn.publish_client_read_requirements(peer_incarnation, BTreeMap::new())
                    .unwrap();
            }
            Conflict::Structural => {
                let mut database = txn
                    .get_databases()
                    .find(|database| database.name == "rewrite_unrelated")
                    .unwrap();
                database.name = "rewrite_renamed".into();
                txn.update_database(database.id, database).unwrap();
            }
            Conflict::Reclamation => {
                // Exercise the reclaimer's exact conditional record change,
                // not its expiry timer or a fabricated coordinator error.
                assert!(
                    txn.reclaim_client_incarnation(incarnation, heartbeat)
                        .unwrap()
                );
            }
        }
        let ts = txn.upper();
        let _ = txn.get_and_commit_op_updates();
        match txn.commit(ts).await {
            Ok(()) => break required,
            Err(CatalogError::Durable(DurableCatalogError::CatalogOutOfSync { .. })) => {
                continue;
            }
            Err(error) => panic!("peer conflict commit: {error}"),
        }
    };
    release_tx.send(()).expect("release prepared commit");
    let result = drop_index.await;
    drop(observer);
    assert!(
        !rendezvous_failed.load(Ordering::SeqCst),
        "peer commit rendezvous timed out"
    );

    match conflict {
        Conflict::Heartbeat => result.unwrap(),
        Conflict::Structural => match result.unwrap_err() {
            PostgresError::Postgres(error) => {
                assert_eq!(error.code(), Some(&SqlState::T_R_SERIALIZATION_FAILURE));
            }
            error => panic!("expected DDL transaction race: {error}"),
        },
        Conflict::Reclamation => match result.unwrap_err() {
            PostgresError::Postgres(error) => {
                let message = error
                    .as_db_error()
                    .expect("SQL protection rejection")
                    .message();
                assert!(
                    message.contains("protection") && message.contains("closed"),
                    "{error}"
                );
            }
            error => panic!("expected closed client rejection: {error}"),
        },
    }
    // Read the outcome through the peer even if reclamation closed SQL's client.
    loop {
        peer.sync_to_current_updates().await.unwrap();
        let txn = match peer.transaction().await {
            Ok(txn) => txn,
            Err(CatalogError::Durable(DurableCatalogError::CatalogOutOfSync { .. })) => continue,
            Err(error) => panic!("durable outcome: {error}"),
        };
        let actual = txn.get_written_plan(mv, &selection.build_version).unwrap();
        let index_exists = txn.get_items().any(|item| item.global_id == index);
        match conflict {
            Conflict::Heartbeat => {
                assert!(!index_exists);
                // Identity, not callback count, proves that the preparation
                // made before the peer commit was the one durably selected.
                assert_eq!(actual, prepared.selections[&mv]);
                for observation in observations.lock().unwrap().iter() {
                    assert!(
                        observation
                            .active_holds
                            .get(&input)
                            .is_some_and(|held| *held <= required),
                        "protection must survive commit retries: {observation:?}"
                    );
                }
            }
            Conflict::Structural | Conflict::Reclamation => {
                assert!(index_exists);
                assert_eq!(actual, selection.revision);
                if matches!(conflict, Conflict::Reclamation) {
                    assert!(
                        !txn.current_snapshot()
                            .client_incarnations
                            .keys()
                            .any(|key| Some(key.id) == prepared.incarnation)
                    );
                } else {
                    assert!(
                        txn.get_databases()
                            .any(|database| database.name == "rewrite_renamed")
                    );
                }
            }
        }
        break;
    }
    if matches!(conflict, Conflict::Heartbeat) {
        let total: i64 = query_one(&client, sql!("SELECT total FROM rewrite_mv"), &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(total, 10);
        batch_execute(&client, sql!("INSERT INTO rewrite_input VALUES (5)"))
            .await
            .unwrap();
        let total: i64 = query_one(&client, sql!("SELECT total FROM rewrite_mv"), &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(total, 15);
    }
}
