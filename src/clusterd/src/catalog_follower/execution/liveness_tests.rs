// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Installation admission at the durable protection liveness boundary.

use std::sync::Arc;
use std::time::{Duration, Instant};

use mz_catalog::catalog::{Catalog, Op};
use mz_catalog::durable::TestCatalogStateBuilder;
use mz_catalog::read_protection::CLIENT_PROTECTION_UNCHANGED_GRACE;
use mz_cluster_client::client::TimelyConfig;
use mz_compute::server::{ComputeInstanceContext, ComputeRuntimeRole};
use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_client::protocol::response::ComputeResponse;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::tracing::TracingHandle;
use mz_repr::Timestamp;
use mz_txn_wal::operator::TxnsContext;
use tokio::time::timeout;
use uuid::Uuid;

use super::tests::{Fixture, assert_rows, in_child};
use super::{ReplicaEffects, ReplicaEnactment, absorb_updates, storage_metadata};

#[mz_ore::test(tokio::test)]
async fn stale_protection_blocks_install_until_renewed() {
    if !in_child(
        "MZ_CLUSTERD_PROTECTION_LIVENESS_TEST_CHILD",
        "catalog_follower::execution::liveness_tests::stale_protection_blocks_install_until_renewed",
        Duration::from_secs(120),
    )
    .await
    {
        return;
    }

    Box::pin(exercise_liveness()).await;
}

async fn exercise_liveness() {
    let mut fixture = Box::pin(Fixture::new((1, 15_000), 20_000, false)).await;
    let config = &fixture.config;
    let cluster = config.cluster_id;
    let replica = config.replica_id;
    let build = config.reconstruction.plan_build.clone();
    // Open a native follower snapshot so bootstrap effects, including log indexes,
    // are absorbed exactly as they are by the production follower.
    let storage = TestCatalogStateBuilder::new(fixture.persist.clone())
        .with_organization_id(config.environment_id.organization_id())
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .join()
        .await
        .unwrap();
    let opened = Box::pin(Catalog::open_committed(
        fixture.writer.replica_config().into_state(
            config.build_info,
            config.environment_id.clone(),
            config.connection_context.clone(),
            fixture.persist.clone(),
        ),
        storage,
    ))
    .await
    .unwrap();
    let mut catalog = opened.catalog;
    let mut effects = ReplicaEffects::default();
    absorb_updates(
        &mut effects,
        &catalog,
        cluster,
        &build,
        opened.initial_updates,
    );
    let publication_started = Instant::now();
    let ts = catalog.current_upper().await;
    let created = catalog
        .transact(
            None,
            ts,
            None,
            vec![Op::CreateClientIncarnation {
                replica_id: Some(replica),
            }],
        )
        .await
        .unwrap();
    let incarnation = created.created_client_incarnations[0];
    absorb_updates(
        &mut effects,
        &catalog,
        cluster,
        &build,
        created.catalog_updates,
    );

    let registry = MetricsRegistry::new();
    let mut server = mz_compute::server::serve(
        TimelyConfig {
            workers: 2,
            addresses: vec!["127.0.0.1:0".into()],
            ..Default::default()
        },
        ComputeRuntimeRole::Solo,
        true,
        &registry,
        Arc::clone(&fixture.clients),
        TxnsContext::default(),
        Arc::new(TracingHandle::disabled()),
        ComputeInstanceContext {
            scratch_directory: None,
            worker_core_affinity: false,
            connection_context: config.connection_context.clone(),
        },
    )
    .await
    .unwrap();
    let endpoint = server.take_replica().unwrap();
    let factory = server.client_builder();
    drop(server);
    let instance = mz_catalog::compute_config::replica_instance_config(
        &catalog,
        cluster,
        replica,
        config.persist_location.clone(),
    );
    let mut driver = ReplicaEnactment::new(
        endpoint,
        instance,
        incarnation,
        publication_started,
        cluster,
        replica,
        &registry,
        None,
    );
    driver.configure(mz_catalog::compute_config::replica_compute_config(
        &catalog, cluster, replica,
    ));
    driver
        .io
        .wait(effects.observe_plans(&catalog, cluster, replica, &fixture.store, &build))
        .await
        .unwrap();
    let wanted = effects
        .selected
        .values()
        .flat_map(|(_, _, plan)| {
            plan.physical_plan
                .imported_source_ids()
                .chain(plan.physical_plan.persist_sink_ids())
        })
        .collect();
    let metadata = driver
        .io
        .wait(storage_metadata::resolve(
            &catalog,
            &wanted,
            &fixture.store,
            &build,
            &fixture.persist,
            &config.persist_location,
            opened.txn_wal_shard,
        ))
        .await
        .unwrap();
    assert!(effects.pending.is_empty());
    assert!(metadata.pending.is_empty());
    assert!(
        effects
            .selected
            .values()
            .any(|(id, _, _)| *id == fixture.index)
    );
    let pending = driver.pending_installations(&effects);
    assert!(pending > 0);

    // Only the local publication clock is aged. Catalog grants, read holds,
    // Persist frontiers, and worker responses all follow their normal APIs.
    driver.published_at = Instant::now() - CLIENT_PROTECTION_UNCHANGED_GRACE;
    let error = driver
        .install(
            &mut catalog,
            &mut effects,
            cluster,
            &build,
            &fixture.store,
            &fixture.persist,
            &metadata,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("requires renewal"), "{error:#}");
    assert_eq!(driver.pending_installations(&effects), pending);

    let mut query = factory();
    query
        .send(ComputeCommand::HelloQuery {
            nonce: Uuid::new_v4(),
        })
        .await
        .unwrap();
    assert!(matches!(
        driver.io.wait(query.recv()).await.unwrap(),
        Some(ComputeResponse::QueryReady)
    ));
    // The query endpoint must not advertise an index for the refused plan.
    driver
        .io
        .wait(async {
            let result = timeout(Duration::from_millis(200), async {
                loop {
                    let response = query.recv().await.unwrap().expect("query connection");
                    assert!(
                        !matches!(response, ComputeResponse::Frontiers(id, _) if id == fixture.index),
                        "stale installation exposed the index: {response:?}"
                    );
                }
            })
            .await;
            assert!(result.is_err());
        })
        .await;

    let heartbeat = catalog.state().client_incarnations()[&incarnation].heartbeat;
    driver
        .publish(&mut catalog, &mut effects, cluster, &build, true, None)
        .await
        .unwrap();
    assert!(catalog.state().client_incarnations()[&incarnation].heartbeat > heartbeat);
    // A peer advances metadata after the follower's snapshot. Admission must
    // absorb that prefix and retry its grant, without another outer install tick.
    fixture.writer.sync_to_current_updates().await.unwrap();
    let ts = fixture.writer.current_upper().await;
    let peer = fixture
        .writer
        .transact(
            None,
            ts,
            None,
            vec![Op::CreateClientIncarnation { replica_id: None }],
        )
        .await
        .unwrap()
        .created_client_incarnations[0];
    driver
        .install(
            &mut catalog,
            &mut effects,
            cluster,
            &build,
            &fixture.store,
            &fixture.persist,
            &metadata,
        )
        .await
        .unwrap();
    assert_eq!(driver.pending_installations(&effects), 0);
    assert!(catalog.state().client_incarnations().contains_key(&peer));
    timeout(Duration::from_secs(30), async {
        loop {
            driver.apply_progress(&catalog);
            driver.apply_catalog(&catalog, &build, &metadata, true);
            let progress = &driver.installed[&fixture.index].progress;
            if progress.hydrated == Some(true)
                && progress
                    .write_frontier
                    .as_ref()
                    .is_some_and(|upper| !upper.less_equal(&Timestamp::new(15_000)))
            {
                break;
            }
            driver
                .io
                .wait(tokio::time::sleep(Duration::from_millis(10)))
                .await;
        }
    })
    .await
    .expect("renewed installation must hydrate through real worker progress");
    driver
        .io
        .wait(assert_rows(
            &mut *query,
            fixture.index,
            &fixture.desc,
            15_000,
            &[1],
        ))
        .await;

    // Durable reclamation, unlike mere local staleness, cannot be repaired by
    // renewing the same incarnation. The heartbeat CAS is the catalog boundary.
    let expected_heartbeat = catalog.state().client_incarnations()[&incarnation].heartbeat;
    driver
        .transact(
            &mut catalog,
            &mut effects,
            cluster,
            &build,
            vec![Op::ReclaimClientIncarnation {
                incarnation,
                expected_heartbeat,
            }],
        )
        .await
        .unwrap();
    assert!(
        !catalog
            .state()
            .client_incarnations()
            .contains_key(&incarnation)
    );
    driver.published_at = Instant::now() - CLIENT_PROTECTION_UNCHANGED_GRACE;
    let stale = driver.published_at;
    let error = driver
        .publish(&mut catalog, &mut effects, cluster, &build, true, None)
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains(&format!("client incarnation {incarnation} is closed")),
        "{error:#}"
    );
    assert_eq!(driver.published_at, stale);
    let error = driver
        .install(
            &mut catalog,
            &mut effects,
            cluster,
            &build,
            &fixture.store,
            &fixture.persist,
            &metadata,
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("incarnation was reclaimed"),
        "{error:#}"
    );
    std::process::exit(0);
}
