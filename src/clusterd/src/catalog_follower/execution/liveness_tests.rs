// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Installation and publication at the durable protection liveness boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mz_catalog::catalog::{Catalog, Op};
use mz_catalog::durable::TestCatalogStateBuilder;
use mz_cluster_client::client::TimelyConfig;
use mz_compute::server::{ComputeInstanceContext, ComputeRuntimeRole};
use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_client::protocol::response::ComputeResponse;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::tracing::TracingHandle;
use mz_repr::Timestamp;
use mz_txn_wal::operator::TxnsContext;
use timely::progress::Antichain;
use tokio::time::timeout;
use uuid::Uuid;

use super::tests::{Fixture, assert_rows, in_child};
use super::{
    PublicationClock, PublicationOutcome, ReplicaEffects, ReplicaEnactment, absorb_updates,
    storage_metadata,
};

#[mz_ore::test]
fn publication_scheduling() {
    let interval = Duration::from_secs(1);
    let before = Instant::now();
    let mut clock = PublicationClock::new(interval);
    assert!(clock.due >= before && clock.due <= Instant::now() + interval);
    let phase = clock.due;
    clock.configure(interval);
    assert_eq!(clock.due, phase);
    let before = Instant::now();
    clock.completed();
    assert!(
        clock.due >= before + interval / 2 && clock.due <= Instant::now() + interval + interval / 2
    );
    let changed = Duration::from_millis(100);
    clock.configure(changed);
    assert!(clock.due >= before && clock.due <= Instant::now() + changed);

    let delay = super::conflict_delay();
    assert!(delay >= Duration::from_millis(10) && delay <= Duration::from_millis(100));
}

#[mz_ore::test(tokio::test)]
async fn protection_publication_installation_and_supersession() {
    if !in_child(
        "MZ_CLUSTERD_PROTECTION_LIVENESS_TEST_CHILD",
        "catalog_follower::execution::liveness_tests::protection_publication_installation_and_supersession",
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
    let ts = catalog.current_upper().await;
    let created = catalog
        .transact(
            None,
            None,
            ts,
            None,
            vec![Op::CreateClientIncarnation {
                replica_id: Some(replica),
                expected: None,
            }],
        )
        .await
        .unwrap();
    let predecessor = created.created_client_incarnations[0];
    let ts = catalog.current_upper().await;
    catalog
        .transact(
            None,
            None,
            ts,
            None,
            vec![Op::GrantClientReadRequirements {
                incarnation: predecessor,
                requirements: BTreeMap::from([(fixture.source, Timestamp::new(17_000))]),
            }],
        )
        .await
        .unwrap();
    let ts = catalog.current_upper().await;
    let created = catalog
        .transact(
            None,
            None,
            ts,
            None,
            vec![Op::CreateClientIncarnation {
                replica_id: Some(replica),
                expected: Some(predecessor),
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
    let requirements = catalog
        .state()
        .client_read_requirements()
        .iter()
        .filter_map(|((owner, id), frontier)| (*owner == incarnation).then_some((*id, *frontier)))
        .collect();

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
        fixture.clients.cfg().state_version_target(),
    );
    let mut driver = ReplicaEnactment::new(
        endpoint,
        instance,
        incarnation,
        requirements,
        cluster,
        replica,
        &registry,
        None,
    );
    // Outer polling honors sub-second phases, even when bounds
    // are blocked on installation. An elapsed bounds clock alone cannot spin.
    driver.configure_publication(Duration::from_secs(3600));
    let mut bounds = PublicationClock::new(Duration::from_secs(3600));
    let now = Instant::now();
    driver.publication_clock.as_mut().unwrap().due = now + Duration::from_millis(20);
    bounds.due = now + Duration::from_millis(40);
    assert!(driver.wake_delay(Duration::from_secs(10), &bounds, true) <= Duration::from_millis(20));
    driver.publication_clock.as_mut().unwrap().due = now + Duration::from_secs(3600);
    assert!(driver.wake_delay(Duration::from_secs(10), &bounds, true) <= Duration::from_millis(40));
    bounds.due = now;
    let delay = driver.wake_delay(Duration::from_secs(10), &bounds, false);
    assert_eq!(delay, Duration::from_secs(10));
    driver.configure(mz_catalog::compute_config::replica_compute_config(
        &catalog,
        cluster,
        replica,
        fixture.clients.cfg().state_version_target(),
    ));
    let catalog_position = catalog
        .planning_position()
        .expect("committed fixture position");
    // This test drives enactment without the follower loop. Certify the applied
    // configuration before installation, not later read-protection publications.
    driver.io.apply_catalog_position(
        catalog
            .observed_position()
            .expect("committed follower position"),
    );
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

    // Keep the advancement timer outside this test's process deadline. Only
    // local clocks are controlled, not catalog protection or Persist progress.
    let deferred = Instant::now() + Duration::from_secs(3600);
    driver.configure_publication(
        catalog
            .system_config()
            .catalog_read_protection_publish_interval(),
    );
    driver.publication_clock.as_mut().unwrap().due = deferred;
    let mut holds = driver
        .acquire(
            &mut catalog,
            &mut effects,
            cluster,
            &build,
            &BTreeSet::from([fixture.source]),
            &BTreeSet::new(),
            &BTreeMap::from([(fixture.source, Timestamp::new(15_000))]),
        )
        .await
        .unwrap();
    let requirement = (incarnation, fixture.source);
    assert_eq!(
        catalog.state().client_read_requirements()[&requirement],
        Timestamp::new(15_000)
    );
    holds
        .get_mut(&fixture.source)
        .unwrap()
        .try_downgrade(Antichain::from_elem(Timestamp::new(16_000)))
        .unwrap();
    driver.configure_publication(
        catalog
            .system_config()
            .catalog_read_protection_publish_interval(),
    );
    driver.publication_clock.as_mut().unwrap().due = deferred;
    driver
        .publish(&mut catalog, &mut effects, cluster, &build, true, None)
        .await
        .unwrap();
    assert_eq!(
        catalog.state().client_read_requirements()[&requirement],
        Timestamp::new(15_000)
    );
    driver.publication_clock.as_mut().unwrap().due = Instant::now();
    driver
        .publish(&mut catalog, &mut effects, cluster, &build, true, None)
        .await
        .unwrap();
    assert_eq!(
        catalog.state().client_read_requirements()[&requirement],
        Timestamp::new(16_000)
    );
    driver.configure_publication(
        catalog
            .system_config()
            .catalog_read_protection_publish_interval(),
    );
    driver.publication_clock.as_mut().unwrap().due = deferred;
    let stronger = driver
        .acquire(
            &mut catalog,
            &mut effects,
            cluster,
            &build,
            &BTreeSet::from([fixture.source]),
            &BTreeSet::new(),
            &BTreeMap::from([(fixture.source, Timestamp::new(15_000))]),
        )
        .await
        .unwrap();
    assert_eq!(
        catalog.state().client_read_requirements()[&requirement],
        Timestamp::new(15_000)
    );
    drop(stronger);
    drop(holds);
    driver.configure_publication(
        catalog
            .system_config()
            .catalog_read_protection_publish_interval(),
    );
    driver.publication_clock.as_mut().unwrap().due = deferred;
    driver
        .publish(&mut catalog, &mut effects, cluster, &build, true, None)
        .await
        .unwrap();
    assert_eq!(
        catalog.state().client_read_requirements()[&requirement],
        Timestamp::new(15_000)
    );

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
    // Pending installation retains inherited history even after local tokens drop.
    assert!(driver.recovery_holds.is_some());
    // A peer advances metadata after the follower's snapshot. Admission must
    // absorb that prefix and retry its grant, without another outer install tick.
    fixture.writer.sync_to_current_updates().await.unwrap();
    let ts = fixture.writer.current_upper().await;
    let peer = fixture
        .writer
        .transact(
            None,
            None,
            ts,
            None,
            vec![Op::CreateClientIncarnation {
                replica_id: None,
                expected: None,
            }],
        )
        .await
        .unwrap()
        .created_client_incarnations[0];
    // Maintenance yields on its first definitive conflict. Repeated entrypoints
    // and catch-up notifications cannot turn that into an inline write loop.
    driver.publication_clock.as_mut().unwrap().due = Instant::now();
    assert!(super::is_catalog_conflict(
        &driver
            .publish(&mut catalog, &mut effects, cluster, &build, true, None)
            .await
            .unwrap_err(),
    ));
    assert!(driver.publication_needs_refresh);
    let retry_after = driver.publication_retry_after.unwrap();
    assert_eq!(retry_after, driver.publication_clock.as_ref().unwrap().due);
    // Hold the gate far ahead while exercising callers, independent of host speed.
    driver.publication_retry_after = Some(Instant::now() + Duration::from_secs(3600));
    driver.io.catalog_catchup.notify_one();
    driver.io.idle(Duration::from_secs(3600)).await;
    for pending in [true, false] {
        assert_eq!(
            driver
                .publish(
                    &mut catalog,
                    &mut effects,
                    cluster,
                    &build,
                    pending,
                    Some(&metadata)
                )
                .await
                .unwrap(),
            PublicationOutcome::Deferred,
        );
    }
    driver.publication_retry_after = Some(retry_after);
    driver
        .io
        .wait(tokio::time::sleep(
            retry_after.saturating_duration_since(Instant::now()),
        ))
        .await;
    driver
        .publish(&mut catalog, &mut effects, cluster, &build, true, None)
        .await
        .unwrap();
    assert_eq!(
        catalog.state().client_read_requirements()[&requirement],
        Timestamp::new(17_000),
    );
    assert!(driver.recovery_holds.is_some());
    assert!(catalog.state().client_incarnations().contains_key(&peer));

    // Another peer commit leaves acquisition itself responsible for fresh grants.
    fixture.writer.sync_to_current_updates().await.unwrap();
    let ts = fixture.writer.current_upper().await;
    fixture
        .writer
        .transact(
            None,
            None,
            ts,
            None,
            vec![Op::GrantClientReadRequirements {
                incarnation: peer,
                requirements: BTreeMap::from([(fixture.source, Timestamp::new(20_000))]),
            }],
        )
        .await
        .unwrap();
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
    assert_eq!(
        catalog.state().client_read_requirements()[&(peer, fixture.source)],
        Timestamp::new(20_000),
    );
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
    .expect("installation must hydrate through real worker progress");
    driver
        .io
        .wait(assert_rows(
            &mut *query,
            catalog_position,
            fixture.index,
            &fixture.desc,
            15_000,
            &[1],
        ))
        .await;

    // Publish the installed reader's advancement before testing bounds. A
    // deferred aggregate would itself retain the old bound and require no write.
    driver.configure_publication(
        catalog
            .system_config()
            .catalog_read_protection_publish_interval(),
    );
    driver.publication_clock.as_mut().unwrap().due = Instant::now();
    driver
        .publish(&mut catalog, &mut effects, cluster, &build, true, None)
        .await
        .unwrap();
    // Bounds conflicts yield too. A peer adds historical protection between
    // attempts, so retry must derive new proposals instead of resubmitting ops.
    driver.publication_clock.as_mut().unwrap().due = deferred;
    fixture.writer.sync_to_current_updates().await.unwrap();
    let ts = fixture.writer.current_upper().await;
    fixture
        .writer
        .transact(
            None,
            None,
            ts,
            None,
            vec![Op::UpdateClientReadRequirements {
                incarnation: peer,
                requirements: BTreeMap::from([(fixture.source, None)]),
            }],
        )
        .await
        .unwrap();
    let old_bound = catalog.state().collection_compaction_bounds()[&fixture.source].clone();
    assert!(super::is_catalog_conflict(
        &driver
            .publish(
                &mut catalog,
                &mut effects,
                cluster,
                &build,
                false,
                Some(&metadata)
            )
            .await
            .unwrap_err(),
    ));
    assert!(driver.publication_needs_refresh);
    assert_eq!(
        driver.publication_retry_after.unwrap(),
        driver.publication_clock.as_ref().unwrap().due,
    );
    let ts = fixture.writer.current_upper().await;
    fixture
        .writer
        .transact(
            None,
            None,
            ts,
            None,
            vec![Op::GrantClientReadRequirements {
                incarnation: peer,
                requirements: BTreeMap::from([(fixture.source, *old_bound.as_option().unwrap())]),
            }],
        )
        .await
        .unwrap();
    let retry_after = driver.publication_retry_after.unwrap();
    driver.publication_retry_after = Some(Instant::now() + Duration::from_secs(3600));
    // A bounds conflict gates writes even when catch-up notifications wake us.
    assert_eq!(
        driver
            .publish(
                &mut catalog,
                &mut effects,
                cluster,
                &build,
                false,
                Some(&metadata)
            )
            .await
            .unwrap(),
        PublicationOutcome::Deferred,
    );
    driver.publication_retry_after = Some(retry_after);
    driver
        .io
        .wait(tokio::time::sleep(
            retry_after.saturating_duration_since(Instant::now()),
        ))
        .await;
    assert_eq!(
        driver
            .publish(
                &mut catalog,
                &mut effects,
                cluster,
                &build,
                false,
                Some(&metadata)
            )
            .await
            .unwrap(),
        PublicationOutcome::Deferred,
    );
    assert_eq!(
        driver
            .publish(
                &mut catalog,
                &mut effects,
                cluster,
                &build,
                false,
                Some(&metadata)
            )
            .await
            .unwrap(),
        PublicationOutcome::Completed,
    );
    assert!(driver.recovery_holds.is_none());
    assert_eq!(
        catalog.state().collection_compaction_bounds()[&fixture.source],
        old_bound
    );

    // A successor makes this execution terminal, including maintenance calls
    // with no changed aggregate to publish.
    driver
        .transact(
            &mut catalog,
            &mut effects,
            cluster,
            &build,
            vec![Op::CreateClientIncarnation {
                replica_id: Some(replica),
                expected: Some(incarnation),
            }],
        )
        .await
        .unwrap();
    let error = driver
        .publish(&mut catalog, &mut effects, cluster, &build, true, None)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("superseded or retired"),
        "{error:#}"
    );
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
        error.to_string().contains("superseded or retired"),
        "{error:#}"
    );
    driver
        .transact(
            &mut catalog,
            &mut effects,
            cluster,
            &build,
            vec![Op::DropClusterReplicaRealization {
                cluster_id: cluster,
                replica_id: replica,
            }],
        )
        .await
        .unwrap();
    let error = driver
        .publish(&mut catalog, &mut effects, cluster, &build, true, None)
        .await
        .unwrap_err();
    assert!(error.is::<super::super::ReplicaRemoved>(), "{error:#}");
    std::process::exit(0);
}
