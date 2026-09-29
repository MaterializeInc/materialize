// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Native catalog metadata contracts. These tests do not exercise expression
//! bytes or controller writer ordering.

use std::collections::{BTreeMap, BTreeSet};

use mz_controller_types::ReplicaId;
use mz_persist_client::PersistClient;
use mz_repr::GlobalId;
use uuid::Uuid;

use super::{DropObjectInfo, ReplicaCreateDropReason};
use crate::catalog::{Catalog, CatalogError, Op};
use crate::durable::objects::ReplicaPlanOwner;
use crate::memory::error::{Error, ErrorKind};

async fn protected_catalog() -> (Catalog, PersistClient, Uuid) {
    let persist = PersistClient::new_for_tests().await;
    let organization = Uuid::new_v4();
    let catalog = open_protected_catalog(persist.clone(), organization).await;
    (catalog, persist, organization)
}

async fn open_protected_catalog(persist: PersistClient, organization: Uuid) -> Catalog {
    open_protected_catalog_with_version(persist, organization, &mz_build_info::DUMMY_BUILD_INFO)
        .await
}

async fn open_protected_catalog_with_version(
    persist: PersistClient,
    organization: Uuid,
    build_info: &'static mz_build_info::BuildInfo,
) -> Catalog {
    let bootstrap = crate::catalog::test_bootstrap_args();
    let storage = crate::durable::TestCatalogStateBuilder::new(persist.clone())
        .with_version(build_info.semver_version())
        .with_organization_id(organization)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(mz_ore::now::SYSTEM_TIME().into(), &bootstrap)
        .await
        .expect("open durable catalog");
    open_protected_catalog_with_storage(persist, organization, storage, build_info).await
}

async fn open_protected_catalog_with_storage(
    persist: PersistClient,
    organization: Uuid,
    storage: Box<dyn crate::durable::DurableCatalogState>,
    build_info: &'static mz_build_info::BuildInfo,
) -> Catalog {
    let bootstrap = crate::catalog::test_bootstrap_args();
    Catalog::open_debug_catalog_inner(
        persist.clone(),
        storage,
        mz_ore::now::SYSTEM_TIME.clone(),
        Some(
            format!("local-az1-{organization}-0")
                .parse()
                .expect("environment ID"),
        ),
        build_info,
        BTreeMap::from([("enable_catalog_read_protection".into(), "true".into())]),
        &bootstrap,
        None,
        None,
    )
    .await
    .expect("open protected catalog")
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
#[cfg_attr(miri, ignore)]
async fn upgrade_preserves_migration_marker_contract() {
    use std::sync::Arc;

    for post_planning in [false, true] {
        let (catalog, persist, organization) = protected_catalog().await;
        let mut config = Catalog::diagnostic_state_config(&catalog.diagnostic_config);
        config.skip_migrations = false;
        catalog.expire().await;
        let mut peer = crate::durable::TestCatalogStateBuilder::new(persist.clone())
            .with_organization_id(organization)
            .with_default_deploy_generation()
            .unwrap_build()
            .await
            .join()
            .await
            .expect("join bootstrap peer");
        peer.sync_to_current_updates()
            .await
            .expect("consume snapshot");
        let old_version = semver::Version::parse("0.0.0-dev.0").expect("valid prior version");
        let mut tx = peer.transaction().await.expect("prepare upgrade");
        crate::catalog::migrate::set_migration_version(&mut tx, old_version.clone())
            .expect("set prior migration marker");
        tx.upsert_system_config(
            "force_source_table_syntax",
            if post_planning { "on" } else { "off" }.into(),
        )
        .expect("configure post-planning migration");
        let _ = tx.get_and_commit_op_updates();
        let ts = tx.upper();
        tx.commit(ts).await.expect("commit upgrade fixture");
        let peer = Arc::new(tokio::sync::Mutex::new(peer));
        let observed = Arc::new(std::sync::Mutex::new(None));
        let point = format!("catalog_initialize_before_replay_{organization}");
        let handle = tokio::runtime::Handle::current();
        fail::cfg_callback(&point, {
            let peer = Arc::clone(&peer);
            let observed = Arc::clone(&observed);
            move || {
                *observed.lock().expect("observation mutex") =
                    Some(tokio::task::block_in_place(|| {
                        handle.block_on(async {
                            let mut peer = peer.lock().await;
                            peer.sync_to_current_updates()
                                .await
                                .expect("refresh migration observer");
                            let tx = peer.transaction().await.expect("read uncommitted marker");
                            crate::catalog::migrate::get_migration_version(&tx)
                        })
                    }));
            }
        })
        .expect("install migration rendezvous");
        let mut storage = crate::durable::TestCatalogStateBuilder::new(persist)
            .with_organization_id(organization)
            .with_default_deploy_generation()
            .unwrap_build()
            .await
            .open(
                mz_ore::now::SYSTEM_TIME().into(),
                &crate::catalog::test_bootstrap_args(),
            )
            .await
            .expect("open migration catalog");
        let result = Catalog::initialize_state(config, &mut storage)
            .await
            .expect("initialize migrated catalog");
        fail::remove(&point);
        if post_planning {
            assert_eq!(
                *observed.lock().expect("observation mutex"),
                Some(Some(old_version.clone()))
            );
        }
        assert_eq!(result.last_seen_version, Some(old_version));
        assert_eq!(
            result.state.system_config().force_source_table_syntax(),
            post_planning
        );
        {
            let mut peer = peer.lock().await;
            peer.sync_to_current_updates()
                .await
                .expect("refresh committed migration");
            let tx = peer.transaction().await.expect("read committed marker");
            assert_eq!(
                crate::catalog::migrate::get_migration_version(&tx),
                Some(mz_build_info::DUMMY_BUILD_INFO.semver_version())
            );
        }
        drop(result);
        storage.expire().await;
        Arc::try_unwrap(peer)
            .expect("rendezvous released peer")
            .into_inner()
            .expire()
            .await;
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn catalog_positions_use_actual_commit_upper() {
    use std::sync::{Arc, Mutex};

    use mz_repr::Timestamp;
    use mz_sql::session::vars::OwnedVarInput;
    use mz_timestamp_oracle::{TimestampOracle, WriteTimestamp};

    #[derive(Debug, Default)]
    struct TestOracle(Mutex<(Timestamp, Timestamp)>);

    #[async_trait::async_trait]
    impl TimestampOracle<Timestamp> for TestOracle {
        async fn write_ts(&self) -> WriteTimestamp {
            let mut times = self.0.lock().expect("oracle lock");
            times.1 = times.1.step_forward();
            WriteTimestamp {
                timestamp: times.1,
                advance_to: times.1.step_forward(),
            }
        }

        async fn peek_write_ts(&self) -> Timestamp {
            self.0.lock().expect("oracle lock").1
        }

        async fn read_ts(&self) -> Timestamp {
            self.0.lock().expect("oracle lock").0
        }

        async fn apply_write(&self, timestamp: Timestamp) {
            let mut times = self.0.lock().expect("oracle lock");
            times.0 = times.0.max(timestamp);
            times.1 = times.1.max(timestamp);
        }
    }

    let persist = PersistClient::new_for_tests().await;
    let organization = Uuid::new_v4();
    let bootstrap = crate::catalog::test_bootstrap_args();
    let oracle = Arc::new(TestOracle::default());
    let storage = crate::durable::TestCatalogStateBuilder::new(persist.clone())
        .with_organization_id(organization)
        .with_default_deploy_generation()
        .with_timestamp_oracle(crate::durable::CatalogTimestampOracle::new(
            Arc::<TestOracle>::clone(&oracle),
            mz_ore::now::SYSTEM_TIME.clone(),
        ))
        .unwrap_build()
        .await
        .open(mz_ore::now::SYSTEM_TIME().into(), &bootstrap)
        .await
        .expect("open oracle-backed catalog");
    let shard_id = storage.shard_id();
    let mut catalog = open_protected_catalog_with_storage(
        persist.clone(),
        organization,
        storage,
        &mz_build_info::DUMMY_BUILD_INFO,
    )
    .await;
    let initial = catalog.observed_position().expect("durable bootstrap");
    assert_eq!(initial.shard_id, shard_id);
    assert_eq!(initial.deployment_generation, 0);
    assert_eq!(initial.upper, catalog.current_upper().await);
    assert_eq!(catalog.planning_position(), Some(initial));
    let snapshot = catalog.clone();

    let storage = crate::durable::TestCatalogStateBuilder::new(persist)
        .with_organization_id(organization)
        .unwrap_build()
        .await
        .open_read_only(&bootstrap)
        .await
        .expect("physical reader");
    let mut opened = Catalog::open_committed(
        Catalog::diagnostic_state_config(&catalog.diagnostic_config),
        storage,
    )
    .await
    .expect("reconstruct committed prefix");
    assert_eq!(opened.catalog.observed_position(), Some(initial));
    assert_eq!(opened.catalog.planning_position(), Some(initial));

    let candidate = catalog.current_upper().await;
    oracle.apply_write(candidate.saturating_add(100)).await;
    let result = catalog
        .transact(
            None,
            candidate,
            None,
            vec![Op::UpdateSystemConfiguration {
                name: "max_connections".into(),
                value: OwnedVarInput::Flat(
                    (catalog.system_config().max_connections() + 1).to_string(),
                ),
            }],
        )
        .await
        .expect("commit at a freshly allocated timestamp");
    let actual_upper = catalog.current_upper().await;
    assert!(actual_upper > candidate.step_forward());
    assert!(!result.catalog_updates.is_empty());
    assert!(
        result
            .catalog_updates
            .iter()
            .all(|update| update.ts.step_forward() == actual_upper)
    );
    let position = catalog.planning_position().expect("durable planning state");
    assert_eq!(position.upper, actual_upper);
    assert_eq!(catalog.observed_position(), Some(position));
    assert_eq!(snapshot.planning_position(), Some(initial));
    assert_eq!(snapshot.observed_position(), Some(initial));

    let progress = actual_upper.saturating_add(100);
    catalog
        .upper_handle()
        .advance_upper(progress)
        .await
        .expect("trailing empty progress");
    opened
        .catalog
        .sync_to_current_updates()
        .await
        .expect("apply planning change and trailing progress together");
    assert_eq!(
        opened
            .catalog
            .observed_position()
            .expect("applied prefix")
            .upper,
        progress
    );
    assert_eq!(opened.catalog.planning_position(), Some(position));

    let first_value = catalog.system_config().max_connections() + 1;
    let mut committed = Vec::new();
    for value in [first_value, first_value + 1] {
        let ts = catalog.current_upper().await;
        catalog
            .transact(
                None,
                ts,
                None,
                vec![Op::UpdateSystemConfiguration {
                    name: "max_connections".into(),
                    value: OwnedVarInput::Flat(value.to_string()),
                }],
            )
            .await
            .expect("commit planning context");
        committed.push(catalog.planning_position().expect("committed position"));
    }
    // A durable handle may have consumed beyond the requested prefix. Keeping
    // that suffix buffered must not certify it as applied to this projection.
    let error = opened
        .catalog
        .storage()
        .await
        .ensure_not_out_of_sync(committed[1].upper)
        .await
        .expect_err("two planning updates remain unapplied");
    assert!(matches!(
        error,
        crate::durable::CatalogError::Durable(
            crate::durable::DurableCatalogError::CatalogOutOfSync { .. }
        )
    ));
    opened
        .catalog
        .sync_updates_through(committed[0].upper)
        .await
        .expect("apply only the requested prefix");
    assert_eq!(opened.catalog.observed_position(), Some(committed[0]));
    assert_eq!(opened.catalog.planning_position(), Some(committed[0]));
    assert_eq!(
        opened.catalog.system_config().max_connections(),
        first_value
    );
    opened
        .catalog
        .sync_updates_through(committed[1].upper)
        .await
        .expect("apply the buffered suffix");
    assert_eq!(opened.catalog.observed_position(), Some(committed[1]));
    assert_eq!(
        opened.catalog.system_config().max_connections(),
        first_value + 1
    );
    opened.catalog.expire().await;
    drop(snapshot);
    catalog.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn joined_prewarming_serving_open_replays_without_shared_writes() {
    // These configured versions share this binary's schema and builtins. Semantic
    // compatibility of real images is the provisioner's contract, not this test's.
    let source_version = Catalog::latest_builtin_schema_migration_version();
    let mut patch_version = source_version.clone();
    patch_version.patch += 1;
    patch_version.pre = semver::Prerelease::EMPTY;
    let mut prerelease_version = patch_version.clone();
    prerelease_version.pre = semver::Prerelease::new("dev.1").expect("valid prerelease");
    // Catalog configuration requires static build info. Only these three test
    // fixtures are retained for the process lifetime.
    let build_info = |version: semver::Version| -> &'static mz_build_info::BuildInfo {
        Box::leak(Box::new(mz_build_info::BuildInfo {
            version: Box::leak(version.to_string().into_boxed_str()),
            ..mz_build_info::DUMMY_BUILD_INFO
        }))
    };
    let source_build = build_info(source_version.clone());
    let patch_build = build_info(patch_version);
    let prerelease_build = build_info(prerelease_version);
    for (build_info, cache_override) in [
        (source_build, None),
        (source_build, Some(false)),
        (patch_build, None),
        (prerelease_build, Some(false)),
    ] {
        let mut cache = mz_persist_client::cache::PersistClientCache::new_no_metrics();
        cache.cfg.build_version = source_version.clone();
        let persist = cache
            .open(mz_persist_client::PersistLocation::new_in_mem())
            .await
            .expect("open source-version Persist client");
        let organization = Uuid::new_v4();
        let active =
            open_protected_catalog_with_version(persist.clone(), organization, source_build).await;
        let bootstrap = crate::catalog::test_bootstrap_args();
        let active_generation = active.state().deployment_generation();
        let pending_generation = active_generation + 1;
        let build = Catalog::expression_build_version(build_info).to_string();
        let storage = crate::durable::TestCatalogStateBuilder::new(persist.clone())
            .with_version(build_info.semver_version())
            .with_organization_id(organization)
            .with_deploy_generation(pending_generation)
            .unwrap_build()
            .await
            .join_prewarming(&build)
            .await
            .expect("join same-schema prewarming deployment");
        assert!(!storage.is_read_only());
        assert!(!storage.is_savepoint());
        let shard_id = storage.shard_id();
        // Leave the joined handle's initial updates for the serving entrypoint.
        let before = active.storage().await.snapshot().await.expect("snapshot");
        let upper = active.current_upper().await;
        {
            let mut storage = active.storage().await;
            storage
                .sync_to_current_updates()
                .await
                .expect("refresh active");
            let tx = storage.transaction().await.expect("read migration marker");
            assert_eq!(
                crate::catalog::migrate::get_migration_version(&tx),
                Some(source_version.clone())
            );
        }
        let replica_storage = crate::durable::TestCatalogStateBuilder::new(persist.clone())
            .with_version(build_info.semver_version())
            .with_organization_id(organization)
            .unwrap_build()
            .await
            .open_read_only(&bootstrap)
            .await
            .expect("open committed replica reader");
        let mut replica_state = Catalog::diagnostic_state_config(&active.diagnostic_config);
        replica_state.build_info = build_info;
        let replica = Catalog::open_committed(replica_state, replica_storage)
            .await
            .expect("reconstruct committed replica with compatible builtins")
            .catalog;
        assert_eq!(
            replica.observed_position().expect("replica position").upper,
            upper
        );
        assert_eq!(
            active.storage().await.snapshot().await.expect("snapshot"),
            before
        );
        assert_eq!(active.current_upper().await, upper);
        replica.expire().await;
        let mut state = Catalog::diagnostic_state_config(&active.diagnostic_config);
        state.build_info = build_info;
        state.read_only = true;
        // The serving entrypoint must choose safe replay, not the caller.
        state.skip_migrations = false;
        state.builtin_item_migration_config.read_only = true;
        state.enable_expression_cache_override = cache_override;
        let opened = Catalog::open(crate::config::Config {
            storage,
            metrics_registry: &mz_ore::metrics::MetricsRegistry::new(),
            state,
        })
        .await
        .expect("open output-readonly joined serving catalog");
        let pending = opened.catalog;
        assert_eq!(
            active.storage().await.snapshot().await.expect("snapshot"),
            before,
            "serving replay must not change shared durable definitions"
        );
        assert_eq!(active.current_upper().await, upper);
        assert_eq!(pending.state().deployment_generation(), pending_generation);
        assert_eq!(
            pending
                .storage()
                .await
                .get_deployment_generation()
                .await
                .expect("joined identity"),
            pending_generation
        );
        assert_eq!(
            pending.state().active_deployment_generation(),
            Some(active_generation)
        );
        let position = pending
            .observed_position()
            .expect("joined catalog position");
        assert_eq!(position.shard_id, shard_id);
        assert_eq!(position.deployment_generation, pending_generation);
        assert_eq!(position.upper, upper);
        assert_eq!(pending.planning_position(), Some(position));
        assert!(pending.state().catalog_read_protection_enabled());
        assert_eq!(
            pending
                .entries()
                .map(|entry| (entry.id(), entry.name().clone()))
                .collect::<BTreeMap<_, _>>(),
            active
                .entries()
                .map(|entry| (entry.id(), entry.name().clone()))
                .collect::<BTreeMap<_, _>>(),
            "replay retains committed catalog definitions"
        );
        assert!(
            !opened.builtin_table_updates.is_empty(),
            "serving open retains bootstrap rows for its output collections"
        );
        assert!(
            pending
                .read_written_plans(vec![(GlobalId::Transient(1_000_000), Uuid::new_v4())])
                .await
                .expect("serving plan store is open even with the cache disabled")
                .is_empty()
        );
        pending.expire().await;
        {
            let mut storage = active.storage().await;
            let tx = storage
                .transaction()
                .await
                .expect("read migration marker after replay");
            assert_eq!(
                crate::catalog::migrate::get_migration_version(&tx),
                Some(source_version.clone())
            );
        }
        let mut state = Catalog::diagnostic_state_config(&active.diagnostic_config);
        state.build_info = build_info;
        state.read_only = false;
        state.skip_migrations = false;
        state.builtin_item_migration_config.read_only = false;
        active.expire().await;
        let storage = crate::durable::TestCatalogStateBuilder::new(persist)
            .with_version(build_info.semver_version())
            .with_organization_id(organization)
            .with_deploy_generation(pending_generation)
            .unwrap_build()
            .await
            .open(mz_ore::now::SYSTEM_TIME().into(), &bootstrap)
            .await
            .expect("authorize writable bootstrap");
        let bootstrapped = Catalog::open(crate::config::Config {
            storage,
            metrics_registry: &mz_ore::metrics::MetricsRegistry::new(),
            state,
        })
        .await
        .expect("bootstrap with target version")
        .catalog;
        {
            let mut storage = bootstrapped.storage().await;
            let tx = storage
                .transaction()
                .await
                .expect("read bootstrap migration marker");
            assert_eq!(
                crate::catalog::migrate::get_migration_version(&tx),
                Some(build_info.semver_version())
            );
        }
        assert!(bootstrapped.current_upper().await > upper);
        bootstrapped.expire().await;
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn committed_replay_rejects_builtin_fingerprint_mismatch() {
    let (catalog, persist, organization) = protected_catalog().await;
    {
        let mut storage = catalog.storage().await;
        let mut tx = storage
            .transaction()
            .await
            .expect("change builtin fingerprint");
        let mut mapping = tx
            .get_system_object_mappings()
            .next()
            .expect("builtin mapping");
        mapping
            .unique_identifier
            .fingerprint
            .push_str("-incompatible");
        tx.set_system_object_mappings(vec![mapping])
            .expect("update fingerprint");
        let _ = tx.get_and_commit_op_updates();
        let ts = tx.upper();
        tx.commit(ts).await.expect("commit mismatched fingerprint");
    }
    let before = catalog.storage().await.snapshot().await.expect("snapshot");
    let upper = catalog.current_upper().await;
    let storage = crate::durable::TestCatalogStateBuilder::new(persist)
        .with_organization_id(organization)
        .unwrap_build()
        .await
        .open_read_only(&crate::catalog::test_bootstrap_args())
        .await
        .expect("open committed reader");
    let result = Catalog::open_committed(
        Catalog::diagnostic_state_config(&catalog.diagnostic_config),
        storage,
    )
    .await;
    assert!(matches!(result, Err(CatalogError::Internal(message))
        if message == "catalog reconstruction requires a builtin schema migration"));
    assert_eq!(
        catalog.storage().await.snapshot().await.expect("snapshot"),
        before
    );
    assert_eq!(catalog.current_upper().await, upper);
    catalog.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn savepoint_catalog_has_no_position_certificate() {
    let (catalog, persist, organization) = protected_catalog().await;
    let bootstrap = crate::catalog::test_bootstrap_args();
    let storage = crate::durable::TestCatalogStateBuilder::new(persist.clone())
        .with_organization_id(organization)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open_savepoint(mz_ore::now::SYSTEM_TIME().into(), &bootstrap)
        .await
        .expect("open savepoint");
    let mut state = Catalog::diagnostic_state_config(&catalog.diagnostic_config);
    state.read_only = true;
    state.builtin_item_migration_config.read_only = true;
    let mut savepoint = Catalog::open(crate::config::Config {
        storage,
        metrics_registry: &mz_ore::metrics::MetricsRegistry::new(),
        state,
    })
    .await
    .expect("open read-only serving catalog")
    .catalog;
    assert_eq!(savepoint.planning_position(), None);
    assert_eq!(savepoint.observed_position(), None);
    let ts = savepoint.current_upper().await;
    savepoint
        .transact(
            None,
            ts,
            None,
            vec![Op::UpdateSystemConfiguration {
                name: "max_connections".into(),
                value: mz_sql::session::vars::OwnedVarInput::Flat(
                    (savepoint.system_config().max_connections() + 1).to_string(),
                ),
            }],
        )
        .await
        .expect("local planning change");
    savepoint
        .sync_to_current_updates()
        .await
        .expect("local sync");
    assert_eq!(savepoint.planning_position(), None);
    assert_eq!(savepoint.observed_position(), None);
    savepoint.expire().await;
    catalog.expire().await;
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
#[cfg_attr(miri, ignore)]
async fn protected_bootstrap_absorbs_publication() {
    use std::sync::{Arc, Mutex};

    for (phase, upgrade) in [
        ("before_transaction", false),
        ("before_commit", false),
        ("before_commit", true),
        ("before_replay", false),
        ("before_replay", true),
    ] {
        let (catalog, persist, organization) = protected_catalog().await;
        catalog.expire().await;
        let mut peer = crate::durable::TestCatalogStateBuilder::new(persist.clone())
            .with_organization_id(organization)
            .with_default_deploy_generation()
            .unwrap_build()
            .await
            .join()
            .await
            .expect("join bootstrap peer");
        peer.sync_to_current_updates()
            .await
            .expect("consume peer snapshot");
        let retired_index = if phase == "before_commit" {
            use crate::durable::objects::{
                SystemObjectDescription, SystemObjectMapping, SystemObjectUniqueIdentifier,
            };
            let mut tx = peer.transaction().await.expect("prepare retired builtin");
            let (catalog_id, global_id) = tx
                .allocate_system_item_ids(1)
                .expect("allocate builtin identity")[0];
            tx.set_system_object_mappings(vec![SystemObjectMapping {
                description: SystemObjectDescription {
                    schema_name: "mz_internal".into(),
                    object_type: mz_sql::catalog::CatalogItemType::Index,
                    object_name: "mz_bootstrap_retired_test_index".into(),
                },
                unique_identifier: SystemObjectUniqueIdentifier {
                    catalog_id,
                    global_id,
                    fingerprint: "retired test index".into(),
                },
            }])
            .expect("persist retired builtin mapping");
            tx.set_collection_compaction_bound(global_id, Some(mz_repr::Timestamp::MIN))
                .expect("publish builtin bound");
            let _ = tx.get_and_commit_op_updates();
            let ts = tx.upper();
            tx.commit(ts).await.expect("commit retired builtin fixture");
            Some(global_id)
        } else {
            None
        };
        if upgrade {
            // Exercise version-changing bootstrap with compatible builtin schemas.
            let mut tx = peer.transaction().await.expect("prepare migration marker");
            crate::catalog::migrate::set_migration_version(
                &mut tx,
                semver::Version::parse("0.0.0-dev.0").expect("older migration version"),
            )
            .expect("set migration marker");
            let _ = tx.get_and_commit_op_updates();
            let ts = tx.upper();
            tx.commit(ts).await.expect("commit migration marker");
        }
        let peer = Arc::new(tokio::sync::Mutex::new(peer));
        let publication = Arc::new(Mutex::new(None));
        let point = format!("catalog_initialize_{phase}_{organization}");
        let handle = tokio::runtime::Handle::current();
        fail::cfg_callback(&point, {
            let peer = Arc::clone(&peer);
            let publication = Arc::clone(&publication);
            move || {
                let mut publication = publication.lock().expect("publication mutex");
                if publication.is_some() {
                    return;
                }
                *publication = Some(tokio::task::block_in_place(|| {
                    handle.block_on(async {
                        let mut peer = peer.lock().await;
                        peer.sync_to_current_updates().await.expect("refresh peer");
                        let mut tx = peer.transaction().await.expect("start peer publication");
                        let incarnation =
                            tx.create_client_incarnation(None).expect("register peer");
                        let _ = tx.get_and_commit_op_updates();
                        let ts = tx.upper();
                        tx.commit(ts).await.expect("publish during bootstrap");
                        (incarnation, peer.current_upper().await)
                    })
                }));
            }
        })
        .expect("install bootstrap rendezvous");
        let mut catalog = open_protected_catalog(persist, organization).await;
        fail::remove(&point);
        let (incarnation, upper) = publication
            .lock()
            .expect("publication mutex")
            .expect("bootstrap crossed rendezvous");
        if phase == "before_replay" {
            assert!(catalog.observed_position().expect("captured prefix").upper < upper);
            assert_eq!(
                catalog.current_upper().await,
                upper,
                "heavy read-only reconstruction must not commit after publication"
            );
            assert!(
                !catalog
                    .state()
                    .client_incarnations()
                    .contains_key(&incarnation)
            );
        } else {
            assert!(
                catalog
                    .observed_position()
                    .expect("applied publication")
                    .upper
                    >= upper
            );
            assert!(
                catalog
                    .state()
                    .client_incarnations()
                    .contains_key(&incarnation)
            );
        }
        let planning_position = catalog.planning_position();
        assert_eq!(planning_position, catalog.observed_position());
        catalog
            .sync_to_current_updates()
            .await
            .expect("apply queued publication");
        assert!(catalog.observed_position().expect("applied prefix").upper >= upper);
        assert_eq!(catalog.planning_position(), planning_position);
        assert!(
            catalog
                .state()
                .client_incarnations()
                .contains_key(&incarnation)
        );
        catalog
            .check_consistency()
            .expect("consistent reconstructed catalog");
        if let Some(id) = retired_index {
            assert!(
                !catalog
                    .state()
                    .collection_compaction_bounds()
                    .contains_key(&id),
                "retired builtin bound must be removed with its identity"
            );
        }
        catalog.expire().await;
        Arc::try_unwrap(peer)
            .expect("rendezvous released peer")
            .into_inner()
            .expire()
            .await;
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn wal_identity_read_preserves_pending_replica_updates() {
    let (mut catalog, persist, organization) = protected_catalog().await;
    let shard = catalog.txn_wal_shard().await.expect("initialized WAL");

    let mut peer = crate::durable::TestCatalogStateBuilder::new(persist)
        .with_organization_id(organization)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .join()
        .await
        .expect("join peer writer");
    peer.sync_to_current_updates()
        .await
        .expect("consume peer snapshot");
    let incarnation = {
        let mut tx = peer.transaction().await.expect("start peer publication");
        let incarnation = tx.create_client_incarnation(None).expect("register peer");
        let _ = tx.get_and_commit_op_updates();
        let ts = tx.upper();
        tx.commit(ts).await.expect("commit peer publication");
        incarnation
    };
    assert_eq!(
        catalog
            .txn_wal_shard()
            .await
            .expect("read WAL despite pending metadata"),
        shard
    );
    assert!(
        !catalog
            .state()
            .client_incarnations()
            .contains_key(&incarnation)
    );
    catalog
        .sync_to_current_updates()
        .await
        .expect("apply pending replica metadata");
    assert!(
        catalog
            .state()
            .client_incarnations()
            .contains_key(&incarnation)
    );
    catalog.expire().await;
    peer.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn replica_plan_dry_run_returns_contention_for_refresh() {
    const CHILD: &str = "MZ_CATALOG_DRY_RUN_CONTENTION_CHILD";
    const DONE: &str = "dry-run contention refreshed successfully";
    if std::env::var_os(CHILD).is_none() {
        // A fatal catalog error can exit with status zero. Require completion
        // evidence from the child, not just a successful process exit.
        let output = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            tokio::process::Command::new(std::env::current_exe().expect("test binary"))
                .args([
                    "--exact",
                    "catalog::transact::replica_plan_tests::replica_plan_dry_run_returns_contention_for_refresh",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("bounded dry-run test")
        .expect("run child");
        assert!(
            output.status.success() && String::from_utf8_lossy(&output.stdout).contains(DONE),
            "child did not complete: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        return;
    }
    Box::pin(exercise_dry_run_contention()).await;
    println!("{DONE}");
}

async fn exercise_dry_run_contention() {
    let (mut catalog, persist, organization) = Box::pin(protected_catalog()).await;
    let cluster = catalog.user_clusters().next().expect("bootstrap cluster");
    let replica = cluster
        .replicas()
        .next()
        .expect("bootstrap replica")
        .replica_id;
    let id = GlobalId::Transient(1_000_010);
    let op = select(
        id,
        &Catalog::expression_build_version(catalog.config().build_info).to_string(),
        None,
        Uuid::new_v4(),
        Some(ReplicaPlanOwner {
            replica_id: replica,
            deployment_generation: catalog.state().deployment_generation(),
            name: "contention_metric".into(),
        }),
        cluster.log_indexes.values().copied().collect(),
    );
    let storage = crate::durable::TestCatalogStateBuilder::new(persist.clone())
        .with_organization_id(organization)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .join()
        .await
        .expect("join peer");
    let state = catalog.replica_config().into_state(
        catalog.config().build_info,
        catalog.config().environment_id.clone(),
        catalog.config().connection_context.clone(),
        persist,
    );
    let mut peer = Box::pin(Catalog::open_committed(state, storage))
        .await
        .expect("open peer")
        .catalog;
    let revision = catalog.transient_revision();
    peer.transact(
        None,
        peer.current_upper().await,
        None,
        vec![Op::CreateClientIncarnation {
            replica_id: Some(replica),
        }],
    )
    .await
    .expect("peer metadata publication");
    let error = catalog
        .transact_incremental_dry_run(
            catalog.state(),
            vec![op.clone()],
            None,
            None,
            catalog.current_upper().await,
        )
        .await
        .expect_err("stale dry run must ask the caller to refresh");
    assert!(matches!(error, CatalogError::Catalog(error) if matches!(
        error.kind, ErrorKind::Durable(crate::durable::DurableCatalogError::CatalogOutOfSync { .. })
    )));
    catalog
        .sync_to_current_updates()
        .await
        .expect("refresh peer metadata");
    assert_eq!(
        catalog.transient_revision(),
        revision,
        "metadata does not invalidate planning"
    );
    let (candidate, _) = catalog
        .transact_incremental_dry_run(
            catalog.state(),
            vec![op],
            None,
            None,
            catalog.current_upper().await,
        )
        .await
        .expect("revalidated dry run");
    assert!(
        candidate
            .written_plans()
            .keys()
            .any(|(selected, _)| *selected == id)
    );
}

fn select(
    id: GlobalId,
    build: &str,
    expected_revision: Option<Uuid>,
    revision: Uuid,
    replica_owner: Option<ReplicaPlanOwner>,
    imports: BTreeSet<GlobalId>,
) -> Op {
    Op::SetWrittenPlan {
        id,
        build_version: build.into(),
        expected_revision,
        revision: Some(revision),
        imports,
        replica_owner,
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn staged_materialized_view_rejects_renamed_replica_binding() {
    use mz_sql::DEFAULT_SCHEMA;
    use mz_sql::catalog::CatalogDatabase;
    use mz_sql::names::{ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier};
    use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
    use mz_sql::session::vars::DEFAULT_DATABASE_NAME;
    use mz_sql_parser::ast::{Ident, QualifiedReplica};

    use crate::SYSTEM_CONN_ID;
    use crate::catalog::state::LocalExpressionCache;
    use crate::memory::objects::{CatalogItem, ClusterConfig, ClusterVariant};

    let (mut catalog, persist, organization) = protected_catalog().await;
    let replica_config = catalog
        .user_cluster_replicas()
        .next()
        .expect("bootstrap replica")
        .config
        .clone();
    let cluster_id = catalog
        .allocate_user_cluster_id(catalog.current_upper().await)
        .await
        .expect("allocate cluster");
    catalog
        .transact(
            None,
            catalog.current_upper().await,
            None,
            vec![Op::CreateCluster {
                id: cluster_id,
                name: "pin_race".into(),
                introspection_sources: crate::builtin::BUILTINS::logs().collect(),
                owner_id: MZ_SYSTEM_ROLE_ID,
                config: ClusterConfig {
                    variant: ClusterVariant::Unmanaged,
                    workload_class: None,
                },
            }],
        )
        .await
        .expect("create unmanaged cluster");
    let replica_ids = catalog
        .allocate_user_replica_ids(2, catalog.current_upper().await)
        .await
        .expect("allocate replica identities");
    let create_replica = |replica_id| Op::CreateClusterReplica {
        cluster_id,
        replica_id,
        name: "r1".into(),
        config: replica_config.clone(),
        owner_id: MZ_SYSTEM_ROLE_ID,
        reason: ReplicaCreateDropReason::Manual,
    };
    catalog
        .transact(
            None,
            catalog.current_upper().await,
            None,
            vec![create_replica(replica_ids[0])],
        )
        .await
        .expect("create original declaration");
    let target = replica_ids[0];
    let database = catalog
        .resolve_database(DEFAULT_DATABASE_NAME)
        .expect("default database");
    let database_spec = ResolvedDatabaseSpecifier::Id(database.id());
    let schema = catalog
        .resolve_schema_in_database(&database_spec, DEFAULT_SCHEMA, &SYSTEM_CONN_ID)
        .expect("default schema");
    let qualifiers = ItemQualifiers {
        database_spec,
        schema_spec: schema.id.clone(),
    };
    let prefix = format!("{}.{}", database.name, schema.name.schema);
    let plan_mv = |catalog: &Catalog, id, global_id, name: &str, replica: &str| {
        let sql = format!(
            "CREATE MATERIALIZED VIEW {prefix}.{name} IN CLUSTER pin_race REPLICA {replica} AS SELECT 1 AS a AS OF 0"
        );
        let item = catalog
            .state()
            .clone()
            .with_enable_for_item_parsing(|state| {
                state.parse_item(
                    global_id,
                    &sql,
                    &BTreeMap::new(),
                    None,
                    false,
                    None,
                    &mut LocalExpressionCache::Closed,
                    None,
                )
            })
            .expect("plan pinned MV");
        let CatalogItem::MaterializedView(mv) = &item else {
            panic!("expected MV");
        };
        assert_eq!(mv.target_replica, Some(target));
        Op::CreateItem {
            id,
            name: QualifiedItemName {
                qualifiers: qualifiers.clone(),
                item: name.into(),
            },
            item,
            owner_id: MZ_SYSTEM_ROLE_ID,
        }
    };
    let (existing_id, existing_gid) = catalog
        .allocate_user_id_for_test()
        .await
        .expect("existing MV identity");
    let existing = plan_mv(&catalog, existing_id, existing_gid, "existing_pin", "r1");
    catalog
        .transact(None, catalog.current_upper().await, None, vec![existing])
        .await
        .expect("commit existing pinned MV");
    let existing_shard = catalog.state().storage_metadata().collection_metadata[&existing_gid];
    catalog
        .transact(
            None,
            catalog.current_upper().await,
            None,
            vec![Op::DropClusterReplicaRealization {
                cluster_id,
                replica_id: replica_ids[0],
            }],
        )
        .await
        .expect("retire execution without dropping shared intent");
    assert!(catalog.try_get_entry(&existing_id).is_some());
    assert_eq!(
        catalog
            .state()
            .resolve_materialized_view_replica(cluster_id, "r1"),
        Ok(target)
    );
    assert_eq!(
        catalog
            .state()
            .physical_replica_for_target(cluster_id, target),
        None
    );
    catalog
        .transact(
            None,
            catalog.current_upper().await,
            None,
            vec![Op::CreateClusterReplicaRealization {
                cluster_id,
                replica_id: replica_ids[0],
                name: "r1".into(),
                config: replica_config.clone(),
                owner_id: MZ_SYSTEM_ROLE_ID,
                carryover_from: None,
            }],
        )
        .await
        .expect("reconstruct execution from the declaration");
    assert_eq!(
        catalog
            .state()
            .physical_replica_for_target(cluster_id, target),
        Some(replica_ids[0])
    );
    let (stale_id, stale_gid) = catalog
        .allocate_user_id_for_test()
        .await
        .expect("staged MV identity");
    // This item is off-thread planning work, not a committed bound object that
    // the rename transaction can rewrite.
    let staged = plan_mv(&catalog, stale_id, stale_gid, "stale_pin", "r1");
    catalog
        .transact(
            None,
            catalog.current_upper().await,
            None,
            vec![Op::RenameClusterReplica {
                cluster_id,
                replica_id: replica_ids[0],
                name: QualifiedReplica {
                    cluster: Ident::new_unchecked("pin_race"),
                    replica: Ident::new_unchecked("r1"),
                },
                to_name: "r2".into(),
            }],
        )
        .await
        .expect("rename original declaration and committed MV reference");

    for reuse_name in [false, true] {
        if reuse_name {
            catalog
                .transact(
                    None,
                    catalog.current_upper().await,
                    None,
                    vec![create_replica(replica_ids[1])],
                )
                .await
                .expect("reuse r1 for a different declaration");
            assert_eq!(
                catalog
                    .state()
                    .resolve_materialized_view_replica(cluster_id, "r1"),
                Ok(replica_ids[1])
            );
        }
        let result = catalog
            .transact(
                None,
                catalog.current_upper().await,
                None,
                vec![staged.clone()],
            )
            .await;
        assert!(
            matches!(result, Err(CatalogError::DDLTransactionRace)),
            "reuse_name={reuse_name}: {:?}",
            result.err()
        );
        assert!(catalog.state().try_get_entry(&stale_id).is_none());
        assert!(
            !catalog
                .state()
                .storage_metadata()
                .collection_metadata
                .contains_key(&stale_gid)
        );
    }

    let (fresh_id, fresh_gid) = catalog
        .allocate_user_id_for_test()
        .await
        .expect("fresh MV identity");
    let fresh = plan_mv(&catalog, fresh_id, fresh_gid, "fresh_pin", "r2");
    catalog
        .transact(None, catalog.current_upper().await, None, vec![fresh])
        .await
        .expect("fresh plan uses renamed declaration");
    let bootstrap = crate::catalog::test_bootstrap_args();
    let follower = Catalog::open_debug_read_only_catalog(persist, organization, &bootstrap)
        .await
        .expect("reconstruct pinned MVs");
    for state in [catalog.state(), follower.state()] {
        assert!(state.try_get_entry(&stale_id).is_none());
        assert_eq!(
            state.resolve_materialized_view_replica(cluster_id, "r2"),
            Ok(target)
        );
        assert_eq!(
            state.storage_metadata().collection_metadata[&existing_gid],
            existing_shard
        );
        for id in [existing_id, fresh_id] {
            let CatalogItem::MaterializedView(mv) = state.get_entry(&id).item() else {
                panic!("expected committed MV");
            };
            assert_eq!(mv.target_replica, Some(target));
            let mz_sql_parser::ast::Statement::CreateMaterializedView(definition) =
                mz_sql::parse::parse(&mv.create_sql)
                    .expect("persisted MV SQL")
                    .remove(0)
                    .ast
            else {
                panic!("expected MV SQL");
            };
            assert_eq!(
                definition
                    .in_cluster_replica
                    .expect("committed MV retains its explicit replica target")
                    .as_str(),
                "r2"
            );
        }
    }
    follower.expire().await;
    catalog.expire().await;
}

#[mz_ore::test(tokio::test)]
async fn replica_plan_scope_survives_reopen_and_drop_is_build_local() {
    for drop_cluster in [false, true] {
        let (mut catalog, persist, organization) = protected_catalog().await;
        let cluster = catalog
            .user_clusters()
            .next()
            .expect("bootstrap user cluster");
        let cluster_id = cluster.id;
        assert!(
            cluster.bound_objects.is_empty(),
            "drop fixture has no SQL dependents"
        );
        let replica_id = cluster
            .replicas()
            .next()
            .expect("bootstrap replica")
            .replica_id;
        let imports: BTreeSet<_> = cluster.log_indexes.values().copied().collect();
        assert!(!imports.is_empty(), "exercise real cluster log imports");
        let owner = ReplicaPlanOwner {
            replica_id,
            deployment_generation: catalog.state().deployment_generation(),
            name: "observer".into(),
        };
        let other_cluster = catalog
            .clusters()
            .find(|c| c.id != cluster_id)
            .expect("another cluster");
        let other_owner = ReplicaPlanOwner {
            replica_id: other_cluster
                .replicas()
                .next()
                .expect("another replica")
                .replica_id,
            deployment_generation: owner.deployment_generation,
            name: owner.name.clone(),
        };
        let other_imports = other_cluster.log_indexes.values().copied().collect();
        let build =
            Catalog::expression_build_version(catalog.state().config().build_info).to_string();
        let foreign_build = format!("{build}-other");
        let id = GlobalId::Transient(1_000_000);
        let other_id = GlobalId::Transient(1_000_001);
        let revision = Uuid::new_v4();
        let foreign_revision = Uuid::new_v4();
        let other_revision = Uuid::new_v4();
        let ts = catalog.current_upper().await;
        catalog
            .transact(
                None,
                ts,
                None,
                vec![
                    select(
                        id,
                        &build,
                        None,
                        revision,
                        Some(owner.clone()),
                        imports.clone(),
                    ),
                    select(
                        id,
                        &foreign_build,
                        None,
                        foreign_revision,
                        Some(owner.clone()),
                        imports,
                    ),
                    select(
                        other_id,
                        &build,
                        None,
                        other_revision,
                        Some(other_owner.clone()),
                        other_imports,
                    ),
                ],
            )
            .await
            .expect("log-only observers may share names across replicas and builds");

        let bootstrap = crate::catalog::test_bootstrap_args();
        let follower =
            Catalog::open_debug_read_only_catalog(persist.clone(), organization, &bootstrap)
                .await
                .expect("reopen selections");
        for state in [catalog.state(), follower.state()] {
            assert!(state.try_get_entry_by_global_id(&id).is_none());
            assert_eq!(state.written_plan(id, &build), Some(revision));
            assert_eq!(
                state.written_plan(id, &foreign_build),
                Some(foreign_revision)
            );
            assert_eq!(state.written_plan_replica_owner(id, &build), Some(&owner));
            assert_eq!(
                state.written_plan_replica_owner(id, &foreign_build),
                Some(&owner)
            );
        }
        drop(follower);

        // SQL planning expands a cluster drop to its replicas. Keep that contract
        // here rather than constructing a cluster-only drop with dangling replicas.
        let mut objects = vec![DropObjectInfo::ClusterReplica((
            cluster_id,
            replica_id,
            ReplicaCreateDropReason::Manual,
        ))];
        if drop_cluster {
            objects.push(DropObjectInfo::Cluster(cluster_id));
        }
        let ts = catalog.current_upper().await;
        catalog
            .transact(None, ts, None, vec![Op::DropObjects(objects)])
            .await
            .expect("drop observer owner");
        let follower = Catalog::open_debug_read_only_catalog(persist, organization, &bootstrap)
            .await
            .expect("reopen after drop");
        for state in [catalog.state(), follower.state()] {
            assert_eq!(state.written_plan(id, &build), None);
            assert_eq!(state.written_plan_replica_owner(id, &build), None);
            assert_eq!(
                state.written_plan(id, &foreign_build),
                Some(foreign_revision)
            );
            assert_eq!(
                state.written_plan_replica_owner(id, &foreign_build),
                Some(&owner)
            );
            assert_eq!(state.written_plan(other_id, &build), Some(other_revision));
            assert_eq!(
                state.written_plan_replica_owner(other_id, &build),
                Some(&other_owner)
            );
        }
    }
}

#[mz_ore::test(tokio::test)]
async fn replica_plan_selection_rejects_invalid_scope() {
    let (catalog, _, _) = protected_catalog().await;
    let base = catalog.state().clone();
    let cluster = catalog
        .user_clusters()
        .next()
        .expect("bootstrap user cluster");
    let owner = ReplicaPlanOwner {
        replica_id: cluster
            .replicas()
            .next()
            .expect("bootstrap replica")
            .replica_id,
        deployment_generation: catalog.state().deployment_generation(),
        name: "observer".into(),
    };
    let imports: BTreeSet<_> = cluster.log_indexes.values().copied().collect();
    assert!(!imports.is_empty());
    let foreign_log = catalog
        .clusters()
        .filter(|c| c.id != cluster.id)
        .flat_map(|c| c.log_indexes.values())
        .next()
        .copied()
        .expect("foreign cluster log");
    let storage_id = catalog
        .entries()
        .find(|entry| matches!(entry.item(), crate::memory::objects::CatalogItem::Table(_)))
        .expect("catalog table definition")
        .latest_global_id();
    let id = GlobalId::Transient(1_000_000);
    let mut foreign_imports = imports.clone();
    foreign_imports.insert(foreign_log);
    let mut storage_imports = imports.clone();
    storage_imports.insert(storage_id);
    let missing_owner = ReplicaPlanOwner {
        replica_id: ReplicaId::User(u64::MAX),
        ..owner.clone()
    };
    let foreign_generation = ReplicaPlanOwner {
        deployment_generation: owner.deployment_generation + 1,
        ..owner.clone()
    };
    let empty_name = ReplicaPlanOwner {
        name: String::new(),
        ..owner.clone()
    };
    for (label, id, owner, inputs) in [
        (
            "foreign cluster log",
            id,
            Some(owner.clone()),
            foreign_imports,
        ),
        ("storage import", id, Some(owner.clone()), storage_imports),
        ("missing replica", id, Some(missing_owner), imports.clone()),
        (
            "foreign deployment",
            id,
            Some(foreign_generation),
            imports.clone(),
        ),
        ("empty name", id, Some(empty_name), imports.clone()),
        (
            "nontransient ID",
            GlobalId::User(1_000_000),
            Some(owner.clone()),
            imports.clone(),
        ),
        ("SQL-owned ID", storage_id, Some(owner), imports.clone()),
        ("ordinary selection needs a SQL entry", id, None, imports),
    ] {
        let result = catalog
            .transact_incremental_dry_run(
                &base,
                vec![select(
                    id,
                    "test-build",
                    None,
                    Uuid::new_v4(),
                    owner,
                    inputs,
                )],
                None,
                None,
                1.into(),
            )
            .await;
        assert!(
            matches!(result, Err(CatalogError::DDLTransactionRace)),
            "{label}: {result:?}"
        );
    }
}

#[mz_ore::test(tokio::test)]
async fn replica_plan_selection_cas_uniqueness_and_immutable_owner() {
    let (catalog, _, _) = protected_catalog().await;
    let base = catalog.state().clone();
    let cluster = catalog
        .user_clusters()
        .next()
        .expect("bootstrap user cluster");
    let owner = ReplicaPlanOwner {
        replica_id: cluster
            .replicas()
            .next()
            .expect("bootstrap replica")
            .replica_id,
        deployment_generation: catalog.state().deployment_generation(),
        name: "observer".into(),
    };
    let imports: BTreeSet<_> = cluster.log_indexes.values().copied().collect();
    let id = GlobalId::Transient(1_000_000);
    let revision = Uuid::new_v4();
    let (selected, snapshot) = catalog
        .transact_incremental_dry_run(
            &base,
            vec![select(
                id,
                "test-build",
                None,
                revision,
                Some(owner.clone()),
                imports.clone(),
            )],
            None,
            None,
            1.into(),
        )
        .await
        .expect("initial observer selection");

    let stale = catalog
        .transact_incremental_dry_run(
            &selected,
            vec![select(
                id,
                "test-build",
                None,
                Uuid::new_v4(),
                Some(owner.clone()),
                imports.clone(),
            )],
            None,
            Some(snapshot.clone()),
            1.into(),
        )
        .await;
    assert!(
        matches!(stale, Err(CatalogError::DDLTransactionRace)),
        "{stale:?}"
    );

    let renamed = ReplicaPlanOwner {
        name: "renamed".into(),
        ..owner.clone()
    };
    let other_replica = catalog
        .clusters()
        .flat_map(|c| c.replicas())
        .find(|r| r.replica_id != owner.replica_id)
        .expect("another replica")
        .replica_id;
    let reassigned = ReplicaPlanOwner {
        replica_id: other_replica,
        ..owner.clone()
    };
    let redeployed = ReplicaPlanOwner {
        deployment_generation: owner.deployment_generation + 1,
        ..owner.clone()
    };
    for (label, next_id, expected, next_owner) in [
        (
            "competing export for same owner",
            GlobalId::Transient(1_000_001),
            None,
            Some(owner.clone()),
        ),
        ("rename selected owner", id, Some(revision), Some(renamed)),
        (
            "reassign selected owner",
            id,
            Some(revision),
            Some(reassigned),
        ),
        (
            "change owner deployment",
            id,
            Some(revision),
            Some(redeployed),
        ),
        ("erase selected owner", id, Some(revision), None),
    ] {
        let result = catalog
            .transact_incremental_dry_run(
                &selected,
                vec![select(
                    next_id,
                    "test-build",
                    expected,
                    Uuid::new_v4(),
                    next_owner,
                    imports.clone(),
                )],
                None,
                Some(snapshot.clone()),
                1.into(),
            )
            .await;
        assert!(
            matches!(
                result,
                Err(CatalogError::Catalog(Error {
                    kind: ErrorKind::Durable(
                        crate::durable::DurableCatalogError::UniquenessViolation
                    ),
                }))
            ),
            "{label}: {result:?}"
        );
    }
    let replacement = Uuid::new_v4();
    let (replaced, _) = catalog
        .transact_incremental_dry_run(
            &selected,
            vec![select(
                id,
                "test-build",
                Some(revision),
                replacement,
                Some(owner.clone()),
                imports,
            )],
            None,
            Some(snapshot),
            1.into(),
        )
        .await
        .expect("matching CAS may replace a revision without changing ownership");
    assert_eq!(replaced.written_plan(id, "test-build"), Some(replacement));
    assert_eq!(
        replaced.written_plan_replica_owner(id, "test-build"),
        Some(&owner)
    );
    assert_eq!(catalog.state().written_plan(id, "test-build"), None);
}
