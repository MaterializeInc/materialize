// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::PersistLocation;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_repr::Diff;
use uuid::Uuid;

use super::UnopenedPersistCatalogState;
use crate::durable::objects::state_update::{StateUpdateKind, StateUpdateKindJson};
use crate::durable::persist::{CATALOG_SEED, fetch_catalog_shard_version, shard_id};
use crate::durable::{
    CatalogError, DurableCatalogError, DurableCatalogState, FenceError, TestCatalogStateBuilder,
    test_bootstrap_args,
};

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_initialized_catalog_open_modes() {
    let builder =
        TestCatalogStateBuilder::new(mz_persist_client::PersistClient::new_for_tests().await)
            .with_default_deploy_generation();
    let args = test_bootstrap_args();
    let mut fresh = builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &args)
        .await
        .expect("initialize fresh catalog");
    assert!(fresh.initialized_catalog());
    fresh.mark_bootstrap_complete().await;
    assert!(fresh.initialized_catalog());
    fresh.expire().await;

    let reopened = builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &args)
        .await
        .expect("reopen initialized catalog");
    assert!(!reopened.initialized_catalog());
    reopened.expire().await;

    let savepoint = builder
        .clone()
        .unwrap_build()
        .await
        .open_savepoint(SYSTEM_TIME().into(), &args)
        .await
        .expect("open savepoint");
    assert!(!savepoint.initialized_catalog());
    savepoint.expire().await;

    let read_only = builder
        .unwrap_build()
        .await
        .open_read_only(&args)
        .await
        .expect("open read-only catalog");
    assert!(!read_only.initialized_catalog());
    read_only.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_initialized_catalog_join() {
    let builder =
        TestCatalogStateBuilder::new(mz_persist_client::PersistClient::new_for_tests().await)
            .with_default_deploy_generation();
    let mut originator = builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .expect("initialize originator");
    originator
        .sync_to_current_updates()
        .await
        .expect("consume initialization updates");
    let mut txn = originator
        .transaction()
        .await
        .expect("open protection transaction");
    txn.set_config("catalog_read_protection_enabled".into(), Some(2))
        .expect("enable protected joins");
    let ts = txn.upper();
    txn.commit(ts)
        .await
        .expect("commit protection configuration");

    let joined = builder
        .unwrap_build()
        .await
        .join()
        .await
        .expect("join initialized catalog");
    assert!(!joined.initialized_catalog());
    assert!(originator.initialized_catalog());
    joined.expire().await;
    originator.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_collection_certification_refreshes_past_cached_fence() {
    use std::collections::{BTreeMap, BTreeSet};

    use mz_persist_client::ShardId;
    use mz_repr::GlobalId;
    use mz_storage_client::controller::StorageTxn;

    let builder =
        TestCatalogStateBuilder::new(mz_persist_client::PersistClient::new_for_tests().await)
            .with_default_deploy_generation();
    let mut writer = builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .expect("initialize catalog");
    writer
        .sync_to_current_updates()
        .await
        .expect("drain bootstrap");
    let dropped = GlobalId::User(1000);
    let retained = GlobalId::User(1001);
    let mappings = BTreeMap::from([(dropped, ShardId::new()), (retained, ShardId::new())]);
    let ids = BTreeSet::from([dropped, retained]);
    let mut txn = writer.transaction().await.expect("transaction");
    txn.set_config("catalog_read_protection_enabled".into(), Some(2))
        .expect("enable joins");
    txn.insert_collection_metadata(mappings.clone())
        .expect("insert mappings");
    // This durable-only test has no in-memory projection to update.
    let _ = txn.get_and_commit_op_updates();
    let ts = txn.upper();
    txn.commit(ts).await.expect("commit mappings");
    let mut joiner = builder
        .unwrap_build()
        .await
        .join()
        .await
        .expect("join catalog");
    joiner.sync_to_current_updates().await.expect("drain join");
    assert_eq!(
        joiner
            .collection_shards_at_current(&ids)
            .await
            .expect("certify current collection mappings"),
        mappings
    );
    let cached_upper = joiner.current_upper().await;

    let mut txn = writer.transaction().await.expect("drop transaction");
    assert_eq!(
        txn.delete_collection_metadata(BTreeSet::from([dropped])),
        vec![(dropped, mappings[&dropped])]
    );
    let _ = txn.get_and_commit_op_updates();
    let ts = txn.upper();
    txn.commit(ts).await.expect("commit retirement");

    // The fence can be satisfied by the cached prefix, even though a peer has
    // since removed a mapping. Certification must fetch the newer durable prefix.
    joiner
        .advance_upper(cached_upper)
        .await
        .expect("cached fence");
    assert_eq!(
        joiner
            .collection_shards_at_current(&ids)
            .await
            .expect("certify current collection mappings"),
        BTreeMap::from([(retained, mappings[&retained])])
    );
    assert!(
        !joiner
            .sync_to_current_updates()
            .await
            .expect("projection updates remain available")
            .is_empty()
    );
    // Replacing the shard under an existing ID must not let a stale observer
    // mistake the successor for the previously certified retired binding.
    let successor = ShardId::new();
    let mut txn = writer.transaction().await.expect("replacement transaction");
    txn.insert_collection_metadata(BTreeMap::from([(dropped, successor)]))
        .expect("insert successor binding");
    let _ = txn.get_and_commit_op_updates();
    let ts = txn.upper();
    txn.commit(ts).await.expect("commit successor binding");
    assert_eq!(
        joiner
            .collection_shards_at_current(&ids)
            .await
            .expect("certify successor"),
        BTreeMap::from([(dropped, successor), (retained, mappings[&retained])])
    );
    assert!(
        !joiner
            .sync_to_current_updates()
            .await
            .expect("successor projection remains available")
            .is_empty()
    );
    joiner.expire().await;
    writer.expire().await;
}

/// Test that the catalog forces users to upgrade one major version at a time.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_version_step() {
    let first_version = semver::Version::parse("0.147.0").expect("failed to parse version");
    let second_version = semver::Version::parse("26.0.0").expect("failed to parse version");
    let second_dev_version =
        semver::Version::parse("26.0.0-dev.0").expect("failed to parse version");
    let third_version = semver::Version::parse("27.1.0").expect("failed to parse version");
    let organization_id = Uuid::new_v4();
    let deploy_generation = 0;
    let mut persist_cache = PersistClientCache::new_no_metrics();
    let catalog_shard_id = shard_id(organization_id, CATALOG_SEED);

    persist_cache.cfg.build_version = first_version.clone();
    let persist_client = persist_cache
        .open(PersistLocation::new_in_mem())
        .await
        .expect("in-mem location is valid");

    assert_eq!(
        None,
        fetch_catalog_shard_version(&persist_client, catalog_shard_id).await
    );

    let persist_openable_state = TestCatalogStateBuilder::new(persist_client.clone())
        .with_organization_id(organization_id)
        .with_deploy_generation(deploy_generation)
        .with_version(first_version.clone())
        .expect_build("failed to create persist catalog")
        .await;
    let mut persist_state = persist_openable_state
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .expect("failed to open persist catalog");
    persist_state.mark_bootstrap_complete().await;

    assert_eq!(
        Some(first_version.clone()),
        fetch_catalog_shard_version(&persist_client, catalog_shard_id).await,
        "writable open + bootstrap should set the catalog shard version"
    );

    persist_cache.cfg.build_version = third_version.clone();
    let persist_client = persist_cache
        .open(PersistLocation::new_in_mem())
        .await
        .expect("in-mem location is valid");
    let err = TestCatalogStateBuilder::new(persist_client.clone())
        .with_organization_id(organization_id)
        .with_deploy_generation(deploy_generation)
        .with_version(third_version.clone())
        .build()
        .await
        .expect_err("skipping versions should error");
    assert!(
        matches!(
            &err,
            DurableCatalogError::IncompatiblePersistVersion {
                found_version,
                catalog_version
            }
            if found_version == &first_version && catalog_version == &third_version
        ),
        "Unexpected error: {err:?}"
    );

    persist_cache.cfg.build_version = second_dev_version.clone();
    let persist_client = persist_cache
        .open(PersistLocation::new_in_mem())
        .await
        .expect("in-mem location is valid");
    TestCatalogStateBuilder::new(persist_client.clone())
        .with_organization_id(organization_id)
        .with_deploy_generation(deploy_generation)
        .with_version(second_dev_version.clone())
        .expect_build("failed to create persist catalog")
        .await;

    persist_cache.cfg.build_version = second_version.clone();
    let persist_client = persist_cache
        .open(PersistLocation::new_in_mem())
        .await
        .expect("in-mem location is valid");
    let state_builder = TestCatalogStateBuilder::new(persist_client.clone())
        .with_organization_id(organization_id)
        .with_deploy_generation(deploy_generation)
        .with_version(second_version.clone());
    let persist_openable_state = state_builder
        .clone()
        .expect_build("failed to create persist catalog")
        .await;
    let _persist_state = persist_openable_state
        .open_savepoint(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .expect("failed to open savepoint persist catalog");

    assert_eq!(
        Some(first_version.clone()),
        fetch_catalog_shard_version(&persist_client, catalog_shard_id).await,
        "opening a savepoint catalog should not increment the catalog shard version"
    );

    let persist_openable_state = state_builder
        .clone()
        .expect_build("failed to create persist catalog")
        .await;
    let _persist_state = persist_openable_state
        .open_read_only(&test_bootstrap_args())
        .await
        .expect("failed to open readonly persist catalog");

    assert_eq!(
        Some(first_version),
        fetch_catalog_shard_version(&persist_client, catalog_shard_id).await,
        "opening a readonly catalog should not increment the catalog shard version"
    );

    let persist_openable_state = state_builder
        .expect_build("failed to create persist catalog")
        .await;
    let mut persist_state = persist_openable_state
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .expect("failed to open persist catalog");
    persist_state.mark_bootstrap_complete().await;

    assert_eq!(
        Some(second_version),
        fetch_catalog_shard_version(&persist_client, catalog_shard_id).await,
        "writable open + bootstrap should increment the catalog shard version"
    );
}

/// A coherent durable-layer v94 catalog, not a current catalog relabeled as v94.
/// The owner, cluster, replica, configuration, and allocators agree. This fixture
/// exercises durable open, not the adapter's SQL bootstrap.
fn legacy_v94_rows() -> Vec<StateUpdateKindJson> {
    let rows: Vec<crate::durable::upgrade::objects_v94::StateUpdateKind> =
        serde_json::from_value(serde_json::json!([
            {"kind": "Config", "key": {"key": "user_version"}, "value": {"value": 94}},
            {"kind": "FenceToken", "deploy_generation": 42, "epoch": 17},
            {"kind": "Setting", "key": {"name": "catalog_content_version"},
                "value": {"value": "26.1.0"}},
            {"kind": "Setting", "key": {"name": "migration_version"},
                "value": {"value": "26.1.0"}},
            {"kind": "IdAlloc", "key": {"name": "user_role"}, "value": {"next_id": 8}},
            {"kind": "IdAlloc", "key": {"name": "user_compute"}, "value": {"next_id": 13}},
            {"kind": "IdAlloc", "key": {"name": "replica"}, "value": {"next_id": 24}},
            {"kind": "Role", "key": {"id": {"User": 7}}, "value": {
                "name": "legacy_owner", "attributes": {"inherit": true,
                    "superuser": false, "login": true, "auto_provision_source": null},
                "membership": {"map": []}, "vars": {"entries": []}, "oid": 20000
            }},
            {"kind": "Cluster", "key": {"id": {"User": 12}}, "value": {
                "name": "legacy_cluster", "owner_id": {"User": 7}, "privileges": [],
                "config": {"workload_class": "legacy", "variant": "Unmanaged"}
            }},
            {"kind": "ClusterReplica", "key": {"id": {"User": 23}}, "value": {
                "cluster_id": {"User": 12}, "name": "legacy_replica", "owner_id": {"User": 7},
                "config": {
                    "logging": {"log_logging": true, "interval": {"secs": 1, "nanos": 0}},
                    "location": {"Managed": {"size": "scale=1,workers=1",
                        "availability_zones": ["az1"], "internal": false,
                        "billed_as": null, "pending": false}},
                    "arrangement_compression": true
                }
            }},
            {"kind": "ClusterSystemConfiguration",
                "key": {"cluster_id": {"User": 12}, "name": "max_result_size"},
                "value": {"value": "2GB"}},
            {"kind": "ReplicaSystemConfiguration",
                "key": {"replica_id": {"User": 23}, "name": "max_result_size"},
                "value": {"value": "1GB"}}
        ]))
        .expect("fixture must decode as actual v94 records");
    rows.into_iter()
        .map(StateUpdateKindJson::from_serde)
        .collect()
}

async fn raw_catalog(builder: &TestCatalogStateBuilder) -> UnopenedPersistCatalogState {
    UnopenedPersistCatalogState::new(
        builder.persist_client.clone(),
        builder.organization_id,
        builder.version.clone(),
        builder.deploy_generation,
        std::sync::Arc::clone(&builder.metrics),
        None,
    )
    .await
    .expect("raw catalog can be inspected")
}

async fn seed_legacy_v94() -> TestCatalogStateBuilder {
    let version = semver::Version::new(26, 1, 0);
    let mut cache = PersistClientCache::new_no_metrics();
    cache.cfg.build_version = version.clone();
    let client = cache
        .open(PersistLocation::new_in_mem())
        .await
        .expect("open legacy Persist client");
    let builder = TestCatalogStateBuilder::new(client)
        .with_version(version)
        .with_deploy_generation(43);
    let mut raw = raw_catalog(&builder).await;
    let ts = raw.upper;
    raw.compare_and_append(
        legacy_v94_rows()
            .into_iter()
            .map(|row| (row, Diff::ONE))
            .collect(),
        ts,
    )
    .await
    .expect("seed v94 before any public open");
    builder
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn unsupported_legacy_open_preserves_catalog_and_admission() {
    for savepoint in [false, true] {
        let builder = seed_legacy_v94().await;
        let before = raw_catalog(&builder).await;
        let openable = builder.clone().unwrap_build().await;
        let result = if savepoint {
            openable
                .open_savepoint(SYSTEM_TIME().into(), &test_bootstrap_args())
                .await
        } else {
            openable
                .open(SYSTEM_TIME().into(), &test_bootstrap_args())
                .await
        };
        assert!(matches!(
            result,
            Err(CatalogError::Durable(
                DurableCatalogError::IncompatibleDataVersion {
                    found_version: 94,
                    min_catalog_version: 96,
                    ..
                }
            ))
        ));
        let after = raw_catalog(&builder).await;
        assert_eq!(before.upper, after.upper);
        assert_eq!(before.snapshot, after.snapshot);
        assert_eq!(
            before.fenceable_token.token(),
            after.fenceable_token.token()
        );
        assert!(after.update_applier.deployment_admission.is_none());
    }
}

/// Each participant has process-local authorization and cached shard state, but
/// the cache retains the same in-memory Blob and Consensus backing.
async fn admission_participant(
    cache: &mut PersistClientCache,
    organization_id: Uuid,
    generation: u64,
    version: &semver::Version,
) -> (PersistConfig, TestCatalogStateBuilder) {
    cache.cfg = PersistConfig::new_for_tests();
    cache.cfg.build_version = version.clone();
    cache
        .cfg
        .require_state_version_target(Some(shard_id(organization_id, CATALOG_SEED)));
    cache.clear_state_cache();
    let client = cache
        .open(PersistLocation::new_in_mem())
        .await
        .expect("in-memory backing is valid");
    assert_eq!(client.build_version(), version);
    let builder = TestCatalogStateBuilder::new(client)
        .with_organization_id(organization_id)
        .with_deploy_generation(generation)
        .with_version(version.clone());
    (cache.cfg.clone(), builder)
}

async fn bootstrap_admission(builder: TestCatalogStateBuilder) -> Box<dyn DurableCatalogState> {
    let mut active = builder
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .expect("initial deployment can bootstrap");
    active
        .sync_to_current_updates()
        .await
        .expect("active deployment can follow catalog updates");
    // Latch protection through the native bootstrap transaction. The initial
    // writable open has already committed this generation's admission record.
    let mut txn = active
        .transaction()
        .await
        .expect("bootstrap can open a transaction");
    txn.set_config("catalog_read_protection_enabled".into(), Some(1))
        .expect("bootstrap can latch read protection");
    txn.finalize_index_compaction_bounds();
    let _ = txn.get_and_commit_op_updates();
    let ts = txn.upper();
    txn.commit(ts).await.expect("protection latch can commit");
    active.mark_bootstrap_complete().await;
    active
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_retirement_retracts_raw_rows_and_fences_before_unreadable_suffix() {
    let version = semver::Version::new(26, 1, 0);
    let organization_id = Uuid::new_v4();
    let mut cache = PersistClientCache::new_no_metrics();
    let (_, active_builder) = admission_participant(&mut cache, organization_id, 7, &version).await;
    let mut active = bootstrap_admission(active_builder.clone()).await;
    let (_, pending_builder) =
        admission_participant(&mut cache, organization_id, 8, &version).await;
    let mut pending = pending_builder
        .unwrap_build()
        .await
        .join_prewarming(&version.to_string())
        .await
        .expect("generation 8 can prewarm");

    // Unknown fields participate in Persist row identity even though typed
    // catalog decoding ignores them.
    let replica_config = |generation| {
        StateUpdateKindJson::from_serde(serde_json::json!({
            "kind": "ReplicaSystemConfiguration",
            "key": {
                "replica_id": {"User": 23},
                "deployment_generation": generation,
                "name": "max_result_size"
            },
            "value": {"value": "1GB", "unknown_extra_field": {"preserve": true}}
        }))
    };
    let target = replica_config(8);
    let peer = replica_config(9);
    let mut raw = raw_catalog(&active_builder).await;
    let ts = raw.upper;
    raw.compare_and_append(
        vec![(target.clone(), Diff::ONE), (peer.clone(), Diff::ONE)],
        ts,
    )
    .await
    .expect("append raw realization metadata");
    pending
        .sync_to_current_updates()
        .await
        .expect("pending handle can read the realization metadata");

    assert!(
        active
            .retire_deployment(8)
            .await
            .expect("retire generation 8")
    );
    let mut raw = raw_catalog(&active_builder).await;
    assert!(
        raw.snapshot.iter().all(|(_, _, diff)| *diff > Diff::ZERO),
        "retirement must not leave unmatched raw retractions"
    );
    assert!(
        raw.snapshot.iter().all(|(row, _, _)| row != &target),
        "retirement must retract the exact target row, including unknown fields"
    );
    let remaining_configs: Vec<_> = raw
        .snapshot
        .iter()
        .filter(|(row, _, _)| row.kind() == "ReplicaSystemConfiguration")
        .map(|(row, _, diff)| (row.clone(), *diff))
        .collect();
    assert_eq!(remaining_configs, vec![(peer, Diff::ONE)]);

    // Keep the pending handle behind retirement until the unreadable suffix is
    // durable, so one sync must stop at retirement rather than decode the suffix.
    let unreadable = StateUpdateKindJson::from_serde(serde_json::json!({
        "kind": "FutureCatalogRow"
    }));
    assert!(StateUpdateKind::try_from(unreadable.clone()).is_err());
    let ts = raw.upper;
    raw.compare_and_append(vec![(unreadable, Diff::ONE)], ts)
        .await
        .expect("append unreadable row after retirement");
    let error = pending
        .sync_to_current_updates()
        .await
        .expect_err("retired handle must stop before decoding the unreadable suffix");
    assert!(
        matches!(
            error,
            CatalogError::Durable(DurableCatalogError::Fence(FenceError::DeploymentRetired {
                generation: 8
            }))
        ),
        "{error}"
    );
    pending.expire().await;
    active.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_deployment_admission_promotion_and_restart() {
    let v1 = semver::Version::new(26, 1, 0);
    let v2 = semver::Version::new(26, 2, 0);
    let organization_id = Uuid::new_v4();
    let mut cache = PersistClientCache::new_no_metrics();
    let (active_cfg, active_builder) =
        admission_participant(&mut cache, organization_id, 1, &v1).await;
    let mut active = bootstrap_admission(active_builder).await;
    assert_eq!(active_cfg.state_version_target(), v1);

    let (pending_cfg, pending_builder) =
        admission_participant(&mut cache, organization_id, 2, &v2).await;
    let error = pending_builder
        .clone()
        .unwrap_build()
        .await
        .join_admitted_prewarming(&v2.to_string())
        .await
        .err()
        .expect("a joining adapter cannot initialize deployment membership");
    assert!(
        error
            .to_string()
            .contains("joining requires an initialized deployment"),
        "{error}"
    );
    let pending = pending_builder
        .unwrap_build()
        .await
        .join_prewarming(&v2.to_string())
        .await
        .expect("compatible deployment can prewarm");
    assert_eq!(pending_cfg.state_version_target(), v1);
    pending.expire().await;

    let (restart_cfg, restart_builder) =
        admission_participant(&mut cache, organization_id, 2, &v2).await;
    let restart = restart_builder.clone().unwrap_build().await;
    assert_eq!(restart_cfg.state_version_target(), v1);
    let restart = restart
        .join_admitted_prewarming(&v2.to_string())
        .await
        .expect("admitted deployment can restart prewarming");
    assert_eq!(restart_cfg.state_version_target(), v1);
    restart.expire().await;

    let promoted = restart_builder
        .unwrap_build()
        .await
        .open_for_promotion(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .expect("compatible deployment can promote");
    assert_eq!(restart_cfg.state_version_target(), v2);
    // A different process's target changes only when it observes committed policy.
    assert_eq!(pending_cfg.state_version_target(), v1);
    assert_eq!(active_cfg.state_version_target(), v1);
    let error = active
        .sync_to_current_updates()
        .await
        .expect_err("promotion fences the old deployment");
    assert!(
        matches!(error, CatalogError::Durable(DurableCatalogError::Fence(_))),
        "{error}"
    );
    assert_eq!(active_cfg.state_version_target(), v1);
    active.expire().await;

    let (follower_cfg, follower_builder) =
        admission_participant(&mut cache, organization_id, 2, &v2).await;
    let follower = follower_builder.unwrap_build().await;
    assert_eq!(follower_cfg.state_version_target(), v2);
    let follower = follower
        .join_active()
        .await
        .expect("same-version follower can join the active deployment");
    assert_eq!(follower_cfg.state_version_target(), v2);
    follower.expire().await;
    promoted.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_deployment_admission_preserves_pending_version_requirement() {
    let v1 = semver::Version::new(26, 1, 0);
    let v2 = semver::Version::new(26, 2, 0);
    let v3 = semver::Version::new(26, 3, 0);
    let organization_id = Uuid::new_v4();
    let mut cache = PersistClientCache::new_no_metrics();
    let (_, active_builder) = admission_participant(&mut cache, organization_id, 1, &v1).await;
    let active = bootstrap_admission(active_builder).await;
    let (promoter_cfg, promoter_builder) =
        admission_participant(&mut cache, organization_id, 2, &v3).await;
    let promoter = promoter_builder
        .clone()
        .unwrap_build()
        .await
        .join_prewarming(&v3.to_string())
        .await
        .expect("compatible deployment can prewarm");
    let (pending_cfg, pending_builder) =
        admission_participant(&mut cache, organization_id, 3, &v2).await;
    let mut pending = pending_builder
        .unwrap_build()
        .await
        .join_prewarming(&v2.to_string())
        .await
        .expect("compatible deployment can prewarm");
    assert_eq!(pending_cfg.state_version_target(), v1);
    promoter.expire().await;
    active.expire().await;

    let promoted = promoter_builder
        .unwrap_build()
        .await
        .open_for_promotion(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .expect("compatible deployment can promote");
    assert_eq!(promoter_cfg.state_version_target(), v2);
    pending
        .sync_to_current_updates()
        .await
        .expect("pending deployment follows promotion");
    assert_eq!(pending_cfg.state_version_target(), v2);
    pending.expire().await;

    // The pending member's lower requirement survives even with no live handle.
    let (restart_cfg, restart_builder) =
        admission_participant(&mut cache, organization_id, 2, &v3).await;
    let restart = restart_builder.unwrap_build().await;
    assert_eq!(restart_cfg.state_version_target(), v2);
    let restart = restart
        .join_active()
        .await
        .expect("same-version follower can join the active deployment");
    assert_eq!(restart_cfg.state_version_target(), v2);
    restart.expire().await;
    promoted.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_deployment_admission_rejections_do_not_publish_target() {
    let v1 = semver::Version::new(26, 1, 0);
    let v2 = semver::Version::new(26, 2, 0);
    let organization_id = Uuid::new_v4();
    let mut cache = PersistClientCache::new_no_metrics();
    let (active_cfg, active_builder) =
        admission_participant(&mut cache, organization_id, 1, &v1).await;
    let mut active = bootstrap_admission(active_builder).await;

    let (different_cfg, different_builder) =
        admission_participant(&mut cache, organization_id, 1, &v2).await;
    let error = different_builder
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .expect_err("a generation cannot admit a different binary version");
    assert!(
        matches!(&error, CatalogError::Durable(DurableCatalogError::NotWritable(reason))
            if reason.contains("already admitted with another version")),
        "{error}"
    );
    assert_eq!(different_cfg.state_version_target(), v1);
    active
        .sync_to_current_updates()
        .await
        .expect("active deployment can follow catalog updates");
    assert_eq!(active_cfg.state_version_target(), v1);

    // Capture an old binary's handle before promotion, so rejection can also
    // prove that its already-authorized local target is not advanced.
    let (old_cfg, old_builder) = admission_participant(&mut cache, organization_id, 3, &v1).await;
    let old = old_builder.unwrap_build().await;
    assert_eq!(old_cfg.state_version_target(), v1);
    let (promoter_cfg, promoter_builder) =
        admission_participant(&mut cache, organization_id, 2, &v2).await;
    let promoted = promoter_builder
        .unwrap_build()
        .await
        .open_for_promotion(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .expect("compatible deployment can promote");
    assert_eq!(promoter_cfg.state_version_target(), v2);
    let error = old
        .join_prewarming(&v1.to_string())
        .await
        .expect_err("older binary cannot rejoin after format advancement");
    assert!(
        matches!(&error, CatalogError::Durable(DurableCatalogError::NotWritable(reason))
            if reason.contains("cannot write")),
        "{error}"
    );
    assert_eq!(old_cfg.state_version_target(), v1);

    let (observer_cfg, observer_builder) =
        admission_participant(&mut cache, organization_id, 2, &v2).await;
    let observer = observer_builder.unwrap_build().await;
    assert_eq!(observer_cfg.state_version_target(), v2);
    let observer = observer
        .join_active()
        .await
        .expect("same-version follower can join the active deployment");
    observer.expire().await;
    active.expire().await;
    promoted.expire().await;
}
