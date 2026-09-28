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
use uuid::Uuid;

use crate::durable::persist::{CATALOG_SEED, fetch_catalog_shard_version, shard_id};
use crate::durable::{
    CatalogError, DurableCatalogError, DurableCatalogState, TestCatalogStateBuilder,
    test_bootstrap_args,
};

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
        .join_prewarming(&v2.to_string())
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
