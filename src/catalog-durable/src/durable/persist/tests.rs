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
use uuid::Uuid;

use crate::durable::persist::{CATALOG_SEED, fetch_catalog_shard_version, shard_id};
use crate::durable::{DurableCatalogError, TestCatalogStateBuilder, test_bootstrap_args};

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
        .expect("failed to open persist catalog")
        .0;
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
        .expect("failed to open savepoint persist catalog")
        .0;

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
        .expect("failed to open persist catalog")
        .0;
    persist_state.mark_bootstrap_complete().await;

    assert_eq!(
        Some(second_version),
        fetch_catalog_shard_version(&persist_client, catalog_shard_id).await,
        "writable open + bootstrap should increment the catalog shard version"
    );
}
