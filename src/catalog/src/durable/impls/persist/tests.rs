// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::now::NOW_ZERO;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::PersistLocation;
use uuid::Uuid;

use crate::durable::impls::persist::{shard_id, UnopenedPersistCatalogState, UPGRADE_SEED};
use crate::durable::{
    test_bootstrap_args, test_persist_backed_catalog_state_with_version,
    OpenableDurableCatalogState,
};

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_upgrade_shard() {
    let first_version = semver::Version::parse("0.10.0").expect("failed to parse version");
    let second_version = semver::Version::parse("0.11.0").expect("failed to parse version");
    let organization_id = Uuid::new_v4();
    let mut persist_cache = PersistClientCache::new_no_metrics();
    let upgrade_shard_id = shard_id(organization_id, UPGRADE_SEED);

    persist_cache.cfg.build_version = first_version.clone();
    let persist_client = persist_cache
        .open(PersistLocation::new_in_mem())
        .await
        .expect("in-mem location is valid");

    assert_eq!(
        None,
        UnopenedPersistCatalogState::fetch_catalog_upgrade_shard_version(
            &persist_client,
            upgrade_shard_id
        )
        .await
    );

    let persist_openable_state = test_persist_backed_catalog_state_with_version(
        persist_client.clone(),
        organization_id.clone(),
        first_version.clone(),
    )
    .await
    .expect("failed to create persist catalog");
    let _persist_state = Box::new(persist_openable_state)
        .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
        .await
        .expect("failed to open persist catalog");

    assert_eq!(
        Some(first_version.clone()),
        UnopenedPersistCatalogState::fetch_catalog_upgrade_shard_version(
            &persist_client,
            upgrade_shard_id
        )
        .await,
        "opening the catalog should increment the upgrade version"
    );

    persist_cache.cfg.build_version = second_version.clone();
    let persist_client = persist_cache
        .open(PersistLocation::new_in_mem())
        .await
        .expect("in-mem location is valid");
    let persist_openable_state = test_persist_backed_catalog_state_with_version(
        persist_client.clone(),
        organization_id.clone(),
        second_version.clone(),
    )
    .await
    .expect("failed to create persist catalog");
    let _persist_state = Box::new(persist_openable_state)
        .open_savepoint(NOW_ZERO(), &test_bootstrap_args(), None, None)
        .await
        .expect("failed to open savepoint persist catalog");

    assert_eq!(
        Some(first_version.clone()),
        UnopenedPersistCatalogState::fetch_catalog_upgrade_shard_version(
            &persist_client,
            upgrade_shard_id
        )
        .await,
        "opening a savepoint catalog should not increment the upgrade version"
    );

    let persist_openable_state = test_persist_backed_catalog_state_with_version(
        persist_client.clone(),
        organization_id.clone(),
        second_version,
    )
    .await
    .expect("failed to create persist catalog");
    let _persist_state = Box::new(persist_openable_state)
        .open_read_only(&test_bootstrap_args())
        .await
        .expect("failed to open readonly persist catalog");

    assert_eq!(
        Some(first_version),
        UnopenedPersistCatalogState::fetch_catalog_upgrade_shard_version(
            &persist_client,
            upgrade_shard_id
        )
        .await,
        "opening a readonly catalog should not increment the upgrade version"
    );
}
