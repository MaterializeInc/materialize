// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::future::BoxFuture;
use futures::FutureExt;
use mz_catalog::durable::initialize::USER_VERSION_KEY;
use mz_catalog::durable::objects::serialization::proto;
use mz_catalog::durable::{
    shadow_catalog_state, stash_backed_catalog_state, test_bootstrap_args,
    test_persist_backed_catalog_state, test_persist_backed_catalog_state_with_version,
    test_stash_backed_catalog_state, test_stash_config, CatalogError, DurableCatalogError,
    DurableCatalogState, Epoch, OpenableDurableCatalogState, CATALOG_VERSION,
};
use mz_ore::now::{NOW_ZERO, SYSTEM_TIME};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::{PersistClient, PersistLocation};
use mz_proto::RustType;
use mz_repr::role_id::RoleId;
use mz_sql::catalog::{RoleAttributes, RoleMembership, RoleVars};
use mz_stash::DebugStashFactory;
use uuid::Uuid;

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_is_initialized() {
    let (debug_factory, stash_config) = test_stash_config().await;
    let openable_state1 = stash_backed_catalog_state(stash_config.clone());
    let openable_state2 = std::future::ready(stash_backed_catalog_state(stash_config)).boxed();
    test_is_initialized(openable_state1, openable_state2).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_debug_stash_is_initialized() {
    let debug_factory = DebugStashFactory::new().await;
    let debug_openable_state1 = test_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state2 =
        std::future::ready(test_stash_backed_catalog_state(&debug_factory)).boxed();
    test_is_initialized(debug_openable_state1, debug_openable_state2).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_is_initialized() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let persist_openable_state1 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state2 =
        test_persist_backed_catalog_state(persist_client, organization_id).boxed();
    test_is_initialized(persist_openable_state1, persist_openable_state2).await;
}

async fn test_is_initialized(
    mut openable_state1: impl OpenableDurableCatalogState,
    openable_state2: BoxFuture<'_, impl OpenableDurableCatalogState>,
) {
    assert!(
        !openable_state1.is_initialized().await.unwrap(),
        "catalog has not been opened yet"
    );

    let state = Box::new(openable_state1)
        .open(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
        .await
        .unwrap();
    state.expire().await;

    let mut openable_state2 = openable_state2.await;
    assert!(
        openable_state2.is_initialized().await.unwrap(),
        "catalog has been opened"
    );
    // Check twice because some implementations will cache a read-only connection.
    assert!(
        openable_state2.is_initialized().await.unwrap(),
        "catalog has been opened"
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_get_deployment_generation() {
    let (debug_factory, stash_config) = test_stash_config().await;
    let openable_state1 = stash_backed_catalog_state(stash_config.clone());
    let openable_state2 = std::future::ready(stash_backed_catalog_state(stash_config)).boxed();
    test_get_deployment_generation(openable_state1, openable_state2).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_debug_stash_get_deployment_generation() {
    let debug_factory = DebugStashFactory::new().await;
    let debug_openable_state1 = test_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state2 =
        std::future::ready(test_stash_backed_catalog_state(&debug_factory)).boxed();
    test_get_deployment_generation(debug_openable_state1, debug_openable_state2).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_get_deployment_generation() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let persist_openable_state1 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state2 =
        test_persist_backed_catalog_state(persist_client, organization_id).boxed();
    test_get_deployment_generation(persist_openable_state1, persist_openable_state2).await;
}

async fn test_get_deployment_generation(
    mut openable_state1: impl OpenableDurableCatalogState,
    openable_state2: BoxFuture<'_, impl OpenableDurableCatalogState>,
) {
    assert_eq!(
        openable_state1.get_deployment_generation().await.unwrap(),
        None,
        "deployment generation has not been set"
    );

    let state = Box::new(openable_state1)
        .open(SYSTEM_TIME(), &test_bootstrap_args(), Some(42), None)
        .await
        .unwrap();
    state.expire().await;

    let mut openable_state2 = openable_state2.await;
    assert_eq!(
        openable_state2.get_deployment_generation().await.unwrap(),
        Some(42),
        "deployment generation has been set to 42"
    );
    // Check twice because some implementations will cache a read-only connection.
    assert_eq!(
        openable_state2.get_deployment_generation().await.unwrap(),
        Some(42),
        "deployment generation has been set to 42"
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_open_savepoint() {
    let (debug_factory, stash_config) = test_stash_config().await;
    let openable_state1 = stash_backed_catalog_state(stash_config.clone());
    let openable_state2 = stash_backed_catalog_state(stash_config.clone());
    let openable_state3 = stash_backed_catalog_state(stash_config.clone());
    let openable_state4 = stash_backed_catalog_state(stash_config);
    test_open_savepoint(
        openable_state1,
        openable_state2,
        openable_state3,
        openable_state4,
    )
    .await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_debug_stash_open_savepoint() {
    let debug_factory = DebugStashFactory::new().await;
    let debug_openable_state1 = test_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state2 = test_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state3 = test_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state4 = test_stash_backed_catalog_state(&debug_factory);
    test_open_savepoint(
        debug_openable_state1,
        debug_openable_state2,
        debug_openable_state3,
        debug_openable_state4,
    )
    .await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_open_savepoint() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let persist_openable_state1 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state2 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state3 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state4 =
        test_persist_backed_catalog_state(persist_client, organization_id).await;
    test_open_savepoint(
        persist_openable_state1,
        persist_openable_state2,
        persist_openable_state3,
        persist_openable_state4,
    )
    .await;
}

async fn test_open_savepoint(
    openable_state1: impl OpenableDurableCatalogState,
    openable_state2: impl OpenableDurableCatalogState,
    openable_state3: impl OpenableDurableCatalogState,
    openable_state4: impl OpenableDurableCatalogState,
) {
    {
        // Can't open a savepoint catalog until it's been initialized.
        let err = Box::new(openable_state1)
            .open_savepoint(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
            .await
            .unwrap_err();
        match err {
            CatalogError::Catalog(_) => panic!("unexpected catalog error"),
            CatalogError::Durable(e) => assert!(e.can_recover_with_write_mode()),
        }

        // Initialize the catalog.
        {
            let mut state = Box::new(openable_state2)
                .open(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
                .await
                .unwrap();
            assert_eq!(state.epoch(), Epoch::new(2).expect("known to be non-zero"));
            Box::new(state).expire().await;
        }

        // Open catalog in check mode.
        let mut state = Box::new(openable_state3)
            .open_savepoint(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();
        // Savepoint catalogs do not increment the epoch.
        assert_eq!(state.epoch(), Epoch::new(2).expect("known to be non-zero"));

        // Perform write.
        let mut txn = state.transaction().await.unwrap();
        txn.insert_user_database("db", RoleId::User(1), Vec::new())
            .unwrap();
        txn.commit().await.unwrap();
        // Read back write.
        let db = state
            .snapshot()
            .await
            .unwrap()
            .databases
            .into_iter()
            .find(|(_k, v)| v.name == "db");
        assert!(db.is_some(), "database should exist");

        Box::new(state).expire().await;
    }

    {
        // Open catalog normally.
        let mut state = Box::new(openable_state4)
            .open(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();
        // Write should not have persisted.
        let db = state
            .snapshot()
            .await
            .unwrap()
            .databases
            .into_iter()
            .find(|(_k, v)| v.name == "db");
        assert_eq!(db, None, "database should not exist");
        Box::new(state).expire().await;
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_open_read_only() {
    let (debug_factory, stash_config) = test_stash_config().await;
    let openable_state1 = stash_backed_catalog_state(stash_config.clone());
    let openable_state2 = stash_backed_catalog_state(stash_config.clone());
    let openable_state3 = stash_backed_catalog_state(stash_config);
    test_open_read_only(openable_state1, openable_state2, openable_state3).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_debug_stash_open_read_only() {
    let debug_factory = DebugStashFactory::new().await;
    let debug_openable_state1 = test_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state2 = test_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state3 = test_stash_backed_catalog_state(&debug_factory);
    test_open_read_only(
        debug_openable_state1,
        debug_openable_state2,
        debug_openable_state3,
    )
    .await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_open_read_only() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let persist_openable_state1 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state2 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state3 =
        test_persist_backed_catalog_state(persist_client, organization_id).await;
    test_open_read_only(
        persist_openable_state1,
        persist_openable_state2,
        persist_openable_state3,
    )
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_shadow_read_only_open() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let (debug_factory, stash_config) = test_stash_config().await;

    let shadow_openable_state1 = shadow_catalog_state(
        stash_config.clone(),
        persist_client.clone(),
        organization_id,
    )
    .await;
    let shadow_openable_state2 = shadow_catalog_state(
        stash_config.clone(),
        persist_client.clone(),
        organization_id,
    )
    .await;
    let shadow_openable_state3 =
        shadow_catalog_state(stash_config.clone(), persist_client, organization_id).await;
    test_open_read_only(
        shadow_openable_state1,
        shadow_openable_state2,
        shadow_openable_state3,
    )
    .await;
    debug_factory.drop().await;
}

async fn test_open_read_only(
    openable_state1: impl OpenableDurableCatalogState,
    openable_state2: impl OpenableDurableCatalogState,
    openable_state3: impl OpenableDurableCatalogState,
) {
    // Can't open a read-only catalog until it's been initialized.
    let err = Box::new(openable_state1)
        .open_read_only(SYSTEM_TIME(), &test_bootstrap_args())
        .await
        .unwrap_err();
    match err {
        CatalogError::Catalog(_) => panic!("unexpected catalog error"),
        CatalogError::Durable(e) => assert!(e.can_recover_with_write_mode()),
    }

    // Initialize the catalog.
    let mut state = Box::new(openable_state2)
        .open(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
        .await
        .unwrap();
    assert_eq!(state.epoch(), Epoch::new(2).expect("known to be non-zero"));

    let mut read_only_state = Box::new(openable_state3)
        .open_read_only(SYSTEM_TIME(), &test_bootstrap_args())
        .await
        .unwrap();
    // Read-only catalogs do not increment the epoch.
    assert_eq!(
        read_only_state.epoch(),
        Epoch::new(2).expect("known to be non-zero")
    );
    let err = read_only_state.allocate_user_id().await.unwrap_err();
    match err {
        CatalogError::Catalog(_) => panic!("unexpected catalog error"),
        CatalogError::Durable(e) => assert!(
            e.can_recover_with_write_mode()
                // Stash returns an opaque Postgres error here and doesn't realize that that the
                // above should be true.
                || e.to_string()
                    .contains("cannot execute UPDATE in a read-only transaction")
        ),
    }

    // Read-only catalog should survive writes from a write-able catalog.
    let mut txn = state.transaction().await.unwrap();
    let role_id = txn
        .insert_user_role(
            "joe".to_string(),
            RoleAttributes::new(),
            RoleMembership::new(),
            RoleVars::default(),
        )
        .unwrap();
    txn.commit().await.unwrap();

    let snapshot = read_only_state.snapshot().await.unwrap();
    let role = snapshot.roles.get(&proto::RoleKey {
        id: Some(role_id.into_proto()),
    });
    assert_eq!(&role.unwrap().name, "joe");

    Box::new(read_only_state).expire().await;
    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_open() {
    let (debug_factory, stash_config) = test_stash_config().await;
    let openable_state1 = stash_backed_catalog_state(stash_config.clone());
    let openable_state2 = stash_backed_catalog_state(stash_config.clone());
    let openable_state3 = stash_backed_catalog_state(stash_config);
    test_open(openable_state1, openable_state2, openable_state3).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_debug_stash_open() {
    let debug_factory = DebugStashFactory::new().await;
    let debug_openable_state1 = test_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state2 = test_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state3 = test_stash_backed_catalog_state(&debug_factory);
    test_open(
        debug_openable_state1,
        debug_openable_state2,
        debug_openable_state3,
    )
    .await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_open() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let persist_openable_state1 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state2 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state3 =
        test_persist_backed_catalog_state(persist_client, organization_id).await;
    test_open(
        persist_openable_state1,
        persist_openable_state2,
        persist_openable_state3,
    )
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_shadow_open() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let (debug_factory, stash_config) = test_stash_config().await;

    let shadow_openable_state1 = shadow_catalog_state(
        stash_config.clone(),
        persist_client.clone(),
        organization_id,
    )
    .await;
    let shadow_openable_state2 = shadow_catalog_state(
        stash_config.clone(),
        persist_client.clone(),
        organization_id,
    )
    .await;
    let shadow_openable_state3 =
        shadow_catalog_state(stash_config.clone(), persist_client, organization_id).await;
    test_open(
        shadow_openable_state1,
        shadow_openable_state2,
        shadow_openable_state3,
    )
    .await;
    debug_factory.drop().await;
}

async fn test_open(
    openable_state1: impl OpenableDurableCatalogState,
    openable_state2: impl OpenableDurableCatalogState,
    openable_state3: impl OpenableDurableCatalogState,
) {
    let (snapshot, audit_log) = {
        let mut state = Box::new(openable_state1)
            // Use `NOW_ZERO` for consistent timestamps in the snapshots.
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        assert_eq!(state.epoch(), Epoch::new(2).expect("known to be non-zero"));
        // Check initial snapshot.
        let mut snapshot = state.snapshot().await.unwrap();
        let user_version_key = proto::ConfigKey {
            key: USER_VERSION_KEY.to_string(),
        };
        let user_version_value = {
            // The user_version changes frequently so we test the value in rust and remove it from
            // the snapshot to avoid changing it in the snapshot file.
            let user_version_value = snapshot.configs.remove(&user_version_key).unwrap();
            assert_eq!(user_version_value.value, CATALOG_VERSION);
            user_version_value
        };
        insta::assert_debug_snapshot!("initial_snapshot", snapshot);
        // Add user_version back in for later comparisons.
        snapshot
            .configs
            .insert(user_version_key, user_version_value);
        let audit_log = state.get_audit_logs().await.unwrap();
        insta::assert_debug_snapshot!("initial_audit_log", audit_log);
        Box::new(state).expire().await;
        (snapshot, audit_log)
    };
    // Reopening the catalog will increment the epoch, but shouldn't change the initial snapshot.
    {
        let mut state = Box::new(openable_state2)
            .open(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        assert_eq!(state.epoch(), Epoch::new(3).expect("known to be non-zero"));
        assert_eq!(state.snapshot().await.unwrap(), snapshot);
        assert_eq!(state.get_audit_logs().await.unwrap(), audit_log);
        Box::new(state).expire().await;
    }
    // Reopen the catalog a third time for good measure.
    {
        let mut state = Box::new(openable_state3)
            .open(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        assert_eq!(state.epoch(), Epoch::new(4).expect("known to be non-zero"));
        assert_eq!(state.snapshot().await.unwrap(), snapshot);
        assert_eq!(state.get_audit_logs().await.unwrap(), audit_log);
        Box::new(state).expire().await;
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_unopened_fencing() {
    let (debug_factory, stash_config) = test_stash_config().await;
    let openable_state1 = stash_backed_catalog_state(stash_config.clone());
    let openable_state2 =
        std::future::ready(stash_backed_catalog_state(stash_config.clone())).boxed();
    let openable_state3 = stash_backed_catalog_state(stash_config);
    test_unopened_fencing(openable_state1, openable_state2, openable_state3).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_debug_stash_unopened_fencing() {
    let debug_factory = DebugStashFactory::new().await;
    let debug_openable_state1 = test_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state2 =
        std::future::ready(test_stash_backed_catalog_state(&debug_factory)).boxed();
    let debug_openable_state3 = test_stash_backed_catalog_state(&debug_factory);
    test_unopened_fencing(
        debug_openable_state1,
        debug_openable_state2,
        debug_openable_state3,
    )
    .await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_unopened_fencing() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let persist_openable_state1 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state2 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).boxed();
    let persist_openable_state3 =
        test_persist_backed_catalog_state(persist_client, organization_id).await;
    test_unopened_fencing(
        persist_openable_state1,
        persist_openable_state2,
        persist_openable_state3,
    )
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_shadow_unopened_fencing() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let (debug_factory, stash_config) = test_stash_config().await;

    let shadow_openable_state1 = shadow_catalog_state(
        stash_config.clone(),
        persist_client.clone(),
        organization_id,
    )
    .await;
    let shadow_openable_state2 = shadow_catalog_state(
        stash_config.clone(),
        persist_client.clone(),
        organization_id,
    )
    .boxed();
    let shadow_openable_state3 =
        shadow_catalog_state(stash_config.clone(), persist_client, organization_id).await;
    test_unopened_fencing(
        shadow_openable_state1,
        shadow_openable_state2,
        shadow_openable_state3,
    )
    .await;
    debug_factory.drop().await;
}

async fn test_unopened_fencing(
    openable_state1: impl OpenableDurableCatalogState,
    openable_state2: BoxFuture<'_, impl OpenableDurableCatalogState>,
    openable_state3: impl OpenableDurableCatalogState,
) {
    let deployment_generation = 42;

    // Initialize catalog.
    {
        let _ = Box::new(openable_state1)
            // Use `NOW_ZERO` for consistent timestamps in the snapshots.
            .open(
                NOW_ZERO(),
                &test_bootstrap_args(),
                Some(deployment_generation),
                None,
            )
            .await
            .unwrap();
    }
    let mut openable_state2 = openable_state2.await;

    // Read config collection with unopened catalog.
    assert_eq!(
        Some(deployment_generation),
        openable_state2.get_deployment_generation().await.unwrap()
    );

    // Open catalog, which should bump the epoch.
    let _state = Box::new(openable_state3)
        // Use `NOW_ZERO` for consistent timestamps in the snapshots.
        .open(
            NOW_ZERO(),
            &test_bootstrap_args(),
            Some(deployment_generation + 1),
            None,
        )
        .await
        .unwrap();

    // Unopened catalog should be fenced now.
    let err = openable_state2
        .get_deployment_generation()
        .await
        .unwrap_err();
    assert!(
        matches!(err, CatalogError::Durable(DurableCatalogError::Fence(_))),
        "unexpected err: {err:?}"
    );

    let err = openable_state2.is_initialized().await.unwrap_err();
    assert!(
        matches!(err, CatalogError::Durable(DurableCatalogError::Fence(_))),
        "unexpected err: {err:?}"
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_tombstone() {
    let (debug_factory, stash_config) = test_stash_config().await;
    let openable_state1 = stash_backed_catalog_state(stash_config.clone());
    let openable_state2 =
        std::future::ready(stash_backed_catalog_state(stash_config.clone())).boxed();
    test_tombstone(openable_state1, openable_state2).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_debug_stash_tombstone() {
    let debug_factory = DebugStashFactory::new().await;
    let debug_openable_state1 = test_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state2 =
        std::future::ready(test_stash_backed_catalog_state(&debug_factory)).boxed();
    test_tombstone(debug_openable_state1, debug_openable_state2).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[should_panic]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_tombstone() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let persist_openable_state1 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state2 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).boxed();
    test_tombstone(persist_openable_state1, persist_openable_state2).await;
}

#[mz_ore::test(tokio::test)]
#[should_panic]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_shadow_tombstone() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let (debug_factory, stash_config) = test_stash_config().await;

    let shadow_openable_state1 = shadow_catalog_state(
        stash_config.clone(),
        persist_client.clone(),
        organization_id,
    )
    .await;
    let shadow_openable_state2 = shadow_catalog_state(
        stash_config.clone(),
        persist_client.clone(),
        organization_id,
    )
    .boxed();
    test_tombstone(shadow_openable_state1, shadow_openable_state2).await;
    debug_factory.drop().await;
}

async fn test_tombstone(
    mut openable_state1: impl OpenableDurableCatalogState,
    openable_state2: BoxFuture<'_, impl OpenableDurableCatalogState>,
) {
    async fn set_tombstone(state: &mut Box<dyn DurableCatalogState>, value: bool) {
        let mut txn = state.transaction().await.unwrap();
        txn.set_tombstone(value).unwrap();
        txn.commit().await.unwrap();
    }

    assert_eq!(openable_state1.get_tombstone().await.unwrap(), None);

    let mut state = Box::new(openable_state1)
        // Use `NOW_ZERO` for consistent timestamps in the snapshots.
        .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
        .await
        .unwrap();
    assert_eq!(state.get_tombstone().await.unwrap(), None);

    set_tombstone(&mut state, false).await;
    assert_eq!(state.get_tombstone().await.unwrap(), Some(false));

    let mut openable_state2 = openable_state2.await;
    assert_eq!(openable_state2.get_tombstone().await.unwrap(), Some(false));

    set_tombstone(&mut state, true).await;
    assert_eq!(state.get_tombstone().await.unwrap(), Some(true));

    assert_eq!(openable_state2.get_tombstone().await.unwrap(), Some(true));
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_fencing() {
    async fn testcase(catalog: &str, reader: &str, expected: Result<(), ()>) {
        let catalog_version = semver::Version::parse(catalog).unwrap();
        let reader_version = semver::Version::parse(reader).unwrap();
        let organization_id = Uuid::new_v4();
        let mut persist_cache = PersistClientCache::new_no_metrics();

        persist_cache.cfg.build_version = catalog_version.clone();
        let persist_client = persist_cache
            .open(PersistLocation::new_in_mem())
            .await
            .expect("in-mem location is valid");
        let persist_openable_state = test_persist_backed_catalog_state_with_version(
            persist_client.clone(),
            organization_id.clone(),
            catalog_version,
        )
        .await
        .unwrap();
        let _persist_state = Box::new(persist_openable_state)
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        persist_cache.cfg.build_version = reader_version.clone();
        let persist_client = persist_cache
            .open(PersistLocation::new_in_mem())
            .await
            .expect("in-mem location is valid");
        let persist_openable_state = test_persist_backed_catalog_state_with_version(
            persist_client.clone(),
            organization_id.clone(),
            reader_version,
        )
        .await
        .map(|_| ())
        .map_err(|err| match err {
            DurableCatalogError::IncompatiblePersistVersion { .. } => (),
            err => panic!("unexpected error: {err:?}"),
        });
        assert_eq!(
            persist_openable_state, expected,
            "test case failed, catalog: {catalog}, reader: {reader}, expected: {expected:?}"
        );
    }

    testcase("0.10.0", "0.10.0", Ok(())).await;
    testcase("0.10.0", "0.11.0", Ok(())).await;
    testcase("0.10.0", "0.12.0", Err(())).await;
}
