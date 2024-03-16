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
use mz_catalog::durable::objects::{DurableType, Snapshot};
use mz_catalog::durable::{
    test_bootstrap_args, test_persist_backed_catalog_state,
    test_persist_backed_catalog_state_with_version, CatalogError, Database, DurableCatalogError,
    Epoch, OpenableDurableCatalogState, Schema, CATALOG_VERSION,
};
use mz_ore::cast::usize_to_u64;
use mz_ore::now::{NOW_ZERO, SYSTEM_TIME};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::{PersistClient, PersistLocation};
use mz_proto::RustType;
use mz_repr::role_id::RoleId;
use mz_sql::catalog::{RoleAttributes, RoleMembership, RoleVars};
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use uuid::Uuid;

/// A new type for [`Snapshot`] that excludes the user_version from the debug output. The
/// user_version changes frequently, so it's useful to print the contents excluding the
/// user_version to avoid having to update the expected value in tests.
struct HiddenUserVersionSnapshot<'a>(&'a Snapshot);

impl HiddenUserVersionSnapshot<'_> {
    fn user_version(&self) -> Option<&proto::ConfigValue> {
        self.0.configs.get(&Self::user_version_key())
    }

    fn user_version_key() -> proto::ConfigKey {
        proto::ConfigKey {
            key: USER_VERSION_KEY.to_string(),
        }
    }
}

impl Debug for HiddenUserVersionSnapshot<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Snapshot {
            databases,
            schemas,
            roles,
            items,
            comments,
            clusters,
            cluster_replicas,
            introspection_sources,
            id_allocator,
            configs,
            settings,
            timestamps,
            system_object_mappings,
            system_configurations,
            default_privileges,
            system_privileges,
            storage_metadata,
            unfinalized_shards,
            persist_txn_shard,
        } = self.0;
        let mut configs: BTreeMap<proto::ConfigKey, proto::ConfigValue> = configs.clone();
        configs.remove(&Self::user_version_key());
        f.debug_struct("Snapshot")
            .field("databases", databases)
            .field("schemas", schemas)
            .field("roles", roles)
            .field("items", items)
            .field("comments", comments)
            .field("clusters", clusters)
            .field("cluster_replicas", cluster_replicas)
            .field("introspection_sources", introspection_sources)
            .field("id_allocator", id_allocator)
            .field("configs", &configs)
            .field("settings", settings)
            .field("timestamps", timestamps)
            .field("system_object_mappings", system_object_mappings)
            .field("system_configurations", system_configurations)
            .field("default_privileges", default_privileges)
            .field("system_privileges", system_privileges)
            .field("storage_metadata", storage_metadata)
            .field("unfinalized_shards", unfinalized_shards)
            .field("persist_txn_shard", persist_txn_shard)
            .finish()
    }
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
    }

    // Initialize the catalog.
    {
        let state = Box::new(openable_state2)
            .open(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();
        assert_eq!(state.epoch(), Epoch::new(2).expect("known to be non-zero"));
        Box::new(state).expire().await;
    }

    {
        // Open catalog in savepoint mode.
        let mut state = Box::new(openable_state3)
            .open_savepoint(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();
        // Savepoint catalogs do not increment the epoch.
        assert_eq!(state.epoch(), Epoch::new(2).expect("known to be non-zero"));

        // Perform writes.
        let mut txn = state.transaction().await.unwrap();
        let mut ids = Vec::new();
        let mut db_schemas = Vec::new();
        for i in 0..10 {
            let (db_id, db_oid) = txn
                .insert_user_database(&format!("db{i}"), RoleId::User(i), Vec::new())
                .unwrap();
            let (schema_id, schema_oid) = txn
                .insert_user_schema(db_id, &format!("sc{i}"), RoleId::User(i), Vec::new())
                .unwrap();
            ids.push((db_id.clone(), schema_id.clone()));
            db_schemas.push((
                Database {
                    id: db_id.clone(),
                    oid: db_oid,
                    name: format!("db{i}"),
                    owner_id: RoleId::User(i),
                    privileges: Vec::new(),
                },
                Schema {
                    id: schema_id,
                    oid: schema_oid,
                    name: format!("sc{i}"),
                    database_id: Some(db_id),
                    owner_id: RoleId::User(i),
                    privileges: Vec::new(),
                },
            ));
        }
        txn.commit().await.unwrap();

        // Read back writes.
        let snapshot = state.snapshot().await.unwrap();
        for (db, schema) in &db_schemas {
            let (db_key, db_value) = db.clone().into_key_value();
            let db_found = snapshot.databases.get(&db_key.into_proto()).unwrap();
            assert_eq!(&db_value.into_proto(), db_found);
            let (schema_key, schema_value) = schema.clone().into_key_value();
            let schema_found = snapshot.schemas.get(&schema_key.into_proto()).unwrap();
            assert_eq!(&schema_value.into_proto(), schema_found);
        }

        // Perform updates.
        let mut txn = state.transaction().await.unwrap();
        for (i, (db, schema)) in db_schemas.iter_mut().enumerate() {
            db.name = format!("foo{i}");
            db.owner_id = RoleId::User(usize_to_u64(i) + 100);
            txn.update_database(db.id.clone().clone(), db.clone())
                .unwrap();
            schema.name = format!("bar{i}");
            schema.owner_id = RoleId::User(usize_to_u64(i) + 100);
            txn.update_schema(schema.id.clone(), schema.clone())
                .unwrap();
        }
        txn.commit().await.unwrap();

        // Read back updates.
        let snapshot = state.snapshot().await.unwrap();
        for (db, schema) in &db_schemas {
            let (db_key, db_value) = db.clone().into_key_value();
            let db_found = snapshot.databases.get(&db_key.into_proto()).unwrap();
            assert_eq!(&db_value.into_proto(), db_found);
            let (schema_key, schema_value) = schema.clone().into_key_value();
            let schema_found = snapshot.schemas.get(&schema_key.into_proto()).unwrap();
            assert_eq!(&schema_value.into_proto(), schema_found);
        }

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

async fn test_open_read_only(
    openable_state1: impl OpenableDurableCatalogState,
    openable_state2: impl OpenableDurableCatalogState,
    openable_state3: impl OpenableDurableCatalogState,
) {
    // Can't open a read-only catalog until it's been initialized.
    let err = Box::new(openable_state1)
        .open_read_only(&test_bootstrap_args())
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
        .open_read_only(&test_bootstrap_args())
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
        CatalogError::Durable(e) => assert!(e.can_recover_with_write_mode()),
    }

    // Read-only catalog should survive writes from a write-able catalog.
    let mut txn = state.transaction().await.unwrap();
    let (role_id, _) = txn
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
async fn test_persist_open() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let persist_openable_state1 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state2 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).boxed();
    let persist_openable_state3 =
        test_persist_backed_catalog_state(persist_client, organization_id).boxed();
    test_open(
        persist_openable_state1,
        persist_openable_state2,
        persist_openable_state3,
    )
    .await;
}

async fn test_open(
    openable_state1: impl OpenableDurableCatalogState,
    openable_state2: BoxFuture<'_, impl OpenableDurableCatalogState>,
    openable_state3: BoxFuture<'_, impl OpenableDurableCatalogState>,
) {
    let (snapshot, audit_log) = {
        let mut state = Box::new(openable_state1)
            // Use `NOW_ZERO` for consistent timestamps in the snapshots.
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        assert_eq!(state.epoch(), Epoch::new(2).expect("known to be non-zero"));
        // Check initial snapshot.
        let snapshot = state.snapshot().await.unwrap();
        {
            let test_snapshot = HiddenUserVersionSnapshot(&snapshot);
            let user_version = test_snapshot.user_version().unwrap();
            assert_eq!(user_version.value, CATALOG_VERSION);
            insta::assert_debug_snapshot!("initial_snapshot", test_snapshot);
        }
        let audit_log = state.get_audit_logs().await.unwrap();
        insta::assert_debug_snapshot!("initial_audit_log", audit_log);
        Box::new(state).expire().await;
        (snapshot, audit_log)
    };
    // Reopening the catalog will increment the epoch, but shouldn't change the initial snapshot.
    {
        let mut state = Box::new(openable_state2.await)
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
        let mut state = Box::new(openable_state3.await)
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
