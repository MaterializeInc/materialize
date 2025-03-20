// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::time::Duration;

use mz_catalog::durable::initialize::USER_VERSION_KEY;
use mz_catalog::durable::objects::serialization::proto;
use mz_catalog::durable::objects::{DurableType, Snapshot};
use mz_catalog::durable::{
    test_bootstrap_args, CatalogError, Database, DurableCatalogError, DurableCatalogState, Epoch,
    FenceError, Schema, TestCatalogStateBuilder, BUILTIN_MIGRATION_SHARD_KEY, CATALOG_VERSION,
    EXPRESSION_CACHE_SHARD_KEY,
};
use mz_ore::cast::usize_to_u64;
use mz_ore::collections::HashSet;
use mz_ore::now::{NOW_ZERO, SYSTEM_TIME};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::{PersistClient, PersistLocation};
use mz_persist_types::ShardId;
use mz_proto::RustType;
use mz_repr::role_id::RoleId;
use mz_sql::catalog::{RoleAttributes, RoleMembership, RoleVars};
use uuid::Uuid;

/// A new type for [`Snapshot`] that excludes fields that change often from the debug output. It's
/// useful to print the contents excluding these fields to avoid having to update the expected value
/// in tests.
struct StableSnapshot<'a>(&'a Snapshot);

impl StableSnapshot<'_> {
    fn user_version(&self) -> Option<&proto::ConfigValue> {
        self.0.configs.get(&Self::user_version_key())
    }

    fn user_version_key() -> proto::ConfigKey {
        proto::ConfigKey {
            key: USER_VERSION_KEY.to_string(),
        }
    }

    fn builtin_migration_shard(&self) -> Option<&proto::SettingValue> {
        self.0.settings.get(&Self::builtin_migration_shard_key())
    }

    fn builtin_migration_shard_key() -> proto::SettingKey {
        proto::SettingKey {
            name: BUILTIN_MIGRATION_SHARD_KEY.to_string(),
        }
    }

    fn expression_cache_shard(&self) -> Option<&proto::SettingValue> {
        self.0.settings.get(&Self::expression_cache_shard_key())
    }

    fn expression_cache_shard_key() -> proto::SettingKey {
        proto::SettingKey {
            name: EXPRESSION_CACHE_SHARD_KEY.to_string(),
        }
    }
}

impl Debug for StableSnapshot<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Snapshot {
            databases,
            schemas,
            roles,
            items,
            comments,
            clusters,
            network_policies,
            cluster_replicas,
            introspection_sources,
            id_allocator,
            configs,
            settings,
            source_references,
            system_object_mappings,
            system_configurations,
            default_privileges,
            system_privileges,
            storage_collection_metadata,
            unfinalized_shards,
            txn_wal_shard,
        } = self.0;
        let mut configs: BTreeMap<proto::ConfigKey, proto::ConfigValue> = configs.clone();
        configs.remove(&Self::user_version_key());
        let mut settings: BTreeMap<proto::SettingKey, proto::SettingValue> = settings.clone();
        settings.remove(&Self::builtin_migration_shard_key());
        settings.remove(&Self::expression_cache_shard_key());
        f.debug_struct("Snapshot")
            .field("databases", databases)
            .field("schemas", schemas)
            .field("roles", roles)
            .field("items", items)
            .field("comments", comments)
            .field("clusters", clusters)
            .field("network_policies", network_policies)
            .field("cluster_replicas", cluster_replicas)
            .field("introspection_sources", introspection_sources)
            .field("id_allocator", id_allocator)
            .field("configs", &configs)
            .field("settings", &settings)
            .field("source_references", source_references)
            .field("system_object_mappings", system_object_mappings)
            .field("system_configurations", system_configurations)
            .field("default_privileges", default_privileges)
            .field("system_privileges", system_privileges)
            .field("storage_collection_metadata", storage_collection_metadata)
            .field("unfinalized_shards", unfinalized_shards)
            .field("txn_wal_shard", txn_wal_shard)
            .finish()
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_is_initialized() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_is_initialized(state_builder).await;
}

async fn test_is_initialized(state_builder: TestCatalogStateBuilder) {
    let state_builder = state_builder.with_default_deploy_generation();

    let mut openable_state1 = state_builder.clone().unwrap_build().await;
    assert!(
        !openable_state1.is_initialized().await.unwrap(),
        "catalog has not been opened yet"
    );

    let state = openable_state1
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;
    state.expire().await;

    let mut openable_state2 = state_builder.unwrap_build().await;
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
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_get_deployment_generation(state_builder).await;
}

async fn test_get_deployment_generation(state_builder: TestCatalogStateBuilder) {
    let deploy_generation = 42;

    {
        let mut openable_state = state_builder
            .clone()
            .with_deploy_generation(deploy_generation)
            .unwrap_build()
            .await;
        assert_eq!(
            openable_state
                .get_deployment_generation()
                .await
                .unwrap_err()
                .to_string(),
            CatalogError::Durable(DurableCatalogError::Uninitialized).to_string()
        );

        let state = openable_state
            .open(SYSTEM_TIME().into(), &test_bootstrap_args())
            .await
            .unwrap()
            .0;
        state.expire().await;
    }

    {
        let mut openable_state = state_builder
            .clone()
            .with_deploy_generation(deploy_generation)
            .unwrap_build()
            .await;
        assert_eq!(
            openable_state.get_deployment_generation().await.unwrap(),
            deploy_generation,
            "deployment generation has been set to {deploy_generation}"
        );
        // Check twice because some implementations will cache a read-only connection.
        assert_eq!(
            openable_state.get_deployment_generation().await.unwrap(),
            deploy_generation,
            "deployment generation has been set to {deploy_generation}"
        );
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_open_savepoint() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_open_savepoint(state_builder).await;
}

async fn test_open_savepoint(state_builder: TestCatalogStateBuilder) {
    let state_builder = state_builder.with_default_deploy_generation();

    {
        // Can't open a savepoint catalog until it's been initialized.
        let err = state_builder
            .clone()
            .unwrap_build()
            .await
            .open_savepoint(SYSTEM_TIME().into(), &test_bootstrap_args())
            .await
            .unwrap_err();
        match err {
            CatalogError::Catalog(_) => panic!("unexpected catalog error"),
            CatalogError::Durable(e) => assert!(e.can_recover_with_write_mode()),
        }
    }

    // Initialize the catalog.
    {
        let state = state_builder
            .clone()
            .unwrap_build()
            .await
            .open(SYSTEM_TIME().into(), &test_bootstrap_args())
            .await
            .unwrap()
            .0;
        assert_eq!(state.epoch(), Epoch::new(2).expect("known to be non-zero"));
        Box::new(state).expire().await;
    }

    {
        // Open catalog in savepoint mode.
        let mut state = state_builder
            .clone()
            .unwrap_build()
            .await
            .open_savepoint(SYSTEM_TIME().into(), &test_bootstrap_args())
            .await
            .unwrap()
            .0;
        // Drain initial updates.
        let _ = state
            .sync_to_current_updates()
            .await
            .expect("unable to sync");
        // Savepoint catalogs do not increment the epoch.
        assert_eq!(state.epoch(), Epoch::new(2).expect("known to be non-zero"));

        // Perform writes.
        let mut txn = state.transaction().await.unwrap();
        let mut ids = Vec::new();
        let mut db_schemas = Vec::new();
        for i in 0..10 {
            let (db_id, db_oid) = txn
                .insert_user_database(
                    &format!("db{i}"),
                    RoleId::User(i),
                    Vec::new(),
                    &HashSet::new(),
                )
                .unwrap();
            let (schema_id, schema_oid) = txn
                .insert_user_schema(
                    db_id,
                    &format!("sc{i}"),
                    RoleId::User(i),
                    Vec::new(),
                    &HashSet::new(),
                )
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
        // Drain txn updates.
        let _ = txn.get_and_commit_op_updates();
        let commit_ts = txn.upper();
        txn.commit(commit_ts).await.unwrap();

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
        // Drain txn updates.
        let _ = txn.get_and_commit_op_updates();
        let commit_ts = txn.upper();
        txn.commit(commit_ts).await.unwrap();

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
        let mut state = state_builder
            .clone()
            .unwrap_build()
            .await
            .open(SYSTEM_TIME().into(), &test_bootstrap_args())
            .await
            .unwrap()
            .0;
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
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_open_read_only(state_builder).await;
}

async fn test_open_read_only(state_builder: TestCatalogStateBuilder) {
    let state_builder = state_builder.with_default_deploy_generation();

    // Can't open a read-only catalog until it's been initialized.
    let err = state_builder
        .clone()
        .unwrap_build()
        .await
        .open_read_only(&test_bootstrap_args())
        .await
        .unwrap_err();
    match err {
        CatalogError::Catalog(_) => panic!("unexpected catalog error"),
        CatalogError::Durable(e) => assert!(e.can_recover_with_write_mode()),
    }

    // Initialize the catalog.
    let mut state = state_builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;
    // Drain initial updates.
    let _ = state
        .sync_to_current_updates()
        .await
        .expect("unable to sync");
    assert_eq!(state.epoch(), Epoch::new(2).expect("known to be non-zero"));

    let mut read_only_state = state_builder
        .clone()
        .unwrap_build()
        .await
        .open_read_only(&test_bootstrap_args())
        .await
        .unwrap();
    // Read-only catalogs do not increment the epoch.
    assert_eq!(
        read_only_state.epoch(),
        Epoch::new(2).expect("known to be non-zero")
    );
    let commit_ts = state.current_upper().await;
    let err = read_only_state
        .allocate_user_id(commit_ts)
        .await
        .unwrap_err();
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
            &HashSet::new(),
        )
        .unwrap();
    // Drain txn updates.
    let _ = txn.get_and_commit_op_updates();
    let commit_ts = txn.upper();
    txn.commit(commit_ts).await.unwrap();

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
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_open(state_builder).await;
}

async fn test_open(state_builder: TestCatalogStateBuilder) {
    let state_builder = state_builder.with_default_deploy_generation();

    let (snapshot, audit_log) = {
        let mut state = state_builder
            .clone()
            .unwrap_build()
            .await
            // Use `NOW_ZERO` for consistent timestamps in the snapshots.
            .open(NOW_ZERO().into(), &test_bootstrap_args())
            .await
            .unwrap()
            .0;

        assert_eq!(state.epoch(), Epoch::new(2).expect("known to be non-zero"));
        // Check initial snapshot.
        let snapshot = state.snapshot().await.unwrap();
        {
            let test_snapshot = StableSnapshot(&snapshot);

            let user_version = test_snapshot.user_version().unwrap();
            assert_eq!(user_version.value, CATALOG_VERSION);

            let builtin_migration_shard = test_snapshot.builtin_migration_shard().unwrap();
            let _shard_id: ShardId = builtin_migration_shard.value.parse().unwrap();

            let expression_cache_shard = test_snapshot.expression_cache_shard().unwrap();
            let _shard_id: ShardId = expression_cache_shard.value.parse().unwrap();

            insta::assert_debug_snapshot!("initial_snapshot", test_snapshot);
        }
        let audit_log = state.get_audit_logs().await.unwrap();
        insta::assert_debug_snapshot!("initial_audit_log", audit_log);
        Box::new(state).expire().await;
        (snapshot, audit_log)
    };
    // Reopening the catalog will increment the epoch, but shouldn't change the initial snapshot.
    {
        let mut state = state_builder
            .clone()
            .unwrap_build()
            .await
            .open(SYSTEM_TIME().into(), &test_bootstrap_args())
            .await
            .unwrap()
            .0;

        assert_eq!(state.epoch(), Epoch::new(3).expect("known to be non-zero"));
        assert_eq!(state.snapshot().await.unwrap(), snapshot);
        assert_eq!(state.get_audit_logs().await.unwrap(), audit_log);
        Box::new(state).expire().await;
    }
    // Reopen the catalog a third time for good measure.
    {
        let mut state = state_builder
            .clone()
            .unwrap_build()
            .await
            .open(SYSTEM_TIME().into(), &test_bootstrap_args())
            .await
            .unwrap()
            .0;

        assert_eq!(state.epoch(), Epoch::new(4).expect("known to be non-zero"));
        assert_eq!(state.snapshot().await.unwrap(), snapshot);
        assert_eq!(state.get_audit_logs().await.unwrap(), audit_log);
        Box::new(state).expire().await;
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_unopened_deploy_generation_fencing() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_unopened_deploy_generation_fencing(state_builder).await;
}

async fn test_unopened_deploy_generation_fencing(state_builder: TestCatalogStateBuilder) {
    // Initialize catalog.
    let deploy_generation = 0;
    let version = semver::Version::new(0, 1, 0);
    let zdt_deployment_max_wait = Duration::from_millis(666);
    let state_builder = state_builder
        .with_deploy_generation(deploy_generation)
        .with_version(version);
    {
        let mut state = state_builder
            .clone()
            .unwrap_build()
            .await
            .open(SYSTEM_TIME().into(), &test_bootstrap_args())
            .await
            .unwrap()
            .0;
        // drain catalog updates.
        let _ = state.sync_to_current_updates().await.unwrap();
        let mut txn = state.transaction().await.unwrap();
        txn.set_0dt_deployment_max_wait(zdt_deployment_max_wait)
            .unwrap();
        let commit_ts = txn.upper();
        txn.commit(commit_ts).await.unwrap();
    }
    let mut openable_state = state_builder.clone().unwrap_build().await;

    // Read config collection with unopened catalog.
    assert_eq!(
        zdt_deployment_max_wait,
        openable_state
            .get_0dt_deployment_max_wait()
            .await
            .unwrap()
            .unwrap()
    );

    // Open catalog, which will bump the epoch AND deploy generation.
    let _state = state_builder
        .clone()
        .with_deploy_generation(deploy_generation + 1)
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;

    // Unopened catalog should be fenced now with a deploy generation fence.
    let err = openable_state
        .get_deployment_generation()
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            CatalogError::Durable(DurableCatalogError::Fence(
                FenceError::DeployGeneration { .. }
            ))
        ),
        "unexpected err: {err:?}"
    );

    let err = openable_state.is_initialized().await.unwrap_err();
    assert!(
        matches!(
            err,
            CatalogError::Durable(DurableCatalogError::Fence(
                FenceError::DeployGeneration { .. }
            ))
        ),
        "unexpected err: {err:?}"
    );

    // Re-initializing with the old deploy version doesn't work.
    let err = state_builder.clone().build().await.unwrap_err();
    assert!(
        matches!(
            err,
            DurableCatalogError::Fence(FenceError::DeployGeneration { .. })
        ),
        "unexpected err: {err:?}"
    );

    // Re-initializing with the old deploy version and earlier version returns a fence error.
    let err = state_builder
        .with_version(semver::Version::new(0, 0, 0))
        .build()
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            DurableCatalogError::Fence(FenceError::DeployGeneration { .. })
        ),
        "unexpected err: {err:?}"
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_opened_epoch_fencing() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_opened_epoch_fencing(state_builder).await;
}

async fn test_opened_epoch_fencing(state_builder: TestCatalogStateBuilder) {
    // Initialize catalog.
    let state_builder = state_builder.with_default_deploy_generation();
    let mut state = state_builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;

    // Open catalog, which will bump the epoch.
    let _state = state_builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;

    // Opened catalog should be fenced now with an epoch fence.
    let err = state.snapshot().await.unwrap_err();
    assert!(
        matches!(
            err,
            CatalogError::Durable(DurableCatalogError::Fence(FenceError::Epoch { .. }))
        ),
        "unexpected err: {err:?}"
    );

    let err = state.transaction().await.unwrap_err();
    assert!(
        matches!(
            err,
            CatalogError::Durable(DurableCatalogError::Fence(FenceError::Epoch { .. }))
        ),
        "unexpected err: {err:?}"
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_opened_deploy_generation_fencing() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_opened_deploy_generation_fencing(state_builder).await;
}

async fn test_opened_deploy_generation_fencing(state_builder: TestCatalogStateBuilder) {
    let deploy_generation = 0;
    let mut state = state_builder
        .clone()
        .with_deploy_generation(deploy_generation)
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;

    // Open catalog, which will bump the epoch AND deploy generation.
    let _state = state_builder
        .clone()
        .with_deploy_generation(deploy_generation + 1)
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;

    // Opened catalog should be fenced now with an epoch fence.
    let err = state.snapshot().await.unwrap_err();
    assert!(
        matches!(
            err,
            CatalogError::Durable(DurableCatalogError::Fence(
                FenceError::DeployGeneration { .. }
            ))
        ),
        "unexpected err: {err:?}"
    );

    let err = state.transaction().await.unwrap_err();
    assert!(
        matches!(
            err,
            CatalogError::Durable(DurableCatalogError::Fence(
                FenceError::DeployGeneration { .. }
            ))
        ),
        "unexpected err: {err:?}"
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_fencing_during_write() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_fencing_during_write(state_builder).await;
}

async fn test_fencing_during_write(state_builder: TestCatalogStateBuilder) {
    let deploy_generation = 0;
    let mut state = state_builder
        .clone()
        .with_deploy_generation(deploy_generation)
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;
    // Drain updates.
    let _ = state.sync_to_current_updates().await;
    let mut txn = state.transaction().await.unwrap();
    txn.set_config("cmu".to_string(), Some(1900)).unwrap();

    // Open catalog, which will bump the epoch.
    let mut state = state_builder
        .clone()
        .with_deploy_generation(deploy_generation)
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;
    // Drain updates.
    let _ = state.sync_to_current_updates().await;

    // Committing results in an epoch fence error.
    let commit_ts = txn.upper();
    let err = txn.commit(commit_ts).await.unwrap_err();
    assert!(
        matches!(
            err,
            CatalogError::Durable(DurableCatalogError::Fence(FenceError::Epoch { .. }))
        ),
        "unexpected err: {err:?}"
    );

    let mut txn = state.transaction().await.unwrap();
    txn.set_config("wes".to_string(), Some(1831)).unwrap();

    // Open catalog, which will bump the epoch AND deploy generation.
    let _state = state_builder
        .clone()
        .with_deploy_generation(deploy_generation + 1)
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;

    // Committing results in a deploy generation fence error.
    let commit_ts = txn.upper();
    let err = txn.commit(commit_ts).await.unwrap_err();
    assert!(
        matches!(
            err,
            CatalogError::Durable(DurableCatalogError::Fence(
                FenceError::DeployGeneration { .. }
            ))
        ),
        "unexpected err: {err:?}"
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_version_fencing() {
    async fn testcase(catalog: &str, reader: &str, expected: Result<(), ()>) {
        let catalog_version = semver::Version::parse(catalog).unwrap();
        let reader_version = semver::Version::parse(reader).unwrap();
        let organization_id = Uuid::new_v4();
        let deploy_generation = 0;
        let mut persist_cache = PersistClientCache::new_no_metrics();

        persist_cache.cfg.build_version = catalog_version.clone();
        let persist_client = persist_cache
            .open(PersistLocation::new_in_mem())
            .await
            .expect("in-mem location is valid");
        let persist_openable_state = TestCatalogStateBuilder::new(persist_client.clone())
            .with_deploy_generation(deploy_generation)
            .with_organization_id(organization_id)
            .with_version(catalog_version)
            .unwrap_build()
            .await;
        let _persist_state = persist_openable_state
            .open(SYSTEM_TIME().into(), &test_bootstrap_args())
            .await
            .unwrap()
            .0;

        persist_cache.cfg.build_version = reader_version.clone();
        let persist_client = persist_cache
            .open(PersistLocation::new_in_mem())
            .await
            .expect("in-mem location is valid");
        let persist_openable_state = TestCatalogStateBuilder::new(persist_client.clone())
            .with_deploy_generation(deploy_generation)
            .with_organization_id(organization_id)
            .with_version(reader_version)
            .build()
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

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_concurrent_open() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_concurrent_open(state_builder).await;
}

async fn test_concurrent_open(state_builder: TestCatalogStateBuilder) {
    let state_builder = state_builder.with_default_deploy_generation();

    async fn run_state(state: &mut Box<dyn DurableCatalogState>) -> Result<(), CatalogError> {
        let mut i = 0;
        loop {
            // Drain updates.
            let _ = state.sync_to_current_updates().await?;
            let commit_ts = state.current_upper().await;
            state.allocate_user_id(commit_ts).await?;
            // After winning the race 100 times, sleep to give the debug state a chance to win the
            // race.
            if i > 100 {
                tokio::time::sleep(Duration::from_nanos(1)).await;
            }
            i += 1;
        }
    }

    let mut state = state_builder
        .clone()
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;
    let state_handle = mz_ore::task::spawn(|| "state", async move {
        // Eventually this state should get fenced by the open below.
        let err = run_state(&mut state).await.unwrap_err();
        assert!(matches!(
            err,
            CatalogError::Durable(DurableCatalogError::Fence(FenceError::Epoch { .. }))
        ))
    });

    // Open should be successful without retrying, even though there is an active state.
    let _state = state_builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;

    state_handle.await.unwrap();

    // Open again to ensure that we didn't commit an invalid retraction.
    let _state = state_builder
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;
}
