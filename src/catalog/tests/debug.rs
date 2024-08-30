// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{Debug, Formatter};

use mz_catalog::durable::debug::{CollectionTrace, ConfigCollection, SettingCollection, Trace};
use mz_catalog::durable::initialize::USER_VERSION_KEY;
use mz_catalog::durable::objects::serialization::proto;
use mz_catalog::durable::{
    test_bootstrap_args, CatalogError, DurableCatalogError, Epoch, FenceError,
    TestCatalogStateBuilder, CATALOG_VERSION,
};
use mz_ore::assert_ok;
use mz_ore::now::{NOW_ZERO, SYSTEM_TIME};
use mz_persist_client::PersistClient;
use mz_repr::{Diff, Timestamp};

/// A new type for [`Trace`] that excludes the user_version from the debug output. The user_version
/// changes frequently, so it's useful to print the contents excluding the user_version to avoid
/// having to update the expected value in tests.
struct HiddenUserVersionTrace<'a>(&'a Trace);

impl HiddenUserVersionTrace<'_> {
    fn user_version(&self) -> Option<&((proto::ConfigKey, proto::ConfigValue), Timestamp, Diff)> {
        self.0
            .configs
            .values
            .iter()
            .find(|value| Self::is_user_version(value))
    }

    fn is_user_version(
        ((key, _), _, _): &((proto::ConfigKey, proto::ConfigValue), Timestamp, Diff),
    ) -> bool {
        key.key == USER_VERSION_KEY
    }
}

impl Debug for HiddenUserVersionTrace<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Trace {
            audit_log,
            clusters,
            introspection_sources,
            cluster_replicas,
            comments,
            configs,
            databases,
            default_privileges,
            id_allocator,
            items,
            roles,
            schemas,
            settings,
            source_references,
            system_object_mappings,
            system_configurations,
            system_privileges,
            storage_collection_metadata,
            unfinalized_shards,
            txn_wal_shard,
        } = self.0;
        let configs: CollectionTrace<ConfigCollection> = CollectionTrace {
            values: configs
                .values
                .iter()
                .filter(|value| !Self::is_user_version(value))
                .cloned()
                .collect(),
        };
        f.debug_struct("Trace")
            .field("audit_log", audit_log)
            .field("clusters", clusters)
            .field("introspection_sources", introspection_sources)
            .field("cluster_replicas", cluster_replicas)
            .field("comments", comments)
            .field("configs", &configs)
            .field("databases", databases)
            .field("default_privileges", default_privileges)
            .field("id_allocator", id_allocator)
            .field("items", items)
            .field("roles", roles)
            .field("schemas", schemas)
            .field("settings", settings)
            .field("source_references", source_references)
            .field("system_object_mappings", system_object_mappings)
            .field("system_configurations", system_configurations)
            .field("system_privileges", system_privileges)
            .field("storage_collection_metadata", storage_collection_metadata)
            .field("unfinalized_shards", unfinalized_shards)
            .field("txn_wal_shard", txn_wal_shard)
            .finish()
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_debug() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_debug(state_builder).await;
}

async fn test_debug<'a>(state_builder: TestCatalogStateBuilder) {
    let state_builder = state_builder.with_default_deploy_generation();
    let mut openable_state1 = state_builder.clone().unwrap_build().await;
    // Check initial empty trace.
    let err = openable_state1.trace_unconsolidated().await.unwrap_err();
    assert_eq!(
        err.to_string(),
        CatalogError::Durable(DurableCatalogError::Uninitialized).to_string()
    );

    // Check initial epoch.
    let err = openable_state1.epoch().await.unwrap_err();
    assert_eq!(
        err.to_string(),
        CatalogError::Durable(DurableCatalogError::Uninitialized).to_string()
    );

    // Use `NOW_ZERO` for consistent timestamps in the snapshots.
    let _ = openable_state1
        .open(NOW_ZERO(), &test_bootstrap_args())
        .await
        .unwrap();

    // Check epoch
    let mut openable_state2 = state_builder.clone().unwrap_build().await;
    let epoch = openable_state2.epoch().await.unwrap();
    assert_eq!(Epoch::new(2).unwrap(), epoch);

    // Check opened trace.
    let unconsolidated_trace = openable_state2.trace_unconsolidated().await.unwrap();
    {
        let test_trace = HiddenUserVersionTrace(&unconsolidated_trace);
        let ((user_version_key, user_version_value), user_version_ts, user_version_diff) =
            test_trace.user_version().unwrap();
        assert_eq!(user_version_key.key, USER_VERSION_KEY);
        assert_eq!(user_version_value.value, CATALOG_VERSION);
        let expected_ts = Timestamp::new(2);
        assert_eq!(user_version_ts, &expected_ts);
        assert_eq!(user_version_diff, &1);
        insta::assert_debug_snapshot!("opened_trace".to_string(), test_trace);
    }

    let mut debug_state = openable_state2.open_debug().await.unwrap();

    let mut openable_state_reader = state_builder.clone().unwrap_build().await;
    assert_eq!(
        openable_state_reader.trace_unconsolidated().await.unwrap(),
        unconsolidated_trace,
        "opening a debug catalog should not modify the contents"
    );

    // Check adding a new value via `edit`.
    let settings = unconsolidated_trace.settings.values;
    assert_eq!(settings.len(), 1);

    let prev = debug_state
        .edit::<SettingCollection>(
            proto::SettingKey {
                name: "debug-key".to_string(),
            },
            proto::SettingValue {
                value: "initial".to_string(),
            },
        )
        .await
        .unwrap();
    assert_eq!(prev, None);
    let mut openable_state_reader = state_builder.clone().unwrap_build().await;
    let unconsolidated_trace = openable_state_reader.trace_unconsolidated().await.unwrap();
    let mut settings = unconsolidated_trace.settings.values;
    differential_dataflow::consolidation::consolidate_updates(&mut settings);
    assert_eq!(settings.len(), 2);
    let ((key, value), _ts, diff) = settings
        .into_iter()
        .find(|((key, _), _, _)| key.name == "debug-key")
        .unwrap();
    assert_eq!(
        key,
        proto::SettingKey {
            name: "debug-key".to_string(),
        }
    );
    assert_eq!(
        value,
        proto::SettingValue {
            value: "initial".to_string(),
        },
    );
    assert_eq!(diff, 1);

    // Check modifying an existing value via `edit`.
    let prev = debug_state
        .edit::<SettingCollection>(
            proto::SettingKey {
                name: "debug-key".to_string(),
            },
            proto::SettingValue {
                value: "final".to_string(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        prev,
        Some(proto::SettingValue {
            value: "initial".to_string(),
        })
    );
    let mut openable_state_reader = state_builder.clone().unwrap_build().await;
    let unconsolidated_trace = openable_state_reader.trace_unconsolidated().await.unwrap();
    let mut settings = unconsolidated_trace.settings.values;
    differential_dataflow::consolidation::consolidate_updates(&mut settings);
    assert_eq!(settings.len(), 2);
    let ((key, value), _ts, diff) = settings
        .into_iter()
        .find(|((key, _), _, _)| key.name == "debug-key")
        .unwrap();
    assert_eq!(
        key,
        proto::SettingKey {
            name: "debug-key".to_string(),
        }
    );
    assert_eq!(
        value,
        proto::SettingValue {
            value: "final".to_string(),
        },
    );
    assert_eq!(diff, 1);

    // Check deleting a value via `delete`.
    debug_state
        .delete::<SettingCollection>(proto::SettingKey {
            name: "debug-key".to_string(),
        })
        .await
        .unwrap();
    let mut openable_state_reader = state_builder.clone().unwrap_build().await;
    let unconsolidated_trace = openable_state_reader.trace_unconsolidated().await.unwrap();
    let mut settings = unconsolidated_trace.settings.values;
    differential_dataflow::consolidation::consolidate_updates(&mut settings);
    assert_eq!(settings.len(), 1);

    let consolidated_trace = openable_state_reader.trace_consolidated().await.unwrap();
    let settings = consolidated_trace.settings.values;
    assert_eq!(settings.len(), 1);
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_debug_edit_fencing() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_debug_edit_fencing(state_builder).await;
}

async fn test_debug_edit_fencing<'a>(state_builder: TestCatalogStateBuilder) {
    let mut state = state_builder
        .clone()
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME(), &test_bootstrap_args())
        .await
        .unwrap();

    let mut debug_state = state_builder
        .unwrap_build()
        .await
        .open_debug()
        .await
        .unwrap();

    // State isn't fenced yet.
    assert_ok!(state.snapshot().await);
    assert_ok!(state.transaction().await);

    // Edit catalog via debug_state.
    debug_state
        .edit::<SettingCollection>(
            proto::SettingKey {
                name: "joe".to_string(),
            },
            proto::SettingValue {
                value: "koshakow".to_string(),
            },
        )
        .await
        .unwrap();

    // Now state should be fenced.
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
async fn test_persist_debug_delete_fencing() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_debug_delete_fencing(state_builder).await;
}

async fn test_debug_delete_fencing<'a>(state_builder: TestCatalogStateBuilder) {
    let mut state = state_builder
        .clone()
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME(), &test_bootstrap_args())
        .await
        .unwrap();
    // Drain state updates.
    let _ = state.sync_to_current_updates().await;

    let mut txn = state.transaction().await.unwrap();
    txn.set_config("joe".to_string(), Some(666)).unwrap();
    txn.commit().await.unwrap();

    let mut debug_state = state_builder
        .unwrap_build()
        .await
        .open_debug()
        .await
        .unwrap();

    // State isn't fenced yet.
    assert_ok!(state.snapshot().await);
    assert_ok!(state.transaction().await);

    // Delete from catalog via debug_state.
    debug_state
        .delete::<SettingCollection>(proto::SettingKey {
            name: "joe".to_string(),
        })
        .await
        .unwrap();

    // Now state should be fenced.
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
