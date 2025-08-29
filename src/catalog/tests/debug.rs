// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{Debug, Formatter};
use std::time::Duration;

use mz_catalog::durable::debug::{CollectionTrace, ConfigCollection, SettingCollection, Trace};
use mz_catalog::durable::initialize::USER_VERSION_KEY;
use mz_catalog::durable::objects::serialization::proto;
use mz_catalog::durable::{
    BUILTIN_MIGRATION_SHARD_KEY, CATALOG_VERSION, CatalogError, DurableCatalogError,
    DurableCatalogState, EXPRESSION_CACHE_SHARD_KEY, Epoch, FenceError,
    MOCK_AUTHENTICATION_NONCE_KEY, TestCatalogStateBuilder, test_bootstrap_args,
};
use mz_ore::now::{NOW_ZERO, SYSTEM_TIME};
use mz_ore::{assert_none, assert_ok};
use mz_persist_client::PersistClient;
use mz_persist_types::ShardId;
use mz_repr::{Diff, Timestamp};

/// A new type for [`Trace`] that excludes fields that change often from the debug output. It's
/// useful to print the contents excluding these fields to avoid having to update the expected value
/// in tests.
struct StableTrace<'a>(&'a Trace);

impl StableTrace<'_> {
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

    fn builtin_migration_shard(
        &self,
    ) -> Option<&((proto::SettingKey, proto::SettingValue), Timestamp, Diff)> {
        self.0
            .settings
            .values
            .iter()
            .find(|value| Self::is_builtin_migration_shard(value))
    }

    fn is_builtin_migration_shard(
        ((key, _), _, _): &((proto::SettingKey, proto::SettingValue), Timestamp, Diff),
    ) -> bool {
        key.name == BUILTIN_MIGRATION_SHARD_KEY
    }

    fn expression_cache_shard(
        &self,
    ) -> Option<&((proto::SettingKey, proto::SettingValue), Timestamp, Diff)> {
        self.0
            .settings
            .values
            .iter()
            .find(|value| Self::is_expression_cache_shard(value))
    }

    fn is_expression_cache_shard(
        ((key, _), _, _): &((proto::SettingKey, proto::SettingValue), Timestamp, Diff),
    ) -> bool {
        key.name == EXPRESSION_CACHE_SHARD_KEY
    }

    fn is_mock_authentication_nonce(
        ((key, _), _, _): &((proto::SettingKey, proto::SettingValue), Timestamp, Diff),
    ) -> bool {
        key.name == MOCK_AUTHENTICATION_NONCE_KEY
    }
}

impl Debug for StableTrace<'_> {
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
            network_policies,
            roles,
            role_auth,
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
        let settings: CollectionTrace<SettingCollection> = CollectionTrace {
            values: settings
                .values
                .iter()
                .filter(|value| {
                    !Self::is_builtin_migration_shard(value)
                        && !Self::is_expression_cache_shard(value)
                        && !Self::is_mock_authentication_nonce(value)
                })
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
            .field("network_policies", network_policies)
            .field("roles", roles)
            .field("role_auth", role_auth)
            .field("schemas", schemas)
            .field("settings", &settings)
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

async fn test_debug(state_builder: TestCatalogStateBuilder) {
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
        .open(NOW_ZERO().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;

    // Check epoch
    let mut openable_state2 = state_builder.clone().unwrap_build().await;
    let epoch = openable_state2.epoch().await.unwrap();
    assert_eq!(Epoch::new(2).unwrap(), epoch);

    // Check opened trace.
    let mut unconsolidated_trace = openable_state2.trace_unconsolidated().await.unwrap();
    unconsolidated_trace.sort();
    {
        let test_trace = StableTrace(&unconsolidated_trace);
        let expected_ts = Timestamp::new(2);

        let ((user_version_key, user_version_value), user_version_ts, user_version_diff) =
            test_trace.user_version().unwrap();
        assert_eq!(user_version_key.key, USER_VERSION_KEY);
        assert_eq!(user_version_value.value, CATALOG_VERSION);
        assert_eq!(user_version_ts, &expected_ts);
        assert_eq!(*user_version_diff, Diff::ONE);

        let (
            (builtin_migration_shard_key, builtin_migration_shard_value),
            builtin_migration_shard_ts,
            builtin_migration_shard_diff,
        ) = test_trace.builtin_migration_shard().unwrap();
        assert_eq!(
            builtin_migration_shard_key.name,
            BUILTIN_MIGRATION_SHARD_KEY
        );
        let _shard_id: ShardId = builtin_migration_shard_value.value.parse().unwrap();
        assert_eq!(builtin_migration_shard_ts, &expected_ts);
        assert_eq!(*builtin_migration_shard_diff, Diff::ONE);

        let (
            (expression_cache_shard_key, expression_cache_shard_value),
            expression_cache_shard_ts,
            expression_cache_shard_diff,
        ) = test_trace.expression_cache_shard().unwrap();
        assert_eq!(expression_cache_shard_key.name, EXPRESSION_CACHE_SHARD_KEY);
        let _shard_id: ShardId = expression_cache_shard_value.value.parse().unwrap();
        assert_eq!(expression_cache_shard_ts, &expected_ts);
        assert_eq!(*expression_cache_shard_diff, Diff::ONE);

        insta::assert_debug_snapshot!("opened_trace".to_string(), test_trace);
    }

    let mut debug_state = openable_state2.open_debug().await.unwrap();

    let mut openable_state_reader = state_builder.clone().unwrap_build().await;
    let mut unconsolidated_trace2 = openable_state_reader.trace_unconsolidated().await.unwrap();
    unconsolidated_trace2.sort();
    assert_eq!(
        unconsolidated_trace2, unconsolidated_trace,
        "opening a debug catalog should not modify the contents"
    );

    // Check adding a new value via `edit`.
    let settings = unconsolidated_trace.settings.values;
    assert_eq!(settings.len(), 4);

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
    assert_eq!(settings.len(), 5);
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
    assert_eq!(diff, Diff::ONE);

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
    assert_eq!(settings.len(), 5);
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
    assert_eq!(diff, Diff::ONE);

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
    assert_eq!(settings.len(), 4);

    let consolidated_trace = openable_state_reader.trace_consolidated().await.unwrap();
    let settings = consolidated_trace.settings.values;
    assert_eq!(settings.len(), 4);
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_debug_edit_fencing() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_debug_edit_fencing(state_builder).await;
}

async fn test_debug_edit_fencing(state_builder: TestCatalogStateBuilder) {
    let mut state = state_builder
        .clone()
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;

    let mut debug_state = state_builder
        .clone()
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

    // Open another state to fence the debug state.
    let _state = state_builder
        .clone()
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;

    // Now debug state should be fenced.
    let err = debug_state
        .edit::<SettingCollection>(
            proto::SettingKey {
                name: "joe".to_string(),
            },
            proto::SettingValue {
                value: "koshakow".to_string(),
            },
        )
        .await
        .unwrap_err();
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

async fn test_debug_delete_fencing(state_builder: TestCatalogStateBuilder) {
    let mut state = state_builder
        .clone()
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;
    // Drain state updates.
    let _ = state.sync_to_current_updates().await;

    let mut txn = state.transaction().await.unwrap();
    txn.set_config("joe".to_string(), Some(666)).unwrap();
    let commit_ts = txn.upper();
    txn.commit(commit_ts).await.unwrap();

    let mut debug_state = state_builder
        .clone()
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

    // Open another state to fence the debug state.
    let _state = state_builder
        .clone()
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;

    // Now debug state should be fenced.
    let err = debug_state
        .delete::<SettingCollection>(proto::SettingKey {
            name: "joe".to_string(),
        })
        .await
        .unwrap_err();
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
async fn test_persist_concurrent_debugs() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_concurrent_debugs(state_builder).await;
}

async fn test_concurrent_debugs(state_builder: TestCatalogStateBuilder) {
    let key = proto::ConfigKey {
        key: "mz".to_string(),
    };
    let value = proto::ConfigValue { value: 42 };

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
        // Eventually this state should get fenced by the edit below.
        let err = run_state(&mut state).await.unwrap_err();
        assert!(matches!(
            err,
            CatalogError::Durable(DurableCatalogError::Fence(FenceError::Epoch { .. }))
        ))
    });

    // Edit should be successful without retrying, even though there is an active state.
    let mut debug_state = state_builder
        .clone()
        .unwrap_build()
        .await
        .open_debug()
        .await
        .unwrap();
    debug_state
        .edit::<ConfigCollection>(key.clone(), value.clone())
        .await
        .unwrap();

    state_handle.await.unwrap();

    let mut state = state_builder
        .clone()
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;
    let configs = state.snapshot().await.unwrap().configs;
    assert_eq!(configs.get(&key).unwrap(), &value);

    let state_handle = mz_ore::task::spawn(|| "state", async move {
        // Eventually this state should get fenced by the delete below.
        let err = run_state(&mut state).await.unwrap_err();
        assert!(matches!(
            err,
            CatalogError::Durable(DurableCatalogError::Fence(FenceError::Epoch { .. }))
        ))
    });

    // Delete should be successful without retrying, even though there is an active state.
    let mut debug_state = state_builder
        .clone()
        .unwrap_build()
        .await
        .open_debug()
        .await
        .unwrap();
    debug_state
        .delete::<ConfigCollection>(key.clone())
        .await
        .unwrap();

    state_handle.await.unwrap();

    let configs = state_builder
        .clone()
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0
        .snapshot()
        .await
        .unwrap()
        .configs;
    assert_none!(configs.get(&key));
}
