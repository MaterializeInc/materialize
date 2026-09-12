// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![recursion_limit = "256"]

use std::fmt::{Debug, Formatter};

use mz_catalog::durable::debug::{CollectionTrace, ConfigCollection, SettingCollection, Trace};
use mz_catalog::durable::initialize::USER_VERSION_KEY;
use mz_catalog::durable::objects::serialization::proto;
use mz_catalog::durable::{
    BUILTIN_MIGRATION_SHARD_KEY, CATALOG_VERSION, CatalogError, DurableCatalogError,
    EXPRESSION_CACHE_SHARD_KEY, Epoch, FenceError, MOCK_AUTHENTICATION_NONCE_KEY,
    TestCatalogStateBuilder, test_bootstrap_args,
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
            cluster_system_configurations,
            replica_system_configurations,
            system_privileges,
            storage_collection_metadata,
            collection_compaction_bounds,
            maintained_read_requirements,
            client_incarnations,
            client_read_requirements,
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
            .field(
                "cluster_system_configurations",
                cluster_system_configurations,
            )
            .field(
                "replica_system_configurations",
                replica_system_configurations,
            )
            .field("system_privileges", system_privileges)
            .field("storage_collection_metadata", storage_collection_metadata)
            .field("collection_compaction_bounds", collection_compaction_bounds)
            .field("maintained_read_requirements", maintained_read_requirements)
            .field("client_incarnations", client_incarnations)
            .field("client_read_requirements", client_read_requirements)
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
        .unwrap();

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
            false,
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
            false,
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
        .delete::<SettingCollection>(
            proto::SettingKey {
                name: "debug-key".to_string(),
            },
            false,
        )
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

// These integration tests exercise the public mutation boundary with real persist CAS.
// Concurrent tasks cover preservation of unrelated keys, but do not deterministically
// pause between a CAS snapshot and append to force a heartbeat-refresh race.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_persist_debug_live_mutations() {
    for heartbeat in [false, true] {
        for protected in [false, true] {
            test_debug_live_mutations(heartbeat, protected).await;
        }
    }
}

async fn test_debug_live_mutations(heartbeat: bool, protected: bool) {
    tracing::info!(heartbeat, protected, "testing debug mutation safety");
    let builder = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation();
    let mut state = builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let key = proto::ConfigKey {
        key: "debug-key".into(),
    };
    let initial = proto::ConfigValue { value: 666 };
    let edited = proto::ConfigValue { value: 42 };
    state.sync_to_current_updates().await.unwrap();
    let mut txn = state.transaction().await.unwrap();
    txn.set_config(key.key.clone(), Some(initial.value))
        .unwrap();
    if protected {
        txn.set_config("catalog_read_protection_enabled".into(), Some(1))
            .unwrap();
    }
    if heartbeat {
        let id = txn.create_client_incarnation().unwrap();
        assert_eq!(
            txn.publish_client_read_requirements(id, Default::default())
                .unwrap(),
            1
        );
    }
    // The open timestamp is not the publication timestamp. Publish at wall-clock
    // time so the advisory check observes genuinely recent durable activity.
    let ts = txn.upper().max(SYSTEM_TIME().into());
    let _ = txn.get_and_commit_op_updates();
    txn.commit(ts).await.unwrap();

    // A pending deployment generation on the debug handle must not promote.
    let mut debug = builder
        .clone()
        .with_deploy_generation(99)
        .unwrap_build()
        .await
        .open_debug()
        .await
        .unwrap();
    let mut observer = builder.clone().unwrap_build().await;
    let epoch = observer.epoch().await.unwrap();
    let generation = observer.get_deployment_generation().await.unwrap();
    let upper = state.current_upper().await;
    let mut before = observer.trace_consolidated().await.unwrap();
    before.sort();
    for err in [
        debug
            .edit::<ConfigCollection>(key.clone(), edited.clone(), false)
            .await
            .unwrap_err(),
        debug
            .delete::<ConfigCollection>(key.clone(), false)
            .await
            .unwrap_err(),
    ] {
        assert!(
            matches!(
                &err,
                CatalogError::Durable(DurableCatalogError::NotWritable(_))
            ),
            "{err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("--force"), "{message}");
        assert!(
            message.contains(if heartbeat {
                "heartbeat"
            } else {
                "catalog publication"
            }),
            "{message}"
        );
    }
    let mut after = observer.trace_consolidated().await.unwrap();
    after.sort();
    assert_eq!(
        before, after,
        "refusals must leave all catalog contents unchanged"
    );
    assert_eq!(observer.epoch().await.unwrap(), epoch);
    assert_eq!(
        observer.get_deployment_generation().await.unwrap(),
        generation
    );
    assert_eq!(
        state.snapshot().await.unwrap().configs.get(&key),
        Some(&initial)
    );
    assert_eq!(state.current_upper().await, upper);
    assert_ok!(state.transaction().await);

    assert_eq!(
        debug
            .edit::<ConfigCollection>(key.clone(), edited.clone(), true)
            .await
            .unwrap(),
        Some(initial)
    );
    assert_eq!(
        state.snapshot().await.unwrap().configs.get(&key),
        Some(&edited)
    );
    assert_ok!(state.transaction().await);
    assert_eq!(observer.epoch().await.unwrap(), epoch);
    assert_eq!(
        observer.get_deployment_generation().await.unwrap(),
        generation
    );

    debug
        .delete::<ConfigCollection>(key.clone(), true)
        .await
        .unwrap();
    assert_none!(state.snapshot().await.unwrap().configs.get(&key));
    // The serving writer can still commit after both foreign mutations.
    let mut txn = state.transaction().await.unwrap();
    txn.set_config("serving-writer".into(), Some(7)).unwrap();
    let ts = txn.upper();
    txn.commit(ts).await.unwrap();
    assert_eq!(observer.epoch().await.unwrap(), epoch);
    assert_eq!(
        observer.get_deployment_generation().await.unwrap(),
        generation
    );

    // Force bypasses liveness only, not a subsequent epoch or generation fence.
    let _replacement = builder
        .clone()
        .with_deploy_generation(if protected { 1 } else { 0 })
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    for err in [
        debug
            .edit::<ConfigCollection>(key.clone(), edited, true)
            .await
            .unwrap_err(),
        debug
            .delete::<ConfigCollection>(key, true)
            .await
            .unwrap_err(),
    ] {
        assert!(
            if protected {
                matches!(
                    err,
                    CatalogError::Durable(DurableCatalogError::Fence(
                        FenceError::DeployGeneration { .. }
                    ))
                )
            } else {
                matches!(
                    err,
                    CatalogError::Durable(DurableCatalogError::Fence(FenceError::Epoch { .. }))
                )
            },
            "{err:?}"
        );
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_persist_debug_unreclaimed_heartbeat_requires_force() {
    let builder = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation();
    let mut state = builder
        .clone()
        .unwrap_build()
        .await
        .open(NOW_ZERO().into(), &test_bootstrap_args())
        .await
        .unwrap();
    state.sync_to_current_updates().await.unwrap();
    let mut txn = state.transaction().await.unwrap();
    let id = txn.create_client_incarnation().unwrap();
    txn.publish_client_read_requirements(id, Default::default())
        .unwrap();
    let ts = txn.upper();
    let _ = txn.get_and_commit_op_updates();
    txn.commit(ts).await.unwrap();
    let mut observer = builder.clone().unwrap_build().await;
    let epoch = observer.epoch().await.unwrap();
    let generation = observer.get_deployment_generation().await.unwrap();
    state.expire().await;
    let mut debug = builder
        .clone()
        .unwrap_build()
        .await
        .open_debug()
        .await
        .unwrap();
    let key = proto::ConfigKey {
        key: "offline-edit".into(),
    };
    let value = proto::ConfigValue { value: 42 };
    // A catalog timestamp is not a heartbeat lease expiry. Even this stopped
    // writer remains ambiguous to an administrator until reclamation or force.
    let refusal = debug
        .edit::<ConfigCollection>(key.clone(), value.clone(), false)
        .await
        .unwrap_err()
        .to_string();
    assert!(refusal.contains("registered heartbeat"), "{refusal}");
    assert!(refusal.contains("--force"), "{refusal}");
    assert_none!(
        debug
            .edit::<ConfigCollection>(key.clone(), value.clone(), true)
            .await
            .unwrap()
    );
    assert!(
        observer
            .trace_consolidated()
            .await
            .unwrap()
            .configs
            .values
            .iter()
            .any(|((k, v), _, diff)| k == &key && v == &value && *diff == Diff::ONE)
    );
    debug
        .delete::<ConfigCollection>(key.clone(), true)
        .await
        .unwrap();
    assert!(
        !observer
            .trace_consolidated()
            .await
            .unwrap()
            .configs
            .values
            .iter()
            .any(|((k, _), _, _)| k == &key)
    );
    assert_eq!(observer.epoch().await.unwrap(), epoch);
    assert_eq!(
        observer.get_deployment_generation().await.unwrap(),
        generation
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_persist_concurrent_debugs() {
    let builder = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation();
    let mut state = builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let epoch = state.epoch();
    let generation = state.get_deployment_generation().await.unwrap();
    state.sync_to_current_updates().await.unwrap();
    state.allocate_user_id(SYSTEM_TIME().into()).await.unwrap();
    let next_user_id = state.get_next_user_item_id().await.unwrap();
    let mut left = builder
        .clone()
        .unwrap_build()
        .await
        .open_debug()
        .await
        .unwrap();
    let mut right = builder
        .clone()
        .unwrap_build()
        .await
        .open_debug()
        .await
        .unwrap();
    for i in 0..16 {
        let left_key = proto::ConfigKey {
            key: format!("left-{i}"),
        };
        let right_key = proto::ConfigKey {
            key: format!("right-{i}"),
        };
        let value = proto::ConfigValue { value: i };
        // Force is intentional: this catalog has a recent serving writer.
        let (a, b, ()) = tokio::join!(
            left.edit::<ConfigCollection>(left_key.clone(), value.clone(), true),
            right.edit::<ConfigCollection>(right_key.clone(), value.clone(), true),
            async {
                state.sync_to_current_updates().await.unwrap();
                let ts = state.current_upper().await;
                state.allocate_user_id(ts).await.unwrap();
            },
        );
        assert_none!(a.unwrap());
        assert_none!(b.unwrap());
        let configs = state.snapshot().await.unwrap().configs;
        assert_eq!(configs.get(&left_key), Some(&value));
        assert_eq!(configs.get(&right_key), Some(&value));
        let (a, b, ()) = tokio::join!(
            left.delete::<ConfigCollection>(left_key.clone(), true),
            right.edit::<ConfigCollection>(
                right_key.clone(),
                proto::ConfigValue { value: i + 1 },
                true
            ),
            async {
                state.sync_to_current_updates().await.unwrap();
                let ts = state.current_upper().await;
                state.allocate_user_id(ts).await.unwrap();
            },
        );
        a.unwrap();
        assert_eq!(b.unwrap(), Some(value));
        let configs = state.snapshot().await.unwrap().configs;
        assert_none!(configs.get(&left_key));
        assert_eq!(
            configs.get(&right_key),
            Some(&proto::ConfigValue { value: i + 1 })
        );
    }
    let configs = state.snapshot().await.unwrap().configs;
    for i in 0..16 {
        assert_eq!(
            configs.get(&proto::ConfigKey {
                key: format!("right-{i}")
            }),
            Some(&proto::ConfigValue { value: i + 1 })
        );
    }
    assert_ok!(state.transaction().await);
    assert_eq!(
        state.get_next_user_item_id().await.unwrap(),
        next_user_id + 32
    );
    let mut observer = builder.unwrap_build().await;
    assert_eq!(observer.epoch().await.unwrap(), epoch);
    assert_eq!(
        observer.get_deployment_generation().await.unwrap(),
        generation
    );
}

// Runtime identity is enforced at the durable API, even though these records do
// not produce memory updates. Force changes no fencing or promotion semantics.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_persist_bootstrap_identity_recovery() {
    use mz_catalog::durable::debug::TxnWalShardCollection;

    for wal in [false, true] {
        for deletion in [false, true] {
            for boundary in ["sync", "snapshot", "transaction", "commit", "advance"] {
                let builder = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
                    .with_default_deploy_generation();
                let mut state = builder
                    .clone()
                    .unwrap_build()
                    .await
                    .open(SYSTEM_TIME().into(), &test_bootstrap_args())
                    .await
                    .unwrap();
                state.sync_to_current_updates().await.unwrap();
                let key = proto::ConfigKey {
                    key: "catalog_read_protection_enabled".into(),
                };
                let shard = proto::TxnWalShardValue {
                    shard: ShardId::new().to_string(),
                };
                let mut debug = builder
                    .clone()
                    .with_deploy_generation(99)
                    .unwrap_build()
                    .await
                    .open_debug()
                    .await
                    .unwrap();
                debug
                    .edit::<ConfigCollection>(key.clone(), proto::ConfigValue { value: 1 }, true)
                    .await
                    .unwrap();
                debug
                    .edit::<TxnWalShardCollection>((), shard.clone(), true)
                    .await
                    .unwrap();
                // Bootstrap is allowed to consume both identity changes.
                state.snapshot().await.unwrap();
                debug
                    .delete::<ConfigCollection>(key.clone(), true)
                    .await
                    .unwrap();
                debug
                    .delete::<TxnWalShardCollection>((), true)
                    .await
                    .unwrap();
                state.snapshot().await.unwrap();
                debug
                    .edit::<ConfigCollection>(key.clone(), proto::ConfigValue { value: 1 }, true)
                    .await
                    .unwrap();
                debug
                    .edit::<TxnWalShardCollection>((), shard.clone(), true)
                    .await
                    .unwrap();
                state.snapshot().await.unwrap();
                state.mark_bootstrap_complete().await;
                let epoch = state.epoch();
                let generation = state.get_deployment_generation().await.unwrap();
                // Retract/reinsert of the same effective values is not a change.
                debug
                    .edit::<ConfigCollection>(key.clone(), proto::ConfigValue { value: 1 }, true)
                    .await
                    .unwrap();
                debug
                    .edit::<TxnWalShardCollection>((), shard, true)
                    .await
                    .unwrap();
                state.sync_to_current_updates().await.unwrap();
                state.snapshot().await.unwrap();
                // The protection latch interprets every nonzero value as enabled.
                debug
                    .edit::<ConfigCollection>(key.clone(), proto::ConfigValue { value: 2 }, true)
                    .await
                    .unwrap();
                state.sync_to_current_updates().await.unwrap();
                // Intermediate values within an unconsumed prefix do not change
                // the runtime identity if the final effective state is unchanged.
                debug
                    .delete::<ConfigCollection>(key.clone(), true)
                    .await
                    .unwrap();
                debug
                    .edit::<ConfigCollection>(key.clone(), proto::ConfigValue { value: 1 }, true)
                    .await
                    .unwrap();
                state.sync_to_current_updates().await.unwrap();
                let mut txn = state.transaction().await.unwrap();
                txn.set_config("ordinary-write".into(), Some(7)).unwrap();
                let ts = txn.upper();
                let _ = txn.get_and_commit_op_updates();
                // Keep the ordinary transaction open across the foreign edit to
                // exercise observation on a write conflict as well as on reads.
                if wal {
                    if deletion {
                        debug
                            .delete::<TxnWalShardCollection>((), true)
                            .await
                            .unwrap();
                    } else {
                        debug
                            .edit::<TxnWalShardCollection>(
                                (),
                                proto::TxnWalShardValue {
                                    shard: ShardId::new().to_string(),
                                },
                                true,
                            )
                            .await
                            .unwrap();
                    }
                } else if deletion {
                    debug
                        .delete::<ConfigCollection>(key.clone(), true)
                        .await
                        .unwrap();
                } else {
                    debug
                        .edit::<ConfigCollection>(
                            key.clone(),
                            proto::ConfigValue { value: 0 },
                            true,
                        )
                        .await
                        .unwrap();
                }
                let result = if boundary == "commit" {
                    txn.commit(ts).await.map(|_| ())
                } else {
                    drop(txn);
                    match boundary {
                        "sync" => state.sync_to_current_updates().await.map(|_| ()),
                        "snapshot" => state.snapshot().await.map(|_| ()),
                        "transaction" => state.transaction().await.map(|_| ()),
                        "advance" => state.advance_upper(ts.step_forward()).await,
                        _ => unreachable!(),
                    }
                };
                let expected_field = if wal { "TxnWalShard" } else { key.key.as_str() };
                assert!(
                    matches!(result, Err(CatalogError::Durable(
                    DurableCatalogError::RestartRequired { field }
                )) if field == expected_field),
                    "wal={wal} deletion={deletion} {boundary}: {result:?}"
                );
                // Cached upper progress and repeated marking must not let a stale
                // runtime continue after it has observed the change.
                state.mark_bootstrap_complete().await;
                let upper = state.current_upper().await;
                assert!(matches!(
                    state.advance_upper(upper).await,
                    Err(CatalogError::Durable(
                        DurableCatalogError::RestartRequired { .. }
                    ))
                ));
                assert_eq!(state.epoch(), epoch);
                // An unmarked diagnostic reader can consume the edited identity.
                let mut diagnostic = builder
                    .clone()
                    .unwrap_build()
                    .await
                    .open_read_only(&test_bootstrap_args())
                    .await
                    .unwrap();
                diagnostic.snapshot().await.unwrap();
                assert_eq!(
                    diagnostic.get_deployment_generation().await.unwrap(),
                    generation
                );
                diagnostic.expire().await;
                // A later generation fence takes precedence over recovery.
                let replacement = builder
                    .with_deploy_generation(1)
                    .unwrap_build()
                    .await
                    .open(SYSTEM_TIME().into(), &test_bootstrap_args())
                    .await
                    .unwrap();
                assert!(matches!(
                    state.sync_to_current_updates().await,
                    Err(CatalogError::Durable(DurableCatalogError::Fence(
                        FenceError::DeployGeneration { .. }
                    )))
                ));
                replacement.expire().await;
                state.expire().await;
            }
        }
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_bootstrap_setting_change_requires_recovery() {
    let builder = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation();
    let mut state = builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let mut admin = builder.unwrap_build().await.open_debug().await.unwrap();
    let key = proto::SettingKey {
        name: "mock_authentication_nonce".into(),
    };
    let value = proto::SettingValue {
        value: "bootstrap nonce".into(),
    };
    admin
        .edit::<SettingCollection>(key.clone(), value.clone(), true)
        .await
        .unwrap();
    state.sync_to_current_updates().await.unwrap();
    state.mark_bootstrap_complete().await;
    let epoch = state.epoch();
    admin
        .edit::<SettingCollection>(key.clone(), value, true)
        .await
        .unwrap();
    state.sync_to_current_updates().await.unwrap();
    admin.delete::<SettingCollection>(key, true).await.unwrap();
    assert!(matches!(
        state.transaction().await,
        Err(CatalogError::Durable(
            DurableCatalogError::RestartRequired { field: "Setting" }
        ))
    ));
    assert_eq!(state.epoch(), epoch);
    state.expire().await;
}
