// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_catalog::durable::debug::{CollectionTrace, ConfigCollection, SettingCollection, Trace};
use mz_catalog::durable::initialize::USER_VERSION_KEY;
use mz_catalog::durable::objects::serialization::proto;
use mz_catalog::durable::{
    test_bootstrap_args, test_persist_backed_catalog_state, CatalogError, DurableCatalogError,
    Epoch, OpenableDurableCatalogState, CATALOG_VERSION,
};
use mz_ore::collections::CollectionExt;
use mz_ore::now::NOW_ZERO;
use mz_persist_client::PersistClient;
use mz_repr::Diff;
use std::fmt::{Debug, Formatter};
use uuid::Uuid;

/// A new type for [`Trace`] that excludes the user_version from the debug output. The user_version
/// changes frequently, so it's useful to print the contents excluding the user_version to avoid
/// having to update the expected value in tests.
struct HiddenUserVersionTrace<'a>(&'a Trace);

impl HiddenUserVersionTrace<'_> {
    fn user_version(&self) -> Option<&((proto::ConfigKey, proto::ConfigValue), String, Diff)> {
        self.0
            .configs
            .values
            .iter()
            .find(|value| Self::is_user_version(value))
    }

    fn is_user_version(
        ((key, _), _, _): &((proto::ConfigKey, proto::ConfigValue), String, Diff),
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
            storage_usage,
            system_object_mappings,
            system_configurations,
            system_privileges,
            timestamps,
            storage_metadata,
            unfinalized_shards,
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
            .field("storage_usage", storage_usage)
            .field("system_object_mappings", system_object_mappings)
            .field("system_configurations", system_configurations)
            .field("system_privileges", system_privileges)
            .field("timestamps", timestamps)
            .field("storage_metadata", storage_metadata)
            .field("unfinalized_shards", unfinalized_shards)
            .finish()
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_debug() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let persist_openable_state1 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state2 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state3 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;

    test_debug(
        persist_openable_state1,
        persist_openable_state2,
        persist_openable_state3,
    )
    .await;
}

async fn test_debug(
    mut openable_state1: impl OpenableDurableCatalogState,
    mut openable_state2: impl OpenableDurableCatalogState,
    mut openable_state3: impl OpenableDurableCatalogState,
) {
    // Check initial empty trace.
    let err = openable_state1.trace().await.unwrap_err();
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
    let _ = Box::new(openable_state1)
        .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
        .await
        .unwrap();

    // Check epoch
    let epoch = openable_state2.epoch().await.unwrap();
    assert_eq!(Epoch::new(2).unwrap(), epoch);

    // Check opened trace.
    let trace = openable_state2.trace().await.unwrap();
    {
        let test_trace = HiddenUserVersionTrace(&trace);
        let ((user_version_key, user_version_value), user_version_ts, user_version_diff) =
            test_trace.user_version().unwrap();
        assert_eq!(user_version_key.key, USER_VERSION_KEY);
        assert_eq!(user_version_value.value, CATALOG_VERSION);
        let expected_ts = "2";
        assert_eq!(user_version_ts, expected_ts);
        assert_eq!(user_version_diff, &1);
        insta::assert_debug_snapshot!("opened_trace".to_string(), test_trace);
    }

    let mut debug_state = Box::new(openable_state2).open_debug().await.unwrap();

    assert_eq!(
        openable_state3.trace().await.unwrap(),
        trace,
        "opening a debug catalog should not modify the contents"
    );

    // Check adding a new value via `edit`.
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
    let trace = openable_state3.trace().await.unwrap();
    let mut settings = trace.settings.values;
    differential_dataflow::consolidation::consolidate_updates(&mut settings);
    assert_eq!(settings.len(), 1);
    let ((key, value), _ts, diff) = settings.into_element();
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
    let trace = openable_state3.trace().await.unwrap();
    let mut settings = trace.settings.values;
    differential_dataflow::consolidation::consolidate_updates(&mut settings);
    assert_eq!(settings.len(), 1);
    let ((key, value), _ts, diff) = settings.into_element();
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
    let trace = openable_state3.trace().await.unwrap();
    let mut settings = trace.settings.values;
    differential_dataflow::consolidation::consolidate_updates(&mut settings);
    assert_eq!(settings.len(), 0);
}
