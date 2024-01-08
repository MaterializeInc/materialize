// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_catalog::durable::debug::SettingCollection;
use mz_catalog::durable::objects::serialization::proto;
use mz_catalog::durable::{
    test_bootstrap_args, test_persist_backed_catalog_state, test_stash_backed_catalog_state,
    CatalogError, DurableCatalogError, OpenableDurableCatalogState,
};
use mz_ore::collections::CollectionExt;
use mz_ore::now::NOW_ZERO;
use mz_persist_client::PersistClient;
use mz_stash::DebugStashFactory;
use uuid::Uuid;

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_debug() {
    let debug_factory = DebugStashFactory::new().await;
    let debug_openable_state1 = test_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state2 = test_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state3 = test_stash_backed_catalog_state(&debug_factory);
    test_debug(
        "stash",
        debug_openable_state1,
        debug_openable_state2,
        debug_openable_state3,
    )
    .await;
    debug_factory.drop().await;
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
        "persist",
        persist_openable_state1,
        persist_openable_state2,
        persist_openable_state3,
    )
    .await;
}

async fn test_debug(
    catalog_kind: &str,
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

    // Use `NOW_ZERO` for consistent timestamps in the snapshots.
    let _ = Box::new(openable_state1)
        .open(NOW_ZERO(), &test_bootstrap_args(), None)
        .await
        .unwrap();

    // Check opened trace.
    let trace = openable_state2.trace().await.unwrap();
    insta::assert_debug_snapshot!(format!("{catalog_kind}_opened_trace"), trace);

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
