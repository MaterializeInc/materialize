// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// This file is derived from the sqlparser-rs project, available at
// https://github.com/andygrove/sqlparser-rs. It was incorporated
// directly into Materialize on December 21, 2019.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(unknown_lints)]
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![allow(clippy::drain_collect)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

use mz_catalog::{
    debug_bootstrap_args, debug_stash_backed_catalog_state, persist_backed_catalog_state,
    stash_backed_catalog_state, CatalogError, Epoch, OpenableDurableCatalogState, StashConfig,
};
use mz_ore::now::{NOW_ZERO, SYSTEM_TIME};
use mz_persist_client::PersistClient;
use mz_repr::role_id::RoleId;
use mz_stash::DebugStashFactory;
use uuid::Uuid;

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_is_initialized() {
    let (debug_factory, stash_config) = stash_config().await;
    let openable_state1 = stash_backed_catalog_state(stash_config.clone());
    let openable_state2 = stash_backed_catalog_state(stash_config);
    test_is_initialized(openable_state1, openable_state2).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_debug_stash_is_initialized() {
    let debug_factory = DebugStashFactory::new().await;
    let debug_openable_state1 = debug_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state2 = debug_stash_backed_catalog_state(&debug_factory);
    test_is_initialized(debug_openable_state1, debug_openable_state2).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_is_initialized() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let persist_openable_state1 =
        persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state2 =
        persist_backed_catalog_state(persist_client, organization_id).await;
    test_is_initialized(persist_openable_state1, persist_openable_state2).await;
}

async fn test_is_initialized(
    mut openable_state1: impl OpenableDurableCatalogState,
    mut openable_state2: impl OpenableDurableCatalogState,
) {
    assert!(
        !openable_state1.is_initialized().await.unwrap(),
        "catalog has not been opened yet"
    );

    let state = Box::new(openable_state1)
        .open(SYSTEM_TIME.clone(), &debug_bootstrap_args(), None)
        .await
        .unwrap();
    state.expire().await;

    assert!(
        openable_state2.is_initialized().await.unwrap(),
        "catalog has been opened yet"
    );
    // Check twice because some implementations will cache a read-only stash.
    assert!(
        openable_state2.is_initialized().await.unwrap(),
        "catalog has been opened yet"
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_get_deployment_generation() {
    let (debug_factory, stash_config) = stash_config().await;
    let openable_state1 = stash_backed_catalog_state(stash_config.clone());
    let openable_state2 = stash_backed_catalog_state(stash_config);
    test_get_deployment_generation(openable_state1, openable_state2).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_debug_stash_get_deployment_generation() {
    let debug_factory = DebugStashFactory::new().await;
    let debug_openable_state1 = debug_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state2 = debug_stash_backed_catalog_state(&debug_factory);
    test_get_deployment_generation(debug_openable_state1, debug_openable_state2).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_get_deployment_generation() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let persist_openable_state1 =
        persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state2 =
        persist_backed_catalog_state(persist_client, organization_id).await;
    test_get_deployment_generation(persist_openable_state1, persist_openable_state2).await;
}

async fn test_get_deployment_generation(
    mut openable_state1: impl OpenableDurableCatalogState,
    mut openable_state2: impl OpenableDurableCatalogState,
) {
    assert_eq!(
        openable_state1.get_deployment_generation().await.unwrap(),
        None,
        "deployment generation has not been set"
    );

    let state = Box::new(openable_state1)
        .open(SYSTEM_TIME.clone(), &debug_bootstrap_args(), Some(42))
        .await
        .unwrap();
    state.expire().await;

    assert_eq!(
        openable_state2.get_deployment_generation().await.unwrap(),
        Some(42),
        "deployment generation has been set to 42"
    );
    // Check twice because some implementations will cache a read-only stash.
    assert_eq!(
        openable_state2.get_deployment_generation().await.unwrap(),
        Some(42),
        "deployment generation has been set to 42"
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_open_savepoint() {
    let (debug_factory, stash_config) = stash_config().await;
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
    let debug_openable_state1 = debug_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state2 = debug_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state3 = debug_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state4 = debug_stash_backed_catalog_state(&debug_factory);
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
        persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state2 =
        persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state3 =
        persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state4 =
        persist_backed_catalog_state(persist_client, organization_id).await;
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
            .open_savepoint(SYSTEM_TIME.clone(), &debug_bootstrap_args(), None)
            .await
            .unwrap_err();
        match err {
            CatalogError::Catalog(_) => panic!("unexpected catalog error"),
            CatalogError::Durable(e) => assert!(e.can_recover_with_write_mode()),
        }

        // Initialize the stash.
        {
            let mut state = Box::new(openable_state2)
                .open(SYSTEM_TIME.clone(), &debug_bootstrap_args(), None)
                .await
                .unwrap();
            assert_eq!(state.epoch(), Epoch::new(2).expect("known to be non-zero"));
            Box::new(state).expire().await;
        }

        // Open catalog in check mode.
        let mut state = Box::new(openable_state3)
            .open_savepoint(SYSTEM_TIME.clone(), &debug_bootstrap_args(), None)
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
            .get_databases()
            .await
            .unwrap()
            .into_iter()
            .find(|db| db.name == "db");
        assert!(db.is_some(), "database should exist");

        Box::new(state).expire().await;
    }

    {
        // Open catalog normally.
        let mut state = Box::new(openable_state4)
            .open(SYSTEM_TIME.clone(), &debug_bootstrap_args(), None)
            .await
            .unwrap();
        // Write should not have persisted.
        let db = state
            .get_databases()
            .await
            .unwrap()
            .into_iter()
            .find(|db| db.name == "db");
        assert!(db.is_none(), "database should not exist");
        Box::new(state).expire().await;
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_open_read_only() {
    let (debug_factory, stash_config) = stash_config().await;
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
    let debug_openable_state1 = debug_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state2 = debug_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state3 = debug_stash_backed_catalog_state(&debug_factory);
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
        persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state2 =
        persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state3 =
        persist_backed_catalog_state(persist_client, organization_id).await;
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
    // Can't open a read-only stash until it's been initialized.
    let err = Box::new(openable_state1)
        .open_read_only(SYSTEM_TIME.clone(), &debug_bootstrap_args())
        .await
        .unwrap_err();
    match err {
        CatalogError::Catalog(_) => panic!("unexpected catalog error"),
        CatalogError::Durable(e) => assert!(e.can_recover_with_write_mode()),
    }

    // Initialize the stash.
    {
        let mut state = Box::new(openable_state2)
            .open(SYSTEM_TIME.clone(), &debug_bootstrap_args(), None)
            .await
            .unwrap();
        assert_eq!(state.epoch(), Epoch::new(2).expect("known to be non-zero"));
        Box::new(state).expire().await;
    }

    let mut state = Box::new(openable_state3)
        .open_read_only(SYSTEM_TIME.clone(), &debug_bootstrap_args())
        .await
        .unwrap();
    // Read-only catalogs do not increment the epoch.
    assert_eq!(state.epoch(), Epoch::new(2).expect("known to be non-zero"));
    let err = state.set_deploy_generation(42).await.unwrap_err();
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
    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_open() {
    let (debug_factory, stash_config) = stash_config().await;
    let openable_state1 = stash_backed_catalog_state(stash_config.clone());
    let openable_state2 = stash_backed_catalog_state(stash_config);
    test_open(openable_state1, openable_state2).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_debug_stash_open() {
    let debug_factory = DebugStashFactory::new().await;
    let debug_openable_state1 = debug_stash_backed_catalog_state(&debug_factory);
    let debug_openable_state2 = debug_stash_backed_catalog_state(&debug_factory);
    test_open(debug_openable_state1, debug_openable_state2).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_open() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let persist_openable_state1 =
        persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let persist_openable_state2 =
        persist_backed_catalog_state(persist_client, organization_id).await;
    test_open(persist_openable_state1, persist_openable_state2).await;
}

async fn test_open(
    openable_state1: impl OpenableDurableCatalogState,
    openable_state2: impl OpenableDurableCatalogState,
) {
    let (snapshot, audit_log) = {
        let mut state = Box::new(openable_state1)
            // Use `NOW_ZERO` for consistent timestamps in the snapshots.
            .open(NOW_ZERO.clone(), &debug_bootstrap_args(), None)
            .await
            .unwrap();

        assert_eq!(state.epoch(), Epoch::new(2).expect("known to be non-zero"));
        // Check initial snapshot.
        let snapshot = state.snapshot().await.unwrap();
        insta::assert_debug_snapshot!("initial_snapshot", snapshot);
        let audit_log = state.get_audit_logs().await.unwrap();
        insta::assert_debug_snapshot!("initial_audit_log", audit_log);
        Box::new(state).expire().await;
        (snapshot, audit_log)
    };
    // Reopening the catalog will increment the epoch, but shouldn't change the initial snapshot.
    {
        let mut state = Box::new(openable_state2)
            .open(SYSTEM_TIME.clone(), &debug_bootstrap_args(), None)
            .await
            .unwrap();

        assert_eq!(state.epoch(), Epoch::new(3).expect("known to be non-zero"));
        assert_eq!(state.snapshot().await.unwrap(), snapshot);
        assert_eq!(state.get_audit_logs().await.unwrap(), audit_log);
        Box::new(state).expire().await;
    }
}

async fn stash_config() -> (DebugStashFactory, StashConfig) {
    // Creating a debug stash factory does a lot of nice stuff like creating a random schema for us.
    // Dropping the factory will drop the schema.
    let debug_stash_factory = DebugStashFactory::new().await;
    let config = StashConfig {
        stash_factory: debug_stash_factory.stash_factory().clone(),
        stash_url: debug_stash_factory.url().to_string(),
        schema: Some(debug_stash_factory.schema().to_string()),
        tls: debug_stash_factory.tls().clone(),
    };
    (debug_stash_factory, config)
}
