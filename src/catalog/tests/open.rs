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
    debug_stash_backed_catalog_state, stash_backed_catalog_state, BootstrapArgs,
    DurableCatalogState, Error, OpenableDurableCatalogState, StashConfig,
};
use mz_ore::now::SYSTEM_TIME;
use mz_repr::role_id::RoleId;
use mz_stash::DebugStashFactory;
use std::num::NonZeroI64;

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_is_initialized() {
    let (debug_factory, stash_config) = stash_config().await;
    let openable_state = stash_backed_catalog_state(stash_config);
    is_initialized(openable_state).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_debug_is_initialized() {
    let debug_factory = DebugStashFactory::new().await;
    let debug_openable_state = debug_stash_backed_catalog_state(&debug_factory);
    is_initialized(debug_openable_state).await;
    debug_factory.drop().await;
}

async fn is_initialized<D: DurableCatalogState>(
    mut openable_state: impl OpenableDurableCatalogState<D>,
) {
    assert!(
        !openable_state.is_initialized().await.unwrap(),
        "catalog has not been opened yet"
    );

    let _ = openable_state
        .open(SYSTEM_TIME.clone(), &bootstrap_args(), None)
        .await
        .unwrap();

    assert!(
        openable_state.is_initialized().await.unwrap(),
        "catalog has been opened yet"
    );
    // Check twice because the implementation will cache a read-only stash.
    assert!(
        openable_state.is_initialized().await.unwrap(),
        "catalog has been opened yet"
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_get_deployment_generation() {
    let (debug_factory, stash_config) = stash_config().await;
    let openable_state = stash_backed_catalog_state(stash_config);
    get_deployment_generation(openable_state).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_debug_get_deployment_generation() {
    let debug_factory = DebugStashFactory::new().await;
    let debug_openable_state = debug_stash_backed_catalog_state(&debug_factory);
    get_deployment_generation(debug_openable_state).await;
    debug_factory.drop().await;
}

async fn get_deployment_generation<D: DurableCatalogState>(
    mut openable_state: impl OpenableDurableCatalogState<D>,
) {
    assert_eq!(
        openable_state.get_deployment_generation().await.unwrap(),
        None,
        "deployment generation has not been set"
    );

    let _ = openable_state
        .open(SYSTEM_TIME.clone(), &bootstrap_args(), Some(42))
        .await
        .unwrap();

    assert_eq!(
        openable_state.get_deployment_generation().await.unwrap(),
        Some(42),
        "deployment generation has been set to 42"
    );
    // Check twice because the implementation will cache a read-only stash.
    assert_eq!(
        openable_state.get_deployment_generation().await.unwrap(),
        Some(42),
        "deployment generation has been set to 42"
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_open_savepoint() {
    let (debug_factory, stash_config) = stash_config().await;
    let openable_state = stash_backed_catalog_state(stash_config);
    open_savepoint(openable_state).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_debug_open_savepoint() {
    let debug_factory = DebugStashFactory::new().await;
    let debug_openable_state = debug_stash_backed_catalog_state(&debug_factory);
    open_savepoint(debug_openable_state).await;
    debug_factory.drop().await;
}

async fn open_savepoint<D: DurableCatalogState>(
    mut openable_state: impl OpenableDurableCatalogState<D>,
) {
    {
        // Can't open a savepoint catalog until it's been initialized.
        let err = openable_state
            .open_savepoint(SYSTEM_TIME.clone(), &bootstrap_args(), None)
            .await
            .unwrap_err();
        match err {
            Error::Catalog(_) => panic!("unexpected catalog error"),
            Error::Stash(e) => assert!(e.can_recover_with_write_mode()),
        }

        // Initialize the stash.
        {
            let mut state = openable_state
                .open(SYSTEM_TIME.clone(), &bootstrap_args(), None)
                .await
                .unwrap();
            assert_eq!(
                state.epoch(),
                NonZeroI64::new(2).expect("known to be non-zero")
            );
        }

        // Open catalog in check mode.
        let mut state = openable_state
            .open_savepoint(SYSTEM_TIME.clone(), &bootstrap_args(), None)
            .await
            .unwrap();
        // Savepoint catalogs do not increment the epoch.
        assert_eq!(
            state.epoch(),
            NonZeroI64::new(2).expect("known to be non-zero")
        );

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
        assert!(db.is_some(), "database should exist")
    }

    {
        // Open catalog normally.
        let mut state = openable_state
            .open(SYSTEM_TIME.clone(), &bootstrap_args(), None)
            .await
            .unwrap();
        // Write should not have persisted.
        let db = state
            .get_databases()
            .await
            .unwrap()
            .into_iter()
            .find(|db| db.name == "db");
        assert!(db.is_none(), "database should not exist")
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_open_read_only() {
    let (debug_factory, stash_config) = stash_config().await;
    let openable_state = stash_backed_catalog_state(stash_config);
    open_read_only(openable_state).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_debug_open_read_only() {
    let debug_factory = DebugStashFactory::new().await;
    let debug_openable_state = debug_stash_backed_catalog_state(&debug_factory);
    open_read_only(debug_openable_state).await;
    debug_factory.drop().await;
}

async fn open_read_only<D: DurableCatalogState>(
    mut openable_state: impl OpenableDurableCatalogState<D>,
) {
    // Can't open a read-only stash until it's been initialized.
    let err = openable_state
        .open_read_only(SYSTEM_TIME.clone(), &bootstrap_args())
        .await
        .unwrap_err();
    match err {
        Error::Catalog(_) => panic!("unexpected catalog error"),
        Error::Stash(e) => assert!(e.can_recover_with_write_mode()),
    }

    // Initialize the stash.
    {
        let mut state = openable_state
            .open(SYSTEM_TIME.clone(), &bootstrap_args(), None)
            .await
            .unwrap();
        assert_eq!(
            state.epoch(),
            NonZeroI64::new(2).expect("known to be non-zero")
        );
    }

    let mut state = openable_state
        .open_read_only(SYSTEM_TIME.clone(), &bootstrap_args())
        .await
        .unwrap();
    // Read-only catalogs do not increment the epoch.
    assert_eq!(
        state.epoch(),
        NonZeroI64::new(2).expect("known to be non-zero")
    );
    let err = state.set_deploy_generation(42).await.unwrap_err();
    assert!(err
        .to_string()
        .contains("cannot execute UPDATE in a read-only transaction"));
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_open() {
    let (debug_factory, stash_config) = stash_config().await;
    let openable_state = stash_backed_catalog_state(stash_config);
    open(openable_state).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_debug_open() {
    let debug_factory = DebugStashFactory::new().await;
    let debug_openable_state = debug_stash_backed_catalog_state(&debug_factory);
    open(debug_openable_state).await;
    debug_factory.drop().await;
}

async fn open<D: DurableCatalogState>(mut openable_state: impl OpenableDurableCatalogState<D>) {
    {
        let mut state = openable_state
            .open(SYSTEM_TIME.clone(), &bootstrap_args(), None)
            .await
            .unwrap();

        assert_eq!(
            state.epoch(),
            NonZeroI64::new(2).expect("known to be non-zero")
        );
        // Check for initial clusters to ensure the catalog was opened properly.
        let clusters = state.get_clusters().await.unwrap();
        assert_eq!(clusters.len(), 3);
    }
    // Reopening the catalog will increment the epoch.
    {
        let mut state = openable_state
            .open(SYSTEM_TIME.clone(), &bootstrap_args(), None)
            .await
            .unwrap();

        assert_eq!(
            state.epoch(),
            NonZeroI64::new(3).expect("known to be non-zero")
        );
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

fn bootstrap_args() -> BootstrapArgs {
    BootstrapArgs {
        default_cluster_replica_size: "1".into(),
        builtin_cluster_replica_size: "1".into(),
        bootstrap_role: None,
    }
}
