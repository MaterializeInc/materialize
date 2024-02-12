// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::panic;

use fail::FailScenario;
use mz_catalog::durable::objects::DurableType;
use mz_catalog::durable::{
    stash_backed_catalog_state, test_bootstrap_args, test_migrate_from_stash_to_persist_state,
    test_persist_backed_catalog_state, test_rollback_from_persist_to_stash_state,
    test_stash_config, CatalogError, Database, OpenableDurableCatalogState, Role,
    TimelineTimestamp,
};
use mz_ore::now::NOW_ZERO;
use mz_persist_client::PersistClient;
use mz_proto::{ProtoType, RustType};
use mz_repr::role_id::RoleId;
use mz_sql::catalog::{RoleAttributes, RoleMembership, RoleVars};
use mz_sql::names::DatabaseId;
use mz_sql::session::vars::CatalogKind;
use mz_storage_types::sources::Timeline;
use uuid::Uuid;

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_migration() {
    let (debug_factory, stash_config) = test_stash_config().await;
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();

    let mut role = Role {
        // Temp placeholder.
        id: RoleId::User(1),
        name: "Joe".to_string(),
        attributes: RoleAttributes::new(),
        membership: RoleMembership::new(),
        vars: RoleVars::default(),
    };
    let timeline_timestamp = TimelineTimestamp {
        timeline: Timeline::User("Bar".to_string()),
        ts: mz_repr::Timestamp::new(42),
    };
    let timeline_key = timeline_timestamp.clone().into_key_value().0;
    let mut database = Database {
        // Temp placeholder.
        id: DatabaseId::User(1),
        name: "DB".to_string(),
        // Temp placeholder.
        owner_id: RoleId::User(1),
        privileges: Vec::new(),
    };

    // Initialize catalog in the stash.
    let role_key = {
        let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
        let mut stash_state = stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Insert some user role.
        let mut txn = stash_state.transaction().await.unwrap();
        let role_id = txn
            .insert_user_role(
                role.name.clone(),
                role.attributes.clone(),
                role.membership.clone(),
                role.vars.clone(),
            )
            .unwrap();
        role.id = role_id;
        database.owner_id = role_id;
        txn.commit().await.unwrap();
        role.clone().into_key_value().0
    };

    // Migrate catalog to persist.
    {
        let migrate_openable_state = Box::new(
            test_migrate_from_stash_to_persist_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut persist_state = migrate_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Check that the role still exists.
        let roles = persist_state.snapshot().await.unwrap().roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);

        // Insert some timeline data.
        let mut txn = persist_state.transaction().await.unwrap();
        txn.set_timestamp(
            timeline_timestamp.timeline.clone(),
            timeline_timestamp.ts.clone(),
        )
        .unwrap();
        txn.commit().await.unwrap();
    }

    // Open plain persist catalog and check that all the catalog data is there.
    {
        let persist_openable_state = Box::new(
            test_persist_backed_catalog_state(persist_client.clone(), organization_id.clone())
                .await,
        );
        let mut persist_state = persist_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let snapshot = persist_state.snapshot().await.unwrap();
        // Check that the role still exists.
        let roles = snapshot.roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);

        // Check that the timeline still exists.
        let timeline_timestamps = snapshot.timestamps;
        let timestamp_value = timeline_timestamps
            .get(&timeline_key.into_proto())
            .expect("timeline timestamp should exist")
            .clone();
        let persist_timeline_timestamp = TimelineTimestamp::from_key_value(
            timeline_key.clone(),
            timestamp_value.into_rust().unwrap(),
        );
        assert_eq!(timeline_timestamp, persist_timeline_timestamp);
    }

    // Check that migrating again is OK.
    {
        let migrate_openable_state = Box::new(
            test_migrate_from_stash_to_persist_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut persist_state = migrate_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let snapshot = persist_state.snapshot().await.unwrap();
        // Check that the role still exists.
        let roles = snapshot.roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);

        // Check that the timeline still exists.
        let timeline_timestamps = snapshot.timestamps;
        let timestamp_value = timeline_timestamps
            .get(&timeline_key.into_proto())
            .expect("timeline timestamp should exist")
            .clone();
        let persist_timeline_timestamp = TimelineTimestamp::from_key_value(
            timeline_key.clone(),
            timestamp_value.into_rust().unwrap(),
        );
        assert_eq!(timeline_timestamp, persist_timeline_timestamp);
    }

    // Rollback from persist to the stash.
    let database_key = {
        let rollback_openable_state = Box::new(
            test_rollback_from_persist_to_stash_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut persist_state = rollback_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let snapshot = persist_state.snapshot().await.unwrap();
        // Check that the role still exists.
        let roles = snapshot.roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);

        // Check that the timeline still exists.
        let timeline_timestamps = snapshot.timestamps;
        let timestamp_value = timeline_timestamps
            .get(&timeline_key.into_proto())
            .expect("timeline timestamp should exist")
            .clone();
        let persist_timeline_timestamp = TimelineTimestamp::from_key_value(
            timeline_key.clone(),
            timestamp_value.into_rust().unwrap(),
        );
        assert_eq!(timeline_timestamp, persist_timeline_timestamp);

        // Insert some database data.
        let mut txn = persist_state.transaction().await.unwrap();
        let database_id = txn
            .insert_user_database(
                &database.name,
                database.owner_id.clone(),
                database.privileges.clone(),
            )
            .unwrap();
        database.id = database_id;
        txn.commit().await.unwrap();
        database.clone().into_key_value().0
    };

    // Open plain stash catalog and check that all the catalog data is there.
    {
        let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
        let mut stash_state = stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let snapshot = stash_state.snapshot().await.unwrap();
        // Check that the role still exists.
        let roles = snapshot.roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);

        // Check that the timeline still exists.
        let timeline_timestamps = snapshot.timestamps;
        let timestamp_value = timeline_timestamps
            .get(&timeline_key.into_proto())
            .expect("timeline timestamp should exist")
            .clone();
        let persist_timeline_timestamp = TimelineTimestamp::from_key_value(
            timeline_key.clone(),
            timestamp_value.into_rust().unwrap(),
        );
        assert_eq!(timeline_timestamp, persist_timeline_timestamp);

        // Check that the database still exists.
        let databases = snapshot.databases;
        let database_value = databases
            .get(&database_key.into_proto())
            .expect("database should exist")
            .clone();
        let rolled_back_database =
            Database::from_key_value(database_key.clone(), database_value.into_rust().unwrap());
        assert_eq!(database, rolled_back_database);
    }

    // Check that rolling back again is OK.
    {
        let rollback_openable_state = Box::new(
            test_rollback_from_persist_to_stash_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut persist_state = rollback_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let snapshot = persist_state.snapshot().await.unwrap();
        // Check that the role still exists.
        let roles = snapshot.roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);

        // Check that the timeline still exists.
        let timeline_timestamps = snapshot.timestamps;
        let timestamp_value = timeline_timestamps
            .get(&timeline_key.into_proto())
            .expect("timeline timestamp should exist")
            .clone();
        let persist_timeline_timestamp = TimelineTimestamp::from_key_value(
            timeline_key.clone(),
            timestamp_value.into_rust().unwrap(),
        );
        assert_eq!(timeline_timestamp, persist_timeline_timestamp);

        // Check that the database still exists.
        let databases = snapshot.databases;
        let database_value = databases
            .get(&database_key.into_proto())
            .expect("database should exist")
            .clone();
        let rolled_back_database =
            Database::from_key_value(database_key.clone(), database_value.into_rust().unwrap());
        assert_eq!(database, rolled_back_database);
    }

    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_migration_panic_after_stash_fence_then_migrate() {
    let scenario = FailScenario::setup();

    let (debug_factory, stash_config) = test_stash_config().await;
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();

    let mut role = Role {
        // Temp placeholder.
        id: RoleId::User(1),
        name: "Joe".to_string(),
        attributes: RoleAttributes::new(),
        membership: RoleMembership::new(),
        vars: RoleVars::default(),
    };

    // Initialize catalog in the stash.
    let role_key = {
        let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
        let mut stash_state = stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Insert some user role.
        let mut txn = stash_state.transaction().await.unwrap();
        let role_id = txn
            .insert_user_role(
                role.name.clone(),
                role.attributes.clone(),
                role.membership.clone(),
                role.vars.clone(),
            )
            .unwrap();
        role.id = role_id;
        txn.commit().await.unwrap();
        role.clone().into_key_value().0
    };

    // Try to migrate catalog to persist, but panic after fencing.
    fail::cfg("post_stash_fence", "panic").unwrap();
    {
        let stash_config = stash_config.clone();
        let persist_client = persist_client.clone();
        let organization_id = organization_id.clone();
        let handle = mz_ore::task::spawn(|| "migrate-panic", async move {
            hide_panic_stack_trace();
            let migrate_openable_state = Box::new(
                test_migrate_from_stash_to_persist_state(
                    stash_config,
                    persist_client,
                    organization_id,
                )
                .await,
            );
            let _persist_state = migrate_openable_state
                .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
                .await
                .unwrap();
        });
        let _err = handle.await.unwrap_err();
    }

    // Try again to migrate catalog to persist and succeed.
    fail::cfg("post_stash_fence", "off").unwrap();
    {
        let migrate_openable_state = Box::new(
            test_migrate_from_stash_to_persist_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut persist_state = migrate_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Check that the role still exists.
        let roles = persist_state.snapshot().await.unwrap().roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);
    }

    // Open plain persist catalog and check that all the catalog data is there.
    {
        let persist_openable_state = Box::new(
            test_persist_backed_catalog_state(persist_client.clone(), organization_id.clone())
                .await,
        );
        let mut persist_state = persist_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let snapshot = persist_state.snapshot().await.unwrap();
        // Check that the role still exists.
        let roles = snapshot.roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);
    }

    debug_factory.drop().await;
    scenario.teardown();
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_migration_panic_after_stash_fence_then_rollback() {
    let scenario = FailScenario::setup();

    let (debug_factory, stash_config) = test_stash_config().await;
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();

    let mut role = Role {
        // Temp placeholder.
        id: RoleId::User(1),
        name: "Joe".to_string(),
        attributes: RoleAttributes::new(),
        membership: RoleMembership::new(),
        vars: RoleVars::default(),
    };

    // Initialize catalog in the stash.
    let role_key = {
        let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
        let mut stash_state = stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Insert some user role.
        let mut txn = stash_state.transaction().await.unwrap();
        let role_id = txn
            .insert_user_role(
                role.name.clone(),
                role.attributes.clone(),
                role.membership.clone(),
                role.vars.clone(),
            )
            .unwrap();
        role.id = role_id;
        txn.commit().await.unwrap();
        role.clone().into_key_value().0
    };

    // Try to migrate catalog to persist, but panic after fencing.
    fail::cfg("post_stash_fence", "panic").unwrap();
    {
        let stash_config = stash_config.clone();
        let persist_client = persist_client.clone();
        let organization_id = organization_id.clone();
        let handle = mz_ore::task::spawn(|| "migrate-panic", async move {
            hide_panic_stack_trace();
            let migrate_openable_state = Box::new(
                test_migrate_from_stash_to_persist_state(
                    stash_config,
                    persist_client,
                    organization_id,
                )
                .await,
            );
            let _persist_state = migrate_openable_state
                .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
                .await
                .unwrap();
        });
        let _err = handle.await.unwrap_err();
    }

    // Rollback from persist to the stash.
    fail::cfg("post_stash_fence", "off").unwrap();
    {
        let rollback_openable_state = Box::new(
            test_rollback_from_persist_to_stash_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut stash_state = rollback_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Check that the role still exists.
        let roles = stash_state.snapshot().await.unwrap().roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);
    }

    // Open plain stash catalog and check that all the catalog data is there.
    {
        let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
        let mut stash_state = stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let snapshot = stash_state.snapshot().await.unwrap();
        // Check that the role still exists.
        let roles = snapshot.roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);
    }

    debug_factory.drop().await;
    scenario.teardown();
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_rollback_panic_after_stash_fence_then_migrate() {
    let scenario = FailScenario::setup();

    let (debug_factory, stash_config) = test_stash_config().await;
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();

    let mut role = Role {
        // Temp placeholder.
        id: RoleId::User(1),
        name: "Joe".to_string(),
        attributes: RoleAttributes::new(),
        membership: RoleMembership::new(),
        vars: RoleVars::default(),
    };
    let timeline_timestamp = TimelineTimestamp {
        timeline: Timeline::User("Bar".to_string()),
        ts: mz_repr::Timestamp::new(42),
    };
    let timeline_key = timeline_timestamp.clone().into_key_value().0;

    // Initialize catalog in the stash.
    let role_key = {
        let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
        let mut stash_state = stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Insert some user role.
        let mut txn = stash_state.transaction().await.unwrap();
        let role_id = txn
            .insert_user_role(
                role.name.clone(),
                role.attributes.clone(),
                role.membership.clone(),
                role.vars.clone(),
            )
            .unwrap();
        role.id = role_id;
        txn.commit().await.unwrap();
        role.clone().into_key_value().0
    };

    // Migrate catalog to persist.
    {
        let migrate_openable_state = Box::new(
            test_migrate_from_stash_to_persist_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut persist_state = migrate_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Check that the role still exists.
        let roles = persist_state.snapshot().await.unwrap().roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);

        // Insert some timeline data.
        let mut txn = persist_state.transaction().await.unwrap();
        txn.set_timestamp(
            timeline_timestamp.timeline.clone(),
            timeline_timestamp.ts.clone(),
        )
        .unwrap();
        txn.commit().await.unwrap();
    }

    // Try to rollback catalog to stash, but panic after fencing.
    fail::cfg("post_stash_fence", "panic").unwrap();
    {
        let stash_config = stash_config.clone();
        let persist_client = persist_client.clone();
        let organization_id = organization_id.clone();
        let handle = mz_ore::task::spawn(|| "migrate-panic", async move {
            hide_panic_stack_trace();
            let rollback_openable_state = Box::new(
                test_rollback_from_persist_to_stash_state(
                    stash_config,
                    persist_client,
                    organization_id,
                )
                .await,
            );
            let _stash_state = rollback_openable_state
                .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
                .await
                .unwrap();
        });
        let _err = handle.await.unwrap_err();
    }

    // Migrate from the stash to persist.
    fail::cfg("post_stash_fence", "off").unwrap();
    {
        let migrate_openable_state = Box::new(
            test_rollback_from_persist_to_stash_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut persist_state = migrate_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let snapshot = persist_state.snapshot().await.unwrap();
        // Check that the role still exists.
        let roles = snapshot.roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);

        // Check that the timeline still exists.
        let timeline_timestamps = snapshot.timestamps;
        let timestamp_value = timeline_timestamps
            .get(&timeline_key.into_proto())
            .expect("timeline timestamp should exist")
            .clone();
        let persist_timeline_timestamp = TimelineTimestamp::from_key_value(
            timeline_key.clone(),
            timestamp_value.into_rust().unwrap(),
        );
        assert_eq!(timeline_timestamp, persist_timeline_timestamp);
    }

    // Open plain persist catalog and check that all the catalog data is there.
    {
        let persist_openable_state = Box::new(
            test_persist_backed_catalog_state(persist_client.clone(), organization_id.clone())
                .await,
        );
        let mut persist_state = persist_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let snapshot = persist_state.snapshot().await.unwrap();
        // Check that the role still exists.
        let roles = snapshot.roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);

        // Check that the timeline still exists.
        let timeline_timestamps = snapshot.timestamps;
        let timestamp_value = timeline_timestamps
            .get(&timeline_key.into_proto())
            .expect("timeline timestamp should exist")
            .clone();
        let persist_timeline_timestamp = TimelineTimestamp::from_key_value(
            timeline_key.clone(),
            timestamp_value.into_rust().unwrap(),
        );
        assert_eq!(timeline_timestamp, persist_timeline_timestamp);
    }

    debug_factory.drop().await;
    scenario.teardown();
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_rollback_panic_after_stash_fence_then_rollback() {
    let scenario = FailScenario::setup();

    let (debug_factory, stash_config) = test_stash_config().await;
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();

    let mut role = Role {
        // Temp placeholder.
        id: RoleId::User(1),
        name: "Joe".to_string(),
        attributes: RoleAttributes::new(),
        membership: RoleMembership::new(),
        vars: RoleVars::default(),
    };
    let timeline_timestamp = TimelineTimestamp {
        timeline: Timeline::User("Bar".to_string()),
        ts: mz_repr::Timestamp::new(42),
    };
    let timeline_key = timeline_timestamp.clone().into_key_value().0;

    // Initialize catalog in the stash.
    let role_key = {
        let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
        let mut stash_state = stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Insert some user role.
        let mut txn = stash_state.transaction().await.unwrap();
        let role_id = txn
            .insert_user_role(
                role.name.clone(),
                role.attributes.clone(),
                role.membership.clone(),
                role.vars.clone(),
            )
            .unwrap();
        role.id = role_id;
        txn.commit().await.unwrap();
        role.clone().into_key_value().0
    };

    // Migrate catalog to persist.
    {
        let migrate_openable_state = Box::new(
            test_migrate_from_stash_to_persist_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut persist_state = migrate_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Check that the role still exists.
        let roles = persist_state.snapshot().await.unwrap().roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);

        // Insert some timeline data.
        let mut txn = persist_state.transaction().await.unwrap();
        txn.set_timestamp(
            timeline_timestamp.timeline.clone(),
            timeline_timestamp.ts.clone(),
        )
        .unwrap();
        txn.commit().await.unwrap();
    }

    // Try to rollback catalog to stash, but panic after fencing.
    fail::cfg("post_stash_fence", "panic").unwrap();
    {
        let stash_config = stash_config.clone();
        let persist_client = persist_client.clone();
        let organization_id = organization_id.clone();
        let handle = mz_ore::task::spawn(|| "migrate-panic", async move {
            hide_panic_stack_trace();
            let rollback_openable_state = Box::new(
                test_rollback_from_persist_to_stash_state(
                    stash_config,
                    persist_client,
                    organization_id,
                )
                .await,
            );
            let _stash_state = rollback_openable_state
                .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
                .await
                .unwrap();
        });
        let _err = handle.await.unwrap_err();
    }

    // Try again to rollback catalog to stash and succeed.
    fail::cfg("post_stash_fence", "off").unwrap();
    {
        let rollback_openable_state = Box::new(
            test_rollback_from_persist_to_stash_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut stash_state = rollback_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let snapshot = stash_state.snapshot().await.unwrap();
        // Check that the role still exists.
        let roles = snapshot.roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);

        // Check that the timeline still exists.
        let timeline_timestamps = snapshot.timestamps;
        let timestamp_value = timeline_timestamps
            .get(&timeline_key.into_proto())
            .expect("timeline timestamp should exist")
            .clone();
        let persist_timeline_timestamp = TimelineTimestamp::from_key_value(
            timeline_key.clone(),
            timestamp_value.into_rust().unwrap(),
        );
        assert_eq!(timeline_timestamp, persist_timeline_timestamp);
    }

    // Open plain stash catalog and check that all the catalog data is there.
    {
        let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
        let mut stash_state = stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let snapshot = stash_state.snapshot().await.unwrap();
        // Check that the role still exists.
        let roles = snapshot.roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);

        // Check that the timeline still exists.
        let timeline_timestamps = snapshot.timestamps;
        let timestamp_value = timeline_timestamps
            .get(&timeline_key.into_proto())
            .expect("timeline timestamp should exist")
            .clone();
        let persist_timeline_timestamp = TimelineTimestamp::from_key_value(
            timeline_key.clone(),
            timestamp_value.into_rust().unwrap(),
        );
        assert_eq!(timeline_timestamp, persist_timeline_timestamp);
    }

    debug_factory.drop().await;
    scenario.teardown();
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_migration_panic_after_fence_then_migrate() {
    let scenario = FailScenario::setup();

    let (debug_factory, stash_config) = test_stash_config().await;
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();

    let mut role = Role {
        // Temp placeholder.
        id: RoleId::User(1),
        name: "Joe".to_string(),
        attributes: RoleAttributes::new(),
        membership: RoleMembership::new(),
        vars: RoleVars::default(),
    };

    // Initialize catalog in the stash.
    let role_key = {
        let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
        let mut stash_state = stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Insert some user role.
        let mut txn = stash_state.transaction().await.unwrap();
        let role_id = txn
            .insert_user_role(
                role.name.clone(),
                role.attributes.clone(),
                role.membership.clone(),
                role.vars.clone(),
            )
            .unwrap();
        role.id = role_id;
        txn.commit().await.unwrap();
        role.clone().into_key_value().0
    };

    // Try to migrate catalog to persist, but panic after fencing.
    fail::cfg("post_persist_fence", "panic").unwrap();
    {
        let stash_config = stash_config.clone();
        let persist_client = persist_client.clone();
        let organization_id = organization_id.clone();
        let handle = mz_ore::task::spawn(|| "migrate-panic", async move {
            hide_panic_stack_trace();
            let migrate_openable_state = Box::new(
                test_migrate_from_stash_to_persist_state(
                    stash_config,
                    persist_client,
                    organization_id,
                )
                .await,
            );
            let _persist_state = migrate_openable_state
                .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
                .await
                .unwrap();
        });
        let _err = handle.await.unwrap_err();
    }

    // Try again to migrate catalog to persist and succeed.
    fail::cfg("post_persist_fence", "off").unwrap();
    {
        let migrate_openable_state = Box::new(
            test_migrate_from_stash_to_persist_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut persist_state = migrate_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Check that the role still exists.
        let roles = persist_state.snapshot().await.unwrap().roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);
    }

    // Open plain persist catalog and check that all the catalog data is there.
    {
        let persist_openable_state = Box::new(
            test_persist_backed_catalog_state(persist_client.clone(), organization_id.clone())
                .await,
        );
        let mut persist_state = persist_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let snapshot = persist_state.snapshot().await.unwrap();
        // Check that the role still exists.
        let roles = snapshot.roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);
    }

    debug_factory.drop().await;
    scenario.teardown();
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_migration_panic_after_fence_then_rollback() {
    let scenario = FailScenario::setup();

    let (debug_factory, stash_config) = test_stash_config().await;
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();

    let mut role = Role {
        // Temp placeholder.
        id: RoleId::User(1),
        name: "Joe".to_string(),
        attributes: RoleAttributes::new(),
        membership: RoleMembership::new(),
        vars: RoleVars::default(),
    };

    // Initialize catalog in the stash.
    let role_key = {
        let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
        let mut stash_state = stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Insert some user role.
        let mut txn = stash_state.transaction().await.unwrap();
        let role_id = txn
            .insert_user_role(
                role.name.clone(),
                role.attributes.clone(),
                role.membership.clone(),
                role.vars.clone(),
            )
            .unwrap();
        role.id = role_id;
        txn.commit().await.unwrap();
        role.clone().into_key_value().0
    };

    // Try to migrate catalog to persist, but panic after fencing.
    fail::cfg("post_persist_fence", "panic").unwrap();
    {
        let stash_config = stash_config.clone();
        let persist_client = persist_client.clone();
        let organization_id = organization_id.clone();
        let handle = mz_ore::task::spawn(|| "migrate-panic", async move {
            hide_panic_stack_trace();
            let migrate_openable_state = Box::new(
                test_migrate_from_stash_to_persist_state(
                    stash_config,
                    persist_client,
                    organization_id,
                )
                .await,
            );
            let _persist_state = migrate_openable_state
                .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
                .await
                .unwrap();
        });
        let _err = handle.await.unwrap_err();
    }

    // Rollback from persist to the stash.
    fail::cfg("post_persist_fence", "off").unwrap();
    {
        let rollback_openable_state = Box::new(
            test_rollback_from_persist_to_stash_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut stash_state = rollback_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Check that the role still exists.
        let roles = stash_state.snapshot().await.unwrap().roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);
    }

    // Open plain stash catalog and check that all the catalog data is there.
    {
        let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
        let mut stash_state = stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let snapshot = stash_state.snapshot().await.unwrap();
        // Check that the role still exists.
        let roles = snapshot.roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);
    }

    debug_factory.drop().await;
    scenario.teardown();
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_rollback_panic_after_fence_then_migrate() {
    let scenario = FailScenario::setup();

    let (debug_factory, stash_config) = test_stash_config().await;
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();

    let mut role = Role {
        // Temp placeholder.
        id: RoleId::User(1),
        name: "Joe".to_string(),
        attributes: RoleAttributes::new(),
        membership: RoleMembership::new(),
        vars: RoleVars::default(),
    };
    let timeline_timestamp = TimelineTimestamp {
        timeline: Timeline::User("Bar".to_string()),
        ts: mz_repr::Timestamp::new(42),
    };
    let timeline_key = timeline_timestamp.clone().into_key_value().0;

    // Initialize catalog in the stash.
    let role_key = {
        let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
        let mut stash_state = stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Insert some user role.
        let mut txn = stash_state.transaction().await.unwrap();
        let role_id = txn
            .insert_user_role(
                role.name.clone(),
                role.attributes.clone(),
                role.membership.clone(),
                role.vars.clone(),
            )
            .unwrap();
        role.id = role_id;
        txn.commit().await.unwrap();
        role.clone().into_key_value().0
    };

    // Migrate catalog to persist.
    {
        let migrate_openable_state = Box::new(
            test_migrate_from_stash_to_persist_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut persist_state = migrate_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Check that the role still exists.
        let roles = persist_state.snapshot().await.unwrap().roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);

        // Insert some timeline data.
        let mut txn = persist_state.transaction().await.unwrap();
        txn.set_timestamp(
            timeline_timestamp.timeline.clone(),
            timeline_timestamp.ts.clone(),
        )
        .unwrap();
        txn.commit().await.unwrap();
    }

    // Try to rollback catalog to stash, but panic after fencing.
    fail::cfg("post_persist_fence", "panic").unwrap();
    {
        let stash_config = stash_config.clone();
        let persist_client = persist_client.clone();
        let organization_id = organization_id.clone();
        let handle = mz_ore::task::spawn(|| "migrate-panic", async move {
            hide_panic_stack_trace();
            let rollback_openable_state = Box::new(
                test_rollback_from_persist_to_stash_state(
                    stash_config,
                    persist_client,
                    organization_id,
                )
                .await,
            );
            let _stash_state = rollback_openable_state
                .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
                .await
                .unwrap();
        });
        let _err = handle.await.unwrap_err();
    }

    // Migrate from the stash to persist.
    fail::cfg("post_persist_fence", "off").unwrap();
    {
        let migrate_openable_state = Box::new(
            test_rollback_from_persist_to_stash_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut persist_state = migrate_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let snapshot = persist_state.snapshot().await.unwrap();
        // Check that the role still exists.
        let roles = snapshot.roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);

        // Check that the timeline still exists.
        let timeline_timestamps = snapshot.timestamps;
        let timestamp_value = timeline_timestamps
            .get(&timeline_key.into_proto())
            .expect("timeline timestamp should exist")
            .clone();
        let persist_timeline_timestamp = TimelineTimestamp::from_key_value(
            timeline_key.clone(),
            timestamp_value.into_rust().unwrap(),
        );
        assert_eq!(timeline_timestamp, persist_timeline_timestamp);
    }

    // Open plain persist catalog and check that all the catalog data is there.
    {
        let persist_openable_state = Box::new(
            test_persist_backed_catalog_state(persist_client.clone(), organization_id.clone())
                .await,
        );
        let mut persist_state = persist_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let snapshot = persist_state.snapshot().await.unwrap();
        // Check that the role still exists.
        let roles = snapshot.roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);

        // Check that the timeline still exists.
        let timeline_timestamps = snapshot.timestamps;
        let timestamp_value = timeline_timestamps
            .get(&timeline_key.into_proto())
            .expect("timeline timestamp should exist")
            .clone();
        let persist_timeline_timestamp = TimelineTimestamp::from_key_value(
            timeline_key.clone(),
            timestamp_value.into_rust().unwrap(),
        );
        assert_eq!(timeline_timestamp, persist_timeline_timestamp);
    }

    debug_factory.drop().await;
    scenario.teardown();
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_rollback_panic_after_fence_then_rollback() {
    let scenario = FailScenario::setup();

    let (debug_factory, stash_config) = test_stash_config().await;
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();

    let mut role = Role {
        // Temp placeholder.
        id: RoleId::User(1),
        name: "Joe".to_string(),
        attributes: RoleAttributes::new(),
        membership: RoleMembership::new(),
        vars: RoleVars::default(),
    };
    let timeline_timestamp = TimelineTimestamp {
        timeline: Timeline::User("Bar".to_string()),
        ts: mz_repr::Timestamp::new(42),
    };
    let timeline_key = timeline_timestamp.clone().into_key_value().0;

    // Initialize catalog in the stash.
    let role_key = {
        let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
        let mut stash_state = stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Insert some user role.
        let mut txn = stash_state.transaction().await.unwrap();
        let role_id = txn
            .insert_user_role(
                role.name.clone(),
                role.attributes.clone(),
                role.membership.clone(),
                role.vars.clone(),
            )
            .unwrap();
        role.id = role_id;
        txn.commit().await.unwrap();
        role.clone().into_key_value().0
    };

    // Migrate catalog to persist.
    {
        let migrate_openable_state = Box::new(
            test_migrate_from_stash_to_persist_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut persist_state = migrate_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Check that the role still exists.
        let roles = persist_state.snapshot().await.unwrap().roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);

        // Insert some timeline data.
        let mut txn = persist_state.transaction().await.unwrap();
        txn.set_timestamp(
            timeline_timestamp.timeline.clone(),
            timeline_timestamp.ts.clone(),
        )
        .unwrap();
        txn.commit().await.unwrap();
    }

    // Try to rollback catalog to stash, but panic after fencing.
    fail::cfg("post_persist_fence", "panic").unwrap();
    {
        let stash_config = stash_config.clone();
        let persist_client = persist_client.clone();
        let organization_id = organization_id.clone();
        let handle = mz_ore::task::spawn(|| "migrate-panic", async move {
            hide_panic_stack_trace();
            let rollback_openable_state = Box::new(
                test_rollback_from_persist_to_stash_state(
                    stash_config,
                    persist_client,
                    organization_id,
                )
                .await,
            );
            let _stash_state = rollback_openable_state
                .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
                .await
                .unwrap();
        });
        let _err = handle.await.unwrap_err();
    }

    // Try again to rollback catalog to stash and succeed.
    fail::cfg("post_persist_fence", "off").unwrap();
    {
        let rollback_openable_state = Box::new(
            test_rollback_from_persist_to_stash_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut stash_state = rollback_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let snapshot = stash_state.snapshot().await.unwrap();
        // Check that the role still exists.
        let roles = snapshot.roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);

        // Check that the timeline still exists.
        let timeline_timestamps = snapshot.timestamps;
        let timestamp_value = timeline_timestamps
            .get(&timeline_key.into_proto())
            .expect("timeline timestamp should exist")
            .clone();
        let persist_timeline_timestamp = TimelineTimestamp::from_key_value(
            timeline_key.clone(),
            timestamp_value.into_rust().unwrap(),
        );
        assert_eq!(timeline_timestamp, persist_timeline_timestamp);
    }

    // Open plain stash catalog and check that all the catalog data is there.
    {
        let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
        let mut stash_state = stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let snapshot = stash_state.snapshot().await.unwrap();
        // Check that the role still exists.
        let roles = snapshot.roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);

        // Check that the timeline still exists.
        let timeline_timestamps = snapshot.timestamps;
        let timestamp_value = timeline_timestamps
            .get(&timeline_key.into_proto())
            .expect("timeline timestamp should exist")
            .clone();
        let persist_timeline_timestamp = TimelineTimestamp::from_key_value(
            timeline_key.clone(),
            timestamp_value.into_rust().unwrap(),
        );
        assert_eq!(timeline_timestamp, persist_timeline_timestamp);
    }

    debug_factory.drop().await;
    scenario.teardown();
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_migration_panic_after_write_then_migrate() {
    let scenario = FailScenario::setup();

    let (debug_factory, stash_config) = test_stash_config().await;
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();

    let mut role = Role {
        // Temp placeholder.
        id: RoleId::User(1),
        name: "Joe".to_string(),
        attributes: RoleAttributes::new(),
        membership: RoleMembership::new(),
        vars: RoleVars::default(),
    };

    // Initialize catalog in the stash.
    let role_key = {
        let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
        let mut stash_state = stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Insert some user role.
        let mut txn = stash_state.transaction().await.unwrap();
        let role_id = txn
            .insert_user_role(
                role.name.clone(),
                role.attributes.clone(),
                role.membership.clone(),
                role.vars.clone(),
            )
            .unwrap();
        role.id = role_id;
        txn.commit().await.unwrap();
        role.clone().into_key_value().0
    };

    // Try to migrate catalog to persist, but panic after fencing.
    fail::cfg("migrate_post_write", "panic").unwrap();
    {
        let stash_config = stash_config.clone();
        let persist_client = persist_client.clone();
        let organization_id = organization_id.clone();
        let handle = mz_ore::task::spawn(|| "migrate-panic", async move {
            hide_panic_stack_trace();
            let migrate_openable_state = Box::new(
                test_migrate_from_stash_to_persist_state(
                    stash_config,
                    persist_client,
                    organization_id,
                )
                .await,
            );
            let _persist_state = migrate_openable_state
                .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
                .await
                .unwrap();
        });
        let _err = handle.await.unwrap_err();
    }

    // Try again to migrate catalog to persist and succeed.
    fail::cfg("migrate_post_write", "off").unwrap();
    {
        let migrate_openable_state = Box::new(
            test_migrate_from_stash_to_persist_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut persist_state = migrate_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Check that the role still exists.
        let roles = persist_state.snapshot().await.unwrap().roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);
    }

    // Open plain persist catalog and check that all the catalog data is there.
    {
        let persist_openable_state = Box::new(
            test_persist_backed_catalog_state(persist_client.clone(), organization_id.clone())
                .await,
        );
        let mut persist_state = persist_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let snapshot = persist_state.snapshot().await.unwrap();
        // Check that the role still exists.
        let roles = snapshot.roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);
    }

    debug_factory.drop().await;
    scenario.teardown();
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_migration_panic_after_write_then_rollback() {
    let scenario = FailScenario::setup();

    let (debug_factory, stash_config) = test_stash_config().await;
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();

    let mut role = Role {
        // Temp placeholder.
        id: RoleId::User(1),
        name: "Joe".to_string(),
        attributes: RoleAttributes::new(),
        membership: RoleMembership::new(),
        vars: RoleVars::default(),
    };

    // Initialize catalog in the stash.
    let role_key = {
        let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
        let mut stash_state = stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Insert some user role.
        let mut txn = stash_state.transaction().await.unwrap();
        let role_id = txn
            .insert_user_role(
                role.name.clone(),
                role.attributes.clone(),
                role.membership.clone(),
                role.vars.clone(),
            )
            .unwrap();
        role.id = role_id;
        txn.commit().await.unwrap();
        role.clone().into_key_value().0
    };

    // Try to migrate catalog to persist, but panic after fencing.
    fail::cfg("migrate_post_write", "panic").unwrap();
    {
        let stash_config = stash_config.clone();
        let persist_client = persist_client.clone();
        let organization_id = organization_id.clone();
        let handle = mz_ore::task::spawn(|| "migrate-panic", async move {
            hide_panic_stack_trace();
            let migrate_openable_state = Box::new(
                test_migrate_from_stash_to_persist_state(
                    stash_config,
                    persist_client,
                    organization_id,
                )
                .await,
            );
            let _persist_state = migrate_openable_state
                .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
                .await
                .unwrap();
        });
        let _err = handle.await.unwrap_err();
    }

    // Rollback from persist to the stash.
    fail::cfg("migrate_post_write", "off").unwrap();
    {
        let rollback_openable_state = Box::new(
            test_rollback_from_persist_to_stash_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut stash_state = rollback_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Check that the role still exists.
        let roles = stash_state.snapshot().await.unwrap().roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);
    }

    // Open plain stash catalog and check that all the catalog data is there.
    {
        let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
        let mut stash_state = stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let snapshot = stash_state.snapshot().await.unwrap();
        // Check that the role still exists.
        let roles = snapshot.roles;
        let role_value = roles
            .get(&role_key.into_proto())
            .expect("role should exist")
            .clone();
        let migrated_role = Role::from_key_value(role_key.clone(), role_value.into_rust().unwrap());
        assert_eq!(role, migrated_role);
    }

    debug_factory.drop().await;
    scenario.teardown();
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_savepoint_persist_uninitialized() {
    let (debug_factory, stash_config) = test_stash_config().await;
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();

    // Initialize catalog only in the stash.
    {
        let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
        let _stash_state = stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();
    }

    // Open a savepoint catalog using the rollback implementation.
    let database_key = {
        let rollback_openable_state = Box::new(
            test_rollback_from_persist_to_stash_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut stash_state = rollback_openable_state
            .open_savepoint(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let mut database = Database {
            // Temp placeholder.
            id: DatabaseId::User(1),
            name: "DB".to_string(),
            owner_id: RoleId::User(1),
            privileges: Vec::new(),
        };

        // Insert some database data.
        let mut txn = stash_state.transaction().await.unwrap();
        let database_id = txn
            .insert_user_database(
                &database.name,
                database.owner_id.clone(),
                database.privileges.clone(),
            )
            .unwrap();
        database.id = database_id;
        txn.commit().await.unwrap();
        let database_key = database.clone().into_key_value().0;

        // Check that the database can be read back.
        let snapshot = stash_state.snapshot().await.unwrap();
        let databases = snapshot.databases;
        let database_value = databases
            .get(&database_key.into_proto())
            .expect("database should exist")
            .clone();
        let rolled_back_database =
            Database::from_key_value(database_key.clone(), database_value.into_rust().unwrap());
        assert_eq!(database, rolled_back_database);

        database_key
    };

    // Try to open a savepoint catalog using the migrate implementation.
    {
        let migrate_openable_state = Box::new(
            test_migrate_from_stash_to_persist_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let open_err = migrate_openable_state
            .open_savepoint(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap_err();
        let durable_err = match open_err {
            CatalogError::Durable(e) => e,
            CatalogError::Catalog(e) => panic!("unexpected err: {e:?}"),
        };
        assert!(
            durable_err.can_recover_with_write_mode(),
            "unexpected err: {durable_err:?}"
        );
    }

    // Open a writable catalog using the rollback implementation.
    {
        let rollback_openable_state = Box::new(
            test_rollback_from_persist_to_stash_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut stash_state = rollback_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Check that the database no longer exists.
        let snapshot = stash_state.snapshot().await.unwrap();
        let databases = snapshot.databases;
        let database_value = databases.get(&database_key.into_proto());
        assert_eq!(database_value, None);
    }

    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_savepoint_stash_uninitialized() {
    let (debug_factory, stash_config) = test_stash_config().await;
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();

    // Initialize catalog only in persist.
    {
        let persist_openable_state = Box::new(
            test_persist_backed_catalog_state(persist_client.clone(), organization_id.clone())
                .await,
        );
        let _persist_state = persist_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();
    }

    // Open a savepoint catalog using the migrate implementation.
    let database_key = {
        let migrate_openable_state = Box::new(
            test_migrate_from_stash_to_persist_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut persist_state = migrate_openable_state
            .open_savepoint(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        let mut database = Database {
            // Temp placeholder.
            id: DatabaseId::User(1),
            name: "DB".to_string(),
            owner_id: RoleId::User(1),
            privileges: Vec::new(),
        };

        // Insert some database data.
        let mut txn = persist_state.transaction().await.unwrap();
        let database_id = txn
            .insert_user_database(
                &database.name,
                database.owner_id.clone(),
                database.privileges.clone(),
            )
            .unwrap();
        database.id = database_id;
        txn.commit().await.unwrap();
        let database_key = database.clone().into_key_value().0;

        // Check that the database can be read back.
        let snapshot = persist_state.snapshot().await.unwrap();
        let databases = snapshot.databases;
        let database_value = databases
            .get(&database_key.into_proto())
            .expect("database should exist")
            .clone();
        let rolled_back_database =
            Database::from_key_value(database_key.clone(), database_value.into_rust().unwrap());
        assert_eq!(database, rolled_back_database);

        database_key
    };

    // Try to open a savepoint catalog using the rollback implementation.
    {
        let rollback_openable_state = Box::new(
            test_rollback_from_persist_to_stash_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let open_err = rollback_openable_state
            .open_savepoint(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap_err();
        let durable_err = match open_err {
            CatalogError::Durable(e) => e,
            CatalogError::Catalog(e) => panic!("unexpected err: {e:?}"),
        };
        assert!(
            durable_err.can_recover_with_write_mode(),
            "unexpected err: {durable_err:?}"
        );
    }

    // Open a writable catalog using the migrate implementation.
    {
        let migrate_openable_state = Box::new(
            test_migrate_from_stash_to_persist_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut persist_state = migrate_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Check that the database no longer exists.
        let snapshot = persist_state.snapshot().await.unwrap();
        let databases = snapshot.databases;
        let database_value = databases.get(&database_key.into_proto());
        assert_eq!(database_value, None);
    }

    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_set_emergency_stash_migrate() {
    let (debug_factory, stash_config) = test_stash_config().await;
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();

    // Set a migrate catalog to emergency-stash and open.
    let mut migrate_openable_state = Box::new(
        test_migrate_from_stash_to_persist_state(
            stash_config.clone(),
            persist_client.clone(),
            organization_id.clone(),
        )
        .await,
    );
    migrate_openable_state.set_catalog_kind(CatalogKind::EmergencyStash);
    let _stash_state = migrate_openable_state
        .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
        .await
        .unwrap();

    // Check that stash is initialized.
    let mut stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
    assert!(stash_openable_state.is_initialized().await.unwrap());

    // Check that persist is still uninitialized.
    let mut persist_openable_state = Box::new(
        test_persist_backed_catalog_state(persist_client.clone(), organization_id.clone()).await,
    );
    assert!(!persist_openable_state.is_initialized().await.unwrap());

    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_set_emergency_stash_rollback() {
    let (debug_factory, stash_config) = test_stash_config().await;
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();

    // Set a rollback catalog to emergency-stash and open.
    let mut rollback_openable_state = Box::new(
        test_rollback_from_persist_to_stash_state(
            stash_config.clone(),
            persist_client.clone(),
            organization_id.clone(),
        )
        .await,
    );
    rollback_openable_state.set_catalog_kind(CatalogKind::EmergencyStash);
    let _stash_state = rollback_openable_state
        .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
        .await
        .unwrap();

    // Check that stash is initialized.
    let mut stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
    assert!(stash_openable_state.is_initialized().await.unwrap());

    // Check that persist is still uninitialized.
    let mut persist_openable_state = Box::new(
        test_persist_backed_catalog_state(persist_client.clone(), organization_id.clone()).await,
    );
    assert!(!persist_openable_state.is_initialized().await.unwrap());

    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_emergency_stash_tombstone() {
    let (debug_factory, stash_config) = test_stash_config().await;
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();

    {
        // Open a persist catalog.
        let persist_openable_state = Box::new(
            test_migrate_from_stash_to_persist_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut persist_state = persist_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Update catalog_kind (in persist) to emergency-stash.
        let mut txn = persist_state.transaction().await.unwrap();
        txn.set_catalog_kind(Some(CatalogKind::EmergencyStash))
            .unwrap();
        txn.commit().await.unwrap();
    }

    {
        // Open an emergency stash catalog (it doesn't matter if we start with a migrate or rollback
        // catalog because we change it to emergency-stash).
        let mut emergency_stash_openable_state = Box::new(
            test_rollback_from_persist_to_stash_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        emergency_stash_openable_state.set_catalog_kind(CatalogKind::EmergencyStash);
        let mut emergency_stash_state = emergency_stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();

        // Update catalog_kind (in the stash) to persist.
        let mut txn = emergency_stash_state.transaction().await.unwrap();
        txn.set_catalog_kind(Some(CatalogKind::Persist)).unwrap();
        txn.commit().await.unwrap();
    }

    // Check that a migrate implementation reads from the correct location pre-boot.
    {
        let mut openable_state = Box::new(
            test_migrate_from_stash_to_persist_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        assert_eq!(
            CatalogKind::Persist,
            openable_state
                .get_catalog_kind_config()
                .await
                .unwrap()
                .unwrap()
        )
    }

    // Check that a rollback implementation reads from the correct location pre-boot.
    {
        let mut openable_state = Box::new(
            test_rollback_from_persist_to_stash_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        assert_eq!(
            CatalogKind::Persist,
            openable_state
                .get_catalog_kind_config()
                .await
                .unwrap()
                .unwrap()
        )
    }

    debug_factory.drop().await;
}

/// Test that the epoch is migrated and rolled back correctly.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_epoch_migration() {
    let (debug_factory, stash_config) = test_stash_config().await;
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();

    let mut epoch = 1;

    // Open the catalog in the stash many times to increment the epoch.
    for _ in 0..10 {
        let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
        let mut stash_state = stash_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();
        epoch += 1;
        assert_eq!(epoch, stash_state.epoch().get());
    }

    // Migrate catalog to persist many times to increment the epoch.
    for _ in 0..10 {
        let migrate_openable_state = Box::new(
            test_migrate_from_stash_to_persist_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await,
        );
        let mut persist_state = migrate_openable_state
            .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
            .await
            .unwrap();
        epoch += 1;
        assert_eq!(epoch, persist_state.epoch().get());
    }

    // Rollback the catalog to the stash.
    let stash_openable_state = Box::new(stash_backed_catalog_state(stash_config.clone()));
    let mut stash_state = stash_openable_state
        .open(NOW_ZERO(), &test_bootstrap_args(), None, None)
        .await
        .unwrap();
    epoch += 1;
    assert_eq!(epoch, stash_state.epoch().get());

    debug_factory.drop().await;
}

fn hide_panic_stack_trace() {
    panic::set_hook(Box::new(|_| {}));
}
