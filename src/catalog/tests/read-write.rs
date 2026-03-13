// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![recursion_limit = "256"]

use std::collections::BTreeMap;

use insta::assert_debug_snapshot;
use itertools::Itertools;
use mz_audit_log::{EventDetails, EventType, EventV1, IdNameV1, VersionedEvent};
use mz_catalog::durable::objects::serialization::proto;
use mz_catalog::durable::objects::{DurableType, IdAlloc};
use mz_catalog::durable::{
    CatalogError, DurableCatalogError, FenceError, Item, TestCatalogStateBuilder,
    USER_ITEM_ALLOC_KEY, test_bootstrap_args,
};
use mz_ore::assert_ok;
use mz_ore::collections::HashSet;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::PersistClient;
use mz_proto::RustType;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, GlobalId};
use mz_sql::catalog::{RoleAttributesRaw, RoleMembership, RoleVars};
use mz_sql::names::{DatabaseId, ResolvedDatabaseSpecifier, SchemaId};

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_confirm_leadership() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_confirm_leadership(state_builder).await;
}

async fn test_confirm_leadership(state_builder: TestCatalogStateBuilder) {
    let state_builder = state_builder.with_default_deploy_generation();
    let mut state1 = state_builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;
    assert_ok!(state1.confirm_leadership().await);

    let mut state2 = state_builder
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;
    assert_ok!(state2.confirm_leadership().await);

    let err = state1.confirm_leadership().await.unwrap_err();
    assert!(matches!(
        err,
        CatalogError::Durable(DurableCatalogError::Fence(FenceError::Epoch { .. }))
    ));

    // Test that state1 can't start a transaction.
    let err = match state1.transaction().await {
        Ok(_) => panic!("unexpected Ok"),
        Err(e) => e,
    };
    assert!(matches!(
        err,
        CatalogError::Durable(DurableCatalogError::Fence(FenceError::Epoch { .. }))
    ));
    Box::new(state1).expire().await;
    Box::new(state2).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_allocate_id() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_allocate_id(state_builder).await;
}

async fn test_allocate_id(state_builder: TestCatalogStateBuilder) {
    let state_builder = state_builder.with_default_deploy_generation();

    let id_type = USER_ITEM_ALLOC_KEY;
    let mut state = state_builder
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;

    let start_id = state.get_next_id(id_type).await.unwrap();
    let commit_ts = state.current_upper().await;
    let ids = state.allocate_id(id_type, 3, commit_ts).await.unwrap();
    assert_eq!(ids, (start_id..(start_id + 3)).collect::<Vec<_>>());

    let snapshot_id_allocs: Vec<_> = state
        .snapshot()
        .await
        .unwrap()
        .id_allocator
        .into_iter()
        .map(RustType::from_proto)
        .map_ok(|(k, v)| IdAlloc::from_key_value(k, v))
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(snapshot_id_allocs.contains(&IdAlloc {
        name: id_type.to_string(),
        next_id: start_id + 3,
    }));
    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_audit_logs() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_audit_logs(state_builder).await;
}

async fn test_audit_logs(state_builder: TestCatalogStateBuilder) {
    let state_builder = state_builder.with_default_deploy_generation();
    let audit_logs = [
        VersionedEvent::V1(EventV1 {
            id: 100,
            event_type: EventType::Create,
            object_type: mz_audit_log::ObjectType::ClusterReplica,
            details: EventDetails::CreateClusterReplicaV2(mz_audit_log::CreateClusterReplicaV2 {
                cluster_id: "1".to_string(),
                cluster_name: "foo".to_string(),
                replica_id: Some("1".to_string()),
                replica_name: "bar".to_string(),
                logical_size: "scale=1,workers=1".to_string(),
                disk: false,
                billed_as: None,
                internal: false,
                reason: mz_audit_log::CreateOrDropClusterReplicaReasonV1::Schedule,
                scheduling_policies: Some(mz_audit_log::SchedulingDecisionsWithReasonsV1 {
                    on_refresh: mz_audit_log::RefreshDecisionWithReasonV1 {
                        decision: mz_audit_log::SchedulingDecisionV1::On,
                        objects_needing_refresh: vec!["u42".to_string(), "u90".to_string()],
                        hydration_time_estimate: "1000s".to_string(),
                    },
                }),
            }),
            user: Some("joe".to_string()),
            occurred_at: 100,
        }),
        VersionedEvent::V1(EventV1 {
            id: 200,
            event_type: EventType::Drop,
            object_type: mz_audit_log::ObjectType::View,
            details: EventDetails::IdNameV1(IdNameV1 {
                id: "2".to_string(),
                name: "v".to_string(),
            }),
            user: Some("mike".to_string()),
            occurred_at: 200,
        }),
    ];

    let mut state = state_builder
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
    let mut txn = state.transaction().await.unwrap();
    for audit_log in &audit_logs {
        txn.insert_audit_log_event(audit_log.clone());
    }
    // Drain txn updates.
    let _ = txn.get_and_commit_op_updates();
    let commit_ts = txn.upper();
    txn.commit(commit_ts).await.unwrap();

    let persisted_audit_logs = state.get_audit_logs().await.unwrap();
    for audit_log in &audit_logs {
        assert!(persisted_audit_logs.contains(audit_log));
    }
    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_items() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_items(state_builder).await;
}

async fn test_items(state_builder: TestCatalogStateBuilder) {
    let state_builder = state_builder.with_default_deploy_generation();
    let items = [
        Item {
            id: CatalogItemId::User(100),
            oid: 20_000,
            global_id: GlobalId::User(100),
            schema_id: SchemaId::User(1),
            name: "foo".to_string(),
            create_sql: "CREATE VIEW v AS SELECT 1".to_string(),
            owner_id: RoleId::User(1),
            privileges: vec![],
            extra_versions: BTreeMap::new(),
        },
        Item {
            id: CatalogItemId::User(200),
            oid: 20_001,
            global_id: GlobalId::User(200),
            schema_id: SchemaId::User(1),
            name: "bar".to_string(),
            create_sql: "CREATE MATERIALIZED VIEW mv AS SELECT 2".to_string(),
            owner_id: RoleId::User(2),
            privileges: vec![],
            extra_versions: BTreeMap::new(),
        },
    ];

    let mut state = state_builder
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
    let mut txn = state.transaction().await.unwrap();
    for item in &items {
        txn.insert_item(
            item.id,
            item.oid,
            item.global_id,
            item.schema_id,
            &item.name,
            item.create_sql.clone(),
            item.owner_id,
            item.privileges.clone(),
            item.extra_versions.clone(),
        )
        .unwrap();
    }
    // Drain txn updates.
    let _ = txn.get_and_commit_op_updates();
    let commit_ts = txn.upper();
    txn.commit(commit_ts).await.unwrap();

    let snapshot_items: Vec<_> = state
        .snapshot()
        .await
        .unwrap()
        .items
        .into_iter()
        .map(RustType::from_proto)
        .map_ok(|(k, v)| Item::from_key_value(k, v))
        .collect::<Result<_, _>>()
        .unwrap();
    for item in &items {
        assert!(snapshot_items.contains(item));
    }
    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_schemas() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_schemas(state_builder).await;
}

async fn test_schemas(state_builder: TestCatalogStateBuilder) {
    let state_builder = state_builder.with_default_deploy_generation();
    let mut state = state_builder
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
    let mut txn = state.transaction().await.unwrap();

    let (schema_id, _oid) = txn
        .insert_user_schema(
            DatabaseId::User(1),
            "foo",
            RoleId::User(1),
            vec![],
            &HashSet::new(),
        )
        .unwrap();
    // Drain txn updates.
    let _ = txn.get_and_commit_op_updates();
    let commit_ts = txn.upper();
    txn.commit(commit_ts).await.unwrap();

    // Test removing schemas where one doesn't exist.
    let mut txn = state.transaction().await.unwrap();

    let schemas = [
        (
            schema_id,
            ResolvedDatabaseSpecifier::Id(DatabaseId::User(1)),
        ),
        (SchemaId::User(100), ResolvedDatabaseSpecifier::Ambient),
    ]
    .into_iter()
    .collect();
    let result = txn.remove_schemas(&schemas);

    assert_debug_snapshot!(result, @r###"
    Err(
        Catalog(
            UnknownSchema(
                ".u100",
            ),
        ),
    )
    "###);

    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_non_writer_commits() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_non_writer_commits(state_builder).await;
}

async fn test_non_writer_commits(state_builder: TestCatalogStateBuilder) {
    let state_builder = state_builder.with_default_deploy_generation();
    let mut writer_state = state_builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;
    let mut savepoint_state = state_builder
        .clone()
        .unwrap_build()
        .await
        .open_savepoint(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap()
        .0;
    let mut reader_state = state_builder
        .clone()
        .unwrap_build()
        .await
        .open_read_only(&test_bootstrap_args())
        .await
        .unwrap();

    // Drain initial snapshots.
    let _ = writer_state.sync_to_current_updates().await.unwrap();
    let _ = savepoint_state.sync_to_current_updates().await.unwrap();
    let _ = reader_state.sync_to_current_updates().await.unwrap();

    // Commit write with writer.
    let role_name = "joe";
    let role_id = {
        let mut txn = writer_state.transaction().await.unwrap();
        let (role_id, _) = txn
            .insert_user_role(
                role_name.to_string(),
                RoleAttributesRaw::new(),
                RoleMembership::new(),
                RoleVars::default(),
                &HashSet::new(),
            )
            .unwrap();
        // Drain updates.
        let _ = txn.get_and_commit_op_updates();
        let commit_ts = txn.upper();
        txn.commit(commit_ts).await.unwrap();

        let roles = writer_state.snapshot().await.unwrap().roles;
        let role = roles
            .get(&proto::RoleKey {
                id: role_id.into_proto(),
            })
            .unwrap();
        assert_eq!(role_name, &role.name);

        role_id
    };

    // Savepoint can successfully commit transaction.
    {
        let db_name = "db";
        let mut txn = savepoint_state.transaction().await.unwrap();
        let (db_id, _) = txn
            .insert_user_database(db_name, RoleId::User(42), Vec::new(), &HashSet::new())
            .unwrap();
        let DatabaseId::User(db_id) = db_id else {
            panic!("unexpected id variant: {db_id:?}");
        };
        // Drain updates.
        let _ = txn.get_and_commit_op_updates();
        let commit_ts = txn.upper();
        txn.commit(commit_ts).await.unwrap();

        let snapshot = savepoint_state.snapshot().await.unwrap();

        // Savepoint catalogs do not yet know how to update themselves in response to concurrent
        // writes from writer catalogs, so it should not see the new role.
        let roles = snapshot.roles;
        let role = roles.get(&proto::RoleKey {
            id: role_id.into_proto(),
        });
        assert_eq!(None, role);

        let dbs = snapshot.databases;
        let db = dbs
            .get(&proto::DatabaseKey {
                id: proto::DatabaseId::User(db_id),
            })
            .unwrap();
        assert_eq!(db_name, &db.name);
    }

    // Read-only catalog can successfully commit empty transaction.
    {
        let txn = reader_state.transaction().await.unwrap();
        let commit_ts = txn.upper();
        txn.commit(commit_ts).await.unwrap();
    }
}

/// Verifies that computing next IDs from max existing catalog items gives
/// the correct baseline for DDL detection, even when the allocator counter
/// has been advanced far ahead by batch allocation (as IdPool does).
///
/// This is a regression test for the 0dt DDL detection bug where
/// `get_next_ids()` in preflight.rs used the allocator counter instead
/// of the max existing item ID, causing it to miss objects created from
/// a pre-allocated ID pool.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_persist_ddl_detection_with_batch_allocated_ids() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    let state_builder = state_builder.with_default_deploy_generation();

    let mut state = state_builder
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
        .expect("sync to current updates failed");

    // Simulate IdPool batch allocation: reserve 500 IDs at once.
    // This advances the allocator counter by 500, but we only create
    // a few items using the first IDs from that batch.
    let commit_ts = state.current_upper().await;
    let batch_ids = state
        .allocate_id(USER_ITEM_ALLOC_KEY, 500, commit_ts)
        .await
        .unwrap();
    assert_eq!(batch_ids.len(), 500);
    let first_id = batch_ids[0];

    // The allocator counter is now far ahead.
    let allocator_next = state.get_next_id(USER_ITEM_ALLOC_KEY).await.unwrap();
    assert_eq!(allocator_next, first_id + 500);

    // Insert only 3 items using the first IDs from the batch.
    let mut txn = state.transaction().await.unwrap();
    for i in 0..3u64 {
        let id = first_id + i;
        txn.insert_item(
            CatalogItemId::User(id),
            20_000 + u32::try_from(i).unwrap(),
            GlobalId::User(id),
            SchemaId::User(1),
            &format!("item_{i}"),
            format!("CREATE VIEW v{i} AS SELECT {i}"),
            RoleId::User(1),
            vec![],
            BTreeMap::new(),
        )
        .unwrap();
    }
    let _ = txn.get_and_commit_op_updates();
    let commit_ts = txn.upper();
    txn.commit(commit_ts).await.unwrap();

    // Now verify the two approaches to computing the next ID baseline.
    let txn = state.transaction().await.unwrap();

    // Approach used by the fix: max existing item ID + 1.
    let max_based_next = txn
        .get_items()
        .filter_map(|item| match item.id {
            CatalogItemId::User(id) => Some(id),
            _ => None,
        })
        .max()
        .map(|id| id + 1)
        .unwrap_or(0);

    // The max-based approach gives first_id + 3 (just past the 3 items).
    assert_eq!(max_based_next, first_id + 3);

    // The allocator counter is still at first_id + 500.
    assert_eq!(allocator_next, first_id + 500);

    // The gap is the bug: using the allocator counter as baseline would
    // miss any items with IDs in [first_id .. first_id + 500) that are
    // created after the baseline is captured.
    assert!(
        max_based_next < allocator_next,
        "max-based next ({max_based_next}) must be below allocator counter \
         ({allocator_next}) to demonstrate the batch allocation gap"
    );

    Box::new(state).expire().await;
}
