// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use insta::assert_debug_snapshot;
use itertools::Itertools;
use mz_audit_log::{
    EventDetails, EventType, EventV1, IdNameV1, StorageUsageV1, VersionedEvent,
    VersionedStorageUsage,
};
use mz_catalog::durable::objects::serialization::proto;
use mz_catalog::durable::objects::{DurableType, IdAlloc};
use mz_catalog::durable::{
    test_bootstrap_args, test_persist_backed_catalog_state, CatalogError, DurableCatalogError,
    Item, OpenableDurableCatalogState, USER_ITEM_ALLOC_KEY,
};
use mz_catalog::memory::objects::{StateDiff, StateUpdateKind};
use mz_ore::collections::CollectionExt;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::PersistClient;
use mz_proto::RustType;
use mz_repr::role_id::RoleId;
use mz_repr::GlobalId;
use mz_sql::catalog::{RoleAttributes, RoleMembership, RoleVars};
use mz_sql::names::{DatabaseId, ResolvedDatabaseSpecifier, SchemaId};
use std::time::Duration;
use uuid::Uuid;

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_confirm_leadership() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let openable_state1 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let openable_state2 = test_persist_backed_catalog_state(persist_client, organization_id).await;
    test_confirm_leadership(openable_state1, openable_state2).await;
}

async fn test_confirm_leadership(
    openable_state1: Box<dyn OpenableDurableCatalogState>,
    openable_state2: Box<dyn OpenableDurableCatalogState>,
) {
    let deploy_generation = 0;
    let mut state1 = openable_state1
        .open(
            SYSTEM_TIME(),
            &test_bootstrap_args(),
            deploy_generation,
            None,
        )
        .await
        .unwrap();
    assert!(state1.confirm_leadership().await.is_ok());

    let mut state2 = openable_state2
        .open(
            SYSTEM_TIME(),
            &test_bootstrap_args(),
            deploy_generation,
            None,
        )
        .await
        .unwrap();
    assert!(state2.confirm_leadership().await.is_ok());

    let err = state1.confirm_leadership().await.unwrap_err();
    assert!(matches!(
        err,
        CatalogError::Durable(DurableCatalogError::Fence(_))
    ));

    // Test that state1 can't start a transaction.
    let err = match state1.transaction().await {
        Ok(_) => panic!("unexpected Ok"),
        Err(e) => e,
    };
    assert!(matches!(
        err,
        CatalogError::Durable(DurableCatalogError::Fence(_))
    ));
    Box::new(state1).expire().await;
    Box::new(state2).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_get_and_prune_storage_usage() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let openable_state =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    test_get_and_prune_storage_usage(openable_state).await;
}

async fn test_get_and_prune_storage_usage(openable_state: Box<dyn OpenableDurableCatalogState>) {
    let old_event = StorageUsageV1 {
        id: 1,
        shard_id: Some("recent".to_string()),
        size_bytes: 42,
        collection_timestamp: 10,
    };
    let recent_event = StorageUsageV1 {
        id: 2,
        shard_id: Some("recent".to_string()),
        size_bytes: 42,
        collection_timestamp: 20,
    };
    let deploy_generation = 0;
    let boot_ts = mz_repr::Timestamp::new(23);

    let mut state = openable_state
        .open(
            SYSTEM_TIME(),
            &test_bootstrap_args(),
            deploy_generation,
            None,
        )
        .await
        .unwrap();
    // Drain initial updates.
    let _ = state
        .sync_to_current_updates()
        .await
        .expect("unable to sync");
    let mut txn = state.transaction().await.unwrap();
    txn.insert_storage_usage_event(
        old_event.shard_id.clone(),
        old_event.size_bytes.clone(),
        old_event.collection_timestamp.clone(),
    )
    .unwrap();
    txn.insert_storage_usage_event(
        recent_event.shard_id.clone(),
        recent_event.size_bytes.clone(),
        recent_event.collection_timestamp.clone(),
    )
    .unwrap();
    // Drain txn updates.
    let _ = txn.get_and_commit_op_updates();
    txn.commit().await.unwrap();

    let old_event = VersionedStorageUsage::V1(old_event);
    let recent_event = VersionedStorageUsage::V1(recent_event);

    // Test with no retention period.
    let updates = state.prune_storage_usage(None, boot_ts).await.unwrap();
    let events = state.get_storage_usage().await.unwrap();
    assert!(updates.is_empty(), "updates should be empty: {updates:?}");
    assert_eq!(events.len(), 2, "unexpected events len: {events:?}");
    assert!(events.contains(&old_event));
    assert!(events.contains(&recent_event));

    // Test with some retention period.
    let updates = state
        .prune_storage_usage(Some(Duration::from_millis(10)), boot_ts)
        .await
        .unwrap();
    let events = state.get_storage_usage().await.unwrap();
    assert_eq!(updates.len(), 1, "unexpected updates len: {updates:?}");
    let update = updates.into_element();
    assert_eq!(update.diff, StateDiff::Retraction);
    let StateUpdateKind::StorageUsage(update_usage) = update.kind else {
        panic!("unexpected update kind: {:?}", update.kind);
    };
    assert_eq!(update_usage.metric, old_event);
    assert_eq!(events.len(), 1, "unexpected events len: {events:?}");
    assert_eq!(events.into_element(), recent_event);
    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_allocate_id() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let openable_state =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    test_allocate_id(openable_state).await;
}

async fn test_allocate_id(openable_state: Box<dyn OpenableDurableCatalogState>) {
    let deploy_generation = 0;
    let id_type = USER_ITEM_ALLOC_KEY;
    let mut state = openable_state
        .open(
            SYSTEM_TIME(),
            &test_bootstrap_args(),
            deploy_generation,
            None,
        )
        .await
        .unwrap();

    let start_id = state.get_next_id(id_type).await.unwrap();
    let ids = state.allocate_id(id_type, 3).await.unwrap();
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
    let organization_id = Uuid::new_v4();
    let openable_state =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    test_audit_logs(openable_state).await;
}

async fn test_audit_logs(openable_state: Box<dyn OpenableDurableCatalogState>) {
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
                logical_size: "1".to_string(),
                disk: false,
                billed_as: None,
                internal: false,
                reason: mz_audit_log::CreateOrDropClusterReplicaReasonV1::Schedule,
                scheduling_policies: Some(mz_audit_log::SchedulingDecisionsWithReasonsV1 {
                    on_refresh: mz_audit_log::RefreshDecisionWithReasonV1 {
                        decision: mz_audit_log::SchedulingDecisionV1::On,
                        objects_needing_refresh: vec!["u42".to_string(), "u90".to_string()],
                        rehydration_time_estimate: "1000s".to_string(),
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

    let deploy_generation = 0;
    let mut state = openable_state
        .open(
            SYSTEM_TIME(),
            &test_bootstrap_args(),
            deploy_generation,
            None,
        )
        .await
        .unwrap();
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
    txn.commit().await.unwrap();

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
    let organization_id = Uuid::new_v4();
    let openable_state =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    test_items(openable_state).await;
}

async fn test_items(openable_state: Box<dyn OpenableDurableCatalogState>) {
    let items = [
        Item {
            id: GlobalId::User(100),
            oid: 20_000,
            schema_id: SchemaId::User(1),
            name: "foo".to_string(),
            create_sql: "CREATE VIEW v AS SELECT 1".to_string(),
            owner_id: RoleId::User(1),
            privileges: vec![],
        },
        Item {
            id: GlobalId::User(200),
            oid: 20_001,
            schema_id: SchemaId::User(1),
            name: "bar".to_string(),
            create_sql: "CREATE MATERIALIZED VIEW mv AS SELECT 2".to_string(),
            owner_id: RoleId::User(2),
            privileges: vec![],
        },
    ];

    let deploy_generation = 0;
    let mut state = openable_state
        .open(
            SYSTEM_TIME(),
            &test_bootstrap_args(),
            deploy_generation,
            None,
        )
        .await
        .unwrap();
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
            item.schema_id,
            &item.name,
            item.create_sql.clone(),
            item.owner_id,
            item.privileges.clone(),
        )
        .unwrap();
    }
    // Drain txn updates.
    let _ = txn.get_and_commit_op_updates();
    txn.commit().await.unwrap();

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
    let organization_id = Uuid::new_v4();
    let openable_state =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    test_schemas(openable_state).await;
}

async fn test_schemas(openable_state: Box<dyn OpenableDurableCatalogState>) {
    let deploy_generation = 0;
    let mut state = openable_state
        .open(
            SYSTEM_TIME(),
            &test_bootstrap_args(),
            deploy_generation,
            None,
        )
        .await
        .unwrap();
    // Drain initial updates.
    let _ = state
        .sync_to_current_updates()
        .await
        .expect("unable to sync");
    let mut txn = state.transaction().await.unwrap();

    let (schema_id, _oid) = txn
        .insert_user_schema(DatabaseId::User(1), "foo", RoleId::User(1), vec![])
        .unwrap();
    // Drain txn updates.
    let _ = txn.get_and_commit_op_updates();
    txn.commit().await.unwrap();

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
    let organization_id = Uuid::new_v4();
    let openable_state1 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let openable_state2 =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    let openable_state3 = test_persist_backed_catalog_state(persist_client, organization_id).await;
    test_non_writer_commits(openable_state1, openable_state2, openable_state3).await;
}

async fn test_non_writer_commits(
    openable_state1: Box<dyn OpenableDurableCatalogState>,
    openable_state2: Box<dyn OpenableDurableCatalogState>,
    openable_state3: Box<dyn OpenableDurableCatalogState>,
) {
    let deploy_generation = 0;
    let mut writer_state = openable_state1
        .open(
            SYSTEM_TIME(),
            &test_bootstrap_args(),
            deploy_generation,
            None,
        )
        .await
        .unwrap();
    let mut savepoint_state = openable_state2
        .open_savepoint(
            SYSTEM_TIME(),
            &test_bootstrap_args(),
            deploy_generation,
            None,
        )
        .await
        .unwrap();
    let mut reader_state = openable_state3
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
                RoleAttributes::new(),
                RoleMembership::new(),
                RoleVars::default(),
            )
            .unwrap();
        // Drain updates.
        let _ = txn.get_and_commit_op_updates();
        txn.commit().await.unwrap();

        let roles = writer_state.snapshot().await.unwrap().roles;
        let role = roles
            .get(&proto::RoleKey {
                id: Some(role_id.into_proto()),
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
            .insert_user_database(db_name, RoleId::User(42), Vec::new())
            .unwrap();
        let DatabaseId::User(db_id) = db_id else {
            panic!("unexpected id variant: {db_id:?}");
        };
        // Drain updates.
        let _ = txn.get_and_commit_op_updates();
        txn.commit().await.unwrap();

        let snapshot = savepoint_state.snapshot().await.unwrap();

        // Savepoint catalogs do not yet know how to update themselves in response to concurrent
        // writes from writer catalogs, so it should not see the new role.
        let roles = snapshot.roles;
        let role = roles.get(&proto::RoleKey {
            id: Some(role_id.into_proto()),
        });
        assert_eq!(None, role);

        let dbs = snapshot.databases;
        let db = dbs
            .get(&proto::DatabaseKey {
                id: Some(proto::DatabaseId {
                    value: Some(proto::database_id::Value::User(db_id)),
                }),
            })
            .unwrap();
        assert_eq!(db_name, &db.name);
    }

    // Read-only catalog can successfully commit empty transaction.
    {
        let txn = reader_state.transaction().await.unwrap();
        txn.commit().await.unwrap();
    }
}
