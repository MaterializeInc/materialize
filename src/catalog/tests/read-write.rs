// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use itertools::Itertools;
use mz_audit_log::{
    CreateClusterReplicaV1, EventDetails, EventType, EventV1, IdNameV1, StorageUsageV1,
    VersionedEvent, VersionedStorageUsage,
};
use mz_catalog::durable::objects::{DurableType, IdAlloc};
use mz_catalog::durable::{
    test_bootstrap_args, test_persist_backed_catalog_state, CatalogError, DurableCatalogError,
    Item, OpenableDurableCatalogState, USER_ITEM_ALLOC_KEY,
};
use mz_ore::collections::CollectionExt;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::PersistClient;
use mz_proto::RustType;
use mz_repr::role_id::RoleId;
use mz_repr::GlobalId;
use mz_sql::names::SchemaId;
use std::time::Duration;
use uuid::Uuid;

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_get_and_prune_storage_usage() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let openable_state =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    test_get_and_prune_storage_usage(openable_state).await;
}

async fn test_get_and_prune_storage_usage(openable_state: impl OpenableDurableCatalogState) {
    let old_event = VersionedStorageUsage::V1(StorageUsageV1 {
        id: 1,
        shard_id: Some("recent".to_string()),
        size_bytes: 42,
        collection_timestamp: 10,
    });
    let recent_event = VersionedStorageUsage::V1(StorageUsageV1 {
        id: 1,
        shard_id: Some("recent".to_string()),
        size_bytes: 42,
        collection_timestamp: 20,
    });
    let boot_ts = mz_repr::Timestamp::new(23);

    let mut state = Box::new(openable_state)
        .open(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
        .await
        .unwrap();
    let mut txn = state.transaction().await.unwrap();
    txn.insert_storage_usage_event(old_event.clone());
    txn.insert_storage_usage_event(recent_event.clone());
    txn.commit().await.unwrap();

    // Test with no retention period.
    let events = state
        .get_and_prune_storage_usage(None, boot_ts, false)
        .await
        .unwrap();
    assert_eq!(events.len(), 2);
    assert!(events.contains(&old_event));
    assert!(events.contains(&recent_event));

    // Test with some retention period.
    let events = state
        .get_and_prune_storage_usage(Some(Duration::from_millis(10)), boot_ts, false)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
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

async fn test_allocate_id(openable_state: impl OpenableDurableCatalogState) {
    let id_type = USER_ITEM_ALLOC_KEY;
    let mut state = Box::new(openable_state)
        .open(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
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

async fn test_audit_logs(openable_state: impl OpenableDurableCatalogState) {
    let audit_logs = [
        VersionedEvent::V1(EventV1 {
            id: 100,
            event_type: EventType::Create,
            object_type: mz_audit_log::ObjectType::ClusterReplica,
            details: EventDetails::CreateClusterReplicaV1(CreateClusterReplicaV1 {
                cluster_id: "1".to_string(),
                cluster_name: "foo".to_string(),
                replica_id: Some("1".to_string()),
                replica_name: "bar".to_string(),
                logical_size: "1".to_string(),
                disk: false,
                billed_as: None,
                internal: false,
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

    let mut state = Box::new(openable_state)
        .open(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
        .await
        .unwrap();
    let mut txn = state.transaction().await.unwrap();
    for audit_log in &audit_logs {
        txn.insert_audit_log_event(audit_log.clone());
    }
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

async fn test_items(openable_state: impl OpenableDurableCatalogState) {
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

    let mut state = Box::new(openable_state)
        .open(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
        .await
        .unwrap();
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
