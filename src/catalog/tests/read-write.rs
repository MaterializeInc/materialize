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
use mz_catalog::durable::objects::{DurableType, IdAlloc, Snapshot};
use mz_catalog::durable::{
    test_bootstrap_args, test_persist_backed_catalog_state, test_stash_backed_catalog_state,
    CatalogError, DurableCatalogError, Item, OpenableDurableCatalogState, USER_ITEM_ALLOC_KEY,
};
use mz_ore::collections::CollectionExt;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::task::JoinHandleExt;
use mz_persist_client::PersistClient;
use mz_proto::RustType;
use mz_repr::role_id::RoleId;
use mz_repr::GlobalId;
use mz_sql::names::SchemaId;
use mz_stash::DebugStashFactory;
use std::collections::BTreeMap;
use std::time::Duration;
use uuid::Uuid;

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_confirm_leadership() {
    let debug_factory = DebugStashFactory::new().await;
    let openable_state1 = test_stash_backed_catalog_state(&debug_factory);
    let openable_state2 = test_stash_backed_catalog_state(&debug_factory);
    test_confirm_leadership(openable_state1, openable_state2).await;
    debug_factory.drop().await;
}

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
    openable_state1: impl OpenableDurableCatalogState,
    openable_state2: impl OpenableDurableCatalogState,
) {
    let mut state1 = Box::new(openable_state1)
        .open(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
        .await
        .unwrap();
    assert!(state1.confirm_leadership().await.is_ok());

    let mut state2 = Box::new(openable_state2)
        .open(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
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
async fn test_stash_get_and_prune_storage_usage_consolidates() {
    let debug_factory = DebugStashFactory::new().await;
    let openable_state = test_stash_backed_catalog_state(&debug_factory);

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

    // Insert many events that would take a long time to consolidate away.
    for i in 0..200_000 {
        let event = VersionedStorageUsage::V1(StorageUsageV1 {
            id: i,
            shard_id: Some("old".to_string()),
            size_bytes: i * 2,
            collection_timestamp: 10,
        });
        txn.insert_storage_usage_event(event.clone());
    }
    txn.insert_storage_usage_event(recent_event.clone());
    txn.commit().await.unwrap();

    // Test with no retention period.
    let events = state
        .get_and_prune_storage_usage(None, boot_ts, false)
        .await
        .unwrap();
    assert_eq!(events.len(), 200_001);

    // Open a client so we can check if the collection has consolidated.
    let (client, connection) =
        tokio_postgres::connect(debug_factory.url(), debug_factory.tls().clone())
            .await
            .expect("failed to open client");
    let client_conn = mz_ore::task::spawn(|| "tokio-postgres stash connection", async move {
        if let Err(e) = connection.await {
            tracing::warn!("postgres stash connection error: {e}");
        }
    });
    client
        .execute(
            &format!("SET search_path = {}", debug_factory.schema()),
            &[],
        )
        .await
        .expect("failed to set schema");

    // Test with some retention period.
    let events = state
        .get_and_prune_storage_usage(Some(Duration::from_millis(10)), boot_ts, true)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events.into_element(), recent_event);

    // We should _never_ have negative diffs here, we should have waited to return until
    // consolidation was complete.
    let count: i64 = client
        .query_one("SELECT count(*) FROM data WHERE diff < 0", &[])
        .await
        .expect("verify select count failed")
        .get(0);
    assert_eq!(count, 0, "Stash failed to consolidate!");

    // Cleanup.
    drop(client);
    client_conn.abort_and_wait().await;
    Box::new(state).expire().await;

    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_get_and_prune_storage_usage() {
    let debug_factory = DebugStashFactory::new().await;
    let openable_state = test_stash_backed_catalog_state(&debug_factory);
    test_get_and_prune_storage_usage(openable_state).await;
    debug_factory.drop().await;
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
async fn test_stash_allocate_id() {
    let debug_factory = DebugStashFactory::new().await;
    let openable_state = test_stash_backed_catalog_state(&debug_factory);
    test_allocate_id(openable_state).await;
    debug_factory.drop().await;
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
async fn test_stash_audit_logs() {
    let debug_factory = DebugStashFactory::new().await;
    let openable_state = test_stash_backed_catalog_state(&debug_factory);
    test_audit_logs(openable_state).await;
    debug_factory.drop().await;
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
async fn test_stash_items() {
    let debug_factory = DebugStashFactory::new().await;
    let openable_state = test_stash_backed_catalog_state(&debug_factory);
    test_items(openable_state).await;
    debug_factory.drop().await;
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
            schema_id: SchemaId::User(1),
            name: "foo".to_string(),
            create_sql: "CREATE VIEW v AS SELECT 1".to_string(),
            owner_id: RoleId::User(1),
            privileges: vec![],
        },
        Item {
            id: GlobalId::User(200),
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

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_set_catalog() {
    let debug_factory = DebugStashFactory::new().await;
    let openable_state = test_stash_backed_catalog_state(&debug_factory);
    test_set_catalog(openable_state).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_set_catalog() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let openable_state =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    test_set_catalog(openable_state).await;
}

async fn test_set_catalog(openable_state: impl OpenableDurableCatalogState) {
    let old_items = [
        Item {
            id: GlobalId::User(100),
            schema_id: SchemaId::User(1),
            name: "foo".to_string(),
            create_sql: "CREATE VIEW v AS SELECT 1".to_string(),
            owner_id: RoleId::User(1),
            privileges: vec![],
        },
        Item {
            id: GlobalId::User(200),
            schema_id: SchemaId::User(1),
            name: "bar".to_string(),
            create_sql: "CREATE MATERIALIZED VIEW mv AS SELECT 2".to_string(),
            owner_id: RoleId::User(2),
            privileges: vec![],
        },
    ];
    let new_items: BTreeMap<_, _> = [
        Item {
            id: GlobalId::User(300),
            schema_id: SchemaId::User(1),
            name: "fooooo".to_string(),
            create_sql: "CREATE VIEW v4 AS SELECT 10".to_string(),
            owner_id: RoleId::User(1),
            privileges: vec![],
        },
        Item {
            id: GlobalId::User(400),
            schema_id: SchemaId::User(1),
            name: "barrrrr".to_string(),
            create_sql: "CREATE MATERIALIZED VIEW mv4 AS SELECT 20".to_string(),
            owner_id: RoleId::User(2),
            privileges: vec![],
        },
    ]
    .into_iter()
    .map(|item| item.into_key_value())
    .map(|(key, value)| (key.into_proto(), value.into_proto()))
    .collect();
    let mut new_snapshot = Snapshot::empty();
    new_snapshot.items = new_items.clone();
    let new_audit_logs = vec![VersionedEvent::V1(EventV1 {
        id: 1,
        event_type: EventType::Create,
        object_type: mz_audit_log::ObjectType::Cluster,
        details: EventDetails::IdNameV1(IdNameV1 {
            id: "a".to_string(),
            name: "b".to_string(),
        }),
        user: None,
        occurred_at: 0,
    })];
    let new_storage_usages = vec![VersionedStorageUsage::V1(StorageUsageV1 {
        id: 1,
        shard_id: None,
        size_bytes: 2,
        collection_timestamp: 3,
    })];

    // Open state and insert some initial items into it.
    let mut state = Box::new(openable_state)
        .open(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
        .await
        .unwrap();
    let mut txn = state.transaction().await.unwrap();
    for item in &old_items {
        txn.insert_item(
            item.id,
            item.schema_id,
            &item.name,
            item.create_sql.clone(),
            item.owner_id,
            item.privileges.clone(),
        )
        .unwrap();
    }
    txn.commit().await.unwrap();

    // Set the contents of the catalog.
    let (mut txn, old_audit_logs, old_storage_usages) =
        state.whole_migration_transaction().await.unwrap();
    txn.set_catalog(
        new_snapshot.clone(),
        old_audit_logs,
        old_storage_usages,
        new_audit_logs.clone(),
        new_storage_usages.clone(),
    )
    .unwrap();
    txn.commit().await.unwrap();

    // Check the current state of the catalog.
    let (mut snapshot, audit_logs, storage_usages) =
        state.whole_migration_snapshot().await.unwrap();
    let items = std::mem::take(&mut snapshot.items);
    assert_eq!(items, new_items);
    assert_eq!(audit_logs, new_audit_logs);
    assert_eq!(storage_usages, new_storage_usages);
    assert_eq!(snapshot, Snapshot::empty());

    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_large_collection_retractions() {
    let debug_factory = DebugStashFactory::new().await;
    let openable_state = test_stash_backed_catalog_state(&debug_factory);
    test_large_collection_retractions(openable_state).await;
    debug_factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_large_collection_retractions() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let openable_state =
        test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
    test_large_collection_retractions(openable_state).await;
}

async fn test_large_collection_retractions(openable_state: impl OpenableDurableCatalogState) {
    let new_audit_logs = vec![VersionedEvent::V1(EventV1 {
        id: 2,
        event_type: EventType::Create,
        object_type: mz_audit_log::ObjectType::Cluster,
        details: EventDetails::IdNameV1(IdNameV1 {
            id: "c".to_string(),
            name: "d".to_string(),
        }),
        user: None,
        occurred_at: 0,
    })];
    let new_storage_usages = vec![VersionedStorageUsage::V1(StorageUsageV1 {
        id: 4,
        shard_id: None,
        size_bytes: 5,
        collection_timestamp: 6,
    })];

    let mut state = Box::new(openable_state)
        .open(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
        .await
        .unwrap();
    let (mut txn, old_audit_logs, old_storage_usages) =
        state.whole_migration_transaction().await.unwrap();
    // This will issue retractions for everything in the catalog.
    txn.set_catalog(
        Snapshot::empty(),
        old_audit_logs,
        old_storage_usages,
        new_audit_logs.clone(),
        new_storage_usages.clone(),
    )
    .unwrap();
    txn.commit().await.unwrap();

    let audit_logs = state.get_audit_logs().await.unwrap();
    let storage_usages = state
        .get_and_prune_storage_usage(None, mz_repr::Timestamp::new(0), false)
        .await
        .unwrap();

    assert_eq!(audit_logs, new_audit_logs);
    assert_eq!(storage_usages, new_storage_usages);

    Box::new(state).expire().await;
}
