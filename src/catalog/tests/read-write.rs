// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![recursion_limit = "256"]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use insta::assert_debug_snapshot;
use itertools::Itertools;
use mz_audit_log::{EventDetails, EventType, EventV1, IdNameV1, VersionedEvent};
use mz_catalog::durable::objects::serialization::proto;
use mz_catalog::durable::objects::{Comment, DurableType, IdAlloc};
use mz_catalog::durable::{
    CatalogError, Database, DurableCatalogError, FenceError, Item, Metrics,
    TestCatalogStateBuilder, USER_ITEM_ALLOC_KEY, test_bootstrap_args,
};
use mz_ore::assert_ok;
use mz_ore::collections::HashSet;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::{PersistClient, ShardId};
use mz_proto::RustType;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, GlobalId, RelationVersion};
use mz_sql::catalog::{RoleAttributesRaw, RoleMembership, RoleVars};
use mz_sql::names::{CommentObjectId, DatabaseId, ResolvedDatabaseSpecifier, SchemaId};
use mz_storage_client::controller::StorageTxn;
use uuid::Uuid;

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_advance_upper_fencing() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_advance_upper_fencing(state_builder).await;
}

async fn test_advance_upper_fencing(state_builder: TestCatalogStateBuilder) {
    let state_builder = state_builder.with_default_deploy_generation();
    let mut state1 = state_builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let ts = state1.current_upper().await.step_forward();
    assert_ok!(state1.advance_upper(ts).await);

    let mut state2 = state_builder
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let ts = state2.current_upper().await.step_forward();
    assert_ok!(state2.advance_upper(ts).await);

    let ts = ts.step_forward();
    let err = state1.advance_upper(ts).await.unwrap_err();
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
        .unwrap();

    let start_id = state.get_next_id(id_type).await.unwrap();
    let commit_ts = state.current_upper().await;
    // Allocation does not require the initial update queue to be drained.
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
async fn test_persist_transaction_rejects_pending_catalog_content() {
    let persist_client = PersistClient::new_for_tests().await;
    let mut state = TestCatalogStateBuilder::new(persist_client)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();

    let mut update_counts = Vec::new();
    for _ in 0..2 {
        let err = state.transaction().await.unwrap_err();
        match err {
            CatalogError::Durable(DurableCatalogError::CatalogOutOfSync {
                update_count, ..
            }) => {
                assert!(update_count > 0);
                update_counts.push(update_count);
            }
            err => panic!("unexpected error: {err:?}"),
        }
    }
    assert_eq!(update_counts[0], update_counts[1]);

    let updates = state.sync_to_current_updates().await.unwrap();
    assert_eq!(updates.len(), update_counts[0]);

    let txn = state.transaction().await.unwrap();
    drop(txn);

    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_dry_run_transaction_rejects_mem_replace_escape() {
    let persist_client = PersistClient::new_for_tests().await;
    let mut dry_run_state = TestCatalogStateBuilder::new(persist_client.clone())
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let mut replacement_state = TestCatalogStateBuilder::new(persist_client)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let _ = dry_run_state.sync_to_current_updates().await.unwrap();
    let _ = replacement_state.sync_to_current_updates().await.unwrap();

    let initial_id = dry_run_state
        .get_next_id(USER_ITEM_ALLOC_KEY)
        .await
        .unwrap();
    let initial_upper = dry_run_state.current_upper().await;
    let snapshot = dry_run_state.snapshot().await.unwrap();
    let mut dry_run = dry_run_state.transaction_from_snapshot(snapshot).unwrap();
    let ids = dry_run
        .transaction_mut()
        .get_and_increment_id_by(USER_ITEM_ALLOC_KEY.to_string(), 1)
        .unwrap();
    assert_eq!(ids, vec![initial_id]);
    let _ = dry_run.transaction_mut().get_and_commit_op_updates();

    let replacement = replacement_state.transaction().await.unwrap();
    let escaped = std::mem::replace(dry_run.transaction_mut(), replacement);
    drop(dry_run);

    let commit_ts = escaped.upper();
    let err = escaped.commit(commit_ts).await.unwrap_err();
    assert!(matches!(
        err,
        CatalogError::Durable(DurableCatalogError::DryRunTransaction)
    ));
    assert_eq!(dry_run_state.current_upper().await, initial_upper);
    assert_eq!(
        dry_run_state
            .get_next_id(USER_ITEM_ALLOC_KEY)
            .await
            .unwrap(),
        initial_id
    );

    Box::new(dry_run_state).expire().await;
    Box::new(replacement_state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_advance_upper_at_least_semantics() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    let state_builder = state_builder.with_default_deploy_generation();
    let mut state = state_builder
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();

    let upper = state.current_upper().await;

    assert_ok!(state.advance_upper(upper).await);
    assert_ok!(
        state
            .advance_upper(upper.step_back().unwrap_or_default())
            .await
    );
    assert_eq!(state.current_upper().await, upper);

    let target = upper.step_forward().step_forward();
    assert_ok!(state.advance_upper(target).await);
    assert_eq!(state.current_upper().await, target);

    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_commit_rebases_over_empty_progress() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    let state_builder = state_builder.with_default_deploy_generation();

    let id_type = USER_ITEM_ALLOC_KEY;
    let mut state = state_builder
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();

    let commit_ts = state.current_upper().await;
    let overtaken = commit_ts.step_forward().step_forward();
    assert_ok!(state.advance_upper(overtaken).await);

    let start_id = state.get_next_id(id_type).await.unwrap();
    let ids = state.allocate_id(id_type, 1, commit_ts).await.unwrap();
    assert_eq!(ids, vec![start_id]);

    assert!(state.current_upper().await > overtaken);
    let next_id = state.get_next_id(id_type).await.unwrap();
    assert_eq!(next_id, start_id + 1);

    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_persist_conflicts_with_empty_progress_rebase() {
    use mz_catalog::durable::persist_desc;
    use mz_persist_client::Diagnostics;
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_storage_types::sources::SourceData;
    use timely::progress::Antichain;

    let persist_client = PersistClient::new_for_tests().await;
    let state_builder =
        TestCatalogStateBuilder::new(persist_client.clone()).with_default_deploy_generation();
    let mut state = state_builder
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    // Exclude bootstrap updates from conflict classification below.
    let _ = state.sync_to_current_updates().await.unwrap();

    // A raw handle advances the upper without fencing, exercising upper-mismatch classification.
    let mut raw_write = persist_client
        .open_writer::<SourceData, (), mz_repr::Timestamp, i64>(
            state.shard_id(),
            Arc::new(persist_desc()),
            Arc::new(UnitSchema::default()),
            Diagnostics {
                shard_name: "catalog".to_string(),
                handle_purpose: "test concurrent empty progress".to_string(),
            },
        )
        .await
        .expect("invalid usage");
    let empty: Vec<((SourceData, ()), mz_repr::Timestamp, i64)> = Vec::new();

    let upper = state.current_upper().await;
    let bumped = upper.step_forward().step_forward();
    raw_write
        .compare_and_append(
            empty.clone(),
            Antichain::from_elem(upper),
            Antichain::from_elem(bumped),
        )
        .await
        .expect("invalid usage")
        .expect("no conflict");

    let txn = state.transaction().await.unwrap();
    assert_eq!(txn.upper(), bumped);
    drop(txn);

    let target = bumped.step_forward();
    assert_ok!(state.advance_upper(target).await);
    assert_eq!(state.current_upper().await, target);

    let id_type = USER_ITEM_ALLOC_KEY;
    let start_id = state.get_next_id(id_type).await.unwrap();
    let mut txn = state.transaction().await.unwrap();
    let commit_ts = txn.upper();
    let ids = txn.get_and_increment_id_by(id_type.to_string(), 1).unwrap();
    assert_eq!(ids, vec![start_id]);
    let _updates = txn.get_and_commit_op_updates();
    raw_write
        .compare_and_append(
            empty,
            Antichain::from_elem(target),
            Antichain::from_elem(target.step_forward()),
        )
        .await
        .expect("invalid usage")
        .expect("no conflict");
    assert_ok!(txn.commit(commit_ts).await);
    let next_id = state.get_next_id(id_type).await.unwrap();
    assert_eq!(next_id, start_id + 1);

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
            ephemeral_owner_session: None,
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
            ephemeral_owner_session: None,
        },
    ];

    let mut state = state_builder
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
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
            item.global_id,
            item.schema_id,
            &item.name,
            item.create_sql.clone(),
            item.owner_id,
            item.privileges.clone(),
            item.extra_versions.clone(),
            item.ephemeral_owner_session,
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
async fn test_persist_ephemeral_items() {
    let persist_client = PersistClient::new_for_tests().await;
    let state_builder = TestCatalogStateBuilder::new(persist_client);
    test_ephemeral_items(state_builder).await;
}

/// Temporary items are durable items tagged with the UUID of the session that
/// created them. Two properties hold them together:
///
/// - Name uniqueness is scoped by that tag, because every session's temporary
///   schema shares one sentinel schema id, so without the scoping two sessions
///   could not both hold a `tt`.
/// - `remove_ephemeral_items` reclaims all of them and nothing else. It is what
///   a writable catalog open uses to clean up after a crash, so an over-broad
///   filter here would silently delete real user items.
async fn test_ephemeral_items(state_builder: TestCatalogStateBuilder) {
    let state_builder = state_builder.with_default_deploy_generation();
    let session_a = Uuid::from_u128(1);
    let session_b = Uuid::from_u128(2);
    // The sentinel schema id that every session's temporary schema shares.
    let temp_schema = SchemaId::User(0);

    let mut state = state_builder
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    // Drain initial updates.
    let _ = state
        .sync_to_current_updates()
        .await
        .expect("unable to sync");

    let mut txn = state.transaction().await.unwrap();

    let insert = |txn: &mut mz_catalog::durable::Transaction,
                  id: u64,
                  schema_id: SchemaId,
                  name: &str,
                  owner_session: Option<Uuid>| {
        txn.insert_item(
            CatalogItemId::User(id),
            u32::try_from(20_000 + id).expect("small"),
            GlobalId::User(id),
            schema_id,
            name,
            format!("CREATE VIEW {name} AS SELECT 1"),
            RoleId::User(1),
            vec![],
            BTreeMap::new(),
            owner_session,
        )
    };

    // A normal item, plus one temporary item per session sharing a name.
    insert(&mut txn, 100, SchemaId::User(1), "keep", None).unwrap();
    insert(&mut txn, 200, temp_schema, "tt", Some(session_a)).unwrap();
    insert(&mut txn, 300, temp_schema, "tt", Some(session_b)).unwrap();

    // A temporary item with an ALTER history: two global ids, one shard.
    txn.insert_item(
        CatalogItemId::User(500),
        20_500,
        GlobalId::User(500),
        temp_schema,
        "versioned",
        "CREATE TABLE versioned (a int)".to_string(),
        RoleId::User(1),
        vec![],
        BTreeMap::from([(RelationVersion::root().bump(), GlobalId::User(501))]),
        Some(session_a),
    )
    .unwrap();

    // Storage mappings like the ones `prepare_state` writes at CREATE, for
    // the normal item, one plain temporary item, and both versions of the
    // versioned one.
    let keep_shard = ShardId::new();
    let temp_shard = ShardId::new();
    let versioned_shard = ShardId::new();
    txn.insert_collection_metadata(BTreeMap::from([
        (GlobalId::User(100), keep_shard),
        (GlobalId::User(200), temp_shard),
        (GlobalId::User(500), versioned_shard),
        (GlobalId::User(501), versioned_shard),
    ]))
    .unwrap();

    // Comments on a temporary and a non-temporary item.
    txn.update_comment(
        CommentObjectId::View(CatalogItemId::User(100)),
        None,
        Some("keep comment".into()),
    )
    .unwrap();
    txn.update_comment(
        CommentObjectId::View(CatalogItemId::User(200)),
        None,
        Some("temp comment".into()),
    )
    .unwrap();

    // One session may not hold the same name twice, though.
    let err = insert(&mut txn, 400, temp_schema, "tt", Some(session_a)).unwrap_err();
    assert!(
        matches!(
            err,
            CatalogError::Catalog(mz_sql::catalog::CatalogError::ItemAlreadyExists(_, ref name))
                if name == "tt"
        ),
        "expected ItemAlreadyExists, got {err:?}"
    );

    txn.remove_ephemeral_items();

    // Drain txn updates.
    let _ = txn.get_and_commit_op_updates();
    let commit_ts = txn.upper();
    txn.commit(commit_ts).await.unwrap();

    let snapshot_items: Vec<Item> = state
        .snapshot()
        .await
        .unwrap()
        .items
        .into_iter()
        .map(RustType::from_proto)
        .map_ok(|(k, v)| Item::from_key_value(k, v))
        .collect::<Result<_, _>>()
        .unwrap();

    // Nothing ephemeral survives, and the normal item is untouched.
    assert!(
        !snapshot_items
            .iter()
            .any(|item| item.ephemeral_owner_session.is_some()),
        "ephemeral items survived: {:?}",
        snapshot_items
            .iter()
            .filter(|item| item.ephemeral_owner_session.is_some())
            .collect::<Vec<_>>()
    );
    assert!(
        snapshot_items
            .iter()
            .any(|item| item.id == CatalogItemId::User(100) && item.name == "keep"),
        "non-ephemeral item was removed: {snapshot_items:?}"
    );

    // Only the non-ephemeral item's comment survives.
    let snapshot_comments: Vec<Comment> = state
        .snapshot()
        .await
        .unwrap()
        .comments
        .into_iter()
        .map(RustType::from_proto)
        .map_ok(|(k, v)| Comment::from_key_value(k, v))
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        snapshot_comments
            .iter()
            .map(|c| c.object_id.clone())
            .collect::<Vec<_>>(),
        vec![CommentObjectId::View(CatalogItemId::User(100))],
        "comments on ephemeral items survived: {snapshot_comments:?}"
    );

    // The ephemeral items' storage mappings moved to the finalization WAL,
    // deduped to one shard per item. The non-ephemeral mapping is untouched.
    let txn = state.transaction().await.unwrap();
    assert_eq!(
        txn.get_collection_metadata(),
        BTreeMap::from([(GlobalId::User(100), keep_shard)]),
        "ephemeral collection metadata survived"
    );
    assert_eq!(
        txn.get_unfinalized_shards(),
        BTreeSet::from([temp_shard, versioned_shard]),
        "ephemeral shards were not enqueued for finalization"
    );
    drop(txn);

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
        .unwrap();
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
        .unwrap();
    let mut savepoint_state = state_builder
        .clone()
        .unwrap_build()
        .await
        .open_savepoint(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
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
        let updates = reader_state.sync_to_current_updates().await.unwrap();
        assert!(!updates.is_empty());
        let txn = reader_state.transaction().await.unwrap();
        let commit_ts = txn.upper();
        txn.commit(commit_ts).await.unwrap();
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_persist_exact_prefix() {
    let persist = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let builder = TestCatalogStateBuilder::new(persist.clone())
        .with_organization_id(organization_id)
        .with_default_deploy_generation();
    let mut writer = builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let _ = writer.sync_to_current_updates().await.unwrap();
    let shard = writer.shard_id();
    async fn resources(persist: &PersistClient, shard: ShardId) -> (usize, usize) {
        let state = serde_json::to_value(
            persist
                .inspect_shard::<mz_repr::Timestamp>(&shard)
                .await
                .unwrap(),
        )
        .unwrap();
        (
            state["leased_readers"].as_object().unwrap().len(),
            state["writers"].as_object().unwrap().len(),
        )
    }
    let initial_resources = resources(&persist, shard).await;
    let reader = |persist: PersistClient| async move {
        mz_catalog::durable::CatalogSnapshotReader::open(
            persist,
            organization_id,
            semver::Version::new(0, 0, 0),
            &test_bootstrap_args(),
        )
        .await
        .unwrap()
    };
    let exact = reader(persist.clone()).await;
    let before_fence = reader(persist.clone()).await;
    let at_fence = reader(persist.clone()).await;
    let live = reader(persist.clone()).await;
    let backwards = reader(persist.clone()).await;
    let opened_upper = writer.current_upper().await;
    assert!(
        backwards
            .into_snapshot_at(opened_upper.saturating_sub(1))
            .await
            .is_err()
    );

    let input = GlobalId::User(1000);
    let output = GlobalId::User(1001);
    let mut txn = writer.transaction().await.unwrap();
    txn.insert_collection_metadata(
        [input, output]
            .into_iter()
            .map(|id| (id, ShardId::new()))
            .collect(),
    )
    .unwrap();
    txn.set_collection_compaction_bound(input, Some(10.into()))
        .unwrap();
    txn.set_maintained_read_requirement(output, BTreeSet::from([input]), Some(10.into()))
        .unwrap();
    txn.insert_item(
        CatalogItemId::User(2000),
        22_000,
        GlobalId::User(2000),
        SchemaId::User(0),
        "temporary_view",
        "CREATE VIEW temporary_view AS SELECT 1".into(),
        RoleId::User(1),
        vec![],
        BTreeMap::new(),
        Some(Uuid::new_v4()),
    )
    .unwrap();
    let expected = txn.current_snapshot();
    let _ = txn.get_and_commit_op_updates();
    let ts = txn.upper();
    txn.commit(ts).await.unwrap();

    let expected_upper = writer.current_upper().await;
    let replayed = reader(persist.clone()).await;
    assert_eq!(
        replayed
            .into_snapshot_at(expected_upper)
            .await
            .unwrap()
            .snapshot,
        expected
    );
    let mut txn = writer.transaction().await.unwrap();
    txn.set_collection_compaction_bound(input, Some(20.into()))
        .unwrap();
    txn.set_maintained_read_requirement(output, BTreeSet::from([input]), None)
        .unwrap();
    txn.remove_ephemeral_items();
    txn.get_and_increment_id(USER_ITEM_ALLOC_KEY.into())
        .unwrap();
    let changed = txn.current_snapshot();
    let _ = txn.get_and_commit_op_updates();
    let ts = txn.upper();
    txn.commit(ts).await.unwrap();
    let later_upper = writer.current_upper().await;
    assert!(later_upper > expected_upper);

    // Keep publishing while extraction consumes the earlier exclusive prefix.
    let (stop, mut stopped) = tokio::sync::oneshot::channel::<()>();
    let (published, publishing) = tokio::sync::oneshot::channel();
    let publisher = mz_ore::task::spawn(|| "catalog-prefix-publisher", async move {
        let mut published = Some(published);
        while matches!(
            stopped.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ) {
            let upper = writer.current_upper().await.step_forward();
            writer.advance_upper(upper).await.unwrap();
            if let Some(published) = published.take() {
                let _ = published.send(());
            }
            tokio::task::yield_now().await;
        }
        writer
    });
    publishing.await.unwrap();
    let owned = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        exact.into_snapshot_at(expected_upper),
    )
    .await;
    let _ = stop.send(());
    let writer = publisher.await;
    let owned = owned
        .expect("extraction must not wait for publication to stop")
        .unwrap();
    assert_eq!(owned.upper, expected_upper);
    assert_eq!(owned.snapshot, expected);
    assert!(!owned.updates.is_empty());
    assert!(
        owned
            .updates
            .iter()
            .all(|update| update.ts < expected_upper)
    );
    assert_eq!(
        live.into_snapshot_at(later_upper).await.unwrap().snapshot,
        changed
    );
    let mut next_writer = builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let fenced_upper = next_writer.current_upper().await;
    assert!(matches!(
        at_fence.into_snapshot_at(fenced_upper).await,
        Err(CatalogError::Durable(DurableCatalogError::Fence(_)))
    ));
    let before_fence = before_fence.into_snapshot_at(expected_upper).await.unwrap();
    assert_eq!(before_fence.snapshot, expected);
    assert_eq!(before_fence.updates, owned.updates);
    next_writer.expire().await;
    assert_eq!(resources(&persist, shard).await, initial_resources);
    writer.expire().await;

    let unused = reader(persist.clone()).await;
    unused.expire().await;
    assert_eq!(resources(&persist, shard).await, (0, 0));
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
        .unwrap();
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
            None,
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

/// Regression test for incident-970: quadratic consolidation during catalog sync.
///
/// When a reader syncs through K timestamps, apply_updates() was calling
/// consolidate() on the entire snapshot for each timestamp, resulting in
/// O(K * N log N) work instead of O(N log N). This test verifies that syncing
/// through many timestamps only consolidates the snapshot a constant number of
/// times, not once per timestamp.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_persist_sync_consolidation_not_quadratic() {
    let persist_client = PersistClient::new_for_tests().await;
    let metrics = Arc::new(Metrics::new(&MetricsRegistry::new()));
    let state_builder =
        TestCatalogStateBuilder::new(persist_client).with_default_deploy_generation();
    // Share metrics between writer and reader so we can observe consolidation counts.
    let state_builder = state_builder.with_metrics(Arc::clone(&metrics));

    // Open a writer catalog.
    let mut writer = state_builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let _ = writer.sync_to_current_updates().await.unwrap();

    // Open a read-only catalog, caught up to the current upper.
    let mut reader = state_builder
        .clone()
        .unwrap_build()
        .await
        .open_read_only(&test_bootstrap_args())
        .await
        .unwrap();
    let _ = reader.sync_to_current_updates().await.unwrap();

    // Writer creates many databases, each in its own transaction at a distinct
    // timestamp. This mirrors the incident scenario where DDL happened across
    // many timestamps while a read-only envd was restarting.
    let num_timestamps: u64 = 100;
    for i in 0..num_timestamps {
        let mut txn = writer.transaction().await.unwrap();
        txn.insert_user_database(
            &format!("db_{i}"),
            RoleId::User(1),
            Vec::new(),
            &HashSet::new(),
        )
        .unwrap();
        let _ = txn.get_and_commit_op_updates();
        let commit_ts = txn.upper();
        txn.commit(commit_ts).await.unwrap();
    }

    // Record the consolidation counter before the reader syncs.
    let consolidations_before = metrics.snapshot_consolidations.get();

    // Reader syncs through all timestamps. With the quadratic bug, this would
    // call consolidate() once per timestamp (num_timestamps times). With the
    // fix, it should consolidate only once after processing all timestamps.
    let updates = reader.sync_to_current_updates().await.unwrap();
    let consolidations_after = metrics.snapshot_consolidations.get();
    let consolidations_during_sync = consolidations_after - consolidations_before;

    // Verify correctness: reader received updates and can see all databases.
    assert!(
        !updates.is_empty(),
        "reader should have received updates from writer"
    );
    let snapshot = reader.snapshot().await.unwrap();
    for i in 0..num_timestamps {
        let db_name = format!("db_{i}");
        let found = snapshot.databases.values().any(|db| db.name == db_name);
        assert!(found, "database {db_name} not found in reader snapshot");
    }

    // The key assertion: consolidation should happen O(log N) times during
    // the sync (from the doubling strategy), NOT once per timestamp (which
    // would be num_timestamps = 100). We allow a generous bound here.
    assert!(
        consolidations_during_sync < 10,
        "sync through {num_timestamps} timestamps triggered {consolidations_during_sync} \
         snapshot consolidations, suggesting quadratic behavior (expected < 10)"
    );

    Box::new(writer).expire().await;
    Box::new(reader).expire().await;
}

/// Verify that the reader's snapshot stays bounded during sync catch-up, even
/// when the writer churns the same object many times across timestamps. Without
/// the doubling consolidation in `sync_inner`, the snapshot would grow with
/// every retract+insert pair; with it, the snapshot stays within ~3x the live
/// catalog size.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_persist_sync_snapshot_stays_bounded_under_churn() {
    let persist_client = PersistClient::new_for_tests().await;
    let metrics = Arc::new(Metrics::new(&MetricsRegistry::new()));
    let state_builder = TestCatalogStateBuilder::new(persist_client)
        .with_default_deploy_generation()
        .with_metrics(Arc::clone(&metrics));

    // Open writer, create one database to churn.
    let mut writer = state_builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let _ = writer.sync_to_current_updates().await.unwrap();

    let mut txn = writer.transaction().await.unwrap();
    let (db_id, db_oid) = txn
        .insert_user_database("churn_db", RoleId::User(1), Vec::new(), &HashSet::new())
        .unwrap();
    let _ = txn.get_and_commit_op_updates();
    let commit_ts = txn.upper();
    txn.commit(commit_ts).await.unwrap();

    // Open reader, sync to current state.
    let mut reader = state_builder
        .unwrap_build()
        .await
        .open_read_only(&test_bootstrap_args())
        .await
        .unwrap();
    let _ = reader.sync_to_current_updates().await.unwrap();
    let peak_before = metrics.snapshot_max_entries.get();

    // Rename the same database 200 times, each in its own transaction.
    let num_renames: u64 = 200;
    let mut db = Database {
        id: db_id,
        oid: db_oid,
        name: "churn_db".to_string(),
        owner_id: RoleId::User(1),
        privileges: Vec::new(),
    };
    for i in 0..num_renames {
        let mut txn = writer.transaction().await.unwrap();
        db.name = format!("churn_db_{i}");
        txn.update_database(db.id, db.clone()).unwrap();
        let _ = txn.get_and_commit_op_updates();
        let commit_ts = txn.upper();
        txn.commit(commit_ts).await.unwrap();
    }

    // Reader syncs through all 200 renames.
    let _ = reader.sync_to_current_updates().await.unwrap();

    // Verify correctness: only one database, with the final name.
    let snapshot = reader.snapshot().await.unwrap();
    let churn_dbs: Vec<_> = snapshot
        .databases
        .values()
        .filter(|d| d.name.starts_with("churn_db"))
        .collect();
    assert_eq!(churn_dbs.len(), 1, "{churn_dbs:#?}");
    assert_eq!(churn_dbs[0].name, format!("churn_db_{}", num_renames - 1));

    // The key assertion: the snapshot high-water mark should stay bounded,
    // not grow proportionally to num_renames. The doubling consolidation
    // keeps it within ~3x the live catalog size.
    let peak_after = metrics.snapshot_max_entries.get();
    let peak_delta = peak_after - peak_before;
    // With doubling consolidation, the snapshot stays bounded. Without
    // consolidation this would grow by ~387 for 200 renames; with it, the
    // delta should be much smaller. We use 3x to allow headroom for
    // variance in how persist batches deliveries.
    let bounded = peak_before * 3;
    assert!(
        peak_delta < bounded,
        "peak unconsolidated snapshot grew by {peak_delta} over {num_renames} \
         renames (peak_before={peak_before}, peak_after={peak_after}); \
         expected < {bounded}"
    );

    Box::new(writer).expire().await;
    Box::new(reader).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_persist_committed_row_traffic() {
    use mz_repr::adt::jsonb::Jsonb;
    use mz_storage_types::sources::SourceData;

    fn traffic(registry: &MetricsRegistry) -> BTreeMap<(String, String), f64> {
        registry
            .gather()
            .into_iter()
            .filter(|family| {
                matches!(
                    family.name(),
                    "mz_catalog_committed_updates" | "mz_catalog_committed_update_bytes"
                )
            })
            .flat_map(|family| {
                family
                    .get_metric()
                    .iter()
                    .map(|metric| {
                        assert_eq!(metric.get_label().len(), 1);
                        let label = &metric.get_label()[0];
                        assert_eq!(label.name(), "kind");
                        (
                            (family.name().to_owned(), label.value().to_owned()),
                            metric.get_counter().as_ref().unwrap().value(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn packed_bytes(kind: proto::StateUpdateKind, tag: &str) -> f64 {
        let json = serde_json::to_value(kind).unwrap();
        assert_eq!(json["kind"], tag);
        let source = SourceData(Ok(Jsonb::from_serde_json(json).unwrap().into_row()));
        let bytes = source.0.unwrap().byte_len();
        assert!(bytes > 0);
        f64::from(u32::try_from(bytes).unwrap())
    }

    let registry = MetricsRegistry::new();
    let metrics = Arc::new(Metrics::new(&registry));
    let zero = traffic(&registry);
    assert_eq!(zero.len(), 6);
    for kind in ["compaction_bound", "maintained_read_requirement", "other"] {
        for metric in [
            "mz_catalog_committed_updates",
            "mz_catalog_committed_update_bytes",
        ] {
            assert_eq!(zero[&(metric.to_owned(), kind.to_owned())], 0.0);
        }
    }
    let builder = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation()
        .with_metrics(metrics);
    let mut state = builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let _ = state.sync_to_current_updates().await.unwrap();
    let input = GlobalId::User(1000);
    let output = GlobalId::User(1001);

    let mut previous_bytes = [0.0; 2];
    for frontier in [10u64, 20] {
        let before = traffic(&registry);
        let mut txn = state.transaction().await.unwrap();
        // A storage collection's first bound must be published with its metadata.
        if frontier == 10 {
            txn.insert_collection_metadata(
                [input, output]
                    .into_iter()
                    .map(|id| (id, ShardId::new()))
                    .collect(),
            )
            .unwrap();
        }
        txn.set_collection_compaction_bound(input, Some(frontier.into()))
            .unwrap();
        txn.set_maintained_read_requirement(output, BTreeSet::from([input]), Some(frontier.into()))
            .unwrap();
        let snapshot = txn.current_snapshot();
        let metadata_bytes = if frontier == 10 {
            snapshot
                .storage_collection_metadata
                .into_iter()
                .map(|(key, value)| {
                    packed_bytes(
                        proto::StateUpdateKind::StorageCollectionMetadata(
                            proto::StorageCollectionMetadata { key, value },
                        ),
                        "StorageCollectionMetadata",
                    )
                })
                .sum::<f64>()
        } else {
            0.0
        };
        let (key, value) = snapshot
            .collection_compaction_bounds
            .into_iter()
            .next()
            .unwrap();
        let bound_bytes = packed_bytes(
            proto::StateUpdateKind::CollectionCompactionBound(proto::CollectionCompactionBound {
                key,
                value,
            }),
            "CollectionCompactionBound",
        );
        let (key, value) = snapshot
            .maintained_read_requirements
            .into_iter()
            .next()
            .unwrap();
        let requirement_bytes = packed_bytes(
            proto::StateUpdateKind::MaintainedReadRequirement(proto::MaintainedReadRequirement {
                key,
                value,
            }),
            "MaintainedReadRequirement",
        );
        let _ = txn.get_and_commit_op_updates();
        let ts = txn.upper();
        txn.commit(ts).await.unwrap();
        let after = traffic(&registry);
        for (i, (kind, bytes)) in [
            ("compaction_bound", bound_bytes),
            ("maintained_read_requirement", requirement_bytes),
        ]
        .into_iter()
        .enumerate()
        {
            let updates_key = ("mz_catalog_committed_updates".to_owned(), kind.to_owned());
            let bytes_key = (
                "mz_catalog_committed_update_bytes".to_owned(),
                kind.to_owned(),
            );
            assert_eq!(
                after[&updates_key] - before[&updates_key],
                if frontier == 10 { 1.0 } else { 2.0 }
            );
            assert_eq!(
                after[&bytes_key] - before[&bytes_key],
                bytes + previous_bytes[i]
            );
            previous_bytes[i] = bytes;
        }
        for (metric, expected) in [
            (
                "mz_catalog_committed_updates",
                if frontier == 10 { 2.0 } else { 0.0 },
            ),
            ("mz_catalog_committed_update_bytes", metadata_bytes),
        ] {
            let key = (metric.to_owned(), "other".to_owned());
            assert_eq!(after[&key] - before[&key], expected);
        }
    }

    let before = traffic(&registry);
    let mut txn = state.transaction().await.unwrap();
    txn.insert_user_database("traffic_db", RoleId::User(1), Vec::new(), &HashSet::new())
        .unwrap();
    let _ = txn.get_and_commit_op_updates();
    let ts = txn.upper();
    txn.commit(ts).await.unwrap();
    let after = traffic(&registry);
    for (key, value) in &before {
        if key.1 == "other" {
            assert!(after[key] > *value);
        } else {
            assert_eq!(after[key], *value);
        }
    }

    let mut txn = state.transaction().await.unwrap();
    txn.set_collection_compaction_bound(input, Some(20.into()))
        .unwrap();
    let _ = txn.get_and_commit_op_updates();
    let ts = txn.upper();
    txn.commit(ts).await.unwrap();
    assert_eq!(traffic(&registry), after);

    let mut reader = builder
        .clone()
        .unwrap_build()
        .await
        .open_read_only(&test_bootstrap_args())
        .await
        .unwrap();
    let _ = reader.sync_to_current_updates().await.unwrap();
    let mut txn = reader.transaction().await.unwrap();
    txn.set_maintained_read_requirement(output, BTreeSet::from([input]), Some(30.into()))
        .unwrap();
    let _ = txn.get_and_commit_op_updates();
    let ts = txn.upper();
    assert!(matches!(
        txn.commit(ts).await.unwrap_err(),
        CatalogError::Durable(DurableCatalogError::NotWritable(_))
    ));
    assert_eq!(traffic(&registry), after);

    let mut savepoint = builder
        .clone()
        .unwrap_build()
        .await
        .open_savepoint(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let _ = savepoint.sync_to_current_updates().await.unwrap();
    let mut txn = savepoint.transaction().await.unwrap();
    txn.set_maintained_read_requirement(output, BTreeSet::from([input]), Some(30.into()))
        .unwrap();
    let _ = txn.get_and_commit_op_updates();
    let ts = txn.upper();
    txn.commit(ts).await.unwrap();
    assert_eq!(traffic(&registry), after);

    // Fence a transaction after it is prepared, so its durable append fails.
    let mut txn = state.transaction().await.unwrap();
    txn.set_maintained_read_requirement(output, BTreeSet::from([input]), Some(30.into()))
        .unwrap();
    let _ = txn.get_and_commit_op_updates();
    let replacement = builder
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let before_failure = traffic(&registry);
    let ts = txn.upper();
    assert!(matches!(
        txn.commit(ts).await.unwrap_err(),
        CatalogError::Durable(DurableCatalogError::Fence(FenceError::Epoch { .. }))
    ));
    assert_eq!(traffic(&registry), before_failure);
    Box::new(replacement).expire().await;
    Box::new(savepoint).expire().await;
    Box::new(reader).expire().await;
    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn test_persist_read_protection() {
    use mz_catalog::durable::objects::{CollectionCompactionBound, MaintainedReadRequirement};

    let builder = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation();
    let mut state = builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let _ = state.sync_to_current_updates().await.unwrap();
    let input = GlobalId::User(1000);
    let output = GlobalId::User(1001);
    let ungoverned = GlobalId::User(1002);
    let mut txn = state.transaction().await.unwrap();
    txn.insert_collection_metadata(
        [input, output, ungoverned]
            .into_iter()
            .map(|id| (id, ShardId::new()))
            .collect(),
    )
    .unwrap();
    txn.set_collection_compaction_bound(input, Some(10.into()))
        .unwrap();
    txn.set_maintained_read_requirement(output, BTreeSet::from([input]), Some(10.into()))
        .unwrap();
    txn.validate_read_protection().unwrap();
    let expected = txn.current_snapshot();
    let _ = txn.get_and_commit_op_updates();
    let ts = txn.upper();
    txn.commit(ts).await.unwrap();
    Box::new(state).expire().await;

    let mut state = builder
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let _ = state.sync_to_current_updates().await.unwrap();
    let snapshot = state.snapshot().await.unwrap();
    assert_eq!(
        snapshot.collection_compaction_bounds,
        expected.collection_compaction_bounds
    );
    assert_eq!(
        snapshot.maintained_read_requirements,
        expected.maintained_read_requirements
    );
    let bounds: Vec<CollectionCompactionBound> = snapshot
        .collection_compaction_bounds
        .into_iter()
        .map(RustType::from_proto)
        .map_ok(|(k, v)| DurableType::from_key_value(k, v))
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        bounds,
        vec![CollectionCompactionBound {
            id: input,
            frontier: Some(10.into())
        }]
    );
    let requirements: Vec<MaintainedReadRequirement> = snapshot
        .maintained_read_requirements
        .into_iter()
        .map(RustType::from_proto)
        .map_ok(|(k, v)| DurableType::from_key_value(k, v))
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        requirements,
        vec![MaintainedReadRequirement {
            id: output,
            inputs: BTreeSet::from([input]),
            frontier: Some(10.into()),
        }]
    );

    // Each rejected commit must leave the persisted protection state unchanged.
    for scenario in 0..13 {
        let mut txn = state.transaction().await.unwrap();
        match scenario {
            0 => txn
                .set_maintained_read_requirement(
                    ungoverned,
                    BTreeSet::from([input]),
                    Some(9.into()),
                )
                .unwrap(),
            1 => txn
                .set_collection_compaction_bound(input, Some(11.into()))
                .unwrap(),
            2 => txn
                .set_collection_compaction_bound(input, Some(9.into()))
                .unwrap(),
            3 | 4 => {
                txn.delete_collection_metadata(BTreeSet::from([input]));
                txn.insert_collection_metadata(BTreeMap::from([(input, ShardId::new())]))
                    .unwrap();
                if scenario == 3 {
                    txn.set_collection_compaction_bound(input, Some(9.into()))
                        .unwrap();
                }
            }
            5 => {
                txn.delete_collection_metadata(BTreeSet::from([input]));
            }
            6 => txn.set_collection_compaction_bound(input, None).unwrap(),
            7 => txn
                .set_maintained_read_requirement(
                    output,
                    BTreeSet::from([ungoverned]),
                    Some(10.into()),
                )
                .unwrap(),
            8 => txn
                .set_collection_compaction_bound(GlobalId::User(9999), Some(0.into()))
                .unwrap(),
            9 => txn
                .set_maintained_read_requirement(GlobalId::User(9999), BTreeSet::new(), None)
                .unwrap(),
            10 => txn
                .set_collection_compaction_bound(ungoverned, Some(10.into()))
                .unwrap(),
            11 => txn
                .set_collection_compaction_bound(ungoverned, None)
                .unwrap(),
            12 => {
                txn.delete_collection_metadata(BTreeSet::from([ungoverned]));
                txn.insert_collection_metadata(BTreeMap::from([(ungoverned, ShardId::new())]))
                    .unwrap();
                txn.set_collection_compaction_bound(ungoverned, Some(10.into()))
                    .unwrap();
            }
            _ => unreachable!(),
        }
        assert!(
            matches!(
                txn.validate_read_protection(),
                Err(CatalogError::Durable(
                    DurableCatalogError::InvalidReadProtection(_)
                ))
            ),
            "scenario {scenario}"
        );
        let _ = txn.get_and_commit_op_updates();
        let ts = txn.upper();
        assert!(
            matches!(
                txn.commit(ts).await,
                Err(CatalogError::Durable(
                    DurableCatalogError::InvalidReadProtection(_)
                ))
            ),
            "scenario {scenario}"
        );
        let snapshot = state.snapshot().await.unwrap();
        assert_eq!(
            snapshot.storage_collection_metadata,
            expected.storage_collection_metadata
        );
        assert_eq!(
            snapshot.collection_compaction_bounds,
            expected.collection_compaction_bounds
        );
        assert_eq!(
            snapshot.maintained_read_requirements,
            expected.maintained_read_requirements
        );
    }

    for bound_first in [true, false] {
        let mut txn = state.transaction().await.unwrap();
        let frontier = Some(if bound_first { 20.into() } else { 30.into() });
        if bound_first {
            txn.set_collection_compaction_bound(input, frontier)
                .unwrap();
        }
        txn.set_maintained_read_requirement(output, BTreeSet::from([input]), frontier)
            .unwrap();
        if !bound_first {
            txn.set_collection_compaction_bound(input, frontier)
                .unwrap();
        }
        txn.validate_read_protection().unwrap();
        let _ = txn.get_and_commit_op_updates();
        let ts = txn.upper();
        txn.commit(ts).await.unwrap();
    }

    let mut txn = state.transaction().await.unwrap();
    txn.delete_collection_metadata(BTreeSet::from([input, output]));
    txn.validate_read_protection().unwrap();
    assert!(
        txn.current_snapshot()
            .collection_compaction_bounds
            .is_empty()
    );
    assert!(
        txn.current_snapshot()
            .maintained_read_requirements
            .is_empty()
    );
    drop(txn);

    let mut txn = state.transaction().await.unwrap();
    txn.set_maintained_read_requirement(output, BTreeSet::from([input, ungoverned]), None)
        .unwrap();
    txn.set_collection_compaction_bound(input, None).unwrap();
    txn.validate_read_protection().unwrap();
    let _ = txn.get_and_commit_op_updates();
    let ts = txn.upper();
    txn.commit(ts).await.unwrap();

    let mut txn = state.transaction().await.unwrap();
    txn.set_collection_compaction_bound(input, Some(40.into()))
        .unwrap();
    assert!(matches!(
        txn.validate_read_protection(),
        Err(CatalogError::Durable(
            DurableCatalogError::InvalidReadProtection(_)
        ))
    ));
    drop(txn);

    let mut txn = state.transaction().await.unwrap();
    txn.delete_collection_metadata(BTreeSet::from([input]));
    txn.validate_read_protection().unwrap();
    assert_eq!(txn.current_snapshot().maintained_read_requirements.len(), 1);
    txn.delete_collection_metadata(BTreeSet::from([output]));
    txn.validate_read_protection().unwrap();
    let _ = txn.get_and_commit_op_updates();
    let ts = txn.upper();
    txn.commit(ts).await.unwrap();
    let snapshot = state.snapshot().await.unwrap();
    assert!(snapshot.collection_compaction_bounds.is_empty());
    assert!(snapshot.maintained_read_requirements.is_empty());
    Box::new(state).expire().await;
}
