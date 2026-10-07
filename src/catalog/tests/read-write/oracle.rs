// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use mz_catalog::durable::{
    CatalogError, CatalogTimestampOracle, DurableCatalogError, TestCatalogStateBuilder,
    USER_ITEM_ALLOC_KEY, test_bootstrap_args,
};
use mz_ore::now::NowFn;
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::Timestamp;
use mz_storage_types::sources::SourceData;
use mz_timestamp_oracle::{TimestampOracle, WriteTimestamp};
use timely::progress::Antichain;
use tokio::sync::Notify;

#[derive(Debug, Default)]
struct TestOracle {
    times: Mutex<(Timestamp, Timestamp)>,
    allocation_gate: Mutex<Option<Arc<Notify>>>,
    allocation_started: Notify,
    completion_gate: Mutex<Option<Arc<Notify>>>,
    completion_started: Notify,
}

#[async_trait]
impl TimestampOracle<Timestamp> for TestOracle {
    async fn write_ts(&self) -> WriteTimestamp {
        let gate = self.allocation_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            self.allocation_started.notify_one();
            gate.notified().await;
        }
        let mut times = self.times.lock().unwrap();
        times.1 = times.1.step_forward();
        WriteTimestamp {
            timestamp: times.1,
            advance_to: times.1.step_forward(),
        }
    }

    async fn peek_write_ts(&self) -> Timestamp {
        self.times.lock().unwrap().1
    }

    async fn read_ts(&self) -> Timestamp {
        self.times.lock().unwrap().0
    }

    async fn apply_write(&self, timestamp: Timestamp) {
        let gate = self.completion_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            self.completion_started.notify_one();
            gate.notified().await;
        }
        let mut times = self.times.lock().unwrap();
        times.0 = times.0.max(timestamp);
        times.1 = times.1.max(timestamp);
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported OpenSSL calls
async fn promotion_cleanup_is_scoped_to_admitted_owners() {
    use std::collections::BTreeMap;

    use mz_catalog::durable::Transaction;
    use mz_catalog::durable::objects::{DurableType, Item};
    use mz_persist_client::ShardId;
    use mz_repr::role_id::RoleId;
    use mz_repr::{CatalogItemId, GlobalId, RelationVersion};
    use mz_sql::names::{CommentObjectId, SchemaId};
    use mz_storage_client::controller::StorageTxn;
    use uuid::Uuid;

    fn insert(txn: &mut Transaction<'_>, id: u64, owner: Uuid, shard: ShardId) {
        txn.insert_item(
            CatalogItemId::User(id),
            u32::try_from(20_000 + id).unwrap(),
            GlobalId::User(id),
            SchemaId::User(0),
            &format!("t{id}"),
            format!("CREATE TABLE t{id} (a int)"),
            RoleId::User(1),
            vec![],
            BTreeMap::from([(RelationVersion::root().bump(), GlobalId::User(id + 1))]),
            Some(owner),
        )
        .unwrap();
        txn.insert_collection_metadata(BTreeMap::from([
            (GlobalId::User(id), shard),
            (GlobalId::User(id + 1), shard),
        ]))
        .unwrap();
        txn.update_comment(
            CommentObjectId::Table(CatalogItemId::User(id)),
            None,
            Some(format!("owner {owner}")),
        )
        .unwrap();
    }

    async fn commit(mut txn: Transaction<'_>) {
        let ts = txn.upper();
        let _ = txn.get_and_commit_op_updates();
        txn.commit(ts).await.unwrap();
    }

    let oracle = Arc::new(TestOracle::default());
    let builder = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_deploy_generation(7)
        .with_timestamp_oracle(CatalogTimestampOracle::new(
            Arc::<TestOracle>::clone(&oracle),
            NowFn::from(|| 1_000),
        ));
    let mut active = builder
        .clone()
        .unwrap_build()
        .await
        .open(1_000.into(), &test_bootstrap_args())
        .await
        .unwrap();
    active.sync_to_current_updates().await.unwrap();
    let old_owner = Uuid::new_v4();
    let old_shard = ShardId::new();
    let mut txn = active.transaction().await.unwrap();
    txn.set_config("catalog_read_protection_enabled".into(), Some(1))
        .unwrap();
    insert(&mut txn, 1000, old_owner, old_shard);
    commit(txn).await;

    let next = builder.with_deploy_generation(8);
    let mut peer = next
        .clone()
        .unwrap_build()
        .await
        .join_prewarming("0.0.0+pending")
        .await
        .unwrap();
    let promotion = next.unwrap_build().await;
    let fence_gate = Arc::new(Notify::new());
    *oracle.completion_gate.lock().unwrap() = Some(Arc::clone(&fence_gate));
    let opening = mz_ore::task::spawn(|| "promotion with publications", async move {
        promotion
            .open_for_promotion(1_000.into(), &test_bootstrap_args())
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), oracle.completion_started.notified())
        .await
        .unwrap();
    // The fence is durable, but open has not yet acknowledged its oracle write.
    *oracle.completion_gate.lock().unwrap() = None;
    assert!(active.sync_to_current_updates().await.is_err());
    peer.sync_to_current_updates().await.unwrap();
    let fresh_owner = Uuid::new_v4();
    let fresh_shard = ShardId::new();
    let mut txn = peer.transaction().await.unwrap();
    insert(&mut txn, 2000, fresh_owner, fresh_shard);
    commit(txn).await;

    // Also race the cleanup commit itself, after it has assembled its snapshot.
    let cleanup_gate = Arc::new(Notify::new());
    *oracle.allocation_gate.lock().unwrap() = Some(Arc::clone(&cleanup_gate));
    fence_gate.notify_one();
    tokio::time::timeout(Duration::from_secs(5), oracle.allocation_started.notified())
        .await
        .unwrap();
    peer.sync_to_current_updates().await.unwrap();
    let later_shard = ShardId::new();
    let mut txn = peer.transaction().await.unwrap();
    insert(&mut txn, 3000, Uuid::new_v4(), later_shard);
    commit(txn).await;
    cleanup_gate.notify_one();

    let mut promoted = tokio::time::timeout(Duration::from_secs(5), opening)
        .await
        .unwrap()
        .unwrap();
    let snapshot = promoted.snapshot().await.unwrap();
    let owners: Vec<_> = snapshot
        .items
        .into_iter()
        .map(|(key, value)| Item::from_key_value(key, value))
        .filter_map(|item| item.ephemeral_owner_session)
        .collect();
    assert_eq!(owners.len(), 2);
    assert!(!owners.contains(&old_owner));
    assert!(owners.contains(&fresh_owner));
    assert_eq!(snapshot.comments.len(), 2);
    promoted.sync_to_current_updates().await.unwrap();
    let txn = promoted.transaction().await.unwrap();
    assert_eq!(
        txn.get_collection_metadata(),
        BTreeMap::from([
            (GlobalId::User(2000), fresh_shard),
            (GlobalId::User(2001), fresh_shard),
            (GlobalId::User(3000), later_shard),
            (GlobalId::User(3001), later_shard),
        ])
    );
    assert_eq!(
        txn.get_unfinalized_shards(),
        std::collections::BTreeSet::from([old_shard])
    );
    drop(txn);
    active.expire().await;
    peer.expire().await;
    promoted.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported OpenSSL calls
async fn catalog_acknowledgement_covers_rebased_timestamp() {
    let oracle = Arc::new(TestOracle::default());
    let mut catalog = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation()
        .with_timestamp_oracle(CatalogTimestampOracle::new(
            Arc::<TestOracle>::clone(&oracle),
            NowFn::from(|| 1_000),
        ))
        .unwrap_build()
        .await
        .open(1_000.into(), &test_bootstrap_args())
        .await
        .unwrap();

    let requested = catalog.current_upper().await;
    let overtaken = requested.saturating_add(100);
    catalog.advance_upper(overtaken).await.unwrap();
    catalog
        .allocate_id(USER_ITEM_ALLOC_KEY, 1, requested)
        .await
        .unwrap();
    let committed = catalog.current_upper().await.step_back().unwrap();
    assert!(committed >= overtaken);
    assert!(
        oracle.read_ts().await >= committed,
        "acknowledged catalog commit at {committed} is ahead of the shared oracle"
    );

    catalog.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported OpenSSL calls
async fn catalog_acknowledgement_waits_for_oracle_completion() {
    let persist = PersistClient::new_for_tests().await;
    let oracle = Arc::new(TestOracle::default());
    let mut catalog = TestCatalogStateBuilder::new(persist.clone())
        .with_default_deploy_generation()
        .with_timestamp_oracle(CatalogTimestampOracle::new(
            Arc::<TestOracle>::clone(&oracle),
            NowFn::from(|| 1_000),
        ))
        .unwrap_build()
        .await
        .open(1_000.into(), &test_bootstrap_args())
        .await
        .unwrap();
    let mut raw = persist
        .open_writer::<SourceData, (), Timestamp, i64>(
            catalog.shard_id(),
            Arc::new(mz_catalog::durable::persist_desc()),
            Arc::new(UnitSchema::default()),
            Diagnostics {
                shard_name: "catalog".into(),
                handle_purpose: "observe durable commit".into(),
            },
        )
        .await
        .unwrap();

    // A read from another participant must precede this catalog allocation.
    oracle.apply_write(100.into()).await;
    let preceding_read = oracle.read_ts().await;
    let gate = Arc::new(Notify::new());
    *oracle.completion_gate.lock().unwrap() = Some(Arc::clone(&gate));
    let commit = mz_ore::task::spawn(|| "catalog oracle completion", async move {
        catalog
            .allocate_id(USER_ITEM_ALLOC_KEY, 1, Timestamp::MIN)
            .await
            .unwrap();
        catalog
    });
    tokio::time::timeout(Duration::from_secs(5), oracle.completion_started.notified())
        .await
        .expect("catalog did not complete its timestamp");
    let committed = raw
        .fetch_recent_upper()
        .await
        .as_option()
        .unwrap()
        .step_back()
        .unwrap();
    assert!(committed > preceding_read);
    assert!(oracle.read_ts().await < committed);
    assert!(
        !commit.is_finished(),
        "acknowledged before oracle completion"
    );
    gate.notify_one();
    let catalog = tokio::time::timeout(Duration::from_secs(5), commit)
        .await
        .unwrap();
    assert!(oracle.read_ts().await >= committed);
    catalog.expire().await;
    raw.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported OpenSSL calls
async fn catalog_rebase_preserves_inherited_future_progress() {
    let persist = PersistClient::new_for_tests().await;
    let oracle = Arc::new(TestOracle::default());
    let clock = Arc::new(AtomicU64::new(20_000));
    let now = NowFn::from({
        let clock = Arc::clone(&clock);
        move || clock.load(Ordering::SeqCst)
    });
    let mut catalog = TestCatalogStateBuilder::new(persist.clone())
        .with_default_deploy_generation()
        .with_timestamp_oracle(CatalogTimestampOracle::new(
            Arc::<TestOracle>::clone(&oracle),
            now,
        ))
        .unwrap_build()
        .await
        .open(1_000.into(), &test_bootstrap_args())
        .await
        .unwrap();
    let mut raw = persist
        .open_writer::<SourceData, (), Timestamp, i64>(
            catalog.shard_id(),
            Arc::new(mz_catalog::durable::persist_desc()),
            Arc::new(UnitSchema::default()),
            Diagnostics {
                shard_name: "catalog".into(),
                handle_purpose: "race empty catalog progress".into(),
            },
        )
        .await
        .unwrap();
    let _ = catalog.sync_to_current_updates().await.unwrap();
    let start_id = catalog.get_next_id(USER_ITEM_ALLOC_KEY).await.unwrap();
    let mut txn = catalog.transaction().await.unwrap();
    let supplied = txn.upper();
    txn.get_and_increment_id_by(USER_ITEM_ALLOC_KEY.into(), 1)
        .unwrap();
    let _ = txn.get_and_commit_op_updates();
    // Simulate progress inherited from a participant whose clock was ahead.
    // This races an already-created transaction, forcing the commit's CAS retry.
    let overtaken = Timestamp::from(20_000);
    raw.compare_and_append(
        Vec::<((SourceData, ()), Timestamp, i64)>::new(),
        Antichain::from_elem(supplied),
        Antichain::from_elem(overtaken),
    )
    .await
    .unwrap()
    .unwrap();
    clock.store(1_000, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(5), txn.commit(supplied))
        .await
        .expect("inherited progress must not wait for the clock")
        .unwrap();
    assert_eq!(
        catalog.get_next_id(USER_ITEM_ALLOC_KEY).await.unwrap(),
        start_id + 1
    );
    assert!(oracle.read_ts().await >= catalog.current_upper().await.step_back().unwrap());
    assert!(oracle.read_ts().await >= overtaken);
    catalog.expire().await;
    raw.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported OpenSSL calls
async fn catalog_bootstrap_and_nonwriting_modes() {
    let oracle = Arc::new(TestOracle::default());
    oracle.apply_write(20_000.into()).await;
    let builder = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation()
        .with_timestamp_oracle(CatalogTimestampOracle::new(
            Arc::<TestOracle>::clone(&oracle),
            NowFn::from(|| 1_000),
        ));
    let args = test_bootstrap_args();
    let mut catalog = tokio::time::timeout(Duration::from_secs(5), async {
        builder
            .clone()
            .unwrap_build()
            .await
            .open(1_000.into(), &args)
            .await
    })
    .await
    .expect("bootstrap must preserve inherited oracle progress")
    .unwrap();
    assert!(oracle.read_ts().await >= catalog.current_upper().await.step_back().unwrap());
    let allocation = oracle.write_ts().await;
    let before = *oracle.times.lock().unwrap();
    // Closing a catalog prefix does not announce completion of the table write
    // that needs that prefix as its fencing prerequisite.
    catalog.advance_upper(allocation.advance_to).await.unwrap();
    assert_eq!(*oracle.times.lock().unwrap(), before);
    let reader = builder
        .clone()
        .unwrap_build()
        .await
        .open_read_only(&args)
        .await
        .unwrap();
    let mut savepoint = builder
        .unwrap_build()
        .await
        .open_savepoint(1_000.into(), &args)
        .await
        .unwrap();
    savepoint
        .allocate_id(USER_ITEM_ALLOC_KEY, 1, Timestamp::MIN)
        .await
        .unwrap();
    assert_eq!(*oracle.times.lock().unwrap(), before);
    reader.expire().await;
    savepoint.expire().await;
    catalog.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported OpenSSL calls
async fn catalog_rejects_new_future_jumps_before_publication() {
    let oracle = Arc::new(TestOracle::default());
    // Inherited progress is not a blanket exemption for caller-chosen jumps.
    oracle.apply_write(20_000.into()).await;
    let mut catalog = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation()
        .with_timestamp_oracle(CatalogTimestampOracle::new(
            Arc::<TestOracle>::clone(&oracle),
            NowFn::from(|| 1_000),
        ))
        .unwrap_build()
        .await
        .open(1_000.into(), &test_bootstrap_args())
        .await
        .unwrap();
    let upper = catalog.current_upper().await;
    let next_id = catalog.get_next_id(USER_ITEM_ALLOC_KEY).await.unwrap();
    let read = oracle.read_ts().await;
    for timestamp in [Timestamp::from(50_000), Timestamp::MAX] {
        let error = catalog
            .allocate_id(USER_ITEM_ALLOC_KEY, 1, timestamp)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            CatalogError::Durable(DurableCatalogError::TimestampTooFarAhead { .. })
        ));
        let error = catalog.advance_upper(timestamp).await.unwrap_err();
        assert!(matches!(
            error,
            CatalogError::Durable(DurableCatalogError::TimestampTooFarAhead { .. })
        ));
        assert_eq!(catalog.current_upper().await, upper);
        assert_eq!(
            catalog.get_next_id(USER_ITEM_ALLOC_KEY).await.unwrap(),
            next_id
        );
        assert_eq!(oracle.read_ts().await, read);
        assert!(oracle.peek_write_ts().await < timestamp);
    }
    catalog.expire().await;
}
