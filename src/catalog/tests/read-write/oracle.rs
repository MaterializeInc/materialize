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
async fn explicit_retirement_is_scoped_and_waits_for_oracle_completion() {
    use std::collections::BTreeMap;

    use mz_catalog::durable::objects::{ClientRealization, DurableType, Item};
    use mz_catalog::durable::{DEFAULT_ADAPTER_ID, FenceError, Transaction};
    use mz_persist_client::ShardId;
    use mz_repr::role_id::RoleId;
    use mz_repr::{CatalogItemId, GlobalId, RelationVersion};
    use mz_sql::names::{CommentObjectId, SchemaId};
    use mz_storage_client::controller::StorageTxn;
    use uuid::Uuid;

    fn insert(txn: &mut Transaction<'_>, id: u64, owner: Uuid, incarnation: u64, shard: ShardId) {
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
            Some(incarnation),
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
    let metrics = Arc::new(mz_catalog::durable::Metrics::new(
        &mz_ore::metrics::MetricsRegistry::new(),
    ));
    let builder = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_metrics(Arc::clone(&metrics))
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
    txn.set_collection_compaction_bound(GlobalId::User(1000), Some(10.into()))
        .unwrap();
    let old_client = txn
        .create_client_incarnation(ClientRealization::Adapter(DEFAULT_ADAPTER_ID))
        .unwrap();
    insert(&mut txn, 1000, old_owner, old_client, old_shard);
    txn.grant_client_read_requirements(
        old_client,
        BTreeMap::from([(GlobalId::User(1000), 10.into())]),
    )
    .unwrap();
    commit(txn).await;

    let next = builder.with_deploy_generation(8);
    let mut peer = next
        .clone()
        .unwrap_build()
        .await
        .join_prewarming("0.0.0+pending")
        .await
        .unwrap();
    let mut promoted = next
        .unwrap_build()
        .await
        .open_for_promotion(1_000.into(), &test_bootstrap_args())
        .await
        .unwrap();
    active.sync_to_current_updates().await.unwrap();
    assert!(
        !active
            .client_incarnation_retirement()
            .unwrap()
            .is_retired(old_client)
    );
    let mut txn = active.transaction().await.unwrap();
    txn.set_config("predecessor_still_writable".into(), Some(1))
        .unwrap();
    commit(txn).await;
    peer.sync_to_current_updates().await.unwrap();
    let fresh_owner = Uuid::new_v4();
    let fresh_shard = ShardId::new();
    let mut txn = peer.transaction().await.unwrap();
    let fresh_client = txn
        .create_client_incarnation(ClientRealization::Adapter(DEFAULT_ADAPTER_ID))
        .unwrap();
    insert(&mut txn, 2000, fresh_owner, fresh_client, fresh_shard);
    txn.grant_client_read_requirements(
        fresh_client,
        BTreeMap::from([(GlobalId::User(1000), 20.into())]),
    )
    .unwrap();
    commit(txn).await;

    // Race the explicit retirement after it has selected its cleanup snapshot.
    let cleanup_gate = Arc::new(Notify::new());
    *oracle.allocation_gate.lock().unwrap() = Some(Arc::clone(&cleanup_gate));
    let retiring = mz_ore::task::spawn(|| "retirement with concurrent publication", async move {
        let retired = promoted.retire_deployment(7).await.unwrap();
        (promoted, retired)
    });
    tokio::time::timeout(Duration::from_secs(5), oracle.allocation_started.notified())
        .await
        .unwrap();
    let later_owner = Uuid::new_v4();
    let later_shard = ShardId::new();
    let mut txn = peer.transaction().await.unwrap();
    insert(&mut txn, 3000, later_owner, fresh_client, later_shard);
    commit(txn).await;
    cleanup_gate.notify_one();
    let (mut promoted, retired) = tokio::time::timeout(Duration::from_secs(5), retiring)
        .await
        .unwrap();
    assert!(
        !retired,
        "retirement must retry after a competing publication"
    );
    active.sync_to_current_updates().await.unwrap();
    assert!(
        !active
            .client_incarnation_retirement()
            .unwrap()
            .is_retired(old_client)
    );

    // A second public attempt reuses preparation, but still rejects a fresh
    // content race. Ending that retry sequence releases its local raw rows.
    let snapshots = metrics.snapshots_taken.get();
    let cleanup_gate = Arc::new(Notify::new());
    *oracle.allocation_gate.lock().unwrap() = Some(Arc::clone(&cleanup_gate));
    let retiring = mz_ore::task::spawn(|| "retirement preparation reuse", async move {
        let result = promoted.retire_deployment(7).await.unwrap();
        (promoted, result)
    });
    tokio::time::timeout(Duration::from_secs(5), oracle.allocation_started.notified())
        .await
        .unwrap();
    assert_eq!(metrics.snapshots_taken.get(), snapshots);
    let mut txn = peer.transaction().await.unwrap();
    txn.set_config("retirement_peer_conflict".into(), Some(1))
        .unwrap();
    commit(txn).await;
    cleanup_gate.notify_one();
    let (mut promoted, retired) = tokio::time::timeout(Duration::from_secs(5), retiring)
        .await
        .unwrap();
    assert!(!retired);
    promoted.discard_retirement_preparation();

    // Unlike content, empty upper progress must preserve the prepared retirement.
    let cleanup_gate = Arc::new(Notify::new());
    *oracle.allocation_gate.lock().unwrap() = Some(Arc::clone(&cleanup_gate));
    let retiring = mz_ore::task::spawn(|| "retirement oracle completion", async move {
        assert!(promoted.retire_deployment(7).await.unwrap());
        promoted
    });
    tokio::time::timeout(Duration::from_secs(5), oracle.allocation_started.notified())
        .await
        .unwrap();
    assert_eq!(metrics.snapshots_taken.get(), snapshots + 1);
    let upper = peer.current_upper().await;
    peer.advance_upper(upper.step_forward()).await.unwrap();

    // Exclusion is durable before acknowledgement, which waits for the oracle.
    let completion_gate = Arc::new(Notify::new());
    *oracle.completion_gate.lock().unwrap() = Some(Arc::clone(&completion_gate));
    cleanup_gate.notify_one();
    tokio::time::timeout(Duration::from_secs(5), oracle.completion_started.notified())
        .await
        .unwrap();
    assert!(
        !retiring.is_finished(),
        "retirement acknowledged before oracle completion"
    );
    assert!(matches!(
        active.sync_to_current_updates().await,
        Err(CatalogError::Durable(DurableCatalogError::Fence(
            FenceError::DeploymentRetired { generation: 7 }
        )))
    ));
    peer.sync_to_current_updates().await.unwrap();
    let retirement = peer.client_incarnation_retirement().unwrap();
    assert!(retirement.is_retired(old_client));
    assert!(!retirement.is_retired(fresh_client));
    *oracle.completion_gate.lock().unwrap() = None;
    completion_gate.notify_one();
    let mut promoted = tokio::time::timeout(Duration::from_secs(5), retiring)
        .await
        .unwrap();
    let committed = promoted.current_upper().await.step_back().unwrap();
    assert!(oracle.read_ts().await >= committed);
    // The surviving grant still constrains compaction after retirement.
    promoted.sync_to_current_updates().await.unwrap();
    let mut txn = promoted.transaction().await.unwrap();
    txn.set_collection_compaction_bound(GlobalId::User(1000), Some(20.into()))
        .unwrap();
    commit(txn).await;
    let mut txn = promoted.transaction().await.unwrap();
    txn.set_collection_compaction_bound(GlobalId::User(1000), Some(21.into()))
        .unwrap();
    let ts = txn.upper();
    let _ = txn.get_and_commit_op_updates();
    assert!(matches!(
        txn.commit(ts).await,
        Err(CatalogError::Durable(
            DurableCatalogError::InvalidReadProtection(_)
        ))
    ));

    // Session ownership is not deployment ownership. Retirement must not
    // invent an all-session temporary-object cleanup policy.
    let snapshot = promoted.snapshot().await.unwrap();
    let owners: Vec<_> = snapshot
        .items
        .into_iter()
        .map(|(key, value)| Item::from_key_value(key, value))
        .filter_map(|item| item.ephemeral_owner_session)
        .collect();
    assert_eq!(owners.len(), 3);
    assert!(owners.contains(&old_owner));
    assert!(owners.contains(&fresh_owner));
    assert!(owners.contains(&later_owner));
    assert_eq!(snapshot.comments.len(), 3);
    promoted.sync_to_current_updates().await.unwrap();
    let txn = promoted.transaction().await.unwrap();
    assert_eq!(
        txn.get_collection_metadata(),
        BTreeMap::from([
            (GlobalId::User(1000), old_shard),
            (GlobalId::User(1001), old_shard),
            (GlobalId::User(2000), fresh_shard),
            (GlobalId::User(2001), fresh_shard),
            (GlobalId::User(3000), later_shard),
            (GlobalId::User(3001), later_shard),
        ])
    );
    assert_eq!(
        txn.get_unfinalized_shards(),
        std::collections::BTreeSet::new()
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
async fn catalog_reuses_supplied_allocation_across_rebase() {
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
                handle_purpose: "race and observe allocated catalog commit".into(),
            },
        )
        .await
        .unwrap();
    catalog.sync_to_current_updates().await.unwrap();
    let start_id = catalog.get_next_id(USER_ITEM_ALLOC_KEY).await.unwrap();
    let preceding_read = oracle.read_ts().await;
    let allocation = oracle.write_ts().await;
    let allocated = allocation.timestamp;
    assert!(allocated > preceding_read);
    let before = *oracle.times.lock().unwrap();
    let mut txn = catalog.transaction().await.unwrap();
    let upper = txn.upper();
    txn.get_and_increment_id_by(USER_ITEM_ALLOC_KEY.into(), 1)
        .unwrap();
    let _ = txn.get_and_commit_op_updates();

    // Empty progress races the prepared transaction and overtakes its allocation.
    let overtaken = upper.max(allocated).saturating_add(100);
    raw.compare_and_append(
        Vec::<((SourceData, ()), Timestamp, i64)>::new(),
        Antichain::from_elem(upper),
        Antichain::from_elem(overtaken),
    )
    .await
    .unwrap()
    .unwrap();
    let gate = Arc::new(Notify::new());
    *oracle.completion_gate.lock().unwrap() = Some(Arc::clone(&gate));
    {
        let commit = txn.commit_with_upper(allocation);
        tokio::pin!(commit);
        tokio::select! {
            result = &mut commit => panic!("acknowledged before oracle completion: {result:?}"),
            result = tokio::time::timeout(
                Duration::from_secs(5),
                oracle.completion_started.notified(),
            ) => result.expect("catalog did not complete its timestamp"),
        }
        let committed = raw
            .fetch_recent_upper()
            .await
            .as_option()
            .unwrap()
            .step_back()
            .unwrap();
        assert!(committed >= overtaken);
        // apply_write is gated, so any extra write_ts call would advance times.1.
        assert_eq!(*oracle.times.lock().unwrap(), before);
        assert!(oracle.read_ts().await < committed);
        assert!(futures::poll!(&mut commit).is_pending());
        gate.notify_one();
        let committed_upper = tokio::time::timeout(Duration::from_secs(5), commit)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(committed_upper, committed.step_forward());
        assert_eq!(oracle.read_ts().await, committed);
    }
    assert_eq!(
        catalog.get_next_id(USER_ITEM_ALLOC_KEY).await.unwrap(),
        start_id + 1
    );
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

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported OpenSSL calls
async fn retired_temporary_cleanup_rebuilds_after_peer_publication() {
    use std::collections::{BTreeMap, BTreeSet};

    use mz_catalog::catalog::{Catalog, CatalogError as CoreCatalogError, Op};
    use mz_catalog::durable::objects::{Comment, DurableType, Item};
    use mz_catalog::durable::{
        AdapterDeclaration, ClientRealization, DEFAULT_ADAPTER_ID, Transaction,
    };
    use mz_catalog::memory::error::ErrorKind;
    use mz_persist_client::ShardId;
    use mz_pgrepr::oid::FIRST_USER_OID;
    use mz_repr::{CatalogItemId, GlobalId};
    use mz_sql::names::{CommentObjectId, SchemaSpecifier};
    use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
    use mz_storage_client::controller::StorageTxn;
    use uuid::Uuid;

    async fn commit(mut txn: Transaction<'_>) {
        let ts = txn.upper();
        let _ = txn.get_and_commit_op_updates();
        txn.commit(ts).await.unwrap();
    }

    let persist = PersistClient::new_for_tests().await;
    let organization = Uuid::new_v4();
    let oracle = Arc::new(TestOracle::default());
    let now = NowFn::from(|| 1_000);
    let builder = TestCatalogStateBuilder::new(persist.clone())
        .with_organization_id(organization)
        .with_default_deploy_generation()
        .with_timestamp_oracle(CatalogTimestampOracle::new(
            Arc::<TestOracle>::clone(&oracle),
            now.clone(),
        ));
    let bootstrap = test_bootstrap_args();
    let storage = builder
        .clone()
        .unwrap_build()
        .await
        .open(1_000.into(), &bootstrap)
        .await
        .unwrap();
    let mut catalog = Catalog::open_debug_catalog_inner(
        persist,
        storage,
        now,
        Some(format!("local-az1-{organization}-0").parse().unwrap()),
        &mz_build_info::DUMMY_BUILD_INFO,
        BTreeMap::from([("enable_catalog_read_protection".into(), "true".into())]),
        &bootstrap,
        None,
        None,
    )
    .await
    .unwrap();
    assert!(catalog.state().catalog_read_protection_enabled());
    let mut peer = builder.unwrap_build().await.join().await.unwrap();
    peer.sync_to_current_updates().await.unwrap();

    let global_id = GlobalId::User(100_000);
    let old_item = CatalogItemId::User(100_000);
    let alias_item = CatalogItemId::User(100_001);
    let shard = ShardId::new();
    // Durable aliases deliberately model shared storage membership, including
    // temporary definitions that this adapter cannot resolve in its SQL catalog.
    let insert = |txn: &mut Transaction<'_>, item, name: &str, incarnation, oid| {
        txn.insert_item(
            item,
            oid,
            global_id,
            SchemaSpecifier::Temporary.into(),
            name,
            format!("CREATE TEMPORARY TABLE mz_temp.{name} (a pg_catalog.int4)"),
            MZ_SYSTEM_ROLE_ID,
            vec![],
            BTreeMap::new(),
            Some(Uuid::new_v4()),
            Some(incarnation),
        )
        .unwrap();
        for column in [None, Some(0)] {
            txn.update_comment(CommentObjectId::Table(item), column, Some(name.to_owned()))
                .unwrap();
        }
    };

    let mut txn = peer.transaction().await.unwrap();
    let old_adapter = txn.allocate_adapter_id().unwrap();
    txn.set_adapter_declaration(AdapterDeclaration {
        id: old_adapter,
        name: "retiring_adapter".into(),
        size: txn
            .get_adapter_declaration(DEFAULT_ADAPTER_ID)
            .unwrap()
            .size,
    })
    .unwrap();
    let old_execution = txn
        .create_client_incarnation(ClientRealization::Adapter(old_adapter))
        .unwrap();
    let live_execution = txn
        .create_client_incarnation(ClientRealization::Adapter(DEFAULT_ADAPTER_ID))
        .unwrap();
    insert(&mut txn, old_item, "retired", old_execution, FIRST_USER_OID);
    txn.insert_collection_metadata(BTreeMap::from([(global_id, shard)]))
        .unwrap();
    txn.set_collection_compaction_bound(global_id, Some(Timestamp::MIN))
        .unwrap();
    txn.grant_client_read_requirements(
        old_execution,
        BTreeMap::from([(global_id, Timestamp::MIN)]),
    )
    .unwrap();
    commit(txn).await;

    // The Item is committed while its creator is live. Removing that adapter
    // retires its execution and grants, but leaves the other adapter alive.
    let mut txn = peer.transaction().await.unwrap();
    assert!(txn.remove_adapter_declaration(old_adapter).is_some());
    commit(txn).await;
    catalog.sync_to_current_updates().await.unwrap();
    assert!(
        !catalog
            .state()
            .client_incarnations()
            .contains_key(&old_execution)
    );
    assert!(
        catalog
            .state()
            .client_incarnations()
            .contains_key(&live_execution)
    );
    assert!(
        !catalog
            .state()
            .client_read_requirements()
            .contains_key(&(old_execution, global_id))
    );
    let owners = BTreeSet::from([old_execution]);
    assert_eq!(catalog.state().temporary_item_cleanup_batch(16), owners);
    let cleanup = Op::ReclaimRetiredTemporaryItems {
        owners: owners.clone(),
    };
    let ts = catalog.current_upper().await;
    let (candidate, _) = catalog
        .transact_incremental_dry_run(catalog.state(), vec![cleanup.clone()], None, None, ts)
        .await
        .unwrap();
    assert!(
        !candidate
            .storage_metadata()
            .collection_metadata
            .contains_key(&global_id)
    );
    assert!(
        !candidate
            .collection_compaction_bounds()
            .contains_key(&global_id)
    );
    // Dry runs model collection deletion, but deliberately do not publish to
    // the finalization WAL. The committing candidate supplies that effect.
    assert!(candidate.client_read_frontier(global_id).is_none());
    assert!(
        !candidate
            .storage_metadata()
            .retained_collections
            .contains(&global_id)
    );
    drop(candidate);

    let before = catalog.state().storage_metadata().clone();
    let cleanup_gate = Arc::new(Notify::new());
    *oracle.allocation_gate.lock().unwrap() = Some(Arc::clone(&cleanup_gate));
    let peer_snapshot = {
        // A plain Timestamp forces allocation after candidate preparation, at
        // durable append. No catch-up context may retry the stale candidate.
        let pending = catalog.transact(None, None, ts, None, vec![cleanup.clone()]);
        tokio::pin!(pending);
        tokio::select! {
            result = &mut pending => panic!("cleanup finished before allocation gate: {:?}", result.err()),
            result = tokio::time::timeout(
                Duration::from_secs(5), oracle.allocation_started.notified(),
            ) => result.expect("cleanup did not reach its commit allocation"),
        }

        let mut txn = peer.transaction().await.unwrap();
        insert(
            &mut txn,
            alias_item,
            "alias",
            live_execution,
            FIRST_USER_OID + 1,
        );
        txn.grant_client_read_requirements(
            live_execution,
            BTreeMap::from([(global_id, Timestamp::MIN)]),
        )
        .unwrap();
        commit(txn).await;
        let snapshot = peer.snapshot().await.unwrap();
        cleanup_gate.notify_one();
        let error = tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .expect("cleanup did not finish after gate release")
            .err()
            .expect("stale cleanup must conflict with peer publication");
        assert!(matches!(
            error,
            CoreCatalogError::Catalog(error)
                if matches!(error.kind, ErrorKind::Durable(DurableCatalogError::CatalogOutOfSync { .. }))
        ));
        snapshot
    };

    // Both the durable publication and the in-memory candidate are atomic.
    peer.sync_to_current_updates().await.unwrap();
    assert_eq!(peer.snapshot().await.unwrap(), peer_snapshot);
    assert_eq!(
        catalog.state().storage_metadata().collection_metadata,
        before.collection_metadata
    );
    assert_eq!(
        catalog.state().storage_metadata().unfinalized_shards,
        before.unfinalized_shards
    );
    assert_eq!(
        catalog.state().storage_metadata().compaction_bounds,
        before.compaction_bounds
    );
    assert_eq!(catalog.state().temporary_item_cleanup_batch(16), owners);

    catalog.sync_to_current_updates().await.unwrap();
    assert_eq!(
        catalog.storage().await.snapshot().await.unwrap(),
        peer_snapshot
    );
    let ts = catalog.current_upper().await;
    catalog
        .transact(None, None, ts, None, vec![cleanup])
        .await
        .unwrap();
    assert!(catalog.state().temporary_item_cleanup_batch(16).is_empty());
    peer.sync_to_current_updates().await.unwrap();
    let after = peer.snapshot().await.unwrap();
    let expected_items: imbl::OrdMap<_, _> = peer_snapshot
        .items
        .clone()
        .into_iter()
        .filter(|(key, value)| Item::from_key_value(key.clone(), value.clone()).id != old_item)
        .collect();
    assert_eq!(after.items, expected_items);
    let expected_comments: imbl::OrdMap<_, _> = peer_snapshot
        .comments
        .clone()
        .into_iter()
        .filter(|(key, value)| {
            Comment::from_key_value(key.clone(), value.clone()).object_id
                != CommentObjectId::Table(old_item)
        })
        .collect();
    assert_eq!(after.comments, expected_comments);
    assert_eq!(after.items.len() + 1, peer_snapshot.items.len());
    assert_eq!(after.comments.len() + 2, peer_snapshot.comments.len());
    assert_eq!(
        after.storage_collection_metadata,
        peer_snapshot.storage_collection_metadata
    );
    assert_eq!(
        after.collection_compaction_bounds,
        peer_snapshot.collection_compaction_bounds
    );
    assert_eq!(
        after.client_read_requirements,
        peer_snapshot.client_read_requirements
    );
    assert_eq!(after.unfinalized_shards, peer_snapshot.unfinalized_shards);
    assert_eq!(
        catalog
            .state()
            .storage_metadata()
            .collection_metadata
            .get(&global_id),
        Some(&shard)
    );
    assert_eq!(
        catalog
            .state()
            .client_read_requirements()
            .get(&(live_execution, global_id)),
        Some(&Timestamp::MIN)
    );
    assert!(
        !catalog
            .state()
            .storage_metadata()
            .unfinalized_shards
            .contains(&shard)
    );
    assert!(
        !catalog
            .state()
            .storage_metadata()
            .retained_collections
            .contains(&global_id)
    );
    peer.expire().await;
    catalog.expire().await;
}
