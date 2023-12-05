// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::max;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;
use mz_storage_types::controller::EnablePersistTxnTables;
use timely::progress::Timestamp as TimelyTimestamp;

use mz_audit_log::{VersionedEvent, VersionedStorageUsage};
use mz_ore::cast::u64_to_usize;
use mz_ore::now::EpochMillis;
use mz_ore::soft_assert_eq_or_log;
use mz_proto::RustType;
use mz_repr::Timestamp;
use mz_storage_types::sources::Timeline;

use crate::durable::debug::{DebugCatalogState, Trace};
use crate::durable::objects::serialization::proto;
use crate::durable::objects::{
    DurableType, Snapshot, TimelineTimestamp, TimestampKey, TimestampValue,
};
use crate::durable::transaction::TransactionBatch;
use crate::durable::{
    BootstrapArgs, CatalogError, DurableCatalogState, Epoch, OpenableDurableCatalogState,
    ReadOnlyDurableCatalogState, Transaction, STORAGE_USAGE_ID_ALLOC_KEY,
};

macro_rules! compare_and_return {
    ($shadow:expr, $method:ident $(, $arg:expr)*) => {{
        let stash = $shadow.stash.$method($($arg.clone()),*);
        let persist = $shadow.persist.$method($($arg),*);
        soft_assert_eq_or_log!(stash, persist);
        stash
    }};
}

macro_rules! compare_and_return_async {
    ($shadow:expr, $method:ident $(, $arg:expr)*) => {{
        let stash = $shadow.stash.$method($($arg),*);
        let persist = $shadow.persist.$method($($arg),*);
        let (stash, persist) = futures::future::join(stash, persist).await;
        soft_assert_eq_or_log!(
            stash.is_ok(),
            persist.is_ok(),
            "stash and persist result variant do not match. stash: {stash:?}. persist: {persist:?}"
        );
        let stash = stash?;
        let persist = persist?;
        soft_assert_eq_or_log!(stash, persist);
        Ok(stash)
    }};
}

#[derive(Debug)]
pub(crate) struct OpenableShadowCatalogState<S, P>
where
    S: OpenableDurableCatalogState,
    P: OpenableDurableCatalogState,
{
    pub stash: Box<S>,
    pub persist: Box<P>,
}

#[async_trait]
impl<S, P> OpenableDurableCatalogState for OpenableShadowCatalogState<S, P>
where
    S: OpenableDurableCatalogState,
    P: OpenableDurableCatalogState,
{
    async fn open_savepoint(
        self: Box<Self>,
        boot_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        let stash =
            self.stash
                .open_savepoint(boot_ts.clone(), bootstrap_args, deploy_generation.clone());
        let persist = self
            .persist
            .open_savepoint(boot_ts, bootstrap_args, deploy_generation);
        let (stash, persist) = futures::future::join(stash, persist).await;
        soft_assert_eq_or_log!(
            stash.is_ok(),
            persist.is_ok(),
            "stash and persist result variant do not match. stash: {stash:?}. persist: {persist:?}"
        );
        let stash = stash?;
        let persist = persist?;
        Ok(Box::new(ShadowCatalogState::new(stash, persist).await?))
    }

    async fn open_read_only(
        self: Box<Self>,
        boot_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        let stash = self.stash.open_read_only(boot_ts.clone(), bootstrap_args);
        let persist = self.persist.open_read_only(boot_ts, bootstrap_args);
        let (stash, persist) = futures::future::join(stash, persist).await;
        soft_assert_eq_or_log!(
            stash.is_ok(),
            persist.is_ok(),
            "stash and persist result variant do not match. stash: {stash:?}. persist: {persist:?}"
        );
        let stash = stash?;
        let persist = persist?;
        Ok(Box::new(ShadowCatalogState::new_read_only(stash, persist)))
    }

    async fn open(
        self: Box<Self>,
        boot_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        let stash = self
            .stash
            .open(boot_ts.clone(), bootstrap_args, deploy_generation.clone());
        let persist = self
            .persist
            .open(boot_ts, bootstrap_args, deploy_generation);
        let (stash, persist) = futures::future::join(stash, persist).await;
        soft_assert_eq_or_log!(
            stash.is_ok(),
            persist.is_ok(),
            "stash and persist result variant do not match. stash: {stash:?}. persist: {persist:?}"
        );
        let stash = stash?;
        let persist = persist?;
        Ok(Box::new(ShadowCatalogState::new(stash, persist).await?))
    }

    async fn open_debug(mut self: Box<Self>) -> Result<DebugCatalogState, CatalogError> {
        panic!("ShadowCatalog is not used for catalog-debug tool");
    }

    async fn is_initialized(&mut self) -> Result<bool, CatalogError> {
        compare_and_return_async!(self, is_initialized)
    }

    async fn get_deployment_generation(&mut self) -> Result<Option<u64>, CatalogError> {
        compare_and_return_async!(self, get_deployment_generation)
    }

    async fn get_enable_persist_txn_tables(
        &mut self,
    ) -> Result<Option<EnablePersistTxnTables>, CatalogError> {
        compare_and_return_async!(self, get_enable_persist_txn_tables)
    }

    async fn trace(&mut self) -> Result<Trace, CatalogError> {
        panic!("ShadowCatalog is not used for catalog-debug tool");
    }

    async fn expire(self: Box<Self>) {
        futures::future::join(self.stash.expire(), self.persist.expire()).await;
    }
}

#[derive(Debug)]
pub struct ShadowCatalogState {
    pub stash: Box<dyn DurableCatalogState>,
    pub persist: Box<dyn DurableCatalogState>,
}

impl ShadowCatalogState {
    async fn new(
        stash: Box<dyn DurableCatalogState>,
        persist: Box<dyn DurableCatalogState>,
    ) -> Result<ShadowCatalogState, CatalogError> {
        let mut state = ShadowCatalogState { stash, persist };
        state.fix_timestamps().await?;
        state.fix_storage_usage().await?;
        Ok(state)
    }

    fn new_read_only(
        stash: Box<dyn DurableCatalogState>,
        persist: Box<dyn DurableCatalogState>,
    ) -> ShadowCatalogState {
        // We cannot fix timestamp discrepancies in a read-only catalog, so we'll just have to
        // ignore them.
        ShadowCatalogState { stash, persist }
    }

    /// The Coordinator will update the timestamps of every timeline continuously on an interval.
    /// If we shut down the Coordinator while it's updating the timestamps, then it's possible that
    /// only one catalog implementation is updated, while the other is not. This will leave the two
    /// catalogs in an inconsistent state. Since this implementation is just used for tests, and
    /// that specific inconsistency is expected, we fix it during open.
    async fn fix_timestamps(&mut self) -> Result<(), CatalogError> {
        let stash_timestamps = self.stash_timestamps().await?;
        let persist_timestamps = self.persist_timestamps().await?;
        let reconciled_timestamps =
            self.reconciled_timestamps(stash_timestamps, persist_timestamps);
        for (timeline, timestamp) in reconciled_timestamps {
            self.stash.set_timestamp(&timeline, timestamp).await?;
            self.persist.set_timestamp(&timeline, timestamp).await?;
        }

        Ok(())
    }

    async fn stash_timestamps(&mut self) -> Result<BTreeMap<Timeline, Timestamp>, CatalogError> {
        Ok(self
            .stash
            .get_timestamps()
            .await?
            .into_iter()
            .map(|timeline_timestamp| (timeline_timestamp.timeline, timeline_timestamp.ts))
            .collect())
    }

    async fn persist_timestamps(&mut self) -> Result<BTreeMap<Timeline, Timestamp>, CatalogError> {
        Ok(self
            .persist
            .get_timestamps()
            .await?
            .into_iter()
            .map(|timeline_timestamp| (timeline_timestamp.timeline, timeline_timestamp.ts))
            .collect())
    }

    fn reconciled_timestamps(
        &mut self,
        stash_timestamps: BTreeMap<Timeline, Timestamp>,
        persist_timestamps: BTreeMap<Timeline, Timestamp>,
    ) -> BTreeMap<Timeline, Timestamp> {
        let mut reconciled = stash_timestamps;

        for (timeline, ts) in persist_timestamps {
            match reconciled.get(&timeline) {
                Some(reconciled_ts) => {
                    if reconciled_ts < &ts {
                        reconciled.insert(timeline, ts);
                    }
                }
                None => {
                    reconciled.insert(timeline, ts);
                }
            }
        }

        reconciled
    }

    /// The Coordinator will update storage usage continuously on an interval.
    /// If we shut down the Coordinator while it's updating storage usage, then it's possible that
    /// only one catalog implementation is updated, while the other is not. This will leave the two
    /// catalogs in an inconsistent state. Since this implementation is just used for tests, and
    /// that specific inconsistency is expected, we fix it during open.
    async fn fix_storage_usage(&mut self) -> Result<(), CatalogError> {
        let stash_storage_usage_id = self.stash.get_next_id(STORAGE_USAGE_ID_ALLOC_KEY).await?;
        let persist_storage_usage_id = self.persist.get_next_id(STORAGE_USAGE_ID_ALLOC_KEY).await?;
        if stash_storage_usage_id > persist_storage_usage_id {
            let diff = stash_storage_usage_id - persist_storage_usage_id;
            let _ = self
                .persist
                .allocate_id(STORAGE_USAGE_ID_ALLOC_KEY, diff)
                .await?;
            let stash_storage_usage = self
                .stash
                .get_and_prune_storage_usage(None, Timestamp::minimum())
                .await?;
            let mut txn = self.persist.transaction().await?;
            for event in &stash_storage_usage[u64_to_usize(persist_storage_usage_id)..] {
                txn.insert_storage_usage_event(event.clone());
            }
            txn.commit().await?;
        } else if persist_storage_usage_id > stash_storage_usage_id {
            let diff = persist_storage_usage_id - stash_storage_usage_id;
            let _ = self
                .stash
                .allocate_id(STORAGE_USAGE_ID_ALLOC_KEY, diff)
                .await?;
            let persist_storage_usage = self
                .persist
                .get_and_prune_storage_usage(None, Timestamp::minimum())
                .await?;
            let mut txn = self.stash.transaction().await?;
            for event in &persist_storage_usage[u64_to_usize(stash_storage_usage_id)..] {
                txn.insert_storage_usage_event(event.clone());
            }
            txn.commit().await?;
        }

        Ok(())
    }
}

#[async_trait]
impl ReadOnlyDurableCatalogState for ShadowCatalogState {
    fn epoch(&mut self) -> Epoch {
        compare_and_return!(self, epoch)
    }

    async fn expire(self: Box<Self>) {
        futures::future::join(self.stash.expire(), self.persist.expire()).await;
    }

    async fn get_timestamps(&mut self) -> Result<Vec<TimelineTimestamp>, CatalogError> {
        if self.is_read_only() {
            // Read-only catalogs cannot fix timestamps so we must ignore them. See
            // `Self::fix_timestamps`.
            let stash_timestamps = self.stash_timestamps().await?;
            let persist_timestamps = self.persist_timestamps().await?;
            Ok(self
                .reconciled_timestamps(stash_timestamps, persist_timestamps)
                .into_iter()
                .map(|(timeline, ts)| TimelineTimestamp { timeline, ts })
                .collect())
        } else {
            compare_and_return_async!(self, get_timestamps)
        }
    }

    async fn get_audit_logs(&mut self) -> Result<Vec<VersionedEvent>, CatalogError> {
        compare_and_return_async!(self, get_audit_logs)
    }

    async fn get_next_id(&mut self, id_type: &str) -> Result<u64, CatalogError> {
        if self.is_read_only() && id_type == STORAGE_USAGE_ID_ALLOC_KEY {
            // Read-only catalogs cannot fix storage usage so we must ignore them. See
            // `Self::fix_storage_usage`.
            let stash_storage_usage_id = self.stash.get_next_id(STORAGE_USAGE_ID_ALLOC_KEY).await?;
            let persist_storage_usage_id =
                self.persist.get_next_id(STORAGE_USAGE_ID_ALLOC_KEY).await?;
            Ok(max(stash_storage_usage_id, persist_storage_usage_id))
        } else {
            compare_and_return_async!(self, get_next_id, id_type)
        }
    }

    async fn has_system_config_synced_once(&mut self) -> Result<bool, CatalogError> {
        compare_and_return_async!(self, has_system_config_synced_once)
    }

    async fn snapshot(&mut self) -> Result<Snapshot, CatalogError> {
        if self.is_read_only() {
            // Read-only catalogs cannot fix timestamps or storage usage ID so we must ignore them.
            // See `Self::fix_timestamps` and `Self::fix_storage_usage`.
            let stash = self.stash.snapshot();
            let persist = self.persist.snapshot();
            let (stash, persist) = futures::future::join(stash, persist).await;
            soft_assert_eq_or_log!(
                stash.is_ok(),
                persist.is_ok(),
                "stash and persist result variant do not match. stash: {stash:?}. persist: {persist:?}"
            );
            let mut stash = stash?;
            let mut persist = persist?;
            let stash_timestamps = stash
                .timestamps
                .into_iter()
                .map(|(timeline, timestamp)| {
                    (
                        TimestampKey::from_proto(timeline).expect("invalid proto persisted"),
                        TimestampValue::from_proto(timestamp).expect("invalid proto persisted"),
                    )
                })
                .map(|(k, v)| DurableType::from_key_value(k, v))
                .map(|timeline_timestamp: TimelineTimestamp| {
                    (timeline_timestamp.timeline, timeline_timestamp.ts)
                })
                .collect();
            let persist_timestamps = persist
                .timestamps
                .into_iter()
                .map(|(timeline, timestamp)| {
                    (
                        TimestampKey::from_proto(timeline).expect("invalid proto persisted"),
                        TimestampValue::from_proto(timestamp).expect("invalid proto persisted"),
                    )
                })
                .map(|(k, v)| DurableType::from_key_value(k, v))
                .map(|timeline_timestamp: TimelineTimestamp| {
                    (timeline_timestamp.timeline, timeline_timestamp.ts)
                })
                .collect();
            let reconciled_timestamps: BTreeMap<_, _> = self
                .reconciled_timestamps(stash_timestamps, persist_timestamps)
                .into_iter()
                .map(|(timeline, ts)| {
                    let timeline_timestamp = TimelineTimestamp { timeline, ts };
                    timeline_timestamp.into_key_value()
                })
                .map(|(k, v)| (k.into_proto(), v.into_proto()))
                .collect();
            stash.timestamps = reconciled_timestamps.clone();
            persist.timestamps = reconciled_timestamps;
            let stash_storage_usage_id = stash
                .id_allocator
                .get(&proto::IdAllocKey {
                    name: STORAGE_USAGE_ID_ALLOC_KEY.to_string(),
                })
                .expect("storage usage id alloc key must exist")
                .next_id;
            let persist_storage_usage_id = persist
                .id_allocator
                .get(&proto::IdAllocKey {
                    name: STORAGE_USAGE_ID_ALLOC_KEY.to_string(),
                })
                .expect("storage usage id alloc key must exist")
                .next_id;
            let reconciled_storage_usage_id = max(stash_storage_usage_id, persist_storage_usage_id);
            stash.id_allocator.insert(
                proto::IdAllocKey {
                    name: STORAGE_USAGE_ID_ALLOC_KEY.to_string(),
                },
                proto::IdAllocValue {
                    next_id: reconciled_storage_usage_id,
                },
            );
            persist.id_allocator.insert(
                proto::IdAllocKey {
                    name: STORAGE_USAGE_ID_ALLOC_KEY.to_string(),
                },
                proto::IdAllocValue {
                    next_id: reconciled_storage_usage_id,
                },
            );
            soft_assert_eq_or_log!(stash, persist);
            Ok(stash)
        } else {
            compare_and_return_async!(self, snapshot)
        }
    }
}

#[async_trait]
impl DurableCatalogState for ShadowCatalogState {
    fn is_read_only(&self) -> bool {
        compare_and_return!(self, is_read_only)
    }

    async fn transaction(&mut self) -> Result<Transaction, CatalogError> {
        // We don't actually want to return this transaction since it's specific to the stash. We
        // just want to compare results.
        if self.is_read_only() {
            let stash = self.stash.transaction();
            let persist = self.persist.transaction();
            let (stash, persist) = futures::future::join(stash, persist).await;
            soft_assert_eq_or_log!(
            stash.is_ok(),
            persist.is_ok(),
            "stash and persist result variant do not match. stash: {stash:?}. persist: {persist:?}"
        );
            let _ = stash?;
            let _ = persist?;
            // We can't actually compare the contents because the timestamps and storage usage IDs
            // may have diverged. When the transaction commits we'll check that they both had the
            // same effect so it's probably fine. Read-only catalogs should not really being
            // transacting a lot either. See `Self::fix_timestamps` and `Self::fix_storage_usage`.
        } else {
            let res: Result<_, CatalogError> = compare_and_return_async!(self, transaction);
            res?;
        }

        // Return a transaction with a reference to the shadow catalog so the commit is applied to
        // both implementations.
        let snapshot = self.snapshot().await?;
        Transaction::new(self, snapshot)
    }

    async fn commit_transaction(
        &mut self,
        txn_batch: TransactionBatch,
    ) -> Result<(), CatalogError> {
        let res = compare_and_return_async!(self, commit_transaction, txn_batch.clone());
        // After committing a transaction, check that both implementations return the same snapshot
        // to ensure that the commit had the same effect on the underlying state. Call
        // `self.snapshot()` directly to avoid timestamp and storage usage ID discrepancies.
        let _: Result<_, CatalogError> = self.snapshot().await;
        res
    }

    async fn confirm_leadership(&mut self) -> Result<(), CatalogError> {
        compare_and_return_async!(self, confirm_leadership)
    }

    async fn get_and_prune_storage_usage(
        &mut self,
        retention_period: Option<Duration>,
        boot_ts: Timestamp,
    ) -> Result<Vec<VersionedStorageUsage>, CatalogError> {
        if self.is_read_only() {
            // Read-only catalogs cannot fix storage usage so we must ignore them. See
            // `Self::fix_storage_usage`.
            let stash_storage_usage = self
                .stash
                .get_and_prune_storage_usage(retention_period, boot_ts)
                .await?;
            let persist_storage_usage = self
                .stash
                .get_and_prune_storage_usage(retention_period, boot_ts)
                .await?;
            if stash_storage_usage.len() >= persist_storage_usage.len() {
                Ok(stash_storage_usage)
            } else {
                Ok(persist_storage_usage)
            }
        } else {
            compare_and_return_async!(self, get_and_prune_storage_usage, retention_period, boot_ts)
        }
    }

    async fn set_timestamp(
        &mut self,
        timeline: &Timeline,
        timestamp: Timestamp,
    ) -> Result<(), CatalogError> {
        compare_and_return_async!(self, set_timestamp, timeline, timestamp)
    }

    async fn allocate_id(&mut self, id_type: &str, amount: u64) -> Result<Vec<u64>, CatalogError> {
        compare_and_return_async!(self, allocate_id, id_type, amount)
    }
}
