// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;

use mz_audit_log::{VersionedEvent, VersionedStorageUsage};
use mz_ore::now::EpochMillis;
use mz_ore::soft_assert_eq_or_log;
use mz_repr::Timestamp;
use mz_storage_types::sources::Timeline;

use crate::durable::debug::{DebugCatalogState, Trace};
use crate::durable::objects::{Snapshot, TimelineTimestamp};
use crate::durable::transaction::TransactionBatch;
use crate::durable::{
    BootstrapArgs, CatalogError, DurableCatalogState, Epoch, OpenableDurableCatalogState,
    ReadOnlyDurableCatalogState, Transaction,
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
        let mut state = ShadowCatalogState { stash, persist };
        state.fix_timestamps().await?;
        Ok(Box::new(state))
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
        Ok(Box::new(ShadowCatalogState { stash, persist }))
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
        let mut state = ShadowCatalogState { stash, persist };
        state.fix_timestamps().await?;
        Ok(Box::new(state))
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

    async fn get_enable_persist_txn_tables(&mut self) -> Result<Option<bool>, CatalogError> {
        compare_and_return_async!(self, get_enable_persist_txn_tables)
    }

    async fn trace(&mut self) -> Result<Trace, CatalogError> {
        panic!("ShadowCatalog is not used for catalog-debug tool");
    }

    async fn expire(self) {
        futures::future::join(self.stash.expire(), self.persist.expire()).await;
    }
}

#[derive(Debug)]
pub struct ShadowCatalogState {
    pub stash: Box<dyn DurableCatalogState>,
    pub persist: Box<dyn DurableCatalogState>,
}

impl ShadowCatalogState {
    /// The Coordinator will update the timestamps of every timeline continuously on an interval.
    /// If we shut down the Coordinator while it's updating the timestamps, then it's possible that
    /// only one catalog implementation is updated, while the other is not. This will leave the two
    /// catalogs in an inconsistent state. Since this implementation is just used for tests, and
    /// that specific inconsistency is expected, we fix it during open.
    async fn fix_timestamps(&mut self) -> Result<(), CatalogError> {
        let mut stash_timelines: BTreeMap<_, _> = self
            .stash
            .get_timestamps()
            .await?
            .into_iter()
            .map(|timeline_timestamp| (timeline_timestamp.timeline, timeline_timestamp.ts))
            .collect();

        for TimelineTimestamp { timeline, ts } in self.persist.get_timestamps().await? {
            match stash_timelines.remove(&timeline) {
                Some(stash_ts) => match stash_ts.cmp(&ts) {
                    Ordering::Less => self.stash.set_timestamp(&timeline, ts).await?,
                    Ordering::Greater => self.persist.set_timestamp(&timeline, ts).await?,
                    Ordering::Equal => {}
                },
                None => self.stash.set_timestamp(&timeline, ts).await?,
            }
        }

        for (timeline, ts) in stash_timelines {
            self.persist.set_timestamp(&timeline, ts).await?;
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
        compare_and_return_async!(self, get_timestamps)
    }

    async fn get_audit_logs(&mut self) -> Result<Vec<VersionedEvent>, CatalogError> {
        compare_and_return_async!(self, get_audit_logs)
    }

    async fn get_next_id(&mut self, id_type: &str) -> Result<u64, CatalogError> {
        compare_and_return_async!(self, get_next_id, id_type)
    }

    async fn snapshot(&mut self) -> Result<Snapshot, CatalogError> {
        compare_and_return_async!(self, snapshot)
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
        let res: Result<_, CatalogError> = compare_and_return_async!(self, transaction);
        res?;
        let snapshot = self.snapshot().await?;
        // Return a transaction with a reference to the shadow catalog so the commit is applied to
        // both implementations.
        Transaction::new(self, snapshot)
    }

    async fn commit_transaction(
        &mut self,
        txn_batch: TransactionBatch,
    ) -> Result<(), CatalogError> {
        let res = compare_and_return_async!(self, commit_transaction, txn_batch.clone());
        // After committing a transaction, check that both implementations return the same snapshot
        // to ensure that the commit had the same effect on the underlying state.
        let _: Result<_, CatalogError> = compare_and_return_async!(self, snapshot);
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
        compare_and_return_async!(self, get_and_prune_storage_usage, retention_period, boot_ts)
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
