// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::max;
use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;
use mz_storage_types::controller::PersistTxnTablesImpl;
use timely::progress::Timestamp as TimelyTimestamp;

use mz_audit_log::{VersionedEvent, VersionedStorageUsage};
use mz_ore::cast::u64_to_usize;
use mz_ore::now::EpochMillis;
use mz_ore::soft_assert_eq_or_log;
use mz_repr::Timestamp;
use mz_sql::session::vars::CatalogKind;

use crate::durable::debug::{DebugCatalogState, Trace};
use crate::durable::objects::serialization::proto;
use crate::durable::objects::Snapshot;
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
        initial_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
        epoch_lower_bound: Option<Epoch>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        let stash = self.stash.open_savepoint(
            initial_ts.clone(),
            bootstrap_args,
            deploy_generation.clone(),
            epoch_lower_bound,
        );
        let persist = self.persist.open_savepoint(
            initial_ts,
            bootstrap_args,
            deploy_generation,
            epoch_lower_bound,
        );
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
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        let stash = self.stash.open_read_only(bootstrap_args);
        let persist = self.persist.open_read_only(bootstrap_args);
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
        initial_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
        epoch_lower_bound: Option<Epoch>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        let stash = self.stash.open(
            initial_ts.clone(),
            bootstrap_args,
            deploy_generation.clone(),
            epoch_lower_bound,
        );
        let persist = self.persist.open(
            initial_ts,
            bootstrap_args,
            deploy_generation,
            epoch_lower_bound,
        );
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

    async fn epoch(&mut self) -> Result<Epoch, CatalogError> {
        compare_and_return_async!(self, epoch)
    }

    async fn get_deployment_generation(&mut self) -> Result<Option<u64>, CatalogError> {
        compare_and_return_async!(self, get_deployment_generation)
    }

    async fn has_system_config_synced_once(&mut self) -> Result<bool, CatalogError> {
        compare_and_return_async!(self, has_system_config_synced_once)
    }

    async fn get_tombstone(&mut self) -> Result<Option<bool>, CatalogError> {
        compare_and_return_async!(self, get_tombstone)
    }

    async fn get_catalog_kind_config(&mut self) -> Result<Option<CatalogKind>, CatalogError> {
        compare_and_return_async!(self, get_catalog_kind_config)
    }

    async fn trace(&mut self) -> Result<Trace, CatalogError> {
        panic!("ShadowCatalog is not used for catalog-debug tool");
    }

    fn set_catalog_kind(&mut self, catalog_kind: CatalogKind) {
        compare_and_return!(self, set_catalog_kind, catalog_kind)
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
                .get_and_prune_storage_usage(None, Timestamp::minimum(), false)
                .await?;
            let mut txn = self.persist.transaction().await?;
            let start_idx = stash_storage_usage.len() - u64_to_usize(diff);
            for event in &stash_storage_usage[start_idx..] {
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
                .get_and_prune_storage_usage(None, Timestamp::minimum(), false)
                .await?;
            let mut txn = self.stash.transaction().await?;
            let start_idx = persist_storage_usage.len() - u64_to_usize(diff);
            for event in &persist_storage_usage[start_idx..] {
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

    async fn get_persist_txn_tables(
        &mut self,
    ) -> Result<Option<PersistTxnTablesImpl>, CatalogError> {
        compare_and_return_async!(self, get_persist_txn_tables)
    }

    async fn get_tombstone(&mut self) -> Result<Option<bool>, CatalogError> {
        compare_and_return_async!(self, get_tombstone)
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

    async fn whole_migration_snapshot(
        &mut self,
    ) -> Result<(Snapshot, Vec<VersionedEvent>, Vec<VersionedStorageUsage>), CatalogError> {
        panic!("Shadow catalog should never get a full snapshot")
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

    async fn whole_migration_transaction(
        &mut self,
    ) -> Result<(Transaction, Vec<VersionedEvent>, Vec<VersionedStorageUsage>), CatalogError> {
        panic!("Shadow catalog should never get a full transaction")
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
        wait_for_consolidation: bool,
    ) -> Result<Vec<VersionedStorageUsage>, CatalogError> {
        if self.is_read_only() {
            // Read-only catalogs cannot fix storage usage so we must ignore them. See
            // `Self::fix_storage_usage`.
            let stash_storage_usage = self
                .stash
                .get_and_prune_storage_usage(retention_period, boot_ts, wait_for_consolidation)
                .await?;
            let persist_storage_usage = self
                .stash
                .get_and_prune_storage_usage(retention_period, boot_ts, wait_for_consolidation)
                .await?;
            if stash_storage_usage.len() >= persist_storage_usage.len() {
                Ok(stash_storage_usage)
            } else {
                Ok(persist_storage_usage)
            }
        } else {
            compare_and_return_async!(
                self,
                get_and_prune_storage_usage,
                retention_period,
                boot_ts,
                wait_for_consolidation
            )
        }
    }

    async fn allocate_id(&mut self, id_type: &str, amount: u64) -> Result<Vec<u64>, CatalogError> {
        compare_and_return_async!(self, allocate_id, id_type, amount)
    }
}

#[cfg(test)]
mod tests {
    use mz_audit_log::{StorageUsageV1, VersionedStorageUsage};
    use mz_ore::cast::u64_to_usize;
    use mz_ore::now::SYSTEM_TIME;
    use mz_persist_client::PersistClient;
    use mz_repr::Timestamp;
    use timely::progress::Timestamp as TimelyTimestamp;
    use uuid::Uuid;

    use crate::durable::{
        shadow_catalog_state, test_bootstrap_args, test_persist_backed_catalog_state,
        test_stash_backed_catalog_state, test_stash_config, OpenableDurableCatalogState,
        STORAGE_USAGE_ID_ALLOC_KEY,
    };

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait
    async fn test_fix_storage_usage_persist() {
        let persist_client = PersistClient::new_for_tests().await;
        let organization_id = Uuid::new_v4();
        let (debug_factory, stash_config) = test_stash_config().await;

        let openable_persist_state =
            test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
        let openable_stash_state = test_stash_backed_catalog_state(&debug_factory);
        let openable_shadow_state = shadow_catalog_state(
            stash_config.clone(),
            persist_client.clone(),
            organization_id,
        )
        .await;

        test_fix_storage_usage(
            openable_persist_state,
            openable_stash_state,
            openable_shadow_state,
        )
        .await;

        debug_factory.drop().await;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait
    async fn test_fix_storage_usage_stash() {
        let persist_client = PersistClient::new_for_tests().await;
        let organization_id = Uuid::new_v4();
        let (debug_factory, stash_config) = test_stash_config().await;

        let openable_persist_state =
            test_persist_backed_catalog_state(persist_client.clone(), organization_id).await;
        let openable_stash_state = test_stash_backed_catalog_state(&debug_factory);
        let openable_shadow_state = shadow_catalog_state(
            stash_config.clone(),
            persist_client.clone(),
            organization_id,
        )
        .await;

        test_fix_storage_usage(
            openable_stash_state,
            openable_persist_state,
            openable_shadow_state,
        )
        .await;

        debug_factory.drop().await;
    }

    async fn test_fix_storage_usage(
        ahead: impl OpenableDurableCatalogState,
        behind: impl OpenableDurableCatalogState,
        shadow: impl OpenableDurableCatalogState,
    ) {
        let ahead_id = 200;
        let behind_id = 100;
        assert!(ahead_id > behind_id);

        let storage_usages: Vec<_> = (0..ahead_id)
            .map(|i| {
                VersionedStorageUsage::V1(StorageUsageV1 {
                    id: i,
                    shard_id: Some(format!("{i}")),
                    size_bytes: i,
                    collection_timestamp: Timestamp::minimum().into(),
                })
            })
            .collect();

        {
            let mut ahead_state = Box::new(ahead)
                .open(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
                .await
                .expect("failed to open");
            let _ = ahead_state
                .allocate_id(STORAGE_USAGE_ID_ALLOC_KEY, ahead_id)
                .await
                .expect("failed to allocate");
            let mut tx = ahead_state
                .transaction()
                .await
                .expect("failed to open transaction");
            tx.insert_storage_usage_events(storage_usages.clone());
            tx.commit().await.expect("failed to commit transaction");
        }

        {
            let mut behind_state = Box::new(behind)
                .open(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
                .await
                .expect("failed to open");
            let _ = behind_state
                .allocate_id(STORAGE_USAGE_ID_ALLOC_KEY, behind_id)
                .await
                .expect("failed to allocate");
            let mut tx = behind_state
                .transaction()
                .await
                .expect("failed to open transaction");
            tx.insert_storage_usage_events(storage_usages[0..u64_to_usize(behind_id)].to_vec());
            tx.commit().await.expect("failed to commit transaction");
        }

        {
            let mut shadow_state = Box::new(shadow)
                .open(SYSTEM_TIME(), &test_bootstrap_args(), None, None)
                .await
                .expect("failed to open");
            let shadow_storage_usage = shadow_state
                .get_and_prune_storage_usage(None, Timestamp::minimum(), false)
                .await
                .expect("failed to get storage usage");
            assert_eq!(shadow_storage_usage, storage_usages)
        }
    }
}
