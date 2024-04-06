// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(test)]
mod tests;

use std::cmp::max;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use differential_dataflow::lattice::Lattice;
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use mz_audit_log::{VersionedEvent, VersionedStorageUsage};
use mz_ore::metrics::MetricsFutureExt;
use mz_ore::now::EpochMillis;
use mz_ore::retry::{Retry, RetryResult};
use mz_ore::{
    soft_assert_eq_no_log, soft_assert_eq_or_log, soft_assert_ne_or_log, soft_assert_no_log,
    soft_assert_or_log, soft_panic_or_log,
};
use mz_persist_client::critical::SinceHandle;
use mz_persist_client::read::{Listen, ListenEvent, ReadHandle};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::Opaque;
use mz_proto::{RustType, TryFromProtoError};
use mz_repr::{Diff, RelationDesc, ScalarType};
use mz_storage_types::controller::PersistTxnTablesImpl;
use mz_storage_types::sources::SourceData;
use sha2::Digest;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::durable::debug::{Collection, DebugCatalogState, Trace};
use crate::durable::initialize::{
    DEPLOY_GENERATION, PERSIST_TXN_TABLES, SYSTEM_CONFIG_SYNCED_KEY, USER_VERSION_KEY,
};
use crate::durable::metrics::Metrics;
use crate::durable::objects::serialization::proto;
use crate::durable::objects::state_update::{
    IntoStateUpdateKindRaw, StateUpdate, StateUpdateKind, StateUpdateKindRaw,
    TryIntoStateUpdateKind,
};
use crate::durable::objects::{AuditLogKey, Snapshot, StorageUsageKey};
use crate::durable::transaction::TransactionBatch;
use crate::durable::upgrade::upgrade;
use crate::durable::{
    initialize, BootstrapArgs, CatalogError, DurableCatalogError, DurableCatalogState, Epoch,
    OpenableDurableCatalogState, ReadOnlyDurableCatalogState, Transaction,
};

/// New-type used to represent timestamps in persist.
pub(crate) type Timestamp = mz_repr::Timestamp;

/// The minimum value of an epoch.
///
/// # Safety
/// `new_unchecked` is safe to call with a non-zero value.
const MIN_EPOCH: Epoch = unsafe { Epoch::new_unchecked(1) };

/// Human readable catalog shard name.
const CATALOG_SHARD_NAME: &str = "catalog";
/// Human readable catalog upgrade shard name.
const UPGRADE_SHARD_NAME: &str = "catalog_upgrade";

/// Seed used to generate the persist shard ID for the catalog.
pub const CATALOG_SEED: usize = 1;
/// Seed used to generate the catalog upgrade shard ID.
///
/// All state that gets written to persist is tagged with the version of the code that wrote that
/// state. Persist has limited forward compatibility in how many versions in the future a reader can
/// read. Reading from persist updates state and the version that the state is tagged with. As a
/// consequence, reading from persist may unintentionally fence out other readers and writers with
/// a lower version. We use the catalog upgrade shard to track what database version is actively
/// deployed so readers from the future, such as the upgrade checker tool, don't accidentally fence out the
/// database from persist. Only writable opened catalogs can increment the version in the upgrade
/// shard.
///
/// One specific example that we are trying to avoid with the catalog upgrade shard is the
/// following:
///
///   1. Database is running on version 0.X.0.
///   2. Upgrade checker is run on version 0.X+1.0.
///   3. Upgrade checker is run on version 0.X+2.0.
///
/// With the catalog upgrade shard, the upgrade checker in step (3) can see that the database is
/// currently running on v0.X.0 and reading the catalog would cause the database to get fenced out.
/// So instead of reading the catalog it errors out. Without the catalog upgrade shard, the upgrade
/// checker could read the version in the catalog shard, and see that it is v0.X+1.0, but it would
/// be impossible to differentiate between the following two scenarios:
///
///   - The database is running on v0.X+1.0 and it's safe to run the upgrade checker at v0.X+2.0.
///   - Some other upgrade checker incremented the version to v0.X+1.0, the database is running on
///   version v0.X.0, and it is not safe to run the upgrade checker.
///
/// Persist guarantees that the shard versions are non-decreasing, so we don't need to worry about
/// race conditions where the shard version decreases after reading it.
const UPGRADE_SEED: usize = 2;

/// Durable catalog mode that dictates the effect of mutable operations.
#[derive(Debug, Clone)]
pub(crate) enum Mode {
    /// Mutable operations are prohibited.
    Readonly,
    /// Mutable operations have an effect in-memory, but aren't persisted durably.
    Savepoint,
    /// Mutable operations have an effect in-memory and durably.
    Writable,
}

/// Enum representing a potentially fenced epoch.
#[derive(Debug)]
pub(crate) enum FenceableEpoch {
    /// The current epoch, if one exists, has not been fenced.
    Unfenced(Option<Epoch>),
    /// The current epoch has been fenced.
    Fenced {
        current_epoch: Epoch,
        fence_epoch: Epoch,
    },
}

impl FenceableEpoch {
    /// Returns the current epoch if it is not fenced, otherwise returns an error.
    fn validate(&self) -> Result<Option<Epoch>, DurableCatalogError> {
        match self {
            FenceableEpoch::Unfenced(epoch) => Ok(epoch.clone()),
            FenceableEpoch::Fenced {
                current_epoch,
                fence_epoch,
            } => Err(DurableCatalogError::Fence(format!(
                "current catalog epoch {current_epoch} fenced by new catalog epoch {fence_epoch}",
            ))),
        }
    }

    /// Returns the current epoch.
    fn epoch(&self) -> Option<Epoch> {
        match self {
            FenceableEpoch::Unfenced(epoch) => epoch.clone(),
            FenceableEpoch::Fenced {
                current_epoch,
                fence_epoch: _,
            } => Some(current_epoch.clone()),
        }
    }

    /// Returns `Err` if `epoch` fences out `self`, `Ok` otherwise.
    fn maybe_fence(&mut self, epoch: Epoch) -> Result<(), DurableCatalogError> {
        match self {
            FenceableEpoch::Unfenced(Some(current_epoch)) => {
                if epoch > *current_epoch {
                    *self = FenceableEpoch::Fenced {
                        current_epoch: *current_epoch,
                        fence_epoch: epoch,
                    };
                    self.validate()?;
                } else if epoch < *current_epoch {
                    panic!("Epoch went backwards from {current_epoch:?} to {epoch:?}");
                }
            }
            FenceableEpoch::Unfenced(None) => {
                *self = FenceableEpoch::Unfenced(Some(epoch));
            }
            FenceableEpoch::Fenced { .. } => {
                self.validate()?;
            }
        }

        Ok(())
    }
}

pub(crate) trait ApplyUpdate<T: IntoStateUpdateKindRaw> {
    /// Process and apply `update`.
    ///
    /// Returns `Some` if `update` should be cached in memory and `None` otherwise.
    fn apply_update(
        &mut self,
        update: StateUpdate<T>,
        current_epoch: &mut FenceableEpoch,
        metrics: &Arc<Metrics>,
    ) -> Result<Option<StateUpdate<T>>, DurableCatalogError>;
}

/// A handle for interacting with the persist catalog shard.
#[derive(Debug)]
pub(crate) struct PersistHandle<T: TryIntoStateUpdateKind, U: ApplyUpdate<T>> {
    /// Since handle to control compaction.
    since_handle: SinceHandle<SourceData, (), Timestamp, Diff, i64>,
    /// Write handle to persist.
    write_handle: WriteHandle<SourceData, (), Timestamp, Diff>,
    /// Listener to catalog changes.
    listen: Listen<SourceData, (), Timestamp, Diff>,
    /// Handle for connecting to persist.
    persist_client: PersistClient,
    /// Catalog shard ID.
    shard_id: ShardId,
    /// Cache of the most recent catalog snapshot.
    ///
    /// We use a tuple instead of [`StateUpdate`] to make consolidation easier.
    pub(crate) snapshot: Vec<(T, Timestamp, Diff)>,
    /// Applies custom processing, filtering, and fencing for each individual update.
    update_applier: U,
    /// The current upper of the persist shard.
    pub(crate) upper: Timestamp,
    /// The epoch of the catalog, if one exists.
    epoch: FenceableEpoch,
    /// Metrics for the persist catalog.
    metrics: Arc<Metrics>,
}

impl<T: TryIntoStateUpdateKind, U: ApplyUpdate<T>> PersistHandle<T, U> {
    /// Increment the version in the catalog upgrade shard to the code's current version.
    async fn increment_catalog_upgrade_shard_version(&mut self, organization_id: Uuid) {
        let upgrade_shard_id = shard_id(organization_id, UPGRADE_SEED);
        let mut write_handle: WriteHandle<(), (), Timestamp, Diff> = self
            .persist_client
            .open_writer(
                upgrade_shard_id,
                Arc::new(UnitSchema::default()),
                Arc::new(UnitSchema::default()),
                Diagnostics {
                    shard_name: UPGRADE_SHARD_NAME.to_string(),
                    handle_purpose: "increment durable catalog upgrade shard version".to_string(),
                },
            )
            .await
            .expect("invalid usage");
        const EMPTY_UPDATES: &[(((), ()), Timestamp, Diff)] = &[];
        let mut upper = write_handle.fetch_recent_upper().await.clone();
        loop {
            let next_upper = upper
                .iter()
                .map(|timestamp| timestamp.step_forward())
                .collect();
            match write_handle
                .compare_and_append(EMPTY_UPDATES, upper, next_upper)
                .await
                .expect("invalid usage")
            {
                Ok(()) => break,
                Err(upper_mismatch) => {
                    upper = upper_mismatch.current;
                }
            }
        }
    }

    #[mz_ore::instrument]
    async fn current_upper(&mut self) -> Timestamp {
        current_upper(&mut self.write_handle).await
    }

    /// Appends `updates` to the catalog state and downgrades the catalog's upper to `next_upper`
    /// iff the current global upper of the catalog is `current_upper`.
    #[mz_ore::instrument]
    pub(crate) async fn compare_and_append<S: IntoStateUpdateKindRaw>(
        &mut self,
        updates: Vec<(S, Diff)>,
    ) -> Result<(), CatalogError> {
        let updates = updates.into_iter().map(|(kind, diff)| {
            let kind: StateUpdateKindRaw = kind.into();
            ((Into::<SourceData>::into(kind), ()), self.upper, diff)
        });
        let next_upper = self.upper.step_forward();
        self.write_handle
            .compare_and_append(
                updates,
                Antichain::from_elem(self.upper),
                Antichain::from_elem(next_upper),
            )
            .await
            .expect("invalid usage")
            .map_err(|upper_mismatch| {
                DurableCatalogError::Fence(format!(
                    "current catalog upper {:?} fenced by new catalog upper {:?}",
                    upper_mismatch.expected, upper_mismatch.current
                ))
            })?;

        // Lag the shard's upper by 1 to keep it readable.
        let downgrade_to = Antichain::from_elem(next_upper.saturating_sub(1));

        // The since handle gives us the ability to fence out other writes using an opaque token.
        // (See the method documentation for details.)
        // That's not needed here, so we use a constant opaque token to avoid any comparison failures.
        let opaque = i64::initial();
        let downgrade = self
            .since_handle
            .maybe_compare_and_downgrade_since(&opaque, (&opaque, &downgrade_to))
            .await;

        match downgrade {
            None => {}
            Some(Err(e)) => soft_panic_or_log!("found opaque value {e}, but expected {opaque}"),
            Some(Ok(updated)) => soft_assert_or_log!(
                updated == downgrade_to,
                "updated bound should match expected"
            ),
        }
        self.sync(next_upper).await?;
        Ok(())
    }

    /// Generates an iterator of [`StateUpdate`] that contain all unconsolidated updates to the
    /// catalog state up to, and including, `as_of`.
    #[mz_ore::instrument]
    async fn snapshot_unconsolidated(&mut self) -> Vec<StateUpdate<StateUpdateKind>> {
        let current_upper = self.current_upper().await;

        let mut snapshot = Vec::new();
        let mut read_handle = self.read_handle().await;
        let as_of = as_of(&read_handle, current_upper);
        let mut stream = Box::pin(
            // We use `snapshot_and_stream` because it guarantees unconsolidated output.
            read_handle
                .snapshot_and_stream(Antichain::from_elem(as_of))
                .await
                .expect("we have advanced the restart_as_of by the since"),
        );
        while let Some(update) = stream.next().await {
            snapshot.push(update)
        }
        read_handle.expire().await;
        snapshot
            .into_iter()
            .map(Into::<StateUpdate<StateUpdateKindRaw>>::into)
            .map(|state_update| state_update.try_into().expect("kind decoding error"))
            .collect()
    }

    /// Listen and apply all updates that are currently in persist.
    ///
    /// Returns an error if this instance has been fenced out.
    #[mz_ore::instrument]
    pub(crate) async fn sync_to_current_upper(&mut self) -> Result<(), DurableCatalogError> {
        let upper = self.current_upper().await;
        self.sync(upper).await
    }

    /// Listen and apply all updates up to `target_upper`.
    ///
    /// Returns an error if this instance has been fenced out.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn sync(
        &mut self,
        target_upper: Timestamp,
    ) -> Result<(), DurableCatalogError> {
        self.metrics.syncs.inc();
        let counter = self.metrics.sync_latency_seconds.clone();
        self.sync_inner(target_upper)
            .wall_time()
            .inc_by(counter)
            .await
    }

    #[mz_ore::instrument(level = "debug")]
    async fn sync_inner(&mut self, target_upper: Timestamp) -> Result<(), DurableCatalogError> {
        self.epoch.validate()?;
        let mut updates = Vec::new();

        while self.upper < target_upper {
            let listen_events = self.listen.fetch_next().await;
            for listen_event in listen_events {
                match listen_event {
                    ListenEvent::Progress(upper) => {
                        debug!("synced up to {upper:?}");
                        self.upper = upper
                            .as_option()
                            .cloned()
                            .expect("we use a totally ordered time and never finalize the shard");
                    }
                    ListenEvent::Updates(batch_updates) => {
                        debug!("syncing updates {batch_updates:?}");
                        let batch_updates = batch_updates
                            .into_iter()
                            .map(Into::<StateUpdate<StateUpdateKindRaw>>::into)
                            .map(|update| {
                                let kind = T::try_from(update.kind).expect("kind decoding error");
                                (kind, update.ts, update.diff)
                            });
                        updates.extend(batch_updates);
                    }
                }
            }
        }
        let updates = updates
            .into_iter()
            .map(|(kind, ts, diff)| StateUpdate { kind, ts, diff });
        self.apply_updates(updates)?;
        Ok(())
    }

    #[mz_ore::instrument(level = "debug")]
    pub(crate) fn apply_updates(
        &mut self,
        updates: impl IntoIterator<Item = StateUpdate<T>>,
    ) -> Result<(), DurableCatalogError> {
        let mut updates: Vec<_> = updates
            .into_iter()
            .map(|StateUpdate { kind, ts, diff }| (kind, ts, diff))
            .collect();

        // This helps guarantee that for a single key, there is at most a single retraction and a
        // single insertion per timestamp. Otherwise, we would need to match the retractions and
        // insertions up by value and manually figure out what the end value should be.
        differential_dataflow::consolidation::consolidate_updates(&mut updates);

        // Updates must be applied in timestamp order. Within a timestamp retractions must be
        // applied before insertions, or we might end up retracting the wrong value.
        updates.sort_by(|(_, ts1, diff1), (_, ts2, diff2)| ts1.cmp(ts2).then(diff1.cmp(diff2)));

        for (kind, ts, diff) in updates {
            if diff != 1 && diff != -1 {
                panic!("invalid update in consolidated trace: ({kind:?}, {ts:?}, {diff:?})");
            }

            if let Some(StateUpdate { kind, ts, diff }) = self.update_applier.apply_update(
                StateUpdate { kind, ts, diff },
                &mut self.epoch,
                &self.metrics,
            )? {
                self.snapshot.push((kind, ts, diff));
            }
        }

        self.consolidate();

        Ok(())
    }

    #[mz_ore::instrument]
    pub(crate) fn consolidate(&mut self) {
        soft_assert_no_log!(
            self.snapshot
                .windows(2)
                .all(|updates| updates[0].1 <= updates[1].1),
            "snapshot should be sorted by timestamp, {:?}",
            self.snapshot
        );

        let new_ts = self
            .snapshot
            .last()
            .map(|(_, ts, _)| *ts)
            .unwrap_or_else(Timestamp::minimum);
        for (_, ts, _) in &mut self.snapshot {
            *ts = new_ts;
        }
        differential_dataflow::consolidation::consolidate_updates(&mut self.snapshot);
    }

    /// Execute and return the results of `f` on the current catalog trace.
    ///
    /// Will return an error if the catalog has been fenced out.
    async fn with_trace<R>(
        &mut self,
        f: impl FnOnce(&Vec<(T, Timestamp, Diff)>) -> Result<R, CatalogError>,
    ) -> Result<R, CatalogError> {
        self.sync_to_current_upper().await?;
        f(&self.snapshot)
    }

    /// Open a read handle to the catalog.
    async fn read_handle(&mut self) -> ReadHandle<SourceData, (), Timestamp, Diff> {
        self.persist_client
            .open_leased_reader(
                self.shard_id,
                Arc::new(desc()),
                Arc::new(UnitSchema::default()),
                Diagnostics {
                    shard_name: CATALOG_SHARD_NAME.to_string(),
                    handle_purpose: "openable durable catalog state temporary reader".to_string(),
                },
            )
            .await
            .expect("invalid usage")
    }

    /// Politely releases all external resources that can only be released in an async context.
    async fn expire(self: Box<Self>) {
        self.write_handle.expire().await;
        self.listen.expire().await;
    }
}

impl<U: ApplyUpdate<StateUpdateKind>> PersistHandle<StateUpdateKind, U> {
    /// Execute and return the results of `f` on the current catalog snapshot.
    ///
    /// Will return an error if the catalog has been fenced out.
    async fn with_snapshot<T>(
        &mut self,
        f: impl FnOnce(Snapshot) -> Result<T, CatalogError>,
    ) -> Result<T, CatalogError> {
        fn apply<K, V>(map: &mut BTreeMap<K, V>, key: &K, value: &V, diff: Diff)
        where
            K: Ord + Clone,
            V: Ord + Clone + Debug,
        {
            let key = key.clone();
            let value = value.clone();
            if diff == 1 {
                let prev = map.insert(key, value);
                assert_eq!(
                    prev, None,
                    "values must be explicitly retracted before inserting a new value"
                );
            } else if diff == -1 {
                let prev = map.remove(&key);
                assert_eq!(
                    prev,
                    Some(value),
                    "retraction does not match existing value"
                );
            }
        }

        self.with_trace(|trace| {
            let mut snapshot = Snapshot::empty();
            for (kind, ts, diff) in trace {
                let diff = *diff;
                if diff != 1 && diff != -1 {
                    panic!("invalid update in consolidated trace: ({kind:?}, {ts:?}, {diff:?})");
                }

                match kind {
                    StateUpdateKind::AuditLog(_key, ()) => {
                        // Ignore for snapshots.
                    }
                    StateUpdateKind::Cluster(key, value) => {
                        apply(&mut snapshot.clusters, key, value, diff);
                    }
                    StateUpdateKind::ClusterReplica(key, value) => {
                        apply(&mut snapshot.cluster_replicas, key, value, diff);
                    }
                    StateUpdateKind::Comment(key, value) => {
                        apply(&mut snapshot.comments, key, value, diff);
                    }
                    StateUpdateKind::Config(key, value) => {
                        apply(&mut snapshot.configs, key, value, diff);
                    }
                    StateUpdateKind::Database(key, value) => {
                        apply(&mut snapshot.databases, key, value, diff);
                    }
                    StateUpdateKind::DefaultPrivilege(key, value) => {
                        apply(&mut snapshot.default_privileges, key, value, diff);
                    }
                    StateUpdateKind::Epoch(_epoch) => {
                        // Ignore for snapshots.
                    }
                    StateUpdateKind::IdAllocator(key, value) => {
                        apply(&mut snapshot.id_allocator, key, value, diff);
                    }
                    StateUpdateKind::IntrospectionSourceIndex(key, value) => {
                        apply(&mut snapshot.introspection_sources, key, value, diff);
                    }
                    StateUpdateKind::Item(key, value) => {
                        apply(&mut snapshot.items, key, value, diff);
                    }
                    StateUpdateKind::Role(key, value) => {
                        apply(&mut snapshot.roles, key, value, diff);
                    }
                    StateUpdateKind::Schema(key, value) => {
                        apply(&mut snapshot.schemas, key, value, diff);
                    }
                    StateUpdateKind::Setting(key, value) => {
                        apply(&mut snapshot.settings, key, value, diff);
                    }
                    StateUpdateKind::StorageUsage(_key, ()) => {
                        // Ignore for snapshots.
                    }
                    StateUpdateKind::SystemConfiguration(key, value) => {
                        apply(&mut snapshot.system_configurations, key, value, diff);
                    }
                    StateUpdateKind::SystemObjectMapping(key, value) => {
                        apply(&mut snapshot.system_object_mappings, key, value, diff);
                    }
                    StateUpdateKind::SystemPrivilege(key, value) => {
                        apply(&mut snapshot.system_privileges, key, value, diff);
                    }
                    StateUpdateKind::StorageCollectionMetadata(key, value) => {
                        apply(&mut snapshot.storage_collection_metadata, key, value, diff);
                    }
                    StateUpdateKind::UnfinalizedShard(key, ()) => {
                        apply(&mut snapshot.unfinalized_shards, key, &(), diff);
                    }
                    StateUpdateKind::PersistTxnShard((), value) => {
                        apply(&mut snapshot.persist_txn_shard, &(), value, diff);
                    }
                }
            }
            f(snapshot)
        })
        .await
    }

    /// Generates an iterator of [`StateUpdate`] that contain all updates to the catalog
    /// state.
    ///
    /// The output is fetched directly from persist instead of the in-memory cache.
    ///
    /// The output is consolidated and sorted by timestamp in ascending order.
    #[mz_ore::instrument(level = "debug")]
    async fn persist_snapshot(
        &mut self,
    ) -> impl Iterator<Item = StateUpdate> + DoubleEndedIterator {
        let mut read_handle = self.read_handle().await;
        let as_of = as_of(&read_handle, self.upper);
        let snapshot = snapshot_binary(&mut read_handle, as_of, &self.metrics)
            .await
            .map(|update| update.try_into().expect("kind decoding error"));
        read_handle.expire().await;
        snapshot
    }
}

#[derive(Debug)]
pub(crate) struct UnopenedCatalogStateInner {
    /// The organization ID of the environment.
    organization_id: Uuid,
    /// A cache of the config collection of the catalog.
    configs: BTreeMap<String, u64>,
}

impl UnopenedCatalogStateInner {
    fn new(organization_id: Uuid) -> UnopenedCatalogStateInner {
        UnopenedCatalogStateInner {
            organization_id,
            configs: BTreeMap::new(),
        }
    }
}

impl ApplyUpdate<StateUpdateKindRaw> for UnopenedCatalogStateInner {
    fn apply_update(
        &mut self,
        update: StateUpdate<StateUpdateKindRaw>,
        current_epoch: &mut FenceableEpoch,
        _metrics: &Arc<Metrics>,
    ) -> Result<Option<StateUpdate<StateUpdateKindRaw>>, DurableCatalogError> {
        if let Ok(kind) =
            <StateUpdateKindRaw as TryIntoStateUpdateKind>::try_into(update.kind.clone())
        {
            match (kind, update.diff) {
                (StateUpdateKind::Config(key, value), 1) => {
                    let prev = self.configs.insert(key.key, value.value);
                    assert_eq!(
                        prev, None,
                        "values must be explicitly retracted before inserting a new value"
                    );
                }
                (StateUpdateKind::Config(key, value), -1) => {
                    let prev = self.configs.remove(&key.key);
                    assert_eq!(
                        prev,
                        Some(value.value),
                        "retraction does not match existing value"
                    );
                }
                (StateUpdateKind::Epoch(epoch), 1) => {
                    current_epoch.maybe_fence(epoch)?;
                    return Ok(Some(update));
                }
                _ => {}
            }
        }

        Ok(Some(update))
    }
}

/// A Handle to an unopened catalog stored in persist. The unopened catalog can serve `Config` data
/// or the current epoch. All other catalog data may be un-migrated and should not be read until the
/// catalog has been opened. The [`UnopenedPersistCatalogState`] is responsible for opening the
/// catalog, see [`OpenableDurableCatalogState::open`] for more details.
///
/// Production users should call [`Self::expire`] before dropping a [`UnopenedPersistCatalogState`]
/// so that it can expire its leases. If/when rust gets AsyncDrop, this will be done automatically.
pub(crate) type UnopenedPersistCatalogState =
    PersistHandle<StateUpdateKindRaw, UnopenedCatalogStateInner>;

impl UnopenedPersistCatalogState {
    /// Create a new [`UnopenedPersistCatalogState`] to the catalog state associated with
    /// `organization_id`.
    ///
    /// All usages of the persist catalog must go through this function. That includes the
    /// catalog-debug tool, the adapter's catalog, etc.
    #[mz_ore::instrument]
    pub(crate) async fn new(
        persist_client: PersistClient,
        organization_id: Uuid,
        version: semver::Version,
        metrics: Arc<Metrics>,
    ) -> Result<UnopenedPersistCatalogState, DurableCatalogError> {
        let catalog_shard_id = shard_id(organization_id, CATALOG_SEED);
        let upgrade_shard_id = shard_id(organization_id, UPGRADE_SEED);
        debug!(
            ?catalog_shard_id,
            ?upgrade_shard_id,
            "new persist backed catalog state"
        );

        // Check the catalog upgrade shard to see ensure that we don't fence anyone out of persist.
        let upgrade_version =
            fetch_catalog_upgrade_shard_version(&persist_client, upgrade_shard_id).await;
        // If this is `None`, no version was found in the upgrade shard. Either this is a brand new
        // environment and we don't need to worry about fencing existing users or we're upgrading
        // from a version that doesn't contain the catalog upgrade shard and we need to proceed
        // with caution for that one week until the catalog upgrade shard exists in all
        // environments.
        if let Some(upgrade_version) = upgrade_version {
            if mz_persist_client::cfg::check_data_version(&upgrade_version, &version).is_err() {
                return Err(DurableCatalogError::IncompatiblePersistVersion {
                    found_version: upgrade_version,
                    catalog_version: version,
                });
            }
        }

        let since_handle = persist_client
            .open_critical_since(
                catalog_shard_id,
                PersistClient::CATALOG_CRITICAL_SINCE,
                Diagnostics {
                    shard_name: CATALOG_SHARD_NAME.to_string(),
                    handle_purpose: "durable catalog state critical since".to_string(),
                },
            )
            .await
            .expect("invalid usage");
        let (mut write_handle, mut read_handle) = persist_client
            .open(
                catalog_shard_id,
                Arc::new(desc()),
                Arc::new(UnitSchema::default()),
                Diagnostics {
                    shard_name: CATALOG_SHARD_NAME.to_string(),
                    handle_purpose: "durable catalog state handles".to_string(),
                },
            )
            .await
            .expect("invalid usage");

        // Commit an empty write at the minimum timestamp so the catalog is always readable.
        const EMPTY_UPDATES: &[((SourceData, ()), Timestamp, Diff)] = &[];
        let _ = write_handle
            .compare_and_append(
                EMPTY_UPDATES,
                Antichain::from_elem(Timestamp::minimum()),
                Antichain::from_elem(Timestamp::minimum().step_forward()),
            )
            .await
            .expect("invalid usage");

        let upper = current_upper(&mut write_handle).await;
        let as_of = as_of(&mut read_handle, upper);
        let snapshot: Vec<_> = snapshot_binary(&mut read_handle, as_of, &metrics)
            .await
            .map(|StateUpdate { kind, ts, diff }| (kind, ts, diff))
            .collect();
        let listen = read_handle
            .listen(Antichain::from_elem(as_of))
            .await
            .expect("invalid usage");
        // Sniff out most recent epoch.
        let epoch = snapshot
            .iter()
            .rev()
            .filter(|(_, _, diff)| *diff == 1)
            .filter_map(|(kind, _, _)| {
                <StateUpdateKindRaw as TryIntoStateUpdateKind>::try_into(kind.clone()).ok()
            })
            .filter_map(|kind| match kind {
                StateUpdateKind::Epoch(epoch) => Some(epoch),
                _ => None,
            })
            .next();
        let epoch = FenceableEpoch::Unfenced(epoch);

        let mut handle = UnopenedPersistCatalogState {
            since_handle,
            write_handle,
            listen,
            persist_client,
            shard_id: catalog_shard_id,
            // Initialize empty in-memory state.
            snapshot: Vec::new(),
            update_applier: UnopenedCatalogStateInner::new(organization_id),
            upper,
            epoch,
            metrics,
        };
        let updates = snapshot
            .into_iter()
            .map(|(kind, ts, diff)| StateUpdate { kind, ts, diff });
        handle.apply_updates(updates)?;

        Ok(handle)
    }

    #[mz_ore::instrument]
    async fn open_inner(
        mut self,
        mode: Mode,
        initial_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
        epoch_lower_bound: Option<Epoch>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        let read_only = matches!(mode, Mode::Readonly);

        self.sync_to_current_upper().await?;
        let prev_epoch = self.epoch.validate()?;
        // Fence out previous catalogs.
        let mut fence_updates = Vec::with_capacity(2);
        if let Some(prev_epoch) = prev_epoch {
            fence_updates.push((StateUpdateKind::Epoch(prev_epoch), -1));
        }
        let mut current_epoch = prev_epoch.unwrap_or(MIN_EPOCH).get();
        // Only writable catalogs attempt to increment the epoch.
        if matches!(mode, Mode::Writable) {
            if let Some(epoch_lower_bound) = &epoch_lower_bound {
                info!(?epoch_lower_bound);
            }

            current_epoch = max(
                current_epoch + 1,
                epoch_lower_bound.unwrap_or(Epoch::MIN).get(),
            );
        }
        let current_epoch = Epoch::new(current_epoch).expect("known to be non-zero");
        fence_updates.push((StateUpdateKind::Epoch(current_epoch), 1));
        let current_epoch = FenceableEpoch::Unfenced(Some(current_epoch));
        debug!(
            ?self.upper,
            ?prev_epoch,
            ?epoch_lower_bound,
            ?current_epoch,
            "fencing previous catalogs"
        );
        self.epoch = current_epoch;
        if matches!(mode, Mode::Writable) {
            self.compare_and_append(fence_updates).await?;
        }

        let is_initialized = self.is_initialized_inner();
        if !matches!(mode, Mode::Writable) && !is_initialized {
            return Err(CatalogError::Durable(DurableCatalogError::NotWritable(
                format!("catalog tables do not exist; will not create in {mode:?} mode"),
            )));
        }
        soft_assert_ne_or_log!(self.upper, Timestamp::minimum());

        // Perform data migrations.
        if is_initialized && !read_only {
            upgrade(&mut self, mode.clone()).await?;
        }

        debug!(
            ?is_initialized,
            ?self.upper,
            "initializing catalog state"
        );
        let mut catalog = PersistCatalogState {
            since_handle: self.since_handle,
            write_handle: self.write_handle,
            listen: self.listen,
            persist_client: self.persist_client,
            shard_id: self.shard_id,
            upper: self.upper,
            epoch: self.epoch,
            // Initialize empty in-memory state.
            snapshot: Vec::new(),
            update_applier: CatalogStateInner::new(mode.clone()),
            metrics: self.metrics,
        };
        catalog.metrics.collection_entries.reset();
        let updates = self.snapshot.into_iter().map(|(kind, ts, diff)| {
            let kind = TryIntoStateUpdateKind::try_into(kind).expect("kind decoding error");
            StateUpdate { kind, ts, diff }
        });
        catalog.apply_updates(updates)?;

        let txn = if is_initialized {
            let mut txn = catalog.transaction().await?;
            if let Some(deploy_generation) = deploy_generation {
                txn.set_config(DEPLOY_GENERATION.into(), Some(deploy_generation))?;
            }
            txn
        } else {
            soft_assert_eq_no_log!(
                catalog
                    .snapshot
                    .iter()
                    .filter(|(kind, _, _)| !matches!(kind, StateUpdateKind::Epoch(_)))
                    .count(),
                0,
                "trace should not contain any updates for an uninitialized catalog: {:?}",
                catalog.snapshot
            );
            let mut txn = catalog.transaction().await?;
            initialize::initialize(&mut txn, bootstrap_args, initial_ts, deploy_generation).await?;
            txn
        };

        if read_only {
            let (txn_batch, _) = txn.into_parts();
            // The upper here doesn't matter because we are only applying the updates in memory.
            let updates = StateUpdate::from_txn_batch_ts(txn_batch, catalog.upper);
            catalog.apply_updates(updates)?;
        } else {
            txn.commit().await?;
        }

        // Now that we've fully opened the catalog at the current version, we can increment the
        // version in the catalog upgrade shard to signal to readers that the allowable versions
        // have increased.
        if matches!(mode, Mode::Writable) {
            catalog
                .increment_catalog_upgrade_shard_version(self.update_applier.organization_id)
                .await;
        }

        Ok(Box::new(catalog))
    }

    /// Reports if the catalog state has been initialized.
    ///
    /// NOTE: This is the answer as of the last call to [`PersistHandle::sync`] or [`PersistHandle::sync_to_current_upper`],
    /// not necessarily what is currently in persist.
    #[mz_ore::instrument]
    fn is_initialized_inner(&self) -> bool {
        !self.update_applier.configs.is_empty()
    }

    /// Get the current value of config `key`.
    ///
    /// Some configs need to be read before the catalog is opened for bootstrapping.
    #[mz_ore::instrument]
    async fn get_current_config(&mut self, key: &str) -> Result<Option<u64>, CatalogError> {
        self.sync_to_current_upper().await?;
        Ok(self.update_applier.configs.get(key).cloned())
    }

    /// Get the user version of this instance.
    ///
    /// The user version is used to determine if a migration is needed.
    #[mz_ore::instrument]
    pub(crate) async fn get_user_version(&mut self) -> Result<Option<u64>, CatalogError> {
        self.get_current_config(USER_VERSION_KEY).await
    }
}

#[async_trait]
impl OpenableDurableCatalogState for UnopenedPersistCatalogState {
    #[mz_ore::instrument]
    async fn open_savepoint(
        mut self: Box<Self>,
        initial_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
        epoch_lower_bound: Option<Epoch>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        self.open_inner(
            Mode::Savepoint,
            initial_ts,
            bootstrap_args,
            deploy_generation,
            epoch_lower_bound,
        )
        .boxed()
        .await
    }

    #[mz_ore::instrument]
    async fn open_read_only(
        mut self: Box<Self>,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        self.open_inner(Mode::Readonly, EpochMillis::MIN, bootstrap_args, None, None)
            .boxed()
            .await
    }

    #[mz_ore::instrument]
    async fn open(
        mut self: Box<Self>,
        initial_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
        epoch_lower_bound: Option<Epoch>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        self.open_inner(
            Mode::Writable,
            initial_ts,
            bootstrap_args,
            deploy_generation,
            epoch_lower_bound,
        )
        .boxed()
        .await
    }

    #[mz_ore::instrument(level = "debug")]
    async fn open_debug(mut self: Box<Self>) -> Result<DebugCatalogState, CatalogError> {
        Ok(DebugCatalogState(*self))
    }

    #[mz_ore::instrument]
    async fn is_initialized(&mut self) -> Result<bool, CatalogError> {
        self.sync_to_current_upper().await?;
        Ok(self.is_initialized_inner())
    }

    #[mz_ore::instrument]
    async fn epoch(&mut self) -> Result<Epoch, CatalogError> {
        self.sync_to_current_upper().await?;
        self.epoch
            .validate()?
            .ok_or(CatalogError::Durable(DurableCatalogError::Uninitialized))
    }

    #[mz_ore::instrument]
    async fn get_deployment_generation(&mut self) -> Result<Option<u64>, CatalogError> {
        self.get_current_config(DEPLOY_GENERATION).await
    }

    #[mz_ore::instrument]
    async fn has_system_config_synced_once(&mut self) -> Result<bool, CatalogError> {
        self.get_current_config(SYSTEM_CONFIG_SYNCED_KEY)
            .await
            .map(|value| value.map(|value| value > 0).unwrap_or(false))
    }

    #[mz_ore::instrument]
    async fn trace_unconsolidated(&mut self) -> Result<Trace, CatalogError> {
        self.sync_to_current_upper().await?;
        if self.is_initialized_inner() {
            let snapshot = self.snapshot_unconsolidated().await;
            Ok(Trace::from_snapshot(snapshot))
        } else {
            Err(CatalogError::Durable(DurableCatalogError::Uninitialized))
        }
    }

    #[mz_ore::instrument]
    async fn trace_consolidated(&mut self) -> Result<Trace, CatalogError> {
        self.sync_to_current_upper().await?;
        if self.is_initialized_inner() {
            let snapshot = self.current_snapshot().await?;
            Ok(Trace::from_snapshot(snapshot))
        } else {
            Err(CatalogError::Durable(DurableCatalogError::Uninitialized))
        }
    }

    #[mz_ore::instrument(level = "debug")]
    async fn expire(self: Box<Self>) {
        self.expire().await
    }
}

/// Certain large collections are only needed during startup. This struct helps us cache these
/// values during startup and ignore them at all other times.
#[derive(Debug)]
enum LargeCollectionStartupCache<T> {
    /// Actively caching any new value that is seen.
    Open(Vec<(T, Diff)>),
    /// Ignoring any new value that is seen.
    Closed,
}

impl<T> LargeCollectionStartupCache<T> {
    /// Create a new opened [`LargeCollectionStartupCache`].
    fn new_open() -> LargeCollectionStartupCache<T> {
        LargeCollectionStartupCache::Open(Vec::new())
    }

    /// Add value to cache if open, otherwise ignore.
    fn push(&mut self, value: (T, Diff)) {
        match self {
            LargeCollectionStartupCache::Open(cache) => {
                cache.push(value);
            }
            LargeCollectionStartupCache::Closed => {
                // Cache is closed, ignore value.
            }
        }
    }
}

impl<T: Ord> LargeCollectionStartupCache<T> {
    /// If the cache is open, then return all the cached values and close the cache, otherwise
    /// return `None`.
    fn take(&mut self) -> Option<Vec<T>> {
        if let Self::Open(mut cache) = std::mem::replace(self, Self::Closed) {
            differential_dataflow::consolidation::consolidate(&mut cache);
            let cache = cache
                .into_iter()
                .map(|(v, diff)| {
                    assert_eq!(1, diff, "consolidated cache should have no retraction");
                    v
                })
                .collect();
            Some(cache)
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct CatalogStateInner {
    /// The [`Mode`] that this catalog was opened in.
    mode: Mode,
    /// A cache of audit logs that is only populated during startup.
    audit_logs: LargeCollectionStartupCache<proto::AuditLogKey>,
    /// A cache of storage usage events that is only populated during startup.
    storage_usage_events: LargeCollectionStartupCache<proto::StorageUsageKey>,
}

impl CatalogStateInner {
    fn new(mode: Mode) -> CatalogStateInner {
        CatalogStateInner {
            mode,
            audit_logs: LargeCollectionStartupCache::new_open(),
            storage_usage_events: LargeCollectionStartupCache::new_open(),
        }
    }
}

impl ApplyUpdate<StateUpdateKind> for CatalogStateInner {
    fn apply_update(
        &mut self,
        update: StateUpdate<StateUpdateKind>,
        current_epoch: &mut FenceableEpoch,
        metrics: &Arc<Metrics>,
    ) -> Result<Option<StateUpdate<StateUpdateKind>>, DurableCatalogError> {
        if let Some(collection_type) = update.kind.collection_type() {
            metrics
                .collection_entries
                .with_label_values(&[&collection_type.to_string()])
                .add(update.diff);
        }

        match (update.kind, update.diff) {
            (StateUpdateKind::AuditLog(key, ()), _) => {
                self.audit_logs.push((key, update.diff));
                Ok(None)
            }
            (StateUpdateKind::StorageUsage(key, ()), _) => {
                self.storage_usage_events.push((key, update.diff));
                Ok(None)
            }
            (StateUpdateKind::Epoch(epoch), 1) => {
                current_epoch.maybe_fence(epoch)?;
                Ok(None)
            }
            (kind, diff) => Ok(Some(StateUpdate {
                kind,
                ts: update.ts,
                diff,
            })),
        }
    }
}

/// A durable store of the catalog state using Persist as an implementation.
type PersistCatalogState = PersistHandle<StateUpdateKind, CatalogStateInner>;

#[async_trait]
impl ReadOnlyDurableCatalogState for PersistCatalogState {
    fn epoch(&self) -> Epoch {
        self.epoch
            .epoch()
            .expect("opened catalog state must have an epoch")
    }

    #[mz_ore::instrument(level = "debug")]
    async fn expire(self: Box<Self>) {
        self.expire().await
    }

    #[mz_ore::instrument(level = "debug")]
    async fn get_audit_logs(&mut self) -> Result<Vec<VersionedEvent>, CatalogError> {
        self.sync_to_current_upper().await?;
        let audit_logs = match self.update_applier.audit_logs.take() {
            Some(audit_logs) => audit_logs,
            None => {
                error!("audit logs were not found in cache, so they were retrieved from persist, this is unexpected and bad for performance");
                self.persist_snapshot()
                    .await
                    .filter_map(
                        |StateUpdate {
                             kind,
                             ts: _,
                             diff: _,
                         }| match kind {
                            StateUpdateKind::AuditLog(key, ()) => Some(key),
                            _ => None,
                        },
                    )
                    .collect()
            }
        };
        let mut audit_logs: Vec<_> = audit_logs
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|key: AuditLogKey| key.event)
            .collect::<Result<_, _>>()?;
        audit_logs.sort_by(|a, b| a.sortable_id().cmp(&b.sortable_id()));
        Ok(audit_logs)
    }

    #[mz_ore::instrument(level = "debug")]
    async fn get_next_id(&mut self, id_type: &str) -> Result<u64, CatalogError> {
        self.with_trace(|trace| {
            Ok(trace
                .into_iter()
                .rev()
                .filter_map(|(kind, _, _)| match kind {
                    StateUpdateKind::IdAllocator(key, value) if key.name == id_type => {
                        Some(value.next_id)
                    }
                    _ => None,
                })
                .next()
                .expect("must exist"))
        })
        .await
    }

    #[mz_ore::instrument(level = "debug")]
    async fn get_persist_txn_tables(
        &mut self,
    ) -> Result<Option<PersistTxnTablesImpl>, CatalogError> {
        let value = self
            .with_trace(|trace| {
                Ok(trace
                    .into_iter()
                    .rev()
                    .filter_map(|(kind, _, _)| match kind {
                        StateUpdateKind::Config(key, value) if key.key == PERSIST_TXN_TABLES => {
                            Some(value.value)
                        }
                        _ => None,
                    })
                    .next())
            })
            .await?;
        value
            .map(PersistTxnTablesImpl::try_from)
            .transpose()
            .map_err(|err| {
                DurableCatalogError::from(TryFromProtoError::UnknownEnumVariant(err.to_string()))
                    .into()
            })
    }

    #[mz_ore::instrument(level = "debug")]
    async fn snapshot(&mut self) -> Result<Snapshot, CatalogError> {
        self.with_snapshot(Ok).await
    }
}

#[async_trait]
impl DurableCatalogState for PersistCatalogState {
    fn is_read_only(&self) -> bool {
        matches!(self.update_applier.mode, Mode::Readonly)
    }

    #[mz_ore::instrument(level = "debug")]
    async fn transaction(&mut self) -> Result<Transaction, CatalogError> {
        self.metrics.transactions_started.inc();
        let snapshot = self.snapshot().await?;
        Transaction::new(self, snapshot)
    }

    #[mz_ore::instrument(level = "debug")]
    async fn commit_transaction(
        &mut self,
        txn_batch: TransactionBatch,
    ) -> Result<(), CatalogError> {
        async fn commit_transaction_inner(
            catalog: &mut PersistCatalogState,
            txn_batch: TransactionBatch,
        ) -> Result<(), CatalogError> {
            // If the transaction is empty then we don't error, even in read-only mode. This matches the
            // semantics that the stash uses.
            if !txn_batch.is_empty() && catalog.is_read_only() {
                return Err(DurableCatalogError::NotWritable(
                    "cannot commit a transaction in a read-only catalog".to_string(),
                )
                .into());
            }

            let updates = StateUpdate::from_txn_batch(txn_batch).collect();
            debug!("committing updates: {updates:?}");

            if matches!(catalog.update_applier.mode, Mode::Writable) {
                catalog.compare_and_append(updates).await?;
            } else if matches!(catalog.update_applier.mode, Mode::Savepoint) {
                let ts = catalog.upper;
                let updates =
                    updates
                        .into_iter()
                        .map(|(kind, diff)| StateUpdate { kind, ts, diff });
                catalog.apply_updates(updates)?;
            }

            Ok(())
        }
        self.metrics.transaction_commits.inc();
        let counter = self.metrics.transaction_commit_latency_seconds.clone();
        commit_transaction_inner(self, txn_batch)
            .wall_time()
            .inc_by(counter)
            .await
    }

    #[mz_ore::instrument(level = "debug")]
    async fn confirm_leadership(&mut self) -> Result<(), CatalogError> {
        // Read only catalog does not care about leadership.
        if self.is_read_only() {
            return Ok(());
        }
        self.sync_to_current_upper().await?;
        Ok(())
    }

    // TODO(jkosh44) For most modifications we delegate to transactions to avoid duplicate code.
    // This is slightly inefficient because we have to clone an entire snapshot when we usually
    // only need one part of the snapshot. A Potential mitigation against these performance hits is
    // to utilize `CoW`s in `Transaction`s to avoid cloning unnecessary state.

    #[mz_ore::instrument]
    async fn get_and_prune_storage_usage(
        &mut self,
        retention_period: Option<Duration>,
        boot_ts: mz_repr::Timestamp,
        _wait_for_consolidation: bool,
    ) -> Result<Vec<VersionedStorageUsage>, CatalogError> {
        self.sync_to_current_upper().await?;
        // If no usage retention period is set, set the cutoff to MIN so nothing
        // is removed.
        let cutoff_ts = match retention_period {
            None => u128::MIN,
            Some(period) => u128::from(boot_ts).saturating_sub(period.as_millis()),
        };
        let storage_usage = match self.update_applier.storage_usage_events.take() {
            Some(storage_usage) => storage_usage,
            None => {
                error!("storage usage events were not found in cache, so they were retrieved from persist, this is unexpected and bad for performance");
                self.persist_snapshot()
                    .await
                    .filter_map(
                        |StateUpdate {
                             kind,
                             ts: _,
                             diff: _,
                         }| match kind {
                            StateUpdateKind::StorageUsage(key, ()) => Some(key),
                            _ => None,
                        },
                    )
                    .collect()
            }
        };

        let storage_usage = storage_usage
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|key: StorageUsageKey| key.metric);
        let mut events = Vec::new();
        let mut expired = Vec::new();

        for event in storage_usage {
            let event = event?;
            if u128::from(event.timestamp()) >= cutoff_ts {
                events.push(event);
            } else if retention_period.is_some() {
                debug!("pruning storage event {event:?}");
                expired.push(event);
            }
        }

        events.sort_by(|event1, event2| event1.sortable_id().cmp(&event2.sortable_id()));

        if !self.is_read_only() {
            let mut txn = self.transaction().await?;
            txn.remove_storage_usage_events(expired);
            txn.commit().await?;
        } else {
            self.confirm_leadership().await?;
        }

        Ok(events)
    }

    fn shard_id(&self) -> ShardId {
        self.shard_id
    }
}

/// Deterministically generate an ID for the given `organization_id` and `seed`.
fn shard_id(organization_id: Uuid, seed: usize) -> ShardId {
    let hash = sha2::Sha256::digest(format!("{organization_id}{seed}")).to_vec();
    soft_assert_eq_or_log!(hash.len(), 32, "SHA256 returns 32 bytes (256 bits)");
    let uuid = Uuid::from_slice(&hash[0..16]).expect("from_slice accepts exactly 16 bytes");
    ShardId::from_str(&format!("s{uuid}")).expect("known to be valid")
}

/// Returns the schema of the `Row`s/`SourceData`s stored in the persist
/// shard backing the catalog.
pub(crate) fn desc() -> RelationDesc {
    RelationDesc::empty().with_column("data", ScalarType::Jsonb.nullable(false))
}

/// Fetch the current upper of the catalog state.
async fn current_upper(
    write_handle: &mut WriteHandle<SourceData, (), Timestamp, Diff>,
) -> Timestamp {
    write_handle
        .fetch_recent_upper()
        .await
        .as_option()
        .cloned()
        .expect("we use a totally ordered time and never finalize the shard")
}

/// Generates a timestamp for reading from `read_handle` that is as fresh as possible, given
/// `upper`.
fn as_of(read_handle: &ReadHandle<SourceData, (), Timestamp, Diff>, upper: Timestamp) -> Timestamp {
    soft_assert_or_log!(
        upper > Timestamp::minimum(),
        "Catalog persist shard is uninitialized"
    );
    let since = read_handle.since().clone();
    let mut as_of = upper.saturating_sub(1);
    as_of.advance_by(since.borrow());
    as_of
}

/// Fetch the persist version of the catalog upgrade shard, if one exists. A version will not
/// exist if the catalog upgrade shard itself doesn't exist which could happen in the following
/// scenarios:
///
///   - We are creating a brand new environment.
///   - We are upgrading from a version of the code where the catalog upgrade shard didn't
///   exist.
async fn fetch_catalog_upgrade_shard_version(
    persist_client: &PersistClient,
    upgrade_shard_id: ShardId,
) -> Option<semver::Version> {
    let shard_state = persist_client
        .inspect_shard::<Timestamp>(&upgrade_shard_id)
        .await
        .ok()?;
    let json_state = serde_json::to_value(shard_state).expect("state serialization error");
    let upgrade_version = json_state
        .get("applier_version")
        .cloned()
        .expect("missing applier_version");
    let upgrade_version =
        serde_json::from_value(upgrade_version).expect("version deserialization error");
    Some(upgrade_version)
}

/// Generates an iterator of [`StateUpdate`] that contain all updates to the catalog
/// state up to, and including, `as_of`.
///
/// The output is consolidated and sorted by timestamp in ascending order.
#[mz_ore::instrument(level = "debug")]
async fn snapshot_binary(
    read_handle: &mut ReadHandle<SourceData, (), Timestamp, Diff>,
    as_of: Timestamp,
    metrics: &Arc<Metrics>,
) -> impl Iterator<Item = StateUpdate<StateUpdateKindRaw>> + DoubleEndedIterator {
    metrics.snapshots_taken.inc();
    let counter = metrics.snapshot_latency_seconds.clone();
    snapshot_binary_inner(read_handle, as_of)
        .wall_time()
        .inc_by(counter)
        .await
}

/// Generates an iterator of [`StateUpdate`] that contain all updates to the catalog
/// state up to, and including, `as_of`.
///
/// The output is consolidated and sorted by timestamp in ascending order.
#[mz_ore::instrument(level = "debug")]
async fn snapshot_binary_inner(
    read_handle: &mut ReadHandle<SourceData, (), Timestamp, Diff>,
    as_of: Timestamp,
) -> impl Iterator<Item = StateUpdate<StateUpdateKindRaw>> + DoubleEndedIterator {
    let snapshot = read_handle
        .snapshot_and_fetch(Antichain::from_elem(as_of))
        .await
        .expect("we have advanced the restart_as_of by the since");
    soft_assert_no_log!(
        snapshot.iter().all(|(_, _, diff)| *diff == 1),
        "snapshot_and_fetch guarantees a consolidated result: {snapshot:?}"
    );
    snapshot
        .into_iter()
        .map(Into::<StateUpdate<StateUpdateKindRaw>>::into)
        .sorted_by(|a, b| Ord::cmp(&b.ts, &a.ts))
}

// Debug methods.
impl Trace {
    /// Generates a [`Trace`] from snapshot.
    fn from_snapshot(snapshot: impl IntoIterator<Item = StateUpdate>) -> Trace {
        let mut trace = Trace::new();
        for StateUpdate { kind, ts, diff } in snapshot {
            match kind {
                StateUpdateKind::AuditLog(k, v) => trace.audit_log.values.push(((k, v), ts, diff)),
                StateUpdateKind::Cluster(k, v) => trace.clusters.values.push(((k, v), ts, diff)),
                StateUpdateKind::ClusterReplica(k, v) => {
                    trace.cluster_replicas.values.push(((k, v), ts, diff))
                }
                StateUpdateKind::Comment(k, v) => trace.comments.values.push(((k, v), ts, diff)),
                StateUpdateKind::Config(k, v) => trace.configs.values.push(((k, v), ts, diff)),
                StateUpdateKind::Database(k, v) => trace.databases.values.push(((k, v), ts, diff)),
                StateUpdateKind::DefaultPrivilege(k, v) => {
                    trace.default_privileges.values.push(((k, v), ts, diff))
                }
                StateUpdateKind::Epoch(_) => {
                    // Epoch not included in trace.
                }
                StateUpdateKind::IdAllocator(k, v) => {
                    trace.id_allocator.values.push(((k, v), ts, diff))
                }
                StateUpdateKind::IntrospectionSourceIndex(k, v) => {
                    trace.introspection_sources.values.push(((k, v), ts, diff))
                }
                StateUpdateKind::Item(k, v) => trace.items.values.push(((k, v), ts, diff)),
                StateUpdateKind::Role(k, v) => trace.roles.values.push(((k, v), ts, diff)),
                StateUpdateKind::Schema(k, v) => trace.schemas.values.push(((k, v), ts, diff)),
                StateUpdateKind::Setting(k, v) => trace.settings.values.push(((k, v), ts, diff)),
                StateUpdateKind::StorageUsage(k, v) => {
                    trace.storage_usage.values.push(((k, v), ts, diff))
                }
                StateUpdateKind::SystemConfiguration(k, v) => {
                    trace.system_configurations.values.push(((k, v), ts, diff))
                }
                StateUpdateKind::SystemObjectMapping(k, v) => {
                    trace.system_object_mappings.values.push(((k, v), ts, diff))
                }
                StateUpdateKind::SystemPrivilege(k, v) => {
                    trace.system_privileges.values.push(((k, v), ts, diff))
                }
                StateUpdateKind::StorageCollectionMetadata(k, v) => trace
                    .storage_collection_metadata
                    .values
                    .push(((k, v), ts, diff)),
                StateUpdateKind::UnfinalizedShard(k, ()) => {
                    trace.unfinalized_shards.values.push(((k, ()), ts, diff))
                }
                StateUpdateKind::PersistTxnShard((), v) => {
                    trace.persist_txn_shard.values.push((((), v), ts, diff))
                }
            }
        }
        trace
    }
}

impl UnopenedPersistCatalogState {
    /// Manually update value of `key` in collection `T` to `value`.
    #[mz_ore::instrument]
    pub(crate) async fn debug_edit<T: Collection>(
        &mut self,
        key: T::Key,
        value: T::Value,
    ) -> Result<Option<T::Value>, CatalogError>
    where
        T::Key: PartialEq + Eq + Debug + Clone,
        T::Value: Debug + Clone,
    {
        let (_, prev) = retry(self, move |s| {
            let key = key.clone();
            let value = value.clone();
            async {
                let prev = s.debug_edit_inner::<T>(key, value).await;
                (s, prev)
            }
        })
        .await;
        prev
    }

    #[mz_ore::instrument]
    pub(crate) async fn debug_edit_inner<T: Collection>(
        &mut self,
        key: T::Key,
        value: T::Value,
    ) -> Result<Option<T::Value>, CatalogError>
    where
        T::Key: PartialEq + Eq + Debug + Clone,
        T::Value: Debug + Clone,
    {
        let snapshot = self.current_snapshot().await?;
        let trace = Trace::from_snapshot(snapshot);
        let collection_trace = T::collection_trace(trace);
        let prev_values: Vec<_> = collection_trace
            .values
            .into_iter()
            .filter(|((k, _), _, diff)| {
                soft_assert_eq_or_log!(*diff, 1, "trace is consolidated");
                &key == k
            })
            .collect();

        let prev_value = match &prev_values[..] {
            [] => None,
            [((_, v), _, _)] => Some(v.clone()),
            prev_values => panic!("multiple values found for key {key:?}: {prev_values:?}"),
        };

        let mut updates: Vec<_> = prev_values
            .into_iter()
            .map(|((k, v), _, _)| (T::update(k, v), -1))
            .collect();
        updates.push((T::update(key.clone(), value.clone()), 1));
        // We must fence out all other catalogs since we are writing.
        let fence_updates = self.increment_epoch()?;
        updates.extend(fence_updates);
        self.compare_and_append(updates).await?;
        Ok(prev_value)
    }

    /// Manually delete `key` from collection `T`.
    #[mz_ore::instrument]
    pub(crate) async fn debug_delete<T: Collection>(
        &mut self,
        key: T::Key,
    ) -> Result<(), CatalogError>
    where
        T::Key: PartialEq + Eq + Debug + Clone,
        T::Value: Debug,
    {
        let (_, res) = retry(self, move |s| {
            let key = key.clone();
            async {
                let res = s.debug_delete_inner::<T>(key).await;
                (s, res)
            }
        })
        .await;
        res
    }

    /// Manually delete `key` from collection `T`.
    #[mz_ore::instrument]
    async fn debug_delete_inner<T: Collection>(&mut self, key: T::Key) -> Result<(), CatalogError>
    where
        T::Key: PartialEq + Eq + Debug,
        T::Value: Debug,
    {
        let snapshot = self.current_snapshot().await?;
        let trace = Trace::from_snapshot(snapshot);
        let collection_trace = T::collection_trace(trace);
        let mut retractions: Vec<_> = collection_trace
            .values
            .into_iter()
            .filter(|((k, _), _, diff)| {
                soft_assert_eq_or_log!(*diff, 1, "trace is consolidated");
                &key == k
            })
            .map(|((k, v), _, _)| (T::update(k, v), -1))
            .collect();
        // We must fence out all other catalogs since we are writing.
        let fence_updates = self.increment_epoch()?;
        retractions.extend(fence_updates);
        self.compare_and_append(retractions).await?;
        Ok(())
    }

    /// Generates a [`Vec<StateUpdate>`] that contain all updates to the catalog
    /// state.
    ///
    /// The output is consolidated and sorted by timestamp in ascending order and the current upper.
    async fn current_snapshot(
        &mut self,
    ) -> Result<impl IntoIterator<Item = StateUpdate> + '_, CatalogError> {
        self.sync_to_current_upper().await?;
        self.consolidate();
        Ok(self.snapshot.iter().cloned().map(|(kind, ts, diff)| {
            let kind = TryIntoStateUpdateKind::try_into(kind).expect("kind decoding error");
            StateUpdate { kind, ts, diff }
        }))
    }

    /// Increment `self.epoch` and return the updates needed to make this change durable.
    ///
    /// The caller is expected to compare and append these updates promptly.
    fn increment_epoch(&mut self) -> Result<[(StateUpdateKind, Diff); 2], DurableCatalogError> {
        let current_epoch = self
            .epoch
            .validate()?
            .expect("cannot edit/delete from unopened catalog");
        let next_epoch =
            Epoch::new(current_epoch.get() + 1).expect("non-zero epoch plus 1 is always non-zero");
        self.epoch = FenceableEpoch::Unfenced(Some(next_epoch));
        Ok([
            (StateUpdateKind::Epoch(current_epoch), -1),
            (StateUpdateKind::Epoch(next_epoch), 1),
        ])
    }
}

/// Wrapper for [`Retry::retry_async_with_state`] so that all commands share the same retry behavior.
async fn retry<F, S, U, R, T, E>(state: S, mut f: F) -> (S, Result<T, E>)
where
    F: FnMut(S) -> U,
    U: Future<Output = (S, R)>,
    R: Into<RetryResult<T, E>>,
{
    Retry::default()
        .max_duration(Duration::from_secs(30))
        .clamp_backoff(Duration::from_secs(1))
        .retry_async_with_state(state, |_, s| f(s))
        .await
}
