// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod metrics;
mod state_update;

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use differential_dataflow::lattice::Lattice;
use futures::StreamExt;
use itertools::Itertools;
use mz_audit_log::{VersionedEvent, VersionedStorageUsage};
use mz_ore::collections::CollectionExt;
use mz_ore::metrics::{MetricsFutureExt, MetricsRegistry};
use mz_ore::now::EpochMillis;
use mz_ore::retry::{Retry, RetryResult};
use mz_ore::{soft_assert, soft_assert_eq, soft_assert_ne};
use mz_persist_client::read::{ListenEvent, ReadHandle, Subscribe};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_proto::{ProtoType, RustType};
use mz_repr::Diff;
use mz_storage_types::sources::Timeline;
use sha2::Digest;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tracing::debug;
use uuid::Uuid;

use crate::durable::debug::{Collection, DebugCatalogState, Trace};
use crate::durable::impls::persist::metrics::Metrics;
use crate::durable::impls::persist::state_update::{
    IntoStateUpdateKindBinary, StateUpdateKindBinary, StateUpdateKindSchema,
};
pub use crate::durable::impls::persist::state_update::{StateUpdate, StateUpdateKind};
use crate::durable::initialize::{DEPLOY_GENERATION, ENABLE_PERSIST_TXN_TABLES, USER_VERSION_KEY};
use crate::durable::objects::serialization::proto;
use crate::durable::objects::{AuditLogKey, Config, DurableType, Snapshot, StorageUsageKey};
use crate::durable::transaction::TransactionBatch;
use crate::durable::upgrade::CATALOG_VERSION;
use crate::durable::{
    initialize, BootstrapArgs, CatalogError, DurableCatalogError, DurableCatalogState, Epoch,
    OpenableDurableCatalogState, ReadOnlyDurableCatalogState, TimelineTimestamp, Transaction,
};

/// New-type used to represent timestamps in persist.
type Timestamp = mz_repr::Timestamp;

/// Durable catalog mode that dictates the effect of mutable operations.
#[derive(Debug)]
enum Mode {
    /// Mutable operations are prohibited.
    Readonly,
    /// Mutable operations have an effect in-memory, but aren't persisted durably.
    Savepoint,
    /// Mutable operations have an effect in-memory and durably.
    Writable,
}

/// Handles and metadata needed to interact with persist.
///
/// Production users should call [`Self::expire`] before dropping a [`PersistHandle`] so that it
/// can expire its leases. If/when rust gets AsyncDrop, this will be done automatically.
#[derive(Debug)]
pub struct PersistHandle {
    /// Write handle to persist.
    write_handle: WriteHandle<StateUpdateKindBinary, (), Timestamp, Diff>,
    /// Read handle to persist.
    read_handle: ReadHandle<StateUpdateKindBinary, (), Timestamp, Diff>,
    /// Handle for connecting to persist.
    persist_client: PersistClient,
    /// Catalog shard ID.
    shard_id: ShardId,
    /// Cache of the most recent catalog snapshot.
    snapshot_cache: Option<(Timestamp, Vec<StateUpdate>)>,
    /// Metrics for the persist catalog.
    metrics: Arc<Metrics>,
}

impl PersistHandle {
    /// Deterministically generate the a ID for the given `organization_id` and `seed`.
    fn shard_id(organization_id: Uuid, seed: usize) -> ShardId {
        let hash = sha2::Sha256::digest(format!("{organization_id}{seed}")).to_vec();
        soft_assert_eq!(hash.len(), 32, "SHA256 returns 32 bytes (256 bits)");
        let uuid = Uuid::from_slice(&hash[0..16]).expect("from_slice accepts exactly 16 bytes");
        ShardId::from_str(&format!("s{uuid}")).expect("known to be valid")
    }

    /// Create a new [`PersistHandle`] to the catalog state associated with `organization_id`.
    #[tracing::instrument(level = "debug", skip(persist_client))]
    pub(crate) async fn new(
        persist_client: PersistClient,
        organization_id: Uuid,
        registry: &MetricsRegistry,
    ) -> PersistHandle {
        const SEED: usize = 1;
        let shard_id = Self::shard_id(organization_id, SEED);
        debug!(?shard_id, "new persist backed catalog state");
        let (write_handle, read_handle) = persist_client
            .open(
                shard_id,
                Arc::new(StateUpdateKindSchema::default()),
                Arc::new(UnitSchema::default()),
                diagnostics(),
            )
            .await
            .expect("invalid usage");
        let metrics = Arc::new(Metrics::new(registry));
        PersistHandle {
            write_handle,
            read_handle,
            persist_client,
            shard_id,
            snapshot_cache: None,
            metrics,
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn open_inner(
        mut self,
        mode: Mode,
        boot_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        let read_only = matches!(mode, Mode::Readonly);
        let (persist_shard_readable, upper) = self.is_persist_shard_readable().await;

        // Fence out previous catalogs.
        let mut fence_updates = Vec::with_capacity(2);
        let prev_epoch = if persist_shard_readable {
            let as_of = self.as_of(upper);
            let prev_epoch = self.get_epoch(as_of).await;
            fence_updates.push(StateUpdate {
                kind: StateUpdateKind::Epoch(prev_epoch),
                ts: upper,
                diff: -1,
            });
            Some(prev_epoch.get())
        } else {
            None
        };
        let mut current_epoch = prev_epoch.unwrap_or(1);
        // Only writable catalogs attempt to increment the epoch.
        if matches!(mode, Mode::Writable) {
            current_epoch = current_epoch + 1;
        }
        let current_epoch = Epoch::new(current_epoch).expect("known to be non-zero");
        fence_updates.push(StateUpdate {
            kind: StateUpdateKind::Epoch(current_epoch),
            ts: upper,
            diff: 1,
        });
        debug!(
            ?upper,
            ?prev_epoch,
            ?current_epoch,
            "fencing previous catalogs"
        );
        if matches!(mode, Mode::Writable) {
            let next_upper = upper.step_forward();
            self.compare_and_append(fence_updates, upper, next_upper)
                .await?;
        }

        let (is_initialized, upper) = self.is_initialized_inner().await;
        if !matches!(mode, Mode::Writable) && !is_initialized {
            return Err(CatalogError::Durable(DurableCatalogError::NotWritable(
                format!("catalog tables do not exist; will not create in {mode:?} mode"),
            )));
        }
        soft_assert_ne!(upper, Timestamp::minimum());

        let restart_as_of = self.as_of(upper);

        // Perform data migrations.
        if is_initialized && !read_only {
            let user_version = self.get_user_version(restart_as_of).await;
            let user_version =
                user_version.ok_or(CatalogError::Durable(DurableCatalogError::Uninitialized))?;
            if user_version != CATALOG_VERSION {
                // TODO(jkosh44) Implement migrations.
                panic!("the persist catalog does not know how to perform migrations yet");
            }
        }

        debug!(
            ?is_initialized,
            ?upper,
            ?restart_as_of,
            "initializing catalog state"
        );
        let subscribe = self
            .read_handle
            .subscribe(Antichain::from_elem(restart_as_of))
            .await
            .expect("invalid usage");
        let mut catalog = PersistCatalogState {
            mode,
            write_handle: self.write_handle,
            subscribe,
            persist_client: self.persist_client,
            shard_id: self.shard_id,
            upper: Timestamp::minimum(),
            epoch: current_epoch,
            // Initialize empty in-memory state.
            snapshot: Snapshot::empty(),
            metrics: self.metrics,
        };
        catalog.sync(upper).await?;

        let txn = if is_initialized {
            let mut txn = catalog.transaction().await?;
            if let Some(deploy_generation) = deploy_generation {
                txn.set_config(DEPLOY_GENERATION.into(), deploy_generation)?;
            }
            txn
        } else {
            soft_assert!(
                catalog.snapshot.is_empty(),
                "snapshot should not contain anything for an uninitialized catalog: {:?}",
                catalog.snapshot
            );
            let mut txn = catalog.transaction().await?;
            initialize::initialize(&mut txn, bootstrap_args, boot_ts, deploy_generation).await?;
            txn
        };

        if read_only {
            let (txn_batch, _) = txn.into_parts();
            // The upper here doesn't matter because we are only apply the updates in memory.
            let updates = StateUpdate::from_txn_batch(txn_batch, catalog.upper);
            catalog.apply_updates(updates)?;
        } else {
            txn.commit().await?;
        }

        Ok(Box::new(catalog))
    }

    /// Fetch the current upper of the catalog state.
    async fn current_upper(&mut self) -> Timestamp {
        current_upper(&mut self.write_handle).await
    }

    /// Generates a timestamp for reading from the catalog that is as fresh as possible, given
    /// `upper`.
    fn as_of(&self, upper: Timestamp) -> Timestamp {
        as_of(&self.read_handle, upper)
    }

    /// Reports if the catalog state has been initialized, and the current upper.
    async fn is_initialized_inner(&mut self) -> (bool, Timestamp) {
        let (persist_shard_readable, upper) = self.is_persist_shard_readable().await;
        let is_initialized = if !persist_shard_readable {
            false
        } else {
            let as_of = self.as_of(upper);
            // Configs are readable using any catalog version since they can't be migrated, so they
            // can be used to tell if the catalog is populated.
            self.get_configs(as_of).await.next().is_some()
        };
        (is_initialized, upper)
    }

    /// Reports if the persist shard can be read at some time, and the current upper. A persist
    /// shard can only be read once it's been written to at least once.
    async fn is_persist_shard_readable(&mut self) -> (bool, Timestamp) {
        let upper = self.current_upper().await;
        (upper > Timestamp::minimum(), upper)
    }

    /// Generates a [`Vec<StateUpdate>`] that contain all updates to the catalog
    /// state.
    ///
    /// The output is consolidated and sorted by timestamp in ascending order and the current upper.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn current_snapshot(&mut self) -> (&Vec<StateUpdate<StateUpdateKind>>, Timestamp) {
        static EMPTY_SNAPSHOT: Vec<StateUpdate> = Vec::new();
        let (persist_shard_readable, current_upper) = self.is_persist_shard_readable().await;
        if persist_shard_readable {
            let as_of = self.as_of(current_upper);
            let snapshot = self.snapshot(as_of).await;
            (snapshot, current_upper)
        } else {
            (&EMPTY_SNAPSHOT, current_upper)
        }
    }

    /// Generates an iterator of [`StateUpdate`] that contain all updates to the catalog
    /// state up to, and including, `as_of`.
    ///
    /// The output is consolidated and sorted by timestamp in ascending order.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn snapshot<'a>(&'a mut self, as_of: Timestamp) -> &'a Vec<StateUpdate<StateUpdateKind>> {
        match &self.snapshot_cache {
            Some((cached_as_of, _)) if as_of == *cached_as_of => {}
            _ => {
                let snapshot: Vec<_> = snapshot(&mut self.read_handle, as_of, &self.metrics)
                    .await
                    .collect();
                self.snapshot_cache = Some((as_of, snapshot));
            }
        }

        &self.snapshot_cache.as_ref().expect("populated above").1
    }

    /// Generates an iterator of [`StateUpdate`] that contain all updates to the catalog
    /// state up to, and including, `as_of`, in binary format.
    ///
    /// The output is consolidated and sorted by timestamp in ascending order.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn snapshot_binary(
        &mut self,
        as_of: Timestamp,
    ) -> impl Iterator<Item = StateUpdate<StateUpdateKindBinary>> + DoubleEndedIterator {
        snapshot_binary(&mut self.read_handle, as_of).await
    }

    /// Generates an iterator of [`StateUpdate`] that contain all unconsolidated updates to the
    /// catalog state up to, and including, `as_of`.
    async fn snapshot_unconsolidated(
        &mut self,
        as_of: Timestamp,
    ) -> impl Iterator<Item = StateUpdate<StateUpdateKind>> + DoubleEndedIterator {
        let mut snapshot = Vec::new();
        let mut stream = Box::pin(
            // We use `snapshot_and_stream` because it guarantees unconsolidated output.
            self.read_handle
                .snapshot_and_stream(Antichain::from_elem(as_of))
                .await
                .expect("we have advanced the restart_as_of by the since"),
        );
        while let Some(update) = stream.next().await {
            snapshot.push(update)
        }
        snapshot
            .into_iter()
            .map(|((kind, _unit), ts, diff)| StateUpdate {
                kind: kind
                    .expect("kind decoding error")
                    .try_into()
                    .expect("kind decoding error"),
                ts,
                diff,
            })
    }

    /// Get the current value of config `key`.
    ///
    /// Some configs need to be read before the catalog is opened for bootstrapping.
    async fn get_current_config(&mut self, key: &str) -> Option<u64> {
        let (persist_shard_readable, current_upper) = self.is_persist_shard_readable().await;
        if persist_shard_readable {
            let as_of = self.as_of(current_upper);
            self.get_config(key, as_of).await
        } else {
            None
        }
    }

    /// Get value of config `key` at `as_of`.
    ///
    /// Some configs need to be read before the catalog is opened for bootstrapping.
    async fn get_config(&mut self, key: &str, as_of: Timestamp) -> Option<u64> {
        let mut configs: Vec<_> = self
            .get_configs(as_of)
            .await
            .filter_map(|config| {
                if key == &config.key {
                    Some(config.value)
                } else {
                    None
                }
            })
            .collect();
        soft_assert!(
            configs.len() <= 1,
            "multiple configs should not share the same key: {configs:?}"
        );
        configs.pop()
    }

    /// Get all Configs.
    ///
    /// Some configs need to be read before the catalog is opened for bootstrapping.
    async fn get_configs(&mut self, as_of: Timestamp) -> impl Iterator<Item = Config> {
        self.snapshot_binary(as_of)
            .await
            .into_iter()
            .rev()
            // Configs can never be migrated so we know that they will always convert successfully
            // from binary.
            .filter_map(|update| {
                soft_assert_eq!(update.diff, 1, "snapshot returns consolidated results");
                let kind: Option<StateUpdateKind> = update.kind.try_into().ok();
                kind
            })
            .filter_map(|kind| match kind {
                StateUpdateKind::Config(k, v) => {
                    let k = k.clone().into_rust().expect("invalid config key persisted");
                    let v = v
                        .clone()
                        .into_rust()
                        .expect("invalid config value persisted");
                    Some(Config::from_key_value(k, v))
                }
                _ => None,
            })
    }

    /// Get the user version of this instance.
    ///
    /// The user version is used to determine if a migration is needed.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_user_version(&mut self, as_of: Timestamp) -> Option<u64> {
        self.get_config(USER_VERSION_KEY, as_of).await
    }

    /// Get epoch at `as_of`.
    async fn get_epoch(&mut self, as_of: Timestamp) -> Epoch {
        let epochs = self
            .snapshot_binary(as_of)
            .await
            .into_iter()
            .rev()
            // The epoch can never be migrated so we know that it will always convert successfully
            // from binary.
            .filter_map(|update| {
                soft_assert_eq!(update.diff, 1, "snapshot returns consolidated results");
                let kind: Option<StateUpdateKind> = update.kind.try_into().ok();
                kind
            })
            .filter_map(|kind| match kind {
                StateUpdateKind::Epoch(epoch) => Some(epoch),
                _ => None,
            });
        // There must always be a single epoch.
        epochs.into_element()
    }

    /// Appends `updates` to the catalog state and downgrades the catalog's upper to `next_upper`
    /// iff the current global upper of the catalog is `current_upper`.
    async fn compare_and_append<T: IntoStateUpdateKindBinary>(
        &mut self,
        updates: Vec<StateUpdate<T>>,
        current_upper: Timestamp,
        next_upper: Timestamp,
    ) -> Result<(), CatalogError> {
        compare_and_append(&mut self.write_handle, updates, current_upper, next_upper).await?;
        self.read_handle
            .downgrade_since(&Antichain::from_elem(current_upper))
            .await;
        Ok(())
    }
}

#[async_trait]
impl OpenableDurableCatalogState for PersistHandle {
    #[tracing::instrument(level = "debug", skip(self))]
    async fn open_savepoint(
        mut self: Box<Self>,
        boot_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        self.open_inner(Mode::Savepoint, boot_ts, bootstrap_args, deploy_generation)
            .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn open_read_only(
        mut self: Box<Self>,
        boot_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        self.open_inner(Mode::Readonly, boot_ts, bootstrap_args, None)
            .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn open(
        mut self: Box<Self>,
        boot_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        self.open_inner(Mode::Writable, boot_ts, bootstrap_args, deploy_generation)
            .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn open_debug(mut self: Box<Self>) -> Result<DebugCatalogState, CatalogError> {
        Ok(DebugCatalogState::Persist(*self))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn is_initialized(&mut self) -> Result<bool, CatalogError> {
        Ok(self.is_initialized_inner().await.0)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_deployment_generation(&mut self) -> Result<Option<u64>, CatalogError> {
        Ok(self.get_current_config(DEPLOY_GENERATION).await)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_enable_persist_txn_tables(&mut self) -> Result<Option<bool>, CatalogError> {
        let value = self.get_current_config(ENABLE_PERSIST_TXN_TABLES).await;
        Ok(value.map(|value| value > 0))
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn trace(&mut self) -> Result<Trace, CatalogError> {
        let (persist_shard_readable, current_upper) = self.is_persist_shard_readable().await;
        if persist_shard_readable {
            let as_of = self.as_of(current_upper);
            let snapshot = self.snapshot_unconsolidated(as_of).await;
            Ok(Trace::from_snapshot(snapshot))
        } else {
            Err(CatalogError::Durable(DurableCatalogError::Uninitialized))
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn expire(self) {
        self.read_handle.expire().await;
        self.write_handle.expire().await;
    }
}

/// A durable store of the catalog state using Persist as an implementation.
#[derive(Debug)]
pub struct PersistCatalogState {
    /// The [`Mode`] that this catalog was opened in.
    mode: Mode,
    /// Write handle to persist.
    write_handle: WriteHandle<StateUpdateKindBinary, (), Timestamp, Diff>,
    /// Subscription to catalog changes.
    subscribe: Subscribe<StateUpdateKindBinary, (), Timestamp, Diff>,
    /// Handle for connecting to persist.
    persist_client: PersistClient,
    /// Catalog shard ID.
    shard_id: ShardId,
    /// The current upper of the persist shard.
    upper: Timestamp,
    /// The epoch of this catalog.
    epoch: Epoch,
    /// A cache of the entire catalogs state.
    snapshot: Snapshot,
    /// Metrics for the persist catalog.
    metrics: Arc<Metrics>,
}

impl PersistCatalogState {
    /// Fetch the current upper of the catalog state.
    async fn current_upper(&mut self) -> Timestamp {
        current_upper(&mut self.write_handle).await
    }

    /// Appends `updates` to the catalog state and downgrades the catalog's upper to `next_upper`
    /// iff the current global upper of the catalog is `current_upper`.
    async fn compare_and_append(
        &mut self,
        updates: Vec<StateUpdate>,
        current_upper: Timestamp,
        next_upper: Timestamp,
    ) -> Result<(), CatalogError> {
        compare_and_append(&mut self.write_handle, updates, current_upper, next_upper).await
    }

    /// Generates an iterator of [`StateUpdate`] that contain all updates to the catalog
    /// state.
    ///
    /// The output is consolidated and sorted by timestamp in ascending order.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn persist_snapshot(
        &mut self,
    ) -> impl Iterator<Item = StateUpdate> + DoubleEndedIterator {
        let mut read_handle = self.read_handle().await;
        let as_of = as_of(&read_handle, self.upper);
        let snapshot = snapshot(&mut read_handle, as_of, &self.metrics).await;
        read_handle.expire().await;
        snapshot
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn sync_to_current_upper(&mut self) -> Result<(), CatalogError> {
        let upper = self.current_upper().await;
        if upper != self.upper {
            if self.is_read_only() {
                self.sync(upper).await?;
            } else {
                // non-read-only catalogs do not know how to deal with other writers.
                return Err(DurableCatalogError::Fence(format!(
                    "current catalog upper {:?} fenced by new catalog upper {:?}",
                    self.upper, upper
                ))
                .into());
            }
        }

        Ok(())
    }

    /// Listen and apply all updates up to `target_upper`.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn sync(&mut self, target_upper: Timestamp) -> Result<(), CatalogError> {
        self.metrics.syncs.inc();
        let counter = self.metrics.sync_latency_duration_seconds.clone();
        self.sync_inner(target_upper)
            .wall_time()
            .inc_by(counter)
            .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn sync_inner(&mut self, target_upper: Timestamp) -> Result<(), CatalogError> {
        let mut updates = Vec::new();

        while self.upper < target_upper {
            let listen_events = self.subscribe.fetch_next().await;
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
                        for ((key, _unit), ts, diff) in batch_updates {
                            let kind = key
                                .expect("key decoding error")
                                .try_into()
                                .expect("key decoding error");
                            updates.push(StateUpdate { kind, ts, diff });
                        }
                    }
                }
            }
        }

        self.apply_updates(updates)?;

        Ok(())
    }

    /// Applies [`StateUpdate`]s to the in memory catalog cache.
    #[tracing::instrument(level = "debug", skip_all)]
    fn apply_updates(
        &mut self,
        updates: impl IntoIterator<Item = StateUpdate>,
    ) -> Result<(), DurableCatalogError> {
        fn apply<K, V>(map: &mut BTreeMap<K, V>, key: K, value: V, diff: Diff)
        where
            K: Ord,
            V: Ord + Debug,
        {
            if diff == 1 {
                let prev = map.insert(key, value);
                soft_assert_eq!(
                    prev,
                    None,
                    "values must be explicitly retracted before inserting a new value"
                );
            } else if diff == -1 {
                let prev = map.remove(&key);
                soft_assert_eq!(
                    prev,
                    Some(value),
                    "retraction does not match existing value"
                );
            }
        }

        let mut updates: Vec<_> = updates
            .into_iter()
            .map(|update| (update.kind, update.ts, update.diff))
            .collect();
        // Consolidation is required for correctness. It guarantees that for a single key, there is
        // at most a single retraction and a single insertion per timestamp. Otherwise, we would
        // need to match the retractions and insertions up by value and manually figure out what the
        // end value should be.
        differential_dataflow::consolidation::consolidate_updates(&mut updates);
        // Updates must be applied in timestamp order. Within a timestamp retractions must be
        // applied before insertions or we might end up retracting the wrong value.
        updates.sort_by(|(_, ts1, diff1), (_, ts2, diff2)| ts1.cmp(ts2).then(diff1.cmp(diff2)));

        for (kind, ts, diff) in updates {
            if diff != 1 && diff != -1 {
                panic!("invalid update in consolidated trace: ({kind:?}, {ts:?}, {diff:?})");
            }

            debug!("applying catalog update: ({kind:?}, {ts:?}, {diff:?})");
            match kind {
                StateUpdateKind::AuditLog(_, _) => {
                    // We can ignore audit log updates since it's not cached in memory.
                }
                StateUpdateKind::Cluster(key, value) => {
                    apply(&mut self.snapshot.clusters, key, value, diff);
                }
                StateUpdateKind::ClusterReplica(key, value) => {
                    apply(&mut self.snapshot.cluster_replicas, key, value, diff);
                }
                StateUpdateKind::Comment(key, value) => {
                    apply(&mut self.snapshot.comments, key, value, diff);
                }
                StateUpdateKind::Config(key, value) => {
                    apply(&mut self.snapshot.configs, key, value, diff);
                }
                StateUpdateKind::Database(key, value) => {
                    apply(&mut self.snapshot.databases, key, value, diff);
                }
                StateUpdateKind::DefaultPrivilege(key, value) => {
                    apply(&mut self.snapshot.default_privileges, key, value, diff);
                }
                StateUpdateKind::Epoch(epoch) => {
                    if epoch > self.epoch {
                        soft_assert_eq!(diff, 1);
                        return Err(DurableCatalogError::Fence(format!(
                            "current catalog epoch {} fenced by new catalog epoch {}",
                            self.epoch, epoch
                        )));
                    }
                }
                StateUpdateKind::IdAllocator(key, value) => {
                    apply(&mut self.snapshot.id_allocator, key, value, diff);
                }
                StateUpdateKind::IntrospectionSourceIndex(key, value) => {
                    apply(&mut self.snapshot.introspection_sources, key, value, diff);
                }
                StateUpdateKind::Item(key, value) => {
                    apply(&mut self.snapshot.items, key, value, diff);
                }
                StateUpdateKind::Role(key, value) => {
                    apply(&mut self.snapshot.roles, key, value, diff);
                }
                StateUpdateKind::Schema(key, value) => {
                    apply(&mut self.snapshot.schemas, key, value, diff);
                }
                StateUpdateKind::Setting(key, value) => {
                    apply(&mut self.snapshot.settings, key, value, diff);
                }
                StateUpdateKind::StorageUsage(_, _) => {
                    // We can ignore storage usage since it's not cached in memory.
                }
                StateUpdateKind::SystemConfiguration(key, value) => {
                    apply(&mut self.snapshot.system_configurations, key, value, diff);
                }
                StateUpdateKind::SystemObjectMapping(key, value) => {
                    apply(&mut self.snapshot.system_object_mappings, key, value, diff);
                }
                StateUpdateKind::SystemPrivilege(key, value) => {
                    apply(&mut self.snapshot.system_privileges, key, value, diff);
                }
                StateUpdateKind::Timestamp(key, value) => {
                    apply(&mut self.snapshot.timestamps, key, value, diff);
                }
            }
        }

        Ok(())
    }

    /// Execute and return the results of `f` on the current catalog snapshot.
    ///
    /// Will return an error if the catalog has been fenced out.
    async fn with_snapshot<T>(
        &mut self,
        f: impl FnOnce(&Snapshot) -> Result<T, CatalogError>,
    ) -> Result<T, CatalogError> {
        self.sync_to_current_upper().await?;
        f(&self.snapshot)
    }

    /// Open a read handle to the catalog.
    async fn read_handle(&mut self) -> ReadHandle<StateUpdateKindBinary, (), Timestamp, Diff> {
        self.persist_client
            .open_leased_reader(
                self.shard_id,
                Arc::new(StateUpdateKindSchema::default()),
                Arc::new(UnitSchema::default()),
                diagnostics(),
            )
            .await
            .expect("invalid usage")
    }
}

#[async_trait]
impl ReadOnlyDurableCatalogState for PersistCatalogState {
    fn epoch(&mut self) -> Epoch {
        self.epoch
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn expire(self: Box<Self>) {
        self.write_handle.expire().await;
        // Note: `Subscribe` doesn't expose an `expire` method at the time of writing this comment.
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_timestamps(&mut self) -> Result<Vec<TimelineTimestamp>, CatalogError> {
        self.with_snapshot(|snapshot| {
            Ok(snapshot
                .timestamps
                .clone()
                .into_iter()
                .map(RustType::from_proto)
                .map_ok(|(k, v)| TimelineTimestamp::from_key_value(k, v))
                .collect::<Result<_, _>>()?)
        })
        .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_audit_logs(&mut self) -> Result<Vec<VersionedEvent>, CatalogError> {
        self.sync_to_current_upper().await?;
        // This is only called during bootstrapping and we don't want to cache all
        // audit logs in memory because they can grow quite large. Therefore, we
        // go back to persist and grab everything again.
        let mut audit_logs: Vec<_> = self
            .persist_snapshot()
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
            .map(RustType::from_proto)
            .map_ok(|key: AuditLogKey| key.event)
            .collect::<Result<_, _>>()?;
        audit_logs.sort_by(|a, b| a.sortable_id().cmp(&b.sortable_id()));
        Ok(audit_logs)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_next_id(&mut self, id_type: &str) -> Result<u64, CatalogError> {
        let key = proto::IdAllocKey {
            name: id_type.to_string(),
        };
        self.with_snapshot(|snapshot| {
            Ok(snapshot.id_allocator.get(&key).expect("must exist").next_id)
        })
        .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn snapshot(&mut self) -> Result<Snapshot, CatalogError> {
        self.with_snapshot(|snapshot| Ok(snapshot.clone())).await
    }
}

#[async_trait]
impl DurableCatalogState for PersistCatalogState {
    fn is_read_only(&self) -> bool {
        matches!(self.mode, Mode::Readonly)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn transaction(&mut self) -> Result<Transaction, CatalogError> {
        self.metrics.transactions.inc();
        let snapshot = self.snapshot().await?;
        Transaction::new(self, snapshot)
    }

    #[tracing::instrument(level = "debug", skip(self))]
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

            let current_upper = catalog.upper.clone();
            let next_upper = current_upper.step_forward();

            let updates = StateUpdate::from_txn_batch(txn_batch, current_upper);
            debug!("committing updates: {updates:?}");

            if matches!(catalog.mode, Mode::Writable) {
                catalog
                    .compare_and_append(updates, current_upper, next_upper)
                    .await?;
                debug!(
                    "commit successful, upper advanced from {current_upper:?} to {next_upper:?}",
                );
                catalog.sync(next_upper).await?;
            } else if matches!(catalog.mode, Mode::Savepoint) {
                catalog.apply_updates(updates)?;
            }

            Ok(())
        }
        let counter = self
            .metrics
            .transaction_commit_latency_duration_seconds
            .clone();
        let res = commit_transaction_inner(self, txn_batch)
            .wall_time()
            .inc_by(counter)
            .await;
        self.metrics.transaction_commits_initiated.inc();
        if let Err(_) = &res {
            self.metrics.transaction_commit_errors.inc();
        }
        res
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn confirm_leadership(&mut self) -> Result<(), CatalogError> {
        // Read only catalog does not care about leadership.
        if self.is_read_only() {
            return Ok(());
        }

        let upper = self.current_upper().await;
        if upper == self.upper {
            Ok(())
        } else {
            Err(DurableCatalogError::Fence(format!(
                "current catalog upper {:?} fenced by new catalog upper {:?}",
                self.upper, upper
            ))
            .into())
        }
    }

    // TODO(jkosh44) For most modifications we delegate to transactions to avoid duplicate code.
    // This is slightly inefficient because we have to clone an entire snapshot when we usually
    // only need one part of the snapshot. A Potential mitigation against these performance hits is
    // to utilize `CoW`s in `Transaction`s to avoid cloning unnecessary state.

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_and_prune_storage_usage(
        &mut self,
        retention_period: Option<Duration>,
        boot_ts: mz_repr::Timestamp,
    ) -> Result<Vec<VersionedStorageUsage>, CatalogError> {
        // If no usage retention period is set, set the cutoff to MIN so nothing
        // is removed.
        let cutoff_ts = match retention_period {
            None => u128::MIN,
            Some(period) => u128::from(boot_ts).saturating_sub(period.as_millis()),
        };
        let storage_usage = self
            .persist_snapshot()
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

    #[tracing::instrument(level = "debug", skip(self))]
    async fn set_timestamp(
        &mut self,
        timeline: &Timeline,
        timestamp: mz_repr::Timestamp,
    ) -> Result<(), CatalogError> {
        let mut txn = self.transaction().await?;
        txn.set_timestamp(timeline.clone(), timestamp)?;
        txn.commit().await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn allocate_id(&mut self, id_type: &str, amount: u64) -> Result<Vec<u64>, CatalogError> {
        if amount == 0 {
            return Ok(Vec::new());
        }
        let mut txn = self.transaction().await?;
        let ids = txn.get_and_increment_id_by(id_type.to_string(), amount)?;
        txn.commit().await?;
        Ok(ids)
    }
}

/// Generate a diagnostic to use when connecting to persist.
fn diagnostics() -> Diagnostics {
    Diagnostics {
        shard_name: "catalog".to_string(),
        handle_purpose: "durable catalog state".to_string(),
    }
}

/// Fetch the current upper of the catalog state.
async fn current_upper(
    write_handle: &mut WriteHandle<StateUpdateKindBinary, (), Timestamp, Diff>,
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
fn as_of(
    read_handle: &ReadHandle<StateUpdateKindBinary, (), Timestamp, Diff>,
    upper: Timestamp,
) -> Timestamp {
    soft_assert!(
        upper > Timestamp::minimum(),
        "Catalog persist shard is uninitialized"
    );
    let since = read_handle.since().clone();
    let mut as_of = upper.saturating_sub(1);
    as_of.advance_by(since.borrow());
    as_of
}

/// Appends `updates` to the catalog state and downgrades the catalog's upper to `next_upper`
/// iff the current global upper of the catalog is `current_upper`.
async fn compare_and_append<T: IntoStateUpdateKindBinary>(
    write_handle: &mut WriteHandle<StateUpdateKindBinary, (), Timestamp, Diff>,
    updates: Vec<StateUpdate<T>>,
    current_upper: Timestamp,
    next_upper: Timestamp,
) -> Result<(), CatalogError> {
    let updates = updates
        .into_iter()
        .map(|update| ((update.kind.into(), ()), update.ts, update.diff));
    write_handle
        .compare_and_append(
            updates,
            Antichain::from_elem(current_upper),
            Antichain::from_elem(next_upper),
        )
        .await
        .expect("invalid usage")
        .map_err(|upper_mismatch| {
            DurableCatalogError::Fence(format!(
                "current catalog upper {:?} fenced by new catalog upper {:?}",
                upper_mismatch.expected, upper_mismatch.current
            ))
            .into()
        })
}

/// Generates an iterator of [`StateUpdate`] that contain all updates to the catalog
/// state up to, and including, `as_of`.
///
/// The output is consolidated and sorted by timestamp in ascending order.
#[tracing::instrument(level = "debug", skip(read_handle, metrics))]
async fn snapshot(
    read_handle: &mut ReadHandle<StateUpdateKindBinary, (), Timestamp, Diff>,
    as_of: Timestamp,
    metrics: &Arc<Metrics>,
) -> impl Iterator<Item = StateUpdate> + DoubleEndedIterator {
    metrics.snapshots_taken.inc();
    let counter = metrics.snapshot_latency_duration_seconds.clone();
    snapshot_inner(read_handle, as_of)
        .wall_time()
        .inc_by(counter)
        .await
}

#[tracing::instrument(level = "debug", skip(read_handle))]
async fn snapshot_inner(
    read_handle: &mut ReadHandle<StateUpdateKindBinary, (), Timestamp, Diff>,
    as_of: Timestamp,
) -> impl Iterator<Item = StateUpdate<StateUpdateKind>> + DoubleEndedIterator {
    snapshot_binary(read_handle, as_of)
        .await
        .map(|update| StateUpdate {
            kind: update.kind.try_into().expect("kind decoding error"),
            ts: update.ts,
            diff: update.diff,
        })
}

/// Generates an iterator of [`StateUpdate`] that contain all updates to the catalog
/// state up to, and including, `as_of`.
///
/// The output is consolidated and sorted by timestamp in ascending order.
#[tracing::instrument(level = "debug", skip(read_handle))]
async fn snapshot_binary(
    read_handle: &mut ReadHandle<StateUpdateKindBinary, (), Timestamp, Diff>,
    as_of: Timestamp,
) -> impl Iterator<Item = StateUpdate<StateUpdateKindBinary>> + DoubleEndedIterator {
    let snapshot = read_handle
        .snapshot_and_fetch(Antichain::from_elem(as_of))
        .await
        .expect("we have advanced the restart_as_of by the since");
    soft_assert!(
        snapshot.iter().all(|(_, _, diff)| *diff == 1),
        "snapshot_and_fetch guarantees a consolidated result: {snapshot:?}"
    );
    snapshot
        .into_iter()
        .map(|((kind, _unit), ts, diff)| StateUpdate {
            kind: kind.expect("kind decoding error"),
            ts,
            diff,
        })
        .sorted_by(|a, b| Ord::cmp(&b.ts, &a.ts))
}

// Debug methods.
impl Trace {
    /// Generates a [`Trace`] from snapshot.
    fn from_snapshot(snapshot: impl IntoIterator<Item = StateUpdate>) -> Trace {
        let mut trace = Trace::new();
        for StateUpdate { kind, ts, diff } in snapshot {
            match kind {
                StateUpdateKind::AuditLog(k, v) => {
                    trace.audit_log.values.push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::Cluster(k, v) => {
                    trace.clusters.values.push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::ClusterReplica(k, v) => {
                    trace
                        .cluster_replicas
                        .values
                        .push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::Comment(k, v) => {
                    trace.comments.values.push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::Config(k, v) => {
                    trace.configs.values.push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::Database(k, v) => {
                    trace.databases.values.push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::DefaultPrivilege(k, v) => {
                    trace
                        .default_privileges
                        .values
                        .push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::Epoch(_) => {
                    // Epoch not included in trace.
                }
                StateUpdateKind::IdAllocator(k, v) => {
                    trace
                        .id_allocator
                        .values
                        .push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::IntrospectionSourceIndex(k, v) => trace
                    .introspection_sources
                    .values
                    .push(((k, v), ts.to_string(), diff)),
                StateUpdateKind::Item(k, v) => {
                    trace.items.values.push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::Role(k, v) => {
                    trace.roles.values.push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::Schema(k, v) => {
                    trace.schemas.values.push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::Setting(k, v) => {
                    trace.settings.values.push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::StorageUsage(k, v) => {
                    trace
                        .storage_usage
                        .values
                        .push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::SystemConfiguration(k, v) => trace
                    .system_configurations
                    .values
                    .push(((k, v), ts.to_string(), diff)),
                StateUpdateKind::SystemObjectMapping(k, v) => trace
                    .system_object_mappings
                    .values
                    .push(((k, v), ts.to_string(), diff)),
                StateUpdateKind::SystemPrivilege(k, v) => {
                    trace
                        .system_privileges
                        .values
                        .push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::Timestamp(k, v) => {
                    trace.timestamps.values.push(((k, v), ts.to_string(), diff))
                }
            }
        }
        trace
    }
}

impl PersistHandle {
    /// Manually update value of `key` in collection `T` to `value`.
    #[tracing::instrument(level = "info", skip(self))]
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

    #[tracing::instrument(level = "info", skip(self))]
    pub(crate) async fn debug_edit_inner<T: Collection>(
        &mut self,
        key: T::Key,
        value: T::Value,
    ) -> Result<Option<T::Value>, CatalogError>
    where
        T::Key: PartialEq + Eq + Debug + Clone,
        T::Value: Debug + Clone,
    {
        let (snapshot, current_upper) = self.current_snapshot().await;
        let next_upper = current_upper.step_forward();
        let trace = Trace::from_snapshot(snapshot.clone());
        let collection_trace = T::collection_trace(trace);
        let prev_values: Vec<_> = collection_trace
            .values
            .into_iter()
            .filter(|((k, _), _, diff)| {
                soft_assert_eq!(*diff, 1, "trace is consolidated");
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
            .map(|((k, v), _, _)| StateUpdate {
                kind: T::persist_update(k, v),
                ts: current_upper,
                diff: -1,
            })
            .collect();
        updates.push(StateUpdate {
            kind: T::persist_update(key, value),
            ts: current_upper,
            diff: 1,
        });
        self.compare_and_append(updates, current_upper, next_upper)
            .await?;
        Ok(prev_value)
    }

    /// Manually delete `key` from collection `T`.
    #[tracing::instrument(level = "info", skip(self))]
    pub(crate) async fn debug_delete<T: Collection>(
        &mut self,
        key: T::Key,
    ) -> Result<(), CatalogError>
    where
        T::Key: PartialEq + Eq + Debug + Clone,
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
    #[tracing::instrument(level = "info", skip(self))]
    async fn debug_delete_inner<T: Collection>(&mut self, key: T::Key) -> Result<(), CatalogError>
    where
        T::Key: PartialEq + Eq + Debug,
    {
        let (snapshot, current_upper) = self.current_snapshot().await;
        let next_upper = current_upper.step_forward();
        let trace = Trace::from_snapshot(snapshot.clone());
        let collection_trace = T::collection_trace(trace);
        let retractions = collection_trace
            .values
            .into_iter()
            .filter(|((k, _), _, diff)| {
                soft_assert_eq!(*diff, 1, "trace is consolidated");
                &key == k
            })
            .map(|((k, v), _, _)| StateUpdate {
                kind: T::persist_update(k, v),
                ts: current_upper,
                diff: -1,
            })
            .collect();
        self.compare_and_append(retractions, current_upper, next_upper)
            .await?;
        Ok(())
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
