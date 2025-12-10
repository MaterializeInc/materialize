// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An abstraction for dealing with storage collections.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::num::NonZeroI64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use differential_dataflow::lattice::Lattice;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{Future, FutureExt, StreamExt};
use itertools::Itertools;

use mz_ore::collections::CollectionExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::task::AbortOnDropHandle;
use mz_ore::{assert_none, instrument, soft_assert_or_log};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::USE_CRITICAL_SINCE_SNAPSHOT;
use mz_persist_client::critical::SinceHandle;
use mz_persist_client::read::{Cursor, ReadHandle};
use mz_persist_client::schema::CaESchema;
use mz_persist_client::stats::{SnapshotPartsStats, SnapshotStats};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_types::Codec64;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::txn::TxnsCodec;
use mz_repr::{GlobalId, RelationDesc, RelationVersion, Row, TimestampManipulation};
use mz_storage_types::StorageDiff;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::connections::inline::InlinedConnection;
use mz_storage_types::controller::{CollectionMetadata, StorageError, TxnsCodecRow};
use mz_storage_types::dyncfgs::STORAGE_DOWNGRADE_SINCE_DURING_FINALIZATION;
use mz_storage_types::errors::CollectionMissing;
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::read_holds::ReadHold;
use mz_storage_types::read_policy::ReadPolicy;
use mz_storage_types::sources::{
    GenericSourceConnection, SourceData, SourceDesc, SourceEnvelope, SourceExport,
    SourceExportDataConfig, Timeline,
};
use mz_storage_types::time_dependence::{TimeDependence, TimeDependenceError};
use mz_txn_wal::metrics::Metrics as TxnMetrics;
use mz_txn_wal::txn_read::{DataSnapshot, TxnsRead};
use mz_txn_wal::txns::TxnsHandle;
use timely::PartialOrder;
use timely::order::TotalOrder;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, ChangeBatch, Timestamp as TimelyTimestamp};
use tokio::sync::{mpsc, oneshot};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, trace, warn};

use crate::client::TimestamplessUpdateBuilder;
use crate::controller::{
    CollectionDescription, DataSource, PersistEpoch, StorageMetadata, StorageTxn,
};
use crate::storage_collections::metrics::{ShardIdSet, StorageCollectionsMetrics};

mod metrics;

/// An abstraction for keeping track of storage collections and managing access
/// to them.
///
/// Responsibilities:
///
/// - Keeps a critical persist handle for holding the since of collections
///   where it need to be.
///
/// - Drives the since forward based on the upper of a collection and a
///   [ReadPolicy].
///
/// - Hands out [ReadHolds](ReadHold) that prevent a collection's since from
/// advancing while it needs to be read at a specific time.
#[async_trait]
pub trait StorageCollections: Debug + Sync {
    type Timestamp: TimelyTimestamp;

    /// On boot, reconcile this [StorageCollections] with outside state. We get
    /// a [StorageTxn] where we can record any durable state that we need.
    ///
    /// We get `init_ids`, which tells us about all collections that currently
    /// exist, so that we can record durable state for those that _we_ don't
    /// know yet about.
    async fn initialize_state(
        &self,
        txn: &mut (dyn StorageTxn<Self::Timestamp> + Send),
        init_ids: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Update storage configuration with new parameters.
    fn update_parameters(&self, config_params: StorageParameters);

    /// Returns the [CollectionMetadata] of the collection identified by `id`.
    fn collection_metadata(&self, id: GlobalId) -> Result<CollectionMetadata, CollectionMissing>;

    /// Acquire an iterator over [CollectionMetadata] for all active
    /// collections.
    ///
    /// A collection is "active" when it has a non empty frontier of read
    /// capabilties.
    fn active_collection_metadatas(&self) -> Vec<(GlobalId, CollectionMetadata)>;

    /// Returns the frontiers of the identified collection.
    fn collection_frontiers(
        &self,
        id: GlobalId,
    ) -> Result<CollectionFrontiers<Self::Timestamp>, CollectionMissing> {
        let frontiers = self
            .collections_frontiers(vec![id])?
            .expect_element(|| "known to exist");

        Ok(frontiers)
    }

    /// Atomically gets and returns the frontiers of all the identified
    /// collections.
    fn collections_frontiers(
        &self,
        id: Vec<GlobalId>,
    ) -> Result<Vec<CollectionFrontiers<Self::Timestamp>>, CollectionMissing>;

    /// Atomically gets and returns the frontiers of all active collections.
    ///
    /// A collection is "active" when it has a non-empty frontier of read
    /// capabilities.
    fn active_collection_frontiers(&self) -> Vec<CollectionFrontiers<Self::Timestamp>>;

    /// Checks whether a collection exists under the given `GlobalId`. Returns
    /// an error if the collection does not exist.
    fn check_exists(&self, id: GlobalId) -> Result<(), StorageError<Self::Timestamp>>;

    /// Returns aggregate statistics about the contents of the local input named
    /// `id` at `as_of`.
    async fn snapshot_stats(
        &self,
        id: GlobalId,
        as_of: Antichain<Self::Timestamp>,
    ) -> Result<SnapshotStats, StorageError<Self::Timestamp>>;

    /// Returns aggregate statistics about the contents of the local input named
    /// `id` at `as_of`.
    ///
    /// Note that this async function itself returns a future. We may
    /// need to block on the stats being available, but don't want to hold a reference
    /// to the controller for too long... so the outer future holds a reference to the
    /// controller but returns quickly, and the inner future is slow but does not
    /// reference the controller.
    async fn snapshot_parts_stats(
        &self,
        id: GlobalId,
        as_of: Antichain<Self::Timestamp>,
    ) -> BoxFuture<'static, Result<SnapshotPartsStats, StorageError<Self::Timestamp>>>;

    /// Returns a snapshot of the contents of collection `id` at `as_of`.
    fn snapshot(
        &self,
        id: GlobalId,
        as_of: Self::Timestamp,
    ) -> BoxFuture<'static, Result<Vec<(Row, StorageDiff)>, StorageError<Self::Timestamp>>>;

    /// Returns a snapshot of the contents of collection `id` at the largest
    /// readable `as_of`.
    async fn snapshot_latest(
        &self,
        id: GlobalId,
    ) -> Result<Vec<Row>, StorageError<Self::Timestamp>>;

    /// Returns a snapshot of the contents of collection `id` at `as_of`.
    fn snapshot_cursor(
        &self,
        id: GlobalId,
        as_of: Self::Timestamp,
    ) -> BoxFuture<'static, Result<SnapshotCursor<Self::Timestamp>, StorageError<Self::Timestamp>>>
    where
        Self::Timestamp: Codec64 + TimelyTimestamp + Lattice;

    /// Generates a snapshot of the contents of collection `id` at `as_of` and
    /// streams out all of the updates in bounded memory.
    ///
    /// The output is __not__ consolidated.
    fn snapshot_and_stream(
        &self,
        id: GlobalId,
        as_of: Self::Timestamp,
    ) -> BoxFuture<
        'static,
        Result<
            BoxStream<'static, (SourceData, Self::Timestamp, StorageDiff)>,
            StorageError<Self::Timestamp>,
        >,
    >;

    /// Create a [`TimestamplessUpdateBuilder`] that can be used to stage
    /// updates for the provided [`GlobalId`].
    fn create_update_builder(
        &self,
        id: GlobalId,
    ) -> BoxFuture<
        'static,
        Result<
            TimestamplessUpdateBuilder<SourceData, (), Self::Timestamp, StorageDiff>,
            StorageError<Self::Timestamp>,
        >,
    >
    where
        Self::Timestamp: Lattice + Codec64;

    /// Update the given [`StorageTxn`] with the appropriate metadata given the
    /// IDs to add and drop.
    ///
    /// The data modified in the `StorageTxn` must be made available in all
    /// subsequent calls that require [`StorageMetadata`] as a parameter.
    async fn prepare_state(
        &self,
        txn: &mut (dyn StorageTxn<Self::Timestamp> + Send),
        ids_to_add: BTreeSet<GlobalId>,
        ids_to_drop: BTreeSet<GlobalId>,
        ids_to_register: BTreeMap<GlobalId, ShardId>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Create the collections described by the individual
    /// [CollectionDescriptions](CollectionDescription).
    ///
    /// Each command carries the source id, the source description, and any
    /// associated metadata needed to ingest the particular source.
    ///
    /// This command installs collection state for the indicated sources, and
    /// they are now valid to use in queries at times beyond the initial `since`
    /// frontiers. Each collection also acquires a read capability at this
    /// frontier, which will need to be repeatedly downgraded with
    /// `allow_compaction()` to permit compaction.
    ///
    /// This method is NOT idempotent; It can fail between processing of
    /// different collections and leave the [StorageCollections] in an
    /// inconsistent state. It is almost always wrong to do anything but abort
    /// the process on `Err`.
    ///
    /// The `register_ts` is used as the initial timestamp that tables are
    /// available for reads. (We might later give non-tables the same treatment,
    /// but hold off on that initially.) Callers must provide a Some if any of
    /// the collections is a table. A None may be given if none of the
    /// collections are a table (i.e. all materialized views, sources, etc).
    ///
    /// `migrated_storage_collections` is a set of migrated storage collections to be excluded
    /// from the txn-wal sub-system.
    async fn create_collections_for_bootstrap(
        &self,
        storage_metadata: &StorageMetadata,
        register_ts: Option<Self::Timestamp>,
        collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
        migrated_storage_collections: &BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Alters the identified ingestion to use the provided [`SourceDesc`].
    ///
    /// NOTE: Ideally, [StorageCollections] would not care about these, but we
    /// have to learn about changes such that when new subsources are created we
    /// can correctly determine a since based on its depenencies' sinces. This
    /// is really only relevant because newly created subsources depend on the
    /// remap shard, and we can't just have them start at since 0.
    async fn alter_ingestion_source_desc(
        &self,
        ingestion_id: GlobalId,
        source_desc: SourceDesc,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Alters the data config for the specified source exports of the specified ingestions.
    async fn alter_ingestion_export_data_configs(
        &self,
        source_exports: BTreeMap<GlobalId, SourceExportDataConfig>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Alters each identified collection to use the correlated
    /// [`GenericSourceConnection`].
    ///
    /// See NOTE on [StorageCollections::alter_ingestion_source_desc].
    async fn alter_ingestion_connections(
        &self,
        source_connections: BTreeMap<GlobalId, GenericSourceConnection<InlinedConnection>>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Updates the [`RelationDesc`] for the specified table.
    async fn alter_table_desc(
        &self,
        existing_collection: GlobalId,
        new_collection: GlobalId,
        new_desc: RelationDesc,
        expected_version: RelationVersion,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Drops the read capability for the sources and allows their resources to
    /// be reclaimed.
    ///
    /// TODO(jkosh44): This method does not validate the provided identifiers.
    /// Currently when the controller starts/restarts it has no durable state.
    /// That means that it has no way of remembering any past commands sent. In
    /// the future we plan on persisting state for the controller so that it is
    /// aware of past commands. Therefore this method is for dropping sources
    /// that we know to have been previously created, but have been forgotten by
    /// the controller due to a restart. Once command history becomes durable we
    /// can remove this method and use the normal `drop_sources`.
    fn drop_collections_unvalidated(
        &self,
        storage_metadata: &StorageMetadata,
        identifiers: Vec<GlobalId>,
    );

    /// Assigns a read policy to specific identifiers.
    ///
    /// The policies are assigned in the order presented, and repeated
    /// identifiers should conclude with the last policy. Changing a policy will
    /// immediately downgrade the read capability if appropriate, but it will
    /// not "recover" the read capability if the prior capability is already
    /// ahead of it.
    ///
    /// This [StorageCollections] may include its own overrides on these
    /// policies.
    ///
    /// Identifiers not present in `policies` retain their existing read
    /// policies.
    fn set_read_policies(&self, policies: Vec<(GlobalId, ReadPolicy<Self::Timestamp>)>);

    /// Acquires and returns the earliest possible read holds for the specified
    /// collections.
    fn acquire_read_holds(
        &self,
        desired_holds: Vec<GlobalId>,
    ) -> Result<Vec<ReadHold<Self::Timestamp>>, CollectionMissing>;

    /// Get the time dependence for a storage collection. Returns no value if unknown or if
    /// the object isn't managed by storage.
    fn determine_time_dependence(
        &self,
        id: GlobalId,
    ) -> Result<Option<TimeDependence>, TimeDependenceError>;

    /// Returns the state of [`StorageCollections`] formatted as JSON.
    fn dump(&self) -> Result<serde_json::Value, anyhow::Error>;
}

/// A cursor over a snapshot, allowing us to read just part of a snapshot in its
/// consolidated form.
pub struct SnapshotCursor<T: Codec64 + TimelyTimestamp + Lattice> {
    // We allocate a temporary read handle for each snapshot, and that handle needs to live at
    // least as long as the cursor itself, which holds part leases. Bundling them together!
    pub _read_handle: ReadHandle<SourceData, (), T, StorageDiff>,
    pub cursor: Cursor<SourceData, (), T, StorageDiff>,
}

impl<T: Codec64 + TimelyTimestamp + Lattice + Sync> SnapshotCursor<T> {
    pub async fn next(
        &mut self,
    ) -> Option<
        impl Iterator<
            Item = (
                (Result<SourceData, String>, Result<(), String>),
                T,
                StorageDiff,
            ),
        > + Sized
        + '_,
    > {
        self.cursor.next().await
    }
}

/// Frontiers of the collection identified by `id`.
#[derive(Debug)]
pub struct CollectionFrontiers<T> {
    /// The [GlobalId] of the collection that these frontiers belong to.
    pub id: GlobalId,

    /// The upper/write frontier of the collection.
    pub write_frontier: Antichain<T>,

    /// The since frontier that is implied by the collection's existence,
    /// disregarding any read holds.
    ///
    /// Concretely, it is the since frontier that is implied by the combination
    /// of the `write_frontier` and a [ReadPolicy]. The implied capability is
    /// derived from the write frontier using the [ReadPolicy].
    pub implied_capability: Antichain<T>,

    /// The frontier of all oustanding [ReadHolds](ReadHold). This includes the
    /// implied capability.
    pub read_capabilities: Antichain<T>,
}

/// Implementation of [StorageCollections] that is shallow-cloneable and uses a
/// background task for doing work concurrently, in the background.
#[derive(Debug, Clone)]
pub struct StorageCollectionsImpl<
    T: TimelyTimestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
> {
    /// The fencing token for this instance of [StorageCollections], and really
    /// all of the controllers and Coordinator.
    envd_epoch: NonZeroI64,

    /// Whether or not this [StorageCollections] is in read-only mode.
    ///
    /// When in read-only mode, we are not allowed to affect changes to external
    /// systems, including, for example, acquiring and downgrading critical
    /// [SinceHandles](SinceHandle)
    read_only: bool,

    /// The set of [ShardIds](ShardId) that we have to finalize. These will have
    /// been persisted by the caller of [StorageCollections::prepare_state].
    finalizable_shards: Arc<ShardIdSet>,

    /// The set of [ShardIds](ShardId) that we have finalized. We keep track of
    /// shards here until we are given a chance to let our callers know that
    /// these have been finalized, for example via
    /// [StorageCollections::prepare_state].
    finalized_shards: Arc<ShardIdSet>,

    /// Collections maintained by this [StorageCollections].
    collections: Arc<std::sync::Mutex<BTreeMap<GlobalId, CollectionState<T>>>>,

    /// A shared TxnsCache running in a task and communicated with over a channel.
    txns_read: TxnsRead<T>,

    /// Storage configuration parameters.
    config: Arc<Mutex<StorageConfiguration>>,

    /// The upper of the txn shard as it was when we booted. We forward the
    /// upper of created/registered tables to make sure that their uppers are at
    /// least not less than the initially known txn upper.
    ///
    /// NOTE: This works around a quirk in how the adapter chooses the as_of of
    /// existing indexes when bootstrapping, where tables that have an upper
    /// that is less than the initially known txn upper can lead to indexes that
    /// cannot hydrate in read-only mode.
    initial_txn_upper: Antichain<T>,

    /// The persist location where all storage collections are being written to
    persist_location: PersistLocation,

    /// A persist client used to write to storage collections
    persist: Arc<PersistClientCache>,

    /// For sending commands to our internal task.
    cmd_tx: mpsc::UnboundedSender<BackgroundCmd<T>>,

    /// For sending updates about read holds to our internal task.
    holds_tx: mpsc::UnboundedSender<(GlobalId, ChangeBatch<T>)>,

    /// Handles to tasks we own, making sure they're dropped when we are.
    _background_task: Arc<AbortOnDropHandle<()>>,
    _finalize_shards_task: Arc<AbortOnDropHandle<()>>,
}

// Supporting methods for implementing [StorageCollections].
//
// Almost all internal methods that are the backing implementation for a trait
// method have the `_inner` suffix.
//
// We follow a pattern where `_inner` methods get a mutable reference to the
// shared collections state, and it's the public-facing method that locks the
// state for the duration of its invocation. This allows calling other `_inner`
// methods from within `_inner` methods.
impl<T> StorageCollectionsImpl<T>
where
    T: TimelyTimestamp
        + Lattice
        + Codec64
        + From<EpochMillis>
        + TimestampManipulation
        + Into<mz_repr::Timestamp>
        + Sync,
{
    /// Creates and returns a new [StorageCollections].
    ///
    /// Note that when creating a new [StorageCollections], you must also
    /// reconcile it with the previous state using
    /// [StorageCollections::initialize_state],
    /// [StorageCollections::prepare_state], and
    /// [StorageCollections::create_collections_for_bootstrap].
    pub async fn new(
        persist_location: PersistLocation,
        persist_clients: Arc<PersistClientCache>,
        metrics_registry: &MetricsRegistry,
        _now: NowFn,
        txns_metrics: Arc<TxnMetrics>,
        envd_epoch: NonZeroI64,
        read_only: bool,
        connection_context: ConnectionContext,
        txn: &dyn StorageTxn<T>,
    ) -> Self {
        let metrics = StorageCollectionsMetrics::register_into(metrics_registry);

        // This value must be already installed because we must ensure it's
        // durably recorded before it is used, otherwise we risk leaking persist
        // state.
        let txns_id = txn
            .get_txn_wal_shard()
            .expect("must call prepare initialization before creating StorageCollections");

        let txns_client = persist_clients
            .open(persist_location.clone())
            .await
            .expect("location should be valid");

        // We have to initialize, so that TxnsRead::start() below does not
        // block.
        let _txns_handle: TxnsHandle<SourceData, (), T, StorageDiff, PersistEpoch, TxnsCodecRow> =
            TxnsHandle::open(
                T::minimum(),
                txns_client.clone(),
                txns_client.dyncfgs().clone(),
                Arc::clone(&txns_metrics),
                txns_id,
            )
            .await;

        // For handing to the background task, for listening to upper updates.
        let (txns_key_schema, txns_val_schema) = TxnsCodecRow::schemas();
        let mut txns_write = txns_client
            .open_writer(
                txns_id,
                Arc::new(txns_key_schema),
                Arc::new(txns_val_schema),
                Diagnostics {
                    shard_name: "txns".to_owned(),
                    handle_purpose: "commit txns".to_owned(),
                },
            )
            .await
            .expect("txns schema shouldn't change");

        let txns_read = TxnsRead::start::<TxnsCodecRow>(txns_client.clone(), txns_id).await;

        let collections = Arc::new(std::sync::Mutex::new(BTreeMap::default()));
        let finalizable_shards =
            Arc::new(ShardIdSet::new(metrics.finalization_outstanding.clone()));
        let finalized_shards =
            Arc::new(ShardIdSet::new(metrics.finalization_pending_commit.clone()));
        let config = Arc::new(Mutex::new(StorageConfiguration::new(
            connection_context,
            mz_dyncfgs::all_dyncfgs(),
        )));

        let initial_txn_upper = txns_write.fetch_recent_upper().await.to_owned();

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (holds_tx, holds_rx) = mpsc::unbounded_channel();
        let mut background_task = BackgroundTask {
            config: Arc::clone(&config),
            cmds_tx: cmd_tx.clone(),
            cmds_rx: cmd_rx,
            holds_rx,
            collections: Arc::clone(&collections),
            finalizable_shards: Arc::clone(&finalizable_shards),
            shard_by_id: BTreeMap::new(),
            since_handles: BTreeMap::new(),
            txns_handle: Some(txns_write),
            txns_shards: Default::default(),
        };

        let background_task =
            mz_ore::task::spawn(|| "storage_collections::background_task", async move {
                background_task.run().await
            });

        let finalize_shards_task = mz_ore::task::spawn(
            || "storage_collections::finalize_shards_task",
            finalize_shards_task::<T>(FinalizeShardsTaskConfig {
                envd_epoch: envd_epoch.clone(),
                config: Arc::clone(&config),
                metrics,
                finalizable_shards: Arc::clone(&finalizable_shards),
                finalized_shards: Arc::clone(&finalized_shards),
                persist_location: persist_location.clone(),
                persist: Arc::clone(&persist_clients),
                read_only,
            }),
        );

        Self {
            finalizable_shards,
            finalized_shards,
            collections,
            txns_read,
            envd_epoch,
            read_only,
            config,
            initial_txn_upper,
            persist_location,
            persist: persist_clients,
            cmd_tx,
            holds_tx,
            _background_task: Arc::new(background_task.abort_on_drop()),
            _finalize_shards_task: Arc::new(finalize_shards_task.abort_on_drop()),
        }
    }

    /// Opens a [WriteHandle] and a [SinceHandleWrapper], for holding back the since.
    ///
    /// `since` is an optional since that the read handle will be forwarded to
    /// if it is less than its current since.
    ///
    /// This will `halt!` the process if we cannot successfully acquire a
    /// critical handle with our current epoch.
    async fn open_data_handles(
        &self,
        id: &GlobalId,
        shard: ShardId,
        since: Option<&Antichain<T>>,
        relation_desc: RelationDesc,
        persist_client: &PersistClient,
    ) -> (
        WriteHandle<SourceData, (), T, StorageDiff>,
        SinceHandleWrapper<T>,
    ) {
        let since_handle = if self.read_only {
            let read_handle = self
                .open_leased_handle(id, shard, relation_desc.clone(), since, persist_client)
                .await;
            SinceHandleWrapper::Leased(read_handle)
        } else {
            // We're managing the data for this shard in read-write mode, which would fence out other
            // processes in read-only mode; it's safe to upgrade the metadata version.
            persist_client
                .upgrade_version::<SourceData, (), T, StorageDiff>(
                    shard,
                    Diagnostics {
                        shard_name: id.to_string(),
                        handle_purpose: format!("controller data for {}", id),
                    },
                )
                .await
                .expect("invalid persist usage");

            let since_handle = self
                .open_critical_handle(id, shard, since, persist_client)
                .await;

            SinceHandleWrapper::Critical(since_handle)
        };

        let mut write_handle = self
            .open_write_handle(id, shard, relation_desc, persist_client)
            .await;

        // N.B.
        // Fetch the most recent upper for the write handle. Otherwise, this may
        // be behind the since of the since handle. Its vital this happens AFTER
        // we create the since handle as it needs to be linearized with that
        // operation. It may be true that creating the write handle after the
        // since handle already ensures this, but we do this out of an abundance
        // of caution.
        //
        // Note that this returns the upper, but also sets it on the handle to
        // be fetched later.
        write_handle.fetch_recent_upper().await;

        (write_handle, since_handle)
    }

    /// Opens a write handle for the given `shard`.
    async fn open_write_handle(
        &self,
        id: &GlobalId,
        shard: ShardId,
        relation_desc: RelationDesc,
        persist_client: &PersistClient,
    ) -> WriteHandle<SourceData, (), T, StorageDiff> {
        let diagnostics = Diagnostics {
            shard_name: id.to_string(),
            handle_purpose: format!("controller data for {}", id),
        };

        let write = persist_client
            .open_writer(
                shard,
                Arc::new(relation_desc),
                Arc::new(UnitSchema),
                diagnostics.clone(),
            )
            .await
            .expect("invalid persist usage");

        write
    }

    /// Opens a critical since handle for the given `shard`.
    ///
    /// `since` is an optional since that the read handle will be forwarded to
    /// if it is less than its current since.
    ///
    /// This will `halt!` the process if we cannot successfully acquire a
    /// critical handle with our current epoch.
    async fn open_critical_handle(
        &self,
        id: &GlobalId,
        shard: ShardId,
        since: Option<&Antichain<T>>,
        persist_client: &PersistClient,
    ) -> SinceHandle<SourceData, (), T, StorageDiff, PersistEpoch> {
        tracing::debug!(%id, ?since, "opening critical handle");

        assert!(
            !self.read_only,
            "attempting to open critical SinceHandle in read-only mode"
        );

        let diagnostics = Diagnostics {
            shard_name: id.to_string(),
            handle_purpose: format!("controller data for {}", id),
        };

        // Construct the handle in a separate block to ensure all error paths
        // are diverging
        let since_handle = {
            // This block's aim is to ensure the handle is in terms of our epoch
            // by the time we return it.
            let mut handle: SinceHandle<_, _, _, _, PersistEpoch> = persist_client
                .open_critical_since(
                    shard,
                    PersistClient::CONTROLLER_CRITICAL_SINCE,
                    diagnostics.clone(),
                )
                .await
                .expect("invalid persist usage");

            // Take the join of the handle's since and the provided `since`;
            // this lets materialized views express the since at which their
            // read handles "start."
            let provided_since = match since {
                Some(since) => since,
                None => &Antichain::from_elem(T::minimum()),
            };
            let since = handle.since().join(provided_since);

            let our_epoch = self.envd_epoch;

            loop {
                let current_epoch: PersistEpoch = handle.opaque().clone();

                // Ensure the current epoch is <= our epoch.
                let unchecked_success = current_epoch.0.map(|e| e <= our_epoch).unwrap_or(true);

                if unchecked_success {
                    // Update the handle's state so that it is in terms of our
                    // epoch.
                    let checked_success = handle
                        .compare_and_downgrade_since(
                            &current_epoch,
                            (&PersistEpoch::from(our_epoch), &since),
                        )
                        .await
                        .is_ok();
                    if checked_success {
                        break handle;
                    }
                } else {
                    mz_ore::halt!("fenced by envd @ {current_epoch:?}. ours = {our_epoch}");
                }
            }
        };

        since_handle
    }

    /// Opens a leased [ReadHandle], for the purpose of holding back a since,
    /// for the given `shard`.
    ///
    /// `since` is an optional since that the read handle will be forwarded to
    /// if it is less than its current since.
    async fn open_leased_handle(
        &self,
        id: &GlobalId,
        shard: ShardId,
        relation_desc: RelationDesc,
        since: Option<&Antichain<T>>,
        persist_client: &PersistClient,
    ) -> ReadHandle<SourceData, (), T, StorageDiff> {
        tracing::debug!(%id, ?since, "opening leased handle");

        let diagnostics = Diagnostics {
            shard_name: id.to_string(),
            handle_purpose: format!("controller data for {}", id),
        };

        let use_critical_since = false;
        let mut handle: ReadHandle<_, _, _, _> = persist_client
            .open_leased_reader(
                shard,
                Arc::new(relation_desc),
                Arc::new(UnitSchema),
                diagnostics.clone(),
                use_critical_since,
            )
            .await
            .expect("invalid persist usage");

        // Take the join of the handle's since and the provided `since`;
        // this lets materialized views express the since at which their
        // read handles "start."
        let provided_since = match since {
            Some(since) => since,
            None => &Antichain::from_elem(T::minimum()),
        };
        let since = handle.since().join(provided_since);

        handle.downgrade_since(&since).await;

        handle
    }

    fn register_handles(
        &self,
        id: GlobalId,
        is_in_txns: bool,
        since_handle: SinceHandleWrapper<T>,
        write_handle: WriteHandle<SourceData, (), T, StorageDiff>,
    ) {
        self.send(BackgroundCmd::Register {
            id,
            is_in_txns,
            since_handle,
            write_handle,
        });
    }

    fn send(&self, cmd: BackgroundCmd<T>) {
        let _ = self.cmd_tx.send(cmd);
    }

    async fn snapshot_stats_inner(
        &self,
        id: GlobalId,
        as_of: SnapshotStatsAsOf<T>,
    ) -> Result<SnapshotStats, StorageError<T>> {
        // TODO: Pull this out of BackgroundTask. Unlike the other methods, the
        // caller of this one drives it to completion.
        //
        // We'd need to either share the critical handle somehow or maybe have
        // two instances around, one in the worker and one in the
        // StorageCollections.
        let (tx, rx) = oneshot::channel();
        self.send(BackgroundCmd::SnapshotStats(id, as_of, tx));
        rx.await.expect("BackgroundTask should be live").0.await
    }

    /// If this identified collection has a dependency, install a read hold on
    /// it.
    ///
    /// This is necessary to ensure that the dependency's since does not advance
    /// beyond its dependents'.
    fn install_collection_dependency_read_holds_inner(
        &self,
        self_collections: &mut BTreeMap<GlobalId, CollectionState<T>>,
        id: GlobalId,
    ) -> Result<(), StorageError<T>> {
        let (deps, collection_implied_capability) = match self_collections.get(&id) {
            Some(CollectionState {
                storage_dependencies: deps,
                implied_capability,
                ..
            }) => (deps.clone(), implied_capability),
            _ => return Ok(()),
        };

        for dep in deps.iter() {
            let dep_collection = self_collections
                .get(dep)
                .ok_or(StorageError::IdentifierMissing(id))?;

            mz_ore::soft_assert_or_log!(
                PartialOrder::less_equal(
                    &dep_collection.implied_capability,
                    collection_implied_capability
                ),
                "dependency since ({dep}@{:?}) cannot be in advance of dependent's since ({id}@{:?})",
                dep_collection.implied_capability,
                collection_implied_capability,
            );
        }

        self.install_read_capabilities_inner(
            self_collections,
            id,
            &deps,
            collection_implied_capability.clone(),
        )?;

        Ok(())
    }

    /// Returns the given collection's dependencies.
    fn determine_collection_dependencies(
        &self,
        self_collections: &BTreeMap<GlobalId, CollectionState<T>>,
        source_id: GlobalId,
        collection_desc: &CollectionDescription<T>,
    ) -> Result<Vec<GlobalId>, StorageError<T>> {
        let mut dependencies = Vec::new();

        if let Some(id) = collection_desc.primary {
            dependencies.push(id);
        }

        match &collection_desc.data_source {
            DataSource::Introspection(_)
            | DataSource::Webhook
            | DataSource::Table
            | DataSource::Progress
            | DataSource::Other => (),
            DataSource::IngestionExport {
                ingestion_id,
                data_config,
                ..
            } => {
                // Ingestion exports depend on their primary source's remap
                // collection, except when they use a CDCv2 envelope.
                let source = self_collections
                    .get(ingestion_id)
                    .ok_or(StorageError::IdentifierMissing(*ingestion_id))?;
                let DataSource::Ingestion(ingestion) = &source.description.data_source else {
                    panic!("SourceExport must refer to a primary source that already exists");
                };

                match data_config.envelope {
                    SourceEnvelope::CdcV2 => (),
                    _ => dependencies.push(ingestion.remap_collection_id),
                }
            }
            // Ingestions depend on their remap collection.
            DataSource::Ingestion(ingestion) => {
                if ingestion.remap_collection_id != source_id {
                    dependencies.push(ingestion.remap_collection_id);
                }
            }
            DataSource::Sink { desc } => dependencies.push(desc.sink.from),
        }

        Ok(dependencies)
    }

    /// Install read capabilities on the given `storage_dependencies`.
    #[instrument(level = "debug")]
    fn install_read_capabilities_inner(
        &self,
        self_collections: &mut BTreeMap<GlobalId, CollectionState<T>>,
        from_id: GlobalId,
        storage_dependencies: &[GlobalId],
        read_capability: Antichain<T>,
    ) -> Result<(), StorageError<T>> {
        let mut changes = ChangeBatch::new();
        for time in read_capability.iter() {
            changes.update(time.clone(), 1);
        }

        if tracing::span_enabled!(tracing::Level::TRACE) {
            // Collecting `user_capabilities` is potentially slow, thus only do it when needed.
            let user_capabilities = self_collections
                .iter_mut()
                .filter(|(id, _c)| id.is_user())
                .map(|(id, c)| {
                    let updates = c.read_capabilities.updates().cloned().collect_vec();
                    (*id, c.implied_capability.clone(), updates)
                })
                .collect_vec();

            trace!(
                %from_id,
                ?storage_dependencies,
                ?read_capability,
                ?user_capabilities,
                "install_read_capabilities_inner");
        }

        let mut storage_read_updates = storage_dependencies
            .iter()
            .map(|id| (*id, changes.clone()))
            .collect();

        StorageCollectionsImpl::update_read_capabilities_inner(
            &self.cmd_tx,
            self_collections,
            &mut storage_read_updates,
        );

        if tracing::span_enabled!(tracing::Level::TRACE) {
            // Collecting `user_capabilities` is potentially slow, thus only do it when needed.
            let user_capabilities = self_collections
                .iter_mut()
                .filter(|(id, _c)| id.is_user())
                .map(|(id, c)| {
                    let updates = c.read_capabilities.updates().cloned().collect_vec();
                    (*id, c.implied_capability.clone(), updates)
                })
                .collect_vec();

            trace!(
                %from_id,
                ?storage_dependencies,
                ?read_capability,
                ?user_capabilities,
                "after install_read_capabilities_inner!");
        }

        Ok(())
    }

    async fn recent_upper(&self, id: GlobalId) -> Result<Antichain<T>, StorageError<T>> {
        let metadata = &self.collection_metadata(id)?;
        let persist_client = self
            .persist
            .open(metadata.persist_location.clone())
            .await
            .unwrap();
        // Duplicate part of open_data_handles here because we don't need the
        // fetch_recent_upper call. The pubsub-updated shared_upper is enough.
        let diagnostics = Diagnostics {
            shard_name: id.to_string(),
            handle_purpose: format!("controller data for {}", id),
        };
        // NB: Opening a WriteHandle is cheap if it's never used in a
        // compare_and_append operation.
        let write = persist_client
            .open_writer::<SourceData, (), T, StorageDiff>(
                metadata.data_shard,
                Arc::new(metadata.relation_desc.clone()),
                Arc::new(UnitSchema),
                diagnostics.clone(),
            )
            .await
            .expect("invalid persist usage");
        Ok(write.shared_upper())
    }

    async fn read_handle_for_snapshot(
        persist: Arc<PersistClientCache>,
        metadata: &CollectionMetadata,
        id: GlobalId,
    ) -> Result<ReadHandle<SourceData, (), T, StorageDiff>, StorageError<T>> {
        let persist_client = persist
            .open(metadata.persist_location.clone())
            .await
            .unwrap();

        // We create a new read handle every time someone requests a snapshot
        // and then immediately expire it instead of keeping a read handle
        // permanently in our state to avoid having it heartbeat continually.
        // The assumption is that calls to snapshot are rare and therefore worth
        // it to always create a new handle.
        let read_handle = persist_client
            .open_leased_reader::<SourceData, (), _, _>(
                metadata.data_shard,
                Arc::new(metadata.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: id.to_string(),
                    handle_purpose: format!("snapshot {}", id),
                },
                USE_CRITICAL_SINCE_SNAPSHOT.get(&persist.cfg),
            )
            .await
            .expect("invalid persist usage");
        Ok(read_handle)
    }

    // TODO(petrosagg): This signature is not very useful in the context of partially ordered times
    // where the as_of frontier might have multiple elements. In the current form the mutually
    // incomparable updates will be accumulated together to a state of the collection that never
    // actually existed. We should include the original time in the updates advanced by the as_of
    // frontier in the result and let the caller decide what to do with the information.
    fn snapshot(
        &self,
        id: GlobalId,
        as_of: T,
        txns_read: &TxnsRead<T>,
    ) -> BoxFuture<'static, Result<Vec<(Row, StorageDiff)>, StorageError<T>>>
    where
        T: Codec64 + From<EpochMillis> + TimestampManipulation,
    {
        let metadata = match self.collection_metadata(id) {
            Ok(metadata) => metadata.clone(),
            Err(e) => return async { Err(e.into()) }.boxed(),
        };
        let txns_read = metadata.txns_shard.as_ref().map(|txns_id| {
            assert_eq!(txns_id, txns_read.txns_id());
            txns_read.clone()
        });
        let persist = Arc::clone(&self.persist);
        async move {
            let mut read_handle = Self::read_handle_for_snapshot(persist, &metadata, id).await?;
            let contents = match txns_read {
                None => {
                    // We're not using txn-wal for tables, so we can take a snapshot directly.
                    read_handle
                        .snapshot_and_fetch(Antichain::from_elem(as_of))
                        .await
                }
                Some(txns_read) => {
                    // We _are_ using txn-wal for tables. It advances the physical upper of the
                    // shard lazily, so we need to ask it for the snapshot to ensure the read is
                    // unblocked.
                    //
                    // Consider the following scenario:
                    // - Table A is written to via txns at time 5
                    // - Tables other than A are written to via txns consuming timestamps up to 10
                    // - We'd like to read A at 7
                    // - The application process of A's txn has advanced the upper to 5+1, but we need
                    //   it to be past 7, but the txns shard knows that (5,10) is empty of writes to A
                    // - This branch allows it to handle that advancing the physical upper of Table A to
                    //   10 (NB but only once we see it get past the write at 5!)
                    // - Then we can read it normally.
                    txns_read.update_gt(as_of.clone()).await;
                    let data_snapshot = txns_read
                        .data_snapshot(metadata.data_shard, as_of.clone())
                        .await;
                    data_snapshot.snapshot_and_fetch(&mut read_handle).await
                }
            };
            match contents {
                Ok(contents) => {
                    let mut snapshot = Vec::with_capacity(contents.len());
                    for ((data, _), _, diff) in contents {
                        // TODO(petrosagg): We should accumulate the errors too and let the user
                        // interprret the result
                        let row = data.expect("invalid protobuf data").0?;
                        snapshot.push((row, diff));
                    }
                    Ok(snapshot)
                }
                Err(_) => Err(StorageError::ReadBeforeSince(id)),
            }
        }
        .boxed()
    }

    fn snapshot_and_stream(
        &self,
        id: GlobalId,
        as_of: T,
        txns_read: &TxnsRead<T>,
    ) -> BoxFuture<'static, Result<BoxStream<'static, (SourceData, T, StorageDiff)>, StorageError<T>>>
    {
        use futures::stream::StreamExt;

        let metadata = match self.collection_metadata(id) {
            Ok(metadata) => metadata.clone(),
            Err(e) => return async { Err(e.into()) }.boxed(),
        };
        let txns_read = metadata.txns_shard.as_ref().map(|txns_id| {
            assert_eq!(txns_id, txns_read.txns_id());
            txns_read.clone()
        });
        let persist = Arc::clone(&self.persist);

        async move {
            let mut read_handle = Self::read_handle_for_snapshot(persist, &metadata, id).await?;
            let stream = match txns_read {
                None => {
                    // We're not using txn-wal for tables, so we can take a snapshot directly.
                    read_handle
                        .snapshot_and_stream(Antichain::from_elem(as_of))
                        .await
                        .map_err(|_| StorageError::ReadBeforeSince(id))?
                        .boxed()
                }
                Some(txns_read) => {
                    txns_read.update_gt(as_of.clone()).await;
                    let data_snapshot = txns_read
                        .data_snapshot(metadata.data_shard, as_of.clone())
                        .await;
                    data_snapshot
                        .snapshot_and_stream(&mut read_handle)
                        .await
                        .map_err(|_| StorageError::ReadBeforeSince(id))?
                        .boxed()
                }
            };

            // Map our stream, unwrapping Persist internal errors.
            let stream = stream
                .map(|((k, _v), t, d)| {
                    // TODO(parkmycar): We should accumulate the errors and pass them on to the
                    // caller.
                    let data = k.expect("error while streaming from Persist");
                    (data, t, d)
                })
                .boxed();
            Ok(stream)
        }
        .boxed()
    }

    fn set_read_policies_inner(
        &self,
        collections: &mut BTreeMap<GlobalId, CollectionState<T>>,
        policies: Vec<(GlobalId, ReadPolicy<T>)>,
    ) {
        trace!("set_read_policies: {:?}", policies);

        let mut read_capability_changes = BTreeMap::default();

        for (id, policy) in policies.into_iter() {
            let collection = match collections.get_mut(&id) {
                Some(c) => c,
                None => {
                    panic!("Reference to absent collection {id}");
                }
            };

            let mut new_read_capability = policy.frontier(collection.write_frontier.borrow());

            if PartialOrder::less_equal(&collection.implied_capability, &new_read_capability) {
                let mut update = ChangeBatch::new();
                update.extend(new_read_capability.iter().map(|time| (time.clone(), 1)));
                std::mem::swap(&mut collection.implied_capability, &mut new_read_capability);
                update.extend(new_read_capability.iter().map(|time| (time.clone(), -1)));
                if !update.is_empty() {
                    read_capability_changes.insert(id, update);
                }
            }

            collection.read_policy = policy;
        }

        for (id, changes) in read_capability_changes.iter() {
            if id.is_user() {
                trace!(%id, ?changes, "in set_read_policies, capability changes");
            }
        }

        if !read_capability_changes.is_empty() {
            StorageCollectionsImpl::update_read_capabilities_inner(
                &self.cmd_tx,
                collections,
                &mut read_capability_changes,
            );
        }
    }

    // This is not an associated function so that we can share it with the task
    // that updates the persist handles and also has a reference to the shared
    // collections state.
    fn update_read_capabilities_inner(
        cmd_tx: &mpsc::UnboundedSender<BackgroundCmd<T>>,
        collections: &mut BTreeMap<GlobalId, CollectionState<T>>,
        updates: &mut BTreeMap<GlobalId, ChangeBatch<T>>,
    ) {
        // Location to record consequences that we need to act on.
        let mut collections_net = BTreeMap::new();

        // We must not rely on any specific relative ordering of `GlobalId`s.
        // That said, it is reasonable to assume that collections generally have
        // greater IDs than their dependencies, so starting with the largest is
        // a useful optimization.
        while let Some(id) = updates.keys().rev().next().cloned() {
            let mut update = updates.remove(&id).unwrap();

            if id.is_user() {
                trace!(id = ?id, update = ?update, "update_read_capabilities");
            }

            let collection = if let Some(c) = collections.get_mut(&id) {
                c
            } else {
                let has_positive_updates = update.iter().any(|(_ts, diff)| *diff > 0);
                if has_positive_updates {
                    panic!(
                        "reference to absent collection {id} but we have positive updates: {:?}",
                        update
                    );
                } else {
                    // Continue purely negative updates. Someone has probably
                    // already dropped this collection!
                    continue;
                }
            };

            let current_read_capabilities = collection.read_capabilities.frontier().to_owned();
            for (time, diff) in update.iter() {
                assert!(
                    collection.read_capabilities.count_for(time) + diff >= 0,
                    "update {:?} for collection {id} would lead to negative \
                        read capabilities, read capabilities before applying: {:?}",
                    update,
                    collection.read_capabilities
                );

                if collection.read_capabilities.count_for(time) + diff > 0 {
                    assert!(
                        current_read_capabilities.less_equal(time),
                        "update {:?} for collection {id} is trying to \
                            install read capabilities before the current \
                            frontier of read capabilities, read capabilities before applying: {:?}",
                        update,
                        collection.read_capabilities
                    );
                }
            }

            let changes = collection.read_capabilities.update_iter(update.drain());
            update.extend(changes);

            if id.is_user() {
                trace!(
                %id,
                ?collection.storage_dependencies,
                ?update,
                "forwarding update to storage dependencies");
            }

            for id in collection.storage_dependencies.iter() {
                updates
                    .entry(*id)
                    .or_insert_with(ChangeBatch::new)
                    .extend(update.iter().cloned());
            }

            let (changes, frontier) = collections_net
                .entry(id)
                .or_insert_with(|| (<ChangeBatch<_>>::new(), Antichain::new()));

            changes.extend(update.drain());
            *frontier = collection.read_capabilities.frontier().to_owned();
        }

        // Translate our net compute actions into downgrades of persist sinces.
        // The actual downgrades are performed by a Tokio task asynchronously.
        let mut persist_compaction_commands = Vec::with_capacity(collections_net.len());
        for (key, (mut changes, frontier)) in collections_net {
            if !changes.is_empty() {
                // If the collection has a "primary" collection, let that primary drive compaction.
                let collection = collections.get(&key).expect("must still exist");
                let should_emit_persist_compaction = collection.description.primary.is_none();

                if frontier.is_empty() {
                    info!(id = %key, "removing collection state because the since advanced to []!");
                    collections.remove(&key).expect("must still exist");
                }

                if should_emit_persist_compaction {
                    persist_compaction_commands.push((key, frontier));
                }
            }
        }

        if !persist_compaction_commands.is_empty() {
            cmd_tx
                .send(BackgroundCmd::DowngradeSince(persist_compaction_commands))
                .expect("cannot fail to send");
        }
    }

    /// Remove any shards that we know are finalized
    fn synchronize_finalized_shards(&self, storage_metadata: &StorageMetadata) {
        self.finalized_shards
            .lock()
            .retain(|shard| storage_metadata.unfinalized_shards.contains(shard));
    }
}

// See comments on the above impl for StorageCollectionsImpl.
#[async_trait]
impl<T> StorageCollections for StorageCollectionsImpl<T>
where
    T: TimelyTimestamp
        + Lattice
        + Codec64
        + From<EpochMillis>
        + TimestampManipulation
        + Into<mz_repr::Timestamp>
        + Sync,
{
    type Timestamp = T;

    async fn initialize_state(
        &self,
        txn: &mut (dyn StorageTxn<T> + Send),
        init_ids: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<T>> {
        let metadata = txn.get_collection_metadata();
        let existing_metadata: BTreeSet<_> = metadata.into_iter().map(|(id, _)| id).collect();

        // Determine which collections we do not yet have metadata for.
        let new_collections: BTreeSet<GlobalId> =
            init_ids.difference(&existing_metadata).cloned().collect();

        self.prepare_state(
            txn,
            new_collections,
            BTreeSet::default(),
            BTreeMap::default(),
        )
        .await?;

        // All shards that belong to collections dropped in the last epoch are
        // eligible for finalization.
        //
        // n.b. this introduces an unlikely race condition: if a collection is
        // dropped from the catalog, but the dataflow is still running on a
        // worker, assuming the shard is safe to finalize on reboot may cause
        // the cluster to panic.
        let unfinalized_shards = txn.get_unfinalized_shards().into_iter().collect_vec();

        info!(?unfinalized_shards, "initializing finalizable_shards");

        self.finalizable_shards.lock().extend(unfinalized_shards);

        Ok(())
    }

    fn update_parameters(&self, config_params: StorageParameters) {
        // We serialize the dyncfg updates in StorageParameters, but configure
        // persist separately.
        config_params.dyncfg_updates.apply(self.persist.cfg());

        self.config
            .lock()
            .expect("lock poisoned")
            .update(config_params);
    }

    fn collection_metadata(&self, id: GlobalId) -> Result<CollectionMetadata, CollectionMissing> {
        let collections = self.collections.lock().expect("lock poisoned");

        collections
            .get(&id)
            .map(|c| c.collection_metadata.clone())
            .ok_or(CollectionMissing(id))
    }

    fn active_collection_metadatas(&self) -> Vec<(GlobalId, CollectionMetadata)> {
        let collections = self.collections.lock().expect("lock poisoned");

        collections
            .iter()
            .filter(|(_id, c)| !c.is_dropped())
            .map(|(id, c)| (*id, c.collection_metadata.clone()))
            .collect()
    }

    fn collections_frontiers(
        &self,
        ids: Vec<GlobalId>,
    ) -> Result<Vec<CollectionFrontiers<Self::Timestamp>>, CollectionMissing> {
        if ids.is_empty() {
            return Ok(vec![]);
        }

        let collections = self.collections.lock().expect("lock poisoned");

        let res = ids
            .into_iter()
            .map(|id| {
                collections
                    .get(&id)
                    .map(|c| CollectionFrontiers {
                        id: id.clone(),
                        write_frontier: c.write_frontier.clone(),
                        implied_capability: c.implied_capability.clone(),
                        read_capabilities: c.read_capabilities.frontier().to_owned(),
                    })
                    .ok_or(CollectionMissing(id))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(res)
    }

    fn active_collection_frontiers(&self) -> Vec<CollectionFrontiers<Self::Timestamp>> {
        let collections = self.collections.lock().expect("lock poisoned");

        let res = collections
            .iter()
            .filter(|(_id, c)| !c.is_dropped())
            .map(|(id, c)| CollectionFrontiers {
                id: id.clone(),
                write_frontier: c.write_frontier.clone(),
                implied_capability: c.implied_capability.clone(),
                read_capabilities: c.read_capabilities.frontier().to_owned(),
            })
            .collect_vec();

        res
    }

    async fn snapshot_stats(
        &self,
        id: GlobalId,
        as_of: Antichain<Self::Timestamp>,
    ) -> Result<SnapshotStats, StorageError<Self::Timestamp>> {
        let metadata = self.collection_metadata(id)?;

        // See the comments in StorageController::snapshot for what's going on
        // here.
        let as_of = match metadata.txns_shard.as_ref() {
            None => SnapshotStatsAsOf::Direct(as_of),
            Some(txns_id) => {
                assert_eq!(txns_id, self.txns_read.txns_id());
                let as_of = as_of
                    .into_option()
                    .expect("cannot read as_of the empty antichain");
                self.txns_read.update_gt(as_of.clone()).await;
                let data_snapshot = self
                    .txns_read
                    .data_snapshot(metadata.data_shard, as_of.clone())
                    .await;
                SnapshotStatsAsOf::Txns(data_snapshot)
            }
        };
        self.snapshot_stats_inner(id, as_of).await
    }

    async fn snapshot_parts_stats(
        &self,
        id: GlobalId,
        as_of: Antichain<Self::Timestamp>,
    ) -> BoxFuture<'static, Result<SnapshotPartsStats, StorageError<Self::Timestamp>>> {
        let metadata = {
            let self_collections = self.collections.lock().expect("lock poisoned");

            let collection_metadata = self_collections
                .get(&id)
                .ok_or(StorageError::IdentifierMissing(id))
                .map(|c| c.collection_metadata.clone());

            match collection_metadata {
                Ok(m) => m,
                Err(e) => return Box::pin(async move { Err(e) }),
            }
        };

        // See the comments in StorageController::snapshot for what's going on
        // here.
        let persist = Arc::clone(&self.persist);
        let read_handle = Self::read_handle_for_snapshot(persist, &metadata, id).await;

        let data_snapshot = match (metadata, as_of.as_option()) {
            (
                CollectionMetadata {
                    txns_shard: Some(txns_id),
                    data_shard,
                    ..
                },
                Some(as_of),
            ) => {
                assert_eq!(txns_id, *self.txns_read.txns_id());
                self.txns_read.update_gt(as_of.clone()).await;
                let data_snapshot = self
                    .txns_read
                    .data_snapshot(data_shard, as_of.clone())
                    .await;
                Some(data_snapshot)
            }
            _ => None,
        };

        Box::pin(async move {
            let read_handle = read_handle?;
            let result = match data_snapshot {
                Some(data_snapshot) => data_snapshot.snapshot_parts_stats(&read_handle).await,
                None => read_handle.snapshot_parts_stats(as_of).await,
            };
            read_handle.expire().await;
            result.map_err(|_| StorageError::ReadBeforeSince(id))
        })
    }

    // TODO(petrosagg): This signature is not very useful in the context of partially ordered times
    // where the as_of frontier might have multiple elements. In the current form the mutually
    // incomparable updates will be accumulated together to a state of the collection that never
    // actually existed. We should include the original time in the updates advanced by the as_of
    // frontier in the result and let the caller decide what to do with the information.
    fn snapshot(
        &self,
        id: GlobalId,
        as_of: Self::Timestamp,
    ) -> BoxFuture<'static, Result<Vec<(Row, StorageDiff)>, StorageError<Self::Timestamp>>> {
        self.snapshot(id, as_of, &self.txns_read)
    }

    async fn snapshot_latest(
        &self,
        id: GlobalId,
    ) -> Result<Vec<Row>, StorageError<Self::Timestamp>> {
        let upper = self.recent_upper(id).await?;
        let res = match upper.as_option() {
            Some(f) if f > &T::minimum() => {
                let as_of = f.step_back().unwrap();

                let snapshot = self.snapshot(id, as_of, &self.txns_read).await.unwrap();
                snapshot
                    .into_iter()
                    .map(|(row, diff)| {
                        assert_eq!(diff, 1, "snapshot doesn't accumulate to set");
                        row
                    })
                    .collect()
            }
            Some(_min) => {
                // The collection must be empty!
                Vec::new()
            }
            // The collection is closed, we cannot determine a latest read
            // timestamp based on the upper.
            _ => {
                return Err(StorageError::InvalidUsage(
                    "collection closed, cannot determine a read timestamp based on the upper"
                        .to_string(),
                ));
            }
        };

        Ok(res)
    }

    fn snapshot_cursor(
        &self,
        id: GlobalId,
        as_of: Self::Timestamp,
    ) -> BoxFuture<'static, Result<SnapshotCursor<Self::Timestamp>, StorageError<Self::Timestamp>>>
    where
        Self::Timestamp: TimelyTimestamp + Lattice + Codec64,
    {
        let metadata = match self.collection_metadata(id) {
            Ok(metadata) => metadata.clone(),
            Err(e) => return async { Err(e.into()) }.boxed(),
        };
        let txns_read = metadata.txns_shard.as_ref().map(|txns_id| {
            // Ensure the txn's shard the controller has is the same that this
            // collection is registered to.
            assert_eq!(txns_id, self.txns_read.txns_id());
            self.txns_read.clone()
        });
        let persist = Arc::clone(&self.persist);

        // See the comments in Self::snapshot for what's going on here.
        async move {
            let mut handle = Self::read_handle_for_snapshot(persist, &metadata, id).await?;
            let cursor = match txns_read {
                None => {
                    let cursor = handle
                        .snapshot_cursor(Antichain::from_elem(as_of), |_| true)
                        .await
                        .map_err(|_| StorageError::ReadBeforeSince(id))?;
                    SnapshotCursor {
                        _read_handle: handle,
                        cursor,
                    }
                }
                Some(txns_read) => {
                    txns_read.update_gt(as_of.clone()).await;
                    let data_snapshot = txns_read
                        .data_snapshot(metadata.data_shard, as_of.clone())
                        .await;
                    let cursor = data_snapshot
                        .snapshot_cursor(&mut handle, |_| true)
                        .await
                        .map_err(|_| StorageError::ReadBeforeSince(id))?;
                    SnapshotCursor {
                        _read_handle: handle,
                        cursor,
                    }
                }
            };

            Ok(cursor)
        }
        .boxed()
    }

    fn snapshot_and_stream(
        &self,
        id: GlobalId,
        as_of: Self::Timestamp,
    ) -> BoxFuture<
        'static,
        Result<
            BoxStream<'static, (SourceData, Self::Timestamp, StorageDiff)>,
            StorageError<Self::Timestamp>,
        >,
    >
    where
        Self::Timestamp: TimelyTimestamp + Lattice + Codec64 + 'static,
    {
        self.snapshot_and_stream(id, as_of, &self.txns_read)
    }

    fn create_update_builder(
        &self,
        id: GlobalId,
    ) -> BoxFuture<
        'static,
        Result<
            TimestamplessUpdateBuilder<SourceData, (), Self::Timestamp, StorageDiff>,
            StorageError<Self::Timestamp>,
        >,
    > {
        let metadata = match self.collection_metadata(id) {
            Ok(m) => m,
            Err(e) => return Box::pin(async move { Err(e.into()) }),
        };
        let persist = Arc::clone(&self.persist);

        async move {
            let persist_client = persist
                .open(metadata.persist_location.clone())
                .await
                .expect("invalid persist usage");
            let write_handle = persist_client
                .open_writer::<SourceData, (), Self::Timestamp, StorageDiff>(
                    metadata.data_shard,
                    Arc::new(metadata.relation_desc.clone()),
                    Arc::new(UnitSchema),
                    Diagnostics {
                        shard_name: id.to_string(),
                        handle_purpose: format!("create write batch {}", id),
                    },
                )
                .await
                .expect("invalid persist usage");
            let builder = TimestamplessUpdateBuilder::new(&write_handle);

            Ok(builder)
        }
        .boxed()
    }

    fn check_exists(&self, id: GlobalId) -> Result<(), StorageError<Self::Timestamp>> {
        let collections = self.collections.lock().expect("lock poisoned");

        if collections.contains_key(&id) {
            Ok(())
        } else {
            Err(StorageError::IdentifierMissing(id))
        }
    }

    async fn prepare_state(
        &self,
        txn: &mut (dyn StorageTxn<Self::Timestamp> + Send),
        ids_to_add: BTreeSet<GlobalId>,
        ids_to_drop: BTreeSet<GlobalId>,
        ids_to_register: BTreeMap<GlobalId, ShardId>,
    ) -> Result<(), StorageError<T>> {
        txn.insert_collection_metadata(
            ids_to_add
                .into_iter()
                .map(|id| (id, ShardId::new()))
                .collect(),
        )?;
        txn.insert_collection_metadata(ids_to_register)?;

        // Delete the metadata for any dropped collections.
        let dropped_mappings = txn.delete_collection_metadata(ids_to_drop);

        let dropped_shards = dropped_mappings
            .into_iter()
            .map(|(_id, shard)| shard)
            .collect();

        txn.insert_unfinalized_shards(dropped_shards)?;

        // Reconcile any shards we've successfully finalized with the shard
        // finalization collection.
        let finalized_shards = self.finalized_shards.lock().iter().copied().collect();
        txn.mark_shards_as_finalized(finalized_shards);

        Ok(())
    }

    // TODO(aljoscha): It would be swell if we could refactor this Leviathan of
    // a method/move individual parts to their own methods.
    #[instrument(level = "debug")]
    async fn create_collections_for_bootstrap(
        &self,
        storage_metadata: &StorageMetadata,
        register_ts: Option<Self::Timestamp>,
        mut collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
        migrated_storage_collections: &BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        let is_in_txns = |id, metadata: &CollectionMetadata| {
            metadata.txns_shard.is_some()
                && !(self.read_only && migrated_storage_collections.contains(&id))
        };

        // Validate first, to avoid corrupting state.
        // 1. create a dropped identifier, or
        // 2. create an existing identifier with a new description.
        // Make sure to check for errors within `ingestions` as well.
        collections.sort_by_key(|(id, _)| *id);
        collections.dedup();
        for pos in 1..collections.len() {
            if collections[pos - 1].0 == collections[pos].0 {
                return Err(StorageError::CollectionIdReused(collections[pos].0));
            }
        }

        {
            // Early sanity check: if we knew about a collection already it's
            // description must match!
            //
            // NOTE: There could be concurrent modifications to
            // `self.collections`, but this sanity check is better than nothing.
            let self_collections = self.collections.lock().expect("lock poisoned");
            for (id, description) in collections.iter() {
                if let Some(existing_collection) = self_collections.get(id) {
                    if &existing_collection.description != description {
                        return Err(StorageError::CollectionIdReused(*id));
                    }
                }
            }
        }

        // We first enrich each collection description with some additional
        // metadata...
        let enriched_with_metadata = collections
            .into_iter()
            .map(|(id, description)| {
                let data_shard = storage_metadata.get_collection_shard::<T>(id)?;

                // If the shard is being managed by txn-wal (initially,
                // tables), then we need to pass along the shard id for the txns
                // shard to dataflow rendering.
                let txns_shard = description
                    .data_source
                    .in_txns()
                    .then(|| *self.txns_read.txns_id());

                let metadata = CollectionMetadata {
                    persist_location: self.persist_location.clone(),
                    data_shard,
                    relation_desc: description.desc.clone(),
                    txns_shard,
                };

                Ok((id, description, metadata))
            })
            .collect_vec();

        // So that we can open `SinceHandle`s for each collections concurrently.
        let persist_client = self
            .persist
            .open(self.persist_location.clone())
            .await
            .unwrap();
        let persist_client = &persist_client;
        // Reborrow the `&mut self` as immutable, as all the concurrent work to
        // be processed in this stream cannot all have exclusive access.
        use futures::stream::{StreamExt, TryStreamExt};
        let this = &*self;
        let mut to_register: Vec<_> = futures::stream::iter(enriched_with_metadata)
            .map(|data: Result<_, StorageError<Self::Timestamp>>| {
                let register_ts = register_ts.clone();
                async move {
                    let (id, description, metadata) = data?;

                    // should be replaced with real introspection
                    // (https://github.com/MaterializeInc/database-issues/issues/4078)
                    // but for now, it's helpful to have this mapping written down
                    // somewhere
                    debug!("mapping GlobalId={} to shard ({})", id, metadata.data_shard);

                    // If this collection has a primary, the primary is responsible for downgrading
                    // the critical since and it would be an error if we did so here while opening
                    // the since handle.
                    let since = if description.primary.is_some() {
                        None
                    } else {
                        description.since.as_ref()
                    };

                    let (write, mut since_handle) = this
                        .open_data_handles(
                            &id,
                            metadata.data_shard,
                            since,
                            metadata.relation_desc.clone(),
                            persist_client,
                        )
                        .await;

                    // Present tables as springing into existence at the register_ts
                    // by advancing the since. Otherwise, we could end up in a
                    // situation where a table with a long compaction window appears
                    // to exist before the environment (and this the table) existed.
                    //
                    // We could potentially also do the same thing for other
                    // sources, in particular storage's internal sources and perhaps
                    // others, but leave them for now.
                    match description.data_source {
                        DataSource::Introspection(_)
                        | DataSource::IngestionExport { .. }
                        | DataSource::Webhook
                        | DataSource::Ingestion(_)
                        | DataSource::Progress
                        | DataSource::Other => {}
                        DataSource::Sink { .. } => {}
                        DataSource::Table => {
                            let register_ts = register_ts.expect(
                                "caller should have provided a register_ts when creating a table",
                            );
                            if since_handle.since().elements() == &[T::minimum()]
                                && !migrated_storage_collections.contains(&id)
                            {
                                debug!("advancing {} to initial since of {:?}", id, register_ts);
                                let token = since_handle.opaque();
                                let _ = since_handle
                                    .compare_and_downgrade_since(
                                        &token,
                                        (&token, &Antichain::from_elem(register_ts.clone())),
                                    )
                                    .await;
                            }
                        }
                    }

                    Ok::<_, StorageError<Self::Timestamp>>((
                        id,
                        description,
                        write,
                        since_handle,
                        metadata,
                    ))
                }
            })
            // Poll each future for each collection concurrently, maximum of 50 at a time.
            .buffer_unordered(50)
            // HERE BE DRAGONS:
            //
            // There are at least 2 subtleties in using `FuturesUnordered`
            // (which `buffer_unordered` uses underneath:
            // - One is captured here
            //   <https://github.com/rust-lang/futures-rs/issues/2387>
            // - And the other is deadlocking if processing an OUTPUT of a
            //   `FuturesUnordered` stream attempts to obtain an async mutex that
            //   is also obtained in the futures being polled.
            //
            // Both of these could potentially be issues in all usages of
            // `buffer_unordered` in this method, so we stick the standard
            // advice: only use `try_collect` or `collect`!
            .try_collect()
            .await?;

        // Reorder in dependency order.
        #[derive(Ord, PartialOrd, Eq, PartialEq)]
        enum DependencyOrder {
            /// Tables should always be registered first, and large ids before small ones.
            Table(Reverse<GlobalId>),
            /// For most collections the id order is the correct one.
            Collection(GlobalId),
            /// Sinks should always be registered last.
            Sink(GlobalId),
        }
        to_register.sort_by_key(|(id, desc, ..)| match &desc.data_source {
            DataSource::Table => DependencyOrder::Table(Reverse(*id)),
            DataSource::Sink { .. } => DependencyOrder::Sink(*id),
            _ => DependencyOrder::Collection(*id),
        });

        // We hold this lock for a very short amount of time, just doing some
        // hashmap inserts and unbounded channel sends.
        let mut self_collections = self.collections.lock().expect("lock poisoned");

        for (id, description, write_handle, since_handle, metadata) in to_register {
            let write_frontier = write_handle.upper();
            let data_shard_since = since_handle.since().clone();

            // Determine if this collection has any dependencies.
            let storage_dependencies =
                self.determine_collection_dependencies(&*self_collections, id, &description)?;

            // Determine the initial since of the collection.
            let initial_since = match storage_dependencies
                .iter()
                .at_most_one()
                .expect("should have at most one dependency")
            {
                Some(dep) => {
                    let dependency_collection = self_collections
                        .get(dep)
                        .ok_or(StorageError::IdentifierMissing(*dep))?;
                    let dependency_since = dependency_collection.implied_capability.clone();

                    // If an item has a dependency, its initial since must be
                    // advanced as far as its dependency, i.e. a dependency's
                    // since may never be in advance of its dependents.
                    //
                    // We have to do this every time we initialize the
                    // collection, though––the invariant might have been upheld
                    // correctly in the previous epoch, but the
                    // `data_shard_since` might not have compacted and, on
                    // establishing a new persist connection, still have data we
                    // said _could_ be compacted.
                    if PartialOrder::less_than(&data_shard_since, &dependency_since) {
                        // The dependency since cannot be beyond the dependent
                        // (our) upper unless the collection is new. In
                        // practice, the depdenency is the remap shard of a
                        // source (export), and if the since is allowed to
                        // "catch up" to the upper, that is `upper <= since`, a
                        // restarting ingestion cannot differentiate between
                        // updates that have already been written out to the
                        // backing persist shard and updates that have yet to be
                        // written. We would write duplicate updates.
                        //
                        // If this check fails, it means that the read hold
                        // installed on the dependency was probably not upheld
                        // –– if it were, the dependency's since could not have
                        // advanced as far the dependent's upper.
                        //
                        // We don't care about the dependency since when the
                        // write frontier is empty. In that case, no-one can
                        // write down any more updates.
                        mz_ore::soft_assert_or_log!(
                            write_frontier.elements() == &[T::minimum()]
                                || write_frontier.is_empty()
                                || PartialOrder::less_than(&dependency_since, write_frontier),
                            "dependency ({dep}) since has advanced past dependent ({id}) upper \n
                            dependent ({id}): since {:?}, upper {:?} \n
                            dependency ({dep}): since {:?}",
                            data_shard_since,
                            write_frontier,
                            dependency_since
                        );

                        dependency_since
                    } else {
                        data_shard_since
                    }
                }
                None => data_shard_since,
            };

            let mut collection_state = CollectionState::new(
                description,
                initial_since,
                write_frontier.clone(),
                storage_dependencies,
                metadata.clone(),
            );

            // Install the collection state in the appropriate spot.
            match &collection_state.description.data_source {
                DataSource::Introspection(_) => {
                    self_collections.insert(id, collection_state);
                }
                DataSource::Webhook => {
                    self_collections.insert(id, collection_state);
                }
                DataSource::IngestionExport {
                    ingestion_id,
                    details,
                    data_config,
                } => {
                    // Adjust the source to contain this export.
                    let source_collection = self_collections
                        .get_mut(ingestion_id)
                        .expect("known to exist");
                    match &mut source_collection.description {
                        CollectionDescription {
                            data_source: DataSource::Ingestion(ingestion_desc),
                            ..
                        } => ingestion_desc.source_exports.insert(
                            id,
                            SourceExport {
                                storage_metadata: (),
                                details: details.clone(),
                                data_config: data_config.clone(),
                            },
                        ),
                        _ => unreachable!(
                            "SourceExport must only refer to primary sources that already exist"
                        ),
                    };

                    self_collections.insert(id, collection_state);
                }
                DataSource::Table => {
                    // See comment on self.initial_txn_upper on why we're doing
                    // this.
                    if is_in_txns(id, &metadata)
                        && PartialOrder::less_than(
                            &collection_state.write_frontier,
                            &self.initial_txn_upper,
                        )
                    {
                        // We could try and be cute and use the join of the txn
                        // upper and the table upper. But that has more
                        // complicated reasoning for why it is or isn't correct,
                        // and we're only dealing with totally ordered times
                        // here.
                        collection_state
                            .write_frontier
                            .clone_from(&self.initial_txn_upper);
                    }
                    self_collections.insert(id, collection_state);
                }
                DataSource::Progress | DataSource::Other => {
                    self_collections.insert(id, collection_state);
                }
                DataSource::Ingestion(_) => {
                    self_collections.insert(id, collection_state);
                }
                DataSource::Sink { .. } => {
                    self_collections.insert(id, collection_state);
                }
            }

            self.register_handles(id, is_in_txns(id, &metadata), since_handle, write_handle);

            // If this collection has a dependency, install a read hold on it.
            self.install_collection_dependency_read_holds_inner(&mut *self_collections, id)?;
        }

        drop(self_collections);

        self.synchronize_finalized_shards(storage_metadata);

        Ok(())
    }

    async fn alter_ingestion_source_desc(
        &self,
        ingestion_id: GlobalId,
        source_desc: SourceDesc,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        // The StorageController checks the validity of these. And we just
        // accept them.

        let mut self_collections = self.collections.lock().expect("lock poisoned");
        let collection = self_collections
            .get_mut(&ingestion_id)
            .ok_or(StorageError::IdentifierMissing(ingestion_id))?;

        let curr_ingestion = match &mut collection.description.data_source {
            DataSource::Ingestion(active_ingestion) => active_ingestion,
            _ => unreachable!("verified collection refers to ingestion"),
        };

        curr_ingestion.desc = source_desc;
        debug!("altered {ingestion_id}'s SourceDesc");

        Ok(())
    }

    async fn alter_ingestion_export_data_configs(
        &self,
        source_exports: BTreeMap<GlobalId, SourceExportDataConfig>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        let mut self_collections = self.collections.lock().expect("lock poisoned");

        for (source_export_id, new_data_config) in source_exports {
            // We need to adjust the data config on the CollectionState for
            // the source export collection directly
            let source_export_collection = self_collections
                .get_mut(&source_export_id)
                .ok_or_else(|| StorageError::IdentifierMissing(source_export_id))?;
            let ingestion_id = match &mut source_export_collection.description.data_source {
                DataSource::IngestionExport {
                    ingestion_id,
                    details: _,
                    data_config,
                } => {
                    *data_config = new_data_config.clone();
                    *ingestion_id
                }
                o => {
                    tracing::warn!("alter_ingestion_export_data_configs called on {:?}", o);
                    Err(StorageError::IdentifierInvalid(source_export_id))?
                }
            };
            // We also need to adjust the data config on the CollectionState of the
            // Ingestion that the export is associated with.
            let ingestion_collection = self_collections
                .get_mut(&ingestion_id)
                .ok_or_else(|| StorageError::IdentifierMissing(ingestion_id))?;

            match &mut ingestion_collection.description.data_source {
                DataSource::Ingestion(ingestion_desc) => {
                    let source_export = ingestion_desc
                        .source_exports
                        .get_mut(&source_export_id)
                        .ok_or_else(|| StorageError::IdentifierMissing(source_export_id))?;

                    if source_export.data_config != new_data_config {
                        tracing::info!(?source_export_id, from = ?source_export.data_config, to = ?new_data_config, "alter_ingestion_export_data_configs, updating");
                        source_export.data_config = new_data_config;
                    } else {
                        tracing::warn!(
                            "alter_ingestion_export_data_configs called on \
                                    export {source_export_id} of {ingestion_id} but \
                                    the data config was the same"
                        );
                    }
                }
                o => {
                    tracing::warn!("alter_ingestion_export_data_configs called on {:?}", o);
                    Err(StorageError::IdentifierInvalid(ingestion_id))?;
                }
            }
        }

        Ok(())
    }

    async fn alter_ingestion_connections(
        &self,
        source_connections: BTreeMap<GlobalId, GenericSourceConnection<InlinedConnection>>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        let mut self_collections = self.collections.lock().expect("lock poisoned");

        for (id, conn) in source_connections {
            let collection = self_collections
                .get_mut(&id)
                .ok_or_else(|| StorageError::IdentifierMissing(id))?;

            match &mut collection.description.data_source {
                DataSource::Ingestion(ingestion) => {
                    // If the connection hasn't changed, there's no sense in
                    // re-rendering the dataflow.
                    if ingestion.desc.connection != conn {
                        info!(from = ?ingestion.desc.connection, to = ?conn, "alter_ingestion_connections, updating");
                        ingestion.desc.connection = conn;
                    } else {
                        warn!(
                            "update_source_connection called on {id} but the \
                            connection was the same"
                        );
                    }
                }
                o => {
                    warn!("update_source_connection called on {:?}", o);
                    Err(StorageError::IdentifierInvalid(id))?;
                }
            }
        }

        Ok(())
    }

    async fn alter_table_desc(
        &self,
        existing_collection: GlobalId,
        new_collection: GlobalId,
        new_desc: RelationDesc,
        expected_version: RelationVersion,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        let data_shard = {
            let self_collections = self.collections.lock().expect("lock poisoned");
            let existing = self_collections
                .get(&existing_collection)
                .ok_or_else(|| StorageError::IdentifierMissing(existing_collection))?;

            // TODO(alter_table): Support changes to sources.
            if existing.description.data_source != DataSource::Table {
                return Err(StorageError::IdentifierInvalid(existing_collection));
            }

            existing.collection_metadata.data_shard
        };

        let persist_client = self
            .persist
            .open(self.persist_location.clone())
            .await
            .unwrap();

        // Evolve the schema of this shard.
        let diagnostics = Diagnostics {
            shard_name: existing_collection.to_string(),
            handle_purpose: "alter_table_desc".to_string(),
        };
        // We map the Adapter's RelationVersion 1:1 with SchemaId.
        let expected_schema = expected_version.into();
        let schema_result = persist_client
            .compare_and_evolve_schema::<SourceData, (), T, StorageDiff>(
                data_shard,
                expected_schema,
                &new_desc,
                &UnitSchema,
                diagnostics,
            )
            .await
            .map_err(|e| StorageError::InvalidUsage(e.to_string()))?;
        tracing::info!(
            ?existing_collection,
            ?new_collection,
            ?new_desc,
            "evolved schema"
        );

        match schema_result {
            CaESchema::Ok(id) => id,
            // TODO(alter_table): If we get an expected mismatch we should retry.
            CaESchema::ExpectedMismatch {
                schema_id,
                key,
                val,
            } => {
                mz_ore::soft_panic_or_log!(
                    "schema expectation mismatch {schema_id:?}, {key:?}, {val:?}"
                );
                return Err(StorageError::Generic(anyhow::anyhow!(
                    "schema expected mismatch, {existing_collection:?}",
                )));
            }
            CaESchema::Incompatible => {
                mz_ore::soft_panic_or_log!(
                    "incompatible schema! {existing_collection} {new_desc:?}"
                );
                return Err(StorageError::Generic(anyhow::anyhow!(
                    "schema incompatible, {existing_collection:?}"
                )));
            }
        };

        // Once the new schema is registered we can open new data handles.
        let (write_handle, since_handle) = self
            .open_data_handles(
                &new_collection,
                data_shard,
                None,
                new_desc.clone(),
                &persist_client,
            )
            .await;

        // TODO(alter_table): Do we need to advance the since of the table to match the time this
        // new version was registered with txn-wal?

        // Great! Our new schema is registered with Persist, now we need to update our internal
        // data structures.
        {
            let mut self_collections = self.collections.lock().expect("lock poisoned");

            // Update the existing collection so we know it's a "projection" of this new one.
            let existing = self_collections
                .get_mut(&existing_collection)
                .expect("existing collection missing");

            // A higher level should already be asserting this, but let's make sure.
            assert_eq!(existing.description.data_source, DataSource::Table);
            assert_none!(existing.description.primary);

            // The existing version of the table will depend on the new version.
            existing.description.primary = Some(new_collection);
            existing.storage_dependencies.push(new_collection);

            // Copy over the frontiers from the previous version.
            // The new table starts with two holds - the implied capability, and the hold from
            // the previous version - both at the previous version's read frontier.
            let implied_capability = existing.read_capabilities.frontier().to_owned();
            let write_frontier = existing.write_frontier.clone();

            // Determine the relevant read capabilities on the new collection.
            //
            // Note(parkmycar): Originally we used `install_collection_dependency_read_holds_inner`
            // here, but that only installed a ReadHold on the new collection for the implied
            // capability of the existing collection. This would cause runtime panics because it
            // would eventually result in negative read capabilities.
            let mut changes = ChangeBatch::new();
            changes.extend(implied_capability.iter().map(|t| (t.clone(), 1)));

            // Note: The new collection is now the "primary collection".
            let collection_desc = CollectionDescription::for_table(new_desc.clone());
            let collection_meta = CollectionMetadata {
                persist_location: self.persist_location.clone(),
                relation_desc: collection_desc.desc.clone(),
                data_shard,
                txns_shard: Some(self.txns_read.txns_id().clone()),
            };
            let collection_state = CollectionState::new(
                collection_desc,
                implied_capability,
                write_frontier,
                Vec::new(),
                collection_meta,
            );

            // Add a record of the new collection.
            self_collections.insert(new_collection, collection_state);

            let mut updates = BTreeMap::from([(new_collection, changes)]);
            StorageCollectionsImpl::update_read_capabilities_inner(
                &self.cmd_tx,
                &mut *self_collections,
                &mut updates,
            );
        };

        // TODO(alter_table): Support changes to sources.
        self.register_handles(new_collection, true, since_handle, write_handle);

        info!(%existing_collection, %new_collection, ?new_desc, "altered table");

        Ok(())
    }

    fn drop_collections_unvalidated(
        &self,
        storage_metadata: &StorageMetadata,
        identifiers: Vec<GlobalId>,
    ) {
        debug!(?identifiers, "drop_collections_unvalidated");

        let mut self_collections = self.collections.lock().expect("lock poisoned");

        for id in identifiers.iter() {
            let metadata = storage_metadata.get_collection_shard::<T>(*id);
            mz_ore::soft_assert_or_log!(
                matches!(metadata, Err(StorageError::IdentifierMissing(_))),
                "dropping {id}, but drop was not synchronized with storage \
                controller via `synchronize_collections`"
            );

            let dropped_data_source = match self_collections.get(id) {
                Some(col) => col.description.data_source.clone(),
                None => continue,
            };

            // If we are dropping source exports, we need to modify the
            // ingestion that it runs on.
            if let DataSource::IngestionExport { ingestion_id, .. } = dropped_data_source {
                // Adjust the source to remove this export.
                let ingestion = match self_collections.get_mut(&ingestion_id) {
                    Some(ingestion) => ingestion,
                    // Primary ingestion already dropped.
                    None => {
                        tracing::error!(
                            "primary source {ingestion_id} seemingly dropped before subsource {id}",
                        );
                        continue;
                    }
                };

                match &mut ingestion.description {
                    CollectionDescription {
                        data_source: DataSource::Ingestion(ingestion_desc),
                        ..
                    } => {
                        let removed = ingestion_desc.source_exports.remove(id);
                        mz_ore::soft_assert_or_log!(
                            removed.is_some(),
                            "dropped subsource {id} already removed from source exports"
                        );
                    }
                    _ => unreachable!(
                        "SourceExport must only refer to primary sources that already exist"
                    ),
                };
            }
        }

        // Policies that advance the since to the empty antichain. We do still
        // honor outstanding read holds, and collections will only be dropped
        // once those are removed as well.
        //
        // We don't explicitly remove read capabilities! Downgrading the
        // frontier of the source to `[]` (the empty Antichain), will propagate
        // to the storage dependencies.
        let mut finalized_policies = Vec::new();

        for id in identifiers {
            // Make sure it's still there, might already have been deleted.
            if self_collections.contains_key(&id) {
                finalized_policies.push((id, ReadPolicy::ValidFrom(Antichain::new())));
            }
        }
        self.set_read_policies_inner(&mut self_collections, finalized_policies);

        drop(self_collections);

        self.synchronize_finalized_shards(storage_metadata);
    }

    fn set_read_policies(&self, policies: Vec<(GlobalId, ReadPolicy<Self::Timestamp>)>) {
        let mut collections = self.collections.lock().expect("lock poisoned");

        if tracing::enabled!(tracing::Level::TRACE) {
            let user_capabilities = collections
                .iter_mut()
                .filter(|(id, _c)| id.is_user())
                .map(|(id, c)| {
                    let updates = c.read_capabilities.updates().cloned().collect_vec();
                    (*id, c.implied_capability.clone(), updates)
                })
                .collect_vec();

            trace!(?policies, ?user_capabilities, "set_read_policies");
        }

        self.set_read_policies_inner(&mut collections, policies);

        if tracing::enabled!(tracing::Level::TRACE) {
            let user_capabilities = collections
                .iter_mut()
                .filter(|(id, _c)| id.is_user())
                .map(|(id, c)| {
                    let updates = c.read_capabilities.updates().cloned().collect_vec();
                    (*id, c.implied_capability.clone(), updates)
                })
                .collect_vec();

            trace!(?user_capabilities, "after! set_read_policies");
        }
    }

    fn acquire_read_holds(
        &self,
        desired_holds: Vec<GlobalId>,
    ) -> Result<Vec<ReadHold<Self::Timestamp>>, CollectionMissing> {
        if desired_holds.is_empty() {
            return Ok(vec![]);
        }

        let mut collections = self.collections.lock().expect("lock poisoned");

        let mut advanced_holds = Vec::new();
        // We advance the holds by our current since frontier. Can't acquire
        // holds for times that have been compacted away!
        //
        // NOTE: We acquire read holds at the earliest possible time rather than
        // at the implied capability. This is so that, for example, adapter can
        // acquire a read hold to hold back the frontier, giving the COMPUTE
        // controller a chance to also acquire a read hold at that early
        // frontier. If/when we change the interplay between adapter and COMPUTE
        // to pass around ReadHold tokens, we might tighten this up and instead
        // acquire read holds at the implied capability.
        for id in desired_holds.iter() {
            let collection = collections.get(id).ok_or(CollectionMissing(*id))?;
            let since = collection.read_capabilities.frontier().to_owned();
            advanced_holds.push((*id, since));
        }

        let mut updates = advanced_holds
            .iter()
            .map(|(id, hold)| {
                let mut changes = ChangeBatch::new();
                changes.extend(hold.iter().map(|time| (time.clone(), 1)));
                (*id, changes)
            })
            .collect::<BTreeMap<_, _>>();

        StorageCollectionsImpl::update_read_capabilities_inner(
            &self.cmd_tx,
            &mut collections,
            &mut updates,
        );

        let acquired_holds = advanced_holds
            .into_iter()
            .map(|(id, since)| ReadHold::with_channel(id, since, self.holds_tx.clone()))
            .collect_vec();

        trace!(?desired_holds, ?acquired_holds, "acquire_read_holds");

        Ok(acquired_holds)
    }

    /// Determine time dependence information for the object.
    fn determine_time_dependence(
        &self,
        id: GlobalId,
    ) -> Result<Option<TimeDependence>, TimeDependenceError> {
        use TimeDependenceError::CollectionMissing;
        let collections = self.collections.lock().expect("lock poisoned");
        let mut collection = Some(collections.get(&id).ok_or(CollectionMissing(id))?);

        let mut result = None;

        while let Some(c) = collection.take() {
            use DataSource::*;
            if let Some(timeline) = &c.description.timeline {
                // Only the epoch timeline follows wall-clock.
                if *timeline != Timeline::EpochMilliseconds {
                    break;
                }
            }
            match &c.description.data_source {
                Ingestion(ingestion) => {
                    use GenericSourceConnection::*;
                    match ingestion.desc.connection {
                        // Kafka, Postgres, MySql, and SQL Server sources all
                        // follow wall clock.
                        Kafka(_) | Postgres(_) | MySql(_) | SqlServer(_) => {
                            result = Some(TimeDependence::default())
                        }
                        // Load generators not further specified.
                        LoadGenerator(_) => {}
                    }
                }
                IngestionExport { ingestion_id, .. } => {
                    let c = collections
                        .get(ingestion_id)
                        .ok_or(CollectionMissing(*ingestion_id))?;
                    collection = Some(c);
                }
                // Introspection, other, progress, table, and webhook sources follow wall clock.
                Introspection(_) | Progress | Table { .. } | Webhook { .. } => {
                    result = Some(TimeDependence::default())
                }
                // Materialized views, continual tasks, etc, aren't managed by storage.
                Other => {}
                Sink { .. } => {}
            };
        }
        Ok(result)
    }

    fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
        // Destructure `self` here so we don't forget to consider dumping newly added fields.
        let Self {
            envd_epoch,
            read_only,
            finalizable_shards,
            finalized_shards,
            collections,
            txns_read: _,
            config,
            initial_txn_upper,
            persist_location,
            persist: _,
            cmd_tx: _,
            holds_tx: _,
            _background_task: _,
            _finalize_shards_task: _,
        } = self;

        let finalizable_shards: Vec<_> = finalizable_shards
            .lock()
            .iter()
            .map(ToString::to_string)
            .collect();
        let finalized_shards: Vec<_> = finalized_shards
            .lock()
            .iter()
            .map(ToString::to_string)
            .collect();
        let collections: BTreeMap<_, _> = collections
            .lock()
            .expect("poisoned")
            .iter()
            .map(|(id, c)| (id.to_string(), format!("{c:?}")))
            .collect();
        let config = format!("{:?}", config.lock().expect("poisoned"));

        Ok(serde_json::json!({
            "envd_epoch": envd_epoch,
            "read_only": read_only,
            "finalizable_shards": finalizable_shards,
            "finalized_shards": finalized_shards,
            "collections": collections,
            "config": config,
            "initial_txn_upper": initial_txn_upper,
            "persist_location": format!("{persist_location:?}"),
        }))
    }
}

/// Wraps either a "critical" [SinceHandle] or a leased [ReadHandle].
///
/// When a [StorageCollections] is in read-only mode, we will only ever acquire
/// [ReadHandle], because acquiring the [SinceHandle] and driving forward its
/// since is considered a write. Conversely, when in read-write mode, we acquire
/// [SinceHandle].
#[derive(Debug)]
enum SinceHandleWrapper<T>
where
    T: TimelyTimestamp + Lattice + Codec64,
{
    Critical(SinceHandle<SourceData, (), T, StorageDiff, PersistEpoch>),
    Leased(ReadHandle<SourceData, (), T, StorageDiff>),
}

impl<T> SinceHandleWrapper<T>
where
    T: TimelyTimestamp + Lattice + Codec64 + TotalOrder + Sync,
{
    pub fn since(&self) -> &Antichain<T> {
        match self {
            Self::Critical(handle) => handle.since(),
            Self::Leased(handle) => handle.since(),
        }
    }

    pub fn opaque(&self) -> PersistEpoch {
        match self {
            Self::Critical(handle) => handle.opaque().clone(),
            Self::Leased(_handle) => {
                // The opaque is expected to be used with
                // `compare_and_downgrade_since`, and the leased handle doesn't
                // have a notion of an opaque. We pretend here and in
                // `compare_and_downgrade_since`.
                PersistEpoch(None)
            }
        }
    }

    pub async fn compare_and_downgrade_since(
        &mut self,
        expected: &PersistEpoch,
        new: (&PersistEpoch, &Antichain<T>),
    ) -> Result<Antichain<T>, PersistEpoch> {
        match self {
            Self::Critical(handle) => handle.compare_and_downgrade_since(expected, new).await,
            Self::Leased(handle) => {
                let (opaque, since) = new;
                assert_none!(opaque.0);

                handle.downgrade_since(since).await;

                Ok(since.clone())
            }
        }
    }

    pub async fn maybe_compare_and_downgrade_since(
        &mut self,
        expected: &PersistEpoch,
        new: (&PersistEpoch, &Antichain<T>),
    ) -> Option<Result<Antichain<T>, PersistEpoch>> {
        match self {
            Self::Critical(handle) => {
                handle
                    .maybe_compare_and_downgrade_since(expected, new)
                    .await
            }
            Self::Leased(handle) => {
                let (opaque, since) = new;
                assert_none!(opaque.0);

                handle.maybe_downgrade_since(since).await;

                Some(Ok(since.clone()))
            }
        }
    }

    pub fn snapshot_stats(
        &self,
        id: GlobalId,
        as_of: Option<Antichain<T>>,
    ) -> BoxFuture<'static, Result<SnapshotStats, StorageError<T>>> {
        match self {
            Self::Critical(handle) => {
                let res = handle
                    .snapshot_stats(as_of)
                    .map(move |x| x.map_err(|_| StorageError::ReadBeforeSince(id)));
                Box::pin(res)
            }
            Self::Leased(handle) => {
                let res = handle
                    .snapshot_stats(as_of)
                    .map(move |x| x.map_err(|_| StorageError::ReadBeforeSince(id)));
                Box::pin(res)
            }
        }
    }

    pub fn snapshot_stats_from_txn(
        &self,
        id: GlobalId,
        data_snapshot: DataSnapshot<T>,
    ) -> BoxFuture<'static, Result<SnapshotStats, StorageError<T>>> {
        match self {
            Self::Critical(handle) => Box::pin(
                data_snapshot
                    .snapshot_stats_from_critical(handle)
                    .map(move |x| x.map_err(|_| StorageError::ReadBeforeSince(id))),
            ),
            Self::Leased(handle) => Box::pin(
                data_snapshot
                    .snapshot_stats_from_leased(handle)
                    .map(move |x| x.map_err(|_| StorageError::ReadBeforeSince(id))),
            ),
        }
    }
}

/// State maintained about individual collections.
#[derive(Debug, Clone)]
struct CollectionState<T> {
    /// Description with which the collection was created
    pub description: CollectionDescription<T>,

    /// Accumulation of read capabilities for the collection.
    ///
    /// This accumulation will always contain `self.implied_capability`, but may
    /// also contain capabilities held by others who have read dependencies on
    /// this collection.
    pub read_capabilities: MutableAntichain<T>,

    /// The implicit capability associated with collection creation.  This
    /// should never be less than the since of the associated persist
    /// collection.
    pub implied_capability: Antichain<T>,

    /// The policy to use to downgrade `self.implied_capability`.
    pub read_policy: ReadPolicy<T>,

    /// Storage identifiers on which this collection depends.
    pub storage_dependencies: Vec<GlobalId>,

    /// Reported write frontier.
    pub write_frontier: Antichain<T>,

    pub collection_metadata: CollectionMetadata,
}

impl<T: TimelyTimestamp> CollectionState<T> {
    /// Creates a new collection state, with an initial read policy valid from
    /// `since`.
    pub fn new(
        description: CollectionDescription<T>,
        since: Antichain<T>,
        write_frontier: Antichain<T>,
        storage_dependencies: Vec<GlobalId>,
        metadata: CollectionMetadata,
    ) -> Self {
        let mut read_capabilities = MutableAntichain::new();
        read_capabilities.update_iter(since.iter().map(|time| (time.clone(), 1)));
        Self {
            description,
            read_capabilities,
            implied_capability: since.clone(),
            read_policy: ReadPolicy::NoPolicy {
                initial_since: since,
            },
            storage_dependencies,
            write_frontier,
            collection_metadata: metadata,
        }
    }

    /// Returns whether the collection was dropped.
    pub fn is_dropped(&self) -> bool {
        self.read_capabilities.is_empty()
    }
}

/// A task that keeps persist handles, downgrades sinces when asked,
/// periodically gets recent uppers from them, and updates the shard collection
/// state when needed.
///
/// This shares state with [StorageCollectionsImpl] via `Arcs` and channels.
#[derive(Debug)]
struct BackgroundTask<T: TimelyTimestamp + Lattice + Codec64> {
    config: Arc<Mutex<StorageConfiguration>>,
    cmds_tx: mpsc::UnboundedSender<BackgroundCmd<T>>,
    cmds_rx: mpsc::UnboundedReceiver<BackgroundCmd<T>>,
    holds_rx: mpsc::UnboundedReceiver<(GlobalId, ChangeBatch<T>)>,
    finalizable_shards: Arc<ShardIdSet>,
    collections: Arc<std::sync::Mutex<BTreeMap<GlobalId, CollectionState<T>>>>,
    // So we know what shard ID corresponds to what global ID, which we need
    // when re-enqueing futures for determining the next upper update.
    shard_by_id: BTreeMap<GlobalId, ShardId>,
    since_handles: BTreeMap<GlobalId, SinceHandleWrapper<T>>,
    txns_handle: Option<WriteHandle<SourceData, (), T, StorageDiff>>,
    txns_shards: BTreeSet<GlobalId>,
}

#[derive(Debug)]
enum BackgroundCmd<T: TimelyTimestamp + Lattice + Codec64> {
    Register {
        id: GlobalId,
        is_in_txns: bool,
        write_handle: WriteHandle<SourceData, (), T, StorageDiff>,
        since_handle: SinceHandleWrapper<T>,
    },
    DowngradeSince(Vec<(GlobalId, Antichain<T>)>),
    SnapshotStats(
        GlobalId,
        SnapshotStatsAsOf<T>,
        oneshot::Sender<SnapshotStatsRes<T>>,
    ),
}

/// A newtype wrapper to hang a Debug impl off of.
pub(crate) struct SnapshotStatsRes<T>(BoxFuture<'static, Result<SnapshotStats, StorageError<T>>>);

impl<T> Debug for SnapshotStatsRes<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotStatsRes").finish_non_exhaustive()
    }
}

impl<T> BackgroundTask<T>
where
    T: TimelyTimestamp
        + Lattice
        + Codec64
        + From<EpochMillis>
        + TimestampManipulation
        + Into<mz_repr::Timestamp>
        + Sync,
{
    async fn run(&mut self) {
        // Futures that fetch the recent upper from all other shards.
        let mut upper_futures: FuturesUnordered<
            std::pin::Pin<
                Box<
                    dyn Future<
                            Output = (
                                GlobalId,
                                WriteHandle<SourceData, (), T, StorageDiff>,
                                Antichain<T>,
                            ),
                        > + Send,
                >,
            >,
        > = FuturesUnordered::new();

        let gen_upper_future =
            |id, mut handle: WriteHandle<_, _, _, _>, prev_upper: Antichain<T>| {
                let fut = async move {
                    soft_assert_or_log!(
                        !prev_upper.is_empty(),
                        "cannot await progress when upper is already empty"
                    );
                    handle.wait_for_upper_past(&prev_upper).await;
                    let new_upper = handle.shared_upper();
                    (id, handle, new_upper)
                };

                fut
            };

        let mut txns_upper_future = match self.txns_handle.take() {
            Some(txns_handle) => {
                let upper = txns_handle.upper().clone();
                let txns_upper_future =
                    gen_upper_future(GlobalId::Transient(1), txns_handle, upper);
                txns_upper_future.boxed()
            }
            None => async { std::future::pending().await }.boxed(),
        };

        loop {
            tokio::select! {
                (id, handle, upper) = &mut txns_upper_future => {
                    trace!("new upper from txns shard: {:?}", upper);
                    let mut uppers = Vec::new();
                    for id in self.txns_shards.iter() {
                        uppers.push((*id, &upper));
                    }
                    self.update_write_frontiers(&uppers).await;

                    let fut = gen_upper_future(id, handle, upper);
                    txns_upper_future = fut.boxed();
                }
                Some((id, handle, upper)) = upper_futures.next() => {
                    if id.is_user() {
                        trace!("new upper for collection {id}: {:?}", upper);
                    }
                    let current_shard = self.shard_by_id.get(&id);
                    if let Some(shard_id) = current_shard {
                        if shard_id == &handle.shard_id() {
                            // Still current, so process the update and enqueue
                            // again!
                            let uppers = &[(id, &upper)];
                            self.update_write_frontiers(uppers).await;
                            if !upper.is_empty() {
                                let fut = gen_upper_future(id, handle, upper);
                                upper_futures.push(fut.boxed());
                            }
                        } else {
                            // Be polite and expire the write handle. This can
                            // happen when we get an upper update for a write
                            // handle that has since been replaced via Update.
                            handle.expire().await;
                        }
                    }
                }
                cmd = self.cmds_rx.recv() => {
                    let cmd = if let Some(cmd) = cmd {
                        cmd
                    } else {
                        // We're done!
                        break;
                    };

                    match cmd {
                        BackgroundCmd::Register{ id, is_in_txns, write_handle, since_handle } => {
                            debug!("registering handles for {}", id);
                            let previous = self.shard_by_id.insert(id, write_handle.shard_id());
                            if previous.is_some() {
                                panic!("already registered a WriteHandle for collection {id}");
                            }

                            let previous = self.since_handles.insert(id, since_handle);
                            if previous.is_some() {
                                panic!("already registered a SinceHandle for collection {id}");
                            }

                            if is_in_txns {
                                self.txns_shards.insert(id);
                            } else {
                                let upper = write_handle.upper().clone();
                                if !upper.is_empty() {
                                    let fut = gen_upper_future(id, write_handle, upper);
                                    upper_futures.push(fut.boxed());
                                }
                            }

                        }
                        BackgroundCmd::DowngradeSince(cmds) => {
                            self.downgrade_sinces(cmds).await;
                        }
                        BackgroundCmd::SnapshotStats(id, as_of, tx) => {
                            // NB: The requested as_of could be arbitrarily far
                            // in the future. So, in order to avoid blocking
                            // this loop until it's available and the
                            // `snapshot_stats` call resolves, instead return
                            // the future to the caller and await it there.
                            let res = match self.since_handles.get(&id) {
                                Some(x) => {
                                    let fut: BoxFuture<
                                        'static,
                                        Result<SnapshotStats, StorageError<T>>,
                                    > = match as_of {
                                        SnapshotStatsAsOf::Direct(as_of) => {
                                            x.snapshot_stats(id, Some(as_of))
                                        }
                                        SnapshotStatsAsOf::Txns(data_snapshot) => {
                                            x.snapshot_stats_from_txn(id, data_snapshot)
                                        }
                                    };
                                    SnapshotStatsRes(fut)
                                }
                                None => SnapshotStatsRes(Box::pin(futures::future::ready(Err(
                                    StorageError::IdentifierMissing(id),
                                )))),
                            };
                            // It's fine if the listener hung up.
                            let _ = tx.send(res);
                        }
                    }
                }
                Some(holds_changes) = self.holds_rx.recv() => {
                    let mut batched_changes = BTreeMap::new();
                    batched_changes.insert(holds_changes.0, holds_changes.1);

                    while let Ok(mut holds_changes) = self.holds_rx.try_recv() {
                        let entry = batched_changes.entry(holds_changes.0);
                        entry
                            .and_modify(|existing| existing.extend(holds_changes.1.drain()))
                            .or_insert_with(|| holds_changes.1);
                    }

                    let mut collections = self.collections.lock().expect("lock poisoned");

                    let user_changes = batched_changes
                        .iter()
                        .filter(|(id, _c)| id.is_user())
                        .map(|(id, c)| {
                            (id.clone(), c.clone())
                        })
                        .collect_vec();

                    if !user_changes.is_empty() {
                        trace!(?user_changes, "applying holds changes from channel");
                    }

                    StorageCollectionsImpl::update_read_capabilities_inner(
                        &self.cmds_tx,
                        &mut collections,
                        &mut batched_changes,
                    );
                }
            }
        }

        warn!("BackgroundTask shutting down");
    }

    #[instrument(level = "debug")]
    async fn update_write_frontiers(&self, updates: &[(GlobalId, &Antichain<T>)]) {
        let mut read_capability_changes = BTreeMap::default();

        let mut self_collections = self.collections.lock().expect("lock poisoned");

        for (id, new_upper) in updates.iter() {
            let collection = if let Some(c) = self_collections.get_mut(id) {
                c
            } else {
                trace!(
                    "Reference to absent collection {id}, due to concurrent removal of that collection"
                );
                continue;
            };

            if PartialOrder::less_than(&collection.write_frontier, *new_upper) {
                collection.write_frontier.clone_from(new_upper);
            }

            let mut new_read_capability = collection
                .read_policy
                .frontier(collection.write_frontier.borrow());

            if id.is_user() {
                trace!(
                    %id,
                    implied_capability = ?collection.implied_capability,
                    policy = ?collection.read_policy,
                    write_frontier = ?collection.write_frontier,
                    ?new_read_capability,
                    "update_write_frontiers");
            }

            if PartialOrder::less_equal(&collection.implied_capability, &new_read_capability) {
                let mut update = ChangeBatch::new();
                update.extend(new_read_capability.iter().map(|time| (time.clone(), 1)));
                std::mem::swap(&mut collection.implied_capability, &mut new_read_capability);
                update.extend(new_read_capability.iter().map(|time| (time.clone(), -1)));

                if !update.is_empty() {
                    read_capability_changes.insert(*id, update);
                }
            }
        }

        if !read_capability_changes.is_empty() {
            StorageCollectionsImpl::update_read_capabilities_inner(
                &self.cmds_tx,
                &mut self_collections,
                &mut read_capability_changes,
            );
        }
    }

    async fn downgrade_sinces(&mut self, cmds: Vec<(GlobalId, Antichain<T>)>) {
        for (id, new_since) in cmds {
            let since_handle = if let Some(c) = self.since_handles.get_mut(&id) {
                c
            } else {
                // This can happen when someone concurrently drops a collection.
                trace!("downgrade_sinces: reference to absent collection {id}");
                continue;
            };

            if id.is_user() {
                trace!("downgrading since of {} to {:?}", id, new_since);
            }

            let epoch = since_handle.opaque().clone();
            let result = if new_since.is_empty() {
                // A shard's since reaching the empty frontier is a prereq for
                // being able to finalize a shard, so the final downgrade should
                // never be rate-limited.
                let res = Some(
                    since_handle
                        .compare_and_downgrade_since(&epoch, (&epoch, &new_since))
                        .await,
                );

                info!(%id, "removing persist handles because the since advanced to []!");

                let _since_handle = self.since_handles.remove(&id).expect("known to exist");
                let dropped_shard_id = if let Some(shard_id) = self.shard_by_id.remove(&id) {
                    shard_id
                } else {
                    panic!("missing GlobalId -> ShardId mapping for id {id}");
                };

                // We're not responsible for writes to tables, so we also don't
                // de-register them from the txn system. Whoever is responsible
                // will remove them. We only make sure to remove the table from
                // our tracking.
                self.txns_shards.remove(&id);

                if !self
                    .config
                    .lock()
                    .expect("lock poisoned")
                    .parameters
                    .finalize_shards
                {
                    info!(
                        "not triggering shard finalization due to dropped storage object because enable_storage_shard_finalization parameter is false"
                    );
                    return;
                }

                info!(%id, %dropped_shard_id, "enqueing shard finalization due to dropped collection and dropped persist handle");

                self.finalizable_shards.lock().insert(dropped_shard_id);

                res
            } else {
                since_handle
                    .maybe_compare_and_downgrade_since(&epoch, (&epoch, &new_since))
                    .await
            };

            if let Some(Err(other_epoch)) = result {
                mz_ore::halt!("fenced by envd @ {other_epoch:?}. ours = {epoch:?}");
            }
        }
    }
}

struct FinalizeShardsTaskConfig {
    envd_epoch: NonZeroI64,
    config: Arc<Mutex<StorageConfiguration>>,
    metrics: StorageCollectionsMetrics,
    finalizable_shards: Arc<ShardIdSet>,
    finalized_shards: Arc<ShardIdSet>,
    persist_location: PersistLocation,
    persist: Arc<PersistClientCache>,
    read_only: bool,
}

async fn finalize_shards_task<T>(
    FinalizeShardsTaskConfig {
        envd_epoch,
        config,
        metrics,
        finalizable_shards,
        finalized_shards,
        persist_location,
        persist,
        read_only,
    }: FinalizeShardsTaskConfig,
) where
    T: TimelyTimestamp + TotalOrder + Lattice + Codec64 + Sync,
{
    if read_only {
        info!("disabling shard finalization in read only mode");
        return;
    }

    let mut interval = tokio::time::interval(Duration::from_secs(5));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        interval.tick().await;

        if !config
            .lock()
            .expect("lock poisoned")
            .parameters
            .finalize_shards
        {
            debug!(
                "not triggering shard finalization due to dropped storage object because enable_storage_shard_finalization parameter is false"
            );
            continue;
        }

        let current_finalizable_shards = {
            // We hold the lock for as short as possible and pull our cloned set
            // of shards.
            finalizable_shards.lock().iter().cloned().collect_vec()
        };

        if current_finalizable_shards.is_empty() {
            debug!("no shards to finalize");
            continue;
        }

        debug!(?current_finalizable_shards, "attempting to finalize shards");

        // Open a persist client to delete unused shards.
        let persist_client = persist.open(persist_location.clone()).await.unwrap();

        let metrics = &metrics;
        let finalizable_shards = &finalizable_shards;
        let finalized_shards = &finalized_shards;
        let persist_client = &persist_client;
        let diagnostics = &Diagnostics::from_purpose("finalizing shards");

        let force_downgrade_since = STORAGE_DOWNGRADE_SINCE_DURING_FINALIZATION
            .get(config.lock().expect("lock poisoned").config_set());

        let epoch = &PersistEpoch::from(envd_epoch);

        futures::stream::iter(current_finalizable_shards.clone())
            .map(|shard_id| async move {
                let persist_client = persist_client.clone();
                let diagnostics = diagnostics.clone();
                let epoch = epoch.clone();

                metrics.finalization_started.inc();

                let is_finalized = persist_client
                    .is_finalized::<SourceData, (), T, StorageDiff>(shard_id, diagnostics)
                    .await
                    .expect("invalid persist usage");

                if is_finalized {
                    debug!(%shard_id, "shard is already finalized!");
                    Some(shard_id)
                } else {
                    debug!(%shard_id, "finalizing shard");
                    let finalize = || async move {
                        // TODO: thread the global ID into the shard finalization WAL
                        let diagnostics = Diagnostics::from_purpose("finalizing shards");

                        // We only use the writer to advance the upper, so using a dummy schema is
                        // fine.
                        let mut write_handle: WriteHandle<SourceData, (), T, StorageDiff> =
                            persist_client
                                .open_writer(
                                    shard_id,
                                    Arc::new(RelationDesc::empty()),
                                    Arc::new(UnitSchema),
                                    diagnostics,
                                )
                                .await
                                .expect("invalid persist usage");
                        write_handle.advance_upper(&Antichain::new()).await;
                        write_handle.expire().await;

                        if force_downgrade_since {
                            let mut since_handle: SinceHandle<
                                SourceData,
                                (),
                                T,
                                StorageDiff,
                                PersistEpoch,
                            > = persist_client
                                .open_critical_since(
                                    shard_id,
                                    PersistClient::CONTROLLER_CRITICAL_SINCE,
                                    Diagnostics::from_purpose("finalizing shards"),
                                )
                                .await
                                .expect("invalid persist usage");
                            let handle_epoch = since_handle.opaque().clone();
                            let our_epoch = epoch.clone();
                            let epoch = if our_epoch.0 > handle_epoch.0 {
                                // We're newer, but it's fine to use the
                                // handle's old epoch to try and downgrade.
                                handle_epoch
                            } else {
                                // Good luck, buddy! The downgrade below will
                                // not succeed. There's a process with a newer
                                // epoch out there and someone at some juncture
                                // will fence out this process.
                                our_epoch
                            };
                            let new_since = Antichain::new();
                            let downgrade = since_handle
                                .compare_and_downgrade_since(&epoch, (&epoch, &new_since))
                                .await;
                            if let Err(e) = downgrade {
                                warn!("tried to finalize a shard with an advancing epoch: {e:?}");
                                return Ok(());
                            }
                            // Not available now, so finalization is broken.
                            // since_handle.expire().await;
                        }

                        persist_client
                            .finalize_shard::<SourceData, (), T, StorageDiff>(
                                shard_id,
                                Diagnostics::from_purpose("finalizing shards"),
                            )
                            .await
                    };

                    match finalize().await {
                        Err(e) => {
                            // Rather than error, just leave this shard as
                            // one to finalize later.
                            warn!("error during finalization of shard {shard_id}: {e:?}");
                            None
                        }
                        Ok(()) => {
                            debug!(%shard_id, "finalize success!");
                            Some(shard_id)
                        }
                    }
                }
            })
            // Poll each future for each collection concurrently, maximum of 10
            // at a time.
            // TODO(benesch): the concurrency here should be configurable
            // via LaunchDarkly.
            .buffer_unordered(10)
            // HERE BE DRAGONS: see warning on other uses of buffer_unordered.
            // The closure passed to `for_each` must remain fast or we risk
            // starving the finalization futures of calls to `poll`.
            .for_each(|shard_id| async move {
                match shard_id {
                    None => metrics.finalization_failed.inc(),
                    Some(shard_id) => {
                        // We make successfully finalized shards available for
                        // removal from the finalization WAL one by one, so that
                        // a handful of stuck shards don't prevent us from
                        // removing the shards that have made progress. The
                        // overhead of repeatedly acquiring and releasing the
                        // locks is negligible.
                        {
                            let mut finalizable_shards = finalizable_shards.lock();
                            let mut finalized_shards = finalized_shards.lock();
                            finalizable_shards.remove(&shard_id);
                            finalized_shards.insert(shard_id);
                        }

                        metrics.finalization_succeeded.inc();
                    }
                }
            })
            .await;

        debug!("done finalizing shards");
    }
}

#[derive(Debug)]
pub(crate) enum SnapshotStatsAsOf<T: TimelyTimestamp + Lattice + Codec64> {
    /// Stats for a shard with an "eager" upper (one that continually advances
    /// as time passes, even if no writes are coming in).
    Direct(Antichain<T>),
    /// Stats for a shard with a "lazy" upper (one that only physically advances
    /// in response to writes).
    Txns(DataSnapshot<T>),
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;

    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_dyncfg::ConfigSet;
    use mz_ore::assert_err;
    use mz_ore::metrics::{MetricsRegistry, UIntGauge};
    use mz_ore::now::SYSTEM_TIME;
    use mz_ore::url::SensitiveUrl;
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_client::cfg::PersistConfig;
    use mz_persist_client::rpc::PubSubClientConnection;
    use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_repr::{RelationDesc, Row};
    use mz_secrets::InMemorySecretsController;

    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: integer-to-pointer casts and `ptr::from_exposed_addr`
    async fn test_snapshot_stats(&self) {
        let persist_location = PersistLocation {
            blob_uri: SensitiveUrl::from_str("mem://").expect("invalid URL"),
            consensus_uri: SensitiveUrl::from_str("mem://").expect("invalid URL"),
        };
        let persist_client = PersistClientCache::new(
            PersistConfig::new_default_configs(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone()),
            &MetricsRegistry::new(),
            |_, _| PubSubClientConnection::noop(),
        );
        let persist_client = Arc::new(persist_client);

        let (cmds_tx, mut background_task) =
            BackgroundTask::new_for_test(persist_location.clone(), Arc::clone(&persist_client));
        let background_task =
            mz_ore::task::spawn(|| "storage_collections::background_task", async move {
                background_task.run().await
            });

        let persist = persist_client.open(persist_location).await.unwrap();

        let shard_id = ShardId::new();
        let since_handle = persist
            .open_critical_since(
                shard_id,
                PersistClient::CONTROLLER_CRITICAL_SINCE,
                Diagnostics::for_tests(),
            )
            .await
            .unwrap();
        let write_handle = persist
            .open_writer::<SourceData, (), mz_repr::Timestamp, StorageDiff>(
                shard_id,
                Arc::new(RelationDesc::empty()),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
            )
            .await
            .unwrap();

        cmds_tx
            .send(BackgroundCmd::Register {
                id: GlobalId::User(1),
                is_in_txns: false,
                since_handle: SinceHandleWrapper::Critical(since_handle),
                write_handle,
            })
            .unwrap();

        let mut write_handle = persist
            .open_writer::<SourceData, (), mz_repr::Timestamp, StorageDiff>(
                shard_id,
                Arc::new(RelationDesc::empty()),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
            )
            .await
            .unwrap();

        // No stats for unknown GlobalId.
        let stats =
            snapshot_stats(&cmds_tx, GlobalId::User(2), Antichain::from_elem(0.into())).await;
        assert_err!(stats);

        // Stats don't resolve for as_of past the upper.
        let stats_fut = snapshot_stats(&cmds_tx, GlobalId::User(1), Antichain::from_elem(1.into()));
        assert_none!(stats_fut.now_or_never());

        // // Call it again because now_or_never consumed our future and it's not clone-able.
        let stats_ts1_fut =
            snapshot_stats(&cmds_tx, GlobalId::User(1), Antichain::from_elem(1.into()));

        // Write some data.
        let data = (
            (SourceData(Ok(Row::default())), ()),
            mz_repr::Timestamp::from(0),
            1i64,
        );
        let () = write_handle
            .compare_and_append(
                &[data],
                Antichain::from_elem(0.into()),
                Antichain::from_elem(1.into()),
            )
            .await
            .unwrap()
            .unwrap();

        // Verify that we can resolve stats for ts 0 while the ts 1 stats call is outstanding.
        let stats = snapshot_stats(&cmds_tx, GlobalId::User(1), Antichain::from_elem(0.into()))
            .await
            .unwrap();
        assert_eq!(stats.num_updates, 1);

        // Write more data and unblock the ts 1 call
        let data = (
            (SourceData(Ok(Row::default())), ()),
            mz_repr::Timestamp::from(1),
            1i64,
        );
        let () = write_handle
            .compare_and_append(
                &[data],
                Antichain::from_elem(1.into()),
                Antichain::from_elem(2.into()),
            )
            .await
            .unwrap()
            .unwrap();

        let stats = stats_ts1_fut.await.unwrap();
        assert_eq!(stats.num_updates, 2);

        // Make sure it runs until at least here.
        drop(background_task);
    }

    async fn snapshot_stats<T: TimelyTimestamp + Lattice + Codec64>(
        cmds_tx: &mpsc::UnboundedSender<BackgroundCmd<T>>,
        id: GlobalId,
        as_of: Antichain<T>,
    ) -> Result<SnapshotStats, StorageError<T>> {
        let (tx, rx) = oneshot::channel();
        cmds_tx
            .send(BackgroundCmd::SnapshotStats(
                id,
                SnapshotStatsAsOf::Direct(as_of),
                tx,
            ))
            .unwrap();
        let res = rx.await.expect("BackgroundTask should be live").0;

        res.await
    }

    impl<T: TimelyTimestamp + Lattice + Codec64> BackgroundTask<T> {
        fn new_for_test(
            _persist_location: PersistLocation,
            _persist_client: Arc<PersistClientCache>,
        ) -> (mpsc::UnboundedSender<BackgroundCmd<T>>, Self) {
            let (cmds_tx, cmds_rx) = mpsc::unbounded_channel();
            let (_holds_tx, holds_rx) = mpsc::unbounded_channel();
            let connection_context =
                ConnectionContext::for_tests(Arc::new(InMemorySecretsController::new()));

            let task = Self {
                config: Arc::new(Mutex::new(StorageConfiguration::new(
                    connection_context,
                    ConfigSet::default(),
                ))),
                cmds_tx: cmds_tx.clone(),
                cmds_rx,
                holds_rx,
                finalizable_shards: Arc::new(ShardIdSet::new(
                    UIntGauge::new("finalizable_shards", "dummy gauge for tests").unwrap(),
                )),
                collections: Arc::new(Mutex::new(BTreeMap::new())),
                shard_by_id: BTreeMap::new(),
                since_handles: BTreeMap::new(),
                txns_handle: None,
                txns_shards: BTreeSet::new(),
            };

            (cmds_tx, task)
        }
    }
}
