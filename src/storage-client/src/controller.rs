// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A controller that provides an interface to the storage layer.
//!
//! The storage controller curates the creation of sources, the progress of readers through these collections,
//! and their eventual dropping and resource reclamation.
//!
//! The storage controller can be viewed as a partial map from `GlobalId` to collection. It is an error to
//! use an identifier before it has been "created" with `create_source()`. Once created, the controller holds
//! a read capability for each source, which is manipulated with `update_read_capabilities()`.
//! Eventually, the source is dropped with either `drop_sources()` or by allowing compaction to the
//! empty frontier.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::future::Future;
use std::num::NonZeroI64;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use differential_dataflow::lattice::Lattice;
use mz_cluster_client::client::ClusterReplicaLocation;
use mz_cluster_client::ReplicaId;
use mz_ore::collections::CollectionExt;
use mz_persist_client::read::{Cursor, ReadHandle};
use mz_persist_client::stats::{SnapshotPartsStats, SnapshotStats};
use mz_persist_types::{Codec64, Opaque, ShardId};
use mz_repr::{Diff, GlobalId, RelationDesc, Row};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::inline::InlinedConnection;
use mz_storage_types::controller::{CollectionMetadata, StorageError};
use mz_storage_types::instances::StorageInstanceId;
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::read_holds::{ReadHold, ReadHoldError};
use mz_storage_types::read_policy::ReadPolicy;
use mz_storage_types::sinks::{MetadataUnfilled, StorageSinkConnection, StorageSinkDesc};
use mz_storage_types::sources::{
    GenericSourceConnection, IngestionDescription, SourceData, SourceDesc, SourceExportDataConfig,
    SourceExportDetails,
};
use serde::{Deserialize, Serialize};
use timely::progress::Timestamp as TimelyTimestamp;
use timely::progress::{Antichain, Timestamp};
use tokio::sync::{mpsc, oneshot};

use crate::client::TimestamplessUpdate;
use crate::statistics::WebhookStatistics;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum IntrospectionType {
    /// We're not responsible for appending to this collection automatically, but we should
    /// automatically bump the write frontier from time to time.
    SinkStatusHistory,
    SourceStatusHistory,
    ShardMapping,

    Frontiers,
    ReplicaFrontiers,

    ReplicaStatusHistory,
    ReplicaMetricsHistory,
    WallclockLagHistory,

    // Note that this single-shard introspection source will be changed to per-replica,
    // once we allow multiplexing multiple sources/sinks on a single cluster.
    StorageSourceStatistics,
    StorageSinkStatistics,

    // The below are for statement logging.
    StatementExecutionHistory,
    SessionHistory,
    PreparedStatementHistory,
    SqlText,
    // For statement lifecycle logging, which is closely related
    // to statement logging
    StatementLifecycleHistory,

    // Collections written by the compute controller.
    ComputeDependencies,
    ComputeOperatorHydrationStatus,
    ComputeMaterializedViewRefreshes,
    ComputeErrorCounts,
    ComputeHydrationTimes,

    // Written by the Adapter for tracking AWS PrivateLink Connection Status History
    PrivatelinkConnectionStatusHistory,
}

/// Describes how data is written to the collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataSource {
    /// Ingest data from some external source.
    Ingestion(IngestionDescription),
    /// This source receives its data from the identified ingestion,
    /// from an external object identified using `SourceExportDetails`.
    ///
    /// The referenced ingestion must be created before all of its exports.
    IngestionExport {
        ingestion_id: GlobalId,
        details: SourceExportDetails,
        data_config: SourceExportDataConfig,
    },
    /// Data comes from introspection sources, which the controller itself is
    /// responsible for generating.
    Introspection(IntrospectionType),
    /// Data comes from the source's remapping/reclock operator.
    Progress,
    /// Data comes from external HTTP requests pushed to Materialize.
    Webhook,
    /// This source's data is does not need to be managed by the storage
    /// controller, e.g. it's a materialized view, table, or subsource.
    Other(DataSourceOther),
}

/// Describes how data is written to a collection maintained outside of the
/// storage controller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataSourceOther {
    /// `environmentd` appends timestamped data, i.e. it is a `TABLE`.
    TableWrites,
    /// Compute maintains, i.e. it is a `MATERIALIZED VIEW`.
    Compute,
}

/// Describes a request to create a source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionDescription<T> {
    /// The schema of this collection
    pub desc: RelationDesc,
    /// The source of this collection's data.
    pub data_source: DataSource,
    /// An optional frontier to which the collection's `since` should be advanced.
    pub since: Option<Antichain<T>>,
    /// A GlobalId to use for this collection to use for the status collection.
    /// Used to keep track of source status/error information.
    pub status_collection_id: Option<GlobalId>,
}

impl<T> CollectionDescription<T> {
    pub fn from_desc(desc: RelationDesc, source: DataSourceOther) -> Self {
        Self {
            desc,
            data_source: DataSource::Other(source),
            since: None,
            status_collection_id: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExportDescription<T = mz_repr::Timestamp> {
    pub sink: StorageSinkDesc<MetadataUnfilled, T>,
    /// The ID of the instance in which to install the export.
    pub instance_id: StorageInstanceId,
}

/// A cursor over a snapshot, allowing us to read just part of a snapshot in its
/// consolidated form.
pub struct SnapshotCursor<T: Codec64 + Timestamp + Lattice> {
    // We allocate a temporary read handle for each snapshot, and that handle needs to live at
    // least as long as the cursor itself, which holds part leases. Bundling them together!
    pub _read_handle: ReadHandle<SourceData, (), T, Diff>,
    pub cursor: Cursor<SourceData, (), T, Diff>,
}

impl<T: Codec64 + Timestamp + Lattice> SnapshotCursor<T> {
    pub async fn next(
        &mut self,
    ) -> Option<
        impl Iterator<Item = ((Result<SourceData, String>, Result<(), String>), T, Diff)> + Sized + '_,
    > {
        self.cursor.next().await
    }
}

#[derive(Debug)]
pub enum Response<T> {
    FrontierUpdates(Vec<(GlobalId, Antichain<T>)>),
}

/// Metadata that the storage controller must know to properly handle the life
/// cycle of creating and dropping collections.j
///
/// This data should be kept consistent with the state modified using
/// [`StorageTxn`].
///
/// n.b. the "txn WAL shard" is also metadata that's persisted, but if we
/// included it in this struct it would never be read.
#[derive(Debug, Clone, Serialize, Default)]
pub struct StorageMetadata {
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")]
    pub collection_metadata: BTreeMap<GlobalId, ShardId>,
    pub unfinalized_shards: BTreeSet<ShardId>,
}

impl StorageMetadata {
    pub fn get_collection_shard<T>(&self, id: GlobalId) -> Result<ShardId, StorageError<T>> {
        let shard_id = self
            .collection_metadata
            .get(&id)
            .ok_or(StorageError::IdentifierMissing(id))?;

        Ok(*shard_id)
    }
}

/// Provides an interface for the storage controller to read and write data that
/// is recorded elsewhere.
///
/// Data written to the implementor of this trait should make a consistent view
/// of the data available through [`StorageMetadata`].
#[async_trait]
pub trait StorageTxn<T> {
    /// Retrieve all of the visible storage metadata.
    ///
    /// The value of this map should be treated as opaque.
    fn get_collection_metadata(&self) -> BTreeMap<GlobalId, ShardId>;

    /// Add new storage metadata for a collection.
    ///
    /// Subsequent calls to [`StorageTxn::get_collection_metadata`] must include
    /// this data.
    fn insert_collection_metadata(
        &mut self,
        s: BTreeMap<GlobalId, ShardId>,
    ) -> Result<(), StorageError<T>>;

    /// Remove the metadata associated with the identified collections.
    ///
    /// Subsequent calls to [`StorageTxn::get_collection_metadata`] must not
    /// include these keys.
    fn delete_collection_metadata(&mut self, ids: BTreeSet<GlobalId>) -> Vec<(GlobalId, ShardId)>;

    /// Retrieve all of the shards that are no longer in use by an active
    /// collection but are yet to be finalized.
    fn get_unfinalized_shards(&self) -> BTreeSet<ShardId>;

    /// Insert the specified values as unfinalized shards.
    fn insert_unfinalized_shards(&mut self, s: BTreeSet<ShardId>) -> Result<(), StorageError<T>>;

    /// Mark the specified shards as finalized, deleting them from the
    /// unfinalized shard collection.
    fn mark_shards_as_finalized(&mut self, shards: BTreeSet<ShardId>);

    /// Get the txn WAL shard for this environment if it exists.
    fn get_txn_wal_shard(&self) -> Option<ShardId>;

    /// Store the specified shard as the environment's txn WAL shard.
    ///
    /// The implementor should error if the shard is already specified.
    fn write_txn_wal_shard(&mut self, shard: ShardId) -> Result<(), StorageError<T>>;
}

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// A predicate for a `Row` filter.
pub type RowPredicate = Box<dyn Fn(&Row) -> bool + Send + Sync>;

/// High-level write operations applicable to storage collections.
pub enum StorageWriteOp {
    /// Append a set of rows with specified multiplicities.
    ///
    /// The multiplicities may be negative, so an `Append` operation can perform
    /// both insertions and retractions.
    Append { updates: Vec<(Row, Diff)> },
    /// Delete all rows matching the given predicate.
    Delete { filter: RowPredicate },
}

impl StorageWriteOp {
    /// Returns whether this operation appends an empty set of updates.
    pub fn is_empty_append(&self) -> bool {
        match self {
            Self::Append { updates } => updates.is_empty(),
            Self::Delete { .. } => false,
        }
    }
}

#[async_trait(?Send)]
pub trait StorageController: Debug {
    type Timestamp: TimelyTimestamp;

    /// Marks the end of any initialization commands.
    ///
    /// The implementor may wait for this method to be called before implementing prior commands,
    /// and so it is important for a user to invoke this method as soon as it is comfortable.
    /// This method can be invoked immediately, at the potential expense of performance.
    fn initialization_complete(&mut self);

    /// Allow this controller and instances controlled by it to write to
    /// external systems.
    ///
    /// If the controller has previously been told about tables (via
    /// [StorageController::create_collections]), the caller must provide a
    /// `register_ts`, the timestamp at which any tables that are known to the
    /// controller should be registered in the txn system.
    ///
    /// # Panics
    ///
    /// Panics when the controller knows about tables but no `register_ts` is
    /// provided.
    async fn allow_writes(&mut self, register_ts: Option<Self::Timestamp>);

    /// Update storage configuration with new parameters.
    fn update_parameters(&mut self, config_params: StorageParameters);

    /// Get the current configuration, including parameters updated with `update_parameters`.
    fn config(&self) -> &StorageConfiguration;

    /// Returns the [CollectionMetadata] of the collection identified by `id`.
    fn collection_metadata(
        &self,
        id: GlobalId,
    ) -> Result<CollectionMetadata, StorageError<Self::Timestamp>>;

    /// Returns the since/upper frontiers of the identified collection.
    fn collection_frontiers(
        &self,
        id: GlobalId,
    ) -> Result<
        (Antichain<Self::Timestamp>, Antichain<Self::Timestamp>),
        StorageError<Self::Timestamp>,
    >;

    /// Returns the since/upper frontiers of the identified collections.
    ///
    /// Having a method that returns both frontiers at the same time, for all
    /// requested collections, ensures that we can get a consistent "snapshot"
    /// of collection state. If we had separate methods instead, and/or would
    /// allow getting frontiers for collections one at a time, it could happen
    /// that collection state changes concurrently, while information is
    /// gathered.
    fn collections_frontiers(
        &self,
        id: Vec<GlobalId>,
    ) -> Result<
        Vec<(
            GlobalId,
            Antichain<Self::Timestamp>,
            Antichain<Self::Timestamp>,
        )>,
        StorageError<Self::Timestamp>,
    >;

    /// Acquire an iterator over [CollectionMetadata] for all active
    /// collections.
    ///
    /// A collection is "active" when it has a non empty frontier of read
    /// capabilties.
    fn active_collection_metadatas(&self) -> Vec<(GlobalId, CollectionMetadata)>;

    /// Checks whether a collection exists under the given `GlobalId`. Returns
    /// an error if the collection does not exist.
    fn check_exists(&self, id: GlobalId) -> Result<(), StorageError<Self::Timestamp>>;

    /// Creates a storage instance with the specified ID.
    ///
    /// A storage instance can have zero or one replicas. The instance is
    /// created with zero replicas.
    ///
    /// Panics if a storage instance with the given ID already exists.
    fn create_instance(&mut self, id: StorageInstanceId);

    /// Drops the storage instance with the given ID.
    ///
    /// If you call this method while the storage instance has a replica
    /// attached, that replica will be leaked. Call `drop_replica` first.
    ///
    /// Panics if a storage instance with the given ID does not exist.
    fn drop_instance(&mut self, id: StorageInstanceId);

    /// Connects the storage instance to the specified replica.
    ///
    /// If the storage instance is already attached to a replica, communication
    /// with that replica is severed in favor of the new replica.
    ///
    /// In the future, this API will be adjusted to support active replication
    /// of storage instances (i.e., multiple replicas attached to a given
    /// storage instance).
    fn connect_replica(
        &mut self,
        instance_id: StorageInstanceId,
        replica_id: ReplicaId,
        location: ClusterReplicaLocation,
    );

    /// Disconnects the storage instance from the specified replica.
    fn drop_replica(&mut self, instance_id: StorageInstanceId, replica_id: ReplicaId);

    /// Create the sources described in the individual RunIngestionCommand commands.
    ///
    /// Each command carries the source id, the source description, and any associated metadata
    /// needed to ingest the particular source.
    ///
    /// This command installs collection state for the indicated sources, and they are
    /// now valid to use in queries at times beyond the initial `since` frontiers. Each
    /// collection also acquires a read capability at this frontier, which will need to
    /// be repeatedly downgraded with `allow_compaction()` to permit compaction.
    ///
    /// This method is NOT idempotent; It can fail between processing of different
    /// collections and leave the controller in an inconsistent state. It is almost
    /// always wrong to do anything but abort the process on `Err`.
    ///
    /// The `register_ts` is used as the initial timestamp that tables are available for reads. (We
    /// might later give non-tables the same treatment, but hold off on that initially.) Callers
    /// must provide a Some if any of the collections is a table. A None may be given if none of the
    /// collections are a table (i.e. all materialized views, sources, etc).
    async fn create_collections(
        &mut self,
        storage_metadata: &StorageMetadata,
        register_ts: Option<Self::Timestamp>,
        collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        self.create_collections_for_bootstrap(
            storage_metadata,
            register_ts,
            collections,
            &BTreeSet::new(),
        )
        .await
    }

    /// Like [`Self::create_collections`], except used specifically for bootstrap.
    ///
    /// `migrated_storage_collections` is a set of migrated storage collections to be excluded
    /// from the txn-wal sub-system.
    async fn create_collections_for_bootstrap(
        &mut self,
        storage_metadata: &StorageMetadata,
        register_ts: Option<Self::Timestamp>,
        collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
        migrated_storage_collections: &BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Check that the ingestion associated with `id` can use the provided
    /// [`SourceDesc`].
    ///
    /// Note that this check is optimistic and its return of `Ok(())` does not
    /// guarantee that subsequent calls to `alter_ingestion_source_desc` are
    /// guaranteed to succeed.
    fn check_alter_ingestion_source_desc(
        &mut self,
        ingestion_id: GlobalId,
        source_desc: &SourceDesc,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Alters the identified collection to use the provided [`SourceDesc`].
    async fn alter_ingestion_source_desc(
        &mut self,
        ingestion_id: GlobalId,
        source_desc: SourceDesc,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Alters each identified collection to use the correlated [`GenericSourceConnection`].
    async fn alter_ingestion_connections(
        &mut self,
        source_connections: BTreeMap<GlobalId, GenericSourceConnection<InlinedConnection>>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    async fn alter_table_desc(
        &mut self,
        table_id: GlobalId,
        new_desc: RelationDesc,
        forget_ts: Self::Timestamp,
        register_ts: Self::Timestamp,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Acquire an immutable reference to the export state, should it exist.
    fn export(
        &self,
        id: GlobalId,
    ) -> Result<&ExportState<Self::Timestamp>, StorageError<Self::Timestamp>>;

    /// Acquire a mutable reference to the export state, should it exist.
    fn export_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut ExportState<Self::Timestamp>, StorageError<Self::Timestamp>>;

    /// Create the sinks described by the `ExportDescription`.
    async fn create_exports(
        &mut self,
        exports: Vec<(GlobalId, ExportDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Alter the sink identified by the given id to match the provided `ExportDescription`.
    async fn alter_export(
        &mut self,
        id: GlobalId,
        export: ExportDescription<Self::Timestamp>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// For each identified export, alter its [`StorageSinkConnection`].
    async fn alter_export_connections(
        &mut self,
        exports: BTreeMap<GlobalId, StorageSinkConnection>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Drops the read capability for the tables and allows their resources to be reclaimed.
    fn drop_tables(
        &mut self,
        storage_metadata: &StorageMetadata,
        identifiers: Vec<GlobalId>,
        ts: Self::Timestamp,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Drops the read capability for the sources and allows their resources to be reclaimed.
    fn drop_sources(
        &mut self,
        storage_metadata: &StorageMetadata,
        identifiers: Vec<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Drops the read capability for the sinks and allows their resources to be reclaimed.
    fn drop_sinks(
        &mut self,
        identifiers: Vec<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Drops the read capability for the sinks and allows their resources to be reclaimed.
    ///
    /// TODO(jkosh44): This method does not validate the provided identifiers. Currently when the
    ///     controller starts/restarts it has no durable state. That means that it has no way of
    ///     remembering any past commands sent. In the future we plan on persisting state for the
    ///     controller so that it is aware of past commands.
    ///     Therefore this method is for dropping sinks that we know to have been previously
    ///     created, but have been forgotten by the controller due to a restart.
    ///     Once command history becomes durable we can remove this method and use the normal
    ///     `drop_sinks`.
    fn drop_sinks_unvalidated(&mut self, identifiers: Vec<GlobalId>);

    /// Drops the read capability for the sources and allows their resources to be reclaimed.
    ///
    /// TODO(jkosh44): This method does not validate the provided identifiers. Currently when the
    ///     controller starts/restarts it has no durable state. That means that it has no way of
    ///     remembering any past commands sent. In the future we plan on persisting state for the
    ///     controller so that it is aware of past commands.
    ///     Therefore this method is for dropping sources that we know to have been previously
    ///     created, but have been forgotten by the controller due to a restart.
    ///     Once command history becomes durable we can remove this method and use the normal
    ///     `drop_sources`.
    fn drop_sources_unvalidated(
        &mut self,
        storage_metadata: &StorageMetadata,
        identifiers: Vec<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Append `updates` into the local input named `id` and advance its upper to `upper`.
    ///
    /// The method returns a oneshot that can be awaited to indicate completion of the write.
    /// The method may return an error, indicating an immediately visible error, and also the
    /// oneshot may return an error if one is encountered during the write.
    ///
    /// All updates in `commands` are applied atomically.
    // TODO(petrosagg): switch upper to `Antichain<Timestamp>`
    fn append_table(
        &mut self,
        write_ts: Self::Timestamp,
        advance_to: Self::Timestamp,
        commands: Vec<(GlobalId, Vec<TimestamplessUpdate>)>,
    ) -> Result<
        tokio::sync::oneshot::Receiver<Result<(), StorageError<Self::Timestamp>>>,
        StorageError<Self::Timestamp>,
    >;

    /// Returns a [`MonotonicAppender`] which is a channel that can be used to monotonically
    /// append to the specified [`GlobalId`].
    fn monotonic_appender(
        &self,
        id: GlobalId,
    ) -> Result<MonotonicAppender<Self::Timestamp>, StorageError<Self::Timestamp>>;

    /// Returns a shared [`WebhookStatistics`] which can be used to report user-facing
    /// statistics for this given webhhook, specified by the [`GlobalId`].
    ///
    // This is used to support a fairly special case, where a source needs to report statistics
    // from outside the ordinary controller-clusterd path. Its possible to merge this with
    // `monotonic_appender`, whose only current user is webhooks, but given that they will
    // likely be moved to clusterd, we just leave this a special case.
    fn webhook_statistics(
        &self,
        id: GlobalId,
    ) -> Result<Arc<WebhookStatistics>, StorageError<Self::Timestamp>>;

    /// Returns the snapshot of the contents of the local input named `id` at `as_of`.
    fn snapshot(
        &mut self,
        id: GlobalId,
        as_of: Self::Timestamp,
    ) -> BoxFuture<Result<Vec<(Row, Diff)>, StorageError<Self::Timestamp>>>;

    /// Returns the snapshot of the contents of the local input named `id` at
    /// the largest readable `as_of`.
    async fn snapshot_latest(
        &mut self,
        id: GlobalId,
    ) -> Result<Vec<Row>, StorageError<Self::Timestamp>>;

    /// Returns the snapshot of the contents of the local input named `id` at `as_of`.
    async fn snapshot_cursor(
        &mut self,
        id: GlobalId,
        as_of: Self::Timestamp,
    ) -> Result<SnapshotCursor<Self::Timestamp>, StorageError<Self::Timestamp>>
    where
        Self::Timestamp: Codec64 + Timestamp + Lattice;

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
    ) -> BoxFuture<Result<SnapshotPartsStats, StorageError<Self::Timestamp>>>;

    /// Assigns a read policy to specific identifiers.
    ///
    /// The policies are assigned in the order presented, and repeated identifiers should
    /// conclude with the last policy. Changing a policy will immediately downgrade the read
    /// capability if appropriate, but it will not "recover" the read capability if the prior
    /// capability is already ahead of it.
    ///
    /// The `StorageController` may include its own overrides on these policies.
    ///
    /// Identifiers not present in `policies` retain their existing read policies.
    fn set_read_policy(&mut self, policies: Vec<(GlobalId, ReadPolicy<Self::Timestamp>)>);

    /// Acquires and returns the desired read holds, advancing them to the since
    /// frontier when necessary.
    fn acquire_read_holds(
        &mut self,
        desired_holds: Vec<GlobalId>,
    ) -> Result<Vec<ReadHold<Self::Timestamp>>, ReadHoldError>;

    /// Acquires and returns the earliest legal read hold.
    fn acquire_read_hold(
        &mut self,
        id: GlobalId,
    ) -> Result<ReadHold<Self::Timestamp>, ReadHoldError> {
        let hold = self.acquire_read_holds(vec![id])?.into_element();

        Ok(hold)
    }

    /// Waits until the controller is ready to process a response.
    ///
    /// This method may block for an arbitrarily long time.
    ///
    /// When the method returns, the owner should call
    /// [`StorageController::process`] to process the ready message.
    ///
    /// This method is cancellation safe.
    async fn ready(&mut self);

    /// Processes the work queued by [`StorageController::ready`].
    fn process(
        &mut self,
        storage_metadata: &StorageMetadata,
    ) -> Result<Option<Response<Self::Timestamp>>, anyhow::Error>;

    /// Exposes the internal state of the data shard for debugging and QA.
    ///
    /// We'll be thoughtful about making unnecessary changes, but the **output
    /// of this method needs to be gated from users**, so that it's not subject
    /// to our backward compatibility guarantees.
    ///
    /// TODO: Ideally this would return `impl Serialize` so the caller can do
    /// with it what they like, but that doesn't work in traits yet. The
    /// workaround (an associated type) doesn't work because persist doesn't
    /// want to make the type public. In the meantime, move the `serde_json`
    /// call from the single user into this method.
    async fn inspect_persist_state(&self, id: GlobalId)
        -> Result<serde_json::Value, anyhow::Error>;

    /// Records append-only updates for the given introspection type.
    ///
    /// Rows passed in `updates` MUST have the correct schema for the given
    /// introspection type, as readers rely on this and might panic otherwise.
    fn append_introspection_updates(&mut self, type_: IntrospectionType, updates: Vec<(Row, Diff)>);

    /// Updates the desired state of the given introspection type.
    ///
    /// Rows passed in `op` MUST have the correct schema for the given
    /// introspection type, as readers rely on this and might panic otherwise.
    fn update_introspection_collection(&mut self, type_: IntrospectionType, op: StorageWriteOp);

    /// On boot, seed the controller's metadata/state.
    async fn initialize_state(
        &mut self,
        txn: &mut (dyn StorageTxn<Self::Timestamp> + Send),
        init_ids: BTreeSet<GlobalId>,
        drop_ids: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Update the implementor of [`StorageTxn`] with the appropriate metadata
    /// given the IDs to add and drop.
    ///
    /// The data modified in the `StorageTxn` must be made available in all
    /// subsequent calls that require [`StorageMetadata`] as a parameter.
    async fn prepare_state(
        &self,
        txn: &mut (dyn StorageTxn<Self::Timestamp> + Send),
        ids_to_add: BTreeSet<GlobalId>,
        ids_to_drop: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    async fn real_time_recent_timestamp(
        &mut self,
        source_ids: BTreeSet<GlobalId>,
        timeout: Duration,
    ) -> Result<
        BoxFuture<Result<Self::Timestamp, StorageError<Self::Timestamp>>>,
        StorageError<Self::Timestamp>,
    >;
}

impl DataSource {
    /// Returns true if the storage controller manages the data shard for this
    /// source using txn-wal.
    pub fn in_txns(&self) -> bool {
        match self {
            DataSource::Other(DataSourceOther::TableWrites) => true,
            DataSource::Other(DataSourceOther::Compute)
            | DataSource::Ingestion(_)
            | DataSource::IngestionExport { .. }
            | DataSource::Introspection(_)
            | DataSource::Progress
            | DataSource::Webhook => false,
        }
    }
}

/// A wrapper struct that presents the adapter token to a format that is understandable by persist
/// and also allows us to differentiate between a token being present versus being set for the
/// first time.
#[derive(PartialEq, Clone, Debug)]
pub struct PersistEpoch(pub Option<NonZeroI64>);

impl Opaque for PersistEpoch {
    fn initial() -> Self {
        PersistEpoch(None)
    }
}

impl Codec64 for PersistEpoch {
    fn codec_name() -> String {
        "PersistEpoch".to_owned()
    }

    fn encode(&self) -> [u8; 8] {
        self.0.map(NonZeroI64::get).unwrap_or(0).to_le_bytes()
    }

    fn decode(buf: [u8; 8]) -> Self {
        Self(NonZeroI64::new(i64::from_le_bytes(buf)))
    }
}

impl From<NonZeroI64> for PersistEpoch {
    fn from(epoch: NonZeroI64) -> Self {
        Self(Some(epoch))
    }
}

/// State maintained about individual exports.
#[derive(Debug)]
pub struct ExportState<T: TimelyTimestamp> {
    /// Description with which the export was created
    pub description: ExportDescription<T>,

    /// The read hold that this export has on its dependencies (inputs). When
    /// the upper of the export changes, we downgrade this, which in turn
    /// downgrades holds we have on our dependencies' sinces.
    pub read_hold: ReadHold<T>,

    /// The policy to use to downgrade `self.read_capability`.
    pub read_policy: ReadPolicy<T>,

    /// Reported write frontier.
    pub write_frontier: Antichain<T>,

    /// Maximum frontier wallclock lag since the last introspection update.
    pub wallclock_lag_max: Duration,
}

impl<T: Timestamp> ExportState<T> {
    pub fn new(
        description: ExportDescription<T>,
        read_hold: ReadHold<T>,
        read_policy: ReadPolicy<T>,
    ) -> Self {
        Self {
            description,
            read_hold,
            read_policy,
            write_frontier: Antichain::from_elem(Timestamp::minimum()),
            wallclock_lag_max: Default::default(),
        }
    }

    /// Returns the cluster to which the export is bound.
    pub fn cluster_id(&self) -> StorageInstanceId {
        self.description.instance_id
    }

    /// Returns whether the export was dropped.
    pub fn is_dropped(&self) -> bool {
        self.read_hold.since().is_empty()
    }
}
/// A channel that allows you to append a set of updates to a pre-defined [`GlobalId`].
///
/// See `CollectionManager::monotonic_appender` to acquire a [`MonotonicAppender`].
#[derive(Clone, Debug)]
pub struct MonotonicAppender<T> {
    /// Channel that sends to a [`tokio::task`] which pushes updates to Persist.
    tx: mpsc::UnboundedSender<(
        Vec<(Row, Diff)>,
        oneshot::Sender<Result<(), StorageError<T>>>,
    )>,
}

impl<T> MonotonicAppender<T> {
    pub fn new(
        tx: mpsc::UnboundedSender<(
            Vec<(Row, Diff)>,
            oneshot::Sender<Result<(), StorageError<T>>>,
        )>,
    ) -> Self {
        MonotonicAppender { tx }
    }

    pub async fn append(&self, updates: Vec<(Row, Diff)>) -> Result<(), StorageError<T>> {
        let (tx, rx) = oneshot::channel();

        // Send our update to the CollectionManager.
        self.tx
            .send((updates, tx))
            .map_err(|_| StorageError::ShuttingDown("collection manager"))?;

        // Wait for a response, if we fail to receive then the CollectionManager has gone away.
        let result = rx
            .await
            .map_err(|_| StorageError::ShuttingDown("collection manager"))?;

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn lag_writes_by_zero() {
        let policy =
            ReadPolicy::lag_writes_by(mz_repr::Timestamp::default(), mz_repr::Timestamp::default());
        let write_frontier = Antichain::from_elem(mz_repr::Timestamp::from(5));
        assert_eq!(policy.frontier(write_frontier.borrow()), write_frontier);
    }
}
