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
use chrono::{DateTime, Utc};
use differential_dataflow::lattice::Lattice;
use mz_cluster_client::ReplicaId;
use mz_cluster_client::client::ClusterReplicaLocation;
use mz_controller_types::dyncfgs::WALLCLOCK_LAG_HISTOGRAM_PERIOD_INTERVAL;
use mz_dyncfg::ConfigSet;
use mz_ore::soft_panic_or_log;
use mz_persist_client::batch::ProtoBatch;
use mz_persist_types::{Codec64, Opaque, ShardId};
use mz_repr::adt::interval::Interval;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, Diff, GlobalId, RelationDesc, RelationVersion, Row};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::inline::InlinedConnection;
use mz_storage_types::controller::{CollectionMetadata, StorageError};
use mz_storage_types::errors::CollectionMissing;
use mz_storage_types::instances::StorageInstanceId;
use mz_storage_types::oneshot_sources::{OneshotIngestionRequest, OneshotResultCallback};
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::read_holds::ReadHold;
use mz_storage_types::read_policy::ReadPolicy;
use mz_storage_types::sinks::{StorageSinkConnection, StorageSinkDesc};
use mz_storage_types::sources::{
    GenericSourceConnection, IngestionDescription, SourceDesc, SourceExportDataConfig,
    SourceExportDetails, Timeline,
};
use serde::{Deserialize, Serialize};
use timely::progress::Timestamp as TimelyTimestamp;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, Timestamp};
use tokio::sync::{mpsc, oneshot};

use crate::client::{AppendOnlyUpdate, StatusUpdate, TableData};
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
    WallclockLagHistogram,

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
pub enum DataSource<T> {
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
    /// The adapter layer appends timestamped data, i.e. it is a `TABLE`.
    Table,
    /// This source's data does not need to be managed by the storage
    /// controller, e.g. it's a materialized view or the catalog collection.
    Other,
    /// This collection is the output collection of a sink.
    Sink { desc: ExportDescription<T> },
}

/// Describes a request to create a source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionDescription<T> {
    /// The schema of this collection
    pub desc: RelationDesc,
    /// The source of this collection's data.
    pub data_source: DataSource<T>,
    /// An optional frontier to which the collection's `since` should be advanced.
    pub since: Option<Antichain<T>>,
    /// A GlobalId to use for this collection to use for the status collection.
    /// Used to keep track of source status/error information.
    pub status_collection_id: Option<GlobalId>,
    /// The timeline of the source. Absent for materialized views, continual tasks, etc.
    pub timeline: Option<Timeline>,
    /// The primary of this collections.
    ///
    /// Multiple storage collections can point to the same persist shard,
    /// possibly with different schemas. In such a configuration, we select one
    /// of the involved collections as the primary, who "owns" the persist
    /// shard. All other involved collections have a dependency on the primary.
    pub primary: Option<GlobalId>,
}

impl<T> CollectionDescription<T> {
    /// Create a CollectionDescription for [`DataSource::Other`].
    pub fn for_other(desc: RelationDesc, since: Option<Antichain<T>>) -> Self {
        Self {
            desc,
            data_source: DataSource::Other,
            since,
            status_collection_id: None,
            timeline: None,
            primary: None,
        }
    }

    /// Create a CollectionDescription for a table.
    pub fn for_table(desc: RelationDesc) -> Self {
        Self {
            desc,
            data_source: DataSource::Table,
            since: None,
            status_collection_id: None,
            timeline: Some(Timeline::EpochMilliseconds),
            primary: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExportDescription<T = mz_repr::Timestamp> {
    pub sink: StorageSinkDesc<(), T>,
    /// The ID of the instance in which to install the export.
    pub instance_id: StorageInstanceId,
}

#[derive(Debug)]
pub enum Response<T> {
    FrontierUpdates(Vec<(GlobalId, Antichain<T>)>),
}

/// Metadata that the storage controller must know to properly handle the life
/// cycle of creating and dropping collections.
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

    /// Update storage configuration with new parameters.
    fn update_parameters(&mut self, config_params: StorageParameters);

    /// Get the current configuration, including parameters updated with `update_parameters`.
    fn config(&self) -> &StorageConfiguration;

    /// Returns the [CollectionMetadata] of the collection identified by `id`.
    fn collection_metadata(&self, id: GlobalId) -> Result<CollectionMetadata, CollectionMissing>;

    /// Returns `true` iff the given collection/ingestion has been hydrated.
    ///
    /// For this check, zero-replica clusters are always considered hydrated.
    /// Their collections would never normally be considered hydrated but it's
    /// clearly intentional that they have no replicas.
    fn collection_hydrated(
        &self,
        collection_id: GlobalId,
    ) -> Result<bool, StorageError<Self::Timestamp>>;

    /// Returns `true` if each non-transient, non-excluded collection is
    /// hydrated on at least one of the provided replicas.
    ///
    /// If no replicas are provided, this checks for hydration on _any_ replica.
    ///
    /// This also returns `true` in case this cluster does not have any
    /// replicas.
    fn collections_hydrated_on_replicas(
        &self,
        target_replica_ids: Option<Vec<ReplicaId>>,
        target_cluster_ids: &StorageInstanceId,
        exclude_collections: &BTreeSet<GlobalId>,
    ) -> Result<bool, StorageError<Self::Timestamp>>;

    /// Returns the since/upper frontiers of the identified collection.
    fn collection_frontiers(
        &self,
        id: GlobalId,
    ) -> Result<(Antichain<Self::Timestamp>, Antichain<Self::Timestamp>), CollectionMissing>;

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
        CollectionMissing,
    >;

    /// Acquire an iterator over [CollectionMetadata] for all active
    /// collections.
    ///
    /// A collection is "active" when it has a non-empty frontier of read
    /// capabilities.
    fn active_collection_metadatas(&self) -> Vec<(GlobalId, CollectionMetadata)>;

    /// Returns the IDs of ingestion exports running on the given instance. This
    /// includes the ingestion itself, if any, and running source tables (aka.
    /// subsources).
    fn active_ingestion_exports(
        &self,
        instance_id: StorageInstanceId,
    ) -> Box<dyn Iterator<Item = &GlobalId> + '_>;

    /// Checks whether a collection exists under the given `GlobalId`. Returns
    /// an error if the collection does not exist.
    fn check_exists(&self, id: GlobalId) -> Result<(), StorageError<Self::Timestamp>>;

    /// Creates a storage instance with the specified ID.
    ///
    /// A storage instance can have zero or one replicas. The instance is
    /// created with zero replicas.
    ///
    /// Panics if a storage instance with the given ID already exists.
    fn create_instance(&mut self, id: StorageInstanceId, workload_class: Option<String>);

    /// Drops the storage instance with the given ID.
    ///
    /// If you call this method while the storage instance has a replica
    /// attached, that replica will be leaked. Call `drop_replica` first.
    ///
    /// Panics if a storage instance with the given ID does not exist.
    fn drop_instance(&mut self, id: StorageInstanceId);

    /// Updates a storage instance's workload class.
    fn update_instance_workload_class(
        &mut self,
        id: StorageInstanceId,
        workload_class: Option<String>,
    );

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

    /// Across versions of Materialize the nullability of columns for some objects can change based
    /// on updates to our optimizer.
    ///
    /// During bootstrap we will register these new schemas with Persist.
    ///
    /// See: <https://github.com/MaterializeInc/database-issues/issues/2488>
    async fn evolve_nullability_for_bootstrap(
        &mut self,
        storage_metadata: &StorageMetadata,
        collections: Vec<(GlobalId, RelationDesc)>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

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

    /// Alters each identified collection to use the correlated [`GenericSourceConnection`].
    async fn alter_ingestion_connections(
        &mut self,
        source_connections: BTreeMap<GlobalId, GenericSourceConnection<InlinedConnection>>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Alters the data config for the specified source exports of the specified ingestions.
    async fn alter_ingestion_export_data_configs(
        &mut self,
        source_exports: BTreeMap<GlobalId, SourceExportDataConfig>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    async fn alter_table_desc(
        &mut self,
        existing_collection: GlobalId,
        new_collection: GlobalId,
        new_desc: RelationDesc,
        expected_version: RelationVersion,
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

    /// Create a oneshot ingestion.
    async fn create_oneshot_ingestion(
        &mut self,
        ingestion_id: uuid::Uuid,
        collection_id: GlobalId,
        instance_id: StorageInstanceId,
        request: OneshotIngestionRequest,
        result_tx: OneshotResultCallback<ProtoBatch>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Cancel a oneshot ingestion.
    fn cancel_oneshot_ingestion(
        &mut self,
        ingestion_id: uuid::Uuid,
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
        storage_metadata: &StorageMetadata,
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
    fn drop_sinks_unvalidated(
        &mut self,
        storage_metadata: &StorageMetadata,
        identifiers: Vec<GlobalId>,
    );

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
        commands: Vec<(GlobalId, Vec<TableData>)>,
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

    /// Records append-only status updates for the given introspection type.
    fn append_status_introspection_updates(
        &mut self,
        type_: IntrospectionType,
        updates: Vec<StatusUpdate>,
    );

    /// Updates the desired state of the given introspection type.
    ///
    /// Rows passed in `op` MUST have the correct schema for the given
    /// introspection type, as readers rely on this and might panic otherwise.
    fn update_introspection_collection(&mut self, type_: IntrospectionType, op: StorageWriteOp);

    /// Returns a sender for updates to the specified append-only introspection collection.
    ///
    /// # Panics
    ///
    /// Panics if the given introspection type is not associated with an append-only collection.
    fn append_only_introspection_tx(
        &self,
        type_: IntrospectionType,
    ) -> mpsc::UnboundedSender<(
        Vec<AppendOnlyUpdate>,
        oneshot::Sender<Result<(), StorageError<Self::Timestamp>>>,
    )>;

    /// Returns a sender for updates to the specified differential introspection collection.
    ///
    /// # Panics
    ///
    /// Panics if the given introspection type is not associated with a differential collection.
    fn differential_introspection_tx(
        &self,
        type_: IntrospectionType,
    ) -> mpsc::UnboundedSender<(
        StorageWriteOp,
        oneshot::Sender<Result<(), StorageError<Self::Timestamp>>>,
    )>;

    async fn real_time_recent_timestamp(
        &self,
        source_ids: BTreeSet<GlobalId>,
        timeout: Duration,
    ) -> Result<
        BoxFuture<Result<Self::Timestamp, StorageError<Self::Timestamp>>>,
        StorageError<Self::Timestamp>,
    >;

    /// Returns the state of the [`StorageController`] formatted as JSON.
    fn dump(&self) -> Result<serde_json::Value, anyhow::Error>;
}

impl<T> DataSource<T> {
    /// Returns true if the storage controller manages the data shard for this
    /// source using txn-wal.
    pub fn in_txns(&self) -> bool {
        match self {
            DataSource::Table => true,
            DataSource::Other
            | DataSource::Ingestion(_)
            | DataSource::IngestionExport { .. }
            | DataSource::Introspection(_)
            | DataSource::Progress
            | DataSource::Webhook => false,
            DataSource::Sink { .. } => false,
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
    /// Really only for keeping track of changes to the `derived_since`.
    pub read_capabilities: MutableAntichain<T>,

    /// The cluster this export is associated with.
    pub cluster_id: StorageInstanceId,

    /// The current since frontier, derived from `write_frontier` using
    /// `hold_policy`.
    pub derived_since: Antichain<T>,

    /// The read holds that this export has on its dependencies (its input and itself). When
    /// the upper of the export changes, we downgrade this, which in turn
    /// downgrades holds we have on our dependencies' sinces.
    pub read_holds: [ReadHold<T>; 2],

    /// The policy to use to downgrade `self.read_capability`.
    pub read_policy: ReadPolicy<T>,

    /// Reported write frontier.
    pub write_frontier: Antichain<T>,
}

impl<T: Timestamp> ExportState<T> {
    pub fn new(
        cluster_id: StorageInstanceId,
        read_hold: ReadHold<T>,
        self_hold: ReadHold<T>,
        write_frontier: Antichain<T>,
        read_policy: ReadPolicy<T>,
    ) -> Self
    where
        T: Lattice,
    {
        let mut dependency_since = Antichain::from_elem(T::minimum());
        for read_hold in [&read_hold, &self_hold] {
            dependency_since.join_assign(read_hold.since());
        }
        Self {
            read_capabilities: MutableAntichain::from(dependency_since.borrow()),
            cluster_id,
            derived_since: dependency_since,
            read_holds: [read_hold, self_hold],
            read_policy,
            write_frontier,
        }
    }

    /// Returns the cluster to which the export is bound.
    pub fn cluster_id(&self) -> StorageInstanceId {
        self.cluster_id
    }

    /// Returns the cluster to which the export is bound.
    pub fn input_hold(&self) -> &ReadHold<T> {
        &self.read_holds[0]
    }

    /// Returns whether the export was dropped.
    pub fn is_dropped(&self) -> bool {
        self.read_holds.iter().all(|h| h.since().is_empty())
    }
}
/// A channel that allows you to append a set of updates to a pre-defined [`GlobalId`].
///
/// See `CollectionManager::monotonic_appender` to acquire a [`MonotonicAppender`].
#[derive(Clone, Debug)]
pub struct MonotonicAppender<T> {
    /// Channel that sends to a [`tokio::task`] which pushes updates to Persist.
    tx: mpsc::UnboundedSender<(
        Vec<AppendOnlyUpdate>,
        oneshot::Sender<Result<(), StorageError<T>>>,
    )>,
}

impl<T> MonotonicAppender<T> {
    pub fn new(
        tx: mpsc::UnboundedSender<(
            Vec<AppendOnlyUpdate>,
            oneshot::Sender<Result<(), StorageError<T>>>,
        )>,
    ) -> Self {
        MonotonicAppender { tx }
    }

    pub async fn append(&self, updates: Vec<AppendOnlyUpdate>) -> Result<(), StorageError<T>> {
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

/// A wallclock lag measurement.
///
/// The enum representation reflects the fact that wallclock lag is undefined for unreadable
/// collections, i.e. collections that contain no readable times.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum WallclockLag {
    /// Lag value in seconds, for readable collections.
    Seconds(u64),
    /// Undefined lag, for unreadable collections.
    Undefined,
}

impl WallclockLag {
    /// The smallest possible wallclock lag measurement.
    pub const MIN: Self = Self::Seconds(0);

    /// Return the maximum of two lag values.
    ///
    /// We treat `Undefined` lags as greater than `Seconds`, to ensure we never report low lag
    /// values when a collection was actually unreadable for some amount of time.
    pub fn max(self, other: Self) -> Self {
        match (self, other) {
            (Self::Seconds(a), Self::Seconds(b)) => Self::Seconds(a.max(b)),
            (Self::Undefined, _) | (_, Self::Undefined) => Self::Undefined,
        }
    }

    /// Return the wrapped seconds value, or a default if the lag is `Undefined`.
    pub fn unwrap_seconds_or(self, default: u64) -> u64 {
        match self {
            Self::Seconds(s) => s,
            Self::Undefined => default,
        }
    }

    /// Create a new `WallclockLag` by transforming the wrapped seconds value.
    pub fn map_seconds(self, f: impl FnOnce(u64) -> u64) -> Self {
        match self {
            Self::Seconds(s) => Self::Seconds(f(s)),
            Self::Undefined => Self::Undefined,
        }
    }

    /// Convert this lag value into a [`Datum::Interval`] or [`Datum::Null`].
    pub fn into_interval_datum(self) -> Datum<'static> {
        match self {
            Self::Seconds(secs) => {
                let micros = i64::try_from(secs * 1_000_000).expect("must fit");
                Datum::Interval(Interval::new(0, 0, micros))
            }
            Self::Undefined => Datum::Null,
        }
    }

    /// Convert this lag value into a [`Datum::UInt64`] or [`Datum::Null`].
    pub fn into_uint64_datum(self) -> Datum<'static> {
        match self {
            Self::Seconds(secs) => Datum::UInt64(secs),
            Self::Undefined => Datum::Null,
        }
    }
}

/// The period covered by a wallclock lag histogram, represented as a `[start, end)` range.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct WallclockLagHistogramPeriod {
    pub start: CheckedTimestamp<DateTime<Utc>>,
    pub end: CheckedTimestamp<DateTime<Utc>>,
}

impl WallclockLagHistogramPeriod {
    /// Construct a `WallclockLagHistogramPeriod` from the given epoch timestamp and dyncfg.
    pub fn from_epoch_millis(epoch_ms: u64, dyncfg: &ConfigSet) -> Self {
        let interval = WALLCLOCK_LAG_HISTOGRAM_PERIOD_INTERVAL.get(dyncfg);
        let interval_ms = u64::try_from(interval.as_millis()).unwrap_or_else(|_| {
            soft_panic_or_log!("excessive wallclock lag histogram period interval: {interval:?}");
            let default = WALLCLOCK_LAG_HISTOGRAM_PERIOD_INTERVAL.default();
            u64::try_from(default.as_millis()).unwrap()
        });
        let interval_ms = std::cmp::max(interval_ms, 1);

        let start_ms = epoch_ms - (epoch_ms % interval_ms);
        let start_dt = mz_ore::now::to_datetime(start_ms);
        let start = start_dt.try_into().expect("must fit");

        let end_ms = start_ms + interval_ms;
        let end_dt = mz_ore::now::to_datetime(end_ms);
        let end = end_dt.try_into().expect("must fit");

        Self { start, end }
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
