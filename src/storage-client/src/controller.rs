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
use std::sync::Arc;

use async_trait::async_trait;
use differential_dataflow::lattice::Lattice;
use mz_cluster_client::client::ClusterReplicaLocation;
use mz_cluster_client::ReplicaId;
use mz_persist_client::read::{Cursor, ReadHandle};
use mz_persist_client::stats::{SnapshotPartsStats, SnapshotStats};
use mz_persist_types::Codec64;
use mz_repr::{Diff, GlobalId, RelationDesc, Row};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::controller::{CollectionMetadata, StorageError};
use mz_storage_types::instances::StorageInstanceId;
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::read_policy::ReadPolicy;
use mz_storage_types::sinks::{MetadataUnfilled, StorageSinkConnection, StorageSinkDesc};
use mz_storage_types::sources::{IngestionDescription, SourceData};
use serde::{Deserialize, Serialize};
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use tokio::sync::mpsc::error::TrySendError;
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
    ComputeReplicaHeartbeats,
    ComputeHydrationStatus,
    ComputeOperatorHydrationStatus,

    // Written by the Adapter for tracking AWS PrivateLink Connection Status History
    PrivatelinkConnectionStatusHistory,
}

/// Describes how data is written to the collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataSource {
    /// Ingest data from some external source.
    Ingestion(IngestionDescription),
    /// Data comes from introspection sources, which the controller itself is
    /// responsible for generating.
    Introspection(IntrospectionType),
    /// Data comes from the source's remapping/reclock operator.
    Progress,
    /// Data comes from external HTTP requests pushed to Materialize.
    Webhook,
    /// This source's data is does not need to be managed by the storage
    /// controller, e.g. it's a materialized view, table, or subsource.
    // TODO? Add a means to track some data sources' GlobalIds.
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
    /// Some other sources writes this data, i.e. a subsource.
    Source,
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
    /// Returns IDs for all storage objects that this `CollectionDescription`
    /// depends on.
    pub fn get_storage_dependencies(&self) -> Vec<GlobalId> {
        let mut result = Vec::new();

        // NOTE: Exhaustive match for future proofing.
        match &self.data_source {
            DataSource::Ingestion(ingestion) => {
                result.push(ingestion.remap_collection_id);
            }
            DataSource::Webhook | DataSource::Introspection(_) | DataSource::Progress => {
                // Introspection, Progress, and Webhook sources have no dependencies, for now.
                //
                // TODO(parkmycar): Once webhook sources support validation, then they will have
                // dependencies.
                //
                // See <https://github.com/MaterializeInc/materialize/issues/20211>.
            }
            DataSource::Other(_) => {
                // We don't know anything about it's dependencies.
            }
        }

        result
    }

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

#[async_trait]
pub trait StorageTxn {
    /// Retrieve all of the visible storage metadata.
    ///
    /// The value of this map should be treated as opaque.
    fn get_storage_metadata(&self) -> BTreeMap<GlobalId, String>;

    /// Add new storage metadata.
    ///
    /// Subsequent calls to `get_storage_metadata` must include this data.
    fn insert_storage_metadata(
        &mut self,
        s: BTreeMap<GlobalId, String>,
    ) -> Result<(), StorageError>;

    /// Remove the metadata associated with the identified collections.
    ///
    /// Returns values of the removed metadata.
    fn delete_storage_metadata(&mut self, ids: BTreeSet<GlobalId>) -> Vec<(GlobalId, String)>;

    /// Retrieve all of the shards that are no longer in use by an active
    /// collection but are yet to be finalized.
    fn get_unfinalized_shards(&self) -> BTreeSet<String>;

    /// Insert the specified values as unfinalized shards.
    fn insert_unfinalized_shards(&mut self, s: BTreeSet<String>) -> Result<(), StorageError>;

    /// Mark the specified shards as finalized, deleting them from the
    /// unfinalized shard collection.
    fn mark_shards_as_finalized(&mut self, shards: BTreeSet<String>);

    /// Get the persist txn shard for this environment if it exists.
    fn get_persist_txn_shard(&self) -> Option<String>;

    /// Store the specified shard as the environment's persist txn shard.
    ///
    /// The implementor should error if the shard is already specified.
    fn write_persist_txn_shard(&mut self, shard: String) -> Result<(), StorageError>;
}

#[async_trait(?Send)]
pub trait StorageController: Debug {
    type Timestamp;

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

    /// Acquire an immutable reference to the collection state, should it exist.
    fn collection(&self, id: GlobalId) -> Result<&CollectionState<Self::Timestamp>, StorageError>;

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

    /// Acquire a mutable reference to the collection state, should it exist.
    fn collection_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut CollectionState<Self::Timestamp>, StorageError>;

    /// Acquire an iterator over all collection states.
    fn collections(
        &self,
    ) -> Box<dyn Iterator<Item = (&GlobalId, &CollectionState<Self::Timestamp>)> + '_>;

    /// Create the sources described in the individual RunIngestionCommand commands.
    ///
    /// Each command carries the source id, the source description, and any associated metadata
    /// needed to ingest the particular source.
    ///
    /// This command installs collection state for the indicated sources, and the are
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
        register_ts: Option<Self::Timestamp>,
        collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError>;

    /// Check that the collection associated with `id` can be altered to represent the given
    /// `ingestion`.
    ///
    /// Note that this check is optimistic and its return of `Ok(())` does not guarantee that
    /// subsequent calls to `alter_collection` are guaranteed to succeed.
    fn check_alter_collection(
        &mut self,
        collections: &BTreeMap<GlobalId, IngestionDescription>,
    ) -> Result<(), StorageError>;

    /// Alter the identified collection to use the described ingestion.
    async fn alter_collection(
        &mut self,
        collections: BTreeMap<GlobalId, IngestionDescription>,
    ) -> Result<(), StorageError>;

    /// Acquire an immutable reference to the export state, should it exist.
    fn export(&self, id: GlobalId) -> Result<&ExportState<Self::Timestamp>, StorageError>;

    /// Acquire a mutable reference to the export state, should it exist.
    fn export_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut ExportState<Self::Timestamp>, StorageError>;

    /// Create the sinks described by the `ExportDescription`.
    async fn create_exports(
        &mut self,
        exports: Vec<(GlobalId, ExportDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError>;

    /// For each identified export, update its `StorageSinkConnection`.
    async fn update_export_connection(
        &mut self,
        exports: BTreeMap<GlobalId, StorageSinkConnection>,
    ) -> Result<(), StorageError>;

    /// Drops the read capability for the sources and allows their resources to be reclaimed.
    fn drop_sources(&mut self, identifiers: Vec<GlobalId>) -> Result<(), StorageError>;

    /// Drops the read capability for the sinks and allows their resources to be reclaimed.
    fn drop_sinks(&mut self, identifiers: Vec<GlobalId>) -> Result<(), StorageError>;

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
    fn drop_sources_unvalidated(&mut self, identifiers: Vec<GlobalId>);

    /// Append `updates` into the local input named `id` and advance its upper to `upper`.
    ///
    /// The method returns a oneshot that can be awaited to indicate completion of the write.
    /// The method may return an error, indicating an immediately visible error, and also the
    /// oneshot may return an error if one is encountered during the write.
    // TODO(petrosagg): switch upper to `Antichain<Timestamp>`
    fn append_table(
        &mut self,
        write_ts: Self::Timestamp,
        advance_to: Self::Timestamp,
        commands: Vec<(GlobalId, Vec<TimestamplessUpdate>)>,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<(), StorageError>>, StorageError>;

    /// Returns a [`MonotonicAppender`] which is a channel that can be used to monotonically
    /// append to the specified [`GlobalId`].
    fn monotonic_appender(&self, id: GlobalId) -> Result<MonotonicAppender, StorageError>;

    /// Returns a shared [`WebhookStatistics`] which can be used to report user-facing
    /// statistics for this given webhhook, specified by the [`GlobalId`].
    ///
    // This is used to support a fairly special case, where a source needs to report statistics
    // from outside the ordinary controller-clusterd path. Its possible to merge this with
    // `monotonic_appender`, whose only current user is webhooks, but given that they will
    // likely be moved to clusterd, we just leave this a special case.
    fn webhook_statistics(&self, id: GlobalId) -> Result<Arc<WebhookStatistics>, StorageError>;

    /// Returns the snapshot of the contents of the local input named `id` at `as_of`.
    async fn snapshot(
        &mut self,
        id: GlobalId,
        as_of: Self::Timestamp,
    ) -> Result<Vec<(Row, Diff)>, StorageError>;

    /// Returns the snapshot of the contents of the local input named `id` at `as_of`.
    async fn snapshot_cursor(
        &mut self,
        id: GlobalId,
        as_of: Self::Timestamp,
    ) -> Result<SnapshotCursor<Self::Timestamp>, StorageError>
    where
        Self::Timestamp: Codec64 + Timestamp + Lattice;

    /// Returns aggregate statistics about the contents of the local input named
    /// `id` at `as_of`.
    async fn snapshot_stats(
        &self,
        id: GlobalId,
        as_of: Antichain<Self::Timestamp>,
    ) -> Result<SnapshotStats, StorageError>;

    /// Returns aggregate statistics about the contents of the local input named
    /// `id` at `as_of`.
    async fn snapshot_parts_stats(
        &self,
        id: GlobalId,
        as_of: Antichain<Self::Timestamp>,
    ) -> Result<SnapshotPartsStats, StorageError>;

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

    /// Ingests write frontier updates for collections that this controller
    /// maintains and potentially generates updates to read capabilities, which
    /// are passed on to [`StorageController::update_read_capabilities`].
    ///
    /// These updates come from the entity that is responsible for writing to
    /// the collection, and in turn advancing its `upper` (aka
    /// `write_frontier`). The most common such "writers" are:
    ///
    /// * `clusterd` instances, for source ingestions
    ///
    /// * introspection collections (which this controller writes to)
    ///
    /// * Tables (which are written to by this controller)
    ///
    /// * Materialized Views, which are running inside COMPUTE, and for which
    /// COMPUTE sends updates to this storage controller
    ///
    /// The so-called "implied capability" is a read capability for a collection
    /// that is updated based on the write frontier and the collections
    /// [`ReadPolicy`]. Advancing the write frontier might change this implied
    /// capability, which in turn might change the overall `since` (a
    /// combination of all read capabilities) of a collection.
    fn update_write_frontiers(&mut self, updates: &[(GlobalId, Antichain<Self::Timestamp>)]);

    /// Applies `updates` and sends any appropriate compaction command.
    fn update_read_capabilities(
        &mut self,
        updates: &mut BTreeMap<GlobalId, ChangeBatch<Self::Timestamp>>,
    );

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
    ///
    /// This method is guaranteed to return "quickly" unless doing so would
    /// compromise the correctness of the system.
    ///
    /// This method is **not** guaranteed to be cancellation safe. It **must**
    /// be awaited to completion.
    async fn process(&mut self) -> Result<Option<Response<Self::Timestamp>>, anyhow::Error>;

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

    /// Records the current read and write frontiers of all known storage objects.
    ///
    /// The provided `external_frontiers` are merged with the frontiers known to
    /// the storage controller. If `external_frontiers` contains entries with
    /// object IDs that are known to storage controller, the storage
    /// controller's frontiers take precedence. The rationale is that the
    /// storage controller should be the authority on frontiers of storage
    /// objects, not the caller of this method.
    async fn record_frontiers(
        &mut self,
        external_frontiers: BTreeMap<
            GlobalId,
            (Antichain<Self::Timestamp>, Antichain<Self::Timestamp>),
        >,
    );

    /// Records the current per-replica write frontiers of all known storage objects.
    ///
    /// The provided `external_frontiers` are merged with the frontiers known to
    /// the storage controller. If `external_frontiers` contains entries with
    /// object IDs that are known to storage controller, the storage
    /// controller's frontiers take precedence. The rationale is that the
    /// storage controller should be the authority on frontiers of storage
    /// objects, not the caller of this method.
    async fn record_replica_frontiers(
        &mut self,
        external_frontiers: BTreeMap<(GlobalId, ReplicaId), Antichain<Self::Timestamp>>,
    );

    /// Records updates for the given introspection type.
    ///
    /// Rows passed in `updates` MUST have the correct schema for the given introspection type,
    /// as readers rely on this and might panic otherwise.
    async fn record_introspection_updates(
        &mut self,
        type_: IntrospectionType,
        updates: Vec<(Row, Diff)>,
    );

    /// Resets the txns system to a set of invariants necessary for correctness.
    ///
    /// Must be called on boot before create_collections or the various appends.
    /// This is true _regardless_ of whether the persist-txn feature is on or
    /// not. See the big comment in the impl of the method for details. Ideally,
    /// this would have just been folded into `Controller::new`, but it needs
    /// the timestamp and there are boot dependency issues.
    ///
    /// TODO: This can be removed once we've flipped to the new txns system for
    /// good and there is no possibility of the old code running concurrently
    /// with the new code.
    async fn init_txns(&mut self, init_ts: Self::Timestamp) -> Result<(), StorageError>;

    /// On boot, seed our set of collections with this data.
    ///
    /// Most likely, this data comes from a persisted source outside of the
    /// storage controller.
    async fn initialize_state(
        &mut self,
        txn: &mut dyn StorageTxn,
        init_ids: BTreeSet<GlobalId>,
        drop_ids: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError>;

    /// Provisionally synchronize the storage controller state with the
    /// implementor of [`StorageTxn`].
    ///
    /// We require an explicit list of IDs to add and drop because no
    /// implementor of `StorageTxn` can express to us these items' values using
    /// its own APIs.
    ///
    /// The storage controller expects the caller to either:
    /// - Call `create_collections` for each `GlobalId` in `ids_to_add` and
    ///   `drop_sources_unvalidated` for each in `ids_to_drop`.
    /// - Call `clear_provisional_state`. This is the case if the operation
    ///   preparing the collections fails.
    async fn provisionally_synchronize_state(
        &mut self,
        txn: &mut dyn StorageTxn,
        ids_to_add: BTreeSet<GlobalId>,
        ids_to_drop: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError>;

    /// Clears any state generated from
    /// [`StorageController::provisionally_synchronize_state`].
    fn clear_provisional_state(&mut self);

    /// Opportunistically clean up any state that is no longer ambiguous if the
    /// last call to [`StorageController::provisionally_synchronize_state`]
    /// committed.
    fn mark_state_synchronized(&mut self, txn: &dyn StorageTxn);
}

/// State maintained about individual collections.
#[derive(Debug)]
pub struct CollectionState<T> {
    /// Description with which the collection was created
    pub description: CollectionDescription<T>,

    /// Accumulation of read capabilities for the collection.
    ///
    /// This accumulation will always contain `self.implied_capability`, but may also contain
    /// capabilities held by others who have read dependencies on this collection.
    pub read_capabilities: MutableAntichain<T>,
    /// The implicit capability associated with collection creation.  This should never be less
    /// than the since of the associated persist collection.
    pub implied_capability: Antichain<T>,
    /// The policy to use to downgrade `self.implied_capability`.
    pub read_policy: ReadPolicy<T>,

    /// Storage identifiers on which this collection depends.
    pub storage_dependencies: Vec<GlobalId>,

    /// Reported write frontier.
    pub write_frontier: Antichain<T>,

    pub collection_metadata: CollectionMetadata,
}

impl<T: Timestamp> CollectionState<T> {
    /// Creates a new collection state, with an initial read policy valid from `since`.
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

    /// Returns the cluster to which the collection is bound, if applicable.
    pub fn cluster_id(&self) -> Option<StorageInstanceId> {
        match &self.description.data_source {
            DataSource::Ingestion(ingestion) => Some(ingestion.instance_id),
            DataSource::Webhook
            | DataSource::Introspection(_)
            | DataSource::Other(_)
            | DataSource::Progress => None,
        }
    }

    /// Returns whether the collection was dropped.
    pub fn is_dropped(&self) -> bool {
        self.read_capabilities.is_empty()
    }
}

/// State maintained about individual exports.
#[derive(Debug, Clone)]
pub struct ExportState<T> {
    /// Description with which the export was created
    pub description: ExportDescription<T>,

    /// The capability (hold on the since) that this export needs from its
    /// dependencies (inputs). When the upper of the export changes, we
    /// downgrade this, which in turn downgrades holds we have on our
    /// dependencies' sinces.
    pub read_capability: Antichain<T>,

    /// The policy to use to downgrade `self.read_capability`.
    pub read_policy: ReadPolicy<T>,

    /// Storage identifiers on which this collection depends.
    pub storage_dependencies: Vec<GlobalId>,

    /// Reported write frontier.
    pub write_frontier: Antichain<T>,
}

impl<T: Timestamp> ExportState<T> {
    pub fn new(
        description: ExportDescription<T>,
        read_capability: Antichain<T>,
        read_policy: ReadPolicy<T>,
        storage_dependencies: Vec<GlobalId>,
    ) -> Self {
        Self {
            description,
            read_capability,
            read_policy,
            storage_dependencies,
            write_frontier: Antichain::from_elem(Timestamp::minimum()),
        }
    }

    /// Returns the cluster to which the export is bound.
    pub fn cluster_id(&self) -> StorageInstanceId {
        self.description.instance_id
    }

    /// Returns whether the export was dropped.
    pub fn is_dropped(&self) -> bool {
        self.read_capability.is_empty()
    }
}
/// A channel that allows you to append a set of updates to a pre-defined [`GlobalId`].
///
/// See `CollectionManager::monotonic_appender` to acquire a [`MonotonicAppender`].
#[derive(Clone, Debug)]
pub struct MonotonicAppender {
    /// Channel that sends to a [`tokio::task`] which pushes updates to Persist.
    tx: mpsc::Sender<(Vec<(Row, Diff)>, oneshot::Sender<Result<(), StorageError>>)>,
}

impl MonotonicAppender {
    pub fn new(
        tx: mpsc::Sender<(Vec<(Row, Diff)>, oneshot::Sender<Result<(), StorageError>>)>,
    ) -> Self {
        MonotonicAppender { tx }
    }

    pub async fn append(&self, updates: Vec<(Row, Diff)>) -> Result<(), StorageError> {
        let (tx, rx) = oneshot::channel();

        // Make sure there is space available on the channel.
        let permit = self.tx.try_reserve().map_err(|e| {
            let msg = "collection manager";
            match e {
                TrySendError::Full(_) => StorageError::ResourceExhausted(msg),
                TrySendError::Closed(_) => StorageError::ShuttingDown(msg),
            }
        })?;

        // Send our update to the CollectionManager.
        permit.send((updates, tx));

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
