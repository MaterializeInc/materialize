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

use std::any::Any;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Debug};
use std::num::NonZeroI64;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use bytes::BufMut;
use derivative::Derivative;
use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use proptest::prelude::{any, Arbitrary, BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use prost::Message;
use serde::{Deserialize, Serialize};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use tokio::sync::Mutex;
use tokio_stream::StreamMap;
use tracing::{debug, info};

use mz_build_info::BuildInfo;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::soft_assert;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::critical::SinceHandle;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{PersistClient, PersistLocation, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::{Codec64, Opaque};
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{Datum, Diff, GlobalId, RelationDesc, Row, TimestampManipulation};
use mz_stash::{self, AppendBatch, StashError, StashFactory, TypedCollection};

use crate::client::{
    CreateSinkCommand, CreateSourceCommand, ProtoStorageCommand, ProtoStorageResponse,
    SinkStatisticsUpdate, SourceStatisticsUpdate, StorageCommand, StorageResponse, Update,
};
use crate::controller::rehydration::RehydratingStorageClient;
use crate::healthcheck;
use crate::metrics::StorageControllerMetrics;
use crate::types::errors::DataflowError;
use crate::types::instances::StorageInstanceId;
use crate::types::parameters::StorageParameters;
use crate::types::sinks::{
    MetadataUnfilled, ProtoDurableExportMetadata, SinkAsOf, StorageSinkDesc,
};
use crate::types::sources::{IngestionDescription, SourceData, SourceExport};

mod collection_mgmt;
mod command_wals;
mod persist_handles;
mod rehydration;
mod statistics;

include!(concat!(env!("OUT_DIR"), "/mz_storage_client.controller.rs"));

pub static METADATA_COLLECTION: TypedCollection<GlobalId, DurableCollectionMetadata> =
    TypedCollection::new("storage-collection-metadata");

pub static METADATA_EXPORT: TypedCollection<GlobalId, DurableExportMetadata<mz_repr::Timestamp>> =
    TypedCollection::new("storage-export-metadata-u64");

pub static ALL_COLLECTIONS: &[&str] = &[
    METADATA_COLLECTION.name(),
    METADATA_EXPORT.name(),
    command_wals::SHARD_FINALIZATION.name(),
];

// Do this dance so that we keep the storage controller expressed in terms of a generic timestamp `T`.
struct MetadataExportFetcher;
trait MetadataExport<T>
where
    // Associated type would be better but you can't express this relationship without unstable
    DurableExportMetadata<T>: mz_stash::Data,
{
    fn get_stash_collection() -> &'static TypedCollection<GlobalId, DurableExportMetadata<T>>;
}

impl MetadataExport<mz_repr::Timestamp> for MetadataExportFetcher {
    fn get_stash_collection(
    ) -> &'static TypedCollection<GlobalId, DurableExportMetadata<mz_repr::Timestamp>> {
        &METADATA_EXPORT
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum IntrospectionType {
    /// We're not responsible for appending to this collection automatically, but we should
    /// automatically bump the write frontier from time to time.
    SinkStatusHistory,
    SourceStatusHistory,
    ShardMapping,

    // Note that this single-shard introspection source will be changed to per-replica,
    // once we allow multiplexing multiple sources/sinks on a single cluster.
    StorageSourceStatistics,
    StorageSinkStatistics,
}

/// Describes how data is written to the collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataSource {
    /// Ingest data from some external source.
    Ingestion(IngestionDescription),
    /// Data comes from introspection sources, which the controller itself is
    /// responsible for generating.
    Introspection(IntrospectionType),
    /// This source's data is does not need to be managed by the storage
    /// controller, e.g. it's a materialized view, table, or subsource.
    // TODO? Add a means to track some data sources' GlobalIds.
    Other,
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

impl<T> From<RelationDesc> for CollectionDescription<T> {
    fn from(desc: RelationDesc) -> Self {
        Self {
            desc,
            data_source: DataSource::Other,
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

/// Opaque token to ensure `prepare_export` is called before `create_exports`.  This token proves
/// that compaction is being held back on `from_id` at least until `id` is created.  It should be
/// held while the AS OF is determined.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateExportToken {
    id: GlobalId,
    from_id: GlobalId,
}

impl CreateExportToken {
    /// Returns the ID of the export with which the token is associated.
    pub fn id(&self) -> GlobalId {
        self.id
    }
}

#[async_trait(?Send)]
pub trait StorageController: Debug + Send {
    type Timestamp;

    /// Marks the end of any initialization commands.
    ///
    /// The implementor may wait for this method to be called before implementing prior commands,
    /// and so it is important for a user to invoke this method as soon as it is comfortable.
    /// This method can be invoked immediately, at the potential expense of performance.
    fn initialization_complete(&mut self);

    /// Update storage configuration.
    fn update_configuration(&mut self, config_params: StorageParameters);

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
    fn connect_replica(&mut self, id: StorageInstanceId, addr: String);

    /// Acquire a mutable reference to the collection state, should it exist.
    fn collection_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut CollectionState<Self::Timestamp>, StorageError>;

    /// Acquire an iterator over all collection states.
    fn collections(
        &self,
    ) -> Box<dyn Iterator<Item = (&GlobalId, &CollectionState<Self::Timestamp>)> + '_>;

    /// Create the sources described in the individual CreateSourceCommand commands.
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
    async fn create_collections(
        &mut self,
        collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
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
        exports: Vec<(CreateExportToken, ExportDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError>;

    /// Notify the storage controller to prepare for an export to be created
    fn prepare_export(
        &mut self,
        id: GlobalId,
        from_id: GlobalId,
    ) -> Result<CreateExportToken, StorageError>;

    /// Cancel the pending export
    fn cancel_prepare_export(&mut self, token: CreateExportToken);

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
    fn append(
        &mut self,
        commands: Vec<(GlobalId, Vec<Update<Self::Timestamp>>, Self::Timestamp)>,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<(), StorageError>>, StorageError>;

    /// Returns the snapshot of the contents of the local input named `id` at `as_of`.
    async fn snapshot(
        &self,
        id: GlobalId,
        as_of: Self::Timestamp,
    ) -> Result<Vec<(Row, Diff)>, StorageError>;

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
    async fn process(&mut self) -> Result<(), anyhow::Error>;

    /// Signal to the controller that the adapter has populated all of its
    /// initial state and the controller can reconcile (i.e. drop) any unclaimed
    /// resources.
    async fn reconcile_state(&mut self);
}

/// Compaction policies for collections maintained by `Controller`.
///
/// NOTE(benesch): this might want to live somewhere besides the storage crate,
/// because it is fundamental to both storage and compute.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub enum ReadPolicy<T> {
    /// Maintain the collection as valid from this frontier onward.
    ValidFrom(Antichain<T>),
    /// Maintain the collection as valid from a function of the write frontier.
    ///
    /// This function will only be re-evaluated when the write frontier changes.
    /// If the intended behavior is to change in response to external signals,
    /// consider using the `ValidFrom` variant to manually pilot compaction.
    ///
    /// The `Arc` makes the function cloneable.
    LagWriteFrontier(
        #[derivative(Debug = "ignore")] Arc<dyn Fn(AntichainRef<T>) -> Antichain<T> + Send + Sync>,
    ),
    /// Allows one to express multiple read policies, taking the least of
    /// the resulting frontiers.
    Multiple(Vec<ReadPolicy<T>>),
}

impl ReadPolicy<mz_repr::Timestamp> {
    /// Creates a read policy that lags the write frontier by the indicated amount, rounded down to a multiple of that amount.
    ///
    /// The rounding down is done to reduce the number of changes the capability undergoes, with the thinking
    /// being that if you are ok with `lag`, then getting something between `lag` and `2 x lag` should be ok.
    pub fn lag_writes_by(lag: mz_repr::Timestamp) -> Self {
        Self::LagWriteFrontier(Arc::new(move |upper| {
            if upper.is_empty() {
                Antichain::from_elem(Timestamp::minimum())
            } else {
                // Subtract the lag from the time, and then round down to a multiple thereof to cut chatter.
                let mut time = upper[0];
                if lag != mz_repr::Timestamp::default() {
                    time = time.saturating_sub(lag);
                    time = time.saturating_sub(time % lag);
                }
                Antichain::from_elem(time)
            }
        }))
    }
}

impl<T: Timestamp> ReadPolicy<T> {
    pub fn frontier(&self, write_frontier: AntichainRef<T>) -> Antichain<T> {
        match self {
            ReadPolicy::ValidFrom(frontier) => frontier.clone(),
            ReadPolicy::LagWriteFrontier(logic) => logic(write_frontier),
            ReadPolicy::Multiple(policies) => {
                let mut frontier = Antichain::new();
                for policy in policies.iter() {
                    for time in policy.frontier(write_frontier).iter() {
                        frontier.insert(time.clone());
                    }
                }
                frontier
            }
        }
    }
}

/// Metadata required by a storage instance to read a storage collection
#[derive(Arbitrary, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionMetadata {
    /// The persist location where the shards are located.
    pub persist_location: PersistLocation,
    /// The persist shard id of the remap collection used to reclock this collection.
    pub remap_shard: ShardId,
    /// The persist shard containing the contents of this storage collection.
    pub data_shard: ShardId,
    /// The persist shard containing the status updates for this storage collection.
    pub status_shard: Option<ShardId>,
    /// The `RelationDesc` that describes the contents of the `data_shard`.
    pub relation_desc: RelationDesc,
}

impl RustType<ProtoCollectionMetadata> for CollectionMetadata {
    fn into_proto(&self) -> ProtoCollectionMetadata {
        ProtoCollectionMetadata {
            blob_uri: self.persist_location.blob_uri.clone(),
            consensus_uri: self.persist_location.consensus_uri.clone(),
            data_shard: self.data_shard.to_string(),
            remap_shard: self.remap_shard.to_string(),
            status_shard: self.status_shard.map(|s| s.to_string()),
            relation_desc: Some(self.relation_desc.into_proto()),
        }
    }

    fn from_proto(value: ProtoCollectionMetadata) -> Result<Self, TryFromProtoError> {
        Ok(CollectionMetadata {
            persist_location: PersistLocation {
                blob_uri: value.blob_uri,
                consensus_uri: value.consensus_uri,
            },
            remap_shard: value
                .remap_shard
                .parse()
                .map_err(TryFromProtoError::InvalidShardId)?,
            data_shard: value
                .data_shard
                .parse()
                .map_err(TryFromProtoError::InvalidShardId)?,
            status_shard: value
                .status_shard
                .map(|s| s.parse().map_err(TryFromProtoError::InvalidShardId))
                .transpose()?,
            relation_desc: value
                .relation_desc
                .into_rust_if_some("ProtoCollectionMetadata::relation_desc")?,
        })
    }
}

/// A trait that is used to calculate safe _resumption frontiers_ for a source.
///
/// Use [`ResumptionFrontierCalculator::initialize_state`] for creating an
/// opaque state that you should keep around. Then repeatedly call
/// [`ResumptionFrontierCalculator::calculate_resumption_frontier`] with the
/// state to efficiently calculate an up-to-date frontier.
#[async_trait]
pub trait ResumptionFrontierCalculator<T> {
    /// Opaque state that a `ResumptionFrontierCalculator` needs to repeatedly
    /// (and efficiently) calculate a _resumption frontier_.
    type State;

    /// Creates an opaque state type that can be used to efficiently calculate a
    /// new _resumption frontier_ when needed.
    async fn initialize_state(&self, client_cache: &mut PersistClientCache) -> Self::State;

    /// Calculates a new, safe _resumption frontier_.
    async fn calculate_resumption_frontier(&self, state: &mut Self::State) -> Antichain<T>;
}

/// The subset of [`CollectionMetadata`] that must be durable stored.
#[derive(Arbitrary, Clone, Debug, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize)]
pub struct DurableCollectionMetadata {
    // See the comments on [`CollectionMetadata`].
    pub remap_shard: ShardId,
    pub data_shard: ShardId,
}

impl RustType<ProtoDurableCollectionMetadata> for DurableCollectionMetadata {
    fn into_proto(&self) -> ProtoDurableCollectionMetadata {
        ProtoDurableCollectionMetadata {
            remap_shard: self.remap_shard.to_string(),
            data_shard: self.data_shard.to_string(),
        }
    }

    fn from_proto(value: ProtoDurableCollectionMetadata) -> Result<Self, TryFromProtoError> {
        Ok(DurableCollectionMetadata {
            remap_shard: value
                .remap_shard
                .parse()
                .map_err(TryFromProtoError::InvalidShardId)?,
            data_shard: value
                .data_shard
                .parse()
                .map_err(TryFromProtoError::InvalidShardId)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableExportMetadata<T> {
    pub initial_as_of: SinkAsOf<T>,
}

impl PartialOrd for DurableExportMetadata<mz_repr::Timestamp> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Ord for DurableExportMetadata<mz_repr::Timestamp> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let mut s = vec![];
        let mut o = vec![];
        self.encode(&mut s);
        other.encode(&mut o);
        s.cmp(&o)
    }
}

impl RustType<ProtoDurableExportMetadata> for DurableExportMetadata<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoDurableExportMetadata {
        ProtoDurableExportMetadata {
            initial_as_of: Some(self.initial_as_of.into_proto()),
        }
    }

    fn from_proto(proto: ProtoDurableExportMetadata) -> Result<Self, TryFromProtoError> {
        Ok(DurableExportMetadata {
            initial_as_of: proto
                .initial_as_of
                .into_rust_if_some("ProtoDurableExportMetadata::initial_as_of")?,
        })
    }
}

impl DurableExportMetadata<mz_repr::Timestamp> {
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        self.into_proto()
            .encode(buf)
            .expect("no required fields means no initialization errors");
    }

    pub fn decode(buf: &[u8]) -> Result<Self, String> {
        let proto = ProtoDurableExportMetadata::decode(buf).map_err(|err| err.to_string())?;
        proto.into_rust().map_err(|err| err.to_string())
    }
}

impl Arbitrary for DurableExportMetadata<mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (any::<SinkAsOf<mz_repr::Timestamp>>(),)
            .prop_map(|(initial_as_of,)| Self { initial_as_of })
            .boxed()
    }
}

/// Controller state maintained for each storage instance.
#[derive(Debug)]
pub struct StorageControllerState<T: Timestamp + Lattice + Codec64 + TimestampManipulation> {
    /// A function that returns the current time.
    now: NowFn,
    /// The fencing token for this instance of the controller.
    envd_epoch: NonZeroI64,

    /// Collections maintained by the storage controller.
    ///
    /// This collection only grows, although individual collections may be rendered unusable.
    /// This is to prevent the re-binding of identifiers to other descriptions.
    pub(super) collections: BTreeMap<GlobalId, CollectionState<T>>,
    pub(super) exports: BTreeMap<GlobalId, ExportState<T>>,
    pub(super) exported_collections: BTreeMap<GlobalId, Vec<GlobalId>>,
    pub(super) stash: mz_stash::Stash,
    /// Write handle for persist shards.
    pub(super) persist_write_handles: persist_handles::PersistWriteWorker<T>,
    /// Read handles for persist shards.
    ///
    /// These handles are on the other end of a Tokio task, so that work can be done asynchronously
    /// without blocking the storage controller.
    persist_read_handles: persist_handles::PersistReadWorker<T>,
    stashed_response: Option<StorageResponse<T>>,
    /// IDs of sources that were dropped whose statuses should be
    /// updated during the next call to `StorageController::process`.
    pending_source_drops: Vec<GlobalId>,
    /// IDs of sinks that were dropped whose statuses should be
    /// updated during the next call to `StorageController::process`.
    pending_sink_drops: Vec<GlobalId>,
    /// Compaction commands to send during the next call to
    /// `StorageController::process`.
    pending_compaction_commands: Vec<(GlobalId, Antichain<T>)>,

    /// Interface for managed collections
    pub(super) collection_manager: collection_mgmt::CollectionManager,
    /// Tracks which collection is responsible for which [`IntrospectionType`].
    pub(super) introspection_ids: BTreeMap<IntrospectionType, GlobalId>,
    /// Tokens for tasks that drive updating introspection collections. Dropping
    /// this will make sure that any tasks (or other resources) will stop when
    /// needed.
    // TODO(aljoscha): Should these live somewhere else?
    introspection_tokens: BTreeMap<GlobalId, Box<dyn Any + Send + Sync>>,

    /// Consolidated metrics updates to periodically write. We do not eagerly initialize this,
    /// and its contents are entirely driven by `StorageResponse::StatisticsUpdates`'s.
    source_statistics:
        Arc<std::sync::Mutex<BTreeMap<GlobalId, BTreeMap<usize, SourceStatisticsUpdate>>>>,
    /// Consolidated metrics updates to periodically write. We do not eagerly initialize this,
    /// and its contents are entirely driven by `StorageResponse::StatisticsUpdates`'s.
    sink_statistics:
        Arc<std::sync::Mutex<BTreeMap<GlobalId, BTreeMap<usize, SinkStatisticsUpdate>>>>,

    /// Clients for all known storage instances.
    clients: BTreeMap<StorageInstanceId, RehydratingStorageClient<T>>,
    /// Set to `true` once `initialization_complete` has been called.
    initialized: bool,
    /// Storage configuration to apply to newly provisioned instances.
    config: StorageParameters,
}

/// A storage controller for a storage instance.
#[derive(Debug)]
pub struct Controller<T: Timestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation>
{
    /// The build information for this process.
    build_info: &'static BuildInfo,
    /// The state for the storage controller.
    /// TODO(benesch): why is this a separate struct?
    state: StorageControllerState<T>,
    /// Mechanism for returning frontier advancement for tables.
    internal_response_queue: tokio::sync::mpsc::UnboundedReceiver<StorageResponse<T>>,
    /// The persist location where all storage collections are being written to
    persist_location: PersistLocation,
    /// A persist client used to write to storage collections
    persist: Arc<Mutex<PersistClientCache>>,
    /// Metrics of the Storage controller
    metrics: StorageControllerMetrics,
}

#[derive(Debug)]
pub enum StorageError {
    /// The source identifier was re-created after having been dropped,
    /// or installed with a different description.
    SourceIdReused(GlobalId),
    /// The source identifier is not present.
    IdentifierMissing(GlobalId),
    /// The update contained in the appended batch was at a timestamp equal or beyond the batch's upper
    UpdateBeyondUpper(GlobalId),
    /// The read was at a timestamp before the collection's since
    ReadBeforeSince(GlobalId),
    /// The expected upper of one or more appends was different from the actual upper of the collection
    InvalidUppers(Vec<GlobalId>),
    /// An error from the underlying client.
    ClientError(anyhow::Error),
    /// An operation failed to read or write state
    IOError(StashError),
    /// Dataflow was not able to process a request
    DataflowError(DataflowError),
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SourceIdReused(_) => None,
            Self::IdentifierMissing(_) => None,
            Self::UpdateBeyondUpper(_) => None,
            Self::ReadBeforeSince(_) => None,
            Self::InvalidUppers(_) => None,
            Self::ClientError(_) => None,
            Self::IOError(err) => Some(err),
            Self::DataflowError(err) => Some(err),
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("storage error: ")?;
        match self {
            Self::SourceIdReused(id) => write!(
                f,
                "source identifier was re-created after having been dropped: {id}"
            ),
            Self::IdentifierMissing(id) => write!(f, "collection identifier is not present: {id}"),
            Self::UpdateBeyondUpper(id) => {
                write!(
                    f,
                    "append batch for {id} contained update at or beyond its upper"
                )
            }
            Self::ReadBeforeSince(id) => {
                write!(f, "read for {id} was at a timestamp before its since")
            }
            Self::InvalidUppers(id) => {
                write!(
                    f,
                    "expected upper was different from the actual upper for: {}",
                    id.iter().map(|id| id.to_string()).join(", ")
                )
            }
            Self::ClientError(err) => write!(f, "underlying client error: {:#}", err),
            Self::IOError(err) => write!(f, "failed to read or write state: {err}"),
            Self::DataflowError(err) => write!(f, "dataflow failed to process request: {err}"),
        }
    }
}

impl From<anyhow::Error> for StorageError {
    fn from(error: anyhow::Error) -> Self {
        Self::ClientError(error)
    }
}

impl From<StashError> for StorageError {
    fn from(error: StashError) -> Self {
        Self::IOError(error)
    }
}

impl From<DataflowError> for StorageError {
    fn from(error: DataflowError) -> Self {
        Self::DataflowError(error)
    }
}

impl<T: Timestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation>
    StorageControllerState<T>
{
    pub(super) async fn new(
        postgres_url: String,
        tx: tokio::sync::mpsc::UnboundedSender<StorageResponse<T>>,
        now: NowFn,
        factory: &StashFactory,
        envd_epoch: NonZeroI64,
    ) -> Self {
        let tls = mz_postgres_util::make_tls(
            &tokio_postgres::config::Config::from_str(&postgres_url)
                .expect("invalid postgres url for storage stash"),
        )
        .expect("could not make storage TLS connection");
        let mut stash = factory
            .open(postgres_url, None, tls)
            .await
            .expect("could not connect to postgres storage stash");

        // Ensure all collections are initialized, otherwise they panic if
        // they're read before being written to.
        async fn maybe_get_init_batch<'tx, K, V>(
            tx: &'tx mz_stash::Transaction<'tx>,
            typed: &TypedCollection<K, V>,
        ) -> Option<AppendBatch>
        where
            K: mz_stash::Data,
            V: mz_stash::Data,
        {
            let collection = tx
                .collection::<K, V>(typed.name())
                .await
                .expect("named collection must exist");
            let upper = tx
                .upper(collection.id)
                .await
                .expect("collection known to exist");
            if upper.elements() == [mz_stash::Timestamp::MIN] {
                Some(
                    collection
                        .make_batch_lower(upper)
                        .expect("stash operation must succeed"),
                )
            } else {
                None
            }
        }

        stash
            .with_transaction(move |tx| {
                Box::pin(async move {
                    // Query all collections in parallel. Makes for triplicated
                    // names, but runs quick.
                    let (metadata_collection, metadata_export, shard_finalization) = futures::join!(
                        maybe_get_init_batch(&tx, &METADATA_COLLECTION),
                        maybe_get_init_batch(&tx, &METADATA_EXPORT),
                        maybe_get_init_batch(&tx, &command_wals::SHARD_FINALIZATION),
                    );
                    let batches: Vec<AppendBatch> =
                        [metadata_collection, metadata_export, shard_finalization]
                            .into_iter()
                            .filter_map(|b| b)
                            .collect();

                    tx.append(batches).await
                })
            })
            .await
            .expect("stash operation must succeed");

        let persist_write_handles = persist_handles::PersistWriteWorker::new(tx);
        let collection_manager_write_handle = persist_write_handles.clone();

        let collection_manager =
            collection_mgmt::CollectionManager::new(collection_manager_write_handle, now.clone());

        Self {
            collections: BTreeMap::default(),
            exports: BTreeMap::default(),
            exported_collections: BTreeMap::default(),
            stash,
            persist_write_handles,
            persist_read_handles: persist_handles::PersistReadWorker::new(),
            stashed_response: None,
            pending_source_drops: vec![],
            pending_sink_drops: vec![],
            pending_compaction_commands: vec![],
            collection_manager,
            introspection_ids: BTreeMap::new(),
            introspection_tokens: BTreeMap::new(),
            now,
            envd_epoch,
            source_statistics: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            sink_statistics: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            clients: BTreeMap::new(),
            initialized: false,
            config: StorageParameters::default(),
        }
    }
}

#[async_trait(?Send)]
impl<T> StorageController for Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,
    MetadataExportFetcher: MetadataExport<T>,
    DurableExportMetadata<T>: mz_stash::Data,
{
    type Timestamp = T;

    fn initialization_complete(&mut self) {
        self.state.initialized = true;
        for client in self.state.clients.values_mut() {
            client.send(StorageCommand::InitializationComplete);
        }
    }

    fn update_configuration(&mut self, config_params: StorageParameters) {
        // TODO(#16753): apply config to `self.persist`

        for client in self.state.clients.values_mut() {
            client.send(StorageCommand::UpdateConfiguration(config_params.clone()));
        }
        self.state.config.update(config_params);
    }

    fn collection(&self, id: GlobalId) -> Result<&CollectionState<Self::Timestamp>, StorageError> {
        self.state
            .collections
            .get(&id)
            .ok_or(StorageError::IdentifierMissing(id))
    }

    fn collection_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut CollectionState<Self::Timestamp>, StorageError> {
        self.state
            .collections
            .get_mut(&id)
            .ok_or(StorageError::IdentifierMissing(id))
    }

    fn collections(
        &self,
    ) -> Box<dyn Iterator<Item = (&GlobalId, &CollectionState<Self::Timestamp>)> + '_> {
        Box::new(self.state.collections.iter())
    }

    fn create_instance(&mut self, id: StorageInstanceId) {
        let mut client = RehydratingStorageClient::new(
            self.build_info,
            Arc::clone(&self.persist),
            self.metrics.for_instance(id),
        );
        if self.state.initialized {
            client.send(StorageCommand::InitializationComplete);
        }
        client.send(StorageCommand::UpdateConfiguration(
            self.state.config.clone(),
        ));
        let old_client = self.state.clients.insert(id, client);
        assert!(old_client.is_none(), "storage instance {id} already exists");
    }

    fn drop_instance(&mut self, id: StorageInstanceId) {
        let client = self.state.clients.remove(&id);
        assert!(client.is_some(), "storage instance {id} does not exist");
    }

    fn connect_replica(&mut self, id: StorageInstanceId, addr: String) {
        let client = self
            .state
            .clients
            .get_mut(&id)
            .unwrap_or_else(|| panic!("instance {id} does not exist"));
        client.connect(addr);
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn create_collections(
        &mut self,
        mut collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError> {
        // Validate first, to avoid corrupting state.
        // 1. create a dropped identifier, or
        // 2. create an existing identifier with a new description.
        // Make sure to check for errors within `ingestions` as well.
        collections.sort_by_key(|(id, _)| *id);
        collections.dedup();
        for pos in 1..collections.len() {
            if collections[pos - 1].0 == collections[pos].0 {
                return Err(StorageError::SourceIdReused(collections[pos].0));
            }
        }
        for (id, description) in collections.iter() {
            if let Ok(collection) = self.collection(*id) {
                if &collection.description != description {
                    return Err(StorageError::SourceIdReused(*id));
                }
            }
        }

        // Install collection state for each bound description.
        // Note that this method implementation attempts to do AS MUCH work
        // concurrently as possible. There are inline comments explaining the motivation
        // behind each section

        // Perform all stash writes in a single transaction, to minimize transaction overhead and
        // the time spent waiting for stash.
        METADATA_COLLECTION
            .insert_without_overwrite(
                &mut self.state.stash,
                collections.iter().map(|(id, _)| {
                    (
                        *id,
                        DurableCollectionMetadata {
                            remap_shard: ShardId::new(),
                            data_shard: ShardId::new(),
                        },
                    )
                }),
            )
            .await?;

        let mut durable_metadata = METADATA_COLLECTION.peek_one(&mut self.state.stash).await?;

        // We first enrich each collection description with some additional metadata...
        use futures::stream::{StreamExt, TryStreamExt};
        let enriched_with_metdata = collections.into_iter().map(|(id, description)| {
            let collection_shards = durable_metadata.remove(&id).expect("inserted above");
            let status_shard = if let Some(status_collection_id) = description.status_collection_id
            {
                Some(
                    durable_metadata
                        .get(&status_collection_id)
                        .ok_or(StorageError::IdentifierMissing(status_collection_id))?
                        .data_shard,
                )
            } else {
                None
            };

            let metadata = CollectionMetadata {
                persist_location: self.persist_location.clone(),
                remap_shard: collection_shards.remap_shard,
                data_shard: collection_shards.data_shard,
                status_shard,
                relation_desc: description.desc.clone(),
            };

            Ok((id, description, metadata))
        });

        // So that we can open `SinceHandle`s for each collections concurrently.
        let persist_client = self
            .persist
            .lock()
            .await
            .open(self.persist_location.clone())
            .await
            .unwrap();
        let persist_client = &persist_client;
        // Reborrow the `&mut self` as immutable, as all the concurrent work to be processed in
        // this stream cannot all have exclusive access.
        let this = &*self;
        let to_register: Vec<_> = futures::stream::iter(enriched_with_metdata)
            .map(|data: Result<_, anyhow::Error>| async move {
                let (id, description, metadata) = data?;

                // should be replaced with real introspection (https://github.com/MaterializeInc/materialize/issues/14266)
                // but for now, it's helpful to have this mapping written down somewhere
                debug!(
                    "mapping GlobalId={} to remap shard ({}), data shard ({}), status shard ({:?})",
                    id, metadata.remap_shard, metadata.data_shard, metadata.status_shard
                );

                let (write, since_handle) = this
                    .acquire_data_handles(
                        id,
                        metadata.data_shard.clone(),
                        metadata.relation_desc.clone(),
                        persist_client,
                    )
                    .await;

                let cs = CollectionState::new(
                    description.clone(),
                    since_handle.since().clone(),
                    write.upper().clone(),
                    metadata.clone(),
                );
                Ok::<_, anyhow::Error>((id, description, write, since_handle, cs))
            })
            // Poll each future for each collection concurrently, maximum of 50 at a time.
            .buffer_unordered(50)
            // HERE BE DRAGONS:
            //
            // There are at least 2 subtleties in using `FuturesUnordered` (which
            // `buffer_unordered` uses underneath:
            // - One is captured here <https://github.com/rust-lang/futures-rs/issues/2387>
            // - And the other is deadlocking if processing an OUTPUT of a `FuturesUnordered`
            // stream attempts to obtain an async mutex that is also obtained in the futures
            // being polled.
            //
            // Both of these could potentially be issues in all usages of `buffer_unordered` in
            // this method, so we stick the standard advice: only use `try_collect` or
            // `collect`!
            .try_collect()
            .await?;

        let mut to_create = Vec::with_capacity(to_register.len());
        // This work mutates the controller state, so must be done serially. Because there
        // is no io-bound work, its very fast.
        for (id, description, write, since_handle, collection_state) in to_register {
            self.state.persist_write_handles.register(id, write);
            self.state.persist_read_handles.register(id, since_handle);

            self.state.collections.insert(id, collection_state);

            to_create.push((id, description));
        }

        // Reborrow `&mut self` immutably, same reasoning as above.
        let this = &*self;

        this.register_shard_mappings(to_create.iter().map(|(id, _)| *id))
            .await;

        // TODO(guswynn): perform the io in this final section concurrently.
        for (id, description) in to_create {
            match description.data_source {
                DataSource::Ingestion(ingestion) => {
                    // Each ingestion is augmented with the collection metadata.
                    let mut source_imports = BTreeMap::new();
                    for (id, _) in ingestion.source_imports {
                        // This _requires_ that the sub-source collection (with
                        // `DataSource::Other`) was registered BEFORE we process this, the
                        // top-level collection.
                        let metadata = self.collection(id)?.collection_metadata.clone();
                        source_imports.insert(id, metadata);
                    }

                    // The ingestion metadata is simply the collection metadata of the collection with
                    // the associated ingestion
                    let ingestion_metadata = self.collection(id)?.collection_metadata.clone();

                    let mut source_exports = BTreeMap::new();
                    for (id, export) in ingestion.source_exports {
                        // Note that these metadata's have been previously enriched with the
                        // required `RelationDesc` for each sub-source above!
                        let storage_metadata = self.collection(id)?.collection_metadata.clone();
                        source_exports.insert(
                            id,
                            SourceExport {
                                storage_metadata,
                                output_index: export.output_index,
                            },
                        );
                    }

                    let desc = IngestionDescription {
                        source_imports,
                        source_exports,
                        ingestion_metadata,
                        // The rest of the fields are identical
                        desc: ingestion.desc,
                        instance_id: ingestion.instance_id,
                    };
                    let mut persist_clients = self.persist.lock().await;
                    let mut state = desc.initialize_state(&mut persist_clients).await;
                    let resume_upper = desc.calculate_resumption_frontier(&mut state).await;

                    // Fetch the client for this ingestion's instance.
                    let client = self
                        .state
                        .clients
                        .get_mut(&ingestion.instance_id)
                        .with_context(|| {
                            format!(
                                "instance {} missing for ingestion {}",
                                ingestion.instance_id, id
                            )
                        })?;
                    let augmented_ingestion = CreateSourceCommand {
                        id,
                        description: desc,
                        resume_upper,
                    };

                    client.send(StorageCommand::CreateSources(vec![augmented_ingestion]));
                }
                DataSource::Introspection(i) => {
                    let prev = self.state.introspection_ids.insert(i, id);
                    assert!(
                        prev.is_none(),
                        "cannot have multiple IDs for introspection type"
                    );

                    self.state.collection_manager.register_collection(id).await;

                    match i {
                        IntrospectionType::ShardMapping => {
                            self.initialize_shard_mapping().await;
                        }
                        IntrospectionType::StorageSourceStatistics => {
                            // Set the collection to empty.
                            self.reconcile_managed_collection(id, vec![]).await;

                            let scraper_token = statistics::spawn_statistics_scraper(
                                id.clone(),
                                // These do a shallow copy.
                                self.state.collection_manager.clone(),
                                Arc::clone(&self.state.source_statistics),
                            );

                            // Make sure this is dropped when the controller is
                            // dropped, so that the internal task will stop.
                            self.state.introspection_tokens.insert(id, scraper_token);
                        }
                        IntrospectionType::StorageSinkStatistics => {
                            // Set the collection to empty.
                            self.reconcile_managed_collection(id, vec![]).await;

                            let scraper_token = statistics::spawn_statistics_scraper(
                                id.clone(),
                                // These do a shallow copy.
                                self.state.collection_manager.clone(),
                                Arc::clone(&self.state.sink_statistics),
                            );

                            // Make sure this is dropped when the controller is
                            // dropped, so that the internal task will stop.
                            self.state.introspection_tokens.insert(id, scraper_token);
                        }
                        IntrospectionType::SourceStatusHistory
                        | IntrospectionType::SinkStatusHistory => {
                            // nothing to do: these collections are append only
                        }
                    }
                }
                DataSource::Other => {}
            }
        }

        Ok(())
    }

    fn export(&self, id: GlobalId) -> Result<&ExportState<Self::Timestamp>, StorageError> {
        self.state
            .exports
            .get(&id)
            .ok_or(StorageError::IdentifierMissing(id))
    }

    fn export_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut ExportState<Self::Timestamp>, StorageError> {
        self.state
            .exports
            .get_mut(&id)
            .ok_or(StorageError::IdentifierMissing(id))
    }

    fn prepare_export(
        &mut self,
        id: GlobalId,
        from_id: GlobalId,
    ) -> Result<CreateExportToken, StorageError> {
        if let Ok(_export) = self.export(id) {
            return Err(StorageError::SourceIdReused(id));
        }

        self.state
            .exported_collections
            .entry(from_id)
            .or_default()
            .push(id);

        Ok(CreateExportToken { id, from_id })
    }

    fn cancel_prepare_export(&mut self, CreateExportToken { id, from_id }: CreateExportToken) {
        self.state
            .exported_collections
            .get_mut(&from_id)
            // Internal logic error NOT due to export not existing
            .expect("Dangling exported collection")
            .retain(|from_export_id| *from_export_id != id);
    }

    async fn create_exports(
        &mut self,
        exports: Vec<(CreateExportToken, ExportDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError> {
        // Validate first, to avoid corrupting state.
        let mut dedup_hashmap = BTreeMap::<&_, &_>::new();
        for (CreateExportToken { id, from_id }, desc) in exports.iter() {
            if dedup_hashmap.insert(id, desc).is_some() {
                return Err(StorageError::SourceIdReused(*id));
            }
            if let Ok(export) = self.export(*id) {
                if &export.description != desc {
                    return Err(StorageError::SourceIdReused(*id));
                }
            }
            if desc.sink.from != *from_id {
                return Err(StorageError::SourceIdReused(*id));
            }
            if self
                .state
                .exported_collections
                .get(from_id)
                // Internal logic error NOT due to export not existing
                .expect("Dangling exported collection")
                .iter()
                .find(|from_export_id| *from_export_id == id)
                .is_none()
            {
                return Err(StorageError::SourceIdReused(*id));
            }
        }

        for (CreateExportToken { id, from_id }, description) in exports {
            self.state
                .exports
                .insert(id, ExportState::new(description.clone()));

            let from_collection = self.collection(from_id)?;
            let from_storage_metadata = from_collection.collection_metadata.clone();
            // We've added the dependency above in `exported_collections` so this guaranteed not to change at least
            // until the sink is started up.
            let from_since = from_collection.implied_capability.clone();

            let as_of = MetadataExportFetcher::get_stash_collection()
                .insert_key_without_overwrite(
                    &mut self.state.stash,
                    id,
                    DurableExportMetadata {
                        initial_as_of: description.sink.as_of,
                    },
                )
                .await?
                .initial_as_of
                .maybe_fast_forward(&from_since);

            let status_id = if let Some(status_collection_id) = description.sink.status_id {
                Some(
                    self.collection(status_collection_id)?
                        .collection_metadata
                        .data_shard,
                )
            } else {
                None
            };

            let cmd = CreateSinkCommand {
                id,
                description: StorageSinkDesc {
                    from: from_id,
                    from_desc: description.sink.from_desc,
                    connection: description.sink.connection,
                    envelope: description.sink.envelope,
                    as_of,
                    status_id,
                    from_storage_metadata,
                },
            };

            // Fetch the client for this exports's cluster.
            let client = self
                .state
                .clients
                .get_mut(&description.instance_id)
                .with_context(|| {
                    format!(
                        "cluster {} missing for export {}",
                        description.instance_id, id
                    )
                })?;

            client.send(StorageCommand::CreateSinks(vec![cmd]));
        }
        Ok(())
    }

    fn drop_sources(&mut self, identifiers: Vec<GlobalId>) -> Result<(), StorageError> {
        self.validate_collection_ids(identifiers.iter().cloned())?;
        self.drop_sources_unvalidated(identifiers);
        Ok(())
    }

    fn drop_sources_unvalidated(&mut self, identifiers: Vec<GlobalId>) {
        let policies = identifiers
            .into_iter()
            .map(|id| (id, ReadPolicy::ValidFrom(Antichain::new())))
            .collect();
        self.set_read_policy(policies);
    }

    /// Drops the read capability for the sinks and allows their resources to be reclaimed.
    fn drop_sinks(&mut self, identifiers: Vec<GlobalId>) -> Result<(), StorageError> {
        self.validate_export_ids(identifiers.iter().cloned())?;
        self.drop_sinks_unvalidated(identifiers);
        Ok(())
    }

    fn drop_sinks_unvalidated(&mut self, identifiers: Vec<GlobalId>) {
        for id in identifiers {
            let export = match self.export(id) {
                Ok(export) => export,
                Err(_) => continue,
            };
            let from = export.from();

            self.state
                .exported_collections
                .get_mut(&from)
                // Internal logic error NOT due to export not existing
                .expect("Dangling exported collection")
                .retain(|from_export_id| *from_export_id != id);

            // Remove sink by removing its write frontier and arranging for deprovisioning.
            self.update_write_frontiers(&[(id, Antichain::new())]);
            self.state.pending_sink_drops.push(id);
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn append(
        &mut self,
        commands: Vec<(GlobalId, Vec<Update<Self::Timestamp>>, Self::Timestamp)>,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<(), StorageError>>, StorageError> {
        // TODO(petrosagg): validate appends against the expected RelationDesc of the collection
        for (id, updates, batch_upper) in commands.iter() {
            for update in updates.iter() {
                if !update.timestamp.less_than(batch_upper) {
                    return Err(StorageError::UpdateBeyondUpper(*id));
                }
            }
        }

        Ok(self.state.persist_write_handles.append(commands))
    }

    // TODO(petrosagg): This signature is not very useful in the context of partially ordered times
    // where the as_of frontier might have multiple elements. In the current form the mutually
    // incomparable updates will be accumulated together to a state of the collection that never
    // actually existed. We should include the original time in the updates advanced by the as_of
    // frontier in the result and let the caller decide what to do with the information.
    async fn snapshot(
        &self,
        id: GlobalId,
        as_of: Self::Timestamp,
    ) -> Result<Vec<(Row, Diff)>, StorageError> {
        let as_of = Antichain::from_elem(as_of);
        let metadata = &self.collection(id)?.collection_metadata;

        let mut persist_clients = self.persist.lock().await;

        let persist_client = persist_clients
            .open(metadata.persist_location.clone())
            .await
            .unwrap();

        // We create a new read handle every time someone requests a snapshot and then immediately
        // expire it instead of keeping a read handle permanently in our state to avoid having it
        // heartbeat continously. The assumption is that calls to snapshot are rare and therefore
        // worth it to always create a new handle.
        let mut read_handle = persist_client
            .open_leased_reader::<SourceData, (), _, _>(
                metadata.data_shard,
                &format!("snapshot {}", id),
                Arc::new(metadata.relation_desc.clone()),
                Arc::new(UnitSchema),
            )
            .await
            .expect("invalid persist usage");

        drop(persist_clients);

        match read_handle.snapshot_and_fetch(as_of).await {
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

    #[tracing::instrument(level = "debug", skip(self))]
    fn set_read_policy(&mut self, policies: Vec<(GlobalId, ReadPolicy<Self::Timestamp>)>) {
        let mut read_capability_changes = BTreeMap::default();
        for (id, policy) in policies.into_iter() {
            if let Ok(mut updates) =
                self.generate_new_capability_for_collection(id, |c| c.read_policy = policy)
            {
                if !updates.is_empty() {
                    read_capability_changes.insert(id, updates);
                }
            } else {
                tracing::warn!("Reference to unregistered id: {:?}", id);
            }
        }
        if !read_capability_changes.is_empty() {
            self.update_read_capabilities(&mut read_capability_changes);
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn update_write_frontiers(&mut self, updates: &[(GlobalId, Antichain<Self::Timestamp>)]) {
        let mut read_capability_changes = BTreeMap::default();
        let mut collections = BTreeMap::new();
        let mut exports = vec![];

        for (id, new_upper) in updates.iter() {
            if let Ok(_) = self.collection(*id) {
                collections.insert(*id, Some(new_upper));
            } else if let Ok(_) = self.export(*id) {
                exports.push((id, new_upper));
            } else {
                panic!("Reference to absent collection");
            }
        }

        // Exports come first so we can update the collections below based on any new export write frontiers
        for (id, new_upper) in exports {
            let export = self
                .export_mut(*id)
                .expect("Export previously validated to exist");
            export.write_frontier.join_assign(new_upper);
            collections.entry(export.from()).or_insert(None);
        }

        for (id, new_upper) in collections {
            let mut update = self
                .generate_new_capability_for_collection(id, |c| {
                    if let Some(new_upper) = new_upper {
                        c.write_frontier.join_assign(new_upper);
                    }
                })
                .expect("Collection previously validated to exist");
            if !update.is_empty() {
                read_capability_changes.insert(id, update);
            }
        }

        if !read_capability_changes.is_empty() {
            self.update_read_capabilities(&mut read_capability_changes);
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn update_read_capabilities(
        &mut self,
        updates: &mut BTreeMap<GlobalId, ChangeBatch<Self::Timestamp>>,
    ) {
        // Location to record consequences that we need to act on.
        let mut storage_net = BTreeMap::new();
        // Repeatedly extract the maximum id, and updates for it.
        while let Some(key) = updates.keys().rev().next().cloned() {
            let mut update = updates.remove(&key).unwrap();
            if let Ok(collection) = self.collection_mut(key) {
                let changes = collection.read_capabilities.update_iter(update.drain());
                update.extend(changes);

                let (changes, frontier) = storage_net
                    .entry(key)
                    .or_insert_with(|| (ChangeBatch::new(), Antichain::new()));

                changes.extend(update.drain());
                *frontier = collection.read_capabilities.frontier().to_owned();
            } else {
                // This is confusing and we should probably error.
                panic!("Unknown collection identifier {}", key);
            }
        }

        // Translate our net compute actions into `AllowCompaction` commands and
        // downgrade persist sinces. The actual downgrades are performed by a Tokio
        // task asynchorously.
        let mut compaction_commands = BTreeMap::default();
        for (key, (mut changes, frontier)) in storage_net {
            if !changes.is_empty() {
                compaction_commands.insert(key, frontier);
            }
        }
        self.state
            .persist_read_handles
            .downgrade(compaction_commands.clone());

        for (id, frontier) in compaction_commands {
            // Acquiring a client for a storage instance requires await, so we
            // instead stash these for later and process when we can.
            self.state.pending_compaction_commands.push((id, frontier));
        }
    }

    async fn ready(&mut self) {
        let mut clients = self
            .state
            .clients
            .values_mut()
            .map(|client| client.response_stream())
            .enumerate()
            .collect::<StreamMap<_, _>>();

        use tokio_stream::StreamExt;
        let msg = tokio::select! {
            // Order matters here. We want to process internal commands
            // before processing external commands.
            biased;

            Some(m) = self.internal_response_queue.recv() => m,
            Some((_id, m)) = clients.next() => m,
        };

        self.state.stashed_response = Some(msg);
    }

    async fn process(&mut self) -> Result<(), anyhow::Error> {
        match self.state.stashed_response.take() {
            None => (),
            Some(StorageResponse::FrontierUppers(updates)) => {
                self.update_write_frontiers(&updates);
            }
            Some(StorageResponse::DroppedIds(_ids)) => {
                // TODO(petrosagg): It looks like the storage controller never cleans up GlobalIds
                // from its state. It should probably be done as a reaction to this response.
            }
            Some(StorageResponse::StatisticsUpdates(source_stats, sink_stats)) => {
                // Note we only hold the locks while moving some plain-old-data around here.

                let mut shared_stats = self.state.source_statistics.lock().expect("poisoned");
                for stat in source_stats {
                    let shared_stats = shared_stats.entry(stat.id).or_default();
                    // We just write the whole object, as the update from storage represents the
                    // current values.
                    shared_stats.insert(stat.worker_id, stat);
                }

                let mut shared_stats = self.state.sink_statistics.lock().expect("poisoned");
                for stat in sink_stats {
                    let shared_stats = shared_stats.entry(stat.id).or_default();
                    // We just write the whole object, as the update from storage represents the
                    // current values.
                    shared_stats.insert(stat.worker_id, stat);
                }
            }
        }

        // TODO(aljoscha): We could consolidate these before sending to
        // instances, but this seems fine for now.
        for (id, frontier) in self.state.pending_compaction_commands.drain(..) {
            // TODO(petrosagg): make this a strict check
            let cluster_id = self.state.collections[&id].cluster_id();
            let client = cluster_id.and_then(|cluster_id| self.state.clients.get_mut(&cluster_id));

            // Only ingestion collections have actual work to do on drop.
            //
            // Note that while collections are dropped, the `client` may already
            // be cleared out, before we do this post-processing!
            if cluster_id.is_some() && frontier.is_empty() {
                self.state.pending_source_drops.push(id);
            }

            if let Some(client) = client {
                client.send(StorageCommand::AllowCompaction(vec![(
                    id,
                    frontier.clone(),
                )]));
            }
        }

        // Record the drop status for all pending source and sink drops.
        let source_status_history_id =
            self.state.introspection_ids[&IntrospectionType::SourceStatusHistory];
        let mut updates = vec![];
        for id in self.state.pending_source_drops.drain(..) {
            let status_row = healthcheck::pack_status_row(id, "dropped", None, (self.state.now)());
            updates.push((status_row, 1));
        }
        self.append_to_managed_collection(source_status_history_id, updates)
            .await;

        // Record the drop status for all pending sink drops.
        let sink_status_history_id =
            self.state.introspection_ids[&IntrospectionType::SinkStatusHistory];
        let mut updates = vec![];
        for id in self.state.pending_sink_drops.drain(..) {
            let status_row = healthcheck::pack_status_row(id, "dropped", None, (self.state.now)());
            updates.push((status_row, 1));
        }
        self.append_to_managed_collection(sink_status_history_id, updates)
            .await;

        Ok(())
    }

    async fn reconcile_state(&mut self) {
        self.reconcile_shards().await
    }
}

/// A wrapper struct that presents the adapter token to a format that is understandable by persist
/// and also allows us to differentiate between a token being present versus being set for the
/// first time.
// TODO(aljoscha): Make this crate-public again once the remap operator doesn't
// hold a critical handle anymore.
#[derive(PartialEq, Clone, Debug)]
pub struct PersistEpoch(Option<NonZeroI64>);

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

impl<T> Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,

    Self: StorageController<Timestamp = T>,
{
    /// Create a new storage controller from a client it should wrap.
    ///
    /// Note that when creating a new storage controller, you must also
    /// reconcile it with the previous state.
    pub async fn new(
        build_info: &'static BuildInfo,
        postgres_url: String,
        persist_location: PersistLocation,
        persist_clients: Arc<Mutex<PersistClientCache>>,
        now: NowFn,
        postgres_factory: &StashFactory,
        envd_epoch: NonZeroI64,
        metrics_registry: MetricsRegistry,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        Self {
            build_info,
            state: StorageControllerState::new(postgres_url, tx, now, postgres_factory, envd_epoch)
                .await,
            internal_response_queue: rx,
            persist_location,
            persist: persist_clients,
            metrics: StorageControllerMetrics::new(metrics_registry),
        }
    }

    /// Validate that a collection exists for all identifiers, and error if any do not.
    fn validate_collection_ids(
        &self,
        ids: impl Iterator<Item = GlobalId>,
    ) -> Result<(), StorageError> {
        for id in ids {
            self.collection(id)?;
        }
        Ok(())
    }

    /// Validate that a collection exists for all identifiers, and error if any do not.
    fn validate_export_ids(&self, ids: impl Iterator<Item = GlobalId>) -> Result<(), StorageError> {
        for id in ids {
            self.export(id)?;
        }
        Ok(())
    }

    /// Acquires persist handles for the given `shard`. This will `halt!` the
    /// process if we cannot successfully acquire a critical handle with our
    /// current epoch.
    async fn acquire_data_handles(
        &self,
        id: GlobalId,
        shard: ShardId,
        relation_desc: RelationDesc,
        persist_client: &PersistClient,
    ) -> (
        WriteHandle<SourceData, (), T, Diff>,
        SinceHandle<SourceData, (), T, Diff, PersistEpoch>,
    ) {
        let purpose = format!("controller data {}", id);

        let write = persist_client
            .open_writer(
                shard,
                &purpose,
                Arc::new(relation_desc),
                Arc::new(UnitSchema),
            )
            .await
            .expect("invalid persist usage");

        // Construct the handle in a separate block to ensure all error paths are diverging
        let since_handle = {
            let mut handle: SinceHandle<_, _, _, _, PersistEpoch> = persist_client
                .open_critical_since(shard, PersistClient::CONTROLLER_CRITICAL_SINCE, &purpose)
                .await
                .expect("invalid persist usage");

            let since = handle.since().clone();

            // We should only continue if we can fence out any other processes
            let our_epoch = self.state.envd_epoch;
            loop {
                let their_epoch: PersistEpoch = handle.opaque().clone();

                let should_exchange = their_epoch.0.map(|e| e < our_epoch).unwrap_or(true);
                if should_exchange {
                    let fenced_others = handle
                        .compare_and_downgrade_since(
                            &their_epoch,
                            (&PersistEpoch::from(our_epoch), &since),
                        )
                        .await
                        .is_ok();
                    if fenced_others {
                        break handle;
                    }
                } else {
                    mz_ore::halt!("fenced by envd @ {their_epoch:?}. ours = {our_epoch}");
                }
            }
        };

        (write, since_handle)
    }

    // Should only fail if collection doesn't exist. N.B. We can't just take in the mut ref because then the borrow checker wouldn't let us read state.
    fn generate_new_capability_for_collection<F>(
        &mut self,
        id: GlobalId,
        f: F,
    ) -> Result<ChangeBatch<<Self as StorageController>::Timestamp>, StorageError>
    where
        F: FnOnce(&mut CollectionState<<Self as StorageController>::Timestamp>),
    {
        let collection = self
            .state
            .collections
            .get_mut(&id)
            .ok_or(StorageError::IdentifierMissing(id))?;
        f(collection);

        let mut update = ChangeBatch::new();

        // Get read policy sent from the coordinator
        let mut new_read_capability = collection
            .read_policy
            .frontier(collection.write_frontier.borrow());

        // Also consider the write frontier of any exports.  It's worth adding a quick note on write frontiers here.
        //
        // The write frontier that sinks communicate back to the controller indicates that all further writes will
        // happen at a time `t` such that `!timely::ParitalOrder::less_than(&t, &write_frontier)` is true.  On restart,
        // the sink will receive an SinkAsOf from this controller indicating that it should ignore everything at or
        // before the `since` of the from collection.  This will not miss any records because, if there were records not
        // yet written out that have an uncompacted time of `since`, the write frontier previously reported from the
        // sink must be less than `since` so we would not have compacted up to `since`!  This is tested by the kafka
        // persistence tests.
        for export_id in self
            .state
            .exported_collections
            .get(&id)
            .cloned()
            .unwrap_or_default()
        {
            new_read_capability.meet_assign(
                &self
                    .state
                    .exports
                    .get(&export_id)
                    .map(|state| state.write_frontier.clone())
                    // If sink has not been fully initialized (only `prepare_export` but not
                    // `create_export` has been called), hold back compaction completely.
                    .unwrap_or_else(|| Antichain::from_elem(Timestamp::minimum())),
            );
        }

        if PartialOrder::less_equal(&collection.implied_capability, &new_read_capability) {
            update.extend(new_read_capability.iter().map(|time| (time.clone(), 1)));
            std::mem::swap(&mut collection.implied_capability, &mut new_read_capability);
            update.extend(new_read_capability.iter().map(|time| (time.clone(), -1)));
        }

        Ok(update)
    }

    /// Effectively truncates the `data_shard` associated with `global_id`
    /// effective as of the system time.
    ///
    /// # Panics
    /// - If `id` does not belong to a collection or is not registered as a
    ///   managed collection.
    async fn reconcile_managed_collection(&self, id: GlobalId, updates: Vec<(Row, Diff)>) {
        let mut reconciled_updates = BTreeMap::<Row, Diff>::new();

        for (row, diff) in updates.into_iter() {
            *reconciled_updates.entry(row).or_default() += diff;
        }

        match self.state.collections[&id]
            .write_frontier
            .elements()
            .iter()
            .min()
        {
            Some(f) if f > &T::minimum() => {
                let as_of = f.step_back().unwrap();

                let negate = self.snapshot(id, as_of).await.unwrap();

                for (row, diff) in negate.into_iter() {
                    *reconciled_updates.entry(row).or_default() -= diff;
                }
            }
            // If collection is closed or the frontier is the minimum, we cannot
            // or don't need to truncate (respectively).
            _ => {}
        }

        let updates: Vec<_> = reconciled_updates
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect();

        if !updates.is_empty() {
            self.append_to_managed_collection(id, updates).await;
        }
    }

    /// Append `updates` to the `data_shard` associated with `global_id`
    /// effective as of the system time.
    ///
    /// # Panics
    /// - If `id` is not registered as a managed collection.
    async fn append_to_managed_collection(&self, id: GlobalId, updates: Vec<(Row, Diff)>) {
        self.state
            .collection_manager
            .append_to_collection(id, updates)
            .await;
    }

    /// Initializes the data expressing which global IDs correspond to which
    /// shards. Necessary because we cannot write any of these mappings that we
    /// discover before the shard mapping collection exists.
    ///
    /// # Panics
    /// - If `IntrospectionType::ShardMapping` is not associated with a
    /// `GlobalId` in `self.state.introspection_ids`.
    /// - If `IntrospectionType::ShardMapping`'s `GlobalId` is not registered as
    ///   a managed collection.
    async fn initialize_shard_mapping(&mut self) {
        let id = self.state.introspection_ids[&IntrospectionType::ShardMapping];

        let mut row_buf = Row::default();
        let mut updates = Vec::with_capacity(self.state.collections.len());
        for (
            global_id,
            CollectionState {
                collection_metadata: CollectionMetadata { data_shard, .. },
                ..
            },
        ) in self.state.collections.iter()
        {
            let mut packer = row_buf.packer();
            packer.push(Datum::from(global_id.to_string().as_str()));
            packer.push(Datum::from(data_shard.to_string().as_str()));
            updates.push((row_buf.clone(), 1));
        }

        self.reconcile_managed_collection(id, updates).await;
    }

    /// Writes a new global ID, shard ID pair to the appropriate collection.
    ///
    /// However, data is written iff we know of the `GlobalId` of the
    /// `IntrospectionType::ShardMapping` collection; in other cases, data is
    /// dropped on the floor. In these cases, the data is later written by
    /// [`Self::initialize_shard_mapping`].
    ///
    /// # Panics
    /// - If `self.state.collections` does not have an entry for `global_id`.
    /// - If `IntrospectionType::ShardMapping`'s `GlobalId` is not registered as
    ///   a managed collection.
    async fn register_shard_mappings<I>(&self, global_ids: I)
    where
        I: Iterator<Item = GlobalId>,
    {
        let id = match self
            .state
            .introspection_ids
            .get(&IntrospectionType::ShardMapping)
        {
            Some(id) => *id,
            _ => return,
        };

        let mut updates = vec![];
        // Pack updates into rows
        let mut row_buf = Row::default();

        for global_id in global_ids {
            let shard_id = self.state.collections[&global_id]
                .collection_metadata
                .data_shard;

            let mut packer = row_buf.packer();
            packer.push(Datum::from(global_id.to_string().as_str()));
            packer.push(Datum::from(shard_id.to_string().as_str()));
            updates.push((row_buf.clone(), 1));
        }

        self.append_to_managed_collection(id, updates).await;
    }

    /// Updates the `DurableCollectionMetadata` associated with `id` to
    /// `new_metadata`.
    ///
    /// Any shards changed between the old and the new version will be
    /// decommissioned/eventually deleted.
    ///
    /// Note that this function expects to be called:
    /// - While no source is currently using the shards identified in the
    ///   current metadata.
    /// - Before any sources begins using the shards identified in
    ///   `new_metadata`.
    async fn _rewrite_collection_metadata(
        &mut self,
        id: GlobalId,
        new_metadata: DurableCollectionMetadata,
    ) {
        let current_metadata = METADATA_COLLECTION
            .peek_key_one(&mut self.state.stash, id)
            .await
            .expect("connect to stash");

        let mut replace_data_shard = false;

        let to_delete_shards = match current_metadata {
            None => {
                // If this ID has not yet been written, nothing to update.
                return;
            }
            Some(metadata) => {
                if metadata == new_metadata {
                    return;
                }

                let mut to_delete_shards = vec![];
                for (old, new, desc, data_shard_replaced) in [
                    (metadata.data_shard, new_metadata.data_shard, "data", true),
                    (
                        metadata.remap_shard,
                        new_metadata.remap_shard,
                        "remap",
                        false,
                    ),
                ] {
                    if old != new {
                        info!(
                            "replacing {:?}'s {} shard {:?} with {:?}",
                            id, desc, old, new
                        );
                        to_delete_shards
                            .push((old, format!("retired {} shard for {:?}", desc, id)));

                        replace_data_shard = replace_data_shard | data_shard_replaced;
                    }
                }

                to_delete_shards
            }
        };

        self.register_shards_for_finalization(to_delete_shards.iter().map(|(shard, _)| *shard))
            .await;

        // Perform the update.
        METADATA_COLLECTION
            .upsert(&mut self.state.stash, vec![(id, new_metadata.clone())])
            .await
            .expect("connect to stash");

        self.finalize_shards(&to_delete_shards).await;

        let DurableCollectionMetadata {
            remap_shard,
            data_shard,
        } = new_metadata;

        // Update in memory collection metadata if the collection has been
        // registered.
        if let Some(metadata) = self.state.collections.get_mut(&id) {
            metadata.collection_metadata.data_shard = data_shard;
            metadata.collection_metadata.remap_shard = remap_shard;

            if replace_data_shard {
                let persist_client = self
                    .persist
                    .lock()
                    .await
                    .open(self.persist_location.clone())
                    .await
                    .unwrap();

                let relation_desc = metadata.collection_metadata.relation_desc.clone();

                // This will halt! if any of the handles cannot be acquired
                // because we're not the leader anymore. But that's fine, we
                // already updated all the persistent state (in stash).
                let (write, since_handle) = self
                    .acquire_data_handles(id, data_shard, relation_desc, &persist_client)
                    .await;

                self.state.persist_write_handles.update(id, write);
                self.state.persist_read_handles.update(id, since_handle);
            }
        }
    }

    /// Closes the identified shards from further reads or writes.
    ///
    /// The string accompanying the `ShardId` is the shard's "purpose",
    /// necessary to open read and write handles to the shard.
    #[allow(dead_code)]
    async fn finalize_shards(&mut self, shards: &[(ShardId, String)]) {
        soft_assert!(
            {
                let mut all_registered = true;
                for (shard, _) in shards {
                    all_registered =
                        self.is_shard_registered_for_finalization(*shard).await && all_registered
                }
                all_registered
            },
            "finalized shards must be registered before calling finalize_shards"
        );

        // Open a persist client to delete unused shards.
        let persist_client = self
            .persist
            .lock()
            .await
            .open(self.persist_location.clone())
            .await
            .unwrap();

        for (shard_id, shard_purpose) in shards {
            let mut critical_since_handle: SinceHandle<
                crate::types::sources::SourceData,
                (),
                T,
                Diff,
                PersistEpoch,
            > = persist_client
                .open_critical_since(
                    *shard_id,
                    PersistClient::CONTROLLER_CRITICAL_SINCE,
                    shard_purpose.as_str(),
                )
                .await
                .expect("invalid persist usage");

            let our_epoch = PersistEpoch::from(self.state.envd_epoch);
            match critical_since_handle
                .compare_and_downgrade_since(&our_epoch, (&our_epoch, &Antichain::new()))
                .await
            {
                Ok(_) => info!("successfully finalized read handle for shard {shard_id:?}"),
                Err(e) => mz_ore::halt!("fenced by envd @ {e:?}. ours = {our_epoch:?}"),
            }

            let mut write = persist_client
                .open_writer(
                    *shard_id,
                    shard_purpose.as_str(),
                    Arc::new(RelationDesc::empty()),
                    Arc::new(UnitSchema),
                )
                .await
                .expect("invalid persist usage");

            if write.upper().is_empty() {
                info!("write handle for shard {:?} already finalized", shard_id);
            } else {
                write
                    .append(
                        Vec::<((crate::types::sources::SourceData, ()), T, Diff)>::new(),
                        write.upper().clone(),
                        Antichain::new(),
                    )
                    .await
                    .expect("failed to connect")
                    .expect("failed to truncate write handle");

                info!(
                    "successfully finalized write handle for shard {:?}",
                    shard_id
                );
            }
        }

        let truncated_shards = shards.iter().map(|(shard, _)| *shard).collect();
        self.clear_from_shard_finalization_register(truncated_shards)
            .await;
    }
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
        metadata: CollectionMetadata,
    ) -> Self {
        let mut read_capabilities = MutableAntichain::new();
        read_capabilities.update_iter(since.iter().map(|time| (time.clone(), 1)));
        Self {
            description,
            read_capabilities,
            implied_capability: since.clone(),
            read_policy: ReadPolicy::ValidFrom(since),
            write_frontier,
            collection_metadata: metadata,
        }
    }

    /// Returns the cluster to which the collection is bound, if applicable.
    fn cluster_id(&self) -> Option<StorageInstanceId> {
        match &self.description.data_source {
            DataSource::Ingestion(ingestion) => Some(ingestion.instance_id),
            DataSource::Introspection(_) => None,
            DataSource::Other => None,
        }
    }
}

/// State maintained about individual exports.
#[derive(Debug)]
pub struct ExportState<T> {
    /// Description with which the export was created
    pub description: ExportDescription<T>,

    /// Reported write frontier.
    pub write_frontier: Antichain<T>,
}
impl<T: Timestamp> ExportState<T> {
    fn new(description: ExportDescription<T>) -> Self {
        Self {
            description,
            write_frontier: Antichain::from_elem(Timestamp::minimum()),
        }
    }
    fn from(&self) -> GlobalId {
        self.description.sink.from
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_writes_by_zero() {
        let policy = ReadPolicy::lag_writes_by(mz_repr::Timestamp::default());
        let write_frontier = Antichain::from_elem(mz_repr::Timestamp::from(5));
        assert_eq!(policy.frontier(write_frontier.borrow()), write_frontier);
    }
}
