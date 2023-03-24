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
use std::collections::{BTreeMap, BTreeSet};
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
use tokio_stream::StreamMap;
use tracing::{debug, info};

use mz_build_info::BuildInfo;
use mz_cluster_client::client::ClusterReplicaLocation;
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
use crate::types::sources::{IngestionDescription, SourceData, SourceEnvelope, SourceExport};

mod collection_mgmt;
mod command_wals;
mod persist_handles;
mod rehydration;
mod remap_migration;
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
    /// Data comes from the source's remapping/reclock operator.
    Progress,
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

impl<T> CollectionDescription<T> {
    /// Returns IDs for all storage objects that this `CollectionDescription`
    /// depends on.
    ///
    /// TODO: @sean: This is where the remap shard would slot in.
    fn get_storage_dependencies(&self) -> Vec<GlobalId> {
        let mut result = Vec::new();

        // NOTE: Exhaustive match for future proofing.
        match &self.data_source {
            DataSource::Ingestion(ingestion) => {
                match &ingestion.desc.envelope {
                    SourceEnvelope::Debezium(envelope_debezium) => {
                        let tx_metadata_topic = &envelope_debezium.dedup.tx_metadata;
                        if let Some(tx_input) = tx_metadata_topic {
                            result.push(tx_input.tx_metadata_global_id);
                        }
                    }
                    // NOTE: We explicitly list envelopes instead of using a catch all to
                    // make sure that we change this when adding/removing and envelope.
                    SourceEnvelope::None(_) | SourceEnvelope::Upsert(_) | SourceEnvelope::CdcV2 => {
                        // No storage dependencies.
                    }
                }
                result.push(ingestion.remap_collection_id);
            }
            DataSource::Introspection(_) | DataSource::Progress => {
                // Introspection, Progress sources have no dependencies, for
                // now.
            }
            DataSource::Other => {
                // We don't know anything about it's dependencies.
            }
        }

        result
    }
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
pub struct CreateExportToken<T = mz_repr::Timestamp> {
    id: GlobalId,
    from_id: GlobalId,
    acquired_since: Antichain<T>,
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
    fn connect_replica(&mut self, id: StorageInstanceId, location: ClusterReplicaLocation);

    /// Acquire a mutable reference to the collection state, should it exist.
    fn collection_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut CollectionState<Self::Timestamp>, StorageError>;

    /// Acquire an iterator over all collection states.
    fn collections(
        &self,
    ) -> Box<dyn Iterator<Item = (&GlobalId, &CollectionState<Self::Timestamp>)> + '_>;

    /// Migrate any storage controller state from previous versions to this
    /// version's expectations.
    ///
    /// This function must "see" the GlobalId of every collection you plan to
    /// create, but can be called with all of the catalog's collections at once.
    async fn migrate_collections(
        &mut self,
        collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError>;

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
        exports: Vec<(
            CreateExportToken<Self::Timestamp>,
            ExportDescription<Self::Timestamp>,
        )>,
    ) -> Result<(), StorageError>;

    /// Notify the storage controller to prepare for an export to be created
    fn prepare_export(
        &mut self,
        id: GlobalId,
        from_id: GlobalId,
    ) -> Result<CreateExportToken<Self::Timestamp>, StorageError>;

    /// Cancel the pending export
    fn cancel_prepare_export(&mut self, token: CreateExportToken<Self::Timestamp>);

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
    /// No-one has yet requested a `ReadPolicy` from us, which means that we can
    /// still change the implied_capability/the collection since if we need
    /// to.
    NoPolicy { initial_since: Antichain<T> },
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

impl<T> ReadPolicy<T>
where
    T: Timestamp + TimestampManipulation,
{
    /// Creates a read policy that lags the write frontier "by one".
    pub fn step_back() -> Self {
        Self::LagWriteFrontier(Arc::new(move |upper| {
            if upper.is_empty() {
                Antichain::from_elem(Timestamp::minimum())
            } else {
                let stepped_back = upper
                    .to_owned()
                    .into_iter()
                    .map(|time| {
                        if time == T::minimum() {
                            time
                        } else {
                            time.step_back().unwrap()
                        }
                    })
                    .collect_vec();
                stepped_back.into()
            }
        }))
    }
}

impl ReadPolicy<mz_repr::Timestamp> {
    /// Creates a read policy that lags the write frontier by the indicated amount, rounded down to the specified value.
    /// The rounding down is done to reduce the number of changes the capability undergoes.
    pub fn lag_writes_by(lag: mz_repr::Timestamp, granularity: mz_repr::Timestamp) -> Self {
        Self::LagWriteFrontier(Arc::new(move |upper| {
            if upper.is_empty() {
                Antichain::from_elem(Timestamp::minimum())
            } else {
                // Subtract the lag from the time, and then round down to a multiple of `granularity` to cut chatter.
                let mut time = upper[0];
                if lag != mz_repr::Timestamp::default() {
                    time = time.saturating_sub(lag);
                    if granularity != mz_repr::Timestamp::default() {
                        time = time.saturating_sub(time % granularity);
                    }
                }
                Antichain::from_elem(time)
            }
        }))
    }
}

impl<T: Timestamp> ReadPolicy<T> {
    pub fn frontier(&self, write_frontier: AntichainRef<T>) -> Antichain<T> {
        match self {
            ReadPolicy::NoPolicy { initial_since } => initial_since.clone(),
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
    pub remap_shard: Option<ShardId>,
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
            remap_shard: self.remap_shard.map(|s| s.to_string()),
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
                .map(|s| s.parse().map_err(TryFromProtoError::InvalidShardId))
                .transpose()?,
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
    async fn initialize_state(&self, client_cache: &PersistClientCache) -> Self::State;

    /// Calculates a new, safe _resumption frontier_.
    async fn calculate_resumption_frontier(&self, state: &mut Self::State) -> Antichain<T>;
}

/// The subset of [`CollectionMetadata`] that must be durable stored.
#[derive(Arbitrary, Clone, Debug, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize)]
pub struct DurableCollectionMetadata {
    // MIGRATION: v0.44 This field can be deleted in a future version of
    // Materialize because we are moving the relationship between a collection
    // and its remap shard into a relationship between a collection and its
    // remap collection, i.e. we will use another collection's data shard as our
    // remap shard, rendering this mapping duplicative.
    pub remap_shard: Option<ShardId>,
    pub data_shard: ShardId,
}

impl RustType<ProtoDurableCollectionMetadata> for DurableCollectionMetadata {
    fn into_proto(&self) -> ProtoDurableCollectionMetadata {
        ProtoDurableCollectionMetadata {
            remap_shard: self.remap_shard.into_proto(),
            data_shard: self.data_shard.to_string(),
        }
    }

    fn from_proto(value: ProtoDurableCollectionMetadata) -> Result<Self, TryFromProtoError> {
        Ok(DurableCollectionMetadata {
            remap_shard: value
                .remap_shard
                .map(|data_shard| {
                    data_shard
                        .parse()
                        .map_err(TryFromProtoError::InvalidShardId)
                })
                .transpose()?,
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
    pending_compaction_commands: Vec<(GlobalId, Antichain<T>, Option<StorageInstanceId>)>,

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
    persist: Arc<PersistClientCache>,
    /// Metrics of the Storage controller
    metrics: StorageControllerMetrics,
}

#[derive(Debug)]
pub enum StorageError {
    /// The source identifier was re-created after having been dropped,
    /// or installed with a different description.
    SourceIdReused(GlobalId),
    /// The sink identifier was re-created after having been dropped, or
    /// installed with a different description.
    SinkIdReused(GlobalId),
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
    /// The controller API was used in some invalid way. This usually indicates
    /// a bug.
    InvalidUsage(String),
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SourceIdReused(_) => None,
            Self::SinkIdReused(_) => None,
            Self::IdentifierMissing(_) => None,
            Self::UpdateBeyondUpper(_) => None,
            Self::ReadBeforeSince(_) => None,
            Self::InvalidUppers(_) => None,
            Self::ClientError(_) => None,
            Self::IOError(err) => Some(err),
            Self::DataflowError(err) => Some(err),
            Self::InvalidUsage(_) => None,
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
            Self::SinkIdReused(id) => write!(
                f,
                "sink identifier was re-created after having been dropped: {id}"
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
            Self::InvalidUsage(err) => write!(f, "invalid usage: {err}"),
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
        config_params.persist.apply(self.persist.cfg());

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
            self.metrics.for_instance(id),
            self.state.envd_epoch,
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

    fn connect_replica(&mut self, id: StorageInstanceId, location: ClusterReplicaLocation) {
        let client = self
            .state
            .clients
            .get_mut(&id)
            .unwrap_or_else(|| panic!("instance {id} does not exist"));
        client.connect(location);
    }

    // Add new migrations below and precede them with a short summary of the
    // migration's purpose and optional additional commentary about safety or
    // approach.
    //
    // Note that:
    // - The sum of all migrations must be idempotent because all migrations run
    //   every time the catalog opens, unless migrations are explicitly
    //   disabled. This might mean changing code outside the migration itself,
    //   or only executing some migrations when encountering certain versions.
    // - Migrations must preserve backwards compatibility with all past releases
    //   of Materialize.
    #[tracing::instrument(level = "debug", skip_all)]
    async fn migrate_collections(
        &mut self,
        collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError> {
        let mut durable_metadata = METADATA_COLLECTION.peek_one(&mut self.state.stash).await?;

        // MIGRATION: v0.44 See comments on remap_shard_migration.
        let remap_shard_migration_delta =
            self.remap_shard_migration(&durable_metadata, &collections);

        self.upsert_collection_metadata(&mut durable_metadata, remap_shard_migration_delta)
            .await;

        Ok(())
    }

    // TODO(aljoscha): It would be swell if we could refactor this Leviathan of
    // a method/move individual parts to their own methods.
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

        // Install collection state for each bound description. Note that this
        // method implementation attempts to do AS MUCH work concurrently as
        // possible. There are inline comments explaining the motivation behind
        // each section.
        let mut entries = Vec::with_capacity(collections.len());

        for (id, _desc) in &collections {
            entries.push((
                *id,
                DurableCollectionMetadata {
                    data_shard: ShardId::new(),
                    remap_shard: None,
                },
            ))
        }

        // Perform all stash writes in a single transaction, to minimize transaction overhead and
        // the time spent waiting for stash.
        METADATA_COLLECTION
            .insert_without_overwrite(&mut self.state.stash, entries.into_iter())
            .await?;

        let mut durable_metadata = METADATA_COLLECTION.peek_one(&mut self.state.stash).await?;

        // We first enrich each collection description with some additional metadata...
        use futures::stream::{StreamExt, TryStreamExt};
        let enriched_with_metadata = collections
            .into_iter()
            .map(|(id, description)| {
                let collection_shards = durable_metadata.remove(&id).expect("inserted above");
                // MIGRATION: v0.44
                assert!(collection_shards.remap_shard.is_none(), "remap shards must be migrated to be the data shard of their remap/progress collections or dropped");

                let status_shard =
                    if let Some(status_collection_id) = description.status_collection_id {
                        Some(
                            durable_metadata
                                .get(&status_collection_id)
                                .ok_or(StorageError::IdentifierMissing(status_collection_id))?
                                .data_shard,
                        )
                    } else {
                        None
                    };

                let remap_shard = match &description.data_source {
                    // Only ingestions can have remap shards.
                    DataSource::Ingestion(IngestionDescription {
                        remap_collection_id,
                        ..
                    }) => {
                        // Iff ingestion has a remap collection, its metadata must
                        // exist (and be correct) by this point.
                        Some(
                            durable_metadata
                                .get(remap_collection_id)
                                .ok_or(StorageError::IdentifierMissing(*remap_collection_id))?
                                .data_shard,
                        )
                    }
                    _ => None,
                };

                let metadata = CollectionMetadata {
                    persist_location: self.persist_location.clone(),
                    remap_shard,
                    data_shard: collection_shards.data_shard,
                    status_shard,
                    relation_desc: description.desc.clone(),
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
        // Reborrow the `&mut self` as immutable, as all the concurrent work to be processed in
        // this stream cannot all have exclusive access.
        let this = &*self;
        let to_register: Vec<_> = futures::stream::iter(enriched_with_metadata)
            .map(|data: Result<_, anyhow::Error>| async move {
                let (id, description, metadata) = data?;

                // should be replaced with real introspection (https://github.com/MaterializeInc/materialize/issues/14266)
                // but for now, it's helpful to have this mapping written down somewhere
                debug!(
                    "mapping GlobalId={} to remap shard ({:?}), data shard ({}), status shard ({:?})",
                    id, metadata.remap_shard, metadata.data_shard, metadata.status_shard
                );

                let (write, since_handle) = this
                    .open_data_handles(
                        format!("controller data {}", id).as_str(),
                        metadata.data_shard,
                        metadata.relation_desc.clone(),
                        persist_client,
                    )
                    .await;

                Ok::<_, anyhow::Error>((id, description, write, since_handle, metadata))
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
        for (id, description, write, since_handle, metadata) in to_register {
            let data_shard_since = since_handle.since().clone();

            let collection_state = CollectionState::new(
                description.clone(),
                data_shard_since,
                write.upper().clone(),
                vec![],
                metadata.clone(),
            );

            self.state.persist_write_handles.register(id, write);
            self.state.persist_read_handles.register(id, since_handle);

            self.state.collections.insert(id, collection_state);

            to_create.push((id, description));
        }

        // Patch up the since of all subsources (which includes the "main"
        // collection) and install read holds from the subsources on the since
        // of the remap collection. We need to do this here because a) the since
        // of the remap collection might be in advance of the since of the data
        // collections because we lazily forward commands to downgrade the since
        // to persist, and b) at the time the subsources are created we know
        // close to nothing about them, not even that they are subsources.
        //
        // N.B. Patching up the since based on the since of the remap collection
        // is correct because the since of the remap collection can advance iff
        // the storage controller allowed it to, which it only does when it
        // would also allow the since of the data collections to advance. It's
        // just that we need to reconcile outselves to the outside world
        // (persist) here.
        //
        // TODO(aljoscha): We should find a way to put this information and the
        // read holds in place when we create the subsource collections. OR, we
        // could create the subsource collections only as part of creating the
        // main source/ingestion.
        for (_id, description) in to_create.iter() {
            match &description.data_source {
                DataSource::Ingestion(ingestion) => {
                    let storage_dependencies = description.get_storage_dependencies();
                    let dependency_since =
                        self.determine_collection_since_joins(&storage_dependencies)?;

                    // Install read capability for all non-remap subsources on
                    // remap collection.
                    //
                    // N.B. The "main" collection of the source is included in
                    // `source_exports`.
                    for id in ingestion.source_exports.keys() {
                        let collection = self.collection(*id).expect("known to exist");

                        // At the time of collection creation, we did not yet
                        // have firm guarantees that the since of our
                        // dependencies was not advanced beyond those of its
                        // dependents, so we need to patch up the
                        // implied_capability/since of the collction.
                        //
                        // TODO(aljoscha): This comes largely from the fact that
                        // subsources are created with a `DataSource::Other`, so
                        // we have no idea (at their creation time) that they
                        // are a subsource, or that they are a subsource of a
                        // source where they need a read hold on that
                        // ingestion's remap collection.
                        if timely::order::PartialOrder::less_than(
                            &collection.implied_capability,
                            &dependency_since,
                        ) {
                            assert!(
                                timely::order::PartialOrder::less_than(
                                    &dependency_since,
                                    &collection.write_frontier
                                ),
                                "write frontier ({:?}) must be in advance dependency collection's since ({:?})",
                                collection.write_frontier,
                                dependency_since,
                            );
                            mz_ore::soft_assert!(
                                matches!(collection.read_policy, ReadPolicy::NoPolicy { .. }),
                                "subsources should not have external read holds installed until \
                                their ingestion is created, but {:?} has read policy {:?}",
                                id,
                                collection.read_policy
                            );

                            // This patches up the implied_capability!
                            self.set_read_policy(vec![(
                                *id,
                                ReadPolicy::NoPolicy {
                                    initial_since: dependency_since.clone(),
                                },
                            )]);

                            // We have to re-borrow.
                            let collection = self.collection(*id).expect("known to exist");
                            assert!(
                                collection.implied_capability == dependency_since,
                                "monkey patching the implied_capability to {:?} did not work, is still {:?}",
                                dependency_since,
                                collection.implied_capability,
                            );
                        }

                        // Fill in the storage dependencies.
                        let collection = self.collection_mut(*id).expect("known to exist");
                        collection
                            .storage_dependencies
                            .extend(storage_dependencies.iter().cloned());

                        assert!(
                            !PartialOrder::less_than(
                                &collection.read_capabilities.frontier(),
                                &collection.implied_capability.borrow()
                            ),
                            "{id}: at this point, there can be no read holds for any time that is not \
                            beyond the implied capability \
                            but we have implied_capability {:?}, read_capabilities {:?}",
                            collection.implied_capability,
                            collection.read_capabilities,
                        );

                        let read_hold = collection.implied_capability.clone();
                        self.install_read_capabilities(*id, &storage_dependencies, read_hold)?;
                    }
                }
                DataSource::Introspection(_) | DataSource::Progress | DataSource::Other => {
                    // No since to patch up and no read holds to install on
                    // dependencies!
                }
            }
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
                        remap_collection_id: ingestion.remap_collection_id,
                    };
                    let mut state = desc.initialize_state(&self.persist).await;
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
                DataSource::Progress | DataSource::Other => {}
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
    ) -> Result<CreateExportToken<T>, StorageError> {
        if let Ok(_export) = self.export(id) {
            return Err(StorageError::SourceIdReused(id));
        }

        let dependency_since = self.determine_collection_since_joins(&[from_id])?;
        self.install_read_capabilities(id, &[from_id], dependency_since.clone())?;

        info!(
            sink_id = id.to_string(),
            from_id = from_id.to_string(),
            acquired_since = ?dependency_since,
            "prepare_export: sink acquired read holds"
        );

        Ok(CreateExportToken {
            id,
            from_id,
            acquired_since: dependency_since,
        })
    }

    fn cancel_prepare_export(
        &mut self,
        CreateExportToken {
            id,
            from_id,
            acquired_since,
        }: CreateExportToken<T>,
    ) {
        info!(
            sink_id = id.to_string(),
            from_id = from_id.to_string(),
            acquired_since = ?acquired_since,
            "cancel_prepare_export: sink releasing read holds",
        );
        self.remove_read_capabilities(acquired_since, &[from_id]);
    }

    async fn create_exports(
        &mut self,
        exports: Vec<(
            CreateExportToken<Self::Timestamp>,
            ExportDescription<Self::Timestamp>,
        )>,
    ) -> Result<(), StorageError> {
        // Validate first, to avoid corrupting state.
        let mut dedup_hashmap = BTreeMap::<&_, &_>::new();
        for (export, desc) in exports.iter() {
            let CreateExportToken {
                id,
                from_id,
                acquired_since: _,
            } = export;

            if dedup_hashmap.insert(id, desc).is_some() {
                return Err(StorageError::SinkIdReused(*id));
            }
            if let Ok(export) = self.export(*id) {
                if &export.description != desc {
                    return Err(StorageError::SinkIdReused(*id));
                }
            }
            if desc.sink.from != *from_id {
                return Err(StorageError::InvalidUsage(format!(
                    "sink {id} was prepared using from_id {from_id}, \
                    but is now presented with from_id {}",
                    desc.sink.from
                )));
            }
        }

        for (export, description) in exports {
            let CreateExportToken {
                id,
                from_id,
                acquired_since,
            } = export;

            // It's worth adding a quick note on write frontiers here.
            //
            // The write frontier that sinks communicate back to the controller
            // indicates that all further writes will happen at a time `t` such
            // that `!timely::ParitalOrder::less_than(&t, &write_frontier)` is
            // true.  On restart, the sink will receive an SinkAsOf from this
            // controller indicating that it should ignore everything at or
            // before the `since` of the from collection. This will not miss any
            // records because, if there were records not yet written out that
            // have an uncompacted time of `since`, the write frontier
            // previously reported from the sink must be less than `since` so we
            // would not have compacted up to `since`! This is tested by the
            // kafka persistence tests.
            //
            // TODO: Remove upper frontier manipulation from sinks, the read
            // policy ensures that we can always resume and discern the updates
            // that happened at upper. The comment above is slightly wrong:
            // sinks report `F-1` as the upper when they are at upper `F`
            // (speaking in terms of a timely frontier). We should change sinks
            // to divorce what they write out to the progress topic and what
            // they report back as the write upper. To make sure that the
            // reported write upper conforms with what other parts of the system
            // think how uppers work.
            //
            // Note: This is where the sink code (kafka) calculates the write
            // frontier that it reports back:
            // https://github.com/MaterializeInc/materialize/blob/ec8560a532eb5e7282041757d6c1d650f0ffaa77/src/storage/src/sink/kafka.rs#L857
            let read_policy = ReadPolicy::step_back();

            let from_collection = self.collection(from_id)?;
            let from_storage_metadata = from_collection.collection_metadata.clone();

            let storage_dependencies = vec![from_id];

            let mut durable_export_data = MetadataExportFetcher::get_stash_collection()
                .insert_key_without_overwrite(
                    &mut self.state.stash,
                    id,
                    DurableExportMetadata {
                        initial_as_of: description.sink.as_of.clone(),
                    },
                )
                .await?;

            durable_export_data.initial_as_of.downgrade(&acquired_since);

            info!(
                sink_id = id.to_string(),
                from_id = from_id.to_string(),
                acquired_since = ?acquired_since,
                initial_as_of = ?durable_export_data.initial_as_of,
                "create_exports: creating sink"
            );

            self.state.exports.insert(
                id,
                ExportState::new(
                    description.clone(),
                    acquired_since,
                    read_policy,
                    storage_dependencies,
                ),
            );

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
                    as_of: durable_export_data.initial_as_of,
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
        // We don't explicitly call `remove_read_capabilities`! Downgrading the
        // frontier of the source to `[]` (the empty Antichain), will propagate
        // to the storage dependencies.
        let policies = identifiers
            .into_iter()
            .filter(|id| self.collection(*id).is_ok())
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

            let read_capability = export.read_capability.clone();
            let storage_dependencies = export.storage_dependencies.clone();
            self.remove_read_capabilities(read_capability, &storage_dependencies);

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

        let persist_client = self
            .persist
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
            let collection = self
                .collection_mut(id)
                .expect("Reference to absent collection");

            let mut new_read_capability = policy.frontier(collection.write_frontier.borrow());

            if timely::order::PartialOrder::less_equal(
                &collection.implied_capability,
                &new_read_capability,
            ) {
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

        if !read_capability_changes.is_empty() {
            self.update_read_capabilities(&mut read_capability_changes);
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn update_write_frontiers(&mut self, updates: &[(GlobalId, Antichain<Self::Timestamp>)]) {
        let mut read_capability_changes = BTreeMap::default();

        for (id, new_upper) in updates.iter() {
            if let Ok(collection) = self.collection_mut(*id) {
                if PartialOrder::less_than(&collection.write_frontier, new_upper) {
                    collection.write_frontier = new_upper.clone();
                }

                let mut new_read_capability = collection
                    .read_policy
                    .frontier(collection.write_frontier.borrow());

                if timely::order::PartialOrder::less_equal(
                    &collection.implied_capability,
                    &new_read_capability,
                ) {
                    let mut update = ChangeBatch::new();
                    update.extend(new_read_capability.iter().map(|time| (time.clone(), 1)));
                    std::mem::swap(&mut collection.implied_capability, &mut new_read_capability);
                    update.extend(new_read_capability.iter().map(|time| (time.clone(), -1)));

                    if !update.is_empty() {
                        read_capability_changes.insert(*id, update);
                    }
                }
            } else if let Ok(export) = self.export_mut(*id) {
                if PartialOrder::less_than(&export.write_frontier, new_upper) {
                    export.write_frontier = new_upper.clone();
                }

                let mut new_read_capability =
                    export.read_policy.frontier(export.write_frontier.borrow());

                if timely::order::PartialOrder::less_equal(
                    &export.read_capability,
                    &new_read_capability,
                ) {
                    let mut update = ChangeBatch::new();
                    update.extend(new_read_capability.iter().map(|time| (time.clone(), 1)));
                    std::mem::swap(&mut export.read_capability, &mut new_read_capability);
                    update.extend(new_read_capability.iter().map(|time| (time.clone(), -1)));

                    if !update.is_empty() {
                        read_capability_changes.insert(*id, update);
                    }
                }
            } else {
                panic!("Reference to absent collection {id}");
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
        let mut collections_net = BTreeMap::new();
        let mut exports_net = BTreeMap::new();

        // Repeatedly extract the maximum id, and updates for it.
        while let Some(key) = updates.keys().rev().next().cloned() {
            let mut update = updates.remove(&key).unwrap();
            if let Ok(collection) = self.collection_mut(key) {
                for (time, diff) in update.iter() {
                    assert!(
                        collection.read_capabilities.count_for(time) + diff >= 0,
                        "update {:?} for collection {key} would lead to negative \
                        read capabilities, read capabilities before applying: {:?}",
                        update,
                        collection.read_capabilities
                    );
                }

                let changes = collection.read_capabilities.update_iter(update.drain());
                update.extend(changes);

                for id in collection.storage_dependencies.iter() {
                    updates
                        .entry(*id)
                        .or_insert_with(ChangeBatch::new)
                        .extend(update.iter().cloned());
                }

                let (changes, frontier, _cluster_id) =
                    collections_net.entry(key).or_insert_with(|| {
                        (
                            ChangeBatch::new(),
                            Antichain::new(),
                            collection.cluster_id(),
                        )
                    });

                changes.extend(update.drain());
                *frontier = collection.read_capabilities.frontier().to_owned();
            } else if let Ok(export) = self.export_mut(key) {
                // Exports are not depended upon by other storage objects. We
                // only need to report changes in our own read_capability to our
                // dependencies.
                for id in export.storage_dependencies.iter() {
                    updates
                        .entry(*id)
                        .or_insert_with(ChangeBatch::new)
                        .extend(update.iter().cloned());
                }

                // Make sure we also send `AllowCompaction` commands for sinks,
                // which drives updating the sink's `as_of`, among other things.
                let (changes, frontier, _cluster_id) = exports_net
                    .entry(key)
                    .or_insert_with(|| (ChangeBatch::new(), Antichain::new(), export.cluster_id()));

                changes.extend(update.drain());
                *frontier = export.read_capability.clone();
            } else {
                // This is confusing and we should probably error.
                panic!("Unknown collection identifier {}", key);
            }
        }

        // Translate our net compute actions into `AllowCompaction` commands and
        // downgrade persist sinces. The actual downgrades are performed by a Tokio
        // task asynchorously.
        //
        // N.B. We only downgrade persist sinces for collections because
        // exports/sinks don't have an associated collection. We still _do_ want
        // to sent `AllowCompaction` commands to workers for them, though.
        let mut worker_compaction_commands = BTreeMap::default();
        let mut persist_compaction_commands = BTreeMap::default();
        for (key, (mut changes, frontier, cluster_id)) in collections_net {
            if !changes.is_empty() {
                worker_compaction_commands.insert(key, (frontier.clone(), cluster_id));
                persist_compaction_commands.insert(key, frontier);
            }
        }
        for (key, (mut changes, frontier, cluster_id)) in exports_net {
            if !changes.is_empty() {
                worker_compaction_commands.insert(key, (frontier, cluster_id));
            }
        }

        self.state
            .persist_read_handles
            .downgrade(persist_compaction_commands);

        for (id, (frontier, cluster_id)) in worker_compaction_commands {
            // Acquiring a client for a storage instance requires await, so we
            // instead stash these for later and process when we can.
            self.state
                .pending_compaction_commands
                .push((id, frontier, cluster_id));
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
        for (id, frontier, cluster_id) in self.state.pending_compaction_commands.drain(..) {
            // TODO(petrosagg): make this a strict check
            // TODO(aljoscha): What's up with this TODO?
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
            let status_row =
                healthcheck::pack_status_row(id, "dropped", None, (self.state.now)(), None);
            updates.push((status_row, 1));
        }
        self.append_to_managed_collection(source_status_history_id, updates)
            .await;

        // Record the drop status for all pending sink drops.
        let sink_status_history_id =
            self.state.introspection_ids[&IntrospectionType::SinkStatusHistory];
        let mut updates = vec![];
        for id in self.state.pending_sink_drops.drain(..) {
            let status_row =
                healthcheck::pack_status_row(id, "dropped", None, (self.state.now)(), None);
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
        persist_clients: Arc<PersistClientCache>,
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

    /// Return the since frontier at which we can read from all the given
    /// collections.
    ///
    /// The outer error is a potentially recoverable internal error, while the
    /// inner error is appropriate to return to the adapter.
    fn determine_collection_since_joins(
        &mut self,
        collections: &[GlobalId],
    ) -> Result<Antichain<T>, StorageError> {
        let mut joined_since = Antichain::from_elem(T::minimum());
        for id in collections {
            let collection = self.collection(*id)?;

            let since = collection.implied_capability.clone();
            joined_since.join_assign(&since);
        }

        Ok(joined_since)
    }

    /// Install read capabilities on the given `storage_dependencies`.
    #[tracing::instrument(level = "info", skip(self))]
    fn install_read_capabilities(
        &mut self,
        from_id: GlobalId,
        storage_dependencies: &[GlobalId],
        read_capability: Antichain<T>,
    ) -> Result<(), StorageError> {
        let mut changes = ChangeBatch::new();
        for time in read_capability.iter() {
            changes.update(time.clone(), 1);
        }

        // This is lifted out of `update_read_capabilitie` because it seems that
        // the compute controller is currently trying to acquire read holds for
        // times that are not beyond the current frontier of
        // `read_capabilities`.
        for id in storage_dependencies {
            let collection = self.collection(id.clone())?;
            let current_read_capabilities = collection.read_capabilities.frontier().to_owned();

            for time in read_capability.iter() {
                assert!(
                    current_read_capabilities.less_equal(time),
                    "trying to acquire hold on {:?} for collection {id} is trying to \
                    install read capabilities before the current \
                    frontier of read capabilities, read capabilities before applying: {:?}",
                    read_capability,
                    collection.read_capabilities
                );
            }
        }

        let mut storage_read_updates = storage_dependencies
            .iter()
            .map(|id| (*id, changes.clone()))
            .collect();

        self.update_read_capabilities(&mut storage_read_updates);

        Ok(())
    }

    /// Removes read holds that were previously acquired via
    /// `install_read_capabilities`.
    ///
    /// ## Panics
    ///
    /// This panics if there are no read capabilities at `capability` for all
    /// depended-upon collections.
    fn remove_read_capabilities(
        &mut self,
        capability: Antichain<T>,
        storage_dependencies: &[GlobalId],
    ) {
        let mut changes = ChangeBatch::new();
        for time in capability.iter() {
            changes.update(time.clone(), -1);
        }

        // Remove holds for all dependencies, which we previously acquired.
        let mut storage_read_updates = storage_dependencies
            .iter()
            .map(|id| (*id, changes.clone()))
            .collect();

        self.update_read_capabilities(&mut storage_read_updates);
    }

    /// Opens a write and critical since handles for the given `shard`.
    ///
    /// This will `halt!` the process if we cannot successfully acquire a
    /// critical handle with our current epoch.
    async fn open_data_handles(
        &self,
        purpose: &str,
        shard: ShardId,
        relation_desc: RelationDesc,
        persist_client: &PersistClient,
    ) -> (
        WriteHandle<SourceData, (), T, Diff>,
        SinceHandle<SourceData, (), T, Diff, PersistEpoch>,
    ) {
        let write = persist_client
            .open_writer(
                shard,
                purpose,
                Arc::new(relation_desc),
                Arc::new(UnitSchema),
            )
            .await
            .expect("invalid persist usage");

        // Construct the handle in a separate block to ensure all error paths are diverging
        let since_handle = {
            // This block's aim is to ensure the handle is in terms of our epoch
            // by the time we return it.
            let mut handle: SinceHandle<_, _, _, _, PersistEpoch> = persist_client
                .open_critical_since(shard, PersistClient::CONTROLLER_CRITICAL_SINCE, purpose)
                .await
                .expect("invalid persist usage");

            let since = handle.since().clone();

            let our_epoch = self.state.envd_epoch;

            loop {
                let current_epoch: PersistEpoch = handle.opaque().clone();

                // Ensure the current epoch is <= our epoch.
                let unchecked_success = current_epoch.0.map(|e| e <= our_epoch).unwrap_or(true);

                if unchecked_success {
                    // Update the handle's state so that it is in terms of our epoch.
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

        (write, since_handle)
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

    /// Updates the on-disk and in-memory representation of
    /// `DurableCollectionMetadata` (i.e. KV pairs in `METADATA_COLLECTION`
    /// on-disk and `all_current_metadata` as its in-memory representation) to
    /// include that of `upsert_state`, i.e. upserting the KV pairs in
    /// `upsert_state` into in `all_current_metadata`, as well as
    /// `METADATA_COLLECTION`.
    ///
    /// Any shards no longer referenced after the upsert will be finalized.
    ///
    /// Note that this function expects to be called:
    /// - While no source is currently using the shards identified in the
    ///   current metadata.
    /// - Before any sources begins using the shards identified in
    ///   `new_metadata`.
    async fn upsert_collection_metadata(
        &mut self,
        all_current_metadata: &mut BTreeMap<GlobalId, DurableCollectionMetadata>,
        upsert_state: BTreeMap<GlobalId, DurableCollectionMetadata>,
    ) {
        // If nothing changed, don't do any work, which might include async
        // calls into stash.
        if upsert_state.is_empty() {
            return;
        }

        let mut new_shards = BTreeSet::new();
        let mut dropped_shards = BTreeSet::new();
        let mut data_shards_to_replace = BTreeSet::new();
        let mut remap_shards_to_replace = BTreeSet::new();
        for (id, new_metadata) in upsert_state.iter() {
            assert!(
                new_metadata.remap_shard.is_none(),
                "must not reintroduce remap shards"
            );

            match all_current_metadata.get(id) {
                Some(metadata) => {
                    for (old, new, data_shard) in [
                        (
                            Some(metadata.data_shard),
                            Some(new_metadata.data_shard),
                            true,
                        ),
                        (metadata.remap_shard, new_metadata.remap_shard, false),
                    ] {
                        if old != new {
                            info!(
                                "replacing {:?}'s {} shard {:?} with {:?}",
                                id,
                                if data_shard { "data" } else { "remap" },
                                old,
                                new
                            );

                            if let Some(new) = new {
                                new_shards.insert(new);
                            }

                            if let Some(old) = old {
                                dropped_shards.insert(old);
                            }

                            if data_shard {
                                data_shards_to_replace.insert(*id);
                            } else {
                                remap_shards_to_replace.insert(*id);
                            }
                        }
                    }
                }
                // New collections, which might use an another collection's
                // dropped shard.
                None => {
                    new_shards.insert(new_metadata.data_shard);
                    continue;
                }
            };

            // Update the in-memory representation.
            all_current_metadata.insert(*id, new_metadata.clone());
        }

        // Reconcile dropped shards reference with shards that moved into a new
        // collection.
        dropped_shards.retain(|shard| !new_shards.contains(shard));

        // Ensure we don't leak any shards by tracking all of them we intend to
        // finalize.
        self.register_shards_for_finalization(dropped_shards.iter().cloned())
            .await;

        // Update the on-disk representation.
        METADATA_COLLECTION
            .upsert(&mut self.state.stash, upsert_state.into_iter())
            .await
            .expect("connect to stash");

        // Finalize any shards that are no longer-in use.
        self.finalize_shards(dropped_shards).await;

        // Update in-memory state for remap shards.
        for id in remap_shards_to_replace {
            let c = match self.collection_mut(id) {
                Ok(c) => c,
                Err(_) => continue,
            };

            c.collection_metadata.remap_shard = all_current_metadata[&id].remap_shard;
        }

        // Avoid taking lock if unnecessary
        if data_shards_to_replace.is_empty() {
            return;
        }

        let persist_client = self
            .persist
            .open(self.persist_location.clone())
            .await
            .unwrap();

        // Update the in-memory state for data shards
        for id in data_shards_to_replace {
            let c = match self.collection_mut(id) {
                Ok(c) => c,
                Err(_) => continue,
            };

            let data_shard = all_current_metadata[&id].data_shard;
            c.collection_metadata.data_shard = data_shard;

            let relation_desc = c.collection_metadata.relation_desc.clone();

            // This will halt! if any of the handles cannot be acquired
            // because we're not the leader anymore. But that's fine, we
            // already updated all the persistent state (in stash).
            let (write, since_handle) = self
                .open_data_handles(
                    format!("controller data for {id}").as_str(),
                    data_shard,
                    relation_desc,
                    &persist_client,
                )
                .await;

            self.state.persist_write_handles.update(id, write);
            self.state.persist_read_handles.update(id, since_handle);
        }
    }

    /// Closes the identified shards from further reads or writes.
    #[allow(dead_code)]
    async fn finalize_shards<I>(&mut self, shards: I)
    where
        I: IntoIterator<Item = ShardId> + Clone,
    {
        soft_assert!(
            {
                let mut all_registered = true;
                for shard in shards.clone() {
                    all_registered =
                        self.is_shard_registered_for_finalization(shard).await && all_registered
                }
                all_registered
            },
            "finalized shards must be registered before calling finalize_shards"
        );

        // Open a persist client to delete unused shards.
        let persist_client = self
            .persist
            .open(self.persist_location.clone())
            .await
            .unwrap();

        let persist_client = &persist_client;
        // Reborrow the `&mut self` as immutable, as all the concurrent work to be processed in
        // this stream cannot all have exclusive access.
        let this = &*self;

        use futures::stream::StreamExt;
        let finalized_shards: BTreeSet<_> = futures::stream::iter(shards)
            .map(|shard_id| async move {
                let (mut write, mut critical_since_handle) = this
                    .open_data_handles(
                        "finalizing shards",
                        shard_id,
                        RelationDesc::empty(),
                        persist_client,
                    )
                    .await;

                let our_epoch = PersistEpoch::from(this.state.envd_epoch);

                match critical_since_handle
                    .compare_and_downgrade_since(&our_epoch, (&our_epoch, &Antichain::new()))
                    .await
                {
                    Ok(_) => info!("successfully finalized read handle for shard {shard_id:?}"),
                    Err(e) => mz_ore::halt!("fenced by envd @ {e:?}. ours = {our_epoch:?}"),
                }

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
                }
                shard_id
            })
            // Poll each future for each collection concurrently, maximum of 10 at a time.
            .buffer_unordered(10)
            // HERE BE DRAGONS: see warning on other uses of buffer_unordered
            // before any changes to `collect`
            .collect()
            .await;

        self.clear_from_shard_finalization_register(finalized_shards)
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
    fn cluster_id(&self) -> Option<StorageInstanceId> {
        match &self.description.data_source {
            DataSource::Ingestion(ingestion) => Some(ingestion.instance_id),
            DataSource::Introspection(_) | DataSource::Other | DataSource::Progress => None,
        }
    }
}

/// State maintained about individual exports.
#[derive(Debug)]
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
    fn new(
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

    /// Returns the cluster to which the export is bound, if applicable.
    fn cluster_id(&self) -> Option<StorageInstanceId> {
        Some(self.description.instance_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_writes_by_zero() {
        let policy = ReadPolicy::lag_writes_by(mz_repr::Timestamp::default(), mz_repr::Timestamp::default());
        let write_frontier = Antichain::from_elem(mz_repr::Timestamp::from(5));
        assert_eq!(policy.frontier(write_frontier.borrow()), write_frontier);
    }
}
