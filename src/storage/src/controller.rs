// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs)]

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

use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::BufMut;
use derivative::Derivative;
use differential_dataflow::lattice::Lattice;
use futures::stream::StreamExt;
use proptest_derive::Arbitrary;
use prost::Message;
use serde::{Deserialize, Serialize};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use tokio::sync::Mutex;
use tokio_stream::StreamMap;

use mz_build_info::BuildInfo;
use mz_expr::PartitionId;
use mz_orchestrator::NamespacedOrchestrator;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::{PersistClient, PersistLocation, ShardId};
use mz_persist_types::{Codec, Codec64};
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use mz_repr::{Diff, GlobalId, RelationDesc, Row};
use mz_stash::{self, StashError, TypedCollection};

use crate::controller::hosts::{StorageHosts, StorageHostsConfig};
use crate::protocol::client::{
    ExportSinkCommand, IngestSourceCommand, ProtoStorageCommand, ProtoStorageResponse,
    StorageCommand, StorageResponse, Update,
};
use crate::types::errors::DataflowError;
use crate::types::hosts::{StorageHostConfig, StorageHostResourceAllocation};
use crate::types::sinks::{PersistSinkConnection, SinkAsOf, SinkConnection, SinkDesc};
use crate::types::sources::{IngestionDescription, MzOffset, SourceData, SourceEnvelope};

mod hosts;
mod rehydration;

include!(concat!(env!("OUT_DIR"), "/mz_storage.controller.rs"));

static METADATA_COLLECTION: TypedCollection<GlobalId, DurableCollectionMetadata> =
    TypedCollection::new("storage-collection-metadata");

/// Describes a request to create a source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionDescription<T> {
    /// The schema of this collection
    pub desc: RelationDesc,
    /// The description of the source to ingest into this collection, if any.
    pub ingestion: Option<IngestionDescription<()>>,
    /// An optional frontier to which the collection's `since` should be advanced.
    pub since: Option<Antichain<T>>,
    /// A GlobalId to use for this collection to use for the status collection.
    /// Used to keep track of source status/error information.
    pub status_collection_id: Option<GlobalId>,
    /// The address of a `storaged` process on which to install the source or the
    /// settings for spinning up a controller-managed process.
    pub host_config: Option<StorageHostConfig>,
}

impl<T> From<RelationDesc> for CollectionDescription<T> {
    fn from(desc: RelationDesc) -> Self {
        Self {
            desc,
            ingestion: None,
            since: None,
            status_collection_id: None,
            host_config: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExportDescription {
    pub sink: SinkDesc,
    /// The address of a `storaged` process on which to install the source.
    ///
    /// If `None`, the controller manages the lifetime of the `storaged`
    /// process.
    pub remote_addr: Option<String>,
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

    /// Acquire an immutable reference to the collection state, should it exist.
    fn collection(&self, id: GlobalId) -> Result<&CollectionState<Self::Timestamp>, StorageError>;

    /// Acquire a mutable reference to the collection state, should it exist.
    fn collection_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut CollectionState<Self::Timestamp>, StorageError>;

    /// Create the sources described in the individual CreateSourceCommand commands.
    ///
    /// Each command carries the source id, the source description, and any associated metadata
    /// needed to ingest the particular source.
    ///
    /// This command installs collection state for the indicated sources, and the are
    /// now valid to use in queries at times beyond the initial `since` frontiers. Each
    /// collection also acquires a read capability at this frontier, which will need to
    /// be repeatedly downgraded with `allow_compaction()` to permit compaction.
    async fn create_collections(
        &mut self,
        collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError>;

    /// Create the sinks described by the `ExportDescription`.
    async fn create_exports(
        &mut self,
        exports: Vec<(GlobalId, ExportDescription)>,
    ) -> Result<(), StorageError>;

    /// Drops the read capability for the sources and allows their resources to be reclaimed.
    async fn drop_sources(&mut self, identifiers: Vec<GlobalId>) -> Result<(), StorageError>;

    /// Drops the read capability for the sinks and allows their resources to be reclaimed.
    async fn drop_sinks(&mut self, identifiers: Vec<GlobalId>) -> Result<(), StorageError>;

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
    async fn drop_sources_unvalidated(
        &mut self,
        identifiers: Vec<GlobalId>,
    ) -> Result<(), StorageError>;

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
        &mut self,
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
    /// Identifiers not present in `policies` retain their existing read policies.
    async fn set_read_policy(
        &mut self,
        policies: Vec<(GlobalId, ReadPolicy<Self::Timestamp>)>,
    ) -> Result<(), StorageError>;

    /// Accept write frontier updates from the compute layer.
    async fn update_write_frontiers(
        &mut self,
        updates: &[(GlobalId, ChangeBatch<Self::Timestamp>)],
    ) -> Result<(), StorageError>;

    /// Applies `updates` and sends any appropriate compaction command.
    async fn update_read_capabilities(
        &mut self,
        updates: &mut BTreeMap<GlobalId, ChangeBatch<Self::Timestamp>>,
    ) -> Result<(), StorageError>;

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
                if lag != 0 {
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
#[derive(Arbitrary, Clone, Debug, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize)]
pub struct CollectionMetadata {
    /// The persist location where the shards are located
    pub persist_location: PersistLocation,
    /// The persist shard id of the remap collection used to reclock this collection
    pub remap_shard: ShardId,
    /// The persist shard containing the contents of this storage collection
    pub data_shard: ShardId,
    /// The persist shard containing the status updates for this storage collection
    pub status_shard: Option<ShardId>,
}

impl RustType<ProtoCollectionMetadata> for CollectionMetadata {
    fn into_proto(&self) -> ProtoCollectionMetadata {
        ProtoCollectionMetadata {
            blob_uri: self.persist_location.blob_uri.clone(),
            consensus_uri: self.persist_location.consensus_uri.clone(),
            data_shard: self.data_shard.to_string(),
            remap_shard: self.remap_shard.to_string(),
            status_shard: self.status_shard.map(|s| s.to_string()),
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
        })
    }
}

impl Codec for CollectionMetadata {
    fn codec_name() -> String {
        "protobuf[CollectionMetadata]".into()
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        self.into_proto()
            .encode(buf)
            .expect("no required fields means no initialization errors");
    }

    fn decode(buf: &[u8]) -> Result<Self, String> {
        let proto = ProtoCollectionMetadata::decode(buf).map_err(|err| err.to_string())?;
        proto.into_rust().map_err(|err| err.to_string())
    }
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

impl Codec for DurableCollectionMetadata {
    fn codec_name() -> String {
        "protobuf[DurableCollectionMetadata]".into()
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        self.into_proto()
            .encode(buf)
            .expect("no required fields means no initialization errors");
    }

    fn decode(buf: &[u8]) -> Result<Self, String> {
        let proto = ProtoDurableCollectionMetadata::decode(buf).map_err(|err| err.to_string())?;
        proto.into_rust().map_err(|err| err.to_string())
    }
}

/// Controller state maintained for each storage instance.
#[derive(Debug)]
pub struct StorageControllerState<
    T: Timestamp + Lattice + Codec64,
    S = mz_stash::Memory<mz_stash::Postgres>,
> {
    /// Collections maintained by the storage controller.
    ///
    /// This collection only grows, although individual collections may be rendered unusable.
    /// This is to prevent the re-binding of identifiers to other descriptions.
    pub(super) collections: BTreeMap<GlobalId, CollectionState<T>>,
    pub(super) stash: S,
    /// Write handle for persist shards.
    pub(super) persist_write_handles: persist_write_handles::PersistWorker<T>,
    /// Read handles for persist shards.
    ///
    /// These handles are on the other end of a Tokio task, so that work can be done asynchronously
    /// without blocking the storage controller.
    persist_read_handles: persist_read_handles::PersistWorker<T>,
    stashed_response: Option<StorageResponse<T>>,
}

/// A storage controller for a storage instance.
#[derive(Debug)]
pub struct Controller<T: Timestamp + Lattice + Codec64 + Unpin> {
    state: StorageControllerState<T>,
    /// Storage host provisioning and storage object assignment.
    hosts: StorageHosts<T>,
    /// Mechanism for returning frontier advancement for tables.
    internal_response_queue: tokio::sync::mpsc::UnboundedReceiver<StorageResponse<T>>,
    /// The persist location where all storage collections are being written to
    persist_location: PersistLocation,
    /// A persist client used to write to storage collections
    persist_client: PersistClient,
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
    /// The expected upper of an append was different than the actual append of the collection
    InvalidUpper(GlobalId),
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
            Self::InvalidUpper(_) => None,
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
            Self::IdentifierMissing(id) => write!(f, "source identifier is not present: {id}"),
            Self::UpdateBeyondUpper(id) => {
                write!(
                    f,
                    "append batch for {id} contained update at or beyond its upper"
                )
            }
            Self::ReadBeforeSince(id) => {
                write!(f, "read for {id} was at a timestamp before its since")
            }
            Self::InvalidUpper(id) => {
                write!(
                    f,
                    "expected upper for {id} was different than its actual upper"
                )
            }
            Self::ClientError(err) => write!(f, "underlying client error: {err}"),
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

impl<T: Timestamp + Lattice + Codec64> StorageControllerState<T> {
    pub(super) async fn new(
        postgres_url: String,
        tx: tokio::sync::mpsc::UnboundedSender<StorageResponse<T>>,
    ) -> Self {
        let tls = mz_postgres_util::make_tls(
            &tokio_postgres::config::Config::from_str(&postgres_url)
                .expect("invalid postgres url for storage stash"),
        )
        .expect("could not make storage TLS connection");
        let stash = mz_stash::Postgres::new(postgres_url, None, tls)
            .await
            .expect("could not connect to postgres storage stash");
        let stash = mz_stash::Memory::new(stash);
        Self {
            collections: BTreeMap::default(),
            stash,
            persist_write_handles: persist_write_handles::PersistWorker::new(tx),
            persist_read_handles: persist_read_handles::PersistWorker::new(),
            stashed_response: None,
        }
    }
}

#[async_trait(?Send)]
impl<T> StorageController for Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + TryInto<i64> + TryFrom<i64> + Codec64 + Unpin,
    <T as TryInto<i64>>::Error: std::fmt::Debug,
    <T as TryFrom<i64>>::Error: std::fmt::Debug,

    // Required to setup grpc clients for new storaged instances.
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,
{
    type Timestamp = T;

    fn initialization_complete(&mut self) {
        self.hosts.initialization_complete();
    }

    fn collection(&self, id: GlobalId) -> Result<&CollectionState<T>, StorageError> {
        self.state
            .collections
            .get(&id)
            .ok_or(StorageError::IdentifierMissing(id))
    }

    fn collection_mut(&mut self, id: GlobalId) -> Result<&mut CollectionState<T>, StorageError> {
        self.state
            .collections
            .get_mut(&id)
            .ok_or(StorageError::IdentifierMissing(id))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn create_collections(
        &mut self,
        mut collections: Vec<(GlobalId, CollectionDescription<T>)>,
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
        for (id, description) in collections {
            let durable_metadata = METADATA_COLLECTION
                .insert_without_overwrite(
                    &mut self.state.stash,
                    &id,
                    DurableCollectionMetadata {
                        remap_shard: ShardId::new(),
                        data_shard: ShardId::new(),
                    },
                )
                .await?;

            let status_shard = if let Some(status_collection_id) = description.status_collection_id
            {
                Some(
                    METADATA_COLLECTION
                        .peek_key_one(&mut self.state.stash, &status_collection_id)
                        .await?
                        .ok_or(StorageError::IdentifierMissing(status_collection_id))?
                        .data_shard,
                )
            } else {
                None
            };

            let metadata = CollectionMetadata {
                persist_location: self.persist_location.clone(),
                remap_shard: durable_metadata.remap_shard,
                data_shard: durable_metadata.data_shard,
                status_shard,
            };

            let (write, mut read) = self
                .persist_client
                .open(metadata.data_shard)
                .await
                .expect("invalid persist usage");

            // Advance the collection's `since` as requested.
            if let Some(since) = &description.since {
                read.downgrade_since(since.clone()).await;
            }

            let collection_state =
                CollectionState::new(description.clone(), read.since().clone(), metadata);

            self.state.persist_write_handles.register(id, write);
            self.state.persist_read_handles.register(id, read);

            self.state.collections.insert(id, collection_state);

            if let Some(ingestion) = description.ingestion {
                // Each ingestion is augmented with the collection metadata.
                let mut source_imports = BTreeMap::new();
                for (id, _) in ingestion.source_imports {
                    let metadata = self.collection(id)?.collection_metadata.clone();
                    source_imports.insert(id, metadata);
                }

                let metadata = self.collection(id)?.collection_metadata.clone();

                // Calculate the point at which we can resume ingestion computing the greatest
                // antichain that is less or equal to all state and output shard uppers.
                let mut resume_upper: Antichain<T> = Antichain::new();
                let remap_write = self
                    .persist_client
                    .open_writer::<(), PartitionId, T, MzOffset>(metadata.remap_shard)
                    .await
                    .unwrap();
                for t in remap_write.upper().elements() {
                    resume_upper.insert(t.clone());
                }
                let data_write = self
                    .persist_client
                    .open_writer::<SourceData, (), T, Diff>(metadata.data_shard)
                    .await
                    .unwrap();
                for t in data_write.upper().elements() {
                    resume_upper.insert(t.clone());
                }

                // Check if this ingestion is using any operators that are stateful AND are not
                // storing their state in persist shards. This whole section should be eventually
                // removed as we make each operator durably record its state in persist shards.
                let resume_upper = match ingestion.desc.envelope {
                    // We can only resume with the None envelope right now which is stateless
                    SourceEnvelope::None(_) => resume_upper,
                    // Otherwise re-ingest everything
                    _ => Antichain::from_elem(T::minimum()),
                };

                let augmented_ingestion = IngestSourceCommand {
                    id,
                    description: IngestionDescription {
                        source_imports,
                        storage_metadata: self.collection(id)?.collection_metadata.clone(),
                        // The rest of the fields are identical
                        desc: ingestion.desc,
                        typ: description.desc.typ().clone(),
                    },
                    resume_upper,
                };

                // Provision a storage host for the ingestion.
                let client = self
                    .hosts
                    .provision(
                        id,
                        description.host_config.clone().expect(
                            "CollectionDescription with ingestion should have host_config set",
                        ),
                    )
                    .await?;
                client.send(StorageCommand::IngestSources(vec![augmented_ingestion]));
            }
        }

        Ok(())
    }

    async fn create_exports(
        &mut self,
        exports: Vec<(GlobalId, ExportDescription)>,
    ) -> Result<(), StorageError> {
        for (id, description) in exports {
            let augmented_sink_desc = SinkDesc {
                from: description.sink.from,
                from_desc: description.sink.from_desc,
                connection: match description.sink.connection {
                    SinkConnection::Kafka(k) => SinkConnection::Kafka(k),
                    SinkConnection::Tail(t) => SinkConnection::Tail(t),
                    SinkConnection::Persist(PersistSinkConnection {
                        value_desc,
                        storage_metadata: (),
                    }) => SinkConnection::Persist(PersistSinkConnection {
                        value_desc,
                        storage_metadata: self.collection(id)?.collection_metadata.clone(),
                    }),
                },
                envelope: description.sink.envelope,
                // TODO(chae): derive this (sink_description.as_of is hardcoded to u64 instead of general timestamp T)
                as_of: SinkAsOf {
                    frontier: Antichain::from_elem(T::minimum()),
                    strict: description.sink.as_of.strict,
                },
            };
            let cmd = ExportSinkCommand {
                id,
                description: augmented_sink_desc,
                // TODO(chae): derive this
                resume_upper: Antichain::from_elem(T::minimum()),
            };
            // TODO: allow specifying a size parameter for sinks, tracked in #13889
            let host_config = match description.remote_addr {
                Some(addr) => StorageHostConfig::Remote { addr },
                None => StorageHostConfig::Managed {
                    allocation: StorageHostResourceAllocation::temp_default_for_sinks(),
                    size: "arbitrary".to_string(),
                },
            };
            // Provision a storage host for the ingestion.
            let client = self.hosts.provision(id, host_config).await?;

            client.send(StorageCommand::ExportSinks(vec![cmd]));
        }
        Ok(())
    }

    async fn drop_sources(&mut self, identifiers: Vec<GlobalId>) -> Result<(), StorageError> {
        self.validate_ids(identifiers.iter().cloned())?;
        let policies = identifiers
            .into_iter()
            .map(|id| (id, ReadPolicy::ValidFrom(Antichain::new())))
            .collect();
        self.set_read_policy(policies).await?;
        Ok(())
    }

    async fn drop_sources_unvalidated(
        &mut self,
        identifiers: Vec<GlobalId>,
    ) -> Result<(), StorageError> {
        let policies = identifiers
            .into_iter()
            .map(|id| (id, ReadPolicy::ValidFrom(Antichain::new())))
            .collect();
        self.set_read_policy(policies).await?;
        Ok(())
    }

    /// Drops the read capability for the sinks and allows their resources to be reclaimed.
    async fn drop_sinks(&mut self, identifiers: Vec<GlobalId>) -> Result<(), StorageError> {
        self.validate_ids(identifiers.iter().cloned())?;
        let policies = identifiers
            .into_iter()
            .map(|id| (id, ReadPolicy::ValidFrom(Antichain::new())))
            .collect();
        self.set_read_policy(policies).await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn append(
        &mut self,
        commands: Vec<(GlobalId, Vec<Update<Self::Timestamp>>, Self::Timestamp)>,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<(), StorageError>>, StorageError> {
        // TODO(petrosagg): validate appends against the expected RelationDesc of the collection
        for (id, updates, batch_upper) in commands.iter() {
            for update in updates.iter() {
                if !update.timestamp.less_than(&batch_upper) {
                    return Err(StorageError::UpdateBeyondUpper(*id));
                }
            }
        }

        Ok(self.state.persist_write_handles.append(commands))
    }

    async fn snapshot(
        &mut self,
        id: GlobalId,
        as_of: Self::Timestamp,
    ) -> Result<Vec<(Row, Diff)>, StorageError> {
        // TODO: replace this with a new tokio task, rather than occupying
        // the existing read downgrader.
        let as_of = Antichain::from_elem(as_of);
        self.state
            .persist_read_handles
            .snapshot(id, as_of)
            .await
            .unwrap()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn set_read_policy(
        &mut self,
        policies: Vec<(GlobalId, ReadPolicy<T>)>,
    ) -> Result<(), StorageError> {
        let mut read_capability_changes = BTreeMap::default();
        for (id, policy) in policies.into_iter() {
            if let Ok(collection) = self.collection_mut(id) {
                let mut new_read_capability = policy.frontier(collection.write_frontier.frontier());

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
            } else {
                tracing::error!("Reference to unregistered id: {:?}", id);
            }
        }
        if !read_capability_changes.is_empty() {
            self.update_read_capabilities(&mut read_capability_changes)
                .await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn update_write_frontiers(
        &mut self,
        updates: &[(GlobalId, ChangeBatch<T>)],
    ) -> Result<(), StorageError> {
        let mut read_capability_changes = BTreeMap::default();
        for (id, changes) in updates.iter() {
            let collection = self
                .collection_mut(*id)
                .expect("Reference to absent collection");

            collection
                .write_frontier
                .update_iter(changes.clone().drain());

            let mut new_read_capability = collection
                .read_policy
                .frontier(collection.write_frontier.frontier());
            if PartialOrder::less_equal(&collection.implied_capability, &new_read_capability) {
                // TODO: reuse change batch above?
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
            self.update_read_capabilities(&mut read_capability_changes)
                .await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn update_read_capabilities(
        &mut self,
        updates: &mut BTreeMap<GlobalId, ChangeBatch<T>>,
    ) -> Result<(), StorageError> {
        // Location to record consequences that we need to act on.
        let mut storage_net = HashMap::new();
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
            if let Some(client) = self.hosts.client(id) {
                client.send(StorageCommand::AllowCompaction(vec![(
                    id,
                    frontier.clone(),
                )]));

                if frontier.is_empty() {
                    self.hosts.deprovision(id).await?;
                }
            }
        }

        Ok(())
    }

    async fn ready(&mut self) {
        let mut clients = self
            .hosts
            .clients()
            .map(|client| client.response_stream())
            .enumerate()
            .collect::<StreamMap<_, _>>();

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
            None => Ok(()),
            Some(StorageResponse::FrontierUppers(updates)) => {
                self.update_write_frontiers(&updates).await?;
                Ok(())
            }
        }
    }
}

impl<T> Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + TryInto<i64> + TryFrom<i64> + Codec64 + Unpin,
    <T as TryInto<i64>>::Error: std::fmt::Debug,
    <T as TryFrom<i64>>::Error: std::fmt::Debug,

    // Required to setup grpc clients for new storaged instances.
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,
{
    /// Create a new storage controller from a client it should wrap.
    pub async fn new(
        build_info: &'static BuildInfo,
        postgres_url: String,
        persist_location: PersistLocation,
        persist_clients: Arc<Mutex<PersistClientCache>>,
        orchestrator: Arc<dyn NamespacedOrchestrator>,
        storaged_image: String,
    ) -> Self {
        let persist_client = persist_clients
            .lock()
            .await
            .open(persist_location.clone())
            .await
            .unwrap();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        Self {
            state: StorageControllerState::new(postgres_url, tx).await,
            hosts: StorageHosts::new(StorageHostsConfig {
                build_info,
                orchestrator,
                storaged_image,
            }),
            internal_response_queue: rx,
            persist_location,
            persist_client,
        }
    }

    /// Validate that a collection exists for all identifiers, and error if any do not.
    fn validate_ids(&self, ids: impl Iterator<Item = GlobalId>) -> Result<(), StorageError> {
        for id in ids {
            self.collection(id)?;
        }
        Ok(())
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
    /// The implicit capability associated with collection creation.
    pub implied_capability: Antichain<T>,
    /// The policy to use to downgrade `self.implied_capability`.
    pub read_policy: ReadPolicy<T>,

    /// Reported progress in the write capabilities.
    ///
    /// Importantly, this is not a write capability, but what we have heard about the
    /// write capabilities of others. All future writes will have times greater than or
    /// equal to `write_frontier.frontier()`.
    pub write_frontier: MutableAntichain<T>,

    pub collection_metadata: CollectionMetadata,
}

impl<T: Timestamp> CollectionState<T> {
    /// Creates a new collection state, with an initial read policy valid from `since`.
    pub fn new(
        description: CollectionDescription<T>,
        since: Antichain<T>,
        metadata: CollectionMetadata,
    ) -> Self {
        let mut read_capabilities = MutableAntichain::new();
        read_capabilities.update_iter(since.iter().map(|time| (time.clone(), 1)));
        Self {
            description,
            read_capabilities,
            implied_capability: since.clone(),
            read_policy: ReadPolicy::ValidFrom(since),
            write_frontier: MutableAntichain::new_bottom(Timestamp::minimum()),
            collection_metadata: metadata,
        }
    }
}

mod persist_read_handles {

    use std::collections::BTreeMap;

    use differential_dataflow::lattice::Lattice;
    use futures::stream::FuturesUnordered;
    use futures_util::StreamExt;
    use timely::progress::{Antichain, Timestamp};
    use tokio::sync::mpsc::UnboundedSender;

    use mz_persist_client::read::ReadHandle;
    use mz_persist_types::Codec64;
    use mz_repr::Row;
    use mz_repr::{Diff, GlobalId};

    use crate::controller::StorageError;
    use crate::types::sources::SourceData;

    /// A wrapper that holds on to backing persist shards/collections that the
    /// storage controller is aware of. The handles hold back the since frontier and
    /// we need to downgrade them when the read capabilities change.
    ///
    /// Internally, this has an async task and the methods for registering a handle
    /// and downgrading sinces add commands to a queue that this task is working
    /// off. This makes the methods non-blocking and moves the work outside the main
    /// coordinator task, meaning the coordinator is spending less time waiting on
    /// persist calls.
    #[derive(Debug)]
    pub struct PersistWorker<T: Timestamp + Lattice + Codec64> {
        tx: UnboundedSender<PersistWorkerCmd<T>>,
    }

    impl<T> Drop for PersistWorker<T>
    where
        T: Timestamp + Lattice + Codec64,
    {
        fn drop(&mut self) {
            self.send(PersistWorkerCmd::Shutdown);
            // TODO: Can't easily block on shutdown occurring.
        }
    }

    /// Commands for [PersistWorker].
    #[derive(Debug)]
    enum PersistWorkerCmd<T: Timestamp + Lattice + Codec64> {
        Register(GlobalId, ReadHandle<SourceData, (), T, Diff>),
        Downgrade(BTreeMap<GlobalId, Antichain<T>>),
        Snapshot(
            GlobalId,
            Antichain<T>,
            tokio::sync::oneshot::Sender<Result<Vec<(Row, Diff)>, StorageError>>,
        ),
        Shutdown,
    }

    impl<T: Timestamp + Lattice + Codec64> PersistWorker<T> {
        pub(crate) fn new() -> Self {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

            mz_ore::task::spawn(|| "PersistReadHandles", async move {
                let mut read_handles: BTreeMap<GlobalId, ReadHandle<SourceData, (), T, Diff>> =
                    BTreeMap::new();

                while let Some(cmd) = rx.recv().await {
                    // Peel off all available commands.
                    // This allows us to catch up if we fall behind on downgrade commands.
                    let mut commands = vec![cmd];
                    while let Ok(cmd) = rx.try_recv() {
                        commands.push(cmd);
                    }
                    // Collect all downgrade requests and apply them last.
                    let mut downgrades = BTreeMap::default();
                    let mut shutdown = false;

                    for command in commands {
                        match command {
                            PersistWorkerCmd::Register(id, read_handle) => {
                                let previous = read_handles.insert(id, read_handle);
                                if previous.is_some() {
                                    panic!(
                                        "already registered a ReadHandle for collection {:?}",
                                        id
                                    );
                                }
                            }
                            PersistWorkerCmd::Downgrade(since_frontiers) => {
                                for (id, frontier) in since_frontiers {
                                    downgrades.insert(id, frontier);
                                }
                            }
                            PersistWorkerCmd::Snapshot(id, as_of, oneshot) => {
                                async fn snapshot<T2: Timestamp + Lattice + Codec64>(
                                    read_handle: &mut ReadHandle<SourceData, (), T2, Diff>,
                                    id: GlobalId,
                                    as_of: Antichain<T2>,
                                ) -> Result<Vec<(Row, Diff)>, StorageError>
                                {
                                    let mut snapshot = read_handle
                                        .snapshot(as_of)
                                        .await
                                        .map_err(|_| StorageError::ReadBeforeSince(id))?;

                                    let mut contents = Vec::new();

                                    while let Some(updates) = snapshot.next().await {
                                        for ((source_data, _pid), _ts, diff) in updates {
                                            let row =
                                                source_data.expect("cannot read snapshot").0?;
                                            contents.push((row, diff));
                                        }
                                    }

                                    Ok(contents)
                                }

                                if let Some(read_handle) = read_handles.get_mut(&id) {
                                    let result = snapshot(read_handle, id, as_of).await;
                                    oneshot.send(result).expect("Oneshot should not fail");
                                } else {
                                    oneshot
                                        .send(Err(StorageError::IdentifierMissing(id)))
                                        .expect("Oneshot should not fail");
                                }
                            }
                            PersistWorkerCmd::Shutdown => {
                                shutdown = true;
                            }
                        }
                    }

                    if !downgrades.is_empty() {
                        let futs = FuturesUnordered::new();

                        for (id, read) in read_handles.iter_mut() {
                            if let Some(since) = downgrades.remove(id) {
                                let fut = async move {
                                    read.downgrade_since(since).await;
                                };

                                futs.push(fut);
                            }
                        }

                        assert!(downgrades.is_empty());
                        futs.collect::<Vec<_>>().await;
                    }
                    if shutdown {
                        tracing::trace!("shutting down persist since downgrade task");
                        break;
                    }
                }
            });

            Self { tx }
        }

        pub(crate) fn register(
            &self,
            id: GlobalId,
            read_handle: ReadHandle<SourceData, (), T, Diff>,
        ) {
            self.send(PersistWorkerCmd::Register(id, read_handle))
        }

        pub(crate) fn downgrade(&self, frontiers: BTreeMap<GlobalId, Antichain<T>>) {
            self.send(PersistWorkerCmd::Downgrade(frontiers))
        }

        pub(crate) fn snapshot(
            &self,
            id: GlobalId,
            since: Antichain<T>,
        ) -> tokio::sync::oneshot::Receiver<Result<Vec<(Row, Diff)>, StorageError>> {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.send(PersistWorkerCmd::Snapshot(id, since, tx));
            rx
        }

        fn send(&self, cmd: PersistWorkerCmd<T>) {
            match self.tx.send(cmd) {
                Ok(()) => (), // All good!
                Err(e) => {
                    tracing::error!("could not forward command: {:?}", e);
                }
            }
        }
    }
}

mod persist_write_handles {

    use std::collections::BTreeMap;

    use differential_dataflow::lattice::Lattice;
    use futures::stream::FuturesUnordered;
    use timely::progress::{Antichain, Timestamp};
    use tokio::sync::mpsc::UnboundedSender;

    use mz_persist_client::write::WriteHandle;
    use mz_persist_types::Codec64;
    use mz_repr::{Diff, GlobalId};

    use crate::controller::StorageError;
    use crate::protocol::client::StorageResponse;
    use crate::protocol::client::Update;
    use crate::types::sources::SourceData;

    #[derive(Debug)]
    pub struct PersistWorker<T: Timestamp + Lattice + Codec64> {
        tx: UnboundedSender<PersistWorkerCmd<T>>,
    }

    impl<T> Drop for PersistWorker<T>
    where
        T: Timestamp + Lattice + Codec64,
    {
        fn drop(&mut self) {
            self.send(PersistWorkerCmd::Shutdown);
            // TODO: Can't easily block on shutdown occurring.
        }
    }

    /// Commands for [PersistWorker].
    #[derive(Debug)]
    enum PersistWorkerCmd<T: Timestamp + Lattice + Codec64> {
        Register(GlobalId, WriteHandle<SourceData, (), T, Diff>),
        Append(
            Vec<(GlobalId, Vec<Update<T>>, T)>,
            tokio::sync::oneshot::Sender<Result<(), StorageError>>,
        ),
        Shutdown,
    }

    impl<T: Timestamp + Lattice + Codec64> PersistWorker<T> {
        pub(crate) fn new(
            mut frontier_responses: tokio::sync::mpsc::UnboundedSender<StorageResponse<T>>,
        ) -> Self {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

            mz_ore::task::spawn(|| "PersistWriteHandles", async move {
                let mut write_handles = BTreeMap::new();

                while let Some(cmd) = rx.recv().await {
                    // Peel off all available commands.
                    // We do this in case we can consolidate commands.
                    // It would be surprising to receive multiple concurrent `Append` commands,
                    // but we might receive multiple *empty* `Append` commands.
                    let mut commands = vec![cmd];
                    while let Ok(cmd) = rx.try_recv() {
                        commands.push(cmd);
                    }

                    // Accumulated updates and upper frontier.
                    let mut all_updates = BTreeMap::default();
                    let mut all_responses = Vec::default();

                    let mut shutdown = false;

                    for command in commands {
                        match command {
                            PersistWorkerCmd::Register(id, write_handle) => {
                                let previous = write_handles
                                    .insert(id, (write_handle, Antichain::from_elem(T::minimum())));
                                if previous.is_some() {
                                    panic!(
                                        "already registered a ReadHandle for collection {:?}",
                                        id
                                    );
                                }
                            }
                            PersistWorkerCmd::Append(updates, response) => {
                                for (id, update, upper) in updates {
                                    let (updates, old_upper) =
                                        all_updates.entry(id).or_insert_with(|| {
                                            (Vec::default(), Antichain::from_elem(T::minimum()))
                                        });

                                    updates.extend(update);
                                    old_upper.join_assign(&Antichain::from_elem(upper));
                                }
                                all_responses.push(response);
                            }
                            PersistWorkerCmd::Shutdown => {
                                shutdown = true;
                            }
                        }
                    }

                    async fn append_work<T2: Timestamp + Lattice + Codec64>(
                        frontier_responses: &mut tokio::sync::mpsc::UnboundedSender<
                            StorageResponse<T2>,
                        >,
                        write_handles: &mut BTreeMap<
                            GlobalId,
                            (WriteHandle<SourceData, (), T2, Diff>, Antichain<T2>),
                        >,
                        mut commands: BTreeMap<GlobalId, (Vec<Update<T2>>, Antichain<T2>)>,
                    ) -> Result<(), GlobalId> {
                        let futs = FuturesUnordered::new();

                        // We cannot iterate through the updates and then set off a persist call
                        // on the write handle because we cannot mutably borrow the write handle
                        // multiple times.
                        //
                        // Instead, we first group the update by ID above and then iterate
                        // through all available write handles and see if there are any updates
                        // for it. If yes, we send them all in one go.
                        for (id, (write, old_upper)) in write_handles.iter_mut() {
                            if let Some((updates, new_upper)) = commands.remove(id) {
                                let persist_upper = write.upper().clone();
                                let updates = updates
                                    .into_iter()
                                    .map(|u| ((SourceData(Ok(u.row)), ()), u.timestamp, u.diff));

                                futs.push(async move {
                                    write
                                        .compare_and_append(
                                            updates,
                                            persist_upper.clone(),
                                            new_upper.clone(),
                                        )
                                        .await
                                        .expect("cannot append updates")
                                        .expect("cannot append updates")
                                        .or(Err(*id))?;

                                    let mut change_batch = timely::progress::ChangeBatch::new();
                                    change_batch.extend(new_upper.iter().cloned().map(|t| (t, 1)));
                                    change_batch.extend(old_upper.iter().cloned().map(|t| (t, -1)));
                                    old_upper.clone_from(&new_upper);

                                    Ok::<_, GlobalId>((*id, change_batch))
                                })
                            }
                        }

                        use futures_util::TryStreamExt;
                        let change_batches = futs.try_collect::<Vec<_>>().await?;

                        // It is not strictly an error for the controller to hang up.
                        let _ = frontier_responses
                            .send(StorageResponse::FrontierUppers(change_batches));

                        Ok(())
                    }

                    let result =
                        append_work(&mut frontier_responses, &mut write_handles, all_updates).await;

                    // It is not an error for the other end to hang up.
                    for response in all_responses {
                        let _ =
                            response.send(result.clone().map_err(StorageError::IdentifierMissing));
                    }

                    if shutdown {
                        tracing::trace!("shutting down persist write append task");
                        break;
                    }
                }
            });

            Self { tx }
        }

        pub(crate) fn register(
            &self,
            id: GlobalId,
            write_handle: WriteHandle<SourceData, (), T, Diff>,
        ) {
            self.send(PersistWorkerCmd::Register(id, write_handle))
        }

        pub(crate) fn append(
            &self,
            updates: Vec<(GlobalId, Vec<Update<T>>, T)>,
        ) -> tokio::sync::oneshot::Receiver<Result<(), StorageError>> {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.send(PersistWorkerCmd::Append(updates, tx));
            rx
        }

        fn send(&self, cmd: PersistWorkerCmd<T>) {
            match self.tx.send(cmd) {
                Ok(()) => (), // All good!
                Err(e) => {
                    tracing::error!("could not forward command: {:?}", e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_writes_by_zero() {
        let policy = ReadPolicy::lag_writes_by(0);
        let write_frontier = Antichain::from_elem(5);
        assert_eq!(policy.frontier(write_frontier.borrow()), write_frontier);
    }
}
