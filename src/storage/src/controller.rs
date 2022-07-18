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
use futures::future;
use futures::stream::TryStreamExt as _;
use futures::stream::{FuturesUnordered, StreamExt};
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
use mz_persist_client::{
    read::ReadHandle, write::WriteHandle, PersistClient, PersistLocation, ShardId,
};
use mz_persist_types::{Codec, Codec64};
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use mz_repr::{Diff, GlobalId, RelationDesc, Row};
use mz_stash::{self, StashError, TypedCollection};

use crate::controller::hosts::{StorageHosts, StorageHostsConfig};
use crate::protocol::client::{
    IngestSourceCommand, ProtoStorageCommand, ProtoStorageResponse, StorageCommand,
    StorageResponse, Update,
};
use crate::types::errors::DataflowError;
use crate::types::sources::{IngestionDescription, MzOffset, SourceData, SourceEnvelope};

mod hosts;
mod rehydration;

include!(concat!(env!("OUT_DIR"), "/mz_storage.controller.rs"));

static METADATA_COLLECTION: TypedCollection<GlobalId, CollectionMetadata> =
    TypedCollection::new("storage-collection-metadata");

/// Describes a request to create a source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionDescription<T> {
    /// The schema of this collection
    pub desc: RelationDesc,
    /// The description of the source to ingest into this collection, if any.
    pub ingestion: Option<IngestionDescription<()>>,
    /// The address of a `storaged` process on which to install the source.
    ///
    /// If `None`, the controller manages the lifetime of the `storaged`
    /// process.
    pub remote_addr: Option<String>,
    /// An optional frontier to which the collection's `since` should be advanced.
    pub since: Option<Antichain<T>>,
}

impl<T> From<RelationDesc> for CollectionDescription<T> {
    fn from(desc: RelationDesc) -> Self {
        Self {
            desc,
            ingestion: None,
            remote_addr: None,
            since: None,
        }
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

    /// Drops the read capability for the sources and allows their resources to be reclaimed.
    async fn drop_sources(&mut self, identifiers: Vec<GlobalId>) -> Result<(), StorageError>;

    /// Append `updates` into the local input named `id` and advance its upper to `upper`.
    // TODO(petrosagg): switch upper to `Antichain<Timestamp>`
    async fn append(
        &mut self,
        commands: Vec<(GlobalId, Vec<Update<Self::Timestamp>>, Self::Timestamp)>,
    ) -> Result<(), StorageError>;

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
}

impl RustType<ProtoCollectionMetadata> for CollectionMetadata {
    fn into_proto(&self) -> ProtoCollectionMetadata {
        ProtoCollectionMetadata {
            blob_uri: self.persist_location.blob_uri.clone(),
            consensus_uri: self.persist_location.consensus_uri.clone(),
            data_shard: self.data_shard.to_string(),
            remap_shard: self.remap_shard.to_string(),
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
    pub(super) persist_handles: BTreeMap<GlobalId, PersistHandles<T>>,
    stashed_response: Option<StorageResponse<T>>,
}

/// A storage controller for a storage instance.
#[derive(Debug)]
pub struct Controller<T: Timestamp + Lattice + Codec64 + Unpin> {
    state: StorageControllerState<T>,
    /// Storage host provisioning and storage object assignment.
    hosts: StorageHosts<T>,
    /// The persist location where all storage collections are being written to
    persist_location: PersistLocation,
    /// A persist client used to write to storage collections
    persist_client: PersistClient,
    /// Set to `true` once `initialization_complete` has been called.
    initialized: bool,
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
    pub(super) async fn new(postgres_url: String) -> Self {
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
            persist_handles: BTreeMap::default(),
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
        self.initialized = true;
        for client in self.hosts.clients() {
            client.send(StorageCommand::InitializationComplete);
        }
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
            let metadata = CollectionMetadata {
                persist_location: self.persist_location.clone(),
                data_shard: ShardId::new(),
                remap_shard: ShardId::new(),
            };
            let metadata = METADATA_COLLECTION
                .insert_without_overwrite(&mut self.state.stash, &id, metadata)
                .await?;

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

            self.state
                .persist_handles
                .insert(id, PersistHandles { read, write });

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
                    .provision(id, description.remote_addr.clone())
                    .await?;

                client.send(StorageCommand::IngestSources(vec![augmented_ingestion]));

                if self.initialized {
                    client.send(StorageCommand::InitializationComplete);
                }
            }
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

    #[tracing::instrument(level = "debug", skip_all)]
    async fn append(
        &mut self,
        commands: Vec<(GlobalId, Vec<Update<Self::Timestamp>>, Self::Timestamp)>,
    ) -> Result<(), StorageError> {
        // TODO(petrosagg): validate appends against the expected RelationDesc of the collection
        let mut updates_by_id = HashMap::new();

        for (id, updates, batch_upper) in commands {
            for update in &updates {
                if !update.timestamp.less_than(&batch_upper) {
                    return Err(StorageError::UpdateBeyondUpper(id));
                }
            }

            let (total_updates, new_upper) = updates_by_id
                .entry(id)
                .or_insert_with(|| (Vec::new(), T::minimum()));
            total_updates.push(updates);
            new_upper.join_assign(&batch_upper);
        }

        let mut appends_by_id = HashMap::new();
        for (id, (updates, upper)) in updates_by_id {
            let current_upper = self.state.persist_handles[&id].write.upper().clone();
            appends_by_id.insert(id, (updates.into_iter().flatten(), current_upper, upper));
        }

        let futs = FuturesUnordered::new();

        // We cannot iterate through the updates and then set off a persist call
        // on the write handle because we cannot mutably borrow the write handle
        // multiple times.
        //
        // Instead, we first group the update by ID above and then iterate
        // through all available write handles and see if there are any updates
        // for it. If yes, we send them all in one go.
        for (id, persist_handle) in self.state.persist_handles.iter_mut() {
            let (updates, upper, new_upper) = match appends_by_id.remove(id) {
                Some(updates) => updates,
                None => continue,
            };

            let new_upper = Antichain::from_elem(new_upper);

            let updates = updates
                .into_iter()
                .map(|u| ((SourceData(Ok(u.row)), ()), u.timestamp, u.diff));

            let write = &mut persist_handle.write;

            futs.push(async move {
                write
                    .compare_and_append(updates, upper.clone(), new_upper.clone())
                    .await
                    .expect("cannot append updates")
                    .expect("cannot append updates")
                    .or(Err(StorageError::InvalidUpper(*id)))?;

                let mut change_batch = ChangeBatch::new();
                change_batch.extend(new_upper.iter().cloned().map(|t| (t, 1)));
                change_batch.extend(upper.iter().cloned().map(|t| (t, -1)));

                Ok::<_, StorageError>((*id, change_batch))
            })
        }

        let change_batches = futs.try_collect::<Vec<_>>().await?;

        self.update_write_frontiers(&change_batches).await?;

        Ok(())
    }

    async fn snapshot(
        &mut self,
        id: GlobalId,
        as_of: Self::Timestamp,
    ) -> Result<Vec<(Row, Diff)>, StorageError> {
        let as_of = Antichain::from_elem(as_of);
        let mut snapshot = self.state.persist_handles[&id]
            .read
            .snapshot(as_of)
            .await
            .map_err(|_| StorageError::ReadBeforeSince(id))?;

        let mut contents = Vec::new();

        while let Some(updates) = snapshot.next().await {
            for ((source_data, _pid), _ts, diff) in updates {
                let row = source_data.expect("cannot read snapshot").0?;
                contents.push((row, diff));
            }
        }

        Ok(contents)
    }

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
        // downgrade persist sinces.

        let futs = FuturesUnordered::new();

        // We cannot iterate through the changes and then set off a persist call
        // on the read handle because we cannot mutably borrow the read handle
        // multiple times.
        //
        // Instead, we iterate through all available read handles and see if
        // there are any changes for it. If yes, we downgrade.
        for (id, persist_handle) in self.state.persist_handles.iter_mut() {
            let (mut changes, frontier) = match storage_net.remove(id) {
                Some(changes) => changes,
                None => continue,
            };
            if changes.is_empty() {
                continue;
            }

            let fut = async move {
                persist_handle.read.downgrade_since(frontier.clone()).await;
                (*id, frontier)
            };

            futs.push(fut);
        }

        let compaction_commands = futs.collect::<Vec<_>>().await;

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
        if clients.is_empty() {
            // If there are no clients, block forever. This signals that there
            // may be more work to do (e.g., if this future is dropped and
            // `create_collections` is called). Awaiting the stream map would
            // return `None`, which would incorrectly indicate the completion
            // of the stream.
            return future::pending().await;
        }
        self.state.stashed_response = clients.next().await.map(|(_id, res)| res)
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

        Self {
            state: StorageControllerState::new(postgres_url).await,
            hosts: StorageHosts::new(StorageHostsConfig {
                build_info,
                orchestrator,
                storaged_image,
            }),
            persist_location,
            persist_client,
            initialized: false,
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

#[derive(Debug)]
pub(super) struct PersistHandles<T: Timestamp + Lattice + Codec64> {
    /// A `ReadHandle` for the backing persist shard/collection. This internally holds back the
    /// since frontier and we need to downgrade that when the read capabilities change.
    read: ReadHandle<SourceData, (), T, Diff>,
    write: WriteHandle<SourceData, (), T, Diff>,
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
