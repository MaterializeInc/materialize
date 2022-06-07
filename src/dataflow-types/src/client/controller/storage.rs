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

use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use differential_dataflow::lattice::Lattice;
use futures::future;
use futures::stream::TryStreamExt as _;
use futures::stream::{FuturesUnordered, StreamExt};
use proptest::prelude::{Arbitrary, BoxedStrategy, Just};
use proptest::strategy::Strategy;
use serde::{Deserialize, Serialize};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use tokio_stream::StreamMap;
use uuid::Uuid;

use mz_orchestrator::{NamespacedOrchestrator, ServiceConfig, ServicePort};
use mz_ore::collections::CollectionExt;
use mz_persist_client::{
    read::ReadHandle, write::WriteHandle, PersistClient, PersistLocation, ShardId,
};
use mz_persist_types::Codec64;
use mz_repr::proto::{RustType, TryFromProtoError};
use mz_repr::{Diff, GlobalId};
use mz_stash::{self, StashError, TypedCollection};

use crate::client::controller::ReadPolicy;
use crate::client::{
    GenericClient, StorageClient, StorageCommand, StorageResponse, StoragedRemoteClient,
};
use crate::sources::{IngestionDescription, SourceConnector, SourceData, SourceDesc};
use crate::Update;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_dataflow_types.client.controller.storage.rs"
));

#[async_trait]
pub trait StorageController: Debug + Send {
    type Timestamp;

    /// Acquire an immutable reference to the collection state, should it exist.
    fn collection(&self, id: GlobalId) -> Result<&CollectionState<Self::Timestamp>, StorageError>;

    /// Acquire a mutable reference to the collection state, should it exist.
    fn collection_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut CollectionState<Self::Timestamp>, StorageError>;

    /// Returns the necessary metadata to read a collection
    fn collection_metadata(&self, id: GlobalId) -> Result<CollectionMetadata, StorageError>;

    /// Create the sources described in the individual CreateSourceCommand commands.
    ///
    /// Each command carries the source id, the  source description, an initial `since` read
    /// validity frontier, and initial timestamp bindings.
    ///
    /// This command installs collection state for the indicated sources, and the are
    /// now valid to use in queries at times beyond the initial `since` frontiers. Each
    /// collection also acquires a read capability at this frontier, which will need to
    /// be repeatedly downgraded with `allow_compaction()` to permit compaction.
    async fn create_sources(
        &mut self,
        ingestions: Vec<IngestionDescription<(), Self::Timestamp>>,
    ) -> Result<(), StorageError>;

    /// Drops the read capability for the sources and allows their resources to be reclaimed.
    async fn drop_sources(&mut self, identifiers: Vec<GlobalId>) -> Result<(), StorageError>;

    /// Append `updates` into the local input named `id` and advance its upper to `upper`.
    // TODO(petrosagg): switch upper to `Antichain<Timestamp>`
    async fn append(
        &mut self,
        commands: Vec<(GlobalId, Vec<Update<Self::Timestamp>>, Self::Timestamp)>,
    ) -> Result<(), StorageError>;

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

    /// Send a request to obtain "linearized" timestamps for the given sources.
    async fn linearize_sources(
        &mut self,
        peek_id: Uuid,
        source_ids: Vec<GlobalId>,
    ) -> Result<(), anyhow::Error>;

    async fn recv(&mut self) -> Result<Option<StorageResponse<Self::Timestamp>>, anyhow::Error>;
}

/// Metadata required by a storage instance to read a storage collection
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionMetadata {
    pub persist_location: PersistLocation,
    pub timestamp_shard_id: ShardId,
    pub persist_shard: ShardId,
}

impl RustType<ProtoCollectionMetadata> for CollectionMetadata {
    fn into_proto(&self) -> ProtoCollectionMetadata {
        ProtoCollectionMetadata {
            blob_uri: self.persist_location.blob_uri.clone(),
            consensus_uri: self.persist_location.consensus_uri.clone(),
            shard_id: self.persist_shard.to_string(),
            timestamp_shard_id: self.timestamp_shard_id.to_string(),
        }
    }

    fn from_proto(value: ProtoCollectionMetadata) -> Result<Self, TryFromProtoError> {
        Ok(CollectionMetadata {
            persist_location: PersistLocation {
                blob_uri: value.blob_uri,
                consensus_uri: value.consensus_uri,
            },
            timestamp_shard_id: value
                .timestamp_shard_id
                .parse()
                .map_err(TryFromProtoError::InvalidShardId)?,
            persist_shard: value
                .shard_id
                .parse()
                .map_err(TryFromProtoError::InvalidShardId)?,
        })
    }
}

impl Arbitrary for CollectionMetadata {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        // TODO (#12359): derive Arbitrary after CollectionMetadata
        // gains proper protobuf support.
        let shard_id = format!("s{}", Uuid::from_bytes([0x00; 16]));
        Just(CollectionMetadata {
            persist_location: PersistLocation {
                blob_uri: "".to_string(),
                consensus_uri: "".to_string(),
            },
            timestamp_shard_id: ShardId::from_str(&shard_id).unwrap(),
            persist_shard: ShardId::new(),
        })
        .boxed()
    }
}

/// Controller state maintained for each storage instance.
#[derive(Debug)]
pub struct StorageControllerState<T: Timestamp + Lattice + Codec64, S = mz_stash::Sqlite> {
    pub(super) clients: BTreeMap<GlobalId, Box<dyn StorageClient<T>>>,
    /// Collections maintained by the storage controller.
    ///
    /// This collection only grows, although individual collections may be rendered unusable.
    /// This is to prevent the re-binding of identifiers to other descriptions.
    pub(super) collections: BTreeMap<GlobalId, CollectionState<T>>,
    pub(super) stash: S,
    pub(super) persist_handles: BTreeMap<GlobalId, PersistHandles<T>>,
}

/// A storage controller for a storage instance.
#[derive(Debug)]
pub struct Controller<T: Timestamp + Lattice + Codec64 + Unpin> {
    state: StorageControllerState<T>,
    /// The persist location where all storage collections are being written to
    persist_location: PersistLocation,
    /// A persist client used to write to storage collections
    persist_client: PersistClient,
    /// An orchestrator to start and stop storage processes.
    orchestrator: Arc<dyn NamespacedOrchestrator>,
    /// The storaged image to use when starting new storage processes.
    storaged_image: String,
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
    /// The expected upper of an append was different than the actual append of the collection
    InvalidUpper(GlobalId),
    /// An error from the underlying client.
    ClientError(anyhow::Error),
    /// An operation failed to read or write state
    IOError(StashError),
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SourceIdReused(_) => None,
            Self::IdentifierMissing(_) => None,
            Self::UpdateBeyondUpper(_) => None,
            Self::InvalidUpper(_) => None,
            Self::ClientError(_) => None,
            Self::IOError(err) => Some(err),
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
            Self::InvalidUpper(id) => {
                write!(
                    f,
                    "expected upper for {id} was different than its actual upper"
                )
            }
            Self::ClientError(err) => write!(f, "underlying client error: {err}"),
            Self::IOError(err) => write!(f, "failed to read or write state: {err}"),
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

impl<T: Timestamp + Lattice + Codec64> StorageControllerState<T> {
    pub(super) fn new(state_dir: PathBuf) -> Self {
        let stash = mz_stash::Sqlite::open(Some(&state_dir.join("storage")))
            .expect("unable to create storage stash");
        Self {
            clients: BTreeMap::default(),
            collections: BTreeMap::default(),
            stash,
            persist_handles: BTreeMap::default(),
        }
    }
}

#[async_trait]
impl<T> StorageController for Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + TryInto<i64> + TryFrom<i64> + Codec64 + Unpin,
    <T as TryInto<i64>>::Error: std::fmt::Debug,
    <T as TryFrom<i64>>::Error: std::fmt::Debug,
{
    type Timestamp = T;

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

    fn collection_metadata(&self, id: GlobalId) -> Result<CollectionMetadata, StorageError> {
        let collection = self.collection(id)?;
        Ok(CollectionMetadata {
            persist_location: self.persist_location.clone(),
            timestamp_shard_id: collection.timestamp_shard_id,
            persist_shard: collection.persist_shard,
        })
    }

    async fn create_sources(
        &mut self,
        mut ingestions: Vec<IngestionDescription<(), T>>,
    ) -> Result<(), StorageError> {
        // Validate first, to avoid corrupting state.
        // 1. create a dropped source identifier, or
        // 2. create an existing source identifier with a new description.
        // Make sure to check for errors within `ingestions` as well.
        ingestions.sort_by_key(|ingestion| ingestion.id);
        ingestions.dedup();
        for pos in 1..ingestions.len() {
            if ingestions[pos - 1].id == ingestions[pos].id {
                return Err(StorageError::SourceIdReused(ingestions[pos].id));
            }
        }
        for ingestion in ingestions.iter() {
            if let Ok(collection) = self.collection(ingestion.id) {
                let (desc, since) = &collection.description;
                if (desc, since) != (&ingestion.desc, &ingestion.since) {
                    return Err(StorageError::SourceIdReused(ingestion.id));
                }
            }
        }

        let mut external_ingestions = vec![];

        // Install collection state for each bound source.
        for ingestion in ingestions {
            // TODO(petrosagg): durably record the persist shard we mint here
            let persist_shard = ShardId::new();
            let (write, read) = self
                .persist_client
                .open(persist_shard)
                .await
                .expect("invalid persist usage");
            self.state
                .persist_handles
                .insert(ingestion.id, PersistHandles { read, write });

            let timestamp_shard_id = TypedCollection::new("timestamp-shard-id")
                .insert_without_overwrite(&mut self.state.stash, &ingestion.id, ShardId::new())
                .await?;

            let collection_state = CollectionState::new(
                ingestion.desc.clone(),
                ingestion.since.clone(),
                persist_shard,
                timestamp_shard_id,
            );

            self.state
                .collections
                .insert(ingestion.id, collection_state);

            // TODO(petrosagg): it's weird that tables go through this path and we filter them
            // manually. Think how to make better types to reflect their differences
            if matches!(ingestion.desc.connector, SourceConnector::External { .. }) {
                external_ingestions.push(ingestion);
            }
        }

        // Here we create a new storaged process to handle each new source. Each
        // ingestion is augmented with the collection metadata.
        for ingestion in external_ingestions {
            let mut source_imports = BTreeMap::new();
            for (id, _) in ingestion.source_imports {
                let metadata = self.collection_metadata(id)?;
                source_imports.insert(id, metadata);
            }

            let augmented_ingestion = IngestionDescription {
                source_imports,
                // The rest of the fields are identical
                id: ingestion.id,
                desc: ingestion.desc,
                since: ingestion.since,
                storage_metadata: self.collection_metadata(ingestion.id)?,
            };

            let storage_service = self
                .orchestrator
                .ensure_service(
                    &ingestion.id.to_string(),
                    ServiceConfig {
                        image: self.storaged_image.clone(),
                        args: &|assigned| {
                            vec![
                                format!("--workers=1"),
                                format!(
                                    "--listen-addr={}:{}",
                                    assigned.listen_host, assigned.ports["controller"]
                                ),
                                format!(
                                    "--http-console-addr={}:{}",
                                    assigned.listen_host, assigned.ports["http"]
                                ),
                            ]
                        },
                        ports: vec![
                            ServicePort {
                                name: "controller".into(),
                                port_hint: 2100,
                            },
                            ServicePort {
                                name: "http".into(),
                                port_hint: 6875,
                            },
                        ],
                        // TODO: limits?
                        cpu_limit: None,
                        memory_limit: None,
                        scale: NonZeroUsize::new(1).unwrap(),
                        labels: HashMap::new(),
                        availability_zone: None,
                    },
                )
                .await?;

            // TODO: don't block waiting for a connection. Put a queue in the
            // middle instead.
            let mut client = Box::new({
                let addr = storage_service.addresses("controller").into_element();
                let mut client = StoragedRemoteClient::new(&[addr]);
                client.connect().await;
                client
            });

            client
                .send(StorageCommand::CreateSources(vec![augmented_ingestion]))
                .await
                .expect("Storage command failed; unrecoverable");

            self.state.clients.insert(ingestion.id, client);
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

    async fn append(
        &mut self,
        commands: Vec<(GlobalId, Vec<Update<Self::Timestamp>>, Self::Timestamp)>,
    ) -> Result<(), StorageError> {
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
            let current_upper = self.collection(id)?.write_frontier.frontier().to_owned();
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
            if let Some(client) = self.state.clients.get_mut(&id) {
                client
                    .send(StorageCommand::AllowCompaction(vec![(
                        id,
                        frontier.clone(),
                    )]))
                    .await?;

                if frontier.is_empty() {
                    self.state.clients.remove(&id);
                    self.orchestrator.drop_service(&id.to_string()).await?;
                }
            }
        }

        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<StorageResponse<Self::Timestamp>>, anyhow::Error> {
        if self.state.clients.is_empty() {
            // If there are no clients, block forever. This signals that there
            // may be more work to do (e.g., if this future is dropped and
            // `create_sources` is called). Awaiting the stream map would
            // return `None`, which would incorrectly indicate the completion
            // of the stream.
            return future::pending().await;
        }
        let mut clients = self
            .state
            .clients
            .iter_mut()
            .map(|(id, client)| (id, client.as_stream()))
            .collect::<StreamMap<_, _>>();
        clients.next().await.map(|(_id, res)| res).transpose()
    }

    /// "Linearize" the listed sources.
    ///
    /// If these sources are valid and "linearizable", then the response
    /// will respond with timestamps that are guaranteed to be up-to-date
    /// with the max offset found at the time of the command issuance.
    ///
    /// Note: "linearizable" in this context may not represent
    /// true linearizability in all cases.
    async fn linearize_sources(
        &mut self,
        _peek_id: Uuid,
        _source_ids: Vec<GlobalId>,
    ) -> Result<(), anyhow::Error> {
        // TODO(guswynn): implement this function
        Ok(())
    }
}

impl<T> Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + TryInto<i64> + TryFrom<i64> + Codec64 + Unpin,
    <T as TryInto<i64>>::Error: std::fmt::Debug,
    <T as TryFrom<i64>>::Error: std::fmt::Debug,
{
    /// Create a new storage controller from a client it should wrap.
    pub async fn new(
        state_dir: PathBuf,
        persist_location: PersistLocation,
        orchestrator: Arc<dyn NamespacedOrchestrator>,
        storaged_image: String,
    ) -> Self {
        let persist_client = persist_location.open().await.unwrap();

        Self {
            state: StorageControllerState::new(state_dir),
            persist_location,
            persist_client,
            orchestrator,
            storaged_image,
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
    /// Description with which the source was created, and its initial `since`.
    pub(super) description: (crate::sources::SourceDesc, Antichain<T>),

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

    // TODO: only makes sense for collections that are ingested so maybe should live elsewhere?
    /// The persist shard id of the remap collection used to reclock this collection
    pub timestamp_shard_id: ShardId,

    /// The persist shard containing the contents of this storage collection
    pub persist_shard: ShardId,
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
        description: SourceDesc,
        since: Antichain<T>,
        persist_shard: ShardId,
        timestamp_shard_id: ShardId,
    ) -> Self {
        let mut read_capabilities = MutableAntichain::new();
        read_capabilities.update_iter(since.iter().map(|time| (time.clone(), 1)));
        Self {
            description: (description, since.clone()),
            read_capabilities,
            implied_capability: since.clone(),
            read_policy: ReadPolicy::ValidFrom(since),
            write_frontier: MutableAntichain::new_bottom(Timestamp::minimum()),
            timestamp_shard_id,
            persist_shard,
        }
    }
}
