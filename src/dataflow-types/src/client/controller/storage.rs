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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use differential_dataflow::lattice::Lattice;
use serde::{Deserialize, Serialize};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use uuid::Uuid;

use mz_expr::PartitionId;
use mz_persist_client::{read::ReadHandle, PersistLocation};
use mz_persist_types::Codec64;
use mz_repr::Diff;
use mz_repr::GlobalId;
use mz_repr::Row;
use mz_stash::{self, Stash, StashError};

use crate::client::controller::ReadPolicy;
use crate::client::{
    CreateSourceCommand, MzOffset, StorageClient, StorageCommand, StorageResponse,
    TimestampBindingFeedback,
};
use crate::sources::SourceDesc;
use crate::Update;

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
        mut bindings: Vec<(GlobalId, (SourceDesc, Antichain<Self::Timestamp>))>,
    ) -> Result<(), StorageError>;

    /// Drops the read capability for the sources and allows their resources to be reclaimed.
    async fn drop_sources(&mut self, identifiers: Vec<GlobalId>) -> Result<(), StorageError>;

    /// Append `updates` into the local input named `id` and advance its upper to `upper`.
    // TODO(petrosagg): switch upper to `Antichain<Timestamp>`
    async fn append(
        &mut self,
        commands: Vec<(GlobalId, Vec<Update<Self::Timestamp>>, Self::Timestamp)>,
    ) -> Result<(), StorageError>;

    async fn update_durability_frontiers(
        &mut self,
        updates: Vec<(GlobalId, Antichain<Self::Timestamp>)>,
    ) -> Result<(), StorageError>;

    /// Persist timestamp bindings updates received from ingestion workers
    async fn persist_timestamp_bindings(
        &mut self,
        feedback: &TimestampBindingFeedback<Self::Timestamp>,
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
}

/// Controller state maintained for each storage instance.
#[derive(Debug)]
pub struct StorageControllerState<T, S = mz_stash::Sqlite> {
    pub(super) client: Box<dyn StorageClient<T>>,
    /// Collections maintained by the storage controller.
    ///
    /// This collection only grows, although individual collections may be rendered unusable.
    /// This is to prevent the re-binding of identifiers to other descriptions.
    pub(super) collections: BTreeMap<GlobalId, CollectionState<T>>,
    pub(super) stash: S,
}

/// A storage controller for a storage instance.
#[derive(Debug)]
pub struct Controller<T> {
    state: StorageControllerState<T>,
    /// The persist location where all storage collections are being written to
    persist_location: PersistLocation,
}

#[derive(Debug)]
pub enum StorageError {
    /// The source identifier was re-created after having been dropped,
    /// or installed with a different description.
    SourceIdReused(GlobalId),
    /// The source identifier is not present.
    IdentifierMissing(GlobalId),
    /// The update contained in the appended batch was at a timestamp beyond the batche's upper
    UpdateBeyondUpper(GlobalId),
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
                write!(f, "append batch for {id} contained update beyond its upper")
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

impl<T> StorageControllerState<T> {
    pub(super) fn new(client: Box<dyn StorageClient<T>>, state_dir: PathBuf) -> Self {
        let stash = mz_stash::Sqlite::open(&state_dir.join("storage"))
            .expect("unable to create storage stash");
        Self {
            client,
            collections: BTreeMap::default(),
            stash,
        }
    }
}

#[async_trait]
impl<T> StorageController for Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + TryInto<i64> + TryFrom<i64> + Codec64,
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

    fn collection_metadata(&self, _id: GlobalId) -> Result<CollectionMetadata, StorageError> {
        Ok(CollectionMetadata {
            persist_location: self.persist_location.clone(),
        })
    }

    async fn create_sources(
        &mut self,
        mut bindings: Vec<(GlobalId, (SourceDesc, Antichain<T>))>,
    ) -> Result<(), StorageError> {
        // Validate first, to avoid corrupting state.
        // 1. create a dropped source identifier, or
        // 2. create an existing source identifier with a new description.
        // Make sure to check for errors within `bindings` as well.
        bindings.sort_by_key(|(id, _)| *id);
        bindings.dedup();
        for pos in 1..bindings.len() {
            if bindings[pos - 1].0 == bindings[pos].0 {
                return Err(StorageError::SourceIdReused(bindings[pos].0));
            }
        }
        for (id, description_since) in bindings.iter() {
            if let Ok(collection) = self.collection(*id) {
                if &collection.description != description_since {
                    return Err(StorageError::SourceIdReused(*id));
                }
            }
        }

        let mut dataflow_commands = vec![];

        // Install collection state for each bound source.
        for (id, (desc, since)) in bindings {
            let read_handle = desc.connector.get_read_handle::<T>().await.map_err(|e| {
                StorageError::ClientError(anyhow!("external persist error: {:?}", e))
            })?;

            // If we got a persist read handle the since we get from coord is meaningless. It's
            // most likely `T::minimum()`, which is just the coordinator telling us that it _hopes_
            // that this is the since. We can go to the source (ha!) of the information.
            let since = read_handle
                .as_ref()
                .map(|read| read.since().clone())
                .unwrap_or(since);

            // TODO(aljoscha): We don't need a timestamp bindings collection for all types of
            // sources but right now we always create one. Perhaps the responsibility for creating
            // this should be moved to the `SourceConnector`, similar to how we have
            // `get_read_handle()` for the persist read handle.
            let ts_binding_collection = self
                .state
                .stash
                .collection::<PartitionId, ()>(&format!("timestamp-bindings-{id}"))
                .await?;

            let mut ts_bindings = Vec::new();
            let mut last_bindings: HashMap<_, MzOffset> = HashMap::new();
            for ((pid, _), time, diff) in self.state.stash.iter(ts_binding_collection).await? {
                let prev_offset = last_bindings.entry(pid.clone()).or_default();
                ts_bindings.push((
                    pid,
                    T::try_from(time).expect("timestamp overflowed i64"),
                    MzOffset {
                        offset: prev_offset.offset + diff,
                    },
                ));
                prev_offset.offset += diff;
            }

            let read_handle = read_handle.map(|read| {
                Box::new(ReadHandleWrapper { read_handle: read })
                    as Box<dyn CollectionReadHandle<T>>
            });

            let collection_state =
                CollectionState::new(desc.clone(), since.clone(), read_handle, last_bindings);
            self.state.collections.insert(id, collection_state);

            let storage_metadata = self.collection_metadata(id)?;

            let command = CreateSourceCommand {
                id,
                desc,
                since,
                ts_bindings,
                storage_metadata,
            };

            dataflow_commands.push(command);
        }

        self.state
            .client
            .send(StorageCommand::CreateSources(dataflow_commands))
            .await
            .expect("Storage command failed; unrecoverable");

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
        for (id, updates, upper) in commands.iter() {
            for update in updates {
                if !update.timestamp.less_than(upper) {
                    return Err(StorageError::UpdateBeyondUpper(*id));
                }
            }
        }
        self.state
            .client
            .send(StorageCommand::Append(commands))
            .await
            .map_err(StorageError::from)
    }

    async fn update_durability_frontiers(
        &mut self,
        updates: Vec<(GlobalId, Antichain<T>)>,
    ) -> Result<(), StorageError> {
        self.state
            .client
            .send(StorageCommand::DurabilityFrontierUpdates(updates))
            .await
            .map_err(StorageError::from)
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

    /// Persist timestamp bindings updates received from ingestion workers
    async fn persist_timestamp_bindings(
        &mut self,
        feedback: &TimestampBindingFeedback<T>,
    ) -> Result<(), StorageError> {
        for (id, bindings) in &feedback.bindings {
            let ts_binding_collection = self
                .state
                .stash
                .collection::<PartitionId, ()>(&format!("timestamp-bindings-{id}"))
                .await?;

            let upper = self.state.stash.upper(ts_binding_collection).await?;

            let collection_state = self.collection_mut(*id).expect("missing source id");

            // Here we differentialize the bindings we got from workers
            // Timestamp bindings as represented as a TVC whose data, time, and diff types
            // correspond to PartitionId, Time, MzOffset.
            //
            // For example, suppose we read from a kafka topic with two partitions and at timestamp
            // 1 we've read up to offsets 5 and 10 for each partition and at timestamp 2 we read up
            // to offsets 7 and 17. The differentialized collection will contain the following
            // updates:
            //
            // (PartitionId::Kafka(1), 1, +5)
            // (PartitionId::Kafka(2), 1, +10)
            // (PartitionId::Kafka(1), 2, +2)  <-- This is how much the offset changed, 7 - 5 = 2
            // (PartitionId::Kafka(2), 2, +7)
            //
            // This representation allows us to compact timestamp bindings simply by adding
            // together offsets and collapsing their timestamps. For example, if we were to compact
            // through timestamp 2 the collection would contain the following updates:
            //
            // (PartitionId::Kafka(1), 2, +7)  <-- 5 + 2 = 7
            // (PartitionId::Kafka(2), 2, +17) <-- 10 + 7 = 17
            let mut bindings = bindings.clone();
            // Sort the bindings by (pid, ts, offset)
            bindings.sort_unstable();
            let mut updates = vec![];
            for (pid, ts, offset) in bindings {
                let prev_offset = collection_state
                    .last_reported_ts_bindings
                    .entry(pid.clone())
                    .or_default();

                let ts = ts.try_into().expect("timestamp overflowed i64");
                let update = ((pid, ()), ts, offset.offset - prev_offset.offset);

                prev_offset.offset = offset.offset;
                // TODO(petrosagg): refactor timestamp binding handling so that we never enter a
                // situation where the previous bindings are re-reported by workers
                if upper.less_equal(&ts) {
                    updates.push(update);
                }
            }
            self.state
                .stash
                .update_many(ts_binding_collection, updates)
                .await?;
        }

        let mut durability_updates = vec![];
        let mut seals = vec![];
        for (id, _changes) in &feedback.changes {
            let ts_binding_collection = self
                .state
                .stash
                .collection::<PartitionId, ()>(&format!("timestamp-bindings-{id}"))
                .await?;
            let collection = self.collection_mut(*id).expect("missing source id");
            let write_frontier = collection.write_frontier.frontier().to_owned();
            let seal_frontier = Antichain::from_iter(
                write_frontier
                    .as_option()
                    .map(|ts| ts.clone().try_into().expect("negative timestamp")),
            );
            let upper = self.state.stash.upper(ts_binding_collection).await?;
            // TODO(petrosagg): This guard should go away by ensuring storage workers never re-send
            // the bindings and frontiers they were initialized with
            if PartialOrder::less_than(&upper, &seal_frontier) {
                seals.push((ts_binding_collection, seal_frontier));
            }
            durability_updates.push((*id, write_frontier));
        }

        // Note: this seal is not performed in a transaction with the above `update_many`.
        // This is fine/correct, but subtle. If we crash in between these 2 writes, the
        // updates will be read in their entirety, and future updates past the old upper will
        // still be accepted. Later seals will never erroneously seal times for ts bindings
        // that were recorded, because we always record bindings before seal-ing here.
        self.state.stash.seal_batch(&seals).await?;

        self.update_durability_frontiers(durability_updates).await?;

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
        let mut storage_net = Vec::default();
        // Repeatedly extract the maximum id, and updates for it.
        while let Some(key) = updates.keys().rev().next().cloned() {
            let mut update = updates.remove(&key).unwrap();
            if let Ok(collection) = self.collection_mut(key) {
                let changes = collection.read_capabilities.update_iter(update.drain());
                update.extend(changes);
                storage_net.push((key, update));
            } else {
                // This is confusing and we should probably error.
                panic!("Unknown collection identifier {}", key);
            }
        }

        // Translate our net compute actions into `AllowCompaction` commands.
        let mut compaction_commands = Vec::new();
        let mut stash_compactions = vec![];
        let mut stash_consolidations = vec![];
        for (id, change) in storage_net.iter_mut() {
            if !change.is_empty() {
                let frontier = self
                    .collection(*id)
                    .unwrap()
                    .read_capabilities
                    .frontier()
                    .to_owned();

                let ts_binding_collection = self
                    .state
                    .stash
                    .collection::<PartitionId, ()>(&format!("timestamp-bindings-{id}"))
                    .await?;

                let mut since = self.state.stash.since(ts_binding_collection).await?;
                since.extend(
                    frontier
                        .iter()
                        .map(|t| t.clone().try_into().expect("timestamp overflowed i64")),
                );
                stash_compactions.push((ts_binding_collection, since));
                stash_consolidations.push(ts_binding_collection);
                compaction_commands.push((*id, frontier.clone()));

                let collection = self.collection_mut(*id).unwrap();
                if let Some(read) = collection.read_handle.as_mut() {
                    read.downgrade_since(frontier).await?;
                }
            }
        }
        self.state.stash.compact_batch(&stash_compactions).await?;
        self.state
            .stash
            .consolidate_batch(&stash_consolidations)
            .await?;

        if !compaction_commands.is_empty() {
            self.state
                .client
                .send(StorageCommand::AllowCompaction(compaction_commands))
                .await
                .expect(
                    "Failed to send storage command; aborting as compute instance state corrupted",
                );
        }
        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<StorageResponse<Self::Timestamp>>, anyhow::Error> {
        self.state.client.recv().await
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
    T: Timestamp + Lattice + TotalOrder + TryInto<i64> + TryFrom<i64> + Codec64,
    <T as TryInto<i64>>::Error: std::fmt::Debug,
    <T as TryFrom<i64>>::Error: std::fmt::Debug,
{
    /// Create a new storage controller from a client it should wrap.
    pub fn new(
        client: Box<dyn StorageClient<T>>,
        state_dir: PathBuf,
        persist_location: PersistLocation,
    ) -> Self {
        Self {
            state: StorageControllerState::new(client, state_dir),
            persist_location,
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

    /// The last reported timestamp bindings, if any.
    /// This is used to differentialize timestamp bindings received before storing them in stash
    pub(super) last_reported_ts_bindings: HashMap<PartitionId, MzOffset>,

    /// A `ReadHandle` for the backing persist shard/collection. This internally holds back the
    /// since frontier and we need to downgrade that when the read capabilities change.
    // TODO(aljoscha): Once all sources are wired up to go through persist/STORAGE, this will stop
    // being optional
    pub read_handle: Option<Box<dyn CollectionReadHandle<T>>>,
}

impl<T: Timestamp> CollectionState<T> {
    /// Creates a new collection state, with an initial read policy valid from `since`.
    pub fn new(
        description: SourceDesc,
        since: Antichain<T>,
        read_handle: Option<Box<dyn CollectionReadHandle<T>>>,
        last_reported_ts_bindings: HashMap<PartitionId, MzOffset>,
    ) -> Self {
        let mut read_capabilities = MutableAntichain::new();
        read_capabilities.update_iter(since.iter().map(|time| (time.clone(), 1)));
        Self {
            description: (description, since.clone()),
            read_capabilities,
            implied_capability: since.clone(),
            read_policy: ReadPolicy::ValidFrom(since),
            write_frontier: MutableAntichain::new_bottom(Timestamp::minimum()),
            last_reported_ts_bindings,
            read_handle,
        }
    }
}

/// A type that hides the details of a real persist read handle and only exposes the methods that
/// the controller needs to know about.
#[async_trait]
pub trait CollectionReadHandle<T>: Send + Sync + Debug {
    /// Forwards the since frontier of this handle, giving up the ability to
    /// read at times not greater or equal to `new_since`.
    async fn downgrade_since(&mut self, since: Antichain<T>) -> Result<(), StorageError>;
}

#[derive(Debug)]
struct ReadHandleWrapper<T: Timestamp + Lattice + Codec64> {
    read_handle: ReadHandle<Row, Row, T, Diff>,
}

#[async_trait]
impl<T: Timestamp + Lattice + Codec64> CollectionReadHandle<T> for ReadHandleWrapper<T> {
    async fn downgrade_since(&mut self, since: Antichain<T>) -> Result<(), StorageError> {
        self.read_handle
            .downgrade_since(Duration::from_secs(60), since)
            .await
            .map_err(|e| StorageError::ClientError(anyhow!("external persist error: {:?}", e)))?
            .expect("invalid usage");

        Ok(())
    }
}
