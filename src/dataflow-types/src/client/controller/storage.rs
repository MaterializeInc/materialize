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
//! a read capability for each source, which is manipulated with `allow_compaction()`. Eventually, the source
//! is dropped with either `drop_sources()` or by allowing compaction to the empty frontier.

use std::collections::BTreeMap;
use std::fmt::Debug;

use async_trait::async_trait;
use differential_dataflow::lattice::Lattice;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, ChangeBatch, Timestamp};

use crate::client::controller::ReadPolicy;
use crate::client::{CreateSourceCommand, StorageClient, StorageCommand, StorageResponse};
use crate::sources::SourceDesc;
use crate::Update;
use mz_expr::GlobalId;

#[async_trait]
pub trait StorageController: Debug + Send {
    type Timestamp: Timestamp;

    /// Acquire an immutable reference to the collection state, should it exist.
    fn collection(&self, id: GlobalId) -> Result<&CollectionState<Self::Timestamp>, StorageError>;

    /// Acquire a mutable reference to the collection state, should it exist.
    fn collection_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut CollectionState<Self::Timestamp>, StorageError>;

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
        mut bindings: Vec<CreateSourceCommand<Self::Timestamp>>,
    ) -> Result<(), StorageError>;

    /// Drops the read capability for the sources and allows their resources to be reclaimed.
    async fn drop_sources(&mut self, identifiers: Vec<GlobalId>) -> Result<(), StorageError>;

    async fn table_insert(
        &mut self,
        id: GlobalId,
        updates: Vec<Update<Self::Timestamp>>,
    ) -> Result<(), StorageError>;

    async fn update_durability_frontiers(
        &mut self,
        updates: Vec<(GlobalId, Antichain<Self::Timestamp>)>,
    ) -> Result<(), StorageError>;

    async fn advance_all_table_timestamps(
        &mut self,
        advance_to: Self::Timestamp,
    ) -> Result<(), StorageError>;

    /// Assigns a read policy to specific identifiers.
    ///
    /// The policies are assigned in the order presented, and repeated identifiers should
    /// conclude with the last policy. Changing a policy will immediately downgrade the read
    /// capability if appropriate, but it will not "recover" the read capability if the prior
    /// capability is already ahead of it.
    ///
    /// Identifiers not present in `policies` retain their existing read policies.
    async fn set_read_policy(&mut self, policies: Vec<(GlobalId, ReadPolicy<Self::Timestamp>)>);

    /// Accept write frontier updates from the compute layer.
    async fn update_write_frontiers(
        &mut self,
        updates: &[(GlobalId, ChangeBatch<Self::Timestamp>)],
    );

    /// Applies `updates` and sends any appropriate compaction command.
    async fn update_read_capabilities(
        &mut self,
        updates: &mut BTreeMap<GlobalId, ChangeBatch<Self::Timestamp>>,
    );

    async fn recv(&mut self) -> Result<Option<StorageResponse<Self::Timestamp>>, anyhow::Error>;
}

/// Controller state maintained for each storage instance.
#[derive(Debug)]
pub struct StorageControllerState<T> {
    pub(super) client: Box<dyn StorageClient<T>>,
    /// Collections maintained by the storage controller.
    ///
    /// This collection only grows, although individual collections may be rendered unusable.
    /// This is to prevent the re-binding of identifiers to other descriptions.
    pub(super) collections: BTreeMap<GlobalId, CollectionState<T>>,
}

/// A storage controller for a storage instance.
#[derive(Debug)]
pub struct Controller<T> {
    state: StorageControllerState<T>,
}

#[derive(Debug)]
pub enum StorageError {
    /// The source identifier was re-created after having been dropped,
    /// or installed with a different description.
    SourceIdReused(GlobalId),
    /// The source identifier is not present.
    IdentifierMissing(GlobalId),
    /// An error from the underlying client.
    ClientError(anyhow::Error),
}

impl From<anyhow::Error> for StorageError {
    fn from(error: anyhow::Error) -> Self {
        Self::ClientError(error)
    }
}

impl<T> StorageControllerState<T> {
    pub(super) fn new(client: Box<dyn StorageClient<T>>) -> Self {
        Self {
            client,
            collections: BTreeMap::default(),
        }
    }
}

#[async_trait]
impl<T: Timestamp + Lattice> StorageController for Controller<T> {
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

    async fn create_sources(
        &mut self,
        mut bindings: Vec<CreateSourceCommand<T>>,
    ) -> Result<(), StorageError> {
        // Validate first, to avoid corrupting state.
        // 1. create a dropped source identifier, or
        // 2. create an existing source identifier with a new description.
        // Make sure to check for errors within `bindings` as well.
        bindings.sort_by_key(|b| b.id);
        bindings.dedup();
        for pos in 1..bindings.len() {
            if bindings[pos - 1].id == bindings[pos].id {
                Err(StorageError::SourceIdReused(bindings[pos].id))?;
            }
        }
        for binding in bindings.iter() {
            if let Ok(collection) = self.collection(binding.id) {
                let (ref desc, ref since) = collection.description;
                if (desc, since) != (&binding.desc, &binding.since) {
                    Err(StorageError::SourceIdReused(binding.id))?
                }
            }
        }
        // Install collection state for each bound source.
        for binding in bindings.iter() {
            let collection = CollectionState::new(binding.desc.clone(), binding.since.clone());
            self.state.collections.insert(binding.id, collection);
        }

        self.state
            .client
            .send(StorageCommand::CreateSources(bindings))
            .await
            .expect("Storage command failed; unrecoverable");

        Ok(())
    }

    async fn drop_sources(&mut self, identifiers: Vec<GlobalId>) -> Result<(), StorageError> {
        self.validate_ids(identifiers.iter().cloned())?;
        let compaction_commands = identifiers
            .into_iter()
            .map(|id| (id, Antichain::new()))
            .collect();
        self.allow_compaction(compaction_commands).await
    }

    async fn table_insert(
        &mut self,
        id: GlobalId,
        updates: Vec<Update<T>>,
    ) -> Result<(), StorageError> {
        self.state
            .client
            .send(StorageCommand::Insert { id, updates })
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

    async fn advance_all_table_timestamps(&mut self, advance_to: T) -> Result<(), StorageError> {
        self.state
            .client
            .send(StorageCommand::AdvanceAllLocalInputs { advance_to })
            .await
            .map_err(StorageError::from)
    }

    async fn set_read_policy(&mut self, policies: Vec<(GlobalId, ReadPolicy<T>)>) {
        let mut read_capability_changes = BTreeMap::default();
        for (id, policy) in policies.into_iter() {
            if let Ok(collection) = self.collection_mut(id) {
                let mut new_read_capability = policy.frontier(collection.write_frontier.frontier());

                if <_ as timely::order::PartialOrder>::less_equal(
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
            } else {
                tracing::error!("Reference to unregistered id: {:?}", id);
            }
        }
        if !read_capability_changes.is_empty() {
            self.update_read_capabilities(&mut read_capability_changes)
                .await;
        }
    }

    async fn update_write_frontiers(&mut self, updates: &[(GlobalId, ChangeBatch<T>)]) {
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
            if <_ as timely::order::PartialOrder>::less_equal(
                &collection.implied_capability,
                &new_read_capability,
            ) {
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
                .await;
        }
    }

    async fn update_read_capabilities(&mut self, updates: &mut BTreeMap<GlobalId, ChangeBatch<T>>) {
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
        for (id, change) in storage_net.iter_mut() {
            if !change.is_empty() {
                let frontier = self
                    .collection(*id)
                    .unwrap()
                    .read_capabilities
                    .frontier()
                    .to_owned();
                compaction_commands.push((*id, frontier));
            }
        }
        if !compaction_commands.is_empty() {
            self.state
                .client
                .send(StorageCommand::AllowCompaction(compaction_commands))
                .await
                .expect(
                    "Failed to send storage command; aborting as compute instance state corrupted",
                );
        }
    }
    async fn recv(&mut self) -> Result<Option<StorageResponse<Self::Timestamp>>, anyhow::Error> {
        self.state.client.recv().await
    }
}

impl<T: Timestamp + Lattice> Controller<T> {
    /// Create a new storage controller from a client it should wrap.
    pub fn new(client: Box<dyn StorageClient<T>>) -> Self {
        Self {
            state: StorageControllerState::new(client),
        }
    }

    /// Validate that a collection exists for all identifiers, and error if any do not.
    fn validate_ids(&self, ids: impl Iterator<Item = GlobalId>) -> Result<(), StorageError> {
        for id in ids {
            self.collection(id)?;
        }
        Ok(())
    }

    async fn allow_compaction(
        &mut self,
        frontiers: Vec<(GlobalId, Antichain<T>)>,
    ) -> Result<(), StorageError> {
        // Validate that the ids exist.
        self.validate_ids(frontiers.iter().map(|(id, _)| *id))?;

        let policies = frontiers
            .into_iter()
            .map(|(id, frontier)| (id, ReadPolicy::ValidFrom(frontier)));
        self.set_read_policy(policies.collect()).await;
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
    /// equal to `upper_frontier.frontier()`.
    pub write_frontier: MutableAntichain<T>,
}

impl<T: Timestamp> CollectionState<T> {
    /// Creates a new collection state, with an initial read policy valid from `since`.
    pub fn new(description: SourceDesc, since: Antichain<T>) -> Self {
        let mut read_capabilities = MutableAntichain::new();
        read_capabilities.update_iter(since.iter().map(|time| (time.clone(), 1)));
        Self {
            description: (description, since.clone()),
            read_capabilities,
            implied_capability: since.clone(),
            read_policy: ReadPolicy::ValidFrom(since),
            write_frontier: MutableAntichain::new_bottom(Timestamp::minimum()),
        }
    }
}
