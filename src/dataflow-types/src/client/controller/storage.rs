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

use differential_dataflow::lattice::Lattice;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, ChangeBatch, Timestamp};

use crate::client::controller::ReadPolicy;
use crate::client::{Client, Command, CreateSourceCommand, StorageCommand};
use crate::sources::SourceDesc;
use crate::Update;
use mz_expr::GlobalId;

/// Controller state maintained for each storage instance.
pub struct StorageControllerState<T> {
    /// Collections maintained by the storage controller.
    ///
    /// This collection only grows, although individual collections may be rendered unusable.
    /// This is to prevent the re-binding of identifiers to other descriptions.
    pub(super) collections: BTreeMap<GlobalId, CollectionState<T>>,
}

/// An immutable controller for a storage instance.
pub struct StorageController<'a, T> {
    pub(super) storage: &'a StorageControllerState<T>,
}

/// A mutable controller for a storage instance.
pub struct StorageControllerMut<'a, C, T> {
    pub(super) storage: &'a mut StorageControllerState<T>,
    pub(super) client: &'a mut C,
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
    pub(super) fn new() -> Self {
        Self {
            collections: BTreeMap::default(),
        }
    }
}

// Public interface
impl<'a, T: Timestamp + Lattice> StorageController<'a, T> {
    /// Acquire an immutable reference to the collection state, should it exist.
    pub fn collection(&self, id: GlobalId) -> Result<&'a CollectionState<T>, StorageError> {
        self.storage
            .collections
            .get(&id)
            .ok_or(StorageError::IdentifierMissing(id))
    }
}

impl<'a, C: Client<T>, T: Timestamp + Lattice> StorageControllerMut<'a, C, T> {
    /// Constructs an immutable handle from this mutable handle.
    pub fn as_ref<'b>(&'b self) -> StorageController<'b, T> {
        StorageController {
            storage: &self.storage,
        }
    }

    /// Create the sources described in the individual CreateSourceCommand commands.
    ///
    /// Each command carries the source id, the  source description, an initial `since` read
    /// validity frontier, and initial timestamp bindings.
    ///
    /// This command installs collection state for the indicated sources, and the are
    /// now valid to use in queries at times beyond the initial `since` frontiers. Each
    /// collection also acquires a read capability at this frontier, which will need to
    /// be repeatedly downgraded with `allow_compaction()` to permit compaction.
    pub async fn create_sources(
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
            if let Ok(collection) = self.as_ref().collection(binding.id) {
                let (ref desc, ref since) = collection.description;
                if (desc, since) != (&binding.desc, &binding.since) {
                    Err(StorageError::SourceIdReused(binding.id))?
                }
            }
        }
        // Install collection state for each bound source.
        for binding in bindings.iter() {
            let collection = CollectionState::new(binding.desc.clone(), binding.since.clone());
            self.storage.collections.insert(binding.id, collection);
        }

        self.client
            .send(Command::Storage(StorageCommand::CreateSources(bindings)))
            .await
            .expect("Storage command failed; unrecoverable");

        Ok(())
    }
    /// Drops the read capability for the sources and allows their resources to be reclaimed.
    pub async fn drop_sources(&mut self, identifiers: Vec<GlobalId>) -> Result<(), StorageError> {
        self.as_ref().validate_ids(identifiers.iter().cloned())?;
        let compaction_commands = identifiers
            .into_iter()
            .map(|id| (id, Antichain::new()))
            .collect();
        self.allow_compaction(compaction_commands).await
    }
    pub async fn table_insert(
        &mut self,
        id: GlobalId,
        updates: Vec<Update<T>>,
    ) -> Result<(), StorageError> {
        self.client
            .send(Command::Storage(StorageCommand::Insert { id, updates }))
            .await
            .map_err(StorageError::from)
    }
    pub async fn update_durability_frontiers(
        &mut self,
        updates: Vec<(GlobalId, Antichain<T>)>,
    ) -> Result<(), StorageError> {
        self.client
            .send(Command::Storage(StorageCommand::DurabilityFrontierUpdates(
                updates,
            )))
            .await
            .map_err(StorageError::from)
    }
    /// Downgrade the read capabilities of specific identifiers to specific frontiers.
    ///
    /// Downgrading any read capability to the empty frontier will drop the item and eventually reclaim its resources.
    pub async fn allow_compaction(
        &mut self,
        frontiers: Vec<(GlobalId, Antichain<T>)>,
    ) -> Result<(), StorageError> {
        // Validate that the ids exist.
        self.as_ref()
            .validate_ids(frontiers.iter().map(|(id, _)| *id))?;

        let policies = frontiers
            .into_iter()
            .map(|(id, frontier)| (id, ReadPolicy::ValidFrom(frontier)));
        self.set_read_policy(policies.collect()).await;
        Ok(())
    }

    pub async fn advance_all_table_timestamps(
        &mut self,
        advance_to: T,
    ) -> Result<(), StorageError> {
        self.client
            .send(Command::Storage(StorageCommand::AdvanceAllLocalInputs {
                advance_to,
            }))
            .await
            .map_err(StorageError::from)
    }

    /// Assigns a read policy to specific identifiers.
    ///
    /// The policies are assigned in the order presented, and repeated identifiers should
    /// conclude with the last policy. Changing a policy will immediately downgrade the read
    /// capability if appropriate, but it will not "recover" the read capability if the prior
    /// capability is already ahead of it.
    ///
    /// Identifiers not present in `policies` retain their existing read policies.
    pub async fn set_read_policy(&mut self, policies: Vec<(GlobalId, ReadPolicy<T>)>) {
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
}

// Internal interface
impl<'a, T: Timestamp + Lattice> StorageController<'a, T> {
    /// Validate that a collection exists for all identifiers, and error if any do not.
    pub(super) fn validate_ids(
        &self,
        ids: impl Iterator<Item = GlobalId>,
    ) -> Result<(), StorageError> {
        for id in ids {
            self.collection(id)?;
        }
        Ok(())
    }
}

impl<'a, C: Client<T>, T: Timestamp + Lattice> StorageControllerMut<'a, C, T> {
    /// Acquire a mutable reference to the collection state, should it exist.
    pub(super) fn collection_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut CollectionState<T>, StorageError> {
        self.storage
            .collections
            .get_mut(&id)
            .ok_or(StorageError::IdentifierMissing(id))
    }

    /// Accept write frontier updates from the compute layer.
    pub(super) async fn update_write_frontiers(&mut self, updates: &[(GlobalId, ChangeBatch<T>)]) {
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

    /// Applies `updates` and sends any appropriate compaction command.
    pub(super) async fn update_read_capabilities(
        &mut self,
        updates: &mut BTreeMap<GlobalId, ChangeBatch<T>>,
    ) {
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
                    .as_ref()
                    .collection(*id)
                    .unwrap()
                    .read_capabilities
                    .frontier()
                    .to_owned();
                compaction_commands.push((*id, frontier));
            }
        }
        if !compaction_commands.is_empty() {
            self.client
                .send(Command::Storage(StorageCommand::AllowCompaction(
                    compaction_commands,
                )))
                .await
                .expect(
                    "Failed to send storage command; aborting as compute instance state corrupted",
                );
        }
    }
}

/// State maintained about individual collections.
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
