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

/// A controller for a storage instance.
pub struct StorageController<'a, C, T> {
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
impl<'a, C: Client<T>, T: Timestamp + Lattice> StorageController<'a, C, T> {
    pub fn collection(&self, id: GlobalId) -> Result<&CollectionState<T>, StorageError> {
        self.storage
            .collections
            .get(&id)
            .ok_or(StorageError::IdentifierMissing(id))
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
        self.validate_ids(identifiers.iter().cloned())?;
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
        self.validate_ids(frontiers.iter().map(|(id, _)| *id))?;

        let mut updates = BTreeMap::new();
        for (id, mut frontier) in frontiers.into_iter() {
            let collection = self.collection_mut(id).unwrap();
            // Ignore frontier updates that go backwards.
            if <_ as timely::order::PartialOrder>::less_equal(
                &collection.implied_capability,
                &frontier,
            ) {
                // Add new frontier, swap, subtract old frontier.
                let mut update = ChangeBatch::new();
                update.extend(frontier.iter().map(|time| (time.clone(), 1)));
                std::mem::swap(&mut collection.implied_capability, &mut frontier);
                update.extend(frontier.iter().map(|time| (time.clone(), -1)));
                // Record updates if something of substance changed.
                if !update.is_empty() {
                    updates.insert(id, update);
                }
            } else {
                tracing::error!("STORAGE::allow_compaction attempted frontier regression for id {:?}: {:?} to {:?}", id, collection.implied_capability, frontier);
            }
        }

        self.update_read_capabilities(&mut updates).await;
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
}

// Internal interface
impl<'a, C: Client<T>, T: Timestamp + Lattice> StorageController<'a, C, T> {
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
    /// Validate that a collection exists for all identifiers, and error if any do not.
    pub fn validate_ids(&self, ids: impl Iterator<Item = GlobalId>) -> Result<(), StorageError> {
        for id in ids {
            self.collection(id)?;
        }
        Ok(())
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
    pub(super) read_capabilities: MutableAntichain<T>,
    /// The implicit capability associated with collection creation.
    pub(super) implied_capability: Antichain<T>,
}

impl<T: Timestamp> CollectionState<T> {
    pub fn new(description: SourceDesc, since: Antichain<T>) -> Self {
        let mut read_capabilities = MutableAntichain::new();
        read_capabilities.update_iter(since.iter().map(|time| (time.clone(), 1)));
        Self {
            description: (description, since.clone()),
            read_capabilities,
            implied_capability: since,
        }
    }
}
