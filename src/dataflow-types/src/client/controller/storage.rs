// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use differential_dataflow::lattice::Lattice;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, Timestamp};
use tracing::error;

use crate::client::SourceConnector;
use crate::client::{Client, Command, StorageCommand};
use crate::sources::SourceDesc;
use crate::Update;
use mz_expr::GlobalId;
use mz_expr::PartitionId;

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

    pub async fn create_sources(
        &mut self,
        mut bindings: Vec<(GlobalId, (crate::sources::SourceDesc, Antichain<T>))>,
    ) -> Result<(), StorageError> {
        // Validate first, to avoid corrupting state.
        // 1. create a dropped source identifier, or
        // 2. create an existing source identifier with a new description.
        // Make sure to check for errors within `bindings` as well.
        bindings.sort_by_key(|b| b.0);
        bindings.dedup();
        for pos in 1..bindings.len() {
            if bindings[pos - 1].0 == bindings[pos].0 {
                Err(StorageError::SourceIdReused(bindings[pos].0))?;
            }
        }
        for (id, description_since) in bindings.iter() {
            if let Ok(collection) = self.collection(*id) {
                if &collection.description != description_since {
                    Err(StorageError::SourceIdReused(*id))?
                }
            }
        }
        // Install collection state for each bound source.
        for (id, (description, since)) in bindings.iter() {
            let collection = CollectionState::new(description.clone(), since.clone());
            self.storage.collections.insert(*id, collection);
        }

        self.client
            .send(Command::Storage(StorageCommand::CreateSources(bindings)))
            .await;

        Ok(())
    }
    pub async fn drop_sources(&mut self, identifiers: Vec<GlobalId>) {
        // Dropping a source is equivalent to releasing any implicit capability.
        // TODO(mcsherry): hold back this announcement until all capabilities are released.
        for id in identifiers.iter() {
            match self.collection_mut(*id) {
                Ok(collection) => {
                    // Apply the updates but ignore the results for now.
                    // TODO(mcsherry): observe the results and allow compaction.
                    let _ = collection.capability_downgrade(Antichain::new());
                }
                Err(_identifier_missing) => {
                    // This isn't an unrecoverable error, just .. probably wrong.
                    error!("Source id {} dropped without first being created", id);
                }
            }
        }

        self.client
            .send(Command::Storage(StorageCommand::DropSources(identifiers)))
            .await
    }
    pub async fn table_insert(&mut self, id: GlobalId, updates: Vec<Update<T>>) {
        self.client
            .send(Command::Storage(StorageCommand::Insert { id, updates }))
            .await
    }
    pub async fn update_durability_frontiers(&mut self, updates: Vec<(GlobalId, Antichain<T>)>) {
        self.client
            .send(Command::Storage(StorageCommand::DurabilityFrontierUpdates(
                updates,
            )))
            .await
    }
    pub async fn add_source_timestamping(
        &mut self,
        id: GlobalId,
        connector: SourceConnector,
        bindings: Vec<(PartitionId, T, crate::sources::MzOffset)>,
    ) {
        self.client
            .send(Command::Storage(StorageCommand::AddSourceTimestamping {
                id,
                connector,
                bindings,
            }))
            .await
    }
    pub async fn allow_source_compaction(
        &mut self,
        frontiers: Vec<(GlobalId, Antichain<T>)>,
    ) -> Result<(), StorageError> {
        // Validate that the ids exist.
        self.validate_ids(frontiers.iter().map(|(id, _)| *id))?;

        // Downgrade the implicit capability for each referenced id.
        for (id, frontier) in frontiers.iter() {
            // Apply the updates but ignore the results for now.
            // TODO(mcsherry): observe the results and allow compaction.
            let _ = self
                .collection_mut(*id)?
                .capability_downgrade(frontier.clone());
        }
        // TODO(mcsherry): Delay compation subject to read capability constraints.
        self.client
            .send(Command::Storage(StorageCommand::AllowSourceCompaction(
                frontiers,
            )))
            .await;

        Ok(())
    }
    pub async fn drop_source_timestamping(&mut self, id: GlobalId) {
        self.client
            .send(Command::Storage(StorageCommand::DropSourceTimestamping {
                id,
            }))
            .await
    }
    pub async fn advance_all_table_timestamps(&mut self, advance_to: T) {
        self.client
            .send(Command::Storage(StorageCommand::AdvanceAllLocalInputs {
                advance_to,
            }))
            .await
    }
}

// Internal interface
impl<'a, C: Client<T>, T: Timestamp + Lattice> StorageController<'a, C, T> {
    pub fn collection_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut CollectionState<T>, StorageError> {
        self.storage
            .collections
            .get_mut(&id)
            .ok_or(StorageError::IdentifierMissing(id))
    }

    pub fn validate_ids(&self, ids: impl Iterator<Item = GlobalId>) -> Result<(), StorageError> {
        for id in ids {
            self.collection(id)?;
        }
        Ok(())
    }
}

/// State maintained about individual collections.
pub struct CollectionState<T> {
    /// Description with which the source was created, and its initial `since`.
    pub(super) description: (crate::sources::SourceDesc, Antichain<T>),
    /// Accumulation of read capabilities for the collection.
    pub(super) since: MutableAntichain<T>,
    /// The implicit capability associated with source creation.
    // TODO(mcsherry): make these capabilities explicit.
    pub(super) capability: Antichain<T>,
}

impl<T: Timestamp> CollectionState<T> {
    pub fn new(description: SourceDesc, since: Antichain<T>) -> Self {
        Self {
            description: (description, since.clone()),
            since: since.borrow().into(),
            capability: since,
        }
    }

    pub fn capability_downgrade(
        &mut self,
        mut frontier: Antichain<T>,
    ) -> impl Iterator<Item = (T, i64)> + '_ {
        std::mem::swap(&mut self.capability, &mut frontier);
        let changes = frontier
            .into_iter()
            .map(|time| (time, -1))
            .chain(self.capability.iter().map(|time| (time.clone(), 1)));
        self.since.update_iter(changes)
    }
}
