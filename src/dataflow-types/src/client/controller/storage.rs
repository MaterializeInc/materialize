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
use timely::progress::{Antichain, Timestamp};
use tracing::error;

use super::SinceUpperMap;
use crate::client::SourceConnector;
use crate::client::{Client, Command, StorageCommand};
use crate::Update;
use mz_expr::GlobalId;
use mz_expr::PartitionId;

/// Controller state maintained for each storage instance.
pub struct StorageControllerState<T> {
    /// Sources that have been created.
    ///
    /// A `None` variant means that the source was dropped before it was first created.
    source_descriptions: BTreeMap<GlobalId, Option<(crate::sources::SourceDesc, Antichain<T>)>>,
    /// Tracks expressed `since` and received `upper` frontiers for sources and tables.
    pub(super) since_uppers: SinceUpperMap<T>,
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
}

impl<T> StorageControllerState<T> {
    pub(super) fn new() -> Self {
        Self {
            source_descriptions: BTreeMap::default(),
            since_uppers: SinceUpperMap::default(),
        }
    }
}

impl<'a, C: Client<T>, T: Timestamp + Lattice> StorageController<'a, C, T> {
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
            match self.storage.source_descriptions.get(&id) {
                Some(None) => Err(StorageError::SourceIdReused(*id))?,
                Some(Some(prior_description)) => {
                    if prior_description != description_since {
                        Err(StorageError::SourceIdReused(*id))?
                    }
                }
                None => {
                    // All is well; no reason to panic.
                }
            }
        }

        for (id, (description, since)) in bindings.iter() {
            self.storage
                .source_descriptions
                .insert(*id, Some((description.clone(), since.clone())));
            self.storage.since_uppers.insert(
                *id,
                (since.clone(), Antichain::from_elem(Timestamp::minimum())),
            );
        }

        self.client
            .send(Command::Storage(StorageCommand::CreateSources(bindings)))
            .await;

        Ok(())
    }
    pub async fn drop_sources(&mut self, identifiers: Vec<GlobalId>) {
        for id in identifiers.iter() {
            if !self.storage.source_descriptions.contains_key(id) {
                // This isn't an unrecoverable error, just .. probably wrong.
                error!("Source id {} dropped without first being created", id);
            } else {
                self.storage.source_descriptions.insert(*id, None);
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
    pub async fn allow_source_compaction(&mut self, frontiers: Vec<(GlobalId, Antichain<T>)>) {
        for (id, frontier) in frontiers.iter() {
            self.storage.since_uppers.advance_since_for(*id, frontier);
        }

        self.client
            .send(Command::Storage(StorageCommand::AllowSourceCompaction(
                frontiers,
            )))
            .await
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
