// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A controller that provides an interface to a compute instance, and the storage layer below it.
//!
//! The compute controller curates the creation of indexes and sinks, the progress of readers through
//! these collections, and their eventual dropping and resource reclamation.
//!
//! The compute controller can be viewed as a partial map from `GlobalId` to collection. It is an error to
//! use an identifier before it has been "created" with `create_dataflows()`. Once created, the controller holds
//! a read capability for each output collection of a dataflow, which is manipulated with `allow_compaction()`.
//! Eventually, a collecction is dropped with either `drop_sources()` or by allowing compaction to the empty frontier.
//!
//! Created dataflows will prevent the compaction of their inputs, including other compute collections but also
//! collections managed by the storage layer. Each dataflow input is prevented from compacting beyond the allowed
//! compaction of each of its outputs, ensuring that we can recover each dataflow to its current state in case of
//! failure or other reconfiguration.

use std::collections::BTreeMap;

use differential_dataflow::lattice::Lattice;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, ChangeBatch, Timestamp};

use crate::client::{Client, Command, ComputeCommand, ComputeInstanceId, StorageCommand};
use crate::logging::LoggingConfig;
use crate::DataflowDescription;
use mz_expr::GlobalId;
use mz_expr::RowSetFinishing;
use mz_repr::Row;

/// Controller state maintained for each compute instance.
pub(super) struct ComputeControllerState<T> {
    /// Tracks expressed `since` and received `upper` frontiers for indexes and sinks.
    pub(super) collections: BTreeMap<GlobalId, CollectionState<T>>,
}

/// A controller for a compute instance.
pub struct ComputeController<'a, C, T> {
    pub(super) instance: ComputeInstanceId,
    pub(super) compute: &'a mut ComputeControllerState<T>,
    pub(super) storage: &'a mut super::StorageControllerState<T>,
    pub(super) client: &'a mut C,
}

/// Errors arising from compute commands.
#[derive(Debug)]
pub enum ComputeError {
    /// Command referenced an instance that was not present.
    InstanceMissing(ComputeInstanceId),
    /// Command referenced an identifier that was not present.
    IdentifierMissing(GlobalId),
    /// Dataflow was malformed (e.g. missing `as_of`).
    DataflowMalformed,
    /// The dataflow `as_of` was not greater than the `since` of the identifier.
    DataflowSinceViolation(GlobalId),
    /// The peek `timestamp` was not greater than the `since` of the identifier.
    PeekSinceViolation(GlobalId),
    /// An error from the underlying client.
    ClientError(anyhow::Error),
}

impl From<anyhow::Error> for ComputeError {
    fn from(error: anyhow::Error) -> Self {
        Self::ClientError(error)
    }
}

impl<T: Timestamp + Lattice> ComputeControllerState<T> {
    pub(super) fn new(logging: &Option<LoggingConfig>) -> Self {
        let mut collections = BTreeMap::default();
        if let Some(logging_config) = logging.as_ref() {
            for id in logging_config.log_identifiers() {
                collections.insert(
                    id,
                    CollectionState::new(
                        Antichain::from_elem(T::minimum()),
                        Vec::new(),
                        Vec::new(),
                    ),
                );
            }
        }
        Self { collections }
    }
}

// Public interface
impl<'a, C: Client<T>, T: Timestamp + Lattice> ComputeController<'a, C, T> {
    /// Acquires a handle to a controller for the storage instance.
    #[inline]
    pub fn storage(&mut self) -> crate::client::controller::StorageController<C, T> {
        crate::client::controller::StorageController {
            storage: &mut self.storage,
            client: &mut self.client,
        }
    }
    /// Acquire a handle to the collection state associated with `id`.
    pub fn collection(&self, id: GlobalId) -> Result<&CollectionState<T>, ComputeError> {
        self.compute
            .collections
            .get(&id)
            .ok_or(ComputeError::IdentifierMissing(id))
    }
    /// Creates and maintains the described dataflows, and initializes state for their output.
    ///
    /// This method creates dataflows whose inputs are still readable at the dataflow `as_of`
    /// frontier, and initializes the outputs as readable from that frontier onward.
    /// It installs read dependencies from the outputs to the inputs, so that the input read
    /// capabilities will be held back to the output read capabilities, ensuring that we are
    /// always able to return to a state that can serve the output read capabilities.
    pub async fn create_dataflows(
        &mut self,
        dataflows: Vec<DataflowDescription<crate::plan::Plan<T>, T>>,
    ) -> Result<(), ComputeError> {
        // Validate dataflows as having inputs whose `since` is less or equal to the dataflow's `as_of`.
        // Start tracking frontiers for each dataflow, using its `as_of` for each index and sink.
        for dataflow in dataflows.iter() {
            let as_of = dataflow
                .as_of
                .as_ref()
                .ok_or(ComputeError::DataflowMalformed)?;

            // Record all transitive dependencies of the outputs.
            let mut storage_dependencies = Vec::new();
            let mut compute_dependencies = Vec::new();

            // Validate sources have `since.less_equal(as_of)`.
            for (source_id, _) in dataflow.source_imports.iter() {
                let since = &self
                    .storage
                    .collections
                    .get(source_id)
                    .ok_or(ComputeError::IdentifierMissing(*source_id))?
                    .read_capabilities
                    .frontier();
                if !(<_ as timely::order::PartialOrder>::less_equal(since, &as_of.borrow())) {
                    Err(ComputeError::DataflowSinceViolation(*source_id))?;
                }

                storage_dependencies.push(*source_id);
            }

            // Validate indexes have `since.less_equal(as_of)`.
            // TODO(mcsherry): Instead, return an error from the constructing method.
            for (index_id, _) in dataflow.index_imports.iter() {
                let collection = self.collection(*index_id)?;
                let since = collection.read_capabilities.frontier();
                if !(<_ as timely::order::PartialOrder>::less_equal(&since, &as_of.borrow())) {
                    Err(ComputeError::DataflowSinceViolation(*index_id))?;
                } else {
                    compute_dependencies.push(*index_id);
                }
            }

            // Canonicalize depedencies.
            // Probably redundant based on key structure, but doing for sanity.
            storage_dependencies.sort();
            storage_dependencies.dedup();
            compute_dependencies.sort();
            compute_dependencies.dedup();

            // We will bump the internals of each input by the number of dependents (outputs).
            let outputs = dataflow.sink_exports.len() + dataflow.index_exports.len();
            let mut changes = ChangeBatch::new();
            for time in as_of.iter() {
                changes.update(time.clone(), outputs as i64);
            }
            // Update storage read capabilities for inputs.
            let mut storage_read_updates = storage_dependencies
                .iter()
                .map(|id| (*id, changes.clone()))
                .collect();
            self.storage()
                .update_read_capabilities(&mut storage_read_updates)
                .await;
            // Update compute read capabilities for inputs.
            let mut compute_read_updates = compute_dependencies
                .iter()
                .map(|id| (*id, changes.clone()))
                .collect();
            self.update_read_capabilities(&mut compute_read_updates)
                .await;

            // Install collection state for each of the exports.
            for (sink_id, _) in dataflow.sink_exports.iter() {
                self.compute.collections.insert(
                    *sink_id,
                    CollectionState::new(
                        as_of.clone(),
                        storage_dependencies.clone(),
                        compute_dependencies.clone(),
                    ),
                );
            }
            for (index_id, _, _) in dataflow.index_exports.iter() {
                self.compute.collections.insert(
                    *index_id,
                    CollectionState::new(
                        as_of.clone(),
                        storage_dependencies.clone(),
                        compute_dependencies.clone(),
                    ),
                );
            }
        }

        let sources = dataflows
            .iter()
            .map(|dataflow| {
                (
                    dataflow.debug_name.clone(),
                    dataflow.id,
                    dataflow.as_of.clone(),
                    dataflow.source_imports.clone(),
                )
            })
            .collect();

        self.client
            .send(Command::Storage(StorageCommand::RenderSources(sources)))
            .await
            .expect("Storage command failed; unrecoverable");
        self.client
            .send(Command::Compute(
                ComputeCommand::CreateDataflows(dataflows),
                self.instance,
            ))
            .await
            .expect("Compute command failed; unrecoverable");

        Ok(())
    }
    /// Drops the read capability for the sinks and allows their resources to be reclaimed.
    pub async fn drop_sinks(&mut self, identifiers: Vec<GlobalId>) -> Result<(), ComputeError> {
        // Validate that the ids exist.
        self.validate_ids(identifiers.iter().cloned())?;

        let compaction_commands = identifiers
            .into_iter()
            .map(|id| (id, Antichain::new()))
            .collect();
        self.allow_compaction(compaction_commands).await;
        Ok(())
    }
    /// Drops the read capability for the indexes and allows their resources to be reclaimed.
    pub async fn drop_indexes(&mut self, identifiers: Vec<GlobalId>) -> Result<(), ComputeError> {
        // Validate that the ids exist.
        self.validate_ids(identifiers.iter().cloned())?;

        let compaction_commands = identifiers
            .into_iter()
            .map(|id| (id, Antichain::new()))
            .collect();
        self.allow_compaction(compaction_commands).await;
        Ok(())
    }
    /// Initiate a peek request for the contents of `id` at `timestamp`.
    pub async fn peek(
        &mut self,
        id: GlobalId,
        key: Option<Row>,
        conn_id: u32,
        timestamp: T,
        finishing: RowSetFinishing,
        map_filter_project: mz_expr::SafeMfpPlan,
    ) -> Result<(), ComputeError> {
        let since = self.collection(id)?.read_capabilities.frontier();

        if !since.less_equal(&timestamp) {
            Err(ComputeError::PeekSinceViolation(id))?;
        }

        self.client
            .send(Command::Compute(
                ComputeCommand::Peek {
                    id,
                    key,
                    conn_id,
                    timestamp,
                    finishing,
                    map_filter_project,
                },
                self.instance,
            ))
            .await
            .map_err(ComputeError::from)
    }
    /// Cancels an existing peek request.
    pub async fn cancel_peek(&mut self, conn_id: u32) -> Result<(), ComputeError> {
        self.client
            .send(Command::Compute(
                ComputeCommand::CancelPeek { conn_id },
                self.instance,
            ))
            .await
            .map_err(ComputeError::from)
    }

    /// Downgrade the read capabilities of specific identifiers to specific frontiers.
    ///
    /// Downgrading any read capability to the empty frontier will drop the item and eventually reclaim its resources.
    pub async fn allow_compaction(&mut self, frontiers: Vec<(GlobalId, Antichain<T>)>) {
        // The coordinator currently sends compaction commands for identifiers that do not exist.
        // Until that changes, we need to be oblivious to errors, or risk not compacting anything.

        // // Validate that the ids exist.
        // self.validate_ids(frontiers.iter().map(|(id, _)| *id))?;

        let mut updates = BTreeMap::new();
        for (id, mut frontier) in frontiers.into_iter() {
            // If-let to evade some identifiers incorrectly sent to us.
            if let Ok(collection) = self.collection_mut(id) {
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
                    tracing::error!("COMPUTE::allow_compaction attempted frontier regression for id {:?}: {:?} to {:?}", id, collection.implied_capability, frontier);
                }
            }
        }

        self.update_read_capabilities(&mut updates).await;
    }
}

// Internal interface
impl<'a, C: Client<T>, T: Timestamp + Lattice> ComputeController<'a, C, T> {
    /// Acquire a mutable reference to the collection state, should it exist.
    pub(super) fn collection_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut CollectionState<T>, ComputeError> {
        self.compute
            .collections
            .get_mut(&id)
            .ok_or(ComputeError::IdentifierMissing(id))
    }
    /// Validate that a collection exists for all identifiers, and error if any do not.
    pub fn validate_ids(&self, ids: impl Iterator<Item = GlobalId>) -> Result<(), ComputeError> {
        for id in ids {
            self.collection(id)?;
        }
        Ok(())
    }

    /// Applies `updates`, propagates consequences through other read capabilities, and sends an appropriate compaction command.
    pub(super) async fn update_read_capabilities(
        &mut self,
        updates: &mut BTreeMap<GlobalId, ChangeBatch<T>>,
    ) {
        // Locations to record consequences that we need to act on.
        let mut storage_todo = BTreeMap::default();
        let mut compute_net = Vec::default();
        // Repeatedly extract the maximum id, and updates for it.
        while let Some(key) = updates.keys().rev().next().cloned() {
            let mut update = updates.remove(&key).unwrap();
            if let Ok(collection) = self.collection_mut(key) {
                let changes = collection.read_capabilities.update_iter(update.drain());
                update.extend(changes);
                for id in collection.storage_dependencies.iter() {
                    storage_todo
                        .entry(*id)
                        .or_insert_with(ChangeBatch::new)
                        .extend(update.iter().cloned());
                }
                for id in collection.compute_dependencies.iter() {
                    updates
                        .entry(*id)
                        .or_insert_with(ChangeBatch::new)
                        .extend(update.iter().cloned());
                }
                compute_net.push((key, update));
            } else {
                // Storage presumably, but verify.
                storage_todo
                    .entry(key)
                    .or_insert_with(ChangeBatch::new)
                    .extend(update.drain())
            }
        }

        // Translate our net compute actions into `AllowCompaction` commands.
        let mut compaction_commands = Vec::new();
        for (id, change) in compute_net.iter_mut() {
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
                .send(Command::Compute(
                    ComputeCommand::AllowCompaction(compaction_commands),
                    self.instance,
                ))
                .await
                .expect("Compute instance command failed; unrecoverable");
        }

        // We may have storage consequences to process.
        if !storage_todo.is_empty() {
            self.storage()
                .update_read_capabilities(&mut storage_todo)
                .await;
        }
    }
}

/// State maintained about individual collections.
pub struct CollectionState<T> {
    /// Accumulation of read capabilities for the collection.
    ///
    /// This accumulation will always contain `self.implied_capability`, but may also contain
    /// capabilities held by others who have read dependencies on this collection.
    pub(super) read_capabilities: MutableAntichain<T>,
    /// The implicit capability associated with collection creation.
    pub(super) implied_capability: Antichain<T>,

    /// Storage identifiers on which this collection depends.
    pub(super) storage_dependencies: Vec<GlobalId>,
    /// Compute identifiers on which this collection depends.
    pub(super) compute_dependencies: Vec<GlobalId>,

    /// Reported progress in the write capabilities.
    ///
    /// Importantly, this is not a write capability, but what we have heard about the
    /// write capabilities of others. All future writes will have times greater than or
    /// equal to `upper_frontier.frontier()`.
    pub(super) write_frontier: MutableAntichain<T>,
}

impl<T: Timestamp> CollectionState<T> {
    pub fn new(
        since: Antichain<T>,
        storage_dependencies: Vec<GlobalId>,
        compute_dependencies: Vec<GlobalId>,
    ) -> Self {
        let mut read_capabilities = MutableAntichain::new();
        read_capabilities.update_iter(since.iter().map(|time| (time.clone(), 1)));
        Self {
            read_capabilities,
            implied_capability: since,
            storage_dependencies,
            compute_dependencies,
            write_frontier: MutableAntichain::new_bottom(Timestamp::minimum()),
        }
    }
}
