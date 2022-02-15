// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A client that maintains summaries of the involved objects.
use std::collections::BTreeMap;

use timely::progress::{frontier::AntichainRef, Antichain, ChangeBatch};

use crate::client::SourceConnector;
use crate::client::{
    Client, Command, ComputeCommand, ComputeInstanceId, ComputeResponse, Response, StorageCommand,
};
use crate::logging::LoggingConfig;
use crate::DataflowDescription;
use crate::Update;
use mz_expr::GlobalId;
use mz_expr::PartitionId;
use mz_expr::RowSetFinishing;
use mz_repr::{Row, Timestamp};

/// A client that maintains soft state and validates commands, in addition to forwarding them.
pub struct Controller<C> {
    /// The underlying client,
    client: C,
    /// Sources that have been created.
    ///
    /// A `None` variant means that the source was dropped before it was first created.
    source_descriptions: std::collections::BTreeMap<
        GlobalId,
        Option<(crate::sources::SourceDesc, Antichain<mz_repr::Timestamp>)>,
    >,
    /// Tracks `since` and `upper` frontiers for indexes and sinks.
    compute_since_uppers: BTreeMap<ComputeInstanceId, SinceUpperMap>,
    /// Tracks `since` and `upper` frontiers for sources and tables.
    storage_since_uppers: SinceUpperMap,
}

/// Controller errors, either from compute or storage commands.
#[derive(Debug)]
pub enum ControllerError {
    /// Errors arising from compute commands.
    Compute(ComputeError),
    /// Errors arising from storage commands.
    Storage(StorageError),
}

impl From<ComputeError> for ControllerError {
    fn from(err: ComputeError) -> ControllerError {
        ControllerError::Compute(err)
    }
}
impl From<StorageError> for ControllerError {
    fn from(err: StorageError) -> ControllerError {
        ControllerError::Storage(err)
    }
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
}

#[derive(Debug)]
pub enum StorageError {
    /// The source identifier was re-created after having been dropped,
    /// or installed with a different description.
    SourceIdReused(GlobalId),
}

// Implementation of COMPUTE commands.
impl<C: Client> Controller<C> {
    pub async fn create_instance(
        &mut self,
        instance: ComputeInstanceId,
        logging: Option<LoggingConfig>,
    ) -> Result<(), ControllerError> {
        self.compute_since_uppers
            .insert(instance, Default::default());

        if let Some(logging_config) = logging.as_ref() {
            for id in logging_config.log_identifiers() {
                // This cannot fail, as we inserted just above.
                self.compute_since_uppers
                    .get_mut(&instance)
                    .ok_or(ComputeError::InstanceMissing(instance))?
                    .insert(id, (Antichain::from_elem(0), Antichain::from_elem(0)));
            }
        }

        self.client
            .send(Command::Compute(
                ComputeCommand::CreateInstance(logging),
                instance,
            ))
            .await;

        Ok(())
    }
    pub async fn drop_instance(&mut self, instance: ComputeInstanceId) {
        self.compute_since_uppers.remove(&instance);

        self.client
            .send(Command::Compute(ComputeCommand::DropInstance, instance))
            .await;
    }
    pub async fn create_dataflows(
        &mut self,
        instance: ComputeInstanceId,
        dataflows: Vec<DataflowDescription<crate::plan::Plan>>,
    ) -> Result<(), ControllerError> {
        let compute_since_uppers = self
            .compute_since_uppers
            .get_mut(&instance)
            .ok_or(ComputeError::InstanceMissing(instance))?;

        // Validate dataflows as having inputs whose `since` is less or equal to the dataflow's `as_of`.
        // Start tracking frontiers for each dataflow, using its `as_of` for each index and sink.
        for dataflow in dataflows.iter() {
            let as_of = dataflow
                .as_of
                .as_ref()
                .ok_or(ComputeError::DataflowMalformed)?;

            // Validate sources have `since.less_equal(as_of)`.
            for (source_id, _) in dataflow.source_imports.iter() {
                let (since, _upper) = self
                    .storage_since_uppers
                    .get(source_id)
                    .ok_or(ComputeError::IdentifierMissing(*source_id))?;
                if !(<_ as timely::order::PartialOrder>::less_equal(&since, &as_of.borrow())) {
                    Err(ComputeError::DataflowSinceViolation(*source_id))?;
                }
            }

            // Validate indexes have `since.less_equal(as_of)`.
            // TODO(mcsherry): Instead, return an error from the constructing method.
            for (index_id, _) in dataflow.index_imports.iter() {
                let (since, _upper) = compute_since_uppers
                    .get(index_id)
                    .ok_or(ComputeError::IdentifierMissing(*index_id))?;
                if !(<_ as timely::order::PartialOrder>::less_equal(&since, &as_of.borrow())) {
                    Err(ComputeError::DataflowSinceViolation(*index_id))?;
                }
            }

            for (sink_id, _) in dataflow.sink_exports.iter() {
                // We start tracking `upper` at 0; correct this should that change (e.g. to `as_of`).
                compute_since_uppers.insert(*sink_id, (as_of.clone(), Antichain::from_elem(0)));
            }
            for (index_id, _, _) in dataflow.index_exports.iter() {
                // We start tracking `upper` at 0; correct this should that change (e.g. to `as_of`).
                compute_since_uppers.insert(*index_id, (as_of.clone(), Antichain::from_elem(0)));
            }
        }

        self.client
            .send(Command::Compute(
                ComputeCommand::CreateDataflows(dataflows),
                instance,
            ))
            .await;

        Ok(())
    }
    pub async fn drop_sinks(
        &mut self,
        instance: ComputeInstanceId,
        sink_identifiers: Vec<GlobalId>,
    ) {
        self.client
            .send(Command::Compute(
                ComputeCommand::DropSinks(sink_identifiers),
                instance,
            ))
            .await;
    }
    pub async fn drop_indexes(
        &mut self,
        instance: ComputeInstanceId,
        index_identifiers: Vec<GlobalId>,
    ) {
        self.client
            .send(Command::Compute(
                ComputeCommand::DropIndexes(index_identifiers),
                instance,
            ))
            .await;
    }
    pub async fn peek(
        &mut self,
        instance: ComputeInstanceId,
        id: GlobalId,
        key: Option<Row>,
        conn_id: u32,
        timestamp: Timestamp,
        finishing: RowSetFinishing,
        map_filter_project: mz_expr::SafeMfpPlan,
    ) -> Result<(), ControllerError> {
        let (since, _upper) = self
            .compute_since_uppers
            .get(&instance)
            .ok_or(ComputeError::InstanceMissing(instance))?
            .get(&id)
            .ok_or(ComputeError::IdentifierMissing(id))?;

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
                instance,
            ))
            .await;

        Ok(())
    }
    pub async fn cancel_peek(&mut self, instance: ComputeInstanceId, conn_id: u32) {
        self.client
            .send(Command::Compute(
                ComputeCommand::CancelPeek { conn_id },
                instance,
            ))
            .await;
    }
    pub async fn allow_index_compaction(
        &mut self,
        instance: ComputeInstanceId,
        frontiers: Vec<(GlobalId, Antichain<Timestamp>)>,
    ) -> Result<(), ControllerError> {
        let compute_since_uppers = self
            .compute_since_uppers
            .get_mut(&instance)
            .ok_or(ComputeError::InstanceMissing(instance))?;

        for (id, frontier) in frontiers.iter() {
            compute_since_uppers.advance_since_for(*id, frontier);
        }

        self.client
            .send(Command::Compute(
                ComputeCommand::AllowIndexCompaction(frontiers),
                instance,
            ))
            .await;

        Ok(())
    }
}

// Implementation of STORAGE commands.
impl<C: Client> Controller<C> {
    pub async fn create_sources(
        &mut self,
        mut bindings: Vec<(GlobalId, (crate::sources::SourceDesc, Antichain<Timestamp>))>,
    ) -> Result<(), ControllerError> {
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
            match self.source_descriptions.get(&id) {
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
            self.source_descriptions
                .insert(*id, Some((description.clone(), since.clone())));
            // We start tracking `upper` at 0; correct this should that change (e.g. to `as_of`).
            self.storage_since_uppers
                .insert(*id, (since.clone(), Antichain::from_elem(0)));
        }

        self.client
            .send(Command::Storage(StorageCommand::CreateSources(bindings)))
            .await;

        Ok(())
    }
    pub async fn drop_sources(&mut self, identifiers: Vec<GlobalId>) {
        for id in identifiers.iter() {
            if !self.source_descriptions.contains_key(id) {
                // This isn't an unrecoverable error, just .. probably wrong.
                tracing::error!("Source id {} dropped without first being created", id);
            } else {
                self.source_descriptions.insert(*id, None);
            }
        }

        self.client
            .send(Command::Storage(StorageCommand::DropSources(identifiers)))
            .await
    }
    pub async fn table_insert(&mut self, id: GlobalId, updates: Vec<Update>) {
        self.client
            .send(Command::Storage(StorageCommand::Insert { id, updates }))
            .await
    }
    pub async fn update_durability_frontiers(
        &mut self,
        updates: Vec<(GlobalId, Antichain<Timestamp>)>,
    ) {
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
        bindings: Vec<(PartitionId, Timestamp, crate::sources::MzOffset)>,
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
        frontiers: Vec<(GlobalId, Antichain<Timestamp>)>,
    ) {
        for (id, frontier) in frontiers.iter() {
            self.storage_since_uppers.advance_since_for(*id, frontier);
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
    pub async fn advance_all_table_timestamps(&mut self, advance_to: Timestamp) {
        self.client
            .send(Command::Storage(StorageCommand::AdvanceAllLocalInputs {
                advance_to,
            }))
            .await
    }
}

impl<C: Client> Controller<C> {
    pub async fn recv(&mut self) -> Option<Response> {
        let response = self.client.recv().await;

        if let Some(response) = response.as_ref() {
            match response {
                Response::Compute(ComputeResponse::FrontierUppers(updates), instance) => {
                    for (id, changes) in updates.iter() {
                        self.compute_since_uppers
                            .get_mut(instance)
                            // TODO: determine if this is an error, or perhaps just a late
                            // response about a terminated instance.
                            .expect("Reference to absent instance")
                            .update_upper_for(*id, changes);
                    }
                }
                _ => {}
            }
        }

        response
    }
}

impl<C> Controller<C> {
    /// Create a new controller from a client it should wrap.
    pub fn new(client: C) -> Self {
        Self {
            client,
            source_descriptions: Default::default(),
            compute_since_uppers: Default::default(),
            storage_since_uppers: Default::default(),
        }
    }
    /// Returns the source description for a given identifier.
    ///
    /// The response does not distinguish between an as yet uncreated source description,
    /// and one that has been created and then dropped (or dropped without creation).
    /// There is a distinction and the client is aware of it, and could plausibly return
    /// this information if we had a use for it.
    pub fn source_description_for(
        &self,
        id: GlobalId,
    ) -> Option<&(crate::sources::SourceDesc, Antichain<mz_repr::Timestamp>)> {
        self.source_descriptions.get(&id).unwrap_or(&None).as_ref()
    }

    /// Returns the pair of `since` and `upper` for a source, if it exists.
    ///
    /// The `since` frontier indicates that the maintained data are certainly valid for times greater
    /// or equal to the frontier, but they may not be for other times. Attempting to create a dataflow
    /// using this `id` with an `as_of` that is not at least `since` will result in an error.
    ///
    /// The `upper` frontier indicates that the data are reported available for all times not greater
    /// or equal to the frontier. Dataflows with an `as_of` greater or equal to this frontier may not
    /// immediately produce results.
    pub fn source_since_upper_for(
        &self,
        id: GlobalId,
    ) -> Option<(AntichainRef<Timestamp>, AntichainRef<Timestamp>)> {
        self.storage_since_uppers.get(&id)
    }
}

#[derive(Default)]
struct SinceUpperMap {
    since_uppers:
        std::collections::BTreeMap<GlobalId, (Antichain<Timestamp>, Antichain<Timestamp>)>,
}

impl SinceUpperMap {
    fn insert(&mut self, id: GlobalId, since_upper: (Antichain<Timestamp>, Antichain<Timestamp>)) {
        self.since_uppers.insert(id, since_upper);
    }
    fn get(&self, id: &GlobalId) -> Option<(AntichainRef<Timestamp>, AntichainRef<Timestamp>)> {
        self.since_uppers
            .get(id)
            .map(|(since, upper)| (since.borrow(), upper.borrow()))
    }
    fn advance_since_for(&mut self, id: GlobalId, frontier: &Antichain<Timestamp>) {
        if let Some((since, _upper)) = self.since_uppers.get_mut(&id) {
            use differential_dataflow::lattice::Lattice;
            since.join_assign(frontier);
        } else {
            // If we allow compaction before the item is created, pre-restrict the valid range.
            // We start tracking `upper` at 0; correct this should that change (e.g. to `as_of`).
            self.since_uppers
                .insert(id, (frontier.clone(), Antichain::from_elem(0)));
        }
    }
    fn update_upper_for(&mut self, id: GlobalId, changes: &ChangeBatch<Timestamp>) {
        if let Some((_since, upper)) = self.since_uppers.get_mut(&id) {
            // Apply `changes` to `upper`.
            let mut changes = changes.clone();
            for time in upper.elements().iter() {
                changes.update(time.clone(), 1);
            }
            upper.clear();
            for (time, count) in changes.drain() {
                assert_eq!(count, 1);
                upper.insert(time);
            }
        } else {
            // No panic, as we could have recently dropped this.
            // If we can tell these are updates to an id that could still be constructed,
            // something is weird and we should error.
        }
    }
}
