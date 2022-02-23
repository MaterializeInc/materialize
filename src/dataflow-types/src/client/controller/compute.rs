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
}

impl<T: Timestamp + Lattice> ComputeControllerState<T> {
    pub(super) fn new(logging: &Option<LoggingConfig>) -> Self {
        let mut collections = BTreeMap::default();
        if let Some(logging_config) = logging.as_ref() {
            for id in logging_config.log_identifiers() {
                collections.insert(id, CollectionState::new(Antichain::from_elem(T::minimum())));
            }
        }
        Self { collections }
    }
}

// Public interface
impl<'a, C: Client<T>, T: Timestamp + Lattice> ComputeController<'a, C, T> {
    pub fn collection(&self, id: GlobalId) -> Result<&CollectionState<T>, ComputeError> {
        self.compute
            .collections
            .get(&id)
            .ok_or(ComputeError::IdentifierMissing(id))
    }

    pub async fn create_dataflows(
        &mut self,
        dataflows: Vec<DataflowDescription<crate::plan::Plan, T>>,
    ) -> Result<(), ComputeError> {
        // Validate dataflows as having inputs whose `since` is less or equal to the dataflow's `as_of`.
        // Start tracking frontiers for each dataflow, using its `as_of` for each index and sink.
        for dataflow in dataflows.iter() {
            let as_of = dataflow
                .as_of
                .as_ref()
                .ok_or(ComputeError::DataflowMalformed)?;

            // Validate sources have `since.less_equal(as_of)`.
            for (source_id, _) in dataflow.source_imports.iter() {
                let since = &self
                    .storage
                    .collections
                    .get(source_id)
                    .ok_or(ComputeError::IdentifierMissing(*source_id))?
                    .since;
                if !(<_ as timely::order::PartialOrder>::less_equal(
                    &since.frontier(),
                    &as_of.borrow(),
                )) {
                    Err(ComputeError::DataflowSinceViolation(*source_id))?;
                }
            }

            // Validate indexes have `since.less_equal(as_of)`.
            // TODO(mcsherry): Instead, return an error from the constructing method.
            for (index_id, _) in dataflow.index_imports.iter() {
                let since = self.collection(*index_id)?.since.frontier();
                if !(<_ as timely::order::PartialOrder>::less_equal(&since, &as_of.borrow())) {
                    Err(ComputeError::DataflowSinceViolation(*index_id))?;
                }
            }

            for (sink_id, _) in dataflow.sink_exports.iter() {
                self.compute
                    .collections
                    .insert(*sink_id, CollectionState::new(as_of.clone()));
            }
            for (index_id, _, _) in dataflow.index_exports.iter() {
                self.compute
                    .collections
                    .insert(*index_id, CollectionState::new(as_of.clone()));
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
            .await;
        self.client
            .send(Command::Compute(
                ComputeCommand::CreateDataflows(dataflows),
                self.instance,
            ))
            .await;

        Ok(())
    }
    pub async fn drop_sinks(&mut self, identifiers: Vec<GlobalId>) -> Result<(), ComputeError> {
        // Validate that the ids exist.
        self.validate_ids(identifiers.iter().cloned())?;

        let compaction_commands = identifiers
            .into_iter()
            .map(|id| (id, Antichain::new()))
            .collect();
        self.allow_index_compaction(compaction_commands).await;
        Ok(())
    }
    pub async fn drop_indexes(&mut self, identifiers: Vec<GlobalId>) -> Result<(), ComputeError> {
        // Validate that the ids exist.
        self.validate_ids(identifiers.iter().cloned())?;

        let compaction_commands = identifiers
            .into_iter()
            .map(|id| (id, Antichain::new()))
            .collect();
        self.allow_index_compaction(compaction_commands).await;
        Ok(())
    }
    pub async fn peek(
        &mut self,
        id: GlobalId,
        key: Option<Row>,
        conn_id: u32,
        timestamp: T,
        finishing: RowSetFinishing,
        map_filter_project: mz_expr::SafeMfpPlan,
    ) -> Result<(), ComputeError> {
        let since = self.collection(id)?.since.frontier();

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
            .await;

        Ok(())
    }
    pub async fn cancel_peek(&mut self, conn_id: u32) {
        self.client
            .send(Command::Compute(
                ComputeCommand::CancelPeek { conn_id },
                self.instance,
            ))
            .await;
    }

    pub async fn allow_index_compaction(&mut self, frontiers: Vec<(GlobalId, Antichain<T>)>) {
        // The coordinator currently sends compaction commands for identifiers that do not exist.
        // Until that changes, we need to be oblivious to errors, or risk not compacting anything.

        // // Validate that the ids exist.
        // self.validate_ids(frontiers.iter().map(|(id, _)| *id))?;
        //
        // // Downgrade the implicit capability for each referenced id.
        // for (id, frontier) in frontiers.iter() {
        //     // Apply the updates but ignore the results for now.
        //     // TODO(mcsherry): observe the results and allow compaction.
        //     let _ = self
        //         .collection_mut(*id)?
        //         .capability_downgrade(frontier.clone());
        // }
        // // TODO(mcsherry): Delay compation subject to read capability constraints.

        self.client
            .send(Command::Compute(
                ComputeCommand::AllowCompaction(frontiers),
                self.instance,
            ))
            .await;
    }
}

// Internal interface
impl<'a, C: Client<T>, T: Timestamp + Lattice> ComputeController<'a, C, T> {
    pub fn collection_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut CollectionState<T>, ComputeError> {
        self.compute
            .collections
            .get_mut(&id)
            .ok_or(ComputeError::IdentifierMissing(id))
    }
    pub fn validate_ids(&self, ids: impl Iterator<Item = GlobalId>) -> Result<(), ComputeError> {
        for id in ids {
            self.collection(id)?;
        }
        Ok(())
    }
}

/// State maintained about individual collections.
pub struct CollectionState<T> {
    /// Accumulation of read capabilities for the collection.
    pub(super) since: MutableAntichain<T>,
    /// Reported progress in the write capabilities.
    pub(super) upper_frontier: MutableAntichain<T>,
    /// The implicit capability associated with source creation.
    // TODO(mcsherry): make these capabilities explicit.
    pub(super) capability: Antichain<T>,
}

impl<T: Timestamp> CollectionState<T> {
    pub fn new(since: Antichain<T>) -> Self {
        Self {
            since: since.borrow().into(),
            upper_frontier: MutableAntichain::new_bottom(Timestamp::minimum()),
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
