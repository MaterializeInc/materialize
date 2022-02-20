// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::lattice::Lattice;
use timely::progress::{Antichain, Timestamp};

use super::SinceUpperMap;
use crate::client::{Client, Command, ComputeCommand, ComputeInstanceId, StorageCommand};
use crate::logging::LoggingConfig;
use crate::DataflowDescription;
use mz_expr::GlobalId;
use mz_expr::RowSetFinishing;
use mz_repr::Row;

/// Controller state maintained for each compute instance.
pub(super) struct ComputeControllerState<T> {
    /// Tracks expressed `since` and received `upper` frontiers for indexes and sinks.
    pub(super) since_uppers: SinceUpperMap<T>,
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
        let mut since_uppers = SinceUpperMap::default();
        if let Some(logging_config) = logging.as_ref() {
            for id in logging_config.log_identifiers() {
                since_uppers.insert(
                    id,
                    (
                        Antichain::from_elem(T::minimum()),
                        Antichain::from_elem(T::minimum()),
                    ),
                );
            }
        }
        Self { since_uppers }
    }
}

impl<'a, C: Client<T>, T: Timestamp + Lattice> ComputeController<'a, C, T> {
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
                let (since, _upper) = self
                    .storage
                    .since_uppers
                    .get(source_id)
                    .ok_or(ComputeError::IdentifierMissing(*source_id))?;
                if !(<_ as timely::order::PartialOrder>::less_equal(&since, &as_of.borrow())) {
                    Err(ComputeError::DataflowSinceViolation(*source_id))?;
                }
            }

            // Validate indexes have `since.less_equal(as_of)`.
            // TODO(mcsherry): Instead, return an error from the constructing method.
            for (index_id, _) in dataflow.index_imports.iter() {
                let (since, _upper) = self
                    .compute
                    .since_uppers
                    .get(index_id)
                    .ok_or(ComputeError::IdentifierMissing(*index_id))?;
                if !(<_ as timely::order::PartialOrder>::less_equal(&since, &as_of.borrow())) {
                    Err(ComputeError::DataflowSinceViolation(*index_id))?;
                }
            }

            for (sink_id, _) in dataflow.sink_exports.iter() {
                self.compute.since_uppers.insert(
                    *sink_id,
                    (as_of.clone(), Antichain::from_elem(Timestamp::minimum())),
                );
            }
            for (index_id, _, _) in dataflow.index_exports.iter() {
                self.compute.since_uppers.insert(
                    *index_id,
                    (as_of.clone(), Antichain::from_elem(Timestamp::minimum())),
                );
            }
        }

        let sources = dataflows
            .iter()
            .map(|dataflow| {
                (
                    dataflow.debug_name.clone(),
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
    pub async fn drop_sinks(&mut self, sink_identifiers: Vec<GlobalId>) {
        self.client
            .send(Command::Compute(
                ComputeCommand::DropSinks(sink_identifiers),
                self.instance,
            ))
            .await;
    }
    pub async fn drop_indexes(&mut self, index_identifiers: Vec<GlobalId>) {
        self.client
            .send(Command::Compute(
                ComputeCommand::DropIndexes(index_identifiers),
                self.instance,
            ))
            .await;
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
        let (since, _upper) = self
            .compute
            .since_uppers
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
    pub async fn allow_index_compaction(
        &mut self,
        frontiers: Vec<(GlobalId, Antichain<T>)>,
    ) -> Result<(), ComputeError> {
        for (id, frontier) in frontiers.iter() {
            self.compute.since_uppers.advance_since_for(*id, frontier);
        }

        self.client
            .send(Command::Compute(
                ComputeCommand::AllowIndexCompaction(frontiers),
                self.instance,
            ))
            .await;

        Ok(())
    }
}
