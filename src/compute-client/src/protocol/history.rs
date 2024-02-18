// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A reducible history of compute commands.

use std::borrow::Borrow;
use std::collections::BTreeMap;

use mz_ore::cast::CastFrom;
use mz_ore::metrics::UIntGauge;
use timely::progress::Antichain;

use crate::metrics::HistoryMetrics;
use crate::protocol::command::{ComputeCommand, ComputeParameters, Peek};

/// TODO(#25239): Add documentation.
#[derive(Debug)]
pub struct ComputeCommandHistory<M, T = mz_repr::Timestamp> {
    /// The number of commands at the last time we compacted the history.
    reduced_count: usize,
    /// The sequence of commands that should be applied.
    ///
    /// This list may not be "compact" in that there can be commands that could be optimized
    /// or removed given the context of other commands, for example compaction commands that
    /// can be unified, or dataflows that can be dropped due to allowed compaction.
    commands: Vec<ComputeCommand<T>>,
    /// Tracked metrics.
    metrics: HistoryMetrics<M>,
}

impl<M, T> ComputeCommandHistory<M, T>
where
    M: Borrow<UIntGauge>,
    T: timely::progress::Timestamp,
{
    /// TODO(#25239): Add documentation.
    pub fn new(metrics: HistoryMetrics<M>) -> Self {
        metrics.reset();

        Self {
            reduced_count: 0,
            commands: Vec::new(),
            metrics,
        }
    }

    /// Add a command to the history.
    ///
    /// This action will reduce the history every time it doubles.
    pub fn push(&mut self, command: ComputeCommand<T>) {
        self.commands.push(command);

        if self.commands.len() > 2 * self.reduced_count {
            self.reduce();
        } else {
            // Refresh reported metrics. `reduce` already refreshes metrics, so we only need to do
            // that here in the non-reduce case.
            let command = self.commands.last().expect("pushed above");
            self.metrics
                .command_counts
                .for_command(command)
                .borrow()
                .inc();
            if matches!(command, ComputeCommand::CreateDataflow(_)) {
                self.metrics.dataflow_count.borrow().inc();
            }
        }
    }

    /// Reduces `self.history` to a minimal form.
    ///
    /// This action not only simplifies the issued history, but importantly reduces the instructions
    /// to only reference inputs from times that are still certain to be valid. Commands that allow
    /// compaction of a collection also remove certainty that the inputs will be available for times
    /// not greater or equal to that compaction frontier.
    pub fn reduce(&mut self) {
        // First determine what the final compacted frontiers will be for each collection.
        // These will determine for each collection whether the command that creates it is required,
        // and if required what `as_of` frontier should be used for its updated command.
        let mut final_frontiers = BTreeMap::new();
        let mut live_dataflows = Vec::new();
        let mut live_peeks = BTreeMap::new();

        let mut create_inst_command = None;
        let mut create_timely_command = None;

        // Collect only the final configuration.
        // Note that this is only correct as long as all config parameters apply globally. If we
        // ever introduce parameters that only affect subsequent commands, we will have to
        // reconsider this approach.
        let mut final_configuration = ComputeParameters::default();

        let mut initialization_complete = false;

        for command in self.commands.drain(..) {
            match command {
                create_timely @ ComputeCommand::CreateTimely { .. } => {
                    assert!(create_timely_command.is_none());
                    create_timely_command = Some(create_timely);
                }
                // We should be able to handle the Create* commands, should this client need to be restartable.
                create_inst @ ComputeCommand::CreateInstance(_) => {
                    assert!(create_inst_command.is_none());
                    create_inst_command = Some(create_inst);
                }
                ComputeCommand::InitializationComplete => {
                    initialization_complete = true;
                }
                ComputeCommand::UpdateConfiguration(params) => {
                    final_configuration.update(params);
                }
                ComputeCommand::CreateDataflow(dataflow) => {
                    live_dataflows.push(dataflow);
                }
                ComputeCommand::AllowCompaction { id, frontier } => {
                    final_frontiers.insert(id, frontier.clone());
                }
                ComputeCommand::Peek(peek) => {
                    live_peeks.insert(peek.uuid, peek);
                }
                ComputeCommand::CancelPeek { uuid } => {
                    live_peeks.remove(&uuid);
                }
            }
        }

        // Determine the required antichains to support live peeks;
        let mut live_peek_frontiers = std::collections::BTreeMap::new();
        for Peek {
            target, timestamp, ..
        } in live_peeks.values()
        {
            // Introduce `time` as a constraint on the `as_of` frontier of `id`.
            live_peek_frontiers
                .entry(target.id())
                .or_insert_with(Antichain::new)
                .insert(timestamp.clone());
        }

        // Update dataflow `as_of` frontiers, constrained by live peeks and allowed compaction.
        // One possible frontier is the empty frontier, indicating that the dataflow can be removed.
        for dataflow in live_dataflows.iter_mut() {
            let mut as_of = Antichain::new();
            for id in dataflow.export_ids() {
                // If compaction has been allowed use that; otherwise use the initial `as_of`.
                if let Some(frontier) = final_frontiers.get(&id) {
                    as_of.extend(frontier.clone());
                } else {
                    as_of.extend(dataflow.as_of.clone().unwrap());
                }
                // If we have requirements from peeks, apply them to hold `as_of` back.
                if let Some(frontier) = live_peek_frontiers.get(&id) {
                    as_of.extend(frontier.clone());
                }
            }

            // Remove compaction for any collection that brought us to `as_of`.
            for id in dataflow.export_ids() {
                if let Some(frontier) = final_frontiers.get(&id) {
                    if frontier == &as_of {
                        final_frontiers.remove(&id);
                    }
                }
            }

            dataflow.as_of = Some(as_of);
        }

        // Discard dataflows whose outputs have all been allowed to compact away.
        live_dataflows.retain(|dataflow| dataflow.as_of != Some(Antichain::new()));

        // Reconstitute the commands as a compact history.

        // When we update `metrics`, we need to be careful to not transiently report incorrect
        // counts, as they would be observable by other threads.
        let command_counts = &self.metrics.command_counts;
        let dataflow_count = &self.metrics.dataflow_count;

        let count = u64::from(create_timely_command.is_some());
        command_counts.create_timely.borrow().set(count);
        if let Some(create_timely_command) = create_timely_command {
            self.commands.push(create_timely_command);
        }

        let count = u64::from(create_inst_command.is_some());
        command_counts.create_instance.borrow().set(count);
        if let Some(create_inst_command) = create_inst_command {
            self.commands.push(create_inst_command);
        }

        let count = u64::from(!final_configuration.all_unset());
        command_counts.update_configuration.borrow().set(count);
        if !final_configuration.all_unset() {
            self.commands
                .push(ComputeCommand::UpdateConfiguration(final_configuration));
        }

        let count = u64::cast_from(live_dataflows.len());
        command_counts.create_dataflow.borrow().set(count);
        dataflow_count.borrow().set(count);
        for dataflow in live_dataflows {
            self.commands.push(ComputeCommand::CreateDataflow(dataflow));
        }

        let count = u64::cast_from(live_peeks.len());
        command_counts.peek.borrow().set(count);
        for peek in live_peeks.into_values() {
            self.commands.push(ComputeCommand::Peek(peek));
        }

        command_counts.cancel_peek.borrow().set(0);

        // Allow compaction only after emmitting peek commands.
        let count = u64::cast_from(final_frontiers.len());
        command_counts.allow_compaction.borrow().set(count);
        for (id, frontier) in final_frontiers {
            self.commands
                .push(ComputeCommand::AllowCompaction { id, frontier });
        }

        let count = u64::from(initialization_complete);
        command_counts.initialization_complete.borrow().set(count);
        if initialization_complete {
            self.commands.push(ComputeCommand::InitializationComplete);
        }

        self.reduced_count = self.commands.len();
    }

    /// Discard all peek commands.
    pub fn discard_peeks(&mut self) {
        self.commands.retain(|command| {
            use ComputeCommand::*;
            let is_peek = matches!(command, Peek(_) | CancelPeek { .. });
            if is_peek {
                self.metrics
                    .command_counts
                    .for_command(command)
                    .borrow()
                    .dec();
            }
            !is_peek
        });
    }

    /// Iterate through the contained commands.
    pub fn iter(&self) -> impl Iterator<Item = &ComputeCommand<T>> {
        self.commands.iter()
    }
}
