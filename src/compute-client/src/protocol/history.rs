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
use std::collections::{BTreeMap, BTreeSet};

use mz_ore::cast::CastFrom;
use mz_ore::metrics::UIntGauge;
use timely::progress::Antichain;

use crate::metrics::HistoryMetrics;
use crate::protocol::command::{ComputeCommand, ComputeParameters, Peek};

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
    /// This action will reduce the history every time it doubles while retaining the
    /// provided peeks.
    pub fn push<V>(&mut self, command: ComputeCommand<T>, peeks: &BTreeMap<uuid::Uuid, V>) {
        self.push_inner(command);
        if self.commands.len() > 2 * self.reduced_count {
            self.retain_peeks(peeks);
            self.reduce();
        }
    }

    /// Add a command to the history.
    fn push_inner(&mut self, command: ComputeCommand<T>) {
        self.metrics
            .command_counts
            .for_command(&command)
            .borrow()
            .inc();
        if let ComputeCommand::CreateDataflows(dataflows) = &command {
            self.metrics
                .dataflow_count
                .borrow()
                .add(u64::cast_from(dataflows.len()));
        }

        self.commands.push(command);
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
        let mut live_peeks = Vec::new();
        let mut live_cancels = BTreeSet::new();

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
                ComputeCommand::CreateDataflows(dataflows) => {
                    live_dataflows.extend(dataflows);
                }
                ComputeCommand::AllowCompaction(frontiers) => {
                    for (id, frontier) in frontiers {
                        final_frontiers.insert(id, frontier.clone());
                    }
                }
                ComputeCommand::Peek(peek) => {
                    live_peeks.push(peek);
                }
                ComputeCommand::CancelPeeks { uuids } => {
                    live_cancels.extend(uuids);
                }
            }
        }

        // Determine the required antichains to support live peeks;
        let mut live_peek_frontiers = std::collections::BTreeMap::new();
        for Peek { id, timestamp, .. } in live_peeks.iter() {
            // Introduce `time` as a constraint on the `as_of` frontier of `id`.
            live_peek_frontiers
                .entry(id)
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

        // Record the volume of post-compaction commands.
        let mut command_count = 1;
        command_count += live_dataflows.len();
        command_count += final_frontiers.len();
        command_count += live_peeks.len();
        command_count += live_cancels.len();
        if !final_configuration.all_unset() {
            command_count += 1;
        }

        self.metrics.reset();

        // Reconstitute the commands as a compact history.
        if let Some(create_timely_command) = create_timely_command {
            self.push_inner(create_timely_command);
        }
        if let Some(create_inst_command) = create_inst_command {
            self.push_inner(create_inst_command);
        }
        if !final_configuration.all_unset() {
            self.push_inner(ComputeCommand::UpdateConfiguration(final_configuration));
        }
        if !live_dataflows.is_empty() {
            self.push_inner(ComputeCommand::CreateDataflows(live_dataflows));
        }
        for peek in live_peeks {
            self.push_inner(ComputeCommand::Peek(peek));
        }
        if !live_cancels.is_empty() {
            self.push_inner(ComputeCommand::CancelPeeks {
                uuids: live_cancels,
            });
        }
        // Allow compaction only after emmitting peek commands.
        if !final_frontiers.is_empty() {
            self.push_inner(ComputeCommand::AllowCompaction(
                final_frontiers.into_iter().collect(),
            ));
        }
        if initialization_complete {
            self.push_inner(ComputeCommand::InitializationComplete);
        }

        self.reduced_count = command_count;
    }

    /// Retain only those peeks present in `peeks` and discard the rest.
    pub fn retain_peeks<V>(&mut self, peeks: &BTreeMap<uuid::Uuid, V>) {
        for command in self.commands.iter_mut() {
            if let ComputeCommand::CancelPeeks { uuids } = command {
                uuids.retain(|uuid| peeks.contains_key(uuid));
            }
        }
        self.commands.retain(|command| {
            let retain = match command {
                ComputeCommand::Peek(peek) => peeks.contains_key(&peek.uuid),
                ComputeCommand::CancelPeeks { uuids } => !uuids.is_empty(),
                _ => true,
            };
            if !retain {
                self.metrics
                    .command_counts
                    .for_command(command)
                    .borrow()
                    .dec();
            }
            retain
        });
    }

    /// Iterate through the contained commands.
    pub fn iter(&self) -> impl Iterator<Item = &ComputeCommand<T>> {
        self.commands.iter()
    }
}
