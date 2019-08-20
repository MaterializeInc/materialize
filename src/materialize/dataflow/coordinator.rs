// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Coordination of installed views, available timestamps, and compacted timestamps.
//!
//! The command coordinator maintains a view of the installed views, and for each tracks
//! the frontier of available times (`upper`) and the frontier of compacted times (`since`).
//! The upper frontier describes times that may not return immediately, as any timestamps in
//! advance of the frontier are still open. The since frontier constrains those times for
//! which the maintained view will be correct, as any timestamps in advance of the frontier
//! must accumulate to the same value as would an un-compacted trace.

// Clone on copy permitted for timestamps, which happen to be Copy at the moment, but which
// may become non-copy in the future.
#![allow(clippy::clone_on_copy)]

use timely::progress::frontier::Antichain;
use timely::synchronization::sequence::Sequencer;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::dataflow::logging;
use crate::dataflow::logging::materialized::MaterializedEvent;
use crate::dataflow::{Dataflow, Timestamp, View};
use crate::glue::*;
use expr::RelationExpr;
use repr::RelationType;

/// Explicit instructions for timely dataflow workers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SequencedCommand {
    /// Create a sequence of dataflows.
    CreateDataflows(Vec<Dataflow>),
    /// Drop the dataflows bound to these names.
    DropDataflows(Vec<String>),
    /// Peek at a materialized view.
    Peek {
        name: String,
        timestamp: Timestamp,
        drop_after_peek: bool,
    },
    /// Enable compaction in views.
    ///
    /// Each entry in the vector names a view and provides a frontier after which
    /// accumulations must be correct. The workers gain the liberty of compacting
    /// the corresponding maintained traces up through that frontier.
    AllowCompaction(Vec<(String, Vec<Timestamp>)>),
    /// Currently unimplemented.
    Tail { typ: RelationType, name: String },
    /// Disconnect inputs, drain dataflows, and shut down timely workers.
    Shutdown,
}

/// State necessary to sequence commands and populate peek timestamps.
pub struct CommandCoordinator {
    /// Per-view maintained state.
    views: HashMap<String, ViewState>,
    command_receiver: UnboundedReceiver<(DataflowCommand, CommandMeta)>,
    since_updates: Vec<(String, Vec<Timestamp>)>,
    pub logger: Option<logging::materialized::Logger>,
}

impl CommandCoordinator {
    /// Creates a new command coordinator from input and output command queues.
    pub fn new(command_receiver: UnboundedReceiver<(DataflowCommand, CommandMeta)>) -> Self {
        Self {
            views: HashMap::new(),
            command_receiver,
            logger: None,
            since_updates: Vec::new(),
        }
    }

    /// Drains commands from the receiver and sequences them to the sequencer.
    pub fn sequence_commands(
        &mut self,
        sequencer: &mut Sequencer<(SequencedCommand, CommandMeta)>,
    ) {
        while let Ok(Some((cmd, cmd_meta))) = self.command_receiver.try_next() {
            self.sequence_command(cmd, cmd_meta, sequencer);
        }
    }

    /// Appends a sequence of commands to `sequenced` in response to a dataflow command.
    pub fn sequence_command(
        &mut self,
        command: DataflowCommand,
        command_meta: CommandMeta,
        sequencer: &mut Sequencer<(SequencedCommand, CommandMeta)>,
    ) {
        match command {
            DataflowCommand::CreateDataflows(dataflows) => {
                for dataflow in dataflows.iter() {
                    self.insert_view(dataflow);
                }
                sequencer.push((SequencedCommand::CreateDataflows(dataflows), command_meta));
            }
            DataflowCommand::DropDataflows(dataflows) => {
                for name in dataflows.iter() {
                    self.remove_view(name);
                }
                sequencer.push((SequencedCommand::DropDataflows(dataflows), command_meta));
            }
            DataflowCommand::Peek { source, when } => {
                // Peeks describe a source of data and a timestamp at which to view its contents.
                //
                // We need to determine both an appropriate timestamp from the description, and
                // also to ensure that there is a view in place to query, if the source of data
                // for the peek is not a base relation.

                // Choose a timestamp for all workers to use in the peek.
                // We minimize over all participating views, to ensure that the query will not
                // need to block on the arrival of further input data.
                let timestamp = self.determine_timestamp(&source, when);

                // Create a transient view if the peek is not of a base relation.
                let (name, drop) = if let RelationExpr::Get { name, typ: _ } = source {
                    // Fast path. We can just look at the existing dataflow directly.
                    (name, false)
                } else {
                    // Slow path. We need to perform some computation, so build
                    // a new transient dataflow that will be dropped after the
                    // peek completes.
                    let name = format!("<temp_{}>", Uuid::new_v4());
                    let typ = source.typ();
                    let create_command =
                        SequencedCommand::CreateDataflows(vec![Dataflow::View(View {
                            name: name.clone(),
                            relation_expr: source,
                            typ,
                        })]);
                    sequencer.push((create_command, CommandMeta::nil()));
                    (name, true)
                };

                let peek_command = SequencedCommand::Peek {
                    name,
                    timestamp,
                    drop_after_peek: drop,
                };
                sequencer.push((peek_command, command_meta));
            }
            DataflowCommand::Tail { typ, name } => {
                sequencer.push((SequencedCommand::Tail { typ, name }, command_meta));
            }
            DataflowCommand::Shutdown => {
                sequencer.push((SequencedCommand::Shutdown, command_meta));
            }
        };
    }

    /// Perform maintenance work associated with the coordinator.
    ///
    /// Primarily, this involves sequencing compaction commands, which should be issued whenever
    /// available, but the sequencer is otherwise only provided in response to received commands.
    ///
    /// This API is a bit wonky, but at the moment the sequencer plays the role of both send and
    /// receive, and so cannot be owned only by the coordinator.
    pub fn maintenance(&mut self, sequencer: &mut Sequencer<(SequencedCommand, CommandMeta)>) {
        // Take this opportunity to drain `since_update` commands.
        sequencer.push((
            SequencedCommand::AllowCompaction(std::mem::replace(
                &mut self.since_updates,
                Vec::new(),
            )),
            CommandMeta::nil(),
        ));
    }

    /// A policy for determining the timestamp for a peek.
    fn determine_timestamp(&mut self, source: &RelationExpr, when: PeekWhen) -> Timestamp {
        match when {
            // Explicitly requested timestamps should be respected.
            PeekWhen::AtTimestamp(timestamp) => timestamp,

            // We should produce the minimum accepted time among inputs sources that
            // `source` depends on transitively, ignoring the accepted times of
            // intermediate views.
            PeekWhen::EarliestSource | PeekWhen::Immediately => {
                let mut bound = Antichain::new(); // lower bound on available data.

                // TODO : RelationExpr has a `uses_inner` method, but it wasn't
                // clear what it does (it suppresses let bound names, for example).
                // Dataflow not yet installed, so we should visit the RelationExpr
                // manually and then call `self.sources_frontier`
                source.visit(&mut |e| {
                    if let RelationExpr::Get { name, typ: _ } = e {
                        match when {
                            PeekWhen::EarliestSource => {
                                self.sources_frontier(name, &mut bound);
                            }
                            PeekWhen::Immediately => {
                                if let Some(upper) = self.upper_of(name) {
                                    bound.extend(upper.elements().iter().cloned());
                                } else {
                                    eprintln!("Alarming! Absent relation in view");
                                    bound.insert(0);
                                }
                            }
                            _ => unreachable!(),
                        };
                    }
                });

                // Pick the first time strictly less than `bound` to ensure that the
                // peek can respond without further input advances.
                // TODO : the subtraction saturates to not wrap zero around, but if
                // we get this far with a zero we are at risk of a peek that may not
                // immediately return.
                if let Some(bound) = bound.elements().get(0) {
                    bound.saturating_sub(1)
                } else {
                    Timestamp::max_value()
                }
            }
        }
    }

    /// Introduces all frontier elements from sources (not views) into `bound`.
    ///
    /// This method transitively traverses view definitions until it finds sources, and incorporates
    /// the accepted frontiers of each source into `bound`.
    fn sources_frontier(&self, name: &str, bound: &mut Antichain<Timestamp>) {
        if let Some(uses) = &self.views[name].uses {
            for name in uses {
                self.sources_frontier(name, bound);
            }
        } else {
            let upper = self.upper_of(name).expect("Name missing at coordinator");
            bound.extend(upper.elements().iter().cloned());
        }
    }

    /// Updates the upper frontier of a named view.
    pub fn update_upper(&mut self, name: &str, upper: &[Timestamp]) {
        if let Some(entry) = self.views.get_mut(name) {
            // We may be informed of non-changes; suppress them.
            if entry.upper.elements() != upper {
                // Log the change to frontiers.
                if let Some(logger) = self.logger.as_mut() {
                    for time in entry.upper.elements().iter() {
                        logger.log(MaterializedEvent::Frontier(
                            name.to_string(),
                            time.clone(),
                            -1,
                        ));
                    }
                    for time in upper.iter() {
                        logger.log(MaterializedEvent::Frontier(
                            name.to_string(),
                            time.clone(),
                            1,
                        ));
                    }
                }

                entry.upper.clear();
                entry.upper.extend(upper.iter().cloned());

                let mut since = Antichain::new();
                for time in entry.upper.elements() {
                    since.insert(time.saturating_sub(entry.compaction_latency_ms));
                }
                self.since_updates
                    .push((name.to_string(), since.elements().to_vec()));
            }
        }
    }

    /// The upper frontier of a maintained view, if it exists.
    pub fn upper_of(&self, name: &str) -> Option<&Antichain<Timestamp>> {
        self.views.get(name).map(|v| &v.upper)
    }

    /// Updates the since frontier of a named view.
    ///
    /// This frontier tracks compaction frontier, and represents a lower bound on times for
    /// which the associated trace is certain to produce valid results. For times greater
    /// or equal to some element of the since frontier the accumulation will be correct,
    /// and for other times no such guarantee holds.
    pub fn update_since(&mut self, name: &str, since: &[Timestamp]) {
        if let Some(entry) = self.views.get_mut(name) {
            entry.since.clear();
            entry.since.extend(since.iter().cloned());
        }
    }

    /// The since frontier of a maintained view, if it exists.
    pub fn since_of(&self, name: &str) -> Option<&Antichain<Timestamp>> {
        self.views.get(name).map(|v| &v.since)
    }

    /// Inserts a view into the coordinator.
    ///
    /// Initializes managed state and logs the insertion (and removal of any existing view).
    pub fn insert_view(&mut self, dataflow: &Dataflow) {
        self.remove_view(dataflow.name());
        let viewstate = ViewState::new(dataflow);
        if let Some(logger) = self.logger.as_mut() {
            for time in viewstate.upper.elements() {
                logger.log(MaterializedEvent::Frontier(
                    dataflow.name().to_string(),
                    time.clone(),
                    1,
                ));
            }
        }
        self.views.insert(dataflow.name().to_string(), viewstate);
    }

    /// Inserts a source into the coordinator.
    ///
    /// Unlike `insert_view`, this method can be called without a dataflow argument.
    /// This is most commonly used for internal sources such as logging.
    pub fn insert_source(&mut self, name: &str, compaction_ms: Timestamp) {
        self.remove_view(name);
        let mut viewstate = ViewState::new_source();
        if let Some(logger) = self.logger.as_mut() {
            for time in viewstate.upper.elements() {
                logger.log(MaterializedEvent::Frontier(
                    name.to_string(),
                    time.clone(),
                    1,
                ));
            }
        }
        viewstate.set_compaction_latency(compaction_ms);
        self.views.insert(name.to_string(), viewstate);
    }

    /// Removes a view from the coordinator.
    ///
    /// Removes the managed state and logs the removal.
    pub fn remove_view(&mut self, name: &str) {
        if let Some(state) = self.views.remove(name) {
            if let Some(logger) = &mut self.logger {
                for time in state.upper.elements() {
                    logger.log(MaterializedEvent::Frontier(
                        name.to_string(),
                        time.clone(),
                        -1,
                    ));
                }
            }
        }
    }
}

/// Per-view state.
pub struct ViewState {
    /// Names of views on which this view depends, or `None` if a source.
    uses: Option<Vec<String>>,
    /// The most recent frontier for new data.
    /// All further changes will be in advance of this bound.
    upper: Antichain<Timestamp>,
    /// The compaction frontier.
    /// All peeks in advance of this frontier will be correct,
    /// but peeks not in advance of this frontier may not be.
    since: Antichain<Timestamp>,
    /// Compaction delay.
    ///
    /// This timestamp drives the advancement of the since frontier as a
    /// function of the upper frontier, trailing it by exactly this much.
    compaction_latency_ms: Timestamp,
}

impl ViewState {
    /// Initialize a new `ViewState` from a name and a dataflow.
    ///
    /// The upper bound and since compaction are initialized to the zero frontier.
    pub fn new(dataflow: &Dataflow) -> Self {
        // determine immediate dependencies.
        let uses = match dataflow {
            Dataflow::Source(_) => None,
            Dataflow::Sink(_) => None,
            v @ Dataflow::View(_) => Some(v.uses().iter().map(|x| x.to_string()).collect()),
        };

        ViewState {
            uses,
            upper: Antichain::from_elem(0),
            since: Antichain::from_elem(0),
            compaction_latency_ms: 60_000,
        }
    }

    /// Creates the state for a source, with no depedencies.
    pub fn new_source() -> Self {
        ViewState {
            uses: None,
            upper: Antichain::from_elem(0),
            since: Antichain::from_elem(0),
            compaction_latency_ms: 60_000,
        }
    }

    /// Sets the latency behind the collection frontier at which compaction occurs.
    pub fn set_compaction_latency(&mut self, latency_ms: Timestamp) {
        self.compaction_latency_ms = latency_ms;
    }
}
