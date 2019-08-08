// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use timely::progress::frontier::Antichain;
use timely::synchronization::sequence::Sequencer;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::RelationExpr;
use crate::dataflow::logging;
use crate::dataflow::{Dataflow, Timestamp, View};
use crate::glue::*;

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SequencedCommand {
    CreateDataflows(Vec<Dataflow>),
    DropDataflows(Vec<String>),
    Peek {
        name: String,
        timestamp: Timestamp,
        drop_after_peek: bool,
    },
    Tail(String),
    Shutdown,
}

/// State necessary to sequence commands and populate peek timestamps.
pub struct CommandCoordinator {
    /// Per-view maintained state.
    views: HashMap<String, ViewState>,
    command_receiver: UnboundedReceiver<(DataflowCommand, CommandMeta)>,
}

impl CommandCoordinator {
    /// Creates a new command coordinator from input and output command queues.
    pub fn new(
        command_receiver: UnboundedReceiver<(DataflowCommand, CommandMeta)>,
        logging_config: &Option<logging::LoggingConfiguration>,
    ) -> Self {
        let mut views = HashMap::new();
        if let Some(config) = &logging_config {
            for log in config.active_logs.iter() {
                views.insert(log.name().to_string(), ViewState::new_source());
            }
        }

        Self {
            views,
            command_receiver,
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
                    self.views
                        .insert(dataflow.name().to_owned(), ViewState::new(dataflow));
                }
                sequencer.push((SequencedCommand::CreateDataflows(dataflows), command_meta));
            }
            DataflowCommand::DropDataflows(dataflows) => {
                for dataflow in dataflows.iter() {
                    self.views.remove(dataflow);
                }
                sequencer.push((SequencedCommand::DropDataflows(dataflows), command_meta));
            }
            DataflowCommand::Tail(name) => {
                sequencer.push((SequencedCommand::Tail(name), command_meta));
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
                let timestamp = match when {
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
                };

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
            DataflowCommand::Shutdown => {
                sequencer.push((SequencedCommand::Shutdown, command_meta));
            }
        };
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
            entry.upper.clear();
            entry.upper.extend(upper.iter().cloned());
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
        }
    }

    /// Creates the state for a source, with no depedencies.
    pub fn new_source() -> Self {
        ViewState {
            uses: None,
            upper: Antichain::from_elem(0),
            since: Antichain::from_elem(0),
        }
    }
}
