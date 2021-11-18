// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Traits and types for fat clients of the dataflow subsystem.

// This appears to be defective at the moment, with false positiives
// for each variant of the `Command` enum, each of which are documented.
// #![warn(missing_docs)]

use async_trait::async_trait;
use enum_iterator::IntoEnumIterator;
use enum_kinds::EnumKind;
use num_enum::IntoPrimitive;
use serde::{Deserialize, Serialize};
use timely::progress::frontier::Antichain;
use timely::progress::ChangeBatch;

use crate::logging::LoggingConfig;
use crate::{
    DataflowDescription, MzOffset, PeekResponse, SourceConnector, TailResponse,
    TimestampSourceUpdate, Update,
};
use expr::{GlobalId, PartitionId, RowSetFinishing};
use persist::indexed::runtime::RuntimeClient;
use repr::{Row, Timestamp};

/// Explicit instructions for timely dataflow workers.
#[derive(Clone, Debug, Serialize, Deserialize, EnumKind)]
#[enum_kind(
    CommandKind,
    derive(Serialize, IntoPrimitive, IntoEnumIterator),
    repr(usize),
    serde(rename_all = "snake_case"),
    doc = "The kind of command that was received"
)]
pub enum Command {
    /// Create a sequence of dataflows.
    ///
    /// Each of the dataflows must contain `as_of` members that are valid
    /// for each of the referenced arrangements, meaning `AllowCompaction`
    /// should be held back to those values until the command.
    /// Subsequent commands may arbitrarily compact the arrangements;
    /// the dataflow runners are responsible for ensuring that they can
    /// correctly maintain the dataflows.
    CreateDataflows(Vec<DataflowDescription<crate::plan::Plan>>),
    /// Drop the sources bound to these names.
    DropSources(Vec<GlobalId>),
    /// Drop the sinks bound to these names.
    DropSinks(Vec<GlobalId>),
    /// Drop the indexes bound to these namees.
    DropIndexes(Vec<GlobalId>),
    /// Peek at an arrangement.
    ///
    /// This request elicits data from the worker, by naming an
    /// arrangement and some actions to apply to the results before
    /// returning them.
    ///
    /// The `timestamp` member must be valid for the arrangement that
    /// is referenced by `id`. This means that `AllowCompaction` for
    /// this arrangement should not pass `timestamp` before this command.
    /// Subsequent commands may arbitrarily compact the arrangements;
    /// the dataflow runners are responsible for ensuring that they can
    /// correctly answer the `Peek`.
    Peek {
        /// The identifier of the arrangement.
        id: GlobalId,
        /// An optional key that should be used for the arrangement.
        key: Option<Row>,
        /// The identifier of this peek request.
        ///
        /// Used in responses and cancelation requests.
        conn_id: u32,
        /// The logical timestamp at which the arrangement is queried.
        timestamp: Timestamp,
        /// Actions to apply to the result set before returning them.
        finishing: RowSetFinishing,
        /// Linear operation to apply in-line on each result.
        map_filter_project: expr::SafeMfpPlan,
    },
    /// Cancel the peek associated with the given `conn_id`.
    CancelPeek {
        /// The identifier of the peek request to cancel.
        conn_id: u32,
    },
    /// Insert `updates` into the local input named `id`.
    Insert {
        /// Identifier of the local input.
        id: GlobalId,
        /// A list of updates to be introduced to the input.
        updates: Vec<Update>,
    },
    /// Enable compaction in views.
    ///
    /// Each entry in the vector names a view and provides a frontier after which
    /// accumulations must be correct. The workers gain the liberty of compacting
    /// the corresponding maintained traces up through that frontier.
    AllowCompaction(Vec<(GlobalId, Antichain<Timestamp>)>),
    /// Update durability information for sources.
    ///
    /// Each entry names a source and provides a frontier before which the source can
    /// be exactly replayed across restarts (i.e. we can assign the same timestamps to
    /// all the same data)
    DurabilityFrontierUpdates(Vec<(GlobalId, Antichain<Timestamp>)>),
    /// Add a new source to be aware of for timestamping.
    AddSourceTimestamping {
        /// The ID of the timestamped source
        id: GlobalId,
        /// The connector for the timestamped source.
        connector: SourceConnector,
        /// Previously stored timestamp bindings.
        bindings: Vec<(PartitionId, Timestamp, MzOffset)>,
    },
    /// Advance worker timestamp
    AdvanceSourceTimestamp {
        /// The ID of the timestamped source
        id: GlobalId,
        /// The associated update (RT or BYO)
        update: TimestampSourceUpdate,
    },
    /// Drop all timestamping info for a source
    DropSourceTimestamping {
        /// The ID id of the formerly timestamped source.
        id: GlobalId,
    },
    /// Advance all local inputs to the given timestamp.
    AdvanceAllLocalInputs {
        /// The timestamp to advance to.
        advance_to: Timestamp,
    },
    /// Request that the logging sources in the contained configuration are
    /// installed.
    EnableLogging(LoggingConfig),
    /// Enable persistence.
    // TODO: to enable persistence in clustered mode, we'll need to figure out
    // an alternative design that doesn't require serializing a persistence
    // client.
    #[serde(skip)]
    EnablePersistence(RuntimeClient),
}

impl Command {
    /// Partitions the command into `parts` many disjoint pieces.
    ///
    /// This is used to subdivide commands that can be sharded across workers,
    /// for example the `plan::Constant` stages of dataflow plans, and the
    /// `Command::Insert` commands that may contain multiple updates.
    pub fn partition_among(self, parts: usize) -> Vec<Self> {
        if parts == 0 {
            Vec::new()
        } else if parts == 1 {
            vec![self]
        } else {
            match self {
                Command::CreateDataflows(dataflows) => {
                    let mut dataflows_parts = vec![Vec::new(); parts];

                    for dataflow in dataflows {
                        // A list of descriptions of objects for each part to build.
                        let mut builds_parts = vec![Vec::new(); parts];
                        // Partition each build description among `parts`.
                        for build_desc in dataflow.objects_to_build {
                            let build_part = build_desc.view.partition_among(parts);
                            for (view, objects_to_build) in
                                build_part.into_iter().zip(builds_parts.iter_mut())
                            {
                                objects_to_build.push(crate::BuildDesc {
                                    id: build_desc.id,
                                    view,
                                });
                            }
                        }
                        // Each list of build descriptions results in a dataflow description.
                        for (dataflows_part, objects_to_build) in
                            dataflows_parts.iter_mut().zip(builds_parts)
                        {
                            dataflows_part.push(DataflowDescription {
                                source_imports: dataflow.source_imports.clone(),
                                index_imports: dataflow.index_imports.clone(),
                                objects_to_build,
                                index_exports: dataflow.index_exports.clone(),
                                sink_exports: dataflow.sink_exports.clone(),
                                dependent_objects: dataflow.dependent_objects.clone(),
                                as_of: dataflow.as_of.clone(),
                                debug_name: dataflow.debug_name.clone(),
                            });
                        }
                    }
                    dataflows_parts
                        .into_iter()
                        .map(|dataflows| Command::CreateDataflows(dataflows))
                        .collect()
                }
                Command::Insert { id, updates } => {
                    let mut updates_parts = vec![Vec::new(); parts];
                    for (index, update) in updates.into_iter().enumerate() {
                        updates_parts[index % parts].push(update);
                    }
                    updates_parts
                        .into_iter()
                        .map(|updates| Command::Insert { id, updates })
                        .collect()
                }
                command => vec![command; parts],
            }
        }
    }
}

impl CommandKind {
    /// Returns the name of the command kind.
    pub fn name(self) -> &'static str {
        match self {
            CommandKind::AddSourceTimestamping => "add_source_timestamping",
            CommandKind::AdvanceAllLocalInputs => "advance_all_local_inputs",
            CommandKind::AdvanceSourceTimestamp => "advance_source_timestamp",
            CommandKind::AllowCompaction => "allow_compaction",
            CommandKind::CancelPeek => "cancel_peek",
            CommandKind::CreateDataflows => "create_dataflows",
            CommandKind::DropIndexes => "drop_indexes",
            CommandKind::DropSinks => "drop_sinks",
            CommandKind::DropSourceTimestamping => "drop_source_timestamping",
            CommandKind::DropSources => "drop_sources",
            CommandKind::DurabilityFrontierUpdates => "durability_frontier_updates",
            CommandKind::EnableLogging => "enable_logging",
            CommandKind::EnablePersistence => "enable_persistence",
            CommandKind::Insert => "insert",
            CommandKind::Peek => "peek",
        }
    }
}

/// Information from timely dataflow workers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Response {
    /// Identifies the worker by its identifier.
    pub worker_id: usize,
    /// The feedback itself.
    pub message: WorkerFeedback,
}

/// Data about timestamp bindings that dataflow workers send to the coordinator
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimestampBindingFeedback {
    /// Durability frontier changes
    pub changes: Vec<(GlobalId, ChangeBatch<Timestamp>)>,
    /// Timestamp bindings for all of those frontier changes
    pub bindings: Vec<(GlobalId, PartitionId, Timestamp, MzOffset)>,
}

/// Responses the worker can provide back to the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WorkerFeedback {
    /// A list of identifiers of traces, with prior and new upper frontiers.
    FrontierUppers(Vec<(GlobalId, ChangeBatch<Timestamp>)>),
    /// Timestamp bindings and prior and new frontiers for those bindings for all
    /// sources
    TimestampBindings(TimestampBindingFeedback),
    /// The worker's response to a specified (by connection id) peek.
    PeekResponse(u32, PeekResponse),
    /// The worker's next response to a specified tail.
    TailResponse(GlobalId, TailResponse),
}

/// A client to a running dataflow server.
#[async_trait]
pub trait Client: Send {
    /// Reports the number of dataflow workers.
    fn num_workers(&self) -> usize;

    /// Sends a command to the dataflow server.
    async fn send(&mut self, cmd: Command);

    /// Receives the next response from the dataflow server.
    ///
    /// This method blocks until the next response is available, or, if the
    /// dataflow server has been shut down, returns `None`.
    async fn recv(&mut self) -> Option<Response>;
}

/// A client to a dataflow server running in the current process.
pub struct LocalClient {
    feedback_rx: tokio::sync::mpsc::UnboundedReceiver<Response>,
    worker_txs: Vec<crossbeam_channel::Sender<Command>>,
    worker_threads: Vec<std::thread::Thread>,
}

#[async_trait]
impl Client for LocalClient {
    fn num_workers(&self) -> usize {
        self.worker_txs.len()
    }

    async fn send(&mut self, cmd: Command) {
        log::trace!("Broadcasting dataflow command: {:?}", cmd);
        let cmd_parts = cmd.partition_among(self.num_workers());
        for (tx, cmd_part) in self.worker_txs.iter().zip(cmd_parts) {
            tx.send(cmd_part)
                .expect("worker command receiver should not drop first")
        }
        for thread in &self.worker_threads {
            thread.unpark()
        }
    }

    async fn recv(&mut self) -> Option<Response> {
        return self.feedback_rx.recv().await;
    }
}

impl LocalClient {
    /// Create a new instance of [LocalClient] from its parts.
    pub fn new(
        feedback_rx: tokio::sync::mpsc::UnboundedReceiver<Response>,
        worker_txs: Vec<crossbeam_channel::Sender<Command>>,
        worker_threads: Vec<std::thread::Thread>,
    ) -> Self {
        Self {
            feedback_rx,
            worker_txs,
            worker_threads,
        }
    }
}

// We implement `Drop` so that we can wake each of the workers and have them notice the drop.
impl Drop for LocalClient {
    fn drop(&mut self) {
        self.worker_txs.clear();
        for thread in &self.worker_threads {
            thread.unpark()
        }
    }
}
