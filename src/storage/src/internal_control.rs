// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Types for cluster-internal control messages that can be broadcast to all
//! workers from individual operators/workers.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::mpsc;

use mz_repr::{GlobalId, Row};
use mz_rocksdb::config::SharedWriteBufferManager;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::oneshot_sources::OneshotIngestionRequest;
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::sinks::StorageSinkDesc;
use mz_storage_types::sources::IngestionDescription;
use serde::{Deserialize, Serialize};
use timely::communication::Allocate;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::{source, OutputHandle};
use timely::dataflow::operators::{Broadcast, Operator};
use timely::progress::Antichain;
use timely::scheduling::{Activator, Scheduler};
use timely::worker::Worker as TimelyWorker;

use crate::statistics::{SinkStatisticsRecord, SourceStatisticsRecord};

/// _Dynamic_ storage instance configuration parameters that are used during dataflow rendering.
/// Changes to these parameters are applied to `StorageWorker`s in a consistent order
/// with source and sink creation.
#[derive(Debug)]
pub struct DataflowParameters {
    /// Configuration/tuning for RocksDB. This also contains
    /// some shared objects, which is why its separate.
    pub upsert_rocksdb_tuning_config: mz_rocksdb::RocksDBConfig,
}

impl DataflowParameters {
    /// Creates a new instance of `DataflowParameters` with given shared rocksdb write buffer manager
    /// and the cluster memory limit
    pub fn new(
        shared_rocksdb_write_buffer_manager: SharedWriteBufferManager,
        cluster_memory_limit: Option<usize>,
    ) -> Self {
        Self {
            upsert_rocksdb_tuning_config: mz_rocksdb::RocksDBConfig::new(
                shared_rocksdb_write_buffer_manager,
                cluster_memory_limit,
            ),
        }
    }
    /// Update the `DataflowParameters` with new configuration.
    pub fn update(&mut self, storage_parameters: StorageParameters) {
        self.upsert_rocksdb_tuning_config
            .apply(storage_parameters.upsert_rocksdb_tuning_config.clone());
    }
}

/// Internal commands that can be sent by individual operators/workers that will
/// be broadcast to all workers. The worker main loop will receive those and act
/// on them.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum InternalStorageCommand {
    /// Suspend and restart the dataflow identified by the `GlobalId`.
    SuspendAndRestart {
        /// The id of the dataflow that should be restarted.
        id: GlobalId,
        /// The reason for the restart request.
        reason: String,
    },
    /// Render an ingestion dataflow at the given resumption frontier.
    CreateIngestionDataflow {
        /// ID of the ingestion/sourve.
        id: GlobalId,
        /// The description of the ingestion/source.
        ingestion_description: IngestionDescription<CollectionMetadata>,
        /// The frontier beyond which ingested updates should be uncompacted. Inputs to the
        /// ingestion are guaranteed to be readable at this frontier.
        as_of: Antichain<mz_repr::Timestamp>,
        /// A frontier in the Materialize time domain with the property that all updates not beyond
        /// it have already been durably ingested.
        resume_uppers: BTreeMap<GlobalId, Antichain<mz_repr::Timestamp>>,
        /// A frontier in the source time domain with the property that all updates not beyond it
        /// have already been durably ingested.
        source_resume_uppers: BTreeMap<GlobalId, Vec<Row>>,
    },
    /// Render a oneshot ingestion dataflow that fetches data from an external system and stages
    /// batches in Persist, that can later be appended to the shard.
    RunOneshotIngestion {
        /// ID of the running dataflow that is doing the ingestion.
        ingestion_id: uuid::Uuid,
        /// ID of the collection we'll create batches for.
        collection_id: GlobalId,
        /// Metadata of the collection we'll create batches for.
        collection_meta: CollectionMetadata,
        /// Description of the oneshot ingestion.
        request: OneshotIngestionRequest,
    },
    /// Render a sink dataflow.
    RunSinkDataflow(
        GlobalId,
        StorageSinkDesc<CollectionMetadata, mz_repr::Timestamp>,
    ),
    /// Drop all state and operators for a dataflow. This is a vec because some
    /// dataflows have their state spread over multiple IDs (i.e. sources that
    /// spawn subsources); this means that actions taken in response to this
    /// command should be permissive about missing state.
    DropDataflow(Vec<GlobalId>),

    /// Update the configuration for rendering dataflows.
    UpdateConfiguration {
        /// The new configuration parameters.
        storage_parameters: StorageParameters,
    },
    /// For moving statistics updates to worker 0.
    StatisticsUpdate {
        /// Local statistics, with their epochs.
        sources: Vec<(usize, SourceStatisticsRecord)>,
        /// Local statistics, with their epochs.
        sinks: Vec<(usize, SinkStatisticsRecord)>,
    },
}

/// A sender broadcasting [`InternalStorageCommand`]s to all workers.
#[derive(Clone)]
pub struct InternalCommandSender {
    tx: mpsc::Sender<InternalStorageCommand>,
    activator: Rc<RefCell<Option<Activator>>>,
}

impl InternalCommandSender {
    /// Broadcasts the given command to all workers.
    pub fn send(&self, cmd: InternalStorageCommand) {
        if self.tx.send(cmd).is_err() {
            panic!("internal command channel disconnected");
        }

        self.activator.borrow().as_ref().map(|a| a.activate());
    }
}

/// A receiver for [`InternalStorageCommand`]s broadcasted by workers.
pub struct InternalCommandReceiver {
    rx: mpsc::Receiver<InternalStorageCommand>,
}

impl InternalCommandReceiver {
    /// Returns the next available command, if any.
    ///
    /// This returns `None` when there are currently no commands but there might be commands again
    /// in the future.
    pub fn try_recv(&self) -> Option<InternalStorageCommand> {
        match self.rx.try_recv() {
            Ok(cmd) => Some(cmd),
            Err(mpsc::TryRecvError::Empty) => None,
            Err(mpsc::TryRecvError::Disconnected) => {
                panic!("internal command channel disconnected")
            }
        }
    }
}

pub(crate) fn setup_command_sequencer<'w, A: Allocate>(
    timely_worker: &'w mut TimelyWorker<A>,
) -> (InternalCommandSender, InternalCommandReceiver) {
    let (input_tx, input_rx) = mpsc::channel();
    let (output_tx, output_rx) = mpsc::channel();
    let activator = Rc::new(RefCell::new(None));

    timely_worker.dataflow_named::<(), _, _>("command_sequencer", {
        let activator = Rc::clone(&activator);
        move |scope| {
            // Create a stream of commands received from `input_rx`.
            //
            // The output commands are tagged by worker ID and command index, allowing downstream
            // operators to ensure their correct relative order.
            let stream = source(scope, "command_sequencer::source", |cap, info| {
                *activator.borrow_mut() = Some(scope.activator_for(info.address));

                let worker_id = scope.index();
                let mut cmd_index = 0;
                let mut capability = Some(cap);

                move |output: &mut OutputHandle<_, _, _>| {
                    let Some(cap) = &capability else {
                        return;
                    };

                    let mut session = output.session(cap);
                    loop {
                        match input_rx.try_recv() {
                            Ok(command) => {
                                let cmd = IndexedCommand {
                                    index: cmd_index,
                                    command,
                                };
                                session.give((worker_id, cmd));
                                cmd_index += 1;
                            }
                            Err(mpsc::TryRecvError::Empty) => break,
                            Err(mpsc::TryRecvError::Disconnected) => {
                                // Drop our capability to shut down.
                                capability = None;
                                break;
                            }
                        }
                    }
                }
            });

            // Sequence all commands through a single worker to establish a unique order.
            //
            // The output commands are tagged by a command index, allowing downstream operators to
            // ensure their correct relative order.
            let stream = stream.unary_frontier(
                Exchange::new(|_| 0),
                "command_sequencer::sequencer",
                |cap, _info| {
                    let mut cmd_index = 0;
                    let mut capability = Some(cap);

                    // For each worker, keep an ordered list of pending commands, as well as the
                    // current index of the next command.
                    let mut pending_commands = vec![(BTreeSet::new(), 0); scope.peers()];

                    move |input, output: &mut OutputHandle<_, _, _>| {
                        let Some(cap) = &capability else {
                            return;
                        };

                        while let Some((_cap, data)) = input.next() {
                            for (worker_id, cmd) in data.drain(..) {
                                pending_commands[worker_id].0.insert(cmd);
                            }
                        }

                        let mut session = output.session(cap);
                        for (commands, next_idx) in &mut pending_commands {
                            while commands.first().is_some_and(|c| c.index == *next_idx) {
                                let mut cmd = commands.pop_first().unwrap();
                                cmd.index = cmd_index;
                                session.give(cmd);

                                *next_idx += 1;
                                cmd_index += 1;
                            }
                        }

                        if input.frontier().is_empty() {
                            // Drop our capability to shut down.
                            capability = None;
                        }
                    }
                },
            );

            // Broadcast the ordered commands to all workers.
            let stream = stream.broadcast();

            // Sink the stream back into `output_tx`.
            stream.sink(Pipeline, "command_sequencer::sink", {
                // Keep an ordered list of pending commands, as well as the current index of the
                // next command.
                let mut pending_commands = BTreeSet::new();
                let mut next_idx = 0;

                move |input| {
                    while let Some((_cap, data)) = input.next() {
                        pending_commands.extend(data.drain(..));
                    }

                    while pending_commands
                        .first()
                        .is_some_and(|c| c.index == next_idx)
                    {
                        let cmd = pending_commands.pop_first().unwrap();
                        let _ = output_tx.send(cmd.command);
                        next_idx += 1;
                    }
                }
            });
        }
    });

    let tx = InternalCommandSender {
        tx: input_tx,
        activator,
    };
    let rx = InternalCommandReceiver { rx: output_rx };

    (tx, rx)
}

// An [`InternalStorageCommand`] tagged with an index.
//
// This is a `(u64, InternalStorageCommand)` in spirit, but implements `Ord` (which
// `InternalStorageCommand` doesn't) by looking only at the index.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct IndexedCommand {
    index: u64,
    command: InternalStorageCommand,
}

impl PartialEq for IndexedCommand {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for IndexedCommand {}

impl PartialOrd for IndexedCommand {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexedCommand {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.index.cmp(&other.index)
    }
}
