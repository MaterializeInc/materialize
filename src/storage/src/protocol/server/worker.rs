// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Worker-local state for storage timely instances.

use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

use crossbeam_channel::TryRecvError;
use mz_persist_client::cache::PersistClientCache;
use timely::communication::Allocate;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::progress::Timestamp as _;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::{mpsc, Mutex};

use mz_ore::now::NowFn;
use mz_repr::{GlobalId, Timestamp};

use crate::controller::CollectionMetadata;
use crate::protocol::client::{StorageCommand, StorageResponse};
use crate::sink::SinkBaseMetrics;
use crate::types::connections::ConnectionContext;
use crate::types::sinks::StorageSinkDesc;
use crate::types::sources::IngestionDescription;

use crate::decode::metrics::DecodeMetrics;
use crate::source::metrics::SourceBaseMetrics;

type CommandReceiver = crossbeam_channel::Receiver<StorageCommand>;
type ResponseSender = mpsc::UnboundedSender<StorageResponse>;

/// State maintained for each worker thread.
///
/// Much of this state can be viewed as local variables for the worker thread,
/// holding state that persists across function calls.
pub struct Worker<'w, A: Allocate> {
    /// The underlying Timely worker.
    pub timely_worker: &'w mut TimelyWorker<A>,
    /// The channel over which communication handles for newly connected clients
    /// are delivered.
    pub client_rx: crossbeam_channel::Receiver<(CommandReceiver, ResponseSender)>,
    /// The state associated with collection ingress and egress.
    pub storage_state: StorageState,
}

/// Worker-local state related to the ingress or egress of collections of data.
pub struct StorageState {
    /// The highest observed upper frontier for collection.
    ///
    /// This is shared among all source instances, so that they can jointly advance the
    /// frontier even as other instances are created and dropped. Ideally, the Storage
    /// module would eventually provide one source of truth on this rather than multiple,
    /// and we should aim for that but are not there yet.
    pub source_uppers: HashMap<GlobalId, Rc<RefCell<Antichain<mz_repr::Timestamp>>>>,
    /// Handles to created sources, keyed by ID
    pub source_tokens: HashMap<GlobalId, Rc<dyn Any>>,
    /// Decoding metrics reported by all dataflows.
    pub decode_metrics: DecodeMetrics,
    /// Tracks the conditional write frontiers we have reported.
    pub reported_frontiers: HashMap<GlobalId, Antichain<Timestamp>>,
    /// Descriptions of each installed ingestion.
    pub ingestions: HashMap<GlobalId, IngestionDescription<CollectionMetadata>>,
    /// Descriptions of each installed export.
    pub exports: HashMap<GlobalId, StorageSinkDesc<CollectionMetadata, mz_repr::Timestamp>>,
    /// Undocumented
    pub now: NowFn,
    /// Metrics for the source-specific side of dataflows.
    pub source_metrics: SourceBaseMetrics,
    /// Undocumented
    pub sink_metrics: SinkBaseMetrics,
    /// Index of the associated timely dataflow worker.
    pub timely_worker_index: usize,
    /// Peers in the associated timely dataflow worker.
    pub timely_worker_peers: usize,
    /// Configuration for source and sink connections.
    pub connection_context: ConnectionContext,
    /// A process-global cache of (blob_uri, consensus_uri) -> PersistClient.
    /// This is intentionally shared between workers
    pub persist_clients: Arc<Mutex<PersistClientCache>>,
    /// Tokens that should be dropped when a dataflow is dropped to clean up
    /// associated state.
    pub sink_tokens: HashMap<GlobalId, SinkToken>,
    /// Frontier of sink writes (all subsequent writes will be at times at or
    /// equal to this frontier)
    pub sink_write_frontiers: HashMap<GlobalId, Rc<RefCell<Antichain<Timestamp>>>>,
}

/// A token that keeps a sink alive.
pub struct SinkToken(Box<dyn Any>);
impl SinkToken {
    /// Create new token
    pub fn new(t: Box<dyn Any>) -> Self {
        Self(t)
    }
}

impl<'w, A: Allocate> Worker<'w, A> {
    /// Waits for client connections and runs them to completion.
    pub fn run(&mut self) {
        let mut shutdown = false;
        while !shutdown {
            match self.client_rx.recv() {
                Ok((rx, tx)) => self.run_client(rx, tx),
                Err(_) => shutdown = true,
            }
        }
    }

    /// Draws commands from a single client until disconnected.
    fn run_client(&mut self, command_rx: CommandReceiver, response_tx: ResponseSender) {
        self.reconcile(&command_rx);

        let mut disconnected = false;
        while !disconnected {
            // Ask Timely to execute a unit of work.
            //
            // If there are no pending commands, we ask Timely to park the
            // thread if there's nothing to do. We rely on another thread
            // unparking us when there's new work to be done, e.g., when sending
            // a command or when new Kafka messages have arrived.
            //
            // It is critical that we allow Timely to park iff there are no
            // pending commands. The command may have already been consumed by
            // the call to `client_rx.recv`.
            // See: https://github.com/MaterializeInc/materialize/pull/13973#issuecomment-1200312212
            if command_rx.is_empty() {
                self.timely_worker.step_or_park(None);
            } else {
                self.timely_worker.step();
            }

            self.report_frontier_progress(&response_tx);

            // Handle any received commands.
            let mut cmds = vec![];
            let mut empty = false;
            while !empty {
                match command_rx.try_recv() {
                    Ok(cmd) => cmds.push(cmd),
                    Err(TryRecvError::Empty) => empty = true,
                    Err(TryRecvError::Disconnected) => {
                        empty = true;
                        disconnected = true;
                    }
                }
            }
            for cmd in cmds {
                self.handle_storage_command(cmd);
            }
        }
    }

    /// Entry point for applying a storage command.
    pub fn handle_storage_command(&mut self, cmd: StorageCommand) {
        match cmd {
            StorageCommand::InitializationComplete => (),
            StorageCommand::IngestSources(ingestions) => {
                for ingestion in ingestions {
                    // Remember the ingestion description to facilitate possible
                    // reconciliation later.
                    self.storage_state
                        .ingestions
                        .insert(ingestion.id, ingestion.description.clone());

                    // Initialize shared frontier tracking.
                    self.storage_state.source_uppers.insert(
                        ingestion.id,
                        Rc::new(RefCell::new(Antichain::from_elem(
                            mz_repr::Timestamp::minimum(),
                        ))),
                    );

                    crate::render::build_ingestion_dataflow(
                        &mut self.timely_worker,
                        &mut self.storage_state,
                        ingestion.id,
                        ingestion.description,
                        ingestion.resume_upper,
                    );

                    self.storage_state.reported_frontiers.insert(
                        ingestion.id,
                        Antichain::from_elem(mz_repr::Timestamp::minimum()),
                    );
                }
            }
            StorageCommand::ExportSinks(exports) => {
                for export in exports {
                    self.storage_state
                        .exports
                        .insert(export.id, export.description.clone());

                    self.storage_state.sink_write_frontiers.insert(
                        export.id,
                        Rc::new(RefCell::new(Antichain::from_elem(
                            mz_repr::Timestamp::minimum(),
                        ))),
                    );

                    crate::render::build_export_dataflow(
                        &mut self.timely_worker,
                        &mut self.storage_state,
                        export.id,
                        export.description,
                    );

                    self.storage_state.reported_frontiers.insert(
                        export.id,
                        Antichain::from_elem(mz_repr::Timestamp::minimum()),
                    );
                }
            }
            StorageCommand::AllowCompaction(list) => {
                for (id, frontier) in list {
                    if frontier.is_empty() {
                        // Indicates that we may drop `id`, as there are no more valid times to read.
                        // Clean up per-source / per-sink state.
                        self.storage_state.source_uppers.remove(&id);
                        self.storage_state.reported_frontiers.remove(&id);
                        self.storage_state.source_tokens.remove(&id);
                        self.storage_state.sink_tokens.remove(&id);
                    }
                }
            }
        }
    }

    /// Emit information about write frontier progress, along with information that should
    /// be made durable for this to be the case.
    ///
    /// The write frontier progress is "conditional" in that it is not until the information is made
    /// durable that the data are emitted to downstream workers, and indeed they should not rely on
    /// the completeness of what they hear until the information is made durable.
    ///
    /// Specifically, this sends information about new timestamp bindings created by dataflow workers,
    /// with the understanding if that if made durable (and ack'd back to the workers) the source will
    /// in fact progress with this write frontier.
    pub fn report_frontier_progress(&mut self, response_tx: &ResponseSender) {
        let mut new_uppers = Vec::new();

        // Check if any observed frontier should advance the reported frontiers.
        for (id, frontier) in self
            .storage_state
            .source_uppers
            .iter()
            .chain(self.storage_state.sink_write_frontiers.iter())
        {
            let reported_frontier = self
                .storage_state
                .reported_frontiers
                .get_mut(&id)
                .expect("Reported frontier missing!");

            let observed_frontier = frontier.borrow();

            // Only do a thing if it *advances* the frontier, not just *changes* the frontier.
            // This is protection against `frontier` lagging behind what we have conditionally reported.
            if PartialOrder::less_than(reported_frontier, &observed_frontier) {
                new_uppers.push((*id, observed_frontier.clone()));
                reported_frontier.clone_from(&observed_frontier);
            }
        }

        if !new_uppers.is_empty() {
            self.send_storage_response(response_tx, StorageResponse::FrontierUppers(new_uppers));
        }
    }

    /// Send a response to the coordinator.
    fn send_storage_response(&self, response_tx: &ResponseSender, response: StorageResponse) {
        // Ignore send errors because the coordinator is free to ignore our
        // responses. This happens during shutdown.
        let _ = response_tx.send(response);
    }

    /// Extract commands until `InitializationComplete`, and make the worker
    /// reflect those commands. If the worker can not be made to reflect the
    /// commands, exit the process.
    ///
    /// This method is meant to be a function of the commands received thus far
    /// (as recorded in the compute state command history) and the new commands
    /// from `command_rx`. It should not be a function of other characteristics,
    /// like whether the worker has managed to respond to a peek or not. Some
    /// effort goes in to narrowing our view to only the existing commands we
    /// can be sure are live at all other workers.
    ///
    /// The methodology here is to drain `command_rx` until an
    /// `InitializationComplete`, at which point the prior commands are
    /// "reconciled" in. Reconciliation takes each goal dataflow and looks for
    /// an existing "compatible" dataflow (per `compatible()`) it can repurpose,
    /// with some additional tests to be sure that we can cut over from one to
    /// the other (no additional compaction, no tails/sinks). With any
    /// connections established, old orphaned dataflows are allow to compact
    /// away, and any new dataflows are created from scratch. "Kept" dataflows
    /// are allowed to compact up to any new `as_of`.
    ///
    /// Some additional tidying happens, e.g. cleaning up reported frontiers.
    /// tail response buffer. We will need to be vigilant with future
    /// modifications to `StorageState` to line up changes there with clean
    /// resets here.
    fn reconcile(&mut self, command_rx: &CommandReceiver) {
        // To initialize the connection, we want to drain all commands until we
        // receive a `StorageCommand::InitializationComplete` command to form a
        // target command state.
        let mut commands = vec![];
        while let Ok(command) = command_rx.recv() {
            match command {
                StorageCommand::InitializationComplete => break,
                _ => commands.push(command),
            }
        }

        // Elide any ingestions we're already aware of while determining what
        // ingestions no longer exist.
        let mut stale_ingestions = self.storage_state.ingestions.keys().collect::<HashSet<_>>();
        let mut stale_exports = self.storage_state.exports.keys().collect::<HashSet<_>>();
        for command in &mut commands {
            match command {
                StorageCommand::IngestSources(ingestions) => {
                    ingestions.retain_mut(|ingestion| {
                        if let Some(existing) = self.storage_state.ingestions.get(&ingestion.id) {
                            stale_ingestions.remove(&ingestion.id);
                            // If we've been asked to create an ingestion that is
                            // already installed, the descriptions must match
                            // exactly.
                            assert_eq!(
                                *existing, ingestion.description,
                                "New ingestion with same ID {:?}",
                                ingestion.id,
                            );
                            false
                        } else {
                            true
                        }
                    })
                }
                StorageCommand::ExportSinks(exports) => {
                    exports.retain_mut(|export| {
                        if let Some(existing) = self.storage_state.exports.get(&export.id) {
                            stale_exports.remove(&export.id);
                            // If we've been asked to create an export that is
                            // already installed, the descriptions must match
                            // exactly.
                            assert_eq!(
                                *existing, export.description,
                                "New export with same ID {:?}",
                                export.id,
                            );
                            false
                        } else {
                            true
                        }
                    })
                }
                StorageCommand::AllowCompaction(_) | StorageCommand::InitializationComplete => (),
            }
        }

        // Synthesize a drop command to remove stale ingestions and exports
        commands.push(StorageCommand::AllowCompaction(
            stale_ingestions
                .into_iter()
                .chain(stale_exports)
                .map(|id| (*id, Antichain::new()))
                .collect(),
        ));

        // Reset the reported frontiers.
        for (_, frontier) in &mut self.storage_state.reported_frontiers {
            *frontier = Antichain::from_elem(<_>::minimum());
        }

        // Execute the modified commands.
        for command in commands {
            self.handle_storage_command(command);
        }
    }
}
