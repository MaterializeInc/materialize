// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Worker-local state for storage timely instances.

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use crossbeam_channel::TryRecvError;
use mz_persist_client::cache::PersistClientCache;
use timely::communication::Allocate;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::progress::ChangeBatch;
use timely::progress::Timestamp as _;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::{mpsc, Mutex};

use mz_ore::now::NowFn;
use mz_repr::{GlobalId, Timestamp};

use crate::controller::CollectionMetadata;
use crate::protocol::client::{StorageCommand, StorageResponse};
use crate::types::connections::ConnectionContext;
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
    /// Undocumented
    pub now: NowFn,
    /// Metrics for the source-specific side of dataflows.
    pub source_metrics: SourceBaseMetrics,
    /// Index of the associated timely dataflow worker.
    pub timely_worker_index: usize,
    /// Peers in the associated timely dataflow worker.
    pub timely_worker_peers: usize,
    /// Configuration for source and sink connections.
    pub connection_context: ConnectionContext,
    /// A process-global cache of (blob_uri, consensus_uri) -> PersistClient.
    /// This is intentionally shared between workers
    pub persist_clients: Arc<Mutex<PersistClientCache>>,
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
        // A new client has connected. Reset state about what we have reported.
        //
        // TODO(benesch,mcsherry): introduce a `InitializationComplete` command
        // and only clear the state that is missing after initialization
        // completes.
        self.storage_state.reported_frontiers.clear();

        let mut disconnected = false;
        while !disconnected {
            // Ask Timely to execute a unit of work. If Timely decides there's
            // nothing to do, it will park the thread. We rely on another thread
            // unparking us when there's new work to be done, e.g., when sending
            // a command or when new Kafka messages have arrived.
            self.timely_worker.step_or_park(None);

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
            StorageCommand::IngestSources(ingestions) => {
                for ingestion in ingestions {
                    if let Some(existing) = self.storage_state.ingestions.get(&ingestion.id) {
                        // If we've been asked to create an ingestion that is
                        // already installed, the descriptions must match
                        // exactly.
                        assert_eq!(
                            *existing, ingestion.description,
                            "New ingestion with same ID {:?}",
                            ingestion.id,
                        );
                        self.storage_state.reported_frontiers.insert(
                            ingestion.id,
                            Antichain::from_elem(mz_repr::Timestamp::minimum()),
                        );
                        continue;
                    }

                    // Initialize shared frontier tracking.
                    self.storage_state.source_uppers.insert(
                        ingestion.id,
                        Rc::new(RefCell::new(Antichain::from_elem(
                            mz_repr::Timestamp::minimum(),
                        ))),
                    );

                    crate::render::build_storage_dataflow(
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
            StorageCommand::AllowCompaction(list) => {
                for (id, frontier) in list {
                    if frontier.is_empty() {
                        // Indicates that we may drop `id`, as there are no more valid times to read.
                        // Clean up per-source state.
                        self.storage_state.source_uppers.remove(&id);
                        self.storage_state.reported_frontiers.remove(&id);
                        self.storage_state.source_tokens.remove(&id);
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
        let mut changes = Vec::new();

        // Check if any observed frontier should advance the reported frontiers.
        for (id, frontier) in self.storage_state.source_uppers.iter() {
            let reported_frontier = match self.storage_state.reported_frontiers.get_mut(&id) {
                None => {
                    // The current client has not expressed interest in this
                    // collection. Move on.
                    //
                    // TODO(benesch): reconciliation should prevent
                    // this situation.
                    continue;
                }
                Some(reported_frontier) => reported_frontier,
            };

            let observed_frontier = frontier.borrow();

            // Only do a thing if it *advances* the frontier, not just *changes* the frontier.
            // This is protection against `frontier` lagging behind what we have conditionally reported.
            if <_ as PartialOrder>::less_than(reported_frontier, &observed_frontier) {
                let mut change_batch = ChangeBatch::new();
                for time in reported_frontier.elements().iter() {
                    change_batch.update(time.clone(), -1);
                }
                for time in observed_frontier.elements().iter() {
                    change_batch.update(time.clone(), 1);
                }
                if !change_batch.is_empty() {
                    changes.push((*id, change_batch));
                }
                reported_frontier.clone_from(&observed_frontier);
            }
        }

        if !changes.is_empty() {
            self.send_storage_response(response_tx, StorageResponse::FrontierUppers(changes));
        }
    }

    /// Send a response to the coordinator.
    fn send_storage_response(&self, response_tx: &ResponseSender, response: StorageResponse) {
        // Ignore send errors because the coordinator is free to ignore our
        // responses. This happens during shutdown.
        let _ = response_tx.send(response);
    }
}
