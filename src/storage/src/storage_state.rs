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
use differential_dataflow::lattice::Lattice;
use timely::communication::Allocate;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::progress::Timestamp as _;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use mz_ore::halt;
use mz_ore::now::NowFn;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::{PersistLocation, ShardId};
use mz_repr::{Diff, GlobalId, Timestamp};
use mz_storage_client::client::{StorageCommand, StorageResponse};
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::types::connections::ConnectionContext;
use mz_storage_client::types::sinks::{MetadataFilled, StorageSinkDesc};
use mz_storage_client::types::sources::{IngestionDescription, SourceData};

use crate::decode::metrics::DecodeMetrics;
use crate::sink::SinkBaseMetrics;
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
    pub exports: HashMap<GlobalId, StorageSinkDesc<MetadataFilled, mz_repr::Timestamp>>,
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
    /// See: [SinkHandle]
    pub sink_handles: HashMap<GlobalId, SinkHandle>,
    /// Collection ids that have been dropped but not yet reported as dropped
    pub dropped_ids: Vec<GlobalId>,
}

/// This maintains an additional read hold on the source data for a sink, alongside
/// the controller's hold and the handle used to read the shard internally.
/// This is useful because environmentd's hold might expire, and the handle we use
/// to read advances ahead of what we've successfully committed.
/// In theory this could be stored alongside the other sink data, but this isn't
/// intended to be a long term solution; either this should become a "critical" handle
/// or environmentd will learn to hold its handles across restarts and this won't be
/// needed.
pub struct SinkHandle {
    downgrade_tx: watch::Sender<Antichain<Timestamp>>,
    _handle: JoinHandle<()>,
}

impl SinkHandle {
    /// A new handle.
    pub fn new(
        persist_location: PersistLocation,
        shard_id: ShardId,
        persist_clients: Arc<Mutex<PersistClientCache>>,
    ) -> SinkHandle {
        let (downgrade_tx, mut rx) = watch::channel(Antichain::from_elem(Timestamp::minimum()));

        let _handle = mz_ore::task::spawn(|| "Sink handle advancement", async move {
            let client = persist_clients
                .lock()
                .await
                .open(persist_location)
                .await
                .expect("opening persist client");

            let mut read_handle: ReadHandle<SourceData, (), Timestamp, Diff> = client
                .open_leased_reader(shard_id)
                .await
                .expect("opening reader for shard");

            let mut downgrade_to = read_handle.since().clone();
            'downgrading: loop {
                // NB: all branches of the select are cancellation safe.
                tokio::select! {
                    result = rx.changed() => {
                        match result {
                            Err(_) => {
                                // The sending end of this channel has been dropped, which means
                                // we're no longer interested in this handle.
                                break 'downgrading;
                            }
                            Ok(()) => {
                                // Join to avoid attempting to rewind the handle
                                downgrade_to.join_assign(&rx.borrow_and_update());
                            }
                        }
                    }
                    _ = sleep(Duration::from_secs(60)) => {}
                };
                read_handle.maybe_downgrade_since(&downgrade_to).await
            }

            // Proactively drop our read hold.
            read_handle.expire().await;
        });

        SinkHandle {
            downgrade_tx,
            _handle,
        }
    }

    /// Request a downgrade of the since. This should be called regularly;
    /// the internal task will debounce.
    pub fn downgrade_since(&self, to: Antichain<Timestamp>) {
        self.downgrade_tx
            .send(to)
            .expect("sending to downgrade task")
    }
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

            // Rerport any dropped ids
            if !self.storage_state.dropped_ids.is_empty() {
                let ids = std::mem::take(&mut self.storage_state.dropped_ids);
                self.send_storage_response(&response_tx, StorageResponse::DroppedIds(ids));
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
            StorageCommand::CreateSources(ingestions) => {
                for ingestion in ingestions {
                    // Remember the ingestion description to facilitate possible
                    // reconciliation later.
                    self.storage_state
                        .ingestions
                        .insert(ingestion.id, ingestion.description.clone());

                    // Initialize shared frontier tracking.
                    for export_id in ingestion.description.source_exports.keys() {
                        self.storage_state.source_uppers.insert(
                            *export_id,
                            Rc::new(RefCell::new(Antichain::from_elem(
                                mz_repr::Timestamp::minimum(),
                            ))),
                        );
                        self.storage_state.reported_frontiers.insert(
                            *export_id,
                            Antichain::from_elem(mz_repr::Timestamp::minimum()),
                        );
                    }

                    crate::render::build_ingestion_dataflow(
                        self.timely_worker,
                        &mut self.storage_state,
                        ingestion.id,
                        ingestion.description,
                        ingestion.resume_upper,
                    );
                }
            }
            StorageCommand::CreateSinks(exports) => {
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

                    self.storage_state.sink_handles.insert(
                        export.id,
                        SinkHandle::new(
                            export
                                .description
                                .from_storage_metadata
                                .persist_location
                                .clone(),
                            export.description.from_storage_metadata.data_shard,
                            Arc::clone(&self.storage_state.persist_clients),
                        ),
                    );

                    crate::render::build_export_dataflow(
                        self.timely_worker,
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
                        self.storage_state.sink_handles.remove(&id);
                        self.storage_state.dropped_ids.push(id);
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
                .get_mut(id)
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
            // Sinks maintain a read handle over their input data, in case environmentd is unable
            // to maintain the global read hold. It's tempting to use environmentd's AllowCompaction
            // messages to maintain an even more conservative hold... but environmentd only sends
            // storaged AllowCompaction messages for its own id, not for its dependencies.
            for (id, upper) in &new_uppers {
                if let Some(handle) = &self.storage_state.sink_handles.get(id) {
                    handle.downgrade_since(upper.clone());
                }
            }
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
    /// subscribe response buffer. We will need to be vigilant with future
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
                StorageCommand::CreateSources(ingestions) => {
                    ingestions.retain_mut(|ingestion| {
                        if let Some(existing) = self.storage_state.ingestions.get(&ingestion.id) {
                            stale_ingestions.remove(&ingestion.id);
                            // If we've been asked to create an ingestion that is
                            // already installed, the descriptions must match
                            // exactly.
                            if *existing != ingestion.description {
                                halt!(
                                    "new ingestion with ID {} does not match existing ingestion:\n{:?}\nvs\n{:?}",
                                    ingestion.id,
                                    ingestion.description,
                                    existing,
                                );
                            }
                            false
                        } else {
                            true
                        }
                    })
                }
                StorageCommand::CreateSinks(exports) => {
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
