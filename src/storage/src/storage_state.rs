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

use crossbeam_channel::{RecvError, TryRecvError};
use differential_dataflow::lattice::Lattice;
use timely::communication::Allocate;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Broadcast;
use timely::dataflow::operators::Operator;
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
use mz_timely_util::builder_async::OperatorBuilder as AsyncOperatorBuilder;

use crate::decode::metrics::DecodeMetrics;
use crate::sink::SinkBaseMetrics;
use crate::source::metrics::SourceBaseMetrics;

type CommandReceiver = mpsc::UnboundedReceiver<StorageCommand>;
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
        sink_id: GlobalId,
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
                .open_leased_reader(shard_id, &format!("sink::since {}", sink_id))
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
        let mut command_rx = self.install_command_dataflow(command_rx);

        if let Err(_) = self.reconcile(&mut command_rx) {
            return;
        }

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

    /// Sets up an internal dataflow that will forward commands from the given
    /// async receiver and forwards them to a local channel. The returned
    /// receiver should be used to receive and process all commands.
    ///
    /// Only worker 0 is receiving commands from the controller and is
    /// responsible for broadcasting them to the other workers.
    fn install_command_dataflow(
        &mut self,
        mut command_rx: mpsc::UnboundedReceiver<StorageCommand>,
    ) -> CommandPump {
        // TODO(aljoscha): As a next step, we can change how commands are sent to
        // the storage timely cluster to make it more similar to compute where
        // commands are only sent to the first worker and are then broadcast using
        // this cluster-internal (replica-internal, really) dataflow.
        let (forwarding_command_tx, forwarding_command_rx) = crossbeam_channel::unbounded();

        let worker_id = self.timely_worker.index();

        self.timely_worker.dataflow::<u64, _, _>(move |scope| {
            let mut pump_op =
                AsyncOperatorBuilder::new("StorageCmdPump".to_string(), scope.clone());

            let (mut cmd_output, cmd_stream) = pump_op.new_output();

            let _shutdown_button = pump_op.build(move |mut capabilities| async move {
                let mut cap = capabilities.pop().expect("missing capability");

                while let Some(cmd) = command_rx.recv().await {
                    // Commands must never be sent to another worker. This
                    // implementation does not guarantee an ordering of events
                    // sent to different workers.
                    assert_eq!(worker_id, 0);

                    let time = cap.time().clone();
                    let mut cmd_output = cmd_output.activate();
                    let mut session = cmd_output.session(&cap);

                    session.give(cmd);

                    cap.downgrade(&(time + 1));
                }
            });

            cmd_stream.broadcast().unary_frontier::<Vec<()>, _, _, _>(
                Pipeline,
                "StorageCmdForwarder",
                |_cap, _info| {
                    let mut container = Default::default();
                    move |input, _output| {
                        while let Some((_, data)) = input.next() {
                            data.swap(&mut container);
                            for cmd in container.drain(..) {
                                let res = forwarding_command_tx.send(Ok(cmd));
                                // TODO(aljoscha): Could completely drop this
                                // error handling code, with the below
                                // reasoning.
                                match res {
                                    Ok(_) => {
                                        // All's well!
                                        //
                                    }
                                    Err(_send_err) => {
                                        // The subscribe loop dropped, meaning
                                        // we're probably shutting down. Seems
                                        // fine to ignore.
                                    }
                                }
                            }
                        }

                        // Send a final "Disconnected" message.
                        if input.frontier().is_empty() {
                            let res = forwarding_command_tx.send(Err(RecvError));
                            // TODO(aljoscha): Could completely drop this
                            // error handling code, with the below
                            // reasoning.
                            match res {
                                Ok(_) => {
                                    // All's well!
                                    //
                                }
                                Err(_send_err) => {
                                    // The subscribe loop dropped, meaning
                                    // we're probably shutting down. Seems
                                    // fine to ignore.
                                }
                            }
                        }
                    }
                },
            );
        });

        CommandPump {
            command_rx: forwarding_command_rx,
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
                            export.id,
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
            // clusterd AllowCompaction messages for its own id, not for its dependencies.
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
    fn reconcile(&mut self, command_rx: &mut CommandPump) -> Result<(), RecvError> {
        // To initialize the connection, we want to drain all commands until we
        // receive a `StorageCommand::InitializationComplete` command to form a
        // target command state.
        let mut commands = vec![];

        // TODO(aljoscha): I don't like that we are passing the timely worker
        // here and that `recv` will internally step that worker until commands
        // are available. I see two options:
        //
        //  1. We live with this, this is also how compute does it currently.
        //  2. We change reconciliation to be a state that we can be in, and
        //     move the logic that we do at the end of this method to be in the
        //     method handler of `InitializationComplete`. The timely main loop
        //     would start out in a state of reconciling and then transition to
        //     "normal" state.
        //
        //  I like how 2. integrates nicer with how message pumping works, but
        //  "shmears" the reconciliation logic across more code.
        loop {
            match command_rx.recv(self)? {
                StorageCommand::InitializationComplete => break,
                command => commands.push(command),
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

        Ok(())
    }
}

struct CommandPump {
    // TODO(aljoscha): Compute uses a shared `VecDeque` for this. I can change
    // it to that if we like the basic shape of this. I used a channel just out
    // of laziness, but we don't need the synchronization/costs that come with
    // it.
    command_rx: crossbeam_channel::Receiver<Result<StorageCommand, RecvError>>,
}

impl CommandPump {
    fn try_recv(&mut self) -> Result<StorageCommand, TryRecvError> {
        let wrapped = self.command_rx.try_recv();
        match wrapped {
            Ok(Ok(msg)) => Ok(msg),
            Ok(Err(RecvError)) => Err(TryRecvError::Disconnected),
            Err(try_recv_error) => Err(try_recv_error),
        }
    }

    /// Blocks until a command is available.
    fn recv<A: Allocate>(&mut self, worker: &mut Worker<A>) -> Result<StorageCommand, RecvError> {
        while self.is_empty() {
            worker.timely_worker.step_or_park(None);
        }
        self.command_rx.recv().expect("must contain element")
    }

    fn is_empty(&self) -> bool {
        self.command_rx.is_empty()
    }
}
