// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Worker-local state for storage timely instances.
//!
//! One instance of a [`Worker`], along with its contained [`StorageState`], is
//! part of an ensemble of storage workers that all run inside the same timely
//! cluster. We call this worker a _storage worker_ to disambiguate it from
//! other kinds of workers, potentially other components that might be sharing
//! the same timely cluster.
//!
//! ## Controller and internal communication
//!
//! A worker receives _external_ [`StorageCommands`](StorageCommand) from the
//! storage controller, via a channel. Storage workers also share an _internal_
//! control/command fabric ([`internal_control`](crate::internal_control)).
//! Internal commands go through a `Sequencer` dataflow that ensures that all
//! workers receive all commands in the same consistent order.
//!
//! We need to make sure that commands that cause dataflows to be rendered are
//! processed in the same consistent order across all workers because timely
//! requires this. To achieve this, we make sure that only internal commands can
//! cause dataflows to be rendered. External commands (from the controller)
//! cause internal commands to be broadcast (by only one worker), to get
//! dataflows rendered.
//!
//! The internal command fabric is also used to broadcast messages from a local
//! operator/worker to all workers. For example, when we need to tear down and
//! restart a dataflow on all workers when an error is encountered.
//!
//! ## Async Storage Worker
//!
//! The storage worker has a companion [`AsyncStorageWorker`] that must be used
//! when running code that requires `async`. This is needed because a timely
//! main loop cannot run `async` code.
//!
//! ## Example flow of commands for `RunIngestions`
//!
//! With external commands, internal commands, and the async worker,
//! understanding where and how commands from the controller are realized can
//! get complicated. We will follow the complete flow for `RunIngestions`, as an
//! example:
//!
//! 1. Worker receives a [`StorageCommand::RunIngestions`] command from the
//!    controller.
//! 2. This command is processed in [`StorageState::handle_storage_command`].
//!    This step cannot render dataflows, because it does not have access to the
//!    timely worker. It will only set up state that stays over the whole
//!    lifetime of the source, such as the `reported_frontier`. Putting in place
//!    this reported frontier will enable frontier reporting for that source. We
//!    will not start reporting when we only see an internal command for
//!    rendering a dataflow, which can "overtake" the external `RunIngestions`
//!    command.
//! 3. During processing of that command, we call
//!    [`AsyncStorageWorker::update_frontiers`], which causes a command to
//!    be sent to the async worker.
//! 4. We eventually get a response from the async worker:
//!    [`AsyncStorageWorkerResponse::FrontiersUpdated`].
//! 5. This response is handled in [`Worker::handle_async_worker_response`].
//! 6. Handling that response causes a
//!    [`InternalStorageCommand::CreateIngestionDataflow`] to be broadcast to
//!    all workers via the internal command fabric.
//! 7. This message will be processed (on each worker) in
//!    [`Worker::handle_internal_storage_command`]. This is what will cause the
//!    required dataflow to be rendered on all workers.
//!
//! The process described above assumes that the `RunIngestions` is _not_ an
//! update, i.e. it is in response to a `CREATE SOURCE`-like statement.
//!
//! The primary distinction when handling a `RunIngestions` that represents an
//! update, is that it might fill out new internal state in the mid-level
//! clients on the way toward being run.

use std::any::Any;
use std::cell::RefCell;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;

use crossbeam_channel::TryRecvError;
use differential_dataflow::lattice::Lattice;
use fail::fail_point;
use mz_ore::now::NowFn;
use mz_ore::vec::VecExt;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::ShardId;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, GlobalId, Timestamp};
use mz_storage_client::client::{
    RunIngestionCommand, SinkStatisticsUpdate, SourceStatisticsUpdate, StorageCommand,
    StorageResponse,
};
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::types::connections::ConnectionContext;
use mz_storage_client::types::sinks::{MetadataFilled, StorageSinkDesc};
use mz_storage_client::types::sources::{IngestionDescription, SourceData};
use timely::communication::Allocate;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::progress::Timestamp as _;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration, Instant};
use tracing::{info, trace};

use crate::decode::metrics::DecodeMetrics;
use crate::internal_control::{
    self, DataflowParameters, InternalCommandSender, InternalStorageCommand,
};
use crate::sink::SinkBaseMetrics;
use crate::source::metrics::SourceBaseMetrics;
use crate::statistics::{SinkStatisticsMetrics, SourceStatisticsMetrics, StorageStatistics};
use crate::storage_state::async_storage_worker::{AsyncStorageWorker, AsyncStorageWorkerResponse};

pub mod async_storage_worker;

type CommandReceiver = crossbeam_channel::Receiver<StorageCommand>;
type ResponseSender = mpsc::UnboundedSender<StorageResponse>;

/// State maintained for each worker thread.
///
/// Much of this state can be viewed as local variables for the worker thread,
/// holding state that persists across function calls.
pub struct Worker<'w, A: Allocate> {
    /// The underlying Timely worker.
    ///
    /// NOTE: This is `pub` for testing.
    pub timely_worker: &'w mut TimelyWorker<A>,
    /// The channel over which communication handles for newly connected clients
    /// are delivered.
    pub client_rx: crossbeam_channel::Receiver<(
        CommandReceiver,
        ResponseSender,
        crossbeam_channel::Sender<std::thread::Thread>,
    )>,
    /// The state associated with collection ingress and egress.
    pub storage_state: StorageState,
}

impl<'w, A: Allocate> Worker<'w, A> {
    /// Creates new `Worker` state from the given components.
    pub fn new(
        timely_worker: &'w mut TimelyWorker<A>,
        client_rx: crossbeam_channel::Receiver<(
            CommandReceiver,
            ResponseSender,
            crossbeam_channel::Sender<std::thread::Thread>,
        )>,
        decode_metrics: DecodeMetrics,
        source_metrics: SourceBaseMetrics,
        sink_metrics: SinkBaseMetrics,
        now: NowFn,
        connection_context: ConnectionContext,
        instance_context: StorageInstanceContext,
        persist_clients: Arc<PersistClientCache>,
    ) -> Self {
        // It is very important that we only create the internal control
        // flow/command sequencer once because a) the worker state is re-used
        // when a new client connects and b) dataflows that have already been
        // rendered into the timely worker are reused as well.
        //
        // If we created a new sequencer every time we get a new client (likely
        // because the controller re-started and re-connected), dataflows that
        // were rendered before would still hold a handle to the old sequencer
        // but we would not read their commands anymore.
        let command_sequencer = internal_control::setup_command_sequencer(timely_worker);
        let command_sequencer = Rc::new(RefCell::new(command_sequencer));

        // Similar to the internal command sequencer, it is very important that
        // we only create the async worker once because a) the worker state is
        // re-used when a new client connects and b) commands that have already
        // been sent and might yield a response will be lost if a new iteration
        // of `run_client` creates a new async worker.
        //
        // If we created a new async worker every time we get a new client
        // (likely because the controller re-started and re-connected), we can
        // get into an inconsistent state where we think that a dataflow has
        // been rendered, for example because there is an entry in
        // `StorageState::ingestions`, while there is not yet a dataflow. This
        // happens because the dataflow only gets rendered once we get a
        // response from the async worker and send off an internal command.
        //
        // The core idea is that both the sequencer and the async worker are
        // part of the per-worker state, and must be treated as such, meaning
        // they must survive between invocations of `run_client`.

        // TODO(aljoscha): This `Activatable` business seems brittle, but that's
        // also how the command channel works currently. We can wrap it inside a
        // struct that holds both a channel and an `Activatable`, but I don't
        // think that would help too much.
        let async_worker = async_storage_worker::AsyncStorageWorker::new(
            thread::current(),
            Arc::clone(&persist_clients),
        );
        let async_worker = Rc::new(RefCell::new(async_worker));

        let storage_state = StorageState {
            source_uppers: BTreeMap::new(),
            source_tokens: BTreeMap::new(),
            decode_metrics,
            reported_frontiers: BTreeMap::new(),
            ingestions: BTreeMap::new(),
            exports: BTreeMap::new(),
            now,
            source_metrics,
            sink_metrics,
            timely_worker_index: timely_worker.index(),
            timely_worker_peers: timely_worker.peers(),
            connection_context,
            instance_context,
            persist_clients,
            sink_tokens: BTreeMap::new(),
            sink_write_frontiers: BTreeMap::new(),
            sink_handles: BTreeMap::new(),
            dropped_ids: BTreeSet::new(),
            source_statistics: BTreeMap::new(),
            sink_statistics: BTreeMap::new(),
            internal_cmd_tx: command_sequencer,
            async_worker,
            dataflow_parameters: Default::default(),
        };

        // TODO(aljoscha): We might want `async_worker` and `internal_cmd_tx` to
        // be fields of `Worker` instead of `StorageState`, but at least for the
        // command flow sources and sinks need access to that. We can refactor
        // this once we have a clearer boundary between what sources/sinks need
        // and the full "power" of the internal command flow, which should stay
        // internal to the worker/not be exposed to source/sink implementations.
        Self {
            timely_worker,
            client_rx,
            storage_state,
        }
    }
}

/// Worker-local state related to the ingress or egress of collections of data.
pub struct StorageState {
    /// The highest observed upper frontier for collection.
    ///
    /// This is shared among all source instances, so that they can jointly advance the
    /// frontier even as other instances are created and dropped. Ideally, the Storage
    /// module would eventually provide one source of truth on this rather than multiple,
    /// and we should aim for that but are not there yet.
    pub source_uppers: BTreeMap<GlobalId, Rc<RefCell<Antichain<mz_repr::Timestamp>>>>,
    /// Handles to created sources, keyed by ID
    pub source_tokens: BTreeMap<GlobalId, Rc<dyn Any>>,
    /// Decoding metrics reported by all dataflows.
    pub decode_metrics: DecodeMetrics,
    /// Tracks the conditional write frontiers we have reported.
    pub reported_frontiers: BTreeMap<GlobalId, Antichain<Timestamp>>,
    /// Descriptions of each installed ingestion.
    pub ingestions: BTreeMap<GlobalId, IngestionDescription<CollectionMetadata>>,
    /// Descriptions of each installed export.
    pub exports: BTreeMap<GlobalId, StorageSinkDesc<MetadataFilled, mz_repr::Timestamp>>,
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
    /// Other configuration for sources and sinks.
    pub instance_context: StorageInstanceContext,
    /// A process-global cache of (blob_uri, consensus_uri) -> PersistClient.
    /// This is intentionally shared between workers
    pub persist_clients: Arc<PersistClientCache>,
    /// Tokens that should be dropped when a dataflow is dropped to clean up
    /// associated state.
    pub sink_tokens: BTreeMap<GlobalId, SinkToken>,
    /// Frontier of sink writes (all subsequent writes will be at times at or
    /// equal to this frontier)
    pub sink_write_frontiers: BTreeMap<GlobalId, Rc<RefCell<Antichain<Timestamp>>>>,
    /// See: [SinkHandle]
    pub sink_handles: BTreeMap<GlobalId, SinkHandle>,
    /// Collection ids that have been dropped but not yet reported as dropped
    pub dropped_ids: BTreeSet<GlobalId>,

    /// Stats objects shared with operators to allow them to update the metrics
    /// we report in `StatisticsUpdates` responses.
    pub source_statistics:
        BTreeMap<GlobalId, StorageStatistics<SourceStatisticsUpdate, SourceStatisticsMetrics>>,
    /// The same as `source_statistics`, but for sinks.
    pub sink_statistics:
        BTreeMap<GlobalId, StorageStatistics<SinkStatisticsUpdate, SinkStatisticsMetrics>>,

    /// Sender for cluster-internal storage commands. These can be sent from
    /// within workers/operators and will be distributed to all workers. For
    /// example, for shutting down an entire dataflow from within a
    /// operator/worker.
    pub internal_cmd_tx: Rc<RefCell<dyn InternalCommandSender>>,

    /// Async worker companion, used for running code that requires async, which
    /// the timely main loop cannot do.
    pub async_worker: Rc<RefCell<AsyncStorageWorker<mz_repr::Timestamp>>>,

    /// Dynamically configurable parameters that control how dataflows are rendered.
    pub dataflow_parameters: DataflowParameters,
}

/// Extra context for a storage instance.
/// This is extra information that is used when rendering source
/// and sinks that is not tied to the source/connection configuration itself.
#[derive(Clone)]
pub struct StorageInstanceContext {
    /// A directory that can be used for scratch work.
    pub scratch_directory: Option<PathBuf>,
    /// A global `rocksdb::Env`, shared across ALL instances of `RocksDB` (even
    /// across sources!). This `Env` lets us control some resources (like background threads)
    /// process-wide.
    pub rocksdb_env: rocksdb::Env,
}

impl StorageInstanceContext {
    /// Build a new `StorageInstanceContext`.
    pub fn new(scratch_directory: Option<PathBuf>) -> Result<Self, anyhow::Error> {
        Ok(Self {
            scratch_directory,
            rocksdb_env: rocksdb::Env::new()?,
        })
    }

    /// Constructs a new connection context for usage in tests.
    pub fn for_tests(rocksdb_env: rocksdb::Env) -> Self {
        Self {
            scratch_directory: None,
            rocksdb_env,
        }
    }
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
        from_metadata: &CollectionMetadata,
        shard_id: ShardId,
        initial_since: Antichain<Timestamp>,
        persist_clients: Arc<PersistClientCache>,
    ) -> SinkHandle {
        let (downgrade_tx, mut rx) = watch::channel(Antichain::from_elem(Timestamp::minimum()));

        let persist_location = from_metadata.persist_location.clone();
        let from_relation_desc = from_metadata.relation_desc.clone();

        let _handle = mz_ore::task::spawn(|| "Sink handle advancement", async move {
            let client = persist_clients
                .open(persist_location)
                .await
                .expect("opening persist client");

            let mut read_handle: ReadHandle<SourceData, (), Timestamp, Diff> = client
                .open_leased_reader(
                    shard_id,
                    &format!("sink::since {}", sink_id),
                    Arc::new(from_relation_desc),
                    Arc::new(UnitSchema),
                )
                .await
                .expect("opening reader for shard");

            assert!(
                !PartialOrder::less_than(&initial_since, read_handle.since()),
                "could not acquire a SinkHandle that can hold the \
                initial since {:?}, the since is already at {:?}",
                initial_since,
                read_handle.since()
            );

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
                Ok((rx, tx, activator_tx)) => {
                    activator_tx
                        .send(std::thread::current())
                        .expect("activator_tx working");
                    self.run_client(rx, tx)
                }
                Err(_) => {
                    shutdown = true;
                }
            }
        }
    }

    /// Runs this (timely) storage worker until the given `command_rx` is
    /// disconnected.
    ///
    /// See the [module documentation](crate::storage_state) for this
    /// workers responsibilities, how it communicates with the other workers and
    /// how commands flow from the controller and through the workers.
    fn run_client(&mut self, command_rx: CommandReceiver, response_tx: ResponseSender) {
        // There can only ever be one timely main loop/thread that sends
        // commands to the async worker, so we borrow it for the whole lifetime
        // of the loop below.
        let async_worker = Rc::clone(&self.storage_state.async_worker);
        let mut async_worker = async_worker.borrow_mut();

        // We need this to get around having to borrow the sequencer but also
        // passing around references to `self.storage_state`.
        let command_sequencer = Rc::clone(&self.storage_state.internal_cmd_tx);

        // At this point, all workers are still reading from the command flow.
        {
            let mut command_sequencer = command_sequencer.borrow_mut();
            self.reconcile(&mut *command_sequencer, &mut async_worker, &command_rx);
        }

        let mut disconnected = false;

        let mut last_stats_time: Option<Instant> = None;

        while !disconnected {
            // Ask Timely to execute a unit of work.
            //
            // If there are no pending commands or responses from the async
            // worker, we ask Timely to park the thread if there's nothing to
            // do. We rely on another thread unparking us when there's new work
            // to be done, e.g., when sending a command or when new Kafka
            // messages have arrived.
            //
            // It is critical that we allow Timely to park iff there are no
            // pending commands or responses. The command may have already been
            // consumed by the call to `client_rx.recv`. See:
            // https://github.com/MaterializeInc/materialize/pull/13973#issuecomment-1200312212
            if command_rx.is_empty() && async_worker.is_empty() {
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

            // Note: this interval configures the level of granularity we expect statistics
            // (at least with this implementation) to have. We expect a statistic in the
            // system tables to be only accurate to within this interval + whatever
            // skew the `CollectionManager` adds. The stats task in the controller will
            // be reporting, for each worker, on some interval,
            // the statistics reported by the most recent call here. This is known to be
            // somewhat inaccurate, but people mostly care about either rates, or the
            // values to within 1 minute.
            //
            // TODO(guswynn): Should this be configurable? Maybe via LaunchDarkly?
            if last_stats_time.is_none()
                || last_stats_time.as_ref().unwrap().elapsed() >= Duration::from_secs(10)
            {
                self.report_storage_statistics(&response_tx);
                last_stats_time = Some(Instant::now());
            }

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
            {
                let mut internal_cmd_tx = command_sequencer.borrow_mut();
                for cmd in cmds {
                    self.storage_state.handle_storage_command(
                        self.timely_worker.index(),
                        &mut *internal_cmd_tx,
                        &mut async_worker,
                        cmd,
                    )
                }
            }

            // Handle responses from the async worker.
            let mut empty = false;
            while !empty {
                match async_worker.try_recv() {
                    Ok(response) => {
                        let mut command_sequencer = command_sequencer.borrow_mut();
                        self.handle_async_worker_response(&mut *command_sequencer, response);
                    }
                    Err(TryRecvError::Empty) => empty = true,
                    Err(TryRecvError::Disconnected) => {
                        empty = true;
                    }
                }
            }

            // Handle any received commands.
            {
                let mut command_sequencer = command_sequencer.borrow_mut();
                while let Some(internal_cmd) = command_sequencer.next() {
                    self.handle_internal_storage_command(
                        &mut *command_sequencer,
                        &mut async_worker,
                        internal_cmd,
                    );
                }
            }
        }
    }

    /// Entry point for applying a response from the async storage worker.
    pub fn handle_async_worker_response(
        &mut self,
        internal_cmd_tx: &mut dyn InternalCommandSender,
        async_response: AsyncStorageWorkerResponse<mz_repr::Timestamp>,
    ) {
        match async_response {
            AsyncStorageWorkerResponse::FrontiersUpdated {
                id,
                ingestion_description,
                as_of,
                resume_uppers,
                source_resume_uppers,
            } => {
                // NOTE: If we want to share the load of async processing we
                // have to change `handle_storage_command` and change this
                // assert.
                assert_eq!(
                    self.timely_worker.index(),
                    0,
                    "only worker #0 is doing async processing"
                );
                internal_cmd_tx.broadcast(InternalStorageCommand::CreateIngestionDataflow {
                    id,
                    ingestion_description,
                    as_of,
                    resume_uppers,
                    source_resume_uppers,
                });
            }
        }
    }

    /// Entry point for applying an internal storage command.
    pub fn handle_internal_storage_command(
        &mut self,
        internal_cmd_tx: &mut dyn InternalCommandSender,
        async_worker: &mut AsyncStorageWorker<mz_repr::Timestamp>,
        internal_cmd: InternalStorageCommand,
    ) {
        match internal_cmd {
            InternalStorageCommand::SuspendAndRestart { id, reason } => {
                info!(
                    "worker {}/{} initiating suspend-and-restart for {id} because of: {reason}",
                    self.timely_worker.index(),
                    self.timely_worker.peers(),
                );

                let maybe_ingestion = self.storage_state.ingestions.get(&id).cloned();
                if let Some(ingestion_description) = maybe_ingestion {
                    // Yank the token of the previously existing source dataflow.Note that this
                    // token also includes any source exports/subsources.

                    let maybe_token = self.storage_state.source_tokens.remove(&id);

                    if maybe_token.is_none() {
                        // Something has dropped the source. Make sure we don't
                        // accidentally re-create it.
                        return;
                    }

                    // This needs to be done by one worker, which will
                    // broadcasts a `CreateIngestionDataflow` command to all
                    // workers based on the response that contains the
                    // resumption upper.
                    //
                    // Doing this separately on each worker could lead to
                    // differing resume_uppers which might lead to all kinds of
                    // mayhem.
                    //
                    // TODO(aljoscha): If we ever become worried that this is
                    // putting undue pressure on worker 0 we can pick the
                    // designated worker for a source/sink based on `id.hash()`.
                    if self.timely_worker.index() == 0 {
                        async_worker.update_frontiers(id, ingestion_description);
                    }

                    // Continue with other commands.
                    return;
                }

                let maybe_sink = self.storage_state.exports.get(&id).cloned();
                if let Some(sink_description) = maybe_sink {
                    // Yank the token of the previously existing sink
                    // dataflow.
                    let maybe_token = self.storage_state.sink_tokens.remove(&id);

                    if maybe_token.is_none() {
                        // Something has dropped the sink. Make sure we don't
                        // accidentally re-create it.
                        return;
                    }

                    // This needs to be broadcast by one worker and go through
                    // the internal command fabric, to ensure consistent
                    // ordering of dataflow rendering across all workers.
                    if self.timely_worker.index() == 0 {
                        internal_cmd_tx.broadcast(InternalStorageCommand::CreateSinkDataflow(
                            id,
                            sink_description,
                        ));
                    }

                    // Continue with other commands.
                    return;
                }

                // Suspensions might come in for source exports; they are suspended and
                // restarted alongside their primary sources.
                if self
                    .storage_state
                    .ingestions
                    .values()
                    .any(|v| v.source_exports.contains_key(&id))
                {
                    return;
                }

                panic!("got InternalStorageCommand::SuspendAndRestart for something that is not a source or sink: {id}");
            }
            InternalStorageCommand::CreateIngestionDataflow {
                id: ingestion_id,
                ingestion_description,
                as_of,
                resume_uppers,
                source_resume_uppers,
            } => {
                info!(
                    "worker {}/{} trying to (re-)start ingestion {ingestion_id} at resumption frontier {:?}",
                    self.timely_worker.index(),
                    self.timely_worker.peers(),
                    as_of
                );

                for (export_id, export) in ingestion_description.source_exports.iter() {
                    // This is a separate line cause rustfmt :(
                    let stats =
                        StorageStatistics::<SourceStatisticsUpdate, SourceStatisticsMetrics>::new(
                            *export_id,
                            self.storage_state.timely_worker_index,
                            &self.storage_state.source_metrics,
                            ingestion_id,
                            &export.storage_metadata.data_shard,
                        );
                    self.storage_state
                        .source_statistics
                        .insert(*export_id, stats);
                }

                for id in ingestion_description.subsource_ids() {
                    // If there is already a shared upper, we re-use it, to make
                    // sure that parties that are already using the shared upper
                    // can continue doing so.
                    let source_upper = self
                        .storage_state
                        .source_uppers
                        .entry(id.clone())
                        .or_insert_with(|| {
                            Rc::new(RefCell::new(Antichain::from_elem(Timestamp::minimum())))
                        });

                    let mut source_upper = source_upper.borrow_mut();
                    if !source_upper.is_empty() {
                        source_upper.clear();
                        source_upper.insert(mz_repr::Timestamp::minimum());
                    }
                }

                crate::render::build_ingestion_dataflow(
                    self.timely_worker,
                    &mut self.storage_state,
                    ingestion_id,
                    ingestion_description,
                    as_of,
                    resume_uppers,
                    source_resume_uppers,
                );
            }
            InternalStorageCommand::CreateSinkDataflow(sink_id, sink_description) => {
                info!(
                    "worker {}/{} trying to (re-)start sink {sink_id}",
                    self.timely_worker.index(),
                    self.timely_worker.peers(),
                );

                {
                    // If there is already a shared write frontier, we re-use it, to
                    // make sure that parties that are already using the shared
                    // frontier can continue doing so.
                    let sink_write_frontier = self
                        .storage_state
                        .sink_write_frontiers
                        .entry(sink_id.clone())
                        .or_insert_with(|| Rc::new(RefCell::new(Antichain::new())));

                    let mut sink_write_frontier = sink_write_frontier.borrow_mut();
                    sink_write_frontier.clear();
                    sink_write_frontier.insert(mz_repr::Timestamp::minimum());
                }
                // This is a separate line cause rustfmt :(
                let stats = StorageStatistics::<SinkStatisticsUpdate, SinkStatisticsMetrics>::new(
                    sink_id,
                    self.storage_state.timely_worker_index,
                    &self.storage_state.sink_metrics,
                );
                self.storage_state.sink_statistics.insert(sink_id, stats);

                crate::render::build_export_dataflow(
                    self.timely_worker,
                    &mut self.storage_state,
                    sink_id,
                    sink_description,
                );
            }
            InternalStorageCommand::DropDataflow(id) => {
                let ids: BTreeSet<GlobalId> = match self.storage_state.ingestions.get(&id) {
                    // Without the source dataflow running, all source exports
                    // should also be considered dropped. n.b. `source_exports`
                    // includes `id`
                    Some(IngestionDescription { source_exports, .. }) => {
                        source_exports.keys().cloned().collect()
                    }
                    None => {
                        let mut ids = BTreeSet::new();
                        ids.insert(id);
                        ids
                    }
                };

                mz_ore::soft_assert!(ids.contains(&id));

                for id in &ids {
                    // Clean up per-source / per-sink state.
                    self.storage_state.source_uppers.remove(id);
                    self.storage_state.source_tokens.remove(id);
                    self.storage_state.source_statistics.remove(id);

                    self.storage_state.sink_tokens.remove(id);
                }

                // The actual prometheus metrics and other state will be dropped
                // when the dataflow shuts down and drop's its `Rc`'s to the stats
                // objects. Also, these are always cloned during rendering,
                // but not inside dataflows, so we don't have TOCTOU issue here!
                self.storage_state.source_statistics.remove(&id);
                self.storage_state.sink_statistics.remove(&id);

                // Report the dataflow as dropped once we went through the whole
                // control flow from external command to this internal command.
                self.storage_state.dropped_ids.extend(ids);
            }
            InternalStorageCommand::UpdateConfiguration(pg, rocksdb) => {
                self.storage_state.dataflow_parameters.update(pg, rocksdb)
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
            let Some(reported_frontier) = self.storage_state.reported_frontiers.get_mut(id) else {
                // Frontier reporting has not yet been started for this object.
                // Potentially because this timely worker has not yet seen the
                // `CreateSources` command.
                continue;
            };

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

    /// Report source statistics back to the controller.
    pub fn report_storage_statistics(&mut self, response_tx: &ResponseSender) {
        // Check if any observed frontier should advance the reported frontiers.
        let mut source_stats = vec![];
        let mut sink_stats = vec![];
        for (_, stats) in self.storage_state.source_statistics.iter() {
            if let Some(snapshot) = stats.snapshot() {
                source_stats.push(snapshot);
            }
        }
        for (_, stats) in self.storage_state.sink_statistics.iter() {
            if let Some(snapshot) = stats.snapshot() {
                sink_stats.push(snapshot);
            }
        }

        if !source_stats.is_empty() || !sink_stats.is_empty() {
            self.send_storage_response(
                response_tx,
                StorageResponse::StatisticsUpdates(source_stats, sink_stats),
            );
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
    fn reconcile(
        &mut self,
        internal_cmd_tx: &mut dyn InternalCommandSender,
        async_worker: &mut AsyncStorageWorker<mz_repr::Timestamp>,
        command_rx: &CommandReceiver,
    ) {
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
        let mut stale_ingestions = self
            .storage_state
            .ingestions
            .keys()
            .collect::<BTreeSet<_>>();
        let mut stale_exports = self.storage_state.exports.keys().collect::<BTreeSet<_>>();

        let mut drop_commands = BTreeSet::new();
        let mut running_ingestion_descriptions = self.storage_state.ingestions.clone();

        for command in &mut commands {
            match command {
                StorageCommand::CreateTimely { .. } => {
                    panic!("CreateTimely must be captured before")
                }
                StorageCommand::AllowCompaction(sinces) => {
                    // collect all "drop commands". These are `AllowCompaction`
                    // commands that compact to the empty since. Then, later, we make sure
                    // we retain only those `Create*` commands that are not dropped. We
                    // assume that the `AllowCompaction` command is ordered after the
                    // `Create*` commands but don't assert that.
                    // WIP: Should we assert?
                    let drops = sinces.drain_filter_swapping(|(_id, since)| since.is_empty());
                    drop_commands.extend(drops.map(|(id, _since)| id));
                }
                StorageCommand::RunIngestions(ingestions) => {
                    // Ensure that ingestions are forward-rolling alter compatible.
                    for ingestion in ingestions {
                        let prev = running_ingestion_descriptions
                            .insert(ingestion.id, ingestion.description.clone());

                        if let Some(prev_ingest) = prev {
                            // If the new ingestion is not exactly equal to the currently running
                            // ingestion, we must either track that we need to synthesize an update
                            // command to change the ingestion, or panic.
                            prev_ingest
                                .alter_compatible(ingestion.id, &ingestion.description)
                                .expect("only alter compatible ingestions permitted");
                        }
                    }
                }
                StorageCommand::InitializationComplete
                | StorageCommand::UpdateConfiguration(_)
                | StorageCommand::CreateSinks(_) => (),
            }
        }

        let mut seen_most_recent_ingestion = BTreeSet::new();

        // We iterate over this backward to ensure that we keep only the most recent ingestion
        // description.
        for command in commands.iter_mut().rev() {
            match command {
                StorageCommand::CreateTimely { .. } => {
                    panic!("CreateTimely must be captured before")
                }
                StorageCommand::RunIngestions(ingestions) => {
                    ingestions.retain_mut(|ingestion| {
                        if drop_commands.remove(&ingestion.id)
                            || self.storage_state.dropped_ids.contains(&ingestion.id)
                        {
                            // Make sure that we report back that the ID was
                            // dropped.
                            self.storage_state.dropped_ids.insert(ingestion.id);

                            false
                        } else {
                            stale_ingestions.remove(&ingestion.id);

                            let running_ingestion =
                                self.storage_state.ingestions.get(&ingestion.id);

                            // Ingestion statements are only considered updates if they are
                            // currently running.
                            ingestion.update = running_ingestion.is_some();

                            // We keep only:
                            // - The most recent version of the ingestion, which
                            //   is why these commands are run in reverse.
                            // - Ingestions whose descriptions are not exactly
                            //   those that are currently running.
                            seen_most_recent_ingestion.insert(ingestion.id)
                                && running_ingestion != Some(&ingestion.description)
                        }
                    })
                }
                StorageCommand::CreateSinks(exports) => {
                    exports.retain_mut(|export| {
                        if drop_commands.remove(&export.id) {
                            // Make sure that we report back that the ID was
                            // dropped.
                            self.storage_state.dropped_ids.insert(export.id);

                            false
                        } else if let Some(existing) = self.storage_state.exports.get(&export.id) {
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
                StorageCommand::InitializationComplete
                | StorageCommand::UpdateConfiguration(_)
                | StorageCommand::AllowCompaction(_) => (),
            }
        }

        // Make sure all the "drop commands" matched up with a source or sink.
        // This is also what the regular handler logic for `AllowCompaction`
        // would do.
        assert!(
            drop_commands.is_empty(),
            "AllowCompaction commands for non-existent IDs {:?}",
            drop_commands
        );

        trace!(
            worker_id = self.timely_worker.index(),
            "reconciliation, stale ingestions: {:?}",
            stale_ingestions
        );

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
            self.storage_state.handle_storage_command(
                self.timely_worker.index(),
                internal_cmd_tx,
                async_worker,
                command,
            );
        }
    }
}

impl StorageState {
    /// Entry point for applying a storage command.
    ///
    /// NOTE: This does not have access to the timely worker and therefore
    /// cannot render dataflows. For dataflow rendering, this needs to either
    /// send asynchronous command to the given `async_worker` or internal
    /// commands to the given `internal_cmd_tx`.
    pub fn handle_storage_command(
        &mut self,
        worker_index: usize,
        internal_cmd_tx: &mut dyn InternalCommandSender,
        async_worker: &mut AsyncStorageWorker<mz_repr::Timestamp>,
        cmd: StorageCommand,
    ) {
        match cmd {
            StorageCommand::CreateTimely { .. } => panic!("CreateTimely must be captured before"),
            StorageCommand::InitializationComplete => (),
            StorageCommand::UpdateConfiguration(params) => {
                tracing::info!("Applying configuration update: {params:?}");
                params.persist.apply(self.persist_clients.cfg());

                // This needs to be broadcast by one worker and go through
                // the internal command fabric, to ensure consistent
                // ordering of dataflow rendering across all workers.
                if worker_index == 0 {
                    internal_cmd_tx.broadcast(InternalStorageCommand::UpdateConfiguration(
                        params.pg_replication_timeouts,
                        params.upsert_rocksdb_tuning_config,
                    ))
                }
            }
            StorageCommand::RunIngestions(ingestions) => {
                for RunIngestionCommand {
                    id,
                    description,
                    update,
                } in ingestions
                {
                    // Remember the ingestion description to facilitate possible
                    // reconciliation later.
                    let prev = self.ingestions.insert(id, description.clone());

                    assert!(
                        prev.is_some() == update,
                        "can only and must update reported frontiers if RunIngestion is update"
                    );

                    // Initialize shared frontier reporting.
                    for id in description.subsource_ids() {
                        match self.reported_frontiers.entry(id) {
                            Entry::Occupied(_) => {
                                assert!(update, "tried to re-insert frontier for {}", id)
                            }
                            Entry::Vacant(v) => {
                                v.insert(Antichain::from_elem(mz_repr::Timestamp::minimum()));
                            }
                        };
                    }

                    // This needs to be done by one worker, which will broadcasts a
                    // `CreateIngestionDataflow` command to all workers based on the response that
                    // contains the resumption upper.
                    //
                    // Doing this separately on each worker could lead to differing resume_uppers
                    // which might lead to all kinds of mayhem.
                    //
                    // n.b. the ingestion on each worker uses the description from worker 0––not the
                    // ingestion in the local storage state. This is something we might have
                    // interest in fixing in the future, e.g. #19907
                    if worker_index == 0 {
                        async_worker.update_frontiers(id, description);
                    }
                }
            }
            StorageCommand::CreateSinks(exports) => {
                for export in exports {
                    // Remember the sink description to facilitate possible
                    // reconciliation later.
                    self.exports.insert(export.id, export.description.clone());

                    self.reported_frontiers.insert(
                        export.id,
                        Antichain::from_elem(mz_repr::Timestamp::minimum()),
                    );

                    self.sink_handles.insert(
                        export.id,
                        SinkHandle::new(
                            export.id,
                            &export.description.from_storage_metadata,
                            export.description.from_storage_metadata.data_shard,
                            export.description.as_of.frontier.clone(),
                            Arc::clone(&self.persist_clients),
                        ),
                    );

                    // This needs to be broadcast by one worker and go through the internal command
                    // fabric, to ensure consistent ordering of dataflow rendering across all
                    // workers.
                    if worker_index == 0 {
                        internal_cmd_tx.broadcast(InternalStorageCommand::CreateSinkDataflow(
                            export.id,
                            export.description,
                        ));
                    }
                }
            }
            StorageCommand::AllowCompaction(list) => {
                for (id, frontier) in list {
                    match self.exports.get_mut(&id) {
                        Some(export_description) => {
                            // Update our knowledge of the `as_of`, in case we need to internally
                            // restart a sink in the future.
                            export_description.as_of.downgrade(&frontier);

                            // Sinks maintain a read handle over their input data to ensure that we
                            // can restart at the `as_of` that we store and update in the export
                            // description.
                            //
                            // Communication between the storage controller and this here worker is
                            // asynchronous, so we might learn that the controller downgraded a
                            // since after it has downgraded it's handle. Keeping a handle here
                            // ensures that we can restart based on our local state.
                            //
                            // NOTE: It's important that we only downgrade `SinkHandle` after
                            // updating the sink `as_of`.
                            let sink_handle =
                                self.sink_handles.get(&id).expect("missing SinkHandle");
                            sink_handle.downgrade_since(frontier.clone());
                        }
                        None if self.ingestions.contains_key(&id) => (),
                        None => panic!("AllowCompaction command for non-existent {id}"),
                    }

                    if frontier.is_empty() {
                        fail_point!("crash_on_drop");
                        // Indicates that we may drop `id`, as there are no more valid times to read.
                        //
                        // This handler removes state that is put in place by
                        // the handler for `RunIngestions`/`CreateSinks`, while
                        // the handler for the internal command does the same
                        // for the state put in place by its corresponding
                        // creation command.

                        // Cleanup exports and ingestions immediately to ensure
                        // they are not re-rendered in the case of
                        // reconciliation.
                        self.exports.remove(&id);
                        self.ingestions.remove(&id);

                        // This will stop reporting of frontiers.
                        self.reported_frontiers.remove(&id);

                        self.sink_handles.remove(&id);

                        // Broadcast from one worker to make sure its sequences
                        // with the other internal commands.
                        if worker_index == 0 {
                            internal_cmd_tx.broadcast(InternalStorageCommand::DropDataflow(id));
                        }
                    }
                }
            }
        }
    }
}
