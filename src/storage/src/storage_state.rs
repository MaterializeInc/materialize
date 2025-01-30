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
//! control/command fabric ([`internal_control`]). Internal commands go through
//! a sequencer dataflow that ensures that all workers receive all commands in
//! the same consistent order.
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

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;

use crossbeam_channel::{RecvError, TryRecvError};
use fail::fail_point;
use mz_ore::now::NowFn;
use mz_ore::tracing::TracingHandle;
use mz_ore::vec::VecExt;
use mz_ore::{soft_assert_or_log, soft_panic_or_log};
use mz_persist_client::batch::ProtoBatch;
use mz_persist_client::cache::PersistClientCache;
use mz_repr::{GlobalId, Timestamp};
use mz_rocksdb::config::SharedWriteBufferManager;
use mz_storage_client::client::{
    RunIngestionCommand, StatusUpdate, StorageCommand, StorageResponse,
};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::oneshot_sources::OneshotIngestionDescription;
use mz_storage_types::sinks::StorageSinkDesc;
use mz_storage_types::sources::IngestionDescription;
use mz_storage_types::AlterCompatible;
use mz_timely_util::builder_async::PressOnDropButton;
use mz_txn_wal::operator::TxnsContext;
use timely::communication::Allocate;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::progress::Timestamp as _;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tracing::{info, warn};

use crate::internal_control::{
    self, DataflowParameters, InternalCommandReceiver, InternalCommandSender,
    InternalStorageCommand,
};
use crate::metrics::StorageMetrics;
use crate::statistics::{AggregatedStatistics, SinkStatistics, SourceStatistics};
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
        mpsc::UnboundedSender<std::thread::Thread>,
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
            mpsc::UnboundedSender<std::thread::Thread>,
        )>,
        metrics: StorageMetrics,
        now: NowFn,
        connection_context: ConnectionContext,
        instance_context: StorageInstanceContext,
        persist_clients: Arc<PersistClientCache>,
        txns_ctx: TxnsContext,
        tracing_handle: Arc<TracingHandle>,
        shared_rocksdb_write_buffer_manager: SharedWriteBufferManager,
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
        let (internal_cmd_tx, internal_cmd_rx) =
            internal_control::setup_command_sequencer(timely_worker);

        let storage_configuration =
            StorageConfiguration::new(connection_context, mz_dyncfgs::all_dyncfgs());

        // We always initialize as read_only=true. Only when we're explicitly
        // allowed do we switch to doing writes.
        let (read_only_tx, read_only_rx) = watch::channel(true);

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
        let cluster_memory_limit = instance_context.cluster_memory_limit;

        let storage_state = StorageState {
            source_uppers: BTreeMap::new(),
            source_tokens: BTreeMap::new(),
            metrics,
            reported_frontiers: BTreeMap::new(),
            ingestions: BTreeMap::new(),
            exports: BTreeMap::new(),
            oneshot_ingestions: BTreeMap::new(),
            now,
            timely_worker_index: timely_worker.index(),
            timely_worker_peers: timely_worker.peers(),
            instance_context,
            persist_clients,
            txns_ctx,
            sink_tokens: BTreeMap::new(),
            sink_write_frontiers: BTreeMap::new(),
            dropped_ids: Vec::new(),
            aggregated_statistics: AggregatedStatistics::new(
                timely_worker.index(),
                timely_worker.peers(),
            ),
            object_status_updates: Default::default(),
            internal_cmd_tx,
            internal_cmd_rx,
            read_only_tx,
            read_only_rx,
            async_worker,
            storage_configuration,
            dataflow_parameters: DataflowParameters::new(
                shared_rocksdb_write_buffer_manager,
                cluster_memory_limit,
            ),
            tracing_handle,
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
    /// NB: The type of the tokens must not be changed to something other than `PressOnDropButton`
    /// to prevent usage of custom shutdown tokens that are tricky to get right.
    pub source_tokens: BTreeMap<GlobalId, Vec<PressOnDropButton>>,
    /// Metrics for storage objects.
    pub metrics: StorageMetrics,
    /// Tracks the conditional write frontiers we have reported.
    pub reported_frontiers: BTreeMap<GlobalId, Antichain<Timestamp>>,
    /// Descriptions of each installed ingestion.
    pub ingestions: BTreeMap<GlobalId, IngestionDescription<CollectionMetadata>>,
    /// Descriptions of each installed export.
    pub exports: BTreeMap<GlobalId, StorageSinkDesc<CollectionMetadata, mz_repr::Timestamp>>,
    /// Descriptions of oneshot ingestions that are currently running.
    pub oneshot_ingestions: BTreeMap<uuid::Uuid, OneshotIngestionDescription<ProtoBatch>>,
    /// Undocumented
    pub now: NowFn,
    /// Index of the associated timely dataflow worker.
    pub timely_worker_index: usize,
    /// Peers in the associated timely dataflow worker.
    pub timely_worker_peers: usize,
    /// Other configuration for sources and sinks.
    pub instance_context: StorageInstanceContext,
    /// A process-global cache of (blob_uri, consensus_uri) -> PersistClient.
    /// This is intentionally shared between workers
    pub persist_clients: Arc<PersistClientCache>,
    /// Context necessary for rendering txn-wal operators.
    pub txns_ctx: TxnsContext,
    /// Tokens that should be dropped when a dataflow is dropped to clean up
    /// associated state.
    /// NB: The type of the tokens must not be changed to something other than `PressOnDropButton`
    /// to prevent usage of custom shutdown tokens that are tricky to get right.
    pub sink_tokens: BTreeMap<GlobalId, Vec<PressOnDropButton>>,
    /// Frontier of sink writes (all subsequent writes will be at times at or
    /// equal to this frontier)
    pub sink_write_frontiers: BTreeMap<GlobalId, Rc<RefCell<Antichain<Timestamp>>>>,
    /// Collection ids that have been dropped but not yet reported as dropped
    pub dropped_ids: Vec<GlobalId>,

    /// Statistics for sources and sinks.
    pub aggregated_statistics: AggregatedStatistics,

    /// Status updates reported by health operators.
    ///
    /// **NOTE**: Operators that append to this collection should take care to only add new
    /// status updates if the status of the ingestion/export in question has _changed_.
    pub object_status_updates: Rc<RefCell<Vec<StatusUpdate>>>,

    /// Sender for cluster-internal storage commands. These can be sent from
    /// within workers/operators and will be distributed to all workers. For
    /// example, for shutting down an entire dataflow from within a
    /// operator/worker.
    pub internal_cmd_tx: InternalCommandSender,
    /// Receiver for cluster-internal storage commands.
    pub internal_cmd_rx: InternalCommandReceiver,

    /// When this replica/cluster is in read-only mode it must not affect any
    /// changes to external state. This flag can only be changed by a
    /// [StorageCommand::AllowWrites].
    ///
    /// Everything running on this replica/cluster must obey this flag. At the
    /// time of writing, nothing currently looks at this flag.
    /// TODO(benesch): fix this.
    ///
    /// NOTE: In the future, we might want a more complicated flag, for example
    /// something that tells us after which timestamp we are allowed to write.
    /// In this first version we are keeping things as simple as possible!
    pub read_only_rx: watch::Receiver<bool>,

    /// Send-side for read-only state.
    pub read_only_tx: watch::Sender<bool>,

    /// Async worker companion, used for running code that requires async, which
    /// the timely main loop cannot do.
    pub async_worker: AsyncStorageWorker<mz_repr::Timestamp>,

    /// Configuration for source and sink connections.
    pub storage_configuration: StorageConfiguration,
    /// Dynamically configurable parameters that control how dataflows are rendered.
    /// NOTE(guswynn): we should consider moving these into `storage_configuration`.
    pub dataflow_parameters: DataflowParameters,

    /// A process-global handle to tracing configuration.
    pub tracing_handle: Arc<TracingHandle>,
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
    /// The memory limit of the materialize cluster replica. This will
    /// be used to calculate and configure the maximum inflight bytes for backpressure
    pub cluster_memory_limit: Option<usize>,
}

impl StorageInstanceContext {
    /// Build a new `StorageInstanceContext`.
    pub fn new(
        scratch_directory: Option<PathBuf>,
        cluster_memory_limit: Option<usize>,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self {
            scratch_directory,
            rocksdb_env: rocksdb::Env::new()?,
            cluster_memory_limit,
        })
    }

    /// Constructs a new connection context for usage in tests.
    pub fn for_tests(rocksdb_env: rocksdb::Env) -> Self {
        Self {
            scratch_directory: None,
            rocksdb_env,
            cluster_memory_limit: None,
        }
    }
}

impl<'w, A: Allocate> Worker<'w, A> {
    /// Waits for client connections and runs them to completion.
    pub fn run(&mut self) {
        let mut shutdown = false;
        while !shutdown {
            match self.client_rx.recv() {
                Ok((rx, tx, activator_tx)) => {
                    // This might fail if the client has already shut down, which is fine.
                    // `run_client` knows how to handle a disconnected client.
                    let _ = activator_tx.send(std::thread::current());
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
        // At this point, all workers are still reading from the command flow.
        if self.reconcile(&command_rx).is_err() {
            return;
        }

        // The last time we reported statistics.
        let mut last_stats_time = Instant::now();

        let mut disconnected = false;
        while !disconnected {
            let config = &self.storage_state.storage_configuration;
            let stats_interval = config.parameters.statistics_collection_interval;

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
            if command_rx.is_empty() && self.storage_state.async_worker.is_empty() {
                // Make sure we wake up again to report any pending statistics updates.
                let park_duration = stats_interval.saturating_sub(last_stats_time.elapsed());
                self.timely_worker.step_or_park(Some(park_duration));
            } else {
                self.timely_worker.step();
            }

            // Rerport any dropped ids
            for id in std::mem::take(&mut self.storage_state.dropped_ids) {
                self.send_storage_response(&response_tx, StorageResponse::DroppedId(id));
            }

            self.report_frontier_progress(&response_tx);
            self.process_oneshot_ingestions(&response_tx);

            // Report status updates if any are present
            if self.storage_state.object_status_updates.borrow().len() > 0 {
                self.send_storage_response(
                    &response_tx,
                    StorageResponse::StatusUpdates(self.storage_state.object_status_updates.take()),
                );
            }

            if last_stats_time.elapsed() >= stats_interval {
                self.report_storage_statistics(&response_tx);
                last_stats_time = Instant::now();
            }

            // Handle any received commands.
            loop {
                match command_rx.try_recv() {
                    Ok(cmd) => self.storage_state.handle_storage_command(cmd),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        disconnected = true;
                        break;
                    }
                }
            }

            // Handle responses from the async worker.
            while let Ok(response) = self.storage_state.async_worker.try_recv() {
                self.handle_async_worker_response(response);
            }

            // Handle any received commands.
            while let Some(command) = self.storage_state.internal_cmd_rx.try_recv() {
                self.handle_internal_storage_command(command);
            }
        }
    }

    /// Entry point for applying a response from the async storage worker.
    pub fn handle_async_worker_response(
        &self,
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
                self.storage_state.internal_cmd_tx.send(
                    InternalStorageCommand::CreateIngestionDataflow {
                        id,
                        ingestion_description,
                        as_of,
                        resume_uppers,
                        source_resume_uppers,
                    },
                );
            }
        }
    }

    /// Entry point for applying an internal storage command.
    pub fn handle_internal_storage_command(&mut self, internal_cmd: InternalStorageCommand) {
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
                        for (id, _) in ingestion_description.source_exports.iter() {
                            self.storage_state
                                .aggregated_statistics
                                .advance_global_epoch(*id);
                        }
                        self.storage_state
                            .async_worker
                            .update_frontiers(id, ingestion_description);
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
                        self.storage_state
                            .aggregated_statistics
                            .advance_global_epoch(id);
                        self.storage_state.internal_cmd_tx.send(
                            InternalStorageCommand::RunSinkDataflow(id, sink_description),
                        );
                    }

                    // Continue with other commands.
                    return;
                }

                if !self
                    .storage_state
                    .ingestions
                    .values()
                    .any(|v| v.source_exports.contains_key(&id))
                {
                    // Our current approach to dropping a source results in a race between shard
                    // finalization (which happens in the controller) and dataflow shutdown (which
                    // happens in clusterd). If a source is created and dropped fast enough -or the
                    // two commands get sufficiently delayed- then it's possible to receive a
                    // SuspendAndRestart command for an unknown source. We cannot assert that this
                    // never happens but we log an error here to track how often this happens.
                    warn!("got InternalStorageCommand::SuspendAndRestart for something that is not a source or sink: {id}");
                }
            }
            InternalStorageCommand::CreateIngestionDataflow {
                id: ingestion_id,
                ingestion_description,
                as_of,
                resume_uppers,
                source_resume_uppers,
            } => {
                info!(
                    ?as_of,
                    ?resume_uppers,
                    "worker {}/{} trying to (re-)start ingestion {ingestion_id}",
                    self.timely_worker.index(),
                    self.timely_worker.peers(),
                );

                for (export_id, export) in ingestion_description.source_exports.iter() {
                    let resume_upper = resume_uppers[export_id].clone();
                    self.storage_state.aggregated_statistics.initialize_source(
                        *export_id,
                        resume_upper.clone(),
                        || {
                            SourceStatistics::new(
                                *export_id,
                                self.storage_state.timely_worker_index,
                                &self.storage_state.metrics.source_statistics,
                                ingestion_id,
                                &export.storage_metadata.data_shard,
                                export.data_config.envelope.clone(),
                                resume_upper,
                            )
                        },
                    );
                }

                for id in ingestion_description.collection_ids() {
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

                // If all subsources of the source are finished, we can skip rendering entirely.
                // Also, if `as_of` is empty, the dataflow has been finalized, so we can skip it as
                // well.
                //
                // TODO(guswynn|petrosagg): this is a bit hacky, and is a consequence of storage state
                // management being a bit of a mess. we should clean this up and remove weird if
                // statements like this.
                if resume_uppers.values().all(|frontier| frontier.is_empty()) || as_of.is_empty() {
                    tracing::info!(
                        ?resume_uppers,
                        ?as_of,
                        "worker {}/{} skipping building ingestion dataflow \
                        for {ingestion_id} because the ingestion is finished",
                        self.timely_worker.index(),
                        self.timely_worker.peers(),
                    );
                    return;
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
            InternalStorageCommand::RunOneshotIngestion {
                ingestion_id,
                collection_id,
                collection_meta,
                request,
            } => {
                crate::render::build_oneshot_ingestion_dataflow(
                    self.timely_worker,
                    &mut self.storage_state,
                    ingestion_id,
                    collection_id,
                    collection_meta,
                    request,
                );
            }
            InternalStorageCommand::RunSinkDataflow(sink_id, sink_description) => {
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
                self.storage_state
                    .aggregated_statistics
                    .initialize_sink(sink_id, || {
                        SinkStatistics::new(
                            sink_id,
                            self.storage_state.timely_worker_index,
                            &self.storage_state.metrics.sink_statistics,
                        )
                    });

                crate::render::build_export_dataflow(
                    self.timely_worker,
                    &mut self.storage_state,
                    sink_id,
                    sink_description,
                );
            }
            InternalStorageCommand::DropDataflow(ids) => {
                for id in &ids {
                    // Clean up per-source / per-sink state.
                    self.storage_state.source_uppers.remove(id);
                    self.storage_state.source_tokens.remove(id);

                    self.storage_state.sink_tokens.remove(id);

                    self.storage_state.aggregated_statistics.deinitialize(*id);
                }
            }
            InternalStorageCommand::UpdateConfiguration { storage_parameters } => {
                self.storage_state
                    .dataflow_parameters
                    .update(storage_parameters.clone());
                self.storage_state
                    .storage_configuration
                    .update(storage_parameters);

                // Clear out the updates as we no longer forward them to anyone else to process.
                // We clone `StorageState::storage_configuration` many times during rendering
                // and want to avoid cloning these unused updates.
                self.storage_state
                    .storage_configuration
                    .parameters
                    .dyncfg_updates = Default::default();
            }
            InternalStorageCommand::StatisticsUpdate { sources, sinks } => self
                .storage_state
                .aggregated_statistics
                .ingest(sources, sinks),
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
        let (sources, sinks) = self.storage_state.aggregated_statistics.emit_local();
        if !sources.is_empty() || !sinks.is_empty() {
            self.storage_state
                .internal_cmd_tx
                .send(InternalStorageCommand::StatisticsUpdate { sources, sinks })
        }

        let (sources, sinks) = self.storage_state.aggregated_statistics.snapshot();
        if !sources.is_empty() || !sinks.is_empty() {
            self.send_storage_response(
                response_tx,
                StorageResponse::StatisticsUpdates(sources, sinks),
            );
        }
    }

    /// Send a response to the coordinator.
    fn send_storage_response(&self, response_tx: &ResponseSender, response: StorageResponse) {
        // Ignore send errors because the coordinator is free to ignore our
        // responses. This happens during shutdown.
        let _ = response_tx.send(response);
    }

    fn process_oneshot_ingestions(&mut self, response_tx: &ResponseSender) {
        use tokio::sync::mpsc::error::TryRecvError;

        let mut to_remove = vec![];

        for (ingestion_id, ingestion_state) in &mut self.storage_state.oneshot_ingestions {
            loop {
                match ingestion_state.results.try_recv() {
                    Ok(result) => {
                        let response = match result {
                            Ok(maybe_batch) => maybe_batch.into_iter().map(Result::Ok).collect(),
                            Err(err) => vec![Err(err)],
                        };
                        let staged_batches = BTreeMap::from([(*ingestion_id, response)]);
                        let _ = response_tx.send(StorageResponse::StagedBatches(staged_batches));
                    }
                    Err(TryRecvError::Empty) => {
                        break;
                    }
                    Err(TryRecvError::Disconnected) => {
                        to_remove.push(*ingestion_id);
                        break;
                    }
                }
            }
        }

        for ingestion_id in to_remove {
            tracing::info!(?ingestion_id, "removing oneshot ingestion");
            self.storage_state.oneshot_ingestions.remove(&ingestion_id);
        }
    }

    /// Extract commands until `InitializationComplete`, and make the worker
    /// reflect those commands. If the worker can not be made to reflect the
    /// commands, return an error.
    fn reconcile(&mut self, command_rx: &CommandReceiver) -> Result<(), RecvError> {
        let worker_id = self.timely_worker.index();

        // To initialize the connection, we want to drain all commands until we
        // receive a `StorageCommand::InitializationComplete` command to form a
        // target command state.
        let mut commands = vec![];
        loop {
            match command_rx.recv()? {
                StorageCommand::InitializationComplete => break,
                command => commands.push(command),
            }
        }

        // Track which frontiers this envd expects; we will also set their
        // initial timestamp to the minimum timestamp to reset them as we don't
        // know what frontiers the new envd expects.
        let mut expected_objects = BTreeSet::new();

        let mut drop_commands = BTreeSet::new();
        let mut running_ingestion_descriptions = self.storage_state.ingestions.clone();
        let mut running_exports_descriptions = self.storage_state.exports.clone();

        for command in &mut commands {
            match command {
                StorageCommand::CreateTimely { .. } => {
                    panic!("CreateTimely must be captured before")
                }
                StorageCommand::AllowCompaction(sinces) => {
                    info!(%worker_id, ?sinces, "reconcile: received AllowCompaction command");

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
                    info!(%worker_id, ?ingestions, "reconcile: received RunIngestions command");

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
                StorageCommand::RunOneshotIngestion(oneshot) => {
                    info!(%worker_id, ?oneshot, "reconcile: received RunOneshotIngestion command");
                    // TODO(cf1): Handle CancelOneshotIngestion, clean out stale oneshot
                    // ingestions from our state. Possibly here we respond to the client
                    // with a cancelation to make sure the client doesn't wait forever.
                }
                StorageCommand::RunSinks(exports) => {
                    info!(%worker_id, ?exports, "reconcile: received RunSinks command");

                    // Ensure that exports are forward-rolling alter compatible.
                    for export in exports {
                        let prev = running_exports_descriptions
                            .insert(export.id, export.description.clone());

                        if let Some(prev_export) = prev {
                            prev_export
                                .alter_compatible(export.id, &export.description)
                                .expect("only alter compatible ingestions permitted");
                        }
                    }
                }
                StorageCommand::InitializationComplete
                | StorageCommand::AllowWrites
                | StorageCommand::UpdateConfiguration(_) => (),
            }
        }

        let mut seen_most_recent_definition = BTreeSet::new();

        // We iterate over this backward to ensure that we keep only the most recent ingestion
        // description.
        for command in commands.iter_mut().rev() {
            match command {
                StorageCommand::CreateTimely { .. } => {
                    panic!("CreateTimely must be captured before")
                }
                StorageCommand::RunIngestions(ingestions) => {
                    ingestions.retain_mut(|ingestion| {
                        // Subsources can be dropped independently of their
                        // primary source, so we evaluate them in a separate
                        // loop.
                        for export_id in ingestion
                            .description
                            .source_exports
                            .keys()
                            .filter(|export_id| **export_id != ingestion.id)
                        {
                            if drop_commands.remove(export_id) {
                                info!(%worker_id, %export_id, "reconcile: dropping subsource");
                                self.storage_state.dropped_ids.push(*export_id);
                            }
                        }

                        if drop_commands.remove(&ingestion.id)
                            || self.storage_state.dropped_ids.contains(&ingestion.id)
                        {
                            info!(%worker_id, %ingestion.id, "reconcile: dropping ingestion");

                            // If an ingestion is dropped, so too must all of
                            // its subsources (i.e. ingestion exports, as well
                            // as its progress subsource).
                            for id in ingestion.description.collection_ids() {
                                drop_commands.remove(&id);
                                self.storage_state.dropped_ids.push(id);
                            }

                            false
                        } else {
                            let most_recent_defintion =
                                seen_most_recent_definition.insert(ingestion.id);

                            if most_recent_defintion {
                                // If this is the most recent definition, this
                                // is what we will be running when
                                // reconciliation completes. This definition
                                // must not include any dropped subsources.
                                ingestion.description.source_exports.retain(|export_id, _| {
                                    !self.storage_state.dropped_ids.contains(export_id)
                                });

                                // After clearing any dropped subsources, we can
                                // state that we expect all of these to exist.
                                expected_objects.extend(ingestion.description.collection_ids());
                            }

                            let running_ingestion =
                                self.storage_state.ingestions.get(&ingestion.id);

                            // We keep only:
                            // - The most recent version of the ingestion, which
                            //   is why these commands are run in reverse.
                            // - Ingestions whose descriptions are not exactly
                            //   those that are currently running.
                            most_recent_defintion
                                && running_ingestion != Some(&ingestion.description)
                        }
                    })
                }
                StorageCommand::RunSinks(exports) => {
                    exports.retain_mut(|export| {
                        if drop_commands.remove(&export.id)
                            // If there were multiple `RunSinks` in the command
                            // stream, we want to ensure none of them are
                            // retained.
                            || self.storage_state.dropped_ids.contains(&export.id)
                        {
                            info!(%worker_id, %export.id, "reconcile: dropping sink");

                            // Make sure that we report back that the ID was
                            // dropped.
                            self.storage_state.dropped_ids.push(export.id);

                            false
                        } else {
                            expected_objects.insert(export.id);

                            let running_sink = self.storage_state.exports.get(&export.id);

                            // We keep only:
                            // - The most recent version of the sink, which
                            //   is why these commands are run in reverse.
                            // - Sinks whose descriptions are not exactly
                            //   those that are currently running.
                            seen_most_recent_definition.insert(export.id)
                                && running_sink != Some(&export.description)
                        }
                    })
                }
                StorageCommand::InitializationComplete
                | StorageCommand::AllowWrites
                | StorageCommand::UpdateConfiguration(_)
                | StorageCommand::AllowCompaction(_)
                | StorageCommand::RunOneshotIngestion(_) => (),
            }
        }

        // Make sure all the "drop commands" matched up with a source or sink.
        // This is also what the regular handler logic for `AllowCompaction`
        // would do.
        soft_assert_or_log!(
            drop_commands.is_empty(),
            "AllowCompaction commands for non-existent IDs {:?}",
            drop_commands
        );

        // Determine the ID of all objects we did _not_ see; these are
        // considered stale.
        let stale_objects = self
            .storage_state
            .ingestions
            .values()
            .map(|i| i.collection_ids())
            .flatten()
            .chain(self.storage_state.exports.keys().copied())
            // Objects are considered stale if we did not see them re-created.
            .filter(|id| !expected_objects.contains(id))
            .collect::<Vec<_>>();

        info!(
            %worker_id, ?expected_objects, ?stale_objects,
            "reconcile: modifing storage state to match expected objects",
        );

        for id in stale_objects {
            self.storage_state.drop_collection(id);
        }

        // Do not report dropping any objects that do not belong to expected
        // objects.
        self.storage_state
            .dropped_ids
            .retain(|id| expected_objects.contains(id));

        // Do not report any frontiers that do not belong to expected objects.
        // Note that this set of objects can differ from the set of sources and
        // sinks.
        self.storage_state
            .reported_frontiers
            .retain(|id, _| expected_objects.contains(id));

        // Reset the reported frontiers for the remaining objects.
        for (_, frontier) in &mut self.storage_state.reported_frontiers {
            *frontier = Antichain::from_elem(<_>::minimum());
        }

        // Execute the modified commands.
        for command in commands {
            self.storage_state.handle_storage_command(command);
        }

        Ok(())
    }
}

impl StorageState {
    /// Entry point for applying a storage command.
    ///
    /// NOTE: This does not have access to the timely worker and therefore
    /// cannot render dataflows. For dataflow rendering, this needs to either
    /// send asynchronous command to the `async_worker` or internal
    /// commands to the `internal_cmd_tx`.
    pub fn handle_storage_command(&mut self, cmd: StorageCommand) {
        match cmd {
            StorageCommand::CreateTimely { .. } => panic!("CreateTimely must be captured before"),
            StorageCommand::InitializationComplete => (),
            StorageCommand::AllowWrites => {
                self.read_only_tx
                    .send(false)
                    .expect("we're holding one other end");
                self.persist_clients.cfg().enable_compaction();
            }
            StorageCommand::UpdateConfiguration(params) => {
                // These can be done from all workers safely.
                tracing::info!("Applying configuration update: {params:?}");

                // We serialize the dyncfg updates in StorageParameters, but configure
                // persist separately.
                self.persist_clients
                    .cfg()
                    .apply_from(&params.dyncfg_updates);

                params.tracing.apply(self.tracing_handle.as_ref());

                if let Some(log_filter) = &params.tracing.log_filter {
                    self.storage_configuration
                        .connection_context
                        .librdkafka_log_level =
                        mz_ore::tracing::crate_level(&log_filter.clone().into(), "librdkafka");
                }

                // This needs to be broadcast by one worker and go through
                // the internal command fabric, to ensure consistent
                // ordering of dataflow rendering across all workers.
                if self.timely_worker_index == 0 {
                    self.internal_cmd_tx
                        .send(InternalStorageCommand::UpdateConfiguration {
                            storage_parameters: params,
                        })
                }
            }
            StorageCommand::RunIngestions(ingestions) => {
                for RunIngestionCommand { id, description } in ingestions {
                    // Remember the ingestion description to facilitate possible
                    // reconciliation later.
                    self.ingestions.insert(id, description.clone());

                    // Initialize shared frontier reporting.
                    for id in description.collection_ids() {
                        self.reported_frontiers
                            .entry(id)
                            .or_insert(Antichain::from_elem(mz_repr::Timestamp::minimum()));
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
                    // interest in fixing in the future, e.g. materialize#19907
                    if self.timely_worker_index == 0 {
                        self.async_worker.update_frontiers(id, description);
                    }
                }
            }
            StorageCommand::RunOneshotIngestion(oneshot) => {
                if self.timely_worker_index == 0 {
                    self.internal_cmd_tx
                        .send(InternalStorageCommand::RunOneshotIngestion {
                            ingestion_id: oneshot.ingestion_id,
                            collection_id: oneshot.collection_id,
                            collection_meta: oneshot.collection_meta,
                            request: oneshot.request,
                        });
                }
            }
            StorageCommand::RunSinks(exports) => {
                for export in exports {
                    // Remember the sink description to facilitate possible
                    // reconciliation later.
                    let prev = self.exports.insert(export.id, export.description.clone());

                    // New sink, add state.
                    if prev.is_none() {
                        self.reported_frontiers.insert(
                            export.id,
                            Antichain::from_elem(mz_repr::Timestamp::minimum()),
                        );
                    }

                    // This needs to be broadcast by one worker and go through the internal command
                    // fabric, to ensure consistent ordering of dataflow rendering across all
                    // workers.
                    if self.timely_worker_index == 0 {
                        self.internal_cmd_tx
                            .send(InternalStorageCommand::RunSinkDataflow(
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
                            export_description.as_of.clone_from(&frontier);
                        }
                        // reported_frontiers contains both ingestions and their
                        // exports
                        None if self.reported_frontiers.contains_key(&id) => (),
                        None => {
                            soft_panic_or_log!("AllowCompaction command for non-existent {id}");
                            continue;
                        }
                    }

                    if frontier.is_empty() {
                        // Indicates that we may drop `id`, as there are no more valid times to read.
                        self.drop_collection(id);
                    }
                }
            }
        }
    }

    /// Drop the identified storage collection from the storage state.
    fn drop_collection(&mut self, id: GlobalId) {
        fail_point!("crash_on_drop");

        self.ingestions.remove(&id);
        self.exports.remove(&id);

        // This will stop reporting of frontiers.
        //
        // If this object still has its frontiers reported, we will notify the
        // client envd of the drop.
        if self.reported_frontiers.remove(&id).is_some() {
            // The only actions left are internal cleanup, so we can commit to
            // the client that these objects have been dropped.
            //
            // This must be done now rather than in response to `DropDataflow`,
            // otherwise we introduce the possibility of a timing issue where:
            // - We remove all tracking state from the storage state and send
            //   `DropDataflow` (i.e. this block).
            // - While waiting to process that command, we reconcile with a new
            //   envd. That envd has already committed to its catalog that this
            //   object no longer exists.
            // - We process the `DropDataflow` command, and identify that this
            //   object has been dropped.
            // - The next time `dropped_ids` is processed, we send a response
            //   that this ID has been dropped, but the upstream state has no
            //   record of that object having ever existed.
            self.dropped_ids.push(id);
        }

        // Broadcast from one worker to make sure its sequences with the other internal commands.
        if self.timely_worker_index == 0 {
            self.internal_cmd_tx
                .send(InternalStorageCommand::DropDataflow(vec![id]));
        }
    }
}
