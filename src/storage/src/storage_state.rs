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
//! ## Example flow of commands for `RunIngestion`
//!
//! With external commands, internal commands, and the async worker,
//! understanding where and how commands from the controller are realized can
//! get complicated. We will follow the complete flow for `RunIngestion`, as an
//! example:
//!
//! 1. Worker receives a [`StorageCommand::RunIngestion`] command from the
//!    controller.
//! 2. This command is processed in [`StorageState::handle_storage_command`].
//!    This step cannot render dataflows, because it does not have access to the
//!    timely worker. It will only set up state that stays over the whole
//!    lifetime of the source, such as the `reported_frontier`. Putting in place
//!    this reported frontier will enable frontier reporting for that source. We
//!    will not start reporting when we only see an internal command for
//!    rendering a dataflow, which can "overtake" the external `RunIngestion`
//!    command.
//! 3. During processing of that command, we call
//!    [`AsyncStorageWorker::update_ingestion_frontiers`], which causes a command to
//!    be sent to the async worker.
//! 4. We eventually get a response from the async worker:
//!    [`AsyncStorageWorkerResponse::IngestionFrontiersUpdated`].
//! 5. This response is handled in [`Worker::handle_async_worker_response`].
//! 6. Handling that response causes a
//!    [`InternalStorageCommand::CreateIngestionDataflow`] to be broadcast to
//!    all workers via the internal command fabric.
//! 7. This message will be processed (on each worker) in
//!    [`Worker::handle_internal_storage_command`]. This is what will cause the
//!    required dataflow to be rendered on all workers.
//!
//! The process described above assumes that the `RunIngestion` is _not_ an
//! update, i.e. it is in response to a `CREATE SOURCE`-like statement.
//!
//! The primary distinction when handling a `RunIngestion` that represents an
//! update, is that it might fill out new internal state in the mid-level
//! clients on the way toward being run.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use fail::fail_point;
use mz_ore::now::NowFn;
use mz_ore::soft_assert_or_log;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::batch::ProtoBatch;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::operators::shard_source::ErrorHandler;
use mz_repr::{GlobalId, Timestamp};
use mz_rocksdb::config::SharedWriteBufferManager;
use mz_storage_client::client::{
    RunIngestionCommand, RunOneshotIngestion, StatusUpdate, StorageCommand, StorageResponse,
};
use mz_storage_types::AlterCompatible;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::dyncfgs::STORAGE_SERVER_MAINTENANCE_INTERVAL;
use mz_storage_types::oneshot_sources::OneshotIngestionDescription;
use mz_storage_types::sinks::StorageSinkDesc;
use mz_timely_util::builder_async::PressOnDropButton;
use mz_txn_wal::operator::TxnsContext;
use timely::order::PartialOrder;
use timely::progress::Timestamp as _;
use timely::progress::frontier::Antichain;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::internal_control::{
    self, DataflowParameters, InternalCommandReceiver, InternalCommandSender,
    InternalStorageCommand,
};
use crate::metrics::StorageMetrics;
use crate::statistics::{AggregatedStatistics, SinkStatistics, SourceStatistics};
use crate::storage_state::async_storage_worker::{AsyncStorageWorker, AsyncStorageWorkerResponse};

pub mod async_storage_worker;

type CommandReceiver = mpsc::UnboundedReceiver<StorageCommand>;
type ResponseSender = mpsc::UnboundedSender<StorageResponse>;

/// A local endpoint is classified by its first worker-visible command.
struct Peer {
    commands: CommandReceiver,
    responses: ResponseSender,
    query: Option<bool>,
}

/// Lives in the sequencer's order, independently of local endpoint arrival.
#[derive(Default)]
struct Query {
    /// Retained until disconnect to suppress repeated runs and runs overtaken by
    /// cancellation, even after the ingestion itself has been reclaimed.
    seen: BTreeSet<Uuid>,
    pending: BTreeMap<Uuid, Box<RunOneshotIngestion>>,
    /// Terminal callbacks observed in the common order, one per worker.
    finished: BTreeMap<Uuid, usize>,
    responses: Vec<StorageResponse>,
    observations: bool,
}

/// State maintained for each worker thread.
///
/// Much of this state can be viewed as local variables for the worker thread,
/// holding state that persists across function calls.
pub struct Worker<'w> {
    /// The underlying Timely worker.
    ///
    /// NOTE: This is `pub` for testing.
    pub timely_worker: &'w mut TimelyWorker,
    /// The channel over which communication handles for newly connected clients
    /// are delivered.
    pub client_rx: mpsc::UnboundedReceiver<(Uuid, CommandReceiver, ResponseSender)>,
    /// The state associated with collection ingress and egress.
    pub storage_state: StorageState,
    peers: BTreeMap<Uuid, Peer>,
    lifecycle: Option<Uuid>,
    initialization: Option<Vec<StorageCommand>>,
    queries: BTreeMap<Uuid, Query>,
    query_ready: bool,
    replica_commands: Option<mpsc::UnboundedReceiver<crate::replica::ReplicaCommand>>,
    replica_progress: Option<mz_cluster::replica_progress::Sender<crate::replica::WorkerResponse>>,
}

impl<'w> Worker<'w> {
    /// Creates new `Worker` state from the given components.
    pub fn new(
        timely_worker: &'w mut TimelyWorker,
        client_rx: mpsc::UnboundedReceiver<(Uuid, CommandReceiver, ResponseSender)>,
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
        // of the client loop creates a new async worker.
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
        // they must survive across client connections.

        // TODO(aljoscha): This thread unparking business seems brittle, but that's
        // also how the command channel works currently. We can wrap it inside a
        // struct that holds both a channel and a `Thread`, but I don't
        // think that would help too much.
        let async_worker = async_storage_worker::AsyncStorageWorker::new(
            thread::current(),
            Arc::clone(&persist_clients),
        );
        let cluster_memory_limit = instance_context.cluster_memory_limit;

        let storage_state = StorageState {
            executions: None,
            source_uppers: BTreeMap::new(),
            source_tokens: BTreeMap::new(),
            metrics,
            reported_frontiers: BTreeMap::new(),
            ingestions: BTreeMap::new(),
            exports: BTreeMap::new(),
            oneshot_ingestions: BTreeMap::new(),
            query_owners: BTreeMap::new(),
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
            shared_status_updates: Default::default(),
            latest_status_updates: Default::default(),
            initial_status_reported: Default::default(),
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
            server_maintenance_interval: Duration::ZERO,
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
            peers: BTreeMap::new(),
            lifecycle: None,
            initialization: None,
            queries: BTreeMap::new(),
            query_ready: false,
            replica_commands: None,
            replica_progress: None,
        }
    }

    /// Installs native ingress and progress in the existing worker runtime.
    /// Call once on every worker, before running or accepting connections.
    pub(crate) fn enable_replica(
        &mut self,
    ) -> (
        mpsc::UnboundedSender<crate::replica::ReplicaCommand>,
        mpsc::UnboundedReceiver<(usize, crate::replica::WorkerResponse)>,
    ) {
        assert!(self.replica_progress.is_none());
        let (progress, responses) = mz_cluster::replica_progress::render(self.timely_worker);
        self.replica_progress = Some(progress);
        self.storage_state.executions = Some(Default::default());
        let (commands, receiver) = mpsc::unbounded_channel();
        if self.timely_worker.index() == 0 {
            self.replica_commands = Some(receiver);
        }
        (commands, responses)
    }
}

/// Worker-local state related to the ingress or egress of collections of data.
pub struct StorageState {
    pub(crate) executions: Option<crate::replica::Executions>,
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
    /// Commands for each installed ingestion, retained for reconciliation and restart.
    pub ingestions: BTreeMap<GlobalId, RunIngestionCommand>,
    /// Descriptions of each installed export.
    pub exports: BTreeMap<GlobalId, StorageSinkDesc<CollectionMetadata, mz_repr::Timestamp>>,
    /// Descriptions of oneshot ingestions that are currently running.
    pub oneshot_ingestions: BTreeMap<uuid::Uuid, OneshotIngestionDescription<ProtoBatch>>,
    /// Query ownership outlives local completion, until all workers finish.
    /// Lifecycle reconciliation and legacy cancellation must not touch these IDs.
    query_owners: BTreeMap<Uuid, Uuid>,
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

    /// A place shared with running dataflows, so that health operators, can
    /// report status updates back to us.
    ///
    /// **NOTE**: Operators that append to this collection should take care to only add new
    /// status updates if the status of the ingestion/export in question has _changed_.
    pub shared_status_updates: Rc<RefCell<Vec<StatusUpdate>>>,

    /// The latest status update for each object.
    pub latest_status_updates: BTreeMap<GlobalId, StatusUpdate>,

    /// Whether we have reported the initial status after connecting to a new client.
    /// This is reset to false when a new client connects.
    pub initial_status_reported: bool,

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

    /// Interval at which to perform server maintenance tasks. Set to a zero interval to
    /// perform maintenance with every `step_or_park` invocation.
    pub server_maintenance_interval: Duration,
}

impl StorageState {
    /// Resolve export errors to the ingestion attempt that owns their readers.
    pub(crate) fn execution_owner(&self, id: GlobalId) -> GlobalId {
        self.ingestions
            .iter()
            .find_map(|(owner, run)| {
                run.description
                    .source_exports
                    .contains_key(&id)
                    .then_some(*owner)
            })
            .unwrap_or(id)
    }

    pub(crate) fn execution(&self, id: GlobalId) -> Option<u64> {
        self.executions
            .as_ref()?
            .current
            .get(&self.execution_owner(id))
            .copied()
    }

    fn finish_startup(&mut self, execution: Option<u64>) {
        if let Some(execution) = execution {
            self.executions
                .as_mut()
                .expect("native execution")
                .started(execution);
        }
    }

    /// Return an error handler that triggers a suspend and restart of the corresponding storage
    /// dataflow.
    pub fn error_handler(&self, context: &'static str, id: GlobalId) -> ErrorHandler {
        let tx = self.internal_cmd_tx.clone();
        let execution = self.execution(id);
        let id = if execution.is_some() {
            self.execution_owner(id)
        } else {
            id
        };
        ErrorHandler::signal(move |e| {
            tx.send(InternalStorageCommand::SuspendAndRestart {
                execution,
                id,
                reason: format!("{context}: {e:#}"),
            })
        })
    }
}

/// Extra context for a storage instance.
/// This is extra information that is used when rendering source
/// and sinks that is not tied to the source/connection configuration itself.
#[derive(Clone)]
pub struct StorageInstanceContext {
    /// A directory that can be used for scratch work.
    pub scratch_directory: Option<PathBuf>,
    /// The memory limit of the materialize cluster replica. This will
    /// be used to calculate and configure the maximum inflight bytes for backpressure
    pub cluster_memory_limit: Option<usize>,
}

impl StorageInstanceContext {
    /// Build a new `StorageInstanceContext`.
    pub fn new(scratch_directory: Option<PathBuf>, cluster_memory_limit: Option<usize>) -> Self {
        Self {
            scratch_directory,
            cluster_memory_limit,
        }
    }

    /// Returns a `rocksdb::Env` for a new RocksDB instance.
    ///
    /// With a scratch directory this is the default `Env`, which stores data
    /// on the host filesystem. Without one, RocksDB runs in memory, and every
    /// call returns a fresh in-memory `Env`. State written through an `Env`
    /// is only reachable through that same `Env`, so a per-instance `Env`
    /// isolates instances from each other and from previous incarnations of
    /// themselves. Background threads are process-wide either way, both
    /// variants delegate them to the default `Env`.
    pub fn rocksdb_env(&self) -> Result<rocksdb::Env, rocksdb::Error> {
        if self.scratch_directory.is_some() {
            rocksdb::Env::new()
        } else {
            rocksdb::Env::mem_env()
        }
    }
}

impl<'w> Worker<'w> {
    /// Services lifecycle and query endpoints without blocking on initialization.
    pub fn run(&mut self) {
        // The last time we reported statistics.
        let mut last_stats_time = Instant::now();

        // The last time we did periodic maintenance.
        let mut last_maintenance = std::time::Instant::now();
        let (discard_responses, _) = mpsc::unbounded_channel();

        loop {
            // Native ingress joins the same global order as queries, restarts,
            // and async worker responses before mutating worker bookkeeping.
            if let Some(commands) = &mut self.replica_commands {
                for _ in 0..commands.len() + 1 {
                    match commands.try_recv() {
                        Ok(command) => self
                            .storage_state
                            .internal_cmd_tx
                            .send(InternalStorageCommand::Replica(command)),
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => panic!("replica storage ingress lost"),
                    }
                }
            }
            self.poll_clients();
            // Client disconnection alone must not stop maintained work. Closing the
            // container's endpoint channel, with no clients left, ends the worker.
            if self.replica_progress.is_none()
                && self.client_rx.is_closed()
                && self.client_rx.is_empty()
                && self.peers.is_empty()
            {
                return;
            }
            // A disconnected lifecycle may ignore responses. Query results are routed
            // separately and maintained work continues while no lifecycle is attached.
            let response_tx = self
                .lifecycle
                .filter(|_| self.initialization.is_none())
                .and_then(|n| self.peers.get(&n))
                .map(|p| p.responses.clone())
                .unwrap_or_else(|| discard_responses.clone());
            let config = &self.storage_state.storage_configuration;
            let stats_interval = config.parameters.statistics_collection_interval;

            let maintenance_interval = self.storage_state.server_maintenance_interval;

            let now = std::time::Instant::now();
            // Determine if we need to perform maintenance, which is true if `maintenance_interval`
            // time has passed since the last maintenance.
            let sleep_duration;
            if now >= last_maintenance + maintenance_interval {
                last_maintenance = now;
                sleep_duration = None;

                self.report_frontier_progress(&response_tx);
                if let Some(executions) = &mut self.storage_state.executions {
                    for response in executions.report() {
                        self.replica_progress
                            .as_ref()
                            .expect("native progress")
                            .send(response.into());
                    }
                }
            } else {
                // We didn't perform maintenance, sleep until the next maintenance interval.
                let next_maintenance = last_maintenance + maintenance_interval;
                sleep_duration = Some(next_maintenance.saturating_duration_since(now))
            }

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
            if self.client_rx.is_empty()
                && self
                    .replica_commands
                    .as_ref()
                    .is_none_or(|rx| rx.is_empty())
                && self.peers.values().all(|p| p.commands.is_empty())
                && self.storage_state.async_worker.is_empty()
            {
                // Make sure we wake up again to report any pending statistics updates.
                let mut park_duration = stats_interval.saturating_sub(last_stats_time.elapsed());
                if let Some(sleep_duration) = sleep_duration {
                    park_duration = std::cmp::min(sleep_duration, park_duration);
                }
                self.timely_worker.step_or_park(Some(park_duration));
            } else {
                self.timely_worker.step();
            }

            // Rerport any dropped ids
            self.report_dropped_ids(&response_tx);

            self.process_oneshot_ingestions(&response_tx);

            self.report_status_updates(&response_tx);

            if last_stats_time.elapsed() >= stats_interval {
                self.report_storage_statistics(&response_tx);
                last_stats_time = Instant::now();
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

    fn poll_clients(&mut self) {
        for _ in 0..self.client_rx.len() {
            let Ok((nonce, commands, responses)) = self.client_rx.try_recv() else {
                break;
            };
            self.peers.entry(nonce).or_insert(Peer {
                commands,
                responses,
                query: None,
            });
        }
        let nonces: Vec<_> = self.peers.keys().copied().collect();
        for nonce in nonces {
            // Bound each turn by the queued work, so a continuous producer cannot
            // prevent sibling clients or Timely from making progress. The extra
            // receive observes disconnect even when the queue starts empty.
            let budget = self.peers.get(&nonce).map_or(0, |p| p.commands.len() + 1);
            for _ in 0..budget {
                let Some(peer) = self.peers.get_mut(&nonce) else {
                    break;
                };
                let command = match peer.commands.try_recv() {
                    Ok(command) => Some(command),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => None,
                };
                if peer.query.is_none() {
                    let Some(first) = &command else {
                        self.peers.remove(&nonce);
                        break;
                    };
                    let query = matches!(first, StorageCommand::HelloQuery { .. });
                    if self.replica_progress.is_some() && !query {
                        self.peers.remove(&nonce);
                        break;
                    }
                    peer.query = Some(query);
                    if !query {
                        if let Some(old) = self.lifecycle.replace(nonce) {
                            if old != nonce {
                                self.peers.remove(&old);
                            }
                        }
                        self.initialization = Some(Vec::new());
                    }
                }
                let query = self.peers[&nonce].query.expect("classified");
                let disconnected = command.is_none();
                if query {
                    // Only worker zero admits commands, including disconnect. Other
                    // process endpoints can arrive after globally sequenced responses.
                    if self.timely_worker.index() == 0 {
                        self.storage_state
                            .internal_cmd_tx
                            .send(InternalStorageCommand::Query { nonce, command });
                    }
                } else if let Some(command) = command {
                    if let Some(commands) = &mut self.initialization {
                        if matches!(command, StorageCommand::InitializationComplete) {
                            let commands = self.initialization.take().expect("initializing");
                            self.reconcile(commands);
                            if self.timely_worker.index() == 0 {
                                self.storage_state
                                    .internal_cmd_tx
                                    .send(InternalStorageCommand::QueryReady);
                            }
                        } else {
                            commands.push(command);
                        }
                    } else {
                        self.storage_state.handle_storage_command(command);
                    }
                }
                if disconnected {
                    self.peers.remove(&nonce);
                    if self.lifecycle == Some(nonce) {
                        self.lifecycle = None;
                        self.initialization = None;
                    }
                    break;
                }
            }
        }
        self.flush_query_responses();
    }

    fn flush_query_responses(&mut self) {
        for (nonce, query) in &mut self.queries {
            if let Some(peer) = self.peers.get(nonce).filter(|p| p.query == Some(true)) {
                for response in query.responses.drain(..) {
                    let _ = peer.responses.send(response);
                }
            }
        }
    }

    fn handle_query(&mut self, nonce: Uuid, command: Option<StorageCommand>) {
        match command {
            Some(StorageCommand::HelloQuery { nonce: hello }) => {
                assert_eq!(nonce, hello);
                if let std::collections::btree_map::Entry::Vacant(entry) = self.queries.entry(nonce)
                {
                    let query = entry.insert(Query::default());
                    if self.query_ready {
                        query.responses.push(StorageResponse::QueryReady);
                    }
                }
            }
            Some(StorageCommand::SubscribeObservations) => {
                if !self.query_ready {
                    // A malformed query peer must not kill maintained work.
                    self.handle_query(nonce, None);
                    return;
                }
                let Some(query) = self.queries.get_mut(&nonce) else {
                    return;
                };
                // The client waits for aggregate readiness before subscribing, so
                // every process has its endpoint and current state available.
                if !query.observations {
                    query.observations = true;
                    let now = mz_ore::now::to_datetime((self.storage_state.now)());
                    query.responses.extend(
                        self.storage_state
                            .latest_status_updates
                            .values()
                            .cloned()
                            .map(|mut update| {
                                update.timestamp = now.clone();
                                StorageResponse::StatusUpdate(update)
                            }),
                    );
                }
            }
            None => {
                self.queries.remove(&nonce);
                self.peers.remove(&nonce);
                self.storage_state.query_owners.retain(|id, owner| {
                    if *owner == nonce {
                        self.storage_state.oneshot_ingestions.remove(id);
                        false
                    } else {
                        true
                    }
                });
            }
            Some(StorageCommand::RunOneshotIngestion(ingestion)) => {
                let Some(query) = self.queries.get_mut(&nonce) else {
                    return;
                };
                let id = ingestion.ingestion_id;
                if !query.seen.insert(id) {
                    return;
                }
                // IDs are replica-wide, including legacy oneshots. A collision must
                // never replace or expose another connection's ingestion.
                if self.storage_state.query_owners.contains_key(&id)
                    || self.storage_state.oneshot_ingestions.contains_key(&id)
                {
                    query
                        .responses
                        .push(StorageResponse::StagedBatches(BTreeMap::from([(
                            id,
                            vec![Err("oneshot ingestion ID is already in use".into())],
                        )])));
                    return;
                }
                self.storage_state.query_owners.insert(id, nonce);
                query.pending.insert(id, ingestion);
            }
            Some(StorageCommand::CancelOneshotIngestion(id)) => {
                if let Some(query) = self.queries.get_mut(&nonce) {
                    // Remember cancellation even when it overtakes admission.
                    query.seen.insert(id);
                    query.pending.remove(&id);
                    query.finished.remove(&id);
                    if self.storage_state.query_owners.get(&id) == Some(&nonce) {
                        self.storage_state.drop_oneshot_ingestion(id);
                        self.storage_state.query_owners.remove(&id);
                    }
                }
            }
            Some(_) => panic!("invalid query command passed role validation"),
        }
        self.admit_queries();
        self.flush_query_responses();
    }

    fn admit_queries(&mut self) {
        if !self.query_ready {
            return;
        }
        for query in self.queries.values_mut() {
            for ingestion in std::mem::take(&mut query.pending).into_values() {
                crate::render::build_oneshot_ingestion_dataflow(
                    self.timely_worker,
                    &mut self.storage_state,
                    ingestion.ingestion_id,
                    ingestion.collection_id,
                    ingestion.collection_meta,
                    ingestion.request,
                );
            }
        }
    }

    /// Entry point for applying a response from the async storage worker.
    pub fn handle_async_worker_response(
        &self,
        async_response: AsyncStorageWorkerResponse<mz_repr::Timestamp>,
    ) {
        // NOTE: If we want to share the load of async processing we
        // have to change `handle_storage_command` and change this
        // assert.
        assert_eq!(
            self.timely_worker.index(),
            0,
            "only worker #0 is doing async processing"
        );
        match async_response {
            AsyncStorageWorkerResponse::IngestionFrontiersUpdated {
                execution,
                id,
                ingestion_description,
                as_of,
                resume_uppers,
                source_resume_uppers,
            } => {
                self.storage_state.internal_cmd_tx.send(
                    InternalStorageCommand::CreateIngestionDataflow {
                        execution,
                        id,
                        ingestion_description,
                        as_of,
                        resume_uppers,
                        source_resume_uppers,
                    },
                );
            }
            AsyncStorageWorkerResponse::ExportFrontiersUpdated { id, description } => {
                // Native reconstruction requires a newly authorized Run command.
                assert!(self.storage_state.executions.is_none());
                self.storage_state
                    .internal_cmd_tx
                    .send(InternalStorageCommand::RunSinkDataflow(
                        None,
                        id,
                        description,
                    ));
            }
            AsyncStorageWorkerResponse::DropDataflow(id) => {
                self.storage_state
                    .internal_cmd_tx
                    .send(InternalStorageCommand::DropDataflow(vec![id]));
            }
        }
    }

    /// Entry point for applying an internal storage command.
    pub fn handle_internal_storage_command(&mut self, internal_cmd: InternalStorageCommand) {
        match internal_cmd {
            InternalStorageCommand::Replica(
                crate::replica::ReplicaCommand::KafkaPreOpenReply {
                    request,
                    execution,
                    id,
                    max_age,
                },
            ) => {
                self.storage_state
                    .executions
                    .as_mut()
                    .expect("native runtime")
                    .kafka_pre_open_reply(request, execution, id, max_age);
            }
            InternalStorageCommand::KafkaPreOpen {
                request,
                execution,
                id,
            } => {
                // Exactly one response, regardless of which process runs the sink.
                if self.timely_worker.index() == 0 {
                    self.replica_progress
                        .as_ref()
                        .expect("native runtime")
                        .send(
                            crate::replica::ReplicaStorageResponse::KafkaPreOpen {
                                request,
                                execution,
                                id,
                            }
                            .into(),
                        );
                }
            }
            InternalStorageCommand::Replica(crate::replica::ReplicaCommand::Storage(
                sequence,
                command,
            )) => {
                assert!(self.replica_progress.is_some());
                let dropping = match &command {
                    StorageCommand::AllowCompaction(_, frontier) => frontier.is_empty(),
                    _ => false,
                };
                if !dropping {
                    self.storage_state
                        .executions
                        .as_mut()
                        .unwrap()
                        .outputs
                        .observe(sequence, &command);
                }
                if let Some((id, inputs)) = crate::replica::inputs(&command) {
                    // Keep existing execution during preparation. Rendering the
                    // replacement drops its tokens, without releasing old reads.
                    self.storage_state
                        .executions
                        .as_mut()
                        .unwrap()
                        .start(sequence, id, inputs);
                }
                if matches!(command, StorageCommand::InitializationComplete) {
                    // Configuration's rendering-stage command was enqueued by
                    // worker zero while processing preceding native ingress.
                    // Put readiness behind it, not at this first-stage barrier.
                    if self.timely_worker.index() == 0 {
                        self.storage_state
                            .internal_cmd_tx
                            .send(InternalStorageCommand::QueryReady);
                    }
                } else {
                    self.storage_state.handle_storage_command(command);
                }
            }
            InternalStorageCommand::Query { nonce, command } => self.handle_query(nonce, command),
            InternalStorageCommand::QueryFinished {
                nonce,
                ingestion_id,
            } => {
                if self.storage_state.query_owners.get(&ingestion_id) == Some(&nonce) {
                    let query = self.queries.get_mut(&nonce).expect("owner exists");
                    let finished = query.finished.entry(ingestion_id).or_default();
                    *finished += 1;
                    if *finished == self.timely_worker.peers() {
                        query.finished.remove(&ingestion_id);
                        self.storage_state.drop_oneshot_ingestion(ingestion_id);
                        self.storage_state.query_owners.remove(&ingestion_id);
                    }
                }
            }
            InternalStorageCommand::CancelOneshotIngestion(id) => {
                if !self.storage_state.query_owners.contains_key(&id) {
                    self.storage_state.drop_oneshot_ingestion(id);
                }
            }
            InternalStorageCommand::QueryReady => {
                if !self.query_ready {
                    self.query_ready = true;
                    for query in self.queries.values_mut() {
                        query.responses.push(StorageResponse::QueryReady);
                    }
                    self.admit_queries();
                    self.flush_query_responses();
                }
            }
            InternalStorageCommand::SuspendAndRestart {
                execution,
                id,
                reason,
            } => {
                info!(
                    ?execution,
                    "worker {}/{} initiating suspend-and-restart for {id} because of: {reason}",
                    self.timely_worker.index(),
                    self.timely_worker.peers(),
                );
                if let Some(executions) = &mut self.storage_state.executions {
                    let Some(execution) = execution else { return };
                    if executions.current.get(&id) != Some(&execution) {
                        return;
                    }
                    // Removing admission fences late startup and coalesces all
                    // workers' requests for this attempt. Only a new Run can restart.
                    executions.retire(id);
                    self.storage_state.source_tokens.remove(&id);
                    self.storage_state.sink_tokens.remove(&id);
                    self.replica_progress.as_ref().unwrap().send(
                        crate::server::ReplicaStorageResponse::RestartRequested { execution, id }
                            .into(),
                    );
                    return;
                }

                let maybe_ingestion = self.storage_state.ingestions.get(&id).cloned();
                if let Some(ingestion) = maybe_ingestion {
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
                        for (id, _) in ingestion.description.source_exports.iter() {
                            self.storage_state
                                .aggregated_statistics
                                .advance_global_epoch(*id);
                        }
                        self.storage_state
                            .async_worker
                            .update_ingestion_frontiers(ingestion);
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
                        self.storage_state
                            .async_worker
                            .update_sink_frontiers(id, sink_description);
                    }

                    // Continue with other commands.
                    return;
                }

                if !self
                    .storage_state
                    .ingestions
                    .values()
                    .any(|v| v.description.source_exports.contains_key(&id))
                {
                    // Our current approach to dropping a source results in a race between shard
                    // finalization (which happens in the controller) and dataflow shutdown (which
                    // happens in clusterd). If a source is created and dropped fast enough -or the
                    // two commands get sufficiently delayed- then it's possible to receive a
                    // SuspendAndRestart command for an unknown source. We cannot assert that this
                    // never happens but we log an error here to track how often this happens.
                    warn!(
                        "got InternalStorageCommand::SuspendAndRestart for something that is not a source or sink: {id}"
                    );
                }
            }
            InternalStorageCommand::CreateIngestionDataflow {
                execution,
                id: ingestion_id,
                mut ingestion_description,
                as_of,
                mut resume_uppers,
                mut source_resume_uppers,
            } => {
                if execution.is_some() && self.storage_state.execution(ingestion_id) != execution {
                    self.storage_state.finish_startup(execution);
                    return;
                }
                if execution.is_some() {
                    self.storage_state.source_tokens.remove(&ingestion_id);
                }
                info!(
                    ?as_of,
                    ?resume_uppers,
                    "worker {}/{} trying to (re-)start ingestion {ingestion_id}",
                    self.timely_worker.index(),
                    self.timely_worker.peers(),
                );

                // We initialize statistics before we prune finished exports. We
                // still want to export statistics for these, plus the rendering
                // machinery will get confused if there are not at least
                // statistics for the "main" source.
                for (export_id, export) in ingestion_description.source_exports.iter() {
                    if execution.is_some() && self.timely_worker.index() == 0 {
                        self.storage_state
                            .aggregated_statistics
                            .advance_global_epoch(*export_id);
                    }
                    let resume_upper = resume_uppers[export_id].clone();
                    self.storage_state.aggregated_statistics.initialize_source(
                        *export_id,
                        ingestion_id,
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

                let finished_exports: BTreeSet<GlobalId> = resume_uppers
                    .iter()
                    .filter(|(_, frontier)| frontier.is_empty())
                    .map(|(id, _)| *id)
                    .collect();

                resume_uppers.retain(|id, _| !finished_exports.contains(id));
                source_resume_uppers.retain(|id, _| !finished_exports.contains(id));
                ingestion_description
                    .source_exports
                    .retain(|id, _| !finished_exports.contains(id));

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
                    info!(
                        ?resume_uppers,
                        ?as_of,
                        "worker {}/{} skipping building ingestion dataflow \
                        for {ingestion_id} because the ingestion is finished",
                        self.timely_worker.index(),
                        self.timely_worker.peers(),
                    );
                    self.storage_state.finish_startup(execution);
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
                self.storage_state.finish_startup(execution);
            }
            InternalStorageCommand::RunOneshotIngestion {
                ingestion_id,
                collection_id,
                collection_meta,
                request,
            } => {
                if self.storage_state.query_owners.contains_key(&ingestion_id) {
                    return;
                }
                crate::render::build_oneshot_ingestion_dataflow(
                    self.timely_worker,
                    &mut self.storage_state,
                    ingestion_id,
                    collection_id,
                    collection_meta,
                    request,
                );
            }
            InternalStorageCommand::RunSinkDataflow(execution, sink_id, sink_description) => {
                if execution.is_some() && self.storage_state.execution(sink_id) != execution {
                    self.storage_state.finish_startup(execution);
                    return;
                }
                if execution.is_some() {
                    self.storage_state.sink_tokens.remove(&sink_id);
                }
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
                if execution.is_some() && self.timely_worker.index() == 0 {
                    self.storage_state
                        .aggregated_statistics
                        .advance_global_epoch(sink_id);
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
                self.storage_state.finish_startup(execution);
            }
            InternalStorageCommand::DropDataflow(ids) => {
                for id in &ids {
                    // Clean up per-source / per-sink state.
                    self.storage_state.source_uppers.remove(id);
                    self.storage_state.source_tokens.remove(id);

                    self.storage_state.sink_tokens.remove(id);
                    self.storage_state.sink_write_frontiers.remove(id);

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

                // Remember the maintenance interval locally to avoid reading it from the config set on
                // every server iteration.
                self.storage_state.server_maintenance_interval =
                    STORAGE_SERVER_MAINTENANCE_INTERVAL
                        .get(self.storage_state.storage_configuration.config_set());

                // Apply storage's upsert spill flag to both stash flavors'
                // mechanisms: the storage leg of the process-wide chunk
                // spill gate (chunked flavor) and the storage-owned column
                // pager (paged flavor). The buffer pool, the pager pool, and
                // their budgets are the shared ones configured by compute's
                // `apply_worker_config` (compute and storage run in the same
                // process). The chunk gate ORs storage's leg with compute's,
                // so chunks spill while either subsystem's flag is set.
                //
                // The flag is replica-scoped: the storage controller merges
                // per-replica overrides into the `UpdateConfiguration`
                // commands it sends, so reading this worker's `ConfigSet`
                // here observes them.
                {
                    use mz_storage_types::dyncfgs::ENABLE_UPSERT_PAGED_SPILL;

                    let enabled = ENABLE_UPSERT_PAGED_SPILL
                        .get(self.storage_state.storage_configuration.config_set());
                    debug!(
                        worker = self.timely_worker.index(),
                        enabled, "upsert stash spill: applying gate",
                    );
                    crate::upsert::upsert_stash_spill::set_enabled(enabled);
                    crate::upsert::upsert_stash_pager::set_enabled(enabled);
                }
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

        for (id, upper) in new_uppers {
            self.send_storage_response(response_tx, StorageResponse::FrontierUpper(id, upper));
        }
    }

    /// Pumps latest status updates from the buffer shared with operators and
    /// reports any updates that need reporting.
    pub fn report_status_updates(&mut self, response_tx: &ResponseSender) {
        // If we haven't done the initial status report, report all current statuses
        if !self.storage_state.initial_status_reported {
            // We pull initially reported status updates to "now", so that they
            // sort as the latest update in internal status collections. This
            // makes it so that a newly bootstrapped envd can append status
            // updates to internal status collections that report an accurate
            // view as of the time when they came up.
            let now_ts = mz_ore::now::to_datetime((self.storage_state.now)());
            let status_updates = self
                .storage_state
                .latest_status_updates
                .values()
                .cloned()
                .map(|mut update| {
                    update.timestamp = now_ts.clone();
                    update
                })
                .collect::<Vec<_>>();
            for update in status_updates {
                self.send_storage_response(response_tx, StorageResponse::StatusUpdate(update));
            }
            self.storage_state.initial_status_reported = true;
        }

        // Pump updates into our state and stage them for reporting.
        for shared_update in self.storage_state.shared_status_updates.take() {
            self.send_storage_response(
                response_tx,
                StorageResponse::StatusUpdate(shared_update.clone()),
            );

            self.storage_state
                .latest_status_updates
                .insert(shared_update.id, shared_update);
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

        // Snapshot resets global counters. Keep aggregating at constant memory
        // through same-process observer outages, without consuming their deltas.
        if self.replica_progress.is_some() && !self.queries.values().any(|q| q.observations) {
            return;
        }
        let (sources, sinks) = self.storage_state.aggregated_statistics.snapshot();
        if !sources.is_empty() || !sinks.is_empty() {
            self.send_storage_response(
                response_tx,
                StorageResponse::StatisticsUpdates(sources, sinks),
            );
        }
    }

    fn report_dropped_ids(&mut self, response_tx: &ResponseSender) {
        if let Some(executions) = &mut self.storage_state.executions {
            self.storage_state.dropped_ids.clear();
            for (id, generation) in std::mem::take(&mut executions.dropped_outputs) {
                self.replica_progress
                    .as_ref()
                    .unwrap()
                    .send(crate::replica::WorkerResponse {
                        output_generation: Some(generation),
                        response: crate::server::ReplicaStorageResponse::Response(
                            StorageResponse::DroppedId(id),
                        ),
                    });
            }
        } else {
            for id in std::mem::take(&mut self.storage_state.dropped_ids) {
                self.send_storage_response(response_tx, StorageResponse::DroppedId(id));
            }
        }
    }

    /// Send a response to the coordinator.
    fn send_storage_response(&mut self, response_tx: &ResponseSender, response: StorageResponse) {
        if matches!(
            response,
            StorageResponse::StatusUpdate(_) | StorageResponse::StatisticsUpdates(..)
        ) {
            for query in self.queries.values_mut().filter(|q| q.observations) {
                query.responses.push(response.clone());
            }
            self.flush_query_responses();
            if self.replica_progress.is_some() {
                return;
            }
        }
        if let Some(progress) = &self.replica_progress {
            let output_generation = if let StorageResponse::FrontierUpper(id, _) = &response {
                Some(self.storage_state.executions.as_ref().unwrap().outputs.0[id])
            } else {
                None
            };
            progress.send(crate::replica::WorkerResponse {
                output_generation,
                response: crate::server::ReplicaStorageResponse::Response(response),
            });
            return;
        }
        // Ignore send errors because the coordinator is free to ignore our
        // responses. This happens during shutdown.
        let _ = response_tx.send(response);
    }

    fn process_oneshot_ingestions(&mut self, response_tx: &ResponseSender) {
        for (ingestion_id, ingestion_state) in &mut self.storage_state.oneshot_ingestions {
            if !self.storage_state.query_owners.contains_key(ingestion_id)
                && (self.lifecycle.is_none() || self.initialization.is_some())
            {
                continue;
            }
            loop {
                match ingestion_state.results.try_recv() {
                    Ok(result) => {
                        let response = match result {
                            Ok(maybe_batch) => maybe_batch.into_iter().map(Result::Ok).collect(),
                            Err(err) => vec![Err(err)],
                        };
                        let staged_batches = BTreeMap::from([(*ingestion_id, response)]);
                        let response = StorageResponse::StagedBatches(staged_batches);
                        if let Some(owner) = self.storage_state.query_owners.get(ingestion_id) {
                            if let Some(query) = self.queries.get_mut(owner) {
                                query.responses.push(response);
                            }
                            // The renderer invokes each worker's callback exactly once.
                            // Releasing local tokens here could freeze capabilities
                            // still needed for another worker's terminal result.
                            self.storage_state.internal_cmd_tx.send(
                                InternalStorageCommand::QueryFinished {
                                    nonce: *owner,
                                    ingestion_id: *ingestion_id,
                                },
                            );
                        } else {
                            let _ = response_tx.send(response);
                        }
                    }
                    Err(TryRecvError::Empty) => {
                        break;
                    }
                    Err(TryRecvError::Disconnected) => {
                        break;
                    }
                }
            }
        }
        self.flush_query_responses();
    }

    /// Reconcile a complete lifecycle snapshot without touching query-owned work.
    fn reconcile(&mut self, mut commands: Vec<StorageCommand>) {
        let worker_id = self.timely_worker.index();

        // Track which frontiers this envd expects; we will also set their
        // initial timestamp to the minimum timestamp to reset them as we don't
        // know what frontiers the new envd expects.
        let mut expected_objects = BTreeSet::new();

        let mut drop_commands = BTreeSet::new();
        let mut running_ingestions = self.storage_state.ingestions.clone();
        let mut running_exports_descriptions = self.storage_state.exports.clone();

        let mut create_oneshot_ingestions: BTreeSet<Uuid> = BTreeSet::new();
        let mut cancel_oneshot_ingestions: BTreeSet<Uuid> = BTreeSet::new();

        for command in &mut commands {
            match command {
                StorageCommand::Hello { .. }
                | StorageCommand::HelloQuery { .. }
                | StorageCommand::SubscribeObservations => {
                    panic!("transport and query commands must be captured before")
                }
                StorageCommand::AllowCompaction(id, since) => {
                    info!(%worker_id, ?id, ?since, "reconcile: received AllowCompaction command");

                    // collect all "drop commands". These are `AllowCompaction`
                    // commands that compact to the empty since. Then, later, we make sure
                    // we retain only those `Create*` commands that are not dropped. We
                    // assume that the `AllowCompaction` command is ordered after the
                    // `Create*` commands but don't assert that.
                    // WIP: Should we assert?
                    if since.is_empty() {
                        drop_commands.insert(*id);
                    }
                }
                StorageCommand::RunIngestion(ingestion) => {
                    info!(%worker_id, ?ingestion, "reconcile: received RunIngestion command");

                    // Ensure that ingestions are forward-rolling alter compatible.
                    let prev = running_ingestions.insert(ingestion.id, ingestion.as_ref().clone());

                    if let Some(prev_ingest) = prev {
                        // If the new ingestion is not exactly equal to the currently running
                        // ingestion, we must either track that we need to synthesize an update
                        // command to change the ingestion, or panic.
                        prev_ingest
                            .description
                            .alter_compatible(ingestion.id, &ingestion.description)
                            .expect("only alter compatible ingestions permitted");
                    }
                }
                StorageCommand::RunSink(export) => {
                    info!(%worker_id, ?export, "reconcile: received RunSink command");

                    // Ensure that exports are forward-rolling alter compatible.
                    let prev =
                        running_exports_descriptions.insert(export.id, export.description.clone());

                    if let Some(prev_export) = prev {
                        prev_export
                            .alter_compatible(export.id, &export.description)
                            .expect("only alter compatible exports permitted");
                    }
                }
                StorageCommand::RunOneshotIngestion(ingestion) => {
                    info!(
                        %worker_id,
                        ingestion_id = %ingestion.ingestion_id,
                        collection_id = %ingestion.collection_id,
                        "reconcile: received RunOneshotIngestion command",
                    );
                    create_oneshot_ingestions.insert(ingestion.ingestion_id);
                }
                StorageCommand::CancelOneshotIngestion(uuid) => {
                    info!(%worker_id, %uuid, "reconcile: received CancelOneshotIngestion command");
                    cancel_oneshot_ingestions.insert(*uuid);
                }
                StorageCommand::InitializationComplete
                | StorageCommand::AllowWrites
                | StorageCommand::UpdateConfiguration(_) => (),
            }
        }

        let mut seen_most_recent_definition = BTreeSet::new();

        // We iterate over this backward to ensure that we keep only the most recent ingestion
        // description.
        let mut filtered_commands = VecDeque::new();
        for mut command in commands.into_iter().rev() {
            let mut should_keep = true;
            match &mut command {
                StorageCommand::Hello { .. }
                | StorageCommand::HelloQuery { .. }
                | StorageCommand::SubscribeObservations => {
                    panic!("transport and query commands must be captured before")
                }
                StorageCommand::RunIngestion(ingestion) => {
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
                        should_keep = false;
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

                        let running_ingestion = self.storage_state.ingestions.get(&ingestion.id);

                        // We keep only:
                        // - The most recent version of the ingestion, which
                        //   is why these commands are run in reverse.
                        // - Ingestions whose commands are not exactly
                        //   those that are currently running.
                        should_keep =
                            most_recent_defintion && running_ingestion != Some(ingestion.as_ref())
                    }
                }
                StorageCommand::RunSink(export) => {
                    if drop_commands.remove(&export.id)
                        // If there were multiple `RunSink` in the command
                        // stream, we want to ensure none of them are
                        // retained.
                        || self.storage_state.dropped_ids.contains(&export.id)
                    {
                        info!(%worker_id, %export.id, "reconcile: dropping sink");

                        // Make sure that we report back that the ID was
                        // dropped.
                        self.storage_state.dropped_ids.push(export.id);

                        should_keep = false
                    } else {
                        expected_objects.insert(export.id);

                        let running_sink = self.storage_state.exports.get(&export.id);

                        // We keep only:
                        // - The most recent version of the sink, which
                        //   is why these commands are run in reverse.
                        // - Sinks whose descriptions are not exactly
                        //   those that are currently running.
                        should_keep = seen_most_recent_definition.insert(export.id)
                            && running_sink != Some(&export.description);
                    }
                }
                StorageCommand::RunOneshotIngestion(ingestion) => {
                    let already_running = self
                        .storage_state
                        .oneshot_ingestions
                        .contains_key(&ingestion.ingestion_id);
                    let was_canceled = cancel_oneshot_ingestions.contains(&ingestion.ingestion_id);

                    should_keep = !already_running && !was_canceled;
                }
                StorageCommand::CancelOneshotIngestion(ingestion_id) => {
                    let already_running = self
                        .storage_state
                        .oneshot_ingestions
                        .contains_key(ingestion_id);
                    should_keep = already_running;
                }
                StorageCommand::InitializationComplete
                | StorageCommand::AllowWrites
                | StorageCommand::UpdateConfiguration(_)
                | StorageCommand::AllowCompaction(_, _) => (),
            }
            if should_keep {
                filtered_commands.push_front(command);
            }
        }
        let commands = filtered_commands;

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
            .map(|i| i.description.collection_ids())
            .flatten()
            .chain(self.storage_state.exports.keys().copied())
            // Objects are considered stale if we did not see them re-created.
            .filter(|id| !expected_objects.contains(id))
            .collect::<Vec<_>>();
        let stale_oneshot_ingestions = self
            .storage_state
            .oneshot_ingestions
            .keys()
            .filter(|ingestion_id| !self.storage_state.query_owners.contains_key(ingestion_id))
            .filter(|ingestion_id| {
                let to_create = create_oneshot_ingestions.contains(ingestion_id);
                let to_drop = cancel_oneshot_ingestions.contains(ingestion_id);
                mz_ore::soft_assert_or_log!(
                    !(!to_create && to_drop),
                    "attempting to drop oneshot source {ingestion_id} that is not expected to be created during reconciliation"
                );
                !to_create && !to_drop
            })
            .copied()
            .collect::<Vec<_>>();

        info!(
            %worker_id, ?expected_objects, ?stale_objects, ?stale_oneshot_ingestions,
            "reconcile: modifing storage state to match expected objects",
        );

        for id in stale_objects {
            self.storage_state.drop_collection(id);
        }
        for id in stale_oneshot_ingestions {
            self.storage_state
                .handle_storage_command(StorageCommand::CancelOneshotIngestion(id));
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

        // Reset the initial status reported flag when a new client connects
        self.storage_state.initial_status_reported = false;

        // Execute the modified commands.
        for command in commands {
            self.storage_state.handle_storage_command(command);
        }
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
            StorageCommand::Hello { .. }
            | StorageCommand::HelloQuery { .. }
            | StorageCommand::SubscribeObservations => {
                panic!("transport and query commands must be captured before")
            }
            StorageCommand::InitializationComplete => (),
            StorageCommand::AllowWrites => {
                self.read_only_tx
                    .send(false)
                    .expect("we're holding one other end");
                self.persist_clients.cfg().enable_compaction();
            }
            StorageCommand::UpdateConfiguration(params) => {
                // These can be done from all workers safely.
                debug!("Applying configuration update: {params:?}");

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
                            storage_parameters: *params,
                        })
                }
            }
            StorageCommand::RunIngestion(ingestion) => {
                self.ingestions
                    .insert(ingestion.id, ingestion.as_ref().clone());

                // Initialize shared frontier reporting.
                for id in ingestion.description.collection_ids() {
                    self.reported_frontiers
                        .entry(id)
                        .or_insert_with(|| Antichain::from_elem(mz_repr::Timestamp::minimum()));
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
                    let execution = self.execution(ingestion.id);
                    self.async_worker
                        .update_ingestion_frontiers_for(*ingestion, execution);
                }
            }
            StorageCommand::RunOneshotIngestion(oneshot) => {
                if self.query_owners.contains_key(&oneshot.ingestion_id) {
                    return;
                }
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
            StorageCommand::CancelOneshotIngestion(id) => {
                if self.timely_worker_index == 0 {
                    self.internal_cmd_tx
                        .send(InternalStorageCommand::CancelOneshotIngestion(id));
                }
            }
            StorageCommand::RunSink(export) => {
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
                            self.execution(export.id),
                            export.id,
                            export.description,
                        ));
                }
            }
            StorageCommand::AllowCompaction(id, frontier) => {
                soft_assert_or_log!(
                    self.exports.contains_key(&id) || self.reported_frontiers.contains_key(&id),
                    "AllowCompaction command for non-existent {id}"
                );

                if frontier.is_empty() {
                    // Indicates that we may drop `id`, as there are no more valid times to read.
                    self.drop_collection(id);
                }
            }
        }
    }

    /// Drop the identified storage collection from the storage state.
    fn drop_collection(&mut self, id: GlobalId) {
        fail_point!("crash_on_drop");

        if let Some(executions) = &mut self.executions {
            executions.retire(id);
            if let Some(generation) = executions.outputs.0.remove(&id) {
                executions.dropped_outputs.push((id, generation));
            }
            self.source_tokens.remove(&id);
            self.sink_tokens.remove(&id);
            self.source_uppers.remove(&id);
            self.sink_write_frontiers.remove(&id);
            self.aggregated_statistics.deinitialize(id);
        }

        self.ingestions.remove(&id);
        self.exports.remove(&id);

        let _ = self.latest_status_updates.remove(&id);

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

        // Send through async worker for correct ordering with RunIngestion, and
        // dropping the dataflow is done on async worker response.
        if self.timely_worker_index == 0 && self.executions.is_none() {
            self.async_worker.drop_dataflow(id);
        }
    }

    /// Drop the identified oneshot ingestion from the storage state.
    fn drop_oneshot_ingestion(&mut self, ingestion_id: uuid::Uuid) {
        let prev = self.oneshot_ingestions.remove(&ingestion_id);
        info!(%ingestion_id, existed = %prev.is_some(), "dropping oneshot ingestion");
    }
}

#[cfg(test)]
#[path = "storage_state/runtime_tests.rs"]
mod native_runtime_tests;

#[cfg(test)]
#[path = "storage_state/query_tests.rs"]
mod query_tests;
