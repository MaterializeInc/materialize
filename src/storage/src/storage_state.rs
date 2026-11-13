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
//! ## Native admission and internal communication
//!
//! The replica's catalog follower authorizes maintained storage commands. Native
//! admission, query commands and worker feedback share the internal sequencer,
//! so every worker renders dataflows in the same order. An operator failure
//! retires its captured execution and requests reauthorization. Workers cannot
//! recreate maintained work from cached descriptions.
//!
//! ## Async ingestion recovery
//!
//! After admitting [`StorageCommand::RunIngestion`], worker zero submits its
//! description and execution identity to
//! [`AsyncStorageWorker::update_ingestion_frontiers_for`]. The async worker
//! calculates readable input and resumption frontiers without blocking timely.
//! Its result is broadcast as [`InternalStorageCommand::CreateIngestionDataflow`].
//! Each worker checks that the original execution is still admitted before
//! rendering. A delayed result for a retired or replaced execution only completes
//! that attempt's startup bookkeeping. It cannot recreate or replace a dataflow.
//!
//! Replacement preparation keeps the previous dataflow alive until the new
//! execution is ready to render. DROP removes its operators immediately, but
//! pending startup and input-reader completion still govern protection release.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use fail::fail_point;
use mz_cluster_client::CatalogPosition;
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
use tracing::{debug, info};
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
    opened: bool,
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
}

/// Complete storage worker state between turns on a shared Timely host.
/// Detaching releases only the Timely borrow, not client or execution state.
pub struct GuestWorker {
    client_rx: mpsc::UnboundedReceiver<(Uuid, CommandReceiver, ResponseSender)>,
    storage_state: StorageState,
}

impl GuestWorker {
    /// Borrows the host to dispatch commands or perform periodic work.
    pub fn attach(self, timely_worker: &mut TimelyWorker) -> Worker<'_> {
        Worker::from_state(timely_worker, self.client_rx, self.storage_state)
    }

    /// Whether arrivals are queued that have already unparked the host.
    pub fn busy(&self) -> bool {
        !self.client_rx.is_empty()
            || self
                .storage_state
                .replica_commands
                .as_ref()
                .is_some_and(|rx| !rx.is_empty())
            || self
                .storage_state
                .peers
                .values()
                .any(|peer| !peer.commands.is_empty())
            || !self.storage_state.async_worker.is_empty()
    }

    /// Bounds host parking by storage's periodic reporting deadlines.
    pub fn park_duration(&self, last_maintenance: Instant, last_stats_time: Instant) -> Duration {
        storage_park_duration(&self.storage_state, last_maintenance, last_stats_time)
    }
}

fn storage_park_duration(
    state: &StorageState,
    last_maintenance: Instant,
    last_stats_time: Instant,
) -> Duration {
    let stats = state
        .storage_configuration
        .parameters
        .statistics_collection_interval
        .saturating_sub(last_stats_time.elapsed());
    match (last_maintenance + state.server_maintenance_interval)
        .checked_duration_since(Instant::now())
    {
        Some(maintenance) => maintenance.min(stats),
        None => stats,
    }
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

        let storage_state = StorageState::new_guest(
            timely_worker.index(),
            timely_worker.peers(),
            internal_cmd_tx,
            Some(internal_cmd_rx),
            metrics,
            now,
            connection_context,
            instance_context,
            persist_clients,
            txns_ctx,
            tracing_handle,
            shared_rocksdb_write_buffer_manager,
        );

        // TODO(aljoscha): We might want `async_worker` and `internal_cmd_tx` to
        // be fields of `Worker` instead of `StorageState`, but at least for the
        // command flow sources and sinks need access to that. We can refactor
        // this once we have a clearer boundary between what sources/sinks need
        // and the full "power" of the internal command flow, which should stay
        // internal to the worker/not be exposed to source/sink implementations.
        Self::from_state(timely_worker, client_rx, storage_state)
    }

    /// Attaches retained storage state to its hosting Timely worker.
    pub fn from_state(
        timely_worker: &'w mut TimelyWorker,
        client_rx: mpsc::UnboundedReceiver<(Uuid, CommandReceiver, ResponseSender)>,
        storage_state: StorageState,
    ) -> Self {
        Self {
            timely_worker,
            client_rx,
            storage_state,
        }
    }

    /// Releases the host borrow while retaining all storage worker state.
    pub fn into_guest(self) -> GuestWorker {
        GuestWorker {
            client_rx: self.client_rx,
            storage_state: self.storage_state,
        }
    }

    /// Enables native execution on every guest worker, returning its endpoint on worker zero.
    pub fn enable_replica_guest(&mut self) -> Option<crate::server::ReplicaStorageBuilder> {
        let (commands, responses) = self.enable_replica();
        (self.timely_worker.index() == 0).then(|| {
            crate::server::ReplicaStorageBuilder((
                commands,
                std::thread::current(),
                responses,
                self.timely_worker.peers(),
            ))
        })
    }

    /// Installs native ingress and progress in the existing worker runtime.
    /// Call once on every worker, before running or accepting connections.
    pub(crate) fn enable_replica(
        &mut self,
    ) -> (
        mpsc::UnboundedSender<crate::replica::ReplicaCommand>,
        mpsc::UnboundedReceiver<(usize, crate::replica::WorkerResponse)>,
    ) {
        assert!(self.storage_state.replica_progress.is_none());
        let (progress, responses) = mz_cluster::replica_progress::render(self.timely_worker);
        self.storage_state.replica_progress = Some(progress);
        self.storage_state.executions = Some(Default::default());
        let (commands, receiver) = mpsc::unbounded_channel();
        if self.timely_worker.index() == 0 {
            self.storage_state.replica_commands = Some(receiver);
        }
        (commands, responses)
    }
}

impl StorageState {
    /// Creates per-worker storage state, for hosting on any Timely worker.
    ///
    /// The caller provides the internal command channel endpoints: the native storage worker wires
    /// them to the sequencer dataflow, a foreign host wires the sender to its own sequencing
    /// channel and passes no receiver, since it dispatches internal commands itself.
    /// Must be called on the hosting worker's thread, because the async worker unparks the
    /// creating thread.
    pub fn new_guest(
        timely_worker_index: usize,
        timely_worker_peers: usize,
        internal_cmd_tx: InternalCommandSender,
        internal_cmd_rx: Option<InternalCommandReceiver>,
        metrics: StorageMetrics,
        now: NowFn,
        connection_context: ConnectionContext,
        instance_context: StorageInstanceContext,
        persist_clients: Arc<PersistClientCache>,
        txns_ctx: TxnsContext,
        tracing_handle: Arc<TracingHandle>,
        shared_rocksdb_write_buffer_manager: SharedWriteBufferManager,
    ) -> Self {
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
            peers: BTreeMap::new(),
            queries: BTreeMap::new(),
            query_ready: false,
            replica_commands: None,
            replica_progress: None,
            applied_catalog_position: None,
            catalog_catchup: None,
            now,
            timely_worker_index,
            timely_worker_peers,
            instance_context,
            persist_clients,
            txns_ctx,
            sink_tokens: BTreeMap::new(),
            sink_write_frontiers: BTreeMap::new(),
            aggregated_statistics: AggregatedStatistics::new(
                timely_worker_index,
                timely_worker_peers,
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

        storage_state
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
    /// Installed ingestions, used for rendering and resolving export ownership.
    pub ingestions: BTreeMap<GlobalId, RunIngestionCommand>,
    /// Descriptions of each installed export.
    pub exports: BTreeMap<GlobalId, StorageSinkDesc<CollectionMetadata, mz_repr::Timestamp>>,
    /// Descriptions of oneshot ingestions that are currently running.
    pub oneshot_ingestions: BTreeMap<uuid::Uuid, OneshotIngestionDescription<ProtoBatch>>,
    /// Query ownership outlives local completion, until all workers finish.
    /// Only the owning query or all-worker completion may reclaim these IDs.
    query_owners: BTreeMap<Uuid, Uuid>,
    // The guest host creates temporary Worker wrappers, so connection and query
    // state must live here rather than in the wrapper.
    peers: BTreeMap<Uuid, Peer>,
    queries: BTreeMap<Uuid, Query>,
    query_ready: bool,
    /// Native maintained ingress, present only on global worker zero.
    replica_commands: Option<mpsc::UnboundedReceiver<crate::replica::ReplicaCommand>>,
    /// Native progress on every worker, independent of host/lane ownership.
    replica_progress: Option<mz_cluster::replica_progress::Sender<crate::replica::WorkerResponse>>,
    /// Undefined until a native catalog marker passes rendering-stage configuration.
    /// Query requirements and catalog observations cannot advance this prefix.
    applied_catalog_position: Option<CatalogPosition>,
    /// Highest outstanding catch-up hint, coalesced across pending queries.
    catalog_catchup: Option<CatalogPosition>,
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

    /// Statistics for sources and sinks.
    pub aggregated_statistics: AggregatedStatistics,

    /// A place shared with running dataflows, so that health operators, can
    /// report status updates back to us.
    ///
    /// **NOTE**: Operators that append to this collection should take care to only add new
    /// status updates if the status of the ingestion/export in question has _changed_.
    pub shared_status_updates: Rc<RefCell<Vec<crate::replica::StatusEvent>>>,

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
    /// Receiver for cluster-internal storage commands. `None` when the state is hosted outside
    /// the storage server, whose host dispatches internal commands itself.
    pub internal_cmd_rx: Option<InternalCommandReceiver>,

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

    fn finish_startup(&mut self, execution: u64) {
        self.executions
            .as_mut()
            .expect("native execution state")
            .started(execution);
    }

    /// Return an error handler that triggers a suspend and restart of the corresponding storage
    /// dataflow.
    pub fn error_handler(&self, context: &'static str, id: GlobalId) -> ErrorHandler {
        let tx = self.internal_cmd_tx.clone();
        let execution = self.execution(id);
        let id = self.execution_owner(id);
        ErrorHandler::signal(move |e| {
            if let Some(execution) = execution {
                tx.send(InternalStorageCommand::SuspendAndRestart {
                    execution,
                    id,
                    reason: format!("{context}: {e:#}"),
                });
            }
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
    /// Services native maintained work and query endpoints for the process lifetime.
    pub fn run(&mut self) {
        let mut last_stats_time = Instant::now();
        let mut last_maintenance = Instant::now();
        loop {
            self.process_guest(&mut last_maintenance, &mut last_stats_time);
            if self.client_rx.is_empty()
                && self
                    .storage_state
                    .replica_commands
                    .as_ref()
                    .is_none_or(|rx| rx.is_empty())
                && self
                    .storage_state
                    .peers
                    .values()
                    .all(|p| p.commands.is_empty())
                && self.storage_state.async_worker.is_empty()
            {
                let park =
                    storage_park_duration(&self.storage_state, last_maintenance, last_stats_time);
                self.timely_worker.step_or_park(Some(park));
            } else {
                self.timely_worker.step();
            }
            while let Some(command) = self
                .storage_state
                .internal_cmd_rx
                .as_ref()
                .expect("storage server always wires a receiver")
                .try_recv()
            {
                self.handle_internal_storage_command(command);
            }
        }
    }

    /// Processes arrivals and periodic duties without stepping the host or reading the lane.
    /// Internal commands must be dispatched separately in the host's common order.
    pub fn process_guest(&mut self, last_maintenance: &mut Instant, last_stats_time: &mut Instant) {
        assert!(
            self.storage_state.replica_progress.is_some(),
            "native storage ingress must be installed before serving work"
        );
        // Native ingress joins the same global order as queries, restarts,
        // and async worker responses before mutating worker bookkeeping.
        if let Some(commands) = &mut self.storage_state.replica_commands {
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
        while let Ok(response) = self.storage_state.async_worker.try_recv() {
            self.handle_async_worker_response(response);
        }
        let now = Instant::now();
        if now >= *last_maintenance + self.storage_state.server_maintenance_interval {
            *last_maintenance = now;
            self.report_frontier_progress();
            if let Some(executions) = &mut self.storage_state.executions {
                for response in executions.report() {
                    self.storage_state
                        .replica_progress
                        .as_ref()
                        .expect("native progress")
                        .send(response.into());
                }
            }
        }
        self.report_dropped_ids();
        self.process_oneshot_ingestions();
        self.report_status_updates();
        if last_stats_time.elapsed()
            >= self
                .storage_state
                .storage_configuration
                .parameters
                .statistics_collection_interval
        {
            self.report_storage_statistics();
            *last_stats_time = Instant::now();
        }
    }

    fn poll_clients(&mut self) {
        for _ in 0..self.client_rx.len() {
            let Ok((nonce, commands, responses)) = self.client_rx.try_recv() else {
                break;
            };
            self.storage_state.peers.entry(nonce).or_insert(Peer {
                commands,
                responses,
                opened: false,
            });
        }
        let nonces: Vec<_> = self.storage_state.peers.keys().copied().collect();
        for nonce in nonces {
            // Bound each turn so a producer cannot starve siblings or Timely.
            // The extra receive observes disconnect even with an empty queue.
            let budget = self
                .storage_state
                .peers
                .get(&nonce)
                .map_or(0, |p| p.commands.len() + 1);
            for _ in 0..budget {
                let Some(peer) = self.storage_state.peers.get_mut(&nonce) else {
                    break;
                };
                let command = match peer.commands.try_recv() {
                    Ok(command) => Some(command),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => None,
                };
                if !peer.opened {
                    if !matches!(&command, Some(StorageCommand::HelloQuery { .. })) {
                        self.storage_state.peers.remove(&nonce);
                        break;
                    }
                    peer.opened = true;
                }
                let disconnected = command.is_none();
                // Only global worker zero admits commands, including disconnect.
                // Other endpoints can arrive after globally sequenced responses.
                if self.timely_worker.index() == 0 {
                    self.storage_state
                        .internal_cmd_tx
                        .send(InternalStorageCommand::Query { nonce, command });
                }
                if disconnected {
                    self.storage_state.peers.remove(&nonce);
                    break;
                }
            }
        }
        self.flush_query_responses();
    }

    fn flush_query_responses(&mut self) {
        for (nonce, query) in &mut self.storage_state.queries {
            if let Some(peer) = self.storage_state.peers.get(nonce).filter(|p| p.opened) {
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
                if let std::collections::btree_map::Entry::Vacant(entry) =
                    self.storage_state.queries.entry(nonce)
                {
                    let query = entry.insert(Query::default());
                    if self.storage_state.query_ready {
                        query.responses.push(StorageResponse::QueryReady);
                    }
                }
            }
            Some(StorageCommand::SubscribeObservations) => {
                if !self.storage_state.query_ready {
                    // A malformed query peer must not kill maintained work.
                    self.handle_query(nonce, None);
                    return;
                }
                let Some(query) = self.storage_state.queries.get_mut(&nonce) else {
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
                self.storage_state.queries.remove(&nonce);
                self.storage_state.peers.remove(&nonce);
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
                let Some(query) = self.storage_state.queries.get_mut(&nonce) else {
                    return;
                };
                let id = ingestion.ingestion_id;
                if !query.seen.insert(id) {
                    return;
                }
                // IDs are replica-wide. A collision must
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
                if let Some(query) = self.storage_state.queries.get_mut(&nonce) {
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
        let applied = self.storage_state.applied_catalog_position;
        let mut ready = Vec::new();
        let mut catchup: Option<CatalogPosition> = None;
        // Oneshots have independent contexts and no connection-local settings.
        // A newer context's wait must not block another ingestion that is ready.
        for query in self.storage_state.queries.values_mut() {
            for ingestion in std::mem::take(&mut query.pending).into_values() {
                let id = ingestion.ingestion_id;
                let error = match ingestion.catalog_position.as_deref() {
                    None => Some("native oneshot ingestion requires catalog position"),
                    Some(required) if applied.is_some_and(|p| !p.same_history(required)) => {
                        Some("oneshot catalog history or deployment generation mismatch")
                    }
                    _ => None,
                };
                if let Some(error) = error {
                    self.storage_state.query_owners.remove(&id);
                    query
                        .responses
                        .push(StorageResponse::StagedBatches(BTreeMap::from([(
                            id,
                            vec![Err(error.into())],
                        )])));
                    continue;
                }
                if let Some(required) = ingestion.catalog_position.as_deref() {
                    if !applied.is_some_and(|p| p.covers(required)) {
                        match &mut catchup {
                            None => catchup = Some(*required),
                            Some(position) if position.same_history(required) => {
                                position.upper = position.upper.max(required.upper);
                            }
                            // Bootstrap has no authority to choose a history from
                            // query tokens. One hint suffices to wake the owner.
                            Some(_) => (),
                        }
                        query.pending.insert(id, ingestion);
                        continue;
                    }
                }
                if !self.storage_state.query_ready {
                    query.pending.insert(id, ingestion);
                    continue;
                }
                ready.push(ingestion);
            }
        }
        for ingestion in ready {
            crate::render::build_oneshot_ingestion_dataflow(
                self.timely_worker,
                &mut self.storage_state,
                ingestion.ingestion_id,
                ingestion.collection_id,
                ingestion.collection_meta,
                ingestion.request,
            );
        }
        match catchup {
            Some(required) => {
                if !self
                    .storage_state
                    .catalog_catchup
                    .is_some_and(|p| p.covers(&required))
                {
                    self.storage_state.catalog_catchup = Some(required);
                    if self.timely_worker.index() == 0 {
                        if let Some(progress) = &self.storage_state.replica_progress {
                            progress.send(crate::replica::WorkerResponse {
                                output_generation: None,
                                response: crate::server::ReplicaStorageResponse::Response(
                                    StorageResponse::CatalogCatchup(Box::new(required)),
                                ),
                            });
                        }
                    }
                }
            }
            None => self.storage_state.catalog_catchup = None,
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
                    allowed,
                },
            ) => {
                self.storage_state
                    .executions
                    .as_mut()
                    .expect("native runtime")
                    .kafka_pre_open_reply(request, execution, id, allowed);
            }
            InternalStorageCommand::KafkaPreOpen {
                request,
                execution,
                id,
            } => {
                // Exactly one response, regardless of which process runs the sink.
                if self.timely_worker.index() == 0 {
                    self.storage_state
                        .replica_progress
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
                assert!(self.storage_state.replica_progress.is_some());
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
            InternalStorageCommand::ApplyCatalogPosition(position) => {
                assert!(
                    self.storage_state
                        .applied_catalog_position
                        .is_none_or(|previous| position.covers(&previous)),
                    "native catalog position must advance within one fenced history"
                );
                self.storage_state.applied_catalog_position = Some(*position);
                self.admit_queries();
                self.flush_query_responses();
            }
            InternalStorageCommand::QueryFinished {
                nonce,
                ingestion_id,
            } => {
                if self.storage_state.query_owners.get(&ingestion_id) == Some(&nonce) {
                    let query = self
                        .storage_state
                        .queries
                        .get_mut(&nonce)
                        .expect("owner exists");
                    let finished = query.finished.entry(ingestion_id).or_default();
                    *finished += 1;
                    if *finished == self.timely_worker.peers() {
                        query.finished.remove(&ingestion_id);
                        self.storage_state.drop_oneshot_ingestion(ingestion_id);
                        self.storage_state.query_owners.remove(&ingestion_id);
                    }
                }
            }
            InternalStorageCommand::QueryReady => {
                if !self.storage_state.query_ready {
                    self.storage_state.query_ready = true;
                    for query in self.storage_state.queries.values_mut() {
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
                let executions = self
                    .storage_state
                    .executions
                    .as_mut()
                    .expect("native execution state");
                if executions.current.get(&id) != Some(&execution) {
                    return;
                }
                // Retirement fences late startup and coalesces requests from all
                // workers. Only the catalog owner can authorize a replacement Run.
                executions.retire(id);
                self.storage_state.source_tokens.remove(&id);
                self.storage_state.sink_tokens.remove(&id);
                self.storage_state.replica_progress.as_ref().unwrap().send(
                    crate::server::ReplicaStorageResponse::RestartRequested { execution, id }
                        .into(),
                );
            }
            InternalStorageCommand::CreateIngestionDataflow {
                execution,
                id: ingestion_id,
                mut ingestion_description,
                as_of,
                mut resume_uppers,
                mut source_resume_uppers,
            } => {
                if self.storage_state.execution(ingestion_id) != Some(execution) {
                    self.storage_state.finish_startup(execution);
                    return;
                }
                self.storage_state.source_tokens.remove(&ingestion_id);
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
                    if self.timely_worker.index() == 0 {
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
            InternalStorageCommand::RunSinkDataflow(execution, sink_id, sink_description) => {
                if self.storage_state.execution(sink_id) != Some(execution) {
                    self.storage_state.finish_startup(execution);
                    return;
                }
                self.storage_state.sink_tokens.remove(&sink_id);
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
                if self.timely_worker.index() == 0 {
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
    pub fn report_frontier_progress(&mut self) {
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
            self.send_storage_response(StorageResponse::FrontierUpper(id, upper));
        }
    }

    /// Pumps latest status updates from the buffer shared with operators and
    /// reports any updates that need reporting.
    pub fn report_status_updates(&mut self) {
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
                self.send_storage_response(StorageResponse::StatusUpdate(update));
            }
            self.storage_state.initial_status_reported = true;
        }

        // Pump updates into our state and stage them for reporting.
        for event in self.storage_state.shared_status_updates.take() {
            let current = event.execution.is_none_or(|execution| {
                self.storage_state.execution(event.owner) == Some(execution)
            });
            if let Some(progress) = &self.storage_state.replica_progress {
                progress.send(
                    crate::server::ReplicaStorageResponse::Status {
                        event: event.clone(),
                        current,
                    }
                    .into(),
                );
            }
            if !current {
                // A retired dataflow must not replace the current attempt's status.
                continue;
            }
            let shared_update = event.update;
            self.send_storage_response(StorageResponse::StatusUpdate(shared_update.clone()));

            self.storage_state
                .latest_status_updates
                .insert(shared_update.id, shared_update);
        }
    }

    /// Report source statistics back to the controller.
    pub fn report_storage_statistics(&mut self) {
        let (sources, sinks) = self.storage_state.aggregated_statistics.emit_local();
        if !sources.is_empty() || !sinks.is_empty() {
            self.storage_state
                .internal_cmd_tx
                .send(InternalStorageCommand::StatisticsUpdate { sources, sinks })
        }

        // Snapshot resets global counters. Keep aggregating at constant memory
        // through same-process observer outages, without consuming their deltas.
        if !self.storage_state.queries.values().any(|q| q.observations) {
            return;
        }
        let (sources, sinks) = self.storage_state.aggregated_statistics.snapshot();
        if !sources.is_empty() || !sinks.is_empty() {
            self.send_storage_response(StorageResponse::StatisticsUpdates(sources, sinks));
        }
    }

    fn report_dropped_ids(&mut self) {
        let executions = self
            .storage_state
            .executions
            .as_mut()
            .expect("native execution state");
        for (id, generation) in std::mem::take(&mut executions.dropped_outputs) {
            self.storage_state
                .replica_progress
                .as_ref()
                .expect("native progress")
                .send(crate::replica::WorkerResponse {
                    output_generation: Some(generation),
                    response: crate::server::ReplicaStorageResponse::Response(
                        StorageResponse::DroppedId(id),
                    ),
                });
        }
    }

    /// Routes observations to subscribers and maintained progress to its native owner.
    pub fn send_storage_response(&mut self, response: StorageResponse) {
        if matches!(
            response,
            StorageResponse::StatusUpdate(_) | StorageResponse::StatisticsUpdates(..)
        ) {
            for query in self
                .storage_state
                .queries
                .values_mut()
                .filter(|q| q.observations)
            {
                query.responses.push(response.clone());
            }
            self.flush_query_responses();
            return;
        }
        let output_generation = if let StorageResponse::FrontierUpper(id, _) = &response {
            Some(
                self.storage_state
                    .executions
                    .as_ref()
                    .expect("native execution state")
                    .outputs
                    .0[id],
            )
        } else {
            None
        };
        self.storage_state
            .replica_progress
            .as_ref()
            .expect("native progress")
            .send(crate::replica::WorkerResponse {
                output_generation,
                response: crate::server::ReplicaStorageResponse::Response(response),
            });
    }

    /// Forward completed oneshot ingestion results to the coordinator.
    pub fn process_oneshot_ingestions(&mut self) {
        for (ingestion_id, ingestion_state) in &mut self.storage_state.oneshot_ingestions {
            // Cancellation and completion remove ingestion and ownership together on
            // this worker. Renderer callbacks only enqueue results, never mutate these maps.
            let owner = *self
                .storage_state
                .query_owners
                .get(ingestion_id)
                .expect("oneshot ingestion must have a query owner");
            loop {
                match ingestion_state.results.try_recv() {
                    Ok(result) => {
                        let response = match result {
                            Ok(maybe_batch) => maybe_batch.into_iter().map(Result::Ok).collect(),
                            Err(err) => vec![Err(err)],
                        };
                        let staged_batches = BTreeMap::from([(*ingestion_id, response)]);
                        let response = StorageResponse::StagedBatches(staged_batches);
                        if let Some(query) = self.storage_state.queries.get_mut(&owner) {
                            query.responses.push(response);
                        }
                        // Retain local tokens until every worker delivers its terminal result.
                        self.storage_state.internal_cmd_tx.send(
                            InternalStorageCommand::QueryFinished {
                                nonce: owner,
                                ingestion_id: *ingestion_id,
                            },
                        );
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
            | StorageCommand::SubscribeObservations
            | StorageCommand::RunOneshotIngestion(_)
            | StorageCommand::CancelOneshotIngestion(_) => {
                panic!("transport and query commands must be captured before")
            }
            StorageCommand::InitializationComplete => (),
            StorageCommand::ApplyCatalogPosition(position) => {
                // Configuration and retirement emit rendering-stage commands.
                // Sequence this marker behind them instead of acknowledging the
                // first-stage native ingress before their effects are applied.
                if self.timely_worker_index == 0 {
                    self.internal_cmd_tx
                        .send(InternalStorageCommand::ApplyCatalogPosition(position));
                }
            }
            StorageCommand::AllowWrites => {
                self.read_only_tx
                    .send(false)
                    .expect("we're holding one other end");
                self.persist_clients.cfg().enable_compaction();
            }
            StorageCommand::UpdateConfiguration(params) => {
                // Install at ingress, before RunIngestion can submit async shard opens.
                if let Some(target) = &params.persist_state_version {
                    self.persist_clients
                        .cfg()
                        .set_state_version_target(target.clone())
                        .expect("invalid authorized Persist state-format target");
                }
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
                    let execution = self.execution(ingestion.id).expect("admitted ingestion");
                    self.async_worker
                        .update_ingestion_frontiers_for(*ingestion, execution);
                }
            }
            StorageCommand::RunSink(export) => {
                // Track the installed sink for reporting and cleanup.
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
                            self.execution(export.id).expect("admitted sink"),
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

        let executions = self.executions.as_mut().expect("native execution state");
        executions.retire(id);
        if let Some(generation) = executions.outputs.0.remove(&id) {
            executions.dropped_outputs.push((id, generation));
        }
        self.source_tokens.remove(&id);
        self.sink_tokens.remove(&id);
        self.source_uppers.remove(&id);
        self.sink_write_frontiers.remove(&id);
        self.aggregated_statistics.deinitialize(id);

        self.ingestions.remove(&id);
        self.exports.remove(&id);

        self.latest_status_updates.remove(&id);

        self.reported_frontiers.remove(&id);
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
