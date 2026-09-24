// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An interactive dataflow server.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::convert::Infallible;
use std::fmt::Debug;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread::Thread;
use std::time::{Duration, Instant};

use anyhow::Error;
use mz_cluster::client::{ClusterClient, ClusterSpec, GuestClusterClient, TimelyContainer};
use mz_cluster_client::client::TimelyConfig;
use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_client::protocol::history::ComputeCommandHistory;
use mz_compute_client::protocol::response::ComputeResponse;
use mz_compute_client::service::{ComputeClient, PartitionedComputeState, RoleClient};
use mz_ore::halt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_rocksdb::config::SharedWriteBufferManager;
use mz_service::client::{Partitionable, PartitionedState};
use mz_storage::internal_control::{InternalCommandSender, InternalStorageCommand};
use mz_storage::metrics::StorageMetrics;
use mz_storage::storage_state::{
    GuestWorker, StorageInstanceContext, StorageState, Worker as StorageWorker,
};
use mz_storage_client::client::{StorageClient, StorageCommand, StorageResponse};
use mz_storage_types::connections::ConnectionContext;
use mz_txn_wal::operator::TxnsContext;
use timely::progress::Antichain;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, trace, warn};
use uuid::Uuid;

use crate::command_channel::{self, StorageLaneInput, UnifiedCommand};
use crate::compute_state::{
    ActiveComputeState, ComputeState, PeekPermits, PendingPeek, ReportedFrontier,
};
use crate::metrics::{ComputeMetrics, WorkerMetrics};
use crate::replica_progress;

/// Runtime-owned compute control, sequenced before worker partitioning.
///
/// This endpoint has no connection nonce or reconciliation handshake. Its first
/// command must initialize the instance exactly once. Responses aggregate every global worker,
/// including workers in other processes. Keep draining responses while it lives.
pub struct ReplicaCompute {
    // Maintained execution must not depend on query listener/client lifetimes.
    _runtime: Arc<Mutex<TimelyContainer<Config>>>,
    commands: command_channel::Sender,
    responses: mpsc::UnboundedReceiver<(usize, ComputeResponse)>,
    aggregation: PartitionedComputeState,
    initialized: bool,
}

type ReplicaChannels = (
    command_channel::Sender,
    mpsc::UnboundedReceiver<(usize, ComputeResponse)>,
    usize,
);

/// The process-local compute runtime and its connection factory.
///
/// Workers run for the process lifetime, without an in-process shutdown protocol.
/// Retain a server, endpoint, or connection factory for that lifetime. Dropping the
/// last runtime owner joins the worker threads.
pub struct ComputeServer {
    runtime: Arc<Mutex<TimelyContainer<Config>>>,
    replica_owned: bool,
    replica: Option<ReplicaCompute>,
}

impl ComputeServer {
    /// Creates a transport factory without transferring runtime ownership.
    pub fn client_builder(&self) -> impl Fn() -> Box<dyn ComputeClient> + use<> {
        let runtime = Arc::clone(&self.runtime);
        let replica_owned = self.replica_owned;
        move || -> Box<dyn ComputeClient> {
            let client = ClusterClient::new(Arc::clone(&runtime));
            if replica_owned {
                Box::new(RoleClient::query_only(client))
            } else {
                Box::new(RoleClient::new(client))
            }
        }
    }

    /// Takes the unique native control endpoint, present only on process zero
    /// of a replica-owned runtime.
    pub fn take_replica(&mut self) -> Option<ReplicaCompute> {
        self.replica.take()
    }
}

impl ReplicaCompute {
    /// Enqueues a maintained command in the replica's common worker order.
    /// The first command must be `CreateInstance`, which must not be repeated.
    pub fn send(&mut self, command: ComputeCommand) {
        assert!(
            !matches!(
                command,
                ComputeCommand::Hello { .. }
                    | ComputeCommand::HelloQuery { .. }
                    | ComputeCommand::SetQueryMaxResultSize { .. }
                    | ComputeCommand::CreateQueryDataflow { .. }
                    | ComputeCommand::Peek(_)
                    | ComputeCommand::CancelPeek { .. }
            ),
            "query and transport commands must use query connections"
        );
        let initializes = matches!(command, ComputeCommand::CreateInstance(_));
        assert_eq!(
            initializes, !self.initialized,
            "replica must initialize its instance exactly once, before other commands"
        );
        self.initialized = true;
        self.aggregation.observe_command(&command);
        self.commands
            .send((Some(command), command_channel::Origin::Replica));
    }

    /// Receives replica-wide progress or another maintained response.
    /// Cancel safe. Partial worker responses remain in the aggregation state.
    pub async fn recv(&mut self) -> Result<Option<ComputeResponse>, Error> {
        while let Some((worker, response)) = self.responses.recv().await {
            if let Some(response) = self.aggregation.absorb_response(worker, response) {
                return response.map(Some);
            }
        }
        Ok(None)
    }
}

/// Caller-provided configuration for compute.
#[derive(Clone, Debug)]
pub struct ComputeInstanceContext {
    /// A directory that can be used for scratch work.
    pub scratch_directory: Option<PathBuf>,
    /// Whether to set core affinity for Timely workers.
    pub worker_core_affinity: bool,
    /// Context required to connect to an external sink from compute,
    /// like the `CopyToS3OneshotSink` compute sink.
    pub connection_context: ConnectionContext,
}

/// Which of a process's compute runtimes a given runtime is.
///
/// A clusterd process runs a single `Solo` runtime by default. When an interactive runtime is
/// configured, the process instead runs a `Maintenance` and an `Interactive` runtime side by side.
/// The named roles share per-process resources (persist cache, metrics registry, log spans). The
/// role distinguishes them so that only the globals-owning runtime runs the non-idempotent
/// process-global initializers, and so metric series and log spans do not collide.
///
/// `Solo` exists so the single-runtime default stays behaviorally identical to a deployment without
/// a second runtime: no `role` metric label, and it owns the process globals just as the sole
/// runtime always has.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComputeRuntimeRole {
    /// The sole runtime of a single-runtime process. Owns index maintenance and the process-global
    /// initializers.
    Solo,
    /// The maintenance runtime of a two-runtime process. Owns index maintenance and the
    /// process-global initializers.
    Maintenance,
    /// The interactive runtime of a two-runtime process. Shares the process globals owned by
    /// maintenance and serves reads.
    ///
    /// Test-only until the interactive runtime exists to construct it. It is present because the
    /// `role` label's entire purpose is that two named roles register into one process registry
    /// without colliding, and nothing else can express that: `Solo` registers the same metric names
    /// with no `role` label, so prometheus rejects it alongside a named role for differing label
    /// dimensions rather than treating it as a second series. Verifying non-collision therefore
    /// needs a second *named* role.
    ///
    /// TODO: drop the `cfg` when the interactive runtime lands and constructs this.
    #[cfg(test)]
    Interactive,
}

impl ComputeRuntimeRole {
    /// The `role` metric/log label for this role, or `None` for `Solo`.
    ///
    /// `Solo` omits the label so a single-runtime deployment registers exactly as it did before a
    /// second runtime existed, keeping exact-match dashboards and alerts unchanged.
    pub fn label(self) -> Option<&'static str> {
        match self {
            ComputeRuntimeRole::Solo => None,
            ComputeRuntimeRole::Maintenance => Some("maintenance"),
            #[cfg(test)]
            ComputeRuntimeRole::Interactive => Some("interactive"),
        }
    }

    /// Whether this role runs the non-idempotent, process-global initializers.
    ///
    /// `Solo` and `Maintenance` run them. An interactive runtime shares the same process and
    /// inherits the globals maintenance installs, so re-running them would either double-apply a
    /// non-idempotent effect or race maintenance.
    ///
    /// NOTE: every role a release build can construct owns the globals, so this is constantly true
    /// outside tests. The distinction becomes load-bearing when the interactive runtime lands.
    pub fn owns_process_globals(self) -> bool {
        matches!(
            self,
            ComputeRuntimeRole::Solo | ComputeRuntimeRole::Maintenance
        )
    }
}

/// Configures the server with compute-specific metrics.
#[derive(Clone)]
struct Config {
    replica_owned: bool,
    replica_ready: Arc<Mutex<Option<oneshot::Sender<ReplicaChannels>>>>,
    /// `persist` client cache.
    pub persist_clients: Arc<PersistClientCache>,
    /// Context necessary for rendering txn-wal operators.
    pub txns_ctx: TxnsContext,
    /// A process-global handle to tracing configuration.
    pub tracing_handle: Arc<TracingHandle>,
    /// Metrics exposed by compute replicas.
    pub metrics: ComputeMetrics,
    /// Other configuration for compute.
    pub context: ComputeInstanceContext,
    /// The process-global metrics registry.
    pub metrics_registry: MetricsRegistry,
    /// The number of timely workers per process.
    pub workers_per_process: usize,
    /// Bounds how many offloaded peek walks run at once, shared by every worker this server runs.
    ///
    /// NOTE: per compute runtime, not global. A process running a maintenance and an interactive
    /// runtime calls `serve` twice and admits the bound once per call.
    pub peek_permits: Arc<PeekPermits>,
    /// Configuration for hosting storage objects on this cluster, if enabled.
    pub storage_guest: Option<Arc<StorageGuestConfig>>,
}

/// A per-worker channel delivering storage client connections.
type StorageClientRx = mpsc::UnboundedReceiver<(
    Uuid,
    mpsc::UnboundedReceiver<StorageCommand>,
    mpsc::UnboundedSender<StorageResponse>,
)>;

/// Configuration for hosting storage objects on the compute cluster.
pub struct StorageGuestConfig {
    replica_ready: Mutex<Option<oneshot::Sender<mz_storage::server::ReplicaStorageBuilder>>>,
    /// Per-worker channels delivering storage client connections, indexed by local worker index.
    client_rxs: Mutex<Vec<Option<StorageClientRx>>>,
    /// Metrics for storage objects.
    metrics: StorageMetrics,
    /// Function to get wall time now.
    now: NowFn,
    /// Configuration for source and sink connections.
    connection_context: ConnectionContext,
    /// Other configuration for storage instances.
    instance_context: StorageInstanceContext,
    /// Shared rocksdb write buffer manager.
    shared_rocksdb_write_buffer_manager: SharedWriteBufferManager,
}

/// Initiates a timely dataflow computation, processing compute commands.
/// Replica-owned runtimes accept only query connections and return their native
/// control endpoint on process zero. Controller-owned runtimes return no endpoint.
pub async fn serve(
    timely_config: TimelyConfig,
    role: ComputeRuntimeRole,
    replica_owned: bool,
    metrics_registry: &MetricsRegistry,
    persist_clients: Arc<PersistClientCache>,
    txns_ctx: TxnsContext,
    tracing_handle: Arc<TracingHandle>,
    context: ComputeInstanceContext,
) -> Result<ComputeServer, Error> {
    let workers_per_process = timely_config.workers;
    let config = Config {
        replica_owned,
        replica_ready: Arc::new(Mutex::new(None)),
        persist_clients,
        txns_ctx,
        tracing_handle,
        metrics: ComputeMetrics::register_with(metrics_registry, role),
        context,
        metrics_registry: metrics_registry.clone(),
        workers_per_process,
        peek_permits: Arc::new(PeekPermits::new(workers_per_process)),
        storage_guest: None,
    };

    let (_worker_threads, server) = serve_inner(config, timely_config).await?;
    Ok(server)
}

/// Initiates a timely dataflow computation that processes compute commands and additionally hosts
/// storage objects, processing storage commands received over a separate client connection.
///
/// Returns the compute server, the native storage endpoint on process zero when
/// replica-owned, and the storage connection factory. Both endpoints retain the host runtime.
pub async fn serve_unified(
    timely_config: TimelyConfig,
    role: ComputeRuntimeRole,
    replica_owned: bool,
    metrics_registry: &MetricsRegistry,
    persist_clients: Arc<PersistClientCache>,
    txns_ctx: TxnsContext,
    tracing_handle: Arc<TracingHandle>,
    context: ComputeInstanceContext,
    now: NowFn,
    storage_connection_context: ConnectionContext,
    storage_instance_context: StorageInstanceContext,
) -> Result<
    (
        ComputeServer,
        Option<mz_storage::server::ReplicaStorage>,
        impl Fn() -> Box<dyn StorageClient> + use<>,
    ),
    Error,
> {
    let workers_per_process = timely_config.workers;

    // Per-worker channels over which storage client connections are delivered.
    let mut storage_client_txs = Vec::new();
    let mut storage_client_rxs = Vec::new();
    for _ in 0..workers_per_process {
        let (tx, rx) = mpsc::unbounded_channel();
        storage_client_txs.push(tx);
        storage_client_rxs.push(Some(rx));
    }

    let (storage_ready_tx, storage_ready_rx) = if replica_owned && timely_config.process == 0 {
        let (tx, rx) = oneshot::channel();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    let storage_guest = StorageGuestConfig {
        replica_ready: Mutex::new(storage_ready_tx),
        client_rxs: Mutex::new(storage_client_rxs),
        metrics: StorageMetrics::register_with(metrics_registry),
        now,
        connection_context: storage_connection_context,
        instance_context: storage_instance_context,
        shared_rocksdb_write_buffer_manager: Default::default(),
    };

    let config = Config {
        replica_owned,
        replica_ready: Arc::new(Mutex::new(None)),
        persist_clients,
        txns_ctx,
        tracing_handle,
        metrics: ComputeMetrics::register_with(metrics_registry, role),
        context,
        metrics_registry: metrics_registry.clone(),
        workers_per_process,
        peek_permits: Arc::new(PeekPermits::new(workers_per_process)),
        storage_guest: Some(Arc::new(storage_guest)),
    };

    let (worker_threads, compute_server) = serve_inner(config, timely_config).await?;
    let replica_storage = match storage_ready_rx {
        Some(rx) => Some(rx.await?.build(Arc::clone(&compute_server.runtime))),
        None => None,
    };
    let runtime = Arc::clone(&compute_server.runtime);

    let storage_client_txs = Arc::new(storage_client_txs);
    let storage_client_builder = move || {
        // Storage connections retain the host runtime just like compute connections.
        let client =
            GuestClusterClient::new(Arc::clone(&storage_client_txs), worker_threads.clone());
        mz_storage::server::guest_client(client, replica_owned, Arc::clone(&runtime))
    };

    Ok((compute_server, replica_storage, storage_client_builder))
}

/// Builds the Timely cluster for the given config and returns its worker threads along with a
/// server retaining runtime ownership.
async fn serve_inner(
    config: Config,
    timely_config: TimelyConfig,
) -> Result<(Vec<Thread>, ComputeServer), Error> {
    let replica_owned = config.replica_owned;
    let ready_rx = if replica_owned && timely_config.process == 0 {
        let (tx, rx) = oneshot::channel();
        *config.replica_ready.lock().expect("poisoned") = Some(tx);
        Some(rx)
    } else {
        None
    };
    mz_timely_util::column_pager::metrics::register(
        &config.metrics_registry,
        mz_timely_util::column_pager::tiered_policy(),
    );
    mz_timely_util::pool_config::metrics::register(&config.metrics_registry);

    let tokio_executor = tokio::runtime::Handle::current();

    let timely_container = config.build_cluster(timely_config, tokio_executor).await?;
    let worker_threads = timely_container.worker_threads();
    let timely_container = Arc::new(Mutex::new(timely_container));

    let replica = match ready_rx {
        Some(rx) => {
            let (commands, responses, peers) = rx.await?;
            Some(ReplicaCompute {
                _runtime: Arc::clone(&timely_container),
                commands,
                responses,
                aggregation: <(ComputeCommand, ComputeResponse) as Partitionable<_, _>>::new(peers),
                initialized: false,
            })
        }
        None => None,
    };
    Ok((
        worker_threads,
        ComputeServer {
            runtime: timely_container,
            replica_owned,
            replica,
        },
    ))
}

/// Error type returned on connection nonce changes.
///
/// A nonce change informs workers that subsequent commands come a from a new client connection
/// and therefore require reconciliation.
struct NonceChange(Uuid);

/// Endpoint used by workers to receive compute commands.
///
/// Separates queries from maintained commands. Only controller-owned runtimes
/// observe lifecycle nonce changes and convert them into reconciliation requests.
struct CommandReceiver {
    replica_owned: bool,
    /// The channel supplying commands.
    inner: command_channel::Receiver,
    /// The ID of the Timely worker.
    worker_id: usize,
    /// The nonce identifying the current cluster protocol incarnation.
    nonce: Option<Uuid>,
    /// A stash to enable peeking the next command, used in `try_recv`.
    stashed_command: Option<ComputeCommand>,
    /// Query commands preceding the next lifecycle command, in sequencer order.
    /// These remain queued only until a compute state exists. Lifecycle initialization
    /// services them against the old state until reconciliation can apply atomically.
    deferred_queries: VecDeque<(Option<ComputeCommand>, Uuid)>,
}

impl CommandReceiver {
    fn new(inner: command_channel::Receiver, worker_id: usize) -> Self {
        Self {
            replica_owned: false,
            inner,
            worker_id,
            nonce: None,
            stashed_command: None,
            deferred_queries: VecDeque::new(),
        }
    }

    /// Receive the next pending command, if any.
    ///
    /// Queries are deferred without changing the lifecycle nonce. A new lifecycle
    /// nonce requests reconciliation. Storage commands retain their lane position.
    fn try_recv(&mut self) -> Result<Option<WorkerCommand>, NonceChange> {
        if let Some(command) = self.stashed_command.take() {
            return Ok(Some(WorkerCommand::Compute(command)));
        }
        let (command, nonce) = loop {
            match self.inner.try_recv() {
                Some(UnifiedCommand::Compute(command, command_channel::Origin::Query(nonce))) => {
                    self.deferred_queries.push_back((command, nonce));
                }
                Some(UnifiedCommand::Compute(Some(command), command_channel::Origin::Replica)) => {
                    assert!(
                        self.replica_owned,
                        "replica command on a controller-owned runtime"
                    );
                    return Ok(Some(WorkerCommand::Compute(command)));
                }
                Some(UnifiedCommand::Compute(None, command_channel::Origin::Replica)) => {
                    unreachable!()
                }
                Some(UnifiedCommand::Compute(
                    Some(command),
                    command_channel::Origin::Lifecycle(nonce),
                )) => {
                    assert!(
                        !self.replica_owned,
                        "lifecycle command on a replica-owned runtime"
                    );
                    break (command, nonce);
                }
                Some(UnifiedCommand::Compute(None, command_channel::Origin::Lifecycle(_))) => {
                    unreachable!()
                }
                Some(UnifiedCommand::Storage(command)) => {
                    return Ok(Some(WorkerCommand::Storage(command)));
                }
                None => return Ok(None),
            }
        };

        trace!(worker = self.worker_id, %nonce, ?command, "received command");

        if Some(nonce) == self.nonce {
            Ok(Some(WorkerCommand::Compute(command)))
        } else {
            self.nonce = Some(nonce);
            self.stashed_command = Some(command);
            Err(NonceChange(nonce))
        }
    }
}

/// A command dispatched in the unified lane's order.
enum WorkerCommand {
    Compute(ComputeCommand),
    Storage(InternalStorageCommand),
}

/// Ordered worker-to-transport routing metadata and responses.
#[derive(Debug)]
pub(crate) enum ResponseEvent {
    Response(ComputeResponse, Uuid),
    Lifecycle(Uuid),
    QueryOpen(Uuid),
    QueryRetired(Uuid),
}

/// Routes query responses to their connection and maintained responses to their owner.
pub(crate) struct ResponseSender {
    replica: Option<replica_progress::Sender>,
    /// The channel consuming responses.
    inner: mpsc::UnboundedSender<ResponseEvent>,
    /// The ID of the Timely worker.
    worker_id: usize,
    /// The nonce identifying the current cluster protocol incarnation.
    nonce: Option<Uuid>,
}

impl ResponseSender {
    /// `pub(crate)` rather than private so the peek tests can build the sender a worker holds.
    pub(crate) fn new(inner: mpsc::UnboundedSender<ResponseEvent>, worker_id: usize) -> Self {
        Self {
            replica: None,
            inner,
            worker_id,
            nonce: None,
        }
    }

    /// Set the cluster protocol nonce.
    pub(crate) fn set_nonce(&mut self, nonce: Uuid) {
        assert!(
            self.replica.is_none(),
            "replica responses have no lifecycle nonce"
        );
        self.nonce = Some(nonce);
        let _ = self.inner.send(ResponseEvent::Lifecycle(nonce));
    }

    /// Sends a maintained response to its owner.
    ///
    /// Controller transport loss is reported to the caller. Replica-owned
    /// progress loss panics because execution must not continue without its
    /// protection owner receiving progress.
    pub fn send(&self, response: ComputeResponse) -> Result<(), SendError<ComputeResponse>> {
        if let Some(replica) = &self.replica {
            replica.send(response);
            return Ok(());
        }
        let nonce = self.nonce.expect("nonce must be initialized");

        self.send_query(nonce, response)
    }

    /// Sends to an explicit connection without changing the lifecycle response nonce.
    pub fn send_query(
        &self,
        nonce: Uuid,
        response: ComputeResponse,
    ) -> Result<(), SendError<ComputeResponse>> {
        trace!(worker = self.worker_id, %nonce, ?response, "sending response");
        self.inner
            .send(ResponseEvent::Response(response, nonce))
            .map_err(|SendError(event)| match event {
                ResponseEvent::Response(response, _) => SendError(response),
                _ => unreachable!(),
            })
    }
}

/// State maintained for each worker thread.
///
/// Much of this state can be viewed as local variables for the worker thread,
/// holding state that persists across function calls.
struct Worker<'w> {
    /// The underlying Timely worker.
    timely_worker: &'w mut TimelyWorker,
    /// The channel over which commands are received.
    command_rx: CommandReceiver,
    /// The channel over which responses are sent.
    response_tx: ResponseSender,
    compute_state: Option<ComputeState>,
    /// Compute metrics.
    metrics: WorkerMetrics,
    /// A process-global cache of (blob_uri, consensus_uri) -> PersistClient.
    /// This is intentionally shared between workers
    persist_clients: Arc<PersistClientCache>,
    /// Context necessary for rendering txn-wal operators.
    txns_ctx: TxnsContext,
    /// A process-global handle to tracing configuration.
    tracing_handle: Arc<TracingHandle>,
    context: ComputeInstanceContext,
    /// The process-global metrics registry.
    metrics_registry: MetricsRegistry,
    /// The number of timely workers per process.
    workers_per_process: usize,
    /// Bounds how many offloaded peek walks run at once, shared by the workers of one `serve`
    /// call rather than by the process.
    peek_permits: Arc<PeekPermits>,
    /// The hosted storage guest, if any.
    storage: Option<StorageGuest>,
}

/// Storage retains its client and execution state between turns on the host worker.
struct StorageGuest {
    worker: GuestWorker,
    last_maintenance: tokio::time::Instant,
    last_stats_time: tokio::time::Instant,
}

impl StorageGuest {
    fn park_cap(&self) -> Option<Duration> {
        Some(
            self.worker
                .park_duration(self.last_maintenance, self.last_stats_time),
        )
    }
}

impl ClusterSpec for Config {
    type Command = ComputeCommand;
    type Response = ComputeResponse;

    const NAME: &str = "compute";

    fn run_worker(
        &self,
        timely_worker: &mut TimelyWorker,
        client_rx: mpsc::UnboundedReceiver<(
            Uuid,
            mpsc::UnboundedReceiver<ComputeCommand>,
            mpsc::UnboundedSender<ComputeResponse>,
        )>,
    ) {
        if self.context.worker_core_affinity {
            set_core_affinity(timely_worker.index());
        }

        let worker_id = timely_worker.index();
        let metrics = self.metrics.for_worker(worker_id);

        let local_index = worker_id % self.workers_per_process;

        // Prepare the storage guest's inputs to the command channel, so
        // storage-internal commands are sequenced through the same lane as compute commands.
        let mut storage_lane_input = None;
        let guest_setup = self.storage_guest.as_ref().map(|cfg| {
            let storage_client_rx = cfg.client_rxs.lock().expect("poisoned")[local_index]
                .take()
                .expect("each worker takes its storage client_rx exactly once");
            let (internal_tx, internal_rx) = std::sync::mpsc::channel();
            let activator_slot = Rc::new(RefCell::new(None));
            storage_lane_input = Some(StorageLaneInput {
                rx: internal_rx,
                activator_slot: Rc::clone(&activator_slot),
            });
            let internal_cmd_tx = InternalCommandSender::from_parts(internal_tx, activator_slot);
            (Arc::clone(cfg), storage_client_rx, internal_cmd_tx)
        });

        // Create the command channel that broadcasts commands from worker 0 to other workers. We
        // reuse this channel between client connections, to avoid bugs where different workers end
        // up creating incompatible sides of the channel dataflow after reconnects.
        // See database-issues#8964.
        let (cmd_tx, cmd_rx) = command_channel::render(timely_worker, storage_lane_input);
        let (resp_tx, resp_rx) = mpsc::unbounded_channel();
        let mut command_rx = CommandReceiver::new(cmd_rx, worker_id);
        command_rx.replica_owned = self.replica_owned;
        let mut response_tx = ResponseSender::new(resp_tx, worker_id);
        if self.replica_owned {
            let (progress_tx, progress_rx) = replica_progress::render(timely_worker);
            response_tx.replica = Some(progress_tx);
            if worker_id == 0 {
                let endpoint = (cmd_tx.clone(), progress_rx, timely_worker.peers());
                self.replica_ready
                    .lock()
                    .expect("poisoned")
                    .take()
                    .expect("replica owner is registered")
                    .send(endpoint)
                    .unwrap_or_else(|_| panic!("replica owner lost during startup"));
            }
        }

        spawn_channel_adapter(client_rx, cmd_tx, resp_rx, worker_id);

        // Create the storage guest state.
        let storage = guest_setup.map(|(cfg, storage_client_rx, internal_cmd_tx)| {
            let storage_state = StorageState::new_guest(
                timely_worker.index(),
                timely_worker.peers(),
                internal_cmd_tx,
                // The host dispatches internal commands from the unified command channel, so
                // the guest reads no receiver of its own.
                None,
                cfg.metrics.clone(),
                cfg.now.clone(),
                cfg.connection_context.clone(),
                cfg.instance_context.clone(),
                Arc::clone(&self.persist_clients),
                self.txns_ctx.clone(),
                Arc::clone(&self.tracing_handle),
                cfg.shared_rocksdb_write_buffer_manager.clone(),
            );

            let mut worker =
                StorageWorker::from_state(timely_worker, storage_client_rx, storage_state);
            if self.replica_owned {
                if let Some(builder) = worker.enable_replica_guest() {
                    cfg.replica_ready
                        .lock()
                        .expect("poisoned")
                        .take()
                        .expect("replica owner registered")
                        .send(builder)
                        .unwrap_or_else(|_| panic!("replica owner lost during startup"));
                }
            }
            StorageGuest {
                worker: worker.into_guest(),
                last_maintenance: tokio::time::Instant::now(),
                last_stats_time: tokio::time::Instant::now(),
            }
        });

        Worker {
            timely_worker,
            command_rx,
            response_tx,
            metrics,
            context: self.context.clone(),
            persist_clients: Arc::clone(&self.persist_clients),
            txns_ctx: self.txns_ctx.clone(),
            compute_state: None,
            tracing_handle: Arc::clone(&self.tracing_handle),
            metrics_registry: self.metrics_registry.clone(),
            workers_per_process: self.workers_per_process,
            peek_permits: Arc::clone(&self.peek_permits),
            storage,
        }
        .run()
    }
}

/// Set the current thread's core affinity, based on the given `worker_id`.
#[cfg(not(target_os = "macos"))]
fn set_core_affinity(worker_id: usize) {
    use tracing::error;

    let Some(mut core_ids) = core_affinity::get_core_ids() else {
        error!(worker_id, "unable to get core IDs for setting affinity");
        return;
    };

    // The `get_core_ids` docs don't say anything about a guaranteed order of the returned Vec,
    // so sort it just to be safe.
    core_ids.sort_unstable_by_key(|i| i.id);

    // On multi-process replicas `worker_id` might be greater than the number of available cores.
    // However, we assume that we always have at least as many cores as there are local workers.
    // Violating this assumption is safe but might lead to degraded performance due to skew in core
    // utilization.
    let idx = worker_id % core_ids.len();
    let core_id = core_ids[idx];

    if core_affinity::set_for_current(core_id) {
        info!(
            worker_id,
            core_id = core_id.id,
            "set core affinity for worker"
        );
    } else {
        error!(
            worker_id,
            core_id = core_id.id,
            "failed to set core affinity for worker"
        )
    }
}

/// Set the current thread's core affinity, based on the given `worker_id`.
#[cfg(target_os = "macos")]
fn set_core_affinity(_worker_id: usize) {
    // Setting core affinity is known to not work on Apple Silicon:
    // https://github.com/Elzair/core_affinity_rs/issues/22
    info!("setting core affinity is not supported on macOS");
}

impl<'w> Worker<'w> {
    /// Runs a compute worker.
    pub fn run(&mut self) {
        if self.command_rx.replica_owned {
            let first = self
                .recv_command()
                .unwrap_or_else(|_| panic!("replica initialization changed nonce"));
            assert!(
                matches!(first, ComputeCommand::CreateInstance(_)),
                "replica must initialize its instance first"
            );
            self.handle_command(first);
            let Err(_) = self.run_commands();
            unreachable!("replica-owned runtime cannot change lifecycle nonce");
        }
        // The command receiver is initialized without an nonce, so receiving the first command
        // always triggers a nonce change.
        let NonceChange(nonce) = self.recv_command().expect_err("change to first nonce");
        self.set_nonce(nonce);

        loop {
            let Err(NonceChange(nonce)) = self.run_client();
            self.set_nonce(nonce);
        }
    }

    fn set_nonce(&mut self, nonce: Uuid) {
        // Query cleanup also runs during initialization. Retired exports must
        // stop reporting before any such cleanup can use the new lifecycle nonce.
        if let Some(state) = &mut self.compute_state {
            state.silence_retired_frontiers();
        }
        self.response_tx.set_nonce(nonce);
    }

    /// Handles commands for a client connection, returns when the nonce changes.
    fn run_client(&mut self) -> Result<Infallible, NonceChange> {
        self.reconcile()?;
        self.run_commands()
    }

    fn run_commands(&mut self) -> Result<Infallible, NonceChange> {
        self.handle_deferred_queries();

        // The last time we did periodic maintenance.
        let mut last_maintenance = Instant::now();

        // Commence normal operation.
        loop {
            // Get the maintenance interval, default to zero if we don't have a compute state.
            let maintenance_interval = self
                .compute_state
                .as_ref()
                .map_or(Duration::ZERO, |state| state.server_maintenance_interval);

            let now = Instant::now();
            // Determine if we need to perform maintenance, which is true if `maintenance_interval`
            // time has passed since the last maintenance.
            let sleep_duration;
            if now >= last_maintenance + maintenance_interval {
                last_maintenance = now;
                sleep_duration = None;

                // Report frontier information back the coordinator.
                if let Some(mut compute_state) = self.activate_compute() {
                    compute_state.compute_state.traces.maintenance();
                    compute_state.report_frontiers();
                    compute_state.report_metrics();
                    compute_state.check_expiration();
                }

                self.metrics.record_shared_row_metrics();
            } else {
                // We didn't perform maintenance, sleep until the next maintenance interval.
                let next_maintenance = last_maintenance + maintenance_interval;
                sleep_duration = Some(next_maintenance.saturating_duration_since(now))
            };

            // Do not sleep while a peek waits for its turn. Only the sweep below gives it one,
            // and nothing else leaves an activation behind to end the park.
            let sleep_duration = match &self.compute_state {
                Some(state) if state.peeks_awaiting_turn() => Some(Duration::ZERO),
                _ => sleep_duration,
            };

            // With a storage guest, cap the park duration so the guest's periodic duties run on
            // time.
            let sleep_duration = match self.storage.as_ref().and_then(StorageGuest::park_cap) {
                Some(cap) => Some(sleep_duration.map_or(cap, |d| d.min(cap))),
                None => sleep_duration,
            };

            // Step the timely worker, recording the time taken.
            let timer = self.metrics.timely_step_duration_seconds.start_timer();
            if self.storage_guest_busy() {
                self.timely_worker.step();
            } else {
                self.timely_worker.step_or_park(sleep_duration);
            }
            timer.observe_duration();

            self.handle_pending_commands()?;

            self.process_storage_guest();

            if let Some(mut compute_state) = self.activate_compute() {
                compute_state.process_peeks();
                compute_state.process_subscribes();
                compute_state.process_copy_tos();
            }
            if let Some(state) = &mut self.compute_state {
                state.poll_query_commands(self.timely_worker, &mut self.response_tx);
            }
        }
    }

    fn handle_pending_commands(&mut self) -> Result<(), NonceChange> {
        loop {
            let command = self.command_rx.try_recv();
            // Query commands preceding a lifecycle reconnect must execute before reconciliation.
            self.handle_deferred_queries();
            match command? {
                Some(WorkerCommand::Compute(cmd)) => self.handle_command(cmd),
                Some(WorkerCommand::Storage(cmd)) => self.handle_storage_internal_command(cmd),
                None => break,
            }
        }
        Ok(())
    }

    fn handle_deferred_queries(&mut self) {
        let Some(state) = self.compute_state.as_mut() else {
            return;
        };
        for (command, nonce) in self.command_rx.deferred_queries.drain(..) {
            // Routing events and responses share one FIFO. Opening precedes QueryReady,
            // and retirement follows the handler's cleanup, even without a local peer.
            if matches!(command, Some(ComputeCommand::HelloQuery { .. })) {
                let _ = self.response_tx.inner.send(ResponseEvent::QueryOpen(nonce));
            }
            let disconnect = command.is_none();
            state.handle_query_command(self.timely_worker, command, nonce, &mut self.response_tx);
            if disconnect {
                let _ = self
                    .response_tx
                    .inner
                    .send(ResponseEvent::QueryRetired(nonce));
            }
        }
    }

    /// Pending arrivals must be drained before parking, since they unpark only once.
    fn storage_guest_busy(&self) -> bool {
        self.storage
            .as_ref()
            .is_some_and(|guest| guest.worker.busy())
    }

    /// All storage rendering is dispatched at its position in the common lane.
    fn handle_storage_internal_command(&mut self, cmd: InternalStorageCommand) {
        let mut guest = self
            .storage
            .take()
            .expect("storage command requires a guest");
        let mut worker = guest.worker.attach(self.timely_worker);
        worker.handle_internal_storage_command(cmd);
        guest.worker = worker.into_guest();
        self.storage = Some(guest);
    }

    fn process_storage_guest(&mut self) {
        let Some(mut guest) = self.storage.take() else {
            return;
        };
        let mut worker = guest.worker.attach(self.timely_worker);
        worker.process_guest(&mut guest.last_maintenance, &mut guest.last_stats_time);
        guest.worker = worker.into_guest();
        self.storage = Some(guest);
    }

    fn handle_command(&mut self, cmd: ComputeCommand) {
        if matches!(&cmd, ComputeCommand::CreateInstance(_)) {
            assert!(
                !self.command_rx.replica_owned || self.compute_state.is_none(),
                "replica instance must not be reinitialized",
            );
            self.compute_state = Some(ComputeState::new(
                Arc::clone(&self.persist_clients),
                self.txns_ctx.clone(),
                self.metrics.clone(),
                Arc::clone(&self.tracing_handle),
                self.context.clone(),
                self.metrics_registry.clone(),
                self.workers_per_process,
                Arc::clone(&self.peek_permits),
            ));
        }
        self.activate_compute().unwrap().handle_compute_command(cmd);
    }

    fn activate_compute(&mut self) -> Option<ActiveComputeState<'_>> {
        if let Some(compute_state) = &mut self.compute_state {
            Some(ActiveComputeState {
                timely_worker: &mut *self.timely_worker,
                compute_state,
                response_tx: &mut self.response_tx,
            })
        } else {
            None
        }
    }

    /// Receive the next compute command.
    ///
    /// This method blocks if no command is currently available, but takes care to step the Timely
    /// worker while doing so.
    fn recv_command(&mut self) -> Result<ComputeCommand, NonceChange> {
        loop {
            let command = self.command_rx.try_recv();
            self.handle_deferred_queries();
            if let Some(state) = &mut self.compute_state {
                state.poll_query_commands(self.timely_worker, &mut self.response_tx);
            }
            if let Some(cmd) = command? {
                match cmd {
                    WorkerCommand::Compute(cmd) => return Ok(cmd),
                    WorkerCommand::Storage(cmd) => {
                        self.handle_storage_internal_command(cmd);
                        continue;
                    }
                }
            }

            // Initialization may never finish. Keep query admission, results and cleanup
            // moving without applying any of the partially received lifecycle state.
            let timeout = self.compute_state.as_ref().map(|state| {
                if state.peeks_awaiting_turn() {
                    Duration::ZERO
                } else {
                    state.server_maintenance_interval
                }
            });
            self.process_storage_guest();
            let park_cap = self.storage.as_ref().and_then(StorageGuest::park_cap);
            let timeout = match park_cap {
                Some(cap) => Some(timeout.map_or(cap, |timeout| timeout.min(cap))),
                None => timeout,
            };
            let start = Instant::now();
            if self.storage_guest_busy() {
                self.timely_worker.step();
            } else {
                self.timely_worker.step_or_park(timeout);
            }
            self.metrics
                .timely_step_duration_seconds
                .observe(start.elapsed().as_secs_f64());
        }
    }

    /// Extract commands until `InitializationComplete`, and make the worker reflect those commands.
    ///
    /// This method is meant to be a function of the commands received thus far (as recorded in the
    /// compute state command history) and the new commands from `command_rx`. It should not be a
    /// function of other characteristics, like whether the worker has managed to respond to a peek
    /// or not. Some effort goes in to narrowing our view to only the existing commands we can be sure
    /// are live at all other workers.
    ///
    /// The methodology here is to drain `command_rx` until an `InitializationComplete`, at which point
    /// the prior commands are "reconciled" in. Reconciliation takes each goal dataflow and looks for an
    /// existing "compatible" dataflow (per `compatible()`) it can repurpose, with some additional tests
    /// to be sure that we can cut over from one to the other (no additional compaction, no tails/sinks).
    /// With any connections established, old orphaned dataflows are allow to compact away, and any new
    /// dataflows are created from scratch. "Kept" dataflows are allowed to compact up to any new `as_of`.
    ///
    /// Some additional tidying happens, cleaning up pending peeks, reported frontiers, and creating a new
    /// subscribe response buffer. We will need to be vigilant with future modifications to `ComputeState` to
    /// line up changes there with clean resets here.
    fn reconcile(&mut self) -> Result<(), NonceChange> {
        // To initialize the connection, we want to drain all commands until we receive a
        // `ComputeCommand::InitializationComplete` command to form a target command state.
        let mut new_commands = Vec::new();
        loop {
            match self.recv_command()? {
                ComputeCommand::InitializationComplete => break,
                command => new_commands.push(command),
            }
        }

        // Commands we will need to apply before entering normal service.
        // These commands may include dropping existing dataflows, compacting existing dataflows,
        // and creating new dataflows, in addition to standard peek and compaction commands.
        // The result should be the same as if dropping all dataflows and running `new_commands`.
        let mut todo_commands = Vec::new();
        // We only have a compute history if we are in an initialized state
        // (i.e. after a `CreateInstance`).
        // If this is not the case, just copy `new_commands` into `todo_commands`.
        if let Some(compute_state) = &mut self.compute_state {
            // Reduce the installed commands.
            // Importantly, act as if all peeks may have been retired (as we cannot know otherwise).
            compute_state.command_history.discard_peeks();
            compute_state.command_history.reduce();

            // At this point, we need to sort out which of the *certainly installed* dataflows are
            // suitable replacements for the requested dataflows. A dataflow is "certainly installed"
            // as of a frontier if its compaction allows it to go no further. We ignore peeks for this
            // reasoning, as we cannot be certain that peeks still exist at any other worker.

            // Having reduced our installed command history retaining no peeks (above), we should be able
            // to use track down installed dataflows we can use as surrogates for requested dataflows (which
            // have retained all of their peeks, creating a more demanding `as_of` requirement).
            // NB: installed dataflows may still be allowed to further compact, and we should double check
            // this before being too confident. It should be rare without peeks, but could happen with e.g.
            // multiple outputs of a dataflow.

            // The values with which a prior `CreateInstance` was called, if it was.
            let mut old_instance_config = None;
            // Index dataflows by `export_ids().collect()`, as this is a precondition for their compatibility.
            let mut old_dataflows = BTreeMap::default();
            // Maintain allowed compaction, in case installed identifiers may have been allowed to compact.
            let mut old_frontiers = BTreeMap::default();
            for command in compute_state.command_history.iter() {
                match command {
                    ComputeCommand::CreateInstance(config) => {
                        old_instance_config = Some(config);
                    }
                    ComputeCommand::CreateDataflow(dataflow) => {
                        let export_ids = dataflow.export_ids().collect::<BTreeSet<_>>();
                        old_dataflows.insert(export_ids, dataflow);
                    }
                    ComputeCommand::AllowCompaction { id, frontier } => {
                        old_frontiers.insert(id, frontier);
                    }
                    _ => {
                        // Nothing to do in these cases.
                    }
                }
            }

            // Compaction commands that can be applied to existing dataflows.
            let mut old_compaction = BTreeMap::default();
            // Exported identifiers from dataflows we retain.
            let mut retain_ids = BTreeSet::default();

            // Traverse new commands, sorting out what remediation we can do.
            for command in new_commands.iter() {
                match command {
                    ComputeCommand::CreateDataflow(dataflow) => {
                        // Attempt to find an existing match for the dataflow.
                        let as_of = dataflow.as_of.as_ref().unwrap();
                        let export_ids = dataflow.export_ids().collect::<BTreeSet<_>>();

                        if let Some(old_dataflow) = old_dataflows.get(&export_ids) {
                            let compatible = old_dataflow.compatible_with(dataflow);
                            let uncompacted = !export_ids
                                .iter()
                                .flat_map(|id| old_frontiers.get(id))
                                .any(|frontier| {
                                    !timely::PartialOrder::less_equal(
                                        *frontier,
                                        dataflow.as_of.as_ref().unwrap(),
                                    )
                                });

                            // We cannot reconcile subscribe and copy-to sinks at the moment,
                            // because the response buffer is shared, and to a first approximation
                            // must be completely reformed.
                            let subscribe_free = dataflow.subscribe_ids().next().is_none();
                            let copy_to_free = dataflow.copy_to_ids().next().is_none();

                            // If we have replaced any dependency of this dataflow, we need to
                            // replace this dataflow, to make it use the replacement.
                            let dependencies_retained = dataflow
                                .imported_index_ids()
                                .all(|id| retain_ids.contains(&id));

                            if compatible
                                && uncompacted
                                && subscribe_free
                                && copy_to_free
                                && dependencies_retained
                            {
                                // Match found; remove the match from the deletion queue,
                                // and compact its outputs to the dataflow's `as_of`.
                                old_dataflows.remove(&export_ids);
                                for id in export_ids.iter() {
                                    old_compaction.insert(*id, as_of.clone());
                                }
                                retain_ids.extend(export_ids);
                            } else {
                                warn!(
                                    ?export_ids,
                                    ?compatible,
                                    ?uncompacted,
                                    ?subscribe_free,
                                    ?copy_to_free,
                                    ?dependencies_retained,
                                    old_as_of = ?old_dataflow.as_of,
                                    new_as_of = ?as_of,
                                    "dataflow reconciliation failed",
                                );

                                // Dump the full dataflow plans if they are incompatible, to
                                // simplify debugging hard-to-reproduce reconciliation failures.
                                if !compatible {
                                    warn!(
                                        old = ?old_dataflow,
                                        new = ?dataflow,
                                        "incompatible dataflows in reconciliation",
                                    );
                                }

                                todo_commands
                                    .push(ComputeCommand::CreateDataflow(dataflow.clone()));
                            }

                            compute_state.metrics.record_dataflow_reconciliation(
                                compatible,
                                uncompacted,
                                subscribe_free,
                                copy_to_free,
                                dependencies_retained,
                            );
                        } else {
                            todo_commands.push(ComputeCommand::CreateDataflow(dataflow.clone()));
                        }
                    }
                    ComputeCommand::CreateInstance(config) => {
                        // Cluster creation should not be performed again!
                        if old_instance_config.map_or(false, |old| !old.compatible_with(config)) {
                            halt!(
                                "new instance configuration not compatible with existing instance configuration:\n{:?}\nvs\n{:?}",
                                config,
                                old_instance_config,
                            );
                        }
                    }
                    // All other commands we apply as requested.
                    command => {
                        todo_commands.push(command.clone());
                    }
                }
            }

            // Issue compaction commands first to reclaim resources.
            for (_, dataflow) in old_dataflows.iter() {
                for id in dataflow.export_ids() {
                    // We want to drop anything that has not yet been dropped,
                    // and nothing that has already been dropped.
                    if old_frontiers.get(&id) != Some(&&Antichain::new()) {
                        old_compaction.insert(id, Antichain::new());
                    }
                }
            }
            for (&id, frontier) in &old_compaction {
                let frontier = frontier.clone();
                todo_commands.insert(0, ComputeCommand::AllowCompaction { id, frontier });
            }

            // Clean up worker-local state.
            //
            // Various aspects of `ComputeState` need to be either uninstalled, or return to a blank slate.
            // All dropped dataflows should clean up after themselves, as we plan to install new dataflows
            // re-using the same identifiers.
            // All re-used dataflows should roll back any believed communicated information (e.g. frontiers)
            // so that they recommunicate that information as if from scratch.

            // Remove all peeks, whether they have started or are still awaiting a turn.
            let queued = std::mem::take(&mut compute_state.queued_peeks);
            let pending = std::mem::take(&mut compute_state.pending_peeks);
            for peek in queued.into_iter().map(PendingPeek::Index).chain(pending) {
                // Log dropping the peek request.
                if let Some(logger) = compute_state.compute_logger.as_mut() {
                    logger.log(&peek.as_log_event(false));
                }
            }

            for (&id, collection) in compute_state.collections.iter_mut() {
                // Adjust reported frontiers:
                //  * For dataflows we continue to use, reset to ensure we report something not
                //    before the new `as_of` next.
                //  * For dataflows we drop, set to the empty frontier, to ensure we don't report
                //    anything for them.
                let retained = retain_ids.contains(&id);
                let compaction = old_compaction.remove(&id);
                let new_reported_frontier = match (retained, compaction) {
                    (true, Some(new_as_of)) => ReportedFrontier::NotReported { lower: new_as_of },
                    (true, None) => {
                        unreachable!("retained dataflows are compacted to the new as_of")
                    }
                    (false, Some(new_frontier)) => {
                        assert!(new_frontier.is_empty());
                        ReportedFrontier::Reported(new_frontier)
                    }
                    (false, None) => {
                        // Logging dataflows are implicitly retained and don't have a new as_of.
                        // Reset them to the minimal frontier.
                        ReportedFrontier::new()
                    }
                };

                collection.reset_reported_frontiers(new_reported_frontier);

                // Sink tokens should be retained for retained dataflows, and dropped for dropped
                // dataflows.
                //
                // Dropping the tokens of active subscribe and copy-tos makes them place
                // `DroppedAt` responses into the respective response buffer. We drop those buffers
                // in the next step, which ensures that we don't send out `DroppedAt` responses for
                // subscribe/copy-tos dropped during reconciliation.
                if !retained {
                    collection.sink_token = None;
                }
            }

            // We must drop the response buffers as they are global across all subscribe/copy-tos.
            // If they were broken out by `GlobalId` then we could drop only the response buffers
            // of dataflows we drop.
            compute_state.subscribe_response_buffer = Rc::new(RefCell::new(Vec::new()));
            compute_state.copy_to_response_buffer = Rc::new(RefCell::new(Vec::new()));

            // The controller expects the logging collections to be readable from the minimum time
            // initially. We cannot recreate the logging arrangements without restarting the
            // instance, but we can pad the compacted times with empty data. Doing so is sound
            // because logging collections from different replica incarnations are considered
            // distinct TVCs, so the controller doesn't expect any historical consistency from
            // these collections when it reconnects to a replica.
            //
            // TODO(database-issues#8152): Consider resolving this with controller-side reconciliation instead.
            if let Some(config) = old_instance_config {
                for id in config.logging.index_logs.values() {
                    let trace = compute_state
                        .traces
                        .remove(id)
                        .expect("logging trace exists");
                    let padded = trace.into_padded();
                    compute_state.traces.set(*id, padded);
                }
            }
        } else {
            todo_commands.clone_from(&new_commands);
        }

        // Execute the commands to bring us to `new_commands`.
        for command in todo_commands.into_iter() {
            self.handle_command(command);
        }

        // Overwrite `self.command_history` to reflect `new_commands`.
        // It is possible that there still isn't a compute state yet.
        if let Some(compute_state) = &mut self.compute_state {
            let mut command_history = ComputeCommandHistory::new(self.metrics.for_history());
            for command in new_commands.iter() {
                command_history.push(command.clone());
            }
            compute_state.command_history = command_history;
        }
        Ok(())
    }
}

/// Only globally live connections can accumulate responses while their local endpoint
/// catches up. Retirement and data arrive on the same FIFO, so no tombstones are needed.
#[derive(Default)]
struct ResponseRouting {
    queries: BTreeSet<Uuid>,
    lifecycle: Option<Uuid>,
    stashed: BTreeMap<Uuid, Vec<ComputeResponse>>,
}

impl ResponseRouting {
    fn observe(&mut self, event: ResponseEvent) -> Option<(ComputeResponse, Uuid)> {
        match event {
            ResponseEvent::QueryOpen(nonce) => {
                self.queries.insert(nonce);
            }
            ResponseEvent::QueryRetired(nonce) => {
                self.queries.remove(&nonce);
                self.stashed.remove(&nonce);
            }
            ResponseEvent::Lifecycle(nonce) => {
                if let Some(old) = self.lifecycle.replace(nonce) {
                    if old != nonce {
                        self.stashed.remove(&old);
                    }
                }
            }
            ResponseEvent::Response(response, nonce) => {
                if self.queries.contains(&nonce) || self.lifecycle == Some(nonce) {
                    return Some((response, nonce));
                }
            }
        }
        None
    }
}

/// Spawn a task to bridge between [`ClusterClient`] and [`Worker`] channels.
///
/// The [`Worker`] expects a pair of persistent channels, with punctuation marking reconnects,
/// while the [`ClusterClient`] provides a new pair of channels on each reconnect.
fn spawn_channel_adapter(
    mut client_rx: mpsc::UnboundedReceiver<(
        Uuid,
        mpsc::UnboundedReceiver<ComputeCommand>,
        mpsc::UnboundedSender<ComputeResponse>,
    )>,
    command_tx: command_channel::Sender,
    mut response_rx: mpsc::UnboundedReceiver<ResponseEvent>,
    worker_id: usize,
) {
    mz_ore::task::spawn(
        || format!("compute-channel-adapter-{worker_id}"),
        async move {
            // Each peer reader owns its receiver. Dropping a peer cancels just that reader
            // and closes just that peer's response channel, including at lifecycle replacement.
            let (event_tx, mut event_rx) = mpsc::unbounded_channel();
            let mut peers = BTreeMap::new();
            let mut lifecycle = None;
            let mut routing = ResponseRouting::default();
            loop {
                tokio::select! {
                    Some((nonce, mut commands, responses)) = client_rx.recv() => {
                        if peers.contains_key(&nonce) {
                            continue;
                        }
                        let events = event_tx.clone();
                        let task = mz_ore::task::spawn(|| "compute-peer-reader", async move {
                            while let Some(command) = commands.recv().await {
                                if events.send((nonce, Some(command))).is_err() { return; }
                            }
                            let _ = events.send((nonce, None));
                        }).abort_on_drop();
                        peers.insert(nonce, (None, responses, task));
                    }
                    Some((nonce, command)) = event_rx.recv() => {
                        let Some((role, _, _)) = peers.get_mut(&nonce) else { continue; };
                        if role.is_none() {
                            let Some(first) = &command else {
                                peers.remove(&nonce);
                                routing.stashed.remove(&nonce);
                                continue;
                            };
                            let query = matches!(first, ComputeCommand::HelloQuery { .. });
                            *role = Some(query);
                            if !query {
                                if let Some(old) = lifecycle.replace(nonce) {
                                    peers.remove(&old);
                                    routing.stashed.remove(&old);
                                }
                            }
                            if let Some(responses) = routing.stashed.remove(&nonce) {
                                for response in responses {
                                    let _ = peers[&nonce].1.send(response);
                                }
                            }
                        }
                        let query = peers[&nonce].0.expect("classified");
                        if command.is_none() {
                            peers.remove(&nonce);
                            if query && worker_id == 0 {
                                command_tx.send((None, command_channel::Origin::Query(nonce)));
                            }
                        } else {
                            let origin = if query { command_channel::Origin::Query(nonce) }
                                else { command_channel::Origin::Lifecycle(nonce) };
                            command_tx.send((command, origin));
                        }
                    }
                    Some(event) = response_rx.recv() => {
                        if let ResponseEvent::QueryRetired(nonce) = &event {
                            peers.remove(nonce);
                        }
                        let Some((response, nonce)) = routing.observe(event) else { continue; };
                        if let Some((Some(_), responses, _)) = peers.get(&nonce) {
                            let _ = responses.send(response);
                        } else {
                            routing.stashed.entry(nonce).or_default().push(response);
                        }
                    }
                }
            }
        },
    );
}

#[cfg(test)]
mod query_wire_tests;

#[cfg(test)]
mod replica_tests;
