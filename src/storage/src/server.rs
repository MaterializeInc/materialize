// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An interactive dataflow server.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mz_cluster::client::{ClusterClient, ClusterSpec, TimelyContainer};
use mz_cluster_client::client::TimelyConfig;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_repr::{GlobalId, Timestamp};
use mz_rocksdb::config::SharedWriteBufferManager;
use mz_service::client::GenericClient;
use mz_storage_client::client::{StorageClient, StorageCommand, StorageResponse};
use mz_storage_types::connections::ConnectionContext;
use mz_txn_wal::operator::TxnsContext;
use timely::PartialOrder;
use timely::progress::Antichain;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::metrics::StorageMetrics;
pub use crate::replica::ReplicaStorageResponse;
use crate::replica::{OutputFrontier, OutputGenerations, ReplicaCommand, WorkerResponse};
use crate::storage_state::{StorageInstanceContext, Worker};

/// The process-local storage runtime and its connection factory.
/// Retain a server, factory, or native endpoint for the process lifetime.
/// Dropping the last owner joins the worker threads.
pub struct StorageServer {
    runtime: Arc<Mutex<TimelyContainer<Config>>>,
    replica_owned: bool,
    replica: Option<ReplicaStorage>,
}

impl StorageServer {
    /// Creates a connection factory retaining this runtime.
    pub fn client_builder(&self) -> impl Fn() -> Box<dyn StorageClient> + use<> {
        let runtime = Arc::clone(&self.runtime);
        let replica_owned = self.replica_owned;
        move || -> Box<dyn StorageClient> {
            let client = mz_storage_client::client::RoleClient::new(ClusterClient::new(
                Arc::clone(&runtime),
            ));
            if replica_owned {
                Box::new(QueryOnly(client))
            } else {
                Box::new(client)
            }
        }
    }

    /// Takes the unique maintained endpoint, present only on process zero of a
    /// replica-owned runtime.
    pub fn take_replica(&mut self) -> Option<ReplicaStorage> {
        self.replica.take()
    }
}

/// Runtime-owned maintained storage control, independent of transport connections.
/// Send configuration before `InitializationComplete` to release waiting queries.
/// Continuously drain responses, which aggregate all global workers. Channel loss
/// is fatal. This endpoint has no reconnect or reconciliation handshake.
pub struct ReplicaStorage {
    _runtime: Arc<dyn Send + Sync>,
    commands: mpsc::UnboundedSender<ReplicaCommand>,
    worker: std::thread::Thread,
    responses: mpsc::UnboundedReceiver<(usize, WorkerResponse)>,
    next_sequence: u64,
    peers: usize,
    inputs: BTreeMap<(u64, GlobalId), (Vec<Antichain<Timestamp>>, Antichain<Timestamp>)>,
    current: BTreeMap<GlobalId, u64>,
    restarts: BTreeSet<u64>,
    starts: BTreeMap<u64, BTreeSet<usize>>,
    output_generations: OutputGenerations,
    outputs: BTreeMap<(u64, GlobalId), OutputFrontier>,
}

impl ReplicaStorage {
    /// Enqueues a maintained command before worker bookkeeping and async resume
    /// observation. Query commands and handshakes belong on query connections.
    /// Returns a unique monotone sequence, identifying the attempt for Run commands.
    pub fn send(&mut self, command: StorageCommand) -> u64 {
        assert!(
            !matches!(
                command,
                StorageCommand::Hello { .. }
                    | StorageCommand::HelloQuery { .. }
                    | StorageCommand::SubscribeObservations
                    | StorageCommand::RunOneshotIngestion(_)
                    | StorageCommand::CancelOneshotIngestion(_)
            ),
            "query and transport commands must use query connections"
        );
        let sequence = self.next_sequence;
        self.next_sequence = sequence.checked_add(1).expect("native sequence exhausted");
        if let Some((id, inputs)) = crate::replica::inputs(&command) {
            self.starts.insert(sequence, BTreeSet::new());
            if let Some(previous) = self.current.insert(id, sequence) {
                self.restarts.remove(&previous);
            }
            for input in inputs {
                let minimum = Antichain::from_elem(Timestamp::MIN);
                self.inputs.insert(
                    (sequence, input),
                    (vec![minimum.clone(); self.peers], minimum),
                );
            }
        }
        if let StorageCommand::AllowCompaction(id, frontier) = &command {
            if frontier.is_empty() {
                if let Some(previous) = self.current.remove(id) {
                    self.restarts.remove(&previous);
                }
            }
        }
        for (id, generation) in self.output_generations.observe(sequence, &command) {
            self.outputs
                .entry((generation, id))
                .or_insert_with(|| OutputFrontier::new(self.peers));
        }
        self.commands
            .send(ReplicaCommand::Storage(sequence, command))
            .expect("replica storage ingress lost");
        self.worker.unpark();
        sequence
    }

    /// Replies once to an attempt-scoped Kafka pre-open request. `None` denies.
    /// The follower must recheck definition, eligibility and input protection.
    /// `max_age` must not exceed unchanged protection grace minus heartbeat margin
    /// and any age already consumed by the protection proof. It is measured from
    /// the worker's request start, including response and blocking-pool delays.
    pub fn reply_kafka_pre_open(
        &mut self,
        request: uuid::Uuid,
        execution: u64,
        id: GlobalId,
        max_age: Option<Duration>,
    ) {
        let max_age = max_age.filter(|_| {
            self.current.get(&id) == Some(&execution) && !self.restarts.contains(&execution)
        });
        self.commands
            .send(ReplicaCommand::KafkaPreOpenReply {
                request,
                execution,
                id,
                max_age,
            })
            .expect("replica storage ingress lost");
        self.worker.unpark();
    }

    /// Receives a replica-wide maintained response. Cancel safe, retaining partial
    /// worker responses in the aggregation state. Progress channel loss is fatal.
    pub async fn recv(&mut self) -> Result<Option<ReplicaStorageResponse>, anyhow::Error> {
        loop {
            let (worker, response) = self.responses.recv().await.expect("replica progress lost");
            let WorkerResponse {
                output_generation,
                response,
            } = response;
            match response {
                response @ ReplicaStorageResponse::KafkaPreOpen { .. } => {
                    return Ok(Some(response));
                }
                ReplicaStorageResponse::ExecutionStarted { execution } => {
                    let workers = self.starts.get_mut(&execution).expect("admitted execution");
                    assert!(
                        workers.insert(worker),
                        "duplicate installation acknowledgement"
                    );
                    if workers.len() == self.peers {
                        self.starts.remove(&execution);
                        return Ok(Some(ReplicaStorageResponse::ExecutionStarted { execution }));
                    }
                }
                ReplicaStorageResponse::Response(StorageResponse::FrontierUpper(id, upper)) => {
                    let generation = output_generation.expect("tagged output");
                    let output = self
                        .outputs
                        .get_mut(&(generation, id))
                        .expect("admitted output");
                    if let Some(upper) = output.update(worker, upper) {
                        return Ok(Some(ReplicaStorageResponse::Response(
                            StorageResponse::FrontierUpper(id, upper),
                        )));
                    }
                }
                ReplicaStorageResponse::Response(StorageResponse::DroppedId(id)) => {
                    let generation = output_generation.expect("tagged drop");
                    let output = self
                        .outputs
                        .get_mut(&(generation, id))
                        .expect("admitted output");
                    if output.drop_worker(worker) {
                        self.outputs.remove(&(generation, id));
                        return Ok(Some(ReplicaStorageResponse::Response(
                            StorageResponse::DroppedId(id),
                        )));
                    }
                }
                ReplicaStorageResponse::Response(response) => {
                    return Ok(Some(ReplicaStorageResponse::Response(response)));
                }
                ReplicaStorageResponse::ExecutionInput {
                    execution,
                    input,
                    frontier,
                } => {
                    let (workers, reported) = self
                        .inputs
                        .get_mut(&(execution, input))
                        .expect("progress for admitted input");
                    assert!(PartialOrder::less_equal(&workers[worker], &frontier));
                    workers[worker] = frontier;
                    let frontier: Antichain<_> =
                        workers.iter().flat_map(|f| f.iter().copied()).collect();
                    if *reported != frontier {
                        *reported = frontier.clone();
                        if frontier.is_empty() {
                            self.inputs.remove(&(execution, input));
                        }
                        return Ok(Some(ReplicaStorageResponse::ExecutionInput {
                            execution,
                            input,
                            frontier,
                        }));
                    }
                }
                ReplicaStorageResponse::RestartRequested { execution, id } => {
                    if self.current.get(&id) == Some(&execution) && self.restarts.insert(execution)
                    {
                        return Ok(Some(ReplicaStorageResponse::RestartRequested {
                            execution,
                            id,
                        }));
                    }
                }
            }
        }
    }
}

type ReplicaChannels = (
    mpsc::UnboundedSender<ReplicaCommand>,
    std::thread::Thread,
    mpsc::UnboundedReceiver<(usize, WorkerResponse)>,
    usize,
);

/// Native storage channels awaiting their host's runtime owner.
/// The host retains execution independently of listener and connection lifetimes.
pub struct ReplicaStorageBuilder(pub(crate) ReplicaChannels);

impl ReplicaStorageBuilder {
    /// Attaches the process-lifetime runtime owner to the native endpoint.
    pub fn build(self, runtime: Arc<impl Send + Sync + 'static>) -> ReplicaStorage {
        let (commands, worker, responses, peers) = self.0;
        ReplicaStorage {
            _runtime: runtime,
            commands,
            worker,
            responses,
            next_sequence: 1,
            peers,
            inputs: BTreeMap::new(),
            current: BTreeMap::new(),
            restarts: BTreeSet::new(),
            starts: BTreeMap::new(),
            output_generations: OutputGenerations::default(),
            outputs: BTreeMap::new(),
        }
    }
}

/// Applies storage role validation and retains the Timely host for a guest connection.
pub fn guest_client<C: StorageClient + 'static>(
    client: C,
    replica_owned: bool,
    runtime: Arc<impl Send + Sync + 'static>,
) -> Box<dyn StorageClient> {
    let client = mz_storage_client::client::RoleClient::new(client);
    let inner: Box<dyn StorageClient> = if replica_owned {
        Box::new(QueryOnly(client))
    } else {
        Box::new(client)
    };
    Box::new(GuestClient {
        inner,
        _runtime: runtime,
    })
}

struct GuestClient {
    inner: Box<dyn StorageClient>,
    _runtime: Arc<dyn Send + Sync>,
}

impl std::fmt::Debug for GuestClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GuestClient").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl GenericClient<StorageCommand, StorageResponse> for GuestClient {
    async fn send(&mut self, command: StorageCommand) -> anyhow::Result<()> {
        self.inner.send(command).await
    }

    async fn recv(&mut self) -> anyhow::Result<Option<StorageResponse>> {
        self.inner.recv().await
    }
}

// RoleClient owns handshake validation. This runtime boundary additionally
// excludes lifecycle connections without changing the shared storage protocol.
#[derive(Debug)]
struct QueryOnly<C>(C);

#[async_trait::async_trait]
impl<C: StorageClient> GenericClient<StorageCommand, StorageResponse> for QueryOnly<C> {
    async fn send(&mut self, command: StorageCommand) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(
                command,
                StorageCommand::HelloQuery { .. }
                    | StorageCommand::SubscribeObservations
                    | StorageCommand::RunOneshotIngestion(_)
                    | StorageCommand::CancelOneshotIngestion(_)
            ),
            "native storage transport accepts only query commands"
        );
        self.0.send(command).await
    }

    async fn recv(&mut self) -> anyhow::Result<Option<StorageResponse>> {
        self.0.recv().await
    }
}

/// Configures a dataflow server.
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
    /// Function to get wall time now.
    pub now: NowFn,
    /// Configuration for source and sink connection.
    pub connection_context: ConnectionContext,
    /// Other configuration for storage instances.
    pub instance_context: StorageInstanceContext,

    /// Metrics for storage
    pub metrics: StorageMetrics,
    /// Shared rocksdb write buffer manager
    pub shared_rocksdb_write_buffer_manager: SharedWriteBufferManager,
}

/// Initiates a timely dataflow computation, processing storage commands.
pub async fn serve(
    timely_config: TimelyConfig,
    metrics_registry: &MetricsRegistry,
    persist_clients: Arc<PersistClientCache>,
    txns_ctx: TxnsContext,
    tracing_handle: Arc<TracingHandle>,
    now: NowFn,
    connection_context: ConnectionContext,
    instance_context: StorageInstanceContext,
) -> Result<impl Fn() -> Box<dyn StorageClient> + use<>, anyhow::Error> {
    Ok(serve_with_replica(
        timely_config,
        false,
        metrics_registry,
        persist_clients,
        txns_ctx,
        tracing_handle,
        now,
        connection_context,
        instance_context,
    )
    .await?
    .client_builder())
}

/// Starts the same storage runtime with optional native maintained control.
/// Native ownership must agree across all processes. It disables lifecycle
/// transport and exposes the maintained endpoint only on process zero.
pub async fn serve_with_replica(
    timely_config: TimelyConfig,
    replica_owned: bool,
    metrics_registry: &MetricsRegistry,
    persist_clients: Arc<PersistClientCache>,
    txns_ctx: TxnsContext,
    tracing_handle: Arc<TracingHandle>,
    now: NowFn,
    connection_context: ConnectionContext,
    instance_context: StorageInstanceContext,
) -> Result<StorageServer, anyhow::Error> {
    let (ready_tx, ready_rx) = if replica_owned && timely_config.process == 0 {
        let (tx, rx) = oneshot::channel();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    let config = Config {
        replica_owned,
        replica_ready: Arc::new(Mutex::new(ready_tx)),
        persist_clients,
        txns_ctx,
        tracing_handle,
        now,
        connection_context,
        instance_context,
        metrics: StorageMetrics::register_with(metrics_registry),
        // The shared RocksDB `WriteBufferManager` is shared between the workers.
        // It protects (behind a shared mutex) a `Weak` that will be upgraded and shared when the
        // first worker attempts to initialize it.
        shared_rocksdb_write_buffer_manager: Default::default(),
    };
    let tokio_executor = tokio::runtime::Handle::current();

    let timely_container = config.build_cluster(timely_config, tokio_executor).await?;
    let timely_container = Arc::new(Mutex::new(timely_container));

    let replica = match ready_rx {
        Some(rx) => {
            let (commands, worker, responses, peers) = rx.await?;
            Some(
                ReplicaStorageBuilder((commands, worker, responses, peers))
                    .build(Arc::clone(&timely_container)),
            )
        }
        None => None,
    };
    Ok(StorageServer {
        runtime: timely_container,
        replica_owned,
        replica,
    })
}

impl ClusterSpec for Config {
    type Command = StorageCommand;
    type Response = StorageResponse;

    const NAME: &str = "storage";

    fn run_worker(
        &self,
        timely_worker: &mut TimelyWorker,
        client_rx: mpsc::UnboundedReceiver<(
            Uuid,
            mpsc::UnboundedReceiver<StorageCommand>,
            mpsc::UnboundedSender<StorageResponse>,
        )>,
    ) {
        let mut worker = Worker::new(
            timely_worker,
            client_rx,
            self.metrics.clone(),
            self.now.clone(),
            self.connection_context.clone(),
            self.instance_context.clone(),
            Arc::clone(&self.persist_clients),
            self.txns_ctx.clone(),
            Arc::clone(&self.tracing_handle),
            self.shared_rocksdb_write_buffer_manager.clone(),
        );
        if self.replica_owned {
            let (commands, responses) = worker.enable_replica();
            if worker.timely_worker.index() == 0 {
                self.replica_ready
                    .lock()
                    .expect("poisoned")
                    .take()
                    .expect("replica owner registered")
                    .send((
                        commands,
                        std::thread::current(),
                        responses,
                        worker.timely_worker.peers(),
                    ))
                    .unwrap_or_else(|_| panic!("replica owner lost during startup"));
            }
        }
        worker.run();
    }
}

#[cfg(test)]
pub(crate) mod replica_tests;
