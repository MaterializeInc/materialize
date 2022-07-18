// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An interactive dataflow server.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use crossbeam_channel::TryRecvError;
use mz_persist_client::PersistConfig;
use timely::communication::initialize::WorkerGuards;
use timely::communication::Allocate;
use timely::execute::execute_from;
use timely::worker::Worker as TimelyWorker;
use timely::WorkerConfig;
use tokio::sync::mpsc;

use mz_compute_client::command::ComputeCommand;
use mz_compute_client::response::ComputeResponse;
use mz_compute_client::service::ComputeClient;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_persist_client::cache::PersistClientCache;
use mz_service::local::LocalClient;
use mz_storage::types::connections::ConnectionContext;

use crate::communication::initialize_networking;
use crate::compute_state::ActiveComputeState;
use crate::compute_state::ComputeState;
use crate::SinkBaseMetrics;
use crate::{TraceManager, TraceMetrics};

/// Configuration of the cluster we will spin up
pub struct CommunicationConfig {
    /// Number of per-process worker threads
    pub threads: usize,
    /// Identity of this process
    pub process: usize,
    /// Addresses of all processes
    pub addresses: Vec<String>,
}

/// Configures a dataflow server.
pub struct Config {
    /// The number of worker threads to spawn.
    pub workers: usize,
    /// Configuration for the communication mesh
    pub comm_config: CommunicationConfig,
    /// Function to get wall time now.
    pub now: NowFn,
    /// Metrics registry through which dataflow metrics will be reported.
    pub metrics_registry: MetricsRegistry,
    /// Configuration for sink connections.
    // TODO: remove when sinks move to storage.
    pub connection_context: ConnectionContext,
}

/// A handle to a running dataflow server.
///
/// Dropping this object will block until the dataflow computation ceases.
pub struct Server {
    _worker_guards: WorkerGuards<()>,
}

/// Initiates a timely dataflow computation, processing compute commands.
pub fn serve(
    config: Config,
) -> Result<(Server, impl Fn() -> Box<dyn ComputeClient>), anyhow::Error> {
    assert!(config.workers > 0);

    // Various metrics related things.
    let sink_metrics = SinkBaseMetrics::register_with(&config.metrics_registry);
    let trace_metrics = TraceMetrics::register_with(&config.metrics_registry);
    // Bundle metrics to conceal complexity.
    let metrics_bundle = (sink_metrics, trace_metrics);

    let (client_txs, client_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
        .map(|_| crossbeam_channel::unbounded())
        .unzip();
    let client_rxs: Mutex<Vec<_>> = Mutex::new(client_rxs.into_iter().map(Some).collect());

    let tokio_executor = tokio::runtime::Handle::current();

    let (builders, other) =
        initialize_networking(config.comm_config).map_err(|e| anyhow!("{e}"))?;

    let persist_clients = PersistClientCache::new(
        PersistConfig::new(config.now.clone()),
        &config.metrics_registry,
    );
    let persist_clients = Arc::new(tokio::sync::Mutex::new(persist_clients));

    let worker_guards = execute_from(
        builders,
        other,
        WorkerConfig::default(),
        move |timely_worker| {
            let timely_worker_index = timely_worker.index();
            let _tokio_guard = tokio_executor.enter();
            let client_rx = client_rxs.lock().unwrap()[timely_worker_index % config.workers]
                .take()
                .unwrap();
            let (_sink_metrics, _trace_metrics) = metrics_bundle.clone();
            let persist_clients = Arc::clone(&persist_clients);
            Worker {
                timely_worker,
                client_rx,
                compute_state: None,
                metrics_bundle: metrics_bundle.clone(),
                connection_context: config.connection_context.clone(),
                persist_clients,
            }
            .run()
        },
    )
    .map_err(|e| anyhow!("{e}"))?;
    let worker_threads = worker_guards
        .guards()
        .iter()
        .map(|g| g.thread().clone())
        .collect::<Vec<_>>();
    let client_builder = move || {
        let (command_txs, command_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
            .map(|_| crossbeam_channel::unbounded())
            .unzip();
        let (response_txs, response_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
            .map(|_| mpsc::unbounded_channel())
            .unzip();
        for (client_tx, channels) in client_txs
            .iter()
            .zip(command_rxs.into_iter().zip(response_txs))
        {
            client_tx
                .send(channels)
                .expect("worker should not drop first");
        }
        let client =
            LocalClient::new_partitioned(response_rxs, command_txs, worker_threads.clone());
        Box::new(client) as Box<dyn ComputeClient>
    };
    let server = Server {
        _worker_guards: worker_guards,
    };
    Ok((server, client_builder))
}

type CommandReceiver = crossbeam_channel::Receiver<ComputeCommand>;
type ResponseSender = mpsc::UnboundedSender<ComputeResponse>;

/// State maintained for each worker thread.
///
/// Much of this state can be viewed as local variables for the worker thread,
/// holding state that persists across function calls.
struct Worker<'w, A: Allocate> {
    /// The underlying Timely worker.
    timely_worker: &'w mut TimelyWorker<A>,
    /// The channel over which communication handles for newly connected clients
    /// are delivered.
    client_rx: crossbeam_channel::Receiver<(CommandReceiver, ResponseSender)>,
    compute_state: Option<ComputeState>,
    /// Metrics bundle.
    metrics_bundle: (SinkBaseMetrics, TraceMetrics),
    /// Configuration for sink connections.
    // TODO: remove when sinks move to storage.
    pub connection_context: ConnectionContext,
    /// A process-global cache of (blob_uri, consensus_uri) -> PersistClient.
    /// This is intentionally shared between workers
    persist_clients: Arc<tokio::sync::Mutex<PersistClientCache>>,
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
    fn run_client(&mut self, command_rx: CommandReceiver, mut response_tx: ResponseSender) {
        // A new client has connected. Reset state about what we have reported.
        //
        // TODO(benesch,mcsherry): introduce a `InitializationComplete` command
        // and only clear the state that is missing after initialization
        // completes.
        if let Some(state) = self.activate_compute(&mut response_tx) {
            state.compute_state.reported_frontiers.clear();
            state.compute_state.pending_peeks.clear();
            state
                .compute_state
                .sink_tokens
                .retain(|_, token| !token.is_tail);
            state
                .compute_state
                .tail_response_buffer
                .borrow_mut()
                .clear();
        }

        let mut shutdown = false;
        while !shutdown {
            // Enable trace compaction.
            if let Some(compute_state) = &mut self.compute_state {
                compute_state.traces.maintenance();
            }

            // Ask Timely to execute a unit of work. If Timely decides there's
            // nothing to do, it will park the thread. We rely on another thread
            // unparking us when there's new work to be done, e.g., when sending
            // a command or when new Kafka messages have arrived.
            self.timely_worker.step_or_park(None);

            // Report frontier information back the coordinator.
            if let Some(mut compute_state) = self.activate_compute(&mut response_tx) {
                compute_state.report_compute_frontiers();
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
                        shutdown = true;
                    }
                }
            }
            for cmd in cmds {
                let mut should_drop_compute = false;
                match &cmd {
                    ComputeCommand::CreateInstance(config) => {
                        self.compute_state = Some(ComputeState {
                            replica_id: config.replica_id,
                            traces: TraceManager::new(
                                self.metrics_bundle.1.clone(),
                                self.timely_worker.index(),
                            ),
                            sink_tokens: HashMap::new(),
                            tail_response_buffer: std::rc::Rc::new(std::cell::RefCell::new(
                                Vec::new(),
                            )),
                            sink_write_frontiers: HashMap::new(),
                            pending_peeks: Vec::new(),
                            reported_frontiers: HashMap::new(),
                            sink_metrics: self.metrics_bundle.0.clone(),
                            compute_logger: None,
                            connection_context: self.connection_context.clone(),
                            persist_clients: Arc::clone(&self.persist_clients),
                            dataflow_descriptions: HashMap::new(),
                        });
                    }
                    ComputeCommand::DropInstance => {
                        should_drop_compute = true;
                    }
                    _ => (),
                }

                self.activate_compute(&mut response_tx)
                    .unwrap()
                    .handle_compute_command(cmd);
                if should_drop_compute {
                    self.compute_state = None;
                }
            }

            if let Some(mut compute_state) = self.activate_compute(&mut response_tx) {
                compute_state.process_peeks();
                compute_state.process_tails();
            }
        }
        if let Some(mut compute_state) = self.activate_compute(&mut response_tx) {
            compute_state.compute_state.traces.del_all_traces();
            compute_state.shutdown_logging();
        }
    }

    fn activate_compute<'a>(
        &'a mut self,
        response_tx: &'a mut ResponseSender,
    ) -> Option<ActiveComputeState<'a, A>> {
        if let Some(compute_state) = &mut self.compute_state {
            Some(ActiveComputeState {
                timely_worker: &mut *self.timely_worker,
                compute_state,
                response_tx,
            })
        } else {
            None
        }
    }
}
