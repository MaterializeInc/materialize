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
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Mutex;
use std::time::Instant;

use anyhow::anyhow;
use crossbeam_channel::TryRecvError;
use timely::communication::initialize::WorkerGuards;
use timely::communication::Allocate;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc;

use mz_dataflow_types::client::{
    Command, ComputeCommand, ComputeResponse, LocalClient, LocalComputeClient, LocalStorageClient,
    Sender, StorageResponse,
};
use mz_dataflow_types::sources::AwsExternalId;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;

use crate::boundary::BoundaryHook;
use crate::boundary::EventLinkBoundary;
use crate::boundary::{ComputeReplay, StorageCapture};
use crate::compute::arrangement::manager::{TraceManager, TraceMetrics};
use crate::compute::compute_state::ActiveComputeState;
use crate::compute::compute_state::ComputeState;
use crate::sink::SinkBaseMetrics;
use crate::storage::decode::metrics::DecodeMetrics;
use crate::storage::render::sources::PersistedSourceManager;
use crate::storage::source::metrics::SourceBaseMetrics;
use crate::storage::storage_state::ActiveStorageState;
use crate::storage::storage_state::StorageState;

/// Configures a dataflow server.
pub struct Config {
    /// The number of worker threads to spawn.
    pub workers: usize,
    /// The Timely configuration
    pub timely_config: timely::Config,
    /// Whether the server is running in experimental mode.
    pub experimental_mode: bool,
    /// Function to get wall time now.
    pub now: NowFn,
    /// Metrics registry through which dataflow metrics will be reported.
    pub metrics_registry: MetricsRegistry,
    /// A handle to a persistence runtime, if persistence is enabled.
    pub persister: Option<mz_persist::client::RuntimeClient>,
    /// An external ID to use for all AWS AssumeRole operations.
    pub aws_external_id: AwsExternalId,
}

/// A handle to a running dataflow server.
///
/// Dropping this object will block until the dataflow computation ceases.
pub struct Server {
    _worker_guards: WorkerGuards<()>,
}

/// Initiates a timely dataflow computation, processing materialized commands.
///
/// It uses the default [EventLinkBoundary] to host both compute and storage dataflows.
pub fn serve(
    config: Config,
) -> Result<(Server, BoundaryHook<LocalStorageClient>, LocalComputeClient), anyhow::Error> {
    let workers = config.workers as u64;
    let (requests_tx, requests_rx) = tokio::sync::mpsc::unbounded_channel();
    let (server, storage_client, compute_client) = serve_boundary(config, move |_| {
        let boundary = Rc::new(RefCell::new(EventLinkBoundary::new(requests_tx.clone())));
        (Rc::clone(&boundary), boundary)
    })?;

    Ok((
        server,
        BoundaryHook::new(storage_client, requests_rx, workers),
        compute_client,
    ))
}

/// Initiates a timely dataflow computation, processing materialized commands.
///
/// * `create_boundary`: A function to obtain the worker-local boundary components.
pub fn serve_boundary_requests<
    SC: StorageCapture,
    CR: ComputeReplay,
    B: Fn(usize) -> (SC, CR) + Send + Sync + 'static,
>(
    config: Config,
    requests: tokio::sync::mpsc::UnboundedReceiver<mz_dataflow_types::SourceInstanceRequest>,
    create_boundary: B,
) -> Result<(Server, BoundaryHook<LocalStorageClient>), anyhow::Error> {
    let workers = config.workers as u64;
    let (server, storage_client, _compute_client) = serve_boundary(config, create_boundary)?;
    Ok((
        server,
        crate::boundary::BoundaryHook::new(storage_client, requests, workers),
    ))
}
/// Initiates a timely dataflow computation, processing materialized commands.
///
/// * `create_boundary`: A function to obtain the worker-local boundary components.
pub fn serve_boundary<
    SC: StorageCapture,
    CR: ComputeReplay,
    B: Fn(usize) -> (SC, CR) + Send + Sync + 'static,
>(
    config: Config,
    create_boundary: B,
) -> Result<(Server, LocalStorageClient, LocalComputeClient), anyhow::Error> {
    assert!(config.workers > 0);

    // Various metrics related things.
    let source_metrics = SourceBaseMetrics::register_with(&config.metrics_registry);
    let sink_metrics = SinkBaseMetrics::register_with(&config.metrics_registry);
    let decode_metrics = DecodeMetrics::register_with(&config.metrics_registry);
    let trace_metrics = TraceMetrics::register_with(&config.metrics_registry);
    // Bundle metrics to conceal complexity.
    let metrics_bundle = (source_metrics, sink_metrics, decode_metrics, trace_metrics);

    // Construct endpoints for each thread that will receive the coordinator's
    // sequenced command stream and send the responses to the coordinator.
    //
    // TODO(benesch): package up this idiom of handing out ownership of N items
    // to the N timely threads that will be spawned. The Mutex<Vec<Option<T>>>
    // is hard to read through.
    let (command_txs, command_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
        .map(|_| crossbeam_channel::unbounded())
        .unzip();
    let (storage_response_txs, storage_response_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
        .map(|_| mpsc::unbounded_channel())
        .unzip();
    let (compute_response_txs, compute_response_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
        .map(|_| mpsc::unbounded_channel())
        .unzip();
    // Mutexes around a vector of optional (take-able) pairs of (tx, rx) for worker/client communication.
    let command_channels: Mutex<Vec<_>> = Mutex::new(command_rxs.into_iter().map(Some).collect());
    let storage_response_channels: Mutex<Vec<_>> =
        Mutex::new(storage_response_txs.into_iter().map(Some).collect());
    let compute_response_channels: Mutex<Vec<_>> =
        Mutex::new(compute_response_txs.into_iter().map(Some).collect());

    let tokio_executor = tokio::runtime::Handle::current();
    let now = config.now;
    let aws_external_id = config.aws_external_id.clone();

    let worker_guards = timely::execute::execute(config.timely_config, move |timely_worker| {
        let timely_worker_index = timely_worker.index();
        let timely_worker_peers = timely_worker.peers();
        let (storage_boundary, compute_boundary) = create_boundary(timely_worker_index);
        let _tokio_guard = tokio_executor.enter();
        let command_rx = command_channels.lock().unwrap()[timely_worker_index % config.workers]
            .take()
            .unwrap();
        let storage_response_tx = storage_response_channels.lock().unwrap()
            [timely_worker_index % config.workers]
            .take()
            .unwrap();
        let compute_response_tx = compute_response_channels.lock().unwrap()
            [timely_worker_index % config.workers]
            .take()
            .unwrap();
        let (source_metrics, _sink_metrics, decode_metrics, _trace_metrics) =
            metrics_bundle.clone();
        Worker {
            timely_worker,
            command_rx,
            compute_state: None,
            storage_state: StorageState {
                table_state: HashMap::new(),
                source_descriptions: HashMap::new(),
                source_uppers: HashMap::new(),
                ts_source_mapping: HashMap::new(),
                ts_histories: HashMap::default(),
                persisted_sources: PersistedSourceManager::new(),
                decode_metrics,
                persist: config.persister.clone(),
                reported_frontiers: HashMap::new(),
                last_bindings_feedback: Instant::now(),
                now: now.clone(),
                source_metrics,
                aws_external_id: aws_external_id.clone(),
                timely_worker_index,
                timely_worker_peers,
            },
            storage_boundary,
            storage_response_tx,
            compute_boundary,
            compute_response_tx,
            metrics_bundle: metrics_bundle.clone(),
        }
        .run()
    })
    .map_err(|e| anyhow!("{}", e))?;
    let worker_threads = worker_guards
        .guards()
        .iter()
        .map(|g| g.thread().clone())
        .collect::<Vec<_>>();
    let storage_client = LocalClient::new(
        storage_response_rxs,
        command_txs
            .iter()
            .map(|tx| {
                let tx = tx.clone();
                Sender::new(move |cmd| {
                    tx.send(Command::Storage(cmd))
                        .expect("worker command receiver should not drop first")
                })
            })
            .collect(),
        worker_threads.clone(),
    );
    let compute_client = LocalClient::new(
        compute_response_rxs,
        command_txs
            .into_iter()
            .map(|tx| {
                Sender::new(move |cmd| {
                    tx.send(Command::Compute(cmd))
                        .expect("worker command receiver should not drop first")
                })
            })
            .collect(),
        worker_threads,
    );
    let server = Server {
        _worker_guards: worker_guards,
    };
    Ok((server, storage_client, compute_client))
}

/// State maintained for each worker thread.
///
/// Much of this state can be viewed as local variables for the worker thread,
/// holding state that persists across function calls.
struct Worker<'w, A, SC, CR>
where
    A: Allocate,
    SC: StorageCapture,
    CR: ComputeReplay,
{
    /// The underlying Timely worker.
    timely_worker: &'w mut TimelyWorker<A>,
    /// The channel from which commands are drawn.
    command_rx: crossbeam_channel::Receiver<Command>,
    /// The state associated with rendering dataflows.
    compute_state: Option<ComputeState>,
    /// The state associated with collection ingress and egress.
    storage_state: StorageState,
    /// The boundary between storage and compute layers, storage side.
    storage_boundary: SC,
    /// The channel over which storage responses are reported.
    storage_response_tx: mpsc::UnboundedSender<StorageResponse>,
    /// The boundary between storage and compute layers, compute side.
    compute_boundary: CR,
    /// The channel over which compute responses are reported.
    compute_response_tx: mpsc::UnboundedSender<ComputeResponse>,
    /// Metrics bundle.
    metrics_bundle: (
        SourceBaseMetrics,
        SinkBaseMetrics,
        DecodeMetrics,
        TraceMetrics,
    ),
}

impl<'w, A, SC, CR> Worker<'w, A, SC, CR>
where
    A: Allocate + 'w,
    SC: StorageCapture,
    CR: ComputeReplay,
{
    /// Draws from `dataflow_command_receiver` until shutdown.
    fn run(&mut self) {
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
            if let Some(mut compute_state) = self.activate_compute() {
                compute_state.report_compute_frontiers();
            }
            self.activate_storage().update_rt_timestamps();
            self.activate_storage()
                .report_conditional_frontier_progress();

            // Handle any received commands.
            let mut cmds = vec![];
            let mut empty = false;
            while !empty {
                match self.command_rx.try_recv() {
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
                    Command::Compute(ComputeCommand::CreateInstance(_logging)) => {
                        self.compute_state = Some(ComputeState {
                            traces: TraceManager::new(
                                self.metrics_bundle.3.clone(),
                                self.timely_worker.index(),
                            ),
                            dataflow_tokens: HashMap::new(),
                            tail_response_buffer: std::rc::Rc::new(std::cell::RefCell::new(
                                Vec::new(),
                            )),
                            sink_write_frontiers: HashMap::new(),
                            pending_peeks: Vec::new(),
                            reported_frontiers: HashMap::new(),
                            sink_metrics: self.metrics_bundle.1.clone(),
                            materialized_logger: None,
                        });
                    }
                    Command::Compute(ComputeCommand::DropInstance) => {
                        should_drop_compute = true;
                    }
                    _ => (),
                }

                self.handle_command(cmd);

                if should_drop_compute {
                    self.compute_state = None;
                }
            }

            if let Some(mut compute_state) = self.activate_compute() {
                compute_state.process_peeks();
                compute_state.process_tails();
            }
        }
        if let Some(mut compute_state) = self.activate_compute() {
            compute_state.compute_state.traces.del_all_traces();
            compute_state.shutdown_logging();
        }
    }

    fn activate_compute(&mut self) -> Option<ActiveComputeState<A, CR>> {
        if let Some(compute_state) = &mut self.compute_state {
            Some(ActiveComputeState {
                timely_worker: &mut *self.timely_worker,
                compute_state,
                response_tx: &mut self.compute_response_tx,
                boundary: &mut self.compute_boundary,
            })
        } else {
            None
        }
    }
    fn activate_storage(&mut self) -> ActiveStorageState<A, SC> {
        ActiveStorageState {
            timely_worker: &mut *self.timely_worker,
            storage_state: &mut self.storage_state,
            response_tx: &mut self.storage_response_tx,
            boundary: &mut self.storage_boundary,
        }
    }

    fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::Compute(cmd) => self.activate_compute().unwrap().handle_compute_command(cmd),
            Command::Storage(cmd) => self.activate_storage().handle_storage_command(cmd),
        }
    }
}
