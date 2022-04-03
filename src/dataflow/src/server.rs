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
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Mutex;
use std::time::Instant;

use anyhow::anyhow;
use crossbeam_channel::TryRecvError;
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::TraceReader;
use timely::communication::initialize::WorkerGuards;
use timely::communication::Allocate;
use timely::dataflow::operators::unordered_input::UnorderedHandle;
use timely::dataflow::operators::ActivateCapability;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc;

use mz_dataflow_types::client::Peek;
use mz_dataflow_types::client::{
    ComputeCommand, ComputeResponse, LocalClient, LocalComputeClient, LocalStorageClient,
    StorageCommand, StorageResponse,
};
use mz_dataflow_types::sources::AwsExternalId;
use mz_dataflow_types::PeekResponse;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::result::ResultExt;
use mz_repr::{DatumVec, Diff, Row, RowArena, Timestamp};

use crate::arrangement::manager::{TraceBundle, TraceManager, TraceMetrics};
use crate::event::ActivatedEventPusher;
use crate::metrics::Metrics;
use crate::render::sources::PersistedSourceManager;
use crate::sink::SinkBaseMetrics;
use crate::source::metrics::SourceBaseMetrics;

use crate::server::boundary::BoundaryHook;

pub mod boundary;
mod compute_state;
mod storage_state;
pub mod tcp_boundary;

use crate::server::boundary::EventLinkBoundary;
use boundary::{ComputeReplay, StorageCapture};
use compute_state::ActiveComputeState;
pub(crate) use compute_state::ComputeState;
use storage_state::ActiveStorageState;
pub(crate) use storage_state::StorageState;

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
        crate::server::boundary::BoundaryHook::new(storage_client, requests, workers),
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
    let unspecified_metrics = Metrics::register_with(&config.metrics_registry);
    let trace_metrics = TraceMetrics::register_with(&config.metrics_registry);
    // Bundle metrics to conceal complexity.
    let metrics_bundle = (
        source_metrics,
        sink_metrics,
        unspecified_metrics,
        trace_metrics,
    );

    // Construct endpoints for each thread that will receive the coordinator's
    // sequenced command stream and send the responses to the coordinator.
    //
    // TODO(benesch): package up this idiom of handing out ownership of N items
    // to the N timely threads that will be spawned. The Mutex<Vec<Option<T>>>
    // is hard to read through.
    let (storage_response_txs, storage_response_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
        .map(|_| mpsc::unbounded_channel())
        .unzip();
    let (storage_command_txs, storage_command_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
        .map(|_| crossbeam_channel::unbounded())
        .unzip();
    let (compute_response_txs, compute_response_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
        .map(|_| mpsc::unbounded_channel())
        .unzip();
    let (compute_command_txs, compute_command_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
        .map(|_| crossbeam_channel::unbounded())
        .unzip();
    // Mutexes around a vector of optional (take-able) pairs of (tx, rx) for worker/client communication.
    let storage_channels: Mutex<Vec<_>> = Mutex::new(
        storage_response_txs
            .into_iter()
            .zip(storage_command_rxs)
            .map(Some)
            .collect(),
    );
    let compute_channels: Mutex<Vec<_>> = Mutex::new(
        compute_response_txs
            .into_iter()
            .zip(compute_command_rxs)
            .map(Some)
            .collect(),
    );

    let tokio_executor = tokio::runtime::Handle::current();
    let now = config.now;
    let aws_external_id = config.aws_external_id.clone();

    let worker_guards = timely::execute::execute(config.timely_config, move |timely_worker| {
        let timely_worker_index = timely_worker.index();
        let timely_worker_peers = timely_worker.peers();
        let (storage_boundary, compute_boundary) = create_boundary(timely_worker_index);
        let _tokio_guard = tokio_executor.enter();
        let (storage_response_tx, storage_command_rx) = storage_channels.lock().unwrap()
            [timely_worker_index % config.workers]
            .take()
            .unwrap();
        let (compute_response_tx, compute_command_rx) = compute_channels.lock().unwrap()
            [timely_worker_index % config.workers]
            .take()
            .unwrap();
        let (source_metrics, _sink_metrics, unspecified_metrics, _trace_metrics) =
            metrics_bundle.clone();
        Worker {
            timely_worker,
            compute_state: None,
            storage_state: StorageState {
                table_state: HashMap::new(),
                source_descriptions: HashMap::new(),
                source_uppers: HashMap::new(),
                ts_source_mapping: HashMap::new(),
                ts_histories: HashMap::default(),
                persisted_sources: PersistedSourceManager::new(),
                unspecified_metrics,
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
            storage_command_rx,
            storage_response_tx,
            compute_boundary,
            compute_command_rx,
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
        storage_command_txs,
        worker_threads.clone(),
    );
    let compute_client =
        LocalClient::new(compute_response_rxs, compute_command_txs, worker_threads);
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
    /// The state associated with rendering dataflows.
    compute_state: Option<ComputeState>,
    /// The state associated with collection ingress and egress.
    storage_state: StorageState,
    /// The boundary between storage and compute layers, storage side.
    storage_boundary: SC,
    /// The channel from which storage commands are drawn.
    storage_command_rx: crossbeam_channel::Receiver<StorageCommand>,
    /// The channel over which storage responses are reported.
    storage_response_tx: mpsc::UnboundedSender<StorageResponse>,
    /// The boundary between storage and compute layers, compute side.
    compute_boundary: CR,
    /// The channel from which compute commands are drawn.
    compute_command_rx: crossbeam_channel::Receiver<ComputeCommand>,
    /// The channel over which compute responses are reported.
    compute_response_tx: mpsc::UnboundedSender<ComputeResponse>,
    /// Metrics bundle.
    metrics_bundle: (SourceBaseMetrics, SinkBaseMetrics, Metrics, TraceMetrics),
}

impl<'w, A, SC, CR> Worker<'w, A, SC, CR>
where
    A: Allocate + 'w,
    SC: StorageCapture,
    CR: ComputeReplay,
{
    /// Draws from `dataflow_command_receiver` until shutdown.
    fn run(&mut self) {
        let mut storage_shutdown = false;
        let mut compute_shutdown = false;
        while !storage_shutdown || !compute_shutdown {
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

            // Handle any received storage commands.
            let mut storage_cmds = vec![];
            storage_shutdown = drain_channel(&self.storage_command_rx, &mut storage_cmds);
            for cmd in storage_cmds {
                self.activate_storage().handle_storage_command(cmd);
            }

            // Handle any received compute commands.
            let mut compute_cmds = vec![];
            compute_shutdown = drain_channel(&self.compute_command_rx, &mut compute_cmds);
            for cmd in compute_cmds {
                let mut should_drop = false;
                match &cmd {
                    ComputeCommand::CreateInstance(_logging) => {
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
                    ComputeCommand::DropInstance => should_drop = true,
                    _ => (),
                }

                self.activate_compute().unwrap().handle_compute_command(cmd);

                if should_drop {
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
}

pub struct LocalInput {
    pub handle: UnorderedHandle<Timestamp, (Row, Timestamp, Diff)>,
    /// A weak reference to the capability, in case all uses are dropped.
    pub capability: std::rc::Weak<RefCell<ActivateCapability<Timestamp>>>,
}

/// An in-progress peek, and data to eventually fulfill it.
///
/// Note that `PendingPeek` intentionally does not implement or derive `Clone`,
/// as each `PendingPeek` is meant to be dropped after it's responded to.
pub(crate) struct PendingPeek {
    peek: Peek,
    /// The data from which the trace derives.
    trace_bundle: TraceBundle,
}

impl PendingPeek {
    /// Produces a corresponding log event.
    pub fn as_log_event(&self) -> crate::logging::materialized::Peek {
        crate::logging::materialized::Peek::new(self.peek.id, self.peek.timestamp, self.peek.uuid)
    }

    /// Attempts to fulfill the peek and reports success.
    ///
    /// To produce output at `peek.timestamp`, we must be certain that
    /// it is no longer changing. A trace guarantees that all future
    /// changes will be greater than or equal to an element of `upper`.
    ///
    /// If an element of `upper` is less or equal to `peek.timestamp`,
    /// then there can be further updates that would change the output.
    /// If no element of `upper` is less or equal to `peek.timestamp`,
    /// then for any time `t` less or equal to `peek.timestamp` it is
    /// not the case that `upper` is less or equal to that timestamp,
    /// and so the result cannot further evolve.
    fn seek_fulfillment(&mut self, upper: &mut Antichain<Timestamp>) -> Option<PeekResponse> {
        self.trace_bundle.oks_mut().read_upper(upper);
        if upper.less_equal(&self.peek.timestamp) {
            return None;
        }
        self.trace_bundle.errs_mut().read_upper(upper);
        if upper.less_equal(&self.peek.timestamp) {
            return None;
        }
        let response = match self.collect_finished_data() {
            Ok(rows) => PeekResponse::Rows(rows),
            Err(text) => PeekResponse::Error(text),
        };
        Some(response)
    }

    /// Collects data for a known-complete peek.
    fn collect_finished_data(&mut self) -> Result<Vec<(Row, NonZeroUsize)>, String> {
        // Check if there exist any errors and, if so, return whatever one we
        // find first.
        let (mut cursor, storage) = self.trace_bundle.errs_mut().cursor();
        while cursor.key_valid(&storage) {
            let mut copies = 0;
            cursor.map_times(&storage, |time, diff| {
                if time.less_equal(&self.peek.timestamp) {
                    copies += diff;
                }
            });
            if copies < 0 {
                return Err(format!(
                    "Invalid data in source errors, saw retractions ({}) for row that does not exist: {}",
                    copies * -1,
                    cursor.key(&storage),
                ));
            }
            if copies > 0 {
                return Err(cursor.key(&storage).to_string());
            }
            cursor.step_key(&storage);
        }

        // Cursor and bound lifetime for `Row` data in the backing trace.
        let (mut cursor, storage) = self.trace_bundle.oks_mut().cursor();
        // Accumulated `Vec<(row, count)>` results that we are likely to return.
        let mut results = Vec::new();

        // When set, a bound on the number of records we need to return.
        // The requirements on the records are driven by the finishing's
        // `order_by` field. Further limiting will happen when the results
        // are collected, so we don't need to have exactly this many results,
        // just at least those results that would have been returned.
        let max_results = self
            .peek
            .finishing
            .limit
            .map(|l| l + self.peek.finishing.offset);

        if let Some(literal) = &self.peek.key {
            cursor.seek_key(&storage, literal);
        }

        let mut row_builder = Row::default();
        let mut datum_vec = DatumVec::new();
        let mut l_datum_vec = DatumVec::new();
        let mut r_datum_vec = DatumVec::new();

        while cursor.key_valid(&storage) {
            while cursor.val_valid(&storage) {
                // TODO: This arena could be maintained and reuse for longer
                // but it wasn't clear at what granularity we should flush
                // it to ensure we don't accidentally spike our memory use.
                // This choice is conservative, and not the end of the world
                // from a performance perspective.
                let arena = RowArena::new();
                let key = cursor.key(&storage);
                let row = cursor.val(&storage);
                // TODO: We could unpack into a re-used allocation, except
                // for the arena above (the allocation would not be allowed
                // to outlive the arena above, from which it might borrow).
                let mut borrow = datum_vec.borrow_with_many(&[key, row]);
                if let Some(result) = self
                    .peek
                    .map_filter_project
                    .evaluate_into(&mut borrow, &arena, &mut row_builder)
                    .map_err_to_string()?
                {
                    let mut copies = 0;
                    cursor.map_times(&storage, |time, diff| {
                        if time.less_equal(&self.peek.timestamp) {
                            copies += diff;
                        }
                    });
                    let copies: usize = if copies < 0 {
                        return Err(format!(
                            "Invalid data in source, saw retractions ({}) for row that does not exist: {:?}",
                            copies * -1,
                            &*borrow,
                        ));
                    } else {
                        copies.try_into().unwrap()
                    };
                    // if copies > 0 ... otherwise skip
                    if let Some(copies) = NonZeroUsize::new(copies) {
                        results.push((result, copies));
                    }

                    // If we hold many more than `max_results` records, we can thin down
                    // `results` using `self.finishing.ordering`.
                    if let Some(max_results) = max_results {
                        // We use a threshold twice what we intend, to amortize the work
                        // across all of the insertions. We could tighten this, but it
                        // works for the moment.
                        if results.len() >= 2 * max_results {
                            if self.peek.finishing.order_by.is_empty() {
                                results.truncate(max_results);
                                return Ok(results);
                            } else {
                                // We can sort `results` and then truncate to `max_results`.
                                // This has an effect similar to a priority queue, without
                                // its interactive dequeueing properties.
                                // TODO: Had we left these as `Vec<Datum>` we would avoid
                                // the unpacking; we should consider doing that, although
                                // it will require a re-pivot of the code to branch on this
                                // inner test (as we prefer not to maintain `Vec<Datum>`
                                // in the other case).
                                results.sort_by(|left, right| {
                                    let left_datums = l_datum_vec.borrow_with(&left.0);
                                    let right_datums = r_datum_vec.borrow_with(&right.0);
                                    mz_expr::compare_columns(
                                        &self.peek.finishing.order_by,
                                        &left_datums,
                                        &right_datums,
                                        || left.0.cmp(&right.0),
                                    )
                                });
                                results.truncate(max_results);
                            }
                        }
                    }
                }
                cursor.step_val(&storage);
            }
            // If we had a key, we are now done and can return.
            if self.peek.key.is_some() {
                return Ok(results);
            } else {
                cursor.step_key(&storage);
            }
        }

        Ok(results)
    }
}

fn drain_channel<T>(rx: &crossbeam_channel::Receiver<T>, out: &mut Vec<T>) -> bool {
    loop {
        match rx.try_recv() {
            Ok(msg) => out.push(msg),
            Err(TryRecvError::Empty) => break false,
            Err(TryRecvError::Disconnected) => break true,
        }
    }
}
