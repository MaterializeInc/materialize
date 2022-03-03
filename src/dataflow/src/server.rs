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
use std::collections::{BTreeMap, HashMap};
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

use mz_dataflow_types::client::ComputeInstanceId;
use mz_dataflow_types::client::{Command, ComputeCommand, LocalClient, Response};
use mz_dataflow_types::sources::AwsExternalId;
use mz_dataflow_types::PeekResponse;
use mz_expr::{GlobalId, RowSetFinishing};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::result::ResultExt;
use mz_repr::{DatumVec, Diff, Row, RowArena, Timestamp};

use self::metrics::{ServerMetrics, WorkerMetrics};
use crate::arrangement::manager::{TraceBundle, TraceManager, TraceMetrics};
use crate::event::ActivatedEventPusher;
use crate::metrics::Metrics;
use crate::render::sources::PersistedSourceManager;
use crate::sink::SinkBaseMetrics;
use crate::source::metrics::SourceBaseMetrics;

pub mod boundary;
mod compute_state;
mod metrics;
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
pub fn serve(config: Config) -> Result<(Server, LocalClient), anyhow::Error> {
    serve_boundary(config, |_| {
        let boundary = Rc::new(RefCell::new(EventLinkBoundary::new()));
        (Rc::clone(&boundary), boundary)
    })
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
) -> Result<(Server, LocalClient), anyhow::Error> {
    assert!(config.workers > 0);

    // Various metrics related things.
    let server_metrics = ServerMetrics::register_with(&config.metrics_registry);
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
    let (response_txs, response_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
        .map(|_| mpsc::unbounded_channel())
        .unzip();
    let (command_txs, command_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
        .map(|_| crossbeam_channel::unbounded())
        .unzip();
    // A mutex around a vector of optional (take-able) pairs of (tx, rx) for worker/client communication.
    let channels: Mutex<Vec<_>> = Mutex::new(
        response_txs
            .into_iter()
            .zip(command_rxs)
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
        let (response_tx, command_rx) = channels.lock().unwrap()
            [timely_worker_index % config.workers]
            .take()
            .unwrap();
        let worker_idx = timely_worker.index();
        let (source_metrics, _sink_metrics, unspecified_metrics, _trace_metrics) =
            metrics_bundle.clone();
        Worker {
            timely_worker,
            compute_state: BTreeMap::default(),
            storage_state: StorageState {
                local_inputs: HashMap::new(),
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
            compute_boundary,
            command_rx,
            response_tx,
            metrics_bundle: (
                server_metrics.for_worker_id(worker_idx),
                metrics_bundle.clone(),
            ),
        }
        .run()
    })
    .map_err(|e| anyhow!("{}", e))?;
    let client = LocalClient::new(
        response_rxs,
        command_txs,
        worker_guards
            .guards()
            .iter()
            .map(|g| g.thread().clone())
            .collect(),
    );
    let server = Server {
        _worker_guards: worker_guards,
    };
    Ok((server, client))
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
    compute_state: BTreeMap<ComputeInstanceId, ComputeState>,
    /// The state associated with collection ingress and egress.
    storage_state: StorageState,
    /// The boundary between storage and compute layers, storage side.
    storage_boundary: SC,
    /// The boundary between storage and compute layers, compute side.
    compute_boundary: CR,
    /// The channel from which commands are drawn.
    command_rx: crossbeam_channel::Receiver<Command>,
    /// The channel over which frontier information is reported.
    response_tx: mpsc::UnboundedSender<Response>,
    /// Metrics bundle.
    metrics_bundle: (
        WorkerMetrics,
        (SourceBaseMetrics, SinkBaseMetrics, Metrics, TraceMetrics),
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
        let mut compute_instances = Vec::new();

        let mut shutdown = false;
        while !shutdown {
            // Enable trace compaction.
            for instance in compute_instances.iter() {
                self.compute_state
                    .get_mut(&instance)
                    .unwrap()
                    .traces
                    .maintenance();
            }

            // Ask Timely to execute a unit of work. If Timely decides there's
            // nothing to do, it will park the thread. We rely on another thread
            // unparking us when there's new work to be done, e.g., when sending
            // a command or when new Kafka messages have arrived.
            self.timely_worker.step_or_park(None);

            // Report frontier information back the coordinator.
            for instance_id in compute_instances.iter() {
                self.activate_compute(*instance_id)
                    .report_compute_frontiers();
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
            self.metrics_bundle.0.observe_command_queue(&cmds);
            for cmd in cmds {
                self.metrics_bundle.0.observe_command(&cmd);

                if let Command::Compute(ComputeCommand::CreateInstance(_logging), instance_id) =
                    &cmd
                {
                    let compute_instance = ComputeState {
                        traces: TraceManager::new(
                            (self.metrics_bundle.1).3.clone(),
                            self.timely_worker.index(),
                        ),
                        dataflow_tokens: HashMap::new(),
                        tail_response_buffer: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
                        sink_write_frontiers: HashMap::new(),
                        pending_peeks: Vec::new(),
                        reported_frontiers: HashMap::new(),
                        sink_metrics: (self.metrics_bundle.1).1.clone(),
                        materialized_logger: None,
                    };
                    self.compute_state.insert(*instance_id, compute_instance);
                    compute_instances.push(*instance_id);
                }

                self.handle_command(cmd);
            }

            self.metrics_bundle.0.observe_command_finish();
            for instance_id in compute_instances.iter() {
                self.metrics_bundle
                    .0
                    .observe_pending_peeks(&self.compute_state[instance_id].pending_peeks);
                self.activate_compute(*instance_id).process_peeks();
                self.activate_compute(*instance_id).process_tails();
            }
        }
        for instance_id in compute_instances.iter() {
            self.compute_state
                .get_mut(instance_id)
                .unwrap()
                .traces
                .del_all_traces();
            self.activate_compute(*instance_id).shutdown_logging();
        }
    }

    fn activate_compute(&mut self, instance_id: ComputeInstanceId) -> ActiveComputeState<A, CR> {
        ActiveComputeState {
            timely_worker: &mut *self.timely_worker,
            compute_state: self.compute_state.get_mut(&instance_id).unwrap(),
            instance_id,
            response_tx: &mut self.response_tx,
            boundary: &mut self.compute_boundary,
        }
    }
    fn activate_storage(&mut self) -> ActiveStorageState<A, SC> {
        ActiveStorageState {
            timely_worker: &mut *self.timely_worker,
            storage_state: &mut self.storage_state,
            response_tx: &mut self.response_tx,
            boundary: &mut self.storage_boundary,
        }
    }

    fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::Compute(cmd, instance) => {
                self.activate_compute(instance).handle_compute_command(cmd)
            }
            Command::Storage(cmd) => self.activate_storage().handle_storage_command(cmd),
        }
    }
}

pub struct LocalInput {
    pub handle: UnorderedHandle<Timestamp, (Row, Timestamp, Diff)>,
    pub capability: ActivateCapability<Timestamp>,
}

/// An in-progress peek, and data to eventually fulfill it.
///
/// Note that `PendingPeek` intentionally does not implement or derive `Clone`,
/// as each `PendingPeek` is meant to be dropped after it's responded to.
pub(crate) struct PendingPeek {
    /// The identifier of the dataflow to peek.
    id: GlobalId,
    /// An optional key to use for the arrangement.
    key: Option<Row>,
    /// The ID of the connection that submitted the peek. For logging only.
    conn_id: u32,
    /// Time at which the collection should be materialized.
    timestamp: Timestamp,
    /// Finishing operations to perform on the peek, like an ordering and a
    /// limit.
    finishing: RowSetFinishing,
    /// Linear operators to apply in-line to all results.
    map_filter_project: mz_expr::SafeMfpPlan,
    /// The data from which the trace derives.
    trace_bundle: TraceBundle,
}

impl PendingPeek {
    /// Produces a corresponding log event.
    pub fn as_log_event(&self) -> crate::logging::materialized::Peek {
        crate::logging::materialized::Peek::new(self.id, self.timestamp, self.conn_id)
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
        if upper.less_equal(&self.timestamp) {
            return None;
        }
        self.trace_bundle.errs_mut().read_upper(upper);
        if upper.less_equal(&self.timestamp) {
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
                if time.less_equal(&self.timestamp) {
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
        let max_results = self.finishing.limit.map(|l| l + self.finishing.offset);

        if let Some(literal) = &self.key {
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
                    .map_filter_project
                    .evaluate_into(&mut borrow, &arena, &mut row_builder)
                    .map_err_to_string()?
                {
                    let mut copies = 0;
                    cursor.map_times(&storage, |time, diff| {
                        if time.less_equal(&self.timestamp) {
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
                            if self.finishing.order_by.is_empty() {
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
                                        &self.finishing.order_by,
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
            if self.key.is_some() {
                return Ok(results);
            } else {
                cursor.step_key(&storage);
            }
        }

        Ok(results)
    }
}
