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
use std::sync::Mutex;
use std::time::{Duration, Instant};
use timely::progress::reachability::logging::TrackerEvent;

use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::Collection;
use enum_iterator::IntoEnumIterator;
use enum_kinds::EnumKind;
use num_enum::IntoPrimitive;
use ore::metrics::MetricsRegistry;
use serde::{Deserialize, Serialize};
use timely::communication::initialize::WorkerGuards;
use timely::communication::Allocate;
use timely::dataflow::operators::unordered_input::UnorderedHandle;
use timely::dataflow::operators::ActivateCapability;
use timely::logging::Logger;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::progress::ChangeBatch;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc;

use dataflow_types::logging::LoggingConfig;
use dataflow_types::{
    Consistency, DataflowDescription, DataflowError, ExternalSourceConnector, MzOffset,
    PeekResponse, SourceConnector, TailResponse, TimestampSourceUpdate, Update,
};
use expr::{GlobalId, PartitionId, RowSetFinishing};
use ore::{now::NowFn, result::ResultExt};
use persist::indexed::runtime::RuntimeClient;
use repr::{Diff, Row, RowArena, Timestamp};

use crate::arrangement::manager::{TraceBundle, TraceManager, TraceMetrics};
use crate::logging;
use crate::logging::materialized::MaterializedEvent;
use crate::metrics::Metrics;
use crate::operator::CollectionExt;
use crate::render::{self, plan::Plan as RenderPlan, RenderState};
use crate::sink::SinkBaseMetrics;
use crate::source::metrics::SourceBaseMetrics;
use crate::source::timestamp::TimestampBindingRc;

use self::metrics::{ServerMetrics, WorkerMetrics};
use crate::activator::RcActivator;

mod metrics;

/// How frequently each dataflow worker sends timestamp binding updates
/// back to the coordinator.
static TS_BINDING_FEEDBACK_INTERVAL_MS: u128 = 1_000;

/// Explicit instructions for timely dataflow workers.
#[derive(Clone, Debug, Serialize, Deserialize, EnumKind)]
#[enum_kind(
    CommandKind,
    derive(Serialize, IntoPrimitive, IntoEnumIterator),
    repr(usize),
    serde(rename_all = "snake_case"),
    doc = "The kind of command that was received"
)]
pub enum Command {
    /// Create a sequence of dataflows.
    ///
    /// Each of the dataflows must contain `as_of` members that are valid
    /// for each of the referenced arrangements, meaning `AllowCompaction`
    /// should be held back to those values until the command.
    /// Subsequent commands may arbitrarily compact the arrangements;
    /// the dataflow runners are responsible for ensuring that they can
    /// correctly maintain the dataflows.
    CreateDataflows(Vec<DataflowDescription<RenderPlan>>),
    /// Drop the sources bound to these names.
    DropSources(Vec<GlobalId>),
    /// Drop the sinks bound to these names.
    DropSinks(Vec<GlobalId>),
    /// Drop the indexes bound to these namees.
    DropIndexes(Vec<GlobalId>),
    /// Peek at an arrangement.
    ///
    /// This request elicits data from the worker, by naming an
    /// arrangement and some actions to apply to the results before
    /// returning them.
    ///
    /// The `timestamp` member must be valid for the arrangement that
    /// is referenced by `id`. This means that `AllowCompaction` for
    /// this arrangement should not pass `timestamp` before this command.
    /// Subsequent commands may arbitrarily compact the arrangements;
    /// the dataflow runners are responsible for ensuring that they can
    /// correctly answer the `Peek`.
    Peek {
        /// The identifier of the arrangement.
        id: GlobalId,
        /// An optional key that should be used for the arrangement.
        key: Option<Row>,
        /// The identifier of this peek request.
        ///
        /// Used in responses and cancelation requests.
        conn_id: u32,
        /// The logical timestamp at which the arrangement is queried.
        timestamp: Timestamp,
        /// Actions to apply to the result set before returning them.
        finishing: RowSetFinishing,
        /// Linear operation to apply in-line on each result.
        map_filter_project: expr::SafeMfpPlan,
    },
    /// Cancel the peek associated with the given `conn_id`.
    CancelPeek {
        /// The identifier of the peek request to cancel.
        conn_id: u32,
    },
    /// Insert `updates` into the local input named `id`.
    Insert {
        /// Identifier of the local input.
        id: GlobalId,
        /// A list of updates to be introduced to the input.
        updates: Vec<Update>,
    },
    /// Enable compaction in views.
    ///
    /// Each entry in the vector names a view and provides a frontier after which
    /// accumulations must be correct. The workers gain the liberty of compacting
    /// the corresponding maintained traces up through that frontier.
    AllowCompaction(Vec<(GlobalId, Antichain<Timestamp>)>),
    /// Update durability information for sources.
    ///
    /// Each entry names a source and provides a frontier before which the source can
    /// be exactly replayed across restarts (i.e. we can assign the same timestamps to
    /// all the same data)
    DurabilityFrontierUpdates(Vec<(GlobalId, Antichain<Timestamp>)>),
    /// Add a new source to be aware of for timestamping.
    AddSourceTimestamping {
        /// The ID of the timestamped source
        id: GlobalId,
        /// The connector for the timestamped source.
        connector: SourceConnector,
        /// Previously stored timestamp bindings.
        bindings: Vec<(PartitionId, Timestamp, MzOffset)>,
    },
    /// Advance worker timestamp
    AdvanceSourceTimestamp {
        /// The ID of the timestamped source
        id: GlobalId,
        /// The associated update (RT or BYO)
        update: TimestampSourceUpdate,
    },
    /// Drop all timestamping info for a source
    DropSourceTimestamping {
        /// The ID id of the formerly timestamped source.
        id: GlobalId,
    },
    /// Advance all local inputs to the given timestamp.
    AdvanceAllLocalInputs {
        /// The timestamp to advance to.
        advance_to: Timestamp,
    },
    /// Request that the logging sources in the contained configuration are
    /// installed.
    EnableLogging(LoggingConfig),
    /// Disconnect inputs, drain dataflows, and shut down timely workers.
    Shutdown,
}

impl CommandKind {
    /// Returns the name of the command kind.
    fn name(self) -> &'static str {
        match self {
            CommandKind::AddSourceTimestamping => "add_source_timestamping",
            CommandKind::AdvanceAllLocalInputs => "advance_all_local_inputs",
            CommandKind::AdvanceSourceTimestamp => "advance_source_timestamp",
            CommandKind::AllowCompaction => "allow_compaction",
            CommandKind::CancelPeek => "cancel_peek",
            CommandKind::CreateDataflows => "create_dataflows",
            CommandKind::DropIndexes => "drop_indexes",
            CommandKind::DropSinks => "drop_sinks",
            CommandKind::DropSourceTimestamping => "drop_source_timestamping",
            CommandKind::DropSources => "drop_sources",
            CommandKind::DurabilityFrontierUpdates => "durability_frontier_updates",
            CommandKind::EnableLogging => "enable_logging",
            CommandKind::Insert => "insert",
            CommandKind::Peek => "peek",
            CommandKind::Shutdown => "shutdown",
        }
    }
}

/// Information from timely dataflow workers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Response {
    /// Identifies the worker by its identifier.
    pub worker_id: usize,
    /// The feedback itself.
    pub message: WorkerFeedback,
}

/// Data about timestamp bindings that dataflow workers send to the coordinator
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimestampBindingFeedback {
    /// Durability frontier changes
    pub changes: Vec<(GlobalId, ChangeBatch<Timestamp>)>,
    /// Timestamp bindings for all of those frontier changes
    pub bindings: Vec<(GlobalId, PartitionId, Timestamp, MzOffset)>,
}

/// Responses the worker can provide back to the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WorkerFeedback {
    /// A list of identifiers of traces, with prior and new upper frontiers.
    FrontierUppers(Vec<(GlobalId, ChangeBatch<Timestamp>)>),
    /// Timestamp bindings and prior and new frontiers for those bindings for all
    /// sources
    TimestampBindings(TimestampBindingFeedback),
    /// The worker's response to a specified (by connection id) peek.
    PeekResponse(u32, PeekResponse),
    /// The worker's next response to a specified tail.
    TailResponse(GlobalId, TailResponse),
}

/// Configures a dataflow server.
pub struct Config {
    /// Command stream receivers for each desired workers.
    ///
    /// The length of this vector determines the number of worker threads that
    /// will be spawned.
    pub command_receivers: Vec<crossbeam_channel::Receiver<Command>>,
    /// The Timely worker configuration.
    pub timely_worker: timely::WorkerConfig,
    /// Whether the server is running in experimental mode.
    pub experimental_mode: bool,
    /// Function to get wall time now.
    pub now: NowFn,
    /// Metrics registry through which dataflow metrics will be reported.
    pub metrics_registry: MetricsRegistry,
    /// Handle to the persistence runtime. None if disabled.
    pub persist: Option<RuntimeClient>,
    /// Responses to commands should be sent into this channel.
    pub feedback_tx: mpsc::UnboundedSender<Response>,
}

/// Initiates a timely dataflow computation, processing materialized commands.
pub fn serve(config: Config) -> Result<WorkerGuards<()>, String> {
    let server_metrics = ServerMetrics::register_with(&config.metrics_registry);
    let dataflow_source_metrics = SourceBaseMetrics::register_with(&config.metrics_registry);
    let dataflow_sink_metrics = SinkBaseMetrics::register_with(&config.metrics_registry);
    let workers = config.command_receivers.len();
    assert!(workers > 0);

    // Construct endpoints for each thread that will receive the coordinator's
    // sequenced command stream.
    //
    // TODO(benesch): package up this idiom of handing out ownership of N items
    // to the N timely threads that will be spawned. The Mutex<Vec<Option<T>>>
    // is hard to read through.
    let command_rxs: Mutex<Vec<_>> =
        Mutex::new(config.command_receivers.into_iter().map(Some).collect());

    let tokio_executor = tokio::runtime::Handle::current();
    let now = config.now;
    let metrics = Metrics::register_with(&config.metrics_registry);
    let trace_metrics = TraceMetrics::register_with(&config.metrics_registry);
    let persist = config.persist;
    let feedback_tx = config.feedback_tx.clone();
    timely::execute::execute(
        timely::Config {
            communication: timely::CommunicationConfig::Process(workers),
            worker: config.timely_worker,
        },
        move |timely_worker| {
            let _tokio_guard = tokio_executor.enter();
            let command_rx = command_rxs.lock().unwrap()[timely_worker.index() % workers]
                .take()
                .unwrap();
            let worker_idx = timely_worker.index();
            let metrics = metrics.clone();
            let trace_metrics = trace_metrics.clone();
            let dataflow_source_metrics = dataflow_source_metrics.clone();
            let dataflow_sink_metrics = dataflow_sink_metrics.clone();
            Worker {
                timely_worker,
                render_state: RenderState {
                    traces: TraceManager::new(trace_metrics, worker_idx),
                    local_inputs: HashMap::new(),
                    ts_source_mapping: HashMap::new(),
                    ts_histories: HashMap::default(),
                    dataflow_tokens: HashMap::new(),
                    sink_write_frontiers: HashMap::new(),
                    metrics,
                    persist: persist.clone(),
                    tail_response_buffer: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
                },
                materialized_logger: None,
                command_rx,
                pending_peeks: Vec::new(),
                feedback_tx: feedback_tx.clone(),
                reported_frontiers: HashMap::new(),
                reported_bindings_frontiers: HashMap::new(),
                last_bindings_feedback: Instant::now(),
                metrics: server_metrics.for_worker_id(worker_idx),
                now,
                dataflow_source_metrics,
                dataflow_sink_metrics,
            }
            .run()
        },
    )
}

/// State maintained for each worker thread.
///
/// Much of this state can be viewed as local variables for the worker thread,
/// holding state that persists across function calls.
struct Worker<'w, A>
where
    A: Allocate,
{
    /// The underlying Timely worker.
    timely_worker: &'w mut TimelyWorker<A>,
    /// The state associated with rendering dataflows.
    render_state: RenderState,
    /// The logger, from Timely's logging framework, if logs are enabled.
    materialized_logger: Option<logging::materialized::Logger>,
    /// The channel from which commands are drawn.
    command_rx: crossbeam_channel::Receiver<Command>,
    /// Peek commands that are awaiting fulfillment.
    pending_peeks: Vec<PendingPeek>,
    /// The channel over which frontier information is reported.
    feedback_tx: mpsc::UnboundedSender<Response>,
    /// Tracks the frontier information that has been sent over `feedback_tx`.
    reported_frontiers: HashMap<GlobalId, Antichain<Timestamp>>,
    /// Tracks the timestamp binding durability information that has been sent over `feedback_tx`.
    reported_bindings_frontiers: HashMap<GlobalId, Antichain<Timestamp>>,
    /// Tracks the last time we sent binding durability info over `feedback_tx`.
    last_bindings_feedback: Instant,
    /// Metrics bundle.
    metrics: WorkerMetrics,
    now: NowFn,
    /// Metrics for the source-specific side of dataflows.
    dataflow_source_metrics: SourceBaseMetrics,
    dataflow_sink_metrics: SinkBaseMetrics,
}

impl<'w, A> Worker<'w, A>
where
    A: Allocate + 'w,
{
    /// Initializes timely dataflow logging and publishes as a view.
    fn initialize_logging(&mut self, logging: &LoggingConfig) {
        if self.materialized_logger.is_some() {
            panic!("dataflow server has already initialized logging");
        }

        use crate::logging::BatchLogger;
        use timely::dataflow::operators::capture::event::link::EventLink;

        let granularity_ms = std::cmp::max(1, logging.granularity_ns / 1_000_000) as Timestamp;

        // Track time relative to the Unix epoch, rather than when the server
        // started, so that the logging sources can be joined with tables and
        // other real time sources for semi-sensible results.
        let now = Instant::now();
        let unix = Duration::from_millis((self.now)());

        // Establish loggers first, so we can either log the logging or not, as we like.
        let t_linked = std::rc::Rc::new(EventLink::new());
        let mut t_logger = BatchLogger::new(t_linked.clone(), granularity_ms);
        let r_linked = std::rc::Rc::new(EventLink::new());
        let mut r_logger = BatchLogger::new(r_linked.clone(), granularity_ms);
        let d_linked = std::rc::Rc::new(EventLink::new());
        let mut d_logger = BatchLogger::new(d_linked.clone(), granularity_ms);
        let m_linked = std::rc::Rc::new(EventLink::new());
        let mut m_logger = BatchLogger::new(m_linked.clone(), granularity_ms);

        let mut t_traces = HashMap::new();
        let mut r_traces = HashMap::new();
        let mut d_traces = HashMap::new();
        let mut m_traces = HashMap::new();

        let activate_after = 128;

        let t_activator = RcActivator::new("t_activator".into(), activate_after);
        let r_activator = RcActivator::new("r_activator".into(), activate_after);
        let d_activator = RcActivator::new("d_activator".into(), activate_after);
        let m_activator = RcActivator::new("m_activator".into(), activate_after);

        if !logging.log_logging {
            // Construct logging dataflows and endpoints before registering any.
            t_traces.extend(logging::timely::construct(
                &mut self.timely_worker,
                logging,
                t_linked.clone(),
                t_activator.clone(),
            ));
            r_traces.extend(logging::reachability::construct(
                &mut self.timely_worker,
                logging,
                r_linked.clone(),
                r_activator.clone(),
            ));
            d_traces.extend(logging::differential::construct(
                &mut self.timely_worker,
                logging,
                d_linked.clone(),
                d_activator.clone(),
            ));
            m_traces.extend(logging::materialized::construct(
                &mut self.timely_worker,
                logging,
                m_linked.clone(),
                m_activator.clone(),
            ));
        }

        // Register each logger endpoint.
        let mut activator = t_activator.clone();
        self.timely_worker.log_register().insert_logger(
            "timely",
            Logger::new(now, unix, self.timely_worker.index(), move |time, data| {
                t_logger.publish_batch(time, data);
                activator.activate();
            }),
        );

        let mut activator = r_activator.clone();
        self.timely_worker.log_register().insert_logger(
            "timely/reachability",
            Logger::new(
                now,
                unix,
                self.timely_worker.index(),
                move |time, data: &mut Vec<(Duration, usize, TrackerEvent)>| {
                    let mut converted_updates = Vec::new();
                    for event in data.drain(..) {
                        match event.2 {
                            TrackerEvent::SourceUpdate(update) => {
                                let massaged: Vec<_> = update
                                    .updates
                                    .iter()
                                    .map(|u| {
                                        let ts = u.2.as_any().downcast_ref::<Timestamp>().copied();
                                        (*u.0, *u.1, true, ts, *u.3 as isize)
                                    })
                                    .collect();

                                converted_updates.push((
                                    event.0,
                                    event.1,
                                    (update.tracker_id, massaged),
                                ));
                            }
                            TrackerEvent::TargetUpdate(update) => {
                                let massaged: Vec<_> = update
                                    .updates
                                    .iter()
                                    .map(|u| {
                                        let ts = u.2.as_any().downcast_ref::<Timestamp>().copied();
                                        (*u.0, *u.1, true, ts, *u.3 as isize)
                                    })
                                    .collect();

                                converted_updates.push((
                                    event.0,
                                    event.1,
                                    (update.tracker_id, massaged),
                                ));
                            }
                        }
                    }
                    r_logger.publish_batch(time, &mut converted_updates);
                    activator.activate();
                },
            ),
        );

        let mut activator = d_activator.clone();
        self.timely_worker.log_register().insert_logger(
            "differential/arrange",
            Logger::new(now, unix, self.timely_worker.index(), move |time, data| {
                d_logger.publish_batch(time, data);
                activator.activate();
            }),
        );

        let mut activator = m_activator.clone();
        self.timely_worker.log_register().insert_logger(
            "materialized",
            Logger::new(now, unix, self.timely_worker.index(), move |time, data| {
                m_logger.publish_batch(time, data);
                activator.activate();
            }),
        );

        let errs = self
            .timely_worker
            .dataflow_named("Dataflow: logging", |scope| {
                Collection::<_, DataflowError, isize>::empty(scope)
                    .arrange()
                    .trace
            });

        let logger = self
            .timely_worker
            .log_register()
            .get("materialized")
            .unwrap();

        if logging.log_logging {
            // Create log processing dataflows after registering logging so we can log the
            // logging.
            t_traces.extend(logging::timely::construct(
                &mut self.timely_worker,
                logging,
                t_linked,
                t_activator,
            ));
            r_traces.extend(logging::reachability::construct(
                &mut self.timely_worker,
                logging,
                r_linked,
                r_activator,
            ));
            d_traces.extend(logging::differential::construct(
                &mut self.timely_worker,
                logging,
                d_linked,
                d_activator,
            ));
            m_traces.extend(logging::materialized::construct(
                &mut self.timely_worker,
                logging,
                m_linked,
                m_activator,
            ));
        }

        // Install traces as maintained indexes
        for (log, (_, trace)) in t_traces {
            let id = logging.active_logs[&log];
            self.render_state
                .traces
                .set(id, TraceBundle::new(trace, errs.clone()));
            self.reported_frontiers.insert(id, Antichain::from_elem(0));
            logger.log(MaterializedEvent::Frontier(id, 0, 1));
        }
        for (log, (_, trace)) in r_traces {
            let id = logging.active_logs[&log];
            self.render_state
                .traces
                .set(id, TraceBundle::new(trace, errs.clone()));
            self.reported_frontiers.insert(id, Antichain::from_elem(0));
            logger.log(MaterializedEvent::Frontier(id, 0, 1));
        }
        for (log, (_, trace)) in d_traces {
            let id = logging.active_logs[&log];
            self.render_state
                .traces
                .set(id, TraceBundle::new(trace, errs.clone()));
            self.reported_frontiers.insert(id, Antichain::from_elem(0));
            logger.log(MaterializedEvent::Frontier(id, 0, 1));
        }
        for (log, (_, trace)) in m_traces {
            let id = logging.active_logs[&log];
            self.render_state
                .traces
                .set(id, TraceBundle::new(trace, errs.clone()));
            self.reported_frontiers.insert(id, Antichain::from_elem(0));
            logger.log(MaterializedEvent::Frontier(id, 0, 1));
        }

        self.materialized_logger = Some(logger);
    }

    /// Disables timely dataflow logging.
    ///
    /// This does not unpublish views and is only useful to terminate logging streams to ensure that
    /// materialized can terminate cleanly.
    fn shutdown_logging(&mut self) {
        self.timely_worker.log_register().remove("timely");
        self.timely_worker
            .log_register()
            .remove("timely/reachability");
        self.timely_worker
            .log_register()
            .remove("differential/arrange");
        self.timely_worker.log_register().remove("materialized");
    }

    /// Draws from `dataflow_command_receiver` until shutdown.
    fn run(&mut self) {
        let mut shutdown = false;
        while !shutdown {
            // Enable trace compaction.
            self.render_state.traces.maintenance();

            // Ask Timely to execute a unit of work. If Timely decides there's
            // nothing to do, it will park the thread. We rely on another thread
            // unparking us when there's new work to be done, e.g., when sending
            // a command or when new Kafka messages have arrived.
            self.timely_worker.step_or_park(None);

            // Report frontier information back the coordinator.
            self.report_frontiers();
            self.update_rt_timestamps();
            self.report_timestamp_bindings();

            // Handle any received commands.
            let cmds: Vec<_> = self.command_rx.try_iter().collect();
            self.metrics.observe_command_queue(&cmds);
            for cmd in cmds {
                if let Command::Shutdown = cmd {
                    shutdown = true;
                }
                self.metrics.observe_command(&cmd);
                self.handle_command(cmd);
            }

            self.metrics.observe_pending_peeks(&self.pending_peeks);
            self.metrics.observe_command_finish();
            self.process_peeks();
            self.process_tails();
        }
    }

    /// Send progress information to the coordinator.
    fn report_frontiers(&mut self) {
        fn add_progress(
            id: GlobalId,
            new_frontier: &Antichain<Timestamp>,
            prev_frontier: &Antichain<Timestamp>,
            progress: &mut Vec<(GlobalId, ChangeBatch<Timestamp>)>,
        ) {
            let mut changes = ChangeBatch::new();
            for time in prev_frontier.elements().iter() {
                changes.update(time.clone(), -1);
            }
            for time in new_frontier.elements().iter() {
                changes.update(time.clone(), 1);
            }
            changes.compact();
            if !changes.is_empty() {
                progress.push((id, changes));
            }
        }

        let mut new_frontier = Antichain::new();
        let mut progress = Vec::new();
        for (id, traces) in self.render_state.traces.traces.iter_mut() {
            // Read the upper frontier and compare to what we've reported.
            traces.oks_mut().read_upper(&mut new_frontier);
            let prev_frontier = self
                .reported_frontiers
                .get_mut(&id)
                .expect("Index frontier missing!");
            if prev_frontier != &new_frontier {
                add_progress(*id, &new_frontier, &prev_frontier, &mut progress);
                prev_frontier.clone_from(&new_frontier);
            }
        }

        // Log index frontier changes
        if let Some(logger) = self.materialized_logger.as_mut() {
            for (id, changes) in &mut progress {
                for (time, diff) in changes.iter() {
                    logger.log(MaterializedEvent::Frontier(*id, *time, *diff));
                }
            }
        }

        for (id, history) in self.render_state.ts_histories.iter() {
            // Read the upper frontier and compare to what we've reported.
            history.read_upper(&mut new_frontier);
            let prev_frontier = self
                .reported_frontiers
                .get_mut(&id)
                .expect("Source frontier missing!");
            assert!(<_ as PartialOrder>::less_equal(
                prev_frontier,
                &new_frontier
            ));
            if prev_frontier != &new_frontier {
                add_progress(*id, &new_frontier, &prev_frontier, &mut progress);
                prev_frontier.clone_from(&new_frontier);
            }
        }

        for (id, frontier) in self.render_state.sink_write_frontiers.iter() {
            new_frontier.clone_from(&frontier.borrow());
            let prev_frontier = self
                .reported_frontiers
                .get_mut(&id)
                .expect("Sink frontier missing!");
            assert!(<_ as PartialOrder>::less_equal(
                prev_frontier,
                &new_frontier
            ));
            if prev_frontier != &new_frontier {
                add_progress(*id, &new_frontier, &prev_frontier, &mut progress);
                prev_frontier.clone_from(&new_frontier);
            }
        }

        if !progress.is_empty() {
            self.feedback_tx
                .send(Response {
                    worker_id: self.timely_worker.index(),
                    message: WorkerFeedback::FrontierUppers(progress),
                })
                .expect("feedback receiver should not drop first");
        }
    }

    /// Send information about new timestamp bindings created by dataflow workers back to
    /// the coordinator.
    fn report_timestamp_bindings(&mut self) {
        // Do nothing if dataflow workers can't send feedback or if not enough time has elapsed since
        // the last time we reported timestamp bindings.
        if self.last_bindings_feedback.elapsed().as_millis() < TS_BINDING_FEEDBACK_INTERVAL_MS {
            return;
        }

        let mut changes = Vec::new();
        let mut bindings = Vec::new();
        let mut new_frontier = Antichain::new();

        // Need to go through all sources that are generating timestamp bindings, and extract their upper frontiers.
        // If that frontier is different than the durability frontier we've previously reported then we also need to
        // get the new bindings we've produced and send them to the coordinator.
        for (id, history) in self.render_state.ts_histories.iter() {
            if !history.requires_persistence() {
                continue;
            }

            // Read the upper frontier and compare to what we've reported.
            history.read_upper(&mut new_frontier);
            let prev_frontier = self
                .reported_bindings_frontiers
                .get_mut(&id)
                .expect("Frontier missing!");
            assert!(<_ as PartialOrder>::less_equal(
                prev_frontier,
                &new_frontier
            ));
            if prev_frontier != &new_frontier {
                let mut change_batch = ChangeBatch::new();
                for time in prev_frontier.elements().iter() {
                    change_batch.update(time.clone(), -1);
                }
                for time in new_frontier.elements().iter() {
                    change_batch.update(time.clone(), 1);
                }
                change_batch.compact();
                if !change_batch.is_empty() {
                    changes.push((*id, change_batch));
                }
                // Add all timestamp bindings we know about between the old and new frontier.
                bindings.extend(
                    history
                        .get_bindings_in_range(prev_frontier.borrow(), new_frontier.borrow())
                        .into_iter()
                        .map(|(pid, ts, offset)| (*id, pid, ts, offset)),
                );
                prev_frontier.clone_from(&new_frontier);
            }
        }

        if !changes.is_empty() || !bindings.is_empty() {
            self.feedback_tx
                .send(Response {
                    worker_id: self.timely_worker.index(),
                    message: WorkerFeedback::TimestampBindings(TimestampBindingFeedback {
                        changes,
                        bindings,
                    }),
                })
                .expect("feedback receiver should not drop first");
        }
        self.last_bindings_feedback = Instant::now();
    }
    /// Instruct all real-time sources managed by the worker to close their current
    /// timestamp and move to the next wall clock time.
    ///
    /// Needs to be called periodically (ideally once per "timestamp_frequency" in order
    /// for real time sources to make progress.
    fn update_rt_timestamps(&self) {
        for (_, history) in self.render_state.ts_histories.iter() {
            history.update_timestamp();
        }
    }

    fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::CreateDataflows(dataflows) => {
                for dataflow in dataflows.into_iter() {
                    for (sink_id, _) in dataflow.sink_exports.iter() {
                        self.reported_frontiers
                            .insert(*sink_id, Antichain::from_elem(0));
                    }
                    for (idx_id, idx, _) in dataflow.index_exports.iter() {
                        self.reported_frontiers
                            .insert(*idx_id, Antichain::from_elem(0));
                        if let Some(logger) = self.materialized_logger.as_mut() {
                            logger.log(MaterializedEvent::Dataflow(*idx_id, true));
                            logger.log(MaterializedEvent::Frontier(*idx_id, 0, 1));
                            for import_id in dataflow.get_imports(&idx.on_id) {
                                logger.log(MaterializedEvent::DataflowDependency {
                                    dataflow: *idx_id,
                                    source: import_id,
                                })
                            }
                        }
                    }

                    render::build_dataflow(
                        self.timely_worker,
                        &mut self.render_state,
                        dataflow,
                        self.now,
                        &self.dataflow_source_metrics,
                        &self.dataflow_sink_metrics,
                    );
                }
            }

            Command::DropSources(names) => {
                for name in names {
                    self.render_state.local_inputs.remove(&name);
                }
            }
            Command::DropSinks(ids) => {
                for id in ids {
                    self.reported_frontiers.remove(&id);
                    self.render_state.sink_write_frontiers.remove(&id);
                    self.render_state.dataflow_tokens.remove(&id);
                }
            }
            Command::DropIndexes(ids) => {
                for id in ids {
                    self.render_state.traces.del_trace(&id);
                    let frontier = self
                        .reported_frontiers
                        .remove(&id)
                        .expect("Dropped index with no frontier");
                    if let Some(logger) = self.materialized_logger.as_mut() {
                        logger.log(MaterializedEvent::Dataflow(id, false));
                        for time in frontier.elements().iter() {
                            logger.log(MaterializedEvent::Frontier(id, *time, -1));
                        }
                    }
                }
            }

            Command::Peek {
                id,
                key,
                timestamp,
                conn_id,
                finishing,
                map_filter_project,
            } => {
                // Acquire a copy of the trace suitable for fulfilling the peek.
                let mut trace_bundle = self.render_state.traces.get(&id).unwrap().clone();
                let timestamp_frontier = Antichain::from_elem(timestamp);
                let empty_frontier = Antichain::new();
                trace_bundle
                    .oks_mut()
                    .set_logical_compaction(timestamp_frontier.borrow());
                trace_bundle
                    .errs_mut()
                    .set_logical_compaction(timestamp_frontier.borrow());
                trace_bundle
                    .oks_mut()
                    .set_physical_compaction(empty_frontier.borrow());
                trace_bundle
                    .errs_mut()
                    .set_physical_compaction(empty_frontier.borrow());
                // Prepare a description of the peek work to do.
                let mut peek = PendingPeek {
                    id,
                    key,
                    conn_id,
                    timestamp,
                    finishing,
                    trace_bundle,
                    map_filter_project,
                };
                // Log the receipt of the peek.
                if let Some(logger) = self.materialized_logger.as_mut() {
                    logger.log(MaterializedEvent::Peek(peek.as_log_event(), true));
                }
                // Attempt to fulfill the peek.
                if let Some(response) = peek.seek_fulfillment(&mut Antichain::new()) {
                    self.send_peek_response(peek, response);
                } else {
                    self.pending_peeks.push(peek);
                }
                self.metrics.observe_pending_peeks(&self.pending_peeks);
            }

            Command::CancelPeek { conn_id } => {
                let pending_peeks_len = self.pending_peeks.len();
                let mut pending_peeks = std::mem::replace(
                    &mut self.pending_peeks,
                    Vec::with_capacity(pending_peeks_len),
                );
                for peek in pending_peeks.drain(..) {
                    if peek.conn_id == conn_id {
                        self.send_peek_response(peek, PeekResponse::Canceled);
                    } else {
                        self.pending_peeks.push(peek);
                    }
                }
            }

            Command::AdvanceAllLocalInputs { advance_to } => {
                for (_, local_input) in self.render_state.local_inputs.iter_mut() {
                    local_input.capability.downgrade(&advance_to);
                }
            }

            Command::Insert { id, updates } => {
                if self.timely_worker.index() == 0 {
                    let input = match self.render_state.local_inputs.get_mut(&id) {
                        Some(input) => input,
                        None => panic!("local input {} missing for insert", id),
                    };
                    let mut session = input.handle.session(input.capability.clone());
                    for update in updates {
                        assert!(update.timestamp >= *input.capability.time());
                        session.give((update.row, update.timestamp, update.diff));
                    }
                }
            }

            Command::AllowCompaction(list) => {
                for (id, frontier) in list {
                    self.render_state
                        .traces
                        .allow_compaction(id, frontier.borrow());
                    if let Some(ts_history) = self.render_state.ts_histories.get_mut(&id) {
                        ts_history.set_compaction_frontier(frontier.borrow());
                    }
                }
            }
            Command::DurabilityFrontierUpdates(list) => {
                for (id, frontier) in list {
                    if let Some(ts_history) = self.render_state.ts_histories.get_mut(&id) {
                        ts_history.set_durability_frontier(frontier.borrow());
                    }
                }
            }
            Command::EnableLogging(config) => {
                self.initialize_logging(&config);
            }
            Command::Shutdown => {
                // this should lead timely to wind down eventually
                self.render_state.traces.del_all_traces();
                self.shutdown_logging();
            }
            Command::AddSourceTimestamping {
                id,
                connector,
                bindings,
            } => {
                let source_timestamp_data = if let SourceConnector::External {
                    connector,
                    consistency,
                    ts_frequency,
                    ..
                } = connector
                {
                    let byo_default = TimestampBindingRc::new(None, self.now, true);
                    let rt_default = TimestampBindingRc::new(
                        Some(ts_frequency.as_millis().try_into().unwrap()),
                        self.now,
                        false,
                    );
                    match (connector, consistency) {
                        (ExternalSourceConnector::Kafka(_), Consistency::BringYourOwn(_)) => {
                            // TODO(aljoscha): Hey Ruchir 😃, should we always pull this to +Inf,
                            // and never persist bindings for BYO sources, like this?
                            byo_default.set_durability_frontier(Antichain::new().borrow());
                            Some(byo_default)
                        }
                        (ExternalSourceConnector::Kafka(_), Consistency::RealTime) => {
                            Some(rt_default)
                        }
                        (ExternalSourceConnector::AvroOcf(_), Consistency::BringYourOwn(_)) => {
                            byo_default.add_partition(PartitionId::None, None);
                            Some(byo_default)
                        }
                        (ExternalSourceConnector::AvroOcf(_), Consistency::RealTime) => {
                            rt_default.add_partition(PartitionId::None, None);
                            Some(rt_default)
                        }
                        (ExternalSourceConnector::File(_), Consistency::BringYourOwn(_)) => {
                            byo_default.add_partition(PartitionId::None, None);
                            Some(byo_default)
                        }
                        (ExternalSourceConnector::File(_), Consistency::RealTime) => {
                            rt_default.add_partition(PartitionId::None, None);
                            Some(rt_default)
                        }
                        (ExternalSourceConnector::Kinesis(_), Consistency::RealTime) => {
                            rt_default.add_partition(PartitionId::None, None);
                            Some(rt_default)
                        }
                        (ExternalSourceConnector::S3(_), Consistency::RealTime) => {
                            rt_default.add_partition(PartitionId::None, None);
                            Some(rt_default)
                        }
                        (ExternalSourceConnector::Kinesis(_), Consistency::BringYourOwn(_)) => {
                            log::error!("BYO timestamping not supported for Kinesis sources");
                            None
                        }
                        (ExternalSourceConnector::S3(_), Consistency::BringYourOwn(_)) => {
                            log::error!("BYO timestamping not supported for S3 sources");
                            None
                        }
                        (ExternalSourceConnector::Postgres(_), _) => {
                            log::debug!(
                                "Postgres sources do not communicate with the timestamper thread"
                            );
                            None
                        }
                        (ExternalSourceConnector::PubNub(_), _) => {
                            log::debug!(
                                "PubNub sources do not communicate with the timestamper thread"
                            );
                            None
                        }
                    }
                } else {
                    log::debug!(
                        "Timestamping not supported for local sources {}. Ignoring",
                        id
                    );
                    None
                };

                // Add any timestamp bindings that we were already aware of on restart.
                if let Some(data) = source_timestamp_data {
                    for (pid, timestamp, offset) in bindings {
                        data.add_partition(pid.clone(), None);
                        data.add_binding(pid, timestamp, offset, false);
                    }

                    let prev = self.render_state.ts_histories.insert(id, data);
                    assert!(prev.is_none());
                    self.reported_frontiers.insert(id, Antichain::from_elem(0));
                    self.reported_bindings_frontiers
                        .insert(id, Antichain::from_elem(0));
                } else {
                    assert!(bindings.is_empty());
                }
            }
            Command::AdvanceSourceTimestamp { id, update } => {
                if let Some(history) = self.render_state.ts_histories.get_mut(&id) {
                    match update {
                        TimestampSourceUpdate::BringYourOwn(pid, timestamp, offset) => {
                            // TODO: change the interface between the dataflow server and the
                            // timestamper. Specifically, we probably want to inform the timestamper
                            // of the timestamps we already know about so that it doesn't send us
                            // duplicate copies again.

                            let mut upper = Antichain::new();
                            history.read_upper(&mut upper);

                            if upper.less_equal(&timestamp) {
                                history.add_partition(pid.clone(), None);
                                history.add_binding(pid, timestamp, offset + 1, false);
                            }
                        }
                        TimestampSourceUpdate::RealTime(new_partition) => {
                            history.add_partition(new_partition, None);
                        }
                    };

                    let sources = self
                        .render_state
                        .ts_source_mapping
                        .entry(id)
                        .or_insert_with(Vec::new);
                    for source in sources {
                        if let Some(source) = source.upgrade() {
                            if let Some(token) = &*source {
                                token.activate();
                            }
                        }
                    }
                }
            }
            Command::DropSourceTimestamping { id } => {
                let prev = self.render_state.ts_histories.remove(&id);

                if prev.is_none() {
                    log::debug!("Attempted to drop timestamping for source {} that was not previously known", id);
                }

                let prev = self.render_state.ts_source_mapping.remove(&id);
                if prev.is_none() {
                    log::debug!("Attempted to drop timestamping for source {} not previously mapped to any instances", id);
                }

                self.reported_frontiers.remove(&id);
                self.reported_bindings_frontiers.remove(&id);
            }
        }
    }

    /// Scan pending peeks and attempt to retire each.
    fn process_peeks(&mut self) {
        let mut upper = Antichain::new();
        let pending_peeks_len = self.pending_peeks.len();
        let mut pending_peeks = std::mem::replace(
            &mut self.pending_peeks,
            Vec::with_capacity(pending_peeks_len),
        );
        for mut peek in pending_peeks.drain(..) {
            if let Some(response) = peek.seek_fulfillment(&mut upper) {
                self.send_peek_response(peek, response);
            } else {
                self.pending_peeks.push(peek);
            }
        }
    }

    /// Sends a response for this peek's resolution to the coordinator.
    ///
    /// Note that this function takes ownership of the `PendingPeek`, which is
    /// meant to prevent multiple responses to the same peek.
    fn send_peek_response(&mut self, peek: PendingPeek, response: PeekResponse) {
        // Respond with the response.
        self.feedback_tx
            .send(Response {
                worker_id: self.timely_worker.index(),
                message: WorkerFeedback::PeekResponse(peek.conn_id, response),
            })
            .expect("feedback receiver should not drop first");

        // Log responding to the peek request.
        if let Some(logger) = self.materialized_logger.as_mut() {
            logger.log(MaterializedEvent::Peek(peek.as_log_event(), false));
        }
    }

    /// Scan the shared tail response buffer, and forward results along.
    fn process_tails(&mut self) {
        let mut tail_responses = self.render_state.tail_response_buffer.borrow_mut();
        for (sink_id, response) in tail_responses.drain(..) {
            self.feedback_tx
                .send(Response {
                    worker_id: self.timely_worker.index(),
                    message: WorkerFeedback::TailResponse(sink_id, response),
                })
                .expect("feedback receiver should not drop first");
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
struct PendingPeek {
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
    map_filter_project: expr::SafeMfpPlan,
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
    fn collect_finished_data(&mut self) -> Result<Vec<Row>, String> {
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
        // Accumulated `Vec<Datum>` results that we are likely to return.
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

        while cursor.key_valid(&storage) {
            while cursor.val_valid(&storage) {
                // TODO: This arena could be maintained and reuse for longer
                // but it wasn't clear at what granularity we should flush
                // it to ensure we don't accidentally spike our memory use.
                // This choice is conservative, and not the end of the world
                // from a performance perspective.
                let arena = RowArena::new();
                let row = cursor.val(&storage);
                // TODO: We could unpack into a re-used allocation, except
                // for the arena above (the allocation would not be allowed
                // to outlive the arena above, from which it might borrow).
                let mut datums = row.unpack();
                if let Some(result) = self
                    .map_filter_project
                    .evaluate_into(&mut datums, &arena, &mut row_builder)
                    .map_err_to_string()?
                {
                    let mut copies = 0;
                    cursor.map_times(&storage, |time, diff| {
                        if time.less_equal(&self.timestamp) {
                            copies += diff;
                        }
                    });
                    if copies < 0 {
                        return Err(format!(
                            "Invalid data in source, saw retractions ({}) for row that does not exist: {:?}",
                            copies * -1,
                            row.unpack(),
                        ));
                    }

                    // When we have a LIMIT we can restrict the number of copies we make.
                    // This protects us when we have many copies of the same records, as
                    // the DD representation uses a binary count and may not exhaust our
                    // memory in situtations where this copying might.
                    if let Some(limit) = max_results {
                        let limit = std::convert::TryInto::<isize>::try_into(limit);
                        if let Ok(limit) = limit {
                            copies = std::cmp::min(copies, limit);
                        }
                    }
                    for _ in 0..copies {
                        results.push(result.clone());
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
                                    expr::compare_columns(
                                        &self.finishing.order_by,
                                        &left.unpack(),
                                        &right.unpack(),
                                        || left.cmp(right),
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
