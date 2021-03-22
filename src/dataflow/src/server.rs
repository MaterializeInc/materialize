// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An interactive dataflow server.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Mutex;
use std::time::{Instant, UNIX_EPOCH};

use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::Collection;
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
use uuid::Uuid;

use dataflow_types::logging::LoggingConfig;
use dataflow_types::{
    Consistency, DataflowDesc, DataflowError, ExternalSourceConnector, MzOffset, PeekResponse,
    SourceConnector, TimestampSourceUpdate, Update,
};
use expr::{GlobalId, MapFilterProject, PartitionId, RowSetFinishing};
use repr::{Diff, Row, RowArena, Timestamp};

use crate::arrangement::manager::{TraceBundle, TraceManager};
use crate::logging;
use crate::logging::materialized::MaterializedEvent;
use crate::operator::CollectionExt;
use crate::render::{self, RenderState};
use crate::server::metrics::Metrics;
use crate::source::cache::WorkerCacheData;

mod metrics;

/// Explicit instructions for timely dataflow workers.
#[derive(Clone, Debug)]
pub enum SequencedCommand {
    /// Create a sequence of dataflows.
    CreateDataflows(Vec<DataflowDesc>),
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
    Peek {
        /// The identifier of the arrangement.
        id: GlobalId,
        /// An optional key that should be used for the arrangement.
        key: Option<Row>,
        /// The identifier of this peek request.
        ///
        /// Used in responses and cancelation requests.
        conn_id: u32,
        /// A communication link for sending a response.
        tx: mpsc::UnboundedSender<PeekResponse>,
        /// The logical timestamp at which the arrangement is queried.
        timestamp: Timestamp,
        /// Actions to apply to the result set before returning them.
        finishing: RowSetFinishing,
        /// Linear operation to apply in-line on each result.
        map_filter_project: MapFilterProject,
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
    /// Add a new source to be aware of for timestamping.
    AddSourceTimestamping {
        /// The ID of the timestamped source
        id: GlobalId,
        /// The connector for the timestamped source.
        connector: SourceConnector,
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
    /// Request that feedback is streamed to the provided channel.
    EnableFeedback(mpsc::UnboundedSender<WorkerFeedbackWithMeta>),
    /// Request that cache data is streamed to the provided channel.
    EnableCaching(mpsc::UnboundedSender<CacheMessage>),
    /// Request that the logging sources in the contained configuration are
    /// installed.
    EnableLogging(LoggingConfig),
    /// Request that a worker inject a materialized logging statement.
    ReportMaterializedLog(MaterializedEvent),
    /// Disconnect inputs, drain dataflows, and shut down timely workers.
    Shutdown,
}

/// Information from timely dataflow workers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerFeedbackWithMeta {
    /// Identifies the worker by its identifier.
    pub worker_id: usize,
    /// The feedback itself.
    pub message: WorkerFeedback,
}

/// All data and metadata messages that can be sent by dataflow workers or coordinator
/// to the cacher thread.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CacheMessage {
    /// Data to be cached (sent from dataflow workers)
    Data(WorkerCacheData),
    /// Add source to cache.
    AddSource(Uuid, GlobalId),
    /// Drop source from cache.
    DropSource(GlobalId),
}

/// Responses the worker can provide back to the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WorkerFeedback {
    /// A list of identifiers of traces, with prior and new upper frontiers.
    FrontierUppers(Vec<(GlobalId, ChangeBatch<Timestamp>)>),
}

/// Configures a dataflow server.
pub struct Config {
    /// Command stream receivers for each desired workers.
    ///
    /// The length of this vector determines the number of worker threads that
    /// will be spawned.
    pub command_receivers: Vec<crossbeam_channel::Receiver<SequencedCommand>>,
    /// The Timely worker configuration.
    pub timely_worker: timely::WorkerConfig,
}

/// Initiates a timely dataflow computation, processing materialized commands.
pub fn serve(config: Config) -> Result<WorkerGuards<()>, String> {
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
            Worker {
                timely_worker,
                render_state: RenderState {
                    traces: TraceManager::new(worker_idx),
                    local_inputs: HashMap::new(),
                    ts_source_mapping: HashMap::new(),
                    ts_histories: Default::default(),
                    dataflow_tokens: HashMap::new(),
                    caching_tx: None,
                },
                materialized_logger: None,
                command_rx,
                pending_peeks: Vec::new(),
                feedback_tx: None,
                reported_frontiers: HashMap::new(),
                metrics: Metrics::for_worker_id(worker_idx),
            }
            .run()
        },
    )
}

/// A type wrapper for a timestamp update
pub enum TimestampDataUpdate {
    /// RT sources see the current set of partitions known to the source.
    RealTime(HashSet<PartitionId>),
    /// BYO sources see a list of (Timestamp, MzOffset) timestamp updates
    BringYourOwn(HashMap<PartitionId, VecDeque<(Timestamp, MzOffset)>>),
}
/// Map of source ID to timestamp data updates (RT or BYO).
pub type TimestampDataUpdates = Rc<RefCell<HashMap<GlobalId, TimestampDataUpdate>>>;

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
    command_rx: crossbeam_channel::Receiver<SequencedCommand>,
    /// Peek commands that are awaiting fulfillment.
    pending_peeks: Vec<PendingPeek>,
    /// The channel over which frontier information is reported.
    feedback_tx: Option<mpsc::UnboundedSender<WorkerFeedbackWithMeta>>,
    /// Tracks the frontier information that has been sent over `feedback_tx`.
    reported_frontiers: HashMap<GlobalId, Antichain<Timestamp>>,
    /// Metrics bundle.
    metrics: Metrics,
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
        let unix = UNIX_EPOCH.elapsed().expect("time went backwards");

        // Establish loggers first, so we can either log the logging or not, as we like.
        let t_linked = std::rc::Rc::new(EventLink::new());
        let mut t_logger = BatchLogger::new(t_linked.clone(), granularity_ms);
        let d_linked = std::rc::Rc::new(EventLink::new());
        let mut d_logger = BatchLogger::new(d_linked.clone(), granularity_ms);
        let m_linked = std::rc::Rc::new(EventLink::new());
        let mut m_logger = BatchLogger::new(m_linked.clone(), granularity_ms);

        let mut t_traces = HashMap::new();
        let mut d_traces = HashMap::new();
        let mut m_traces = HashMap::new();

        if !logging.log_logging {
            // Construct logging dataflows and endpoints before registering any.
            t_traces.extend(logging::timely::construct(
                &mut self.timely_worker,
                logging,
                t_linked.clone(),
            ));
            d_traces.extend(logging::differential::construct(
                &mut self.timely_worker,
                logging,
                d_linked.clone(),
            ));
            m_traces.extend(logging::materialized::construct(
                &mut self.timely_worker,
                logging,
                m_linked.clone(),
            ));
        }

        // Register each logger endpoint.
        self.timely_worker.log_register().insert_logger(
            "timely",
            Logger::new(now, unix, self.timely_worker.index(), move |time, data| {
                t_logger.publish_batch(time, data)
            }),
        );

        self.timely_worker.log_register().insert_logger(
            "differential/arrange",
            Logger::new(now, unix, self.timely_worker.index(), move |time, data| {
                d_logger.publish_batch(time, data)
            }),
        );

        self.timely_worker.log_register().insert_logger(
            "materialized",
            Logger::new(now, unix, self.timely_worker.index(), move |time, data| {
                m_logger.publish_batch(time, data)
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
            ));
            d_traces.extend(logging::differential::construct(
                &mut self.timely_worker,
                logging,
                d_linked,
            ));
            m_traces.extend(logging::materialized::construct(
                &mut self.timely_worker,
                logging,
                m_linked,
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

            // Handle any received commands.
            let cmds: Vec<_> = self.command_rx.try_iter().collect();
            self.metrics.observe_command_queue(&cmds);
            for cmd in cmds {
                if let SequencedCommand::Shutdown = cmd {
                    shutdown = true;
                }
                self.metrics.observe_command(&cmd);
                self.handle_command(cmd);
            }

            self.metrics.observe_pending_peeks(&self.pending_peeks);
            self.metrics.observe_command_finish();
            self.process_peeks();
        }
    }

    /// Send progress information to the coordinator.
    fn report_frontiers(&mut self) {
        if let Some(feedback_tx) = &mut self.feedback_tx {
            let mut upper = Antichain::new();
            let mut progress = Vec::new();
            for (id, traces) in self.render_state.traces.traces.iter_mut() {
                // Read the upper frontier and compare to what we've reported.
                traces.oks_mut().read_upper(&mut upper);
                let lower = self
                    .reported_frontiers
                    .get_mut(&id)
                    .expect("Frontier missing!");
                if lower != &upper {
                    let mut changes = ChangeBatch::new();
                    for time in lower.elements().iter() {
                        changes.update(time.clone(), -1);
                    }
                    for time in upper.elements().iter() {
                        changes.update(time.clone(), 1);
                    }
                    changes.compact();
                    if !changes.is_empty() {
                        progress.push((*id, changes));
                    }
                    lower.clone_from(&upper);
                }
            }
            if let Some(logger) = self.materialized_logger.as_mut() {
                for (id, changes) in &mut progress {
                    for (time, diff) in changes.iter() {
                        logger.log(MaterializedEvent::Frontier(*id, *time, *diff));
                    }
                }
            }
            if !progress.is_empty() {
                feedback_tx
                    .send(WorkerFeedbackWithMeta {
                        worker_id: self.timely_worker.index(),
                        message: WorkerFeedback::FrontierUppers(progress),
                    })
                    .expect("feedback receriver should not drop first");
            }
        }
    }

    fn handle_command(&mut self, cmd: SequencedCommand) {
        match cmd {
            SequencedCommand::CreateDataflows(dataflows) => {
                for dataflow in dataflows.into_iter() {
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

                    render::build_dataflow(self.timely_worker, &mut self.render_state, dataflow);
                }
            }

            SequencedCommand::DropSources(names) => {
                for name in names {
                    self.render_state.local_inputs.remove(&name);
                }
            }
            SequencedCommand::DropSinks(ids) => {
                for id in ids {
                    self.render_state.dataflow_tokens.remove(&id);
                }
            }
            SequencedCommand::DropIndexes(ids) => {
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

            SequencedCommand::Peek {
                id,
                key,
                timestamp,
                conn_id,
                tx,
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
                    tx,
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
                let fulfilled = peek.seek_fulfillment(&mut Antichain::new());
                if !fulfilled {
                    self.pending_peeks.push(peek);
                } else {
                    // Log the fulfillment of the peek.
                    if let Some(logger) = self.materialized_logger.as_mut() {
                        logger.log(MaterializedEvent::Peek(peek.as_log_event(), false));
                    }
                }
                self.metrics.observe_pending_peeks(&self.pending_peeks);
            }

            SequencedCommand::CancelPeek { conn_id } => {
                let logger = &mut self.materialized_logger;
                self.pending_peeks.retain(|peek| {
                    if peek.conn_id == conn_id {
                        peek.tx
                            .send(PeekResponse::Canceled)
                            .expect("peek receiver should not drop first");

                        if let Some(logger) = logger {
                            logger.log(MaterializedEvent::Peek(peek.as_log_event(), false));
                        }

                        false // don't retain
                    } else {
                        true // retain
                    }
                })
            }

            SequencedCommand::AdvanceAllLocalInputs { advance_to } => {
                for (_, local_input) in self.render_state.local_inputs.iter_mut() {
                    local_input.capability.downgrade(&advance_to);
                }
            }

            SequencedCommand::Insert { id, updates } => {
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

            SequencedCommand::AllowCompaction(list) => {
                for (id, frontier) in list {
                    self.render_state
                        .traces
                        .allow_compaction(id, frontier.borrow());
                }
            }

            SequencedCommand::EnableFeedback(tx) => {
                self.feedback_tx = Some(tx);
            }
            SequencedCommand::EnableLogging(config) => {
                self.initialize_logging(&config);
            }
            SequencedCommand::EnableCaching(tx) => {
                self.render_state.caching_tx = Some(tx);
            }
            SequencedCommand::Shutdown => {
                // this should lead timely to wind down eventually
                self.render_state.traces.del_all_traces();
                self.shutdown_logging();
            }
            SequencedCommand::AddSourceTimestamping { id, connector } => {
                let byo_default = TimestampDataUpdate::BringYourOwn(HashMap::new());

                let source_timestamp_data = if let SourceConnector::External {
                    connector,
                    consistency,
                    ..
                } = connector
                {
                    match (connector, consistency) {
                        (ExternalSourceConnector::Kafka(_), Consistency::BringYourOwn(_)) => {
                            Some(byo_default)
                        }
                        (ExternalSourceConnector::Kafka(_), Consistency::RealTime) => {
                            let mut partitions = HashSet::new();
                            partitions.insert(PartitionId::Kafka(0));
                            Some(TimestampDataUpdate::RealTime(partitions))
                        }
                        (ExternalSourceConnector::AvroOcf(_), Consistency::BringYourOwn(_)) => {
                            Some(byo_default)
                        }
                        (ExternalSourceConnector::AvroOcf(_), Consistency::RealTime) => {
                            let mut partitions = HashSet::new();
                            partitions.insert(PartitionId::File);
                            Some(TimestampDataUpdate::RealTime(partitions))
                        }
                        (ExternalSourceConnector::File(_), Consistency::BringYourOwn(_)) => {
                            Some(byo_default)
                        }
                        (ExternalSourceConnector::File(_), Consistency::RealTime) => {
                            let mut partitions = HashSet::new();
                            partitions.insert(PartitionId::File);
                            Some(TimestampDataUpdate::RealTime(partitions))
                        }
                        (ExternalSourceConnector::Kinesis(_), Consistency::RealTime) => {
                            let mut partitions = HashSet::new();
                            partitions.insert(PartitionId::Kinesis);
                            Some(TimestampDataUpdate::RealTime(partitions))
                        }
                        (ExternalSourceConnector::S3(_), Consistency::RealTime) => {
                            let mut partitions = HashSet::new();
                            partitions.insert(PartitionId::S3);
                            Some(TimestampDataUpdate::RealTime(partitions))
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

                if let Some(data) = source_timestamp_data {
                    let prev = self.render_state.ts_histories.borrow_mut().insert(id, data);
                    assert!(prev.is_none());
                }
            }
            SequencedCommand::AdvanceSourceTimestamp { id, update } => {
                let mut timestamps = self.render_state.ts_histories.borrow_mut();
                if let Some(ts_entries) = timestamps.get_mut(&id) {
                    match ts_entries {
                        TimestampDataUpdate::BringYourOwn(entries) => {
                            if let TimestampSourceUpdate::BringYourOwn(pid, timestamp, offset) =
                                update
                            {
                                let partition_entries =
                                    entries.entry(pid).or_insert_with(VecDeque::new);
                                let (last_ts, last_offset) = partition_entries
                                    .back()
                                    .unwrap_or(&(0, MzOffset { offset: 0 }));
                                assert!(
                                    offset >= *last_offset,
                                    "offset should not go backwards, but {} < {}",
                                    offset,
                                    last_offset
                                );
                                assert!(
                                    timestamp > *last_ts,
                                    "timestamp should move forwards, but {} <= {}",
                                    timestamp,
                                    last_ts
                                );
                                partition_entries.push_back((timestamp, offset));
                            } else {
                                panic!("Unexpected message type. Expected BYO update.")
                            }
                        }
                        TimestampDataUpdate::RealTime(partitions) => {
                            if let TimestampSourceUpdate::RealTime(new_partition) = update {
                                partitions.insert(new_partition);
                            } else {
                                panic!("Expected message type. Expected RT update.");
                            }
                        }
                    }

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
            SequencedCommand::DropSourceTimestamping { id } => {
                let mut timestamps = self.render_state.ts_histories.borrow_mut();
                let prev = timestamps.remove(&id);

                if prev.is_none() {
                    log::debug!("Attempted to drop timestamping for source {} that was not previously known", id);
                }

                let prev = self.render_state.ts_source_mapping.remove(&id);
                if prev.is_none() {
                    log::debug!("Attempted to drop timestamping for source {} not previously mapped to any instances", id);
                }
            }
            SequencedCommand::ReportMaterializedLog(ev) => {
                if self.timely_worker.index() == 0 {
                    self.materialized_logger
                        .as_ref()
                        .map(|logger| logger.log(ev));
                }
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
            let success = peek.seek_fulfillment(&mut upper);
            if !success {
                self.pending_peeks.push(peek);
            } else {
                // Log the fulfillment of the peek.
                if let Some(logger) = self.materialized_logger.as_mut() {
                    logger.log(MaterializedEvent::Peek(peek.as_log_event(), false));
                }
            }
        }
    }
}

pub struct LocalInput {
    pub handle: UnorderedHandle<Timestamp, (Row, Timestamp, Diff)>,
    pub capability: ActivateCapability<Timestamp>,
}

/// An in-progress peek, and data to eventually fulfill it.
#[derive(Clone)]
struct PendingPeek {
    /// The identifier of the dataflow to peek.
    id: GlobalId,
    /// An optional key to use for the arrangement.
    key: Option<Row>,
    /// The ID of the connection that submitted the peek. For logging only.
    conn_id: u32,
    /// A transmitter connected to the intended recipient of the peek.
    tx: mpsc::UnboundedSender<PeekResponse>,
    /// Time at which the collection should be materialized.
    timestamp: Timestamp,
    /// Finishing operations to perform on the peek, like an ordering and a
    /// limit.
    finishing: RowSetFinishing,
    /// Linear operators to apply in-line to all results.
    map_filter_project: MapFilterProject,
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
    fn seek_fulfillment(&mut self, upper: &mut Antichain<Timestamp>) -> bool {
        self.trace_bundle.oks_mut().read_upper(upper);
        if upper.less_equal(&self.timestamp) {
            return false;
        }
        self.trace_bundle.errs_mut().read_upper(upper);
        if upper.less_equal(&self.timestamp) {
            return false;
        }
        let response = match self.collect_finished_data() {
            Ok(rows) => PeekResponse::Rows(rows),
            Err(text) => PeekResponse::Error(text),
        };
        self.tx
            .send(response)
            .expect("peek receiver should not drop first");
        true
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
                    "Invalid data in source, saw retractions ({}) for row that does not exist: {}",
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
                    .evaluate(&mut datums, &arena)
                    .map_err(|e| e.to_string())?
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
