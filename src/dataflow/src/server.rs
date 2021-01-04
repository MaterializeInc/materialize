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
use std::collections::HashMap;
use std::collections::VecDeque;
use std::net::TcpStream;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Mutex;
use std::time::{Instant, UNIX_EPOCH};

use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::Collection;
use futures::channel::mpsc::UnboundedReceiver;
use futures::executor::block_on;
use futures::future::TryFutureExt;
use futures::sink::{Sink, SinkExt};
use serde::{Deserialize, Serialize};
use timely::communication::allocator::generic::GenericBuilder;
use timely::communication::allocator::zero_copy::initialize::initialize_networking_from_sockets;
use timely::communication::initialize::WorkerGuards;
use timely::communication::Allocate;
use timely::dataflow::operators::unordered_input::UnorderedHandle;
use timely::dataflow::operators::ActivateCapability;
use timely::logging::Logger;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::progress::ChangeBatch;
use timely::worker::Worker as TimelyWorker;
use uuid::Uuid;

use dataflow_types::logging::LoggingConfig;
use dataflow_types::{
    DataflowDesc, DataflowError, MzOffset, PeekResponse, TimestampSourceUpdate, Update,
};
use expr::{GlobalId, MapFilterProject, PartitionId, RowSetFinishing, SourceInstanceId};
use ore::future::channel::mpsc::ReceiverExt;
use repr::{Diff, Row, RowArena, Timestamp};

use crate::arrangement::manager::{TraceBundle, TraceManager};
use crate::logging;
use crate::logging::materialized::MaterializedEvent;
use crate::operator::CollectionExt;
use crate::render::{self, RenderState};
use crate::server::metrics::Metrics;
use crate::source::cache::WorkerCacheData;

mod metrics;

/// A [`comm::broadcast::Token`] that permits broadcasting commands to the
/// Timely workers.
pub struct BroadcastToken;

impl comm::broadcast::Token for BroadcastToken {
    type Item = SequencedCommand;

    /// Returns true, to enable loopback.
    ///
    /// Since the coordinator lives on the same process as one set of
    /// workers, we need to enable loopback so that broadcasts are
    /// transmitted intraprocess and visible to those workers.
    fn loopback(&self) -> bool {
        true
    }
}

/// Explicit instructions for timely dataflow workers.
#[derive(Clone, Debug, Serialize, Deserialize)]
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
        tx: comm::mpsc::Sender<PeekResponse>,
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
    /// Advance worker timestamp
    AdvanceSourceTimestamp {
        /// The ID of the timestamped source
        id: SourceInstanceId,
        /// The associated update (RT or BYO)
        update: TimestampSourceUpdate,
    },
    /// Advance all local inputs to the given timestamp.
    AdvanceAllLocalInputs {
        /// The timestamp to advance to.
        advance_to: Timestamp,
    },
    /// Request that feedback is streamed to the provided channel.
    EnableFeedback(comm::mpsc::Sender<WorkerFeedbackWithMeta>),
    /// Request that cache data is streamed to the provided channel.
    EnableCaching(comm::mpsc::Sender<CacheMessage>),
    /// Request that the logging sources in the contained configuration are
    /// installed.
    EnableLogging(LoggingConfig),
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
    /// Shut down caching thread
    Shutdown,
}

/// Responses the worker can provide back to the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WorkerFeedback {
    /// A list of identifiers of traces, with prior and new upper frontiers.
    FrontierUppers(Vec<(GlobalId, ChangeBatch<Timestamp>)>),
    /// The id of a source whose source connector has been dropped
    DroppedSource(SourceInstanceId),
    /// The id of a source whose source connector has been created
    CreateSource(SourceInstanceId),
}

/// Initiates a timely dataflow computation, processing materialized commands.
///
/// TODO(benesch): pass a config struct here, or find some other way to cut
/// down on the number of arguments.
pub fn serve<C>(
    sockets: Vec<Option<TcpStream>>,
    threads: usize,
    process: usize,
    switchboard: comm::Switchboard<C>,
) -> Result<WorkerGuards<()>, String>
where
    C: comm::Connection,
{
    assert!(threads > 0);

    // Construct endpoints for each thread that will receive the coordinator's
    // sequenced command stream.
    //
    // TODO(benesch): package up this idiom of handing out ownership of N items
    // to the N timely threads that will be spawned. The Mutex<Vec<Option<T>>>
    // is hard to read through.
    let command_rxs = {
        let mut rx = switchboard.broadcast_rx(BroadcastToken).fanout();
        let command_rxs = Mutex::new((0..threads).map(|_| Some(rx.attach())).collect::<Vec<_>>());
        tokio::spawn(
            rx.shuttle()
                .map_err(|err| panic!("failure shuttling dataflow receiver commands: {}", err)),
        );
        command_rxs
    };

    let log_fn = Box::new(|_| None);
    let (builders, guard) = initialize_networking_from_sockets(sockets, process, threads, log_fn)
        .map_err(|err| format!("failed to initialize networking: {}", err))?;
    let builders = builders.into_iter().map(GenericBuilder::ZeroCopy).collect();

    let tokio_executor = tokio::runtime::Handle::current();
    timely::execute::execute_from(builders, Box::new(guard), move |timely_worker| {
        tokio_executor.enter(|| {
            let command_rx = command_rxs.lock().unwrap()[timely_worker.index() % threads]
                .take()
                .unwrap()
                .request_unparks();
            let worker_idx = timely_worker.index();
            Worker {
                timely_worker,
                render_state: RenderState {
                    traces: TraceManager::new(worker_idx),
                    local_inputs: HashMap::new(),
                    ts_source_mapping: HashMap::new(),
                    ts_histories: Default::default(),
                    ts_source_updates: Default::default(),
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
        })
    })
}

/// A type wrapper for the number of partitions associated with a source.
pub type PartitionCount = i32;

/// A type wrapper for a timestamp update
/// For real-time sources, it consists of a PartitionCount
/// For BYO sources, it consists of a mapping from PartitionId to a vector of
/// (PartitionCount, Timestamp, MzOffset) tuple.
pub enum TimestampDataUpdate {
    /// RT sources see a current estimate of the number of partitions for the soruce
    RealTime(PartitionCount),
    /// BYO sources see a list of (PartitionCount, Timestamp, MzOffset) timestamp updates
    BringYourOwn(HashMap<PartitionId, VecDeque<(PartitionCount, Timestamp, MzOffset)>>),
}
/// Map of source ID to timestamp data updates (RT or BYO).
pub type TimestampDataUpdates = Rc<RefCell<HashMap<SourceInstanceId, TimestampDataUpdate>>>;

/// List of sources that need to start being timestamped or have been dropped and no longer require
/// timestamping.
/// A source inserts a StartTimestamping to this vector on source creation, and adds a
/// StopTimestamping request once the operator for the source is dropped.
pub type TimestampMetadataUpdates = Rc<RefCell<Vec<TimestampMetadataUpdate>>>;

/// Possible timestamping metadata information messages that get sent from workers to coordinator
pub enum TimestampMetadataUpdate {
    /// Requests to start timestamping a source with given id
    StartTimestamping(SourceInstanceId),
    /// Request to stop timestamping a source wth given id
    StopTimestamping(SourceInstanceId),
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
    command_rx: UnboundedReceiver<SequencedCommand>,
    /// Peek commands that are awaiting fulfillment.
    pending_peeks: Vec<PendingPeek>,
    /// The channel over which fontier information is reported.
    feedback_tx: Option<Pin<Box<dyn Sink<WorkerFeedbackWithMeta, Error = ()>>>>,
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

            self.report_source_modifications();

            // Handle any received commands.
            let mut cmds = vec![];
            while let Ok(Some(cmd)) = self.command_rx.try_next() {
                cmds.push(cmd);
            }
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

    /// Report source drops or creations to the coordinator
    fn report_source_modifications(&mut self) {
        let mut updates = self.render_state.ts_source_updates.borrow_mut();
        for source_update in updates.iter() {
            match source_update {
                TimestampMetadataUpdate::StopTimestamping(id) => {
                    // A source was deleted
                    self.render_state.ts_histories.borrow_mut().remove(id);
                    self.render_state.ts_source_mapping.remove(id);
                    let connector = self.feedback_tx.as_mut().unwrap();
                    block_on(connector.send(WorkerFeedbackWithMeta {
                        worker_id: self.timely_worker.index(),
                        message: WorkerFeedback::DroppedSource(*id),
                    }))
                    .unwrap();
                }
                TimestampMetadataUpdate::StartTimestamping(id) => {
                    // A source was created
                    let connector = self.feedback_tx.as_mut().unwrap();
                    block_on(connector.send(WorkerFeedbackWithMeta {
                        worker_id: self.timely_worker.index(),
                        message: WorkerFeedback::CreateSource(*id),
                    }))
                    .unwrap();
                }
            }
        }
        updates.clear();
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
                    let lower = self.reported_frontiers.get_mut(&id).unwrap();
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
                block_on(feedback_tx.send(WorkerFeedbackWithMeta {
                    worker_id: self.timely_worker.index(),
                    message: WorkerFeedback::FrontierUppers(progress),
                }))
                .unwrap();
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
                    .advance_by(timestamp_frontier.borrow());
                trace_bundle
                    .errs_mut()
                    .advance_by(timestamp_frontier.borrow());
                trace_bundle
                    .oks_mut()
                    .distinguish_since(empty_frontier.borrow());
                trace_bundle
                    .errs_mut()
                    .distinguish_since(empty_frontier.borrow());
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
                        let mut tx = block_on(peek.tx.connect()).unwrap();
                        block_on(tx.send(PeekResponse::Canceled)).unwrap();

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
                self.feedback_tx =
                    Some(Box::pin(block_on(tx.connect()).unwrap().sink_map_err(
                        |err| panic!("error sending worker feedback: {}", err),
                    )));
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
            SequencedCommand::AdvanceSourceTimestamp { id, update } => {
                let mut timestamps = self.render_state.ts_histories.borrow_mut();
                if let Some(ts_entries) = timestamps.get_mut(&id) {
                    match ts_entries {
                        TimestampDataUpdate::BringYourOwn(entries) => {
                            if let TimestampSourceUpdate::BringYourOwn(
                                partition_count,
                                pid,
                                timestamp,
                                offset,
                            ) = update
                            {
                                let partition_entries =
                                    entries.entry(pid).or_insert_with(VecDeque::new);
                                let (_, last_ts, last_offset) = partition_entries
                                    .back()
                                    .unwrap_or(&(0, 0, MzOffset { offset: 0 }));
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
                                partition_entries.push_back((partition_count, timestamp, offset));
                            } else {
                                panic!("Unexpected message type. Expected BYO update.")
                            }
                        }
                        TimestampDataUpdate::RealTime(current_partition_count) => {
                            if let TimestampSourceUpdate::RealTime(partition_count) = update {
                                assert!(
                                    *current_partition_count <= partition_count,
                                    "The number of partitions \
                                     for source {} decreased from {} to {}",
                                    id,
                                    partition_count,
                                    current_partition_count
                                );
                                *current_partition_count = partition_count;
                            } else {
                                panic!("Expected message type. Expected RT update.");
                            }
                        }
                    }
                    let source = self
                        .render_state
                        .ts_source_mapping
                        .get(&id)
                        .expect("Id should be present");
                    if let Some(source) = source.upgrade() {
                        if let Some(token) = &*source {
                            token.activate();
                        }
                    }
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
    tx: comm::mpsc::Sender<PeekResponse>,
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
        let mut tx = block_on(self.tx.connect()).unwrap();
        let tx_result = block_on(tx.send(response));
        if let Err(e) = tx_result {
            block_on(tx.send(PeekResponse::Error(e.to_string()))).unwrap();
        }
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
                    "Negative multiplicity: {} for {}",
                    copies,
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
        let mut row_packer = repr::RowPacker::new();

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
                    .evaluate(&mut datums, &arena, &mut row_packer)
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
                            "Negative multiplicity: {} for {:?}",
                            copies,
                            row.unpack(),
                        ));
                    }

                    // TODO: In an ORDER BY .. LIMIT .. setting, once we have a full output
                    // we could compare each of these to the "least" current output, and
                    // avoid stashing the result and growing results.
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

/// The presence of this function forces `rustc` to instantiate the
/// slow-to-compile differential and timely templates while compiling this
/// crate. This means that iterating on crates that depend upon this crate is
/// much faster, because these templates don't need to be reinstantiated
/// whenever a downstream dependency changes. And iterating on this crate
/// doesn't really become slower, because you needed to instantiate these
/// templates anyway to run tests.
pub fn __explicit_instantiation__() {
    ore::hint::black_box(serve::<tokio::net::TcpStream> as fn(_, _, _, _) -> _);
}
