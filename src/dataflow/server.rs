// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An interactive dataflow server.

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::net::TcpStream;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Mutex;

use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::TraceReader;

use lazy_static::lazy_static;
use timely::communication::allocator::generic::GenericBuilder;
use timely::communication::allocator::zero_copy::initialize::initialize_networking_from_sockets;
use timely::communication::initialize::WorkerGuards;
use timely::communication::Allocate;
use timely::dataflow::operators::unordered_input::UnorderedHandle;
use timely::dataflow::operators::ActivateCapability;
use timely::progress::frontier::Antichain;
use timely::progress::ChangeBatch;
use timely::worker::Worker as TimelyWorker;

use futures::channel::mpsc::UnboundedReceiver;
use futures::executor::block_on;
use futures::future::TryFutureExt;
use futures::sink::{Sink, SinkExt};
use prometheus::{register_int_gauge_vec, IntGauge, IntGaugeVec};
use serde::{Deserialize, Serialize};

use super::render;
use crate::arrangement::{
    manager::{KeysValsHandle, WithDrop},
    TraceManager,
};
use dataflow_types::logging::LoggingConfig;
use dataflow_types::{
    compare_columns, Consistency, DataflowDesc, Diff, IndexDesc, KafkaSourceConnector,
    PeekResponse, RowSetFinishing, Timestamp, Update,
};
use expr::{EvalEnv, GlobalId, SourceInstanceId};
use ore::future::channel::mpsc::ReceiverExt;
use repr::{Datum, RelationType, Row, RowArena};

use crate::logging;
use crate::logging::materialized::MaterializedEvent;

use crate::source::SourceToken;

lazy_static! {
    static ref COMMAND_QUEUE_RAW: IntGaugeVec = register_int_gauge_vec!(
        "mz_worker_command_queue_size",
        "the number of commands we would like to handle",
        &["worker"]
    )
    .unwrap();
    static ref PENDING_PEEKS_RAW: IntGaugeVec = register_int_gauge_vec!(
        "mz_worker_pending_peeks_queue_size",
        "the number of peeks remaining to be processed",
        &["worker"]
    )
    .unwrap();
}

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
    /// Peek at a materialized view.
    Peek {
        id: GlobalId,
        conn_id: u32,
        tx: comm::mpsc::Sender<PeekResponse>,
        timestamp: Timestamp,
        finishing: RowSetFinishing,
        project: Option<Vec<usize>>,
        filter: Vec<expr::ScalarExpr>,
        eval_env: EvalEnv,
    },
    /// Cancel the peek associated with the given `conn_id`.
    CancelPeek { conn_id: u32 },
    /// Create a local input named `id`
    CreateLocalInput {
        name: String,
        index_id: GlobalId,
        index: IndexDesc,
        on_type: RelationType,
        advance_to: Timestamp,
    },
    /// Insert `updates` into the local input named `id`.
    Insert {
        id: GlobalId,
        updates: Vec<Update>,
        advance_to: Timestamp,
    },
    /// Enable compaction in views.
    ///
    /// Each entry in the vector names a view and provides a frontier after which
    /// accumulations must be correct. The workers gain the liberty of compacting
    /// the corresponding maintained traces up through that frontier.
    AllowCompaction(Vec<(GlobalId, Vec<Timestamp>)>),
    /// Append a new event to the log stream.
    AppendLog(MaterializedEvent),
    /// Advance worker timestamp
    AdvanceSourceTimestamp {
        id: SourceInstanceId,
        timestamp: Timestamp,
        offset: i64,
    },
    /// Request that feedback is streamed to the provided channel.
    EnableFeedback(comm::mpsc::Sender<WorkerFeedbackWithMeta>),
    /// Disconnect inputs, drain dataflows, and shut down timely workers.
    Shutdown,
}

/// Information from timely dataflow workers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerFeedbackWithMeta {
    pub worker_id: usize,
    pub message: WorkerFeedback,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WorkerFeedback {
    /// A list of identifiers of traces, with prior and new upper frontiers.
    FrontierUppers(Vec<(GlobalId, ChangeBatch<Timestamp>)>),
    /// The id of a source whose source connector has been dropped
    DroppedSource(SourceInstanceId),
    /// The id of a source whose source connector has been created
    CreateSource(SourceInstanceId, KafkaSourceConnector, Consistency),
}

/// Initiates a timely dataflow computation, processing materialized commands.
///
/// TODO(benesch): pass a config struct here, or find some other way to cut
/// down on the number of arguments.
#[allow(clippy::too_many_arguments)]
pub fn serve<C>(
    sockets: Vec<Option<TcpStream>>,
    threads: usize,
    process: usize,
    switchboard: comm::Switchboard<C>,
    executor: tokio::runtime::Handle,
    advance_timestamp: bool,
    logging_config: Option<dataflow_types::logging::LoggingConfig>,
) -> Result<WorkerGuards<()>, String>
where
    C: comm::Connection,
{
    // Construct endpoints for each thread that will receive the coordinator's
    // sequenced command stream.
    //
    // TODO(benesch): package up this idiom of handing out ownership of N items
    // to the N timely threads that will be spawned. The Mutex<Vec<Option<T>>>
    // is hard to read through.
    let command_rxs = {
        let mut rx = switchboard.broadcast_rx(BroadcastToken).fanout();
        let command_rxs = Mutex::new((0..threads).map(|_| Some(rx.attach())).collect::<Vec<_>>());
        executor.spawn(
            rx.shuttle()
                .map_err(|err| panic!("failure shuttling dataflow receiver commands: {}", err)),
        );
        command_rxs
    };

    let log_fn = Box::new(|_| None);
    let (builders, guard) = initialize_networking_from_sockets(sockets, process, threads, log_fn)
        .map_err(|err| format!("failed to initialize networking: {}", err))?;
    let builders = builders
        .into_iter()
        .map(|x| GenericBuilder::ZeroCopy(x))
        .collect();

    timely::execute::execute_from(builders, Box::new(guard), move |timely_worker| {
        executor.enter(|| {
            let command_rx = command_rxs.lock().unwrap()[timely_worker.index() % threads]
                .take()
                .unwrap()
                .request_unparks(&executor);
            let worker_idx = timely_worker.index();
            Worker {
                inner: timely_worker,
                pending_peeks: Vec::new(),
                traces: TraceManager::default(),
                logging_config: logging_config.clone(),
                feedback_tx: None,
                command_rx,
                materialized_logger: None,
                sink_tokens: HashMap::new(),
                local_inputs: HashMap::new(),
                reported_frontiers: HashMap::new(),
                executor: executor.clone(),
                metrics: Metrics::for_worker_id(worker_idx),
                advance_timestamp,
                ts_histories: Default::default(),
                ts_source_mapping: HashMap::new(),
                ts_source_drops: Default::default(),
            }
            .run()
        })
    })
}

pub type TimestampHistories = Rc<RefCell<HashMap<SourceInstanceId, Vec<(Timestamp, i64)>>>>;
pub type TimestampChanges = Rc<
    RefCell<
        Vec<(
            SourceInstanceId,
            Option<(KafkaSourceConnector, Consistency)>,
        )>,
    >,
>;

struct Worker<'w, A>
where
    A: Allocate,
{
    inner: &'w mut TimelyWorker<A>,
    pending_peeks: Vec<PendingPeek>,
    traces: TraceManager,
    logging_config: Option<LoggingConfig>,
    feedback_tx: Option<Pin<Box<dyn Sink<WorkerFeedbackWithMeta, Error = ()>>>>,
    command_rx: UnboundedReceiver<SequencedCommand>,
    materialized_logger: Option<logging::materialized::Logger>,
    sink_tokens: HashMap<GlobalId, Box<dyn Any>>,
    local_inputs: HashMap<GlobalId, LocalInput>,
    advance_timestamp: bool,
    ts_source_mapping: HashMap<SourceInstanceId, Weak<Option<SourceToken>>>,
    ts_histories: TimestampHistories,
    ts_source_drops: TimestampChanges,
    reported_frontiers: HashMap<GlobalId, Antichain<Timestamp>>,
    executor: tokio::runtime::Handle,
    metrics: Metrics,
}

/// Prometheus metrics that we would like to easily export
struct Metrics {
    /// The size of the command queue
    ///
    /// Updated every time we decide to handle some
    command_queue: IntGauge,
    /// The number of pending peeks
    ///
    /// Updated every time we successfully fulfill a peek
    pending_peeks: IntGauge,
}

impl Metrics {
    fn for_worker_id(id: usize) -> Metrics {
        let worker_id = id.to_string();

        Metrics {
            command_queue: COMMAND_QUEUE_RAW.with_label_values(&[&worker_id]),
            pending_peeks: PENDING_PEEKS_RAW.with_label_values(&[&worker_id]),
        }
    }

    fn observe_command_queue(&self, commands: &[SequencedCommand]) {
        self.command_queue.set(commands.len() as i64);
    }

    fn observe_pending_peeks(&self, pending_peeks: &[PendingPeek]) {
        self.pending_peeks.set(pending_peeks.len() as i64);
    }
}

impl<'w, A> Worker<'w, A>
where
    A: Allocate + 'w,
{
    /// Initializes timely dataflow logging and publishes as a view.
    ///
    /// The initialization respects the setting of `self.logging_config`, and in particular
    /// if it is set to `None` then nothing happens. This has the potential to crash and burn
    /// if logging is not initialized and anyone tries to use it.
    fn initialize_logging(&mut self) {
        if let Some(logging) = &self.logging_config {
            use crate::logging::BatchLogger;
            use timely::dataflow::operators::capture::event::link::EventLink;

            let granularity_ms =
                std::cmp::max(1, logging.granularity_ns() / 1_000_000) as Timestamp;

            // Establish loggers first, so we can either log the logging or not, as we like.
            let t_linked = std::rc::Rc::new(EventLink::new());
            let mut t_logger = BatchLogger::new(t_linked.clone(), granularity_ms);
            let d_linked = std::rc::Rc::new(EventLink::new());
            let mut d_logger = BatchLogger::new(d_linked.clone(), granularity_ms);
            let m_linked = std::rc::Rc::new(EventLink::new());
            let mut m_logger = BatchLogger::new(m_linked.clone(), granularity_ms);

            // Construct logging dataflows and endpoints before registering any.
            let t_traces = logging::timely::construct(&mut self.inner, logging, t_linked);
            let d_traces = logging::differential::construct(&mut self.inner, logging, d_linked);
            let m_traces = logging::materialized::construct(&mut self.inner, logging, m_linked);

            // Register each logger endpoint.
            self.inner
                .log_register()
                .insert::<timely::logging::TimelyEvent, _>("timely", move |time, data| {
                    t_logger.publish_batch(time, data)
                });

            self.inner
                .log_register()
                .insert::<differential_dataflow::logging::DifferentialEvent, _>(
                    "differential/arrange",
                    move |time, data| d_logger.publish_batch(time, data),
                );

            self.inner
                .log_register()
                .insert::<logging::materialized::MaterializedEvent, _>(
                    "materialized",
                    move |time, data| m_logger.publish_batch(time, data),
                );

            // Install traces as maintained indexes
            for (log, (_, trace)) in t_traces {
                self.traces.set(log.index_id(), WithDrop::from(trace));
                self.reported_frontiers
                    .insert(log.index_id(), Antichain::from_elem(0));
            }
            for (log, (_, trace)) in d_traces {
                self.traces.set(log.index_id(), WithDrop::from(trace));
                self.reported_frontiers
                    .insert(log.index_id(), Antichain::from_elem(0));
            }
            for (log, (_, trace)) in m_traces {
                self.traces.set(log.index_id(), WithDrop::from(trace));
                self.reported_frontiers
                    .insert(log.index_id(), Antichain::from_elem(0));
            }

            self.materialized_logger = self.inner.log_register().get("materialized");
        }
    }

    /// Disables timely dataflow logging.
    ///
    /// This does not unpublish views and is only useful to terminate logging streams to ensure that
    /// materialized can terminate cleanly.
    fn shutdown_logging(&mut self) {
        self.inner.log_register().remove("timely");
        self.inner.log_register().remove("differential/arrange");
        self.inner.log_register().remove("materialized");
    }

    /// Draws from `dataflow_command_receiver` until shutdown.
    fn run(&mut self) {
        // Logging can be initialized with a "granularity" in nanoseconds, so that events are only
        // produced at logical times that are multiples of this many nanoseconds, which can reduce
        // the churn of the underlying computation.

        self.initialize_logging();

        let mut shutdown = false;
        while !shutdown {
            // Enable trace compaction.
            self.traces.maintenance();

            // Ask Timely to execute a unit of work. If Timely decides there's
            // nothing to do, it will park the thread. We rely on another thread
            // unparking us when there's new work to be done, e.g., when sending
            // a command or when new Kafka messages have arrived.
            self.inner.step_or_park(None);

            // Report frontier information back the coordinator.
            self.report_frontiers();

            self.report_source_drops();

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
                self.handle_command(cmd);
            }

            self.metrics.observe_pending_peeks(&self.pending_peeks);
            self.process_peeks();
        }
    }

    /// Send source drop notifications to the coordinator
    fn report_source_drops(&mut self) {
        let mut updates = self.ts_source_drops.borrow_mut();
        for (id, ksc) in updates.iter() {
            if ksc.is_none() {
                // A source was deleted
                self.ts_histories.borrow_mut().remove(id);
                self.ts_source_mapping.remove(id);
                let connector = self.feedback_tx.as_mut().unwrap();
                block_on(connector.send(WorkerFeedbackWithMeta {
                    worker_id: self.inner.index(),
                    message: WorkerFeedback::DroppedSource(*id),
                }))
                .unwrap();
            } else {
                // A source was created
                let connector = self.feedback_tx.as_mut().unwrap();
                block_on(connector.send(WorkerFeedbackWithMeta {
                    worker_id: self.inner.index(),
                    message: WorkerFeedback::CreateSource(
                        *id,
                        ksc.as_ref().unwrap().0.clone(),
                        ksc.as_ref().unwrap().1.clone(),
                    ),
                }))
                .unwrap();
            }
        }
        updates.clear();
    }

    /// Send progress information to the coordinator.
    fn report_frontiers(&mut self) {
        if let Some(feedback_tx) = &mut self.feedback_tx {
            let mut upper = Antichain::new();
            let mut progress = Vec::new();
            let ids = self.traces.traces.keys().cloned().collect::<Vec<_>>();
            for id in ids {
                if let Some(trace) = self.traces.get(&id) {
                    // Read the upper frontier and compare to what we've reported.
                    trace.clone().read_upper(&mut upper);
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
                            progress.push((id, changes));
                        }
                        lower.clone_from(&upper);
                    }
                }
            }
            block_on(feedback_tx.send(WorkerFeedbackWithMeta {
                worker_id: self.inner.index(),
                message: WorkerFeedback::FrontierUppers(progress),
            }))
            .unwrap();
        }
    }

    fn handle_command(&mut self, cmd: SequencedCommand) {
        match cmd {
            SequencedCommand::CreateDataflows(dataflows) => {
                for dataflow in dataflows.into_iter() {
                    for (id, _, _) in dataflow.index_exports.iter() {
                        self.reported_frontiers.insert(*id, Antichain::from_elem(0));
                        if let Some(logger) = self.materialized_logger.as_mut() {
                            logger.log(MaterializedEvent::Dataflow(*id, true));
                        }
                    }

                    render::build_dataflow(
                        dataflow,
                        &mut self.traces,
                        self.inner,
                        &mut self.sink_tokens,
                        self.advance_timestamp,
                        &mut self.ts_source_mapping,
                        self.ts_histories.clone(),
                        self.ts_source_drops.clone(),
                        &mut self.materialized_logger,
                        &self.executor,
                    );
                }
            }

            SequencedCommand::DropSources(names) => {
                for name in names {
                    self.local_inputs.remove(&name);
                }
            }
            SequencedCommand::DropSinks(ids) => {
                for id in ids {
                    self.sink_tokens.remove(&id);
                }
            }
            SequencedCommand::DropIndexes(ids) => {
                for id in ids {
                    self.traces.del_trace(&id);
                    if let Some(logger) = self.materialized_logger.as_mut() {
                        logger.log(MaterializedEvent::Dataflow(id, false));
                    }
                    self.reported_frontiers
                        .remove(&id)
                        .expect("Dropped index with no frontier");
                }
            }

            SequencedCommand::Peek {
                id,
                timestamp,
                conn_id,
                tx,
                finishing,
                project,
                filter,
                eval_env,
            } => {
                // Acquire a copy of the trace suitable for fulfilling the peek.
                let mut trace = self.traces.get(&id).unwrap().clone();
                trace.advance_by(&[timestamp]);
                trace.distinguish_since(&[]);
                // Prepare a description of the peek work to do.
                let mut peek = PendingPeek {
                    id,
                    conn_id,
                    tx,
                    timestamp,
                    finishing,
                    trace,
                    project,
                    filter,
                    eval_env,
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
                self.metrics
                    .pending_peeks
                    .set(self.pending_peeks.len() as i64);
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

            SequencedCommand::CreateLocalInput {
                name,
                index_id,
                index,
                on_type,
                advance_to,
            } => {
                let view_id = index.on_id;
                render::build_local_input(
                    &mut self.traces,
                    self.inner,
                    &mut self.local_inputs,
                    index_id,
                    &name,
                    index,
                    on_type,
                );
                self.reported_frontiers
                    .insert(index_id, Antichain::from_elem(0));
                if let Some(input) = self.local_inputs.get_mut(&view_id) {
                    input.capability.downgrade(&advance_to);
                }
            }

            SequencedCommand::Insert {
                id,
                updates,
                advance_to,
            } => {
                if let Some(input) = self.local_inputs.get_mut(&id) {
                    let mut session = input.handle.session(input.capability.clone());
                    for update in updates {
                        assert!(update.timestamp >= *input.capability.time());
                        session.give((update.row, update.timestamp, update.diff));
                    }
                }
                for (_, local_input) in self.local_inputs.iter_mut() {
                    local_input.capability.downgrade(&advance_to);
                }
            }

            SequencedCommand::AllowCompaction(list) => {
                for (id, frontier) in list {
                    self.traces.allow_compaction(id, &frontier[..]);
                }
            }

            SequencedCommand::AppendLog(event) => {
                if self.inner.index() == 0 {
                    if let Some(logger) = self.materialized_logger.as_mut() {
                        logger.log(event);
                    }
                }
            }

            SequencedCommand::EnableFeedback(tx) => {
                self.feedback_tx =
                    Some(Box::pin(block_on(tx.connect()).unwrap().sink_map_err(
                        |err| panic!("error sending worker feedback: {}", err),
                    )));
            }
            SequencedCommand::Shutdown => {
                // this should lead timely to wind down eventually
                self.traces.del_all_traces();
                self.shutdown_logging();
            }
            SequencedCommand::AdvanceSourceTimestamp {
                id,
                timestamp,
                offset,
            } => {
                let mut timestamps = self.ts_histories.borrow_mut();
                if let Some(entries) = timestamps.get_mut(&id) {
                    entries.push((timestamp, offset));
                    let last_offset = if let Some(offs) = timestamps.get(&id).unwrap().last() {
                        offs.1
                    } else {
                        -1
                    };
                    if last_offset == offset {
                        // We only activate the Kakfa source if the offset is the same as the last
                        // offset as new data already triggers the Kafka source's activation
                        let source = self
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

pub(crate) struct LocalInput {
    pub handle: UnorderedHandle<Timestamp, (Row, Timestamp, Diff)>,
    pub capability: ActivateCapability<Timestamp>,
}

/// An in-progress peek, and data to eventually fulfill it.
#[derive(Clone)]
struct PendingPeek {
    /// The identifier of the dataflow to peek.
    id: GlobalId,
    /// The ID of the connection that submitted the peek. For logging only.
    conn_id: u32,
    /// A transmitter connected to the intended recipient of the peek.
    tx: comm::mpsc::Sender<PeekResponse>,
    /// Time at which the collection should be materialized.
    timestamp: Timestamp,
    /// Finishing operations to perform on the peek, like an ordering and a
    /// limit.
    finishing: RowSetFinishing,
    project: Option<Vec<usize>>,
    filter: Vec<expr::ScalarExpr>,
    eval_env: EvalEnv,
    /// The data from which the trace derives.
    trace: WithDrop<KeysValsHandle>,
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
        self.trace.read_upper(upper);
        if !upper.less_equal(&self.timestamp) {
            let response = match self.collect_finished_data() {
                Ok(rows) => PeekResponse::Rows(rows),
                Err(text) => PeekResponse::Error(text),
            };

            let mut tx = block_on(self.tx.connect()).unwrap();
            block_on(tx.send(response)).unwrap();

            true
        } else {
            false
        }
    }

    /// Collects data for a known-complete peek.
    fn collect_finished_data(&mut self) -> Result<Vec<Row>, String> {
        let (mut cursor, storage) = self.trace.cursor();
        let mut results = Vec::new();

        // We can limit the record enumeration if i. there is a limit set,
        // and ii. if the specified ordering is empty (specifies no order).
        let limit = if self.finishing.order_by.is_empty() {
            self.finishing.limit.map(|l| l + self.finishing.offset)
        } else {
            None
        };

        while cursor.key_valid(&storage) && limit.map(|l| results.len() < l).unwrap_or(true) {
            while cursor.val_valid(&storage) && limit.map(|l| results.len() < l).unwrap_or(true) {
                let row = cursor.val(&storage);
                let datums = row.unpack();
                // Before (expensively) determining how many copies of a row
                // we have, let's eliminate rows that we don't care about.
                if self.filter.iter().all(|predicate| {
                    let temp_storage = RowArena::new();
                    predicate.eval(&datums, &self.eval_env, &temp_storage) == Datum::True
                }) {
                    // Differential dataflow represents collections with binary counts,
                    // but our output representation is unary (as many rows as reported
                    // by the count). We should determine this count, and especially if
                    // it is non-zero, before producing any output data.
                    let mut copies = 0;
                    cursor.map_times(&storage, |time, diff| {
                        use timely::order::PartialOrder;
                        if time.less_equal(&self.timestamp) {
                            copies += diff;
                        }
                    });
                    if copies < 0 {
                        return Result::Err(format!(
                            "Negative multiplicity: {} for {:?}",
                            copies,
                            row.unpack(),
                        ));
                    }

                    // TODO: We could push a count here, as we create owned output later.
                    for _ in 0..copies {
                        results.push(row);
                    }
                }
                cursor.step_val(&storage);
            }
            cursor.step_key(&storage)
        }

        // If we have extracted a projection, we should re-write the order_by columns.
        if let Some(columns) = &self.project {
            for key in self.finishing.order_by.iter_mut() {
                key.column = columns[key.column];
            }
        }

        // TODO: We could sort here in any case, as it allows a merge sort at the coordinator.
        if let Some(limit) = self.finishing.limit {
            let offset_plus_limit = limit + self.finishing.offset;
            if results.len() > offset_plus_limit {
                // The `results` should be sorted by `Row`, which means we only
                // need to re-order `results` when there is a non-empty order_by.
                if !self.finishing.order_by.is_empty() {
                    pdqselect::select_by(&mut results, offset_plus_limit, |left, right| {
                        compare_columns(
                            &self.finishing.order_by,
                            &left.unpack(),
                            &right.unpack(),
                            || left.cmp(right),
                        )
                    });
                }
                results.truncate(offset_plus_limit);
            }
        }

        Ok(if let Some(columns) = &self.project {
            results
                .iter()
                .map({
                    move |row| {
                        let datums = row.unpack();
                        Row::pack(columns.iter().map(|i| datums[*i]))
                    }
                })
                .collect()
        } else {
            results.iter().map(|row| (*row).clone()).collect()
        })
    }
}
