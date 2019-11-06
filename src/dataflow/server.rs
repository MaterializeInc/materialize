// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! An interactive dataflow server.

use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::TraceReader;

use timely::communication::allocator::generic::GenericBuilder;
use timely::communication::allocator::zero_copy::initialize::initialize_networking_from_sockets;
use timely::communication::initialize::WorkerGuards;
use timely::communication::Allocate;
use timely::dataflow::operators::unordered_input::{ActivateCapability, UnorderedHandle};
use timely::progress::frontier::Antichain;
use timely::progress::ChangeBatch;
use timely::worker::Worker as TimelyWorker;

use futures::sync::mpsc::UnboundedReceiver;
use futures::{Future, Sink};
use ore::future::sync::mpsc::ReceiverExt;
use ore::future::FutureExt;
use repr::{Datum, Row, RowPacker, RowUnpacker};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::Mutex;

use super::render;
use crate::arrangement::{
    manager::{KeysValsHandle, WithDrop},
    TraceManager,
};
use crate::logging;
use crate::logging::materialized::MaterializedEvent;
use dataflow_types::logging::LoggingConfig;
use dataflow_types::{
    compare_columns, DataflowDesc, Diff, PeekResponse, RowSetFinishing, Timestamp, Update,
};

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
    DropSources(Vec<String>),
    /// Drop the views bound to these names.
    DropViews(Vec<String>),
    /// Drop the sinks bound to these names.
    DropSinks(Vec<String>),
    /// Drop the indexes bound to these names.
    DropIndexes(Vec<(String, Vec<usize>)>),
    /// Peek at a materialized view.
    Peek {
        name: String,
        conn_id: u32,
        tx: comm::mpsc::Sender<PeekResponse>,
        timestamp: Timestamp,
        finishing: RowSetFinishing,
        project: Option<Vec<usize>>,
        filter: Vec<expr::ScalarExpr>,
    },
    /// Cancel the peek associated with the given `conn_id`.
    CancelPeek { conn_id: u32 },
    /// Insert `updates` into the local input named `name`.
    Insert { name: String, updates: Vec<Update> },
    /// Advance the timestamp for the local input named `name`.
    AdvanceTime { name: String, to: Timestamp },
    /// Enable compaction in views.
    ///
    /// Each entry in the vector names a view and provides a frontier after which
    /// accumulations must be correct. The workers gain the liberty of compacting
    /// the corresponding maintained traces up through that frontier.
    AllowCompaction(Vec<(String, Vec<Timestamp>)>),
    /// Append a new event to the log stream.
    AppendLog(MaterializedEvent),
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
    /// A list of names of traces, with prior and new upper frontiers.
    FrontierUppers(Vec<(String, ChangeBatch<Timestamp>)>),
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
    mut executor: impl tokio::executor::Executor + Clone + Send + Sync + 'static,
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
        executor
            .spawn(
                rx.shuttle()
                    .map_err(|err| panic!("failure shuttling dataflow receiver commands: {}", err))
                    .boxed(),
            )
            .map_err(|err| format!("error spawning future: {}", err))?;
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
        let command_rx = command_rxs.lock().unwrap()[timely_worker.index() % threads]
            .take()
            .unwrap()
            .request_unparks(executor.clone())
            .unwrap();

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
        }
        .run()
    })
}

struct Worker<'w, A>
where
    A: Allocate,
{
    inner: &'w mut TimelyWorker<A>,
    pending_peeks: Vec<PendingPeek>,
    traces: TraceManager,
    logging_config: Option<LoggingConfig>,
    feedback_tx: Option<Box<dyn Sink<SinkItem = WorkerFeedbackWithMeta, SinkError = ()>>>,
    command_rx: UnboundedReceiver<SequencedCommand>,
    materialized_logger: Option<logging::materialized::Logger>,
    sink_tokens: HashMap<String, Box<dyn Any>>,
    local_inputs: HashMap<String, LocalInput>,
    reported_frontiers: HashMap<String, Antichain<Timestamp>>,
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

            // Install traces as maintained views.
            for (log, (key, trace)) in t_traces {
                self.traces
                    .set_by_keys(log.name().to_string(), &key[..], WithDrop::from(trace));
                self.reported_frontiers
                    .insert(log.name().to_string(), Antichain::from_elem(0));
            }
            for (log, (key, trace)) in d_traces {
                self.traces
                    .set_by_keys(log.name().to_string(), &key[..], WithDrop::from(trace));
                self.reported_frontiers
                    .insert(log.name().to_string(), Antichain::from_elem(0));
            }
            for (log, (key, trace)) in m_traces {
                self.traces
                    .set_by_keys(log.name().to_string(), &key[..], WithDrop::from(trace));
                self.reported_frontiers
                    .insert(log.name().to_string(), Antichain::from_elem(0));
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

            // Handle any received commands.
            while let Ok(Some(cmd)) = self.command_rx.try_next() {
                if let SequencedCommand::Shutdown = cmd {
                    shutdown = true;
                }
                // Avoid non-determinism in post-shutdown commands.
                if !shutdown {
                    self.handle_command(cmd);
                }
            }

            if !shutdown {
                self.process_peeks();
            }
        }
    }

    /// Send progress information to the coordinator.
    fn report_frontiers(&mut self) {
        if let Some(feedback_tx) = &mut self.feedback_tx {
            let mut upper = Antichain::new();
            let mut progress = Vec::new();
            let names = self.traces.traces.keys().cloned().collect::<Vec<_>>();
            for name in names {
                if let Some(mut traces) = self.traces.get_all_keyed(&name) {
                    // Read the upper frontier and compare to what we've reported.
                    traces.ne  xt().unwrap().1.clone().read_upper(&mut upper);
                    let lower = self
                        .reported_frontiers
                        .get_mut(&name)
                        .expect("Frontier missing!");
                    let mut changes = ChangeBatch::new();
                    for time in lower.elements().iter() {
                        changes.update(time.clone(), -1);
                    }
                    for time in upper.elements().iter() {
                        changes.update(time.clone(), 1);
                    }
                    let lower = self.reported_frontiers.get_mut(&name).unwrap();
                    changes.compact();
                    progress.push((name, changes));
                    lower.clone_from(&upper);
                }
            }
            feedback_tx
                .send(WorkerFeedbackWithMeta {
                    worker_id: self.inner.index(),
                    message: WorkerFeedback::FrontierUppers(progress),
                })
                .wait()
                .unwrap();
        }
    }

    fn handle_command(&mut self, cmd: SequencedCommand) {
        match cmd {
            SequencedCommand::CreateDataflows(dataflows) => {
                for dataflow in dataflows.into_iter() {
                    if let Some(logger) = self.materialized_logger.as_mut() {
                        for view in dataflow.views.iter() {
                            if self.traces.traces.contains_key(&view.name) {
                                panic!("View already installed: {}", view.name);
                            }
                            logger.log(MaterializedEvent::Dataflow(view.name.to_string(), true));
                        }
                    }
                    for view in dataflow.views.iter() {
                        let prior = self
                            .reported_frontiers
                            .insert(view.name.to_string(), Antichain::from_elem(0));
                        assert!(prior == None);
                    }

                    render::build_dataflow(
                        dataflow,
                        &mut self.traces,
                        self.inner,
                        &mut self.sink_tokens,
                        &mut self.local_inputs,
                        &mut self.materialized_logger,
                    );
                }
            }

            SequencedCommand::DropSources(names) => {
                for name in names {
                    self.local_inputs.remove(&name);
                }
            }

            SequencedCommand::DropViews(names) => {
                for name in &names {
                    if self.traces.del_collection_traces(name).is_some() {
                        if let Some(logger) = self.materialized_logger.as_mut() {
                            logger.log(MaterializedEvent::Dataflow(name.to_string(), false));
                        }
                    }
                    self.reported_frontiers
                        .remove(name)
                        .expect("Dropped view with no frontier");
                }
            }

            SequencedCommand::DropSinks(names) => {
                for name in &names {
                    self.sink_tokens.remove(name);
                }
            }

            SequencedCommand::DropIndexes(trace_keys) => {
                for (collection_name, keys) in &trace_keys {
                    self.traces.del_user_trace(collection_name, keys);
                }
            }

            SequencedCommand::Peek {
                name,
                timestamp,
                conn_id,
                tx,
                finishing,
                project,
                filter,
            } => {
                // Acquire a copy of the trace suitable for fulfilling the peek.
                let mut trace = self.traces.get_default(&name).unwrap().clone();
                trace.advance_by(&[timestamp]);
                trace.distinguish_since(&[]);
                // Prepare a description of the peek work to do.
                let mut peek = PendingPeek {
                    name,
                    conn_id,
                    tx,
                    timestamp,
                    finishing,
                    trace,
                    project,
                    filter,
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
            }

            SequencedCommand::CancelPeek { conn_id } => {
                let logger = &mut self.materialized_logger;
                self.pending_peeks.retain(|peek| {
                    if peek.conn_id == conn_id {
                        peek.tx
                            .connect()
                            .wait()
                            .unwrap()
                            .send(PeekResponse::Canceled)
                            .wait()
                            .unwrap();

                        if let Some(logger) = logger {
                            logger.log(MaterializedEvent::Peek(peek.as_log_event(), false));
                        }

                        false // don't retain
                    } else {
                        true // retain
                    }
                })
            }

            SequencedCommand::Insert { name, updates } => {
                if let Some(input) = self.local_inputs.get_mut(&name) {
                    let mut session = input.handle.session(input.capability.clone());
                    for update in updates {
                        assert!(update.timestamp >= *input.capability.time());
                        session.give((update.row, update.timestamp, update.diff));
                    }
                }
            }

            SequencedCommand::AdvanceTime { name, to } => {
                if let Some(input) = self.local_inputs.get_mut(&name) {
                    input.capability.downgrade(&to);
                }
            }

            SequencedCommand::AllowCompaction(list) => {
                for (name, frontier) in list {
                    self.traces.allow_compaction(&name, &frontier[..]);
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
                    Some(Box::new(tx.connect().wait().unwrap().sink_map_err(|err| {
                        panic!("error sending worker feedback: {}", err)
                    })));
            }

            SequencedCommand::Shutdown => {
                // this should lead timely to wind down eventually
                self.traces.del_all_traces();
                self.shutdown_logging();
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
    /// The name of the dataflow to peek.
    name: String,
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
    /// The data from which the trace derives.
    trace: WithDrop<KeysValsHandle>,
}

impl PendingPeek {
    /// Produces a corresponding log event.
    pub fn as_log_event(&self) -> crate::logging::materialized::Peek {
        crate::logging::materialized::Peek::new(&self.name, self.timestamp, self.conn_id)
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
            let rows = self.collect_finished_data();

            self.tx
                .connect()
                .wait()
                .unwrap()
                .send(PeekResponse::Rows(rows))
                .wait()
                .unwrap();

            true
        } else {
            false
        }
    }

    /// Collects data for a known-complete peek.
    fn collect_finished_data(&mut self) -> Vec<Row> {
        let (mut cur, storage) = self.trace.cursor();
        let mut results = Vec::new();
        let mut unpacker = RowUnpacker::new();
        let mut left_unpacker = RowUnpacker::new();
        let mut right_unpacker = RowUnpacker::new();
        while let Some(_key) = cur.get_key(&storage) {
            while let Some(row) = cur.get_val(&storage) {
                let datums = unpacker.unpack(row);
                // Before (expensively) determining how many copies of a row
                // we have, let's eliminate rows that we don't care about.
                if self
                    .filter
                    .iter()
                    .all(|predicate| predicate.eval(&datums) == Datum::True)
                {
                    // Differential dataflow represents collections with binary counts,
                    // but our output representation is unary (as many rows as reported
                    // by the count). We should determine this count, and especially if
                    // it is non-zero, before producing any output data.
                    let mut copies = 0;
                    cur.map_times(&storage, |time, diff| {
                        use timely::order::PartialOrder;
                        if time.less_equal(&self.timestamp) {
                            copies += diff;
                        }
                    });
                    assert!(
                        copies >= 0,
                        "Negative multiplicity: {} for {:?} in view {}",
                        copies,
                        row.unpack(),
                        self.name
                    );

                    // TODO: We could push a count here, as we create owned output later.
                    for _ in 0..copies {
                        results.push(row);
                    }
                }
                cur.step_val(&storage);
            }
            cur.step_key(&storage)
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
                pdqselect::select_by(&mut results, offset_plus_limit, |left, right| {
                    compare_columns(
                        &self.finishing.order_by,
                        &left_unpacker.unpack(left.iter()),
                        &right_unpacker.unpack(right.iter()),
                    )
                });
                results.truncate(offset_plus_limit);
            }
        }

        if let Some(columns) = &self.project {
            results
                .iter()
                .map({
                    let mut unpacker = RowUnpacker::new();
                    let mut packer = RowPacker::new();
                    move |row| {
                        let datums = unpacker.unpack(*row);
                        packer.pack(columns.iter().map(|i| datums[*i]))
                    }
                })
                .collect()
        } else {
            results.iter().map(|row| (*row).clone()).collect()
        }
    }
}
