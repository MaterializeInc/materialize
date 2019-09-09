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
use timely::progress::frontier::Antichain;
use timely::worker::Worker as TimelyWorker;

use futures::sync::mpsc::UnboundedReceiver;
use futures::{Future, Sink};
use ore::future::sync::mpsc::ReceiverExt;
use ore::future::FutureExt;
use ore::mpmc::Mux;
use serde::{Deserialize, Serialize};
use std::mem;
use std::net::TcpStream;
use std::rc::Rc;
use std::sync::Mutex;
use uuid::Uuid;

use super::render;
use crate::arrangement::{
    manager::{KeysOnlyHandle, WithDrop},
    TraceManager,
};
use crate::exfiltrate::{Exfiltrator, ExfiltratorConfig};
use crate::logging;
use crate::logging::materialized::MaterializedEvent;
use dataflow_types::logging::LoggingConfig;
use dataflow_types::{compare_columns, Dataflow, LocalInput, PeekWhen, RowSetFinishing, Timestamp};
use expr::RelationExpr;

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
    fn loopback() -> bool {
        true
    }
}

/// The commands that a running dataflow server can accept.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataflowCommand {
    CreateDataflows(Vec<Dataflow>),
    DropDataflows(Vec<String>),
    Peek {
        conn_id: u32,
        source: RelationExpr,
        when: PeekWhen,
        transform: RowSetFinishing,
    },
    Explain {
        conn_id: u32,
        relation_expr: RelationExpr,
    },
    Shutdown,
}

/// Explicit instructions for timely dataflow workers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SequencedCommand {
    /// Create a sequence of dataflows.
    CreateDataflows(Vec<Dataflow>),
    /// Drop the dataflows bound to these names.
    DropDataflows(Vec<String>),
    /// Peek at a materialized view.
    Peek {
        name: String,
        timestamp: Timestamp,
        conn_id: u32,
        transform: RowSetFinishing,
    },
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
    FrontierUppers(Vec<(String, Vec<Timestamp>)>),
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
    local_input_mux: Mux<Uuid, LocalInput>,
    exfiltrator_config: ExfiltratorConfig,
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
        let mut rx = switchboard.broadcast_rx::<BroadcastToken>().fanout();
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
            local_input_mux: local_input_mux.clone(),
            exfiltrator: Rc::new(exfiltrator_config.clone().into()),
            pending_peeks: Vec::new(),
            traces: TraceManager::default(),
            logging_config: logging_config.clone(),
            feedback_tx: None,
            command_rx,
            materialized_logger: None,
        }
        .run()
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PendingPeek {
    /// The name of the dataflow to peek.
    name: String,
    /// Identifies intended recipient of the peek.
    conn_id: u32,
    /// Time at which the collection should be materialized.
    timestamp: Timestamp,
    transform: RowSetFinishing,
}

struct Worker<'w, A>
where
    A: Allocate,
{
    inner: &'w mut TimelyWorker<A>,
    local_input_mux: Mux<Uuid, LocalInput>,
    exfiltrator: Rc<Exfiltrator>,
    pending_peeks: Vec<(PendingPeek, WithDrop<KeysOnlyHandle>)>,
    traces: TraceManager,
    logging_config: Option<LoggingConfig>,
    feedback_tx: Option<Box<dyn Sink<SinkItem = WorkerFeedbackWithMeta, SinkError = ()>>>,
    command_rx: UnboundedReceiver<SequencedCommand>,
    materialized_logger: Option<logging::materialized::Logger>,
}

impl<'w, A> Worker<'w, A>
where
    A: Allocate,
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

            // Establish loggers first, so we can either log the logging or not, as we like.
            let t_linked = std::rc::Rc::new(EventLink::new());
            let mut t_logger = BatchLogger::new(t_linked.clone());
            let d_linked = std::rc::Rc::new(EventLink::new());
            let mut d_logger = BatchLogger::new(d_linked.clone());
            let m_linked = std::rc::Rc::new(EventLink::new());
            let mut m_logger = BatchLogger::new(m_linked.clone());

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
            for (log, trace) in t_traces {
                self.traces
                    .set_by_self(log.name().to_string(), WithDrop::from(trace));
            }
            for (log, trace) in d_traces {
                self.traces
                    .set_by_self(log.name().to_string(), WithDrop::from(trace));
            }
            for (log, trace) in m_traces {
                self.traces
                    .set_by_self(log.name().to_string(), WithDrop::from(trace));
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

            // Send progress information to the coordinator.
            if let Some(feedback_tx) = &mut self.feedback_tx {
                let mut upper = Antichain::new();
                let mut progress = Vec::new();
                for name in self.traces.traces.keys() {
                    if let Some(by_self) = self.traces.get_by_self(name) {
                        by_self.clone().read_upper(&mut upper);
                        progress.push((name.to_owned(), upper.elements().to_vec()));
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

            // Handle any received commands.
            while let Ok(Some(cmd)) = self.command_rx.try_next() {
                if let SequencedCommand::Shutdown = cmd {
                    shutdown = true;
                }
                self.handle_command(cmd);
            }

            if !shutdown {
                self.process_peeks();
            }
        }
    }

    fn handle_command(&mut self, cmd: SequencedCommand) {
        match cmd {
            SequencedCommand::CreateDataflows(dataflows) => {
                for dataflow in dataflows.into_iter() {
                    if let Some(logger) = self.materialized_logger.as_mut() {
                        logger.log(MaterializedEvent::Dataflow(
                            dataflow.name().to_string(),
                            true,
                        ));
                    }
                    render::build_dataflow(
                        dataflow,
                        &mut self.traces,
                        self.inner,
                        &mut self.local_input_mux,
                        self.exfiltrator.clone(),
                        &mut self.materialized_logger,
                    );
                }
            }

            SequencedCommand::DropDataflows(dataflows) => {
                for name in &dataflows {
                    if let Some(logger) = self.materialized_logger.as_mut() {
                        logger.log(MaterializedEvent::Dataflow(name.to_string(), false));
                    }
                    self.traces.del_trace(name);
                }
            }

            SequencedCommand::Peek {
                name,
                timestamp,
                conn_id,
                transform,
            } => {
                let mut trace = self
                    .traces
                    .get_by_self(&name)
                    .expect("Failed to find trace for peek")
                    .clone();
                trace.advance_by(&[timestamp]);
                trace.distinguish_since(&[]);
                let pending_peek = PendingPeek {
                    name,
                    conn_id,
                    timestamp,
                    transform,
                };
                if let Some(logger) = self.materialized_logger.as_mut() {
                    logger.log(MaterializedEvent::Peek(
                        crate::logging::materialized::Peek::new(
                            &pending_peek.name,
                            pending_peek.timestamp,
                            pending_peek.conn_id,
                        ),
                        true,
                    ));
                }
                self.pending_peeks.push((pending_peek, trace));
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
        // See if time has advanced enough to handle any of our pending
        // peeks.
        let mut pending_peeks = mem::replace(&mut self.pending_peeks, Vec::new());
        pending_peeks.retain(|(peek, trace)| {
            let mut upper = timely::progress::frontier::Antichain::new();
            let mut trace = trace.clone();
            trace.read_upper(&mut upper);

            // To produce output at `peek.timestamp`, we must be certain that
            // it is no longer changing. A trace guarantees that all future
            // changes will be greater than or equal to an element of `upper`.
            //
            // If an element of `upper` is less or equal to `peek.timestamp`,
            // then there can be further updates that would change the output.
            // If no element of `upper` is less or equal to `peek.timestamp`,
            // then for any time `t` less or equal to `peek.timestamp` it is
            // not the case that `upper` is less or equal to that timestamp,
            // and so the result cannot further evolve.
            if upper.less_equal(&peek.timestamp) {
                return true; // retain
            }
            let (mut cur, storage) = trace.cursor();
            let mut results = Vec::new();
            while let Some(key) = cur.get_key(&storage) {
                // TODO: Absent value iteration might be weird (in principle
                // the cursor *could* say no `()` values associated with the
                // key, though I can't imagine how that would happen for this
                // specific trace implementation).

                let mut copies = 0;
                cur.map_times(&storage, |time, diff| {
                    use timely::order::PartialOrder;
                    if time.less_equal(&peek.timestamp) {
                        copies += diff;
                    }
                });
                assert!(
                    copies >= 0,
                    "Negative multiplicity: {} for {:?} in view {}",
                    copies,
                    key,
                    peek.name
                );
                for _ in 0..copies {
                    results.push(key.clone());
                }
                cur.step_key(&storage)
            }
            if let Some(limit) = peek.transform.limit {
                if results.len() > limit {
                    pdqselect::select_by(&mut results, limit, |left, right| {
                        compare_columns(&peek.transform.order_by, left, right)
                    });
                    results.truncate(limit);
                }
            }
            self.exfiltrator.send_peek(peek.conn_id, results);
            if let Some(logger) = self.materialized_logger.as_mut() {
                logger.log(MaterializedEvent::Peek(
                    crate::logging::materialized::Peek::new(
                        &peek.name,
                        peek.timestamp,
                        peek.conn_id,
                    ),
                    false,
                ));
            }
            false // don't retain
        });
        mem::replace(&mut self.pending_peeks, pending_peeks);
    }
}
