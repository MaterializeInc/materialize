// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! An interactive dataflow server.

use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::TraceReader;

use timely::communication::initialize::WorkerGuards;
use timely::communication::Allocate;
use timely::progress::frontier::Antichain;
use timely::synchronization::sequence::Sequencer;
use timely::worker::Worker as TimelyWorker;

use futures::sync::mpsc::UnboundedReceiver;
use ore::mpmc::Mux;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::mem;
use std::rc::Rc;
use std::sync::Mutex;
use std::time::Instant;
use uuid::Uuid;

use super::render;
use crate::arrangement::{manager::KeysOnlyHandle, TraceManager};
use crate::coordinator;
use crate::exfiltrate::{Exfiltrator, ExfiltratorConfig};
use crate::logging;
use crate::logging::materialized::MaterializedEvent;
use dataflow_types::logging::LoggingConfig;
use dataflow_types::{compare_columns, Dataflow, LocalInput, PeekWhen, RowSetFinishing, Timestamp};
use expr::RelationExpr;

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

/// Initiates a timely dataflow computation, processing materialized commands.
pub fn serve(
    dataflow_command_receiver: UnboundedReceiver<DataflowCommand>,
    local_input_mux: Mux<Uuid, LocalInput>,
    exfiltrator_config: ExfiltratorConfig,
    timely_configuration: timely::Configuration,
    logging_config: Option<dataflow_types::logging::LoggingConfig>,
) -> Result<WorkerGuards<()>, String> {
    let dataflow_command_receiver = Mutex::new(Some(dataflow_command_receiver));

    timely::execute(timely_configuration, move |worker| {
        let dataflow_command_receiver = if worker.index() == 0 {
            dataflow_command_receiver.lock().unwrap().take()
        } else {
            None
        };
        Worker::new(
            worker,
            dataflow_command_receiver,
            local_input_mux.clone(),
            exfiltrator_config.clone(),
            logging_config.clone(),
        )
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
    /// Whether to drop the dataflow when the peek completes.
    drop_after_peek: bool,
    transform: RowSetFinishing,
}

struct Worker<'w, A>
where
    A: Allocate,
{
    inner: &'w mut TimelyWorker<A>,
    local_input_mux: Mux<Uuid, LocalInput>,
    exfiltrator: Rc<Exfiltrator>,
    pending_peeks: Vec<(PendingPeek, KeysOnlyHandle)>,
    traces: TraceManager,
    names: HashMap<String, Box<dyn Drop>>,
    sequencer: Sequencer<coordinator::SequencedCommand>,
    logging_config: Option<LoggingConfig>,
    command_coordinator: Option<coordinator::CommandCoordinator>,
    materialized_logger: Option<logging::materialized::Logger>,
}

impl<'w, A> Worker<'w, A>
where
    A: Allocate,
{
    fn new(
        w: &'w mut TimelyWorker<A>,
        dataflow_command_receiver: Option<UnboundedReceiver<DataflowCommand>>,
        local_input_mux: Mux<Uuid, LocalInput>,
        exfiltrator_config: ExfiltratorConfig,
        logging_config: Option<LoggingConfig>,
    ) -> Worker<'w, A> {
        let sequencer = Sequencer::new(w, Instant::now());
        let exfiltrator = Rc::new(exfiltrator_config.into());
        let command_coordinator = dataflow_command_receiver
            .map(|dcr| coordinator::CommandCoordinator::new(dcr, Rc::clone(&exfiltrator)));

        Worker {
            inner: w,
            local_input_mux,
            exfiltrator,
            pending_peeks: Vec::new(),
            traces: TraceManager::default(),
            names: HashMap::new(),
            sequencer,
            logging_config,
            command_coordinator,
            materialized_logger: None,
        }
    }

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
                self.traces.set_by_self(log.name().to_string(), trace);
            }
            for (log, trace) in d_traces {
                self.traces.set_by_self(log.name().to_string(), trace);
            }
            for (log, trace) in m_traces {
                self.traces.set_by_self(log.name().to_string(), trace);
            }

            self.materialized_logger = self.inner.log_register().get("materialized");

            if let Some(coordinator) = &mut self.command_coordinator {
                coordinator.logger = self.inner.log_register().get("materialized");
                for log in logging.active_logs().iter() {
                    // Insert with 1 second compaction latency.
                    coordinator.insert_source(log.name(), 1_000);
                }
            }
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

            if let Some(coordinator) = &mut self.command_coordinator {
                // Sequence any pending commands.
                coordinator.sequence_commands(&mut self.sequencer);
                coordinator.maintenance(&mut self.sequencer);

                // Update upper bounds for each maintained trace.
                let mut upper = Antichain::new();
                for name in self.traces.traces.keys() {
                    if let Some(by_self) = self.traces.get_by_self(name) {
                        by_self.clone().read_upper(&mut upper);
                        coordinator.update_upper(name, upper.elements());
                    }
                }
            }

            // Handle any received commands
            while let Some(cmd) = self.sequencer.next() {
                if let coordinator::SequencedCommand::Shutdown = cmd {
                    shutdown = true;
                }
                self.handle_command(cmd);
            }

            if !shutdown {
                self.process_peeks();
            }
        }
    }

    fn handle_command(&mut self, cmd: coordinator::SequencedCommand) {
        match cmd {
            coordinator::SequencedCommand::CreateDataflows(dataflows) => {
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
                        &mut self.names,
                        &mut self.local_input_mux,
                        self.exfiltrator.clone(),
                    );
                }
            }

            coordinator::SequencedCommand::DropDataflows(dataflows) => {
                for name in &dataflows {
                    if let Some(logger) = self.materialized_logger.as_mut() {
                        logger.log(MaterializedEvent::Dataflow(name.to_string(), false));
                    }
                    self.names.remove(name);
                    self.traces.del_trace(name);
                }
            }

            coordinator::SequencedCommand::Peek {
                name,
                timestamp,
                conn_id,
                drop_after_peek,
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
                    drop_after_peek,
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

            coordinator::SequencedCommand::AllowCompaction(list) => {
                for (name, frontier) in list {
                    self.traces.allow_compaction(&name, &frontier[..]);
                }
            }

            coordinator::SequencedCommand::Shutdown => {
                // this should lead timely to wind down eventually
                self.names.clear();
                self.traces.del_all_traces();
                self.shutdown_logging();
            }
        }
    }

    /// Scan pending peeks and attempt to retire each.
    fn process_peeks(&mut self) {
        // See if time has advanced enough to handle any of our pending
        // peeks.
        let mut dataflows_to_be_dropped = vec![];
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
            if peek.drop_after_peek {
                dataflows_to_be_dropped.push(peek.name.clone());
            }
            false // don't retain
        });
        mem::replace(&mut self.pending_peeks, pending_peeks);
        if !dataflows_to_be_dropped.is_empty() {
            self.handle_command(coordinator::SequencedCommand::DropDataflows(
                dataflows_to_be_dropped,
            ));
        }
    }
}
