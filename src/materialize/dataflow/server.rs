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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::mem;
use std::sync::Mutex;
use std::time::Instant;

use super::render;
use super::render::InputCapability;
use crate::dataflow::arrangement::{manager::KeysOnlyHandle, TraceManager};
use crate::dataflow::Timestamp;
use crate::glue::*;

use crate::dataflow::coordinator;
use crate::dataflow::logging;
use crate::dataflow::logging::materialized::MaterializedEvent;

/// Initiates a timely dataflow computation, processing materialized commands.
pub fn serve(
    dataflow_command_receiver: UnboundedReceiver<(DataflowCommand, CommandMeta)>,
    local_input_mux: LocalInputMux,
    dataflow_results_handler: DataflowResultsHandler,
    timely_configuration: timely::Configuration,
    logging_config: Option<logging::LoggingConfiguration>,
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
            dataflow_results_handler.clone(),
            logging_config.clone(),
        )
        .run()
    })
}

/// Options for how dataflow results return to those that posed the queries.
#[derive(Clone)]
pub enum DataflowResultsHandler {
    /// A local exchange fabric.
    Local(DataflowResultsMux),
    /// An address to post results at.
    Remote(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PendingPeek {
    /// The name of the dataflow to peek.
    name: String,
    /// Identifies intended recipient of the peek.
    connection_uuid: uuid::Uuid,
    /// Time at which the collection should be materialized.
    timestamp: Timestamp,
    /// Whether to drop the dataflow when the peek completes.
    drop_after_peek: bool,
}

struct Worker<'w, A>
where
    A: Allocate,
{
    inner: &'w mut TimelyWorker<A>,
    local_input_mux: LocalInputMux,
    dataflow_results_handler: DataflowResultsHandler,
    pending_peeks: Vec<(PendingPeek, KeysOnlyHandle)>,
    traces: TraceManager,
    rpc_client: reqwest::Client,
    inputs: HashMap<String, InputCapability>,
    sequencer: Sequencer<(coordinator::SequencedCommand, CommandMeta)>,
    logging_config: Option<logging::LoggingConfiguration>,
    command_coordinator: Option<coordinator::CommandCoordinator>,
    materialized_logger: Option<logging::materialized::Logger>,
}

impl<'w, A> Worker<'w, A>
where
    A: Allocate,
{
    fn new(
        w: &'w mut TimelyWorker<A>,
        dataflow_command_receiver: Option<UnboundedReceiver<(DataflowCommand, CommandMeta)>>,
        local_input_mux: LocalInputMux,
        dataflow_results_handler: DataflowResultsHandler,
        logging_config: Option<logging::LoggingConfiguration>,
    ) -> Worker<'w, A> {
        let sequencer = Sequencer::new(w, Instant::now());
        let command_coordinator =
            dataflow_command_receiver.map(|dcr| coordinator::CommandCoordinator::new(dcr));

        Worker {
            inner: w,
            local_input_mux,
            dataflow_results_handler,
            pending_peeks: Vec::new(),
            traces: TraceManager::default(),
            rpc_client: reqwest::Client::new(),
            inputs: HashMap::new(),
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
            use crate::dataflow::logging::BatchLogger;
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
                self.traces.set_by_self(log.name().to_string(), trace, None);
            }
            for (log, trace) in d_traces {
                self.traces.set_by_self(log.name().to_string(), trace, None);
            }
            for (log, trace) in m_traces {
                self.traces.set_by_self(log.name().to_string(), trace, None);
            }

            self.materialized_logger = self.inner.log_register().get("materialized");

            if let Some(coordinator) = &mut self.command_coordinator {
                coordinator.logger = self.inner.log_register().get("materialized");
                for log in logging.active_logs.iter() {
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

            // Ask Timely to execute a unit of work.
            // Can either yield tastefully, or busy-wait.
            // self.inner.step_or_park(None);
            self.inner.step();

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
            while let Some((cmd, cmd_meta)) = self.sequencer.next() {
                if let coordinator::SequencedCommand::Shutdown = cmd {
                    shutdown = true;
                }
                self.handle_command(cmd, cmd_meta);
            }

            if !shutdown {
                self.process_peeks();
            }
        }
    }

    fn handle_command(&mut self, cmd: coordinator::SequencedCommand, cmd_meta: CommandMeta) {
        match cmd {
            coordinator::SequencedCommand::CreateDataflows(dataflows) => {
                for dataflow in dataflows.iter() {
                    if let Some(logger) = self.materialized_logger.as_mut() {
                        logger.log(MaterializedEvent::Dataflow(
                            dataflow.name().to_string(),
                            true,
                        ));
                    }
                    render::build_dataflow(
                        &dataflow,
                        &mut self.traces,
                        self.inner,
                        &mut self.inputs,
                        &mut self.local_input_mux,
                    );
                }
            }

            coordinator::SequencedCommand::DropDataflows(dataflows) => {
                for name in &dataflows {
                    if let Some(logger) = self.materialized_logger.as_mut() {
                        logger.log(MaterializedEvent::Dataflow(name.to_string(), false));
                    }
                    self.inputs.remove(name);
                    self.traces.del_trace(name);
                }
            }

            coordinator::SequencedCommand::Peek {
                name,
                timestamp,
                drop_after_peek,
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
                    connection_uuid: cmd_meta.connection_uuid,
                    timestamp,
                    drop_after_peek,
                };
                if let Some(logger) = self.materialized_logger.as_mut() {
                    logger.log(MaterializedEvent::Peek(
                        crate::dataflow::logging::materialized::Peek::new(
                            &pending_peek.name,
                            pending_peek.timestamp,
                            &pending_peek.connection_uuid,
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

            coordinator::SequencedCommand::Tail(_) => unimplemented!(),

            coordinator::SequencedCommand::Shutdown => {
                // this should lead timely to wind down eventually
                self.inputs.clear();
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
            let result = DataflowResults::Peeked(results);
            match &self.dataflow_results_handler {
                DataflowResultsHandler::Local(peek_results_mux) => {
                    // The sender is allowed disappear at any time, so the
                    // error handling here is deliberately relaxed.
                    if let Ok(sender) = peek_results_mux
                        .read()
                        .unwrap()
                        .sender(&peek.connection_uuid)
                    {
                        drop(sender.unbounded_send(result))
                    }
                }
                DataflowResultsHandler::Remote(response_address) => {
                    let encoded = bincode::serialize(&result).unwrap();
                    self.rpc_client
                        .post(response_address)
                        .header("X-Materialize-Query-UUID", peek.connection_uuid.to_string())
                        .body(encoded)
                        .send()
                        .unwrap();
                }
            }
            if let Some(logger) = self.materialized_logger.as_mut() {
                logger.log(MaterializedEvent::Peek(
                    crate::dataflow::logging::materialized::Peek::new(
                        &peek.name,
                        peek.timestamp,
                        &peek.connection_uuid,
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
            self.handle_command(
                coordinator::SequencedCommand::DropDataflows(dataflows_to_be_dropped),
                CommandMeta {
                    connection_uuid: Uuid::nil(),
                },
            );
        }
    }
}
