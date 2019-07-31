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
use super::RelationExpr;
use crate::dataflow::arrangement::{manager::KeysOnlyHandle, TraceManager};
use crate::dataflow::{Dataflow, Timestamp, View};
use crate::glue::*;

pub fn serve(
    dataflow_command_receiver: UnboundedReceiver<(DataflowCommand, CommandMeta)>,
    local_input_mux: LocalInputMux,
    dataflow_results_handler: DataflowResultsHandler,
    timely_configuration: timely::Configuration,
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

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Serialize, Deserialize)]
enum SequencedCommand {
    CreateDataflow(Dataflow),
    DropDataflows(Vec<String>),
    Peek {
        name: String,
        timestamp: Timestamp,
        drop_after_peek: bool,
    },
    Tail(String),
    Shutdown,
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
    dataflow_command_receiver: Option<UnboundedReceiver<(DataflowCommand, CommandMeta)>>,
    local_input_mux: LocalInputMux,
    dataflow_results_handler: DataflowResultsHandler,
    pending_peeks: Vec<(PendingPeek, KeysOnlyHandle)>,
    traces: TraceManager,
    rpc_client: reqwest::Client,
    inputs: HashMap<String, InputCapability>,
    dataflows: HashMap<String, Dataflow>,
    sequencer: Sequencer<(SequencedCommand, CommandMeta)>,
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
    ) -> Worker<'w, A> {
        let sequencer = Sequencer::new(w, Instant::now());
        Worker {
            inner: w,
            dataflow_command_receiver,
            local_input_mux,
            dataflow_results_handler,
            pending_peeks: Vec::new(),
            traces: TraceManager::default(),
            rpc_client: reqwest::Client::new(),
            inputs: HashMap::new(),
            dataflows: HashMap::new(),
            sequencer,
        }
    }

    /// Draws from `dataflow_command_receiver` until shutdown.
    fn run(&mut self) {
        let mut shutdown = false;
        while !shutdown {
            // Enable trace compaction.
            self.traces.maintenance();

            // Ask Timely to execute a unit of work.
            // Can either yield tastefully, or busy-wait.
            // self.inner.step_or_park(None);
            self.inner.step();

            if self.dataflow_command_receiver.is_some() {
                while let Ok(Some((cmd, cmd_meta))) =
                    self.dataflow_command_receiver.as_mut().unwrap().try_next()
                {
                    self.sequence_command(cmd, cmd_meta)
                }
            }

            // Handle any received commands
            while let Some((cmd, cmd_meta)) = self.sequencer.next() {
                if let SequencedCommand::Shutdown = cmd {
                    shutdown = true;
                }
                self.handle_command(cmd, cmd_meta);
            }

            if !shutdown {
                self.process_peeks();
            }
        }
    }

    fn sequence_command(&mut self, cmd: DataflowCommand, cmd_meta: CommandMeta) {
        let sequenced_cmd = match cmd {
            DataflowCommand::CreateDataflow(dataflow) => SequencedCommand::CreateDataflow(dataflow),
            DataflowCommand::DropDataflows(dataflows) => SequencedCommand::DropDataflows(dataflows),
            DataflowCommand::Tail(name) => SequencedCommand::Tail(name),
            DataflowCommand::Peek { source, when } => {
                let (name, drop) = if let RelationExpr::Get { name, typ: _ } = source {
                    // Fast path. We can just look at the existing dataflow
                    // directly.
                    (name, false)
                } else {
                    // Slow path. We need to perform some computation, so build
                    // a new transient dataflow that will be dropped after the
                    // peek completes.
                    let name = format!("<temp_{}>", Uuid::new_v4());
                    let typ = source.typ();
                    self.sequencer.push((
                        SequencedCommand::CreateDataflow(Dataflow::View(View {
                            name: name.clone(),
                            relation_expr: source,
                            typ,
                        })),
                        CommandMeta::nil(),
                    ));
                    (name, true)
                };

                let timestamp = match when {
                    PeekWhen::Immediately => {
                        match self.traces.get_by_self(&name) {
                            Some(trace) => {
                                // Ask the trace for the latest time it knows about. The latest
                                // "committed" time (i.e., the time at which we know results can
                                // never change) is one less than that.
                                //
                                // TODO(benesch, fms): this approach does not work for arbitrary
                                // timestamp types, where a predecessor operation may not exist.
                                //
                                // TODO(benesch): this is perhaps not the correct means of
                                // determining a timestamp that is "immediately" available, as this
                                // only considers the view from worker 0. The other workers may be
                                // farther behind. A better approach would involve using a probe,
                                // which considers the view from all workers. However, this isn't
                                // much of a big deal in the meantime, since if this worker has
                                // progressed to time T, the other workers are guaranteed to
                                // progress to T "very soon."
                                let mut upper = Antichain::new();
                                trace.clone().read_upper(&mut upper);
                                if upper.elements().is_empty() {
                                    Timestamp::max_value()
                                } else {
                                    assert_eq!(upper.elements().len(), 1);
                                    upper.elements()[0].saturating_sub(1)
                                }
                            }
                            None => 0,
                        }
                    }
                    PeekWhen::AtTimestamp(timestamp) => timestamp,
                };
                SequencedCommand::Peek {
                    name,
                    timestamp,
                    drop_after_peek: drop,
                }
            }
            DataflowCommand::Shutdown => SequencedCommand::Shutdown,
        };
        self.sequencer.push((sequenced_cmd, cmd_meta));
    }

    fn handle_command(&mut self, cmd: SequencedCommand, cmd_meta: CommandMeta) {
        match cmd {
            SequencedCommand::CreateDataflow(dataflow) => {
                render::build_dataflow(
                    &dataflow,
                    &mut self.traces,
                    self.inner,
                    &mut self.inputs,
                    &mut self.local_input_mux,
                );
                self.dataflows.insert(dataflow.name().to_owned(), dataflow);
            }

            SequencedCommand::DropDataflows(dataflows) => {
                for name in &dataflows {
                    self.inputs.remove(name);
                    self.dataflows.remove(name);
                    self.traces.del_trace(name);
                }
            }

            SequencedCommand::Peek {
                name,
                timestamp,
                drop_after_peek,
            } => {
                let trace = self.traces.get_by_self(&name).unwrap();
                let pending_peek = PendingPeek {
                    name,
                    connection_uuid: cmd_meta.connection_uuid,
                    timestamp,
                    drop_after_peek,
                };
                self.pending_peeks.push((pending_peek, trace));
            }

            SequencedCommand::Tail(_) => unimplemented!(),

            SequencedCommand::Shutdown => {
                // this should lead timely to wind down eventually
                self.inputs.clear();
                self.traces.del_all_traces();
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
                assert!(copies >= 0);
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
            if peek.drop_after_peek {
                dataflows_to_be_dropped.push(peek.name.clone());
            }
            false // don't retain
        });
        mem::replace(&mut self.pending_peeks, pending_peeks);
        if !dataflows_to_be_dropped.is_empty() {
            self.handle_command(
                SequencedCommand::DropDataflows(dataflows_to_be_dropped),
                CommandMeta {
                    connection_uuid: Uuid::nil(),
                },
            );
        }
    }
}
