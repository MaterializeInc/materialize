// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! An interactive dataflow server.

use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::TraceReader;

use timely::communication::initialize::WorkerGuards;
use timely::communication::Allocate;
// use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::worker::Worker as TimelyWorker;

use std::sync::{Arc, Mutex};

use super::render;
use super::trace::{KeysOnlyHandle, TraceManager};
use super::types;
use crate::clock::{Clock, Timestamp};
use crate::dataflow::source;
use crate::glue::*;

pub fn serve(
    dataflow_command_receivers: Vec<UnboundedReceiver<(DataflowCommand, CommandMeta)>>,
    peek_results_handler: PeekResultsHandler,
    clock: Clock,
    num_workers: usize,
) -> Result<WorkerGuards<()>, String> {
    assert_eq!(dataflow_command_receivers.len(), num_workers);
    // wrap up receivers so individual workers can take them
    let dataflow_command_receivers = Arc::new(Mutex::new(
        dataflow_command_receivers
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>(),
    ));
    let insert_mux = source::InsertMux::default();
    timely::execute(timely::Configuration::Process(num_workers), move |worker| {
        let dataflow_command_receivers = dataflow_command_receivers.clone();
        let dataflow_command_receiver = {
            dataflow_command_receivers.lock().unwrap()[worker.index()]
                .take()
                .unwrap()
        };
        Worker::new(
            worker,
            dataflow_command_receiver,
            peek_results_handler.clone(),
            clock.clone(),
            insert_mux.clone(),
        )
        .run()
    })
}

#[derive(Clone)]
pub enum PeekResultsHandler {
    Local(PeekResultsMux),
    Remote,
}

struct PendingPeek {
    /// Identifies intended recipient of the peek.
    connection_uuid: uuid::Uuid,
    /// Time at which the collection should be materialized.
    timestamp: Timestamp,
    /// Handle to trace.
    trace: KeysOnlyHandle,
    drop_after_peek: Option<String>,
}

struct Worker<'w, A>
where
    A: Allocate,
{
    inner: &'w mut TimelyWorker<A>,
    clock: Clock,
    dataflow_command_receiver: UnboundedReceiver<(DataflowCommand, CommandMeta)>,
    peek_results_handler: PeekResultsHandler,
    pending_peeks: Vec<PendingPeek>,
    traces: TraceManager,
    rpc_client: reqwest::Client,
    insert_mux: source::InsertMux,
}

impl<'w, A> Worker<'w, A>
where
    A: Allocate,
{
    fn new(
        w: &'w mut TimelyWorker<A>,
        dataflow_command_receiver: UnboundedReceiver<(DataflowCommand, CommandMeta)>,
        peek_results_handler: PeekResultsHandler,
        clock: Clock,
        insert_mux: source::InsertMux,
    ) -> Worker<'w, A> {
        let mut traces = TraceManager::new();
        render::add_builtin_dataflows(&mut traces, w);
        Worker {
            inner: w,
            clock,
            dataflow_command_receiver,
            peek_results_handler,
            pending_peeks: Vec::new(),
            traces,
            rpc_client: reqwest::Client::new(),
            insert_mux,
        }
    }

    fn run(&mut self) {
        loop {
            // Handle any received commands
            while let Ok(Some((cmd, cmd_meta))) = self.dataflow_command_receiver.try_next() {
                let is_shutdown = if let DataflowCommand::Shutdown = cmd {
                    true
                } else {
                    false
                };
                self.handle_command(cmd, cmd_meta);
                if is_shutdown {
                    return;
                }
            }

            // Ask Timely to execute a unit of work.
            self.inner.step();

            // See if time has advanced enough to handle any of our pending
            // peeks.
            let mut dataflows_to_be_dropped = vec![];
            {
                let Worker {
                    pending_peeks,
                    peek_results_handler,
                    rpc_client,
                    ..
                } = self;
                pending_peeks.retain(|peek| {
                    let mut upper = timely::progress::frontier::Antichain::new();
                    let mut trace = peek.trace.clone();
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
                    if !upper.less_equal(&peek.timestamp) {
                        let (mut cur, storage) = peek.trace.clone().cursor();
                        let mut out = Vec::new();
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
                                out.push(key.clone());
                            }

                            cur.step_key(&storage)
                        }
                        match peek_results_handler {
                            PeekResultsHandler::Local(peek_results_mux) => {
                                // the sender is allowed disappear at any time, so the error handling here is deliberately relaxed
                                if let Ok(sender) = peek_results_mux
                                    .read()
                                    .unwrap()
                                    .sender(&peek.connection_uuid)
                                {
                                    drop(sender.unbounded_send(out))
                                }
                            }
                            PeekResultsHandler::Remote => {
                                let encoded = bincode::serialize(&out).unwrap();
                                rpc_client
                                    .post("http://localhost:6875/api/peek-results")
                                    .header(
                                        "X-Materialize-Query-UUID",
                                        peek.connection_uuid.to_string(),
                                    )
                                    .body(encoded)
                                    .send()
                                    .unwrap();
                            }
                        }
                        if let Some(name) = &peek.drop_after_peek {
                            dataflows_to_be_dropped.push(name.to_owned());
                        }
                        false // don't retain
                    } else {
                        true
                    }
                });
            }
            self.handle_command(
                DataflowCommand::DropDataflows(dataflows_to_be_dropped),
                CommandMeta {
                    connection_uuid: Uuid::nil(),
                    timestamp: Some(self.clock.now()),
                },
            );
        }
    }

    fn handle_command(&mut self, cmd: DataflowCommand, cmd_meta: CommandMeta) {
        match cmd {
            DataflowCommand::CreateDataflow(dataflow) => {
                render::build_dataflow(
                    &dataflow,
                    &mut self.traces,
                    self.inner,
                    &self.clock,
                    &self.insert_mux,
                );
            }
            DataflowCommand::DropDataflows(names) => {
                let mut insert_mux = self.insert_mux.write().unwrap();
                for name in names {
                    insert_mux.close(&name);
                    let plan = types::Plan::Source(name.to_string());
                    self.traces.del_trace(&plan);
                }
            }
            DataflowCommand::PeekExisting(ref name) => {
                let plan = types::Plan::Source(name.to_string());
                if let Some(trace) = self.traces.get_trace(&plan) {
                    self.pending_peeks.push(PendingPeek {
                        connection_uuid: cmd_meta.connection_uuid,
                        timestamp: cmd_meta.timestamp.unwrap(),
                        trace,
                        drop_after_peek: None,
                    });
                } else {
                    panic!(format!("Failed to find arrangement for Peek({})", name));
                }
            }
            DataflowCommand::PeekTransient(dataflow) => {
                let name = dataflow.name().to_string();
                render::build_dataflow(
                    &dataflow,
                    &mut self.traces,
                    self.inner,
                    &self.clock,
                    &self.insert_mux,
                );
                let plan = types::Plan::Source(name.to_string());
                if let Some(trace) = self.traces.get_trace(&plan) {
                    self.pending_peeks.push(PendingPeek {
                        connection_uuid: cmd_meta.connection_uuid,
                        timestamp: cmd_meta.timestamp.unwrap(),
                        trace,
                        drop_after_peek: Some(name),
                    });
                } else {
                    panic!(format!("Failed to find arrangement for Peek({})", name));
                }
            }
            DataflowCommand::Insert(name, datums) => {
                // Only the first worker actually broadcasts the insert to the sources, otherwise we would get multiple copies
                if self.inner.index() == 0 {
                    self.insert_mux
                        .read()
                        .unwrap()
                        .sender(&name)
                        .unwrap()
                        .unbounded_send(datums)
                        .unwrap();
                }
            }
            DataflowCommand::Tail(_) => unimplemented!(),
            DataflowCommand::Shutdown => {
                // this should lead timely to wind down eventually
                self.traces.del_all_traces();
            }
        }
    }
}
