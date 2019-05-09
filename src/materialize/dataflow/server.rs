// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! An interactive dataflow server.

use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::TraceReader;

use timely::communication::initialize::WorkerGuards;
use timely::communication::Allocate;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::worker::Worker as TimelyWorker;

use std::sync::{Arc, Mutex};

use super::render;
use super::trace::{KeysOnlyHandle, TraceManager};
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
            .map(|r| Some(r))
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
    connection_uuid: uuid::Uuid,
    timestamp: Timestamp,
    trace: KeysOnlyHandle,
    probe: ProbeHandle<Timestamp>,
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
                self.handle_command(cmd, cmd_meta)
            }

            // Ask Timely to execute a unit of work.
            self.inner.step();

            // See if time has advanced enough to handle any of our pending
            // peeks.
            let Worker {
                pending_peeks,
                peek_results_handler,
                rpc_client,
                ..
            } = self;
            pending_peeks.retain(|peek| {
                if peek.probe.less_than(&peek.timestamp) {
                    return true; // retain
                }
                let (mut cur, storage) = peek.trace.clone().cursor();
                let mut out = Vec::new();
                while cur.key_valid(&storage) {
                    let key = cur.key(&storage).clone();
                    let mut copies = 0;
                    cur.map_times(&storage, |_, diff| {
                        copies += diff;
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
                            .header("X-Materialize-Query-UUID", peek.connection_uuid.to_string())
                            .body(encoded)
                            .send()
                            .unwrap();
                    }
                }
                false // don't retain
            });
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
            DataflowCommand::DropDataflow(name) => self.traces.del_trace(&name),
            DataflowCommand::PeekExisting(ref name) => {
                let (trace, probe) = self.traces.get_trace_and_probe(name.clone()).unwrap();
                self.pending_peeks.push(PendingPeek {
                    connection_uuid: cmd_meta.connection_uuid,
                    timestamp: cmd_meta.timestamp.unwrap(),
                    trace,
                    probe,
                });
            }
            DataflowCommand::PeekTransient(dataflow) => {
                let name = dataflow.name().to_string();
                self.handle_command(DataflowCommand::CreateDataflow(dataflow), cmd_meta.clone());
                self.handle_command(
                    DataflowCommand::PeekExisting(name.clone()),
                    cmd_meta.clone(),
                );
                self.handle_command(DataflowCommand::DropDataflow(name), cmd_meta);
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
        }
    }
}
