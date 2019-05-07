// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! An interactive dataflow server.

use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::TraceReader;
use std::collections::HashMap;

use timely::communication::initialize::WorkerGuards;
use timely::communication::Allocate;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::synchronization::Sequencer;
use timely::worker::Worker as TimelyWorker;

use super::render;
use super::trace::{KeysOnlyHandle, TraceManager};
use crate::clock::{Clock, Timestamp};
use crate::glue::*;
use ore::sync::Lottery;

pub fn serve(
    dataflow_command_receiver: std::sync::mpsc::Receiver<DataflowCommand>,
    peek_results_handler: PeekResultsHandler,
    clock: Clock,
    num_workers: usize,
) -> Result<WorkerGuards<()>, String> {
    let lottery = Lottery::new(dataflow_command_receiver, dummy_command_receiver);
    timely::execute(timely::Configuration::Process(num_workers), move |worker| {
        let dataflow_command_receiver = lottery.draw();
        Worker::new(
            worker,
            dataflow_command_receiver,
            peek_results_handler.clone(),
            clock.clone(),
        )
        .run()
    })
}

fn dummy_command_receiver() -> std::sync::mpsc::Receiver<DataflowCommand> {
    let (_tx, rx) = std::sync::mpsc::channel();
    rx
}

#[derive(Clone)]
pub enum PeekResultsHandler {
    Local(PeekResultsMux),
    Remote,
}

struct PendingPeek {
    id: uuid::Uuid,
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
    dataflow_command_receiver: std::sync::mpsc::Receiver<DataflowCommand>,
    peek_results_handler: PeekResultsHandler,
    sequencer: Sequencer<DataflowCommand>,
    pending_cmds: HashMap<String, Vec<DataflowCommand>>,
    pending_peeks: Vec<PendingPeek>,
    traces: TraceManager,
    rpc_client: reqwest::Client,
}

impl<'w, A> Worker<'w, A>
where
    A: Allocate,
{
    fn new(
        w: &'w mut TimelyWorker<A>,
        dataflow_command_receiver: std::sync::mpsc::Receiver<DataflowCommand>,
        peek_results_handler: PeekResultsHandler,
        clock: Clock,
    ) -> Worker<'w, A> {
        let sequencer = Sequencer::new(w, std::time::Instant::now());
        let mut traces = TraceManager::new();
        render::add_builtin_dataflows(&mut traces, w);
        Worker {
            inner: w,
            clock,
            dataflow_command_receiver,
            peek_results_handler,
            sequencer,
            pending_cmds: HashMap::new(),
            pending_peeks: Vec::new(),
            traces,
            rpc_client: reqwest::Client::new(),
        }
    }

    fn run(&mut self) {
        loop {
            // TOOD(jamii) would it be cheaper to replace the sequencer by just streaming `dataflow_command_receiver` into `num_workers` copies?

            // Submit any external commands for sequencing.
            while let Ok(cmd) = self.dataflow_command_receiver.try_recv() {
                self.sequencer.push(cmd)
            }

            // Handle any sequenced commands.
            while let Some(cmd) = self.sequencer.next() {
                self.handle_command(cmd)
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
                        if let Ok(sender) = peek_results_mux.read().unwrap().sender(&peek.id) {
                            drop(sender.unbounded_send(out))
                        }
                    }
                    PeekResultsHandler::Remote => {
                        let encoded = bincode::serialize(&out).unwrap();
                        rpc_client
                            .post("http://localhost:6875/api/peek-results")
                            .header("X-Materialize-Query-UUID", peek.id.to_string())
                            .body(encoded)
                            .send()
                            .unwrap();
                    }
                }
                false // don't retain
            });
        }
    }

    fn handle_command(&mut self, cmd: DataflowCommand) {
        match cmd {
            DataflowCommand::CreateDataflow(dataflow) => {
                render::build_dataflow(&dataflow, &mut self.traces, self.inner, &self.clock);
                if let Some(cmds) = self.pending_cmds.remove(dataflow.name()) {
                    for cmd in cmds {
                        self.handle_command(cmd);
                    }
                }
            }
            DataflowCommand::DropDataflow(name) => self.traces.del_trace(&name),
            DataflowCommand::Peek(ref name, id, timestamp) => {
                match self.traces.get_trace_and_probe(name.clone()) {
                    Some((trace, probe)) => self.pending_peeks.push(PendingPeek {
                        id,
                        timestamp,
                        trace,
                        probe,
                    }),
                    None => {
                        // We might see a Peek command before the corresponding
                        // CreateDataflow command. That's entirely expected.
                        // Just stash the Peek command so that it can be run
                        // after the dataflow is created.
                        self.pending_cmds
                            .entry(name.clone())
                            .or_insert_with(Vec::new)
                            .push(cmd);
                    }
                }
            }
            DataflowCommand::Tail(_) => unimplemented!(),
            DataflowCommand::Insert(_) => unimplemented!(),
        }
    }
}
