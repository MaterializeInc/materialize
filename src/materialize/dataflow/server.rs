// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! An interactive dataflow server.

use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::TraceReader;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::mpsc;
use timely::communication::initialize::WorkerGuards;
use timely::communication::Allocate;
use timely::synchronization::Sequencer;
use timely::worker::Worker as TimelyWorker;

use super::render;
use super::trace::TraceManager;
use super::types::Dataflow;
use ore::sync::Lottery;

pub fn serve(cmd_rx: CommandReceiver) -> Result<WorkerGuards<()>, String> {
    let lottery = Lottery::new(cmd_rx, dummy_command_receiver);
    timely::execute(timely::Configuration::Process(4), move |worker| {
        let cmd_rx = lottery.draw();
        Worker::new(worker, cmd_rx).run()
    })
}

/// The commands that a running dataflow server can accept.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    CreateDataflow(Dataflow),
    DropDataflow(String),
    Peek(String, uuid::Uuid),
    Tail(String),
}

pub type CommandSender = mpsc::Sender<Command>;
pub type CommandReceiver = mpsc::Receiver<Command>;

fn dummy_command_receiver() -> CommandReceiver {
    let (_tx, rx) = mpsc::channel();
    rx
}

struct Worker<'w, A>
where
    A: Allocate,
{
    inner: &'w mut TimelyWorker<A>,
    cmd_rx: CommandReceiver,
    sequencer: Sequencer<Command>,
    pending_cmds: HashMap<String, Vec<Command>>,
    traces: TraceManager,
    rpc_client: reqwest::Client,
}

impl<'w, A> Worker<'w, A>
where
    A: Allocate,
{
    fn new(w: &'w mut TimelyWorker<A>, cmd_rx: CommandReceiver) -> Worker<'w, A> {
        let sequencer = Sequencer::new(w, std::time::Instant::now());
        let mut traces = TraceManager::new();
        render::add_builtin_dataflows(&mut traces, w);
        Worker {
            inner: w,
            cmd_rx,
            sequencer,
            pending_cmds: HashMap::new(),
            traces,
            rpc_client: reqwest::Client::new(),
        }
    }

    fn run(&mut self) {
        loop {
            // Submit any external commands for sequencing.
            while let Ok(cmd) = self.cmd_rx.try_recv() {
                self.sequencer.push(cmd)
            }

            // Handle any sequenced commands.
            while let Some(cmd) = self.sequencer.next() {
                self.handle_command(cmd)
            }

            // Ask Timely to execute a unit of work.
            self.inner.step();
        }
    }

    fn handle_command(&mut self, cmd: Command) {
        match &cmd {
            Command::CreateDataflow(dataflow) => {
                render::build_dataflow(dataflow, &mut self.traces, self.inner);
                if let Some(cmds) = self.pending_cmds.remove(dataflow.name()) {
                    for cmd in cmds {
                        self.handle_command(cmd);
                    }
                }
            }
            Command::DropDataflow(name) => self.traces.del_trace(name),
            Command::Peek(name, uuid) => {
                match self.traces.get_trace(name.clone()) {
                    Some(mut trace) => {
                        let (mut cur, storage) = trace.cursor();
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
                        let encoded = bincode::serialize(&out).unwrap();
                        self.rpc_client
                            .post("http://localhost:6875/api/peek-results")
                            .header("X-Materialize-Query-UUID", uuid.to_string())
                            .body(encoded)
                            .send()
                            .unwrap();
                    }
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
            Command::Tail(_) => unimplemented!(),
        }
    }
}
