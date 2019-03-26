// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

//! An interactive dataflow server.

use serde::{Deserialize, Serialize};
use std::sync::mpsc;
use timely::communication::initialize::WorkerGuards;
use timely::communication::Allocate;
use timely::synchronization::Sequencer;
use timely::worker::Worker as TimelyWorker;

use super::render;
use super::types::Dataflow;
use super::trace::TraceManager;

pub fn serve(cmd_rx: CommandReceiver) -> Result<WorkerGuards<()>, String> {
    let cmd_rx = std::sync::Mutex::new(Some(cmd_rx));

    timely::execute(timely::Configuration::Process(4), move |worker| {
        let cmd_rx = match cmd_rx.try_lock().ok() {
            Some(mut guard) => guard.take().unwrap_or_else(|| dummy_command_receiver()),
            None => dummy_command_receiver(),
        };
        Worker::new(worker, cmd_rx).run()
    })
}

/// The commands that a running dataflow server can accept.
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
    traces: TraceManager,
}

impl<'w, A> Worker<'w, A>
where
    A: Allocate,
{
    fn new(w: &'w mut TimelyWorker<A>, cmd_rx: CommandReceiver) -> Worker<'w, A> {
        let sequencer = Sequencer::new(w, std::time::Instant::now());
        Worker {
            inner: w,
            cmd_rx,
            sequencer,
            traces: TraceManager::new(),
        }
    }

    fn run(&mut self) {
        loop {
            // Submit any external commands for sequencing,.
            while let Ok(cmd) = self.cmd_rx.try_recv() {
                self.sequencer.push(cmd)
            }

            // Handle any sequenced commands.
            while let Some(cmd) = self.sequencer.next() {
                self.handle_command(&cmd)
            }

            // Ask Timely to execute a unit of work.
            self.inner.step();
        }
    }

    fn handle_command(&mut self, cmd: &Command) {
        match cmd {
            Command::CreateDataflow(dataflow) => {
                render::build_dataflow(dataflow, &mut self.traces, self.inner)
            }
            Command::DropDataflow(_) => unimplemented!(),
            Command::Peek(name, uuid) => {
                use differential_dataflow::trace::TraceReader;
                use differential_dataflow::trace::cursor::Cursor;
                if let Some(mut trace) = self.traces.get_trace(name.clone()) {
                    let (mut cur, storage) = trace.cursor();
                    let mut out = Vec::new();
                    while cur.key_valid(&storage) {
                        out.push(cur.key(&storage));
                        cur.step_key(&storage)
                    }
                    let encoded = bincode::serialize(&out).unwrap();
                    let client = reqwest::Client::new();
                    client
                        .post("http://localhost:6875/api/peek-results")
                        .header("X-Materialize-Query-UUID", uuid.to_string())
                        .body(encoded)
                        .send()
                        .unwrap();
                } else {
                    println!("no trace named {}", name);
                }
            }
            Command::Tail(_) => unimplemented!(),
        }
    }
}
