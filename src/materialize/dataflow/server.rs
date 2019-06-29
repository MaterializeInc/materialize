// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! An interactive dataflow server.

use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::TraceReader;

use timely::communication::initialize::WorkerGuards;
use timely::communication::Allocate;
use timely::dataflow::operators::unordered_input::ActivateCapability;
use timely::progress::frontier::Antichain;
use timely::synchronization::sequence::Sequencer;
use timely::worker::Worker as TimelyWorker;

use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::mem;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use uuid::Uuid;

use super::render;
use super::render::InputCapability;
use super::trace::{KeysOnlyHandle, TraceManager};
use super::{LocalSourceConnector, RelationExpr, Source, SourceConnector};
use crate::dataflow::{Dataflow, Timestamp, View};
use crate::glue::*;
use crate::repr::{ColumnType, Datum, RelationType, ScalarType};

pub fn serve(
    dataflow_command_receivers: Vec<UnboundedReceiver<(DataflowCommand, CommandMeta)>>,
    dataflow_results_handler: DataflowResultsHandler,
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
            dataflow_results_handler.clone(),
        )
        .run()
    })
}

#[derive(Clone)]
pub enum DataflowResultsHandler {
    Local(DataflowResultsMux),
    Remote,
}

#[derive(Clone, Serialize, Deserialize)]
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

struct PendingInsert {
    /// The name of the dataflow to insert into.
    name: String,
    /// The capability to use to perform the insert.
    capability: ActivateCapability<Timestamp>,
}

lazy_static! {
    // Bootstrapping adds a dummy table, "dual", with one row, which the SQL
    // planner depends upon.
    //
    // TODO(benesch): perhaps the SQL layer should be responsible for installing
    // it, then? It's not fundamental to this module.
    static ref BOOTSTRAP_COMMANDS: Vec<DataflowCommand> = vec![
        DataflowCommand::CreateDataflow(Dataflow::Source(Source {
            name: "dual".into(),
            connector: SourceConnector::Local(LocalSourceConnector {}),
            typ: RelationType::new(vec![ColumnType {
                name: Some("x".into()),
                nullable: false,
                scalar_type: ScalarType::String,
            }]),
        })),
        DataflowCommand::Peek {
            source: RelationExpr::Constant {
                rows: vec![vec![Datum::String("X".into())]],
                typ: RelationType::new(vec![ColumnType {
                    name: None,
                    nullable: false,
                    scalar_type: ScalarType::String,
                }]),
            },
            when: PeekWhen::AfterFlush,
            insert_into: Some("dual".into()),
        },
    ];
}

struct Worker<'w, A>
where
    A: Allocate,
{
    inner: &'w mut TimelyWorker<A>,
    dataflow_command_receiver: UnboundedReceiver<(DataflowCommand, CommandMeta)>,
    dataflow_results_handler: DataflowResultsHandler,
    pending_peeks: Vec<(PendingPeek, KeysOnlyHandle)>,
    pending_inserts: HashMap<String, PendingInsert>,
    traces: TraceManager,
    rpc_client: reqwest::Client,
    inputs: HashMap<String, InputCapability>,
    input_time: u64,
    transient_view_counter: u64,
    dataflows: HashMap<String, Dataflow>,
    sequencer: Sequencer<PendingPeek>,
}

impl<'w, A> Worker<'w, A>
where
    A: Allocate,
{
    fn new(
        w: &'w mut TimelyWorker<A>,
        dataflow_command_receiver: UnboundedReceiver<(DataflowCommand, CommandMeta)>,
        dataflow_results_handler: DataflowResultsHandler,
    ) -> Worker<'w, A> {
        let sequencer = Sequencer::new(w, Instant::now());
        Worker {
            inner: w,
            dataflow_command_receiver,
            dataflow_results_handler,
            pending_peeks: Vec::new(),
            pending_inserts: HashMap::new(),
            traces: TraceManager::new(),
            rpc_client: reqwest::Client::new(),
            inputs: HashMap::new(),
            input_time: 1,
            transient_view_counter: 1,
            dataflows: HashMap::new(),
            sequencer,
        }
    }

    /// Draws from `dataflow_command_receiver` until shutdown.
    fn run(&mut self) {
        for cmd in BOOTSTRAP_COMMANDS.iter() {
            self.handle_command(
                cmd.clone(),
                CommandMeta {
                    connection_uuid: Uuid::nil(),
                },
            );
        }

        let mut shutdown = false;
        while !shutdown {
            // Ask Timely to execute a unit of work.
            // Can either yield tastefully, or busy-wait.
            // self.inner.step_or_park(None);
            self.inner.step();

            self.process_peeks();

            // Handle any received commands
            while let Ok(Some((cmd, cmd_meta))) = self.dataflow_command_receiver.try_next() {
                if let DataflowCommand::Shutdown = cmd {
                    shutdown = true;
                }
                self.handle_command(cmd, cmd_meta);
            }
        }
    }

    fn handle_command(&mut self, cmd: DataflowCommand, cmd_meta: CommandMeta) {
        match cmd {
            DataflowCommand::CreateDataflow(dataflow) => {
                render::build_dataflow(
                    &dataflow,
                    &mut self.traces,
                    self.inner,
                    &mut self.inputs,
                    self.input_time,
                );
                self.dataflows.insert(dataflow.name().to_owned(), dataflow);
            }

            DataflowCommand::DropDataflows(dataflows) => {
                for name in &dataflows {
                    self.inputs.remove(name);
                    self.dataflows.remove(name);
                    self.traces.del_trace(name);
                }
            }

            DataflowCommand::Peek {
                source,
                when,
                insert_into,
            } => {
                self.peek(cmd_meta, source, when, insert_into);
            }

            DataflowCommand::Tail(_) => unimplemented!(),

            DataflowCommand::Shutdown => {
                // this should lead timely to wind down eventually
                self.inputs.clear();
                self.traces.del_all_traces();
            }
        }
    }

    fn peek(
        &mut self,
        cmd_meta: CommandMeta,
        source: RelationExpr,
        when: PeekWhen,
        insert_into: Option<String>,
    ) {
        let (name, drop) = if let RelationExpr::Get { name, typ: _ } = source {
            // Fast path. Just look at the existing dataflow directly.
            (name, false)
        } else {
            // Slow path. We need to perform some computation, so build a new
            // transient dataflow that will be dropped after the peek completes.
            let name = format!("<temp_{}>", self.transient_view_counter);
            self.transient_view_counter += 1;
            let typ = source.typ();
            let dataflow = Dataflow::View(View {
                name: name.clone(),
                relation_expr: source,
                typ,
            });
            render::build_dataflow(
                &dataflow,
                &mut self.traces,
                self.inner,
                &mut self.inputs,
                self.input_time,
            );
            self.dataflows
                .insert(dataflow.name().to_owned(), dataflow.clone());
            (name, true)
        };

        if let Some(insert_into) = insert_into {
            // This results of this peek should be inserted into an existing
            // dataflow. Stash away the capability to insert as the current
            // time, because it might be a while before the peek completes.
            match self
                .inputs
                .get_mut(&insert_into)
                .expect("Failed to find input")
            {
                InputCapability::Local { capability, .. } => self.pending_inserts.insert(
                    name.clone(),
                    PendingInsert {
                        name: insert_into,
                        capability: capability.clone(),
                    },
                ),
                InputCapability::External(_) => panic!("attempted to insert into external source"),
            };

            // Unconditionally advance time after an insertion to allow the
            // computation to make progress. Importantly, this occurs on *all*
            // internal inputs.
            self.input_time += 1;
            for handle in self.inputs.values_mut() {
                match handle {
                    InputCapability::Local { capability, .. } => {
                        capability.downgrade(&self.input_time);
                    }
                    InputCapability::External(_) => (),
                }
            }
        }

        if self.inner.index() != 0 {
            // Only worker 0 sequences peeks. It will broadcast the sequenced
            // peek to other workers.
            return;
        }

        let timestamp = match when {
            PeekWhen::Immediately => {
                let trace = self
                    .traces
                    .get_trace(&name)
                    .unwrap_or_else(|| panic!("failed to find arrangement for PEEK {}", name));

                // Ask the trace for the latest time it knows about. The latest
                // "committed" time (i.e., the time at which we know results can
                // never change) is one less than that.
                //
                // TODO(benesch, fms): this approach does not work for arbitrary
                // timestamp types, where a predecessor operation may not exist.
                let mut upper = Antichain::new();
                trace.clone().read_upper(&mut upper);
                if upper.elements().is_empty() {
                    0
                } else {
                    assert_eq!(upper.elements().len(), 1);
                    upper.elements()[0].saturating_sub(1)
                }
            }

            // Compute the lastest time that is committed by all inputs. Peeking
            // at this time may involve waiting for the outputs to catch up.
            PeekWhen::AfterFlush => self.root_input_time(&name).saturating_sub(1),

            PeekWhen::AtTimestamp(timestamp) => timestamp,
        };

        self.sequencer.push(PendingPeek {
            name,
            connection_uuid: cmd_meta.connection_uuid,
            timestamp,
            drop_after_peek: drop,
        });
    }

    fn root_input_time(&self, name: &str) -> u64 {
        match &self.dataflows[name] {
            Dataflow::Source(_) => match &self.inputs[name] {
                InputCapability::External(capability) => *capability.borrow().time(),
                InputCapability::Local { capability, .. } => *capability.time(),
            },
            Dataflow::Sink(_) => unreachable!(),
            v @ Dataflow::View(_) => v
                .uses()
                .iter()
                .map(|n| self.root_input_time(n))
                .min()
                .unwrap_or(u64::max_value()),
        }
    }

    /// Scan pending peeks and attempt to retire each.
    fn process_peeks(&mut self) {
        // Look for new peeks.
        while let Some(pending_peek) = self.sequencer.next() {
            let trace = self.traces.get_trace(&pending_peek.name).unwrap();
            self.pending_peeks.push((pending_peek, trace));
        }

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
            let results = if let Some(insert) = self.pending_inserts.remove(&peek.name) {
                let n = results.len();
                match self
                    .inputs
                    .get_mut(&insert.name)
                    .expect("Failed to find input")
                {
                    InputCapability::Local { handle, .. } => {
                        let time = *insert.capability.time();
                        let mut session = handle.session(insert.capability);
                        for row in results {
                            session.give((row, time, 1));
                        }
                    }
                    InputCapability::External(_) => {
                        panic!("attempted to insert into external source")
                    }
                }
                DataflowResults::Inserted(n)
            } else {
                DataflowResults::Peeked(results)
            };
            match &self.dataflow_results_handler {
                DataflowResultsHandler::Local(peek_results_mux) => {
                    // The sender is allowed disappear at any time, so the
                    // error handling here is deliberately relaxed.
                    if let Ok(sender) = peek_results_mux
                        .read()
                        .unwrap()
                        .sender(&peek.connection_uuid)
                    {
                        drop(sender.unbounded_send(results))
                    }
                }
                DataflowResultsHandler::Remote => {
                    let encoded = bincode::serialize(&results).unwrap();
                    self.rpc_client
                        .post("http://localhost:6875/api/dataflow-results")
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
                DataflowCommand::DropDataflows(dataflows_to_be_dropped),
                CommandMeta {
                    connection_uuid: Uuid::nil(),
                },
            );
        }
    }
}
