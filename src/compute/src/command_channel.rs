// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A channel for sequencing commands between all workers of a Timely cluster.
//!
//! Compute uses a dataflow to distribute commands between workers. This is to ensure workers
//! retain a consistent dataflow state across reconnects. If each worker would receive its commands
//! directly from the controller, there wouldn't be any guarantee that after a reconnect all
//! workers have seen the same sequence of commands. This is particularly problematic for
//! `CreateDataflow` commands, since Timely requires that all workers render the same dataflows in
//! the same order. So the controller instead sends commands only to worker 0, which then
//! broadcasts them to other workers through the Timely fabric, taking care of the correct
//! sequencing.
//!
//! Commands in the command channel are tagged with a nonce identifying the incarnation of the
//! compute protocol the command belongs to, allowing workers to recognize client reconnects that
//! require a reconciliation.
//!
//! SPIKE(unified-cluster): The channel optionally also carries storage-internal commands, for
//! clusters that host storage objects alongside compute objects. Both command kinds are sequenced
//! through a single lane, so all workers observe one consistent interleaving and therefore
//! construct all dataflows, compute and storage alike, in the same order. Unlike compute commands,
//! storage-internal commands may be injected from any worker (e.g. by health operators triggering
//! a suspend-and-restart), so the channel uses a two-hop structure copied from storage's command
//! sequencer: producers tag commands with a per-producer index, worker 0 fixes one definitive
//! order and assigns a global index, and receivers restore that order.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::mpsc::{self, TryRecvError};
use std::sync::{Arc, Mutex};

use itertools::Itertools;
use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_types::dataflows::{BuildDesc, DataflowDescription};
use mz_ore::cast::CastFrom;
use mz_storage::internal_control::InternalStorageCommand;
use mz_timely_util::scope_label::ScopeExt;
use serde::{Deserialize, Serialize};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::generic::source;
use timely::scheduling::{Activator, SyncActivator};
use timely::worker::Worker as TimelyWorker;
use uuid::Uuid;

/// A command in the unified command lane.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum UnifiedCommand {
    /// A compute command, tagged with the client nonce.
    Compute(ComputeCommand, Uuid),
    /// A storage-internal command.
    Storage(InternalStorageCommand),
}

/// A sender pushing compute commands onto the command channel.
pub struct Sender {
    tx: mpsc::Sender<(ComputeCommand, Uuid)>,
    activator: Arc<Mutex<Option<SyncActivator>>>,
}

impl Sender {
    /// Broadcasts the given command to all workers.
    pub fn send(&self, message: (ComputeCommand, Uuid)) {
        if self.tx.send(message).is_err() {
            unreachable!("command channel never shuts down");
        }

        self.activator
            .lock()
            .expect("poisoned")
            .as_ref()
            .map(|a| a.activate());
    }
}

/// A receiver reading commands from the command channel.
pub struct Receiver {
    rx: mpsc::Receiver<UnifiedCommand>,
}

impl Receiver {
    /// Returns the next available command, if any.
    ///
    /// This returns `None` when there are currently no commands but there might be commands again
    /// in the future.
    pub fn try_recv(&self) -> Option<UnifiedCommand> {
        match self.rx.try_recv() {
            Ok(msg) => Some(msg),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => {
                unreachable!("command channel never shuts down");
            }
        }
    }
}

/// Per-worker storage-side inputs to the command channel.
///
/// SPIKE(unified-cluster): Created by the host before rendering the channel; the sending halves
/// back the guest's `InternalCommandSender`.
pub struct StorageLaneInput {
    /// Receiver for storage-internal commands injected on this worker.
    pub rx: mpsc::Receiver<InternalStorageCommand>,
    /// Slot the channel fills with an activator for the source operator, so sends wake the
    /// dataflow.
    pub activator_slot: Rc<RefCell<Option<Activator>>>,
}

/// Render the command channel dataflow.
pub fn render(
    timely_worker: &mut TimelyWorker,
    storage_input: Option<StorageLaneInput>,
) -> (Sender, Receiver) {
    let (input_tx, input_rx) = mpsc::channel();
    let (output_tx, output_rx) = mpsc::channel();
    let activator = Arc::new(Mutex::new(None));

    timely_worker.dataflow_named::<(), _, _>("command_channel", {
        let activator = Arc::clone(&activator);
        move |scope| {
            let scope = scope.with_label();

            let peers = scope.peers();

            // Create a stream of commands received from this worker's input queues.
            //
            // The output commands are tagged by worker ID and a per-producer command index,
            // allowing the sequencer to restore their correct relative order.
            let stream = source(scope, "command_channel::source", |cap, info| {
                let sync_activator = scope.worker().sync_activator_for(info.address.to_vec());
                *activator.lock().expect("poisoned") = Some(sync_activator);

                if let Some(input) = &storage_input {
                    let act = scope.activator_for(info.address);
                    *input.activator_slot.borrow_mut() = Some(act);
                }

                let worker_id = scope.index();
                let mut cmd_index = 0_u64;
                let capability = Some(cap);

                move |output| {
                    let Some(cap) = &capability else {
                        return;
                    };

                    let mut session = output.session(cap);

                    while let Ok((cmd, nonce)) = input_rx.try_recv() {
                        if worker_id == 0 {
                            session.give((
                                worker_id,
                                cmd_index,
                                UnifiedCommand::Compute(cmd, nonce),
                            ));
                            cmd_index += 1;
                        } else {
                            // Non-leader workers only receive `UpdateConfiguration` commands
                            // from the controller and must drop them to not sequence duplicates.
                            assert!(matches!(cmd, ComputeCommand::UpdateConfiguration(_)));
                        }
                    }

                    if let Some(input) = &storage_input {
                        while let Ok(cmd) = input.rx.try_recv() {
                            session.give((worker_id, cmd_index, UnifiedCommand::Storage(cmd)));
                            cmd_index += 1;
                        }
                    }
                }
            });

            // Sequence all commands through a single worker to establish a unique order.
            //
            // The output commands are tagged with a global command index and a target worker,
            // allowing downstream operators to ensure their correct relative order.
            let stream = stream.unary_frontier(
                Exchange::new(|_| 0),
                "command_channel::sequencer",
                |cap, _info| {
                    let mut global_index = 0_u64;
                    let mut capability = Some(cap);

                    // For each producer, keep an ordered list of pending commands, as well as the
                    // index of the next command.
                    let mut pending: Vec<(BTreeMap<u64, UnifiedCommand>, u64)> =
                        vec![(BTreeMap::new(), 0); peers];

                    move |(input, frontier), output| {
                        let Some(cap) = capability.clone() else {
                            return;
                        };

                        input.for_each(|_time, data| {
                            for (producer, index, cmd) in data.drain(..) {
                                pending[producer].0.insert(index, cmd);
                            }
                        });

                        let mut session = output.session(&cap);
                        for (commands, next_idx) in &mut pending {
                            while commands
                                .first_key_value()
                                .is_some_and(|(i, _)| i == next_idx)
                            {
                                let (_, cmd) = commands.pop_first().unwrap();
                                for (target, part) in split_command(cmd, peers) {
                                    session.give((target, global_index, part));
                                }

                                *next_idx += 1;
                                global_index += 1;
                            }
                        }

                        drop(session);

                        if frontier.is_empty() {
                            // Drop our capability to shut down.
                            capability = None;
                        }
                    }
                },
            );

            // Sink the stream back into `output_tx`, restoring the global order.
            stream.sink(
                Exchange::new(|(target, _, _)| u64::cast_from(*target)),
                "command_channel::sink",
                {
                    // Pending commands by global index, and the index of the next command.
                    let mut pending = BTreeMap::new();
                    let mut next_idx = 0_u64;

                    move |(input, _frontier)| {
                        input.for_each(|_time, data| {
                            for (_target, index, cmd) in data.drain(..) {
                                pending.insert(index, cmd);
                            }
                        });

                        while pending
                            .first_key_value()
                            .is_some_and(|(i, _)| *i == next_idx)
                        {
                            let (_, cmd) = pending.pop_first().unwrap();
                            let _ = output_tx.send(cmd);
                            next_idx += 1;
                        }
                    }
                },
            );
        }
    });

    let tx = Sender {
        tx: input_tx,
        activator,
    };
    let rx = Receiver { rx: output_rx };

    (tx, rx)
}

/// Split the given command into one part per target worker.
///
/// Compute `CreateDataflow` commands are partitioned among the workers; every other command is
/// replicated to all workers.
fn split_command(
    command: UnifiedCommand,
    parts: usize,
) -> impl Iterator<Item = (usize, UnifiedCommand)> {
    use itertools::Either;

    let commands = match command {
        UnifiedCommand::Compute(ComputeCommand::CreateDataflow(dataflow), nonce) => {
            let dataflow = *dataflow;

            // A list of descriptions of objects for each part to build.
            let mut builds_parts = vec![Vec::new(); parts];
            // Partition each build description among `parts`.
            for build_desc in dataflow.objects_to_build {
                let build_part = build_desc.plan.partition_among(parts);
                for (plan, objects_to_build) in
                    build_part.into_iter().zip_eq(builds_parts.iter_mut())
                {
                    objects_to_build.push(BuildDesc {
                        id: build_desc.id,
                        plan,
                    });
                }
            }

            // Each list of build descriptions results in a dataflow description.
            let commands = builds_parts
                .into_iter()
                .map(move |objects_to_build| DataflowDescription {
                    source_imports: dataflow.source_imports.clone(),
                    index_imports: dataflow.index_imports.clone(),
                    objects_to_build,
                    index_exports: dataflow.index_exports.clone(),
                    sink_exports: dataflow.sink_exports.clone(),
                    as_of: dataflow.as_of.clone(),
                    until: dataflow.until.clone(),
                    debug_name: dataflow.debug_name.clone(),
                    initial_storage_as_of: dataflow.initial_storage_as_of.clone(),
                    refresh_schedule: dataflow.refresh_schedule.clone(),
                    time_dependence: dataflow.time_dependence.clone(),
                })
                .map(Box::new)
                .map(move |dataflow| {
                    UnifiedCommand::Compute(ComputeCommand::CreateDataflow(dataflow), nonce)
                });
            Either::Left(commands)
        }
        command => {
            let commands = std::iter::repeat_n(command, parts);
            Either::Right(commands)
        }
    };

    commands.into_iter().enumerate()
}
