// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A channel for broadcasting compute commands from worker 0 to other workers.
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

use std::sync::{Arc, Mutex};

use crossbeam_channel::TryRecvError;
use itertools::Itertools;
use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_types::dataflows::{BuildDesc, DataflowDescription};
use mz_ore::cast::CastFrom;
use timely::communication::Allocate;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::generic::source;
use timely::scheduling::{Scheduler, SyncActivator};
use timely::worker::Worker as TimelyWorker;
use uuid::Uuid;

/// A sender pushing commands onto the command channel.
pub struct Sender {
    tx: crossbeam_channel::Sender<(ComputeCommand, Uuid)>,
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
    rx: crossbeam_channel::Receiver<(ComputeCommand, Uuid)>,
}

impl Receiver {
    /// Returns the next available command, if any.
    ///
    /// This returns `None` when there are currently no commands but there might be commands again
    /// in the future.
    pub fn try_recv(&self) -> Option<(ComputeCommand, Uuid)> {
        match self.rx.try_recv() {
            Ok(msg) => Some(msg),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => {
                unreachable!("command channel never shuts down");
            }
        }
    }
}

/// Render the command channel dataflow.
pub fn render<A: Allocate>(timely_worker: &mut TimelyWorker<A>) -> (Sender, Receiver) {
    let (input_tx, input_rx) = crossbeam_channel::unbounded();
    let (output_tx, output_rx) = crossbeam_channel::unbounded();
    let activator = Arc::new(Mutex::new(None));

    // TODO(teskje): This implementation relies on Timely channels preserving the order of their
    // inputs, which is not something they guarantee. We can avoid that by using explicit indexing,
    // like storage's command sequencer does.
    timely_worker.dataflow_named::<u64, _, _>("command_channel", {
        let activator = Arc::clone(&activator);
        move |scope| {
            source(scope, "command_channel::source", |cap, info| {
                let sync_activator = scope.sync_activator_for(info.address.to_vec());
                *activator.lock().expect("poisoned") = Some(sync_activator);

                let worker_id = scope.index();
                let peers = scope.peers();

                // Only worker 0 broadcasts commands, other workers must drop their capability to
                // avoid holding up dataflow progress.
                let mut capability = (worker_id == 0).then_some(cap);

                move |output| {
                    let Some(cap) = &mut capability else {
                        // Non-leader workers will still receive `UpdateConfiguration` commands and
                        // we must drain those to not leak memory.
                        while let Ok((cmd, _nonce)) = input_rx.try_recv() {
                            assert_ne!(worker_id, 0);
                            assert!(matches!(cmd, ComputeCommand::UpdateConfiguration(_)));
                        }
                        return;
                    };

                    assert_eq!(worker_id, 0);

                    let input: Vec<_> = input_rx.try_iter().collect();
                    for (cmd, nonce) in input {
                        let worker_cmds =
                            split_command(cmd, peers).map(|(idx, cmd)| (idx, cmd, nonce));
                        output.session(&cap).give_iterator(worker_cmds);

                        cap.downgrade(&(cap.time() + 1));
                    }
                }
            })
            .sink(
                Exchange::new(|(idx, _, _)| u64::cast_from(*idx)),
                "command_channel::sink",
                move |(input, _)| {
                    input.for_each(|_time, data| {
                        for (_idx, cmd, nonce) in data.drain(..) {
                            let _ = output_tx.send((cmd, nonce));
                        }
                    });
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

/// Split the given command into the given number of parts.
///
/// Returns an iterator that produces each command part, along with its part index.
fn split_command(
    command: ComputeCommand,
    parts: usize,
) -> impl Iterator<Item = (usize, ComputeCommand)> {
    use itertools::Either;

    let commands = match command {
        ComputeCommand::CreateDataflow(dataflow) => {
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
                .map(ComputeCommand::CreateDataflow);
            Either::Left(commands)
        }
        command => {
            let commands = std::iter::repeat_n(command, parts);
            Either::Right(commands)
        }
    };

    commands.into_iter().enumerate()
}
