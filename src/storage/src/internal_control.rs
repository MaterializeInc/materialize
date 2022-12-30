// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Types for cluster-internal control messages that can be broadcast to all
//! workers from individual operators/workers.

use serde::{Deserialize, Serialize};
use timely::communication::Allocate;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Broadcast, Operator};

use mz_repr::GlobalId;
use mz_timely_util::builder_async::OperatorBuilder as AsyncOperatorBuilder;

use crate::storage_state::Worker;

/// Internal commands that can be sent by individual operators/workers that will
/// be broadcast to all workers. The worker main loop will receive those and act
/// on them.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum InternalStorageCommand {
    /// For testing.
    Message(String),
    /// Suspend and restart the dataflow identified by the `GlobalId`.
    SuspendAndRestart(GlobalId),
}

// TODO(aljoscha): Should these be bounded?
/// And unbounded receiver of internal storage commands.
pub type InternalCommandReceiver = tokio::sync::mpsc::UnboundedReceiver<InternalStorageCommand>;
/// And unbounded receiver of internal storage commands.
pub type InternalCommandSender = tokio::sync::mpsc::UnboundedSender<InternalStorageCommand>;

impl<'w, A: Allocate> Worker<'w, A> {
    /// Sets up an internal dataflow that will receive worker-local commands on
    /// the given receiver and broadcast them to all workers. The returned
    /// receiver should be used to receive all broadcast commands.
    pub fn setup_internal_command_pump(
        &mut self,
        mut internal_cmd_rx: InternalCommandReceiver,
    ) -> crossbeam_channel::Receiver<InternalStorageCommand> {
        let (forwarding_command_tx, forwarding_command_rx) = crossbeam_channel::unbounded();

        self.timely_worker.dataflow::<u64, _, _>(move |scope| {
            let mut pump_op =
                AsyncOperatorBuilder::new("InternalStorageCmdPump".to_string(), scope.clone());

            let (mut cmd_output, cmd_stream) = pump_op.new_output();

            let _shutdown_button = pump_op.build(move |mut capabilities| async move {
                let mut cap = capabilities.pop().expect("missing capability");

                while let Some(cmd) = internal_cmd_rx.recv().await {
                    let time = cap.time().clone();
                    let mut cmd_output = cmd_output.activate();
                    let mut session = cmd_output.session(&cap);
                    session.give(cmd);

                    // TODO(aljoscha): This will not work when we have more than
                    // one worker. They will have to use something like
                    // wall-clock time, which advances roughly in lock-step on
                    // all workers instead of workers just counting the number
                    // of commands.
                    //
                    // Also, though, the above is only true if we care about
                    // frontiers. Which I think we do because we will need to
                    // get commands into a deterministic order before working
                    // them off. And we can only do this once we know that we
                    // have seen all commands before a certain time.
                    cap.downgrade(&(time + 1));
                }
            });

            cmd_stream.broadcast().unary_frontier::<Vec<()>, _, _, _>(
                Pipeline,
                "InternalStorageCmdForwarder",
                |_cap, _info| {
                    let mut container = Default::default();
                    move |input, _output| {
                        while let Some((_, data)) = input.next() {
                            data.swap(&mut container);
                            for cmd in container.drain(..) {
                                let res = forwarding_command_tx.send(cmd);
                                match res {
                                    Ok(_) => {
                                        // All's well!
                                        //
                                    }
                                    Err(_send_err) => {
                                        // The subscribe loop dropped, meaning
                                        // we're probably shutting down. Seems
                                        // fine to ignore.
                                    }
                                }
                            }
                        }
                    }
                },
            );
        });

        forwarding_command_rx
    }
}
