// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! How far the maintenance runtime applied its compute commands, as the interactive runtime sees
//! it.
//!
//! Each maintenance worker reports the [`CommandNumber`]s it applied through a
//! [`mz_timely_util::capture::progress_link`] that its [`crate::sharing::ArrangementSharingRegistry`]
//! holds, and the interactive worker of the same pair replays the link. The command channel's own
//! frontier is not a substitute: it passes a command once the channel delivered it to a worker's
//! queue, before the worker applied it.

use std::any::Any;
use std::rc::Rc;

use mz_timely_util::capture::{ProgressPusher, ProgressSource};
use timely::dataflow::operators::Probe;
use timely::dataflow::operators::probe::Handle;
use timely::progress::Antichain;
use timely::worker::Worker as TimelyWorker;

use crate::command_channel::CommandNumber;

/// The maintenance worker's report of the commands it applied.
pub(crate) struct CommandProgress {
    pusher: ProgressPusher<CommandNumber>,
}

impl CommandProgress {
    /// Reports through `pusher`, whose replayed frontier starts at command 0.
    pub(crate) fn new(pusher: ProgressPusher<CommandNumber>) -> Self {
        Self { pusher }
    }

    /// Reports that the worker applied every command through `number`.
    ///
    /// `number` must not decrease across calls.
    pub(crate) fn applied(&mut self, number: CommandNumber) {
        let next = number + 1;
        self.pusher.advance_to(Antichain::from_elem(next).borrow());
    }
}

/// The interactive worker's replay of its maintenance peer's [`CommandProgress`].
pub(crate) struct MaintenanceProgress {
    probe: Handle<CommandNumber>,
    /// Keeps the replay operator running.
    _token: Rc<dyn Any>,
}

impl MaintenanceProgress {
    /// Renders the `maintenance_progress` dataflow, which replays `source` for the lifetime of
    /// the returned value.
    ///
    /// Every worker of a runtime must call this at the same point of its dataflow construction.
    /// The probed frontier is the meet over the replays of all workers of the runtime, so a
    /// command has passed once every maintenance worker paired with one of them applied it.
    pub(crate) fn render(worker: &mut TimelyWorker, source: ProgressSource<CommandNumber>) -> Self {
        let (probe, token) =
            worker.dataflow_named::<CommandNumber, _, _>("maintenance_progress", |scope| {
                let (stream, token) = source.replay(scope, "maintenance_progress");
                (stream.probe().0, token)
            });
        Self {
            probe,
            _token: token,
        }
    }

    /// Whether the maintenance runtime applied command `number`.
    pub(crate) fn passed(&self, number: CommandNumber) -> bool {
        self.probe
            .with_frontier(|frontier| !frontier.less_equal(&number))
    }

    /// The first command the maintenance runtime has not applied.
    pub(crate) fn frontier(&self) -> Antichain<CommandNumber> {
        self.probe.with_frontier(|frontier| frontier.to_owned())
    }
}

#[cfg(test)]
mod tests;
