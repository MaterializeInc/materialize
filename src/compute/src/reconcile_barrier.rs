// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A barrier between the reconciliations of a process's two compute runtimes.
//!
//! Each runtime reconciles a new controller connection on its own: it drops the dataflows the
//! connection's history replaces, then creates the ones it adds. An index recreated at the same
//! `as_of` maps to the same registry key on both runtimes, so without coordination the interactive
//! runtime can bind the new incarnation's peer to the slot of the old incarnation while the
//! maintenance runtime still runs it, and read that incarnation's frozen chain once it drops. The
//! barrier keeps either runtime from creating dataflows before the other one finished its drops.
//!
//! A worker arrives once its drops for a connection are done and then waits until the paired
//! worker of the other runtime arrived at the same connection or a later one. A later arrival
//! counts because the barrier keeps only each side's latest arrival, and the other side can
//! arrive at the connection, pass, and arrive at the next one before the waiting worker rechecks.
//! Both runtimes receive the same commands of every connection apart from peeks, because each
//! runtime's channel adapter forwards a connection's commands until the connection's command
//! sender is dropped, so both reconcile the same connections.
//! Waiting is the only way out of the barrier, so every worker of a runtime executes the same
//! commands in the same order, which timely's dataflow construction requires. The wait ends: of two
//! waiting workers the one at the earlier connection passes, and the other side reconciles every
//! connection the waiting side does.
//!
//! Connections are ordered by a [`ConnectionOrder`] that the process's command multiplexer records
//! in, as it forwards each connection's `Hello`. Only process 0's order is read. Global worker 0 of
//! each runtime runs there, looks up a connection's position when it sequences the connection's
//! commands, and the command channel delivers the position with them to every worker of its runtime.
//! Both runtimes' sequencers read the same order, so the two workers of a pair agree on positions.
//! A worker never looks a position up in its own process's order: the controller sends a
//! connection's `Hello` to every process but the commands workers reconcile to process 0 only, so
//! another process can receive the `Hello` after those commands reached its workers through the
//! command channel, or not at all.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::thread::Thread;

use mz_ore::cast::CastFrom;
use uuid::Uuid;

/// The order in which a process accepted its controller connections, by the nonce of each
/// connection's `Hello`.
///
/// Cloning shares the same order. Holds one entry per connection the process accepted.
#[derive(Clone, Debug, Default)]
pub struct ConnectionOrder {
    positions: Arc<Mutex<BTreeMap<Uuid, u64>>>,
}

impl ConnectionOrder {
    /// Records the connection identified by `nonce` after every connection recorded so far.
    ///
    /// Must be called before either runtime's command sequencer receives a command of the
    /// connection, so the sequencer finds its position. Recording a nonce again keeps its position.
    pub fn record(&self, nonce: Uuid) {
        let mut positions = self.positions.lock().expect("connection order poisoned");
        let next = u64::cast_from(positions.len());
        positions.entry(nonce).or_insert(next);
    }

    /// The position of the connection identified by `nonce`, if it was recorded.
    pub(crate) fn position(&self, nonce: Uuid) -> Option<u64> {
        let positions = self.positions.lock().expect("connection order poisoned");
        positions.get(&nonce).copied()
    }
}

/// One of the two runtimes a [`ReconcileBarrier`] synchronizes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BarrierSide {
    /// The runtime that renders maintained dataflows and publishes their indexes.
    Maintenance,
    /// The runtime that renders one-shot reads over the published indexes.
    Interactive,
}

impl BarrierSide {
    fn index(self) -> usize {
        match self {
            BarrierSide::Maintenance => 0,
            BarrierSide::Interactive => 1,
        }
    }

    fn other(self) -> Self {
        match self {
            BarrierSide::Maintenance => BarrierSide::Interactive,
            BarrierSide::Interactive => BarrierSide::Maintenance,
        }
    }
}

/// The reconciliation barrier between one worker of each runtime. See the module documentation.
///
/// Cloning shares the same barrier.
#[derive(Clone, Debug, Default)]
pub(crate) struct ReconcileBarrier {
    state: Arc<Mutex<BarrierState>>,
}

#[derive(Debug, Default)]
struct BarrierState {
    /// Per side, the position of the latest connection whose drops it finished.
    arrived: [Option<u64>; 2],
    /// Per side, the thread to wake when the other side arrives, registered while it waits.
    waiting: [Option<Thread>; 2],
}

impl ReconcileBarrier {
    /// Records that `side` finished the drops of its reconciliation of the connection at
    /// `position`, and wakes the other side if it waits.
    pub(crate) fn arrive(&self, side: BarrierSide, position: u64) {
        let mut state = self.state.lock().expect("reconcile barrier poisoned");
        let arrived = &mut state.arrived[side.index()];
        // A runtime reconciles connections in the order the process accepted them.
        mz_ore::soft_assert_or_log!(
            arrived.is_none_or(|previous| previous <= position),
            "reconcile barrier: {side:?} arrived at connection {position} after {arrived:?}"
        );
        *arrived = Some(position);
        if let Some(thread) = &state.waiting[side.other().index()] {
            thread.unpark();
        }
    }

    /// Whether the other side finished the drops of its reconciliation of the connection at
    /// `position` or of a later one, so `side` may create dataflows.
    ///
    /// Until it may, the calling thread is registered to be unparked by the other side's next
    /// arrival. A thread that parks after a `false` therefore misses no arrival.
    pub(crate) fn passable(&self, side: BarrierSide, position: u64) -> bool {
        let mut state = self.state.lock().expect("reconcile barrier poisoned");
        let passable = state.arrived[side.other().index()].is_some_and(|other| other >= position);
        state.waiting[side.index()] = (!passable).then(std::thread::current);
        passable
    }

    /// The position of the latest connection whose drops `side` finished. Test-only.
    #[cfg(test)]
    pub(crate) fn arrived(&self, side: BarrierSide) -> Option<u64> {
        self.state
            .lock()
            .expect("reconcile barrier poisoned")
            .arrived[side.index()]
    }
}

#[cfg(test)]
mod tests;
