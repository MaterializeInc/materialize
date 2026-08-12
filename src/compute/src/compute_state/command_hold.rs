// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Read holds one compute runtime keeps on behalf of the other one in the same process.
//!
//! A dataflow on the interactive runtime importing an index the maintenance runtime maintains needs
//! that index held at its `as_of`. It cannot install the hold itself: the hold has to be a handle on
//! the maintenance trace, and a `TraceAgent` is neither `Send` nor reachable across the runtime
//! boundary. So the multiplexer synthesizes a `ComputeCommand::AcquireHolds` onto the maintenance
//! runtime's command stream, which is also the stream that carries the `AllowCompaction` the hold has
//! to precede, and the maintenance worker installs the hold from here.
//!
//! ## Why not the publication point's own hold mechanism
//!
//! A reader registered against a publication point records a *request*, and the publisher forwards
//! the meet of the requests into its own agent. That agent's setter joins, so it only ever advances,
//! and a request below where it already sits cannot be honoured. That is exactly the case here: the
//! controller has been allowing compaction on this index for as long as it has existed, so the
//! publisher's agent sits at the controller's frontier, while a new reader must be admitted at an
//! `as_of` at or below it.
//!
//! A hold acquired here escapes that because it clones from `ComputeState::traces`, whose handle sits
//! at the controller's read frontier. The controller holds that frontier at or below every `as_of` it
//! may offer, so the clone starts low enough to be set to the `as_of` and the pin is real. It is
//! recorded in the publication point through `Published::acquire_command_hold` only so the published
//! `since` reflects it, which is what admits the reader.

use std::sync::Arc;

use differential_dataflow::lattice::{Lattice, antichain_meet};
use differential_dataflow::trace::TraceReader;
use mz_repr::Timestamp;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;

use crate::arrangement::manager::TraceBundle;
use crate::sharing::SharedIndexArrangement;

/// A read hold on one collection, kept for a dataflow the other compute runtime renders.
///
/// Holding it pins the collection's trace at [`Self::at`]; dropping it releases the pin.
pub struct CommandHold {
    /// The frontier the hold was acquired at, and the floor it never downgrades below.
    ///
    /// The importing dataflow may not have been built yet, and until it has there is nothing whose
    /// progress could justify moving off this frontier.
    as_of: Antichain<Timestamp>,
    /// Where the hold currently sits, equal to what `traces` holds and to what `slot` publishes.
    at: Antichain<Timestamp>,
    /// Trace handles pinning the collection. Their `Drop` releases the pin.
    ///
    /// Logical only. `TraceAgent::clone` inherits the cloner's physical hold as well, and a physical
    /// hold nobody downgrades blocks batch merging for as long as it lives, so
    /// [`CommandHold::acquire`] releases the inherited one. Logical compaction is what coalesces
    /// times, so holding it back is what keeps a read at `at` accurate. Physical granularity for the
    /// reader comes from the publisher, which forwards the published `since` as its physical target.
    traces: TraceBundle,
    /// The publication point, so the pin is visible in the `since` a reader gates on.
    slot: Arc<SharedIndexArrangement>,
    /// The `oks` hold's id in `slot`.
    oks_hold: usize,
    /// The `errs` hold's id in `slot`.
    errs_hold: usize,
}

impl CommandHold {
    /// Pins `base` at `as_of` and records the pin in `slot`.
    ///
    /// `Err` carries `base`'s compaction frontier when it is already beyond `as_of`, which no clone
    /// of it can represent. That is a protocol-ordering failure rather than a serving failure: the
    /// controller promises a collection's read frontier never passes the `as_of` of a dataflow
    /// importing it, so this means either the acquisition was ordered after a compaction it should
    /// have preceded, or the controller offered an `as_of` it had not held.
    pub fn acquire(
        base: &mut TraceBundle,
        slot: Arc<SharedIndexArrangement>,
        as_of: Antichain<Timestamp>,
    ) -> Result<Self, Antichain<Timestamp>> {
        let current = base.compaction_frontier();
        if !PartialOrder::less_equal(&current, &as_of) {
            return Err(current);
        }

        let mut traces = TraceBundle::new(base.oks_mut().clone(), base.errs_mut().clone());

        // Release the physical hold the clone inherited before taking the logical one. Both setters
        // join, so the physical hold could never be lowered again once kept, and it would pin batch
        // granularity at whatever `TraceManager::maintenance` had most recently set. The empty
        // antichain is the top of the lattice, so joining onto it releases outright.
        let released = Antichain::new();
        traces.oks_mut().set_physical_compaction(released.borrow());
        traces.errs_mut().set_physical_compaction(released.borrow());
        traces.oks_mut().set_logical_compaction(as_of.borrow());
        traces.errs_mut().set_logical_compaction(as_of.borrow());

        // Record after pinning, so the published `since` never claims a frontier the trace does not
        // yet hold.
        let oks_hold = slot.oks.acquire_command_hold(&as_of);
        let errs_hold = slot.errs.acquire_command_hold(&as_of);

        Ok(Self {
            at: as_of.clone(),
            as_of,
            traces,
            slot,
            oks_hold,
            errs_hold,
        })
    }

    /// Follows the importing dataflow's own progress, and returns the frontier the hold now sits at
    /// if it moved.
    ///
    /// The target is the meet of the publication point's reader registrations, floored at the
    /// acquisition frontier. That floor is what makes this safe without knowing which registrations
    /// belong to the dataflow this hold was acquired for: the meet is at or below every registration,
    /// so flooring it at `as_of` keeps the hold at or below the frontier this dataflow's own
    /// registration holds. A dataflow on another holder that is further behind therefore delays this
    /// hold's downgrade, but nothing can advance it past its own reader.
    ///
    /// Not downgrading at all would be a permanent pin. An interactive `SUBSCRIBE` lives for as long
    /// as its client, and a hold frozen at the `as_of` it started from would stop the index it reads
    /// from ever compacting again.
    pub fn downgrade(&mut self) -> Option<Antichain<Timestamp>> {
        // Both points must have a registration. `None` means the importing dataflow has not been
        // built yet, so there is no reader whose progress could justify a downgrade. Requiring both
        // rather than reading only `oks` keeps the hold behind whichever side lags.
        let (Some(oks), Some(errs)) = (
            self.slot.oks.reader_hold_meet(),
            self.slot.errs.reader_hold_meet(),
        ) else {
            return None;
        };
        let readers = antichain_meet(&oks.borrow()[..], &errs.borrow()[..]);
        let target = self.as_of.join(&readers);
        if !PartialOrder::less_than(&self.at, &target) {
            return None;
        }

        // Record before advancing the handles, so the published `since` leads the trace rather than
        // lagging it. A `since` that lagged would admit a reader below what the trace still holds.
        self.slot.oks.downgrade_command_hold(self.oks_hold, &target);
        self.slot
            .errs
            .downgrade_command_hold(self.errs_hold, &target);
        self.traces
            .oks_mut()
            .set_logical_compaction(target.borrow());
        self.traces
            .errs_mut()
            .set_logical_compaction(target.borrow());
        self.at = target.clone();
        Some(target)
    }

    /// The frontier the hold currently sits at.
    #[cfg(test)]
    pub fn frontier(&self) -> &Antichain<Timestamp> {
        &self.at
    }
}

impl Drop for CommandHold {
    fn drop(&mut self) {
        // Deregister before `traces` drops, which happens after this body: the published `since`
        // must give up the frontier before the trace does, never after. The reverse order leaves a
        // window in which `since` claims a frontier nothing holds.
        self.slot.oks.release_command_hold(self.oks_hold);
        self.slot.errs.release_command_hold(self.errs_hold);
    }
}
