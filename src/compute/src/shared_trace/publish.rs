// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The publisher half: the publication point's owner-facing API and the operator that refreshes it.

use std::sync::Arc;

use differential_dataflow::lattice::{Lattice, antichain_meet};
use differential_dataflow::operators::arrange::{Arranged, TraceAgent, TraceReplayInstruction};
use differential_dataflow::trace::TraceReader;
use timely::dataflow::operators::generic::Operator;
use timely::progress::Antichain;

use crate::shared_trace::handle::SharedTraceHandle;
use crate::shared_trace::state::{SharedTrace, SharedTraceRef, seed_frontier};

/// Why a publication point refused an `as_of`.
///
/// Read off the point rather than off a handle, so a failure path registers no hold on its way to a
/// panic, and so the caller reports the point that actually refused rather than a sibling. The
/// refusing `since` is already in [`Published::handle_at`]'s `Err`, so it is not repeated here.
pub(crate) struct Diagnostics<T> {
    /// The controller's last `AllowCompaction` frontier, or `None` if none has arrived.
    ///
    /// Distinguishes a `since` the controller drove from one the publisher's own hold drove, which
    /// is what tells a protocol-ordering violation apart from a local compaction bug.
    pub(crate) writer_logical: Option<Antichain<T>>,
    /// The frontier the importing runtime has applied.
    ///
    /// A refusal with this AT the refusing `since` means that runtime had already applied the
    /// compaction before it built the importing dataflow, and no replica-side hold could have
    /// prevented it. BELOW that `since` means the publisher escaped its own bound, which is a bug
    /// here rather than upstream.
    pub(crate) standing_hold: Antichain<T>,
}

/// The result of publishing an arrangement. Holding it keeps the publication point registered;
/// dropping it does not stop the publisher (the publisher lives with its dataflow), but no further
/// handles can be minted from it.
pub(crate) struct Published<Tr: TraceReader> {
    pub(super) shared: SharedTraceRef<Tr>,
}

impl<Tr: TraceReader> Published<Tr> {
    /// Hands out a `Clone + Send` handle to the published arrangement.
    ///
    /// The handle registers a logical hold at the current published `since`, so the arrangement
    /// will not compact past it until the handle (and all its clones) drop.
    pub(crate) fn handle(&self) -> SharedTraceHandle<Tr> {
        SharedTraceHandle::register(Arc::clone(&self.shared))
    }

    /// Hands out a handle whose hold is registered at `as_of`, failing when the published `since` is
    /// already beyond it.
    ///
    /// This is the mint a reader that intends to read at `as_of` must use. Observing `since`,
    /// deciding it permits `as_of`, and then advancing a hold are three separate acquisitions of the
    /// state lock, and the publisher can advance `since` between any two of them. Checking and
    /// registering under one acquisition means a returned handle's hold is one the trace can still
    /// honour, so a caller never holds a frontier the arrangement has compacted past.
    ///
    /// `Err` carries the published `since` that ruled `as_of` out. That is a protocol-ordering
    /// failure rather than a serving failure, since the controller promises an index's `since` never
    /// passes the `as_of` of a dataflow importing it, so callers report it loudly rather than
    /// degrading.
    pub(crate) fn handle_at(
        &self,
        as_of: &Antichain<Tr::Time>,
    ) -> Result<SharedTraceHandle<Tr>, Antichain<Tr::Time>> {
        SharedTraceHandle::register_at(Arc::clone(&self.shared), as_of)
    }

    /// Creates a publication point. It starts unbacked: an empty chain with `since` and `upper` at
    /// the minimum time and no publisher, until one attaches via [`PublishArrangement::adopt`].
    ///
    /// A reader may mint handles ([`Self::handle`]) and build imports over it before that happens,
    /// but they produce nothing (the import frontier stays at the minimum) until adoption begins the
    /// refresh loop. Adoption fills the same `Arc`, so a handle captured by value at construction (as
    /// a differential join captures its input trace) observes the filled chain: the handle is a live
    /// proxy into the shared state, not a snapshot.
    ///
    /// `peers` must equal the total peer count of the scope that later adopts the point, the same
    /// invariant [`SharedTraceHandle::import_snapshot_at`] enforces.
    pub(crate) fn new(peers: usize) -> Self {
        Published {
            shared: Arc::new(SharedTrace::new_empty(peers)),
        }
    }

    /// Why this point would refuse an `as_of`. See [`Diagnostics`].
    pub(crate) fn diagnostics(&self) -> Diagnostics<Tr::Time> {
        let state = self.shared.state.lock().expect("shared trace poisoned");
        Diagnostics {
            writer_logical: state.writer_logical.clone(),
            standing_hold: state.standing_hold.clone(),
        }
    }

    /// Records the controller's logical compaction frontier for this arrangement.
    ///
    /// The publisher reads it to publish `since`, which is the meet of the trace's agents. Called
    /// from `handle_allow_compaction` through the registry.
    pub(crate) fn note_writer_logical(&self, frontier: &Antichain<Tr::Time>) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.writer_logical = Some(frontier.clone());
        }
    }

    /// Advances the standing hold to `frontier`, recording that the runtime which may import this
    /// arrangement has applied the controller's compaction that far.
    ///
    /// Joins rather than assigning, so a reordered or replayed command cannot lower a bound the
    /// publisher already forwarded, which its agent's own joining setter could not honour anyway.
    pub(crate) fn note_standing_hold(&self, frontier: &Antichain<Tr::Time>) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.advance_standing_hold(frontier);
        }
    }
}

/// Publishes an [`Arranged`] arrangement through a publication point on its owning worker.
///
/// Materialize cannot add inherent methods to differential's foreign `Arranged` type, so it exposes
/// them as this extension trait instead. Bring it into scope at a call site to use
/// `arranged.adopt(...)`.
pub(crate) trait PublishArrangement<Tr: TraceReader> {
    /// Installs this arrangement's publisher into `point`, created by [`Published::new`].
    ///
    /// Attaches a publisher operator to the arrangement stream, named `PublishShared({name})`. On
    /// each activation the publisher refreshes the published chain, `since`, and `upper` from the
    /// trace, appends newly arrived batches to importer queues, and forwards the accumulated holds
    /// to the trace's compaction.
    ///
    /// Adoption is late-binding: a reader may build handles and imports over `point` before this
    /// arrangement is rendered. Those imports produce nothing (their frontier stays at the minimum)
    /// until adoption begins the refresh loop, at which point the already-registered importer queues
    /// fill from the same publisher iteration that serves any later-registered reader.
    ///
    /// Requires the adopting scope's total peer count to equal `point`'s, panicking otherwise.
    ///
    /// `on_seal` fires once per activation on which the published `upper` advances, after the state
    /// lock is released and `upper` reflects the advance. A fast-path peek parked on this
    /// arrangement's seal is re-examined only through this callback, so it must observe the advanced
    /// `upper`. See the lost-wakeup contract on
    /// `crate::sharing::ArrangementSharingRegistry::notify`.
    fn adopt<F: Fn() + 'static>(&self, point: &Published<Tr>, name: &str, on_seal: F);
}

impl<'scope, Tr> PublishArrangement<Tr> for Arranged<'scope, TraceAgent<Tr>>
where
    Tr: differential_dataflow::trace::Trace + 'static,
    Tr::Batch: Send + Sync,
    Tr::Time: Sync,
{
    fn adopt<F: Fn() + 'static>(&self, point: &Published<Tr>, name: &str, on_seal: F) {
        assert_eq!(
            self.stream.scope().peers(),
            point.shared.peers,
            "adopt requires equal total peers (workers_per_process * num_processes)"
        );

        // The publisher owns a `TraceAgent` clone: its read capability is the aggregate lease for
        // all readers, so the trace cannot compact or drop out from under them.
        let mut agent = self.trace.clone();

        // Stands in for the writer's frontier until the controller's first `AllowCompaction` arrives,
        // captured ONCE at adoption. Re-reading it from the agent each activation would close a
        // feedback loop, since the agent's own hold is driven up from the accumulated holds: the
        // published `since` would chase the readers' `as_of`s and refuse a later read at an earlier
        // time, which no writer ever asked for.
        let initial_logical = agent.get_logical_compaction().to_owned();

        // Seed the standing hold at the same floor. The importing runtime may not have applied any
        // compaction for this collection yet, and until it has, this is the frontier the publisher may
        // compact to: the controller offers no `as_of` below a collection's own `since`, so no importer
        // can need a frontier below it. Without this seed a point created before adoption would
        // hold at the minimum time and stop the arrangement compacting at all.
        {
            let mut state = point.shared.state.lock().expect("shared trace poisoned");
            state.advance_standing_hold(&initial_logical);
        }

        let publisher = Publisher {
            shared: Arc::clone(&point.shared),
        };

        let sink_shared = Arc::clone(&point.shared);
        self.stream.clone().sink(
            timely::dataflow::channels::pact::Pipeline,
            &format!("PublishShared({name})"),
            move |(input, frontier)| {
                // Keep `publisher` alive with the operator, so operator (dataflow) drop closes the
                // publication point.
                let _publisher = &publisher;

                // Batches arriving on the stream, each with a capability time that lower-bounds the
                // batch's updates. Empty seal batches do not travel the stream; they are picked up
                // from the trace below.
                let mut arrived: Vec<(Tr::Batch, Tr::Time)> = Vec::new();
                input.for_each(|cap, data| {
                    let hint = cap.time().clone();
                    for batch in data.drain(..) {
                        arrived.push((batch, hint.clone()));
                    }
                });

                // The stream frontier is the authoritative upper. It never leads the batches
                // delivered on the stream, unlike the trace's `map_batches` upper, which can run
                // ahead within a worker step and strand the importer's capability below a
                // not-yet-emitted batch.
                let upper = frontier.frontier().to_owned();

                // Seed every importer with the full trace snapshot: forward all batches, do NOT gate
                // on the stream frontier. The stream frontier lags the trace by a scheduling round
                // (the batch data is delivered, the frontier notification catches up a round later),
                // so a `batch.upper() <= stream_frontier` gate wrongly drops batches whose data has
                // already been sealed. When the Spine has merged an old batch and a leading one into
                // a single batch whose upper leads the frontier, that gate drops the whole batch,
                // stranding its historical part and leaving a late importer's snapshot missing rows.
                // Forwarding all batches costs only momentary memory, since batches are Arc-shared.
                // It cannot double-count: the stream emits each original batch once and never
                // re-emits a merged batch, so future `arrived` batches never carry what the seed
                // already holds. The stream frontier still drives the published `upper` and the
                // incremental `Frontier` instructions below, which is where it is authoritative.
                let mut chain = Vec::new();
                agent.map_batches(|batch| chain.push(batch.clone()));
                // Publishing carries no compaction floor of its own: in Materialize the controller
                // drives `since` through the maintained trace's own handle. The publisher keeps a
                // holding agent solely so the shared holds have somewhere to forward to, so its hold
                // FOLLOWS them rather than pinning the trace.
                let publisher_logical = agent.get_logical_compaction().to_owned();

                let (logical_target, physical_target, upper_advanced) = {
                    let mut state = sink_shared.state.lock().expect("shared trace poisoned");

                    let queues = state.live_queues();
                    for (batch, hint) in arrived.drain(..) {
                        for queue in &queues {
                            queue.push(TraceReplayInstruction::Batch(
                                batch.clone(),
                                Some(hint.clone()),
                            ));
                        }
                    }

                    let upper_advanced = state.upper != upper;
                    if upper_advanced {
                        for queue in &queues {
                            queue.push(TraceReplayInstruction::Frontier(upper.clone()));
                        }
                    }

                    // The other agent's hold on this trace: the controller's last `AllowCompaction`,
                    // forwarded into the slot by
                    // `crate::sharing::ArrangementSharingRegistry::note_allow_compaction`, or the
                    // frontier at adoption until the first one arrives.
                    let writer_logical = state
                        .writer_logical
                        .clone()
                        .unwrap_or_else(|| initial_logical.clone());
                    // The standing hold is one of the accumulated holds, so this frontier is at or
                    // below it and the arrangement compacts only as fast as the slowest runtime's
                    // command stream.
                    let held = state.logical_compaction.frontier().to_owned();
                    // An empty accumulation means every hold released, the standing one included.
                    // Forwarding it would tell the agent to compact everything, and the agent's
                    // joining setter could never take that back, so leave the agent where it is and
                    // let the dataflow's own drop release the trace.
                    let logical = if held.is_empty() {
                        publisher_logical.clone()
                    } else {
                        held
                    };

                    state.chain = chain;
                    state.upper = upper;
                    // The trace's real logical compaction, which is the meet over its agents: the
                    // publisher's hold once it has joined `logical` in below, and the writer's.
                    // Publishing exactly that keeps the gate a reader checks in step with the trace,
                    // and never below it, so a handle registering in this window cannot latch a
                    // `since` claiming accuracy at already-merged times.
                    let publisher_after = publisher_logical.join(&logical);
                    state.since =
                        antichain_meet(&publisher_after.borrow()[..], &writer_logical.borrow()[..]);
                    // `set_physical_compaction(F)` lets the Spine merge batches whose upper is at or
                    // below `F`, destroying the boundaries there. A reader needs a boundary at every
                    // frontier it passes to `cursor_through`, which is why its floor is forwarded
                    // rather than discarded, and why this is a correctness mechanism rather than a
                    // tuning knob.
                    //
                    // With no reader registered, forward the chain coverage: nobody cuts below it,
                    // and holding anything lower would stop the spine merging for a shared
                    // arrangement that nothing reads.
                    let physical = state.physical_compaction.frontier().to_owned();
                    let physical = if physical.is_empty() {
                        seed_frontier::<Tr>(&state.chain, &state.upper)
                    } else {
                        physical
                    };

                    // Wake importers and any peek waiters.
                    for queue in &queues {
                        queue.activate();
                    }

                    (logical, physical, upper_advanced)
                };

                // Apply compaction to the agent OUTSIDE the lock: `set_physical_compaction` can run
                // an unbounded merge synchronously, which must not block concurrent readers.
                agent.set_logical_compaction(logical_target.borrow());
                agent.set_physical_compaction(physical_target.borrow());

                // Wake fast-path peeks parked on this arrangement's seal, AFTER the state lock above
                // is released and `state.upper` reflects the advance. The registry wake takes the
                // `wakers` lock, and a reader takes `wakers` then the trace `state` lock, so firing it
                // while holding `state` would invert that order and can deadlock. Firing it here, past
                // the advance, is also what a peek the wake re-examines needs: it reads the advanced
                // `upper` and completes. An upstream stream tap would fire before this advance, so the
                // peek would read a stale upper and never be re-woken once this was the last advance.
                if upper_advanced {
                    on_seal();
                }
            },
        );
    }
}

/// Guard that marks the publication point closed when the publisher operator drops, waking readers
/// so they drain and shut down.
struct Publisher<Tr: TraceReader> {
    pub(super) shared: SharedTraceRef<Tr>,
}

impl<Tr: TraceReader> Drop for Publisher<Tr> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.closed = true;
            let empty = Antichain::new();
            for queue in state.live_queues() {
                queue.push(TraceReplayInstruction::Frontier(empty.clone()));
                queue.activate();
            }
        }
    }
}
