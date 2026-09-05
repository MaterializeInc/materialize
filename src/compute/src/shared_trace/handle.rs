// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The reader half: a `Send` trace handle over a publication point, and the import operator.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceReplayInstruction};
use differential_dataflow::trace::cursor::Navigable;
use differential_dataflow::trace::wrappers::frontier::{BatchFrontier, TraceFrontier};
use differential_dataflow::trace::{BatchReader, TraceReader};
use timely::dataflow::Scope;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::operators::generic::source;
use timely::order::TotalOrder;
use timely::progress::Antichain;
use timely::progress::frontier::AntichainRef;

use crate::shared_trace::state::{ImportQueue, SharedTraceRef, seed_frontier};

/// A `Clone + Send` reader of a published arrangement.
///
/// Implements [`TraceReader`], so downstream operators drive its compaction and acquire cursors as
/// with any trace handle. Each clone carries an independent registration, so two consumers of one
/// import cannot release each other's holds.
pub(crate) struct SharedTraceHandle<Tr: TraceReader> {
    pub(super) shared: SharedTraceRef<Tr>,
    /// This handle's logical frontier, and its contribution to `logical_compaction`. Kept locally
    /// both so `get_logical_compaction` can return a borrow and so the setter and `Drop` can adjust
    /// the accumulation by a delta without the point tracking per-handle state.
    pub(super) logical: Antichain<Tr::Time>,
    /// This handle's physical frontier, and its contribution to `physical_compaction`. Seeded at
    /// the chain coverage, for the reasons `Self::register_at` gives.
    pub(super) physical: Antichain<Tr::Time>,
}

impl<Tr: TraceReader> SharedTraceHandle<Tr> {
    /// Registers a fresh logical hold at the current published `since` and returns a handle for it.
    pub(super) fn register(shared: SharedTraceRef<Tr>) -> Self {
        let empty = Antichain::new();
        let (since, coverage) = {
            let mut state = shared.state.lock().expect("shared trace poisoned");
            let since = state.since.clone();
            state.move_logical_hold(&empty, &since);
            // The cut floor starts at the chain coverage, NOT at `since`. See `register_at`.
            let coverage = seed_frontier::<Tr>(&state.chain, &state.upper);
            state.move_physical_hold(&empty, &coverage);
            (since, coverage)
        };
        Self {
            shared,
            logical: since,
            physical: coverage,
        }
    }

    /// Registers a hold at `as_of` under a single lock acquisition, failing with the published
    /// `since` when it is already beyond `as_of`. See
    /// [`Published::handle_at`](crate::shared_trace::publish::Published::handle_at).
    pub(super) fn register_at(
        shared: SharedTraceRef<Tr>,
        as_of: &Antichain<Tr::Time>,
    ) -> Result<Self, Antichain<Tr::Time>> {
        let empty = Antichain::new();
        let coverage = {
            let mut state = shared.state.lock().expect("shared trace poisoned");
            if !timely::PartialOrder::less_equal(&state.since, as_of) {
                return Err(state.since.clone());
            }
            state.move_logical_hold(&empty, as_of);
            // The cut floor is the CHAIN COVERAGE, and it is neither `as_of` nor `since`.
            //
            // Not `since`: `since` is a logical frontier, the floor on which times stay
            // distinguishable. Using it here would hold every batch boundary above the controller's
            // read frontier, which stops the spine merging at all.
            //
            // Not `as_of` either, and not because `as_of` is too low to be honoured. It is that no
            // cut ever happens there. `Self::import_snapshot_at` seeds an import with the whole
            // chain and initialises `acknowledged` to that seed's coverage, and `TraceFrontier`
            // advances times rather than cutting, so a batch straddling `as_of` is harmless. Cuts
            // only ever happen at or above the coverage, and only rise from there.
            //
            // Not `upper` either. It leads the coverage (see `seed_frontier`), so it would sit above
            // the seed a reader registering now will get, and permit a merge across the very first
            // frontier that reader cuts at.
            //
            // The coverage is also the only value `get_physical_compaction` may report. A consumer
            // asserts the reported frontier against the coverage it derives from `map_batches`: see
            // `crate::render::join::mz_join_core`, which differential's own `join_core` also carries.
            // An `as_of` legitimately leads the coverage, for an import over a point no publisher
            // has adopted yet, or for a read at a timestamp beyond the index's seal,
            // so reporting `as_of` would abort the worker on a correct import.
            let coverage = seed_frontier::<Tr>(&state.chain, &state.upper);
            state.move_physical_hold(&empty, &coverage);
            coverage
        };
        Ok(Self {
            shared,
            logical: as_of.clone(),
            physical: coverage,
        })
    }
}

impl<Tr: TraceReader> Clone for SharedTraceHandle<Tr> {
    fn clone(&self) -> Self {
        // A clone must be an independent hold: `import` returns `Arranged { trace: handle.clone() }`
        // and `Arranged` is itself `Clone`, so distinct downstream operators drive compaction on
        // distinct clones. Sharing one hold would let the faster operator release the slower one's.
        // This mirrors `TraceAgent::clone`, which registers an independent counted hold.
        {
            let empty = Antichain::new();
            let mut state = self.shared.state.lock().expect("shared trace poisoned");
            state.move_logical_hold(&empty, &self.logical);
            state.move_physical_hold(&empty, &self.physical);
        }
        Self {
            shared: Arc::clone(&self.shared),
            logical: self.logical.clone(),
            physical: self.physical.clone(),
        }
    }
}

impl<Tr: TraceReader> Drop for SharedTraceHandle<Tr> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            let empty = Antichain::new();
            state.move_logical_hold(&self.logical, &empty);
            state.move_physical_hold(&self.physical, &empty);
        }
    }
}

impl<Tr: TraceReader> TraceReader for SharedTraceHandle<Tr>
where
    // `TotalOrder`: `batches_through` stops at the first batch whose lower is beyond the cut and
    // takes every later batch to be past it too, which holds because the chain is totally ordered.
    Tr::Time: TotalOrder,
{
    type Time = Tr::Time;
    type Batch = Tr::Batch;

    fn batches_through(&mut self, upper: AntichainRef<Tr::Time>) -> Option<Vec<Self::Batch>> {
        let state = self.shared.state.lock().expect("shared trace poisoned");
        // NOTE: `Spine::batches_through` asserts that the cut is at or beyond the spine's physical
        // frontier. That precondition does not hold for a shared handle. A local reader is one of
        // the trace's agents, so the spine's frontier is a meet that includes its own hold. A shared
        // reader's hold reaches the spine only through the publisher's joining setter, so the
        // spine's frontier can sit above a cut this reader still legitimately makes while draining
        // a seed. The straddle check below is the guard instead. It is also what catches
        // physical-hold forwarding that merged away a boundary a reader still needs, where removing
        // it would leave a consumer silently double counting updates at times not before its cut.
        //
        // A clean cut of the published chain: all non-empty batches whose upper is not beyond
        // `upper`, and none whose lower is beyond `upper`. Empty batches are dropped, as
        // `Spine::batches_through` does.
        let mut out = Vec::new();
        for batch in state.chain.iter() {
            // A batch whose lower is beyond the cut, and everything after it in the totally
            // ordered chain, lies past `upper`. Empty batches never carry updates to read.
            if timely::PartialOrder::less_equal(&upper, &batch.lower().borrow()) {
                break;
            }
            if !batch.is_empty() {
                // Fail-stop on a batch that straddles the cut (`lower < upper < batch.upper()`),
                // matching `Spine::batches_through`. Returning it would hand back updates at times
                // not before `upper`, corrupting a downstream `cursor_through` consumer such as
                // `join`. The published chain is totally ordered by description, so this cut is
                // clean unless a caller requested a frontier that is not batch-aligned.
                assert!(
                    timely::PartialOrder::less_equal(&batch.upper().borrow(), &upper),
                    "batches_through: upper straddles batch"
                );
                out.push(batch.clone());
            }
        }
        Some(out)
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) {
        // Join rather than overwrite, and report the join, as `TraceAgent` does: a handle's hold is
        // the joint consequence of every frontier it has been asked to hold. Overwriting would let a
        // consumer lower its own hold below a frontier the trace was already told it could compact
        // past, and then `get_logical_compaction` would report a frontier that is not held.
        let next = self.logical.join(&frontier.to_owned());
        let previous = std::mem::replace(&mut self.logical, next);
        let mut state = self.shared.state.lock().expect("shared trace poisoned");
        state.move_logical_hold(&previous, &self.logical);
    }

    fn get_logical_compaction(&mut self) -> AntichainRef<'_, Tr::Time> {
        self.logical.borrow()
    }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, Tr::Time>) {
        // Join, never assign: the hold starts at the chain coverage, and a request below that
        // coverage must not pull it down to where a merge could eat the boundary this reader was
        // seeded with. `join` with the empty antichain is absorbing, which still lets an empty
        // request release.
        let next = self.physical.join(&frontier.to_owned());
        let previous = std::mem::replace(&mut self.physical, next);
        let mut state = self.shared.state.lock().expect("shared trace poisoned");
        state.move_physical_hold(&previous, &self.physical);
    }

    fn get_physical_compaction(&mut self) -> AntichainRef<'_, Tr::Time> {
        self.physical.borrow()
    }

    fn map_batches<F: FnMut(&Self::Batch)>(&self, mut f: F) {
        let state = self.shared.state.lock().expect("shared trace poisoned");
        for batch in state.chain.iter() {
            f(batch);
        }
    }
}

impl<Tr: TraceReader> SharedTraceHandle<Tr>
where
    Tr: 'static,
    Tr::Time: TotalOrder,
    Tr::Batch: Navigable,
{
    /// Imports the published arrangement restricted to `[as_of, until)`, presented at `as_of`.
    ///
    /// This is the port of differential's `TraceAgent::import_frontier_core` onto the shared trace.
    /// It registers a replay queue seeded with the current chain and drains it as the publisher
    /// appends, wrapping the emitted batches in [`BatchFrontier`] and the trace in [`TraceFrontier`],
    /// both advanced to `as_of` and bounded by `until`. Every update at a time not beyond `as_of`
    /// therefore coalesces to `as_of`, so pre-`as_of` retractions cancel and a downstream monotonic
    /// operator sees only the accumulation at `as_of`.
    ///
    /// The wrapper advances times on read, so the shared `Arc` batches are reused as-is, never
    /// re-arranged.
    ///
    /// Requires `scope`'s total peer count (workers-per-process times processes) to equal the
    /// publisher's, panicking otherwise. See `SharedTrace::peers` for why a mismatch cannot be
    /// served.
    ///
    /// The importer registration is owned by the source operator, so dropping the import dataflow
    /// deregisters it and releases its holds even while other handle clones and the reader worker
    /// live on.
    ///
    /// # Why replay rather than a one-shot emit
    ///
    /// The returned [`Arranged`]'s `stream` and `trace` must stay consistent: the trace is the
    /// accumulation of the stream, and their frontiers advance together. A differential join relies
    /// on this (it computes `A.stream x B.trace + B.stream x A.trace`, counting each match once only
    /// when the trace never runs ahead of the stream). Driving the output capability off the replayed
    /// `Frontier` instructions keeps `stream.frontier == trace.upper`. A one-shot emit that shipped
    /// the whole chain and then dropped straight to the empty frontier would leave the pre-populated
    /// shared trace ahead of the stream, and the join would read the same record from both and double
    /// it.
    ///
    /// For a single-time interactive read pass `until = as_of.step_forward()`: the capability then
    /// drops once the trace's frontier passes `as_of`, so the one-shot result completes.
    pub(crate) fn import_snapshot_at<'scope>(
        &self,
        scope: Scope<'scope, Tr::Time>,
        name: &str,
        as_of: Antichain<Tr::Time>,
        until: Antichain<Tr::Time>,
    ) -> Arranged<'scope, TraceFrontier<SharedTraceHandle<Tr>>> {
        assert_eq!(
            scope.peers(),
            self.shared.peers,
            "shared-trace import requires equal total peers (workers_per_process * num_processes)"
        );

        let trace = TraceFrontier::make_from(self.clone(), as_of.borrow(), until.borrow());
        let shared = Arc::clone(&self.shared);
        // The read hold that lives as long as the import.
        //
        // The returned `Arranged`'s own trace is a hold too, but only a consumer that keeps the trace
        // keeps it: `mz_join_core` moves its input traces into its operator, while `as_collection`
        // and the reduce path take the stream and drop the handle during dataflow construction. So
        // for every consumer but a join there would otherwise be no registration left once the
        // dataflow is built, and the publisher would fall back to the writer-driven frontier and
        // compact past the `as_of` the dataflow still reads at.
        //
        // Owning it here rather than in the dataflow's token set is what makes it downgradeable. The
        // publisher forwards the MEET of the registered holds, so a hold nobody downgrades is a floor
        // under every hold that is. This one follows `acknowledged` below.
        let mut hold = Some(self.clone());

        let stream = source(scope, name, move |capability, info| {
            let activator = scope.worker().sync_activator_for(info.address.to_vec());

            // Register under one lock acquisition: seed the queue with the current chain (hint
            // `minimum`, as the local replay does for historical batches) followed by the frontier
            // that chain covers, and publish it weakly. Later batches append; earlier ones are
            // seeded. Nothing is missed or duplicated.
            //
            // `queue` is the only strong reference. It is captured by the operator closure below, so
            // dropping the import dataflow deregisters this importer.
            let (queue, seed) = {
                let mut state = shared.state.lock().expect("shared trace poisoned");
                let mut instructions = VecDeque::new();
                for batch in state.chain.iter() {
                    instructions.push_back(TraceReplayInstruction::Batch(
                        batch.clone(),
                        Some(<Tr::Time as timely::progress::Timestamp>::minimum()),
                    ));
                }
                let seed = seed_frontier::<Tr>(&state.chain, &state.upper);
                instructions.push_back(TraceReplayInstruction::Frontier(seed.clone()));
                // If the publisher already closed, its one-shot terminal frontier has been and gone,
                // so seed our own. Otherwise a late importer would drain the chain and then wait
                // forever for a frontier that never arrives, leaking its capability.
                if state.closed {
                    instructions.push_back(TraceReplayInstruction::Frontier(Antichain::new()));
                }
                let queue = Arc::new(ImportQueue {
                    instructions: Mutex::new(instructions),
                    activator,
                });
                state.queues.push(Arc::downgrade(&queue));
                (queue, seed)
            };

            let mut capabilities = Some(CapabilitySet::new());
            capabilities.as_mut().unwrap().insert(capability);
            let mut acknowledged = seed.clone();
            // The seeded instructions come first and are emitted as-is. Everything after the seed's
            // own `Frontier` is a live instruction from the publisher, and is filtered against
            // `seed` below.
            let mut draining_seed = true;

            move |output| {
                // Drains this importer's own queue only. The publication point's state lock, which
                // the publisher holds while it rebuilds the chain, is not on this path.
                let drained: Vec<_> = {
                    let mut instructions =
                        queue.instructions.lock().expect("import queue poisoned");
                    instructions.drain(..).collect()
                };

                if let Some(caps) = capabilities.as_mut() {
                    for instruction in drained {
                        match instruction {
                            TraceReplayInstruction::Frontier(frontier) => {
                                // The publisher's instructions carry the stream frontier, which lags
                                // the chain coverage seeded at registration by up to a scheduling
                                // round. Skip the ones that do not advance what we already hold: a
                                // capability set cannot be downgraded backwards, and the seeded
                                // coverage is already correct.
                                if !timely::PartialOrder::less_equal(&acknowledged, &frontier) {
                                    continue;
                                }
                                acknowledged = frontier.clone();
                                draining_seed = false;
                                // Follow the stream with the read hold. Everything at or below
                                // `acknowledged` has been delivered and will never be replayed, so this
                                // import will not read there again and the publisher is free to
                                // compact behind it.
                                //
                                // Because the setter joins, this never lowers the hold while the seed
                                // is draining: the seeded coverage can already lead `as_of`, and the
                                // hold must stay at `as_of` until the stream really passes it.
                                //
                                // This is the import's own obligation only. A consumer that reads the
                                // returned trace rather than the stream needs accuracy at times its own
                                // progress governs, which can lag this, and it holds its own separate
                                // registration for exactly that. The publisher forwards the meet, so the
                                // slower of the two wins.
                                if let Some(hold) = hold.as_mut() {
                                    hold.set_logical_compaction(acknowledged.borrow());
                                    // Both axes. `acknowledged` is exactly the frontier below which
                                    // this import will never cut again, which is what the physical
                                    // hold expresses. Without this the hold pins the spine at the
                                    // coverage it registered at and the chain grows one batch per
                                    // seal for the life of the import.
                                    hold.set_physical_compaction(acknowledged.borrow());
                                }
                                // Bound the read at `until`: once the trace's frontier reaches it, drop
                                // the capability so a single-time read completes. Otherwise track the
                                // trace's `upper`, keeping the stream frontier equal to the trace upper
                                // (the consistency the join depends on). The empty frontier is the
                                // publisher's terminal signal and likewise drops the capability.
                                if frontier.is_empty()
                                    || timely::PartialOrder::less_equal(&until, &frontier)
                                {
                                    capabilities = None;
                                    // The read is over, so stop holding the trace back. A consumer of
                                    // the returned trace still holds its own registration.
                                    hold = None;
                                    break;
                                }
                                caps.downgrade(&frontier.borrow()[..]);
                            }
                            TraceReplayInstruction::Batch(batch, hint) => {
                                // The seed and the live instructions come from different places:
                                // the seed is the trace's chain at registration, the live ones are
                                // batches off the arrangement stream. The trace seals a batch a
                                // scheduling round before the stream delivers it, so a batch in the
                                // seed can arrive again as a live instruction. Emitting it twice
                                // would double count it, and its hint sits below the frontier the
                                // seed already claimed, which `delayed` panics on.
                                if !draining_seed
                                    && timely::PartialOrder::less_equal(
                                        &batch.upper().borrow(),
                                        &seed.borrow(),
                                    )
                                {
                                    continue;
                                }
                                if let Some(time) = hint {
                                    if !batch.is_empty() {
                                        // Emit under a capability delayed to the batch's hint. The
                                        // `BatchFrontier` wrapper advances times to `as_of` and drops
                                        // times at or beyond `until` on read, so the stream presents
                                        // the same `[as_of, until)` view as the trace.
                                        let cap = caps.delayed(&time);
                                        output.session(&cap).give(BatchFrontier::make_from(
                                            batch,
                                            as_of.borrow(),
                                            until.borrow(),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        Arranged { stream, trace }
    }
}
