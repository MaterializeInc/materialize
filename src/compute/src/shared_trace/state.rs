// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The state a publisher and its readers share, and the frontier arithmetic over it.
//!
//! Everything here is reached by both halves of the module:
//! [`crate::shared_trace::publish`] writes the chain and the frontiers,
//! [`crate::shared_trace::handle`] reads them and registers holds. The publication point's fields
//! are `pub(super)` for exactly that reason and for no other.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::TraceReplayInstruction;
use differential_dataflow::trace::{BatchReader, TraceReader};
use timely::progress::Antichain;
use timely::progress::frontier::MutableAntichain;
use timely::scheduling::activate::SyncActivator;

/// The queue and wakeup for one importer registered against a publication point.
///
/// The importer owns the only strong reference, so dropping its source operator deregisters it and
/// the publisher prunes the dangling `Weak` on its next activation. Mirrors `TraceAgent`'s listener
/// queues, which are likewise owned by the reader and held weakly by the writer.
///
/// The queue carries its own lock rather than living inside `SharedTraceState`, so an importer
/// draining on the reader's worker never contends with the publisher rebuilding the chain. The
/// publisher takes the state lock and then a queue's; an importer takes only a queue's.
pub(super) struct ImportQueue<Tr: TraceReader> {
    /// Replay instructions the publisher appends and the importer drains, mirroring the local
    /// arrange replay queue. Batches carry a hint time that lower-bounds their updates.
    pub(super) instructions: Mutex<VecDeque<TraceReplayInstruction<Tr>>>,
    /// Wakes the importer's source operator on the reader's worker when new instructions arrive.
    pub(super) activator: SyncActivator,
}

impl<Tr: TraceReader> ImportQueue<Tr> {
    /// Appends `instruction` and wakes the importer.
    pub(super) fn push(&self, instruction: TraceReplayInstruction<Tr>) {
        let mut instructions = self.instructions.lock().expect("import queue poisoned");
        instructions.push_back(instruction);
    }

    /// Wakes the importer's source operator.
    pub(super) fn activate(&self) {
        let _ = self.activator.activate();
    }
}

/// State shared between a publisher (on the owning worker) and its readers (on any thread).
///
/// The `chain`, `since`, and `upper` are always updated together under the lock, so every reader
/// observes a frontier-consistent view.
pub(super) struct SharedTraceState<Tr: TraceReader> {
    /// The published chain, sourced from `map_batches`: contiguous descriptions including the
    /// seal-only empty batches that never travel on the arrangement stream. Coverage is at least
    /// `upper` (within a worker step it may briefly run ahead, never behind).
    pub(super) chain: Vec<Tr::Batch>,
    /// Logical compaction frontier of the published view. Reads at times not beyond `since` are not
    /// accurate. A snapshot must pick a time at or beyond it.
    ///
    /// Written only by the publisher, as the meet of its agent's post-forward hold and the
    /// writer-driven frontier. That is the trace's real logical compaction, since those are the only
    /// agents on it.
    pub(super) since: Antichain<Tr::Time>,
    /// Seal frontier: the join of the chain's batch uppers. Batches strictly below `upper` are
    /// complete and readable.
    pub(super) upper: Antichain<Tr::Time>,
    /// Accumulated logical holds: every reader registration plus `standing_hold`. The publisher
    /// forwards this frontier to its agent.
    pub(super) logical_compaction: MutableAntichain<Tr::Time>,
    /// Accumulated physical holds: the lowest frontier each reader may still cut at.
    ///
    /// This is the path a shared reader's request travels, and a local consumer needs no equivalent:
    /// `crate::render::join::mz_join_core` is an agent on its own trace, so its
    /// `set_physical_compaction(acknowledged)` reaches the `TraceBox` directly.
    pub(super) physical_compaction: MutableAntichain<Tr::Time>,
    /// The controller's last logical compaction frontier for this arrangement, forwarded from
    /// `handle_allow_compaction` via `crate::sharing::ArrangementSharingRegistry::note_allow_compaction`.
    /// `None` until the first `AllowCompaction` arrives.
    ///
    /// Not a hold here: it is another agent's hold on the same trace, so it belongs to the trace's
    /// own meet. The publisher reads it to publish `since`, which is that meet.
    pub(super) writer_logical: Option<Antichain<Tr::Time>>,
    /// Logical hold at the compaction frontier the runtime that may import this arrangement has
    /// applied.
    ///
    /// The two runtimes drain their command streams independently, so the owning runtime can apply a
    /// compaction the importing one has not. An importing dataflow whose `CreateDataflow` is still
    /// queued there has registered no hold, and would be built against an arrangement already
    /// compacted past its `as_of`. This hold forbids that: a shared arrangement compacts only as fast
    /// as the slowest runtime's stream position.
    ///
    /// Joins, so it only ever rises.
    /// [`PublishArrangement::adopt`](crate::shared_trace::publish::PublishArrangement::adopt) seeds it.
    pub(super) standing_hold: Antichain<Tr::Time>,
    /// Live importer queues. Entries whose importer has dropped are pruned when the publisher next
    /// walks them.
    pub(super) queues: Vec<Weak<ImportQueue<Tr>>>,
    /// Set when the publisher drops. A terminal empty frontier is enqueued to each importer, so
    /// readers close only after draining what was already published.
    pub(super) closed: bool,
}

impl<Tr: TraceReader> SharedTraceState<Tr> {
    /// Moves a logical hold from `previous` to `next`.
    ///
    /// The empty antichain means "compaction is permitted everywhere", so passing it as `next`
    /// releases. The reduce operator does exactly that on every dataflow whose input finishes: it
    /// forwards `upper_limit` to its source trace, and `upper_limit` is the join of the input
    /// frontiers, which empties when the input does.
    pub(super) fn move_logical_hold(
        &mut self,
        previous: &Antichain<Tr::Time>,
        next: &Antichain<Tr::Time>,
    ) {
        adjust(&mut self.logical_compaction, previous, next);
    }

    /// Moves a physical hold from `previous` to `next`.
    ///
    /// An empty `next` means this reader will never cut again, so it stops constraining where the
    /// spine may merge.
    pub(super) fn move_physical_hold(
        &mut self,
        previous: &Antichain<Tr::Time>,
        next: &Antichain<Tr::Time>,
    ) {
        adjust(&mut self.physical_compaction, previous, next);
    }

    /// The live importer queues, dropping entries whose importer has gone.
    ///
    /// Returns strong references so the caller can append and activate after releasing the state
    /// lock, which is what keeps an importer's own drain off this lock.
    pub(super) fn live_queues(&mut self) -> Vec<Arc<ImportQueue<Tr>>> {
        let mut live = Vec::with_capacity(self.queues.len());
        self.queues.retain(|weak| match weak.upgrade() {
            Some(queue) => {
                live.push(queue);
                true
            }
            None => false,
        });
        live
    }

    /// Advances the standing hold to its join with `frontier`.
    pub(super) fn advance_standing_hold(&mut self, frontier: &Antichain<Tr::Time>) {
        let next = self.standing_hold.join(frontier);
        let previous = std::mem::replace(&mut self.standing_hold, next);
        adjust(&mut self.logical_compaction, &previous, &self.standing_hold);
    }
}

/// Replaces the elements of `lower` with those of `upper` in `accumulated`.
///
/// The delta form differential's `TraceBox` uses, so the accumulated frontier costs the times that
/// changed rather than a walk over every hold.
pub(super) fn adjust<T: timely::progress::Timestamp>(
    accumulated: &mut MutableAntichain<T>,
    lower: &Antichain<T>,
    upper: &Antichain<T>,
) {
    accumulated.update_iter(upper.iter().cloned().map(|time| (time, 1)));
    accumulated.update_iter(lower.iter().cloned().map(|time| (time, -1)));
}

/// A publication point: the shared state every reader of one published arrangement sees.
pub(super) struct SharedTrace<Tr: TraceReader> {
    pub(super) state: Mutex<SharedTraceState<Tr>>,
    /// Total peer count (workers-per-process times processes) of the scope that published this
    /// arrangement. Set once at publish time and never mutated afterward, so `import` reads it
    /// without taking `state`'s lock. Pairwise import (importer worker `i` reads publisher worker
    /// `i`) is sound only when an importing scope shards keys the same way, which requires this to
    /// match the importing scope's own `peers()`.
    pub(super) peers: usize,
}

pub(super) type SharedTraceRef<Tr> = Arc<SharedTrace<Tr>>;

impl<Tr: TraceReader> SharedTrace<Tr> {
    /// A fresh publication point: empty chain, `since` and `upper` at the minimum time, no reader
    /// holds, no publisher attached.
    ///
    /// Used by [`Published::new`](crate::shared_trace::publish::Published::new), which leaves the point
    /// unbacked for a later
    /// [`PublishArrangement::adopt`](crate::shared_trace::publish::PublishArrangement::adopt).
    pub(super) fn new_empty(peers: usize) -> Self {
        let minimum = Antichain::from_elem(<Tr::Time as timely::progress::Timestamp>::minimum());
        SharedTrace {
            state: Mutex::new(SharedTraceState {
                chain: Vec::new(),
                // NOTE: `Antichain::from_elem(minimum)`, never `Antichain::new()`. The empty
                // frontier reads as "complete through the end of time", making every snapshot wait
                // vacuously true and returning empty results instead of blocking.
                since: minimum.clone(),
                upper: minimum.clone(),
                // Seeded with the standing hold's own contribution. The physical accumulation has
                // no such hold and starts empty.
                logical_compaction: MutableAntichain::from_elem(
                    <Tr::Time as timely::progress::Timestamp>::minimum(),
                ),
                physical_compaction: MutableAntichain::new(),
                writer_logical: None,
                standing_hold: minimum,
                queues: Vec::new(),
                closed: false,
            }),
            peers,
        }
    }
}

/// The frontier a chain seeded into a fresh importer covers.
///
/// That is the last batch's upper, because the published chain is contiguous and totally ordered by
/// description. `upper` serves only the empty chain, which covers nothing.
///
/// Not `upper` itself: `upper` is the stream frontier and lags the chain by up to a scheduling round
/// (the publisher refreshes the chain from the trace, which seals a batch a round before the
/// frontier notification catches up). Seeding it would leave the importer's trace holding batches
/// above its own stream frontier, and a join reads both and would count a record from the trace that
/// the stream has not yet delivered. Mirrors `TraceAgent::new_listener`, which seeds the last
/// batch's upper for the same reason.
pub(super) fn seed_frontier<Tr: TraceReader>(
    chain: &[Tr::Batch],
    upper: &Antichain<Tr::Time>,
) -> Antichain<Tr::Time> {
    chain
        .last()
        .map_or_else(|| upper.clone(), |batch| batch.upper().clone())
}
