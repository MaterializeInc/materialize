// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Sharing arrangements across timely runtimes.
//!
//! An arrangement is normally readable only from the worker that maintains it: its batches are
//! reference counted with `Rc` and its trace handle is `Rc<RefCell<..>>`, both pinned to one
//! thread. This module lets a worker publish an arrangement whose batches are `Arc`'d (and whose
//! contents are `Send + Sync`) through a *publication point*, from which readers on other threads
//! take consistent snapshots or import the arrangement into a second timely runtime.
//!
//! The unit that crosses the thread boundary is not the
//! [`Spine`](differential_dataflow::trace::implementations::spine_fueled::Spine), which holds
//! thread-local state and has a single writer, but the spine's *contents*: a chain of immutable
//! `Arc`'d batches together with the trace's `since` and `upper` frontiers. Because batches are
//! immutable, a chain plus frontiers is a self-describing, consistent view. When the publishing
//! worker later merges batches, a reader holding an older chain is unaffected: its `Arc`s keep the
//! pre-merge batches alive until it drops them.
//!
//! ## Pieces
//!
//! * [`PublishArrangement::adopt`] attaches a publisher to an arrangement on the owning worker,
//!   filling a [`Published`] whose [`Published::handle`] hands out `Clone + Send`
//!   [`SharedTraceHandle`]s.
//! * [`SharedTraceHandle`] implements [`TraceReader`], so it drives compaction and cursors like any
//!   trace handle, from any thread. [`SharedTraceHandle::import_snapshot_at`] replays the shared
//!   arrangement into another scope.
//!
//! ## Compaction
//!
//! A publication point is differential's `TraceBox` for readers that are not agents of the trace: it
//! accumulates their holds in a `MutableAntichain` and forwards its frontier to the publisher's own
//! `TraceAgent`, the sole writer of the trace's compaction frontiers. Each handle mirrors its own
//! frontier locally and adjusts the accumulation as a delta, the way a `TraceAgent` does.
//!
//! Logical compaction decides which times stay *distinguishable*, physical compaction which batches
//! may *merge*. A reader needs distinguishability at its `as_of`, and a boundary at each frontier it
//! passes to `cursor_through`. It needs no boundary at its `as_of`: an import is seeded with the whole
//! chain and wrapped in `TraceFrontier`, which advances times rather than cutting. Conflating the two
//! is the mistake to avoid, and forwarding `since` as the physical frontier is what stopped published
//! arrangements from merging at all.
//!
//! Two of the holds have no reader behind them, and both exist because a reader registers only once
//! its dataflow is built, while the agent's setter joins and so only ever advances. The *standing
//! hold* tracks the frontier the importing runtime has applied, keeping the agent at or below every
//! `as_of` that runtime can still present. The *coverage hold* is its physical counterpart: a reader
//! registering on the next activation seeds at the current chain coverage and needs a boundary there.
//!
//! The sharing machinery lives entirely in Materialize, so it builds against a released
//! differential-dataflow rather than a fork. Publishing is exposed as the [`PublishArrangement`]
//! extension trait, since Materialize cannot add inherent methods to differential's foreign
//! `Arranged` type. Cross-thread batch sharing rests on the local `mz_row_spine::ArcBatch` newtype,
//! not on any differential-side `Arc` batch impls.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use differential_dataflow::lattice::{Lattice, antichain_meet};
use differential_dataflow::operators::arrange::{Arranged, TraceAgent, TraceReplayInstruction};
use differential_dataflow::trace::cursor::Navigable;
#[cfg(test)]
use differential_dataflow::trace::cursor::{CursorList, cursor_list};
use differential_dataflow::trace::wrappers::enter::TraceEnter;
use differential_dataflow::trace::wrappers::frontier::{BatchFrontier, TraceFrontier};
use differential_dataflow::trace::{BatchReader, TraceReader};
use mz_repr::{Diff, Timestamp};
use timely::dataflow::Scope;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::operators::generic::{Operator, source};
use timely::progress::Antichain;
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::scheduling::activate::SyncActivator;

use crate::typedefs::{ErrSpine, RowRowSpine};

/// A `Send` reader handle for a published `oks` arrangement.
pub type SharedOksHandle = SharedTraceHandle<RowRowSpine<Timestamp, Diff>>;
/// A `Send` reader handle for a published `errs` arrangement.
pub type SharedErrsHandle = SharedTraceHandle<ErrSpine<Timestamp, Diff>>;

/// A [`SharedOksHandle`] imported as a static `as_of` snapshot, wrapped in a `TraceFrontier`.
///
/// The interactive runtime imports a shared index via [`SharedTraceHandle::import_snapshot_at`],
/// which returns a `TraceFrontier`-wrapped arrangement whose times are advanced to the dataflow
/// `as_of` and bounded by `until`. Mirrors the maintenance import's `RowRowEnter`, which is likewise
/// `TraceFrontier`-wrapped.
pub type SharedOksFrontier = TraceFrontier<SharedOksHandle>;
/// An `ErrSpine` counterpart to [`SharedOksFrontier`].
pub type SharedErrsFrontier = TraceFrontier<SharedErrsHandle>;

/// A [`SharedOksFrontier`] entered into a render scope whose timestamp is `TEnter`.
pub type SharedOksEnter<TEnter> = TraceEnter<SharedOksFrontier, TEnter>;
/// A [`SharedErrsFrontier`] entered into a render scope whose timestamp is `TEnter`.
pub type SharedErrsEnter<TEnter> = TraceEnter<SharedErrsFrontier, TEnter>;

/// The queue and wakeup for one importer registered against a publication point.
struct ImportQueue<Tr: TraceReader> {
    /// Replay instructions the publisher appends and the importer drains, mirroring the local
    /// arrange replay queue. Batches carry a hint time that lower-bounds their updates.
    instructions: VecDeque<TraceReplayInstruction<Tr>>,
    /// Wakes the importer's source operator on the reader's worker when new instructions arrive.
    activator: SyncActivator,
}

/// State shared between a publisher (on the owning worker) and its readers (on any thread).
///
/// The `chain`, `since`, and `upper` are always updated together under the lock, so every reader
/// observes a frontier-consistent view.
struct SharedTraceState<Tr: TraceReader> {
    /// The published chain, sourced from `map_batches`: contiguous descriptions including the
    /// seal-only empty batches that never travel on the arrangement stream. Coverage is at least
    /// `upper` (within a worker step it may briefly run ahead, never behind).
    chain: Vec<Tr::Batch>,
    /// Logical compaction frontier of the published view. Reads at times not beyond `since` are not
    /// accurate. A snapshot must pick a time at or beyond it.
    ///
    /// Written only by the publisher, as the meet of its agent's post-forward hold and the
    /// writer-driven frontier. That is the trace's real logical compaction, since those are the only
    /// agents on it.
    since: Antichain<Tr::Time>,
    /// Seal frontier: the join of the chain's batch uppers. Batches strictly below `upper` are
    /// complete and readable.
    upper: Antichain<Tr::Time>,
    /// Accumulated logical holds: every reader registration plus `standing_hold`. The publisher
    /// forwards this frontier to its agent.
    logical_compaction: MutableAntichain<Tr::Time>,
    /// Accumulated physical holds: every reader registration plus `coverage_hold`.
    physical_compaction: MutableAntichain<Tr::Time>,
    /// Per-registration logical holds, mirroring what each handle contributes to
    /// `logical_compaction`.
    ///
    /// The accumulation alone cannot say which reader holds what, and a refusal diagnostic needs
    /// exactly that: "a hold sits at `f`" and "no hold exists and the frontier happens to be `f`"
    /// are the difference between an import that is protected and one that is not.
    logical_holds: BTreeMap<usize, Antichain<Tr::Time>>,
    /// Per-registration physical holds: the lowest frontier each reader may still cut at.
    ///
    /// This is the path a shared reader's request travels, and a local consumer needs no equivalent:
    /// `crate::render::join::mz_join_core` is an agent on its own trace, so its
    /// `set_physical_compaction(acknowledged)` reaches the `TraceBox` directly.
    ///
    /// An entry starts at the chain coverage at registration, never at `since` and never at `as_of`:
    /// `acknowledged` is initialised to that coverage in `SharedTraceHandle::import_snapshot_at` and
    /// only advances, so no cut ever happens below it.
    physical_holds: BTreeMap<usize, Antichain<Tr::Time>>,
    /// The controller's last logical compaction frontier for this arrangement, forwarded from
    /// `handle_allow_compaction` via `crate::sharing::ArrangementSharingRegistry::note_allow_compaction`.
    /// `None` until the first `AllowCompaction` arrives.
    ///
    /// Not a hold here: it is another agent's hold on the same trace, so it belongs to the trace's
    /// own meet. The publisher reads it to publish `since`, which is that meet.
    writer_logical: Option<Antichain<Tr::Time>>,
    /// Logical hold at the compaction frontier the runtime that may import this arrangement has
    /// applied.
    ///
    /// The two runtimes drain their command streams independently, so the owning runtime can apply a
    /// compaction the importing one has not. An importing dataflow whose `CreateDataflow` is still
    /// queued there has registered no hold, and would be built against an arrangement already
    /// compacted past its `as_of`. This hold forbids that: a shared arrangement compacts only as fast
    /// as the slowest runtime's stream position. See
    /// `doc/developer/design/20260720_two_runtime_compute/design.md`.
    ///
    /// Joins, so it only ever rises, and it starts at the publisher's compaction frontier at adoption
    /// (`crate::shared_trace::PublishArrangement::adopt`). That start is at or below the `as_of` of
    /// every dataflow that may import the collection, because the controller does not offer an `as_of`
    /// below a collection's own `since`.
    standing_hold: Antichain<Tr::Time>,
    /// Physical hold at the coverage of the published chain, the publisher's own.
    ///
    /// A reader registers at the coverage it finds, so a merge spanning the current coverage would
    /// destroy the boundary the next reader to arrive cuts at. With no readers this is the whole
    /// physical frontier, which is what an unshared index gets from
    /// `crate::arrangement::manager::TraceManager::maintenance`.
    coverage_hold: Antichain<Tr::Time>,
    /// Importer queues, keyed by registration id. A handle may back several registrations, so this
    /// is keyed separately from any handle.
    queues: BTreeMap<usize, ImportQueue<Tr>>,
    /// Monotonic source of registration ids for holds and queues.
    next_id: usize,
    /// Set when the publisher drops. A terminal empty frontier is enqueued to each importer, so
    /// readers close only after draining what was already published.
    closed: bool,
}

impl<Tr: TraceReader> SharedTraceState<Tr>
where
    Tr::Time: Lattice + Clone,
{
    /// Sets registration `id`'s logical hold to `frontier`, releasing it when `frontier` is empty.
    ///
    /// The empty antichain means "compaction is permitted everywhere", so a handle that reaches it
    /// has released. The reduce operator reaches it on every dataflow whose input finishes: it
    /// forwards `upper_limit` to its source trace, and `upper_limit` is the join of the input
    /// frontiers, which empties when the input does.
    fn set_logical_hold(&mut self, id: usize, frontier: &Antichain<Tr::Time>) {
        let previous = if frontier.is_empty() {
            self.logical_holds.remove(&id)
        } else {
            self.logical_holds.insert(id, frontier.clone())
        };
        let previous = previous.unwrap_or_else(Antichain::new);
        adjust(&mut self.logical_compaction, &previous, frontier);
    }

    /// Sets registration `id`'s physical hold to `frontier`, releasing it when `frontier` is empty.
    ///
    /// An empty request means this reader will never cut again, so it stops constraining where the
    /// spine may merge.
    fn set_physical_hold(&mut self, id: usize, frontier: &Antichain<Tr::Time>) {
        let previous = if frontier.is_empty() {
            self.physical_holds.remove(&id)
        } else {
            self.physical_holds.insert(id, frontier.clone())
        };
        let previous = previous.unwrap_or_else(Antichain::new);
        adjust(&mut self.physical_compaction, &previous, frontier);
    }

    /// Releases registration `id`'s holds on both axes.
    fn release_holds(&mut self, id: usize) {
        let empty = Antichain::new();
        self.set_logical_hold(id, &empty);
        self.set_physical_hold(id, &empty);
    }

    /// Advances the standing hold to its join with `frontier`.
    fn advance_standing_hold(&mut self, frontier: &Antichain<Tr::Time>) {
        let next = self.standing_hold.join(frontier);
        let previous = std::mem::replace(&mut self.standing_hold, next);
        adjust(&mut self.logical_compaction, &previous, &self.standing_hold);
    }

    /// Moves the publisher's own physical hold to `frontier`, the coverage of the published chain.
    fn set_coverage_hold(&mut self, frontier: Antichain<Tr::Time>) {
        let previous = std::mem::replace(&mut self.coverage_hold, frontier);
        adjust(
            &mut self.physical_compaction,
            &previous,
            &self.coverage_hold,
        );
    }
}

/// Replaces the elements of `lower` with those of `upper` in `accumulated`.
///
/// The delta form differential's `TraceBox` uses, so the accumulated frontier costs the times that
/// changed rather than a walk over every hold.
fn adjust<T: timely::progress::Timestamp>(
    accumulated: &mut MutableAntichain<T>,
    lower: &Antichain<T>,
    upper: &Antichain<T>,
) {
    accumulated.update_iter(upper.iter().cloned().map(|time| (time, 1)));
    accumulated.update_iter(lower.iter().cloned().map(|time| (time, -1)));
}

/// A publication point: the shared state every reader of one published arrangement sees.
struct SharedTrace<Tr: TraceReader> {
    state: Mutex<SharedTraceState<Tr>>,
    /// Total peer count (workers-per-process times processes) of the scope that published this
    /// arrangement. Set once at publish time and never mutated afterward, so `import` reads it
    /// without taking `state`'s lock. Pairwise import (importer worker `i` reads publisher worker
    /// `i`) is sound only when an importing scope shards keys the same way, which requires this to
    /// match the importing scope's own `peers()`.
    peers: usize,
}

type SharedTraceRef<Tr> = Arc<SharedTrace<Tr>>;

impl<Tr: TraceReader> SharedTrace<Tr> {
    /// A fresh publication point: empty chain, `since` and `upper` at the minimum time, no reader
    /// holds, no publisher attached.
    ///
    /// Used by [`Published::placeholder`], which leaves the point unbacked for a later
    /// [`PublishArrangement::adopt`].
    fn new_empty(peers: usize) -> Self {
        let minimum = Antichain::from_elem(batch_min::<Tr>());
        SharedTrace {
            state: Mutex::new(SharedTraceState {
                chain: Vec::new(),
                // NOTE: `Antichain::from_elem(minimum)`, never `Antichain::new()`. The empty
                // frontier reads as "complete through the end of time", making every snapshot wait
                // vacuously true and returning empty results instead of blocking.
                since: minimum.clone(),
                upper: minimum.clone(),
                // Seeded with the two holds that have no reader behind them.
                logical_compaction: MutableAntichain::from_elem(batch_min::<Tr>()),
                physical_compaction: MutableAntichain::from_elem(batch_min::<Tr>()),
                logical_holds: BTreeMap::new(),
                physical_holds: BTreeMap::new(),
                writer_logical: None,
                standing_hold: minimum.clone(),
                coverage_hold: minimum,
                queues: BTreeMap::new(),
                next_id: 0,
                closed: false,
            }),
            peers,
        }
    }
}

/// The result of publishing an arrangement. Holding it keeps the publication point registered;
/// dropping it does not stop the publisher (the publisher lives with its dataflow), but no further
/// handles can be minted from it.
pub struct Published<Tr: TraceReader> {
    shared: SharedTraceRef<Tr>,
}

impl<Tr: TraceReader> Published<Tr>
where
    Tr::Time: Lattice + Clone,
{
    /// Hands out a `Clone + Send` handle to the published arrangement.
    ///
    /// The handle registers a logical hold at the current published `since`, so the arrangement
    /// will not compact past it until the handle (and all its clones) drop.
    pub fn handle(&self) -> SharedTraceHandle<Tr> {
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
    pub fn handle_at(
        &self,
        as_of: &Antichain<Tr::Time>,
    ) -> Result<SharedTraceHandle<Tr>, Antichain<Tr::Time>> {
        SharedTraceHandle::register_at(Arc::clone(&self.shared), as_of)
    }

    /// Creates an unbacked publication point: an empty chain with `since` and `upper` at the minimum
    /// time and no publisher.
    ///
    /// A reader may immediately mint handles ([`Self::handle`]) and build imports over it, but they
    /// produce nothing (the import frontier stays at the minimum) until a publisher adopts this point
    /// via [`PublishArrangement::adopt`] and begins refreshing it. Adoption fills the same `Arc`, so
    /// a handle captured by value at construction (as a differential join captures its input trace)
    /// observes the filled chain: the handle is a live proxy into the shared state, not a snapshot.
    ///
    /// `peers` must equal the total peer count of the scope that later adopts the point, the same
    /// invariant [`SharedTraceHandle::import_snapshot_at`] enforces.
    pub fn placeholder(peers: usize) -> Self {
        Published {
            shared: Arc::new(SharedTrace::new_empty(peers)),
        }
    }

    /// The logical holds currently registered against this publication point.
    ///
    /// Test-only, and the only way to distinguish "a hold exists and sits at `f`" from "no hold
    /// exists and the accumulated frontier happens to be `f`", which look identical from the
    /// published frontiers.
    #[cfg(test)]
    pub(crate) fn logical_holds(&self) -> Vec<Antichain<Tr::Time>> {
        let state = self.shared.state.lock().expect("shared trace poisoned");
        state.logical_holds.values().cloned().collect()
    }

    /// The number of batches in the published chain.
    ///
    /// Test-only. Counting through a handle would register a physical hold and perturb the merge
    /// behaviour being measured, which is the whole observable here.
    #[cfg(test)]
    pub(crate) fn chain_len(&self) -> usize {
        let state = self.shared.state.lock().expect("shared trace poisoned");
        state.chain.len()
    }

    /// The controller's last `AllowCompaction` frontier for this arrangement, or `None` if none has
    /// arrived, the standing hold, and the published `(since, upper)`.
    ///
    /// Diagnostics for a caller whose `as_of` was refused. Reading them off the publication point
    /// rather than off a handle keeps a failure path from registering a hold on its way to a panic,
    /// and lets the caller report the point that actually refused rather than a sibling.
    ///
    /// The standing hold is what makes a refusal diagnosable. It is the frontier the importing
    /// runtime has applied, so a refusal with the standing hold AT the refusing `since` means that
    /// runtime had already applied this compaction before it built the importing dataflow, and no
    /// replica-side hold could have prevented it. A standing hold BELOW that `since` means the
    /// publisher escaped its own bound, which is a bug here rather than upstream.
    pub fn diagnostics(
        &self,
    ) -> (
        Option<Antichain<Tr::Time>>,
        Antichain<Tr::Time>,
        Antichain<Tr::Time>,
        Antichain<Tr::Time>,
    ) {
        let state = self.shared.state.lock().expect("shared trace poisoned");
        (
            state.writer_logical.clone(),
            state.standing_hold.clone(),
            state.since.clone(),
            state.upper.clone(),
        )
    }

    /// Records the controller's logical compaction frontier for this arrangement.
    ///
    /// The publisher reads it to publish `since`, which is the meet of the trace's agents. Called
    /// from `handle_allow_compaction` through the registry.
    pub fn note_writer_logical(&self, frontier: &Antichain<Tr::Time>) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.writer_logical = Some(frontier.clone());
        }
    }

    /// Advances the standing hold to `frontier`, recording that the runtime which may import this
    /// arrangement has applied the controller's compaction that far.
    ///
    /// Joins rather than assigning, so a reordered or replayed command cannot lower a bound the
    /// publisher already forwarded, which its agent's own joining setter could not honour anyway.
    pub fn note_standing_hold(&self, frontier: &Antichain<Tr::Time>) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.advance_standing_hold(frontier);
        }
    }

    /// The standing hold currently bounding this arrangement's logical compaction.
    #[cfg(test)]
    pub(crate) fn standing_hold(&self) -> Antichain<Tr::Time> {
        let state = self.shared.state.lock().expect("shared trace poisoned");
        state.standing_hold.clone()
    }
}

/// A `Clone + Send` reader of a published arrangement.
///
/// Implements [`TraceReader`], so downstream operators drive its compaction and acquire cursors as
/// with any trace handle. Each clone carries an independent registration: cloning mints a fresh id
/// and copies the source's holds, so two consumers of one import cannot release each other's holds.
pub struct SharedTraceHandle<Tr: TraceReader> {
    shared: SharedTraceRef<Tr>,
    /// This handle's hold registration id.
    id: usize,
    /// This handle's own logical frontier, mirrored into `logical_holds[id]`. Kept locally so
    /// `get_logical_compaction` can return a borrow.
    logical: Antichain<Tr::Time>,
    /// This handle's own physical frontier, mirrored into `physical_holds[id]`. Kept locally so
    /// `get_physical_compaction` can return a borrow.
    physical: Antichain<Tr::Time>,
}

impl<Tr: TraceReader> SharedTraceHandle<Tr>
where
    Tr::Time: Lattice + Clone,
{
    /// Registers a fresh hold at the current published `since` and returns a handle for it.
    fn register(shared: SharedTraceRef<Tr>) -> Self {
        let (id, since) = {
            let mut state = shared.state.lock().expect("shared trace poisoned");
            let id = state.next_id;
            state.next_id += 1;
            let since = state.since.clone();
            state.set_logical_hold(id, &since);
            // The cut floor starts at the chain coverage, NOT at `since`. See `register_at`.
            let coverage = seed_frontier::<Tr>(&state.chain, &state.upper);
            state.set_physical_hold(id, &coverage);
            (id, since)
        };
        Self {
            shared,
            id,
            logical: since.clone(),
            physical: since,
        }
    }

    /// Registers a hold at `as_of` under a single lock acquisition, failing with the published
    /// `since` when it is already beyond `as_of`. See [`Published::handle_at`].
    fn register_at(
        shared: SharedTraceRef<Tr>,
        as_of: &Antichain<Tr::Time>,
    ) -> Result<Self, Antichain<Tr::Time>> {
        let (id, since) = {
            let mut state = shared.state.lock().expect("shared trace poisoned");
            if !timely::PartialOrder::less_equal(&state.since, as_of) {
                return Err(state.since.clone());
            }
            let id = state.next_id;
            state.next_id += 1;
            let since = state.since.clone();
            state.set_logical_hold(id, as_of);
            // The cut floor is the CHAIN COVERAGE, and it is neither `as_of` nor `since`.
            //
            // Not `since`: `since` is a logical frontier, the floor on which times stay
            // distinguishable. Using it here would hold every batch boundary above the controller's
            // read frontier, which is what stopped published arrangements from merging at all.
            //
            // Not `as_of` either, and not because `as_of` is too low to be honoured. It is that no
            // cut ever happens there. `Self::import_snapshot_at` seeds an import with the whole
            // chain and initialises `acknowledged` to that seed's coverage, and `TraceFrontier`
            // advances times rather than cutting, so a batch straddling `as_of` is harmless. Cuts
            // only ever happen at or above the coverage, and only rise from there.
            //
            // The coverage lags the stream `upper` when the publisher has sealed batches the chain
            // does not yet carry (see `seed_frontier_covers_the_chain_not_the_stream_frontier`), so
            // `upper` is the wrong value too: it would sit above the seed a reader registering now
            // will get, and permit a merge across the very first frontier that reader cuts at.
            let coverage = seed_frontier::<Tr>(&state.chain, &state.upper);
            state.set_physical_hold(id, &coverage);
            (id, since)
        };
        Ok(Self {
            shared,
            id,
            logical: as_of.clone(),
            // NOTE: `since`, NOT `as_of`. `get_physical_compaction` must never report a frontier
            // that leads the published chain's coverage, because a consumer checks exactly that
            // against the coverage it derives from `map_batches`: see the assertion in
            // `crate::render::join::mz_join_core`, which differential's own `join_core` also carries.
            // An `as_of` legitimately leads the coverage, for an import over a placeholder whose
            // publisher has not adopted it yet, or for a read at a timestamp beyond the index's seal.
            // Reporting it here aborts the worker on a correct import.
            //
            // `since` is right for the same reason it is right in `TraceAgent::clone`, which inherits
            // the frontier of the agent it clones: it is what the trace actually guarantees. The
            // publisher forwards the published `since` as its physical target, so this reports the
            // grant.
            physical: since,
        })
    }

    /// Takes a consistent snapshot of the published arrangement as of `time`, waiting until `upper`
    /// passes `time`.
    ///
    /// Test-only. Production reads go through [`Self::import_snapshot_at`], which is notification
    /// driven and never parks a worker. This waits by polling, so it must not be called from a
    /// timely worker thread: a worker blocked here cannot step, and on a single-worker test that
    /// includes the publisher it is waiting for.
    ///
    /// Returns `None` when the snapshot cannot serve `time`, which is either the publisher closed
    /// before `upper` passed `time`, or compaction has advanced `since` beyond `time` so the
    /// accumulation at `time` is no longer accurate. The gate on `since` mirrors the single-runtime
    /// peek path, which errors when the compaction frontier is beyond the read time rather than
    /// returning coalesced results.
    #[cfg(test)]
    pub(crate) fn snapshot_at(&self, time: &Tr::Time) -> Option<TraceSnapshot<Tr>> {
        loop {
            {
                let state = self.shared.state.lock().expect("shared trace poisoned");
                // `upper` not less-equal `time` means all updates at `time` are sealed.
                if !state.upper.less_equal(time) {
                    // `since` beyond `time` means times at `time` have been coalesced and a read
                    // there would be inaccurate. Fail to `None` rather than serve stale data.
                    if !state.since.less_equal(time) {
                        return None;
                    }
                    return Some(TraceSnapshot {
                        chain: state.chain.clone(),
                    });
                }
                if state.closed {
                    return None;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// The controller's last `AllowCompaction` frontier for this arrangement, or `None` if none has
    /// arrived.
    ///
    /// Diagnostic only. It distinguishes a published `since` that the controller drove from one the
    /// publisher's own hold drove, which is what tells apart a protocol-ordering violation from a
    /// local compaction bug when a reader finds `since` beyond its `as_of`.
    pub fn writer_logical(&self) -> Option<Antichain<Tr::Time>> {
        let state = self.shared.state.lock().expect("shared trace poisoned");
        state.writer_logical.clone()
    }

    /// The published arrangement's current `(since, upper)` frontiers, read under the state lock.
    ///
    /// A point-in-time observation for diagnostics. Either frontier may have advanced by the time
    /// the caller inspects the returned values.
    pub fn frontiers(&self) -> (Antichain<Tr::Time>, Antichain<Tr::Time>) {
        let state = self.shared.state.lock().expect("shared trace poisoned");
        (state.since.clone(), state.upper.clone())
    }

    /// Mirrors this handle's logical frontier into the publication point.
    fn update_hold(&self) {
        let mut state = self.shared.state.lock().expect("shared trace poisoned");
        state.set_logical_hold(self.id, &self.logical);
    }

    /// Mirrors this handle's physical frontier into the publication point.
    fn update_physical_hold(&self) {
        let mut state = self.shared.state.lock().expect("shared trace poisoned");
        state.set_physical_hold(self.id, &self.physical);
    }
}

impl<Tr: TraceReader> Clone for SharedTraceHandle<Tr>
where
    Tr::Time: Lattice + Clone,
{
    fn clone(&self) -> Self {
        // A clone must be an independent hold: `import` returns `Arranged { trace: handle.clone() }`
        // and `Arranged` is itself `Clone`, so distinct downstream operators drive compaction on
        // distinct clones. Sharing one hold slot would let the faster operator release the slower
        // one's hold. This mirrors `TraceAgent::clone`, which registers an independent counted hold.
        let id = {
            let mut state = self.shared.state.lock().expect("shared trace poisoned");
            let id = state.next_id;
            state.next_id += 1;
            state.set_logical_hold(id, &self.logical);
            state.set_physical_hold(id, &self.physical);
            id
        };
        Self {
            shared: Arc::clone(&self.shared),
            id,
            logical: self.logical.clone(),
            physical: self.physical.clone(),
        }
    }
}

impl<Tr: TraceReader> Drop for SharedTraceHandle<Tr> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.release_holds(self.id);
        }
    }
}

impl<Tr: TraceReader> TraceReader for SharedTraceHandle<Tr>
where
    Tr::Time: Lattice + Clone,
{
    type Time = Tr::Time;
    type Batch = Tr::Batch;

    fn batches_through(&mut self, upper: AntichainRef<Tr::Time>) -> Option<Vec<Self::Batch>> {
        let state = self.shared.state.lock().expect("shared trace poisoned");
        // NOTE: `Spine::batches_through` asserts that the cut is at or beyond the spine's physical
        // frontier, and that precondition does NOT transfer to a shared handle. A local reader is one
        // of the trace's own agents, so the spine's frontier is the meet across agents including this
        // reader's own, and the reader can never cut below it. A shared reader's request reaches the
        // spine only through the publisher's single agent, whose setter joins, so the spine's frontier
        // can sit above where this reader still legitimately cuts: it may be draining a seed whose
        // coverage predates a later forward. Asserting the spine's precondition here therefore panics
        // on a correct read, which is what it did.
        //
        // The straddle check below is the real guard, and it does not depend on the precondition. It
        // is also the falsifier for the physical-hold forwarding in `PublishArrangement`: if a reader
        // ever does need a boundary that forwarding merged away, this fires and names the frontier,
        // where deleting it would leave a consumer silently double counting updates at times not
        // before its cut. Do not remove it.
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
        self.logical = self.logical.join(&frontier.to_owned());
        self.update_hold();
    }

    fn get_logical_compaction(&mut self) -> AntichainRef<'_, Tr::Time> {
        self.logical.borrow()
    }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, Tr::Time>) {
        self.physical = self.physical.join(&frontier.to_owned());
        self.update_physical_hold();
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

/// Smallest time, used only to satisfy the borrow in the cut check for empty lower frontiers.
fn batch_min<Tr: TraceReader>() -> Tr::Time {
    <Tr::Time as timely::progress::Timestamp>::minimum()
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
fn seed_frontier<Tr: TraceReader>(
    chain: &[Tr::Batch],
    upper: &Antichain<Tr::Time>,
) -> Antichain<Tr::Time> {
    chain
        .last()
        .map_or_else(|| upper.clone(), |batch| batch.upper().clone())
}

/// An owned, consistent snapshot of a published arrangement: an immutable chain plus its frontiers.
///
/// Test-only, the result of [`SharedTraceHandle::snapshot_at`]. Holding it pins the chain's batches,
/// keeping their memory alive even as the publishing worker merges.
#[cfg(test)]
pub(crate) struct TraceSnapshot<Tr: TraceReader> {
    chain: Vec<Tr::Batch>,
}

#[cfg(test)]
impl<Tr: TraceReader> TraceSnapshot<Tr> {
    /// A cursor merging the snapshot's batch cursors, with the batches as its storage.
    pub(crate) fn cursor(&self) -> (CursorList<<Tr::Batch as Navigable>::Cursor>, Vec<Tr::Batch>)
    where
        Tr::Batch: Navigable,
    {
        cursor_list(self.chain.clone())
    }
}

/// Publishes an [`Arranged`] arrangement through a publication point on its owning worker.
///
/// Materialize cannot add inherent methods to differential's foreign `Arranged` type, so it exposes
/// them as this extension trait instead. Bring it into scope at a call site to use
/// `arranged.adopt(...)`.
pub trait PublishArrangement<Tr: TraceReader> {
    /// Installs this arrangement's publisher into an existing `placeholder` publication point,
    /// created by [`Published::placeholder`], rather than minting a fresh one.
    ///
    /// Attaches a publisher operator to the arrangement stream. On each activation the publisher
    /// refreshes the published chain, `since`, and `upper` from the trace, appends newly arrived
    /// batches to importer queues, and forwards the accumulated holds to the trace's compaction.
    ///
    /// This is the late-binding path: a reader may create the placeholder and build handles and
    /// imports over it before this arrangement is rendered. Those imports produce nothing (their
    /// frontier stays at the minimum) until adoption begins the refresh loop, at which point the
    /// already-registered importer queues fill from the same publisher iteration that serves any
    /// later-registered reader.
    ///
    /// Requires the adopting scope's total peer count to equal the placeholder's, panicking
    /// otherwise.
    ///
    /// `on_seal` fires once per activation on which the published `upper` advances, after the state
    /// lock is released and `upper` reflects the advance. A fast-path peek parked on this
    /// arrangement's seal is re-examined only through this callback, so it must observe the advanced
    /// `upper`. Firing the wake from an upstream stream tap instead notifies before the sink advances
    /// `upper`, so the peek reads a stale upper, parks, and is never re-woken once that advance was
    /// the last one. See the lost-wakeup contract on
    /// `crate::sharing::ArrangementSharingRegistry::notify`.
    fn adopt<F: Fn() + 'static>(&self, placeholder: &Published<Tr>, on_seal: F);

    /// [`PublishArrangement::adopt`], with a name for the publisher operator.
    fn adopt_named<F: Fn() + 'static>(&self, placeholder: &Published<Tr>, name: &str, on_seal: F);
}

impl<'scope, Tr> PublishArrangement<Tr> for Arranged<'scope, TraceAgent<Tr>>
where
    Tr: differential_dataflow::trace::Trace + 'static,
    Tr::Batch: Send + Sync,
    Tr::Time: Lattice + Clone + Send + Sync,
{
    fn adopt<F: Fn() + 'static>(&self, placeholder: &Published<Tr>, on_seal: F) {
        PublishArrangement::adopt_named(self, placeholder, "PublishShared", on_seal);
    }

    fn adopt_named<F: Fn() + 'static>(&self, placeholder: &Published<Tr>, name: &str, on_seal: F) {
        assert_eq!(
            self.stream.scope().peers(),
            placeholder.shared.peers,
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
        // can need a frontier below it. Without this seed a placeholder created before adoption would
        // hold at the minimum time and stop the arrangement compacting at all.
        {
            let mut state = placeholder
                .shared
                .state
                .lock()
                .expect("shared trace poisoned");
            state.advance_standing_hold(&initial_logical);
        }

        let publisher = Publisher {
            shared: Arc::clone(&placeholder.shared),
        };

        let sink_shared = Arc::clone(&placeholder.shared);
        self.stream.clone().sink(
            timely::dataflow::channels::pact::Pipeline,
            name,
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

                    for (batch, hint) in arrived.drain(..) {
                        for queue in state.queues.values_mut() {
                            queue.instructions.push_back(TraceReplayInstruction::Batch(
                                batch.clone(),
                                Some(hint.clone()),
                            ));
                        }
                    }

                    let upper_advanced = state.upper != upper;
                    if upper_advanced {
                        for queue in state.queues.values_mut() {
                            queue
                                .instructions
                                .push_back(TraceReplayInstruction::Frontier(upper.clone()));
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
                    // I1c comes with the standing hold being one of the accumulated holds: the
                    // frontier is at or below it, so the arrangement compacts only as fast as the
                    // slowest runtime's command stream.
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
                    let coverage = seed_frontier::<Tr>(&state.chain, &state.upper);
                    state.set_coverage_hold(coverage);
                    let physical = state.physical_compaction.frontier().to_owned();

                    // Wake importers and any peek waiters.
                    for queue in state.queues.values() {
                        let _ = queue.activator.activate();
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
    shared: SharedTraceRef<Tr>,
}

impl<Tr: TraceReader> Drop for Publisher<Tr> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.closed = true;
            let empty = Antichain::new();
            for queue in state.queues.values_mut() {
                queue
                    .instructions
                    .push_back(TraceReplayInstruction::Frontier(empty.clone()));
                let _ = queue.activator.activate();
            }
        }
    }
}

impl<Tr: TraceReader> SharedTraceHandle<Tr>
where
    Tr: 'static,
    Tr::Time: Lattice + Clone,
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
    /// publisher's, panicking otherwise. Pairwise import (importer worker `i` reads publisher
    /// worker `i`) is sound only when both sides shard by the same `key.hashed() % peers`; a
    /// mismatched peer count would silently read the wrong shard instead of failing loudly.
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
    /// drops once the trace's frontier passes `as_of`, so the one-shot result completes. An empty
    /// `until` performs no bounding and the import stays live with the trace.
    pub fn import_snapshot_at<'scope>(
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

            // Register under one lock acquisition: mint an id, seed the queue with the current
            // chain (hint `minimum`, as the local replay does for historical batches) followed by
            // the frontier that chain covers, and install the queue. Later batches append; earlier
            // ones are seeded. Nothing is missed or duplicated.
            let (reg_id, seed) = {
                let mut state = shared.state.lock().expect("shared trace poisoned");
                let reg_id = state.next_id;
                state.next_id += 1;
                let mut instructions = VecDeque::new();
                for batch in state.chain.iter() {
                    instructions.push_back(TraceReplayInstruction::Batch(
                        batch.clone(),
                        Some(batch_min::<Tr>()),
                    ));
                }
                let seed = seed_frontier::<Tr>(&state.chain, &state.upper);
                instructions.push_back(TraceReplayInstruction::Frontier(seed.clone()));
                // If the publisher already closed, its one-shot terminal frontier has been and gone,
                // so seed our own. Otherwise a late importer would drain the chain and then wait
                // forever for a frontier that never arrives, leaking its capability. Mirrors the
                // `state.closed` guard in `snapshot_at`.
                if state.closed {
                    instructions.push_back(TraceReplayInstruction::Frontier(Antichain::new()));
                }
                state.queues.insert(
                    reg_id,
                    ImportQueue {
                        instructions,
                        activator,
                    },
                );
                (reg_id, seed)
            };

            // Deregisters the queue when the source operator (and thus this closure) drops.
            let _guard = QueueGuard {
                shared: Arc::clone(&shared),
                reg_id,
            };

            let mut capabilities = Some(CapabilitySet::new());
            capabilities.as_mut().unwrap().insert(capability);
            let mut acknowledged = seed.clone();
            // The seeded instructions come first and are emitted as-is. Everything after the seed's
            // own `Frontier` is a live instruction from the publisher, and is filtered against
            // `seed` below.
            let mut draining_seed = true;

            move |output| {
                let _guard = &_guard;
                let mut drained = Vec::new();
                {
                    let mut state = shared.state.lock().expect("shared trace poisoned");
                    if let Some(queue) = state.queues.get_mut(&reg_id) {
                        drained.extend(queue.instructions.drain(..));
                    }
                }

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
                                // The setter joins, so this never lowers the hold, which matters while
                                // the seed is draining: the seeded coverage can already lead `as_of`,
                                // and the hold must stay at `as_of` until the stream really passes it.
                                //
                                // This is the import's own obligation only. A consumer that reads the
                                // returned trace rather than the stream needs accuracy at times its own
                                // progress governs, which can lag this, and it holds its own separate
                                // registration for exactly that. The publisher forwards the meet, so the
                                // slower of the two wins.
                                if let Some(hold) = hold.as_mut() {
                                    hold.set_logical_compaction(acknowledged.borrow());
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
                                // A batch the seed already covers. The chain is read from the
                                // trace, which can hold a batch the arrangement stream has not
                                // delivered yet, so the publisher will push that same batch as a
                                // live instruction on a later activation. Emitting it twice would
                                // double count it, and its hint sits below the frontier the seed
                                // already claimed, which `delayed` panics on.
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

/// Deregisters an importer's replay queue when its source operator drops.
struct QueueGuard<Tr: TraceReader> {
    shared: SharedTraceRef<Tr>,
    reg_id: usize,
}

impl<Tr: TraceReader> Drop for QueueGuard<Tr> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.queues.remove(&self.reg_id);
        }
    }
}

#[cfg(test)]
mod tests;
