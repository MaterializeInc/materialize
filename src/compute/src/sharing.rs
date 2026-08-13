// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A per-process registry of published index arrangements.
//!
//! An index arrangement is normally readable only from the timely worker that maintains it. When
//! `crate::render` publishes a maintained index through the differential shared-trace primitive
//! it records the resulting [`Published`] handles here, keyed by [`GlobalId`] and worker ordinal, so
//! a reader on another thread or runtime can mint a `Send` [`SharedTraceHandle`] for the same
//! arrangement.
//!
//! The registry is per PROCESS and shared across all timely workers of the runtime. Each worker has
//! its own `ComputeState`, but they all share one registry `Arc`, the way the persist client cache
//! is shared. Worker `i` publishes into slot `i`; a reader on worker `i` of another runtime looks up
//! slot `i`, which is sound only because both sides shard keys by the same `key.hashed() % peers`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use differential_dataflow::trace::wrappers::enter::TraceEnter;
use differential_dataflow::trace::wrappers::frontier::TraceFrontier;
use mz_repr::{Diff, GlobalId, Timestamp};
use timely::progress::Antichain;
use timely::scheduling::activate::SyncActivator;

use crate::shared_trace::{Published, SharedTraceHandle};
use crate::typedefs::{ErrSpine, RowRowSpine};

/// A `Send` reader handle for a published `oks` arrangement.
pub type SharedOksHandle = SharedTraceHandle<RowRowSpine<Timestamp, Diff>>;
/// A `Send` reader handle for a published `errs` arrangement.
pub type SharedErrsHandle = SharedTraceHandle<ErrSpine<Timestamp, Diff>>;

/// A [`SharedOksHandle`] imported as a static `as_of` snapshot, wrapped in a `TraceFrontier`.
///
/// The interactive runtime imports a shared index via `SharedTraceHandle::import_snapshot_at`, which
/// returns a `TraceFrontier`-wrapped arrangement whose times are advanced to the dataflow `as_of`
/// and bounded by `until`. Mirrors the maintenance import's `RowRowEnter`, which is likewise
/// `TraceFrontier`-wrapped.
pub type SharedOksFrontier = TraceFrontier<SharedOksHandle>;
/// An `ErrSpine` counterpart to [`SharedOksFrontier`].
pub type SharedErrsFrontier = TraceFrontier<SharedErrsHandle>;

/// A [`SharedOksFrontier`] entered into a render scope whose timestamp is `TEnter`.
pub type SharedOksEnter<TEnter> = TraceEnter<SharedOksFrontier, TEnter>;
/// A [`SharedErrsFrontier`] entered into a render scope whose timestamp is `TEnter`.
pub type SharedErrsEnter<TEnter> = TraceEnter<SharedErrsFrontier, TEnter>;

/// The published `oks`/`errs` arrangements of one maintained index on one worker.
///
/// An index's `oks` is always a `RowRowSpine` and its `errs` always an `ErrSpine`. Holding the
/// [`Published`] values keeps the publication points registered and lets us mint further handles.
pub struct SharedIndexArrangement {
    /// The published `oks` arrangement.
    pub oks: Published<RowRowSpine<Timestamp, Diff>>,
    /// The published `errs` arrangement.
    pub errs: Published<ErrSpine<Timestamp, Diff>>,
}

/// A per-interactive-worker wake channel: a coalescing cross-thread activator plus the set of ids
/// marked dirty since the worker last drained.
///
/// The interactive worker parks in `step_or_park`; a publication, removal, or frontier advance on a
/// dependency it is waiting for must push it back to work. `activator` fires the worker out of the
/// park, and `dirty` names the ids that changed so the worker re-examines only the affected pending
/// work rather than rescanning everything.
struct Waker {
    /// Fires the interactive worker out of `step_or_park`. `Send`, minted by the worker via
    /// `sync_activator_for`.
    activator: SyncActivator,
    /// Ids marked dirty (published, re-exported, removed, or frontier-advanced) since the worker's
    /// last `take_dirty`.
    dirty: BTreeSet<GlobalId>,
    /// Coalescing flag: `true` once `activator` has fired without the worker having drained since.
    /// While set, further marks skip re-activating, so a burst of events between wakes collapses to
    /// one activation. `take_dirty` clears it. Mirrors `ArcActivator`'s pending flag.
    pending: bool,
}

/// The map of published slots, plus one [`Waker`] per interactive worker index.
///
/// The `map` and `wakers` locks are independent. The lost-wakeup argument that lets them stay
/// separate is documented on [`ArrangementSharingRegistry::notify`].
#[derive(Default)]
struct Inner {
    map: Mutex<BTreeMap<GlobalId, Vec<Option<Arc<SharedIndexArrangement>>>>>,
    /// Source id to the ids that re-export its arrangement (see [`ArrangementSharingRegistry::reexport`]).
    ///
    /// A re-exported id shares another index's arrangement and has no dataflow streams of its own,
    /// so it installs no seal-signal frontier tap. Its seal signal must ride on the source's tap:
    /// [`ArrangementSharingRegistry::notify`] wakes an id together with everything that re-exports it, transitively.
    /// Resolved outside the `wakers` lock, so it does not enter the lost-wakeup argument.
    aliases: Mutex<BTreeMap<GlobalId, BTreeSet<GlobalId>>>,
    /// Indexed by worker ordinal; `None` until that interactive worker registers its waker.
    wakers: Mutex<Vec<Option<Waker>>>,
    /// Per worker ordinal, the dataflows whose command-acquired read holds may be reclaimed.
    ///
    /// The command that releases a hold is routed to the runtime that renders the holding dataflow,
    /// while the hold itself lives on the runtime that owns the held collections. This carries the
    /// release across, since both runtimes' worker `i` share this registry. See
    /// [`ArrangementSharingRegistry::release_holder`].
    released_holders: Mutex<Vec<BTreeSet<GlobalId>>>,
}

/// Per-process registry of published index arrangements.
///
/// One slot per (`GlobalId`, worker ordinal). Cloning shares the same underlying map, so a clone
/// handed to each worker's `ComputeState` writes into the same registry.
///
/// The stored value is an `Arc<SharedIndexArrangement>` so the same publication can be registered
/// under several ids. A `Trace` re-export (one index aliasing another's arrangement) shares the
/// existing `Arc` rather than republishing.
#[derive(Clone, Default)]
pub struct ArrangementSharingRegistry {
    inner: Arc<Inner>,
}

impl ArrangementSharingRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the existing slot for `(id, worker_index)`, or creates one backed by unbacked
    /// `Published` placeholders ([`Published::placeholder`]) and returns that instead.
    ///
    /// Whichever side touches `(id, worker_index)` first creates the slot; the other observes and
    /// shares the same `Arc`, so a placeholder a reader already imported is filled in place by a
    /// later [`crate::shared_trace::PublishArrangement::adopt`] rather than being overwritten by a second, disconnected
    /// arrangement. Grows the slot vector to `peers` when `id` is not yet present.
    ///
    /// Creating a placeholder carries no data, so this does not `notify`: there is nothing yet for
    /// a waiting reader to act on. The caller that later fills the placeholder via `adopt` is
    /// responsible for calling `notify` once the fill is installed.
    pub fn get_or_create_placeholder(
        &self,
        id: GlobalId,
        worker_index: usize,
        peers: usize,
    ) -> Arc<SharedIndexArrangement> {
        let mut map = self.inner.map.lock().expect("registry poisoned");
        let slots = map
            .entry(id)
            .or_insert_with(|| (0..peers).map(|_| None).collect());
        Arc::clone(slots[worker_index].get_or_insert_with(|| {
            Arc::new(SharedIndexArrangement {
                oks: Published::placeholder(peers),
                errs: Published::placeholder(peers),
            })
        }))
    }

    /// Re-registers the arrangement published for `from` under `to`, on worker `worker_index`.
    ///
    /// Used by the `Trace` re-export path, where one index reuses another's arrangement. Shares the
    /// existing `Arc` under the new id rather than republishing. Returns `true` if `from` had a
    /// published arrangement on this worker to share.
    pub fn reexport(
        &self,
        from: &GlobalId,
        to: GlobalId,
        worker_index: usize,
        peers: usize,
    ) -> bool {
        {
            let mut map = self.inner.map.lock().expect("registry poisoned");
            let Some(arr) = map
                .get(from)
                .and_then(|slots| slots.get(worker_index))
                .and_then(|slot| slot.clone())
            else {
                return false;
            };
            let slots = map
                .entry(to)
                .or_insert_with(|| (0..peers).map(|_| None).collect());
            slots[worker_index] = Some(arr);
        }
        // Record `to` as a re-export of `from` so `from`'s seal signal wakes `to` as well. `to` has
        // no streams of its own to tap, so this alias is its only source of frontier notifications.
        {
            let mut aliases = self.inner.aliases.lock().expect("registry poisoned");
            aliases.entry(*from).or_default().insert(to);
        }
        // Mark dirty and fire the worker's waker after the slot write. The map lock is released
        // first, and the lost-wakeup argument for this ordering is on `notify`.
        self.notify(to, worker_index);
        true
    }

    /// Removes all slots for `id`, called when the index drops.
    pub fn remove(&self, id: &GlobalId) {
        {
            let mut map = self.inner.map.lock().expect("registry poisoned");
            map.remove(id);
        }
        {
            // Prune `id` only as a re-export target: a dropped target no longer needs waking. Do
            // NOT drop `id`'s own source entry here. A source's dataflow outlives its catalog drop
            // while a re-export still imports its arrangement (the source keeps sealing, and its tap
            // keeps firing `note_frontier`), and that trailing seal signal is the re-export's only
            // way to learn its arrangement advanced. The source entry is cleared when its last
            // target is pruned below, leaving it empty and inert.
            let mut aliases = self.inner.aliases.lock().expect("registry poisoned");
            for targets in aliases.values_mut() {
                targets.remove(id);
            }
            aliases.retain(|_, targets| !targets.is_empty());
        }
        // `remove` is not worker-specific: any interactive worker may have pending work on `id`, so
        // mark it dirty for every registered waker. A waiter re-checks and, finding the slot gone,
        // drops or keeps its item.
        self.notify_all(*id);
    }

    /// Mints reader handles for `id` on `worker_index`, if published.
    pub fn handles(
        &self,
        id: &GlobalId,
        worker_index: usize,
    ) -> Option<(SharedOksHandle, SharedErrsHandle)> {
        let map = self.inner.map.lock().expect("registry poisoned");
        Self::mint(&map, id, worker_index)
    }

    /// The `oks` logical holds registered against `id` on `worker_index`, if published.
    ///
    /// Test-only. Minting a handle to observe the published frontiers cannot distinguish a live
    /// reader hold from the publisher's writer-driven fallback, and that distinction is what says
    /// whether an import is still protected.
    #[cfg(test)]
    pub(crate) fn published_logical_holds(
        &self,
        id: &GlobalId,
        worker_index: usize,
    ) -> Option<Vec<Antichain<Timestamp>>> {
        let map = self.inner.map.lock().expect("registry poisoned");
        let slot = map.get(id)?.get(worker_index)?.as_ref()?;
        Some(slot.oks.logical_holds())
    }

    /// Registers `activator` as interactive worker `worker_index`'s waker, growing the waker vector
    /// as needed. Called once per interactive worker at startup.
    ///
    /// Overwrites any prior waker for that index, starting with an empty dirty set and a cleared
    /// coalescing flag.
    pub fn register_waker(&self, worker_index: usize, activator: SyncActivator) {
        let mut wakers = self.inner.wakers.lock().expect("registry poisoned");
        if worker_index >= wakers.len() {
            wakers.resize_with(worker_index + 1, || None);
        }
        wakers[worker_index] = Some(Waker {
            activator,
            dirty: BTreeSet::new(),
            pending: false,
        });
    }

    /// Atomically drains and returns worker `worker_index`'s dirty set, clearing its coalescing
    /// flag so the next event re-arms the waker. Returns empty if no waker is registered.
    ///
    /// Called by the interactive server loop on wake. See `notify` for why the loop MUST
    /// call this before re-reading the map: draining before the map re-check is what closes the
    /// lost-wakeup window.
    pub fn take_dirty(&self, worker_index: usize) -> BTreeSet<GlobalId> {
        let mut wakers = self.inner.wakers.lock().expect("registry poisoned");
        match wakers.get_mut(worker_index).and_then(|w| w.as_mut()) {
            Some(waker) => {
                waker.pending = false;
                std::mem::take(&mut waker.dirty)
            }
            None => BTreeSet::new(),
        }
    }

    /// Marks `id` dirty for interactive worker `worker_index` and fires its waker.
    ///
    /// The seal signal: maintenance calls this from an export's frontier probe when the shared
    /// trace's `upper` advances, so a fast-path peek waiting on the seal is re-examined. Delegates
    /// to `notify`.
    pub fn note_frontier(&self, id: GlobalId, worker_index: usize) {
        self.notify(id, worker_index);
    }

    /// Forwards the controller's logical compaction `frontier` for `id` into its published slot on
    /// `worker_index`, if one exists.
    ///
    /// Called from `handle_allow_compaction` alongside the local `TraceManager` update, so a
    /// cross-runtime publisher follows the controller's compaction without reading trace internals.
    /// The same frontier drives the index's `oks` and `errs`, matching `TraceManager::allow_compaction`.
    /// A no-op for unshared ids (no slot) and for a slot still holding placeholders (a placeholder's
    /// `note_writer_logical` simply records the floor a later `adopt` will honor). Does not `notify`:
    /// compaction bookkeeping alone gives a waiting reader nothing new to serve.
    pub fn note_allow_compaction(
        &self,
        id: GlobalId,
        worker_index: usize,
        frontier: &Antichain<Timestamp>,
    ) {
        let map = self.inner.map.lock().expect("registry poisoned");
        if let Some(arr) = map
            .get(&id)
            .and_then(|slots| slots.get(worker_index))
            .and_then(|slot| slot.as_ref())
        {
            arr.oks.note_writer_logical(frontier);
            arr.errs.note_writer_logical(frontier);
        }
    }

    /// Advances the standing hold on `id`'s published slot on `worker_index`, if one exists.
    ///
    /// Called from `handle_allow_compaction` on the runtime that may import `id` but does not host it,
    /// which reaches it because the multiplexer broadcasts `AllowCompaction`. The publisher bounds its
    /// logical compaction by this, so a frontier the importing runtime has not applied does not
    /// compact the arrangement.
    ///
    /// A no-op for ids with no slot on this worker. Nothing has been published there, so no import can
    /// have been built over it, and the frontier a later publisher seeds the hold with (its own
    /// compaction frontier at adoption) is at or below every `as_of` the controller may offer for it.
    /// Does not `notify`: compaction bookkeeping gives a waiting reader nothing new to serve.
    pub fn note_standing_hold(
        &self,
        id: GlobalId,
        worker_index: usize,
        frontier: &Antichain<Timestamp>,
    ) {
        let map = self.inner.map.lock().expect("registry poisoned");
        if let Some(arr) = map
            .get(&id)
            .and_then(|slots| slots.get(worker_index))
            .and_then(|slot| slot.as_ref())
        {
            arr.oks.note_standing_hold(frontier);
            arr.errs.note_standing_hold(frontier);
        }
    }

    /// Records that `holder`'s command-acquired read holds on worker `worker_index` may be
    /// reclaimed.
    ///
    /// Called from the release handler on the runtime that renders `holder`, which is where the
    /// release command is routed so that it is ordered against `holder`'s own lifecycle commands. The
    /// holds themselves sit on the runtime that owns the held collections, and it reclaims them from
    /// here through [`Self::reclaim_holder`].
    pub fn release_holder(&self, worker_index: usize, holder: GlobalId) {
        let mut released = self
            .inner
            .released_holders
            .lock()
            .expect("registry poisoned");
        if worker_index >= released.len() {
            released.resize_with(worker_index + 1, BTreeSet::new);
        }
        released[worker_index].insert(holder);
    }

    /// Whether `holder`'s holds on worker `worker_index` have been released, consuming the record if
    /// so.
    ///
    /// The two runtimes' command streams are independent, so a release can be processed on one before
    /// the matching acquisition is processed on the other. The record therefore persists until it is
    /// consumed, and the acquisition path consumes it too, declining to install a hold whose release
    /// has already arrived. Draining unconditionally would drop such a record and leak the hold that
    /// follows it.
    pub fn reclaim_holder(&self, worker_index: usize, holder: &GlobalId) -> bool {
        let mut released = self
            .inner
            .released_holders
            .lock()
            .expect("registry poisoned");
        match released.get_mut(worker_index) {
            Some(holders) => holders.remove(holder),
            None => false,
        }
    }

    /// The dataflows with an outstanding release record on worker `worker_index`.
    ///
    /// A snapshot for the reclaim pass to iterate, which consumes each record it matches through
    /// [`Self::reclaim_holder`]. Records for holders whose acquisition has not arrived yet stay.
    pub fn released_holders(&self, worker_index: usize) -> BTreeSet<GlobalId> {
        let released = self
            .inner
            .released_holders
            .lock()
            .expect("registry poisoned");
        released.get(worker_index).cloned().unwrap_or_default()
    }

    /// Discards worker `worker_index`'s outstanding release records.
    ///
    /// Called at the connection boundary, where the holds those records would have released are
    /// discarded too. A record that outlived its connection would be consumed by the next
    /// connection's acquisition for the same holder, which would install no hold at all.
    pub fn clear_released(&self, worker_index: usize) {
        let mut released = self
            .inner
            .released_holders
            .lock()
            .expect("registry poisoned");
        if let Some(holders) = released.get_mut(worker_index) {
            holders.clear();
        }
    }

    /// Marks `id` dirty for worker `worker_index` and fires its coalescing waker.
    ///
    /// Visible at `pub(crate)` so the maintenance publish path in `render.rs` can call it directly
    /// once it has adopted a placeholder's publication points: `get_or_create_placeholder` itself
    /// does not notify (see its doc comment), so the caller that fills the slot must.
    ///
    /// # Lost-wakeup contract
    ///
    /// `map` and `wakers` are separate locks. `reexport` writes the slot under `map`, releases it,
    /// then calls this under `wakers`. `get_or_create_placeholder` writes the slot the same way,
    /// under `map` then released, but its caller calls this separately once it has adopted the slot
    /// rather than immediately after the write. Either way, the slot write always
    /// precedes this call, which is all the argument below needs. On wake the interactive server
    /// loop runs `take_dirty` (under `wakers`) and only then re-reads the slot via `handles` (under
    /// `map`). Label the four steps: publisher P1 = slot write, P2 = this mark+activate; worker
    /// W1 = `take_dirty`, W2 = map re-read. Program order gives P1 -> P2 and W1 -> W2.
    ///
    /// The `map` lock totally orders P1 against W2, so the worker's re-read either observes the slot
    /// or does not:
    ///
    /// * W2 observes P1's write: the worker serves the work immediately, no park, no lost wake.
    /// * W2 precedes P1: the worker misses the slot and will park. Then W2 -> P1 combined with
    ///   W1 -> W2 and P1 -> P2 gives W1 -> P2, so this mark lands in a dirty set the worker has
    ///   ALREADY drained, sets `pending = true`, and activates. The activation is a retained unpark
    ///   token: the worker's next `step_or_park` returns at once (or never parks), it re-runs
    ///   `take_dirty` and sees `id`, re-reads the map (now past P1), and serves. No lost wake.
    ///
    /// The contradictory interleaving P2 -> W1 with W2 -> P1 is impossible: it would require
    /// P1 -> P2 -> W1 -> W2 -> P1, a cycle. Hence the drain-before-map-read ordering the server loop
    /// guarantees is exactly what makes two independent locks lost-wakeup-free.
    ///
    /// A failing `activate()` means the interactive worker has gone away, so there is nothing to
    /// wake and the dirty mark is harmless. It is a backstop, not the primary path.
    ///
    /// Marks `id` together with every id that re-exports it, transitively. A re-export has no tap of
    /// its own, so a peek waiting on its seal is re-examined only through this fan-out. The alias
    /// closure is computed under the `aliases` lock, which is released before the `wakers` lock is
    /// taken, so it stays outside the lost-wakeup argument.
    pub(crate) fn notify(&self, id: GlobalId, worker_index: usize) {
        let ids = self.notify_closure(id);
        let mut wakers = self.inner.wakers.lock().expect("registry poisoned");
        if let Some(waker) = wakers.get_mut(worker_index).and_then(|w| w.as_mut()) {
            for id in ids {
                Self::mark(waker, id);
            }
        }
    }

    /// `id` plus the transitive closure of ids that re-export it.
    fn notify_closure(&self, id: GlobalId) -> BTreeSet<GlobalId> {
        let aliases = self.inner.aliases.lock().expect("registry poisoned");
        let mut closure = BTreeSet::new();
        let mut frontier = vec![id];
        while let Some(next) = frontier.pop() {
            if closure.insert(next) {
                if let Some(targets) = aliases.get(&next) {
                    frontier.extend(targets.iter().copied());
                }
            }
        }
        closure
    }

    /// Marks `id` dirty for every registered worker and fires each coalescing waker. Used by
    /// `remove`, which is not worker-specific. Per worker, the lost-wakeup argument on
    /// [`Self::notify`] applies unchanged.
    fn notify_all(&self, id: GlobalId) {
        let mut wakers = self.inner.wakers.lock().expect("registry poisoned");
        for waker in wakers.iter_mut().flatten() {
            Self::mark(waker, id);
        }
    }

    /// Inserts `id` into `waker`'s dirty set and, if no activation is outstanding, arms the flag and
    /// fires the activator. The coalescing flag collapses a burst of marks into one activation.
    fn mark(waker: &mut Waker, id: GlobalId) {
        waker.dirty.insert(id);
        if !waker.pending {
            waker.pending = true;
            let _ = waker.activator.activate();
        }
    }

    /// Mints reader handles for `id` on `worker_index` from an already-locked map, if published.
    fn mint(
        map: &BTreeMap<GlobalId, Vec<Option<Arc<SharedIndexArrangement>>>>,
        id: &GlobalId,
        worker_index: usize,
    ) -> Option<(SharedOksHandle, SharedErrsHandle)> {
        let slot = map.get(id)?.get(worker_index)?.as_ref()?;
        Some((slot.oks.handle(), slot.errs.handle()))
    }

    /// Whether worker `worker_index`'s coalescing flag is armed. Lets tests assert that a burst of
    /// marks collapses to one activation without observing the (asynchronous) fire.
    #[cfg(test)]
    fn waker_pending(&self, worker_index: usize) -> bool {
        let wakers = self.inner.wakers.lock().expect("registry poisoned");
        wakers
            .get(worker_index)
            .and_then(|w| w.as_ref())
            .is_some_and(|w| w.pending)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    use differential_dataflow::input::Input;
    use differential_dataflow::trace::{BatchReader, Cursor, TraceReader};
    use mz_repr::{Datum, Row};
    use mz_row_spine::{RowRowBatcher, RowRowBuilder};
    use mz_timely_util::columnation::ColumnationChunker;
    use timely::dataflow::ProbeHandle;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, Probe};
    use timely::progress::Antichain;

    use crate::extensions::arrange::{KeyCollection, MzArrange};
    use crate::render::context::ArrangementFlavor;
    use crate::render::errors::DataflowErrorSer;
    use crate::shared_trace::PublishArrangement;
    use crate::typedefs::{ErrBatcher, ErrBuilder};

    use super::*;

    /// Builds a tiny dataflow that arranges `rows` into a `RowRow` `oks` arrangement and an empty
    /// `errs` arrangement, publishes both, and returns a registry that holds them under `id` on
    /// worker 0 (of 1). The dataflow runs to completion inside `execute_directly`; the returned
    /// registry keeps the published chains alive through their `Arc`s, so the snapshot reads below
    /// observe the sealed contents even after the publishing worker has torn down.
    fn publish_index(id: GlobalId, rows: Vec<(Row, Row)>) -> ArrangementSharingRegistry {
        let registry = ArrangementSharingRegistry::new();
        publish_index_into(&registry, id, rows);
        registry
    }

    /// Like `publish_index`, but publishes into the given `registry` instead of a fresh one, so a
    /// caller can hand the same registry to a concurrent reader before publication happens.
    ///
    /// Routes through `get_or_create_placeholder` and `PublishArrangement::adopt`, the path the
    /// maintenance render side uses. Exercises the get-or-create half of the convergence contract:
    /// whatever slot `get_or_create_placeholder` returns (existing or freshly created) is the one
    /// that gets filled.
    fn publish_index_into(
        registry: &ArrangementSharingRegistry,
        id: GlobalId,
        rows: Vec<(Row, Row)>,
    ) {
        let registry_in = registry.clone();
        timely::execute_directly(move |worker| {
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let (mut oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
                let oks = oks_collection.mz_arrange::<
                    ColumnationChunker<_>,
                    RowRowBatcher<_, _>,
                    RowRowBuilder<_, _>,
                    RowRowSpine<_, _>,
                >("test oks");

                let (mut errs_input, errs_collection) =
                    scope.new_collection::<DataflowErrorSer, Diff>();
                let errs = KeyCollection::from(errs_collection).mz_arrange::<
                    ColumnationChunker<_>,
                    ErrBatcher<_, _>,
                    ErrBuilder<_, _>,
                    ErrSpine<_, _>,
                >("test errs");

                let slot = registry_in.get_or_create_placeholder(id, 0, 1);
                PublishArrangement::adopt(&oks, &slot.oks, || {});
                PublishArrangement::adopt(&errs, &slot.errs, || {});
                registry_in.notify(id, 0);

                for (k, v) in rows {
                    oks_input.update((k, v), Diff::ONE);
                }
                oks_input.advance_to(Timestamp::from(1_u64));
                oks_input.flush();
                errs_input.advance_to(Timestamp::from(1_u64));
                errs_input.flush();
            });
        });
    }

    fn test_rows() -> Vec<(Row, Row)> {
        vec![
            (
                Row::pack_slice(&[Datum::Int32(1)]),
                Row::pack_slice(&[Datum::String("a")]),
            ),
            (
                Row::pack_slice(&[Datum::Int32(2)]),
                Row::pack_slice(&[Datum::String("b")]),
            ),
        ]
    }

    #[mz_ore::test]
    fn get_or_create_converges_on_one_slot() {
        // A reader and a publisher both touch the same id. Whichever is first creates the
        // placeholder; the second must observe the same Arc, not a second slot, and after
        // adoption the reader sees the published rows.
        let id = GlobalId::User(1);
        let registry = ArrangementSharingRegistry::new();

        // Reader creates the placeholder first and mints its handle straight off it.
        // `SharedIndexArrangement` has no `handles_for_worker`: a slot returned by
        // `get_or_create_placeholder` is already scoped to one worker, so its handles come
        // directly off `Published::handle`, the same way `ArrangementSharingRegistry::mint`
        // builds them once a slot is located.
        let slot = registry.get_or_create_placeholder(id, 0, 1);
        let oks = slot.oks.handle();

        // A second get_or_create_placeholder call for the same (id, worker_index) must return
        // the SAME Arc, not a second, disconnected slot, before we even get to the adopt below.
        let republished = registry.get_or_create_placeholder(id, 0, 1);
        assert!(Arc::ptr_eq(&slot, &republished));

        // Publisher adopts the same slot and fills it.
        publish_index_into(&registry, id, test_rows());

        assert_eq!(
            read_rows(&oks, Timestamp::from(0_u64)),
            expected_rows(&test_rows())
        );
    }

    #[mz_ore::test]
    fn handles_available_after_insert_gone_after_remove() {
        let id = GlobalId::User(1);
        let registry = publish_index(id, test_rows());

        // A published index yields handles on its worker, and no handles on an unpublished worker
        // slot or an unknown id.
        assert!(registry.handles(&id, 0).is_some());
        assert!(registry.handles(&id, 1).is_none());
        assert!(registry.handles(&GlobalId::User(2), 0).is_none());

        registry.remove(&id);
        assert!(registry.handles(&id, 0).is_none());
    }

    #[mz_ore::test]
    fn reexport_shares_arrangement_under_new_id() {
        let id = GlobalId::User(1);
        let alias = GlobalId::User(2);
        let registry = publish_index(id, test_rows());

        assert!(registry.reexport(&id, alias, 0, 1));
        assert!(registry.handles(&alias, 0).is_some());
        // Removing the alias leaves the original registered.
        registry.remove(&alias);
        assert!(registry.handles(&id, 0).is_some());
        // Re-export of an unpublished id reports that nothing was shared.
        assert!(!registry.reexport(&GlobalId::User(9), GlobalId::User(10), 0, 1));
    }

    /// Walks a snapshot of `handle` at `at` into a sorted `Vec` of owned (key, value) rows,
    /// keeping only entries whose accumulated diff at `at` is nonzero.
    ///
    /// Packs each key/value into an owned `Row` rather than returning borrowed `Datum`s, since the
    /// snapshot's `storage` is local to this function.
    fn read_rows(handle: &SharedOksHandle, at: Timestamp) -> Vec<(Row, Row)> {
        let snapshot = handle.snapshot_at(&at).expect("snapshot at sealed time");

        let (mut cursor, storage) = snapshot.cursor();
        let mut found: Vec<(Row, Row)> = Vec::new();
        while cursor.key_valid(&storage) {
            while cursor.val_valid(&storage) {
                let key = Row::pack_slice(&cursor.key(&storage).into_iter().collect::<Vec<_>>());
                let val = Row::pack_slice(&cursor.val(&storage).into_iter().collect::<Vec<_>>());
                let mut diff = Diff::ZERO;
                cursor.map_times(&storage, |_t, d| diff += d);
                if !diff.is_zero() {
                    found.push((key, val));
                }
                cursor.step_val(&storage);
            }
            cursor.step_key(&storage);
        }
        found.sort();
        found
    }

    /// The row shape `read_rows` returns for the same `(Row, Row)` pairs given to
    /// `publish_index`/`publish_index_into`, so a test can compare the two directly.
    fn expected_rows(rows: &[(Row, Row)]) -> Vec<(Row, Row)> {
        let mut expected = rows.to_vec();
        expected.sort();
        expected
    }

    #[mz_ore::test]
    fn minted_handle_snapshots_the_index_rows() {
        let id = GlobalId::User(1);
        let rows = test_rows();
        let registry = publish_index(id, rows.clone());

        // The rows were written at time 0 and sealed by advancing the input to 1.
        let (oks, _errs) = registry.handles(&id, 0).expect("published");
        assert_eq!(
            read_rows(&oks, Timestamp::from(0_u64)),
            expected_rows(&rows)
        );
    }

    #[mz_ore::test]
    fn cross_runtime_read_sees_published_rows() {
        let id = GlobalId::User(1);
        let rows = test_rows();
        let registry = ArrangementSharingRegistry::new();

        // Runtime A: a bare timely cluster that arranges and publishes the rows, then tears down.
        publish_index_into(&registry, id, rows.clone());

        // Runtime B: read the published rows from a different thread than the one that ran A's
        // dataflow, exercising the `Send` handle across a runtime boundary.
        let reader_registry = registry.clone();
        let found = thread::spawn(move || {
            let (oks, _errs) = reader_registry.handles(&id, 0).expect("published by A");
            read_rows(&oks, Timestamp::from(0_u64))
        })
        .join()
        .expect("reader thread panicked");

        assert_eq!(found, expected_rows(&rows));
    }

    /// A single-worker timely cluster kept alive on a background thread so registry tests can
    /// register a real, cross-thread [`SyncActivator`] and exercise `notify`'s live `activate()`
    /// path. The worker builds one no-op source operator, exports that operator's `SyncActivator`,
    /// and then parks until `done` is set. Dropping the handle sets `done`, nudges the worker out of
    /// its park, and joins the thread.
    ///
    /// The tests below assert only the dirty-set and coalescing semantics, not that a fire wakes the
    /// worker. That the fire unparks the target worker is proved by `sync_activator_fires_cross_thread`
    /// and covered end to end by the integration tests. Registering a live activator here keeps the
    /// `activate()` call on its non-erroring path so the tests exercise real wakers rather than
    /// already-hung-up ones.
    struct ParkedWorker {
        activator: timely::scheduling::activate::SyncActivator,
        done: Arc<std::sync::atomic::AtomicBool>,
        handle: Option<thread::JoinHandle<()>>,
    }

    impl ParkedWorker {
        fn new() -> Self {
            use std::sync::atomic::{AtomicBool, Ordering};

            use timely::container::CapacityContainerBuilder;
            use timely::dataflow::operators::generic::source;
            use timely::scheduling::activate::SyncActivator;

            let done = Arc::new(AtomicBool::new(false));
            let done_worker = Arc::clone(&done);
            let (act_tx, act_rx) = mpsc::channel::<SyncActivator>();
            // `execute_directly` needs a `Send + Sync` closure; `Sender` is not `Sync`.
            let act_tx = std::sync::Mutex::new(act_tx);

            let handle = thread::spawn(move || {
                timely::execute_directly(move |worker| {
                    worker.dataflow::<Timestamp, _, _>(|scope| {
                        let scope_handle = scope.clone();
                        let act_tx = act_tx.lock().unwrap().clone();
                        let done = Arc::clone(&done_worker);
                        let _stream = source::<_, CapacityContainerBuilder<Vec<()>>, _, _>(
                            scope,
                            "test-waker-source",
                            move |cap, info| {
                                let activator = scope_handle
                                    .worker()
                                    .sync_activator_for(info.address.to_vec());
                                act_tx.send(activator).expect("test receives activator");
                                // Hold the capability so the operator stays reschedulable while
                                // parked; release it once `done` is set so the dataflow drains. The
                                // `take` is a side effect (dropping the capability), not a read.
                                #[allow(clippy::collection_is_never_read)]
                                let mut cap = Some(cap);
                                move |_output| {
                                    if done.load(Ordering::SeqCst) {
                                        cap.take();
                                    }
                                }
                            },
                        );
                    });

                    while !done_worker.load(Ordering::SeqCst) {
                        worker.step_or_park(Some(Duration::from_millis(100)));
                    }
                });
            });

            let activator = act_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("worker exports its activator");
            Self {
                activator,
                done,
                handle: Some(handle),
            }
        }

        /// A clone of worker 0's activator, for [`ArrangementSharingRegistry::register_waker`].
        fn activator(&self) -> timely::scheduling::activate::SyncActivator {
            self.activator.clone()
        }
    }

    impl Drop for ParkedWorker {
        fn drop(&mut self) {
            self.done.store(true, std::sync::atomic::Ordering::SeqCst);
            // Nudge the worker out of `step_or_park` so it observes `done` and shuts down.
            let _ = self.activator.activate();
            if let Some(handle) = self.handle.take() {
                handle.join().expect("worker thread panicked");
            }
        }
    }

    #[mz_ore::test]
    fn insert_marks_dirty_and_take_drains() {
        let id = GlobalId::User(1);
        let worker = ParkedWorker::new();
        let registry = ArrangementSharingRegistry::new();
        registry.register_waker(0, worker.activator());

        // Publication on worker 0 marks `id` dirty for worker 0.
        publish_index_into(&registry, id, test_rows());
        assert_eq!(registry.take_dirty(0), BTreeSet::from([id]));
        // A second drain returns nothing: `take_dirty` empties the inbox.
        assert!(registry.take_dirty(0).is_empty());
    }

    #[mz_ore::test]
    fn insert_dirties_only_its_worker() {
        let id = GlobalId::User(1);
        let worker0 = ParkedWorker::new();
        let worker1 = ParkedWorker::new();
        let registry = ArrangementSharingRegistry::new();
        registry.register_waker(0, worker0.activator());
        registry.register_waker(1, worker1.activator());

        // `publish_index_into` inserts on worker 0, so only worker 0's inbox is dirtied.
        publish_index_into(&registry, id, test_rows());
        assert_eq!(registry.take_dirty(0), BTreeSet::from([id]));
        assert!(registry.take_dirty(1).is_empty());
    }

    /// A release record survives until the runtime holding the hold consumes it, and is consumed
    /// exactly once.
    ///
    /// The release travels on one runtime's command stream and the acquisition on the other's, so the
    /// release can be recorded before the acquisition is applied. A record that drained on the first
    /// look would be gone by the time the acquisition arrived, and that hold would then never be
    /// released.
    #[mz_ore::test]
    fn release_record_persists_until_consumed() {
        let holder = GlobalId::User(1);
        let other = GlobalId::User(2);
        let registry = ArrangementSharingRegistry::new();

        registry.release_holder(0, holder);
        assert_eq!(registry.released_holders(0), BTreeSet::from([holder]));
        // Listing does not consume, so a pass that finds no matching hold leaves the record for the
        // acquisition that has not arrived yet.
        assert_eq!(registry.released_holders(0), BTreeSet::from([holder]));

        assert!(registry.reclaim_holder(0, &holder));
        assert!(
            !registry.reclaim_holder(0, &holder),
            "a record must be consumed exactly once"
        );
        assert!(registry.released_holders(0).is_empty());
        assert!(!registry.reclaim_holder(0, &other));
    }

    /// Release records are per worker ordinal, since each worker holds its own slice of a
    /// collection and installs its own hold on it.
    #[mz_ore::test]
    fn release_record_is_per_worker() {
        let holder = GlobalId::User(1);
        let registry = ArrangementSharingRegistry::new();

        registry.release_holder(1, holder);
        assert!(registry.released_holders(0).is_empty());
        assert!(!registry.reclaim_holder(0, &holder));
        assert!(registry.reclaim_holder(1, &holder));
    }

    #[mz_ore::test]
    fn remove_dirties_all_registered_workers() {
        let id = GlobalId::User(1);
        let worker0 = ParkedWorker::new();
        let worker1 = ParkedWorker::new();
        let registry = ArrangementSharingRegistry::new();
        registry.register_waker(0, worker0.activator());
        registry.register_waker(1, worker1.activator());

        // Drain the publication signal so the remove signal is observed in isolation.
        publish_index_into(&registry, id, test_rows());
        let _ = registry.take_dirty(0);

        // `remove` is not worker-specific: every registered worker must re-check `id`.
        registry.remove(&id);
        assert_eq!(registry.take_dirty(0), BTreeSet::from([id]));
        assert_eq!(registry.take_dirty(1), BTreeSet::from([id]));
    }

    #[mz_ore::test]
    fn note_frontier_dirties_its_worker() {
        let id = GlobalId::User(1);
        let worker = ParkedWorker::new();
        let registry = ArrangementSharingRegistry::new();
        registry.register_waker(0, worker.activator());

        // The seal signal marks the id dirty for its worker without requiring publication.
        registry.note_frontier(id, 0);
        assert_eq!(registry.take_dirty(0), BTreeSet::from([id]));
    }

    #[mz_ore::test]
    fn note_frontier_fans_out_to_reexports() {
        let src = GlobalId::User(1);
        let alias = GlobalId::User(2);
        let worker = ParkedWorker::new();
        let registry = ArrangementSharingRegistry::new();
        registry.register_waker(0, worker.activator());

        // Publish `src` and re-export it under `alias`. The re-export has no tap of its own, so its
        // only seal signal is `src`'s. Drain the publication marks first.
        publish_index_into(&registry, src, test_rows());
        assert!(registry.reexport(&src, alias, 0, 1));
        let _ = registry.take_dirty(0);

        // A frontier advance on the source wakes both the source and the re-export.
        registry.note_frontier(src, 0);
        assert_eq!(registry.take_dirty(0), BTreeSet::from([src, alias]));

        // A frontier advance on the re-export alone does not spuriously wake the source.
        registry.note_frontier(alias, 0);
        assert_eq!(registry.take_dirty(0), BTreeSet::from([alias]));

        // Dropping the source's catalog entry does not sever the fan-out: the source's dataflow
        // outlives the drop while the re-export still imports it, and its tap keeps sealing the
        // re-export. This is the `DROP INDEX` case where one of two same-key indexes is removed.
        registry.remove(&src);
        let _ = registry.take_dirty(0);
        registry.note_frontier(src, 0);
        assert_eq!(registry.take_dirty(0), BTreeSet::from([src, alias]));

        // Once the re-export target itself drops, the fan-out stops.
        registry.remove(&alias);
        let _ = registry.take_dirty(0);
        registry.note_frontier(src, 0);
        assert_eq!(registry.take_dirty(0), BTreeSet::from([src]));
    }

    #[mz_ore::test]
    fn notifications_coalesce_until_taken() {
        let id1 = GlobalId::User(1);
        let id2 = GlobalId::User(2);
        let id3 = GlobalId::User(3);
        let worker = ParkedWorker::new();
        let registry = ArrangementSharingRegistry::new();
        registry.register_waker(0, worker.activator());

        // The first notification arms the coalescing flag (one activation outstanding).
        registry.note_frontier(id1, 0);
        assert!(registry.waker_pending(0));
        // A second notification before the worker drains stays coalesced: still one activation.
        registry.note_frontier(id2, 0);
        assert!(registry.waker_pending(0));

        // Draining returns both accumulated ids and disarms the flag.
        assert_eq!(registry.take_dirty(0), BTreeSet::from([id1, id2]));
        assert!(!registry.waker_pending(0));

        // A notification after the drain re-arms the flag.
        registry.note_frontier(id3, 0);
        assert!(registry.waker_pending(0));
        assert_eq!(registry.take_dirty(0), BTreeSet::from([id3]));
    }

    /// An input update: `(key, value, time, diff)`. Keys and values are single-column rows, so a
    /// join emits a three-column `(key, value1, value2)` row that we can compare directly.
    type Update = (i64, &'static str, u64, i64);

    fn key_row(k: i64) -> Row {
        Row::pack_slice(&[Datum::Int64(k)])
    }

    fn val_row(v: &str) -> Row {
        Row::pack_slice(&[Datum::String(v)])
    }

    /// The join of `a` and `b` computed directly, consolidated per `(row, time)`.
    ///
    /// Matches the differential contract: a pair of matching updates produces one output at the
    /// lattice join (here, the max) of their times, with the product of their diffs. This is the
    /// oracle the imported-arrangement join must reproduce.
    fn expected_join(a: &[Update], b: &[Update]) -> Vec<(Row, Timestamp, Diff)> {
        let mut out: BTreeMap<(Row, Timestamp), Diff> = BTreeMap::new();
        for &(ka, la, ta, da) in a {
            for &(kb, rb, tb, db) in b {
                if ka != kb {
                    continue;
                }
                let row =
                    Row::pack_slice(&[Datum::Int64(ka), Datum::String(la), Datum::String(rb)]);
                let time = Timestamp::from(ta.max(tb));
                *out.entry((row, time)).or_insert(Diff::ZERO) += Diff::from(da * db);
            }
        }
        let mut v: Vec<_> = out
            .into_iter()
            .filter(|(_, d)| !d.is_zero())
            .map(|((row, t), d)| (row, t, d))
            .collect();
        v.sort();
        v
    }

    /// Publishes `updates` as a `RowRow` index under `id`, driving the input across its distinct
    /// times and stepping the worker between them so the trace seals several batches. An empty
    /// `errs` arrangement is published alongside, as the registry slot requires both halves.
    ///
    /// `seal` is one past the last update time, the frontier at which the last batch closes.
    fn publish_join_input(
        registry: &ArrangementSharingRegistry,
        worker: &mut timely::worker::Worker,
        id: GlobalId,
        updates: &[Update],
        seal: u64,
    ) -> impl FnMut(&mut timely::worker::Worker) + use<> {
        let registry_in = registry.clone();
        let worker_index = worker.index();
        let peers = worker.peers();
        let updates = updates.to_vec();

        let (mut oks_input, mut errs_input) = worker.dataflow::<Timestamp, _, _>(move |scope| {
            let (oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
            let oks = oks_collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("input oks");

            let (errs_input, errs_collection) = scope.new_collection::<DataflowErrorSer, Diff>();
            let errs = KeyCollection::from(errs_collection).mz_arrange::<
                ColumnationChunker<_>,
                ErrBatcher<_, _>,
                ErrBuilder<_, _>,
                ErrSpine<_, _>,
            >("input errs");

            let slot = registry_in.get_or_create_placeholder(id, worker_index, peers);
            PublishArrangement::adopt(&oks, &slot.oks, || {});
            PublishArrangement::adopt(&errs, &slot.errs, || {});
            registry_in.notify(id, worker_index);
            (oks_input, errs_input)
        });

        // Distinct update times in order. Insert each time's updates, then advance and step, so the
        // publisher seals and appends one batch per time rather than one batch for everything.
        let mut times: Vec<u64> = updates.iter().map(|&(_, _, t, _)| t).collect();
        times.sort_unstable();
        times.dedup();

        for &t in &times {
            oks_input.advance_to(Timestamp::from(t));
            for &(k, v, ut, d) in &updates {
                if ut == t {
                    oks_input.update((key_row(k), val_row(v)), Diff::from(d));
                }
            }
            oks_input.flush();
            // Step so the arrange operator observes this frontier and the publisher appends the
            // sealed batch to importer queues before the next time is loaded.
            for _ in 0..16 {
                worker.step();
            }
        }
        oks_input.advance_to(Timestamp::from(seal));
        oks_input.flush();
        errs_input.advance_to(Timestamp::from(seal));
        errs_input.flush();

        // Return a closure that keeps the input handles alive and continues stepping. Dropping the
        // handles would drop the inputs and let the publisher dataflow drain to the empty frontier,
        // closing the publication before the importer has read it.
        //
        // Each call also advances the inputs to a fresh filler time. Stepping alone is not enough to
        // run the publisher: timely only schedules its sink when its input is active, so an
        // out-of-band change such as a controller `AllowCompaction` landing in `writer_logical` is not
        // picked up until something ticks the dataflow. A live index in production always has that
        // tick. The filler times carry no updates, so they add empty seal-only batches and advance
        // `upper` without changing any accumulation.
        let mut filler = seal;
        move |worker: &mut timely::worker::Worker| {
            filler += 1;
            oks_input.advance_to(Timestamp::from(filler));
            oks_input.flush();
            errs_input.advance_to(Timestamp::from(filler));
            errs_input.flush();
            worker.step();
        }
    }

    /// The core spike: a maintenance-published index consumed *as an arrangement* by a join.
    ///
    /// Two `RowRow` indexes are published (each sealing several batches), imported through
    /// `SharedTraceHandle::import_snapshot_at` over the full `[0, seal)` range so every distinct time
    /// stays visible, and joined with differential's `join_core`, which drives the same
    /// `cursor_through`/`batches_through` boundary as `mz_join_core`. The join runs live alongside
    /// the publishers in one worker, so the imported batches arrive incrementally and the join
    /// performs incremental `cursor_through` cuts as its acknowledged frontiers advance. The captured
    /// output must equal the join computed directly.
    ///
    /// Publisher and importer share a worker only to step in lockstep for a deterministic read. The
    /// handle that crosses between them is `Send` and the code exercised (import replay, chain cut)
    /// is identical to a true second runtime.
    #[mz_ore::test]
    fn join_over_imported_arrangements_matches_direct() {
        let id_a = GlobalId::User(1);
        let id_b = GlobalId::User(2);

        // A: key 1 inserted then retracted, plus keys 2 and 3 at later times.
        let a: Vec<Update> = vec![
            (1, "a", 0, 1),
            (2, "b", 0, 1),
            (3, "c", 1, 1),
            (1, "a", 2, -1),
        ];
        // B: one value per key, appearing at staggered times.
        let b: Vec<Update> = vec![(1, "x", 0, 1), (2, "y", 1, 1), (3, "z", 2, 1)];
        let seal = 3;

        let expected = expected_join(&a, &b);

        let (capture_tx, capture_rx) = mpsc::channel();

        timely::execute_directly(move |worker| {
            let registry = ArrangementSharingRegistry::new();

            // Maintenance side: publish both indexes, sealing several batches each.
            let mut keep_a = publish_join_input(&registry, worker, id_a, &a, seal);
            let mut keep_b = publish_join_input(&registry, worker, id_b, &b, seal);

            let worker_index = worker.index();
            let (oks_a, _errs_a) = registry.handles(&id_a, worker_index).expect("A published");
            let (oks_b, _errs_b) = registry.handles(&id_b, worker_index).expect("B published");

            // Interactive side: import both as arrangements and join them. `as_of = 0` matches the
            // earliest real time in either input, so no update coalesces; `until = seal` keeps every
            // distinct time in `[0, seal)` visible.
            let as_of = Antichain::from_elem(Timestamp::from(0_u64));
            let until = Antichain::from_elem(Timestamp::from(seal));
            let probe = ProbeHandle::new();
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let arr_a = oks_a.import_snapshot_at(
                    scope.clone(),
                    "import A",
                    as_of.clone(),
                    until.clone(),
                );
                let arr_b = oks_b.import_snapshot_at(scope.clone(), "import B", as_of, until);
                let joined = arr_a.join_core(arr_b, |key, v1, v2| {
                    let row =
                        Row::pack(key.into_iter().chain(v1.into_iter()).chain(v2.into_iter()));
                    Some(row)
                });
                joined
                    .inner
                    .probe_with(&probe)
                    .capture_into(capture_tx.clone());
            });

            // Step until the join has sealed through the seal frontier, keeping the publisher
            // inputs alive so their publication points stay open.
            let seal_ts = Timestamp::from(seal);
            let mut steps = 0;
            while probe.less_than(&seal_ts) {
                keep_a(worker);
                keep_b(worker);
                worker.step();
                steps += 1;
                assert!(steps < 10_000, "join did not seal through {seal_ts:?}");
            }
        });

        let mut got: Vec<(Row, Timestamp, Diff)> = capture_rx
            .extract()
            .into_iter()
            .flat_map(|(_, data)| data)
            .collect();
        // Consolidate the captured stream per `(row, time)` so we compare final deltas.
        got.sort();
        let mut consolidated: BTreeMap<(Row, Timestamp), Diff> = BTreeMap::new();
        for (row, time, diff) in got {
            *consolidated.entry((row, time)).or_insert(Diff::ZERO) += diff;
        }
        let got: Vec<(Row, Timestamp, Diff)> = consolidated
            .into_iter()
            .filter(|(_, d)| !d.is_zero())
            .map(|((row, t), d)| (row, t, d))
            .collect();

        assert_eq!(
            got, expected,
            "join over imported arrangements diverged from the direct join"
        );
    }

    /// Renders a `RowRow` index that ADOPTS an existing `placeholder` publication point, driving the
    /// input across its distinct times and stepping between them so the trace seals several batches.
    ///
    /// Mirrors [`publish_join_input`], but instead of minting a fresh publication and registering it,
    /// it installs its publisher into the caller-provided `placeholder` via
    /// [`PublishArrangement::adopt`]. The placeholder may already back live importers (see
    /// [`join_over_placeholder_adopted_late_matches_direct`]). Adoption fills their queues from the
    /// same publisher iteration. Only the `oks` arrangement is adopted, since the test joins on `oks`.
    fn adopt_join_input(
        placeholder: &Published<RowRowSpine<Timestamp, Diff>>,
        worker: &mut timely::worker::Worker,
        updates: &[Update],
        seal: u64,
    ) -> impl FnMut(&mut timely::worker::Worker) + use<> {
        let updates = updates.to_vec();

        let mut oks_input = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
            let oks = oks_collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("adopt oks");
            // Install this arrangement's publisher into the pre-existing placeholder Arc, rather than
            // minting a fresh publication point. Importers already registered against the placeholder
            // (built before this call) now begin to fill.
            PublishArrangement::adopt(&oks, placeholder, || {});
            oks_input
        });

        let mut times: Vec<u64> = updates.iter().map(|&(_, _, t, _)| t).collect();
        times.sort_unstable();
        times.dedup();
        for &t in &times {
            oks_input.advance_to(Timestamp::from(t));
            for &(k, v, ut, d) in &updates {
                if ut == t {
                    oks_input.update((key_row(k), val_row(v)), Diff::from(d));
                }
            }
            oks_input.flush();
            for _ in 0..16 {
                worker.step();
            }
        }
        oks_input.advance_to(Timestamp::from(seal));
        oks_input.flush();

        move |worker: &mut timely::worker::Worker| {
            let _keep = &oks_input;
            worker.step();
        }
    }

    /// A differential join whose input trace is a PLACEHOLDER at construction fills correctly once
    /// the placeholder is adopted in place.
    ///
    /// This reproduces the command-arrival-order hazard directly. `id_a` is imported and joined
    /// BEFORE any publisher for it exists: the interactive side mints a placeholder, takes a handle,
    /// imports it, and builds `join_core` over the EMPTY placeholder, capturing the trace by value at
    /// construction. `id_b` is published normally as an already-materialized co-input.
    ///
    /// The test asserts two things:
    /// * While `a` is unadopted, the join produces nothing and its frontier stays pinned at the
    ///   minimum (held by the placeholder import at `upper = [0]`).
    /// * After the maintenance side renders `a`'s arrangement and ADOPTS the same `Arc` (installing a
    ///   publisher that fills the already-registered importer queue), the captured output equals the
    ///   direct join, with correct multiplicities and no doubling.
    #[mz_ore::test]
    fn join_over_placeholder_adopted_late_matches_direct() {
        let id_b = GlobalId::User(2);

        // Same inputs as `join_over_imported_arrangements_matches_direct`: key 1 inserted then
        // retracted, plus keys 2 and 3, joined against one value per key.
        let a: Vec<Update> = vec![
            (1, "a", 0, 1),
            (2, "b", 0, 1),
            (3, "c", 1, 1),
            (1, "a", 2, -1),
        ];
        let b: Vec<Update> = vec![(1, "x", 0, 1), (2, "y", 1, 1), (3, "z", 2, 1)];
        let seal = 3;

        let expected = expected_join(&a, &b);

        let (capture_tx, capture_rx) = mpsc::channel();

        timely::execute_directly(move |worker| {
            let registry = ArrangementSharingRegistry::new();
            let peers = worker.peers();
            let worker_index = worker.index();

            // B: published normally, an already-materialized co-input.
            let mut keep_b = publish_join_input(&registry, worker, id_b, &b, seal);
            let (oks_b, _errs_b) = registry.handles(&id_b, worker_index).expect("B published");

            // A: a PLACEHOLDER, created before any publisher exists. Mint its reader handle now.
            let placeholder_a: Published<RowRowSpine<Timestamp, Diff>> =
                Published::placeholder(peers);
            let oks_a = placeholder_a.handle();

            // Interactive side: import both as arrangements and join them. A is imported over the
            // EMPTY placeholder. `join_core` captures `arr_a.trace` by value here, before A has any
            // publisher. This is exactly the construction-time capture that late-binding import must
            // survive. `as_of = 0` matches the earliest real time in either input, so no update
            // coalesces; `until = seal` keeps every distinct time in `[0, seal)` visible.
            let as_of = Antichain::from_elem(Timestamp::from(0_u64));
            let until = Antichain::from_elem(Timestamp::from(seal));
            let probe = ProbeHandle::new();
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let arr_a = oks_a.import_snapshot_at(
                    scope.clone(),
                    "import A (placeholder)",
                    as_of.clone(),
                    until.clone(),
                );
                let arr_b = oks_b.import_snapshot_at(scope.clone(), "import B", as_of, until);
                let joined = arr_a.join_core(arr_b, |key, v1, v2| {
                    let row =
                        Row::pack(key.into_iter().chain(v1.into_iter()).chain(v2.into_iter()));
                    Some(row)
                });
                joined
                    .inner
                    .probe_with(&probe)
                    .capture_into(capture_tx.clone());
            });

            // Step with A still unadopted. The placeholder import holds A's frontier at the minimum,
            // so the join frontier cannot pass 0 and no output is produced.
            for _ in 0..64 {
                keep_b(worker);
                worker.step();
            }
            assert!(
                probe.less_than(&Timestamp::from(1_u64)),
                "join advanced past time 0 before A was adopted"
            );

            // Maintenance side: NOW render A's arrangement and ADOPT the same placeholder Arc, feeding
            // A's updates. The join built above must observe the filled chain through its captured
            // handle without being rebuilt.
            let mut keep_a = adopt_join_input(&placeholder_a, worker, &a, seal);

            // Step until the join has sealed through the seal frontier.
            let seal_ts = Timestamp::from(seal);
            let mut steps = 0;
            while probe.less_than(&seal_ts) {
                keep_a(worker);
                keep_b(worker);
                worker.step();
                steps += 1;
                assert!(
                    steps < 10_000,
                    "join did not seal through {seal_ts:?} after adopt"
                );
            }
        });

        let mut got: Vec<(Row, Timestamp, Diff)> = capture_rx
            .extract()
            .into_iter()
            .flat_map(|(_, data)| data)
            .collect();
        got.sort();
        let mut consolidated: BTreeMap<(Row, Timestamp), Diff> = BTreeMap::new();
        for (row, time, diff) in got {
            *consolidated.entry((row, time)).or_insert(Diff::ZERO) += diff;
        }
        let got: Vec<(Row, Timestamp, Diff)> = consolidated
            .into_iter()
            .filter(|(_, d)| !d.is_zero())
            .map(|((row, t), d)| (row, t, d))
            .collect();

        assert_eq!(
            got, expected,
            "join over a late-adopted placeholder diverged from the direct join"
        );
    }

    /// Consolidates a captured `(Row, Timestamp, Diff)` stream per `(row, time)`, dropping entries
    /// whose accumulated diff is zero, and returns them sorted. Shared by the assertions below.
    fn consolidate_capture(
        rx: mpsc::Receiver<
            timely::dataflow::operators::capture::Event<Timestamp, Vec<(Row, Timestamp, Diff)>>,
        >,
    ) -> Vec<(Row, Timestamp, Diff)> {
        let got: Vec<(Row, Timestamp, Diff)> = rx
            .extract()
            .into_iter()
            .flat_map(|(_, data)| data)
            .collect();
        let mut consolidated: BTreeMap<(Row, Timestamp), Diff> = BTreeMap::new();
        for (row, time, diff) in got {
            *consolidated.entry((row, time)).or_insert(Diff::ZERO) += diff;
        }
        consolidated
            .into_iter()
            .filter(|(_, d)| !d.is_zero())
            .map(|((row, t), d)| (row, t, d))
            .collect()
    }

    /// Exercises [`ArrangementFlavor::SharedTrace`], the render variant that carries a
    /// maintenance-published index imported into the interactive runtime *as an arrangement*.
    ///
    /// Two `RowRow` indexes are published, imported through `SharedTraceHandle::import_snapshot_at`
    /// as a static `as_of` snapshot, entered into a region, and wrapped in
    /// `ArrangementFlavor::SharedTrace`, exactly as `import_index_shared` does with its
    /// `.enter(self.scope)`. Because the import is a snapshot at `as_of`, every update is coalesced
    /// to `as_of`, so key 1's insert and retraction cancel. The flavor is then consumed two ways,
    /// standing in for the two downstream operator families that matter:
    ///
    /// * REDUCE input surface: `ArrangementFlavor::as_collection` reconstructs rows through the
    ///   render's generic arrangement body, the same surface `as_specific_collection` feeds a
    ///   reduce. The reconstructed `(key, value)` rows must equal the published rows coalesced at
    ///   `as_of`.
    /// * JOIN surface: the two flavors' arrangements are joined with `join_core`, the differential
    ///   surface the linear join's `DifferentialDataflow` path calls. The output must equal the
    ///   direct join.
    ///
    /// Both consume the imported shared arrangement AS an arrangement, never re-deriving it from a
    /// collection. That is the property the `SharedTrace` variant exists to preserve, and the
    /// property the prior `CollectionBundle::from_collections` degradation broke.
    #[mz_ore::test]
    fn shared_trace_flavor_feeds_join_and_reduce() {
        let id_a = GlobalId::User(1);
        let id_b = GlobalId::User(2);

        // Same inputs as `join_over_imported_arrangements_matches_direct`: key 1 inserted then
        // retracted, plus keys 2 and 3, joined against one value per key.
        let a: Vec<Update> = vec![
            (1, "a", 0, 1),
            (2, "b", 0, 1),
            (3, "c", 1, 1),
            (1, "a", 2, -1),
        ];
        let b: Vec<Update> = vec![(1, "x", 0, 1), (2, "y", 1, 1), (3, "z", 2, 1)];
        let seal = 3;
        // Read as of `seal - 1`, one tick below the sealed upper `{seal}`: `import_snapshot_at`
        // emits only once `upper` is strictly beyond `as_of` (as `snapshot_at` does). All input
        // times (0, 1, 2) are at or below `as_of`, so they coalesce to it and key 1 cancels.
        let as_of_ts = Timestamp::from(seal - 1);

        // The interactive import is a static snapshot at `as_of`, so every update is coalesced to
        // `as_of`: all times advance to `as_of_ts` and cancel there. Key 1's insert and retraction
        // therefore net to zero, so it appears in neither the join nor the reduce output.
        let coalesce_at = |rows: Vec<(Row, Timestamp, Diff)>| -> Vec<(Row, Timestamp, Diff)> {
            let mut out: BTreeMap<Row, Diff> = BTreeMap::new();
            for (row, _time, diff) in rows {
                *out.entry(row).or_insert(Diff::ZERO) += diff;
            }
            let mut v: Vec<_> = out
                .into_iter()
                .filter(|(_, d)| !d.is_zero())
                .map(|(row, d)| (row, as_of_ts, d))
                .collect();
            v.sort();
            v
        };

        let expected_join_rows = coalesce_at(expected_join(&a, &b));

        // Reduce-surface oracle: `a`'s updates coalesced at `as_of` into the two-column
        // `(key, value)` rows that `as_collection` reconstructs.
        let expected_reduce_rows = coalesce_at(
            a.iter()
                .map(|&(k, v, t, d)| {
                    (
                        Row::pack_slice(&[Datum::Int64(k), Datum::String(v)]),
                        Timestamp::from(t),
                        Diff::from(d),
                    )
                })
                .collect(),
        );

        let (join_tx, join_rx) = mpsc::channel();
        let (reduce_tx, reduce_rx) = mpsc::channel();

        timely::execute_directly(move |worker| {
            let registry = ArrangementSharingRegistry::new();

            // Maintenance side: publish both indexes, sealing several batches each.
            let mut keep_a = publish_join_input(&registry, worker, id_a, &a, seal);
            let mut keep_b = publish_join_input(&registry, worker, id_b, &b, seal);

            let worker_index = worker.index();
            let (oks_a, errs_a) = registry.handles(&id_a, worker_index).expect("A published");
            let (oks_b, errs_b) = registry.handles(&id_b, worker_index).expect("B published");

            let join_probe = ProbeHandle::new();
            let reduce_probe = ProbeHandle::new();
            worker.dataflow::<Timestamp, _, _>(|scope| {
                // Import each index as a static snapshot at `as_of`, with no upper suppression, the
                // interactive single-time read path (`import_index_shared`).
                let as_of = Antichain::from_elem(as_of_ts);
                let until = Antichain::new();
                let arr_a = oks_a.import_snapshot_at(
                    scope.clone(),
                    "import A",
                    as_of.clone(),
                    until.clone(),
                );
                let err_a = errs_a.import_snapshot_at(
                    scope.clone(),
                    "import A errs",
                    as_of.clone(),
                    until.clone(),
                );
                let arr_b = oks_b.import_snapshot_at(
                    scope.clone(),
                    "import B",
                    as_of.clone(),
                    until.clone(),
                );
                let err_b = errs_b.import_snapshot_at(scope.clone(), "import B errs", as_of, until);

                scope.region_named("SharedTraceFlavor", |inner| {
                    // Enter the region and wrap as `SharedTrace`, mirroring `import_index_shared`.
                    let flavor_a = ArrangementFlavor::SharedTrace(
                        id_a,
                        arr_a.enter(inner),
                        err_a.enter(inner),
                    );
                    let flavor_b = ArrangementFlavor::SharedTrace(
                        id_b,
                        arr_b.enter(inner),
                        err_b.enter(inner),
                    );

                    // REDUCE surface: reconstruct A's rows through the flavor's generic body.
                    #[allow(deprecated)]
                    let (oks_coll, _errs_coll) = flavor_a.as_collection();
                    oks_coll
                        .inner
                        .probe_with(&reduce_probe)
                        .capture_into(reduce_tx.clone());

                    // JOIN surface: join the two flavors' arrangements. Extracting them by matching
                    // the variant proves the flavor holds real arrangements the join consumes.
                    let (join_a, join_b) = match (&flavor_a, &flavor_b) {
                        (
                            ArrangementFlavor::SharedTrace(_, a, _),
                            ArrangementFlavor::SharedTrace(_, b, _),
                        ) => (a.clone(), b.clone()),
                        _ => unreachable!("both flavors constructed as SharedTrace above"),
                    };
                    let joined = join_a.join_core(join_b, |key, v1, v2| {
                        let row =
                            Row::pack(key.into_iter().chain(v1.into_iter()).chain(v2.into_iter()));
                        Some(row)
                    });
                    joined
                        .inner
                        .probe_with(&join_probe)
                        .capture_into(join_tx.clone());
                });
            });

            // Step until both operators have sealed through the seal frontier, keeping the
            // publisher inputs alive so their publication points stay open.
            let seal_ts = Timestamp::from(seal);
            let mut steps = 0;
            while join_probe.less_than(&seal_ts) || reduce_probe.less_than(&seal_ts) {
                keep_a(worker);
                keep_b(worker);
                worker.step();
                steps += 1;
                assert!(steps < 10_000, "dataflow did not seal through {seal_ts:?}");
            }
        });

        assert_eq!(
            consolidate_capture(join_rx),
            expected_join_rows,
            "join over SharedTrace flavor diverged from the direct join"
        );
        assert_eq!(
            consolidate_capture(reduce_rx),
            expected_reduce_rows,
            "as_collection over SharedTrace flavor diverged from the published rows"
        );
    }

    /// A join and a reduce over an arrangement imported at a stale `as_of`, where the publisher's
    /// spine has folded the history below it into fewer, larger batches.
    ///
    /// This is the regime production reads in and no other test reaches. The other join and reduce
    /// tests publish four updates and read at `as_of = 0`, so their chains are one batch per time and
    /// no merge ever precedes the read time. Here sixteen times are published and the controller then
    /// allows compaction to the read time, which is what raises the published `since` and lets the
    /// spine fold the batches below it together.
    ///
    /// The test asserts both halves of the shape rather than assuming them. A merge must have
    /// happened, so the import really does seed from a folded chain. And a batch must straddle the
    /// `as_of`, because that is the case being covered: an import does not cut at its `as_of`, it is
    /// seeded with the whole chain and wrapped in `TraceFrontier`, which advances times instead of
    /// cutting. The join and reduce output is the observable, so a straddling batch mishandled would
    /// show up as updates at times not before the cut, double counted.
    #[mz_ore::test]
    fn stale_as_of_import_over_merged_chain_matches_direct() {
        let id_a = GlobalId::User(1);
        let id_b = GlobalId::User(2);

        // Sixteen distinct times, four keys cycling, so every key accumulates several updates and
        // the publisher seals sixteen batches for the spine to merge.
        let times = 16u64;
        let mut a: Vec<Update> = Vec::new();
        let mut b: Vec<Update> = Vec::new();
        for t in 0..times {
            let key = i64::try_from(t % 4).expect("small") + 1;
            a.push((key, "a", t, 1));
            b.push((key, "x", t, 1));
        }
        // Retract key 1's first insert at a time still below `as_of`, so the stale read must
        // coalesce the pair away rather than report both.
        a.push((1, "a", 3, -1));
        let seal = times;
        // Read from the middle of the history, far enough below the seal that the batches around it
        // have been merged over.
        let as_of_ts = Timestamp::from(times / 2);

        // The import advances times at or below `as_of` up to it and leaves later times alone, so
        // the oracle is the direct computation over the same advanced updates.
        let advance = |updates: &[Update]| -> Vec<Update> {
            updates
                .iter()
                .map(|&(k, v, t, d)| (k, v, t.max(u64::from(as_of_ts)), d))
                .collect()
        };
        let a_advanced = advance(&a);
        let b_advanced = advance(&b);

        let expected_join_rows = expected_join(&a_advanced, &b_advanced);

        // Reduce-surface oracle: A's advanced updates as the two-column `(key, value)` rows that
        // `as_collection` reconstructs, consolidated per `(row, time)`.
        let expected_reduce_rows = {
            let mut out: BTreeMap<(Row, Timestamp), Diff> = BTreeMap::new();
            for &(k, v, t, d) in &a_advanced {
                let row = Row::pack_slice(&[Datum::Int64(k), Datum::String(v)]);
                *out.entry((row, Timestamp::from(t))).or_insert(Diff::ZERO) += Diff::from(d);
            }
            let mut v: Vec<_> = out
                .into_iter()
                .filter(|(_, d)| !d.is_zero())
                .map(|((row, t), d)| (row, t, d))
                .collect();
            v.sort();
            v
        };

        let (join_tx, join_rx) = mpsc::channel();
        let (reduce_tx, reduce_rx) = mpsc::channel();

        timely::execute_directly(move |worker| {
            let registry = ArrangementSharingRegistry::new();

            // Publish both indexes to completion BEFORE any importer registers.
            let mut keep_a = publish_join_input(&registry, worker, id_a, &a, seal);
            let mut keep_b = publish_join_input(&registry, worker, id_b, &b, seal);
            for _ in 0..64 {
                keep_a(worker);
                keep_b(worker);
            }

            // The controller allows compaction up to the read time, exactly as
            // `handle_allow_compaction` does in production. That raises the published `since`, so the
            // spine may coalesce the history below the read time. No importer has registered yet, so
            // the publisher's physical target is the chain coverage and the spine is free to fold
            // those batches together. The extra ticks give it activations to do so.
            let allow = Antichain::from_elem(as_of_ts);
            registry.note_allow_compaction(id_a, 0, &allow);
            registry.note_allow_compaction(id_b, 0, &allow);
            for _ in 0..64 {
                keep_a(worker);
                keep_b(worker);
            }

            let worker_index = worker.index();
            let (oks_a, errs_a) = registry.handles(&id_a, worker_index).expect("A published");
            let (oks_b, errs_b) = registry.handles(&id_b, worker_index).expect("B published");

            // First half of the premise: the spine folded batches, so the import seeds from a merged
            // chain rather than from the one-batch-per-time shape the other tests cover. Each
            // published time seals its own `[t, t+1)` batch, so a batch spanning more than one time
            // can only come from a merge.
            //
            // Second half: a batch *does* straddle `as_of`, which is the case this fixture exists to
            // cover. An import does not cut at `as_of`, it is seeded with the whole chain and wrapped
            // in `TraceFrontier`, which advances times instead. So a straddling batch is harmless and
            // the observable is the join and reduce output below, which must still match the direct
            // computation. Asserting the straddle rather than its absence keeps this test as the
            // detector for a publisher that holds physical compaction down collectively again.
            let mut merged = false;
            let mut straddles_as_of = false;
            oks_a.map_batches(|batch| {
                let lower = batch.lower().elements().first().copied();
                let upper = batch.upper().elements().first().copied();
                if let (Some(lower), Some(upper)) = (lower, upper) {
                    if upper.saturating_sub(lower) > Timestamp::from(1_u64) {
                        merged = true;
                    }
                    if lower < as_of_ts && as_of_ts < upper {
                        straddles_as_of = true;
                    }
                }
            });
            assert!(
                merged,
                "no published batch spans more than one time; the spine did not merge and the test \
                 is not exercising the merged-chain cut"
            );
            assert!(
                straddles_as_of,
                "no published batch straddles as_of {as_of_ts:?}, so this fixture is not reaching \
                 the case it exists for: an import whose `as_of` falls inside a batch. If this \
                 fires, the publisher has gone back to holding physical compaction down to a \
                 collective floor such as the published `since`, which stops the spine merging \
                 across `as_of` at all"
            );

            let join_probe = ProbeHandle::new();
            let reduce_probe = ProbeHandle::new();
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let as_of = Antichain::from_elem(as_of_ts);
                let until = Antichain::new();
                let arr_a = oks_a.import_snapshot_at(
                    scope.clone(),
                    "import A",
                    as_of.clone(),
                    until.clone(),
                );
                let err_a = errs_a.import_snapshot_at(
                    scope.clone(),
                    "import A errs",
                    as_of.clone(),
                    until.clone(),
                );
                let arr_b = oks_b.import_snapshot_at(
                    scope.clone(),
                    "import B",
                    as_of.clone(),
                    until.clone(),
                );
                let err_b = errs_b.import_snapshot_at(scope.clone(), "import B errs", as_of, until);

                scope.region_named("SharedTraceFlavor", |inner| {
                    let flavor_a = ArrangementFlavor::SharedTrace(
                        id_a,
                        arr_a.enter(inner),
                        err_a.enter(inner),
                    );
                    let flavor_b = ArrangementFlavor::SharedTrace(
                        id_b,
                        arr_b.enter(inner),
                        err_b.enter(inner),
                    );

                    #[allow(deprecated)]
                    let (oks_coll, _errs_coll) = flavor_a.as_collection();
                    oks_coll
                        .inner
                        .probe_with(&reduce_probe)
                        .capture_into(reduce_tx.clone());

                    let (join_a, join_b) = match (&flavor_a, &flavor_b) {
                        (
                            ArrangementFlavor::SharedTrace(_, a, _),
                            ArrangementFlavor::SharedTrace(_, b, _),
                        ) => (a.clone(), b.clone()),
                        _ => unreachable!("both flavors constructed as SharedTrace above"),
                    };
                    let joined = join_a.join_core(join_b, |key, v1, v2| {
                        let row =
                            Row::pack(key.into_iter().chain(v1.into_iter()).chain(v2.into_iter()));
                        Some(row)
                    });
                    joined
                        .inner
                        .probe_with(&join_probe)
                        .capture_into(join_tx.clone());
                });
            });

            let seal_ts = Timestamp::from(seal);
            let mut steps = 0;
            while join_probe.less_than(&seal_ts) || reduce_probe.less_than(&seal_ts) {
                keep_a(worker);
                keep_b(worker);
                worker.step();
                steps += 1;
                assert!(steps < 10_000, "dataflow did not seal through {seal_ts:?}");
            }
        });

        assert_eq!(
            consolidate_capture(join_rx),
            expected_join_rows,
            "join over a merged chain read at a stale as_of diverged from the direct join"
        );
        assert_eq!(
            consolidate_capture(reduce_rx),
            expected_reduce_rows,
            "as_collection over a merged chain read at a stale as_of diverged from the published rows"
        );
    }

    /// SPIKE (decision 4, hardest risk, assumption 1): a bare `SharedOksHandle` held on a reader
    /// thread that runs NO import/replay operator observes the published `upper` advance via
    /// `TraceReader::read_upper` as the publisher (a separate thread's worker) seals successive
    /// times.
    ///
    /// The publisher and reader are on separate threads, handshaking per sealed time so the check is
    /// deterministic rather than timing-dependent: the publisher steps its worker (refreshing the
    /// shared chain under the lock), announces the sealed time, and blocks; the reader then reads
    /// `read_upper` on its bare handle and must see exactly that frontier before acking. The reader
    /// never builds a dataflow, so this proves `read_upper` reflects the publisher-refreshed chain
    /// directly, not a locally-drained copy.
    #[mz_ore::test]
    fn bare_handle_read_upper_advances_cross_thread() {
        use differential_dataflow::trace::TraceReader;

        let id = GlobalId::User(1);
        let registry = ArrangementSharingRegistry::new();
        let publisher_registry = registry.clone();

        // Handshake channels: publisher -> reader announces each sealed time; reader -> publisher
        // acks so the publisher advances only after the reader has observed the current seal.
        let (tick_tx, tick_rx) = mpsc::channel::<u64>();
        let (ack_tx, ack_rx) = mpsc::channel::<()>();

        let seals: Vec<u64> = vec![1, 2, 3, 4, 5];
        let publisher_seals = seals.clone();

        // `execute_directly` requires a `Send + Sync` closure, but `mpsc` endpoints are not `Sync`.
        // A `Mutex` makes them `Sync`; the single publisher worker is the only user.
        let tick_tx = std::sync::Mutex::new(tick_tx);
        let ack_rx = std::sync::Mutex::new(ack_rx);

        let publisher = thread::spawn(move || {
            timely::execute_directly(move |worker| {
                let worker_index = worker.index();
                let peers = worker.peers();
                let (mut oks_input, mut errs_input) = worker.dataflow::<Timestamp, _, _>(|scope| {
                    let (oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
                    let oks = oks_collection.mz_arrange::<
                            ColumnationChunker<_>,
                            RowRowBatcher<_, _>,
                            RowRowBuilder<_, _>,
                            RowRowSpine<_, _>,
                        >("spike oks");

                    let (errs_input, errs_collection) =
                        scope.new_collection::<DataflowErrorSer, Diff>();
                    let errs = KeyCollection::from(errs_collection).mz_arrange::<
                            ColumnationChunker<_>,
                            ErrBatcher<_, _>,
                            ErrBuilder<_, _>,
                            ErrSpine<_, _>,
                        >("spike errs");

                    let slot =
                        publisher_registry.get_or_create_placeholder(id, worker_index, peers);
                    PublishArrangement::adopt(&oks, &slot.oks, || {});
                    PublishArrangement::adopt(&errs, &slot.errs, || {});
                    publisher_registry.notify(id, worker_index);
                    (oks_input, errs_input)
                });

                for &t in &publisher_seals {
                    // Add a row just below the seal time, then advance the frontier to `t` and step
                    // so the arrange operator seals the batch and the publisher refreshes the shared
                    // chain to `upper = {t}`.
                    oks_input.update(
                        (
                            Row::pack_slice(&[Datum::Int64(i64::from(u32::try_from(t).unwrap()))]),
                            Row::pack_slice(&[Datum::String("v")]),
                        ),
                        Diff::ONE,
                    );
                    oks_input.advance_to(Timestamp::from(t));
                    oks_input.flush();
                    errs_input.advance_to(Timestamp::from(t));
                    errs_input.flush();
                    for _ in 0..32 {
                        worker.step();
                    }
                    tick_tx
                        .lock()
                        .unwrap()
                        .send(t)
                        .expect("reader waits for ticks");
                    ack_rx
                        .lock()
                        .unwrap()
                        .recv()
                        .expect("reader acks each tick");
                }
            });
        });

        // Reader thread (the current thread): acquire a BARE handle and drive only `read_upper`.
        let (mut oks, _errs) = {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if let Some(handles) = registry.handles(&id, 0) {
                    break handles;
                }
                assert!(Instant::now() < deadline, "publisher never published id");
                thread::sleep(Duration::from_millis(5));
            }
        };

        let mut observed: Vec<u64> = Vec::new();
        for _ in &seals {
            let t = tick_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("publisher announces each sealed time");
            // Read the bare handle's upper. The publisher already refreshed the shared chain under
            // the lock during its `step` before announcing `t`, so a single read suffices; a short
            // retry only guards against scheduler slack, never masks a missing advance.
            let mut upper = Antichain::new();
            let expected = Timestamp::from(t);
            let read_deadline = Instant::now() + Duration::from_secs(2);
            loop {
                oks.read_upper(&mut upper);
                if upper.elements().first() == Some(&expected) {
                    break;
                }
                assert!(
                    Instant::now() < read_deadline,
                    "read_upper never reached {expected:?}; observed {:?}",
                    upper.elements()
                );
                thread::sleep(Duration::from_millis(2));
            }
            observed.push(t);
            ack_tx.send(()).expect("publisher waits for ack");
        }

        publisher.join().expect("publisher thread panicked");

        // The reader saw every seal advance, in order, with no operator of its own: proof that a
        // bare handle's `read_upper` tracks the publisher-driven chain.
        assert_eq!(observed, seals);
    }

    /// SPIKE (decision 4, hardest risk, assumption 2): a `SyncActivator` minted by one worker and
    /// fired from a DIFFERENT thread unparks that worker and schedules the targeted operator.
    ///
    /// This is the primitive the seal-notification path relies on: maintenance holds an interactive
    /// worker's `SyncActivator` (via the registry) and fires it on frontier advance. The DD sharing
    /// primitive already uses exactly this to wake importers from the publisher (`ImportQueue`); the
    /// test isolates the cross-thread fire so its unpark behavior is explicit.
    ///
    /// The worker builds a source operator, exports its `SyncActivator`, and drops its capability so
    /// it goes quiescent and the worker parks. The main thread fires the activator and observes the
    /// operator's run counter increment, proving the fire (not a timeout) rescheduled it.
    #[mz_ore::test]
    fn sync_activator_fires_cross_thread() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        use timely::container::CapacityContainerBuilder;
        use timely::dataflow::operators::generic::source;
        use timely::scheduling::activate::SyncActivator;

        let runs = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(AtomicBool::new(false));
        let runs_worker = Arc::clone(&runs);
        let done_worker = Arc::clone(&done);

        let (act_tx, act_rx) = mpsc::channel::<SyncActivator>();
        // `execute_directly` needs a `Send + Sync` closure; `Sender` is not `Sync`.
        let act_tx = std::sync::Mutex::new(act_tx);

        let worker = thread::spawn(move || {
            timely::execute_directly(move |worker| {
                worker.dataflow::<Timestamp, _, _>(|scope| {
                    let scope_handle = scope.clone();
                    let runs = Arc::clone(&runs_worker);
                    let done = Arc::clone(&done_worker);
                    let act_tx = act_tx.lock().unwrap().clone();
                    let _stream = source::<_, CapacityContainerBuilder<Vec<()>>, _, _>(
                        scope,
                        "spike-activator-source",
                        move |cap, info| {
                            // Mint this operator's cross-thread activator and hand it to the main thread.
                            let activator = scope_handle
                                .worker()
                                .sync_activator_for(info.address.to_vec());
                            act_tx
                                .send(activator)
                                .expect("main thread receives activator");
                            // Keep the capability alive (as the DD import source does) so the operator
                            // stays registered and reschedulable. Holding a capability does not force
                            // rescheduling, so after its initial run the operator parks and is woken only
                            // by an explicit activation. Release it once `done` is set so the dataflow can
                            // drain and the worker shut down cleanly. The `take` is a side effect
                            // (dropping the capability), not a read.
                            #[allow(clippy::collection_is_never_read)]
                            let mut cap = Some(cap);
                            move |_output| {
                                if done.load(Ordering::SeqCst) {
                                    cap.take();
                                }
                                runs.fetch_add(1, Ordering::SeqCst);
                            }
                        },
                    );
                });

                // Park until fired. A finite park timeout bounds the test if the fire is ever lost,
                // and lets the loop observe the `done` flag; the assertion below proves the wake came
                // from the fire, since a quiescent operator is not rescheduled by a timeout alone.
                while !done_worker.load(Ordering::SeqCst) {
                    worker.step_or_park(Some(Duration::from_millis(100)));
                }
            });
        });

        let activator = act_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("worker exported its activator");

        // Let the worker settle into a park with the operator quiescent.
        thread::sleep(Duration::from_millis(200));
        let baseline = runs.load(Ordering::SeqCst);

        activator
            .activate()
            .expect("cross-thread activation delivered");

        // The fired activation must reschedule the operator, incrementing its run counter.
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if runs.load(Ordering::SeqCst) > baseline {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "operator was not rescheduled after a cross-thread SyncActivator fire"
            );
            thread::sleep(Duration::from_millis(2));
        }

        done.store(true, Ordering::SeqCst);
        // Nudge the worker so it leaves `step_or_park` promptly and observes `done`.
        let _ = activator.activate();
        worker.join().expect("worker thread panicked");
    }
}
