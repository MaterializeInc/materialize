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
    /// [`Self::notify`] wakes an id together with everything that re-exports it, transitively.
    /// Resolved outside the `wakers` lock, so it does not enter the lost-wakeup argument.
    aliases: Mutex<BTreeMap<GlobalId, BTreeSet<GlobalId>>>,
    /// Indexed by worker ordinal; `None` until that interactive worker registers its waker.
    wakers: Mutex<Vec<Option<Waker>>>,
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

    /// Publishes worker `worker_index`'s (of `peers` total) arrangement for `id`.
    ///
    /// Overwrites any prior arrangement in the slot.
    pub fn insert(
        &self,
        id: GlobalId,
        worker_index: usize,
        peers: usize,
        arr: SharedIndexArrangement,
    ) {
        self.insert_shared(id, worker_index, peers, Arc::new(arr));
    }

    /// Registers `arr` in slot `worker_index` of `id`, growing the slot vector to `peers` if `id`
    /// is not yet present.
    fn insert_shared(
        &self,
        id: GlobalId,
        worker_index: usize,
        peers: usize,
        arr: Arc<SharedIndexArrangement>,
    ) {
        {
            let mut map = self.inner.map.lock().expect("registry poisoned");
            let slots = map
                .entry(id)
                .or_insert_with(|| (0..peers).map(|_| None).collect());
            slots[worker_index] = Some(arr);
        }
        // Mark the slot dirty for its worker and fire its waker. The map lock is released first; the
        // lost-wakeup argument for this ordering is on `notify`.
        self.notify(id, worker_index);
    }

    /// Returns the existing slot for `(id, worker_index)`, or creates one backed by unbacked
    /// `Published` placeholders ([`Published::placeholder`]) and returns that instead.
    ///
    /// Whichever side touches `(id, worker_index)` first creates the slot; the other observes and
    /// shares the same `Arc`, so a placeholder a reader already imported is filled in place by a
    /// later [`PublishArrangement::adopt`] rather than being overwritten by a second, disconnected
    /// arrangement. Grows the slot vector to `peers` like [`Self::insert_shared`] when `id` is not
    /// yet present.
    ///
    /// Creating a placeholder carries no data, so this does not `notify`: there is nothing yet for
    /// a waiting reader to act on. The caller that later fills the placeholder via `adopt` is
    /// responsible for calling `notify` once the fill is installed, the way `insert` does for a
    /// freshly published slot.
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
        // See `insert_shared`: mark dirty and fire the worker's waker after the slot write.
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

    /// Evicts a never-adopted placeholder slot for `(id, worker_index)` once its last reader leaves.
    ///
    /// A placeholder created by [`Self::get_or_create_placeholder`] whose index creation is cancelled
    /// is never adopted, so no publisher drop cleans it up. This removes the slot when it is both
    /// unadopted and unreferenced, so the registry does not leak it. An ADOPTED slot is left alone: it
    /// is cleaned up by the maintenance publisher's drop through [`Self::remove`].
    ///
    /// This is registry HYGIENE, not a correctness mechanism, and has no production caller today. The
    /// leaked slot it reclaims is a bounded, empty placeholder. Correctness of when an imported index
    /// may compact or drop rests entirely on the controller's read-hold discipline (see
    /// `mz_compute_client::controller::instance::Instance::finish_peek`), not on eviction: the
    /// controller holds a read hold on the index for every reader and frees it only after the reader
    /// retires. Retained for a future reader-teardown hygiene path and exercised by tests.
    ///
    /// Call this from a reader's teardown after it has dropped its `Arc<SharedIndexArrangement>`. It is
    /// a no-op unless the map is the sole owner of the slot Arc, so calling it while another reader (or
    /// an in-flight adopter) still holds a clone does nothing.
    ///
    /// # Reader-liveness contract
    ///
    /// The strong count of the slot Arc counts the map plus every holder of a clone returned by
    /// [`Self::get_or_create_placeholder`]. A [`SharedTraceHandle`] holds an `Arc<SharedTrace>` one
    /// level down and does NOT contribute, so a reader MUST keep its `Arc<SharedIndexArrangement>`
    /// alive for as long as it imports. Then strong count 1 (the map alone) means no live reader.
    /// The reader import in `crate::render::import_shared_index` keeps its slot Arc alive in the
    /// dataflow's token set for exactly this reason.
    ///
    /// # Why this cannot race adoption
    ///
    /// The whole check-and-remove runs under the map lock, and both the adoption flag and the slot's
    /// strong count are read inside one critical section of the oks point's state mutex (via
    /// [`Published::adopted_and`]). An adopter clones the slot Arc under the map lock and holds that
    /// clone across its `adopt` call, which sets the flag under the same state mutex. So the state-lock
    /// read here either follows the adopt (sees adopted, spares the slot) or fully precedes it, in
    /// which case the adopter's clone has not yet dropped and the strong count read in the same section
    /// is at least 2 (spares the slot). Removal therefore happens only for a slot no reader and no
    /// in-flight adopter holds. A later adopter, blocked on the map lock, then creates a fresh slot.
    pub fn evict_unadopted(&self, id: &GlobalId, worker_index: usize) {
        let mut map = self.inner.map.lock().expect("registry poisoned");
        let Some(slots) = map.get_mut(id) else {
            return;
        };
        let should_evict = match slots.get(worker_index).and_then(|slot| slot.as_ref()) {
            Some(slot) => {
                // NOTE: this consults only the `oks` adoption flag, not `errs`. That is sound
                // only because every publish site (both maintenance sites in render.rs and
                // `publish_logging_index`) adopts `oks` before `errs`, so there is no window
                // where `errs` is adopted but `oks` is not.
                let (adopted, strong_count) = slot.oks.adopted_and(|| Arc::strong_count(slot));
                !adopted && strong_count == 1
            }
            None => false,
        };
        if !should_evict {
            return;
        }
        slots[worker_index] = None;
        // Drop the id entry entirely once no worker slot remains, so an evicted placeholder leaves no
        // empty vector behind.
        if slots.iter().all(Option::is_none) {
            map.remove(id);
        }
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
    /// trace's `upper` advances, so a fast-path peek waiting on the seal is re-examined. Same body
    /// as the notification half of `insert`.
    pub fn note_frontier(&self, id: GlobalId, worker_index: usize) {
        self.notify(id, worker_index);
    }

    /// Marks `id` dirty for worker `worker_index` and fires its coalescing waker.
    ///
    /// Visible at `pub(crate)` so the maintenance publish path in `render.rs` can call it directly
    /// once it has adopted a placeholder's publication points: `get_or_create_placeholder` itself
    /// does not notify (see its doc comment), so the caller that fills the slot must.
    ///
    /// # Lost-wakeup contract
    ///
    /// `map` and `wakers` are separate locks. `insert` and `reexport` write the slot under `map`,
    /// release it, then call this under `wakers`. `get_or_create_placeholder` writes the slot the
    /// same way, under `map` then released, but its caller calls this separately once it has
    /// adopted the slot rather than immediately after the write. Either way, the slot write always
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
    use differential_dataflow::trace::Cursor;
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
                let published_oks = PublishArrangement::publish(&oks);

                let (mut errs_input, errs_collection) =
                    scope.new_collection::<DataflowErrorSer, Diff>();
                let errs = KeyCollection::from(errs_collection).mz_arrange::<
                    ColumnationChunker<_>,
                    ErrBatcher<_, _>,
                    ErrBuilder<_, _>,
                    ErrSpine<_, _>,
                >("test errs");
                let published_errs = PublishArrangement::publish(&errs);

                registry_in.insert(
                    id,
                    0,
                    1,
                    SharedIndexArrangement {
                        oks: published_oks,
                        errs: published_errs,
                    },
                );

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

    /// Like `publish_index_into`, but instead of publishing fresh arrangements and `insert`ing
    /// them, routes through `get_or_create_placeholder` and `PublishArrangement::adopt`, the path
    /// the maintenance render side uses. Exercises the get-or-create half of the convergence
    /// contract: whatever slot `get_or_create_placeholder` returns (existing or freshly created) is
    /// the one that gets filled.
    fn publish_index_into_adopting(
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
                PublishArrangement::adopt(&oks, &slot.oks);
                PublishArrangement::adopt(&errs, &slot.errs);
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

        // Publisher adopts the same slot and fills it (reuse publish_index_into, but routed
        // through get_or_create_placeholder + adopt).
        publish_index_into_adopting(&registry, id, test_rows());

        assert_eq!(
            read_rows(&oks, Timestamp::from(0_u64)),
            expected_rows(&test_rows())
        );
    }

    #[mz_ore::test]
    fn placeholder_close_terminates_importers() {
        // An import over a never-adopted placeholder pins its frontier at the minimum. Closing the
        // placeholder pushes a terminal empty frontier, so the importer completes rather than
        // wedging, regardless of the `until` bound (this test uses a single-time read).
        let (capture_tx, capture_rx) = mpsc::channel();

        timely::execute_directly(move |worker| {
            let peers = worker.peers();
            let placeholder: Published<RowRowSpine<Timestamp, Diff>> =
                Published::placeholder(peers);
            let oks = placeholder.handle();

            let probe = ProbeHandle::new();
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let as_of = Antichain::from_elem(Timestamp::from(0_u64));
                let until = Antichain::from_elem(Timestamp::from(1_u64));
                let arr = oks.import_snapshot_at(scope.clone(), "import placeholder", as_of, until);
                arr.as_collection(|_k, _v| ())
                    .inner
                    .probe_with(&probe)
                    .capture_into(capture_tx.clone());
            });

            // Before close: the import holds its frontier at the minimum [0] and emits nothing.
            for _ in 0..32 {
                worker.step();
            }
            assert!(
                probe.less_equal(&Timestamp::from(0_u64)),
                "placeholder import frontier should be pinned at 0 before close"
            );
            assert!(
                !probe.done(),
                "placeholder import frontier should be non-empty before close"
            );

            // Close the never-adopted placeholder. The importer drains the terminal empty frontier,
            // drops its capability, and completes.
            placeholder.close();
            let mut steps = 0;
            while !probe.done() {
                worker.step();
                steps += 1;
                assert!(
                    steps < 10_000,
                    "importer did not terminate after placeholder close"
                );
            }
        });

        // The placeholder never carried a batch, so the importer produced no data.
        let data: Vec<((), Timestamp, Diff)> = capture_rx
            .extract()
            .into_iter()
            .flat_map(|(_, d)| d)
            .collect();
        assert!(data.is_empty(), "a closed placeholder must produce no data");
    }

    #[mz_ore::test]
    fn last_reader_evicts_unadopted_placeholder() {
        let id = GlobalId::User(1);
        let registry = ArrangementSharingRegistry::new();
        let slot = registry.get_or_create_placeholder(id, 0, 1);
        assert!(registry.handles(&id, 0).is_some());
        // Dropping the sole reader leaves the map as the only owner of the slot Arc.
        drop(slot);
        registry.evict_unadopted(&id, 0);
        assert!(registry.handles(&id, 0).is_none());
    }

    #[mz_ore::test]
    fn evict_spares_placeholder_with_live_reader() {
        let id = GlobalId::User(1);
        let registry = ArrangementSharingRegistry::new();
        let reader1 = registry.get_or_create_placeholder(id, 0, 1);
        let reader2 = registry.get_or_create_placeholder(id, 0, 1);
        // One of two readers leaving must not evict: the other still holds a clone of the slot Arc.
        drop(reader1);
        registry.evict_unadopted(&id, 0);
        assert!(
            registry.handles(&id, 0).is_some(),
            "a placeholder with a live reader must not be evicted"
        );
        // The last reader leaving does evict.
        drop(reader2);
        registry.evict_unadopted(&id, 0);
        assert!(registry.handles(&id, 0).is_none());
    }

    #[mz_ore::test]
    fn evict_spares_adopted_slot() {
        let id = GlobalId::User(1);
        let registry = ArrangementSharingRegistry::new();
        // A reader creates the placeholder, then leaves. Adoption fills the same slot via the
        // maintenance path, which drops its own slot clone when its build closure returns, so the map
        // becomes the sole owner of the slot Arc, exactly the strong-count-1 shape eviction keys on.
        let slot = registry.get_or_create_placeholder(id, 0, 1);
        drop(slot);
        publish_index_into_adopting(&registry, id, test_rows());
        // The adopted flag, not the strong count, is what spares it: an adopted slot is fed by its
        // publisher and cleaned up by `remove`, never by eviction.
        registry.evict_unadopted(&id, 0);
        assert!(
            registry.handles(&id, 0).is_some(),
            "an adopted slot must not be evicted"
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
            let published_oks = PublishArrangement::publish(&oks);

            let (errs_input, errs_collection) = scope.new_collection::<DataflowErrorSer, Diff>();
            let errs = KeyCollection::from(errs_collection).mz_arrange::<
                ColumnationChunker<_>,
                ErrBatcher<_, _>,
                ErrBuilder<_, _>,
                ErrSpine<_, _>,
            >("input errs");
            let published_errs = PublishArrangement::publish(&errs);

            registry_in.insert(
                id,
                worker_index,
                peers,
                SharedIndexArrangement {
                    oks: published_oks,
                    errs: published_errs,
                },
            );
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
        move |worker: &mut timely::worker::Worker| {
            let _keep = (&oks_input, &errs_input);
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
            PublishArrangement::adopt(&oks, placeholder);
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
                    let published_oks = PublishArrangement::publish(&oks);

                    let (errs_input, errs_collection) =
                        scope.new_collection::<DataflowErrorSer, Diff>();
                    let errs = KeyCollection::from(errs_collection).mz_arrange::<
                            ColumnationChunker<_>,
                            ErrBatcher<_, _>,
                            ErrBuilder<_, _>,
                            ErrSpine<_, _>,
                        >("spike errs");
                    let published_errs = PublishArrangement::publish(&errs);

                    publisher_registry.insert(
                        id,
                        worker_index,
                        peers,
                        SharedIndexArrangement {
                            oks: published_oks,
                            errs: published_errs,
                        },
                    );
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
