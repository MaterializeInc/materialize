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
//! a reader on another thread or runtime can mint a `Send`
//! [`SharedTraceHandle`](crate::shared_trace::SharedTraceHandle) for the same
//! arrangement.
//!
//! The registry is per PROCESS and shared across all timely workers of the runtime. Each worker has
//! its own `ComputeState`, but they all share one registry `Arc`, the way the persist client cache
//! is shared. Worker `i` publishes into slot `i`; a reader on worker `i` of another runtime looks up
//! slot `i`, which is sound only because both sides shard keys by the same `key.hashed() % peers`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use mz_repr::{Diff, GlobalId, Timestamp};
use timely::progress::Antichain;
use timely::scheduling::activate::SyncActivator;

use crate::shared_trace::{Published, SharedErrsHandle, SharedOksHandle};
use crate::typedefs::{ErrSpine, RowRowSpine};

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
    /// reader hold from a frontier that happens to sit there, and that distinction is what says
    /// whether an import is still protected.
    #[cfg(test)]
    pub fn published_logical_holds(
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
    /// `note_writer_logical` simply records the frontier a later `adopt` will publish against).
    /// Does not `notify`: compaction bookkeeping alone gives a waiting reader nothing new to serve.
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
mod tests;
