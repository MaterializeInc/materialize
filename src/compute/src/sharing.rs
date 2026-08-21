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
//! `crate::render` publishes a maintained index through `crate::shared_trace` it records the
//! resulting `Published` points here, keyed by [`GlobalId`] and worker ordinal, so a reader on
//! another thread or runtime can mint a `Send` `SharedTraceHandle` for the same arrangement.
//!
//! The registry is per PROCESS and shared across all timely workers of the runtime. Each worker has
//! its own `ComputeState`, but they all share one registry `Arc`, the way the persist client cache
//! is shared. Worker `i` publishes into slot `i`; a reader on worker `i` of another runtime looks up
//! slot `i`, which is sound only because both sides shard keys by the same `key.hashed() % peers`.

// TODO(CPU-215): drop once `crate::render` and `crate::compute_state` call this registry. Only the
// registry's constructor is reachable yet, so the rest reads as dead.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::thread::Thread;

use differential_dataflow::operators::arrange::Arranged;
use mz_repr::{Diff, GlobalId, Timestamp};
use timely::PartialOrder;
use timely::progress::Antichain;

use crate::shared_trace::{PublishArrangement, Published, SharedErrsHandle, SharedOksHandle};
use crate::typedefs::{ErrAgent, ErrSpine, RowRowAgent, RowRowSpine};

/// The published `oks`/`errs` arrangements of one maintained index on one worker.
///
/// An index's `oks` is always a `RowRowSpine` and its `errs` always an `ErrSpine`. Holding the
/// `Published` values keeps the publication points registered and lets us mint further handles.
pub struct SharedIndexArrangement {
    /// The published `oks` arrangement.
    pub(crate) oks: Published<RowRowSpine<Timestamp, Diff>>,
    /// The published `errs` arrangement.
    pub(crate) errs: Published<ErrSpine<Timestamp, Diff>>,
}

/// A per-interactive-worker wake channel: a handle to the worker's thread plus the set of ids
/// marked dirty since the worker last drained.
///
/// The interactive worker parks in `step_or_park`; a publication, removal, or frontier advance on a
/// dependency it is waiting for must push it back to work. `worker` unparks it, and `dirty` names
/// the ids that changed so the worker re-examines only the affected pending work rather than
/// rescanning everything.
struct Waker {
    /// The interactive worker's thread. Unparked rather than activated: every timely allocator's
    /// `await_events` bottoms out in `std::thread::park`, and a root-path `SyncActivator` would
    /// additionally mark the worker's dataflows schedulable, which is work this wake does not need.
    /// Matches the peek-offload wake path.
    worker: Thread,
    /// Ids marked dirty (published, removed, or frontier-advanced) since the worker's last
    /// `take_dirty`.
    dirty: BTreeSet<GlobalId>,
    /// Coalescing flag: `true` once `worker` has been unparked without the worker having drained
    /// since.
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
    /// Indexed by worker ordinal; `None` until that interactive worker registers its waker.
    wakers: Mutex<Vec<Option<Waker>>>,
    /// Taken after `map` and before `wakers`, never the other way around.
    aliases: Mutex<Aliases>,
}

/// Indexes that re-export another index's arrangement and share its slots, see
/// [`ArrangementSharingRegistry::publish_alias`].
///
/// The frontiers the controller notes for every id are kept per (id, worker) so a shared point can
/// be re-derived from the aliases once the target drops.
#[derive(Default)]
struct Aliases {
    /// Alias to the id whose slots it shares.
    target_of: BTreeMap<GlobalId, GlobalId>,
    /// Target to the aliases sharing its slots.
    aliases_of: BTreeMap<GlobalId, BTreeSet<GlobalId>>,
    /// The frontier last passed to `note_allow_compaction` per (id, worker).
    allowed: BTreeMap<(GlobalId, usize), Antichain<Timestamp>>,
    /// The frontier last passed to `note_standing_hold` per (id, worker).
    holds: BTreeMap<(GlobalId, usize), Antichain<Timestamp>>,
}

impl Aliases {
    /// Records `frontier` for `id` and returns the frontier that should reach the point `id`
    /// publishes through, or `None` if another id governs that point.
    ///
    /// An alias dataflow imports its target, so the controller never advances the target's `since`
    /// past an alias's, and the target's frontier bounds every reader of the shared point while the
    /// target lives. Once the target has dropped, the meet of the remaining aliases' frontiers
    /// does, since the shared trace then compacts to exactly that meet. `live` says whether an id
    /// still has slots.
    fn note(
        table: &mut BTreeMap<(GlobalId, usize), Antichain<Timestamp>>,
        target_of: &BTreeMap<GlobalId, GlobalId>,
        aliases_of: &BTreeMap<GlobalId, BTreeSet<GlobalId>>,
        id: GlobalId,
        worker_index: usize,
        frontier: &Antichain<Timestamp>,
        live: impl Fn(&GlobalId) -> bool,
    ) -> Option<Antichain<Timestamp>> {
        table.insert((id, worker_index), frontier.clone());
        let target = *target_of.get(&id).unwrap_or(&id);
        if live(&target) {
            return (id == target).then(|| frontier.clone());
        }
        Self::meet_over(table, aliases_of.get(&target)?, worker_index)
    }

    /// The meet of the frontiers noted for `ids` on `worker_index`, ignoring ids without one.
    fn meet_over(
        table: &BTreeMap<(GlobalId, usize), Antichain<Timestamp>>,
        ids: &BTreeSet<GlobalId>,
        worker_index: usize,
    ) -> Option<Antichain<Timestamp>> {
        ids.iter()
            .filter_map(|id| table.get(&(*id, worker_index)))
            .fold(None, |meet: Option<Antichain<Timestamp>>, frontier| {
                Some(match meet {
                    Some(meet) if PartialOrder::less_equal(&meet, frontier) => meet,
                    _ => frontier.clone(),
                })
            })
    }
}

/// Per-process registry of published index arrangements.
///
/// One slot per (`GlobalId`, worker ordinal). Cloning shares the same underlying map, so a clone
/// handed to each worker's `ComputeState` writes into the same registry.
///
/// A slot is an `Arc` so a reader can retain it for the life of its import while the map entry
/// comes and goes.
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
    /// [`Published`] points and returns that instead.
    ///
    /// Whichever side touches `(id, worker_index)` first creates the slot; the other observes and
    /// shares the same `Arc`, so a point a reader already imported is backed in place by a later
    /// [`crate::shared_trace::PublishArrangement::adopt`] rather than being overwritten by a second,
    /// disconnected arrangement. Grows the slot vector to `peers` when `id` is not yet present.
    ///
    /// An unbacked point carries no data, so this does not `notify`: there is nothing yet for a
    /// waiting reader to act on. [`Self::publish`] notifies once the publishers are installed.
    pub(crate) fn get_or_create(
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
                oks: Published::new(peers),
                errs: Published::new(peers),
            })
        }))
    }

    /// Publishes index `id`'s `oks` and `errs` arrangements on their worker and wakes readers
    /// waiting on `id`.
    ///
    /// Adopts the slot for `id` rather than inserting a fresh one, so a placeholder a reader has
    /// already imported is backed in place. Each half signals its own seal: a peek whose result is
    /// an error carries its data on the errs arrangement, whose frontier is held back until the
    /// error is emitted, so an oks-only signal would leave that peek parked.
    ///
    pub(crate) fn publish<'scope>(
        &self,
        id: GlobalId,
        oks: &Arranged<'scope, RowRowAgent<Timestamp, Diff>>,
        errs: &Arranged<'scope, ErrAgent<Timestamp, Diff>>,
    ) {
        let scope = oks.stream.scope();
        let worker_index = scope.index();
        let slot = self.get_or_create(id, worker_index, scope.peers());
        let registry = self.clone();
        oks.adopt(&slot.oks, &format!("{id} oks"), move || {
            registry.notify(id, worker_index)
        });
        let registry = self.clone();
        errs.adopt(&slot.errs, &format!("{id} errs"), move || {
            registry.notify(id, worker_index)
        });
        self.notify(id, worker_index);
    }

    /// Registers `alias` as a second name for `target`'s slot on `worker_index`, for an index that
    /// re-exports `target`'s arrangement. Readers of either id then share one publication point, and
    /// the re-export's dataflow needs no operators of its own.
    ///
    /// Returns `false` without registering when `alias` already has a slot on this worker, which a
    /// reader created before the publisher rendered. That is the point the reader imported, and only
    /// a publisher writing into it can back it, so the caller publishes through an import instead.
    ///
    /// While `target` lives its frontiers govern the shared point, see [`Aliases::note`]. An alias
    /// outlives its target's removal: the slot stays reachable under the alias and the publisher
    /// keeps running, because the alias's `TraceBundle` holds the dataflow's tokens.
    pub(crate) fn publish_alias(
        &self,
        alias: GlobalId,
        target: GlobalId,
        worker_index: usize,
        peers: usize,
    ) -> bool {
        {
            let mut map = self.inner.map.lock().expect("registry poisoned");
            let Some(shared) = map
                .get(&target)
                .and_then(|slots| slots.get(worker_index))
                .and_then(|slot| slot.clone())
            else {
                return false;
            };
            let slots = map
                .entry(alias)
                .or_insert_with(|| (0..peers).map(|_| None).collect());
            if slots[worker_index].is_some() {
                return false;
            }
            slots[worker_index] = Some(shared);
            let mut aliases = self.inner.aliases.lock().expect("registry poisoned");
            aliases.target_of.insert(alias, target);
            aliases.aliases_of.entry(target).or_default().insert(alias);
        }
        self.notify(alias, worker_index);
        true
    }

    /// Removes all slots for `id`, called when the index drops.
    ///
    /// Dropping a target that still has aliases hands its shared points over to them: the points
    /// stay reachable under the alias ids, and their frontiers move to the meet of what the aliases
    /// have noted, since the shared trace compacts to exactly that from now on.
    pub(crate) fn remove(&self, id: &GlobalId) {
        {
            let mut map = self.inner.map.lock().expect("registry poisoned");
            let mut aliases = self.inner.aliases.lock().expect("registry poisoned");
            map.remove(id);
            aliases.allowed.retain(|(other, _), _| other != id);
            aliases.holds.retain(|(other, _), _| other != id);
            if let Some(target) = aliases.target_of.remove(id) {
                if let Some(set) = aliases.aliases_of.get_mut(&target) {
                    set.remove(id);
                    if set.is_empty() {
                        aliases.aliases_of.remove(&target);
                    }
                }
            }
            if let Some(remaining) = aliases.aliases_of.get(id) {
                // Any alias's slots are the shared ones; the first with a slot on a worker will do.
                for worker_index in 0..remaining
                    .iter()
                    .filter_map(|alias| map.get(alias).map(Vec::len))
                    .max()
                    .unwrap_or(0)
                {
                    let Some(slot) = remaining.iter().find_map(|alias| {
                        map.get(alias)
                            .and_then(|slots| slots.get(worker_index))
                            .and_then(|slot| slot.as_ref())
                    }) else {
                        continue;
                    };
                    if let Some(f) = Aliases::meet_over(&aliases.allowed, remaining, worker_index) {
                        slot.oks.note_writer_logical(&f);
                        slot.errs.note_writer_logical(&f);
                    }
                    if let Some(f) = Aliases::meet_over(&aliases.holds, remaining, worker_index) {
                        slot.oks.note_standing_hold(&f);
                        slot.errs.note_standing_hold(&f);
                    }
                }
            }
        }
        // `remove` is not worker-specific: any interactive worker may have pending work on `id`, so
        // mark it dirty for every registered waker. A waiter re-checks and, finding the slot gone,
        // drops or keeps its item.
        self.notify_all(*id);
    }

    /// Mints reader handles for `id` on `worker_index`, if published.
    pub(crate) fn handles(
        &self,
        id: &GlobalId,
        worker_index: usize,
    ) -> Option<(SharedOksHandle, SharedErrsHandle)> {
        let map = self.inner.map.lock().expect("registry poisoned");
        Self::mint(&map, id, worker_index)
    }

    /// The accumulated `oks` logical holds registered against `id` on `worker_index`, if published.
    ///
    /// Test-only. Minting a handle to observe the published frontiers cannot distinguish a live
    /// reader hold from a frontier that happens to sit there, and that distinction is what says
    /// whether an import is still protected. Empty when every hold has released.
    #[cfg(test)]
    pub(crate) fn published_logical_holds(
        &self,
        id: &GlobalId,
        worker_index: usize,
    ) -> Option<Antichain<Timestamp>> {
        let map = self.inner.map.lock().expect("registry poisoned");
        let slot = map.get(id)?.get(worker_index)?.as_ref()?;
        Some(slot.oks.logical_holds())
    }

    /// The published `oks` point's diagnostics for `id` on `worker_index`, if published. Test-only.
    #[cfg(test)]
    pub(crate) fn published_diagnostics(
        &self,
        id: &GlobalId,
        worker_index: usize,
    ) -> Option<crate::shared_trace::Diagnostics<Timestamp>> {
        let map = self.inner.map.lock().expect("registry poisoned");
        let slot = map.get(id)?.get(worker_index)?.as_ref()?;
        Some(slot.oks.diagnostics())
    }

    /// Registers `worker` as interactive worker `worker_index`'s waker, growing the waker vector as
    /// needed. Called once per interactive worker at startup, from that worker's own thread.
    ///
    /// Overwrites any prior waker for that index, starting with an empty dirty set and a cleared
    /// coalescing flag.
    pub(crate) fn register_waker(&self, worker_index: usize, worker: Thread) {
        let mut wakers = self.inner.wakers.lock().expect("registry poisoned");
        if worker_index >= wakers.len() {
            wakers.resize_with(worker_index + 1, || None);
        }
        wakers[worker_index] = Some(Waker {
            worker,
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
    pub(crate) fn take_dirty(&self, worker_index: usize) -> BTreeSet<GlobalId> {
        let mut wakers = self.inner.wakers.lock().expect("registry poisoned");
        match wakers.get_mut(worker_index).and_then(|w| w.as_mut()) {
            Some(waker) => {
                waker.pending = false;
                std::mem::take(&mut waker.dirty)
            }
            None => BTreeSet::new(),
        }
    }

    /// Forwards the controller's logical compaction `frontier` for `id` into its published slot on
    /// `worker_index`, if one exists.
    ///
    /// Called from `handle_allow_compaction` alongside the local `TraceManager` update, so a
    /// cross-runtime publisher follows the controller's compaction without reading trace internals.
    /// The same frontier drives the index's `oks` and `errs`, matching `TraceManager::allow_compaction`.
    /// A no-op for unshared ids (no slot) and for a slot whose points are still unbacked (an unbacked point's
    /// `note_writer_logical` simply records the frontier a later `adopt` will publish against).
    /// Does not `notify`: compaction bookkeeping alone gives a waiting reader nothing new to serve.
    pub(crate) fn note_allow_compaction(
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
            let mut aliases = self.inner.aliases.lock().expect("registry poisoned");
            let Aliases {
                target_of,
                aliases_of,
                allowed,
                ..
            } = &mut *aliases;
            if let Some(frontier) = Aliases::note(
                allowed,
                target_of,
                aliases_of,
                id,
                worker_index,
                frontier,
                |id| map.contains_key(id),
            ) {
                arr.oks.note_writer_logical(&frontier);
                arr.errs.note_writer_logical(&frontier);
            }
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
    pub(crate) fn note_standing_hold(
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
            let mut aliases = self.inner.aliases.lock().expect("registry poisoned");
            let Aliases {
                target_of,
                aliases_of,
                holds,
                ..
            } = &mut *aliases;
            if let Some(frontier) = Aliases::note(
                holds,
                target_of,
                aliases_of,
                id,
                worker_index,
                frontier,
                |id| map.contains_key(id),
            ) {
                arr.oks.note_standing_hold(&frontier);
                arr.errs.note_standing_hold(&frontier);
            }
        }
    }

    /// Marks `id` dirty for worker `worker_index` and fires its coalescing waker.
    ///
    /// [`Self::publish`] calls this once a slot's publishers are installed, and each publisher calls
    /// it again on every seal, since a fast-path peek waiting on the shared trace's `upper` is
    /// re-examined only when that advance marks `id` dirty.
    ///
    /// # Lost-wakeup contract
    ///
    /// `map` and `wakers` are separate locks. A publisher writes its slot under `map`, releases it,
    /// then calls this under `wakers`. On wake the interactive server loop runs `take_dirty` (under
    /// `wakers`) and only then re-reads the slot via `handles` (under `map`). Label the four steps:
    /// publisher P1 = slot write, P2 = this mark+unpark; worker W1 = `take_dirty`, W2 = map re-read.
    /// Program order gives P1 -> P2 and W1 -> W2.
    ///
    /// The `map` lock totally orders P1 against W2, so the worker's re-read either observes the slot
    /// or does not:
    ///
    /// * W2 observes P1's write: the worker serves the work immediately, no park, no lost wake.
    /// * W2 precedes P1: the worker misses the slot and will park. Then W2 -> P1 combined with
    ///   W1 -> W2 and P1 -> P2 gives W1 -> P2, so this mark lands in a dirty set the worker has
    ///   ALREADY drained, sets `pending = true`, and unparks. An unpark landing before the park is
    ///   remembered, so the worker's next `step_or_park` returns at once (or never parks), it
    ///   re-runs `take_dirty` and sees `id`, re-reads the map (now past P1), and serves. No lost
    ///   wake.
    ///
    /// The contradictory interleaving P2 -> W1 with W2 -> P1 is impossible: it would require
    /// P1 -> P2 -> W1 -> W2 -> P1, a cycle. Hence the drain-before-map-read ordering the server loop
    /// guarantees is exactly what makes two independent locks lost-wakeup-free.
    pub(crate) fn notify(&self, id: GlobalId, worker_index: usize) {
        // A reader waits under the id it imported, which for a shared point may be an alias.
        let aliases: Vec<GlobalId> = {
            let aliases = self.inner.aliases.lock().expect("registry poisoned");
            aliases
                .aliases_of
                .get(&id)
                .map(|set| set.iter().copied().collect())
                .unwrap_or_default()
        };
        let mut wakers = self.inner.wakers.lock().expect("registry poisoned");
        if let Some(waker) = wakers.get_mut(worker_index).and_then(|w| w.as_mut()) {
            Self::mark(waker, id);
            for alias in aliases {
                Self::mark(waker, alias);
            }
        }
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

    /// Inserts `id` into `waker`'s dirty set and, if no wake is outstanding, arms the flag and
    /// unparks the worker. The coalescing flag collapses a burst of marks into one unpark.
    fn mark(waker: &mut Waker, id: GlobalId) {
        waker.dirty.insert(id);
        if !waker.pending {
            waker.pending = true;
            waker.worker.unpark();
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
