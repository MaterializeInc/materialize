// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A registry of published index arrangements, shared by one maintenance worker and its
//! interactive peer.
//!
//! An index arrangement is normally readable only from the timely worker that maintains it. When
//! `crate::render` publishes a maintained index through `crate::shared_trace` it records the
//! resulting `Published` points here, keyed by [`GlobalId`], so a reader on another thread or
//! runtime can mint a `Send` `SharedReader` for the same arrangement.
//!
//! A process holds one registry per local worker ordinal, and each runtime's worker with that
//! ordinal holds a clone of it. Pairing worker `i` of one runtime with worker `i` of the other is
//! sound only because both runtimes run the same number of workers per process at the same process
//! ordinal, so both sides shard keys by the same `key.hashed() % peers`.

// TODO(CPU-215): drop once `crate::render` and `crate::compute_state` call this registry. Only the
// registry's constructor is reachable yet, so the rest reads as dead.
#![expect(unused)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::thread::Thread;

use mz_repr::{Diff, GlobalId, Timestamp};
use timely::progress::Antichain;
use timely::worker::Worker;

use crate::shared_trace::{Published, SharedErrsHandle, SharedOksHandle, adopt_trace};
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
}

/// The registry's state: the published slots and the interactive peer's [`Waker`]. One lock
/// covers both. Every critical section is a few map operations, and the publisher takes it once per
/// seal, not per record.
#[derive(Default)]
struct Inner {
    /// Weak, so an entry lives exactly as long as a publisher or a reader holds its slot. Dead
    /// entries are pruned whenever a slot is created.
    map: BTreeMap<GlobalId, Weak<SharedIndexArrangement>>,
    /// `None` until the interactive peer registers its waker.
    waker: Option<Waker>,
}

/// A registry of published index arrangements, shared by one maintenance worker and its
/// interactive peer.
///
/// Cloning shares the same underlying map. A slot is an `Arc` held by its publisher, through an
/// [`UnpublishToken`], and by its readers. The registry itself holds none, so a slot nobody holds
/// is gone, and the publisher's trace detaches its points (`SharedSpine::detach_unreachable`).
#[derive(Clone, Default)]
pub struct ArrangementSharingRegistry {
    inner: Arc<Mutex<Inner>>,
}

impl ArrangementSharingRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates one registry per local worker ordinal, for a process running `workers_per_process`
    /// workers per runtime.
    pub fn per_worker(workers_per_process: usize) -> Vec<Self> {
        (0..workers_per_process).map(|_| Self::new()).collect()
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().expect("registry poisoned")
    }

    /// Returns the existing slot for `id`, or creates one backed by unbacked [`Published`] points
    /// and returns that instead.
    ///
    /// Whichever side touches `id` first creates the slot; the other observes and shares the same
    /// `Arc`, so a point a reader already imported is backed in place by a later
    /// [`crate::shared_trace::adopt_trace`] rather than being overwritten by a second,
    /// disconnected arrangement.
    ///
    /// An unbacked point carries no data, so this does not `notify`: there is nothing yet for a
    /// waiting reader to act on. [`Self::publish`] notifies once the publishers are installed.
    pub(crate) fn get_or_create(&self, id: GlobalId) -> Arc<SharedIndexArrangement> {
        let mut inner = self.lock();
        if let Some(slot) = inner.map.get(&id).and_then(Weak::upgrade) {
            return slot;
        }
        let slot = Arc::new(SharedIndexArrangement {
            oks: Published::new(),
            errs: Published::new(),
        });
        inner.map.retain(|_, slot| slot.strong_count() > 0);
        inner.map.insert(id, Arc::downgrade(&slot));
        slot
    }

    /// The slot for `id`, if someone holds it.
    fn slot(inner: &Inner, id: &GlobalId) -> Option<Arc<SharedIndexArrangement>> {
        inner.map.get(id).and_then(Weak::upgrade)
    }

    /// Publishes index `id`'s `oks` and `errs` traces and wakes readers waiting on `id`. `worker`
    /// must be the worker that maintains the traces.
    ///
    /// Adopts the slot for `id` rather than inserting a fresh one, so a placeholder a reader has
    /// already imported is backed in place. Each half signals its own seal: a peek whose result is
    /// an error carries its data on the errs arrangement, whose frontier is held back until the
    /// error is emitted, so an oks-only signal would leave that peek parked.
    ///
    /// Every id gets its own publication point, including an index that re-exports another's
    /// arrangement. The point's writer frontier and standing hold are per collection, and the
    /// controller compacts two collections independently even when they share a trace.
    ///
    /// The publication lasts as long as the returned token.
    #[must_use]
    pub(crate) fn publish(
        &self,
        id: GlobalId,
        worker: &Worker,
        oks: &RowRowAgent<Timestamp, Diff>,
        errs: &ErrAgent<Timestamp, Diff>,
    ) -> UnpublishToken {
        let slot = self.get_or_create(id);
        let registry = self.clone();
        adopt_trace(oks, worker, &slot.oks, move || registry.notify(id));
        let registry = self.clone();
        adopt_trace(errs, worker, &slot.errs, move || registry.notify(id));
        self.notify(id);
        UnpublishToken {
            registry: self.clone(),
            id,
            slot: Some(slot),
        }
    }

    /// Mints reader handles for `id`, if published.
    pub(crate) fn handles(&self, id: &GlobalId) -> Option<(SharedOksHandle, SharedErrsHandle)> {
        let inner = self.lock();
        let slot = Self::slot(&inner, id)?;
        Some((slot.oks.handle(), slot.errs.handle()))
    }

    /// The accumulated `oks` logical holds registered against `id`, if published.
    ///
    /// Test-only. Minting a handle to observe the published frontiers cannot distinguish a live
    /// reader hold from a frontier that happens to sit there, and that distinction is what says
    /// whether an import is still protected. Empty when every hold has released.
    #[cfg(test)]
    pub(crate) fn published_logical_holds(&self, id: &GlobalId) -> Option<Antichain<Timestamp>> {
        let inner = self.lock();
        let slot = Self::slot(&inner, id)?;
        Some(slot.oks.logical_holds())
    }

    /// Registers `worker` as the interactive peer's waker. Called once at startup, from that
    /// worker's own thread.
    ///
    /// Overwrites any prior waker, starting with an empty dirty set.
    pub(crate) fn register_waker(&self, worker: Thread) {
        let mut inner = self.lock();
        inner.waker = Some(Waker {
            worker,
            dirty: BTreeSet::new(),
        });
    }

    /// Atomically drains and returns the interactive peer's dirty set. Returns empty if no waker is
    /// registered.
    ///
    /// Called by the interactive server loop on wake. See `notify` for why the loop MUST
    /// call this before re-reading the map: draining before the map re-check is what closes the
    /// lost-wakeup window.
    pub(crate) fn take_dirty(&self) -> BTreeSet<GlobalId> {
        let mut inner = self.lock();
        match &mut inner.waker {
            Some(waker) => std::mem::take(&mut waker.dirty),
            None => BTreeSet::new(),
        }
    }

    /// Marks `id` dirty for the interactive peer and unparks it.
    ///
    /// [`Self::publish`] calls this once a slot's publishers are installed, and each publisher calls
    /// it again on every seal, since a fast-path peek waiting on the shared trace's `upper` is
    /// re-examined only when that advance marks `id` dirty.
    ///
    /// # Lost-wakeup contract
    ///
    /// The publication a mark announces and the mark itself are separate critical sections: a
    /// publisher backs its slot (the point's own state lock, released before `on_seal` fires), then
    /// calls this. On wake the interactive server loop runs `take_dirty` and only then re-reads the
    /// slot via `handles`, again two acquisitions. Label the four steps: publisher P1 = slot write,
    /// P2 = this mark+unpark; worker W1 = `take_dirty`, W2 = slot re-read. Program order gives
    /// P1 -> P2 and W1 -> W2.
    ///
    /// P1 and W2 are totally ordered, so the worker's re-read either observes the slot or does not:
    ///
    /// * W2 observes P1's write: the worker serves the work immediately, no park, no lost wake.
    /// * W2 precedes P1: the worker misses the slot and will park. Then W2 -> P1 combined with
    ///   W1 -> W2 and P1 -> P2 gives W1 -> P2, so this mark lands in a dirty set the worker has
    ///   ALREADY drained, and unparks. An unpark landing before the park is remembered, so the
    ///   worker's next `step_or_park` returns at once (or never parks), it re-runs `take_dirty`
    ///   and sees `id`, re-reads the slot (now past P1), and serves. No lost wake.
    ///
    /// The contradictory interleaving P2 -> W1 with W2 -> P1 is impossible: it would require
    /// P1 -> P2 -> W1 -> W2 -> P1, a cycle. Hence the drain-before-re-read ordering the server loop
    /// guarantees is what makes the separate critical sections lost-wakeup-free.
    pub(crate) fn notify(&self, id: GlobalId) {
        let mut inner = self.lock();
        if let Some(waker) = &mut inner.waker {
            Self::mark(waker, id);
        }
    }

    /// Inserts `id` into `waker`'s dirty set and unparks the worker.
    fn mark(waker: &mut Waker, id: GlobalId) {
        waker.dirty.insert(id);
        // `unpark` coalesces by itself: the thread keeps one token, and a wake while it runs costs
        // an atomic swap without a syscall.
        waker.worker.unpark();
    }
}

/// The publisher's hold on a published slot. Dropping it ends the publication.
pub(crate) struct UnpublishToken {
    registry: ArrangementSharingRegistry,
    id: GlobalId,
    /// `Some` until dropped.
    slot: Option<Arc<SharedIndexArrangement>>,
}

impl Drop for UnpublishToken {
    fn drop(&mut self) {
        // Released before the mark, so the reader that re-checks finds the slot gone unless it
        // holds the slot itself.
        drop(self.slot.take());
        self.registry.notify(self.id);
    }
}

#[cfg(test)]
mod tests;
