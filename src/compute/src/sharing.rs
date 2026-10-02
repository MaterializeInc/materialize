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

// TODO(CPU-215): drop once `crate::compute_state` serves peeks through this registry. Until then
// some of its methods are called only from tests, so the expectation holds outside tests alone.
#![cfg_attr(not(test), expect(unused))]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::thread::Thread;

use mz_repr::{Diff, GlobalId, Timestamp};
use timely::progress::Antichain;
use timely::worker::Worker;

use crate::arrangement::manager::TraceBundle;
use crate::shared_trace::{Published, adopt_trace};
#[cfg(test)]
use crate::shared_trace::{SharedErrsHandle, SharedOksHandle};
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

/// The registry's state: the published slots and the thread of the interactive peer that reads
/// them. One lock covers both. Every critical section is a few map operations, and the publisher takes it once per
/// seal, not per record.
#[derive(Default)]
struct Inner {
    /// Weak, so an entry lives exactly as long as a publisher or a reader holds its slot. Dead
    /// entries are pruned whenever a slot is created.
    map: BTreeMap<GlobalId, Weak<SharedIndexArrangement>>,
    /// `None` until the interactive peer registers itself.
    ///
    /// Unparked rather than activated: every timely allocator's `await_events` bottoms out in
    /// `std::thread::park`, and a root-path `SyncActivator` would additionally mark the worker's
    /// dataflows schedulable, which is work this wake does not need.
    waker: Option<Thread>,
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
    #[cfg(test)]
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
    /// arrangement. The point's writer frontier and peer holds are per collection, and the
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
        adopt_trace(oks, worker, &slot.oks, move || registry.notify());
        let registry = self.clone();
        adopt_trace(errs, worker, &slot.errs, move || registry.notify());
        self.notify();
        UnpublishToken {
            registry: self.clone(),
            slot: Some(slot),
        }
    }

    /// The bundle through which this runtime holds and reads index `id`, which its peer publishes.
    ///
    /// The bundle's logical compaction is this runtime's hold on the publication. It holds nothing
    /// physically, so the publisher keeps merging. Imports from the bundle mint readers that do.
    /// The slot lives as long as the bundle.
    pub(crate) fn peer_bundle(&self, id: GlobalId, as_of: &Antichain<Timestamp>) -> TraceBundle {
        let slot = self.get_or_create(id);
        let oks = slot.oks.peer_handle(as_of);
        let errs = slot.errs.peer_handle(as_of);
        TraceBundle::shared(oks, errs).with_drop(slot)
    }

    /// Mints reader handles for `id`, if published. Test-only: production reads hold a slot through
    /// [`Self::peer_bundle`].
    #[cfg(test)]
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

    /// Registers `worker` as the interactive peer to wake. Called once at startup, from that
    /// worker's own thread.
    pub(crate) fn register_waker(&self, worker: Thread) {
        self.lock().waker = Some(worker);
    }

    /// Unparks the interactive peer, if one is registered.
    ///
    /// [`Self::publish`] calls this once a slot's publishers are installed, each publisher calls it
    /// again on every seal, and an unpublication calls it once more, since a peek waiting on a
    /// shared trace is re-examined only when its worker runs.
    ///
    /// No wake is lost. The publisher updates the point before it calls this, and the worker
    /// re-reads every waiting peek's trace each time it runs, before it parks. A wake that lands
    /// after that read is remembered by `unpark`, so the worker's next park returns at once and it
    /// reads again.
    pub(crate) fn notify(&self) {
        if let Some(waker) = &self.lock().waker {
            // `unpark` coalesces by itself: the thread keeps one token, and a wake while it runs
            // costs an atomic swap without a syscall.
            waker.unpark();
        }
    }
}

/// Publishes this runtime's index arrangements for the process's other compute runtime, or does
/// nothing where that runtime reads none. Chosen once, when the runtime is built.
#[derive(Clone)]
pub(crate) enum Publisher {
    /// The runtime publishes nothing.
    None,
    /// The runtime publishes into the registry it shares with its peer worker.
    Registry(ArrangementSharingRegistry),
}

impl Publisher {
    /// Publishes index `id`'s traces, see [`ArrangementSharingRegistry::publish`]. The publication
    /// lasts as long as the returned token.
    pub(crate) fn publish(
        &self,
        id: GlobalId,
        worker: &Worker,
        oks: &RowRowAgent<Timestamp, Diff>,
        errs: &ErrAgent<Timestamp, Diff>,
    ) -> Option<UnpublishToken> {
        match self {
            Publisher::None => None,
            Publisher::Registry(registry) => Some(registry.publish(id, worker, oks, errs)),
        }
    }
}

/// Reads the indexes the process's other compute runtime publishes, or reads none where that runtime
/// publishes none. Chosen once, when the runtime is built.
#[derive(Clone)]
pub(crate) enum PeerTraces {
    /// The other runtime publishes nothing for this one.
    None,
    /// The other runtime publishes into the registry this runtime's worker shares with it.
    Registry(ArrangementSharingRegistry),
}

impl PeerTraces {
    /// Reads the indexes published into `registry`, and has a publication's seal unpark the current
    /// thread, which must be the worker that reads them. A peek waiting on a seal is served by the
    /// worker's next sweep, so the unpark is all it needs.
    pub(crate) fn reading(registry: ArrangementSharingRegistry) -> Self {
        registry.register_waker(std::thread::current());
        PeerTraces::Registry(registry)
    }

    /// The bundle through which this runtime holds and reads index `id`, if its peer publishes it.
    /// See [`ArrangementSharingRegistry::peer_bundle`].
    pub(crate) fn bundle(&self, id: GlobalId, as_of: &Antichain<Timestamp>) -> Option<TraceBundle> {
        match self {
            PeerTraces::None => None,
            PeerTraces::Registry(registry) => Some(registry.peer_bundle(id, as_of)),
        }
    }
}

/// The publisher's hold on a published slot. Dropping it ends the publication.
pub(crate) struct UnpublishToken {
    registry: ArrangementSharingRegistry,
    /// `Some` until dropped.
    slot: Option<Arc<SharedIndexArrangement>>,
}

impl Drop for UnpublishToken {
    fn drop(&mut self) {
        // Released before the mark, so the reader that re-checks finds the slot gone unless it
        // holds the slot itself.
        drop(self.slot.take());
        self.registry.notify();
    }
}

#[cfg(test)]
mod tests;
