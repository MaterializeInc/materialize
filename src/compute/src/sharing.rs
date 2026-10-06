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
use std::thread::{Thread, ThreadId};

use mz_repr::{Diff, GlobalId, Timestamp};
use timely::progress::Antichain;
use timely::worker::Worker;

use crate::shared_trace::{Published, adopt_trace};
#[cfg(test)]
use crate::shared_trace::{SharedErrsHandle, SharedOksHandle};
use crate::typedefs::{ErrAgent, ErrSpine, RowRowAgent, RowRowSpine};

/// The published `oks`/`errs` arrangements of one maintained index on one worker.
///
/// An index's `oks` is always a `RowRowSpine` and its `errs` always an `ErrSpine`. Holding the
/// `Published` values keeps the publication points registered and lets us mint further handles.
pub struct SharedIndexArrangement {
    /// The `as_of` of the dataflow that exports the index, which tells two incarnations of one id
    /// apart.
    as_of: Antichain<Timestamp>,
    /// The published `oks` arrangement.
    pub(crate) oks: Published<RowRowSpine<Timestamp, Diff>>,
    /// The published `errs` arrangement.
    pub(crate) errs: Published<ErrSpine<Timestamp, Diff>>,
}

/// The registry's state: the published slots and the two workers attached to them. One lock covers
/// all three. Every critical section is a few map operations, and the publisher takes it on every
/// seal of every point it backs, never per record.
#[derive(Default)]
struct Inner {
    /// Weak, so an entry lives exactly as long as a publisher or a reader holds its slot. Dead
    /// entries are pruned whenever a slot is created. Holds the newest incarnation of each id, and
    /// an older one lives on outside the map for as long as someone holds it.
    map: BTreeMap<GlobalId, Weak<SharedIndexArrangement>>,
    /// The worker that publishes here, `None` until it attaches.
    publisher: Option<ThreadId>,
    /// The worker that reads here and is woken on a publication, `None` until it attaches.
    ///
    /// Unparked rather than activated: every timely allocator's `await_events` bottoms out in
    /// `std::thread::park`, and a root-path `SyncActivator` would additionally mark the worker's
    /// dataflows schedulable, which is work this wake does not need.
    reader: Option<Thread>,
}

/// A registry of published index arrangements, shared by one maintenance worker and its
/// interactive peer.
///
/// Cloning shares the same underlying map. A slot is an `Arc` held by its publisher, through a
/// `PublishToken`, and by its readers. The registry itself holds none, so a slot nobody holds
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

    /// Returns the slot for the incarnation of `id` whose dataflow has `as_of`, creating one backed
    /// by unbacked [`Published`] points if there is none.
    ///
    /// Whichever side touches an incarnation first creates the slot, and the other observes and
    /// shares the same `Arc`. So a point a reader already imported is backed in place by a later
    /// [`crate::shared_trace::adopt_trace`] rather than being overwritten by a second,
    /// disconnected arrangement.
    ///
    /// A reconnect can recreate an index under its id with an `as_of` below the old incarnation's
    /// `since`, and the two runtimes reconcile in either order. Keying on the `as_of` keeps a read of
    /// the new incarnation off the old one's compacted chain.
    ///
    /// An unbacked point carries no data, so this does not `notify`: there is nothing yet for a
    /// waiting reader to act on. [`Self::publish`] notifies once the publishers are installed.
    pub(crate) fn get_or_create(
        &self,
        id: GlobalId,
        as_of: &Antichain<Timestamp>,
    ) -> Arc<SharedIndexArrangement> {
        let mut inner = self.lock();
        if let Some(slot) = inner.map.get(&id).and_then(Weak::upgrade) {
            if slot.as_of == *as_of {
                return slot;
            }
        }
        let slot = Arc::new(SharedIndexArrangement {
            as_of: as_of.clone(),
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

    /// Publishes index `id`'s `oks` and `errs` traces and wakes readers waiting on `id`. `as_of` is
    /// the `as_of` of the dataflow that exports the index, and `worker` must be the worker that
    /// maintains the traces.
    ///
    /// Adopts the slot for `id` at `as_of`, see [`Self::get_or_create`]. Each half signals its own
    /// seal. A peek whose result is an error carries its data on the errs arrangement, whose
    /// frontier is held back until the error is emitted, so an oks-only signal would leave that
    /// peek parked.
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
        as_of: &Antichain<Timestamp>,
        worker: &Worker,
        oks: &RowRowAgent<Timestamp, Diff>,
        errs: &ErrAgent<Timestamp, Diff>,
    ) -> PublishToken {
        let slot = self.get_or_create(id, as_of);
        let registry = self.clone();
        adopt_trace(oks, worker, &slot.oks, move || registry.notify());
        let registry = self.clone();
        adopt_trace(errs, worker, &slot.errs, move || registry.notify());
        self.notify();
        PublishToken {
            registry: self.clone(),
            slot: Some(slot),
        }
    }

    /// Mints reader handles for `id`, if published. Test-only.
    #[cfg(test)]
    pub(crate) fn handles(&self, id: &GlobalId) -> Option<(SharedOksHandle, SharedErrsHandle)> {
        let inner = self.lock();
        let slot = Self::slot(&inner, id)?;
        Some((slot.oks.handle(), slot.errs.handle()))
    }

    /// Attaches the current thread as the worker that publishes here.
    ///
    /// Worker `i` of each runtime holds registry `i`, and both sides must be a single worker. Two
    /// publishing workers hold different shards of an index and would back one slot with both, and
    /// a second reader would take over the wake, so the first one's waiting peeks never wake.
    ///
    /// # Panics
    ///
    /// Panics if another thread attached as the publisher.
    pub(crate) fn attach_publisher(&self) {
        let current = std::thread::current().id();
        let previous = self.lock().publisher.replace(current);
        assert!(
            previous.is_none_or(|previous| previous == current),
            "a sharing registry has one publishing worker"
        );
    }

    /// Attaches the current thread as the worker that reads here, which a publication unparks. A
    /// peek waiting on a seal is served by the worker's next sweep, so the unpark is all it needs.
    ///
    /// # Panics
    ///
    /// Panics if another thread attached as the reader, see [`Self::attach_publisher`].
    pub(crate) fn attach_reader(&self) {
        let current = std::thread::current();
        let mut inner = self.lock();
        if let Some(previous) = &inner.reader {
            assert_eq!(
                previous.id(),
                current.id(),
                "a sharing registry has one reading worker"
            );
        }
        inner.reader = Some(current);
    }

    /// Unparks the reading worker, if one is attached.
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
        if let Some(reader) = &self.lock().reader {
            // `unpark` coalesces by itself: the thread keeps one token, and a wake while it runs
            // costs an atomic swap without a syscall.
            reader.unpark();
        }
    }
}

/// The publisher's hold on a published slot. Dropping it ends the publication.
pub(crate) struct PublishToken {
    registry: ArrangementSharingRegistry,
    /// `Some` until dropped.
    slot: Option<Arc<SharedIndexArrangement>>,
}

impl Drop for PublishToken {
    fn drop(&mut self) {
        // Dropped before the wake, so a woken reader finds the slot gone unless it holds the slot
        // itself.
        drop(self.slot.take());
        self.registry.notify();
    }
}

#[cfg(test)]
mod tests;
