// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Test-only accessors, drivers, and hooks of the buffer pool. Child module so
//! the items reach the pool's private state without per-item gates.

// The file-mode and file-store tests are Linux-only, so their helpers are
// unused in non-Linux test builds.
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use super::*;

// Test hook fired inside `PoolInner::enforce_budget`, between a pass's final
// counter read and the release of the `enforcing` guard. A test arms it on the
// thread whose pass it wants to freeze, to interleave a concurrent over-budget
// insert. One-shot: the hook is taken before it runs, so a re-enforcing pass
// does not re-arm.
thread_local! {
    pub(super) static ENFORCE_BUDGET_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

pub(super) fn run_enforce_budget_hook() {
    let hook = ENFORCE_BUDGET_HOOK.with(|cell| cell.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

// Test hook fired inside a demotion, after the chunk lock is released and
// before the file write. One-shot, like `ENFORCE_BUDGET_HOOK`.
thread_local! {
    pub(super) static DEMOTION_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

pub(super) fn run_demotion_hook() {
    let hook = DEMOTION_HOOK.with(|cell| cell.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

// Test counter of the file store slot probes demotions made on this thread,
// so tests can bound the work a pass does against the store.
thread_local! {
    pub(super) static DEMOTION_PROBES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

pub(super) fn demotion_probes() -> u64 {
    DEMOTION_PROBES.with(std::cell::Cell::get)
}

impl Pool {
    /// As [`Pool::new`], with a caller-chosen virtual reservation per size
    /// class. Small reservations let tests exercise slot exhaustion.
    pub(super) fn with_class_capacity(class_capacity_bytes: usize) -> std::io::Result<Pool> {
        Pool::with_backend_and_class_capacity(ExtentBackend::Swap, class_capacity_bytes)
    }

    /// Test hook: performs one eager-backing step on the calling thread.
    /// Returns whether progress was made.
    pub(super) fn back_step(&self) -> bool {
        self.0.back_one()
    }

    /// Test hook: waits until the spill queue is empty and no entry is being
    /// processed, so tests observe deterministic post-eviction states.
    pub(super) fn quiesce_spill(&self) {
        while self.0.spill.in_flight.load(Ordering::Relaxed) > 0 {
            std::thread::yield_now();
        }
    }

    /// Test hook: stops and joins the spill threads, so a test binary exits
    /// with none alive (which Miri requires). Stopped threads process no
    /// further queued work; call [`Pool::quiesce_spill`] first when the test
    /// depends on the queue draining.
    pub(super) fn join_spill_threads(&self) {
        self.0.spill.stop.store(true, Ordering::Relaxed);
        self.0.spill.cv.notify_all();
        let handles =
            std::mem::take(&mut *self.0.spill.handles.lock().expect("spill handles poisoned"));
        for handle in handles {
            handle.join().expect("spill thread panicked");
        }
    }

    /// Test hook: enables spill scheduling without spawning threads, so tests
    /// drive the queue deterministically via [`Pool::spill_step`].
    pub(super) fn enable_spill_without_threads(&self) {
        self.0.spill.enabled.store(true, Ordering::Relaxed);
    }

    /// Test hook: makes the pool behave as if spill threads exist, without
    /// spawning any or enabling hand-off, so the inline backstop and the
    /// backstop threshold apply to inline passes and nothing else demotes.
    pub(super) fn fake_spill_threads(&self) {
        self.0.spill.threads.store(1, Ordering::Relaxed);
    }

    /// Test hook: processes one queued spill entry on the calling thread.
    /// Returns whether an entry was processed.
    pub(super) fn spill_step(&self) -> bool {
        let popped = self.0.spill_queue().pop_front();
        let Some(meta) = popped else {
            return false;
        };
        self.0.spill_process(&meta, SpillKind::Evict);
        self.0.spill.in_flight.fetch_sub(1, Ordering::Relaxed);
        // The spill loop's trim, without its pacing, so tests observe
        // deterministic post-commit states. While the full hint is set, a
        // pass would probe the store for nothing.
        if !self.0.full_hint.load(Ordering::Relaxed) {
            self.0.enforce_compressed_cap(Pass::Background);
        }
        true
    }

    /// Test hook: runs one inline compressed-cap enforcement pass on the
    /// calling thread.
    pub(super) fn enforce_compressed(&self) {
        self.0.enforce_compressed_cap(Pass::Inline);
    }

    /// Test hook: the file store of a file-mode pool.
    pub(super) fn file_store(&self) -> Option<Arc<FileStore>> {
        match &self.0.store {
            ExtentStore::Swap => None,
            ExtentStore::File(store) => Some(Arc::clone(store)),
        }
    }

    /// Test hook: evicts cold chunks until resident bytes fall to the budget
    /// or every queued chunk has been visited once. Enforcement runs
    /// automatically on every insert and budget shrink.
    pub(super) fn enforce_budget(&self) {
        self.0.enforce_budget();
    }

    /// Test hook: runs one compressed-cap enforcement pass inline on the
    /// calling thread, where the fake residency observation applies.
    pub(super) fn enforce_rss_target(&self) {
        self.0.enforce_compressed_cap(Pass::Inline);
    }

    /// Test-only: the number of entries across the second-chance queues,
    /// live and stale.
    pub(super) fn queue_len(&self) -> usize {
        (0..DEPTH_BANDS).map(|band| self.0.queue(band).len()).sum()
    }

    /// Test-only: the number of resident-extent queue entries, live and
    /// stale.
    pub(super) fn extent_queue_len(&self) -> usize {
        self.0.extent_queue().len()
    }

    /// Test hook: explicitly evicts one chunk. No-op if the chunk is already
    /// evicted, in flight, empty, or oversize. With spill threads enabled the
    /// compression is handed off and completes asynchronously (observable via
    /// [`Residency::WriteInFlight`]); without them it runs inline.
    pub(super) fn evict(&self, handle: &ChunkHandle) {
        let meta = &handle.meta;
        let mut state = meta.state();
        if !meta.pool.spill_handoff(meta, &mut state) {
            meta.pool.evict_locked(meta, &mut state);
        }
        drop(state);
        meta.pool.enforce_or_defer_compressed_cap();
    }

    /// Test hook: overwrites every free slot's bytes with `0xDE`. The free
    /// list keeps a freed slot's old bytes on platforms where
    /// `MADV_DONTNEED` retains contents (macOS); poisoning lets tests prove
    /// that reads of evicted chunks decompress from the extent rather than
    /// passing stale slot memory through.
    pub(super) fn poison_free_slots(&self) {
        for region in &self.0.regions {
            region.poison_free_slots();
        }
    }
}

impl ChunkHandle {
    /// Test hook: the chunk's current residency state.
    pub(super) fn residency(&self) -> Residency {
        self.meta.state().residency
    }

    /// Test hook: the file slot of the chunk's extent, if demoted.
    pub(super) fn file_slot(&self) -> Option<file::FileSlot> {
        self.demoted_file().map(|(_, slot)| slot)
    }

    /// Test hook: the store and file slot of the chunk's extent, if
    /// demoted.
    pub(super) fn demoted_file(&self) -> Option<(Arc<FileStore>, file::FileSlot)> {
        self.meta.state().extent.as_ref()?.file_slot()
    }

    /// Test hook: corrupts the file slot of the chunk's demoted extent.
    pub(super) fn corrupt_file_extent(&self) {
        let (store, slot) = self.demoted_file().expect("extent is demoted");
        store.corrupt(slot);
    }

    /// Test hook: the byte size of the chunk's size class, or `None` for
    /// empty and oversize chunks.
    pub(super) fn size_class_bytes(&self) -> Option<usize> {
        self.meta.class.map(|class| SIZE_CLASSES[class])
    }
}
