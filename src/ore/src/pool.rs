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

//! Prototype buffer pool for dataflow state. See
//! `doc/developer/design/20260610_buffer_managed_state.md`.
//!
//! The pool is the cache: size-class anonymous virtual-memory regions whose
//! slot addresses are stable for a chunk's whole lifetime, so a fault-in never
//! invalidates a pointer. The backing is the swap-backed extent store of the
//! design's Layer 1: a page-aligned anonymous allocation holding the chunk's
//! lz4-compressed bytes, pushed to the swap device with `MADV_PAGEOUT`.
//!
//! Residency is a state, not a type. This prototype uses the design's
//! synchronous executor, so the `WriteInFlight` and `Faulting` transitions
//! collapse into the evicting and faulting calls themselves and a chunk is
//! always in one of the [`Residency`] states. Chunks are immutable after
//! [`Pool::insert`], which is what makes a `BackedResident` slot always
//! identical to its extent and re-eviction free of I/O.
//!
//! Freeing an `UnbackedResident` chunk is a pure memory operation — the
//! design's "never write dead data" win, surfaced as `elided_frees` in
//! [`PoolStats`]. Budget pressure evicts cold chunks via a second-chance FIFO,
//! the design's backstop policy for unannotated chunks.

mod extent;
mod region;

use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::cast::CastFrom;
use crate::pool::extent::SwapExtent;
use crate::pool::region::{Region, SIZE_CLASSES};

/// Configuration for a [`Pool`].
#[derive(Debug, Clone, Copy)]
pub struct PoolConfig {
    /// Resident-bytes budget. [`Pool::enforce_budget`] evicts until the
    /// uncompressed bytes of resident chunks fall to this bound.
    pub budget_bytes: usize,
    /// Virtual reservation per size class. Purely virtual: physical memory
    /// materializes only for slots in use.
    pub class_capacity_bytes: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            budget_bytes: 256 << 20,
            class_capacity_bytes: 4 << 30,
        }
    }
}

/// Residency state of a chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Residency {
    /// Lives only in the pool; no extent copy exists. Freeing it never
    /// touches the backing store.
    UnbackedResident,
    /// Resident, and an identical extent copy exists; eviction releases
    /// physical pages without I/O.
    BackedResident,
    /// Extent copy only. The pool slot's contents are undefined and must
    /// never be read; access faults the chunk back in from the extent.
    Evicted,
    /// Larger than the largest size class; held as a plain heap allocation,
    /// always resident. A prototype limitation, not a design state.
    Oversize,
}

/// Snapshot of pool counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolStats {
    /// Chunks inserted.
    pub inserts: u64,
    /// Chunks freed (handle dropped or [`ChunkHandle::take`]n).
    pub frees: u64,
    /// Chunks freed while `UnbackedResident`: dead data whose backing write
    /// was never performed.
    pub elided_frees: u64,
    /// Evictions that compressed the chunk into a new extent.
    pub evictions_compress: u64,
    /// Evictions of `BackedResident` chunks: pure page release, no I/O.
    pub evictions_cheap: u64,
    /// Fault-ins of `Evicted` chunks.
    pub faults: u64,
    /// Compressed bytes written into extents.
    pub extent_bytes_written: u64,
    /// Uncompressed bytes of currently resident chunks (including oversize).
    pub resident_bytes: u64,
    /// Uncompressed bytes of live oversize chunks.
    pub oversize_bytes: u64,
}

#[derive(Debug, Default)]
struct Counters {
    inserts: AtomicU64,
    frees: AtomicU64,
    elided_frees: AtomicU64,
    evictions_compress: AtomicU64,
    evictions_cheap: AtomicU64,
    faults: AtomicU64,
    extent_bytes_written: AtomicU64,
    resident_bytes: AtomicU64,
    oversize_bytes: AtomicU64,
}

/// A buffer pool over swap-backed extents. Cheap to clone; all clones share
/// one budget and one backing store.
#[derive(Debug, Clone)]
pub struct Pool(Arc<PoolInner>);

#[derive(Debug)]
struct PoolInner {
    budget_bytes: u64,
    /// One region per entry of [`SIZE_CLASSES`], same order.
    regions: Vec<Region>,
    /// Second-chance FIFO of eviction candidates. Entries for freed chunks
    /// go stale in place and are dropped by [`PoolInner::prune_queue`].
    queue: Mutex<VecDeque<Weak<ChunkMeta>>>,
    /// Number of live slotted chunks, which is the number of non-stale queue
    /// entries; [`PoolInner::prune_queue`] compacts the queue against it.
    live_slotted: AtomicU64,
    counters: Counters,
}

/// Location of a chunk's pool slot.
#[derive(Debug, Clone, Copy)]
struct Slot {
    /// Index into [`SIZE_CLASSES`] and `PoolInner::regions`.
    class: usize,
    /// Slot index within the region.
    index: u32,
}

#[derive(Debug)]
struct ChunkMeta {
    pool: Arc<PoolInner>,
    /// Length in `u64` words; immutable.
    len: usize,
    /// `None` for empty and oversize chunks; immutable.
    slot: Option<Slot>,
    state: Mutex<ChunkState>,
}

#[derive(Debug)]
struct ChunkState {
    residency: Residency,
    pins: u32,
    /// Second-chance bit, set on pin and cleared (in lieu of eviction) when
    /// the budget enforcer first visits the chunk.
    touched: bool,
    /// Set when the owning handle is dropped, so a queue entry upgraded
    /// concurrently with the free cannot touch a recycled slot.
    freed: bool,
    /// The backing copy; present exactly in the `BackedResident` and
    /// `Evicted` states.
    extent: Option<SwapExtent>,
    /// The payload of an `Oversize` chunk.
    oversize: Option<Vec<u64>>,
}

impl ChunkMeta {
    fn len_bytes(&self) -> usize {
        self.len * std::mem::size_of::<u64>()
    }
}

/// Handle to one immutable chunk in a [`Pool`]. Dropping the handle frees the
/// chunk: the slot's physical pages are released, the slot returns to the
/// region free list, and the extent (if any) is deallocated, discarding any
/// swapped copy for free. Releasing the pages keeps RSS aligned with the
/// `resident_bytes` gauge the budget enforcer trusts; without it, freed slots
/// would hold warm pages the enforcer cannot see.
#[derive(Debug)]
pub struct ChunkHandle {
    meta: Arc<ChunkMeta>,
}

/// Pins a chunk resident for the guard's lifetime; derefs to the chunk's
/// contents. Pinned chunks are never evicted.
#[derive(Debug)]
pub struct PinGuard<'a> {
    meta: &'a ChunkMeta,
    ptr: *const u64,
    len: usize,
}

impl Pool {
    /// Creates a pool, reserving one virtual region per size class.
    pub fn new(cfg: PoolConfig) -> std::io::Result<Pool> {
        let regions = SIZE_CLASSES
            .iter()
            .map(|&class_size| Region::new(class_size, cfg.class_capacity_bytes))
            .collect::<std::io::Result<Vec<_>>>()?;
        Ok(Pool(Arc::new(PoolInner {
            budget_bytes: u64::cast_from(cfg.budget_bytes),
            regions,
            queue: Mutex::new(VecDeque::new()),
            live_slotted: AtomicU64::new(0),
            counters: Counters::default(),
        })))
    }

    /// Copies `data` into a pool slot of the smallest class that fits and
    /// clears `data`, preserving its capacity. The returned handle starts
    /// `UnbackedResident`. Empty input returns a length-0 handle holding no
    /// slot; input larger than the largest class falls back to a plain heap
    /// allocation ([`Residency::Oversize`]), a prototype limitation.
    pub fn insert(&self, data: &mut Vec<u64>) -> ChunkHandle {
        let inner = &self.0;
        inner.counters.inserts.fetch_add(1, Ordering::Relaxed);
        let len = data.len();
        let len_bytes = len * std::mem::size_of::<u64>();
        if len == 0 {
            return ChunkHandle {
                meta: Arc::new(ChunkMeta {
                    pool: Arc::clone(inner),
                    len: 0,
                    slot: None,
                    state: Mutex::new(ChunkState {
                        residency: Residency::UnbackedResident,
                        pins: 0,
                        touched: false,
                        freed: false,
                        extent: None,
                        oversize: None,
                    }),
                }),
            };
        }
        let class = SIZE_CLASSES.iter().position(|&c| c >= len_bytes);
        let meta = match class {
            Some(class) => {
                let region = &inner.regions[class];
                let index = region.alloc();
                let src: &[u8] = bytemuck::cast_slice(data.as_slice());
                // SAFETY: the freshly allocated slot is at least `len_bytes`
                // long (the class fits the payload) and is exclusively owned
                // by this chunk; the source is a live borrow of `data`, which
                // cannot alias an anonymous mapping.
                unsafe {
                    std::ptr::copy_nonoverlapping(src.as_ptr(), region.slot_ptr(index), len_bytes);
                }
                data.clear();
                inner
                    .counters
                    .resident_bytes
                    .fetch_add(u64::cast_from(len_bytes), Ordering::Relaxed);
                ChunkMeta {
                    pool: Arc::clone(inner),
                    len,
                    slot: Some(Slot { class, index }),
                    state: Mutex::new(ChunkState {
                        residency: Residency::UnbackedResident,
                        pins: 0,
                        touched: false,
                        freed: false,
                        extent: None,
                        oversize: None,
                    }),
                }
            }
            None => {
                let payload = data.as_slice().to_vec();
                data.clear();
                inner
                    .counters
                    .resident_bytes
                    .fetch_add(u64::cast_from(len_bytes), Ordering::Relaxed);
                inner
                    .counters
                    .oversize_bytes
                    .fetch_add(u64::cast_from(len_bytes), Ordering::Relaxed);
                ChunkMeta {
                    pool: Arc::clone(inner),
                    len,
                    slot: None,
                    state: Mutex::new(ChunkState {
                        residency: Residency::Oversize,
                        pins: 0,
                        touched: false,
                        freed: false,
                        extent: None,
                        oversize: Some(payload),
                    }),
                }
            }
        };
        let meta = Arc::new(meta);
        if meta.slot.is_some() {
            inner.live_slotted.fetch_add(1, Ordering::Relaxed);
            inner
                .queue
                .lock()
                .expect("pool queue poisoned")
                .push_back(Arc::downgrade(&meta));
        }
        inner.enforce_budget();
        ChunkHandle { meta }
    }

    /// Snapshot of the pool's counters.
    pub fn stats(&self) -> PoolStats {
        let c = &self.0.counters;
        PoolStats {
            inserts: c.inserts.load(Ordering::Relaxed),
            frees: c.frees.load(Ordering::Relaxed),
            elided_frees: c.elided_frees.load(Ordering::Relaxed),
            evictions_compress: c.evictions_compress.load(Ordering::Relaxed),
            evictions_cheap: c.evictions_cheap.load(Ordering::Relaxed),
            faults: c.faults.load(Ordering::Relaxed),
            extent_bytes_written: c.extent_bytes_written.load(Ordering::Relaxed),
            resident_bytes: c.resident_bytes.load(Ordering::Relaxed),
            oversize_bytes: c.oversize_bytes.load(Ordering::Relaxed),
        }
    }

    /// Evicts cold chunks until resident bytes fall to the budget or every
    /// queued chunk has been visited once. Runs automatically on every insert
    /// and fault-in; explicit calls are for tests and pressure hooks.
    pub fn enforce_budget(&self) {
        self.0.enforce_budget();
    }

    /// Test-only: the number of entries in the second-chance queue, live and
    /// stale.
    #[cfg(test)]
    fn queue_len(&self) -> usize {
        self.0.queue.lock().expect("pool queue poisoned").len()
    }

    /// Explicitly evicts one chunk. No-op if the chunk is pinned, already
    /// evicted, empty, or oversize.
    pub fn evict(&self, handle: &ChunkHandle) {
        let mut state = handle.meta.state.lock().expect("chunk state poisoned");
        handle.meta.pool.evict_locked(&handle.meta, &mut state);
    }

    /// Test hook: overwrites the chunk's slot bytes with `0xDE` if it has a
    /// slot and is unpinned, regardless of residency state. Simulates the
    /// kernel discarding `MADV_DONTNEED`ed pages — which macOS may not do —
    /// so tests can prove that fault-in reads the extent rather than stale
    /// slot memory.
    #[doc(hidden)]
    pub fn poison_resident(&self, handle: &ChunkHandle) {
        let state = handle.meta.state.lock().expect("chunk state poisoned");
        let Some(slot) = handle.meta.slot else {
            return;
        };
        if state.pins > 0 || state.freed {
            return;
        }
        let region = &handle.meta.pool.regions[slot.class];
        // SAFETY: the slot belongs to this live chunk and `pins == 0` under
        // the state lock, so no `PinGuard` borrow into it exists; the write
        // stays within the slot (`len_bytes` fits the class).
        unsafe {
            std::ptr::write_bytes(region.slot_ptr(slot.index), 0xDE, handle.meta.len_bytes());
        }
        drop(state);
    }
}

impl PoolInner {
    /// Drops queue entries whose chunk has been freed, detected by their
    /// `Weak` no longer holding a live chunk. The compaction runs only when
    /// stale entries outnumber live chunks (plus a small floor), so its cost
    /// amortizes to a constant per insert and the queue length stays
    /// proportional to the number of live slotted chunks even when the pool
    /// never comes under budget pressure.
    fn prune_queue(&self) {
        let live = usize::cast_from(self.live_slotted.load(Ordering::Relaxed));
        let mut queue = self.queue.lock().expect("pool queue poisoned");
        if queue.len() > 2 * live + 16 {
            queue.retain(|weak| weak.strong_count() > 0);
        }
    }

    fn enforce_budget(&self) {
        self.prune_queue();
        let resident = |counters: &Counters| counters.resident_bytes.load(Ordering::Relaxed);
        // Visit each queued chunk at most twice per call: a first visit may
        // only clear the second-chance bit, so a second is needed before an
        // over-budget call is guaranteed to evict every unpinned chunk it
        // saw. The bound keeps a queue of pinned chunks from spinning this
        // loop forever.
        let mut remaining = self
            .queue
            .lock()
            .expect("pool queue poisoned")
            .len()
            .saturating_mul(2);
        while remaining > 0 && resident(&self.counters) > self.budget_bytes {
            remaining -= 1;
            let popped = self.queue.lock().expect("pool queue poisoned").pop_front();
            let Some(weak) = popped else {
                break;
            };
            let Some(meta) = weak.upgrade() else {
                continue;
            };
            let requeue = {
                let mut state = meta.state.lock().expect("chunk state poisoned");
                if state.freed {
                    false
                } else if state.pins > 0 {
                    true
                } else if state.touched {
                    state.touched = false;
                    true
                } else {
                    self.evict_locked(&meta, &mut state);
                    // An evicted chunk stays queued: a later fault-in makes
                    // it resident again without re-inserting it.
                    true
                }
            };
            if requeue {
                self.queue
                    .lock()
                    .expect("pool queue poisoned")
                    .push_back(weak);
            }
        }
    }

    fn evict_locked(&self, meta: &ChunkMeta, state: &mut ChunkState) {
        let Some(slot) = meta.slot else {
            return;
        };
        if state.pins > 0 || state.freed {
            return;
        }
        let region = &self.regions[slot.class];
        match state.residency {
            Residency::UnbackedResident => {
                // SAFETY: the slot belongs to this live chunk, the state lock
                // is held and `pins == 0`, so nothing writes the slot while
                // this borrow is live; resident contents are initialized
                // (written at insert or fault-in) and `len` fits the class.
                let data = unsafe {
                    std::slice::from_raw_parts(
                        region.slot_ptr(slot.index).cast_const().cast::<u64>(),
                        meta.len,
                    )
                };
                let extent = SwapExtent::write(data);
                self.counters
                    .extent_bytes_written
                    .fetch_add(u64::cast_from(extent.comp_len()), Ordering::Relaxed);
                self.counters
                    .evictions_compress
                    .fetch_add(1, Ordering::Relaxed);
                state.extent = Some(extent);
            }
            Residency::BackedResident => {
                self.counters
                    .evictions_cheap
                    .fetch_add(1, Ordering::Relaxed);
            }
            Residency::Evicted | Residency::Oversize => return,
        }
        // SAFETY: the slot belongs to this live chunk with no pins (checked
        // above under the state lock), so no reference into it exists. The
        // chunk transitions to `Evicted` below, and every path that reads the
        // slot again first overwrites it in full from the extent, satisfying
        // the contents-undefined contract of `dontneed`.
        unsafe {
            region::dontneed(region.slot_ptr(slot.index), region.class_size());
        }
        state.residency = Residency::Evicted;
        self.counters
            .resident_bytes
            .fetch_sub(u64::cast_from(meta.len_bytes()), Ordering::Relaxed);
    }
}

impl ChunkHandle {
    /// Length of the chunk in `u64` words.
    pub fn len(&self) -> usize {
        self.meta.len
    }

    /// Returns `true` if the chunk holds no data.
    pub fn is_empty(&self) -> bool {
        self.meta.len == 0
    }

    /// Length of the chunk in bytes.
    pub fn len_bytes(&self) -> usize {
        self.meta.len_bytes()
    }

    /// The chunk's current residency state.
    pub fn residency(&self) -> Residency {
        self.meta
            .state
            .lock()
            .expect("chunk state poisoned")
            .residency
    }

    /// Pins the chunk resident, faulting it in from its extent if evicted,
    /// and returns a guard dereferencing to its contents. Concurrent pinners
    /// of an evicted chunk serialize on the chunk's state lock; the second
    /// observes `BackedResident` and skips the fault.
    ///
    /// A fault-in raises resident bytes and so enforces the budget, keeping
    /// read-only traffic (a seek-heavy phase performs no inserts) bounded by
    /// the budget; the just-pinned chunk is protected by its pin count.
    pub fn pin(&self) -> PinGuard<'_> {
        let meta = &*self.meta;
        let mut state = meta.state.lock().expect("chunk state poisoned");
        let mut faulted = false;
        let ptr = match state.residency {
            Residency::Oversize => {
                let payload = state.oversize.as_ref().expect("oversize chunk has payload");
                payload.as_ptr()
            }
            Residency::Evicted => {
                let slot = meta.slot.expect("evicted chunk has a slot");
                let region = &meta.pool.regions[slot.class];
                let slot_ptr = region.slot_ptr(slot.index);
                // SAFETY: the slot belongs to this live chunk, the state lock
                // is held and `pins == 0` in the `Evicted` state, so no other
                // reference into the slot exists; `len_bytes` fits the class.
                let dst = unsafe { std::slice::from_raw_parts_mut(slot_ptr, meta.len_bytes()) };
                state
                    .extent
                    .as_ref()
                    .expect("evicted chunk has an extent")
                    .read_into(dst);
                state.residency = Residency::BackedResident;
                faulted = true;
                meta.pool.counters.faults.fetch_add(1, Ordering::Relaxed);
                meta.pool
                    .counters
                    .resident_bytes
                    .fetch_add(u64::cast_from(meta.len_bytes()), Ordering::Relaxed);
                slot_ptr.cast_const().cast::<u64>()
            }
            Residency::UnbackedResident | Residency::BackedResident => match meta.slot {
                Some(slot) => meta.pool.regions[slot.class]
                    .slot_ptr(slot.index)
                    .cast_const()
                    .cast::<u64>(),
                None => std::ptr::NonNull::<u64>::dangling().as_ptr().cast_const(),
            },
        };
        state.touched = true;
        state.pins += 1;
        drop(state);
        // Enforce after releasing the state lock: the enforcer locks chunk
        // states itself, and the pin count already shields this chunk from
        // being chosen as a victim.
        if faulted {
            meta.pool.enforce_budget();
        }
        PinGuard {
            meta,
            ptr,
            len: meta.len,
        }
    }

    /// If the chunk is evicted, hints the kernel to swap its extent back in
    /// ahead of need; otherwise a no-op.
    pub fn prefetch(&self) {
        let state = self.meta.state.lock().expect("chunk state poisoned");
        if state.residency == Residency::Evicted {
            if let Some(extent) = &state.extent {
                extent.prefetch();
            }
        }
    }

    /// Copies the whole contents into `dst` (cleared first), then frees the
    /// chunk.
    pub fn take(self, dst: &mut Vec<u64>) {
        dst.clear();
        if self.meta.len > 0 {
            let pin = self.pin();
            dst.extend_from_slice(&pin);
        }
    }

    /// Test-only: the byte size of the chunk's size class, or `None` for
    /// empty and oversize chunks.
    #[cfg(test)]
    pub(crate) fn size_class_bytes(&self) -> Option<usize> {
        self.meta.slot.map(|slot| SIZE_CLASSES[slot.class])
    }
}

impl Drop for ChunkHandle {
    fn drop(&mut self) {
        let pool = &self.meta.pool;
        let mut state = self.meta.state.lock().expect("chunk state poisoned");
        debug_assert_eq!(state.pins, 0, "chunk freed while pinned");
        pool.counters.frees.fetch_add(1, Ordering::Relaxed);
        state.freed = true;
        if self.meta.slot.is_some() {
            pool.live_slotted.fetch_sub(1, Ordering::Relaxed);
        }
        let len_bytes = u64::cast_from(self.meta.len_bytes());
        // Releases the slot's physical pages and returns it to the free list.
        // The release is what makes freeing a resident chunk lower actual RSS
        // in step with the `resident_bytes` decrement; a pure free-list push
        // would leave the pages warm in the free slot, invisible to the
        // budget enforcer.
        let release_slot = |slot: Slot, resident: bool| {
            let region = &pool.regions[slot.class];
            if resident {
                // SAFETY: the handle is being dropped, so no `PinGuard`
                // (which borrows the handle) exists, and `freed` was set
                // under the state lock held here, so concurrent queue
                // visitors skip the chunk. The slot's next occupant fully
                // overwrites every byte it later reads, satisfying the
                // contents-undefined contract of `dontneed`.
                unsafe {
                    region::dontneed(region.slot_ptr(slot.index), region.class_size());
                }
            }
            region.free(slot.index);
        };
        match state.residency {
            Residency::UnbackedResident => {
                if let Some(slot) = self.meta.slot {
                    pool.counters.elided_frees.fetch_add(1, Ordering::Relaxed);
                    pool.counters
                        .resident_bytes
                        .fetch_sub(len_bytes, Ordering::Relaxed);
                    release_slot(slot, true);
                }
            }
            Residency::BackedResident => {
                let slot = self.meta.slot.expect("backed chunk has a slot");
                pool.counters
                    .resident_bytes
                    .fetch_sub(len_bytes, Ordering::Relaxed);
                release_slot(slot, true);
                state.extent = None;
            }
            Residency::Evicted => {
                let slot = self.meta.slot.expect("evicted chunk has a slot");
                // Eviction already released the slot's pages.
                release_slot(slot, false);
                state.extent = None;
            }
            Residency::Oversize => {
                pool.counters
                    .resident_bytes
                    .fetch_sub(len_bytes, Ordering::Relaxed);
                pool.counters
                    .oversize_bytes
                    .fetch_sub(len_bytes, Ordering::Relaxed);
                state.oversize = None;
            }
        }
    }
}

impl Deref for PinGuard<'_> {
    type Target = [u64];

    fn deref(&self) -> &[u64] {
        // SAFETY: `ptr`/`len` were captured under the chunk's state lock with
        // the pin count incremented. Eviction, poisoning, and freeing all
        // check the pin count under that lock and skip pinned chunks, and
        // chunks are immutable after insert, so the pointee is initialized,
        // valid, and unaliased by writers for the guard's lifetime. Slot
        // addresses are stable, and for the empty chunk `ptr` is a dangling
        // well-aligned pointer, which is valid for a zero-length slice.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for PinGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.meta.state.lock().expect("chunk state poisoned");
        state.pins -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Keep test pools small: 64 MiB of virtual reservation per class.
    fn test_pool(budget_bytes: usize) -> Pool {
        Pool::new(PoolConfig {
            budget_bytes,
            class_capacity_bytes: 64 << 20,
        })
        .expect("pool creation")
    }

    fn payload(words: usize, seed: u64) -> Vec<u64> {
        (0..u64::cast_from(words))
            .map(|i| seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(i))
            .collect()
    }

    /// Words that fill a 64 KiB class exactly.
    const SMALL: usize = (64 << 10) / 8;

    #[allow(dead_code)]
    fn assert_handle_send_sync() {
        fn check<T: Send + Sync>() {}
        check::<Pool>();
        check::<ChunkHandle>();
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn round_trip_resident() {
        let pool = test_pool(256 << 20);
        let orig = payload(1000, 1);
        let mut data = orig.clone();
        let capacity = data.capacity();
        let handle = pool.insert(&mut data);
        assert!(data.is_empty());
        assert_eq!(data.capacity(), capacity, "insert preserves capacity");
        assert_eq!(handle.len(), orig.len());
        assert_eq!(handle.len_bytes(), orig.len() * 8);
        assert_eq!(handle.residency(), Residency::UnbackedResident);
        {
            let pin = handle.pin();
            assert_eq!(&*pin, orig.as_slice());
        }
        let mut out = Vec::new();
        handle.take(&mut out);
        assert_eq!(out, orig);
        let stats = pool.stats();
        assert_eq!(stats.inserts, 1);
        assert_eq!(stats.frees, 1);
        assert_eq!(stats.resident_bytes, 0);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn evict_then_fault_preserves_contents() {
        let pool = test_pool(256 << 20);
        let orig = payload(SMALL, 2);
        let handle = pool.insert(&mut orig.clone());
        pool.evict(&handle);
        assert_eq!(handle.residency(), Residency::Evicted);
        let stats = pool.stats();
        assert_eq!(stats.evictions_compress, 1);
        assert_eq!(stats.resident_bytes, 0);
        assert!(stats.extent_bytes_written > 0);
        {
            let pin = handle.pin();
            assert_eq!(&*pin, orig.as_slice());
        }
        assert_eq!(handle.residency(), Residency::BackedResident);
        assert_eq!(pool.stats().faults, 1);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn poison_proves_fault_in_reads_extent() {
        let pool = test_pool(256 << 20);
        let orig = payload(SMALL, 3);
        let handle = pool.insert(&mut orig.clone());
        pool.evict(&handle);
        // On macOS `MADV_DONTNEED` may leave the old bytes resident; poison
        // the slot so a fault-in that read stale slot memory would fail.
        pool.poison_resident(&handle);
        let pin = handle.pin();
        assert_eq!(&*pin, orig.as_slice());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn re_eviction_of_backed_chunk_is_cheap() {
        let pool = test_pool(256 << 20);
        let orig = payload(SMALL, 4);
        let handle = pool.insert(&mut orig.clone());
        pool.evict(&handle);
        let written_after_first = pool.stats().extent_bytes_written;
        assert!(written_after_first > 0);
        {
            let pin = handle.pin();
            assert_eq!(&*pin, orig.as_slice());
        }
        pool.evict(&handle);
        let stats = pool.stats();
        assert_eq!(stats.evictions_compress, 1);
        assert_eq!(stats.evictions_cheap, 1);
        assert_eq!(stats.extent_bytes_written, written_after_first);
        let pin = handle.pin();
        assert_eq!(&*pin, orig.as_slice());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn pinned_chunks_are_never_evicted() {
        let pool = test_pool(0);
        let orig = payload(SMALL, 5);
        // Insert enforces the (zero) budget, evicting the chunk immediately.
        let handle = pool.insert(&mut orig.clone());
        assert_eq!(handle.residency(), Residency::Evicted);
        let pin = handle.pin();
        pool.enforce_budget();
        assert_eq!(handle.residency(), Residency::BackedResident);
        assert_eq!(&*pin, orig.as_slice());
        pool.evict(&handle);
        assert_eq!(handle.residency(), Residency::BackedResident);
        drop(pin);
        // The pin set the second-chance bit: enforcement clears it on the
        // first visit and evicts on the second.
        pool.enforce_budget();
        assert_eq!(handle.residency(), Residency::Evicted);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn addresses_are_stable_across_eviction() {
        let pool = test_pool(256 << 20);
        let handle = pool.insert(&mut payload(SMALL, 6));
        let before = {
            let pin = handle.pin();
            pin.as_ptr()
        };
        pool.evict(&handle);
        let after = {
            let pin = handle.pin();
            pin.as_ptr()
        };
        assert_eq!(before, after, "fault-in must not move the chunk");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn dead_data_is_never_written() {
        let pool = test_pool(256 << 20);
        let handle = pool.insert(&mut payload(SMALL, 7));
        drop(handle);
        let stats = pool.stats();
        assert_eq!(stats.frees, 1);
        assert_eq!(stats.elided_frees, 1);
        assert_eq!(stats.extent_bytes_written, 0);
        assert_eq!(stats.resident_bytes, 0);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn budget_is_enforced_on_insert() {
        let budget = 128 << 10;
        let pool = test_pool(budget);
        let mut handles = Vec::new();
        for seed in 0..8 {
            handles.push(pool.insert(&mut payload(SMALL, 100 + seed)));
        }
        let stats = pool.stats();
        assert!(
            stats.resident_bytes <= u64::cast_from(budget),
            "resident {} exceeds budget {}",
            stats.resident_bytes,
            budget,
        );
        assert!(stats.evictions_compress >= 6);
        let resident = handles
            .iter()
            .filter(|h| {
                matches!(
                    h.residency(),
                    Residency::UnbackedResident | Residency::BackedResident
                )
            })
            .count();
        assert_eq!(resident, 2, "budget holds exactly two small chunks");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn second_chance_prefers_untouched_victims() {
        // Budget holds one and a half small chunks.
        let pool = test_pool((64 << 10) + (32 << 10));
        let orig_a = payload(SMALL, 8);
        let handle_a = pool.insert(&mut orig_a.clone());
        {
            let pin = handle_a.pin();
            assert_eq!(&*pin, orig_a.as_slice());
        }
        // Inserting B overflows the budget; A is older but touched, so the
        // enforcer gives it a second chance and evicts untouched B instead.
        let handle_b = pool.insert(&mut payload(SMALL, 9));
        assert_eq!(handle_a.residency(), Residency::UnbackedResident);
        assert_eq!(handle_b.residency(), Residency::Evicted);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn empty_insert_consumes_no_slot() {
        let pool = test_pool(256 << 20);
        let mut data = Vec::new();
        let handle = pool.insert(&mut data);
        assert_eq!(handle.len(), 0);
        assert!(handle.is_empty());
        assert_eq!(handle.size_class_bytes(), None);
        {
            let pin = handle.pin();
            assert!(pin.is_empty());
        }
        let mut out = vec![1u64, 2, 3];
        handle.take(&mut out);
        assert!(out.is_empty());
        let stats = pool.stats();
        assert_eq!(stats.resident_bytes, 0);
        assert_eq!(stats.elided_frees, 0);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn oversize_round_trips() {
        let pool = test_pool(256 << 20);
        let words = (2 << 20) / 8 + 1;
        let orig = payload(words, 10);
        let handle = pool.insert(&mut orig.clone());
        assert_eq!(handle.residency(), Residency::Oversize);
        assert_eq!(handle.size_class_bytes(), None);
        let stats = pool.stats();
        assert_eq!(stats.oversize_bytes, u64::cast_from(words * 8));
        // Explicit eviction and budget enforcement leave oversize chunks
        // resident.
        pool.evict(&handle);
        pool.enforce_budget();
        assert_eq!(handle.residency(), Residency::Oversize);
        {
            let pin = handle.pin();
            assert_eq!(&*pin, orig.as_slice());
        }
        let mut out = Vec::new();
        handle.take(&mut out);
        assert_eq!(out, orig);
        let stats = pool.stats();
        assert_eq!(stats.oversize_bytes, 0);
        assert_eq!(stats.resident_bytes, 0);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn payload_lands_in_smallest_fitting_class() {
        let pool = test_pool(256 << 20);
        let handle = pool.insert(&mut payload((100 << 10) / 8, 11));
        assert_eq!(handle.size_class_bytes(), Some(128 << 10));
        let exact = pool.insert(&mut payload(SMALL, 12));
        assert_eq!(exact.size_class_bytes(), Some(64 << 10));
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn prefetch_is_safe_in_all_states() {
        let pool = test_pool(256 << 20);
        let orig = payload(SMALL, 13);
        let handle = pool.insert(&mut orig.clone());
        handle.prefetch();
        pool.evict(&handle);
        handle.prefetch();
        let pin = handle.pin();
        assert_eq!(&*pin, orig.as_slice());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn multithreaded_smoke() {
        // Budget of one small chunk: four threads each holding one resident
        // chunk keep the pool over budget, so every insert's and fault-in's
        // enforcement pass selects victims owned by other threads, racing
        // cross-thread eviction against pin, fault-in, and free.
        let pool = test_pool(64 << 10);
        let threads: Vec<_> = (0..4u64)
            .map(|t| {
                let pool = pool.clone();
                std::thread::spawn(move || {
                    for round in 0..50u64 {
                        let seed = t * 1000 + round;
                        let orig = payload(SMALL, seed);
                        let handle = pool.insert(&mut orig.clone());
                        pool.evict(&handle);
                        {
                            let pin = handle.pin();
                            assert_eq!(&*pin, orig.as_slice());
                            // Enforcement under a held pin must spare the
                            // pinned chunk and may evict everyone else's.
                            pool.enforce_budget();
                            assert_eq!(&*pin, orig.as_slice());
                        }
                        let mut out = Vec::new();
                        handle.take(&mut out);
                        assert_eq!(out, orig);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("worker thread panicked");
        }
        let stats = pool.stats();
        assert_eq!(stats.inserts, 200);
        assert_eq!(stats.frees, 200);
        assert_eq!(stats.resident_bytes, 0);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn concurrent_pin_enforce_churn() {
        // Races the three actors that can touch one chunk's slot: readers
        // pinning and verifying shared chunks, an enforcer evicting them
        // (the zero budget makes every unpinned chunk a victim), and a
        // churner whose insert/free traffic turns the queue over. Contents
        // are asserted on every pin, so an eviction or slot recycle racing a
        // fault-in shows up as corruption.
        let pool = test_pool(0);
        let shared: Arc<Vec<(Vec<u64>, ChunkHandle)>> = Arc::new(
            (0..4u64)
                .map(|seed| {
                    let orig = payload(SMALL, 600 + seed);
                    let handle = pool.insert(&mut orig.clone());
                    (orig, handle)
                })
                .collect(),
        );
        let mut threads = Vec::new();
        for t in 0..2u64 {
            let shared = Arc::clone(&shared);
            threads.push(std::thread::spawn(move || {
                for round in 0..300u64 {
                    let (orig, handle) = &shared[usize::cast_from((t + round) % 4)];
                    let pin = handle.pin();
                    assert_eq!(&*pin, orig.as_slice());
                }
            }));
        }
        {
            let pool = pool.clone();
            threads.push(std::thread::spawn(move || {
                for _ in 0..600 {
                    pool.enforce_budget();
                }
            }));
        }
        {
            let pool = pool.clone();
            threads.push(std::thread::spawn(move || {
                for round in 0..300u64 {
                    let orig = payload(SMALL, 700 + round);
                    let handle = pool.insert(&mut orig.clone());
                    let pin = handle.pin();
                    assert_eq!(&*pin, orig.as_slice());
                }
            }));
        }
        for thread in threads {
            thread.join().expect("worker thread panicked");
        }
        drop(shared);
        assert_eq!(pool.stats().resident_bytes, 0);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn budget_is_enforced_on_fault_in() {
        // Read-only traffic: every chunk starts evicted and is then pinned
        // once, with no inserts in between. Fault-in itself must enforce the
        // budget, or the working set would grow to the whole run.
        let budget = 128 << 10;
        let pool = test_pool(budget);
        let origs: Vec<_> = (0..8u64).map(|seed| payload(SMALL, 300 + seed)).collect();
        let handles: Vec<_> = origs.iter().map(|o| pool.insert(&mut o.clone())).collect();
        for handle in &handles {
            pool.evict(handle);
        }
        assert_eq!(pool.stats().resident_bytes, 0);
        for (index, handle) in handles.iter().enumerate() {
            {
                let pin = handle.pin();
                assert_eq!(&*pin, origs[index].as_slice());
            }
            let resident = pool.stats().resident_bytes;
            assert!(
                resident <= u64::cast_from(budget),
                "resident {resident} exceeds budget {budget} on the fault-in path",
            );
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn queue_stays_bounded_under_budget() {
        // Chunk churn that never exceeds the budget: the enforcer's eviction
        // loop never runs, so stale queue entries must be reclaimed by
        // pruning alone.
        let pool = test_pool(256 << 20);
        for seed in 0..1000u64 {
            let handle = pool.insert(&mut payload(SMALL, seed));
            drop(handle);
        }
        let len = pool.queue_len();
        assert!(len <= 32, "queue holds {len} entries for zero live chunks");
    }
}
