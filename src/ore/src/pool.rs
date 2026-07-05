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
//! slots hold resident chunks. Slots are scoped to residency — eviction
//! returns a chunk's slot to the free list along with its physical pages, and
//! fault-in allocates a fresh one — so slot demand tracks the resident set
//! (bounded by the budget), not the potentially unbounded live backlog, and
//! a chunk's address is stable only between a fault-in and the next eviction.
//! Pointers into a chunk are valid only under a [`PinGuard`], which blocks
//! eviction; nothing may cache a pointer across pins. The backing is the
//! swap-backed extent store of the design's Layer 1: a page-aligned anonymous
//! allocation holding the chunk's lz4-compressed bytes.
//!
//! Memory descends a ladder of tiers, each with its own ceiling and each
//! cheaper to vacate than the one above:
//!
//! * **Slots** (uncompressed, free reads) — bounded by the budget; crossing
//!   it compresses the oldest chunks into extents and releases their slots.
//! * **Warm free slots** (pages kept for fault-free reuse) — bounded by the
//!   warm cap.
//! * **Compressed-resident extents** (reads decompress, no device) — bounded
//!   by the headroom the RSS target leaves above the first two; crossing it
//!   pushes the oldest extents to the swap device with `MADV_PAGEOUT`.
//! * **The swap device** — overflow; reads fault and decompress.
//!
//! The identity `total pool RSS <= budget + warm cap + compressed cap`,
//! where `compressed cap = rss_target - budget - warm cap`, makes every
//! resident byte's ceiling nameable; a zero RSS target collapses the
//! compressed tier and extents page out as soon as they are written.
//! Nothing is ever both uncompressed and on the device.
//!
//! Residency is a state, not a type. Fault-in is synchronous on the pinning
//! caller (the design's `Faulting` transition collapses into the call), while
//! eviction I/O runs on spill threads when enabled — `WriteInFlight` marks a
//! chunk whose compression a spill thread owns — and inline on the evicting
//! caller otherwise. Chunks are immutable after [`Pool::insert`], which is
//! what makes a `BackedResident` slot always identical to its extent and
//! re-eviction free of I/O.
//!
//! Freeing an `UnbackedResident` chunk is a pure memory operation — the
//! design's "never write dead data" win, surfaced as `writes_elided` in
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

/// Construction-time configuration for a [`Pool`]: what cannot change once
/// the virtual reservations are mapped. Everything tunable at runtime —
/// budget, RSS target, spill threads, eager backing — goes through setters
/// on the live pool instead.
#[derive(Debug, Clone, Copy)]
pub struct PoolConfig {
    /// Virtual reservation per size class. Purely virtual: physical memory
    /// materializes only for slots in use, and slots are scoped to residency,
    /// so this must exceed the largest plausible *resident* set per class —
    /// the budget plus pinned and in-flight slack, not the backlog. The
    /// default is deliberately enormous (address space costs nothing, and
    /// touched pages are bounded by peak residency) so that no realistic
    /// budget, on any machine size, reaches the heap-fallback path.
    pub class_capacity_bytes: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            class_capacity_bytes: 1 << 40,
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
    /// Resident and readable, with compression into an extent scheduled on a
    /// spill thread. Completion moves the chunk to [`Residency::Evicted`]
    /// (or [`Residency::BackedResident`] if pins appeared meanwhile); a free
    /// or a pin observed at dequeue cancels the write instead.
    WriteInFlight,
    /// Extent copy only; the chunk holds no slot. The extent itself may
    /// still be RAM-resident (the compressed tier) or paged out to the swap
    /// device. Access faults the chunk back in from the extent into a
    /// freshly allocated slot, so its address may differ from its last
    /// residence.
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
    /// Backing writes elided: chunks freed while `UnbackedResident`, dead
    /// before any compression or extent write happened.
    pub writes_elided: u64,
    /// Evictions that compressed the chunk into a new extent.
    pub evictions_compress: u64,
    /// Evictions of `BackedResident` chunks: pure page release, no I/O.
    pub evictions_cheap: u64,
    /// Fault-ins of `Evicted` chunks.
    pub faults: u64,
    /// Compressed bytes written into extents.
    pub extent_bytes_written: u64,
    /// Evictions handed to spill threads.
    pub spill_scheduled: u64,
    /// Scheduled evictions cancelled before compressing (freed or pinned at
    /// dequeue).
    pub spill_cancelled: u64,
    /// Entries currently queued for or being processed by spill threads.
    pub spill_in_flight: u64,
    /// Inserts that fell back to the heap because their size class had no
    /// free slot (the live set outgrew the class reservation). Heap-backed
    /// chunks behave like oversize ones: always resident, never paged.
    pub slot_exhausted_fallbacks: u64,
    /// Live size-classed chunks across all classes, whatever their residency.
    /// For backlog-shaped consumers this tracks the un-drained backlog in
    /// chunks. (Slots are scoped to residency, so the quantity that exhausts
    /// a class reservation is the resident subset, bounded by the budget.)
    pub live_chunks: u64,
    /// Uncompressed bytes of currently resident chunks (including oversize).
    pub resident_bytes: u64,
    /// Uncompressed bytes of live oversize chunks.
    pub oversize_bytes: u64,
    /// Class bytes of free slots currently kept warm (pages resident for
    /// fault-free reuse). Bounded by a fraction of the budget; RSS exceeds
    /// `resident_bytes` by up to this amount.
    pub warm_bytes: u64,
    /// Slot allocations served from the warm list: reuses that faulted no
    /// pages and skipped the kernel's page zeroing.
    pub warm_reuses: u64,
    /// Chunks eagerly compressed to `BackedResident` by idle spill threads
    /// (write-behind): still readable in their slots, with eviction
    /// pre-paid.
    pub eager_backs: u64,
    /// Allocation bytes of compressed extents currently resident — the
    /// compressed-but-resident middle tier. Bounded by the RSS target;
    /// exceeding it pages the oldest extents out to the swap device.
    pub extent_resident_bytes: u64,
    /// Extents pushed to the swap device by RSS-target enforcement.
    pub extent_pageouts: u64,
}

#[derive(Debug, Default)]
struct Counters {
    inserts: AtomicU64,
    spill_scheduled: AtomicU64,
    spill_cancelled: AtomicU64,
    slot_exhausted_fallbacks: AtomicU64,
    frees: AtomicU64,
    writes_elided: AtomicU64,
    evictions_compress: AtomicU64,
    evictions_cheap: AtomicU64,
    faults: AtomicU64,
    extent_bytes_written: AtomicU64,
    resident_bytes: AtomicU64,
    oversize_bytes: AtomicU64,
    warm_bytes: AtomicU64,
    warm_reuses: AtomicU64,
    eager_backs: AtomicU64,
    extent_resident_bytes: AtomicU64,
    extent_pageouts: AtomicU64,
}

/// A buffer pool over swap-backed extents. Cheap to clone; all clones share
/// one budget and one backing store.
#[derive(Debug, Clone)]
pub struct Pool(Arc<PoolInner>);

/// Lock order: a chunk's `state` mutex may be held while taking any of the
/// leaf locks — the eviction `queue`, the `extent_queue`, the spill queue,
/// and the region slot allocators — but never the reverse. The enforcement
/// and backing scans additionally drop the queue guard before trying a
/// chunk's state lock (and only ever `try_lock` it), so no path holds a
/// queue lock while waiting on chunk state.
#[derive(Debug)]
struct PoolInner {
    /// Resident-bytes target. Atomic so a running pool can be retuned in
    /// place (operator-driven budget changes) without orphaning live
    /// handles, which share this value through their `Arc<PoolInner>`.
    budget_bytes: AtomicU64,
    /// Ceiling on the pool's *total* RSS: slots (the budget) plus warm free
    /// slots plus compressed-resident extents. The compressed tier's
    /// capacity derives as `rss_target - budget - warm cap`; zero (the
    /// default) collapses the tier, paging every extent out as soon as it
    /// is written.
    rss_target_bytes: AtomicU64,
    /// One region per entry of [`SIZE_CLASSES`], same order.
    regions: Vec<Region>,
    /// Second-chance FIFO of eviction candidates. Entries for freed chunks
    /// go stale in place and are dropped by [`PoolInner::prune_queue`].
    ///
    /// Two scanners walk it with different obligations. Budget enforcement
    /// is the one that ages chunks: it spends the touched bit (second
    /// chance) and drops entries it evicts. Eager backing
    /// ([`PoolInner::back_one`]) rotates visited entries to the back but
    /// never spends a touched bit, so a backing pass shuffles FIFO order
    /// without aging any chunk toward eviction.
    queue: Mutex<VecDeque<Weak<ChunkMeta>>>,
    /// FIFO of chunks whose extents are resident, oldest first — the
    /// RSS-target enforcement's victim queue. Entries go stale when an
    /// extent pages out, is dropped, or its chunk dies; visits drop them.
    extent_queue: Mutex<VecDeque<Weak<ChunkMeta>>>,
    /// Number of live size-classed chunks (whatever their residency), which
    /// is the number of non-stale queue entries; [`PoolInner::prune_queue`]
    /// compacts the queue against it.
    live_chunks: AtomicU64,
    /// Single-flight claim for budget enforcement.
    enforcing: Mutex<()>,
    counters: Counters,
    spill: Spill,
}

/// Hand-off point between budget enforcement and spill threads. Eviction I/O
/// (compression and the synchronous-reclaim `pageout`) runs on spill threads
/// when enabled, keeping multi-millisecond work off the threads that trip the
/// budget; with no spill threads, eviction runs inline on the caller.
#[derive(Debug, Default)]
struct Spill {
    /// Chunks in `WriteInFlight`, awaiting a spill thread.
    queue: Mutex<VecDeque<Arc<ChunkMeta>>>,
    cv: std::sync::Condvar,
    /// Whether evictions are handed to spill threads. Set when threads are
    /// first spawned; cleared to fall back to inline eviction.
    enabled: std::sync::atomic::AtomicBool,
    /// Whether idle spill threads eagerly compress unbacked chunks to
    /// `BackedResident` (write-behind): the chunk stays readable in its slot
    /// while a compressed extent accumulates on the swap device, so a later
    /// budget-driven eviction is a pure page release instead of a
    /// compression. Costs CPU on chunks that die before eviction would have
    /// reached them; pays at every pressure event.
    eager: std::sync::atomic::AtomicBool,
    /// Number of spill threads spawned (spawn-once; later config changes
    /// only toggle `enabled`).
    threads: AtomicU64,
    /// Queued plus currently-processing entries; `quiesce` waits on zero.
    in_flight: AtomicU64,
}

/// Beyond this many queued spill entries, eviction degrades to inline on the
/// caller: bounded memory overshoot under burst beats an unbounded queue of
/// still-resident chunks.
const SPILL_QUEUE_MAX: usize = 64;

/// What a spill thread does with a chunk once compressed.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SpillKind {
    /// Budget-driven: release the slot, leaving the chunk `Evicted`. Pins
    /// observed at dequeue cancel the work — a chunk being read is
    /// demonstrably hot and should not be evicted.
    Evict,
    /// Eager write-behind: keep the slot, leaving the chunk
    /// `BackedResident`. Pins are irrelevant — concurrent reads of the
    /// immutable slot coexist with compression, and the slot stays put.
    Back,
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
    /// Size class for slot allocations; `None` for empty chunks and payloads
    /// beyond the largest class. Immutable: the chunk's *slot* comes and goes
    /// with residency, but it is always drawn from this class.
    class: Option<usize>,
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
    /// Whether the chunk currently has an entry in the eviction queue. The
    /// queue holds resident chunks only: entries are dropped when a visit
    /// finds the chunk evicted, and fault-in re-enqueues. The flag is queue
    /// hygiene, not a safety invariant — duplicate entries would be benign
    /// (visits are idempotent); it exists so fault-hot chunks cannot grow
    /// the queue without bound between enforcement passes.
    queued: bool,
    /// The chunk's slot, held exactly while the chunk occupies pool memory
    /// (the resident states and `WriteInFlight`). Eviction returns the slot
    /// to the region free list; fault-in allocates a fresh one, so a chunk's
    /// address is stable only between a fault-in and the next eviction.
    /// Pointers into the slot are valid only under a pin, which blocks
    /// eviction.
    slot: Option<Slot>,
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
/// chunk: the slot (if resident) returns to the region free list with its
/// physical pages released, and the extent (if any) is deallocated,
/// discarding any swapped copy for free. Releasing the pages keeps RSS
/// aligned with the `resident_bytes` gauge the budget enforcer trusts;
/// without it, freed slots would hold warm pages the enforcer cannot see.
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
    /// Creates a pool, reserving one virtual region per size class. The
    /// pool starts with an unlimited budget — nothing is evicted until
    /// [`Pool::set_budget`] tunes it.
    pub fn new(cfg: PoolConfig) -> std::io::Result<Pool> {
        let regions = SIZE_CLASSES
            .iter()
            .map(|&class_size| Region::new(class_size, cfg.class_capacity_bytes))
            .collect::<std::io::Result<Vec<_>>>()?;
        Ok(Pool(Arc::new(PoolInner {
            budget_bytes: AtomicU64::new(u64::MAX),
            rss_target_bytes: AtomicU64::new(0),
            regions,
            queue: Mutex::new(VecDeque::new()),
            extent_queue: Mutex::new(VecDeque::new()),
            live_chunks: AtomicU64::new(0),
            enforcing: Mutex::new(()),
            counters: Counters::default(),
            spill: Spill::default(),
        })))
    }

    /// Copies `data` into a pool slot of the smallest class that fits and
    /// clears `data`, preserving its capacity. The returned handle starts
    /// `UnbackedResident`. Empty input returns a length-0 handle holding no
    /// slot; input larger than the largest class falls back to a plain heap
    /// allocation ([`Residency::Oversize`]), a prototype limitation.
    pub fn insert(&self, data: &mut Vec<u64>) -> ChunkHandle {
        let handle = self.insert_with(data.len(), |dst| dst.copy_from_slice(data.as_slice()));
        data.clear();
        handle
    }

    /// Allocates a chunk of `len` words and fills it in place: `fill`
    /// receives the chunk's slot memory directly and must overwrite all of
    /// it (the slot's prior contents are unspecified). Payloads beyond the
    /// largest size class fall back to a heap allocation, as in
    /// [`Pool::insert`].
    ///
    /// This is the zero-staging insert: serialization can write its single
    /// copy straight into pool memory, paying one page population instead of
    /// staging through caller-side buffers that fault their own pages and
    /// die immediately after.
    pub fn insert_with(&self, len: usize, fill: impl FnOnce(&mut [u64])) -> ChunkHandle {
        let inner = &self.0;
        inner.counters.inserts.fetch_add(1, Ordering::Relaxed);
        let len_bytes = len * std::mem::size_of::<u64>();
        if len == 0 {
            fill(&mut []);
            return ChunkHandle {
                meta: Arc::new(ChunkMeta {
                    pool: Arc::clone(inner),
                    len: 0,
                    class: None,
                    state: Mutex::new(ChunkState {
                        residency: Residency::UnbackedResident,
                        pins: 0,
                        touched: false,
                        freed: false,
                        queued: false,
                        slot: None,
                        extent: None,
                        oversize: None,
                    }),
                }),
            };
        }
        let class = SIZE_CLASSES.iter().position(|&c| c >= len_bytes);
        // A class with no free slot degrades to the heap path below: the
        // resident set outgrew the class reservation, and an unpageable chunk
        // beats a dead replica. Warn once; the fallback counter tracks scale.
        let slot = class.and_then(|class| inner.alloc_slot(class, len_bytes));
        // Whichever home the payload found, it is resident.
        inner
            .counters
            .resident_bytes
            .fetch_add(u64::cast_from(len_bytes), Ordering::Relaxed);
        let meta = match slot {
            Some(slot) => {
                let region = &inner.regions[slot.class];
                // SAFETY: the freshly allocated slot is at least `len_bytes`
                // long (the class fits the payload) and is exclusively owned
                // by this not-yet-shared chunk, so the mutable borrow is
                // unique; region memory is mapped and writable, and `u64` has
                // no validity requirements beyond size, so exposing the
                // unspecified prior contents through `&mut [u64]` is sound.
                let dst = unsafe {
                    std::slice::from_raw_parts_mut(region.slot_ptr(slot.index).cast::<u64>(), len)
                };
                fill(dst);
                ChunkMeta {
                    pool: Arc::clone(inner),
                    len,
                    class,
                    state: Mutex::new(ChunkState {
                        residency: Residency::UnbackedResident,
                        pins: 0,
                        touched: false,
                        freed: false,
                        queued: true,
                        slot: Some(slot),
                        extent: None,
                        oversize: None,
                    }),
                }
            }
            None => {
                let mut payload = vec![0u64; len];
                fill(&mut payload);
                inner
                    .counters
                    .oversize_bytes
                    .fetch_add(u64::cast_from(len_bytes), Ordering::Relaxed);
                ChunkMeta {
                    pool: Arc::clone(inner),
                    len,
                    class: None,
                    state: Mutex::new(ChunkState {
                        residency: Residency::Oversize,
                        pins: 0,
                        touched: false,
                        freed: false,
                        queued: false,
                        slot: None,
                        extent: None,
                        oversize: Some(payload),
                    }),
                }
            }
        };
        let meta = Arc::new(meta);
        if meta.class.is_some() {
            inner.live_chunks.fetch_add(1, Ordering::Relaxed);
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
            writes_elided: c.writes_elided.load(Ordering::Relaxed),
            evictions_compress: c.evictions_compress.load(Ordering::Relaxed),
            evictions_cheap: c.evictions_cheap.load(Ordering::Relaxed),
            faults: c.faults.load(Ordering::Relaxed),
            extent_bytes_written: c.extent_bytes_written.load(Ordering::Relaxed),
            resident_bytes: c.resident_bytes.load(Ordering::Relaxed),
            oversize_bytes: c.oversize_bytes.load(Ordering::Relaxed),
            warm_bytes: c.warm_bytes.load(Ordering::Relaxed),
            warm_reuses: c.warm_reuses.load(Ordering::Relaxed),
            eager_backs: c.eager_backs.load(Ordering::Relaxed),
            extent_resident_bytes: c.extent_resident_bytes.load(Ordering::Relaxed),
            extent_pageouts: c.extent_pageouts.load(Ordering::Relaxed),
            spill_scheduled: c.spill_scheduled.load(Ordering::Relaxed),
            spill_cancelled: c.spill_cancelled.load(Ordering::Relaxed),
            spill_in_flight: self.0.spill.in_flight.load(Ordering::Relaxed),
            slot_exhausted_fallbacks: c.slot_exhausted_fallbacks.load(Ordering::Relaxed),
            live_chunks: self.0.live_chunks.load(Ordering::Relaxed),
        }
    }

    /// Enables or disables off-worker eviction I/O. The first call with
    /// `threads > 0` spawns that many spill threads (spawn-once: later calls
    /// only toggle participation); `threads == 0` falls back to inline
    /// eviction on the caller for subsequent victims, letting any queued
    /// work drain.
    pub fn set_spill_threads(&self, threads: usize) {
        if threads == 0 {
            self.0.spill.enabled.store(false, Ordering::Relaxed);
            return;
        }
        let spawned = self.0.spill.threads.load(Ordering::Relaxed);
        if spawned == 0 {
            let to_spawn = u64::cast_from(threads);
            if self
                .0
                .spill
                .threads
                .compare_exchange(0, to_spawn, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                for i in 0..threads {
                    let inner = Arc::clone(&self.0);
                    std::thread::Builder::new()
                        .name(format!("pool-spill-{i}"))
                        .spawn(move || inner.spill_worker())
                        .expect("spawn pool spill thread");
                }
            }
        }
        self.0.spill.enabled.store(true, Ordering::Relaxed);
    }

    /// Enables or disables eager backing: when on, idle spill threads
    /// compress unbacked chunks to `BackedResident` ahead of pressure, so
    /// budget-driven eviction becomes a pure page release. Only meaningful
    /// with spill threads spawned.
    pub fn set_eager_backing(&self, eager: bool) {
        self.0.spill.eager.store(eager, Ordering::Relaxed);
        if eager {
            self.0.spill.cv.notify_all();
        }
    }

    /// Test hook: performs one eager-backing step on the calling thread.
    /// Returns whether progress was made.
    #[cfg(test)]
    fn back_step(&self) -> bool {
        self.0.back_one()
    }

    /// Test hook: waits until the spill queue is empty and no entry is being
    /// processed, so tests observe deterministic post-eviction states.
    #[cfg(test)]
    fn quiesce_spill(&self) {
        while self.0.spill.in_flight.load(Ordering::Relaxed) > 0 {
            std::thread::yield_now();
        }
    }

    /// Test hook: enables spill scheduling without spawning threads, so tests
    /// drive the queue deterministically via [`Pool::spill_step`].
    #[cfg(test)]
    fn enable_spill_without_threads(&self) {
        self.0.spill.enabled.store(true, Ordering::Relaxed);
    }

    /// Test hook: processes one queued spill entry on the calling thread.
    /// Returns whether an entry was processed.
    #[cfg(test)]
    fn spill_step(&self) -> bool {
        let popped = self
            .0
            .spill
            .queue
            .lock()
            .expect("spill queue poisoned")
            .pop_front();
        let Some(meta) = popped else {
            return false;
        };
        self.0.spill_process(&meta, SpillKind::Evict);
        self.0.spill.in_flight.fetch_sub(1, Ordering::Relaxed);
        true
    }

    /// Evicts cold chunks until resident bytes fall to the budget or every
    /// queued chunk has been visited once. Runs automatically on every insert
    /// and fault-in; explicit calls are for tests and pressure hooks.
    pub fn enforce_budget(&self) {
        self.0.enforce_budget();
    }

    /// Retunes the resident-bytes budget in place and enforces it. Live
    /// handles share the new value immediately through their `Arc<PoolInner>`;
    /// a shrink takes effect by evicting on this call, a grow simply leaves
    /// more headroom for future inserts and fault-ins.
    pub fn set_budget(&self, budget_bytes: usize) {
        let new = u64::cast_from(budget_bytes);
        let prev = self.0.budget_bytes.swap(new, Ordering::Relaxed);
        // Config application calls this per worker per tick; only a change
        // warrants an enforcement pass (a grow needs none, and inserts and
        // fault-ins enforce continuously anyway).
        if new < prev {
            self.0.enforce_budget();
        }
    }

    /// Retunes the ceiling on the pool's total RSS — slots plus warm slots
    /// plus compressed-resident extents. The compressed tier's capacity is
    /// the gap above the budget and warm cap; zero (the default) collapses
    /// the tier, paging extents out as soon as they are written. A shrink
    /// takes effect by paging out the oldest extents on this call.
    pub fn set_rss_target(&self, target_bytes: usize) {
        let new = u64::cast_from(target_bytes);
        let prev = self.0.rss_target_bytes.swap(new, Ordering::Relaxed);
        if new < prev {
            self.0.enforce_compressed_cap();
        }
    }

    /// Test-only: the number of entries in the second-chance queue, live and
    /// stale.
    #[cfg(test)]
    fn queue_len(&self) -> usize {
        self.0.queue.lock().expect("pool queue poisoned").len()
    }

    /// Explicitly evicts one chunk. No-op if the chunk is pinned, already
    /// evicted, in flight, empty, or oversize. With spill threads enabled the
    /// compression is handed off and completes asynchronously (observable via
    /// [`Residency::WriteInFlight`]); without them it runs inline.
    pub fn evict(&self, handle: &ChunkHandle) {
        let meta = &handle.meta;
        let mut state = meta.state.lock().expect("chunk state poisoned");
        if !meta.pool.spill_handoff(meta, &mut state) {
            meta.pool.evict_locked(meta, &mut state);
        }
        drop(state);
        meta.pool.enforce_or_defer_compressed_cap();
    }

    /// Test hook: overwrites every free slot's bytes with `0xDE`. The free
    /// list can hand a faulting chunk the very slot it occupied before
    /// eviction, still holding its old bytes on platforms where
    /// `MADV_DONTNEED` keeps contents (macOS); poisoning lets tests prove
    /// that fault-in decompresses from the extent rather than passing stale
    /// slot memory through.
    #[doc(hidden)]
    pub fn poison_free_slots(&self) {
        for region in &self.0.regions {
            region.poison_free_slots();
        }
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
        let live = usize::cast_from(self.live_chunks.load(Ordering::Relaxed));
        let mut queue = self.queue.lock().expect("pool queue poisoned");
        if queue.len() > 2 * live + 16 {
            queue.retain(|weak| weak.strong_count() > 0);
        }
    }

    fn enforce_budget(&self) {
        // Single-flight: enforcement runs synchronously on whichever thread
        // trips it (every insert and fault-in), and concurrent passes would
        // convoy on the queue mutex doing redundant scans of the same
        // candidates. One pass at a time reaches the budget just as well;
        // skipped callers rely on the in-progress pass. A poisoned claim
        // means a prior pass panicked; recover and keep enforcing rather
        // than silently disabling the budget for the process's lifetime.
        let guard = match self.enforcing.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => return,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
        };
        self.enforce_budget_inner();
        drop(guard);
        // Inline evictions above may have grown the compressed tier.
        self.enforce_or_defer_compressed_cap();
    }

    fn enforce_budget_inner(&self) {
        self.prune_queue();
        let resident = |counters: &Counters| counters.resident_bytes.load(Ordering::Relaxed);
        // The queue holds resident chunks only (evicted chunks leave it and
        // fault-in re-enqueues), so a full pass is proportional to the
        // resident set. Visit each queued chunk at most twice per call: a
        // first visit may only clear the second-chance bit, so a second is
        // needed before an over-budget call is guaranteed to evict every
        // unpinned chunk it saw. The bound keeps a queue of pinned chunks
        // from spinning this loop forever.
        let mut remaining = self
            .queue
            .lock()
            .expect("pool queue poisoned")
            .len()
            .saturating_mul(2);
        while remaining > 0 && resident(&self.counters) > self.budget_bytes.load(Ordering::Relaxed)
        {
            remaining -= 1;
            let popped = self.queue.lock().expect("pool queue poisoned").pop_front();
            let Some(weak) = popped else {
                break;
            };
            let Some(meta) = weak.upgrade() else {
                continue;
            };
            let requeue = {
                // `try_lock`: a chunk mid-eviction or mid-fault holds its
                // lock for milliseconds; skipping it beats convoying every
                // budget enforcer in the process behind one chunk's I/O.
                let Ok(mut state) = meta.state.try_lock() else {
                    self.queue
                        .lock()
                        .expect("pool queue poisoned")
                        .push_back(weak);
                    continue;
                };
                if state.freed {
                    state.queued = false;
                    false
                } else if matches!(state.residency, Residency::Evicted | Residency::Oversize) {
                    // Nothing to evict: drop the entry. A fault-in re-enqueues
                    // the chunk, so the queue stays proportional to the
                    // resident set rather than accumulating every chunk ever
                    // evicted.
                    state.queued = false;
                    false
                } else if state.pins > 0 {
                    true
                } else if state.touched {
                    state.touched = false;
                    true
                } else if self.spill_handoff(&meta, &mut state) {
                    // Stays queued while in flight; once the spill commits to
                    // `Evicted`, the next visit drops the entry.
                    true
                } else {
                    self.evict_locked(&meta, &mut state);
                    if state.residency == Residency::Evicted {
                        state.queued = false;
                        false
                    } else {
                        true
                    }
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

    fn evict_locked(&self, meta: &Arc<ChunkMeta>, state: &mut ChunkState) {
        let Some(slot) = state.slot else {
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
                self.note_extent_resident(meta, extent.alloc_size());
                state.extent = Some(extent);
            }
            Residency::BackedResident => {
                self.counters
                    .evictions_cheap
                    .fetch_add(1, Ordering::Relaxed);
            }
            Residency::WriteInFlight | Residency::Evicted | Residency::Oversize => return,
        }
        // `release_slot`'s precondition holds: `pins == 0` and `!freed`,
        // checked above under the held state lock.
        self.release_slot(meta, state);
        state.residency = Residency::Evicted;
    }

    /// Records and (once) warns about a size-class slot exhaustion forcing a
    /// heap fallback.
    fn note_slot_exhausted(&self, len_bytes: usize) {
        self.counters
            .slot_exhausted_fallbacks
            .fetch_add(1, Ordering::Relaxed);
        static EXHAUSTED_ONCE: std::sync::Once = std::sync::Once::new();
        EXHAUSTED_ONCE.call_once(|| {
            tracing::warn!(
                len_bytes,
                "buffer pool size class exhausted; falling back to heap chunks \
                 (raise PoolConfig::class_capacity_bytes)",
            );
        });
    }

    /// Whether the next eviction should be handed to spill threads: enabled,
    /// and the queue is below the backpressure bound (beyond it, callers
    /// evict inline rather than growing an unbounded queue of still-resident
    /// chunks).
    fn spill_eligible(&self) -> bool {
        self.spill.enabled.load(Ordering::Relaxed)
            && usize::cast_from(self.spill.in_flight.load(Ordering::Relaxed)) < SPILL_QUEUE_MAX
    }

    /// Hands a `WriteInFlight` chunk to the spill threads.
    fn spill_schedule(&self, meta: Arc<ChunkMeta>) {
        self.counters
            .spill_scheduled
            .fetch_add(1, Ordering::Relaxed);
        self.spill.in_flight.fetch_add(1, Ordering::Relaxed);
        self.spill
            .queue
            .lock()
            .expect("spill queue poisoned")
            .push_back(meta);
        self.spill.cv.notify_one();
    }

    /// Spill-thread main loop. The thread owns an `Arc<PoolInner>`, so the
    /// pool (a process-wide singleton in production) lives as long as its
    /// threads. Queued (budget-driven) evictions take priority; with eager
    /// backing enabled, idle threads compress unbacked chunks to
    /// `BackedResident` instead of parking, and park with a timeout once
    /// everything reachable is backed.
    fn spill_worker(self: Arc<Self>) {
        loop {
            // Tier-2 pageouts ride the spill threads: every pass through the
            // loop (job completion, condvar wakeup, park timeout) trims the
            // compressed tier if needed. A single atomic load when under cap.
            self.enforce_compressed_cap();
            let popped = self
                .spill
                .queue
                .lock()
                .expect("spill queue poisoned")
                .pop_front();
            if let Some(meta) = popped {
                self.spill_process(&meta, SpillKind::Evict);
                self.spill.in_flight.fetch_sub(1, Ordering::Relaxed);
                continue;
            }
            if self.spill.eager.load(Ordering::Relaxed) && self.back_one() {
                continue;
            }
            // Nothing to evict or back: park. Re-checking emptiness under
            // the queue lock closes the lost-wakeup window (hand-offs push
            // under this lock before notifying); the timeout backstops
            // everything else (fresh inserts, tier growth, lost notifies).
            let queue = self.spill.queue.lock().expect("spill queue poisoned");
            if queue.is_empty() {
                let _ = self
                    .spill
                    .cv
                    .wait_timeout(queue, std::time::Duration::from_millis(100))
                    .expect("spill queue poisoned");
            }
        }
    }

    /// Eagerly compresses one unbacked chunk from the eviction queue into
    /// `BackedResident`, returning whether a chunk was backed — `false`
    /// means nothing was actionable (queue empty, or a bounded scan found
    /// only already-backed, in-flight, contended, or stale entries) and the
    /// caller should park rather than rescan. Non-actionable entries are
    /// requeued or dropped per the same rules budget enforcement uses,
    /// except that the second-chance `touched` bit is left alone — backing
    /// is not an eviction and must not consume a chunk's reprieve.
    fn back_one(&self) -> bool {
        for _ in 0..16 {
            let popped = self.queue.lock().expect("pool queue poisoned").pop_front();
            let Some(weak) = popped else {
                return false;
            };
            let Some(meta) = weak.upgrade() else {
                continue;
            };
            {
                let Ok(mut state) = meta.state.try_lock() else {
                    self.queue
                        .lock()
                        .expect("pool queue poisoned")
                        .push_back(weak);
                    continue;
                };
                if state.freed {
                    state.queued = false;
                    continue;
                }
                match state.residency {
                    Residency::Evicted | Residency::Oversize => {
                        state.queued = false;
                        continue;
                    }
                    Residency::UnbackedResident => {
                        state.residency = Residency::WriteInFlight;
                    }
                    Residency::BackedResident | Residency::WriteInFlight => {
                        self.queue
                            .lock()
                            .expect("pool queue poisoned")
                            .push_back(weak);
                        continue;
                    }
                }
            }
            self.spill.in_flight.fetch_add(1, Ordering::Relaxed);
            self.spill_process(&meta, SpillKind::Back);
            self.spill.in_flight.fetch_sub(1, Ordering::Relaxed);
            // The chunk remains an eviction candidate (now a cheap one).
            self.queue
                .lock()
                .expect("pool queue poisoned")
                .push_back(weak);
            return true;
        }
        false
    }

    /// Performs (or cancels) one scheduled compression. Lock discipline: the
    /// chunk lock is held only to validate and to commit — never across the
    /// compression or the `pageout` reclaim, which are the multi-millisecond
    /// costs this path exists to keep off budget-enforcing threads.
    fn spill_process(&self, meta: &Arc<ChunkMeta>, kind: SpillKind) {
        // Validate under the lock, then release it for the I/O. The slot is
        // captured under the lock and remains owned by this chunk for the
        // unlocked compression: in `WriteInFlight`, eviction skips the chunk
        // and `ChunkHandle::drop` defers slot release to this thread.
        let slot;
        {
            let mut state = meta.state.lock().expect("chunk state poisoned");
            if state.freed {
                // Freed while queued: the deferred cleanup is ours, and the
                // chunk dies without ever compressing — the write-behind
                // cancellation window. `ChunkHandle::drop` already counted
                // the free and the live-chunks decrement.
                self.counters
                    .spill_cancelled
                    .fetch_add(1, Ordering::Relaxed);
                self.counters.writes_elided.fetch_add(1, Ordering::Relaxed);
                self.release_slot(meta, &mut state);
                return;
            }
            if state.residency != Residency::WriteInFlight {
                return;
            }
            if kind == SpillKind::Evict && state.pins > 0 {
                // Being read: cancel rather than compress data that is
                // demonstrably hot. The chunk stays in the second-chance
                // queue and a later pass reconsiders it. (Backing proceeds
                // pinned: reads of the immutable slot coexist with
                // compression, and the slot is staying put anyway.)
                state.residency = Residency::UnbackedResident;
                self.counters
                    .spill_cancelled
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            slot = state.slot.expect("write-in-flight chunk has a slot");
        }
        let region = &self.regions[slot.class];
        // SAFETY: the chunk is live (the queue holds an `Arc`) and in
        // `WriteInFlight`, so the slot is not recycled (`ChunkHandle::drop`
        // defers slot release to this thread in that state) and its contents
        // are initialized and immutable; concurrent pins may read the slot
        // but nothing writes it. `len` fits the class.
        let data = unsafe {
            std::slice::from_raw_parts(
                region.slot_ptr(slot.index).cast_const().cast::<u64>(),
                meta.len,
            )
        };
        let extent = SwapExtent::write(data);
        // Commit under the lock.
        let mut state = meta.state.lock().expect("chunk state poisoned");
        if state.freed {
            // Freed during compression: the extent is garbage; cleanup is
            // ours as above. Compression ran, so this is not an elided free.
            self.counters
                .spill_cancelled
                .fetch_add(1, Ordering::Relaxed);
            self.release_slot(meta, &mut state);
            return;
        }
        self.counters
            .extent_bytes_written
            .fetch_add(u64::cast_from(extent.comp_len()), Ordering::Relaxed);
        self.note_extent_resident(meta, extent.alloc_size());
        state.extent = Some(extent);
        // The slot stays for write-behind (the chunk remains readable; the
        // extent makes a later budget eviction a pure page release) and for
        // chunks pinned during compression. Otherwise `release_slot`'s
        // precondition holds: `pins == 0` and `!freed`, both observed under
        // the held state lock.
        let keep_slot = kind == SpillKind::Back || state.pins > 0;
        match kind {
            SpillKind::Back => self.counters.eager_backs.fetch_add(1, Ordering::Relaxed),
            SpillKind::Evict => self
                .counters
                .evictions_compress
                .fetch_add(1, Ordering::Relaxed),
        };
        if keep_slot {
            state.residency = Residency::BackedResident;
        } else {
            self.release_slot(meta, &mut state);
            state.residency = Residency::Evicted;
        }
        drop(state);
        // Counted a fresh resident extent: the tier may need trimming. Kept
        // here (rather than relying on the spill loop alone) so the
        // threadless test hooks observe deterministic post-commit states.
        self.enforce_compressed_cap();
    }

    /// Releases `state`'s slot — slot returned to the region free list,
    /// physical pages discarded unless the slot joins the bounded warm pool —
    /// and decrements resident bytes. Releasing pages beyond the warm pool is
    /// what keeps RSS aligned with the `resident_bytes` gauge the budget
    /// enforcer trusts; the warm pool relaxes that alignment by an explicit,
    /// bounded amount (`warm_bytes`, capped at a fraction of the budget) so
    /// slot reuse faults no pages and skips the kernel's page zeroing. Slots
    /// are scoped to residency: fault-in allocates a fresh slot, so a chunk's
    /// address is stable only between a fault-in and the next eviction.
    ///
    /// Precondition, established by every caller under the held state lock:
    /// no reference into the slot exists — either `pins == 0`, or the handle
    /// is gone (`freed` set) so no `PinGuard` can be created and none
    /// survives. This is what makes the `dontneed` below sound, and what
    /// makes keeping a warm slot's stale contents safe: the slot's next
    /// occupant fully overwrites every byte it reads, satisfying the
    /// contents-undefined contract either way.
    fn release_slot(&self, meta: &ChunkMeta, state: &mut ChunkState) {
        let slot = state.slot.take().expect("slotted chunk");
        let region = &self.regions[slot.class];
        let warm = self.try_keep_warm(region.class_size());
        if !warm {
            // SAFETY: no reference into the slot exists (the function-level
            // precondition, established under the held state lock).
            unsafe {
                region::dontneed(region.slot_ptr(slot.index), region.class_size());
            }
        }
        region.free(slot.index, warm);
        self.counters
            .resident_bytes
            .fetch_sub(u64::cast_from(meta.len_bytes()), Ordering::Relaxed);
    }

    /// The warm pool's byte ceiling: an eighth of the budget, clamped at an
    /// absolute maximum. The fraction sizes fault amortization at small
    /// budgets; the clamp keeps large budgets from parking gigabytes of idle
    /// warm slots no fault rate could justify.
    fn warm_cap(&self) -> u64 {
        (self.budget_bytes.load(Ordering::Relaxed) / 8).min(1 << 30)
    }

    /// Claims warm-pool capacity for a slot of `class_size` bytes, returning
    /// whether the slot may keep its pages. The RSS overshoot the warm pool
    /// introduces is bounded by [`PoolInner::warm_cap`] and visible as the
    /// `warm_bytes` stat.
    fn try_keep_warm(&self, class_size: usize) -> bool {
        let cap = self.warm_cap();
        let class_bytes = u64::cast_from(class_size);
        self.counters
            .warm_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                (cur + class_bytes <= cap).then_some(cur + class_bytes)
            })
            .is_ok()
    }

    /// Records a warm slot reuse: the allocation faulted no pages, and its
    /// bytes leave the warm pool.
    fn note_warm_reuse(&self, class_size: usize) {
        self.counters
            .warm_bytes
            .fetch_sub(u64::cast_from(class_size), Ordering::Relaxed);
        self.counters.warm_reuses.fetch_add(1, Ordering::Relaxed);
    }

    /// Allocates a slot in `class`, with the accounting both allocation
    /// sites (insert and fault-in) share: a warm allocation is noted as a
    /// reuse, an exhausted class as a heap fallback for a `len_bytes`
    /// payload. `None` means the caller must degrade to the heap.
    fn alloc_slot(&self, class: usize, len_bytes: usize) -> Option<Slot> {
        match self.regions[class].alloc() {
            Some((index, warm)) => {
                if warm {
                    self.note_warm_reuse(self.regions[class].class_size());
                }
                Some(Slot { class, index })
            }
            None => {
                self.note_slot_exhausted(len_bytes);
                None
            }
        }
    }

    /// Capacity of the compressed-resident tier: the RSS target's headroom
    /// above the slot budget and the warm cap. Zero when no target is set —
    /// extents then page out as soon as written, today's pre-tier behavior.
    fn compressed_cap(&self) -> u64 {
        let target = self.rss_target_bytes.load(Ordering::Relaxed);
        let floor = self
            .budget_bytes
            .load(Ordering::Relaxed)
            .saturating_add(self.warm_cap());
        target.saturating_sub(floor)
    }

    /// Counts a newly resident extent (written or faulted back in) against
    /// the compressed tier and enqueues its chunk for RSS-target
    /// enforcement. Callers hold the chunk's state lock with the extent
    /// present and resident, and follow up with
    /// [`PoolInner::enforce_compressed_cap`] once the lock is released.
    ///
    /// Invariant: `extent_resident_bytes` equals the sum of `alloc_size`
    /// over live chunks' extents whose `is_resident()` is true. This method
    /// and [`PoolInner::note_extent_released`] are the only adjusters; every
    /// flag flip pairs with one of them under the chunk's state lock.
    fn note_extent_resident(&self, meta: &Arc<ChunkMeta>, extent_alloc: usize) {
        self.counters
            .extent_resident_bytes
            .fetch_add(u64::cast_from(extent_alloc), Ordering::Relaxed);
        self.extent_queue
            .lock()
            .expect("extent queue poisoned")
            .push_back(Arc::downgrade(meta));
    }

    /// Uncounts a resident extent that is being dropped (chunk freed or
    /// degraded). Its queue entry goes stale and is dropped on visit.
    fn note_extent_released(&self, extent: &SwapExtent) {
        if extent.is_resident() {
            self.counters
                .extent_resident_bytes
                .fetch_sub(u64::cast_from(extent.alloc_size()), Ordering::Relaxed);
        }
    }

    /// Routes compressed-cap enforcement off latency-sensitive threads: with
    /// spill threads running, wakes one to perform the pageouts
    /// (`MADV_PAGEOUT` is synchronous reclaim — page-table walks, TLB
    /// shootdowns, writeback submission — bounded per compressed extent but
    /// not free at chunk rates); without them, enforces inline as the only
    /// option.
    ///
    /// The routing rule across the two ceilings: budget pressure goes
    /// through [`PoolInner::enforce_budget`], single-flighted because
    /// concurrent passes would convoy on redundant compression scans; tier
    /// pressure goes through this router, and the enforcement itself is
    /// deliberately *not* single-flighted, since concurrent passes pop
    /// disjoint victims and each visit is microseconds. Spill threads call
    /// [`PoolInner::enforce_compressed_cap`] directly (they *are* the
    /// deferral target), and [`Pool::set_rss_target`] enforces a shrink
    /// inline so config changes land synchronously, mirroring `set_budget`.
    ///
    /// Deferral makes the target eventually-enforced with bounded lag (a
    /// notify with every spill thread mid-job is absorbed; the next loop
    /// pass catches up). The backstop below turns that into a bound by
    /// construction: a caller finding the tier at double its capacity
    /// enforces inline regardless, so sustained creation can never outrun
    /// trimming by more than one capacity's worth.
    fn enforce_or_defer_compressed_cap(&self) {
        if self.spill_threads_running() {
            let resident = self.counters.extent_resident_bytes.load(Ordering::Relaxed);
            if resident > self.compressed_cap().saturating_mul(2) {
                self.enforce_compressed_cap();
            } else {
                self.spill.cv.notify_one();
            }
        } else {
            self.enforce_compressed_cap();
        }
    }

    /// Whether spill threads exist and participate — the deferral test.
    /// Distinct from [`PoolInner::spill_eligible`], which deliberately
    /// omits the thread-count check so the threadless test mode
    /// (`enable_spill_without_threads` + `spill_step`) can drive the queue.
    fn spill_threads_running(&self) -> bool {
        self.spill.enabled.load(Ordering::Relaxed) && self.spill.threads.load(Ordering::Relaxed) > 0
    }

    /// Pages out the oldest resident extents until the compressed tier falls
    /// to its capacity. The compression is already paid and the device write
    /// is the kernel's async writeback, so each pageout is one bounded
    /// madvise; spill threads run this between jobs, and other threads only
    /// when no spill threads exist (see
    /// [`PoolInner::enforce_or_defer_compressed_cap`]). Not single-flighted:
    /// concurrent passes pop disjoint victims. Visits are bounded by the
    /// queue's length at entry; stale entries (extent paged out, dropped, or
    /// chunk dead) are dropped.
    fn enforce_compressed_cap(&self) {
        let cap = self.compressed_cap();
        let resident = |c: &Counters| c.extent_resident_bytes.load(Ordering::Relaxed);
        // Under-cap is the common case: answer it with one atomic load and
        // no queue lock, so frequent callers (the spill loop) stay cheap.
        if resident(&self.counters) <= cap {
            return;
        }
        let mut remaining = self
            .extent_queue
            .lock()
            .expect("extent queue poisoned")
            .len();
        while remaining > 0 && resident(&self.counters) > cap {
            remaining -= 1;
            let popped = self
                .extent_queue
                .lock()
                .expect("extent queue poisoned")
                .pop_front();
            let Some(weak) = popped else {
                break;
            };
            let Some(meta) = weak.upgrade() else {
                continue;
            };
            // `try_lock`: a chunk mid-fault or mid-compression holds its lock
            // for milliseconds; requeue rather than convoy behind it.
            let Ok(mut state) = meta.state.try_lock() else {
                self.extent_queue
                    .lock()
                    .expect("extent queue poisoned")
                    .push_back(weak);
                continue;
            };
            match &mut state.extent {
                Some(extent) if extent.is_resident() => {
                    // Uncount before the flag flips.
                    self.note_extent_released(extent);
                    extent.pageout();
                    self.counters
                        .extent_pageouts
                        .fetch_add(1, Ordering::Relaxed);
                }
                // Paged out already, dropped, or the chunk degraded: the
                // entry is stale. A later resident event re-enqueues.
                _ => {}
            }
        }
    }

    /// If the chunk is an unpinned, live `UnbackedResident` and the spill
    /// threads have capacity, transitions it to `WriteInFlight` and hands it
    /// to them, returning `true`. The hand-off happens under the held state
    /// lock; the spill thread blocks on that lock only after this call
    /// returns and the caller releases it.
    fn spill_handoff(&self, meta: &Arc<ChunkMeta>, state: &mut ChunkState) -> bool {
        if state.residency != Residency::UnbackedResident
            || state.pins > 0
            || state.freed
            || !self.spill_eligible()
        {
            return false;
        }
        state.residency = Residency::WriteInFlight;
        self.spill_schedule(Arc::clone(meta));
        true
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
        // The empty chunk holds no slot and nothing to protect: hand out a
        // dangling-but-aligned pointer (valid for a zero-length slice)
        // without touching the lock or the pin count. `PinGuard::drop`
        // mirrors the skip.
        if meta.len == 0 {
            return PinGuard {
                meta,
                ptr: std::ptr::NonNull::<u64>::dangling().as_ptr().cast_const(),
                len: 0,
            };
        }
        let mut state = meta.state.lock().expect("chunk state poisoned");
        let mut faulted = false;
        let ptr = match state.residency {
            Residency::Oversize => {
                let payload = state.oversize.as_ref().expect("oversize chunk has payload");
                payload.as_ptr()
            }
            Residency::Evicted => {
                // Fault-in allocates a fresh slot: slots are scoped to
                // residency, so the chunk's address may differ from its last
                // residence. If the class is exhausted, decompress to the
                // heap instead and let the chunk live out its days as an
                // oversize-style resident — degraded, never dead.
                let class = meta.class.expect("evicted chunk has a size class");
                faulted = true;
                meta.pool.counters.faults.fetch_add(1, Ordering::Relaxed);
                // Whichever home the payload finds, it becomes resident.
                meta.pool
                    .counters
                    .resident_bytes
                    .fetch_add(u64::cast_from(meta.len_bytes()), Ordering::Relaxed);
                match meta.pool.alloc_slot(class, meta.len_bytes()) {
                    Some(slot) => {
                        let region = &meta.pool.regions[slot.class];
                        let slot_ptr = region.slot_ptr(slot.index);
                        // SAFETY: the freshly allocated slot is exclusively
                        // owned by this chunk under the held state lock, so
                        // no other reference into it exists; `len_bytes` fits
                        // the class.
                        let dst =
                            unsafe { std::slice::from_raw_parts_mut(slot_ptr, meta.len_bytes()) };
                        let extent = state.extent.as_mut().expect("evicted chunk has an extent");
                        // Reading faults the extent's pages back in; re-count
                        // it against the compressed tier and re-enqueue.
                        let was_resident = extent.is_resident();
                        extent.read_into(dst);
                        if !was_resident {
                            let alloc = extent.alloc_size();
                            meta.pool.note_extent_resident(&self.meta, alloc);
                        }
                        state.slot = Some(slot);
                        state.residency = Residency::BackedResident;
                        slot_ptr.cast_const().cast::<u64>()
                    }
                    None => {
                        let mut payload = vec![0u64; meta.len];
                        let dst: &mut [u8] = bytemuck::cast_slice_mut(payload.as_mut_slice());
                        let extent = state.extent.as_mut().expect("evicted chunk has an extent");
                        // Uncount before the read revives the resident flag;
                        // the extent is dropped immediately after.
                        meta.pool.note_extent_released(extent);
                        extent.read_into(dst);
                        state.extent = None;
                        let ptr = payload.as_ptr();
                        state.oversize = Some(payload);
                        state.residency = Residency::Oversize;
                        meta.pool
                            .counters
                            .oversize_bytes
                            .fetch_add(u64::cast_from(meta.len_bytes()), Ordering::Relaxed);
                        ptr
                    }
                }
            }
            Residency::UnbackedResident | Residency::BackedResident | Residency::WriteInFlight => {
                let slot = state.slot.expect("resident non-empty chunk has a slot");
                meta.pool.regions[slot.class]
                    .slot_ptr(slot.index)
                    .cast_const()
                    .cast::<u64>()
            }
        };
        state.touched = true;
        state.pins += 1;
        // A fault-in made the chunk resident again: re-enqueue it as an
        // eviction candidate (its entry was dropped when a queue visit found
        // it evicted). The flag dedups against entries still circulating.
        // (Queue locks are leaves and the push could happen under the state
        // lock; deferring it is stylistic. The *enforcement* below must wait
        // for the unlock, since the enforcer try-locks chunk states.)
        let enqueue = faulted && !state.queued && state.residency != Residency::Oversize;
        if enqueue {
            state.queued = true;
        }
        drop(state);
        if enqueue {
            meta.pool
                .queue
                .lock()
                .expect("pool queue poisoned")
                .push_back(Arc::downgrade(&self.meta));
        }
        // Enforce after releasing the state lock: the enforcer locks chunk
        // states itself, and the pin count already shields this chunk from
        // being chosen as a victim. The fault also revived the extent's
        // pages, so the compressed tier may need trimming too.
        if faulted {
            meta.pool.enforce_budget();
            meta.pool.enforce_or_defer_compressed_cap();
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

    /// Copies the whole contents into `dst` (cleared first), leaving the
    /// chunk's residency untouched: a resident slot is copied out directly,
    /// and an evicted extent decompresses straight into `dst` without
    /// allocating a slot. A read therefore never raises resident bytes,
    /// never converts the chunk's state, and hands out no reference into
    /// pool memory.
    ///
    /// The copy runs under the chunk's state lock, which is what makes the
    /// no-reference contract cheap: eviction takes the same lock, so there
    /// is no reader it could race, and no pin accounting is needed.
    pub fn read_into(&self, dst: &mut Vec<u64>) {
        dst.clear();
        let meta = &*self.meta;
        if meta.len == 0 {
            return;
        }
        let mut state = meta.state.lock().expect("chunk state poisoned");
        state.touched = true;
        let mut extent_revived = false;
        match state.residency {
            Residency::Oversize => {
                let payload = state.oversize.as_ref().expect("oversize chunk has payload");
                dst.extend_from_slice(payload);
            }
            Residency::Evicted => {
                dst.resize(meta.len, 0);
                let bytes: &mut [u8] = bytemuck::cast_slice_mut(dst.as_mut_slice());
                let extent = state.extent.as_mut().expect("evicted chunk has an extent");
                // Reading faults the extent's pages back in; re-count it
                // against the compressed tier.
                let was_resident = extent.is_resident();
                extent.read_into(bytes);
                if !was_resident {
                    let alloc = extent.alloc_size();
                    meta.pool.note_extent_resident(&self.meta, alloc);
                    extent_revived = true;
                }
            }
            Residency::UnbackedResident | Residency::BackedResident | Residency::WriteInFlight => {
                let slot = state.slot.expect("resident non-empty chunk has a slot");
                let region = &meta.pool.regions[slot.class];
                // SAFETY: the slot belongs to this chunk while the state lock
                // is held (eviction and free both take it), and `meta.len`
                // words fit the class by construction.
                let src = unsafe {
                    std::slice::from_raw_parts(
                        region.slot_ptr(slot.index).cast_const().cast::<u64>(),
                        meta.len,
                    )
                };
                dst.extend_from_slice(src);
            }
        }
        drop(state);
        // The read revived the extent's compressed pages; the tier may need
        // trimming. Enforcement locks chunk states itself, so it must run
        // after the unlock.
        if extent_revived {
            meta.pool.enforce_or_defer_compressed_cap();
        }
    }

    /// Copies the whole contents into `dst` (cleared first), then frees the
    /// chunk.
    pub fn take(self, dst: &mut Vec<u64>) {
        self.read_into(dst);
    }

    /// Test-only: the byte size of the chunk's size class, or `None` for
    /// empty and oversize chunks.
    #[cfg(test)]
    pub(crate) fn size_class_bytes(&self) -> Option<usize> {
        self.meta.class.map(|class| SIZE_CLASSES[class])
    }
}

impl Drop for ChunkHandle {
    fn drop(&mut self) {
        let pool = &self.meta.pool;
        let mut state = self.meta.state.lock().expect("chunk state poisoned");
        debug_assert_eq!(state.pins, 0, "chunk freed while pinned");
        pool.counters.frees.fetch_add(1, Ordering::Relaxed);
        state.freed = true;
        if self.meta.class.is_some() {
            pool.live_chunks.fetch_sub(1, Ordering::Relaxed);
        }
        let len_bytes = u64::cast_from(self.meta.len_bytes());
        // `release_slot`'s precondition holds in every arm below: the handle
        // is being dropped, so no `PinGuard` (which borrows the handle)
        // exists, and `freed` was set under the state lock held here, so
        // concurrent queue visitors skip the chunk.
        match state.residency {
            Residency::UnbackedResident => {
                if state.slot.is_some() {
                    pool.counters.writes_elided.fetch_add(1, Ordering::Relaxed);
                    pool.release_slot(&self.meta, &mut state);
                }
            }
            Residency::BackedResident => {
                pool.release_slot(&self.meta, &mut state);
                if let Some(extent) = &state.extent {
                    pool.note_extent_released(extent);
                }
                state.extent = None;
            }
            Residency::Evicted => {
                // Eviction already released the slot.
                debug_assert!(state.slot.is_none(), "evicted chunk holds no slot");
                if let Some(extent) = &state.extent {
                    pool.note_extent_released(extent);
                }
                state.extent = None;
            }
            Residency::WriteInFlight => {
                // A spill thread may be reading the slot to compress it.
                // `freed` (set above) tells it the chunk died; it owns the
                // slot release, the `resident_bytes` decrement, and the
                // cancellation accounting from here.
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
        // check the pin count under that lock and skip pinned chunks, so the
        // chunk's slot (or heap payload) cannot be released or relocated
        // while this guard lives, and chunks are immutable after insert, so
        // the pointee is initialized, valid, and unaliased by writers for the
        // guard's lifetime. For the empty chunk `ptr` is a dangling
        // well-aligned pointer, which is valid for a zero-length slice.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for PinGuard<'_> {
    fn drop(&mut self) {
        // Empty-chunk pins never took the lock or incremented the count.
        if self.len == 0 {
            return;
        }
        let mut state = self.meta.state.lock().expect("chunk state poisoned");
        state.pins -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Keep test pools small: 64 MiB of virtual reservation per class.
    fn test_pool(budget_bytes: usize) -> Pool {
        let pool = Pool::new(PoolConfig {
            class_capacity_bytes: 64 << 20,
        })
        .expect("pool creation");
        pool.set_budget(budget_bytes);
        pool
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

    /// With an RSS target set, evicted chunks keep their extents resident
    /// (the compressed tier); shrinking the target pages the oldest extents
    /// out; reads revive them and re-count them.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn compressed_tier_round_trip() {
        let pool = test_pool(256 << 20);
        pool.set_rss_target(1 << 30);
        let orig = payload(SMALL, 21);
        let handle = pool.insert(&mut orig.clone());
        pool.evict(&handle);
        assert_eq!(handle.residency(), Residency::Evicted);
        let stats = pool.stats();
        assert!(
            stats.extent_resident_bytes > 0,
            "under the target, the extent stays resident",
        );
        assert_eq!(stats.extent_pageouts, 0);

        // Shrinking the target to zero pages the extent out.
        pool.set_rss_target(0);
        let stats = pool.stats();
        assert_eq!(stats.extent_resident_bytes, 0, "tier collapsed");
        assert_eq!(stats.extent_pageouts, 1);

        // Reading revives the extent: contents round-trip, and with the
        // target restored the revived extent is counted again.
        pool.set_rss_target(1 << 30);
        pool.poison_free_slots();
        {
            let pin = handle.pin();
            assert_eq!(&*pin, orig.as_slice());
        }
        assert!(
            pool.stats().extent_resident_bytes > 0,
            "revived and counted"
        );

        // Dropping the handle uncounts the resident extent.
        drop(handle);
        assert_eq!(pool.stats().extent_resident_bytes, 0);
    }

    /// With no RSS target (the default), extents page out as soon as they
    /// are written — the pre-tier behavior.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn default_target_pages_extents_immediately() {
        let pool = test_pool(256 << 20);
        let handle = pool.insert(&mut payload(SMALL, 22));
        pool.evict(&handle);
        let stats = pool.stats();
        assert_eq!(stats.extent_resident_bytes, 0);
        assert_eq!(stats.extent_pageouts, 1);
    }

    /// Eager backing compresses a chunk to `BackedResident` while it stays
    /// readable in its slot; the later budget-driven eviction is a pure page
    /// release, and the contents round-trip through the extent.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn eager_backing_round_trip() {
        let pool = test_pool(256 << 20);
        let orig = payload(SMALL, 11);
        let handle = pool.insert(&mut orig.clone());
        assert_eq!(handle.residency(), Residency::UnbackedResident);

        assert!(pool.back_step(), "one chunk is backable");
        assert_eq!(handle.residency(), Residency::BackedResident);
        let stats = pool.stats();
        assert_eq!(stats.eager_backs, 1);
        assert_eq!(
            stats.evictions_compress, 0,
            "backing is not an eviction and compresses off the eviction counter",
        );
        assert!(stats.extent_bytes_written > 0);

        // Still readable without a fault path: the slot is resident.
        {
            let pin = handle.pin();
            assert_eq!(&*pin, orig.as_slice());
        }

        // The pre-paid eviction is cheap, and the extent round-trips.
        pool.evict(&handle);
        assert_eq!(handle.residency(), Residency::Evicted);
        assert_eq!(pool.stats().evictions_cheap, 1);
        pool.poison_free_slots();
        let pin = handle.pin();
        assert_eq!(&*pin, orig.as_slice());
    }

    /// Once everything reachable is backed, the backing scan reports no
    /// progress so spill threads park instead of rescanning a fully-backed
    /// queue forever.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn backing_reports_no_progress_when_all_backed() {
        let pool = test_pool(256 << 20);
        let _handle = pool.insert(&mut payload(SMALL, 31));
        assert!(pool.back_step(), "one unbacked chunk is actionable");
        assert!(
            !pool.back_step(),
            "a fully-backed queue is not progress; callers must park",
        );
        assert_eq!(pool.stats().eager_backs, 1);
    }

    /// Backing proceeds while the chunk is pinned: reads of the immutable
    /// slot coexist with compression, and the slot stays put.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn eager_backing_proceeds_pinned() {
        let pool = test_pool(256 << 20);
        let orig = payload(SMALL, 12);
        let handle = pool.insert(&mut orig.clone());
        let pin = handle.pin();
        assert!(pool.back_step());
        assert_eq!(handle.residency(), Residency::BackedResident);
        assert_eq!(&*pin, orig.as_slice());
        drop(pin);
        assert_eq!(pool.stats().eager_backs, 1);
    }

    /// Freeing under the warm cap parks the slot warm; the next insert of the
    /// same class reuses it fault-free and the accounting balances.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn warm_slot_reuse() {
        // Budget 8 MiB: warm cap = 1 MiB, so a 64 KiB slot fits warm.
        let pool = test_pool(8 << 20);
        let orig = payload(SMALL, 7);
        let handle = pool.insert(&mut orig.clone());
        drop(handle);
        let after_free = pool.stats();
        assert_eq!(after_free.warm_bytes, 64 << 10, "freed slot parks warm");
        assert_eq!(after_free.warm_reuses, 0);

        let handle = pool.insert(&mut orig.clone());
        let after_reuse = pool.stats();
        assert_eq!(after_reuse.warm_reuses, 1, "second insert reuses warm slot");
        assert_eq!(after_reuse.warm_bytes, 0, "reuse drains the warm pool");
        // Contents are correct despite the skipped page release.
        let pin = handle.pin();
        assert_eq!(&*pin, orig.as_slice());
    }

    /// The warm pool is capped at an eighth of the budget; frees beyond the
    /// cap release their pages and park cold.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn warm_pool_respects_cap() {
        // Budget 1 MiB: warm cap = 128 KiB = two 64 KiB slots.
        let pool = test_pool(1 << 20);
        let handles: Vec<_> = (0..4)
            .map(|seed| pool.insert(&mut payload(SMALL, seed)))
            .collect();
        drop(handles);
        let stats = pool.stats();
        assert_eq!(
            stats.warm_bytes,
            128 << 10,
            "warm pool stops at the budget/8 cap",
        );
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
        // The free list can hand fault-in the chunk's previous slot, and on
        // macOS `MADV_DONTNEED` may have left the old bytes in it; poison all
        // free slots so a fault-in passing stale memory through would fail.
        pool.poison_free_slots();
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

    /// Slots are scoped to residency: eviction releases the slot, so a
    /// capacity holding exactly one chunk can serve any number of chunks one
    /// at a time. (Addresses are deliberately NOT stable across evictions —
    /// pointers are valid only under a pin.)
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn eviction_releases_the_slot() {
        // One 64 KiB slot per class.
        let pool = Pool::new(PoolConfig {
            class_capacity_bytes: 64 << 10,
        })
        .expect("pool creation");
        let a = pool.insert(&mut payload(SMALL, 6));
        pool.evict(&a);
        // The class's only slot is free again: a second chunk fits without
        // falling back to the heap.
        let b = pool.insert(&mut payload(SMALL, 7));
        assert_eq!(b.residency(), Residency::UnbackedResident);
        assert_eq!(pool.stats().slot_exhausted_fallbacks, 0);
        // Faulting `a` back in needs the slot `b` now holds: evict `b` first,
        // then both round-trip through their extents.
        pool.evict(&b);
        assert_eq!(&*a.pin(), &payload(SMALL, 6)[..]);
        drop(a);
        assert_eq!(&*b.pin(), &payload(SMALL, 7)[..]);
    }

    /// The eviction queue holds resident chunks only: an enforcement pass
    /// drops entries for evicted chunks, and fault-in re-enqueues, so the
    /// scan each insert pays stays proportional to the resident set rather
    /// than every chunk ever evicted.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn queue_holds_resident_chunks_only() {
        let pool = test_pool(128 << 10);
        let mut handles = Vec::new();
        for seed in 0..8 {
            handles.push(pool.insert(&mut payload(SMALL, 800 + seed)));
        }
        // Budget pressure evicted ~6 of 8; one more pass visits the evicted
        // entries and drops them (their first visit performed the eviction
        // and dropped them already, but second-chance survivors may linger).
        pool.enforce_budget();
        let resident = handles
            .iter()
            .filter(|h| h.residency() != Residency::Evicted)
            .count();
        assert!(
            pool.queue_len() <= resident + 1,
            "queue ({}) tracks the resident set ({resident}), not all 8 live chunks",
            pool.queue_len(),
        );
        // Fault one back in: it must become an eviction candidate again.
        let evicted = handles
            .iter()
            .find(|h| h.residency() == Residency::Evicted)
            .expect("something was evicted");
        drop(evicted.pin());
        pool.evict(evicted);
        assert_eq!(
            evicted.residency(),
            Residency::Evicted,
            "fault-in re-enqueued the chunk, so it could be evicted again",
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn dead_data_is_never_written() {
        let pool = test_pool(256 << 20);
        let handle = pool.insert(&mut payload(SMALL, 7));
        drop(handle);
        let stats = pool.stats();
        assert_eq!(stats.frees, 1);
        assert_eq!(stats.writes_elided, 1);
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
    fn set_budget_retunes_in_place() {
        let pool = test_pool(usize::MAX);
        let mut handles = Vec::new();
        for seed in 0..8 {
            handles.push(pool.insert(&mut payload(SMALL, 200 + seed)));
        }
        assert_eq!(pool.stats().evictions_compress, 0);

        // Shrinking the budget evicts immediately.
        pool.set_budget(128 << 10);
        let stats = pool.stats();
        assert!(stats.resident_bytes <= 128 << 10);
        assert!(stats.evictions_compress >= 6);

        // Growing it leaves headroom: a fresh insert stays resident.
        pool.set_budget(usize::MAX);
        let h = pool.insert(&mut payload(SMALL, 300));
        assert_eq!(h.residency(), Residency::UnbackedResident);
        for h in &handles {
            let pin = h.pin();
            assert_eq!(pin.len(), SMALL);
        }
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
        assert_eq!(stats.writes_elided, 0);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn oversize_round_trips() {
        let pool = test_pool(256 << 20);
        let words = SIZE_CLASSES[SIZE_CLASSES.len() - 1] / 8 + 1;
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

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn spill_async_evict_round_trip() {
        let pool = test_pool(usize::MAX);
        pool.enable_spill_without_threads();
        let h = pool.insert(&mut payload(SMALL, 400));
        pool.evict(&h);
        assert_eq!(h.residency(), Residency::WriteInFlight);
        // Readable while in flight.
        {
            let pin = h.pin();
            assert_eq!(&pin[..3], &payload(SMALL, 400)[..3]);
        }
        // The guard dropped before processing, so the eviction commits.
        assert!(pool.spill_step());
        assert_eq!(h.residency(), Residency::Evicted);
        let stats = pool.stats();
        assert_eq!(stats.spill_scheduled, 1);
        assert_eq!(stats.evictions_compress, 1);
        pool.poison_free_slots();
        let pin = h.pin();
        assert_eq!(&*pin, &payload(SMALL, 400)[..]);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn spill_freed_while_queued_is_elided() {
        let pool = test_pool(usize::MAX);
        pool.enable_spill_without_threads();
        let h = pool.insert(&mut payload(SMALL, 401));
        pool.evict(&h);
        assert_eq!(h.residency(), Residency::WriteInFlight);
        drop(h);
        assert!(pool.spill_step());
        let stats = pool.stats();
        assert_eq!(stats.spill_cancelled, 1);
        assert_eq!(stats.writes_elided, 1, "freed before compression: elided");
        assert_eq!(stats.extent_bytes_written, 0, "no extent was written");
        assert_eq!(stats.resident_bytes, 0, "slot accounting settled");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn spill_pinned_at_processing_cancels() {
        let pool = test_pool(usize::MAX);
        pool.enable_spill_without_threads();
        let h = pool.insert(&mut payload(SMALL, 402));
        pool.evict(&h);
        let pin = h.pin();
        assert!(pool.spill_step());
        // Pinned at processing time: cancelled back to resident, no extent.
        assert_eq!(h.residency(), Residency::UnbackedResident);
        assert_eq!(pool.stats().spill_cancelled, 1);
        assert_eq!(pool.stats().extent_bytes_written, 0);
        assert_eq!(&*pin, &payload(SMALL, 402)[..]);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap, madvise, and threads
    fn spill_threads_end_to_end() {
        let pool = test_pool(128 << 10);
        pool.set_spill_threads(2);
        let mut handles = Vec::new();
        for seed in 0..16 {
            handles.push(pool.insert(&mut payload(SMALL, 500 + seed)));
        }
        pool.quiesce_spill();
        let stats = pool.stats();
        assert!(
            stats.spill_scheduled > 0,
            "budget pressure should have scheduled spills",
        );
        for (i, h) in handles.iter().enumerate() {
            let pin = h.pin();
            assert_eq!(&*pin, &payload(SMALL, 500 + u64::cast_from(i))[..]);
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn insert_with_fills_in_place() {
        let pool = test_pool(usize::MAX);
        let want = payload(SMALL, 600);
        let h = pool.insert_with(SMALL, |dst| {
            assert_eq!(dst.len(), SMALL, "fill sees exactly the chunk length");
            dst.copy_from_slice(&want);
        });
        assert_eq!(h.residency(), Residency::UnbackedResident);
        assert_eq!(&*h.pin(), &want[..]);
        pool.evict(&h);
        let pin = h.pin();
        assert_eq!(&*pin, &want[..], "round-trips through the extent");

        // Empty and oversize fall back like `insert`.
        let empty = pool.insert_with(0, |dst| assert!(dst.is_empty()));
        assert!(empty.is_empty());
        let big_len = (SIZE_CLASSES[SIZE_CLASSES.len() - 1] / 8) + 1;
        let big = pool.insert_with(big_len, |dst| dst.fill(7));
        assert_eq!(big.residency(), Residency::Oversize);
        assert_eq!(big.pin().len(), big_len);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn slot_exhaustion_degrades_to_heap() {
        // Two 64 KiB slots per class at this capacity; the third insert finds
        // no slot and must fall back to the heap rather than panic.
        let pool = Pool::new(PoolConfig {
            class_capacity_bytes: 128 << 10,
        })
        .expect("pool creation");
        let a = pool.insert(&mut payload(SMALL, 700));
        let b = pool.insert(&mut payload(SMALL, 701));
        let c = pool.insert(&mut payload(SMALL, 702));
        assert_eq!(a.residency(), Residency::UnbackedResident);
        assert_eq!(b.residency(), Residency::UnbackedResident);
        assert_eq!(
            c.residency(),
            Residency::Oversize,
            "fallback is heap-backed"
        );
        assert_eq!(pool.stats().slot_exhausted_fallbacks, 1);
        assert_eq!(&*c.pin(), &payload(SMALL, 702)[..]);
        // Freeing a slotted chunk lets the next insert use the region again.
        drop(a);
        let d = pool.insert(&mut payload(SMALL, 703));
        assert_eq!(d.residency(), Residency::UnbackedResident);
        assert_eq!(&*d.pin(), &payload(SMALL, 703)[..]);
    }
}
