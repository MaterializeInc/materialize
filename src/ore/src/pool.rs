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
//! returns a chunk's slot to the free list along with its physical pages —
//! so slot demand tracks the resident set (bounded by the budget), not the
//! potentially unbounded live backlog. Reads are copy-out
//! ([`ChunkHandle::read_into`]): a resident slot is copied and an evicted
//! extent decompressed straight into the caller's buffer, all under the
//! chunk's state lock, so no reference into pool memory escapes the pool and
//! a read leaves residency untouched. The backing is the swap-backed extent
//! store of the design's Layer 1: a slot in a pool-owned anonymous-memory
//! extent arena holding the chunk's lz4-compressed bytes.
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
//! where `compressed cap = max(0, rss_target - budget - warm cap)`, makes
//! every resident byte's ceiling nameable. A zero RSS target (or one below
//! the budget plus warm cap) collapses the compressed tier and extents page
//! out as soon as they are written. Heap-backed chunks (oversize payloads
//! and class-exhaustion fallbacks) sit outside the identity: they can never
//! be evicted, so the budget is enforced against evictable bytes only.
//! Slots never reach the swap device: only compressed extents are offered
//! to it. Pageout is observed rather than assumed: an extent counts against
//! the compressed tier until the page table shows its whole range unmapped,
//! so reclaim the kernel declines surfaces as `extent_pageout_incomplete`
//! (and a tier settled above its capacity) instead of as RSS the ledger
//! cannot see.
//!
//! Residency is a state, not a type. It descends through eviction and
//! ascends through exactly one transition: an admitting read
//! ([`ChunkHandle::read_into_admit`]) lifts an evicted chunk back to
//! `BackedResident` when a slot is free within the budget or stealable from
//! a clean backed victim of the same class, never by evicting or
//! compressing anything. Plain reads ([`ChunkHandle::read_into`]) leave
//! residency untouched. Eviction I/O runs on spill threads when enabled —
//! `WriteInFlight` marks a chunk whose compression a spill thread owns — and
//! inline on the evicting caller otherwise. Chunks are immutable after
//! [`Pool::insert_with`], which is what makes a `BackedResident` slot always
//! identical to its extent and its eviction free of I/O.
//!
//! Freeing an `UnbackedResident` chunk is a pure memory operation — the
//! design's "never write dead data" win, surfaced as `writes_elided` in
//! [`PoolStats`]. Budget pressure evicts cold chunks via second-chance
//! FIFOs banded by the caller-supplied generational depth ([`ChunkHints`]):
//! eviction and eager backing both visit deeper (colder) bands first, so
//! the youngest data keeps its die-before-write chance longest. Within a
//! band the FIFO is the design's backstop policy, and unannotated chunks
//! (depth 0) get exactly that.

mod extent;
mod region;

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use crate::cast::CastFrom;
use crate::pool::extent::{ExtentArena, Scratch, SwapExtent};
use crate::pool::region::{Region, SIZE_CLASSES};

/// Virtual reservation per size class. Purely virtual: physical memory
/// materializes only for slots in use, and slots are scoped to residency,
/// so this must exceed the largest plausible *resident* set per class, the
/// budget plus in-flight slack, not the backlog. It is deliberately enormous
/// (address space costs nothing, and touched pages are bounded by peak
/// residency) so that no realistic budget, on any machine size, reaches the
/// heap-fallback path.
const CLASS_CAPACITY_BYTES: usize = 1 << 40;

/// A chunk-provided transform between a chunk's body bytes and the stored
/// bytes its extent holds. The pool owns scheduling: spill threads, the
/// residency state machine, cancellation, and the ledger. It invokes the
/// codec on opaque bytes at the extent boundary, `encode` when backing a
/// chunk (on a spill thread, or inline under overload) and `decode` when
/// reading an evicted one, under the chunk's state lock. The pool itself
/// has no opinion on the stored form: framing, compression, and validation
/// all belong to the codec.
///
/// Implementations must be pure transforms: no locking, no calls back into
/// the pool (the state lock is held at `decode` sites), and no panic on
/// bytes their own `encode` produced. `decode` must exactly invert
/// `encode`, and `encode`'s output must never exceed
/// [`max_stored_len`]`(body.len())`, the bound the extent store's size
/// classes are provisioned to.
pub trait ExtentCodec: std::fmt::Debug + Send + Sync {
    /// Transforms `body` into its stored form, replacing `out`'s contents.
    /// `out`'s capacity is reused across calls; implementations size it
    /// themselves.
    fn encode(&self, body: &[u8], out: &mut Vec<u8>);

    /// Inverts [`ExtentCodec::encode`]: reconstructs into `body` exactly
    /// the bytes whose encoding produced `stored`. `body` is exactly the
    /// original body's length, and implementations must panic on a length
    /// mismatch rather than truncate or pad.
    fn decode(&self, stored: &[u8], body: &mut [u8]);

    /// Reconstructs the byte range `[offset, offset + dst.len())` of the
    /// `body_len`-byte body into `dst`. The range lies within the body.
    ///
    /// The default decodes the whole body into a reused scratch and copies
    /// the range out, which is all a whole-block stored form can do. A
    /// codec whose stored form has interior structure (independently
    /// compressed sub-blocks with an offset index) overrides this to decode
    /// only the parts the range touches, which is what makes a sub-range
    /// read cost less than a whole-body read.
    fn decode_range(&self, stored: &[u8], body_len: usize, offset: usize, dst: &mut [u8]) {
        let end = offset
            .checked_add(dst.len())
            .expect("range end overflows usize");
        assert!(end <= body_len, "range end {end} exceeds body {body_len}");
        // Reads run on worker threads, so the scratch mirrors the write
        // side's `Shrink` policy: capacity beyond the ~2 MiB chunk target
        // is released rather than parked per thread.
        use std::cell::RefCell;
        thread_local! {
            static SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
        }
        SCRATCH.with(|cell| {
            let mut scratch = cell.borrow_mut();
            scratch.resize(body_len, 0);
            self.decode(stored, &mut scratch);
            dst.copy_from_slice(&scratch[offset..end]);
            if scratch.capacity() > 2 << 20 {
                scratch.clear();
                scratch.shrink_to_fit();
            }
        });
    }
}

/// The largest stored form [`ExtentCodec::encode`] may produce for a
/// `body_len`-byte body: an incompressible-input expansion matching lz4's
/// worst case plus a four-byte length prefix. The extent store's size-class
/// ladder is provisioned to this bound, so a codec that exceeds it can
/// strand payloads with no class to hold them (they degrade to unpageable
/// heap fallbacks).
pub fn max_stored_len(body_len: usize) -> usize {
    4 + body_len + body_len / 255 + 16
}

/// Advisory placement hints for a chunk, supplied at insert and immutable
/// thereafter (merges mint new chunks, so a chunk's generation never
/// changes). Hints steer policy — eviction order and write-behind
/// candidacy — never correctness: a mislabeled chunk performs worse, while
/// the budget and residency invariants hold regardless.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChunkHints {
    /// Generational depth of the chunk in its producer's merge structure,
    /// 0 for the youngest generation (and the unannotated default). Deeper
    /// chunks are treated as colder: preferred write-behind candidates and
    /// preferred eviction victims, cheap to evict once backed.
    pub depth: u8,
}

/// Number of depth bands the eviction queues are split into; depths at or
/// beyond the last band share it.
const DEPTH_BANDS: usize = 4;

/// The eviction-queue band for a chunk of `depth`.
fn band(depth: u8) -> usize {
    usize::from(depth).min(DEPTH_BANDS - 1)
}

/// Residency state of a chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Residency {
    /// Lives only in the pool; no extent copy exists. Freeing it never
    /// touches the backing store.
    UnbackedResident,
    /// Resident, and an identical extent copy exists; eviction releases
    /// physical pages without I/O.
    BackedResident,
    /// Resident and readable, with compression into an extent scheduled on a
    /// spill thread. Completion moves an evicting chunk to
    /// [`Residency::Evicted`] and an eagerly backed one to
    /// [`Residency::BackedResident`]; a free observed at dequeue cancels the
    /// write instead.
    WriteInFlight,
    /// Extent copy only; the chunk holds no slot. The extent itself may
    /// still be RAM-resident (the compressed tier) or paged out to the swap
    /// device. Reads decompress the extent straight into the caller's
    /// buffer and leave the chunk evicted, except that an admitting read
    /// may lift it back to [`Residency::BackedResident`].
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
    /// Chunks freed (handle dropped).
    pub frees: u64,
    /// Backing writes elided: chunks dead before their compression
    /// completed, so no extent write happened. Covers chunks freed while
    /// `UnbackedResident` and chunks freed while queued for a spill thread
    /// that had not yet compressed them.
    pub writes_elided: u64,
    /// Evictions that compressed the chunk into a new extent.
    pub evictions_compress: u64,
    /// Evictions of `BackedResident` chunks: pure page release, no I/O.
    pub evictions_cheap: u64,
    /// Compressed bytes written into extents.
    pub extent_bytes_written: u64,
    /// Evictions handed to spill threads.
    pub spill_scheduled: u64,
    /// Compressions cancelled because the chunk was freed while queued or
    /// in flight, whatever scheduled them. Eager-backing work counts here
    /// but never in `spill_scheduled`, so this can exceed that counter.
    pub spill_cancelled: u64,
    /// Entries currently queued for or being processed by spill threads.
    pub spill_in_flight: u64,
    /// Inserts that fell back to the heap because their size class had no
    /// free slot (the live set outgrew the class reservation). Heap-backed
    /// chunks behave like oversize ones: always resident, never paged.
    pub slot_exhausted_fallbacks: u64,
    /// Inserts whose payload exceeded the largest size class and therefore
    /// went straight to a heap-backed oversize chunk.
    pub oversize_payloads: u64,
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
    /// Evicted chunks re-admitted to `BackedResident` by an admitting read
    /// out of free budget headroom.
    pub admissions_budget: u64,
    /// Evicted chunks re-admitted to `BackedResident` by an admitting read
    /// stealing the slot of a clean backed victim of the same size class.
    /// The victim becomes `Evicted` with zero I/O and its extent intact.
    pub admissions_steal: u64,
    /// Admitting reads of evicted chunks served as a plain decompress
    /// instead: no budget headroom (or an exhausted size class), and no
    /// clean victim whose growth the budget could absorb.
    pub admissions_denied: u64,
    /// Allocation bytes of compressed extents currently resident — the
    /// compressed-but-resident middle tier. Bounded by the RSS target;
    /// exceeding it pages the oldest extents out to the swap device.
    pub extent_resident_bytes: u64,
    /// Allocation bytes of resident extents the RSS target cannot push out:
    /// retry-capped arena extents (the kernel declined the reclaim advice
    /// until the retry budget ran out) and heap-fallback extents. The
    /// compressed tier settles above its capacity by this amount. Climbing
    /// steadily means pages cannot reach the swap device (no swap, or a
    /// cgroup that cannot reclaim).
    pub extent_unreclaimable_bytes: u64,
    /// Extents pushed to the swap device by RSS-target enforcement, with
    /// the whole range observed nonresident afterwards.
    pub extent_pageouts: u64,
    /// Pageout passes whose observation found some of the extent's pages
    /// still mapped: `MADV_PAGEOUT` may decline pages and still succeed, so
    /// the page table decides. The extent keeps its full resident
    /// accounting and is retried until its per-extent retry cap. Climbing
    /// steadily on a loaded pool means pages cannot actually be unmapped to
    /// the swap device (no swap, or a cgroup that cannot reclaim).
    pub extent_pageout_incomplete: u64,
    /// Extent writes that fell back to the heap because their extent-arena
    /// class had no free slot. Heap-backed extents stay readable but are
    /// never paged out, so their compressed bytes hold RAM until freed.
    pub extent_arena_fallbacks: u64,
}

#[derive(Debug, Default)]
struct Counters {
    inserts: AtomicU64,
    spill_scheduled: AtomicU64,
    spill_cancelled: AtomicU64,
    slot_exhausted_fallbacks: AtomicU64,
    oversize_payloads: AtomicU64,
    frees: AtomicU64,
    writes_elided: AtomicU64,
    evictions_compress: AtomicU64,
    evictions_cheap: AtomicU64,
    extent_bytes_written: AtomicU64,
    resident_bytes: AtomicU64,
    oversize_bytes: AtomicU64,
    warm_bytes: AtomicU64,
    warm_reuses: AtomicU64,
    eager_backs: AtomicU64,
    admissions_budget: AtomicU64,
    admissions_steal: AtomicU64,
    admissions_denied: AtomicU64,
    extent_resident_bytes: AtomicU64,
    extent_unreclaimable_bytes: AtomicU64,
    extent_pageouts: AtomicU64,
    extent_pageout_incomplete: AtomicU64,
}

/// A buffer pool over swap-backed extents. Cheap to clone; all clones share
/// one budget and one backing store.
#[derive(Debug, Clone)]
pub struct Pool(Arc<PoolInner>);

/// The shared state behind every [`Pool`] handle: the budget and RSS
/// ledgers, the size-class slot regions and the extent arena, the eviction
/// and backing queues, and the spill-thread hand-off. One per process in
/// practice; [`Pool`] clones and chunk handles share it through an `Arc`,
/// so it lives until the last handle and spill thread release it.
///
/// Lock order: a chunk's `state` mutex may be held while taking any of the
/// leaf locks — the eviction `queue`, the `extent_queue`, the spill queue,
/// and the region slot allocators — but never the reverse. The enforcement
/// and backing scans additionally drop the queue guard before trying a
/// chunk's state lock (and only ever `try_lock` it), so no path holds a
/// queue lock while waiting on chunk state. The admitting read's victim
/// steal is the one place a chunk's state lock is held while probing
/// another chunk's, and the victim is only ever `try_lock`ed, so two
/// admitters stealing toward each other skip instead of deadlocking. Reads
/// copy out under the chunk's state lock — the same lock eviction takes —
/// so there is no reader-side count and no reader the evictor must account
/// for.
#[derive(Debug)]
struct PoolInner {
    /// Resident-bytes target, enforced against evictable bytes (resident
    /// minus heap-backed, which no eviction can reclaim). Atomic so a
    /// running pool can be retuned in place (operator-driven budget
    /// changes) without orphaning live handles, which share this value
    /// through their `Arc<PoolInner>`.
    budget_bytes: AtomicU64,
    /// Ceiling on the pool's *total* RSS: slots (the budget) plus warm free
    /// slots plus compressed-resident extents. The compressed tier's
    /// capacity derives as `max(0, rss_target - budget - warm cap)`; zero
    /// (the default) collapses the tier, paging every extent out as soon as
    /// it is written.
    rss_target_bytes: AtomicU64,
    /// One region per entry of [`SIZE_CLASSES`], same order.
    regions: Vec<Region>,
    /// The arena backing extents. Shared with every live [`SwapExtent`],
    /// whose drop returns its slot.
    extent_arena: Arc<ExtentArena>,
    /// Second-chance FIFOs of eviction candidates, one per depth band; a
    /// chunk joins the band of its [`ChunkHints`] depth at insert and again
    /// on re-admission. Entries for freed chunks go stale in place and are
    /// dropped by [`PoolInner::prune_queues`].
    ///
    /// Two scanners walk them with different obligations, both visiting the
    /// deepest band first. Budget enforcement is the one that ages chunks:
    /// it spends the touched bit (second chance) and drops entries it
    /// evicts. Eager backing ([`PoolInner::back_one`]) rotates visited
    /// entries to the back but never spends a touched bit, so a backing
    /// pass shuffles FIFO order without aging any chunk toward eviction.
    queues: [Mutex<VecDeque<Weak<ChunkMeta>>>; DEPTH_BANDS],
    /// FIFO of chunks whose extents are resident, oldest first — the
    /// RSS-target enforcement's victim queue. Entries go stale when an
    /// extent pages out, is dropped, or its chunk dies; visits drop them,
    /// and [`PoolInner::prune_extent_queue`] compacts dead-chunk entries
    /// that under-cap operation never visits.
    extent_queue: Mutex<VecDeque<Weak<ChunkMeta>>>,
    /// Number of live size-classed chunks (whatever their residency), which
    /// is the number of non-stale queue entries across all bands;
    /// [`PoolInner::prune_queues`] compacts the queues against it.
    live_chunks: AtomicU64,
    /// Number of live chunks whose extent is currently resident, including
    /// unreclaimable extents that deliberately hold no `extent_queue` entry
    /// (heap-backed and retry-capped ones). It therefore upper-bounds the
    /// queue's non-stale entries, and [`PoolInner::prune_extent_queue`]'s
    /// compaction threshold is conservative by the unreclaimable count.
    extent_residents: AtomicU64,
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
    /// Parks idle spill threads. Notified when work lands in `queue`, when
    /// eager backing turns on, and at shutdown; threads additionally wake
    /// on a timeout so eager backing scans for write-behind work without a
    /// dedicated wakeup per candidate.
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
    /// Test-only lifecycle: production spill threads are immortal (the pool
    /// is a process singleton), but Miri rejects a test binary exiting with
    /// live threads, so tests stop and join them.
    #[cfg(test)]
    stop: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    handles: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

/// Beyond this many queued or in-flight spill entries, eviction degrades to
/// inline on the caller: bounded memory overshoot under burst beats an
/// unbounded queue of still-resident chunks.
const SPILL_IN_FLIGHT_MAX: usize = 64;

/// What a spill thread does with a chunk once compressed.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SpillKind {
    /// Budget-driven: release the slot, leaving the chunk `Evicted`.
    Evict,
    /// Eager write-behind: keep the slot, leaving the chunk
    /// `BackedResident`.
    Back,
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
    /// The insert-time [`ChunkHints`] depth; immutable. Names the eviction
    /// band the chunk's queue entries belong to.
    depth: u8,
    /// The insert-time [`ExtentCodec`]; immutable. Encodes the chunk when it
    /// is backed and decodes its extent on reads, so it must outlive any
    /// extent it produced, hence `'static`.
    codec: &'static dyn ExtentCodec,
    state: Mutex<ChunkState>,
}

#[derive(Debug)]
struct ChunkState {
    residency: Residency,
    /// Second-chance bit, set on read and cleared (in lieu of eviction) when
    /// the budget enforcer first visits the chunk.
    touched: bool,
    /// Set when the owning handle is dropped, so a queue entry upgraded
    /// concurrently with the free cannot touch a recycled slot.
    freed: bool,
    /// The chunk's slot index within its class's region, held exactly while
    /// the chunk occupies pool memory (the resident states and
    /// `WriteInFlight`). Eviction returns the slot to the region free list.
    /// Reads copy the slot out under the state lock; no pointer into the
    /// slot outlives the lock under which it was formed.
    slot: Option<u32>,
    /// The backing copy; present exactly in the `BackedResident` and
    /// `Evicted` states.
    extent: Option<SwapExtent>,
    /// The payload of an `Oversize` chunk.
    oversize: Option<Vec<u64>>,
}

impl ChunkMeta {
    /// A fresh chunk in its insert-time state.
    fn new(
        pool: &Arc<PoolInner>,
        len: usize,
        class: Option<usize>,
        depth: u8,
        codec: &'static dyn ExtentCodec,
        residency: Residency,
        slot: Option<u32>,
        oversize: Option<Vec<u64>>,
    ) -> ChunkMeta {
        ChunkMeta {
            pool: Arc::clone(pool),
            len,
            class,
            depth,
            codec,
            state: Mutex::new(ChunkState {
                residency,
                touched: false,
                freed: false,
                slot,
                extent: None,
                oversize,
            }),
        }
    }

    fn len_bytes(&self) -> usize {
        self.len * std::mem::size_of::<u64>()
    }

    /// Locks the chunk's state.
    fn state(&self) -> MutexGuard<'_, ChunkState> {
        self.state.lock().expect("chunk state poisoned")
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

impl Pool {
    /// Creates a pool, reserving one virtual region per size class. The
    /// pool starts with an unlimited budget — nothing is evicted until
    /// [`Pool::set_budget`] tunes it.
    pub fn new() -> std::io::Result<Pool> {
        Pool::with_class_capacity(CLASS_CAPACITY_BYTES)
    }

    /// As [`Pool::new`], with a caller-chosen virtual reservation per size
    /// class. Small reservations let tests exercise slot exhaustion.
    fn with_class_capacity(class_capacity_bytes: usize) -> std::io::Result<Pool> {
        let regions = SIZE_CLASSES
            .iter()
            .map(|&class_size| Region::new(class_size, class_capacity_bytes))
            .collect::<std::io::Result<Vec<_>>>()?;
        let extent_arena = Arc::new(ExtentArena::new(class_capacity_bytes)?);
        Ok(Pool(Arc::new(PoolInner {
            budget_bytes: AtomicU64::new(u64::MAX),
            rss_target_bytes: AtomicU64::new(0),
            regions,
            extent_arena,
            queues: std::array::from_fn(|_| Mutex::new(VecDeque::new())),
            extent_queue: Mutex::new(VecDeque::new()),
            live_chunks: AtomicU64::new(0),
            extent_residents: AtomicU64::new(0),
            enforcing: Mutex::new(()),
            counters: Counters::default(),
            spill: Spill::default(),
        })))
    }

    /// Allocates a chunk of `len` words and fills it in place: `fill`
    /// receives the chunk's slot memory directly and must overwrite all of
    /// it (the slot's prior contents are unspecified). The returned handle
    /// starts `UnbackedResident`. A zero `len` returns a length-0 handle
    /// holding no slot; payloads beyond the largest size class fall back to
    /// a plain heap allocation, always resident, a prototype limitation.
    /// `hints` steer eviction and write-behind policy; callers without
    /// placement knowledge pass the default.
    ///
    /// This is the zero-staging insert: serialization can write its single
    /// copy straight into pool memory, paying one page population instead of
    /// staging through caller-side buffers that fault their own pages and
    /// die immediately after.
    ///
    /// `codec` is the chunk's [`ExtentCodec`], fixed for its lifetime: the
    /// pool invokes it whenever the chunk moves across the extent boundary,
    /// and takes no interest in the stored form it produces.
    ///
    /// Relies on abort-on-panic: a panic in `fill` that was caught would
    /// leak the slot and its resident-bytes accounting. All hosting
    /// binaries abort via `mz_ore::panic::install_enhanced_handler`, and
    /// pool consumers are dataflow operators, never code hosted under a
    /// `catch_unwind` boundary the way the optimizer is.
    pub fn insert_with(
        &self,
        len: usize,
        hints: ChunkHints,
        codec: &'static dyn ExtentCodec,
        fill: impl FnOnce(&mut [u64]),
    ) -> ChunkHandle {
        let inner = &self.0;
        inner.counters.inserts.fetch_add(1, Ordering::Relaxed);
        let len_bytes = len * std::mem::size_of::<u64>();
        if len == 0 {
            fill(&mut []);
            let meta = ChunkMeta::new(
                inner,
                0,
                None,
                hints.depth,
                codec,
                Residency::UnbackedResident,
                None,
                None,
            );
            return ChunkHandle {
                meta: Arc::new(meta),
            };
        }
        let class = region::size_class_for(len_bytes);
        if class.is_none() {
            // The payload exceeds the largest size class, so it goes straight
            // to a heap-backed oversize chunk.
            inner
                .counters
                .oversize_payloads
                .fetch_add(1, Ordering::Relaxed);
        }
        // A class with no free slot degrades to the heap path below: the
        // resident set outgrew the class reservation, and an unpageable chunk
        // beats a dead replica. Warn once; the fallback counter tracks scale.
        let slot = class.and_then(|class| inner.alloc_slot(class, len_bytes));
        // Whichever home the payload found, it is resident.
        inner
            .counters
            .resident_bytes
            .fetch_add(u64::cast_from(len_bytes), Ordering::Relaxed);
        let meta = match (class, slot) {
            (Some(class), Some(slot)) => {
                let region = &inner.regions[class];
                // SAFETY: the freshly allocated slot is at least `len_bytes`
                // long (the class fits the payload) and is exclusively owned
                // by this not-yet-shared chunk, so the mutable borrow is
                // unique; region memory is mapped and writable, and `u64` has
                // no validity requirements beyond size, so exposing the
                // unspecified prior contents through `&mut [u64]` is sound.
                let dst = unsafe {
                    std::slice::from_raw_parts_mut(region.slot_ptr(slot).cast::<u64>(), len)
                };
                // The fill contract (overwrite all `len` words) is
                // discipline-only. Poison in debug builds so an
                // under-writing fill reads back as deterministic garbage
                // instead of a previous occupant's bytes, which the heap
                // path's zero fill would otherwise mask in tests.
                #[cfg(debug_assertions)]
                dst.fill(u64::from_ne_bytes([0xDE; 8]));
                fill(dst);
                ChunkMeta::new(
                    inner,
                    len,
                    Some(class),
                    hints.depth,
                    codec,
                    Residency::UnbackedResident,
                    Some(slot),
                    None,
                )
            }
            _ => {
                let mut payload = vec![0u64; len];
                fill(&mut payload);
                inner
                    .counters
                    .oversize_bytes
                    .fetch_add(u64::cast_from(len_bytes), Ordering::Relaxed);
                ChunkMeta::new(
                    inner,
                    len,
                    None,
                    hints.depth,
                    codec,
                    Residency::Oversize,
                    None,
                    Some(payload),
                )
            }
        };
        let meta = Arc::new(meta);
        if meta.class.is_some() {
            inner.live_chunks.fetch_add(1, Ordering::Relaxed);
            inner
                .queue(band(meta.depth))
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
            extent_bytes_written: c.extent_bytes_written.load(Ordering::Relaxed),
            resident_bytes: c.resident_bytes.load(Ordering::Relaxed),
            oversize_bytes: c.oversize_bytes.load(Ordering::Relaxed),
            warm_bytes: c.warm_bytes.load(Ordering::Relaxed),
            warm_reuses: c.warm_reuses.load(Ordering::Relaxed),
            eager_backs: c.eager_backs.load(Ordering::Relaxed),
            admissions_budget: c.admissions_budget.load(Ordering::Relaxed),
            admissions_steal: c.admissions_steal.load(Ordering::Relaxed),
            admissions_denied: c.admissions_denied.load(Ordering::Relaxed),
            extent_resident_bytes: c.extent_resident_bytes.load(Ordering::Relaxed),
            extent_unreclaimable_bytes: c.extent_unreclaimable_bytes.load(Ordering::Relaxed),
            extent_pageouts: c.extent_pageouts.load(Ordering::Relaxed),
            extent_pageout_incomplete: c.extent_pageout_incomplete.load(Ordering::Relaxed),
            extent_arena_fallbacks: self.0.extent_arena.fallbacks(),
            spill_scheduled: c.spill_scheduled.load(Ordering::Relaxed),
            spill_cancelled: c.spill_cancelled.load(Ordering::Relaxed),
            spill_in_flight: self.0.spill.in_flight.load(Ordering::Relaxed),
            slot_exhausted_fallbacks: c.slot_exhausted_fallbacks.load(Ordering::Relaxed),
            oversize_payloads: c.oversize_payloads.load(Ordering::Relaxed),
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
                    let handle = std::thread::Builder::new()
                        .name(format!("pool-spill-{i}"))
                        .spawn(move || inner.spill_worker())
                        .expect("spawn pool spill thread");
                    #[cfg(test)]
                    self.0
                        .spill
                        .handles
                        .lock()
                        .expect("spill handles poisoned")
                        .push(handle);
                    #[cfg(not(test))]
                    drop(handle);
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

    /// Test hook: stops and joins the spill threads, so a test binary exits
    /// with none alive (which Miri requires). Stopped threads process no
    /// further queued work; call [`Pool::quiesce_spill`] first when the test
    /// depends on the queue draining.
    #[cfg(test)]
    fn join_spill_threads(&self) {
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
    #[cfg(test)]
    fn enable_spill_without_threads(&self) {
        self.0.spill.enabled.store(true, Ordering::Relaxed);
    }

    /// Test hook: processes one queued spill entry on the calling thread.
    /// Returns whether an entry was processed.
    #[cfg(test)]
    fn spill_step(&self) -> bool {
        let popped = self.0.spill_queue().pop_front();
        let Some(meta) = popped else {
            return false;
        };
        self.0.spill_process(&meta, SpillKind::Evict);
        self.0.spill.in_flight.fetch_sub(1, Ordering::Relaxed);
        true
    }

    /// Test hook: runs one compressed-cap enforcement pass on the calling
    /// thread.
    #[cfg(test)]
    fn enforce_compressed(&self) {
        self.0.enforce_compressed_cap();
    }

    /// Test hook: evicts cold chunks until resident bytes fall to the budget
    /// or every queued chunk has been visited once. Enforcement runs
    /// automatically on every insert and budget shrink.
    #[cfg(test)]
    fn enforce_budget(&self) {
        self.0.enforce_budget();
    }

    /// Test hook: runs one compressed-cap enforcement pass inline on the
    /// calling thread, where the fake residency observation applies.
    #[cfg(test)]
    fn enforce_rss_target(&self) {
        self.0.enforce_compressed_cap();
    }

    /// Retunes the resident-bytes budget in place and enforces it. Live
    /// handles share the new value immediately through their `Arc<PoolInner>`;
    /// a shrink takes effect by evicting on this call, a grow simply leaves
    /// more headroom for future inserts.
    pub fn set_budget(&self, budget_bytes: usize) {
        let new = u64::cast_from(budget_bytes);
        let prev = self.0.budget_bytes.swap(new, Ordering::Relaxed);
        // Config application calls this per worker per tick; only a change
        // warrants an enforcement pass (a grow needs none, and inserts
        // enforce continuously anyway).
        if new < prev {
            self.0.trim_warm_pool();
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

    /// Test-only: the number of entries across the second-chance queues,
    /// live and stale.
    #[cfg(test)]
    fn queue_len(&self) -> usize {
        (0..DEPTH_BANDS).map(|band| self.0.queue(band).len()).sum()
    }

    /// Test-only: the number of resident-extent queue entries, live and
    /// stale.
    #[cfg(test)]
    fn extent_queue_len(&self) -> usize {
        self.0.extent_queue().len()
    }

    /// Test hook: explicitly evicts one chunk. No-op if the chunk is already
    /// evicted, in flight, empty, or oversize. With spill threads enabled the
    /// compression is handed off and completes asynchronously (observable via
    /// [`Residency::WriteInFlight`]); without them it runs inline.
    #[cfg(test)]
    fn evict(&self, handle: &ChunkHandle) {
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
    #[cfg(test)]
    fn poison_free_slots(&self) {
        for region in &self.0.regions {
            region.poison_free_slots();
        }
    }
}

impl PoolInner {
    /// Locks the eviction queue of one depth band.
    fn queue(&self, band: usize) -> MutexGuard<'_, VecDeque<Weak<ChunkMeta>>> {
        self.queues[band].lock().expect("pool queue poisoned")
    }

    /// Locks the resident-extent queue.
    fn extent_queue(&self) -> MutexGuard<'_, VecDeque<Weak<ChunkMeta>>> {
        self.extent_queue.lock().expect("extent queue poisoned")
    }

    /// Locks the spill hand-off queue.
    fn spill_queue(&self) -> MutexGuard<'_, VecDeque<Arc<ChunkMeta>>> {
        self.spill.queue.lock().expect("spill queue poisoned")
    }

    /// The region behind a slotted chunk's size class.
    fn region_of(&self, meta: &ChunkMeta) -> &Region {
        &self.regions[meta.class.expect("slotted chunk has a class")]
    }

    /// Borrows the payload of a slotted chunk.
    ///
    /// # Safety
    ///
    /// `slot` must be `meta`'s slot, its contents must be initialized (they
    /// are from insert onward), and nothing may write the slot while the
    /// borrow lives.
    unsafe fn slot_data(&self, meta: &ChunkMeta, slot: u32) -> &[u64] {
        let ptr = self
            .region_of(meta)
            .slot_ptr(slot)
            .cast_const()
            .cast::<u64>();
        // SAFETY: per the function contract; `meta.len` words fit the class
        // by construction.
        unsafe { std::slice::from_raw_parts(ptr, meta.len) }
    }

    /// Records a freshly written extent under the chunk's state lock: the
    /// compressed-bytes counter, the compressed-tier accounting, and the
    /// state's extent field.
    fn commit_extent(&self, meta: &Arc<ChunkMeta>, state: &mut ChunkState, extent: SwapExtent) {
        self.counters
            .extent_bytes_written
            .fetch_add(u64::cast_from(extent.comp_len()), Ordering::Relaxed);
        // A heap-fallback extent is born permanently capped and counts as
        // unreclaimable from the start.
        self.note_extent_resident(meta, extent.alloc_size(), !extent.pageout_capped());
        state.extent = Some(extent);
    }

    /// Drops queue entries whose chunk has been freed, detected by their
    /// `Weak` no longer holding a live chunk. Each band compacts only when
    /// its stale entries outnumber all live chunks (plus a small floor), so
    /// the cost amortizes to a constant per insert and the total queue
    /// length stays proportional to the number of live slotted chunks even
    /// when the pool never comes under budget pressure.
    fn prune_queues(&self) {
        let live = usize::cast_from(self.live_chunks.load(Ordering::Relaxed));
        for band in 0..DEPTH_BANDS {
            let mut queue = self.queue(band);
            if queue.len() > 2 * live + 16 {
                queue.retain(|weak| weak.strong_count() > 0);
            }
        }
    }

    fn enforce_budget(&self) {
        // Single-flight: enforcement runs synchronously on whichever thread
        // trips it (every insert), and concurrent passes would
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

    /// Bytes budget enforcement can actually reclaim: resident bytes minus
    /// heap-backed (oversize and class-exhaustion) chunks, which hold no
    /// slot and can never be evicted. Enforcing against raw resident bytes
    /// would, once unevictable bytes alone exceed the budget, compress
    /// every slotted chunk on arrival forever.
    fn evictable_bytes(&self) -> u64 {
        self.counters
            .resident_bytes
            .load(Ordering::Relaxed)
            .saturating_sub(self.counters.oversize_bytes.load(Ordering::Relaxed))
    }

    fn enforce_budget_inner(&self) {
        self.prune_queues();
        // Deepest band first: deep chunks are the coldest, and once eager
        // backing has visited them (same order) their eviction is a pure
        // page release. The youngest band is reached only when the deeper
        // bands cannot satisfy the budget, keeping young data's
        // die-before-write chance longest.
        for band in (0..DEPTH_BANDS).rev() {
            if self.evictable_bytes() <= self.budget_bytes.load(Ordering::Relaxed) {
                return;
            }
            self.enforce_budget_band(band);
        }
    }

    fn enforce_budget_band(&self, band: usize) {
        // The queue holds resident chunks only (entries for evicted chunks
        // are dropped on visit and never re-added), so a full pass is
        // proportional to the resident set. Visit each queued chunk at most
        // twice per call: a first visit may only clear the second-chance
        // bit, so a second is needed before an over-budget call is
        // guaranteed to evict every chunk it saw. The bound keeps contended
        // and in-flight entries from spinning this loop forever.
        let mut remaining = self.queue(band).len().saturating_mul(2);
        while remaining > 0 && self.evictable_bytes() > self.budget_bytes.load(Ordering::Relaxed) {
            remaining -= 1;
            let popped = self.queue(band).pop_front();
            let Some(weak) = popped else {
                break;
            };
            let Some(meta) = weak.upgrade() else {
                continue;
            };
            let requeue = {
                // `try_lock`: a chunk mid-eviction or mid-read holds its
                // lock for milliseconds; skipping it beats convoying every
                // budget enforcer in the process behind one chunk's I/O.
                let Ok(mut state) = meta.state.try_lock() else {
                    self.queue(band).push_back(weak);
                    continue;
                };
                if state.freed {
                    false
                } else if matches!(state.residency, Residency::Evicted | Residency::Oversize) {
                    // Nothing to evict: drop the entry. A chunk re-enters
                    // the queue only when it becomes resident again (insert
                    // or re-admission), so the queue stays proportional to
                    // the resident set rather than accumulating every chunk
                    // ever evicted.
                    false
                } else if state.touched {
                    state.touched = false;
                    true
                } else if self.spill_handoff(&meta, &mut state) {
                    // Stays queued while in flight; once the spill commits to
                    // `Evicted`, the next visit drops the entry.
                    true
                } else {
                    self.evict_locked(&meta, &mut state);
                    state.residency != Residency::Evicted
                }
            };
            if requeue {
                self.queue(band).push_back(weak);
            }
        }
    }

    fn evict_locked(&self, meta: &Arc<ChunkMeta>, state: &mut ChunkState) {
        let Some(slot) = state.slot else {
            return;
        };
        if state.freed {
            return;
        }
        match state.residency {
            Residency::UnbackedResident => {
                // SAFETY: the slot belongs to this live chunk and the state
                // lock is held, so nothing else touches the slot while this
                // borrow is live (reads copy out under the same lock).
                let data = unsafe { self.slot_data(meta, slot) };
                // Inline eviction runs on whichever thread tripped the
                // budget, so the compression scratch must not stay parked
                // on it.
                let extent =
                    SwapExtent::write(&self.extent_arena, data, meta.codec, Scratch::Shrink);
                self.counters
                    .evictions_compress
                    .fetch_add(1, Ordering::Relaxed);
                self.commit_extent(meta, state, extent);
            }
            Residency::BackedResident => {
                self.counters
                    .evictions_cheap
                    .fetch_add(1, Ordering::Relaxed);
            }
            Residency::WriteInFlight | Residency::Evicted | Residency::Oversize => return,
        }
        // `release_slot`'s precondition holds: the state lock is held and
        // `!freed` was checked above under it.
        self.release_slot(meta, state);
        state.residency = Residency::Evicted;
    }

    /// Whether the next eviction should be handed to spill threads: enabled,
    /// and the queue is below the backpressure bound (beyond it, callers
    /// evict inline rather than growing an unbounded queue of still-resident
    /// chunks).
    fn spill_eligible(&self) -> bool {
        self.spill.enabled.load(Ordering::Relaxed)
            && usize::cast_from(self.spill.in_flight.load(Ordering::Relaxed)) < SPILL_IN_FLIGHT_MAX
    }

    /// Hands a `WriteInFlight` chunk to the spill threads.
    fn spill_schedule(&self, meta: Arc<ChunkMeta>) {
        self.counters
            .spill_scheduled
            .fetch_add(1, Ordering::Relaxed);
        self.spill.in_flight.fetch_add(1, Ordering::Relaxed);
        self.spill_queue().push_back(meta);
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
            #[cfg(test)]
            if self.spill.stop.load(Ordering::Relaxed) {
                return;
            }
            // Tier-2 pageouts ride the spill threads: every pass through the
            // loop (job completion, condvar wakeup, park timeout) trims the
            // compressed tier if needed. A single atomic load when under cap.
            self.enforce_compressed_cap();
            let popped = self.spill_queue().pop_front();
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
            let queue = self.spill_queue();
            if queue.is_empty() {
                let _ = self
                    .spill
                    .cv
                    .wait_timeout(queue, std::time::Duration::from_millis(100))
                    .expect("spill queue poisoned");
            }
        }
    }

    /// Eagerly compresses one unbacked chunk from the eviction queues into
    /// `BackedResident`, returning whether a chunk was backed — `false`
    /// means nothing was actionable (queues empty, or the bounded scans
    /// found only already-backed, in-flight, contended, or stale entries)
    /// and the caller should park rather than rescan. Bands are visited
    /// deepest first, mirroring eviction order so the chunks evicted first
    /// are the ones whose backing is already pre-paid.
    fn back_one(&self) -> bool {
        for band in (0..DEPTH_BANDS).rev() {
            if self.back_one_from(band) {
                return true;
            }
        }
        false
    }

    /// One bounded backing scan over a single band's queue. Non-actionable
    /// entries are requeued or dropped per the same rules budget
    /// enforcement uses, except that the second-chance `touched` bit is
    /// left alone — backing is not an eviction and must not consume a
    /// chunk's reprieve.
    fn back_one_from(&self, band: usize) -> bool {
        for _ in 0..16 {
            let popped = self.queue(band).pop_front();
            let Some(weak) = popped else {
                return false;
            };
            let Some(meta) = weak.upgrade() else {
                continue;
            };
            {
                let Ok(mut state) = meta.state.try_lock() else {
                    self.queue(band).push_back(weak);
                    continue;
                };
                if state.freed {
                    continue;
                }
                match state.residency {
                    Residency::Evicted | Residency::Oversize => {
                        continue;
                    }
                    Residency::UnbackedResident => {
                        state.residency = Residency::WriteInFlight;
                    }
                    Residency::BackedResident | Residency::WriteInFlight => {
                        self.queue(band).push_back(weak);
                        continue;
                    }
                }
            }
            self.spill.in_flight.fetch_add(1, Ordering::Relaxed);
            self.spill_process(&meta, SpillKind::Back);
            self.spill.in_flight.fetch_sub(1, Ordering::Relaxed);
            // The chunk remains an eviction candidate (now a cheap one).
            self.queue(band).push_back(weak);
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
            let mut state = meta.state();
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
            slot = state.slot.expect("write-in-flight chunk has a slot");
        }
        // SAFETY: the chunk is live (the queue holds an `Arc`) and in
        // `WriteInFlight`, so the slot is not recycled (`ChunkHandle::drop`
        // defers slot release to this thread in that state) and its contents
        // are immutable; concurrent copy-out reads take the state lock and
        // read the slot, but nothing writes it.
        let data = unsafe { self.slot_data(meta, slot) };
        // Spill threads see a steady job stream, so they keep the grown
        // compression scratch for the next job.
        let extent = SwapExtent::write(&self.extent_arena, data, meta.codec, Scratch::Retain);
        // Commit under the lock.
        let mut state = meta.state();
        if state.freed {
            // Freed during compression: the extent is garbage; cleanup is
            // ours as above. Compression ran, so this is not an elided free.
            self.counters
                .spill_cancelled
                .fetch_add(1, Ordering::Relaxed);
            self.release_slot(meta, &mut state);
            return;
        }
        self.commit_extent(meta, &mut state, extent);
        match kind {
            SpillKind::Back => {
                // The slot stays for write-behind: the chunk remains
                // readable, and the extent makes a later budget eviction a
                // pure page release.
                self.counters.eager_backs.fetch_add(1, Ordering::Relaxed);
                state.residency = Residency::BackedResident;
            }
            SpillKind::Evict => {
                // `release_slot`'s precondition holds: the state lock is
                // held and `!freed` was observed under it.
                self.counters
                    .evictions_compress
                    .fetch_add(1, Ordering::Relaxed);
                self.release_slot(meta, &mut state);
                state.residency = Residency::Evicted;
            }
        };
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
    /// slot reuse faults no pages and skips the kernel's page zeroing.
    ///
    /// Precondition: the caller holds the chunk's state lock, and no
    /// reference into the slot exists — copy-out reads borrow the slot only
    /// under that same lock, and a `WriteInFlight` chunk's unlocked
    /// compression read belongs to the spill thread, which is the only
    /// caller that releases the slot in that state. This is what makes the
    /// `dontneed` below sound, and what makes keeping a warm slot's stale
    /// contents safe: the slot's next occupant fully overwrites every byte
    /// it reads, satisfying the contents-undefined contract either way.
    fn release_slot(&self, meta: &ChunkMeta, state: &mut ChunkState) {
        let slot = state.slot.take().expect("slotted chunk");
        let region = self.region_of(meta);
        let warm = self.try_keep_warm(region.class_size());
        if !warm {
            // SAFETY: no reference into the slot exists (the function-level
            // precondition, established under the held state lock).
            unsafe {
                region::dontneed(region.slot_ptr(slot), region.class_size());
            }
        }
        region.free(slot, warm);
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

    /// Cools warm free slots until `warm_bytes` falls to the warm cap. A
    /// budget shrink lowers the cap, and warm capacity is checked only when
    /// a slot is freed, so without this pass slots parked under the old cap
    /// would hold their pages until same-class reuse happened to drain them,
    /// exactly when the shrink wanted the memory back.
    fn trim_warm_pool(&self) {
        let mut over = self
            .counters
            .warm_bytes
            .load(Ordering::Relaxed)
            .saturating_sub(self.warm_cap());
        for region in &self.regions {
            if over == 0 {
                return;
            }
            let cooled = u64::cast_from(region.cool_warm_slots(usize::cast_from(over)));
            self.counters
                .warm_bytes
                .fetch_sub(cooled, Ordering::Relaxed);
            over = over.saturating_sub(cooled);
        }
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

    /// Allocates a slot in `class` for a payload of `len_bytes` with
    /// warm-pool accounting (a warm allocation is counted as a reuse and
    /// trimmed to the payload), or `None` when the class has no free slot.
    fn try_alloc_slot(&self, class: usize, len_bytes: usize) -> Option<u32> {
        let (index, warm) = self.regions[class].alloc()?;
        if warm {
            // The allocation faulted no pages; its bytes leave the warm
            // pool.
            let class_bytes = u64::cast_from(self.regions[class].class_size());
            self.counters
                .warm_bytes
                .fetch_sub(class_bytes, Ordering::Relaxed);
            self.counters.warm_reuses.fetch_add(1, Ordering::Relaxed);
            // A warm slot keeps the prior occupant's resident pages, which
            // may extend past the new payload while the ledger credits only
            // `len_bytes`.
            self.trim_slot_tail(class, index, len_bytes);
        }
        Some(index)
    }

    /// Releases a slot's pages beyond the first `len_bytes` (rounded up to
    /// a page), so a slot reused for a smaller payload does not keep its
    /// prior occupant's tail pages resident with no bytes in the ledger to
    /// answer for them.
    ///
    /// Precondition: the caller exclusively owns the slot (freshly
    /// allocated, or taken from a victim under the victim's state lock)
    /// with no reference into it.
    fn trim_slot_tail(&self, class: usize, slot: u32, len_bytes: usize) {
        let region = &self.regions[class];
        // Hugepage-class slots trim at huge-page granularity: a base-page
        // trim would split the slot's `MADV_HUGEPAGE` folios, and khugepaged
        // may later re-collapse a partially trimmed range, re-instantiating
        // pages the ledger counts as released. Whole-folio trims leave no
        // partial folio to split or resurrect.
        let granule = if region.class_size() >= region::HUGE_PAGE {
            region::HUGE_PAGE
        } else {
            region::page_size()
        };
        let keep = len_bytes.next_multiple_of(granule).min(region.class_size());
        let tail = region.class_size() - keep;
        if tail == 0 {
            return;
        }
        // SAFETY: the caller exclusively owns the slot per the
        // precondition, and `keep + tail` is exactly the class size, so the
        // range stays within the slot.
        unsafe {
            region::dontneed(region.slot_ptr(slot).add(keep), tail);
        }
    }

    /// Allocates a slot in `class` for an insert: as
    /// [`PoolInner::try_alloc_slot`], with an exhausted class counted as a
    /// heap fallback for a `len_bytes` payload (warned about once). `None`
    /// means the caller must degrade to the heap.
    fn alloc_slot(&self, class: usize, len_bytes: usize) -> Option<u32> {
        match self.try_alloc_slot(class, len_bytes) {
            Some(index) => Some(index),
            None => {
                self.counters
                    .slot_exhausted_fallbacks
                    .fetch_add(1, Ordering::Relaxed);
                static EXHAUSTED_ONCE: std::sync::Once = std::sync::Once::new();
                EXHAUSTED_ONCE.call_once(|| {
                    tracing::warn!(
                        len_bytes,
                        "buffer pool size class exhausted; falling back to heap chunks \
                         (raise the pool's per-class virtual reservation)",
                    );
                });
                None
            }
        }
    }

    /// Acquires a slot for re-admitting an evicted chunk, from free budget
    /// headroom or by stealing a clean backed victim's slot, never by
    /// evicting or compressing anything. `None` counts a denied admission.
    /// On success the admitted chunk's resident-bytes accounting and the
    /// admission counter are settled, and the caller (who holds the chunk's
    /// state lock) owns the slot: its contents are unspecified (fresh,
    /// warm, or the victim's stale bytes) and must be fully overwritten.
    fn admit_slot(&self, meta: &ChunkMeta) -> Option<u32> {
        let class = meta.class.expect("evicted chunk has a class");
        let len_bytes = u64::cast_from(meta.len_bytes());
        // Free budget first: reserve the bytes, then a slot. The
        // reservation never pushes resident bytes past the budget, and a
        // class with no free slot hands the reservation back rather than
        // evicting anything to make room. The headroom test uses evictable
        // bytes, matching budget enforcement: unevictable heap-backed bytes
        // must not permanently veto budget-path admissions the enforcer
        // would never need to undo.
        let reserved = self
            .counters
            .resident_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                // Loaded inside the closure so a CAS retry sees oversize
                // frees that landed since the last attempt.
                let oversize = self.counters.oversize_bytes.load(Ordering::Relaxed);
                let next = cur.checked_add(len_bytes)?;
                (next.saturating_sub(oversize) <= self.budget_bytes.load(Ordering::Relaxed))
                    .then_some(next)
            })
            .is_ok();
        if reserved {
            if let Some(slot) = self.try_alloc_slot(class, meta.len_bytes()) {
                self.counters
                    .admissions_budget
                    .fetch_add(1, Ordering::Relaxed);
                return Some(slot);
            }
            self.counters
                .resident_bytes
                .fetch_sub(len_bytes, Ordering::Relaxed);
        }
        if let Some(slot) = self.steal_clean_victim(class, meta.len_bytes()) {
            // The ledger was settled inside the steal: the victim's bytes
            // out, the admitted chunk's in, with any growth reserved
            // against the budget. The slot's physical pages transfer
            // deliberately, but only up to the admitted payload: the
            // victim's pages past it would stay resident with no ledger
            // bytes to answer for them.
            self.trim_slot_tail(class, slot, meta.len_bytes());
            self.counters
                .admissions_steal
                .fetch_add(1, Ordering::Relaxed);
            return Some(slot);
        }
        self.counters
            .admissions_denied
            .fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Takes the slot of a clean victim in `class` for an admitted payload
    /// of `admitted_len_bytes`: a `BackedResident` chunk with a clear
    /// touched bit, whose extent already duplicates its slot, so the victim
    /// transitions to `Evicted` with zero I/O, its extent intact, and its
    /// queue entry dropped. The returned slot keeps its physical pages (no
    /// `dontneed`, no free-list round trip). They hold the victim's stale
    /// bytes. The ledger is settled inside the steal: the victim's bytes
    /// leave and the admitted payload's enter in one step, and a steal that
    /// grows resident bytes must fit the budget like any other admission
    /// (a shrinking steal always may proceed). `None` when the bounded scan
    /// finds no such victim, or none whose growth the budget can absorb.
    ///
    /// The caller holds its own chunk's state lock. The scan follows the
    /// enforcement discipline (deepest band first, queue guard dropped
    /// before any chunk lock, victims only ever `try_lock`ed), which is
    /// what keeps the chunk-lock-while-probing-chunk-lock window
    /// deadlock-free: two admitters stealing toward each other both fail
    /// the `try_lock` and skip. Unlike enforcement, the scan rotates
    /// unsuitable entries (touched, wrong class, unbacked) to the back
    /// without spending touched bits, shuffling FIFO order the way the
    /// backing scan does.
    fn steal_clean_victim(&self, class: usize, admitted_len_bytes: usize) -> Option<u32> {
        // Bound on entries examined per band before moving to the next.
        // The budget is per band, not shared across the scan: a deep band
        // densely populated with touched resident chunks (a probe-heavy
        // arrangement whose reads refresh every bit) would otherwise spend
        // the entire scan on hopeless candidates and starve the shallower
        // bands where the clean-victim stock actually sits (eager backing
        // stocks young backed chunks in band zero). Eight visits per band
        // absorb a handful of lock-busy or freshly touched entries without
        // degrading a hopeless scan into a full queue walk, and cap the
        // whole scan at eight times the band count.
        const VISITS_PER_BAND: usize = 8;
        for band in (0..DEPTH_BANDS).rev() {
            let mut visits = VISITS_PER_BAND;
            while visits > 0 {
                let popped = self.queue(band).pop_front();
                let Some(weak) = popped else {
                    // Band exhausted; the next band has its own budget.
                    break;
                };
                let Some(meta) = weak.upgrade() else {
                    // Stale entries drop for free and do not spend a visit.
                    continue;
                };
                visits -= 1;
                let Ok(mut state) = meta.state.try_lock() else {
                    self.queue(band).push_back(weak);
                    continue;
                };
                if state.freed {
                    continue;
                }
                match state.residency {
                    // Entries for non-resident chunks drop, as in
                    // enforcement.
                    Residency::Evicted | Residency::Oversize => continue,
                    Residency::UnbackedResident | Residency::WriteInFlight => {
                        self.queue(band).push_back(weak);
                        continue;
                    }
                    Residency::BackedResident => {}
                }
                if state.touched || meta.class != Some(class) {
                    self.queue(band).push_back(weak);
                    continue;
                }
                // Settle the ledger in one step: the victim's bytes out, the
                // admitted payload's in. A steal that grows resident bytes
                // is an admission and must fit the budget (against evictable
                // bytes, as everywhere); a shrinking steal always may
                // proceed. On failure the victim is requeued untouched.
                let victim_len = u64::cast_from(meta.len_bytes());
                let admitted_len = u64::cast_from(admitted_len_bytes);
                let settled = self
                    .counters
                    .resident_bytes
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                        let next = cur.checked_add(admitted_len)?.saturating_sub(victim_len);
                        let oversize = self.counters.oversize_bytes.load(Ordering::Relaxed);
                        (next <= cur
                            || next.saturating_sub(oversize)
                                <= self.budget_bytes.load(Ordering::Relaxed))
                        .then_some(next)
                    })
                    .is_ok();
                if !settled {
                    self.queue(band).push_back(weak);
                    continue;
                }
                let slot = state.slot.take().expect("backed chunk has a slot");
                state.residency = Residency::Evicted;
                return Some(slot);
            }
        }
        None
    }

    /// Capacity of the compressed-resident tier: the RSS target's headroom
    /// above the slot budget and the warm cap. With no target set the tier
    /// has zero capacity, so extents page out as soon as they are written.
    fn compressed_cap(&self) -> u64 {
        let target = self.rss_target_bytes.load(Ordering::Relaxed);
        let floor = self
            .budget_bytes
            .load(Ordering::Relaxed)
            .saturating_add(self.warm_cap());
        target.saturating_sub(floor)
    }

    /// Counts a newly resident extent (written, or revived by a read)
    /// against the compressed tier. A reclaimable extent additionally
    /// enqueues its chunk for RSS-target enforcement; an unreclaimable one
    /// (a heap-fallback extent, which is never advised out) counts against
    /// the unreclaimable gauge instead and stays out of the queue, so
    /// enforcement never walks entries it cannot act on. Callers hold the
    /// chunk's state lock with the extent present and resident, and follow
    /// up with [`PoolInner::enforce_compressed_cap`] once the lock is
    /// released.
    ///
    /// Invariant: `extent_resident_bytes` equals the sum of `alloc_size`
    /// over live chunks' extents whose `is_resident()` is true, and
    /// `extent_residents` counts those extents; `extent_unreclaimable_bytes`
    /// is the subset whose `pageout_capped()` is true. This method,
    /// [`PoolInner::note_extent_reclaimable`],
    /// [`PoolInner::note_extent_released`], and the pageout arms in
    /// [`PoolInner::enforce_compressed_cap`] are the only adjusters; every
    /// flag flip pairs with one of them under the chunk's state lock.
    fn note_extent_resident(&self, meta: &Arc<ChunkMeta>, extent_alloc: usize, reclaimable: bool) {
        self.counters
            .extent_resident_bytes
            .fetch_add(u64::cast_from(extent_alloc), Ordering::Relaxed);
        self.extent_residents.fetch_add(1, Ordering::Relaxed);
        if reclaimable {
            self.prune_extent_queue();
            self.extent_queue().push_back(Arc::downgrade(meta));
        } else {
            self.counters
                .extent_unreclaimable_bytes
                .fetch_add(u64::cast_from(extent_alloc), Ordering::Relaxed);
        }
    }

    /// Returns a retry-capped resident extent to the reclaimable set after a
    /// read restored its pageout budget: uncounts it from the unreclaimable
    /// gauge and re-enqueues its chunk for RSS-target enforcement. The
    /// caller holds the chunk's state lock with the extent present, resident,
    /// and no longer `pageout_capped()`.
    fn note_extent_reclaimable(&self, meta: &Arc<ChunkMeta>, extent_alloc: usize) {
        self.counters
            .extent_unreclaimable_bytes
            .fetch_sub(u64::cast_from(extent_alloc), Ordering::Relaxed);
        self.prune_extent_queue();
        self.extent_queue().push_back(Arc::downgrade(meta));
    }

    /// Uncounts a resident extent that is being dropped (chunk freed or
    /// degraded). Its queue entry goes stale and is dropped on visit or by
    /// [`PoolInner::prune_extent_queue`].
    fn note_extent_released(&self, extent: &SwapExtent) {
        if extent.is_resident() {
            self.counters
                .extent_resident_bytes
                .fetch_sub(u64::cast_from(extent.alloc_size()), Ordering::Relaxed);
            self.extent_residents.fetch_sub(1, Ordering::Relaxed);
            if extent.pageout_capped() {
                self.counters
                    .extent_unreclaimable_bytes
                    .fetch_sub(u64::cast_from(extent.alloc_size()), Ordering::Relaxed);
            }
        }
    }

    /// Drops extent-queue entries whose chunk has been freed, mirroring
    /// [`PoolInner::prune_queues`]: compact only when the queue outgrows
    /// all live resident extents (plus a small floor), so the cost
    /// amortizes to a constant per push. Enforcement drops stale entries
    /// too, but only while the tier is over capacity. A pool that stays
    /// under its compressed cap would otherwise accumulate an entry (and a
    /// pin on the dead chunk's allocation) per freed extent forever.
    fn prune_extent_queue(&self) {
        let live = usize::cast_from(self.extent_residents.load(Ordering::Relaxed));
        let mut queue = self.extent_queue();
        if queue.len() > 2 * live + 16 {
            queue.retain(|weak| weak.strong_count() > 0);
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
    ///
    /// Deferral tests for thread existence alone, not `spill.enabled`:
    /// spawned threads trim the tier in their loop for as long as they live,
    /// even with eviction hand-off disabled, so they remain the better home
    /// for the pageouts.
    fn enforce_or_defer_compressed_cap(&self) {
        if self.spill.threads.load(Ordering::Relaxed) > 0 {
            // The inline backstop keys on the bytes enforcement can actually
            // reclaim. Unreclaimable extents (retry-capped, heap-backed)
            // would otherwise hold the backstop permanently over threshold
            // and put a full enforcement pass on every caller.
            let resident = self.counters.extent_resident_bytes.load(Ordering::Relaxed);
            let unreclaimable = self
                .counters
                .extent_unreclaimable_bytes
                .load(Ordering::Relaxed);
            if resident.saturating_sub(unreclaimable) > self.compressed_cap().saturating_mul(2) {
                self.enforce_compressed_cap();
            } else {
                self.spill.cv.notify_one();
            }
        } else {
            self.enforce_compressed_cap();
        }
    }

    /// Pages out the oldest resident extents until the compressed tier falls
    /// to its capacity. The compression is already paid and the device write
    /// is the kernel's async writeback, so each pageout is one bounded
    /// madvise plus a page-table observation; spill threads run this between
    /// jobs, and other threads only when no spill threads exist (see
    /// [`PoolInner::enforce_or_defer_compressed_cap`]). Not single-flighted:
    /// concurrent passes pop disjoint victims. Visits are bounded by the
    /// queue's length at entry; stale entries (extent paged out, dropped, or
    /// chunk dead) are dropped. Incomplete extents are requeued with their
    /// accounting intact until their retry budget runs out, at which point
    /// they leave the queue with their bytes on the unreclaimable gauge, so
    /// the tier may settle above its capacity by the bytes the kernel
    /// declined to reclaim without enforcement re-walking them.
    fn enforce_compressed_cap(&self) {
        let cap = self.compressed_cap();
        let resident = |c: &Counters| c.extent_resident_bytes.load(Ordering::Relaxed);
        // Under-cap is the common case: answer it with one atomic load and
        // no queue lock, so frequent callers (the spill loop) stay cheap.
        if resident(&self.counters) <= cap {
            return;
        }
        let mut remaining = self.extent_queue().len();
        while remaining > 0 && resident(&self.counters) > cap {
            remaining -= 1;
            let popped = self.extent_queue().pop_front();
            let Some(weak) = popped else {
                break;
            };
            let Some(meta) = weak.upgrade() else {
                continue;
            };
            // `try_lock`: a chunk mid-read or mid-compression holds its lock
            // for milliseconds; requeue rather than convoy behind it.
            let Ok(mut state) = meta.state.try_lock() else {
                self.extent_queue().push_back(weak);
                continue;
            };
            match &mut state.extent {
                Some(extent) if extent.is_resident() => {
                    if extent.pageout_capped() {
                        // A leftover entry for an already-capped extent (its
                        // capping transition below accounted it and dropped
                        // its entry): drop this one too. The read that
                        // restores the retry budget re-enqueues the chunk.
                    } else if extent.pageout() {
                        self.counters
                            .extent_resident_bytes
                            .fetch_sub(u64::cast_from(extent.alloc_size()), Ordering::Relaxed);
                        self.extent_residents.fetch_sub(1, Ordering::Relaxed);
                        self.counters
                            .extent_pageouts
                            .fetch_add(1, Ordering::Relaxed);
                    } else {
                        // The advice left pages resident. The extent keeps
                        // its full accounting (the ledger may over-count
                        // RSS, the safe direction).
                        self.counters
                            .extent_pageout_incomplete
                            .fetch_add(1, Ordering::Relaxed);
                        if extent.pageout_capped() {
                            // The retry budget just ran out: the extent
                            // leaves the queue and its bytes move to the
                            // unreclaimable gauge, so enforcement and the
                            // inline backstop stop chasing memory the kernel
                            // will not give back. A read that restores the
                            // budget re-counts and re-enqueues it.
                            self.counters
                                .extent_unreclaimable_bytes
                                .fetch_add(u64::cast_from(extent.alloc_size()), Ordering::Relaxed);
                        } else {
                            // Budget remains: keep the queue slot so later
                            // passes retry it up to the cap.
                            self.extent_queue().push_back(weak);
                        }
                    }
                }
                // Paged out already or dropped: the entry is stale. A later
                // resident event re-enqueues.
                _ => {}
            }
        }
    }

    /// If the chunk is a live `UnbackedResident` holding a slot and the
    /// spill threads have capacity, transitions it to `WriteInFlight` and
    /// hands it to them, returning `true`. The hand-off happens under the
    /// held state lock; the spill thread blocks on that lock only after this
    /// call returns and the caller releases it.
    fn spill_handoff(&self, meta: &Arc<ChunkMeta>, state: &mut ChunkState) -> bool {
        // The slot check excludes empty chunks, which are `UnbackedResident`
        // without a slot: handing one off would panic the spill thread on
        // the missing slot.
        if state.residency != Residency::UnbackedResident
            || state.freed
            || state.slot.is_none()
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
    /// Test hook: the chunk's current residency state.
    #[cfg(test)]
    fn residency(&self) -> Residency {
        self.meta.state().residency
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
    /// is no reader it could race. The admitting variant is
    /// [`ChunkHandle::read_into_admit`].
    pub fn read_into(&self, dst: &mut Vec<u64>) {
        self.read_impl(0..self.meta.len, dst, false);
    }

    /// As [`ChunkHandle::read_into`], restricted to the word range `range`
    /// of the chunk's contents, which must lie within them. `dst` receives
    /// exactly the range.
    ///
    /// The range narrows only the copy into `dst`: the swap backend's
    /// stored form is a whole compressed block, so a cold read still
    /// faults and decompresses the entire extent, and accounting is that
    /// of a whole-chunk read. A backend with a rangeable stored form can
    /// serve the same call with range-proportional I/O.
    pub fn read_range_into(&self, range: Range<usize>, dst: &mut Vec<u64>) {
        self.read_impl(range, dst, false);
    }

    /// As [`ChunkHandle::read_into`], except that an evicted chunk is
    /// re-admitted to `BackedResident` (its extent kept, its touched bit
    /// set) when a slot is available from free budget headroom or by
    /// stealing from a clean backed victim of the same size class, never by
    /// evicting or compressing anything. When neither source yields a slot
    /// the read is served as a plain decompress and the chunk stays
    /// evicted.
    ///
    /// For demand reads on probe paths, where the same chunk is likely to
    /// be read again. Merge, drain, and other consume-once paths should use
    /// [`ChunkHandle::read_into`] or [`ChunkHandle::take`]: admitting there
    /// churns the clean-victim stock that eager backing exists to build,
    /// evicting probe targets to house data about to die.
    pub fn read_into_admit(&self, dst: &mut Vec<u64>) {
        self.read_impl(0..self.meta.len, dst, true);
    }

    /// As [`ChunkHandle::read_into_admit`], restricted to the word range
    /// `range` per [`ChunkHandle::read_range_into`]. Admission is
    /// whole-chunk regardless of the range: the acquired slot holds the
    /// entire body.
    pub fn read_range_into_admit(&self, range: Range<usize>, dst: &mut Vec<u64>) {
        self.read_impl(range, dst, true);
    }

    /// Shared body of the copy-out reads: fills `dst` with the word range
    /// `range` of the chunk's contents under the chunk's state lock,
    /// re-admitting an evicted chunk when `admit` is set and a slot is
    /// available. An empty range returns without locking or touching the
    /// chunk, like the whole-chunk read of an empty chunk always has.
    fn read_impl(&self, range: Range<usize>, dst: &mut Vec<u64>, admit: bool) {
        dst.clear();
        let meta = &*self.meta;
        assert!(
            range.start <= range.end && range.end <= meta.len,
            "range {range:?} exceeds the chunk's {} words",
            meta.len,
        );
        if range.is_empty() {
            return;
        }
        let mut state = meta.state();
        state.touched = true;
        let mut extent_revived = false;
        match state.residency {
            Residency::Oversize => {
                let payload = state.oversize.as_ref().expect("oversize chunk has payload");
                dst.extend_from_slice(&payload[range]);
            }
            Residency::Evicted => {
                let slot = if admit {
                    meta.pool.admit_slot(meta)
                } else {
                    None
                };
                let extent = state.extent.as_mut().expect("evicted chunk has an extent");
                // Reading faults the extent's pages back in either way, so
                // it is re-counted against the compressed tier below.
                let was_resident = extent.is_resident();
                let was_capped = extent.pageout_capped();
                let extent_alloc = extent.alloc_size();
                match slot {
                    Some(slot) => {
                        // Admission: the extent decompresses straight into
                        // the acquired slot, fully overwriting its
                        // unspecified prior contents, and the caller's
                        // buffer is filled from the slot.
                        let region = meta.pool.region_of(meta);
                        // SAFETY: the slot was acquired for this chunk
                        // under its held state lock (freshly allocated, or
                        // transferred from the victim under the victim's
                        // lock), so it is exclusively owned with no other
                        // reference into it, and `len_bytes` fits the
                        // class.
                        let slot_bytes = unsafe {
                            std::slice::from_raw_parts_mut(region.slot_ptr(slot), meta.len_bytes())
                        };
                        extent.read_into(meta.codec, slot_bytes);
                        state.slot = Some(slot);
                        state.residency = Residency::BackedResident;
                        // SAFETY: the slot belongs to this chunk while the
                        // state lock is held (eviction and free both take
                        // it).
                        let src = unsafe { meta.pool.slot_data(meta, slot) };
                        dst.extend_from_slice(&src[range.start..range.end]);
                        // Resident again: rejoin the eviction candidates.
                        // A leftover entry from before the chunk's eviction
                        // stays sound (each entry is validated against the
                        // chunk's state on visit) but costs policy: two live
                        // entries give the enforcer two chances to spend
                        // this chunk's single touched bit, halving its
                        // second chance until one entry drains.
                        meta.pool
                            .queue(band(meta.depth))
                            .push_back(Arc::downgrade(&self.meta));
                    }
                    None => {
                        // The zero-fill ahead of the decompress is deliberate
                        // waste (~a tenth of the decompress cost): the extent
                        // read takes an initialized `&mut [u8]`, so skipping
                        // the fill would mean exposing uninitialized memory
                        // through a safe reference. Callers that read
                        // repeatedly amortize it by reusing `dst` within a
                        // pass, shrinking it back after oversized reads (a
                        // reused buffer otherwise ratchets to the largest
                        // chunk it ever carried, invisible to every pool
                        // gauge; the design doc's reader discipline).
                        dst.resize(range.end - range.start, 0);
                        let bytes: &mut [u8] = bytemuck::cast_slice_mut(dst.as_mut_slice());
                        extent.read_range_into(
                            meta.codec,
                            meta.len_bytes(),
                            range.start * 8,
                            bytes,
                        );
                    }
                }
                // TODO: a sub-range read of a rangeable stored form (file
                // extents, a sub-block-framed codec) revives only part of
                // the extent; the whole-extent accounting below would then
                // overcount and needs a partial-revival variant.
                if !was_resident {
                    // Revived from the device: the decompress reset any
                    // retry budget, so the extent re-enters reclaimable.
                    meta.pool
                        .note_extent_resident(&self.meta, extent_alloc, true);
                    extent_revived = true;
                } else if was_capped {
                    // The decompress faulted every page and reset the
                    // pageout retry budget, so a retry-capped extent is
                    // reclaimable again. Heap-backed extents stay
                    // structurally capped and stay out of the queue.
                    let capped = state
                        .extent
                        .as_ref()
                        .expect("evicted chunk has an extent")
                        .pageout_capped();
                    if !capped {
                        meta.pool.note_extent_reclaimable(&self.meta, extent_alloc);
                        extent_revived = true;
                    }
                }
            }
            Residency::UnbackedResident | Residency::BackedResident | Residency::WriteInFlight => {
                let slot = state.slot.expect("resident non-empty chunk has a slot");
                // SAFETY: the slot belongs to this chunk while the state lock
                // is held (eviction and free both take it).
                let src = unsafe { meta.pool.slot_data(meta, slot) };
                dst.extend_from_slice(&src[range.start..range.end]);
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

    /// Copies the whole contents into `dst` (per [`ChunkHandle::read_into`],
    /// never admitting) and frees the chunk, cancelling any in-flight
    /// backing write.
    pub fn take(self, dst: &mut Vec<u64>) {
        self.read_into(dst);
    }

    /// Advisory a consumer may issue before a bulk read: hints the kernel to
    /// swap an evicted chunk's extent back in, and is a no-op in every other
    /// state. Never blocks on I/O (`MADV_WILLNEED` is asynchronous).
    pub fn prefetch(&self) {
        let state = self.meta.state();
        if state.residency == Residency::Evicted {
            let extent = state.extent.as_ref().expect("evicted chunk has an extent");
            extent.prefetch();
        }
    }

    /// As [`ChunkHandle::prefetch`], scoped to the word range `range` of the
    /// chunk's contents. The range is advisory: a backend hints at whatever
    /// granularity its stored form permits, and the swap backend's stored
    /// form is a whole compressed block, so it hints the entire extent.
    pub fn prefetch_range(&self, range: Range<usize>) {
        let _ = range;
        self.prefetch();
    }

    /// Test hook: the byte size of the chunk's size class, or `None` for
    /// empty and oversize chunks.
    #[cfg(test)]
    fn size_class_bytes(&self) -> Option<usize> {
        self.meta.class.map(|class| SIZE_CLASSES[class])
    }
}

impl Drop for ChunkHandle {
    fn drop(&mut self) {
        let pool = &self.meta.pool;
        let mut state = self.meta.state();
        pool.counters.frees.fetch_add(1, Ordering::Relaxed);
        state.freed = true;
        if self.meta.class.is_some() {
            pool.live_chunks.fetch_sub(1, Ordering::Relaxed);
        }
        let len_bytes = u64::cast_from(self.meta.len_bytes());
        // `release_slot`'s precondition holds in every arm below: the handle
        // is being dropped, so no copy-out read (which borrows the handle)
        // is in progress, and `freed` was set under the state lock held
        // here, so concurrent queue visitors skip the chunk.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::extent::TEST_CODEC;

    /// Keep test pools small: 64 MiB of virtual reservation per class.
    /// Under Miri the backing is real interpreter heap rather than lazy
    /// virtual memory, so shrink further. Classes above the capacity yield
    /// empty regions whose inserts degrade to the heap fallback, which is
    /// fine: slotted-chunk tests exercise only the smallest classes.
    fn test_pool(budget_bytes: usize) -> Pool {
        let capacity = if cfg!(miri) { 1 << 20 } else { 64 << 20 };
        let pool = Pool::with_class_capacity(capacity).expect("pool creation");
        pool.set_budget(budget_bytes);
        pool
    }

    /// Scales an iteration count down under Miri, where one interpreted
    /// compression costs what thousands do natively.
    fn rounds(native: u64, miri: u64) -> u64 {
        if cfg!(miri) { miri } else { native }
    }

    fn payload(words: usize, seed: u64) -> Vec<u64> {
        (0..u64::cast_from(words))
            .map(|i| seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(i))
            .collect()
    }

    /// Copies `data` into the pool and clears it.
    fn insert(pool: &Pool, data: &mut Vec<u64>) -> ChunkHandle {
        insert_at_depth(pool, 0, data)
    }

    /// Copies `data` into the pool at a hinted depth and clears it.
    fn insert_at_depth(pool: &Pool, depth: u8, data: &mut Vec<u64>) -> ChunkHandle {
        let hints = ChunkHints { depth };
        let handle = pool.insert_with(data.len(), hints, &TEST_CODEC, |dst| {
            dst.copy_from_slice(data.as_slice())
        });
        data.clear();
        handle
    }

    /// Copies a chunk's contents out into a fresh buffer.
    fn read(handle: &ChunkHandle) -> Vec<u64> {
        let mut out = Vec::new();
        handle.read_into(&mut out);
        out
    }

    /// Copies a chunk's contents out into a fresh buffer via the admitting
    /// read.
    fn read_admit(handle: &ChunkHandle) -> Vec<u64> {
        let mut out = Vec::new();
        handle.read_into_admit(&mut out);
        out
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
    fn compressed_tier_round_trip() {
        let pool = test_pool(256 << 20);
        pool.set_rss_target(1 << 30);
        let orig = payload(SMALL, 21);
        let handle = insert(&pool, &mut orig.clone());
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

        // Reading revives the extent: contents round-trip, the chunk stays
        // evicted, and with the target restored the revived extent is
        // counted again.
        pool.set_rss_target(1 << 30);
        assert_eq!(read(&handle), orig);
        assert_eq!(handle.residency(), Residency::Evicted);
        assert!(
            pool.stats().extent_resident_bytes > 0,
            "revived and counted"
        );

        // Dropping the handle uncounts the resident extent.
        drop(handle);
        assert_eq!(pool.stats().extent_resident_bytes, 0);
    }

    /// A ranged read returns exactly the corresponding slice of a
    /// whole-chunk read in every residency state, and changes residency
    /// exactly as the equivalent whole-chunk read would.
    #[mz_ore::test]
    fn ranged_reads_match_full_read_slice() {
        let pool = test_pool(256 << 20);
        pool.set_rss_target(1 << 30);
        let orig = payload(SMALL, 33);
        let handle = insert(&pool, &mut orig.clone());
        let ranges = [
            (0usize, 7usize),
            (13, 100),
            (SMALL - 9, 9),
            (0, SMALL),
            (5, 0),
        ];
        let check = |label: &str| {
            for (start, len) in ranges {
                let mut out = Vec::new();
                handle.read_range_into(start..start + len, &mut out);
                assert_eq!(
                    out,
                    &orig[start..start + len],
                    "{label} range ({start}, {len})"
                );
            }
        };
        assert_eq!(handle.residency(), Residency::UnbackedResident);
        check("resident");
        pool.evict(&handle);
        assert_eq!(handle.residency(), Residency::Evicted);
        check("evicted");
        assert_eq!(
            handle.residency(),
            Residency::Evicted,
            "plain ranged reads do not admit"
        );
        // An admitting ranged read returns the range and admits the whole
        // chunk.
        let mut out = Vec::new();
        handle.read_range_into_admit(3..19, &mut out);
        assert_eq!(out, &orig[3..19]);
        assert_eq!(handle.residency(), Residency::BackedResident);
        check("backed");
    }

    #[mz_ore::test]
    #[should_panic(expected = "exceeds the chunk's")]
    fn ranged_read_out_of_bounds_panics() {
        let pool = test_pool(256 << 20);
        let handle = insert(&pool, &mut payload(SMALL, 34));
        let mut out = Vec::new();
        handle.read_range_into(SMALL - 1..SMALL + 1, &mut out);
    }

    /// With no RSS target (the default), the compressed tier has zero
    /// capacity and extents page out as soon as they are written.
    #[mz_ore::test]
    fn default_target_pages_extents_immediately() {
        let pool = test_pool(256 << 20);
        let handle = insert(&pool, &mut payload(SMALL, 22));
        pool.evict(&handle);
        let stats = pool.stats();
        assert_eq!(stats.extent_resident_bytes, 0);
        assert_eq!(stats.extent_pageouts, 1);
    }

    /// A pageout pass whose residency observation finds the whole range
    /// gone uncounts exactly the extent's allocation bytes.
    #[mz_ore::test]
    fn full_pageout_uncounts_exactly_the_extent() {
        let pool = test_pool(256 << 20);
        pool.set_rss_target(1 << 30);
        let handle = insert(&pool, &mut payload(SMALL, 50));
        pool.evict(&handle);
        let counted = pool.stats().extent_resident_bytes;
        assert!(counted > 0, "under the target, the extent stays counted");
        pool.set_rss_target(0);
        let stats = pool.stats();
        assert_eq!(stats.extent_resident_bytes, 0, "exactly `counted` left");
        assert_eq!(stats.extent_pageouts, 1);
        assert_eq!(stats.extent_pageout_incomplete, 0);
    }

    /// A pageout pass the kernel declines leaves the extent fully counted
    /// and queued, and increments the incomplete counter. The next pass
    /// (advice now accepted) pages it out.
    #[mz_ore::test]
    fn incomplete_pageout_keeps_accounting_and_queue_position() {
        let pool = test_pool(256 << 20);
        pool.set_rss_target(1 << 30);
        let handle = insert(&pool, &mut payload(SMALL, 51));
        pool.evict(&handle);
        let counted = pool.stats().extent_resident_bytes;
        assert!(counted > 0);
        region::fake_residency::decline_next(1);
        pool.set_rss_target(0);
        let stats = pool.stats();
        assert_eq!(
            stats.extent_resident_bytes, counted,
            "full accounting stays"
        );
        assert_eq!(stats.extent_pageouts, 0);
        assert_eq!(stats.extent_pageout_incomplete, 1);
        assert_eq!(handle.residency(), Residency::Evicted);
        // The requeued entry is retried by the next enforcement pass.
        pool.enforce_rss_target();
        let stats = pool.stats();
        assert_eq!(stats.extent_resident_bytes, 0);
        assert_eq!(stats.extent_pageouts, 1);
        assert_eq!(stats.extent_pageout_incomplete, 1);
    }

    /// A never-reclaimable extent stops being advised after the retry cap:
    /// the incomplete counter stops climbing, the bytes stay counted
    /// resident, and the tier keeps paging other extents out around it.
    #[mz_ore::test]
    fn pageout_retry_cap_stops_advising() {
        let pool = test_pool(256 << 20);
        let handle = insert(&pool, &mut payload(SMALL, 52));
        region::fake_residency::decline_next(u64::MAX);
        // RSS target zero: the eviction's enforcement pass advises at once.
        pool.evict(&handle);
        for _ in 0..5 {
            pool.enforce_rss_target();
        }
        let stats = pool.stats();
        assert_eq!(
            stats.extent_pageout_incomplete,
            u64::from(extent::PAGEOUT_RETRY_CAP),
            "advised exactly retry-cap times, then left alone",
        );
        assert_eq!(stats.extent_pageouts, 0);
        let counted = stats.extent_resident_bytes;
        assert!(counted > 0, "capped extent stays counted resident");
        // The tier functions around the capped extent: a fresh extent still
        // pages out.
        region::fake_residency::decline_next(0);
        let other = insert(&pool, &mut payload(SMALL, 53));
        pool.evict(&other);
        let stats = pool.stats();
        assert_eq!(stats.extent_pageouts, 1);
        assert_eq!(
            stats.extent_resident_bytes, counted,
            "only the capped extent remains counted",
        );
        assert_eq!(read(&handle).len(), SMALL, "capped extent stays readable");
    }

    /// Reading a retry-capped extent faults its pages back in and resets
    /// the retry budget, so a later pass can page it out.
    #[mz_ore::test]
    fn read_resets_pageout_retry_budget() {
        let pool = test_pool(256 << 20);
        let orig = payload(SMALL, 54);
        let handle = insert(&pool, &mut orig.clone());
        region::fake_residency::decline_next(u64::MAX);
        pool.evict(&handle);
        for _ in 0..4 {
            pool.enforce_rss_target();
        }
        assert_eq!(
            pool.stats().extent_pageout_incomplete,
            u64::from(extent::PAGEOUT_RETRY_CAP),
            "capped",
        );
        assert!(pool.stats().extent_resident_bytes > 0);
        region::fake_residency::decline_next(0);
        assert_eq!(read(&handle), orig);
        pool.enforce_rss_target();
        let stats = pool.stats();
        assert_eq!(stats.extent_pageouts, 1, "the budget reset re-advised it");
        assert_eq!(stats.extent_resident_bytes, 0);
        // The paged-out extent still round-trips.
        assert_eq!(read(&handle), orig);
    }

    /// Eager backing compresses a chunk to `BackedResident` while it stays
    /// readable in its slot; the later budget-driven eviction is a pure page
    /// release, and the contents round-trip through the extent.
    #[mz_ore::test]
    fn eager_backing_round_trip() {
        let pool = test_pool(256 << 20);
        let orig = payload(SMALL, 11);
        let handle = insert(&pool, &mut orig.clone());
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

        // Still readable straight from the slot: the chunk is resident.
        assert_eq!(read(&handle), orig);

        // The pre-paid eviction is cheap, and the extent round-trips.
        pool.evict(&handle);
        assert_eq!(handle.residency(), Residency::Evicted);
        assert_eq!(pool.stats().evictions_cheap, 1);
        pool.poison_free_slots();
        assert_eq!(read(&handle), orig);
    }

    /// Once everything reachable is backed, the backing scan reports no
    /// progress so spill threads park instead of rescanning a fully-backed
    /// queue forever.
    #[mz_ore::test]
    fn backing_reports_no_progress_when_all_backed() {
        let pool = test_pool(256 << 20);
        let _handle = insert(&pool, &mut payload(SMALL, 31));
        assert!(pool.back_step(), "one unbacked chunk is actionable");
        assert!(
            !pool.back_step(),
            "a fully-backed queue is not progress; callers must park",
        );
        assert_eq!(pool.stats().eager_backs, 1);
    }

    /// Freeing under the warm cap parks the slot warm; the next insert of the
    /// same class reuses it fault-free and the accounting balances.
    #[mz_ore::test]
    fn warm_slot_reuse() {
        // Budget 8 MiB: warm cap = 1 MiB, so a 64 KiB slot fits warm.
        let pool = test_pool(8 << 20);
        let orig = payload(SMALL, 7);
        let handle = insert(&pool, &mut orig.clone());
        drop(handle);
        let after_free = pool.stats();
        assert_eq!(after_free.warm_bytes, 64 << 10, "freed slot parks warm");
        assert_eq!(after_free.warm_reuses, 0);

        let handle = insert(&pool, &mut orig.clone());
        let after_reuse = pool.stats();
        assert_eq!(after_reuse.warm_reuses, 1, "second insert reuses warm slot");
        assert_eq!(after_reuse.warm_bytes, 0, "reuse drains the warm pool");
        // Contents are correct despite the skipped page release.
        assert_eq!(read(&handle), orig);
    }

    /// The warm pool is capped at an eighth of the budget; frees beyond the
    /// cap release their pages and park cold.
    #[mz_ore::test]
    fn warm_pool_respects_cap() {
        // Budget 1 MiB: warm cap = 128 KiB = two 64 KiB slots.
        let pool = test_pool(1 << 20);
        let handles: Vec<_> = (0..4)
            .map(|seed| insert(&pool, &mut payload(SMALL, seed)))
            .collect();
        drop(handles);
        let stats = pool.stats();
        assert_eq!(
            stats.warm_bytes,
            128 << 10,
            "warm pool stops at the budget/8 cap",
        );
    }

    /// A kernel that keeps declining the reclaim advice caps the extent's
    /// retry budget: the extent leaves the enforcement queue and moves to
    /// the unreclaimable gauge, so enforcement stops walking it, and a read
    /// that restores the budget makes it reclaimable and pageable again.
    #[mz_ore::test]
    fn capped_extents_leave_the_enforcement_queue() {
        let pool = test_pool(256 << 20);
        let orig = payload(SMALL, 960);
        let handle = insert(&pool, &mut orig.clone());
        // Decline every observation: eviction's own enforcement pass plus
        // the passes below spend the whole retry budget.
        region::fake_residency::decline_next(u64::from(extent::PAGEOUT_RETRY_CAP));
        pool.evict(&handle);
        for _ in 0..extent::PAGEOUT_RETRY_CAP {
            pool.enforce_compressed();
        }
        let stats = pool.stats();
        assert_eq!(
            stats.extent_pageout_incomplete,
            u64::from(extent::PAGEOUT_RETRY_CAP),
        );
        assert!(stats.extent_unreclaimable_bytes > 0, "capped bytes counted");
        assert_eq!(pool.extent_queue_len(), 0, "capped extents leave the queue",);
        // Further enforcement is a no-op: nothing queued, no advice spent.
        pool.enforce_compressed();
        assert_eq!(
            pool.stats().extent_pageout_incomplete,
            u64::from(extent::PAGEOUT_RETRY_CAP),
        );

        // A read faults everything back in and restores the retry budget:
        // the extent re-enters the reclaimable set and, with the kernel now
        // cooperating, the read's own enforcement pass pages it out.
        assert_eq!(read(&handle), orig);
        let stats = pool.stats();
        assert_eq!(stats.extent_unreclaimable_bytes, 0, "budget restored");
        assert_eq!(stats.extent_pageouts, 1, "re-enqueued extent pages out");
        assert_eq!(stats.extent_resident_bytes, 0);
        assert_eq!(pool.extent_queue_len(), 0);
    }

    /// Shrinking the budget cools warm slots parked under the old, larger
    /// cap: their pages are released and `warm_bytes` falls to the new cap
    /// on the shrink itself, not on eventual same-class reuse.
    #[mz_ore::test]
    fn budget_shrink_trims_warm_pool() {
        // Budget 8 MiB: warm cap 1 MiB, so four 64 KiB frees all park warm.
        let pool = test_pool(8 << 20);
        let handles: Vec<_> = (0..4)
            .map(|seed| insert(&pool, &mut payload(SMALL, 950 + seed)))
            .collect();
        drop(handles);
        assert_eq!(pool.stats().warm_bytes, 4 * (64 << 10));

        // Budget 1 MiB: warm cap 128 KiB, so two of the four slots cool.
        pool.set_budget(1 << 20);
        assert_eq!(pool.stats().warm_bytes, 128 << 10);
    }

    #[mz_ore::test]
    fn round_trip_resident() {
        let pool = test_pool(256 << 20);
        let orig = payload(1000, 1);
        let mut data = orig.clone();
        let capacity = data.capacity();
        let handle = insert(&pool, &mut data);
        assert!(data.is_empty());
        assert_eq!(data.capacity(), capacity, "insert preserves capacity");
        assert_eq!(handle.residency(), Residency::UnbackedResident);
        assert_eq!(read(&handle), orig);
        drop(handle);
        let stats = pool.stats();
        assert_eq!(stats.inserts, 1);
        assert_eq!(stats.frees, 1);
        assert_eq!(stats.resident_bytes, 0);
    }

    /// `take` is the terminal read: the contents come back and the consumed
    /// handle frees the chunk, eliding the backing write of a chunk that was
    /// still unbacked.
    #[mz_ore::test]
    fn take_reads_and_frees() {
        let pool = test_pool(256 << 20);
        let orig = payload(SMALL, 40);
        let handle = insert(&pool, &mut orig.clone());
        let mut out = Vec::new();
        handle.take(&mut out);
        assert_eq!(out, orig);
        let stats = pool.stats();
        assert_eq!(stats.frees, 1);
        assert_eq!(stats.writes_elided, 1, "a resident take never writes");
        assert_eq!(stats.resident_bytes, 0);
        assert_eq!(stats.live_chunks, 0);
    }

    /// `prefetch` is safe wherever it lands: on a resident chunk (a no-op),
    /// on an evicted chunk (whose read then round-trips), and issued with no
    /// read following it. It never changes residency or resident bytes.
    #[mz_ore::test]
    fn prefetch_is_safe_in_every_state() {
        let pool = test_pool(256 << 20);
        let orig = payload(SMALL, 41);
        let handle = insert(&pool, &mut orig.clone());
        handle.prefetch();
        assert_eq!(handle.residency(), Residency::UnbackedResident);
        assert_eq!(read(&handle), orig);
        pool.evict(&handle);
        handle.prefetch();
        assert_eq!(handle.residency(), Residency::Evicted);
        assert_eq!(read(&handle), orig);
        // An advisory with no read behind it leaves nothing to clean up.
        let idle = insert(&pool, &mut payload(SMALL, 42));
        idle.prefetch();
        drop(idle);
        drop(handle);
        assert_eq!(pool.stats().resident_bytes, 0);
    }

    /// Reading an evicted chunk decompresses its extent straight into the
    /// caller's buffer and leaves the chunk evicted. Free slots are poisoned
    /// first, so a read passing stale slot memory through (the macOS
    /// `MADV_DONTNEED` hazard) would fail the content check.
    #[mz_ore::test]
    fn evict_then_read_preserves_contents() {
        let pool = test_pool(256 << 20);
        let orig = payload(SMALL, 2);
        let handle = insert(&pool, &mut orig.clone());
        pool.evict(&handle);
        assert_eq!(handle.residency(), Residency::Evicted);
        let stats = pool.stats();
        assert_eq!(stats.evictions_compress, 1);
        assert_eq!(stats.resident_bytes, 0);
        assert!(stats.extent_bytes_written > 0);
        pool.poison_free_slots();
        assert_eq!(read(&handle), orig);
        assert_eq!(handle.residency(), Residency::Evicted);
        assert_eq!(pool.stats().resident_bytes, 0, "reads copy out");
    }

    /// An admitting read of an evicted chunk with budget headroom re-admits
    /// it: contents round-trip, the chunk lands `BackedResident` with its
    /// extent kept, and later reads serve from the slot without touching
    /// the extent.
    #[mz_ore::test]
    fn admit_from_free_budget_backs_the_chunk() {
        let pool = test_pool(256 << 20);
        let orig = payload(SMALL, 70);
        let handle = insert(&pool, &mut orig.clone());
        pool.evict(&handle);
        assert_eq!(handle.residency(), Residency::Evicted);
        assert_eq!(pool.stats().resident_bytes, 0);

        pool.poison_free_slots();
        assert_eq!(read_admit(&handle), orig);
        assert_eq!(handle.residency(), Residency::BackedResident);
        let stats = pool.stats();
        assert_eq!(stats.admissions_budget, 1);
        assert_eq!(stats.admissions_steal, 0);
        assert_eq!(stats.admissions_denied, 0);
        assert_eq!(stats.resident_bytes, 64 << 10);

        // Later reads serve from the slot and never touch the extent: a
        // decompress would revive its pages and move the revival and
        // pageout counters.
        let pageouts = stats.extent_pageouts;
        let extent_resident = stats.extent_resident_bytes;
        assert_eq!(read(&handle), orig);
        assert_eq!(handle.residency(), Residency::BackedResident);
        let stats = pool.stats();
        assert_eq!(stats.extent_pageouts, pageouts);
        assert_eq!(stats.extent_resident_bytes, extent_resident);

        // The kept extent pre-pays the next eviction, and round-trips.
        pool.evict(&handle);
        assert_eq!(handle.residency(), Residency::Evicted);
        let stats = pool.stats();
        assert_eq!(stats.evictions_cheap, 1);
        assert_eq!(stats.evictions_compress, 1, "admission wrote no extent");
        pool.poison_free_slots();
        assert_eq!(read(&handle), orig);
        drop(handle);
        assert_eq!(pool.stats().resident_bytes, 0);
    }

    /// With the budget pinned full and a clean backed victim of the same
    /// class, an admitting read steals the victim's slot: the victim is
    /// evicted with zero I/O and its extent intact, the admitted chunk
    /// lands `BackedResident`, and resident bytes, warm bytes, and the
    /// compression and pageout counters are all unchanged.
    #[mz_ore::test]
    fn admit_steals_clean_victim_slot() {
        let pool = test_pool(256 << 20);
        pool.set_rss_target(1 << 30);
        let victim_orig = payload(SMALL, 71);
        let target_orig = payload(SMALL, 72);
        let victim = insert(&pool, &mut victim_orig.clone());
        let target = insert(&pool, &mut target_orig.clone());
        pool.evict(&target);
        assert!(pool.back_step(), "victim is backable");
        assert_eq!(victim.residency(), Residency::BackedResident);
        // The budget now holds exactly the victim: no admission headroom.
        pool.set_budget(64 << 10);
        assert_eq!(victim.residency(), Residency::BackedResident);
        let before = pool.stats();

        assert_eq!(read_admit(&target), target_orig);
        assert_eq!(target.residency(), Residency::BackedResident);
        assert_eq!(victim.residency(), Residency::Evicted);
        let after = pool.stats();
        assert_eq!(after.admissions_steal, 1);
        assert_eq!(after.admissions_budget, 0);
        assert_eq!(after.admissions_denied, 0);
        assert_eq!(
            after.resident_bytes, before.resident_bytes,
            "same class, same bytes",
        );
        assert_eq!(
            after.evictions_compress, before.evictions_compress,
            "no compression",
        );
        assert_eq!(
            after.evictions_cheap, before.evictions_cheap,
            "a steal is not an enforcement eviction",
        );
        assert_eq!(after.extent_bytes_written, before.extent_bytes_written);
        assert_eq!(after.extent_pageouts, 0, "no pageout");
        assert_eq!(
            after.warm_bytes, before.warm_bytes,
            "the stolen slot skipped the free list",
        );
        assert_eq!(after.warm_reuses, before.warm_reuses);

        // The victim's extent is intact: its old slot now holds the
        // admitted chunk's bytes, so a correct read must come from the
        // extent.
        assert_eq!(read(&victim), victim_orig);
        assert_eq!(victim.residency(), Residency::Evicted);

        drop(victim);
        drop(target);
        let stats = pool.stats();
        assert_eq!(stats.resident_bytes, 0);
        assert_eq!(stats.extent_resident_bytes, 0);
    }

    /// With the budget full and every candidate touched, the admitting read
    /// still returns correct data, the chunk stays evicted, and the denial
    /// counter increments.
    #[mz_ore::test]
    fn admit_denied_when_victims_touched() {
        let pool = test_pool(256 << 20);
        let victim_orig = payload(SMALL, 73);
        let target_orig = payload(SMALL, 74);
        let victim = insert(&pool, &mut victim_orig.clone());
        let target = insert(&pool, &mut target_orig.clone());
        pool.evict(&target);
        assert!(pool.back_step());
        // Reading the victim sets its second-chance bit, disqualifying it.
        assert_eq!(read(&victim), victim_orig);
        pool.set_budget(64 << 10);
        let resident = pool.stats().resident_bytes;

        assert_eq!(read_admit(&target), target_orig);
        assert_eq!(target.residency(), Residency::Evicted);
        assert_eq!(victim.residency(), Residency::BackedResident);
        let stats = pool.stats();
        assert_eq!(stats.admissions_denied, 1);
        assert_eq!(stats.admissions_budget, 0);
        assert_eq!(stats.admissions_steal, 0);
        assert_eq!(stats.resident_bytes, resident);
    }

    /// An unbacked resident candidate is never stolen from: evicting it
    /// would require the compression that admission forbids.
    #[mz_ore::test]
    fn admit_denied_when_victims_unbacked() {
        let pool = test_pool(256 << 20);
        let victim = insert(&pool, &mut payload(SMALL, 75));
        let target_orig = payload(SMALL, 76);
        let target = insert(&pool, &mut target_orig.clone());
        pool.evict(&target);
        pool.set_budget(64 << 10);
        assert_eq!(read_admit(&target), target_orig);
        assert_eq!(target.residency(), Residency::Evicted);
        assert_eq!(victim.residency(), Residency::UnbackedResident);
        assert_eq!(pool.stats().admissions_denied, 1);
    }

    /// A clean backed victim of a different size class is never stolen
    /// from: slot reuse in place requires the classes to match.
    #[mz_ore::test]
    fn admit_denied_when_victims_wrong_class() {
        let pool = test_pool(256 << 20);
        // The victim fills the 128 KiB class; the target lives in the
        // 64 KiB one.
        let victim = insert(&pool, &mut payload(2 * SMALL, 77));
        let target_orig = payload(SMALL, 78);
        let target = insert(&pool, &mut target_orig.clone());
        pool.evict(&target);
        assert!(pool.back_step());
        assert_eq!(victim.residency(), Residency::BackedResident);
        // The budget holds exactly the victim: no headroom for the target.
        pool.set_budget(128 << 10);
        assert_eq!(read_admit(&target), target_orig);
        assert_eq!(target.residency(), Residency::Evicted);
        assert_eq!(victim.residency(), Residency::BackedResident);
        assert_eq!(pool.stats().admissions_denied, 1);
    }

    /// Plain reads and `take` never admit: an evicted chunk with plenty of
    /// budget headroom stays evicted through both, and neither counts a
    /// denial.
    #[mz_ore::test]
    fn plain_read_and_take_never_admit() {
        let pool = test_pool(256 << 20);
        let orig = payload(SMALL, 79);
        let handle = insert(&pool, &mut orig.clone());
        pool.evict(&handle);
        assert_eq!(read(&handle), orig);
        assert_eq!(handle.residency(), Residency::Evicted);
        assert_eq!(pool.stats().resident_bytes, 0);
        let mut out = Vec::new();
        handle.take(&mut out);
        assert_eq!(out, orig);
        let stats = pool.stats();
        assert_eq!(stats.admissions_budget, 0);
        assert_eq!(stats.admissions_steal, 0);
        assert_eq!(stats.admissions_denied, 0);
        assert_eq!(stats.resident_bytes, 0);
        assert_eq!(stats.frees, 1);
    }

    /// A steal settles the ledger with the payload difference: a shrinking
    /// steal always proceeds, while a steal that would grow resident bytes
    /// past the budget is denied and leaves the victim untouched.
    #[mz_ore::test]
    fn steal_admission_charges_the_budget() {
        // Shrinking steal: the victim is larger than the admitted payload,
        // so the steal lowers resident bytes and always may proceed.
        let pool = test_pool(256 << 20);
        let victim_orig = payload(SMALL, 84);
        let victim = insert(&pool, &mut victim_orig.clone());
        assert!(pool.back_step(), "victim backs");
        let small_orig = payload(SMALL / 2, 85);
        let handle = insert(&pool, &mut small_orig.clone());
        pool.evict(&handle);
        pool.set_budget(64 << 10);
        assert_eq!(read_admit(&handle), small_orig);
        let stats = pool.stats();
        assert_eq!(stats.admissions_steal, 1, "no headroom, so the read steals");
        assert_eq!(stats.resident_bytes, u64::cast_from(SMALL / 2 * 8));
        assert_eq!(victim.residency(), Residency::Evicted);
        assert_eq!(read(&victim), victim_orig, "victim serves from its extent");

        // Growing steal: the admitted payload is larger than the only
        // victim, and the growth does not fit the budget, so the admission
        // is denied and the victim is left untouched.
        let pool = test_pool(256 << 20);
        let big_orig = payload(SMALL, 86);
        let big = insert(&pool, &mut big_orig.clone());
        pool.evict(&big);
        let small_victim = insert(&pool, &mut payload(SMALL / 2, 87));
        assert!(pool.back_step(), "victim backs");
        pool.set_budget(32 << 10);
        assert_eq!(read_admit(&big), big_orig);
        let stats = pool.stats();
        assert_eq!(stats.admissions_denied, 1, "growth exceeds the budget");
        assert_eq!(stats.admissions_steal, 0);
        assert_eq!(big.residency(), Residency::Evicted);
        assert_eq!(small_victim.residency(), Residency::BackedResident);
    }

    /// Admission of a chunk whose extent was pushed to the device: the read
    /// revives the extent into the acquired slot and re-counts it.
    #[mz_ore::test]
    fn admission_revives_paged_out_extent() {
        let pool = test_pool(256 << 20);
        let orig = payload(SMALL, 88);
        let handle = insert(&pool, &mut orig.clone());
        pool.evict(&handle);
        assert_eq!(
            pool.stats().extent_resident_bytes,
            0,
            "zero RSS target pages the extent out on eviction",
        );
        // Raise the target so the read's own tier enforcement does not
        // page the revived extent straight back out.
        pool.set_rss_target(1 << 30);
        assert_eq!(read_admit(&handle), orig);
        assert_eq!(handle.residency(), Residency::BackedResident);
        let stats = pool.stats();
        assert_eq!(stats.admissions_budget, 1);
        assert!(stats.extent_resident_bytes > 0, "revived and re-counted");
    }

    /// An admitting read of a chunk that is not evicted is a plain read:
    /// no admission counter moves and no state changes.
    #[mz_ore::test]
    fn admit_is_plain_read_on_non_evicted_chunks() {
        let pool = test_pool(256 << 20);
        let orig = payload(SMALL, 89);
        let resident = insert(&pool, &mut orig.clone());
        assert_eq!(read_admit(&resident), orig);
        assert_eq!(resident.residency(), Residency::UnbackedResident);
        let words = SIZE_CLASSES[SIZE_CLASSES.len() - 1] / 8 + 1;
        let oversize_orig = payload(words, 90);
        let oversize = insert(&pool, &mut oversize_orig.clone());
        assert_eq!(read_admit(&oversize), oversize_orig);
        let empty = insert(&pool, &mut Vec::new());
        assert!(read_admit(&empty).is_empty());
        let stats = pool.stats();
        assert_eq!(stats.admissions_budget, 0);
        assert_eq!(stats.admissions_steal, 0);
        assert_eq!(stats.admissions_denied, 0);
    }

    /// Concurrent admitting reads with no budget headroom: every admission
    /// must go through the steal path, racing steals against each other on
    /// the same victims (the pool's only two-chunk lock edge). Contents are
    /// asserted on every read.
    #[mz_ore::test]
    fn concurrent_admits_exercise_the_steal_path() {
        const CHUNKS: u64 = 8;
        let pool = test_pool(usize::MAX);
        let origs: Vec<_> = (0..CHUNKS).map(|seed| payload(SMALL, 900 + seed)).collect();
        let evicted: Arc<Vec<(Vec<u64>, ChunkHandle)>> = Arc::new(
            origs
                .iter()
                .map(|orig| {
                    let handle = insert(&pool, &mut orig.clone());
                    pool.evict(&handle);
                    (orig.clone(), handle)
                })
                .collect(),
        );
        let mut victims = Vec::new();
        for seed in 0..CHUNKS {
            victims.push(insert(&pool, &mut payload(SMALL, 950 + seed)));
            assert!(pool.back_step(), "victim backs");
        }
        // Exactly the victims' bytes: no free headroom, so every admission
        // steals or is denied.
        pool.set_budget(usize::cast_from(CHUNKS) * (64 << 10));
        let threads: Vec<_> = (0..2u64)
            .map(|t| {
                let evicted = Arc::clone(&evicted);
                std::thread::spawn(move || {
                    for round in 0..CHUNKS {
                        let (orig, handle) = &evicted[usize::cast_from((t + round) % CHUNKS)];
                        let mut out = Vec::new();
                        handle.read_into_admit(&mut out);
                        assert_eq!(&out, orig);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("admitting thread panicked");
        }
        let stats = pool.stats();
        assert!(stats.admissions_steal > 0, "no headroom forces steals");
        assert!(
            stats.resident_bytes <= u64::cast_from(usize::cast_from(CHUNKS) * (64 << 10)),
            "steals never grow resident bytes past the budget",
        );
        // The victims were held live as steal targets; a steal leaves its
        // victim evicted, so at least one is evicted here.
        let stolen = victims
            .iter()
            .filter(|v| v.residency() == Residency::Evicted)
            .count();
        assert!(stolen > 0, "a steal evicts its victim");
    }

    /// A re-admitted chunk keeps its insert-time depth: under budget
    /// pressure it is evicted from its own deeper band before a younger
    /// band-0 chunk, which a re-admission into band 0 would have inverted.
    #[mz_ore::test]
    fn admitted_chunk_keeps_its_depth() {
        let pool = test_pool(256 << 20);
        let deep_orig = payload(SMALL, 80);
        let deep = insert_at_depth(&pool, 2, &mut deep_orig.clone());
        let young = insert(&pool, &mut payload(SMALL, 81));
        pool.evict(&deep);
        assert_eq!(read_admit(&deep), deep_orig);
        assert_eq!(deep.residency(), Residency::BackedResident);
        assert_eq!(pool.stats().admissions_budget, 1);

        // Budget of one chunk: enforcement visits the deep band first.
        pool.set_budget(64 << 10);
        assert_eq!(deep.residency(), Residency::Evicted);
        assert_eq!(young.residency(), Residency::UnbackedResident);
        assert_eq!(
            pool.stats().evictions_cheap,
            1,
            "the extent kept through admission pre-paid the eviction",
        );
    }

    /// Admitting reads racing enforcement, opposing steals, and frees:
    /// contents stay correct, contended steals degrade to skips, and the
    /// accounting identity settles to zero.
    #[mz_ore::test]
    fn concurrent_admits_race_cleanly() {
        let pool = test_pool(64 << 10);
        let per_thread = rounds(50, 3);
        let threads: Vec<_> = (0..4u64)
            .map(|t| {
                let pool = pool.clone();
                std::thread::spawn(move || {
                    let mut out = Vec::new();
                    for round in 0..per_thread {
                        let orig = payload(SMALL, t * 1000 + round);
                        let handle = insert(&pool, &mut orig.clone());
                        pool.evict(&handle);
                        handle.read_into_admit(&mut out);
                        assert_eq!(out, orig);
                        handle.read_into_admit(&mut out);
                        assert_eq!(out, orig);
                        assert_eq!(read(&handle), orig);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("worker thread panicked");
        }
        let stats = pool.stats();
        assert_eq!(stats.inserts, 4 * per_thread);
        assert_eq!(stats.frees, 4 * per_thread);
        assert_eq!(stats.resident_bytes, 0);
        assert_eq!(stats.extent_resident_bytes, 0);
    }

    /// Slots are scoped to residency: eviction releases the slot, so a
    /// capacity holding exactly one chunk can serve any number of chunks one
    /// at a time, and reads of evicted chunks need no slot at all.
    #[mz_ore::test]
    fn eviction_releases_the_slot() {
        // One 64 KiB slot per class.
        let pool = Pool::with_class_capacity(64 << 10).expect("pool creation");
        let a = insert(&pool, &mut payload(SMALL, 6));
        pool.evict(&a);
        // The class's only slot is free again: a second chunk fits without
        // falling back to the heap.
        let b = insert(&pool, &mut payload(SMALL, 7));
        assert_eq!(b.residency(), Residency::UnbackedResident);
        assert_eq!(pool.stats().slot_exhausted_fallbacks, 0);
        // Reading `a` decompresses straight from its extent while `b` holds
        // the class's only slot: copy-out allocates nothing.
        assert_eq!(read(&a), payload(SMALL, 6));
        assert_eq!(a.residency(), Residency::Evicted);
        assert_eq!(read(&b), payload(SMALL, 7));
    }

    /// The eviction queue holds resident chunks only: an enforcement pass
    /// drops entries for evicted chunks, and reads never re-add them, so the
    /// scan each insert pays stays proportional to the resident set rather
    /// than every chunk ever evicted.
    #[mz_ore::test]
    fn queue_holds_resident_chunks_only() {
        let pool = test_pool(128 << 10);
        let mut handles = Vec::new();
        for seed in 0..8 {
            handles.push(insert(&pool, &mut payload(SMALL, 800 + seed)));
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
        // Reading an evicted chunk copies out of its extent and does not
        // re-enqueue it: the queue keeps tracking the resident set.
        let evicted = handles
            .iter()
            .find(|h| h.residency() == Residency::Evicted)
            .expect("something was evicted");
        let before = pool.queue_len();
        assert_eq!(read(evicted).len(), SMALL);
        assert_eq!(evicted.residency(), Residency::Evicted);
        assert_eq!(pool.queue_len(), before, "reads leave the queue alone");
    }

    #[mz_ore::test]
    fn dead_data_is_never_written() {
        let pool = test_pool(256 << 20);
        let handle = insert(&pool, &mut payload(SMALL, 7));
        drop(handle);
        let stats = pool.stats();
        assert_eq!(stats.frees, 1);
        assert_eq!(stats.writes_elided, 1);
        assert_eq!(stats.extent_bytes_written, 0);
        assert_eq!(stats.resident_bytes, 0);
    }

    #[mz_ore::test]
    fn budget_is_enforced_on_insert() {
        let budget = 128 << 10;
        let pool = test_pool(budget);
        let mut handles = Vec::new();
        for seed in 0..8 {
            handles.push(insert(&pool, &mut payload(SMALL, 100 + seed)));
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
    fn set_budget_retunes_in_place() {
        let pool = test_pool(usize::MAX);
        let mut handles = Vec::new();
        for seed in 0..8 {
            handles.push(insert(&pool, &mut payload(SMALL, 200 + seed)));
        }
        assert_eq!(pool.stats().evictions_compress, 0);

        // Shrinking the budget evicts immediately.
        pool.set_budget(128 << 10);
        let stats = pool.stats();
        assert!(stats.resident_bytes <= 128 << 10);
        assert!(stats.evictions_compress >= 6);

        // Growing it leaves headroom: a fresh insert stays resident.
        pool.set_budget(usize::MAX);
        let h = insert(&pool, &mut payload(SMALL, 300));
        assert_eq!(h.residency(), Residency::UnbackedResident);
        for h in &handles {
            assert_eq!(read(h).len(), SMALL);
        }
    }

    #[mz_ore::test]
    fn second_chance_prefers_untouched_victims() {
        // Budget holds one and a half small chunks.
        let pool = test_pool((64 << 10) + (32 << 10));
        let orig_a = payload(SMALL, 8);
        let handle_a = insert(&pool, &mut orig_a.clone());
        assert_eq!(read(&handle_a), orig_a);
        // Inserting B overflows the budget; A is older but touched, so the
        // enforcer gives it a second chance and evicts untouched B instead.
        let handle_b = insert(&pool, &mut payload(SMALL, 9));
        assert_eq!(handle_a.residency(), Residency::UnbackedResident);
        assert_eq!(handle_b.residency(), Residency::Evicted);
    }

    /// Depth-hinted chunks are evicted before younger ones: the deep chunk
    /// loses even though the young chunk is older and both are untouched
    /// (plain FIFO would have evicted the older, young one). Also exercises
    /// band clamping: depths beyond the last band share it.
    #[mz_ore::test]
    fn eviction_prefers_deeper_chunks() {
        // Budget of one small chunk.
        let pool = test_pool(64 << 10);
        let young = insert(&pool, &mut payload(SMALL, 900));
        let deep = insert_at_depth(&pool, 255, &mut payload(SMALL, 901));
        assert_eq!(young.residency(), Residency::UnbackedResident);
        assert_eq!(deep.residency(), Residency::Evicted);
    }

    /// Eager backing visits deeper chunks first, mirroring eviction order,
    /// so the chunks evicted first are the ones already backed.
    #[mz_ore::test]
    fn backing_prefers_deeper_chunks() {
        let pool = test_pool(256 << 20);
        let young = insert(&pool, &mut payload(SMALL, 902));
        let deep = insert_at_depth(&pool, 2, &mut payload(SMALL, 903));
        assert!(pool.back_step());
        assert_eq!(deep.residency(), Residency::BackedResident);
        assert_eq!(young.residency(), Residency::UnbackedResident);
        assert!(pool.back_step());
        assert_eq!(young.residency(), Residency::BackedResident);
    }

    #[mz_ore::test]
    fn empty_insert_consumes_no_slot() {
        let pool = test_pool(256 << 20);
        let mut data = Vec::new();
        let handle = insert(&pool, &mut data);
        assert_eq!(handle.size_class_bytes(), None);
        assert!(read(&handle).is_empty());
        // Reads clear the destination even for empty chunks.
        let mut out = vec![1u64, 2, 3];
        handle.read_into(&mut out);
        assert!(out.is_empty());
        drop(handle);
        let stats = pool.stats();
        assert_eq!(stats.resident_bytes, 0);
        assert_eq!(stats.writes_elided, 0);
    }

    #[mz_ore::test]
    fn oversize_round_trips() {
        let pool = test_pool(256 << 20);
        let words = SIZE_CLASSES[SIZE_CLASSES.len() - 1] / 8 + 1;
        let orig = payload(words, 10);
        let handle = insert(&pool, &mut orig.clone());
        assert_eq!(handle.residency(), Residency::Oversize);
        assert_eq!(handle.size_class_bytes(), None);
        let stats = pool.stats();
        assert_eq!(stats.oversize_bytes, u64::cast_from(words * 8));
        // The payload outgrew the largest class, and no class was exhausted.
        assert_eq!(stats.oversize_payloads, 1);
        assert_eq!(stats.slot_exhausted_fallbacks, 0);
        // Explicit eviction and budget enforcement leave oversize chunks
        // resident.
        pool.evict(&handle);
        pool.enforce_budget();
        assert_eq!(handle.residency(), Residency::Oversize);
        assert_eq!(read(&handle), orig);
        drop(handle);
        let stats = pool.stats();
        assert_eq!(stats.oversize_bytes, 0);
        assert_eq!(stats.resident_bytes, 0);
    }

    #[mz_ore::test]
    fn payload_lands_in_smallest_fitting_class() {
        let pool = test_pool(256 << 20);
        let handle = insert(&pool, &mut payload((100 << 10) / 8, 11));
        assert_eq!(handle.size_class_bytes(), Some(128 << 10));
        let exact = insert(&pool, &mut payload(SMALL, 12));
        assert_eq!(exact.size_class_bytes(), Some(64 << 10));
    }

    #[mz_ore::test]
    fn multithreaded_smoke() {
        // Budget of one small chunk: four inserting threads keep the pool
        // over budget, so every insert's enforcement pass selects victims
        // owned by other threads, racing cross-thread eviction against
        // copy-out reads and frees.
        let pool = test_pool(64 << 10);
        let per_thread = rounds(50, 3);
        let threads: Vec<_> = (0..4u64)
            .map(|t| {
                let pool = pool.clone();
                std::thread::spawn(move || {
                    for round in 0..per_thread {
                        let seed = t * 1000 + round;
                        let orig = payload(SMALL, seed);
                        let handle = insert(&pool, &mut orig.clone());
                        pool.evict(&handle);
                        assert_eq!(read(&handle), orig);
                        // Enforcement racing reads must never corrupt them.
                        pool.enforce_budget();
                        assert_eq!(read(&handle), orig);
                        drop(handle);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("worker thread panicked");
        }
        let stats = pool.stats();
        assert_eq!(stats.inserts, 4 * per_thread);
        assert_eq!(stats.frees, 4 * per_thread);
        assert_eq!(stats.resident_bytes, 0);
    }

    #[mz_ore::test]
    fn concurrent_read_enforce_churn() {
        // Races the three actors that can touch one chunk's slot: readers
        // copying shared chunks out and verifying them, an enforcer evicting
        // them (the zero budget makes every chunk a victim), and a churner
        // whose insert/free traffic turns the queue over. Contents are
        // asserted on every read, so an eviction or slot recycle racing a
        // copy-out shows up as corruption.
        let pool = test_pool(0);
        let shared: Arc<Vec<(Vec<u64>, ChunkHandle)>> = Arc::new(
            (0..4u64)
                .map(|seed| {
                    let orig = payload(SMALL, 600 + seed);
                    let handle = insert(&pool, &mut orig.clone());
                    (orig, handle)
                })
                .collect(),
        );
        let churn = rounds(300, 6);
        let mut threads = Vec::new();
        for t in 0..2u64 {
            let shared = Arc::clone(&shared);
            threads.push(std::thread::spawn(move || {
                for round in 0..churn {
                    let (orig, handle) = &shared[usize::cast_from((t + round) % 4)];
                    assert_eq!(&read(handle), orig);
                }
            }));
        }
        {
            let pool = pool.clone();
            threads.push(std::thread::spawn(move || {
                for _ in 0..2 * churn {
                    pool.enforce_budget();
                }
            }));
        }
        {
            let pool = pool.clone();
            threads.push(std::thread::spawn(move || {
                for round in 0..churn {
                    let orig = payload(SMALL, 700 + round);
                    let handle = insert(&pool, &mut orig.clone());
                    assert_eq!(read(&handle), orig);
                }
            }));
        }
        for thread in threads {
            thread.join().expect("worker thread panicked");
        }
        drop(shared);
        assert_eq!(pool.stats().resident_bytes, 0);
    }

    /// Read-only traffic never raises resident bytes: every chunk starts
    /// evicted and is then read once, with no inserts in between. Reads copy
    /// out of the extents and leave every chunk evicted, so a seek-heavy
    /// phase costs no pool memory at all.
    #[mz_ore::test]
    fn reads_never_raise_resident_bytes() {
        let pool = test_pool(128 << 10);
        let origs: Vec<_> = (0..8u64).map(|seed| payload(SMALL, 300 + seed)).collect();
        let handles: Vec<_> = origs
            .iter()
            .map(|o| insert(&pool, &mut o.clone()))
            .collect();
        for handle in &handles {
            pool.evict(handle);
        }
        assert_eq!(pool.stats().resident_bytes, 0);
        for (index, handle) in handles.iter().enumerate() {
            assert_eq!(read(handle), origs[index]);
            assert_eq!(handle.residency(), Residency::Evicted);
            assert_eq!(pool.stats().resident_bytes, 0);
        }
    }

    /// Evict-then-free churn under a generous RSS target: the compressed
    /// tier never crosses its cap, so enforcement never visits (and never
    /// drops) extent-queue entries, and pruning alone must keep the queue
    /// proportional to the live resident extents.
    #[mz_ore::test]
    fn extent_queue_stays_bounded_under_cap() {
        let pool = test_pool(256 << 20);
        pool.set_rss_target(1 << 40);
        for seed in 0..rounds(1000, 48) {
            let handle = insert(&pool, &mut payload(SMALL, seed));
            pool.evict(&handle);
            drop(handle);
        }
        assert_eq!(pool.stats().extent_resident_bytes, 0);
        let len = pool.extent_queue_len();
        assert!(
            len <= 32,
            "extent queue holds {len} entries for zero resident extents",
        );
    }

    /// A warm slot reused for a smaller payload round-trips: the tail
    /// release past the new payload must not disturb the payload itself,
    /// and the ledger credits exactly the payload.
    #[mz_ore::test]
    fn warm_reuse_with_smaller_payload_round_trips() {
        // Budget 8 MiB: warm cap = 1 MiB, so a 64 KiB slot parks warm.
        let pool = test_pool(8 << 20);
        let full = insert(&pool, &mut payload(SMALL, 60));
        drop(full);
        assert_eq!(pool.stats().warm_bytes, 64 << 10, "freed slot parks warm");
        // A payload of just over a page reuses the warm slot; the slot's
        // pages past it are released.
        let words = 4096 / 8 + 1;
        let orig = payload(words, 61);
        let handle = insert(&pool, &mut orig.clone());
        let stats = pool.stats();
        assert_eq!(stats.warm_reuses, 1, "reused the warm slot");
        assert_eq!(stats.resident_bytes, u64::cast_from(words * 8));
        assert_eq!(read(&handle), orig);
        // Round-trips through the extent as well.
        pool.evict(&handle);
        pool.poison_free_slots();
        assert_eq!(read(&handle), orig);
        drop(handle);
        assert_eq!(pool.stats().resident_bytes, 0);
    }

    /// Heap-backed chunks count as resident but can never be evicted, so
    /// the budget must not force slotted chunks out on their account: with
    /// unevictable bytes alone exceeding the budget, a slotted chunk that
    /// fits the budget stays resident.
    #[mz_ore::test]
    fn unevictable_bytes_do_not_force_eviction() {
        // One 64 KiB slot per class: the second and third inserts fall
        // back to the heap.
        let pool = Pool::with_class_capacity(64 << 10).expect("pool creation");
        pool.set_budget(64 << 10);
        let slotted = insert(&pool, &mut payload(SMALL, 91));
        let heap_a = insert(&pool, &mut payload(SMALL, 92));
        let heap_b = insert(&pool, &mut payload(SMALL, 93));
        assert_eq!(heap_a.residency(), Residency::Oversize);
        assert_eq!(heap_b.residency(), Residency::Oversize);
        let stats = pool.stats();
        assert!(stats.oversize_bytes > 64 << 10, "unevictable exceed budget");
        assert_eq!(slotted.residency(), Residency::UnbackedResident);
        assert_eq!(stats.evictions_compress, 0);
        assert_eq!(read(&slotted), payload(SMALL, 91));
        assert_eq!(read(&heap_a), payload(SMALL, 92));
    }

    /// A slotless empty chunk survives an explicit evict with spill
    /// scheduling enabled: nothing is handed to the spill threads and the
    /// chunk stays readable.
    #[mz_ore::test]
    fn evict_of_empty_chunk_is_a_no_op() {
        let pool = test_pool(usize::MAX);
        pool.enable_spill_without_threads();
        let empty = insert(&pool, &mut Vec::new());
        pool.evict(&empty);
        assert_eq!(empty.residency(), Residency::UnbackedResident);
        assert!(!pool.spill_step(), "nothing was scheduled");
        assert_eq!(pool.stats().spill_scheduled, 0);
        assert!(read(&empty).is_empty());
    }

    #[mz_ore::test]
    fn queue_stays_bounded_under_budget() {
        // Chunk churn that never exceeds the budget: the enforcer's eviction
        // loop never runs, so stale queue entries must be reclaimed by
        // pruning alone.
        let pool = test_pool(256 << 20);
        for seed in 0..rounds(1000, 48) {
            let handle = insert(&pool, &mut payload(SMALL, seed));
            drop(handle);
        }
        let len = pool.queue_len();
        assert!(len <= 32, "queue holds {len} entries for zero live chunks");
    }

    #[mz_ore::test]
    fn spill_async_evict_round_trip() {
        let pool = test_pool(usize::MAX);
        pool.enable_spill_without_threads();
        let h = insert(&pool, &mut payload(SMALL, 400));
        pool.evict(&h);
        assert_eq!(h.residency(), Residency::WriteInFlight);
        // Readable while in flight: the slot is still populated, and the
        // copy-out coexists with the spill thread's compression read.
        assert_eq!(read(&h), payload(SMALL, 400));
        // Reads leave no trace, so the eviction commits.
        assert!(pool.spill_step());
        assert_eq!(h.residency(), Residency::Evicted);
        let stats = pool.stats();
        assert_eq!(stats.spill_scheduled, 1);
        assert_eq!(stats.evictions_compress, 1);
        pool.poison_free_slots();
        assert_eq!(read(&h), payload(SMALL, 400));
    }

    #[mz_ore::test]
    fn spill_freed_while_queued_is_elided() {
        let pool = test_pool(usize::MAX);
        pool.enable_spill_without_threads();
        let h = insert(&pool, &mut payload(SMALL, 401));
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

    /// `take` on a chunk whose backing write is still in flight copies the
    /// contents out of the slot and cancels the write: the spill thread finds
    /// the chunk freed, elides the extent, and settles the slot accounting.
    #[mz_ore::test]
    fn spill_take_in_flight_cancels_write() {
        let pool = test_pool(usize::MAX);
        pool.enable_spill_without_threads();
        let orig = payload(SMALL, 402);
        let h = insert(&pool, &mut orig.clone());
        pool.evict(&h);
        assert_eq!(h.residency(), Residency::WriteInFlight);
        let mut out = Vec::new();
        h.take(&mut out);
        assert_eq!(out, orig);
        assert!(pool.spill_step());
        let stats = pool.stats();
        assert_eq!(stats.frees, 1);
        assert_eq!(stats.spill_cancelled, 1);
        assert_eq!(stats.writes_elided, 1, "taken before compression: elided");
        assert_eq!(stats.extent_bytes_written, 0, "no extent was written");
        assert_eq!(stats.resident_bytes, 0, "slot accounting settled");
        assert_eq!(stats.live_chunks, 0);
    }

    #[mz_ore::test]
    fn spill_threads_end_to_end() {
        let pool = test_pool(128 << 10);
        pool.set_spill_threads(2);
        let mut handles = Vec::new();
        for seed in 0..rounds(16, 6) {
            handles.push(insert(&pool, &mut payload(SMALL, 500 + seed)));
        }
        pool.quiesce_spill();
        let stats = pool.stats();
        assert!(
            stats.spill_scheduled > 0,
            "budget pressure should have scheduled spills",
        );
        for (i, h) in handles.iter().enumerate() {
            assert_eq!(read(h), payload(SMALL, 500 + u64::cast_from(i)));
        }
        pool.join_spill_threads();
    }

    /// Races the `WriteInFlight` protocol in its true concurrent form:
    /// spill threads compress slots without the state lock while owner
    /// threads copy the same chunks out under it and drop chunks mid-flight
    /// (both cancellation windows). Contents are asserted on every read, so
    /// a compression or slot release racing a copy-out shows up as
    /// corruption; under Miri the aliasing itself is checked.
    #[mz_ore::test]
    fn spill_threads_race_reads_and_drops() {
        let pool = test_pool(usize::MAX);
        pool.set_spill_threads(2);
        let iters = rounds(50, 6);
        let mut threads = Vec::new();
        for t in 0..2u64 {
            let pool = pool.clone();
            threads.push(std::thread::spawn(move || {
                for round in 0..iters {
                    let orig = payload(SMALL, t * 10_000 + round);
                    let handle = insert(&pool, &mut orig.clone());
                    // Hands the chunk to the spill threads (`WriteInFlight`).
                    pool.evict(&handle);
                    // Copy-out read racing the unlocked compression read.
                    assert_eq!(read(&handle), orig);
                    if round % 2 == 0 {
                        // Free while queued or mid-compression: the
                        // cancellation windows own the deferred cleanup.
                        drop(handle);
                    } else {
                        assert_eq!(read(&handle), orig);
                    }
                }
            }));
        }
        for thread in threads {
            thread.join().expect("worker thread panicked");
        }
        pool.quiesce_spill();
        pool.join_spill_threads();
        assert_eq!(pool.stats().resident_bytes, 0);
    }

    #[mz_ore::test]
    fn insert_with_fills_in_place() {
        let pool = test_pool(usize::MAX);
        let want = payload(SMALL, 600);
        let h = pool.insert_with(SMALL, ChunkHints::default(), &TEST_CODEC, |dst| {
            assert_eq!(dst.len(), SMALL, "fill sees exactly the chunk length");
            dst.copy_from_slice(&want);
        });
        assert_eq!(h.residency(), Residency::UnbackedResident);
        assert_eq!(read(&h), want);
        pool.evict(&h);
        assert_eq!(read(&h), want, "round-trips through the extent");

        // Empty and oversize take their fallback paths.
        let empty = pool.insert_with(0, ChunkHints::default(), &TEST_CODEC, |dst| {
            assert!(dst.is_empty())
        });
        assert!(read(&empty).is_empty());
        let big_len = (SIZE_CLASSES[SIZE_CLASSES.len() - 1] / 8) + 1;
        let big = pool.insert_with(big_len, ChunkHints::default(), &TEST_CODEC, |dst| {
            dst.fill(7)
        });
        assert_eq!(big.residency(), Residency::Oversize);
        assert_eq!(read(&big).len(), big_len);
    }

    #[mz_ore::test]
    fn slot_exhaustion_degrades_to_heap() {
        // Two 64 KiB slots per class at this capacity; the third insert finds
        // no slot and must fall back to the heap rather than panic.
        let pool = Pool::with_class_capacity(128 << 10).expect("pool creation");
        let a = insert(&pool, &mut payload(SMALL, 700));
        let b = insert(&pool, &mut payload(SMALL, 701));
        let c = insert(&pool, &mut payload(SMALL, 702));
        assert_eq!(a.residency(), Residency::UnbackedResident);
        assert_eq!(b.residency(), Residency::UnbackedResident);
        assert_eq!(
            c.residency(),
            Residency::Oversize,
            "fallback is heap-backed"
        );
        assert_eq!(pool.stats().slot_exhausted_fallbacks, 1);
        assert_eq!(read(&c), payload(SMALL, 702));
        // Freeing a slotted chunk lets the next insert use the region again.
        drop(a);
        let d = insert(&pool, &mut payload(SMALL, 703));
        assert_eq!(d.residency(), Residency::UnbackedResident);
        assert_eq!(read(&d), payload(SMALL, 703));
    }
}
