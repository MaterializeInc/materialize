// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! [`ColumnChunk`]: differential's [`Chunk`] over [`Column`]-shaped updates.
//!
//! A chunk is a sorted, consolidated run of `(D, T, R)` updates in the flat
//! columnar layout, in one of two homes:
//!
//! * **Resident**: an `Rc`-shared [`Column`] on the heap. Fresh input, merge
//!   output, and small tails live here.
//! * **Spilled**: the serialized body in the process [`Pool`], with the record
//!   count and the first and last data items resident. The pool owns residency
//!   from there, with slots under a memory budget and compression and device
//!   pageout under pressure, and a body that dies before pressure reaches it
//!   is freed without I/O.
//!
//! Reads of a spilled body are copy-out and scoped to the call that needs
//! them, the contract that lets the pool evict with no reader accounting
//! (see [`mz_ore::pool`]).
//!
//! Spilling happens in [`Chunk::settle`], the trait's designated commit point:
//! chunks moved to settled output are handed to the pool when spilling is
//! enabled (see [`set_compute_spill_enabled`] and [`set_storage_spill_enabled`]
//! for how the per-commit destination resolves). Grading is by serialized
//! bytes, the ship size [`Column`] already targets, rather than by the
//! record-count `TARGET`, since record count does not bound bytes for
//! variable-width data.
//!
//! Chunks whose data is a `(key, val)` pair additionally implement
//! [`UnloadChunk`], the bulk-read capability: sorted probe keys in, matching
//! updates appended to caller-owned staging, with `locate` answered from the
//! resident fence metadata so a probe set faults only the chunk bodies it
//! actually touches.

use std::borrow::Cow;
#[cfg(test)]
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use columnar::bytes::indexed;
use columnar::{Borrow, BorrowedOf, Columnar, Container as _, FromBytes, Index, Len, Push as _};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::chunk::Chunk;
use mz_ore::cast::CastFrom;
use mz_ore::pool::{ChunkHandle, ChunkHints, ExtentCodec, IDENTITY_CODEC, Pool};
use smallvec::SmallVec;
use timely::Accountable;
use timely::PartialOrder;
use timely::container::{ContainerBuilder, PushInto};
use timely::dataflow::channels::ContainerBytes;
use timely::progress::Timestamp;
use timely::progress::frontier::{Antichain, AntichainRef};

use crate::columnar::batcher::{ColumnChunker, gallop};
use crate::columnar::unload::UnloadChunk;
use crate::columnar::{Column, at_serialized_capacity};

/// Compute's leg of the process spill gate. See [`set_compute_spill_enabled`].
static COMPUTE_SPILL_ENABLED: AtomicBool = AtomicBool::new(false);

/// Storage's leg of the process spill gate. See [`set_storage_spill_enabled`].
static STORAGE_SPILL_ENABLED: AtomicBool = AtomicBool::new(false);

thread_local! {
    /// A thread-scoped pool override, taking precedence over the global
    /// enable flag and pool. Lets tests and benches spill through a private
    /// pool without touching process-global state.
    static SPILL_OVERRIDE: RefCell<Option<Pool>> = const { RefCell::new(None) };

    /// A thread-scoped depth-floor override, taking precedence over the
    /// global value. Lets tests pin the floor without racing concurrently
    /// running tests on the process-global state.
    #[cfg(test)]
    static COMPRESS_MIN_DEPTH_OVERRIDE: Cell<Option<u8>> = const { Cell::new(None) };

    /// Reusable staging for call-scoped reads of spilled bodies.
    static READ_SCRATCH: RefCell<Vec<u64>> = const { RefCell::new(Vec::new()) };
}

/// Enable or disable chunk spilling on behalf of compute's arrangement
/// batchers.
///
/// Chunks carry no subsystem identity, so the spill decision is process-wide:
/// committed chunks spill while *either* the compute or the storage gate is
/// set. Each subsystem's config application writes only its own gate, so the
/// two dyncfg flags compose as an OR instead of clobbering each other.
///
/// Takes effect at the next `settle`. Already-spilled chunks are unaffected
/// either way. The pool is resolved per commit through
/// [`crate::pool_config::active_pool`], so chunks spill only once
/// `apply_pool_config` has installed and budgeted the pool. With no pool
/// installed chunks stay resident regardless of the gates.
pub fn set_compute_spill_enabled(enabled: bool) {
    COMPUTE_SPILL_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Enable or disable chunk spilling on behalf of storage's upsert dataflows.
///
/// See [`set_compute_spill_enabled`] for the shared-gate semantics.
pub fn set_storage_spill_enabled(enabled: bool) {
    STORAGE_SPILL_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Set or unset the pool through which this thread's chunk spills are
/// routed, taking precedence over the gates and the process pool. `None`
/// restores the global resolution.
pub fn set_spill_override(pool: Option<Pool>) {
    SPILL_OVERRIDE.with(|cell| *cell.borrow_mut() = pool);
}

/// The youngest generational depth whose spilled bodies are compressed. See
/// [`set_compress_min_depth`].
static COMPRESS_MIN_DEPTH: AtomicU8 = AtomicU8::new(DEFAULT_COMPRESS_MIN_DEPTH);

/// Set the youngest generational depth whose spilled bodies are compressed.
///
/// A chunk at depth `d` is rewritten (merged, extracted, advanced) with
/// frequency proportional to `2^-d` under geometric merging, so compressing
/// a shallow chunk buys a short stay in the pool at the cost of a guaranteed
/// near-term codec round-trip: the body is encoded only to be read back and
/// decoded by the next rewrite. Generations below the floor spill under the
/// identity codec instead: still budgeted and swap-backed like every extent,
/// but encode and decode are copies. The floor never exempts a body from the
/// pool, so it cannot grow unbudgeted resident state.
///
/// The floor cannot strand a long-lived body uncompressed: a chunk that a
/// merge carries forward untouched also ages a generation, and its body is
/// re-spilled under the compressing codec when it crosses the floor (see
/// `survive_merge`).
///
/// `0` compresses every spilled body. Consulted at every commit, so changes
/// apply to running dataflows.
pub fn set_compress_min_depth(depth: u8) {
    COMPRESS_MIN_DEPTH.store(depth, Ordering::Relaxed);
}

/// Set or unset a thread-scoped depth-floor override, taking precedence over
/// [`set_compress_min_depth`]. Tests run concurrently and must not race on
/// the process-global floor.
#[cfg(test)]
pub fn set_compress_min_depth_override(depth: Option<u8>) {
    COMPRESS_MIN_DEPTH_OVERRIDE.with(|cell| cell.set(depth));
}

/// The depth floor in effect for this thread's commits.
fn compress_min_depth() -> u8 {
    #[cfg(test)]
    if let Some(depth) = COMPRESS_MIN_DEPTH_OVERRIDE.with(|cell| cell.get()) {
        return depth;
    }
    COMPRESS_MIN_DEPTH.load(Ordering::Relaxed)
}

/// The codec a body at `depth` stores under, identity below the compression
/// floor and lz4 at and past it, paired with whether that codec compresses.
/// One read of the floor, so the pair cannot disagree with itself when the
/// floor moves under a concurrent commit.
fn codec_for_depth(depth: u8) -> (&'static dyn ExtentCodec, bool) {
    if depth < compress_min_depth() {
        (&IDENTITY_CODEC, false)
    } else {
        (&LZ4_CODEC, true)
    }
}

/// The pool committed chunks spill to, if any.
fn spill_pool() -> Option<Pool> {
    if let Some(pool) = SPILL_OVERRIDE.with(|cell| cell.borrow().clone()) {
        return Some(pool);
    }
    let enabled = COMPUTE_SPILL_ENABLED.load(Ordering::Relaxed)
        || STORAGE_SPILL_ENABLED.load(Ordering::Relaxed);
    if enabled {
        crate::pool_config::active_pool()
    } else {
        None
    }
}

/// Scratch capacity retained across reads, in words. A read larger than this
/// releases the buffer afterward, so a thread's scratch does not ratchet to
/// the largest body it ever carried (heap no pool gauge can see).
const SCRATCH_RETAIN_WORDS: usize = 1 << 18;

/// Run `f` with this thread's read scratch, cleared of any previous use.
fn with_scratch<Out>(f: impl FnOnce(&mut Vec<u64>) -> Out) -> Out {
    READ_SCRATCH.with(|cell| {
        let mut scratch = cell.take();
        scratch.clear();
        let out = f(&mut scratch);
        if scratch.capacity() > SCRATCH_RETAIN_WORDS {
            scratch.clear();
            scratch.shrink_to_fit();
        }
        cell.replace(scratch);
        out
    })
}

/// The serialized-byte size committed chunks aim for, matching the ship size
/// of the columnar merge machinery.
const COMMIT_BYTES: usize = 2 << 20;

/// Bodies smaller than this stay resident: the pool's smallest size class is
/// 64 KiB, so spilling below it trades no meaningful memory for slot waste.
///
/// Sub-floor bodies are invisible to the pool's budget, which is safe only
/// while they are rare. `settle` coalesces toward `COMMIT_BYTES` before
/// committing, so in the harness only a final `done` tail commits below the
/// floor. A caller that commits many small chunks directly accumulates
/// unbudgeted heap, and no accounting here would catch it.
const SPILL_MIN_BYTES: usize = 64 << 10;

/// The default compression depth floor: fresh (depth 0) bodies spill
/// uncompressed.
///
/// A fresh chunk is consumed by its first merge with certainty, so
/// compressing it can never save pool bytes for longer than one merge
/// cadence and always costs a full encode plus decode. Depth 1 and beyond
/// have survived a merge and wait geometrically longer for the next, so
/// their compression amortizes.
const DEFAULT_COMPRESS_MIN_DEPTH: u8 = 1;

/// Whether a column is big enough to commit on its own. A monotone
/// threshold, so settle's carry, which grows by whole chunks, cannot step
/// over it.
fn at_commit_size<C: Columnar>(column: &Column<C>) -> bool {
    column.length_in_bytes() >= COMMIT_BYTES - COMMIT_BYTES / 10
}

/// Reconstructs the borrowed columnar view from serialized words, the same
/// zero-copy decode [`Column::borrow`] performs on its `Align` variant.
fn borrow_words<C: Columnar>(words: &[u64]) -> BorrowedOf<'_, C> {
    <BorrowedOf<'_, C>>::from_bytes(&mut indexed::decode(words))
}

/// Narrow a columnar ref to a shorter lifetime, so refs from different
/// borrows, such as a probe column and a chunk's own columns, can be compared
/// (the refs are lifetime-invariant).
#[inline(always)]
fn rr<'b, 'a: 'b, C: Columnar>(item: columnar::Ref<'a, C>) -> columnar::Ref<'b, C> {
    columnar::ContainerOf::<C>::reborrow_ref(item)
}

/// A spilled chunk body: the serialized column in the pool, plus the resident
/// metadata every [`Chunk`] must answer without fetching. That metadata is
/// the record count, the first and last data items (the fence entries
/// [`UnloadChunk::locate`] consults), and the time bounds `extract` consults
/// to pass frontier-disjoint chunks through without loading them.
pub struct SpilledBody<D: Columnar, T> {
    /// Number of updates in the body.
    records: usize,
    /// The first and last data items, as a two-element container. One
    /// container rather than two singletons, so the leaf allocations are not
    /// duplicated per fence.
    fences: D::Container,
    /// The minimal times in the body: a lower bound antichain every
    /// contained time is greater-or-equal to. Folded into `extract`'s
    /// residual frontier when the chunk is kept whole.
    time_lower: Antichain<T>,
    /// The maximal times in the body. Some contained time is
    /// greater-or-equal to a frontier exactly when some maximal time is,
    /// which is `extract`'s ship-whole test. A single element for totally
    /// ordered times, hence the inline capacity.
    time_upper: SmallVec<[T; 1]>,
    /// Whether the body was inserted under the compressing codec. The pool
    /// stores the codec itself and reads decode through it, so this is the
    /// only handle chunk code has on what a body is stored as, and it is
    /// what `survive_merge` consults to decide a body wants migrating.
    /// Deriving that from depth instead would tie it to a single transition
    /// and miss every path that skips it.
    compressed: bool,
    /// The pool chunk holding the serialized column.
    handle: ChunkHandle,
}

/// A sorted, consolidated run of `(D, T, R)` updates, resident or spilled.
///
/// Every chunk carries a generational depth counting the merge cadences it
/// has lived through: fresh chunks are depth 0, a merge output is one
/// generation past its deepest input (saturating at `u8::MAX`, where
/// remerged long-lived chunks stay), a chunk a merge carries forward
/// untouched also gains a generation (see `survive_merge`), and rewrites
/// within a generation (extract, advance, settle coalescing) preserve
/// depth.
///
/// Depth belongs to the chunk, not to the body: a body outlives the chunks
/// that share it, and aging must not depend on whether a caller happens to
/// hold the only reference. At spill time the depth becomes the pool's
/// [`ChunkHints`], so repeatedly merged (older, colder) data lands in deeper
/// eviction bands. Hints are fixed at insert, so a chunk aged without a
/// re-spill keeps the band it spilled into.
pub enum ColumnChunk<D: Columnar, T: Columnar, R: Columnar> {
    /// Body on the heap, shared via `Rc`, with its generational depth.
    Resident(Rc<Column<(D, T, R)>>, u8),
    /// Body in the pool, with its generational depth. See [`SpilledBody`].
    Spilled(Rc<SpilledBody<D, T>>, u8),
}

impl<D: Columnar, T: Columnar, R: Columnar> Clone for ColumnChunk<D, T, R> {
    fn clone(&self) -> Self {
        match self {
            ColumnChunk::Resident(col, depth) => ColumnChunk::Resident(Rc::clone(col), *depth),
            ColumnChunk::Spilled(body, depth) => ColumnChunk::Spilled(Rc::clone(body), *depth),
        }
    }
}

impl<D: Columnar, T: Columnar, R: Columnar> Default for ColumnChunk<D, T, R> {
    fn default() -> Self {
        ColumnChunk::Resident(Rc::new(Column::default()), 0)
    }
}

impl<D: Columnar, T: Columnar, R: Columnar> Accountable for ColumnChunk<D, T, R> {
    fn record_count(&self) -> i64 {
        i64::try_from(self.records()).expect("record count fits i64")
    }
}

impl<D: Columnar, T: Columnar, R: Columnar> ColumnChunk<D, T, R> {
    /// Wrap a sorted, consolidated, non-empty column as a resident chunk of
    /// the youngest generation.
    pub fn from_column(column: Column<(D, T, R)>) -> Self {
        mz_ore::soft_assert_no_log!(!column.is_empty(), "chunks must be non-empty");
        ColumnChunk::Resident(Rc::new(column), 0)
    }

    /// The body as an owned column. A spilled body is copied out of the pool
    /// within this call. A shared resident body is copied.
    pub fn into_column(self) -> Column<(D, T, R)> {
        match self {
            ColumnChunk::Resident(col, _) => {
                Rc::try_unwrap(col).unwrap_or_else(|shared| copy_column(&shared))
            }
            ColumnChunk::Spilled(body, _) => {
                let mut words = Vec::new();
                body.handle.read_into(&mut words);
                Column::Align(words)
            }
        }
    }

    /// True when the body lives in the pool.
    pub fn is_spilled(&self) -> bool {
        matches!(self, ColumnChunk::Spilled(_, _))
    }

    /// The number of updates, from resident state only.
    fn records(&self) -> usize {
        match self {
            ColumnChunk::Resident(col, _) => col.borrow().len(),
            ColumnChunk::Spilled(body, _) => body.records,
        }
    }

    /// The generational depth, from resident state only.
    fn depth(&self) -> u8 {
        match self {
            ColumnChunk::Resident(_, depth) | ColumnChunk::Spilled(_, depth) => *depth,
        }
    }

    /// The first and last data items, from resident state only.
    fn data_span(&self) -> (columnar::Ref<'_, D>, columnar::Ref<'_, D>) {
        match self {
            ColumnChunk::Resident(col, _) => {
                let data = col.borrow().0;
                (data.get(0), data.get(data.len() - 1))
            }
            ColumnChunk::Spilled(body, _) => {
                let fences = body.fences.borrow();
                (fences.get(0), fences.get(1))
            }
        }
    }

    /// Commit a non-empty column at the given generational depth: spill it to
    /// the pool when spilling is on and the body is worth a slot, else keep it
    /// resident.
    fn commit(column: Column<(D, T, R)>, depth: u8) -> Self
    where
        T: Timestamp,
    {
        mz_ore::soft_assert_no_log!(!column.is_empty(), "chunks must be non-empty");
        if let Some(pool) = spill_pool() {
            if column.length_in_bytes() >= SPILL_MIN_BYTES {
                return Self::spill_body(column, &pool, depth);
            }
        }
        ColumnChunk::Resident(Rc::new(column), depth)
    }

    /// Spill a non-empty column into `pool` unconditionally, capturing the
    /// resident fence metadata.
    ///
    /// Generations below the compression depth floor store under the
    /// identity codec: rewritten too soon for compression to amortize, they
    /// stay budgeted and swap-backed while encode and decode reduce to
    /// copies.
    fn spill_body(column: Column<(D, T, R)>, pool: &Pool, depth: u8) -> Self
    where
        T: Timestamp,
    {
        let (codec, compressed) = codec_for_depth(depth);
        let len_bytes = column.length_in_bytes();
        let (time_lower, time_upper) = Self::time_bounds(&column);
        let view = column.borrow();
        let records = view.len();
        let mut fences = D::Container::default();
        fences.push(view.0.get(0));
        fences.push(view.0.get(records - 1));
        let handle = spill_column(column, pool, len_bytes, ChunkHints { depth }, codec);
        ColumnChunk::Spilled(
            Rc::new(SpilledBody {
                records,
                fences,
                time_lower,
                time_upper: time_upper.into(),
                compressed,
                handle,
            }),
            depth,
        )
    }

    /// Age a chunk that a merge carried forward untouched by one generation.
    /// Depth counts merge cadences lived through, not rewrites, so a
    /// pass-through survivor ages like a merged chunk; the bump rides on the
    /// chunk, so it is free whether or not the body is shared.
    ///
    /// An identity-coded body at or past the floor wants migrating, so a
    /// sole owner re-spills it compressed; without the re-spill,
    /// key-disjoint input would keep its whole spilled backlog
    /// identity-coded for as long as it lived. The test is the body's stored
    /// codec against the floor, not a depth transition, so a migration that
    /// cannot happen now (a shared body, no pool installed, a floor lowered
    /// long after the spill) is retried at the next survival rather than
    /// consumed. A shared body is skipped because re-spilling this reference
    /// cannot change what the other holder stores, and the compaction merger
    /// that shares bodies rewrites its clones immediately.
    fn survive_merge(self) -> Self
    where
        T: Timestamp,
    {
        let depth = self.depth().saturating_add(1);
        match self {
            ColumnChunk::Resident(col, _) => ColumnChunk::Resident(col, depth),
            ColumnChunk::Spilled(body, was) => {
                let migrate = !body.compressed && depth >= compress_min_depth();
                if !migrate || Rc::strong_count(&body) > 1 {
                    return ColumnChunk::Spilled(body, depth);
                }
                match spill_pool() {
                    Some(pool) => {
                        let column = ColumnChunk::Spilled(body, was).into_column();
                        Self::spill_body(column, &pool, depth)
                    }
                    None => ColumnChunk::Spilled(body, depth),
                }
            }
        }
    }

    /// The chunk's time bounds: borrowed from the stored metadata for
    /// spilled bodies, computed by a time-column scan for resident ones. The
    /// scan costs less than the copy it lets `extract` avoid when the chunk
    /// passes through whole.
    fn chunk_time_bounds(&self) -> (Cow<'_, Antichain<T>>, Cow<'_, [T]>)
    where
        T: Timestamp,
    {
        match self {
            ColumnChunk::Resident(col, _) => {
                let (lower, upper) = Self::time_bounds(col);
                (Cow::Owned(lower), Cow::Owned(upper))
            }
            ColumnChunk::Spilled(body, _) => (
                Cow::Borrowed(&body.time_lower),
                Cow::Borrowed(&body.time_upper[..]),
            ),
        }
    }

    /// The time bounds of a non-empty column: the antichain of minimal times
    /// (every contained time is greater-or-equal to some element) and the
    /// set of maximal times (some contained time is greater-or-equal to a
    /// frontier exactly when some maximal one is).
    fn time_bounds(column: &Column<(D, T, R)>) -> (Antichain<T>, Vec<T>)
    where
        T: Timestamp,
    {
        let (_, times, _) = column.borrow();
        let mut lower = Antichain::new();
        let mut upper: Vec<T> = Vec::new();
        // One owned time reused across the scan, so times with owned
        // allocations do not allocate per element; the bound sets clone only
        // the elements they retain.
        let mut time = T::minimum();
        for i in 0..times.len() {
            time.copy_from(rr::<T>(times.get(i)));
            if !upper.iter().any(|u| PartialOrder::less_equal(&time, u)) {
                upper.retain(|u| !PartialOrder::less_equal(u, &time));
                upper.push(time.clone());
            }
            lower.insert_ref(&time);
        }
        (lower, upper)
    }
}

/// Copy a column into a fresh `Typed` column via bulk per-leaf extension.
fn copy_column<C: Columnar>(column: &Column<C>) -> Column<C> {
    let view = column.borrow();
    let mut fresh = C::Container::default();
    fresh.extend_from_self(view, 0..view.len());
    Column::Typed(fresh)
}

/// The chunk-side [`ExtentCodec`]: a little-endian `u32` body-length prefix
/// followed by one lz4 block, the framing
/// `lz4_flex::block::compress_prepend_size` produces. Every chunk consumer
/// passes [`LZ4_CODEC`] at insert; the pool itself has no codec opinion.
#[derive(Debug)]
pub struct Lz4Codec;

/// The [`Lz4Codec`] instance chunk consumers pass to
/// [`Pool::insert_with`].
pub static LZ4_CODEC: Lz4Codec = Lz4Codec;

impl ExtentCodec for Lz4Codec {
    fn encode(&self, body: &[u8], out: &mut Vec<u8>) {
        let max_out = lz4_flex::block::get_maximum_output_size(body.len());
        out.resize(4 + max_out, 0);
        let len = u32::try_from(body.len()).expect("chunk bodies are bounded by the size classes");
        out[..4].copy_from_slice(&len.to_le_bytes());
        let compressed = lz4_flex::block::compress_into(body, &mut out[4..])
            .expect("output sized to the maximum");
        out.truncate(4 + compressed);
    }

    fn decode(&self, stored: &[u8], body: &mut [u8]) {
        let prefix: [u8; 4] = stored[..4].try_into().expect("prefix length");
        let len = usize::try_from(u32::from_le_bytes(prefix)).expect("length fits usize");
        assert_eq!(
            len,
            body.len(),
            "destination must match the encoded body length"
        );
        let written = lz4_flex::block::decompress_into(&stored[4..], body)
            .expect("stored bytes hold a valid lz4 block");
        assert_eq!(written, body.len(), "decoded length mismatch");
    }
}

/// Serialize a column into a pool slot. The `Align` variant is already the
/// serialized form and copies in directly. Other variants write their
/// [`ContainerBytes`] encoding through a cursor over the slot memory. Sizing
/// is exact, so a short or overlong write is a contract violation and panics.
fn spill_column<C: Columnar>(
    column: Column<C>,
    pool: &Pool,
    len_bytes: usize,
    hints: ChunkHints,
    codec: &'static dyn ExtentCodec,
) -> ChunkHandle {
    mz_ore::soft_assert_eq_no_log!(len_bytes % 8, 0);
    match column {
        Column::Align(words) => {
            pool.insert_with(words.len(), hints, codec, |dst| dst.copy_from_slice(&words))
        }
        other => pool.insert_with(len_bytes / 8, hints, codec, |dst| {
            let bytes: &mut [u8] = bytemuck::cast_slice_mut(dst);
            let mut cursor = std::io::Cursor::new(bytes);
            other.into_bytes(&mut cursor);
            assert_eq!(
                usize::try_from(cursor.position()).expect("usize position"),
                len_bytes,
                "serialized body must fill the chunk exactly",
            );
        }),
    }
}

/// A column is `Typed`, or becomes one by copy. Merge and settle accumulate
/// into `Typed` targets. Serialized variants arrive from spill reads and
/// remote channels.
fn to_typed<C: Columnar>(column: Column<C>) -> Column<C> {
    match column {
        typed @ Column::Typed(_) => typed,
        other => copy_column(&other),
    }
}

impl<D, T, R> Chunk for ColumnChunk<D, T, R>
where
    D: Columnar,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Timestamp + Lattice + Ord,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>>,
{
    type Time = T;

    /// A nominal record count for the harness's fuel and ladder accounting,
    /// not a bound. Actual chunk sizing is by serialized bytes: `merge` and
    /// `extract` cut output at the [`Column`] ship threshold, and `settle`
    /// grades by `at_commit_size`, so a chunk of narrow records can hold more
    /// records than this and nothing here consults it.
    const TARGET: usize = 65536;

    fn len(&self) -> usize {
        self.records()
    }

    /// [`Column::merge_from`] does the work: gallop bulk-copies for disjoint
    /// runs, semigroup consolidation on equal `(data, time)`, output cut at
    /// the ship threshold.
    ///
    /// Fronts whose data ranges are disjoint never load at all: the resident
    /// fence entries decide, and the lower front moves to the output whole.
    fn merge(in1: &mut VecDeque<Self>, in2: &mut VecDeque<Self>, out: &mut VecDeque<Self>) {
        // Disjoint fast path: when one front lies strictly below the other's
        // first data item (equal boundary data could still interleave on
        // time), the merged prefix through the shared horizon is exactly that
        // front, unchanged.
        let (a_first, a_last) = in1
            .front()
            .expect("caller guarantees non-empty input")
            .data_span();
        let (b_first, b_last) = in2
            .front()
            .expect("caller guarantees non-empty input")
            .data_span();
        let a_low = rr::<D>(a_last) < rr::<D>(b_first);
        let b_low = rr::<D>(b_last) < rr::<D>(a_first);
        if a_low {
            let chunk = in1.pop_front().expect("front observed above");
            out.push_back(chunk.survive_merge());
            return;
        }
        if b_low {
            let chunk = in2.pop_front().expect("front observed above");
            out.push_back(chunk.survive_merge());
            return;
        }

        let a = in1.pop_front().expect("caller guarantees non-empty input");
        let b = in2.pop_front().expect("caller guarantees non-empty input");
        // Merged output is one generation past its deepest input. A survivor
        // (untouched or rewritten from its remainder) keeps its own depth.
        let depths = [a.depth(), b.depth()];
        let out_depth = depths[0].max(depths[1]).saturating_add(1);
        let mut spill_a = match &a {
            ColumnChunk::Spilled(body, _) => Some(Rc::clone(body)),
            ColumnChunk::Resident(_, _) => None,
        };
        let mut spill_b = match &b {
            ColumnChunk::Spilled(body, _) => Some(Rc::clone(body)),
            ColumnChunk::Resident(_, _) => None,
        };
        let mut cols = [a.into_column(), b.into_column()];
        let mut positions = [0usize, 0usize];
        loop {
            let mut result: Column<(D, T, R)> = Column::default();
            let yielded = result.merge_from(&mut cols, &mut positions);
            if !result.is_empty() {
                out.push_back(ColumnChunk::Resident(Rc::new(result), out_depth));
            }
            if !yielded {
                break;
            }
        }
        let [col_a, col_b] = &mut cols;
        // Per input side: the loaded column and the merge's consumed position
        // within it, the side's pre-merge depth, its original spilled body
        // when it had one, and the deque a survivor returns to.
        for (col, pos, depth, spilled, queue) in [
            (col_a, positions[0], depths[0], &mut spill_a, in1),
            (col_b, positions[1], depths[1], &mut spill_b, in2),
        ] {
            let len = col.borrow().len();
            if pos == 0 && len > 0 {
                // Untouched survivor: restore it as it was (the loaded copy
                // is dropped), aged one generation by its survival.
                let chunk = match spilled.take() {
                    Some(body) => ColumnChunk::Spilled(body, depth),
                    None => ColumnChunk::Resident(Rc::new(std::mem::take(col)), depth),
                };
                queue.push_front(chunk.survive_merge());
            } else if pos < len {
                let view = col.borrow();
                let mut rest = <(D, T, R) as Columnar>::Container::default();
                rest.extend_from_self(view, pos..len);
                queue.push_front(ColumnChunk::Resident(Rc::new(Column::Typed(rest)), depth));
            }
        }
    }

    /// Partition one front chunk by `frontier`, folding kept times into
    /// `residual`. One chunk per call, so the harness settles both sides
    /// between chunks. Output is cut at the ship threshold.
    fn extract(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<T>,
        residual: &mut timely::progress::Antichain<T>,
        keep: &mut VecDeque<Self>,
        ship: &mut VecDeque<Self>,
    ) {
        let Some(chunk) = input.pop_front() else {
            return;
        };
        // Whole-chunk pass-through from the resident time bounds: a chunk
        // the frontier is entirely past ships unchanged, one entirely at or
        // past the frontier keeps unchanged. Spilled bodies pass through
        // without a load, a re-commit, or any codec work; only chunks the
        // frontier actually splits are loaded below.
        let (time_lower, time_upper) = chunk.chunk_time_bounds();
        if time_upper.iter().all(|t| !frontier.less_equal(t)) {
            ship.push_back(chunk);
            return;
        }
        if time_lower.elements().iter().all(|m| frontier.less_equal(m)) {
            // The residual must lower-bound every kept time, which is the
            // chunk's lower bound antichain by construction.
            for m in time_lower.elements() {
                residual.insert_ref(m);
            }
            keep.push_back(chunk);
            return;
        }
        // Partitioning rewrites within a generation, so both sides keep the
        // input chunk's depth.
        let depth = chunk.depth();
        let mut col = chunk.into_column();
        let len = col.borrow().len();
        let mut pos = 0;
        let mut keep_col: Column<(D, T, R)> = Column::default();
        let mut ship_col: Column<(D, T, R)> = Column::default();
        // TODO: rewrite the underlying `Column::extract` as two passes, the
        // time column first to find run boundaries, then bulk per-range
        // copies of the remaining leaves.
        // Move a side's accumulation to its queue, at the ship threshold
        // mid-loop, or any non-empty remainder at the end.
        let cut = |col: &mut Column<(D, T, R)>, queue: &mut VecDeque<Self>, force: bool| {
            if !col.is_empty() && (force || at_serialized_capacity(&col.borrow())) {
                queue.push_back(ColumnChunk::Resident(Rc::new(std::mem::take(col)), depth));
            }
        };
        while pos < len {
            col.extract(&mut pos, frontier, residual, &mut keep_col, &mut ship_col);
            if pos < len {
                cut(&mut keep_col, keep, false);
                cut(&mut ship_col, ship, false);
            }
        }
        cut(&mut keep_col, keep, true);
        cut(&mut ship_col, ship, true);
    }

    /// Advance times by `frontier` and consolidate, withholding the trailing
    /// `D` group as the carry unless `done` (its updates may continue in input
    /// this call has not seen).
    ///
    /// The input concatenates into the carry's container, so a group that
    /// grows across many calls is appended to, not rebuilt. Each record is
    /// copied once on arrival, keeping the run linear. Advancing is
    /// lattice-monotone but not order-monotone, so each group's advanced
    /// times are re-sorted before adjacent equal times fold.
    fn advance(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<T>,
        done: bool,
        out: &mut VecDeque<Self>,
    ) {
        let Some(front) = input.pop_front() else {
            return;
        };
        // Advancing rewrites within a generation, so output and carry keep
        // the deepest input depth. Only merges increment.
        let mut depth = front.depth();
        // Concatenate the input into one column, reusing the front chunk's
        // storage when it is exclusively owned (the usual case: it is last
        // call's carry).
        let mut base = to_typed(front.into_column());
        {
            let Column::Typed(base_c) = &mut base else {
                unreachable!("to_typed returns Typed");
            };
            for chunk in input.drain(..) {
                depth = depth.max(chunk.depth());
                let col = chunk.into_column();
                let view = col.borrow();
                base_c.extend_from_self(view, 0..view.len());
            }
        }
        let view = base.borrow();
        let total = view.len();
        if total == 0 {
            return;
        }
        let data = view.0;

        // Giant-group early-out: if the whole input is one `D` group, nothing
        // is provably complete. Unless `done`, push it all back as the carry.
        if !done && data.get(0) == data.get(total - 1) {
            input.push_front(ColumnChunk::Resident(Rc::new(base), depth));
            return;
        }

        // The processing bound: everything, or everything before the trailing
        // `D` group when it must be withheld.
        let end = if done {
            total
        } else {
            let last = data.get(total - 1);
            let mut end = total - 1;
            while end > 0 && data.get(end - 1) == last {
                end -= 1;
            }
            end
        };

        let mut result = <(D, T, R) as Columnar>::Container::default();
        // Per-group scratch: advanced owned times with owned diffs.
        let mut scratch: Vec<(T, R)> = Vec::new();
        let mut index = 0;
        // Cut output at the commit size, checked amortized by emitted records
        // (the size test walks the container's leaves, so probing it per
        // record would be quadratic). Records, not groups: a single group may
        // carry arbitrarily many advanced times, and a cut is legal anywhere
        // in the sorted sequence, so bounding by records keeps the largest
        // possible output chunk within one check period of the target. It
        // must not outgrow the pool's largest size class, past which a body
        // degrades to a permanently resident heap chunk.
        const CUT_CHECK_RECORDS: usize = 1024;
        let mut records_since_check = 0usize;
        // TODO: the output leaves are addressed independently, so a group
        // that folds nothing (no time collisions, no zeroed diffs) could bulk
        // `extend_from_self` the D leaf over the whole group range and push
        // only the advanced times and diffs per record, and a singleton group
        // (the common case for mostly-unique D) could skip the scratch and
        // sort round trip entirely.
        while index < end {
            let group_d = data.get(index);
            scratch.clear();
            while index < end && data.get(index) == group_d {
                let (_, t, r) = view.get(index);
                let mut owned_t = T::into_owned(t);
                owned_t.advance_by(frontier);
                scratch.push((owned_t, R::into_owned(r)));
                index += 1;
            }
            scratch.sort_by(|a, b| a.0.cmp(&b.0));
            let mut run = scratch.drain(..).peekable();
            while let Some((t, mut r)) = run.next() {
                while run.peek().is_some_and(|(t2, _)| *t2 == t) {
                    let (_, r2) = run.next().expect("peeked");
                    r.plus_equals(&r2);
                }
                if !r.is_zero() {
                    result.0.push(group_d);
                    result.1.push(&t);
                    result.2.push(&r);
                    records_since_check += 1;
                    if records_since_check >= CUT_CHECK_RECORDS {
                        records_since_check = 0;
                        if u64::cast_from(indexed::length_in_words(&result.borrow()))
                            >= u64::cast_from(COMMIT_BYTES / 8)
                        {
                            out.push_back(ColumnChunk::Resident(
                                Rc::new(Column::Typed(std::mem::take(&mut result))),
                                depth,
                            ));
                        }
                    }
                }
            }
        }
        if !result.is_empty() {
            out.push_back(ColumnChunk::Resident(Rc::new(Column::Typed(result)), depth));
        }

        // Rebuild the withheld trailing group as the carry.
        if end < total {
            let mut carry = <(D, T, R) as Columnar>::Container::default();
            carry.extend_from_self(view, end..total);
            input.push_front(ColumnChunk::Resident(Rc::new(Column::Typed(carry)), depth));
        }
    }

    /// Grade by serialized bytes and commit: spilled chunks pass through
    /// untouched, resident chunks at the commit size commit as they are, and
    /// smaller neighbors coalesce until the accumulation reaches it.
    /// Committing is the spill hook (see `ColumnChunk::commit`). A
    /// sub-threshold tail is withheld as the carry unless `done`.
    fn settle(input: &mut VecDeque<Self>, done: bool, out: &mut VecDeque<Self>) {
        // Coalescing rewrites within a generation, so the carry commits at
        // the deepest depth among its constituent chunks.
        let mut carry: Option<(Column<(D, T, R)>, u8)> = None;
        while let Some(chunk) = input.pop_front() {
            let (rc, depth) = match chunk {
                spilled @ ColumnChunk::Spilled(_, _) => {
                    if let Some((col, depth)) = carry.take() {
                        out.push_back(ColumnChunk::commit(col, depth));
                    }
                    out.push_back(spilled);
                    continue;
                }
                ColumnChunk::Resident(rc, depth) => (rc, depth),
            };
            let full = at_commit_size(&rc);
            // A sub-threshold chunk coalesces into the open carry by borrow,
            // never unwrapping a shared body.
            if !full && let Some((mut acc, acc_depth)) = carry.take() {
                let Column::Typed(acc_c) = &mut acc else {
                    unreachable!("carry is always Typed");
                };
                let view = rc.borrow();
                acc_c.extend_from_self(view, 0..view.len());
                let acc_depth = acc_depth.max(depth);
                if at_commit_size(&acc) {
                    out.push_back(ColumnChunk::commit(acc, acc_depth));
                } else {
                    carry = Some((acc, acc_depth));
                }
                continue;
            }
            // Otherwise any open carry flushes, and the chunk either commits
            // whole or opens the next carry.
            if let Some((acc, acc_depth)) = carry.take() {
                out.push_back(ColumnChunk::commit(acc, acc_depth));
            }
            let col = Rc::try_unwrap(rc).unwrap_or_else(|rc| copy_column(&rc));
            if full {
                out.push_back(ColumnChunk::commit(col, depth));
            } else {
                carry = Some((to_typed(col), depth));
            }
        }
        if let Some((col, depth)) = carry {
            if done {
                out.push_back(ColumnChunk::commit(col, depth));
            } else {
                input.push_front(ColumnChunk::Resident(Rc::new(col), depth));
            }
        }
    }
}

/// Append every update in `view` whose key matches a probe at or after
/// `*probe_index` into `staging`, per the [`UnloadChunk`] consume-index
/// protocol: probes strictly below the view's last key are consumed, a probe
/// equal to it is extracted but left for the next chunk.
fn extract_view_into<'v, 'p, K, V, T, R>(
    view: BorrowedOf<'v, ((K, V), T, R)>,
    probes: BorrowedOf<'p, K>,
    probe_index: &mut usize,
    staging: &mut <((K, V), T, R) as Columnar>::Container,
) where
    K: Columnar,
    V: Columnar,
    T: Columnar,
    R: Columnar,
    for<'b> columnar::Ref<'b, K>: Copy + Ord,
{
    let keys = view.0.0;
    let len = keys.len();
    let last = keys.get(len - 1);
    let count = probes.len();
    let mut pos = 0;
    while *probe_index < count {
        let probe = probes.get(*probe_index);
        mz_ore::soft_assert_no_log!(
            *probe_index == 0 || rr::<K>(probes.get(*probe_index - 1)) < rr::<K>(probe),
            "probe keys must be sorted and deduplicated"
        );
        if rr::<K>(probe) > rr::<K>(last) {
            return;
        }
        gallop(len, &mut pos, |i| rr::<K>(keys.get(i)) < rr::<K>(probe));
        let start = pos;
        while pos < len && rr::<K>(keys.get(pos)) == rr::<K>(probe) {
            pos += 1;
        }
        staging.extend_from_self(view, start..pos);
        if rr::<K>(probe) == rr::<K>(last) {
            return;
        }
        *probe_index += 1;
    }
}

impl<K, V, T, R> UnloadChunk for ColumnChunk<(K, V), T, R>
where
    K: Columnar,
    for<'a> columnar::Ref<'a, K>: Copy + Ord,
    V: Columnar,
    for<'a> columnar::Ref<'a, V>: Copy + Ord,
    T: Columnar + Default + Timestamp + Lattice + Ord,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>>,
{
    /// The flat columnar accumulation. Appends are bulk column-range copies,
    /// and a group straddling chunks stitches by plain concatenation.
    type Staging = <((K, V), T, R) as Columnar>::Container;

    /// A borrowed key column, e.g. of a `Column<K>` the consumer assembled
    /// from its sorted, deduplicated probe keys.
    type Probes<'a> = BorrowedOf<'a, K>;

    fn probe_count(probes: Self::Probes<'_>) -> usize {
        probes.len()
    }

    fn locate(&self, probes: Self::Probes<'_>, probe_index: usize) -> std::cmp::Ordering {
        let probe = probes.get(probe_index);
        // A data ref is a `(key ref, val ref)` tuple, so the key fences are a
        // projection of the data fences.
        let (first, last) = self.data_span();
        let (first, last) = (first.0, last.0);
        if rr::<K>(probe) < rr::<K>(first) {
            std::cmp::Ordering::Less
        } else if rr::<K>(probe) > rr::<K>(last) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    }

    fn extract_into(
        &self,
        probes: Self::Probes<'_>,
        probe_index: &mut usize,
        staging: &mut Self::Staging,
    ) {
        match self {
            ColumnChunk::Resident(col, _) => {
                extract_view_into::<K, V, T, R>(col.borrow(), probes, probe_index, staging);
            }
            ColumnChunk::Spilled(body, _) => with_scratch(|scratch| {
                // NOTE: deliberately the non-admitting read. One probe set
                // touching a chunk is weak evidence it will be touched again,
                // and probing a spilled trace must not accrete it back into
                // residency. The cost is a full decode per probe set against
                // an evicted chunk.
                body.handle.read_into(scratch);
                let view = borrow_words::<((K, V), T, R)>(scratch);
                extract_view_into::<K, V, T, R>(view, probes, probe_index, staging);
            }),
        }
    }

    fn fetch_into(&self, staging: &mut Self::Staging) {
        match self {
            ColumnChunk::Resident(col, _) => {
                let view = col.borrow();
                staging.extend_from_self(view, 0..view.len());
            }
            ColumnChunk::Spilled(body, _) => with_scratch(|scratch| {
                body.handle.read_into(scratch);
                let view = borrow_words::<((K, V), T, R)>(scratch);
                staging.extend_from_self(view, 0..view.len());
            }),
        }
    }
}

/// A batch builder over [`ColumnChunk`] input that delegates to a builder
/// over [`Column`] input, loading each chunk's body as it is pushed.
///
/// This is the adapter that lets a [`ChunkBatcher`] feed the existing
/// column-input batch builders (and through them the existing spine layouts):
/// the batcher's chains carry pool-spillable chunks, and bodies are read back
/// copy-out only at the seal, one chunk at a time.
///
/// [`ChunkBatcher`]: differential_dataflow::trace::chunk::ChunkBatcher
pub struct UnchunkBuilder<Bu, D: Columnar, T: Columnar, R: Columnar> {
    inner: Bu,
    _marker: std::marker::PhantomData<(D, T, R)>,
}

impl<Bu, D, T, R> differential_dataflow::trace::Builder for UnchunkBuilder<Bu, D, T, R>
where
    Bu: differential_dataflow::trace::Builder<Input = Column<(D, T, R)>>,
    D: Columnar + 'static,
    T: Columnar + 'static,
    R: Columnar + 'static,
{
    type Input = ColumnChunk<D, T, R>;
    type Time = Bu::Time;
    type Output = Bu::Output;

    fn with_capacity(keys: usize, vals: usize, upds: usize) -> Self {
        Self {
            inner: Bu::with_capacity(keys, vals, upds),
            _marker: std::marker::PhantomData,
        }
    }

    fn push(&mut self, chunk: &mut Self::Input) {
        let mut column = std::mem::take(chunk).into_column();
        self.inner.push(&mut column);
    }

    fn done(
        self,
        description: differential_dataflow::trace::Description<Self::Time>,
    ) -> Self::Output {
        self.inner.done(description)
    }

    fn seal(
        chain: &mut Vec<Self::Input>,
        description: differential_dataflow::trace::Description<Self::Time>,
    ) -> Self::Output {
        // One chunk at a time through `push`, so peak transient memory is a
        // single loaded body rather than the whole chain at once.
        let mut builder = Self::new();
        for chunk in chain.iter_mut() {
            builder.push(chunk);
        }
        chain.clear();
        builder.done(description)
    }
}

/// A chunker for `arrange_core` over [`ColumnChunk`]s: sorts and consolidates
/// raw input columns through a [`ColumnChunker`] and wraps its output chunks.
pub struct ChunkChunker<D: Columnar, T: Columnar, R: Columnar> {
    inner: ColumnChunker<(D, T, R)>,
    staged: ColumnChunk<D, T, R>,
}

impl<D, T, R> Default for ChunkChunker<D, T, R>
where
    D: Columnar,
    T: Columnar,
    R: Columnar,
    ColumnChunker<(D, T, R)>: Default,
{
    fn default() -> Self {
        Self {
            inner: Default::default(),
            staged: Default::default(),
        }
    }
}

impl<'a, D, T, R> PushInto<&'a mut Column<(D, T, R)>> for ChunkChunker<D, T, R>
where
    D: Columnar,
    T: Columnar,
    R: Columnar,
    ColumnChunker<(D, T, R)>: PushInto<&'a mut Column<(D, T, R)>>,
{
    fn push_into(&mut self, item: &'a mut Column<(D, T, R)>) {
        self.inner.push_into(item);
    }
}

impl<D, T, R> ContainerBuilder for ChunkChunker<D, T, R>
where
    D: Columnar + 'static,
    T: Columnar + 'static,
    R: Columnar + 'static,
    ColumnChunker<(D, T, R)>: ContainerBuilder<Container = Column<(D, T, R)>>,
{
    type Container = ColumnChunk<D, T, R>;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        let col = self.inner.extract()?;
        self.staged = ColumnChunk::from_column(std::mem::take(col));
        Some(&mut self.staged)
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        let col = self.inner.finish()?;
        self.staged = ColumnChunk::from_column(std::mem::take(col));
        Some(&mut self.staged)
    }
}

#[cfg(test)]
mod tests {
    //! Property tests for the [`Chunk`] and [`UnloadChunk`] contracts on
    //! [`ColumnChunk`].
    //!
    //! Strategy: generate sorted+consolidated inputs (the chunk invariant),
    //! drive the trait methods the way the differential harness does, and
    //! compare against brute-force references on owned tuples. Test types are
    //! `D = (u64, u64)`, `T = u64`, `R = i64` from small ranges so equal-key
    //! collisions are common and consolidation actually runs.

    use differential_dataflow::trace::chunk::{ChunkBatch, ChunkBatcher};
    use differential_dataflow::trace::{Batcher, Description};
    use mz_ore::pool::Pool;
    use proptest::prelude::*;
    use timely::container::PushInto;
    use timely::progress::Antichain;

    use crate::columnar::unload::UnloadBatch;

    use super::*;

    type Tuple = ((u64, u64), u64, i64);
    type TestChunk = ColumnChunk<(u64, u64), u64, i64>;

    /// The delegated codec's stored form is byte-identical to the extent
    /// store's previous hard-coded framing: a little-endian `u32`
    /// body-length prefix followed by one lz4 block, which is exactly what
    /// `compress_prepend_size` produces.
    #[mz_ore::test]
    fn lz4_codec_matches_the_previous_extent_framing() {
        let body: Vec<u8> = (0..100_000u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut stored = Vec::new();
        LZ4_CODEC.encode(&body, &mut stored);
        assert_eq!(stored, lz4_flex::block::compress_prepend_size(&body));
        let mut round = vec![0u8; body.len()];
        LZ4_CODEC.decode(&stored, &mut round);
        assert_eq!(round, body);
    }

    #[mz_ore::test]
    #[should_panic(expected = "destination must match")]
    fn lz4_codec_decode_length_mismatch_panics() {
        let mut stored = Vec::new();
        LZ4_CODEC.encode(&[7u8; 64], &mut stored);
        let mut short = vec![0u8; 32];
        LZ4_CODEC.decode(&stored, &mut short);
    }

    /// Reference consolidation: sort by `(data, time)`, sum diffs over equal
    /// pairs, drop zeros.
    fn consolidate(mut v: Vec<Tuple>) -> Vec<Tuple> {
        v.sort();
        let mut out: Vec<Tuple> = Vec::new();
        for (d, t, r) in v {
            if let Some(last) = out.last_mut() {
                if last.0 == d && last.1 == t {
                    last.2 += r;
                    continue;
                }
            }
            out.push((d, t, r));
        }
        out.retain(|x| x.2 != 0);
        out
    }

    fn arb_consolidated() -> impl Strategy<Value = Vec<Tuple>> {
        prop::collection::vec(((0u64..5, 0u64..5), 0u64..4, -3i64..=3i64), 0..40)
            .prop_map(consolidate)
    }

    fn build_column(v: &[Tuple]) -> Column<Tuple> {
        let mut col: Column<Tuple> = Default::default();
        for tup in v {
            col.push_into(*tup);
        }
        col
    }

    fn collect_column(col: &Column<Tuple>) -> Vec<Tuple> {
        col.borrow()
            .into_index_iter()
            .map(|((k, v), t, r)| {
                (
                    (u64::into_owned(k), u64::into_owned(v)),
                    u64::into_owned(t),
                    i64::into_owned(r),
                )
            })
            .collect()
    }

    fn collect_chunks(chunks: impl IntoIterator<Item = TestChunk>) -> Vec<Tuple> {
        chunks
            .into_iter()
            .flat_map(|chunk| collect_column(&chunk.into_column()))
            .collect()
    }

    fn collect_staging(staging: &<Tuple as Columnar>::Container) -> Vec<Tuple> {
        staging
            .borrow()
            .into_index_iter()
            .map(|((k, v), t, r)| {
                (
                    (u64::into_owned(k), u64::into_owned(v)),
                    u64::into_owned(t),
                    i64::into_owned(r),
                )
            })
            .collect()
    }

    /// Cut consolidated data into non-empty chunks at the given points.
    fn chunked(data: &[Tuple], cuts: &[usize]) -> VecDeque<TestChunk> {
        let mut chunks = VecDeque::new();
        let mut start = 0;
        for cut in cuts {
            let end = (start + 1 + cut % 7).min(data.len());
            if end > start {
                chunks.push_back(ColumnChunk::from_column(build_column(&data[start..end])));
                start = end;
            }
        }
        if start < data.len() {
            chunks.push_back(ColumnChunk::from_column(build_column(&data[start..])));
        }
        chunks
    }

    /// The chunked cut, with every chunk force-spilled through a private pool
    /// (bounds captured, bodies in the pool) regardless of size thresholds.
    fn chunked_spilled(data: &[Tuple], cuts: &[usize], pool: &Pool) -> VecDeque<TestChunk> {
        chunked(data, cuts)
            .into_iter()
            .map(|chunk| force_spill(chunk, pool))
            .collect()
    }

    /// Whether a spilled chunk's body is stored compressed. Deliberately
    /// does not retain the body: an `Rc` held across a `survive_merge` would
    /// itself make the body shared and suppress the migration under test.
    fn body_compressed(chunk: &TestChunk) -> bool {
        match chunk {
            ColumnChunk::Spilled(body, _) => body.compressed,
            ColumnChunk::Resident(_, _) => panic!("chunk must be spilled"),
        }
    }

    /// Spill one chunk through `pool`, bypassing the size threshold and
    /// keeping the chunk's depth.
    fn force_spill(chunk: TestChunk, pool: &Pool) -> TestChunk {
        let depth = chunk.depth();
        TestChunk::spill_body(chunk.into_column(), pool, depth)
    }

    /// A single pool shared by every test in the module. A pool reserves a
    /// large slab of address space, so one per test (let alone per proptest
    /// case) exhausts the VM map under parallel test threads.
    fn test_pool() -> Pool {
        static POOL: std::sync::OnceLock<Pool> = std::sync::OnceLock::new();
        POOL.get_or_init(|| Pool::new().expect("pool creation"))
            .clone()
    }

    proptest! {
        /// A full batcher round trip: push chunked inputs, seal everything,
        /// and compare with the reference consolidation of the union.
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn batcher_round_trip(
            inputs in prop::collection::vec(arb_consolidated(), 1..6),
            cuts in prop::collection::vec(0usize..7, 0..8),
        ) {
            let mut batcher: ChunkBatcher<TestChunk> = Batcher::new(None, 0);
            let mut union = Vec::new();
            for input in &inputs {
                Extend::extend(&mut union, input.iter().copied());
                for chunk in chunked(input, &cuts) {
                    batcher.push_into(chunk);
                }
            }
            // An empty upper ships everything.
            let (sealed, _description) = batcher.seal(Antichain::new());
            prop_assert_eq!(collect_chunks(sealed), consolidate(union));
        }

        /// The same round trip over force-spilled inputs: merge and extract
        /// read bodies back from the pool call-scoped.
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn batcher_round_trip_spilled(
            inputs in prop::collection::vec(arb_consolidated(), 1..4),
            cuts in prop::collection::vec(0usize..7, 0..6),
        ) {
            let pool = test_pool();
            let mut batcher: ChunkBatcher<TestChunk> = Batcher::new(None, 0);
            let mut union = Vec::new();
            for input in &inputs {
                Extend::extend(&mut union, input.iter().copied());
                for chunk in chunked_spilled(input, &cuts, &pool) {
                    batcher.push_into(chunk);
                }
            }
            let (sealed, _description) = batcher.seal(Antichain::new());
            prop_assert_eq!(collect_chunks(sealed), consolidate(union));
        }

        /// Sealing at an intermediate upper partitions by time and reports
        /// the kept lower envelope as the frontier.
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn seal_partitions_by_time(
            input in arb_consolidated(),
            cuts in prop::collection::vec(0usize..7, 0..8),
            upper in 0u64..5,
        ) {
            let mut batcher: ChunkBatcher<TestChunk> = Batcher::new(None, 0);
            for chunk in chunked(&input, &cuts) {
                batcher.push_into(chunk);
            }
            let (shipped, _) = batcher.seal(Antichain::from_elem(upper));
            let expected_shipped: Vec<Tuple> =
                input.iter().copied().filter(|(_, t, _)| *t < upper).collect();
            prop_assert_eq!(collect_chunks(shipped), consolidate(expected_shipped));

            let kept_min = input.iter().filter(|(_, t, _)| *t >= upper).map(|(_, t, _)| *t).min();
            let frontier = batcher.frontier().to_owned();
            prop_assert_eq!(frontier.elements().first().copied(), kept_min);

            let (rest, _) = batcher.seal(Antichain::new());
            let expected_rest: Vec<Tuple> =
                input.iter().copied().filter(|(_, t, _)| *t >= upper).collect();
            prop_assert_eq!(collect_chunks(rest), consolidate(expected_rest));
        }

        /// The intermediate-upper partition of `seal_partitions_by_time`, over
        /// force-spilled inputs: bodies read back from the pool and split by
        /// time in one seal.
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn seal_partitions_by_time_spilled(
            input in arb_consolidated(),
            cuts in prop::collection::vec(0usize..7, 0..8),
            upper in 0u64..5,
        ) {
            let pool = test_pool();
            let mut batcher: ChunkBatcher<TestChunk> = Batcher::new(None, 0);
            for chunk in chunked_spilled(&input, &cuts, &pool) {
                batcher.push_into(chunk);
            }
            let (shipped, _) = batcher.seal(Antichain::from_elem(upper));
            let expected_shipped: Vec<Tuple> =
                input.iter().copied().filter(|(_, t, _)| *t < upper).collect();
            prop_assert_eq!(collect_chunks(shipped), consolidate(expected_shipped));

            let kept_min = input.iter().filter(|(_, t, _)| *t >= upper).map(|(_, t, _)| *t).min();
            let frontier = batcher.frontier().to_owned();
            prop_assert_eq!(frontier.elements().first().copied(), kept_min);

            let (rest, _) = batcher.seal(Antichain::new());
            let expected_rest: Vec<Tuple> =
                input.iter().copied().filter(|(_, t, _)| *t >= upper).collect();
            prop_assert_eq!(collect_chunks(rest), consolidate(expected_rest));
        }

        /// `advance` equals per-record time advancement plus reference
        /// consolidation, including across a `done = false` carry.
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn advance_matches_reference(
            input in arb_consolidated(),
            cuts in prop::collection::vec(0usize..7, 0..8),
            frontier_elem in 0u64..5,
        ) {
            let frontier = Antichain::from_elem(frontier_elem);
            let mut chunks = chunked(&input, &cuts);
            let mut out = VecDeque::new();
            TestChunk::advance(&mut chunks, frontier.borrow(), false, &mut out);
            TestChunk::advance(&mut chunks, frontier.borrow(), true, &mut out);
            prop_assert!(chunks.is_empty());

            let expected = consolidate(
                input
                    .iter()
                    .map(|&(d, mut t, r)| {
                        t.advance_by(frontier.borrow());
                        (d, t, r)
                    })
                    .collect(),
            );
            prop_assert_eq!(collect_chunks(out), expected);
        }

        /// `settle` preserves contents and order, moves everything on `done`,
        /// and coalesces small neighbors.
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn settle_preserves_and_packs(
            input in arb_consolidated(),
            cuts in prop::collection::vec(0usize..7, 1..8),
        ) {
            let mut chunks = chunked(&input, &cuts);
            let mut out = VecDeque::new();
            TestChunk::settle(&mut chunks, true, &mut out);
            prop_assert!(chunks.is_empty());
            // Test chunks are far below the byte threshold, so maximal
            // packing coalesces everything into a single chunk.
            prop_assert!(out.len() <= 1);
            prop_assert_eq!(collect_chunks(out), input);
        }

        /// `ChunkBatch::extract_into` over sorted, deduplicated probe keys
        /// equals the reference filter, resident and spilled alike, straddled
        /// keys included.
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn unload_extract_matches_filter(
            input in arb_consolidated(),
            cuts in prop::collection::vec(0usize..7, 0..8),
            probe_keys in prop::collection::btree_set(0u64..6, 0..6),
            spill in any::<bool>(),
        ) {
            prop_assume!(!input.is_empty());
            let pool = test_pool();
            let chunks: Vec<TestChunk> = if spill {
                chunked_spilled(&input, &cuts, &pool).into()
            } else {
                chunked(&input, &cuts).into()
            };
            let description = Description::new(
                Antichain::from_elem(0u64),
                Antichain::new(),
                Antichain::from_elem(0u64),
            );
            let batch = ChunkBatch::new(chunks, description);

            let mut probe_col = <u64 as Columnar>::Container::default();
            for key in &probe_keys {
                probe_col.push(*key);
            }
            let mut staging = <Tuple as Columnar>::Container::default();
            batch.extract_into(probe_col.borrow(), &mut staging);

            let expected: Vec<Tuple> = input
                .iter()
                .copied()
                .filter(|((k, _), _, _)| probe_keys.contains(k))
                .collect();
            prop_assert_eq!(collect_staging(&staging), expected);

            // The scan path reproduces the batch exactly, resident and
            // spilled alike.
            let mut staging = <Tuple as Columnar>::Container::default();
            batch.fetch_into(&mut staging);
            prop_assert_eq!(collect_staging(&staging), input);
        }
    }

    /// `locate` answers the three-way span comparison for every probe
    /// placement: below, within, and past the chunk's keys.
    #[mz_ore::test]
    fn locate_spans_keys() {
        let chunk = ColumnChunk::from_column(build_column(&[
            ((2, 0), 0, 1),
            ((4, 0), 0, 1),
            ((6, 0), 0, 1),
        ]));
        let mut probe_col = <u64 as Columnar>::Container::default();
        for key in [0u64, 2, 3, 6, 9] {
            probe_col.push(key);
        }
        let probes = probe_col.borrow();
        use std::cmp::Ordering::*;
        let expected = [Less, Equal, Equal, Equal, Greater];
        for (index, expected) in expected.iter().enumerate() {
            assert_eq!(chunk.locate(probes, index), *expected, "probe {index}");
        }
    }

    /// Collect chunk contents while asserting each chunk's serialized size
    /// stays within `bound` bytes.
    fn collect_bounded(chunks: impl IntoIterator<Item = TestChunk>, bound: usize) -> Vec<Tuple> {
        let mut collected = Vec::new();
        for chunk in chunks {
            let col = chunk.into_column();
            let bytes = col.length_in_bytes();
            assert!(bytes <= bound, "chunk of {bytes} bytes exceeds {bound}");
            Extend::extend(&mut collected, collect_column(&col));
        }
        collected
    }

    /// Advancing a large input cuts the output into several chunks near the
    /// ship threshold, and their concatenation is the reference result.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn advance_cuts_large_output() {
        let records: Vec<Tuple> = (0..300_000u64).map(|k| ((k, 0), 0, 1)).collect();
        let mut input = VecDeque::from([ColumnChunk::from_column(build_column(&records))]);
        let frontier = Antichain::from_elem(0u64);
        let mut out = VecDeque::new();
        TestChunk::advance(&mut input, frontier.borrow(), true, &mut out);
        assert!(input.is_empty());
        assert!(
            out.len() >= 2,
            "expected a cut output, got {} chunk(s)",
            out.len()
        );
        assert_eq!(collect_bounded(out, 2 * COMMIT_BYTES), records);
    }

    /// An input that is entirely one `D` group is withheld whole as the
    /// carry unless `done`: none of it is provably complete.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn advance_withholds_giant_group() {
        let records: Vec<Tuple> = (0..100u64).map(|t| ((7, 7), t, 1)).collect();
        let mut input: VecDeque<TestChunk> = VecDeque::new();
        for piece in records.chunks(30) {
            input.push_back(ColumnChunk::from_column(build_column(piece)));
        }
        let frontier = Antichain::from_elem(50u64);
        let mut out = VecDeque::new();
        TestChunk::advance(&mut input, frontier.borrow(), false, &mut out);
        assert!(out.is_empty(), "nothing may ship from a single open group");
        assert_eq!(input.len(), 1, "the whole input becomes one carry chunk");
        // Sealing the carry advances and consolidates it.
        TestChunk::advance(&mut input, frontier.borrow(), true, &mut out);
        assert!(input.is_empty());
        let advanced = records.iter().map(|&(d, t, r)| (d, t.max(50), r)).collect();
        assert_eq!(collect_chunks(out), consolidate(advanced));
    }

    /// Chunks the extract frontier does not split pass through whole from
    /// their resident time bounds: spilled bodies land on their side still
    /// spilled, with no load or re-commit, and a kept chunk's minimal times
    /// feed the residual frontier.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn extract_passes_frontier_disjoint_chunks_through() {
        set_spill_override(Some(test_pool()));
        let low: Vec<Tuple> = (0..20_000u64).map(|i| ((i, 0), i % 4, 1)).collect();
        let high: Vec<Tuple> = (0..20_000u64).map(|i| ((i, 0), 6 + i % 4, 1)).collect();
        let spilled_chunk = |data: &[Tuple]| {
            let chunk = TestChunk::commit(build_column(&consolidate(data.to_vec())), 1);
            assert!(chunk.is_spilled());
            chunk
        };

        // A frontier between the two chunks' time ranges: the low chunk
        // ships whole and the high chunk keeps whole, both still spilled
        // (no load, no re-commit), and the residual is the kept chunk's
        // minimal time.
        let mut input = VecDeque::from([spilled_chunk(&low), spilled_chunk(&high)]);
        let frontier = Antichain::from_elem(5u64);
        let mut residual = Antichain::new();
        let (mut keep, mut ship) = (VecDeque::new(), VecDeque::new());
        while !input.is_empty() {
            TestChunk::extract(
                &mut input,
                frontier.borrow(),
                &mut residual,
                &mut keep,
                &mut ship,
            );
        }
        assert_eq!(ship.len(), 1);
        assert!(ship[0].is_spilled(), "shipped whole: body untouched");
        assert_eq!(keep.len(), 1);
        assert!(keep[0].is_spilled(), "kept whole: body untouched");
        assert_eq!(residual, Antichain::from_elem(6));
        let shipped = ship.pop_front().unwrap().into_column();
        assert_eq!(collect_column(&shipped), consolidate(low));
        let kept = keep.pop_front().unwrap().into_column();
        assert_eq!(collect_column(&kept), consolidate(high));
        set_spill_override(None);
    }

    /// Extracting a large chunk at an intermediate frontier cuts both sides
    /// into several chunks and partitions exactly by time.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn extract_cuts_large_output() {
        let records: Vec<Tuple> = (0..300_000u64).map(|k| ((k, 0), k % 2, 1)).collect();
        let mut input = VecDeque::from([ColumnChunk::from_column(build_column(&records))]);
        let frontier = Antichain::from_elem(1u64);
        let mut residual = Antichain::new();
        let (mut keep, mut ship) = (VecDeque::new(), VecDeque::new());
        while !input.is_empty() {
            TestChunk::extract(
                &mut input,
                frontier.borrow(),
                &mut residual,
                &mut keep,
                &mut ship,
            );
        }
        assert!(
            keep.len() >= 2,
            "expected a cut keep side, got {} chunk(s)",
            keep.len()
        );
        assert!(
            ship.len() >= 2,
            "expected a cut ship side, got {} chunk(s)",
            ship.len()
        );
        let kept: Vec<Tuple> = records.iter().copied().filter(|r| r.1 >= 1).collect();
        let shipped: Vec<Tuple> = records.iter().copied().filter(|r| r.1 < 1).collect();
        assert_eq!(collect_bounded(keep, 2 * COMMIT_BYTES), kept);
        assert_eq!(collect_bounded(ship, 2 * COMMIT_BYTES), shipped);
        assert_eq!(residual, Antichain::from_elem(1));
    }

    /// `locate` answers from resident metadata on spilled chunks and follows
    /// the probe-relative-to-span convention.
    #[mz_ore::test]
    fn locate_uses_resident_bounds() {
        let pool = test_pool();
        let data: Vec<Tuple> = vec![((2, 0), 0, 1), ((4, 0), 0, 1)];
        let chunk = force_spill(ColumnChunk::from_column(build_column(&data)), &pool);

        let mut probe_col = <u64 as Columnar>::Container::default();
        for key in [1u64, 3, 5] {
            probe_col.push(key);
        }
        let probes = probe_col.borrow();
        assert_eq!(chunk.locate(probes, 0), std::cmp::Ordering::Less);
        assert_eq!(chunk.locate(probes, 1), std::cmp::Ordering::Equal);
        assert_eq!(chunk.locate(probes, 2), std::cmp::Ordering::Greater);
    }

    /// A body large enough to spill round-trips through the pool with resident
    /// metadata intact, and the batcher produces spilled sealed output.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn spill_round_trip() {
        set_spill_override(Some(test_pool()));

        let data: Vec<Tuple> = (0..40_000u64)
            .map(|i| ((i / 4, i % 4), i % 8, 1i64))
            .collect();
        let data = consolidate(data);

        let column = build_column(&data);
        let committed = TestChunk::commit(column, 0);
        assert!(committed.is_spilled(), "large body must spill");
        assert_eq!(committed.len(), data.len());
        assert_eq!(collect_column(&committed.clone().into_column()), data);

        let mut batcher: ChunkBatcher<TestChunk> = Batcher::new(None, 0);
        for piece in data.chunks(10_000) {
            batcher.push_into(ColumnChunk::from_column(build_column(piece)));
        }
        let (sealed, _) = batcher.seal(Antichain::new());
        assert!(
            sealed.iter().any(ColumnChunk::is_spilled),
            "sealed output should contain spilled chunks",
        );
        assert_eq!(collect_chunks(sealed), data);

        set_spill_override(None);
    }

    /// Merging spilled chains loads bodies call-scoped and consolidates
    /// correctly, and an untouched survivor keeps its spilled body.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn merge_spilled_chains() {
        set_spill_override(Some(test_pool()));

        let a: Vec<Tuple> = (0..20_000u64).map(|i| ((i, 0), 0, 1i64)).collect();
        let b: Vec<Tuple> = (0..20_000u64).map(|i| ((i, 0), 0, 2i64)).collect();

        let mut in1 = VecDeque::from([TestChunk::commit(build_column(&a), 0)]);
        let mut in2 = VecDeque::from([TestChunk::commit(build_column(&b), 0)]);
        assert!(in1[0].is_spilled() && in2[0].is_spilled());

        let mut out = VecDeque::new();
        while !in1.is_empty() && !in2.is_empty() {
            TestChunk::merge(&mut in1, &mut in2, &mut out);
        }
        for tail in in1.drain(..).chain(in2.drain(..)) {
            out.push_back(tail);
        }

        let expected: Vec<Tuple> = (0..20_000u64).map(|i| ((i, 0), 0, 3i64)).collect();
        assert_eq!(collect_chunks(out), expected);

        set_spill_override(None);
    }

    /// A merge whose fronts have disjoint key ranges pushes the untouched
    /// survivor back in its original (spilled) form rather than rewriting it.
    #[mz_ore::test]
    fn merge_untouched_survivor_stays_spilled() {
        let pool = test_pool();
        let low: Vec<Tuple> = (0..100u64).map(|i| ((i, 0), 0, 1i64)).collect();
        let high: Vec<Tuple> = (1000..1100u64).map(|i| ((i, 0), 0, 1i64)).collect();

        let mut in1 = VecDeque::from([force_spill(
            ColumnChunk::from_column(build_column(&low)),
            &pool,
        )]);
        let mut in2 = VecDeque::from([force_spill(
            ColumnChunk::from_column(build_column(&high)),
            &pool,
        )]);
        let mut out = VecDeque::new();
        TestChunk::merge(&mut in1, &mut in2, &mut out);

        // `low` is fully consumed. `high` was never touched and must come
        // back spilled.
        assert!(in1.is_empty());
        assert_eq!(in2.len(), 1);
        assert!(in2[0].is_spilled(), "untouched survivor must stay spilled");
        let mut all = collect_chunks(out);
        Extend::extend(&mut all, collect_chunks(in2.drain(..)));
        let mut expected = low;
        Extend::extend(&mut expected, high);
        assert_eq!(all, expected);
    }

    /// Merge output is one generation past its deepest input, a survivor
    /// rewritten from its remainder keeps its own depth, and a chunk passed
    /// through the disjoint fast path ages by its survival.
    #[mz_ore::test]
    fn merge_derives_generational_depth() {
        let low: Vec<Tuple> = (0..100u64).map(|i| ((i, 0), 0, 1i64)).collect();
        let high: Vec<Tuple> = (50..150u64).map(|i| ((i, 0), 0, 1i64)).collect();
        let mut in1 = VecDeque::from([ColumnChunk::from_column(build_column(&low))]);
        let mut in2 = VecDeque::from([ColumnChunk::from_column(build_column(&high))]);
        assert_eq!(in1[0].depth(), 0, "fresh chunks start at depth 0");
        let mut out = VecDeque::new();
        TestChunk::merge(&mut in1, &mut in2, &mut out);
        assert!(!out.is_empty());
        for chunk in &out {
            assert_eq!(chunk.depth(), 1, "merge output is one past its inputs");
        }
        // The merge runs through the shared horizon, so `high` survives with
        // its unmerged remainder at its original depth.
        assert!(in1.is_empty());
        assert_eq!(in2.len(), 1);
        assert_eq!(in2[0].depth(), 0, "rewritten survivor keeps its depth");

        // A disjoint merge moves the lower front to the output with its data
        // unchanged, one generation older for having outlived the merge.
        let mut in1 = VecDeque::from([ColumnChunk::Resident(Rc::new(build_column(&low)), 3)]);
        let far: Vec<Tuple> = (1000..1100u64).map(|i| ((i, 0), 0, 1i64)).collect();
        let mut in2 = VecDeque::from([ColumnChunk::from_column(build_column(&far))]);
        let mut out = VecDeque::new();
        TestChunk::merge(&mut in1, &mut in2, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].depth(), 4, "pass-through ages a generation");
        assert_eq!(collect_chunks(out), low);
    }

    /// Advance output and carry keep the deepest input depth, since
    /// compaction rewrites within a generation.
    #[mz_ore::test]
    fn advance_preserves_depth() {
        let data: Vec<Tuple> = (0..100u64).map(|i| ((i, 0), 1, 1i64)).collect();
        let mut input = VecDeque::from([
            ColumnChunk::Resident(Rc::new(build_column(&data[..50])), 2),
            ColumnChunk::Resident(Rc::new(build_column(&data[50..])), 1),
        ]);
        let frontier = Antichain::from_elem(5u64);
        let mut out = VecDeque::new();
        TestChunk::advance(&mut input, frontier.borrow(), false, &mut out);
        for chunk in out.iter().chain(input.iter()) {
            assert_eq!(chunk.depth(), 2);
        }
        TestChunk::advance(&mut input, frontier.borrow(), true, &mut out);
        assert!(input.is_empty());
        assert!(!out.is_empty());
        for chunk in &out {
            assert_eq!(chunk.depth(), 2);
        }
    }

    /// Settle commits at the deepest depth among coalesced chunks, and a
    /// commit large enough to spill carries the depth into its spilled
    /// metadata (and thus into the pool hints).
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn settle_commits_at_accumulated_depth() {
        set_spill_override(Some(test_pool()));
        let big: Vec<Tuple> = (0..100_000u64).map(|i| ((i, 0), 0, 1i64)).collect();
        let mut input = VecDeque::from([
            ColumnChunk::Resident(Rc::new(build_column(&big)), 1),
            ColumnChunk::Resident(Rc::new(build_column(&[((0, 0), 0, 1)])), 0),
            ColumnChunk::Resident(Rc::new(build_column(&[((1, 0), 0, 1)])), 2),
        ]);
        let mut out = VecDeque::new();
        TestChunk::settle(&mut input, true, &mut out);
        assert!(input.is_empty());
        assert_eq!(out.len(), 2);
        assert!(out[0].is_spilled(), "large commit must spill");
        assert_eq!(out[0].depth(), 1, "sole commit keeps its depth");
        assert!(!out[1].is_spilled(), "small commit stays resident");
        assert_eq!(out[1].depth(), 2, "coalesced commit takes the max depth");
        set_spill_override(None);
    }

    /// The settle carry commits at a monotone size threshold rather than the
    /// periodic ship window, so mid-window chunk sizes cannot make it grow
    /// past the target unbounded.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn settle_carry_commits_at_target() {
        // ~1.5 MiB per chunk (a row serializes to 32 bytes): under
        // `at_commit_size`, so the carry has to coalesce, and a coalesced
        // pair lands in the dead zone of the periodic window check.
        let chunk_rows = u64::cast_from(1_500_000usize / 32);
        let mut input: VecDeque<TestChunk> = (0..4u64)
            .map(|c| {
                let data: Vec<Tuple> = (0..chunk_rows)
                    .map(|i| ((c * chunk_rows + i, 0), 0, 1i64))
                    .collect();
                ColumnChunk::from_column(build_column(&data))
            })
            .collect();
        let mut out = VecDeque::new();
        TestChunk::settle(&mut input, true, &mut out);
        // Catches the fixture drifting above `at_commit_size`, where settle
        // commits each chunk as-is and the size cap below holds vacuously.
        assert!(out.len() < 4, "nothing coalesced");
        for chunk in &out {
            let col = chunk.clone().into_column();
            assert!(
                col.length_in_bytes() < 2 * COMMIT_BYTES,
                "settled chunk of {} bytes exceeds twice the commit target",
                col.length_in_bytes(),
            );
        }
        assert_eq!(
            collect_chunks(out).len(),
            usize::try_from(4 * chunk_rows).unwrap(),
        );
    }

    #[mz_ore::test]
    fn small_chunks_stay_resident() {
        set_spill_override(Some(test_pool()));
        let committed = TestChunk::commit(build_column(&[((1, 1), 0, 1)]), 0);
        assert!(!committed.is_spilled());
        set_spill_override(None);
    }

    /// The smallest column whose serialized size reaches `SPILL_MIN_BYTES`.
    /// One record less sits under the spill floor.
    fn column_at_spill_floor() -> (Column<Tuple>, u64) {
        let mut col: Column<Tuple> = Column::default();
        let mut n = 0u64;
        while col.length_in_bytes() < SPILL_MIN_BYTES {
            col.push_into(((n, n), 0, 1));
            n += 1;
        }
        (col, n)
    }

    /// Bodies straddling the spill floor: one record under stays resident,
    /// at the floor spills.
    #[mz_ore::test]
    fn spill_floor_boundary() {
        set_spill_override(Some(test_pool()));
        let (col, n) = column_at_spill_floor();
        let mut under: Column<Tuple> = Column::default();
        for m in 0..n - 1 {
            under.push_into(((m, m), 0, 1));
        }
        assert!(under.length_in_bytes() < SPILL_MIN_BYTES);
        assert!(!TestChunk::commit(under, 0).is_spilled());
        assert!(TestChunk::commit(col, 0).is_spilled());
        set_spill_override(None);
    }

    /// The compression depth floor picks the codec, not whether a body
    /// spills: shallow generations store at identity, the floor and deeper
    /// at lz4, and every depth spills and round-trips.
    #[mz_ore::test]
    fn spill_codec_depth_floor() {
        set_spill_override(Some(test_pool()));
        set_compress_min_depth_override(Some(2));
        // Codec identity via Debug: ZST statics and dyn vtables make
        // pointer comparison unreliable. The flag must agree with the codec,
        // since it is what decides whether a body wants migrating.
        let codec_name = |depth: u8| {
            let (codec, compressed) = codec_for_depth(depth);
            let name = format!("{:?}", codec);
            assert_eq!(compressed, name == "Lz4Codec", "flag tracks the codec");
            name
        };
        assert_eq!(codec_name(0), "IdentityCodec");
        assert_eq!(codec_name(1), "IdentityCodec");
        assert_eq!(codec_name(2), "Lz4Codec");
        assert_eq!(codec_name(u8::MAX), "Lz4Codec");

        let data: Vec<Tuple> = (0..20_000u64).map(|i| ((i, 0), 0, 1i64)).collect();
        let data = consolidate(data);
        let column = build_column(&data);
        for depth in [0u8, 1, 2, 3] {
            let chunk = TestChunk::commit(column.clone(), depth);
            assert!(chunk.is_spilled(), "depth {depth} must spill");
            assert_eq!(collect_column(&chunk.into_column()), data);
        }
        set_spill_override(None);
        set_compress_min_depth_override(None);

        // The default floor stores only fresh (depth 0) bodies at identity.
        set_compress_min_depth_override(Some(DEFAULT_COMPRESS_MIN_DEPTH));
        assert_eq!(codec_name(0), "IdentityCodec");
        assert_eq!(codec_name(1), "Lz4Codec");
        set_compress_min_depth_override(None);
    }

    /// A chunk a merge carries forward untouched ages a generation, and a
    /// spilled body crossing the compression floor by doing so is re-spilled
    /// under the compressing codec. Key-disjoint input takes that path on
    /// every merge, so without the crossing its backlog would stay
    /// identity-coded for as long as it lived.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn merge_survivor_crosses_compression_floor() {
        set_spill_override(Some(test_pool()));
        set_compress_min_depth_override(Some(1));

        let low = consolidate((0..20_000u64).map(|i| ((i, 0), 0, 1i64)).collect());
        let far = consolidate((100_000..120_000u64).map(|i| ((i, 0), 0, 1i64)).collect());
        let fresh_far = || VecDeque::from([TestChunk::commit(build_column(&far), 0)]);

        // Fresh spilled chunks sit below the floor, so both store identity
        // coded, and their data ranges are disjoint.
        let mut in1 = VecDeque::from([TestChunk::commit(build_column(&low), 0)]);
        let mut in2 = fresh_far();
        assert!(in1[0].is_spilled() && in2[0].is_spilled());
        assert!(
            !body_compressed(&in1[0]),
            "a fresh body below the floor is identity coded"
        );

        let mut out = VecDeque::new();
        TestChunk::merge(&mut in1, &mut in2, &mut out);
        assert_eq!(out.len(), 1);
        let survived = out.pop_front().expect("the lower front passes through");
        assert_eq!(survived.depth(), 1, "survival ages across the floor");
        assert!(
            survived.is_spilled(),
            "the crossing re-spills, it does not evict"
        );
        assert!(
            body_compressed(&survived),
            "the survivor is re-spilled under the compressing codec"
        );

        // Past the floor the next survival is a metadata-only bump: the body
        // is already compressed and stays where it is.
        let mut in1 = VecDeque::from([survived]);
        let mut in2 = fresh_far();
        let mut out = VecDeque::new();
        TestChunk::merge(&mut in1, &mut in2, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].depth(), 2, "an aged survivor keeps aging");
        assert!(out[0].is_spilled());
        assert_eq!(
            collect_chunks(out),
            low,
            "the body reads back intact across both survivals"
        );

        set_spill_override(None);
        set_compress_min_depth_override(None);
    }

    /// Aging does not depend on holding the only reference to a body. The
    /// trace's compaction merger feeds `merge` clones of a source batch's
    /// chunks and keeps the batch alive throughout, so a shared body must
    /// still age. It must not be re-spilled: the other holder goes on
    /// storing the original whatever this reference does, and the merger
    /// rewrites its clone immediately.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn merge_survivor_ages_while_shared() {
        set_spill_override(Some(test_pool()));
        set_compress_min_depth_override(Some(1));

        let low = consolidate((0..20_000u64).map(|i| ((i, 0), 0, 1i64)).collect());
        let far = consolidate((100_000..120_000u64).map(|i| ((i, 0), 0, 1i64)).collect());

        // The source batch's chunk, held for the whole merge as the spine
        // holds it.
        let source = TestChunk::commit(build_column(&low), 0);
        let ColumnChunk::Spilled(source_body, 0) = &source else {
            panic!("a fresh commit above the spill floor is spilled at depth 0");
        };
        let source_body = Rc::clone(source_body);

        let mut in1 = VecDeque::from([source.clone()]);
        let mut in2 = VecDeque::from([TestChunk::commit(build_column(&far), 0)]);
        let mut out = VecDeque::new();
        TestChunk::merge(&mut in1, &mut in2, &mut out);

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].depth(), 1, "a shared body ages all the same");
        let ColumnChunk::Spilled(survived_body, _) = &out[0] else {
            panic!("the survivor stays spilled");
        };
        assert!(
            Rc::ptr_eq(&source_body, survived_body),
            "a shared body is aged in place, not re-spilled"
        );
        assert_eq!(source.depth(), 0, "the other holder is left as it was");

        // Past the floor, where no re-spill is in question, a shared body
        // goes on aging rather than pinning at the crossing depth.
        let mut in1 = VecDeque::from([out.pop_front().expect("survivor observed above")]);
        let mut in2 = VecDeque::from([TestChunk::commit(build_column(&far), 0)]);
        let mut out = VecDeque::new();
        TestChunk::merge(&mut in1, &mut in2, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].depth(), 2, "aging past the floor is not pinned");
        assert_eq!(collect_chunks(out), low);

        set_spill_override(None);
        set_compress_min_depth_override(None);
    }

    /// A migration that cannot happen when a body first qualifies is retried
    /// at the next survival, never consumed. Each case leaves an
    /// identity-coded body at or past the floor, which would be stranded
    /// uncompressed for the rest of its life if the test were a depth
    /// transition rather than the body's stored codec.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn survive_merge_retries_missed_migrations() {
        let low = consolidate((0..20_000u64).map(|i| ((i, 0), 0, 1i64)).collect());
        let far = consolidate((100_000..120_000u64).map(|i| ((i, 0), 0, 1i64)).collect());

        // Age a chunk one generation through a disjoint merge, which passes
        // the lower front through `survive_merge`.
        let survive = |chunk: TestChunk| {
            let mut in1 = VecDeque::from([chunk]);
            let mut in2 = VecDeque::from([TestChunk::commit(build_column(&far), 0)]);
            let mut out = VecDeque::new();
            TestChunk::merge(&mut in1, &mut in2, &mut out);
            out.pop_front().expect("the lower front passes through")
        };

        // No pool installed when the body qualifies: spilling can be toggled
        // off at runtime while existing handles stay valid.
        set_spill_override(Some(test_pool()));
        set_compress_min_depth_override(Some(1));
        let chunk = TestChunk::commit(build_column(&low), 0);
        assert!(!body_compressed(&chunk));
        set_spill_override(None);
        let chunk = survive(chunk);
        assert_eq!(chunk.depth(), 1, "aging does not need a pool");
        assert!(!body_compressed(&chunk), "no pool, no migration");
        set_spill_override(Some(test_pool()));
        let chunk = survive(chunk);
        assert!(
            body_compressed(&chunk),
            "the migration retries once a pool is back"
        );

        // Shared when the body qualifies: the compaction merger holds the
        // source batch while merging clones of its chunks.
        let chunk = TestChunk::commit(build_column(&low), 0);
        let held = chunk.clone();
        let chunk = survive(chunk);
        assert!(!body_compressed(&chunk), "shared, so not migrated");
        drop(held);
        let chunk = survive(chunk);
        assert!(
            body_compressed(&chunk),
            "the migration retries once the body is unshared"
        );

        // The floor lowered long after the body spilled, which is what an
        // operator reaches for under pool pressure. Nothing here is a
        // transition: the body is already several generations past the new
        // floor when it moves.
        set_compress_min_depth_override(Some(8));
        let chunk = TestChunk::commit(build_column(&low), 3);
        assert!(!body_compressed(&chunk));
        set_compress_min_depth_override(Some(1));
        let chunk = survive(chunk);
        assert_eq!(chunk.depth(), 4);
        assert!(
            body_compressed(&chunk),
            "lowering the floor migrates bodies already past it"
        );

        set_spill_override(None);
        set_compress_min_depth_override(None);
    }

    /// The compute and storage spill gates compose as an OR: either gate
    /// routes commits to the installed pool, and each setter writes only its
    /// own gate.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn spill_gates_compose_as_or() {
        let installed =
            crate::pool_config::apply_pool_config(crate::pool_config::PoolPagerConfig {
                budget_bytes: 32 << 20,
                spill_threads: 1,
                eager_backing: false,
                rss_target_bytes: 16 << 20,
            });
        assert!(installed, "pool reservation failed");
        // A body at the spill floor, so the gates alone decide.
        let (col, _) = column_at_spill_floor();
        let commit = |col: &Column<Tuple>| TestChunk::commit(col.clone(), 0).is_spilled();

        assert!(!commit(&col), "both gates off");
        set_storage_spill_enabled(true);
        assert!(commit(&col), "the storage gate alone spills");
        set_compute_spill_enabled(false);
        assert!(
            commit(&col),
            "the compute setter must not clobber the storage gate"
        );
        set_compute_spill_enabled(true);
        set_storage_spill_enabled(false);
        assert!(commit(&col), "the compute gate alone spills");
        set_compute_spill_enabled(false);
        assert!(!commit(&col), "both gates off again");
        set_compress_min_depth_override(None);
    }

    /// Re-spilling an already-serialized body exercises the `Column::Align`
    /// branch of `spill_column` and round-trips byte-identically.
    #[mz_ore::test]
    fn spill_align_round_trip() {
        let pool = test_pool();
        let data: Vec<Tuple> = (0..64u64).map(|k| ((k, k), 0, 1)).collect();
        let spilled = force_spill(ColumnChunk::from_column(build_column(&data)), &pool);
        let column = spilled.into_column();
        let Column::Align(words) = &column else {
            panic!("a spilled body reads back as Column::Align");
        };
        let words = words.clone();
        let respilled = force_spill(ColumnChunk::from_column(column), &pool);
        let reread = respilled.into_column();
        let Column::Align(words2) = &reread else {
            panic!("a spilled body reads back as Column::Align");
        };
        assert_eq!(&words, words2, "byte-identical round trip");
        assert_eq!(collect_column(&reread), data);
    }

    #[mz_ore::test]
    fn merge_depth_saturates() {
        let a = ColumnChunk::Resident(
            Rc::new(build_column(&[((1, 0), 0, 1), ((3, 0), 0, 1)])),
            u8::MAX,
        );
        let b = ColumnChunk::Resident(
            Rc::new(build_column(&[((2, 0), 0, 1), ((4, 0), 0, 1)])),
            u8::MAX,
        );
        let mut in1 = VecDeque::from([a]);
        let mut in2 = VecDeque::from([b]);
        let mut out = VecDeque::new();
        TestChunk::merge(&mut in1, &mut in2, &mut out);
        for chunk in out.iter().chain(in1.iter()).chain(in2.iter()) {
            assert_eq!(chunk.depth(), u8::MAX, "depth saturates");
        }
    }

    #[mz_ore::test]
    fn into_column_copies_shared_resident() {
        let data: Vec<Tuple> = vec![((1, 1), 0, 1), ((2, 2), 0, 1)];
        let a = ColumnChunk::from_column(build_column(&data));
        let b = a.clone();
        assert_eq!(collect_column(&a.into_column()), data);
        assert_eq!(collect_column(&b.into_column()), data);
    }
}
