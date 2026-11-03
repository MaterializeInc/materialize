// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! [`AlignBuffer`]: the owned serialized body behind
//! [`Column::Align`](crate::columnar::Column::Align), and the instrumentation
//! that measures how long such bodies live.
//!
//! Every `Column::Align` payload is one of these. The population is wider than
//! the bodies in flight between operators: it also covers bodies deliberately
//! retained by a merge chain, bodies relocated off the network, and
//! bodies copied out of a backing store. Each buffer records the [`Origin`]
//! that minted it, so those groups can be read apart.
//!
//! A buffer is immutable once built. Every constructor sizes it before the
//! value exists, which is what lets the tracker charge a byte count that stays
//! correct for the buffer's whole life.
//!
//! See [`metrics`] for what is recorded and how to turn recording on.

use std::cell::RefCell;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use columnar::AsBytes;
use columnar::bytes::indexed;
use mz_ore::pool::{ChunkHandle, ChunkHints, ExtentCodec, IDENTITY_CODEC, Pool};

use crate::columnar::chunk::LZ4_CODEC;

pub mod metrics;

/// What minted a buffer. Recorded per buffer so the metrics separate bodies in
/// flight on a dataflow edge from bodies that are retained on purpose or that a
/// read produced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    /// Shipped by [`ColumnBuilder`](crate::columnar::builder::ColumnBuilder),
    /// or re-encoded by a scope crossing (`Enter`, `ResultsIn`): a body on a
    /// dataflow edge.
    Ship,
    /// Shipped by
    /// [`ConsolidatingColumnBuilder`](crate::columnar::consolidate::ConsolidatingColumnBuilder):
    /// also a body on a dataflow edge.
    Consolidate,
    /// Serialized to a fitting size by the column pager, which does this to
    /// every typed body it is handed, before parking or paging it.
    Pager,
    /// Copied out of a backing store to serve a read.
    Fetch,
    /// A consumer's copy of a paged edge body, made when it first borrowed
    /// the body. Distinct from [`Origin::Fetch`], which is a trace chunk read:
    /// the count says how often paging actually cost a copy, which a body
    /// shared by several consumers does once per consumer.
    Unpage,
    /// Relocated from received bytes that could not be borrowed in place,
    /// because of misalignment or because a clone was needed.
    Decode,
}

impl Origin {
    /// Every origin, in metric-label order.
    pub const ALL: [Origin; 6] = [
        Origin::Ship,
        Origin::Consolidate,
        Origin::Pager,
        Origin::Fetch,
        Origin::Unpage,
        Origin::Decode,
    ];

    /// The `origin` metric label value.
    pub const fn label(self) -> &'static str {
        match self {
            Origin::Ship => "ship",
            Origin::Consolidate => "consolidate",
            Origin::Pager => "pager",
            Origin::Fetch => "fetch",
            Origin::Unpage => "unpage",
            Origin::Decode => "decode",
        }
    }

    /// This origin's index into the per-origin counter array.
    const fn index(self) -> usize {
        match self {
            Origin::Ship => 0,
            Origin::Consolidate => 1,
            Origin::Pager => 2,
            Origin::Fetch => 3,
            Origin::Unpage => 4,
            Origin::Decode => 5,
        }
    }
}

/// An owned, immutable, `u64`-aligned serialized column body: the payload of
/// [`Column::Align`](crate::columnar::Column::Align).
///
/// `u64` alignment is what lets
/// [`Column::borrow`](crate::columnar::Column::borrow) decode the body in
/// place, so the words are held as a `Vec<u64>` rather than a `Vec<u8>`.
pub struct AlignBuffer {
    body: Body,
    /// Serialized length in words. Resident in both states, because
    /// `length_in_bytes` is on the hot path and a paged body must answer it
    /// without a copy-out.
    words: usize,
    /// Record count, when the producer knew it at mint.
    ///
    /// Resident for the same reason as `words`, and load-bearing for paging:
    /// `Column`'s `Accountable::record_count` would otherwise decode the body,
    /// and Timely calls it at both push and pull. A paged body that
    /// materialized there would be back on the heap before it ever sat in a
    /// queue, which is the whole interval paging exists to cover.
    records: Option<usize>,
    origin: Origin,
    /// The mint instant and the bytes charged to the in-flight gauges, present
    /// exactly when tracking was on at construction.
    ///
    /// The charge is stored rather than recomputed at drop because
    /// [`metrics::set_tracking_enabled`] can flip while buffers are in flight.
    /// Crediting back a buffer that was never charged would walk the gauges
    /// away from the truth, and the arithmetic is unsigned.
    charge: Option<metrics::Charge>,
}

/// Where a buffer's words live.
///
/// A paged body is owned by the buffer pool, so it is budgeted and evictable
/// for as long as nobody looks at it. Paging covers the interval between
/// minting a body and the consumer reaching it, which measurement puts at
/// seconds under memory pressure and is where essentially all of an edge
/// body's life is spent.
///
/// The pool hands out no references into its memory, so every consumer that
/// looks copies the body out into its own buffer. Clones share the chunk, which
/// keeps the body in the pool until the last consumer reaches it: a fan-out
/// edge clones the body once per extra consumer at push time, and a shared
/// heap copy would sit outside the pool's budget for as long as the slowest
/// consumer takes.
enum Body {
    /// Words on the heap.
    Heap(Vec<u64>),
    /// Words in the buffer pool, shared by every clone of the body.
    Paged {
        /// The pool chunk, taken and freed by the last holder to copy it out.
        ///
        /// A `Mutex` because a `Column` crosses worker threads, and because
        /// it orders the sharing: clones are made under the lock, so the
        /// holder count read under it cannot grow before a take.
        chunk: Arc<Mutex<Option<ChunkHandle>>>,
        /// This holder's copy, made on its first access.
        resident: OnceLock<Resident>,
    },
}

/// A consumer's copy of a paged body, tracked as an [`Origin::Unpage`] buffer
/// of its own from the copy-out until it is dropped or surrendered.
struct Resident {
    words: Vec<u64>,
    charge: Option<metrics::Charge>,
}

impl Drop for Resident {
    fn drop(&mut self) {
        if let Some(charge) = self.charge.take() {
            metrics::record_drop(Origin::Unpage, charge);
        }
    }
}

/// Whether newly minted edge bodies go to the buffer pool. Read once per mint.
static EDGE_PAGING: AtomicBool = AtomicBool::new(false);

/// Whether newly paged edge bodies are stored under lz4. Read once per mint.
static EDGE_PAGING_LZ4: AtomicBool = AtomicBool::new(false);

/// Bodies below this stay on the heap: the pool's smallest size class is
/// 64 KiB, so paging under it trades no meaningful memory for slot waste.
/// Edge bodies cluster at ~1.8 MiB, so this only excludes the ragged tail a
/// builder flushes at the end of a run.
const PAGE_MIN_BYTES: usize = 64 << 10;

thread_local! {
    /// One retired materialized copy, kept for the next materialization on this thread
    /// to refill.
    ///
    /// Capacity one, and per worker thread rather than per operator. That
    /// distinction is the whole safety argument: a per-builder buffer would be
    /// held by every idle operator on every worker, which is why Timely's own
    /// container recycling is disabled. One body per thread is 2 MiB times the
    /// worker count and does not grow with the dataflow.
    static UNPAGE_STASH: RefCell<Option<Vec<u64>>> = const { RefCell::new(None) };
}

/// Words a stashed buffer may retain, one ship-sized body. Matches
/// `SCRATCH_RETAIN_WORDS` in [`crate::columnar::chunk`], which bounds the
/// equivalent read scratch on the chunk paths.
const STASH_RETAIN_WORDS: usize = 1 << 18;

/// Retires `words` into this thread's slot, if it is worth keeping and the slot
/// is free.
///
/// `try_with` because a buffer can be dropped while the thread's locals are
/// being torn down, where `with` would panic.
fn stash_words(mut words: Vec<u64>) {
    if words.capacity() == 0 || words.capacity() > STASH_RETAIN_WORDS {
        return;
    }
    words.clear();
    let _ = UNPAGE_STASH.try_with(|cell| {
        let mut slot = cell.borrow_mut();
        // Only fill an empty slot, so no `Vec` is dropped while the borrow is
        // held and the slot cannot churn between two live bodies.
        if slot.is_none() {
            *slot = Some(words);
        }
    });
}

/// This thread's retired buffer, or a fresh empty one.
fn take_stashed_words() -> Vec<u64> {
    UNPAGE_STASH
        .try_with(|cell| cell.borrow_mut().take())
        .ok()
        .flatten()
        .unwrap_or_default()
}

/// The capacity of this thread's retired buffer, or `None` when the slot is
/// empty.
///
/// Exists so tests can assert the buffer is retired and then consumed. Pointer
/// equality is not a sound substitute: the allocator may hand back the same
/// address whether or not the slot was used.
#[doc(hidden)]
pub fn stashed_capacity() -> Option<usize> {
    UNPAGE_STASH
        .try_with(|cell| cell.borrow().as_ref().map(Vec::capacity))
        .ok()
        .flatten()
}

/// Turns edge paging on or off for this process. Takes effect for bodies
/// minted after the call. Bodies already paged stay paged.
pub fn set_edge_paging_enabled(enabled: bool) {
    EDGE_PAGING.store(enabled, Ordering::Relaxed);
}

/// Whether edge paging is on.
pub fn edge_paging_enabled() -> bool {
    EDGE_PAGING.load(Ordering::Relaxed)
}

/// Selects the codec newly paged edge bodies are stored under: lz4 when
/// `enabled`, identity otherwise. The codec only does work when the pool
/// evicts a body and when a consumer reads an evicted one. Bodies already
/// paged keep the codec they were inserted with.
pub fn set_edge_paging_lz4(enabled: bool) {
    EDGE_PAGING_LZ4.store(enabled, Ordering::Relaxed);
}

/// The codec newly paged edge bodies are stored under.
fn edge_codec() -> &'static dyn ExtentCodec {
    if EDGE_PAGING_LZ4.load(Ordering::Relaxed) {
        &LZ4_CODEC
    } else {
        &IDENTITY_CODEC
    }
}

/// The pool edge bodies page to, if any. `None` leaves them on the heap, which
/// is the behavior with the gate off or before a config apply has installed and
/// budgeted a pool.
fn edge_pool() -> Option<Pool> {
    if EDGE_PAGING.load(Ordering::Relaxed) {
        crate::pool_config::active_pool()
    } else {
        None
    }
}

impl AlignBuffer {
    /// Serializes `item`'s columnar byte slices into a buffer sized to fit
    /// them exactly.
    ///
    /// `indexed::encode` appends through `push`/`extend_from_slice`, so an
    /// exact `with_capacity` means no word is written twice and no word is
    /// zero-initialized before its real value lands.
    pub fn encode<'a, A>(origin: Origin, records: usize, item: &A) -> Self
    where
        A: AsBytes<'a>,
    {
        let words = indexed::length_in_words(item);
        if let Some(paged) = Self::page(origin, records, words, item) {
            return paged;
        }
        let mut heap = Vec::with_capacity(words);
        indexed::encode(&mut heap, item);
        let mut buffer = Self::from_words(origin, heap);
        buffer.records = Some(records);
        buffer
    }

    /// Serializes `item` straight into a pool slot, or returns `None` when the
    /// body is not worth a slot or no pool is installed.
    ///
    /// This is the zero-staging path: the single copy lands in pool memory, so
    /// a shipped body costs one page population instead of faulting a fresh
    /// heap allocation that dies a few seconds later.
    ///
    /// The destination is unknown here. A body bound for another process is
    /// copied back out at push, when the zero-copy pusher serializes it, so on
    /// that path paging costs an insert and a copy-out and buys no residency.
    fn page<'a, A>(origin: Origin, records: usize, words: usize, item: &A) -> Option<Self>
    where
        A: AsBytes<'a>,
    {
        if words * 8 < PAGE_MIN_BYTES {
            return None;
        }
        let pool = edge_pool()?;
        let handle = pool.insert_with(words, ChunkHints::default(), edge_codec(), |dst| {
            let bytes: &mut [u8] = bytemuck::cast_slice_mut(dst);
            let mut cursor = std::io::Cursor::new(bytes);
            indexed::write(&mut cursor, item).expect("writing to a slice cannot fail");
            // `insert_with` requires the fill to overwrite the whole slot, and
            // the slot was sized from `length_in_words`. A short write would
            // leave the tail unspecified and decode as garbage.
            assert_eq!(
                usize::try_from(cursor.position()).expect("position fits usize"),
                words * 8,
                "serialized body must fill the slot exactly",
            );
        });
        let charge = metrics::record_mint_paged(origin, words);
        Some(AlignBuffer {
            body: Body::Paged {
                chunk: Arc::new(Mutex::new(Some(handle))),
                resident: OnceLock::new(),
            },
            words,
            records: Some(records),
            origin,
            charge,
        })
    }

    /// Builds a buffer by filling a `Vec<u64>` in place, for producers whose
    /// length only the filler knows, such as a copy-out from a backing store.
    pub fn build(origin: Origin, fill: impl FnOnce(&mut Vec<u64>)) -> Self {
        let mut words = Vec::new();
        fill(&mut words);
        Self::from_words(origin, words)
    }

    /// Takes ownership of an already-serialized buffer.
    pub fn from_words(origin: Origin, words: Vec<u64>) -> Self {
        let charge = metrics::record_mint(origin, words.capacity());
        AlignBuffer {
            words: words.len(),
            body: Body::Heap(words),
            records: None,
            origin,
            charge,
        }
    }

    /// Surrenders the allocation to a caller that will own it from here on.
    /// The buffer's tracked life ends at this call, not when the returned
    /// `Vec` is dropped.
    ///
    /// A paged body is copied out of the pool here if this holder has not
    /// already done so, and the returned `Vec` may carry spare capacity from a
    /// recycled buffer.
    pub fn into_words(mut self) -> Vec<u64> {
        self.release();
        match std::mem::replace(&mut self.body, Body::Heap(Vec::new())) {
            Body::Heap(words) => words,
            Body::Paged { chunk, resident } => {
                let mut resident = resident
                    .into_inner()
                    .unwrap_or_else(|| Self::copy_out(&chunk));
                std::mem::take(&mut resident.words)
            }
        }
    }

    /// The record count, when the producer knew it at mint. Serialized bodies
    /// built from raw words (a network relocation, a backing-store read) do
    /// not, and answer `None`.
    #[inline]
    pub fn records(&self) -> Option<usize> {
        self.records
    }

    /// The serialized length in words.
    ///
    /// Inherent so it shadows the [`Deref`] slice's `len`, which would
    /// materialize a paged body to answer.
    #[inline]
    pub fn len(&self) -> usize {
        self.words
    }

    /// Whether the body is empty, without materializing it.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.words == 0
    }

    /// Whether the body still lives in the pool.
    #[inline]
    pub fn is_paged(&self) -> bool {
        matches!(&self.body, Body::Paged { resident, .. } if resident.get().is_none())
    }

    /// Copies a paged body out of the pool into a buffer of this holder's own.
    ///
    /// The last holder takes the chunk, which frees it with the copy. Any
    /// other holder reads it and leaves it in the pool for the rest. A holder
    /// count that drops between the check and the read only means this holder
    /// copied where it could have taken, and the chunk is then freed when the
    /// last `Arc` drops.
    fn copy_out(chunk: &Arc<Mutex<Option<ChunkHandle>>>) -> Resident {
        // Refill this thread's retired buffer rather than faulting a fresh
        // one. The pool's reads clear the destination themselves, so a stashed
        // buffer with spare capacity is filled without allocating at all.
        let mut words = take_stashed_words();
        let mut slot = chunk.lock().expect("chunk mutex poisoned");
        if Arc::strong_count(chunk) == 1 {
            slot.take()
                .expect("only the last holder takes the chunk")
                .take(&mut words);
        } else {
            slot.as_ref()
                .expect("the chunk stays while other holders remain")
                .read_into(&mut words);
        }
        drop(slot);
        let charge = metrics::record_mint(Origin::Unpage, words.capacity());
        Resident { words, charge }
    }

    /// The words, copying a paged body out of the pool on first call.
    fn resident_words(&self) -> &[u64] {
        match &self.body {
            Body::Heap(words) => words,
            Body::Paged { chunk, resident } => {
                &resident.get_or_init(|| Self::copy_out(chunk)).words
            }
        }
    }

    /// The serialized body.
    ///
    /// [`Deref`] covers call sites whose parameter type is a slice. This
    /// exists for the ones that are generic over it, where deref coercion has
    /// nothing to coerce toward.
    #[inline]
    pub fn as_words(&self) -> &[u64] {
        self.resident_words()
    }

    /// What minted this buffer.
    pub fn origin(&self) -> Origin {
        self.origin
    }

    /// Records the end of the tracked life, at most once.
    fn release(&mut self) {
        if let Some(charge) = self.charge.take() {
            metrics::record_drop(self.origin, charge);
        }
    }
}

impl Deref for AlignBuffer {
    type Target = [u64];
    #[inline]
    fn deref(&self) -> &[u64] {
        self.resident_words()
    }
}

impl Clone for AlignBuffer {
    /// A clone inherits the origin, because a body cloned to fan out to a
    /// second consumer is still on the edge that produced it.
    ///
    /// A heap clone is a separate allocation, minted and tracked in its own
    /// right. A paged clone shares the chunk and is minted with no heap bytes,
    /// since it holds none until it copies the body out. Once the last holder
    /// has taken the chunk there is nothing left to share, and the clone copies
    /// this holder's words instead.
    fn clone(&self) -> Self {
        let mut clone = match &self.body {
            Body::Heap(words) => Self::from_words(self.origin, words.clone()),
            Body::Paged { chunk, .. } => {
                let slot = chunk.lock().expect("chunk mutex poisoned");
                if slot.is_some() {
                    // Cloned under the lock, so a concurrent `copy_out` sees
                    // the new holder before it decides whether to take.
                    let chunk = Arc::clone(chunk);
                    drop(slot);
                    AlignBuffer {
                        body: Body::Paged {
                            chunk,
                            resident: OnceLock::new(),
                        },
                        words: self.words,
                        records: None,
                        origin: self.origin,
                        charge: metrics::record_mint_paged(self.origin, self.words),
                    }
                } else {
                    drop(slot);
                    Self::from_words(self.origin, self.resident_words().to_vec())
                }
            }
        };
        clone.records = self.records;
        clone
    }
}

impl Drop for AlignBuffer {
    fn drop(&mut self) {
        self.release();
        // Retire this holder's copy for the next copy-out to refill. Only a
        // paged body's copy is retired: a heap body's buffer would park in the
        // slot with nothing to consume it whenever paging is off. Dropping the
        // last `chunk` here is what frees a chunk no holder took.
        if let Body::Paged { resident, .. } =
            std::mem::replace(&mut self.body, Body::Heap(Vec::new()))
            && let Some(mut resident) = resident.into_inner()
        {
            stash_words(std::mem::take(&mut resident.words));
        }
    }
}

impl std::fmt::Debug for AlignBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlignBuffer")
            .field("origin", &self.origin)
            .field("words", &self.words)
            .field("paged", &self.is_paged())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Origin indices are dense and distinct, which the counter array indexes
    /// by without a bounds story of its own.
    #[mz_ore::test]
    fn origin_indices_are_dense() {
        for (expected, origin) in Origin::ALL.into_iter().enumerate() {
            assert_eq!(origin.index(), expected, "{origin:?}");
        }
    }

    /// Labels are distinct, so the metric series do not collide.
    #[mz_ore::test]
    fn origin_labels_are_distinct() {
        let mut labels: Vec<_> = Origin::ALL.iter().map(|o| o.label()).collect();
        labels.sort_unstable();
        let count = labels.len();
        labels.dedup();
        assert_eq!(labels.len(), count);
    }

    /// `into_words` hands out the same bytes the buffer held.
    #[mz_ore::test]
    fn into_words_round_trips() {
        let buf = AlignBuffer::from_words(Origin::Ship, vec![1, 2, 3]);
        assert_eq!(&*buf, &[1, 2, 3]);
        assert_eq!(buf.into_words(), vec![1, 2, 3]);
    }

    /// A clone is independent of its source and keeps the origin.
    #[mz_ore::test]
    fn clone_inherits_origin() {
        let buf = AlignBuffer::from_words(Origin::Decode, vec![7; 4]);
        let clone = buf.clone();
        assert_eq!(clone.origin(), Origin::Decode);
        assert_eq!(&*clone, &*buf);
    }
}
