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
//! retained by a sink or a merge chain, bodies relocated off the network, and
//! bodies copied out of a backing store. Each buffer records the [`Origin`]
//! that minted it, so those groups can be read apart.
//!
//! A buffer is immutable once built. Every constructor sizes it before the
//! value exists, which is what lets the tracker charge a byte count that stays
//! correct for the buffer's whole life.
//!
//! See [`metrics`] for what is recorded and how to turn recording on.

use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};

use columnar::AsBytes;
use columnar::bytes::indexed;
use mz_ore::pool::{ChunkHandle, ChunkHints, Pool};

use crate::columnar::chunk::LZ4_CODEC;

pub mod metrics;

/// What minted a buffer. Recorded per buffer so the metrics separate bodies in
/// flight on a dataflow edge from bodies that are retained on purpose or that a
/// read produced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    /// Shipped by [`ColumnBuilder`](crate::columnar::builder::ColumnBuilder):
    /// a body on a dataflow edge.
    Ship,
    /// Shipped by
    /// [`ConsolidatingColumnBuilder`](crate::columnar::consolidate::ConsolidatingColumnBuilder):
    /// also a body on a dataflow edge.
    Consolidate,
    /// Minted by the container builder behind the materialized-view sink's
    /// correction buffer, which holds its chunks across timestamps. Split out
    /// from [`Origin::Ship`] because a retained body's life says nothing about
    /// a body in flight on an edge.
    Correction,
    /// Serialized to a fitting size by the column pager, which does this to
    /// every typed body it is handed, before parking or paging it. Covers both
    /// arrangement merge chains and the sink correction chains, which route
    /// through the pager whatever the batcher gate says. A correction chunk the
    /// builder minted itself is [`Origin::Correction`]; one the pager
    /// serialized lands here.
    Pager,
    /// Copied out of a backing store to serve a read.
    Fetch,
    /// Materialized out of the buffer pool because a consumer borrowed a
    /// paged edge body. Distinct from [`Origin::Fetch`], which is a trace
    /// chunk read: an `Unpage` mint is an edge body whose paging round trip
    /// completed, so the count says how often paging actually cost a copy
    /// rather than how many bodies were paged.
    Unpage,
    /// Relocated from received bytes that could not be borrowed in place,
    /// because of misalignment or because a clone was needed.
    Decode,
}

impl Origin {
    /// Every origin, in metric-label order.
    pub const ALL: [Origin; 7] = [
        Origin::Ship,
        Origin::Consolidate,
        Origin::Correction,
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
            Origin::Correction => "correction",
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
            Origin::Correction => 2,
            Origin::Pager => 3,
            Origin::Fetch => 4,
            Origin::Unpage => 5,
            Origin::Decode => 6,
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
/// for as long as nobody looks at it. The pool hands out no references, so the
/// first look copies the body out and the buffer stays heap-backed from then
/// on: paging covers the interval between minting a body and the consumer
/// reaching it, which measurement puts at seconds under memory pressure and is
/// where essentially all of an edge body's life is spent.
enum Body {
    /// Words on the heap.
    Heap(Vec<u64>),
    /// Words in the buffer pool, copied out and freed on first access.
    Paged {
        /// The pool chunk, present until the body is materialized.
        ///
        /// A `Mutex` rather than a `RefCell` because a `Column` crosses
        /// worker threads, and taking the handle out is what lets
        /// materialization free the chunk: holding both the slot and the
        /// heap copy for the rest of the body's life would make paging cost
        /// memory rather than save it.
        spilled: Mutex<Option<ChunkHandle>>,
        resident: OnceLock<Vec<u64>>,
    },
}

/// Whether newly minted edge bodies go to the buffer pool. Read once per mint.
static EDGE_PAGING: AtomicBool = AtomicBool::new(false);

/// Bodies below this stay on the heap: the pool's smallest size class is
/// 64 KiB, so paging under it trades no meaningful memory for slot waste.
/// Edge bodies cluster at ~1.8 MiB, so this only excludes the ragged tail a
/// builder flushes at the end of a run.
const PAGE_MIN_BYTES: usize = 64 << 10;

/// Turns edge paging on or off for this process. Takes effect for bodies
/// minted after the call; bodies already paged stay paged.
pub fn set_edge_paging_enabled(enabled: bool) {
    EDGE_PAGING.store(enabled, Ordering::Relaxed);
}

/// Whether edge paging is on.
pub fn edge_paging_enabled() -> bool {
    EDGE_PAGING.load(Ordering::Relaxed)
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
    fn page<'a, A>(origin: Origin, records: usize, words: usize, item: &A) -> Option<Self>
    where
        A: AsBytes<'a>,
    {
        if words * 8 < PAGE_MIN_BYTES {
            return None;
        }
        let pool = edge_pool()?;
        let handle = pool.insert_with(words, ChunkHints::default(), &LZ4_CODEC, |dst| {
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
        let charge = metrics::record_mint(origin, words);
        Some(AlignBuffer {
            body: Body::Paged {
                spilled: Mutex::new(Some(handle)),
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
    pub fn into_words(mut self) -> Vec<u64> {
        self.release();
        match std::mem::replace(&mut self.body, Body::Heap(Vec::new())) {
            Body::Heap(words) => words,
            Body::Paged { spilled, resident } => resident
                .into_inner()
                .unwrap_or_else(|| Self::take_spilled(&spilled)),
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

    /// Copies a paged body out of the pool and frees the chunk, at most once.
    fn take_spilled(spilled: &Mutex<Option<ChunkHandle>>) -> Vec<u64> {
        let handle = spilled
            .lock()
            .expect("spill mutex poisoned")
            .take()
            .expect("the handle is present until the body is materialized");
        let mut words = Vec::new();
        // `take` copies out and frees, where `read_into` would leave the chunk
        // allocated and hold the body twice for the rest of its life.
        handle.take(&mut words);
        metrics::record_unpage(words.capacity());
        words
    }

    /// The words, copying a paged body out of the pool on first call.
    fn resident_words(&self) -> &[u64] {
        match &self.body {
            Body::Heap(words) => words,
            Body::Paged { spilled, resident } => {
                resident.get_or_init(|| Self::take_spilled(spilled))
            }
        }
    }

    /// The serialized body.
    ///
    /// [`Deref`] covers call sites whose parameter type is a slice; this
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
    /// A clone is a separate allocation with its own life, so it is minted and
    /// tracked in its own right. It inherits the origin, because a body cloned
    /// to fan out to a second consumer is still on the edge that produced it.
    fn clone(&self) -> Self {
        match &self.body {
            Body::Heap(words) => {
                let mut clone = Self::from_words(self.origin, words.clone());
                clone.records = self.records;
                clone
            }
            // A paged body materializes to be cloned. `ChunkHandle` is not
            // `Clone`, deliberately: one handle owns one chunk, and sharing
            // would move the pool to refcounted ownership for the sake of a
            // path that barely runs. Cloning an edge body is rare (a fan-out
            // to a second consumer), and the copy is what the pool's
            // no-references contract would charge for the read anyway.
            Body::Paged { .. } => {
                let mut clone = Self::from_words(self.origin, self.resident_words().to_vec());
                clone.records = self.records;
                clone
            }
        }
    }
}

impl Drop for AlignBuffer {
    fn drop(&mut self) {
        self.release();
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
