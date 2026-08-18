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

use columnar::AsBytes;
use columnar::bytes::indexed;

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
    /// Relocated from received bytes that could not be borrowed in place,
    /// because of misalignment or because a clone was needed.
    Decode,
}

impl Origin {
    /// Every origin, in metric-label order.
    pub const ALL: [Origin; 6] = [
        Origin::Ship,
        Origin::Consolidate,
        Origin::Correction,
        Origin::Pager,
        Origin::Fetch,
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
    words: Vec<u64>,
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

impl AlignBuffer {
    /// Serializes `item`'s columnar byte slices into a buffer sized to fit
    /// them exactly.
    ///
    /// `indexed::encode` appends through `push`/`extend_from_slice`, so an
    /// exact `with_capacity` means no word is written twice and no word is
    /// zero-initialized before its real value lands.
    pub fn encode<'a, A>(origin: Origin, item: &A) -> Self
    where
        A: AsBytes<'a>,
    {
        let mut words = Vec::with_capacity(indexed::length_in_words(item));
        indexed::encode(&mut words, item);
        Self::from_words(origin, words)
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
            words,
            origin,
            charge,
        }
    }

    /// Surrenders the allocation to a caller that will own it from here on.
    /// The buffer's tracked life ends at this call, not when the returned
    /// `Vec` is dropped.
    pub fn into_words(mut self) -> Vec<u64> {
        self.release();
        std::mem::take(&mut self.words)
    }

    /// The serialized body.
    ///
    /// [`Deref`] covers call sites whose parameter type is a slice; this
    /// exists for the ones that are generic over it, where deref coercion has
    /// nothing to coerce toward.
    #[inline]
    pub fn as_words(&self) -> &[u64] {
        &self.words
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
        &self.words
    }
}

impl Clone for AlignBuffer {
    /// A clone is a separate allocation with its own life, so it is minted and
    /// tracked in its own right. It inherits the origin, because a body cloned
    /// to fan out to a second consumer is still on the edge that produced it.
    fn clone(&self) -> Self {
        Self::from_words(self.origin, self.words.clone())
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
            .field("words", &self.words.len())
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
