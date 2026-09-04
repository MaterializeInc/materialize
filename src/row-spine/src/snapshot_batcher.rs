// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A batcher that defers sorting while every held update shares one time.
//!
//! The merge batcher sorts each incoming chunk and folds it into a geometric ladder of sorted
//! chains, copying every row once per merge level and comparing full rows at every step. While
//! a dataflow hydrates, every update carries the snapshot time, so nothing can cancel across
//! times and the ladder buys nothing: the chain that `seal` eventually emits is every held update
//! in `(data, time)` order, consolidated.
//!
//! [`SnapshotBatcher`] holds incoming chunks untouched while they all share one time. At `seal`
//! it sorts a compact index of `(key prefix, chunk, position)` entries, comparing full rows only
//! on equal prefixes, and emits the sorted, consolidated chain in one copy pass. The first chunk
//! carrying a second time hands everything held to a [`MergeBatcher`], and the batcher stays on
//! that path from then on, so steady-state behaviour is the merge batcher's.
//!
//! [`UnsortedChunker`] pairs with it: chunks arrive in arrival order, since sorting them ahead
//! of a batcher that sorts everything at `seal` is wasted, and the batcher sorts and
//! consolidates each chunk itself before handing it to the merge batcher on the fallback path.

use std::cmp::Ordering;
use std::collections::VecDeque;

use columnar::{Columnar, Index, Len};
use columnation::Columnation;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::Logger;
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use differential_dataflow::trace::{Batcher, Description};
use mz_repr::Row;
use mz_timely_util::columnar::Column;
use mz_timely_util::columnation::{ColInternalMerger, ColumnationStack};
use timely::container::{ContainerBuilder, PushInto};
use timely::progress::Timestamp;
use timely::progress::frontier::{Antichain, AntichainRef};

/// Data whose order has a cheap `u64` approximation: for `a < b`, `a.sort_prefix() <=
/// b.sort_prefix()`. Equal prefixes decide nothing.
///
/// Tuples take the prefix of their first component, since tuple order is lexicographic.
pub trait SortPrefix {
    /// See the trait documentation.
    fn sort_prefix(&self) -> u64;
}

impl SortPrefix for Row {
    #[inline]
    fn sort_prefix(&self) -> u64 {
        mz_repr::RowRef::sort_prefix(self)
    }
}

impl<A: SortPrefix, B> SortPrefix for (A, B) {
    #[inline]
    fn sort_prefix(&self) -> u64 {
        self.0.sort_prefix()
    }
}

type Chunk<D, T, R> = ColumnationStack<(D, T, R)>;

/// See the module documentation.
pub struct SnapshotBatcher<D, T, R>
where
    D: SortPrefix + Ord + Columnation + Clone + 'static,
    T: Timestamp + Lattice + Columnation + Clone + 'static,
    R: Default + Semigroup + Columnation + Clone + 'static,
{
    /// Chunks held while every update received since the last emission is at `time`.
    ///
    /// Chunks arrive sorted and consolidated from the chunker, which the general path relies on
    /// when they are handed over. The snapshot path does not depend on it.
    pending: Vec<Chunk<D, T, R>>,
    pending_len: usize,
    /// The one time all pending updates carry, if any are pending.
    time: Option<T>,
    /// The general batcher, engaged for good once a second time appears.
    inner: MergeBatcher<ColInternalMerger<D, T, R>>,
    general: bool,
    /// The upper of the previous `seal`, and the lower of the next description.
    lower: Antichain<T>,
    /// Lower envelope of held times, as of the last `seal`.
    frontier: Antichain<T>,
}

impl<D, T, R> SnapshotBatcher<D, T, R>
where
    D: SortPrefix + Ord + Columnation + Clone + 'static,
    T: Timestamp + Lattice + Columnation + Clone + 'static,
    R: Default + Semigroup + Columnation + Clone + 'static,
{
    /// Records per output chunk, matching the merge batcher's 64 KiB chunks.
    fn chunk_capacity() -> usize {
        const BUFFER_SIZE_BYTES: usize = 64 << 10;
        let size = std::mem::size_of::<(D, T, R)>();
        if size == 0 {
            BUFFER_SIZE_BYTES
        } else if size <= BUFFER_SIZE_BYTES {
            BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }

    /// Sort and consolidate one chunk, for handing to the general path, which requires it.
    fn sort_chunk(chunk: Chunk<D, T, R>) -> Chunk<D, T, R> {
        let mut index: Vec<usize> = (0..chunk.len()).collect();
        index.sort_unstable_by(|&a, &b| {
            let (da, ta, _) = &chunk[a];
            let (db, tb, _) = &chunk[b];
            (da, ta).cmp(&(db, tb))
        });
        let mut out: Chunk<D, T, R> = ColumnationStack::with_capacity(chunk.len());
        let mut iter = index.iter().peekable();
        while let Some(&i) = iter.next() {
            let (d, t, r) = &chunk[i];
            let mut diff = r.clone();
            while let Some(&&j) = iter.peek() {
                let (d2, t2, r2) = &chunk[j];
                if (d2, t2).cmp(&(d, t)) == Ordering::Equal {
                    diff.plus_equals(r2);
                    iter.next();
                } else {
                    break;
                }
            }
            if !diff.is_zero() {
                out.copy_destructured(d, t, &diff);
            }
        }
        out
    }

    /// Sort and consolidate everything pending into a chain of chunks, in one copy pass.
    fn sort_pending(&mut self) -> Vec<Chunk<D, T, R>> {
        let mut index: Vec<(u64, u32, u32)> = Vec::with_capacity(self.pending_len);
        for (ci, chunk) in self.pending.iter().enumerate() {
            let ci = u32::try_from(ci).expect("chunk count fits in u32");
            for (pi, (data, _, _)) in chunk.iter().enumerate() {
                let pi = u32::try_from(pi).expect("chunk length fits in u32");
                index.push((data.sort_prefix(), ci, pi));
            }
        }
        let pending = &self.pending;
        let get = |e: &(u64, u32, u32)| {
            &pending[usize::try_from(e.1).unwrap()][usize::try_from(e.2).unwrap()]
        };
        // Full comparisons only on equal prefixes, which `sort_prefix` guarantees agree with the
        // row order otherwise.
        index.sort_unstable_by(|a, b| {
            a.0.cmp(&b.0).then_with(|| {
                let (da, ta, _) = get(a);
                let (db, tb, _) = get(b);
                (da, ta).cmp(&(db, tb))
            })
        });

        let cap = Self::chunk_capacity();
        let mut output = Vec::with_capacity(self.pending_len / cap + 1);
        let mut result: Chunk<D, T, R> = ColumnationStack::with_capacity(cap);
        let mut iter = index.iter().peekable();
        while let Some(e) = iter.next() {
            let (d, t, r) = get(e);
            let mut diff = r.clone();
            while let Some(n) = iter.peek() {
                let (d2, t2, r2) = get(n);
                if (d2, t2).cmp(&(d, t)) == Ordering::Equal {
                    diff.plus_equals(r2);
                    iter.next();
                } else {
                    break;
                }
            }
            if !diff.is_zero() {
                if result.len() == cap {
                    output.push(std::mem::replace(
                        &mut result,
                        ColumnationStack::with_capacity(cap),
                    ));
                }
                result.copy_destructured(d, t, &diff);
            }
        }
        if !result.is_empty() {
            output.push(result);
        }
        self.pending.clear();
        self.pending_len = 0;
        output
    }
}

impl<D, T, R> Batcher for SnapshotBatcher<D, T, R>
where
    D: SortPrefix + Ord + Columnation + Clone + 'static,
    T: Timestamp + Lattice + Columnation + Clone + 'static,
    R: Default + Semigroup + Columnation + Clone + 'static,
{
    type Time = T;
    type Output = Chunk<D, T, R>;

    fn new(logger: Option<Logger>, operator_id: usize) -> Self {
        Self {
            pending: Vec::new(),
            pending_len: 0,
            time: None,
            inner: MergeBatcher::new(logger, operator_id),
            general: false,
            lower: Antichain::from_elem(T::minimum()),
            frontier: Antichain::new(),
        }
    }

    fn seal(&mut self, upper: Antichain<T>) -> (Vec<Self::Output>, Description<T>) {
        self.frontier.clear();
        let readied = if self.general {
            // The inner batcher's own description starts at the minimum time, so only its
            // chain and frontier are used and the description is formed here.
            let (readied, _) = self.inner.seal(upper.clone());
            self.frontier.extend(self.inner.frontier().iter().cloned());
            readied
        } else {
            match self.time.clone() {
                Some(t) if !upper.less_equal(&t) => {
                    self.time = None;
                    self.sort_pending()
                }
                Some(t) => {
                    self.frontier.insert(t);
                    Vec::new()
                }
                None => Vec::new(),
            }
        };
        let description = Description::new(
            self.lower.clone(),
            upper.clone(),
            Antichain::from_elem(T::minimum()),
        );
        self.lower = upper;
        (readied, description)
    }

    #[inline]
    fn frontier(&mut self) -> AntichainRef<'_, T> {
        self.frontier.borrow()
    }
}

impl<D, T, R> PushInto<Chunk<D, T, R>> for SnapshotBatcher<D, T, R>
where
    D: SortPrefix + Ord + Columnation + Clone + 'static,
    T: Timestamp + Lattice + Columnation + Clone + 'static,
    R: Default + Semigroup + Columnation + Clone + 'static,
{
    fn push_into(&mut self, chunk: Chunk<D, T, R>) {
        if chunk.is_empty() {
            return;
        }
        if !self.general {
            let t0 = match &self.time {
                Some(t) => t.clone(),
                None => {
                    let t = chunk[0].1.clone();
                    self.time = Some(t.clone());
                    t
                }
            };
            if chunk.iter().all(|(_, t, _)| *t == t0) {
                self.pending_len += chunk.len();
                self.pending.push(chunk);
                return;
            }
            // A second time: hand everything held to the general path, for good. The merge
            // batcher needs sorted, consolidated chunks, which the chunker did not provide.
            self.general = true;
            self.time = None;
            self.pending_len = 0;
            for held in self.pending.drain(..) {
                self.inner.push_into(Self::sort_chunk(held));
            }
        }
        self.inner.push_into(Self::sort_chunk(chunk));
    }
}

/// A chunker that packs incoming updates into [`ColumnationStack`] chunks in arrival order.
///
/// The sorting chunkers sort and consolidate every input container before the batcher sees
/// it. A [`SnapshotBatcher`] sorts everything it holds at `seal`, so that work is wasted on
/// its fast path; on the fallback path it sorts the held chunks itself (see
/// [`SnapshotBatcher::sort_chunk`]). Only pair this chunker with that batcher.
pub struct UnsortedChunker<D, T, R>
where
    D: Columnation,
    T: Columnation,
    R: Columnation,
{
    pending: Vec<(D, T, R)>,
    ready: VecDeque<ColumnationStack<(D, T, R)>>,
    empty: Option<ColumnationStack<(D, T, R)>>,
}

impl<D, T, R> Default for UnsortedChunker<D, T, R>
where
    D: Columnation,
    T: Columnation,
    R: Columnation,
{
    fn default() -> Self {
        Self {
            pending: Vec::new(),
            ready: VecDeque::new(),
            empty: None,
        }
    }
}

impl<D, T, R> UnsortedChunker<D, T, R>
where
    D: Columnation,
    T: Columnation,
    R: Columnation,
{
    /// Records per chunk, matching the merge batcher's 64 KiB chunks.
    fn chunk_capacity() -> usize {
        const BUFFER_SIZE_BYTES: usize = 64 << 10;
        let size = std::mem::size_of::<(D, T, R)>();
        if size == 0 {
            BUFFER_SIZE_BYTES
        } else if size <= BUFFER_SIZE_BYTES {
            BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }

    fn form_chunks(&mut self, all: bool) {
        let cap = Self::chunk_capacity();
        while self.pending.len() >= cap || (all && !self.pending.is_empty()) {
            let take = std::cmp::min(self.pending.len(), cap);
            let mut chunk = ColumnationStack::with_capacity(cap);
            for item in self.pending.drain(..take) {
                chunk.copy(&item);
            }
            self.ready.push_back(chunk);
        }
    }
}

impl<'a, D, T, R> PushInto<&'a mut Vec<(D, T, R)>> for UnsortedChunker<D, T, R>
where
    D: Columnation,
    T: Columnation,
    R: Columnation,
{
    fn push_into(&mut self, container: &'a mut Vec<(D, T, R)>) {
        self.pending.append(container);
        self.form_chunks(false);
    }
}

impl<'a, D, T, R> PushInto<&'a mut Column<(D, T, R)>> for UnsortedChunker<D, T, R>
where
    D: Columnar + Columnation,
    T: Columnar + Columnation,
    R: Columnar + Columnation,
{
    fn push_into(&mut self, container: &'a mut Column<(D, T, R)>) {
        let borrowed = container.borrow();
        self.pending.reserve(borrowed.len());
        for (d, t, r) in borrowed.into_index_iter() {
            self.pending
                .push((D::into_owned(d), T::into_owned(t), R::into_owned(r)));
        }
        self.form_chunks(false);
    }
}

impl<D, T, R> ContainerBuilder for UnsortedChunker<D, T, R>
where
    D: Columnation + Clone + 'static,
    T: Columnation + Clone + 'static,
    R: Columnation + Clone + 'static,
{
    type Container = ColumnationStack<(D, T, R)>;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        if let Some(ready) = self.ready.pop_front() {
            self.empty = Some(ready);
            self.empty.as_mut()
        } else {
            None
        }
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        self.form_chunks(true);
        self.extract()
    }
}

#[cfg(test)]
mod tests {
    use mz_repr::Datum;

    use super::*;

    type B = SnapshotBatcher<(Row, ()), u64, i64>;

    /// A chunk as the chunker would produce it: sorted by `(data, time)` and consolidated.
    fn chunk(updates: &[(i64, u64, i64)]) -> Chunk<(Row, ()), u64, i64> {
        let mut rows: Vec<((Row, ()), u64, i64)> = updates
            .iter()
            .map(|(k, t, r)| ((Row::pack_slice(&[Datum::Int64(*k)]), ()), *t, *r))
            .collect();
        rows.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        let mut out = ColumnationStack::with_capacity(rows.len());
        for row in &rows {
            out.copy(row);
        }
        out
    }

    fn collect(chain: &[Chunk<(Row, ()), u64, i64>]) -> Vec<(i64, u64, i64)> {
        chain
            .iter()
            .flat_map(|c| c.iter())
            .map(|((k, ()), t, r)| (k.iter().next().unwrap().unwrap_int64(), *t, *r))
            .collect()
    }

    fn upper(t: u64) -> Antichain<u64> {
        Antichain::from_elem(t)
    }

    #[mz_ore::test]
    fn single_time_sorts_and_consolidates_across_chunks() {
        let mut b = B::new(None, 0);
        b.push_into(chunk(&[(5, 1, 1), (3, 1, 1)]));
        b.push_into(chunk(&[(4, 1, 1), (3, 1, 2)]));
        b.push_into(chunk(&[(1, 1, 1), (5, 1, -1)]));
        let (chain, desc) = b.seal(upper(2));
        assert_eq!(
            collect(&chain),
            vec![(1, 1, 1), (3, 1, 3), (4, 1, 1)],
            "sorted, duplicates summed, cancellations dropped"
        );
        assert_eq!(desc.lower(), &Antichain::from_elem(0));
        assert_eq!(desc.upper(), &upper(2));
        assert!(b.frontier().is_empty());
    }

    #[mz_ore::test]
    fn seal_below_the_held_time_keeps_everything() {
        let mut b = B::new(None, 0);
        b.push_into(chunk(&[(2, 5, 1), (1, 5, 1)]));
        let (chain, desc) = b.seal(upper(5));
        assert!(chain.is_empty());
        assert_eq!(desc.lower(), &Antichain::from_elem(0));
        assert_eq!(b.frontier(), AntichainRef::new(&[5]));
        let (chain, desc) = b.seal(upper(6));
        assert_eq!(collect(&chain), vec![(1, 5, 1), (2, 5, 1)]);
        assert_eq!(desc.lower(), &upper(5), "descriptions abut");
        assert!(b.frontier().is_empty());
    }

    #[mz_ore::test]
    fn second_time_falls_back_to_the_general_path() {
        let mut b = B::new(None, 0);
        b.push_into(chunk(&[(2, 1, 1), (1, 1, 1)]));
        b.push_into(chunk(&[(1, 2, 1), (3, 1, 1)]));
        // Still consistent after the handover.
        b.push_into(chunk(&[(0, 3, 1)]));
        let (chain, _) = b.seal(upper(3));
        assert_eq!(
            collect(&chain),
            vec![(1, 1, 1), (1, 2, 1), (2, 1, 1), (3, 1, 1)]
        );
        assert_eq!(b.frontier(), AntichainRef::new(&[3]));
        let (chain, desc) = b.seal(upper(4));
        assert_eq!(collect(&chain), vec![(0, 3, 1)]);
        assert_eq!(desc.lower(), &upper(3));
    }

    #[mz_ore::test]
    fn later_single_time_batches_stay_on_the_fast_path() {
        let mut b = B::new(None, 0);
        b.push_into(chunk(&[(2, 1, 1)]));
        let (chain, _) = b.seal(upper(2));
        assert_eq!(collect(&chain), vec![(2, 1, 1)]);
        b.push_into(chunk(&[(9, 2, 1), (8, 2, 1)]));
        let (chain, _) = b.seal(upper(3));
        assert_eq!(collect(&chain), vec![(8, 2, 1), (9, 2, 1)]);
        assert!(!b.general);
    }

    /// A chunk in arrival order, as [`UnsortedChunker`] produces.
    fn unsorted(updates: &[(i64, u64, i64)]) -> Chunk<(Row, ()), u64, i64> {
        let mut out = ColumnationStack::with_capacity(updates.len());
        for (k, t, r) in updates {
            out.copy(&((Row::pack_slice(&[Datum::Int64(*k)]), ()), *t, *r));
        }
        out
    }

    #[mz_ore::test]
    fn unsorted_chunks_on_both_paths() {
        let mut b = B::new(None, 0);
        b.push_into(unsorted(&[(3, 1, 1), (1, 1, 1), (3, 1, 1)]));
        let (chain, _) = b.seal(upper(2));
        assert_eq!(collect(&chain), vec![(1, 1, 1), (3, 1, 2)]);
        b.push_into(unsorted(&[(9, 3, 1), (2, 3, 1), (9, 3, -1)]));
        b.push_into(unsorted(&[(5, 4, 1), (2, 3, 1)]));
        let (chain, _) = b.seal(upper(5));
        assert!(b.general);
        assert_eq!(collect(&chain), vec![(2, 3, 2), (5, 4, 1)]);
    }

    #[mz_ore::test]
    fn unsorted_chunker_keeps_arrival_order_and_chunk_size() {
        let mut c: UnsortedChunker<(Row, ()), u64, i64> = UnsortedChunker::default();
        let mut input: Vec<((Row, ()), u64, i64)> = (0..5)
            .rev()
            .map(|k| ((Row::pack_slice(&[Datum::Int64(k)]), ()), 1, 1))
            .collect();
        c.push_into(&mut input);
        assert!(c.extract().is_none(), "below chunk capacity, nothing ready");
        let chunk = c.finish().expect("finish flushes");
        let keys: Vec<i64> = chunk
            .iter()
            .map(|((k, ()), _, _)| k.iter().next().unwrap().unwrap_int64())
            .collect();
        assert_eq!(keys, vec![4, 3, 2, 1, 0]);
    }

    #[mz_ore::test]
    fn sort_prefix_agrees_with_row_order() {
        let rows: Vec<Row> = [
            vec![Datum::Int64(-5)],
            vec![Datum::Int64(0)],
            vec![Datum::Int64(1)],
            vec![Datum::Int64(1), Datum::Int64(2)],
            vec![Datum::Int64(1 << 40)],
            vec![Datum::Null],
            vec![Datum::String("a")],
            vec![Datum::String("abcdefgh")],
            vec![Datum::String("abcdefgi")],
        ]
        .iter()
        .map(|d| Row::pack_slice(d))
        .collect();
        for a in &rows {
            for b in &rows {
                if a < b {
                    assert!(a.sort_prefix() <= b.sort_prefix(), "{a:?} < {b:?}");
                }
            }
        }
    }
}
