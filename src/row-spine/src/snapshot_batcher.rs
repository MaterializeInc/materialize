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
use mz_ore::cast::CastFrom;
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
        // Sort by prefix alone; equal prefixes are settled by full comparison as they are gathered,
        // which `sort_prefix` guarantees agrees with the row order otherwise.
        radix_sort_by_prefix(&mut index);

        let cap = Self::chunk_capacity();
        let mut output = Vec::with_capacity(self.pending_len / cap + 1);
        let mut result: Chunk<D, T, R> = ColumnationStack::with_capacity(cap);
        // The sorted order visits the held rows at random, so each row is a cache miss. Touching
        // the row `LOOKAHEAD` entries ahead lets that miss overlap the work on this row. The
        // value read is unused.
        const LOOKAHEAD: usize = 16;
        let n = index.len();
        let mut i = 0;
        while i < n {
            if i + LOOKAHEAD < n {
                let (d, _, _) = get(&index[i + LOOKAHEAD]);
                // SAFETY: `d` is a live element of a held chunk.
                unsafe { std::ptr::read_volatile(std::ptr::from_ref(d).cast::<u8>()) };
            }
            // A run of equal prefixes is the only place rows can compare equal, and the only
            // place the prefix order leaves undecided. Order the run by full comparison first.
            let prefix = index[i].0;
            let mut end = i + 1;
            while end < n && index[end].0 == prefix {
                end += 1;
            }
            if end - i > 1 {
                index[i..end].sort_unstable_by(|a, b| {
                    let (da, ta, _) = get(a);
                    let (db, tb, _) = get(b);
                    (da, ta).cmp(&(db, tb))
                });
            }
            while i < end {
                let (d, t, r) = get(&index[i]);
                let mut diff = r.clone();
                i += 1;
                while i < end {
                    let (d2, t2, r2) = get(&index[i]);
                    if (d2, t2).cmp(&(d, t)) == Ordering::Equal {
                        diff.plus_equals(r2);
                        i += 1;
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
        }
        if !result.is_empty() {
            output.push(result);
        }
        self.pending.clear();
        self.pending_len = 0;
        output
    }
}

/// Sorts `index` by its `u64` prefix with a least-significant-digit radix sort, skipping the
/// byte positions on which every prefix agrees.
///
/// Row prefixes share their length and leading tag bytes, so typically three or four of the
/// eight passes run. Each pass streams the index once, against the log-factor of a comparison
/// sort over a 16-byte-entry index that no longer fits in cache. Small inputs use the
/// comparison sort, whose constant is lower.
fn radix_sort_by_prefix(index: &mut Vec<(u64, u32, u32)>) {
    const RADIX_MIN: usize = 1 << 16;
    let n = index.len();
    if n < RADIX_MIN {
        index.sort_unstable_by_key(|e| e.0);
        return;
    }
    let mut histograms = [[0u32; 256]; 8];
    for (prefix, _, _) in index.iter() {
        for (digit, histogram) in histograms.iter_mut().enumerate() {
            histogram[usize::cast_from((prefix >> (8 * digit)) & 0xFF)] += 1;
        }
    }
    let mut scratch: Vec<(u64, u32, u32)> = vec![(0, 0, 0); n];
    let mut in_index = true;
    for (digit, histogram) in histograms.iter().enumerate() {
        if histogram.iter().any(|&count| usize::cast_from(count) == n) {
            continue;
        }
        let mut offsets = [0usize; 256];
        let mut sum = 0;
        for (bucket, &count) in histogram.iter().enumerate() {
            offsets[bucket] = sum;
            sum += usize::cast_from(count);
        }
        let (src, dst) = if in_index {
            (&*index, &mut scratch)
        } else {
            (&scratch, &mut *index)
        };
        for entry in src.iter() {
            let bucket = usize::cast_from((entry.0 >> (8 * digit)) & 0xFF);
            dst[offsets[bucket]] = *entry;
            offsets[bucket] += 1;
        }
        in_index = !in_index;
    }
    if !in_index {
        std::mem::swap(index, &mut scratch);
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
/// its fast path; on the fallback path it sorts the held chunks itself before handing them to
/// the merge batcher. Only pair this chunker with that batcher.
pub struct UnsortedChunker<D, T, R>
where
    D: Columnation,
    T: Columnation,
    R: Columnation,
{
    /// The chunk being filled; updates are copied into it as they arrive.
    open: ColumnationStack<(D, T, R)>,
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
            open: ColumnationStack::default(),
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

    /// Moves the open chunk to `ready` once it is full.
    #[inline]
    fn roll_if_full(&mut self) {
        let cap = Self::chunk_capacity();
        if self.open.len() >= cap {
            let full = std::mem::replace(&mut self.open, ColumnationStack::with_capacity(cap));
            self.ready.push_back(full);
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
        for item in container.drain(..) {
            self.open.copy(&item);
            self.roll_if_full();
        }
    }
}

impl<'a, D, T, R> PushInto<&'a mut Column<(D, T, R)>> for UnsortedChunker<D, T, R>
where
    D: Columnar + Columnation,
    T: Columnar + Columnation,
    R: Columnar + Columnation,
{
    fn push_into(&mut self, container: &'a mut Column<(D, T, R)>) {
        for (d, t, r) in container.borrow().into_index_iter() {
            self.open
                .copy_destructured(&D::into_owned(d), &T::into_owned(t), &R::into_owned(r));
            self.roll_if_full();
        }
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
        if !self.open.is_empty() {
            let open = std::mem::take(&mut self.open);
            self.ready.push_back(open);
        }
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

    /// Timing of the snapshot path on exchange-shaped input. Run with
    /// `cargo test --profile optimized -p mz-row-spine -- --ignored --nocapture bench_snapshot`;
    /// `BENCH_ROWS` and `BENCH_KEY_MOD` (0 for full-range keys) shape the input.
    #[mz_ore::test]
    #[ignore]
    fn bench_snapshot_path() {
        use differential_dataflow::trace::Builder;
        use mz_repr::Timestamp;
        use std::time::Instant;

        let rows_n: usize = std::env::var("BENCH_ROWS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10_000_000);
        let key_mod: u64 = std::env::var("BENCH_KEY_MOD")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let per_container = 200;
        let mut containers: Vec<Vec<((Row, Row), Timestamp, i64)>> = Vec::new();
        let mut cur = Vec::with_capacity(per_container);
        for i in 0..rows_n {
            let mut k = u64::cast_from(i).wrapping_mul(0x9E3779B97F4A7C15) >> 1;
            if key_mod > 0 {
                k %= key_mod;
            }
            let k = i64::try_from(k).expect("fits after the shift");
            let key = Row::pack_slice(&[Datum::Int64(k)]);
            let val = Row::pack_slice(&[Datum::Int64(i64::try_from(i).expect("row count fits"))]);
            cur.push(((key, val), Timestamp::from(1u64), 1i64));
            if cur.len() == per_container {
                containers.push(std::mem::take(&mut cur));
            }
        }
        if !cur.is_empty() {
            containers.push(cur);
        }
        println!(
            "{} rows in {} containers, key_mod {}",
            rows_n,
            containers.len(),
            key_mod
        );
        let upper = Antichain::from_elem(Timestamp::from(2u64));

        for round in 0..2 {
            let t = Instant::now();
            let mut ch: UnsortedChunker<(Row, Row), Timestamp, i64> = Default::default();
            let mut b: SnapshotBatcher<(Row, Row), Timestamp, i64> = Batcher::new(None, 0);
            for c in containers.iter() {
                let mut c = c.clone();
                ch.push_into(&mut c);
                while let Some(chunk) = ch.extract() {
                    b.push_into(std::mem::take(chunk));
                }
            }
            while let Some(chunk) = ch.finish() {
                b.push_into(std::mem::take(chunk));
            }
            let t_push = t.elapsed();
            let (mut chain, desc) = b.seal(upper.clone());
            let t_seal = t.elapsed() - t_push;
            let batch = crate::RowRowBuilder::<Timestamp, i64>::seal(&mut chain, desc);
            let t_build = t.elapsed() - t_push - t_seal;
            println!(
                "round {round}: chunk {:?} seal {:?} build {:?} total {:?} ({} updates)",
                t_push,
                t_seal,
                t_build,
                t.elapsed(),
                differential_dataflow::trace::BatchReader::len(&batch)
            );
        }
    }

    #[mz_ore::test]
    fn radix_sort_matches_comparison_sort() {
        // Above the radix threshold, with two constant byte positions to skip and ties.
        let mut index: Vec<(u64, u32, u32)> = (0..100_000u32)
            .map(|i| {
                let scrambled = u64::from(i).wrapping_mul(0x9E3779B97F4A7C15);
                let prefix =
                    (0x0009 << 48) | (scrambled & 0x0000_00FF_FFFF_0000) | (u64::from(i % 7) << 8);
                (prefix, i / 1000, i % 1000)
            })
            .collect();
        let mut expected = index.clone();
        expected.sort_unstable();
        radix_sort_by_prefix(&mut index);
        assert!(
            index.windows(2).all(|w| w[0].0 <= w[1].0),
            "sorted by prefix"
        );
        index.sort_unstable();
        assert_eq!(index, expected, "a permutation of the input");
    }

    #[mz_ore::test]
    fn equal_prefixes_are_ordered_by_full_comparison() {
        // Two-column rows sharing the first column share the six-byte prefix; the second column
        // must still order them, and equal rows must still consolidate.
        let mut updates: Vec<((Row, ()), u64, i64)> = Vec::new();
        for y in [5i64, -3, 9, 0, 5, 9] {
            let row = Row::pack_slice(&[Datum::Int64(1 << 40), Datum::Int64(y)]);
            updates.push(((row, ()), 1, 1));
        }
        let mut b = B::new(None, 0);
        let mut chunk = ColumnationStack::with_capacity(updates.len());
        for u in &updates {
            chunk.copy(u);
        }
        b.push_into(chunk);
        let (chain, _) = b.seal(upper(2));
        let sealed: Vec<(Row, i64)> = chain
            .iter()
            .flat_map(|c| c.iter())
            .map(|((k, ()), _, r)| (k.clone(), *r))
            .collect();
        let mut expected: Vec<(Row, i64)> = Vec::new();
        let mut rows: Vec<Row> = updates.iter().map(|((k, ()), _, _)| k.clone()).collect();
        rows.sort();
        for row in rows {
            match expected.last_mut() {
                Some((prev, r)) if *prev == row => *r += 1,
                _ => expected.push((row, 1)),
            }
        }
        assert_eq!(sealed, expected);
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
