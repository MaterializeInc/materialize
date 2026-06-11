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

//! Paged sealed runs: Layer 3 of the buffer-managed state design
//! (`doc/developer/design/20260610_buffer_managed_state.md`) prototyped over
//! [`Column`] and the [`mz_ore::pool`] buffer pool.
//!
//! A [`PagedRun`] stores a sealed sorted run of `(D, T, R)` updates as
//! pool-resident column pages plus a small always-resident header: per-page
//! fence keys (the first and last `D` of each page, owned) and per-page update
//! counts. Pages are sealed into the pool and evicted eagerly,
//! hydration-style, so a freshly built run holds no resident data beyond its
//! header.
//!
//! The read paths follow the design's access-pattern story:
//!
//! * [`PagedRun::seek`] binary-searches the resident fence keys with zero I/O
//!   and faults exactly the pages whose key range can contain the key — one
//!   page in the common case, more only when the key straddles page
//!   boundaries.
//! * [`PagedRun::iter`] scans the run pinning one page at a time, prefetching
//!   the next page before reading the current one, and re-evicting each page
//!   after consuming it so the resident window stays bounded.
//! * [`PagedRun::merge`] streams two runs into a new one, holding at most one
//!   pinned input page per side plus one output accumulation column. Consumed
//!   input pages are re-evicted cheaply (they are `BackedResident`, so
//!   eviction is a pure page release).
//!
//! Reads from pinned pool memory are zero-copy: the borrowed columnar view is
//! reconstructed from the pinned `&[u64]` exactly as [`Column::borrow`] does
//! for its serialized variants. This is sound because a pinned chunk is never
//! evicted or relocated for the life of the borrow; views are re-derived from
//! a fresh pin on every access and never cached across pins.

use std::marker::PhantomData;

use columnar::bytes::indexed;
use columnar::{Borrow, BorrowedOf, Columnar, ContainerOf, FromBytes, Index, Len, Push};
use differential_dataflow::difference::Semigroup;
use mz_ore::pool::{ChunkHandle, PinGuard, Pool, Residency};
use timely::dataflow::channels::ContainerBytes;

use crate::columnar::{Column, at_serialized_capacity};

/// Reconstructs the borrowed columnar view from serialized words, the same
/// zero-copy decode [`Column::borrow`] performs on its `Align` variant. The
/// words here come from pinned pool memory instead of an owned `Vec<u64>`.
fn borrow_words<C: Columnar>(words: &[u64]) -> BorrowedOf<'_, C> {
    <BorrowedOf<'_, C>>::from_bytes(&mut indexed::decode(words))
}

/// Serializes a column into `u64` words for pool insertion. `Column::Align`
/// already is the wanted representation and moves with no copy; other
/// variants serialize through [`ContainerBytes::into_bytes`] and widen.
fn into_words<C: Columnar>(column: Column<C>) -> Vec<u64> {
    match column {
        Column::Align(words) => words,
        other => {
            let len_bytes = other.length_in_bytes();
            let mut bytes = Vec::with_capacity(len_bytes);
            other.into_bytes(&mut bytes);
            debug_assert_eq!(bytes.len() % 8, 0);
            bytemuck::allocation::pod_collect_to_vec(&bytes)
        }
    }
}

/// A sealed sorted run of `(D, T, R)` updates, stored as eagerly evicted pool
/// pages plus an always-resident header of fence keys and update counts.
///
/// Updates are sorted by `(D, T)` across the whole run. A single key's
/// updates may straddle consecutive pages, which is why the header keeps both
/// the first and the last key of each page.
pub struct PagedRun<D: Columnar, T: Columnar, R: Columnar> {
    /// The pool holding this run's pages.
    pool: Pool,
    /// One serialized `Column<(D, T, R)>` per page.
    chunks: Vec<ChunkHandle>,
    /// First key of each page, owned and always resident.
    first_keys: Vec<D>,
    /// Last key of each page, owned and always resident.
    last_keys: Vec<D>,
    /// Number of updates in each page.
    update_counts: Vec<usize>,
    /// The pages hold `(D, T, R)` updates; only `D` appears in the resident
    /// header fields.
    _marker: PhantomData<(T, R)>,
}

/// Accumulates sealed pages and their header entries while a run is built.
struct RunBuilder<'a, D: Columnar, T: Columnar, R: Columnar> {
    pool: &'a Pool,
    chunks: Vec<ChunkHandle>,
    first_keys: Vec<D>,
    last_keys: Vec<D>,
    update_counts: Vec<usize>,
    /// Output accumulation container for update-at-a-time building (merge).
    current: ContainerOf<(D, T, R)>,
    current_len: usize,
}

impl<'a, D: Columnar, T: Columnar, R: Columnar> RunBuilder<'a, D, T, R> {
    fn new(pool: &'a Pool) -> Self {
        RunBuilder {
            pool,
            chunks: Vec::new(),
            first_keys: Vec::new(),
            last_keys: Vec::new(),
            update_counts: Vec::new(),
            current: Default::default(),
            current_len: 0,
        }
    }

    /// Pushes one update into the accumulation container, sealing it as a
    /// page once its serialized size reaches the ship target.
    fn push(&mut self, update: &(D, T, R)) {
        self.current.push(update);
        self.current_len += 1;
        if at_serialized_capacity(&self.current.borrow()) {
            self.flush();
        }
    }

    /// Seals the accumulation container as a page, if it holds any updates.
    fn flush(&mut self) {
        if self.current_len == 0 {
            return;
        }
        let len = self.current_len;
        self.current_len = 0;
        let column = Column::Typed(std::mem::take(&mut self.current));
        self.seal(column, len);
    }

    /// Seals one non-empty column as a page: captures its fence keys as
    /// owned values, serializes it into the pool, and evicts it eagerly.
    fn seal(&mut self, column: Column<(D, T, R)>, len: usize) {
        debug_assert!(len > 0, "sealed pages are non-empty");
        let (first, last) = {
            let borrow = column.borrow();
            let (first_d, _, _) = borrow.get(0);
            let (last_d, _, _) = borrow.get(borrow.len() - 1);
            (D::into_owned(first_d), D::into_owned(last_d))
        };
        let mut words = into_words(column);
        let handle = self.pool.insert(&mut words);
        self.pool.evict(&handle);
        self.chunks.push(handle);
        self.first_keys.push(first);
        self.last_keys.push(last);
        self.update_counts.push(len);
    }

    fn finish(mut self) -> PagedRun<D, T, R> {
        self.flush();
        PagedRun {
            pool: self.pool.clone(),
            chunks: self.chunks,
            first_keys: self.first_keys,
            last_keys: self.last_keys,
            update_counts: self.update_counts,
            _marker: PhantomData,
        }
    }
}

impl<D: Columnar, T: Columnar, R: Columnar> PagedRun<D, T, R> {
    /// Builds a run from columns that are globally sorted by `(D, T)` with
    /// arbitrary breakpoints (the merge-batcher chain output contract). Each
    /// non-empty column becomes one page, sealed into the pool and evicted
    /// eagerly; empty columns are skipped.
    ///
    /// A column whose serialized size exceeds the pool's largest size class
    /// becomes a [`Residency::Oversize`] page: always resident, exempt from
    /// eviction and budget enforcement, and counted against the budget. The
    /// prototype accepts this degradation rather than splitting pages;
    /// `PoolStats::oversize_bytes` makes it observable.
    pub fn build(pool: &Pool, columns: impl IntoIterator<Item = Column<(D, T, R)>>) -> Self {
        let mut builder = RunBuilder::new(pool);
        for column in columns {
            let len = column.borrow().len();
            if len == 0 {
                continue;
            }
            builder.seal(column, len);
        }
        builder.finish()
    }

    /// The number of pages in the run.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// The total number of updates in the run.
    pub fn len(&self) -> usize {
        self.update_counts.iter().sum()
    }

    /// Returns `true` if the run holds no updates.
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// The residency state of each page, in page order.
    pub fn residencies(&self) -> Vec<Residency> {
        self.chunks.iter().map(|c| c.residency()).collect()
    }

    /// Evicts every page of the run. Pages faulted in by reads are cheap to
    /// re-evict (`BackedResident`); the call performs no compression I/O for
    /// them.
    pub fn evict_all(&self) {
        for chunk in &self.chunks {
            self.pool.evict(chunk);
        }
    }

    /// Returns the `(T, R)` pairs recorded for `key`, in update order.
    ///
    /// Binary-searches the resident fence keys with zero I/O for the page
    /// range that can contain `key`. The range covers more than one page only
    /// when the key's updates straddle page boundaries
    /// (`last_keys[i] == key == first_keys[i + 1]`); a key falling in the gap
    /// between two pages yields an empty range and faults nothing. Each
    /// visited page is pinned (faulting it from its extent if evicted) and
    /// binary-searched within; faulted pages stay `BackedResident` afterwards,
    /// leaving re-eviction to the pool's budget enforcement.
    pub fn seek(&self, key: &D) -> Vec<(T, R)>
    where
        D: Ord,
    {
        let lo = self.last_keys.partition_point(|k| k < key);
        let hi = self.first_keys.partition_point(|k| k <= key);
        let mut out = Vec::new();
        for chunk in &self.chunks[lo..hi] {
            let pin = chunk.pin();
            let view = borrow_words::<(D, T, R)>(&pin);
            let len = view.len();
            let start = partition_point(len, |i| {
                let (d, _, _) = view.get(i);
                D::into_owned(d) < *key
            });
            for index in start..len {
                let (d, t, r) = view.get(index);
                if D::into_owned(d) != *key {
                    break;
                }
                out.push((T::into_owned(t), R::into_owned(r)));
            }
        }
        out
    }

    /// Iterates over the whole run in `(D, T)` order, yielding owned updates.
    ///
    /// Pins one page at a time and prefetches the next page before reading
    /// the current one. Consumed pages are re-evicted so a scan keeps a
    /// bounded resident window regardless of run size.
    pub fn iter(&self) -> Iter<'_, D, T, R> {
        Iter {
            cursor: ChunkCursor::new(self),
        }
    }

    /// Merges two runs into a new one, consolidating updates with equal
    /// `(D, T)` by adding their `R`s and dropping updates whose sum is zero,
    /// as [`differential_dataflow::consolidation::consolidate_updates`] does.
    ///
    /// The merge streams: at most one pinned input page per side plus one
    /// output accumulation column are held at a time. Output pages are cut at
    /// the crate's serialized ship target, sealed into `pool`, and evicted
    /// eagerly; fully consumed input pages are re-evicted (cheaply, they are
    /// `BackedResident`). The resident window is therefore bounded regardless
    /// of the input run sizes.
    pub fn merge(pool: &Pool, a: &Self, b: &Self) -> Self
    where
        D: Ord,
        T: Ord,
        R: Semigroup,
    {
        let mut builder = RunBuilder::new(pool);
        let mut a_cur = ChunkCursor::new(a);
        let mut b_cur = ChunkCursor::new(b);
        let mut pending: Option<(D, T, R)> = None;
        loop {
            a_cur.fill();
            b_cur.fill();
            let take_a = match (&a_cur.head, &b_cur.head) {
                (Some(x), Some(y)) => (&x.0, &x.1) <= (&y.0, &y.1),
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            let head = if take_a {
                &mut a_cur.head
            } else {
                &mut b_cur.head
            };
            let update = head.take().expect("head filled");
            match &mut pending {
                Some(p) if p.0 == update.0 && p.1 == update.1 => p.2.plus_equals(&update.2),
                _ => {
                    if let Some(prev) = pending.take() {
                        if !prev.2.is_zero() {
                            builder.push(&prev);
                        }
                    }
                    pending = Some(update);
                }
            }
        }
        if let Some(prev) = pending.take() {
            if !prev.2.is_zero() {
                builder.push(&prev);
            }
        }
        builder.finish()
    }
}

/// Index of the first element in `0..len` for which `pred` is false, assuming
/// `pred` is true for a prefix and false for the rest. `partition_point` over
/// indices rather than a slice, for searching borrowed columnar views.
fn partition_point(len: usize, mut pred: impl FnMut(usize) -> bool) -> usize {
    let (mut lo, mut hi) = (0, len);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if pred(mid) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Streams a run's updates page by page, holding at most one pin.
///
/// Opening page `i` first prefetches page `i + 1`, the readahead of the
/// design's scan path. A fully consumed page is re-evicted, keeping the
/// resident window at one page.
///
/// The borrowed columnar view cannot be stored next to the pin it borrows
/// from, so it is reconstructed per access; the decode is a handful of slice
/// splits over the pinned words.
struct ChunkCursor<'a, D: Columnar, T: Columnar, R: Columnar> {
    run: &'a PagedRun<D, T, R>,
    chunk: usize,
    pos: usize,
    pin: Option<PinGuard<'a>>,
    head: Option<(D, T, R)>,
}

impl<'a, D: Columnar, T: Columnar, R: Columnar> ChunkCursor<'a, D, T, R> {
    fn new(run: &'a PagedRun<D, T, R>) -> Self {
        ChunkCursor {
            run,
            chunk: 0,
            pos: 0,
            pin: None,
            head: None,
        }
    }

    /// Ensures `head` holds the next update, if any remain. Advances across
    /// page boundaries, re-evicting each consumed page.
    fn fill(&mut self) {
        while self.head.is_none() && self.chunk < self.run.chunks.len() {
            if self.pin.is_none() {
                if let Some(next) = self.run.chunks.get(self.chunk + 1) {
                    next.prefetch();
                }
                self.pin = Some(self.run.chunks[self.chunk].pin());
                self.pos = 0;
            }
            let pin = self.pin.as_ref().expect("pinned above");
            let view = borrow_words::<(D, T, R)>(pin);
            if self.pos < view.len() {
                let (d, t, r) = view.get(self.pos);
                self.pos += 1;
                self.head = Some((D::into_owned(d), T::into_owned(t), R::into_owned(r)));
            } else {
                self.pin = None;
                self.run.pool.evict(&self.run.chunks[self.chunk]);
                self.chunk += 1;
            }
        }
    }
}

/// Iterator over a [`PagedRun`]'s updates, returned by [`PagedRun::iter`].
pub struct Iter<'a, D: Columnar, T: Columnar, R: Columnar> {
    cursor: ChunkCursor<'a, D, T, R>,
}

impl<D: Columnar, T: Columnar, R: Columnar> Iterator for Iter<'_, D, T, R> {
    type Item = (D, T, R);

    fn next(&mut self) -> Option<(D, T, R)> {
        self.cursor.fill();
        self.cursor.head.take()
    }
}

#[cfg(test)]
mod tests {
    use mz_ore::pool::PoolConfig;

    use super::*;

    type Update = (u64, u64, i64);

    /// Pool with a small virtual reservation per class, suitable for tests.
    fn test_pool(budget_bytes: usize) -> Pool {
        Pool::new(PoolConfig {
            budget_bytes,
            class_capacity_bytes: 64 << 20,
        })
        .expect("pool creation")
    }

    fn column(updates: &[Update]) -> Column<Update> {
        Column::Typed(Columnar::as_columns(updates.iter()))
    }

    /// Three sorted pages with key 100 straddling the first page boundary and
    /// a key gap between the second and third pages.
    fn straddle_input() -> Vec<Vec<Update>> {
        let mut page0: Vec<Update> = (0..100).map(|d| (d, d, 1)).collect();
        page0.push((100, 0, 1));
        page0.push((100, 1, 2));
        let page1: Vec<Update> = [(100, 2, 3), (100, 3, 4)]
            .into_iter()
            .chain((101..200).map(|d| (d, d, 1)))
            .collect();
        let page2: Vec<Update> = (250..300).map(|d| (d, d, 1)).collect();
        vec![page0, page1, page2]
    }

    fn straddle_run(pool: &Pool) -> PagedRun<u64, u64, i64> {
        PagedRun::build(pool, straddle_input().iter().map(|p| column(p)))
    }

    /// Reference merge: sort by `(D, T)`, sum `R` for equal `(D, T)`, drop
    /// zero sums.
    fn reference_merge(mut updates: Vec<Update>) -> Vec<Update> {
        updates.sort();
        let mut out: Vec<Update> = Vec::new();
        for (d, t, r) in updates {
            match out.last_mut() {
                Some(prev) if prev.0 == d && prev.1 == t => prev.2 += r,
                _ => out.push((d, t, r)),
            }
            if out.last().map_or(false, |u| u.2 == 0) {
                out.pop();
            }
        }
        out
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn build_then_seek() {
        let pool = test_pool(256 << 20);
        let run = straddle_run(&pool);
        assert_eq!(run.chunk_count(), 3);
        assert_eq!(run.len(), 100 + 2 + 2 + 99 + 50);
        assert!(!run.is_empty());
        assert!(
            run.residencies().iter().all(|r| *r == Residency::Evicted),
            "all pages are evicted after build"
        );

        // First key of the run: one page faulted.
        let before = pool.stats().faults;
        assert_eq!(run.seek(&0), vec![(0, 1)]);
        assert_eq!(pool.stats().faults - before, 1);

        // Straddling key: exactly the two straddled pages faulted.
        run.evict_all();
        let before = pool.stats().faults;
        assert_eq!(run.seek(&100), vec![(0, 1), (1, 2), (2, 3), (3, 4)]);
        assert_eq!(pool.stats().faults - before, 2);

        // Interior and last keys: one page each.
        run.evict_all();
        let before = pool.stats().faults;
        assert_eq!(run.seek(&150), vec![(150, 1)]);
        assert_eq!(pool.stats().faults - before, 1);
        run.evict_all();
        let before = pool.stats().faults;
        assert_eq!(run.seek(&299), vec![(299, 1)]);
        assert_eq!(pool.stats().faults - before, 1);

        // Absent keys: in the gap between pages and past the end. The fence
        // search rejects both without faulting anything.
        run.evict_all();
        let before = pool.stats().faults;
        assert_eq!(run.seek(&225), Vec::new());
        assert_eq!(run.seek(&1000), Vec::new());
        assert_eq!(pool.stats().faults - before, 0);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn iter_matches_input() {
        let pool = test_pool(256 << 20);
        let input = straddle_input();
        let run = PagedRun::build(&pool, input.iter().map(|p| column(p)));
        let expected: Vec<Update> = input.into_iter().flatten().collect();
        assert_eq!(run.iter().collect::<Vec<_>>(), expected);
        assert!(
            run.residencies().iter().all(|r| *r == Residency::Evicted),
            "a scan re-evicts every page it consumed"
        );

        // macOS `MADV_DONTNEED` may leave freed slot contents intact and the
        // free list may hand a faulting chunk its previous slot, so a bug
        // that skipped the extent read could pass by accident. Poison the
        // free slots to prove the reads come from the extents.
        pool.poison_free_slots();
        assert_eq!(run.iter().collect::<Vec<_>>(), expected);
        assert_eq!(run.seek(&100), vec![(0, 1), (1, 2), (2, 3), (3, 4)]);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn merge_matches_reference() {
        // Budget of three 64 KiB-class pages: two pinned inputs plus slack.
        let budget = 3 * (64 << 10);
        let pool = test_pool(budget);

        // Two overlapping runs, split into 1000-update pages (24 KB each,
        // landing in the 64 KiB class). Keys divisible by six cancel exactly.
        let a_updates: Vec<Update> = (0..6000).step_by(2).map(|d| (d, 0, 1)).collect();
        let b_updates: Vec<Update> = (0..6000)
            .step_by(3)
            .flat_map(|d| [(d, 0, -1), (d, 1, 1)])
            .collect();
        let a = PagedRun::build(&pool, a_updates.chunks(1000).map(column));
        let b = PagedRun::build(&pool, b_updates.chunks(1000).map(column));
        assert!(a.chunk_count() > 1 && b.chunk_count() > 1);

        let merged = PagedRun::merge(&pool, &a, &b);
        let expected = reference_merge(
            a_updates
                .iter()
                .chain(b_updates.iter())
                .copied()
                .collect::<Vec<_>>(),
        );
        assert!(expected.iter().any(|u| u.1 == 1), "some updates survive");
        assert_eq!(merged.iter().collect::<Vec<_>>(), expected);

        let stats = pool.stats();
        assert!(
            stats.resident_bytes <= u64::try_from(budget).expect("fits"),
            "resident {} exceeds budget {}",
            stats.resident_bytes,
            budget,
        );
        assert!(
            stats.evictions_cheap > 0,
            "consumed input pages are re-evicted cheaply"
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn merge_of_empty_runs() {
        let pool = test_pool(256 << 20);
        let empty = PagedRun::<u64, u64, i64>::build(&pool, [column(&[])]);
        assert!(empty.is_empty());
        assert_eq!(empty.chunk_count(), 0);
        assert_eq!(empty.seek(&0), Vec::new());
        assert_eq!(empty.iter().count(), 0);

        let run = straddle_run(&pool);
        let merged = PagedRun::merge(&pool, &empty, &run);
        assert_eq!(
            merged.iter().collect::<Vec<_>>(),
            reference_merge(run.iter().collect()),
        );
        let both_empty = PagedRun::merge(&pool, &empty, &empty);
        assert!(both_empty.is_empty());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn seek_traffic_respects_budget() {
        // Read-only probing of a run much larger than the budget: seeks
        // perform no inserts, so only fault-in-triggered budget enforcement
        // keeps the faulted pages from accumulating without bound.
        let budget = 2 * (64 << 10);
        let pool = test_pool(budget);
        let updates: Vec<Update> = (0..20_000).map(|d| (d, d, 1)).collect();
        let run = PagedRun::build(&pool, updates.chunks(1000).map(column));
        assert!(run.chunk_count() >= 10, "run spans many pages");
        assert!(
            run.len() * 24 > 2 * budget,
            "run is larger than twice the budget"
        );
        for d in (0..20_000).step_by(97) {
            assert_eq!(run.seek(&d), vec![(d, 1)]);
            let resident = pool.stats().resident_bytes;
            assert!(
                resident <= u64::try_from(budget).expect("fits"),
                "resident {resident} exceeds budget {budget} under seek-only traffic",
            );
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn oversize_pages_stay_resident() {
        // A page whose serialized size exceeds the largest pool size class
        // lands `Oversize`: always resident, exempt from eviction and budget
        // enforcement. The prototype accepts this degradation; this test
        // pins the behavior, that reads stay correct in it, and that the
        // `oversize_bytes` gauge surfaces it.
        let budget = 64 << 10;
        let pool = test_pool(budget);
        // Ten 1 MiB values serialize past the largest (8 MiB) size class.
        let updates: Vec<(Vec<u8>, u64, i64)> = (0..10u8)
            .map(|d| (vec![d; 1 << 20], u64::from(d), 1))
            .collect();
        let run = PagedRun::build(&pool, [Column::Typed(Columnar::as_columns(updates.iter()))]);
        assert_eq!(run.chunk_count(), 1);
        assert_eq!(run.residencies(), vec![Residency::Oversize]);
        let stats = pool.stats();
        assert!(
            stats.oversize_bytes > 8 << 20,
            "oversize gauge reflects the page: {stats:?}"
        );
        assert!(
            stats.resident_bytes > u64::try_from(budget).expect("fits"),
            "oversize pages escape the budget: {stats:?}"
        );
        // Eviction and enforcement are no-ops for oversize pages.
        run.evict_all();
        pool.enforce_budget();
        assert_eq!(run.residencies(), vec![Residency::Oversize]);
        // Reads remain correct in the degraded mode.
        assert_eq!(run.seek(&vec![3u8; 1 << 20]), vec![(3u64, 1i64)]);
        assert_eq!(run.iter().count(), updates.len());
    }

    /// Pages may relocate across evict/fault-in cycles (slots are scoped to
    /// residency); correctness rests on pin-mediated access, so contents must
    /// round-trip regardless of where a page lands.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn pages_round_trip_across_eviction() {
        let pool = test_pool(256 << 20);
        let run = straddle_run(&pool);
        let before: Vec<u64> = run.chunks[0].pin().to_vec();
        pool.evict(&run.chunks[0]);
        assert_eq!(run.chunks[0].residency(), Residency::Evicted);
        pool.poison_free_slots();
        let pin = run.chunks[0].pin();
        assert_eq!(&*pin, &before[..], "contents survive relocation");
    }
}
