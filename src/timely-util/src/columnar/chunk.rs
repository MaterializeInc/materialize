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
//! columnar layout, held resident on the heap behind an `Rc`. Implementing
//! [`Chunk`] buys the full differential harness — batches, the merge batcher,
//! fueled batch merging, and spines — with the four transducers below as the
//! only bespoke code; the merge and extract transducers delegate to the
//! chain-level machinery in [`merge_batcher`](crate::columnar::merge_batcher).
//!
//! Bodies are resident-only for now. The designed spill point is
//! [`Chunk::settle`]'s commit step ([`ColumnChunk::commit`]): committed
//! chunks are the long-lived ones, and routing them through the
//! [`ColumnPager`](crate::column_pager::ColumnPager) there — with resident
//! fence metadata retained for [`UnloadChunk::locate`] — is the deliberate
//! next step, not an accident of omission. The read side is already shaped
//! for it: consumers read through [`UnloadChunk`], whose copy-out contract
//! does not require bodies to stay resident.

use std::collections::VecDeque;
use std::rc::Rc;

use columnar::{Borrow, BorrowedOf, Columnar, Container as _, Index, Len, Push as _};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::chunk::Chunk;
use timely::Accountable;
use timely::container::{ContainerBuilder, PushInto};
use timely::dataflow::channels::ContainerBytes;
use timely::progress::Timestamp;
use timely::progress::frontier::AntichainRef;

use crate::column_pager::ColumnPager;
use crate::columnar::batcher::ColumnChunker;
use crate::columnar::merge_batcher::{FetchIter, extract_chain, merge_chains};
use crate::columnar::unload::UnloadChunk;
use crate::columnar::{Column, at_serialized_capacity};

/// The serialized-byte size committed chunks aim for, matching the ship size
/// of the columnar merge machinery.
const COMMIT_BYTES: usize = 2 << 20;

/// Whether a column is big enough to commit on its own. A monotone
/// threshold, deliberately not the periodic window of
/// [`at_serialized_capacity`]: settle grows its carry by whole chunks, which
/// can step over any fixed-width window, so the carry-full test must be
/// monotone in size or the carry grows far past the target.
fn at_commit_size<C: Columnar>(column: &Column<C>) -> bool {
    column.length_in_bytes() >= COMMIT_BYTES - COMMIT_BYTES / 10
}

/// Narrow a columnar ref to a shorter lifetime, so refs from different
/// borrows — a probe column and a chunk's own columns, say — can be compared
/// (the refs are lifetime-invariant).
fn rr<'b, 'a: 'b, C: Columnar>(item: columnar::Ref<'a, C>) -> columnar::Ref<'b, C> {
    columnar::ContainerOf::<C>::reborrow_ref(item)
}

/// First index at or after `start` where `pred` turns false, by exponential
/// then binary search. `pred` must hold on a prefix of `[start, upper)`.
fn gallop(upper: usize, start: usize, pred: impl Fn(usize) -> bool) -> usize {
    let mut pos = start;
    if pos < upper && pred(pos) {
        let mut step = 1;
        while pos + step < upper && pred(pos + step) {
            pos += step;
            step <<= 1;
        }
        step >>= 1;
        while step > 0 {
            if pos + step < upper && pred(pos + step) {
                pos += step;
            }
            step >>= 1;
        }
        pos += 1;
    }
    pos
}

/// A sorted, consolidated run of `(D, T, R)` updates, shared via `Rc`.
pub struct ColumnChunk<D: Columnar, T: Columnar, R: Columnar>(Rc<Column<(D, T, R)>>);

impl<D: Columnar, T: Columnar, R: Columnar> Clone for ColumnChunk<D, T, R> {
    fn clone(&self) -> Self {
        ColumnChunk(Rc::clone(&self.0))
    }
}

impl<D: Columnar, T: Columnar, R: Columnar> Default for ColumnChunk<D, T, R> {
    fn default() -> Self {
        ColumnChunk(Rc::new(Column::default()))
    }
}

impl<D: Columnar, T: Columnar, R: Columnar> Accountable for ColumnChunk<D, T, R> {
    fn record_count(&self) -> i64 {
        i64::try_from(self.0.borrow().len()).expect("record count fits i64")
    }
}

impl<D: Columnar, T: Columnar, R: Columnar> ColumnChunk<D, T, R> {
    /// Wrap a sorted, consolidated, non-empty column as a chunk.
    pub fn from_column(column: Column<(D, T, R)>) -> Self {
        debug_assert!(column.borrow().len() > 0, "chunks must be non-empty");
        ColumnChunk(Rc::new(column))
    }

    /// The body as an owned column. A shared body is copied.
    pub fn into_column(self) -> Column<(D, T, R)> {
        Rc::try_unwrap(self.0).unwrap_or_else(|shared| copy_column(&shared))
    }

    /// Borrow the body's columns.
    pub fn borrow(&self) -> BorrowedOf<'_, (D, T, R)> {
        self.0.borrow()
    }

    /// The first and last data items.
    fn data_span(&self) -> (columnar::Ref<'_, D>, columnar::Ref<'_, D>) {
        let data = self.0.borrow().0;
        (data.get(0), data.get(data.len() - 1))
    }

    /// Commit a non-empty column: the designated spill point. Bodies stay
    /// resident for now; a pager-backed variant would page the column here
    /// and retain resident fence metadata for [`UnloadChunk::locate`].
    fn commit(column: Column<(D, T, R)>) -> Self {
        Self::from_column(column)
    }
}

/// Copy a column into a fresh `Typed` column via bulk per-leaf extension.
fn copy_column<C: Columnar>(column: &Column<C>) -> Column<C> {
    let view = column.borrow();
    let mut fresh = C::Container::default();
    fresh.extend_from_self(view, 0..view.len());
    Column::Typed(fresh)
}

/// A column is `Typed`, or becomes one by copy. Merge and settle accumulate
/// into `Typed` targets; serialized variants arrive from remote channels.
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
    for<'a> columnar::Ref<'a, R>: Ord,
{
    type Time = T;

    /// A record-count ceiling for harness bookkeeping. Actual chunk sizing is
    /// by serialized bytes: `merge` and `extract` cut output at the [`Column`]
    /// ship threshold, and `settle` grades by [`at_commit_size`], so this
    /// bound is rarely the binding one.
    const TARGET: usize = 65536;

    fn len(&self) -> usize {
        self.0.borrow().len()
    }

    /// Merge the two fronts to completion through the chain-level merger
    /// ([`merge_chains`]): gallop bulk-copies for disjoint runs, semigroup
    /// consolidation on equal `(data, time)`, output cut at the ship
    /// threshold. Draining both deques merges through the horizon trivially
    /// (the horizon is the end of both), and the harness invokes this on
    /// bounded bursts, so a full drain is bounded work.
    ///
    /// Fronts whose data ranges are disjoint never merge at all: the fence
    /// entries decide, and the lower front moves to the output verbatim.
    fn merge(in1: &mut VecDeque<Self>, in2: &mut VecDeque<Self>, out: &mut VecDeque<Self>) {
        // Disjoint fast path: when one front lies strictly below the other's
        // first data item (equal boundary data could still interleave on
        // time), the merged prefix through the shared horizon is exactly that
        // front, unchanged.
        let low_side = {
            let (a_first, a_last) = in1
                .front()
                .expect("caller guarantees non-empty input")
                .data_span();
            let (b_first, b_last) = in2
                .front()
                .expect("caller guarantees non-empty input")
                .data_span();
            if rr::<D>(a_last) < rr::<D>(b_first) {
                Some(true)
            } else if rr::<D>(b_last) < rr::<D>(a_first) {
                Some(false)
            } else {
                None
            }
        };
        match low_side {
            Some(true) => {
                out.push_back(in1.pop_front().expect("front observed above"));
                return;
            }
            Some(false) => {
                out.push_back(in2.pop_front().expect("front observed above"));
                return;
            }
            None => {}
        }

        // Route through the chain merger with a pager that keeps everything
        // resident: the pager here is inert plumbing, not a spill decision.
        let pager = ColumnPager::disabled();
        let queue = |chunks: &mut VecDeque<Self>| {
            chunks
                .drain(..)
                .map(|chunk| pager.page(&mut chunk.into_column()))
                .collect::<VecDeque<_>>()
        };
        let (q1, q2) = (queue(in1), queue(in2));
        let mut stash = Vec::new();
        let sink_pager = pager.clone();
        merge_chains(
            FetchIter::new(q1, &pager),
            FetchIter::new(q2, &pager),
            |paged| {
                let column = sink_pager.take(paged);
                if column.borrow().len() > 0 {
                    out.push_back(ColumnChunk::from_column(column));
                }
            },
            &mut stash,
        );
    }

    /// Partition the input by `frontier` through the chain-level extractor
    /// ([`extract_chain`]), folding kept times into `residual`; output is cut
    /// at the ship threshold.
    fn extract(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<T>,
        residual: &mut timely::progress::Antichain<T>,
        keep: &mut VecDeque<Self>,
        ship: &mut VecDeque<Self>,
    ) {
        let pager = ColumnPager::disabled();
        let queue = input
            .drain(..)
            .map(|chunk| pager.page(&mut chunk.into_column()))
            .collect::<VecDeque<_>>();
        let mut stash = Vec::new();
        let (ship_pager, keep_pager) = (pager.clone(), pager.clone());
        extract_chain(
            FetchIter::new(queue, &pager),
            frontier,
            residual,
            |paged| {
                let column = ship_pager.take(paged);
                if column.borrow().len() > 0 {
                    ship.push_back(ColumnChunk::from_column(column));
                }
            },
            |paged| {
                let column = keep_pager.take(paged);
                if column.borrow().len() > 0 {
                    keep.push_back(ColumnChunk::from_column(column));
                }
            },
            &mut stash,
        );
    }

    /// Advance times by `frontier` and consolidate, withholding the trailing
    /// `D` group as the carry unless `done` (its updates may continue in input
    /// this call has not seen).
    ///
    /// The input concatenates into the carry's container, so a group that
    /// grows across many calls is appended to, not rebuilt — each record is
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
        // Concatenate the input into one column, reusing the front chunk's
        // storage when it is exclusively owned (the usual case: it is last
        // call's carry).
        let mut base = to_typed(front.into_column());
        {
            let Column::Typed(base_c) = &mut base else {
                unreachable!("to_typed returns Typed");
            };
            for chunk in input.drain(..) {
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
        // is provably complete; unless `done`, push it all back as the carry.
        if !done && data.get(0) == data.get(total - 1) {
            input.push_front(ColumnChunk(Rc::new(base)));
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
        // Cut output at the ship threshold, checked amortized by emitted
        // records (the size test walks the container's leaves, so probing it
        // per record would be quadratic). Records, not groups: a single group
        // may carry arbitrarily many advanced times, and a cut is legal
        // anywhere in the sorted sequence.
        const CUT_CHECK_RECORDS: usize = 1024;
        let mut records_since_check = 0usize;
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
                        if at_serialized_capacity(&result.borrow()) {
                            out.push_back(ColumnChunk(Rc::new(Column::Typed(std::mem::take(
                                &mut result,
                            )))));
                        }
                    }
                }
            }
        }
        if result.borrow().len() > 0 {
            out.push_back(ColumnChunk(Rc::new(Column::Typed(result))));
        }

        // Rebuild the withheld trailing group as the carry.
        if end < total {
            let mut carry = <(D, T, R) as Columnar>::Container::default();
            carry.extend_from_self(view, end..total);
            input.push_front(ColumnChunk(Rc::new(Column::Typed(carry))));
        }
    }

    /// Grade by serialized bytes and commit: chunks at the commit size commit
    /// as they are, and smaller neighbors coalesce until the accumulation
    /// reaches it. Committing is the designated spill hook (see
    /// [`ColumnChunk::commit`]). A sub-threshold tail is withheld as the
    /// carry unless `done`.
    fn settle(input: &mut VecDeque<Self>, done: bool, out: &mut VecDeque<Self>) {
        let mut carry: Option<Column<(D, T, R)>> = None;
        while let Some(chunk) = input.pop_front() {
            let rc = chunk.0;
            let full = at_commit_size(&rc);
            match carry.take() {
                None if full => {
                    let col = Rc::try_unwrap(rc).unwrap_or_else(|rc| copy_column(&rc));
                    out.push_back(ColumnChunk::commit(col));
                }
                None => {
                    let col = Rc::try_unwrap(rc).unwrap_or_else(|rc| copy_column(&rc));
                    carry = Some(to_typed(col));
                }
                Some(acc) if full => {
                    out.push_back(ColumnChunk::commit(acc));
                    let col = Rc::try_unwrap(rc).unwrap_or_else(|rc| copy_column(&rc));
                    out.push_back(ColumnChunk::commit(col));
                }
                Some(mut acc) => {
                    let Column::Typed(acc_c) = &mut acc else {
                        unreachable!("carry is always Typed");
                    };
                    let view = rc.borrow();
                    acc_c.extend_from_self(view, 0..view.len());
                    if at_commit_size(&acc) {
                        out.push_back(ColumnChunk::commit(acc));
                    } else {
                        carry = Some(acc);
                    }
                }
            }
        }
        if let Some(col) = carry {
            if done {
                out.push_back(ColumnChunk::commit(col));
            } else {
                input.push_front(ColumnChunk(Rc::new(col)));
            }
        }
    }
}

impl<K, V, T, R> ColumnChunk<(K, V), T, R>
where
    K: Columnar,
    V: Columnar,
    T: Columnar,
    R: Columnar,
    for<'a> columnar::Ref<'a, K>: Copy + Ord,
{
    /// The first and last key.
    fn key_span(&self) -> (columnar::Ref<'_, K>, columnar::Ref<'_, K>) {
        let keys = self.0.borrow().0.0;
        (keys.get(0), keys.get(keys.len() - 1))
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
        if rr::<K>(probe) > rr::<K>(last) {
            return;
        }
        pos = gallop(len, pos, |i| rr::<K>(keys.get(i)) < rr::<K>(probe));
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
    for<'a> columnar::Ref<'a, R>: Ord,
{
    /// The flat columnar accumulation; appends are bulk column-range copies,
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
        let (first, last) = self.key_span();
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
        extract_view_into::<K, V, T, R>(self.0.borrow(), probes, probe_index, staging);
    }

    fn fetch_into(&self, staging: &mut Self::Staging) {
        let view = self.0.borrow();
        staging.extend_from_self(view, 0..view.len());
    }
}

/// A batch builder over [`ColumnChunk`] input that delegates to a builder
/// over [`Column`] input, unwrapping each chunk's body as it is pushed.
///
/// This is the adapter that lets a [`ChunkBatcher`] feed the existing
/// column-input batch builders (and through them the existing spine layouts).
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

    fn new() -> Self {
        Self {
            inner: Bu::new(),
            _marker: std::marker::PhantomData,
        }
    }

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
        let mut columns: Vec<Column<(D, T, R)>> =
            chain.drain(..).map(ColumnChunk::into_column).collect();
        Bu::seal(&mut columns, description)
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

    use differential_dataflow::trace::Batcher;
    use differential_dataflow::trace::chunk::{ChunkBatch, ChunkBatcher};
    use proptest::prelude::*;
    use timely::container::PushInto;
    use timely::progress::Antichain;

    use crate::columnar::unload::UnloadBatch;

    use super::*;

    type Tuple = ((u64, u64), u64, i64);
    type TestChunk = ColumnChunk<(u64, u64), u64, i64>;

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
        /// equals the reference filter, straddled keys included.
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn unload_extract_matches_filter(
            input in arb_consolidated(),
            cuts in prop::collection::vec(0usize..7, 0..8),
            probe_keys in prop::collection::btree_set(0u64..6, 0..6),
        ) {
            prop_assume!(!input.is_empty());
            let chunks: Vec<TestChunk> = chunked(&input, &cuts).into();
            let description = differential_dataflow::trace::Description::new(
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

            let staged: Column<Tuple> = Column::Typed(staging);
            let got: Vec<Tuple> = staged
                .borrow()
                .into_index_iter()
                .map(|((k, v), t, r)| {
                    (
                        (u64::into_owned(k), u64::into_owned(v)),
                        u64::into_owned(t),
                        i64::into_owned(r),
                    )
                })
                .collect();
            let want: Vec<Tuple> = input
                .iter()
                .copied()
                .filter(|((k, _), _, _)| probe_keys.contains(k))
                .collect();
            prop_assert_eq!(got, want);

            // The scan path reproduces the batch exactly.
            let mut staging = <Tuple as Columnar>::Container::default();
            batch.fetch_into(&mut staging);
            let staged: Column<Tuple> = Column::Typed(staging);
            let got: Vec<Tuple> = staged
                .borrow()
                .into_index_iter()
                .map(|((k, v), t, r)| {
                    (
                        (u64::into_owned(k), u64::into_owned(v)),
                        u64::into_owned(t),
                        i64::into_owned(r),
                    )
                })
                .collect();
            prop_assert_eq!(got, input);
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
}
