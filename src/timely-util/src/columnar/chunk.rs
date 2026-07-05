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
//!   count resident. The pool owns residency from there — slots under a memory
//!   budget, compression and device pageout under pressure — and a body that
//!   dies before pressure reaches it is freed without I/O.
//!
//! Reads of a spilled body are scoped to the call that needs them: the body is
//! pinned, copied out, and the pin released before the method returns. No
//! borrow of chunk contents crosses the trait boundary, which is what lets the
//! pool evict freely between calls.
//!
//! Spilling happens in [`Chunk::settle`], the trait's designated commit point:
//! chunks moved to settled output are handed to the pool when spilling is
//! enabled (see [`set_spill_enabled`]). Grading is by serialized bytes — the
//! ship threshold [`Column`] already uses — rather than by the record-count
//! `TARGET`, since record count does not bound bytes for variable-width data.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};

use columnar::{Borrow, Columnar, Container as _, Index, Len, Push as _};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::chunk::Chunk;
use mz_ore::pool::{ChunkHandle, Pool};
use timely::dataflow::channels::ContainerBytes;
use timely::progress::Timestamp;
use timely::progress::frontier::AntichainRef;

use crate::columnar::{Column, at_serialized_capacity};

/// Whether committed chunks spill to the process pool.
static SPILL_ENABLED: AtomicBool = AtomicBool::new(false);

thread_local! {
    /// A thread-scoped pool override, taking precedence over the global
    /// enable flag and pool. Lets tests and benches spill through a private
    /// pool without touching process-global state.
    static SPILL_OVERRIDE: RefCell<Option<Pool>> = const { RefCell::new(None) };
}

/// Enable or disable spilling of committed chunks to the process pool.
///
/// Takes effect at the next `settle`; already-spilled chunks are unaffected
/// either way. The pool itself is the one installed by
/// [`crate::column_pager::apply_pool_config`]; with no pool installed, chunks
/// stay resident regardless of this flag.
pub fn set_spill_enabled(enabled: bool) {
    SPILL_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Route this thread's chunk spills through `pool` (or back to the global
/// resolution with `None`).
pub fn set_spill_override(pool: Option<Pool>) {
    SPILL_OVERRIDE.with(|cell| *cell.borrow_mut() = pool);
}

/// The pool committed chunks spill to, if any.
fn spill_pool() -> Option<Pool> {
    if let Some(pool) = SPILL_OVERRIDE.with(|cell| cell.borrow().clone()) {
        return Some(pool);
    }
    if SPILL_ENABLED.load(Ordering::Relaxed) {
        crate::column_pager::global_pool()
    } else {
        None
    }
}

/// Bodies smaller than this stay resident: the pool's smallest size class is
/// 64 KiB, so spilling below it trades no meaningful memory for slot waste.
const SPILL_MIN_BYTES: usize = 64 << 10;

/// A spilled chunk body: the serialized column in the pool, plus the resident
/// metadata every [`Chunk`] must answer without fetching.
pub struct SpilledBody {
    /// Number of updates in the body.
    records: usize,
    /// The pool chunk holding the serialized column.
    handle: ChunkHandle,
}

/// A sorted, consolidated run of `(D, T, R)` updates, resident or spilled.
pub enum ColumnChunk<C: Columnar> {
    /// Body on the heap, shared via `Rc`.
    Resident(Rc<Column<C>>),
    /// Body in the pool; see [`SpilledBody`].
    Spilled(Rc<SpilledBody>),
}

impl<C: Columnar> Clone for ColumnChunk<C> {
    fn clone(&self) -> Self {
        match self {
            ColumnChunk::Resident(col) => ColumnChunk::Resident(Rc::clone(col)),
            ColumnChunk::Spilled(body) => ColumnChunk::Spilled(Rc::clone(body)),
        }
    }
}

impl<C: Columnar> Default for ColumnChunk<C> {
    fn default() -> Self {
        ColumnChunk::Resident(Rc::new(Column::default()))
    }
}

impl<C: Columnar> ColumnChunk<C> {
    /// Wrap a sorted, consolidated, non-empty column as a resident chunk.
    pub fn from_column(column: Column<C>) -> Self {
        debug_assert!(column.borrow().len() > 0, "chunks must be non-empty");
        ColumnChunk::Resident(Rc::new(column))
    }

    /// The body as an owned column. A spilled body is pinned, copied out, and
    /// unpinned within this call; a shared resident body is copied.
    pub fn into_column(self) -> Column<C> {
        match self {
            ColumnChunk::Resident(col) => {
                Rc::try_unwrap(col).unwrap_or_else(|shared| copy_column(&shared))
            }
            ColumnChunk::Spilled(body) => {
                let pin = body.handle.pin();
                let mut words = Vec::with_capacity(pin.len());
                words.extend_from_slice(&pin);
                Column::Align(words)
            }
        }
    }

    /// True when the body lives in the pool.
    pub fn is_spilled(&self) -> bool {
        matches!(self, ColumnChunk::Spilled(_))
    }

    /// The number of updates, from resident state only.
    fn records(&self) -> usize {
        match self {
            ColumnChunk::Resident(col) => col.borrow().len(),
            ColumnChunk::Spilled(body) => body.records,
        }
    }

    /// Commit a non-empty column: spill it to the pool when spilling is on and
    /// the body is worth a slot, else keep it resident.
    fn commit(column: Column<C>) -> Self {
        debug_assert!(column.borrow().len() > 0, "chunks must be non-empty");
        if let Some(pool) = spill_pool() {
            let len_bytes = column.length_in_bytes();
            if len_bytes >= SPILL_MIN_BYTES {
                let records = column.borrow().len();
                let handle = spill_column(column, &pool, len_bytes);
                return ColumnChunk::Spilled(Rc::new(SpilledBody { records, handle }));
            }
        }
        ColumnChunk::Resident(Rc::new(column))
    }
}

/// Copy a column into a fresh `Typed` column via bulk per-leaf extension.
fn copy_column<C: Columnar>(column: &Column<C>) -> Column<C> {
    let view = column.borrow();
    let mut fresh = C::Container::default();
    fresh.extend_from_self(view, 0..view.len());
    Column::Typed(fresh)
}

/// Serialize a column into a pool slot. The `Align` variant is already the
/// serialized form and copies in directly; other variants write their
/// [`ContainerBytes`] encoding through a cursor over the slot memory. Sizing
/// is exact, so a short or overlong write is a contract violation and panics.
fn spill_column<C: Columnar>(column: Column<C>, pool: &Pool, len_bytes: usize) -> ChunkHandle {
    debug_assert_eq!(len_bytes % 8, 0);
    match column {
        Column::Align(words) => pool.insert_with(words.len(), |dst| dst.copy_from_slice(&words)),
        other => pool.insert_with(len_bytes / 8, |dst| {
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
/// into `Typed` targets; serialized variants arrive from spill fetches and
/// remote channels.
fn to_typed<C: Columnar>(column: Column<C>) -> Column<C> {
    match column {
        typed @ Column::Typed(_) => typed,
        other => copy_column(&other),
    }
}

impl<D, T, R> Chunk for ColumnChunk<(D, T, R)>
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
    /// ship threshold, and `settle` grades by the same measure, so this bound
    /// is rarely the binding one.
    const TARGET: usize = 65536;

    fn len(&self) -> usize {
        self.records()
    }

    /// Merge the two fronts through their shared horizon with
    /// [`Column::merge_from`]: gallop bulk-copies for disjoint runs, semigroup
    /// consolidation on equal `(data, time)`, output cut at the ship
    /// threshold. The exhausted front retires; the survivor's remainder is
    /// rewritten and pushed back for the next call.
    fn merge(in1: &mut VecDeque<Self>, in2: &mut VecDeque<Self>, out: &mut VecDeque<Self>) {
        let left = in1.pop_front().expect("caller guarantees non-empty input");
        let right = in2.pop_front().expect("caller guarantees non-empty input");
        let mut cols = [left.into_column(), right.into_column()];
        let mut positions = [0usize, 0usize];
        loop {
            let mut result: Column<(D, T, R)> = Column::default();
            let yielded = result.merge_from(&mut cols, &mut positions);
            if result.borrow().len() > 0 {
                out.push_back(ColumnChunk::Resident(Rc::new(result)));
            }
            if !yielded {
                break;
            }
        }
        for (col, pos, queue) in [(&cols[0], positions[0], in1), (&cols[1], positions[1], in2)] {
            let view = col.borrow();
            if pos < view.len() {
                let mut rest = <(D, T, R) as Columnar>::Container::default();
                rest.extend_from_self(view, pos..view.len());
                queue.push_front(ColumnChunk::Resident(Rc::new(Column::Typed(rest))));
            }
        }
    }

    /// Partition one front chunk by `frontier`, folding kept times into
    /// `residual`. One chunk per call, so the harness settles both sides
    /// between chunks; output is cut at the ship threshold.
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
        let mut col = chunk.into_column();
        let len = col.borrow().len();
        let mut pos = 0;
        let mut keep_col: Column<(D, T, R)> = Column::default();
        let mut ship_col: Column<(D, T, R)> = Column::default();
        while pos < len {
            col.extract(&mut pos, frontier, residual, &mut keep_col, &mut ship_col);
            if pos < len {
                if at_serialized_capacity(&keep_col.borrow()) {
                    keep.push_back(ColumnChunk::Resident(Rc::new(std::mem::take(
                        &mut keep_col,
                    ))));
                }
                if at_serialized_capacity(&ship_col.borrow()) {
                    ship.push_back(ColumnChunk::Resident(Rc::new(std::mem::take(
                        &mut ship_col,
                    ))));
                }
            }
        }
        if keep_col.borrow().len() > 0 {
            keep.push_back(ColumnChunk::Resident(Rc::new(keep_col)));
        }
        if ship_col.borrow().len() > 0 {
            ship.push_back(ColumnChunk::Resident(Rc::new(ship_col)));
        }
    }

    /// Advance times by `frontier` and consolidate, withholding the trailing
    /// `D` group as the carry unless `done` (its updates may continue in input
    /// this call has not seen).
    ///
    /// Advancing is lattice-monotone but not order-monotone, so each group's
    /// advanced times are re-sorted before adjacent equal times fold. The
    /// call materializes its whole input, which the fueled batch merger
    /// bounds by feeding a small burst of chunks per call.
    fn advance(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<T>,
        done: bool,
        out: &mut VecDeque<Self>,
    ) {
        if input.is_empty() {
            return;
        }
        let loaded: Vec<Column<(D, T, R)>> =
            input.drain(..).map(ColumnChunk::into_column).collect();
        let views: Vec<_> = loaded.iter().map(|col| col.borrow()).collect();
        let counts: Vec<usize> = views.iter().map(|view| view.len()).collect();
        let total: usize = counts.iter().sum();
        if total == 0 {
            return;
        }

        let get = |index: usize| {
            let mut index = index;
            for (view, count) in views.iter().zip(&counts) {
                if index < *count {
                    return view.get(index);
                }
                index -= count;
            }
            unreachable!("index within total")
        };

        // Giant-group early-out: if the whole input is one `D` group, nothing
        // is provably complete; unless `done`, push it all back as the carry.
        let (first_d, _, _) = get(0);
        let (last_d, _, _) = get(total - 1);
        if !done && first_d == last_d {
            let mut all = <(D, T, R) as Columnar>::Container::default();
            for view in &views {
                all.extend_from_self(*view, 0..view.len());
            }
            input.push_front(ColumnChunk::Resident(Rc::new(Column::Typed(all))));
            return;
        }

        // The processing bound: everything, or everything before the trailing
        // `D` group when it must be withheld.
        let end = if done {
            total
        } else {
            let mut end = total - 1;
            while end > 0 {
                let (d, _, _) = get(end - 1);
                if d == last_d { end -= 1 } else { break }
            }
            end
        };

        let mut result = <(D, T, R) as Columnar>::Container::default();
        let (mut rd, mut rt, mut rr);
        // Per-group scratch: advanced owned times with owned diffs.
        let mut scratch: Vec<(T, R)> = Vec::new();
        let mut index = 0;
        while index < end {
            let (group_d, _, _) = get(index);
            scratch.clear();
            while index < end {
                let (d, t, r) = get(index);
                if d != group_d {
                    break;
                }
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
                    (rd, rt, rr) = (&mut result.0, &mut result.1, &mut result.2);
                    rd.push(group_d);
                    rt.push(&t);
                    rr.push(&r);
                }
            }
            if at_serialized_capacity(&result.borrow()) {
                out.push_back(ColumnChunk::Resident(Rc::new(Column::Typed(
                    std::mem::take(&mut result),
                ))));
            }
        }
        if result.borrow().len() > 0 {
            out.push_back(ColumnChunk::Resident(Rc::new(Column::Typed(result))));
        }

        // Rebuild the withheld trailing group as the carry.
        if end < total {
            let mut carry = <(D, T, R) as Columnar>::Container::default();
            let mut remaining = end;
            for (view, count) in views.iter().zip(&counts) {
                if remaining >= *count {
                    remaining -= count;
                    continue;
                }
                carry.extend_from_self(*view, remaining..*count);
                remaining = 0;
            }
            input.push_front(ColumnChunk::Resident(Rc::new(Column::Typed(carry))));
        }
    }

    /// Grade by serialized bytes and commit: spilled chunks pass through
    /// untouched, resident chunks at the ship threshold commit as they are,
    /// and smaller neighbors coalesce until the accumulation reaches the
    /// threshold. Committing is the spill hook (see [`ColumnChunk::commit`]).
    /// A sub-threshold tail is withheld as the carry unless `done`.
    fn settle(input: &mut VecDeque<Self>, done: bool, out: &mut VecDeque<Self>) {
        let mut carry: Option<Column<(D, T, R)>> = None;
        while let Some(chunk) = input.pop_front() {
            match chunk {
                spilled @ ColumnChunk::Spilled(_) => {
                    if let Some(col) = carry.take() {
                        out.push_back(ColumnChunk::commit(col));
                    }
                    out.push_back(spilled);
                }
                ColumnChunk::Resident(rc) => {
                    let full = at_serialized_capacity(&rc.borrow());
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
                            if at_serialized_capacity(&acc.borrow()) {
                                out.push_back(ColumnChunk::commit(acc));
                            } else {
                                carry = Some(acc);
                            }
                        }
                    }
                }
            }
        }
        if let Some(col) = carry {
            if done {
                out.push_back(ColumnChunk::commit(col));
            } else {
                input.push_front(ColumnChunk::Resident(Rc::new(col)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Property tests for the [`Chunk`] contract on [`ColumnChunk`].
    //!
    //! Strategy: generate sorted+consolidated inputs (the chunk invariant),
    //! drive the trait methods the way the differential harness does, and
    //! compare against brute-force references on owned tuples. Test types are
    //! `D = (u64, u64)`, `T = u64`, `R = i64` from small ranges so equal-key
    //! collisions are common and consolidation actually runs.

    use differential_dataflow::trace::Batcher;
    use differential_dataflow::trace::chunk::ChunkBatcher;
    use mz_ore::pool::{Pool, PoolConfig};
    use proptest::prelude::*;
    use timely::container::PushInto;
    use timely::progress::Antichain;

    use super::*;

    type Tuple = ((u64, u64), u64, i64);
    type TestChunk = ColumnChunk<Tuple>;

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

    fn test_pool() -> Pool {
        Pool::new(PoolConfig::default()).expect("pool creation")
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
    }

    /// A body large enough to spill round-trips through the pool with resident
    /// metadata intact, and the batcher produces spilled sealed output.
    #[mz_ore::test]
    fn spill_round_trip() {
        set_spill_override(Some(test_pool()));

        let data: Vec<Tuple> = (0..40_000u64)
            .map(|i| ((i / 4, i % 4), i % 8, 1i64))
            .collect();
        let data = consolidate(data);

        let column = build_column(&data);
        let committed = TestChunk::commit(column);
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
    /// correctly; sub-threshold chunks stay resident.
    #[mz_ore::test]
    fn merge_spilled_chains() {
        set_spill_override(Some(test_pool()));

        let a: Vec<Tuple> = (0..20_000u64).map(|i| ((i, 0), 0, 1i64)).collect();
        let b: Vec<Tuple> = (0..20_000u64).map(|i| ((i, 0), 0, 2i64)).collect();

        let mut in1 = VecDeque::from([TestChunk::commit(build_column(&a))]);
        let mut in2 = VecDeque::from([TestChunk::commit(build_column(&b))]);
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

    /// A tiny chunk stays resident regardless of the spill gate.
    #[mz_ore::test]
    fn small_chunks_stay_resident() {
        set_spill_override(Some(test_pool()));
        let committed = TestChunk::commit(build_column(&[((1, 1), 0, 1)]));
        assert!(!committed.is_spilled());
        set_spill_override(None);
    }
}
