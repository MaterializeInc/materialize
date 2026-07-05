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
//!   from there — slots under a memory budget, compression and device pageout
//!   under pressure — and a body that dies before pressure reaches it is freed
//!   without I/O.
//!
//! Reads of a spilled body are copy-out and scoped to the call that needs
//! them: the body is read into caller-owned memory and no reference into pool
//! memory ever exists outside the pool. That contract is what lets the pool
//! evict with no reader accounting at all.
//!
//! Spilling happens in [`Chunk::settle`], the trait's designated commit point:
//! chunks moved to settled output are handed to the pool when spilling is
//! enabled (see [`set_spill_enabled`]). Grading is by serialized bytes — the
//! ship size [`Column`] already targets — rather than by the record-count
//! `TARGET`, since record count does not bound bytes for variable-width data.
//!
//! Chunks whose data is a `(key, val)` pair additionally implement
//! [`UnloadChunk`], the bulk-read capability: sorted probe keys in, matching
//! updates appended to caller-owned staging, with `locate` answered from the
//! resident fence metadata so a probe set faults only the chunk bodies it
//! actually touches.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};

use columnar::bytes::indexed;
use columnar::{Borrow, BorrowedOf, Columnar, Container as _, FromBytes, Index, Len, Push as _};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::chunk::{Chunk, UnloadChunk};
use mz_ore::cast::CastFrom;
use mz_ore::pool::{ChunkHandle, Pool};
use timely::Accountable;
use timely::container::{ContainerBuilder, PushInto};
use timely::dataflow::channels::ContainerBytes;
use timely::progress::Timestamp;
use timely::progress::frontier::AntichainRef;

use crate::columnar::batcher::ColumnChunker;
use crate::columnar::{Column, at_serialized_capacity};

/// Whether committed chunks spill to the process pool.
static SPILL_ENABLED: AtomicBool = AtomicBool::new(false);

thread_local! {
    /// A thread-scoped pool override, taking precedence over the global
    /// enable flag and pool. Lets tests and benches spill through a private
    /// pool without touching process-global state.
    static SPILL_OVERRIDE: RefCell<Option<Pool>> = const { RefCell::new(None) };

    /// Reusable staging for call-scoped reads of spilled bodies.
    static READ_SCRATCH: RefCell<Vec<u64>> = const { RefCell::new(Vec::new()) };
}

/// Enable or disable spilling of committed chunks to the process pool.
///
/// Takes effect at the next `settle`; already-spilled chunks are unaffected
/// either way. The pool is resolved per commit through
/// [`crate::column_pager::active_pool`], so chunks spill only when pool mode
/// is the process's active shared mechanism; under the tiered mechanism (or
/// with no pool installed) chunks stay resident regardless of this flag.
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
        crate::column_pager::active_pool()
    } else {
        None
    }
}

/// Run `f` over this thread's read scratch, cleared of any previous use.
fn with_scratch<Out>(f: impl FnOnce(&mut Vec<u64>) -> Out) -> Out {
    READ_SCRATCH.with(|cell| {
        let mut scratch = cell.take();
        let out = f(&mut scratch);
        cell.replace(scratch);
        out
    })
}

/// The serialized-byte size committed chunks aim for, matching the ship size
/// of the columnar merge machinery.
const COMMIT_BYTES: usize = 2 << 20;

/// Bodies smaller than this stay resident: the pool's smallest size class is
/// 64 KiB, so spilling below it trades no meaningful memory for slot waste.
const SPILL_MIN_BYTES: usize = 64 << 10;

/// Whether a column is big enough to commit on its own. A monotone
/// threshold, deliberately not the periodic window of
/// [`at_serialized_capacity`]: settle grows its carry by whole chunks, which
/// can step over any fixed-width window, so the carry-full test must be
/// monotone in size or the carry grows far past the target.
fn at_commit_size<C: Columnar>(column: &Column<C>) -> bool {
    column.length_in_bytes() >= COMMIT_BYTES - COMMIT_BYTES / 10
}

/// Reconstructs the borrowed columnar view from serialized words, the same
/// zero-copy decode [`Column::borrow`] performs on its `Align` variant.
fn borrow_words<C: Columnar>(words: &[u64]) -> BorrowedOf<'_, C> {
    <BorrowedOf<'_, C>>::from_bytes(&mut indexed::decode(words))
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

/// A spilled chunk body: the serialized column in the pool, plus the resident
/// metadata every [`Chunk`] must answer without fetching — the record count
/// and the first and last data items as singleton containers (the fence
/// entries `UnloadChunk::locate` consults).
pub struct SpilledBody<D: Columnar> {
    /// Number of updates in the body.
    records: usize,
    /// The first data item, as a one-element container.
    first: D::Container,
    /// The last data item, as a one-element container.
    last: D::Container,
    /// The pool chunk holding the serialized column.
    handle: ChunkHandle,
}

/// A sorted, consolidated run of `(D, T, R)` updates, resident or spilled.
pub enum ColumnChunk<D: Columnar, T: Columnar, R: Columnar> {
    /// Body on the heap, shared via `Rc`.
    Resident(Rc<Column<(D, T, R)>>),
    /// Body in the pool; see [`SpilledBody`].
    Spilled(Rc<SpilledBody<D>>),
}

impl<D: Columnar, T: Columnar, R: Columnar> Clone for ColumnChunk<D, T, R> {
    fn clone(&self) -> Self {
        match self {
            ColumnChunk::Resident(col) => ColumnChunk::Resident(Rc::clone(col)),
            ColumnChunk::Spilled(body) => ColumnChunk::Spilled(Rc::clone(body)),
        }
    }
}

impl<D: Columnar, T: Columnar, R: Columnar> Default for ColumnChunk<D, T, R> {
    fn default() -> Self {
        ColumnChunk::Resident(Rc::new(Column::default()))
    }
}

impl<D: Columnar, T: Columnar, R: Columnar> Accountable for ColumnChunk<D, T, R> {
    fn record_count(&self) -> i64 {
        i64::try_from(self.records()).expect("record count fits i64")
    }
}

impl<D: Columnar, T: Columnar, R: Columnar> ColumnChunk<D, T, R> {
    /// Wrap a sorted, consolidated, non-empty column as a resident chunk.
    pub fn from_column(column: Column<(D, T, R)>) -> Self {
        debug_assert!(column.borrow().len() > 0, "chunks must be non-empty");
        ColumnChunk::Resident(Rc::new(column))
    }

    /// The body as an owned column. A spilled body is copied out of the pool
    /// within this call; a shared resident body is copied.
    pub fn into_column(self) -> Column<(D, T, R)> {
        match self {
            ColumnChunk::Resident(col) => {
                Rc::try_unwrap(col).unwrap_or_else(|shared| copy_column(&shared))
            }
            ColumnChunk::Spilled(body) => {
                let mut words = Vec::new();
                body.handle.read_into(&mut words);
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
    fn commit(column: Column<(D, T, R)>) -> Self {
        debug_assert!(column.borrow().len() > 0, "chunks must be non-empty");
        if let Some(pool) = spill_pool() {
            let len_bytes = column.length_in_bytes();
            if len_bytes >= SPILL_MIN_BYTES {
                let view = column.borrow();
                let records = view.len();
                let mut first = D::Container::default();
                let mut last = D::Container::default();
                first.push(view.0.get(0));
                last.push(view.0.get(records - 1));
                let handle = spill_column(column, &pool, len_bytes);
                return ColumnChunk::Spilled(Rc::new(SpilledBody {
                    records,
                    first,
                    last,
                    handle,
                }));
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
/// into `Typed` targets; serialized variants arrive from spill reads and
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
    for<'a> columnar::Ref<'a, R>: Ord,
{
    type Time = T;

    /// A record-count ceiling for harness bookkeeping. Actual chunk sizing is
    /// by serialized bytes: `merge` and `extract` cut output at the [`Column`]
    /// ship threshold, and `settle` grades by [`at_commit_size`], so this
    /// bound is rarely the binding one.
    const TARGET: usize = 65536;

    fn len(&self) -> usize {
        self.records()
    }

    /// Merge the two fronts through their shared horizon with
    /// [`Column::merge_from`]: gallop bulk-copies for disjoint runs, semigroup
    /// consolidation on equal `(data, time)`, output cut at the ship
    /// threshold. The exhausted front retires. A survivor consumed partway is
    /// rewritten and pushed back; a survivor not consumed at all goes back as
    /// it was — in particular a spilled body is neither rebuilt nor re-spilled.
    fn merge(in1: &mut VecDeque<Self>, in2: &mut VecDeque<Self>, out: &mut VecDeque<Self>) {
        let a = in1.pop_front().expect("caller guarantees non-empty input");
        let b = in2.pop_front().expect("caller guarantees non-empty input");
        let mut spill_a = match &a {
            ColumnChunk::Spilled(body) => Some(Rc::clone(body)),
            ColumnChunk::Resident(_) => None,
        };
        let mut spill_b = match &b {
            ColumnChunk::Spilled(body) => Some(Rc::clone(body)),
            ColumnChunk::Resident(_) => None,
        };
        let mut cols = [a.into_column(), b.into_column()];
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
        let [col_a, col_b] = &mut cols;
        for (col, pos, spilled, queue) in [
            (col_a, positions[0], &mut spill_a, in1),
            (col_b, positions[1], &mut spill_b, in2),
        ] {
            let len = col.borrow().len();
            if pos == 0 && len > 0 {
                // Untouched survivor: restore it as it was, spilled bodies
                // included (the loaded copy is dropped).
                let chunk = match spilled.take() {
                    Some(body) => ColumnChunk::Spilled(body),
                    None => ColumnChunk::Resident(Rc::new(std::mem::take(col))),
                };
                queue.push_front(chunk);
            } else if pos < len {
                let view = col.borrow();
                let mut rest = <(D, T, R) as Columnar>::Container::default();
                rest.extend_from_self(view, pos..len);
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
            input.push_front(ColumnChunk::Resident(Rc::new(base)));
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
        let mut groups_since_cut = 0usize;
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
                }
            }
            // Cut output at the commit size, checked amortized: the size test
            // walks the container's leaves, so probing it per group would be
            // quadratic in small-group runs.
            groups_since_cut += 1;
            if groups_since_cut & 31 == 0
                && u64::cast_from(indexed::length_in_words(&result.borrow()))
                    >= u64::cast_from(COMMIT_BYTES / 8)
            {
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
            carry.extend_from_self(view, end..total);
            input.push_front(ColumnChunk::Resident(Rc::new(Column::Typed(carry))));
        }
    }

    /// Grade by serialized bytes and commit: spilled chunks pass through
    /// untouched, resident chunks at the commit size commit as they are, and
    /// smaller neighbors coalesce until the accumulation reaches it.
    /// Committing is the spill hook (see [`ColumnChunk::commit`]). A
    /// sub-threshold tail is withheld as the carry unless `done`.
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

impl<K, V, T, R> ColumnChunk<(K, V), T, R>
where
    K: Columnar,
    V: Columnar,
    T: Columnar,
    R: Columnar,
    for<'a> columnar::Ref<'a, K>: Copy + Ord,
{
    /// The first and last key, from resident state only.
    fn key_span(&self) -> (columnar::Ref<'_, K>, columnar::Ref<'_, K>) {
        match self {
            ColumnChunk::Resident(col) => {
                let keys = col.borrow().0.0;
                (keys.get(0), keys.get(keys.len() - 1))
            }
            ColumnChunk::Spilled(body) => {
                (body.first.borrow().0.get(0), body.last.borrow().0.get(0))
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
        match self {
            ColumnChunk::Resident(col) => {
                extract_view_into::<K, V, T, R>(col.borrow(), probes, probe_index, staging);
            }
            ColumnChunk::Spilled(body) => with_scratch(|scratch| {
                body.handle.read_into(scratch);
                let view = borrow_words::<((K, V), T, R)>(scratch);
                extract_view_into::<K, V, T, R>(view, probes, probe_index, staging);
            }),
        }
    }

    fn fetch_into(&self, staging: &mut Self::Staging) {
        match self {
            ColumnChunk::Resident(col) => {
                let view = col.borrow();
                staging.extend_from_self(view, 0..view.len());
            }
            ColumnChunk::Spilled(body) => with_scratch(|scratch| {
                body.handle.read_into(scratch);
                let view = borrow_words::<((K, V), T, R)>(scratch);
                staging.extend_from_self(view, 0..view.len());
            }),
        }
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
    D: Columnar + Send + Sync + 'static,
    T: Columnar + Send + Sync + 'static,
    R: Columnar + Send + Sync + 'static,
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
    use mz_ore::pool::{Pool, PoolConfig};
    use proptest::prelude::*;
    use timely::container::PushInto;
    use timely::progress::Antichain;

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

    /// The chunked cut, with every chunk force-spilled through a private pool
    /// (bounds captured, bodies in the pool) regardless of size thresholds.
    fn chunked_spilled(data: &[Tuple], cuts: &[usize], pool: &Pool) -> VecDeque<TestChunk> {
        chunked(data, cuts)
            .into_iter()
            .map(|chunk| force_spill(chunk, pool))
            .collect()
    }

    /// Spill one chunk through `pool`, bypassing the size threshold.
    fn force_spill(chunk: TestChunk, pool: &Pool) -> TestChunk {
        let column = chunk.into_column();
        let view = column.borrow();
        let records = view.len();
        let mut first = <(u64, u64) as Columnar>::Container::default();
        let mut last = <(u64, u64) as Columnar>::Container::default();
        first.push(view.0.get(0));
        last.push(view.0.get(records - 1));
        let len_bytes = column.length_in_bytes();
        let handle = spill_column(column, pool, len_bytes);
        ColumnChunk::Spilled(Rc::new(SpilledBody {
            records,
            first,
            last,
            handle,
        }))
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

            let got: Vec<Tuple> = staging
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
            let expected: Vec<Tuple> = input
                .iter()
                .copied()
                .filter(|((k, _), _, _)| probe_keys.contains(k))
                .collect();
            prop_assert_eq!(got, expected);
        }
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
    /// correctly, and an untouched survivor keeps its spilled body.
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

        // `low` is fully consumed; `high` was never touched and must come
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

    /// The settle carry commits at a monotone size threshold rather than the
    /// periodic ship window, so mid-window chunk sizes cannot make it grow
    /// past the target unbounded.
    #[mz_ore::test]
    fn settle_carry_commits_at_target() {
        // ~1.5 MiB per chunk: inside the dead zone of the periodic window
        // check (see `at_commit_size`).
        let chunk_rows = (1_500_000usize / 24) as u64;
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

    /// A tiny chunk stays resident regardless of the spill gate.
    #[mz_ore::test]
    fn small_chunks_stay_resident() {
        set_spill_override(Some(test_pool()));
        let committed = TestChunk::commit(build_column(&[((1, 1), 0, 1)]));
        assert!(!committed.is_spilled());
        set_spill_override(None);
    }
}
