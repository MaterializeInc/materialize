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

//! Merge-batcher for [`Column`] chunks with per-chunk paging.
//!
//! Forks the [`differential_dataflow`] merge-batcher framework so chains can
//! hold [`PagedColumn`] entries — letting the [`ColumnPager`] page chunks
//! out as they're produced and fetch them back lazily during merge / extract.
//!
//! Reuses the resident building blocks from [`super::batcher`]:
//! [`ColumnChunker`] (input consolidation) and the inherent
//! `Column::merge_from` / `Column::extract` methods (per-chunk merge / split).
//!
//! [`differential_dataflow`]: differential_dataflow::trace::implementations::merge_batcher

use std::collections::VecDeque;

use columnar::{Columnar, Index, Len};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::logging::{BatcherEvent, Logger};
use differential_dataflow::trace::{Batcher, Builder, Description};
use timely::Accountable;
use timely::PartialOrder;
use timely::container::{ContainerBuilder, PushInto, SizableContainer};
use timely::dataflow::channels::ContainerBytes;
use timely::progress::Timestamp;
use timely::progress::frontier::{Antichain, AntichainRef};

use crate::column_pager::{self, ColumnPager, PagedColumn};
use crate::columnar::Column;
use crate::columnar::batcher::ColumnChunker;

/// Drives the merge-batcher over [`Column`] chunks routed through a
/// [`ColumnPager`].
///
/// Chains hold [`PagedColumn`] entries rather than resident [`Column`]s, so
/// each insert / merge / extract step can hand its output to the pager and
/// store whatever the policy returns (resident, paged, or compressed). Reads
/// during merge materialize lazily via [`FetchIter`].
///
/// Resolves its pager lazily per call via [`column_pager::global_pager`], so
/// late-arriving dyncfg updates (e.g. `enable_column_paged_batcher` flipping
/// on after the batcher was constructed) take effect without rebuilding the
/// operator. Tests may override that lookup via [`Self::set_pager`].
pub struct ColumnMergeBatcher<D, T, R>
where
    D: Columnar,
    T: Columnar,
    R: Columnar,
{
    chunker: ColumnChunker<(D, T, R)>,
    chains: Vec<VecDeque<PagedColumn<(D, T, R)>>>,
    lower: Antichain<T>,
    frontier: Antichain<T>,
    /// Free-list of empty, capacity-retaining `Column::Typed` buffers reused
    /// across merges and extracts so the merge loop doesn't reallocate a fresh
    /// `Column` for every shipped chunk. Mirrors the `stash` the framework
    /// hands the non-paged `ColumnMerger`; capped at [`STASH_CAP`] so the pool
    /// stays small relative to the paging budget. Chunks paged out to a backend
    /// never reach here — only transient resident merge buffers are recycled.
    stash: Vec<Column<(D, T, R)>>,
    /// Optional override. `None` means "read [`column_pager::global_pager`]
    /// fresh on every use" — the production path, so worker_config dyncfg
    /// changes that re-install the process-global pager take effect on the
    /// very next chunk this batcher processes.
    pager_override: Option<ColumnPager>,
    logger: Option<Logger>,
    operator_id: usize,
}

impl<D, T, R> ColumnMergeBatcher<D, T, R>
where
    D: Columnar,
    T: Columnar,
    R: Columnar,
{
    /// Pin the pager this batcher uses, overriding the thread-local lookup.
    /// Mainly for tests; production should leave the override unset so
    /// dyncfg-driven re-installs take effect immediately.
    pub fn set_pager(&mut self, pager: ColumnPager) {
        self.pager_override = Some(pager);
    }

    /// Current pager — override if set, else the process-global pager
    /// installed by `apply_worker_config`. `ColumnPager` is cheaply
    /// cloneable (Arc inside).
    fn pager(&self) -> ColumnPager {
        self.pager_override
            .clone()
            .unwrap_or_else(column_pager::global_pager)
    }

    /// Push a chain into `self.chains`, emitting a positive `BatcherEvent`
    /// covering its resident entries.
    fn chain_push(&mut self, chain: VecDeque<PagedColumn<(D, T, R)>>) {
        self.emit_account(&chain, 1);
        self.chains.push(chain);
    }

    /// Pop a chain from `self.chains`, emitting a negative `BatcherEvent`
    /// retracting its resident entries.
    ///
    /// Invariant for the retract to reconcile against the matching
    /// `chain_push`: chain entries are never mutated in place between push
    /// and pop. The only allowed mutation is a full pop / push pair (see
    /// `insert_chain` and `merge_by`), so each entry's accounting category
    /// — `Resident` vs `Paged` vs `Compressed` — is the same at both ends.
    /// If a future change ever pages an entry out in place after push, this
    /// path silently double-counts.
    fn chain_pop(&mut self) -> Option<VecDeque<PagedColumn<(D, T, R)>>> {
        let chain = self.chains.pop()?;
        self.emit_account(&chain, -1);
        Some(chain)
    }

    /// Emit a single `BatcherEvent` summing resident accounting across
    /// `chain` with the given sign. No-op when no logger is attached.
    fn emit_account(&self, chain: &VecDeque<PagedColumn<(D, T, R)>>, diff: isize) {
        let Some(logger) = &self.logger else {
            return;
        };
        let (mut records, mut size, mut capacity, mut allocations) =
            (0isize, 0isize, 0isize, 0isize);
        for entry in chain {
            let (r, s, c, a) = account_chunk(entry);
            records = records.saturating_add_unsigned(r);
            size = size.saturating_add_unsigned(s);
            capacity = capacity.saturating_add_unsigned(c);
            allocations = allocations.saturating_add_unsigned(a);
        }
        logger.log(BatcherEvent {
            operator: self.operator_id,
            records_diff: records.saturating_mul(diff),
            size_diff: size.saturating_mul(diff),
            capacity_diff: capacity.saturating_mul(diff),
            allocations_diff: allocations.saturating_mul(diff),
        });
    }
}

impl<D, T, R> Drop for ColumnMergeBatcher<D, T, R>
where
    D: Columnar,
    T: Columnar,
    R: Columnar,
{
    fn drop(&mut self) {
        // Retract accounting for any chains still resident at drop time so
        // the BatcherEvent counters end at zero per-operator.
        while self.chain_pop().is_some() {}
    }
}

/// Resident-only accounting. Returns `(records, size_bytes, capacity_bytes,
/// allocations)` for a single chain entry; paged-out entries contribute 0
/// across the board.
///
/// `BatcherEvent` feeds the `mz_arrangement_batcher_*_raw` introspection
/// tables, which downstream surface as memory-resource dashboards. Bytes
/// living on swap or in a pager file aren't part of RSS and shouldn't be
/// reported there.
fn account_chunk<C: Columnar>(entry: &PagedColumn<C>) -> (usize, usize, usize, usize) {
    match entry {
        PagedColumn::Resident(col, _) => {
            let records = usize::try_from(col.record_count()).expect("non-negative");
            let bytes = col.length_in_bytes();
            (records, bytes, bytes, 1)
        }
        PagedColumn::Paged { .. } | PagedColumn::Compressed { .. } => (0, 0, 0, 0),
    }
}

impl<D, T, R> Batcher for ColumnMergeBatcher<D, T, R>
where
    D: Columnar,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Timestamp + PartialOrder,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>>,
    for<'a> columnar::Ref<'a, R>: Ord,
{
    type Input = Column<(D, T, R)>;
    type Output = Column<(D, T, R)>;
    type Time = T;

    fn new(logger: Option<Logger>, operator_id: usize) -> Self {
        // No pager snapshot taken here — `self.pager()` reads
        // `column_pager::global_pager` per call, so dyncfg-driven re-installs
        // take effect on the next chunk.
        Self {
            chunker: ColumnChunker::default(),
            chains: Vec::new(),
            lower: Antichain::from_elem(T::minimum()),
            frontier: Antichain::new(),
            stash: Vec::new(),
            pager_override: None,
            logger,
            operator_id,
        }
    }

    fn push_container(&mut self, container: &mut Self::Input) {
        let pager = self.pager();
        self.chunker.push_into(container);
        while let Some(chunk) = self.chunker.extract() {
            let paged = pager.page(chunk);
            self.insert_chain(VecDeque::from([paged]));
        }
    }

    fn seal<B: Builder<Input = Self::Output, Time = Self::Time>>(
        &mut self,
        upper: Antichain<Self::Time>,
    ) -> B::Output {
        let pager = self.pager();
        // Finish chunker, fold any tail chunks in.
        while let Some(chunk) = self.chunker.finish() {
            let paged = pager.page(chunk);
            self.insert_chain(VecDeque::from([paged]));
        }

        // Merge all remaining chains into one.
        while self.chains.len() > 1 {
            let a = self.chain_pop().unwrap();
            let b = self.chain_pop().unwrap();
            let merged = self.merge_by(a, b);
            self.chain_push(merged);
        }
        let merged = self.chain_pop().unwrap_or_default();

        // Extract `merged` into `readied` (ship side, materialized for the
        // builder) and `kept_chain` (keep side, stays paged for the next
        // round).
        let mut readied: Vec<Column<(D, T, R)>> = Vec::new();
        let mut kept_chain: VecDeque<PagedColumn<(D, T, R)>> = VecDeque::new();
        self.frontier.clear();
        {
            let pager = &pager;
            let frontier = &mut self.frontier;
            let stash = &mut self.stash;
            extract_chain(
                FetchIter::new(merged, pager),
                upper.borrow(),
                frontier,
                |paged| readied.push(pager.take(paged)),
                |paged| kept_chain.push_back(paged),
                stash,
            );
        }

        if !kept_chain.is_empty() {
            self.chain_push(kept_chain);
        }

        let description = Description::new(
            self.lower.clone(),
            upper.clone(),
            Antichain::from_elem(T::minimum()),
        );
        let seal = B::seal(&mut readied, description);
        self.lower = upper;
        seal
    }

    fn frontier(&mut self) -> AntichainRef<'_, Self::Time> {
        self.frontier.borrow()
    }
}

impl<D, T, R> ColumnMergeBatcher<D, T, R>
where
    D: Columnar,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Clone + PartialOrder,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>>,
{
    /// Insert `chain` and rebalance: while the youngest chain is at least
    /// half the size of its predecessor, merge them.
    fn insert_chain(&mut self, chain: VecDeque<PagedColumn<(D, T, R)>>) {
        if chain.is_empty() {
            return;
        }
        self.chain_push(chain);
        while self.chains.len() > 1
            && self.chains[self.chains.len() - 1].len()
                >= self.chains[self.chains.len() - 2].len() / 2
        {
            let a = self.chain_pop().unwrap();
            let b = self.chain_pop().unwrap();
            let merged = self.merge_by(a, b);
            self.chain_push(merged);
        }
    }

    /// Merge two sorted chains. Outputs are routed through `self.pager.page`
    /// per chunk produced, so the result chain holds `PagedColumn`s and the
    /// caller never sees a fully materialized merge result.
    fn merge_by(
        &mut self,
        a: VecDeque<PagedColumn<(D, T, R)>>,
        b: VecDeque<PagedColumn<(D, T, R)>>,
    ) -> VecDeque<PagedColumn<(D, T, R)>> {
        let mut output: VecDeque<PagedColumn<(D, T, R)>> = VecDeque::new();
        let pager = self.pager();
        merge_chains(
            FetchIter::new(a, &pager),
            FetchIter::new(b, &pager),
            |paged| output.push_back(paged),
            &mut self.stash,
        );
        output
    }
}

/// Streaming materializer over a chain of [`PagedColumn`] entries.
///
/// `next` consumes one entry and calls [`ColumnPager::take`] to produce a
/// resident [`Column`]. Bounds materialized chunks to whatever the consumer
/// holds (typically one head per chain in [`merge_chains`]).
pub struct FetchIter<'a, D, T, R>
where
    (D, T, R): Columnar,
{
    queue: VecDeque<PagedColumn<(D, T, R)>>,
    pager: &'a ColumnPager,
}

impl<'a, D, T, R> FetchIter<'a, D, T, R>
where
    (D, T, R): Columnar,
{
    /// Wraps `queue` for streaming materialization through `pager`.
    pub fn new(queue: VecDeque<PagedColumn<(D, T, R)>>, pager: &'a ColumnPager) -> Self {
        Self { queue, pager }
    }

    /// Borrow the pager backing this iter so drivers can route output chunks
    /// back through `page()` without threading a separate `&pager`. The
    /// returned reference is tied to the outer `'a`, not to `&self`, so it
    /// stays valid across subsequent `next()` calls.
    pub fn pager(&self) -> &'a ColumnPager {
        self.pager
    }

    /// Drain remaining queued entries as `PagedColumn`s without materializing.
    /// Used by `merge_chains`'s drain-tail phase: once the other side is
    /// exhausted, the remaining entries on this side can pass straight to the
    /// output sink.
    pub fn into_paged(self) -> std::collections::vec_deque::IntoIter<PagedColumn<(D, T, R)>> {
        self.queue.into_iter()
    }
}

impl<D, T, R> Iterator for FetchIter<'_, D, T, R>
where
    (D, T, R): Columnar,
{
    type Item = Column<(D, T, R)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.queue.pop_front().map(|p| self.pager.take(p))
    }
}

/// Max recycled buffers held per merge/extract stash. Deliberately tiny: the
/// free-list is a hot-buffer cache for the `result`/`keep`/`ship` churn, not a
/// hoard. There is one stash per arrange batcher per worker (dozens across a
/// 16-worker join dataflow), and on the resident path (pager disabled or data
/// that fits) nothing else bounds these buffers — a large cap shows up directly
/// as arrangement memory. Two buffers cover steady-state reuse: ship one,
/// immediately refill the next.
const STASH_CAP: usize = 2;

/// Don't park a buffer larger than this in the free-list. A transiently large
/// merge buffer (well past the ship threshold) held resident would fight the
/// pager's memory budget; drop it and let a fresh default regrow. `2 × SHIP`
/// keeps normal ship-sized chunks while excluding pathological ones.
const MAX_RECYCLE_BYTES: usize = 1 << 22;

/// Pop a recycled empty buffer, or allocate a fresh `Column`. Recycled buffers
/// were cleared by [`recycle`], so they're ready to push into.
#[inline]
fn take_empty<C: Columnar>(stash: &mut Vec<Column<C>>) -> Column<C> {
    stash.pop().unwrap_or_default()
}

/// Clear `chunk` and return it to `stash` for reuse, subject to [`STASH_CAP`]
/// and [`MAX_RECYCLE_BYTES`].
///
/// Only `Typed` chunks carry a reusable typed allocation; `Bytes`/`Align`
/// have nothing worth caching. `length_in_bytes` (measured before we clear) is
/// the data the chunk holds — a proxy for the capacity we'd park resident — so
/// oversized buffers are dropped instead of hoarded.
#[inline]
fn recycle<C: Columnar>(mut chunk: Column<C>, stash: &mut Vec<Column<C>>) {
    if stash.len() < STASH_CAP
        && matches!(chunk, Column::Typed(_))
        && chunk.length_in_bytes() <= MAX_RECYCLE_BYTES
    {
        // `Column::clear` keeps the `Typed` allocation (clears in place); we
        // only reach here for `Typed`, so no buffer is discarded.
        chunk.clear();
        stash.push(chunk);
    }
}

/// Two-way merge driver. Reuses today's per-chunk gallop / ship-threshold
/// logic from `Column::merge_from`, but pulls heads from [`FetchIter`] and
/// emits finished output chunks through `sink` after routing them through
/// the pager exposed by [`FetchIter::pager`].
///
/// Transient merge buffers are recycled through `stash` rather than freshly
/// allocated per shipped chunk: `result` is drawn from the free-list and
/// exhausted heads are returned to it. Heads arrive already materialized from
/// [`FetchIter`], so the whole-chunk passthrough fast path (peek both heads'
/// endpoints; ship one wholesale when it sorts entirely before the other) is
/// free here — no extra fetch — and skips the per-record merge for the common
/// non-overlapping case.
pub fn merge_chains<D, T, R, Sink>(
    list1: FetchIter<'_, D, T, R>,
    list2: FetchIter<'_, D, T, R>,
    mut sink: Sink,
    stash: &mut Vec<Column<(D, T, R)>>,
) where
    D: Columnar,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Clone + PartialOrder,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>>,
    Sink: FnMut(PagedColumn<(D, T, R)>),
{
    let pager = list1.pager();
    let mut list1 = list1;
    let mut list2 = list2;

    let mut heads = [
        list1.next().unwrap_or_default(),
        list2.next().unwrap_or_default(),
    ];
    let mut positions = [0usize, 0usize];
    let mut result = take_empty(stash);

    loop {
        let upper_l = heads[0].borrow().len();
        let upper_r = heads[1].borrow().len();
        if positions[0] >= upper_l || positions[1] >= upper_r {
            break;
        }

        // Whole-chunk passthrough. The head is already resident, so peeking
        // its last record is free. When the entire left head (from position 0)
        // sorts before the right side's current record — and therefore before
        // all remaining right input, which is sorted after it — ship the head
        // wholesale and skip the per-record merge.
        let lhs_passthrough = positions[0] == 0 && upper_l > 0 && {
            let lhs = heads[0].borrow();
            let rhs = heads[1].borrow();
            let last_l = (lhs.0.get(upper_l - 1), lhs.1.get(upper_l - 1));
            let cur_r = (rhs.0.get(positions[1]), rhs.1.get(positions[1]));
            last_l < cur_r
        };
        if lhs_passthrough {
            if !result.is_empty() {
                sink(pager.page(&mut result));
                result = take_empty(stash);
            }
            let mut head = std::mem::replace(&mut heads[0], list1.next().unwrap_or_default());
            sink(pager.page(&mut head));
            positions[0] = 0;
            continue;
        }

        let rhs_passthrough = positions[1] == 0 && upper_r > 0 && {
            let lhs = heads[0].borrow();
            let rhs = heads[1].borrow();
            let last_r = (rhs.0.get(upper_r - 1), rhs.1.get(upper_r - 1));
            let cur_l = (lhs.0.get(positions[0]), lhs.1.get(positions[0]));
            last_r < cur_l
        };
        if rhs_passthrough {
            if !result.is_empty() {
                sink(pager.page(&mut result));
                result = take_empty(stash);
            }
            let mut head = std::mem::replace(&mut heads[1], list2.next().unwrap_or_default());
            sink(pager.page(&mut head));
            positions[1] = 0;
            continue;
        }

        let yielded = result.merge_from(&mut heads, &mut positions);

        if positions[0] >= heads[0].borrow().len() {
            let old = std::mem::replace(&mut heads[0], list1.next().unwrap_or_default());
            recycle(old, stash);
            positions[0] = 0;
        }
        if positions[1] >= heads[1].borrow().len() {
            let old = std::mem::replace(&mut heads[1], list2.next().unwrap_or_default());
            recycle(old, stash);
            positions[1] = 0;
        }
        if yielded || result.at_capacity() {
            sink(pager.page(&mut result));
            result = take_empty(stash);
        }
    }

    // Drain remaining: copy partial head through `merge_from`'s 1-input
    // dispatch, then hand the rest of the chain's `PagedColumn`s straight to
    // the sink without materializing.
    drain_side(
        &mut heads[0],
        &mut positions[0],
        list1,
        &mut result,
        &mut sink,
        pager,
        stash,
    );
    drain_side(
        &mut heads[1],
        &mut positions[1],
        list2,
        &mut result,
        &mut sink,
        pager,
        stash,
    );

    if !result.is_empty() {
        sink(pager.page(&mut result));
    }
}

/// Helper for `merge_chains`'s drain phase: copy a partially-consumed head
/// into `result` (via 1-input `merge_from`), ship `result` if non-empty, then
/// pass the remaining queued `PagedColumn`s straight through.
fn drain_side<D, T, R, Sink>(
    head: &mut Column<(D, T, R)>,
    pos: &mut usize,
    rest: FetchIter<'_, D, T, R>,
    result: &mut Column<(D, T, R)>,
    sink: &mut Sink,
    pager: &ColumnPager,
    stash: &mut Vec<Column<(D, T, R)>>,
) where
    D: Columnar,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Clone + PartialOrder,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>>,
    Sink: FnMut(PagedColumn<(D, T, R)>),
{
    if *pos < head.borrow().len() {
        // 1-input dispatch — bulk copy that runs to completion.
        let _ = result.merge_from(std::slice::from_mut(head), std::slice::from_mut(pos));
    }
    if !result.is_empty() {
        sink(pager.page(result));
        // Refill so the second `drain_side` call (other side) doesn't push
        // into a freshly-allocated `result`.
        *result = take_empty(stash);
    }
    for paged in rest.into_paged() {
        sink(paged);
    }
}

/// Streaming extract: walks `merged` chunk-by-chunk via `Column::extract`,
/// routing each filled keep/ship chunk through its sink after pageing.
/// Mirrors the per-chunk ship-threshold yield already inside
/// `Column::extract`.
pub fn extract_chain<D, T, R, SinkShip, SinkKeep>(
    merged: FetchIter<'_, D, T, R>,
    upper: AntichainRef<T>,
    frontier: &mut Antichain<T>,
    mut ship: SinkShip,
    mut keep: SinkKeep,
    stash: &mut Vec<Column<(D, T, R)>>,
) where
    D: Columnar,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Clone + PartialOrder,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>>,
    SinkShip: FnMut(PagedColumn<(D, T, R)>),
    SinkKeep: FnMut(PagedColumn<(D, T, R)>),
{
    let pager = merged.pager();
    let mut keep_buf = take_empty(stash);
    let mut ship_buf = take_empty(stash);

    for mut buffer in merged {
        let mut position = 0;
        let len = buffer.borrow().len();
        while position < len {
            buffer.extract(&mut position, upper, frontier, &mut keep_buf, &mut ship_buf);
            if keep_buf.at_capacity() {
                keep(pager.page(&mut keep_buf));
                keep_buf = take_empty(stash);
            }
            if ship_buf.at_capacity() {
                ship(pager.page(&mut ship_buf));
                ship_buf = take_empty(stash);
            }
        }
        // `buffer` is fully consumed (its records copied into keep/ship);
        // return its allocation to the pool.
        recycle(buffer, stash);
    }
    if !keep_buf.is_empty() {
        keep(pager.page(&mut keep_buf));
    }
    if !ship_buf.is_empty() {
        ship(pager.page(&mut ship_buf));
    }
}

#[cfg(test)]
#[allow(clippy::clone_on_ref_ptr)]
mod tests {
    use std::sync::Arc;

    use columnar::Index;
    use timely::container::PushInto as _;

    use super::*;
    use crate::column_pager::{PageDecision, PageEvent, PageHint, PagingPolicy};

    // ----- helpers -----------------------------------------------------------

    type KvUpdate = ((u64, u64), u64, i64);

    fn col(rows: &[KvUpdate]) -> Column<KvUpdate> {
        let mut c: Column<KvUpdate> = Default::default();
        for &t in rows {
            c.push_into(t);
        }
        c
    }

    fn collect_pc(chunks: &[PagedColumn<KvUpdate>], pager: &ColumnPager) -> Vec<KvUpdate> {
        // `collect_pc` peeks via materialization on a side path so the test's
        // assertions don't consume the chain.
        chunks
            .iter()
            .flat_map(|p| {
                let view: Column<KvUpdate> = match p {
                    PagedColumn::Resident(c, _) => clone_column(c),
                    _ => pager.take(clone_paged(p)),
                };
                collect_column(&view).into_iter()
            })
            .collect()
    }

    fn collect_column(c: &Column<KvUpdate>) -> Vec<KvUpdate> {
        c.borrow()
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

    fn clone_column(c: &Column<KvUpdate>) -> Column<KvUpdate> {
        // `Column` is `Clone` when `C::Container: Clone`, which is true for
        // tuple-of-primitive containers. Used so test helpers can peek at a
        // chain without consuming it.
        c.clone()
    }

    /// Helper that bypasses `pager.take` for non-`Resident` variants by
    /// taking and re-pageing. Only used in test inspection paths where the
    /// extra round-trip is acceptable.
    fn clone_paged(p: &PagedColumn<KvUpdate>) -> PagedColumn<KvUpdate> {
        match p {
            PagedColumn::Resident(c, _) => {
                // Wrap via a disabled pager so the ticket is fresh.
                let mut c = c.clone();
                ColumnPager::disabled().page(&mut c)
            }
            // For paged/compressed variants we can't clone without
            // re-reading; the tests below only inspect Resident chains.
            _ => panic!("clone_paged only supports Resident"),
        }
    }

    /// Always-page policy: bypasses any resident shortcut so we can assert
    /// the chains remain in `Paged` form regardless of memory pressure.
    struct ForcePagePolicy {
        out: std::sync::atomic::AtomicUsize,
        r#in: std::sync::atomic::AtomicUsize,
    }
    impl ForcePagePolicy {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                out: std::sync::atomic::AtomicUsize::new(0),
                r#in: std::sync::atomic::AtomicUsize::new(0),
            })
        }
    }
    impl PagingPolicy for ForcePagePolicy {
        fn decide(&self, _hint: PageHint) -> PageDecision {
            PageDecision::Page {
                backend: mz_ore::pager::Backend::Swap,
                codec: None,
            }
        }
        fn record(&self, event: PageEvent) {
            use std::sync::atomic::Ordering;
            match event {
                PageEvent::PagedOut { .. } => {
                    self.out.fetch_add(1, Ordering::Relaxed);
                }
                PageEvent::PagedIn { .. } => {
                    self.r#in.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
        }
    }

    /// Wrap a Vec<Column> as a paged chain for `FetchIter`.
    fn to_chain(
        cols: Vec<Column<KvUpdate>>,
        pager: &ColumnPager,
    ) -> VecDeque<PagedColumn<KvUpdate>> {
        cols.into_iter().map(|mut c| pager.page(&mut c)).collect()
    }

    /// Drive `merge_chains` with a disabled pager and return owned tuples.
    fn drive_merge(chain1: Vec<Column<KvUpdate>>, chain2: Vec<Column<KvUpdate>>) -> Vec<KvUpdate> {
        let pager = ColumnPager::disabled();
        let q1 = to_chain(chain1, &pager);
        let q2 = to_chain(chain2, &pager);
        let mut output: Vec<PagedColumn<KvUpdate>> = Vec::new();
        let mut stash = Vec::new();
        merge_chains(
            FetchIter::new(q1, &pager),
            FetchIter::new(q2, &pager),
            |paged| output.push(paged),
            &mut stash,
        );
        collect_pc(&output, &pager)
    }

    // ----- merge_chains correctness -----------------------------------------

    /// Disjoint chains: same data as the legacy passthrough test. Without
    /// passthrough, the merger runs per-record but should still produce the
    /// fully ordered output.
    #[mz_ore::test]
    fn merge_chains_disjoint_ranges() {
        let out = drive_merge(
            vec![
                col(&[((0, 0), 0, 1), ((1, 0), 0, 1)]),
                col(&[((2, 0), 0, 1), ((3, 0), 0, 1)]),
            ],
            vec![
                col(&[((10, 0), 0, 1), ((11, 0), 0, 1)]),
                col(&[((12, 0), 0, 1), ((13, 0), 0, 1)]),
            ],
        );
        let expected: Vec<_> = (0..4u64)
            .map(|d| ((d, 0u64), 0u64, 1i64))
            .chain((10..14u64).map(|d| ((d, 0u64), 0u64, 1i64)))
            .collect();
        assert_eq!(out, expected);
    }

    /// Interleaved chains: every record alternates between the two chains.
    #[mz_ore::test]
    fn merge_chains_interleaved() {
        let out = drive_merge(
            vec![
                col(&[((0, 0), 0, 1), ((2, 0), 0, 1)]),
                col(&[((4, 0), 0, 1), ((6, 0), 0, 1)]),
            ],
            vec![
                col(&[((1, 0), 0, 1), ((3, 0), 0, 1)]),
                col(&[((5, 0), 0, 1), ((7, 0), 0, 1)]),
            ],
        );
        let expected: Vec<_> = (0..8u64).map(|d| ((d, 0u64), 0u64, 1i64)).collect();
        assert_eq!(out, expected);
    }

    /// Equal-key consolidation across chunk boundaries: chain1's last record
    /// shares `(d, t)` with chain2's first; sum of diffs should land on a
    /// single output record.
    #[mz_ore::test]
    fn merge_chains_equal_boundary() {
        let out = drive_merge(
            vec![col(&[((0, 0), 0, 1), ((5, 0), 0, 1)])],
            vec![col(&[((5, 0), 0, 1), ((10, 0), 0, 1)])],
        );
        assert_eq!(out, vec![((0, 0), 0, 1), ((5, 0), 0, 2), ((10, 0), 0, 1)]);
    }

    /// Same merge, force-paged: chains stay in `Paged` form throughout, and
    /// the consolidated result still matches.
    #[mz_ore::test]
    fn merge_chains_force_paged_round_trip() {
        let policy = ForcePagePolicy::new();
        let pager = ColumnPager::new(policy.clone());
        let q1 = to_chain(vec![col(&[((0, 0), 0, 1), ((2, 0), 0, 1)])], &pager);
        let q2 = to_chain(vec![col(&[((1, 0), 0, 1), ((3, 0), 0, 1)])], &pager);

        // Confirm the chains started paged-out (not Resident).
        assert!(matches!(q1.front().unwrap(), PagedColumn::Paged { .. }));
        assert!(matches!(q2.front().unwrap(), PagedColumn::Paged { .. }));

        let mut output: Vec<PagedColumn<KvUpdate>> = Vec::new();
        let mut stash = Vec::new();
        merge_chains(
            FetchIter::new(q1, &pager),
            FetchIter::new(q2, &pager),
            |paged| output.push(paged),
            &mut stash,
        );

        // Output entries should also have been routed through the pager.
        for p in &output {
            assert!(matches!(p, PagedColumn::Paged { .. }));
        }

        // Materialize the output and check correctness.
        let mut collected = Vec::new();
        for p in output {
            let c = pager.take(p);
            collected.extend(collect_column(&c));
        }
        let expected: Vec<_> = (0..4u64).map(|d| ((d, 0u64), 0u64, 1i64)).collect();
        assert_eq!(collected, expected);
    }

    // ----- extract_chain correctness ----------------------------------------

    #[mz_ore::test]
    fn extract_chain_partitions_by_frontier() {
        let pager = ColumnPager::disabled();
        let data = vec![
            ((0, 0), 0u64, 1i64),
            ((1, 0), 1, 1),
            ((2, 0), 2, 1),
            ((3, 0), 3, 1),
        ];
        let chain = to_chain(vec![col(&data)], &pager);
        let upper = Antichain::from_elem(2u64);
        let mut frontier: Antichain<u64> = Antichain::new();
        let mut ship: Vec<PagedColumn<KvUpdate>> = Vec::new();
        let mut keep: Vec<PagedColumn<KvUpdate>> = Vec::new();

        let mut stash = Vec::new();
        extract_chain(
            FetchIter::new(chain, &pager),
            upper.borrow(),
            &mut frontier,
            |p| ship.push(p),
            |p| keep.push(p),
            &mut stash,
        );

        let shipped = collect_pc(&ship, &pager);
        let kept = collect_pc(&keep, &pager);
        for (_, t, _) in &shipped {
            assert!(*t < 2, "shipped time {t} should be < upper");
        }
        for (_, t, _) in &kept {
            assert!(*t >= 2, "kept time {t} should be >= upper");
        }
        assert_eq!(shipped.len() + kept.len(), data.len());
    }

    // ----- ColumnMergeBatcher end-to-end ------------------------------------

    /// Trivial Builder used by `seal`: collects inputs into a Vec for the
    /// test to inspect.
    #[derive(Default)]
    struct VecBuilder;
    impl differential_dataflow::trace::Builder for VecBuilder {
        type Input = Column<KvUpdate>;
        type Time = u64;
        type Output = Vec<KvUpdate>;
        fn with_capacity(_keys: usize, _vals: usize, _upds: usize) -> Self {
            Self
        }
        fn push(&mut self, _chunk: &mut Self::Input) {}
        fn done(
            self,
            _description: differential_dataflow::trace::Description<u64>,
        ) -> Self::Output {
            Vec::new()
        }
        fn seal(
            chain: &mut Vec<Self::Input>,
            _description: differential_dataflow::trace::Description<u64>,
        ) -> Self::Output {
            let mut out = Vec::new();
            for c in chain.drain(..) {
                out.extend(collect_column(&c));
            }
            out
        }
    }

    #[mz_ore::test]
    fn batcher_seal_round_trip() {
        let mut b: ColumnMergeBatcher<(u64, u64), u64, i64> =
            differential_dataflow::trace::Batcher::new(None, 0);
        // Two pushes; second has an equal-key collision with the first.
        let mut input1 = col(&[((1, 1), 0, 1), ((2, 0), 0, 1), ((3, 0), 0, 1)]);
        let mut input2 = col(&[((2, 0), 0, 2), ((4, 0), 0, 1)]);
        differential_dataflow::trace::Batcher::push_container(&mut b, &mut input1);
        differential_dataflow::trace::Batcher::push_container(&mut b, &mut input2);

        // Seal everything (upper = ∞-ish, here just past any time we used).
        let upper = Antichain::from_elem(u64::MAX);
        let out: Vec<KvUpdate> =
            differential_dataflow::trace::Batcher::seal::<VecBuilder>(&mut b, upper);

        // (2, 0)@0 was pushed with +1 then +2; sums to +3 after consolidation.
        let mut expected = vec![
            ((1u64, 1u64), 0u64, 1i64),
            ((2, 0), 0, 3),
            ((3, 0), 0, 1),
            ((4, 0), 0, 1),
        ];
        expected.sort();
        let mut out_sorted = out.clone();
        out_sorted.sort();
        assert_eq!(out_sorted, expected);
    }

    #[mz_ore::test]
    fn account_chunk_resident_vs_paged() {
        let policy = ForcePagePolicy::new();
        let pager_paged = ColumnPager::new(policy.clone());
        let pager_res = ColumnPager::disabled();

        let mut c1 = col(&[((1, 1), 0, 1), ((2, 0), 0, 1), ((3, 0), 0, 1)]);
        let resident = pager_res.page(&mut c1);
        let (records, size, capacity, allocations) = account_chunk(&resident);
        assert_eq!(records, 3);
        assert!(size > 0);
        assert_eq!(size, capacity);
        assert_eq!(allocations, 1);

        let mut c2 = col(&[((1, 1), 0, 1), ((2, 0), 0, 1)]);
        let paged = pager_paged.page(&mut c2);
        assert!(matches!(paged, PagedColumn::Paged { .. }));
        // Paged variants contribute zero to memory accounting.
        assert_eq!(account_chunk(&paged), (0, 0, 0, 0));
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `madvise` on OS `linux`
    fn batcher_seal_keeps_kept_chain_paged() {
        // Force-page policy; verify that after seal, the kept chain in
        // self.chains contains only Paged entries (no Resident).
        let policy = ForcePagePolicy::new();
        let pager = ColumnPager::new(policy.clone());

        let mut b: ColumnMergeBatcher<(u64, u64), u64, i64> =
            differential_dataflow::trace::Batcher::new(None, 0);
        b.set_pager(pager);

        // Push records straddling an upper of 5 — half should be kept, half
        // shipped. Use enough records to fill at least one chunk.
        let n: u64 = 200;
        for i in 0..n {
            let mut input = col(&[((i, 0), i % 10, 1)]);
            differential_dataflow::trace::Batcher::push_container(&mut b, &mut input);
        }
        let upper = Antichain::from_elem(5u64);
        let _ = differential_dataflow::trace::Batcher::seal::<VecBuilder>(&mut b, upper);

        // Anything kept (times >= 5) should be sitting in b.chains as paged.
        let kept_records: usize = b
            .chains
            .iter()
            .flat_map(|c| c.iter())
            .map(|p| match p {
                PagedColumn::Paged { meta, .. } => {
                    // Records aren't directly available here; sanity-check
                    // that no Resident snuck in.
                    let _ = meta;
                    1
                }
                PagedColumn::Compressed { meta, .. } => {
                    let _ = meta;
                    1
                }
                PagedColumn::Resident(_, _) => {
                    panic!("kept chain entry was Resident under ForcePagePolicy");
                }
            })
            .sum();
        // We expect *some* kept entries (times in [5..10) loop slot).
        assert!(kept_records > 0, "expected at least one kept paged entry");
        assert!(policy.out.load(std::sync::atomic::Ordering::Relaxed) > 0);
        let _ = n;
    }
}
