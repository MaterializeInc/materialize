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
//! Reuses the resident building blocks from [`super::batcher`]: the inherent
//! `Column::merge_from` / `Column::extract` methods (per-chunk merge / split).
//! Input consolidation happens upstream: the chunker
//! ([`super::batcher::ColumnChunker`]) is supplied to the arrange operator
//! separately, so this batcher receives already-consolidated [`Column`] chunks
//! via [`PushInto`].
//!
//! [`differential_dataflow`]: differential_dataflow::trace::implementations::merge_batcher

use std::collections::VecDeque;

use columnar::{Columnar, Index, Len};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::logging::{BatcherEvent, Logger};
use differential_dataflow::trace::{Batcher, Description};
use timely::Accountable;
use timely::PartialOrder;
use timely::container::{PushInto, SizableContainer};
use timely::dataflow::channels::ContainerBytes;
use timely::progress::Timestamp;
use timely::progress::frontier::{Antichain, AntichainRef};

use crate::column_pager::{self, ColumnPager, PagedColumn};
use crate::columnar::Column;
use crate::columnar::batcher::{empty_chunk, recycle_chunk};

/// Max recycled empty chunks held in the per-batcher stash. Deliberately
/// tight: the stash is a hot-buffer cache for the result/keep/ship churn,
/// not a hoard. Stash entries are cleared `Column::Typed` allocations that
/// retain capacity but are *not* tracked by [`ColumnPager`]'s
/// `ResidentTicket` accounting, so each one is a chunk's worth of resident
/// bytes the pager's budget doesn't see. There's one stash per arrange
/// batcher per worker, so this multiplies fast.
///
/// 2 covers steady-state reuse for both code paths: `merge_chains` ships
/// `result` and immediately pulls a refill; `extract_chain` ships `keep` /
/// `ship` and pulls a refill for whichever was at capacity. Heads that
/// drain mid-loop arrive resident from `FetchIter`, so the whole-chunk
/// passthrough fast path keeps most of them off the merge inner loop
/// entirely — only a small minority ever flow back through the stash.
const STASH_CAP: usize = 2;

/// Don't park a buffer larger than this in the free-list. A transiently
/// oversize merge buffer (post-explosion, past the natural ship threshold)
/// held resident would compete with the pager's budget; drop it and let a
/// fresh default regrow. 2 × the natural ship word count (≈ 4 MiB
/// serialized) keeps normal ship-sized chunks while excluding pathological
/// ones.
const MAX_RECYCLE_BYTES: usize = 1 << 22;

/// Recycle `chunk` only if the stash isn't already at [`STASH_CAP`] and the
/// chunk isn't oversize per [`MAX_RECYCLE_BYTES`]. `length_in_bytes` is
/// measured before clear, so it reflects the data the chunk was carrying
/// (a proxy for the capacity we'd park).
fn recycle_capped<C: Columnar>(chunk: Column<C>, stash: &mut Vec<Column<C>>) {
    if stash.len() < STASH_CAP && chunk.length_in_bytes() <= MAX_RECYCLE_BYTES {
        recycle_chunk(chunk, stash);
    }
}

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
    chains: Vec<VecDeque<PagedColumn<(D, T, R)>>>,
    lower: Antichain<T>,
    frontier: Antichain<T>,
    /// Recycled empty `Column::Typed` chunks. Drained heads and shipped result
    /// buffers feed in here; subsequent merge / extract calls pop from here
    /// instead of starting from a zero-capacity `Column::default()`. Mirrors
    /// the stash carried by the upstream `differential_dataflow` merge-batcher
    /// framework, which this type forks. Without it, each shipped chunk
    /// triggers a fresh per-leaf grow cycle and per-merge-round allocation
    /// dominates the inner loop.
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
/// reported there. Pooled chunks likewise contribute zero: the buffer pool
/// budgets and accounts its own resident bytes.
fn account_chunk<C: Columnar>(entry: &PagedColumn<C>) -> (usize, usize, usize, usize) {
    match entry {
        PagedColumn::Resident(col, _) => {
            let records = usize::try_from(col.record_count()).expect("non-negative");
            let bytes = col.length_in_bytes();
            (records, bytes, bytes, 1)
        }
        PagedColumn::Paged { .. } | PagedColumn::Compressed { .. } | PagedColumn::Pooled { .. } => {
            (0, 0, 0, 0)
        }
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
    type Output = Column<(D, T, R)>;
    type Time = T;

    fn new(logger: Option<Logger>, operator_id: usize) -> Self {
        // No pager snapshot taken here — `self.pager()` reads
        // `column_pager::global_pager` per call, so dyncfg-driven re-installs
        // take effect on the next chunk.
        Self {
            chains: Vec::new(),
            lower: Antichain::from_elem(T::minimum()),
            frontier: Antichain::new(),
            stash: Vec::new(),
            pager_override: None,
            logger,
            operator_id,
        }
    }

    fn seal(
        &mut self,
        upper: Antichain<Self::Time>,
    ) -> (Vec<Self::Output>, Description<Self::Time>) {
        let (chunks, description) = self.seal_paged(upper);
        (chunks.collect(), description)
    }

    fn frontier(&mut self) -> AntichainRef<'_, Self::Time> {
        self.frontier.borrow()
    }
}

impl<D, T, R> ColumnMergeBatcher<D, T, R>
where
    D: Columnar,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Timestamp + PartialOrder,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>>,
    for<'a> columnar::Ref<'a, R>: Ord,
{
    /// Seals like [`Batcher::seal`], but returns the ship side as
    /// [`SealedChunks`]: chunk handles that stay paged until iterated,
    /// rehydrating one chunk per `next` call.
    ///
    /// [`Batcher::seal`] materializes every shipped chunk into one `Vec`, so a
    /// seal's transient memory is the full uncompressed ship side — unbounded
    /// when a frontier advance releases a large backlog at once. A consumer
    /// that processes chunks sequentially and drops each before the next keeps
    /// that footprint to a single chunk by sealing through this method
    /// instead.
    pub fn seal_paged(&mut self, upper: Antichain<T>) -> (SealedChunks<D, T, R>, Description<T>) {
        let pager = self.pager();
        // Merge all remaining chains into one.
        while self.chains.len() > 1 {
            let a = self.chain_pop().unwrap();
            let b = self.chain_pop().unwrap();
            let merged = self.merge_by(a, b);
            self.chain_push(merged);
        }
        let merged = self.chain_pop().unwrap_or_default();

        // Extract `merged` into `readied` (ship side, still paged) and
        // `kept_chain` (keep side, stays paged for the next round).
        let mut readied: Vec<PagedColumn<(D, T, R)>> = Vec::new();
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
                |paged| readied.push(paged),
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
        self.lower = upper;

        // Drop the recycle stash now that this round's hot work is done.
        // The next merge after the next `push_into` will re-pay one
        // chunk's worth of leaf-`Vec` grow tax, but that's a few hundred µs
        // amortized over a seal cycle, well worth handing the leaf bytes
        // back to the allocator so they're not held resident across what
        // may be a quiet stretch.
        self.stash.clear();

        let chunks = SealedChunks {
            chunks: readied.into_iter(),
            pager,
        };
        (chunks, description)
    }
}

/// The ship side of a [`ColumnMergeBatcher::seal_paged`] call: sorted,
/// consolidated chunks that stay paged until iterated.
///
/// Each `next` call rehydrates exactly one chunk, so a consumer that drops
/// each chunk before requesting the next holds at most one chunk resident,
/// however large the sealed backlog.
pub struct SealedChunks<D: Columnar, T: Columnar, R: Columnar> {
    chunks: std::vec::IntoIter<PagedColumn<(D, T, R)>>,
    pager: ColumnPager,
}

impl<D: Columnar, T: Columnar, R: Columnar> Iterator for SealedChunks<D, T, R> {
    type Item = Column<(D, T, R)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.chunks.next().map(|paged| self.pager.take(paged))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.chunks.size_hint()
    }
}

impl<D: Columnar, T: Columnar, R: Columnar> ExactSizeIterator for SealedChunks<D, T, R> {}

impl<D, T, R> PushInto<Column<(D, T, R)>> for ColumnMergeBatcher<D, T, R>
where
    D: Columnar,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Clone + PartialOrder,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>>,
{
    /// Accept an already-consolidated chunk from the upstream chunker, route
    /// it through the pager, and insert it as a singleton chain.
    fn push_into(&mut self, mut chunk: Column<(D, T, R)>) {
        let pager = self.pager();
        let paged = pager.page(&mut chunk);
        self.insert_chain(VecDeque::from([paged]));
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
        let pager = &pager;
        let stash = &mut self.stash;
        merge_chains(
            FetchIter::new(a, pager),
            FetchIter::new(b, pager),
            |paged| output.push_back(paged),
            stash,
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

/// Two-way merge driver. Reuses today's per-chunk gallop / ship-threshold
/// logic from `Column::merge_from`, but pulls heads from [`FetchIter`] and
/// emits finished output chunks through `sink` after routing them through
/// the pager exposed by [`FetchIter::pager`].
///
/// `stash` is a pool of empty `Column::Typed` chunks. Drained heads and
/// shipped result buffers get recycled into it; the next result chunk is
/// pulled from it instead of starting from a zero-capacity default. This
/// matches the recycling discipline the upstream `differential_dataflow`
/// merge-batcher carries via `Merger::merge`'s `stash` parameter.
///
/// Whole-chunk passthrough: heads arrive materialized from [`FetchIter`], so
/// peeking endpoints is free. When the current head on one side sorts
/// entirely before the current record on the other side, ship it wholesale
/// and skip the per-record merge. Gated on `positions[i] == 0` so we hand
/// the head off intact — partial-tail passthrough would need a 1-input
/// `merge_from` to copy the tail, which is what the inner loop's gallop
/// already covers.
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
    let mut result: Column<(D, T, R)> = empty_chunk(stash);

    loop {
        let upper_l = heads[0].borrow().len();
        let upper_r = heads[1].borrow().len();
        if positions[0] >= upper_l || positions[1] >= upper_r {
            break;
        }

        // Whole-chunk passthrough. Two probes on already-resident heads.
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
                if let Some(reuse) = stash.pop() {
                    result = reuse;
                }
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
                if let Some(reuse) = stash.pop() {
                    result = reuse;
                }
            }
            let mut head = std::mem::replace(&mut heads[1], list2.next().unwrap_or_default());
            sink(pager.page(&mut head));
            positions[1] = 0;
            continue;
        }

        let yielded = result.merge_from(&mut heads, &mut positions);

        if positions[0] >= heads[0].borrow().len() {
            let old = std::mem::replace(&mut heads[0], list1.next().unwrap_or_default());
            recycle_capped(old, stash);
            positions[0] = 0;
        }
        if positions[1] >= heads[1].borrow().len() {
            let old = std::mem::replace(&mut heads[1], list2.next().unwrap_or_default());
            recycle_capped(old, stash);
            positions[1] = 0;
        }
        if yielded || result.at_capacity() {
            sink(pager.page(&mut result));
            // `pager.page` either took `result`'s allocation (Skip path leaves
            // a zero-cap default) or kept the Typed buffer (Paged / Compressed
            // paths clear in place). Pull a fresh chunk from the stash so the
            // next `merge_from` starts with retained capacity; if the stash is
            // empty, fall back to whatever `result` already is.
            if let Some(reuse) = stash.pop() {
                result = reuse;
            }
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
    } else {
        // Empty `result` may still carry a useful Typed allocation; recycle
        // so subsequent calls (next `merge_by`, the seal `extract_chain`)
        // can pick it up.
        recycle_capped(result, stash);
    }
    // Recycle the now-exhausted (or default) head slots too — for `Resident`
    // heads that finished naturally, this preserves their Typed allocation
    // for the next call.
    let [h0, h1] = heads;
    recycle_capped(h0, stash);
    recycle_capped(h1, stash);
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
        if let Some(reuse) = stash.pop() {
            *result = reuse;
        }
    }
    for paged in rest.into_paged() {
        sink(paged);
    }
}

/// Streaming extract: walks `merged` chunk-by-chunk via `Column::extract`,
/// routing each filled keep/ship chunk through its sink after pageing.
/// Mirrors the per-chunk ship-threshold yield already inside
/// `Column::extract`.
///
/// `stash` carries recycled `Column::Typed` buffers in and out so the
/// per-chunk extract loop doesn't restart from zero capacity each time
/// `keep_buf` / `ship_buf` ships and the source `buffer` is dropped.
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
    let mut keep_buf: Column<(D, T, R)> = empty_chunk(stash);
    let mut ship_buf: Column<(D, T, R)> = empty_chunk(stash);

    for mut buffer in merged {
        let mut position = 0;
        let len = buffer.borrow().len();
        while position < len {
            buffer.extract(&mut position, upper, frontier, &mut keep_buf, &mut ship_buf);
            if keep_buf.at_capacity() {
                keep(pager.page(&mut keep_buf));
                if let Some(reuse) = stash.pop() {
                    keep_buf = reuse;
                }
            }
            if ship_buf.at_capacity() {
                ship(pager.page(&mut ship_buf));
                if let Some(reuse) = stash.pop() {
                    ship_buf = reuse;
                }
            }
        }
        // Buffer fully consumed; recycle whatever Typed allocation it had.
        recycle_capped(buffer, stash);
    }
    if !keep_buf.is_empty() {
        keep(pager.page(&mut keep_buf));
    } else {
        recycle_capped(keep_buf, stash);
    }
    if !ship_buf.is_empty() {
        ship(pager.page(&mut ship_buf));
    } else {
        recycle_capped(ship_buf, stash);
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
        let mut stash: Vec<Column<KvUpdate>> = Vec::new();
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

    /// Regression: under the disabled (always-resident) pager, shipped chunks
    /// must be serialized into a fitting `Column::Align`, never parked as
    /// `Column::Typed`. A `Typed` result carries `Column::merge_from`'s
    /// worst-case `reserve_for` capacity; leaving it in the chain across merge
    /// rounds was the dominant source of merge-batcher resident memory. Only
    /// the live accumulator (`result`) and not-yet-shipped heads may be
    /// `Typed` — every entry that reaches the sink should be `Align`.
    #[mz_ore::test]
    fn merge_chains_ships_fitting_align() {
        let pager = ColumnPager::disabled();
        // Interleaved keys force the per-record merge path: records flow
        // through the `result` accumulator and ship as a merged chunk rather
        // than passing a head through wholesale.
        let q1 = to_chain(vec![col(&[((0, 0), 0, 1), ((2, 0), 0, 1)])], &pager);
        let q2 = to_chain(vec![col(&[((1, 0), 0, 1), ((3, 0), 0, 1)])], &pager);

        let mut output: Vec<PagedColumn<KvUpdate>> = Vec::new();
        let mut stash: Vec<Column<KvUpdate>> = Vec::new();
        merge_chains(
            FetchIter::new(q1, &pager),
            FetchIter::new(q2, &pager),
            |paged| output.push(paged),
            &mut stash,
        );

        assert!(!output.is_empty(), "merge produced no chunks");
        for entry in &output {
            match entry {
                PagedColumn::Resident(col, _) => assert!(
                    matches!(col, Column::Align(_)),
                    "shipped chunk parked as non-Align resident: {:?}",
                    std::mem::discriminant(col),
                ),
                other => panic!(
                    "disabled pager should ship Resident, got a paged variant: {:?}",
                    std::mem::discriminant(other)
                ),
            }
        }

        // Sanity: data round-trips through the fitting Align buffers.
        assert_eq!(
            collect_pc(&output, &pager),
            vec![
                ((0, 0), 0, 1),
                ((1, 0), 0, 1),
                ((2, 0), 0, 1),
                ((3, 0), 0, 1)
            ],
        );
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
        let mut stash: Vec<Column<KvUpdate>> = Vec::new();
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
        let mut stash: Vec<Column<KvUpdate>> = Vec::new();

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

    #[mz_ore::test]
    fn batcher_seal_round_trip() {
        let mut b: ColumnMergeBatcher<(u64, u64), u64, i64> =
            differential_dataflow::trace::Batcher::new(None, 0);
        // Two pushes; second has an equal-key collision with the first.
        // Inputs arrive pre-consolidated chunk-by-chunk, as from the upstream
        // chunker.
        let input1 = col(&[((1, 1), 0, 1), ((2, 0), 0, 1), ((3, 0), 0, 1)]);
        let input2 = col(&[((2, 0), 0, 2), ((4, 0), 0, 1)]);
        b.push_into(input1);
        b.push_into(input2);

        // Seal everything (upper = ∞-ish, here just past any time we used).
        let upper = Antichain::from_elem(u64::MAX);
        let (chain, _description) = differential_dataflow::trace::Batcher::seal(&mut b, upper);
        let out: Vec<KvUpdate> = chain.iter().flat_map(collect_column).collect();

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
    fn seal_paged_ships_lazily() {
        let policy = ForcePagePolicy::new();
        let pager = ColumnPager::new(policy.clone());

        let mut b: ColumnMergeBatcher<(u64, u64), u64, i64> =
            differential_dataflow::trace::Batcher::new(None, 0);
        b.set_pager(pager);

        let n: u64 = 200;
        for i in 0..n {
            b.push_into(col(&[((i, 0), i % 10, 1)]));
        }

        let upper = Antichain::from_elem(5u64);
        let (chunks, _description) = b.seal_paged(upper);

        // The ship side must come out paged; rehydration happens per `next`.
        for paged in chunks.chunks.as_slice() {
            assert!(
                !matches!(paged, PagedColumn::Resident(..)),
                "ship side must stay paged until iterated",
            );
        }

        let mut out: Vec<KvUpdate> = Vec::new();
        for chunk in chunks {
            out.extend(collect_column(&chunk));
        }
        out.sort();
        let mut expected: Vec<KvUpdate> = (0..n)
            .filter(|i| i % 10 < 5)
            .map(|i| ((i, 0), i % 10, 1))
            .collect();
        expected.sort();
        assert_eq!(out, expected);
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
            let input = col(&[((i, 0), i % 10, 1)]);
            b.push_into(input);
        }
        let upper = Antichain::from_elem(5u64);
        let _ = differential_dataflow::trace::Batcher::seal(&mut b, upper);

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
                PagedColumn::Pooled { meta, .. } => {
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

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn batcher_seal_round_trip_pooled() {
        // Zero-budget pool: every inserted chunk is evicted to its extent as
        // soon as it lands, so the merge / seal path must fault everything
        // back in from extents rather than reading pool slots.
        let pool = mz_ore::pool::Pool::new(mz_ore::pool::PoolConfig {
            budget_bytes: 0,
            class_capacity_bytes: 64 << 20,
        })
        .expect("pool creation");

        let mut b: ColumnMergeBatcher<(u64, u64), u64, i64> =
            differential_dataflow::trace::Batcher::new(None, 0);
        b.set_pager(ColumnPager::pooled(pool.clone()));

        let n: u64 = 200;
        for i in 0..n {
            b.push_into(col(&[((i, 0), 0, 1)]));
        }
        let upper = Antichain::from_elem(u64::MAX);
        let (chain, _description) = differential_dataflow::trace::Batcher::seal(&mut b, upper);
        let mut out: Vec<KvUpdate> = chain.iter().flat_map(collect_column).collect();
        out.sort();
        let expected: Vec<KvUpdate> = (0..n).map(|i| ((i, 0u64), 0u64, 1i64)).collect();
        assert_eq!(out, expected);

        // The data really round-tripped through extents: the zero budget
        // forced compressing evictions, and reading the chains back faulted
        // chunks in from those extents.
        let stats = pool.stats();
        assert!(stats.inserts > 0, "expected pool inserts: {stats:?}");
        assert!(
            stats.evictions_compress > 0,
            "expected compressing evictions: {stats:?}"
        );
        assert!(stats.faults > 0, "expected extent fault-ins: {stats:?}");
    }
}
