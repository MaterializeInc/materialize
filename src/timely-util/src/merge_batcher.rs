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

//! A merge batcher that holds back far-future updates in a [`BucketChain`].
//!
//! [`TemporalBucketingMergeBatcher`] is a drop-in replacement for
//! differential's `MergeBatcher` that avoids the quadratic behavior the plain
//! batcher exhibits on future-stamped updates. The plain batcher re-merges and
//! re-extracts every held update at every seal, so an update at time `t` costs
//! work proportional to the number of seals until the frontier passes `t`.
//! Temporal filters produce updates days or weeks ahead of the frontier, which
//! turns that into `O(updates x seals)`.
//!
//! This batcher instead decides *empirically* at each seal, based on the data
//! it actually holds. The seal's single extract walk partitions updates three
//! ways against the seal `upper` and `upper + threshold`:
//!
//! * ready (`t < upper`): emitted, exactly like the plain batcher;
//! * near (`upper <= t < upper + threshold`): returned to the flat chains,
//!   exactly like the plain batcher's `kept` handling. Data raced a tick or
//!   two ahead of the frontier is re-merged a bounded number of times, which
//!   is cheaper than bucket bookkeeping;
//! * far (`upper + threshold <= t`): parked in a [`BucketChain`] and only
//!   touched again when splits or peels require it, amortized logarithmic in
//!   the update's distance from the frontier.
//!
//! When no far-future data is present the bucket chain is never touched and
//! the batcher's behavior and cost are the plain batcher's plus one branch.
//!
//! Every bucket carries the `(min, max)` times of its contents. Bounds exist
//! if and only if the bucket holds data, there is no "unknown" state. This is
//! what makes bucket splits cheap: a split whose midpoint falls outside a
//! bucket's bounds moves the bucket wholesale without touching records.
//! Bounds are established during the extract walks that move data anyway, so
//! maintaining them costs no extra passes. Consolidation inside a bucket can
//! cancel the records that attained a bound, so bounds are conservative
//! (possibly wider than the data) but always within the bucket's time range.
//! As a byproduct, [`differential_dataflow::trace::Batcher::frontier`] is
//! bucket-bound precise (the lowest non-empty bucket's minimum bound, exact
//! absent cancellation) rather than coarsened to bucket boundaries. The
//! arrange operator requires that it never falls below a sealed upper, which
//! bucket ranges guarantee: peels leave no bucket range, and hence no bound,
//! below the seal upper.

use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

use differential_dataflow::logging::{BatcherEvent, Logger};
use differential_dataflow::trace::{Batcher, Description};
use mz_ore::cast::CastFrom;
use mz_ore::soft_panic_or_log;
use timely::PartialOrder;
use timely::container::PushInto;
use timely::progress::frontier::{Antichain, AntichainRef};
use timely::progress::{PathSummary, Timestamp};

use crate::temporal::{Bucket, BucketChain, BucketTimestamp};

/// Fuel for [`BucketChain::restore`] per seal. Restoration work beyond this
/// budget is deferred to subsequent seals; `peel` tolerates chains that are
/// not well-formed, at the cost of extra splits.
const RESTORE_FUEL: i64 = 1_000_000;

/// The threshold by which a timestamp type advances seal uppers to separate
/// "near" from "far" future updates. See the module documentation.
pub trait TemporalThreshold: Timestamp {
    /// The current threshold. Read once per batcher at construction time.
    fn temporal_threshold() -> Self::Summary;
}

/// Process-global near/far threshold in milliseconds, shared by the
/// [`TemporalThreshold`] impls of millisecond-based timestamp types (`u64`
/// here, `mz_repr::Timestamp` in its own crate). Installed from the worker
/// configuration; batchers read it at construction time.
pub static TEMPORAL_THRESHOLD_MS: AtomicU64 = AtomicU64::new(2_000);

impl TemporalThreshold for u64 {
    fn temporal_threshold() -> u64 {
        TEMPORAL_THRESHOLD_MS.load(Ordering::Relaxed)
    }
}

/// The chain-merging engine a [`TemporalBucketingMergeBatcher`] is built on:
/// differential's
/// [`Merger`](differential_dataflow::trace::implementations::merge_batcher::Merger)
/// role plus time partitioning with attained bounds, self-contained so chain
/// entries need not be the containers the
/// batcher exchanges with the outside (the paged implementation absorbs
/// resident columns into pager-managed entries and materializes them back on
/// the way out).
///
/// The bounds are what enable [`MergeBucket`] splits to short-circuit, and
/// they must be *attained*: for each non-empty part, `min` and `max` are times
/// that actually occur in the part, and every time `t` in the part satisfies
/// `min <= t <= max`. Implementations compute them during the extract walks
/// they perform anyway. Consolidation may later cancel a bound's attaining
/// record, so consumers treat bounds as conservative.
///
/// Implementations carry their own scratch state (recycled chunk buffers),
/// released via [`Self::seal_done`] at the end of every seal.
pub trait TemporalMerger: Default {
    /// The type of time used in frontiers.
    type Time;
    /// The chain-entry representation held by the batcher.
    type Chunk;
    /// The chunk type the batcher exchanges with the outside: pushed chunks
    /// arrive as it and the seal emits it ([`Batcher`] requires the two to
    /// coincide).
    type Output: Default;

    /// Convert a pushed chunk into a chain entry.
    fn absorb(&mut self, input: Self::Output) -> Self::Chunk;

    /// Convert a readied chain entry into seal output.
    fn materialize(&mut self, chunk: Self::Chunk) -> Self::Output;

    /// Merge two sorted, consolidated chains into one, consolidating.
    fn merge(
        &mut self,
        list1: Vec<Self::Chunk>,
        list2: Vec<Self::Chunk>,
        output: &mut Vec<Self::Chunk>,
    );

    /// The number of updates in the chunk. Must be cheap, in particular it
    /// must not require materializing an offloaded chunk.
    fn chunk_len(chunk: &Self::Chunk) -> usize;

    /// The attained `(min, max)` times in the chunk, or `None` if it is
    /// empty or the implementation cannot inspect it in place. Intended for
    /// validation, not hot paths.
    fn chunk_time_bounds(chunk: &Self::Chunk) -> Option<(Self::Time, Self::Time)>;

    /// Partition `merged`, a sorted and consolidated chain, into three parts:
    /// `before` (`t` not in advance of `split_lo`), `beyond` (`t` in advance
    /// of `split_hi`), and `within` (the rest). With `split_lo == split_hi`
    /// this is a two-way partition with an empty `within`.
    ///
    /// Reports attained time bounds per part and the total number of records
    /// walked. Bounds for the `before` part are reported only when
    /// `track_before` is set: bounds tracking costs per-record work, and the
    /// seal never uses that part's bounds, so its dominant ready-data case
    /// must not pay for them.
    fn extract_time_partitioned(
        &mut self,
        merged: Vec<Self::Chunk>,
        split_lo: AntichainRef<'_, Self::Time>,
        split_hi: AntichainRef<'_, Self::Time>,
        track_before: bool,
    ) -> TimePartitioned<Self::Chunk, Self::Time>;

    /// Account size and allocation changes for a chunk. Returns
    /// `(records, size, capacity, allocations)`, counting only resident
    /// memory.
    fn account(chunk: &Self::Chunk) -> (usize, usize, usize, usize);

    /// A seal completed. Implementations drop per-seal scratch here, e.g.
    /// recycled chunk buffers that should not stay resident across a quiet
    /// stretch.
    fn seal_done(&mut self) {}
}

/// Track the attained inclusive `(min, max)` over observed times.
pub(crate) fn update_bounds<T: Ord + Clone>(bounds: &mut Option<(T, T)>, time: &T) {
    match bounds {
        None => *bounds = Some((time.clone(), time.clone())),
        Some((lo, hi)) => {
            if *time < *lo {
                *lo = time.clone();
            }
            if *time > *hi {
                *hi = time.clone();
            }
        }
    }
}

/// The result of [`TemporalMerger::extract_time_partitioned`].
pub struct TimePartitioned<C, T> {
    /// Chunks with times not in advance of `split_lo`.
    pub before: Vec<C>,
    /// Attained `(min, max)` times in `before`; `None` iff empty.
    pub before_bounds: Option<(T, T)>,
    /// Chunks with times in advance of `split_lo` but not of `split_hi`.
    pub within: Vec<C>,
    /// Attained `(min, max)` times in `within`; `None` iff empty.
    pub within_bounds: Option<(T, T)>,
    /// Chunks with times in advance of `split_hi`.
    pub beyond: Vec<C>,
    /// Attained `(min, max)` times in `beyond`; `None` iff empty.
    pub beyond_bounds: Option<(T, T)>,
    /// Total records walked.
    pub records: usize,
}

/// State shared between the batcher and its buckets: the records-touched
/// counter and size accounting. Buckets need it because [`Bucket::split`] has
/// no way to reach back to the batcher.
struct BucketCtx {
    /// Cumulative records touched by bucket-chain operations (routing, bucket
    /// merges, splits, peel drains). Deliberately excludes the flat-chain
    /// work that the plain batcher performs identically.
    touched: Cell<u64>,
    logger: Option<Logger>,
    operator_id: usize,
}

impl BucketCtx {
    fn touch(&self, records: usize) {
        self.touched
            .set(self.touched.get() + u64::cast_from(records));
    }

    /// Emit a [`BatcherEvent`] for the given per-chunk accounting tuples,
    /// multiplied by `diff` (+1 for data entering the bucket chain or a new
    /// chunk shape, -1 for data leaving or a consumed chunk shape).
    fn account<I: IntoIterator<Item = (usize, usize, usize, usize)>>(&self, items: I, diff: isize) {
        log_batcher_event(&self.logger, self.operator_id, items, diff);
    }
}

/// Sum per-chunk `(records, size, capacity, allocations)` accounting tuples
/// and log one [`BatcherEvent`] multiplied by `diff`. Without a logger this
/// returns before consuming `items`, so callers may pass lazy iterators over
/// work they would not otherwise perform.
pub(crate) fn log_batcher_event<I: IntoIterator<Item = (usize, usize, usize, usize)>>(
    logger: &Option<Logger>,
    operator: usize,
    items: I,
    diff: isize,
) {
    let Some(logger) = logger else {
        return;
    };
    let (mut records, mut size, mut capacity, mut allocations) = (0isize, 0isize, 0isize, 0isize);
    for (records_, size_, capacity_, allocations_) in items {
        records = records.saturating_add_unsigned(records_);
        size = size.saturating_add_unsigned(size_);
        capacity = capacity.saturating_add_unsigned(capacity_);
        allocations = allocations.saturating_add_unsigned(allocations_);
    }
    logger.log(BatcherEvent {
        operator,
        records_diff: records.saturating_mul(diff),
        size_diff: size.saturating_mul(diff),
        capacity_diff: capacity.saturating_mul(diff),
        allocations_diff: allocations.saturating_mul(diff),
    })
}

/// Contents of a non-empty [`MergeBucket`].
struct BucketContent<M: TemporalMerger> {
    /// Chains of sorted, consolidated chunks, geometrically sized.
    chains: Vec<Vec<M::Chunk>>,
    /// Number of records across all chains.
    records: usize,
    /// Attained minimum time.
    lo: M::Time,
    /// Attained maximum time (inclusive).
    hi: M::Time,
}

impl<M: TemporalMerger> BucketContent<M> {
    /// Content holding `chain` with attained bounds `(lo, hi)`.
    fn new(chain: Vec<M::Chunk>, lo: M::Time, hi: M::Time) -> Self {
        let records = chain.iter().map(|c| M::chunk_len(c)).sum();
        Self {
            chains: vec![chain],
            records,
            lo,
            hi,
        }
    }

    fn recount(&mut self) {
        self.records = self.chains.iter().flatten().map(|c| M::chunk_len(c)).sum();
    }
}

/// A bucket of merge-batcher chunks for one time range of a [`BucketChain`].
///
/// `content` is `None` iff the bucket holds no records; time bounds exist
/// whenever data does, so splits never lack the information for their
/// short-circuit checks.
pub struct MergeBucket<M: TemporalMerger> {
    content: Option<BucketContent<M>>,
    ctx: Rc<BucketCtx>,
}

impl<M: TemporalMerger> MergeBucket<M>
where
    M::Time: Ord + Clone,
{
    fn empty(ctx: Rc<BucketCtx>) -> Self {
        Self { content: None, ctx }
    }

    /// Construct a bucket holding `chain` with attained bounds `(lo, hi)`.
    /// Accounts the chunks as entering the bucket chain.
    fn full(chain: Vec<M::Chunk>, lo: M::Time, hi: M::Time, ctx: Rc<BucketCtx>) -> Self {
        ctx.account(chain.iter().map(M::account), 1);
        Self {
            content: Some(BucketContent::new(chain, lo, hi)),
            ctx,
        }
    }

    fn records(&self) -> usize {
        self.content.as_ref().map_or(0, |c| c.records)
    }

    /// Insert `chain` (attained bounds `(lo, hi)`) and re-establish geometric
    /// chain sizes. Accounts the chunks as entering the bucket chain and any
    /// merges as chunk-shape changes.
    fn insert_chain(&mut self, chain: Vec<M::Chunk>, lo: M::Time, hi: M::Time, merger: &mut M) {
        if chain.is_empty() {
            return;
        }
        self.ctx.account(chain.iter().map(M::account), 1);
        match &mut self.content {
            None => {
                self.content = Some(BucketContent::new(chain, lo, hi));
            }
            Some(content) => {
                if lo < content.lo {
                    content.lo = lo;
                }
                if hi > content.hi {
                    content.hi = hi;
                }
                content.chains.push(chain);
                while content.chains.len() > 1
                    && content.chains[content.chains.len() - 1].len()
                        >= content.chains[content.chains.len() - 2].len() / 2
                {
                    let list1 = content.chains.pop().expect("checked above");
                    let list2 = content.chains.pop().expect("checked above");
                    let walked: usize = list1
                        .iter()
                        .chain(list2.iter())
                        .map(|c| M::chunk_len(c))
                        .sum();
                    self.ctx.touch(walked);
                    self.ctx
                        .account(list1.iter().chain(list2.iter()).map(M::account), -1);
                    let mut merged = Vec::with_capacity(list1.len() + list2.len());
                    merger.merge(list1, list2, &mut merged);
                    self.ctx.account(merged.iter().map(M::account), 1);
                    content.chains.push(merged);
                }
                content.recount();
                // The merges consolidate, so the bucket may now be empty. It
                // must then drop its content: stale bounds would feed the
                // frontier times for which no data exists, and with no
                // records held anywhere the seal skips the peel that would
                // otherwise clean them up. The arrange operator would hold a
                // capability at such a phantom time below later seal uppers,
                // and the trace import panics minting a capability from the
                // next batch's hint.
                if content.records == 0 {
                    self.content = None;
                }
            }
        }
    }

    /// Take the content, accounting the chunks as leaving the bucket chain.
    fn take_content(mut self) -> Option<BucketContent<M>> {
        let content = self.content.take()?;
        self.ctx
            .account(content.chains.iter().flatten().map(M::account), -1);
        Some(content)
    }
}

/// Pairwise-merge `chains` into a single sorted chain, counting the walked
/// records into `ctx`.
fn merge_into_one<M: TemporalMerger>(
    mut chains: Vec<Vec<M::Chunk>>,
    merger: &mut M,
    ctx: &BucketCtx,
) -> Vec<M::Chunk> {
    while chains.len() > 1 {
        let list1 = chains.pop().expect("len checked");
        let list2 = chains.pop().expect("len checked");
        let walked: usize = list1
            .iter()
            .chain(list2.iter())
            .map(|c| M::chunk_len(c))
            .sum();
        ctx.touch(walked);
        let mut merged = Vec::with_capacity(list1.len() + list2.len());
        merger.merge(list1, list2, &mut merged);
        chains.push(merged);
    }
    chains.pop().unwrap_or_default()
}

impl<M: TemporalMerger> Bucket for MergeBucket<M>
where
    M::Time: BucketTimestamp,
{
    type Timestamp = M::Time;

    fn split(mut self, timestamp: &Self::Timestamp, fuel: &mut i64) -> (Self, Self) {
        let Some(content) = self.content.take() else {
            let empty = Self::empty(Rc::clone(&self.ctx));
            return (empty, self);
        };
        // Short-circuit when the attained bounds prove one-sidedness. The
        // surviving side keeps its content (and bounds) untouched.
        if content.hi < *timestamp {
            let empty = Self::empty(Rc::clone(&self.ctx));
            self.content = Some(content);
            return (self, empty);
        }
        if content.lo >= *timestamp {
            let empty = Self::empty(Rc::clone(&self.ctx));
            self.content = Some(content);
            return (empty, self);
        }
        // The split genuinely cuts through the data: merge and re-extract,
        // deriving exact bounds for both sides from the walk.
        //
        // NOTE: `Bucket::split` cannot reach the batcher's merger, so this
        // works with a fresh default one. The split's chunks miss out on the
        // batcher merger's recycled buffers, and any per-instance merger
        // configuration (e.g. a test pager override) does not apply here.
        let ctx = Rc::clone(&self.ctx);
        ctx.account(content.chains.iter().flatten().map(M::account), -1);
        let mut merger = M::default();
        let merged = merge_into_one::<M>(content.chains, &mut merger, &ctx);
        let at = Antichain::from_elem(timestamp.clone());
        let parts = merger.extract_time_partitioned(merged, at.borrow(), at.borrow(), true);
        *fuel = fuel.saturating_sub(i64::try_from(parts.records).unwrap_or(i64::MAX));
        ctx.touch(parts.records);
        ctx.account(
            parts
                .before
                .iter()
                .chain(parts.beyond.iter())
                .map(M::account),
            1,
        );
        let mk = |chain: Vec<M::Chunk>, bounds: Option<(M::Time, M::Time)>, ctx: Rc<BucketCtx>| {
            match bounds {
                Some((lo, hi)) => MergeBucket {
                    content: Some(BucketContent::new(chain, lo, hi)),
                    ctx,
                },
                None => MergeBucket::empty(ctx),
            }
        };
        let bot = mk(parts.before, parts.before_bounds, Rc::clone(&ctx));
        let top = mk(parts.beyond, parts.beyond_bounds, ctx);
        (bot, top)
    }
}

/// Statistics for observing the batcher's bucket-chain work. See
/// [`TemporalBucketingMergeBatcher::temporal_stats`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TemporalBatcherStats {
    /// Cumulative records touched by bucket-chain operations.
    pub touched: u64,
    /// Most records touched by any single seal.
    pub high_water_touched: u64,
    /// Records held in the bucket chain at the high-water seal, after routing
    /// and before peeling.
    pub high_water_held: u64,
}

/// A merge batcher that holds back far-future updates in a [`BucketChain`].
/// See the module documentation for the design.
pub struct TemporalBucketingMergeBatcher<M: TemporalMerger>
where
    M::Time: BucketTimestamp,
{
    /// Chains of sorted, consolidated chunks, geometrically sized; identical
    /// to the plain `MergeBatcher`'s chains. All input lands here, and
    /// near-future data returns here after each seal.
    chains: Vec<Vec<M::Chunk>>,
    /// Far-future data, deposited by seals, keyed by time range.
    bucket_chain: BucketChain<MergeBucket<M>>,
    merger: M,
    /// Separates near-future from far-future relative to each seal's upper.
    threshold: <M::Time as Timestamp>::Summary,
    ctx: Rc<BucketCtx>,
    /// `(touched, held)` of the seal with the most touched records.
    high_water: (u64, u64),
    /// We sealed up to here.
    lower: Antichain<M::Time>,
    /// Lower bound of held update times, recomputed at each seal. Never
    /// below the seal's upper, which the arrange operator relies on for its
    /// capability handling.
    frontier: Antichain<M::Time>,
}

impl<M: TemporalMerger> TemporalBucketingMergeBatcher<M>
where
    M::Time: BucketTimestamp + TemporalThreshold,
{
    /// Override the near/far threshold. Intended for tests; production reads
    /// [`TemporalThreshold`] at construction.
    #[cfg(test)]
    fn set_threshold(&mut self, threshold: <M::Time as Timestamp>::Summary) {
        self.threshold = threshold;
    }

    /// The underlying merger. Intended for tests that need to configure
    /// implementation-specific state (e.g. a pager override).
    #[cfg(test)]
    fn merger_mut(&mut self) -> &mut M {
        &mut self.merger
    }

    /// Observation counters for the bucket-chain work performed so far.
    pub fn temporal_stats(&self) -> TemporalBatcherStats {
        TemporalBatcherStats {
            touched: self.ctx.touched.get(),
            high_water_touched: self.high_water.0,
            high_water_held: self.high_water.1,
        }
    }

    /// Records currently held in the bucket chain.
    fn held_records(&self) -> usize {
        self.bucket_chain.buckets().map(|b| b.records()).sum()
    }

    /// Verify that every bucket's stored bounds contain exactly its data and
    /// respect its time range, and that record counts are consistent.
    ///
    /// Walks every held record. For tests only, never for production paths.
    #[cfg(test)]
    fn validate_bounds_invariant(&self) -> Result<(), String> {
        for (range, bucket) in self.bucket_chain.ranges() {
            let Some(content) = &bucket.content else {
                continue;
            };
            if content.lo > content.hi {
                return Err(format!(
                    "bucket bounds inverted: lo {:?} > hi {:?}",
                    content.lo, content.hi
                ));
            }
            let mut records = 0;
            for chunk in content.chains.iter().flatten() {
                records += M::chunk_len(chunk);
                if let Some((min, max)) = M::chunk_time_bounds(chunk) {
                    if min < content.lo || max > content.hi {
                        return Err(format!(
                            "chunk times [{min:?}, {max:?}] outside bucket bounds [{:?}, {:?}]",
                            content.lo, content.hi
                        ));
                    }
                }
            }
            if records != content.records {
                return Err(format!(
                    "bucket record count {} != actual {records}",
                    content.records
                ));
            }
            if records == 0 {
                return Err(format!(
                    "bucket with content but no records, bounds [{:?}, {:?}]",
                    content.lo, content.hi
                ));
            }
            if content.lo < range.start {
                return Err(format!(
                    "bucket bounds lo {:?} below bucket range start {:?}",
                    content.lo, range.start
                ));
            }
            if let Some(end) = range.end() {
                if content.hi >= *end {
                    return Err(format!(
                        "bucket bounds hi {:?} at or beyond bucket range end {end:?}",
                        content.hi
                    ));
                }
            }
        }
        Ok(())
    }

    /// Route a sorted, consolidated chain of far-future data (attained bounds
    /// `(lo, hi)`) into the bucket chain.
    fn route_into_bucket_chain(&mut self, chain: Vec<M::Chunk>, lo: M::Time, hi: M::Time) {
        let records: usize = chain.iter().map(|c| M::chunk_len(c)).sum();
        self.ctx.touch(records);

        // A fully-drained chain (all buckets peeled) is reseeded with one
        // bucket spanning the whole domain, which adopts the chain wholesale.
        if self.bucket_chain.is_empty() {
            self.bucket_chain =
                BucketChain::new(MergeBucket::full(chain, lo, hi, Rc::clone(&self.ctx)));
            return;
        }

        // Walk bucket boundaries in time order, splitting off each bucket's
        // portion. A chain that fits into a single bucket (the common case)
        // exits on the first iteration without an extract.
        let starts: Vec<M::Time> = self
            .bucket_chain
            .ranges()
            .map(|(range, _)| range.start)
            .collect();
        let mut idx = match starts.partition_point(|s| *s <= lo).checked_sub(1) {
            Some(idx) => idx,
            None => {
                // Data below the lowest bucket start. This violates the
                // routing invariant (far data is in advance of the seal
                // upper, and peels leave bucket starts at or beyond it), but
                // inserting into the lowest bucket is safe: the data is
                // released no later than that bucket, splits redistribute by
                // value, and the attained bounds keep the frontier exact.
                soft_panic_or_log!("temporal batcher: routed data below the lowest bucket start");
                0
            }
        };
        let mut chain = chain;
        let mut lo = lo;
        loop {
            let end = starts.get(idx + 1);
            let fits = match end {
                None => true,
                Some(end) => hi < *end,
            };
            if fits {
                let start = starts[idx].clone();
                let bucket = self
                    .bucket_chain
                    .find_mut(&start)
                    .expect("bucket exists for a known start");
                bucket.insert_chain(chain, lo, hi.clone(), &mut self.merger);
                return;
            }
            let at = Antichain::from_elem(end.expect("checked above").clone());
            let parts = self
                .merger
                .extract_time_partitioned(chain, at.borrow(), at.borrow(), true);
            self.ctx.touch(parts.records);
            if let Some((part_lo, part_hi)) = parts.before_bounds {
                let start = starts[idx].clone();
                let bucket = self
                    .bucket_chain
                    .find_mut(&start)
                    .expect("bucket exists for a known start");
                bucket.insert_chain(parts.before, part_lo, part_hi, &mut self.merger);
            }
            let Some((rest_lo, _)) = &parts.beyond_bounds else {
                return;
            };
            lo = rest_lo.clone();
            chain = parts.beyond;
            idx += 1;
            // Skip buckets the remaining data lies entirely beyond.
            while idx + 1 < starts.len() && starts[idx + 1] <= lo {
                idx += 1;
            }
        }
    }

    /// Merge two sorted chains via the merger. Counts the walked records.
    fn merge_chains(&mut self, list1: Vec<M::Chunk>, list2: Vec<M::Chunk>) -> Vec<M::Chunk> {
        if list1.is_empty() {
            return list2;
        }
        if list2.is_empty() {
            return list1;
        }
        let walked: usize = list1
            .iter()
            .chain(list2.iter())
            .map(|c| M::chunk_len(c))
            .sum();
        self.ctx.touch(walked);
        let mut merged = Vec::with_capacity(list1.len() + list2.len());
        self.merger.merge(list1, list2, &mut merged);
        merged
    }

    /// Insert a chain into the flat chains and maintain the geometric chain
    /// property, mirroring the plain `MergeBatcher`.
    fn insert_chain(&mut self, chain: Vec<M::Chunk>) {
        if !chain.is_empty() {
            self.chain_push(chain);
            while self.chains.len() > 1
                && (self.chains[self.chains.len() - 1].len()
                    >= self.chains[self.chains.len() - 2].len() / 2)
            {
                let list1 = self.chain_pop().expect("len checked");
                let list2 = self.chain_pop().expect("len checked");
                let mut merged = Vec::with_capacity(list1.len() + list2.len());
                self.merger.merge(list1, list2, &mut merged);
                self.chain_push(merged);
            }
        }
    }

    fn chain_pop(&mut self) -> Option<Vec<M::Chunk>> {
        let chain = self.chains.pop();
        self.ctx.account(chain.iter().flatten().map(M::account), -1);
        chain
    }

    fn chain_push(&mut self, chain: Vec<M::Chunk>) {
        self.ctx.account(chain.iter().map(M::account), 1);
        self.chains.push(chain);
    }
}

impl<M: TemporalMerger> PushInto<M::Output> for TemporalBucketingMergeBatcher<M>
where
    M::Time: BucketTimestamp + TemporalThreshold,
{
    fn push_into(&mut self, input: M::Output) {
        let chunk = self.merger.absorb(input);
        if M::chunk_len(&chunk) > 0 {
            self.insert_chain(vec![chunk]);
        }
    }
}

impl<M: TemporalMerger> Batcher for TemporalBucketingMergeBatcher<M>
where
    M::Time: BucketTimestamp + TemporalThreshold,
{
    type Time = M::Time;
    type Output = M::Output;

    fn new(logger: Option<Logger>, operator_id: usize) -> Self {
        let ctx = Rc::new(BucketCtx {
            touched: Cell::new(0),
            logger,
            operator_id,
        });
        Self {
            chains: Vec::new(),
            bucket_chain: BucketChain::new(MergeBucket::empty(Rc::clone(&ctx))),
            merger: M::default(),
            threshold: M::Time::temporal_threshold(),
            ctx,
            high_water: (0, 0),
            lower: Antichain::from_elem(M::Time::minimum()),
            frontier: Antichain::new(),
        }
    }

    fn seal(
        &mut self,
        upper: Antichain<Self::Time>,
    ) -> (Vec<Self::Output>, Description<Self::Time>) {
        // Merge the flat chains into one and partition it by time. This is
        // the same work the plain batcher performs at every seal.
        while self.chains.len() > 1 {
            let list1 = self.chain_pop().expect("len checked");
            let list2 = self.chain_pop().expect("len checked");
            let mut merged = Vec::with_capacity(list1.len() + list2.len());
            self.merger.merge(list1, list2, &mut merged);
            self.chain_push(merged);
        }
        let merged = self.chain_pop().unwrap_or_default();

        // The far boundary is `upper + threshold`. Elements whose advance
        // overflows drop out; if that empties the boundary, nothing
        // classifies as far and all future data stays in the flat chains,
        // which is the plain batcher's behavior.
        let mut far_lower = Antichain::new();
        for time in upper.elements() {
            if let Some(advanced) = self.threshold.results_in(time) {
                far_lower.insert(advanced);
            }
        }

        // The `before` part is emitted, so its bounds are never needed; the
        // `within` minimum feeds the frontier and the `beyond` bounds feed
        // the bucket routing.
        let parts =
            self.merger
                .extract_time_partitioned(merged, upper.borrow(), far_lower.borrow(), false);
        let mut readied = parts.before;

        // Near-future data returns to the flat chains, exactly like the
        // plain batcher's kept updates.
        if !parts.within.is_empty() {
            self.chain_push(parts.within);
        }

        let seal_touched_start = self.ctx.touched.get();

        if !parts.beyond.is_empty() {
            match parts.beyond_bounds.clone() {
                Some((lo, hi)) => self.route_into_bucket_chain(parts.beyond, lo, hi),
                None => {
                    // The extract reports bounds for every non-empty part.
                    // Recover with another extract against the minimum
                    // frontier: everything classifies as beyond and the walk
                    // re-derives the attained bounds.
                    soft_panic_or_log!("temporal batcher: non-empty extract part without bounds");
                    let at = Antichain::from_elem(M::Time::minimum());
                    let parts = self.merger.extract_time_partitioned(
                        parts.beyond,
                        at.borrow(),
                        at.borrow(),
                        false,
                    );
                    if let Some((lo, hi)) = parts.beyond_bounds {
                        self.route_into_bucket_chain(parts.beyond, lo, hi);
                    }
                }
            }
        }

        let held = self.held_records();
        if held > 0 {
            // Release matured buckets. `peel`'s contract guarantees peeled
            // buckets lie entirely below `upper`, so their contents join the
            // readied chain without a further extract.
            let peeled = self.bucket_chain.peel(upper.borrow());
            let mut peeled_chain: Vec<M::Chunk> = Vec::new();
            for bucket in peeled {
                let Some(content) = bucket.take_content() else {
                    continue;
                };
                let one = merge_into_one::<M>(content.chains, &mut self.merger, &self.ctx);
                // TODO: A merge tree would beat sequential merging when many
                // data-bearing buckets mature at once (large frontier jumps).
                peeled_chain = self.merge_chains(peeled_chain, one);
            }
            readied = self.merge_chains(readied, peeled_chain);

            let mut fuel = RESTORE_FUEL;
            self.bucket_chain.restore(&mut fuel);
        }

        // The frontier is the exact minimum of held times: the near data
        // returned to the flat chains and the lowest non-empty bucket's
        // attained minimum (buckets are range-ordered, and each bucket's data
        // lies within its range, so the first non-empty bucket attains the
        // bucket-chain minimum).
        self.frontier.clear();
        if let Some((lo, _)) = &parts.within_bounds {
            self.frontier.insert(lo.clone());
        }
        if let Some(content) = self
            .bucket_chain
            .buckets()
            .find_map(|bucket| bucket.content.as_ref())
        {
            self.frontier.insert(content.lo.clone());
        }

        // The arrange operator downgrades its capabilities to our frontier
        // after each seal, so a frontier element below `upper` would let it
        // hold a capability below the sealed trace upper, and the trace
        // import panics minting a capability from that batch's hint.
        if !PartialOrder::less_equal(&upper.borrow(), &self.frontier.borrow()) {
            let buckets: Vec<_> = self
                .bucket_chain
                .ranges()
                .map(|(range, bucket)| {
                    let bounds = bucket
                        .content
                        .as_ref()
                        .map(|c| (c.lo.clone(), c.hi.clone(), c.records));
                    (range.start.clone(), range.end().cloned(), bounds)
                })
                .collect();
            soft_panic_or_log!(
                "temporal batcher: frontier regressed below seal upper: \
                 upper {:?}, frontier {:?}, lower {:?}, within_bounds {:?}, \
                 held {}, buckets (start, end, (lo, hi, records)) {:?}",
                upper.elements(),
                self.frontier.elements(),
                self.lower.elements(),
                parts.within_bounds,
                held,
                buckets,
            );
        }

        let seal_touched = self.ctx.touched.get() - seal_touched_start;
        if seal_touched > self.high_water.0 {
            self.high_water = (seal_touched, u64::cast_from(held));
        }
        if seal_touched > 0 {
            // Observability for the bucket-chain work: expected to stay
            // within a small multiple of held records per seal (large only
            // at hydration-shaped seals). Fast-path seals never log.
            tracing::debug!(
                operator_id = self.ctx.operator_id,
                seal_touched,
                held,
                upper = ?upper.elements(),
                "temporal batcher seal performed bucket-chain work",
            );
        }

        self.merger.seal_done();

        let description = Description::new(
            self.lower.clone(),
            upper.clone(),
            Antichain::from_elem(M::Time::minimum()),
        );
        self.lower = upper;
        let readied = readied
            .into_iter()
            .map(|chunk| self.merger.materialize(chunk))
            .collect();
        (readied, description)
    }

    fn frontier(&mut self) -> AntichainRef<'_, Self::Time> {
        self.frontier.borrow()
    }
}

impl<M: TemporalMerger> Drop for TemporalBucketingMergeBatcher<M>
where
    M::Time: BucketTimestamp,
{
    fn drop(&mut self) {
        // Retract all accounted data: the flat chains and the bucket chain.
        for chain in self.chains.drain(..) {
            self.ctx.account(chain.iter().map(M::account), -1);
        }
        for bucket in self.bucket_chain.buckets_mut() {
            if let Some(content) = bucket.content.take() {
                self.ctx
                    .account(content.chains.iter().flatten().map(M::account), -1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use columnar::{Columnar, Index, Len};
    use differential_dataflow::consolidation::consolidate_updates;
    use proptest::prelude::*;
    use timely::container::PushInto;

    use crate::column_pager::{ColumnPager, PageDecision, PageEvent, PageHint, PagingPolicy};
    use crate::columnar::Column;
    use crate::columnar::merge_batcher::PagedColumnMerger;
    use crate::columnation::{ColTemporalMerger, ColumnationStack};

    use super::*;

    type TestUpdate = (u64, u64, i64);
    type TestMerger = ColTemporalMerger<u64, u64, i64>;
    type TestBatcher = TemporalBucketingMergeBatcher<TestMerger>;
    type PagedTestMerger = PagedColumnMerger<u64, u64, i64>;
    type PagedTestBatcher = TemporalBucketingMergeBatcher<PagedTestMerger>;

    /// Chunk conversions that let the generic drivers run over any merger.
    trait TestChunks: TemporalMerger<Time = u64> {
        fn chunk(updates: Vec<TestUpdate>) -> Self::Output;
        fn unchunk(chunks: Vec<Self::Output>) -> Vec<TestUpdate>;
    }

    impl TestChunks for TestMerger {
        fn chunk(updates: Vec<TestUpdate>) -> ColumnationStack<TestUpdate> {
            let mut chunk = ColumnationStack::default();
            for update in updates {
                chunk.push_into(update);
            }
            chunk
        }

        fn unchunk(chunks: Vec<ColumnationStack<TestUpdate>>) -> Vec<TestUpdate> {
            let mut out = Vec::new();
            for chunk in chunks {
                out.extend_from_slice(&chunk[..]);
            }
            out
        }
    }

    impl TestChunks for PagedTestMerger {
        fn chunk(updates: Vec<TestUpdate>) -> Column<TestUpdate> {
            let mut chunk = Column::default();
            for update in updates {
                chunk.push_into(update);
            }
            chunk
        }

        fn unchunk(chunks: Vec<Column<TestUpdate>>) -> Vec<TestUpdate> {
            let mut out = Vec::new();
            for chunk in chunks {
                let view = chunk.borrow();
                for i in 0..view.len() {
                    let (data, time, diff) = view.get(i);
                    out.push((
                        u64::into_owned(data),
                        u64::into_owned(time),
                        i64::into_owned(diff),
                    ));
                }
            }
            out
        }
    }

    fn batcher(threshold: u64) -> TestBatcher {
        let mut batcher = TestBatcher::new(None, 0);
        batcher.set_threshold(threshold);
        batcher
    }

    /// Policy that pages every chunk out (swap-backed, uncompressed), so the
    /// paged tests exercise offloaded chains rather than the resident
    /// fast path.
    struct PageAllPolicy;

    impl PagingPolicy for PageAllPolicy {
        fn decide(&self, _hint: PageHint) -> PageDecision {
            PageDecision::Page {
                backend: mz_ore::pager::Backend::Swap,
                codec: None,
            }
        }
        fn record(&self, _event: PageEvent) {}
    }

    fn paged_batcher(threshold: u64, page_all: bool) -> PagedTestBatcher {
        let mut batcher = PagedTestBatcher::new(None, 0);
        batcher.set_threshold(threshold);
        let pager = if page_all {
            ColumnPager::new(Arc::new(PageAllPolicy))
        } else {
            ColumnPager::disabled()
        };
        batcher.merger_mut().set_pager(pager);
        batcher
    }

    /// Consolidate and push `updates` as a single chunk, mirroring the
    /// chunker contract (sorted, consolidated input).
    fn push<M: TestChunks>(
        batcher: &mut TemporalBucketingMergeBatcher<M>,
        mut updates: Vec<TestUpdate>,
    ) {
        consolidate_updates(&mut updates);
        batcher.push_into(M::chunk(updates));
    }

    fn seal<M: TestChunks>(
        batcher: &mut TemporalBucketingMergeBatcher<M>,
        upper: Option<u64>,
    ) -> Vec<TestUpdate> {
        let upper = match upper {
            Some(time) => Antichain::from_elem(time),
            None => Antichain::new(),
        };
        let (chain, _description) = batcher.seal(upper);
        M::unchunk(chain)
    }

    fn consolidated(mut updates: Vec<TestUpdate>) -> Vec<TestUpdate> {
        consolidate_updates(&mut updates);
        updates
    }

    #[mz_ore::test]
    fn all_past_takes_fast_path() {
        let mut b = batcher(2_000);
        push(&mut b, vec![(1, 10, 1), (2, 20, 1), (1, 30, -1)]);
        let out = seal(&mut b, Some(100));
        assert_eq!(
            consolidated(out),
            consolidated(vec![(1, 10, 1), (2, 20, 1), (1, 30, -1)])
        );
        assert_eq!(b.temporal_stats(), TemporalBatcherStats::default());
        assert!(b.frontier().is_empty());
    }

    #[mz_ore::test]
    fn near_future_stays_flat() {
        let mut b = batcher(2_000);
        // Data one "tick" ahead of the upper: within the threshold.
        push(&mut b, vec![(1, 150, 1), (2, 1_050, 1)]);
        let out = seal(&mut b, Some(100));
        assert_eq!(out, vec![]);
        // Never entered the bucket chain.
        assert_eq!(b.temporal_stats().touched, 0);
        assert_eq!(b.frontier().to_owned(), Antichain::from_elem(150));
        let out = seal(&mut b, Some(200));
        assert_eq!(out, vec![(1, 150, 1)]);
        assert_eq!(b.frontier().to_owned(), Antichain::from_elem(1_050));
        let out = seal(&mut b, Some(2_000));
        assert_eq!(out, vec![(2, 1_050, 1)]);
        assert_eq!(b.temporal_stats().touched, 0);
        assert!(b.frontier().is_empty());
    }

    #[mz_ore::test]
    fn far_future_bucketed_and_released() {
        let mut b = batcher(1_000);
        push(&mut b, vec![(1, 50, 1), (2, 5_000, 1), (3, 100_000, 1)]);
        let out = seal(&mut b, Some(100));
        assert_eq!(out, vec![(1, 50, 1)]);
        // Both future updates are beyond upper + threshold: bucketed.
        assert!(b.temporal_stats().touched > 0);
        assert_eq!(b.frontier().to_owned(), Antichain::from_elem(5_000));
        b.validate_bounds_invariant().unwrap();

        let out = seal(&mut b, Some(6_000));
        assert_eq!(out, vec![(2, 5_000, 1)]);
        assert_eq!(b.frontier().to_owned(), Antichain::from_elem(100_000));
        b.validate_bounds_invariant().unwrap();

        let out = seal(&mut b, Some(200_000));
        assert_eq!(out, vec![(3, 100_000, 1)]);
        assert!(b.frontier().is_empty());
        b.validate_bounds_invariant().unwrap();
    }

    #[mz_ore::test]
    fn retraction_consolidates_across_seals() {
        let mut b = batcher(0);
        push(&mut b, vec![(7, 10_000, 1)]);
        let out = seal(&mut b, Some(100));
        assert_eq!(out, vec![]);
        push(&mut b, vec![(7, 10_000, -1)]);
        let out = seal(&mut b, Some(200));
        assert_eq!(out, vec![]);
        b.validate_bounds_invariant().unwrap();
        // The insert and its retraction met in the bucket chain; nothing
        // remains to emit.
        let out = seal(&mut b, None);
        assert_eq!(consolidated(out), vec![]);
    }

    #[mz_ore::test]
    fn final_seal_flushes_everything() {
        let mut b = batcher(1_000);
        push(&mut b, vec![(1, 50, 1), (2, 1_500, 1), (3, 1 << 40, 1)]);
        let out = seal(&mut b, Some(100));
        assert_eq!(out, vec![(1, 50, 1)]);
        let out = seal(&mut b, None);
        assert_eq!(consolidated(out), vec![(2, 1_500, 1), (3, 1 << 40, 1)]);
        assert!(b.frontier().is_empty());
        assert_eq!(b.held_records(), 0);
    }

    #[mz_ore::test]
    fn hydration_touched_within_bound() {
        // Snapshot below the upper plus a uniform 45-day tail above it,
        // sealed once: the simulation-backed contract is that bucket work
        // stays within a small multiple of the tail.
        let now = 1_752_192_000_000_u64;
        let window = 45 * 86_400_000_u64;
        let tail = 10_000_u64;
        let mut b = batcher(2_000);
        let mut updates = Vec::new();
        for i in 0..tail {
            updates.push((i, now - 1_000 - (i % 1_000), 1));
            updates.push((i, now + 1 + (i * (window / tail)), -1));
        }
        push(&mut b, updates);
        let out = seal(&mut b, Some(now));
        assert_eq!(out.len(), usize::try_from(tail).unwrap());
        b.validate_bounds_invariant().unwrap();
        let stats = b.temporal_stats();
        assert!(
            stats.touched <= 5 * tail,
            "hydration touched {} > 5x tail {tail}",
            stats.touched
        );
        // The single hydration seal did all the bucket work so far, so it is
        // the high-water seal, with all far retractions held at its start.
        // The i == 0 retraction lands within the threshold and stays in the
        // flat chains.
        assert_eq!(stats.high_water_touched, stats.touched);
        assert_eq!(stats.high_water_held, tail - 1);
        // Tick forward; steady-state work must stay negligible.
        let before = b.temporal_stats().touched;
        let mut released = 0;
        for tick in 1..=60 {
            released += seal(&mut b, Some(now + tick * 1_000)).len();
            b.validate_bounds_invariant().unwrap();
        }
        let per_tick = (b.temporal_stats().touched - before) / 60;
        assert!(
            per_tick <= 4 * (u64::try_from(released).unwrap() / 60 + 1),
            "steady-state per-tick touched {per_tick} too high"
        );
    }

    /// Model-based test: the batcher must emit, at each seal, exactly the
    /// not-yet-emitted updates below the upper, consolidated, and report the
    /// exact minimum of what it retains.
    fn run_model<M: TestChunks>(
        mut b: TemporalBucketingMergeBatcher<M>,
        ops: Vec<(Vec<(u8, u16)>, u16)>,
    ) {
        let mut pending: Vec<TestUpdate> = Vec::new();
        let mut lower = 0_u64;
        for (batch, advance) in ops {
            let updates: Vec<TestUpdate> = batch
                .into_iter()
                .map(|(data, dt)| (u64::from(data), lower + u64::from(dt), 1))
                .collect();
            pending.extend(updates.iter().copied());
            push(&mut b, updates);

            let upper = lower + u64::from(advance);
            let out = seal(&mut b, Some(upper));
            b.validate_bounds_invariant().unwrap();

            let expected: Vec<TestUpdate> = pending
                .iter()
                .copied()
                .filter(|(_, t, _)| *t < upper)
                .collect();
            pending.retain(|(_, t, _)| *t >= upper);
            assert_eq!(consolidated(out), consolidated(expected));

            let min_pending = pending.iter().map(|(_, t, _)| *t).min();
            let expected_frontier = match min_pending {
                Some(t) => Antichain::from_elem(t),
                None => Antichain::new(),
            };
            assert_eq!(b.frontier().to_owned(), expected_frontier);

            lower = upper;
        }
        let out = seal(&mut b, None);
        assert_eq!(consolidated(out), consolidated(pending));
    }

    /// Strategy for [`run_model`] ops: per step, a batch of
    /// `(data, time_delta)` updates and a frontier advance.
    fn model_ops() -> impl Strategy<Value = Vec<(Vec<(u8, u16)>, u16)>> {
        prop::collection::vec(
            (
                prop::collection::vec((any::<u8>(), any::<u16>()), 0..20),
                any::<u16>(),
            ),
            1..20,
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn batcher_matches_model(
            threshold in proptest::sample::select(vec![0_u64, 1, 10, 1_000]),
            ops in model_ops(),
        ) {
            run_model(batcher(threshold), ops);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn paged_batcher_matches_model(
            threshold in proptest::sample::select(vec![0_u64, 1, 10, 1_000]),
            page_all in any::<bool>(),
            ops in model_ops(),
        ) {
            run_model(paged_batcher(threshold, page_all), ops);
        }
    }

    /// Simulate the capability protocol between differential's arrange
    /// operator and a downstream trace import, asserting the import can
    /// always mint a delayed capability for the batches it receives.
    ///
    /// The arrange holds one capability (the minimum, since the time is
    /// totally ordered), downgrades it to `batcher.frontier()` after each
    /// real seal, and uses it as the batch hint. The import's capability set
    /// tracks the trace upper. A batcher frontier below a sealed upper lets
    /// the arrange hold a capability below the trace upper, and the next
    /// batch's hint then panics the import.
    ///
    /// Each step mirrors one arrange invocation: optionally deliver one
    /// message (capability at `frontier + cap_delta`, update times at
    /// `capability + time_delta`, diffs `±1` so updates can cancel), then
    /// advance the input frontier by `advance`.
    fn run_arrange_protocol<M: TestChunks>(
        mut b: TemporalBucketingMergeBatcher<M>,
        steps: Vec<(u8, Vec<(u8, u8, bool)>, u8)>,
    ) {
        let mut frontier = 0_u64;
        let mut cap: Option<u64> = None;
        let mut writer_upper = 0_u64;
        let mut import_cap = 0_u64;

        for (cap_delta, updates, advance) in steps {
            if !updates.is_empty() {
                let msg_cap = frontier + u64::from(cap_delta);
                let updates: Vec<TestUpdate> = updates
                    .into_iter()
                    .map(|(data, dt, sign)| {
                        let diff = if sign { 1 } else { -1 };
                        (u64::from(data), msg_cap + u64::from(dt), diff)
                    })
                    .collect();
                push(&mut b, updates);
                cap = Some(cap.map_or(msg_cap, |c| c.min(msg_cap)));
            }

            // The arrange seals only on strict progress.
            if advance == 0 {
                continue;
            }
            frontier += u64::from(advance);

            if cap.is_some_and(|c| c < frontier) {
                // Real seal: emit a batch with the capability as its hint.
                let hint = cap.expect("checked above");
                let (chain, description) = b.seal(Antichain::from_elem(frontier));
                b.validate_bounds_invariant().unwrap();
                assert_eq!(
                    description.lower().as_ref(),
                    &[writer_upper],
                    "writer continuity"
                );
                writer_upper = frontier;
                // The import processes Batch(chain, hint), then
                // Frontier(upper). Empty batches skip the mint.
                if !M::unchunk(chain).is_empty() {
                    assert!(
                        import_cap <= hint,
                        "import cannot mint delayed capability: \
                         hint {hint}, import capability {import_cap}",
                    );
                }
                import_cap = frontier;
                // Downgrade the arrange capability to the batcher frontier.
                cap = match b.frontier().as_ref() {
                    [] => None,
                    [time] => {
                        assert!(hint <= *time, "failed to find capability");
                        Some(*time)
                    }
                    _ => unreachable!("total order"),
                };
            } else {
                // All capabilities in advance of the frontier: the arrange
                // seals the batcher, discards the output, and seals the
                // writer, which announces the new upper to the import.
                let _ = b.seal(Antichain::from_elem(frontier));
                b.validate_bounds_invariant().unwrap();
                if writer_upper != frontier {
                    writer_upper = frontier;
                    import_cap = frontier;
                }
            }
        }
    }

    /// Strategy for [`run_arrange_protocol`] steps: per step, an optional
    /// message (capability delta, updates) and a frontier advance.
    fn protocol_steps() -> impl Strategy<Value = Vec<(u8, Vec<(u8, u8, bool)>, u8)>> {
        prop::collection::vec(
            (
                0..4_u8,
                prop::collection::vec((0..3_u8, 0..12_u8, any::<bool>()), 0..6),
                0..8_u8,
            ),
            1..40,
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1024))]
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn arrange_protocol_mints_capabilities(
            threshold in proptest::sample::select(vec![0_u64, 1, 2, 4]),
            steps in protocol_steps(),
        ) {
            run_arrange_protocol(batcher(threshold), steps);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn paged_arrange_protocol_mints_capabilities(
            threshold in proptest::sample::select(vec![0_u64, 1, 2, 4]),
            page_all in any::<bool>(),
            steps in protocol_steps(),
        ) {
            run_arrange_protocol(paged_batcher(threshold, page_all), steps);
        }
    }
}
